use std::{collections::HashMap, sync::Arc};

use turso_parser::parser::Parser;

use crate::{
    alloc::{TursoAllocExt, TursoVecExt},
    database_tape::{run_stmt_once, DatabaseReplaySessionOpts},
    errors::Error,
    types::{
        Coro, DatabaseChangeType, DatabaseRowMutation, DatabaseTapeRowChange,
        DatabaseTapeRowChangeType,
    },
    Result,
};

pub struct DatabaseReplayGenerator {
    pub conn: Arc<turso_core::Connection>,
    pub opts: DatabaseReplaySessionOpts,
}

#[derive(Debug)]
pub struct ReplayInfo {
    pub change_type: DatabaseChangeType,
    pub query: String,
    pub pk_column_indices: Option<Vec<usize>>,
    pub rowid_alias_pk_column_index: Option<usize>,
    pub column_names: Vec<String>,
    pub is_ddl_replay: bool,
}

const SQLITE_SCHEMA_TABLE: &str = "sqlite_schema";

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Identity predicate for one primary-key column.
///
/// `IS` instead of `=` because rowid tables allow NULL in PRIMARY KEY columns
/// (only a rowid-alias INTEGER PRIMARY KEY is implicitly NOT NULL), and
/// `NULL = NULL` is not true: an `=` predicate silently matches no row, so a
/// replayed DELETE or UPDATE of such a row applies nowhere. `IS` is NULL-safe
/// and, like SQLite, still seeks the index.
fn identity_predicate(column_name: &str) -> String {
    format!("{} IS ?", quote_ident(column_name))
}

/// The aliases SQLite accepts for a rowid table's implicit rowid, in the order
/// the replay generator prefers them.
const IMPLICIT_ROWID_ALIASES: [&str; 3] = ["rowid", "_rowid_", "oid"];

/// Resolve the identifier that names the implicit rowid of `table_name`.
///
/// A user column may shadow any of the three aliases, in which case that
/// identifier resolves to the user column and a rowid-based replay statement
/// would match nothing (or write the wrong column). Pick the first alias the
/// table does not shadow; if it shadows all three the implicit rowid is
/// unreachable from SQL, so refuse the replay instead of corrupting the row.
fn implicit_rowid_alias(table_name: &str, column_names: &[String]) -> Result<&'static str> {
    IMPLICIT_ROWID_ALIASES
        .into_iter()
        .find(|alias| {
            !column_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(alias))
        })
        .ok_or_else(|| {
            Error::DatabaseTapeError(format!(
                "table '{table_name}' has user columns shadowing every implicit rowid alias ({}); refusing rowid-based replay",
                IMPLICIT_ROWID_ALIASES.join(", ")
            ))
        })
}

impl DatabaseReplayGenerator {
    pub fn new(conn: Arc<turso_core::Connection>, opts: DatabaseReplaySessionOpts) -> Self {
        Self { conn, opts }
    }
    pub fn create_mutation(
        &self,
        info: &ReplayInfo,
        change: &DatabaseTapeRowChange,
    ) -> Result<DatabaseRowMutation> {
        match &change.change {
            DatabaseTapeRowChangeType::Delete { before, .. } => Ok(DatabaseRowMutation {
                change_time: change.change_time,
                table_name: change.table_name.to_string(),
                id: change.id,
                change_type: info.change_type,
                before: Some(self.create_row_full(info, before)),
                after: None,
                updates: None,
            }),
            DatabaseTapeRowChangeType::Insert { after } => Ok(DatabaseRowMutation {
                change_time: change.change_time,
                table_name: change.table_name.to_string(),
                id: change.id,
                change_type: info.change_type,
                before: None,
                after: Some(self.create_row_full(info, after)),
                updates: None,
            }),
            DatabaseTapeRowChangeType::Update {
                before,
                after,
                updates,
            } => Ok(DatabaseRowMutation {
                change_time: change.change_time,
                table_name: change.table_name.to_string(),
                id: change.id,
                change_type: info.change_type,
                before: Some(self.create_row_full(info, before)),
                after: Some(self.create_row_full(info, after)),
                updates: updates
                    .as_ref()
                    .map(|updates| self.create_row_update(info, updates)),
            }),
        }
    }
    fn create_row_full(
        &self,
        info: &ReplayInfo,
        values: &[turso_core::Value],
    ) -> HashMap<String, turso_core::Value> {
        let mut row = HashMap::with_capacity(info.column_names.len());
        for (i, value) in values.iter().enumerate() {
            tracing::trace!("info: {:?}, value: {:?}", info, value);
            row.insert(info.column_names[i].clone(), value.clone());
        }
        row
    }
    fn create_row_update(
        &self,
        info: &ReplayInfo,
        updates: &[turso_core::Value],
    ) -> HashMap<String, turso_core::Value> {
        let mut row = HashMap::with_capacity(info.column_names.len());
        assert!(updates.len().is_multiple_of(2));
        let columns_cnt = updates.len() / 2;
        for (i, value) in updates.iter().take(columns_cnt).enumerate() {
            let updated = match value {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(x @ (1 | 0))) => *x > 0,
                _ => {
                    panic!("unexpected 'changes' binary record first-half component: {value:?}")
                }
            };
            if !updated {
                continue;
            }
            row.insert(
                info.column_names[i].clone(),
                updates[columns_cnt + i].clone(),
            );
        }
        row
    }
    /// Bind values for an INSERT/UPSERT/UPDATE replay statement.
    ///
    /// `record` is the row's after image. `before` is the row's before image and
    /// is only used for UPDATE: the WHERE clause pins the row by the primary key
    /// it had *before* the change, so an update that moves a key still finds its
    /// row. Binding the after image there makes a key-changing UPDATE match zero
    /// rows — silently, since affecting no rows is not an error.
    pub fn replay_values(
        &self,
        info: &ReplayInfo,
        change: DatabaseChangeType,
        id: i64,
        record: crate::alloc::Vec<turso_core::Value>,
        updates: Option<crate::alloc::Vec<turso_core::Value>>,
        before: Option<crate::alloc::Vec<turso_core::Value>>,
    ) -> crate::alloc::Vec<turso_core::Value> {
        let mut record = record;
        if info.is_ddl_replay {
            return <crate::alloc::Vec<turso_core::Value> as TursoAllocExt>::new();
        }
        match change {
            DatabaseChangeType::Delete => {
                unreachable!("DELETE replay values are built by replay_delete_values")
            }
            DatabaseChangeType::Insert => {
                if let Some(pk) = info.rowid_alias_pk_column_index {
                    record[pk] = turso_core::Value::from_i64(id);
                    return record;
                }
                if self.opts.use_implicit_rowid && info.pk_column_indices.is_none() {
                    record.push(turso_core::Value::from_i64(id));
                }
                record
            }
            DatabaseChangeType::Update => {
                let mut updates = updates.unwrap();
                assert!(updates.len().is_multiple_of(2));
                let columns_cnt = updates.len() / 2;
                let mut values =
                    <crate::alloc::Vec<turso_core::Value> as TursoVecExt<_>>::with_capacity(
                        columns_cnt + 1,
                    );
                for i in 0..columns_cnt {
                    let changed = match updates[i] {
                        turso_core::Value::Numeric(turso_core::Numeric::Integer(x @ (1 | 0))) => {
                            x > 0
                        }
                        _ => panic!(
                            "unexpected 'changes' binary record first-half component: {:?}",
                            updates[i]
                        ),
                    };
                    if !changed {
                        continue;
                    }
                    let value =
                        std::mem::replace(&mut updates[i + columns_cnt], turso_core::Value::Null);
                    values.push(value);
                }
                if let Some(pk_column_indices) = &info.pk_column_indices {
                    // Identity predicates are emitted in `pk_column_indices`
                    // order by `update_query`, so the binds follow that order.
                    // They come from the before image: see `replay_values`.
                    let mut identity = before.unwrap_or(record);
                    for pk in pk_column_indices {
                        let value = if info.rowid_alias_pk_column_index == Some(*pk) {
                            turso_core::Value::from_i64(id)
                        } else {
                            std::mem::replace(&mut identity[*pk], turso_core::Value::Null)
                        };
                        values.push(value);
                    }
                } else {
                    values.push(turso_core::Value::from_i64(id));
                }
                values
            }
            DatabaseChangeType::Commit => {
                // COMMIT records are handled at the tape level, not here
                <crate::alloc::Vec<turso_core::Value> as TursoAllocExt>::new()
            }
        }
    }

    pub fn replay_delete_values(
        &self,
        info: &ReplayInfo,
        id: i64,
        mut before: crate::alloc::Vec<turso_core::Value>,
        key: Option<crate::alloc::Vec<turso_core::Value>>,
    ) -> Result<crate::alloc::Vec<turso_core::Value>> {
        if info.is_ddl_replay {
            return Ok(<crate::alloc::Vec<turso_core::Value> as TursoAllocExt>::new());
        }
        if let Some(key) = key {
            let Some(pk_column_indices) = info.pk_column_indices.as_ref() else {
                return Err(Error::DatabaseTapeError(format!(
                    "DELETE primary-key projection cannot be used with a rowid replay query: {}",
                    info.query
                )));
            };
            if key.len() != pk_column_indices.len() {
                return Err(Error::DatabaseTapeError(format!(
                    "DELETE primary-key projection has {} values, expected {}: {}",
                    key.len(),
                    pk_column_indices.len(),
                    info.query
                )));
            }
            return Ok(key);
        }
        let Some(pk_column_indices) = info.pk_column_indices.as_ref() else {
            return Ok(crate::alloc::vec![turso_core::Value::from_i64(id)]);
        };
        let mut values = <crate::alloc::Vec<turso_core::Value> as TursoAllocExt>::new();
        for &pk in pk_column_indices {
            let value = if info.rowid_alias_pk_column_index == Some(pk) {
                turso_core::Value::from_i64(id)
            } else {
                let Some(value) = before.get_mut(pk) else {
                    return Err(Error::DatabaseTapeError(format!(
                        "DELETE before image is missing primary-key column {pk}: {}",
                        info.query
                    )));
                };
                std::mem::replace(value, turso_core::Value::Null)
            };
            values.push(value);
        }
        Ok(values)
    }

    /// Whether an upsert replay of `record` needs a NULL-safe pre-delete.
    ///
    /// `INSERT ... ON CONFLICT(pk) DO UPDATE` resolves nothing when a key
    /// component is NULL: NULLs are distinct in a unique index, so no conflict
    /// is raised and the upsert appends a duplicate row instead of updating the
    /// existing one. Such a row must be deleted through the NULL-safe identity
    /// predicates first, after which the insert lands cleanly.
    pub(crate) fn upsert_needs_null_safe_predelete(
        &self,
        info: &ReplayInfo,
        record: &[turso_core::Value],
    ) -> bool {
        let Some(pk_column_indices) = info.pk_column_indices.as_ref() else {
            return false;
        };
        pk_column_indices.iter().any(|&pk| {
            // A rowid-alias primary key is the implicit rowid: never NULL.
            info.rowid_alias_pk_column_index != Some(pk)
                && matches!(record.get(pk), Some(turso_core::Value::Null))
        })
    }

    /// Whether a DELETE replay must fall back to the implicit rowid: only when
    /// the change carries neither a primary-key projection nor a before image.
    /// In that case the rowid is the row's only identity, so replaying it
    /// requires rowid preservation — and `delete_query` additionally rejects
    /// the fallback for tables whose PRIMARY KEY is not the rowid, where a
    /// rowid-based delete could target the wrong row.
    pub(crate) fn delete_uses_rowid(
        &self,
        before: &[turso_core::Value],
        key: Option<&[turso_core::Value]>,
    ) -> Result<bool> {
        if key.is_some() {
            return Ok(false);
        }
        if !before.is_empty() {
            return Ok(false);
        }
        if self.opts.use_implicit_rowid {
            return Ok(true);
        }
        Err(Error::DatabaseTapeError(
            "DELETE replay without a row image requires implicit rowid preservation".to_string(),
        ))
    }

    pub async fn replay_info<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        change: &DatabaseTapeRowChange,
    ) -> Result<ReplayInfo> {
        tracing::trace!("replay: change={:?}", change);
        let table_name = &change.table_name;

        if table_name == SQLITE_SCHEMA_TABLE {
            // sqlite_schema table: type, name, tbl_name, rootpage, sql
            match &change.change {
                DatabaseTapeRowChangeType::Delete { before, .. } => {
                    assert!(before.len() == 5);
                    let Some(turso_core::Value::Text(entity_type)) = before.first() else {
                        panic!(
                            "unexpected 'type' column of sqlite_schema table: {:?}",
                            before.first()
                        );
                    };
                    let Some(turso_core::Value::Text(entity_name)) = before.get(1) else {
                        panic!(
                            "unexpected 'name' column of sqlite_schema table: {:?}",
                            before.get(1)
                        );
                    };
                    let query = format!("DROP {} {}", entity_type.as_str(), entity_name.as_str());
                    let delete = ReplayInfo {
                        change_type: DatabaseChangeType::Delete,
                        query,
                        pk_column_indices: None,
                        rowid_alias_pk_column_index: None,
                        column_names: Vec::new(),
                        is_ddl_replay: true,
                    };
                    Ok(delete)
                }
                DatabaseTapeRowChangeType::Insert { after } => {
                    assert!(after.len() == 5);
                    let Some(turso_core::Value::Text(sql)) = after.last() else {
                        return Err(Error::DatabaseTapeError(format!(
                            "unexpected 'sql' column of sqlite_schema table: {:?}",
                            after.last()
                        )));
                    };
                    let insert = ReplayInfo {
                        change_type: DatabaseChangeType::Insert,
                        query: sql.as_str().to_string(),
                        pk_column_indices: None,
                        rowid_alias_pk_column_index: None,
                        column_names: Vec::new(),
                        is_ddl_replay: true,
                    };
                    Ok(insert)
                }
                DatabaseTapeRowChangeType::Update { updates, .. } => {
                    let Some(updates) = updates else {
                        return Err(Error::DatabaseTapeError(
                            "'updates' column of CDC table must be populated".to_string(),
                        ));
                    };
                    assert!(updates.len() % 2 == 0);
                    assert!(updates.len() / 2 == 5);
                    let turso_core::Value::Text(ddl_stmt) = updates.last().unwrap() else {
                        panic!(
                            "unexpected 'sql' column of sqlite_schema table update record: {:?}",
                            updates.last()
                        );
                    };
                    let update = ReplayInfo {
                        change_type: DatabaseChangeType::Update,
                        query: ddl_stmt.as_str().to_string(),
                        pk_column_indices: None,
                        rowid_alias_pk_column_index: None,
                        column_names: Vec::new(),
                        is_ddl_replay: true,
                    };
                    Ok(update)
                }
            }
        } else {
            match &change.change {
                DatabaseTapeRowChangeType::Delete { before, key } => {
                    let use_rowid = self.delete_uses_rowid(before, key.as_deref())?;
                    let delete = self.delete_query(coro, table_name, use_rowid).await?;
                    Ok(delete)
                }
                DatabaseTapeRowChangeType::Update { updates, after, .. } => {
                    if let Some(updates) = updates {
                        assert!(updates.len() % 2 == 0);
                        let columns_cnt = updates.len() / 2;
                        let mut columns = Vec::with_capacity(columns_cnt);
                        for value in updates.iter().take(columns_cnt) {
                            columns.push(match value {
                                turso_core::Value::Numeric(turso_core::Numeric::Integer(x @ (1 | 0))) => *x > 0,
                                _ => panic!("unexpected 'changes' binary record first-half component: {value:?}")
                            });
                        }
                        let update = self.update_query(coro, table_name, &columns).await?;
                        Ok(update)
                    } else {
                        let upsert = self.upsert_query(coro, table_name, after.len()).await?;
                        Ok(upsert)
                    }
                }
                DatabaseTapeRowChangeType::Insert { after } => {
                    let insert = self.upsert_query(coro, table_name, after.len()).await?;
                    Ok(insert)
                }
            }
        }
    }
    pub(crate) async fn update_query<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
        columns: &[bool],
    ) -> Result<ReplayInfo> {
        let (column_names, pk_column_indices, rowid_alias_pk_column_index) =
            self.table_columns_info(coro, table_name).await?;
        // The CDC record may have fewer columns than the current schema
        // (e.g. records captured before ALTER TABLE ADD COLUMN).
        // Only reference columns present in the record.
        let record_len = columns.len();
        let record_columns = if record_len < column_names.len() {
            &column_names[..record_len]
        } else {
            &column_names[..]
        };
        let mut pk_predicates = Vec::with_capacity(1);
        let mut column_updates = Vec::with_capacity(1);
        for &idx in &pk_column_indices {
            if idx >= record_columns.len() {
                return Err(Error::DatabaseTapeError(format!(
                    "primary key column index {} is outside CDC record with {} columns for table '{}'",
                    idx, record_columns.len(), table_name
                )));
            }
            pk_predicates.push(identity_predicate(&record_columns[idx]));
        }
        for (idx, name) in record_columns.iter().enumerate() {
            if columns[idx] {
                column_updates.push(format!("{} = ?", quote_ident(name)));
            }
        }
        let quoted_table_name = quote_ident(table_name);
        let (query, pk_column_indices) = if pk_column_indices.is_empty() {
            let rowid_alias = implicit_rowid_alias(table_name, &column_names)?;
            (
                format!(
                    "UPDATE {quoted_table_name} SET {} WHERE {} = ?",
                    column_updates.join(", "),
                    quote_ident(rowid_alias)
                ),
                None,
            )
        } else {
            (
                format!(
                    "UPDATE {quoted_table_name} SET {} WHERE {}",
                    column_updates.join(", "),
                    pk_predicates.join(" AND ")
                ),
                Some(pk_column_indices),
            )
        };
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Update,
            query,
            column_names: record_columns.to_vec(),
            pk_column_indices,
            rowid_alias_pk_column_index,
            is_ddl_replay: false,
        })
    }
    pub(crate) async fn upsert_query<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
        columns: usize,
    ) -> Result<ReplayInfo> {
        let (column_names, pk_column_indices, rowid_alias_pk_column_index) =
            self.table_columns_info(coro, table_name).await?;
        // The CDC record may have fewer columns than the current schema
        // (e.g. records captured before ALTER TABLE ADD COLUMN).
        // Only reference columns present in the record.
        let record_columns = if columns < column_names.len() {
            &column_names[..columns]
        } else {
            &column_names[..]
        };
        let conflict_clause = if !pk_column_indices.is_empty() {
            let mut pk_column_names = Vec::new();
            for &idx in &pk_column_indices {
                if idx >= record_columns.len() {
                    return Err(Error::DatabaseTapeError(format!(
                        "primary key column index {} is outside CDC record with {} columns for table '{}'",
                        idx, record_columns.len(), table_name
                    )));
                }
                pk_column_names.push(quote_ident(&record_columns[idx]));
            }
            let mut update_clauses = Vec::new();
            for name in record_columns {
                let name = quote_ident(name);
                update_clauses.push(format!("{name} = excluded.{name}"));
            }
            format!(
                " ON CONFLICT({}) DO UPDATE SET {}",
                pk_column_names.join(","),
                update_clauses.join(",")
            )
        } else {
            String::new()
        };
        let quoted_table_name = quote_ident(table_name);
        if !self.opts.use_implicit_rowid || !pk_column_indices.is_empty() {
            let col_list = record_columns
                .iter()
                .map(|name| quote_ident(name))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = ["?"].repeat(columns).join(",");
            let query = format!(
                "INSERT INTO {quoted_table_name}({col_list}) VALUES ({placeholders}){conflict_clause}"
            );
            return Ok(ReplayInfo {
                change_type: DatabaseChangeType::Insert,
                query,
                pk_column_indices: (!pk_column_indices.is_empty()).then_some(pk_column_indices),
                rowid_alias_pk_column_index,
                column_names: record_columns.to_vec(),
                is_ddl_replay: false,
            });
        };
        let mut insert_columns = record_columns.to_vec();
        let original_column_names = insert_columns.clone();
        // Rowid preservation names the implicit rowid explicitly in the column
        // list, so it must use an alias the table does not shadow with a user
        // column of the same name.
        let rowid_alias = implicit_rowid_alias(table_name, &column_names)?;
        insert_columns.push(rowid_alias.to_string());

        let placeholders = ["?"].repeat(columns + 1).join(",");
        let col_list = insert_columns
            .iter()
            .map(|name| quote_ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_kind = if conflict_clause.is_empty() {
            "INSERT OR REPLACE"
        } else {
            "INSERT"
        };
        let query = format!(
            "{insert_kind} INTO {quoted_table_name}({col_list}) VALUES ({placeholders}){conflict_clause}"
        );
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Insert,
            query,
            column_names: original_column_names,
            pk_column_indices: None,
            rowid_alias_pk_column_index: None,
            is_ddl_replay: false,
        })
    }
    pub(crate) async fn delete_query<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
        use_rowid: bool,
    ) -> Result<ReplayInfo> {
        let (column_names, pk_column_indices, rowid_alias_pk_column_index) =
            self.table_columns_info(coro, table_name).await?;
        let mut pk_predicates = Vec::with_capacity(1);
        for &idx in &pk_column_indices {
            pk_predicates.push(identity_predicate(&column_names[idx]));
        }
        let use_implicit_rowid = self.opts.use_implicit_rowid;
        let quoted_table_name = quote_ident(table_name);
        if use_rowid || pk_column_indices.is_empty() {
            // A rowid-based delete is exact only when the rowid IS the row's
            // identity: tables with no PRIMARY KEY, or a rowid-alias INTEGER
            // PRIMARY KEY. For any other PK the local rowid can diverge from
            // the remote's, so a delete that arrived without a primary-key
            // projection (and without a before image) must fail instead of
            // possibly deleting the wrong row. A current server always encodes
            // the projection for such tables, so this only rejects logs from
            // servers predating the portable delete extension.
            if use_rowid && !pk_column_indices.is_empty() && rowid_alias_pk_column_index.is_none() {
                return Err(Error::DatabaseTapeError(format!(
                    "DELETE for table '{table_name}' has no primary-key projection and no before image, but its PRIMARY KEY is not the rowid; refusing rowid-based replay"
                )));
            }
            let rowid_alias = implicit_rowid_alias(table_name, &column_names)?;
            let query = format!(
                "DELETE FROM {quoted_table_name} WHERE {} = ?",
                quote_ident(rowid_alias)
            );
            tracing::trace!("delete_query: table_name={table_name}, query={query}, use_implicit_rowid={use_implicit_rowid}");
            return Ok(ReplayInfo {
                change_type: DatabaseChangeType::Delete,
                query,
                column_names,
                pk_column_indices: None,
                rowid_alias_pk_column_index: None,
                is_ddl_replay: false,
            });
        }
        let pk_predicates = pk_predicates.join(" AND ");
        let query = format!("DELETE FROM {quoted_table_name} WHERE {pk_predicates}");

        tracing::trace!("delete_query: table_name={table_name}, query={query}, use_implicit_rowid={use_implicit_rowid}");
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Delete,
            query,
            column_names,
            pk_column_indices: Some(pk_column_indices),
            rowid_alias_pk_column_index,
            is_ddl_replay: false,
        })
    }

    /// Execute a DDL statement idempotently: CREATE TABLE is replayed with
    /// `IF NOT EXISTS`, named schema objects are skipped when already present,
    /// and `ALTER TABLE ADD COLUMN` only adds missing columns. Falls back to
    /// direct execution for other DDL.
    pub async fn execute_ddl_idempotent<Ctx>(&self, coro: &Coro<Ctx>, ddl: &str) -> Result<()> {
        let mut parser = Parser::new(ddl.as_bytes());
        let Some(Ok(turso_parser::ast::Cmd::Stmt(mut stmt))) = parser.next() else {
            self.execute_ddl(ddl)?;
            return Ok(());
        };
        match &mut stmt {
            turso_parser::ast::Stmt::CreateTable {
                if_not_exists,
                tbl_name,
                body,
                ..
            } => {
                *if_not_exists = true;
                let table_name = tbl_name.name.as_str();
                let (current_columns, _, _) = self.table_columns_info(coro, table_name).await?;
                if current_columns.is_empty() {
                    self.execute_ddl(ddl)?;
                    return Ok(());
                }
                if let turso_parser::ast::CreateTableBody::ColumnsAndConstraints {
                    columns, ..
                } = body
                {
                    for column in columns {
                        let col_name = column.col_name.as_str();
                        if current_columns.iter().any(|c| c == col_name) {
                            continue;
                        }
                        let add_column = format!("ALTER TABLE {tbl_name} ADD COLUMN {column}");
                        self.execute_ddl(&add_column)?;
                    }
                }
                return Ok(());
            }
            turso_parser::ast::Stmt::CreateIndex { idx_name, .. } => {
                if self
                    .schema_object_exists(coro, "index", idx_name.name.as_str())
                    .await?
                {
                    return Ok(());
                }
                self.execute_ddl(ddl)?;
                return Ok(());
            }
            turso_parser::ast::Stmt::CreateTrigger { trigger_name, .. } => {
                if self
                    .schema_object_exists(coro, "trigger", trigger_name.name.as_str())
                    .await?
                {
                    return Ok(());
                }
                self.execute_ddl(ddl)?;
                return Ok(());
            }
            turso_parser::ast::Stmt::CreateMaterializedView { view_name, .. }
            | turso_parser::ast::Stmt::CreateView { view_name, .. } => {
                if self
                    .schema_object_exists(coro, "view", view_name.name.as_str())
                    .await?
                {
                    return Ok(());
                }
                self.execute_ddl(ddl)?;
                return Ok(());
            }
            _ => {}
        }
        let turso_parser::ast::Stmt::AlterTable(turso_parser::ast::AlterTable {
            name: tbl_name,
            body: turso_parser::ast::AlterTableBody::AddColumn(col_def),
        }) = stmt
        else {
            self.conn.execute(ddl)?;
            return Ok(());
        };
        let table_name = tbl_name.name.as_str();
        let (current_columns, _, _) = self.table_columns_info(coro, table_name).await?;
        let col_name = col_def.col_name.as_str();
        if current_columns.iter().any(|c| c == col_name) {
            tracing::debug!(
                "execute_ddl_idempotent: column {col_name} already exists in {table_name}, skipping"
            );
            return Ok(());
        }
        self.execute_ddl(ddl)?;
        Ok(())
    }

    fn execute_ddl(&self, ddl: &str) -> Result<()> {
        self.conn.execute(ddl).map_err(|error| {
            Error::DatabaseTapeError(format!("failed to execute DDL `{ddl}`: {error}"))
        })
    }

    async fn schema_object_exists<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        object_type: &str,
        name: &str,
    ) -> Result<bool> {
        let object_type = sql_string_literal(object_type);
        let name = sql_string_literal(name);
        let query =
            format!("SELECT 1 FROM sqlite_schema WHERE type = {object_type} AND name = {name}");
        let mut stmt = self.conn.prepare(query)?;
        Ok(run_stmt_once(coro, &mut stmt).await?.is_some())
    }

    async fn table_columns_info<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<(Vec<String>, Vec<usize>, Option<usize>)> {
        let table_name_literal = sql_string_literal(table_name);
        let mut table_info_stmt = self.conn.prepare(format!(
            "SELECT cid, name, type, pk FROM pragma_table_info({table_name_literal})"
        ))?;
        let mut pk_columns = Vec::with_capacity(1);
        let mut column_names = Vec::new();
        let mut column_types = Vec::new();
        while let Some(column) = run_stmt_once(coro, &mut table_info_stmt).await? {
            let turso_core::Value::Numeric(turso_core::Numeric::Integer(column_id)) =
                column.get_value(0)
            else {
                return Err(Error::DatabaseTapeError(
                    "unexpected column type for pragma_table_info query".to_string(),
                ));
            };
            let turso_core::Value::Text(name) = column.get_value(1) else {
                return Err(Error::DatabaseTapeError(
                    "unexpected column type for pragma_table_info query".to_string(),
                ));
            };
            let turso_core::Value::Text(column_type) = column.get_value(2) else {
                return Err(Error::DatabaseTapeError(
                    "unexpected column type for pragma_table_info query".to_string(),
                ));
            };
            let turso_core::Value::Numeric(turso_core::Numeric::Integer(pk)) = column.get_value(3)
            else {
                return Err(Error::DatabaseTapeError(
                    "unexpected column type for pragma_table_info query".to_string(),
                ));
            };
            let column_id = usize::try_from(*column_id).map_err(|_| {
                Error::DatabaseTapeError(format!(
                    "negative column index returned for table '{table_name}'"
                ))
            })?;
            if column_id != column_names.len() {
                return Err(Error::DatabaseTapeError(format!(
                    "non-contiguous column index {column_id} returned for table '{table_name}'"
                )));
            }
            if *pk > 0 {
                let pk_ordinal = usize::try_from(*pk).map_err(|_| {
                    Error::DatabaseTapeError(format!(
                        "invalid primary key ordinal returned for table '{table_name}'"
                    ))
                })?;
                pk_columns.push((pk_ordinal, column_id));
            }
            column_names.push(name.as_str().to_string());
            column_types.push(column_type.as_str().to_string());
        }
        pk_columns.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        let pk_column_indices = pk_columns
            .into_iter()
            .map(|(_, column_id)| column_id)
            .collect::<Vec<_>>();
        let rowid_alias_pk_column_index = if pk_column_indices.len() == 1 {
            let pk = pk_column_indices[0];
            column_types
                .get(pk)
                .is_some_and(|column_type| column_type.eq_ignore_ascii_case("INTEGER"))
                .then_some(pk)
        } else {
            None
        };
        Ok((column_names, pk_column_indices, rowid_alias_pk_column_index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_tape::DatabaseReplaySessionOpts;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use turso_core::SqliteDialect;

    enum QueryKind {
        Delete { use_rowid: bool },
        Update(Vec<bool>),
        Upsert(usize),
    }

    fn replay_info_for(
        ddl: &[&str],
        table: &str,
        kind: QueryKind,
        use_implicit_rowid: bool,
    ) -> Result<ReplayInfo> {
        let temp_file = NamedTempFile::new().unwrap();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = turso_core::Database::open_file(
            io.clone(),
            temp_file.path().to_str().unwrap(),
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        for stmt in ddl {
            conn.execute(stmt).unwrap();
        }
        let generator =
            DatabaseReplayGenerator::new(conn, DatabaseReplaySessionOpts { use_implicit_rowid });
        let table = table.to_string();
        let mut gen = genawaiter::sync::Gen::new(|coro| async move {
            let coro: Coro<()> = coro.into();
            match kind {
                QueryKind::Delete { use_rowid } => {
                    generator.delete_query(&coro, &table, use_rowid).await
                }
                QueryKind::Update(columns) => generator.update_query(&coro, &table, &columns).await,
                QueryKind::Upsert(columns) => generator.upsert_query(&coro, &table, columns).await,
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result,
            }
        }
    }

    #[test]
    fn test_identity_predicates_are_null_safe() {
        // Rowid tables allow NULL in PRIMARY KEY columns, and `NULL = NULL` is
        // not true: `=` predicates make replay of such a row a silent no-op.
        let ddl = &["CREATE TABLE t (a, b, c, PRIMARY KEY (a, c))"];
        let delete =
            replay_info_for(ddl, "t", QueryKind::Delete { use_rowid: false }, false).unwrap();
        assert_eq!(
            delete.query,
            r#"DELETE FROM "t" WHERE "a" IS ? AND "c" IS ?"#
        );

        let update =
            replay_info_for(ddl, "t", QueryKind::Update(vec![false, true, false]), false).unwrap();
        assert_eq!(
            update.query,
            r#"UPDATE "t" SET "b" = ? WHERE "a" IS ? AND "c" IS ?"#
        );
    }

    #[test]
    fn test_identity_predicates_follow_primary_key_ordinal_order() {
        // `PRIMARY KEY (c, a)`: predicates and binds are both ordered by PK
        // ordinal, not by column declaration order.
        let ddl = &["CREATE TABLE t (a, b, c, PRIMARY KEY (c, a))"];
        let update =
            replay_info_for(ddl, "t", QueryKind::Update(vec![false, true, false]), false).unwrap();
        assert_eq!(
            update.query,
            r#"UPDATE "t" SET "b" = ? WHERE "c" IS ? AND "a" IS ?"#
        );
        assert_eq!(update.pk_column_indices, Some(vec![2, 0]));
    }

    #[test]
    fn test_rowid_replay_avoids_user_columns_shadowing_rowid_aliases() {
        // A user column named `rowid` hijacks `WHERE rowid = ?`: the predicate
        // resolves to the user column, so the integer row-id bind matches
        // nothing and the replayed delete is a silent no-op.
        let shadow_rowid = &["CREATE TABLE t (rowid, v)"];
        let delete = replay_info_for(
            shadow_rowid,
            "t",
            QueryKind::Delete { use_rowid: true },
            true,
        )
        .unwrap();
        assert_eq!(delete.query, r#"DELETE FROM "t" WHERE "_rowid_" = ?"#);
        let update = replay_info_for(
            shadow_rowid,
            "t",
            QueryKind::Update(vec![false, true]),
            true,
        )
        .unwrap();
        assert_eq!(
            update.query,
            r#"UPDATE "t" SET "v" = ? WHERE "_rowid_" = ?"#
        );
        let upsert = replay_info_for(shadow_rowid, "t", QueryKind::Upsert(2), true).unwrap();
        assert_eq!(
            upsert.query,
            r#"INSERT OR REPLACE INTO "t"("rowid", "v", "_rowid_") VALUES (?,?,?)"#
        );

        let shadow_two = &["CREATE TABLE t (rowid, _rowid_, v)"];
        let delete =
            replay_info_for(shadow_two, "t", QueryKind::Delete { use_rowid: true }, true).unwrap();
        assert_eq!(delete.query, r#"DELETE FROM "t" WHERE "oid" = ?"#);

        // Shadowing all three aliases is legal SQLite, but then the implicit
        // rowid is unreachable from SQL: refuse instead of writing the wrong row.
        let shadow_all = &["CREATE TABLE t (rowid, _rowid_, oid, v)"];
        let err = replay_info_for(shadow_all, "t", QueryKind::Delete { use_rowid: true }, true)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("shadowing every implicit rowid alias"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_update_identity_values_come_from_before_image() {
        // A key-changing UPDATE must be identified by the key it had *before*
        // the change: binding the after image matches zero rows, silently.
        let ddl = &["CREATE TABLE t (a, b, c, PRIMARY KEY (c, a))"];
        let info =
            replay_info_for(ddl, "t", QueryKind::Update(vec![false, false, true]), false).unwrap();
        assert_eq!(
            info.query,
            r#"UPDATE "t" SET "c" = ? WHERE "c" IS ? AND "a" IS ?"#
        );

        let temp_file = NamedTempFile::new().unwrap();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = turso_core::Database::open_file(
            io,
            temp_file.path().to_str().unwrap(),
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let generator = DatabaseReplayGenerator::new(
            db.connect().unwrap(),
            DatabaseReplaySessionOpts {
                use_implicit_rowid: false,
            },
        );

        let text = |value: &str| turso_core::Value::from_text(value.to_string());
        // changes mask: only column `c` was updated; second half holds new values.
        let updates = turso_core::alloc::vec![
            turso_core::Value::from_i64(0),
            turso_core::Value::from_i64(0),
            turso_core::Value::from_i64(1),
            text("a1"),
            text("b1"),
            text("c9"),
        ];
        let before = turso_core::alloc::vec![text("a1"), text("b1"), text("c1")];
        let after = turso_core::alloc::vec![text("a1"), text("b1"), text("c9")];

        let values = generator.replay_values(
            &info,
            DatabaseChangeType::Update,
            0,
            after,
            Some(updates),
            Some(before),
        );

        // SET c = 'c9', then identity in PK ordinal order: c (before), a.
        assert_eq!(values.to_vec(), vec![text("c9"), text("c1"), text("a1")]);
    }

    #[test]
    fn test_upsert_predelete_only_for_null_key_components() {
        let ddl = &["CREATE TABLE t (a, b, c, PRIMARY KEY (a, c))"];
        let info = replay_info_for(ddl, "t", QueryKind::Upsert(3), false).unwrap();
        let temp_file = NamedTempFile::new().unwrap();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = turso_core::Database::open_file(
            io,
            temp_file.path().to_str().unwrap(),
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let generator = DatabaseReplayGenerator::new(
            db.connect().unwrap(),
            DatabaseReplaySessionOpts {
                use_implicit_rowid: false,
            },
        );
        let text = |value: &str| turso_core::Value::from_text(value.to_string());

        assert!(!generator
            .upsert_needs_null_safe_predelete(&info, &[text("a1"), text("b1"), text("c1")]));
        // NULL in a key column: ON CONFLICT cannot fire, so the row must be
        // pre-deleted or the upsert duplicates it.
        assert!(generator.upsert_needs_null_safe_predelete(
            &info,
            &[text("a1"), text("b1"), turso_core::Value::Null]
        ));
        // NULL in a non-key column is irrelevant.
        assert!(!generator.upsert_needs_null_safe_predelete(
            &info,
            &[text("a1"), turso_core::Value::Null, text("c1")]
        ));

        // A rowid-alias primary key is the implicit rowid: never NULL.
        let rowid_alias = replay_info_for(
            &["CREATE TABLE r (id INTEGER PRIMARY KEY, v)"],
            "r",
            QueryKind::Upsert(2),
            false,
        )
        .unwrap();
        assert!(!generator
            .upsert_needs_null_safe_predelete(&rowid_alias, &[turso_core::Value::Null, text("v")]));
    }
}
