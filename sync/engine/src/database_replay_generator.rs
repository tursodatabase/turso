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

/// SQL and ancillary info required to replay a change type (insert, update, delete) on a given table.
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
        assert!(updates.len() % 2 == 0);
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
                assert!(updates.len() % 2 == 0);
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
                    let entity_type = match entity_type.as_str() {
                        entity_type @ ("table" | "index" | "view" | "trigger") => entity_type,
                        other => {
                            return Err(Error::DatabaseTapeError(format!(
                                "unexpected 'type' column of sqlite_schema table: {other:?}"
                            )));
                        }
                    };
                    let query = format!("DROP {entity_type} {}", quote_ident(entity_name.as_str()));
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
                    assert_eq!(after.len(), 5);
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
                    assert_eq!(updates.len(), 10);
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
            self.writable_columns(coro, table_name).await?;
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
            self.writable_columns(coro, table_name).await?;
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
            self.writable_columns(coro, table_name).await?;
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

    pub async fn rename_and_drop_local_columns_to_match_remote<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        create_table: &str,
    ) -> Result<()> {
        let mut parser = Parser::new(create_table.as_bytes());
        let Some(Ok(turso_core::ast::Cmd::Stmt(turso_core::ast::Stmt::CreateTable {
            tbl_name,
            body: turso_core::ast::CreateTableBody::ColumnsAndConstraints { columns, .. },
            ..
        }))) = parser.next()
        else {
            return Ok(());
        };
        let table_name = tbl_name.name.as_str();
        let Some(mut local_columns) = self.local_column_definitions(coro, table_name).await? else {
            return Ok(());
        };
        let (renamed_columns, dropped_columns) =
            renamed_and_dropped_columns(&local_columns, &columns);
        let dropped_columns = dropped_columns
            .into_iter()
            .map(|name| name.as_str().to_string())
            .collect::<Vec<_>>();
        let renamed_any_column = !renamed_columns.is_empty();
        for (old_name, new_name) in renamed_columns {
            let rename_column =
                format!("ALTER TABLE {tbl_name} RENAME COLUMN {old_name} TO {new_name}");
            self.execute_ddl(&rename_column)?;
        }
        if dropped_columns.is_empty() {
            return Ok(());
        }
        if renamed_any_column {
            // RENAME COLUMN also changes the expressions of the generated
            // columns that use the renamed column.
            local_columns = self
                .local_column_definitions(coro, table_name)
                .await?
                .expect("RENAME COLUMN keeps the table");
        }
        self.drop_columns_and_their_users(
            &tbl_name,
            &mut local_columns,
            dropped_columns,
            &columns,
        )?;
        Ok(())
    }

    /// Changes the columns of the local table to the columns of the remote
    /// table, `create_table`, after the renames and drops of
    /// `rename_and_drop_local_columns_to_match_remote`. It adds the missing
    /// columns, and it changes each generated column that differs from the
    /// remote one. A pull matches the values of a row with the columns by
    /// position, and a later refresh matches the columns by position, so a
    /// column must keep its place. So a changed generated column changes in
    /// place, with ALTER COLUMN, unless it is at the end of the table with the
    /// generated columns that use it: those are dropped and added again. Core
    /// also refuses to drop a column that an index or a foreign key uses. If
    /// core refuses a change in place, this fails and does not move the column.
    /// Returns false if there is no local table.
    pub async fn change_columns_to_match_remote<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        create_table: &str,
    ) -> Result<bool> {
        let mut parser = Parser::new(create_table.as_bytes());
        let Some(Ok(turso_core::ast::Cmd::Stmt(turso_core::ast::Stmt::CreateTable {
            tbl_name,
            body:
                turso_core::ast::CreateTableBody::ColumnsAndConstraints {
                    columns,
                    constraints,
                    ..
                },
            ..
        }))) = parser.next()
        else {
            return Ok(false);
        };
        let table_name = tbl_name.name.as_str();
        let Some((mut local_columns, local_constraints)) =
            self.local_table_definition(coro, table_name).await?
        else {
            return Ok(false);
        };
        let changed_columns = changed_generated_columns(
            &tbl_name,
            (&local_columns, &local_constraints),
            (&columns, &constraints),
        )
        .into_iter()
        .filter(|name| has_column(&columns, name))
        .collect::<Vec<_>>();
        let indexes = if changed_columns.is_empty() {
            Vec::new()
        } else {
            self.local_indexes(coro, table_name).await?
        };
        let undroppable_columns = local_columns
            .iter()
            .map(|column| column.col_name.as_str())
            .filter(|name| {
                has_foreign_key(name, &local_columns, &local_constraints)
                    || indexes
                        .iter()
                        .any(|(_, _, read_columns)| contains_name(read_columns, name))
            })
            .map(str::to_string)
            .collect::<Vec<_>>();
        let altered_columns = changed_columns_to_alter_in_place(
            &local_columns,
            &changed_columns,
            &columns,
            &undroppable_columns,
        );
        // Core decides by itself whether ALTER COLUMN builds an index again, and
        // it keeps an index as it is after a change of COLLATE. So each index
        // that reads an altered column, directly or through a generated column,
        // is dropped first and created again at the end.
        let mut changed_values = generated_columns_that_use(&local_columns, &altered_columns);
        changed_values.extend(altered_columns.iter().cloned());
        let indexes = indexes
            .into_iter()
            .filter(|(_, _, read_columns)| {
                read_columns
                    .iter()
                    .any(|name| contains_name(&changed_values, name))
            })
            .collect::<Vec<_>>();
        for (index_name, _, _) in &indexes {
            self.execute_ddl(&format!("DROP INDEX {}", quote_ident(index_name)))?;
        }
        // The other changed columns are at the end of the table, so dropping
        // and adding them again keeps their place. A new definition can read a
        // column that the remote added, so the missing columns are added before
        // the changes.
        let dropped_columns = changed_columns
            .into_iter()
            .filter(|name| !contains_name(&altered_columns, name))
            .collect();
        self.drop_columns_and_their_users(
            &tbl_name,
            &mut local_columns,
            dropped_columns,
            &columns,
        )?;
        self.add_missing_columns(&tbl_name, &local_columns, &columns)?;
        if altered_columns.is_empty() {
            return Ok(true);
        }
        // DROP COLUMN and ADD COLUMN write the table text again, with each
        // foreign key as a table constraint.
        let (local_columns, local_constraints) = self
            .local_table_definition(coro, table_name)
            .await?
            .expect("ALTER TABLE keeps the table");
        let mut pending_columns = altered_columns;
        while !pending_columns.is_empty() {
            let name = pending_columns.remove(next_column_to_alter(&pending_columns, &columns));
            let definition = definition_for_alter_column(
                &tbl_name,
                &name,
                (&local_columns, &local_constraints),
                (&columns, &constraints),
            )?;
            let alter_column = format!(
                "ALTER TABLE {tbl_name} ALTER COLUMN {} TO {definition}",
                definition.col_name
            );
            self.execute_ddl(&alter_column).map_err(|error| {
                Error::DatabaseTapeError(format!(
                    "the pull cannot change the generated column {name} of table {tbl_name} to its definition on the server: {error}"
                ))
            })?;
        }
        for (_, index_sql, _) in &indexes {
            self.execute_ddl(index_sql)?;
        }
        Ok(true)
    }

    /// Execute a DDL statement idempotently: CREATE TABLE for an existing table
    /// drops the generated columns that it removes or changes and adds the
    /// columns that are missing, named schema objects are skipped when already
    /// present, and `ALTER TABLE ADD COLUMN` only adds missing columns. Falls
    /// back to direct execution for other DDL.
    pub async fn execute_ddl_idempotent<Ctx>(&self, coro: &Coro<Ctx>, ddl: &str) -> Result<()> {
        let mut parser = Parser::new(ddl.as_bytes());
        let Some(Ok(turso_core::ast::Cmd::Stmt(mut stmt))) = parser.next() else {
            self.execute_ddl(ddl)?;
            return Ok(());
        };
        match &mut stmt {
            turso_core::ast::Stmt::CreateTable {
                if_not_exists,
                tbl_name,
                body,
                ..
            } => {
                *if_not_exists = true;
                let table_name = tbl_name.name.as_str();
                let Some((mut local_columns, local_constraints)) =
                    self.local_table_definition(coro, table_name).await?
                else {
                    // the local table didn't exist
                    self.execute_ddl(ddl)?;
                    return Ok(());
                };
                if let turso_core::ast::CreateTableBody::ColumnsAndConstraints {
                    columns: remote_columns,
                    constraints: remote_constraints,
                    ..
                } = body
                {
                    // drop the generated columns that were changed
                    let changed_columns = changed_generated_columns(
                        tbl_name,
                        (&local_columns, &local_constraints),
                        (remote_columns, remote_constraints),
                    );
                    self.drop_columns_and_their_users(
                        tbl_name,
                        &mut local_columns,
                        changed_columns,
                        remote_columns,
                    )?;
                    self.add_missing_columns(tbl_name, &local_columns, remote_columns)?;
                }
                Ok(())
            }
            turso_core::ast::Stmt::CreateIndex { idx_name, .. } => {
                if self
                    .schema_object_exists(coro, "index", idx_name.name.as_str())
                    .await?
                {
                    return Ok(());
                }
                self.execute_ddl(ddl)?;
                Ok(())
            }
            turso_core::ast::Stmt::CreateTrigger { trigger_name, .. } => {
                if self
                    .schema_object_exists(coro, "trigger", trigger_name.name.as_str())
                    .await?
                {
                    return Ok(());
                }
                self.execute_ddl(ddl)?;
                Ok(())
            }
            turso_core::ast::Stmt::CreateMaterializedView { view_name, .. }
            | turso_core::ast::Stmt::CreateView { view_name, .. } => {
                if self
                    .schema_object_exists(coro, "view", view_name.name.as_str())
                    .await?
                {
                    return Ok(());
                }
                self.execute_ddl(ddl)?;
                Ok(())
            }
            turso_core::ast::Stmt::AlterTable(turso_core::ast::AlterTable {
                name: tbl_name,
                body: turso_core::ast::AlterTableBody::AddColumn(col_def),
            }) => {
                let table_name = tbl_name.name.as_str();
                let local_columns = self
                    .local_column_definitions(coro, table_name)
                    .await?
                    .unwrap_or_default();
                let col_name = col_def.col_name.as_str();

                if has_column(&local_columns, col_name) {
                    tracing::debug!(
                        "execute_ddl_idempotent: column {col_name} already exists in {table_name}, skipping"
                    );
                } else {
                    self.execute_ddl(ddl)?;
                }
                Ok(())
            }
            _ => {
                self.conn.execute(ddl)?;
                Ok(())
            }
        }
    }

    /// Drops the columns `names` of the table `tbl_name`, and removes them from
    /// `local_columns`, the current columns of the table. Core refuses to drop
    /// a column that a generated column uses. So this also drops each
    /// generated column that uses a column of `names`, directly or through
    /// other generated columns, and it drops a column only when no remaining
    /// column uses it. The caller then adds those generated columns again
    /// from `remote_columns`.
    fn drop_columns_and_their_users(
        &self,
        tbl_name: &turso_core::ast::QualifiedName,
        local_columns: &mut Vec<turso_core::ast::ColumnDefinition>,
        mut names: Vec<String>,
        remote_columns: &[turso_core::ast::ColumnDefinition],
    ) -> Result<()> {
        if names.is_empty() {
            return Ok(());
        }
        let users = generated_columns_that_use(local_columns, &names);
        if !users.is_empty() {
            // The users are added again from `remote_columns`. A generated
            // column gets its values again, but a regular column stays empty.
            // A user that the remote table has as a regular column comes from
            // a wrong match of the columns, for example after the remote
            // renamed columns in a chain.
            let regular_users = users
                .iter()
                .filter(|user| {
                    !remote_columns.iter().any(|column| {
                        column.col_name.as_str().eq_ignore_ascii_case(user) && is_generated(column)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            if !regular_users.is_empty() {
                return Err(Error::DatabaseTapeError(format!(
                    "the generated columns {} of table {tbl_name} use a dropped column, but the remote table does not have them as generated columns",
                    regular_users.join(", ")
                )));
            }
            names.extend(users.iter().cloned());
            // The users are added again at the end of the table. A pull
            // matches the values of a row with the columns by position, so the
            // columns that stay must be the first columns of the remote table.
            // A column that stays and is generated on one side only also comes
            // from a wrong match of the columns. So does a regular column that
            // stays with another type than the remote column at its place, for
            // example after the remote dropped a column and gave its name to
            // another column.
            let Some(staying_columns) =
                staying_columns_with_remote(local_columns, &names, remote_columns)
            else {
                return Err(Error::DatabaseTapeError(format!(
                    "the generated columns {} of table {tbl_name} use a dropped column, but they are not the last columns of the remote table",
                    users.join(", ")
                )));
            };
            if let Some((column, _)) = staying_columns
                .iter()
                .find(|(column, remote_column)| is_generated(column) != is_generated(remote_column))
            {
                return Err(Error::DatabaseTapeError(format!(
                    "the generated columns {} of table {tbl_name} use a dropped column, but the column {} is generated on one side only",
                    users.join(", "),
                    column.col_name
                )));
            }
            if let Some((column, _)) = staying_columns.iter().find(|(column, remote_column)| {
                !is_generated(column)
                    && !is_generated(remote_column)
                    && !same_type(column, remote_column)
            }) {
                return Err(Error::DatabaseTapeError(format!(
                    "the generated columns {} of table {tbl_name} use a dropped column, but the column {} has another type in the remote table",
                    users.join(", "),
                    column.col_name
                )));
            }
        }
        while let Some(index) = next_column_to_drop(local_columns, &names) {
            let dropped = local_columns.remove(index);
            let drop_column = format!("ALTER TABLE {tbl_name} DROP COLUMN {}", dropped.col_name);
            self.execute_ddl(&drop_column)?;
        }
        Ok(())
    }

    /// Adds the columns of `remote_columns` that `local_columns`, the columns
    /// of the table `tbl_name`, do not have, in the order of `remote_columns`.
    fn add_missing_columns(
        &self,
        tbl_name: &turso_core::ast::QualifiedName,
        local_columns: &[turso_core::ast::ColumnDefinition],
        remote_columns: &[turso_core::ast::ColumnDefinition],
    ) -> Result<()> {
        for column in remote_columns {
            if has_column(local_columns, column.col_name.as_str()) {
                continue;
            }
            let add_column = format!("ALTER TABLE {tbl_name} ADD COLUMN {column}");
            self.execute_ddl(&add_column)?;
        }
        Ok(())
    }

    /// Returns the name, the SQL text and the read columns of each index of the
    /// table `table_name` that has an SQL text. The read columns are the ones
    /// that its key or its WHERE clause reads. The indexes of UNIQUE and
    /// PRIMARY KEY constraints have no SQL text.
    async fn local_indexes<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<Vec<(String, String, Vec<String>)>> {
        let table_name_literal = sql_string_literal(table_name);
        let mut stmt = self.conn.prepare(format!(
            "SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = {table_name_literal} COLLATE NOCASE AND sql IS NOT NULL"
        ))?;
        let mut indexes = Vec::new();
        while let Some(row) = run_stmt_once(coro, &mut stmt).await? {
            let (turso_core::Value::Text(name), turso_core::Value::Text(sql)) =
                (row.get_value(0), row.get_value(1))
            else {
                return Err(Error::DatabaseTapeError(format!(
                    "unexpected index row for table '{table_name}' in sqlite_schema"
                )));
            };
            let mut parser = Parser::new(sql.as_str().as_bytes());
            let Some(Ok(turso_core::ast::Cmd::Stmt(turso_core::ast::Stmt::CreateIndex {
                columns,
                where_clause,
                ..
            }))) = parser.next()
            else {
                return Err(Error::DatabaseTapeError(format!(
                    "unexpected definition of an index of table '{table_name}' in sqlite_schema: {}",
                    sql.as_str()
                )));
            };
            let read_columns = columns
                .iter()
                .map(|column| column.expr.as_ref())
                .chain(where_clause.as_deref())
                .flat_map(|expr| turso_core::schema::collect_column_dependencies_of_expr(expr, &[]))
                .collect();
            indexes.push((
                name.as_str().to_string(),
                sql.as_str().to_string(),
                read_columns,
            ));
        }
        Ok(indexes)
    }

    /// Returns `None` if there is no matching local table
    async fn local_column_definitions<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<Option<Vec<turso_core::ast::ColumnDefinition>>> {
        Ok(self
            .local_table_definition(coro, table_name)
            .await?
            .map(|(columns, _)| columns))
    }

    /// Returns the columns and the table constraints of the local table
    /// `table_name`, or `None` if there is no such table.
    async fn local_table_definition<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<
        Option<(
            Vec<turso_core::ast::ColumnDefinition>,
            Vec<turso_core::ast::NamedTableConstraint>,
        )>,
    > {
        let table_name_literal = sql_string_literal(table_name);
        let mut stmt = self.conn.prepare(format!(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = {table_name_literal} COLLATE NOCASE"
        ))?;
        let Some(row) = run_stmt_once(coro, &mut stmt).await? else {
            return Ok(None);
        };
        let turso_core::Value::Text(sql) = row.get_value(0) else {
            return Err(Error::DatabaseTapeError(format!(
                "unexpected sql column type for table '{table_name}' in sqlite_schema"
            )));
        };
        let sql = sql.as_str().to_string();
        let mut parser = Parser::new(sql.as_bytes());
        let Some(Ok(turso_core::ast::Cmd::Stmt(turso_core::ast::Stmt::CreateTable {
            body:
                turso_core::ast::CreateTableBody::ColumnsAndConstraints {
                    columns,
                    constraints,
                    ..
                },
            ..
        }))) = parser.next()
        else {
            return Err(Error::DatabaseTapeError(format!(
                "unexpected definition of table '{table_name}' in sqlite_schema: {sql}"
            )));
        };
        Ok(Some((columns, constraints)))
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

    /// Returns which columns can be INSERTed or UPDATEd, skipping generated columns.
    /// Returns `(Vec<col_name>, Vec<pk_column_indices>, Option<rowid_alias_pk_column_index>)`
    async fn writable_columns<Ctx>(
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

/// Matches the columns of the local table with the columns of the remote table
/// by position. Returns the local columns to rename, each with its remote name,
/// and the local columns to drop.
fn renamed_and_dropped_columns<'a>(
    local_columns: &'a [turso_core::ast::ColumnDefinition],
    remote_columns: &'a [turso_core::ast::ColumnDefinition],
) -> (
    Vec<(&'a turso_core::ast::Name, &'a turso_core::ast::Name)>,
    Vec<&'a turso_core::ast::Name>,
) {
    let mut renamed = Vec::new();
    let mut dropped = Vec::new();
    let mut remote_columns = remote_columns.iter().peekable();
    for (index, local_column) in local_columns.iter().enumerate() {
        let local_name = &local_column.col_name;
        let Some(remote_column) = remote_columns.peek() else {
            dropped.push(local_name);
            continue;
        };
        let remote_name = &remote_column.col_name;
        if local_name
            .as_str()
            .eq_ignore_ascii_case(remote_name.as_str())
        {
            // The remote can change only the case of a name.
            if local_name.as_str() != remote_name.as_str() {
                renamed.push((local_name, remote_name));
            }
            remote_columns.next();
        } else if has_column(&local_columns[index + 1..], remote_name.as_str()) {
            // This doesn't correctly handle a remote transaction that swaps two columns,
            // https://github.com/tursodatabase/turso/issues/9430 . But for now, this is the
            // best we can do. Note that this change didn't introduce the bug.
            dropped.push(local_name);
        } else if local_columns[index..].iter().all(is_generated)
            && dropped
                .iter()
                .any(|name| name.as_str().eq_ignore_ascii_case(remote_name.as_str()))
        {
            // The remote dropped the column `remote_name` and added it again at
            // the end, so it added the remote columns from here on after that
            // column. A rename to that name would fail, because the local column
            // that is dropped still has it. This column and the later local
            // columns are generated and keep no values, so they are dropped too
            // and added again from the remote table. When one of them is regular,
            // the column goes to the rename, as before: the remote can have
            // renamed it to the dropped name. A drop here would make the match
            // give that name to the next regular column, with its values.
            dropped.push(local_name);
        } else {
            renamed.push((local_name, remote_name));
            remote_columns.next();
        }
    }
    (renamed, dropped)
}

/// Returns the names of the generated columns of the local table, `local`,
/// that the remote table, `remote`, removed or changed. Each table is its
/// columns and its table constraints. A generated column is changed when its
/// text differs from the text of the remote column. But ADD COLUMN and DROP
/// COLUMN write the table text again in the form of core, which also writes a
/// foreign key as a table constraint. So a column is not changed when its text
/// and its foreign keys are the same in that form. If core cannot make a table
/// of them, only the texts are compared.
fn changed_generated_columns(
    tbl_name: &turso_core::ast::QualifiedName,
    local: (
        &[turso_core::ast::ColumnDefinition],
        &[turso_core::ast::NamedTableConstraint],
    ),
    remote: (
        &[turso_core::ast::ColumnDefinition],
        &[turso_core::ast::NamedTableConstraint],
    ),
) -> Vec<String> {
    let (local_columns, local_constraints) = local;
    let (remote_columns, remote_constraints) = remote;
    let columns_with_changed_text = local_columns
        .iter()
        .filter(|column| generated_column_changed_between_local_and_remote(column, remote_columns))
        .collect::<Vec<_>>();
    if columns_with_changed_text.is_empty() {
        return Vec::new();
    }
    let (
        Some((local_written_columns, local_written_constraints)),
        Some((remote_written_columns, remote_written_constraints)),
    ) = (
        table_as_core_writes_it(tbl_name, local_columns, local_constraints),
        table_as_core_writes_it(tbl_name, remote_columns, remote_constraints),
    )
    else {
        return columns_with_changed_text
            .iter()
            .map(|column| column.col_name.as_str().to_string())
            .collect();
    };
    columns_with_changed_text
        .iter()
        .filter(|column| {
            let name = column.col_name.as_str();
            let local_written_column = local_written_columns
                .iter()
                .find(|written_column| written_column.col_name.as_str().eq_ignore_ascii_case(name))
                .expect("core writes each column of the table");
            generated_column_changed_between_local_and_remote(
                local_written_column,
                &remote_written_columns,
            ) || foreign_keys_of(name, &local_written_constraints)
                != foreign_keys_of(name, &remote_written_constraints)
        })
        .map(|column| column.col_name.as_str().to_string())
        .collect()
}

/// Returns the columns of `changed`, generated columns of `columns` that differ
/// from the remote table, that must change in place. Dropping columns and
/// adding them again keeps their place only when they are the last columns,
/// with the generated columns that use them. Core also refuses to drop the
/// columns of `undroppable`, which an index or a foreign key uses. ADD COLUMN
/// cannot add a column whose new definition in `remote_columns` reads a later
/// column. The other changed columns change in place.
fn changed_columns_to_alter_in_place(
    columns: &[turso_core::ast::ColumnDefinition],
    changed: &[String],
    remote_columns: &[turso_core::ast::ColumnDefinition],
    undroppable: &[String],
) -> Vec<String> {
    let remote_position = |name: &str| {
        remote_columns
            .iter()
            .position(|column| column.col_name.as_str().eq_ignore_ascii_case(name))
    };
    let reads_later_column = |name: &str| {
        remote_position(name).is_some_and(|position| {
            columns_read_by(&remote_columns[position])
                .iter()
                .any(|read| remote_position(read).is_some_and(|read| read > position))
        })
    };
    // An end that can be dropped and added again holds only changed columns and
    // the generated columns that use them, so it starts in the longest such end.
    let all_users = generated_columns_that_use(columns, changed);
    let first_start = columns
        .iter()
        .rposition(|column| {
            let name = column.col_name.as_str();
            !contains_name(changed, name) && !contains_name(&all_users, name)
        })
        .map_or(0, |position| position + 1);
    let start = (first_start..=columns.len())
        .find(|&start| {
            let end = &columns[start..];
            let changed_at_end = changed
                .iter()
                .filter(|name| has_column(end, name))
                .cloned()
                .collect::<Vec<_>>();
            let users = generated_columns_that_use(columns, &changed_at_end);
            users.iter().all(|user| has_column(end, user))
                && end.iter().all(|column| {
                    let name = column.col_name.as_str();
                    (contains_name(&changed_at_end, name) || contains_name(&users, name))
                        && !reads_later_column(name)
                        && !contains_name(undroppable, name)
                })
        })
        .expect("an empty end is always valid");
    changed
        .iter()
        .filter(|name| !has_column(&columns[start..], name))
        .cloned()
        .collect()
}

/// Returns the index in `names` of the next column to change in place: the
/// first one whose new definition in `remote_columns` reads no other column of
/// `names`. If each of them reads another one, it returns the first one, and
/// core then refuses the change with an error.
fn next_column_to_alter(
    names: &[String],
    remote_columns: &[turso_core::ast::ColumnDefinition],
) -> usize {
    names
        .iter()
        .position(|name| {
            let read_columns = remote_columns
                .iter()
                .find(|column| column.col_name.as_str().eq_ignore_ascii_case(name))
                .map(columns_read_by)
                .unwrap_or_default();
            !names.iter().any(|other| {
                !other.eq_ignore_ascii_case(name) && contains_name(&read_columns, other)
            })
        })
        .unwrap_or(0)
}

/// Returns the definition that ALTER COLUMN gets to change the column `name` of
/// the local table, `local`, to the column of the remote table, `remote`. Each
/// table is its columns and its table constraints. ALTER COLUMN replaces the
/// foreign keys written in the definition of the column, and keeps the ones
/// that are table constraints. So the definition holds each foreign key of the
/// remote column that the local table does not have as a table constraint.
/// This fails when the local table has a foreign key of the column as a table
/// constraint that the remote column does not have, or when the definition
/// must hold a foreign key on more than one column. If core cannot make a
/// table of the columns, this returns the remote column as it is.
fn definition_for_alter_column(
    tbl_name: &turso_core::ast::QualifiedName,
    name: &str,
    local: (
        &[turso_core::ast::ColumnDefinition],
        &[turso_core::ast::NamedTableConstraint],
    ),
    remote: (
        &[turso_core::ast::ColumnDefinition],
        &[turso_core::ast::NamedTableConstraint],
    ),
) -> Result<turso_core::ast::ColumnDefinition> {
    let (local_columns, local_constraints) = local;
    let (remote_columns, remote_constraints) = remote;
    let remote_column = remote_columns
        .iter()
        .find(|column| column.col_name.as_str().eq_ignore_ascii_case(name))
        .expect("the remote table has each altered column");
    let without_foreign_keys = |column: &turso_core::ast::ColumnDefinition| {
        let mut column = column.clone();
        column.constraints.retain(|constraint| {
            !matches!(
                constraint.constraint,
                turso_core::ast::ColumnConstraint::ForeignKey { .. }
            )
        });
        column
    };
    let local_columns = local_columns
        .iter()
        .map(without_foreign_keys)
        .collect::<Vec<_>>();
    let (Some((_, local_table_constraints)), Some((_, remote_written_constraints))) = (
        table_as_core_writes_it(tbl_name, &local_columns, local_constraints),
        table_as_core_writes_it(tbl_name, remote_columns, remote_constraints),
    ) else {
        return Ok(remote_column.clone());
    };
    let mut definition = without_foreign_keys(remote_column);
    let mut local_foreign_keys = foreign_keys_of(name, &local_table_constraints);
    for foreign_key in foreign_key_constraints_of(name, &remote_written_constraints) {
        let text = foreign_key.to_string();
        if let Some(position) = local_foreign_keys.iter().position(|local| *local == text) {
            local_foreign_keys.remove(position);
            continue;
        }
        let turso_core::ast::TableConstraint::ForeignKey {
            columns,
            clause,
            defer_clause,
        } = &foreign_key.constraint
        else {
            unreachable!("foreign_key_constraints_of returns only foreign keys");
        };
        if columns.len() != 1 {
            return Err(Error::DatabaseTapeError(format!(
                "the pull cannot change the generated column {name} of table {tbl_name} to its definition on the server: ALTER COLUMN cannot add the foreign key {text}, which uses more than one column"
            )));
        }
        definition
            .constraints
            .push(turso_core::ast::NamedColumnConstraint {
                name: foreign_key.name.clone(),
                constraint: turso_core::ast::ColumnConstraint::ForeignKey {
                    clause: clause.clone(),
                    defer_clause: defer_clause.clone(),
                },
            });
    }
    if !local_foreign_keys.is_empty() {
        return Err(Error::DatabaseTapeError(format!(
            "the pull cannot change the generated column {name} of table {tbl_name} to its definition on the server: ALTER COLUMN cannot remove the foreign keys {}, which are table constraints",
            local_foreign_keys.join(", ")
        )));
    }
    Ok(definition)
}

/// Returns whether a foreign key of the table with the columns `columns` and
/// the table constraints `constraints` uses the column `name`.
fn has_foreign_key(
    name: &str,
    columns: &[turso_core::ast::ColumnDefinition],
    constraints: &[turso_core::ast::NamedTableConstraint],
) -> bool {
    columns.iter().any(|column| {
        column.col_name.as_str().eq_ignore_ascii_case(name)
            && column.constraints.iter().any(|constraint| {
                matches!(
                    constraint.constraint,
                    turso_core::ast::ColumnConstraint::ForeignKey { .. }
                )
            })
    }) || !foreign_key_constraints_of(name, constraints).is_empty()
}

/// returns true if a local column is generated and the remote either removed it or modified it
fn generated_column_changed_between_local_and_remote(
    local_column: &turso_core::ast::ColumnDefinition,
    remote_columns: &[turso_core::ast::ColumnDefinition],
) -> bool {
    if !is_generated(local_column) {
        return false;
    }
    let local_name = local_column.col_name.as_str();
    let Some(remote_column) = remote_columns.iter().find(|remote_column| {
        remote_column
            .col_name
            .as_str()
            .eq_ignore_ascii_case(local_name)
    }) else {
        // remote has removed the gencol
        return true;
    };
    is_generated(remote_column) && remote_column.to_string() != local_column.to_string()
}

/// Returns the columns and the table constraints of the table `tbl_name` in
/// the form that core writes when ADD COLUMN or DROP COLUMN writes the table
/// text again, or `None` if core cannot make a table of them. That form
/// leaves out the VIRTUAL keyword, puts NOT NULL, UNIQUE and COLLATE before
/// AS, and writes each foreign key as a table constraint.
fn table_as_core_writes_it(
    tbl_name: &turso_core::ast::QualifiedName,
    columns: &[turso_core::ast::ColumnDefinition],
    constraints: &[turso_core::ast::NamedTableConstraint],
) -> Option<(
    Vec<turso_core::ast::ColumnDefinition>,
    Vec<turso_core::ast::NamedTableConstraint>,
)> {
    let body = turso_core::ast::CreateTableBody::ColumnsAndConstraints {
        columns: columns.to_vec(),
        constraints: constraints.to_vec(),
        options: turso_core::ast::TableOptions::empty(),
    };
    let table =
        turso_core::schema::BTreeTable::from_create_table_ast(tbl_name, &body, 0, false).ok()?;
    let sql = table.to_sql();
    let mut parser = Parser::new(sql.as_bytes());
    let Some(Ok(turso_core::ast::Cmd::Stmt(turso_core::ast::Stmt::CreateTable {
        body:
            turso_core::ast::CreateTableBody::ColumnsAndConstraints {
                columns,
                constraints,
                ..
            },
        ..
    }))) = parser.next()
    else {
        return None;
    };
    Some((columns, constraints))
}

/// Returns the sorted texts of the foreign keys of `constraints` that use the
/// column `name`.
fn foreign_keys_of(
    name: &str,
    constraints: &[turso_core::ast::NamedTableConstraint],
) -> Vec<String> {
    let mut foreign_keys = foreign_key_constraints_of(name, constraints)
        .into_iter()
        .map(|constraint| constraint.to_string())
        .collect::<Vec<_>>();
    foreign_keys.sort();
    foreign_keys
}

/// Returns the foreign keys of `constraints` that use the column `name`.
fn foreign_key_constraints_of<'a>(
    name: &str,
    constraints: &'a [turso_core::ast::NamedTableConstraint],
) -> Vec<&'a turso_core::ast::NamedTableConstraint> {
    constraints
        .iter()
        .filter(|constraint| {
            matches!(
                &constraint.constraint,
                turso_core::ast::TableConstraint::ForeignKey { columns, .. }
                    if columns.iter().any(|column| column.col_name.as_str().eq_ignore_ascii_case(name))
            )
        })
        .collect()
}

/// Returns whether `column` is a generated column.
fn is_generated(column: &turso_core::ast::ColumnDefinition) -> bool {
    column.constraints.iter().any(|constraint| {
        matches!(
            constraint.constraint,
            turso_core::ast::ColumnConstraint::Generated { .. }
        )
    })
}

/// Returns the names of the generated columns of `columns` that use a column
/// of `names`, directly or through other generated columns. The result holds
/// no name of `names`.
fn generated_columns_that_use(
    columns: &[turso_core::ast::ColumnDefinition],
    names: &[String],
) -> Vec<String> {
    let mut users = Vec::new();
    while let Some(user) = columns.iter().find(|column| {
        let name = column.col_name.as_str();
        !contains_name(names, name)
            && !contains_name(&users, name)
            && columns_read_by(column)
                .iter()
                .any(|read| contains_name(names, read) || contains_name(&users, read))
    }) {
        users.push(user.col_name.as_str().to_string());
    }
    users
}

/// Returns each column of `local_columns` that is not in `names`, with the
/// column at its place among the first columns of `remote_columns`. Returns
/// `None` if their names differ: then dropping the columns `names` and adding
/// them again at the end changes the column order of the remote table.
fn staying_columns_with_remote<'a>(
    local_columns: &'a [turso_core::ast::ColumnDefinition],
    names: &[String],
    remote_columns: &'a [turso_core::ast::ColumnDefinition],
) -> Option<
    Vec<(
        &'a turso_core::ast::ColumnDefinition,
        &'a turso_core::ast::ColumnDefinition,
    )>,
> {
    let mut remote_columns = remote_columns.iter();
    local_columns
        .iter()
        .filter(|column| !contains_name(names, column.col_name.as_str()))
        .map(|column| {
            let remote_column = remote_columns.next()?;
            remote_column
                .col_name
                .as_str()
                .eq_ignore_ascii_case(column.col_name.as_str())
                .then_some((column, remote_column))
        })
        .collect()
}

/// Returns whether the local column `local` has the declared type of the
/// remote column `remote`. It ignores ASCII case. It does not compare the
/// constraints, because core writes them in another form when it writes the
/// table text again after ALTER TABLE, for example a REFERENCES clause as a
/// table FOREIGN KEY.
fn same_type(
    local: &turso_core::ast::ColumnDefinition,
    remote: &turso_core::ast::ColumnDefinition,
) -> bool {
    match (&local.col_type, &remote.col_type) {
        (None, None) => true,
        (Some(local), Some(remote)) => local.to_string().eq_ignore_ascii_case(&remote.to_string()),
        _ => false,
    }
}

/// Returns the index in `columns` of the next column of `names` to drop: the
/// last one that no other column of `columns` uses. If each of them is in
/// use, it returns the last one, and core then refuses the drop with an error.
/// That needs a cycle of generated columns, which core refuses to create. The
/// schema comes from another database, so a pull reports an error for it and
/// does not stop the process with an assertion.
fn next_column_to_drop(
    columns: &[turso_core::ast::ColumnDefinition],
    names: &[String],
) -> Option<usize> {
    let candidates = (0..columns.len())
        .rev()
        .filter(|&index| contains_name(names, columns[index].col_name.as_str()))
        .collect::<Vec<_>>();
    let last = *candidates.first()?;
    let is_unused = |index: usize| {
        let name = columns[index].col_name.as_str();
        columns
            .iter()
            .enumerate()
            .all(|(other, column)| other == index || !contains_name(&columns_read_by(column), name))
    };
    Some(
        candidates
            .into_iter()
            .find(|&index| is_unused(index))
            .unwrap_or(last),
    )
}

/// Returns the names that the expression of the generated column `column`
/// uses, as core finds them, or no names if `column` is not generated.
fn columns_read_by(column: &turso_core::ast::ColumnDefinition) -> Vec<String> {
    column
        .constraints
        .iter()
        .filter_map(|constraint| match &constraint.constraint {
            turso_core::ast::ColumnConstraint::Generated { expr, .. } => Some(expr),
            _ => None,
        })
        .flat_map(|expr| turso_core::schema::collect_column_dependencies_of_expr(expr, &[]))
        .collect()
}

/// Returns whether `names` holds `name`. Like SQL, it ignores ASCII case.
fn contains_name(names: &[String], name: &str) -> bool {
    names.iter().any(|other| other.eq_ignore_ascii_case(name))
}

fn has_column(columns: &[turso_core::ast::ColumnDefinition], name: &str) -> bool {
    columns
        .iter()
        .any(|column| column.col_name.as_str().eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_tape::DatabaseReplaySessionOpts;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use turso_core::{ConnectOpts, SessionRole};
    use turso_sqlite::SqliteDialect;

    enum QueryKind {
        Delete { use_rowid: bool },
        Update(Vec<bool>),
        Upsert(usize),
        Change(DatabaseTapeRowChange),
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
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
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
                QueryKind::Change(change) => generator.replay_info(&coro, &change).await,
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result,
            }
        }
    }

    fn sqlite_schema_delete(entity_type: &str, name: &str) -> QueryKind {
        let text =
            |value: &str| turso_core::Value::Text(turso_core::types::Text::new(value.to_string()));
        QueryKind::Change(DatabaseTapeRowChange {
            change_id: 1,
            change_time: 0,
            change: DatabaseTapeRowChangeType::Delete {
                before: vec![
                    text(entity_type),
                    text(name),
                    text(name),
                    turso_core::Value::from_i64(2),
                    text(""),
                ],
                key: None,
            },
            table_name: SQLITE_SCHEMA_TABLE.to_string(),
            id: 1,
        })
    }

    #[test]
    fn test_sqlite_schema_delete_replays_quoted_drop() {
        for entity_type in ["table", "index", "view", "trigger"] {
            let kind = sqlite_schema_delete(entity_type, r#"my "odd" name"#);
            let drop = replay_info_for(&[], SQLITE_SCHEMA_TABLE, kind, false).unwrap();
            assert_eq!(
                drop.query,
                format!(r#"DROP {entity_type} "my ""odd"" name""#)
            );
        }
        let kind = sqlite_schema_delete("table; DROP TABLE x", "t");
        let error = replay_info_for(&[], SQLITE_SCHEMA_TABLE, kind, false).unwrap_err();
        assert!(
            matches!(&error, Error::DatabaseTapeError(message) if message.contains("'type' column")),
            "{error:?}"
        );
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
            db.connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap(),
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
            db.connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap(),
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

    /// The remote drops `k` and renames the generated column `g` to `k`. The
    /// match must rename `g`, and not drop `g` and give the name `k` to the
    /// regular column `z` after it.
    #[test]
    fn match_renames_generated_column_that_a_regular_column_follows() {
        let local = column_definitions(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, x TEXT, g TEXT AS (upper(x)), z TEXT)",
        );
        let remote = column_definitions(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT, k TEXT AS (upper(x)), z TEXT)",
        );
        let (renamed, dropped) = renamed_and_dropped_columns(&local, &remote);
        let renamed = renamed
            .iter()
            .map(|(old_name, new_name)| (old_name.as_str(), new_name.as_str()))
            .collect::<Vec<_>>();
        let dropped = dropped.iter().map(|name| name.as_str()).collect::<Vec<_>>();
        assert_eq!(renamed, vec![("g", "k")]);
        assert_eq!(dropped, vec!["k"]);
    }

    /// The remote renames `x` to `y`, and then the generated column `l`, the
    /// last column, to `x`. Only generated columns follow `l`, so the rule for
    /// a name that the remote added again can apply to it. The match must still
    /// rename `l`, because it drops no column with the name `x`.
    #[test]
    fn match_renames_trailing_generated_column_to_name_of_renamed_column() {
        let local = column_definitions(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT, l TEXT AS (upper(x)))",
        );
        let remote = column_definitions(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, y TEXT, x TEXT AS (upper(y)))",
        );
        let (renamed, dropped) = renamed_and_dropped_columns(&local, &remote);
        let renamed = renamed
            .iter()
            .map(|(old_name, new_name)| (old_name.as_str(), new_name.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(renamed, vec![("x", "y"), ("l", "x")]);
        assert!(dropped.is_empty());
    }

    /// The number of random cases of
    /// `fuzz_match_of_remote_drops_renames_and_adds`. Set `SEED` to run other
    /// cases.
    const MATCH_FUZZ_CASES: u64 = 2000;

    /// The remote keeps, drops or renames each column, and adds columns at the
    /// end. A rename gives a new name, or the old name of an earlier column
    /// that the remote renamed. The final remote table shows what the remote
    /// did, so the match must find exactly the renames and the drops of the
    /// remote. Each table writes the names in a random form, with or without
    /// quotes.
    #[test]
    fn fuzz_match_of_remote_drops_renames_and_adds() {
        let seed = crate::tests::seed_u64();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        for case in 0..MATCH_FUZZ_CASES {
            let local = random_columns(&mut rng);
            let (remote, renamed, dropped) = random_remote_change(&mut rng, &local);
            let local_sql = create_table_sql(&mut rng, &local);
            let remote_sql = create_table_sql(&mut rng, &remote);
            let local_columns = column_definitions(&local_sql);
            let remote_columns = column_definitions(&remote_sql);
            let (actual_renamed, actual_dropped) =
                renamed_and_dropped_columns(&local_columns, &remote_columns);
            let actual_renamed = actual_renamed
                .iter()
                .map(|(old_name, new_name)| {
                    (old_name.as_str().to_string(), new_name.as_str().to_string())
                })
                .collect::<Vec<_>>();
            let actual_dropped = actual_dropped
                .iter()
                .map(|name| name.as_str().to_string())
                .collect::<Vec<_>>();
            assert_eq!(
                (actual_renamed, actual_dropped),
                (renamed, dropped),
                "seed {seed}, case {case}\nlocal:  {local_sql}\nremote: {remote_sql}"
            );
        }
    }

    /// A column of a table in `fuzz_match_of_remote_drops_renames_and_adds`.
    #[derive(Clone, Debug)]
    struct FuzzColumn {
        name: String,
        generated: bool,
    }

    /// What the remote does with a local column in
    /// `fuzz_match_of_remote_drops_renames_and_adds`.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Change {
        Keep,
        Drop,
        Rename,
    }

    /// Returns the column `id` and up to eight more columns with the names
    /// that `column_name` gives for 0, 1, 2 and so on, each regular or
    /// generated at random. `id` is first, so that no table is empty.
    fn random_columns(rng: &mut ChaCha8Rng) -> Vec<FuzzColumn> {
        let count = rng.random_range(0..=8);
        let mut columns = vec![FuzzColumn {
            name: "id".to_string(),
            generated: false,
        }];
        for index in 0..count {
            columns.push(FuzzColumn {
                name: column_name(index),
                generated: rng.random_bool(0.4),
            });
        }
        columns
    }

    /// Returns the name of the column number `index`. An odd number gives a
    /// name with a space, which needs quotes.
    fn column_name(index: usize) -> String {
        if index % 2 == 0 {
            format!("c{index}")
        } else {
            format!("c {index}")
        }
    }

    /// Returns the remote columns after a random change of the table `local`,
    /// as `fuzz_match_of_remote_drops_renames_and_adds` describes it, and the
    /// renames and the drops of the change, in the local order. The first
    /// column, `id`, stays.
    fn random_remote_change(
        rng: &mut ChaCha8Rng,
        local: &[FuzzColumn],
    ) -> (Vec<FuzzColumn>, Vec<(String, String)>, Vec<String>) {
        let mut changes = local[1..]
            .iter()
            .map(|_| match rng.random_range(0..4) {
                0 => Change::Drop,
                1 => Change::Rename,
                _ => Change::Keep,
            })
            .collect::<Vec<_>>();
        // A drop right before a rename gives the remote table of a rename of
        // the dropped column, so the column after one or more drops keeps its
        // name.
        let mut after_drop = false;
        for change in &mut changes {
            if after_drop && *change == Change::Rename {
                *change = Change::Keep;
            }
            after_drop = *change == Change::Drop;
        }
        // A drop of the last column and an added column give the remote table
        // of a rename of the last column.
        let added_columns: usize = if changes.last() == Some(&Change::Drop) {
            0
        } else {
            rng.random_range(0..3)
        };

        let mut remote = vec![local[0].clone()];
        let mut renamed = Vec::new();
        let mut dropped = Vec::new();
        // The old names of the renamed columns that no column has now.
        let mut free_names = Vec::new();
        for (column, change) in local[1..].iter().zip(changes) {
            match change {
                Change::Keep => remote.push(column.clone()),
                Change::Drop => dropped.push(column.name.clone()),
                Change::Rename => {
                    let new_name = if !free_names.is_empty() && rng.random_bool(0.5) {
                        free_names.swap_remove(rng.random_range(0..free_names.len()))
                    } else {
                        format!("r{}", renamed.len())
                    };
                    free_names.push(column.name.clone());
                    renamed.push((column.name.clone(), new_name.clone()));
                    remote.push(FuzzColumn {
                        name: new_name,
                        generated: column.generated,
                    });
                }
            }
        }
        for index in 0..added_columns {
            remote.push(FuzzColumn {
                name: format!("a{index}"),
                generated: rng.random_bool(0.4),
            });
        }
        (remote, renamed, dropped)
    }

    /// Returns a CREATE TABLE statement for `columns`. It writes each name
    /// without quotes, or in double quotes, brackets or backticks, at random,
    /// so that two tables can write a name in other forms.
    fn create_table_sql(rng: &mut ChaCha8Rng, columns: &[FuzzColumn]) -> String {
        let columns = columns
            .iter()
            .map(|column| {
                let name = &column.name;
                let name = match rng.random_range(0..4) {
                    0 if !name.contains(' ') => name.clone(),
                    0 | 1 => format!("\"{name}\""),
                    2 => format!("[{name}]"),
                    _ => format!("`{name}`"),
                };
                if column.generated {
                    format!("{name} TEXT AS (1)")
                } else {
                    format!("{name} TEXT")
                }
            })
            .collect::<Vec<_>>();
        format!("CREATE TABLE t ({})", columns.join(", "))
    }

    /// Returns the column definitions of the CREATE TABLE statement `sql`.
    fn column_definitions(sql: &str) -> Vec<turso_core::ast::ColumnDefinition> {
        let mut parser = Parser::new(sql.as_bytes());
        let Some(Ok(turso_core::ast::Cmd::Stmt(turso_core::ast::Stmt::CreateTable {
            body: turso_core::ast::CreateTableBody::ColumnsAndConstraints { columns, .. },
            ..
        }))) = parser.next()
        else {
            panic!("not a CREATE TABLE statement: {sql}");
        };
        columns
    }
}
