use std::{collections::HashMap, sync::Arc};

use turso_parser::parser::Parser;

use crate::{
    database_tape::{run_stmt_once, DatabaseReplaySessionOpts},
    errors::Error,
    types::{
        Coro, DatabaseChangeType, DatabaseRowMutation, DatabaseTapeRowChange,
        DatabaseTapeRowChangeType,
    },
    Result,
};

/// Generates SQL and bind values for replaying CDC or logical row changes.
pub struct DatabaseReplayGenerator {
    /// Connection used to inspect target schema and prepare replay SQL.
    pub conn: Arc<turso_core::Connection>,
    /// Replay options that control rowid handling.
    pub opts: DatabaseReplaySessionOpts,
}

/// Prepared SQL and schema metadata for replaying one tape row change.
pub struct ReplayInfo {
    /// Row-level change type to replay.
    pub change_type: DatabaseChangeType,
    /// SQL statement used by replay for the change.
    pub query: String,
    /// Primary-key column positions for explicit-key replay.
    pub pk_column_indices: Option<Vec<usize>>,
    /// Target table column names in record order.
    pub column_names: Vec<String>,
    /// True when the change replays DDL rather than row values.
    pub is_ddl_replay: bool,
}

const SQLITE_SCHEMA_TABLE: &str = "sqlite_schema";

/// Quotes an identifier for generated replay SQL.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quotes a string literal for generated metadata queries.
fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Decodes the CDC update bitmap stored in the first half of an update record.
pub(crate) fn decode_update_bitmap(updates: &[turso_core::Value]) -> Result<Vec<bool>> {
    if updates.len() % 2 != 0 {
        return Err(Error::DatabaseSyncEngineError(format!(
            "malformed update record: expected even number of fields, got {}",
            updates.len()
        )));
    }
    let columns_cnt = updates.len() / 2;
    let mut columns = Vec::with_capacity(columns_cnt);
    for value in updates.iter().take(columns_cnt) {
        columns.push(match value {
            turso_core::Value::Numeric(turso_core::Numeric::Integer(x @ (1 | 0))) => *x > 0,
            _ => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "unexpected 'changes' binary record first-half component: {value:?}"
                )));
            }
        });
    }
    Ok(columns)
}

impl DatabaseReplayGenerator {
    /// Creates a replay SQL generator for one target connection.
    pub fn new(conn: Arc<turso_core::Connection>, opts: DatabaseReplaySessionOpts) -> Self {
        Self { conn, opts }
    }

    /// Converts a tape row change into the public row-mutation shape.
    ///
    /// Transform hooks use this when they need named before/after/update values
    /// rather than raw SQLite record vectors.
    pub fn create_mutation(
        &self,
        info: &ReplayInfo,
        change: &DatabaseTapeRowChange,
    ) -> Result<DatabaseRowMutation> {
        match &change.change {
            DatabaseTapeRowChangeType::Delete { before } => Ok(DatabaseRowMutation {
                change_time: change.change_time,
                table_name: change.table_name.to_string(),
                id: change.id,
                change_type: info.change_type,
                before: Some(self.create_row_full(info, before)?),
                after: None,
                updates: None,
            }),
            DatabaseTapeRowChangeType::Insert { after } => Ok(DatabaseRowMutation {
                change_time: change.change_time,
                table_name: change.table_name.to_string(),
                id: change.id,
                change_type: info.change_type,
                before: None,
                after: Some(self.create_row_full(info, after)?),
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
                before: Some(self.create_row_full(info, before)?),
                after: Some(self.create_row_full(info, after)?),
                updates: updates
                    .as_ref()
                    .map(|updates| self.create_row_update(info, updates))
                    .transpose()?,
            }),
        }
    }

    /// Converts a full record vector into a column-name keyed row map.
    fn create_row_full(
        &self,
        info: &ReplayInfo,
        values: &[turso_core::Value],
    ) -> Result<HashMap<String, turso_core::Value>> {
        if values.len() > info.column_names.len() {
            return Err(Error::DatabaseSyncEngineError(format!(
                "record has {} values but replay info has {} columns",
                values.len(),
                info.column_names.len()
            )));
        }
        let mut row = HashMap::with_capacity(info.column_names.len());
        for (i, value) in values.iter().enumerate() {
            row.insert(info.column_names[i].clone(), value.clone());
        }
        Ok(row)
    }

    /// Converts a CDC update record into a map of changed columns.
    fn create_row_update(
        &self,
        info: &ReplayInfo,
        updates: &[turso_core::Value],
    ) -> Result<HashMap<String, turso_core::Value>> {
        let mut row = HashMap::with_capacity(info.column_names.len());
        let columns = decode_update_bitmap(updates)?;
        let columns_cnt = updates.len() / 2;
        if columns_cnt > info.column_names.len() {
            return Err(Error::DatabaseSyncEngineError(format!(
                "update record has {} columns but replay info has {} columns",
                columns_cnt,
                info.column_names.len()
            )));
        }
        for (i, updated) in columns.into_iter().enumerate() {
            if !updated {
                continue;
            }
            row.insert(
                info.column_names[i].clone(),
                updates[columns_cnt + i].clone(),
            );
        }
        Ok(row)
    }

    /// Builds bound values for the SQL generated by [`Self::replay_info`].
    ///
    /// Inserts bind the new row, deletes bind either the rowid or primary key,
    /// and updates bind changed columns followed by the row locator.
    pub fn replay_values(
        &self,
        info: &ReplayInfo,
        change: DatabaseChangeType,
        id: i64,
        mut record: Vec<turso_core::Value>,
        updates: Option<Vec<turso_core::Value>>,
    ) -> Result<Vec<turso_core::Value>> {
        if info.is_ddl_replay {
            return Ok(Vec::new());
        }
        Ok(match change {
            DatabaseChangeType::Delete => {
                if self.opts.use_implicit_rowid || info.pk_column_indices.is_none() {
                    vec![turso_core::Value::from_i64(id)]
                } else {
                    let mut values = Vec::new();
                    let pk_column_indices = info.pk_column_indices.as_ref().unwrap();
                    for pk in pk_column_indices {
                        if *pk >= record.len() {
                            return Err(Error::DatabaseSyncEngineError(format!(
                                "primary key column index {} is outside record with {} columns",
                                pk,
                                record.len()
                            )));
                        }
                        let value = std::mem::replace(&mut record[*pk], turso_core::Value::Null);
                        values.push(value);
                    }
                    values
                }
            }
            DatabaseChangeType::Insert => {
                if self.opts.use_implicit_rowid {
                    record.push(turso_core::Value::from_i64(id));
                }
                record
            }
            DatabaseChangeType::Update => {
                let mut updates = updates.ok_or_else(|| {
                    Error::DatabaseSyncEngineError(
                        "update replay requires an updates record".to_string(),
                    )
                })?;
                let columns = decode_update_bitmap(&updates)?;
                let columns_cnt = updates.len() / 2;
                let mut values = Vec::with_capacity(columns_cnt + 1);
                for (i, changed) in columns.into_iter().enumerate() {
                    if !changed {
                        continue;
                    }
                    let value =
                        std::mem::replace(&mut updates[i + columns_cnt], turso_core::Value::Null);
                    values.push(value);
                }
                if let Some(pk_column_indices) = &info.pk_column_indices {
                    for pk in pk_column_indices {
                        if *pk >= record.len() {
                            return Err(Error::DatabaseSyncEngineError(format!(
                                "primary key column index {} is outside record with {} columns",
                                pk,
                                record.len()
                            )));
                        }
                        let value = std::mem::replace(&mut record[*pk], turso_core::Value::Null);
                        values.push(value);
                    }
                } else {
                    values.push(turso_core::Value::from_i64(id));
                }
                values
            }
            DatabaseChangeType::Commit => {
                // COMMIT records are handled at the tape level, not here
                Vec::new()
            }
        })
    }

    /// Inspects schema and returns the SQL needed to replay a row change.
    ///
    /// For ordinary tables this produces insert/update/delete statements. For
    /// `sqlite_schema` CDC rows it converts schema-table mutations into DDL
    /// replay operations with idempotent create handling.
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
                DatabaseTapeRowChangeType::Delete { before } => {
                    if before.len() != 5 {
                        return Err(Error::DatabaseSyncEngineError(format!(
                            "sqlite_schema delete record must have 5 columns, got {}",
                            before.len()
                        )));
                    }
                    let Some(turso_core::Value::Text(entity_type)) = before.first() else {
                        return Err(Error::DatabaseSyncEngineError(format!(
                            "unexpected 'type' column of sqlite_schema table: {:?}",
                            before.first()
                        )));
                    };
                    let Some(turso_core::Value::Text(entity_name)) = before.get(1) else {
                        return Err(Error::DatabaseSyncEngineError(format!(
                            "unexpected 'name' column of sqlite_schema table: {:?}",
                            before.get(1)
                        )));
                    };
                    let query = format!(
                        "DROP {} {}",
                        entity_type.as_str(),
                        quote_ident(entity_name.as_str())
                    );
                    let delete = ReplayInfo {
                        change_type: DatabaseChangeType::Delete,
                        query,
                        pk_column_indices: None,
                        column_names: Vec::new(),
                        is_ddl_replay: true,
                    };
                    Ok(delete)
                }
                DatabaseTapeRowChangeType::Insert { after } => {
                    if after.len() != 5 {
                        return Err(Error::DatabaseSyncEngineError(format!(
                            "sqlite_schema insert record must have 5 columns, got {}",
                            after.len()
                        )));
                    }
                    let Some(turso_core::Value::Text(sql)) = after.last() else {
                        return Err(Error::DatabaseTapeError(format!(
                            "unexpected 'sql' column of sqlite_schema table: {:?}",
                            after.last()
                        )));
                    };
                    let mut parser = Parser::new(sql.as_str().as_bytes());
                    let mut ast = parser
                        .next()
                        .ok_or_else(|| {
                            Error::DatabaseTapeError(format!(
                                "unexpected DDL query: {}",
                                sql.as_str()
                            ))
                        })?
                        .map_err(|e| {
                            Error::DatabaseTapeError(format!(
                                "unexpected DDL query {}: {}",
                                e,
                                sql.as_str()
                            ))
                        })?;
                    let turso_parser::ast::Cmd::Stmt(stmt) = &mut ast else {
                        return Err(Error::DatabaseTapeError(format!(
                            "unexpected DDL query: {}",
                            sql.as_str()
                        )));
                    };
                    match stmt {
                        turso_parser::ast::Stmt::CreateTable { if_not_exists, .. }
                        | turso_parser::ast::Stmt::CreateIndex { if_not_exists, .. }
                        | turso_parser::ast::Stmt::CreateTrigger { if_not_exists, .. }
                        | turso_parser::ast::Stmt::CreateMaterializedView {
                            if_not_exists, ..
                        }
                        | turso_parser::ast::Stmt::CreateView { if_not_exists, .. } => {
                            *if_not_exists = true
                        }
                        _ => {}
                    }
                    let insert = ReplayInfo {
                        change_type: DatabaseChangeType::Insert,
                        query: ast.to_string(),
                        pk_column_indices: None,
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
                    if updates.len() % 2 != 0 || updates.len() / 2 != 5 {
                        return Err(Error::DatabaseSyncEngineError(format!(
                            "sqlite_schema update record must have 5 update columns, got {} values",
                            updates.len()
                        )));
                    }
                    let turso_core::Value::Text(ddl_stmt) = updates.last().unwrap() else {
                        return Err(Error::DatabaseSyncEngineError(format!(
                            "unexpected 'sql' column of sqlite_schema table update record: {:?}",
                            updates.last()
                        )));
                    };
                    let update = ReplayInfo {
                        change_type: DatabaseChangeType::Update,
                        query: ddl_stmt.as_str().to_string(),
                        pk_column_indices: None,
                        column_names: Vec::new(),
                        is_ddl_replay: true,
                    };
                    Ok(update)
                }
            }
        } else {
            match &change.change {
                DatabaseTapeRowChangeType::Delete { .. } => {
                    let delete = self.delete_query(coro, table_name).await?;
                    Ok(delete)
                }
                DatabaseTapeRowChangeType::Update { updates, after, .. } => {
                    if let Some(updates) = updates {
                        let columns = decode_update_bitmap(updates)?;
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

    /// Builds an UPDATE replay statement for the subset of changed columns.
    pub(crate) async fn update_query<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
        columns: &[bool],
    ) -> Result<ReplayInfo> {
        let (column_names, pk_column_indices) = self.table_columns_info(coro, table_name).await?;
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
                    idx,
                    record_columns.len(),
                    table_name
                )));
            }
            pk_predicates.push(format!("{} = ?", quote_ident(&record_columns[idx])));
        }
        for (idx, name) in record_columns.iter().enumerate() {
            if columns[idx] {
                column_updates.push(format!("{} = ?", quote_ident(name)));
            }
        }
        let (query, pk_column_indices) =
            if self.opts.use_implicit_rowid || pk_column_indices.is_empty() {
                (
                    format!(
                        "UPDATE {} SET {} WHERE rowid = ?",
                        quote_ident(table_name),
                        column_updates.join(", ")
                    ),
                    None,
                )
            } else {
                (
                    format!(
                        "UPDATE {} SET {} WHERE {}",
                        quote_ident(table_name),
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
            is_ddl_replay: false,
        })
    }

    /// Builds an INSERT/UPSERT replay statement for a row record.
    pub(crate) async fn upsert_query<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
        columns: usize,
    ) -> Result<ReplayInfo> {
        let (column_names, pk_column_indices) = self.table_columns_info(coro, table_name).await?;
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
                        idx,
                        record_columns.len(),
                        table_name
                    )));
                }
                pk_column_names.push(quote_ident(&record_columns[idx]));
            }
            let mut update_clauses = Vec::new();
            for name in record_columns {
                let quoted = quote_ident(name);
                update_clauses.push(format!("{quoted} = excluded.{quoted}"));
            }
            format!(
                " ON CONFLICT({}) DO UPDATE SET {}",
                pk_column_names.join(","),
                update_clauses.join(",")
            )
        } else {
            String::new()
        };
        if !self.opts.use_implicit_rowid {
            let col_list = record_columns
                .iter()
                .map(|name| quote_ident(name))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = ["?"].repeat(columns).join(",");
            let query = format!(
                "INSERT INTO {}({col_list}) VALUES ({placeholders}){conflict_clause}",
                quote_ident(table_name)
            );
            return Ok(ReplayInfo {
                change_type: DatabaseChangeType::Insert,
                query,
                pk_column_indices: None,
                column_names: record_columns.to_vec(),
                is_ddl_replay: false,
            });
        };
        let mut insert_columns = record_columns.to_vec();
        let original_column_names = insert_columns.clone();
        insert_columns.push("rowid".to_string());

        let rowid_conflict_clause = if record_columns.is_empty() {
            " ON CONFLICT DO NOTHING".to_string()
        } else {
            let mut update_clauses = Vec::with_capacity(record_columns.len());
            for name in record_columns {
                let quoted = quote_ident(name);
                update_clauses.push(format!("{quoted} = excluded.{quoted}"));
            }
            format!(" ON CONFLICT DO UPDATE SET {}", update_clauses.join(","))
        };
        let placeholders = ["?"].repeat(columns + 1).join(",");
        let col_list = insert_columns
            .iter()
            .map(|name| quote_ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "INSERT INTO {}({col_list}) VALUES ({placeholders}){rowid_conflict_clause}",
            quote_ident(table_name)
        );
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Insert,
            query,
            column_names: original_column_names,
            pk_column_indices: Some(pk_column_indices),
            is_ddl_replay: false,
        })
    }

    /// Builds a DELETE replay statement using rowid or primary-key columns.
    pub(crate) async fn delete_query<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<ReplayInfo> {
        let (column_names, pk_column_indices) = self.table_columns_info(coro, table_name).await?;
        let mut pk_predicates = Vec::with_capacity(1);
        for &idx in &pk_column_indices {
            pk_predicates.push(format!("{} = ?", quote_ident(&column_names[idx])));
        }
        let use_implicit_rowid = self.opts.use_implicit_rowid;
        if pk_column_indices.is_empty() || use_implicit_rowid {
            let query = format!("DELETE FROM {} WHERE rowid = ?", quote_ident(table_name));
            tracing::trace!(
                "delete_query: table_name={table_name}, query={query}, use_implicit_rowid={use_implicit_rowid}"
            );
            return Ok(ReplayInfo {
                change_type: DatabaseChangeType::Delete,
                query,
                column_names,
                pk_column_indices: None,
                is_ddl_replay: false,
            });
        }
        let pk_predicates = pk_predicates.join(" AND ");
        let query = format!(
            "DELETE FROM {} WHERE {pk_predicates}",
            quote_ident(table_name)
        );

        tracing::trace!(
            "delete_query: table_name={table_name}, query={query}, use_implicit_rowid={use_implicit_rowid}"
        );
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Delete,
            query,
            column_names,
            pk_column_indices: Some(pk_column_indices),
            is_ddl_replay: false,
        })
    }

    /// Execute a DDL statement idempotently.
    ///
    /// For `ALTER TABLE .. ADD COLUMN`, skip the statement if the column already
    /// exists. For `CREATE TABLE`, if the table already exists, compare the
    /// declared columns and `ALTER TABLE .. ADD COLUMN` any columns that are
    /// missing locally. Falls back to direct execution for other DDL.
    pub async fn execute_ddl_idempotent<Ctx>(&self, coro: &Coro<Ctx>, ddl: &str) -> Result<()> {
        let mut parser = Parser::new(ddl.as_bytes());
        let Some(Ok(mut ast)) = parser.next() else {
            self.conn.execute(ddl)?;
            return Ok(());
        };
        let turso_parser::ast::Cmd::Stmt(stmt) = &mut ast else {
            self.conn.execute(ddl)?;
            return Ok(());
        };
        match stmt {
            turso_parser::ast::Stmt::AlterTable(turso_parser::ast::AlterTable {
                name: tbl_name,
                body: turso_parser::ast::AlterTableBody::AddColumn(col_def),
            }) => {
                let table_name = tbl_name.name.as_str();
                let (current_columns, _) = self.table_columns_info(coro, table_name).await?;
                let col_name = col_def.col_name.as_str();
                if current_columns.iter().any(|c| c == col_name) {
                    tracing::debug!(
                        "execute_ddl_idempotent: column {col_name} already exists in {table_name}, skipping"
                    );
                    return Ok(());
                }
                self.conn.execute(ddl)?;
                Ok(())
            }
            turso_parser::ast::Stmt::CreateTable { tbl_name, body, .. } => {
                let table_name = tbl_name.name.as_str();
                let table_exists = self
                    .conn
                    .prepare(format!(
                        "SELECT 1 FROM sqlite_schema WHERE type='table' AND name={}",
                        quote_sql_string(table_name)
                    ))?
                    .run_collect_rows()?
                    .into_iter()
                    .next()
                    .is_some();
                if !table_exists {
                    self.conn.execute(ddl)?;
                    return Ok(());
                }

                let turso_parser::ast::CreateTableBody::ColumnsAndConstraints { columns, .. } =
                    body
                else {
                    tracing::debug!(
                        "execute_ddl_idempotent: table {table_name} already exists and CREATE TABLE AS SELECT cannot be reconciled; skipping"
                    );
                    return Ok(());
                };

                let (current_columns, _) = self.table_columns_info(coro, table_name).await?;
                for col_def in columns {
                    let col_name = col_def.col_name.as_str();
                    if current_columns.iter().any(|c| c == col_name) {
                        continue;
                    }
                    let alter_sql = format!(
                        "ALTER TABLE {} ADD COLUMN {col_def}",
                        quote_ident(table_name)
                    );
                    tracing::debug!(
                        "execute_ddl_idempotent: add missing column {col_name} to {table_name} via {alter_sql}"
                    );
                    self.conn.execute(alter_sql)?;
                }
                Ok(())
            }
            turso_parser::ast::Stmt::CreateIndex { if_not_exists, .. }
            | turso_parser::ast::Stmt::CreateTrigger { if_not_exists, .. }
            | turso_parser::ast::Stmt::CreateMaterializedView { if_not_exists, .. }
            | turso_parser::ast::Stmt::CreateView { if_not_exists, .. } => {
                *if_not_exists = true;
                self.conn.execute(ast.to_string())?;
                Ok(())
            }
            _ => {
                self.conn.execute(ddl)?;
                Ok(())
            }
        }
    }

    /// Reads column names and primary-key positions for a replay target table.
    async fn table_columns_info<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<(Vec<String>, Vec<usize>)> {
        let mut table_info_stmt = self.conn.prepare(format!(
            "SELECT cid, name, pk FROM pragma_table_info({})",
            quote_sql_string(table_name)
        ))?;
        let mut pk_column_indices = Vec::with_capacity(1);
        let mut column_names = Vec::new();
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
            let turso_core::Value::Numeric(turso_core::Numeric::Integer(pk)) = column.get_value(2)
            else {
                return Err(Error::DatabaseTapeError(
                    "unexpected column type for pragma_table_info query".to_string(),
                ));
            };
            if *pk == 1 {
                pk_column_indices.push(*column_id as usize);
            }
            column_names.push(name.as_str().to_string());
        }
        Ok((column_names, pk_column_indices))
    }
}
