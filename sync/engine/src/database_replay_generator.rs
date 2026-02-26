use std::{collections::HashMap, sync::Arc, sync::Mutex};

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

/// Column names and primary key indices for a table.
type TableColumnInfo = (Vec<String>, Vec<usize>);

pub struct DatabaseReplayGenerator {
    pub conn: Arc<turso_core::Connection>,
    pub opts: DatabaseReplaySessionOpts,
    /// Cache of table column info derived from CREATE TABLE statements in the
    /// CDC stream. This tracks schema evolution so that CDC records captured
    /// before ALTER TABLE DROP/ADD COLUMN are correctly mapped to the schema
    /// that was active at capture time.
    schema_cache: Mutex<HashMap<String, TableColumnInfo>>,
}

pub struct ReplayInfo {
    pub change_type: DatabaseChangeType,
    pub query: String,
    pub pk_column_indices: Option<Vec<usize>>,
    pub column_names: Vec<String>,
    pub is_ddl_replay: bool,
}

const SQLITE_SCHEMA_TABLE: &str = "sqlite_schema";

/// Parse column names and primary key indices from a CREATE TABLE SQL statement.
fn parse_columns_from_create_table_sql(sql: &str) -> Option<(Vec<String>, Vec<usize>)> {
    let mut parser = Parser::new(sql.as_bytes());
    let ast = parser.next()?.ok()?;
    let turso_parser::ast::Cmd::Stmt(turso_parser::ast::Stmt::CreateTable { body, .. }) = ast
    else {
        return None;
    };
    let turso_parser::ast::CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        ..
    } = body
    else {
        return None;
    };

    let mut names = Vec::with_capacity(columns.len());
    let mut pk_indices = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        names.push(col.col_name.as_str().to_string());
        for c in &col.constraints {
            if matches!(
                c.constraint,
                turso_parser::ast::ColumnConstraint::PrimaryKey { .. }
            ) {
                pk_indices.push(i);
            }
        }
    }

    // Check table-level PRIMARY KEY constraint if no column-level PK found
    if pk_indices.is_empty() {
        for tc in &constraints {
            if let turso_parser::ast::TableConstraint::PrimaryKey {
                columns: pk_cols, ..
            } = &tc.constraint
            {
                for pk_col in pk_cols {
                    if let turso_parser::ast::Expr::Id(name) = pk_col.expr.as_ref() {
                        for (i, col_name) in names.iter().enumerate() {
                            if col_name.eq_ignore_ascii_case(name.as_str()) {
                                pk_indices.push(i);
                            }
                        }
                    }
                }
            }
        }
    }

    Some((names, pk_indices))
}

/// Extract the table name from a sqlite_schema row's values.
/// sqlite_schema columns: type, name, tbl_name, rootpage, sql
fn extract_table_name_from_schema_row(row: &[turso_core::Value]) -> Option<String> {
    if let Some(turso_core::Value::Text(name)) = row.get(2) {
        Some(name.as_str().to_string())
    } else {
        None
    }
}

/// Extract and parse the CREATE TABLE SQL from a sqlite_schema row's values.
fn extract_schema_from_row(row: &[turso_core::Value]) -> Option<(String, Vec<String>, Vec<usize>)> {
    let turso_core::Value::Text(entity_type) = row.first()? else {
        return None;
    };
    if entity_type.as_str() != "table" {
        return None;
    }
    let table_name = extract_table_name_from_schema_row(row)?;
    let turso_core::Value::Text(sql) = row.last()? else {
        return None;
    };
    let (col_names, pk_indices) = parse_columns_from_create_table_sql(sql.as_str())?;
    Some((table_name, col_names, pk_indices))
}

impl DatabaseReplayGenerator {
    pub fn new(conn: Arc<turso_core::Connection>, opts: DatabaseReplaySessionOpts) -> Self {
        Self {
            conn,
            opts,
            schema_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Pre-scan CDC changes to seed the schema cache with the initial column
    /// ordering for each table that has DDL changes in this batch.
    ///
    /// For tables with CREATE TABLE: uses the CREATE TABLE SQL directly.
    /// For tables with ALTER TABLE: uses the 'before' record of the first
    /// ALTER TABLE to recover the pre-alter schema.
    ///
    /// This must be called before processing DML changes so that CDC records
    /// captured before schema changes are correctly mapped.
    pub fn seed_schema_from_changes(&self, changes: &[DatabaseTapeRowChange]) {
        let mut cache = self.schema_cache.lock().unwrap();
        for change in changes {
            if change.table_name != SQLITE_SCHEMA_TABLE {
                continue;
            }
            match &change.change {
                DatabaseTapeRowChangeType::Insert { after } => {
                    // CREATE TABLE/INDEX/etc - parse and cache the initial schema
                    if let Some((table_name, col_names, pk_indices)) =
                        extract_schema_from_row(after)
                    {
                        cache.entry(table_name).or_insert((col_names, pk_indices));
                    }
                }
                DatabaseTapeRowChangeType::Update { before, .. } => {
                    // ALTER TABLE - seed with the BEFORE schema (pre-alter column order)
                    // if this is the first DDL change for this table in the batch
                    if let Some((table_name, col_names, pk_indices)) =
                        extract_schema_from_row(before)
                    {
                        cache.entry(table_name).or_insert((col_names, pk_indices));
                    }
                }
                DatabaseTapeRowChangeType::Delete { .. } => {
                    // DROP TABLE - no action needed during pre-scan
                }
            }
        }
    }

    /// Update the schema cache when a DDL change is processed.
    /// Called from replay_info when handling sqlite_schema changes.
    fn update_schema_for_ddl(&self, change: &DatabaseTapeRowChange) {
        match &change.change {
            DatabaseTapeRowChangeType::Insert { after } => {
                if let Some((table_name, col_names, pk_indices)) = extract_schema_from_row(after) {
                    self.schema_cache
                        .lock()
                        .unwrap()
                        .insert(table_name, (col_names, pk_indices));
                }
            }
            DatabaseTapeRowChangeType::Update { after, .. } => {
                // After ALTER TABLE, update cache with the new schema from the 'after' record
                if let Some((table_name, col_names, pk_indices)) = extract_schema_from_row(after) {
                    self.schema_cache
                        .lock()
                        .unwrap()
                        .insert(table_name, (col_names, pk_indices));
                }
            }
            DatabaseTapeRowChangeType::Delete { before } => {
                if let Some(table_name) = extract_table_name_from_schema_row(before) {
                    self.schema_cache.lock().unwrap().remove(&table_name);
                }
            }
        }
    }

    /// Returns the table name affected by a DDL change, if any.
    /// Used by the replay session to invalidate cached statements.
    pub fn ddl_affected_table(change: &DatabaseTapeRowChange) -> Option<String> {
        if change.table_name != SQLITE_SCHEMA_TABLE {
            return None;
        }
        match &change.change {
            DatabaseTapeRowChangeType::Insert { after } => {
                extract_table_name_from_schema_row(after)
            }
            DatabaseTapeRowChangeType::Update { after, .. } => {
                extract_table_name_from_schema_row(after)
            }
            DatabaseTapeRowChangeType::Delete { before } => {
                extract_table_name_from_schema_row(before)
            }
        }
    }

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
    pub fn replay_values(
        &self,
        info: &ReplayInfo,
        change: DatabaseChangeType,
        id: i64,
        mut record: Vec<turso_core::Value>,
        updates: Option<Vec<turso_core::Value>>,
    ) -> Vec<turso_core::Value> {
        if info.is_ddl_replay {
            return Vec::new();
        }
        match change {
            DatabaseChangeType::Delete => {
                if self.opts.use_implicit_rowid || info.pk_column_indices.is_none() {
                    vec![turso_core::Value::from_i64(id)]
                } else {
                    let mut values = Vec::new();
                    let pk_column_indices = info.pk_column_indices.as_ref().unwrap();
                    for pk in pk_column_indices {
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
                let mut updates = updates.unwrap();
                assert!(updates.len() % 2 == 0);
                let columns_cnt = updates.len() / 2;
                let mut values = Vec::with_capacity(columns_cnt + 1);
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
                    for pk in pk_column_indices {
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
        }
    }
    pub async fn replay_info<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        change: &DatabaseTapeRowChange,
    ) -> Result<ReplayInfo> {
        tracing::trace!("replay: change={:?}", change);
        let table_name = &change.table_name;

        if table_name == SQLITE_SCHEMA_TABLE {
            // Update schema cache from DDL changes
            self.update_schema_for_ddl(change);

            // sqlite_schema table: type, name, tbl_name, rootpage, sql
            match &change.change {
                DatabaseTapeRowChangeType::Delete { before } => {
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
                    idx, record_columns.len(), table_name
                )));
            }
            pk_predicates.push(format!("{} = ?", record_columns[idx]));
        }
        for (idx, name) in record_columns.iter().enumerate() {
            if columns[idx] {
                column_updates.push(format!("{name} = ?"));
            }
        }
        let (query, pk_column_indices) =
            if self.opts.use_implicit_rowid || pk_column_indices.is_empty() {
                (
                    format!(
                        "UPDATE {table_name} SET {} WHERE rowid = ?",
                        column_updates.join(", ")
                    ),
                    None,
                )
            } else {
                (
                    format!(
                        "UPDATE {table_name} SET {} WHERE {}",
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
                        idx, record_columns.len(), table_name
                    )));
                }
                pk_column_names.push(record_columns[idx].clone());
            }
            let mut update_clauses = Vec::new();
            for name in record_columns {
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
        if !self.opts.use_implicit_rowid {
            let col_list = record_columns.join(", ");
            let placeholders = ["?"].repeat(columns).join(",");
            let query = format!(
                "INSERT INTO {table_name}({col_list}) VALUES ({placeholders}){conflict_clause}"
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

        let placeholders = ["?"].repeat(columns + 1).join(",");
        let col_list = insert_columns.join(", ");
        let query = format!("INSERT INTO {table_name}({col_list}) VALUES ({placeholders})");
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Insert,
            query,
            column_names: original_column_names,
            pk_column_indices: None,
            is_ddl_replay: false,
        })
    }
    pub(crate) async fn delete_query<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<ReplayInfo> {
        let (column_names, pk_column_indices) = self.table_columns_info(coro, table_name).await?;
        let mut pk_predicates = Vec::with_capacity(1);
        for &idx in &pk_column_indices {
            pk_predicates.push(format!("{} = ?", column_names[idx]));
        }
        let use_implicit_rowid = self.opts.use_implicit_rowid;
        if pk_column_indices.is_empty() || use_implicit_rowid {
            let query = format!("DELETE FROM {table_name} WHERE rowid = ?");
            tracing::trace!("delete_query: table_name={table_name}, query={query}, use_implicit_rowid={use_implicit_rowid}");
            return Ok(ReplayInfo {
                change_type: DatabaseChangeType::Delete,
                query,
                column_names,
                pk_column_indices: None,
                is_ddl_replay: false,
            });
        }
        let pk_predicates = pk_predicates.join(" AND ");
        let query = format!("DELETE FROM {table_name} WHERE {pk_predicates}");

        tracing::trace!("delete_query: table_name={table_name}, query={query}, use_implicit_rowid={use_implicit_rowid}");
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Delete,
            query,
            column_names,
            pk_column_indices: Some(pk_column_indices),
            is_ddl_replay: false,
        })
    }

    /// Get column info for a table, checking the schema cache first.
    ///
    /// The schema cache is populated from CREATE TABLE statements in the CDC
    /// stream and tracks schema evolution through ALTER TABLE changes.
    /// If the table is not in the cache (no DDL changes in this batch),
    /// falls back to pragma_table_info which returns the correct current schema.
    async fn table_columns_info<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<(Vec<String>, Vec<usize>)> {
        // Check schema cache first - this has the correct column order
        // for tables that have had DDL changes in the current batch
        if let Some(info) = self.schema_cache.lock().unwrap().get(table_name).cloned() {
            return Ok(info);
        }

        // No cached schema - table has not had DDL changes in this batch.
        // pragma_table_info gives the correct current schema.
        let mut table_info_stmt = self.conn.prepare(format!(
            "SELECT cid, name, pk FROM pragma_table_info('{table_name}')"
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
