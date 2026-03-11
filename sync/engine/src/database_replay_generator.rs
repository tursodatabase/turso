use std::{collections::HashMap, sync::Arc};

use turso_parser::{
    ast::{ColumnConstraint, CreateTableBody, Expr, TableConstraint},
    parser::Parser,
};

use crate::{
    database_tape::{run_stmt_once, DatabaseReplaySessionOpts},
    errors::Error,
    types::{
        Coro, DatabaseChangeType, DatabaseRowMutation, DatabaseTapeRowChange,
        DatabaseTapeRowChangeType,
    },
    Result,
};

/// Cached column info: (column_names, pk_column_indices)
type SchemaInfo = (Vec<String>, Vec<usize>);

pub struct DatabaseReplayGenerator {
    pub conn: Arc<turso_core::Connection>,
    pub opts: DatabaseReplaySessionOpts,
    /// Tracks the schema for each table as it evolves through the batch of changes.
    /// This prevents column order mismatches when CDC records captured before
    /// DROP COLUMN + ADD COLUMN are replayed against the post-DDL schema.
    schema_cache: HashMap<String, SchemaInfo>,
}

pub struct ReplayInfo {
    pub change_type: DatabaseChangeType,
    pub query: String,
    pub pk_column_indices: Option<Vec<usize>>,
    pub column_names: Vec<String>,
    pub is_ddl_replay: bool,
}

const SQLITE_SCHEMA_TABLE: &str = "sqlite_schema";
impl DatabaseReplayGenerator {
    pub fn new(conn: Arc<turso_core::Connection>, opts: DatabaseReplaySessionOpts) -> Self {
        Self {
            conn,
            opts,
            schema_cache: HashMap::new(),
        }
    }

    /// Parse a CREATE TABLE SQL statement to extract column names and PK indices.
    fn parse_columns_from_sql(sql: &str) -> Result<SchemaInfo> {
        let mut parser = Parser::new(sql.as_bytes());
        let ast = parser
            .next()
            .ok_or_else(|| {
                Error::DatabaseTapeError(format!("could not parse DDL for schema cache: {sql}"))
            })?
            .map_err(|e| {
                Error::DatabaseTapeError(format!(
                    "could not parse DDL for schema cache: {e}: {sql}"
                ))
            })?;
        let turso_parser::ast::Cmd::Stmt(stmt) = &ast else {
            return Err(Error::DatabaseTapeError(format!(
                "unexpected DDL for schema cache: {sql}"
            )));
        };
        let turso_parser::ast::Stmt::CreateTable { body, .. } = stmt else {
            return Err(Error::DatabaseTapeError(format!(
                "expected CREATE TABLE for schema cache: {sql}"
            )));
        };
        let CreateTableBody::ColumnsAndConstraints {
            columns,
            constraints,
            ..
        } = body
        else {
            return Err(Error::DatabaseTapeError(format!(
                "expected ColumnsAndConstraints for schema cache: {sql}"
            )));
        };
        let mut column_names = Vec::with_capacity(columns.len());
        let mut pk_column_indices = Vec::new();
        for (i, col) in columns.iter().enumerate() {
            column_names.push(col.col_name.as_str().to_string());
            for named_constraint in &col.constraints {
                if matches!(
                    named_constraint.constraint,
                    ColumnConstraint::PrimaryKey { .. }
                ) {
                    pk_column_indices.push(i);
                }
            }
        }
        for named_constraint in constraints {
            if let TableConstraint::PrimaryKey {
                columns: pk_cols, ..
            } = &named_constraint.constraint
            {
                for pk_col in pk_cols {
                    if let Expr::Id(ref name) = *pk_col.expr {
                        if let Some(idx) = column_names.iter().position(|n| n == name.as_str()) {
                            pk_column_indices.push(idx);
                        }
                    }
                }
            }
        }
        Ok((column_names, pk_column_indices))
    }

    /// Pre-scan a batch of changes to seed the schema cache with the initial
    /// column ordering for tables that undergo DDL changes in this batch.
    /// Must be called before processing any changes. Clears the existing cache first.
    pub fn seed_schema_from_changes(&mut self, changes: &[DatabaseTapeRowChange]) {
        self.schema_cache.clear();
        for change in changes {
            if change.table_name != SQLITE_SCHEMA_TABLE {
                continue;
            }
            match &change.change {
                DatabaseTapeRowChangeType::Update { before, .. }
                | DatabaseTapeRowChangeType::Delete { before } => {
                    // sqlite_schema columns: type, name, tbl_name, rootpage, sql
                    if before.len() != 5 {
                        continue;
                    }
                    let Some(turso_core::Value::Text(entity_type)) = before.first() else {
                        continue;
                    };
                    if entity_type.as_str() != "table" {
                        continue;
                    }
                    let Some(turso_core::Value::Text(tbl_name)) = before.get(2) else {
                        continue;
                    };
                    let tbl_name = tbl_name.as_str().to_string();
                    if self.schema_cache.contains_key(&tbl_name) {
                        // Already seeded from an earlier DDL change for this table
                        continue;
                    }
                    let Some(turso_core::Value::Text(sql)) = before.last() else {
                        continue;
                    };
                    if let Ok(info) = Self::parse_columns_from_sql(sql.as_str()) {
                        tracing::trace!("schema_cache: seeded '{}' with {:?}", tbl_name, info.0);
                        self.schema_cache.insert(tbl_name, info);
                    }
                }
                DatabaseTapeRowChangeType::Insert { .. } => {
                    // CREATE TABLE - table didn't exist before, no seeding needed
                }
            }
        }
    }

    /// Update the schema cache after a DDL change has been processed.
    /// Must be called after each sqlite_schema change is replayed.
    pub fn notify_ddl_applied(&mut self, change: &DatabaseTapeRowChange) {
        if change.table_name != SQLITE_SCHEMA_TABLE {
            return;
        }
        match &change.change {
            DatabaseTapeRowChangeType::Insert { after } => {
                // CREATE TABLE: add to cache
                if after.len() != 5 {
                    return;
                }
                let Some(turso_core::Value::Text(entity_type)) = after.first() else {
                    return;
                };
                if entity_type.as_str() != "table" {
                    return;
                }
                let Some(turso_core::Value::Text(tbl_name)) = after.get(2) else {
                    return;
                };
                let Some(turso_core::Value::Text(sql)) = after.last() else {
                    return;
                };
                if let Ok(info) = Self::parse_columns_from_sql(sql.as_str()) {
                    tracing::trace!(
                        "schema_cache: CREATE TABLE '{}' -> {:?}",
                        tbl_name.as_str(),
                        info.0
                    );
                    self.schema_cache
                        .insert(tbl_name.as_str().to_string(), info);
                }
            }
            DatabaseTapeRowChangeType::Update { updates, .. } => {
                // ALTER TABLE: update cache from new SQL
                let Some(updates) = updates else {
                    return;
                };
                if updates.len() != 10 {
                    return;
                }
                // Extract tbl_name from the updates (index 7 = value for column 2 = tbl_name)
                let tbl_name = match &updates[7] {
                    turso_core::Value::Text(name) => name.as_str().to_string(),
                    _ => return,
                };
                // Extract new SQL from updates (index 9 = value for column 4 = sql)
                let turso_core::Value::Text(sql) = &updates[9] else {
                    return;
                };
                if let Ok(info) = Self::parse_columns_from_sql(sql.as_str()) {
                    tracing::trace!("schema_cache: ALTER TABLE '{}' -> {:?}", tbl_name, info.0);
                    self.schema_cache.insert(tbl_name, info);
                }
            }
            DatabaseTapeRowChangeType::Delete { before } => {
                // DROP TABLE: remove from cache
                if before.len() != 5 {
                    return;
                }
                let Some(turso_core::Value::Text(entity_type)) = before.first() else {
                    return;
                };
                if entity_type.as_str() != "table" {
                    return;
                }
                let Some(turso_core::Value::Text(tbl_name)) = before.get(2) else {
                    return;
                };
                tracing::trace!("schema_cache: DROP TABLE '{}'", tbl_name.as_str());
                self.schema_cache.remove(tbl_name.as_str());
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

    async fn table_columns_info<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        table_name: &str,
    ) -> Result<(Vec<String>, Vec<usize>)> {
        // Use cached schema if available (prevents column order mismatch after DDL changes)
        if let Some(cached) = self.schema_cache.get(table_name) {
            return Ok(cached.clone());
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::types::Text;

    fn text_val(s: &str) -> turso_core::Value {
        turso_core::Value::Text(Text::new(s.to_string()))
    }

    fn int_val(i: i64) -> turso_core::Value {
        turso_core::Value::from_i64(i)
    }

    fn make_test_generator() -> DatabaseReplayGenerator {
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = turso_core::Database::open_file(io, ":memory:").unwrap();
        let conn = db.connect().unwrap();
        DatabaseReplayGenerator::new(
            conn,
            DatabaseReplaySessionOpts {
                use_implicit_rowid: false,
            },
        )
    }

    fn make_schema_change(change: DatabaseTapeRowChangeType) -> DatabaseTapeRowChange {
        DatabaseTapeRowChange {
            change_id: 1,
            change_time: 0,
            change,
            table_name: SQLITE_SCHEMA_TABLE.to_string(),
            id: 1,
        }
    }

    /// sqlite_schema columns: type, name, tbl_name, rootpage, sql
    fn schema_row(
        entity_type: &str,
        name: &str,
        tbl_name: &str,
        sql: &str,
    ) -> Vec<turso_core::Value> {
        vec![
            text_val(entity_type),
            text_val(name),
            text_val(tbl_name),
            int_val(1),
            text_val(sql),
        ]
    }

    /// sqlite_schema UPDATE "updates" record: [changed_0..4, value_0..4]
    fn schema_updates(sql: &str, tbl_name: &str) -> Vec<turso_core::Value> {
        vec![
            int_val(0),
            int_val(0),
            int_val(0),
            int_val(0),
            int_val(1),
            text_val("table"),
            text_val(tbl_name),
            text_val(tbl_name),
            int_val(1),
            text_val(sql),
        ]
    }

    #[test]
    fn test_parse_columns_basic() {
        let (cols, pks) = DatabaseReplayGenerator::parse_columns_from_sql(
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        )
        .unwrap();
        assert_eq!(cols, vec!["id", "name", "email"]);
        assert_eq!(pks, vec![0]);
    }

    #[test]
    fn test_parse_columns_table_level_pk() {
        let (cols, pks) = DatabaseReplayGenerator::parse_columns_from_sql(
            "CREATE TABLE t(a TEXT, b TEXT, c TEXT, PRIMARY KEY(b))",
        )
        .unwrap();
        assert_eq!(cols, vec!["a", "b", "c"]);
        assert_eq!(pks, vec![1]);
    }

    #[test]
    fn test_parse_columns_no_pk() {
        let (cols, pks) =
            DatabaseReplayGenerator::parse_columns_from_sql("CREATE TABLE t(x TEXT, y TEXT)")
                .unwrap();
        assert_eq!(cols, vec!["x", "y"]);
        assert!(pks.is_empty());
    }

    #[test]
    fn test_seed_schema_from_alter_table_update() {
        let old_sql =
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, temp TEXT, email TEXT)";
        let new_sql = "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT)";

        let changes = vec![make_schema_change(DatabaseTapeRowChangeType::Update {
            before: schema_row("table", "users", "users", old_sql),
            after: schema_row("table", "users", "users", new_sql),
            updates: Some(schema_updates(new_sql, "users")),
        })];

        let mut gen = make_test_generator();
        gen.seed_schema_from_changes(&changes);

        let (cols, pks) = gen.schema_cache.get("users").unwrap();
        assert_eq!(cols, &["id", "name", "temp", "email"]);
        assert_eq!(pks, &[0]);
    }

    #[test]
    fn test_notify_ddl_create_table() {
        let sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c TEXT)";
        let change = make_schema_change(DatabaseTapeRowChangeType::Insert {
            after: schema_row("table", "t", "t", sql),
        });

        let mut gen = make_test_generator();
        gen.notify_ddl_applied(&change);

        let (cols, pks) = gen.schema_cache.get("t").unwrap();
        assert_eq!(cols, &["a", "b", "c"]);
        assert_eq!(pks, &[0]);
    }

    #[test]
    fn test_notify_ddl_alter_table() {
        let old_sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c TEXT)";
        let new_sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, c TEXT)";

        let mut gen = make_test_generator();
        gen.schema_cache.insert(
            "t".to_string(),
            DatabaseReplayGenerator::parse_columns_from_sql(old_sql).unwrap(),
        );

        let change = make_schema_change(DatabaseTapeRowChangeType::Update {
            before: schema_row("table", "t", "t", old_sql),
            after: schema_row("table", "t", "t", new_sql),
            updates: Some(schema_updates(new_sql, "t")),
        });
        gen.notify_ddl_applied(&change);

        let (cols, _) = gen.schema_cache.get("t").unwrap();
        assert_eq!(cols, &["a", "c"]);
    }

    #[test]
    fn test_notify_ddl_drop_table() {
        let sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)";

        let mut gen = make_test_generator();
        gen.schema_cache.insert(
            "t".to_string(),
            DatabaseReplayGenerator::parse_columns_from_sql(sql).unwrap(),
        );

        let change = make_schema_change(DatabaseTapeRowChangeType::Delete {
            before: schema_row("table", "t", "t", sql),
        });
        gen.notify_ddl_applied(&change);

        assert!(!gen.schema_cache.contains_key("t"));
    }

    #[test]
    fn test_aba_column_order_mismatch() {
        // Reproduces the ABA problem from issue #5640:
        // Table starts as users(id, name, temp, email)
        // INSERT captured with old schema: [1, 'Alice', 'junk', 'alice@ex.com']
        // ALTER TABLE DROP COLUMN temp → (id, name, email)
        // ALTER TABLE ADD COLUMN temp → (id, name, email, temp)
        // Without fix: values[2]='junk'→email, values[3]='alice@ex.com'→temp (WRONG)
        // With fix: schema cache tracks original ordering

        let original_sql =
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, temp TEXT, email TEXT)";
        let after_drop_sql = "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT)";
        let after_add_sql =
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT, temp TEXT)";

        let dml_insert = DatabaseTapeRowChange {
            change_id: 1,
            change_time: 0,
            change: DatabaseTapeRowChangeType::Insert {
                after: vec![
                    int_val(1),
                    text_val("Alice"),
                    text_val("junk"),
                    text_val("alice@ex.com"),
                ],
            },
            table_name: "users".to_string(),
            id: 1,
        };
        let ddl_drop = make_schema_change(DatabaseTapeRowChangeType::Update {
            before: schema_row("table", "users", "users", original_sql),
            after: schema_row("table", "users", "users", after_drop_sql),
            updates: Some(schema_updates(after_drop_sql, "users")),
        });
        let ddl_add = make_schema_change(DatabaseTapeRowChangeType::Update {
            before: schema_row("table", "users", "users", after_drop_sql),
            after: schema_row("table", "users", "users", after_add_sql),
            updates: Some(schema_updates(after_add_sql, "users")),
        });

        let all_changes = vec![dml_insert, ddl_drop, ddl_add];

        let mut gen = make_test_generator();
        gen.seed_schema_from_changes(&all_changes);

        // Cache should have ORIGINAL schema (before first DDL)
        let (cols, _) = gen.schema_cache.get("users").unwrap();
        assert_eq!(cols, &["id", "name", "temp", "email"]);

        // create_row_full with original schema should map correctly
        let info = ReplayInfo {
            change_type: DatabaseChangeType::Insert,
            query: String::new(),
            pk_column_indices: Some(vec![0]),
            column_names: cols.clone(),
            is_ddl_replay: false,
        };
        let row = gen.create_row_full(
            &info,
            &[
                int_val(1),
                text_val("Alice"),
                text_val("junk"),
                text_val("alice@ex.com"),
            ],
        );

        assert_eq!(row.get("temp").unwrap(), &text_val("junk"));
        assert_eq!(row.get("email").unwrap(), &text_val("alice@ex.com"));

        // After DDL drop: schema evolves to (id, name, email)
        gen.notify_ddl_applied(&all_changes[1]);
        let (cols, _) = gen.schema_cache.get("users").unwrap();
        assert_eq!(cols, &["id", "name", "email"]);

        // After DDL add: schema evolves to (id, name, email, temp)
        gen.notify_ddl_applied(&all_changes[2]);
        let (cols, _) = gen.schema_cache.get("users").unwrap();
        assert_eq!(cols, &["id", "name", "email", "temp"]);
    }

    #[test]
    fn test_seed_ignores_non_table_entities() {
        let change = make_schema_change(DatabaseTapeRowChangeType::Delete {
            before: schema_row(
                "index",
                "idx_users_email",
                "users",
                "CREATE INDEX idx_users_email ON users(email)",
            ),
        });

        let mut gen = make_test_generator();
        gen.seed_schema_from_changes(&[change]);
        assert!(gen.schema_cache.is_empty());
    }

    #[test]
    fn test_seed_clears_previous_cache() {
        let mut gen = make_test_generator();
        gen.schema_cache
            .insert("stale".to_string(), (vec!["a".to_string()], vec![]));

        gen.seed_schema_from_changes(&[]);
        assert!(gen.schema_cache.is_empty());
    }

    #[test]
    fn test_simple_drop_column_mismatch() {
        // Simpler variant: single DROP COLUMN without re-add
        // users(id, name, temp, email) → INSERT → DROP COLUMN temp → (id, name, email)
        // Old 4-value record mapped to 3-column schema would corrupt data

        let original_sql =
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, temp TEXT, email TEXT)";
        let after_drop_sql = "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT)";

        let dml_insert = DatabaseTapeRowChange {
            change_id: 1,
            change_time: 0,
            change: DatabaseTapeRowChangeType::Insert {
                after: vec![
                    int_val(1),
                    text_val("Alice"),
                    text_val("junk"),
                    text_val("alice@ex.com"),
                ],
            },
            table_name: "users".to_string(),
            id: 1,
        };
        let ddl_drop = make_schema_change(DatabaseTapeRowChangeType::Update {
            before: schema_row("table", "users", "users", original_sql),
            after: schema_row("table", "users", "users", after_drop_sql),
            updates: Some(schema_updates(after_drop_sql, "users")),
        });

        let all_changes = vec![dml_insert, ddl_drop];
        let mut gen = make_test_generator();
        gen.seed_schema_from_changes(&all_changes);

        let (cols, _) = gen.schema_cache.get("users").unwrap();
        assert_eq!(cols, &["id", "name", "temp", "email"]);

        let info = ReplayInfo {
            change_type: DatabaseChangeType::Insert,
            query: String::new(),
            pk_column_indices: Some(vec![0]),
            column_names: cols.clone(),
            is_ddl_replay: false,
        };
        let row = gen.create_row_full(
            &info,
            &[
                int_val(1),
                text_val("Alice"),
                text_val("junk"),
                text_val("alice@ex.com"),
            ],
        );

        // Without fix, values[2]='junk' would map to 'email' (wrong)
        assert_eq!(row.get("temp").unwrap(), &text_val("junk"));
        assert_eq!(row.get("email").unwrap(), &text_val("alice@ex.com"));
        assert_eq!(row.get("name").unwrap(), &text_val("Alice"));
    }
}
