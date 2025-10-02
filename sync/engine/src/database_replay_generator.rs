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

pub struct DatabaseReplayGenerator {
    pub conn: Arc<turso_core::Connection>,
    pub opts: DatabaseReplaySessionOpts,
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
        Self { conn, opts }
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
                turso_core::Value::Integer(x @ (1 | 0)) => *x > 0,
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
                    vec![turso_core::Value::Integer(id)]
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
                    record.push(turso_core::Value::Integer(id));
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
                        turso_core::Value::Integer(x @ (1 | 0)) => x > 0,
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
                    values.push(turso_core::Value::Integer(id));
                }
                values
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
                                turso_core::Value::Integer(x @ (1 | 0)) => *x > 0,
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
        let mut pk_predicates = Vec::with_capacity(1);
        let mut column_updates = Vec::with_capacity(1);
        for &idx in &pk_column_indices {
            pk_predicates.push(format!("{} = ?", column_names[idx]));
        }
        for (idx, name) in column_names.iter().enumerate() {
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
            column_names,
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
        let (mut column_names, pk_column_indices) =
            self.table_columns_info(coro, table_name).await?;
        let conflict_clause = if !pk_column_indices.is_empty() {
            let mut pk_column_names = Vec::new();
            for &idx in &pk_column_indices {
                pk_column_names.push(column_names[idx].clone());
            }
            let mut update_clauses = Vec::new();
            for name in &column_names {
                update_clauses.push(format!("{name} = excluded.{name}"));
            }
            format!(
                "ON CONFLICT({}) DO UPDATE SET {}",
                pk_column_names.join(","),
                update_clauses.join(",")
            )
        } else {
            String::new()
        };
        if !self.opts.use_implicit_rowid {
            let placeholders = ["?"].repeat(columns).join(",");
            let query =
                format!("INSERT INTO {table_name} VALUES ({placeholders}){conflict_clause}");
            return Ok(ReplayInfo {
                change_type: DatabaseChangeType::Insert,
                query,
                pk_column_indices: None,
                column_names,
                is_ddl_replay: false,
            });
        };
        let original_column_names = column_names.clone();
        column_names.push("rowid".to_string());

        let placeholders = ["?"].repeat(columns + 1).join(",");
        let column_names = column_names.join(", ");
        let query = format!("INSERT INTO {table_name}({column_names}) VALUES ({placeholders})");
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
        let mut table_info_stmt = self.conn.prepare(format!(
            "SELECT cid, name, pk FROM pragma_table_info('{table_name}')"
        ))?;
        let mut pk_column_indices = Vec::with_capacity(1);
        let mut column_names = Vec::new();
        while let Some(column) = run_stmt_once(coro, &mut table_info_stmt).await? {
            let turso_core::Value::Integer(column_id) = column.get_value(0) else {
                return Err(Error::DatabaseTapeError(
                    "unexpected column type for pragma_table_info query".to_string(),
                ));
            };
            let turso_core::Value::Text(name) = column.get_value(1) else {
                return Err(Error::DatabaseTapeError(
                    "unexpected column type for pragma_table_info query".to_string(),
                ));
            };
            let turso_core::Value::Integer(pk) = column.get_value(2) else {
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
