use std::sync::Arc;

use crate::{
    database_tape::{run_stmt_once, DatabaseReplaySessionOpts},
    errors::Error,
    types::{Coro, DatabaseChangeType, DatabaseTapeRowChange, DatabaseTapeRowChangeType},
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
    pub is_ddl_replay: bool,
}

const SQLITE_SCHEMA_TABLE: &str = "sqlite_schema";
impl DatabaseReplayGenerator {
    pub fn new(conn: Arc<turso_core::Connection>, opts: DatabaseReplaySessionOpts) -> Self {
        Self { conn, opts }
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
                if self.opts.use_implicit_rowid {
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
    pub async fn replay_info(
        &self,
        coro: &Coro,
        change: &DatabaseTapeRowChange,
    ) -> Result<Vec<ReplayInfo>> {
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
                        is_ddl_replay: true,
                    };
                    Ok(vec![delete])
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
                        is_ddl_replay: true,
                    };
                    Ok(vec![insert])
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
                        is_ddl_replay: true,
                    };
                    Ok(vec![update])
                }
            }
        } else {
            match &change.change {
                DatabaseTapeRowChangeType::Delete { .. } => {
                    let delete = self.delete_query(coro, table_name).await?;
                    Ok(vec![delete])
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
                        Ok(vec![update])
                    } else {
                        let delete = self.delete_query(coro, table_name).await?;
                        let insert = self.insert_query(coro, table_name, after.len()).await?;
                        Ok(vec![delete, insert])
                    }
                }
                DatabaseTapeRowChangeType::Insert { after } => {
                    let insert = self.insert_query(coro, table_name, after.len()).await?;
                    Ok(vec![insert])
                }
            }
        }
    }
    pub(crate) async fn update_query(
        &self,
        coro: &Coro,
        table_name: &str,
        columns: &[bool],
    ) -> Result<ReplayInfo> {
        let mut table_info_stmt = self.conn.prepare(format!(
            "SELECT cid, name, pk FROM pragma_table_info('{table_name}')"
        ))?;
        let mut pk_predicates = Vec::with_capacity(1);
        let mut pk_column_indices = Vec::with_capacity(1);
        let mut column_updates = Vec::with_capacity(1);
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
                pk_predicates.push(format!("{name} = ?"));
                pk_column_indices.push(*column_id as usize);
            }
            if columns[*column_id as usize] {
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
            pk_column_indices,
            is_ddl_replay: false,
        })
    }
    pub(crate) async fn insert_query(
        &self,
        coro: &Coro,
        table_name: &str,
        columns: usize,
    ) -> Result<ReplayInfo> {
        if !self.opts.use_implicit_rowid {
            let placeholders = ["?"].repeat(columns).join(",");
            let query = format!("INSERT INTO {table_name} VALUES ({placeholders})");
            return Ok(ReplayInfo {
                change_type: DatabaseChangeType::Insert,
                query,
                pk_column_indices: None,
                is_ddl_replay: false,
            });
        };
        let mut table_info_stmt = self.conn.prepare(format!(
            "SELECT name FROM pragma_table_info('{table_name}')"
        ))?;
        let mut column_names = Vec::with_capacity(columns + 1);
        while let Some(column) = run_stmt_once(coro, &mut table_info_stmt).await? {
            let turso_core::Value::Text(text) = column.get_value(0) else {
                return Err(Error::DatabaseTapeError(
                    "unexpected column type for pragma_table_info query".to_string(),
                ));
            };
            column_names.push(text.to_string());
        }
        column_names.push("rowid".to_string());

        let placeholders = ["?"].repeat(columns + 1).join(",");
        let column_names = column_names.join(", ");
        let query = format!("INSERT INTO {table_name}({column_names}) VALUES ({placeholders})");
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Insert,
            query,
            pk_column_indices: None,
            is_ddl_replay: false,
        })
    }
    pub(crate) async fn delete_query(&self, coro: &Coro, table_name: &str) -> Result<ReplayInfo> {
        let (query, pk_column_indices) = if self.opts.use_implicit_rowid {
            (format!("DELETE FROM {table_name} WHERE rowid = ?"), None)
        } else {
            let mut pk_info_stmt = self.conn.prepare(format!(
                "SELECT cid, name FROM pragma_table_info('{table_name}') WHERE pk = 1"
            ))?;
            let mut pk_predicates = Vec::with_capacity(1);
            let mut pk_column_indices = Vec::with_capacity(1);
            while let Some(column) = run_stmt_once(coro, &mut pk_info_stmt).await? {
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
                pk_predicates.push(format!("{name} = ?"));
                pk_column_indices.push(*column_id as usize);
            }

            if pk_column_indices.is_empty() {
                (format!("DELETE FROM {table_name} WHERE rowid = ?"), None)
            } else {
                let pk_predicates = pk_predicates.join(" AND ");
                let query = format!("DELETE FROM {table_name} WHERE {pk_predicates}");
                (query, Some(pk_column_indices))
            }
        };
        let use_implicit_rowid = self.opts.use_implicit_rowid;
        tracing::trace!("delete_query: table_name={table_name}, query={query}, use_implicit_rowid={use_implicit_rowid}");
        Ok(ReplayInfo {
            change_type: DatabaseChangeType::Delete,
            query,
            pk_column_indices,
            is_ddl_replay: false,
        })
    }
}
