use crate::sync::Arc;
use crate::sync::RwLock;
use crate::vtab::{InternalVirtualTable, InternalVirtualTableCursor};
use crate::{Connection, Result, Value};
use turso_ext::{ConstraintInfo, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode};

#[derive(Debug)]
pub struct TursoTypesTable;

impl Default for TursoTypesTable {
    fn default() -> Self {
        Self::new()
    }
}

impl TursoTypesTable {
    pub fn new() -> Self {
        Self
    }
}

impl InternalVirtualTable for TursoTypesTable {
    fn name(&self) -> String {
        "sqlite_turso_types".to_string()
    }

    fn sql(&self) -> String {
        "CREATE TABLE sqlite_turso_types(name TEXT, sql TEXT)".to_string()
    }

    fn open(&self, conn: Arc<Connection>) -> Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        let cursor = TursoTypesCursor::new(conn);
        Ok(Arc::new(RwLock::new(cursor)))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        let constraint_usages = constraints
            .iter()
            .map(|_| ConstraintUsage {
                argv_index: None,
                omit: false,
            })
            .collect();

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 10.0,
            estimated_rows: 20,
            constraint_usages,
        })
    }
}

pub struct TursoTypesCursor {
    conn: Arc<Connection>,
    /// Snapshot of type entries: (display_name, sql_string)
    entries: Vec<(String, String)>,
    index: usize,
}

impl TursoTypesCursor {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            entries: Vec::new(),
            index: 0,
        }
    }

    fn snapshot_types(&mut self) {
        self.entries.clear();
        self.conn.with_schema(0, |schema| {
            let mut names: Vec<_> = schema
                .type_registry
                .iter()
                .filter(|(key, td)| *key == &td.name.to_lowercase())
                .map(|(key, _)| key.clone())
                .collect();
            names.sort();
            for name in names {
                let td = &schema.type_registry[&name];
                let display_name = if td.params.is_empty() {
                    td.name.clone()
                } else {
                    let params: Vec<String> = td
                        .params
                        .iter()
                        .map(|p| match &p.ty {
                            Some(ty) => format!("{} {}", p.name, ty),
                            None => p.name.clone(),
                        })
                        .collect();
                    format!("{}({})", td.name, params.join(", "))
                };
                self.entries.push((display_name, td.to_sql()));
            }
        });
    }
}

impl InternalVirtualTableCursor for TursoTypesCursor {
    fn filter(&mut self, _args: &[Value], _idx_str: Option<String>, _idx_num: i32) -> Result<bool> {
        self.snapshot_types();
        self.index = 0;
        Ok(!self.entries.is_empty())
    }

    fn next(&mut self) -> Result<bool> {
        self.index += 1;
        Ok(self.index < self.entries.len())
    }

    fn column(&self, column: usize) -> Result<Value> {
        if self.index >= self.entries.len() {
            return Ok(Value::Null);
        }
        let (ref name, ref sql) = self.entries[self.index];
        match column {
            0 => Ok(Value::from_text(name.clone())),
            1 => Ok(Value::from_text(sql.clone())),
            _ => Ok(Value::Null),
        }
    }

    fn rowid(&self) -> i64 {
        self.index as i64 + 1
    }
}
