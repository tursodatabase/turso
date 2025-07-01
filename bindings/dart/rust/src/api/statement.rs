use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;

use flutter_rust_bridge::{frb, RustAutoOpaqueNom};
pub use turso_core::Connection;
pub use turso_core::Statement;
use turso_core::Value;

use crate::helpers::result::ExecuteResult;
use crate::helpers::return_value::ReturnValue;
use crate::helpers::wrapper::Wrapper;
use crate::helpers::{params::Params, result::QueryResult};

#[frb(opaque)]
pub struct LibsqlStatement {
    inner: RustAutoOpaqueNom<Wrapper<Statement>>,
    connection: RustAutoOpaqueNom<Wrapper<Arc<Connection>>>,
}

impl LibsqlStatement {
    pub fn new(
        statement: Wrapper<Statement>,
        connection: Wrapper<Arc<Connection>>,
    ) -> LibsqlStatement {
        LibsqlStatement {
            inner: RustAutoOpaqueNom::new(statement),
            connection: RustAutoOpaqueNom::new(connection),
        }
    }

    pub async fn reset(&mut self) {
        self.inner.try_write().unwrap().inner.reset();
    }

    pub async fn query(&mut self, params: Params) -> QueryResult {
        let _rows = self.run(params).await;
        let mut columns: Vec<String> = Vec::new();
        let col_count = self.inner.try_write().unwrap().num_columns();
        for i in 0..col_count {
            let name = self
                .inner
                .try_read()
                .unwrap()
                .get_column_name(i)
                .into_owned();
            columns.push(name);
        }
        let mut rows: Vec<HashMap<String, ReturnValue>> = Vec::new();
        for _row in _rows {
            let mut row: HashMap<String, ReturnValue> = HashMap::new();
            for idx in 0.._row.len() as i32 {
                row.insert(
                    columns[idx as usize].clone(),
                    _row[idx as usize].clone().into(),
                );
            }
            rows.push(row);
        }
        let rows_affected = self.connection.try_read().unwrap().total_changes() as u64;
        let last_insert_rowid = self.connection.try_read().unwrap().last_insert_rowid();
        QueryResult {
            rows,
            columns,
            rows_affected,
            last_insert_rowid,
        }
    }

    pub async fn execute(&mut self, params: Params) -> ExecuteResult {
        let _rows = self.run(params).await;
        let rows_affected = self.connection.try_read().unwrap().total_changes() as u64;
        ExecuteResult { rows_affected }
    }

    async fn run(&mut self, params: Params) -> Vec<Vec<Value>> {
        match params {
            Params::None => (),
            Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    self.inner
                        .try_write()
                        .unwrap()
                        .inner
                        .bind_at(NonZero::new(i + 1).unwrap(), value.into());
                }
            }
            Params::Named(_items) => todo!(),
        }
        let mut rows: Vec<Vec<Value>> = Vec::new();
        loop {
            let mut inner = self.inner.try_write().unwrap();
            match inner.inner.step() {
                Ok(turso_core::StepResult::Row) => {
                    let row = inner.row().unwrap();
                    rows.push(row.get_values().cloned().collect());
                }
                Ok(turso_core::StepResult::Done) => {
                    break;
                }
                Ok(turso_core::StepResult::IO) => {
                    self.inner.try_read().unwrap().run_once().unwrap();
                }
                _ => break,
            };
        }
        rows
    }
}
