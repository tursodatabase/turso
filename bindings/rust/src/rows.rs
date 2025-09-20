use turso_core::types::FromValue;

use crate::{Error, Result, Value};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Results of a prepared statement query.
pub struct Rows {
    inner: Arc<Mutex<turso_core::Statement>>,
}

impl Clone for Rows {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for Rows {}
unsafe impl Sync for Rows {}

impl Rows {
    pub(crate) fn new(inner: &Arc<Mutex<turso_core::Statement>>) -> Self {
        Self {
            inner: Arc::clone(inner),
        }
    }
    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        loop {
            let mut stmt = self
                .inner
                .lock()
                .map_err(|e| Error::MutexError(e.to_string()))?;
            match stmt.step()? {
                turso_core::StepResult::Row => {
                    let row = stmt.row().unwrap();
                    return Ok(Some(Row {
                        values: row.get_values().map(|v| v.to_owned()).collect(),
                    }));
                }
                turso_core::StepResult::Done => return Ok(None),
                turso_core::StepResult::IO => {
                    if let Err(e) = stmt.run_once() {
                        return Err(e.into());
                    }
                    continue;
                }
                turso_core::StepResult::Busy => {
                    return Err(Error::SqlExecutionFailure("database is locked".to_string()))
                }
                turso_core::StepResult::Interrupt => {
                    return Err(Error::SqlExecutionFailure("interrupted".to_string()))
                }
            }
        }
    }
}

/// Query result row.
#[derive(Debug)]
pub struct Row {
    values: Vec<turso_core::Value>,
}

unsafe impl Send for Row {}
unsafe impl Sync for Row {}

impl Row {
    pub fn get_value(&self, index: usize) -> Result<Value> {
        let value = &self.values[index];
        match value {
            turso_core::Value::Integer(i) => Ok(Value::Integer(*i)),
            turso_core::Value::Null => Ok(Value::Null),
            turso_core::Value::Float(f) => Ok(Value::Real(*f)),
            turso_core::Value::Text(text) => Ok(Value::Text(text.to_string())),
            turso_core::Value::Blob(items) => {
                if items.unalloc_bytes > 0 {
                    panic!(
                        "get_value called on unexpanded zeroblob with {} unallocated bytes",
                        items.unalloc_bytes
                    );
                }
                Ok(Value::Blob(items.value.clone()))
            }
        }
    }

    pub fn get<T>(&self, idx: usize) -> Result<T>
    where
        T: FromValue,
    {
        let val = &self.values[idx];
        T::from_sql(val.clone()).map_err(|err| Error::ConversionFailure(err.to_string()))
    }

    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}

impl<'a> FromIterator<&'a turso_core::Value> for Row {
    fn from_iter<T: IntoIterator<Item = &'a turso_core::Value>>(iter: T) -> Self {
        let values = iter
            .into_iter()
            .map(|v| match v {
                turso_core::Value::Integer(i) => turso_core::Value::Integer(*i),
                turso_core::Value::Null => turso_core::Value::Null,
                turso_core::Value::Float(f) => turso_core::Value::Float(*f),
                turso_core::Value::Text(s) => turso_core::Value::Text(s.clone()),
                turso_core::Value::Blob(b) => turso_core::Value::Blob(b.clone()),
            })
            .collect();

        Row { values }
    }
}
