use crate::{Error, Result, Statement, Value};
use std::fmt::Debug;

/// Results of a prepared statement query.
pub struct Rows {
    inner: Statement,
    columns: usize,
}

impl Rows {
    pub(crate) fn new(inner: Statement) -> Self {
        let columns = inner.inner.lock().unwrap().column_count();
        Self { inner, columns }
    }
    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        self.inner.step(Some(self.columns)).await
    }
}

/// Query result row.
#[derive(Debug, PartialEq)]
pub struct Row {
    pub(crate) values: Vec<turso_sdk_kit::rsapi::Value>,
}

impl Row {
    pub fn get_value(&self, idx: usize) -> Result<Value> {
        let val = self.values.get(idx).ok_or_else(|| {
            Error::Misuse(format!(
                "column index {idx} out of bounds (row has {} columns)",
                self.values.len()
            ))
        })?;
        match val {
            turso_sdk_kit::rsapi::Value::Integer(i) => Ok(Value::Integer(*i)),
            turso_sdk_kit::rsapi::Value::Null => Ok(Value::Null),
            turso_sdk_kit::rsapi::Value::Float(f) => Ok(Value::Real(*f)),
            turso_sdk_kit::rsapi::Value::Text(text) => Ok(Value::Text(text.to_string())),
            turso_sdk_kit::rsapi::Value::Blob(items) => Ok(Value::Blob(items.to_vec())),
        }
    }

    pub fn get<T>(&self, idx: usize) -> Result<T>
    where
        T: turso_sdk_kit::rsapi::FromValue,
    {
        let val = self.values.get(idx).ok_or_else(|| {
            Error::Misuse(format!(
                "column index {idx} out of bounds (row has {} columns)",
                self.values.len()
            ))
        })?;
        T::from_sql(val.clone()).map_err(|err| Error::ConversionFailure(err.to_string()))
    }

    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}
