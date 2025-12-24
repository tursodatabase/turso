use crate::{Error, Result, Statement, Value};
use std::fmt::Debug;
use std::future::Future;

/// Results of a prepared statement query.
pub struct Rows {
    inner: Statement,
}

impl Rows {
    pub(crate) fn new(inner: Statement) -> Self {
        Self { inner }
    }
    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        struct Next {
            columns: usize,
            stmt: Statement,
        }

        impl Future for Next {
            type Output = Result<Option<Row>>;

            fn poll(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                self.stmt.step(Some(self.columns), cx)
            }
        }

        unsafe impl Send for Next {}

        let next = Next {
            columns: self.inner.inner.lock().unwrap().column_count(),
            stmt: self.inner.clone(),
        };

        next.await
    }
}

/// Query result row.
#[derive(Debug)]
pub struct Row {
    pub(crate) values: Vec<turso_sdk_kit::rsapi::Value>,
}

impl Row {
    pub fn get_value(&self, index: usize) -> Result<Value> {
        let value = &self.values[index];
        match value {
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
        let val = &self.values[idx];
        T::from_sql(val.clone()).map_err(|err| Error::ConversionFailure(err.to_string()))
    }

    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}
