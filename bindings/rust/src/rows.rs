use crate::{Error, Result, Value};
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::Poll;

/// Results of a prepared statement query.
pub struct Rows {
    inner: Arc<Mutex<Box<turso_sdk_kit::rsapi::TursoStatement>>>,
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
    pub(crate) fn new(inner: Arc<Mutex<Box<turso_sdk_kit::rsapi::TursoStatement>>>) -> Self {
        Self { inner }
    }
    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        struct Next {
            columns: usize,
            stmt: Arc<Mutex<Box<turso_sdk_kit::rsapi::TursoStatement>>>,
        }

        impl Future for Next {
            type Output = Result<Option<Row>>;

            fn poll(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                let mut stmt = self.stmt.lock().unwrap();
                match stmt.step(Some(cx.waker()))? {
                    turso_sdk_kit::rsapi::TursoStatusCode::Row => {
                        let mut values = Vec::with_capacity(self.columns);
                        for i in 0..self.columns {
                            let value = stmt.row_value(i)?;
                            values.push(value.to_owned());
                        }
                        Poll::Ready(Ok(Some(Row { values })))
                    }
                    turso_sdk_kit::rsapi::TursoStatusCode::Done => Poll::Ready(Ok(None)),
                    turso_sdk_kit::rsapi::TursoStatusCode::Io => {
                        stmt.run_io()?;
                        Poll::Pending
                    }
                }
            }
        }

        unsafe impl Send for Next {}

        let next = Next {
            columns: self.inner.lock().unwrap().column_count(),
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

unsafe impl Send for Row {}
unsafe impl Sync for Row {}

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
