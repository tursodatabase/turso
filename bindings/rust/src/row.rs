use std::sync::{Arc, Mutex};

use crate::Error;
use crate::Value;

/// Results of a prepared statement query.
#[must_use = "Rows is lazy and will do nothing unless consumed"]
pub struct Rows {
    pub(crate) inner: Arc<Mutex<turso_core::Statement>>,
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

#[cfg(not(feature = "futures"))]
impl Rows {
    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> crate::Result<Option<Row>> {
        loop {
            let mut stmt = self
                .inner
                .lock()
                .map_err(|e| Error::MutexError(e.to_string()))?;
            match stmt.step().inspect_err(|_| stmt.reset())? {
                turso_core::StepResult::Row => {
                    let row = stmt.row().unwrap();
                    return Ok(Some(Row {
                        values: row.get_values().map(|v| v.to_owned()).collect(),
                    }));
                }
                turso_core::StepResult::Done => {
                    stmt.reset();
                    return Ok(None);
                }
                turso_core::StepResult::IO => {
                    if let Err(e) = stmt.run_once() {
                        return Err(e.into());
                    }
                    continue;
                }
                // TODO: Busy should probably be an error
                turso_core::StepResult::Busy | turso_core::StepResult::Interrupt => {
                    stmt.reset();
                    return Ok(None);
                }
            }
        }
    }
}

#[cfg(feature = "futures")]
impl futures_util::Stream for Rows {
    type Item = crate::Result<Row>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let stmt = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()));

        if let Err(err) = stmt {
            return Poll::Ready(Some(Err(err)));
        }
        let mut stmt = stmt.unwrap();
        match stmt.step() {
            Ok(step_result) => match step_result {
                turso_core::StepResult::Row => {
                    let row = stmt.row().unwrap();
                    Poll::Ready(Some(Ok(Row {
                        values: row.get_values().map(|v| v.to_owned()).collect(),
                    })))
                }
                turso_core::StepResult::Done => {
                    stmt.reset();
                    Poll::Ready(None)
                }
                turso_core::StepResult::IO => {
                    if let Err(e) = stmt.run_once() {
                        return Poll::Ready(Some(Err(e.into())));
                    }
                    // TODO: see correct way to signal for this task to wake up
                    Poll::Pending
                }
                // TODO: Busy and Interrupt should probably return errors
                turso_core::StepResult::Busy | turso_core::StepResult::Interrupt => {
                    stmt.reset();
                    Poll::Ready(None)
                }
            },
            Err(err) => {
                stmt.reset();
                Poll::Ready(Some(Err(Error::from(err))))
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
    pub fn get_value(&self, index: usize) -> crate::Result<Value> {
        let value = &self.values[index];
        match value {
            turso_core::Value::Integer(i) => Ok(Value::Integer(*i)),
            turso_core::Value::Null => Ok(Value::Null),
            turso_core::Value::Float(f) => Ok(Value::Real(*f)),
            turso_core::Value::Text(text) => Ok(Value::Text(text.to_string())),
            turso_core::Value::Blob(items) => Ok(Value::Blob(items.to_vec())),
        }
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

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_row() {}
}
