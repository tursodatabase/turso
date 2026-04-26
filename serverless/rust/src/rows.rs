use crate::{Column, Error, Result, Value};

/// A result set returned from a query.
pub struct Rows {
    pub(crate) columns: Vec<Column>,
    pub(crate) rows: Vec<Row>,
    pub(crate) index: usize,
}

impl Rows {
    pub(crate) fn new(columns: Vec<Column>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows,
            index: 0,
        }
    }

    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<&Row>> {
        if self.index < self.rows.len() {
            let row = &self.rows[self.index];
            self.index += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    /// Returns the number of columns in the result set.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns the name of the column at the given index.
    pub fn column_name(&self, idx: usize) -> Result<String> {
        if idx >= self.columns.len() {
            return Err(Error::Misuse(format!(
                "column index {idx} out of bounds (result has {} columns)",
                self.columns.len()
            )));
        }
        Ok(self.columns[idx].name().to_string())
    }

    /// Returns the names of all columns in the result set.
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name().to_string()).collect()
    }

    /// Returns the index of the column with the given name.
    pub fn column_index(&self, name: &str) -> Result<usize> {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name().eq_ignore_ascii_case(name) {
                return Ok(i);
            }
        }
        Err(Error::Misuse(format!(
            "column '{name}' not found in result set"
        )))
    }

    /// Returns columns of the result set.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

/// Query result row.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub(crate) values: Vec<Value>,
}

impl Row {
    pub(crate) fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Get the value at the given column index.
    pub fn get_value(&self, idx: usize) -> Result<Value> {
        self.values.get(idx).cloned().ok_or_else(|| {
            Error::Misuse(format!(
                "column index {idx} out of bounds (row has {} columns)",
                self.values.len()
            ))
        })
    }

    /// Get a typed value from a column index.
    pub fn get<T: FromRemoteValue>(&self, idx: usize) -> Result<T> {
        let val = self.values.get(idx).ok_or_else(|| {
            Error::Misuse(format!(
                "column index {idx} out of bounds (row has {} columns)",
                self.values.len()
            ))
        })?;
        T::from_value(val.clone())
    }

    /// Returns the number of columns in this row.
    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}

/// Trait for converting a `Value` into a Rust type.
pub trait FromRemoteValue: Sized {
    fn from_value(value: Value) -> Result<Self>;
}

impl FromRemoteValue for Value {
    fn from_value(value: Value) -> Result<Self> {
        Ok(value)
    }
}

impl FromRemoteValue for i32 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Integer(n) => Ok(n as i32),
            _ => Err(Error::ConversionFailure(format!(
                "expected integer, got {value:?}"
            ))),
        }
    }
}

impl FromRemoteValue for i64 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Integer(n) => Ok(n),
            _ => Err(Error::ConversionFailure(format!(
                "expected integer, got {value:?}"
            ))),
        }
    }
}

impl FromRemoteValue for f64 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Real(n) => Ok(n),
            Value::Integer(n) => Ok(n as f64),
            _ => Err(Error::ConversionFailure(format!(
                "expected float, got {value:?}"
            ))),
        }
    }
}

impl FromRemoteValue for String {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s),
            _ => Err(Error::ConversionFailure(format!(
                "expected text, got {value:?}"
            ))),
        }
    }
}

impl FromRemoteValue for Vec<u8> {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Blob(b) => Ok(b),
            _ => Err(Error::ConversionFailure(format!(
                "expected blob, got {value:?}"
            ))),
        }
    }
}

impl FromRemoteValue for bool {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Integer(n) => Ok(n != 0),
            _ => Err(Error::ConversionFailure(format!(
                "expected integer for bool, got {value:?}"
            ))),
        }
    }
}

impl<T: FromRemoteValue> FromRemoteValue for Option<T> {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            other => Ok(Some(T::from_value(other)?)),
        }
    }
}
