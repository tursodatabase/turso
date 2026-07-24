use std::collections::VecDeque;

use crate::value::FromValue;
use crate::{Column, Error, Result, Value};

/// Results of a query.
///
/// The result set is buffered: the server streams it over the cursor
/// endpoint and the driver collects it before returning, so `next()` never
/// performs I/O. This matches the JavaScript serverless driver, where
/// shared-connection iteration is buffered as well.
pub struct Rows {
    columns: Vec<Column>,
    rows: VecDeque<Row>,
}

impl Rows {
    pub(crate) fn new(columns: Vec<Column>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows: rows.into(),
        }
    }

    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        Ok(self.rows.pop_front())
    }

    /// Returns the number of columns in the result set.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns the name of the column at the given index.
    pub fn column_name(&self, idx: usize) -> Result<String> {
        self.columns
            .get(idx)
            .map(|c| c.name().to_string())
            .ok_or_else(|| {
                Error::Misuse(format!(
                    "column index {idx} out of bounds (result has {} columns)",
                    self.columns.len()
                ))
            })
    }

    /// Returns the names of all columns in the result set.
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name().to_string()).collect()
    }

    /// Returns the index of the column with the given name.
    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::Misuse(format!("column '{name}' not found in result set")))
    }

    /// Returns columns of the result set.
    pub fn columns(&self) -> Vec<Column> {
        self.columns.clone()
    }
}

/// Query result row.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    values: Vec<Value>,
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
    pub fn get<T: FromValue>(&self, idx: usize) -> Result<T> {
        T::from_value(self.get_value(idx)?)
    }

    /// Returns the number of columns in this row.
    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}
