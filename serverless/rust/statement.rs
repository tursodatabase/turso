use crate::connection::Connection;
use crate::params::IntoParams;
use crate::rows::{Row, Rows};
use crate::{Column, Error, Result};

/// A prepared statement.
///
/// The server does not keep prepared statement state across HTTP requests;
/// preparing describes the statement (validating it and fetching column
/// metadata) and each execution sends the SQL text.
#[derive(Clone)]
pub struct Statement {
    conn: Connection,
    sql: String,
    columns: Vec<Column>,
    last_n_change: u64,
}

impl Statement {
    pub(crate) fn new(conn: Connection, sql: String, columns: Vec<Column>) -> Self {
        Self {
            conn,
            sql,
            columns,
            last_n_change: 0,
        }
    }

    /// Query the statement with the given parameters, returning the result rows.
    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        self.conn.query(&self.sql, params).await
    }

    /// Execute the statement with the given parameters, returning the number
    /// of rows affected.
    pub async fn execute(&mut self, params: impl IntoParams) -> Result<u64> {
        let n = self.conn.execute(&self.sql, params).await?;
        self.last_n_change = n;
        Ok(n)
    }

    /// Execute a query that returns the first row.
    ///
    /// Returns [`Error::QueryReturnedNoRows`] if the query returns no rows.
    pub async fn query_row(&mut self, params: impl IntoParams) -> Result<Row> {
        let mut rows = self.query(params).await?;
        match rows.next().await? {
            Some(row) => Ok(row),
            None => Err(Error::QueryReturnedNoRows),
        }
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
                    "column index {idx} out of bounds (statement has {} columns)",
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
            .ok_or_else(|| Error::Misuse(format!("no column named '{name}'")))
    }

    /// Returns columns of the result set.
    pub fn columns(&self) -> Vec<Column> {
        self.columns.clone()
    }

    /// Returns the number of rows modified by the most recent execution.
    pub fn n_change(&self) -> u64 {
        self.last_n_change
    }

    /// Reset the statement. A no-op for the serverless driver, provided for
    /// compatibility with the embedded driver.
    pub fn reset(&self) -> Result<()> {
        Ok(())
    }
}
