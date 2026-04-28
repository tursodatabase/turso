use crate::{Column, Error, Params, Result};

use crate::connection::Connection;
use crate::rows::{Row, Rows};

/// A prepared statement for a remote connection.
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

    /// Execute the statement with the given parameters, returning the number of affected rows.
    pub async fn execute(&mut self, params: impl Into<Params>) -> Result<u64> {
        let n = self.conn.execute(&self.sql, params).await?;
        self.last_n_change = n;
        Ok(n)
    }

    /// Query with the given parameters, returning all result rows.
    pub async fn query(&mut self, params: impl Into<Params>) -> Result<Rows> {
        self.conn.query(&self.sql, params).await
    }

    /// Execute a query that returns the first [`Row`].
    ///
    /// Returns `QueryReturnedNoRows` if no rows were returned.
    pub async fn query_row(&mut self, params: impl Into<Params>) -> Result<Row> {
        let mut rows = self.query(params).await?;
        match rows.next().await? {
            Some(row) => Ok(row.clone()),
            None => Err(Error::QueryReturnedNoRows),
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
                "column index {idx} out of bounds (statement has {} columns)",
                self.columns.len()
            )));
        }
        Ok(self.columns[idx].name().to_string())
    }

    /// Returns the names of all columns in the result set.
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name().to_string()).collect()
    }

    /// Returns columns of the result set.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the index of the column with the given name (case-insensitive).
    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::Misuse(format!("no column named '{name}'")))
    }

    /// Returns the number of rows modified by the most recent execution.
    pub fn n_change(&self) -> u64 {
        self.last_n_change
    }

    /// Reset/no-op for remote statements (compatibility with turso API).
    pub fn reset(&self) -> Result<()> {
        Ok(())
    }
}
