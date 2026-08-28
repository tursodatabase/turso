//! Parameterized statement batches.
//!
//! A batch executes multiple statements — each with its own bind
//! parameters — in order, stopping at the first failure. See
//! [`Connection::batch`](crate::Connection::batch) and
//! [`Connection::transactional_batch`](crate::Connection::transactional_batch).

use crate::{
    params::{IntoParams, Params},
    rows::Row,
    Column, Result,
};

mod sealed {
    pub trait Sealed {}
}

use sealed::Sealed;

/// One statement of a batch, with its bind parameters.
///
/// Usually built implicitly from a SQL string or a `(sql, params)` pair
/// (see [`IntoBatchStatement`]). Build `BatchStatement`s explicitly to mix
/// parameter shapes in one batch, since the elements of a single array or
/// `Vec` must share one type:
///
/// ```rust
/// # fn build() -> turso::Result<Vec<turso::BatchStatement>> {
/// use turso::BatchStatement;
/// Ok(vec![
///     BatchStatement::new("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())?,
///     BatchStatement::new("INSERT INTO t (v) VALUES (?1)", ("x",))?,
/// ])
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct BatchStatement {
    pub(crate) sql: String,
    pub(crate) params: Params,
}

impl BatchStatement {
    /// Create a batch statement from SQL text and parameters, accepting the
    /// same parameter forms as [`Connection::execute`](crate::Connection::execute).
    pub fn new(sql: impl Into<String>, params: impl IntoParams) -> Result<Self> {
        Ok(Self {
            sql: sql.into(),
            params: params.into_params()?,
        })
    }
}

/// Converts some type into one statement of a batch.
///
/// Implemented for:
///
/// - SQL strings without parameters: `"DELETE FROM t"`.
/// - `(sql, params)` pairs, with the same parameter forms as
///   [`Connection::execute`](crate::Connection::execute):
///   `("INSERT INTO t (v) VALUES (?1)", ("x",))`.
/// - [`BatchStatement`], for batches that mix parameter shapes.
pub trait IntoBatchStatement: Sealed {
    #[doc(hidden)]
    fn into_batch_statement(self) -> Result<BatchStatement>;
}

impl Sealed for BatchStatement {}
impl IntoBatchStatement for BatchStatement {
    fn into_batch_statement(self) -> Result<BatchStatement> {
        Ok(self)
    }
}

impl Sealed for &str {}
impl IntoBatchStatement for &str {
    fn into_batch_statement(self) -> Result<BatchStatement> {
        BatchStatement::new(self, ())
    }
}

impl Sealed for String {}
impl IntoBatchStatement for String {
    fn into_batch_statement(self) -> Result<BatchStatement> {
        BatchStatement::new(self, ())
    }
}

impl<S: Into<String>, P: IntoParams> Sealed for (S, P) {}
impl<S: Into<String>, P: IntoParams> IntoBatchStatement for (S, P) {
    fn into_batch_statement(self) -> Result<BatchStatement> {
        BatchStatement::new(self.0, self.1)
    }
}

/// The result of one statement of a batch.
#[derive(Debug)]
pub struct BatchResult {
    columns: Vec<Column>,
    rows: Vec<Row>,
    rows_affected: u64,
    last_insert_rowid: Option<i64>,
    rows_read: Option<u64>,
    rows_written: Option<u64>,
    query_duration_ms: Option<f64>,
}

impl BatchResult {
    pub(crate) fn new(
        columns: Vec<Column>,
        rows: Vec<Row>,
        rows_affected: u64,
        last_insert_rowid: Option<i64>,
    ) -> Self {
        Self {
            columns,
            rows,
            rows_affected,
            last_insert_rowid,
            rows_read: None,
            rows_written: None,
            query_duration_ms: None,
        }
    }

    /// Returns the columns of the statement's result set.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the rows returned by the statement.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Returns the number of rows changed by the statement.
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Returns the rowid inserted by the statement, when the statement was
    /// an INSERT into a table with a rowid.
    pub fn last_insert_rowid(&self) -> Option<i64> {
        self.last_insert_rowid
    }

    /// Returns the number of rows read while executing the statement.
    /// The embedded engine does not report this counter per statement,
    /// so this is `None`; the serverless driver reports the server's
    /// value.
    pub fn rows_read(&self) -> Option<u64> {
        self.rows_read
    }

    /// Returns the number of rows written while executing the statement.
    /// The embedded engine does not report this counter per statement,
    /// so this is `None`; the serverless driver reports the server's
    /// value.
    pub fn rows_written(&self) -> Option<u64> {
        self.rows_written
    }

    /// Returns the execution time of the statement in milliseconds. The
    /// embedded engine does not report this per statement, so this is
    /// `None`; the serverless driver reports the server's value.
    pub fn query_duration_ms(&self) -> Option<f64> {
        self.query_duration_ms
    }
}
