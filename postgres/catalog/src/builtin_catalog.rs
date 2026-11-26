//! A catalog with its built-in rows: the rows that it has before anything
//! is stored.
//!
//! The PostgreSQL frontend serves the built-in rows of a catalog from memory,
//! with a virtual table that it registers under the name of the catalog.
//! `create_sql` and `index_sql` are for the statement that stores the
//! catalog.

use std::ops::Range;

use turso_core::Value;

use crate::Oid;

/// A system catalog of PostgreSQL: its SQL and its built-in rows. The
/// generator writes the impl of each catalog, from `postgres.bki`, so
/// nothing of the file is copied by hand.
///
/// The PostgreSQL frontend serves a catalog as a virtual table. Its rows
/// are the built-in rows of [`ROWS`](CatalogTable::ROWS), which come from
/// the binary that runs, and the rows that statements stored.
pub trait CatalogTable: 'static {
    /// The name of the catalog, such as `pg_description`.
    const NAME: &'static str;
    /// The oid of the catalog, its row in `pg_class`, and the `tableoid`
    /// of its rows: the number of its `create` line in `postgres.bki`.
    const OID: Oid;
    /// The `CREATE TABLE` statement of the catalog, in SQLite syntax.
    const CREATE_SQL: &'static str;
    /// The `CREATE INDEX` statements of the catalog, in SQLite syntax.
    const INDEX_SQL: &'static [&'static str];
    /// The built-in rows of the catalog.
    const ROWS: &'static dyn BuiltinRows;
}

/// A catalog with its SQL and its built-in rows.
pub struct BuiltinCatalog {
    /// The name of the catalog, such as `pg_proc`.
    pub name: &'static str,
    /// The `CREATE TABLE` statement of the catalog, in SQLite syntax. It
    /// gives the columns of the rows of `rows`.
    pub create_sql: &'static str,
    /// The `CREATE INDEX` statements of the stored catalog, in SQLite
    /// syntax.
    pub index_sql: &'static [&'static str],
    /// The built-in rows of the catalog.
    pub rows: &'static dyn BuiltinRows,
}

/// The built-in rows of a [`BuiltinCatalog`]. A row has an index from 0 to
/// `len() - 1`, and the rows are in index order.
///
/// The rowid of a row is the value of the column that the table's
/// `create_sql` declares `INTEGER PRIMARY KEY`. When no column is declared
/// so, the rowid of the row at `index` is `index + 1`. Rowids are unique.
pub trait BuiltinRows: Send + Sync {
    /// The number of rows.
    fn len(&self) -> usize;

    /// Whether there are no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Writes the values of the row at `index`, in column order, to the end
    /// of `out`. The caller gives an empty `out`. When the table has an
    /// `INTEGER PRIMARY KEY` column, the value of that column is an integer.
    fn row(&self, index: usize, out: &mut Vec<Value>);

    /// The position of the `INTEGER PRIMARY KEY` column in a row, or None
    /// when the table has no such column.
    fn rowid_column(&self) -> Option<usize>;

    /// The index of the row whose rowid is `rowid`, or None when no row has
    /// it, so that `WHERE oid = ...` does not scan.
    fn find(&self, rowid: i64) -> Option<usize>;

    /// Changes when the rows change.
    fn version(&self) -> u32;

    /// The positions of the columns that identify a row, in the order of
    /// the rows: the `oid`, or the columns of a unique index of the
    /// catalog whose columns are all integers. Empty when the catalog has
    /// no such index, and its rows are then in no order.
    fn key_columns(&self) -> &'static [usize];

    /// The indexes of the rows whose first key columns have the values
    /// `prefix`, so that a lookup by key does not scan. An empty prefix
    /// gives all rows. `prefix` has at most as many values as the key
    /// has columns.
    fn key_range(&self, prefix: &[i64]) -> Range<usize>;
}
