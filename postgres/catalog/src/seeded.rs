//! A catalog as a seeded table: a table that has a fixed set of rows, the
//! seed, before anything is stored.
//!
//! The PostgreSQL frontend serves the seed rows of a catalog from memory,
//! with a virtual table that it registers under the name of the catalog.
//! `create_sql` and `index_sql` are for the statement that stores the
//! catalog.

use turso_core::Value;

/// A catalog with its SQL and its built-in rows.
pub struct SeededTable {
    /// The name of the catalog, such as `pg_proc`.
    pub name: &'static str,
    /// The `CREATE TABLE` statement of the catalog, in SQLite syntax. It
    /// gives the columns of the rows of `seed`.
    pub create_sql: &'static str,
    /// The `CREATE INDEX` statements of the stored catalog, in SQLite
    /// syntax.
    pub index_sql: &'static [&'static str],
    /// The built-in rows of the catalog.
    pub seed: &'static dyn Seed,
}

/// The built-in rows of a [`SeededTable`]. A row has an index from 0 to
/// `len() - 1`, and the rows are in index order.
///
/// The rowid of a row is the value of the column that the table's
/// `create_sql` declares `INTEGER PRIMARY KEY`. When no column is declared
/// so, the rowid of the row at `index` is `index + 1`. Rowids are unique.
pub trait Seed: Send + Sync {
    /// The number of rows.
    fn len(&self) -> usize;

    /// Whether the seed has no rows.
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
}
