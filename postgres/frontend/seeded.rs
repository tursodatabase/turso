//! Catalogs that serve the built-in rows of PostgreSQL 18 from memory.
//!
//! `turso_pg_catalog` has each catalog as a [`SeededTable`]: its
//! `CREATE TABLE` statement and its built-in rows, the seed. A
//! [`SeededCatalog`] is a virtual table over one of them. It reads a row
//! from the seed when a statement asks for it, and stores nothing.
//!
//! [`SEEDED_CATALOGS`] lists the catalogs. The first statement that writes
//! to a catalog can store it: [`statements_that_write`] puts the statements
//! that create and fill the table before the statements of the write.

use std::sync::{Arc, LazyLock};

use turso_core::native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule};
use turso_core::schema::Schema;
use turso_core::types::IOResultOr;
use turso_core::{
    Connection, IOResult, LimboError, NamespaceId, Numeric, OpenOptions, Result, Value,
};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode, VTabKind,
};
use turso_pg_catalog::SeededTable;

/// `idx_num` of a scan of all rows.
const SCAN: i32 = 0;
/// `idx_num` of a lookup of the row whose rowid is the first argument.
const FIND: i32 = 1;

/// A catalog that has the rows of a seed, and its virtual table. The table
/// can have the hidden system column `tableoid` after the columns of the
/// seed.
#[derive(Clone, Copy)]
pub(crate) struct SeededCatalog {
    table: &'static SeededTable,
    /// The oid of the catalog in `pg_class`, which is the `tableoid` of
    /// each row. It is the number of the `create` line of the catalog in
    /// `postgres.bki`. None for a catalog without the column: a catalog
    /// that a statement can store has the columns of the stored table and
    /// no other.
    table_oid: Option<i64>,
    /// Whether the first statement that writes to the catalog stores it.
    /// The stored table then has the name in place of the virtual table.
    /// A catalog that no statement writes is never stored.
    first_write_stores: bool,
}

/// The seeded table of `pg_collation`. A seeded table is a static, so that
/// its catalog can refer to it for the life of the process.
static PG_COLLATION: SeededTable = turso_pg_catalog::PG_COLLATION_SEEDED_TABLE;
/// The seeded table of `pg_description`.
static PG_DESCRIPTION: SeededTable = turso_pg_catalog::PG_DESCRIPTION_SEEDED_TABLE;

/// Each catalog that serves the built-in rows of PostgreSQL 18. To add a
/// catalog, add its line.
static SEEDED_CATALOGS: [SeededCatalog; 2] = [
    // `create pg_collation 3456` in `postgres.bki`. No statement writes it.
    SeededCatalog {
        table: &PG_COLLATION,
        table_oid: Some(3456),
        first_write_stores: false,
    },
    // COMMENT ON writes it.
    SeededCatalog {
        table: &PG_DESCRIPTION,
        table_oid: None,
        first_write_stores: true,
    },
];

/// Registers the virtual table of each seeded catalog under the name of the
/// catalog.
pub(crate) fn register_seeded_catalogs(mut options: OpenOptions) -> OpenOptions {
    for catalog in &SEEDED_CATALOGS {
        options =
            options.native_module(catalog.table.name, VTabKind::TableValuedFunction, *catalog);
    }
    options
}

/// The names of the catalogs that the first write stores, for
/// `Catalog::seeded_tables`: core lets a stored table of one of these names
/// replace the virtual table.
pub(crate) fn names_of_catalogs_that_a_write_stores() -> &'static [&'static str] {
    static NAMES: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
        SEEDED_CATALOGS
            .iter()
            .filter(|catalog| catalog.first_write_stores)
            .map(|catalog| catalog.table.name)
            .collect()
    });
    &NAMES
}

/// Whether the seeded catalog `name` is stored in the database of `schema`.
pub(crate) fn is_stored(schema: &Schema, name: &str) -> bool {
    schema.get_btree_table(NamespaceId::ROOT, name).is_some()
}

/// `statements`, which write to the seeded catalog `name`, after the engine
/// statements that store the catalog when it is not stored. The first
/// statement that writes to a catalog so stores it, in its own transaction.
pub(crate) fn statements_that_write(
    schema: &Schema,
    name: &str,
    statements: Vec<String>,
) -> Result<Vec<String>> {
    if is_stored(schema, name) {
        return Ok(statements);
    }
    let Some(catalog) = SEEDED_CATALOGS
        .iter()
        .find(|catalog| catalog.first_write_stores && catalog.table.name == name)
    else {
        return Err(LimboError::InternalError(format!(
            "{name} is not a catalog that a write stores"
        )));
    };
    let mut all = store_statements(catalog.table)?;
    all.extend(statements);
    Ok(all)
}

/// The number of rows that one INSERT statement of [`store_statements`]
/// has.
const ROWS_IN_ONE_INSERT: usize = 500;

/// The engine statements that store `table`: they create the table, insert
/// the rows of the seed, and create the indexes. The indexes come after the
/// rows, so that each index is built one time from all rows.
fn store_statements(table: &SeededTable) -> Result<Vec<String>> {
    let seed = table.seed;
    let mut statements = vec![table.create_sql.to_string()];
    let mut row = Vec::new();
    let mut rows = Vec::with_capacity(ROWS_IN_ONE_INSERT);
    for index in 0..seed.len() {
        row.clear();
        seed.row(index, &mut row);
        let values = row
            .iter()
            .map(sql_literal)
            .collect::<Result<Vec<String>>>()?;
        rows.push(format!("({})", values.join(", ")));
        if rows.len() == ROWS_IN_ONE_INSERT || index + 1 == seed.len() {
            statements.push(format!(
                "INSERT INTO {} VALUES {}",
                table.name,
                rows.join(", ")
            ));
            rows.clear();
        }
    }
    statements.extend(table.index_sql.iter().map(|sql| sql.to_string()));
    Ok(statements)
}

/// `value` as a literal of SQLite.
fn sql_literal(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => "NULL".to_string(),
        Value::Numeric(Numeric::Integer(integer)) => integer.to_string(),
        Value::Numeric(Numeric::Float(real)) => format!("{:?}", f64::from(*real)),
        Value::Text(text) => format!("'{}'", text.as_str().replace('\'', "''")),
        Value::Blob(_) => {
            return Err(LimboError::InternalError(
                "a seed row has a blob, which no catalog of PostgreSQL has".to_string(),
            ))
        }
    })
}

impl std::fmt::Debug for SeededCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeededCatalog")
            .field("name", &self.table.name)
            .finish()
    }
}

impl VirtualTableModule for SeededCatalog {
    type Table = Self;

    /// The `CREATE TABLE` statement of the seeded table, with the column
    /// `tableoid` after its columns.
    fn schema(&self, _args: &[Value]) -> Result<String> {
        let Some(columns) = self.table.create_sql.strip_suffix(')') else {
            return Err(LimboError::InternalError(format!(
                "the CREATE TABLE of {} does not end with its columns",
                self.table.name
            )));
        };
        Ok(match self.table_oid {
            Some(_) => format!("{columns}, tableoid INTEGER HIDDEN)"),
            None => self.table.create_sql.to_string(),
        })
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(*self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for SeededCatalog {
    type Cursor = SeededCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(SeededCursor {
            catalog: *self,
            position: Position::Done,
            row: Vec::new(),
        })
    }

    /// Uses an equality on the `INTEGER PRIMARY KEY` column, which is the
    /// `oid` of a catalog, to find one row. The constraint is not omitted:
    /// the cursor can return a row that does not match, for example when
    /// the value is text, and VDBE then compares the value with SQL rules.
    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        let mut constraint_usages = vec![
            ConstraintUsage {
                argv_index: None,
                omit: false,
            };
            constraints.len()
        ];
        let seed = self.table.seed;
        let rowid_equality = seed.rowid_column().and_then(|rowid_column| {
            constraints.iter().position(|constraint| {
                constraint.usable
                    && constraint.op == ConstraintOp::Eq
                    && constraint.column_index as usize == rowid_column
            })
        });
        if let Some(position) = rowid_equality {
            constraint_usages[position].argv_index = Some(1);
            return Ok(IndexInfo {
                idx_num: FIND,
                idx_str: None,
                order_by_consumed: false,
                estimated_cost: 1.0,
                estimated_rows: 1,
                constraint_usages,
            });
        }
        let len = seed.len();
        Ok(IndexInfo {
            idx_num: SCAN,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: len as f64,
            estimated_rows: u32::try_from(len).unwrap_or(u32::MAX - 1),
            constraint_usages,
        })
    }
}

/// Where a [`SeededCursor`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    /// At the row with this index, in a scan of all rows.
    Scan(usize),
    /// At the one row that a lookup found.
    Found(usize),
    /// After the last row.
    Done,
}

/// A cursor over the rows of a seed. It reads one row at a time.
pub(crate) struct SeededCursor {
    catalog: SeededCatalog,
    position: Position,
    /// The values of the current row, without `tableoid`.
    row: Vec<Value>,
}

impl SeededCursor {
    /// Moves to the row at `index` and reads its values.
    fn read_row(&mut self, index: usize, position: Position) {
        self.row.clear();
        self.catalog.table.seed.row(index, &mut self.row);
        self.position = position;
    }

    /// Starts a scan at the first row.
    fn scan_from_start(&mut self) -> bool {
        if self.catalog.table.seed.is_empty() {
            self.position = Position::Done;
            return false;
        }
        self.read_row(0, Position::Scan(0));
        true
    }

    /// Moves to the row whose rowid is `rowid`, if there is one.
    fn find(&mut self, rowid: i64) -> bool {
        let Some(index) = self.catalog.table.seed.find(rowid) else {
            self.position = Position::Done;
            return false;
        };
        self.read_row(index, Position::Found(index));
        true
    }
}

impl VirtualTableCursor for SeededCursor {
    fn filter(&mut self, args: &[Value], _idx_str: Option<&str>, idx_num: i32) -> IOResultOr<bool> {
        let has_row = match (idx_num, args) {
            (FIND, [key]) => match key_of(key) {
                Key::Rowid(rowid) => self.find(rowid),
                Key::NoRow => {
                    self.position = Position::Done;
                    false
                }
                Key::AnyRow => self.scan_from_start(),
            },
            (SCAN, _) => self.scan_from_start(),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "{} has no plan {idx_num} with {} arguments",
                    self.catalog.table.name,
                    args.len()
                ))
                .into())
            }
        };
        Ok(IOResult::Done(has_row))
    }

    fn next(&mut self) -> IOResultOr<bool> {
        let has_row = match self.position {
            Position::Scan(index) if index + 1 < self.catalog.table.seed.len() => {
                self.read_row(index + 1, Position::Scan(index + 1));
                true
            }
            Position::Scan(_) | Position::Found(_) | Position::Done => {
                self.position = Position::Done;
                false
            }
        };
        Ok(IOResult::Done(has_row))
    }

    /// The rowid of the current row: the value of its `INTEGER PRIMARY KEY`
    /// column, or its index plus 1 when the catalog has no such column.
    fn rowid(&self) -> i64 {
        let index = match self.position {
            Position::Scan(index) | Position::Found(index) => index,
            Position::Done => return 0,
        };
        match self.catalog.table.seed.rowid_column() {
            Some(column) => match &self.row[column] {
                Value::Numeric(Numeric::Integer(rowid)) => *rowid,
                other => unreachable!(
                    "the INTEGER PRIMARY KEY of row {index} of {} is {other:?}",
                    self.catalog.table.name
                ),
            },
            None => index as i64 + 1,
        }
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        let value = match self.position {
            Position::Done => Value::Null,
            // `tableoid` is the column after the columns of the seed.
            _ if column == self.row.len() => match self.catalog.table_oid {
                Some(table_oid) => Value::from_i64(table_oid),
                None => Value::Null,
            },
            _ => self.row[column].clone(),
        };
        Ok(IOResult::Done(value))
    }
}

/// What the argument of a lookup asks for.
enum Key {
    /// The row with this rowid.
    Rowid(i64),
    /// No row: no integer is equal to the value.
    NoRow,
    /// Any row. VDBE compares each row with the value.
    AnyRow,
}

/// The rowid that `value` can be equal to. An integer column is equal only
/// to an integer, or to a real with the same value. A text or blob value can
/// be equal after the comparison converts it, so the cursor then scans and
/// VDBE decides.
fn key_of(value: &Value) -> Key {
    match value {
        Value::Null => Key::NoRow,
        Value::Numeric(Numeric::Integer(rowid)) => Key::Rowid(*rowid),
        Value::Numeric(Numeric::Float(real)) => {
            let real = f64::from(*real);
            // -2^63 and 2^63 are exact as f64. Every real in this range
            // that has no fraction is an i64.
            if real.fract() == 0.0
                && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&real)
            {
                Key::Rowid(real as i64)
            } else {
                Key::NoRow
            }
        }
        Value::Text(_) | Value::Blob(_) => Key::AnyRow,
    }
}
