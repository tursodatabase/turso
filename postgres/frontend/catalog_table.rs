//! The PostgreSQL catalogs that the frontend serves from a [`CatalogTable`]
//! of `turso_pg_catalog`.
//!
//! Each catalog is a stored table of the frontend with the name and the
//! indexes of the catalog in PostgreSQL. It holds only what statements
//! wrote. The table has an overlay of the same name (see
//! [`OpenOptions::table_overlay`]): a virtual table whose rows are the
//! built-in rows of the binary that runs, which nothing copies into the
//! database file, and the rows of the stored table. The statements of the
//! user read the overlay, and the statements of the engine read and write
//! the stored table. The two sources are a plain union: a statement cannot
//! change or remove a built-in row, and a statement that would is refused
//! (see [`Catalog::insert_statement`]).
//!
//! [`CATALOG_TABLES`] lists the catalogs. To serve a catalog this way, add
//! its line there.
//!
//! A lookup by the key of a catalog, which is its `oid` or the columns of a
//! unique index of PostgreSQL, reads only the matching rows of each source:
//! the built-in rows are found by binary search, and the stored rows by the
//! index of the table. The cursor reads the stored table with a statement of
//! the engine that runs inside the statement that reads the catalog, on the
//! same connection, so it sees the same transaction.

use std::num::NonZero;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

use turso_core::ast;
use turso_core::dialect::parse_statement;
use turso_core::native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule};
use turso_core::schema::{BTreeTable, Schema};
use turso_core::types::{IOCompletions, IOResultOr};
use turso_core::{
    Completion, Connection, IOResult, LimboError, NamespaceId, Numeric, OpenOptions, Result,
    Statement, StepResult, Value,
};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};
use turso_pg_catalog::{BuiltinRows, CatalogTable, PgDescription};
use turso_sqlite::SqliteDialect;

/// Each catalog that the frontend serves from its built-in rows and its
/// stored table.
static CATALOG_TABLES: LazyLock<[Catalog; 1]> = LazyLock::new(|| {
    [
        // COMMENT ON and the statements that drop an object with a comment
        // write it.
        Catalog::of::<PgDescription>(),
    ]
});

/// Registers the virtual table of each catalog of [`CATALOG_TABLES`] as
/// the overlay of its stored table.
pub(crate) fn register_catalog_tables(mut options: OpenOptions) -> OpenOptions {
    for catalog in CATALOG_TABLES.iter() {
        options = options.table_overlay(catalog.name(), CatalogVTab(catalog));
    }
    options
}

/// Whether `name` is the name of a catalog of [`CATALOG_TABLES`].
pub(crate) fn is_catalog_table(name: &str) -> bool {
    CATALOG_TABLES
        .iter()
        .any(|catalog| name.eq_ignore_ascii_case(catalog.name()))
}

/// The catalog `C` of [`CATALOG_TABLES`]. Fails when the list does not
/// have it, which is a bug of the frontend.
pub(crate) fn catalog<C: CatalogTable>() -> &'static Catalog {
    CATALOG_TABLES
        .iter()
        .find(|catalog| catalog.name() == C::NAME)
        .unwrap_or_else(|| panic!("{} is not in CATALOG_TABLES", C::NAME))
}

/// The statements that create the stored tables of the catalogs that the
/// database of `schema` does not have yet. The frontend runs them when it
/// opens a database, with its other tables, so the first write to a
/// catalog writes a row and does not change the schema.
pub(crate) fn create_missing_catalog_tables(
    schema: &Schema,
) -> impl Iterator<Item = &'static str> + '_ {
    CATALOG_TABLES
        .iter()
        .filter(|catalog| {
            schema
                .get_btree_table(NamespaceId::ROOT, catalog.name())
                .is_none()
        })
        .flat_map(|catalog| catalog.create_sql.iter().map(String::as_str))
}

/// A catalog of [`CATALOG_TABLES`]: what its [`CatalogTable`] says, and the
/// SQL of its stored table.
pub(crate) struct Catalog {
    /// The catalog as core reads its `CREATE TABLE`: its name, its columns
    /// and its rowid.
    table: BTreeTable,
    /// The oid of the catalog in `pg_class`, the `tableoid` of its rows.
    table_oid: i64,
    rows: &'static dyn BuiltinRows,
    /// The `CREATE TABLE` and `CREATE INDEX` statements of the stored
    /// table, which holds the rows that statements wrote: the columns and
    /// the indexes of the catalog in PostgreSQL.
    create_sql: Vec<String>,
    /// At position `n`, the query that reads the stored rows whose first
    /// `n` key columns are equal to the parameters. Position 0 reads all
    /// rows.
    select_sql: Vec<String>,
    /// The name of the unique index of the catalog on its key. None when
    /// the key is the rowid.
    key_index: Option<String>,
}

impl Catalog {
    fn of<C: CatalogTable>() -> Self {
        // FIXME(registered-types): see parse_catalog_sql.
        let table = BTreeTable::from_stored_sql(&SqliteDialect, C::CREATE_SQL, 0, false)
            .unwrap_or_else(|error| {
                panic!("the CREATE TABLE of {} is malformed: {error}", C::NAME)
            });
        let columns = column_names(&table);
        let key = C::ROWS.key_columns();
        let create_sql = std::iter::once(table.to_sql_as(C::NAME))
            .chain(C::INDEX_SQL.iter().map(|sql| index_sql(sql, C::NAME)))
            .collect();
        let key_index = table.get_rowid_alias_column().is_none().then(|| {
            let key_columns: Vec<&str> = key.iter().map(|&column| columns[column]).collect();
            unique_index_on(C::NAME, C::INDEX_SQL, &key_columns)
        });
        let select_list = columns.join(", ");
        let select_sql = (0..=key.len())
            .map(|prefix| {
                let condition: Vec<String> = key[..prefix]
                    .iter()
                    .enumerate()
                    .map(|(position, &column)| format!("{} = ?{}", columns[column], position + 1))
                    .collect();
                let where_clause = if condition.is_empty() {
                    String::new()
                } else {
                    format!(" WHERE {}", condition.join(" AND "))
                };
                format!("SELECT {select_list}, rowid FROM {}{where_clause}", C::NAME)
            })
            .collect();
        Self {
            table,
            table_oid: i64::from(C::OID),
            rows: C::ROWS,
            create_sql,
            select_sql,
            key_index,
        }
    }

    /// The name of the catalog, such as `pg_description`, which is also the
    /// name of its stored table.
    pub(crate) fn name(&self) -> &str {
        &self.table.name
    }

    /// The names of the columns, in order.
    fn columns(&self) -> Vec<&str> {
        column_names(&self.table)
    }

    /// The number of columns.
    fn column_count(&self) -> usize {
        self.table.columns().len()
    }

    /// Whether the key is the `oid` column, which is the rowid.
    fn key_is_rowid(&self) -> bool {
        self.table.get_rowid_alias_column().is_some()
    }

    /// The name of the unique index on the key. Fails when the key of the catalog is its rowid, which has no
    /// index: a bug of the caller.
    pub(crate) fn key_index(&self) -> &str {
        self.key_index.as_deref().unwrap_or_else(|| {
            panic!(
                "the key of {} is its rowid, which has no index",
                self.name()
            )
        })
    }

    /// The positions of the key columns.
    fn key(&self) -> &'static [usize] {
        self.rows.key_columns()
    }

    /// The engine statement that inserts `row`, one value for each column,
    /// into the stored table.
    ///
    /// Fails when the binary has a built-in row with the key of `row`: a
    /// statement does not change a built-in row, and the caller refuses
    /// such a statement before it gets here.
    pub(crate) fn insert_statement(&self, row: &[Value]) -> Result<String> {
        assert_eq!(row.len(), self.column_count(), "a row of {}", self.name());
        let key = self.key_values(row)?;
        if !self.rows.key_range(&key).is_empty() {
            return Err(LimboError::InternalError(format!(
                "a statement would write the built-in row {key:?} of {}",
                self.name()
            )));
        }
        let values: Vec<String> = row
            .iter()
            .map(|value| value.exec_quote().to_string())
            .collect();
        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.name(),
            self.columns().join(", "),
            values.join(", ")
        ))
    }

    /// The engine statement that deletes the stored row whose key columns
    /// have the values `key`, in the order of the key.
    pub(crate) fn delete_statement(&self, key: &[i64]) -> String {
        assert_eq!(key.len(), self.key().len(), "a key of {}", self.name());
        let condition: Vec<String> = self
            .key()
            .iter()
            .zip(key)
            .map(|(&column, value)| format!("{} = {value}", self.columns()[column]))
            .collect();
        format!(
            "DELETE FROM {} WHERE {}",
            self.name(),
            condition.join(" AND ")
        )
    }

    /// The values of the key columns of `row`.
    fn key_values(&self, row: &[Value]) -> Result<Vec<i64>> {
        self.key()
            .iter()
            .map(|&column| {
                row[column].as_int().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "the key column {} of a row of {} is not an integer",
                        self.columns()[column],
                        self.name()
                    ))
                })
            })
            .collect()
    }
}

/// The statement `sql`, a `CREATE TABLE` or `CREATE INDEX` statement that
/// the generator writes for a catalog.
///
/// FIXME(registered-types): The generator writes the SQL of a catalog in
/// SQLite syntax, with SQLite types (INTEGER, TEXT, REAL) in place of the
/// types of PostgreSQL, so it is parsed with the SQLite dialect. Once the
/// types of the catalogs are registered, the generator writes the types of
/// PostgreSQL, and the PostgreSQL dialect parses this SQL.
fn parse_catalog_sql(sql: &str) -> ast::Stmt {
    match parse_statement(&SqliteDialect, sql) {
        Ok(Some(ast::Cmd::Stmt(stmt))) => stmt,
        other => panic!("the generated statement {sql:?} does not parse: {other:?}"),
    }
}

/// The name of the unique index of `index_sql`, the `CREATE INDEX`
/// statements of the catalog `name`, whose columns are `columns`.
fn unique_index_on(name: &str, index_sql: &[&str], columns: &[&str]) -> String {
    index_sql
        .iter()
        .find_map(|sql| {
            let ast::Stmt::CreateIndex {
                unique: true,
                idx_name,
                columns: index_columns,
                ..
            } = parse_catalog_sql(sql)
            else {
                return None;
            };
            let index_columns: Vec<String> = index_columns
                .iter()
                .map(|column| match column.expr.as_ref() {
                    ast::Expr::Id(column) => column.as_str().to_string(),
                    other => panic!("an index of {name} is on the expression {other}"),
                })
                .collect();
            (index_columns == columns).then(|| idx_name.name.as_str().to_string())
        })
        .unwrap_or_else(|| panic!("{name} has no unique index on ({})", columns.join(", ")))
}

/// `sql`, a `CREATE INDEX` statement that the generator writes for the
/// catalog `name`, as the engine writes it. Fails when the statement is
/// not an index on the catalog, which is a bug of the generator.
fn index_sql(sql: &str, name: &str) -> String {
    let stmt = parse_catalog_sql(sql);
    let ast::Stmt::CreateIndex { tbl_name, .. } = &stmt else {
        panic!("an index statement of {name} is not CREATE INDEX: {stmt}");
    };
    assert_eq!(tbl_name.as_str(), name, "an index statement of {name}");
    stmt.to_string()
}

/// The names of the columns of `table`, in order.
fn column_names(table: &BTreeTable) -> Vec<&str> {
    table
        .columns()
        .iter()
        .map(|column| {
            column
                .name
                .as_deref()
                .expect("a column of a catalog has a name")
        })
        .collect()
}

/// The virtual table of a catalog of [`CATALOG_TABLES`]. It has the columns
/// of the catalog and the hidden system column `tableoid` after them.
#[derive(Clone, Copy)]
pub(crate) struct CatalogVTab(&'static Catalog);

impl std::fmt::Debug for CatalogVTab {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogVTab")
            .field("name", &self.0.name())
            .finish()
    }
}

impl VirtualTableModule for CatalogVTab {
    type Table = Self;

    /// The `CREATE TABLE` statement of the catalog, with the column
    /// `tableoid` after its columns.
    fn schema(&self, _args: &[Value]) -> Result<String> {
        let create_sql = self.0.table.to_sql();
        Ok(format!(
            "{}, tableoid INTEGER HIDDEN)",
            &create_sql[..create_sql.len() - 1]
        ))
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(*self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for CatalogVTab {
    type Cursor = CatalogCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(CatalogCursor {
            conn,
            catalog: self.0,
            builtin: 0..0,
            statements: (0..self.0.select_sql.len()).map(|_| None).collect(),
            active: None,
            filtering: false,
            current: Current::Done,
            row: Vec::new(),
        })
    }

    /// Uses equalities on the first columns of the key to read only the
    /// rows with these key values. `idx_num` is the number of key columns
    /// with an equality, and the arguments are their values in key order.
    ///
    /// The constraints are not omitted: the cursor can return rows that do
    /// not match, for example when a value is text, and VDBE then compares
    /// each value with SQL rules.
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
        let key = self.0.key();
        let mut prefix = 0;
        for &column in key {
            let Some(position) = constraints.iter().position(|constraint| {
                constraint.usable
                    && constraint.op == ConstraintOp::Eq
                    && constraint.column_index as usize == column
            }) else {
                break;
            };
            prefix += 1;
            constraint_usages[position].argv_index = Some(prefix as u32);
        }
        let rows = self.0.rows.len();
        let (estimated_cost, estimated_rows) = if prefix > 0 && prefix == key.len() {
            (1.0, 1)
        } else if prefix > 0 {
            (10.0, 10)
        } else {
            // The stored table adds rows that no estimate knows.
            (rows as f64, u32::try_from(rows).unwrap_or(u32::MAX - 1))
        };
        Ok(IndexInfo {
            idx_num: prefix as i32,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost,
            estimated_rows,
            constraint_usages,
        })
    }
}

/// Where the row of a [`CatalogCursor`] comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Current {
    /// The built-in row with this index in the built-in rows.
    Builtin(usize),
    /// The stored row with this rowid.
    Stored(i64),
    /// After the last row.
    Done,
}

/// A cursor over the rows of a catalog: first the matching built-in rows,
/// then the matching stored rows. It reads one row at a time.
pub(crate) struct CatalogCursor {
    conn: Arc<Connection>,
    catalog: &'static Catalog,
    /// The built-in rows of the filter that the cursor did not read yet.
    builtin: Range<usize>,
    /// The statement that reads the stored table, at the position of its
    /// query in `Catalog::select_sql`. A statement is prepared the first
    /// time a filter needs it, and the next filters reuse it, because a
    /// cursor on the inner side of a join is filtered many times.
    statements: Vec<Option<Statement>>,
    /// The position in `statements` of the statement of the filter, while
    /// the cursor reads the stored rows.
    active: Option<usize>,
    /// Whether `filter` returned IO and has to continue when VDBE calls it
    /// again.
    filtering: bool,
    current: Current,
    /// The values of the current row, without `tableoid`.
    row: Vec<Value>,
}

impl VirtualTableCursor for CatalogCursor {
    fn filter(&mut self, args: &[Value], _idx_str: Option<&str>, idx_num: i32) -> IOResultOr<bool> {
        if !self.filtering {
            let key_len = self.catalog.key().len();
            let prefix_len = usize::try_from(idx_num).unwrap_or(usize::MAX);
            if prefix_len > key_len || args.len() != prefix_len {
                return Err(LimboError::InternalError(format!(
                    "{} has no plan {idx_num} with {} arguments",
                    self.catalog.name(),
                    args.len()
                ))
                .into());
            }
            let mut prefix = Vec::with_capacity(prefix_len);
            for arg in args {
                match key_of(arg) {
                    Key::Value(value) => prefix.push(value),
                    Key::NoRow => {
                        self.current = Current::Done;
                        self.row.clear();
                        return Ok(IOResult::Done(false));
                    }
                    // VDBE compares each row with the value.
                    Key::AnyRow => break,
                }
            }
            self.start(&prefix)?;
            self.filtering = true;
        }
        let result = self.advance();
        if !matches!(result, Ok(IOResult::IO(_))) {
            self.filtering = false;
        }
        result
    }

    fn next(&mut self) -> IOResultOr<bool> {
        if self.current == Current::Done {
            return Ok(IOResult::Done(false));
        }
        self.advance()
    }

    /// The rowid of the current row. For a catalog whose key is `oid`, it
    /// is the oid. For another catalog, it is the index of a built-in row
    /// in the built-in rows plus 1, and minus the rowid of a stored row, so that
    /// no two rows have the same rowid.
    fn rowid(&self) -> i64 {
        match self.current {
            Current::Builtin(_) | Current::Stored(_) if self.catalog.key_is_rowid() => {
                match &self.row[0] {
                    Value::Numeric(Numeric::Integer(oid)) => *oid,
                    other => {
                        unreachable!("the oid of a row of {} is {other:?}", self.catalog.name())
                    }
                }
            }
            Current::Builtin(index) => index as i64 + 1,
            Current::Stored(rowid) => -rowid,
            Current::Done => 0,
        }
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        let value = match self.current {
            Current::Done => Value::Null,
            // `tableoid` is the column after the columns of the catalog.
            _ if column == self.catalog.column_count() => Value::from_i64(self.catalog.table_oid),
            _ => self.row[column].clone(),
        };
        Ok(IOResult::Done(value))
    }
}

impl CatalogCursor {
    /// Starts a filter that reads the rows whose key starts with `prefix`.
    fn start(&mut self, prefix: &[i64]) -> Result<()> {
        self.builtin = self.catalog.rows.key_range(prefix);
        self.active = None;
        // Core reads the overlay only while the stored table exists, so
        // the statement finds it.
        let slot = &mut self.statements[prefix.len()];
        let statement = match slot {
            Some(statement) => {
                statement.reset()?;
                statement
            }
            None => slot.insert(
                self.conn
                    .prepare_internal(&self.catalog.select_sql[prefix.len()])?,
            ),
        };
        for (position, value) in prefix.iter().enumerate() {
            let index = NonZero::new(position + 1).expect("a parameter index is at least 1");
            statement.bind_at(index, Value::from_i64(*value))?;
        }
        self.active = Some(prefix.len());
        Ok(())
    }

    /// Moves to the next row: the next built-in row, and after the last one
    /// the next stored row. Calling it again after it returned I/O
    /// continues the read of the stored row.
    fn advance(&mut self) -> IOResultOr<bool> {
        if let Some(index) = self.builtin.next() {
            self.row.clear();
            self.catalog.rows.row(index, &mut self.row);
            self.current = Current::Builtin(index);
            return Ok(IOResult::Done(true));
        }
        let Some(position) = self.active else {
            self.current = Current::Done;
            self.row.clear();
            return Ok(IOResult::Done(false));
        };
        let statement = self.statements[position]
            .as_mut()
            .expect("the statement of the filter is prepared");
        match statement.step()? {
            StepResult::Row => {
                let row = statement.row().expect("a step that returns a row has one");
                let mut values: Vec<Value> = row.get_values().cloned().collect();
                let rowid = values
                    .pop()
                    .and_then(|rowid| rowid.as_int())
                    .ok_or_else(|| {
                        LimboError::Corrupt(format!(
                            "a row of {} has no integer rowid",
                            self.catalog.name()
                        ))
                    })?;
                self.row = values;
                self.current = Current::Stored(rowid);
                Ok(IOResult::Done(true))
            }
            StepResult::Done => {
                self.active = None;
                self.current = Current::Done;
                self.row.clear();
                Ok(IOResult::Done(false))
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                let completions = statement
                    .take_io_completions()
                    .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                Ok(IOResult::IO(completions))
            }
            StepResult::Interrupt => Err(LimboError::Interrupt.into()),
            StepResult::Busy => Err(LimboError::Busy.into()),
        }
    }
}

/// What the argument of a lookup asks for.
enum Key {
    /// The rows whose key column has this value.
    Value(i64),
    /// No row: no integer is equal to the value.
    NoRow,
    /// Any row. VDBE compares each row with the value.
    AnyRow,
}

/// The integer that `value` can be equal to. A key column is an integer,
/// which is equal only to an integer, or to a real with the same value. A
/// text or blob value can be equal after the comparison converts it, so the
/// cursor then reads all rows from this column on and VDBE decides.
fn key_of(value: &Value) -> Key {
    match value {
        Value::Null => Key::NoRow,
        Value::Numeric(Numeric::Integer(integer)) => Key::Value(*integer),
        Value::Numeric(Numeric::Float(real)) => {
            let real = f64::from(*real);
            // -2^63 and 2^63 are exact as f64. Every real in this range
            // that has no fraction is an i64.
            if real.fract() == 0.0
                && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&real)
            {
                Key::Value(real as i64)
            } else {
                Key::NoRow
            }
        }
        Value::Text(_) | Value::Blob(_) => Key::AnyRow,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_pg_catalog::PgCollation;

    #[test]
    fn the_stored_table_has_the_name_the_columns_and_the_indexes_of_the_catalog() {
        let description = Catalog::of::<PgDescription>();
        assert_eq!(description.name(), "pg_description");
        assert_eq!(
            description.create_sql,
            [
                "CREATE TABLE pg_description (objoid INTEGER NOT NULL, \
                 classoid INTEGER NOT NULL, objsubid INTEGER NOT NULL, \
                 description TEXT NOT NULL)",
                "CREATE UNIQUE INDEX pg_description_o_c_o_index \
                 ON pg_description (objoid, classoid, objsubid)",
            ]
        );
        assert_eq!(
            description.select_sql[2],
            "SELECT objoid, classoid, objsubid, description, rowid \
             FROM pg_description WHERE objoid = ?1 AND classoid = ?2"
        );
        assert_eq!(description.key_index(), "pg_description_o_c_o_index");
        // A catalog whose key is its oid. pg_collation is not on the list,
        // but its stored table would have this SQL.
        let collation = Catalog::of::<PgCollation>();
        assert!(collation.key_is_rowid());
        assert!(collation.create_sql[0]
            .starts_with("CREATE TABLE pg_collation (oid INTEGER PRIMARY KEY,"));
        assert_eq!(
            collation.select_sql[1],
            format!(
                "SELECT {}, rowid FROM pg_collation WHERE oid = ?1",
                collation.columns().join(", ")
            )
        );
    }

    #[test]
    fn write_statements_insert_and_delete_a_stored_row() {
        let description = Catalog::of::<PgDescription>();
        let row = [
            Value::from_i64(16384),
            Value::from_i64(1259),
            Value::from_i64(0),
            Value::from_text("it's mine"),
        ];
        assert_eq!(
            description.insert_statement(&row).unwrap(),
            "INSERT INTO pg_description \
             (objoid, classoid, objsubid, description) VALUES (16384, 1259, 0, 'it''s mine')"
        );
        assert_eq!(
            description.delete_statement(&[16384, 1259, 0]),
            "DELETE FROM pg_description \
             WHERE objoid = 16384 AND classoid = 1259 AND objsubid = 0"
        );
    }

    /// A statement does not change a built-in row, so a write with the key
    /// of one is a bug of the frontend.
    #[test]
    fn a_write_with_the_key_of_a_builtin_row_fails() {
        let row = [
            Value::from_i64(2200),
            Value::from_i64(2615),
            Value::from_i64(0),
            Value::from_text("mine"),
        ];
        assert!(Catalog::of::<PgDescription>()
            .insert_statement(&row)
            .is_err());
    }
}
