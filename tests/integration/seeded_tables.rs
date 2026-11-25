//! Seeded tables: a dialect registers a virtual table that serves rows from
//! memory, and lists its name in `Catalog::seeded_tables`. A stored table
//! of that name replaces the virtual table. A stored table cannot replace
//! any other virtual table.

use std::path::Path;
use std::sync::Arc;
use turso_core::dialect::{Catalog, CatalogCmd, CmdContext, Dialect, SqliteCatalog};
use turso_core::native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule};
use turso_core::security::roles::SessionRole;
use turso_core::types::IOResultOr;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, IOResult, OpenOptions, PlatformIO,
    ProgramBuilder, Result, Value,
};
use turso_ext::{ConstraintInfo, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode, VTabKind};
use turso_sqlite::SqliteDialect;

/// The value of the one row that the virtual table `seed_catalog` has.
const SEED_VALUE: i64 = 42;

/// A catalog that stores objects as SQLite does and lists `seeded` as its
/// seeded tables.
struct SeedCatalog {
    seeded: &'static [&'static str],
}

impl Catalog for SeedCatalog {
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        cmd: &CatalogCmd<'_>,
    ) -> Result<()> {
        SqliteCatalog.emit(program, ctx, cmd)
    }

    fn seeded_tables(&self) -> &'static [&'static str] {
        self.seeded
    }
}

/// A SQLite dialect that registers the virtual table `seed_catalog`, as a
/// frontend registers a catalog such as `pg_class`.
struct SeedDialect {
    catalog: SeedCatalog,
}

impl SeedDialect {
    fn new(seeded: &'static [&'static str]) -> Arc<Self> {
        Arc::new(Self {
            catalog: SeedCatalog { seeded },
        })
    }
}

impl Dialect for SeedDialect {
    fn name(&self) -> &'static str {
        "seed-test"
    }

    fn parse(&self, sql: &str) -> Result<(Option<turso_core::ast::Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<turso_core::ast::Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> Result<turso_core::ast::ColumnDefinition> {
        SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parameter_marker_ranges(&self, sql: &str) -> Result<Vec<std::ops::Range<usize>>> {
        SqliteDialect.parameter_marker_ranges(sql)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }

    fn catalog(&self) -> &dyn Catalog {
        &self.catalog
    }

    fn register_native_extensions(&self, options: OpenOptions) -> OpenOptions {
        options.native_module("seed_catalog", VTabKind::TableValuedFunction, SeedTable)
    }
}

/// A virtual table with one row, which has the value [`SEED_VALUE`].
#[derive(Debug)]
struct SeedTable;

impl VirtualTableModule for SeedTable {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE seed_catalog (value INTEGER)".to_string())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(Self)
    }
}

impl VirtualTable for SeedTable {
    type Cursor = SeedCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(SeedCursor { done: false })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 1,
            constraint_usages: constraints
                .iter()
                .map(|_| ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
        })
    }
}

struct SeedCursor {
    done: bool,
}

impl VirtualTableCursor for SeedCursor {
    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> IOResultOr<bool> {
        self.done = false;
        Ok(IOResult::Done(true))
    }

    fn next(&mut self) -> IOResultOr<bool> {
        self.done = true;
        Ok(IOResult::Done(false))
    }

    fn rowid(&self) -> i64 {
        1
    }

    fn column(&mut self, _column: usize) -> IOResultOr<Value> {
        Ok(IOResult::Done(Value::from_i64(SEED_VALUE)))
    }
}

/// Opens the database at `path` and connects to it.
fn open(path: &Path, dialect: Arc<dyn Dialect>, mvcc: bool) -> Result<Arc<Connection>> {
    let options = OpenOptions::new(dialect).db_opts(DatabaseOpts::new().with_mvcc(mvcc));
    let db = Database::open(
        Arc::new(PlatformIO::new()?),
        path.to_str().unwrap(),
        options,
    )?;
    db.connect(ConnectOpts::new(SessionRole::Superuser))
}

/// The integers that `sql` returns, one for each row.
fn integers(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    let mut values = Vec::new();
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.run_with_row_callback(|row| {
        values.push(row.get::<i64>(0)?);
        Ok(())
    })
    .unwrap();
    values
}

/// Stores, with SQLite, a table named `name` that has the one value 7.
fn store_table_with_sqlite(path: &Path, name: &str) {
    let sqlite = rusqlite::Connection::open(path).unwrap();
    sqlite
        .execute_batch(&format!(
            "CREATE TABLE {name} (value INTEGER); INSERT INTO {name} VALUES (7);"
        ))
        .unwrap();
}

fn stored_table_replaces_the_virtual_table(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new(&["seed_catalog"]);

    // Nothing is stored: the virtual table has the name.
    let conn = open(&dir.path().join("new.db"), dialect.clone(), mvcc).unwrap();
    assert_eq!(
        integers(&conn, "SELECT value FROM seed_catalog"),
        vec![SEED_VALUE]
    );
    conn.close().unwrap();

    // A table is stored under the name: the stored table has the name.
    let path = dir.path().join("stored.db");
    store_table_with_sqlite(&path, "seed_catalog");
    let conn = open(&path, dialect, mvcc).unwrap();
    assert_eq!(integers(&conn, "SELECT value FROM seed_catalog"), vec![7]);
    // It is an ordinary table: it takes rows and an index.
    conn.execute("INSERT INTO seed_catalog VALUES (9)").unwrap();
    conn.execute("CREATE INDEX seed_catalog_value ON seed_catalog (value)")
        .unwrap();
    // A schema change makes each connection read its schema again, and the
    // stored table keeps the name.
    conn.execute("CREATE TABLE other (a)").unwrap();
    assert_eq!(
        integers(&conn, "SELECT value FROM seed_catalog ORDER BY value"),
        vec![7, 9]
    );
}

#[test]
fn stored_table_replaces_the_virtual_table_wal() {
    stored_table_replaces_the_virtual_table(false);
}

#[test]
fn stored_table_replaces_the_virtual_table_mvcc() {
    stored_table_replaces_the_virtual_table(true);
}

/// A second connection reads its schema again after the first one changes
/// it, and still reads the stored table.
#[test]
fn stored_table_keeps_the_name_when_another_connection_reloads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stored.db");
    store_table_with_sqlite(&path, "seed_catalog");
    let options = OpenOptions::new(SeedDialect::new(&["seed_catalog"]));
    let db = Database::open(
        Arc::new(PlatformIO::new().unwrap()),
        path.to_str().unwrap(),
        options,
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let other = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(integers(&other, "SELECT value FROM seed_catalog"), vec![7]);
    conn.execute("CREATE TABLE changed (a)").unwrap();
    conn.execute("INSERT INTO seed_catalog VALUES (9)").unwrap();
    assert_eq!(
        integers(&other, "SELECT value FROM seed_catalog ORDER BY value"),
        vec![7, 9]
    );
}

/// A stored table cannot replace a virtual table that the catalog does not
/// list as a seeded table.
#[test]
fn stored_table_does_not_replace_another_virtual_table() {
    let dir = tempfile::tempdir().unwrap();

    // The dialect registers `seed_catalog` and does not list it.
    let path = dir.path().join("not-listed.db");
    store_table_with_sqlite(&path, "seed_catalog");
    let err = match open(&path, SeedDialect::new(&[]), false) {
        Ok(_) => panic!("the database opened"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("table seed_catalog already exists"), "{err}");

    // `json_each` is a virtual table of core. The dialect lists only
    // `seed_catalog`.
    let path = dir.path().join("json-each.db");
    store_table_with_sqlite(&path, "json_each");
    let err = match open(&path, SeedDialect::new(&["seed_catalog"]), false) {
        Ok(_) => panic!("the database opened"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("table json_each already exists"), "{err}");
}

/// A statement cannot give a new table the name of a seeded table.
#[test]
fn statement_cannot_take_the_name_of_a_seeded_table() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(
        &dir.path().join("new.db"),
        SeedDialect::new(&["seed_catalog"]),
        false,
    )
    .unwrap();
    conn.execute("CREATE TABLE x (a)").unwrap();
    for sql in [
        "CREATE TABLE seed_catalog (value INTEGER)",
        "ALTER TABLE x RENAME TO seed_catalog",
    ] {
        let err = conn.execute(sql).expect_err(sql).to_string();
        assert!(err.contains("seed_catalog"), "{sql}: {err}");
        assert_eq!(
            integers(&conn, "SELECT value FROM seed_catalog"),
            vec![SEED_VALUE],
            "{sql}"
        );
    }
}
