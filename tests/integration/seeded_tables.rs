//! Seeded tables: the read-only table that core serves from a dialect's
//! seed before the table is stored, and a stored table of the same name.

use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value as SqlValue;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use turso_core::dialect::{
    Catalog, CatalogCmd, CmdContext, Dialect, Seed, SeededTable, SqliteCatalog,
};
use turso_core::{Connection, ProgramBuilder, Result, Value};
use turso_sqlite::SqliteDialect;

const ITEMS_SQL: &str =
    "CREATE TABLE seed_items (oid INTEGER PRIMARY KEY, name TEXT, note TEXT, weight REAL)";
const WORDS_SQL: &str = "CREATE TABLE seed_words (word TEXT, length INTEGER)";

/// A seed with fixed rows that counts the calls of `find` and `row`.
struct CountingSeed {
    rows: Vec<Vec<Value>>,
    /// The position of the INTEGER PRIMARY KEY column, if there is one.
    rowid_column: Option<usize>,
    find_calls: AtomicUsize,
    row_calls: AtomicUsize,
}

impl CountingSeed {
    fn new(rows: Vec<Vec<Value>>, rowid_column: Option<usize>) -> Self {
        Self {
            rows,
            rowid_column,
            find_calls: AtomicUsize::new(0),
            row_calls: AtomicUsize::new(0),
        }
    }

    /// The number of calls of `find` and of `row` since the last reset.
    fn calls(&self) -> (usize, usize) {
        (
            self.find_calls.load(Ordering::SeqCst),
            self.row_calls.load(Ordering::SeqCst),
        )
    }

    fn reset(&self) {
        self.find_calls.store(0, Ordering::SeqCst);
        self.row_calls.store(0, Ordering::SeqCst);
    }
}

impl Seed for CountingSeed {
    fn len(&self) -> usize {
        self.rows.len()
    }

    fn row(&self, index: usize, out: &mut Vec<Value>) {
        self.row_calls.fetch_add(1, Ordering::SeqCst);
        out.extend(self.rows[index].iter().cloned());
    }

    fn find(&self, rowid: i64) -> Option<usize> {
        self.find_calls.fetch_add(1, Ordering::SeqCst);
        match self.rowid_column {
            Some(column) => self
                .rows
                .iter()
                .position(|row| row[column] == Value::from_i64(rowid)),
            None => usize::try_from(rowid - 1)
                .ok()
                .filter(|index| *index < self.rows.len()),
        }
    }

    fn version(&self) -> u32 {
        1
    }
}

/// A catalog that stores objects as SQLite does and declares two seeded
/// tables: `seed_items`, with an `oid INTEGER PRIMARY KEY` column and an
/// index, and `seed_words`, without a rowid alias.
struct SeedCatalog {
    tables: &'static [SeededTable],
    items: &'static CountingSeed,
    words: &'static CountingSeed,
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

    fn seeded_tables(&self) -> &'static [SeededTable] {
        self.tables
    }
}

/// A SQLite dialect whose catalog has seeded tables. Each test makes its
/// own, so that the call counters of one test do not see another test.
struct SeedDialect {
    catalog: SeedCatalog,
}

impl SeedDialect {
    fn new() -> Arc<Self> {
        let items: &'static CountingSeed = Box::leak(Box::new(CountingSeed::new(
            vec![
                vec![
                    Value::from_i64(10),
                    Value::from_text("alpha"),
                    Value::Null,
                    Value::from_f64(1.5),
                ],
                vec![
                    Value::from_i64(20),
                    Value::from_text("beta"),
                    Value::from_text("second"),
                    Value::from_f64(2.0),
                ],
                vec![
                    Value::from_i64(30),
                    Value::from_text("gamma"),
                    Value::Null,
                    Value::from_f64(-0.25),
                ],
            ],
            Some(0),
        )));
        let words: &'static CountingSeed = Box::leak(Box::new(CountingSeed::new(
            vec![
                vec![Value::from_text("one"), Value::from_i64(3)],
                vec![Value::from_text("three"), Value::from_i64(5)],
            ],
            None,
        )));
        let tables: &'static [SeededTable] = Box::leak(Box::new([
            SeededTable {
                name: "seed_items",
                create_sql: ITEMS_SQL,
                index_sql: &["CREATE INDEX seed_items_name ON seed_items (name)"],
                seed: items,
            },
            SeededTable {
                name: "seed_words",
                create_sql: WORDS_SQL,
                index_sql: &[],
                seed: words,
            },
        ]));
        Arc::new(Self {
            catalog: SeedCatalog {
                tables,
                items,
                words,
            },
        })
    }

    fn items(&self) -> &'static CountingSeed {
        self.catalog.items
    }

    fn words(&self) -> &'static CountingSeed {
        self.catalog.words
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
}

fn open(path: &Path, dialect: Arc<dyn Dialect>, mvcc: bool) -> TempDatabase {
    TempDatabase::builder()
        .with_db_path(path)
        .with_dialect(dialect)
        .with_mvcc(mvcc)
        .build()
}

fn new_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("seeded.db")
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<SqlValue>> {
    limbo_exec_rows(conn, sql)
}

fn error_of(conn: &Arc<Connection>, sql: &str) -> String {
    match conn.execute(sql) {
        Ok(_) => panic!("{sql} did not fail"),
        Err(err) => err.to_string(),
    }
}

fn int(value: i64) -> SqlValue {
    SqlValue::Integer(value)
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_string())
}

fn real(value: f64) -> SqlValue {
    SqlValue::Real(value)
}

fn seed_item_rows() -> Vec<Vec<SqlValue>> {
    vec![
        vec![int(10), text("alpha"), SqlValue::Null, real(1.5)],
        vec![int(20), text("beta"), text("second"), real(2.0)],
        vec![int(30), text("gamma"), SqlValue::Null, real(-0.25)],
    ]
}

fn full_scan_returns_seed_rows_in_order(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
    assert_eq!(
        rows(
            &conn,
            "SELECT typeof(oid), typeof(name), typeof(note), typeof(weight) FROM seed_items"
        ),
        vec![
            vec![text("integer"), text("text"), text("null"), text("real")],
            vec![text("integer"), text("text"), text("text"), text("real")],
            vec![text("integer"), text("text"), text("null"), text("real")],
        ]
    );
    dialect.items().reset();
    assert_eq!(
        rows(&conn, "SELECT rowid, oid FROM seed_items"),
        vec![
            vec![int(10), int(10)],
            vec![int(20), int(20)],
            vec![int(30), int(30)],
        ]
    );
    let (find_calls, row_calls) = dialect.items().calls();
    assert_eq!(find_calls, 0, "a scan does not look up rows");
    assert_eq!(row_calls, 3, "a scan reads each row once");
    assert_eq!(
        rows(&conn, "SELECT * FROM seed_words"),
        vec![vec![text("one"), int(3)], vec![text("three"), int(5)]]
    );
    // A condition that is not on the rowid is checked by VDBE.
    assert_eq!(
        rows(&conn, "SELECT name FROM seed_items WHERE weight > 1"),
        vec![vec![text("alpha")], vec![text("beta")]]
    );
}

#[test]
fn full_scan_returns_seed_rows_in_order_wal() {
    full_scan_returns_seed_rows_in_order(false);
}

#[test]
fn full_scan_returns_seed_rows_in_order_mvcc() {
    full_scan_returns_seed_rows_in_order(true);
}

fn lookup_by_rowid_uses_find(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let items = dialect.items();

    let lookups = [
        (
            "SELECT name FROM seed_items WHERE oid = 20",
            vec![vec![text("beta")]],
        ),
        (
            "SELECT name FROM seed_items WHERE 30 = oid",
            vec![vec![text("gamma")]],
        ),
        (
            "SELECT name FROM seed_items WHERE rowid = 10",
            vec![vec![text("alpha")]],
        ),
        (
            "SELECT name FROM seed_items WHERE oid = 20.0",
            vec![vec![text("beta")]],
        ),
        ("SELECT name FROM seed_items WHERE oid = 25", vec![]),
        ("SELECT name FROM seed_items WHERE rowid = 99", vec![]),
    ];
    for (sql, expected) in lookups {
        items.reset();
        assert_eq!(rows(&conn, sql), expected, "{sql}");
        let (find_calls, row_calls) = items.calls();
        assert_eq!(find_calls, 1, "{sql} looks up one row");
        assert!(row_calls <= 1, "{sql} reads {row_calls} rows");
    }

    // A value that no integer is equal to finds no row without a lookup.
    for sql in [
        "SELECT name FROM seed_items WHERE oid = NULL",
        "SELECT name FROM seed_items WHERE oid = 20.5",
    ] {
        items.reset();
        assert_eq!(rows(&conn, sql), Vec::<Vec<SqlValue>>::new(), "{sql}");
        assert_eq!(items.calls(), (0, 0), "{sql}");
    }

    // A text value can be equal after conversion, so the cursor scans and
    // VDBE compares.
    items.reset();
    assert_eq!(
        rows(&conn, "SELECT name FROM seed_items WHERE oid = '20'"),
        vec![vec![text("beta")]]
    );
    assert_eq!(items.calls(), (0, 3));

    // A bound parameter is looked up too.
    items.reset();
    let mut stmt = conn
        .prepare("SELECT name FROM seed_items WHERE oid = ?")
        .unwrap();
    stmt.bind_at(1.try_into().unwrap(), Value::from_i64(30))
        .unwrap();
    let mut names = Vec::new();
    stmt.run_with_row_callback(|row| {
        names.push(row.get::<&str>(0)?.to_string());
        Ok(())
    })
    .unwrap();
    assert_eq!(names, vec!["gamma".to_string()]);
    assert_eq!(items.calls(), (1, 1));

    // A join looks up the seeded table once for each outer row.
    conn.execute("CREATE TABLE refs (k INTEGER)").unwrap();
    conn.execute("INSERT INTO refs VALUES (20), (99), (10)")
        .unwrap();
    items.reset();
    assert_eq!(
        rows(
            &conn,
            "SELECT refs.k, s.name FROM refs LEFT JOIN seed_items s ON s.oid = refs.k \
             ORDER BY refs.rowid"
        ),
        vec![
            vec![int(20), text("beta")],
            vec![int(99), SqlValue::Null],
            vec![int(10), text("alpha")],
        ]
    );
    assert_eq!(items.calls(), (3, 2));
}

#[test]
fn lookup_by_rowid_uses_find_wal() {
    lookup_by_rowid_uses_find(false);
}

#[test]
fn lookup_by_rowid_uses_find_mvcc() {
    lookup_by_rowid_uses_find(true);
}

#[test]
fn rowid_without_alias_is_index_plus_one() {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), false);
    let conn = db.connect_limbo();
    assert_eq!(
        rows(&conn, "SELECT rowid, * FROM seed_words"),
        vec![
            vec![int(1), text("one"), int(3)],
            vec![int(2), text("three"), int(5)],
        ]
    );
    // VDBE checks a rowid condition on a table without a rowid alias.
    dialect.words().reset();
    assert_eq!(
        rows(&conn, "SELECT word FROM seed_words WHERE rowid = 2"),
        vec![vec![text("three")]]
    );
    assert_eq!(dialect.words().calls(), (0, 2));
}

fn writes_fail_and_change_nothing(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect, mvcc);
    let conn = db.connect_limbo();
    let insert = error_of(
        &conn,
        "INSERT INTO seed_items VALUES (40, 'delta', NULL, 0.5)",
    );
    assert_eq!(insert, "table seed_items may not be modified");
    let update = error_of(&conn, "UPDATE seed_items SET name = 'x' WHERE oid = 10");
    assert_eq!(update, "table seed_items may not be modified");
    let delete = error_of(&conn, "DELETE FROM seed_items");
    assert_eq!(delete, "table seed_items may not be modified");
    let drop = error_of(&conn, "DROP TABLE seed_items");
    assert!(drop.contains("seed_items"), "{drop}");
    let create = error_of(&conn, "CREATE TABLE seed_items (x)");
    assert!(
        create.contains("table seed_items already exists"),
        "{create}"
    );
    assert!(
        error_of(&conn, "CREATE TABLE seed_words (x)").contains("table seed_words already exists")
    );
    assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
    assert_eq!(
        rows(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'seed%'"
        ),
        vec![vec![int(0)]]
    );
}

#[test]
fn writes_fail_and_change_nothing_wal() {
    writes_fail_and_change_nothing(false);
}

#[test]
fn writes_fail_and_change_nothing_mvcc() {
    writes_fail_and_change_nothing(true);
}

fn visible_after_schema_change_and_reopen(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    let dialect = SeedDialect::new();
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        let other = db.connect_limbo();
        assert_eq!(
            rows(&other, "SELECT count(*) FROM seed_items"),
            vec![vec![int(3)]]
        );
        conn.execute("CREATE TABLE x (a)").unwrap();
        conn.execute("INSERT INTO x VALUES (1)").unwrap();
        assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
        // The other connection reads the schema change first.
        assert_eq!(rows(&other, "SELECT * FROM x"), vec![vec![int(1)]]);
        assert_eq!(rows(&other, "SELECT * FROM seed_items"), seed_item_rows());
        // A connection made after the change.
        let third = db.connect_limbo();
        assert_eq!(rows(&third, "SELECT * FROM seed_items"), seed_item_rows());
        // A temp object does not hide the table, and temp has none.
        conn.execute("CREATE TEMP TABLE t (a)").unwrap();
        assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
        let temp = error_of(&conn, "SELECT * FROM temp.seed_items");
        assert!(temp.contains("no such table"), "{temp}");
        if mvcc {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
            assert_eq!(rows(&other, "SELECT * FROM seed_items"), seed_item_rows());
        }
        conn.close().unwrap();
        other.close().unwrap();
        third.close().unwrap();
    }
    let db = open(&path, dialect, mvcc);
    let conn = db.connect_limbo();
    assert_eq!(rows(&conn, "SELECT * FROM x"), vec![vec![int(1)]]);
    assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
    assert_eq!(
        rows(&conn, "SELECT rowid, * FROM seed_words"),
        vec![
            vec![int(1), text("one"), int(3)],
            vec![int(2), text("three"), int(5)],
        ]
    );
}

#[test]
fn visible_after_schema_change_and_reopen_wal() {
    visible_after_schema_change_and_reopen(false);
}

#[test]
fn visible_after_schema_change_and_reopen_mvcc() {
    visible_after_schema_change_and_reopen(true);
}

#[test]
fn sqlite_dialect_has_no_seeded_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&new_path(&dir), Arc::new(SqliteDialect), false);
    let conn = db.connect_limbo();
    let err = error_of(&conn, "SELECT * FROM seed_items");
    assert!(err.contains("no such table"), "{err}");
}

fn stored_rows() -> Vec<Vec<SqlValue>> {
    vec![
        vec![int(1), text("stored one"), SqlValue::Null, real(9.0)],
        vec![int(2), text("stored two"), text("x"), real(8.0)],
    ]
}

/// A stored table takes the name from the read-only table: a table that a
/// SQLite connection stored under a seeded name is the table that a
/// connection with the seeding dialect reads and writes.
fn stored_table_keeps_its_name(mvcc: bool, checkpoint: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    {
        let db = open(&path, Arc::new(SqliteDialect), mvcc);
        let conn = db.connect_limbo();
        conn.execute(ITEMS_SQL).unwrap();
        conn.execute(
            "INSERT INTO seed_items VALUES (1, 'stored one', NULL, 9.0), (2, 'stored two', 'x', 8.0)",
        )
        .unwrap();
        if checkpoint {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        conn.close().unwrap();
    }
    if mvcc {
        let log = std::fs::metadata(format!("{}-log", path.display()))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if checkpoint {
            assert!(
                log <= 56,
                "the checkpoint emptied the log, which has {log} bytes"
            );
        } else {
            assert!(
                log > 56,
                "the table is only in the logical log, which has {log} bytes"
            );
        }
    }

    let dialect = SeedDialect::new();
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        assert_eq!(rows(&conn, "SELECT * FROM seed_items"), stored_rows());
        assert_eq!(
            rows(&conn, "SELECT name FROM seed_items WHERE oid = 2"),
            vec![vec![text("stored two")]]
        );
        assert_eq!(dialect.items().calls(), (0, 0), "the seed is not read");
        // The table that was not stored still has its read-only table.
        assert_eq!(
            rows(&conn, "SELECT count(*) FROM seed_words"),
            vec![vec![int(2)]]
        );

        conn.execute("INSERT INTO seed_items VALUES (3, 'stored three', NULL, 7.0)")
            .unwrap();
        conn.execute("UPDATE seed_items SET note = 'y' WHERE oid = 1")
            .unwrap();
        let other = db.connect_limbo();
        assert_eq!(
            rows(&other, "SELECT oid, note FROM seed_items"),
            vec![
                vec![int(1), text("y")],
                vec![int(2), text("x")],
                vec![int(3), SqlValue::Null],
            ]
        );
        conn.close().unwrap();
        other.close().unwrap();
    }
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        assert_eq!(
            rows(&conn, "SELECT count(*) FROM seed_items"),
            vec![vec![int(3)]]
        );
        // Dropping the stored table gives the name back to the read-only
        // table, as a reopen does.
        conn.execute("DROP TABLE seed_items").unwrap();
        assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
        conn.close().unwrap();
    }
    let db = open(&path, dialect, mvcc);
    let conn = db.connect_limbo();
    assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
}

#[test]
fn stored_table_keeps_its_name_wal() {
    stored_table_keeps_its_name(false, false);
}

#[test]
fn stored_table_keeps_its_name_wal_after_checkpoint() {
    stored_table_keeps_its_name(false, true);
}

#[test]
fn stored_table_keeps_its_name_mvcc_before_checkpoint() {
    stored_table_keeps_its_name(true, false);
}

#[test]
fn stored_table_keeps_its_name_mvcc_after_checkpoint() {
    stored_table_keeps_its_name(true, true);
}

/// Renaming the stored table gives the name back to the read-only table.
#[test]
fn renamed_stored_table_gives_back_the_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    {
        let db = open(&path, Arc::new(SqliteDialect), false);
        let conn = db.connect_limbo();
        conn.execute(ITEMS_SQL).unwrap();
        conn.execute("INSERT INTO seed_items VALUES (1, 'stored one', NULL, 9.0)")
            .unwrap();
        conn.close().unwrap();
    }
    let db = open(&path, SeedDialect::new(), false);
    let conn = db.connect_limbo();
    conn.execute("ALTER TABLE seed_items RENAME TO kept")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT name FROM kept"),
        vec![vec![text("stored one")]]
    );
    assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
}
