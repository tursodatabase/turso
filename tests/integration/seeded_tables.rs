//! Seeded tables: the read-only table that core serves from a dialect's
//! seed before the table is stored, and a stored table of the same name.

use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value as SqlValue;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use turso_core::dialect::{
    Catalog, CatalogCmd, CmdContext, CmdOrigin, Dialect, HiddenColumn, Seed, SeedRowRange,
    SeededTable, SqliteCatalog,
};
use turso_core::{Connection, Insn, LimboError, ProgramBuilder, Result, Value};
use turso_sqlite::SqliteDialect;

const ITEMS_SQL: &str =
    "CREATE TABLE seed_items (oid INTEGER PRIMARY KEY, name TEXT, note TEXT, weight REAL)";
const WORDS_SQL: &str = "CREATE TABLE seed_words (word TEXT, length INTEGER)";

/// A seed with fixed rows that counts the calls of `find` and `row`.
struct CountingSeed {
    rows: Vec<Vec<Value>>,
    /// The position of the INTEGER PRIMARY KEY column, if there is one.
    rowid_column: Option<usize>,
    version: u32,
    find_calls: AtomicUsize,
    row_calls: AtomicUsize,
}

impl CountingSeed {
    fn new(rows: Vec<Vec<Value>>, rowid_column: Option<usize>, version: u32) -> Self {
        Self {
            rows,
            rowid_column,
            version,
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
        self.version
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
    /// Stores objects as SQLite does. Like pg_class, `seed_items` also gets
    /// a row for each table that a user creates in the main database:
    /// `(NULL, name, 'user table', NULL)`. The name of the table chooses
    /// what else the catalog does:
    /// - `plain...`: no row.
    /// - `dup...`: the row has oid 10, which a seed row has.
    /// - `inside...`: the row has oid 15, which is in the seed row range.
    /// - `far...`: the row has oid 150.
    /// - `tempwrite...`: a temporary table that gets a row, which the
    ///   statement writes without a change to the schema of the main
    ///   database.
    /// - `twice...`: the catalog opens `seed_items` twice and writes two rows.
    /// - `words...`: `seed_words` also gets a row `(name, length of name)`.
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        cmd: &CatalogCmd<'_>,
    ) -> Result<()> {
        SqliteCatalog.emit(program, ctx, cmd)?;
        let CatalogCmd::CreateTable(table) = cmd else {
            return Ok(());
        };
        // As pg_class has a row for a temporary table, a temporary table
        // named `tempwrite...` gets a row in the main database.
        let temporary_with_row = table.temporary && table.name.starts_with("tempwrite");
        if ctx.origin != CmdOrigin::User
            || (ctx.database_id != 0 && !temporary_with_row)
            || (table.temporary && !temporary_with_row)
            || table.name.starts_with("plain")
        {
            return Ok(());
        }
        let oid = if table.name.starts_with("dup") {
            Some(10)
        } else if table.name.starts_with("inside") {
            Some(15)
        } else if table.name.starts_with("far") {
            Some(150)
        } else {
            None
        };
        emit_item_row(program, ctx, table.name, oid)?;
        if table.name.starts_with("twice") {
            emit_item_row(program, ctx, &format!("{} again", table.name), None)?;
        }
        if table.name.starts_with("words") {
            let words = ctx.open_seeded_table_for_write(program, "seed_words")?;
            let start = program.alloc_registers(2);
            program.emit_insn(Insn::String8 {
                value: table.name.to_string(),
                dest: start,
            });
            program.emit_insn(Insn::Integer {
                value: table.name.len() as i64,
                dest: start + 1,
            });
            words.emit_insert(program, start);
        }
        Ok(())
    }

    fn seeded_tables(&self) -> &'static [SeededTable] {
        self.tables
    }
}

/// Writes the row `(oid, name, 'user table', NULL)` to `seed_items`. A
/// `None` oid gives a new one.
fn emit_item_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    name: &str,
    oid: Option<i64>,
) -> Result<()> {
    let items = ctx.open_seeded_table_for_write(program, "seed_items")?;
    let start = program.alloc_registers(4);
    match oid {
        Some(oid) => program.emit_insn(Insn::Integer {
            value: oid,
            dest: start,
        }),
        None => program.emit_insn(Insn::Null {
            dest: start,
            dest_end: None,
        }),
    }
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: start + 1,
    });
    program.emit_insn(Insn::String8 {
        value: "user table".to_string(),
        dest: start + 2,
    });
    program.emit_insn(Insn::Null {
        dest: start + 3,
        dest_end: None,
    });
    items.emit_insert(program, start);
    Ok(())
}

/// The seeds of a [`SeedDialect`].
struct Seeds {
    items: Vec<Vec<Value>>,
    /// The seed row range of `seed_items`.
    item_rows: SeedRowRange,
    words: Vec<Vec<Value>>,
    /// The version of both seeds.
    version: u32,
}

/// The seeds of [`SeedDialect::newer`].
fn newer_seeds() -> Seeds {
    Seeds {
        items: vec![
            vec![
                Value::from_i64(10),
                Value::from_text("alpha"),
                Value::from_text("changed"),
                Value::from_f64(1.5),
            ],
            vec![
                Value::from_i64(25),
                Value::from_text("delta"),
                Value::Null,
                Value::from_f64(4.0),
            ],
            vec![
                Value::from_i64(30),
                Value::from_text("gamma"),
                Value::Null,
                Value::from_f64(-0.25),
            ],
        ],
        item_rows: SeedRowRange {
            column: "oid",
            range: 0..31,
        },
        words: vec![
            vec![Value::from_text("uno"), Value::from_i64(3)],
            vec![Value::from_text("four"), Value::from_i64(4)],
        ],
        version: 2,
    }
}

/// A SQLite dialect whose catalog has seeded tables. Each test makes its
/// own, so that the call counters of one test do not see another test.
struct SeedDialect {
    catalog: SeedCatalog,
}

impl SeedDialect {
    fn new() -> Arc<Self> {
        Self::with_items(vec![
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
        ])
    }

    /// A dialect whose `seed_items` has the rows `items`.
    fn with_items(items: Vec<Vec<Value>>) -> Arc<Self> {
        Self::with_tables(items, &[])
    }

    /// A dialect whose `seed_words` has the hidden column `tableoid`, with
    /// the value 1234 in each row.
    fn with_hidden_column() -> Arc<Self> {
        let dialect = Self::new();
        Self::with_tables(
            dialect.items().rows.clone(),
            &[HiddenColumn {
                name: "tableoid",
                value: 1234,
            }],
        )
    }

    /// A dialect whose `seed_items` has the rows `items`, and whose
    /// `seed_words` has the hidden columns `words_hidden_columns`. The seed
    /// rows of `seed_items` are those with an oid up to the last oid of
    /// `items`.
    fn with_tables(
        items: Vec<Vec<Value>>,
        words_hidden_columns: &'static [HiddenColumn],
    ) -> Arc<Self> {
        let last_oid = items
            .iter()
            .map(|row| match row[0] {
                Value::Numeric(turso_core::numeric::Numeric::Integer(oid)) => oid,
                ref other => panic!("the oid of a seed row is {other:?}"),
            })
            .max()
            .unwrap_or(0);
        Self::with_item_rows(
            items,
            SeedRowRange {
                column: "oid",
                range: 0..last_oid + 1,
            },
            words_hidden_columns,
        )
    }

    /// A dialect whose `seed_items` has the rows `items` and the seed row
    /// range `item_rows`. The seed rows of `seed_words` are those with a
    /// length below 6, and the catalog writes longer words.
    fn with_item_rows(
        items: Vec<Vec<Value>>,
        item_rows: SeedRowRange,
        words_hidden_columns: &'static [HiddenColumn],
    ) -> Arc<Self> {
        Self::with_seeds(
            Seeds {
                items,
                item_rows,
                words: vec![
                    vec![Value::from_text("one"), Value::from_i64(3)],
                    vec![Value::from_text("three"), Value::from_i64(5)],
                ],
                version: 1,
            },
            words_hidden_columns,
        )
    }

    /// The dialect of a newer binary, whose seeds have other rows and the
    /// version 2. Against [`Self::new`], `seed_items` changes the row 10,
    /// has no row 20 and has a new row 25, and `seed_words` changes the row
    /// `one`, has no row `three` and has a new row `four`.
    fn newer() -> Arc<Self> {
        Self::with_seeds(newer_seeds(), &[])
    }

    /// A dialect with the seeds `seeds`, whose `seed_words` has the hidden
    /// columns `words_hidden_columns`.
    fn with_seeds(seeds: Seeds, words_hidden_columns: &'static [HiddenColumn]) -> Arc<Self> {
        let Seeds {
            items,
            item_rows,
            words,
            version,
        } = seeds;
        let items: &'static CountingSeed =
            Box::leak(Box::new(CountingSeed::new(items, Some(0), version)));
        let words: &'static CountingSeed =
            Box::leak(Box::new(CountingSeed::new(words, None, version)));
        let tables: &'static [SeededTable] = Box::leak(Box::new([
            SeededTable {
                name: "seed_items",
                create_sql: ITEMS_SQL,
                index_sql: &["CREATE INDEX seed_items_name ON seed_items (name)"],
                seed: items,
                hidden_columns: &[],
                seed_rows: Some(item_rows),
            },
            SeededTable {
                name: "seed_words",
                create_sql: WORDS_SQL,
                index_sql: &[],
                seed: words,
                hidden_columns: words_hidden_columns,
                seed_rows: Some(SeedRowRange {
                    column: "length",
                    range: 0..6,
                }),
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
    conn.execute("CREATE TABLE plain_refs (k INTEGER)").unwrap();
    conn.execute("INSERT INTO plain_refs VALUES (20), (99), (10)")
        .unwrap();
    items.reset();
    assert_eq!(
        rows(
            &conn,
            "SELECT r.k, s.name FROM plain_refs r LEFT JOIN seed_items s ON s.oid = r.k \
             ORDER BY r.rowid"
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

/// A hidden column of a read-only table has its value in each row. `SELECT *`
/// does not show it, and a query reads it by name. The stored table would
/// not have it, so the first write fails and changes nothing.
fn hidden_column_is_served_and_blocks_the_first_write(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&new_path(&dir), SeedDialect::with_hidden_column(), mvcc);
    let conn = db.connect_limbo();
    let words = vec![vec![text("one"), int(3)], vec![text("three"), int(5)]];
    assert_eq!(rows(&conn, "SELECT * FROM seed_words"), words);
    assert_eq!(
        rows(
            &conn,
            "SELECT tableoid, word, w.tableoid FROM seed_words AS w ORDER BY length DESC"
        ),
        vec![
            vec![int(1234), text("three"), int(1234)],
            vec![int(1234), text("one"), int(1234)],
        ]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT word FROM seed_words WHERE tableoid = 1234 AND rowid = 2"
        ),
        vec![vec![text("three")]]
    );
    let error = error_of(&conn, "CREATE TABLE words_x (a)");
    assert!(
        error.contains("cannot store seeded table seed_words"),
        "{error}"
    );
    assert_eq!(rows(&conn, "SELECT * FROM seed_words"), words);
    assert_eq!(
        rows(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'seed%' OR name = 'words_x'"
        ),
        vec![vec![int(0)]]
    );
}

#[test]
fn hidden_column_is_served_and_blocks_the_first_write_wal() {
    hidden_column_is_served_and_blocks_the_first_write(false);
}

#[test]
fn hidden_column_is_served_and_blocks_the_first_write_mvcc() {
    hidden_column_is_served_and_blocks_the_first_write(true);
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
        conn.execute("CREATE TABLE plain_x (a)").unwrap();
        conn.execute("INSERT INTO plain_x VALUES (1)").unwrap();
        assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
        // The other connection reads the schema change first.
        assert_eq!(rows(&other, "SELECT * FROM plain_x"), vec![vec![int(1)]]);
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
    assert_eq!(rows(&conn, "SELECT * FROM plain_x"), vec![vec![int(1)]]);
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

/// The rows that the catalog wrote to `seed_items` for user tables.
fn user_table_rows(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(
        conn,
        "SELECT name FROM seed_items WHERE note = 'user table' ORDER BY oid",
    )
}

/// The `sqlite_schema` rows of `seed_items`.
fn seed_items_schema_rows(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(
        conn,
        "SELECT type, name, tbl_name, sql IS NULL FROM sqlite_schema \
         WHERE tbl_name = 'seed_items' ORDER BY rowid",
    )
}

fn stored_seed_items_schema_rows() -> Vec<Vec<SqlValue>> {
    vec![
        vec![
            text("table"),
            text("seed_items"),
            text("seed_items"),
            int(0),
        ],
        vec![
            text("index"),
            text("seed_items_name"),
            text("seed_items"),
            int(0),
        ],
    ]
}

/// A statement stores `seed_items` and writes to it: the stored table takes
/// the name from the read-only table in the same statement, and keeps it in
/// the transaction, after the commit, for another connection and after a
/// reopen.
fn same_statement_takes_the_name(mvcc: bool, explicit_transaction: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    let dialect = SeedDialect::new();
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        let other = db.connect_limbo();
        assert_eq!(rows(&other, "SELECT * FROM seed_items"), seed_item_rows());
        if explicit_transaction {
            conn.execute("BEGIN").unwrap();
        }
        conn.execute("CREATE TABLE t1 (a)").unwrap();
        dialect.items().reset();
        assert_eq!(user_table_rows(&conn), vec![vec![text("t1")]]);
        assert_eq!(
            dialect.items().calls(),
            (0, 0),
            "the stored table has the name"
        );
        assert_eq!(
            seed_items_schema_rows(&conn),
            stored_seed_items_schema_rows()
        );
        // A second write in the transaction opens the stored table.
        conn.execute("CREATE TABLE t2 (a)").unwrap();
        assert_eq!(
            user_table_rows(&conn),
            vec![vec![text("t1")], vec![text("t2")]]
        );
        if explicit_transaction {
            conn.execute("COMMIT").unwrap();
        }
        dialect.items().reset();
        assert_eq!(
            user_table_rows(&other),
            vec![vec![text("t1")], vec![text("t2")]]
        );
        assert_eq!(
            seed_items_schema_rows(&other),
            stored_seed_items_schema_rows()
        );
        assert_eq!(dialect.items().calls(), (0, 0));
        conn.close().unwrap();
        other.close().unwrap();
    }
    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    dialect.items().reset();
    assert_eq!(
        user_table_rows(&conn),
        vec![vec![text("t1")], vec![text("t2")]]
    );
    assert_eq!(
        seed_items_schema_rows(&conn),
        stored_seed_items_schema_rows()
    );
    assert_eq!(dialect.items().calls(), (0, 0));
}

#[test]
fn same_statement_takes_the_name_wal() {
    same_statement_takes_the_name(false, false);
}

#[test]
fn same_statement_takes_the_name_wal_in_transaction() {
    same_statement_takes_the_name(false, true);
}

#[test]
fn same_statement_takes_the_name_mvcc() {
    same_statement_takes_the_name(true, false);
}

#[test]
fn same_statement_takes_the_name_mvcc_in_transaction() {
    same_statement_takes_the_name(true, true);
}

fn turso_integrity_check(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(conn, "PRAGMA integrity_check")
}

fn sqlite_integrity_check(path: &Path) -> String {
    let sqlite = rusqlite::Connection::open(path).unwrap();
    sqlite
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap()
}

/// All rows of `seed_items`, by oid.
fn all_items(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(conn, "SELECT * FROM seed_items ORDER BY oid")
}

/// The seed rows, then a row that the catalog wrote for each of `tables`,
/// with the oids that follow the last seed row.
fn items_with_user_tables(tables: &[&str]) -> Vec<Vec<SqlValue>> {
    let mut expected = seed_item_rows();
    for (position, table) in tables.iter().enumerate() {
        expected.push(vec![
            int(31 + position as i64),
            text(table),
            text("user table"),
            SqlValue::Null,
        ]);
    }
    expected
}

/// Asserts that `seed_items` is the read-only table on `conn`: it returns
/// the seed rows, which it reads from the seed, and no `sqlite_schema` row
/// has the name.
fn assert_read_only_items(conn: &Arc<Connection>, dialect: &SeedDialect) {
    dialect.items().reset();
    assert_eq!(all_items(conn), seed_item_rows());
    let (_, row_calls) = dialect.items().calls();
    assert!(row_calls > 0, "the rows come from the seed");
    assert_eq!(
        rows(
            conn,
            "SELECT count(*) FROM sqlite_schema WHERE tbl_name = 'seed_items'"
        ),
        vec![vec![int(0)]]
    );
}

/// Fact 1: the first write stores the seed rows and the catalog's row, in
/// the statement's transaction. Every later reader sees the stored table,
/// and the file is intact.
fn first_write_stores_seed_rows(mvcc: bool, explicit_transaction: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    let dialect = SeedDialect::new();
    let expected = items_with_user_tables(&["t1"]);
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        let other = db.connect_limbo();
        if explicit_transaction {
            conn.execute("BEGIN").unwrap();
        }
        conn.execute("CREATE TABLE t1 (a)").unwrap();
        assert_eq!(all_items(&conn), expected);
        if explicit_transaction {
            // The transaction is not committed: the other connection still
            // reads the read-only table.
            assert_read_only_items(&other, &dialect);
            conn.execute("COMMIT").unwrap();
        }
        dialect.items().reset();
        assert_eq!(all_items(&other), expected);
        let third = db.connect_limbo();
        assert_eq!(all_items(&third), expected);
        assert_eq!(dialect.items().calls(), (0, 0));
        // Only the table that the statement wrote is stored.
        assert_eq!(
            rows(
                &conn,
                "SELECT count(*) FROM sqlite_schema WHERE tbl_name = 'seed_words'"
            ),
            vec![vec![int(0)]]
        );
        assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
        if !mvcc {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        conn.close().unwrap();
        other.close().unwrap();
        third.close().unwrap();
    }
    if !mvcc {
        assert_eq!(sqlite_integrity_check(&path), "ok");
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        let mut stmt = sqlite
            .prepare("SELECT * FROM seed_items ORDER BY oid")
            .unwrap();
        let stored: Vec<Vec<SqlValue>> = stmt
            .query_map([], |row| (0..4).map(|i| row.get(i)).collect())
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(stored, expected);
    }
    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    dialect.items().reset();
    assert_eq!(all_items(&conn), expected);
    assert_eq!(dialect.items().calls(), (0, 0));
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn first_write_stores_seed_rows_wal() {
    first_write_stores_seed_rows(false, false);
}

#[test]
fn first_write_stores_seed_rows_wal_in_transaction() {
    first_write_stores_seed_rows(false, true);
}

#[test]
fn first_write_stores_seed_rows_mvcc() {
    first_write_stores_seed_rows(true, false);
}

#[test]
fn first_write_stores_seed_rows_mvcc_in_transaction() {
    first_write_stores_seed_rows(true, true);
}

/// Asserts that each seed row is stored once: the oids are distinct, and
/// the stored rows are the seed rows and one row for each of `tables`.
fn assert_seed_stored_once(conn: &Arc<Connection>, tables: &[&str]) {
    assert_eq!(
        rows(conn, "SELECT count(*), count(DISTINCT oid) FROM seed_items"),
        vec![vec![
            int(3 + tables.len() as i64),
            int(3 + tables.len() as i64)
        ]]
    );
    assert_eq!(all_items(conn), items_with_user_tables(tables));
}

/// Fact 2: a statement compiled while the table was not stored, run after
/// another connection stored it, compiles again and does not fill again.
fn prepared_first_write_runs_after_another_store(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let a = db.connect_limbo();
    let b = db.connect_limbo();
    let mut statement = a.prepare("CREATE TABLE a1 (a)").unwrap();
    b.execute("CREATE TABLE b1 (a)").unwrap();
    dialect.items().reset();
    statement.run_ignore_rows().unwrap();
    assert_eq!(dialect.items().calls(), (0, 0), "no second fill");
    assert_seed_stored_once(&a, &["b1", "a1"]);
    assert_seed_stored_once(&b, &["b1", "a1"]);
    assert_eq!(turso_integrity_check(&a), vec![vec![text("ok")]]);
}

#[test]
fn prepared_first_write_runs_after_another_store_wal() {
    prepared_first_write_runs_after_another_store(false);
}

#[test]
fn prepared_first_write_runs_after_another_store_mvcc() {
    prepared_first_write_runs_after_another_store(true);
}

/// Fact 2: two transactions do the first write at the same time. A schema
/// change takes the database's write lock, so the second first write fails
/// with `expected` (Busy, or BusySnapshot when its WAL snapshot is older
/// than the first commit) and fills nothing. A retry in a new transaction
/// opens the stored table and does not fill again.
fn concurrent_first_writes(
    mvcc: bool,
    begin: &str,
    second_reads_first: bool,
    expected: fn(&LimboError) -> bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let a = db.connect_limbo();
    let b = db.connect_limbo();
    a.execute(begin).unwrap();
    a.execute("CREATE TABLE a1 (a)").unwrap();
    b.execute(begin).unwrap();
    if second_reads_first {
        // The second transaction reads the read-only table, then the first
        // commits.
        assert_read_only_items(&b, &dialect);
        a.execute("COMMIT").unwrap();
    }
    dialect.items().reset();
    let err = b.execute("CREATE TABLE b1 (a)").unwrap_err();
    assert!(expected(&err), "mvcc={mvcc} {begin}: {err:?}");
    assert_eq!(dialect.items().calls(), (0, 0), "no fill");
    if !second_reads_first {
        a.execute("COMMIT").unwrap();
    }
    if !b.get_auto_commit() {
        b.execute("ROLLBACK").unwrap();
    }
    assert_seed_stored_once(&b, &["a1"]);

    dialect.items().reset();
    b.execute(begin).unwrap();
    b.execute("CREATE TABLE b1 (a)").unwrap();
    b.execute("COMMIT").unwrap();
    assert_eq!(dialect.items().calls(), (0, 0), "no second fill");
    assert_seed_stored_once(&b, &["a1", "b1"]);
    assert_seed_stored_once(&a, &["a1", "b1"]);
    assert_eq!(turso_integrity_check(&a), vec![vec![text("ok")]]);
}

fn is_busy(err: &LimboError) -> bool {
    matches!(err, LimboError::Busy)
}

fn is_busy_snapshot(err: &LimboError) -> bool {
    matches!(err, LimboError::BusySnapshot)
}

#[test]
fn concurrent_first_writes_wal() {
    concurrent_first_writes(false, "BEGIN", false, is_busy);
}

#[test]
fn concurrent_first_writes_wal_after_commit() {
    concurrent_first_writes(false, "BEGIN", true, is_busy_snapshot);
}

#[test]
fn concurrent_first_writes_mvcc() {
    concurrent_first_writes(true, "BEGIN", false, is_busy);
}

#[test]
fn concurrent_first_writes_mvcc_after_commit() {
    concurrent_first_writes(true, "BEGIN", true, is_busy);
}

#[test]
fn concurrent_first_writes_mvcc_concurrent_transactions() {
    concurrent_first_writes(true, "BEGIN CONCURRENT", false, is_busy);
}

#[test]
fn concurrent_first_writes_mvcc_concurrent_transactions_after_commit() {
    concurrent_first_writes(true, "BEGIN CONCURRENT", true, is_busy);
}

/// How a test rolls back the first write.
#[derive(Clone, Copy, Debug)]
enum FirstWriteRollback {
    /// The statement fails after the fill in autocommit mode: the
    /// catalog's row has the oid of a seed row.
    FailedStatement,
    /// The same statement fails inside a transaction, which goes on and
    /// commits.
    FailedStatementInTransaction,
    /// The statement succeeds inside a transaction, and ROLLBACK ends it.
    Rollback,
}

/// Fact 3: a first write that is rolled back leaves the name on the
/// read-only table, for this connection and for a connection that shares
/// the schema, and leaves no `sqlite_schema` row. A later first write
/// stores the table.
fn rolled_back_first_write_leaves_read_only_table(mvcc: bool, rollback: FirstWriteRollback) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let other = db.connect_limbo();
    assert_read_only_items(&other, &dialect);

    let assert_failed = |err: String| {
        assert!(
            err.contains("UNIQUE constraint failed: seed_items.oid"),
            "{rollback:?}: {err}"
        );
    };
    match rollback {
        FirstWriteRollback::FailedStatement => {
            assert_failed(error_of(&conn, "CREATE TABLE dup1 (a)"));
        }
        FirstWriteRollback::FailedStatementInTransaction => {
            conn.execute("BEGIN").unwrap();
            assert_failed(error_of(&conn, "CREATE TABLE dup1 (a)"));
            assert_read_only_items(&conn, &dialect);
            assert_read_only_items(&other, &dialect);
            conn.execute("CREATE TABLE plain_kept (a)").unwrap();
            conn.execute("COMMIT").unwrap();
            assert_eq!(
                rows(&other, "SELECT count(*) FROM plain_kept"),
                vec![vec![int(0)]]
            );
        }
        FirstWriteRollback::Rollback => {
            conn.execute("BEGIN").unwrap();
            conn.execute("CREATE TABLE t1 (a)").unwrap();
            assert_eq!(all_items(&conn), items_with_user_tables(&["t1"]));
            conn.execute("ROLLBACK").unwrap();
        }
    }
    assert_read_only_items(&conn, &dialect);
    assert_read_only_items(&other, &dialect);
    for table in ["dup1", "t1"] {
        let err = error_of(&other, &format!("SELECT * FROM {table}"));
        assert!(err.contains("no such table"), "{err}");
    }

    // A new first write.
    conn.execute("CREATE TABLE t2 (a)").unwrap();
    assert_eq!(all_items(&conn), items_with_user_tables(&["t2"]));
    assert_eq!(all_items(&other), items_with_user_tables(&["t2"]));
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn failed_first_write_leaves_read_only_table_wal() {
    rolled_back_first_write_leaves_read_only_table(false, FirstWriteRollback::FailedStatement);
}

#[test]
fn failed_first_write_leaves_read_only_table_mvcc() {
    rolled_back_first_write_leaves_read_only_table(true, FirstWriteRollback::FailedStatement);
}

#[test]
fn failed_first_write_in_transaction_leaves_read_only_table_wal() {
    rolled_back_first_write_leaves_read_only_table(
        false,
        FirstWriteRollback::FailedStatementInTransaction,
    );
}

#[test]
fn failed_first_write_in_transaction_leaves_read_only_table_mvcc() {
    rolled_back_first_write_leaves_read_only_table(
        true,
        FirstWriteRollback::FailedStatementInTransaction,
    );
}

#[test]
fn rolled_back_first_write_leaves_read_only_table_wal() {
    rolled_back_first_write_leaves_read_only_table(false, FirstWriteRollback::Rollback);
}

#[test]
fn rolled_back_first_write_leaves_read_only_table_mvcc() {
    rolled_back_first_write_leaves_read_only_table(true, FirstWriteRollback::Rollback);
}

/// The opcodes of the program of `sql`.
fn opcodes(conn: &Arc<Connection>, sql: &str) -> Vec<String> {
    rows(conn, &format!("EXPLAIN {sql}"))
        .into_iter()
        .map(|row| match &row[1] {
            SqlValue::Text(opcode) => opcode.clone(),
            other => panic!("opcode is {other:?}"),
        })
        .collect()
}

fn count_opcode(opcodes: &[String], opcode: &str) -> usize {
    opcodes.iter().filter(|other| *other == opcode).count()
}

/// A statement that asks twice for the same seeded table stores it once.
fn same_table_twice_in_one_statement(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let program = opcodes(&conn, "CREATE TABLE twice1 (a)");
    // twice1, seed_items and its index seed_items_name, and the table of
    // seed versions and its automatic index.
    assert_eq!(count_opcode(&program, "CreateBtree"), 5, "{program:?}");
    assert_eq!(count_opcode(&program, "VOpen"), 1, "{program:?}");
    assert_eq!(count_opcode(&program, "ParseSchema"), 3, "{program:?}");
    assert_eq!(count_opcode(&program, "CatalogApply"), 1, "{program:?}");
    conn.execute("CREATE TABLE twice1 (a)").unwrap();
    assert_eq!(
        all_items(&conn),
        items_with_user_tables(&["twice1", "twice1 again"])
    );
    // Once stored, the statement creates only its own table.
    let program = opcodes(&conn, "CREATE TABLE twice2 (a)");
    assert_eq!(count_opcode(&program, "CreateBtree"), 1, "{program:?}");
    assert_eq!(count_opcode(&program, "VOpen"), 0, "{program:?}");
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn same_table_twice_in_one_statement_wal() {
    same_table_twice_in_one_statement(false);
}

#[test]
fn same_table_twice_in_one_statement_mvcc() {
    same_table_twice_in_one_statement(true);
}

/// One statement stores two seeded tables. The table without a rowid alias
/// keeps the rowids of the seed rows.
fn two_tables_in_one_statement(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    let dialect = SeedDialect::new();
    let expected_words = vec![
        vec![int(1), text("one"), int(3)],
        vec![int(2), text("three"), int(5)],
        vec![int(3), text("words1"), int(6)],
    ];
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE words1 (a)").unwrap();
        assert_eq!(all_items(&conn), items_with_user_tables(&["words1"]));
        assert_eq!(
            rows(&conn, "SELECT rowid, * FROM seed_words ORDER BY rowid"),
            expected_words
        );
        assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
        conn.close().unwrap();
    }
    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    dialect.words().reset();
    assert_eq!(
        rows(&conn, "SELECT rowid, * FROM seed_words ORDER BY rowid"),
        expected_words
    );
    assert_eq!(dialect.words().calls(), (0, 0));
}

#[test]
fn two_tables_in_one_statement_wal() {
    two_tables_in_one_statement(false);
}

#[test]
fn two_tables_in_one_statement_mvcc() {
    two_tables_in_one_statement(true);
}

/// The fill writes the index of `index_sql`, so a query that uses the index
/// finds the seed rows.
fn index_has_the_seed_rows(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t1 (a)").unwrap();
    let plan = rows(
        &conn,
        "EXPLAIN QUERY PLAN SELECT oid FROM seed_items WHERE name = 'beta'",
    );
    assert!(
        format!("{plan:?}").contains("seed_items_name"),
        "the query uses the index: {plan:?}"
    );
    assert_eq!(
        rows(&conn, "SELECT oid FROM seed_items WHERE name = 'beta'"),
        vec![vec![int(20)]]
    );
    assert_eq!(
        rows(&conn, "SELECT oid FROM seed_items WHERE name = 't1'"),
        vec![vec![int(31)]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT name FROM seed_items WHERE name > 'a' ORDER BY name"
        ),
        vec![
            vec![text("alpha")],
            vec![text("beta")],
            vec![text("gamma")],
            vec![text("t1")],
        ]
    );
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn index_has_the_seed_rows_wal() {
    index_has_the_seed_rows(false);
}

#[test]
fn index_has_the_seed_rows_mvcc() {
    index_has_the_seed_rows(true);
}

/// A seed of some thousands of rows fills a table of many pages.
fn large_seed_fills(mvcc: bool) {
    const ROWS: i64 = 5000;
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    let items = (0..ROWS)
        .map(|i| {
            vec![
                Value::from_i64(i * 2 + 1),
                Value::from_text(format!("name {i:05}")),
                Value::from_text("x".repeat(100)),
                Value::from_f64(i as f64 / 4.0),
            ]
        })
        .collect();
    let dialect = SeedDialect::with_items(items);
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        // A small cache makes the fill write pages out before the commit.
        conn.execute("PRAGMA cache_size = 20").unwrap();
        conn.execute("CREATE TABLE t1 (a)").unwrap();
        assert_eq!(
            rows(
                &conn,
                "SELECT count(*), count(DISTINCT oid), max(oid) FROM seed_items"
            ),
            vec![vec![int(ROWS + 1), int(ROWS + 1), int(ROWS * 2)]]
        );
        conn.close().unwrap();
    }
    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    dialect.items().reset();
    assert_eq!(
        rows(
            &conn,
            "SELECT oid, name, weight FROM seed_items WHERE oid = 4001"
        ),
        vec![vec![int(4001), text("name 02000"), real(500.0)]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT oid FROM seed_items WHERE name = 'name 04999'"
        ),
        vec![vec![int(9999)]]
    );
    assert_eq!(
        rows(&conn, "SELECT name, note FROM seed_items WHERE oid = 10000"),
        vec![vec![text("t1"), text("user table")]]
    );
    assert_eq!(dialect.items().calls(), (0, 0));
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn large_seed_fills_wal() {
    large_seed_fills(false);
}

#[test]
fn large_seed_fills_mvcc() {
    large_seed_fills(true);
}

fn open_with_custom_types(path: &Path, mvcc: bool) -> TempDatabase {
    TempDatabase::builder()
        .with_db_path(path)
        .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
        .with_mvcc(mvcc)
        .build()
}

/// The table of the types made by CREATE TYPE has the automatic index of
/// its `name TEXT PRIMARY KEY` column, so SQLite finds the file intact.
#[test]
fn types_table_has_its_automatic_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    {
        let db = open_with_custom_types(&path, false);
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE cents BASE integer").unwrap();
        assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }
    assert_eq!(sqlite_integrity_check(&path), "ok");
}

/// `SELECT name FROM __turso_internal_types`, by name.
fn stored_type_names(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(
        conn,
        "SELECT name FROM __turso_internal_types ORDER BY name",
    )
}

/// The `type` and `name` of the `sqlite_schema` rows of the table of types.
fn types_table_schema_rows(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(
        conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = '__turso_internal_types' ORDER BY rowid",
    )
}

/// CREATE TYPE stores the table of types with its index on the first type,
/// and DROP TYPE deletes the index entry of the type, so the index stays
/// intact. A database without types has no table of types.
fn types_table_index_is_kept(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    {
        let db = open_with_custom_types(&path, mvcc);
        let conn = db.connect_limbo();
        let err = error_of(&conn, "SELECT * FROM __turso_internal_types");
        assert!(err.contains("no such table"), "{err}");
        conn.execute("CREATE TYPE cents BASE integer").unwrap();
        conn.execute("CREATE TYPE dollars BASE integer").unwrap();
        assert_eq!(
            types_table_schema_rows(&conn),
            vec![
                vec![text("table"), text("__turso_internal_types")],
                vec![
                    text("index"),
                    text("sqlite_autoindex___turso_internal_types_1")
                ],
            ]
        );
        conn.execute("DROP TYPE cents").unwrap();
        assert_eq!(stored_type_names(&conn), vec![vec![text("dollars")]]);
        assert_eq!(
            rows(
                &conn,
                "SELECT name FROM __turso_internal_types WHERE name = 'dollars'"
            ),
            vec![vec![text("dollars")]]
        );
        assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
        conn.execute("CREATE TYPE cents BASE text").unwrap();
        if !mvcc {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        conn.close().unwrap();
    }
    if !mvcc {
        assert_eq!(sqlite_integrity_check(&path), "ok");
    }
    let db = open_with_custom_types(&path, mvcc);
    let conn = db.connect_limbo();
    assert_eq!(
        stored_type_names(&conn),
        vec![vec![text("cents")], vec![text("dollars")]]
    );
    conn.execute("CREATE TABLE prices (amount dollars, label cents) STRICT")
        .unwrap();
    conn.execute("INSERT INTO prices VALUES (5, 'five')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT amount, label FROM prices"),
        vec![vec![int(5), text("five")]]
    );
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn types_table_index_is_kept_wal() {
    types_table_index_is_kept(false);
}

#[test]
fn types_table_index_is_kept_mvcc() {
    types_table_index_is_kept(true);
}

/// The SQL text that Turso stores for `create_type`.
fn stored_type_sql(create_type: &str) -> String {
    let dir = tempfile::tempdir().unwrap();
    let db = open_with_custom_types(&new_path(&dir), false);
    let conn = db.connect_limbo();
    conn.execute(create_type).unwrap();
    match &rows(&conn, "SELECT sql FROM __turso_internal_types")[..] {
        [row] => match &row[0] {
            SqlValue::Text(sql) => sql.clone(),
            other => panic!("the sql of a type is {other:?}"),
        },
        other => panic!("one type has {} rows", other.len()),
    }
}

/// Writes at `path` the file that an older Turso wrote after
/// `CREATE TYPE cents BASE integer`: the table of types has a
/// `name TEXT PRIMARY KEY` column but no automatic index.
fn write_old_types_database(path: &Path) {
    let cents_sql = stored_type_sql("CREATE TYPE cents BASE integer");
    let sqlite = rusqlite::Connection::open(path).unwrap();
    let mode: String = sqlite
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    sqlite
        .execute_batch("CREATE TABLE __turso_internal_types(name TEXT, sql TEXT)")
        .unwrap();
    sqlite
        .execute(
            "INSERT INTO __turso_internal_types VALUES ('cents', ?1)",
            [&cents_sql],
        )
        .unwrap();
    sqlite
        .execute_batch(
            "PRAGMA writable_schema = ON;
             UPDATE sqlite_schema
                SET sql = 'CREATE TABLE __turso_internal_types(name TEXT PRIMARY KEY, sql TEXT)'
              WHERE name = '__turso_internal_types';
             PRAGMA writable_schema = OFF;",
        )
        .unwrap();
}

/// A database that an older Turso wrote keeps working: its table of types
/// has no automatic index, and Turso goes on without one. It does not write
/// an index, because an index that misses the rows of the older types would
/// be broken.
fn old_types_table_keeps_working(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    write_old_types_database(&path);
    {
        let db = open_with_custom_types(&path, mvcc);
        let conn = db.connect_limbo();
        assert_eq!(stored_type_names(&conn), vec![vec![text("cents")]]);
        conn.execute("CREATE TABLE prices (amount cents) STRICT")
            .unwrap();
        conn.execute("INSERT INTO prices VALUES (5)").unwrap();
        conn.execute("CREATE TYPE dollars BASE integer").unwrap();
        conn.execute("CREATE TYPE euros BASE integer").unwrap();
        conn.execute("DROP TYPE dollars").unwrap();
        assert_eq!(
            stored_type_names(&conn),
            vec![vec![text("cents")], vec![text("euros")]]
        );
        assert_eq!(
            types_table_schema_rows(&conn),
            vec![vec![text("table"), text("__turso_internal_types")]]
        );
        assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
        conn.close().unwrap();
    }
    let db = open_with_custom_types(&path, mvcc);
    let conn = db.connect_limbo();
    assert_eq!(
        stored_type_names(&conn),
        vec![vec![text("cents")], vec![text("euros")]]
    );
    assert_eq!(rows(&conn, "SELECT amount FROM prices"), vec![vec![int(5)]]);
    conn.execute("CREATE TABLE costs (amount euros) STRICT")
        .unwrap();
    let err = error_of(&conn, "DROP TYPE cents");
    assert!(err.contains("used by"), "{err}");
    conn.execute("DROP TABLE prices").unwrap();
    conn.execute("DROP TYPE cents").unwrap();
    assert_eq!(stored_type_names(&conn), vec![vec![text("euros")]]);
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn old_types_table_keeps_working_wal() {
    old_types_table_keeps_working(false);
}

#[test]
fn old_types_table_keeps_working_mvcc() {
    old_types_table_keeps_working(true);
}

/// CREATE TYPE and DROP TYPE write catalog rows, which are not changes that
/// the user made: as for other DDL, `changes()`, `total_changes()` and
/// `last_insert_rowid()` keep the values of the last INSERT. The first type
/// stores the table of types, and the second one writes to it.
fn type_statements_keep_change_counters(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let db = open_with_custom_types(&new_path(&dir), mvcc);
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (a)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let counters = "SELECT changes(), total_changes(), last_insert_rowid()";
    let expected = vec![vec![int(3), int(3), int(3)]];
    assert_eq!(rows(&conn, counters), expected);
    for sql in [
        "CREATE TYPE cents BASE integer",
        "CREATE TYPE dollars BASE integer",
        "CREATE DOMAIN positive AS integer CHECK (value > 0)",
        "DROP TYPE cents",
        "DROP DOMAIN positive",
    ] {
        conn.execute(sql).unwrap();
        assert_eq!(rows(&conn, counters), expected, "after {sql}");
    }
}

#[test]
fn type_statements_keep_change_counters_wal() {
    type_statements_keep_change_counters(false);
}

#[test]
fn type_statements_keep_change_counters_mvcc() {
    type_statements_keep_change_counters(true);
}

/// The rows of the table of seed versions, by name.
fn seed_versions(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(
        conn,
        "SELECT name, version FROM __turso_internal_seed_versions ORDER BY name",
    )
}

/// The `type` and `name` of the `sqlite_schema` rows of the table of seed
/// versions.
fn seed_versions_schema_rows(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(
        conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = '__turso_internal_seed_versions' ORDER BY rowid",
    )
}

/// The first write of a seeded table stores the table of seed versions in
/// its transaction, with a row that has the version and the seed row range
/// of the seed. A rollback leaves neither the table nor the row. Each
/// seeded table that a statement stores gets its own row.
fn first_write_records_the_seed_version(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    let dialect = SeedDialect::new();
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        let err = error_of(&conn, "SELECT * FROM __turso_internal_seed_versions");
        assert!(err.contains("no such table"), "{err}");

        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE TABLE t1 (a)").unwrap();
        assert_eq!(seed_versions(&conn), vec![vec![text("seed_items"), int(1)]]);
        conn.execute("ROLLBACK").unwrap();
        let err = error_of(&conn, "SELECT * FROM __turso_internal_seed_versions");
        assert!(err.contains("no such table"), "{err}");

        conn.execute("CREATE TABLE words1 (a)").unwrap();
        assert_eq!(
            seed_versions(&conn),
            vec![
                vec![text("seed_items"), int(1)],
                vec![text("seed_words"), int(1)],
            ]
        );
        assert_eq!(
            seed_versions_schema_rows(&conn),
            vec![
                vec![text("table"), text("__turso_internal_seed_versions")],
                vec![
                    text("index"),
                    text("sqlite_autoindex___turso_internal_seed_versions_1")
                ],
            ]
        );
        // The row also has the seed row range of the seed.
        assert_eq!(
            rows(
                &conn,
                "SELECT * FROM __turso_internal_seed_versions ORDER BY name"
            ),
            vec![
                vec![text("seed_items"), int(1), text("oid"), int(0), int(31)],
                vec![text("seed_words"), int(1), text("length"), int(0), int(6)],
            ]
        );
        // A write to a stored table writes no new row.
        conn.execute("CREATE TABLE t2 (a)").unwrap();
        assert_eq!(seed_versions(&conn).len(), 2);
        let other = db.connect_limbo();
        assert_eq!(seed_versions(&other).len(), 2);
        assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
        let err = error_of(&conn, "DELETE FROM __turso_internal_seed_versions");
        assert!(err.contains("may not be modified"), "{err}");
        if !mvcc {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        conn.close().unwrap();
        other.close().unwrap();
    }
    if !mvcc {
        assert_eq!(sqlite_integrity_check(&path), "ok");
    }
    let db = open(&path, dialect, mvcc);
    let conn = db.connect_limbo();
    assert_eq!(
        seed_versions(&conn),
        vec![
            vec![text("seed_items"), int(1)],
            vec![text("seed_words"), int(1)],
        ]
    );
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn first_write_records_the_seed_version_wal() {
    first_write_records_the_seed_version(false);
}

#[test]
fn first_write_records_the_seed_version_mvcc() {
    first_write_records_the_seed_version(true);
}

/// A stored seeded table that is dropped and stored again replaces its row
/// in the table of seed versions.
fn stored_again_replaces_the_seed_version_row(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&new_path(&dir), SeedDialect::new(), mvcc);
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t1 (a)").unwrap();
    conn.execute("DROP TABLE seed_items").unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM seed_items"), seed_item_rows());
    assert_eq!(seed_versions(&conn), vec![vec![text("seed_items"), int(1)]]);
    conn.execute("CREATE TABLE t2 (a)").unwrap();
    assert_eq!(seed_versions(&conn), vec![vec![text("seed_items"), int(1)]]);
    assert_eq!(all_items(&conn), items_with_user_tables(&["t2"]));
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn stored_again_replaces_the_seed_version_row_wal() {
    stored_again_replaces_the_seed_version_row(false);
}

#[test]
fn stored_again_replaces_the_seed_version_row_mvcc() {
    stored_again_replaces_the_seed_version_row(true);
}

/// A row that the catalog writes inside the seed row range fails the
/// statement, whether the statement stores the table or the table was
/// stored before, so that a refill never deletes a row that the catalog
/// wrote. The statement changes nothing.
fn catalog_row_inside_the_seed_row_range_fails(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let dialect = SeedDialect::new();
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let items_error = "cannot write a row of seeded table seed_items: \
                       a row whose oid is at least 0 and less than 31 is a seed row";

    let err = error_of(&conn, "CREATE TABLE inside1 (a)");
    assert!(err.contains(items_error), "{err}");
    assert_read_only_items(&conn, &dialect);
    let err = error_of(&conn, "SELECT * FROM inside1");
    assert!(err.contains("no such table"), "{err}");

    conn.execute("CREATE TABLE t1 (a)").unwrap();
    let err = error_of(&conn, "CREATE TABLE inside2 (a)");
    assert!(err.contains(items_error), "{err}");
    assert_eq!(all_items(&conn), items_with_user_tables(&["t1"]));

    // The column of the range of seed_words is not the rowid.
    let err = error_of(&conn, "CREATE TABLE words (a)");
    assert!(
        err.contains(
            "cannot write a row of seeded table seed_words: \
             a row whose length is at least 0 and less than 6 is a seed row"
        ),
        "{err}"
    );
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM seed_words"),
        vec![vec![int(2)]]
    );
    let err = error_of(&conn, "SELECT * FROM words");
    assert!(err.contains("no such table"), "{err}");
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn catalog_row_inside_the_seed_row_range_fails_wal() {
    catalog_row_inside_the_seed_row_range_fails(false);
}

#[test]
fn catalog_row_inside_the_seed_row_range_fails_mvcc() {
    catalog_row_inside_the_seed_row_range_fails(true);
}

/// A seed row outside the seed row range fails the first write, which then
/// stores nothing.
fn seed_row_outside_the_seed_row_range_fails(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let rows_of_seed = SeedDialect::new().items().rows.clone();
    let dialect = SeedDialect::with_item_rows(
        rows_of_seed,
        SeedRowRange {
            column: "oid",
            range: 0..25,
        },
        &[],
    );
    let db = open(&new_path(&dir), dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let err = error_of(&conn, "CREATE TABLE t1 (a)");
    assert!(
        err.contains(
            "a seed row of seeded table seed_items is outside its seed row range: \
             oid must be at least 0 and less than 25"
        ),
        "{err}"
    );
    assert_read_only_items(&conn, &dialect);
    let err = error_of(&conn, "SELECT * FROM __turso_internal_seed_versions");
    assert!(err.contains("no such table"), "{err}");
}

#[test]
fn seed_row_outside_the_seed_row_range_fails_wal() {
    seed_row_outside_the_seed_row_range_fails(false);
}

#[test]
fn seed_row_outside_the_seed_row_range_fails_mvcc() {
    seed_row_outside_the_seed_row_range_fails(true);
}

/// The column of a seed row range must be an INTEGER column of the table.
/// A database whose dialect has another column does not open.
#[test]
fn seed_row_range_column_is_checked_at_open() {
    let rows_of_seed = SeedDialect::new().items().rows.clone();
    for (column, reason) in [
        ("missing", "the column of its seed row range does not exist"),
        (
            "name",
            "the column of its seed row range is not an INTEGER column",
        ),
        (
            "weight",
            "the column of its seed row range is not an INTEGER column",
        ),
    ] {
        let dialect = SeedDialect::with_item_rows(
            rows_of_seed.clone(),
            SeedRowRange {
                column,
                range: 0..31,
            },
            &[],
        );
        let err = turso_core::Database::open(
            Arc::new(turso_core::MemoryIO::new()),
            ":memory:",
            turso_core::OpenOptions::new(dialect),
        )
        .err()
        .unwrap_or_else(|| panic!("the open with column {column} did not fail"))
        .to_string();
        assert!(
            err.contains(&format!(
                "seeded table seed_items cannot be stored: {reason}"
            )),
            "{column}: {err}"
        );
    }
}

/// Opens the file at `path` with `dialect`, stores `seed_items` and
/// `seed_words` with the rows of the user tables `t1` and `words1`, and
/// closes it. With `checkpoint`, the rows are in the database file, and
/// otherwise in the WAL or the logical log.
fn store_both_tables(path: &Path, dialect: Arc<SeedDialect>, mvcc: bool, checkpoint: bool) {
    let db = open(path, dialect, mvcc);
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t1 (a)").unwrap();
    conn.execute("CREATE TABLE words1 (a)").unwrap();
    if checkpoint {
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    conn.close().unwrap();
}

/// The rows of `seed_words`, by length and word.
fn all_words(conn: &Arc<Connection>) -> Vec<Vec<SqlValue>> {
    rows(
        conn,
        "SELECT word, length FROM seed_words ORDER BY length, word",
    )
}

/// The rows of [`SeedDialect::new`] in `seed_words`, and the rows that the
/// catalog wrote for each of `tables`.
fn words_with_user_tables(seed: &[(&str, i64)], tables: &[&str]) -> Vec<Vec<SqlValue>> {
    let mut expected: Vec<Vec<SqlValue>> = seed
        .iter()
        .map(|(word, length)| vec![text(word), int(*length)])
        .collect();
    for table in tables {
        expected.push(vec![text(table), int(table.len() as i64)]);
    }
    expected.sort_by_key(|row| match (&row[0], &row[1]) {
        (SqlValue::Text(word), SqlValue::Integer(length)) => (*length, word.clone()),
        other => panic!("a row of seed_words is {other:?}"),
    });
    expected
}

const OLDER_WORDS: [(&str, i64); 2] = [("one", 3), ("three", 5)];
const NEWER_WORDS: [(&str, i64); 2] = [("uno", 3), ("four", 4)];

/// The rows of [`SeedDialect::newer`] in `seed_items`, then a row that the
/// catalog wrote for each of `tables`, with the oids from 31.
fn newer_items_with_user_tables(tables: &[&str]) -> Vec<Vec<SqlValue>> {
    let mut expected = vec![
        vec![int(10), text("alpha"), text("changed"), real(1.5)],
        vec![int(25), text("delta"), SqlValue::Null, real(4.0)],
        vec![int(30), text("gamma"), SqlValue::Null, real(-0.25)],
    ];
    for (position, table) in tables.iter().enumerate() {
        expected.push(vec![
            int(31 + position as i64),
            text(table),
            text("user table"),
            SqlValue::Null,
        ]);
    }
    expected
}

/// The bytes of the database file and of its WAL and logical log, for each
/// file that exists.
fn file_bytes(path: &Path) -> Vec<(String, Vec<u8>)> {
    ["", "-wal", "-log"]
        .iter()
        .filter_map(|suffix| {
            let file = format!("{}{suffix}", path.display());
            std::fs::read(&file).ok().map(|bytes| (file, bytes))
        })
        .collect()
}

/// A database whose seeded tables were filled by the seeds of the binary
/// opens and is written without a refill, and an open that only reads
/// writes nothing to the files.
fn same_version_fills_nothing_again(mvcc: bool, checkpoint: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), mvcc, checkpoint);
    let before = file_bytes(&path);
    let dialect = SeedDialect::new();
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        assert_eq!(all_items(&conn), items_with_user_tables(&["t1", "words1"]));
        assert_eq!(
            all_words(&conn),
            words_with_user_tables(&OLDER_WORDS, &["words1"])
        );
        assert_eq!(
            seed_versions(&conn),
            vec![
                vec![text("seed_items"), int(1)],
                vec![text("seed_words"), int(1)],
            ]
        );
        assert_eq!(dialect.items().calls(), (0, 0));
        conn.close().unwrap();
    }
    assert!(
        file_bytes(&path) == before,
        "an open that reads writes nothing"
    );

    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let program = opcodes(&conn, "CREATE TABLE t2 (a)");
    assert_eq!(count_opcode(&program, "VOpen"), 0, "{program:?}");
    assert_eq!(count_opcode(&program, "CatalogApply"), 0, "{program:?}");
    conn.execute("CREATE TABLE t2 (a)").unwrap();
    assert_eq!(dialect.items().calls(), (0, 0), "no refill");
    assert_eq!(
        all_items(&conn),
        items_with_user_tables(&["t1", "words1", "t2"])
    );
    assert_eq!(seed_versions(&conn).len(), 2);
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn same_version_fills_nothing_again_wal() {
    same_version_fills_nothing_again(false, false);
}

#[test]
fn same_version_fills_nothing_again_wal_after_checkpoint() {
    same_version_fills_nothing_again(false, true);
}

#[test]
fn same_version_fills_nothing_again_mvcc_before_checkpoint() {
    same_version_fills_nothing_again(true, false);
}

#[test]
fn same_version_fills_nothing_again_mvcc_after_checkpoint() {
    same_version_fills_nothing_again(true, true);
}

/// A newer binary has other seeds. Until a statement writes a stored
/// seeded table, readers see its old seed rows. The first write of each
/// table replaces its seed rows with the new ones, keeps the rows that the
/// catalog wrote, and records the new version. The indexes follow, and a
/// later write or reopen does not fill the table again.
fn changed_seed_fills_again_on_write(mvcc: bool, checkpoint: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), mvcc, checkpoint);
    let dialect = SeedDialect::newer();
    {
        let db = open(&path, dialect.clone(), mvcc);
        let conn = db.connect_limbo();
        let other = db.connect_limbo();
        assert_eq!(all_items(&conn), items_with_user_tables(&["t1", "words1"]));
        assert_eq!(
            seed_versions(&conn),
            vec![
                vec![text("seed_items"), int(1)],
                vec![text("seed_words"), int(1)],
            ]
        );
        let user_items = |conn: &Arc<Connection>| {
            rows(
                conn,
                "SELECT rowid, * FROM seed_items WHERE oid >= 31 ORDER BY rowid",
            )
        };
        let user_words = |conn: &Arc<Connection>| {
            rows(
                conn,
                "SELECT rowid, * FROM seed_words WHERE length >= 6 ORDER BY rowid",
            )
        };
        let items_before = user_items(&conn);
        let words_before = user_words(&conn);
        assert_eq!(items_before.len(), 2);
        assert_eq!(words_before.len(), 1);

        conn.execute("CREATE TABLE t2 (a)").unwrap();
        // The rows that the catalog wrote are as they were, with their
        // rowids, and the index finds them.
        assert_eq!(&user_items(&conn)[..2], &items_before[..]);
        let plan = rows(
            &conn,
            "EXPLAIN QUERY PLAN SELECT oid FROM seed_items WHERE name = 't1'",
        );
        assert!(
            format!("{plan:?}").contains("seed_items_name"),
            "the query uses the index: {plan:?}"
        );
        for (name, oid) in [("t1", 31), ("words1", 32), ("t2", 33), ("alpha", 10)] {
            assert_eq!(
                rows(
                    &conn,
                    &format!("SELECT oid FROM seed_items WHERE name = '{name}'")
                ),
                vec![vec![int(oid)]]
            );
        }
        let expected = newer_items_with_user_tables(&["t1", "words1", "t2"]);
        assert_eq!(all_items(&conn), expected);
        assert_eq!(all_items(&other), expected);
        assert_eq!(
            seed_versions(&other),
            vec![
                vec![text("seed_items"), int(2)],
                vec![text("seed_words"), int(1)],
            ]
        );
        // seed_words was not written, so it keeps its old seed rows.
        assert_eq!(
            all_words(&conn),
            words_with_user_tables(&OLDER_WORDS, &["words1"])
        );
        assert_eq!(
            rows(&conn, "SELECT oid FROM seed_items WHERE name = 'delta'"),
            vec![vec![int(25)]]
        );
        assert!(rows(&conn, "SELECT oid FROM seed_items WHERE name = 'beta'").is_empty());

        // The refill of a table without a rowid alias gives new rowids.
        conn.execute("CREATE TABLE words2 (a)").unwrap();
        assert_eq!(&user_words(&conn)[..1], &words_before[..]);
        assert_eq!(
            all_words(&conn),
            words_with_user_tables(&NEWER_WORDS, &["words1", "words2"])
        );
        assert_eq!(
            rows(&conn, "SELECT count(DISTINCT rowid) FROM seed_words"),
            vec![vec![int(4)]]
        );
        assert_eq!(
            seed_versions(&conn),
            vec![
                vec![text("seed_items"), int(2)],
                vec![text("seed_words"), int(2)],
            ]
        );
        assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
        if !mvcc {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        conn.close().unwrap();
        other.close().unwrap();
    }
    if !mvcc {
        assert_eq!(sqlite_integrity_check(&path), "ok");
    }
    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    dialect.items().reset();
    let program = opcodes(&conn, "CREATE TABLE t3 (a)");
    assert_eq!(count_opcode(&program, "VOpen"), 0, "{program:?}");
    conn.execute("CREATE TABLE t3 (a)").unwrap();
    assert_eq!(dialect.items().calls(), (0, 0), "no second refill");
    assert_eq!(
        all_items(&conn),
        newer_items_with_user_tables(&["t1", "words1", "t2", "words2", "t3"])
    );
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn changed_seed_fills_again_on_write_wal() {
    changed_seed_fills_again_on_write(false, false);
}

#[test]
fn changed_seed_fills_again_on_write_wal_after_checkpoint() {
    changed_seed_fills_again_on_write(false, true);
}

#[test]
fn changed_seed_fills_again_on_write_mvcc_before_checkpoint() {
    changed_seed_fills_again_on_write(true, false);
}

#[test]
fn changed_seed_fills_again_on_write_mvcc_after_checkpoint() {
    changed_seed_fills_again_on_write(true, true);
}

/// In a transaction, the first write fills the table again and the next
/// statement does not. Another connection sees the new rows after the
/// commit. A ROLLBACK leaves the old rows and the old version, and the next
/// write fills the table again.
fn changed_seed_fills_again_in_transaction(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), mvcc, false);
    let dialect = SeedDialect::newer();
    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let other = db.connect_limbo();

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t2 (a)").unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(all_items(&conn), items_with_user_tables(&["t1", "words1"]));
    assert_eq!(seed_versions(&conn)[0], vec![text("seed_items"), int(1)]);

    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t2 (a)").unwrap();
    let program = opcodes(&conn, "CREATE TABLE t3 (a)");
    assert_eq!(count_opcode(&program, "VOpen"), 0, "{program:?}");
    dialect.items().reset();
    conn.execute("CREATE TABLE t3 (a)").unwrap();
    assert_eq!(dialect.items().calls(), (0, 0), "no second refill");
    assert_eq!(seed_versions(&conn)[0], vec![text("seed_items"), int(2)]);
    assert_eq!(all_items(&other), items_with_user_tables(&["t1", "words1"]));
    conn.execute("COMMIT").unwrap();
    // In MVCC mode the rolled back transaction used the oid 33, so the
    // test compares the seed rows and the names of the user tables.
    let seed_part = |conn: &Arc<Connection>| rows(conn, "SELECT * FROM seed_items WHERE oid < 31");
    let expected = newer_items_with_user_tables(&[]);
    for conn in [&conn, &other] {
        assert_eq!(seed_part(conn), expected);
        assert_eq!(
            user_table_rows(conn),
            vec![
                vec![text("t1")],
                vec![text("words1")],
                vec![text("t2")],
                vec![text("t3")],
            ]
        );
    }
    assert_eq!(seed_versions(&other)[0], vec![text("seed_items"), int(2)]);
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn changed_seed_fills_again_in_transaction_wal() {
    changed_seed_fills_again_in_transaction(false);
}

#[test]
fn changed_seed_fills_again_in_transaction_mvcc() {
    changed_seed_fills_again_in_transaction(true);
}

/// A refill that fails changes nothing: the old rows and the old version
/// stay. Inside a transaction, the transaction goes on and commits.
fn failed_refill_changes_nothing(mvcc: bool, explicit_transaction: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), mvcc, false);
    // The newer seed has a row outside its range, which stops the refill
    // after it deleted the old seed rows.
    let mut seeds = newer_seeds();
    seeds.items.push(vec![
        Value::from_i64(40),
        Value::from_text("outside"),
        Value::Null,
        Value::Null,
    ]);
    let dialect = SeedDialect::with_seeds(seeds, &[]);
    let db = open(&path, dialect, mvcc);
    let conn = db.connect_limbo();
    let other = db.connect_limbo();
    if explicit_transaction {
        conn.execute("BEGIN").unwrap();
    }
    let err = error_of(&conn, "CREATE TABLE t2 (a)");
    assert!(
        err.contains(
            "a seed row of seeded table seed_items is outside its seed row range: \
             oid must be at least 0 and less than 31"
        ),
        "{err}"
    );
    let unchanged = |conn: &Arc<Connection>| {
        assert_eq!(all_items(conn), items_with_user_tables(&["t1", "words1"]));
        assert_eq!(
            seed_versions(conn),
            vec![
                vec![text("seed_items"), int(1)],
                vec![text("seed_words"), int(1)],
            ]
        );
        let err = error_of(conn, "SELECT * FROM t2");
        assert!(err.contains("no such table"), "{err}");
    };
    unchanged(&conn);
    if explicit_transaction {
        conn.execute("CREATE TABLE plain_kept (a)").unwrap();
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            rows(&other, "SELECT count(*) FROM plain_kept"),
            vec![vec![int(0)]]
        );
    }
    unchanged(&other);
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn failed_refill_changes_nothing_wal() {
    failed_refill_changes_nothing(false, false);
}

#[test]
fn failed_refill_changes_nothing_wal_in_transaction() {
    failed_refill_changes_nothing(false, true);
}

#[test]
fn failed_refill_changes_nothing_mvcc() {
    failed_refill_changes_nothing(true, false);
}

#[test]
fn failed_refill_changes_nothing_mvcc_in_transaction() {
    failed_refill_changes_nothing(true, true);
}

/// A statement that another connection prepared before a refill prepares
/// again after it, and does not fill the table a second time. A second
/// refill that starts while the first is not committed fails as two schema
/// changes do, and its retry fills nothing.
fn refill_on_two_connections(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), mvcc, false);
    let dialect = SeedDialect::newer();
    let db = open(&path, dialect.clone(), mvcc);
    let a = db.connect_limbo();
    let b = db.connect_limbo();

    let mut prepared = b.prepare("CREATE TABLE b1 (a)").unwrap();
    a.execute("CREATE TABLE a1 (a)").unwrap();
    dialect.items().reset();
    prepared.run_ignore_rows().unwrap();
    assert_eq!(dialect.items().calls(), (0, 0), "no second refill");
    assert_eq!(
        all_items(&b),
        newer_items_with_user_tables(&["t1", "words1", "a1", "b1"])
    );

    a.execute("BEGIN").unwrap();
    a.execute("CREATE TABLE words2 (a)").unwrap();
    b.execute("BEGIN").unwrap();
    let err = b.execute("CREATE TABLE words3 (a)").unwrap_err();
    assert!(matches!(err, LimboError::Busy), "{err:?}");
    a.execute("COMMIT").unwrap();
    b.execute("ROLLBACK").unwrap();
    dialect.words().reset();
    b.execute("CREATE TABLE words3 (a)").unwrap();
    assert_eq!(dialect.words().calls(), (0, 0), "no second refill");
    assert_eq!(
        all_words(&a),
        words_with_user_tables(&NEWER_WORDS, &["words1", "words2", "words3"])
    );
    assert_eq!(seed_versions(&a)[1], vec![text("seed_words"), int(2)]);
    assert_eq!(turso_integrity_check(&a), vec![vec![text("ok")]]);
}

#[test]
fn refill_on_two_connections_wal() {
    refill_on_two_connections(false);
}

#[test]
fn refill_on_two_connections_mvcc() {
    refill_on_two_connections(true);
}

/// A read-only open of a database whose seeds are old shows the old seed
/// rows, fails a write and changes no file.
///
/// The test records what core does now. What a read-only database with old
/// seed rows must show is an open question for the user.
fn read_only_open_of_old_seeds(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), mvcc, false);
    let before = file_bytes(&path);
    {
        let db = TempDatabase::builder()
            .with_db_path(&path)
            .with_dialect(SeedDialect::newer())
            .with_flags(turso_core::OpenFlags::ReadOnly)
            .build();
        let conn = db.connect_limbo();
        assert_eq!(all_items(&conn), items_with_user_tables(&["t1", "words1"]));
        assert_eq!(seed_versions(&conn)[0], vec![text("seed_items"), int(1)]);
        assert!(conn.execute("CREATE TABLE t2 (a)").is_err());
        assert_eq!(all_items(&conn), items_with_user_tables(&["t1", "words1"]));
        conn.close().unwrap();
    }
    assert!(
        file_bytes(&path) == before,
        "a read-only open writes nothing"
    );
}

#[test]
fn read_only_open_of_old_seeds_wal() {
    read_only_open_of_old_seeds(false);
}

#[test]
fn read_only_open_of_old_seeds_mvcc() {
    read_only_open_of_old_seeds(true);
}

/// A database whose seeded tables were stored before core kept seed
/// versions has no table of seed versions. Core does not know which seed
/// filled the tables, so it never fills them again, and it adds no version.
#[test]
fn table_stored_without_a_version_is_never_filled_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), false, true);
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch("DROP TABLE __turso_internal_seed_versions")
        .unwrap();
    drop(sqlite);

    let dialect = SeedDialect::newer();
    let db = open(&path, dialect.clone(), false);
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t2 (a)").unwrap();
    conn.execute("CREATE TABLE words2 (a)").unwrap();
    assert_eq!(dialect.items().calls(), (0, 0));
    assert_eq!(dialect.words().calls(), (0, 0));
    assert_eq!(
        all_items(&conn),
        items_with_user_tables(&["t1", "words1", "t2", "words2"])
    );
    assert_eq!(
        all_words(&conn),
        words_with_user_tables(&OLDER_WORDS, &["words1", "words2"])
    );
    let err = error_of(&conn, "SELECT * FROM __turso_internal_seed_versions");
    assert!(err.contains("no such table"), "{err}");
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

fn open_with_dialect_and_custom_types(
    path: &Path,
    dialect: Arc<SeedDialect>,
    mvcc: bool,
) -> TempDatabase {
    TempDatabase::builder()
        .with_db_path(path)
        .with_dialect(dialect)
        .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
        .with_mvcc(mvcc)
        .build()
}

/// The table of types is a seeded table of core with no version and no
/// seed rows: a refill of a seeded table of the catalog leaves it and its
/// rows as they are.
fn refill_leaves_the_types_table(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    {
        let db = open_with_dialect_and_custom_types(&path, SeedDialect::new(), mvcc);
        let conn = db.connect_limbo();
        conn.execute("CREATE TYPE cents BASE integer").unwrap();
        conn.execute("CREATE TABLE t1 (a)").unwrap();
        conn.close().unwrap();
    }
    let db = open_with_dialect_and_custom_types(&path, SeedDialect::newer(), mvcc);
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t2 (a)").unwrap();
    assert_eq!(
        all_items(&conn),
        newer_items_with_user_tables(&["t1", "t2"])
    );
    assert_eq!(stored_type_names(&conn), vec![vec![text("cents")]]);
    assert_eq!(seed_versions(&conn), vec![vec![text("seed_items"), int(2)]]);
    conn.execute("CREATE TYPE dollars BASE integer").unwrap();
    conn.execute("DROP TYPE cents").unwrap();
    assert_eq!(stored_type_names(&conn), vec![vec![text("dollars")]]);
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn refill_leaves_the_types_table_wal() {
    refill_leaves_the_types_table(false);
}

#[test]
fn refill_leaves_the_types_table_mvcc() {
    refill_leaves_the_types_table(true);
}

/// A newer binary whose seed has another seed row range does not fill the
/// table again: a row that the catalog wrote outside the old range can be
/// inside the new one, and a refill would delete it. The statement fails
/// and changes nothing.
fn changed_seed_row_range_refuses_the_refill(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    let older_items = SeedDialect::new().items().rows.clone();
    {
        let dialect = SeedDialect::with_item_rows(
            older_items.clone(),
            SeedRowRange {
                column: "oid",
                range: 0..100,
            },
            &[],
        );
        let db = open(&path, dialect, mvcc);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE far1 (a)").unwrap();
        conn.close().unwrap();
    }
    let mut seeds = newer_seeds();
    seeds.item_rows = SeedRowRange {
        column: "oid",
        range: 0..200,
    };
    let dialect = SeedDialect::with_seeds(seeds, &[]);
    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let expected = {
        let mut expected = seed_item_rows();
        expected.push(vec![
            int(150),
            text("far1"),
            text("user table"),
            SqlValue::Null,
        ]);
        expected
    };
    assert_eq!(all_items(&conn), expected);
    let err = error_of(&conn, "CREATE TABLE t2 (a)");
    assert!(
        err.contains(
            "cannot fill seeded table seed_items again: its seed row range changed from \
             oid at least 0 and less than 100 to oid at least 0 and less than 200"
        ),
        "{err}"
    );
    assert_eq!(dialect.items().calls(), (0, 0));
    assert_eq!(all_items(&conn), expected);
    assert_eq!(
        rows(&conn, "SELECT * FROM __turso_internal_seed_versions"),
        vec![vec![
            text("seed_items"),
            int(1),
            text("oid"),
            int(0),
            int(100)
        ]]
    );
    let err = error_of(&conn, "SELECT * FROM t2");
    assert!(err.contains("no such table"), "{err}");
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn changed_seed_row_range_refuses_the_refill_wal() {
    changed_seed_row_range_refuses_the_refill(false);
}

#[test]
fn changed_seed_row_range_refuses_the_refill_mvcc() {
    changed_seed_row_range_refuses_the_refill(true);
}

/// A refill bumps the schema cookie although it changes no schema row. A
/// statement that writes a temporary table changes only the cookie of the
/// temp database, but its catalog row fills `seed_items` of the main
/// database again. A statement that another connection prepared before
/// must then prepare again: it sees the new version and does not fill the
/// table a second time.
fn refill_makes_other_connections_prepare_again(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), mvcc, false);
    let dialect = SeedDialect::newer();
    let db = open(&path, dialect.clone(), mvcc);
    let a = db.connect_limbo();
    let b = db.connect_limbo();
    let mut prepared = b.prepare("CREATE TABLE b1 (a)").unwrap();

    let program = opcodes(&a, "CREATE TEMP TABLE tempwrite1 (a)");
    assert_eq!(count_opcode(&program, "VOpen"), 1, "{program:?}");
    a.execute("CREATE TEMP TABLE tempwrite1 (a)").unwrap();
    assert_eq!(seed_versions(&b)[0], vec![text("seed_items"), int(2)]);

    dialect.items().reset();
    prepared.run_ignore_rows().unwrap();
    assert_eq!(dialect.items().calls(), (0, 0), "no second refill");
    assert_eq!(
        all_items(&b),
        newer_items_with_user_tables(&["t1", "words1", "tempwrite1", "b1"])
    );
    assert_eq!(seed_versions(&b)[0], vec![text("seed_items"), int(2)]);
    assert_eq!(turso_integrity_check(&b), vec![vec![text("ok")]]);
}

#[test]
fn refill_makes_other_connections_prepare_again_wal() {
    refill_makes_other_connections_prepare_again(false);
}

#[test]
fn refill_makes_other_connections_prepare_again_mvcc() {
    refill_makes_other_connections_prepare_again(true);
}

/// How a test rolls back a refill.
#[derive(Clone, Copy, Debug)]
enum RefillRollback {
    /// The statement fails after the refill in autocommit mode: the
    /// catalog's row has the oid of a seed row.
    FailedStatement,
    /// The same statement fails inside a transaction, which goes on.
    FailedStatementInTransaction,
    /// The statement succeeds inside a transaction, and ROLLBACK ends it.
    Rollback,
}

/// A refill that is rolled back leaves the old seed rows, the old version
/// and every row that the catalog wrote. The next write fills the table
/// again, once.
fn rolled_back_refill_leaves_old_rows(mvcc: bool, rollback: RefillRollback) {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), mvcc, false);
    let dialect = SeedDialect::newer();
    let db = open(&path, dialect.clone(), mvcc);
    let conn = db.connect_limbo();
    let other = db.connect_limbo();
    let unchanged = |conn: &Arc<Connection>| {
        assert_eq!(
            all_items(conn),
            items_with_user_tables(&["t1", "words1"]),
            "{rollback:?}"
        );
        assert_eq!(
            seed_versions(conn)[0],
            vec![text("seed_items"), int(1)],
            "{rollback:?}"
        );
    };
    match rollback {
        RefillRollback::FailedStatement => {
            let err = error_of(&conn, "CREATE TABLE dup1 (a)");
            assert!(err.contains("UNIQUE constraint failed"), "{err}");
        }
        RefillRollback::FailedStatementInTransaction => {
            conn.execute("BEGIN").unwrap();
            let err = error_of(&conn, "CREATE TABLE dup1 (a)");
            assert!(err.contains("UNIQUE constraint failed"), "{err}");
            unchanged(&conn);
        }
        RefillRollback::Rollback => {
            conn.execute("BEGIN").unwrap();
            conn.execute("CREATE TABLE t2 (a)").unwrap();
            assert_eq!(seed_versions(&conn)[0], vec![text("seed_items"), int(2)]);
            conn.execute("ROLLBACK").unwrap();
        }
    }
    unchanged(&conn);
    unchanged(&other);

    dialect.items().reset();
    conn.execute("CREATE TABLE t3 (a)").unwrap();
    conn.execute("CREATE TABLE t4 (a)").unwrap();
    // One refill reads each of the 3 seed rows once.
    assert_eq!(dialect.items().calls(), (0, 3), "one refill");
    if !conn.get_auto_commit() {
        conn.execute("COMMIT").unwrap();
    }
    assert_eq!(
        user_table_rows(&other),
        vec![
            vec![text("t1")],
            vec![text("words1")],
            vec![text("t3")],
            vec![text("t4")],
        ]
    );
    assert_eq!(
        rows(&other, "SELECT * FROM seed_items WHERE oid < 31"),
        newer_items_with_user_tables(&[])
    );
    assert_eq!(seed_versions(&other)[0], vec![text("seed_items"), int(2)]);
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}

#[test]
fn failed_statement_rolls_back_the_refill_wal() {
    rolled_back_refill_leaves_old_rows(false, RefillRollback::FailedStatement);
}

#[test]
fn failed_statement_rolls_back_the_refill_mvcc() {
    rolled_back_refill_leaves_old_rows(true, RefillRollback::FailedStatement);
}

#[test]
fn failed_statement_in_transaction_rolls_back_the_refill_wal() {
    rolled_back_refill_leaves_old_rows(false, RefillRollback::FailedStatementInTransaction);
}

#[test]
fn failed_statement_in_transaction_rolls_back_the_refill_mvcc() {
    rolled_back_refill_leaves_old_rows(true, RefillRollback::FailedStatementInTransaction);
}

#[test]
fn rollback_rolls_back_the_refill_wal() {
    rolled_back_refill_leaves_old_rows(false, RefillRollback::Rollback);
}

#[test]
fn rollback_rolls_back_the_refill_mvcc() {
    rolled_back_refill_leaves_old_rows(true, RefillRollback::Rollback);
}

/// A stored row inside the seed row range belongs to the seed, also when
/// the old seed did not have it, as in a file that another program wrote.
/// A refill deletes it with the old seed rows: that is the contract of the
/// range.
#[test]
fn refill_deletes_a_row_inside_the_range_that_the_seed_did_not_have() {
    let dir = tempfile::tempdir().unwrap();
    let path = new_path(&dir);
    store_both_tables(&path, SeedDialect::new(), false, true);
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute(
            "INSERT INTO seed_items VALUES (15, 'crafted', NULL, NULL)",
            [],
        )
        .unwrap();
    drop(sqlite);

    let db = open(&path, SeedDialect::newer(), false);
    let conn = db.connect_limbo();
    assert_eq!(
        rows(&conn, "SELECT name FROM seed_items WHERE oid = 15"),
        vec![vec![text("crafted")]]
    );
    conn.execute("CREATE TABLE t2 (a)").unwrap();
    assert_eq!(
        all_items(&conn),
        newer_items_with_user_tables(&["t1", "words1", "t2"])
    );
    assert!(rows(&conn, "SELECT oid FROM seed_items WHERE name = 'crafted'").is_empty());
    assert_eq!(turso_integrity_check(&conn), vec![vec![text("ok")]]);
}
