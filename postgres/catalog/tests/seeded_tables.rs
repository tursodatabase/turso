//! The catalogs of `turso_pg_catalog` as seeded tables of core: a dialect
//! whose catalog returns all of `SEEDED_TABLES` reads their rows before
//! anything is stored, and stores them all on a first write.

use std::num::NonZero;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use turso_core::dialect::{
    Catalog, CatalogCmd, CmdContext, CmdOrigin, Dialect, SeededTable, SqliteCatalog,
};
use turso_core::{
    ConnectOpts, Connection, Database, OpenOptions, PlatformIO, ProgramBuilder, Result,
    SessionRole, Statement, Value,
};
use turso_pg_catalog::SEEDED_TABLES;
use turso_sqlite::SqliteDialect;

/// The name of a table whose creation stores every seeded table.
const STORE_ALL: &str = "store_seeds";

/// A catalog that stores objects as SQLite does and has every catalog of
/// `turso_pg_catalog` as a seeded table. `CREATE TABLE store_seeds` also
/// opens each seeded table for writing, which stores it.
struct AllCatalogs;

impl Catalog for AllCatalogs {
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
        if ctx.origin == CmdOrigin::User
            && ctx.database_id == 0
            && !table.temporary
            && table.name == STORE_ALL
        {
            for seeded_table in &SEEDED_TABLES {
                ctx.open_seeded_table_for_write(program, seeded_table.name)?;
            }
        }
        Ok(())
    }

    fn seeded_tables(&self) -> &'static [SeededTable] {
        &SEEDED_TABLES
    }
}

/// The SQLite dialect with the catalog [`AllCatalogs`].
struct AllCatalogsDialect {
    catalog: AllCatalogs,
}

impl Dialect for AllCatalogsDialect {
    fn name(&self) -> &'static str {
        "pg-catalog-seed-test"
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

/// Opens the database at `path` with [`AllCatalogsDialect`], in MVCC mode
/// when `mvcc` is true, and connects to it.
fn open(path: &Path, mvcc: bool) -> (Arc<Database>, Arc<Connection>) {
    let io = Arc::new(PlatformIO::new().unwrap());
    let dialect = Arc::new(AllCatalogsDialect {
        catalog: AllCatalogs,
    });
    let db = Database::open(io, path.to_str().unwrap(), OpenOptions::new(dialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    if mvcc {
        conn.pragma_update("journal_mode", "'mvcc'").unwrap();
    }
    (db, conn)
}

fn run(stmt: &mut Statement) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect());
        Ok(())
    })
    .unwrap();
    rows
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    run(&mut stmt)
}

fn int(value: i64) -> Value {
    Value::from_i64(value)
}

fn text(value: &str) -> Value {
    Value::from_text(value.to_string())
}

/// The rows of the seed of `table`, in the order of the seed.
fn seed_rows(table: &SeededTable) -> Vec<Vec<Value>> {
    (0..table.seed.len())
        .map(|index| {
            let mut row = Vec::new();
            table.seed.row(index, &mut row);
            row
        })
        .collect()
}

/// Whether the rowid of `table` is its column `oid`.
fn oid_is_rowid(table: &SeededTable) -> bool {
    table.create_sql.contains("(oid INTEGER PRIMARY KEY,")
}

/// Every row of every table, in rowid order.
fn all_rows(conn: &Arc<Connection>) -> Vec<Vec<Vec<Value>>> {
    SEEDED_TABLES
        .iter()
        .map(|table| {
            rows(
                conn,
                &format!("SELECT * FROM {} ORDER BY rowid", table.name),
            )
        })
        .collect()
}

fn read_only_tables_have_the_seed_rows(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let (_db, conn) = open(&dir.path().join("seeded.db"), mvcc);

    for table in &SEEDED_TABLES {
        let name = table.name;
        assert_eq!(
            rows(&conn, &format!("SELECT count(*) FROM {name}")),
            [[int(table.seed.len() as i64)]],
            "{name}"
        );
        assert_eq!(
            rows(&conn, &format!("SELECT * FROM {name}")),
            seed_rows(table),
            "{name}"
        );
    }
    // No catalog is stored.
    assert_eq!(
        rows(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'pg%'"
        ),
        [[int(0)]]
    );

    assert_eq!(
        rows(
            &conn,
            "SELECT typname, typlen, typbyval, typcategory, typinput FROM pg_type WHERE oid = 16"
        ),
        [[text("bool"), int(1), int(1), text("B"), int(1242)]]
    );
    assert_eq!(
        rows(&conn, "SELECT proname FROM pg_proc WHERE oid = 1242"),
        [[text("boolin")]]
    );
    assert_eq!(
        rows(&conn, "SELECT * FROM pg_cast WHERE oid = 10000"),
        [[int(10000), int(20), int(21), int(714), text("a"), text("f")]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT proname, pronargs, proargtypes, proargnames, typeof(procost) \
             FROM pg_proc WHERE oid = 2284"
        ),
        [[
            text("regexp_replace"),
            int(3),
            text("25 25 25"),
            text("{string,pattern,replacement}"),
            text("real"),
        ]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT oprname, oprleft, oprright, oprresult, oprcode FROM pg_operator WHERE oid = 96"
        ),
        [[text("="), int(23), int(23), int(16), int(65)]]
    );
    assert_eq!(
        rows(&conn, "SELECT nspname FROM pg_namespace ORDER BY oid"),
        [[text("pg_catalog")], [text("pg_toast")], [text("public")]]
    );

    // Each row of each catalog with an oid is found by its oid.
    let mut lookups = 0;
    let expected_lookups: usize = SEEDED_TABLES
        .iter()
        .filter(|table| oid_is_rowid(table))
        .map(|table| table.seed.len())
        .sum();
    for table in SEEDED_TABLES.iter().filter(|table| oid_is_rowid(table)) {
        let mut stmt = conn
            .prepare(format!("SELECT * FROM {} WHERE oid = ?", table.name))
            .unwrap();
        for row in seed_rows(table) {
            stmt.reset().unwrap();
            stmt.bind_at(NonZero::new(1).unwrap(), row[0].clone())
                .unwrap();
            assert_eq!(run(&mut stmt), [row.clone()], "{}", table.name);
            lookups += 1;
        }
        stmt.reset().unwrap();
        stmt.bind_at(NonZero::new(1).unwrap(), int(4_000_000_000))
            .unwrap();
        assert!(run(&mut stmt).is_empty(), "{}", table.name);
    }
    assert_eq!(lookups, expected_lookups);
}

#[test]
fn read_only_tables_have_the_seed_rows_wal() {
    read_only_tables_have_the_seed_rows(false);
}

#[test]
fn read_only_tables_have_the_seed_rows_mvcc() {
    read_only_tables_have_the_seed_rows(true);
}

/// One statement stores every seeded table: each table and its indexes,
/// with the seed rows. The copy checks NOT NULL and each UNIQUE index, so
/// the seeds satisfy them.
fn first_write_stores_every_seed(mvcc: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seeded.db");
    let before;
    {
        let (_db, conn) = open(&path, mvcc);
        before = all_rows(&conn);

        let start = Instant::now();
        conn.execute(format!("CREATE TABLE {STORE_ALL}(x)"))
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(all_rows(&conn), before);
        for table in &SEEDED_TABLES {
            let name = table.name;
            assert_eq!(
                rows(&conn, &format!("SELECT count(*) FROM {name}")),
                [[int(table.seed.len() as i64)]],
                "{name}"
            );
            assert_eq!(
                rows(
                    &conn,
                    &format!(
                        "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = '{name}'"
                    )
                ),
                [[int(1)]],
                "{name}"
            );
        }
        let indexes = rows(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND name LIKE 'pg_%'",
        );
        let expected_indexes: usize = SEEDED_TABLES
            .iter()
            .map(|table| table.index_sql.len())
            .sum();
        assert_eq!(indexes, [[int(expected_indexes as i64)]]);
        assert_eq!(rows(&conn, "PRAGMA integrity_check"), [[text("ok")]]);
        if !mvcc {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        conn.close().unwrap();

        let size = std::fs::metadata(&path).unwrap().len();
        // In MVCC mode the rows are in the logical log until a checkpoint.
        let files: u64 = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum();
        eprintln!(
            "first write of all {} seeded tables ({}, debug build): {elapsed:?}, \
             database file {size} bytes, all files {files} bytes",
            SEEDED_TABLES.len(),
            if mvcc { "MVCC" } else { "WAL" },
        );
    }
    if !mvcc {
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        let check: String = sqlite
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(check, "ok");
        let count: i64 = sqlite
            .query_row("SELECT count(*) FROM pg_proc", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, turso_pg_catalog::PG_PROC.len() as i64);
    }

    // The stored tables keep their rows after the database opens again.
    let (_db, conn) = open(&path, mvcc);
    assert_eq!(all_rows(&conn), before);
    assert_eq!(rows(&conn, "PRAGMA integrity_check"), [[text("ok")]]);
}

#[test]
fn first_write_stores_every_seed_wal() {
    first_write_stores_every_seed(false);
}

#[test]
fn first_write_stores_every_seed_mvcc() {
    first_write_stores_every_seed(true);
}
