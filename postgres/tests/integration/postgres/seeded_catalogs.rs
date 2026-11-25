//! The catalogs with the built-in rows of PostgreSQL 18, which core serves
//! as seeded tables: pg_opclass, pg_opfamily, pg_amop and pg_collation.
//! pg_cast, pg_operator, pg_amproc and pg_range stay empty until pg_proc has
//! the oids of PostgreSQL.

use crate::common::TempDatabase;
use std::path::Path;
use std::sync::Arc;
use turso_core::{Insn, Value};
use turso_pg::{ErrorReport, PgConnection, TextType};

/// Each seeded catalog, with its oid in pg_class and its number of rows.
const CATALOGS: [(&str, i64, usize); 4] = [
    ("pg_opclass", 2616, turso_pg_catalog::PG_OPCLASS.len()),
    ("pg_opfamily", 2753, turso_pg_catalog::PG_OPFAMILY.len()),
    ("pg_amop", 2602, turso_pg_catalog::PG_AMOP.len()),
    ("pg_collation", 3456, turso_pg_catalog::PG_COLLATION.len()),
];

/// The catalogs whose rows refer to functions, which stay empty, with their
/// number of columns.
const EMPTY_CATALOGS: [(&str, usize); 4] = [
    ("pg_operator", 15),
    ("pg_cast", 6),
    ("pg_amproc", 6),
    ("pg_range", 7),
];

/// A new database, in MVCC mode when `mvcc` is true.
fn open(mvcc: bool) -> TempDatabase {
    let db = TempDatabase::builder()
        .with_opts(
            turso_core::DatabaseOpts::new()
                .with_encryption(true)
                .with_mvcc(mvcc),
        )
        .build();
    // A new database of the PostgreSQL frontend can start in MVCC mode.
    let core = db.connect_limbo();
    if journal_mode(&core) != mvcc_mode_name(mvcc) {
        core.pragma_update("journal_mode", format!("'{}'", mvcc_mode_name(mvcc)))
            .unwrap();
    }
    assert_eq!(journal_mode(&core), mvcc_mode_name(mvcc));
    drop(core);
    db
}

fn journal_mode(conn: &Arc<turso_core::Connection>) -> String {
    let rows = conn.pragma_query("journal_mode").unwrap();
    rows[0][0].to_string()
}

fn mvcc_mode_name(mvcc: bool) -> &'static str {
    if mvcc {
        "mvcc"
    } else {
        "wal"
    }
}

fn rows(conn: &PgConnection, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .run_collect_rows()
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

fn int(value: i64) -> Value {
    Value::from_i64(value)
}

fn text(value: &str) -> Value {
    Value::build_text(value.to_string())
}

/// The SQLSTATE and the message of the error of `sql`.
fn error_of(conn: &PgConnection, sql: &str) -> (&'static str, String) {
    let error = match conn.execute(sql) {
        Ok(()) => panic!("{sql} did not fail"),
        Err(error) => error,
    };
    let report = ErrorReport::from(error.core_error());
    (report.sqlstate, report.message)
}

/// The names of the columns of `catalog` in `postgres.bki`, in order.
fn bki_columns(catalog: &str) -> Vec<String> {
    let bki = include_str!("../../../catalog/upstream/postgres.bki");
    let start = bki
        .find(&format!("\ncreate {catalog} "))
        .unwrap_or_else(|| panic!("postgres.bki has no catalog {catalog}"));
    let block = &bki[start..];
    let open = block.find("\n (\n").unwrap();
    let close = block.find("\n )\n").unwrap();
    block[open + 4..close]
        .lines()
        .map(|line| line.split_whitespace().next().unwrap().to_string())
        .collect()
}

fn columns_and_rows_are_those_of_postgres_18(mvcc: bool) {
    let db = open(mvcc);
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for (catalog, table_oid, row_count) in CATALOGS {
        let expected = bki_columns(catalog);
        assert!(!expected.is_empty(), "{catalog}");
        for name in [catalog.to_string(), format!("pg_catalog.{catalog}")] {
            let mut stmt = conn.prepare(format!("SELECT * FROM {name}")).unwrap();
            let columns: Vec<String> = (0..stmt.num_columns())
                .map(|i| stmt.get_column_name(i).to_string())
                .collect();
            assert_eq!(columns, expected, "{name}");
            assert_eq!(stmt.run_collect_rows().unwrap().len(), row_count, "{name}");
            assert_eq!(
                rows(&conn, &format!("SELECT DISTINCT tableoid FROM {name} AS c"),),
                vec![vec![int(table_oid)]],
                "{name}"
            );
            assert_eq!(
                rows(&conn, &format!("SELECT count(c.tableoid) FROM {name} c")),
                vec![vec![int(row_count as i64)]],
                "{name}"
            );
        }
    }
}

#[test]
fn columns_and_rows_are_those_of_postgres_18_wal() {
    columns_and_rows_are_those_of_postgres_18(false);
}

#[test]
fn columns_and_rows_are_those_of_postgres_18_mvcc() {
    columns_and_rows_are_those_of_postgres_18(true);
}

fn builtin_rows_have_the_values_of_postgres_18(mvcc: bool) {
    let db = open(mvcc);
    let conn = db.connect_postgres();
    assert_eq!(
        rows(&conn, "SELECT * FROM pg_opclass WHERE oid = 1978"),
        vec![vec![
            int(1978),
            int(403),
            text("int4_ops"),
            int(11),
            int(10),
            int(1976),
            int(23),
            int(1),
            int(0)
        ]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT f.opfname, t.typname FROM pg_catalog.pg_amop o
             JOIN pg_opfamily f ON f.oid = o.amopfamily
             JOIN pg_type t ON t.oid = o.amoplefttype
             WHERE o.amopopr = 96 AND o.amopmethod = 403"
        ),
        vec![vec![text("integer_ops"), text("int4")]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT collname FROM pg_collation
             WHERE collname IN ('default', 'C', 'POSIX') ORDER BY oid"
        ),
        vec![vec![text("default")], vec![text("C")], vec![text("POSIX")]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT opcname FROM pg_opclass WHERE opcdefault AND opcintype = 23
             AND opcmethod = 403"
        ),
        vec![vec![text("int4_ops")]]
    );
}

#[test]
fn builtin_rows_have_the_values_of_postgres_18_wal() {
    builtin_rows_have_the_values_of_postgres_18(false);
}

#[test]
fn builtin_rows_have_the_values_of_postgres_18_mvcc() {
    builtin_rows_have_the_values_of_postgres_18(true);
}

/// A boolean column prints `t` or `f`, as the boolean columns of the other
/// catalogs do.
#[test]
fn boolean_columns_print_as_booleans() {
    let db = open(false);
    let conn = db.connect_postgres();
    let mut stmt = conn
        .prepare(
            "SELECT opcdefault, opcintype FROM pg_opclass WHERE oid IN (1978, 10105) ORDER BY oid",
        )
        .unwrap();
    assert_eq!(stmt.get_column_decltype(0).as_deref(), Some("BOOLEAN"));
    assert_eq!(TextType::of_column(&stmt, 0), TextType::Boolean);
    assert_eq!(TextType::of_column(&stmt, 1), TextType::Plain);
    let printed: Vec<Vec<String>> = stmt
        .run_collect_rows()
        .unwrap()
        .iter()
        .map(|row| {
            row.iter()
                .zip([TextType::Boolean, TextType::Plain])
                .map(|(value, text_type)| {
                    turso_pg::value_text(value, text_type).unwrap().into_owned()
                })
                .collect()
        })
        .collect();
    assert_eq!(printed, [["t", "23"], ["f", "23"]]);
}

/// The `VFilter` instructions of the program of `sql`, as `(idx_num,
/// arg_count)`.
fn seed_filters(conn: &PgConnection, sql: &str) -> Vec<(usize, usize)> {
    let stmt = conn.prepare(sql).unwrap();
    stmt.get_program()
        .insns
        .iter()
        .filter_map(|(insn, _)| match insn {
            Insn::VFilter {
                idx_num, arg_count, ..
            } => Some((*idx_num, *arg_count)),
            _ => None,
        })
        .collect()
}

/// A lookup by oid reads one row of the seed, and does not scan.
#[test]
fn lookup_by_oid_does_not_scan() {
    let db = open(false);
    let conn = db.connect_postgres();
    // idx_num 1 is the lookup of one row, and 0 the scan of all rows.
    assert_eq!(
        seed_filters(&conn, "SELECT opcname FROM pg_opclass WHERE oid = 1978"),
        [(1, 1)]
    );
    assert_eq!(
        seed_filters(
            &conn,
            "SELECT opcname FROM pg_catalog.pg_opclass WHERE 1978 = oid"
        ),
        [(1, 1)]
    );
    assert_eq!(
        seed_filters(&conn, "SELECT collname FROM pg_collation WHERE oid = 950"),
        [(1, 1)]
    );
    assert_eq!(
        seed_filters(
            &conn,
            "SELECT opcname FROM pg_opclass WHERE opcname = 'int4_ops'"
        ),
        [(0, 0)]
    );
    assert_eq!(
        rows(&conn, "SELECT opcname FROM pg_opclass WHERE oid = 1978"),
        vec![vec![text("int4_ops")]]
    );
    assert!(rows(&conn, "SELECT opcname FROM pg_opclass WHERE oid = 1").is_empty());
}

fn writes_are_refused(mvcc: bool) {
    let db = open(mvcc);
    let conn = db.connect_postgres();
    for (catalog, _, _) in CATALOGS {
        // PostgreSQL lets a superuser change the rows, which TursoPG cannot
        // do as PostgreSQL does.
        let not_supported = (
            "0A000",
            format!("changing the rows of pg_catalog table \"{catalog}\" is not supported"),
        );
        for sql in [
            format!("DELETE FROM {catalog}"),
            format!("DELETE FROM pg_catalog.{catalog} WHERE false"),
            format!("UPDATE {catalog} SET tableoid = 0"),
            format!("INSERT INTO {catalog} DEFAULT VALUES"),
        ] {
            assert_eq!(error_of(&conn, &sql), not_supported, "{sql}");
        }
        // PostgreSQL refuses these for a superuser too.
        assert_eq!(
            error_of(&conn, &format!("TRUNCATE {catalog}")),
            (
                "42501",
                format!("cannot delete from pg_catalog table \"{catalog}\"")
            )
        );
        assert_eq!(
            error_of(&conn, &format!("DROP TABLE {catalog}")),
            (
                "42501",
                format!("permission denied: \"{catalog}\" is a system catalog")
            )
        );
        assert_eq!(
            error_of(&conn, &format!("ALTER TABLE {catalog} ADD COLUMN x int")),
            (
                "42501",
                format!("permission denied: \"{catalog}\" is a system catalog")
            )
        );
        assert_eq!(
            error_of(&conn, &format!("CREATE TABLE pg_catalog.{catalog} (x int)")),
            ("42P07", format!("relation \"{catalog}\" already exists"))
        );
    }

    conn.execute("CREATE ROLE reader").unwrap();
    conn.execute("SET ROLE reader").unwrap();
    for (catalog, _, row_count) in CATALOGS {
        assert_eq!(
            rows(&conn, &format!("SELECT count(*) FROM {catalog}")),
            vec![vec![int(row_count as i64)]]
        );
        assert_eq!(
            error_of(&conn, &format!("DELETE FROM {catalog}")),
            (
                "42501",
                format!("cannot delete from pg_catalog table \"{catalog}\"")
            )
        );
        assert_eq!(
            error_of(&conn, &format!("INSERT INTO {catalog} DEFAULT VALUES")),
            (
                "42501",
                format!("cannot insert into pg_catalog table \"{catalog}\"")
            )
        );
    }
    conn.execute("RESET ROLE").unwrap();
    for (catalog, _, row_count) in CATALOGS {
        assert_eq!(
            rows(&conn, &format!("SELECT count(*) FROM {catalog}")),
            vec![vec![int(row_count as i64)]]
        );
    }
}

#[test]
fn writes_are_refused_wal() {
    writes_are_refused(false);
}

#[test]
fn writes_are_refused_mvcc() {
    writes_are_refused(true);
}

/// The catalogs are not relations of the user: pg_class, pg_tables and
/// pg_attribute do not list them, and the database file has no rows for
/// them.
fn catalogs_are_not_user_relations(mvcc: bool) {
    let db = open(mvcc);
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int)").unwrap();
    for (catalog, _, _) in CATALOGS {
        for sql in [
            format!("SELECT relname FROM pg_class WHERE relname = '{catalog}'"),
            format!("SELECT tablename FROM pg_tables WHERE tablename = '{catalog}'"),
            format!(
                "SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
                 WHERE c.relname = '{catalog}'"
            ),
        ] {
            assert!(rows(&conn, &sql).is_empty(), "{sql}");
        }
    }
    assert_eq!(
        rows(
            &conn,
            "SELECT relname FROM pg_class WHERE relnamespace = 2200 ORDER BY relname"
        ),
        vec![vec![text("items")]]
    );
    let core = db.connect_limbo();
    let mut stmt = core
        .prepare("SELECT name FROM sqlite_schema WHERE name LIKE 'pg%' ORDER BY name")
        .unwrap();
    let stored = stmt.run_collect_rows().unwrap();
    assert_eq!(stored, vec![vec![text("pg_description")]]);
}

#[test]
fn catalogs_are_not_user_relations_wal() {
    catalogs_are_not_user_relations(false);
}

#[test]
fn catalogs_are_not_user_relations_mvcc() {
    catalogs_are_not_user_relations(true);
}

/// A table that a user creates in public under the name of a catalog is a
/// table of the user, as before the catalogs had rows. A name without a
/// schema finds the catalog, as in PostgreSQL, where pg_catalog comes first
/// in the search path.
fn user_table_with_a_catalog_name(mvcc: bool) {
    let db = open(mvcc);
    let conn = db.connect_postgres();
    for (catalog, row_count) in [("pg_operator", 0), ("pg_opclass", 177)] {
        conn.execute(format!("CREATE TABLE {catalog} (id int, label text)"))
            .unwrap();
        assert!(rows(&conn, &format!("SELECT * FROM public.{catalog}")).is_empty());
        for name in [catalog.to_string(), format!("pg_catalog.{catalog}")] {
            assert_eq!(
                rows(&conn, &format!("SELECT count(*) FROM {name}")),
                vec![vec![int(row_count)]],
                "{name}"
            );
        }
        // Before the catalogs had rows, the frontend refused this too,
        // because it compares only the name of the table with the names of
        // catalogs.
        assert_eq!(
            error_of(
                &conn,
                &format!("INSERT INTO public.{catalog} VALUES (1, 'mine')")
            ),
            (
                "42501",
                format!("cannot insert into pg_catalog table \"{catalog}\"")
            )
        );
        conn.execute(format!("DROP TABLE public.{catalog}"))
            .unwrap();
        assert_eq!(
            error_of(&conn, &format!("SELECT * FROM public.{catalog}")).0,
            "42P01"
        );
        assert_eq!(
            rows(&conn, &format!("SELECT count(*) FROM {catalog}")),
            vec![vec![int(row_count)]]
        );
    }
}

#[test]
fn user_table_with_a_catalog_name_wal() {
    user_table_with_a_catalog_name(false);
}

#[test]
fn user_table_with_a_catalog_name_mvcc() {
    user_table_with_a_catalog_name(true);
}

/// The file of a database that TursoPG 0.9.0-pre.6 wrote before the
/// catalogs had rows, with its MVCC log. It has a table `items` with one
/// row, an index, the comment `the items` on `items`, and an empty table
/// `public.pg_operator`.
const OLD_DATABASE: &str = "catalogs_before_seeded_tables_v0.9.0-pre.6.db";

/// A database that an older version wrote opens, and reads the catalogs
/// with the built-in rows, its own tables, and its comments.
#[test]
fn database_written_before_the_catalogs_had_rows_opens() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("integration/postgres/fixtures");
    let dir = tempfile::tempdir().unwrap();
    for suffix in ["", "-log"] {
        std::fs::copy(
            fixtures.join(format!("{OLD_DATABASE}{suffix}")),
            dir.path().join(format!("{OLD_DATABASE}{suffix}")),
        )
        .unwrap();
    }
    let path = dir.path().join(OLD_DATABASE);
    let (_io, db) = turso_pg::open_database(
        path.to_str().unwrap(),
        None,
        turso_core::OpenFlags::default(),
        turso_pg::tursopg_database_opts(),
    )
    .unwrap();
    let conn = turso_pg::Connection::connect(&db).unwrap();
    for (catalog, table_oid, row_count) in CATALOGS {
        assert_eq!(
            rows(
                &conn,
                &format!("SELECT count(*), min(tableoid) FROM pg_catalog.{catalog}")
            ),
            vec![vec![int(row_count as i64), int(table_oid)]]
        );
    }
    assert_eq!(
        rows(&conn, "SELECT id, name FROM items"),
        vec![vec![int(1), text("one")]]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT obj_description(oid, 'pg_class') FROM pg_class WHERE relname = 'items'"
        ),
        vec![vec![text("the items")]]
    );
    assert!(rows(&conn, "SELECT * FROM public.pg_operator").is_empty());
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM pg_operator"),
        vec![vec![int(0)]]
    );
    conn.execute("INSERT INTO items VALUES (2, 'two')").unwrap();
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM items"),
        vec![vec![int(2)]]
    );
}

/// A database that the release before seed versions wrote opens without a
/// change: it has no stored seeded table, so the open and the reads of the
/// catalogs write nothing, and no table of seed versions appears.
#[test]
fn database_written_before_seed_versions_stays_unchanged() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("integration/postgres/fixtures");
    let dir = tempfile::tempdir().unwrap();
    let mut before = Vec::new();
    for suffix in ["", "-log"] {
        let to = dir.path().join(format!("{OLD_DATABASE}{suffix}"));
        std::fs::copy(fixtures.join(format!("{OLD_DATABASE}{suffix}")), &to).unwrap();
        before.push((to.clone(), std::fs::read(&to).unwrap()));
    }
    let path = dir.path().join(OLD_DATABASE);
    {
        let (_io, db) = turso_pg::open_database(
            path.to_str().unwrap(),
            None,
            turso_core::OpenFlags::default(),
            turso_pg::tursopg_database_opts(),
        )
        .unwrap();
        let conn = turso_pg::Connection::connect(&db).unwrap();
        for (catalog, _, row_count) in CATALOGS {
            assert_eq!(
                rows(&conn, &format!("SELECT count(*) FROM pg_catalog.{catalog}")),
                vec![vec![int(row_count as i64)]]
            );
        }
        // No seeded table and no table of seed versions is stored.
        let core = db
            .connect(turso_core::ConnectOpts::new(
                turso_core::SessionRole::Superuser,
            ))
            .unwrap();
        let tables: Vec<String> = core
            .pragma_query("table_list")
            .unwrap()
            .iter()
            .map(|row| row[1].to_string())
            .collect();
        // The table `items` of the namespace `public`.
        assert!(
            tables.iter().any(|table| table == "__turso_ns1_items"),
            "{tables:?}"
        );
        assert!(
            !tables
                .iter()
                .any(|table| table.starts_with("__turso_internal_seed")
                    || CATALOGS.iter().any(|(catalog, _, _)| table == catalog)),
            "{tables:?}"
        );
    }
    for (file, bytes) in before {
        assert!(
            std::fs::read(&file).unwrap() == bytes,
            "{} changed",
            file.display()
        );
    }
}

/// The catalogs whose rows refer to functions keep no rows and the columns
/// they had, and every role gets 42501 for a change, as before.
fn catalogs_that_refer_to_functions_are_empty(mvcc: bool) {
    let db = open(mvcc);
    let conn = db.connect_postgres();
    conn.execute("SELECT set_config('search_path', '', false)")
        .unwrap();
    for (catalog, columns) in EMPTY_CATALOGS {
        for name in [catalog.to_string(), format!("pg_catalog.{catalog}")] {
            let mut stmt = conn.prepare(format!("SELECT * FROM {name}")).unwrap();
            assert_eq!(stmt.num_columns(), columns, "{name}");
            assert!(stmt.run_collect_rows().unwrap().is_empty(), "{name}");
            assert!(rows(&conn, &format!("SELECT tableoid FROM {name}")).is_empty());
        }
        assert_eq!(
            error_of(&conn, &format!("DELETE FROM {catalog}")),
            (
                "42501",
                format!("cannot delete from pg_catalog table \"{catalog}\"")
            )
        );
    }
}

#[test]
fn catalogs_that_refer_to_functions_are_empty_wal() {
    catalogs_that_refer_to_functions_are_empty(false);
}

#[test]
fn catalogs_that_refer_to_functions_are_empty_mvcc() {
    catalogs_that_refer_to_functions_are_empty(true);
}
