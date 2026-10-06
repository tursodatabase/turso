//! Files that tursopg wrote before canonical table storage (the fixtures in
//! fixtures/pg_v1, made by tursopg at commit e6c79b43) must keep their
//! values and stay writable.

use crate::common::{core_rows, open_with_sqlite_dialect, rows, TempDatabase};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use turso_pg::PgConnection;

const MAIN: &str = "pg_v1_storage.db";
const SCHEMA_FILE: &str = "turso-postgres-schema-s.db";

fn copy_fixtures(files: &[&str]) -> TempDir {
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("integration/postgres/fixtures/pg_v1");
    let dir = TempDir::new().unwrap();
    for file in files {
        std::fs::copy(fixtures.join(file), dir.path().join(file)).unwrap();
    }
    dir
}

fn open(path: PathBuf, mvcc: bool) -> TempDatabase {
    TempDatabase::builder()
        .with_db_path(path)
        .with_mvcc(mvcc)
        .build()
}

/// An MVCC main database cannot attach a WAL database, so the MVCC runs
/// check the main file only.
fn connect_with_schema_file(db: &TempDatabase, mvcc: bool) -> PgConnection {
    let core = db.connect_limbo();
    if !mvcc {
        core.execute(format!(
            "ATTACH '{}' AS s",
            db.path.with_file_name(SCHEMA_FILE).display()
        ))
        .unwrap();
    }
    PgConnection::new(core)
}

const ALL_TYPES: &str =
    "SELECT id, b, s, i, bi, r, dp, n, v, t, hex(by), u, d, tm, ts, tz, j, jb, \
                         ip, cd, mac, ia, ta, m, p, mo, created FROM all_types ORDER BY id";

/// The values that the base tursopg shows for the fixture.
fn base_values() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        (
            ALL_TYPES,
            vec![
                "1|1|-7|42|9000000000|1.5|2.25|12.34|short|some text|5C78303130326666|\
                 01945ca0-3189-76c0-9a8f-caf310fc8b8e|2024-02-29|10:11:12.5|\
                 2024-01-02 03:04:05.678|2024-01-02 01:04:05|{\"a\":1}|{\"b\":[1,2]}|\
                 192.168.1.1|10.0.0.0/8|08:00:2b:01:02:03|{1,2,3}|{x,y}|happy|5|7.5|\
                 2024-05-06 07:08:09",
                "2|0|0|-1|-9000000000|-0.5|0.0|-0.01||||\
                 2bedfa4b-8035-4dff-9925-ed3b9cc24e19|1999-12-31|00:00:00|\
                 2024-01-01 00:00:00|1970-01-01 00:00:00|NULL|NULL|NULL|NULL|NULL|NULL|\
                 NULL|sad|1|NULL|2024-05-06 07:08:10",
                "3|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL||\
                 53963d18-2239-4e3a-a6bd-c6aadeaa8b19|NULL|NULL|NULL|NULL|NULL|NULL|NULL|\
                 NULL|NULL|NULL|NULL|NULL|NULL|NULL|2024-05-06 07:08:11",
            ],
        ),
        (
            "SELECT id, label, qty FROM big ORDER BY id",
            vec!["-5|second|0", "9000000001|first|1"],
        ),
        (
            "SELECT a, b, c FROM comp ORDER BY a, b",
            vec!["1|x|1.5", "1|y|2.5"],
        ),
        (
            "SELECT id, big_id, note FROM child",
            vec!["1|9000000001|none"],
        ),
        (
            "SELECT id, a, extra, when_ts FROM added ORDER BY id",
            vec!["1|a|7|NULL", "2|b|8|2024-03-04 05:06:07"],
        ),
        ("SELECT id, keep, n FROM dropped", vec!["1|k|3.75"]),
        (
            "SELECT id, m, p, ts FROM s.st",
            vec!["1|ok|3|2024-01-01 12:00:00"],
        ),
    ]
}

#[test]
fn base_file_keeps_its_values() {
    for mvcc in [false, true] {
        let dir = copy_fixtures(&[MAIN, SCHEMA_FILE]);
        let db = open(dir.path().join(MAIN), mvcc);
        let conn = connect_with_schema_file(&db, mvcc);
        for (sql, expected) in base_values() {
            if mvcc && sql.contains("s.st") {
                continue;
            }
            assert_eq!(rows(&conn, sql), expected, "mvcc={mvcc}: {sql}");
        }
        for (indexed, scan) in [
            (
                "SELECT id FROM all_types WHERE ts > '2024-01-01' ORDER BY id",
                "SELECT id FROM all_types NOT INDEXED WHERE ts > '2024-01-01' ORDER BY id",
            ),
            (
                "SELECT id FROM all_types WHERE b ORDER BY id",
                "SELECT id FROM all_types NOT INDEXED WHERE b ORDER BY id",
            ),
            (
                "SELECT id FROM all_types WHERE lower(t) = 'some text'",
                "SELECT id FROM all_types NOT INDEXED WHERE lower(t) = 'some text'",
            ),
        ] {
            assert_eq!(
                core_rows(conn.inner(), indexed),
                core_rows(conn.inner(), scan),
                "mvcc={mvcc}: {indexed}"
            );
        }
        for bad in [
            "INSERT INTO all_types (m) VALUES ('angry')",
            "INSERT INTO all_types (p) VALUES (-1)",
            "INSERT INTO all_types (s) VALUES (100000)",
            "INSERT INTO big VALUES (1, 'third', -1)",
        ] {
            assert!(conn.execute(bad).is_err(), "mvcc={mvcc}: {bad}");
        }
        assert_eq!(
            core_rows(conn.inner(), "PRAGMA integrity_check"),
            ["ok"],
            "mvcc={mvcc}"
        );
    }
}

#[test]
fn base_file_accepts_writes_and_alter_table() {
    for mvcc in [false, true] {
        let dir = copy_fixtures(&[MAIN, SCHEMA_FILE]);
        let db = open(dir.path().join(MAIN), mvcc);
        let conn = connect_with_schema_file(&db, mvcc);
        conn.execute(
            "INSERT INTO all_types (b, n, ts, m, p, ia, created) \
             VALUES (true, 1.5, '2024-06-01 00:00:00', 'ok', 2, ARRAY[4], '2024-06-01 00:00:00')",
        )
        .unwrap();
        conn.execute("ALTER TABLE all_types ADD COLUMN extra text DEFAULT 'e'")
            .unwrap();
        conn.execute("ALTER TABLE big RENAME TO big2").unwrap();
        conn.execute("ALTER TABLE comp RENAME COLUMN c TO c2")
            .unwrap();
        conn.execute("ALTER TABLE added DROP COLUMN when_ts")
            .unwrap();
        conn.execute("INSERT INTO big2 VALUES (7, 'third', 3)")
            .unwrap();
        if !mvcc {
            conn.execute("INSERT INTO s.st VALUES (2, 'happy', 4, '2024-02-02 00:00:00')")
                .unwrap();
        }
        drop(conn);
        let path = db.path.clone();
        drop(db);

        let db = open(path, mvcc);
        let core = db.connect_limbo();
        assert_eq!(
            core_rows(
                &core,
                "SELECT sql FROM sqlite_schema WHERE name IN ('all_types', 'big2', 'comp', 'child') \
                 ORDER BY name"
            ),
            [
                "CREATE TABLE all_types (id INTEGER PRIMARY KEY DEFAULT (nextval ('all_types_id_seq')), \
                 b boolean, s smallint, i INTEGER, bi bigint, r REAL, dp REAL, n numeric(10, 2), \
                 v varchar(10), t TEXT, \"by\" bytea, u uuid, d date, tm time, ts timestamp, \
                 tz timestamptz, j json, jb jsonb, ip inet, cd cidr, mac macaddr, ia INTEGER[], \
                 ta TEXT[], m mood, p posint, mo money2, created timestamp DEFAULT (now ()), \
                 extra TEXT DEFAULT 'e') STRICT, PGSTORAGE",
                "CREATE TABLE big2 (id bigint PRIMARY KEY, label TEXT UNIQUE, qty INTEGER \
                 CHECK (qty >= 0)) STRICT, PGSTORAGE",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, big_id bigint REFERENCES big2 (id), \
                 note TEXT DEFAULT 'none') STRICT, PGSTORAGE",
                "CREATE TABLE comp (a INTEGER, b TEXT, c2 numeric (5, 1), PRIMARY KEY (a, b), \
                 UNIQUE (c2)) STRICT, PGSTORAGE",
            ],
            "mvcc={mvcc}"
        );
        let conn = connect_with_schema_file(&db, mvcc);
        assert_eq!(
            rows(
                &conn,
                "SELECT id, b, n, ts, m, p, ia, created, extra FROM all_types ORDER BY id"
            ),
            [
                "1|1|12.34|2024-01-02 03:04:05.678|happy|5|{1,2,3}|2024-05-06 07:08:09|e",
                "2|0|-0.01|2024-01-01 00:00:00|sad|1|NULL|2024-05-06 07:08:10|e",
                "3|NULL|NULL|NULL|NULL|NULL|NULL|2024-05-06 07:08:11|e",
                "4|1|1.50|2024-06-01 00:00:00|ok|2|{4}|2024-06-01 00:00:00|e",
            ],
            "mvcc={mvcc}"
        );
        assert_eq!(
            rows(&conn, "SELECT id, label, qty FROM big2 ORDER BY id"),
            ["-5|second|0", "7|third|3", "9000000001|first|1"],
            "mvcc={mvcc}"
        );
        assert_eq!(
            rows(&conn, "SELECT a, b, c2 FROM comp ORDER BY a, b"),
            ["1|x|1.5", "1|y|2.5"],
            "mvcc={mvcc}"
        );
        assert_eq!(
            rows(&conn, "SELECT * FROM added ORDER BY id"),
            ["1|a|7", "2|b|8"],
            "mvcc={mvcc}"
        );
        if !mvcc {
            assert_eq!(
                rows(&conn, "SELECT id, m, p, ts FROM s.st ORDER BY id"),
                [
                    "1|ok|3|2024-01-01 12:00:00",
                    "2|happy|4|2024-02-02 00:00:00"
                ],
            );
        }
        assert_eq!(
            core_rows(conn.inner(), "PRAGMA integrity_check"),
            ["ok"],
            "mvcc={mvcc}"
        );
    }
}

#[test]
fn sqlite_dialect_refuses_base_file() {
    let dir = copy_fixtures(&[MAIN]);
    let Err(err) = open_with_sqlite_dialect(&dir.path().join(MAIN)) else {
        panic!("the SQLite dialect must refuse the PostgreSQL DDL of the base file");
    };
    assert!(
        err.to_string()
            .contains("created by the PostgreSQL frontend"),
        "{err}"
    );
}

/// VACUUM stores each table of the base file as canonical SQL with its V1
/// types. After that, the SQLite dialect can read the file too.
#[test]
fn vacuum_stores_base_tables_as_canonical_sql() {
    let dir = copy_fixtures(&[MAIN, SCHEMA_FILE]);
    let db = open(dir.path().join(MAIN), false);
    let conn = connect_with_schema_file(&db, false);
    conn.inner().execute("VACUUM").unwrap();
    for (sql, expected) in base_values() {
        assert_eq!(rows(&conn, sql), expected, "{sql}");
    }
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
    assert_eq!(
        core_rows(
            conn.inner(),
            "SELECT sql FROM sqlite_schema WHERE name = 'big'"
        ),
        [
            "CREATE TABLE big (id bigint PRIMARY KEY, label TEXT UNIQUE, qty INTEGER \
          CHECK (qty >= 0)) STRICT, PGSTORAGE"
        ]
    );
    drop(conn);
    let path = db.path.clone();
    drop(db);

    let sqlite = open_with_sqlite_dialect(&path).unwrap().connect().unwrap();
    let (sql, expected) = &base_values()[1];
    assert_eq!(core_rows(&sqlite, sql), *expected);
}

/// RENAME of the base tursopg stored the marker before canonical STRICT SQL;
/// the base tursopg cannot open such a file.
#[test]
fn renamed_table_of_base_file_loads() {
    let dir = copy_fixtures(&["pg_v1_renamed.db"]);
    let path = dir.path().join("pg_v1_renamed.db");
    let db = open(path.clone(), false);
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO r2 (id, ts, n) VALUES (2, '2024-01-02 00:00:00', 3)")
        .unwrap();
    let expected = [
        "1|2024-01-01 10:00:00|2.50|first",
        "2|2024-01-02 00:00:00|3.00|x",
    ];
    assert_eq!(
        rows(&conn, "SELECT id, ts, n, note FROM r2 ORDER BY id"),
        expected
    );
    drop(conn);
    drop(db);

    let sqlite = open_with_sqlite_dialect(&path).unwrap().connect().unwrap();
    assert_eq!(
        core_rows(&sqlite, "SELECT id, ts, n, note FROM r2 ORDER BY id"),
        expected
    );
}

/// A user type named like a built-in type of the next steps keeps working.
#[test]
fn user_type_with_pg_prefix_of_base_file_still_works() {
    let dir = copy_fixtures(&["pg_v1_pg_prefix_type.db"]);
    let db = open(dir.path().join("pg_v1_pg_prefix_type.db"), false);
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO pd VALUES (2, 'b')").unwrap();
    assert!(conn.execute("INSERT INTO pd VALUES (3, 'c')").is_err());
    assert_eq!(
        rows(&conn, "SELECT id, x FROM pd ORDER BY id"),
        ["1|a", "2|b"]
    );
}
