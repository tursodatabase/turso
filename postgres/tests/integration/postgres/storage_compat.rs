//! Files that tursopg wrote before canonical table storage (the fixtures in
//! fixtures/pg_v1, made by tursopg at commit e6c79b43) must keep their
//! values and stay writable. The fixtures named sqlite_v1_* were made by
//! tursodb at the same commit with the SQLite dialect.

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
        conn.execute("ALTER TABLE dropped RENAME TO dropped2")
            .unwrap();
        conn.execute("ALTER TABLE child DROP COLUMN note").unwrap();
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
        // The base stored `added` and `dropped` without the marker after its
        // ADD and DROP COLUMN, so nothing shows that they are PostgreSQL tables.
        assert_eq!(
            core_rows(
                &core,
                "SELECT sql FROM sqlite_schema \
                 WHERE name IN ('added', 'all_types', 'big2', 'child', 'comp', 'dropped2') \
                 ORDER BY name"
            ),
            [
                "CREATE TABLE added (id INTEGER PRIMARY KEY, a TEXT, extra INTEGER DEFAULT 7) STRICT",
                "CREATE TABLE all_types (id INTEGER PRIMARY KEY DEFAULT (nextval ('all_types_id_seq')), \
                 b boolean, s smallint, i INTEGER, bi bigint, r REAL, dp REAL, n numeric(10, 2), \
                 v varchar(10), t TEXT, \"by\" bytea, u uuid, d date, tm time, ts timestamp, \
                 tz timestamptz, j json, jb jsonb, ip inet, cd cidr, mac macaddr, ia INTEGER[], \
                 ta TEXT[], m mood, p posint, mo money2, created timestamp DEFAULT (now ()), \
                 extra TEXT DEFAULT 'e') STRICT, PGSTORAGE",
                "CREATE TABLE big2 (id bigint PRIMARY KEY, label TEXT UNIQUE, qty INTEGER \
                 CHECK (qty >= 0)) STRICT, PGSTORAGE",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, big_id bigint, \
                 FOREIGN KEY (big_id) REFERENCES big2(id)) STRICT, PGSTORAGE",
                "CREATE TABLE comp (a INTEGER, b TEXT, c2 numeric (5, 1), PRIMARY KEY (a, b), \
                 UNIQUE (c2)) STRICT, PGSTORAGE",
                "CREATE TABLE dropped2 (id INTEGER PRIMARY KEY, keep TEXT, n numeric (10, 2)) STRICT",
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
        assert_eq!(
            rows(&conn, "SELECT * FROM child"),
            ["1|9000000001"],
            "mvcc={mvcc}"
        );
        assert_eq!(
            rows(&conn, "SELECT * FROM dropped2"),
            ["1|k|3.75"],
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
/// the base tursopg cannot open such a file. The marker shows that the table
/// is a PostgreSQL table, so the next ALTER stores it with PGSTORAGE.
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
    drop(sqlite);

    let db = open(path.clone(), false);
    let conn = db.connect_postgres();
    conn.execute("ALTER TABLE r2 ADD COLUMN z text").unwrap();
    drop(conn);
    let db = db.reopen();
    let core = db.connect_limbo();
    assert_eq!(
        core_rows(&core, "SELECT sql FROM sqlite_schema WHERE name = 'r2'"),
        [
            "CREATE TABLE r2 (id bigint PRIMARY KEY, ts timestamp, n numeric(10, 2), \
          note TEXT DEFAULT 'x', z TEXT) STRICT, PGSTORAGE"
        ]
    );
    assert_eq!(
        core_rows(&core, "SELECT id, ts, n, note, z FROM r2 ORDER BY id"),
        [
            "1|2024-01-01 10:00:00|2.50|first|NULL",
            "2|2024-01-02 00:00:00|3.00|x|NULL"
        ]
    );
}

/// RENAME of the base tursopg also printed a DEFAULT function call without
/// parentheses, which the SQLite parser refuses. The base cannot open the
/// file at all.
#[test]
fn renamed_table_with_function_defaults_of_base_file_loads() {
    let dir = copy_fixtures(&["pg_v1_renamed_serial.db"]);
    let path = dir.path().join("pg_v1_renamed_serial.db");
    let db = open(path.clone(), false);
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO rs2 (a) VALUES ('second')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, ts, flag, a FROM rs2 WHERE id = 1"),
        ["1|2024-01-01 10:00:00|0|first"]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT id, ts IS NOT NULL, flag, a FROM rs2 WHERE id > 1"
        ),
        ["2|1|0|second"]
    );
    drop(conn);
    drop(db);

    let Err(err) = open_with_sqlite_dialect(&path) else {
        panic!("the SQLite dialect must refuse the row");
    };
    assert!(
        err.to_string()
            .contains("created by the PostgreSQL frontend"),
        "{err}"
    );

    let db = open(path.clone(), false);
    let conn = db.connect_postgres();
    conn.execute("ALTER TABLE rs2 ADD COLUMN z text").unwrap();
    drop(conn);
    drop(db);
    let sqlite = open_with_sqlite_dialect(&path).unwrap().connect().unwrap();
    assert_eq!(
        core_rows(&sqlite, "SELECT sql FROM sqlite_schema WHERE name = 'rs2'"),
        [
            "CREATE TABLE rs2 (id INTEGER PRIMARY KEY DEFAULT (nextval ('rs_id_seq')), \
          ts timestamp DEFAULT (now ()), flag boolean DEFAULT 0, a TEXT, z TEXT) STRICT, PGSTORAGE"
        ]
    );
    assert_eq!(
        core_rows(&sqlite, "SELECT id, a, z FROM rs2 ORDER BY id"),
        ["1|first|NULL", "2|second|NULL"]
    );
}

/// More types of the base tursopg, and CHECK constraints with casts to date
/// and timestamp. The base translated these casts to a cast to TEXT, so the
/// rows of the base file keep the meaning of their CHECK.
#[test]
fn more_types_of_base_file_keep_their_values() {
    let dir = copy_fixtures(&["pg_v1_more_types.db"]);
    let db = open(dir.path().join("pg_v1_more_types.db"), false);
    let conn = db.connect_postgres();
    assert_eq!(
        rows(&conn, "SELECT * FROM more_types ORDER BY id"),
        [
            "1|10:00:00|12345|12345.67890|3.25|ab|08:00:2b:01:02:03:04:05",
            "2|NULL|-7|NULL|NULL|NULL|NULL"
        ]
    );
    assert_eq!(
        rows(&conn, "SELECT * FROM more_arrays ORDER BY id"),
        [
            "1|{9000000000,-1}|{1,0}|{01945ca0-3189-76c0-9a8f-caf310fc8b8e}",
            "2|{}|{}|{}"
        ]
    );
    assert_eq!(
        rows(&conn, "SELECT * FROM ck ORDER BY id"),
        [
            "1|2024-01-01 10:00:00|NULL",
            "2|garbage|NULL",
            "3|NULL|2024-01-02"
        ]
    );
    conn.execute("INSERT INTO more_types (tt, n5, n30) VALUES ('11:00:00', 5, 1.5)")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, tt, n5, n30 FROM more_types WHERE id = 3"),
        ["3|11:00:00|5|1.50000"]
    );
    for accepted in [
        "INSERT INTO ck VALUES (4, '2024-01-01 12:00:00', NULL)",
        "INSERT INTO ck VALUES (5, 'garbage', NULL)",
    ] {
        conn.execute(accepted)
            .unwrap_or_else(|e| panic!("{accepted}: {e}"));
    }
    for refused in [
        "INSERT INTO ck VALUES (6, NULL, '2024-01-01')",
        "INSERT INTO ck VALUES (7, '2024-01-01', NULL)",
        "INSERT INTO more_types (n5) VALUES (123456)",
    ] {
        assert!(conn.execute(refused).is_err(), "{refused}");
    }
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
}

/// A user type named like a built-in type of the next steps keeps working,
/// also after an ALTER stores the table as canonical SQL.
#[test]
fn user_type_with_pg_prefix_of_base_file_still_works() {
    let dir = copy_fixtures(&["pg_v1_pg_prefix_type.db"]);
    let db = open(dir.path().join("pg_v1_pg_prefix_type.db"), false);
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO pd VALUES (2, 'b')").unwrap();
    assert!(conn.execute("INSERT INTO pd VALUES (3, 'c')").is_err());
    conn.execute("ALTER TABLE pd ADD COLUMN note text").unwrap();
    drop(conn);
    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(
        core_rows(
            conn.inner(),
            "SELECT sql FROM sqlite_schema WHERE name = 'pd'"
        ),
        ["CREATE TABLE pd (id INTEGER PRIMARY KEY, x pg_date, note TEXT) STRICT, PGSTORAGE"]
    );
    assert!(conn.execute("INSERT INTO pd VALUES (3, 'c', 'n')").is_err());
    assert_eq!(
        rows(&conn, "SELECT id, x, note FROM pd ORDER BY id"),
        ["1|a|NULL", "2|b|NULL"]
    );
}

/// A user type of the base file has the name of the built-in type pg_int8
/// and is the type of a PRIMARY KEY. Only the built-in type makes a rowid
/// alias, so the tables keep their rows and their indexes. DDL that would
/// store such a table as canonical SQL, where the column would be a rowid
/// alias, is refused.
#[test]
fn user_type_pg_int8_primary_key_of_base_file_is_not_a_rowid_alias() {
    let dir = copy_fixtures(&["pg_v1_pg_int8_pk.db"]);
    let db = open(dir.path().join("pg_v1_pg_int8_pk.db"), false);
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO k VALUES ('c', 'third')").unwrap();
    conn.execute("INSERT INTO r2 VALUES ('c', 'third')")
        .unwrap();
    assert!(conn.execute("INSERT INTO k VALUES ('d', 'x')").is_err());
    assert!(conn.execute("INSERT INTO k VALUES ('a', 'x')").is_err());
    for refused in [
        "ALTER TABLE k ADD COLUMN w text",
        "ALTER TABLE r2 ADD COLUMN w text",
        "ALTER TABLE k RENAME TO k5",
        "VACUUM",
    ] {
        let err = conn.inner().execute(refused).unwrap_err();
        assert!(
            err.to_string().contains(
                "PRIMARY KEY column id has the user type pg_int8, which has the name of a built-in type"
            ),
            "{refused}: {err}"
        );
    }
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
    drop(conn);
    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(
        rows(&conn, "SELECT id, v FROM k ORDER BY id"),
        ["a|first", "c|third"]
    );
    assert_eq!(
        rows(&conn, "SELECT id, v FROM r2 ORDER BY id"),
        ["b|second", "c|third"]
    );
}

/// The base file has an index on numeric arithmetic and a partial index on
/// a numeric comparison, made after the rows. An INSERT, an UPDATE and a
/// DELETE compute the keys of these indexes as CREATE INDEX did.
#[test]
fn numeric_expression_indexes_of_base_file_keep_their_keys() {
    let dir = copy_fixtures(&["pg_v1_numeric_expr_index.db"]);
    let db = open(dir.path().join("pg_v1_numeric_expr_index.db"), false);
    let conn = db.connect_postgres();
    conn.execute("DELETE FROM x WHERE id = 1").unwrap();
    conn.execute("UPDATE x SET val = 21 WHERE id = 2").unwrap();
    conn.execute("INSERT INTO x VALUES (4, 30), (5, 1)")
        .unwrap();
    conn.execute("UPDATE x SET val = 16 WHERE id = 3").unwrap();
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
    assert_eq!(
        rows(&conn, "SELECT id, val * 2 FROM x ORDER BY id"),
        ["2|42.0000", "3|32.0000", "4|60.0000", "5|2.00"]
    );
    assert_eq!(
        rows(&conn, "SELECT id FROM x WHERE val > 15 ORDER BY id"),
        ["2", "3", "4"]
    );
}

/// The base file has indexes whose keys and WHERE clauses compare a numeric
/// with arithmetic, a list and a decimal literal. The base binary computed
/// them with the comparison rules of its version, which are not the rules of
/// queries today. A DELETE, an UPDATE, an INSERT and an UPSERT compute the
/// keys as the base binary did.
#[test]
fn numeric_comparison_indexes_of_base_file_keep_their_keys() {
    let dir = copy_fixtures(&["pg_v1_numeric_comparison_index.db"]);
    let db = open(dir.path().join("pg_v1_numeric_comparison_index.db"), false);
    let conn = db.connect_postgres();
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
    for sql in [
        "DELETE FROM p WHERE id = 1",
        "UPDATE p SET price = 70 WHERE id = 2",
        "INSERT INTO p VALUES (4, 'a', 70, 10, 60)",
        "INSERT INTO p VALUES (3, 'c', 1, 10, 20) ON CONFLICT (id) DO UPDATE SET price = excluded.price",
        "DELETE FROM p WHERE id = 2",
        "INSERT INTO p VALUES (5, 'b', 80, 10, 60)",
    ] {
        conn.execute(sql).unwrap();
    }
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
    assert_eq!(
        rows(&conn, "SELECT id, name, price FROM p ORDER BY id"),
        ["3|c|1.00", "4|a|70.00", "5|b|80.00"]
    );
}

/// [numeric_comparison_indexes_of_base_file_keep_their_keys] for a file of
/// the SQLite dialect, whose indexes also compare with a negative literal.
#[test]
fn numeric_comparison_indexes_of_sqlite_dialect_base_file_keep_their_keys() {
    let file = "sqlite_v1_numeric_comparison_index.db";
    let dir = copy_fixtures(&[file]);
    let db = open_with_sqlite_dialect(&dir.path().join(file)).unwrap();
    let conn = db.connect().unwrap();
    assert_eq!(core_rows(&conn, "PRAGMA integrity_check"), ["ok"]);
    for sql in [
        "DELETE FROM v WHERE id = 1",
        "UPDATE v SET val = -3 WHERE id = 2",
        "INSERT INTO v VALUES (4, 'a', 7, 1)",
        "INSERT OR REPLACE INTO v VALUES (3, 'c', -2, 1)",
        "INSERT INTO v VALUES (2, 'b', 9, 1) ON CONFLICT (id) DO UPDATE SET val = excluded.val",
        "DELETE FROM v WHERE id = 4",
        "INSERT INTO v VALUES (5, 'a', 8, 1)",
    ] {
        conn.execute(sql).unwrap();
    }
    assert_eq!(core_rows(&conn, "PRAGMA integrity_check"), ["ok"]);
    assert_eq!(
        core_rows(&conn, "SELECT id, name, val FROM v ORDER BY id"),
        ["2|b|9.00", "3|c|-2.00", "5|a|8.00"]
    );
}

/// The base file has a user type named pg_date. A database whose tables use
/// the built-in pg_date refuses to attach it: one name cannot have two types.
#[test]
fn attach_of_base_file_with_user_type_named_like_a_built_in_type_is_refused() {
    let dir = copy_fixtures(&["pg_v1_pg_prefix_type.db"]);
    let db = open(dir.path().join("main.db"), false);
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE m (id int PRIMARY KEY, d date)")
        .unwrap();
    let attach = format!(
        "ATTACH '{}' AS old",
        dir.path().join("pg_v1_pg_prefix_type.db").display()
    );
    let err = conn
        .inner()
        .execute(attach)
        .and_then(|_| conn.inner().execute("SELECT x FROM old.pd"))
        .unwrap_err();
    assert!(
        err.to_string().contains(
            "column pd.x uses type \"pg_date\", which the attached database and the main database define differently"
        ),
        "{err}"
    );
}

/// A FOREIGN KEY compares stored values. A new table stores a numeric of
/// precision at most 18 as an integer, so a reference from it to a numeric
/// column of the base file is refused. A bigint stores the same integer in
/// both versions.
#[test]
fn foreign_keys_to_base_file_columns_need_the_same_stored_form() {
    let dir = copy_fixtures(&[MAIN]);
    let db = open(dir.path().join(MAIN), false);
    let conn = db.connect_postgres();
    let err = conn
        .execute("CREATE TABLE refs_comp (id int PRIMARY KEY, c numeric(5,1) REFERENCES comp (c))")
        .unwrap_err();
    assert!(
        err.to_string().contains(
            "refs_comp.c stores decimals as integers with scale 1, but comp.c stores the values of type numeric"
        ),
        "{err}"
    );
    conn.execute("CREATE TABLE refs_big (id int PRIMARY KEY, b bigint REFERENCES big (id))")
        .unwrap();
    conn.execute("INSERT INTO refs_big SELECT id, id FROM big")
        .unwrap();
    assert_eq!(
        rows(
            &conn,
            "SELECT count(*) FROM refs_big JOIN big ON big.id = refs_big.b"
        ),
        rows(&conn, "SELECT count(*) FROM big")
    );
}

/// New tables store timestamps, dates, times, numerics and bigints as
/// integers. Their values equal the values of the base file, also in joins
/// with and without an index.
#[test]
fn new_tables_join_tables_of_base_file() {
    let dir = copy_fixtures(&[MAIN]);
    let db = open(dir.path().join(MAIN), false);
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE fresh (id int PRIMARY KEY, bi bigint, n numeric(10,2), d date, \
         tm time, ts timestamp, tz timestamptz)",
    )
    .unwrap();
    conn.execute("INSERT INTO fresh SELECT id, bi, n, d, tm, ts, tz FROM all_types")
        .unwrap();
    assert_eq!(
        core_rows(
            conn.inner(),
            "SELECT sql FROM sqlite_schema WHERE name = 'fresh'"
        ),
        [
            "CREATE TABLE fresh (id pg_int4 PRIMARY KEY, bi pg_int8, n pg_numeric (10, 2), \
          d pg_date, tm pg_time, ts pg_timestamp, tz pg_timestamptz) STRICT, PGSTORAGE"
        ]
    );
    let columns = "id, bi, n, d, tm, ts, tz";
    assert_eq!(
        rows(&conn, &format!("SELECT {columns} FROM fresh ORDER BY id")),
        rows(
            &conn,
            &format!("SELECT {columns} FROM all_types ORDER BY id")
        )
    );
    for column in ["bi", "n", "d", "tm", "ts", "tz"] {
        let join =
            format!("SELECT count(*) FROM all_types a JOIN fresh f ON f.{column} = a.{column}");
        let reverse =
            format!("SELECT count(*) FROM fresh f JOIN all_types a ON a.{column} = f.{column}");
        assert_eq!(rows(&conn, &join), ["2"], "{column}");
        conn.execute(format!("CREATE INDEX fresh_{column} ON fresh ({column})"))
            .unwrap();
        assert_eq!(rows(&conn, &join), ["2"], "{column} with an index");
        assert_eq!(rows(&conn, &reverse), ["2"], "{column} with an index");
    }
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
}

/// A user type of the base file has the name of the built-in type pg_date.
/// The tables of the file keep the user type, and a cast to date still gives
/// a date. A new table cannot use the built-in type until the user type is
/// dropped.
#[test]
fn user_type_with_built_in_name_of_base_file_hides_the_built_in_type() {
    let dir = copy_fixtures(&["pg_v1_pg_prefix_type.db"]);
    let db = open(dir.path().join("pg_v1_pg_prefix_type.db"), false);
    let conn = db.connect_postgres();
    assert_eq!(
        rows(&conn, "SELECT '2024-01-01 10:00'::date"),
        ["2024-01-01"]
    );
    let err = conn.execute("CREATE TABLE n (d date)").unwrap_err();
    assert!(
        err.to_string()
            .contains("column n.d needs the built-in type pg_date, but a user type of this database has that name"),
        "{err}"
    );
    conn.execute("CREATE TABLE n (ts timestamp)").unwrap();
    conn.execute("ALTER TABLE pd ADD COLUMN note text").unwrap();
    assert_eq!(
        rows(
            &conn,
            "SELECT atttypid FROM pg_attribute WHERE attname = 'x'"
        ),
        ["25"]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT ddl FROM pg_get_tabledef WHERE table_name IN ('pd', 'none')"
        ),
        ["CREATE TABLE pd (id integer PRIMARY KEY, x pg_date, note text)"]
    );
    conn.inner().execute("VACUUM").unwrap();
    assert_eq!(rows(&conn, "SELECT id, x FROM pd"), ["1|a"]);
    conn.execute("DROP TABLE pd").unwrap();
    conn.execute("DROP TYPE pg_date").unwrap();
    conn.execute("CREATE TABLE n2 (d date)").unwrap();
    conn.execute("INSERT INTO n2 VALUES ('2024-01-01 10:00')")
        .unwrap();
    assert_eq!(rows(&conn, "SELECT d FROM n2"), ["2024-01-01"]);
    drop(conn);
    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(rows(&conn, "SELECT d FROM n2"), ["2024-01-01"]);
    assert_eq!(
        core_rows(
            conn.inner(),
            "SELECT sql FROM sqlite_schema WHERE name = 'n2'"
        ),
        ["CREATE TABLE n2 (d pg_date) STRICT, PGSTORAGE"]
    );
}

/// A cast to timestamp gives the text of the new type, with up to six
/// fraction digits. A timestamp column of the base file keeps three
/// fraction digits, so a cast with more digits does not find its value.
#[test]
fn casts_compare_with_the_text_of_base_file_columns() {
    let dir = copy_fixtures(&[MAIN]);
    let db = open(dir.path().join(MAIN), false);
    let conn = db.connect_postgres();
    for (sql, expected) in [
        (
            "SELECT id FROM all_types WHERE ts = '2024-01-02 03:04:05.678'::timestamp",
            vec!["1"],
        ),
        (
            "SELECT id FROM all_types WHERE ts = '2024-01-02 03:04:05.678123'::timestamp",
            vec![],
        ),
        (
            "SELECT id FROM all_types WHERE d = '2024-02-29 10:00'::date",
            vec!["1"],
        ),
        (
            "SELECT id FROM all_types WHERE tz = '2024-01-02 03:04:05+02'::timestamptz",
            vec!["1"],
        ),
    ] {
        assert_eq!(rows(&conn, sql), expected, "{sql}");
    }
}

#[test]
fn covering_numeric_expression_indexes_of_base_file_give_the_rows_of_a_scan() {
    let dir = copy_fixtures(&["pg_v1_numeric_comparison_index.db"]);
    let db = open(dir.path().join("pg_v1_numeric_comparison_index.db"), false);
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO p VALUES (4, 'd', 1, 10, 20)")
        .unwrap();
    for (sql, expected) in [
        ("SELECT max(price - cost > 0) FROM p", vec!["1"]),
        (
            "SELECT id, price - cost > 0 FROM p ORDER BY price - cost > 0, id",
            vec!["1|0", "4|0", "2|1", "3|1"],
        ),
        (
            "SELECT price = 1.005 FROM p ORDER BY price = 1.005",
            vec!["0", "0", "0", "0"],
        ),
        (
            "SELECT DISTINCT price * 2 > 15 FROM p ORDER BY 1",
            vec!["0", "1"],
        ),
        (
            "SELECT price - cost > 0, count(*) FROM p GROUP BY price - cost > 0",
            vec!["0|2", "1|2"],
        ),
    ] {
        assert_eq!(rows(&conn, sql), expected, "{sql}");
    }
}

#[test]
fn covering_numeric_expression_indexes_of_sqlite_dialect_base_file_give_the_rows_of_a_scan() {
    let file = "sqlite_v1_numeric_comparison_index.db";
    let dir = copy_fixtures(&[file]);
    let db = open_with_sqlite_dialect(&dir.path().join(file)).unwrap();
    let conn = db.connect().unwrap();
    for (sql, expected) in [
        (
            "SELECT val > -1 FROM v ORDER BY val > -1",
            vec!["0", "1", "1"],
        ),
        (
            "SELECT val + other > 4, count(*) FROM v GROUP BY val + other > 4",
            vec!["0|1", "1|2"],
        ),
        ("SELECT min(val > -1), max(val > -1) FROM v", vec!["0|1"]),
    ] {
        assert_eq!(core_rows(&conn, sql), expected, "{sql}");
        let not_indexed = sql.replacen("FROM v", "FROM v NOT INDEXED", 1);
        assert_eq!(core_rows(&conn, &not_indexed), expected, "{not_indexed}");
    }
}

/// The base stored `added` and `dropped` without the marker and without
/// PGSTORAGE after its ALTER TABLE, so a new column of these tables gets the
/// types of the base version.
#[test]
fn add_column_to_base_tables_without_pgstorage_uses_the_types_of_the_base_version() {
    for mvcc in [false, true] {
        let dir = copy_fixtures(&[MAIN]);
        let db = open(dir.path().join(MAIN), mvcc);
        let conn = db.connect_postgres();
        for table in ["added", "dropped"] {
            for (column, pg_type) in [
                ("c_int", "integer"),
                ("c_big", "bigint"),
                ("c_serial", "serial"),
                ("c_ts", "timestamp"),
                ("c_tz", "timestamptz"),
                ("c_date", "date"),
                ("c_time", "time"),
                ("c_num", "numeric(10,2)"),
            ] {
                conn.execute(format!("ALTER TABLE {table} ADD COLUMN {column} {pg_type}"))
                    .unwrap_or_else(|e| panic!("{table}.{column} {pg_type}: {e}"));
            }
        }
        conn.execute(
            "INSERT INTO dropped (id, keep, n, c_int, c_big, c_ts, c_tz, c_date, c_time, c_num) \
             VALUES (2, 'x', 1, 5, 9000000000, '2024-01-01 10:00:00', '2024-01-01 10:00:00+02:00', \
             '2024-02-02', '10:11:12', 3.14159)",
        )
        .unwrap();
        assert_eq!(
            rows(
                &conn,
                "SELECT id, c_int, c_big, c_ts, c_tz, c_date, c_time, c_num FROM dropped ORDER BY id"
            ),
            [
                "1|NULL|NULL|NULL|NULL|NULL|NULL|NULL",
                "2|5|9000000000|2024-01-01 10:00:00|2024-01-01 08:00:00|2024-02-02|10:11:12|3.14"
            ],
            "mvcc={mvcc}"
        );
        assert_eq!(
            core_rows(
                conn.inner(),
                "SELECT sql FROM sqlite_schema WHERE name = 'dropped'"
            ),
            [
                "CREATE TABLE dropped (id INTEGER PRIMARY KEY, keep TEXT, n numeric(10, 2), \
                 c_int INTEGER, c_big bigint, c_serial INTEGER, c_ts timestamp, c_tz timestamptz, \
                 c_date date, c_time time, c_num numeric(10, 2)) STRICT"
            ],
            "mvcc={mvcc}"
        );
        assert_eq!(
            core_rows(conn.inner(), "PRAGMA integrity_check"),
            ["ok"],
            "mvcc={mvcc}"
        );
    }
}

/// A file with a domain over a built-in type of the new tables gets a table
/// with the PGSTORAGE option, which older versions refuse. The catalog does
/// not show the table.
#[test]
fn domain_over_a_new_type_gets_a_pgstorage_table_that_the_catalog_hides() {
    let dir = copy_fixtures(&[MAIN]);
    let db = open(dir.path().join(MAIN), false);
    let conn = db.connect_postgres();
    let gate = "SELECT sql FROM sqlite_schema WHERE name = '__turso_internal_pg_storage'";
    assert!(core_rows(conn.inner(), gate).is_empty());
    conn.execute("CREATE DOMAIN dd AS timestamp").unwrap();
    assert_eq!(
        core_rows(conn.inner(), gate),
        ["CREATE TABLE __turso_internal_pg_storage (x TEXT) STRICT, PGSTORAGE"]
    );
    assert!(rows(
        &conn,
        "SELECT tablename FROM pg_tables WHERE tablename LIKE '%turso%'"
    )
    .is_empty());
    conn.execute("CREATE TABLE r (id integer PRIMARY KEY, v dd)")
        .unwrap();
    conn.execute("INSERT INTO r VALUES (1, '2024-01-01 10:00:00')")
        .unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM r"), ["1|2024-01-01 10:00:00"]);
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
}

/// The new tables store numeric(p,s) with p above 18 with the type of older
/// versions. Index keys compute their comparisons with the rules of older
/// versions, so an index whose keys differ from the values of queries is
/// refused.
#[test]
fn new_index_on_a_wide_numeric_refuses_keys_that_queries_compute_in_another_way() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE q (id int PRIMARY KEY, a numeric(30,2), b numeric(30,2), name text)",
    )
    .unwrap();
    conn.execute("INSERT INTO q VALUES (1, 5, 10, 'a'), (2, 50, 10, 'a'), (3, -5, 0, 'a')")
        .unwrap();
    for sql in [
        "CREATE INDEX q_margin ON q ((a - b > 0))",
        "CREATE INDEX q_negative ON q ((a > -1))",
        "CREATE UNIQUE INDEX q_positive ON q (name) WHERE a > -1",
        "CREATE INDEX q_frac ON q (id) WHERE a = 1.005",
    ] {
        let err = conn.execute(sql).unwrap_err().to_string();
        assert!(
            err.contains("with the numeric rules of older versions"),
            "{sql}: {err}"
        );
    }
    conn.execute("CREATE INDEX q_big ON q (id) WHERE a > 1")
        .unwrap();
    conn.execute("CREATE INDEX q_shift ON q ((a + -1))")
        .unwrap();
    conn.execute("INSERT INTO q VALUES (4, -1.5, 0, 'b'), (5, 2, 0, 'b')")
        .unwrap();
    conn.execute("UPDATE q SET a = a").unwrap();
    assert_eq!(
        rows(&conn, "SELECT id FROM q WHERE a > 1 ORDER BY id"),
        ["1", "2", "5"]
    );
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
    conn.execute("DELETE FROM q WHERE id > 0").unwrap();
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
}

/// A timestamptz value is stored in UTC and Turso has no time zone setting,
/// so its cast to time gives the same value in every session. PostgreSQL
/// refuses this cast in an index because its result depends on TimeZone.
#[test]
fn index_on_a_cast_of_timestamptz_to_time_keeps_its_keys() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id int PRIMARY KEY, tz timestamptz)")
        .unwrap();
    conn.execute("CREATE INDEX t_time ON t ((tz::time))")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, '2024-01-01 10:00+02'), (2, '2024-01-02')")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&conn, "SELECT id, tz::time FROM t"), ["2|00:00:00"]);
    assert_eq!(core_rows(conn.inner(), "PRAGMA integrity_check"), ["ok"]);
}

/// The base gave ids from the rowid and did not advance the sequence of a
/// serial PRIMARY KEY, so DEFAULT of such a key gives the next rowid, as an
/// omitted key does.
#[test]
fn default_of_a_serial_primary_key_of_base_file_gives_the_next_rowid() {
    let dir = copy_fixtures(&["pg_v1_renamed_serial.db"]);
    let db = open(dir.path().join("pg_v1_renamed_serial.db"), false);
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO rs2 VALUES (DEFAULT, '2024-02-02 10:00:00', true, 'second')")
        .unwrap();
    conn.execute("INSERT INTO rs2 (id, a) VALUES (DEFAULT, 'third')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, a FROM rs2 ORDER BY id"),
        ["1|first", "2|second", "3|third"]
    );
}

#[test]
fn alter_table_keeps_negative_defaults_of_base_file() {
    let dir = copy_fixtures(&["pg_v1_negative_default.db"]);
    let path = dir.path().join("pg_v1_negative_default.db");
    let db = open(path.clone(), false);
    let conn = db.connect_postgres();
    conn.execute("ALTER TABLE nd RENAME COLUMN note TO memo")
        .unwrap();
    conn.execute("ALTER TABLE nd ADD COLUMN b int DEFAULT -2")
        .unwrap();
    conn.execute("INSERT INTO nd (id, memo) VALUES (2, 'y')")
        .unwrap();
    let expected = ["1|-1|-1.50|x|-2", "2|-1|-1.50|y|-2"];
    assert_eq!(
        rows(&conn, "SELECT id, a, n, memo, b FROM nd ORDER BY id"),
        expected
    );
    drop(conn);
    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(
        rows(&conn, "SELECT id, a, n, memo, b FROM nd ORDER BY id"),
        expected
    );
}
