//! DML prepared through semantic HIR must change the database exactly as the
//! legacy planner does. Each case runs on two fresh databases, one per
//! planner, and compares what is left behind.

use crate::common::{limbo_exec_rows, TempDatabase};

const SETUP: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER, c TEXT, d REAL, g AS (b * 2) VIRTUAL)",
    "CREATE INDEX t_b ON t(b)",
    "CREATE INDEX t_c_desc ON t(c DESC, b)",
    "CREATE INDEX t_d ON t(d)",
    "CREATE INDEX t_g ON t(g)",
    "CREATE INDEX t_expr ON t(b + length(c))",
    // The WHERE names the table, so an aliased DELETE must still read it.
    "CREATE INDEX t_partial ON t(c) WHERE t.b > 2",
    "CREATE UNIQUE INDEX t_unique ON t(c, d)",
    "CREATE TABLE log(a INTEGER)",
    "INSERT INTO t(a, b, c, d) VALUES (1, 1, 'x', 1.0), (2, 2, 'yy', 2.5), (3, 3, 'z', 3), \
     (4, NULL, NULL, NULL), (5, 5, 'x', 5.5), (6, 2, 'w', -0.0), (7, 7, 'yy', 7)",
];

/// Run `statements` on a fresh database with HIR on or off, then return
/// every row left in `t` and `log` and the integrity check.
fn run(hir: bool, statements: &[&str]) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_generated_columns(true))
        .build();
    let conn = db.connect_limbo();
    for statement in SETUP {
        conn.execute(statement).unwrap();
    }
    conn.set_experimental_hir(hir);
    let mut results = Vec::new();
    for statement in statements {
        results.push(limbo_exec_rows(&conn, statement));
        results.push(limbo_exec_rows(&conn, "SELECT changes()"));
    }
    results.push(limbo_exec_rows(
        &conn,
        "SELECT a, b, c, d, g FROM t ORDER BY a",
    ));
    results.push(limbo_exec_rows(&conn, "SELECT a FROM log ORDER BY a"));
    results.push(limbo_exec_rows(&conn, "PRAGMA integrity_check"));
    results
}

#[test]
fn hir_delete_matches_legacy() {
    for statements in [
        &["DELETE FROM t WHERE b = 2"][..],
        &["DELETE FROM t WHERE a = 3"],
        &["DELETE FROM t WHERE a > 2 AND a < 6"],
        &["DELETE FROM t WHERE a IN (1, 4, 9)"],
        &["DELETE FROM t WHERE b IN (2, 5)"],
        &["DELETE FROM t WHERE c = 'yy' AND d = 2.5"],
        &["DELETE FROM t WHERE c > 'x'"],
        &["DELETE FROM t WHERE d = 3"],
        &["DELETE FROM t WHERE d = 0"],
        &["DELETE FROM t WHERE g = 4"],
        &["DELETE FROM t WHERE b + length(c) = 3"],
        &["DELETE FROM t WHERE b IS NULL"],
        &["DELETE FROM t WHERE c IS NULL OR b > 4"],
        &["DELETE FROM t WHERE 0"],
        &["DELETE FROM t WHERE 1"],
        &["DELETE FROM t"],
        &["DELETE FROM main.t AS x WHERE x.b < 3"],
        &["DELETE FROM t INDEXED BY t_b WHERE b > 1"],
        &["DELETE FROM t NOT INDEXED WHERE b > 1"],
        // A second DELETE runs on what the first one left behind.
        &["DELETE FROM t WHERE b = 2", "DELETE FROM t WHERE c = 'x'"],
        &["DELETE FROM t WHERE b = 2 RETURNING a, g"],
        &["DELETE FROM t WHERE c > 'x' RETURNING *, rowid, d * 2, upper(c) AS u"],
        &["DELETE FROM t WHERE 0 RETURNING a"],
        // A count row is returned only without RETURNING.
        &[
            "PRAGMA count_changes = 1",
            "DELETE FROM t WHERE b = 2 RETURNING a",
            "DELETE FROM t WHERE b = 5",
        ],
        // A WHERE subquery: the rowids are collected before any row is
        // deleted, so a subquery that reads `t` sees all of it.
        &["DELETE FROM t WHERE b IN (SELECT b FROM t WHERE c = 'x')"],
        &["DELETE FROM t WHERE b > (SELECT avg(b) FROM t)"],
        &["DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS o WHERE o.b = t.b - 2)"],
        &["DELETE FROM t WHERE EXISTS (SELECT 1 FROM log WHERE log.a = t.d) OR c IS NULL"],
        &["DELETE FROM t WHERE c NOT IN (SELECT c FROM t WHERE b > 4 AND c IS NOT NULL)"],
        &["DELETE FROM t WHERE a IN (SELECT a FROM t ORDER BY a DESC LIMIT 2) RETURNING a, g"],
        &["WITH m AS (SELECT max(a) AS v FROM t) DELETE FROM t WHERE a = (SELECT v FROM m)"],
        // Statements that still go through legacy.
        &["DELETE FROM t WHERE b = 2 RETURNING (SELECT count(*) FROM t)"],
        &[
            "CREATE TRIGGER tr AFTER DELETE ON t BEGIN INSERT INTO log VALUES (old.a); END",
            "DELETE FROM t WHERE b = 2",
        ],
        // A temp trigger also fires for a main table.
        &[
            "CREATE TEMP TRIGGER tr AFTER DELETE ON main.t BEGIN \
             INSERT INTO log VALUES (old.a); END",
            "DELETE FROM t WHERE b = 2",
        ],
        &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE child(x INTEGER REFERENCES t(a) ON DELETE CASCADE)",
            "INSERT INTO child VALUES (2), (6), (7)",
            "DELETE FROM t WHERE b = 2",
            "SELECT x FROM child ORDER BY x",
        ],
    ] {
        assert_eq!(
            run(true, statements),
            run(false, statements),
            "results differ for {statements:?}"
        );
    }
}

#[test]
fn hir_delete_returning_names_columns_like_legacy() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE r(a INTEGER PRIMARY KEY, b TEXT, c REAL)")
        .unwrap();
    for sql in [
        "DELETE FROM r RETURNING *",
        "DELETE FROM r AS x RETURNING x.a, b AS bee, c + 1, rowid",
    ] {
        let metadata = |hir: bool| {
            conn.set_experimental_hir(hir);
            let stmt = conn.prepare(sql).unwrap();
            (0..stmt.num_columns())
                .map(|column| {
                    (
                        stmt.get_column_name(column).to_string(),
                        stmt.get_column_decltype(column),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(metadata(true), metadata(false), "{sql}");
    }
}

#[test]
fn hir_delete_returning_commits_when_reset_after_the_first_row() {
    let run = |hir: bool| {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE r(a INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO r VALUES (1), (2), (3)").unwrap();
        conn.set_experimental_hir(hir);
        let mut stmt = conn
            .prepare("DELETE FROM r WHERE a > 1 RETURNING a")
            .unwrap();
        loop {
            match stmt.step().unwrap() {
                turso_core::StepResult::Row => break,
                turso_core::StepResult::IO => stmt.get_pager().io.step().unwrap(),
                other => panic!("DELETE RETURNING stopped before its first row: {other:?}"),
            }
        }
        // Every row is deleted before the first one is returned, so the
        // DELETE commits even though the caller stops reading here.
        stmt.reset().unwrap();
        drop(stmt);
        limbo_exec_rows(&conn, "SELECT a FROM r ORDER BY a")
    };
    assert_eq!(run(true), run(false));
}

#[test]
fn hir_delete_binds_parameters_like_legacy() {
    let run = |hir: bool| {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX p_b ON p(b)").unwrap();
        conn.execute("INSERT INTO p VALUES (1, 'x'), (2, 'y'), (3, 'x'), (4, 'z')")
            .unwrap();
        conn.set_experimental_hir(hir);
        let mut stmt = conn
            .prepare("DELETE FROM p WHERE b = ?1 OR a = :id")
            .unwrap();
        stmt.bind_at(1.try_into().unwrap(), turso_core::Value::build_text("x"))
            .unwrap();
        stmt.bind_at(2.try_into().unwrap(), turso_core::Value::from_i64(4))
            .unwrap();
        stmt.run_ignore_rows().unwrap();
        limbo_exec_rows(&conn, "SELECT a, b FROM p ORDER BY a")
    };
    assert_eq!(run(true), run(false));
}

#[test]
fn hir_delete_errors_match_legacy() {
    let db = TempDatabase::new_empty();
    let legacy = db.connect_limbo();
    legacy
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b)")
        .unwrap();
    legacy.set_experimental_hir(false);
    let hir = db.connect_limbo();
    hir.set_experimental_hir(true);
    for sql in [
        "DELETE FROM nosuch",
        "DELETE FROM main.nosuch",
        "DELETE FROM nosuch.t",
        "DELETE FROM t WHERE nosuch = 1",
        "DELETE FROM t WHERE x.b = 1",
        "DELETE FROM sqlite_schema",
        "DELETE FROM t INDEXED BY nosuch",
        "DELETE FROM t RETURNING t.*",
    ] {
        let legacy_error = legacy
            .prepare(sql)
            .err()
            .expect("legacy rejects")
            .to_string();
        let hir_error = hir.prepare(sql).err().expect("HIR rejects").to_string();
        assert_eq!(hir_error, legacy_error, "{sql}");
    }
}
