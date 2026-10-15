//! DML prepared through semantic HIR must change the database exactly as the
//! legacy planner does. Each case runs on two fresh databases, one per
//! planner, and compares what is left behind.

use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, TempDatabase};

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
    // No generated column, so UPDATEs of `u` can go through HIR.
    "CREATE TABLE u(a INTEGER PRIMARY KEY, b INTEGER NOT NULL, c TEXT, d REAL)",
    "CREATE UNIQUE INDEX u_b ON u(b)",
    "CREATE INDEX u_c ON u(c, d DESC)",
    "CREATE INDEX u_expr ON u(c || 'x', b)",
    "CREATE INDEX u_partial ON u(c) WHERE u.d > 0",
    "CREATE UNIQUE INDEX u_partial_unique ON u(d) WHERE c = 'p'",
    "INSERT INTO u VALUES (1, 1, 'x', 1.0), (2, 2, 'yy', -1), (3, 3, 'z', 3), \
     (4, 4, NULL, NULL), (5, 5, 'p', 5), (6, 6, 'q', 5)",
    "INSERT INTO t(a, b, c, d) VALUES (1, 1, 'x', 1.0), (2, 2, 'yy', 2.5), (3, 3, 'z', 3), \
     (4, NULL, NULL, NULL), (5, 5, 'x', 5.5), (6, 2, 'w', -0.0), (7, 7, 'yy', 7)",
];

type Rows = Vec<Vec<rusqlite::types::Value>>;

/// Run `statements` on a fresh database with HIR on or off, then return
/// what each one returned or its error, every row left in the tables, and
/// the integrity check.
fn run(hir: bool, statements: &[&str]) -> Vec<Result<Rows, String>> {
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
        results.push(limbo_exec_rows_fallible(&db, &conn, statement).map_err(|e| e.to_string()));
        results.push(Ok(limbo_exec_rows(&conn, "SELECT changes()")));
    }
    for sql in [
        "SELECT a, b, c, d, g FROM t ORDER BY a",
        "SELECT a, b, c, d FROM u ORDER BY a",
        "SELECT a FROM log ORDER BY a",
        "PRAGMA integrity_check",
    ] {
        results.push(Ok(limbo_exec_rows(&conn, sql)));
    }
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
        // Triggers run from HIR-compiled subprograms.
        &[
            "CREATE TRIGGER tr AFTER DELETE ON t BEGIN INSERT INTO log VALUES (old.a); END",
            "DELETE FROM t WHERE b = 2",
        ],
        &[
            "CREATE TRIGGER bd BEFORE DELETE ON t BEGIN INSERT INTO log VALUES (old.g); END",
            "CREATE TRIGGER ad AFTER DELETE ON t WHEN old.b > 2 BEGIN \
             INSERT INTO log VALUES ((SELECT count(*) FROM t)); SELECT 1; \
             UPDATE u SET d = old.a WHERE a = 1; END",
            "DELETE FROM t WHERE b > 1 RETURNING a",
        ],
        &[
            "CREATE TRIGGER ig BEFORE DELETE ON t WHEN old.a = 3 BEGIN SELECT RAISE(IGNORE); END",
            "DELETE FROM t",
        ],
        &[
            "CREATE TRIGGER ab BEFORE DELETE ON t WHEN old.a = 3 BEGIN SELECT RAISE(ABORT, 'no'); END",
            "DELETE FROM t",
        ],
        // A trigger whose body deletes from its own table does not fire
        // itself again.
        &[
            "CREATE TRIGGER chain AFTER DELETE ON t BEGIN \
             DELETE FROM t WHERE a = old.a + 1; INSERT INTO log VALUES (old.a); END",
            "DELETE FROM t WHERE a = 1",
        ],
        // A temp trigger also fires for a main table, and before the table's
        // own triggers.
        &[
            "CREATE TEMP TRIGGER tr AFTER DELETE ON main.t BEGIN \
             INSERT INTO log VALUES (old.a); END",
            "DELETE FROM t WHERE b = 2",
        ],
        &[
            "CREATE TRIGGER m1 AFTER DELETE ON t BEGIN INSERT INTO log VALUES (1); END",
            "CREATE TRIGGER m2 AFTER DELETE ON t BEGIN INSERT INTO log VALUES (2); END",
            "CREATE TEMP TRIGGER t1 AFTER DELETE ON main.t BEGIN INSERT INTO log VALUES (3); END",
            "CREATE TEMP TRIGGER t2 AFTER DELETE ON main.t BEGIN INSERT INTO log VALUES (4); END",
            "DELETE FROM t WHERE a = 1",
            "SELECT rowid, a FROM log ORDER BY rowid",
        ],
        // OLD.x compares without the column's affinity, so the REAL 7.0
        // is not equal to the text '7'. OLD.a reads the rowid, which keeps
        // INTEGER affinity.
        &[
            "CREATE TRIGGER aff BEFORE DELETE ON t WHEN old.d = '7' OR old.a = '1' BEGIN \
             INSERT INTO log VALUES (old.a); END",
            "DELETE FROM t",
        ],
        // A trigger on a TEMP table finds an unqualified main table.
        &[
            "CREATE TEMP TABLE tt(x)",
            "INSERT INTO tt VALUES (2), (5)",
            "CREATE TRIGGER tt_d AFTER DELETE ON temp.tt BEGIN \
             DELETE FROM t WHERE a = old.x; INSERT INTO log VALUES (old.x); END",
            "DELETE FROM tt WHERE x = 2",
        ],
        // With foreign keys off, a child table whose parent table is
        // missing can still be written.
        &[
            "CREATE TABLE orphan(x REFERENCES missing(id))",
            "INSERT INTO orphan VALUES (1), (2)",
            "UPDATE orphan SET x = 3 WHERE x = 2",
            "DELETE FROM orphan WHERE x = 1",
            "SELECT x FROM orphan",
        ],
        // Statements that still go through legacy.
        &["DELETE FROM t WHERE b = 2 RETURNING (SELECT count(*) FROM t)"],
        &[
            "CREATE TRIGGER lg AFTER INSERT ON log BEGIN DELETE FROM u WHERE a = new.a; END",
            "CREATE TRIGGER tr AFTER DELETE ON t BEGIN INSERT INTO log VALUES (old.a); END",
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
fn hir_update_matches_legacy() {
    for statements in [
        &["UPDATE u SET c = 'k' WHERE a = 3"][..],
        &["UPDATE u SET b = b + 10, d = 5 WHERE c = 'yy'"],
        &["UPDATE u SET d = d * 2"],
        &["UPDATE u SET c = NULL, d = -d WHERE d > 0"],
        &["UPDATE u SET d = '7', c = 12 WHERE a = 4"],
        &["UPDATE u SET c = 'p' WHERE a = 6"],
        &["UPDATE u SET c = 'p', d = 9 WHERE a = 6"],
        &["UPDATE u AS x SET d = x.d + 1 WHERE x.c > 'p'"],
        &["UPDATE u SET b = 7 WHERE b = 2"],
        &["UPDATE u SET b = b WHERE 1"],
        &["UPDATE u SET c = 'y' WHERE 0"],
        &["UPDATE u SET d = 1 WHERE c IN ('x', 'z') RETURNING *, rowid, d + 1"],
        // The new row reads an INTEGER PRIMARY KEY as its rowid.
        &[
            "CREATE TABLE w(a INTEGER PRIMARY KEY, b INTEGER)",
            "CREATE INDEX w_expr ON w(a * 10 + b)",
            "INSERT INTO w VALUES (1, 1), (2, 2)",
            "UPDATE w SET b = b + 1 RETURNING a, a + 0, w.a",
            "SELECT a FROM w WHERE a * 10 + b = 13",
        ],
        // The loop reads u_b while c changes.
        &["UPDATE u INDEXED BY u_b SET c = 'w' WHERE b > 2"],
        // Constraint failures and OR IGNORE.
        &["UPDATE u SET b = 1 WHERE a = 3"],
        &["UPDATE OR IGNORE u SET b = 1 WHERE a > 1"],
        &["UPDATE OR FAIL u SET b = 3 WHERE a IN (1, 2)"],
        &["UPDATE OR ROLLBACK u SET b = 3 WHERE a = 1"],
        &["UPDATE u SET b = NULL WHERE a = 2"],
        &["UPDATE OR IGNORE u SET b = NULL WHERE a > 4"],
        &["UPDATE u SET c = 'p' WHERE a = 6"],
        &["PRAGMA count_changes = 1", "UPDATE u SET d = 0 WHERE a < 3"],
        // Statements that still go through legacy.
        &[
            "CREATE TABLE r(a INTEGER PRIMARY KEY, b INTEGER NOT NULL ON CONFLICT REPLACE DEFAULT 9)",
            "INSERT INTO r VALUES (1, 1)",
            "UPDATE r SET b = NULL",
            "SELECT * FROM r",
        ],
        &["UPDATE u SET a = a + 10 WHERE a = 1"],
        &["UPDATE OR REPLACE u SET b = 1 WHERE a = 2"],
        &["UPDATE u SET b = b + 100 WHERE b > 2"],
        &["UPDATE u SET c = (SELECT max(c) FROM u) WHERE a = 1"],
        &["UPDATE u SET d = 1 WHERE b IN (SELECT b FROM u WHERE c = 'x')"],
        &["UPDATE u SET c = log.a FROM log WHERE log.a = u.a"],
        &["UPDATE t SET b = 9 WHERE a = 1"],
        // Triggers run from HIR-compiled subprograms.
        &[
            "CREATE TRIGGER tr AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (old.a); END",
            "UPDATE u SET c = 'k' WHERE a < 3",
        ],
        // A temp trigger also fires for a main table.
        &[
            "CREATE TEMP TRIGGER tr AFTER UPDATE OF c ON main.u BEGIN \
             INSERT INTO log VALUES (old.a); END",
            "UPDATE u SET c = 'k' WHERE a < 3",
        ],
        &[
            "CREATE TRIGGER bu BEFORE UPDATE ON u WHEN new.d > old.d BEGIN \
             INSERT INTO log VALUES (new.rowid * 100 + old.b); END",
            "CREATE TRIGGER au AFTER UPDATE OF d ON u BEGIN \
             INSERT INTO log VALUES (new.d); END",
            "CREATE TRIGGER ac AFTER UPDATE OF c ON u BEGIN INSERT INTO log VALUES (-1); END",
            "UPDATE u SET d = d + 1 WHERE a IN (1, 3, 4) RETURNING a, d",
        ],
        // A BEFORE trigger that changes the row: the columns the UPDATE does
        // not set are read again, so its change is kept.
        &[
            "CREATE TRIGGER bu BEFORE UPDATE OF c ON u BEGIN \
             UPDATE u SET d = 100 WHERE a = old.a; END",
            "UPDATE u SET c = 'k' WHERE a < 4",
        ],
        // A BEFORE trigger that deletes the row skips it.
        &[
            "CREATE TRIGGER bu BEFORE UPDATE ON u BEGIN DELETE FROM u WHERE a = old.a + 1; END",
            "UPDATE u SET c = 'k'",
        ],
        &[
            "CREATE TRIGGER ig BEFORE UPDATE ON u WHEN old.a = 2 BEGIN SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER au AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "UPDATE u SET c = 'k'",
        ],
        &[
            "CREATE TRIGGER ab AFTER UPDATE ON u WHEN new.a = 3 BEGIN \
             SELECT RAISE(ABORT, 'no'); END",
            "UPDATE u SET c = 'k'",
        ],
        // NOT NULL is checked after the BEFORE triggers, which can fix the row.
        &[
            "CREATE TRIGGER bu BEFORE UPDATE ON u BEGIN INSERT INTO log VALUES (old.a); END",
            "UPDATE u SET b = NULL WHERE a = 2",
        ],
        // OR IGNORE in the statement also applies inside the trigger.
        &[
            "CREATE TRIGGER au AFTER UPDATE ON u BEGIN \
             UPDATE u SET b = 1 WHERE a = new.a + 1; INSERT INTO log VALUES (new.a); END",
            "UPDATE OR IGNORE u SET c = 'k' WHERE a < 3",
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
fn hir_insert_matches_legacy() {
    for statements in [
        &["INSERT INTO u VALUES (10, 10, 'n', 1.5)"][..],
        &["INSERT INTO u(b, d) VALUES (20, 2), (21, -1), (22, '4')"],
        &["INSERT INTO u(rowid, b) VALUES (30, 30)"],
        &["INSERT INTO u(a, b) VALUES (NULL, 31)"],
        &["INSERT INTO u(b, c) VALUES (32, 'r') RETURNING *, rowid, c || '!'"],
        &["INSERT INTO u(b, c) VALUES ('33', 5), (x'01', NULL)"],
        &["INSERT INTO u(c, b, c) VALUES ('first', 34, 'second')"],
        &[
            "INSERT INTO log DEFAULT VALUES",
            "INSERT INTO log VALUES (1), (2), (3)",
        ],
        // Constraint failures and their conflict modes.
        &["INSERT INTO u VALUES (1, 99, 'y', 1)"],
        &["INSERT INTO u(b) VALUES (1)"],
        &["INSERT INTO u(b) VALUES (NULL)"],
        &["INSERT INTO u(b, c, d) VALUES (40, 'p', 5)"],
        &["INSERT OR IGNORE INTO u VALUES (1, 99, 'y', 1), (40, 1, 'z', 1), (41, 41, 'q', 0)"],
        &["INSERT OR IGNORE INTO u(b) VALUES (NULL), (42)"],
        &["INSERT OR FAIL INTO u(b) VALUES (43), (1), (44)"],
        &["INSERT OR ROLLBACK INTO u(b) VALUES (45), (1)"],
        &["INSERT OR ABORT INTO u(b) VALUES (46), (1)"],
        &[
            "PRAGMA count_changes = 1",
            "INSERT INTO u(b) VALUES (47), (48)",
        ],
        &["INSERT INTO u(b) VALUES (49)", "SELECT last_insert_rowid()"],
        &["INSERT INTO u(rowid, b) VALUES ('x', 50)"],
        // Like legacy, a double-quoted name is a string only as a whole
        // value of a single VALUES row.
        &["INSERT INTO u(b, c) VALUES (51, \"s\")"],
        &["INSERT INTO u(b, c) VALUES (52, upper(\"s\"))"],
        &["INSERT INTO u(b, c) VALUES (53, upper(\"s\")), (54, \"t\")"],
        // Statements that still go through legacy.
        &["INSERT INTO u(b) SELECT b + 100 FROM u"],
        &["INSERT OR REPLACE INTO u VALUES (1, 60, 'v', 1)"],
        &["INSERT INTO u(b) VALUES (1) ON CONFLICT DO NOTHING"],
        &["INSERT INTO u(b) VALUES (1) ON CONFLICT(b) DO UPDATE SET c = log.a"],
        &["INSERT INTO u(b, c) VALUES (61, (SELECT max(c) FROM u))"],
        &["INSERT INTO t(b) VALUES (62)"],
        // Triggers run from HIR-compiled subprograms. NEW.rowid reads -1 in
        // a BEFORE trigger until the rowid is known.
        &[
            "CREATE TRIGGER tr AFTER INSERT ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "INSERT INTO u(b) VALUES (63)",
        ],
        &[
            "CREATE TRIGGER bi BEFORE INSERT ON u BEGIN INSERT INTO log VALUES (new.rowid); END",
            "CREATE TRIGGER ai AFTER INSERT ON u WHEN new.b > 64 BEGIN \
             INSERT INTO log VALUES (new.a * 1000 + new.b); END",
            "INSERT INTO u(b) VALUES (64), (65)",
            "INSERT INTO u(a, b) VALUES (100, 66), (NULL, 67)",
            "INSERT INTO u(rowid, b) VALUES (200, 68) RETURNING a",
        ],
        &[
            "CREATE TRIGGER ig BEFORE INSERT ON u WHEN new.b = 70 BEGIN SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER aig AFTER INSERT ON u WHEN new.b = 71 BEGIN \
             SELECT RAISE(IGNORE); INSERT INTO log VALUES (0); END",
            "INSERT INTO u(b) VALUES (69), (70), (71) RETURNING b",
        ],
        // The statement's OR IGNORE also applies inside its triggers.
        &[
            "CREATE TABLE once(p UNIQUE)",
            "CREATE TRIGGER uo AFTER INSERT ON u BEGIN INSERT INTO once VALUES (1); END",
            "INSERT OR IGNORE INTO u(b) VALUES (72), (73)",
            "INSERT INTO u(b) VALUES (74), (75)",
            "SELECT p FROM once",
        ],
        // A trigger fired from another trigger.
        &[
            "CREATE TRIGGER lg AFTER INSERT ON log BEGIN DELETE FROM u WHERE a = 1; END",
            "CREATE TRIGGER tr AFTER INSERT ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "INSERT INTO u(b) VALUES (76)",
        ],
        &[
            "CREATE TEMP TRIGGER tr AFTER INSERT ON main.u BEGIN \
             INSERT INTO log VALUES (new.a); END",
            "INSERT INTO u(b) VALUES (64)",
        ],
        &[
            "CREATE TABLE auto(a INTEGER PRIMARY KEY AUTOINCREMENT, b)",
            "INSERT INTO auto(b) VALUES (1), (2)",
            "SELECT * FROM auto",
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
