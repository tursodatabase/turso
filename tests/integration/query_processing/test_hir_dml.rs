//! DML prepared through semantic HIR must change the database exactly as the
//! legacy planner does. Each case runs on two fresh databases, one per
//! planner, and compares what is left behind.

use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, sqlite_exec_rows, TempDatabase};

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
        // REPLACE deletes the rows the new row conflicts with.
        &["UPDATE OR REPLACE u SET b = 1 WHERE a = 2"],
        &["UPDATE OR REPLACE u SET b = b + 1"],
        &["UPDATE OR REPLACE u SET b = 7 - b"],
        &["UPDATE OR REPLACE u SET c = 'p', d = 5 WHERE a = 6"],
        &["UPDATE OR REPLACE u SET b = 3 WHERE a = 1 RETURNING *"],
        &["UPDATE OR REPLACE u SET b = NULL WHERE a = 1"],
        &[
            "CREATE TRIGGER tr AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "UPDATE OR REPLACE u SET b = 1 WHERE a > 2",
        ],
        // A DELETE inside the trigger fires its own triggers with ABORT.
        &[
            "CREATE UNIQUE INDEX log_a ON log(a)",
            "CREATE TRIGGER lbd BEFORE DELETE ON log BEGIN INSERT INTO log VALUES (1), (1); END",
            "INSERT INTO log VALUES (5)",
            "CREATE TRIGGER tr AFTER UPDATE ON u BEGIN DELETE FROM log WHERE a = 5; END",
            "UPDATE OR REPLACE u SET d = 9 WHERE a = 1",
        ],
        &[
            "CREATE TABLE ur(a INTEGER PRIMARY KEY, b UNIQUE ON CONFLICT REPLACE, \
             c NOT NULL ON CONFLICT REPLACE DEFAULT 'dflt', d UNIQUE)",
            "INSERT INTO ur VALUES (1, 1, 'x', 1), (2, 2, 'y', 2), (3, 3, 'z', 3)",
            "UPDATE ur SET b = 2 WHERE a = 1",
            "UPDATE ur SET c = NULL WHERE a = 3",
            "UPDATE ur SET d = 3 WHERE a = 1",
            "UPDATE OR IGNORE ur SET b = 3 WHERE a = 1",
            "UPDATE OR ABORT ur SET c = NULL",
            "SELECT * FROM ur ORDER BY a",
            "PRAGMA integrity_check",
        ],
        // A subquery in WHERE: the rows to change are collected first, so the
        // subquery never sees a changed row.
        &["UPDATE u SET d = 1 WHERE b IN (SELECT b FROM u WHERE c = 'x')"],
        &["UPDATE u SET b = b + 1 WHERE b < (SELECT max(b) FROM u)"],
        &["UPDATE u SET d = d + 1 WHERE EXISTS (SELECT 1 FROM t WHERE t.a = u.a AND t.c = u.c)"],
        &["UPDATE u SET b = b + 10 WHERE (SELECT count(*) FROM u AS x WHERE x.b > u.b) > 2"],
        &["UPDATE OR IGNORE u SET b = 2 WHERE a IN (SELECT a FROM t) RETURNING a, b"],
        // A RETURNING subquery that reads the target runs again for each
        // row.
        &["UPDATE u SET d = 0 WHERE a < 3 RETURNING a, (SELECT sum(d) FROM u), \
           (SELECT count(*) FROM u AS x WHERE x.d = u.d)"],
        // Subqueries in SET read the table as the loop changes it, row by
        // row.
        &["UPDATE u SET c = (SELECT max(c) FROM u) WHERE a = 1"],
        &["UPDATE u SET d = (SELECT max(x.d) FROM u AS x WHERE x.a < u.a)"],
        &["UPDATE u SET d = (SELECT sum(x.d) FROM u AS x WHERE x.a <= u.a) WHERE a > 2"],
        &["UPDATE u SET b = (SELECT max(b) FROM u) + a"],
        // A new rowid moves the row: every index entry changes, a taken rowid
        // conflicts, and the rows are collected first.
        &[
            "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d CHECK (d IS NOT 'bad'))",
            "CREATE INDEX u_c ON u(c)",
            "CREATE TABLE n(x, y)",
            "CREATE TABLE log(o, nw)",
            "INSERT INTO u VALUES (1, 1, 'x', 1), (2, 2, 'y', 2), (3, 3, 'z', 3), (5, 5, 'p', 5)",
            "INSERT INTO n(rowid, x, y) VALUES (1, 'a', 1), (2, 'b', 2)",
            "UPDATE u SET a = a + 10",
            "SELECT * FROM u ORDER BY a",
            "UPDATE u SET a = 12 WHERE a = 11",
            "UPDATE OR IGNORE u SET a = 12 WHERE a = 11",
            "UPDATE OR REPLACE u SET a = 12 WHERE a = 11",
            "SELECT * FROM u ORDER BY a",
            "SELECT c, a FROM u INDEXED BY u_c ORDER BY c",
            "UPDATE u SET rowid = 100, c = 'q' WHERE b = 3",
            "UPDATE u SET a = a WHERE b = 5",
            "UPDATE u SET a = NULL WHERE b = 5",
            "UPDATE u SET a = 'abc' WHERE b = 5",
            "UPDATE u SET a = '7' WHERE b = 5",
            "SELECT * FROM u ORDER BY a",
            "UPDATE n SET rowid = rowid + 1",
            "SELECT rowid, * FROM n ORDER BY rowid",
            "UPDATE n SET rowid = 3 WHERE x = 'a'",
            "CREATE TRIGGER t AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (old.a, new.a); END",
            "CREATE TRIGGER tb BEFORE UPDATE ON u BEGIN INSERT INTO log VALUES (old.rowid, new.rowid); END",
            "UPDATE u SET a = a * 2 WHERE a < 50 RETURNING a, b, rowid",
            "SELECT * FROM log",
            "UPDATE u SET (a, c) = (SELECT 300, 'row') WHERE a = 200",
            "UPDATE u SET a = 7, d = 'bad' WHERE a = 300",
            "UPDATE OR IGNORE u SET a = a + 1, d = 'bad'",
            "SELECT * FROM u ORDER BY a",
            "PRAGMA integrity_check",
        ],
        // The rightmost of two assignments to the rowid wins.
        &[
            "UPDATE u SET a = 1, rowid = 20 WHERE a = 2",
            "UPDATE u SET rowid = 7, a = 8 WHERE a = 20",
        ],
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
        &["UPDATE u SET (c, d) = (SELECT 'm', a FROM u AS x WHERE x.a = u.a + 1) WHERE a > 4"],
        // UPDATE ... FROM: the join fills a scratch table first.
        &["UPDATE u SET c = log.a FROM log WHERE log.a = u.a"],
        &["UPDATE u SET c = t.c, d = t.d + u.d FROM t WHERE t.a = u.a RETURNING *"],
        &["UPDATE u SET d = t.b FROM t WHERE t.b = u.b AND t.c IS NOT NULL"],
        &["UPDATE u SET c = x.c FROM (SELECT a, c FROM t WHERE a > 2) AS x WHERE x.a = u.a"],
        &["WITH x AS (SELECT a, count(*) AS n FROM t GROUP BY a) UPDATE u SET d = n FROM x WHERE x.a = u.a"],
        &["UPDATE u SET b = t.b + 100 FROM t LEFT JOIN log ON log.a = t.a WHERE t.a = u.a"],
        &["UPDATE u SET c = coalesce(t.c, 'none') FROM t RIGHT JOIN log ON t.a = log.a WHERE u.a = 1"],
        &["UPDATE u SET b = 2 FROM t WHERE t.a = u.a AND u.a = 1"],
        &["UPDATE OR IGNORE u SET b = 2 FROM t WHERE t.a = u.a"],
        &["UPDATE u SET (c, d) = ((t.c, t.d)) FROM t WHERE t.a = u.a"],
        &["UPDATE u SET c = 'k' FROM t, t WHERE t.a = u.a"],
        &[
            "CREATE TRIGGER tr AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "UPDATE u SET c = t.c FROM t WHERE t.a = u.a",
        ],
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
        // INSERT ... SELECT runs the query as a coroutine; a query that
        // reads the target first fills a temp table.
        &["INSERT INTO u(b, c) SELECT b + 100, c FROM t WHERE b IS NOT NULL RETURNING a, b"],
        &["INSERT INTO u(b, c, d) SELECT b + 10, c, d FROM u", "SELECT last_insert_rowid()"],
        &["INSERT INTO u SELECT a + 100, b + 100, c, d FROM u ORDER BY a DESC LIMIT 2 RETURNING *"],
        &["INSERT INTO log SELECT count(*) FROM t"],
        &["INSERT INTO u(b) SELECT 1"],
        &["INSERT OR IGNORE INTO u(b, c) SELECT b, 'dup' FROM t"],
        &["INSERT INTO u(b) SELECT 200 UNION ALL SELECT 201 UNION SELECT 200"],
        &["WITH x AS (SELECT 300 AS v) INSERT INTO u(b) SELECT v FROM x"],
        &["INSERT INTO u(b, c) SELECT g, 'g' FROM t WHERE g > 4 AND EXISTS (SELECT 1 FROM u WHERE u.b = 1)"],
        &[
            "CREATE TRIGGER tr AFTER INSERT ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "INSERT INTO u(b) SELECT a + 50 FROM t",
        ],
        &[
            "INSERT INTO u(a, b) SELECT a, b + 1000 FROM t WHERE true \
             ON CONFLICT(a) DO UPDATE SET c = 'from t'",
        ],
        &["INSERT OR REPLACE INTO u(a, b, c) SELECT a, a, 'r' FROM t"],
        &["PRAGMA count_changes = 1", "INSERT INTO u(b) SELECT a + 60 FROM t"],
        // UPSERT: the clause naming a conflict decides what happens.
        &["INSERT INTO u VALUES (1, 50, 'n', 0) ON CONFLICT DO NOTHING"],
        &["INSERT INTO u VALUES (1, 50, 'n', 0) ON CONFLICT(a) DO UPDATE SET c = excluded.c || c, d = d + 1"],
        &["INSERT INTO u(b, c) VALUES (2, 'q') ON CONFLICT(b) DO UPDATE SET c = 'up' WHERE d > 0 RETURNING *"],
        &["INSERT INTO u(b, c) VALUES (2, 'q') ON CONFLICT(b) DO UPDATE SET c = 'up' WHERE d < 0 RETURNING *"],
        &[
            "INSERT INTO u(b, c) VALUES (2, 'q'), (3, 'r'), (60, 's') \
             ON CONFLICT(b) DO UPDATE SET b = excluded.b + 100 RETURNING a, b",
        ],
        &["INSERT INTO u VALUES (1, 2, 'x', 1) ON CONFLICT(a) DO UPDATE SET c = 'z'"],
        &["INSERT INTO u VALUES (9, 2, 'x', 1) ON CONFLICT(a) DO NOTHING"],
        &[
            "INSERT INTO u VALUES (1, 2, 'x', 1) ON CONFLICT(b) DO UPDATE SET c = 'b' \
             ON CONFLICT DO NOTHING",
        ],
        &["INSERT INTO u VALUES (1, 9, 'x', 1) ON CONFLICT(a) DO UPDATE SET b = 2"],
        &["INSERT INTO u VALUES (1, 9, 'x', 1) ON CONFLICT(a) DO UPDATE SET b = NULL"],
        &["INSERT INTO u VALUES (20, 20, 'p', 5) ON CONFLICT DO UPDATE SET d = 6, c = 'pp'"],
        &["INSERT INTO u VALUES (1, 1, 'x', 1) ON CONFLICT DO UPDATE SET c = 'same' RETURNING *"],
        &["INSERT OR REPLACE INTO u VALUES (1, 2, 'x', 1) ON CONFLICT(a) DO UPDATE SET c = 'r'"],
        &["INSERT OR FAIL INTO u VALUES (30, 30, 'x', 1), (31, 2, 'y', 1) ON CONFLICT(a) DO NOTHING"],
        &[
            "CREATE TRIGGER ai AFTER INSERT ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "INSERT INTO u VALUES (1, 70, 'x', 1), (71, 71, 'y', 1) ON CONFLICT DO UPDATE SET d = 0",
        ],
        &[
            "CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER, n INTEGER DEFAULT 0)",
            "INSERT INTO kv VALUES ('a', 1, 0)",
            "INSERT INTO kv(k, v) VALUES ('a', 5), ('b', 6), ('a', 7) \
             ON CONFLICT(k) DO UPDATE SET v = v + excluded.v, n = n + 1",
            "SELECT * FROM kv ORDER BY k",
        ],
        // The ON CONFLICT target picks the UNIQUE index with the same key
        // terms, in any order, with the same collation and WHERE clause.
        &[
            "CREATE TABLE m(x, y, z TEXT COLLATE nocase, w, UNIQUE(x, y))",
            "CREATE UNIQUE INDEX m_z ON m(z)",
            "CREATE UNIQUE INDEX m_w ON m(abs(w)) WHERE w > 0",
            "INSERT INTO m VALUES (1, 1, 'a', 1), (1, 2, 'b', 2)",
            "INSERT INTO m VALUES (1, 2, 'c', 3) ON CONFLICT(y, x) DO UPDATE SET z = 'yx'",
            "INSERT INTO m VALUES (9, 9, 'A', 4) ON CONFLICT(z) DO UPDATE SET w = 10",
            "INSERT INTO m VALUES (9, 9, 'A', 4) ON CONFLICT(z COLLATE NOCASE) DO UPDATE SET w = 11",
            "INSERT INTO m VALUES (9, 9, 'A', 4) ON CONFLICT(z COLLATE binary) DO NOTHING",
            "INSERT INTO m VALUES (8, 8, 'q', -11) ON CONFLICT(abs(w)) WHERE w > 0 DO UPDATE SET x = 0",
            "INSERT INTO m VALUES (8, 8, 'q', 11) ON CONFLICT(abs(w)) DO NOTHING",
            "INSERT INTO m VALUES (8, 8, 'q', 11) ON CONFLICT(abs(w)) WHERE w > 0 DO UPDATE SET x = 0",
            "INSERT INTO m VALUES (8, 8, 'q', 12) ON CONFLICT(x) DO NOTHING",
            "SELECT * FROM m ORDER BY rowid",
        ],
        // CHECK constraints: IGNORE skips the row, REPLACE still aborts, an
        // UPSERT checks with ABORT, and an UPDATE checks only the
        // constraints that read a changed column.
        &[
            "CREATE TABLE ck(a INTEGER PRIMARY KEY, b INT CHECK (b > 0), c TEXT, d, \
             CONSTRAINT named CHECK (length(c) < 4), CHECK ( \"d\" IS NOT 'bad' ))",
            "INSERT INTO ck VALUES (1, 1, 'x', 0)",
            "INSERT INTO ck VALUES (2, 0, 'x', 0)",
            "INSERT INTO ck VALUES (3, 1, 'xxxxx', 0)",
            "INSERT INTO ck VALUES (4, 1, 'x', 'bad')",
            "INSERT INTO ck VALUES (5, NULL, NULL, NULL)",
            "INSERT OR IGNORE INTO ck VALUES (6, -1, 'x', 0), (7, 7, 'x', 0)",
            "INSERT OR REPLACE INTO ck VALUES (1, -1, 'x', 0)",
            "INSERT INTO ck VALUES (1, 2, 'y', 0) ON CONFLICT(a) DO UPDATE SET b = -excluded.b",
            "INSERT INTO ck VALUES (1, 2, 'y', 0) ON CONFLICT(a) DO UPDATE SET c = 'zz'",
            "UPDATE ck SET b = 0 WHERE a = 1",
            "UPDATE ck SET c = 'long' WHERE a = 1",
            "UPDATE OR IGNORE ck SET b = b - 1",
            "PRAGMA ignore_check_constraints = 1",
            "INSERT INTO ck VALUES (8, -8, 'xxxxxx', 'bad')",
            "UPDATE ck SET b = -9 WHERE a = 7",
            "PRAGMA ignore_check_constraints = 0",
            "UPDATE ck SET c = c WHERE a = 8",
            "UPDATE ck SET d = 1 WHERE a = 8",
            "CREATE TABLE ckr(x INTEGER PRIMARY KEY CHECK (x < 100), y CHECK (y <> x))",
            "INSERT INTO ckr VALUES (5, 6)",
            "INSERT INTO ckr VALUES (500, 6)",
            "INSERT INTO ckr(y) VALUES (1)",
            "UPDATE ckr SET y = 5",
            "SELECT * FROM ck ORDER BY a",
            "SELECT * FROM ckr ORDER BY x",
        ],
        // AUTOINCREMENT keeps the largest rowid ever used in sqlite_sequence.
        &[
            "CREATE TABLE ai(a INTEGER PRIMARY KEY AUTOINCREMENT, b UNIQUE, c CHECK (c IS NOT 'bad'))",
            "CREATE TABLE log(x)",
            "INSERT INTO ai(b) VALUES (1)",
            "INSERT INTO ai(b) VALUES (2), (3)",
            "SELECT * FROM sqlite_sequence",
            "INSERT INTO ai VALUES (10, 4, NULL)",
            "SELECT * FROM sqlite_sequence",
            "INSERT INTO ai VALUES (5, 5, NULL)",
            "SELECT * FROM sqlite_sequence",
            "DELETE FROM ai WHERE a = 10",
            "INSERT INTO ai(b) VALUES (6)",
            "SELECT * FROM ai ORDER BY a",
            "SELECT * FROM sqlite_sequence",
            "INSERT OR IGNORE INTO ai VALUES (20, 7, 'bad')",
            "SELECT * FROM sqlite_sequence",
            "INSERT INTO ai VALUES (NULL, 8, NULL), (30, 9, NULL), (NULL, 10, NULL)",
            "SELECT * FROM ai ORDER BY a",
            "SELECT * FROM sqlite_sequence",
            "INSERT INTO ai(b) VALUES (1)",
            "INSERT OR IGNORE INTO ai(b) VALUES (1), (11)",
            "SELECT * FROM sqlite_sequence",
            "INSERT OR REPLACE INTO ai(b, c) VALUES (1, 'r')",
            "SELECT * FROM ai ORDER BY a",
            "SELECT * FROM sqlite_sequence",
            "CREATE TABLE ai2(a INTEGER PRIMARY KEY AUTOINCREMENT, b)",
            "INSERT INTO ai2(b) SELECT b FROM ai WHERE 0",
            "SELECT * FROM sqlite_sequence",
            "CREATE TRIGGER t AFTER INSERT ON ai2 BEGIN INSERT INTO ai(b) VALUES (new.b + 1000); END",
            "INSERT INTO ai2(b) SELECT b FROM ai WHERE b < 4",
            "SELECT * FROM ai ORDER BY a",
            "SELECT * FROM ai2 ORDER BY a",
            "SELECT * FROM sqlite_sequence",
            "INSERT INTO ai VALUES (9223372036854775807, 99, NULL)",
            "INSERT INTO ai(b) VALUES (100)",
            "SELECT * FROM sqlite_sequence",
            "INSERT INTO ai(b) VALUES (2) ON CONFLICT(b) DO UPDATE SET c = 'up'",
            "INSERT INTO ai VALUES (50, 2, NULL) ON CONFLICT(b) DO NOTHING",
            "SELECT * FROM sqlite_sequence",
        ],
        // STRICT tables check each value against its column type instead of
        // applying affinity.
        &[
            "CREATE TABLE s(a INTEGER PRIMARY KEY, b INT, c TEXT, d REAL, e BLOB, f ANY, g INTEGER NOT NULL DEFAULT 0, UNIQUE(b)) STRICT",
            "INSERT INTO s(b, c, d, e, f) VALUES (1, 'x', 1.5, x'00', 'any')",
            "INSERT INTO s(b, c, d, e, f) VALUES ('2', 3, 4, x'01', 5)",
            "INSERT INTO s(b) VALUES ('x')",
            "INSERT INTO s(b, d) VALUES (3, 'abc')",
            "INSERT INTO s(b, c) VALUES (4, x'00')",
            "INSERT INTO s(b, e) VALUES (5, 'text')",
            "INSERT INTO s(b, d) VALUES (6, '7.5')",
            "INSERT INTO s(b, d) VALUES (7, 8)",
            "INSERT INTO s(a, b) VALUES ('9', 9)",
            "INSERT INTO s(a, b) VALUES ('z', 10)",
            "INSERT OR IGNORE INTO s(b) VALUES ('bad'), (11)",
            "SELECT a, b, typeof(b), c, typeof(c), d, typeof(d), e, f, typeof(f), g FROM s ORDER BY a",
            "UPDATE s SET b = '12' WHERE a = 1",
            "UPDATE s SET b = 'nope' WHERE a = 1",
            "UPDATE s SET d = '1e2' WHERE a = 2",
            "UPDATE OR IGNORE s SET c = 5",
            "UPDATE s SET g = NULL WHERE a = 1",
            "INSERT INTO s(b, c) VALUES (1, 'u') ON CONFLICT(b) DO NOTHING",
            "INSERT INTO s(b, c) VALUES (12, 'u') ON CONFLICT(b) DO UPDATE SET d = '3'",
            "INSERT INTO s(b, c) VALUES (12, 'u') ON CONFLICT(b) DO UPDATE SET d = 'q'",
            "SELECT a, b, typeof(b), c, typeof(c), d, typeof(d), e, f, typeof(f), g FROM s ORDER BY a",
            "CREATE TABLE p(x INT PRIMARY KEY, y TEXT) STRICT",
            "INSERT INTO p VALUES (NULL, 'a')",
            "INSERT INTO p VALUES ('1', 2)",
            "SELECT x, typeof(x), y, typeof(y) FROM p",
        ],
        // REPLACE deletes the rows the new row conflicts with.
        &["INSERT OR REPLACE INTO u VALUES (1, 10, 'r', 1)"],
        &["INSERT OR REPLACE INTO u VALUES (10, 2, 'r', 1)"],
        &["INSERT OR REPLACE INTO u VALUES (3, 4, 'r', 1) RETURNING *"],
        &["INSERT OR REPLACE INTO u VALUES (11, 12, 'p', 5)"],
        &["REPLACE INTO u VALUES (1, 1, 'x', 1.0), (12, 1, 'y', 2), (NULL, 3, 'p', 5)"],
        &["INSERT OR REPLACE INTO u(a, b) VALUES (2, NULL)"],
        &[
            "INSERT OR REPLACE INTO u(b, c) VALUES (5, 'yy')",
            "SELECT last_insert_rowid()",
        ],
        &[
            "CREATE TABLE rr(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
             b UNIQUE ON CONFLICT REPLACE, c NOT NULL ON CONFLICT REPLACE DEFAULT 7, \
             d UNIQUE, e NOT NULL ON CONFLICT REPLACE)",
            "CREATE INDEX rr_c ON rr(c)",
            "INSERT INTO rr VALUES (1, 1, NULL, 1, 0), (2, 2, 3, 2, 0)",
            "INSERT INTO rr VALUES (1, 2, NULL, 9, 0)",
            "INSERT INTO rr VALUES (5, 9, 1, 9, 0)",
            "INSERT OR IGNORE INTO rr VALUES (1, 5, 5, 5, 0)",
            "INSERT OR FAIL INTO rr VALUES (6, 6, 6, 6, 0), (1, 7, 7, 7, 0)",
            "INSERT INTO rr VALUES (8, 8, 8, 8, NULL)",
            "SELECT *, typeof(c) FROM rr ORDER BY a",
            "SELECT c FROM rr INDEXED BY rr_c ORDER BY c",
            "PRAGMA integrity_check",
        ],
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
        &["INSERT INTO t(b) VALUES (62)"],
        // Like legacy, a RETURNING subquery that reads the target runs again
        // for each row.
        &["INSERT INTO u(b) VALUES (69), (70) RETURNING (SELECT count(*) FROM u)"],
        // Subqueries in VALUES. Rows that read the target are all computed
        // before any is inserted.
        &["INSERT INTO u(b, c) VALUES (61, (SELECT max(c) FROM u))"],
        &["INSERT INTO u(b, c) VALUES ((SELECT max(b) FROM u) + 1, 'r1'), \
           ((SELECT max(b) FROM u) + 1, 'r2')"],
        &["INSERT INTO u(b, c) VALUES ((SELECT max(b) FROM u) + 1, 'r1'), \
           ((SELECT max(b) FROM u) + 2, (SELECT count(*) FROM u))"],
        &["INSERT INTO u(b, c) VALUES ((SELECT max(a) FROM log) + 70, 'l'), (71, 'm')"],
        // With triggers, the rows are computed before any is inserted too.
        &[
            "CREATE TRIGGER tr AFTER INSERT ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "INSERT INTO u(b, c) VALUES (72, last_insert_rowid()), (73, last_insert_rowid()), \
             (74, (SELECT count(*) FROM log))",
        ],
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

/// Tables with foreign keys for `hir_foreign_keys_match_legacy`, filled
/// with foreign keys off so that some child rows have no parent.
const FK_SETUP: &[&str] = &[
    "CREATE TABLE p(id INTEGER PRIMARY KEY, k TEXT UNIQUE, a INTEGER, b INTEGER, UNIQUE(a, b))",
    "CREATE TABLE c(x INTEGER REFERENCES p(id), y TEXT REFERENCES p(k))",
    "CREATE INDEX c_y ON c(y)",
    "CREATE TABLE r(x INTEGER REFERENCES p(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
    "CREATE TABLE d(x INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED, a, b, \
     FOREIGN KEY(a, b) REFERENCES p(a, b) DEFERRABLE INITIALLY DEFERRED)",
    "CREATE TABLE s(id INTEGER PRIMARY KEY, up INTEGER REFERENCES s(id))",
    "CREATE TABLE q(k TEXT UNIQUE, up INTEGER REFERENCES q(k))",
    "CREATE TABLE rk(k TEXT REFERENCES p(k) ON UPDATE RESTRICT)",
    // ON UPDATE CASCADE, on a parent of its own so the other UPDATEs of `p`
    // check only.
    "CREATE TABLE p2(id INTEGER PRIMARY KEY, k TEXT UNIQUE)",
    "CREATE TABLE ck(k TEXT REFERENCES p2(k) ON UPDATE CASCADE)",
    "INSERT INTO p VALUES (1, 'one', 1, 1), (2, 'two', 2, 2), (3, 'three', 3, 3), \
     (4, 'four', 4, 4), (5, 'five', 5, 5)",
    "INSERT INTO c VALUES (1, NULL), (NULL, 'two'), (9, NULL)",
    "INSERT INTO r VALUES (3)",
    "INSERT INTO d VALUES (4, 4, 4), (8, 8, 8)",
    "INSERT INTO s VALUES (1, 1), (2, 1), (3, 2), (4, NULL)",
    "INSERT INTO q VALUES ('m', 'm'), ('n', 'm')",
    "INSERT INTO rk VALUES ('three')",
    "INSERT INTO p2 VALUES (1, 'one')",
    "INSERT INTO ck VALUES ('one')",
    "PRAGMA foreign_keys = ON",
];

#[test]
fn hir_foreign_keys_match_legacy() {
    for statements in [
        &["DELETE FROM p WHERE id = 5"][..],
        // A new rowid changes a rowid parent key; a row may be its own parent
        // under its new rowid.
        &[
            "CREATE TABLE p(id INTEGER PRIMARY KEY, k)",
            "CREATE TABLE c(x REFERENCES p(id))",
            "CREATE TABLE s(id INTEGER PRIMARY KEY, up REFERENCES s(id))",
            "CREATE TABLE d(x REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
            "INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO c VALUES (1)",
            "INSERT INTO s VALUES (1, NULL), (2, 1)",
            "UPDATE p SET id = 10 WHERE id = 1",
            "UPDATE p SET id = 20 WHERE id = 2",
            "UPDATE p SET rowid = 30 WHERE id = 3",
            "SELECT * FROM p ORDER BY id",
            "UPDATE s SET id = 5 WHERE id = 1",
            "UPDATE s SET id = 6, up = 6 WHERE id = 2",
            "SELECT * FROM s ORDER BY id",
            "BEGIN",
            "INSERT INTO d VALUES (40)",
            "UPDATE p SET id = 40 WHERE id = 20",
            "COMMIT",
            "SELECT * FROM p ORDER BY id",
            "SELECT * FROM d",
        ],
        // A child found by scanning `c`, and one found through `c_y`.
        &["DELETE FROM p WHERE id = 1"],
        &["DELETE FROM p WHERE id = 2"],
        // RESTRICT stops at once.
        &["DELETE FROM p WHERE id = 3"],
        // The whole statement is undone.
        &["DELETE FROM p WHERE id IN (5, 1)"],
        &["DELETE FROM c", "DELETE FROM p WHERE id < 3"],
        // A deferred child is checked at COMMIT.
        &["DELETE FROM p WHERE id = 4"],
        &["BEGIN", "DELETE FROM p WHERE a = 4", "COMMIT", "ROLLBACK"],
        &[
            "BEGIN",
            "DELETE FROM p WHERE id = 4",
            "DELETE FROM d WHERE x = 4",
            "COMMIT",
        ],
        // Deleting a child whose parent is missing takes back its violation.
        &[
            "BEGIN",
            "INSERT INTO d VALUES (7, NULL, NULL)",
            "DELETE FROM d WHERE x = 7",
            "COMMIT",
        ],
        // A row may point at itself.
        &["DELETE FROM s WHERE id = 1"],
        &["DELETE FROM s WHERE id = 4", "DELETE FROM s WHERE id = 3"],
        &["DELETE FROM s"],
        &["DELETE FROM s WHERE id > 1 RETURNING id"],
        // INSERT into a child: the parent must exist.
        &["INSERT INTO c VALUES (2, 'one'), (NULL, NULL), ('1', 'two') RETURNING *"],
        &["INSERT INTO c VALUES (7, NULL)"],
        &["INSERT INTO c VALUES (NULL, 'nope')"],
        &["INSERT INTO c VALUES ('abc', NULL)"],
        &["INSERT INTO c VALUES (1, 'one'), (2, 'nope')"],
        &["INSERT OR IGNORE INTO c VALUES (8, NULL)"],
        &["INSERT INTO r VALUES (6)"],
        &["INSERT INTO d VALUES (6, NULL, NULL)"],
        &["INSERT INTO d VALUES (NULL, 6, 6)"],
        // INSERT into a parent repairs a deferred child.
        &[
            "BEGIN",
            "INSERT INTO d VALUES (6, 6, 6)",
            "INSERT INTO p VALUES (6, 'six', 6, 6)",
            "COMMIT",
        ],
        &["BEGIN", "INSERT INTO d VALUES (6, 6, 6)", "COMMIT"],
        // A new row may be its own parent.
        &["INSERT INTO s VALUES (5, 5), (6, 5), (NULL, NULL)"],
        &["INSERT INTO s VALUES (6, 7)"],
        &["INSERT INTO s VALUES (7, 8), (8, 7)"],
        &["INSERT INTO q VALUES ('a', 'a'), ('b', 'a')"],
        &["INSERT INTO q VALUES (1, '1')"],
        &["INSERT INTO q VALUES ('1', 1)"],
        &["INSERT INTO q VALUES ('x', 'y')"],
        // UPSERT DO UPDATE checks foreign keys like UPDATE.
        &["INSERT INTO p VALUES (2, 'two', 2, 2) ON CONFLICT(id) DO UPDATE SET k = 'TWO'"],
        &["INSERT INTO p VALUES (5, 'x', 5, 5) ON CONFLICT(id) DO UPDATE SET k = 'FIVE'"],
        &["INSERT INTO s VALUES (3, 3) ON CONFLICT(id) DO UPDATE SET up = 9"],
        &["INSERT INTO s VALUES (3, 3) ON CONFLICT(id) DO UPDATE SET up = excluded.up"],
        &["INSERT INTO p2 VALUES (1, 'x') ON CONFLICT(id) DO UPDATE SET k = 'ONE'"],
        // REPLACE checks the foreign keys of the row it deletes.
        &["INSERT OR REPLACE INTO p VALUES (1, 'one', 1, 1)"],
        &["INSERT OR REPLACE INTO p VALUES (1, 'uno', 1, 1)"],
        &["INSERT OR REPLACE INTO p VALUES (6, 'two', 6, 6)"],
        &["INSERT OR REPLACE INTO s VALUES (1, 1)"],
        &[
            "INSERT OR REPLACE INTO d VALUES (4, 4, 4)",
            "INSERT OR REPLACE INTO c VALUES (7, NULL)",
        ],
        // UPDATE of a parent key: children of the old key become violations.
        &["UPDATE p SET k = 'TWO' WHERE id = 2"],
        &["UPDATE p SET k = 'uno' WHERE id = 1 RETURNING *"],
        &["UPDATE p SET k = k WHERE id = 2"],
        &["UPDATE p SET k = 'THREE' WHERE id = 3"],
        &["UPDATE p SET a = 40 WHERE id = 4"],
        &[
            "BEGIN",
            "UPDATE p SET a = 40, b = 40 WHERE id = 4",
            "UPDATE d SET a = 40, b = 40 WHERE x = 4",
            "COMMIT",
        ],
        // A new parent key repairs a deferred child.
        &[
            "BEGIN",
            "INSERT INTO d VALUES (NULL, 7, 7)",
            "UPDATE p SET a = 7, b = 7 WHERE id = 5",
            "COMMIT",
        ],
        &["UPDATE p2 SET k = 'ONE'"],
        // UPDATE of a child key: the new parent must exist.
        &["UPDATE c SET y = 'one' WHERE y = 'two'"],
        &["UPDATE c SET y = 'zzz'"],
        &["UPDATE c SET x = 3 WHERE x = 1"],
        &["UPDATE c SET x = 77 WHERE x = 1"],
        &["UPDATE OR IGNORE c SET x = 78"],
        &[
            "BEGIN",
            "UPDATE d SET x = 3 WHERE x = 8",
            "UPDATE d SET x = 4 WHERE x = 3",
            "COMMIT",
        ],
        &["BEGIN", "UPDATE d SET x = 99 WHERE x = 4", "COMMIT"],
        // A row may be its own parent.
        &["UPDATE s SET up = 4 WHERE id = 3"],
        &["UPDATE s SET up = 9"],
        &["UPDATE s SET up = id"],
        &["UPDATE q SET k = 'z' WHERE k = 'm'"],
        &["UPDATE q SET k = 'z', up = 'z' WHERE k = 'm'"],
        &["UPDATE q SET k = 'z', up = 'z' WHERE k = 'n'"],
        &["UPDATE q SET up = 'n' WHERE k = 'n'"],
        &["UPDATE q SET up = 'nope' WHERE k = 'n'"],
        // UPDATE OR REPLACE checks the foreign keys of the rows it deletes.
        &["UPDATE OR REPLACE p SET k = 'one' WHERE id = 2"],
        &["UPDATE OR REPLACE p SET k = 'one' WHERE id = 5"],
        &[
            "BEGIN",
            "UPDATE OR REPLACE p SET a = 4, b = 4 WHERE id = 5",
            "COMMIT",
        ],
        &["UPDATE OR REPLACE q SET k = 'n' WHERE k = 'm'"],
    ] {
        let statements = FK_SETUP
            .iter()
            .chain(statements)
            .chain(&[
                "SELECT * FROM p ORDER BY id",
                "SELECT * FROM c ORDER BY rowid",
                "SELECT * FROM r ORDER BY rowid",
                "SELECT * FROM d ORDER BY rowid",
                "SELECT * FROM s ORDER BY id",
                "SELECT k, up, typeof(k), typeof(up) FROM q ORDER BY rowid",
                "SELECT * FROM rk ORDER BY rowid",
                "SELECT * FROM ck ORDER BY rowid",
                "PRAGMA foreign_key_check",
            ])
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            run(true, &statements),
            run(false, &statements),
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

/// Subqueries in an UPDATE's SET and WHERE see the table as SQLite's do.
#[test]
fn hir_update_subqueries_match_sqlite() {
    let setup = [
        "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d)",
        "INSERT INTO u VALUES (1, 1, 'x', 1.0), (2, 2, 'yy', -1), (3, 3, 'z', 3), \
         (4, 4, NULL, NULL), (5, 5, 'p', 5), (6, 6, 'q', 5)",
    ];
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.set_experimental_hir(true);
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        conn.execute(sql).unwrap();
        sqlite.execute(sql, ()).unwrap();
    }
    for sql in [
        "UPDATE u SET c = (SELECT max(c) FROM u) WHERE a = 1",
        "UPDATE u SET d = (SELECT max(x.d) FROM u AS x WHERE x.a < u.a)",
        "UPDATE u SET d = (SELECT sum(x.d) FROM u AS x WHERE x.a <= u.a) WHERE a > 2",
        "UPDATE u SET b = (SELECT max(b) FROM u) + a",
        "UPDATE u SET b = b + 10 WHERE (SELECT count(*) FROM u AS x WHERE x.b > u.b) > 2",
        "UPDATE u SET d = d + 1 WHERE b < (SELECT max(b) FROM u)",
        // One subquery row assigned to several columns runs once per row.
        "UPDATE u SET (c, d) = (SELECT 'm', a FROM u AS x WHERE x.a = u.a + 1) WHERE a > 4",
        "UPDATE u SET (c, b) = (SELECT 'k', 100), d = 7 WHERE a = 1",
        "UPDATE u SET (b, c) = (SELECT max(b) + 1, count(*) FROM u) WHERE a = 2",
    ] {
        conn.execute(sql).unwrap();
        sqlite.execute(sql, ()).unwrap();
        let select = "SELECT * FROM u ORDER BY a";
        assert_eq!(
            limbo_exec_rows(&conn, select),
            sqlite_exec_rows(&sqlite, select),
            "{sql}"
        );
    }
}

/// Subqueries in INSERT VALUES see the table as SQLite's do: each VALUES
/// row is computed after the rows before it are written.
#[test]
fn hir_insert_values_subqueries_match_sqlite() {
    let setup = [
        "CREATE TABLE v(a INTEGER PRIMARY KEY, b UNIQUE, c)",
        "INSERT INTO v VALUES (1, 1, 'x'), (2, 2, 'y')",
        "CREATE TABLE w(x)",
        "INSERT INTO w VALUES (10), (20)",
    ];
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.set_experimental_hir(true);
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        conn.execute(sql).unwrap();
        sqlite.execute(sql, ()).unwrap();
    }
    for sql in [
        "INSERT INTO v(b, c) VALUES ((SELECT max(b) FROM v) + 1, 'one')",
        "INSERT INTO v(b, c) VALUES ((SELECT max(b) FROM v) + 1, 'r1'), \
         ((SELECT max(b) FROM v) + 1, 'r2'), ((SELECT count(*) FROM v) * 10, 'r3')",
        "INSERT INTO v(b, c) VALUES ((SELECT max(x) FROM w), (SELECT group_concat(c) FROM v))",
        "INSERT INTO v(b, c) VALUES (100, 'p'), ((SELECT b FROM v WHERE c = 'p') + 1, 'q')",
        "INSERT INTO v(b, c) VALUES ((SELECT 1 WHERE 0), (SELECT c FROM v WHERE a = 1))",
        "INSERT INTO v(b) VALUES ((SELECT b FROM v WHERE a = 1))",
    ] {
        let turso = conn.execute(sql).map_err(|e| e.to_string());
        let reference = sqlite.execute(sql, ()).map_err(|e| e.to_string());
        assert_eq!(
            turso.is_ok(),
            reference.is_ok(),
            "{sql}: {turso:?} {reference:?}"
        );
        let select = "SELECT * FROM v ORDER BY a";
        assert_eq!(
            limbo_exec_rows(&conn, select),
            sqlite_exec_rows(&sqlite, select),
            "{sql}"
        );
    }
}

/// Like SQLite, every subquery in RETURNING runs again for each row, so it
/// sees the rows written so far.
#[test]
fn hir_returning_subqueries_match_sqlite() {
    let setup = [
        "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d)",
        "CREATE TABLE log(x)",
        "CREATE TRIGGER tr AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (new.a); END",
    ];
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.set_experimental_hir(true);
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        conn.execute(sql).unwrap();
        sqlite.execute(sql, ()).unwrap();
    }
    for sql in [
        "INSERT INTO u VALUES (1, 1, 'x', 1.0), (2, 2, 'yy', -1), (3, 3, 'z', 3), \
         (4, 4, NULL, NULL), (5, 5, 'p', 5), (6, 6, 'q', 5) \
         RETURNING a, (SELECT count(*) FROM u), (SELECT max(b) FROM u WHERE b < 4)",
        "UPDATE u SET d = 0 WHERE a < 3 RETURNING a, (SELECT sum(d) FROM u), \
         (SELECT count(*) FROM u AS x WHERE x.d = u.d), (SELECT count(*) FROM log)",
        "INSERT INTO u(b) VALUES (10), (11) RETURNING (SELECT group_concat(b) FROM u WHERE b > 5), \
         (SELECT count(*) FROM (SELECT * FROM u))",
        "UPDATE u SET c = 'k' WHERE a = 4 RETURNING EXISTS (SELECT 1 FROM u WHERE c = 'k'), \
         (SELECT a FROM u WHERE c = 'k') IN (SELECT a FROM u)",
        "INSERT INTO u(b) VALUES (12), (13) RETURNING \
         (SELECT count(*) FROM log WHERE x IN (SELECT a FROM u)), \
         (SELECT count(*) FROM log UNION ALL SELECT count(*) FROM u LIMIT 1 OFFSET 1), \
         (WITH w AS (SELECT b FROM u) SELECT max(b) FROM w), \
         (SELECT max(x.b) FROM u AS x WHERE x.b < u.b)",
        "UPDATE u SET b = b + 100 WHERE a > 7 RETURNING b, (SELECT max(b) FROM u), \
         (SELECT count(*) FROM log)",
        "DELETE FROM u WHERE a < 3 RETURNING a, (SELECT count(*) FROM u), \
         (SELECT group_concat(a) FROM u WHERE a < 5), (SELECT count(*) FROM log)",
        "DELETE FROM u WHERE b > 10 RETURNING b, (SELECT max(b) FROM u), \
         EXISTS (SELECT 1 FROM u AS x WHERE x.b = u.b)",
        "SELECT * FROM u ORDER BY a",
        "SELECT * FROM log",
    ] {
        assert_eq!(
            limbo_exec_rows(&conn, sql),
            sqlite_exec_rows(&sqlite, sql),
            "{sql}"
        );
    }
}

/// Virtual generated columns: computed for each new row from its other
/// columns, indexed, checked and returned as SQLite does, and not stored.
/// Legacy reports a NOT NULL failure for the UNIQUE conflict on `e` below.
#[test]
fn hir_generated_columns_match_sqlite() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_generated_columns(true))
        .build();
    let conn = db.connect_limbo();
    conn.set_experimental_hir(true);
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE g(a INTEGER PRIMARY KEY, b, c AS (b * 2), d TEXT, e AS (c + length(d)) NOT NULL, f AS (a + 100), CHECK (c < 100))",
        "CREATE INDEX g_c ON g(c)",
        "CREATE UNIQUE INDEX g_e ON g(e)",
        "CREATE INDEX g_x ON g(f * 2)",
        "CREATE TABLE log(x, y)",
        "INSERT INTO g(b, d) VALUES (1, 'x'), (2, 'yy')",
        "INSERT INTO g(a, b, d) VALUES (10, 3, 'zzz')",
        "SELECT * FROM g ORDER BY a",
        "INSERT INTO g(b, d) VALUES (60, 'x')",
        "INSERT INTO g(b, d) VALUES (1, NULL)",
        "INSERT INTO g(b, d) VALUES (2, 'x')",
        "INSERT OR IGNORE INTO g(b, d) VALUES (2, 'x'), (5, 'k')",
        "INSERT INTO g(c) VALUES (1)",
        "SELECT * FROM g ORDER BY a",
        "UPDATE g SET b = b + 1",
        "SELECT * FROM g ORDER BY a",
        "UPDATE g SET d = 'long' WHERE a = 1",
        "UPDATE g SET b = 70 WHERE a = 1",
        "UPDATE g SET c = 1",
        "UPDATE g SET a = a + 1000 WHERE a = 10",
        "SELECT * FROM g ORDER BY a",
        "SELECT c, a FROM g INDEXED BY g_c ORDER BY c",
        "SELECT e, a FROM g INDEXED BY g_e ORDER BY e",
        "CREATE TRIGGER tb BEFORE UPDATE ON g BEGIN INSERT INTO log VALUES (old.c, new.c); END",
        "CREATE TRIGGER ta AFTER INSERT ON g BEGIN INSERT INTO log VALUES (new.c, new.e); END",
        "UPDATE g SET b = 0 WHERE a = 2",
        "INSERT INTO g(b, d) VALUES (7, 'q') RETURNING a, c, e, f",
        "INSERT INTO g(a, b, d) VALUES (2, 9, 'w') ON CONFLICT(a) DO UPDATE SET b = excluded.b RETURNING *",
        "SELECT * FROM log",
        "SELECT * FROM g ORDER BY a",
        "DELETE FROM g WHERE c > 10",
        "SELECT * FROM g ORDER BY a",
        "PRAGMA integrity_check",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// Foreign-key actions: ON DELETE and ON UPDATE CASCADE, SET NULL and SET
/// DEFAULT change the child rows, through triggers and through a table that
/// references itself. HIR matches SQLite where legacy does not: a DELETE
/// whose SET DEFAULT leaves a child pointing at a missing parent fails.
#[test]
fn hir_foreign_key_actions_match_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.set_experimental_hir(true);
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE p(id INTEGER PRIMARY KEY, k TEXT UNIQUE)",
        "CREATE TABLE c1(x REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE, v)",
        "CREATE TABLE c2(y TEXT DEFAULT 'two' REFERENCES p(k) ON DELETE SET DEFAULT ON UPDATE SET NULL)",
        "CREATE TABLE c3(z REFERENCES p(id) ON DELETE SET NULL)",
        "CREATE TABLE tree(id INTEGER PRIMARY KEY, up REFERENCES tree(id) ON DELETE CASCADE ON UPDATE CASCADE)",
        "CREATE TABLE log(m)",
        "CREATE TRIGGER tc AFTER DELETE ON c1 BEGIN INSERT INTO log VALUES ('c1 ' || old.x); END",
        "INSERT INTO p VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        "INSERT INTO c1 VALUES (1, 'a'), (1, 'b'), (2, 'c'), (NULL, 'd')",
        "INSERT INTO c2 VALUES ('one'), ('three'), (NULL)",
        "INSERT INTO c3 VALUES (1), (3)",
        "INSERT INTO tree VALUES (1, NULL), (2, 1), (3, 2), (4, 1), (5, NULL)",
        "DELETE FROM p WHERE id = 1",
        "SELECT * FROM c1 ORDER BY v",
        "SELECT * FROM c2 ORDER BY y",
        "SELECT * FROM c3 ORDER BY z",
        "SELECT * FROM log",
        "UPDATE p SET id = 20 WHERE id = 2",
        "SELECT * FROM c1 ORDER BY v",
        "UPDATE p SET k = 'tres' WHERE k = 'three'",
        "SELECT * FROM c2 ORDER BY y",
        "DELETE FROM tree WHERE id = 1",
        "SELECT * FROM tree ORDER BY id",
        "INSERT INTO tree VALUES (6, 5), (7, 6)",
        "UPDATE tree SET id = 50 WHERE id = 5",
        "SELECT * FROM tree ORDER BY id",
        "DELETE FROM p WHERE id = 20 RETURNING id, (SELECT count(*) FROM c1)",
        "SELECT * FROM c1 ORDER BY v",
        "DELETE FROM p",
        "SELECT * FROM c2 ORDER BY y",
        "PRAGMA foreign_key_check",
        // Two tables whose CASCADEs reach each other, a key of two columns
        // whose SET DEFAULT points at a missing parent (legacy lets this
        // last DELETE through), actions fired from a trigger, and a deferred
        // SET NULL.
        "CREATE TABLE a(id INTEGER PRIMARY KEY, b_id REFERENCES b(id) ON DELETE CASCADE)",
        "CREATE TABLE b(id INTEGER PRIMARY KEY, a_id REFERENCES a(id) ON DELETE CASCADE)",
        "INSERT INTO a VALUES (1, NULL), (2, NULL)",
        "INSERT INTO b VALUES (10, 1), (20, 2)",
        "UPDATE a SET b_id = 20 WHERE id = 1",
        "DELETE FROM a WHERE id = 2",
        "SELECT * FROM a ORDER BY id",
        "SELECT * FROM b ORDER BY id",
        "CREATE TABLE mp(x, y, PRIMARY KEY (x, y))",
        "CREATE TABLE mc(u DEFAULT 9, w DEFAULT 9, FOREIGN KEY (u, w) REFERENCES mp(x, y) ON DELETE SET DEFAULT ON UPDATE CASCADE)",
        "INSERT INTO mp VALUES (1, 1), (2, 2), (9, 9)",
        "INSERT INTO mc VALUES (1, 1), (2, 2)",
        "UPDATE mp SET y = 3 WHERE x = 2",
        "SELECT * FROM mc ORDER BY u",
        "DELETE FROM mp WHERE x = 1",
        "SELECT * FROM mc ORDER BY u",
        "DELETE FROM mp WHERE x = 9",
        "SELECT * FROM mp ORDER BY x",
        "CREATE TABLE tp(id INTEGER PRIMARY KEY)",
        "CREATE TABLE tc(p REFERENCES tp(id) ON DELETE CASCADE)",
        "CREATE TABLE gone(id)",
        "CREATE TRIGGER tg AFTER INSERT ON gone BEGIN DELETE FROM tp WHERE id = new.id; END",
        "INSERT INTO tp VALUES (1), (2)",
        "INSERT INTO tc VALUES (1), (2), (2)",
        "INSERT INTO gone VALUES (2)",
        "SELECT * FROM tc ORDER BY p",
        "CREATE TABLE dp(id INTEGER PRIMARY KEY)",
        "CREATE TABLE dc(p REFERENCES dp(id) ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED)",
        "INSERT INTO dp VALUES (1)",
        "INSERT INTO dc VALUES (1)",
        "BEGIN",
        "UPDATE dp SET id = 2",
        "COMMIT",
        "SELECT * FROM dc",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// An UPSERT takes over conflicts on the constraints it names; any other
/// conflict resolves as without it, by the statement's OR clause or the
/// constraint's own ON CONFLICT, as in SQLite. Legacy drops the UPSERT under
/// OR IGNORE and stops the statement on the other conflicts.
#[test]
fn hir_upsert_other_conflicts_match_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.set_experimental_hir(true);
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE uo(a INTEGER PRIMARY KEY, b UNIQUE, c)",
        "INSERT INTO uo VALUES (1, 1, 'x')",
        "INSERT OR IGNORE INTO uo VALUES (1, 2, 'y') ON CONFLICT(a) DO UPDATE SET c = 'up'",
        "SELECT * FROM uo",
        "INSERT OR IGNORE INTO uo VALUES (2, 1, 'z') ON CONFLICT(a) DO UPDATE SET c = 'up2'",
        "SELECT * FROM uo",
        "INSERT OR REPLACE INTO uo VALUES (5, 1, 'r') ON CONFLICT(a) DO UPDATE SET c = 'up3'",
        "SELECT * FROM uo",
        "CREATE TABLE uc(a INTEGER PRIMARY KEY, b UNIQUE ON CONFLICT REPLACE, c UNIQUE ON CONFLICT IGNORE, d)",
        "INSERT INTO uc VALUES (1, 1, 1, 'x'), (2, 2, 2, 'y')",
        "INSERT INTO uc VALUES (3, 1, 9, 'z') ON CONFLICT(a) DO UPDATE SET d = 'up'",
        "SELECT * FROM uc ORDER BY a",
        "INSERT INTO uc VALUES (4, 8, 2, 'w') ON CONFLICT(a) DO UPDATE SET d = 'up'",
        "SELECT * FROM uc ORDER BY a",
        "INSERT INTO uc VALUES (3, 2, 7, 'v') ON CONFLICT(a) DO UPDATE SET d = 'up3'",
        "SELECT * FROM uc ORDER BY a",
        "INSERT OR FAIL INTO uc VALUES (10, 2, 70, 'f') ON CONFLICT(a) DO NOTHING",
        "SELECT * FROM uc ORDER BY a",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// An UPSERT's DO UPDATE fires the UPDATE triggers of the columns it sets,
/// with the conflicting row as OLD: BEFORE once the new row passed its CHECK
/// constraints, AFTER once RETURNING has read the row, as in SQLite.
#[test]
fn hir_upsert_triggers_match_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.set_experimental_hir(true);
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b UNIQUE, c, n DEFAULT 0)",
        "CREATE TABLE log(m)",
        "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN INSERT INTO log VALUES ('before ' || old.c || '>' || new.c); END",
        "CREATE TRIGGER au AFTER UPDATE OF c ON t BEGIN INSERT INTO log VALUES ('after ' || old.c || '>' || new.c || ' ' || (SELECT count(*) FROM log)); END",
        "CREATE TRIGGER an AFTER UPDATE OF n ON t BEGIN INSERT INTO log VALUES ('n ' || new.n); END",
        "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('insert ' || new.a); END",
        "INSERT INTO t(a, b, c) VALUES (1, 1, 'x')",
        "INSERT INTO t(a, b, c) VALUES (1, 2, 'y') ON CONFLICT(a) DO UPDATE SET c = excluded.c",
        "INSERT INTO t(a, b, c) VALUES (2, 1, 'z') ON CONFLICT(b) DO UPDATE SET n = n + 1 RETURNING a, n, (SELECT count(*) FROM log)",
        "INSERT INTO t(a, b, c) VALUES (3, 3, 'w') ON CONFLICT DO UPDATE SET c = 'never'",
        "CREATE TRIGGER ig BEFORE UPDATE ON t WHEN new.c = 'skip' BEGIN SELECT RAISE(IGNORE); END",
        "INSERT INTO t(a, b, c) VALUES (1, 9, 'skip') ON CONFLICT(a) DO UPDATE SET c = excluded.c",
        "CREATE TRIGGER del BEFORE UPDATE ON t WHEN new.c = 'gone' BEGIN DELETE FROM t WHERE a = old.a; END",
        "INSERT INTO t(a, b, c) VALUES (3, 9, 'gone') ON CONFLICT(a) DO UPDATE SET c = excluded.c",
        "SELECT * FROM t ORDER BY a",
        "SELECT * FROM log",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// `ON CONFLICT(rowid)` names the rowid of any rowid table, as in SQLite.
/// Legacy rejects it.
#[test]
fn hir_upsert_on_rowid_matches_sqlite() {
    let statements = [
        "CREATE TABLE a1(x INTEGER PRIMARY KEY, y)",
        "CREATE TABLE a2(x, y)",
        "INSERT INTO a1 VALUES (1, 1)",
        "INSERT INTO a2(rowid, x, y) VALUES (1, 1, 1)",
        "INSERT INTO a1 VALUES (1, 2) ON CONFLICT(rowid) DO UPDATE SET y = 10",
        "INSERT INTO a2(rowid, x, y) VALUES (1, 1, 2) ON CONFLICT(rowid) DO UPDATE SET y = 20",
        "INSERT INTO a2(rowid, x, y) VALUES (2, 1, 2) ON CONFLICT(_rowid_) DO NOTHING",
    ];
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.set_experimental_hir(true);
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in statements {
        conn.execute(sql).unwrap();
        sqlite.execute(sql, ()).unwrap();
    }
    for sql in ["SELECT * FROM a1", "SELECT rowid, * FROM a2 ORDER BY rowid"] {
        assert_eq!(
            limbo_exec_rows(&conn, sql),
            sqlite_exec_rows(&sqlite, sql),
            "{sql}"
        );
    }
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
        "UPDATE sqlite_schema SET name = 'x'",
        "INSERT INTO sqlite_master VALUES (1, 2, 3, 4, 5)",
        "INSERT INTO t VALUES (1, 2) ON CONFLICT(b) DO NOTHING",
        "INSERT INTO t VALUES (1, 2) ON CONFLICT(a NULLS FIRST) DO NOTHING",
    ] {
        let legacy_error = legacy
            .prepare(sql)
            .err()
            .unwrap_or_else(|| panic!("legacy rejects {sql}"))
            .to_string();
        let hir_error = hir.prepare(sql).err().expect("HIR rejects").to_string();
        assert_eq!(hir_error, legacy_error, "{sql}");
    }
    // Legacy differs from SQLite on these: it reports a missing constraint
    // for an unknown column and accepts COLLATE on the INTEGER PRIMARY KEY.
    // HIR follows SQLite.
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b)", ())
        .unwrap();
    for (sql, message) in [
        (
            "INSERT INTO t VALUES (1, 2) ON CONFLICT(nosuch) DO NOTHING",
            "no such column: nosuch",
        ),
        (
            "INSERT INTO t VALUES (1, 2) ON CONFLICT(a COLLATE nocase) DO NOTHING",
            "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint",
        ),
    ] {
        let sqlite_error = sqlite
            .prepare(sql)
            .err()
            .expect("SQLite rejects")
            .to_string();
        let hir_error = hir.prepare(sql).err().expect("HIR rejects").to_string();
        assert!(
            sqlite_error.contains(message),
            "{sql}: SQLite {sqlite_error}"
        );
        assert!(hir_error.contains(message), "{sql}: HIR {hir_error}");
    }
}

/// Run `sql` on Turso through HIR and on rusqlite's SQLite, and check that
/// both return the same rows, or both fail with SQLite's message (in any
/// case).
fn assert_matches_sqlite(
    db: &TempDatabase,
    conn: &std::sync::Arc<turso_core::Connection>,
    sqlite: &rusqlite::Connection,
    sql: &str,
) {
    let turso = limbo_exec_rows_fallible(db, conn, sql).map_err(|e| e.to_string());
    let reference = sqlite
        .prepare(sql)
        .and_then(|mut statement| {
            let mut rows = statement.query(())?;
            let mut values = Vec::new();
            while let Some(row) = rows.next()? {
                let mut value = Vec::new();
                for i in 0..row.as_ref().column_count() {
                    value.push(row.get::<_, rusqlite::types::Value>(i)?);
                }
                values.push(value);
            }
            Ok(values)
        })
        .map_err(|e| e.to_string());
    match (&turso, &reference) {
        (Ok(turso), Ok(reference)) => assert_eq!(turso, reference, "{sql}"),
        // Turso words some errors differently, e.g. "immediate foreign key
        // constraint failed" for "FOREIGN KEY constraint failed".
        (Err(turso), Err(reference)) => assert!(
            turso.to_lowercase().contains(&reference.to_lowercase()),
            "{sql}: {turso} vs {reference}"
        ),
        _ => panic!("{sql}: {turso:?} vs {reference:?}"),
    }
}
