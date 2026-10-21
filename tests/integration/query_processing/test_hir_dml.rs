//! DML prepared through semantic HIR must change the database exactly as
//! SQLite does. Each case runs on a fresh database and on SQLite, and
//! compares what each statement returned and what is left behind.

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

/// A fresh database with the test tables, and a connection to it.
fn connect() -> (TempDatabase, std::sync::Arc<turso_core::Connection>) {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_generated_columns(true))
        .build();
    let conn = db.connect_limbo();
    for statement in SETUP {
        conn.execute(statement).unwrap();
    }
    (db, conn)
}

/// SQLite with the test tables, set up like a Turso connection: the bundled
/// SQLite enforces foreign keys by default and Turso does not, so a run
/// turns them on itself when it wants them.
fn connect_sqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
    // Turso rejects double-quoted strings in DML by default.
    conn.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DML, false)
        .unwrap();
    for statement in SETUP {
        conn.execute_batch(statement).unwrap();
    }
    conn
}

/// The EXPLAIN QUERY PLAN rows of `sql`, without their ids.
fn query_plan(sql: &str) -> Vec<String> {
    let (_db, conn) = connect();
    limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {sql}"))
        .into_iter()
        .map(|row| match row.last() {
            Some(rusqlite::types::Value::Text(detail)) => detail.clone(),
            other => panic!("EXPLAIN QUERY PLAN detail is not text: {other:?}"),
        })
        .collect()
}

/// What a run returns per statement: its rows, or that it failed. SQLite
/// and Turso word their errors differently, so only the failure counts.
type Outcome = Result<Rows, ()>;

/// Run `statements` on a fresh database, then return what each one returned
/// or whether it failed, every row left in the tables, and the integrity
/// check.
fn run(statements: &[&str]) -> Vec<Outcome> {
    let (db, conn) = connect();
    run_each(statements, |sql| {
        limbo_exec_rows_fallible(&db, &conn, sql).map_err(|_| ())
    })
}

/// The outcomes of `statements` on one engine, in a form SQLite's run can be
/// compared with: `changes()` after each data change (SQLite leaves it
/// untouched after other statements, Turso resets it), rows in any order
/// unless the statement orders them, then the final state.
fn run_each(statements: &[&str], mut execute: impl FnMut(&str) -> Outcome) -> Vec<Outcome> {
    let mut results = Vec::new();
    for statement in statements {
        let rows = execute(statement);
        results.push(if statement.contains("ORDER BY") {
            rows
        } else {
            rows.map(sorted)
        });
        if is_data_change(statement) {
            results.push(execute("SELECT changes()"));
        }
    }
    for sql in FINAL_STATE {
        results.push(execute(sql));
    }
    results
}

fn is_data_change(statement: &str) -> bool {
    let first_word = statement
        .trim_start()
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(
        first_word.as_str(),
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "WITH"
    )
}

fn sorted(mut rows: Rows) -> Rows {
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

/// What is read back after a run.
const FINAL_STATE: &[&str] = &[
    "SELECT a, b, c, d, g FROM t ORDER BY a",
    "SELECT a, b, c, d FROM u ORDER BY a",
    "SELECT a FROM log ORDER BY a",
    "PRAGMA integrity_check",
];

/// Like [`run`], with each failure's message, for runs SQLite cannot mirror.
fn run_with_errors(statements: &[&str]) -> Vec<Result<Rows, String>> {
    let (db, conn) = connect();
    let mut results = Vec::new();
    for statement in statements {
        results.push(limbo_exec_rows_fallible(&db, &conn, statement).map_err(|e| e.to_string()));
        results.push(Ok(limbo_exec_rows(&conn, "SELECT changes()")));
    }
    for sql in FINAL_STATE {
        results.push(Ok(limbo_exec_rows(&conn, sql)));
    }
    results
}

/// The same run on SQLite.
fn run_sqlite(statements: &[&str]) -> Vec<Outcome> {
    let conn = connect_sqlite();
    run_each(statements, |sql| {
        sqlite_exec_rows_fallible(&conn, sql).map_err(|_| ())
    })
}

/// The rows of `sql` on SQLite, or its error.
fn sqlite_exec_rows_fallible(
    conn: &rusqlite::Connection,
    sql: &str,
) -> Result<Rows, rusqlite::Error> {
    let mut stmt = conn.prepare(sql)?;
    let columns = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            (0..columns).map(|column| row.get(column)).collect()
        })?
        .collect::<Result<Vec<Vec<rusqlite::types::Value>>, _>>()?;
    Ok(rows)
}

/// `statements` leave the same results and tables behind on HIR and SQLite.
fn assert_run_matches_sqlite(statements: &[&str]) {
    assert_eq!(
        run(statements),
        run_sqlite(statements),
        "results differ from SQLite for {statements:?}"
    );
}

#[test]
fn hir_delete_matches_sqlite() {
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
        assert_run_matches_sqlite(statements);
    }
}

#[test]
fn hir_update_matches_sqlite() {
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
        assert_run_matches_sqlite(statements);
    }
}

#[test]
fn hir_insert_matches_sqlite() {
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
            "INSERT INTO u(b) SELECT a + 50 FROM t ORDER BY a",
        ],
        &[
            "INSERT INTO u(a, b) SELECT a, b + 1000 FROM t WHERE true \
             ON CONFLICT(a) DO UPDATE SET c = 'from t'",
        ],
        &["INSERT OR REPLACE INTO u(a, b, c) SELECT a, a, 'r' FROM t"],
        &["PRAGMA count_changes = 1", "INSERT INTO u(b) SELECT a + 60 FROM t ORDER BY a"],
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
        assert_run_matches_sqlite(statements);
    }

    // Like legacy, a double-quoted name is a string only as a whole value
    // of a single VALUES row. SQLite accepts or rejects every double-quoted
    // string together, so these are checked on their own.
    assert_eq!(
        run(&[
            "INSERT INTO u(b, c) VALUES (51, \"s\")",
            "SELECT b, c FROM u WHERE b = 51",
        ])[2],
        Ok(vec![vec![
            rusqlite::types::Value::Integer(51),
            rusqlite::types::Value::Text("s".to_string()),
        ]])
    );
    assert!(run(&["INSERT INTO u(b, c) VALUES (52, upper(\"s\"))"])[0].is_err());
    // A VALUES list of several rows takes every double-quoted name as a
    // string, wherever it is.
    assert_eq!(
        run(&[
            "INSERT INTO u(b, c) VALUES (53, upper(\"s\")), (54, \"t\")",
            "SELECT b, c FROM u WHERE b >= 53 ORDER BY b",
        ])[2],
        Ok(vec![
            vec![
                rusqlite::types::Value::Integer(53),
                rusqlite::types::Value::Text("S".to_string()),
            ],
            vec![
                rusqlite::types::Value::Integer(54),
                rusqlite::types::Value::Text("t".to_string()),
            ],
        ])
    );
}

/// Tables with foreign keys for `hir_foreign_keys_match_sqlite`, filled
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
fn hir_foreign_keys_match_sqlite() {
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
            ])
            .copied()
            .collect::<Vec<_>>();
        assert_run_matches_sqlite(&statements);
    }
}

#[test]
fn hir_delete_returning_names_columns() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE r(a INTEGER PRIMARY KEY, b TEXT, c REAL)")
        .unwrap();
    for (sql, expected) in [
        (
            "DELETE FROM r RETURNING *",
            vec![
                ("a".to_string(), Some("INTEGER".to_string())),
                ("b".to_string(), Some("TEXT".to_string())),
                ("c".to_string(), Some("REAL".to_string())),
            ],
        ),
        (
            // Like SQLite, `rowid` takes the alias column's name and type.
            "DELETE FROM r AS x RETURNING x.a, b AS bee, c + 1, rowid",
            vec![
                ("a".to_string(), Some("INTEGER".to_string())),
                ("bee".to_string(), Some("TEXT".to_string())),
                ("c + 1".to_string(), None),
                ("a".to_string(), Some("INTEGER".to_string())),
            ],
        ),
    ] {
        let stmt = conn.prepare(sql).unwrap();
        let metadata = (0..stmt.num_columns())
            .map(|column| {
                (
                    stmt.get_column_name(column).to_string(),
                    stmt.get_column_decltype(column),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(metadata, expected, "{sql}");
    }
}

#[test]
fn hir_delete_returning_commits_when_reset_after_the_first_row() {
    let run = || {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE r(a INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO r VALUES (1), (2), (3)").unwrap();
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
    assert_eq!(run(), [[rusqlite::types::Value::Integer(1)]]);
}

#[test]
fn hir_delete_binds_parameters() {
    let run = || {
        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE p(a INTEGER PRIMARY KEY, b TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX p_b ON p(b)").unwrap();
        conn.execute("INSERT INTO p VALUES (1, 'x'), (2, 'y'), (3, 'x'), (4, 'z')")
            .unwrap();
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
    assert_eq!(
        run(),
        [[
            rusqlite::types::Value::Integer(2),
            rusqlite::types::Value::Text("y".into()),
        ]]
    );
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
        // A STRICT table checks the types of its stored columns only.
        "CREATE TABLE sg(a INT PRIMARY KEY, b INT AS (a || 'x'), c TEXT UNIQUE, d ANY AS (a * 2)) STRICT",
        "INSERT INTO sg(a, c) VALUES (1, 'q'), ('2', 3)",
        "INSERT INTO sg(a, c) VALUES ('x', 'r')",
        "UPDATE sg SET a = '7' WHERE a = 1",
        "UPDATE sg SET a = 'zz' WHERE a = 2",
        "INSERT INTO sg(a, c) VALUES (11, 'q') ON CONFLICT(c) DO UPDATE SET a = excluded.a, c = 12",
        "INSERT INTO sg(a, c) VALUES (13, '12') ON CONFLICT(c) DO UPDATE SET a = 'bad'",
        "SELECT a, typeof(a), b, typeof(b), c, typeof(c), d FROM sg ORDER BY a",
        // A DELETE whose RETURNING has a subquery reads the virtual columns
        // of the deleted row. Legacy returns another column's value.
        "CREATE TABLE dg(a INTEGER PRIMARY KEY, b, c AS (b * 2), d TEXT, e AS (d || c))",
        "INSERT INTO dg(a, b, d) VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 3, 'z')",
        "DELETE FROM dg WHERE a = 2 RETURNING a, c, e, (SELECT count(*) FROM dg), (SELECT max(c) FROM dg)",
        "DELETE FROM dg WHERE a > 0 RETURNING e, (SELECT group_concat(e) FROM dg)",
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
        // Rows that REPLACE deletes fire their ON DELETE actions, whether
        // the statement, the constraint or a rowid change asks for REPLACE.
        "CREATE TABLE rp(id INTEGER PRIMARY KEY, k UNIQUE)",
        "CREATE TABLE rc(x REFERENCES rp(id) ON DELETE CASCADE)",
        "CREATE TABLE rd(y REFERENCES rp(k) ON DELETE SET NULL)",
        "INSERT INTO rp VALUES (1, 'a'), (2, 'b')",
        "INSERT INTO rc VALUES (1), (2)",
        "INSERT INTO rd VALUES ('a'), ('b')",
        "INSERT OR REPLACE INTO rp VALUES (3, 'a')",
        "SELECT * FROM rc ORDER BY x",
        "SELECT * FROM rd ORDER BY y",
        "REPLACE INTO rp VALUES (2, 'z')",
        "SELECT * FROM rc ORDER BY x",
        "UPDATE OR REPLACE rp SET k = 'z' WHERE id = 3",
        "SELECT * FROM rp ORDER BY id",
        "SELECT * FROM rd ORDER BY y",
        "INSERT INTO rc VALUES (3)",
        "UPDATE OR REPLACE rp SET id = 3 WHERE id = 2",
        "SELECT * FROM rc ORDER BY x",
        "CREATE TABLE rq(id INTEGER PRIMARY KEY, v UNIQUE ON CONFLICT REPLACE)",
        "CREATE TABLE re(z REFERENCES rq(v) ON DELETE CASCADE)",
        "INSERT INTO rq VALUES (1, 'v')",
        "INSERT INTO re VALUES ('v')",
        "INSERT INTO rq VALUES (2, 'v')",
        "SELECT * FROM re",
        "CREATE TABLE rs(id INTEGER PRIMARY KEY, up REFERENCES rs(id) ON DELETE CASCADE)",
        "INSERT INTO rs VALUES (1, NULL), (2, 1), (3, 2)",
        "REPLACE INTO rs VALUES (1, NULL)",
        "SELECT * FROM rs ORDER BY id",
        "CREATE TRIGGER rt AFTER INSERT ON log BEGIN REPLACE INTO rq VALUES (3, new.m); END",
        "INSERT INTO rq VALUES (4, 'w')",
        "INSERT INTO re VALUES ('w')",
        "INSERT INTO log VALUES ('w')",
        "SELECT * FROM re",
        "PRAGMA foreign_key_check",
        // An UPSERT's DO UPDATE fires the ON UPDATE actions of a parent key
        // it changes, after the row is written and before RETURNING.
        "CREATE TABLE up(id INTEGER PRIMARY KEY, k UNIQUE, v)",
        "CREATE TABLE uc(x REFERENCES up(k) ON UPDATE CASCADE, n)",
        "CREATE TABLE ud(y REFERENCES up(k) ON UPDATE SET NULL)",
        "CREATE TABLE ue(z REFERENCES up(id) ON UPDATE CASCADE)",
        "CREATE TABLE ulog(m)",
        "CREATE TRIGGER utc AFTER UPDATE ON uc BEGIN INSERT INTO ulog VALUES (old.x || '->' || new.x); END",
        "INSERT INTO up VALUES (1, 'a', 0), (2, 'b', 0)",
        "INSERT INTO uc VALUES ('a', 1), ('b', 2)",
        "INSERT INTO ud VALUES ('a'), ('b')",
        "INSERT INTO ue VALUES (1), (2)",
        "INSERT INTO up VALUES (1, 'zz', 5) ON CONFLICT(id) DO UPDATE SET k = excluded.k RETURNING *, (SELECT count(*) FROM uc WHERE x = 'zz')",
        "SELECT * FROM uc ORDER BY n",
        "SELECT * FROM ud ORDER BY y",
        "INSERT INTO up VALUES (9, 'b', 5) ON CONFLICT(k) DO UPDATE SET v = v + 1",
        "INSERT INTO up VALUES (9, 'b', 5) ON CONFLICT(k) DO UPDATE SET id = 20",
        "SELECT * FROM ue ORDER BY z",
        "INSERT INTO up VALUES (20, 'q', 0), (1, 'r', 0) ON CONFLICT(id) DO UPDATE SET k = excluded.k || '!'",
        "INSERT INTO up VALUES (20, 'q', 0) ON CONFLICT(k) DO NOTHING ON CONFLICT(id) DO UPDATE SET k = 'w'",
        "SELECT * FROM uc ORDER BY n",
        "SELECT * FROM ulog",
        "CREATE TABLE us(id INTEGER PRIMARY KEY, k UNIQUE, up REFERENCES us(k) ON UPDATE CASCADE)",
        "INSERT INTO us VALUES (1, 'a', NULL), (2, 'b', 'a'), (3, 'c', 'a')",
        "INSERT INTO us VALUES (1, 'x', NULL) ON CONFLICT(id) DO UPDATE SET k = 'aa'",
        "SELECT * FROM us ORDER BY id",
        "PRAGMA foreign_key_check",
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

/// A DO UPDATE that sets the INTEGER PRIMARY KEY moves the row: the new
/// rowid must be free, and every index entry and the row are rewritten.
#[test]
fn hir_upsert_moving_rows_match_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE um(a INTEGER PRIMARY KEY, b UNIQUE, c)",
        "CREATE INDEX um_c ON um(c)",
        "CREATE TABLE umlog(m)",
        "CREATE TRIGGER au AFTER UPDATE ON um BEGIN INSERT INTO umlog VALUES (old.a || '>' || new.a); END",
        "INSERT INTO um VALUES (1, 1, 'x'), (2, 2, 'y')",
        "INSERT INTO um VALUES (3, 1, 'z') ON CONFLICT(b) DO UPDATE SET a = 10",
        "SELECT * FROM um ORDER BY a",
        "INSERT INTO um VALUES (3, 1, 'z') ON CONFLICT(b) DO UPDATE SET a = 2",
        "INSERT INTO um VALUES (3, 1, 'z') ON CONFLICT(b) DO UPDATE SET a = a + 5, c = 'moved' RETURNING *",
        "INSERT INTO um VALUES (3, 1, 'z') ON CONFLICT(b) DO UPDATE SET a = 'abc'",
        "INSERT INTO um VALUES (15, 7, 'q') ON CONFLICT(a) DO UPDATE SET a = excluded.b, b = 70",
        "SELECT * FROM um ORDER BY a",
        "SELECT c, a FROM um INDEXED BY um_c ORDER BY c",
        "SELECT * FROM umlog",
        "PRAGMA integrity_check",
        // `rowid` names the rowid, on a table without an INTEGER PRIMARY KEY
        // too, unless the table has a column of that name. Legacy rejects it.
        "INSERT INTO um VALUES (3, 70, 'z') ON CONFLICT(b) DO UPDATE SET rowid = 30 RETURNING *",
        "CREATE TABLE un(v UNIQUE, w)",
        "CREATE INDEX un_w ON un(w)",
        "INSERT INTO un VALUES ('a', 1), ('b', 3)",
        "INSERT INTO un VALUES ('a', 2) ON CONFLICT(v) DO UPDATE SET oid = 9, w = excluded.w RETURNING rowid, *",
        "INSERT INTO un VALUES ('b', 4) ON CONFLICT(v) DO UPDATE SET _rowid_ = 9",
        "INSERT INTO un VALUES ('b', 4) ON CONFLICT(v) DO UPDATE SET rowid = 'x'",
        "SELECT rowid, * FROM un ORDER BY rowid",
        "SELECT w FROM un INDEXED BY un_w ORDER BY w",
        "CREATE TABLE ur(rowid, v UNIQUE)",
        "INSERT INTO ur VALUES (1, 'a')",
        "INSERT INTO ur VALUES (2, 'a') ON CONFLICT(v) DO UPDATE SET rowid = 50",
        "SELECT _rowid_, * FROM ur",
        "PRAGMA integrity_check",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// An UPDATE that changes the key of the index its loop reads, or that may
/// REPLACE rows while reading an index, collects its rows first, as SQLite
/// does. Legacy loses a row when a constraint-level REPLACE runs during an
/// index loop.
#[test]
fn hir_update_through_a_changing_index_matches_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b, c UNIQUE ON CONFLICT REPLACE)",
        "CREATE INDEX ta ON t(a)",
        "CREATE INDEX tb ON t(b)",
        "INSERT INTO t VALUES (1, 1, 10, 'x'), (2, 2, 20, 'y'), (3, 3, 30, 'z'), (4, 4, 40, 'w'), (5, 5, 50, 'v')",
        "UPDATE t INDEXED BY ta SET a = a + 1 WHERE a > 1",
        "SELECT * FROM t ORDER BY id",
        "UPDATE t SET a = a * 10 WHERE a > 3 RETURNING id, a",
        "UPDATE t INDEXED BY tb SET c = 'x' WHERE b > 25",
        "SELECT * FROM t ORDER BY id",
        "INSERT INTO t VALUES (6, 6, 60, 'q'), (7, 7, 70, 'r'), (8, 8, 80, 's')",
        "UPDATE t INDEXED BY tb SET c = 'q', b = b + 1 WHERE b >= 60",
        "SELECT * FROM t ORDER BY id",
        "UPDATE t INDEXED BY ta SET a = 100 - a WHERE a BETWEEN 1 AND 70",
        "SELECT * FROM t ORDER BY id",
        "PRAGMA integrity_check",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// DML on a table with a materialized view goes through HIR, and the view
/// follows each change. The UPDATE is one legacy gets wrong (it loses a row
/// when a constraint's REPLACE runs during an index loop), so the table is
/// checked against SQLite too.
#[test]
fn hir_dml_keeps_materialized_views_up_to_date() {
    let db = TempDatabase::builder().with_views(true).build();
    let conn = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b, c UNIQUE ON CONFLICT REPLACE)",
        "CREATE INDEX tb ON t(b)",
        "INSERT INTO t VALUES (1, 1, 10, 'x'), (2, 2, 20, 'y'), (3, 3, 30, 'z'), (4, 4, 40, 'w')",
    ] {
        conn.execute(sql).unwrap();
        sqlite.execute(sql, ()).unwrap();
    }
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT id, a, c FROM t WHERE a > 1")
        .unwrap();
    for sql in [
        "UPDATE t INDEXED BY tb SET c = 'x' WHERE b > 25",
        "INSERT INTO t VALUES (5, 5, 50, 'v') ON CONFLICT DO NOTHING",
        "INSERT INTO t VALUES (5, 6, 60, 'q') ON CONFLICT(id) DO UPDATE SET a = excluded.a",
        "UPDATE t SET id = id + 10 WHERE a > 4",
        "DELETE FROM t WHERE a = 2",
        "REPLACE INTO t VALUES (20, 7, 70, 'x')",
    ] {
        conn.execute(sql).unwrap();
        sqlite.execute(sql, ()).unwrap();
        let rows = "SELECT * FROM t ORDER BY id";
        assert_eq!(
            limbo_exec_rows(&conn, rows),
            sqlite_exec_rows(&sqlite, rows),
            "{sql}"
        );
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT * FROM v ORDER BY id"),
            limbo_exec_rows(&conn, "SELECT id, a, c FROM t WHERE a > 1 ORDER BY id"),
            "{sql}"
        );
    }
}

/// INSERT, UPSERT and UPDATE on STRICT tables with custom types, as legacy
/// does them: the values written are type checked, encoded and checked
/// again; CHECK constraints and foreign keys compare stored values; index
/// expressions, RETURNING, `excluded` and triggers see decoded values.
#[test]
fn hir_custom_type_writes_keep_their_results() {
    let statements = [
        "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
        "CREATE TYPE rev BASE text ENCODE upper(value) DECODE lower(value)",
        "CREATE TYPE tricky BASE integer ENCODE value DECODE CASE WHEN value = 42 THEN NULL ELSE value END",
        "CREATE TABLE p(id cents PRIMARY KEY, name rev) STRICT",
        "CREATE INDEX p_initial ON p(substr(name, 1, 1))",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid cents REFERENCES p(id) ON DELETE CASCADE, \
         amount cents CHECK (amount < CAST(50 AS cents)), t tricky NOT NULL) STRICT",
        "CREATE TABLE log(m TEXT) STRICT",
        "CREATE TRIGGER tl AFTER INSERT ON p BEGIN INSERT INTO log VALUES (new.name || ':' || new.id); END",
        "PRAGMA foreign_keys = ON",
        "INSERT INTO p VALUES (1, 'Hello'), (2, 'world') RETURNING id, name",
        "INSERT INTO p VALUES ('x', 'bad')",
        "INSERT INTO c VALUES (1, 1, 20, 7)",
        "INSERT INTO c VALUES (2, 3, 20, 7)",
        "INSERT INTO c VALUES (3, 2, 60, 7)",
        "INSERT INTO c VALUES (4, 2, 10, 42)",
        "INSERT OR IGNORE INTO c VALUES (5, 2, 10, 42)",
        "SELECT * FROM p ORDER BY id",
        "SELECT id, hex(name) FROM p ORDER BY id",
        "SELECT * FROM p WHERE substr(name, 1, 1) = 'h'",
        "SELECT * FROM c ORDER BY id",
        "SELECT * FROM log",
        "CREATE TRIGGER tu AFTER UPDATE ON p BEGIN INSERT INTO log VALUES (old.name || '>' || new.name); END",
        "UPDATE p SET name = 'Again' WHERE id = 2 RETURNING id, name",
        "SELECT * FROM p WHERE substr(name, 1, 1) = 'a'",
        "UPDATE p SET id = 'x'",
        "UPDATE c SET amount = 60 WHERE id = 1",
        "UPDATE c SET amount = amount + 5 WHERE id = 1 RETURNING amount",
        "UPDATE c SET pid = 3 WHERE id = 1",
        "UPDATE c SET t = 42 WHERE id = 1",
        "UPDATE OR IGNORE c SET t = 42",
        "UPDATE c SET id = 9 WHERE id = 1 RETURNING *",
        "SELECT id, hex(name) FROM p ORDER BY id",
        "SELECT * FROM c ORDER BY id",
        "SELECT * FROM log",
        "CREATE TABLE u(k rev PRIMARY KEY, v cents, w cents NOT NULL) STRICT",
        "CREATE TRIGGER uu BEFORE UPDATE ON u BEGIN INSERT INTO log VALUES (old.k || old.v || '>' || new.k || new.v); END",
        "INSERT INTO u VALUES ('A', 1, 2)",
        "INSERT INTO u VALUES ('a', 5, 6) ON CONFLICT(k) DO UPDATE SET v = excluded.v + v RETURNING *",
        "INSERT INTO u VALUES ('a', 7, 8) ON CONFLICT DO UPDATE SET w = excluded.w WHERE v < excluded.v RETURNING k, v, w",
        "INSERT INTO u VALUES ('a', 7, 8) ON CONFLICT DO UPDATE SET w = NULL",
        "INSERT INTO u VALUES ('a', 7, 8) ON CONFLICT DO UPDATE SET w = 'x'",
        "INSERT INTO u VALUES ('b', 1, 1), ('B', 2, 2) ON CONFLICT DO UPDATE SET k = 'c'",
        "SELECT k, hex(k), v, w FROM u",
        "SELECT * FROM log",
        "INSERT INTO p VALUES (2, 'x') ON CONFLICT DO UPDATE SET id = 5",
        "SELECT * FROM c",
        "CREATE TABLE arr(id INTEGER PRIMARY KEY, tags TEXT[] NOT NULL, grid INTEGER[][]) STRICT",
        "INSERT INTO arr VALUES (1, '{a,b}', '{{1,2},{3}}'), (2, ARRAY['c'], NULL) RETURNING *",
        "INSERT INTO arr VALUES (3, 'not an array', NULL)",
        "INSERT INTO arr VALUES (4, NULL, NULL)",
        "UPDATE arr SET tags[1] = 'z' WHERE id = 1 RETURNING tags",
        "UPDATE arr SET grid = '{{9}}' WHERE id = 2",
        // Legacy decodes arrays to text here, so `excluded.tags || tags`
        // would join text where HIR joins arrays.
        "INSERT INTO arr VALUES (2, '{d}', NULL) ON CONFLICT DO UPDATE SET tags = excluded.tags",
        "SELECT id, tags, grid, tags[2] FROM arr ORDER BY id",
        "DELETE FROM p WHERE id = 1",
        "SELECT count(*) FROM c",
    ];
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
        .build();
    let conn = db.connect_limbo();
    let results = statements
        .iter()
        .map(|sql| limbo_exec_rows_fallible(&db, &conn, sql).map_err(|e| e.to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        format!("{results:?}"),
        r#"[Ok([]), Ok([]), Ok([]), Ok([]), Ok([]), Ok([]), Ok([]), Ok([]), Ok([]), Ok([[Integer(1), Text("hello")], [Integer(2), Text("world")]]), Err("cannot store TEXT value in INTEGER column p.id (19)"), Ok([]), Err("immediate foreign key constraint failed"), Err("CHECK constraint failed: amount < CAST(50 AS cents)"), Err("NOT NULL constraint failed: c.t"), Ok([]), Err("Parse error: cannot ORDER BY column 'id' of type 'cents': type does not declare OPERATOR '<'"), Err("Parse error: cannot ORDER BY column 'id' of type 'cents': type does not declare OPERATOR '<'"), Ok([[Integer(1), Text("hello")]]), Ok([[Integer(1), Integer(1), Integer(20), Integer(7)]]), Ok([[Text("hello:1")], [Text("world:2")]]), Ok([]), Ok([[Integer(2), Text("again")]]), Ok([[Integer(2), Text("again")]]), Err("cannot store TEXT value in INTEGER column p.id (19)"), Err("CHECK constraint failed: amount < CAST(50 AS cents)"), Ok([[Integer(25)]]), Err("immediate foreign key constraint failed"), Ok([]), Ok([]), Ok([[Integer(9), Integer(1), Integer(25), Null]]), Err("Parse error: cannot ORDER BY column 'id' of type 'cents': type does not declare OPERATOR '<'"), Ok([[Integer(9), Integer(1), Integer(25), Null]]), Ok([[Text("hello:1")], [Text("world:2")], [Text("world>again")]]), Ok([]), Ok([]), Ok([]), Ok([[Text("a"), Integer(6), Integer(2)]]), Ok([[Text("a"), Integer(6), Integer(8)]]), Err("NOT NULL constraint failed: u.w"), Err("cannot store TEXT value in INTEGER column u.w (19)"), Ok([]), Ok([[Text("a"), Text("61"), Integer(6), Integer(8)], [Text("c"), Text("63"), Integer(1), Integer(1)]]), Ok([[Text("hello:1")], [Text("world:2")], [Text("world>again")], [Text("a1>a6")], [Text("a6>a6")], [Text("b1>c1")]]), Ok([]), Ok([[Integer(9), Integer(1), Integer(25), Null]]), Ok([]), Ok([[Integer(1), Text("{a,b}"), Text("{\"{1\",\"2}\",\"{3}\"}")], [Integer(2), Text("{c}"), Null]]), Err("cannot store non-array value in TEXT column arr.tags (19)"), Err("NOT NULL constraint failed: arr.tags"), Ok([[Text("{z,b}")]]), Ok([]), Ok([]), Ok([[Integer(1), Text("{z,b}"), Text("{\"{1\",\"2}\",\"{3}\"}"), Text("b")], [Integer(2), Text("{d}"), Text("{\"{9}\"}"), Null]]), Ok([]), Ok([[Integer(0)]])]"#
    );
}

/// BEFORE triggers and virtual columns of a table with custom types read
/// SQL values: NEW holds the values a statement writes and the decoded
/// values of the columns an UPDATE leaves alone. Legacy shows a BEFORE
/// UPDATE trigger those columns as stored, and decodes the values a virtual
/// column reads twice or not at all.
#[test]
fn hir_custom_type_triggers_and_virtual_columns_read_sql_values() {
    let db = TempDatabase::builder()
        .with_opts(
            turso_core::DatabaseOpts::new()
                .with_custom_types(true)
                .with_generated_columns(true),
        )
        .build();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a cents, b cents, v INTEGER AS (a + b)) STRICT",
        "CREATE TABLE log(m TEXT)",
        "CREATE TRIGGER bi BEFORE INSERT ON t BEGIN \
         INSERT INTO log VALUES ('bi ' || new.a || ' ' || new.b || ' ' || new.v); END",
        "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN \
         INSERT INTO log VALUES ('bu ' || old.a || ' ' || old.b || ' > ' || new.a || ' ' || new.b || ' ' || new.v); END",
        "CREATE TRIGGER au AFTER UPDATE ON t BEGIN \
         INSERT INTO log VALUES ('au ' || new.a || ' ' || new.b || ' ' || new.v); END",
        "INSERT INTO t VALUES (1, 2, 3)",
        "UPDATE t SET a = 7 WHERE id = 1",
        "INSERT INTO t VALUES (1, 9, 9) ON CONFLICT DO UPDATE SET b = excluded.b",
    ] {
        limbo_exec_rows(&conn, sql);
    }
    let rows = |sql| {
        limbo_exec_rows(&conn, sql)
            .into_iter()
            .map(|row| format!("{row:?}"))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rows("SELECT a, b, v FROM t"),
        ["[Integer(7), Integer(9), Integer(16)]"]
    );
    assert_eq!(
        rows("SELECT m FROM log"),
        [
            "[Text(\"bi 2 3 5\")]",
            "[Text(\"bu 2 3 > 7 3 10\")]",
            "[Text(\"au 7 3 10\")]",
            "[Text(\"bi 9 9 18\")]",
            "[Text(\"bu 7 3 > 7 9 16\")]",
            "[Text(\"au 7 9 16\")]",
        ]
    );
}

/// Queries over custom-typed columns: GROUP BY keeps the grouped columns as
/// stored so aggregates decode them once, BETWEEN uses the type's own
/// comparison operators, and an array of a custom type has its elements
/// decoded for display, as legacy does.
#[test]
fn hir_custom_type_queries_decode_once() {
    let statements = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val numeric(10,2), grp TEXT, flag INTEGER) STRICT",
        "INSERT INTO t VALUES (1, 100, 'a', 1), (2, 9, 'a', 0), (3, 50, 'b', 1), (4, -5, 'b', 0)",
        "SELECT grp, sum(val), max(val) FROM t GROUP BY grp ORDER BY grp",
        "SELECT grp, sum(val) FILTER (WHERE flag = 1) AS s FROM t GROUP BY grp HAVING s > 0",
        "SELECT id, val FROM t WHERE val BETWEEN 9 AND 50 ORDER BY id",
        "SELECT id FROM t WHERE val NOT BETWEEN 9 AND 50 ORDER BY id",
        "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
        "CREATE TABLE a(id INTEGER PRIMARY KEY, c cents[]) STRICT",
        "INSERT INTO a VALUES (1, '{1,2,3}'), (2, ARRAY[7])",
        "SELECT id, c FROM a ORDER BY id",
        "SELECT c FROM a ORDER BY id DESC",
    ];
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
        .build();
    let conn = db.connect_limbo();
    let results = statements
        .iter()
        .map(|sql| limbo_exec_rows_fallible(&db, &conn, sql).map_err(|e| e.to_string()))
        .collect::<Vec<_>>();
    for (sql, result) in statements.iter().zip(&results) {
        assert!(result.is_ok(), "{sql}: {result:?}");
    }
    assert_eq!(
        format!("{results:?}"),
        r#"[Ok([]), Ok([]), Ok([[Text("a"), Real(109.0), Text("100.00")], [Text("b"), Real(45.0), Text("50.00")]]), Ok([[Text("a"), Real(100.0)], [Text("b"), Real(50.0)]]), Ok([[Integer(2), Text("9.00")], [Integer(3), Text("50.00")]]), Ok([[Integer(1)], [Integer(4)]]), Ok([]), Ok([]), Ok([]), Ok([[Integer(1), Text("{1,2,3}")], [Integer(2), Text("{7}")]]), Ok([[Text("{7}")], [Text("{1,2,3}")]])]"#
    );
}

/// Change data capture through HIR writes one row per changed row and one
/// COMMIT row per statement. Legacy also writes a COMMIT row for each
/// trigger or foreign-key action it runs, files the new image of a row an
/// UPDATE moves under `before`, and gives UPSERT DO UPDATE the new row as its
/// before image.
#[test]
fn hir_change_capture_records_each_change_once() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE TABLE p(id INTEGER PRIMARY KEY, v)",
        "CREATE TABLE l(m)",
        "CREATE TRIGGER tr AFTER INSERT ON p BEGIN INSERT INTO l VALUES (new.id); END",
    ] {
        conn.execute(sql).unwrap();
    }
    limbo_exec_rows(&conn, "PRAGMA capture_data_changes_conn('full')");
    for sql in [
        "INSERT INTO p VALUES (1, 10)",
        "UPDATE p SET id = 2",
        "INSERT INTO p VALUES (2, 0) ON CONFLICT(id) DO UPDATE SET v = 30",
    ] {
        conn.execute(sql).unwrap();
    }
    let image = |column: &str| {
        format!(
            "CASE WHEN table_name IS NULL THEN NULL \
             ELSE bin_record_json_object(table_columns_json_array(table_name), {column}) END"
        )
    };
    let rows = limbo_exec_rows(
        &conn,
        &format!(
            "SELECT change_type, table_name, id, {}, {} FROM turso_cdc ORDER BY change_id",
            image("before"),
            image("after")
        ),
    );
    let text = |value: &str| rusqlite::types::Value::Text(value.to_string());
    let int = rusqlite::types::Value::Integer;
    let null = rusqlite::types::Value::Null;
    let commit = vec![
        int(2),
        null.clone(),
        null.clone(),
        null.clone(),
        null.clone(),
    ];
    assert_eq!(
        rows,
        vec![
            vec![int(1), text("l"), int(1), null.clone(), text(r#"{"m":1}"#)],
            vec![
                int(1),
                text("p"),
                int(1),
                null.clone(),
                text(r#"{"id":1,"v":10}"#)
            ],
            commit.clone(),
            vec![
                int(-1),
                text("p"),
                int(1),
                text(r#"{"id":1,"v":10}"#),
                null.clone()
            ],
            vec![
                int(1),
                text("p"),
                int(2),
                null.clone(),
                text(r#"{"id":2,"v":10}"#)
            ],
            commit.clone(),
            vec![
                int(0),
                text("p"),
                int(2),
                text(r#"{"id":2,"v":10}"#),
                text(r#"{"id":2,"v":30}"#),
            ],
            commit,
        ]
    );
}

/// RETURNING subqueries of UPDATE ... FROM: a correlated one runs per row,
/// any other once, as in SQLite. Legacy runs one that reads the target
/// through a derived table again for each row.
#[test]
fn hir_update_from_returning_subqueries_match_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TABLE s(id INTEGER PRIMARY KEY, bump INTEGER)",
        "INSERT INTO t VALUES(1,0),(2,0),(3,0)",
        "INSERT INTO s VALUES(1,1),(2,2),(3,3)",
        "CREATE TRIGGER bu BEFORE UPDATE ON t WHEN NEW.id = 1 BEGIN UPDATE s SET bump = bump + 100 WHERE id = 2; END",
        "UPDATE t SET v = s.bump FROM s WHERE t.id = s.id RETURNING id, v, (SELECT sum(bump) FROM s), (SELECT bump FROM s WHERE s.id = t.id)",
        "UPDATE t SET v = s.bump + 1 FROM s WHERE t.id = s.id RETURNING id, (SELECT max(v) FROM t)",
        "UPDATE t SET v = s.bump + 2 FROM s WHERE t.id = s.id RETURNING id, (SELECT count(*) FROM t AS x WHERE x.v > 5)",
        "UPDATE t SET v = v + 1 FROM (SELECT 1) RETURNING id, (SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id))",
        "UPDATE t SET v = v + 1 FROM (SELECT 1) RETURNING id, EXISTS (SELECT 1 FROM s WHERE bump > 200), id IN (SELECT id FROM s WHERE bump > 50)",
        "UPDATE t SET v = 1 WHERE id < 3 RETURNING id, (SELECT max(v) FROM t), (SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id))",
        // A CTE that FROM and RETURNING both read.
        "WITH c(total) AS (SELECT sum(bump) FROM s) UPDATE t SET v = 0 FROM c WHERE t.id > 0 RETURNING id, v, (SELECT total FROM c)",
        "WITH c(id, bump) AS (SELECT id, bump FROM s) UPDATE t SET v = src.bump FROM c AS src WHERE t.id = src.id RETURNING id, (SELECT bump FROM c WHERE c.id = t.id)",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// UPDATE ... FROM that sets the rowid moves each joined row, with its
/// index entries, as SQLite does.
#[test]
fn hir_update_from_moving_rows_match_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v, w UNIQUE)",
        "CREATE INDEX tv ON t(v)",
        "CREATE TABLE m(old, new, tag)",
        "INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
        "INSERT INTO m VALUES (1, 100, 'x'), (2, 200, 'y')",
        "UPDATE t SET id = m.new, v = v || m.tag FROM m WHERE m.old = t.id RETURNING id, v, w",
        "SELECT id, v FROM t INDEXED BY tv ORDER BY v",
        "UPDATE t SET rowid = m.old FROM m WHERE m.new = t.id",
        "SELECT * FROM t ORDER BY id",
        "UPDATE t SET id = 3 FROM m WHERE m.old = t.id AND m.tag = 'x'",
        "UPDATE OR REPLACE t SET id = 3 FROM m WHERE m.old = t.id AND m.tag = 'x'",
        "SELECT * FROM t ORDER BY id",
        "UPDATE t SET (id, v) = (SELECT m.new + 1, m.tag) FROM m WHERE m.old = t.id",
        "UPDATE t SET rowid = m.new + 2, id = m.new + 3 FROM m WHERE m.new + 1 = t.id RETURNING *",
        "UPDATE t SET id = id + 1, _rowid_ = id + 2 WHERE id > 100 RETURNING *",
        "UPDATE t SET id = 'abc' FROM m WHERE m.new + 3 = t.id",
        "SELECT * FROM t ORDER BY id",
        "CREATE TABLE log(m)",
        "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN INSERT INTO log VALUES (old.id || '->' || new.id); END",
        "UPDATE t SET id = id + 1000 FROM (SELECT 1 AS k) WHERE k = 1",
        "SELECT * FROM t ORDER BY id",
        "SELECT * FROM log ORDER BY m",
        "PRAGMA integrity_check",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// Like SQLite, a DEFAULT on an INTEGER PRIMARY KEY is never used: a row
/// that does not give the rowid gets a new one. Legacy uses the DEFAULT for
/// DEFAULT VALUES.
#[test]
fn hir_rowid_default_matches_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE d1(id INTEGER PRIMARY KEY DEFAULT 5, v)",
        "INSERT INTO d1(v) VALUES (1), (2)",
        "INSERT INTO d1 DEFAULT VALUES",
        "INSERT INTO d1 DEFAULT VALUES RETURNING id",
        "INSERT INTO d1(v) SELECT 3",
        "SELECT * FROM d1 ORDER BY id",
        "CREATE TABLE d2(id INTEGER PRIMARY KEY AUTOINCREMENT DEFAULT 10, v)",
        "INSERT INTO d2 DEFAULT VALUES",
        "SELECT * FROM d2",
        "SELECT * FROM sqlite_sequence",
        "CREATE TABLE d3(id INTEGER PRIMARY KEY DEFAULT 'x', v)",
        "INSERT INTO d3 DEFAULT VALUES",
        "SELECT * FROM d3",
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
fn hir_delete_errors_name_what_is_wrong() {
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    hir.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b)")
        .unwrap();
    for (sql, message) in [
        ("DELETE FROM nosuch", "Parse error: no such table: nosuch"),
        (
            "DELETE FROM main.nosuch",
            "Parse error: no such table: main.nosuch",
        ),
        ("DELETE FROM nosuch.t", "no such database: nosuch"),
        (
            "DELETE FROM t WHERE nosuch = 1",
            "Parse error: no such column: nosuch",
        ),
        (
            "DELETE FROM sqlite_schema",
            "Parse error: table sqlite_schema may not be modified",
        ),
        (
            "DELETE FROM t INDEXED BY nosuch",
            "Parse error: no such index: nosuch",
        ),
        (
            "DELETE FROM t RETURNING t.*",
            "Parse error: RETURNING may not use \"TABLE.*\" wildcards",
        ),
        (
            "UPDATE sqlite_schema SET name = 'x'",
            "Parse error: table sqlite_schema may not be modified",
        ),
        (
            "INSERT INTO sqlite_master VALUES (1, 2, 3, 4, 5)",
            "Parse error: table sqlite_master may not be modified",
        ),
        (
            "INSERT INTO t VALUES (1, 2) ON CONFLICT(b) DO NOTHING",
            "Parse error: ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint",
        ),
        (
            "INSERT INTO t VALUES (1, 2) ON CONFLICT(a NULLS FIRST) DO NOTHING",
            "Parse error: unsupported use of NULLS FIRST",
        ),
    ] {
        let hir_error = hir.prepare(sql).expect_err("HIR rejects").to_string();
        assert_eq!(hir_error, message, "{sql}");
    }
    // Legacy differs from SQLite on these: it reports a missing constraint
    // for an unknown column, accepts COLLATE on the INTEGER PRIMARY KEY and
    // reports a column of an unknown table as "no such table". HIR follows
    // SQLite.
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
        ("DELETE FROM t WHERE x.b = 1", "no such column: x.b"),
    ] {
        let sqlite_error = sqlite.prepare(sql).expect_err("SQLite rejects").to_string();
        let hir_error = hir.prepare(sql).expect_err("HIR rejects").to_string();
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

/// INSERT, UPDATE and DELETE through HIR keep an FTS index, which has no
/// B-tree of its own, up to date, as legacy does.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn hir_dml_writes_fts_indexes() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)",
        "CREATE INDEX docs_fts ON docs USING fts(body)",
        "CREATE TABLE src(v INTEGER)",
        "CREATE TRIGGER tr AFTER INSERT ON src BEGIN \
         INSERT INTO docs VALUES (new.v, 'fromtrigger'); END",
        "INSERT INTO docs VALUES (1, 'apple'), (2, 'banana'), (3, 'apple pie')",
        "INSERT INTO src VALUES (4)",
        "UPDATE docs SET body = 'cherry' WHERE id = 3",
        "DELETE FROM docs WHERE id = 1",
    ] {
        limbo_exec_rows(&conn, sql);
    }
    let matching = |word: &str| {
        limbo_exec_rows(
            &conn,
            &format!("SELECT id FROM docs WHERE fts_match(body, '{word}') ORDER BY id"),
        )
    };
    let ids = |ids: &[i64]| {
        ids.iter()
            .map(|id| vec![rusqlite::types::Value::Integer(*id)])
            .collect::<Vec<_>>()
    };
    assert_eq!(matching("apple"), ids(&[]));
    assert_eq!(matching("banana"), ids(&[2]));
    assert_eq!(matching("cherry"), ids(&[3]));
    assert_eq!(matching("fromtrigger"), ids(&[4]));
}

/// INSERT into a WITHOUT ROWID table through HIR checks the whole primary
/// key and stores its columns first. Legacy lets a duplicate composite key
/// in and rejects INSERT OR IGNORE.
#[test]
fn hir_inserts_into_without_rowid_tables() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_without_rowid(true))
        .build();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE TABLE w(b, a TEXT PRIMARY KEY) WITHOUT ROWID",
        "CREATE TABLE w2(a INTEGER, b TEXT, c, PRIMARY KEY (b, a)) WITHOUT ROWID",
        "INSERT INTO w VALUES (3, 'z'), (1, 'a')",
        "INSERT OR IGNORE INTO w VALUES (5, 'z'), (6, 'm')",
        "INSERT INTO w2 VALUES (1, 'x', 'p'), (2, 'x', 'q'), (1, 'y', 'r')",
    ] {
        limbo_exec_rows(&conn, sql);
    }
    let error = |sql| {
        limbo_exec_rows_fallible(&db, &conn, sql)
            .unwrap_err()
            .to_string()
    };
    assert!(error("INSERT INTO w VALUES (5, 'z')").contains("UNIQUE constraint failed: w.a"));
    assert!(error("INSERT INTO w VALUES (5, NULL)").contains("NOT NULL constraint failed: w.a"));
    assert!(error("INSERT INTO w2 VALUES (1, 'x', 'dup')")
        .contains("UNIQUE constraint failed: w2.b, w2.a"));
    assert!(error("INSERT OR REPLACE INTO w VALUES (7, 'q')").contains("not supported"));
    let rows = |sql| {
        limbo_exec_rows(&conn, sql)
            .into_iter()
            .map(|row| format!("{row:?}"))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rows("SELECT * FROM w"),
        [
            "[Integer(1), Text(\"a\")]",
            "[Integer(6), Text(\"m\")]",
            "[Integer(3), Text(\"z\")]",
        ]
    );
    assert_eq!(
        rows("SELECT a, b FROM w2"),
        [
            "[Integer(1), Text(\"x\")]",
            "[Integer(2), Text(\"x\")]",
            "[Integer(1), Text(\"y\")]",
        ]
    );
}

/// ALTER TABLE rewrites its schema row with a generated UPDATE of
/// sqlite_schema. With HIR on, HIR lowers that UPDATE, and change data
/// capture still records the ALTER statement as the new `sql` value, so the
/// schema, the rows and the captured changes match legacy.
#[test]
fn hir_alter_table_schema_updates_are_recorded() {
    let statements = [
        "PRAGMA capture_data_changes_conn('full')",
        "CREATE TABLE w(a INTEGER PRIMARY KEY, b TEXT, c INT)",
        "INSERT INTO w VALUES (1, 'x', 10), (2, 'y', 20)",
        "CREATE INDEX w_b ON w(b)",
        "ALTER TABLE w ADD COLUMN d TEXT DEFAULT 'z'",
        "ALTER TABLE w RENAME COLUMN b TO bb",
        "ALTER TABLE w DROP COLUMN c",
        "ALTER TABLE w RENAME TO w2",
        "SELECT * FROM w2 ORDER BY a",
        "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY name",
        "SELECT change_type, table_name, id, \
         bin_record_json_object(table_columns_json_array('sqlite_schema'), updates) \
         FROM turso_cdc WHERE table_name = 'sqlite_schema' ORDER BY change_id",
    ];
    assert_eq!(
        format!("{:?}", run_with_errors(&statements)),
        r#"[Ok([]), Ok([[Integer(1)]]), Ok([]), Ok([[Integer(1)]]), Ok([]), Ok([[Integer(2)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([[Integer(1), Text("x"), Text("z")], [Integer(2), Text("y"), Text("z")]]), Ok([[Integer(0)]]), Ok([[Text("table"), Text("__turso_internal_seq___turso_internal_autoincrement_turso_cdc"), Text("__turso_internal_seq___turso_internal_autoincrement_turso_cdc"), Text("CREATE TABLE \"__turso_internal_seq___turso_internal_autoincrement_turso_cdc\"(value INTEGER PRIMARY KEY,is_called INTEGER,start INTEGER,inc INTEGER,min INTEGER,max INTEGER,cycle INTEGER)")], [Text("table"), Text("log"), Text("log"), Text("CREATE TABLE log (a INTEGER)")], [Text("index"), Text("sqlite_autoindex_turso_cdc_version_1"), Text("turso_cdc_version"), Null], [Text("table"), Text("sqlite_sequence"), Text("sqlite_sequence"), Text("CREATE TABLE sqlite_sequence(name,seq)")], [Text("table"), Text("t"), Text("t"), Text("CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER, c TEXT, d REAL, g AS (b * 2) VIRTUAL)")], [Text("index"), Text("t_b"), Text("t"), Text("CREATE INDEX t_b ON t (b)")], [Text("index"), Text("t_c_desc"), Text("t"), Text("CREATE INDEX t_c_desc ON t (c DESC, b)")], [Text("index"), Text("t_d"), Text("t"), Text("CREATE INDEX t_d ON t (d)")], [Text("index"), Text("t_expr"), Text("t"), Text("CREATE INDEX t_expr ON t (b + length (c))")], [Text("index"), Text("t_g"), Text("t"), Text("CREATE INDEX t_g ON t (g)")], [Text("index"), Text("t_partial"), Text("t"), Text("CREATE INDEX t_partial ON t (c) WHERE t.b > 2")], [Text("index"), Text("t_unique"), Text("t"), Text("CREATE UNIQUE INDEX t_unique ON t (c, d)")], [Text("table"), Text("turso_cdc"), Text("turso_cdc"), Text("CREATE TABLE turso_cdc (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_txn_id INTEGER, change_type INTEGER, table_name TEXT, id, \"before\" BLOB, \"after\" BLOB, updates BLOB)")], [Text("table"), Text("turso_cdc_version"), Text("turso_cdc_version"), Text("CREATE TABLE turso_cdc_version (table_name TEXT PRIMARY KEY, version TEXT NOT NULL)")], [Text("table"), Text("u"), Text("u"), Text("CREATE TABLE u (a INTEGER PRIMARY KEY, b INTEGER NOT NULL, c TEXT, d REAL)")], [Text("index"), Text("u_b"), Text("u"), Text("CREATE UNIQUE INDEX u_b ON u (b)")], [Text("index"), Text("u_c"), Text("u"), Text("CREATE INDEX u_c ON u (c, d DESC)")], [Text("index"), Text("u_expr"), Text("u"), Text("CREATE INDEX u_expr ON u (c || 'x', b)")], [Text("index"), Text("u_partial"), Text("u"), Text("CREATE INDEX u_partial ON u (c) WHERE u.d > 0")], [Text("index"), Text("u_partial_unique"), Text("u"), Text("CREATE UNIQUE INDEX u_partial_unique ON u (d) WHERE c = 'p'")], [Text("table"), Text("w2"), Text("w2"), Text("CREATE TABLE w2 (a INTEGER PRIMARY KEY, bb TEXT, d TEXT DEFAULT 'z')")], [Text("index"), Text("w_b"), Text("w2"), Text("CREATE INDEX w_b ON w2 (bb)")]]), Ok([[Integer(0)]]), Ok([[Integer(1), Text("sqlite_schema"), Integer(21), Null], [Integer(1), Text("sqlite_schema"), Integer(22), Null], [Integer(0), Text("sqlite_schema"), Integer(21), Text("{\"type\":0,\"name\":0,\"tbl_name\":0,\"rootpage\":0,\"sql\":1}")], [Integer(0), Text("sqlite_schema"), Integer(21), Text("{\"type\":0,\"name\":0,\"tbl_name\":0,\"rootpage\":0,\"sql\":1}")]]), Ok([[Integer(0)]]), Ok([[Integer(1), Integer(1), Text("x"), Real(1.0), Integer(2)], [Integer(2), Integer(2), Text("yy"), Real(2.5), Integer(4)], [Integer(3), Integer(3), Text("z"), Real(3.0), Integer(6)], [Integer(4), Null, Null, Null, Null], [Integer(5), Integer(5), Text("x"), Real(5.5), Integer(10)], [Integer(6), Integer(2), Text("w"), Real(-0.0), Integer(4)], [Integer(7), Integer(7), Text("yy"), Real(7.0), Integer(14)]]), Ok([[Integer(1), Integer(1), Text("x"), Real(1.0)], [Integer(2), Integer(2), Text("yy"), Real(-1.0)], [Integer(3), Integer(3), Text("z"), Real(3.0)], [Integer(4), Integer(4), Null, Null], [Integer(5), Integer(5), Text("p"), Real(5.0)], [Integer(6), Integer(6), Text("q"), Real(5.0)]]), Ok([]), Ok([[Text("ok")]])]"#
    );
}

/// CREATE TABLE ... AS SELECT plans its SELECT through HIR when HIR is on:
/// the new table's column names and types come from HIR's outputs, and its
/// rows from HIR's SELECT, matching legacy.
#[test]
fn hir_create_table_as_select_names_and_types_its_columns() {
    let statements = [
        "CREATE TABLE s(a INTEGER PRIMARY KEY, b TEXT, c REAL, d, e NUMERIC, f BLOB)",
        "INSERT INTO s VALUES (1, 'x', 1.5, NULL, 10, x'01'), (2, 'y', 2.5, 'q', 20, NULL), \
         (3, NULL, NULL, 3, 30, x'02')",
        "CREATE TABLE c1 AS SELECT * FROM s",
        "CREATE TABLE c2 AS SELECT a, b AS bee, c * 2, 1, 'lit', a + 1, a || b, \
         count(*) OVER () FROM s",
        "CREATE TABLE c3 AS SELECT b, count(*), sum(c), max(a) FROM s GROUP BY b ORDER BY b",
        "CREATE TABLE c4 AS SELECT a, a, b AS a FROM s",
        "CREATE TABLE c5 AS SELECT x.a, y.b FROM s x JOIN s y ON y.a = x.a + 1",
        "CREATE TABLE c6 AS SELECT a FROM s UNION SELECT c FROM s ORDER BY 1",
        "CREATE TABLE c7 AS WITH w AS (SELECT a, e FROM s WHERE a > 1) SELECT * FROM w",
        "CREATE TABLE c8 AS SELECT CAST(a AS TEXT) AS t, (SELECT max(c) FROM s) AS m, e FROM s",
        "CREATE TABLE c9 AS SELECT * FROM s WHERE 0",
        "CREATE TABLE c10 AS VALUES (1, 'a'), (2, 'b')",
        "SELECT name, sql FROM sqlite_schema WHERE name LIKE 'c%' ORDER BY name",
        "SELECT * FROM c2 ORDER BY a",
        "SELECT * FROM c3 ORDER BY b",
        "SELECT * FROM c4 ORDER BY 1",
        "SELECT * FROM c5 ORDER BY 1",
        "SELECT * FROM c6 ORDER BY 1",
        "SELECT * FROM c8 ORDER BY 1",
        "SELECT * FROM c10 ORDER BY 1",
        "SELECT typeof(a), typeof(b), typeof(e) FROM c1 ORDER BY a",
    ];
    assert_eq!(
        format!("{:?}", run_with_errors(&statements)),
        r#"[Ok([]), Ok([[Integer(7)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([[Text("c1"), Text("CREATE TABLE c1(a INT,b TEXT,c REAL,d,e NUM,f)")], [Text("c10"), Text("CREATE TABLE c10(column1,column2)")], [Text("c2"), Text("CREATE TABLE c2(a INT,bee TEXT,\"c * 2\",\"1\",\"'lit'\",\"a + 1\",\"a || b\",\"count(*) OVER ()\")")], [Text("c3"), Text("CREATE TABLE c3(b TEXT,\"count(*)\",\"sum(c)\",\"max(a)\")")], [Text("c4"), Text("CREATE TABLE c4(a INT,\"a:1\" INT,\"a:2\" TEXT)")], [Text("c5"), Text("CREATE TABLE c5(a INT,b TEXT)")], [Text("c6"), Text("CREATE TABLE c6(a INT)")], [Text("c7"), Text("CREATE TABLE c7(a INT,e NUM)")], [Text("c8"), Text("CREATE TABLE c8(t TEXT,m,e NUM)")], [Text("c9"), Text("CREATE TABLE c9(a INT,b TEXT,c REAL,d,e NUM,f)")]]), Ok([[Integer(3)]]), Ok([[Integer(1), Text("x"), Real(3.0), Integer(1), Text("lit"), Integer(2), Text("1x"), Integer(3)], [Integer(2), Text("y"), Real(5.0), Integer(1), Text("lit"), Integer(3), Text("2y"), Integer(3)], [Integer(3), Null, Null, Integer(1), Text("lit"), Integer(4), Null, Integer(3)]]), Ok([[Integer(3)]]), Ok([[Null, Integer(1), Null, Integer(3)], [Text("x"), Integer(1), Real(1.5), Integer(1)], [Text("y"), Integer(1), Real(2.5), Integer(2)]]), Ok([[Integer(3)]]), Ok([[Integer(1), Integer(1), Text("x")], [Integer(2), Integer(2), Text("y")], [Integer(3), Integer(3), Null]]), Ok([[Integer(3)]]), Ok([[Integer(1), Text("y")], [Integer(2), Null]]), Ok([[Integer(3)]]), Ok([[Null], [Integer(1)], [Real(1.5)], [Integer(2)], [Real(2.5)], [Integer(3)]]), Ok([[Integer(3)]]), Ok([[Text("1"), Real(2.5), Integer(10)], [Text("2"), Real(2.5), Integer(20)], [Text("3"), Real(2.5), Integer(30)]]), Ok([[Integer(3)]]), Ok([[Integer(1), Text("a")], [Integer(2), Text("b")]]), Ok([[Integer(3)]]), Ok([[Text("integer"), Text("text"), Text("integer")], [Text("integer"), Text("text"), Text("integer")], [Text("integer"), Text("null"), Text("integer")]]), Ok([[Integer(3)]]), Ok([[Integer(1), Integer(1), Text("x"), Real(1.0), Integer(2)], [Integer(2), Integer(2), Text("yy"), Real(2.5), Integer(4)], [Integer(3), Integer(3), Text("z"), Real(3.0), Integer(6)], [Integer(4), Null, Null, Null, Null], [Integer(5), Integer(5), Text("x"), Real(5.5), Integer(10)], [Integer(6), Integer(2), Text("w"), Real(-0.0), Integer(4)], [Integer(7), Integer(7), Text("yy"), Real(7.0), Integer(14)]]), Ok([[Integer(1), Integer(1), Text("x"), Real(1.0)], [Integer(2), Integer(2), Text("yy"), Real(-1.0)], [Integer(3), Integer(3), Text("z"), Real(3.0)], [Integer(4), Integer(4), Null, Null], [Integer(5), Integer(5), Text("p"), Real(5.0)], [Integer(6), Integer(6), Text("q"), Real(5.0)]]), Ok([]), Ok([[Text("ok")]])]"#
    );
}

/// CREATE INDEX and REINDEX fill the index from a SELECT planned through
/// HIR: plain, expression, partial and generated-column indexes, collations,
/// a table whose `rowid` name is taken, and a UNIQUE index that fails.
#[test]
fn hir_create_index_and_reindex_fill_the_index() {
    let statements = [
        "CREATE TABLE x(id INTEGER PRIMARY KEY, a TEXT, b INT, c REAL, v INT AS (a || '') VIRTUAL)",
        "INSERT INTO x(id, a, b, c) VALUES (1, 'B', 3, 1.5), (2, 'a', 1, NULL), (3, NULL, 2, 2.5), \
         (4, '12', NULL, 0), (5, 'A', 5, 9)",
        "CREATE INDEX x1 ON x(a COLLATE NOCASE)",
        "CREATE INDEX x2 ON x(b * 10 + 1) WHERE c > 1",
        "CREATE INDEX x3 ON x(v, id)",
        "CREATE INDEX x4 ON x(id, b DESC)",
        "CREATE INDEX x5 ON x(lower(a)) WHERE a IS NOT NULL AND x.b < 4",
        "CREATE TABLE r(rowid TEXT, z)",
        "INSERT INTO r VALUES ('r1', 10), ('r2', 20)",
        "CREATE INDEX r1 ON r(z, rowid)",
        "REINDEX",
        "REINDEX NOCASE",
        "REINDEX x2",
        "CREATE UNIQUE INDEX bad ON x(b % 2)",
        "SELECT a FROM x INDEXED BY x1 WHERE a > 'a' ORDER BY a COLLATE NOCASE",
        "SELECT id FROM x INDEXED BY x2 WHERE b * 10 + 1 > 0 AND c > 1",
        "SELECT v, typeof(v), id FROM x INDEXED BY x3 WHERE v IS NOT NULL",
        "SELECT id, b FROM x INDEXED BY x4 WHERE id > 0",
        "SELECT lower(a) FROM x INDEXED BY x5 WHERE lower(a) > '' AND a IS NOT NULL AND x.b < 4",
        "SELECT z, rowid FROM r INDEXED BY r1 WHERE z > 0",
        "SELECT name FROM sqlite_schema WHERE type = 'index' ORDER BY name",
        "PRAGMA integrity_check",
    ];
    assert_eq!(
        format!("{:?}", run_with_errors(&statements)),
        r#"[Ok([]), Ok([[Integer(7)]]), Ok([]), Ok([[Integer(5)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(2)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Err("UNIQUE constraint failed: x.b % 2"), Ok([[Integer(0)]]), Ok([]), Ok([[Integer(0)]]), Ok([[Integer(3)], [Integer(1)], [Integer(5)]]), Ok([[Integer(0)]]), Ok([[Integer(12), Text("integer"), Integer(4)], [Text("A"), Text("text"), Integer(5)], [Text("B"), Text("text"), Integer(1)], [Text("a"), Text("text"), Integer(2)]]), Ok([[Integer(0)]]), Ok([[Integer(1), Integer(3)], [Integer(2), Integer(1)], [Integer(3), Integer(2)], [Integer(4), Null], [Integer(5), Integer(5)]]), Ok([[Integer(0)]]), Ok([[Text("a")], [Text("b")]]), Ok([[Integer(0)]]), Ok([[Integer(10), Text("r1")], [Integer(20), Text("r2")]]), Ok([[Integer(0)]]), Ok([[Text("r1")], [Text("t_b")], [Text("t_c_desc")], [Text("t_d")], [Text("t_expr")], [Text("t_g")], [Text("t_partial")], [Text("t_unique")], [Text("u_b")], [Text("u_c")], [Text("u_expr")], [Text("u_partial")], [Text("u_partial_unique")], [Text("x1")], [Text("x2")], [Text("x3")], [Text("x4")], [Text("x5")]]), Ok([[Integer(0)]]), Ok([[Text("ok")]]), Ok([[Integer(0)]]), Ok([[Integer(1), Integer(1), Text("x"), Real(1.0), Integer(2)], [Integer(2), Integer(2), Text("yy"), Real(2.5), Integer(4)], [Integer(3), Integer(3), Text("z"), Real(3.0), Integer(6)], [Integer(4), Null, Null, Null, Null], [Integer(5), Integer(5), Text("x"), Real(5.5), Integer(10)], [Integer(6), Integer(2), Text("w"), Real(-0.0), Integer(4)], [Integer(7), Integer(7), Text("yy"), Real(7.0), Integer(14)]]), Ok([[Integer(1), Integer(1), Text("x"), Real(1.0)], [Integer(2), Integer(2), Text("yy"), Real(-1.0)], [Integer(3), Integer(3), Text("z"), Real(3.0)], [Integer(4), Integer(4), Null, Null], [Integer(5), Integer(5), Text("p"), Real(5.0)], [Integer(6), Integer(6), Text("q"), Real(5.0)]]), Ok([]), Ok([[Text("ok")]])]"#
    );
}

/// ALTER TABLE ADD COLUMN checks the new column's CHECK constraints against
/// every existing row, through a SELECT planned by HIR: the new column reads
/// its default, and other columns read the row, as in SQLite. Legacy reads
/// another column's name as text, so `c > b` compares 3 with 'b'.
#[test]
fn hir_alter_add_column_checks_each_row_like_sqlite() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE t(a, b)",
        "INSERT INTO t VALUES (1, 1), (2, 2)",
        "ALTER TABLE t ADD COLUMN c INT DEFAULT 3 CHECK (c > b)",
        "ALTER TABLE t ADD COLUMN d INT DEFAULT 3 CHECK (d < 10) CHECK (t.d + b > 4)",
        "ALTER TABLE t ADD COLUMN e INT DEFAULT 3 CHECK (e + b < 5)",
        "INSERT INTO t(a, b) VALUES (3, 9)",
        "ALTER TABLE t ADD COLUMN f INT DEFAULT 3 CHECK (f > b)",
        "CREATE TABLE empty(a)",
        "ALTER TABLE empty ADD COLUMN c INT DEFAULT 3 CHECK (c > 4)",
        "SELECT * FROM t ORDER BY a",
        "SELECT * FROM empty",
    ] {
        assert_matches_sqlite(&db, &conn, &sqlite, sql);
    }
}

/// ATTACH and DETACH evaluate their arguments through HIR, so an argument
/// may be any expression, a subquery too (legacy rejects one).
#[test]
fn hir_attach_evaluates_its_arguments() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_attach(true))
        .build();
    let conn = db.connect_limbo();
    let dir = db
        .path
        .parent()
        .unwrap()
        .to_str()
        .unwrap()
        .replace('\'', "''");
    for sql in [
        format!("ATTACH '{dir}/' || 'aux1.db' AS 'a' || '1'"),
        format!("ATTACH (SELECT '{dir}/aux2.db') AS a2"),
        "CREATE TABLE a1.x(v)".to_string(),
        "INSERT INTO a1.x VALUES (1)".to_string(),
        "DETACH 'a' || '2'".to_string(),
    ] {
        conn.execute(&sql).unwrap();
    }
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT name FROM pragma_database_list ORDER BY seq"),
        vec![
            vec![rusqlite::types::Value::Text("main".into())],
            vec![rusqlite::types::Value::Text("a1".into())],
        ]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT v FROM a1.x"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
}

/// PRAGMA integrity_check and quick_check read each table's rows through a
/// SELECT planned by HIR, and find the same broken NOT NULL and CHECK
/// constraints and missing or repeated index entries as legacy.
#[test]
fn hir_integrity_check_finds_each_kind_of_corruption() {
    let fixtures =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../sqlite/conformance/database");
    let check = |fixture: &str| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(fixture);
        std::fs::copy(fixtures.join(fixture), &path).unwrap();
        let db = TempDatabase::new_with_existent_with_opts(
            &path,
            turso_core::DatabaseOpts::new().with_generated_columns(true),
        );
        let conn = db.connect_limbo();
        ["PRAGMA integrity_check", "PRAGMA quick_check"].map(|sql| limbo_exec_rows(&conn, sql))
    };
    for fixture in [
        "integrity_check_constraint_violation.db",
        "integrity_gencol_not_null_violation.db",
        "integrity_missing_expression_index_entry.db",
        "integrity_missing_index_entry.db",
        "integrity_missing_partial_index_entry.db",
        "integrity_missing_unique_index_entry.db",
        "integrity_non_unique_index_entry.db",
        "integrity_not_null_violation.db",
    ] {
        let ok = vec![vec![rusqlite::types::Value::Text("ok".into())]];
        let [check, quick] = check(fixture);
        assert_ne!(check, ok, "{fixture} is broken");
        // quick_check skips the index checks, so only a row that breaks a
        // constraint of its own is found twice.
        if fixture.contains("violation") {
            assert_ne!(quick, ok, "{fixture} is broken for quick_check too");
        }
    }
}

/// Like legacy, a DELETE or UPDATE whose WHERE is an OR of terms that
/// indexes can each seek reads the table through one seek per branch. The
/// branches give rowids in rowid order through a row set, so the statement
/// collects them first, then writes each row once.
#[test]
fn hir_dml_with_or_terms_seeks_each_branch() {
    for (sql, plan) in [
        (
            "DELETE FROM u WHERE b = 2 OR c = 'z'",
            "MULTI-INDEX OR u (u_b, u_c)",
        ),
        (
            "UPDATE u SET d = 9 WHERE b = 1 OR c = 'q'",
            "MULTI-INDEX OR u (u_b, u_c)",
        ),
        // A branch reads the index the UPDATE changes.
        (
            "UPDATE u SET b = b + 10 WHERE b = 3 OR c = 'p'",
            "MULTI-INDEX OR u (u_b, u_c)",
        ),
        (
            "DELETE FROM u WHERE (b = 1 OR c = 'yy') AND d < 2",
            "MULTI-INDEX OR u (u_b, u_c)",
        ),
        (
            "UPDATE u SET d = 0 WHERE b = 2 OR c = 'x' RETURNING a, d",
            "MULTI-INDEX OR u (u_b, u_c)",
        ),
        (
            "DELETE FROM t WHERE (b = 2 AND d > 0) OR (d = 3 AND c = 'z')",
            "MULTI-INDEX OR t (t_b, t_unique)",
        ),
        (
            "DELETE FROM t WHERE b = 2 OR g = 10",
            "MULTI-INDEX OR t (t_b, t_g)",
        ),
        (
            "DELETE FROM t WHERE b = 5 OR b + length(c) = 3",
            "MULTI-INDEX OR t (t_b, t_expr)",
        ),
    ] {
        assert_eq!(query_plan(sql), vec![plan.to_string()], "{sql}");
        assert_run_matches_sqlite(&[sql]);
    }
    for statements in [
        &[
            "CREATE TRIGGER ad AFTER DELETE ON u BEGIN INSERT INTO log VALUES (old.a); END",
            "DELETE FROM u WHERE b = 2 OR c = 'z'",
        ][..],
        &[
            "CREATE TRIGGER au AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (new.d); END",
            "UPDATE OR REPLACE u SET b = 1 WHERE b = 3 OR c = 'q'",
        ],
    ] {
        assert_run_matches_sqlite(statements);
    }
}

/// Like legacy, a DELETE or UPDATE whose WHERE term matches an index
/// method's query pattern, such as `fts_match`, reads its rows through the
/// index. The index may stream rows lazily, so the statement collects their
/// rowids first, then writes each row.
#[test]
fn hir_dml_through_an_index_method_collects_rowids_first() {
    const FTS_SETUP: &[&str] = &[
        "CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT, n INT)",
        "CREATE INDEX docs_fts ON docs USING fts (body)",
        "CREATE INDEX docs_n ON docs(n)",
        "INSERT INTO docs VALUES (1, 'red apple', 1), (2, 'green apple', 2), \
         (3, 'red pear', 3), (4, 'apple apple', 4), (5, 'plum', 5)",
    ];
    let run = |statements: &[&str]| -> (Vec<String>, Vec<Result<Rows, String>>) {
        let db = TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
            .build();
        let conn = db.connect_limbo();
        for statement in FTS_SETUP {
            conn.execute(statement).unwrap();
        }
        let mut plans = Vec::new();
        let mut results = Vec::new();
        for statement in statements {
            if statement.starts_with("DELETE") || statement.starts_with("UPDATE") {
                for row in limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {statement}")) {
                    let Some(rusqlite::types::Value::Text(detail)) = row.last() else {
                        panic!("EXPLAIN QUERY PLAN detail is not text: {row:?}");
                    };
                    plans.push(detail.clone());
                }
            }
            results
                .push(limbo_exec_rows_fallible(&db, &conn, statement).map_err(|e| e.to_string()));
            results.push(Ok(limbo_exec_rows(&conn, "SELECT changes()")));
        }
        for sql in [
            "SELECT id, body, n FROM docs ORDER BY id",
            "SELECT id FROM docs WHERE fts_match(body, 'apple') ORDER BY id",
            "SELECT id FROM docs WHERE fts_match(body, 'pear') ORDER BY id",
            "PRAGMA integrity_check",
        ] {
            results.push(Ok(limbo_exec_rows(&conn, sql)));
        }
        (plans, results)
    };
    for (statements, expected) in [
        (
            vec!["DELETE FROM docs WHERE fts_match(body, 'apple')"],
            r#"[Ok([]), Ok([[Integer(3)]]), Ok([[Integer(3), Text("red pear"), Integer(3)], [Integer(5), Text("plum"), Integer(5)]]), Ok([]), Ok([[Integer(3)]]), Ok([[Text("ok")]])]"#,
        ),
        (
            vec!["DELETE FROM docs WHERE fts_match(body, 'apple') AND n > 1"],
            r#"[Ok([]), Ok([[Integer(2)]]), Ok([[Integer(1), Text("red apple"), Integer(1)], [Integer(3), Text("red pear"), Integer(3)], [Integer(5), Text("plum"), Integer(5)]]), Ok([[Integer(1)]]), Ok([[Integer(3)]]), Ok([[Text("ok")]])]"#,
        ),
        (
            vec!["UPDATE docs SET body = 'pear', n = n + 10 WHERE fts_match(body, 'apple')"],
            r#"[Ok([]), Ok([[Integer(3)]]), Ok([[Integer(1), Text("pear"), Integer(11)], [Integer(2), Text("pear"), Integer(12)], [Integer(3), Text("red pear"), Integer(3)], [Integer(4), Text("pear"), Integer(14)], [Integer(5), Text("plum"), Integer(5)]]), Ok([]), Ok([[Integer(1)], [Integer(2)], [Integer(3)], [Integer(4)]]), Ok([[Text("ok")]])]"#,
        ),
        (
            vec!["UPDATE docs SET n = 0 WHERE fts_match(body, 'red') RETURNING id, n"],
            r#"[Ok([[Integer(1), Integer(0)], [Integer(3), Integer(0)]]), Ok([[Integer(2)]]), Ok([[Integer(1), Text("red apple"), Integer(0)], [Integer(2), Text("green apple"), Integer(2)], [Integer(3), Text("red pear"), Integer(0)], [Integer(4), Text("apple apple"), Integer(4)], [Integer(5), Text("plum"), Integer(5)]]), Ok([[Integer(1)], [Integer(2)], [Integer(4)]]), Ok([[Integer(3)]]), Ok([[Text("ok")]])]"#,
        ),
        (
            vec!["DELETE FROM docs WHERE fts_match(body, 'apple') RETURNING id"],
            r#"[Ok([[Integer(1)], [Integer(2)], [Integer(4)]]), Ok([[Integer(3)]]), Ok([[Integer(3), Text("red pear"), Integer(3)], [Integer(5), Text("plum"), Integer(5)]]), Ok([]), Ok([[Integer(3)]]), Ok([[Text("ok")]])]"#,
        ),
        (
            vec!["CREATE TRIGGER ad AFTER DELETE ON docs BEGIN UPDATE docs SET n = n + 100 WHERE id = old.id + 1; END", "DELETE FROM docs WHERE fts_match(body, 'red')"],
            r#"[Ok([]), Ok([[Integer(5)]]), Ok([]), Ok([[Integer(2)]]), Ok([[Integer(2), Text("green apple"), Integer(102)], [Integer(4), Text("apple apple"), Integer(104)], [Integer(5), Text("plum"), Integer(5)]]), Ok([[Integer(2)], [Integer(4)]]), Ok([]), Ok([[Text("ok")]])]"#,
        ),
        (
            vec!["DELETE FROM docs WHERE fts_match(body, 'apple')", "UPDATE docs SET body = 'apple' WHERE fts_match(body, 'plum')", "DELETE FROM docs WHERE fts_match(body, 'apple')"],
            r#"[Ok([]), Ok([[Integer(3)]]), Ok([]), Ok([[Integer(1)]]), Ok([]), Ok([[Integer(1)]]), Ok([[Integer(3), Text("red pear"), Integer(3)]]), Ok([]), Ok([[Integer(3)]]), Ok([[Text("ok")]])]"#,
        ),
    ] {
        let (plans, results) = run(&statements);
        assert!(
            plans.iter().all(|row| row == "QUERY INDEX METHOD fts"),
            "{statements:?}: {plans:?}"
        );
        assert_eq!(format!("{results:?}"), expected, "{statements:?}");
    }
}

/// A virtual column of a custom type holds the value its expression
/// computes from the decoded columns it reads: that value is what the
/// column shows, what its index stores, and what a seek on the index looks
/// for. Legacy decodes the computed value once more, so `cents AS (a * 2)`
/// reads 0 while its index holds 4 and a seek looks for 400.
#[test]
fn hir_custom_typed_virtual_columns_keep_their_computed_value() {
    let db = TempDatabase::builder()
        .with_opts(
            turso_core::DatabaseOpts::new()
                .with_custom_types(true)
                .with_generated_columns(true),
        )
        .build();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a cents, v cents AS (a * 2), w INTEGER AS (a + 1)) \
         STRICT",
        "CREATE INDEX tv ON t(v)",
        "INSERT INTO t VALUES (1, 2), (2, 3)",
        "UPDATE t SET a = 5 WHERE id = 1",
        "INSERT INTO t VALUES (1, 7) ON CONFLICT DO UPDATE SET a = excluded.a",
    ] {
        limbo_exec_rows(&conn, sql);
    }
    let rows = |sql| {
        limbo_exec_rows(&conn, sql)
            .into_iter()
            .map(|row| format!("{row:?}"))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rows("SELECT id, a, v, w FROM t ORDER BY id"),
        [
            "[Integer(1), Integer(7), Integer(14), Integer(8)]",
            "[Integer(2), Integer(3), Integer(6), Integer(4)]"
        ]
    );
    assert_eq!(
        rows("SELECT id FROM t INDEXED BY tv WHERE v = 14"),
        ["[Integer(1)]"]
    );
    assert_eq!(
        rows("SELECT id FROM t WHERE v BETWEEN 5 AND 7"),
        ["[Integer(2)]"]
    );
    limbo_exec_rows(&conn, "DELETE FROM t WHERE v = 14");
    assert_eq!(rows("SELECT id FROM t"), ["[Integer(2)]"]);
    assert_eq!(rows("PRAGMA integrity_check"), ["[Text(\"ok\")]"]);
}
