//! INSERT, UPDATE and DELETE inside WITH, enabled with `writable_ctes` in the
//! SQLite dialect. The semantics are PostgreSQL's; the PostgreSQL corpus in
//! postgres/conformance/pg-sqltests/writable_cte.sqltest and the differential
//! fuzzer in postgres/differential compare them with a real PostgreSQL server.
//! These tests cover what those cannot: statement atomicity inside explicit
//! transactions, statement reset, change counters, SQLite-only features and
//! the statements the engine rejects.

use crate::common::TempDatabase;
use std::sync::Arc;
use turso_core::vdbe::StepResult;
use turso_core::Value;

fn exec(conn: &Arc<turso_core::Connection>, sql: &str) -> turso_core::Result<()> {
    let mut stmt = conn.prepare(sql)?;
    stmt.run_ignore_rows()
}

fn rows(conn: &Arc<turso_core::Connection>, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values: Vec<String> = row.get_values().map(|value| format!("{value}")).collect();
        rows.push(values.join("|"));
        Ok(())
    })
    .unwrap();
    rows
}

fn error(conn: &Arc<turso_core::Connection>, sql: &str) -> String {
    match conn
        .prepare(sql)
        .and_then(|mut stmt| stmt.run_ignore_rows())
    {
        Ok(()) => panic!("expected an error from: {sql}"),
        Err(error) => error.to_string(),
    }
}

fn setup(conn: &Arc<turso_core::Connection>) {
    exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    exec(conn, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    exec(conn, "CREATE TABLE u (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
}

#[turso_macros::test]
fn rejected_without_the_flag(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
    );
    assert!(
        message.contains("--experimental-writable-ctes"),
        "unexpected error: {message}"
    );
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["3"]);
}

#[turso_macros::test(mvcc, writable_ctes)]
fn moves_rows_and_reads_the_table_as_of_statement_start(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    assert_eq!(
        rows(
            &conn,
            "WITH up AS (UPDATE t SET v = v + 1 RETURNING id, v)
             SELECT t.id, t.v, up.v FROM t JOIN up ON t.id = up.id ORDER BY t.id"
        ),
        vec!["1|10|11", "2|20|21", "3|30|31"]
    );
    exec(
        &conn,
        "WITH d AS (DELETE FROM t WHERE id >= 2 RETURNING *) INSERT INTO u SELECT * FROM d",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM t"), vec!["1|11"]);
    assert_eq!(rows(&conn, "SELECT * FROM u"), vec!["2|21", "3|31"]);
    assert_eq!(
        rows(&conn, "PRAGMA integrity_check"),
        vec!["ok".to_string()]
    );
}

/// The copy that a part reads keeps the rowids of the table.
#[turso_macros::test(mvcc, writable_ctes)]
fn rowid_reads_through_a_copy(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    exec(&conn, "CREATE TABLE r (a TEXT, b INTEGER)").unwrap();
    exec(
        &conn,
        "INSERT INTO r(rowid, a, b) VALUES (5, 'x', 1), (9, 'y', 2)",
    )
    .unwrap();
    assert_eq!(
        rows(
            &conn,
            "WITH d AS (DELETE FROM r WHERE rowid = 5 RETURNING rowid)
             SELECT r.rowid, r.a, (SELECT count(*) FROM d) FROM r ORDER BY r.rowid"
        ),
        vec!["5|x|1", "9|y|1"]
    );
    assert_eq!(rows(&conn, "SELECT rowid, a FROM r"), vec!["9|y"]);
}

#[turso_macros::test(mvcc, writable_ctes)]
fn a_row_changed_by_one_part_is_not_changed_by_another(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    // The unread CTE runs after the main UPDATE and skips the row it changed.
    assert_eq!(
        rows(
            &conn,
            "WITH a AS (UPDATE t SET v = 300 WHERE id = 3 RETURNING *)
             UPDATE t SET v = 400 WHERE id >= 2 RETURNING id, v"
        )
        .len(),
        2
    );
    assert_eq!(rows(&conn, "SELECT v FROM t WHERE id = 3"), vec!["400"]);
    // Rows the statement inserted are not deleted by a later part.
    exec(
        &conn,
        "WITH d AS (DELETE FROM u) INSERT INTO u VALUES (7, 70)",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM u"), vec!["7|70"]);
}

#[turso_macros::test(mvcc, writable_ctes)]
fn upsert_of_a_row_the_statement_changed_fails(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    let message = error(
        &conn,
        "WITH a AS (INSERT INTO t VALUES (9, 1) RETURNING *)
         INSERT INTO t SELECT 9, 2 FROM a ON CONFLICT (id) DO UPDATE SET v = excluded.v",
    );
    assert!(
        message.contains("cannot affect row a second time"),
        "unexpected error: {message}"
    );
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM t WHERE id = 9"),
        vec!["0"]
    );
}

/// A part that fails rolls back the writes of the parts that ran before it,
/// and only those: the earlier statements of the transaction stay.
#[turso_macros::test(mvcc, writable_ctes)]
fn a_failing_part_rolls_back_the_whole_statement_in_a_transaction(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(&conn, "BEGIN").unwrap();
    exec(&conn, "INSERT INTO u VALUES (100, 1)").unwrap();
    // The CTE runs first and deletes every row of t; the main INSERT then
    // fails on a duplicate key in u.
    error(
        &conn,
        "WITH d AS (DELETE FROM t RETURNING *) INSERT INTO u SELECT id, v FROM d UNION ALL SELECT 100, 2",
    );
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["3"]);
    assert_eq!(rows(&conn, "SELECT * FROM u"), vec!["100|1"]);
    exec(&conn, "COMMIT").unwrap();
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["3"]);
    assert_eq!(rows(&conn, "SELECT * FROM u"), vec!["100|1"]);
}

#[turso_macros::test(mvcc, writable_ctes)]
fn a_failing_part_rolls_back_the_whole_statement_in_autocommit(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    error(
        &conn,
        "WITH d AS (DELETE FROM t RETURNING *), i AS (INSERT INTO u SELECT * FROM d RETURNING *)
         INSERT INTO t VALUES (1, 1)",
    );
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["3"]);
    assert_eq!(rows(&conn, "SELECT count(*) FROM u"), vec!["0"]);
}

/// Every write finishes before the first row is returned, so resetting the
/// statement after one row keeps all the writes, like PostgreSQL.
#[turso_macros::test(mvcc, writable_ctes)]
fn reset_after_the_first_row_keeps_all_writes(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    for sql in [
        "WITH d AS (DELETE FROM t RETURNING id) SELECT id FROM d",
        "WITH d AS (DELETE FROM u RETURNING id) INSERT INTO u VALUES (1, 1), (2, 2), (3, 3) RETURNING id",
    ] {
        let mut stmt = conn.prepare(sql).unwrap();
        loop {
            match stmt.step().unwrap() {
                StepResult::IO => stmt._io().step().unwrap(),
                StepResult::Row => break,
                other => panic!("expected a row from {sql}, got {other:?}"),
            }
        }
        stmt.reset().unwrap();
    }
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["0"]);
    assert_eq!(rows(&conn, "SELECT count(*) FROM u"), vec!["3"]);
}

/// changes() counts the rows of the main statement only, like the command tag
/// of PostgreSQL; total_changes() counts the rows of every part.
#[turso_macros::test(mvcc, writable_ctes)]
fn changes_count_the_main_statement(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    let total_before = conn.total_changes();
    exec(
        &conn,
        "WITH d AS (DELETE FROM t WHERE id <= 2) INSERT INTO u VALUES (1, 1)",
    )
    .unwrap();
    assert_eq!(conn.changes(), 1);
    assert_eq!(conn.total_changes() - total_before, 3);
}

/// The unread CTE runs after the main INSERT, so its row is the last insert.
#[turso_macros::test(mvcc, writable_ctes)]
fn last_insert_rowid_is_the_last_insert_of_any_part(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(
        &conn,
        "WITH a AS (INSERT INTO u VALUES (50, 1)) INSERT INTO t VALUES (60, 1)",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT last_insert_rowid()"), vec!["50"]);
    exec(
        &conn,
        "WITH a AS (INSERT INTO u VALUES (51, 1) RETURNING id) INSERT INTO t SELECT 61, id FROM a",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT last_insert_rowid()"), vec!["61"]);
}

#[turso_macros::test(mvcc, writable_ctes)]
fn total_changes_count_the_parts_of_a_select(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(&conn, "INSERT INTO u VALUES (1, 1)").unwrap();
    let changes_before = conn.changes();
    let total_before = conn.total_changes();
    assert_eq!(
        rows(
            &conn,
            "WITH d AS (DELETE FROM t WHERE id <= 2 RETURNING id) SELECT count(*) FROM d"
        ),
        vec!["2"]
    );
    assert_eq!(conn.total_changes() - total_before, 2);
    assert_eq!(conn.changes(), changes_before);
}

/// REPLACE deletes the row it conflicts with, which can be a row another part
/// wrote, and PostgreSQL never changes a row twice in one statement.
#[turso_macros::test(mvcc, writable_ctes)]
fn replace_on_a_table_that_another_part_writes_is_rejected(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(
        &conn,
        "CREATE TABLE r (id INTEGER PRIMARY KEY, k INTEGER UNIQUE ON CONFLICT REPLACE)",
    )
    .unwrap();
    exec(&conn, "INSERT INTO r VALUES (1, 1), (2, 2)").unwrap();
    for sql in [
        "WITH a AS (UPDATE t SET v = 5 WHERE id = 1 RETURNING *) REPLACE INTO t SELECT id, 99 FROM a",
        "WITH a AS (UPDATE t SET v = 5 WHERE id = 1 RETURNING *) INSERT OR REPLACE INTO t SELECT id, 99 FROM a",
        "WITH a AS (DELETE FROM t WHERE id = 3) UPDATE OR REPLACE t SET id = 2 WHERE id = 1",
        "WITH a AS (UPDATE r SET k = 5 WHERE id = 1) INSERT INTO r VALUES (3, 5)",
    ] {
        let message = error(&conn, sql);
        assert!(
            message.contains("REPLACE"),
            "unexpected error for {sql}: {message}"
        );
    }
    assert_eq!(rows(&conn, "SELECT * FROM t"), vec!["1|10", "2|20", "3|30"]);
    assert_eq!(rows(&conn, "SELECT * FROM r"), vec!["1|1", "2|2"]);
    // OR IGNORE overrides the REPLACE of the constraint.
    exec(
        &conn,
        "WITH a AS (DELETE FROM r WHERE id = 1) INSERT OR IGNORE INTO r VALUES (3, 2)",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM r"), vec!["2|2"]);
    // REPLACE on a table that no other part writes runs.
    exec(
        &conn,
        "WITH a AS (DELETE FROM u) REPLACE INTO t VALUES (1, 11)",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT v FROM t WHERE id = 1"), vec!["11"]);
}

/// A CTE that another part reads runs to completion before that part, which
/// is one of the orders PostgreSQL can use. PostgreSQL 17 runs this statement
/// in another order: it updates row 1 first and the DELETE then skips it.
#[turso_macros::test(mvcc, writable_ctes)]
fn a_cte_that_another_part_reads_changes_rows_first(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    assert_eq!(
        rows(
            &conn,
            "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id)
             UPDATE t SET v = 0 WHERE id = 1 OR id IN (SELECT id FROM d) RETURNING id"
        ),
        Vec::<String>::new()
    );
    assert_eq!(rows(&conn, "SELECT * FROM t"), vec!["2|20", "3|30"]);
    assert_eq!(
        rows(
            &conn,
            "WITH up AS (UPDATE t SET v = 1 RETURNING id)
             DELETE FROM t WHERE id IN (SELECT id FROM up) OR id = 3 RETURNING id"
        ),
        Vec::<String>::new()
    );
    assert_eq!(rows(&conn, "SELECT * FROM t"), vec!["2|1", "3|1"]);
}

/// Like PostgreSQL, ON CONFLICT DO UPDATE fails on a row that the same part
/// already inserted or updated.
#[turso_macros::test(mvcc, writable_ctes)]
fn upsert_of_a_row_the_same_part_wrote_fails(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    let message = error(
        &conn,
        "WITH x AS (INSERT INTO t VALUES (7, 1), (7, 2) ON CONFLICT (id) DO UPDATE SET v = excluded.v RETURNING id)
         SELECT * FROM x",
    );
    assert!(
        message.contains("cannot affect row a second time"),
        "unexpected error: {message}"
    );
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM u)
         INSERT INTO t VALUES (1, 1), (1, 2) ON CONFLICT (id) DO UPDATE SET v = excluded.v",
    );
    assert!(
        message.contains("cannot affect row a second time"),
        "unexpected error: {message}"
    );
    assert_eq!(rows(&conn, "SELECT * FROM t"), vec!["1|10", "2|20", "3|30"]);
}

/// Change data capture records one COMMIT for the whole statement, after the
/// changes of every part. A COMMIT after each part would let a consumer of
/// the changes apply part of the statement.
#[turso_macros::test(mvcc, writable_ctes)]
fn change_data_capture_records_one_commit_per_statement(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(&conn, "PRAGMA capture_data_changes_conn('id')").unwrap();
    exec(
        &conn,
        "WITH d AS (DELETE FROM t WHERE id <= 2 RETURNING *) INSERT INTO u SELECT * FROM d",
    )
    .unwrap();
    assert_eq!(
        rows(
            &conn,
            "SELECT change_type, table_name, id FROM turso_cdc ORDER BY change_id"
        ),
        vec!["-1|t|1", "-1|t|2", "1|u|1", "1|u|2", "2||"]
    );
}

#[turso_macros::test(writable_ctes)]
fn tables_with_triggers_are_rejected(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(
        &conn,
        "CREATE TRIGGER tr AFTER DELETE ON t BEGIN INSERT INTO u VALUES (old.id, old.v); END",
    )
    .unwrap();
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
    );
    assert!(message.contains("triggers"), "unexpected error: {message}");
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["3"]);
}

#[turso_macros::test(writable_ctes)]
fn dml_inside_with_must_be_at_the_top_level(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    for sql in [
        "SELECT * FROM (WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d)",
        "WITH x AS (WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d) SELECT * FROM x",
        "INSERT INTO u WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
        "SELECT (WITH d AS (DELETE FROM t RETURNING v) SELECT max(v) FROM d)",
    ] {
        let message = error(&conn, sql);
        assert!(
            message.contains("must be at the top level"),
            "unexpected error for {sql}: {message}"
        );
    }
    for sql in [
        "CREATE VIEW v AS WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
        "CREATE TRIGGER tr AFTER INSERT ON u BEGIN INSERT INTO t SELECT * FROM (WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d); END",
    ] {
        let message = error(&conn, sql);
        assert!(
            message.contains("not allowed in a view or trigger"),
            "unexpected error for {sql}: {message}"
        );
    }
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["3"]);
}

#[turso_macros::test(writable_ctes)]
fn reading_a_cte_without_returning_fails(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    let message = error(&conn, "WITH d AS (DELETE FROM t) SELECT * FROM d");
    assert!(
        message.contains("does not have a RETURNING clause"),
        "unexpected error: {message}"
    );
    let message = error(
        &conn,
        "WITH RECURSIVE d AS (DELETE FROM t WHERE id IN (SELECT id FROM d) RETURNING *) SELECT * FROM d",
    );
    assert!(
        message.contains("must not contain data-modifying statements"),
        "unexpected error: {message}"
    );
}

#[turso_macros::test(writable_ctes)]
fn query_only_rejects_a_select_with_dml_inside_with(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(&conn, "PRAGMA query_only = 1").unwrap();
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
    );
    assert!(
        message.contains("query_only"),
        "unexpected error: {message}"
    );
    exec(&conn, "PRAGMA query_only = 0").unwrap();
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["3"]);
}

/// A prepared statement is prepared again from its SQL text after a schema
/// change, so the parser must accept DML inside WITH.
#[turso_macros::test(mvcc, writable_ctes)]
fn statement_runs_after_a_schema_change(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    let mut stmt = conn
        .prepare(
            "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING v) INSERT INTO u SELECT 1, v FROM d",
        )
        .unwrap();
    exec(&conn, "CREATE INDEX t_v ON t(v)").unwrap();
    stmt.run_ignore_rows().unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM u"), vec!["1|10"]);
}

#[turso_macros::test(writable_ctes)]
fn explain_shows_the_program(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    let program = rows(
        &conn,
        "EXPLAIN WITH up AS (UPDATE t SET v = 0 RETURNING id) SELECT t.v FROM t JOIN up ON t.id = up.id",
    );
    assert!(program.iter().any(|row| row.contains("OpenDup")));
    assert_eq!(rows(&conn, "SELECT sum(v) FROM t"), vec!["60"]);
}

#[turso_macros::test(writable_ctes)]
fn foreign_key_actions_that_reach_another_part_are_rejected(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    exec(&conn, "PRAGMA foreign_keys = ON").unwrap();
    exec(&conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &conn,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, v INTEGER)",
    )
    .unwrap();
    exec(&conn, "INSERT INTO parent VALUES (1), (2)").unwrap();
    exec(&conn, "INSERT INTO child VALUES (10, 1, 0), (20, 2, 0)").unwrap();
    // PostgreSQL runs the cascade after both parts; the engine would run it
    // inside the DELETE, so it rejects the statement.
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM parent WHERE id = 1) INSERT INTO child VALUES (30, 1, 0)",
    );
    assert!(
        message.contains("a foreign key action of"),
        "unexpected error: {message}"
    );
    assert_eq!(rows(&conn, "SELECT count(*) FROM child"), vec!["2"]);
    // When no other part writes the child table, the cascade runs.
    assert_eq!(
        rows(
            &conn,
            "WITH d AS (DELETE FROM parent WHERE id = 1 RETURNING id) SELECT count(*) FROM child"
        ),
        vec!["2"]
    );
    assert_eq!(rows(&conn, "SELECT id FROM child"), vec!["20"]);
}

/// Like PostgreSQL, foreign keys are checked after every part, so the order of
/// the parts does not matter. Only a violation left at the end fails.
#[turso_macros::test(mvcc, writable_ctes)]
fn foreign_keys_hold_between_parts(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    exec(&conn, "PRAGMA foreign_keys = ON").unwrap();
    exec(&conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &conn,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    exec(&conn, "INSERT INTO parent VALUES (1), (2)").unwrap();
    exec(&conn, "INSERT INTO child VALUES (10, 1)").unwrap();
    exec(
        &conn,
        "WITH p AS (INSERT INTO parent VALUES (4) RETURNING id) INSERT INTO child SELECT 40, id FROM p",
    )
    .unwrap();
    exec(
        &conn,
        "WITH c AS (DELETE FROM child WHERE id = 10 RETURNING pid)
         DELETE FROM parent WHERE id IN (SELECT pid FROM c)",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT id FROM parent"), vec!["2", "4"]);
    assert_eq!(rows(&conn, "SELECT * FROM child"), vec!["40|4"]);
    // The main statement runs first and needs a row of the CTE that runs after it.
    exec(
        &conn,
        "WITH p AS (INSERT INTO parent VALUES (3)) INSERT INTO child VALUES (30, 3)",
    )
    .unwrap();
    exec(
        &conn,
        "WITH c AS (DELETE FROM child WHERE id = 40) DELETE FROM parent WHERE id = 4",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT id FROM parent"), vec!["2", "3"]);
    assert_eq!(rows(&conn, "SELECT * FROM child"), vec!["30|3"]);
    for sql in [
        "WITH c AS (INSERT INTO child VALUES (50, 5) RETURNING id) SELECT * FROM c",
        "WITH d AS (DELETE FROM parent WHERE id = 3) INSERT INTO child VALUES (60, 2)",
    ] {
        let message = error(&conn, sql);
        assert!(
            message.contains("FOREIGN KEY"),
            "unexpected error for {sql}: {message}"
        );
    }
    assert_eq!(rows(&conn, "SELECT id FROM parent"), vec!["2", "3"]);
    assert_eq!(rows(&conn, "SELECT * FROM child"), vec!["30|3"]);
}

/// Deleting a parent counts one violation per child row, also through an index
/// on the child column, so repairing one of two children is not enough.
#[turso_macros::test(mvcc, writable_ctes)]
fn foreign_key_violations_are_counted_per_child_row(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    exec(&conn, "PRAGMA foreign_keys = ON").unwrap();
    exec(&conn, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &conn,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, a INTEGER REFERENCES p(id))",
    )
    .unwrap();
    exec(&conn, "CREATE INDEX c_a ON c(a)").unwrap();
    exec(&conn, "INSERT INTO p VALUES (40)").unwrap();
    exec(&conn, "INSERT INTO c VALUES (9, 40), (13, 40)").unwrap();
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM c WHERE id = 13) DELETE FROM p",
    );
    assert!(message.contains("FOREIGN KEY"), "unexpected error: {message}");
    assert_eq!(rows(&conn, "SELECT count(*) FROM p"), vec!["1"]);
    assert_eq!(rows(&conn, "SELECT count(*) FROM c"), vec!["2"]);
    exec(&conn, "WITH d AS (DELETE FROM c) DELETE FROM p").unwrap();
    assert_eq!(rows(&conn, "SELECT count(*) FROM p"), vec!["0"]);
}

/// Each execution of a prepared statement starts with no tracked rows and
/// takes fresh copies of the tables.
#[turso_macros::test(mvcc, writable_ctes)]
fn re_executing_with_new_bindings(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    // The unread CTE runs after the main UPDATE and skips the rows it changed.
    let mut stmt = conn
        .prepare(
            "WITH a AS (UPDATE t SET v = v + 1 WHERE id = ?1) UPDATE t SET v = 0 WHERE id = ?2",
        )
        .unwrap();
    for (a_id, main_id) in [(1, 1), (1, 2)] {
        stmt.reset().unwrap();
        stmt.bind_at(1.try_into().unwrap(), Value::from_i64(a_id))
            .unwrap();
        stmt.bind_at(2.try_into().unwrap(), Value::from_i64(main_id))
            .unwrap();
        stmt.run_ignore_rows().unwrap();
    }
    // The first execution set row 1 to 0 and skipped it in the CTE; the second
    // must not remember that and adds 1 to it.
    assert_eq!(rows(&conn, "SELECT * FROM t"), vec!["1|1", "2|0", "3|30"]);

    let mut stmt = conn
        .prepare("WITH d AS (DELETE FROM t WHERE id = ?1 RETURNING id) SELECT count(*) FROM t, d")
        .unwrap();
    let mut counts = Vec::new();
    for id in [1, 2] {
        stmt.reset().unwrap();
        stmt.bind_at(1.try_into().unwrap(), Value::from_i64(id))
            .unwrap();
        stmt.run_with_row_callback(|row| {
            counts.push(format!("{}", row.get_values().next().unwrap()));
            Ok(())
        })
        .unwrap();
    }
    // Each execution reads t as it was when that execution started.
    assert_eq!(counts, vec!["3", "2"]);

    let mut stmt = conn
        .prepare(
            "WITH x AS (INSERT INTO u VALUES (?1, 1), (?2, 2) ON CONFLICT (id) DO UPDATE SET v = excluded.v RETURNING id)
             SELECT count(*) FROM x",
        )
        .unwrap();
    for (first, second, fails) in [(7, 7, true), (8, 9, false)] {
        stmt.reset().unwrap();
        stmt.bind_at(1.try_into().unwrap(), Value::from_i64(first))
            .unwrap();
        stmt.bind_at(2.try_into().unwrap(), Value::from_i64(second))
            .unwrap();
        assert_eq!(stmt.run_ignore_rows().is_err(), fails, "{first}, {second}");
    }
    assert_eq!(rows(&conn, "SELECT id FROM u"), vec!["8", "9"]);
}

#[turso_macros::test(writable_ctes)]
fn rejected_statements_change_nothing(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(&conn, "CREATE VIEW tv AS SELECT * FROM t").unwrap();
    exec(
        &conn,
        "CREATE TEMP TRIGGER tt AFTER UPDATE ON u BEGIN SELECT 1; END",
    )
    .unwrap();
    for (sql, expected) in [
        (
            "WITH d AS (DELETE FROM tv RETURNING *) SELECT * FROM d",
            "can only change ordinary tables",
        ),
        (
            "WITH a AS (UPDATE u SET v = 1) DELETE FROM t",
            "which has triggers",
        ),
        (
            "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING *), d AS (SELECT 1) SELECT * FROM d",
            "duplicate WITH table name",
        ),
        (
            "WITH RECURSIVE a AS (SELECT id, v FROM b), b AS (DELETE FROM t WHERE id IN (SELECT id FROM a) RETURNING id, v) SELECT * FROM a",
            "mutual recursion",
        ),
        (
            "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d(1)",
            "is not a function",
        ),
    ] {
        let message = error(&conn, sql);
        assert!(
            message.contains(expected),
            "unexpected error for {sql}: {message}"
        );
    }
    exec(&conn, "PRAGMA require_where = 1").unwrap();
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
    );
    assert!(
        message.contains("require_where"),
        "unexpected error: {message}"
    );
    exec(&conn, "PRAGMA require_where = 0").unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM t"), vec!["1|10", "2|20", "3|30"]);
    assert_eq!(rows(&conn, "SELECT count(*) FROM u"), vec!["0"]);
}

/// A trigger on a table that a foreign key action of a part changes runs
/// inside that part, so it is rejected like a trigger on the target.
#[turso_macros::test(writable_ctes)]
fn trigger_on_a_table_changed_by_a_foreign_key_action_is_rejected(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    exec(&conn, "PRAGMA foreign_keys = ON").unwrap();
    exec(&conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &conn,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(&conn, "CREATE TABLE log (x)").unwrap();
    exec(
        &conn,
        "CREATE TRIGGER child_gone AFTER DELETE ON child BEGIN INSERT INTO log VALUES (old.id); END",
    )
    .unwrap();
    exec(&conn, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&conn, "INSERT INTO child VALUES (10, 1)").unwrap();
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM parent RETURNING id) SELECT * FROM d",
    );
    assert!(
        message.contains("which has triggers"),
        "unexpected error: {message}"
    );
    assert_eq!(rows(&conn, "SELECT count(*) FROM child"), vec!["1"]);
}

/// The statement's CDC COMMIT record also follows the parts of a SELECT.
#[turso_macros::test(mvcc, writable_ctes)]
fn change_data_capture_with_a_select_main_statement(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(&conn, "PRAGMA capture_data_changes_conn('id')").unwrap();
    assert_eq!(
        rows(
            &conn,
            "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id) SELECT * FROM d"
        ),
        vec!["1"]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT change_type, table_name, id FROM turso_cdc ORDER BY change_id"
        ),
        vec!["-1|t|1", "2||"]
    );
}

/// A failing statement rolls back only its own writes; the savepoint before
/// it still rolls back the earlier ones.
#[turso_macros::test(mvcc, writable_ctes)]
fn failing_statement_inside_a_savepoint(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(&conn, "BEGIN").unwrap();
    exec(&conn, "SAVEPOINT s").unwrap();
    exec(&conn, "INSERT INTO u VALUES (1, 1)").unwrap();
    error(
        &conn,
        "WITH d AS (DELETE FROM t RETURNING *) INSERT INTO u SELECT id, v FROM d",
    );
    assert_eq!(rows(&conn, "SELECT count(*) FROM t"), vec!["3"]);
    assert_eq!(rows(&conn, "SELECT * FROM u"), vec!["1|1"]);
    exec(&conn, "ROLLBACK TO s").unwrap();
    assert_eq!(rows(&conn, "SELECT count(*) FROM u"), vec!["0"]);
    exec(
        &conn,
        "WITH d AS (DELETE FROM t WHERE id = 3 RETURNING *) INSERT INTO u SELECT id, v FROM d",
    )
    .unwrap();
    exec(&conn, "COMMIT").unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM t"), vec!["1|10", "2|20"]);
    assert_eq!(rows(&conn, "SELECT * FROM u"), vec!["3|30"]);
}

#[turso_macros::test(writable_ctes)]
fn tables_of_an_attached_database(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    let aux_path = tmp_db
        .path
        .parent()
        .unwrap()
        .join("aux_writable_cte.db")
        .to_string_lossy()
        .to_string();
    exec(&conn, &format!("ATTACH '{aux_path}' AS aux1")).unwrap();
    exec(
        &conn,
        "CREATE TABLE aux1.t (id INTEGER PRIMARY KEY, v INTEGER)",
    )
    .unwrap();
    exec(&conn, "INSERT INTO aux1.t VALUES (1, 100), (2, 200)").unwrap();
    // The CTE writes aux1.t, which the main SELECT reads as of statement
    // start; main.t, with the same name, is read as it is.
    assert_eq!(
        rows(
            &conn,
            "WITH up AS (UPDATE aux1.t SET v = 0 RETURNING id)
             SELECT (SELECT sum(v) FROM aux1.t), (SELECT sum(v) FROM main.t), (SELECT count(*) FROM up)"
        ),
        vec!["300|60|2"]
    );
    exec(
        &conn,
        "WITH d AS (DELETE FROM aux1.t WHERE id = 1 RETURNING id) INSERT INTO t SELECT id + 10, 1 FROM d",
    )
    .unwrap();
    assert_eq!(rows(&conn, "SELECT * FROM aux1.t"), vec!["2|0"]);
    assert_eq!(rows(&conn, "SELECT id FROM t WHERE id > 3"), vec!["11"]);
}

/// A table that a part reads as it was when the statement started has no
/// indexes, so INDEXED BY cannot be used on it.
#[turso_macros::test(writable_ctes)]
fn indexed_by_on_a_table_read_as_of_statement_start(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    setup(&conn);
    exec(&conn, "CREATE INDEX t_v ON t(v)").unwrap();
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id) SELECT t.* FROM t INDEXED BY t_v, d WHERE v > 0",
    );
    assert!(
        message.contains("as it was when the statement started"),
        "unexpected error: {message}"
    );
    assert_eq!(
        rows(
            &conn,
            "WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id) SELECT t.id FROM t NOT INDEXED, d WHERE v > 0"
        ),
        vec!["1", "2", "3"]
    );
}

/// CREATE TABLE, CREATE VIEW and CREATE TRIGGER never store INSERT, UPDATE or
/// DELETE inside WITH, but a hand-edited schema can. Renaming a table then
/// leaves that SQL as it is instead of crashing.
#[turso_macros::test]
fn renaming_a_table_with_dml_inside_with_in_a_hand_edited_check(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    exec(&conn, "CREATE TABLE p (id INTEGER PRIMARY KEY)").unwrap();
    exec(&conn, "CREATE TABLE t (a INTEGER CHECK (a > 0))").unwrap();
    let edit_schema = "UPDATE sqlite_schema SET sql = 'CREATE TABLE t (a INTEGER CHECK (a > (WITH d AS (DELETE FROM p RETURNING id) SELECT count(*) FROM d)))' WHERE name = 't'";
    // Prepared as a nested statement, which may write sqlite_schema.
    conn.start_nested();
    let stmt = conn.prepare(edit_schema);
    conn.end_nested();
    stmt.unwrap().run_ignore_rows().unwrap();
    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::new_with_existent(&path);
    let conn = reopened.connect_limbo();
    exec(&conn, "ALTER TABLE t RENAME TO t2").unwrap();
    assert_eq!(
        rows(&conn, "SELECT sql FROM sqlite_schema WHERE name = 't2'"),
        vec!["CREATE TABLE t2 (a INTEGER CHECK (a > (WITH d AS (DELETE FROM p RETURNING id) SELECT count (*) FROM d)))"]
    );
}

#[turso_macros::test(writable_ctes)]
fn without_rowid_targets_are_rejected(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    exec(
        &conn,
        "CREATE TABLE w (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
    )
    .unwrap();
    let message = error(
        &conn,
        "WITH d AS (DELETE FROM w RETURNING *) SELECT * FROM d",
    );
    assert!(
        message.contains("WITHOUT ROWID"),
        "unexpected error: {message}"
    );
}
