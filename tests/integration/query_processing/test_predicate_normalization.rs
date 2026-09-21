use crate::assertions::AssertQueryPlan;
use crate::common::{limbo_exec_rows, TempDatabase};
use asserting::prelude::*;

/// The planner converts a predicate to conjunctive normal form before it
/// splits the predicate into conditions. A condition below a NOT or below an
/// OR then becomes a condition of its own, which the optimizer can turn into
/// an index seek. Each test below scans the table without that conversion.
fn database() -> std::sync::Arc<turso_core::Connection> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    for statement in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER, y INTEGER)",
        "CREATE INDEX ta ON t(a)",
        "CREATE INDEX tb ON t(b)",
        "INSERT INTO t VALUES (1,1,1,10,20),(2,1,NULL,10,0),(3,NULL,2,0,20),(4,7,7,10,20),\
         (5,3,9,0,0),(6,NULL,NULL,10,20),(7,0,0,0,0)",
        "CREATE TABLE u(id INTEGER PRIMARY KEY, k INTEGER)",
        "CREATE INDEX uk ON u(k)",
        "INSERT INTO u VALUES (1,1),(2,7),(3,NULL),(4,3)",
        "CREATE TABLE p(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
        "CREATE INDEX pa ON p(a) WHERE a IS NOT NULL AND b IS NOT NULL",
        "INSERT INTO p VALUES (1,5,5),(2,5,NULL),(3,NULL,5)",
    ] {
        limbo_exec_rows(&conn, statement);
    }
    conn
}

#[test]
fn ranges_below_a_not_seek_an_index() {
    let conn = database();
    let predicate = "NOT (a > 5 OR b > 5)";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN SELECT id FROM t WHERE {predicate}"),
    ))
    .searches_table("t");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("SELECT id FROM t WHERE {predicate} ORDER BY id"),
    ))
    .is_equal_to(vec![row![1], row![7]]);
}

#[test]
fn a_not_over_two_inequalities_uses_both_indexes() {
    let conn = database();
    let predicate = "NOT (a <> 1 AND b <> 2)";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN SELECT id FROM t WHERE {predicate}"),
    ))
    .has_step_containing("MULTI-INDEX OR");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("SELECT id FROM t WHERE {predicate} ORDER BY id"),
    ))
    .is_equal_to(vec![row![1], row![2], row![3]]);
}

/// The optimizer already lifts a condition that every OR branch shares, but
/// only when the branches are ANDs. A NOT above the OR hides them until the
/// NOT moves in.
#[test]
fn a_shared_condition_below_a_not_seeks_an_index() {
    let conn = database();
    let predicate = "NOT ((a <> 1 OR x <> 10) AND (a <> 1 OR y <> 20))";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN SELECT id FROM t WHERE {predicate}"),
    ))
    .uses_index("ta");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("SELECT id FROM t WHERE {predicate} ORDER BY id"),
    ))
    .is_equal_to(vec![row![1], row![2]]);
}

#[test]
fn a_join_condition_below_a_not_drives_an_index_seek() {
    let conn = database();
    let predicate = "NOT (t.a <> u.k OR t.x > 5)";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN SELECT t.id, u.id FROM t, u WHERE {predicate}"),
    ))
    .uses_index("uk");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("SELECT t.id, u.id FROM t, u WHERE {predicate} ORDER BY t.id, u.id"),
    ))
    .is_equal_to(vec![row![5, 4]]);
}

#[test]
fn a_not_in_list_below_a_not_seeks_an_index() {
    let conn = database();
    let predicate = "NOT (a NOT IN (1, 3) OR x > 5)";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN SELECT id FROM t WHERE {predicate}"),
    ))
    .uses_index("ta");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("SELECT id FROM t WHERE {predicate} ORDER BY id"),
    ))
    .is_equal_to(vec![row![5]]);
}

#[test]
fn a_not_between_below_a_not_seeks_a_range() {
    let conn = database();
    let predicate = "NOT (a NOT BETWEEN 2 AND 8 OR x > 5)";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN SELECT id FROM t WHERE {predicate}"),
    ))
    .uses_index("ta");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("SELECT id FROM t WHERE {predicate} ORDER BY id"),
    ))
    .is_equal_to(vec![row![5]]);
}

/// A partial index is usable only when a condition of the query implies each
/// condition of the index. The index WHERE clause and the query WHERE clause
/// now reach that comparison in the same form.
#[test]
fn a_null_test_below_a_not_matches_a_partial_index() {
    let conn = database();
    let predicate = "a = 5 AND NOT (a IS NULL OR b IS NULL)";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN SELECT id FROM p WHERE {predicate}"),
    ))
    .uses_index("pa");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("SELECT id FROM p WHERE {predicate} ORDER BY id"),
    ))
    .is_equal_to(vec![row![1]]);
}

/// Distribution would turn these two branches into four clauses of two
/// literals, which hides the branches from the scan that reads one index per
/// branch. The predicate keeps its shape instead.
#[test]
fn or_branches_that_read_one_index_each_keep_their_plan() {
    let conn = database();
    let predicate = "(a = 1 AND x = 10) OR (b = 2 AND y = 20)";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN SELECT id FROM t WHERE {predicate}"),
    ))
    .has_step_containing("MULTI-INDEX OR");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("SELECT id FROM t WHERE {predicate} ORDER BY id"),
    ))
    .is_equal_to(vec![row![1], row![2], row![3]]);
}

/// The planner already sees that the whole NOT term rejects a null-extended
/// row, so it reads `t` first. What it could not do is read `t` through an
/// index, because `t.b = 1 OR t.b = 7` was still hidden below the NOT.
#[test]
fn a_condition_below_a_not_seeks_the_table_of_a_left_join() {
    let conn = database();
    let query = "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.k \
                 WHERE NOT (t.b <> 1 AND t.b <> 7)";

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {query}"),
    ))
    .has_step_containing("MULTI-INDEX OR");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("{query} ORDER BY u.id, t.id")
    ))
    .is_equal_to(vec![row![1, 1], row![2, 4]]);
}
