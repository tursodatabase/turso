use std::sync::Arc;

use crate::assertions::{AssertQueryPlan, PlanDetails};
use crate::common::{limbo_exec_rows, TempDatabase};
use asserting::prelude::*;

/// Return the rows of `EXPLAIN QUERY PLAN`.
fn explain(
    connection: &Arc<turso_core::Connection>,
    sql: &str,
) -> Vec<Vec<rusqlite::types::Value>> {
    limbo_exec_rows(connection, &format!("EXPLAIN QUERY PLAN {sql}"))
}

/// Compute the same average once per key, not once per outer row.
#[test]
fn correlated_average_uses_one_grouped_table() -> anyhow::Result<()> {
    let database =
        TempDatabase::new_with_rusqlite("CREATE TABLE outer_rows(id INT, key1 INT, amount INT)");
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE inner_rows(key1 INT, amount INT)")?;

    let details = explain(
        &connection,
        "SELECT id FROM outer_rows o
         WHERE amount < (
             SELECT 0.2 * avg(i.amount)
             FROM inner_rows i
             WHERE i.key1 = o.key1
         )",
    );

    assert_that!(details)
        .has_step_containing("scalar_subquery")
        .has_step_containing("GROUP BY")
        .has_no_step_containing("CORRELATED");
    Ok(())
}

/// Keep one index search when the outer query reads one row.
#[test]
fn indexed_average_for_one_outer_row_stays_a_subquery() -> anyhow::Result<()> {
    let database = TempDatabase::new_with_rusqlite(
        "CREATE TABLE outer_rows(id INTEGER PRIMARY KEY, amount INT)",
    );
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE inner_rows(key1 INT, amount INT)")?;
    connection.execute("CREATE INDEX inner_rows_key1 ON inner_rows(key1)")?;

    let details = explain(
        &connection,
        "SELECT id FROM outer_rows o
         WHERE o.id = 1
           AND amount < (
               SELECT avg(i.amount)
               FROM inner_rows i
               WHERE i.key1 = o.id
           )",
    );

    assert_that!(details)
        .has_step_containing("CORRELATED")
        .has_step_containing("inner_rows_key1 (key1=?)");
    Ok(())
}

/// A TPC-H Q2 query should compute its minimum values once.
#[test]
fn minimum_over_a_join_becomes_a_joined_table() -> anyhow::Result<()> {
    let database = TempDatabase::new_with_rusqlite(
        "CREATE TABLE partsupp(partkey INT, suppkey INT, supplycost INT)",
    );
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE supplier(suppkey INT, region INT)")?;

    let details = explain(
        &connection,
        "SELECT ps.partkey, ps.suppkey
         FROM partsupp ps JOIN supplier s ON s.suppkey = ps.suppkey
         WHERE ps.supplycost = (
             SELECT min(ps2.supplycost)
             FROM partsupp ps2 JOIN supplier s2 ON s2.suppkey = ps2.suppkey
             WHERE ps2.partkey = ps.partkey AND s2.region = 1
         )",
    );

    assert_that!(details)
        .has_step_containing("scalar_subquery")
        .has_no_step_containing("CORRELATED");
    Ok(())
}

/// A grouped table can use a NULL outer value and a text order.
#[test]
fn grouped_table_can_use_null_and_text_order() -> anyhow::Result<()> {
    let database = TempDatabase::new_with_rusqlite("CREATE TABLE outer_rows(id INT, key1 INT)");
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE outer_key2(id INT, key2 INT)")?;
    connection.execute("CREATE TABLE inner_rows(key1 INT, key2 INT, amount INT)")?;
    connection.execute("CREATE TABLE nocase_outer(key1 TEXT COLLATE NOCASE)")?;
    connection.execute("CREATE TABLE nocase_inner(key1 TEXT COLLATE NOCASE, amount INT)")?;

    let queries = [
        "SELECT o.id, (SELECT count(*) FROM inner_rows i WHERE i.key2 = x.key2)
         FROM outer_rows o LEFT JOIN outer_key2 x ON x.id = o.id",
        "SELECT (SELECT avg(i.amount) FROM nocase_inner i WHERE o.key1 = i.key1)
         FROM nocase_outer o",
    ];

    for query in queries {
        assert_that!(explain(&connection, query))
            .named(format!("query plan of {query}"))
            .has_step_containing("scalar_subquery")
            .has_no_step_containing("CORRELATED");
    }
    Ok(())
}

/// A direct positive `IN` can use the inner table as a semi-join.
#[test]
fn correlated_in_uses_a_semi_join() -> anyhow::Result<()> {
    let database =
        TempDatabase::new_with_rusqlite("CREATE TABLE outer_rows(id INT, key1 INT, amount INT)");
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE inner_rows(key1 INT, amount INT)")?;

    let details = explain(
        &connection,
        "SELECT id FROM outer_rows o
         WHERE amount IN (
             SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1
         )",
    );

    assert_that!(details.plan_details())
        .named("query plan")
        .any_satisfies(|step| step.contains("inner_rows") && step.contains("key1=?"));
    assert_that!(details).has_no_step_containing("CORRELATED");
    Ok(())
}

/// An indexed `EXISTS` keeps the index search after it becomes a semi-join.
#[test]
fn indexed_correlated_exists_uses_a_semi_join() -> anyhow::Result<()> {
    let database =
        TempDatabase::new_with_rusqlite("CREATE TABLE outer_rows(id INTEGER PRIMARY KEY)");
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE inner_rows(outer_id INT, value INT)")?;
    connection.execute("CREATE INDEX inner_rows_outer_id ON inner_rows(outer_id)")?;

    let details = explain(
        &connection,
        "SELECT id FROM outer_rows o
         WHERE EXISTS (
             SELECT 1 FROM inner_rows i
             WHERE i.outer_id = o.id AND i.value > 10
         )",
    );

    assert_that!(details)
        .has_no_step_containing("CORRELATED")
        .has_step_containing("inner_rows_outer_id (outer_id=?)");
    Ok(())
}

/// Cases that the rewrite does not support must stay as subqueries.
#[test]
fn subqueries_that_cannot_be_rewritten_stay_correlated() -> anyhow::Result<()> {
    let database =
        TempDatabase::new_with_rusqlite("CREATE TABLE outer_rows(id INT, key1 INT, amount INT)");
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE inner_rows(key1 INT, amount INT)")?;
    connection.execute("CREATE TABLE outer_key2(id INT, key2 INT)")?;
    connection.execute("CREATE TABLE text_inner(key1 TEXT, amount INT)")?;
    connection.execute("CREATE TABLE nocase_outer(key1 TEXT COLLATE NOCASE)")?;
    connection.execute("CREATE TABLE binary_inner(key1 TEXT COLLATE BINARY, amount INT)")?;

    let queries = [
        "SELECT id FROM outer_rows o WHERE amount <
         (SELECT avg(i.amount) FROM inner_rows i WHERE i.key1 < o.key1)",
        "SELECT id FROM outer_rows o WHERE amount =
         (SELECT max(i.amount) FROM inner_rows i WHERE i.key1 IS o.key1)",
        "SELECT id FROM outer_rows o WHERE amount =
         (SELECT coalesce(sum(i.amount), 0) FROM inner_rows i WHERE i.key1 = o.key1)",
        "SELECT id FROM outer_rows o WHERE amount NOT IN
         (SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1)",
        "SELECT id FROM outer_rows o WHERE id = 1 OR amount IN
         (SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1)",
        "SELECT id FROM outer_rows o WHERE amount =
         (SELECT sum(i.amount) + random() FROM inner_rows i WHERE i.key1 = o.key1)",
        "SELECT id FROM outer_rows o WHERE random() IN
         (SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1)",
        "SELECT id FROM outer_rows o WHERE amount =
         (SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1)",
        "SELECT id FROM outer_rows o WHERE amount =
         (SELECT sum(i.amount) FROM inner_rows i WHERE i.key1 = o.key1 GROUP BY i.key1)",
        "SELECT o.id FROM outer_rows o LEFT JOIN outer_key2 x
         ON x.key2 = (SELECT min(i.amount) FROM inner_rows i WHERE i.key1 = o.key1)",
        "SELECT (SELECT sum(i.amount) FROM text_inner i WHERE i.key1 = o.key1)
         FROM outer_rows o",
        "SELECT (SELECT sum(i.amount) FROM binary_inner i WHERE o.key1 = i.key1)
         FROM nocase_outer o",
    ];

    for query in queries {
        assert_that!(explain(&connection, query))
            .named(format!("query plan of {query}"))
            .has_step_containing("CORRELATED");
    }
    Ok(())
}

/// NULL values and repeated keys must give the same rows as SQLite.
#[test]
fn one_value_and_in_results_match_sqlite_on_nulls_and_repeated_keys() -> anyhow::Result<()> {
    let database =
        TempDatabase::new_with_rusqlite("CREATE TABLE outer_rows(id INT, key1 INT, amount INT)");
    let connection = database.connect_limbo();
    connection.execute("CREATE TABLE inner_rows(key1 INT, amount INT)")?;
    connection
        .execute("INSERT INTO outer_rows VALUES (1,1,10),(2,1,11),(3,2,6),(4,3,9),(5,NULL,NULL)")?;
    connection
        .execute("INSERT INTO inner_rows VALUES (1,10),(1,10),(1,20),(2,6),(2,NULL),(NULL,NULL)")?;

    assert_that!(limbo_exec_rows(
        &connection,
        "SELECT id,
                coalesce((SELECT avg(i.amount) FROM inner_rows i WHERE i.key1 = o.key1), -1.0),
                (SELECT count(*) FROM inner_rows i WHERE i.key1 = o.key1)
         FROM outer_rows o ORDER BY id",
    ))
    .is_equal_to(vec![
        row![1, 40.0 / 3.0, 3],
        row![2, 40.0 / 3.0, 3],
        row![3, 6.0, 2],
        row![4, -1.0, 0],
        row![5, -1.0, 0],
    ]);

    assert_that!(limbo_exec_rows(
        &connection,
        "SELECT id FROM outer_rows o
         WHERE amount IN (SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1)
         ORDER BY id",
    ))
    .is_equal_to(vec![row![1], row![3]]);
    Ok(())
}
