use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;
use std::sync::Arc;
use turso_core::Connection;

/// The three TPC-H tables that query 11 joins, with their real column counts.
/// The optimizer reads the column count and the primary keys, so a plan test
/// needs the real shape. It does not read the rows, so the tables stay empty.
const TPCH_SCHEMA: [&str; 3] = [
    "CREATE TABLE nation (
        n_nationkey INTEGER PRIMARY KEY NOT NULL,
        n_name      TEXT NOT NULL,
        n_regionkey INTEGER NOT NULL,
        n_comment   TEXT)",
    "CREATE TABLE supplier (
        s_suppkey   INTEGER PRIMARY KEY NOT NULL,
        s_name      TEXT NOT NULL,
        s_address   TEXT NOT NULL,
        s_nationkey INTEGER NOT NULL,
        s_phone     TEXT NOT NULL,
        s_acctbal   INTEGER NOT NULL,
        s_comment   TEXT NOT NULL)",
    "CREATE TABLE partsupp (
        ps_partkey    INTEGER NOT NULL,
        ps_suppkey    INTEGER NOT NULL,
        ps_availqty   INTEGER NOT NULL,
        ps_supplycost INTEGER NOT NULL,
        ps_comment    TEXT NOT NULL,
        PRIMARY KEY (ps_partkey, ps_suppkey))",
];

const TPCH_QUERY_11: &str = "SELECT ps_partkey, sum(ps_supplycost * ps_availqty)
     FROM partsupp, supplier, nation
     WHERE ps_suppkey = s_suppkey
       AND s_nationkey = n_nationkey
       AND n_name = 'ARGENTINA'
     GROUP BY ps_partkey";

/// Without `ANALYZE` the optimizer has no row counts, so the fallback
/// parameters in `cost_params.rs` decide this plan on their own. Building an
/// in-memory index used to be priced with `cpu_cost_per_seek`, at 0.01 of a
/// page read per key comparison. Under that number the optimizer copied
/// `partsupp` into an in-memory index and hash-joined `supplier`, which is
/// 6 times slower than seeking the primary key of each row.
/// `ephemeral_index_build_cost` prices the copy above the seeks.
#[test]
fn join_without_statistics_seeks_primary_keys_instead_of_copying_a_table() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    for ddl in TPCH_SCHEMA {
        limbo_exec_rows(&conn, ddl);
    }

    let plan = query_plan(&conn, TPCH_QUERY_11);

    assert!(
        plan.iter()
            .any(|step| step.contains("SEARCH supplier USING INTEGER PRIMARY KEY")),
        "supplier must be reached by its primary key, plan was:\n{}",
        plan.join("\n")
    );
    assert!(
        plan.iter()
            .any(|step| step.contains("SEARCH nation USING INTEGER PRIMARY KEY")),
        "nation must be reached by its primary key, plan was:\n{}",
        plan.join("\n")
    );
    assert!(
        !plan.iter().any(|step| step.contains("ephemeral_partsupp")),
        "partsupp must not be copied into an in-memory index, plan was:\n{}",
        plan.join("\n")
    );
}

fn query_plan(conn: &Arc<Connection>, query: &str) -> Vec<String> {
    limbo_exec_rows(conn, &format!("EXPLAIN QUERY PLAN {query}"))
        .into_iter()
        .map(|row| match row.last() {
            Some(Value::Text(step)) => step.to_string(),
            other => panic!("query plan step must be text, got {other:?}"),
        })
        .collect()
}
