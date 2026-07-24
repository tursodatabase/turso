use crate::common::TempDatabase;
use turso_core::{Statement, Value};

const EXPLAIN_COLUMNS: [&str; 1] = ["QUERY PLAN"];

fn run_explain(statement: &mut Statement) -> Vec<Vec<Value>> {
    assert_eq!(statement.num_columns(), EXPLAIN_COLUMNS.len());
    for (index, expected) in EXPLAIN_COLUMNS.into_iter().enumerate() {
        assert_eq!(statement.get_column_name(index), expected);
    }

    let rows = statement.run_collect_rows().unwrap();
    assert!(rows.iter().all(|row| row.len() == EXPLAIN_COLUMNS.len()));
    rows
}

#[turso_macros::test(mvcc)]
fn postgres_explain_returns_query_plan_column(db: TempDatabase) {
    let conn = db.connect_postgres();
    let mut statement = conn.prepare("EXPLAIN SELECT 1").unwrap();

    assert!(!run_explain(&mut statement).is_empty());
}

#[turso_macros::test(mvcc)]
fn postgres_explain_reprepares_with_the_postgres_dialect(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (value int)").unwrap();
    conn.execute("INSERT INTO items VALUES (41)").unwrap();

    // PostgreSQL-only cast syntax ensures that schema invalidation sends the
    // original EXPLAIN command back through the PostgreSQL translator.
    let mut statement = conn
        .prepare("EXPLAIN SELECT (value + 1)::text FROM items")
        .unwrap();

    let other = db.connect_postgres();
    other
        .execute("CREATE TABLE invalidates_schema (id int)")
        .unwrap();

    // MVCC shares schema invalidation between connections and therefore
    // exercises reprepare here. Non-MVCC databases keep connection-local
    // schema state, but must still execute the command successfully.
    assert!(!run_explain(&mut statement).is_empty());
}
