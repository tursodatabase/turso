use super::common::{limbo_exec_rows, TempDatabase};

fn count_orders(conn: &std::sync::Arc<turso_core::Connection>) -> i64 {
    match limbo_exec_rows(conn, "SELECT COUNT(*) FROM orders")[0][0] {
        rusqlite::types::Value::Integer(n) => n,
        ref other => panic!("unexpected count value: {other:?}"),
    }
}

#[test]
fn failed_commit_with_materialized_view_leaves_no_rows_behind() {
    let tmp_db = TempDatabase::builder().with_views(true).build();
    let conn = tmp_db.connect_limbo();
    conn.execute(
        "CREATE TABLE orders (collection TEXT NOT NULL, key TEXT NOT NULL, \
         value TEXT NOT NULL, PRIMARY KEY (collection, key))",
    )
    .unwrap();
    conn.execute(
        "CREATE MATERIALIZED VIEW mv AS SELECT json_extract(value,'$.customer') AS grp, \
         SUM(json_extract(value,'$.amount')) AS agg FROM orders \
         WHERE collection='o' GROUP BY grp",
    )
    .unwrap();

    let mut committed = 0i64;
    let mut commit_error = None;
    for start in (0..60_000).step_by(1000) {
        conn.execute("BEGIN").unwrap();
        for i in start..start + 1000 {
            let key = format!("order-{i:05}");
            let value = format!(
                r#"{{"customer":"c{}","amount":{}}}"#,
                i % 316,
                (i % 97) as f64 + 0.5
            );
            let mut stmt = conn
                .prepare(
                    "INSERT OR REPLACE INTO orders (collection, key, value) VALUES ('o', ?, ?)",
                )
                .unwrap();
            stmt.bind_at(1.try_into().unwrap(), turso_core::Value::build_text(key))
                .unwrap();
            stmt.bind_at(2.try_into().unwrap(), turso_core::Value::build_text(value))
                .unwrap();
            stmt.run_ignore_rows().unwrap();
        }
        match conn.execute("COMMIT") {
            Ok(()) => committed += 1000,
            Err(e) => {
                commit_error = Some(e.to_string());
                break;
            }
        }
    }

    let Some(commit_error) = commit_error else {
        return;
    };

    let same_conn_rows = count_orders(&conn);
    let fresh = tmp_db.connect_limbo();
    let fresh_rows = count_orders(&fresh);
    assert_eq!(
        (same_conn_rows, fresh_rows),
        (committed, committed),
        "rows from the transaction whose COMMIT failed ({commit_error}) are still visible \
         (same connection, fresh connection)"
    );
}
