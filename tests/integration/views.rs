use super::alter_table::{column_names, schema_with_a_table_t_in_two_namespaces, select};
use super::common::{ExecRows, TempDatabase};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use turso_core::namespace::NamespaceId;
use turso_core::schema::view::{extract_view_columns, referenced_tables};
use turso_core::ConnectOpts;
use turso_core::SessionRole;
use turso_sqlite::SqliteDialect;

#[test]
fn concurrent_view_expansion_is_not_spuriously_circular() {
    let tmp_db = TempDatabase::builder().with_views(true).build();
    {
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute(
            "CREATE VIEW v AS SELECT id FROM t a \
             WHERE NOT EXISTS (SELECT 1 FROM t b WHERE b.id = a.id + 1)",
        )
        .unwrap();
    }

    let db = tmp_db.db.clone();
    let failed = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            let failed = failed.clone();
            std::thread::spawn(move || {
                let conn = db
                    .connect(ConnectOpts::new(SessionRole::Superuser))
                    .unwrap();
                for _ in 0..5000 {
                    if let Err(e) = conn.prepare("SELECT id FROM v") {
                        assert!(
                            e.to_string().contains("circularly defined"),
                            "unexpected prepare error: {e}"
                        );
                        failed.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert!(
        !failed.load(Ordering::Relaxed),
        "concurrent view expansion produced a spurious 'circularly defined' error"
    );
}

#[test]
fn create_materialized_view_is_rejected_in_mvcc_mode() {
    let tmp_db = TempDatabase::builder()
        .with_views(true)
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
        .unwrap();

    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT v, count(*) c FROM t GROUP BY v")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Materialized views are not supported in MVCC mode"),
        "unexpected error: {err}"
    );

    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, v FROM t");
    assert_eq!(rows, vec![(1, 10)]);
}

#[test]
fn a_view_body_reads_the_tables_of_the_view_namespace() {
    let (schema, other) = schema_with_a_table_t_in_two_namespaces();
    let body = select("SELECT * FROM t");
    assert_eq!(
        column_names(&extract_view_columns(other, &body, &schema, &SqliteDialect).unwrap()),
        ["b", "c"]
    );
    assert_eq!(
        column_names(
            &extract_view_columns(NamespaceId::ROOT, &body, &schema, &SqliteDialect).unwrap()
        ),
        ["a"]
    );
    let qualified = select("SELECT * FROM main.t");
    assert_eq!(
        column_names(&extract_view_columns(other, &qualified, &schema, &SqliteDialect).unwrap()),
        ["a"]
    );
}

#[test]
fn referenced_tables_finds_subqueries_of_order_by_limit_and_values() {
    assert_eq!(
        tables_read_by("SELECT a FROM u ORDER BY (SELECT max(a) FROM t)"),
        [table("t"), table("u")]
    );
    assert_eq!(
        tables_read_by("SELECT a FROM u LIMIT (SELECT count(*) FROM t) OFFSET (SELECT 1 FROM w)"),
        [table("t"), table("u"), table("w")]
    );
    assert_eq!(
        tables_read_by("VALUES ((SELECT max(a) FROM t))"),
        [table("t")]
    );
}

#[test]
fn referenced_tables_finds_subqueries_of_window_clauses_and_table_function_arguments() {
    assert_eq!(
        tables_read_by(
            "SELECT sum(a) OVER w FROM u WINDOW w AS (PARTITION BY (SELECT max(a) FROM t))"
        ),
        [table("t"), table("u")]
    );
    assert_eq!(
        tables_read_by("SELECT value FROM json_each((SELECT j FROM t))"),
        [table("t")]
    );
}

#[test]
fn referenced_tables_leaves_out_a_cte_only_where_it_is_in_scope() {
    assert_eq!(
        tables_read_by("SELECT a FROM t WHERE EXISTS (WITH t AS (SELECT 1 AS a) SELECT a FROM t)"),
        [table("t")]
    );
    assert_eq!(
        tables_read_by("WITH c AS (SELECT a FROM t) SELECT a FROM c"),
        [table("t")]
    );
}

#[test]
fn referenced_tables_finds_a_table_named_with_a_schema_next_to_a_cte_of_its_name() {
    assert_eq!(
        tables_read_by("WITH t AS (SELECT 1 AS a) SELECT a FROM t UNION ALL SELECT a FROM main.t"),
        [(Some("main".to_string()), "t".to_string())]
    );
}

fn tables_read_by(sql: &str) -> Vec<(Option<String>, String)> {
    let mut tables = referenced_tables(&select(sql));
    tables.sort();
    tables
}

fn table(name: &str) -> (Option<String>, String) {
    (None, name.to_string())
}
