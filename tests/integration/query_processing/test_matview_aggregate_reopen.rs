//! The persisted aggregate state of a materialized view must round-trip.
//!
//! The DBSP aggregate state is serialized into a blob per group. Anything the
//! serializer cannot represent is silently replaced when the blob is read back,
//! so a view that answered correctly while its state was still in memory can
//! start answering wrongly after the database is reopened.
//!
//! Expectations come from sqlite3 running the same query against a plain view.

use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;
use tempfile::TempDir;

fn open(path: &std::path::Path) -> TempDatabase {
    TempDatabase::builder()
        .with_db_path(path)
        .with_views(true)
        .build()
}

/// sum()/avg() over a column that only holds NULLs are NULL, and that must
/// still be true after the aggregate state has been through the blob format.
#[test]
fn matview_sum_avg_over_all_null_column_stay_null_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("matview_agg_null_reopen.db");

    {
        let db = open(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, NULL), (2, NULL)")
            .unwrap();
        conn.execute(
            "CREATE MATERIALIZED VIEW v AS
             SELECT count(x) AS cx, sum(x) AS sx, avg(x) AS ax, count(*) AS n FROM t",
        )
        .unwrap();
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT cx, sx, ax, n FROM v"),
            vec![vec![
                Value::Integer(0),
                Value::Null,
                Value::Null,
                Value::Integer(2)
            ]],
            "before reopen"
        );
        conn.close().unwrap();
    }

    {
        let db = open(&path);
        let conn = db.connect_limbo();
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT cx, sx, ax, n FROM v"),
            vec![vec![
                Value::Integer(0),
                Value::Null,
                Value::Null,
                Value::Integer(2)
            ]],
            "after reopen"
        );
    }
}

/// sum() over integer input is an INTEGER, and the blob format must not turn
/// it into a REAL.
#[test]
fn matview_integer_sum_stays_integer_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("matview_agg_int_reopen.db");

    {
        let db = open(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(k TEXT, a INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES ('a', 3), ('a', 4), ('b', 5)")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW v AS SELECT k, sum(a) AS s FROM t GROUP BY k")
            .unwrap();
        conn.close().unwrap();
    }

    {
        let db = open(&path);
        let conn = db.connect_limbo();
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT k, s FROM v ORDER BY k"),
            vec![
                vec![Value::Text("a".into()), Value::Integer(7)],
                vec![Value::Text("b".into()), Value::Integer(5)],
            ],
            "after reopen"
        );
        // A write after the reopen loads the state from the blob and writes it
        // back, so the type must survive that round trip too.
        conn.execute("INSERT INTO t VALUES ('a', 1)").unwrap();
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT s FROM v WHERE k = 'a'"),
            vec![vec![Value::Integer(8)]],
            "after a write following the reopen"
        );
    }
}
