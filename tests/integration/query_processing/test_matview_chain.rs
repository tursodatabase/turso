//! Materialized views built on top of other materialized views.
//!
//! Two things must hold across a database reopen:
//!   * a chained view keeps the rows the maintenance cascade gave it, and
//!   * a chain of any depth can be rebuilt from the stored schema, which needs
//!     the sources of a view to be loaded before the view itself.

use crate::common::{ExecRows, TempDatabase};
use tempfile::TempDir;

fn open_with_views(path: &std::path::Path) -> TempDatabase {
    TempDatabase::builder()
        .with_db_path(path)
        .with_views(true)
        .build()
}

#[test]
fn test_matview_chain_depth2_survives_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("matview_chain_depth2.db");

    {
        let db = open_with_views(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a, b)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv1 AS SELECT a, b FROM t")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv2 AS SELECT b FROM mv1")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();

        let mv2: Vec<(i64,)> = conn.exec_rows("SELECT b FROM mv2 ORDER BY b");
        assert_eq!(mv2, vec![(10,), (20,), (30,)]);
        conn.close().unwrap();
    }

    {
        let db = open_with_views(&path);
        let conn = db.connect_limbo();
        let mv2: Vec<(i64,)> = conn.exec_rows("SELECT b FROM mv2 ORDER BY b");
        assert_eq!(mv2, vec![(10,), (20,), (30,)]);
        conn.close().unwrap();
    }
}

#[test]
fn test_matview_chain_depth3_survives_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("matview_chain_depth3.db");

    {
        let db = open_with_views(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a, b)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv1 AS SELECT a, b FROM t")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv2 AS SELECT b FROM mv1")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv3 AS SELECT b * 2 AS bb FROM mv2")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();

        let mv3: Vec<(i64,)> = conn.exec_rows("SELECT bb FROM mv3 ORDER BY bb");
        assert_eq!(mv3, vec![(20,), (40,), (60,)]);
        conn.close().unwrap();
    }

    {
        let db = open_with_views(&path);
        let conn = db.connect_limbo();
        let mv2: Vec<(i64,)> = conn.exec_rows("SELECT b FROM mv2 ORDER BY b");
        assert_eq!(mv2, vec![(10,), (20,), (30,)]);
        let mv3: Vec<(i64,)> = conn.exec_rows("SELECT bb FROM mv3 ORDER BY bb");
        assert_eq!(mv3, vec![(20,), (40,), (60,)]);
        conn.close().unwrap();
    }
}

/// Writes made after a reopen must keep flowing down the chain: the cascade has
/// to be rebuilt from the stored schema, not only from the CREATE statements.
#[test]
fn test_matview_chain_accepts_writes_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("matview_chain_write_after_reopen.db");

    {
        let db = open_with_views(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a, b)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv1 AS SELECT a, b FROM t")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW mv2 AS SELECT b FROM mv1")
            .unwrap();
        conn.close().unwrap();
    }

    {
        let db = open_with_views(&path);
        let conn = db.connect_limbo();
        conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        conn.execute("DELETE FROM t WHERE a = 1").unwrap();

        let mv2: Vec<(i64,)> = conn.exec_rows("SELECT b FROM mv2 ORDER BY b");
        assert_eq!(mv2, vec![(20,)]);
        conn.close().unwrap();
    }

    {
        let db = open_with_views(&path);
        let conn = db.connect_limbo();
        let mv2: Vec<(i64,)> = conn.exec_rows("SELECT b FROM mv2 ORDER BY b");
        assert_eq!(mv2, vec![(20,)]);
        conn.close().unwrap();
    }
}
