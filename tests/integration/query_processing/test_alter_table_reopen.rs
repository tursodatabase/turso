//! Reopen-DB tests for ALTER TABLE ADD COLUMN with table-level UNIQUE constraints.
//!
//! BTreeTable::to_sql() must emit table-level UNIQUE (...) so that after ADD COLUMN
//! the stored schema in sqlite_schema still matches the sqlite_autoindex_* entries.
//! Otherwise reopen triggers populate_indices panic: "all automatic indexes parsed
//! from sqlite_schema should have been consumed, but N remain".
//! See https://github.com/tursodatabase/turso/issues/5616

use crate::common::{ExecRows, TempDatabase};
use tempfile::TempDir;

/// After ALTER TABLE ADD COLUMN on a table with UNIQUE(stream_id, version), reopen
/// must succeed (no orphan autoindex) and the unique constraint must still be enforced.
#[test]
fn test_alter_table_add_column_preserves_unique_constraint_reopen() {
    let path = TempDir::new()
        .unwrap()
        .keep()
        .join("alter_add_col_unique_reopen.db");

    // Session 1: create table with table-level UNIQUE, add column, close
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE events (
                id TEXT,
                stream_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                PRIMARY KEY (id),
                UNIQUE (stream_id, version)
            )",
        )
        .unwrap();
        conn.execute("ALTER TABLE events ADD COLUMN extra TEXT")
            .unwrap();
        conn.close().unwrap();
    }

    // Session 2: reopen must not panic; UNIQUE(stream_id, version) must still be enforced
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        conn.execute("INSERT INTO events VALUES('e1', 's1', 1, 'x')")
            .unwrap();
        let err = conn.execute("INSERT INTO events VALUES('e2', 's1', 1, 'y')");
        assert!(
            err.is_err(),
            "UNIQUE(stream_id, version) must still be enforced after reopen"
        );

        let rows: Vec<(String, String, i64, String)> =
            conn.exec_rows("SELECT id, stream_id, version, extra FROM events ORDER BY id");
        assert_eq!(rows, vec![("e1".into(), "s1".into(), 1, "x".into())]);
        conn.close().unwrap();
    }
}

/// After ALTER TABLE ADD COLUMN on a table with multiple UNIQUE(a,b) and UNIQUE(b,c),
/// reopen must succeed and both constraints must still be enforced.
#[test]
fn test_alter_table_add_column_preserves_multiple_unique_constraints_reopen() {
    let path = TempDir::new()
        .unwrap()
        .keep()
        .join("alter_add_col_multi_unique_reopen.db");

    // Session 1: create table with two table-level UNIQUEs, add column, close
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE TABLE t (
                a TEXT,
                b INTEGER,
                c TEXT,
                UNIQUE (a, b),
                UNIQUE (b, c)
            )",
        )
        .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN d TEXT").unwrap();
        conn.close().unwrap();
    }

    // Session 2: reopen must not panic; both UNIQUEs must still be enforced
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        conn.execute("INSERT INTO t VALUES('x', 1, 'c1', 'd1')")
            .unwrap();
        let err = conn.execute("INSERT INTO t VALUES('x', 1, 'c2', 'd2')");
        assert!(
            err.is_err(),
            "UNIQUE(a, b) must still be enforced after reopen"
        );

        conn.execute("INSERT INTO t VALUES('a2', 2, 'c', 'd2')")
            .unwrap();
        let err = conn.execute("INSERT INTO t VALUES('a3', 2, 'c', 'd3')");
        assert!(
            err.is_err(),
            "UNIQUE(b, c) must still be enforced after reopen"
        );

        let rows: Vec<(String, i64, String, String)> =
            conn.exec_rows("SELECT a, b, c, d FROM t ORDER BY a, b");
        assert_eq!(
            rows,
            vec![
                ("a2".into(), 2, "c".into(), "d2".into()),
                ("x".into(), 1, "c1".into(), "d1".into()),
            ]
        );
        conn.close().unwrap();
    }
}
