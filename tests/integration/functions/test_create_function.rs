use crate::common::{ExecRows, TempDatabase, TempDatabaseBuilder};
use tempfile::TempDir;

fn udf_opts() -> turso_core::DatabaseOpts {
    turso_core::DatabaseOpts::new().with_udfs(true)
}

/// Function definitions must be loaded from __turso_internal_functions when
/// reopening a database.
#[test]
fn test_create_function_persists_across_reopen() {
    let path = TempDir::new().unwrap().keep().join("udf_reopen.db");

    // First session: create functions and use them.
    {
        let db = TempDatabase::new_with_existent_with_opts(&path, udf_opts());
        let conn = db.connect_limbo();
        conn.execute(
            "CREATE FUNCTION myadd(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE starlark AS $$\nreturn a + b\n$$",
        )
        .unwrap();
        conn.execute(
            "CREATE FUNCTION fact(n INTEGER) RETURNS INTEGER LANGUAGE starlark AS $$\nacc = 1\nfor i in range(1, n + 1):\n    acc = acc * i\nreturn acc\n$$",
        )
        .unwrap();
        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT myadd(2, 3), fact(5)");
        assert_eq!(rows, vec![(5, 120)]);
        conn.close().unwrap();
    }

    // Second session: reopen and call the persisted functions.
    {
        let db = TempDatabase::new_with_existent_with_opts(&path, udf_opts());
        let conn = db.connect_limbo();
        let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT myadd(40, 2), fact(6)");
        assert_eq!(
            rows,
            vec![(42, 720)],
            "user-defined functions must survive a database reopen"
        );

        // The persisted canonical SQL must round-trip: verify the stored text
        // re-parses by replacing the function from its own definition.
        let sqls: Vec<(String,)> =
            conn.exec_rows("SELECT sql FROM __turso_internal_functions WHERE name = 'myadd'");
        assert_eq!(sqls.len(), 1);
        let stored_sql = &sqls[0].0;
        assert!(stored_sql.starts_with("CREATE FUNCTION"), "{stored_sql}");
        conn.execute(stored_sql.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION"))
            .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT myadd(1, 1)");
        assert_eq!(rows, vec![(2,)]);
        conn.close().unwrap();
    }
}

/// Dropping the connection without close() simulates a crash after commit;
/// the committed function must still be there on reopen, and a dropped
/// function must stay dropped.
#[test]
fn test_create_and_drop_function_survive_unclean_shutdown() {
    let path = TempDir::new().unwrap().keep().join("udf_crash.db");

    {
        let db = TempDatabase::new_with_existent_with_opts(&path, udf_opts());
        let conn = db.connect_limbo();
        conn.execute("CREATE FUNCTION keep(x INTEGER) LANGUAGE starlark AS 'return x + 1'")
            .unwrap();
        conn.execute("CREATE FUNCTION gone(x INTEGER) LANGUAGE starlark AS 'return x'")
            .unwrap();
        conn.execute("DROP FUNCTION gone").unwrap();
        // No close(): simulate the process dying after the statements commit.
    }

    {
        let db = TempDatabase::new_with_existent_with_opts(&path, udf_opts());
        let conn = db.connect_limbo();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT keep(1)");
        assert_eq!(rows, vec![(2,)]);
        let err = conn.execute("SELECT gone(1)").unwrap_err();
        assert!(
            err.to_string().contains("no such function: gone"),
            "unexpected error: {err}"
        );
        conn.close().unwrap();
    }
}

/// A second connection on the same database must observe CREATE FUNCTION /
/// DROP FUNCTION from the first connection via the schema cookie bump.
#[test]
fn test_function_visible_across_connections() {
    let db = TempDatabaseBuilder::new()
        .with_db_name("udf_multi_conn.db")
        .with_opts(udf_opts())
        .build();
    let conn1 = db.connect_limbo();
    let conn2 = db.connect_limbo();

    conn1
        .execute("CREATE FUNCTION shared(x INTEGER) LANGUAGE starlark AS 'return x * 10'")
        .unwrap();
    let rows: Vec<(i64,)> = conn2.exec_rows("SELECT shared(4)");
    assert_eq!(
        rows,
        vec![(40,)],
        "second connection must see the new function"
    );

    conn1.execute("DROP FUNCTION shared").unwrap();
    let err = conn2.execute("SELECT shared(4)").unwrap_err();
    assert!(
        err.to_string().contains("no such function: shared"),
        "unexpected error: {err}"
    );
}

/// Without the experimental flag, CREATE FUNCTION is rejected and persisted
/// functions are not loaded (calls fail cleanly instead of misbehaving).
#[test]
fn test_udfs_require_experimental_flag() {
    let path = TempDir::new().unwrap().keep().join("udf_flag.db");

    {
        let db = TempDatabase::new_with_existent_with_opts(&path, udf_opts());
        let conn = db.connect_limbo();
        conn.execute("CREATE FUNCTION f(x INTEGER) LANGUAGE starlark AS 'return x'")
            .unwrap();
        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent_with_opts(&path, turso_core::DatabaseOpts::new());
        let conn = db.connect_limbo();
        let err = conn
            .execute("CREATE FUNCTION g(x INTEGER) LANGUAGE starlark AS 'return x'")
            .unwrap_err();
        assert!(
            err.to_string().contains("--experimental-udfs"),
            "unexpected error: {err}"
        );
        let err = conn.execute("SELECT f(1)").unwrap_err();
        assert!(
            err.to_string().contains("no such function: f"),
            "unexpected error: {err}"
        );
        conn.close().unwrap();
    }
}
