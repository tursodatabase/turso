use asserting::prelude::*;
use std::sync::Arc;
use tempfile::TempDir;

use turso_core::{Connection, LimboError, Result, StatementStatusCounter, Value};

use crate::common::{ExecRows, TempDatabase};

/// Test that SchemaUpdated error reprepares the statement.
///
/// Scenario:
/// 1. Connection 1 starts a transaction and prepares a SELECT statement
/// 2. Connection 2 changes the schema (ALTER TABLE)
/// 3. Connection 1 tries to execute the prepared statement - gets SchemaUpdated
/// 4. Verify the transaction is still active (can execute other statements)
/// 5. Verify the statement can be retried and will succeed after reprepare
#[turso_macros::test]
fn test_schema_update_reprepares_statement(tmp_db: TempDatabase) -> Result<()> {
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    // Create initial table
    conn1.execute("CREATE TABLE t (a INTEGER, b TEXT)")?;
    conn1.execute("INSERT INTO t VALUES (1, 'first'), (2, 'second')")?;

    // Connection 1 starts a transaction
    conn1.execute("BEGIN")?;

    // Prepare a SELECT statement (this captures the schema cookie at prepare time)
    let mut stmt = conn1.prepare("SELECT a, b FROM t WHERE a = 1")?;

    // Connection 2 changes the schema (this increments the schema cookie)
    conn2.execute("ALTER TABLE t ADD COLUMN c INTEGER")?;

    // Connection 1 tries to execute the prepared statement
    // Note: Statement::step() will automatically reprepare and retry on SchemaUpdated
    // for statements that access the database. However, we can still verify that
    // the transaction is not rolled back even if SchemaUpdated occurs.
    // For this test, we'll use a statement that should trigger SchemaUpdated
    // but the automatic reprepare should handle it.

    // First, let's verify the statement can execute (it will be automatically reprepared)
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })?;
    assert_that!(rows).is_equal_to(vec![row![1, "first"]]);

    // Verify the transaction is still active by executing another statement
    conn1.execute("INSERT INTO t (a, b) VALUES (3, 'third')")?;

    // Verify we can still query within the transaction
    let mut stmt2 = conn1.prepare("SELECT COUNT(*) FROM t")?;
    stmt2.run_with_row_callback(|row| {
        let count = row.get::<&Value>(0).unwrap();
        assert_eq!(*count, Value::from_i64(3));
        Ok(())
    })?;

    // Commit the transaction
    conn1.execute("COMMIT")?;

    // Verify all changes are committed
    let mut stmt3 = conn1.prepare("SELECT COUNT(*) FROM t")?;
    stmt3.run_with_row_callback(|row| {
        let count = row.get::<&Value>(0).unwrap();
        assert_eq!(*count, Value::from_i64(3));
        Ok(())
    })?;

    Ok(())
}

#[turso_macros::test]
fn test_temp_shadowing_reprepares_prepared_statement(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (x INTEGER)")?;
    conn.execute("INSERT INTO main.t VALUES (1)")?;

    let mut stmt = conn.prepare("SELECT x FROM t")?;

    conn.execute("CREATE TEMP TABLE t (x INTEGER)")?;
    conn.execute("INSERT INTO temp.t VALUES (2)")?;

    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get::<i64>(0)?);
        Ok(())
    })?;

    assert_eq!(
        rows,
        vec![2],
        "prepared statement should observe temp-table shadowing after reprepare"
    );

    Ok(())
}

#[turso_macros::test]
fn test_temp_schema_change_invalidates_unrelated_prepared_statement(
    tmp_db: TempDatabase,
) -> Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE m(x INTEGER)")?;
    conn.execute("INSERT INTO m VALUES (1)")?;

    let mut stmt = conn.prepare("SELECT x FROM m")?;
    assert_that!(stmt.stmt_status(StatementStatusCounter::Reprepare)).is_zero();

    conn.execute("CREATE TEMP TABLE temp_t(y INTEGER)")?;

    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get::<i64>(0)?);
        Ok(())
    })?;

    assert_that!(rows).is_equal_to(vec![1]);
    assert_that!(stmt.stmt_status(StatementStatusCounter::Reprepare)).is_one();

    Ok(())
}

/// A statement prepared inside a transaction may read an index that the
/// transaction created. ROLLBACK removes the index, and a later CREATE TABLE
/// brings the schema version back to the number the statement was prepared
/// with, reusing the index's page. The statement must still be reprepared
/// instead of reading that page as the index.
#[turso_macros::test(mvcc)]
fn test_rolled_back_schema_change_reprepares_statement(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(k INTEGER, v INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")?;

    conn.execute("BEGIN")?;
    conn.execute("CREATE INDEX t_k ON t(k)")?;
    let mut stmt = conn.prepare("SELECT v FROM t INDEXED BY t_k WHERE k = 2")?;
    conn.execute("ROLLBACK")?;
    conn.execute("CREATE TABLE zz(a INTEGER, b INTEGER)")?;
    conn.execute("INSERT INTO zz VALUES (2, 99)")?;

    let mut rows = Vec::new();
    let result = stmt.run_with_row_callback(|row| {
        rows.push(row.get::<i64>(0)?);
        Ok(())
    });
    let error = result.expect_err("the index the statement names was rolled back");
    assert_that!(error.to_string()).contains("no such index: t_k");
    assert_that!(rows).is_empty();

    Ok(())
}

#[test]
fn test_prepared_schema_pragmas_reprepare_after_ddl() -> Result<()> {
    let cases: [(&[&str], &str, &[&str]); 7] = [
        (
            &["CREATE TABLE t1(a)"],
            "PRAGMA table_info(t1)",
            &["ALTER TABLE t1 ADD COLUMN b"],
        ),
        (
            &["CREATE TABLE t1(a)"],
            "PRAGMA table_xinfo(t1)",
            &["ALTER TABLE t1 ADD COLUMN b"],
        ),
        (
            &["CREATE TABLE t1(a)"],
            "PRAGMA table_list",
            &["CREATE TABLE t2(a)"],
        ),
        (
            &["CREATE TABLE t1(a, b)", "CREATE INDEX i1 ON t1(a)"],
            "PRAGMA index_list(t1)",
            &["CREATE INDEX i2 ON t1(b)"],
        ),
        (
            &["CREATE TABLE t1(a, b)", "CREATE INDEX i1 ON t1(a)"],
            "PRAGMA index_info(i1)",
            &["DROP INDEX i1", "CREATE INDEX i1 ON t1(b)"],
        ),
        (
            &["CREATE TABLE t1(a, b)", "CREATE INDEX i1 ON t1(a)"],
            "PRAGMA index_xinfo(i1)",
            &["DROP INDEX i1", "CREATE INDEX i1 ON t1(b)"],
        ),
        (
            &[
                "CREATE TABLE t1(a PRIMARY KEY)",
                "CREATE TABLE t2(b REFERENCES t1(a))",
            ],
            "PRAGMA foreign_key_list(t2)",
            &["DROP TABLE t2", "CREATE TABLE t2(b)"],
        ),
    ];
    for (setup, pragma, ddl) in cases {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();
        for sql in setup {
            conn.execute(*sql)?;
        }
        let mut stmt = conn.prepare(pragma)?;
        stmt.run_collect_rows()?;
        for sql in ddl {
            conn.execute(*sql)?;
        }

        stmt.reset()?;
        let reused = stmt.run_collect_rows()?;
        let fresh = conn.prepare(pragma)?.run_collect_rows()?;
        assert_that!(reused).named(pragma).is_equal_to(fresh);
    }
    Ok(())
}

/// Test that deferred_seeks vector is properly resized when a statement is reprepared
/// with a larger cursor count due to schema changes (e.g., new index creation).
///
/// This is a regression test for the bug where ProgramState::reset() didn't resize
/// the deferred_seeks vector when resizing cursors and cursor_seqs, causing an
/// index out of bounds panic in op_column when accessing deferred_seeks with a
/// cursor_id larger than the original allocation.
///
/// Scenario:
/// 1. Create tables with data
/// 2. Prepare a JOIN query (multiple cursors)
/// 3. Another connection creates indexes on the tables (schema change)
/// 4. Execute the prepared statement - triggers reprepare
/// 5. The reprepared query uses the new indexes with deferred seeks
/// 6. If deferred_seeks wasn't resized, accessing a higher cursor_id would panic
#[turso_macros::test]
fn test_deferred_seeks_resize_on_reprepare(tmp_db: TempDatabase) -> Result<()> {
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    // Create two tables to JOIN - this gives us multiple table cursors
    conn1.execute(
        "CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            total REAL
        )",
    )?;

    conn1.execute(
        "CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        )",
    )?;

    // Insert test data
    conn1.execute(
        "INSERT INTO customers VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')",
    )?;

    conn1.execute(
        "INSERT INTO orders VALUES
            (100, 1, 150.00),
            (101, 2, 250.00),
            (102, 1, 75.00),
            (103, 3, 500.00)",
    )?;

    // Prepare a JOIN query that selects columns from both tables
    // Initially no indexes on customer_id, so it's a nested loop scan
    // This uses 2 cursors (one for each table)
    let mut stmt = conn1.prepare(
        "SELECT c.name, o.total FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id
         WHERE o.total > 100.0",
    )?;

    // Create indexes on both tables (schema change) - causes reprepare
    // After reprepare, the optimizer may use these indexes, requiring more cursors
    // and potentially using deferred seeks
    conn2.execute("CREATE INDEX idx_orders_customer ON orders(customer_id)")?;
    conn2.execute("CREATE INDEX idx_orders_total ON orders(total)")?;

    // Execute the statement - triggers reprepare with potentially more cursors
    // Without the fix, this could panic with index out of bounds on deferred_seeks
    let mut results = Vec::new();
    stmt.run_with_row_callback(|row| {
        let name: String = row.get(0)?;
        let total: f64 = row.get(1)?;
        results.push((name, total));
        Ok(())
    })?;

    // Three orders have a total over 100.0.
    assert_that!(results).has_length(3);

    Ok(())
}

/// Regression test for https://github.com/tursodatabase/turso/issues/6769.
///
/// ALTER TABLE ... ALTER COLUMN that turns an AUTOINCREMENT rowid alias into a
/// non-rowid column must persist a consistent state on disk: the table's row in
/// `sqlite_sequence` is removed and the rewritten `sqlite_schema.sql` no longer
/// carries the AUTOINCREMENT keyword. After reopening the database, those
/// invariants must still hold and subsequent inserts must not revive the
/// sequence row.
#[test]
fn test_alter_table_alter_column_clears_autoincrement_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("alter_col_autoincrement_clear_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t(b) VALUES ('x'), ('y')")
            .unwrap();

        let seq_before: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='t'");
        assert_eq!(
            seq_before,
            vec![("t".into(), 2)],
            "sqlite_sequence should be populated before ALTER"
        );

        conn.execute("ALTER TABLE t ALTER COLUMN a TO a2 TEXT")
            .unwrap();

        let schema_has_autoinc: Vec<(i64,)> =
            conn.exec_rows("SELECT sql LIKE '%AUTOINCREMENT%' FROM sqlite_schema WHERE name='t'");
        assert_eq!(
            schema_has_autoinc,
            vec![(0,)],
            "AUTOINCREMENT must be gone from sqlite_schema.sql after ALTER COLUMN"
        );

        let seq_after_alter: Vec<(i64,)> =
            conn.exec_rows("SELECT COUNT(*) FROM sqlite_sequence WHERE name='t'");
        assert_eq!(
            seq_after_alter,
            vec![(0,)],
            "sqlite_sequence row must be cleared by ALTER COLUMN"
        );

        conn.close().unwrap();
    }

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        let schema_has_autoinc: Vec<(i64,)> =
            conn.exec_rows("SELECT sql LIKE '%AUTOINCREMENT%' FROM sqlite_schema WHERE name='t'");
        assert_eq!(
            schema_has_autoinc,
            vec![(0,)],
            "persisted sqlite_schema.sql must not contain AUTOINCREMENT after reopen"
        );

        let seq_after_reopen: Vec<(i64,)> =
            conn.exec_rows("SELECT COUNT(*) FROM sqlite_sequence WHERE name='t'");
        assert_eq!(
            seq_after_reopen,
            vec![(0,)],
            "sqlite_sequence row must remain absent after reopen"
        );

        conn.execute("INSERT INTO t(b) VALUES ('z')").unwrap();

        let seq_after_insert: Vec<(i64,)> =
            conn.exec_rows("SELECT COUNT(*) FROM sqlite_sequence WHERE name='t'");
        assert_eq!(
            seq_after_insert,
            vec![(0,)],
            "INSERT after reopen must not recreate the sequence row \
             (in-memory has_autoincrement must have been cleared)"
        );

        let rewritten: Vec<(String, String)> =
            conn.exec_rows("SELECT a2, b FROM t WHERE a2 IS NOT NULL ORDER BY rowid");
        assert_eq!(
            rewritten,
            vec![("1".into(), "x".into()), ("2".into(), "y".into())],
            "pre-ALTER rows must carry the old rowid as TEXT after the column rewrite"
        );

        let post_insert: Vec<(i64, String)> =
            conn.exec_rows("SELECT a2 IS NULL, b FROM t WHERE b='z'");
        assert_eq!(
            post_insert,
            vec![(1, "z".into())],
            "INSERT after reopen must leave a2 NULL — column is no longer a rowid alias"
        );

        conn.close().unwrap();
    }
}

#[test]
fn test_strict_generated_any_index_after_reopen() -> Result<()> {
    for (create_sql, alter_sql) in [
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a ANY, g aNy AS(a)) STRICT",
            None,
        ),
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a ANY) STRICT",
            Some("ALTER TABLE t ADD COLUMN g aNy AS(a)"),
        ),
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a ANY, c ANY) STRICT",
            Some("ALTER TABLE t ALTER COLUMN c TO g aNy AS(a)"),
        ),
    ] {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("generated_any.db");
        let opts = turso_core::DatabaseOpts::new().with_generated_columns(true);
        let expected = vec![
            (1, "'00042'".to_string(), "text".to_string()),
            (2, "42".to_string(), "integer".to_string()),
            (3, "'42'".to_string(), "text".to_string()),
        ];

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            conn.execute(create_sql)?;
            conn.execute("INSERT INTO t(id,a) VALUES(1,'00042'),(2,42)")?;
            if let Some(alter_sql) = alter_sql {
                conn.execute(alter_sql)?;
            }
            conn.execute("CREATE UNIQUE INDEX t_g ON t(g)")?;
            conn.execute("INSERT INTO t(id,a) VALUES(3,'42')")?;
            let rows: Vec<(i64, String, String)> =
                conn.exec_rows("SELECT id,quote(g),typeof(g) FROM t INDEXED BY t_g ORDER BY id");
            assert_that!(rows)
                .named(create_sql)
                .is_equal_to(expected.clone());
            conn.close()?;
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts);
            let conn = db.connect_limbo();
            for access in ["NOT INDEXED", "INDEXED BY t_g"] {
                let rows: Vec<(i64, String, String)> = conn.exec_rows(&format!(
                    "SELECT id,quote(g),typeof(g) FROM t {access} ORDER BY id"
                ));
                assert_that!(rows)
                    .named(create_sql)
                    .is_equal_to(expected.clone());
            }

            conn.execute("UPDATE t SET a='00043' WHERE g='00042'")?;
            conn.execute("DELETE FROM t WHERE g=42")?;
            let rows: Vec<(i64, String, String)> =
                conn.exec_rows("SELECT id,quote(g),typeof(g) FROM t INDEXED BY t_g ORDER BY id");
            assert_that!(rows).named(create_sql).is_equal_to(vec![
                (1, "'00043'".to_string(), "text".to_string()),
                (3, "'42'".to_string(), "text".to_string()),
            ]);
            let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
            assert_that!(integrity)
                .named(create_sql)
                .is_equal_to(vec![("ok".to_string(),)]);
            conn.close()?;
        }
    }
    Ok(())
}

/// A statement that went stale because a connection setting changed is
/// prepared again before it starts. That must not end the transaction of
/// another statement that is still returning rows.
#[turso_macros::test(mvcc)]
fn test_stale_statement_keeps_writes_of_a_statement_returning_rows(
    tmp_db: TempDatabase,
) -> Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (x)")?;
    let mut stale = conn.prepare("SELECT 1")?;
    let mut insert = conn.prepare("INSERT INTO t VALUES (1), (2) RETURNING x")?;
    assert_eq!(step_to_first_row(&mut insert)?, vec![Value::from_i64(1)]);

    conn.execute("PRAGMA foreign_keys = ON")?;
    assert_eq!(stale.run_collect_rows()?, vec![vec![Value::from_i64(1)]]);
    insert.run_ignore_rows()?;

    assert_eq!(count_rows(&conn, "t")?, Value::from_i64(2));
    assert_eq!(
        count_rows(&tmp_db.connect_limbo(), "t")?,
        Value::from_i64(2)
    );
    Ok(())
}

/// Preparing a stale statement again must not end the read transaction of a
/// SELECT that is part-way through its rows: the SELECT keeps reading the
/// snapshot it started with, even after another connection deletes the rows
/// and checkpoints.
#[turso_macros::test(mvcc)]
fn test_stale_statement_keeps_the_snapshot_of_a_running_select(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();
    let other = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")?;
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;
    let mut stale = conn.prepare("SELECT 1")?;
    let mut select = conn.prepare("SELECT x FROM t ORDER BY x")?;
    assert_eq!(step_to_first_row(&mut select)?, vec![Value::from_i64(1)]);

    conn.execute("PRAGMA foreign_keys = ON")?;
    assert_eq!(stale.run_collect_rows()?, vec![vec![Value::from_i64(1)]]);
    other.execute("DELETE FROM t")?;
    match other.execute("PRAGMA wal_checkpoint(TRUNCATE)") {
        Ok(()) | Err(LimboError::Busy) => {}
        Err(err) => return Err(err),
    }

    assert_eq!(
        select.run_collect_rows()?,
        vec![vec![Value::from_i64(2)], vec![Value::from_i64(3)]]
    );
    Ok(())
}

/// With no other statement running, a stale statement is prepared again and
/// runs with the new setting.
#[turso_macros::test(mvcc)]
fn test_stale_statement_uses_the_new_setting(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")?;
    conn.execute("CREATE TABLE child (parent_id INTEGER REFERENCES parent(id))")?;
    let mut insert = conn.prepare("INSERT INTO child VALUES (1)")?;

    conn.execute("PRAGMA foreign_keys = ON")?;
    let err = insert.run_ignore_rows().unwrap_err();
    assert!(
        matches!(err, LimboError::ForeignKeyConstraint(_)),
        "unexpected error: {err:?}"
    );

    tmp_db
        .connect_limbo()
        .execute("INSERT INTO parent VALUES (1)")?;
    assert_eq!(count_rows(&conn, "parent")?, Value::from_i64(1));
    assert_eq!(count_rows(&conn, "child")?, Value::from_i64(0));
    Ok(())
}

fn step_to_first_row(stmt: &mut turso_core::Statement) -> Result<Vec<Value>> {
    let row = stmt
        .run_one_step_blocking(|| Ok(()), || Ok(()))?
        .expect("statement should return a row");
    Ok(row.get_values().cloned().collect())
}

fn count_rows(conn: &Arc<Connection>, table: &str) -> Result<Value> {
    let mut rows = conn
        .prepare(format!("SELECT count(*) FROM {table}"))?
        .run_collect_rows()?;
    Ok(rows.remove(0).remove(0))
}
