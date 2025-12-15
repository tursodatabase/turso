use turso_core::{Result, StepResult, Value};

use crate::common::TempDatabase;

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
    let mut found_row = false;
    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let a = row.get::<&Value>(0).unwrap();
                let b = row.get::<&Value>(1).unwrap();
                assert_eq!(*a, Value::Integer(1));
                assert_eq!(*b, Value::build_text("first"));
                found_row = true;
            }
            StepResult::IO => stmt.run_once()?,
            StepResult::Done => break,
            r => panic!("Unexpected step result: {r:?}"),
        }
    }
    assert!(found_row, "Expected to find a row");

    // Verify the transaction is still active by executing another statement
    conn1.execute("INSERT INTO t (a, b) VALUES (3, 'third')")?;

    // Verify we can still query within the transaction
    let mut stmt2 = conn1.prepare("SELECT COUNT(*) FROM t")?;
    loop {
        match stmt2.step()? {
            StepResult::Row => {
                let row = stmt2.row().unwrap();
                let count = row.get::<&Value>(0).unwrap();
                assert_eq!(*count, Value::Integer(3));
            }
            StepResult::IO => stmt2.run_once()?,
            StepResult::Done => break,
            r => panic!("Unexpected step result: {r:?}"),
        }
    }

    // Commit the transaction
    conn1.execute("COMMIT")?;

    // Verify all changes are committed
    let mut stmt3 = conn1.prepare("SELECT COUNT(*) FROM t")?;
    loop {
        match stmt3.step()? {
            StepResult::Row => {
                let row = stmt3.row().unwrap();
                let count = row.get::<&Value>(0).unwrap();
                assert_eq!(*count, Value::Integer(3));
            }
            StepResult::IO => stmt3.run_once()?,
            StepResult::Done => break,
            r => panic!("Unexpected step result: {r:?}"),
        }
    }

    Ok(())
}
