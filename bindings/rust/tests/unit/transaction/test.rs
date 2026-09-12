use crate::{Builder, Connection, Error, Result};

use super::{DropBehavior, TransactionBehavior};

async fn checked_memory_handle() -> Result<Connection> {
    let db = Builder::new_local(":memory:").build().await?;
    let conn = db.connect()?;
    conn.execute("CREATE TABLE foo (x INTEGER)", ()).await?;
    Ok(conn)
}

#[tokio::test]
async fn test_drop_rollback_on_new_transaction() {
    let mut conn = checked_memory_handle().await.unwrap();
    {
        let tx = conn.transaction().await.unwrap();
        tx.execute("INSERT INTO foo VALUES(?)", &[1]).await.unwrap();
        // Drop without finish - should be rolled back when next transaction starts
    }

    // Start a new transaction - this should rollback the dangling one
    let tx = conn.transaction().await.unwrap();
    tx.execute("INSERT INTO foo VALUES(?)", &[2]).await.unwrap();
    let result = tx
        .prepare("SELECT SUM(x) FROM foo")
        .await
        .unwrap()
        .query_row(())
        .await
        .unwrap();

    // The insert from the dropped transaction should have been rolled back
    assert_eq!(2, result.get::<i32>(0).unwrap());
    tx.finish().await.unwrap();
}

#[tokio::test]
async fn test_drop_rollback_on_query() {
    let mut conn = checked_memory_handle().await.unwrap();
    {
        let tx = conn.transaction().await.unwrap();
        tx.execute("INSERT INTO foo VALUES(?)", &[1]).await.unwrap();
        // Drop without finish - should be rolled back when conn.query is called
    }

    // Using conn.query should rollback the dangling transaction
    let mut rows = conn.query("SELECT count(*) FROM foo", ()).await.unwrap();
    let result = rows.next().await.unwrap().unwrap();

    // The insert from the dropped transaction should have been rolled back
    assert_eq!(0, result.get::<i32>(0).unwrap());
}

#[tokio::test]
async fn test_drop_rollback_on_execute() {
    let mut conn = checked_memory_handle().await.unwrap();
    {
        let tx = conn.transaction().await.unwrap();
        tx.execute("INSERT INTO foo VALUES(?)", &[1]).await.unwrap();
        // Drop without finish - should be rolled back when conn.execute is called
    }

    // Using conn.execute should rollback the dangling transaction
    conn.execute("INSERT INTO foo VALUES(?)", &[2])
        .await
        .unwrap();

    let mut rows = conn.query("SELECT count(*) FROM foo", ()).await.unwrap();
    let result = rows.next().await.unwrap().unwrap();

    // The insert from the dropped transaction should have been rolled back
    assert_eq!(1, result.get::<i32>(0).unwrap());
}

#[tokio::test]
async fn test_drop() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let mut conn = checked_memory_handle().await?;
    {
        let tx = conn.transaction().await?;
        tx.execute("INSERT INTO foo VALUES(?)", &[1]).await?;
        // default: rollback
    }
    {
        let mut tx = conn.transaction().await?;
        tx.execute("INSERT INTO foo VALUES(?)", &[2]).await?;
        tx.set_drop_behavior(DropBehavior::Commit);
    }
    {
        let tx = conn.transaction().await?;
        let result = tx
            .prepare("SELECT SUM(x) FROM foo")
            .await?
            .query_row(())
            .await?;

        assert_eq!(2, result.get::<i32>(0)?);
    }
    Ok(())
}

fn assert_nested_tx_error(e: Error) {
    if let Error::Error(e) = &e {
        assert!(e.contains("transaction"));
    } else {
        panic!("Unexpected error type: {e:?}");
    }
}

#[tokio::test]
async fn test_unchecked_nesting() -> Result<()> {
    let conn = checked_memory_handle().await?;

    {
        let tx = conn.unchecked_transaction().await?;
        let e = tx.unchecked_transaction().await.unwrap_err();
        assert_nested_tx_error(e);
        tx.finish().await?;
        // default: rollback
    }
    {
        let tx = conn.unchecked_transaction().await?;
        tx.execute("INSERT INTO foo VALUES(?)", &[1]).await?;
        // Ensure this doesn't interfere with ongoing transaction
        let e = tx.unchecked_transaction().await.unwrap_err();
        assert_nested_tx_error(e);

        tx.execute("INSERT INTO foo VALUES(?)", &[1]).await?;
        tx.commit().await?;
    }

    let result = conn
        .prepare("SELECT SUM(x) FROM foo")
        .await?
        .query_row(())
        .await?;
    assert_eq!(2, result.get::<i32>(0)?);
    Ok(())
}

#[tokio::test]
async fn test_concurrent_transactions() -> Result<()> {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db = Builder::new_local(tmp.path().to_str().unwrap())
        .build()
        .await?;
    let conn = db.connect()?;
    conn.pragma_update("journal_mode", "'mvcc'").await?;
    conn.execute("CREATE TABLE foo (x INTEGER)", ()).await?;

    let mut conn1 = db.connect()?;
    let mut conn2 = db.connect()?;

    // Two concurrent write transactions may be open at the same time.
    let tx1 = conn1
        .transaction_with_behavior(TransactionBehavior::Concurrent)
        .await?;
    let tx2 = conn2
        .transaction_with_behavior(TransactionBehavior::Concurrent)
        .await?;
    tx1.execute("INSERT INTO foo VALUES (?)", &[1]).await?;
    tx2.execute("INSERT INTO foo VALUES (?)", &[2]).await?;
    tx1.commit().await?;
    tx2.commit().await?;

    let result = conn
        .prepare("SELECT SUM(x) FROM foo")
        .await?
        .query_row(())
        .await?;
    assert_eq!(3, result.get::<i32>(0)?);
    Ok(())
}

#[tokio::test]
async fn test_explicit_rollback_commit() -> Result<()> {
    let mut conn = checked_memory_handle().await?;
    {
        let tx = conn.transaction().await?;
        tx.execute("INSERT INTO foo VALUES(?)", &[1]).await?;
        tx.rollback().await?;

        // This is a current Turso's limitation.
        // Since we don't have support for savepoints yet,
        // a rollback ends with a transaction so we need to immediately open a new one.
        let tx = conn.transaction().await?;
        tx.execute("INSERT INTO foo VALUES(?)", &[2]).await?;
        tx.commit().await?;
    }
    {
        let tx = conn.transaction().await?;
        tx.execute("INSERT INTO foo VALUES(?)", &[4]).await?;
        tx.commit().await?;
    }
    {
        let result = conn
            .prepare("SELECT SUM(x) FROM foo")
            .await?
            .query_row(())
            .await?;
        assert_eq!(6, result.get::<i32>(0)?);
    }
    Ok(())
}
