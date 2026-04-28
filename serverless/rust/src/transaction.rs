use crate::Result;

use crate::connection::Connection;
use crate::statement::Statement;

/// The behavior for starting a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBehavior {
    /// DEFERRED means that the transaction does not actually start until the
    /// database is first accessed.
    Deferred,
    /// IMMEDIATE cause the database connection to start a new write
    /// immediately, without waiting for a writes statement.
    Immediate,
    /// EXCLUSIVE prevents other database connections from reading the database
    /// while the transaction is underway.
    Exclusive,
}

/// A transaction on a remote connection.
///
/// Transactions will roll back by default when dropped (on the next
/// connection use). Call `commit()` to commit explicitly.
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    finished: bool,
}

impl<'conn> Transaction<'conn> {
    /// Begin a new transaction with the given behavior.
    pub async fn new(
        conn: &'conn mut Connection,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'conn>> {
        Self::new_unchecked(conn, behavior).await
    }

    /// Begin a new transaction without requiring `&mut`.
    ///
    /// This allows starting a transaction without compile-time nesting prevention.
    pub async fn new_unchecked(
        conn: &'conn Connection,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'conn>> {
        let sql = match behavior {
            TransactionBehavior::Deferred => "BEGIN DEFERRED",
            TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
        };
        conn.execute(sql, ()).await?;
        Ok(Self {
            conn,
            finished: false,
        })
    }

    /// Get a reference to the underlying connection.
    pub fn conn(&self) -> &Connection {
        self.conn
    }

    /// Prepare a SQL statement on the transaction's connection.
    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        self.conn.prepare(sql).await
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<()> {
        self.conn.execute("COMMIT", ()).await?;
        self.finished = true;
        Ok(())
    }

    /// Roll back the transaction.
    pub async fn rollback(mut self) -> Result<()> {
        self.conn.execute("ROLLBACK", ()).await?;
        self.finished = true;
        Ok(())
    }

    /// Consume the transaction, committing or rolling back based on the default behavior.
    ///
    /// For remote transactions, finish always commits (equivalent to `commit()`).
    pub async fn finish(mut self) -> Result<()> {
        self.conn.execute("COMMIT", ()).await?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Can't do async in Drop. The session will handle the dangling
            // transaction on the next request (the server will auto-rollback
            // when the baton expires, or we send ROLLBACK on next use).
            //
            // For explicit cleanup, users should call commit() or rollback().
        }
    }
}
