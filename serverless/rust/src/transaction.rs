use std::ops::Deref;

use crate::connection::{Connection, PENDING_COMMIT, PENDING_ROLLBACK};
use crate::statement::Statement;
use crate::Result;

/// The behavior for starting a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBehavior {
    /// DEFERRED means the transaction does not actually start until the
    /// database is first accessed.
    Deferred,
    /// IMMEDIATE starts a new write immediately, without waiting for a write
    /// statement.
    Immediate,
    /// EXCLUSIVE prevents other connections from reading the database while the
    /// transaction is underway.
    Exclusive,
    /// CONCURRENT starts a concurrent (BEGIN CONCURRENT) transaction.
    Concurrent,
}

impl TransactionBehavior {
    /// The SQL keyword used after `BEGIN`.
    pub fn as_sql_mode(&self) -> &'static str {
        match self {
            TransactionBehavior::Deferred => "DEFERRED",
            TransactionBehavior::Immediate => "IMMEDIATE",
            TransactionBehavior::Exclusive => "EXCLUSIVE",
            TransactionBehavior::Concurrent => "CONCURRENT",
        }
    }
}

/// What happens to a pending transaction when its [`Transaction`] is dropped
/// without an explicit `commit()`/`rollback()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropBehavior {
    /// Roll back the transaction (the default).
    Rollback,
    /// Commit the transaction.
    Commit,
    /// Do nothing — leave the transaction as-is.
    Ignore,
    /// Panic (useful to catch forgotten commits/rollbacks in tests).
    Panic,
}

/// A transaction on a remote connection.
///
/// Dereferences to the underlying [`Connection`], so `tx.execute(...)` and
/// friends work directly. On drop without an explicit `commit()`/`rollback()`,
/// the `DropBehavior` is honored on a best-effort basis — a true async
/// rollback cannot run in `Drop`, so the server rolls the stream back when the
/// baton expires (call `rollback().await` for a synchronous guarantee).
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    drop_behavior: DropBehavior,
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
    pub async fn new_unchecked(
        conn: &'conn Connection,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'conn>> {
        conn.execute(format!("BEGIN {}", behavior.as_sql_mode()), ())
            .await?;
        Ok(Self {
            conn,
            drop_behavior: DropBehavior::Rollback,
            finished: false,
        })
    }

    /// Get a reference to the underlying connection.
    pub fn conn(&self) -> &Connection {
        self.conn
    }

    /// The current drop behavior (default [`DropBehavior::Rollback`]).
    pub fn drop_behavior(&self) -> DropBehavior {
        self.drop_behavior
    }

    /// Set what happens when the transaction is dropped without an explicit
    /// commit/rollback.
    pub fn set_drop_behavior(&mut self, drop_behavior: DropBehavior) {
        self.drop_behavior = drop_behavior;
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

    /// Consume the transaction, applying the current [`drop_behavior`].
    pub async fn finish(mut self) -> Result<()> {
        self.finished = true;
        match self.drop_behavior {
            DropBehavior::Commit => self.conn.execute("COMMIT", ()).await.map(|_| ()),
            DropBehavior::Rollback => self.conn.execute("ROLLBACK", ()).await.map(|_| ()),
            DropBehavior::Ignore | DropBehavior::Panic => Ok(()),
        }
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // A true async rollback/commit cannot run in Drop, so record the action
        // and let the next connection operation apply it (embedded-driver
        // parity via Connection::flush_pending_drop).
        match self.drop_behavior {
            DropBehavior::Rollback => self.conn.set_pending_drop(PENDING_ROLLBACK),
            DropBehavior::Commit => self.conn.set_pending_drop(PENDING_COMMIT),
            DropBehavior::Ignore => {}
            DropBehavior::Panic => {
                panic!("Transaction dropped without commit/rollback (DropBehavior::Panic)")
            }
        }
    }
}
