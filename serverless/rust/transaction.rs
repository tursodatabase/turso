use std::ops::Deref;

use crate::connection::Connection;
use crate::statement::Statement;
use crate::Result;

/// Options for transaction behavior. See [BEGIN
/// TRANSACTION](http://www.sqlite.org/lang_transaction.html) for details.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransactionBehavior {
    /// DEFERRED means that the transaction does not actually start until the
    /// database is first accessed.
    Deferred,
    /// IMMEDIATE causes the database connection to start a new write
    /// immediately, without waiting for a write statement.
    Immediate,
    /// EXCLUSIVE prevents other database connections from reading the database
    /// while the transaction is underway.
    Exclusive,
}

/// Options for how a Transaction should behave when it is dropped.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DropBehavior {
    /// Roll back the changes. This is the default.
    Rollback,

    /// Commit the changes.
    Commit,

    /// Do not commit or roll back changes - this will leave the transaction or
    /// savepoint open, so should be used with care.
    Ignore,

    /// Panic. Used to enforce intentional behavior during development.
    Panic,
}

impl From<DropBehavior> for u8 {
    fn from(behavior: DropBehavior) -> Self {
        match behavior {
            DropBehavior::Rollback => 0,
            DropBehavior::Commit => 1,
            DropBehavior::Ignore => 2,
            DropBehavior::Panic => 3,
        }
    }
}

impl From<u8> for DropBehavior {
    fn from(value: u8) -> Self {
        match value {
            0 => DropBehavior::Rollback,
            1 => DropBehavior::Commit,
            2 => DropBehavior::Ignore,
            3 => DropBehavior::Panic,
            _ => panic!("Invalid drop behavior: {value}"),
        }
    }
}

/// An interactive transaction.
///
/// The transaction spans multiple HTTP requests: the driver sends `BEGIN`
/// on the connection's stream and the server keeps the stream (and thereby
/// the transaction) alive across requests via the baton (section 4.7 of
/// the protocol specification).
///
/// Call [`commit`](Transaction::commit) to commit the transaction. A
/// transaction that is dropped without being finished is completed
/// according to its [`DropBehavior`] (rollback by default): on the next
/// use of the connection if there is one, or by the server rolling back
/// when the stream is closed or expires. Because `Drop` cannot perform an
/// HTTP request, the drop behavior is always applied as deferred work.
#[derive(Debug)]
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    drop_behavior: DropBehavior,
    in_progress: bool,
}

impl Transaction<'_> {
    /// Begin a new transaction. Cannot be nested.
    ///
    /// Even though we don't mutate the connection, we take a `&mut Connection`
    /// to prevent nested transactions on the same connection. For cases
    /// where this is unacceptable, [`Transaction::new_unchecked`] is available.
    #[inline]
    pub async fn new(
        conn: &mut Connection,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'_>> {
        Self::new_unchecked(conn, behavior).await
    }

    /// Begin a new transaction, failing if a transaction is open.
    ///
    /// If a transaction is already open, this will return an error. Where
    /// possible, [`Transaction::new`] should be preferred, as it provides a
    /// compile-time guarantee that transactions are not nested.
    #[inline]
    pub async fn new_unchecked(
        conn: &Connection,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'_>> {
        let sql = match behavior {
            TransactionBehavior::Deferred => "BEGIN DEFERRED",
            TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
        };
        conn.execute(sql, ()).await?;
        Ok(Transaction {
            conn,
            drop_behavior: DropBehavior::Rollback,
            in_progress: true,
        })
    }

    /// Prepare a statement through the transaction's connection.
    ///
    /// This allows a database update function to be passed a transaction,
    /// prepare a statement, and use it without needing direct access to
    /// the connection.
    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        self.conn.prepare(sql).await
    }

    /// Get the current setting for what happens to the transaction when it is
    /// dropped.
    #[inline]
    #[must_use]
    pub fn drop_behavior(&self) -> DropBehavior {
        self.drop_behavior
    }

    /// Configure the transaction to perform the specified action when it is
    /// dropped.
    #[inline]
    pub fn set_drop_behavior(&mut self, drop_behavior: DropBehavior) {
        self.drop_behavior = drop_behavior;
    }

    /// A convenience method which consumes and commits a transaction.
    #[inline]
    pub async fn commit(mut self) -> Result<()> {
        self._commit().await
    }

    #[inline]
    async fn _commit(&mut self) -> Result<()> {
        self.conn.execute("COMMIT", ()).await?;
        self.in_progress = false;
        Ok(())
    }

    /// A convenience method which consumes and rolls back a transaction.
    #[inline]
    pub async fn rollback(mut self) -> Result<()> {
        self._rollback().await
    }

    #[inline]
    async fn _rollback(&mut self) -> Result<()> {
        self.conn.execute("ROLLBACK", ()).await?;
        self.in_progress = false;
        Ok(())
    }

    /// Consumes the transaction, committing or rolling back according to the
    /// current setting (see `drop_behavior`).
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows
    /// callers to see any errors that occur.
    #[inline]
    pub async fn finish(mut self) -> Result<()> {
        self._finish().await
    }

    #[inline]
    async fn _finish(&mut self) -> Result<()> {
        if self.conn.is_autocommit()? {
            self.in_progress = false;
            return Ok(());
        }
        match self.drop_behavior() {
            DropBehavior::Commit => {
                if (self._commit().await).is_err() {
                    self._rollback().await
                } else {
                    Ok(())
                }
            }
            DropBehavior::Rollback => self._rollback().await,
            DropBehavior::Ignore => {
                self.in_progress = false;
                Ok(())
            }
            DropBehavior::Panic => panic!("Transaction dropped unexpectedly."),
        }
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    #[inline]
    fn deref(&self) -> &Connection {
        self.conn
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.in_progress {
            // Completing the transaction requires an HTTP request, which
            // cannot run in a synchronous drop. The connection applies the
            // drop behavior before its next statement; if the connection is
            // dropped too, the server rolls the transaction back when the
            // stream closes or expires (section 4.7).
            self.conn.set_dangling_tx(self.drop_behavior);
        } else {
            self.conn.set_dangling_tx(DropBehavior::Ignore);
        }
    }
}
