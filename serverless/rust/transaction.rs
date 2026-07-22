use std::ops::Deref;

use crate::connection::Connection;
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

/// An interactive transaction.
///
/// The transaction spans multiple HTTP requests: the driver sends `BEGIN`
/// on the connection's stream and the server keeps the stream (and thereby
/// the transaction) alive across requests via the baton (section 4.7 of
/// the protocol specification).
///
/// Call [`commit`](Transaction::commit) to commit the transaction. A
/// transaction that is dropped without an explicit commit or rollback is
/// rolled back: on the next use of the connection if there is one, or by
/// the server when the stream is closed or expires.
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    finished: bool,
}

impl<'conn> Transaction<'conn> {
    pub(crate) async fn new(
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
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Rollback requires an HTTP request, which cannot run in a
            // synchronous drop. The connection sends ROLLBACK before its
            // next statement; if the connection is dropped too, the server
            // rolls the transaction back when the stream closes or expires
            // (section 4.7).
            self.conn.set_needs_rollback();
        }
    }
}
