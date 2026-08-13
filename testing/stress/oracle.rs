//! Lost-write oracle for the stress loop.
//!
//! Each thread owns a writer id and periodically commits, in one
//! transaction, a new (writer, seq) row into `stress_oracle` and the same
//! seq into `stress_oracle_progress` — the durable watermark. Because data
//! and watermark commit atomically, `COUNT(*)` and `MAX(seq)` for a writer
//! must exactly equal the watermark at every point in time, across
//! reconnects and database reopens. A lost or skipped committed write (the
//! WAL-reset bug class) breaks that equality long before integrity_check
//! notices anything.
//!
//! The oracle tables are fixed and separate from the random schema, so the
//! random DML churn never touches them.
//!
//! A COMMIT that returns an error may still have landed (for example an
//! injected fsync fault after the commit frame was written), so a failed
//! commit marks the oracle ambiguous. Before the next write or check, the
//! ambiguity is resolved by rolling back any transaction left open on the
//! connection and probing whether the row is durably visible.

use crate::conn::StressConn;
use turso::Value;
use turso_stress::ThreadId;

pub struct Oracle {
    writer: usize,
    committed_seq: i64,
    /// A COMMIT failed and we do not yet know whether it landed.
    ambiguous: bool,
}

/// Create the oracle tables. Called once during setup, before the worker
/// threads start.
pub async fn init_schema(conn: &StressConn) -> turso::connection::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS stress_oracle(id INTEGER PRIMARY KEY, writer INTEGER, seq INTEGER)",
        (),
    )
    .await?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS stress_oracle_writer_seq ON stress_oracle(writer, seq)",
        (),
    )
    .await?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS stress_oracle_progress(writer INTEGER PRIMARY KEY, committed_seq INTEGER)",
        (),
    )
    .await?;
    Ok(())
}

/// Single-value query. Ok(None) means the read could not run (busy) or
/// returned no row; the caller skips its check in that case.
async fn query_i64(conn: &StressConn, sql: &str) -> turso::connection::Result<Option<i64>> {
    let mut rows = match conn.query(sql, ()).await {
        Ok(rows) => rows,
        Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    match rows.next().await {
        Ok(Some(row)) => match row.get_value(0) {
            Ok(Value::Integer(v)) => Ok(Some(v)),
            _ => Ok(None),
        },
        Ok(None) => Ok(None),
        Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

impl Oracle {
    /// Read the durable watermark for this writer and check that all of its
    /// committed state survived. Runs at the start of every batch, which is
    /// right after a database reopen for every batch but the first, so this
    /// is also the recovery check. Returns None when the oracle state could
    /// not be read; the batch then runs without the oracle.
    pub async fn attach(conn: &StressConn, thread: &ThreadId, writer: usize) -> Option<Oracle> {
        for _ in 0..10 {
            match conn
                .execute(
                    &format!("INSERT OR IGNORE INTO stress_oracle_progress VALUES ({writer}, 0)"),
                    (),
                )
                .await
            {
                Ok(_) => break,
                Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => continue,
                Err(turso::Error::Corrupt(e)) => {
                    turso_macros::turso_assert_unreachable!("corrupt error seeding oracle watermark", { "thread": thread, "writer": writer, "error": e });
                }
                Err(e) => {
                    eprintln!("oracle: failed to seed watermark for writer {writer}: {e}");
                    return None;
                }
            }
        }
        let watermark = query_i64(
            conn,
            &format!("SELECT committed_seq FROM stress_oracle_progress WHERE writer = {writer}"),
        )
        .await
        .ok()
        .flatten()?;
        let count = query_i64(
            conn,
            &format!("SELECT count(*) FROM stress_oracle WHERE writer = {writer}"),
        )
        .await
        .ok()
        .flatten()?;
        let max_seq = query_i64(
            conn,
            &format!("SELECT coalesce(max(seq), 0) FROM stress_oracle WHERE writer = {writer}"),
        )
        .await
        .ok()
        .flatten()?;
        turso_macros::turso_assert!(
            count == watermark && max_seq == watermark,
            "oracle: committed rows exactly match the durable watermark after reopen",
            { "thread": thread, "writer": writer, "count": count, "max_seq": max_seq, "watermark": watermark }
        );
        Some(Oracle {
            writer,
            committed_seq: watermark,
            ambiguous: false,
        })
    }

    /// Settle an ambiguous commit: roll back any transaction still open on
    /// this connection, then probe whether the row landed durably. Returns
    /// false while the ambiguity could not be resolved yet.
    async fn resolve(&mut self, conn: &StressConn) -> bool {
        if !self.ambiguous {
            return true;
        }
        // Harmless if no transaction is open; guarantees the probe below
        // reads committed state instead of our own uncommitted row.
        let _ = conn.execute("ROLLBACK", ()).await;
        let candidate = self.committed_seq + 1;
        let w = self.writer;
        match query_i64(
            conn,
            &format!("SELECT count(*) FROM stress_oracle WHERE writer = {w} AND seq = {candidate}"),
        )
        .await
        {
            Ok(Some(1)) => {
                // The commit landed after all.
                self.committed_seq = candidate;
                self.ambiguous = false;
                true
            }
            Ok(Some(_)) => {
                // The transaction was rolled back; the seq will be reused.
                self.ambiguous = false;
                true
            }
            _ => false,
        }
    }

    /// Commit one oracle row and the watermark in a single transaction.
    pub async fn write(&mut self, conn: &StressConn, thread: &ThreadId) {
        if !self.resolve(conn).await {
            return;
        }
        let w = self.writer;
        let seq = self.committed_seq + 1;
        match conn.execute("BEGIN IMMEDIATE", ()).await {
            Ok(_) => {}
            Err(turso::Error::Corrupt(e)) => {
                turso_macros::turso_assert_unreachable!("corrupt error starting oracle transaction", { "thread": thread, "writer": w, "error": e });
            }
            Err(_) => return,
        }
        let statements = [
            format!("INSERT INTO stress_oracle(writer, seq) VALUES ({w}, {seq})"),
            format!("UPDATE stress_oracle_progress SET committed_seq = {seq} WHERE writer = {w}"),
        ];
        for sql in &statements {
            if let Err(e) = conn.execute(sql, ()).await {
                let _ = conn.execute("ROLLBACK", ()).await;
                if let turso::Error::Corrupt(e) = e {
                    turso_macros::turso_assert_unreachable!("corrupt error in oracle transaction", { "thread": thread, "writer": w, "error": e });
                }
                return;
            }
        }
        match conn.execute("COMMIT", ()).await {
            Ok(_) => {
                self.committed_seq = seq;
                // Our own commit must be immediately visible.
                if let Ok(Some(n)) = query_i64(
                    conn,
                    &format!(
                        "SELECT count(*) FROM stress_oracle WHERE writer = {w} AND seq = {seq}"
                    ),
                )
                .await
                {
                    turso_macros::turso_assert!(
                        n == 1,
                        "oracle: a committed row is immediately visible to its writer",
                        { "thread": thread, "writer": w, "seq": seq, "count": n }
                    );
                }
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                if let turso::Error::Corrupt(e) = e {
                    turso_macros::turso_assert_unreachable!("corrupt error committing oracle transaction", { "thread": thread, "writer": w, "error": e });
                }
                // The commit may still have landed; settle it later.
                self.ambiguous = true;
            }
        }
    }

    /// Spot-check that every committed row is still present with no gaps.
    pub async fn verify(&mut self, conn: &StressConn, thread: &ThreadId) {
        if !self.resolve(conn).await {
            return;
        }
        let w = self.writer;
        let Ok(Some(count)) = query_i64(
            conn,
            &format!("SELECT count(*) FROM stress_oracle WHERE writer = {w}"),
        )
        .await
        else {
            return;
        };
        let Ok(Some(max_seq)) = query_i64(
            conn,
            &format!("SELECT coalesce(max(seq), 0) FROM stress_oracle WHERE writer = {w}"),
        )
        .await
        else {
            return;
        };
        turso_macros::turso_assert!(
            count == self.committed_seq && max_seq == self.committed_seq,
            "oracle: no lost committed writes: rows exactly match the watermark",
            { "thread": thread, "writer": w, "count": count, "max_seq": max_seq, "watermark": self.committed_seq }
        );
    }
}

/// Final check from durable state only, after all worker threads are done:
/// for every writer, the rows must exactly match the durable watermark.
pub async fn verify_all(conn: &StressConn, nr_threads: usize) {
    for writer in 0..nr_threads {
        let Ok(Some(watermark)) = query_i64(
            conn,
            &format!("SELECT committed_seq FROM stress_oracle_progress WHERE writer = {writer}"),
        )
        .await
        else {
            eprintln!("oracle: no durable watermark for writer {writer}; skipping final check");
            continue;
        };
        let count = query_i64(
            conn,
            &format!("SELECT count(*) FROM stress_oracle WHERE writer = {writer}"),
        )
        .await
        .ok()
        .flatten();
        let max_seq = query_i64(
            conn,
            &format!("SELECT coalesce(max(seq), 0) FROM stress_oracle WHERE writer = {writer}"),
        )
        .await
        .ok()
        .flatten();
        turso_macros::turso_assert!(
            count == Some(watermark) && max_seq == Some(watermark),
            "oracle: final state has no lost committed writes for any writer",
            { "writer": writer, "count": count, "max_seq": max_seq, "watermark": watermark }
        );
        println!(
            "oracle: writer {writer}: count={count:?} max_seq={max_seq:?} watermark={watermark}"
        );
    }
}
