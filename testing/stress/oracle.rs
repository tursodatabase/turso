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

use crate::conn::{StressConn, StressDb};
use turso::Value;
use turso_stress::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use turso_stress::sync::Arc;
use turso_stress::sync::AsyncMutex as Mutex;
use turso_stress::ThreadId;

/// Per-writer watermarks carried in memory across database reopens.
///
/// The durable watermark alone cannot detect a lost committed suffix: rows
/// and watermark commit atomically, so a database that lost its last
/// committed transactions during recovery still looks self-consistent. The
/// reopen is in-process, so memory survives it: the next attach can demand
/// that the durable watermark exactly match what this writer had committed
/// before the reopen.
pub struct RecoveryExpectations {
    /// Expected watermark per writer; -1 means no expectation yet.
    seqs: Vec<AtomicI64>,
    /// The writer's last commit outcome stayed unknown, so the durable
    /// watermark is allowed to be one higher than expected.
    ambiguous: Vec<AtomicBool>,
}

impl RecoveryExpectations {
    pub fn new(nr_writers: usize) -> Arc<Self> {
        Arc::new(Self {
            seqs: (0..nr_writers).map(|_| AtomicI64::new(-1)).collect(),
            ambiguous: (0..nr_writers).map(|_| AtomicBool::new(false)).collect(),
        })
    }

    fn store(&self, writer: usize, seq: i64, ambiguous: bool) {
        self.seqs[writer].store(seq, Ordering::Release);
        self.ambiguous[writer].store(ambiguous, Ordering::Release);
    }

    fn load(&self, writer: usize) -> Option<(i64, bool)> {
        let seq = self.seqs[writer].load(Ordering::Acquire);
        (seq >= 0).then(|| (seq, self.ambiguous[writer].load(Ordering::Acquire)))
    }
}

pub struct Oracle {
    /// Dedicated connection, used for nothing but oracle transactions. On a
    /// connection shared with the random DML, a failed oracle transaction
    /// can be left open and a later unrelated COMMIT can land parts of it,
    /// breaking the row/watermark atomicity the checks depend on.
    conn: StressConn,
    writer: usize,
    committed_seq: i64,
    /// An oracle transaction failed after BEGIN and we do not yet know
    /// whether it landed.
    ambiguous: bool,
}

/// Create the oracle tables. Called once during setup, before the worker
/// threads start. Retries lock contention; any other error is returned for
/// the caller to handle like the random-schema creation loop does.
pub async fn init_schema(conn: &StressConn) -> turso::connection::Result<()> {
    let statements = [
        "CREATE TABLE IF NOT EXISTS stress_oracle(id INTEGER PRIMARY KEY, writer INTEGER, seq INTEGER)",
        "CREATE INDEX IF NOT EXISTS stress_oracle_writer_seq ON stress_oracle(writer, seq)",
        "CREATE TABLE IF NOT EXISTS stress_oracle_progress(writer INTEGER PRIMARY KEY, committed_seq INTEGER)",
    ];
    for sql in statements {
        let mut retries = 0;
        loop {
            match conn.execute(sql, ()).await {
                Ok(_) => break,
                Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) if retries < 10 => {
                    retries += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
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
    pub async fn attach(
        db: &Arc<Mutex<StressDb>>,
        thread: &ThreadId,
        writer: usize,
        busy_timeout: u64,
        expectations: &RecoveryExpectations,
    ) -> Option<Oracle> {
        let own_conn = match StressDb::connect(db, thread.clone(), busy_timeout).await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("oracle: failed to connect for writer {writer}: {e}");
                return None;
            }
        };
        let conn = &own_conn;
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
        // The self-consistency check above cannot see a lost committed
        // suffix (rows and watermark vanish together), so also compare
        // against the watermark this writer held before the reopen.
        if let Some((expected, ambiguous)) = expectations.load(writer) {
            turso_macros::turso_assert!(
                watermark == expected || (ambiguous && watermark == expected + 1),
                "oracle: recovery preserved this writer's committed watermark across reopen",
                { "thread": thread, "writer": writer, "watermark": watermark, "expected": expected, "ambiguous_commit": ambiguous }
            );
        }
        // The durable state is the ground truth from here on; remember it in
        // case this batch ends without reaching a clean detach.
        expectations.store(writer, watermark, false);
        Some(Oracle {
            conn: own_conn,
            writer,
            committed_seq: watermark,
            ambiguous: false,
        })
    }

    /// Settle an ambiguous commit: roll back any transaction still open on
    /// this connection, then probe whether the row landed durably. Returns
    /// false while the ambiguity could not be resolved yet.
    async fn resolve(&mut self) -> bool {
        if !self.ambiguous {
            return true;
        }
        // Harmless if no transaction is open; guarantees the probe below
        // reads committed state instead of our own uncommitted row. Because
        // the connection is exclusively ours, the probe outcome is
        // definitive: the transaction either committed whole or not at all.
        let _ = self.conn.execute("ROLLBACK", ()).await;
        let candidate = self.committed_seq + 1;
        let w = self.writer;
        match query_i64(
            &self.conn,
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
    pub async fn write(&mut self, thread: &ThreadId) {
        if !self.resolve().await {
            return;
        }
        let w = self.writer;
        let seq = self.committed_seq + 1;
        match self.conn.execute("BEGIN IMMEDIATE", ()).await {
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
            if let Err(e) = self.conn.execute(sql, ()).await {
                // The transaction may be left open with partial changes; a
                // later resolve settles what actually landed.
                self.ambiguous = true;
                let _ = self.conn.execute("ROLLBACK", ()).await;
                if let turso::Error::Corrupt(e) = e {
                    turso_macros::turso_assert_unreachable!("corrupt error in oracle transaction", { "thread": thread, "writer": w, "error": e });
                }
                return;
            }
        }
        match self.conn.execute("COMMIT", ()).await {
            Ok(_) => {
                self.committed_seq = seq;
                // Our own commit must be immediately visible.
                if let Ok(Some(n)) = query_i64(
                    &self.conn,
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
                // The commit may still have landed; settle it later.
                self.ambiguous = true;
                let _ = self.conn.execute("ROLLBACK", ()).await;
                if let turso::Error::Corrupt(e) = e {
                    turso_macros::turso_assert_unreachable!("corrupt error committing oracle transaction", { "thread": thread, "writer": w, "error": e });
                }
            }
        }
    }

    /// Record this writer's final watermark before the database is reopened
    /// so the next attach can verify that recovery preserved it. A last
    /// resolve attempt settles any ambiguous commit; if it stays unresolved,
    /// the next attach accepts the watermark being one higher.
    pub async fn detach(mut self, expectations: &RecoveryExpectations) {
        let resolved = self.resolve().await;
        expectations.store(self.writer, self.committed_seq, !resolved);
    }

    /// Spot-check that every committed row is still present with no gaps.
    pub async fn verify(&mut self, thread: &ThreadId) {
        if !self.resolve().await {
            return;
        }
        let w = self.writer;
        let Ok(Some(count)) = query_i64(
            &self.conn,
            &format!("SELECT count(*) FROM stress_oracle WHERE writer = {w}"),
        )
        .await
        else {
            return;
        };
        let Ok(Some(max_seq)) = query_i64(
            &self.conn,
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
pub async fn verify_all(conn: &StressConn, nr_threads: usize, expectations: &RecoveryExpectations) {
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
        if let Some((expected, ambiguous)) = expectations.load(writer) {
            turso_macros::turso_assert!(
                watermark == expected || (ambiguous && watermark == expected + 1),
                "oracle: final durable watermark matches the writer's last committed watermark",
                { "writer": writer, "watermark": watermark, "expected": expected, "ambiguous_commit": ambiguous }
            );
        }
        println!(
            "oracle: writer {writer}: count={count:?} max_seq={max_seq:?} watermark={watermark}"
        );
    }
}
