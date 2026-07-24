#![cfg(feature = "io_memory_yield")]

//! User-facing reproducer for the "suspended pager state machine vs. cache
//! clear" hazard family addressed by PR #8012 — driven exclusively through
//! public APIs: SQL over `Connection`/`Statement::step()` with the
//! `MemoryYieldIO` backend supplying the I/O-boundary yields.
//!
//! Scenario: a write statement is suspended at a `StepResult::IO` boundary
//! (with the freelist seeded below, the pager's `allocate_page` machine is
//! suspended in `SearchAvailableFreeListLeaf` — freelist trunk page pinned —
//! or `ReuseFreelistLeaf` — trunk + leaf pinned — for nearly every boundary of
//! the INSERT). This is legal API usage: any binding whose driver interleaves
//! statements on a connection can produce it.
//!
//! While the statement is suspended, the same connection runs
//! `PRAGMA wal_checkpoint(PASSIVE)`. op_checkpoint's only guard is
//! `!auto_commit`, so the implicit transaction of the suspended autocommit
//! writer sails past it. The checkpoint's `Finalize { clear_page_cache: true }`
//! phase then calls `invalidate_all_cursors()` *before* attempting
//! `PageCache::clear(false)`. The clear itself fails (the suspended
//! `allocate_page`/`free_page` machine holds a dirty page 1 via
//! `HeaderRefMut`), the checkpoint reports "busy" — but the cursor
//! invalidation has already happened and nothing reset the suspended pager
//! machines. Resuming the statement then panics:
//!
//!     panicked at core/storage/btree.rs:8250: self.current_page >= 0
//!
//! at every single I/O boundary of the INSERT.
//!
//! This is the same root cause PR #8012 describes — a suspended state machine
//! surviving into cache-clear machinery that frees state under it — reached
//! without touching any pager internals. (The PR's exact
//! "Freelist trunk page is not loaded" assert is only reachable when the
//! *page buffers* are also freed; on current main the dirty header page makes
//! that particular clear bail out, so the cursor assert fires first.)
//!
//! The test sweeps every I/O boundary of the INSERT, mirroring
//! `abandoned_statement_pager.rs`: whatever boundary the statement was
//! suspended at, a checkpoint on the same connection must leave the statement
//! resumable, the row must land, and the database must stay intact.
//!
//! Run with:
//!   cargo test -p core_tester --test integration_tests \
//!       --features io_memory_yield suspended_statement_checkpoint

use std::sync::Arc;

use turso_core::{Connection, Database, IO, MemoryYieldIO, SqliteDialect, StepResult};

fn open_conn(io: Arc<MemoryYieldIO>, path: &str) -> Arc<Connection> {
    Database::open_file(io, path, Arc::new(SqliteDialect))
        .unwrap()
        .connect()
        .unwrap()
}

fn integrity_check(conn: &Arc<Connection>) -> Vec<String> {
    conn.prepare("PRAGMA integrity_check")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].to_string())
        .collect()
}

fn ids(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    conn.prepare(sql)
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].as_int().unwrap())
        .collect()
}

/// Seed a database whose freelist has a trunk page with leaf pointers, so a
/// subsequent INSERT allocates from the freelist: `allocate_page` reads the
/// trunk (pinning it, state `SearchAvailableFreeListLeaf`), then a leaf
/// (state `ReuseFreelistLeaf`) — the exact suspended-machine states PR #8012
/// is about. The final TRUNCATE checkpoint leaves a clean WAL so the victim
/// connection starts from a cold, consistent state.
fn seed_freelist_db(io: &Arc<MemoryYieldIO>, path: &str) {
    let conn = open_conn(io.clone(), path);
    conn.execute("PRAGMA page_size=512").unwrap();
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES
         (1, zeroblob(50000)),
         (2, zeroblob(50000)),
         (3, zeroblob(50000)),
         (4, zeroblob(50000)),
         (5, zeroblob(50000))",
    )
    .unwrap();
    conn.execute("DELETE FROM t WHERE id IN (2,3,4)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

/// A checkpoint issued on a connection while one of its write statements is
/// suspended at an I/O boundary must not poison that statement: whatever the
/// checkpoint reports (today: "busy"), the suspended statement must resume,
/// complete, and leave the database intact.
///
/// On current main (and on PR #8012's branch, which does not touch the
/// checkpoint path) this panics at the FIRST boundary:
/// `self.current_page >= 0` (btree.rs), because checkpoint Finalize
/// invalidates the suspended statement's cursors before its cache clear
/// bails out on the dirty header page, and no teardown ever runs for the
/// suspended statement's pager machines.
#[test]
fn test_wal_checkpoint_with_suspended_write_statement_keeps_statement_resumable() {
    for target_io in 1..=512 {
        let io = Arc::new(MemoryYieldIO::new());
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("suspended-ckpt-{target_io}.db"));
        let path = path.to_str().unwrap();

        seed_freelist_db(&io, path);

        // Fresh connection: cold page cache, so every page the INSERT touches
        // (freelist trunk included) is read through a deferred completion and
        // yields StepResult::IO.
        let conn = open_conn(io.clone(), path);

        // Autocommit write: allocates ~100 pages for the overflow chain, all
        // served from the freelist seeded above.
        let mut stmt = conn
            .prepare("INSERT INTO t VALUES (100, zeroblob(50000))")
            .unwrap();

        // Step to the target_io-th I/O boundary and stop there, completion
        // still pending — the statement is now suspended mid-execution
        // (mid-allocate_page for nearly every boundary of this INSERT).
        let mut io_count = 0;
        let mut suspended = false;
        loop {
            match stmt.step().unwrap() {
                StepResult::IO => {
                    io_count += 1;
                    if io_count == target_io {
                        suspended = true;
                        break;
                    }
                    io.step().unwrap();
                }
                StepResult::Done => break,
                StepResult::Yield => {}
                other => panic!("unexpected step result: {other:?}"),
            }
        }
        if !suspended {
            // INSERT finished before reaching target_io I/Os: sweep done.
            assert!(
                target_io > 1,
                "INSERT finished without any I/O; the sweep never ran"
            );
            break;
        }

        // Interleave a checkpoint on the SAME connection. Today this is
        // accepted (op_checkpoint only rejects explicit transactions) and
        // reports "busy". Its outcome is immaterial here — a fix may keep the
        // busy report or reject it outright — the property under test is that
        // it must not corrupt the suspended statement.
        let _ = conn
            .prepare("PRAGMA wal_checkpoint(PASSIVE)")
            .unwrap()
            .run_collect_rows();

        // Resume the suspended INSERT to completion. On main this panics at
        // btree.rs `self.current_page >= 0` for every target_io.
        loop {
            match stmt
                .step()
                .unwrap_or_else(|e| panic!("target_io={target_io}: resumed INSERT failed: {e}"))
            {
                StepResult::IO => io.step().unwrap(),
                StepResult::Done => break,
                StepResult::Yield => {}
                other => panic!("unexpected step result on resume: {other:?}"),
            }
        }
        drop(stmt);

        assert_eq!(
            ids(&conn, "SELECT id FROM t ORDER BY id"),
            vec![1, 5, 100],
            "target_io={target_io}: INSERT resumed after checkpoint must commit its row"
        );
        assert_eq!(
            integrity_check(&conn),
            vec!["ok"],
            "target_io={target_io}: checkpoint during suspended INSERT corrupted the database"
        );
    }
}
