//! Two threads use clones of one `turso::Connection`. The clones share one pager
//! and one page cache, so they share the page buffers (`ArenaBuffer`).
//! These tests use only the public `turso` API.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use turso::{Builder, Value};

const BLOB_LEN: usize = 2000;

/// Thread W: `UPDATE` with a blob of the same size, which `overwrite_content`
/// copies into the page buffer in place.
/// Thread F: `Connection::cacheflush`, which copies each dirty page buffer into a
/// WAL frame. Nothing orders the two copies.
///
/// COMMIT writes the page again from memory, so SQL readers do not see the frames
/// that `cacheflush` wrote. The frames stay in the WAL file, so this test reads the
/// file and counts page images that never existed.
#[test]
fn cacheflush_writes_torn_page_images_to_wal_file() {
    const FLUSHES: usize = 20_000;
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime();
    let (_db, conn) = rt.block_on(open_db_with_one_row(&dir));
    // No checkpoint, so the WAL file keeps every frame.
    rt.block_on(conn.execute("PRAGMA wal_autocheckpoint = 0", ()))
        .unwrap();
    // One transaction, so that page 2 stays dirty and every flush copies it.
    // The first UPDATE makes page 2 dirty before the flusher starts.
    rt.block_on(conn.execute("BEGIN IMMEDIATE", ())).unwrap();
    rt.block_on(conn.execute(
        "UPDATE t SET v = ?1 WHERE id = 1",
        vec![Value::Blob(vec![0xAA; BLOB_LEN])],
    ))
    .unwrap();

    let flusher_done = Arc::new(AtomicBool::new(false));
    let writer = {
        let conn = conn.clone();
        let flusher_done = flusher_done.clone();
        std::thread::spawn(move || {
            let rt = runtime();
            let mut stmt = rt
                .block_on(conn.prepare("UPDATE t SET v = ?1 WHERE id = 1"))
                .unwrap();
            let mut updates = 0u64;
            while !flusher_done.load(Ordering::SeqCst) {
                let byte = if updates % 2 == 0 { 0xBB } else { 0xAA };
                rt.block_on(stmt.execute(vec![Value::Blob(vec![byte; BLOB_LEN])]))
                    .unwrap();
                updates += 1;
            }
            updates
        })
    };
    let flusher = {
        let conn = conn.clone();
        std::thread::spawn(move || {
            for _ in 0..FLUSHES {
                conn.cacheflush().unwrap();
            }
            flusher_done.store(true, Ordering::SeqCst);
        })
    };
    flusher.join().unwrap();
    let updates = writer.join().unwrap();
    rt.block_on(conn.execute("COMMIT", ())).unwrap();
    println!("updates={updates}");

    // WAL file: a 32-byte header, then frames of a 24-byte header and one page.
    let wal = std::fs::read(dir.path().join("race.db-wal")).unwrap();
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let (mut page_2_frames, mut torn_frames) = (0usize, 0usize);
    let mut first_torn: Option<String> = None;
    for (idx, frame) in wal[32..].chunks_exact(24 + page_size).enumerate() {
        // Page 2 is the root page of table t, and its only page.
        if u32::from_be_bytes(frame[0..4].try_into().unwrap()) != 2 {
            continue;
        }
        page_2_frames += 1;
        let (mut n_aa, mut n_bb) = (0usize, 0usize);
        for &byte in &frame[24..] {
            match byte {
                0xAA => n_aa += 1,
                0xBB => n_bb += 1,
                _ => {}
            }
        }
        if n_aa > 64 && n_bb > 64 {
            torn_frames += 1;
            first_torn.get_or_insert_with(|| {
                format!("frame {}: {n_aa} bytes 0xAA and {n_bb} bytes 0xBB", idx + 1)
            });
        }
    }
    println!("page 2 frames={page_2_frames} torn={torn_frames} first torn: {first_torn:?}");
    assert_eq!(
        torn_frames, 0,
        "the WAL file holds page images that never existed"
    );
}

/// `Statement::query` and `Statement::execute` first call `reset()`, and `reset()`
/// does not take the per-connection guard that `step()` takes. A reset of an
/// unfinished statement calls `abort()`, which changes the transaction state of the
/// connection while a statement on the other thread is in `step()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_on_one_thread_while_step_on_another() {
    let dir = tempfile::tempdir().unwrap();
    let (_db, conn) = open_db_with_one_row(&dir).await;

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let conn = conn.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut stmt = conn
                .prepare("UPDATE t SET v = ?1 WHERE id = 1")
                .await
                .unwrap();
            let mut errors = std::collections::BTreeMap::<String, u64>::new();
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let byte = if i % 2 == 0 { 0xBB } else { 0xAA };
                if let Err(e) = stmt.execute(vec![Value::Blob(vec![byte; BLOB_LEN])]).await {
                    *errors.entry(format!("{e}")).or_default() += 1;
                }
                i += 1;
            }
            (i, errors)
        })
    };
    let reader = {
        let conn = conn.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut stmt = conn.prepare("SELECT v FROM t WHERE id = 1").await.unwrap();
            let start = Instant::now();
            let mut reads = 0u64;
            let mut errors = std::collections::BTreeMap::<String, u64>::new();
            while start.elapsed() < Duration::from_secs(10) {
                // The statement returned a row in the last loop and did not get to
                // Done, so query() resets an unfinished statement.
                let mut rows = match stmt.query(()).await {
                    Ok(rows) => rows,
                    Err(e) => {
                        *errors.entry(format!("{e}")).or_default() += 1;
                        continue;
                    }
                };
                match rows.next().await {
                    Ok(Some(_)) => reads += 1,
                    Ok(None) => *errors.entry("row 1 not found".into()).or_default() += 1,
                    Err(e) => *errors.entry(format!("{e}")).or_default() += 1,
                }
            }
            stop.store(true, Ordering::Relaxed);
            (reads, errors)
        })
    };
    let reader = reader.await;
    let writer = writer.await;
    println!("reader: {reader:?}");
    println!("writer: {writer:?}");
    reader.expect("reader task panicked");
    writer.expect("writer task panicked");
}

async fn open_db_with_one_row(dir: &tempfile::TempDir) -> (turso::Database, turso::Connection) {
    let path = dir.path().join("race.db");
    let db = Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)", ())
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES (1, ?1)",
        vec![Value::Blob(vec![0xAA; BLOB_LEN])],
    )
    .await
    .unwrap();
    (db, conn)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
