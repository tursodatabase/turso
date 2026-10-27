//! Commits from several connections in one process must all succeed on the
//! Windows IOCP backend (#10909).
//!
//! The reported failure: with `vfs="experimental_win_iocp"`, 1–7 % of commits
//! from 8 threads — one connection each — came back as `IoError: write: Other`,
//! while the same work split across processes passed. No test drove several
//! connections through commits at once, so nothing caught it.
//!
//! These tests assert that every commit succeeds. They deliberately do not
//! retry, because a retry hides the failure rate the issue is about.
//!
//! Round and thread counts come from `TURSO_IOCP_REPRO_ROUNDS` and
//! `TURSO_IOCP_REPRO_THREADS` so the case can be soaked on a Windows machine.
//!
//! Windows-only: the backend sits behind `experimental_win_iocp`, which this
//! crate's `win_iocp` feature enables.

use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, EncryptionKey, EncryptionOpts, LimboError,
    OpenFlags, SqliteDialect, WindowsIOCP, IO,
};

const ROUNDS_ENV: &str = "TURSO_IOCP_REPRO_ROUNDS";
const THREADS_ENV: &str = "TURSO_IOCP_REPRO_THREADS";
/// 32 bytes, hex encoded.
const HEX_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

/// The reported shape: a worker thread, a `Database` and therefore a
/// completion port of its own, all committing to the same file.
#[test]
fn commits_from_threads_with_their_own_databases_all_succeed() -> anyhow::Result<()> {
    assert_concurrent_commits_succeed(Shape {
        database_per_thread: true,
        ..Shape::default()
    })
}

/// Every failing run in the report had encryption enabled. This keeps that
/// combination covered until the cause of #10909 is known.
#[test]
fn commits_from_threads_with_their_own_encrypted_databases_all_succeed() -> anyhow::Result<()> {
    assert_concurrent_commits_succeed(Shape {
        encrypted: true,
        database_per_thread: true,
        ..Shape::default()
    })
}

/// The reported run with the most failures enabled encryption and
/// `multiprocess_wal` together, which reaches the shared WAL coordination
/// file and not just the WAL itself.
#[test]
fn commits_from_threads_with_multiprocess_wal_all_succeed() -> anyhow::Result<()> {
    assert_concurrent_commits_succeed(Shape {
        encrypted: true,
        multiprocess_wal: true,
        database_per_thread: true,
    })
}

/// Connections on one `Database` share a single completion port, so one
/// thread's `step()` reaps another thread's packets. That is a different
/// sharing pattern from the reported one and has to work too.
#[test]
fn commits_from_connections_on_one_database_all_succeed() -> anyhow::Result<()> {
    assert_concurrent_commits_succeed(Shape::default())
}

#[derive(Clone, Copy, Default)]
struct Shape {
    encrypted: bool,
    database_per_thread: bool,
    multiprocess_wal: bool,
}

fn assert_concurrent_commits_succeed(shape: Shape) -> anyhow::Result<()> {
    let (encrypted, database_per_thread) = (shape.encrypted, shape.database_per_thread);
    let (threads, rounds) = (env_usize(THREADS_ENV, 8), env_usize(ROUNDS_ENV, 100));
    let dir = tempfile::tempdir()?;
    let path = dir
        .path()
        .join("concurrent-commits.db")
        .to_str()
        .expect("temp dir path is valid utf-8")
        .to_string();
    create_schema(&path, shape)?;

    let errors: Mutex<Vec<String>> = Mutex::new(Vec::new());
    // One Database for every thread shares one completion port; one per
    // thread gives each its own, like a separate `turso.connect()`.
    let shared = if database_per_thread {
        None
    } else {
        Some(open(&path, shape)?)
    };

    thread::scope(|scope| {
        for worker in 0..threads {
            let errors = &errors;
            let shared = shared.as_ref();
            let path = &path;
            scope.spawn(move || {
                let db = match shared {
                    Some(db) => db.clone(),
                    None => match open(path, shape) {
                        Ok(db) => db,
                        Err(err) => {
                            errors.lock().unwrap().push(format!("open: {err}"));
                            return;
                        }
                    },
                };
                let conn = match connect(&db, encrypted) {
                    Ok(conn) => conn,
                    Err(err) => {
                        errors.lock().unwrap().push(format!("connect: {err}"));
                        return;
                    }
                };
                let mut busy_retries = 0;
                for round in 0..rounds {
                    if let Err(err) = commit_retrying_busy(&conn, worker, round, &mut busy_retries)
                    {
                        errors.lock().unwrap().push(err.to_string());
                        let _ = conn.execute("ROLLBACK");
                    }
                }
                if busy_retries > 0 {
                    println!("worker {worker}: {busy_retries} retries after Busy");
                }
            });
        }
    });

    let errors = errors.into_inner().unwrap();
    anyhow::ensure!(
        errors.is_empty(),
        "{} of {} commits failed, first {}: {:?}",
        errors.len(),
        threads * rounds,
        errors.len().min(5),
        &errors[..errors.len().min(5)],
    );

    let db = open(&path, encrypted)?;
    let conn = connect(&db, encrypted)?;
    let rows = row_count(&conn)?;
    anyhow::ensure!(
        rows == (threads * rounds) as i64,
        "expected {} rows, found {rows}",
        threads * rounds,
    );
    Ok(())
}

fn commit_once(conn: &Arc<Connection>, worker: usize, round: usize) -> turso_core::Result<()> {
    conn.execute("BEGIN IMMEDIATE")?;
    conn.execute(format!("INSERT INTO t VALUES ({worker}, {round})"))?;
    conn.execute("COMMIT")?;
    Ok(())
}

/// The core returns `Busy` as soon as another connection holds the write lock:
/// `PRAGMA busy_timeout` only records the value, and the bindings are the ones
/// that wait and retry. Do the same here so the test measures lost commits
/// rather than lock contention.
///
/// Only `Busy` is retried. Retrying anything else would hide the failures
/// #10909 is about. The budget is per commit: a worker that hits `Busy`
/// hundreds of times over the whole run has not failed, only lost the race.
const MAX_BUSY_RETRIES: usize = 1000;

fn commit_retrying_busy(
    conn: &Arc<Connection>,
    worker: usize,
    round: usize,
    busy_retries: &mut usize,
) -> anyhow::Result<()> {
    let mut attempts = 0;
    loop {
        match commit_once(conn, worker, round) {
            Ok(()) => return Ok(()),
            Err(LimboError::Busy) => {
                attempts += 1;
                *busy_retries += 1;
                anyhow::ensure!(
                    attempts <= MAX_BUSY_RETRIES,
                    "still Busy after {MAX_BUSY_RETRIES} retries"
                );
                let _ = conn.execute("ROLLBACK");
                // Grow the wait so a starved worker cannot keep losing every
                // race, and jitter it so the workers do not wake together.
                // Cheap arithmetic because the crate has no random source in
                // its test dependencies.
                let backoff_ms = 1u64 << attempts.min(4);
                let jitter_ms = (worker as u64 * 31 + round as u64 * 17 + attempts as u64 * 7) % 5;
                thread::sleep(Duration::from_millis(backoff_ms + jitter_ms));
            }
            Err(err) => return Err(err.into()),
        }
    }
}

fn create_schema(path: &str, shape: Shape) -> anyhow::Result<()> {
    let db = open(path, shape)?;
    let conn = connect(&db, shape.encrypted)?;
    conn.execute("PRAGMA journal_mode = WAL")?;
    conn.execute("CREATE TABLE t (w INTEGER, i INTEGER, PRIMARY KEY (w, i))")?;
    Ok(())
}

fn row_count(conn: &Arc<Connection>) -> anyhow::Result<i64> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t")?;
    let mut count = None;
    stmt.run_with_row_callback(|row| {
        count = Some(row.get::<i64>(0)?);
        Ok(())
    })?;
    count.ok_or_else(|| anyhow::anyhow!("COUNT(*) returned no rows"))
}

/// A fresh backend per `Database`, which is what a separate connect gives a
/// caller: the completion port is not shared between them.
fn iocp() -> Arc<dyn IO> {
    Arc::new(WindowsIOCP::new().expect("the Windows IOCP backend must initialize"))
}

fn open(path: &str, shape: Shape) -> anyhow::Result<Arc<Database>> {
    let mut opts = DatabaseOpts::new();
    if shape.encrypted {
        opts = opts.with_encryption(true);
    }
    if shape.multiprocess_wal {
        opts = opts.with_multiprocess_wal(true);
    }
    Ok(Database::open_file_with_flags(
        iocp(),
        path,
        OpenFlags::default(),
        opts,
        encryption_opts(shape.encrypted),
        Arc::new(SqliteDialect),
    )?)
}

fn connect(db: &Arc<Database>, encrypted: bool) -> anyhow::Result<Arc<Connection>> {
    let opts = if encrypted {
        ConnectOpts::new().with_encryption_key(Some(EncryptionKey::from_hex_string(HEX_KEY)?))
    } else {
        ConnectOpts::new()
    };
    Ok(db.connect(opts)?)
}

fn encryption_opts(encrypted: bool) -> Option<EncryptionOpts> {
    encrypted.then(|| EncryptionOpts {
        cipher: "aes256gcm".to_string(),
        hexkey: HEX_KEY.to_string(),
    })
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(default)
}
