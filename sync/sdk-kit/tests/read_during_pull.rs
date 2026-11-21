//! A read on another connection while a pull is applying remote pages. MemoryYieldIO holds every
//! read and write until `IO::step`, so the test stops the pull partway through its apply and runs
//! the read at that point, every run.

#[allow(dead_code, unused_imports)]
#[path = "../../engine/tests/mvcc_logical_pull/harness/mod.rs"]
mod harness;

use std::sync::Arc;

use harness::{InProcessServer, Remote};
use turso_core::{MemoryYieldIO, OpenFlags, Value, IO};
use turso_sdk_kit::rsapi::{TursoConnection, TursoDatabaseConfig, TursoStatusCode};
use turso_sync_engine::database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo};
use turso_sync_sdk_kit::rsapi::{TursoDatabaseSync, TursoDatabaseSyncConfig};
use turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest;
use turso_sync_sdk_kit::turso_async_operation::{
    TursoAsyncOperationResult, TursoDatabaseAsyncOperation,
};

#[test]
fn read_while_a_pull_applies_remote_pages() {
    let remote =
        Remote::new_wal(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"]).unwrap();
    let server = InProcessServer::pages_only(remote.clone());
    let io = Arc::new(MemoryYieldIO::new());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("local.db").to_str().unwrap().to_string();
    let sync = open(&path, io.clone(), None);

    run(&sync, &server, &io, &sync.create());
    let conn = connect(&sync, &server, &io);
    assert_eq!(read(&conn, &io), Ok(vec!["first".to_string()]));

    remote
        .execute_transaction(&["INSERT INTO t VALUES ('second')"])
        .unwrap();
    let wait = sync.wait_changes();
    run(&sync, &server, &io, &wait);
    let TursoAsyncOperationResult::Changes { changes } = wait.take_result().unwrap() else {
        panic!("wait_changes returned no changes");
    };
    let apply = sync.apply_changes(changes);
    // The apply now waits on its first disk read or write, which MemoryYieldIO holds until
    // `io.step()`.
    assert_eq!(apply.resume().unwrap(), TursoStatusCode::Io);

    // The apply has not committed, so the read returns the rows from before the pull. With the
    // SDK's SyncBusyGate held, it fails with "database is locked" instead.
    assert_eq!(read(&conn, &io), Ok(vec!["first".to_string()]));

    run(&sync, &server, &io, &apply);
    assert_eq!(
        read(&conn, &io),
        Ok(vec!["first".to_string(), "second".to_string()])
    );
}

/// The app holds a write transaction when a pull starts. The pull waits for the lock and
/// returns to its caller meanwhile, so the app, on the same thread, can commit.
#[test]
fn pull_waits_while_the_app_holds_a_write_transaction() {
    let remote =
        Remote::new_wal(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"]).unwrap();
    let server = InProcessServer::pages_only(remote.clone());
    let io = Arc::new(MemoryYieldIO::new());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("local.db").to_str().unwrap().to_string();
    let sync = open(&path, io.clone(), None);

    run(&sync, &server, &io, &sync.create());
    let conn = connect(&sync, &server, &io);
    execute(&conn, &io, "BEGIN IMMEDIATE");
    execute(&conn, &io, "INSERT INTO t VALUES ('app')");

    remote
        .execute_transaction(&["INSERT INTO t VALUES ('second')"])
        .unwrap();
    let wait = sync.wait_changes();
    run(&sync, &server, &io, &wait);
    let TursoAsyncOperationResult::Changes { changes } = wait.take_result().unwrap() else {
        panic!("wait_changes returned no changes");
    };
    let apply = sync.apply_changes(changes);

    // Without the wait, the pull fails with Busy on its first attempt to start a WAL transaction.
    for _ in 0..50 {
        assert_eq!(run_once(&sync, &server, &io, &apply), TursoStatusCode::Io);
    }

    execute(&conn, &io, "COMMIT");
    run(&sync, &server, &io, &apply);
    assert_eq!(
        read(&conn, &io),
        Ok(vec![
            "app".to_string(),
            "first".to_string(),
            "second".to_string()
        ])
    );
    assert_eq!(
        pending_cdc_operations(&sync, &server, &io),
        1,
        "the app's row must be waiting to be pushed"
    );
}

/// The SDK holds its SyncBusyGate for a whole checkpoint, on a WAL database too. The final
/// TRUNCATE checkpoint fails with Busy if any other connection reads or writes, so the app's
/// statements must fail with "database is locked" instead. MemoryYieldIO stops the checkpoint at
/// each disk write, and the test runs an app write at every stop.
#[test]
fn checkpoint_rejects_app_writes_and_truncates_the_wal() {
    let remote =
        Remote::new_wal(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"]).unwrap();
    let server = InProcessServer::pages_only(remote);
    let io = Arc::new(MemoryYieldIO::new());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("local.db").to_str().unwrap().to_string();
    let sync = open(&path, io.clone(), None);

    run(&sync, &server, &io, &sync.create());
    let conn = connect(&sync, &server, &io);
    execute(&conn, &io, "INSERT INTO t VALUES ('pre')");

    let checkpoint = sync.checkpoint();
    let mut rejected_writes = 0;
    while run_once(&sync, &server, &io, &checkpoint) == TursoStatusCode::Io {
        let error = try_execute(&conn, &io, "INSERT INTO t VALUES ('during checkpoint')")
            .expect_err("an app write during the checkpoint must fail");
        assert!(error.contains("database is locked"), "{error}");
        rejected_writes += 1;
    }
    assert!(rejected_writes > 0);

    assert_eq!(
        wal_frames(&conn, &io),
        0,
        "the checkpoint must truncate the WAL"
    );
    assert_eq!(
        read(&conn, &io),
        Ok(vec!["first".to_string(), "pre".to_string()])
    );
    // The gate is open again.
    execute(&conn, &io, "INSERT INTO t VALUES ('after')");
}

/// Number of frames in the WAL, from the second column of `PRAGMA wal_checkpoint`.
fn wal_frames(conn: &TursoConnection, io: &MemoryYieldIO) -> i64 {
    let mut stmt = conn
        .prepare_single("PRAGMA wal_checkpoint(PASSIVE)")
        .unwrap();
    let mut frames = None;
    loop {
        match stmt.step(None).unwrap() {
            TursoStatusCode::Row => match stmt.row_value(1).unwrap() {
                Value::Numeric(turso_core::Numeric::Integer(value)) => frames = Some(value),
                other => panic!("unexpected value {other:?}"),
            },
            TursoStatusCode::Io => io.step().unwrap(),
            TursoStatusCode::Done => return frames.expect("no row"),
        }
    }
}

fn try_execute(conn: &TursoConnection, io: &MemoryYieldIO, sql: &str) -> Result<(), String> {
    let mut stmt = conn.prepare_single(sql).map_err(|e| e.to_string())?;
    loop {
        match stmt.step(None).map_err(|e| e.to_string())? {
            TursoStatusCode::Io => io.step().unwrap(),
            TursoStatusCode::Done => return Ok(()),
            TursoStatusCode::Row => panic!("{sql} returned a row"),
        }
    }
}

fn execute(conn: &TursoConnection, io: &MemoryYieldIO, sql: &str) {
    let mut stmt = conn.prepare_single(sql).unwrap();
    loop {
        match stmt.step(None).unwrap() {
            TursoStatusCode::Io => io.step().unwrap(),
            TursoStatusCode::Done => return,
            TursoStatusCode::Row => panic!("{sql} returned a row"),
        }
    }
}

/// The app holds a write transaction on the same thread that drives a logical MVCC pull. While
/// the pull waits for the app's lock, each `resume()` returns at once, so the app can insert a
/// row and commit. The pull then finishes, and the row is still there and still waiting to be
/// pushed. (A pull that read the local changes before it began its transaction would replay
/// only the changes from before the app's commit, and the app's row would be gone.)
#[test]
fn logical_pull_waits_for_the_app_and_keeps_the_apps_commit() {
    let remote =
        Remote::new(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"]).unwrap();
    let server = InProcessServer::new(remote.clone());
    let io = Arc::new(MemoryYieldIO::new());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("local.db").to_str().unwrap().to_string();
    let sync = open(&path, io.clone(), Some(true));

    run(&sync, &server, &io, &sync.create());
    let conn = connect(&sync, &server, &io);
    execute(&conn, &io, "BEGIN IMMEDIATE");

    remote
        .execute_transaction(&["INSERT INTO t VALUES ('second')"])
        .unwrap();
    let wait = sync.wait_changes();
    run(&sync, &server, &io, &wait);
    let TursoAsyncOperationResult::Changes { changes } = wait.take_result().unwrap() else {
        panic!("wait_changes returned no changes");
    };
    let apply = sync.apply_changes(changes);
    let started = std::time::Instant::now();
    while started.elapsed() < std::time::Duration::from_millis(200) {
        let resume_started = std::time::Instant::now();
        // Blocking here for the pull's busy timeout (5 s) would also block the app's COMMIT.
        assert_eq!(
            run_once(&sync, &server, &io, &apply),
            TursoStatusCode::Io,
            "the pull finished while the app held its write transaction"
        );
        assert!(
            resume_started.elapsed() < std::time::Duration::from_secs(1),
            "resume() blocked for {:?}",
            resume_started.elapsed()
        );
    }

    execute(&conn, &io, "INSERT INTO t VALUES ('app')");
    execute(&conn, &io, "COMMIT");
    run(&sync, &server, &io, &apply);
    assert_eq!(
        read(&conn, &io),
        Ok(vec![
            "app".to_string(),
            "first".to_string(),
            "second".to_string()
        ])
    );

    assert_eq!(
        pending_cdc_operations(&sync, &server, &io),
        1,
        "the app's row must be waiting to be pushed"
    );
}

fn open(
    path: &str,
    io: Arc<MemoryYieldIO>,
    logical_mvcc_pull: Option<bool>,
) -> Arc<TursoDatabaseSync<Vec<u8>>> {
    TursoDatabaseSync::new(
        TursoDatabaseConfig {
            path: path.to_string(),
            experimental_features: None,
            async_io: true,
            encryption: None,
            vfs: Default::default(),
            io: Some(io),
            db_file: None,
            page_codec: None,
            open_flags: OpenFlags::default(),
        },
        TursoDatabaseSyncConfig {
            remote_url: Some("http://in-process".to_string()),
            path: path.to_string(),
            client_name: "test".to_string(),
            long_poll_timeout_ms: None,
            bootstrap_if_empty: true,
            reserved_bytes: None,
            partial_sync_opts: None,
            remote_encryption_key: None,
            push_operations_threshold: None,
            pull_bytes_threshold: None,
            pull_parallel_requests: None,
            logical_mvcc_pull,
            busy_timeout_ms: None,
        },
    )
    .unwrap()
}

fn connect(
    sync: &TursoDatabaseSync<Vec<u8>>,
    server: &InProcessServer,
    io: &MemoryYieldIO,
) -> Arc<TursoConnection> {
    let operation = sync.connect();
    run(sync, server, io, &operation);
    match operation.take_result().unwrap() {
        TursoAsyncOperationResult::Connection { connection } => connection,
        _ => panic!("connect returned no connection"),
    }
}

/// Runs `operation` to the end. Its HTTP requests and metadata file reads and writes go to
/// `server`; its database reads and writes finish on `io.step()`.
fn run(
    sync: &TursoDatabaseSync<Vec<u8>>,
    server: &InProcessServer,
    io: &MemoryYieldIO,
    operation: &TursoDatabaseAsyncOperation,
) {
    while run_once(sync, server, io, operation) == TursoStatusCode::Io {}
}

/// Resumes `operation` once, then answers the IO it asked for.
fn run_once(
    sync: &TursoDatabaseSync<Vec<u8>>,
    server: &InProcessServer,
    io: &MemoryYieldIO,
    operation: &TursoDatabaseAsyncOperation,
) -> TursoStatusCode {
    try_run_once(sync, server, io, operation).unwrap()
}

/// Like `run_once`, but returns the operation's error instead of panicking.
fn try_run_once(
    sync: &TursoDatabaseSync<Vec<u8>>,
    server: &InProcessServer,
    io: &MemoryYieldIO,
    operation: &TursoDatabaseAsyncOperation,
) -> Result<TursoStatusCode, String> {
    let status = operation.resume().map_err(|error| error.to_string())?;
    if status == TursoStatusCode::Io {
        while let Some(item) = sync.take_io_item() {
            let (request, completion) = item.into_parts();
            let response = match request {
                SyncEngineIoRequest::Http {
                    url,
                    method,
                    path,
                    body,
                    headers,
                } => {
                    let headers: Vec<(&str, &str)> = headers
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_str()))
                        .collect();
                    server.http(url.as_deref(), &method, &path, body, &headers)
                }
                SyncEngineIoRequest::FullRead { path } => server.full_read(&path),
                SyncEngineIoRequest::FullWrite { path, content } => {
                    server.full_write(&path, content)
                }
            }
            .unwrap();
            completion.status(200);
            while let Some(chunk) = response.poll_data().unwrap() {
                completion.push_buffer(chunk.data().to_vec());
            }
            completion.done();
        }
        sync.step_io_callbacks();
        io.step().unwrap();
    }
    Ok(status)
}

fn read(conn: &TursoConnection, io: &MemoryYieldIO) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare_single("SELECT x FROM t ORDER BY x")
        .map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    loop {
        match stmt.step(None).map_err(|error| error.to_string())? {
            TursoStatusCode::Row => match stmt.row_value(0).unwrap() {
                Value::Text(text) => rows.push(text.as_str().to_string()),
                other => panic!("unexpected value {other:?}"),
            },
            TursoStatusCode::Io => io.step().unwrap(),
            TursoStatusCode::Done => return Ok(rows),
        }
    }
}

/// The pull writes the last synced change id in the same commit as the remote pages. If it wrote
/// the id after the page commit, an app commit between the two would count as synced and never
/// be pushed. MemoryYieldIO stops the pull at each disk write, so the test looks at the database
/// at every stop: once the remote row is readable, the change id must already be its final value.
#[test]
fn pull_commits_the_change_id_together_with_the_pages() {
    let remote =
        Remote::new_wal(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"]).unwrap();
    let server = InProcessServer::pages_only(remote.clone());
    let io = Arc::new(MemoryYieldIO::new());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("local.db").to_str().unwrap().to_string();
    let sync = open(&path, io.clone(), None);

    run(&sync, &server, &io, &sync.create());
    let conn = connect(&sync, &server, &io);
    // A local change makes the final change id non-zero.
    execute(&conn, &io, "INSERT INTO t VALUES ('app')");

    remote
        .execute_transaction(&["INSERT INTO t VALUES ('second')"])
        .unwrap();
    let wait = sync.wait_changes();
    run(&sync, &server, &io, &wait);
    let TursoAsyncOperationResult::Changes { changes } = wait.take_result().unwrap() else {
        panic!("wait_changes returned no changes");
    };
    let apply = sync.apply_changes(changes);

    let mut change_ids_with_new_pages = Vec::new();
    while run_once(&sync, &server, &io, &apply) == TursoStatusCode::Io {
        if read(&conn, &io).unwrap().contains(&"second".to_string()) {
            change_ids_with_new_pages.push(last_synced_change_id(&conn, &io));
        }
    }
    let final_change_id = last_synced_change_id(&conn, &io);
    assert_ne!(final_change_id, 0);
    assert!(
        !change_ids_with_new_pages.is_empty(),
        "the test never saw the remote pages"
    );
    assert!(
        change_ids_with_new_pages
            .iter()
            .all(|id| *id == final_change_id),
        "the remote pages were readable before the final change id: \
         {change_ids_with_new_pages:?}, final {final_change_id}"
    );
}

/// Number of local changes that are waiting to be pushed.
fn pending_cdc_operations(
    sync: &TursoDatabaseSync<Vec<u8>>,
    server: &InProcessServer,
    io: &MemoryYieldIO,
) -> i64 {
    let stats = sync.stats();
    run(sync, server, io, &stats);
    let TursoAsyncOperationResult::Stats { stats } = stats.take_result().unwrap() else {
        panic!("stats returned no stats");
    };
    stats.cdc_operations
}

fn last_synced_change_id(conn: &TursoConnection, io: &MemoryYieldIO) -> i64 {
    query_integer(conn, io, "SELECT change_id FROM turso_sync_last_change_id")
}

fn query_integer(conn: &TursoConnection, io: &MemoryYieldIO, sql: &str) -> i64 {
    let mut stmt = conn.prepare_single(sql).unwrap();
    let mut result = None;
    loop {
        match stmt.step(None).unwrap() {
            TursoStatusCode::Row => match stmt.row_value(0).unwrap() {
                Value::Numeric(turso_core::Numeric::Integer(value)) => result = Some(value),
                other => panic!("unexpected value {other:?}"),
            },
            TursoStatusCode::Io => io.step().unwrap(),
            TursoStatusCode::Done => {
                return result.unwrap_or_else(|| panic!("{sql} returned no row"))
            }
        }
    }
}
