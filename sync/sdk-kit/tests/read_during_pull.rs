//! Pulls and checkpoints through the SDK, with the app on another connection. MemoryYieldIO holds
//! every read and write until `IO::step`, so a test stops an operation partway through and runs
//! the app's statements at that point, every run.

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
