use std::process::Command;
use std::time::Duration;
use turso_core::security::roles::SessionRole;
use turso_core::storage::database::DatabaseFile;
use turso_core::sync::Arc;
use turso_core::*;
use turso_sqlite::SqliteDialect;

#[cfg(all(target_os = "windows", feature = "win_iocp"))]
use turso_core::WindowsIOCP;

const MULTIPROCESS_SEQUENCE_NEXTVAL_CHILD_TEST: &str =
    "multiprocess::multiprocess_sequence_nextval_child_process";

const MULTIPROCESS_SEQUENCE_NEXTVAL_BURST_CHILD_TEST: &str =
    "multiprocess::multiprocess_sequence_nextval_burst_child_process";

const MULTIPROCESS_AUTOINC_BURST_CHILD_TEST: &str =
    "multiprocess::multiprocess_autoinc_burst_child_process";

const MULTIPROCESS_SHM_EXPECT_OPEN_FAILURE_CHILD_TEST: &str =
    "multiprocess::multiprocess_shm_expect_open_failure_child_process";
const DEFAULT_LOCKED_DB_CHILD_TEST: &str = "multiprocess::default_locked_db_child_process";
const MULTIPROCESS_ASYNC_OPEN_CHILD_TEST: &str =
    "multiprocess::multiprocess_async_open_child_process";
const MULTIPROCESS_SHM_INSERT_CHILD_TEST: &str =
    "multiprocess::multiprocess_shm_insert_child_process";
const MULTIPROCESS_SHM_INSERT_AND_CLOSE_CHILD_TEST: &str =
    "multiprocess::multiprocess_shm_insert_and_close_child_process";
const MULTIPROCESS_SHM_COUNT_CHILD_TEST: &str =
    "multiprocess::multiprocess_shm_count_child_process";
const MULTIPROCESS_SHM_SCHEMA_CHILD_TEST: &str =
    "multiprocess::multiprocess_shm_schema_child_process";

fn multiprocess_test_io() -> Arc<dyn IO> {
    #[cfg(all(target_os = "windows", feature = "win_iocp"))]
    {
        Arc::new(WindowsIOCP::new().unwrap())
    }

    #[cfg(not(all(target_os = "windows", feature = "win_iocp")))]
    {
        Arc::new(PlatformIO::new().unwrap())
    }
}

fn count_test_rows(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("select count(*) from test").unwrap();
    let mut count = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    count
}

fn get_rows(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    for _attempt in 0..3 {
        let mut stmt = match conn.prepare(query) {
            Ok(s) => s,
            Err(LimboError::SchemaUpdated) => {
                conn.maybe_reparse_schema().unwrap();
                continue;
            }
            Err(e) => panic!("prepare failed: {e}"),
        };
        let mut rows = Vec::new();
        match stmt.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect::<Vec<_>>());
            Ok(())
        }) {
            Ok(()) => return rows,
            Err(LimboError::SchemaUpdated) => {
                drop(stmt);
                conn.maybe_reparse_schema().unwrap();
                continue;
            }
            Err(e) => panic!("run_with_row_callback failed: {e}"),
        }
    }
    panic!("get_rows: exhausted SchemaUpdated retries for: {query}");
}

fn multiprocess_wal_db_opts() -> DatabaseOpts {
    DatabaseOpts::new().with_multiprocess_wal(true)
}

fn open_multiprocess_db(io: Arc<dyn IO>, path: &str) -> Result<Arc<Database>> {
    Database::open_file_with_flags(
        io,
        path,
        OpenFlags::default(),
        multiprocess_wal_db_opts(),
        None,
        Arc::new(SqliteDialect),
    )
}

/// Mimic the sdk-kit async open path: the caller pre-opens the DB file with
/// NoLock but hands `OpenFlags::default()` to `open_async`, so the async
/// entrypoint itself must re-derive the multiprocess lock mode before the WAL
/// file is opened.
fn open_multiprocess_db_async(io: Arc<dyn IO>, path: &str) -> Result<Arc<Database>> {
    let file = io.open_file(path, OpenFlags::default() | OpenFlags::NoLock, true)?;
    let db_file = Arc::new(DatabaseFile::new(file));
    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .storage(db_file)
        .db_opts(multiprocess_wal_db_opts());
    let mut state = OpenDbAsyncState::new();
    loop {
        match Database::open_async(&mut state, io.clone(), path, &options)? {
            IOResult::Done(db) => return Ok(db),
            IOResult::IO(io_completion) => io_completion.wait(&*io)?,
        }
    }
}

#[test]
fn database_open_without_experimental_multiprocess_wal_rejects_second_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-default-locked.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let _db = Database::open_file(io, db_path_str, Arc::new(SqliteDialect)).unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(DEFAULT_LOCKED_DB_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "second default-open child process unexpectedly succeeded: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );
}

#[test]
fn database_open_with_experimental_multiprocess_wal_rejects_unsupported_io_backend() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let err = Database::open_file_with_flags(
        io,
        "unsupported-multiprocess.db",
        OpenFlags::default(),
        multiprocess_wal_db_opts(),
        None,
        Arc::new(SqliteDialect),
    )
    .expect_err("multiprocess WAL should reject IO backends without shared coordination");
    assert!(
        matches!(err, LimboError::InvalidArgument(ref message) if message.contains("active IO backend")),
        "expected InvalidArgument about unsupported IO backend, got {err:?}"
    );
}

#[test]
fn multiprocess_wal_rejects_journal_mode_mvcc_pragma() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("multiprocess-rejects-mvcc-pragma.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let err = conn
        .execute("PRAGMA journal_mode = mvcc")
        .expect_err("switching a multiprocess-coordinated database to MVCC must be rejected");
    assert!(
        matches!(err, LimboError::InvalidArgument(ref message) if message.contains("multiprocess")),
        "expected InvalidArgument about multiprocess WAL, got {err:?}"
    );
}

#[test]
fn multiprocess_wal_rejects_opening_mvcc_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("multiprocess-rejects-mvcc-open.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    {
        let db = Database::open_file(io.clone(), db_path_str, Arc::new(SqliteDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        conn.execute("PRAGMA journal_mode = mvcc").unwrap();
        conn.execute("insert into test(value) values ('mvcc-row')")
            .unwrap();
    }

    let err = open_multiprocess_db(io, db_path_str)
        .expect_err("opening an MVCC-marked database with multiprocess WAL must be rejected");
    assert!(
        matches!(err, LimboError::InvalidArgument(ref message) if message.contains("MVCC")),
        "expected InvalidArgument about MVCC, got {err:?}"
    );
}

#[test]
fn database_open_with_experimental_multiprocess_wal_rejects_second_default_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-multiprocess-parent-default-child.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let _db = open_multiprocess_db(io, db_path_str).unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(DEFAULT_LOCKED_DB_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "default-open child process unexpectedly succeeded against multiprocess parent: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );
}

#[test]
fn database_open_without_experimental_multiprocess_wal_rejects_second_multiprocess_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-default-parent-multiprocess-child.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let _db = Database::open_file(io, db_path_str, Arc::new(SqliteDialect)).unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_EXPECT_OPEN_FAILURE_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "multiprocess child process unexpectedly opened against default parent: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );
}

#[test]
fn database_open_async_with_default_flags_allows_second_multiprocess_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-async-open.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db_async(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('parent')")
        .unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_ASYNC_OPEN_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "second async multiprocess open must not be blocked by a WAL file lock: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );
}

#[test]
fn multiprocess_async_open_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db_async(io, db_path.to_str().unwrap()).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(count_test_rows(&conn), 1);
}

#[test]
fn default_locked_db_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let err = Database::open_file(io, db_path.to_str().unwrap(), Arc::new(SqliteDialect))
        .expect_err("default non-multiprocess open should stay DB-file locked across processes");
    assert!(
        matches!(err, LimboError::LockingError(_)),
        "expected LockingError from second default open, got {err:?}"
    );
}

#[test]
fn multiprocess_shm_expect_open_failure_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let err = open_multiprocess_db(io, db_path.to_str().unwrap())
        .expect_err("multiprocess open should fail when a legacy opener already owns the DB");
    assert!(
        matches!(err, LimboError::LockingError(_)),
        "expected LockingError from incompatible multiprocess open, got {err:?}"
    );
}

#[test]
fn plain_vacuum_rejects_multiprocess_wal_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("vacuum-multiprocess.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = Database::open_file_with_flags(
        io,
        db_path_str,
        OpenFlags::default(),
        multiprocess_wal_db_opts().with_vacuum(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('parent')")
        .unwrap();

    let err = conn
        .execute("VACUUM")
        .expect_err("VACUUM should reject on a multiprocess-WAL database");
    assert!(
        matches!(err, LimboError::ParseError(ref msg) if msg.contains("experimental multiprocess WAL")),
        "expected explicit multiprocess VACUUM rejection, got {err:?}"
    );
    assert_eq!(
        count_test_rows(&conn),
        1,
        "rejecting VACUUM on a multiprocess-WAL database must not disturb the existing connection"
    );
}

#[test]
fn memory_database_query_can_close_without_checkpointing() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.query("VALUES ('ok')").unwrap();
    conn.close().unwrap();
}

// ============================================================================
// Multi-process sequence tests.
//
// Sequences are disk-only: each `nextval` reads the latest committed
// watermark from the backing table under the WAL write lock and writes
// the next value back. Cross-process synchronization is therefore the
// pager's WAL lock plus MVCC's commit-validation path; there is no
// in-memory atomic to keep in sync.
//
// These tests pin the cross-process contract:
//
//   1. `test_multiprocess_concurrent_nextval_returns_distinct_values`
//      Two processes open the DB before either commits and each calls
//      nextval. The WAL-locked RMW guarantees the second writer reads
//      the first writer's committed row, so the two emissions must be
//      distinct.
//
//   2. `test_multiprocess_nextval_after_other_process_committed_is_monotonic`
//      Process A opens, calls nextval, commits, stays open. Process B
//      opens (sees A's commit), then A does another nextval. B's next
//      nextval must observe both of A's commits and emit a strictly
//      greater value.
//
//   3. `test_multiprocess_setval_visible_to_other_process_nextval`
//      Process A calls `setval('s', 100, true)` and commits. Process B
//      (already open before A's setval) calls nextval — must return
//      101, picking up A's committed setval from disk.

/// Coordinates child processes via a wall-clock barrier. Children open the
/// DB *before* the barrier (so all of them have their atomics initialized
/// from the same disk state) and only call nextval *after* the barrier, so
/// the in-memory writes overlap.
#[test]
fn multiprocess_sequence_nextval_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let result_path = std::env::var("TURSO_MULTIPROCESS_RESULT_PATH")
        .expect("TURSO_MULTIPROCESS_RESULT_PATH must be set for sequence child");
    let ready_path = std::env::var("TURSO_MULTIPROCESS_READY_PATH")
        .expect("TURSO_MULTIPROCESS_READY_PATH must be set for sequence child");
    let go_path = std::env::var("TURSO_MULTIPROCESS_GO_PATH")
        .expect("TURSO_MULTIPROCESS_GO_PATH must be set for sequence child");

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // Signal that the connection is open (atomic loaded from disk). This is
    // what we need to overlap across all participating children.
    std::fs::write(&ready_path, b"").expect("write ready marker");

    // Wait for the parent's go signal. Bounded wait so a buggy parent
    // doesn't hang CI.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !std::path::Path::new(&go_path).exists() {
        if std::time::Instant::now() >= deadline {
            panic!("child timed out waiting for go signal at {go_path}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // Retry on Busy: in WAL multiprocess mode the write lock is held by
    // whichever process is committing right now; that's expected, not a
    // sequence bug. The bug we're hunting is in-memory atomic divergence,
    // which only matters once each process actually gets to commit. Cap
    // the retry loop so a real hang doesn't disguise itself as flakiness.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let value = loop {
        let mut stmt = conn.prepare("SELECT nextval('s')").unwrap();
        let mut got: Option<i64> = None;
        let step_result = stmt.run_with_row_callback(|row| {
            got = Some(row.get::<i64>(0).unwrap());
            Ok(())
        });
        match step_result {
            Ok(()) => break got.expect("nextval must produce a row"),
            Err(LimboError::Busy) => {
                if std::time::Instant::now() >= deadline {
                    panic!("child kept hitting Busy on nextval for 15s");
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("unexpected nextval error: {e:?}"),
        }
    };

    std::fs::write(&result_path, value.to_string()).expect("write result");
    conn.close().unwrap();
}

/// Spawn `n` child processes that each call `SELECT nextval('s')` after a
/// shared wall-clock barrier (so all children's atomics start from the same
/// pre-nextval disk state and their nextval calls overlap in time). Return
/// the values they emitted. Helper for the parent tests below.
fn spawn_concurrent_nextval_children(db_path: &str, n: usize) -> Vec<i64> {
    let coord_dir = tempfile::tempdir().expect("coord tempdir");
    let go_path = coord_dir.path().join("go");
    let go_path_str = go_path.to_str().unwrap().to_string();

    let mut children = Vec::with_capacity(n);
    let mut result_paths = Vec::with_capacity(n);
    let mut ready_paths = Vec::with_capacity(n);
    let current_exe = std::env::current_exe().unwrap();

    for i in 0..n {
        let result_path = coord_dir.path().join(format!("result-{i}"));
        let ready_path = coord_dir.path().join(format!("ready-{i}"));
        let child = Command::new(&current_exe)
            .arg(MULTIPROCESS_SEQUENCE_NEXTVAL_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", db_path)
            .env(
                "TURSO_MULTIPROCESS_RESULT_PATH",
                result_path.to_str().unwrap(),
            )
            .env(
                "TURSO_MULTIPROCESS_READY_PATH",
                ready_path.to_str().unwrap(),
            )
            .env("TURSO_MULTIPROCESS_GO_PATH", &go_path_str)
            .spawn()
            .expect("spawn child");
        children.push(child);
        result_paths.push(result_path);
        ready_paths.push(ready_path);
        // Brief stagger: the multiprocess-WAL open path runs a probe-open
        // (without the multiproc flag) to detect a stale legacy holder.
        // When several children race the open they trip each other's
        // probes — that's a test-infra artifact, not a sequence bug. The
        // bug the suite hunts (atomic divergence under concurrent calls)
        // is preserved because the children still hit the barrier at the
        // same wall-clock moment and call nextval concurrently.
        std::thread::sleep(Duration::from_millis(50));
    }

    // Wait for every child to signal ready (atomic loaded, waiting at
    // barrier). Bounded so a failing child doesn't hang the parent.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if ready_paths.iter().all(|p| p.exists()) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            for child in &mut children {
                let _ = child.kill();
            }
            panic!("not all children became ready within 30s");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Release the barrier — all children now race to nextval.
    std::fs::write(&go_path, b"").expect("write go signal");

    let mut values = Vec::with_capacity(n);
    for (i, mut child) in children.into_iter().enumerate() {
        let status = child.wait().expect("wait child");
        let result_path = &result_paths[i];
        assert!(
            status.success(),
            "child {i} failed (status={status:?}); no result at {result_path:?}",
        );
        let raw = std::fs::read_to_string(result_path)
            .unwrap_or_else(|e| panic!("read child {i} result at {result_path:?}: {e}"));
        values.push(raw.trim().parse::<i64>().expect("parse i64 result"));
    }
    values
}

#[test]
fn test_multiprocess_concurrent_nextval_returns_distinct_values() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mp-seq-nextval.db");
    let db_path_str = db_path.to_str().unwrap();

    {
        let io: Arc<dyn IO> = multiprocess_test_io();
        let db = open_multiprocess_db(io, db_path_str).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1")
            .unwrap();
        conn.close().unwrap();
    }

    let values = spawn_concurrent_nextval_children(db_path_str, 3);

    let mut sorted = values.clone();
    sorted.sort_unstable();
    let unique: std::collections::HashSet<_> = sorted.iter().copied().collect();
    assert_eq!(
        unique.len(),
        sorted.len(),
        "expected 3 distinct nextval results across processes, got {values:?}",
    );
}

#[test]
fn test_multiprocess_nextval_after_other_process_committed_is_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mp-seq-monotonic.db");
    let db_path_str = db_path.to_str().unwrap();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn_a = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn_a
        .execute("CREATE SEQUENCE s START 1 INCREMENT 1")
        .unwrap();

    // Process A first nextval: returns 1 (start value).
    let mut stmt = conn_a.prepare("SELECT nextval('s')").unwrap();
    let mut a_first: Option<i64> = None;
    stmt.run_with_row_callback(|row| {
        a_first = Some(row.get::<i64>(0).unwrap());
        Ok(())
    })
    .unwrap();
    drop(stmt);
    assert_eq!(a_first, Some(1));

    // Now spawn a child (B). B opens AFTER A's commit, so its atomic is
    // loaded from the up-to-date disk state. B should see at least 2.
    // But the child runs in parallel with another nextval on A below.
    let values = spawn_concurrent_nextval_children(db_path_str, 1);
    let b_value = values[0];

    // A's second nextval: should NOT collide with B.
    let mut stmt = conn_a.prepare("SELECT nextval('s')").unwrap();
    let mut a_second: Option<i64> = None;
    stmt.run_with_row_callback(|row| {
        a_second = Some(row.get::<i64>(0).unwrap());
        Ok(())
    })
    .unwrap();
    let a_second = a_second.expect("A's second nextval must produce a row");

    let all = [1, b_value, a_second];
    let unique: std::collections::HashSet<_> = all.iter().copied().collect();
    assert_eq!(
        unique.len(),
        3,
        "expected 3 distinct nextval results (A1={}, B={}, A2={}), got {all:?}",
        1,
        b_value,
        a_second,
    );

    conn_a.close().unwrap();
}

#[test]
fn test_multiprocess_setval_visible_to_other_process_nextval() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mp-seq-setval.db");
    let db_path_str = db_path.to_str().unwrap();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn_a = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn_a
        .execute("CREATE SEQUENCE s START 1 INCREMENT 1")
        .unwrap();

    // Spawn the child *before* setval so its atomic is loaded from the
    // pre-setval disk state. The child waits at the barrier, then does
    // nextval after we move the watermark.
    let coord_dir = tempfile::tempdir().expect("coord tempdir");
    let go_path = coord_dir.path().join("go");
    let ready_path = coord_dir.path().join("ready");
    let result_path = coord_dir.path().join("result");
    let current_exe = std::env::current_exe().unwrap();
    let mut child = Command::new(&current_exe)
        .arg(MULTIPROCESS_SEQUENCE_NEXTVAL_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env(
            "TURSO_MULTIPROCESS_RESULT_PATH",
            result_path.to_str().unwrap(),
        )
        .env(
            "TURSO_MULTIPROCESS_READY_PATH",
            ready_path.to_str().unwrap(),
        )
        .env("TURSO_MULTIPROCESS_GO_PATH", go_path.to_str().unwrap())
        .spawn()
        .expect("spawn child");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !ready_path.exists() {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            panic!("child not ready within 30s");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Now move the watermark while the child is parked at the barrier.
    let mut stmt = conn_a.prepare("SELECT setval('s', 100, 1)").unwrap();
    stmt.run_with_row_callback(|_| Ok(())).unwrap();
    drop(stmt);

    // Release the barrier — child does nextval against a stale atomic.
    std::fs::write(&go_path, b"").expect("write go signal");

    let status = child.wait().expect("wait child");
    assert!(status.success(), "child failed: {status:?}");
    let value: i64 = std::fs::read_to_string(&result_path)
        .expect("read child result")
        .trim()
        .parse()
        .expect("parse i64");

    assert_eq!(
        value, 101,
        "child opened before setval(100, true) must still produce 101 after barrier release, got {value}",
    );

    conn_a.close().unwrap();
}

/// Burst child: opens the DB, waits at the barrier, then loops
/// `TURSO_MULTIPROCESS_NEXTVAL_COUNT` times calling `SELECT nextval('s')`.
/// Retries on Busy (WAL multiproc write lock contention is expected — the
/// bug we're hunting is duplicate-value emission, not Busy). Writes all
/// emitted values to the result file, one per line.
#[test]
fn multiprocess_sequence_nextval_burst_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let result_path = std::env::var("TURSO_MULTIPROCESS_RESULT_PATH")
        .expect("TURSO_MULTIPROCESS_RESULT_PATH must be set for burst child");
    let ready_path = std::env::var("TURSO_MULTIPROCESS_READY_PATH")
        .expect("TURSO_MULTIPROCESS_READY_PATH must be set for burst child");
    let go_path = std::env::var("TURSO_MULTIPROCESS_GO_PATH")
        .expect("TURSO_MULTIPROCESS_GO_PATH must be set for burst child");
    let count: usize = std::env::var("TURSO_MULTIPROCESS_NEXTVAL_COUNT")
        .expect("TURSO_MULTIPROCESS_NEXTVAL_COUNT must be set for burst child")
        .parse()
        .expect("parse count");

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    std::fs::write(&ready_path, b"").expect("write ready marker");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !std::path::Path::new(&go_path).exists() {
        if std::time::Instant::now() >= deadline {
            panic!("child timed out waiting for go signal at {go_path}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    let mut values: Vec<i64> = Vec::with_capacity(count);
    for _ in 0..count {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let value = loop {
            let mut stmt = match conn.prepare("SELECT nextval('s')") {
                Ok(s) => s,
                Err(LimboError::Busy) => {
                    if std::time::Instant::now() >= deadline {
                        panic!("child timed out preparing nextval after 30s");
                    }
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(e) => panic!("unexpected prepare error: {e:?}"),
            };
            let mut got: Option<i64> = None;
            let step_result = stmt.run_with_row_callback(|row| {
                got = Some(row.get::<i64>(0).unwrap());
                Ok(())
            });
            match step_result {
                Ok(()) => break got.expect("nextval must produce a row"),
                Err(LimboError::Busy) | Err(LimboError::WriteWriteConflict) => {
                    if std::time::Instant::now() >= deadline {
                        panic!("child timed out on nextval after 30s of contention");
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => panic!("unexpected nextval error: {e:?}"),
            }
        };
        values.push(value);
    }

    let body = values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&result_path, body).expect("write result");
    conn.close().unwrap();
}

/// Volume stress: many processes each pull many values from the same sequence
/// in parallel. Every returned value across the entire pool must be distinct.
///
/// Failure mode under the old atomic design: each process initialized its
/// atomic from the same disk state at open. Once the barrier releases,
/// the first burst of nextvals from each process all emit values they
/// computed against that same starting watermark — yielding the same
/// integer from multiple processes.
///
/// After disk-only RMW: each nextval reads the latest committed watermark
/// from the backing table under WAL write-lock serialization; concurrent
/// processes naturally observe each other's commits and never collide.
#[test]
fn test_multiprocess_nextval_burst_no_duplicates() {
    const PROCESSES: usize = 4;
    const PER_PROCESS: usize = 25;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mp-seq-burst.db");
    let db_path_str = db_path.to_str().unwrap();

    {
        let io: Arc<dyn IO> = multiprocess_test_io();
        let db = open_multiprocess_db(io, db_path_str).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1")
            .unwrap();
        conn.close().unwrap();
    }

    let coord_dir = tempfile::tempdir().expect("coord tempdir");
    let go_path = coord_dir.path().join("go");
    let current_exe = std::env::current_exe().unwrap();

    let mut children = Vec::with_capacity(PROCESSES);
    let mut result_paths = Vec::with_capacity(PROCESSES);
    let mut ready_paths = Vec::with_capacity(PROCESSES);

    for i in 0..PROCESSES {
        let result_path = coord_dir.path().join(format!("result-{i}"));
        let ready_path = coord_dir.path().join(format!("ready-{i}"));
        let child = Command::new(&current_exe)
            .arg(MULTIPROCESS_SEQUENCE_NEXTVAL_BURST_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
            .env(
                "TURSO_MULTIPROCESS_RESULT_PATH",
                result_path.to_str().unwrap(),
            )
            .env(
                "TURSO_MULTIPROCESS_READY_PATH",
                ready_path.to_str().unwrap(),
            )
            .env("TURSO_MULTIPROCESS_GO_PATH", go_path.to_str().unwrap())
            .env("TURSO_MULTIPROCESS_NEXTVAL_COUNT", PER_PROCESS.to_string())
            .spawn()
            .expect("spawn burst child");
        children.push(child);
        result_paths.push(result_path);
        ready_paths.push(ready_path);
        // Stagger spawn — see comment in spawn_concurrent_nextval_children.
        std::thread::sleep(Duration::from_millis(50));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if ready_paths.iter().all(|p| p.exists()) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            for child in &mut children {
                let _ = child.kill();
            }
            panic!("not all burst children became ready within 60s");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    std::fs::write(&go_path, b"").expect("write go signal");

    let mut all_values: Vec<i64> = Vec::with_capacity(PROCESSES * PER_PROCESS);
    for (i, mut child) in children.into_iter().enumerate() {
        let status = child.wait().expect("wait burst child");
        let result_path = &result_paths[i];
        assert!(
            status.success(),
            "burst child {i} failed (status={status:?})",
        );
        let raw = std::fs::read_to_string(result_path)
            .unwrap_or_else(|e| panic!("read burst child {i} result: {e}"));
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            all_values.push(trimmed.parse::<i64>().expect("parse i64 result line"));
        }
    }

    assert_eq!(
        all_values.len(),
        PROCESSES * PER_PROCESS,
        "expected {} total values across {} processes, got {}",
        PROCESSES * PER_PROCESS,
        PROCESSES,
        all_values.len()
    );

    let unique: std::collections::HashSet<_> = all_values.iter().copied().collect();
    assert_eq!(
        unique.len(),
        all_values.len(),
        "expected all {} multiprocess nextvals to be distinct; \
         duplicates present (atomic divergence)",
        all_values.len()
    );
}

/// Child process for the AUTOINCREMENT burst test.
///
/// Waits for the coordination "go" signal, then issues
/// `INSERT INTO t(payload) VALUES('...') RETURNING id` repeatedly,
/// recording each assigned rowid. Mirrors the structure of
/// `multiprocess_sequence_nextval_burst_child_process` but targets
/// the AUTOINCREMENT code path (implicit
/// `__turso_internal_autoincrement_t` sequence + `sqlite_sequence`
/// sync), which is structurally distinct from explicit
/// `nextval('s')` and has its own multiprocess race window:
/// the per-INSERT WAL-locked RMW against the backing table AND
/// the sqlite_sequence update must both serialize against
/// concurrent writers in other processes.
#[test]
fn multiprocess_autoinc_burst_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let result_path = std::env::var("TURSO_MULTIPROCESS_RESULT_PATH")
        .expect("TURSO_MULTIPROCESS_RESULT_PATH must be set for autoinc burst child");
    let ready_path = std::env::var("TURSO_MULTIPROCESS_READY_PATH")
        .expect("TURSO_MULTIPROCESS_READY_PATH must be set for autoinc burst child");
    let go_path = std::env::var("TURSO_MULTIPROCESS_GO_PATH")
        .expect("TURSO_MULTIPROCESS_GO_PATH must be set for autoinc burst child");
    let count: usize = std::env::var("TURSO_MULTIPROCESS_AUTOINC_COUNT")
        .expect("TURSO_MULTIPROCESS_AUTOINC_COUNT must be set for autoinc burst child")
        .parse()
        .expect("parse count");

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    std::fs::write(&ready_path, b"").expect("write ready marker");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !std::path::Path::new(&go_path).exists() {
        if std::time::Instant::now() >= deadline {
            panic!("autoinc child timed out waiting for go signal at {go_path}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    let mut ids: Vec<i64> = Vec::with_capacity(count);
    for _ in 0..count {
        let insert_deadline = std::time::Instant::now() + Duration::from_secs(30);
        let assigned = loop {
            let mut stmt = match conn.prepare("INSERT INTO t(payload) VALUES('x') RETURNING id") {
                Ok(s) => s,
                Err(LimboError::Busy) | Err(LimboError::WriteWriteConflict) => {
                    if std::time::Instant::now() >= insert_deadline {
                        panic!("autoinc child timed out preparing INSERT after 30s");
                    }
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(e) => panic!("unexpected prepare error: {e:?}"),
            };
            let mut got: Option<i64> = None;
            let step_result = stmt.run_with_row_callback(|row| {
                got = Some(row.get::<i64>(0).unwrap());
                Ok(())
            });
            match step_result {
                Ok(()) => break got.expect("INSERT RETURNING must yield a rowid"),
                Err(LimboError::Busy) | Err(LimboError::WriteWriteConflict) => {
                    if std::time::Instant::now() >= insert_deadline {
                        panic!("autoinc child timed out on INSERT after 30s of contention");
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => panic!("unexpected INSERT error: {e:?}"),
            }
        };
        ids.push(assigned);
    }

    let body = ids
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&result_path, body).expect("write result");
    conn.close().unwrap();
}

/// Multiprocess AUTOINCREMENT stress: many processes each pull many
/// rowids from a single AUTOINCREMENT table in parallel, then verify
/// global invariants from a fresh observer:
///
///   1. Every assigned rowid is distinct across the entire cohort
///      (no duplicate primary keys — a hard SQLite invariant).
///   2. `sqlite_sequence.seq` for the table equals the maximum
///      assigned rowid, which equals `processes * per_process`
///      (the engine's high-water mark stayed in sync with what was
///      actually written across processes).
///   3. `sqlite_sequence` has exactly one row for the table (no
///      duplicate watermark rows from cross-process flushes — that
///      was a real bug class fixed during this PR).
///   4. The table contains exactly the expected row count with
///      contiguous ids 1..=N (no gaps in autoinc allocation).
///
/// Why this test exists separately from
/// `test_multiprocess_nextval_burst_no_duplicates`: AUTOINCREMENT
/// goes through the implicit
/// `__turso_internal_autoincrement_<table>` sequence path, which
/// has different bytecode (`sqlite_sequence` sync, the per-INSERT
/// advance-past) than explicit `nextval('s')`. Any cross-process
/// regression in that path (sqlite_sequence dup rows, missed
/// watermark advance, stale read of the backing-table row) would
/// be invisible to the nextval burst test.
#[test]
fn test_multiprocess_autoinc_burst_no_duplicates() {
    const PROCESSES: usize = 4;
    const PER_PROCESS: usize = 25;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mp-autoinc-burst.db");
    let db_path_str = db_path.to_str().unwrap();

    {
        let io: Arc<dyn IO> = multiprocess_test_io();
        let db = open_multiprocess_db(io, db_path_str).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)")
            .unwrap();
        conn.close().unwrap();
    }

    let coord_dir = tempfile::tempdir().expect("coord tempdir");
    let go_path = coord_dir.path().join("go");
    let current_exe = std::env::current_exe().unwrap();

    let mut children = Vec::with_capacity(PROCESSES);
    let mut result_paths = Vec::with_capacity(PROCESSES);
    let mut ready_paths = Vec::with_capacity(PROCESSES);

    for i in 0..PROCESSES {
        let result_path = coord_dir.path().join(format!("result-{i}"));
        let ready_path = coord_dir.path().join(format!("ready-{i}"));
        let child = Command::new(&current_exe)
            .arg(MULTIPROCESS_AUTOINC_BURST_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
            .env(
                "TURSO_MULTIPROCESS_RESULT_PATH",
                result_path.to_str().unwrap(),
            )
            .env(
                "TURSO_MULTIPROCESS_READY_PATH",
                ready_path.to_str().unwrap(),
            )
            .env("TURSO_MULTIPROCESS_GO_PATH", go_path.to_str().unwrap())
            .env("TURSO_MULTIPROCESS_AUTOINC_COUNT", PER_PROCESS.to_string())
            .spawn()
            .expect("spawn autoinc burst child");
        children.push(child);
        result_paths.push(result_path);
        ready_paths.push(ready_path);
        // Stagger spawn so children don't all race the same coordination
        // file-system events at startup — same idiom as the nextval burst.
        std::thread::sleep(Duration::from_millis(50));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if ready_paths.iter().all(|p| p.exists()) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            for child in &mut children {
                let _ = child.kill();
            }
            panic!("not all autoinc burst children became ready within 60s");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    std::fs::write(&go_path, b"").expect("write go signal");

    let mut all_ids: Vec<i64> = Vec::with_capacity(PROCESSES * PER_PROCESS);
    for (i, mut child) in children.into_iter().enumerate() {
        let status = child.wait().expect("wait autoinc burst child");
        let result_path = &result_paths[i];
        assert!(
            status.success(),
            "autoinc burst child {i} failed (status={status:?})",
        );
        let raw = std::fs::read_to_string(result_path)
            .unwrap_or_else(|e| panic!("read autoinc burst child {i} result: {e}"));
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            all_ids.push(trimmed.parse::<i64>().expect("parse i64 result line"));
        }
    }

    assert_eq!(
        all_ids.len(),
        PROCESSES * PER_PROCESS,
        "expected {} total inserts across {} processes, got {}",
        PROCESSES * PER_PROCESS,
        PROCESSES,
        all_ids.len()
    );

    // Invariant 1: every assigned rowid is unique.
    let unique: std::collections::HashSet<_> = all_ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        all_ids.len(),
        "expected all {} multiprocess AUTOINCREMENT rowids to be distinct; \
         duplicates present (cross-process watermark divergence)",
        all_ids.len()
    );

    // Invariants 2-4: open a fresh observer and read the persisted state.
    let observer_io: Arc<dyn IO> = multiprocess_test_io();
    let observer_db =
        open_multiprocess_db(observer_io, db_path_str).expect("reopen db for fresh observer");
    let observer_conn = observer_db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .expect("connect fresh observer");

    let count_rows = get_rows(&observer_conn, "SELECT COUNT(*) FROM t");
    let total_rows = count_rows[0][0]
        .as_int()
        .expect("COUNT(*) should be integer");
    assert_eq!(
        total_rows,
        (PROCESSES * PER_PROCESS) as i64,
        "expected {} rows in autoinc table, got {}",
        PROCESSES * PER_PROCESS,
        total_rows,
    );

    let max_id_rows = get_rows(&observer_conn, "SELECT MAX(id), MIN(id) FROM t");
    let max_id = max_id_rows[0][0]
        .as_int()
        .expect("MAX(id) should be integer");
    let min_id = max_id_rows[0][1]
        .as_int()
        .expect("MIN(id) should be integer");
    assert_eq!(
        max_id,
        (PROCESSES * PER_PROCESS) as i64,
        "AUTOINCREMENT should have assigned rowids contiguously up to {}",
        PROCESSES * PER_PROCESS,
    );
    assert_eq!(min_id, 1, "AUTOINCREMENT should start at 1");

    let seq_rows = get_rows(
        &observer_conn,
        "SELECT seq FROM sqlite_sequence WHERE name='t'",
    );
    assert_eq!(
        seq_rows.len(),
        1,
        "sqlite_sequence must have exactly one row for the AUTOINCREMENT table; \
         duplicate rows indicate cross-process flush divergence",
    );
    let persisted_seq = seq_rows[0][0]
        .as_int()
        .expect("sqlite_sequence.seq should be integer");
    assert_eq!(
        persisted_seq, max_id,
        "sqlite_sequence.seq ({persisted_seq}) must match max assigned rowid \
         ({max_id}); engine watermark drifted from actual table state across processes",
    );

    observer_conn.close().unwrap();
}

#[test]
fn readonly_open_with_experimental_multiprocess_wal_allows_missing_coordination_file() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("readonly-multiprocess-no-tshm.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    {
        let db = Database::open_file(io.clone(), db_path_str, Arc::new(SqliteDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
    }

    let readonly = Database::open_file_with_flags(
        io,
        db_path_str,
        OpenFlags::ReadOnly,
        multiprocess_wal_db_opts(),
        None,
        Arc::new(SqliteDialect),
    )
    .expect("read-only open should degrade gracefully when no .tshm exists");
    assert!(coordination_open_mode(&readonly).is_none());
}

#[test]
fn subprocess_database_open_selects_multiprocess_shm_backend() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-subprocess.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    assert_eq!(
        coordination_open_mode(&db),
        Some(SharedWalCoordinationOpenTelemetryMode::Exclusive)
    );

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('parent')")
        .unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let insert_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_INSERT_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        insert_output.status.success(),
        "child process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&insert_output.stdout),
        String::from_utf8_lossy(&insert_output.stderr)
    );

    let count_output = Command::new(current_exe)
        .arg(MULTIPROCESS_SHM_COUNT_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env("TURSO_MULTIPROCESS_EXPECTED_COUNT", "2")
        .output()
        .unwrap();
    assert!(
        count_output.status.success(),
        "count child process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&count_output.stdout),
        String::from_utf8_lossy(&count_output.stderr)
    );
}

#[test]
fn subprocess_child_close_skips_shutdown_checkpoint_in_multiprocess_wal_mode() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("close-skip-shutdown-checkpoint.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    run_checkpoint(
        &conn,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_INSERT_AND_CLOSE_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "child process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );

    let wal_path = format!("{db_path_str}-wal");
    let wal_len = std::fs::metadata(&wal_path)
        .unwrap_or_else(|_| panic!("expected WAL file at {wal_path}"))
        .len();
    assert!(
        wal_len > 0,
        "child close should not run shutdown checkpoint while another process still has the DB open"
    );

    assert_eq!(
        count_test_rows(&conn),
        1,
        "parent connection should still see the child commit via WAL"
    );
}

#[test]
fn subprocess_database_open_survives_truncate_rewrite_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-truncate-rewrite.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('parent-0')")
        .unwrap();

    let current_exe = std::env::current_exe().unwrap();
    for expected_count in [2_i64, 3_i64] {
        let insert_output = Command::new(&current_exe)
            .arg(MULTIPROCESS_SHM_INSERT_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
            .output()
            .unwrap();
        assert!(
            insert_output.status.success(),
            "child insert failed: stdout={}; stderr={}",
            String::from_utf8_lossy(&insert_output.stdout),
            String::from_utf8_lossy(&insert_output.stderr)
        );
        assert_eq!(count_test_rows(&conn), expected_count);

        let checkpoint = run_checkpoint(
            &conn,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            checkpoint.everything_backfilled(),
            "truncate checkpoint should fully backfill before WAL rewrite"
        );

        let count_output = Command::new(&current_exe)
            .arg(MULTIPROCESS_SHM_COUNT_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
            .env(
                "TURSO_MULTIPROCESS_EXPECTED_COUNT",
                expected_count.to_string(),
            )
            .output()
            .unwrap();
        assert!(
            count_output.status.success(),
            "child count after truncate failed: stdout={}; stderr={}",
            String::from_utf8_lossy(&count_output.stdout),
            String::from_utf8_lossy(&count_output.stderr)
        );
    }

    assert_eq!(count_test_rows(&conn), 3);
}

#[test]
fn multiprocess_shm_insert_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    assert_eq!(
        coordination_open_mode(&db),
        Some(SharedWalCoordinationOpenTelemetryMode::MultiProcess)
    );

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("insert into test(value) values ('child')")
        .unwrap();
}

#[test]
fn multiprocess_shm_insert_and_close_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    assert_eq!(
        coordination_open_mode(&db),
        Some(SharedWalCoordinationOpenTelemetryMode::MultiProcess)
    );

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("insert into test(value) values ('child-close')")
        .unwrap();
    conn.close().unwrap();
}

#[test]
fn multiprocess_shm_count_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let expected_count = std::env::var("TURSO_MULTIPROCESS_EXPECTED_COUNT")
        .unwrap()
        .parse::<i64>()
        .unwrap();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    assert_eq!(
        coordination_open_mode(&db),
        Some(SharedWalCoordinationOpenTelemetryMode::MultiProcess)
    );

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(count_test_rows(&conn), expected_count);
}

#[test]
fn subprocess_database_open_peer_refreshes_remote_schema_without_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-peer-schema-refresh.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table t(value integer, next_value integer)")
        .unwrap();

    let series_rows = get_rows(&conn, "select value from generate_series(1, 3)");
    assert_eq!(series_rows.len(), 3);
    assert_eq!(series_rows[0][0].to_string(), "1");
    assert_eq!(series_rows[1][0].to_string(), "2");
    assert_eq!(series_rows[2][0].to_string(), "3");

    let initial_schema_rows = get_rows(
        &conn,
        "select name, type from sqlite_schema where name = 'child_table'",
    );
    assert!(
        initial_schema_rows.is_empty(),
        "fresh parent connection should not see child_table before child creates it"
    );

    let current_exe = std::env::current_exe().unwrap();
    let schema_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_SCHEMA_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        schema_output.status.success(),
        "child schema process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&schema_output.stdout),
        String::from_utf8_lossy(&schema_output.stderr)
    );

    let schema_rows = get_rows_without_schema_retry(
        &conn,
        "select name, type from sqlite_schema where name = 'child_table'",
    );
    assert_eq!(schema_rows.len(), 1);
    assert_eq!(schema_rows[0][0].to_string(), "child_table");
    assert_eq!(schema_rows[0][1].to_string(), "table");

    let child_rows = get_rows_without_schema_retry(&conn, "select value from child_table");
    assert_eq!(child_rows.len(), 1);
    assert_eq!(child_rows[0][0].to_string(), "child-schema");

    conn.execute("insert into t select value, value + 1 from generate_series(1, 3)")
        .unwrap();
    let inserted_rows = get_rows(&conn, "select value, next_value from t order by value");
    assert_eq!(inserted_rows.len(), 3);
    assert_eq!(inserted_rows[0][0].to_string(), "1");
    assert_eq!(inserted_rows[0][1].to_string(), "2");
    assert_eq!(inserted_rows[2][0].to_string(), "3");
    assert_eq!(inserted_rows[2][1].to_string(), "4");
}

#[test]
fn subprocess_database_open_parent_directly_uses_child_created_table() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-direct-child-table-use.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table t(value integer)").unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let schema_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_SCHEMA_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        schema_output.status.success(),
        "child schema process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&schema_output.stdout),
        String::from_utf8_lossy(&schema_output.stderr)
    );

    let mut stmt = conn
        .prepare("insert into child_table(value) values ('parent-schema')")
        .unwrap();
    stmt.run_ignore_rows().unwrap();

    let child_rows =
        get_rows_without_schema_retry(&conn, "select value from child_table order by rowid");
    assert_eq!(child_rows.len(), 2);
    assert_eq!(child_rows[0][0].to_string(), "child-schema");
    assert_eq!(child_rows[1][0].to_string(), "parent-schema");
}

#[test]
fn subprocess_database_open_parent_translated_stmt_uses_child_created_table() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-translated-child-table-use.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table t(value integer)").unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let schema_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_SCHEMA_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        schema_output.status.success(),
        "child schema process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&schema_output.stdout),
        String::from_utf8_lossy(&schema_output.stderr)
    );

    let input = "insert into child_table(value) values ('parent-schema')";
    let (cmd, _) = Dialect::parse_internal(&SqliteDialect, input).unwrap();
    let Some(turso_parser::ast::Cmd::Stmt(stmt)) = cmd else {
        panic!("translated statement input did not parse as a statement");
    };
    conn.prepare_translated_stmt(stmt, input)
        .unwrap()
        .run_ignore_rows()
        .unwrap();

    let child_rows =
        get_rows_without_schema_retry(&conn, "select value from child_table order by rowid");
    assert_eq!(child_rows.len(), 2);
    assert_eq!(child_rows[0][0].to_string(), "child-schema");
    assert_eq!(child_rows[1][0].to_string(), "parent-schema");
}

#[test]
fn subprocess_database_open_parent_execute_uses_child_created_table() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-execute-child-table-use.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table t(value integer)").unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let schema_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_SCHEMA_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        schema_output.status.success(),
        "child schema process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&schema_output.stdout),
        String::from_utf8_lossy(&schema_output.stderr)
    );

    conn.execute("insert into child_table(value) values ('parent-schema')")
        .unwrap();

    let child_rows =
        get_rows_without_schema_retry(&conn, "select value from child_table order by rowid");
    assert_eq!(child_rows.len(), 2);
    assert_eq!(child_rows[0][0].to_string(), "child-schema");
    assert_eq!(child_rows[1][0].to_string(), "parent-schema");
}

#[test]
fn subprocess_idle_begin_sees_table_created_by_other_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-idle-begin-schema.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("begin").unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let schema_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_SCHEMA_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        schema_output.status.success(),
        "child schema process failed: stderr={}",
        String::from_utf8_lossy(&schema_output.stderr)
    );

    let result = conn
        .prepare("select value from child_table")
        .and_then(|mut stmt| stmt.run_collect_rows());
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
    conn.execute("commit").unwrap();
}

#[test]
fn multiprocess_shm_schema_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    assert_eq!(
        coordination_open_mode(&db),
        Some(SharedWalCoordinationOpenTelemetryMode::MultiProcess)
    );

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table child_table(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into child_table(value) values ('child-schema')")
        .unwrap();
}

#[test]
fn write_after_full_checkpoint_restarts_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-restart-after-checkpoint.db");
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    for _ in 0..20 {
        conn.execute("insert into test(value) values ('x')")
            .unwrap();
    }
    let checkpoint = run_checkpoint(
        &conn,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );
    assert!(checkpoint.everything_backfilled());
    let backfilled_frame = checkpoint.wal_total_backfilled;

    conn.execute("insert into test(value) values ('after')")
        .unwrap();
    let max_frame = wal_max_frame(&conn);
    assert!(
        max_frame < backfilled_frame,
        "a write that starts after a full checkpoint must restart the WAL from frame 1, \
         but it appended at frame {max_frame} after frame {backfilled_frame}"
    );
    assert_eq!(count_test_rows(&conn), 21);
}

#[test]
fn test_rollback_releases_shared_read_lock_slot() {
    let (db, _dir) = open_rollback_test_db();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();

    let locked_slots_before = db.wal_read_slots_with_readers();
    assert_eq!(
        locked_slots_before.len(),
        1,
        "expected exactly one shared read-lock slot while transaction is active"
    );

    conn.execute("ROLLBACK").unwrap();

    let locked_slots_after = db.wal_read_slots_with_readers();
    assert!(
        locked_slots_after.is_empty(),
        "ROLLBACK must release the shared read-lock slot"
    );
}

#[test]
fn test_rollback_releases_slot_zero_read_lock() {
    let (db, _dir) = open_rollback_test_db();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 3, 3);
    {
        let result = run_checkpoint(&conn, CheckpointMode::Restart);
        assert!(
            result.everything_backfilled(),
            "restart checkpoint setup must fully backfill WAL"
        );
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO test(value) VALUES('slot0')")
        .unwrap();

    let locked_slots_before = db.wal_read_slots_with_readers();
    assert_eq!(
        locked_slots_before,
        vec![0],
        "writer should use slot 0 when WAL is fully checkpointed"
    );

    conn.execute("ROLLBACK").unwrap();

    let locked_slots_after = db.wal_read_slots_with_readers();
    assert!(
        locked_slots_after.is_empty(),
        "ROLLBACK must release slot 0 shared read-lock as well"
    );
}

#[test]
fn test_savepoint_then_tx_rollback_allows_restart_checkpoint_from_other_connection() {
    let (db, _dir) = open_rollback_test_db();
    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn1
        .execute("CREATE TABLE test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 2, 2);
    let count_before = count_test_rows(&conn1);

    conn1.execute("BEGIN").unwrap();
    conn1
        .execute("INSERT INTO test(id, value) VALUES(1000, 'first')")
        .unwrap();
    let duplicate = conn1.execute("INSERT INTO test(id, value) VALUES(1000, 'dup')");
    assert!(duplicate.is_err(), "duplicate PK insert must fail");

    {
        let pager = conn1.get_pager();
        assert!(
            pager.holds_read_lock(),
            "read lock must still be held after savepoint rollback"
        );
        assert!(
            pager.holds_write_lock(),
            "write lock must still be held after savepoint rollback"
        );
    }

    conn1.execute("ROLLBACK").unwrap();

    {
        let pager = conn1.get_pager();
        assert!(
            !pager.holds_read_lock(),
            "read lock must be released after transaction rollback"
        );
        assert!(
            !pager.holds_write_lock(),
            "write lock must be released after transaction rollback"
        );
    }

    let locked_slots_after_rollback = db.wal_read_slots_with_readers();
    assert!(
        locked_slots_after_rollback.is_empty(),
        "transaction rollback after savepoint failure must not leak shared read locks"
    );
    assert_eq!(
        count_test_rows(&conn1),
        count_before,
        "transaction rollback should remove writes made before savepoint failure"
    );

    let result = run_checkpoint(&conn2, CheckpointMode::Restart);
    assert!(
        result.everything_backfilled(),
        "restart checkpoint from another connection must succeed after full rollback"
    );
}

fn coordination_open_mode(db: &Database) -> Option<SharedWalCoordinationOpenTelemetryMode> {
    db.shared_wal_open_telemetry()
        .unwrap()
        .coordination_open_mode
}

fn get_rows_without_schema_retry(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })
    .unwrap();
    rows
}

fn run_checkpoint(conn: &Arc<Connection>, mode: CheckpointMode) -> CheckpointResult {
    let pager = conn.get_pager();
    pager
        .io
        .block(|| pager.checkpoint(mode, SyncMode::Full, true))
        .unwrap()
}

fn wal_max_frame(conn: &Arc<Connection>) -> u64 {
    conn.get_pager()
        .wal_max_frame()
        .expect("wal should be present")
}

/// Opens a new database with a multiprocess WAL. The returned `TempDir`
/// deletes the database directory when it drops.
fn open_rollback_test_db() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = open_multiprocess_db(multiprocess_test_io(), db_path.to_str().unwrap()).unwrap();
    (db, dir)
}

fn bulk_inserts(conn: &Arc<Connection>, n_txns: usize, rows_per_txn: usize) {
    for _ in 0..n_txns {
        conn.execute("begin transaction").unwrap();
        for i in 0..rows_per_txn {
            conn.execute(format!("insert into test(value) values ('v{i}')"))
                .unwrap();
        }
        conn.execute("commit").unwrap();
    }
}
