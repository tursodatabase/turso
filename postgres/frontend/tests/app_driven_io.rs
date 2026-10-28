use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use turso_core::io::FileId;
use turso_core::{
    Clock, File, IOResult, MemoryYieldIO, MonotonicInstant, OpenFlags, Result, StepResult, Value,
    WallClockInstant, IO,
};
use turso_pg::{DatabaseOpts, PgConnection};

struct AppDrivenIO {
    inner: MemoryYieldIO,
    engine_can_complete_io: AtomicBool,
    engine_steps: AtomicUsize,
}

impl AppDrivenIO {
    fn new() -> Self {
        Self {
            inner: MemoryYieldIO::new(),
            engine_can_complete_io: AtomicBool::new(true),
            engine_steps: AtomicUsize::new(0),
        }
    }
}

impl Clock for AppDrivenIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for AppDrivenIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.inner.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn file_id(&self, path: &str) -> Result<FileId> {
        Ok(FileId::from_path_hash(path))
    }

    fn step(&self) -> Result<()> {
        if self.engine_can_complete_io.load(Ordering::SeqCst) {
            return self.inner.step();
        }
        let calls = self.engine_steps.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(
            calls < 10_000,
            "deferred work pumped IO without returning to the caller"
        );
        Ok(())
    }
}

fn open() -> (Arc<AppDrivenIO>, PgConnection) {
    let io = Arc::new(AppDrivenIO::new());
    let db = turso_pg::open_database_with_io(
        io.clone(),
        "app_driven.db",
        OpenFlags::default(),
        DatabaseOpts::new().with_attach(true),
    )
    .unwrap();
    let conn = PgConnection::connect(&db).unwrap();
    (io, conn)
}

fn run(io: &AppDrivenIO, conn: &PgConnection, sql: &str) -> usize {
    let mut stmt = conn.prepare(sql).unwrap();
    io.engine_can_complete_io.store(false, Ordering::SeqCst);
    let mut yields = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Done => break,
            StepResult::Row => {}
            StepResult::IO | StepResult::Yield => {
                yields += 1;
                io.inner.step().unwrap();
            }
            other => panic!("unexpected result {other:?} for {sql}"),
        }
    }
    io.engine_can_complete_io.store(true, Ordering::SeqCst);
    yields
}

#[test]
fn serial_column_returns_io() {
    let (io, conn) = open();
    assert!(run(&io, &conn, "CREATE TABLE t(id serial PRIMARY KEY, v TEXT)") > 0);
    run(&io, &conn, "INSERT INTO t(v) VALUES ('a')");
}

#[test]
fn create_schema_returns_io() {
    let (io, conn) = open();
    assert!(run(&io, &conn, "CREATE SCHEMA s") > 0);
}

#[test]
fn create_schema_with_elements_returns_io() {
    let (io, conn) = open();
    assert!(
        run(
            &io,
            &conn,
            "CREATE SCHEMA s CREATE TABLE t(id serial PRIMARY KEY)"
        ) > 0
    );
    run(&io, &conn, "INSERT INTO s.t(id) VALUES (1)");
}

#[test]
fn drop_schema_with_table_returns_io() {
    let (io, conn) = open();
    run(&io, &conn, "CREATE SCHEMA s");
    run(&io, &conn, "CREATE TABLE s.t(id int PRIMARY KEY)");
    assert!(run(&io, &conn, "DROP SCHEMA s CASCADE") > 0);
}

#[test]
fn copy_from_returns_io() {
    use std::io::Write;
    let (io, conn) = open();
    run(&io, &conn, "CREATE TABLE t(id int PRIMARY KEY, v TEXT)");
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "1\ta").unwrap();
    assert!(
        run(
            &io,
            &conn,
            &format!("COPY t FROM '{}'", file.path().display())
        ) > 0
    );
    assert_eq!(
        conn.prepare("SELECT count(*) FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap()[0][0],
        Value::from_i64(1)
    );
}

#[test]
fn other_nonblocking_methods_return_io() {
    for method in 0..3 {
        let (io, conn) = open();
        let mut stmt = conn
            .prepare("CREATE TABLE t(id serial PRIMARY KEY)")
            .unwrap();
        io.engine_can_complete_io.store(false, Ordering::SeqCst);
        let mut yields = 0;
        loop {
            let done = match method {
                0 => match stmt.step_with_waker(std::task::Waker::noop()).unwrap() {
                    StepResult::Done => true,
                    StepResult::IO | StepResult::Yield => false,
                    result => panic!("unexpected step result {result:?}"),
                },
                1 => matches!(stmt.run_ignore_rows_nonblock().unwrap(), IOResult::Done(())),
                2 => matches!(
                    stmt.run_with_row_callback_nonblock(|_| Ok(())).unwrap(),
                    IOResult::Done(())
                ),
                _ => unreachable!(),
            };
            if done {
                break;
            }
            yields += 1;
            io.inner.step().unwrap();
        }
        io.engine_can_complete_io.store(true, Ordering::SeqCst);
        assert!(yields > 0, "method {method} did not yield IO");
    }
}
