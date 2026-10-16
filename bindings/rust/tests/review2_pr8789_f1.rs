#![cfg(feature = "io_memory_yield")]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use turso_core::io::clock::DefaultClock;
use turso_core::io::FileSyncType;
use turso_core::{
    Buffer, CheckpointMode, Clock, Completion, CompletionError, ConnectOpts, Connection, Database,
    DatabaseOpts, File, LimboError, MemoryIO, MonotonicInstant, OpenFlags, SqliteDialect,
    Statement, StepResult, Value, WallClockInstant, IO,
};

type ReadResult = Result<(Arc<Buffer>, i32), CompletionError>;

struct DeferredRead {
    completion: Completion,
    result: ReadResult,
}

#[derive(Default)]
struct Queues {
    pending: VecDeque<DeferredRead>,
    held: VecDeque<DeferredRead>,
}

pub struct DeferredReadIO {
    inner: Arc<MemoryIO>,
    queues: Arc<Mutex<Queues>>,
}

fn is_table_leaf(result: &ReadResult) -> bool {
    match result {
        Ok((buf, n)) => *n > 0 && buf.as_slice()[0] == 0x0D,
        Err(_) => false,
    }
}

fn leaf_cell_count(result: &ReadResult) -> usize {
    match result {
        Ok((buf, _)) => {
            let s = buf.as_slice();
            u16::from_be_bytes([s[3], s[4]]) as usize
        }
        Err(e) => panic!("read failed: {e:?}"),
    }
}

impl DeferredReadIO {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryIO::new()),
            queues: Arc::new(Mutex::new(Queues::default())),
        }
    }

    fn pending_table_leaf_reads(&self) -> usize {
        let queues = self.queues.lock().unwrap();
        queues
            .pending
            .iter()
            .filter(|d| is_table_leaf(&d.result))
            .count()
    }

    fn hold_pending(&self) -> Vec<usize> {
        let mut queues = self.queues.lock().unwrap();
        let moved = std::mem::take(&mut queues.pending);
        let cell_counts = moved.iter().map(|d| leaf_cell_count(&d.result)).collect();
        queues.held.extend(moved);
        cell_counts
    }

    fn release_held(&self) {
        let mut queues = self.queues.lock().unwrap();
        let held = std::mem::take(&mut queues.held);
        queues.pending.extend(held);
    }
}

impl Clock for DeferredReadIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for DeferredReadIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>, LimboError> {
        let inner = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(DeferredReadFile {
            inner,
            queues: self.queues.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> Result<(), LimboError> {
        self.inner.remove_file(path)
    }

    fn file_id(&self, path: &str) -> Result<turso_core::io::FileId, LimboError> {
        self.inner.file_id(path)
    }

    fn step(&self) -> Result<(), LimboError> {
        self.inner.step()?;
        let drained = {
            let mut queues = self.queues.lock().unwrap();
            std::mem::take(&mut queues.pending)
        };
        for d in drained {
            match d.result {
                Ok((_, n)) => d.completion.complete(n),
                Err(e) => d.completion.error(e),
            }
        }
        Ok(())
    }
}

struct DeferredReadFile {
    inner: Arc<dyn File>,
    queues: Arc<Mutex<Queues>>,
}

impl File for DeferredReadFile {
    fn lock_file(&self, exclusive: bool) -> Result<(), LimboError> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<(), LimboError> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion, LimboError> {
        let buf = c.as_read().buf.clone();
        let queues = self.queues.clone();
        let outer = c.clone();
        let inner_c = Completion::new_read(buf, move |res: ReadResult| {
            queues.lock().unwrap().pending.push_back(DeferredRead {
                completion: outer.clone(),
                result: res,
            });
            None
        });
        let _ = self.inner.pread(pos, inner_c)?;
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> Result<Completion, LimboError> {
        self.inner.pwrite(pos, buffer, c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> Result<Completion, LimboError> {
        self.inner.pwritev(pos, buffers, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion, LimboError> {
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> Result<u64, LimboError> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion, LimboError> {
        self.inner.truncate(len, c)
    }
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut out = Vec::new();
    stmt.run_with_row_callback(|row| {
        out.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })
    .unwrap();
    out
}

fn ints(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    rows(conn, sql)
        .into_iter()
        .map(|r| match r[0] {
            Value::Numeric(turso_core::Numeric::Integer(i)) => i,
            ref other => panic!("not an integer: {other:?}"),
        })
        .collect()
}

fn texts(conn: &Arc<Connection>, sql: &str) -> Vec<String> {
    rows(conn, sql)
        .into_iter()
        .map(|r| format!("{}", r[0]))
        .collect()
}

fn step_until_table_leaf_read_pending(stmt: &mut Statement, io: &DeferredReadIO) {
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield => {
                if io.pending_table_leaf_reads() > 0 {
                    return;
                }
                io.step().unwrap();
            }
            StepResult::Row => {}
            StepResult::Done => panic!("statement finished before it read a table leaf page"),
            other => panic!("unexpected step result {other:?}"),
        }
    }
    panic!("no table leaf read after 10000 steps");
}

fn run_to_done(stmt: &mut Statement, io: &DeferredReadIO) {
    for _ in 0..100_000 {
        match stmt.step().unwrap() {
            StepResult::Done => return,
            StepResult::IO | StepResult::Yield => io.step().unwrap(),
            StepResult::Row => {}
            other => panic!("unexpected step result {other:?}"),
        }
    }
    panic!("statement did not finish");
}

fn open_db(name: &str, mvcc: bool) -> (Arc<DeferredReadIO>, Arc<Database>) {
    let io = Arc::new(DeferredReadIO::new());
    let io_dyn: Arc<dyn IO> = io.clone();
    let opts = if mvcc {
        DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true)
    } else {
        DatabaseOpts::new()
    };
    let db = Database::open_file_with_flags(
        io_dyn,
        &format!("/review2_pr8789_f1/{name}.db"),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let setup = db.connect(ConnectOpts::new()).unwrap();
    if mvcc {
        rows(&setup, "PRAGMA journal_mode = mvcc");
        setup
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
    }
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    rows(&setup, "PRAGMA wal_checkpoint(TRUNCATE)");
    setup.close().unwrap();
    (io, db)
}

fn scenario(name: &str, tear_probe: bool) -> (Arc<Connection>, Result<(), LimboError>) {
    let (io, db) = open_db(name, true);
    let p = db.connect(ConnectOpts::new()).unwrap();
    let w = db.connect(ConnectOpts::new()).unwrap();
    for conn in [&p, &w] {
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
    }

    p.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(ints(&p, "SELECT count(*) FROM t"), [2]);

    w.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    p.checkpoint(CheckpointMode::Passive {
        upper_bound_inclusive: None,
    })
    .unwrap();

    w.execute("INSERT INTO t VALUES (4, 'd')").unwrap();
    rows(&w, "PRAGMA wal_checkpoint(PASSIVE)");

    let mut s1 = p.prepare("INSERT INTO t VALUES (50, 'p')").unwrap();
    if tear_probe {
        step_until_table_leaf_read_pending(&mut s1, &io);
        let held = io.hold_pending();
        assert_eq!(
            held,
            [4],
            "exactly one table leaf read is held, and it is the page with rows 1..4 (view N1)"
        );
    } else {
        run_to_done(&mut s1, &io);
    }

    w.execute("INSERT INTO t VALUES (10, 'w')").unwrap();
    rows(&w, "PRAGMA wal_checkpoint(PASSIVE)");

    let p_count = ints(&p, "SELECT count(*) FROM t");
    eprintln!("[{name}] P (unpinned, open tx) counts {p_count:?} rows after refresh");

    if tear_probe {
        io.release_held();
        run_to_done(&mut s1, &io);
    }
    drop(s1);

    p.execute("COMMIT").unwrap();

    w.execute("PRAGMA mvcc_gc_threshold = 1").unwrap();
    w.execute("INSERT INTO t VALUES (200, 'gc')").unwrap();

    let v = db.connect(ConnectOpts::new()).unwrap();
    let visible = ints(&v, "SELECT id FROM t ORDER BY id");
    assert!(
        visible.contains(&10),
        "row 10 must be visible to the victim: {visible:?}"
    );
    let result = v.execute("INSERT INTO t VALUES (10, 'dup')");
    (v, result)
}

#[test]
fn duplicate_rowid_accepted_when_probe_is_torn_across_a_refresh() {
    let (v, result) = scenario("torn", true);
    let values = texts(&v, "SELECT v FROM t WHERE id = 10");
    let all = ints(&v, "SELECT id FROM t ORDER BY id");
    assert!(
        result.is_err(),
        "rowid 10 is in the B-tree and visible, but the INSERT was accepted; \
         t.v where id=10 now reads {values:?}; ids: {all:?}"
    );
    let err = result.unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    assert_eq!(values, ["w"]);
}

#[test]
fn control_duplicate_rowid_rejected_when_probe_finishes_before_the_refresh() {
    let (v, result) = scenario("control", false);
    let err = result.unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    assert_eq!(texts(&v, "SELECT v FROM t WHERE id = 10"), ["w"]);
}

#[test]
fn control_non_mvcc_duplicate_rowid_rejected() {
    let (_io, db) = open_db("plain", false);
    let w = db.connect(ConnectOpts::new()).unwrap();
    w.execute("INSERT INTO t VALUES (3, 'c'), (4, 'd')")
        .unwrap();
    rows(&w, "PRAGMA wal_checkpoint(PASSIVE)");
    w.execute("INSERT INTO t VALUES (10, 'w')").unwrap();
    rows(&w, "PRAGMA wal_checkpoint(PASSIVE)");
    w.execute("INSERT INTO t VALUES (200, 'gc')").unwrap();

    let v = db.connect(ConnectOpts::new()).unwrap();
    assert!(ints(&v, "SELECT id FROM t ORDER BY id").contains(&10));
    let err = v.execute("INSERT INTO t VALUES (10, 'dup')").unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    assert_eq!(texts(&v, "SELECT v FROM t WHERE id = 10"), ["w"]);
}
