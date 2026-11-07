use super::*;
use crate::io::clock::{MonotonicInstant, WallClockInstant};
use crate::io::{Clock, File, FileId, MemoryYieldIO};

struct CountingIO {
    inner: MemoryYieldIO,
    path: String,
    opens: AtomicUsize,
    fail: AtomicBool,
}

impl Clock for CountingIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for CountingIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> crate::Result<Arc<dyn File>> {
        if path == self.path {
            self.opens.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(LimboError::InvalidArgument(
                    "injected default resolver failure".into(),
                ));
            }
        }
        self.inner.open_file(path, flags, direct)
    }
    fn file_id(&self, path: &str) -> crate::Result<FileId> {
        self.inner.file_id(path)
    }
    fn remove_file(&self, path: &str) -> crate::Result<()> {
        self.inner.remove_file(path)
    }
    fn step(&self) -> crate::Result<()> {
        self.inner.step()
    }
}

fn fixture(path: String) -> Arc<CountingIO> {
    let io = Arc::new(CountingIO {
        inner: MemoryYieldIO::new(),
        path,
        opens: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
    });
    let db = Database::open_file(io.clone(), &io.path, Arc::new(EngineDialect)).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t(x); INSERT INTO t VALUES(7)")
        .unwrap();
    drop(conn);
    drop(db);
    io.opens.store(0, Ordering::SeqCst);
    io
}

#[test]
fn async_default_storage_is_opened_once_across_real_read_yields() {
    let directory = tempfile::tempdir().unwrap();
    let io = fixture(
        directory
            .path()
            .join("yield.db")
            .to_string_lossy()
            .into_owned(),
    );
    let options = OpenOptions::new(Arc::new(EngineDialect));
    let mut state = OpenDbAsyncState::new();
    let mut yields = 0;
    let db = loop {
        match Database::open_async(&mut state, io.clone(), &io.path, &options).unwrap() {
            IOResult::IO(completion) => {
                yields += 1;
                assert_eq!(io.opens.load(Ordering::SeqCst), 1);
                completion.wait(&*io).unwrap();
            }
            IOResult::Done(db) => break db,
        }
    };
    assert!(
        yields > 0,
        "the backend must force real read completion yields"
    );
    assert_eq!(io.opens.load(Ordering::SeqCst), 1);
    db.connect(ConnectOpts::new())
        .unwrap()
        .execute("SELECT * FROM t")
        .unwrap();
}

#[test]
fn async_default_resolver_error_and_cancellation_release_reservation() {
    let directory = tempfile::tempdir().unwrap();
    let io = fixture(
        directory
            .path()
            .join("retry.db")
            .to_string_lossy()
            .into_owned(),
    );
    let key = DatabaseKey::File(io.file_id(&io.path).unwrap());
    let options = OpenOptions::new(Arc::new(EngineDialect));
    io.fail.store(true, Ordering::SeqCst);
    let mut failed = OpenDbAsyncState::new();
    let result = Database::open_async(&mut failed, io.clone(), &io.path, &options);
    assert!(
        matches!(result, Err(error) if matches!(*error, LimboError::InvalidArgument(ref message) if message == "injected default resolver failure"))
    );
    assert!(!DATABASE_MANAGER.lock().contains_key(&key));
    assert!(failed.registry_key.is_none());
    io.fail.store(false, Ordering::SeqCst);
    let mut cancelled = OpenDbAsyncState::new();
    let completion =
        match Database::open_async(&mut cancelled, io.clone(), &io.path, &options).unwrap() {
            IOResult::IO(completion) => completion,
            IOResult::Done(_) => panic!("expected real deferred read"),
        };
    assert!(DATABASE_MANAGER.lock().contains_key(&key));
    drop(cancelled);
    assert!(!DATABASE_MANAGER.lock().contains_key(&key));
    completion.wait(&*io).unwrap();
    let reopened = Database::open(io.clone(), &io.path, options).unwrap();
    reopened
        .connect(ConnectOpts::new())
        .unwrap()
        .execute("SELECT * FROM t")
        .unwrap();
}

#[test]
fn async_default_cached_open_rejects_new_custom_durable_storage() {
    use crate::mvcc::persistent_storage::{DurableStorage, Storage};
    let directory = tempfile::tempdir().unwrap();
    let io = fixture(
        directory
            .path()
            .join("custom.db")
            .to_string_lossy()
            .into_owned(),
    );
    let db = Database::open(
        io.clone(),
        &io.path,
        OpenOptions::new(Arc::new(EngineDialect)),
    )
    .unwrap();
    let log = io
        .open_file("custom-log", OpenFlags::Create, false)
        .unwrap();
    let durable: Arc<dyn DurableStorage> = Arc::new(Storage::new(log, io.clone(), None));
    let options = OpenOptions::new(Arc::new(EngineDialect)).durable_storage(durable);
    let before = io.opens.load(Ordering::SeqCst);
    let mut state = OpenDbAsyncState::new();
    let error = Database::open_async(&mut state, io.clone(), &io.path, &options).unwrap_err();
    assert!(
        matches!(*error, LimboError::InvalidArgument(ref message) if message.contains("without custom durable storage"))
    );
    let error = Database::open(io.clone(), &io.path, options).unwrap_err();
    assert!(
        matches!(error, LimboError::InvalidArgument(ref message) if message.contains("without custom durable storage"))
    );
    assert_eq!(io.opens.load(Ordering::SeqCst), before);
    drop(db);
}
