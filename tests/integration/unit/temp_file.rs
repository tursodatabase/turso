use turso_core::io::clock::Clock;
#[allow(unused_imports)]
use turso_core::io::completions::*;
use turso_core::io::memory::MemoryIO;
use turso_core::io::*;
use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_core::Result;
use turso_sqlite::SqliteDialect;

#[test]
fn creates_the_file_on_first_write_and_removes_it_on_drop() {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().expect("platform IO must initialize"));
    let temp_file = DiskTempFile::new(io.clone());
    assert!(temp_file.created.get().is_none());
    assert_eq!(temp_file.size().unwrap(), 0);
    let c = temp_file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync);
    io.wait_for_completion(c.unwrap()).unwrap();
    assert!(temp_file.created.get().is_none());

    let buffer = Arc::new(Buffer::new(vec![7; 512]));
    let c = temp_file.pwrite(0, buffer, Completion::new_write(|_| {}));
    io.wait_for_completion(c.unwrap()).unwrap();
    assert_eq!(temp_file.size().unwrap(), 512);
    let temp_dir = temp_file.created.get().unwrap().dir.path().to_owned();
    assert!(temp_dir.exists());

    drop(temp_file);
    assert!(!temp_dir.exists());
}

#[test]
fn ephemeral_table_opens_a_temp_file_only_when_it_spills() {
    let io = Arc::new(TempFileCountingIO::default());
    let db = turso_core::Database::open_file(
        io.clone(),
        "ephemeral-temp-file.db",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let count_union = |rows: i64| -> i64 {
        let sql = format!(
            "SELECT count(*) FROM (\
             SELECT printf('%02500d', value) FROM generate_series(1, {rows}) \
             UNION SELECT printf('%02500d', value) FROM generate_series(1, {rows}))"
        );
        let mut stmt = conn.prepare(sql).unwrap();
        let mut count = None;
        stmt.run_with_row_callback(|row| {
            count = Some(row.get::<i64>(0).unwrap());
            Ok(())
        })
        .unwrap();
        count.unwrap()
    };

    assert_eq!(count_union(10), 10);
    assert_eq!(io.temp_files_opened.load(Ordering::Acquire), 0);

    assert_eq!(count_union(4_000), 4_000);
    assert!(io.temp_files_opened.load(Ordering::Acquire) > 0);
}

#[derive(Default)]
struct TempFileCountingIO {
    inner: MemoryIO,
    temp_files_opened: turso_core::sync::atomic::AtomicUsize,
}

use turso_core::sync::atomic::Ordering;
use turso_core::{MonotonicInstant, WallClockInstant};

impl Clock for TempFileCountingIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for TempFileCountingIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        if path.ends_with("tursodb_temp_file") {
            self.temp_files_opened.fetch_add(1, Ordering::AcqRel);
        }
        self.inner.open_file(path, flags, direct)
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}
