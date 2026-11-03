use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use crate::common::ExecRows;
use turso_core::{
    Clock, Completion, ConnectOpts, Database, File, MemoryIO, MonotonicInstant, OpenFlags,
    SqliteDialect, WallClockInstant, IO,
};

struct TestClockIo {
    inner: MemoryIO,
    unix_secs: AtomicI64,
    secs_added_per_read: i64,
}

impl Clock for TestClockIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        WallClockInstant {
            secs: self
                .unix_secs
                .fetch_add(self.secs_added_per_read, Ordering::SeqCst),
            micros: 123_456,
        }
    }
}

impl IO for TestClockIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        self.inner.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }

    fn drain_completions(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.inner.drain_completions(completions)
    }

    fn cancel(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.inner.cancel(completions)
    }
}

#[test]
fn current_time_comes_from_the_io_clock() {
    let io = Arc::new(TestClockIo {
        inner: MemoryIO::new(),
        unix_secs: AtomicI64::new(1_000_000_000),
        secs_added_per_read: 0,
    });
    let db = Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();

    let rows: Vec<(String, String, String, String, f64, String)> = conn.exec_rows(
        "SELECT date('now'), time(), datetime('now'), strftime('%s', 'now'), \
         unixepoch('now', 'subsec'), timediff('now', '2001-09-08 01:46:40')",
    );
    assert_eq!(
        rows,
        vec![(
            "2001-09-09".to_string(),
            "01:46:40".to_string(),
            "2001-09-09 01:46:40".to_string(),
            "1000000000".to_string(),
            1_000_000_000.123,
            "+0000-00-01 00:00:00.123".to_string(),
        )]
    );

    let rows: Vec<(f64,)> = conn.exec_rows("SELECT julianday('now')");
    assert_eq!(rows, vec![(2_452_161.574_075_497_7,)]);

    let mut stmt = conn
        .prepare("SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP")
        .unwrap();
    let read_row = |stmt: &mut turso_core::Statement| {
        let mut row = Vec::new();
        stmt.run_with_row_callback(|r| {
            row = (0..3).map(|i| r.get::<String>(i).unwrap()).collect();
            Ok(())
        })
        .unwrap();
        stmt.reset().unwrap();
        row
    };
    assert_eq!(
        read_row(&mut stmt),
        vec!["2001-09-09", "01:46:40", "2001-09-09 01:46:40"]
    );
    io.unix_secs.store(1_000_086_400, Ordering::SeqCst);
    assert_eq!(
        read_row(&mut stmt),
        vec!["2001-09-10", "01:46:40", "2001-09-10 01:46:40"]
    );
}

#[test]
fn current_time_does_not_change_within_one_step() {
    let io = Arc::new(TestClockIo {
        inner: MemoryIO::new(),
        unix_secs: AtomicI64::new(1_000_000_000),
        secs_added_per_read: 1,
    });
    let db = Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();

    let rows: Vec<(i64, i64, String, String)> = conn.exec_rows(
        "SELECT unixepoch(), unixepoch('now'), datetime(), \
         strftime('%Y-%m-%d %H:%M:%S', 'now')",
    );
    assert_eq!(
        rows,
        vec![(
            1_000_000_000,
            1_000_000_000,
            "2001-09-09 01:46:40".to_string(),
            "2001-09-09 01:46:40".to_string(),
        )]
    );

    let rows: Vec<(i64,)> = conn.exec_rows(
        "SELECT count(DISTINCT unixepoch('now', column1 || ' seconds') - column1) \
         FROM (VALUES (1), (2), (3))",
    );
    assert_eq!(rows, vec![(1,)]);

    let rows: Vec<(i64, i64)> = conn.exec_rows(
        "SELECT unixepoch(), unixepoch('now', column1 || ' seconds') - column1 \
         FROM (VALUES (1), (2), (3))",
    );
    assert_eq!(
        rows,
        vec![
            (1_000_000_002, 1_000_000_002),
            (1_000_000_002, 1_000_000_003),
            (1_000_000_002, 1_000_000_004),
        ]
    );
}
