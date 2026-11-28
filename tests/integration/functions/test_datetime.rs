use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use crate::common::ExecRows;
use turso_core::{
    Clock, Completion, ConnectOpts, Database, File, MemoryIO, MonotonicInstant, OpenFlags,
    SessionRole, WallClockInstant, IO,
};
use turso_sqlite::SqliteDialect;

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
        .connect(ConnectOpts::new(SessionRole::Superuser))
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
        .connect(ConnectOpts::new(SessionRole::Superuser))
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

/// Runs `sql` with the local time zone set to `tz` and returns its one row.
#[cfg(unix)]
fn exec_row_in_zone(tz: &str, sql: &str) -> Vec<String> {
    std::env::set_var("TZ", tz);
    // chrono caches the zone per thread and reads TZ again only after a
    // second, so run the query on a new thread, which reads TZ on first use.
    let sql = sql.to_string();
    let row = std::thread::spawn(move || {
        let db = Database::open_file(
            Arc::new(MemoryIO::new()),
            ":memory:",
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut row = Vec::new();
        stmt.run_with_row_callback(|r| {
            row = (0..r.len()).map(|i| r.get::<String>(i).unwrap()).collect();
            Ok(())
        })
        .unwrap();
        row
    })
    .join()
    .unwrap();
    std::env::remove_var("TZ");
    row
}

// chrono reads TZ only on Unix. The tests that set TZ run one at a time, and no
// other test in this binary reads the local time zone.
#[cfg(unix)]
#[test]
#[serial_test::serial]
fn utc_and_localtime_modifiers_match_sqlite_near_offset_changes() {
    assert_eq!(
        exec_row_in_zone(
            "America/Los_Angeles",
            "SELECT datetime('2024-06-15 03:00:00','utc'), \
             datetime('2024-03-10 03:00:00','utc'), \
             CAST(unixepoch('2024-03-10 03:00:00','utc') AS TEXT), \
             datetime('2024-03-10 03:00:00','utc','localtime'), \
             datetime('2024-11-03 01:30:00','utc'), \
             datetime('2024-03-10 02:30:00','utc'), \
             datetime('2024-03-10 03:00:00.123','subsec','utc')",
        ),
        vec![
            "2024-06-15 10:00:00",
            "2024-03-10 10:00:00",
            "1710064800",
            "2024-03-10 03:00:00",
            "2024-11-03 08:30:00",
            "2024-03-10 10:30:00",
            "2024-03-10 10:00:00",
        ]
    );
    assert_eq!(
        exec_row_in_zone(
            "Europe/Moscow",
            "SELECT datetime('2014-10-25 23:00:00','utc')"
        ),
        vec!["2014-10-25 19:00:00"]
    );
    assert_eq!(
        exec_row_in_zone(
            "Australia/Lord_Howe",
            "SELECT datetime('2024-04-06 20:00:00','utc')"
        ),
        vec!["2024-04-06 09:00:00"]
    );
}

// SQLite reads the offset of a year before 1970 or after 2037 in one of the
// years 2000 to 2003, and keeps the local date as fields, even when the date
// does not exist (1900-02-29).
#[cfg(unix)]
#[test]
#[serial_test::serial]
fn utc_and_localtime_modifiers_map_years_outside_1970_to_2037_like_sqlite() {
    assert_eq!(
        exec_row_in_zone(
            "Europe/Moscow",
            "SELECT datetime('1960-07-15 12:00:00','localtime'), \
             datetime('2050-07-15 12:00:00','localtime'), \
             datetime('2050-07-15 12:00:00','utc')",
        ),
        vec![
            "1960-07-15 16:00:00",
            "2050-07-15 16:00:00",
            "2050-07-15 08:00:00",
        ]
    );
    assert_eq!(
        exec_row_in_zone(
            "America/Los_Angeles",
            "SELECT datetime('1900-02-28 20:00:00','localtime'), \
             datetime('1900-03-01 04:00:00','localtime')",
        ),
        vec!["1900-02-28 12:00:00", "1900-02-29 20:00:00"]
    );
}
