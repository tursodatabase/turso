use super::*;
use crate::io::{FileId, FileSyncType};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::schema::Schema;
use crate::storage::encryption::{CipherMode, EncryptionKey};
use crate::storage::pager::Page;
use crate::util::IOExt;
use crate::vdbe::execute::TransactionYieldPoint;
use crate::SqliteDialect;
use crate::{
    Buffer, Clock, Completion, DatabaseOpts, File, MemoryIO, MonotonicInstant, OpenFlags,
    WallClockInstant, IO,
};
use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};

fn page_ref(id: i64) -> crate::storage::pager::PageRef {
    Arc::new(Page::new(id))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SyncCounts {
    db: usize,
    wal: usize,
}

struct SyncCountingIo {
    inner: Arc<MemoryIO>,
    tracked_db_path: String,
    tracked_wal_path: String,
    db_syncs: Arc<AtomicUsize>,
    wal_syncs: Arc<AtomicUsize>,
}

impl SyncCountingIo {
    fn new(tracked_db_path: &str) -> Self {
        Self {
            inner: Arc::new(MemoryIO::new()),
            tracked_db_path: tracked_db_path.to_string(),
            tracked_wal_path: format!("{tracked_db_path}-wal"),
            db_syncs: Arc::new(AtomicUsize::new(0)),
            wal_syncs: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn counts(&self) -> SyncCounts {
        SyncCounts {
            db: self.db_syncs.load(StdOrdering::SeqCst),
            wal: self.wal_syncs.load(StdOrdering::SeqCst),
        }
    }
}

impl Clock for SyncCountingIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for SyncCountingIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(SyncCountingFile {
            inner,
            count_as_db_sync: path == self.tracked_db_path,
            count_as_wal_sync: path == self.tracked_wal_path,
            db_syncs: self.db_syncs.clone(),
            wal_syncs: self.wal_syncs.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> Result<()> {
        self.inner.step()
    }

    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        self.inner.drain_completions(completions)
    }

    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        self.inner.cancel(completions)
    }

    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}

struct SyncCountingFile {
    inner: Arc<dyn File>,
    count_as_db_sync: bool,
    count_as_wal_sync: bool,
    db_syncs: Arc<AtomicUsize>,
    wal_syncs: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct OneShotTransactionYieldInjector {
    yielded: AtomicUsize,
}

impl OneShotTransactionYieldInjector {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            yielded: AtomicUsize::new(0),
        })
    }

    fn yielded(&self) -> bool {
        self.yielded.load(StdOrdering::SeqCst) != 0
    }
}

impl YieldInjector for OneShotTransactionYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        point == TransactionYieldPoint::BeforeStart.point()
            && self
                .yielded
                .compare_exchange(0, 1, StdOrdering::SeqCst, StdOrdering::SeqCst)
                .is_ok()
    }
}

impl File for SyncCountingFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        if self.count_as_db_sync {
            self.db_syncs.fetch_add(1, StdOrdering::SeqCst);
        }
        if self.count_as_wal_sync {
            self.wal_syncs.fetch_add(1, StdOrdering::SeqCst);
        }
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}

fn vacuum_copy_batch_ranges(total_pages: u32) -> Vec<(u32, u32)> {
    let mut ranges = Vec::new();
    let mut start = 1;
    while start <= total_pages {
        let end = next_vacuum_copy_batch_start(start, total_pages);
        ranges.push((start, end));
        start = end;
    }
    ranges
}

#[test]
fn coalesce_frame_runs_single_page_is_one_run_of_one() {
    let runs = coalesce_frame_runs(vec![(page_ref(1), 42)]);
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].0, 42);
    assert_eq!(runs[0].1.len(), 1);
    assert_eq!(runs[0].1[0].get().id(), 1);
}

#[test]
fn coalesce_frame_runs_sorts_by_frame_id_then_coalesces_contiguous() {
    // Target-build writes frames out of page-id order. The input is
    // sorted by page id (1,2,3,4), but the frame ids interleave with a
    // gap at frame 3, producing two runs once sorted by frame id.
    let pairs = vec![
        (page_ref(1), 10),
        (page_ref(2), 11),
        (page_ref(3), 12),
        (page_ref(4), 14),
    ];
    let runs = coalesce_frame_runs(pairs);
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].0, 10);
    assert_eq!(runs[0].1.len(), 3);
    assert_eq!(
        runs[0].1.iter().map(|p| p.get().id()).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(runs[1].0, 14);
    assert_eq!(runs[1].1.len(), 1);
    assert_eq!(runs[1].1[0].get().id(), 4);
}

#[test]
fn coalesce_frame_runs_handles_non_monotonic_page_to_frame_map() {
    // Realistic target-build shape: pages 1..=6 but frames assigned by
    // phase order (schema page goes last, indexes interleave). The
    // coalescer must still produce runs of length > 1 for contiguous
    // frame ranges rather than falling back to length-1 runs.
    let pairs = vec![
        (page_ref(1), 10), // schema written last (gap between 6 and 10)
        (page_ref(2), 1),  // data pages written first
        (page_ref(3), 2),
        (page_ref(4), 3),
        (page_ref(5), 5), // index pages later
        (page_ref(6), 6),
    ];
    let runs = coalesce_frame_runs(pairs);
    // After sort by frame id: frames 1,2,3 contiguous; 5,6 contiguous;
    // 10 isolated.
    assert_eq!(runs.len(), 3);
    assert_eq!(runs[0].0, 1);
    assert_eq!(runs[0].1.len(), 3);
    assert_eq!(runs[1].0, 5);
    assert_eq!(runs[1].1.len(), 2);
    assert_eq!(runs[2].0, 10);
    assert_eq!(runs[2].1.len(), 1);

    // Average run length should be non-trivial (>1).
    let total: usize = runs.iter().map(|(_, pages)| pages.len()).sum();
    let avg = total as f64 / runs.len() as f64;
    assert!(avg > 1.0, "expected average run length > 1, got {avg}");
}

#[test]
fn coalesce_frame_runs_all_scattered_is_singleton_runs() {
    let pairs = vec![(page_ref(1), 10), (page_ref(2), 20), (page_ref(3), 30)];
    let runs = coalesce_frame_runs(pairs);
    assert_eq!(runs.len(), 3);
    for (_, pages) in &runs {
        assert_eq!(pages.len(), 1);
    }
}

#[test]
fn coalesce_frame_runs_keeps_run_pages_in_physical_frame_order() {
    let runs = coalesce_frame_runs(vec![
        (page_ref(1), 12),
        (page_ref(2), 10),
        (page_ref(3), 11),
        (page_ref(4), 13),
    ]);

    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].0, 10);
    assert_eq!(
        runs[0].1.iter().map(|p| p.get().id()).collect::<Vec<_>>(),
        vec![2, 3, 1, 4]
    );
}

#[test]
fn vacuum_copy_batch_ranges_cover_boundary_page_counts() {
    let size = VACUUM_COPY_BATCH_SIZE;
    assert!(vacuum_copy_batch_ranges(0).is_empty());
    assert_eq!(vacuum_copy_batch_ranges(1), vec![(1, 2)]);
    assert_eq!(vacuum_copy_batch_ranges(2), vec![(1, 3)]);
    assert_eq!(vacuum_copy_batch_ranges(size - 1), vec![(1, size)]);
    assert_eq!(vacuum_copy_batch_ranges(size), vec![(1, size + 1)]);
    assert_eq!(
        vacuum_copy_batch_ranges(size + 1),
        vec![(1, size + 1), (size + 1, size + 2)]
    );
    assert_eq!(
        vacuum_copy_batch_ranges(size * 2),
        vec![(1, size + 1), (size + 1, size * 2 + 1)]
    );
    assert_eq!(
        vacuum_copy_batch_ranges(size * 2 + 1),
        vec![
            (1, size + 1),
            (size + 1, size * 2 + 1),
            (size * 2 + 1, size * 2 + 2),
        ]
    );
}

#[test]
fn vacuum_db_header_meta_bumps_schema_cookie_and_preserves_sqlite_metadata() {
    let mut source = DatabaseHeader::default();
    source.schema_cookie = u32::MAX.into();
    source.default_page_cache_size = CacheSize::new(123);
    source.text_encoding = TextEncoding::Utf8;
    source.user_version = 7.into();
    source.application_id = 12.into();

    let VacuumDbHeaderMeta {
        read_version,
        write_version,
        schema_cookie,
        default_page_cache_size,
        text_encoding,
        user_version,
        application_id,
    } = VacuumDbHeaderMeta::from_source_header(&source);

    assert_eq!(read_version, source.read_version);
    assert_eq!(write_version, source.write_version);
    assert_eq!(schema_cookie, 0);
    assert_eq!(default_page_cache_size, CacheSize::new(123));
    assert_eq!(text_encoding, TextEncoding::Utf8);
    assert_eq!(user_version, 7);
    assert_eq!(application_id, 12);
}

#[test]
fn replace_shared_schema_after_vacuum_replaces_wrapped_schema_cookie() -> Result<()> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;

    db.with_schema_mut(|schema| {
        schema.schema_version = u32::MAX;
        Ok(())
    })?;

    let replacement = Schema {
        generated_columns_enabled: true,
        schema_version: 0,
        ..Default::default()
    };
    replace_shared_schema_after_vacuum(db.as_ref(), Arc::new(replacement));

    let schema = db.clone_schema();
    assert_eq!(schema.schema_version, 0);
    assert!(schema.generated_columns_enabled);

    Ok(())
}

#[test]
fn install_committed_vacuum_image_updates_connection_and_shared_schema() -> Result<()> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
    let conn = db.connect()?;

    conn.with_schema_mut(|schema| {
        schema.schema_version = 1;
    })?;
    db.with_schema_mut(|schema| {
        schema.schema_version = 1;
        Ok(())
    })?;

    let mut header = DatabaseHeader::default();
    header.schema_cookie = 42.into();
    let committed = VacuumCommittedImageMeta {
        header,
        schema: Arc::new(Schema {
            generated_columns_enabled: true,
            schema_version: 42,
            ..Default::default()
        }),
    };

    install_committed_vacuum_image(&conn, crate::MAIN_DB_ID, &committed);

    assert_eq!(conn.schema.read().schema_version, 42);
    assert!(conn.schema.read().generated_columns_enabled);
    let shared = db.clone_schema();
    assert_eq!(shared.schema_version, 42);
    assert!(shared.generated_columns_enabled);

    Ok(())
}

#[test]
fn cleanup_after_published_vacuum_installs_committed_image() -> Result<()> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
    let conn = db.connect()?;

    conn.with_schema_mut(|schema| {
        schema.schema_version = 1;
    })?;
    db.with_schema_mut(|schema| {
        schema.schema_version = 1;
        Ok(())
    })?;

    let mut header = DatabaseHeader::default();
    header.schema_cookie = 43.into();
    let committed = VacuumCommittedImageMeta {
        header,
        schema: Arc::new(Schema {
            generated_columns_enabled: true,
            schema_version: 43,
            ..Default::default()
        }),
    };

    vacuum_in_place_cleanup(
        &conn,
        crate::MAIN_DB_ID,
        VacuumInPlacePhase::Checkpoint,
        None,
        Some(committed),
        VacuumInPlaceCleanupState::default(),
    )?;

    assert_eq!(conn.schema.read().schema_version, 43);
    assert!(conn.schema.read().generated_columns_enabled);
    let shared = db.clone_schema();
    assert_eq!(shared.schema_version, 43);
    assert!(shared.generated_columns_enabled);

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn capture_target_metadata_uses_final_header_cookie_and_preserves_tvfs() -> Result<()> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("source.db");
    let source_path = source_path.to_str().unwrap();
    let source_db = Database::open_file_with_flags(
        io,
        source_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect()?;
    // Source is uninitialized so its header reserved_space isn't usable.
    // Derive from the IOContext to keep reserved_space and the auto-
    // installed checksum context consistent under `feature = "checksum"`.
    let reserved_space = source_conn
        .get_pager()
        .io_ctx
        .read()
        .get_reserved_space_bytes();
    let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, reserved_space)?;

    temp.conn.execute("BEGIN IMMEDIATE")?;
    temp.conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    temp.conn.execute("CREATE INDEX idx_t_v ON t(v)")?;
    temp.conn.execute("INSERT INTO t VALUES (1, 'x')")?;
    let mvcc_version = RawVersion::from(crate::storage::sqlite3_ondisk::Version::Mvcc);
    let header_meta = VacuumDbHeaderMeta {
        read_version: mvcc_version,
        write_version: mvcc_version,
        schema_cookie: 42,
        default_page_cache_size: CacheSize::new(321),
        text_encoding: TextEncoding::Utf8,
        user_version: 17,
        application_id: 29,
    };
    match finalize_vacuum_target_header(&temp.conn, &header_meta)? {
        crate::IOResult::Done(()) => {}
        crate::IOResult::IO(_) => panic!("fresh temp header should not need async I/O"),
    }
    temp.conn.execute("COMMIT")?;

    let committed = capture_target_metadata(&temp, FxHashMap::default())?;

    assert_eq!(committed.header.schema_cookie.get(), 42);
    assert_eq!(committed.header.read_version, mvcc_version);
    assert_eq!(committed.header.write_version, mvcc_version);
    assert_eq!(committed.schema.schema_version, 42);
    assert!(
        committed.schema.tables.contains_key("generate_series"),
        "captured schema must retain built-in table-valued functions"
    );
    let table_root = match committed.schema.tables.get("t").expect("table t").as_ref() {
        crate::schema::Table::BTree(btree) => btree.root_page,
        _ => panic!("expected btree table"),
    };
    assert!(table_root > 0);
    let index_root = committed
        .schema
        .indexes
        .get("t")
        .and_then(|indexes| indexes.front())
        .map(|index| index.root_page)
        .expect("index idx_t_v");
    assert!(index_root > 0);

    Ok(())
}

#[test]
fn finalize_vacuum_into_output_syncs_destination_db_file() -> Result<()> {
    let io = Arc::new(SyncCountingIo::new("vacuum-into-output.db"));
    let io_dyn: Arc<dyn IO> = io.clone();
    let db = Database::open_file_with_flags(
        io_dyn,
        "vacuum-into-output.db",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;
    conn.set_sync_mode(crate::SyncMode::Off);
    conn.wal_auto_actions_disable();

    conn.execute("BEGIN IMMEDIATE")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'x')")?;
    conn.execute("COMMIT")?;

    let before = io.counts();
    assert_eq!(
        before.db, 1,
        "creating the database does exactly one db fsync (page 1 must be durable \
             before the first WAL commit, even with synchronous=OFF); the target \
             build must not add more before finalization"
    );

    finalize_vacuum_into_output(&VacuumTargetBuildContext::new(conn))?;

    let after = io.counts();
    assert_eq!(
        after.db - before.db,
        1,
        "VACUUM INTO finalization must do exactly one destination DB fsync"
    );
    assert_eq!(
        after.wal - before.wal,
        2,
        "VACUUM INTO finalization must do one WAL fsync before backfill (the \
             checkpoint crash-atomicity barrier; finalization forces synchronous=FULL) \
             and one after truncation"
    );

    Ok(())
}

#[test]
fn vacuum_target_build_preserves_nested_statement_explicit_yield() -> Result<()> {
    let io = Arc::new(MemoryIO::new());
    let source_io: Arc<dyn IO> = io.clone();
    let source_db = Database::open_file_with_flags(
        source_io,
        "vacuum-yield-source.db",
        OpenFlags::Create,
        DatabaseOpts::new().with_vacuum(true),
        None,
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect()?;
    source_conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")?;
    source_conn.execute("CREATE INDEX idx_t_payload ON t(payload)")?;
    for id in 1..=8 {
        source_conn.execute(format!("INSERT INTO t VALUES({id}, 'payload-{id}')"))?;
    }
    source_conn.execute("BEGIN")?;

    let source_pager = source_conn.get_pager();
    let header_meta = source_pager
        .io
        .block(|| source_pager.with_header(VacuumDbHeaderMeta::from_source_header))?;

    let target_io: Arc<dyn IO> = io.clone();
    let target_db = Database::open_file_with_flags(
        target_io,
        "vacuum-yield-target.db",
        OpenFlags::Create,
        vacuum_target_opts_from_source(&source_db),
        None,
        Arc::new(SqliteDialect),
    )?;
    let target_conn = target_db.connect()?;
    mirror_symbols(&source_conn, &target_conn);

    let config = VacuumTargetBuildConfig {
        source_conn: source_conn.clone(),
        escaped_schema_name: "main".to_string(),
        source_db_id: crate::MAIN_DB_ID,
        header_meta,
        source_custom_types: capture_custom_types(&source_conn, crate::MAIN_DB_ID),
        target_mvcc_enabled: source_db.mvcc_enabled(),
        target_auto_vacuum_mode: source_pager.get_auto_vacuum_mode(),
        copy_mvcc_metadata_table: false,
    };

    let injector = OneShotTransactionYieldInjector::new();
    source_conn.set_yield_injector(Some(injector.clone()));

    let mut context = VacuumTargetBuildContext::new(target_conn.clone());
    let mut saw_explicit_yield = false;
    loop {
        match vacuum_target_build_step(&config, &mut context)? {
            crate::IOResult::Done(()) => break,
            crate::IOResult::IO(completions) => {
                saw_explicit_yield |= completions.is_explicit_yield();
                if !completions.is_explicit_yield() {
                    completions.wait(io.as_ref())?;
                }
            }
        }
    }

    source_conn.set_yield_injector(None);
    source_conn.execute("COMMIT")?;

    assert!(injector.yielded(), "test must trigger the injected yield");
    assert!(
        saw_explicit_yield,
        "vacuum target build must return the nested explicit yield instead of panicking"
    );

    let mut copied_rows = None;
    let mut stmt = target_conn.prepare("SELECT COUNT(*) FROM t")?;
    stmt.run_with_row_callback(|row| {
        copied_rows = Some(row.get::<i64>(0)?);
        Ok(())
    })?;
    assert_eq!(copied_rows, Some(8));

    Ok(())
}

#[test]
fn in_place_vacuum_with_sync_off_syncs_source_db_once_and_wal_twice() -> Result<()> {
    let io = Arc::new(SyncCountingIo::new("vacuum-source.db"));
    let io_dyn: Arc<dyn IO> = io.clone();
    let db = Database::open_file_with_flags(
        io_dyn,
        "vacuum-source.db",
        OpenFlags::Create,
        DatabaseOpts::new().with_vacuum(true),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;
    conn.set_sync_mode(crate::SyncMode::Off);

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'x')")?;
    conn.execute("INSERT INTO t VALUES (2, 'y')")?;

    let before = io.counts();
    assert_eq!(
        before.db, 1,
        "creating the database does exactly one source db fsync (page 1 must be \
             durable before the first WAL commit, even with synchronous=OFF); \
             ordinary writes must not add more"
    );

    conn.execute("VACUUM")?;

    let after = io.counts();
    assert_eq!(
        after.db - before.db,
        1,
        "in-place VACUUM must do exactly one source DB fsync before truncating WAL"
    );
    assert_eq!(
        after.wal - before.wal,
        2,
        "in-place VACUUM with synchronous=OFF must do one WAL fsync before backfill \
             (the checkpoint crash-atomicity barrier; VACUUM forces the checkpoint to \
             synchronous=FULL) and one after truncation"
    );

    Ok(())
}

#[test]
fn in_place_vacuum_with_sync_full_adds_pre_publish_wal_sync() -> Result<()> {
    let io = Arc::new(SyncCountingIo::new("vacuum-source-full.db"));
    let io_dyn: Arc<dyn IO> = io.clone();
    let db = Database::open_file_with_flags(
        io_dyn,
        "vacuum-source-full.db",
        OpenFlags::Create,
        DatabaseOpts::new().with_vacuum(true),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;
    conn.set_sync_mode(crate::SyncMode::Off);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'x')")?;
    conn.execute("INSERT INTO t VALUES (2, 'y')")?;

    conn.set_sync_mode(crate::SyncMode::Full);
    let before = io.counts();

    conn.execute("VACUUM")?;

    let after = io.counts();
    assert_eq!(
        after.db - before.db,
        1,
        "in-place VACUUM must still do exactly one source DB fsync under synchronous=FULL"
    );
    assert_eq!(
        after.wal - before.wal,
        3,
        "in-place VACUUM with synchronous=FULL must do one WAL fsync before publish, \
             one before backfill (the checkpoint crash-atomicity barrier), and one after \
             truncation"
    );

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn vacuum_db_header_meta_updates_target_header() -> Result<()> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("source.db");
    let source_path = source_path.to_str().unwrap();
    let source_db = Database::open_file_with_flags(
        io,
        source_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect()?;
    let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, 0)?;

    let mut source_header = DatabaseHeader::default();
    source_header.schema_cookie = 41.into();
    source_header.default_page_cache_size = CacheSize::new(321);
    source_header.text_encoding = TextEncoding::Utf8;
    source_header.user_version = 17.into();
    source_header.application_id = 29.into();
    let header_meta = VacuumDbHeaderMeta::from_source_header(&source_header);

    temp.conn.execute("BEGIN")?;
    match finalize_vacuum_target_header(&temp.conn, &header_meta)? {
        crate::IOResult::Done(()) => {}
        crate::IOResult::IO(_) => panic!("fresh temp header should not need async I/O"),
    }
    temp.conn.execute("COMMIT")?;

    let pager = temp.conn.pager.load();
    let header = pager.io.block(|| {
        pager.with_header(|header| {
            (
                header.schema_cookie.get(),
                header.default_page_cache_size,
                header.text_encoding,
                header.user_version.get(),
                header.application_id.get(),
            )
        })
    })?;

    assert_eq!(
        header,
        (42, CacheSize::new(321), TextEncoding::Utf8, 17, 29)
    );

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn internal_vacuum_temp_db_uses_source_runtime_and_disables_auto_checkpoint() -> Result<()> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("source.db");
    let source_path = source_path.to_str().unwrap();
    let source_db = Database::open_file_with_flags(
        io,
        source_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect()?;

    let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, 0)?;

    assert!(Arc::ptr_eq(&temp._db.io, &source_db.io));
    assert_ne!(temp.path, source_db.path);
    assert_eq!(
        temp.conn.wal_auto_actions(),
        crate::storage::wal::WalAutoActions::empty()
    );
    assert_eq!(temp.conn.get_page_size().get(), 4096);
    assert_eq!(temp.conn.get_reserved_bytes(), Some(0));

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn internal_vacuum_temp_db_preserves_source_encryption() -> Result<()> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("encrypted-source.db");
    let source_path = source_path.to_str().unwrap();
    let key_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let key = EncryptionKey::from_hex_string(key_hex)?;
    let source_db = Database::open_file_with_flags(
        io,
        source_path,
        OpenFlags::Create,
        DatabaseOpts::new().with_encryption(true),
        Some(EncryptionOpts {
            cipher: CipherMode::Aes256Gcm.to_string(),
            hexkey: key_hex.to_string(),
        }),
        Arc::new(SqliteDialect),
    )?;
    let source_conn = source_db.connect_with_encryption(Some(key))?;
    let reserved_space = source_conn
        .get_reserved_bytes()
        .expect("encrypted source should have reserved bytes");

    let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, reserved_space)?;

    assert!(temp._db.experimental_encryption_enabled());
    assert_eq!(
        temp.conn.get_encryption_cipher_mode(),
        source_conn.get_encryption_cipher_mode()
    );
    assert!(temp.conn.encryption_key.read().is_some());
    assert_eq!(temp.conn.get_reserved_bytes(), Some(reserved_space));

    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn vacuum_target_build_cleanup_clears_target_connection_state() -> Result<()> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
    let target_dir = tempfile::tempdir().unwrap();
    let target_path = target_dir.path().join("target.db");
    let target_path = target_path.to_str().unwrap();
    let target_db = Database::open_file_with_flags(
        io,
        target_path,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let target_conn = target_db.connect()?;
    target_conn.execute("CREATE TABLE seed(x)")?;

    let mut context = VacuumTargetBuildContext::new(target_conn.clone());
    let helper_stmt = target_conn.prepare_internal("SELECT 1")?;
    context.phase = VacuumTargetBuildPhase::CollectSchemaRows {
        schema_stmt: Box::new(helper_stmt),
    };

    target_conn.execute("BEGIN IMMEDIATE")?;
    target_conn.execute("INSERT INTO seed VALUES (1)")?;
    let pager = target_conn.pager.load();

    assert!(!target_conn.get_auto_commit());
    assert!(target_conn.is_nested_stmt());

    context.cleanup_after_error()?;

    assert!(target_conn.get_auto_commit());
    assert_eq!(
        target_conn.get_tx_state(),
        crate::connection::TransactionState::None
    );
    assert!(!target_conn.is_nested_stmt());
    assert!(!pager.holds_read_lock());
    assert!(!pager.holds_write_lock());

    let mut stmt = target_conn.prepare("SELECT COUNT(*) FROM seed")?;
    let mut saw_row = false;
    stmt.run_with_row_callback(|_| {
        saw_row = true;
        Ok(())
    })?;
    assert!(
        saw_row,
        "target cleanup should leave the connection queryable"
    );

    Ok(())
}
