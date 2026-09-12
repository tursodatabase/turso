#[cfg(host_shared_wal)]
use super::{
    classify_authority_snapshot_against_wal, AuthoritySnapshotRebuildReason,
    AuthoritySnapshotValidation, ShmWalCoordination,
};
use super::{
    CheckpointLocks, InProcessWalCoordination, ReadGuardKind, RollbackTo, TryBeginReadResult, Wal,
    WalAutoActions, WalCommitState, WalConnectionState, WalCoordination, WalFile, WalSnapshot,
    NO_LOCK_HELD,
};
#[cfg(host_shared_wal)]
use crate::storage::shared_wal_coordination::{
    MappedSharedWalCoordination, SharedWalCoordinationHeader, SharedWalCoordinationOpenMode,
};
use crate::sync::{atomic::Ordering, Arc};
use crate::sync::{Mutex, RwLock};
use crate::SqliteDialect;
use crate::{
    io::FileSyncType,
    storage::{
        buffer_pool::BufferPool,
        database::{DatabaseFile, DatabaseStorage},
        encryption::{CipherMode, EncryptionContext, EncryptionKey},
        page_transform::{PageCodec, PageCodecContext, PageCodecId},
        pager::{allocate_new_page, PageRef},
        sqlite3_ondisk::{self, PageSize, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE},
        wal::READMARK_NOT_USED,
    },
    types::IOResult,
    util::IOExt,
    Buffer, CheckpointMode, CheckpointResult, Completion, CompletionError, Connection, Database,
    File, IOContext, LimboError, MemoryIO, OpenFlags, PlatformIO, Result, SyncMode, WalFileShared,
    IO,
};
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
/// Returns an IO backend that supports shared WAL coordination on the host.
/// On Windows the default `PlatformIO` (`WindowsIO`) lacks the byte-locking
/// and mapping primitives, so the experimental IOCP backend is used when
/// the `experimental_win_iocp` feature is enabled.
fn shared_wal_test_io() -> Arc<dyn IO> {
    #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
    {
        Arc::new(crate::WindowsIOCP::new().unwrap())
    }
    #[cfg(not(all(target_os = "windows", feature = "experimental_win_iocp")))]
    {
        Arc::new(PlatformIO::new().unwrap())
    }
}

/// The returned `TempDir` deletes the database directory when it drops, so
/// callers must hold it for as long as they use the database.
#[allow(clippy::arc_with_non_send_sync)]
pub(crate) fn get_database() -> (Arc<Database>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("test.db");
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "wal")
            .unwrap();
    }
    let io = shared_wal_test_io();
    let db = Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new().with_multiprocess_wal(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    // db + tmp directory
    (db, temp_dir)
}

struct DeferredReadFile {
    inner: Arc<dyn File>,
    pending_reads: Mutex<Vec<(u64, Completion)>>,
}

impl DeferredReadFile {
    fn new(inner: Arc<dyn File>) -> Self {
        Self {
            inner,
            pending_reads: Mutex::new(Vec::new()),
        }
    }

    fn complete_pending_reads(&self) {
        let pending_reads = std::mem::take(&mut *self.pending_reads.lock());
        for (pos, completion) in pending_reads {
            std::mem::drop(self.inner.pread(pos, completion).unwrap());
        }
    }
}

impl File for DeferredReadFile {
    fn lock_file(&self, exclusive: bool) -> crate::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> crate::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> crate::Result<Completion> {
        self.pending_reads.lock().push((pos, c.clone()));
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> crate::Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Completion, sync_type: crate::io::FileSyncType) -> crate::Result<Completion> {
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> crate::Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> crate::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn replace_after_external_restore_preserves_lock_identity() {
    let shared = WalFileShared::new_noop();
    let restored = WalFileShared::new_noop();

    let read_lock_ptrs = {
        let shared = shared.read();
        shared
            .runtime
            .read_locks
            .iter()
            .map(std::ptr::from_ref)
            .collect::<Vec<_>>()
    };
    {
        let shared = shared.read();
        assert!(shared.runtime.read_locks[1].write());
        shared.runtime.read_locks[1].set_value_exclusive(7);
        shared.runtime.read_locks[1].unlock();
    }
    {
        let restored = restored.read();
        restored.metadata.max_frame.store(42, Ordering::Release);
        assert!(restored.runtime.read_locks[1].write());
        restored.runtime.read_locks[1].set_value_exclusive(99);
        restored.runtime.read_locks[1].unlock();
    }

    let restored = match Arc::try_unwrap(restored) {
        Ok(restored) => restored.into_inner(),
        Err(_) => panic!("restored WAL test state should not be shared"),
    };
    shared.write().replace_after_external_restore(restored);

    let shared = shared.read();
    assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 42);
    assert_eq!(shared.runtime.read_locks[1].get_value(), 7);
    for (idx, lock) in shared.runtime.read_locks.iter().enumerate() {
        assert_eq!(std::ptr::from_ref(lock), read_lock_ptrs[idx]);
    }
}

#[test]
fn test_truncate_file() {
    let (db, _path) = get_database();
    let conn = db.connect().unwrap();
    conn.execute("create table test (id integer primary key, value text)")
        .unwrap();
    let _ = conn.execute("insert into test (value) values ('test1'), ('test2'), ('test3')");
    let wal = db.shared_wal.write();
    let wal_file = wal.runtime.file.as_ref().unwrap().clone();
    let done = Arc::new(Mutex::new(false));
    let _done = done.clone();
    let _ = wal_file.truncate(
        WAL_HEADER_SIZE as u64,
        Completion::new_trunc(move |_| {
            *_done.lock() = true;
        }),
    );
    assert!(wal_file.size().unwrap() == WAL_HEADER_SIZE as u64);
    assert!(*done.lock());
}

#[test]
fn test_wal_truncate_checkpoint() {
    let (db, path) = get_database();
    let walpath = path.path().join("test.db-wal");

    let conn = db.connect().unwrap();
    conn.execute("create table test (id integer primary key, value text)")
        .unwrap();
    for _i in 0..25 {
        let _ = conn.execute("insert into test (value) values (randomblob(1024)), (randomblob(1024)), (randomblob(1024))");
    }
    let pager = conn.pager.load();
    let _ = pager.cacheflush();

    let stat = std::fs::metadata(&walpath).unwrap();
    let meta_before = std::fs::metadata(&walpath).unwrap();
    let bytes_before = meta_before.len();
    run_checkpoint_until_done(
        &pager,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    assert_eq!(pager.wal_state().unwrap().max_frame, 0);

    tracing::debug!("wal filepath: {walpath:?}, size: {}", stat.len());
    let meta_after = std::fs::metadata(&walpath).unwrap();
    let bytes_after = meta_after.len();
    assert_ne!(
        bytes_before, bytes_after,
        "WAL file should not have been empty before checkpoint"
    );
    assert_eq!(
        bytes_after, 0,
        "WAL file should be truncated to 0 bytes, but is {bytes_after} bytes",
    );
}

#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "experimental_win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shutdown_checkpoint_truncates_after_restart() {
    let (db, path) = get_database();
    let walpath = path.path().join("test.db-wal");

    let conn = db.connect().unwrap();
    conn.execute("create table test (id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test (value) values ('v1'), ('v2')")
        .unwrap();

    let pager = conn.pager.load();
    run_checkpoint_until_done(&pager, CheckpointMode::Restart);

    let bytes_before = std::fs::metadata(&walpath).unwrap().len();
    assert!(
        bytes_before > 0,
        "WAL should still have data after RESTART checkpoint"
    );

    conn.close().unwrap();

    let bytes_after = std::fs::metadata(&walpath).unwrap().len();
    assert_eq!(
        bytes_after, 0,
        "Shutdown checkpoint should truncate WAL after RESTART, but WAL is {bytes_after} bytes",
    );
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

fn count_test_table(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("select count(*) from test").unwrap();
    let mut count: i64 = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    count
}

fn run_checkpoint_until_done(pager: &crate::Pager, mode: CheckpointMode) -> CheckpointResult {
    // Use pager.checkpoint() instead of wal.checkpoint() directly because
    // WAL truncation (for TRUNCATE mode) now happens in pager's TruncateWalFile phase.
    pager
        .io
        .block(|| pager.checkpoint(mode, crate::SyncMode::Full, true))
        .unwrap()
}

fn run_wal_checkpoint_until_done(
    db: &Database,
    pager: &crate::Pager,
    mode: CheckpointMode,
) -> CheckpointResult {
    let wal = pager.wal.as_ref().expect("wal should be present");
    loop {
        match wal.checkpoint(pager, mode, SyncMode::Full) {
            Ok(IOResult::IO(io)) => io.wait(db.io.as_ref()).unwrap(),
            Ok(IOResult::Done(result)) => return result,
            Err(err) => panic!("checkpoint should succeed: {err:?}"),
        }
    }
}

#[test]
fn test_wal_checkpoint_defers_backfill_publication_until_db_sync() {
    let (db, _path) = get_database();
    let wal_shared = db.shared_wal.clone();
    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 2);

    let pager = conn.pager.load();
    let result = run_wal_checkpoint_until_done(&db, &pager, CheckpointMode::Full);
    assert!(
        result.wal_total_backfilled > 0,
        "checkpoint setup should backfill frames before DB sync"
    );
    assert_eq!(
        wal_shared.read().metadata.nbackfills.load(Ordering::SeqCst),
        0,
        "wal.checkpoint() must not publish positive nbackfills before DB sync completes"
    );
}

#[test]
fn test_checkpoint_sync_mode_off_leaves_backfill_unpublished() {
    let (db, _path) = get_database();
    let wal_shared = db.shared_wal.clone();
    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 2);

    let pager = conn.pager.load();
    let result = pager
        .io
        .block(|| pager.checkpoint(CheckpointMode::Full, SyncMode::Off, true))
        .unwrap();
    assert!(
        result.wal_total_backfilled > 0,
        "sync-mode-off checkpoint setup should still backfill frames into the DB file"
    );
    assert_eq!(
        wal_shared.read().metadata.nbackfills.load(Ordering::SeqCst),
        0,
        "SyncMode::Off must not publish positive nbackfills as durable shared state"
    );
}

fn make_test_wal() -> (Arc<RwLock<WalFileShared>>, WalFile) {
    let io = shared_wal_test_io();
    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    let shared = WalFileShared::new_noop();
    let coordination: Arc<dyn WalCoordination> =
        Arc::new(InProcessWalCoordination::new(shared.clone()));
    let wal = WalFile::new_with_coordination(io, coordination, ((0, 0), 0), buffer_pool);
    (shared, wal)
}

fn make_test_wal_from_shared(shared: Arc<RwLock<WalFileShared>>) -> WalFile {
    let io = shared_wal_test_io();
    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    let snapshot = shared.read().last_checksum_and_max_frame();
    WalFile::new(io, shared, snapshot, buffer_pool)
}

fn make_initialized_memory_wal(page_size: u32) -> (Arc<dyn IO>, Arc<BufferPool>, WalFile) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    buffer_pool
        .finalize_with_page_size(page_size as usize)
        .unwrap();
    let file = io
        .open_file("direct-batch-read.db-wal", OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    let wal = WalFile::new(io.clone(), shared, ((0, 0), 0), buffer_pool.clone());
    let page_size = PageSize::new(page_size).unwrap();

    if let Some(c) = wal.prepare_wal_start(page_size).unwrap() {
        io.wait_for_completion(c).unwrap();
    }
    let c = wal.prepare_wal_finish(FileSyncType::Fsync).unwrap();
    io.wait_for_completion(c).unwrap();

    (io, buffer_pool, wal)
}

/// Like `make_initialized_memory_wal`, but backed by `MemoryYieldIO`, which
/// writes bytes synchronously yet defers every I/O *completion* until the
/// next `io.step()`. That makes the "write submitted but not yet durable"
/// window observable in a single-threaded test.
#[cfg(feature = "io_memory_yield")]
fn make_initialized_memory_yield_wal(page_size: u32) -> (Arc<dyn IO>, Arc<BufferPool>, WalFile) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryYieldIO::new());
    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    buffer_pool
        .finalize_with_page_size(page_size as usize)
        .unwrap();
    let file = io
        .open_file("spill-visibility.db-wal", OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    let wal = WalFile::new(io.clone(), shared, ((0, 0), 0), buffer_pool.clone());
    let page_size = PageSize::new(page_size).unwrap();

    if let Some(c) = wal.prepare_wal_start(page_size).unwrap() {
        io.wait_for_completion(c).unwrap();
    }
    let c = wal.prepare_wal_finish(FileSyncType::Fsync).unwrap();
    io.wait_for_completion(c).unwrap();

    (io, buffer_pool, wal)
}

/// Regression test for the cache-spill WAL append path: a frame appended via
/// `append_frames_vectored` must not become resolvable by `find_frame`
/// (reads) or `iter_latest_frames` (checkpoint) until its write is durable.
///
/// An earlier version of the async spill fix published the page->frame
/// mapping (`cache_frame`) synchronously at submission, before the write
/// landed on disk — so a reader or a concurrent checkpoint could resolve a
/// frame whose bytes were not yet written. `MemoryYieldIO` defers the write
/// completion until `io.step()`, so this test can observe the frame while
/// the write is still in flight: the mapping must not be visible yet.
#[cfg(feature = "io_memory_yield")]
#[test]
fn append_frames_vectored_frame_hidden_until_write_is_durable() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_yield_wal(page_size);
    let page = page_with_pattern(7, 0x70, &buffer_pool);

    let completion = wal
        .append_frames_vectored(vec![page], PageSize::new(page_size).unwrap())
        .unwrap();

    // The write cursor advances synchronously (so a following batch chains
    // correctly), but the completion has not fired: the write is not durable.
    assert!(
        !completion.succeeded(),
        "MemoryYieldIO must defer the write completion until io.step()"
    );
    assert_eq!(
        wal.get_max_frame(),
        1,
        "write cursor advances synchronously"
    );

    // The frame must NOT be resolvable before the write is durable. The
    // buggy version cached the mapping at submission and returned Some(1)
    // here, exposing bytes that were not on disk yet.
    assert_eq!(
        wal.find_frame(7, None).unwrap(),
        None,
        "frame must not be visible to readers before its write is durable"
    );

    // Drive the deferred completion: the write is now durable and the
    // completion callback publishes the page->frame mapping.
    io.step().unwrap();
    assert!(completion.succeeded());

    assert_eq!(
        wal.find_frame(7, None).unwrap(),
        Some(1),
        "frame must be visible once its write is durable"
    );
}

fn page_with_pattern(page_id: i64, seed: u8, buffer_pool: &Arc<BufferPool>) -> PageRef {
    let page = allocate_new_page(page_id, buffer_pool);
    for (idx, byte) in page.get_contents().as_ptr().iter_mut().enumerate() {
        *byte = seed.wrapping_add(idx as u8).wrapping_add(page_id as u8);
    }
    page
}

#[derive(Debug)]
enum TestPageCodec {
    Xor(u8),
    ErrorEncode,
    ErrorDecode,
}

impl PageCodec for TestPageCodec {
    fn codec_id(&self) -> PageCodecId {
        let mut id = *b"wal-test-codec--";
        id[15] = match self {
            Self::Xor(mask) => *mask,
            Self::ErrorEncode => 1,
            Self::ErrorDecode => 2,
        };
        PageCodecId::new(id)
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        _context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        match self {
            Self::Xor(mask) => {
                for (input, output) in input.iter().zip(output) {
                    *output = input ^ mask;
                }
                Ok(())
            }
            Self::ErrorEncode => Err(LimboError::InternalError("codec encode failed".into())),
            Self::ErrorDecode => {
                output.copy_from_slice(input);
                Ok(())
            }
        }
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        match self {
            Self::Xor(_) => self.encode_page(context, input, output),
            Self::ErrorEncode => {
                output.copy_from_slice(input);
                Ok(())
            }
            Self::ErrorDecode => Err(LimboError::InternalError("codec decode failed".into())),
        }
    }
}

#[derive(Debug)]
struct XorFailOnPageCodec {
    mask: u8,
    fail_encode_page: Option<u32>,
    fail_decode_page: Option<u32>,
}

impl XorFailOnPageCodec {
    fn transform(&self, input: &[u8], output: &mut [u8]) {
        for (input, output) in input.iter().zip(output) {
            *output = input ^ self.mask;
        }
    }
}

impl PageCodec for XorFailOnPageCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"wal-fail-page---")
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        if self.fail_encode_page == Some(context.page_no) {
            return Err(LimboError::InternalError("codec encode failed".into()));
        }
        self.transform(input, output);
        Ok(())
    }

    fn decode_page(
        &self,
        context: PageCodecContext,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<()> {
        if self.fail_decode_page == Some(context.page_no) {
            return Err(LimboError::InternalError("codec decode failed".into()));
        }
        self.transform(input, output);
        Ok(())
    }
}

fn set_test_page_codec(wal: &WalFile, codec: Arc<dyn PageCodec>) {
    let mut io_ctx = IOContext::default();
    io_ctx.set_page_codec(codec);
    wal.set_io_context(io_ctx);
}

fn set_test_encryption(wal: &WalFile, page_size: usize) {
    let key = EncryptionKey::from_hex_string("000102030405060708090a0b0c0d0e0f").unwrap();
    let mut io_ctx = IOContext::default();
    io_ctx.set_encryption(EncryptionContext::new(CipherMode::Aes128Gcm, &key, page_size).unwrap());
    wal.set_io_context(io_ctx);
}

fn append_test_pages(
    io: &Arc<dyn IO>,
    wal: &WalFile,
    page_size: u32,
    pages: &[PageRef],
) -> Vec<Vec<u8>> {
    let prepared = wal
        .prepare_frames(pages, PageSize::new(page_size).unwrap(), Some(99), None)
        .unwrap();
    let expected = pages
        .iter()
        .map(|page| page.get_contents().as_ptr().to_vec())
        .collect::<Vec<_>>();

    let file = wal.wal_file().unwrap();
    let c = file
        .pwritev(
            prepared.offset,
            prepared.bufs.clone(),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    wal.commit_prepared_frames(&[prepared]);
    wal.finish_append_frames_commit().unwrap();
    expected
}

#[test]
fn append_frames_vectored_spill_frames_are_not_reused_by_next_prepare() {
    let page_size = 512;
    let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    let spill_page = page_with_pattern(7, 0x70, &buffer_pool);

    let completion = wal
        .append_frames_vectored(vec![spill_page], PageSize::new(page_size).unwrap())
        .unwrap();
    assert!(completion.succeeded());
    assert_eq!(wal.get_max_frame(), 1);
    assert_eq!(wal.get_max_frame_in_wal(), 0);

    let commit_page = page_with_pattern(9, 0x90, &buffer_pool);
    let prepared = wal
        .prepare_frames(
            &[commit_page],
            PageSize::new(page_size).unwrap(),
            Some(99),
            None,
        )
        .unwrap();

    assert_eq!(
        prepared.metadata[0].1, 2,
        "prepare_frames must chain after unpublished spill frames"
    );
    assert_eq!(prepared.final_max_frame, 2);
}

fn wait_for_completion_error(io: &Arc<dyn IO>, completion: Completion) -> CompletionError {
    match io.wait_for_completion(completion) {
        Err(LimboError::CompletionError(err)) => err,
        other => panic!("expected completion error, got {other:?}"),
    }
}

#[test]
fn read_frames_batch_reads_contiguous_wal_frames_directly() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    let source_pages = vec![
        page_with_pattern(2, 0x10, &buffer_pool),
        page_with_pattern(3, 0x20, &buffer_pool),
        page_with_pattern(4, 0x30, &buffer_pool),
        page_with_pattern(5, 0x40, &buffer_pool),
    ];
    let expected = append_test_pages(&io, &wal, page_size, &source_pages);

    let target_pages = vec![
        Arc::new(crate::Page::new(2)),
        Arc::new(crate::Page::new(3)),
        Arc::new(crate::Page::new(4)),
        Arc::new(crate::Page::new(5)),
    ];
    let c = wal
        .read_frames_batch(1, &target_pages, buffer_pool, None, None)
        .unwrap();
    io.wait_for_completion(c).unwrap();

    for (idx, page) in target_pages.iter().enumerate() {
        assert!(
            page.is_loaded(),
            "page {} should be loaded",
            page.get().id()
        );
        assert!(!page.is_locked(), "page {} lock leaked", page.get().id());
        assert_eq!(page.wal_tag_pair(), ((idx + 1) as u64, 0));
        assert_eq!(page.get_contents().as_ptr(), expected[idx].as_slice());
    }
}

#[test]
fn page_codec_round_trips_wal_batch_reads() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
    let source_pages = vec![
        page_with_pattern(32, 0x10, &buffer_pool),
        page_with_pattern(33, 0x20, &buffer_pool),
    ];
    let expected = append_test_pages(&io, &wal, page_size, &source_pages);

    let target_pages = vec![
        Arc::new(crate::Page::new(32)),
        Arc::new(crate::Page::new(33)),
    ];
    let c = wal
        .read_frames_batch(1, &target_pages, buffer_pool, None, None)
        .unwrap();
    io.wait_for_completion(c).unwrap();

    for (idx, page) in target_pages.iter().enumerate() {
        assert_eq!(page.get_contents().as_ptr(), expected[idx].as_slice());
    }
}

#[test]
fn page_codec_missing_wal_frame_reports_short_read() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
    let target_page = Arc::new(crate::Page::new(43));

    let completion = wal.read_frame(1, target_page, buffer_pool, None).unwrap();
    let error = wait_for_completion_error(&io, completion);

    assert!(matches!(
        error,
        CompletionError::ShortReadWalFrame {
            expected: 512,
            actual: 0,
            ..
        }
    ));
}

#[test]
fn page_codec_round_trips_vectored_wal_spill_frames() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
    let source_page = page_with_pattern(32, 0x10, &buffer_pool);
    let expected = source_page.get_contents().as_ptr().to_vec();

    let completion = wal
        .append_frames_vectored(vec![source_page], PageSize::new(page_size).unwrap())
        .unwrap();
    io.wait_for_completion(completion).unwrap();

    let target_page = Arc::new(crate::Page::new(32));
    let completion = wal
        .read_frames_batch(1, &[target_page.clone()], buffer_pool, None, None)
        .unwrap();
    io.wait_for_completion(completion).unwrap();
    assert_eq!(target_page.get_contents().as_ptr(), expected.as_slice());
}

#[test]
fn page_codec_vectored_spill_encode_error_does_not_advance_wal() {
    let page_size = 512;
    let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(
        &wal,
        Arc::new(XorFailOnPageCodec {
            mask: 0xa5,
            fail_encode_page: Some(33),
            fail_decode_page: None,
        }),
    );
    let pages = vec![
        page_with_pattern(32, 0x10, &buffer_pool),
        page_with_pattern(33, 0x20, &buffer_pool),
    ];

    let err = wal
        .append_frames_vectored(pages, PageSize::new(page_size).unwrap())
        .unwrap_err();

    assert!(err.to_string().contains("codec encode failed"));
    assert_eq!(wal.get_max_frame(), 0);
    assert_eq!(wal.find_frame(32, None).unwrap(), None);
    assert_eq!(wal.find_frame(33, None).unwrap(), None);
}

#[test]
fn page_codec_round_trips_raw_wal_frames() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
    let expected = (0..page_size).map(|i| i as u8).collect::<Vec<_>>();

    wal.write_frame_raw(buffer_pool, 1, 44, 0, &expected, FileSyncType::Fsync)
        .unwrap();

    let mut frame = vec![0; WAL_FRAME_HEADER_SIZE + page_size as usize];
    let completion = wal.read_frame_raw(1, &mut frame).unwrap();
    io.wait_for_completion(completion).unwrap();
    assert_eq!(&frame[WAL_FRAME_HEADER_SIZE..], expected.as_slice());
}

#[test]
fn page_codec_raw_wal_encode_error_does_not_advance_wal() {
    let page_size = 512;
    let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::ErrorEncode));
    let page = vec![0; page_size as usize];

    let err = wal
        .write_frame_raw(buffer_pool, 1, 44, 0, &page, FileSyncType::Fsync)
        .unwrap_err();

    assert!(err.to_string().contains("codec encode failed"));
    assert_eq!(wal.get_max_frame(), 0);
    assert_eq!(wal.find_frame(44, None).unwrap(), None);
}

#[test]
fn page_codec_raw_wal_duplicate_is_idempotent_and_detects_conflict() {
    let page_size = 512;
    let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
    let page = (0..page_size).map(|i| i as u8).collect::<Vec<_>>();

    wal.write_frame_raw(buffer_pool.clone(), 1, 44, 0, &page, FileSyncType::Fsync)
        .unwrap();
    wal.write_frame_raw(buffer_pool.clone(), 1, 44, 0, &page, FileSyncType::Fsync)
        .unwrap();

    let mut different_page = page;
    different_page[0] ^= 1;
    let err = wal
        .write_frame_raw(buffer_pool, 1, 44, 0, &different_page, FileSyncType::Fsync)
        .unwrap_err();
    assert!(matches!(err, LimboError::Conflict(_)));
    assert_eq!(wal.get_max_frame(), 1);
    assert_eq!(wal.find_frame(44, None).unwrap(), Some(1));
}

#[test]
fn page_codec_raw_wal_read_rejects_wrong_buffer_size() {
    let page_size = 512;
    let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
    let page = vec![0; page_size as usize];
    wal.write_frame_raw(buffer_pool, 1, 44, 0, &page, FileSyncType::Fsync)
        .unwrap();

    let expected_frame_len = WAL_FRAME_HEADER_SIZE + page_size as usize;
    for frame_len in [expected_frame_len - 1, expected_frame_len + 1] {
        let mut frame = vec![0; frame_len];
        match wal.read_frame_raw(1, &mut frame) {
            Err(LimboError::InvalidArgument(message)) => assert_eq!(
                message,
                format!(
                    "unexpected WAL frame buffer size: got={frame_len}, expected={expected_frame_len}"
                )
            ),
            Err(error) => panic!("expected invalid argument, got {error:?}"),
            Ok(_) => panic!("expected frame size {frame_len} to be rejected"),
        }
    }
}

#[test]
fn page_codec_encode_error_does_not_publish_wal_frames() {
    let page_size = 512;
    let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::ErrorEncode));
    let source_page = page_with_pattern(43, 0x43, &buffer_pool);

    assert!(wal
        .prepare_frames(
            &[source_page],
            PageSize::new(page_size).unwrap(),
            Some(1),
            None,
        )
        .is_err());
    assert_eq!(wal.get_max_frame(), 0);
    assert_eq!(wal.find_frame(43, None).unwrap(), None);
}

#[test]
fn encryption_round_trips_raw_wal_frames() {
    let page_size = 4096;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_encryption(&wal, page_size as usize);
    let mut expected = (0..page_size).map(|i| i as u8).collect::<Vec<_>>();
    expected[page_size as usize - 64..].fill(0);

    wal.write_frame_raw(buffer_pool, 1, 44, 0, &expected, FileSyncType::Fsync)
        .unwrap();

    let mut frame = vec![0; WAL_FRAME_HEADER_SIZE + page_size as usize];
    let completion = wal.read_frame_raw(1, &mut frame).unwrap();
    io.wait_for_completion(completion).unwrap();
    assert_eq!(&frame[WAL_FRAME_HEADER_SIZE..], expected.as_slice());
}

#[cfg(feature = "checksum")]
#[test]
fn checksum_is_applied_to_raw_wal_frames() {
    let page_size = 4096;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    let mut source = (0..page_size).map(|i| i as u8).collect::<Vec<_>>();
    source[page_size as usize - 8..].fill(0);

    wal.write_frame_raw(buffer_pool.clone(), 1, 44, 0, &source, FileSyncType::Fsync)
        .unwrap();

    let target = Arc::new(crate::Page::new(44));
    let completion = wal
        .read_frame(1, target.clone(), buffer_pool, None)
        .unwrap();
    io.wait_for_completion(completion).unwrap();
    assert_eq!(
        &target.get_contents().as_ptr()[..page_size as usize - 8],
        &source[..page_size as usize - 8]
    );
}

#[test]
fn page_codec_raw_wal_read_propagates_decode_error() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::ErrorDecode));
    let source_pages = vec![page_with_pattern(43, 0x43, &buffer_pool)];
    append_test_pages(&io, &wal, page_size, &source_pages);

    let mut frame = vec![0; WAL_FRAME_HEADER_SIZE + page_size as usize];
    let c = wal.read_frame_raw(1, &mut frame).unwrap();
    let err = wait_for_completion_error(&io, c);

    assert!(matches!(
        err,
        CompletionError::PageCodecError { page_idx: 43 }
    ));
}

#[test]
fn page_codec_wal_batch_read_reports_codec_error() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::ErrorDecode));
    let source_pages = vec![page_with_pattern(43, 0x43, &buffer_pool)];
    append_test_pages(&io, &wal, page_size, &source_pages);

    let target_page = Arc::new(crate::Page::new(43));
    let c = wal
        .read_frames_batch(1, &[target_page], buffer_pool, None, None)
        .unwrap();
    let err = wait_for_completion_error(&io, c);

    assert!(matches!(
        err,
        CompletionError::PageCodecError { page_idx: 43 }
    ));
}

#[test]
fn page_codec_wal_batch_decode_failure_does_not_publish_earlier_pages() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
    let source_pages = vec![
        page_with_pattern(32, 0x10, &buffer_pool),
        page_with_pattern(33, 0x20, &buffer_pool),
        page_with_pattern(34, 0x30, &buffer_pool),
    ];
    append_test_pages(&io, &wal, page_size, &source_pages);
    set_test_page_codec(
        &wal,
        Arc::new(XorFailOnPageCodec {
            mask: 0xa5,
            fail_encode_page: None,
            fail_decode_page: Some(33),
        }),
    );
    let target_pages = vec![
        Arc::new(crate::Page::new(32)),
        Arc::new(crate::Page::new(33)),
        Arc::new(crate::Page::new(34)),
    ];

    let completion = wal
        .read_frames_batch(1, &target_pages, buffer_pool, None, None)
        .unwrap();
    let err = wait_for_completion_error(&io, completion);

    assert!(matches!(
        err,
        CompletionError::PageCodecError { page_idx: 33 }
    ));
    for page in target_pages {
        assert!(!page.is_locked());
        assert!(!page.is_loaded());
        assert!(!page.has_wal_tag());
    }
}

#[test]
fn read_frames_batch_can_start_from_middle_frame() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    let source_pages = vec![
        page_with_pattern(10, 0x01, &buffer_pool),
        page_with_pattern(11, 0x02, &buffer_pool),
        page_with_pattern(12, 0x03, &buffer_pool),
        page_with_pattern(13, 0x04, &buffer_pool),
    ];
    let expected = append_test_pages(&io, &wal, page_size, &source_pages);

    let target_pages = vec![
        Arc::new(crate::Page::new(11)),
        Arc::new(crate::Page::new(12)),
        Arc::new(crate::Page::new(13)),
    ];
    let c = wal
        .read_frames_batch(2, &target_pages, buffer_pool, None, None)
        .unwrap();
    io.wait_for_completion(c).unwrap();

    for (idx, page) in target_pages.iter().enumerate() {
        assert!(
            page.is_loaded(),
            "page {} should be loaded",
            page.get().id()
        );
        assert!(!page.is_locked(), "page {} lock leaked", page.get().id());
        assert_eq!(page.wal_tag_pair(), ((idx + 2) as u64, 0));
        assert_eq!(page.get_contents().as_ptr(), expected[idx + 1].as_slice());
    }
}

#[test]
fn read_frames_batch_follows_physical_frame_order_not_page_id_order() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    let source_pages = vec![
        page_with_pattern(7, 0x71, &buffer_pool),
        page_with_pattern(2, 0x22, &buffer_pool),
        page_with_pattern(5, 0x55, &buffer_pool),
        page_with_pattern(9, 0x99, &buffer_pool),
    ];
    let expected = append_test_pages(&io, &wal, page_size, &source_pages);

    let target_pages = vec![
        Arc::new(crate::Page::new(7)),
        Arc::new(crate::Page::new(2)),
        Arc::new(crate::Page::new(5)),
        Arc::new(crate::Page::new(9)),
    ];
    let c = wal
        .read_frames_batch(1, &target_pages, buffer_pool, None, None)
        .unwrap();
    io.wait_for_completion(c).unwrap();

    for (idx, page) in target_pages.iter().enumerate() {
        assert_eq!(
            page.get_contents().as_ptr(),
            expected[idx].as_slice(),
            "frame-order read should preserve page {} contents",
            page.get().id()
        );
        assert_eq!(page.wal_tag_pair(), ((idx + 1) as u64, 0));
    }
}

#[test]
fn read_frames_batch_short_read_errors_and_clears_page_locks() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    let source_pages = vec![
        page_with_pattern(20, 0x20, &buffer_pool),
        page_with_pattern(21, 0x21, &buffer_pool),
    ];
    append_test_pages(&io, &wal, page_size, &source_pages);

    let target_pages = vec![
        Arc::new(crate::Page::new(20)),
        Arc::new(crate::Page::new(21)),
        Arc::new(crate::Page::new(22)),
    ];
    let c = wal
        .read_frames_batch(1, &target_pages, buffer_pool, None, None)
        .unwrap();
    let err = wait_for_completion_error(&io, c);

    assert!(
        matches!(err, CompletionError::ShortReadWalFrame { .. }),
        "unexpected error: {err:?}"
    );
    for page in &target_pages {
        assert!(!page.is_locked(), "page {} lock leaked", page.get().id());
        assert!(
            !page.is_loaded(),
            "page {} should not be loaded",
            page.get().id()
        );
        assert!(
            !page.has_wal_tag(),
            "page {} should not be tagged",
            page.get().id()
        );
    }
}

#[test]
fn read_frames_batch_page_number_mismatch_returns_error_not_panic() {
    let page_size = 512;
    let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
    let source_pages = vec![
        page_with_pattern(30, 0x30, &buffer_pool),
        page_with_pattern(31, 0x31, &buffer_pool),
    ];
    append_test_pages(&io, &wal, page_size, &source_pages);

    let target_pages = vec![
        Arc::new(crate::Page::new(30)),
        Arc::new(crate::Page::new(99)),
    ];
    let c = wal
        .read_frames_batch(1, &target_pages, buffer_pool, None, None)
        .unwrap();
    let err = wait_for_completion_error(&io, c);

    assert!(
        matches!(
            err,
            CompletionError::WalFramePageMismatch {
                frame_id: 2,
                expected: 99,
                actual: 31
            }
        ),
        "unexpected error: {err:?}"
    );
    for page in &target_pages {
        assert!(!page.is_locked(), "page {} lock leaked", page.get().id());
        assert!(
            !page.is_loaded(),
            "page {} should not be loaded",
            page.get().id()
        );
        assert!(
            !page.has_wal_tag(),
            "page {} should not be tagged",
            page.get().id()
        );
        assert!(
            page.get().buffer().is_none(),
            "page {} should not retain a buffer",
            page.get().id()
        );
    }
}

fn set_shared_snapshot(shared: &Arc<RwLock<WalFileShared>>, snapshot: WalSnapshot) {
    let mut guard = shared.write();
    guard
        .metadata
        .max_frame
        .store(snapshot.max_frame, Ordering::Release);
    guard
        .metadata
        .nbackfills
        .store(snapshot.nbackfills, Ordering::Release);
    guard.metadata.last_checksum = snapshot.last_checksum;
    guard.metadata.wal_header.lock().checkpoint_seq = snapshot.checkpoint_seq;
    guard
        .metadata
        .transaction_count
        .store(snapshot.transaction_count, Ordering::Release);
}

fn make_test_coordination(shared: &Arc<RwLock<WalFileShared>>) -> InProcessWalCoordination {
    InProcessWalCoordination::new(shared.clone())
}

#[cfg(host_shared_wal)]
fn make_test_shm_coordination(
    shared: &Arc<RwLock<WalFileShared>>,
    path: &std::path::Path,
) -> (Arc<MappedSharedWalCoordination>, ShmWalCoordination) {
    let io = shared_wal_test_io();
    let authority = Arc::new(MappedSharedWalCoordination::create_or_open(&io, path, 64).unwrap());
    let coordination = ShmWalCoordination::new(shared.clone(), authority.clone());
    (authority, coordination)
}

#[cfg(host_shared_wal)]
fn active_shared_reader_slot_count(authority: &MappedSharedWalCoordination) -> usize {
    let reader_slot_count = authority.snapshot().reader_slot_count;
    (0..reader_slot_count)
        .filter(|&slot_index| authority.reader_owner(slot_index).is_some())
        .count()
}

#[cfg(host_shared_wal)]
fn write_test_wal_with_single_commit_frame(
    io: &Arc<dyn IO>,
    wal_path: &std::path::Path,
) -> SharedWalCoordinationHeader {
    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let mut wal_header = sqlite3_ondisk::WalHeader {
        page_size: 4096,
        checkpoint_seq: 5,
        salt_1: 17,
        salt_2: 23,
        ..sqlite3_ondisk::WalHeader::new()
    };
    let use_native_endian = cfg!(target_endian = "big") == ((wal_header.magic & 1) != 0);
    let mut header_prefix = [0u8; WAL_HEADER_SIZE - 8];
    header_prefix[0..4].copy_from_slice(&wal_header.magic.to_be_bytes());
    header_prefix[4..8].copy_from_slice(&wal_header.file_format.to_be_bytes());
    header_prefix[8..12].copy_from_slice(&wal_header.page_size.to_be_bytes());
    header_prefix[12..16].copy_from_slice(&wal_header.checkpoint_seq.to_be_bytes());
    header_prefix[16..20].copy_from_slice(&wal_header.salt_1.to_be_bytes());
    header_prefix[20..24].copy_from_slice(&wal_header.salt_2.to_be_bytes());
    let header_checksum =
        sqlite3_ondisk::checksum_wal(&header_prefix, &wal_header, (0, 0), use_native_endian);
    wal_header.checksum_1 = header_checksum.0;
    wal_header.checksum_2 = header_checksum.1;

    io.wait_for_completion(
        sqlite3_ondisk::begin_write_wal_header(file.as_ref(), &wal_header, None).unwrap(),
    )
    .unwrap();

    let buffer_pool = BufferPool::begin_init(io, BufferPool::TEST_ARENA_SIZE);
    buffer_pool
        .finalize_with_page_size(wal_header.page_size as usize)
        .unwrap();
    #[allow(unused_mut)]
    let mut page = vec![0x5a; wal_header.page_size as usize];
    #[cfg(feature = "checksum")]
    crate::storage::checksum::ChecksumContext::new()
        .add_checksum_to_page(&mut page, 7)
        .unwrap();
    let (frame_checksum, frame_buf) = sqlite3_ondisk::prepare_wal_frame(
        &buffer_pool,
        &wal_header,
        header_checksum,
        wal_header.page_size,
        7,
        1,
        &page,
    );
    let c = file
        .pwrite(
            WAL_HEADER_SIZE as u64,
            frame_buf,
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let c = file
        .sync(Completion::new_sync(|_| {}), crate::io::FileSyncType::Fsync)
        .unwrap();
    io.wait_for_completion(c).unwrap();

    SharedWalCoordinationHeader {
        max_frame: 1,
        nbackfills: 0,
        transaction_count: 9,
        visibility_generation: 3,
        checkpoint_seq: wal_header.checkpoint_seq,
        checkpoint_epoch: 7,
        page_size: wal_header.page_size,
        salt_1: wal_header.salt_1,
        salt_2: wal_header.salt_2,
        checksum_1: frame_checksum.0,
        checksum_2: frame_checksum.1,
        reader_slot_count: 64,
    }
}

#[cfg(host_shared_wal)]
fn open_test_db_file_for_wal(
    io: &Arc<dyn IO>,
    wal_path: &std::path::Path,
) -> Arc<dyn DatabaseStorage> {
    let db_path = wal_path.with_extension("db");
    Arc::new(DatabaseFile::new(
        io.open_file(db_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap(),
    ))
}

#[test]
#[cfg(host_shared_wal)]
fn test_read_frame_keeps_epoch_from_issue_time() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("epoch-race.db-wal");
    let io = shared_wal_test_io();
    let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);

    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file.clone()).unwrap();
    let deferred_file = Arc::new(DeferredReadFile::new(file));
    {
        let mut shared = shared.write();
        shared.runtime.file = Some(deferred_file.clone());
        shared
            .runtime
            .epoch
            .store(snapshot.checkpoint_epoch, Ordering::Release);
    }

    let coordination: Arc<dyn WalCoordination> = Arc::new(make_test_coordination(&shared));
    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    buffer_pool
        .finalize_with_page_size(snapshot.page_size as usize)
        .unwrap();
    let wal = WalFile::new_with_coordination(
        io.clone(),
        coordination,
        (
            (snapshot.checksum_1, snapshot.checksum_2),
            snapshot.max_frame,
        ),
        buffer_pool.clone(),
    );

    let page = Arc::new(crate::storage::pager::Page::new(7));
    let issued_epoch = wal.coordination.checkpoint_epoch();
    let completion = wal.read_frame(1, page.clone(), buffer_pool, None).unwrap();

    wal.increment_checkpoint_epoch();
    deferred_file.complete_pending_reads();
    io.wait_for_completion(completion).unwrap();

    assert_eq!(
        page.wal_tag_pair(),
        (1, issued_epoch),
        "WAL reads must retain the epoch from when the read was issued"
    );
}

#[cfg(test)]
fn read_slots_with_readers(shared: &WalFileShared) -> Vec<usize> {
    shared
        .runtime
        .read_locks
        .iter()
        .enumerate()
        .filter_map(|(slot, lock)| {
            let state = lock.0.load(Ordering::Acquire);
            let has_readers = (state & super::TursoRwLock::READER_COUNT_MASK) != 0;
            has_readers.then_some(slot)
        })
        .collect()
}

fn wal_header_snapshot(shared: &Arc<RwLock<WalFileShared>>) -> (u32, u32, u32, u32) {
    // (checkpoint_seq, salt1, salt2, page_size)
    let shared_guard = shared.read();
    let hdr = shared_guard.metadata.wal_header.lock();
    (hdr.checkpoint_seq, hdr.salt_1, hdr.salt_2, hdr.page_size)
}

#[test]
fn test_wal_connection_state_round_trip() {
    let (_shared, wal) = make_test_wal();
    let state = WalConnectionState::new(
        WalSnapshot {
            max_frame: 11,
            nbackfills: 7,
            last_checksum: (31, 47),
            checkpoint_seq: 5,
            transaction_count: 13,
        },
        ReadGuardKind::ReadMark(NonZeroUsize::new(3).unwrap()),
    );

    wal.install_connection_state(state);

    assert_eq!(wal.connection_state(), state);
    assert_eq!(wal.connection_state().snapshot.min_frame(), 8);
}

#[test]
fn test_wal_explicit_backend_constructor_does_not_keep_shared_handle() {
    let io = shared_wal_test_io();
    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    let shared = WalFileShared::new_noop();
    let coordination: Arc<dyn WalCoordination> =
        Arc::new(InProcessWalCoordination::new(shared.clone()));

    assert_eq!(Arc::strong_count(&shared), 2);

    let _wal = WalFile::new_with_coordination(io, coordination, ((0, 0), 0), buffer_pool);

    assert_eq!(Arc::strong_count(&shared), 2);
}

#[test]
fn test_mvcc_refresh_updates_snapshot_only_when_no_read_guard_is_held() {
    let (shared, wal) = make_test_wal();
    let initial = WalSnapshot {
        max_frame: 4,
        nbackfills: 2,
        last_checksum: (9, 10),
        checkpoint_seq: 1,
        transaction_count: 3,
    };
    set_shared_snapshot(&shared, initial);
    wal.install_connection_state(WalConnectionState::new(
        initial,
        ReadGuardKind::ReadMark(NonZeroUsize::new(2).unwrap()),
    ));

    assert!(!wal.mvcc_refresh_if_db_changed());

    let updated = WalSnapshot {
        max_frame: 8,
        nbackfills: 5,
        last_checksum: (21, 34),
        checkpoint_seq: 7,
        transaction_count: 4,
    };
    set_shared_snapshot(&shared, updated);

    // A held read guard pins this connection's WAL view; the refresh must not
    // advance it past the snapshot an active transaction captured.
    assert!(!wal.mvcc_refresh_if_db_changed());
    assert_eq!(
        wal.connection_state(),
        WalConnectionState::new(
            initial,
            ReadGuardKind::ReadMark(NonZeroUsize::new(2).unwrap())
        )
    );

    // With no read guard held the refresh picks up the new shared snapshot.
    wal.install_connection_state(WalConnectionState::new(initial, ReadGuardKind::None));
    assert!(wal.mvcc_refresh_if_db_changed());
    assert_eq!(
        wal.connection_state(),
        WalConnectionState::new(updated, ReadGuardKind::None)
    );
}

#[test]
fn test_in_process_coordination_uses_shared_authority() {
    let (shared, _wal) = make_test_wal();
    let coordination = make_test_coordination(&shared);
    let snapshot = WalSnapshot {
        max_frame: 9,
        nbackfills: 3,
        last_checksum: (55, 89),
        checkpoint_seq: 7,
        transaction_count: 11,
    };
    set_shared_snapshot(&shared, snapshot);
    {
        let guard = shared.write();
        guard.runtime.epoch.store(5, Ordering::Release);
        guard.runtime.frame_cache.lock().extend([
            (1, vec![1, 4, 8]),
            (2, vec![2, 6]),
            (3, vec![3]),
        ]);
    }

    assert_eq!(coordination.load_snapshot(), snapshot);
    assert_eq!(coordination.checkpoint_epoch(), 5);
    assert_eq!(coordination.find_frame(1, 4, 9, None), Some(8));
    assert_eq!(coordination.find_frame(2, 4, 9, Some(5)), Some(2));
    assert_eq!(coordination.iter_latest_frames(4, 9), vec![(1, 8), (2, 6)]);

    coordination.publish_commit(WalCommitState {
        max_frame: 12,
        last_checksum: (144, 233),
        transaction_count: 12,
    });
    let published = coordination.load_snapshot();
    assert_eq!(published.max_frame, 12);
    assert_eq!(published.last_checksum, (144, 233));
    assert_eq!(published.transaction_count, 12);
    assert_eq!(published.nbackfills, snapshot.nbackfills);
    assert_eq!(published.checkpoint_seq, snapshot.checkpoint_seq);
}

#[test]
fn test_in_process_coordination_publishes_checkpoint_and_restart_state() {
    let (shared, _wal) = make_test_wal();
    let coordination = make_test_coordination(&shared);
    let io = PlatformIO::new().unwrap();
    let snapshot = WalSnapshot {
        max_frame: 9,
        nbackfills: 3,
        last_checksum: (55, 89),
        checkpoint_seq: 7,
        transaction_count: 11,
    };
    set_shared_snapshot(&shared, snapshot);
    {
        let guard = shared.write();
        let mut header = guard.metadata.wal_header.lock();
        header.page_size = 4096;
        header.checksum_1 = 144;
        header.checksum_2 = 233;
        guard.metadata.initialized.store(true, Ordering::Release);
        guard.runtime.epoch.store(5, Ordering::Release);
        guard
            .runtime
            .frame_cache
            .lock()
            .extend([(1, vec![1, 4, 8]), (2, vec![2, 6])]);
    }

    coordination.publish_backfill(8);
    assert_eq!(coordination.load_snapshot().nbackfills, 8);
    assert_eq!(coordination.bump_checkpoint_epoch(), 5);
    assert_eq!(coordination.checkpoint_epoch(), 6);

    assert!(coordination.try_read_mark_exclusive(0));
    let restarted = coordination.begin_restart(&io).unwrap();
    coordination.end_restart();
    coordination.unlock_read_mark(0);

    assert_eq!(restarted.max_frame, 0);
    assert_eq!(restarted.nbackfills, 0);
    assert_eq!(restarted.last_checksum, (144, 233));
    assert_eq!(restarted.checkpoint_seq, 8);
    assert_eq!(restarted.transaction_count, 11);

    let guard = shared.read();
    assert_eq!(guard.runtime.read_locks[0].get_value(), 0);
    assert_eq!(guard.runtime.read_locks[1].get_value(), 0);
    for lock in &guard.runtime.read_locks[2..] {
        assert_eq!(lock.get_value(), READMARK_NOT_USED);
    }
    assert!(guard.runtime.frame_cache.lock().is_empty());
    assert!(!guard.metadata.initialized.load(Ordering::Acquire));
}

#[test]
fn test_in_process_coordination_manages_frame_cache() {
    let (shared, _wal) = make_test_wal();
    let coordination = make_test_coordination(&shared);

    // Frames are cached in WAL append order (globally ascending): page 7 at
    // frame 2, page 9 at frame 4, page 7 again at frame 5.
    coordination.cache_frame(7, 2);
    coordination.cache_frame(9, 4);
    coordination.cache_frame(7, 5);

    assert_eq!(coordination.find_frame(7, 0, 5, None), Some(5));
    assert_eq!(coordination.iter_latest_frames(0, 5), vec![(7, 5), (9, 4)]);

    coordination.rollback_cache(4);

    assert_eq!(coordination.find_frame(7, 0, 5, None), Some(2));
    assert_eq!(coordination.iter_latest_frames(0, 5), vec![(7, 2), (9, 4)]);
    assert_eq!(
        shared.read().runtime.frame_cache.lock().get(&7),
        Some(&vec![2])
    );
}

/// Regression test for WAL frame-index aliasing corruption: when a WAL
/// frame slot is reused for a different page (the append position rewinds
/// to an already-cached frame — e.g. an aborted/uncommitted append's slots
/// being overwritten, or a different connection reusing the slots), the
/// stale `page -> frame` mapping for that slot must be purged. Otherwise
/// `find_frame` can hand a page a frame number whose slot now physically
/// holds a different page, and the reader gets the wrong page's bytes
/// (surfacing as "non-index page" / "Invalid page type" / corruption).
#[test]
fn cache_frame_purges_stale_mapping_on_frame_slot_reuse() {
    let (shared, _wal) = make_test_wal();
    let coordination = make_test_coordination(&shared);

    // Ascending append: page 7 @3, page 9 @4, page 7 @5.
    coordination.cache_frame(7, 3);
    coordination.cache_frame(9, 4);
    coordination.cache_frame(7, 5);
    assert_eq!(coordination.find_frame(9, 0, 10, None), Some(4));

    // The append position rewinds and frame slots 4 and 5 are overwritten,
    // now belonging to page 11 (@4) and page 13 (@5). The earlier owners of
    // those slots (page 9 @4, page 7 @5) must no longer be reachable.
    coordination.cache_frame(11, 4);
    coordination.cache_frame(13, 5);

    assert_eq!(
        coordination.find_frame(9, 0, 10, None),
        None,
        "stale page 9 -> frame 4 mapping must be purged once slot 4 is reused"
    );
    assert_eq!(
        coordination.find_frame(11, 0, 10, None),
        Some(4),
        "page 11 now owns frame slot 4"
    );
    assert_eq!(
        coordination.find_frame(13, 0, 10, None),
        Some(5),
        "page 13 now owns frame slot 5"
    );
    // Page 7's still-valid lower frame (3) survives; its stale 5 is gone.
    assert_eq!(coordination.find_frame(7, 0, 10, None), Some(3));
}

#[test]
fn test_savepoint_rollback_discards_frame_cache_past_rollback_point() {
    let (shared, wal) = make_test_wal();
    let coordination = make_test_coordination(&shared);
    set_shared_snapshot(
        &shared,
        WalSnapshot {
            max_frame: 25,
            nbackfills: 0,
            last_checksum: (55, 89),
            checkpoint_seq: 1,
            transaction_count: 3,
        },
    );

    // The connection spilled uncommitted frames past the committed
    // high-water mark (25); a savepoint opened mid-transaction recorded
    // frame 27.
    coordination.cache_frame(7, 10);
    coordination.cache_frame(9, 26);
    coordination.cache_frame(11, 28);
    wal.max_frame.store(30, Ordering::Release);

    wal.rollback(Some(RollbackTo {
        frame: 27,
        checksum: (13, 21),
        checkpoint_seq: 1,
    }));

    // Mappings at or below the rollback point survive, later ones are
    // discarded; frames 26..=27 remain as unpublished spills.
    assert_eq!(coordination.find_frame(7, 0, 30, None), Some(10));
    assert_eq!(coordination.find_frame(9, 0, 30, None), Some(26));
    assert_eq!(coordination.find_frame(11, 0, 30, None), None);
    assert_eq!(wal.get_max_frame(), 27);
    assert_eq!(wal.last_checksum(), (13, 21));

    // Rolling back to the committed high-water mark discards every
    // spill.
    wal.rollback(Some(RollbackTo {
        frame: 25,
        checksum: (55, 89),
        checkpoint_seq: 1,
    }));
    assert_eq!(coordination.find_frame(9, 0, 30, None), None);
    assert_eq!(wal.get_max_frame(), 25);
}

#[test]
fn test_in_process_coordination_transaction_guards() {
    let (shared, _wal) = make_test_wal();
    let coordination = make_test_coordination(&shared);

    let db_file_snapshot = WalSnapshot {
        max_frame: 0,
        nbackfills: 0,
        last_checksum: (0, 0),
        checkpoint_seq: 0,
        transaction_count: 0,
    };
    set_shared_snapshot(&shared, db_file_snapshot);
    let read_guard = coordination.try_begin_read_tx(db_file_snapshot).unwrap();
    assert_eq!(read_guard, ReadGuardKind::DbFile);
    coordination.end_read_tx(read_guard);

    let wal_snapshot = WalSnapshot {
        max_frame: 5,
        nbackfills: 2,
        last_checksum: (11, 13),
        checkpoint_seq: 1,
        transaction_count: 2,
    };
    set_shared_snapshot(&shared, wal_snapshot);
    let read_guard = coordination.try_begin_read_tx(wal_snapshot).unwrap();
    assert!(matches!(read_guard, ReadGuardKind::ReadMark(_)));
    coordination.end_read_tx(read_guard);

    assert!(coordination.try_begin_write_tx());
    assert!(!coordination.try_begin_write_tx());
    coordination.end_write_tx();
}

#[cfg(host_shared_wal)]
#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "experimental_win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shm_coordination_uses_shared_authority() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();
    let file_a = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let file_b = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_a = WalFileShared::new_shared(file_a).unwrap();
    let shared_b = WalFileShared::new_shared(file_b).unwrap();
    let snapshot = WalSnapshot {
        max_frame: 14,
        nbackfills: 8,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: 9,
    };
    set_shared_snapshot(&shared_a, snapshot);
    {
        let shared = shared_a.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = snapshot.last_checksum.0;
        header.checksum_2 = snapshot.last_checksum.1;
    }

    let (_authority_a, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
    let (authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);
    coordination_a.cache_frame(7, 2);
    coordination_a.cache_frame(9, 4);
    coordination_a.cache_frame(7, 5);

    assert_eq!(coordination_b.load_snapshot(), snapshot);
    assert_eq!(coordination_b.wal_header().page_size, 4096);
    assert_eq!(coordination_b.wal_header().salt_1, 17);
    assert_eq!(coordination_b.wal_header().salt_2, 23);
    assert_eq!(coordination_b.find_frame(7, 0, 5, None), Some(5));
    assert_eq!(
        coordination_b.iter_latest_frames(0, 5),
        vec![(7, 5), (9, 4)]
    );
    assert_eq!(coordination_a.checkpoint_epoch(), 0);
    assert_eq!(coordination_b.bump_checkpoint_epoch(), 0);
    assert_eq!(coordination_a.checkpoint_epoch(), 1);

    assert!(coordination_a.try_begin_write_tx());
    assert!(!coordination_b.try_begin_write_tx());
    coordination_a.end_write_tx();

    let read_guard = coordination_a.try_begin_read_tx(snapshot).unwrap();
    assert_eq!(
        authority_b.min_active_reader_frame(),
        Some(snapshot.max_frame)
    );
    coordination_a.end_read_tx(read_guard);
    assert_eq!(authority_b.min_active_reader_frame(), None);

    coordination_b.publish_commit(WalCommitState {
        max_frame: 21,
        last_checksum: (55, 89),
        transaction_count: 10,
    });
    assert_eq!(
        coordination_a.load_snapshot(),
        WalSnapshot {
            max_frame: 21,
            nbackfills: 8,
            last_checksum: (55, 89),
            checkpoint_seq: 5,
            transaction_count: 10,
        }
    );

    coordination_b.rollback_cache(4);
    assert_eq!(coordination_a.find_frame(7, 0, 5, None), Some(2));
    assert_eq!(
        coordination_a.iter_latest_frames(0, 5),
        vec![(7, 2), (9, 4)]
    );
    assert!(shm_path.exists());
}

#[cfg(host_shared_wal)]
#[test]
fn test_shm_coordination_many_same_snapshot_readers_share_one_published_slot() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-many-same-snapshot-readers.db-wal");
    let shm_path = dir.path().join("test-many-same-snapshot-readers.db-tshm");
    let io = shared_wal_test_io();
    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    let snapshot = WalSnapshot {
        max_frame: 9,
        nbackfills: 2,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: 9,
    };
    set_shared_snapshot(&shared, snapshot);
    {
        let shared = shared.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = snapshot.last_checksum.0;
        header.checksum_2 = snapshot.last_checksum.1;
    }

    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    let mut readers = Vec::new();
    for _ in 0..128 {
        let coordination = ShmWalCoordination::new(shared.clone(), authority.clone());
        let read_guard = coordination
            .try_begin_read_tx(snapshot)
            .expect("same-snapshot readers should share a published reader barrier");
        readers.push((coordination, read_guard));
    }

    assert_eq!(
        authority.min_active_reader_frame(),
        Some(snapshot.max_frame)
    );
    assert_eq!(
        active_shared_reader_slot_count(&authority),
        1,
        "same-snapshot readers should collapse onto one shared reader slot"
    );

    for (coordination, read_guard) in readers {
        coordination.end_read_tx(read_guard);
    }
    assert_eq!(authority.min_active_reader_frame(), None);
    assert_eq!(active_shared_reader_slot_count(&authority), 0);
}

#[cfg(host_shared_wal)]
#[test]
fn test_shm_coordination_uses_one_published_slot_per_active_snapshot_generation() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-mixed-snapshot-readers.db-wal");
    let shm_path = dir.path().join("test-mixed-snapshot-readers.db-tshm");
    let io = shared_wal_test_io();
    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    let snapshot_a = WalSnapshot {
        max_frame: 5,
        nbackfills: 2,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: 9,
    };
    set_shared_snapshot(&shared, snapshot_a);
    {
        let shared = shared.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = snapshot_a.last_checksum.0;
        header.checksum_2 = snapshot_a.last_checksum.1;
    }

    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    let reader_a1 = ShmWalCoordination::new(shared.clone(), authority.clone());
    let guard_a1 = reader_a1.try_begin_read_tx(snapshot_a).unwrap();
    let reader_a2 = ShmWalCoordination::new(shared.clone(), authority.clone());
    let guard_a2 = reader_a2.try_begin_read_tx(snapshot_a).unwrap();
    assert_eq!(
        authority.min_active_reader_frame(),
        Some(snapshot_a.max_frame)
    );
    assert_eq!(active_shared_reader_slot_count(&authority), 1);

    let snapshot_b = WalSnapshot {
        max_frame: 9,
        nbackfills: 2,
        last_checksum: (41, 43),
        checkpoint_seq: 5,
        transaction_count: 10,
    };
    reader_a1.publish_commit(WalCommitState {
        max_frame: snapshot_b.max_frame,
        last_checksum: snapshot_b.last_checksum,
        transaction_count: snapshot_b.transaction_count,
    });

    let reader_b1 = ShmWalCoordination::new(shared.clone(), authority.clone());
    let guard_b1 = reader_b1.try_begin_read_tx(snapshot_b).unwrap();
    let reader_b2 = ShmWalCoordination::new(shared, authority.clone());
    let guard_b2 = reader_b2.try_begin_read_tx(snapshot_b).unwrap();

    assert_eq!(
        active_shared_reader_slot_count(&authority),
        2,
        "distinct live snapshots should each publish one shared reader slot"
    );
    assert_eq!(
        authority.min_active_reader_frame(),
        Some(snapshot_a.max_frame),
        "checkpoint barrier should stay pinned to the oldest active snapshot"
    );

    reader_a1.end_read_tx(guard_a1);
    reader_a2.end_read_tx(guard_a2);
    assert_eq!(
        authority.min_active_reader_frame(),
        Some(snapshot_b.max_frame)
    );
    assert_eq!(active_shared_reader_slot_count(&authority), 1);

    reader_b1.end_read_tx(guard_b1);
    reader_b2.end_read_tx(guard_b2);
    assert_eq!(authority.min_active_reader_frame(), None);
    assert_eq!(active_shared_reader_slot_count(&authority), 0);
}

#[cfg(host_shared_wal)]
#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "experimental_win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shm_coordination_shared_index_grows_past_old_fixed_limit() {
    const OLD_FIXED_LIMIT: u64 = 65_536;

    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();
    let file_a = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let file_b = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_a = WalFileShared::new_shared(file_a).unwrap();
    let shared_b = WalFileShared::new_shared(file_b).unwrap();
    let snapshot = WalSnapshot {
        max_frame: OLD_FIXED_LIMIT + 2,
        nbackfills: 0,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: OLD_FIXED_LIMIT + 2,
    };
    set_shared_snapshot(&shared_a, snapshot);
    {
        let shared = shared_a.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = snapshot.last_checksum.0;
        header.checksum_2 = snapshot.last_checksum.1;
    }

    let (_authority_a, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
    let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);

    coordination_a.cache_frame(7, 2);
    for frame_id in 3..=OLD_FIXED_LIMIT + 1 {
        coordination_a.cache_frame(100 + (frame_id % 31), frame_id);
    }
    coordination_a.cache_frame(7, OLD_FIXED_LIMIT + 2);

    assert_eq!(
        coordination_b.find_frame(7, 0, OLD_FIXED_LIMIT + 2, None),
        Some(OLD_FIXED_LIMIT + 2)
    );
    assert_eq!(
        coordination_b.find_frame(7, 0, OLD_FIXED_LIMIT + 2, Some(OLD_FIXED_LIMIT + 1)),
        Some(2)
    );
    assert!(coordination_b
        .iter_latest_frames(0, OLD_FIXED_LIMIT + 2)
        .contains(&(7, OLD_FIXED_LIMIT + 2)));
}

#[cfg(host_shared_wal)]
#[test]
fn test_shm_coordination_restart_uses_authority_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = PlatformIO::new().unwrap();
    let file_a = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let file_b = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_a = WalFileShared::new_shared(file_a).unwrap();
    let shared_b = WalFileShared::new_shared(file_b).unwrap();
    let snapshot = WalSnapshot {
        max_frame: 12,
        nbackfills: 12,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: 9,
    };
    set_shared_snapshot(&shared_a, snapshot);
    {
        let shared = shared_a.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = snapshot.last_checksum.0;
        header.checksum_2 = snapshot.last_checksum.1;
        shared.metadata.initialized.store(true, Ordering::Release);
        shared.runtime.epoch.store(5, Ordering::Release);
    }

    let (_authority_a, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
    let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);
    coordination_a.cache_frame(7, 2);
    coordination_a.cache_frame(9, 4);

    {
        let mut shared = shared_b.write();
        shared.metadata.max_frame.store(99, Ordering::Release);
        shared.metadata.nbackfills.store(77, Ordering::Release);
        shared.metadata.last_checksum = (1, 2);
        shared
            .metadata
            .transaction_count
            .store(42, Ordering::Release);
        shared.runtime.epoch.store(99, Ordering::Release);
        shared.metadata.initialized.store(true, Ordering::Release);
        let mut header = shared.metadata.wal_header.lock();
        header.checkpoint_seq = 88;
        header.page_size = 2048;
        header.salt_1 = 91;
        header.salt_2 = 92;
        header.checksum_1 = 93;
        header.checksum_2 = 94;
    }

    assert!(coordination_b.fallback.try_read_mark_exclusive(0));
    let restarted = coordination_b.begin_restart(&io).unwrap();
    coordination_b.end_restart();
    coordination_b.fallback.unlock_read_mark(0);

    assert_eq!(
        restarted,
        WalSnapshot {
            max_frame: 0,
            nbackfills: 0,
            last_checksum: snapshot.last_checksum,
            checkpoint_seq: snapshot.checkpoint_seq.wrapping_add(1),
            transaction_count: snapshot.transaction_count,
        }
    );
    assert_eq!(
        coordination_a.load_snapshot(),
        WalSnapshot {
            max_frame: 0,
            nbackfills: 0,
            last_checksum: snapshot.last_checksum,
            checkpoint_seq: snapshot.checkpoint_seq.wrapping_add(1),
            transaction_count: snapshot.transaction_count,
        }
    );
    let header = coordination_a.wal_header();
    assert_eq!(header.page_size, 4096);
    assert_eq!(
        header.checkpoint_seq,
        snapshot.checkpoint_seq.wrapping_add(1)
    );
    assert_eq!(header.salt_1, 18);
    assert_ne!(header.salt_2, 23);
    assert_eq!(header.checksum_1, snapshot.last_checksum.0);
    assert_eq!(header.checksum_2, snapshot.last_checksum.1);
    assert_eq!(coordination_a.iter_latest_frames(0, u64::MAX), Vec::new());
    assert_eq!(coordination_a.checkpoint_epoch(), 5);

    let shared = shared_b.read();
    assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 0);
    assert_eq!(shared.metadata.nbackfills.load(Ordering::Acquire), 0);
    assert_eq!(shared.metadata.last_checksum, snapshot.last_checksum);
    assert_eq!(
        shared.metadata.transaction_count.load(Ordering::Acquire),
        snapshot.transaction_count
    );
    assert_eq!(shared.runtime.epoch.load(Ordering::Acquire), 5);
    assert!(!shared.metadata.initialized.load(Ordering::Acquire));
    assert!(shared.runtime.frame_cache.lock().is_empty());
}

#[cfg(host_shared_wal)]
#[test]
fn test_shm_coordination_exclusive_reopen_reuses_persisted_authority() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    {
        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        let snapshot = WalSnapshot {
            max_frame: 12,
            nbackfills: 8,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared, snapshot);
        {
            let shared = shared.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = snapshot.last_checksum.0;
            header.checksum_2 = snapshot.last_checksum.1;
        }

        let (authority, coordination) = make_test_shm_coordination(&shared, &shm_path);
        coordination.cache_frame(7, 2);
        coordination.cache_frame(7, 5);
        assert_eq!(
            authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );
        assert_eq!(coordination.load_snapshot(), snapshot);
        assert_eq!(coordination.find_frame(7, 0, 5, None), Some(5));
    }

    let reopened_file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let reopened_shared = WalFileShared::new_shared(reopened_file).unwrap();
    let (reopened_authority, reopened_coordination) =
        make_test_shm_coordination(&reopened_shared, &shm_path);

    assert_eq!(
        reopened_authority.open_mode(),
        SharedWalCoordinationOpenMode::Exclusive
    );
    assert_eq!(
        reopened_coordination.load_snapshot(),
        WalSnapshot {
            max_frame: 12,
            nbackfills: 8,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        }
    );
    assert_eq!(
        reopened_coordination.iter_latest_frames(0, u64::MAX),
        vec![(7, 5)]
    );
    assert_eq!(reopened_authority.min_active_reader_frame(), None);
}

#[cfg(host_shared_wal)]
#[test]
fn test_open_shared_from_authority_reuses_trusted_snapshot_after_exclusive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();
    let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
    {
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        authority.install_snapshot(snapshot);
        authority.record_frame(7, 1);
    }
    let reopened_authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    assert_eq!(
        reopened_authority.open_mode(),
        SharedWalCoordinationOpenMode::Exclusive
    );

    let shared = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &reopened_authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();

    let shared = shared.read();
    assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 1);
    assert_eq!(shared.metadata.nbackfills.load(Ordering::Acquire), 0);
    assert_eq!(
        shared.metadata.transaction_count.load(Ordering::Acquire),
        snapshot.transaction_count
    );
    assert_eq!(
        shared.metadata.last_checksum,
        (snapshot.checksum_1, snapshot.checksum_2)
    );
    assert_eq!(
        shared.runtime.epoch.load(Ordering::Acquire),
        snapshot.checkpoint_epoch
    );
    assert!(shared.metadata.initialized.load(Ordering::Acquire));
    assert!(!shared
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));
    assert!(shared.runtime.frame_cache.lock().is_empty());
}

#[cfg(host_shared_wal)]
#[test]
fn test_shm_coordination_live_overflow_returns_busy_without_runtime_disk_scan() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-live-overflow.db-wal");
    let shm_path = dir.path().join("test-live-overflow.db-tshm");
    let io = shared_wal_test_io();
    let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    authority.install_snapshot(snapshot);
    authority.record_frame(7, 1);

    let reopened_authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    let shared = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &reopened_authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();
    assert!(shared.read().runtime.frame_cache.lock().is_empty());

    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    buffer_pool.finalize_with_page_size(4096).unwrap();
    let wal = WalFile::new_with_shared_coordination(
        io.clone(),
        shared.clone(),
        reopened_authority.clone(),
        ((0, 0), 0),
        buffer_pool,
    );

    wal.begin_read_tx().unwrap();
    reopened_authority.mark_frame_index_overflowed_for_tests();

    assert!(
        matches!(wal.find_frame(7, None), Err(LimboError::Busy)),
        "page lookup must refuse the overflowed path instead of rescanning the WAL synchronously"
    );
    assert!(
        shared.read().runtime.frame_cache.lock().is_empty(),
        "refusing the overflow refresh must leave the local fallback cache untouched"
    );

    wal.end_read_tx();
    assert!(
        matches!(wal.begin_read_tx(), Err(LimboError::Busy)),
        "new readers must also refuse an uncovered overflowed frame index without blocking"
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_open_shared_from_authority_exclusive_rebuilds_positive_snapshot_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-exclusive-positive.db-wal");
    let shm_path = dir.path().join("test-exclusive-positive.db-tshm");
    let io = shared_wal_test_io();
    let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
    {
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        authority.install_snapshot(SharedWalCoordinationHeader {
            nbackfills: snapshot.max_frame,
            ..snapshot
        });
        authority.record_frame(7, 1);
        assert_eq!(
            authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );
    }

    let reopened_authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    assert_eq!(
        reopened_authority.open_mode(),
        SharedWalCoordinationOpenMode::Exclusive
    );

    let shared = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &reopened_authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();

    let shared = shared.read();
    assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 1);
    assert_eq!(shared.metadata.nbackfills.load(Ordering::Acquire), 0);
    assert!(shared
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));
    assert_eq!(
        shared.runtime.frame_cache.lock().get(&7).cloned(),
        Some(vec![1])
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_shared_coordination_open_uses_reconciled_snapshot_for_local_wal_state() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();

    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    let snapshot = SharedWalCoordinationHeader {
        max_frame: 1,
        nbackfills: 0,
        transaction_count: 9,
        visibility_generation: 3,
        checkpoint_seq: 5,
        checkpoint_epoch: 7,
        page_size: 4096,
        salt_1: 17,
        salt_2: 23,
        checksum_1: 31,
        checksum_2: 37,
        reader_slot_count: 64,
    };
    authority.install_snapshot(snapshot);
    authority.record_frame(7, 1);

    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    buffer_pool.finalize_with_page_size(4096).unwrap();
    let wal =
        WalFile::new_with_shared_coordination(io, shared, authority, ((0, 0), 0), buffer_pool);

    assert_eq!(wal.get_max_frame(), 1);
    assert_eq!(wal.get_last_checksum(), (31, 37));
}

#[cfg(host_shared_wal)]
#[test]
fn test_open_shared_from_authority_rebuilds_from_disk_when_snapshot_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-stale.db-wal");
    let shm_path = dir.path().join("test-stale.db-tshm");
    let io = shared_wal_test_io();
    let valid_snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    authority.install_snapshot(SharedWalCoordinationHeader {
        max_frame: 0,
        nbackfills: 0,
        transaction_count: 0,
        visibility_generation: 0,
        checkpoint_seq: valid_snapshot.checkpoint_seq,
        checkpoint_epoch: 0,
        page_size: valid_snapshot.page_size,
        salt_1: valid_snapshot.salt_1,
        salt_2: valid_snapshot.salt_2,
        checksum_1: 0,
        checksum_2: 0,
        reader_slot_count: 64,
    });

    let shared = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();

    let shared = shared.read();
    assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 1);
    assert_eq!(
        shared.metadata.last_checksum,
        (valid_snapshot.checksum_1, valid_snapshot.checksum_2)
    );
    assert!(shared
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));
    assert_eq!(
        shared.runtime.frame_cache.lock().get(&7).cloned(),
        Some(vec![1])
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_open_shared_from_authority_rebuilt_authority_persists_across_exclusive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-republish.db-wal");
    let shm_path = dir.path().join("test-republish.db-tshm");
    let io = shared_wal_test_io();
    let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    authority.install_snapshot(snapshot);

    let exclusive = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();
    assert!(exclusive
        .read()
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));
    assert!(
        authority.iter_latest_frames(0, u64::MAX).is_empty(),
        "open_shared_from_authority_if_exists should not republish authority before coordination reconciliation"
    );

    let exclusive_coordination = ShmWalCoordination::new(exclusive, authority.clone());
    assert_eq!(authority.iter_latest_frames(0, u64::MAX), vec![(7, 1)]);
    assert_eq!(exclusive_coordination.find_frame(7, 0, 1, None), Some(1));

    drop(exclusive_coordination);
    drop(authority);

    let reopened_authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    assert_eq!(
        reopened_authority.open_mode(),
        SharedWalCoordinationOpenMode::Exclusive
    );

    let reopened_shared = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &reopened_authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();
    assert!(!reopened_shared
        .read()
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));
    let reopened_coordination = ShmWalCoordination::new(reopened_shared, reopened_authority);
    assert_eq!(reopened_coordination.find_frame(7, 0, 1, None), Some(1));
}

#[cfg(host_shared_wal)]
#[test]
fn test_open_shared_from_authority_exclusive_disk_scan_does_not_downgrade_newer_zero_frame_generation(
) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-zero-frame-reopen.db-wal");
    let shm_path = dir.path().join("test-zero-frame-reopen.db-tshm");
    let io = shared_wal_test_io();
    let prior_generation = write_test_wal_with_single_commit_frame(&io, &wal_path);
    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    let restarted_generation = SharedWalCoordinationHeader {
        max_frame: 0,
        nbackfills: 0,
        transaction_count: prior_generation.transaction_count,
        visibility_generation: prior_generation.visibility_generation,
        checkpoint_seq: prior_generation.checkpoint_seq.wrapping_add(1),
        checkpoint_epoch: prior_generation.checkpoint_epoch,
        page_size: prior_generation.page_size,
        salt_1: prior_generation.salt_1.wrapping_add(1),
        salt_2: prior_generation.salt_2.wrapping_add(1),
        checksum_1: prior_generation.checksum_1,
        checksum_2: prior_generation.checksum_2,
        reader_slot_count: prior_generation.reader_slot_count,
    };
    authority.install_snapshot(restarted_generation);

    let shared = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();
    assert!(shared
        .read()
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));

    let coordination = ShmWalCoordination::new(shared.clone(), authority.clone());
    let reopened = coordination.load_snapshot();
    assert_eq!(reopened.max_frame, 0);
    assert_eq!(reopened.nbackfills, 0);
    assert_eq!(reopened.checkpoint_seq, restarted_generation.checkpoint_seq);
    assert_eq!(
        authority.snapshot().checkpoint_seq,
        restarted_generation.checkpoint_seq
    );
    assert!(
        !coordination.wal_is_initialized(),
        "preserving a newer zero-frame generation must require the first append to rewrite the WAL header"
    );
    assert!(
        shared.read().runtime.frame_cache.lock().is_empty(),
        "older WAL frames from a prior generation must not survive zero-frame authority recovery"
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_open_shared_from_authority_ignores_unpublished_backfill_proof_after_exclusive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-unpublished-proof.db-wal");
    let shm_path = dir.path().join("test-unpublished-proof.db-tshm");
    let io = shared_wal_test_io();
    let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
    {
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        authority.install_snapshot(snapshot);
        authority.install_backfill_proof(
            SharedWalCoordinationHeader {
                nbackfills: snapshot.max_frame,
                ..snapshot
            },
            11,
            0xAABB_CCDD,
        );
    }
    let reopened_authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    assert_eq!(
        reopened_authority.open_mode(),
        SharedWalCoordinationOpenMode::Exclusive
    );

    let shared = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &reopened_authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();

    let shared = shared.read();
    assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 1);
    assert_eq!(shared.metadata.nbackfills.load(Ordering::Acquire), 0);
    assert!(shared
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));
}

#[cfg(host_shared_wal)]
#[test]
fn test_restart_checkpoint_clears_backfill_proof_and_later_replaces_it() {
    let (db, path) = get_database();
    let wal_path = path.path().join("test.db-wal");
    let wal_path_str = wal_path.to_str().unwrap();
    let conn = db.connect().unwrap();
    conn.wal_auto_actions_disable();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 2);

    let pager = conn.pager.load();
    let partial = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );
    assert!(
        partial.wal_total_backfilled > 0 && !partial.everything_backfilled(),
        "setup must create a partial checkpoint with a positive durable backfill proof"
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before_restart = authority.snapshot();
    let (db_size_before, db_crc_before) =
        super::read_database_identity_from_file_path(&db.io, wal_path_str)
            .unwrap()
            .unwrap();
    assert!(
        authority.validate_backfill_proof(snapshot_before_restart, db_size_before, db_crc_before),
        "setup must install a valid proof before RESTART"
    );

    let restart = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
    assert!(
        restart.everything_backfilled(),
        "RESTART should fully backfill before resetting the WAL generation"
    );

    let snapshot_after_restart = authority.snapshot();
    assert_eq!(snapshot_after_restart.max_frame, 0);
    assert_eq!(snapshot_after_restart.nbackfills, 0);
    assert!(
        !authority.validate_backfill_proof(snapshot_before_restart, db_size_before, db_crc_before),
        "RESTART must clear the proof for the old WAL generation"
    );

    bulk_inserts(&conn, 6, 2);
    let replacement = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );
    assert!(
        replacement.wal_total_backfilled > 0 && !replacement.everything_backfilled(),
        "replacement setup must create a new partial checkpoint after RESTART"
    );

    let snapshot_after_replacement = authority.snapshot();
    let (db_size_after, db_crc_after) =
        super::read_database_identity_from_file_path(&db.io, wal_path_str)
            .unwrap()
            .unwrap();
    assert!(
        authority.validate_backfill_proof(snapshot_after_replacement, db_size_after, db_crc_after),
        "partial checkpoint after RESTART must install a replacement proof for the new generation"
    );
    assert_ne!(
        snapshot_after_replacement.checkpoint_seq, snapshot_before_restart.checkpoint_seq,
        "replacement proof must belong to the restarted WAL generation"
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_truncate_checkpoint_clears_backfill_proof_and_later_replaces_it() {
    let (db, path) = get_database();
    let wal_path = path.path().join("test.db-wal");
    let wal_path_str = wal_path.to_str().unwrap();
    let conn = db.connect().unwrap();
    conn.wal_auto_actions_disable();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 2);

    let pager = conn.pager.load();
    let partial = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );
    assert!(
        partial.wal_total_backfilled > 0 && !partial.everything_backfilled(),
        "setup must create a partial checkpoint with a positive durable backfill proof"
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before_truncate = authority.snapshot();
    let (db_size_before, db_crc_before) =
        super::read_database_identity_from_file_path(&db.io, wal_path_str)
            .unwrap()
            .unwrap();
    assert!(
        authority.validate_backfill_proof(snapshot_before_truncate, db_size_before, db_crc_before),
        "setup must install a valid proof before TRUNCATE"
    );

    let truncate = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    assert!(
        truncate.everything_backfilled(),
        "TRUNCATE should fully backfill before truncating the WAL"
    );

    let snapshot_after_truncate = authority.snapshot();
    assert_eq!(snapshot_after_truncate.max_frame, 0);
    assert_eq!(snapshot_after_truncate.nbackfills, 0);
    assert!(
        !authority.validate_backfill_proof(snapshot_before_truncate, db_size_before, db_crc_before),
        "TRUNCATE must clear the proof for the truncated WAL generation"
    );
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        0,
        "TRUNCATE must leave the WAL file empty before the new generation begins"
    );

    bulk_inserts(&conn, 6, 2);
    let replacement = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );
    assert!(
        replacement.wal_total_backfilled > 0 && !replacement.everything_backfilled(),
        "replacement setup must create a new partial checkpoint after TRUNCATE"
    );

    let snapshot_after_replacement = authority.snapshot();
    let (db_size_after, db_crc_after) =
        super::read_database_identity_from_file_path(&db.io, wal_path_str)
            .unwrap()
            .unwrap();
    assert!(
        authority.validate_backfill_proof(snapshot_after_replacement, db_size_after, db_crc_after),
        "partial checkpoint after TRUNCATE must install a replacement proof for the new generation"
    );
    assert_ne!(
        snapshot_after_replacement.checkpoint_seq, snapshot_before_truncate.checkpoint_seq,
        "replacement proof must belong to the truncated WAL generation"
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_classify_authority_snapshot_marks_truncated_wal_for_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-truncated.db-wal");
    let io = shared_wal_test_io();
    let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);

    let wal_len = std::fs::metadata(&wal_path).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .unwrap()
        .set_len(wal_len - 1)
        .unwrap();

    assert_eq!(
        classify_authority_snapshot_against_wal(
            &io,
            &io.open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
                .unwrap(),
            snapshot,
        )
        .unwrap(),
        AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::WalLengthMismatch
        )
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_classify_authority_snapshot_marks_corrupt_header_for_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-corrupt-header.db-wal");
    let io = shared_wal_test_io();
    std::fs::write(&wal_path, [0u8; WAL_HEADER_SIZE]).unwrap();

    let snapshot = SharedWalCoordinationHeader {
        max_frame: 0,
        nbackfills: 0,
        transaction_count: 9,
        visibility_generation: 1,
        checkpoint_seq: 5,
        checkpoint_epoch: 7,
        page_size: 4096,
        salt_1: 17,
        salt_2: 23,
        checksum_1: 31,
        checksum_2: 37,
        reader_slot_count: 64,
    };

    assert_eq!(
        classify_authority_snapshot_against_wal(
            &io,
            &io.open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
                .unwrap(),
            snapshot,
        )
        .unwrap(),
        AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::WalHeaderUnreadable
        )
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_open_shared_from_authority_keeps_zero_length_wal_uninitialized_after_exclusive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test-empty.db-wal");
    let shm_path = dir.path().join("test-empty.db-tshm");
    let io = shared_wal_test_io();

    io.open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    {
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        authority.install_snapshot(SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        });
    }
    let reopened_authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    assert_eq!(
        reopened_authority.open_mode(),
        SharedWalCoordinationOpenMode::Exclusive
    );

    let shared = WalFileShared::open_shared_from_authority_if_exists(
        &io,
        wal_path.to_str().unwrap(),
        crate::OpenFlags::Create,
        &reopened_authority,
        &open_test_db_file_for_wal(&io, &wal_path),
    )
    .unwrap();

    let shared = shared.read();
    assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 0);
    assert_eq!(shared.metadata.last_checksum, (31, 37));
    assert!(!shared.metadata.initialized.load(Ordering::Acquire));
    assert!(!shared
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));
}

#[cfg(host_shared_wal)]
#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "experimental_win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shm_coordination_secondary_disk_scan_does_not_reseed_authority_while_writer_active() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let file_a = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_a = WalFileShared::new_shared(file_a).unwrap();
    let authoritative = WalSnapshot {
        max_frame: 5,
        nbackfills: 0,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: 9,
    };
    set_shared_snapshot(&shared_a, authoritative);
    {
        let shared = shared_a.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = authoritative.last_checksum.0;
        header.checksum_2 = authoritative.last_checksum.1;
    }
    let (authority, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
    coordination_a.cache_frame(7, 2);
    coordination_a.cache_frame(7, 5);
    assert!(authority.try_acquire_writer(authority.owner_record()));

    let file_b = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_b = WalFileShared::new_shared(file_b).unwrap();
    let stale = WalSnapshot {
        max_frame: 2,
        nbackfills: 0,
        last_checksum: (11, 13),
        checkpoint_seq: 4,
        transaction_count: 3,
    };
    set_shared_snapshot(&shared_b, stale);
    {
        let shared = shared_b.write();
        shared
            .metadata
            .loaded_from_disk_scan
            .store(true, Ordering::Release);
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = stale.last_checksum.0;
        header.checksum_2 = stale.last_checksum.1;
        shared.runtime.frame_cache.lock().insert(7, vec![2]);
    }

    let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);

    assert_eq!(coordination_b.load_snapshot(), authoritative);
    assert_eq!(authority.snapshot().max_frame, authoritative.max_frame);
    assert_eq!(
        authority.snapshot().transaction_count,
        authoritative.transaction_count
    );
    assert_eq!(coordination_b.find_frame(7, 0, 5, None), Some(5));
    authority.release_writer(authority.owner_record());
}

#[cfg(host_shared_wal)]
#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "experimental_win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shm_coordination_disk_scan_matching_authority_keeps_frame_index() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let file_a = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_a = WalFileShared::new_shared(file_a).unwrap();
    let authoritative = WalSnapshot {
        max_frame: 5,
        nbackfills: 2,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: 9,
    };
    set_shared_snapshot(&shared_a, authoritative);
    {
        let shared = shared_a.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = authoritative.last_checksum.0;
        header.checksum_2 = authoritative.last_checksum.1;
    }
    let (authority, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
    coordination_a.cache_frame(7, 2);
    coordination_a.cache_frame(9, 5);

    let file_b = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_b = WalFileShared::new_shared(file_b).unwrap();
    set_shared_snapshot(&shared_b, authoritative);
    {
        let shared = shared_b.write();
        shared
            .metadata
            .loaded_from_disk_scan
            .store(true, Ordering::Release);
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = authoritative.last_checksum.0;
        header.checksum_2 = authoritative.last_checksum.1;
        let mut frame_cache = shared.runtime.frame_cache.lock();
        frame_cache.insert(7, vec![2]);
        frame_cache.insert(9, vec![5]);
    }

    let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);

    let reopened = coordination_b.load_snapshot();
    assert_eq!(reopened.max_frame, authoritative.max_frame);
    assert_eq!(reopened.last_checksum, authoritative.last_checksum);
    assert_eq!(reopened.checkpoint_seq, authoritative.checkpoint_seq);
    assert_eq!(reopened.transaction_count, authoritative.transaction_count);
    assert_eq!(
        reopened.nbackfills, 0,
        "disk-scan reconciliation must preserve the frame index without reviving positive nbackfills"
    );
    assert_eq!(authority.find_frame(7, 0, 5, None), Some(2));
    assert_eq!(authority.find_frame(9, 0, 5, None), Some(5));
    assert_eq!(
        authority.iter_latest_frames(0, authoritative.max_frame),
        vec![(7, 2), (9, 5)]
    );
}

#[cfg(host_shared_wal)]
#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "experimental_win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shm_coordination_disk_scan_matching_snapshot_rebuilds_stale_frame_index() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let file_a = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_a = WalFileShared::new_shared(file_a).unwrap();
    let authoritative = WalSnapshot {
        max_frame: 5,
        nbackfills: 0,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: 9,
    };
    set_shared_snapshot(&shared_a, authoritative);
    {
        let shared = shared_a.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = authoritative.last_checksum.0;
        header.checksum_2 = authoritative.last_checksum.1;
    }
    {
        let (_authority, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
        coordination_a.cache_frame(7, 2);
        coordination_a.cache_frame(9, 4);
    }

    let file_b = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_b = WalFileShared::new_shared(file_b).unwrap();
    set_shared_snapshot(&shared_b, authoritative);
    {
        let shared = shared_b.write();
        shared
            .metadata
            .loaded_from_disk_scan
            .store(true, Ordering::Release);
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = authoritative.last_checksum.0;
        header.checksum_2 = authoritative.last_checksum.1;
        shared.runtime.frame_cache.lock().insert(7, vec![2]);
        shared.runtime.frame_cache.lock().insert(9, vec![5]);
    }

    let (authority, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);
    assert_eq!(
        authority.open_mode(),
        SharedWalCoordinationOpenMode::Exclusive
    );

    let reopened = coordination_b.load_snapshot();
    assert_eq!(reopened.max_frame, authoritative.max_frame);
    assert_eq!(reopened.last_checksum, authoritative.last_checksum);
    assert_eq!(reopened.checkpoint_seq, authoritative.checkpoint_seq);
    assert_eq!(reopened.transaction_count, authoritative.transaction_count);
    assert_eq!(authority.find_frame(7, 0, 5, None), Some(2));
    assert_eq!(
        authority.find_frame(9, 0, 5, None),
        Some(5),
        "matching snapshot metadata must not preserve a stale shared frame index across restart recovery"
    );
    assert_eq!(
        authority.iter_latest_frames(0, authoritative.max_frame),
        vec![(7, 2), (9, 5)]
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_shm_coordination_empty_disk_scan_keeps_zero_frame_authority_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    let authoritative = SharedWalCoordinationHeader {
        max_frame: 0,
        nbackfills: 0,
        transaction_count: 9,
        visibility_generation: 3,
        checkpoint_seq: 5,
        checkpoint_epoch: 7,
        page_size: 4096,
        salt_1: 17,
        salt_2: 23,
        checksum_1: 31,
        checksum_2: 37,
        reader_slot_count: 64,
    };
    authority.install_snapshot(authoritative);

    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    {
        let shared = shared.write();
        shared
            .metadata
            .loaded_from_disk_scan
            .store(true, Ordering::Release);
    }

    let coordination = ShmWalCoordination::new(shared, authority.clone());
    let snapshot = authority.snapshot();
    assert_eq!(snapshot, authoritative);
    let header = coordination.wal_header();
    assert_eq!(header.page_size, 4096);
    assert_eq!(header.checkpoint_seq, authoritative.checkpoint_seq);
    assert_eq!(header.salt_1, authoritative.salt_1);
    assert_eq!(header.salt_2, authoritative.salt_2);
}

#[cfg(host_shared_wal)]
#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "experimental_win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shm_coordination_empty_disk_scan_does_not_clobber_positive_authority() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let file_a = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_a = WalFileShared::new_shared(file_a).unwrap();
    let authoritative = WalSnapshot {
        max_frame: 5,
        nbackfills: 0,
        last_checksum: (31, 37),
        checkpoint_seq: 5,
        transaction_count: 9,
    };
    set_shared_snapshot(&shared_a, authoritative);
    {
        let shared = shared_a.write();
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = authoritative.last_checksum.0;
        header.checksum_2 = authoritative.last_checksum.1;
    }
    let (authority, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
    coordination_a.cache_frame(7, 2);
    coordination_a.cache_frame(9, 5);

    let file_b = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_b = WalFileShared::new_shared(file_b).unwrap();
    {
        let shared = shared_b.write();
        shared
            .metadata
            .loaded_from_disk_scan
            .store(true, Ordering::Release);
        let mut header = shared.metadata.wal_header.lock();
        header.page_size = 4096;
        header.checkpoint_seq = authoritative.checkpoint_seq;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = 11;
        header.checksum_2 = 13;
    }

    let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);
    let reopened = coordination_b.load_snapshot();
    assert_eq!(reopened.max_frame, authoritative.max_frame);
    assert_eq!(reopened.checkpoint_seq, authoritative.checkpoint_seq);
    assert_eq!(reopened.transaction_count, authoritative.transaction_count);
    assert_eq!(
        authority.find_frame(7, 0, authoritative.max_frame, None),
        Some(2)
    );
    assert_eq!(
        authority.find_frame(9, 0, authoritative.max_frame, None),
        Some(5)
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_shm_zero_frame_authority_invalidates_stale_local_initialized_state() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    let authoritative = SharedWalCoordinationHeader {
        max_frame: 0,
        nbackfills: 0,
        transaction_count: 9,
        visibility_generation: 3,
        checkpoint_seq: 5,
        checkpoint_epoch: 7,
        page_size: 4096,
        salt_1: 17,
        salt_2: 23,
        checksum_1: 31,
        checksum_2: 37,
        reader_slot_count: 64,
    };
    authority.install_snapshot(authoritative);

    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    {
        let mut shared = shared.write();
        shared.metadata.max_frame.store(11, Ordering::Release);
        shared.metadata.nbackfills.store(11, Ordering::Release);
        shared.metadata.last_checksum = (11, 13);
        shared
            .metadata
            .transaction_count
            .store(3, Ordering::Release);
        shared.metadata.initialized.store(true, Ordering::Release);
        let mut header = shared.metadata.wal_header.lock();
        header.checkpoint_seq = 2;
        header.page_size = 4096;
        header.salt_1 = 7;
        header.salt_2 = 13;
        header.checksum_1 = 11;
        header.checksum_2 = 13;
        shared.runtime.epoch.store(1, Ordering::Release);
    }

    let coordination = ShmWalCoordination::new(shared.clone(), authority);
    assert!(
        !coordination.wal_is_initialized(),
        "a stale local initialized bit must not suppress the first header rewrite after RESTART/TRUNCATE"
    );
    {
        let shared = shared.read();
        assert!(
            !shared.metadata.initialized.load(Ordering::Acquire),
            "stale local initialized state must be cleared"
        );
        let header = shared.metadata.wal_header.lock();
        assert_eq!(header.checkpoint_seq, authoritative.checkpoint_seq);
        assert_eq!(header.page_size, authoritative.page_size);
        assert_eq!(header.salt_1, authoritative.salt_1);
        assert_eq!(header.salt_2, authoritative.salt_2);
        assert_eq!(header.checksum_1, authoritative.checksum_1);
        assert_eq!(header.checksum_2, authoritative.checksum_2);
    }

    let prepared = coordination
        .prepare_wal_header(io.as_ref(), PageSize::new(4096).unwrap())
        .expect("zero-frame authority should force a header rewrite");
    assert_eq!(prepared.checkpoint_seq, authoritative.checkpoint_seq);
    coordination.mark_initialized();
    assert!(
        coordination.wal_is_initialized(),
        "once the current-generation header is durably rewritten, wal_is_initialized should succeed"
    );
}

#[cfg(host_shared_wal)]
#[test]
fn test_shm_prepare_wal_header_seeds_uninitialized_authority_from_prepared_header() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    let coordination = ShmWalCoordination::new(shared, authority.clone());

    let prepared = coordination
        .prepare_wal_header(io.as_ref(), PageSize::new(4096).unwrap())
        .expect("fresh authority should accept the first prepared header");

    let snapshot = authority.snapshot();
    assert_eq!(
        snapshot.page_size, prepared.page_size,
        "authority must publish the prepared page size for later writers and checkpointers"
    );
    assert_eq!(
        snapshot.checkpoint_seq, prepared.checkpoint_seq,
        "authority must publish the prepared checkpoint generation"
    );
    assert_eq!(snapshot.salt_1, prepared.salt_1);
    assert_eq!(snapshot.salt_2, prepared.salt_2);
}

#[cfg(host_shared_wal)]
#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "experimental_win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shm_prepare_wal_header_does_not_clobber_zero_frame_authority_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.db-wal");
    let shm_path = dir.path().join("test.db-tshm");
    let io = shared_wal_test_io();

    let file_a = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_a = WalFileShared::new_shared(file_a).unwrap();
    let authoritative = SharedWalCoordinationHeader {
        max_frame: 0,
        nbackfills: 0,
        transaction_count: 9,
        visibility_generation: 3,
        checkpoint_seq: 5,
        checkpoint_epoch: 7,
        page_size: 4096,
        salt_1: 17,
        salt_2: 23,
        checksum_1: 31,
        checksum_2: 37,
        reader_slot_count: 64,
    };
    {
        let mut shared = shared_a.write();
        shared.metadata.max_frame.store(0, Ordering::Release);
        shared.metadata.nbackfills.store(0, Ordering::Release);
        shared
            .metadata
            .transaction_count
            .store(authoritative.transaction_count, Ordering::Release);
        shared.metadata.last_checksum = (31, 37);
        let mut header = shared.metadata.wal_header.lock();
        header.checkpoint_seq = authoritative.checkpoint_seq;
        header.page_size = authoritative.page_size;
        header.salt_1 = authoritative.salt_1;
        header.salt_2 = authoritative.salt_2;
        header.checksum_1 = authoritative.checksum_1;
        header.checksum_2 = authoritative.checksum_2;
        shared
            .runtime
            .epoch
            .store(authoritative.checkpoint_epoch, Ordering::Release);
        shared.metadata.initialized.store(false, Ordering::Release);
    }
    let authority =
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
    authority.install_snapshot(authoritative);

    let file_b = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared_b = WalFileShared::new_shared(file_b).unwrap();
    let coordination_b = ShmWalCoordination::new(shared_b.clone(), authority.clone());
    // Simulate a long-lived process whose process-wide shared WAL metadata
    // fell behind the authority after another process checkpointed and
    // restarted the WAL back to frame 0.
    {
        let mut shared = shared_b.write();
        shared.metadata.max_frame.store(0, Ordering::Release);
        shared.metadata.nbackfills.store(0, Ordering::Release);
        shared.metadata.last_checksum = (11, 13);
        shared
            .metadata
            .transaction_count
            .store(3, Ordering::Release);
        let mut header = shared.metadata.wal_header.lock();
        header.checkpoint_seq = 2;
        header.page_size = 4096;
        header.salt_1 = 17;
        header.salt_2 = 23;
        header.checksum_1 = 11;
        header.checksum_2 = 13;
        shared.runtime.epoch.store(1, Ordering::Release);
        shared.metadata.initialized.store(false, Ordering::Release);
    }

    let page_size = PageSize::new(4096).unwrap();
    let prepared = coordination_b
        .prepare_wal_header(io.as_ref(), page_size)
        .expect("prepare_wal_header should produce a header");

    let snapshot = authority.snapshot();
    assert_eq!(
        snapshot.transaction_count, authoritative.transaction_count,
        "first writer after restart must not downgrade authority transaction_count"
    );
    assert_eq!(
        snapshot.checkpoint_seq, authoritative.checkpoint_seq,
        "first writer after restart must not downgrade checkpoint metadata"
    );
    assert_eq!(
        prepared.checkpoint_seq, authoritative.checkpoint_seq,
        "header written after restart must use authority checkpoint metadata"
    );
    assert_eq!(prepared.page_size, authoritative.page_size);
    assert_eq!(prepared.salt_1, authoritative.salt_1);
    assert_eq!(prepared.salt_2, authoritative.salt_2);
    let refreshed = authority.snapshot();
    assert_eq!(
        refreshed.checksum_1, prepared.checksum_1,
        "preparing the first zero-frame header must refresh the authoritative checksum seed"
    );
    assert_eq!(
        refreshed.checksum_2, prepared.checksum_2,
        "preparing the first zero-frame header must refresh the authoritative checksum seed"
    );
}

#[test]
fn test_in_process_coordination_lock_primitives() {
    let (shared, _wal) = make_test_wal();
    let coordination = make_test_coordination(&shared);

    assert!(coordination.try_checkpoint_lock());
    coordination.unlock_checkpoint_lock();

    assert!(coordination.try_write_lock());
    assert!(!coordination.try_write_lock());
    coordination.unlock_write_lock();

    assert!(coordination.try_read_mark_exclusive(1));
    coordination.set_read_mark_value_exclusive(1, 42);
    assert_eq!(coordination.read_mark_value(1), 42);
    coordination.unlock_read_mark(1);

    assert!(coordination.try_read_mark_shared(1));
    assert!(coordination.try_upgrade_read_mark(1));
    coordination.downgrade_read_mark(1);
    coordination.unlock_read_mark(1);

    // The coordination backend should still observe the shared state underneath.
    assert_eq!(shared.read().runtime.read_locks[1].get_value(), 42);
}

#[test]
fn test_in_process_coordination_prepare_truncate_marks_wal_uninitialized() {
    let io = shared_wal_test_io();
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    let coordination = make_test_coordination(&shared);

    shared
        .read()
        .metadata
        .initialized
        .store(true, Ordering::Release);
    let file = coordination.prepare_truncate().unwrap();

    assert!(file.size().is_ok());
    assert!(!shared.read().metadata.initialized.load(Ordering::Acquire));
}

#[test]
fn test_in_process_coordination_exposes_wal_io_state() {
    let io = shared_wal_test_io();
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let file = io
        .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
        .unwrap();
    let shared = WalFileShared::new_shared(file).unwrap();
    let coordination = make_test_coordination(&shared);

    assert!(!coordination.wal_is_initialized());
    assert_eq!(coordination.wal_header().page_size, 0);
    assert!(coordination.wal_file().unwrap().size().is_ok());

    let header = coordination
        .prepare_wal_header(io.as_ref(), PageSize::new(4096).unwrap())
        .unwrap();
    assert_eq!(header.page_size, 4096);
    assert_eq!(
        coordination.load_snapshot().last_checksum,
        (header.checksum_1, header.checksum_2)
    );
    assert!(!coordination.wal_is_initialized());

    coordination.mark_initialized();
    assert!(coordination.wal_is_initialized());
    assert!(coordination
        .prepare_wal_header(io.as_ref(), PageSize::new(4096).unwrap())
        .is_none());
}

#[test]
fn test_vacuum_lock_blocks_new_read_transactions_until_release() {
    let (shared, vacuum_wal) = make_test_wal();
    let reader_wal = make_test_wal_from_shared(shared);

    vacuum_wal.try_begin_vacuum_checkpoint_lock().unwrap();
    vacuum_wal.begin_vacuum_blocking_tx().unwrap();

    assert!(
        matches!(reader_wal.try_begin_read_tx(), TryBeginReadResult::Busy),
        "VACUUM lock should block new WAL readers before they take a read-mark slot"
    );
    assert!(
        !vacuum_wal.holds_read_lock(),
        "exclusive VACUUM snapshot must not masquerade as a read-mark lock"
    );
    assert!(
        vacuum_wal.holds_write_lock(),
        "begin_vacuum_blocking_tx should acquire the source write lock"
    );

    vacuum_wal.end_write_tx();
    vacuum_wal.release_vacuum_lock();
    vacuum_wal.release_vacuum_checkpoint_lock();

    assert!(
        matches!(reader_wal.try_begin_read_tx(), TryBeginReadResult::Ok(_)),
        "reader should start after VACUUM releases the lock"
    );
    reader_wal.end_read_tx();
}

#[test]
fn test_active_reader_blocks_vacuum_exclusive_tx() {
    let (shared, reader_wal) = make_test_wal();
    let vacuum_wal = make_test_wal_from_shared(shared);

    assert!(matches!(
        reader_wal.try_begin_read_tx(),
        TryBeginReadResult::Ok(_)
    ));
    vacuum_wal.try_begin_vacuum_checkpoint_lock().unwrap();

    assert!(
        matches!(vacuum_wal.begin_vacuum_blocking_tx(), Err(LimboError::Busy)),
        "active reader should prevent VACUUM from acquiring its exclusive lock"
    );

    reader_wal.end_read_tx();
    vacuum_wal.begin_vacuum_blocking_tx().unwrap();
    vacuum_wal.end_write_tx();
    vacuum_wal.release_vacuum_lock();
    vacuum_wal.release_vacuum_checkpoint_lock();
}

#[test]
fn test_read_retry_does_not_leak_vacuum_guard_or_block_vacuum() {
    let (shared, _) = make_test_wal();
    let retry_reader = make_test_wal_from_shared(shared.clone());
    let vacuum_wal = make_test_wal_from_shared(shared.clone());

    set_shared_snapshot(
        &shared,
        WalSnapshot {
            max_frame: 5,
            nbackfills: 0,
            last_checksum: (0, 0),
            checkpoint_seq: 0,
            transaction_count: 1,
        },
    );

    for idx in 1..5 {
        assert!(
            shared.read().runtime.read_locks[idx].write(),
            "expected setup to occupy read-mark slot {idx}"
        );
    }

    assert!(
        matches!(retry_reader.try_begin_read_tx(), TryBeginReadResult::Retry),
        "reader should retry when all read-mark slots are transiently unavailable"
    );
    assert!(
        !retry_reader.has_vacuum_read_lock_guard(),
        "retry path must not retain a shared VACUUM lock guard"
    );
    assert_eq!(
        retry_reader
            .max_frame_read_lock_index
            .load(Ordering::Acquire),
        NO_LOCK_HELD,
        "retry path must not retain a read-mark slot"
    );

    for idx in 1..5 {
        shared.read().runtime.read_locks[idx].unlock();
    }

    vacuum_wal.try_begin_vacuum_checkpoint_lock().unwrap();
    vacuum_wal.begin_vacuum_blocking_tx().unwrap();
    vacuum_wal.end_write_tx();
    vacuum_wal.release_vacuum_lock();
    vacuum_wal.release_vacuum_checkpoint_lock();
}

#[test]
fn test_held_vacuum_checkpoint_locks_do_not_release_vacuum_lock() {
    let (shared, vacuum_wal) = make_test_wal();
    let contender_wal = make_test_wal_from_shared(shared);

    vacuum_wal.try_begin_vacuum_checkpoint_lock().unwrap();
    vacuum_wal.begin_vacuum_blocking_tx().unwrap();

    assert!(vacuum_wal.holds_write_lock());
    assert!(!vacuum_wal.holds_read_lock());
    assert!(
        matches!(
            contender_wal.try_begin_vacuum_checkpoint_lock(),
            Err(LimboError::Busy)
        ),
        "held checkpoint lock should block other checkpointers"
    );

    vacuum_wal.end_write_tx();
    assert!(!vacuum_wal.holds_write_lock());

    let guard =
        CheckpointLocks::from_held_vacuum_checkpoint_lock(vacuum_wal.coordination.clone()).unwrap();
    assert!(
        matches!(contender_wal.try_begin_read_tx(), TryBeginReadResult::Busy),
        "VACUUM lock should continue blocking readers during final checkpoint"
    );

    drop(guard);
    assert!(
        contender_wal.try_begin_vacuum_checkpoint_lock().is_ok(),
        "checkpoint cleanup should release the checkpoint lock"
    );
    contender_wal.release_vacuum_checkpoint_lock();
    assert!(
        matches!(contender_wal.try_begin_read_tx(), TryBeginReadResult::Busy),
        "checkpoint cleanup must not release the VACUUM lock"
    );

    vacuum_wal.release_vacuum_lock();
    assert!(matches!(
        contender_wal.try_begin_read_tx(),
        TryBeginReadResult::Ok(_)
    ));
    contender_wal.end_read_tx();
}

#[test]
fn restart_checkpoint_reset_wal_state_handling() {
    let (db, path) = get_database();

    let walpath = path.path().join("test.db-wal");

    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 20, 3);
    let IOResult::Done(completions) = conn.pager.load().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Snapshot header & counters before the RESTART checkpoint.
    let wal_shared = db.shared_wal.clone();
    let (seq_before, salt1_before, salt2_before, _ps_before) = wal_header_snapshot(&wal_shared);
    let (mx_before, backfill_before) = {
        let s = wal_shared.read();
        (
            s.metadata.max_frame.load(Ordering::SeqCst),
            s.metadata.nbackfills.load(Ordering::SeqCst),
        )
    };
    assert!(mx_before > 0);
    assert_eq!(backfill_before, 0);

    let meta_before = std::fs::metadata(&walpath).unwrap();
    #[cfg(unix)]
    let size_before = meta_before.blocks();
    #[cfg(not(unix))]
    let size_before = meta_before.len();
    // Run a RESTART checkpoint, should backfill everything and reset WAL counters,
    // but NOT truncate the file.
    {
        let pager = conn.pager.load();
        let res = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert_eq!(res.wal_max_frame, mx_before);
        assert_eq!(res.wal_total_backfilled, mx_before);
        assert_eq!(res.wal_checkpoint_backfilled, mx_before);
    }

    // Validate post‑RESTART header & counters.
    let (seq_after, salt1_after, salt2_after, _ps_after) = wal_header_snapshot(&wal_shared);
    assert_eq!(
        seq_after,
        seq_before.wrapping_add(1),
        "checkpoint_seq must increment on RESTART"
    );
    assert_eq!(
        salt1_after,
        salt1_before.wrapping_add(1),
        "salt_1 is incremented"
    );
    assert_ne!(salt2_after, salt2_before, "salt_2 is randomized");

    let (mx_after, backfill_after) = {
        let s = wal_shared.read();
        (
            s.metadata.max_frame.load(Ordering::SeqCst),
            s.metadata.nbackfills.load(Ordering::SeqCst),
        )
    };
    assert_eq!(mx_after, 0, "mxFrame reset to 0 after RESTART");
    assert_eq!(backfill_after, 0, "nBackfill reset to 0 after RESTART");

    // File size should be unchanged for RESTART (no truncate).
    let meta_after = std::fs::metadata(&walpath).unwrap();
    #[cfg(unix)]
    let size_after = meta_after.blocks();
    #[cfg(not(unix))]
    let size_after = meta_after.len();
    assert_eq!(
        size_before, size_after,
        "RESTART must not change WAL file size"
    );

    // Next write should start a new sequence at frame 1.
    conn.execute("insert into test(value) values ('post_restart')")
        .unwrap();
    conn.pager
        .load()
        .wal
        .as_ref()
        .unwrap()
        .finish_append_frames_commit()
        .unwrap();
    let new_max = wal_shared.read().metadata.max_frame.load(Ordering::SeqCst);
    assert_eq!(new_max, 1, "first append after RESTART starts at frame 1");
}

#[test]
fn test_wal_passive_partial_then_complete() {
    let (db, _tmp) = get_database();
    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 15, 2);
    let IOResult::Done(completions) = conn1.pager.load().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Force a read transaction that will freeze a lower read mark
    let readmark = {
        let pager = conn2.pager.load();
        let wal2 = pager.wal.as_ref().unwrap();
        wal2.begin_read_tx().unwrap();
        wal2.get_max_frame()
    };

    // generate more frames that the reader will not see.
    bulk_inserts(&conn1, 15, 2);
    let IOResult::Done(completions) = conn1.pager.load().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Run passive checkpoint, expect partial
    let (res1, max_before) = {
        let pager = conn1.pager.load();
        let res = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        let maxf = db
            .shared_wal
            .read()
            .metadata
            .max_frame
            .load(Ordering::SeqCst);
        (res, maxf)
    };
    assert_eq!(res1.wal_max_frame, max_before);
    assert!(
        res1.wal_total_backfilled < res1.wal_max_frame,
        "Partial backfill expected, {} : {}",
        res1.wal_total_backfilled,
        res1.wal_max_frame
    );
    assert_eq!(
        res1.wal_total_backfilled, readmark,
        "Checkpointed frames should match read mark"
    );
    // Release reader
    {
        let pager = conn2.pager.load();
        let wal2 = pager.wal.as_ref().unwrap();
        wal2.end_read_tx();
    }

    // Second passive checkpoint should finish
    let pager = conn1.pager.load();
    let res2 = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );
    assert_eq!(
        res2.wal_total_backfilled, res2.wal_max_frame,
        "Second checkpoint completes remaining frames"
    );
}

#[test]
fn test_wal_restart_blocks_readers() {
    let (db, _temp_dir) = get_database();
    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    // Start a read transaction
    conn2
        .pager
        .load()
        .wal
        .as_ref()
        .unwrap()
        .begin_read_tx()
        .unwrap();

    // checkpoint should succeed here because the wal is fully checkpointed (empty)
    // so the reader is using readmark0 to read directly from the db file.
    let p = conn1.pager.load();
    let w = p.wal.as_ref().unwrap();
    loop {
        match w.checkpoint(&p, CheckpointMode::Restart, SyncMode::Full) {
            Ok(IOResult::IO(io)) => {
                io.wait(db.io.as_ref()).unwrap();
            }
            e => {
                assert!(
                    matches!(&e, Err(err) if matches!(**err, LimboError::Busy)),
                    "reader is holding readmark0 we should return Busy"
                );
                break;
            }
        }
    }
    conn2.pager.load().end_read_tx();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    for i in 0..10 {
        conn1
            .execute(format!("insert into test(value) values ('value{i}')"))
            .unwrap();
    }
    // now that we have some frames to checkpoint, try again
    conn2.pager.load().begin_read_tx().unwrap();
    let p = conn1.pager.load();
    let w = p.wal.as_ref().unwrap();
    loop {
        match w.checkpoint(&p, CheckpointMode::Restart, SyncMode::Full) {
            Ok(IOResult::IO(io)) => {
                io.wait(db.io.as_ref()).unwrap();
            }
            Ok(IOResult::Done(_)) => {
                panic!("Checkpoint should not have succeeded");
            }
            Err(e) => {
                assert!(
                    matches!(*e, LimboError::Busy),
                    "should return busy if we have readers"
                );
                break;
            }
        }
    }
}

#[test]
fn test_wal_read_marks_after_restart() {
    let (db, _path) = get_database();
    let wal_shared = db.shared_wal.clone();

    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 10, 5);
    // Checkpoint with restart
    {
        let pager = conn.pager.load();
        let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert!(result.everything_backfilled());
    }

    // Verify read marks after restart
    let read_marks_after: Vec<_> = {
        let s = wal_shared.read();
        (0..5)
            .map(|i| s.runtime.read_locks[i].get_value())
            .collect()
    };

    assert_eq!(read_marks_after[0], 0, "Slot 0 should remain 0");
    assert_eq!(
        read_marks_after[1], 0,
        "Slot 1 (default reader) should be reset to 0"
    );
    for (i, item) in read_marks_after.iter().take(5).skip(2).enumerate() {
        assert_eq!(
            *item, READMARK_NOT_USED,
            "Slot {i} should be READMARK_NOT_USED after restart",
        );
    }
}

#[test]
fn test_wal_concurrent_readers_during_checkpoint() {
    let (db, _path) = get_database();
    let conn_writer = db.connect().unwrap();

    conn_writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn_writer, 5, 10);

    // Start multiple readers at different points
    let conn_r1 = db.connect().unwrap();
    let conn_r2 = db.connect().unwrap();

    // R1 starts reading
    let r1_max_frame = {
        let pager = conn_r1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_read_tx().unwrap();
        wal.get_max_frame()
    };
    bulk_inserts(&conn_writer, 5, 10);

    // R2 starts reading, sees more frames than R1
    let r2_max_frame = {
        let pager = conn_r2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_read_tx().unwrap();
        wal.get_max_frame()
    };

    // try passive checkpoint, should only checkpoint up to R1's position
    let checkpoint_result = {
        let pager = conn_writer.pager.load();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        )
    };

    assert!(
        checkpoint_result.wal_total_backfilled < checkpoint_result.wal_max_frame,
        "Should not checkpoint all frames when readers are active"
    );
    assert_eq!(
        checkpoint_result.wal_total_backfilled, r1_max_frame,
        "Should have checkpointed up to R1's max frame"
    );

    // Verify R2 still sees its frames
    assert_eq!(
        conn_r2.pager.load().wal.as_ref().unwrap().get_max_frame(),
        r2_max_frame,
        "Reader should maintain its snapshot"
    );
}

#[test]
fn test_wal_checkpoint_updates_read_marks() {
    let (db, _path) = get_database();
    let wal_shared = db.shared_wal.clone();

    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 10, 5);

    // get max frame before checkpoint
    let max_frame_before = wal_shared.read().metadata.max_frame.load(Ordering::SeqCst);

    {
        let pager = conn.pager.load();
        let _result = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
    }

    // check that read mark 1 (default reader) was updated to max_frame
    let read_mark_1 = wal_shared.read().runtime.read_locks[1].get_value();

    assert_eq!(
        read_mark_1 as u64, max_frame_before,
        "Read mark 1 should be updated to max frame during checkpoint"
    );
}

#[test]
fn test_wal_writer_blocks_restart_checkpoint() {
    let (db, _path) = get_database();
    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 5, 5);

    // start a write transaction
    {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        let _ = wal.begin_read_tx().unwrap();
        wal.begin_write_tx(WalAutoActions::all_enabled()).unwrap();
    }

    // should fail because writer lock is held
    let result = {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.checkpoint(&pager, CheckpointMode::Restart, SyncMode::Full)
    };

    assert!(
        matches!(&result, Err(err) if matches!(**err, LimboError::Busy)),
        "Restart checkpoint should fail when write lock is held"
    );

    conn2.pager.load().wal.as_ref().unwrap().end_read_tx();
    // release write lock
    conn2.pager.load().wal.as_ref().unwrap().end_write_tx();

    // now restart should succeed
    let result = {
        let pager = conn1.pager.load();
        run_checkpoint_until_done(&pager, CheckpointMode::Restart)
    };

    assert!(result.everything_backfilled());
}

#[test]
#[should_panic(expected = "must have a read transaction to begin a write transaction")]
fn test_wal_read_transaction_required_before_write() {
    let (db, _path) = get_database();
    let conn = db.connect().unwrap();

    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();

    // Attempt to start a write transaction without a read transaction
    let pager = conn.pager.load();
    let wal = pager.wal.as_ref().unwrap();
    let _ = wal.begin_write_tx(WalAutoActions::all_enabled());
}

fn check_read_lock_slot(conn: &Arc<Connection>, _expected_slot: usize) -> bool {
    let pager = conn.pager.load();
    let _wal = pager.wal.as_ref().unwrap();
    #[cfg(debug_assertions)]
    {
        let wal_any = _wal.as_any();
        if let Some(wal_file) = wal_any.downcast_ref::<crate::WalFile>() {
            return wal_file.max_frame_read_lock_index.load(Ordering::Acquire) == _expected_slot;
        }
    }

    false
}

#[test]
fn test_wal_multiple_readers_at_different_frames() {
    let (db, _path) = get_database();
    let conn_writer = db.connect().unwrap();

    conn_writer
        .execute("CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    fn start_reader(conn: &Arc<Connection>) -> (u64, crate::Statement) {
        conn.execute("BEGIN").unwrap();
        let mut stmt = conn.prepare("SELECT * FROM test").unwrap();
        stmt.step().unwrap();
        let frame = conn.pager.load().wal.as_ref().unwrap().get_max_frame();
        (frame, stmt)
    }

    bulk_inserts(&conn_writer, 3, 5);

    let conn1 = &db.connect().unwrap();
    let (r1_frame, _stmt) = start_reader(conn1); // reader 1

    bulk_inserts(&conn_writer, 3, 5);

    let conn_r2 = db.connect().unwrap();
    let (r2_frame, _stmt2) = start_reader(&conn_r2); // reader 2

    bulk_inserts(&conn_writer, 3, 5);

    let conn_r3 = db.connect().unwrap();
    let (r3_frame, _stmt3) = start_reader(&conn_r3); // reader 3

    assert!(r1_frame < r2_frame && r2_frame < r3_frame);

    // passive checkpoint #1
    let result1 = {
        let pager = conn_writer.pager.load();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        )
    };
    assert_eq!(result1.wal_total_backfilled, r1_frame);

    // finish reader‑1
    conn1.execute("COMMIT").unwrap();

    // passive checkpoint #2
    let result2 = {
        let pager = conn_writer.pager.load();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        )
    };
    assert_eq!(
        result1.wal_checkpoint_backfilled + result2.wal_checkpoint_backfilled,
        r2_frame
    );

    // verify visible rows
    let r2_cnt = count_test_table(&conn_r2);
    let r3_cnt = count_test_table(&conn_r3);

    assert_eq!(r2_cnt, 30);
    assert_eq!(r3_cnt, 45);
}

#[test]
fn test_checkpoint_truncate_reset_handling() {
    let (db, path) = get_database();
    let conn = db.connect().unwrap();

    let walpath = path.path().join("test.db-wal");

    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 10, 10);

    // Get size before checkpoint
    let size_before = std::fs::metadata(&walpath).unwrap().len();
    assert!(size_before > 0, "WAL file should have content");

    // Do a TRUNCATE checkpoint
    {
        let pager = conn.pager.load();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
    }

    // Check file size after truncate
    let size_after = std::fs::metadata(&walpath).unwrap().len();
    assert_eq!(size_after, 0, "WAL file should be truncated to 0 bytes");

    // Verify we can still write to the database
    conn.execute("INSERT INTO test VALUES (1001, 'after-truncate')")
        .unwrap();

    // Check WAL has new content
    let new_size = std::fs::metadata(&walpath).unwrap().len();
    assert!(new_size >= 32, "WAL file too small");
    let hdr = read_wal_header(&walpath);
    let expected_magic = if cfg!(target_endian = "big") {
        sqlite3_ondisk::WAL_MAGIC_BE
    } else {
        sqlite3_ondisk::WAL_MAGIC_LE
    };
    assert!(
        hdr.magic == expected_magic,
        "bad WAL magic: {:#X}, expected: {:#X}",
        hdr.magic,
        sqlite3_ondisk::WAL_MAGIC_BE
    );
    assert_eq!(hdr.file_format, 3007000);
    assert_eq!(hdr.page_size, 4096, "invalid page size");
    assert_eq!(hdr.checkpoint_seq, 1, "invalid checkpoint_seq");
}

#[test]
fn test_wal_checkpoint_truncate_db_file_contains_data() {
    let (db, path) = get_database();
    let conn = db.connect().unwrap();

    let walpath = path.path().join("test.db-wal");

    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 10, 100);

    // Get size before checkpoint
    let size_before = std::fs::metadata(&walpath).unwrap().len();
    assert!(size_before > 0, "WAL file should have content");

    // Do a TRUNCATE checkpoint
    {
        let pager = conn.pager.load();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
    }

    // Check file size after truncate
    let size_after = std::fs::metadata(&walpath).unwrap().len();
    assert_eq!(size_after, 0, "WAL file should be truncated to 0 bytes");

    // Verify we can still write to the database
    conn.execute("INSERT INTO test VALUES (1001, 'after-truncate')")
        .unwrap();

    // Check WAL has new content
    let new_size = std::fs::metadata(&walpath).unwrap().len();
    assert!(new_size >= 32, "WAL file too small");
    let hdr = read_wal_header(&walpath);
    let expected_magic = if cfg!(target_endian = "big") {
        sqlite3_ondisk::WAL_MAGIC_BE
    } else {
        sqlite3_ondisk::WAL_MAGIC_LE
    };
    assert!(
        hdr.magic == expected_magic,
        "bad WAL magic: {:#X}, expected: {:#X}",
        hdr.magic,
        sqlite3_ondisk::WAL_MAGIC_BE
    );
    assert_eq!(hdr.file_format, 3007000);
    assert_eq!(hdr.page_size, 4096, "invalid page size");
    assert_eq!(hdr.checkpoint_seq, 1, "invalid checkpoint_seq");
    {
        let pager = conn.pager.load();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
    }
    // delete the WAL file so we can read right from db and assert
    // that everything was backfilled properly
    std::fs::remove_file(&walpath).unwrap();

    let count = count_test_table(&conn);
    assert_eq!(
        count, 1001,
        "we should have 1001 rows in the table all together"
    );
}

fn read_wal_header(path: &std::path::Path) -> sqlite3_ondisk::WalHeader {
    use std::{fs::File, io::Read};
    let mut hdr = [0u8; 32];
    File::open(path).unwrap().read_exact(&mut hdr).unwrap();
    let be = |i| u32::from_be_bytes(hdr[i..i + 4].try_into().unwrap());
    sqlite3_ondisk::WalHeader {
        magic: be(0x00),
        file_format: be(0x04),
        page_size: be(0x08),
        checkpoint_seq: be(0x0C),
        salt_1: be(0x10),
        salt_2: be(0x14),
        checksum_1: be(0x18),
        checksum_2: be(0x1C),
    }
}

#[test]
fn test_wal_stale_snapshot_in_write_transaction() {
    let (db, _path) = get_database();
    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    // Start a read transaction on conn2
    {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_read_tx().unwrap();
    }
    // Make changes using conn1
    bulk_inserts(&conn1, 5, 5);
    // Try to start a write transaction on conn2 with a stale snapshot
    let result = {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_write_tx(WalAutoActions::all_enabled())
    };
    // Should get BusySnapShot due to stale snapshot
    assert!(matches!(result, Err(LimboError::BusySnapshot)));

    // End read transaction and start a fresh one
    {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.end_read_tx();
        wal.begin_read_tx().unwrap();
    }
    // Now write transaction should work
    let result = {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_write_tx(WalAutoActions::all_enabled())
    };
    assert!(matches!(result, Ok(())));
}

#[test]
fn test_wal_readlock0_optimization_behavior() {
    let (db, _path) = get_database();
    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 5, 5);
    // Do a full checkpoint to move all data to DB file
    {
        let pager = conn1.pager.load();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
    }

    // Start a read transaction on conn2
    {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_read_tx().unwrap();
    }
    // should use slot 0, as everything is backfilled
    assert!(check_read_lock_slot(&conn2, 0));
    {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        let frame = wal.find_frame(5, None);
        // since we hold readlock0, we should ignore the db file and find_frame should return none
        assert!(frame.is_ok_and(|f| f.is_none()));
    }
    // Try checkpoint, should fail because reader has slot 0
    {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        let result = wal.checkpoint(&pager, CheckpointMode::Restart, SyncMode::Full);

        assert!(
            matches!(&result, Err(err) if matches!(**err, LimboError::Busy)),
            "RESTART checkpoint should fail when a reader is using slot 0"
        );
    }
    // End the read transaction
    {
        let pager = conn2.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.end_read_tx();
    }
    {
        let pager = conn1.pager.load();
        let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert!(
            result.everything_backfilled(),
            "RESTART checkpoint should succeed after reader releases slot 0"
        );
    }
}

#[test]
fn test_wal_full_backfills_all() {
    let (db, _tmp) = get_database();
    let conn = db.connect().unwrap();

    // Write some data to put frames in the WAL
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 4);

    // Ensure frames are flushed to the WAL
    let IOResult::Done(completions) = conn.pager.load().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Snapshot the current mxFrame before running FULL
    let wal_shared = db.shared_wal.clone();
    let mx_before = wal_shared.read().metadata.max_frame.load(Ordering::SeqCst);
    assert!(mx_before > 0, "expected frames in WAL before FULL");

    // Run FULL checkpoint - must backfill *all* frames up to mx_before
    let result = {
        let pager = conn.pager.load();
        run_checkpoint_until_done(&pager, CheckpointMode::Full)
    };

    assert_eq!(result.wal_checkpoint_backfilled, mx_before);
    assert_eq!(result.wal_total_backfilled, mx_before);
}

#[test]
fn test_wal_full_waits_for_old_reader_then_succeeds() {
    let (db, _tmp) = get_database();
    let writer = db.connect().unwrap();
    let reader = db.connect().unwrap();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();

    // First commit some data and flush (reader will snapshot here)
    bulk_inserts(&writer, 2, 3);
    let IOResult::Done(completions) = writer.pager.load().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Start a read transaction pinned at the current snapshot
    {
        let pager = reader.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_read_tx().unwrap();
    }
    let r_snapshot = {
        let pager = reader.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.get_max_frame()
    };

    // Advance WAL beyond the reader's snapshot
    bulk_inserts(&writer, 3, 4);
    let IOResult::Done(completions) = writer.pager.load().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }
    let mx_now = db
        .shared_wal
        .read()
        .metadata
        .max_frame
        .load(Ordering::SeqCst);
    assert!(mx_now > r_snapshot);

    // FULL must return Busy while a reader is stuck behind
    {
        let pager = writer.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        loop {
            match wal.checkpoint(&pager, CheckpointMode::Full, SyncMode::Full) {
                Ok(IOResult::IO(io)) => {
                    // Drive any pending IO (should quickly become Busy or Done)
                    io.wait(db.io.as_ref()).unwrap();
                }
                Err(err) if matches!(*err, LimboError::Busy) => {
                    break;
                }
                other => panic!("expected Busy from FULL with old reader, got {other:?}"),
            }
        }
    }
    assert_eq!(
        db.shared_wal
            .read()
            .metadata
            .nbackfills
            .load(Ordering::SeqCst),
        0,
        "a FULL checkpoint that returns Busy must not publish positive nbackfills before DB sync"
    );

    // Release the reader, now full mode should succeed and backfill everything
    {
        let pager = reader.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.end_read_tx();
    }

    let result = {
        let pager = writer.pager.load();
        run_checkpoint_until_done(&pager, CheckpointMode::Full)
    };

    assert_eq!(
        result.wal_checkpoint_backfilled, mx_now,
        "the successful FULL reruns from the last durable backfill point because the Busy attempt did not publish progress"
    );
    assert!(result.everything_backfilled());
}

#[test]
fn test_rollback_releases_read_lock() {
    let (db, _path) = get_database();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();

    {
        let pager = conn.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        assert!(
            wal.holds_read_lock(),
            "read lock must be held during write tx"
        );
    }

    conn.execute("ROLLBACK").unwrap();

    {
        let pager = conn.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        assert!(
            !wal.holds_read_lock(),
            "read lock must be released after ROLLBACK"
        );
    }
}

#[test]
fn test_rollback_releases_shared_read_lock_slot() {
    let (db, _path) = get_database();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();

    let locked_slots_before = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert_eq!(
        locked_slots_before.len(),
        1,
        "expected exactly one shared read-lock slot while transaction is active"
    );

    conn.execute("ROLLBACK").unwrap();

    let locked_slots_after = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert!(
        locked_slots_after.is_empty(),
        "ROLLBACK must release the shared read-lock slot"
    );
}

#[test]
fn test_rollback_releases_slot_zero_read_lock() {
    let (db, _path) = get_database();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 3, 3);
    {
        let pager = conn.pager.load();
        let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert!(
            result.everything_backfilled(),
            "restart checkpoint setup must fully backfill WAL"
        );
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO test(value) VALUES('slot0')")
        .unwrap();

    let locked_slots_before = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert_eq!(
        locked_slots_before,
        vec![0],
        "writer should use slot 0 when WAL is fully checkpointed"
    );

    conn.execute("ROLLBACK").unwrap();

    let locked_slots_after = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert!(
        locked_slots_after.is_empty(),
        "ROLLBACK must release slot 0 shared read-lock as well"
    );
}

#[test]
fn test_savepoint_rollback_preserves_read_lock() {
    let (db, _path) = get_database();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();

    // Trigger a statement failure that causes savepoint rollback.
    // A duplicate primary key on the second INSERT will fail the
    // statement, rolling back to the anonymous savepoint while
    // keeping the write transaction open.
    let res = conn.execute("INSERT INTO t VALUES(1)");
    assert!(res.is_err(), "duplicate PK insert must fail");

    {
        let pager = conn.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        assert!(
            wal.holds_read_lock(),
            "read lock must still be held after savepoint rollback"
        );
        assert!(
            wal.holds_write_lock(),
            "write lock must still be held after savepoint rollback"
        );
    }

    // The transaction should still be usable: commit succeeds and
    // the first insert is preserved.
    conn.execute("COMMIT").unwrap();

    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
    let mut count: i64 = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 1, "first insert should survive savepoint rollback");
}

#[test]
fn test_savepoint_then_tx_rollback_allows_restart_checkpoint_from_other_connection() {
    let (db, _path) = get_database();
    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    conn1
        .execute("CREATE TABLE test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 2, 2);
    let count_before = count_test_table(&conn1);

    conn1.execute("BEGIN").unwrap();
    conn1
        .execute("INSERT INTO test(id, value) VALUES(1000, 'first')")
        .unwrap();
    let duplicate = conn1.execute("INSERT INTO test(id, value) VALUES(1000, 'dup')");
    assert!(duplicate.is_err(), "duplicate PK insert must fail");

    {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        assert!(
            wal.holds_read_lock(),
            "read lock must still be held after savepoint rollback"
        );
        assert!(
            wal.holds_write_lock(),
            "write lock must still be held after savepoint rollback"
        );
    }

    conn1.execute("ROLLBACK").unwrap();

    {
        let pager = conn1.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        assert!(
            !wal.holds_read_lock(),
            "read lock must be released after transaction rollback"
        );
        assert!(
            !wal.holds_write_lock(),
            "write lock must be released after transaction rollback"
        );
    }

    let locked_slots_after_rollback = {
        let shared = db.shared_wal.read();
        read_slots_with_readers(&shared)
    };
    assert!(
        locked_slots_after_rollback.is_empty(),
        "transaction rollback after savepoint failure must not leak shared read locks"
    );
    assert_eq!(
        count_test_table(&conn1),
        count_before,
        "transaction rollback should remove writes made before savepoint failure"
    );

    let result = {
        let pager = conn2.pager.load();
        run_checkpoint_until_done(&pager, CheckpointMode::Restart)
    };
    assert!(
        result.everything_backfilled(),
        "restart checkpoint from another connection must succeed after full rollback"
    );
}

#[test]
fn test_checkpoint_succeeds_after_rollback() {
    let (db, _path) = get_database();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 5, 3);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO test(value) VALUES('rollback_me')")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();

    let pager = conn.pager.load();
    let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
    assert!(
        result.everything_backfilled(),
        "checkpoint must succeed after rollback, not return Busy"
    );
}
