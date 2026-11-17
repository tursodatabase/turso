use crate::namespace::NamespaceId;
use crate::EngineDialect;
use rustc_hash::FxHashSet as HashSet;

use super::*;
use crate::alloc::{TursoAllocator, TursoIteratorExt, TursoTryWithCapacityExt};
use crate::database::remove_file_from_database_registry;
use crate::io::{PlatformIO, IO};
use crate::mvcc::clock::MvccClock;
use crate::mvcc::cursor::{CursorYieldPoint, MvccCursorType};
use crate::mvcc::database::checkpoint::CheckpointYieldPoint;
use crate::mvcc::database::{CommitYieldPoint, ExclusiveTxYieldPoint};
use crate::mvcc::persistent_storage::logical_log::{
    LogicalLog, ENCRYPTED_PAYLOAD_CHUNK_SIZE, EXT_FRAME_MAGIC, FRAME_MAGIC, LOG_HDR_SIZE,
};
#[cfg(feature = "conn_raw_api")]
use crate::mvcc::persistent_storage::logical_log::{ParsedOp, StreamingLogicalLogReader};
#[cfg(feature = "conn_raw_api")]
use crate::mvcc::portable_logical::{PortableLogicalBuilder, PortableObjectMapEntry};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::state_machine::{StateTransition, TransitionResult};
use crate::storage::sqlite3_ondisk::{
    checksum_wal, read_varint, write_varint, DatabaseHeader, WalHeader, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
};
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::Mutex;
use crate::sync::RwLock;
use crate::translate::emitter::TransactionMode;
use crate::types::ImmutableRecordRef;
use crate::vdbe::execute::TransactionYieldPoint;
use crate::{Completion, ConnectOpts, DatabaseOpts, EncryptionKey, LimboError, OpenFlags};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

const TX_BASE_HEADER_SIZE: usize = 24;
const TX_EXT_HEADER_SIZE: usize = 40;
const TX_TRAILER_SIZE: usize = 8;

pub(crate) struct MvccTestDbNoConn {
    pub(crate) db: Option<Arc<Database>>,
    path: Option<String>,
    opts: DatabaseOpts,
    enc_opts: Option<crate::EncryptionOpts>,
    // Stored mainly to not drop the temp dir before the test is done.
    _temp_dir: Option<tempfile::TempDir>,
}
pub(crate) struct MvccTestDb {
    pub(crate) mvcc_store: Arc<crate::MvStore>,
    pub(crate) db: Arc<Database>,
    pub(crate) conn: Arc<Connection>,
}

#[derive(Debug)]
struct FixedYieldInjector {
    remaining: Mutex<HashSet<YieldPoint>>,
}

impl FixedYieldInjector {
    fn new(points: impl IntoIterator<Item = YieldPoint>) -> Arc<Self> {
        Arc::new(Self {
            remaining: Mutex::new(points.into_iter().collect()),
        })
    }

    fn is_empty(&self) -> bool {
        self.remaining.lock().is_empty()
    }
}

impl YieldInjector for FixedYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        self.remaining.lock().remove(&point)
    }
}

fn drive_attach(conn: &Arc<Connection>, path: &str, alias: &str) {
    let mut state = crate::attach::AttachDatabaseState::default();
    loop {
        match conn.attach_database(path, alias, &mut state).unwrap() {
            crate::IOResult::Done(()) => return,
            crate::IOResult::IO(io) => io.wait(conn.db.io.as_ref()).unwrap(),
        }
    }
}

/// Steps `writer`'s COMMIT until it finishes or yields when the transaction
/// `selection_key` reaches `point`. A yielded COMMIT is kept unfinished.
struct CommitWriterOnExclusiveAcquireInjector {
    point: YieldPoint,
    selection_key: u64,
    writer: Arc<Connection>,
    writer_commit: Mutex<Option<crate::Statement>>,
    fired: AtomicBool,
}

impl CommitWriterOnExclusiveAcquireInjector {
    fn new(point: YieldPoint, selection_key: u64, writer: Arc<Connection>) -> Arc<Self> {
        Arc::new(Self {
            point,
            selection_key,
            writer,
            writer_commit: Mutex::new(None),
            fired: AtomicBool::new(false),
        })
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }

    fn take_writer_commit(&self) -> Option<crate::Statement> {
        self.writer_commit.lock().take()
    }
}

impl std::fmt::Debug for CommitWriterOnExclusiveAcquireInjector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitWriterOnExclusiveAcquireInjector")
            .field("point", &self.point)
            .field("selection_key", &self.selection_key)
            .field("fired", &self.fired())
            .finish_non_exhaustive()
    }
}

impl YieldInjector for CommitWriterOnExclusiveAcquireInjector {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        if point != self.point
            || selection_key != self.selection_key
            || self.fired.swap(true, Ordering::AcqRel)
        {
            return false;
        }
        let mut commit = self.writer.prepare("COMMIT").unwrap();
        loop {
            match commit.step().unwrap() {
                StepResult::Done => break,
                StepResult::Yield => {
                    *self.writer_commit.lock() = Some(commit);
                    break;
                }
                _ => {}
            }
        }
        true
    }
}

#[derive(Clone, Default)]
struct FailOnDemandAlloc {
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl FailOnDemandAlloc {
    fn fail_allocations(&self, fail: bool) {
        self.fail.store(fail, std::sync::atomic::Ordering::Relaxed);
    }
}

#[test]
fn write_set_take_transfers_entries_without_cloning() {
    let mut write_set = WriteSet::<TursoAllocator>::new();
    let row_versions: RowVersions<TursoAllocator> = Arc::new(RwLock::new(empty_version_chain()));
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));

    write_set.push(row_id, Arc::clone(&row_versions));
    let entries_ptr = write_set.entries.as_ptr();
    let entries_capacity = write_set.entries.capacity();
    let strong_count = Arc::strong_count(&row_versions);

    let transferred = write_set.take();

    assert!(write_set.entries.is_empty());
    assert_eq!(transferred.entries.as_ptr(), entries_ptr);
    assert_eq!(transferred.entries.capacity(), entries_capacity);
    assert_eq!(Arc::strong_count(&row_versions), strong_count);
    assert!(Arc::ptr_eq(&transferred.entries[0].1, &row_versions));
}

#[test]
#[should_panic(expected = "write set cannot be modified unless transaction is active")]
fn aborted_transaction_rejects_write_set_insert() {
    let tx = new_tx(1, 1, TransactionState::Aborted);
    let row_versions: RowVersions<TursoAllocator> = Arc::new(RwLock::new(empty_version_chain()));
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));

    tx.add_to_write_set(row_id, row_versions);
}

#[test]
fn rollback_only_reports_rowids_restored_by_a_delete() {
    let tx_id = 1;
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(666));
    let row = Row::new_table_row(row_id, &[]).unwrap();

    let replacement = RowVersion::new(
        1,
        Some(TxTimestampOrID::TxID(tx_id)),
        None,
        row.data.clone(),
        true,
    );
    assert!(!rollback_restores_rowid(tx_id, &replacement));

    let deleted_existing_row = RowVersion::new(
        2,
        Some(TxTimestampOrID::Timestamp(1)),
        Some(TxTimestampOrID::TxID(tx_id)),
        row.data.clone(),
        true,
    );
    assert!(rollback_restores_rowid(tx_id, &deleted_existing_row));

    let deleted_btree_row = RowVersion::new(
        3,
        None,
        Some(TxTimestampOrID::TxID(tx_id)),
        row.data.clone(),
        true,
    );
    assert!(rollback_restores_rowid(tx_id, &deleted_btree_row));

    let deleted_replacement = RowVersion::new(
        4,
        Some(TxTimestampOrID::TxID(tx_id)),
        Some(TxTimestampOrID::TxID(tx_id)),
        row.data.clone(),
        true,
    );
    assert!(rollback_restores_rowid(tx_id, &deleted_replacement));

    let inserted_then_deleted = RowVersion::new(
        5,
        Some(TxTimestampOrID::TxID(tx_id)),
        Some(TxTimestampOrID::TxID(tx_id)),
        row.data,
        false,
    );
    assert!(!rollback_restores_rowid(tx_id, &inserted_then_deleted));
}

unsafe impl crate::alloc::ApiAllocator for FailOnDemandAlloc {
    fn allocate(
        &self,
        layout: crate::alloc::Layout,
    ) -> std::result::Result<std::ptr::NonNull<[u8]>, crate::alloc::AllocError> {
        if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::alloc::AllocError);
        }
        <crate::alloc::TursoAllocator as crate::alloc::ApiAllocator>::allocate(
            &crate::alloc::TursoAllocator,
            layout,
        )
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: crate::alloc::Layout) {
        unsafe {
            <crate::alloc::TursoAllocator as crate::alloc::ApiAllocator>::deallocate(
                &crate::alloc::TursoAllocator,
                ptr,
                layout,
            )
        }
    }
}

fn test_mvcc_storage(name: &str) -> Arc<dyn crate::mvcc::persistent_storage::DurableStorage> {
    let io = Arc::new(MemoryIO::new());
    let file = io.open_file(name, OpenFlags::Create, false).unwrap();
    Arc::new(crate::mvcc::persistent_storage::Storage::new(
        file, io, None,
    ))
}

#[test]
fn mv_store_skiplist_allocations_are_fallible() {
    let alloc = FailOnDemandAlloc::default();
    alloc.fail_allocations(true);
    let store = MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("mv-store-oom-new.db-log"),
        alloc.clone(),
        false,
    );
    assert!(matches!(store, Err(LimboError::OutOfMemory)));

    alloc.fail_allocations(false);
    let store = MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("mv-store-oom-insert.db-log"),
        alloc.clone(),
        false,
    )
    .unwrap();
    alloc.fail_allocations(true);

    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));
    let row_version = RowVersion {
        id: 1,
        begin: PackedTs::pack(Some(TxTimestampOrID::TxID(1))),
        end: PackedTs::pack(None),
        data: Row::new_table_row(row_id.clone(), &[]).unwrap().data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let result = store.insert_version(row_id, row_version);
    assert!(matches!(result, Err(crate::alloc::TryReserveError)));
    assert!(store.rows.is_empty());
}

#[cfg(nightly)]
#[test]
fn row_payload_allocation_uses_passed_allocator() {
    let alloc = FailOnDemandAlloc::default();
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));

    alloc.fail_allocations(true);
    let result = Row::new_table_row_in(row_id.clone(), &[1, 2, 3], alloc.clone());
    assert!(matches!(result, Err(crate::alloc::TryReserveError)));

    alloc.fail_allocations(false);
    let row = Row::new_table_row_in(row_id, &[1, 2, 3], alloc).unwrap();
    assert_eq!(row.payload(), &[1, 2, 3]);
}

#[cfg(nightly)]
#[test]
fn index_key_payload_allocation_uses_passed_allocator() {
    let alloc = FailOnDemandAlloc::default();
    let record = ImmutableRecord::from_values(&[Value::from_i64(42)], 1).unwrap();
    let record_ref = ImmutableRecordRef::from_bin_record(record.get_payload());
    let index_info = Arc::new(
        IndexInfo::new(
            [crate::types::KeyInfo {
                sort_order: turso_parser::ast::SortOrder::Asc,
                collation: crate::translate::collate::CollationSeq::BINARY,
                nulls_order: None,
            }],
            false,
            1,
            false,
        )
        .unwrap(),
    );

    alloc.fail_allocations(true);
    let result = SortableIndexKey::new_from_payload_in(&record, index_info.clone(), alloc.clone());
    assert!(matches!(result, Err(crate::alloc::TryReserveError)));

    alloc.fail_allocations(false);
    let key = SortableIndexKey::new_from_payload_in(record_ref, index_info, alloc).unwrap();
    assert_eq!(key.key.get_payload(), record.get_payload());
}

#[cfg(nightly)]
#[test]
#[should_panic(expected = "empty tombstone row")]
fn seqcompact_tombstone_payload_uses_store_allocator() {
    let alloc = FailOnDemandAlloc::default();
    let store = MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("mv-store-seqcompact-allocator.db-log"),
        alloc.clone(),
        false,
    )
    .unwrap();
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));
    let versions = store
        .get_or_create_table_row_versions(row_id.clone())
        .unwrap();
    versions.write().try_reserve(1).unwrap();

    alloc.fail_allocations(true);
    store.seqcompact_commit_delete(row_id, 10);
}

#[test]
fn mv_store_insert_allocation_failure_leaves_tx_state_untouched() {
    let alloc = FailOnDemandAlloc::default();
    let store = MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("mv-store-oom-insert-ordering.db-log"),
        alloc.clone(),
        false,
    )
    .unwrap();

    let tx_id = 7;
    let tx = new_tx_in::<FailOnDemandAlloc>(tx_id, 1, TransactionState::Active);
    tx.begin_savepoint();
    store.txs.try_insert(tx_id, tx).unwrap();

    let table_id = MVTableId::from(-2);
    let row_id = RowID::new(table_id, RowKey::Int(42));
    let row = Row::new_table_row(row_id.clone(), &[]).unwrap();
    let allocator = store.get_rowid_allocator(&table_id);

    alloc.fail_allocations(true);
    let result = store.insert(tx_id, row);
    assert!(matches!(result, Err(LimboError::OutOfMemory)));

    assert!(store.rows.get(&row_id).is_none());
    assert_eq!(allocator.max_rowid.load(Ordering::SeqCst), 0);

    let tx = store.txs.get(&tx_id).unwrap();
    let tx = tx.value();
    assert!(tx.write_set.lock().is_empty());

    let savepoints = tx.savepoint_stack.read();
    let savepoint = savepoints.last().unwrap();
    assert!(savepoint.created_table_versions.is_empty());
}

impl MvccTestDb {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(EngineDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        // Enable MVCC via PRAGMA
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let mvcc_store = db.get_mv_store().clone().unwrap();
        Self {
            mvcc_store,
            db,
            conn,
        }
    }

    #[cfg(feature = "conn_raw_api")]
    fn new_with_portable_logical_changes() -> Self {
        let db = Self::new();
        db.conn.set_portable_logical_changes_enabled(true);
        db
    }
}

#[test]
fn mvcc_active_read_tx_blocks_vacuum_gate() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let tx_id = db.mvcc_store.begin_tx(pager).unwrap();

    assert!(matches!(
        db.mvcc_store.try_begin_vacuum_gate(),
        Err(LimboError::Busy)
    ));

    db.mvcc_store.remove_tx(tx_id);
    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_active_write_tx_blocks_vacuum_gate() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let tx_id = db
        .mvcc_store
        .begin_exclusive_tx(pager.clone(), None, &db.conn, None)
        .unwrap();

    assert!(matches!(
        db.mvcc_store.try_begin_vacuum_gate(),
        Err(LimboError::Busy)
    ));

    db.mvcc_store
        .rollback_tx(tx_id, pager, &db.conn, crate::MAIN_DB_ID);
    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_vacuum_gate_blocks_new_read_and_write_tx() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();

    db.mvcc_store.try_begin_vacuum_gate().unwrap();

    assert!(matches!(
        db.mvcc_store.begin_tx(pager.clone()),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        db.mvcc_store
            .begin_exclusive_tx(pager, None, &db.conn, None),
        Err(LimboError::Busy)
    ));

    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_reset_after_vacuum_installs_header_and_rootpages() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    db.conn.demote_to_mvcc_connection();
    db.conn.reparse_schema().unwrap();
    let schema = db.conn.schema.read().clone();
    db.conn.promote_to_regular_connection();
    let table_root = match schema
        .get_table(NamespaceId::ROOT, "t")
        .expect("table t")
        .as_ref()
    {
        Table::BTree(btree) => btree.root_page,
        _ => panic!("expected btree table"),
    };
    let index_root = schema
        .indices_including_backing(NamespaceId::ROOT, "t")
        .next()
        .map(|index| index.root_page)
        .expect("index idx_t_v");

    let mut header = DatabaseHeader::default();
    header.schema_cookie = 77.into();

    db.mvcc_store
        .global_header
        .write()
        .replace(DatabaseHeader::default());
    db.mvcc_store
        .insert_table_id_to_rootpage(MVTableId::from(-999_i64), Some(999));

    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.reset_after_vacuum(header, schema.as_ref());
    db.mvcc_store.release_vacuum_gate();

    assert_eq!(
        db.mvcc_store
            .with_header(|header| header.schema_cookie.get(), None)
            .unwrap(),
        77
    );
    assert_eq!(
        db.mvcc_store
            .current_root_page(&SQLITE_SCHEMA_MVCC_TABLE_ID),
        Some(1)
    );
    assert_eq!(
        db.mvcc_store
            .current_root_page(&MVTableId::from(-(table_root))),
        Some(table_root as u64)
    );
    assert_eq!(
        db.mvcc_store
            .current_root_page(&MVTableId::from(-(index_root))),
        Some(index_root as u64)
    );
    assert!(
        db.mvcc_store
            .table_id_to_rootpage
            .get(&MVTableId::from(-999_i64))
            .is_none(),
        "stale root-page entries must be cleared"
    );
}

#[test]
fn mvcc_passive_gc_retains_until_reader_mark_reaches_materialization() {
    use crate::mvcc::database::WalPos;
    let frame = |f: u64| WalPos {
        checkpoint_seq: 1,
        frame: f,
    };

    // Sole current insert (begin=Ts(5), end=None), materialized at WAL frame 100.
    let stamped_insert = || -> RowVersionChain {
        let mut rv = make_rv(ts(5), None);
        rv.set_materialized_at(frame(100));
        crate::alloc::vec![rv].into()
    };
    // Superseded delete (begin=Ts(3), end=Ts(5)<=lwm), materialized at frame 100.
    let stamped_delete = || -> RowVersionChain {
        let mut rv = make_rv(ts(3), ts(5));
        rv.set_materialized_at(frame(100));
        crate::alloc::vec![rv].into()
    };

    // Reader pinned below the materialization frame: keep both current and delete.
    for mut v in [stamped_insert(), stamped_delete()] {
        let dropped =
            MvStore::<MvccClock>::gc_version_chain(&mut v, 10, 10, true, frame(50), false);
        assert_eq!(
            dropped, 0,
            "version needed by a reader pinned below frame 100 must be kept"
        );
        assert_eq!(v.len(), 1);
    }

    // Readers at the materialization frame: Rule 2 drops deletes; Passive keeps
    // currents unless drop_current_if_in_btree (Truncate).
    let mut deleted = stamped_delete();
    let dropped =
        MvStore::<MvccClock>::gc_version_chain(&mut deleted, 10, 10, true, frame(100), false);
    assert_eq!(
        dropped, 1,
        "materialized + reader-reachable superseded delete must be reclaimed"
    );
    assert!(deleted.is_empty());

    let mut current = stamped_insert();
    let dropped =
        MvStore::<MvccClock>::gc_version_chain(&mut current, 10, 10, true, frame(100), false);
    assert_eq!(
        dropped, 0,
        "Passive keeps the current SkipMap version when drop_current_if_in_btree is false"
    );
    assert_eq!(current.len(), 1);

    let mut current = stamped_insert();
    let dropped =
        MvStore::<MvccClock>::gc_version_chain(&mut current, 10, 10, true, frame(100), true);
    assert_eq!(
        dropped, 0,
        "Passive keeps a live copy while a snapshot is open, even when materialized"
    );
    assert_eq!(current.len(), 1);

    let mut current = stamped_insert();
    let dropped =
        MvStore::<MvccClock>::gc_version_chain(&mut current, u64::MAX, 10, true, frame(100), true);
    assert_eq!(
        dropped, 1,
        "Passive Rule 3 drops a materialized current once no snapshot is open"
    );
    assert!(current.is_empty());

    // Unmaterialized versions are never reclaimed on the Passive path.
    for drop_current_if_in_btree in [false, true] {
        let mut v: RowVersionChain = crate::alloc::vec![make_rv(ts(5), None)].into();
        let dropped = MvStore::<MvccClock>::gc_version_chain(
            &mut v,
            10,
            10,
            true,
            WalPos::STAGED,
            drop_current_if_in_btree,
        );
        assert_eq!(
            dropped, 0,
            "passive GC must not reclaim a version not yet in the B-tree"
        );
        assert_eq!(v.len(), 1);
    }
}

#[test]
fn gc_keeps_sole_current_version_for_reader_that_began_after_lwm_sample() {
    use crate::alloc::{DynAllocator, TursoVecInExt};
    use crate::mvcc::database::WalPos;

    let db = MvccTestDb::new();
    let store = &db.mvcc_store;
    let lwm_sampled_while_idle = store.sample_gc_lwm();
    assert_eq!(lwm_sampled_while_idle, u64::MAX);

    let reader_id: TxID = 9_000_100;
    store.txs.insert(
        reader_id,
        new_tx_in::<DynAllocator>(reader_id, 100, TransactionState::Active),
    );

    let materialized_at = WalPos {
        checkpoint_seq: 1,
        frame: 100,
    };
    let mut current = make_rv(ts(5), None);
    current.set_materialized_at(materialized_at);
    let mut chain =
        <RowVersionChain<DynAllocator> as TursoVecInExt<RowVersion, DynAllocator>>::new_in(
            DynAllocator::default(),
        );
    chain.push(current);

    let dropped = store.gc_chain_now(
        &mut chain,
        lwm_sampled_while_idle,
        10,
        materialized_at,
        true,
    );
    assert_eq!(
        dropped, 0,
        "GC dropped the only current version while a reader that began after the LWM sample was open"
    );
    assert_eq!(chain.len(), 1);
}

/// After Passive WAL backfill + DB fsync, `BEGIN CONCURRENT` on another
/// connection must not Busy. Publishing `nbackfills == max_frame` while
/// exclusive `read_locks[0]` is still held forces every new begin onto slot 0.
#[test]
fn mvcc_passive_begin_concurrent_after_backfill_does_not_busy() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    for i in 0..50 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'seed')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let pager = conn.pager.load().clone();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mv,
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    let mut reached_tail = false;
    for _ in 0..50_000 {
        match checkpoint_sm.state() {
            CheckpointState::TruncateLogicalLog
            | CheckpointState::FsyncLogicalLog
            | CheckpointState::TruncateWal
            | CheckpointState::GcTableRows { .. }
            | CheckpointState::GcIndexRows { .. } => {
                reached_tail = true;
                break;
            }
            _ => {}
        }
        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finished before the post-backfill tail")
            }
        }
    }
    assert!(
        reached_tail,
        "passive checkpoint must reach the post-backfill tail with frames to backfill"
    );

    {
        let database = db.get_db();
        let shared = database.shared_wal.read();
        let max_frame = shared.metadata.max_frame.load(Ordering::SeqCst);
        let nbackfills = shared.metadata.nbackfills.load(Ordering::SeqCst);
        assert!(
            max_frame > 0,
            "setup must leave WAL frames to backfill, max_frame={max_frame}"
        );
        if max_frame == nbackfills {
            let slot0_shared = shared.runtime.read_locks[0].read();
            if slot0_shared {
                shared.runtime.read_locks[0].unlock();
            }
            assert!(
                slot0_shared,
                "nbackfills==max_frame ({max_frame}) must not overlap exclusive read_locks[0]"
            );
        }
    }

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").expect(
        "BEGIN CONCURRENT must succeed after Passive backfill while the checkpointer still runs",
    );
    writer
        .execute("INSERT INTO t VALUES (1000, 'during')")
        .unwrap();
    writer.execute("COMMIT").unwrap();

    loop {
        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => break,
        }
    }

    let wal_bf = pager.wal_backfill_frame().unwrap_or(0);
    assert!(
        wal_bf > 0,
        "passive checkpoint must still publish nbackfills, got {wal_bf}"
    );
}

#[test]
fn mvcc_reset_after_vacuum_clears_checkpointed_empty_version_buckets() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();

    db.conn
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    db.conn
        .execute("UPDATE t SET v = 'z' WHERE id = 1")
        .unwrap();
    db.conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Checkpoint-time GC removes the buckets it empties (it runs under the
    // blocking lock), so recreate the condition VACUUM reset must handle:
    // lazy background GC empties chains but leaves the buckets in the maps.
    db.conn.execute("BEGIN").unwrap();
    db.conn.execute("INSERT INTO t VALUES (4, 'd')").unwrap();
    db.conn.execute("ROLLBACK").unwrap();
    db.mvcc_store.drop_unused_row_versions();

    let empty_row_ids = db
        .mvcc_store
        .rows
        .iter()
        .filter(|entry| entry.value().read().is_empty())
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    let empty_index_ids = db
        .mvcc_store
        .index_rows
        .iter()
        .filter(|entry| {
            entry
                .value()
                .iter()
                .all(|row_entry| row_entry.value().read().is_empty())
        })
        .map(|entry| *entry.key())
        .collect::<Vec<_>>();
    assert!(
        !empty_row_ids.is_empty(),
        "lazy GC should leave empty table row buckets before VACUUM reset"
    );
    assert!(
        !empty_index_ids.is_empty(),
        "lazy GC should leave empty index buckets before VACUUM reset"
    );

    db.conn.demote_to_mvcc_connection();
    db.conn.reparse_schema().unwrap();
    let schema = db.conn.current_schema();
    db.conn.promote_to_regular_connection();

    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store
        .reset_after_vacuum(DatabaseHeader::default(), schema.as_ref());
    db.mvcc_store.release_vacuum_gate();

    for row_id in empty_row_ids {
        assert!(
            db.mvcc_store.rows.get(&row_id).is_none(),
            "empty table row buckets must be cleared across VACUUM reset"
        );
    }
    for index_id in empty_index_ids {
        assert!(
            db.mvcc_store.index_rows.get(&index_id).is_none(),
            "empty index buckets must be cleared across VACUUM reset"
        );
    }
}

impl MvccTestDbNoConn {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let opts = DatabaseOpts::new();
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::default(),
            opts,
            None,
            Arc::new(EngineDialect),
        )
        .unwrap();
        // Enable MVCC via PRAGMA
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: None,
            opts,
            enc_opts: None,
            _temp_dir: None,
        }
    }

    /// Opens a database with a file
    pub fn new_with_random_db() -> Self {
        Self::new_with_random_db_with_opts(DatabaseOpts::new())
    }

    /// Opens a database with the experimental passive (non-blocking)
    /// auto-checkpoint enabled. Used by the passive-checkpoint-specific tests.
    pub fn new_with_random_db_passive() -> Self {
        Self::new_with_random_db_with_opts(
            DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
        )
    }

    /// Opens a database with a file and the requested options.
    pub fn new_with_random_db_with_opts(opts: DatabaseOpts) -> Self {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("test_{}", rand::random::<u64>()));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let io = Arc::new(PlatformIO::new().unwrap());
        println!("path: {}", path.as_os_str().to_str().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.as_os_str().to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
            Arc::new(EngineDialect),
        )
        .unwrap();
        // Enable MVCC via PRAGMA
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            enc_opts: None,
            _temp_dir: Some(temp_dir),
        }
    }

    /// Opens a file-backed encrypted database with the given hex key.
    pub fn new_encrypted(hex_key: &str) -> Self {
        let opts = DatabaseOpts::new().with_encryption(true);
        let enc_opts = crate::EncryptionOpts {
            cipher: "aes256gcm".to_string(),
            hexkey: hex_key.to_string(),
        };
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.as_os_str().to_str().unwrap(),
            OpenFlags::default(),
            opts,
            Some(enc_opts.clone()),
            Arc::new(EngineDialect),
        )
        .unwrap();
        let encryption_key = EncryptionKey::from_hex_string(hex_key).unwrap();
        let conn = db
            .connect(
                ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(encryption_key)),
            )
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            enc_opts: Some(enc_opts),
            _temp_dir: Some(temp_dir),
        }
    }

    /// Restarts the database, make sure there is no connection to the database open before calling this!
    pub fn restart(&mut self) {
        self.restart_result().unwrap();
    }

    /// Like `restart`, but returns the error instead of panicking.
    /// Useful for testing wrong-key scenarios.
    pub fn restart_result(&mut self) -> crate::Result<()> {
        // Remove this database from the database manager in order to force restart.
        // If not, we will load the same database instance again.
        let path = self.path.as_ref().unwrap();
        remove_file_from_database_registry(path);
        // Now open again.
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path,
            OpenFlags::default(),
            self.opts,
            self.enc_opts.clone(),
            Arc::new(EngineDialect),
        )?;
        self.db.replace(db);
        Ok(())
    }

    /// Asumes there is a database open
    pub fn get_db(&self) -> Arc<Database> {
        self.db.as_ref().unwrap().clone()
    }

    pub fn connect(&self) -> Arc<Connection> {
        let enc_key = self
            .enc_opts
            .as_ref()
            .map(|e| EncryptionKey::from_hex_string(&e.hexkey).unwrap());
        self.get_db()
            .connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(enc_key))
            .unwrap()
    }

    pub fn get_mvcc_store(&self) -> Arc<crate::MvStore> {
        self.get_db().get_mv_store().clone().unwrap()
    }
}

pub(crate) fn generate_simple_string_row(table_id: MVTableId, id: i64, data: &str) -> Row {
    let record =
        ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1).unwrap();
    Row::new_table_row(RowID::new(table_id, RowKey::Int(id)), record.as_blob()).unwrap()
}

pub(crate) fn generate_simple_string_record(data: &str) -> ImmutableRecord {
    ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1).unwrap()
}

fn advance_checkpoint_until_wal_has_commit_frame(
    mvcc_store: Arc<crate::MvStore>,
    conn: &Arc<Connection>,
) {
    let pager = conn.pager.load().clone();
    let initial_wal_max_frame = pager.wal_max_frame().expect("mvcc mode requires wal");
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store,
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    for _ in 0..10_000 {
        if pager.wal_max_frame().expect("mvcc mode requires wal") > initial_wal_max_frame {
            return;
        }

        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finalized before WAL had committed frames")
            }
        }
    }

    panic!("checkpoint did not produce committed WAL frame in bounded steps");
}

fn overwrite_log_header_byte(path: &str, offset: u64, value: u8) {
    let log_path = std::path::Path::new(path).with_extension("db-log");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(log_path)
        .unwrap();
    use std::io::{Seek, SeekFrom, Write};
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[value]).unwrap();
    file.sync_all().unwrap();
}

fn overwrite_file_with_junk(path: &std::path::Path, size: usize, byte: u8) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    let portable_changes = vec![byte; size];
    use std::io::Write;
    file.write_all(&portable_changes).unwrap();
    file.sync_all().unwrap();
}

fn wal_path_for_db(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{path}-wal"))
}

fn force_close_for_artifact_tamper(db: &mut MvccTestDbNoConn) {
    db.db.take();
    remove_file_from_database_registry(db.path.as_ref().unwrap());
}

fn read_db_page_size(path: &str) -> usize {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut header = [0u8; 100];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut header).unwrap();
    let raw = u16::from_be_bytes([header[16], header[17]]);
    if raw == 1 {
        65536
    } else {
        raw as usize
    }
}

fn page_file_offset(page_no: u32, page_size: usize) -> u64 {
    (page_no as u64 - 1) * page_size as u64
}

fn page_header_offset(page_no: u32) -> usize {
    if page_no == 1 {
        100
    } else {
        0
    }
}

fn read_db_page(path: &str, page_no: u32, page_size: usize) -> Vec<u8> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut page = vec![0u8; page_size];
    file.seek(SeekFrom::Start(page_file_offset(page_no, page_size)))
        .unwrap();
    file.read_exact(&mut page).unwrap();
    page
}

fn write_db_page(path: &str, page_no: u32, page_size: usize, page: &[u8]) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(page_file_offset(page_no, page_size)))
        .unwrap();
    file.write_all(page).unwrap();
    file.sync_all().unwrap();
}

#[derive(Debug, Clone, Copy)]
struct TableLeafCellLoc {
    cell_offset: usize,
    payload_varint_len: usize,
    payload_len: usize,
    payload_offset: usize,
}

fn table_leaf_cell_locs(page: &[u8], page_no: u32) -> Vec<TableLeafCellLoc> {
    let hdr_off = page_header_offset(page_no);
    assert_eq!(page[hdr_off], 0x0D, "expected table-leaf page type");
    let cell_count = u16::from_be_bytes([page[hdr_off + 3], page[hdr_off + 4]]) as usize;
    let ptr_base = hdr_off + 8;
    let mut locs = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let ptr_off = ptr_base + i * 2;
        let cell_ptr = u16::from_be_bytes([page[ptr_off], page[ptr_off + 1]]) as usize;
        let (payload_len_u64, payload_varint_len) = read_varint(&page[cell_ptr..]).unwrap();
        let payload_len = payload_len_u64 as usize;
        let (_, rowid_varint_len) = read_varint(&page[cell_ptr + payload_varint_len..]).unwrap();
        let payload_offset = cell_ptr + payload_varint_len + rowid_varint_len;
        locs.push(TableLeafCellLoc {
            cell_offset: cell_ptr,
            payload_varint_len,
            payload_len,
            payload_offset,
        });
    }
    locs
}

fn rewrite_table_leaf_cell_payload(page: &mut [u8], loc: TableLeafCellLoc, new_payload: &[u8]) {
    assert!(
        new_payload.len() <= loc.payload_len,
        "new payload {} exceeds existing payload {}",
        new_payload.len(),
        loc.payload_len
    );
    let mut varint_buf = [0u8; 9];
    let n = write_varint(&mut varint_buf, new_payload.len() as u64);
    assert_eq!(
        n, loc.payload_varint_len,
        "payload varint length changed; in-place rewrite is unsafe"
    );
    page[loc.cell_offset..loc.cell_offset + n].copy_from_slice(&varint_buf[..n]);
    page[loc.payload_offset..loc.payload_offset + new_payload.len()].copy_from_slice(new_payload);
    if new_payload.len() < loc.payload_len {
        page[loc.payload_offset + new_payload.len()..loc.payload_offset + loc.payload_len].fill(0);
    }
}

fn metadata_root_page(conn: &Arc<Connection>) -> u32 {
    let rows = get_rows(
        conn,
        "SELECT rootpage FROM sqlite_schema
         WHERE type = 'table' AND name = '__turso_internal_mvcc_meta'",
    );
    assert_eq!(rows.len(), 1, "expected exactly one metadata table row");
    rows[0][0].as_int().unwrap() as u32
}

fn tamper_db_metadata_row_value_by_key(
    db_path: &str,
    metadata_root_page: u32,
    target_key: &str,
    new_value: i64,
) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    let mut updated = false;
    for loc in table_leaf_cell_locs(&page, metadata_root_page) {
        let payload = &page[loc.payload_offset..loc.payload_offset + loc.payload_len];
        let record = ImmutableRecordRef::from_bin_record(payload);
        let key = record
            .get_value_opt(0)
            .expect("metadata key column missing");
        let ValueRef::Text(key) = key else {
            panic!("metadata key must be text");
        };
        if key.as_str() != target_key {
            continue;
        }
        let new_record = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new(target_key.to_string())),
                Value::from_i64(new_value),
            ],
            2,
        )
        .unwrap();
        rewrite_table_leaf_cell_payload(&mut page, loc, new_record.as_blob());
        updated = true;
    }
    assert!(updated, "expected metadata key {target_key} to exist");
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn rewrite_wal_frames_as_non_commit(path: &std::path::Path) {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() >= WAL_HEADER_SIZE);

    let header = WalHeader {
        magic: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        file_format: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        page_size: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        checkpoint_seq: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        salt_1: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        salt_2: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        checksum_1: u32::from_be_bytes(bytes[24..28].try_into().unwrap()),
        checksum_2: u32::from_be_bytes(bytes[28..32].try_into().unwrap()),
    };
    let use_native_endian = cfg!(target_endian = "big") == ((header.magic & 1) != 0);
    let frame_size = WAL_FRAME_HEADER_SIZE + header.page_size as usize;
    let mut frame_offset = WAL_HEADER_SIZE;
    let mut prev_checksums = (header.checksum_1, header.checksum_2);

    while frame_offset + frame_size <= bytes.len() {
        let frame = &mut bytes[frame_offset..frame_offset + frame_size];
        frame[4..8].copy_from_slice(&0u32.to_be_bytes());
        let header_checksum =
            checksum_wal(&frame[0..8], &header, prev_checksums, use_native_endian);
        let final_checksum = checksum_wal(
            &frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize],
            &header,
            header_checksum,
            use_native_endian,
        );
        frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
        frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());
        prev_checksums = final_checksum;
        frame_offset += frame_size;
    }

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
}

/// What this test checks: Recovery replays a transaction frame in which data ops precede the
/// sqlite_schema op that registers their table, and the recovered table is queryable afterwards.
/// Why this matters: Logs written before the schema-first frame serialization fix (#7218) sorted
/// the whole write set, so a same-transaction CREATE TABLE + INSERT could serialize the data row
/// before the schema row. Recovery must stay tolerant to logs written by those older versions.
#[test]
fn test_recovery_replays_schema_op_after_data_op_in_frame() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();
    let io = db.get_db().io.clone();
    let file = mvcc_store.get_logical_log_file();

    let c = file.truncate(0, Completion::new_trunc(|_| {})).unwrap();
    io.wait_for_completion(c).unwrap();

    // A CREATE TABLE t2 + INSERT INTO t2 transaction as serialized by a pre-#7218 writer:
    // the data row comes before the sqlite_schema row that registers its table_id.
    let table_id = MVTableId::from(-999);
    let commit_ts = 1u64 << 40;
    let data_version = RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(commit_ts))),
        end: crate::mvcc::database::PackedTs::pack(None),
        data: generate_simple_string_row(table_id, 1, "data").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let schema_record = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new("table".to_string())),
            Value::Text(Text::new("t2".to_string())),
            Value::Text(Text::new("t2".to_string())),
            Value::from_i64(i64::from(table_id)),
            Value::Text(Text::new("CREATE TABLE t2 (v TEXT)".to_string())),
        ],
        5,
    )
    .unwrap();
    let schema_version = RowVersion {
        id: 2,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(commit_ts))),
        end: crate::mvcc::database::PackedTs::pack(None),
        data: Row::new_table_row(
            RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(2)),
            schema_record.as_blob(),
        )
        .unwrap()
        .data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let tx = LogRecord::for_test(
        commit_ts,
        &[
            (RowID::new(table_id, RowKey::Int(1)), data_version),
            (
                RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(2)),
                schema_version,
            ),
        ],
        None,
    );

    let mut log = LogicalLog::new(file.clone(), io.clone(), None);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();
    drop(log);
    drop(file);
    drop(mvcc_store);

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT v FROM t2");
    assert_eq!(rows, vec![vec![Value::Text(Text::new("data".to_string()))]]);
}

#[test]
fn abandoned_journal_mode_checkpoint_releases_pager_transaction_and_lock() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'one')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let injector = FixedYieldInjector::new([CheckpointYieldPoint::BeforePagerCommit.point()]);
    conn.set_yield_injector(Some(injector.clone()));
    let mut stmt = conn.prepare("PRAGMA journal_mode = 'wal'").unwrap();
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            crate::StepResult::Yield if injector.is_empty() => break,
            crate::StepResult::IO => stmt.get_pager().io.step().unwrap(),
            crate::StepResult::Yield => {}
            other => {
                panic!("journal-mode checkpoint completed before pager-commit yield: {other:?}")
            }
        }
    }
    assert!(injector.is_empty(), "checkpoint did not reach pager commit");

    stmt.reset().unwrap();
    conn.set_yield_injector(None);

    let tx_state = conn.get_tx_state();
    let checkpoint_lock_released = mvcc_store.blocking_checkpoint_lock.write();
    if checkpoint_lock_released {
        mvcc_store.blocking_checkpoint_lock.unlock();
    }
    assert!(
        tx_state == crate::connection::TransactionState::None && checkpoint_lock_released,
        "reset left journal-mode checkpoint resources owned: \
         transaction_state={tx_state:?}, checkpoint_lock_released={checkpoint_lock_released}"
    );
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

/// `checkpoint_snapshot_ts` must clamp the published checkpoint boundary below any
/// in-flight (Preparing) commit. `last_committed_tx_ts` is a fetch_max high-water mark,
/// so a commit that assigned a LOWER end_ts and is still Preparing (commits finalize out
/// of timestamp order) sits below it; a checkpoint that published a boundary above that
/// end_ts and skipped the commit (not yet Committed) would lose it on reopen. Regression
/// for the boundary-straddle data-loss path (review finding #1).
#[test]
fn test_checkpoint_snapshot_ts_clamps_below_inflight_preparing() {
    let db = MvccTestDb::new();
    let store = &db.mvcc_store;
    let pager = db.conn.pager.load().clone();
    let inflight = store.begin_tx(pager).unwrap();

    // Out-of-order finalize: a higher-ts commit advanced the watermark to 1000 while
    // `inflight` is still Preparing at the lower end_ts 500.
    store.last_committed_tx_ts.store(1000, Ordering::SeqCst);
    store
        .txs
        .get(&inflight)
        .unwrap()
        .value()
        .state
        .store(TransactionState::Preparing(500));
    assert_eq!(
        store.checkpoint_snapshot_ts(),
        499,
        "boundary must clamp below in-flight Preparing(500), not reach last_committed=1000"
    );

    // Once it commits, the clamp lifts back to the watermark.
    store
        .txs
        .get(&inflight)
        .unwrap()
        .value()
        .state
        .store(TransactionState::Committed(500));
    assert_eq!(store.checkpoint_snapshot_ts(), 1000);
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_rejects_torn_log_header_with_committed_wal() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'y')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    overwrite_log_header_byte(&db_path, 0, 0x00);

    remove_file_from_database_registry(&db_path);

    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path, Arc::new(EngineDialect)) {
        Ok(db) => match db.connect(ConnectOpts::new(SessionRole::Superuser)) {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert!(
        wal_len > 0,
        "failed bootstrap must not truncate WAL before header validation"
    );
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_rejects_corrupt_log_header_without_wal() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    }

    overwrite_log_header_byte(&db_path, 0, 0x00);

    {
        let wal_path = wal_path_for_db(&db_path);
        let _ = std::fs::remove_file(&wal_path);
        overwrite_file_with_junk(&wal_path, 0, 0x00);
    }

    remove_file_from_database_registry(&db_path);

    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path, Arc::new(EngineDialect)) {
        Ok(db) => match db.connect(ConnectOpts::new(SessionRole::Superuser)) {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
}

/// What this test checks: WAL frames without a commit marker are treated as non-committed tail and ignored.
/// Why this matters: Recovery must preserve availability by discarding invalid WAL tail bytes instead of failing startup.
#[test]
fn test_bootstrap_ignores_wal_frames_without_commit_marker() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    rewrite_wal_frames_as_non_commit(&wal_path);
    remove_file_from_database_registry(&db_path);
    let io = Arc::new(PlatformIO::new().unwrap());
    let db2 =
        Database::open_file(io, &db_path, Arc::new(EngineDialect)).expect("open should succeed");
    let conn2 = db2
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .expect("connect should succeed");
    let rows = get_rows(&conn2, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "x");
}

#[test]
fn test_ddl_upgrade_sets_cached_tx_mode_to_write() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mv_store = db.get_mvcc_store();

    conn.execute("BEGIN CONCURRENT").unwrap();
    // BEGIN CONCURRENT is deferred: no MVCC tx is open yet at BEGIN time.
    assert!(
        conn.get_mv_tx().is_none(),
        "no MVCC tx before first statement"
    );

    conn.execute("CREATE TABLE u(y INTEGER)").unwrap();
    let (tx_id_after, mode_after) = conn.get_mv_tx().expect("DDL must materialize the MVCC tx");
    assert_eq!(mode_after, TransactionMode::Write);
    assert!(mv_store.is_exclusive_tx(&tx_id_after));

    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn test_analyze_in_concurrent_tx_upgrades_to_exclusive() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let other = db.connect();
    let mv_store = db.get_mvcc_store();

    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    other.execute("BEGIN CONCURRENT").unwrap();
    other.execute("INSERT INTO t VALUES (2)").unwrap();

    conn.execute("ANALYZE").unwrap();
    let (tx_id, mode) = conn
        .get_mv_tx()
        .expect("ANALYZE must keep the outer tx open");
    assert_eq!(mode, TransactionMode::Write);
    assert!(mv_store.is_exclusive_tx(&tx_id));

    other
        .execute("COMMIT")
        .expect_err("other concurrent commit must fail while ANALYZE holds exclusive");

    conn.execute("COMMIT").unwrap();
    let rows = get_rows(&conn, "SELECT count(*) FROM sqlite_stat1");
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0][0].as_int().unwrap() > 0,
        "ANALYZE must write sqlite_stat1"
    );
}

#[test]
fn test_ddl_on_attached_db_in_concurrent_tx_upgrades() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir.path().join("aux.db");

    let conn = db.connect();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux");
    conn.execute("CREATE TABLE aux.t (x INTEGER)").unwrap();

    let other = db.connect();
    drive_attach(&other, aux_path.to_str().unwrap(), "aux");

    conn.execute("BEGIN CONCURRENT").unwrap();
    other.execute("BEGIN CONCURRENT").unwrap();
    other.execute("INSERT INTO aux.t VALUES (1)").unwrap();

    conn.execute("CREATE TABLE aux.u (y INTEGER)").unwrap();

    let aux_db_id = conn.get_database_id_by_name("aux").unwrap();
    let aux_mv_store = conn
        .mv_store_for_db(aux_db_id)
        .expect("attached aux database must be MVCC");
    let (aux_tx_id, aux_mode) = conn
        .get_mv_tx_for_db(aux_db_id)
        .expect("attached MVCC tx must be open after DDL");
    assert_eq!(aux_mode, TransactionMode::Write);
    assert!(
        aux_mv_store.is_exclusive_tx(&aux_tx_id),
        "schema write on attached DB must upgrade that DB to exclusive"
    );

    other
        .execute("COMMIT")
        .expect_err("other concurrent commit on attached DB must fail while exclusive");

    conn.execute("COMMIT").unwrap();
    let rows = get_rows(&conn, "SELECT count(*) FROM aux.u");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 0);
}

fn step_until_checkpoint_before_pager_commit_yield(
    stmt: &mut crate::Statement,
    injector: &FixedYieldInjector,
    io: &Arc<dyn IO>,
    context: &str,
) {
    for _ in 0..100_000 {
        match stmt.step().unwrap() {
            crate::StepResult::Yield if injector.is_empty() => return,
            crate::StepResult::IO | crate::StepResult::Yield => io.step().unwrap(),
            crate::StepResult::Row => {}
            crate::StepResult::Done => {
                panic!("{context} completed before BeforePagerCommit yield")
            }
            other => panic!("unexpected {context} step before yield: {other:?}"),
        }
    }
    panic!("checkpoint did not reach its guarded write phase");
}

fn step_until_done(stmt: &mut crate::Statement, io: &Arc<dyn IO>, context: &str) {
    for _ in 0..100_000 {
        match stmt.step().unwrap() {
            crate::StepResult::Done => return,
            crate::StepResult::Row => {}
            crate::StepResult::IO | crate::StepResult::Yield => io.step().unwrap(),
            other => panic!("unexpected {context} step after yield: {other:?}"),
        }
    }
    panic!("{context} did not finish");
}

#[test]
fn attached_reader_does_not_pin_read_mark_until_checkpoint_gate_is_available() {
    use crate::StepResult;

    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir
        .path()
        .join(format!("aux_{}.db", rand::random::<u64>()));
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    let writer = db.connect();
    writer
        .execute(format!("ATTACH '{aux_path_str}' AS aux"))
        .unwrap();
    writer
        .execute("PRAGMA aux.journal_mode = 'experimental_mvcc'")
        .unwrap();
    let aux_db_id = writer.get_database_id_by_name("aux").unwrap();
    let aux_mv_store = writer
        .mv_store_for_db(aux_db_id)
        .expect("attached aux database must be MVCC");
    aux_mv_store.set_checkpoint_threshold(-1);
    writer
        .execute("CREATE TABLE aux.t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();
    writer
        .execute("INSERT INTO aux.t VALUES (1, 'seed')")
        .unwrap();
    aux_mv_store.set_checkpoint_threshold(0);

    let checkpoint_io = writer
        .get_pager_from_database_index(&aux_db_id)
        .unwrap()
        .io
        .clone();
    let checkpoint_injector =
        FixedYieldInjector::new([CheckpointYieldPoint::BeforePagerCommit.point()]);
    writer.set_yield_injector(Some(checkpoint_injector.clone()));
    let mut checkpointing_insert = writer
        .prepare("INSERT INTO aux.t VALUES (2, 'checkpointed')")
        .unwrap();
    step_until_checkpoint_before_pager_commit_yield(
        &mut checkpointing_insert,
        &checkpoint_injector,
        &checkpoint_io,
        "attached insert",
    );

    let reader = db.connect();
    reader
        .execute(format!("ATTACH '{aux_path_str}' AS aux"))
        .unwrap();
    let reader_aux_db_id = reader.get_database_id_by_name("aux").unwrap();
    let reader_aux_pager = reader
        .get_pager_from_database_index(&reader_aux_db_id)
        .unwrap();
    let read_mark_probe = FixedYieldInjector::new([TransactionYieldPoint::BeforeMvccBegin.point()]);
    reader.set_yield_injector(Some(read_mark_probe.clone()));
    let mut read = reader.prepare("SELECT v FROM aux.t WHERE id = 1").unwrap();
    assert!(
        matches!(read.step().unwrap(), StepResult::Yield) && read_mark_probe.is_empty(),
        "attached reader should yield before opening its MVCC transaction"
    );
    assert!(
        !reader_aux_pager.holds_read_lock(),
        "attached reader must not pin a pager read mark before MVCC begin is allowed"
    );

    match read.step() {
        Ok(StepResult::Busy) | Err(LimboError::Busy) => {}
        other => panic!("attached reader should be Busy before pinning a read mark, got {other:?}"),
    }
    assert!(
        !reader_aux_pager.holds_read_lock(),
        "Busy attached reader must not leave a pager read mark pinned"
    );
    drop(read);
    reader.set_yield_injector(None);

    writer.set_yield_injector(None);
    step_until_done(&mut checkpointing_insert, &checkpoint_io, "attached insert");
}

#[test]
fn drop_attached_checkpoint_paused_at_before_pager_commit() {
    let (db, writer, aux_path_str, _aux_dir) =
        drop_attached_statement_paused_at_checkpoint_before_pager_commit(
            "INSERT INTO aux.t VALUES (2, 'checkpointed')",
        );
    writer
        .execute("PRAGMA aux.wal_checkpoint(TRUNCATE)")
        .unwrap();

    let reader = db.connect();
    reader
        .execute(format!("ATTACH '{aux_path_str}' AS aux"))
        .unwrap();
    assert_eq!(
        get_rows(&reader, "SELECT id, v FROM aux.t ORDER BY id"),
        vec![
            vec![Value::from_i64(1), Value::build_text("seed")],
            vec![Value::from_i64(2), Value::build_text("checkpointed")],
        ]
    );
}

#[test]
fn drop_attached_ddl_checkpoint_paused_at_before_pager_commit_keeps_new_schema() {
    let (db, writer, aux_path_str, _aux_dir) =
        drop_attached_statement_paused_at_checkpoint_before_pager_commit("CREATE TABLE aux.t2(x)");
    assert_eq!(
        get_rows(&writer, "SELECT id, v FROM aux.t ORDER BY id"),
        vec![vec![Value::from_i64(1), Value::build_text("seed")]]
    );
    assert!(get_rows(&writer, "SELECT x FROM aux.t2").is_empty());
    writer.execute("INSERT INTO aux.t2 VALUES (7)").unwrap();
    writer
        .execute("PRAGMA aux.wal_checkpoint(TRUNCATE)")
        .unwrap();

    let reader = db.connect();
    reader
        .execute(format!("ATTACH '{aux_path_str}' AS aux"))
        .unwrap();
    for conn in [&writer, &reader] {
        assert_eq!(
            get_rows(conn, "SELECT id, v FROM aux.t ORDER BY id"),
            vec![vec![Value::from_i64(1), Value::build_text("seed")]]
        );
        assert_eq!(
            get_rows(conn, "SELECT x FROM aux.t2"),
            vec![vec![Value::from_i64(7)]]
        );
    }
}

fn drop_attached_statement_paused_at_checkpoint_before_pager_commit(
    sql: &str,
) -> (MvccTestDbNoConn, Arc<Connection>, String, tempfile::TempDir) {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir
        .path()
        .join(format!("aux_{}.db", rand::random::<u64>()));
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    let writer = db.connect();
    writer
        .execute(format!("ATTACH '{aux_path_str}' AS aux"))
        .unwrap();
    writer
        .execute("PRAGMA aux.journal_mode = 'experimental_mvcc'")
        .unwrap();
    let aux_db_id = writer.get_database_id_by_name("aux").unwrap();
    let aux_mv_store = writer
        .mv_store_for_db(aux_db_id)
        .expect("attached aux database must be MVCC");
    aux_mv_store.set_checkpoint_threshold(-1);
    writer
        .execute("CREATE TABLE aux.t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    writer
        .execute("INSERT INTO aux.t VALUES (1, 'seed')")
        .unwrap();
    aux_mv_store.set_checkpoint_threshold(0);

    let aux_pager = writer.get_pager_from_database_index(&aux_db_id).unwrap();
    let checkpoint_io = aux_pager.io.clone();
    let injector = FixedYieldInjector::new([CheckpointYieldPoint::BeforePagerCommit.point()]);
    writer.set_yield_injector(Some(injector.clone()));
    let mut stmt = writer.prepare(sql).unwrap();
    step_until_checkpoint_before_pager_commit_yield(&mut stmt, &injector, &checkpoint_io, sql);
    assert!(aux_pager.holds_write_lock());
    drop(stmt);
    writer.set_yield_injector(None);

    assert!(
        !aux_pager.holds_write_lock(),
        "dropped checkpoint must release the aux write lock"
    );
    (db, writer, aux_path_str, aux_dir)
}

/// Replay gate uses metadata boundary and never applies frames at or below it.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_12_replay_gate_skips_at_or_below_metadata_boundary() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let boundary = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let root_page = metadata_root_page(&conn);
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        let ts3 = db.get_mvcc_store().watermarks().last_committed_tx_ts;
        drop(conn);
        force_close_for_artifact_tamper(&mut db);
        tamper_db_metadata_row_value_by_key(
            &db_path,
            root_page,
            MVCC_META_KEY_PERSISTENT_TX_TS_MAX,
            ts3 as i64,
        );
        ts3
    };

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0][0].as_int().unwrap() as u64, boundary);
}

use crate::mvcc::cursor::MvccLazyCursor;
use crate::mvcc::database::{MvStore, Row, RowID};
use crate::schema::IndexColumn;
use crate::security::roles::SessionRole;
use crate::types::Text;
use crate::MemoryIO;
use crate::Value;
use crate::ValueRef;
use crate::{Database, StepResult};
// Simple atomic clock implementation for testing

fn setup_test_db() -> (MvccTestDb, u64, MVTableId, i64) {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE mvcc_lazy_gap_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'mvcc_lazy_gap_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let btree_root_page = root_page.abs();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let test_rows = [
        (5, "row5"),
        (10, "row10"),
        (15, "row15"),
        (20, "row20"),
        (30, "row30"),
    ];

    for (row_id, data) in test_rows.iter() {
        let id = RowID::new(table_id, RowKey::Int(*row_id));
        let record =
            ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1).unwrap();
        let row = Row::new_table_row(id, record.as_blob()).unwrap();
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    (db, tx_id, table_id, btree_root_page)
}

fn setup_lazy_db(initial_keys: &[i64]) -> (MvccTestDb, u64, MVTableId, i64) {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE mvcc_lazy_basic_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'mvcc_lazy_basic_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let btree_root_page = root_page.abs();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    for i in initial_keys {
        let id = RowID::new(table_id, RowKey::Int(*i));
        let data = format!("row{i}");
        let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data))], 1).unwrap();
        let row = Row::new_table_row(id, record.as_blob()).unwrap();
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    (db, tx_id, table_id, btree_root_page)
}

#[test]
fn test_mvcc_cursor_next_yields_with_injected_yield() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE cursor_yield_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'cursor_yield_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.conn.set_yield_injector(Some(FixedYieldInjector::new([
        CursorYieldPoint::NextStart.point()
    ])));

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            root_page.abs(),
            1,
        )),
    )
    .unwrap();

    let saw_yield = matches!(
        cursor.next().unwrap(),
        IOResult::IO(io) if io.is_explicit_yield()
    );
    db.mvcc_store
        .rollback_tx(tx_id, db.conn.pager.load().clone(), db.conn.as_ref(), 0);

    assert!(
        saw_yield,
        "MVCC cursor should inject an explicit yield on the first next() transition",
    );
}

pub(crate) fn commit_tx(
    mv_store: Arc<crate::MvStore>,
    conn: &Arc<Connection>,
    tx_id: u64,
) -> Result<()> {
    let mut sm = mv_store.commit_tx(tx_id, conn, crate::MAIN_DB_ID).unwrap();
    // TODO: sync IO hack
    loop {
        let res = sm.step(&mv_store)?;
        match res {
            IOResult::IO(io) => {
                io.wait(conn.db.io.as_ref())?;
            }
            IOResult::Done(_) => break,
        }
    }
    assert!(sm.is_finalized());
    Ok(())
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_lazy_scan_cursor_basic() {
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 3, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Check first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_lazy_scan_cursor_with_gaps() {
    let (db, tx_id, table_id, btree_root_page) = setup_test_db();

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Check first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 5);

    // Test moving forward and checking IDs
    let expected_ids = [5, 10, 15, 20, 30];
    let mut index = 0;

    let IOResult::Done(rowid) = cursor.rowid().unwrap() else {
        unreachable!();
    };
    let rowid = rowid.unwrap();
    assert_eq!(rowid, expected_ids[index]);

    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        index += 1;
        if index < expected_ids.len() {
            let IOResult::Done(rowid) = cursor.rowid().unwrap() else {
                unreachable!();
            };
            let rowid = rowid.unwrap();
            assert_eq!(rowid, expected_ids[index]);
        }
    }

    // Should have found all 5 rows
    assert_eq!(index, expected_ids.len() - 1);
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_basic() {
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 3, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    let _ = cursor.next().unwrap();

    // Check first row
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_with_empty_table() {
    let db = MvccTestDb::new();
    {
        // FIXME: force page 1 initialization
        let pager = db.conn.pager.load().clone();
        let tx_id = db.mvcc_store.begin_tx(pager).unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();
    }
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let table_id = -1; // Empty table

    // Test LazyScanCursor with empty table
    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        table_id,
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(db.conn.pager.load().clone(), -table_id, 1)),
    )
    .unwrap();
    assert!(cursor.is_empty());
    let rowid = cursor.rowid().unwrap();
    assert!(matches!(rowid, IOResult::Done(None)));
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_modification_during_scan() {
    let _ = tracing_subscriber::fmt::try_init();
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Read first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    let first_row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(first_row.id.row_id.to_int_or_panic(), 1);

    // Insert a new row with ID between existing rows
    let new_row_id = RowID::new(table_id, RowKey::Int(3));
    let new_row = generate_simple_string_record("new_row");

    let _ = cursor
        .insert(&BTreeKey::TableRowId((
            new_row_id.row_id.to_int_or_panic(),
            Some(&new_row),
        )))
        .unwrap();

    // `insert` leaves the cursor on the inserted row, so the scan continues after 3.
    let mut read_rowids = vec![];
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        read_rowids.push(
            cursor
                .read_mvcc_current_row()
                .unwrap()
                .unwrap()
                .id
                .row_id
                .to_int_or_panic(),
        );
    }
    assert_eq!(read_rowids, vec![4, 5]);
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/* States described in the Hekaton paper *for serializability*:

Table 1: Case analysis of action to take when version V’s
Begin field contains the ID of transaction TB
------------------------------------------------------------------------------------------------------
TB’s state   | TB’s end timestamp | Action to take when transaction T checks visibility of version V.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TB=T and V’s end timestamp equals infinity.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s begin timestamp will be TS ut V is not yet committed. Use TS
                                  | as V’s begin time when testing visibility. If the test is true,
                                  | allow T to speculatively read V. Committed TS V’s begin timestamp
                                  | will be TS and V is committed. Use TS as V’s begin time to test
                                  | visibility.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s begin timestamp will be TS and V is committed. Use TS as V’s
                                  | begin time to test visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | Ignore V; it’s a garbage version.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s Begin field. TB has terminated so it must have finalized
or not found |                    | the timestamp.
------------------------------------------------------------------------------------------------------

Table 2: Case analysis of action to take when V's End field
contains a transaction ID TE.
------------------------------------------------------------------------------------------------------
TE’s state   | TE’s end timestamp | Action to take when transaction T checks visibility of a version V
             |                    | as of read time RT.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TE is not T.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s end timestamp will be TS provided that TE commits. If TS > RT,
                                  | V is visible to T. If TS < RT, T speculatively ignores V.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s end timestamp will be TS and V is committed. Use TS as V’s end
                                  | timestamp when testing visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | V is visible.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s End field. TE has terminated so it must have finalized
or not found |                    | the timestamp.
*/

fn new_tx(tx_id: TxID, begin_ts: u64, state: TransactionState) -> Transaction {
    new_tx_in(tx_id, begin_ts, state)
}

fn new_tx_in<A: super::RowVersionAllocator>(
    tx_id: TxID,
    begin_ts: u64,
    state: TransactionState,
) -> Transaction<A> {
    let state = state.into();
    Transaction {
        state,
        tx_id,
        begin_ts,
        write_set: Mutex::new(WriteSet::new()),
        header: RwLock::new(DatabaseHeader::default()),
        header_dirty: AtomicBool::new(false),
        savepoint_stack: RwLock::new(Vec::new()),
        pager_commit_lock_held: AtomicBool::new(false),
        log_appended: AtomicBool::new(false),
        commit_dep_counter: AtomicU64::new(0),
        abort_now: AtomicBool::new(false),
        commit_dep_set: Mutex::new(HashSet::default()),
        holds_blocking_checkpoint_read: AtomicBool::new(false),
        schema_generation_at_begin: 0,
        read_mark: crate::mvcc::database::WalPos::ORIGIN,
        created_table_ids: Mutex::new(Vec::new()),
        portable_logical_changes: false,
    }
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_snapshot_isolation_tx_visible1() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (2, new_tx(2, 2, TransactionState::Committed(5))),
        (3, new_tx(3, 3, TransactionState::Aborted)),
        (5, new_tx(5, 5, TransactionState::Preparing(8))),
        (6, new_tx(6, 6, TransactionState::Committed(10))),
        (7, new_tx(7, 7, TransactionState::Active)),
        // tx 8 with Preparing(3): current_tx (begin_ts=4) can speculatively read
        (8, new_tx(8, 1, TransactionState::Preparing(3))),
    ]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let current_tx = new_tx(4, 4, TransactionState::Preparing(7));

    let rv_visible = |begin: Option<TxTimestampOrID>, end: Option<TxTimestampOrID>| {
        let row_version = RowVersion {
            id: 0, // Dummy ID for visibility tests
            begin: crate::mvcc::database::PackedTs::pack(begin),
            end: crate::mvcc::database::PackedTs::pack(end),
            data: generate_simple_string_row((-2).into(), 1, "testme").data,
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        tracing::debug!("Testing visibility of {row_version:?}");
        row_version.is_visible_to(&current_tx, &txs, &finalized_tx_states)
    };

    // begin visible:   transaction committed with ts < current_tx.begin_ts
    // end visible:     inf
    assert!(rv_visible(Some(TxTimestampOrID::TxID(1)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(2)), None));

    // begin invisible: transaction aborted
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(3)), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(1))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction committed with ts < current_tx.begin_ts
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(2))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction aborted, delete never happened (Table 2)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(3))
    ));

    // begin invisible: transaction preparing with end_ts(8) > begin_ts(4)
    // Speculative read condition (begin_ts >= end_ts) is false: 4 >= 8 is false
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(5)), None));

    // begin VISIBLE via speculative read: tx 8 is Preparing(3), begin_ts(4) >= end_ts(3)
    // Hekaton Table 1: speculatively read and register commit dependency
    assert!(rv_visible(Some(TxTimestampOrID::TxID(8)), None));
    // Verify dependency was registered via register-and-report protocol
    assert_eq!(
        current_tx.commit_dep_counter.load(Ordering::Acquire),
        1,
        "speculative read should register a commit dependency"
    );

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(6)), None));

    // begin invisible: transaction active
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(7)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(6)), None));

    // begin invisible:   transaction active
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(7)), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction preparing with TS(8) > RT(4) (Table 2)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(5))
    ));

    // begin invisible: timestamp > current_tx.begin_ts
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(6)),
        Some(TxTimestampOrID::TxID(6))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     some active transaction will eventually overwrite this version,
    //                  but that hasn't happened
    //                  (this is the https://avi.im/blag/2023/hekaton-paper-typo/ case, I believe!)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(7))
    ));

    assert!(!rv_visible(None, None));
}

#[test]
fn test_visibility_uses_finalized_state_for_removed_committed_tx() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::new();
    let finalized_tx_states: SkipMap<TxID, TransactionState> =
        SkipMap::from_iter([(42, TransactionState::Committed(5))]);
    let reader = new_tx(7, 10, TransactionState::Active);

    let inserted_row = RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(42))),
        end: crate::mvcc::database::PackedTs::pack(None),
        data: generate_simple_string_row((-2).into(), 1, "x").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    assert!(
        inserted_row.is_visible_to(&reader, &txs, &finalized_tx_states),
        "stale begin=TxID should resolve via finalized committed state"
    );

    let deleted_row = RowVersion {
        id: 2,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(1))),
        end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(42))),
        data: generate_simple_string_row((-2).into(), 2, "y").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    assert!(
        !deleted_row.is_visible_to(&reader, &txs, &finalized_tx_states),
        "stale end=TxID should resolve via finalized committed state"
    );
}

#[test]
fn test_gc_keeps_finalized_tx_state_of_tx_still_in_txs() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    let tx_id = conn.get_mv_tx_id().unwrap();
    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::BeforeFinishCommittedTx.point(),
    ])));
    let mut commit = conn.prepare("COMMIT").unwrap();
    assert!(matches!(commit.step().unwrap(), StepResult::Yield));

    let commit_ts = match mvcc_store.txs.get(&tx_id).unwrap().value().state.load() {
        TransactionState::Committed(ts) => ts,
        other => panic!("writer should be committed, got {other:?}"),
    };
    mvcc_store
        .insert_finalized_tx_state(tx_id, commit_ts)
        .unwrap();
    mvcc_store.drop_unused_row_versions();

    assert!(
        lookup_finalized_tx_state(&mvcc_store.finalized_tx_states, tx_id).is_some(),
        "GC removed the finalized state of a writer that is still in txs"
    );
}

/// Test Hekaton register-and-report: speculative read increments CommitDepCounter
/// and adds to CommitDepSet.
#[test]
fn test_commit_dependency_speculative_read() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    // Reader with begin_ts=10 > end_ts=5 → speculative read → dependency
    let reader = new_tx(2, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(1))),
        end: crate::mvcc::database::PackedTs::pack(None),
        data: generate_simple_string_row((-2).into(), 1, "test").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);

    // Speculative read: begin_ts(10) >= end_ts(5) → visible, dependency registered
    assert!(rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // Verify tx 1's CommitDepSet contains reader's tx_id
    let dep_set = txs.get(&1).unwrap();
    assert_eq!(
        *dep_set.value().commit_dep_set.lock(),
        HashSet::from_iter([2])
    );
}

/// Test cascade abort: when depended-on tx aborts, it sets AbortNow on dependents
/// and decrements their CommitDepCounter.
#[test]
fn test_commit_dependency_cascade_abort() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(1))),
        end: crate::mvcc::database::PackedTs::pack(None),
        data: generate_simple_string_row((-2).into(), 1, "test").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    // Speculative read registers dependency
    assert!(rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);
    assert!(!reader.abort_now.load(Ordering::Acquire));

    // Simulate tx 1 aborting and cascading to dependents
    let tx1 = txs.get(&1).unwrap();
    let tx1 = tx1.value();
    tx1.state.store(TransactionState::Aborted);

    // Add reader to txs so cascade can find it
    txs.insert(2, reader);

    for dep_tx_id in tx1.commit_dep_set.lock().drain() {
        if let Some(dep_tx_entry) = txs.get(&dep_tx_id) {
            let dep_tx = dep_tx_entry.value();
            dep_tx.abort_now.store(true, Ordering::Release);
            dep_tx.commit_dep_counter.fetch_sub(1, Ordering::AcqRel);
        }
    }

    let reader = txs.get(&2).unwrap();
    let reader = reader.value();
    assert!(reader.abort_now.load(Ordering::Acquire));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
}

/// Test that registering a dependency on an already-committed tx is a no-op.
#[test]
fn test_commit_dependency_already_committed() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Committed(5)))]);

    let reader = new_tx(2, 10, TransactionState::Active);

    register_commit_dependency(&txs, &reader, 1);

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
    assert!(!reader.abort_now.load(Ordering::Acquire));
}

/// Test that registering a dependency on an already-aborted tx sets AbortNow.
#[test]
fn test_commit_dependency_already_aborted() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Aborted))]);

    let reader = new_tx(2, 10, TransactionState::Active);

    register_commit_dependency(&txs, &reader, 1);

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
    assert!(reader.abort_now.load(Ordering::Acquire));
}

/// Test speculative ignore in is_end_visible registers dependency.
#[test]
fn test_commit_dependency_speculative_ignore() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (3, new_tx(3, 3, TransactionState::Preparing(5))),
    ]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    // Reader with begin_ts=10 > end_ts=5: will speculatively ignore (treat as deleted)
    let reader = new_tx(4, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(2))),
        end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(3))),
        data: generate_simple_string_row((-2).into(), 1, "test").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    // is_end_visible: Preparing(5), begin_ts(10) < 5 = false → deletion visible
    // is_begin_visible: Timestamp(2), 10 >= 2 = true
    // Combined: true && false = false (row not visible because it was deleted)
    assert!(!rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        1,
        "speculative ignore should register a commit dependency"
    );
}

/// Regression: the forward-scan [`IndexShadowScan`] must NOT evaluate the
/// shadow predicate (and thus must not fire its `register_commit_dependency`
/// side effect) for index versions whose keys it merely *steps over*, i.e.
/// MVCC-only keys with no matching B-tree row. The authoritative
/// `query_btree_version_is_valid` path only ever evaluates the version chain for
/// keys that exactly match a B-tree row (`index_rows.get(btree_key)`), so an
/// eager scan that resolved the shadow bit on every advance would register a
/// commit dependency on a `Preparing` writer for a row the scan never observes.
/// That spurious dependency cascade-aborts the reader if that writer aborts.
///
/// Setup: B-tree keys 10 and 30 (no MVCC versions), plus a single MVCC-only
/// tombstone at key 20 deleted by a `Preparing` writer that the reader would
/// speculatively invalidate. A forward scan checks 10 (scan ahead → visible)
/// then 30 (scan behind → steps over key 20). Key 20 is never an exact match,
/// so no dependency may be registered.
#[test]
fn test_index_shadow_scan_no_spurious_dep_on_stepped_over_key() {
    use crate::mvcc::cursor::IndexShadowScan;

    let db = MvccTestDb::new();
    let store = &db.mvcc_store;
    let table_id = MVTableId::from(-999_i64);

    // Single-column ascending integer index key.
    let info = std::sync::Arc::new(
        crate::types::IndexInfo::new(
            crate::alloc::vec![crate::types::KeyInfo {
                sort_order: turso_parser::ast::SortOrder::Asc,
                collation: crate::translate::collate::CollationSeq::BINARY,
                nulls_order: None,
            }],
            false,
            1,
            false,
        )
        .unwrap(),
    );
    let idx_key = |v: i64| {
        let rec = crate::types::ImmutableRecord::from_values(&[Value::from_i64(v)], 1).unwrap();
        std::sync::Arc::new(
            SortableIndexKey::new_from_payload_in(&rec, info.clone(), crate::alloc::TursoAllocator)
                .unwrap(),
        )
    };

    // Reader started after the writer's prepared end_ts → speculatively
    // invalidates the writer's tombstone (the dependency-registering path).
    let reader_id: TxID = 9_000_100;
    let writer_id: TxID = 9_000_050;
    store.txs.insert(
        writer_id,
        new_tx_in::<crate::alloc::DynAllocator>(writer_id, 1, TransactionState::Preparing(40)),
    );
    store.txs.insert(
        reader_id,
        new_tx_in::<crate::alloc::DynAllocator>(reader_id, 100, TransactionState::Active),
    );

    // MVCC-only tombstone at key 20: committed insert (begin Timestamp) deleted
    // by the Preparing writer (end TxID). Not present in the B-tree.
    let key20 = idx_key(20);
    let row_id = RowID::new(table_id, RowKey::Record(key20.clone()));
    let tombstone = RowVersion {
        id: 20,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(5))),
        end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(writer_id))),
        data: Row::new_index_row(row_id).data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let mut tombstone_versions =
        <RowVersionChain<crate::alloc::DynAllocator> as crate::alloc::TursoVecInExt<
            RowVersion,
            crate::alloc::DynAllocator,
        >>::new_in(crate::alloc::DynAllocator::default());
    tombstone_versions.push(tombstone);
    store
        .get_or_create_index_rows(table_id)
        .unwrap()
        .value()
        .insert(key20, Arc::new(RwLock::new(tombstone_versions)));

    let mut scan = IndexShadowScan::default();
    // B-tree key 10: scan seeds at the first index key >= 10 (key 20), which is
    // ahead → row visible, predicate not evaluated.
    assert!(scan
        .btree_row_is_valid(store, table_id, reader_id, &idx_key(10))
        .unwrap());
    // B-tree key 30: scan (at key 20) is behind → steps over the tombstone.
    // It must advance past it WITHOUT evaluating the shadow predicate.
    assert!(scan
        .btree_row_is_valid(store, table_id, reader_id, &idx_key(30))
        .unwrap());

    let reader = store.txs.get(&reader_id).unwrap();
    assert_eq!(
        reader.value().commit_dep_counter.load(Ordering::Acquire),
        0,
        "scan registered a spurious commit dependency for a key it only stepped over"
    );
    let writer = store.txs.get(&writer_id).unwrap();
    assert!(
        writer.value().commit_dep_set.lock().is_empty(),
        "writer's commit-dep set must stay empty: reader never observed the tombstoned row"
    );
}

/// Test that multiple speculative reads from the same preparing tx only
/// register one commit dependency (dedup).
#[test]
fn test_commit_dependency_multiple_reads_dedup() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);

    let make_rv = |row_id: i64| RowVersion {
        id: row_id as u64,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(1))),
        end: crate::mvcc::database::PackedTs::pack(None),
        data: generate_simple_string_row((-2).into(), row_id, "test").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    // Read 3 rows from the same preparing tx — dependency is deduplicated
    assert!(make_rv(1).is_visible_to(&reader, &txs, &finalized_tx_states));
    assert!(make_rv(2).is_visible_to(&reader, &txs, &finalized_tx_states));
    assert!(make_rv(3).is_visible_to(&reader, &txs, &finalized_tx_states));

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // tx 1's CommitDepSet has 1 entry for reader (deduplicated)
    let dep_set = txs.get(&1).unwrap();
    assert_eq!(dep_set.value().commit_dep_set.lock().len(), 1);
}

/// Hekaton §2.7 cascade abort with real connections and threads.
///
/// A Preparing writer is speculatively read by a reader on another thread.
/// When the writer aborts, the reader's COMMIT must fail with
/// CommitDependencyAborted (cascade abort via AbortNow).
///
/// Sequence:
///   1. Writer: BEGIN CONCURRENT → UPDATE (real SQL)
///   2. Writer state manually set to Preparing(end_ts)
///   3. Reader thread: BEGIN → SELECT (speculative read → dependency) → INSERT
///   4. Main thread: rollback writer → cascade abort via AbortNow
///   5. Reader COMMIT → CommitDependencyAborted
#[test]
fn test_commit_dep_threaded_abort_cascades() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: real SQL operations
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    // Simulate mid-commit: transition writer to Preparing.
    // end_ts comes from the global clock so the reader's begin_ts will be >=
    // end_ts, satisfying the speculative read condition (Hekaton Table 1).
    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    // Reader signals after speculative read so main thread can abort writer.
    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // SELECT triggers speculative read: reader.begin_ts >= writer.end_ts
        // → Hekaton Table 1: visible, register commit dependency
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        // Write so COMMIT exercises the full commit state machine path.
        reader_conn
            .execute("INSERT INTO t VALUES (2, 'reader_data')")
            .unwrap();

        // Signal: speculative read done, dependency registered
        signal_tx.send(()).unwrap();

        // COMMIT blocks in WaitForDependencies until the writer resolves.
        // Writer will abort → AbortNow set → CommitDependencyAborted.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close(); // cleanup (rolls back if still active)
        (rows, commit_result)
    });

    // Wait for reader to complete speculative read
    signal_rx.recv().unwrap();

    // Abort writer → cascade: sets AbortNow on reader, decrements counter
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    let (rows, commit_result) = reader_handle.join().unwrap();

    // Reader saw the writer's modified value via speculative read
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].to_text().unwrap(),
        "modified",
        "reader should have speculatively read the Preparing writer's value"
    );

    // Reader's COMMIT must fail: depended-on writer aborted
    assert!(
        matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
        "expected CommitDependencyAborted, got: {commit_result:?}",
    );

    // Verify database consistency
    {
        let conn = db.connect();

        // Only the initial value should remain
        let mut stmt = conn.prepare("SELECT value FROM t WHERE id = 1").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_text().unwrap(), "initial");

        // Reader's INSERT must not be visible (cascade-aborted)
        let mut stmt = conn.prepare("SELECT * FROM t WHERE id = 2").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert!(
            rows.is_empty(),
            "reader's write should not be visible after cascade abort"
        );
    }
}

/// Hekaton §2.7: multiple readers depending on the same Preparing writer
/// all cascade-abort when the writer aborts.
#[test]
fn test_commit_dep_threaded_multiple_dependents_abort() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let num_readers = 4;
    // Barrier: all readers + main thread synchronize after speculative reads
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_readers + 1));

    let mut handles = Vec::new();
    for i in 0..num_readers {
        let db_arc = db.get_db();
        let barrier_clone = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let conn = db_arc
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            conn.execute("BEGIN CONCURRENT").unwrap();

            // Speculative read from Preparing writer
            let mut stmt = conn.prepare("SELECT value FROM t WHERE id = 1").unwrap();
            let rows = stmt.run_collect_rows().unwrap();

            // Each reader writes to a unique row (no conflicts)
            conn.execute(format!("INSERT INTO t VALUES ({}, 'reader_{i}')", i + 10,))
                .unwrap();

            // Signal: all readers done with speculative reads
            barrier_clone.wait();

            let commit_result = conn.execute("COMMIT");
            let _ = conn.close();
            (rows, commit_result)
        }));
    }

    // Wait for all readers to complete speculative reads
    barrier.wait();

    // Abort writer → cascade to ALL readers
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    for handle in handles {
        let (rows, commit_result) = handle.join().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_text().unwrap(), "modified");
        assert!(
            matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
            "expected CommitDependencyAborted, got: {commit_result:?}",
        );
    }

    // All reader writes should be invisible — only the initial row remains
    {
        let conn = db.connect();
        let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows[0][0].as_int().unwrap(), 1);
    }
}

/// Hekaton §2.7 happy path: when a Preparing writer commits, the dependent
/// reader's CommitDepCounter is decremented and the reader can proceed.
#[test]
fn test_commit_dep_threaded_commit_resolves() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: UPDATE via real connection, then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'committed' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        reader_conn
            .execute("INSERT INTO t VALUES (2, 'reader_data')")
            .unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT blocks in WaitForDependencies. Writer will commit →
        // counter decremented → reader proceeds.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        (rows, commit_result)
    });

    signal_rx.recv().unwrap();

    // Complete the writer's commit manually (postprocessing):
    // 1. Convert TxID → Timestamp in row versions
    // 2. Set state to Committed
    // 3. Drain CommitDepSet, decrement dependents' counters
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();

        // Convert TxID→Timestamp in row versions (Hekaton §3.3 postprocessing)
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
                if rv.end() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
            }
        }

        // Committed state + notify dependents
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let (rows, commit_result) = reader_handle.join().unwrap();

    // Reader speculatively read the writer's value
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_text().unwrap(), "committed");

    // Reader's COMMIT succeeds: dependency resolved by writer's commit
    assert!(
        commit_result.is_ok(),
        "expected reader COMMIT to succeed, got: {commit_result:?}",
    );

    // Both writes are visible
    {
        let conn = db.connect();
        let mut stmt = conn.prepare("SELECT value FROM t ORDER BY id").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].to_text().unwrap(), "committed");
        assert_eq!(rows[1][0].to_text().unwrap(), "reader_data");
    }
}

/// Regression: the write_set.is_empty() fast path used to commit read-only
/// transactions without checking commit dependencies. A read-only tx that
/// speculatively read from a Preparing writer must still honour AbortNow.
#[test]
fn test_commit_dep_threaded_readonly_abort_cascades() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // Read-only: no writes, only SELECT → triggers speculative read
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT on a read-only tx hits the write_set.is_empty() fast path.
        // It must still check commit dependencies.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        (rows, commit_result)
    });

    signal_rx.recv().unwrap();

    // Abort writer → cascade to read-only reader
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    let (rows, commit_result) = reader_handle.join().unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_text().unwrap(), "modified");

    // Read-only tx must still fail when its dependency aborts
    assert!(
        matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
        "read-only tx should fail with CommitDependencyAborted, got: {commit_result:?}",
    );
}

/// Test that register_commit_dependency increments counter before pushing to
/// dep_set, preventing underflow. If counter is incremented after push+unlock,
/// a concurrent drain could fetch_sub(1) on a zero counter, wrapping to MAX.
#[test]
fn test_commit_dependency_counter_no_underflow() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let reader = new_tx(2, 10, TransactionState::Active);

    // Register dependency: counter should go 0 → 1
    register_commit_dependency(&txs, &reader, 1);
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // Simulate drain (as in CommitEnd): fetch_sub should go 1 → 0, not wrap
    reader.commit_dep_counter.fetch_sub(1, Ordering::AcqRel);
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        0,
        "counter should be exactly 0, not u64::MAX (underflow)"
    );
}

/// Test that registering a dependency on a Terminated (aborted+removed from map)
/// transaction correctly sets AbortNow. Before the fix, rollback_tx removed the
/// tx from txs, so register_commit_dependency saw None and assumed "committed."
#[test]
fn test_commit_dependency_terminated_tx_sets_abort() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Terminated))]);

    let reader = new_tx(2, 10, TransactionState::Active);
    register_commit_dependency(&txs, &reader, 1);

    // Terminated means the tx aborted — must set abort_now
    assert!(
        reader.abort_now.load(Ordering::Acquire),
        "dependency on Terminated tx should set abort_now"
    );
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        0,
        "no counter increment for aborted/terminated dependency"
    );
}

/// Test that when tx is NOT in the map (removed), register_commit_dependency
/// treats it as committed (no abort_now, no counter increment). This is correct
/// only for committed transactions. Aborted transactions should NOT be removed
/// from the map (Issue #3 fix ensures this).
#[test]
fn test_commit_dependency_missing_tx_assumes_committed() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);
    register_commit_dependency(&txs, &reader, 99);

    assert!(
        !reader.abort_now.load(Ordering::Acquire),
        "missing tx (committed+removed) should not set abort_now"
    );
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
}

/// Test that read-only transactions with resolved dependencies do NOT advance
/// last_committed_tx_ts. A read-only tx going through WaitForDependencies →
/// CommitEnd would update last_committed_tx_ts, causing spurious Busy errors
/// from acquire_exclusive_tx.
#[test]
fn test_commit_dep_readonly_does_not_advance_timestamp() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();
    let ts_before = mvcc_store.last_committed_tx_ts.load(Ordering::Acquire);

    // Writer: UPDATE then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let mvcc_clone = mvcc_store.clone();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // Read-only: SELECT only → speculative read registers dependency
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let _rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT: read-only with dependency → WaitForDependencies
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        commit_result
    });

    signal_rx.recv().unwrap();

    // Complete writer's commit manually (resolve dependency)
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
                if rv.end() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
            }
        }
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let commit_result = reader_handle.join().unwrap();
    assert!(
        commit_result.is_ok(),
        "read-only tx with resolved dependency should commit: {commit_result:?}",
    );

    let ts_after = mvcc_clone.last_committed_tx_ts.load(Ordering::Acquire);
    assert_eq!(
        ts_before, ts_after,
        "read-only tx should NOT advance last_committed_tx_ts (was {ts_before}, now {ts_after})"
    );
}

/// Test that a new transaction can still acquire the exclusive lock after a
/// read-only dependent tx commits. Before the fix, the read-only tx would
/// advance last_committed_tx_ts via CommitEnd, making acquire_exclusive_tx
/// return Busy for transactions that started before the read.
#[test]
fn test_commit_dep_readonly_does_not_cause_spurious_busy() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: UPDATE then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    // Start a tx that will try to get the exclusive lock later. BEGIN
    // CONCURRENT is deferred, so run a read to materialize it: its begin_ts
    // must be assigned now, before the read-only tx commits.
    let exclusive_conn = db.connect();
    exclusive_conn.execute("BEGIN CONCURRENT").unwrap();
    let _ = get_rows(&exclusive_conn, "SELECT count(*) FROM t");
    let exclusive_tx_id = exclusive_conn.get_mv_tx_id().unwrap();

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let _rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        commit_result
    });

    signal_rx.recv().unwrap();

    // Resolve the writer's commit (unblocks reader's WaitForDependencies)
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
                if rv.end() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
            }
        }
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let commit_result = reader_handle.join().unwrap();
    assert!(commit_result.is_ok());

    // Now try to acquire exclusive lock for the tx that started before the
    // read-only dependent committed. Should succeed because the read-only tx
    // did not advance last_committed_tx_ts.
    let acquire_result = mvcc_store.acquire_exclusive_tx(&exclusive_tx_id, None);
    assert!(
        acquire_result.is_ok(),
        "acquire_exclusive_tx should not return Busy after a read-only dependent committed: {acquire_result:?}",
    );
    mvcc_store.release_exclusive_tx(&exclusive_tx_id);
}

/// Insert a synthetic table and a single row via the MVCC store, then commit.
/// Used by restart / recovery tests to seed durable state before a restart cycle.
fn write_synthetic_row(db: &MvccTestDbNoConn, value: &str) {
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let max_root_page = get_rows(
        &conn,
        "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
    )[0][0]
        .as_int()
        .unwrap();
    let next_schema_rowid = get_rows(
        &conn,
        "SELECT COALESCE(MAX(rowid), 0) + 1 FROM sqlite_schema",
    )[0][0]
        .as_int()
        .unwrap();
    let synthetic_root = -(max_root_page + 100);
    let synthetic_table_id = MVTableId::new(synthetic_root);
    let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    let data = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new("table")),
            Value::Text(Text::new("test")),
            Value::Text(Text::new("test")),
            Value::from_i64(synthetic_root),
            Value::Text(Text::new(
                "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
            )),
        ],
        5,
    )
    .unwrap();
    mvcc_store
        .insert(
            tx_id,
            Row::new_table_row(
                RowID::new((-1).into(), RowKey::Int(next_schema_rowid)),
                data.as_blob(),
            )
            .unwrap(),
        )
        .unwrap();
    let row = generate_simple_string_row(synthetic_table_id, 1, value);
    mvcc_store.insert(tx_id, row).unwrap();
    commit_tx(mvcc_store, &conn, tx_id).unwrap();
    conn.close().unwrap();
}

fn get_record_value(row: &Row) -> ImmutableRecord {
    let mut record = ImmutableRecord::new(1024).unwrap();
    record.start_serialization(row.payload()).unwrap();
    record
}

fn get_rows(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().cloned().collect::<Vec<_>>();
        rows.push(values);
        Ok(())
    })
    .unwrap();
    rows
}

/// The implementation maintains the intended invariant for this scenario.
#[test]
fn transaction_display() {
    let state = AtomicTransactionState::from(TransactionState::Preparing(20250915));
    let tx_id = 42;
    let begin_ts = 20250914;

    let empty_versions = || Arc::new(RwLock::new(empty_version_chain()));
    let write_set = Mutex::new({
        let mut write_set: WriteSet = WriteSet::new();
        write_set.push(RowID::new((-2).into(), RowKey::Int(11)), empty_versions());
        write_set.push(RowID::new((-2).into(), RowKey::Int(13)), empty_versions());
        write_set
    });

    let tx = Transaction {
        state,
        tx_id,
        begin_ts,
        write_set,
        header: RwLock::new(DatabaseHeader::default()),
        header_dirty: AtomicBool::new(false),
        savepoint_stack: RwLock::new(Vec::new()),
        pager_commit_lock_held: AtomicBool::new(false),
        log_appended: AtomicBool::new(false),
        commit_dep_counter: AtomicU64::new(0),
        abort_now: AtomicBool::new(false),
        commit_dep_set: Mutex::new(HashSet::default()),
        holds_blocking_checkpoint_read: AtomicBool::new(false),
        schema_generation_at_begin: 0,
        read_mark: crate::mvcc::database::WalPos::ORIGIN,
        created_table_ids: Mutex::new(Vec::new()),
        portable_logical_changes: false,
    };

    let expected = "{ state: Preparing(20250915), id: 42, begin_ts: 20250914, write_set: [RowID { table_id: MVTableId(-2), row_id: Int(11) }, RowID { table_id: MVTableId(-2), row_id: Int(13) }] }";
    let output = format!("{tx}");
    assert_eq!(output, expected);
}

pub fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    );
    let rng = ChaCha8Rng::seed_from_u64(seed as u64);
    (rng, seed as u64)
}

#[test]
fn test_checkpoint_index_writer_overwrites_existing_interior_key() {
    fn run_pager_until_done<T>(
        mut action: impl FnMut() -> IOResultOr<T>,
        pager: &Pager,
    ) -> Result<T> {
        loop {
            match action()? {
                IOResult::Done(value) => return Ok(value),
                IOResult::IO(io) => io.wait(pager.io.as_ref())?,
            }
        }
    }

    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let index = crate::schema::Index {
        name: "testindex".to_string(),
        table_name: "test".to_string(),
        root_page: 0,
        columns: IndexColumn::new_many(vec!["id"]),
        unique: true,
        ephemeral: false,
        has_rowid: true,
        where_clause: None,
        index_method: None,
        on_conflict: None,
        constraint_name: None,
    };

    pager.begin_read_tx().unwrap();
    run_pager_until_done(
        || pager.begin_write_tx(crate::storage::wal::WalAutoActions::all_enabled()),
        pager.as_ref(),
    )
    .unwrap();
    let root_page = pager
        .io
        .block(|| pager.btree_create(&crate::storage::pager::CreateBTreeFlags::new_index()))
        .unwrap() as i64;
    let cursor = Arc::new(RwLock::new(
        BTreeCursor::new_index(pager.clone(), root_page, &index, index.columns.len()).unwrap(),
    ));

    for key in 1..=600 {
        let record =
            ImmutableRecord::from_values(&[Value::from_i64(key), Value::from_i64(key)], 2).unwrap();
        let seek_result = run_pager_until_done(
            || {
                cursor.write().seek(
                    crate::types::SeekKey::IndexKey(record.as_record_ref()),
                    crate::types::SeekOp::GE { eq_only: true },
                )
            },
            pager.as_ref(),
        )
        .unwrap();
        if matches!(seek_result, SeekResult::TryAdvance) {
            run_pager_until_done(|| cursor.write().next(), pager.as_ref()).unwrap();
        }
        run_pager_until_done(
            || {
                cursor
                    .write()
                    .insert(&BTreeKey::new_index_key(record.as_record_ref()))
            },
            pager.as_ref(),
        )
        .unwrap();
    }
    run_pager_until_done(
        || pager.commit_tx(&db.conn, db.conn.get_sync_mode(), true),
        pager.as_ref(),
    )
    .unwrap();

    pager.begin_read_tx().unwrap();
    let mut interior_key = None;
    for key in 1..=600 {
        let record =
            ImmutableRecord::from_values(&[Value::from_i64(key), Value::from_i64(key)], 2).unwrap();
        let seek_result = run_pager_until_done(
            || {
                cursor.write().seek(
                    crate::types::SeekKey::IndexKey(record.as_record_ref()),
                    crate::types::SeekOp::GE { eq_only: true },
                )
            },
            pager.as_ref(),
        )
        .unwrap();
        if matches!(seek_result, SeekResult::TryAdvance) {
            interior_key = Some(key);
            break;
        }
    }
    let interior_key = interior_key.expect("test setup should create an index interior key");
    let count_before = run_pager_until_done(|| cursor.write().count(), pager.as_ref()).unwrap();

    run_pager_until_done(
        || pager.begin_write_tx(crate::storage::wal::WalAutoActions::all_enabled()),
        pager.as_ref(),
    )
    .unwrap();
    let index_info = Arc::new(IndexInfo::new_from_index(&index).unwrap());
    let record = ImmutableRecord::from_values(
        &[Value::from_i64(interior_key), Value::from_i64(interior_key)],
        2,
    )
    .unwrap();
    let row_key =
        SortableIndexKey::new_from_payload_in(&record, index_info, crate::alloc::TursoAllocator)
            .unwrap();
    let row = Row::new_index_row(RowID::new(
        MVTableId::new(-42),
        RowKey::Record(Arc::new(row_key)),
    ));
    let mut write_row_sm = db
        .mvcc_store
        .write_row_to_pager(&row, cursor.clone(), true)
        .unwrap();
    loop {
        match write_row_sm.step(&()).unwrap() {
            IOResult::Done(()) => break,
            IOResult::IO(io) => io.wait(pager.io.as_ref()).unwrap(),
        }
    }
    run_pager_until_done(
        || pager.commit_tx(&db.conn, db.conn.get_sync_mode(), true),
        pager.as_ref(),
    )
    .unwrap();

    pager.begin_read_tx().unwrap();
    let count_after = run_pager_until_done(|| cursor.write().count(), pager.as_ref()).unwrap();
    assert_eq!(
        count_after, count_before,
        "checkpoint index writer should overwrite an existing interior key, not insert a duplicate"
    );
}

#[test]
fn test_sql_checkpoint_reinsert_existing_interior_index_key_keeps_sqlite_integrity() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE t(payload BLOB, id INTEGER UNIQUE)")
        .unwrap();

    for id in 1..=600 {
        conn.execute(format!(
            "INSERT INTO t(rowid, payload, id) VALUES ({id}, x'70796c6f6164', {id})"
        ))
        .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    for id in 1..=600 {
        conn.execute(format!("DELETE FROM t WHERE id = {id}"))
            .unwrap();
        conn.execute(format!(
            "INSERT INTO t(rowid, payload, id) VALUES ({id}, x'7265696e73657274', {id})"
        ))
        .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();

    conn.close().unwrap();
    force_close_for_artifact_tamper(&mut db);

    let sqlite = rusqlite::Connection::open(db_path).unwrap();
    let integrity: String = sqlite
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
}

// ─── GC helpers ───────────────────────────────────────────────────────────

fn unique_index_btree_is_allocated(mv: &crate::MvStore) -> bool {
    mv.index_rows
        .iter()
        .any(|idx| mv.is_btree_allocated(idx.key()))
}

/// An index current that no checkpoint has written to the B-tree yet. GC must
/// keep it: the SkipMap is the only copy of that key.
fn skipmap_has_unmaterialized_index_current(mv: &crate::MvStore) -> bool {
    mv.index_rows.iter().any(|idx| {
        idx.value().iter().any(|e| {
            e.value().read().iter().any(|rv| {
                rv.end().is_none() && rv.materialized_at() == crate::mvcc::database::WalPos::ORIGIN
            })
        })
    })
}

fn skipmap_version_count(mv: &crate::MvStore) -> usize {
    mv.rows.iter().map(|e| e.value().read().len()).sum()
}

fn make_rv(begin: Option<TxTimestampOrID>, end: Option<TxTimestampOrID>) -> RowVersion {
    RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(begin),
        end: crate::mvcc::database::PackedTs::pack(end),
        data: generate_simple_string_row((-2).into(), 1, "gc_test").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

/// [`make_rv`] for a version a checkpoint already wrote to the B-tree, which is
/// what Rule 3 requires before it may drop the last SkipMap copy.
fn make_materialized_rv(
    begin: Option<TxTimestampOrID>,
    end: Option<TxTimestampOrID>,
) -> RowVersion {
    let mut rv = make_rv(begin, end);
    rv.set_materialized_at(crate::mvcc::database::WalPos {
        checkpoint_seq: 1,
        frame: 1,
    });
    rv
}

fn ts(v: u64) -> Option<TxTimestampOrID> {
    Some(TxTimestampOrID::Timestamp(v))
}

fn txid(v: u64) -> Option<TxTimestampOrID> {
    Some(TxTimestampOrID::TxID(v))
}

// ─── GC unit tests ───────────────────────────────────────────────────────

#[test]
/// Rolled-back transactions leave versions with begin=None, end=None. These are
/// invisible to every transaction and must be removed unconditionally by Rule 1.
fn test_gc_rule1_aborted_garbage_removed() {
    let mut versions: RowVersionChain = crate::alloc::vec![make_rv(None, None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Rule 1 removes only aborted garbage, leaving live and superseded versions intact.
fn test_gc_rule1_aborted_among_live_versions() {
    let mut versions: RowVersionChain = crate::alloc::vec![
        make_rv(ts(5), None),  // current
        make_rv(None, None),   // aborted
        make_rv(ts(3), ts(5)), // superseded
    ]
    .into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        2,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // Only aborted removed; superseded has e=5 > lwm=2 so retained
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 2);
    assert!(versions
        .iter()
        .all(|rv| rv.begin().is_some() || rv.end().is_some()));
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A superseded version whose end timestamp is at or below the low-water mark is
/// invisible to all active readers. When a committed current version exists to
/// take over B-tree invalidation, the superseded version is safely removable.
fn test_gc_rule2_superseded_below_lwm_with_current() {
    // Superseded version (end=Timestamp(3)) below LWM=10, and there's a current version.
    let mut versions: RowVersionChain = crate::alloc::vec![
        make_rv(ts(3), ts(5)), // superseded, e=5 <= lwm=10
        make_rv(ts(5), None),  // current
    ]
    .into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 1);
    assert!(versions[0].end().is_none()); // only current remains
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A superseded version whose end timestamp exceeds the LWM may still be visible
/// to an active reader. It must be retained regardless of other conditions.
fn test_gc_rule2_superseded_above_lwm_retained() {
    // Superseded version (end=Timestamp(15)) above LWM=10 — must be retained.
    let mut versions: RowVersionChain =
        crate::alloc::vec![make_rv(ts(3), ts(15)), make_rv(ts(15), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// When a row was deleted but the deletion hasn't been checkpointed to the B-tree
/// yet (e > ckpt_max), the tombstone is the only thing hiding the stale B-tree
/// row. Removing it would resurrect a deleted row. Must be retained.
fn test_gc_rule2_tombstone_guard_uncheckpointed() {
    // Tombstone: end is set, no current version, and e > ckpt_max.
    // Must be retained to prevent row resurrection via dual cursor.
    let mut versions: RowVersionChain = crate::alloc::vec![
        make_rv(ts(3), ts(5)), // tombstone (sole version, no current)
    ]
    .into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // e=5 > ckpt_max=2, no current → tombstone guard retains it
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Once the deletion has been checkpointed (e <= ckpt_max), the B-tree no longer
/// contains the row, so the tombstone is safe to remove.
fn test_gc_rule2_tombstone_guard_checkpointed() {
    // Tombstone with e <= ckpt_max — deletion is checkpointed, safe to remove.
    let mut versions: RowVersionChain = crate::alloc::vec![make_rv(ts(3), ts(5))].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // e=5 <= ckpt_max=5, e=5 <= lwm=10 → removable
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A current version that's been checkpointed to B-tree, with no other versions in
/// the chain and no transaction open at all, is redundant. The dual cursor will
/// fall through to the B-tree which has identical data. Safe to remove.
fn test_gc_rule3_drop_current_when_in_btree() {
    // Single stamped current with b <= ckpt_max, and no transaction open
    // (lwm == u64::MAX).
    let mut versions: RowVersionChain =
        crate::alloc::vec![make_materialized_rv(ts(5), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Truncate Rule 3 matches Passive: sole stamped currents clear only when idle.
#[test]
fn test_gc_rule3_truncate_idle_only() {
    let mut versions: RowVersionChain =
        crate::alloc::vec![make_materialized_rv(ts(5), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0, "open snapshot must block Truncate Rule 3 clear");
    assert_eq!(versions.len(), 1);

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A current version not yet checkpointed (b > ckpt_max) cannot be removed —
/// the B-tree doesn't have the data, so fallthrough would return stale results.
fn test_gc_rule3_not_checkpointed_retained() {
    // Single current version with b > ckpt_max — B-tree doesn't have it yet.
    // Nothing is open (lwm == u64::MAX), so ckpt_max is the only thing holding
    // this version back.
    let mut versions: RowVersionChain =
        crate::alloc::vec![make_materialized_rv(ts(5), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        3,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Any finite LWM means some transaction is still open, and an open snapshot may
/// be running a dual-cursor scan that would lose this row if the chain emptied
/// under it. Rule 3 only fires when nothing is open, so this is retained.
fn test_gc_rule3_visible_to_active_tx_retained() {
    // Single stamped current, already inside ckpt_max, but a transaction is open.
    let mut versions: RowVersionChain =
        crate::alloc::vec![make_materialized_rv(ts(5), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        5,
        10,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // lwm=5 != u64::MAX, so a transaction is open and Rule 3 must not fire.
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A current version cannot be removed before checkpoint has persisted it.
fn test_gc_rule3_current_retained_before_first_checkpoint() {
    let mut versions: RowVersionChain =
        crate::alloc::vec![make_materialized_rv(ts(1), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Once checkpoint has persisted a sole current version, it becomes GC-eligible.
fn test_gc_rule3_current_collected_after_checkpoint() {
    let mut versions: RowVersionChain =
        crate::alloc::vec![make_materialized_rv(ts(1), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 0);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Rule 3 only drops the last remaining current version. After Rule 2 removes
/// superseded history, Rule 3 can then drop that current.
fn test_gc_rule3_after_history_reclaimed() {
    // Rule 3 only fires when exactly one version remains after rules 1 & 2.
    let mut versions: RowVersionChain =
        crate::alloc::vec![make_rv(ts(3), ts(5)), make_materialized_rv(ts(5), None)].into();
    // Both versions are inside ckpt_max and nothing is open, but the chain
    // starts with 2 entries. Rule 2 removes the superseded one
    // (has_current=true), then Rule 3 drops the remaining current.
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());
}

/// Rule 3 drops a sole current only once a checkpoint stamped it. A high
/// `ckpt_max` says a checkpoint ran, not that this row reached a B-tree leaf.
#[test]
fn test_gc_rule3_keeps_unstamped_current_and_drops_stamped_one() {
    let mut versions: RowVersionChain = crate::alloc::vec![make_rv(ts(5), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        10,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);

    versions[0].set_materialized_at(crate::mvcc::database::WalPos {
        checkpoint_seq: 1,
        frame: 7,
    });
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        10,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Versions referencing an active transaction (begin=TxID) represent uncommitted
/// inserts. They don't match any removal rule and must always be retained.
fn test_gc_txid_refs_retained() {
    // Versions with TxID (uncommitted) references are never collected.
    let mut versions: RowVersionChain = crate::alloc::vec![make_rv(txid(99), None)].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        u64::MAX,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Versions with end=TxID represent an uncommitted deletion. Rule 2 only matches
/// end=Timestamp, so these are never collected until the deleting tx resolves.
fn test_gc_txid_end_retained() {
    // end=TxID means the deletion is uncommitted; rule 2 only matches Timestamp.
    let mut versions: RowVersionChain = crate::alloc::vec![make_rv(ts(3), txid(50))].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        u64::MAX,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A pending insert (begin=TxID) must NOT count as a "committed current version"
/// for the tombstone guard. If it rolled back, the tombstone would be the only
/// thing hiding the stale B-tree row, and removing it would resurrect deleted data.
fn test_gc_rule2_pending_insert_does_not_disable_tombstone_guard() {
    // A pending insert (begin=TxID, end=None) coexists with a tombstone.
    // has_current must NOT count the pending insert — if it rolls back,
    // the tombstone is the only thing hiding the B-tree row.
    let mut versions: RowVersionChain = crate::alloc::vec![
        make_rv(ts(3), ts(5)), // tombstone: deletion at e=5, not checkpointed (ckpt_max=2)
        make_rv(txid(99), None), // pending insert (uncommitted)
    ]
    .into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // Tombstone must be retained: e=5 > ckpt_max=2, and pending insert doesn't count.
    // Only nothing changes (pending insert is not aborted garbage either).
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// When a committed current version exists (begin=Timestamp, end=None), it takes
/// over MVCC visibility from a non-B-tree superseded version. The tombstone guard
/// is no longer needed, so the superseded version can be safely removed.
fn test_gc_rule2_committed_current_disables_non_btree_tombstone_guard() {
    // A committed current version (begin=Timestamp, end=None) means the row
    // has a live successor — the tombstone can safely be removed.
    let mut versions: RowVersionChain = crate::alloc::vec![
        make_rv(ts(3), ts(5)), // superseded, e=5 <= lwm=10
        make_rv(ts(5), None),  // committed current
    ]
    .into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // Superseded removed (has_current=true for committed version), current remains.
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 1);
    assert!(versions[0].end().is_none());
}

/// A B-tree-resident version whose ending timestamp has not been checkpointed
/// still records a required physical B-tree delete or overwrite. A committed
/// replacement can hide it from readers but cannot make it GC-eligible until
/// checkpoint makes that physical change durable.
#[test]
fn test_gc_rule2_btree_resident_marker_with_current_retained_until_checkpoint() {
    let mut tombstone = make_rv(None, ts(5));
    tombstone.btree_resident = true;
    let current = make_materialized_rv(ts(5), None);
    let mut versions: RowVersionChain = crate::alloc::vec![tombstone, current.clone()].into();

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
    assert!(versions[0].btree_resident);

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());

    let mut rewritten_btree_row = make_rv(ts(3), ts(5));
    rewritten_btree_row.btree_resident = true;
    let mut versions: RowVersionChain = crate::alloc::vec![rewritten_btree_row, current].into();

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
    assert!(versions[0].btree_resident);

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());
}

/// A superseded version whose insert was checkpointed (begin <= ckpt_max) is
/// physically in the B-tree even when its `btree_resident` flag is unset (the
/// flag is only seeded by the dual cursor, not by checkpoint). Until its
/// overwrite/delete is checkpointed (end > ckpt_max), it must be retained even
/// with a committed current replacement: the checkpointer derives DB-file
/// existence from begin/end timestamps against the durable boundary, so
/// removing this version would make a later delete skip the B-tree write and
/// resurrect the row (issue #7638).
#[test]
fn test_gc_rule2_checkpointed_insert_with_current_retained_until_checkpoint() {
    // begin=2 <= ckpt_max=2 (insert checkpointed), end=5 > ckpt_max (overwrite not).
    let checkpointed_btree_row = make_rv(ts(2), ts(5));
    let current = make_materialized_rv(ts(5), None);
    let mut versions: RowVersionChain = crate::alloc::vec![checkpointed_btree_row, current].into();

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);

    // Once the overwrite is checkpointed (end <= ckpt_max), both versions are
    // reclaimable (rule 2 for the superseded, rule 3 for the current).
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// B-tree tombstones (begin=None, end=e) represent rows that existed in the B-tree
/// before MVCC was enabled and were then deleted. Before checkpoint writes the
/// deletion, the tombstone hides the stale B-tree row. After checkpoint, it's safe
/// to remove. Tests the full lifecycle: retained → checkpointed → collected.
fn test_gc_rule2_btree_tombstone_lifecycle() {
    // B-tree tombstone: begin=None, end=Timestamp(e) where e > 0.
    // Represents a row deleted in MVCC that existed in B-tree before MVCC.
    // Before checkpoint (ckpt_max < e): tombstone must be retained.
    let mut versions: RowVersionChain = crate::alloc::vec![make_rv(None, ts(5))].into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        3,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0, "tombstone retained: e=5 > ckpt_max=3");
    assert_eq!(versions.len(), 1);

    // After checkpoint (ckpt_max >= e): tombstone is collected.
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1, "tombstone collected: e=5 <= ckpt_max=5");
    assert_eq!(versions.len(), 0);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Rule 3 must never fire when superseded versions remain in the chain — removing
/// the current version would leave orphaned superseded versions that "poison" the
/// dual cursor, making it hide the B-tree row without providing a replacement.
fn test_gc_rule3_not_firing_with_unremovable_superseded() {
    // Two versions: superseded with e > lwm (can't remove), and current.
    // Rule 2 can't remove the superseded one, so 2 versions remain.
    // Rule 3 needs a single remaining current, so it must NOT fire.
    let mut versions: RowVersionChain = crate::alloc::vec![
        make_rv(ts(3), ts(15)),             // e=15 > lwm=10 — retained
        make_materialized_rv(ts(15), None), // current
    ]
    .into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        20,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// GC on an empty version chain is a no-op. Verifies no panics or off-by-one errors.
fn test_gc_noop_on_empty() {
    let mut versions = empty_version_chain();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// All three rules fire together with nothing open: aborted garbage (Rule 1), two
/// checkpointed superseded versions with a committed current (Rule 2), and the sole
/// surviving stamped current inside `ckpt_max` (Rule 3). The chain is fully reclaimed.
fn test_gc_combined_rules() {
    let mut versions: RowVersionChain = crate::alloc::vec![
        make_rv(None, None),   // aborted → rule 1
        make_rv(ts(1), ts(3)), // superseded, e=3 <= ckpt_max=5 → rule 2 (has_current=true)
        make_rv(ts(3), ts(5)), // superseded, e=5 <= ckpt_max=5 → rule 2
        // current, stamped, b=5 <= ckpt_max=5, nothing open → rule 3
        make_materialized_rv(ts(5), None),
    ]
    .into();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 4);
    assert!(versions.is_empty());
}

/// GC trims chains with retain()/clear(), which keeps the Vec's allocation.
/// After a burst of versions is collected, the chain's capacity must be
/// released down to a quarter of its previous value (deliberately not to fit)
/// so a hot row doesn't pin its peak allocation forever.
#[test]
fn test_gc_shrinks_version_chain_capacity() {
    let make_version = |begin, end| RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(begin),
        end: crate::mvcc::database::PackedTs::pack(end),
        data: generate_simple_string_row((-2).into(), 1, "shrink").data,
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    // One committed current version that survives GC (b=1 > ckpt_max=0, so
    // rule 3 doesn't fire), plus a burst of aborted garbage (always removed).
    let mut versions: RowVersionChain<TursoAllocator> =
        std::iter::once(make_version(Some(TxTimestampOrID::Timestamp(1)), None))
            .chain((0..1023).map(|_| make_version(None, None)))
            .try_collect()
            .unwrap();
    let capacity_before = versions.capacity();
    assert!(capacity_before >= 1024);

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        0,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1023);
    assert_eq!(versions.len(), 1);
    assert!(
        versions.capacity() <= capacity_before / 4,
        "chain capacity should shrink to a quarter of {capacity_before}, got {}",
        versions.capacity()
    );

    // A chain emptied entirely also releases its allocation.
    let mut versions: RowVersionChain<TursoAllocator> = (0..1024)
        .map(|_| make_version(None, None))
        .try_collect()
        .unwrap();
    let capacity_before = versions.capacity();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        0,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1024);
    assert!(versions.is_empty());
    assert!(
        versions.capacity() <= capacity_before / 4,
        "empty chain capacity should shrink to a quarter of {capacity_before}, got {}",
        versions.capacity()
    );

    // Small chains are not worth a realloc: capacity at or below the minimum
    // threshold is left untouched even when fully emptied.
    let mut small: RowVersionChain<TursoAllocator> =
        <RowVersionChain<TursoAllocator> as TursoTryWithCapacityExt>::try_with_capacity_ext(16)
            .unwrap();
    small.push(make_version(None, None));
    let capacity_before = small.capacity();
    MvStore::<MvccClock>::gc_version_chain(
        &mut small,
        0,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert!(small.is_empty());
    // A `Vec` keeps a small allocation. The stable build keeps one version
    // inline, so its chain frees the buffer when one version or none remains.
    #[cfg(nightly)]
    assert_eq!(small.capacity(), capacity_before);
    #[cfg(not(nightly))]
    {
        assert_eq!(capacity_before, 16);
        assert_eq!(small.capacity(), 1);
    }
}

/// Most chains hold one version. The stable build stores that version inside
/// the chain, in the allocation of the `Arc` of the chain, so a new row needs
/// no second allocation.
#[cfg(not(nightly))]
#[test]
fn test_one_version_is_stored_inside_its_chain() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();
    let row_id = RowID::new(table_id, RowKey::Int(1));
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .insert(tx, generate_simple_string_row(table_id, 1, "version_0"))
        .unwrap();
    let entry = db.mvcc_store.rows.get(&row_id).unwrap();
    let versions = entry.value().read();
    let chain_start = std::ptr::addr_of!(*versions) as usize;
    let chain = chain_start..chain_start + std::mem::size_of_val(&*versions);
    assert!(chain.contains(&(versions.as_ptr() as usize)));
}

/// A version chain stores its versions inline, so the size of `RowVersion` is a
/// large part of the heap that MVCC uses for each row. The nightly build stores
/// an allocator in each payload, so its sizes are different.
#[cfg(not(nightly))]
#[test]
fn test_row_version_size() {
    assert_eq!(std::mem::size_of::<WalPos>(), 12);
    assert_eq!(std::mem::size_of::<RowVersion>(), 56);
}

/// A version does not store its column count. It reads the count from its
/// record, and a tombstone with an empty payload has no columns.
#[test]
fn test_row_version_column_count_comes_from_the_record() {
    let id = RowID::new(MVTableId::from(-2), RowKey::Int(1));
    let record =
        ImmutableRecord::from_values(&[Value::from_i64(7), Value::build_text("seven")], 2).unwrap();
    let row = Row::new_table_row(id.clone(), record.as_blob()).unwrap();
    let version = RowVersion::new(
        1,
        Some(TxTimestampOrID::Timestamp(1)),
        None,
        row.data,
        false,
    );
    assert_eq!(version.column_count(&id), 2);
    let empty = Row::new_table_row(id.clone(), &[]).unwrap();
    let tombstone = RowVersion::new(
        2,
        None,
        Some(TxTimestampOrID::Timestamp(2)),
        empty.data,
        true,
    );
    assert_eq!(tombstone.column_count(&id), 0);
}

/// A transaction adds a version chain to its write set only on its first write
/// to the chain. Later writes to the same table row or index key find a version
/// of the transaction in the chain and add nothing.
#[test]
fn test_write_set_has_one_entry_for_each_written_chain() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("UPDATE t SET v = 'b' WHERE id = 1").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let tx_id = conn.get_mv_tx_id().unwrap();
    // The table row 1 and the index keys 'a' and 'b'.
    assert_eq!(
        write_set_len_and_distinct_chains(&mvcc_store, tx_id),
        (3, 3)
    );
    conn.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t");
    assert_eq!(rows, vec![vec![Value::from_i64(1), Value::build_text("a")]]);
}

/// A rollback to a savepoint removes the chains that the savepoint added to the
/// write set. A chain that the transaction wrote before the savepoint stays,
/// also when the savepoint wrote to it again. A second rollback to the same
/// savepoint removes the chains that were added after the first rollback.
#[test]
fn test_savepoint_rollback_removes_only_the_chains_that_the_savepoint_added() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let tx_id = conn.get_mv_tx_id().unwrap();
    assert_eq!(
        write_set_len_and_distinct_chains(&mvcc_store, tx_id),
        (2, 2)
    );

    conn.execute("SAVEPOINT sp").unwrap();
    conn.execute("UPDATE t SET v = 'b' WHERE id = 1").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'c')").unwrap();
    assert_eq!(
        write_set_len_and_distinct_chains(&mvcc_store, tx_id),
        (5, 5)
    );
    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(
        write_set_len_and_distinct_chains(&mvcc_store, tx_id),
        (2, 2)
    );

    conn.execute("INSERT INTO t VALUES (3, 'd')").unwrap();
    assert_eq!(
        write_set_len_and_distinct_chains(&mvcc_store, tx_id),
        (4, 4)
    );
    conn.execute("ROLLBACK TO sp").unwrap();
    assert_eq!(
        write_set_len_and_distinct_chains(&mvcc_store, tx_id),
        (2, 2)
    );

    conn.execute("RELEASE sp").unwrap();
    conn.execute("COMMIT").unwrap();
    let rows = get_rows(&conn, "SELECT id, v FROM t");
    assert_eq!(rows, vec![vec![Value::from_i64(1), Value::build_text("a")]]);
}

fn write_set_len_and_distinct_chains(
    mvcc_store: &Arc<crate::MvStore>,
    tx_id: TxID,
) -> (usize, usize) {
    let tx = mvcc_store.txs.get(&tx_id).unwrap();
    let write_set = tx.value().write_set.lock();
    let distinct_chains: HashSet<usize> = write_set
        .iter()
        .map(|(_, row_versions)| Arc::as_ptr(row_versions) as usize)
        .collect();
    (write_set.len(), distinct_chains.len())
}

/// `drop_unused_row_versions_and_slots` removes emptied SkipMap entries.
#[test]
fn test_gc_with_slot_removal_drops_empty_skipmap_entries() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "will_rollback");
    db.mvcc_store.insert(tx1, row).unwrap();
    db.mvcc_store.rollback_tx(
        tx1,
        db.conn.pager.load().clone(),
        &db.conn,
        crate::MAIN_DB_ID,
    );

    let row_id = RowID::new((-2).into(), RowKey::Int(1));
    let entry = db.mvcc_store.rows.get(&row_id).unwrap();
    db.mvcc_store
        .insert_version_raw(&mut entry.value().write(), make_rv(None, None))
        .unwrap();

    // The slot-removing GC variant collects the garbage AND drops the slot.
    // No concurrent writers exist in this test, satisfying the caller contract.
    let dropped = db.mvcc_store.drop_unused_row_versions_and_slots();
    assert_eq!(dropped, 1);
    assert!(
        db.mvcc_store.rows.get(&row_id).is_none(),
        "empty chain slot should be removed from the SkipMap"
    );
}

/// Index chains are swept by their own bounded, resumable cursor
/// (`gc_index_cursor`), just like table rows — a single huge index can't force
/// an unbounded pass. Aborted index garbage (Rule 1) is reclaimable
/// unconditionally, so it lets us exercise the index sweep without depending on
/// checkpoint timing. This asserts the sweep is resumable mid-pass and that
/// driving it in tiny chunks reclaims exactly what a full sweep would.
#[test]
fn test_gc_incremental_reclaims_index_chains_resumably() {
    let db = MvccTestDb::new();
    let conn = &db.conn;
    // Drive GC manually: disable both the checkpoint and the inline-GC trigger.
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("PRAGMA mvcc_gc_threshold = -1").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();

    // Insert many indexed rows in one transaction, then roll back. Rollback
    // keeps the empty slots but removes their versions immediately.
    conn.execute("BEGIN").unwrap();
    for i in 100..200 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'g{i}')"))
            .unwrap();
    }
    conn.execute("ROLLBACK").unwrap();

    // Populate the empty slots with stale versions to exercise the GC cursor
    // directly rather than relying on rollback to manufacture garbage.
    for outer in db.mvcc_store.index_rows.iter() {
        for inner in outer.value().iter() {
            let mut versions = inner.value().write();
            if versions.is_empty() {
                db.mvcc_store
                    .insert_version_raw(&mut versions, make_rv(None, None))
                    .unwrap();
            }
        }
    }

    let count_index_versions = || -> usize {
        db.mvcc_store
            .index_rows
            .iter()
            .map(|outer| {
                outer
                    .value()
                    .iter()
                    .map(|inner| inner.value().read().len())
                    .sum::<usize>()
            })
            .sum()
    };
    let before = count_index_versions();
    assert!(
        before > 8,
        "expected accumulated index garbage chains, got {before}"
    );
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // A single tiny pass is bounded and leaves the index sweep mid-flight —
    // the cursor is parked partway through (there are far more than 4 chains).
    db.mvcc_store.gc_incremental(4);
    assert!(
        db.mvcc_store.debug_gc_snapshot().index_sweep_unfinished,
        "index sweep should resume from a saved cursor, not restart each pass"
    );

    // Drive to convergence in 4-chain chunks: done when a pass reclaims nothing
    // and both cursors have wrapped back to the start.
    let mut passes = 0;
    loop {
        let dropped = db.mvcc_store.gc_incremental(4);
        passes += 1;
        if dropped == 0 {
            let sweeps = db.mvcc_store.debug_gc_snapshot();
            if !sweeps.table_sweep_unfinished && !sweeps.index_sweep_unfinished {
                break;
            }
        }
        assert!(passes < 100_000, "incremental GC failed to converge");
    }

    // Everything the full sweep would reclaim is already gone (table + index).
    assert_eq!(db.mvcc_store.drop_unused_row_versions(), 0);
    let after = count_index_versions();
    assert!(
        after < before,
        "index versions should shrink: before={before} after={after}"
    );

    // The surviving committed row is still correct and index-readable.
    let rows = get_rows(conn, "SELECT id FROM t WHERE v = 'keep'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
}

/// Inline GC must never run concurrently with a stop-the-world checkpoint:
/// the checkpoint reads version chains to flush them to the B-tree and removes
/// empty slots, so a concurrent GC mutating those chains would corrupt the
/// on-disk image (e.g. drop an index version before the checkpoint flushed it,
/// leaving a table row without its index entry). `gc_incremental` enforces this
/// by holding the checkpoint read lock; if the checkpoint holds the write lock,
/// GC must skip entirely. Regression test for the Antithesis integrity-check
/// failure ("row N missing from index ...").
#[test]
fn test_gc_incremental_skips_while_checkpoint_holds_write_lock() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // One reclaimable superseded version (insert+commit, then update+commit).
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .insert(tx, generate_simple_string_row(table_id, 1, "v1"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .update(tx, generate_simple_string_row(table_id, 1, "v2"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    // No active txns -> all reader read-locks released, LWM = MAX.
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    let row_id = RowID::new(table_id, RowKey::Int(1));

    // Simulate a stop-the-world checkpoint by taking the write lock.
    assert!(
        db.mvcc_store.blocking_checkpoint_lock.write(),
        "no readers should remain after commits, so write lock is acquirable"
    );

    // GC cannot take the read lock -> it must skip and reclaim nothing.
    assert_eq!(
        db.mvcc_store
            .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC),
        0,
        "GC must not run while a checkpoint holds the write lock"
    );
    assert_eq!(
        db.mvcc_store
            .rows
            .get(&row_id)
            .unwrap()
            .value()
            .read()
            .len(),
        2,
        "superseded version must survive while the checkpoint holds the lock"
    );

    // Once the checkpoint releases, GC runs and reclaims the superseded version.
    db.mvcc_store.blocking_checkpoint_lock.unlock();
    assert_eq!(
        db.mvcc_store
            .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC),
        1,
        "GC runs once the checkpoint lock is released"
    );
    assert_eq!(
        db.mvcc_store
            .rows
            .get(&row_id)
            .unwrap()
            .value()
            .read()
            .len(),
        1
    );
}

/// Passive incremental GC empties a chain's version vec but leaves the (now empty)
/// SkipMap slot; Finalize `unlink_empty` unlinks slots. Truncate incremental unlinks.
#[test]
fn test_gc_incremental_lazy_leaves_empty_slots() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let table_id: MVTableId = (-2).into();

    // Rollback keeps an empty SkipMap slot. Add stale data to that slot so the
    // incremental GC path is what empties the chain.
    let tx = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    mvcc_store
        .insert(tx, generate_simple_string_row(table_id, 1, "rollback"))
        .unwrap();
    mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn, crate::MAIN_DB_ID);

    let row_id = RowID::new(table_id, RowKey::Int(1));
    let entry = mvcc_store.rows.get(&row_id).unwrap();
    mvcc_store
        .insert_version_raw(&mut entry.value().write(), make_rv(None, None))
        .unwrap();

    // Drive incremental GC to completion.
    for _ in 0..4 {
        mvcc_store.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }

    let entry = mvcc_store.rows.get(&row_id);
    assert!(
        entry.is_some(),
        "Passive lazy path keeps the SkipMap slot in place"
    );
    assert!(
        entry.unwrap().value().read().is_empty(),
        "but the version vec is emptied"
    );
}

/// Elle list-append: `INSERT ... ON CONFLICT(key) DO UPDATE` against a unique
/// index whose only copy of the key lives in the SkipMap.
///
/// CREATE TABLE is checkpointed so the unique-index root is readable. The
/// user row is not. `durable_txid_max` is then raised, which used to be enough
/// for Truncate Rule 3 to reclaim that unwritten current; fallthrough then hit
/// an empty unique index (`["r", "k9", []]`) and a later insert forked a second
/// rowid (`incompatible-order`). Unique `WHERE key =` can hide that fork, so
/// the assertion is a table scan of `rowid`.
#[test]
fn test_gc_unwritten_text_pk_upsert_does_not_fork() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k9', '1')").unwrap();
    assert!(
        unique_index_btree_is_allocated(&mv),
        "CREATE TABLE must publish the unique index so fallthrough can hit an empty btree"
    );

    let older = db.connect();
    older.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&older, "SELECT vals FROM t WHERE key = 'k9'"),
        vec![vec![Value::from_text("1")]]
    );

    mv.durable_txid_max.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();
    assert!(
        skipmap_has_unmaterialized_index_current(&mv),
        "GC must keep a unique-index current that no checkpoint wrote"
    );

    let newer = db.connect();
    newer.execute("BEGIN CONCURRENT").unwrap();
    let read = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k9'");
    assert_eq!(
        read,
        vec![vec![Value::from_text("1")]],
        "unique lookup must still see k9 after GC"
    );
    newer
        .execute(
            "INSERT INTO t (key, vals) VALUES ('k9', '2') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE \
               WHEN vals = '' THEN '2' \
               ELSE vals || ',' || '2' \
             END",
        )
        .unwrap();
    newer.execute("COMMIT").unwrap();
    older.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT rowid, key, vals FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 1, "forked into two rows for k9: {rows:?}");
    assert_eq!(rows[0][1].to_string(), "k9");
    let vals = rows[0][2].to_string();
    assert!(vals.starts_with("1,"), "lost the SkipMap prefix: {vals}");
}

/// Same shape as `test_gc_unwritten_text_pk_upsert_does_not_fork`, but a real
/// checkpoint writes the unique-index key first, so GC may reclaim the chain.
/// Elle list-append must then read its own append back in the same txn instead
/// of `[]` from the B-tree.
#[test]
fn test_gc_reclaimed_text_pk_same_txn_sees_own_append() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k6', '1')").unwrap();
    assert!(
        unique_index_btree_is_allocated(&mv),
        "CREATE TABLE must publish the unique index"
    );

    mv.durable_txid_max.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();

    let newer = db.connect();
    newer.execute("BEGIN CONCURRENT").unwrap();
    let before = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k6'");
    assert_eq!(
        before,
        vec![vec![Value::from_text("1")]],
        "unique lookup must still see k6 after the chain was reclaimed"
    );
    newer
        .execute(
            "INSERT INTO t (key, vals) VALUES ('k6', '2') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE \
               WHEN vals = '' THEN '2' \
               ELSE vals || ',' || '2' \
             END",
        )
        .unwrap();
    let after = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k6'");
    assert_eq!(
        after,
        vec![vec![Value::from_text("1,2")]],
        "same txn must see its own unique append, got {after:?}"
    );
    newer.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT rowid, key, vals FROM t ORDER BY rowid");
    let k6: Vec<_> = rows.iter().filter(|r| r[1].to_string() == "k6").collect();
    assert_eq!(k6.len(), 1, "forked k6: {rows:?}");
    let vals = k6[0][2].to_string();
    assert!(vals.starts_with("1,"), "lost the SkipMap prefix: {vals}");
}

#[test]
fn test_gc_unstamped_text_pk_unique_lookup_still_sees_key() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k7', '1')").unwrap();
    assert!(
        unique_index_btree_is_allocated(&mv),
        "CREATE TABLE must publish the unique index so fallthrough can hit an empty btree"
    );

    mv.durable_txid_max.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();
    assert!(
        skipmap_has_unmaterialized_index_current(&mv),
        "Passive GC must not drop an index current that was never written"
    );

    let read = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k7'");
    assert_eq!(
        read,
        vec![vec![Value::from_text("1")]],
        "unique lookup must still see k7 after unstamped Passive GC"
    );
}

/// A reader that has already selected a multi-column row must keep seeing every
/// column after GC. No checkpoint ran, so the SkipMap holds the only copy and
/// GC must keep it even though `durable_txid_max` claims everything is durable.
#[test]
fn test_gc_keeps_columns_of_positioned_reader() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 20, 30)")
        .unwrap();

    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let before = get_rows(&reader, "SELECT a, b, c FROM t WHERE id = 1");
    assert_eq!(
        before,
        vec![vec![
            Value::from_i64(10),
            Value::from_i64(20),
            Value::from_i64(30)
        ]]
    );

    let versions_before_gc = skipmap_version_count(&mv);
    mv.durable_txid_max.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();
    assert_eq!(
        skipmap_version_count(&mv),
        versions_before_gc,
        "GC must keep live versions no checkpoint ever wrote"
    );

    let after = get_rows(&reader, "SELECT a, b, c FROM t WHERE id = 1");
    assert_eq!(after, before, "must not get NULL or empty columns after GC");
    reader.execute("COMMIT").unwrap();
}

/// SkipMap `read` after incremental GC on a chain no checkpoint wrote. Dropping
/// the sole current here would make this return None.
#[test]
fn test_gc_incremental_keeps_held_reader_row() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();
    let row_id = RowID::new(table_id, RowKey::Int(1));

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = generate_simple_string_row(table_id, 1, "keep_me");
    db.mvcc_store.insert(tx1, row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    db.mvcc_store
        .durable_txid_max
        .store(u64::MAX, Ordering::SeqCst);

    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    assert_eq!(db.mvcc_store.read(tx2, &row_id).unwrap().unwrap(), row);

    db.mvcc_store
        .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    db.mvcc_store.drop_unused_row_versions();

    assert_eq!(
        db.mvcc_store.read(tx2, &row_id).unwrap().unwrap(),
        row,
        "held reader still reads the current version"
    );
    let versions = db.mvcc_store.rows.get(&row_id).unwrap();
    let versions = versions.value().read();
    assert_eq!(versions.len(), 1);
    assert_eq!(
        versions[0].materialized_at(),
        crate::mvcc::database::WalPos::ORIGIN
    );
    assert_eq!(versions[0].end(), None);
}

// ─── GC quickcheck property tests ────────────────────────────────────────

/// Represents a version chain entry for quickcheck.
#[derive(Debug, Clone)]
struct ArbitraryVersionChain {
    versions: RowVersionChain<TursoAllocator>,
    lwm: u64,
    ckpt_max: u64,
}

/// Generates RowVersions matching realistic MVCC states.
/// Only produces valid (begin, end) combinations that can actually occur.
fn arbitrary_row_version(g: &mut Gen) -> RowVersion {
    // Weight toward realistic states:
    // 32% current (Timestamp, None), 24% superseded (Timestamp, Timestamp),
    // 8% aborted (None, None), 8% pending insert (TxID, None),
    // 8% pending delete (Timestamp, TxID), 20% B-tree tombstone (None, Timestamp)
    let kind = u8::arbitrary(g) % 25;
    let (begin, end) = match kind {
        0..=7 => {
            // Current committed version
            let b = u64::arbitrary(g) % 20 + 1;
            (Some(TxTimestampOrID::Timestamp(b)), None)
        }
        8..=13 => {
            // Superseded version
            let b = u64::arbitrary(g) % 15 + 1;
            let e = b + u64::arbitrary(g) % 10 + 1;
            (
                Some(TxTimestampOrID::Timestamp(b)),
                Some(TxTimestampOrID::Timestamp(e)),
            )
        }
        14..=15 => {
            // Aborted garbage
            (None, None)
        }
        16..=17 => {
            // Pending insert
            let t = u64::arbitrary(g) % 20 + 1;
            (Some(TxTimestampOrID::TxID(t)), None)
        }
        18..=19 => {
            // Pending delete
            let b = u64::arbitrary(g) % 15 + 1;
            let t = u64::arbitrary(g) % 20 + 1;
            (
                Some(TxTimestampOrID::Timestamp(b)),
                Some(TxTimestampOrID::TxID(t)),
            )
        }
        20..=24 => {
            // B-tree tombstone (begin=None, end=e) — row existed before MVCC, then deleted
            let e = u64::arbitrary(g) % 20 + 1;
            (None, Some(TxTimestampOrID::Timestamp(e)))
        }
        _ => unreachable!(),
    };

    RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(begin),
        end: crate::mvcc::database::PackedTs::pack(end),
        data: generate_simple_string_row((-2).into(), 1, "qc").data,
        btree_resident: bool::arbitrary(g),
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

impl Arbitrary for ArbitraryVersionChain {
    fn arbitrary(g: &mut Gen) -> Self {
        // 1..8 versions (no empty chains — they trivially pass all properties)
        let len = usize::arbitrary(g) % 8 + 1;
        let versions: RowVersionChain<TursoAllocator> = (0..len)
            .map(|_| arbitrary_row_version(g))
            .try_collect()
            .unwrap();
        // Include boundary values with ~20% probability each.
        let lwm = match u8::arbitrary(g) % 5 {
            0 => 0,
            1 => u64::MAX, // blocking checkpoint case
            _ => u64::arbitrary(g) % 30,
        };
        let ckpt_max = match u8::arbitrary(g) % 5 {
            0 => 0,        // no checkpoint has run
            1 => u64::MAX, // everything checkpointed
            _ => u64::arbitrary(g) % 30,
        };
        Self {
            versions,
            lwm,
            ckpt_max,
        }
    }
}

/// GC only removes versions — it never synthesizes new ones. For any input chain,
/// the output must be a subset (same length or shorter).
#[quickcheck]
fn prop_gc_never_increases_version_count(chain: ArbitraryVersionChain) -> bool {
    let before = chain.versions.len();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    versions.len() <= before
}

/// Running GC twice with the same LWM and ckpt_max must produce the same result
/// as running it once. A non-idempotent GC would indicate that GC output triggers
/// further removals on re-evaluation, which means the first pass missed something.
/// Compares actual version content (begin/end), not just chain length.
#[quickcheck]
fn prop_gc_is_idempotent(chain: ArbitraryVersionChain) -> bool {
    let mut v1 = chain.versions.clone();
    MvStore::<MvccClock>::gc_version_chain(
        &mut v1,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    let snapshot = v1.clone();
    MvStore::<MvccClock>::gc_version_chain(
        &mut v1,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // Compare content, not just length — a swap bug would pass a length-only check.
    v1.len() == snapshot.len()
        && v1
            .iter()
            .zip(snapshot.iter())
            .all(|(a, b)| a.begin() == b.begin() && a.end() == b.end())
}

/// Aborted garbage (begin=None, end=None) is invisible to every transaction and
/// has no B-tree implications. GC must remove all of it unconditionally (Rule 1).
/// No aborted garbage should survive a GC pass, regardless of LWM or ckpt_max.
#[quickcheck]
fn prop_gc_removes_all_aborted_garbage(chain: ArbitraryVersionChain) -> bool {
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    versions
        .iter()
        .all(|rv| !matches!((&rv.begin(), &rv.end()), (None, None)))
}

/// Uncommitted inserts (begin=TxID, end=None) belong to an in-flight transaction.
/// GC cannot know whether it will commit or abort, so it must never touch them.
/// Verifies all such versions survive GC regardless of other chain contents.
#[quickcheck]
fn prop_gc_retains_txid_begins(chain: ArbitraryVersionChain) -> bool {
    let txid_begins_before: usize = chain
        .versions
        .iter()
        .filter(|rv| matches!(&rv.begin(), Some(TxTimestampOrID::TxID(_))) && rv.end().is_none())
        .count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    let txid_begins_after: usize = versions
        .iter()
        .filter(|rv| matches!(&rv.begin(), Some(TxTimestampOrID::TxID(_))) && rv.end().is_none())
        .count();
    // Active uncommitted versions (begin=TxID, end=None) are never aborted garbage
    // and don't match rule 2 or 3, so they should be retained.
    txid_begins_after == txid_begins_before
}

/// Uncommitted deletions (end=TxID) represent a pending delete by an in-flight
/// transaction. Rule 2 only matches end=Timestamp, so these must be retained.
/// Verifies GC never removes versions with TxID end markers.
#[quickcheck]
fn prop_gc_retains_txid_ends(chain: ArbitraryVersionChain) -> bool {
    // Versions with end=TxID and non-None begin are not matched by any removal
    // rule (rule 1 requires (None,None), rule 2 requires end=Timestamp).
    let filter = |rv: &&RowVersion| {
        matches!(&rv.end(), Some(TxTimestampOrID::TxID(_))) && rv.begin().is_some()
    };
    let txid_ends_before: usize = chain.versions.iter().filter(filter).count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    let txid_ends_after: usize = versions.iter().filter(filter).count();
    txid_ends_after == txid_ends_before
}

/// Current versions (begin=Timestamp(b), end=None) are not removable before they
/// are checkpointed. Forces ckpt_max=0 and verifies all committed current versions
/// survive.
#[quickcheck]
fn prop_gc_current_versions_protected_before_checkpoint(chain: ArbitraryVersionChain) -> bool {
    let current_before: usize = chain
        .versions
        .iter()
        .filter(|rv| {
            matches!(
                (&rv.begin(), &rv.end()),
                (Some(TxTimestampOrID::Timestamp(_)), None)
            )
        })
        .count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    let current_after: usize = versions
        .iter()
        .filter(|rv| {
            matches!(
                (&rv.begin(), &rv.end()),
                (Some(TxTimestampOrID::Timestamp(_)), None)
            )
        })
        .count();
    current_after == current_before
}

/// When a row has been deleted but the deletion isn't checkpointed yet, the
/// tombstone (superseded version with end > ckpt_max) is the only thing preventing
/// the dual cursor from reading a stale B-tree row. If GC empties such a chain,
/// the deleted row reappears. Verifies GC never empties a chain that has
/// uncheckpointed tombstones and no committed current version to take over.
#[quickcheck]
fn prop_gc_tombstone_guard_preserves_btree_safety(chain: ArbitraryVersionChain) -> bool {
    // If a chain has only superseded versions (no committed current) and at
    // least one has e > ckpt_max, GC must not empty the chain — removing all
    // versions would let the dual cursor fall through to a stale B-tree row.
    let mut versions = chain.versions.clone();
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );

    // Check: if pre-GC chain had no committed current version AND had a
    // superseded version with e > ckpt_max, post-GC chain must not be empty.
    let had_committed_current = chain
        .versions
        .iter()
        .any(|rv| rv.end().is_none() && matches!(&rv.begin(), Some(TxTimestampOrID::Timestamp(_))));
    let had_uncheckpointed_tombstone = chain
        .versions
        .iter()
        .any(|rv| matches!(&rv.end(), Some(TxTimestampOrID::Timestamp(e)) if *e > chain.ckpt_max));
    // Only non-garbage versions matter (aborted garbage is always removed first)
    let had_non_garbage = chain
        .versions
        .iter()
        .any(|rv| !matches!((&rv.begin(), &rv.end()), (None, None)));

    if !had_committed_current && had_uncheckpointed_tombstone && had_non_garbage {
        !versions.is_empty()
    } else {
        true // no constraint in this case
    }
}

/// Superseded versions without a committed current version are dangerous — their
/// presence tells the dual cursor "this row was modified" but there's no current
/// version to serve reads. GC must only leave such orphans when they're justifiably
/// retained: still visible to a reader (e > lwm), guarding an uncheckpointed
/// deletion (e > ckpt_max).
#[quickcheck]
fn prop_gc_no_orphaned_superseded_versions(chain: ArbitraryVersionChain) -> bool {
    // After GC, if a chain has superseded versions without a committed current
    // version, each superseded version must be justifiably retained:
    // - e > lwm (Rule 2 didn't fire — still visible to some reader)
    // - e > ckpt_max (tombstone guard — deletion not yet in B-tree)
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );

    let has_committed_current = versions
        .iter()
        .any(|rv| rv.end().is_none() && matches!(&rv.begin(), Some(TxTimestampOrID::Timestamp(_))));
    let has_superseded = versions.iter().any(|rv| {
        matches!(
            (&rv.begin(), &rv.end()),
            (
                Some(TxTimestampOrID::Timestamp(_)),
                Some(TxTimestampOrID::Timestamp(_))
            )
        )
    });

    if has_superseded && !has_committed_current {
        versions
            .iter()
            .filter(|rv| {
                matches!(
                    (&rv.begin(), &rv.end()),
                    (
                        Some(TxTimestampOrID::Timestamp(_)),
                        Some(TxTimestampOrID::Timestamp(_))
                    )
                )
            })
            .all(|rv| {
                if let Some(TxTimestampOrID::Timestamp(e)) = &rv.end() {
                    *e > chain.lwm || *e > chain.ckpt_max
                } else {
                    false
                }
            })
    } else {
        true
    }
}

// ─── End-to-end GC + dual cursor tests ───────────────────────────────────

/// Regression test for MVCC concurrent commit spinning inside one step.
///
/// A COMMIT that finds `pager_commit_lock` held parks on a completion the
/// lock holder finishes. `step()` must return right away with IO, never
/// spin inside a single call, or cooperative schedulers deadlock.
///
/// We simulate lock contention by pre-acquiring pager_commit_lock before
/// calling COMMIT.
#[test]
fn test_concurrent_commit_yield_spin() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mv_store = db.get_mvcc_store();
    let coordinator = &mv_store.commit_coordinator;
    assert!(coordinator.pager_commit_lock.write(), "should acquire lock");

    let mut stmt = conn.prepare("COMMIT").unwrap();
    assert!(
        matches!(stmt.step().unwrap(), crate::StepResult::IO),
        "step() should park when pager_commit_lock is contended"
    );
    assert_eq!(coordinator.parked_tickets().len(), 1);

    coordinator.unlock_pager_commit_lock();
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Done => break,
            crate::StepResult::IO => {}
            _ => {}
        }
    }

    // Verify the insert is visible
    let rows = get_rows(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
}

fn abandon_commit_after_first_io(conn: &Arc<Connection>, mv_store: &Arc<crate::MvStore>) {
    let coordinator = &mv_store.commit_coordinator;
    assert!(
        coordinator.pager_commit_lock.write(),
        "should acquire commit lock"
    );

    let mut stmt = conn.prepare("COMMIT").unwrap();
    assert!(
        matches!(stmt.step().unwrap(), crate::StepResult::IO),
        "COMMIT should park while the commit lock is held",
    );
    assert_eq!(coordinator.parked_tickets().len(), 1);

    drop(stmt);
    assert!(
        coordinator.parked_tickets().is_empty(),
        "dropping a parked COMMIT withdraws it from the queue"
    );
    coordinator.unlock_pager_commit_lock();
    conn.close().unwrap();
}

/// if a txn made some inserts, then aborted (or abandoned due to some IO issue), then those
/// inserted rows should not be visible
#[test]
fn test_abandoned_commit_rolls_back_insert() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();

    let mv_store = db.get_mvcc_store();
    abandon_commit_after_first_io(&conn, &mv_store);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id FROM t WHERE id = 1");
    assert!(
        rows.is_empty(),
        "row from abandoned INSERT commit remained visible: {rows:?}",
    );
    observer.close().unwrap();
}

/// if a txn deleted some existing rows, but then aborted (or abandoned due to some IO issue), then
/// those rows should not become deleted
#[test]
fn test_abandoned_commit_rolls_back_delete() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    let mv_store = db.get_mvcc_store();
    abandon_commit_after_first_io(&conn, &mv_store);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id, v FROM t WHERE id = 1");
    assert_eq!(
        rows,
        vec![vec![
            Value::Numeric(Numeric::Integer(1)),
            Value::Text(Text::new("seed".to_string())),
        ]],
        "row disappeared after abandoned DELETE commit: {rows:?}",
    );
    observer.close().unwrap();
}

/// Encrypted MVCC: write rows, restart with same key, verify recovery replays them.
/// Then swap to a wrong key and verify that restart fails.
#[test]
fn test_mvcc_encrypted_log_recovery_and_wrong_key() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    write_synthetic_row(&db, "encrypted_value");

    // --- Verify the raw log file is encrypted (no plaintext leakage) ---
    {
        let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
        let log_bytes = std::fs::read(&log_path).expect("MVCC log file should exist");
        assert!(
            log_bytes.len() > 56,
            "MVCC log should contain data beyond the header"
        );
        let plaintext = b"encrypted_value";
        assert!(
            !log_bytes.windows(plaintext.len()).any(|w| w == plaintext),
            "MVCC log must not contain plaintext data when encryption is enabled"
        );
    }

    // --- Restart with correct key: recovery should replay the encrypted log ---
    db.restart();
    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let max_root_page = get_rows(
            &conn,
            "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
        )[0][0]
            .as_int()
            .unwrap();
        let synthetic_table_id = MVTableId::new(-(max_root_page + 100));
        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = mvcc_store
            .read(tx_id, &RowID::new(synthetic_table_id, RowKey::Int(1)))
            .unwrap()
            .unwrap();
        let record = get_record_value(&row);
        match record.get_value(0).unwrap() {
            ValueRef::Text(text) => assert_eq!(text.as_str(), "encrypted_value"),
            other => panic!("Expected Text, got {other:?}"),
        }
        conn.close().unwrap();
    }

    // --- Restart with wrong key: should fail ---
    let wrong_key = "ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    db.enc_opts = Some(crate::EncryptionOpts {
        cipher: "aes256gcm".to_string(),
        hexkey: wrong_key.to_string(),
    });
    assert!(
        db.restart_result().is_err(),
        "Expected error when reopening encrypted MVCC DB with wrong key"
    );
}

#[test]
fn test_encrypted_recovery_large_payload_multi_chunk() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let large_value = "x".repeat(ENCRYPTED_PAYLOAD_CHUNK_SIZE * 3);
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute(format!("INSERT INTO t VALUES (1, '{large_value}')"))
            .unwrap();
    }

    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
    assert!(log_path.exists(), "db-log should exist before restart");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    db.restart();
    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT id, length(v), substr(v, 1, 16), substr(v, length(v) - 15, 16) FROM t",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), large_value.len() as i64);
    assert_eq!(rows[0][2].to_string(), "xxxxxxxxxxxxxxxx");
    assert_eq!(rows[0][3].to_string(), "xxxxxxxxxxxxxxxx");
}

#[test]
fn test_encrypted_recovery_corrupted_later_chunk_keeps_checkpointed_prefix() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let large_value = "z".repeat(ENCRYPTED_PAYLOAD_CHUNK_SIZE * 3);
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'survives')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute(format!("INSERT INTO t VALUES (2, '{large_value}')"))
            .unwrap();
    }

    let mut log_bytes = std::fs::read(&log_path).expect("db-log should exist");
    let payload_size = u64::from_le_bytes(
        log_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    let chunk_count = payload_size.div_ceil(ENCRYPTED_PAYLOAD_CHUNK_SIZE);
    assert!(
        chunk_count >= 3,
        "expected multi-chunk encrypted recovery tail"
    );

    let enc_ctx = crate::storage::encryption::EncryptionContext::new(
        crate::storage::encryption::CipherMode::Aes256Gcm,
        &EncryptionKey::from_hex_string(hex_key).unwrap(),
        4096,
    )
    .unwrap();
    let first_chunk_on_disk_size =
        ENCRYPTED_PAYLOAD_CHUNK_SIZE + enc_ctx.tag_size() + enc_ctx.nonce_size();
    let corrupt_offset = LOG_HDR_SIZE + TX_BASE_HEADER_SIZE + first_chunk_on_disk_size + 1;
    log_bytes[corrupt_offset] ^= 0xFF;
    std::fs::write(&log_path, &log_bytes).unwrap();

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "survives");
}

/// Read the raw db-log file and verify every TX frame portable_changes can be decrypted.
/// Panics if the file is missing, has no frames, or any portable_changes fails to decrypt.
fn assert_log_payloads_decrypt(
    log_path: &std::path::Path,
    hex_key: &str,
    cipher: crate::storage::encryption::CipherMode,
) {
    use crate::storage::encryption::EncryptionContext;

    let log_bytes = std::fs::read(log_path).expect("db-log file should exist");
    assert!(
        log_bytes.len() > LOG_HDR_SIZE,
        "db-log should contain data beyond the header"
    );

    let key = EncryptionKey::from_hex_string(hex_key).unwrap();
    let enc_ctx = EncryptionContext::new(cipher, &key, 4096).unwrap();
    let nonce_size = enc_ctx.nonce_size();
    let tag_size = enc_ctx.tag_size();

    // Parse salt from log header (bytes 8..16, little-endian u64)
    let salt = u64::from_le_bytes(log_bytes[8..16].try_into().unwrap());

    let mut offset = LOG_HDR_SIZE;
    let mut frame_count = 0;

    while offset + TX_BASE_HEADER_SIZE + TX_TRAILER_SIZE <= log_bytes.len() {
        // TX Header: frame_magic(4) | payload_size(8) | op_count(4) | commit_ts(8)
        // | extension_size(8) | extension_record_count(4) | frame_flags(4)
        let frame_magic = u32::from_le_bytes(log_bytes[offset..offset + 4].try_into().unwrap());
        let (header_size, extension_size) = if frame_magic == FRAME_MAGIC {
            (TX_BASE_HEADER_SIZE, 0)
        } else if frame_magic == EXT_FRAME_MAGIC {
            if offset + TX_EXT_HEADER_SIZE + TX_TRAILER_SIZE > log_bytes.len() {
                break;
            }
            (
                TX_EXT_HEADER_SIZE,
                u64::from_le_bytes(log_bytes[offset + 24..offset + 32].try_into().unwrap())
                    as usize,
            )
        } else {
            break; // not a valid frame
        };
        let payload_size =
            u64::from_le_bytes(log_bytes[offset + 4..offset + 12].try_into().unwrap()) as usize;
        let op_count = u32::from_le_bytes(log_bytes[offset + 12..offset + 16].try_into().unwrap());
        let commit_ts = u64::from_le_bytes(log_bytes[offset + 16..offset + 24].try_into().unwrap());

        let encrypted_plaintext_size = payload_size + extension_size;
        let mut payload_offset = offset + header_size;
        let chunk_count = if encrypted_plaintext_size == 0 {
            0
        } else {
            encrypted_plaintext_size.div_ceil(ENCRYPTED_PAYLOAD_CHUNK_SIZE)
        };

        let mut frame_complete = true;
        for chunk_index in 0..chunk_count {
            let chunk_plaintext_len = (encrypted_plaintext_size
                - chunk_index * ENCRYPTED_PAYLOAD_CHUNK_SIZE)
                .min(ENCRYPTED_PAYLOAD_CHUNK_SIZE);
            let chunk_on_disk_size = chunk_plaintext_len + tag_size + nonce_size;
            if payload_offset + chunk_on_disk_size + TX_TRAILER_SIZE > log_bytes.len() {
                frame_complete = false;
                break;
            }

            let blob = &log_bytes[payload_offset..payload_offset + chunk_on_disk_size];
            let ciphertext = &blob[..chunk_plaintext_len + tag_size];
            let nonce = &blob[chunk_plaintext_len + tag_size..];

            let mut aad = [0u8; 32];
            aad[..8].copy_from_slice(&salt.to_le_bytes());
            if chunk_index + 1 == chunk_count {
                aad[8..16].copy_from_slice(&(encrypted_plaintext_size as u64).to_le_bytes());
            }
            aad[16..20].copy_from_slice(&op_count.to_le_bytes());
            aad[20..28].copy_from_slice(&commit_ts.to_le_bytes());
            aad[28..32].copy_from_slice(&(chunk_index as u32).to_le_bytes());

            enc_ctx
                .decrypt_chunk(ciphertext, nonce, &aad)
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to decrypt frame {frame_count} chunk {chunk_index} at offset {offset}: {e}"
                    )
                });

            payload_offset += chunk_on_disk_size;
        }
        if !frame_complete {
            break;
        }

        frame_count += 1;
        offset = payload_offset + TX_TRAILER_SIZE; // skip encrypted body and trailer
    }

    assert!(
        frame_count > 0,
        "db-log should contain at least one TX frame"
    );
}

#[cfg(feature = "conn_raw_api")]
fn collect_mvcc_portable_change_bytes(conn: &Arc<Connection>) -> Vec<u8> {
    let mv_store = conn
        .mv_store()
        .as_ref()
        .expect("test database must be in MVCC mode")
        .clone();
    let io = conn.get_pager().io.clone();
    let mut reader = StreamingLogicalLogReader::new(mv_store.get_logical_log_file(), None);
    reader.read_header(&io).unwrap();

    let mut portable_changes = Vec::new();
    while let Some(frame) = io.block(|| reader.next_portable_changes()).unwrap() {
        portable_changes.extend_from_slice(&frame.payload);
    }
    portable_changes
}

#[cfg(feature = "conn_raw_api")]
fn collect_mvcc_recovery_ops(conn: &Arc<Connection>) -> Vec<ParsedOp> {
    let mv_store = conn
        .mv_store()
        .as_ref()
        .expect("test database must be in MVCC mode")
        .clone();
    let io = conn.get_pager().io.clone();
    let mut reader = StreamingLogicalLogReader::new(mv_store.get_logical_log_file(), None);
    reader.read_header(&io).unwrap();

    let mut ops = Vec::new();
    while let Some(frame_ops) = reader.next_frame_blocking(&io).unwrap() {
        ops.extend(frame_ops);
    }
    ops
}

#[cfg(feature = "conn_raw_api")]
fn read_proto_varint(bytes: &[u8], offset: &mut usize) -> u64 {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let byte = bytes[*offset];
        *offset += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}

#[cfg(feature = "conn_raw_api")]
fn decode_proto_sint64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

#[cfg(feature = "conn_raw_api")]
fn skip_proto_field(bytes: &[u8], offset: &mut usize, wire_type: u64) {
    match wire_type {
        0 => {
            let _ = read_proto_varint(bytes, offset);
        }
        2 => {
            let len = read_proto_varint(bytes, offset) as usize;
            *offset += len;
        }
        other => panic!("unsupported protobuf wire type in test decoder: {other}"),
    }
}

#[cfg(feature = "conn_raw_api")]
#[derive(Clone, Debug)]
struct DecodedObjectMap {
    name: String,
}

#[cfg(feature = "conn_raw_api")]
#[derive(Clone, Debug, Default)]
struct DecodedPortableTxn {
    objects: Vec<DecodedObjectMap>,
    metadata: std::collections::HashMap<String, String>,
}

#[cfg(feature = "conn_raw_api")]
fn decode_object_map(bytes: &[u8], strings: &[String]) -> Option<DecodedObjectMap> {
    let mut offset = 0usize;
    let mut mv_table_id = None;
    let mut name = String::new();
    while offset < bytes.len() {
        let key = read_proto_varint(bytes, &mut offset);
        let field = key >> 3;
        let wire_type = key & 7;
        match (field, wire_type) {
            (1, 0) => {
                mv_table_id = Some(decode_proto_sint64(read_proto_varint(bytes, &mut offset)))
            }
            (2, 0) => {
                let idx = read_proto_varint(bytes, &mut offset) as usize;
                name = strings.get(idx).cloned().unwrap_or_default();
            }
            _ => skip_proto_field(bytes, &mut offset, wire_type),
        }
    }
    mv_table_id?;
    Some(DecodedObjectMap { name })
}

#[cfg(feature = "conn_raw_api")]
fn decode_metadata(bytes: &[u8], strings: &[String]) -> Option<(String, String)> {
    let mut offset = 0usize;
    let mut key = None;
    let mut value = None;
    while offset < bytes.len() {
        let field_key = read_proto_varint(bytes, &mut offset);
        let field = field_key >> 3;
        let wire_type = field_key & 7;
        match (field, wire_type) {
            (1, 0) => {
                let idx = read_proto_varint(bytes, &mut offset) as usize;
                key = strings.get(idx).cloned();
            }
            (2, 0) => {
                let idx = read_proto_varint(bytes, &mut offset) as usize;
                value = strings.get(idx).cloned();
            }
            _ => skip_proto_field(bytes, &mut offset, wire_type),
        }
    }
    Some((key?, value?))
}

#[cfg(feature = "conn_raw_api")]
fn decode_portable_change_txns(portable_changes: &[u8]) -> Vec<DecodedPortableTxn> {
    let mut txns = Vec::new();
    let mut offset = 0usize;
    while offset < portable_changes.len() {
        let txn_len = read_proto_varint(portable_changes, &mut offset) as usize;
        let txn_end = offset + txn_len;
        let txn = &portable_changes[offset..txn_end];
        offset = txn_end;

        let mut strings = Vec::new();
        let mut object_blobs = Vec::new();
        let mut meta_blobs = Vec::new();
        let mut txn_offset = 0usize;
        while txn_offset < txn.len() {
            let key = read_proto_varint(txn, &mut txn_offset);
            let field = key >> 3;
            let wire_type = key & 7;
            match (field, wire_type) {
                (12, 2) => {
                    let len = read_proto_varint(txn, &mut txn_offset) as usize;
                    strings.push(
                        String::from_utf8(txn[txn_offset..txn_offset + len].to_vec()).unwrap(),
                    );
                    txn_offset += len;
                }
                (13, 2) => {
                    let len = read_proto_varint(txn, &mut txn_offset) as usize;
                    object_blobs.push(txn[txn_offset..txn_offset + len].to_vec());
                    txn_offset += len;
                }
                (14, 2) => {
                    let len = read_proto_varint(txn, &mut txn_offset) as usize;
                    meta_blobs.push(txn[txn_offset..txn_offset + len].to_vec());
                    txn_offset += len;
                }
                _ => skip_proto_field(txn, &mut txn_offset, wire_type),
            }
        }
        let objects = object_blobs
            .iter()
            .filter_map(|object| decode_object_map(object, &strings))
            .collect();
        let mut metadata = std::collections::HashMap::new();
        for meta in &meta_blobs {
            if let Some((key, value)) = decode_metadata(meta, &strings) {
                metadata.insert(key, value);
            }
        }
        txns.push(DecodedPortableTxn { objects, metadata });
    }
    txns
}

#[cfg(feature = "conn_raw_api")]
fn decoded_object_maps(txns: &[DecodedPortableTxn]) -> Vec<&DecodedObjectMap> {
    txns.iter().flat_map(|txn| txn.objects.iter()).collect()
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_encoder_matches_metadata_wire_golden() {
    let mut builder = PortableLogicalBuilder::new();
    builder.add_metadata("client", "client-a").unwrap();
    builder
        .add_object_map(PortableObjectMapEntry {
            mv_table_id: -5,
            name: "items",
        })
        .unwrap();
    let encoded = builder.finish().unwrap();

    assert_eq!(
        encoded,
        vec![
            0x62, 0x06, b'c', b'l', b'i', b'e', b'n', b't', 0x62, 0x08, b'c', b'l', b'i', b'e',
            b'n', b't', b'-', b'a', 0x62, 0x05, b'i', b't', b'e', b'm', b's', 0x6a, 0x04, 0x08,
            0x09, // mv_table_id = -5
            0x10, 0x02, // name_ref = "items"
            0x72, 0x04, 0x08, 0x00, // metadata key_ref = "client"
            0x10, 0x01, // metadata value_ref = "client-a"
        ]
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_emit_header_only_commits() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn.execute("PRAGMA user_version = 42").unwrap();
    db.conn.execute("PRAGMA application_id = 1337").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let recovery_ops = collect_mvcc_recovery_ops(&db.conn);

    // Header-only commits touch no user object, but a portable-enabled writer
    // still emits the extension block so readers can tell an empty change set
    // apart from pre-portable history. The payload therefore carries the frame
    // cursor and commit timestamp and nothing else.
    let txns = decode_portable_change_txns(&portable_changes);
    assert_eq!(txns.len(), 2);
    assert!(decoded_object_maps(&txns).is_empty());
    assert!(txns.iter().all(|txn| txn.metadata.is_empty()));
    assert_eq!(
        recovery_ops
            .iter()
            .filter(|op| matches!(op, ParsedOp::UpdateHeader { .. }))
            .count(),
        2
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_resolve_user_table_after_cross_connection_checkpoint() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(EngineDialect)).unwrap();
    let creator = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    creator.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    creator.set_portable_logical_changes_enabled(true);
    creator
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    let rows = get_rows(
        &creator,
        "SELECT rootpage FROM sqlite_schema WHERE name = 'items'",
    );
    let initial_rootpage = rows[0][0].as_int().unwrap();
    assert!(initial_rootpage < 0);

    let checkpoint = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    checkpoint
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let rows = get_rows(
        &checkpoint,
        "SELECT rootpage FROM sqlite_schema WHERE name = 'items'",
    );
    let rootpage = rows[0][0].as_int().unwrap();
    assert!(rootpage > 0);
    {
        let schema = checkpoint.db.clone_schema();
        assert_eq!(schema.table_name_for_root_page(rootpage), Some("items"));
        assert_eq!(schema.table_name_for_root_page(initial_rootpage), None);
    }
    {
        let schema = checkpoint.current_schema();
        assert_eq!(schema.table_name_for_root_page(rootpage), Some("items"));
        assert_eq!(schema.table_name_for_root_page(initial_rootpage), None);
    }

    let writer = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    writer.set_portable_logical_changes_enabled(true);
    writer
        .execute("INSERT INTO items(id, payload) VALUES (1, 'after-checkpoint')")
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&writer);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(
        objects.iter().any(|object| object.name == "items"),
        "DML-only portable frame should resolve user table after cross-connection checkpoint"
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_delete_carries_pk_projection_not_old_record() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id TEXT PRIMARY KEY, payload TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES ('item-a', 'alpha')")
        .unwrap();
    db.conn
        .execute("DELETE FROM items WHERE id = 'item-a'")
        .unwrap();

    let recovery_ops = collect_mvcc_recovery_ops(&db.conn);

    let delete_pk_record = recovery_ops.iter().find_map(|op| match op {
        ParsedOp::DeleteTable {
            rowid,
            record_bytes,
            pk_record_bytes,
            ..
        } if rowid.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID => {
            assert!(
                record_bytes.is_empty(),
                "data DELETE must not duplicate the full row record"
            );
            Some(pk_record_bytes.clone())
        }
        _ => None,
    });
    let delete_pk_record = delete_pk_record.expect("expected data DELETE op");
    let pk_values = ImmutableRecord::from_bin_record(
        crate::types::value_blob_from_slice(&delete_pk_record).expect(crate::alloc::ALLOC_ERR_MSG),
    )
    .get_values_owned()
    .unwrap();
    assert_eq!(
        pk_values,
        vec![Value::Text(Text::new("item-a".to_string()))]
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_delete_of_checkpointed_row_carries_pk_projection() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id TEXT PRIMARY KEY, payload TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES ('item-a', 'alpha')")
        .unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    db.conn
        .execute("DELETE FROM items WHERE id = 'item-a'")
        .unwrap();

    let delete_pk_record = collect_mvcc_recovery_ops(&db.conn)
        .into_iter()
        .find_map(|op| match op {
            ParsedOp::DeleteTable {
                rowid,
                pk_record_bytes,
                ..
            } if rowid.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID => Some(pk_record_bytes),
            _ => None,
        })
        .expect("expected data DELETE op");
    let pk_values = ImmutableRecord::from_bin_record(
        crate::types::value_blob_from_slice(&delete_pk_record).expect(crate::alloc::ALLOC_ERR_MSG),
    )
    .get_values_owned()
    .unwrap();
    assert_eq!(
        pk_values,
        vec![Value::Text(Text::new("item-a".to_string()))]
    );
}

/// Encrypted version of test_recovery_checkpoint_then_more_writes.
/// Checkpoint some rows, write more without checkpointing, restart, verify all rows survive.
#[test]
fn test_encrypted_recovery_checkpoint_then_more_writes() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
    assert!(log_path.exists(), "db-log file should exist before restart");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
    assert_eq!(rows[2][1].to_string(), "c");
}

/// Write, restart, write more, restart again, verify all data accumulates correctly.
#[test]
fn test_encrypted_recovery_multiple_restart_cycles() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    // Cycle 1: create table + insert
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    }

    assert!(log_path.exists(), "db-log file should exist after cycle 1");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );
    db.restart();

    // Cycle 2: insert more rows
    {
        let conn = db.connect();
        conn.execute("INSERT INTO t VALUES (2, 'second')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'third')").unwrap();
    }

    db.restart();

    // Verify all rows survived two restart cycles
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1].to_string(), "first");
    assert_eq!(rows[1][1].to_string(), "second");
    assert_eq!(rows[2][1].to_string(), "third");
}

/// Corrupt ciphertext bytes in the encrypted log payload. Recovery should treat the
/// corrupted frame as a torn tail and stop cleanly without losing earlier valid frames.
#[test]
fn test_encrypted_recovery_corrupted_ciphertext() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    // Write two transactions: checkpoint the first, leave the second only in the log.
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'survives')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'corrupted')")
            .unwrap();
    }

    assert!(
        log_path.exists(),
        "db-log file should exist before corruption"
    );
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    // Corrupt the portable_changes of the second (non-checkpointed) frame in the log file.
    // The log header is 56 bytes, then the TX header is 24 bytes. Flip a byte
    // in the encrypted payload area right after that.
    {
        let mut log_bytes = std::fs::read(&log_path).expect("log file should exist");
        assert!(
            log_bytes.len() > 56 + 24 + 1,
            "log should have data beyond header + tx header"
        );
        // Flip a byte in the encrypted payload region
        let corrupt_offset = 56 + 24 + 1;
        log_bytes[corrupt_offset] ^= 0xFF;
        std::fs::write(&log_path, &log_bytes).unwrap();
    }

    // Restart: recovery should discard the corrupted frame but the checkpointed
    // row (id=1) must survive because it's already in the DB file.
    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "survives");
}

fn create_wide_table_like_schema(conn: &Arc<Connection>) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS core(
            id INTEGER PRIMARY KEY,
            row_number INTEGER NOT NULL,
            sheet_id INTEGER NOT NULL,
            created_by TEXT,
            updated_by TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            col_1 TEXT,
            col_2 TEXT,
            col_3 TEXT,
            col_4 TEXT,
            col_5 TEXT,
            col_6 TEXT,
            col_7 TEXT,
            col_8 TEXT
        )",
    )
    .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_sheet_row ON core(sheet_id, row_number)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_created ON core(created_at)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_updated ON core(updated_at, sheet_id)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_created_by ON core(created_by, sheet_id)")
        .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS metadata(
            sheet_id INTEGER PRIMARY KEY,
            next_row_number INTEGER NOT NULL DEFAULT 1,
            row_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS audit_log(
            id INTEGER PRIMARY KEY,
            sheet_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            row_id INTEGER,
            row_number INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            details TEXT
        )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trigger_gate(
            id INTEGER PRIMARY KEY,
            sheet_id INTEGER NOT NULL,
            trigger_type TEXT NOT NULL,
            portable_changes TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )",
    )
    .unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO metadata(sheet_id, next_row_number, row_count, updated_at)
         VALUES (1, 1, 0, datetime('now'))",
    )
    .unwrap();
}

fn drop_wide_table_like_schema(conn: &Arc<Connection>) {
    conn.execute("DROP TABLE IF EXISTS trigger_gate").unwrap();
    conn.execute("DROP TABLE IF EXISTS audit_log").unwrap();
    conn.execute("DROP TABLE IF EXISTS metadata").unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_sheet_row")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_created")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_updated")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_created_by")
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS core").unwrap();
}

fn insert_wide_table_like_batch(conn: &Arc<Connection>, start_row_number: i64, rows: usize) {
    conn.execute("BEGIN").unwrap();

    for offset in 0..rows {
        let row_number = start_row_number + offset as i64;
        conn.execute(format!(
            "INSERT INTO core(
                row_number, sheet_id, created_by, updated_by,
                created_at, updated_at,
                col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8
             ) VALUES (
                {row_number}, 1, 'seed', 'seed',
                datetime('now'), datetime('now'),
                hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)),
                hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8))
             )",
        ))
        .unwrap();

        conn.execute(format!(
            "INSERT INTO audit_log(sheet_id, action, row_number, details, created_at)
             VALUES (1, 'INSERT', {row_number}, 'wide table repro', datetime('now'))",
        ))
        .unwrap();
    }

    conn.execute(format!(
        "UPDATE metadata
         SET next_row_number = next_row_number + {rows},
             row_count = row_count + {rows},
             updated_at = datetime('now')
         WHERE sheet_id = 1",
    ))
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, portable_changes, created_at)
         VALUES (1, 'ROW_INSERT', '{\"count\": 1}', datetime('now'))",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, portable_changes, created_at)
         VALUES (1, 'RECALC', '{\"sheet_id\": 1}', datetime('now'))",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, portable_changes, created_at)
         VALUES (1, 'WEBHOOK', '{\"event\": \"rows_added\"}', datetime('now'))",
    )
    .unwrap();

    conn.execute("COMMIT").unwrap();
}

/// Reproducer for an MVCC crash-restart bug in checkpointing.
///
/// Sequence:
/// 1. Create a wide-table style schema and write one row.
/// 2. Simulate an abrupt process death (no clean connection close).
/// 3. Restart, drop the old schema, recreate it, write one new row.
/// 4. Checkpoint.
///
/// Checkpoint should retire the dropped table before creating the replacement table,
/// even when sqlite_schema rowids are reused across a crash + restart cycle.
#[test]
fn test_checkpoint_recovers_after_crash_restart_drop_recreate_table() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        create_wide_table_like_schema(&conn);
        insert_wide_table_like_batch(&conn, 1, 1);
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    drop_wide_table_like_schema(&conn);
    create_wide_table_like_schema(&conn);
    insert_wide_table_like_batch(&conn, 1, 1);
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(
        &conn,
        "SELECT row_number, sheet_id, created_by FROM core ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);
    assert_eq!(rows[0][2].to_string(), "seed");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    conn.close().unwrap();
    db.restart();

    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT row_number, sheet_id, created_by FROM core ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);
    assert_eq!(rows[0][2].to_string(), "seed");
}

/// Reproducer for the original index-side panic:
/// "Index struct for index_id ... must exist when checkpointing index rows".
///
/// Sequence:
/// 1. Create and checkpoint a table with one row.
/// 2. Create an index on that existing table.
/// 3. Simulate an abrupt process death before the index is checkpointed.
/// 4. Restart, drop and recreate the index, insert one more row.
/// 5. Checkpoint.
///
/// Checkpoint should retire the dropped index before processing recovered index rows,
/// even when sqlite_schema reuses the same rowid for the recreated index.
#[test]
fn test_checkpoint_recovers_after_crash_restart_drop_recreate_index() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, portable_changes TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'seed_1', hex(randomblob(16)))")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_t_v").unwrap();
    conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'post_2', hex(randomblob(16)))")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "seed_1");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "post_2");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    conn.close().unwrap();
    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "seed_1");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "post_2");
}

/// `begin_tx`'s schema-generation gate: a begin whose caller validated its prepared schema at a
/// generation that no longer matches the store's current `schema_generation` (a passive checkpoint
/// republished roots in the begin window) must fail with `SchemaUpdated` so the statement
/// reprepares rather than begin against stale physical roots. A matching generation (or no gate)
/// begins normally.
#[test]
fn test_begin_tx_schema_generation_gate() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let generation = db.mvcc_store.schema_generation();

    // Matching generation: begins normally.
    let tx = db
        .mvcc_store
        .begin_tx_with_schema_generation(pager.clone(), Some(generation))
        .unwrap();
    db.mvcc_store
        .rollback_tx(tx, pager.clone(), &db.conn, crate::MAIN_DB_ID);

    // Stale (mismatched) generation: forced reprepare.
    let err = db
        .mvcc_store
        .begin_tx_with_schema_generation(pager.clone(), Some(generation + 1))
        .unwrap_err();
    assert!(
        matches!(err, LimboError::SchemaUpdated),
        "stale schema generation should force reprepare, got {err:?}"
    );

    // No gate: begins normally.
    let tx = db
        .mvcc_store
        .begin_tx_with_schema_generation(pager.clone(), None)
        .unwrap();
    db.mvcc_store
        .rollback_tx(tx, pager, &db.conn, crate::MAIN_DB_ID);
}

/// Passive mode: freelist fields must come from the pager's live page 1, not a stale MVCC header.
#[test]
fn test_integrity_check_passive_reads_freelist_from_pager_not_stale_mvcc_header() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..500 {
        conn.execute(format!(
            "INSERT INTO t VALUES ({i}, 'wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww')"
        ))
        .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    let page_count = get_rows(&conn, "PRAGMA page_count")[0][0].as_int().unwrap();
    assert!(page_count > 2, "t should span multiple pages");

    {
        let mv_guard = conn.db.get_mv_store();
        let mv = mv_guard.as_ref().expect("mvcc store");
        let mut gh = mv.global_header.write();
        let h = gh.as_mut().expect("global_header initialized");
        h.freelist_trunk_page = pack1::U32BE::new(page_count as u32);
        h.freelist_pages = pack1::U32BE::new(1);
    }

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        &rows[0][0].to_string(),
        "ok",
        "passive integrity_check must read the freelist from the pager's live page 1, not the stale MVCC header"
    );
}

// https://github.com/tursodatabase/turso/issues/6752

/// Regression for abandoned COMMIT cleanup with attached MVCC databases.
/// Dropping the COMMIT while the main-db CommitStateMachine is paused must
/// also roll back attached MVCC txs opened by the same SQL transaction.
#[test]
fn dropped_main_commit_rolls_back_attached_mvcc_txs() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir.path().join("aux.db");

    let conn = db.connect();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE aux.u (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'main')").unwrap();
    conn.execute("INSERT INTO aux.u VALUES (1, 'aux')").unwrap();

    let aux_db_id = conn.get_database_id_by_name("aux").unwrap();
    let aux_mv_store = conn
        .mv_store_for_db(aux_db_id)
        .expect("attached aux database must be MVCC");
    let aux_pager = conn.get_pager_from_database_index(&aux_db_id).unwrap();
    let aux_tx_id = conn
        .get_mv_tx_id_for_db(aux_db_id)
        .expect("attached MVCC tx must be open after INSERT");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    assert!(
        conn.get_mv_tx_id_for_db(aux_db_id).is_none(),
        "attached MVCC tx slot must be cleared when abandoned COMMIT is rolled back"
    );
    assert!(
        aux_mv_store.tx_snapshot(aux_tx_id).is_none(),
        "attached MVCC tx must be removed from txs"
    );
    assert!(
        !aux_pager.holds_read_lock(),
        "attached pager read lock must be released"
    );

    let rows = get_rows(&conn, "SELECT id FROM aux.u ORDER BY id");
    assert!(
        rows.is_empty(),
        "abandoned attached INSERT must not become visible"
    );
}

/// Regression for abandoning COMMIT after it has advanced from the main
/// MVCC phase into an attached MVCC CommitStateMachine.
#[test]
fn dropped_attached_commit_releases_attached_read_lock() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir.path().join("aux.db");

    let conn = db.connect();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux");
    conn.execute("CREATE TABLE aux.u (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO aux.u VALUES (1, 'aux')").unwrap();

    let aux_db_id = conn.get_database_id_by_name("aux").unwrap();
    let aux_mv_store = conn
        .mv_store_for_db(aux_db_id)
        .expect("attached aux database must be MVCC");
    let aux_pager = conn.get_pager_from_database_index(&aux_db_id).unwrap();
    let aux_tx_id = conn
        .get_mv_tx_id_for_db(aux_db_id)
        .expect("attached MVCC tx must be open after INSERT");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at attached LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    assert!(
        conn.get_mv_tx_id_for_db(aux_db_id).is_none(),
        "attached MVCC tx slot must be cleared"
    );
    assert!(
        aux_mv_store.tx_snapshot(aux_tx_id).is_none(),
        "attached MVCC tx must be removed from txs"
    );
    assert!(
        !aux_pager.holds_read_lock(),
        "attached pager read lock must be released"
    );
}

/// Regression for abandoning COMMIT while one attached MVCC database is
/// paused mid-commit and another attached MVCC transaction is still pending.
#[test]
fn dropped_attached_commit_rolls_back_remaining_attached_mvcc_txs() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux1_path = aux_dir.path().join("aux1.db");
    let aux2_path = aux_dir.path().join("aux2.db");

    let conn = db.connect();
    drive_attach(&conn, aux1_path.to_str().unwrap(), "aux1");
    drive_attach(&conn, aux2_path.to_str().unwrap(), "aux2");
    conn.execute("CREATE TABLE aux1.u (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE aux2.v (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO aux1.u VALUES (1, 'aux1')")
        .unwrap();
    conn.execute("INSERT INTO aux2.v VALUES (1, 'aux2')")
        .unwrap();

    let aux1_db_id = conn.get_database_id_by_name("aux1").unwrap();
    let aux2_db_id = conn.get_database_id_by_name("aux2").unwrap();
    let aux1_mv_store = conn
        .mv_store_for_db(aux1_db_id)
        .expect("attached aux1 database must be MVCC");
    let aux2_mv_store = conn
        .mv_store_for_db(aux2_db_id)
        .expect("attached aux2 database must be MVCC");
    let aux1_pager = conn.get_pager_from_database_index(&aux1_db_id).unwrap();
    let aux2_pager = conn.get_pager_from_database_index(&aux2_db_id).unwrap();
    let aux1_tx_id = conn
        .get_mv_tx_id_for_db(aux1_db_id)
        .expect("attached aux1 MVCC tx must be open after INSERT");
    let aux2_tx_id = conn
        .get_mv_tx_id_for_db(aux2_db_id)
        .expect("attached aux2 MVCC tx must be open after INSERT");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at attached LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    assert!(
        conn.get_mv_tx_id_for_db(aux1_db_id).is_none(),
        "attached aux1 MVCC tx slot must be cleared"
    );
    assert!(
        conn.get_mv_tx_id_for_db(aux2_db_id).is_none(),
        "attached aux2 MVCC tx slot must be cleared"
    );
    assert!(
        aux1_mv_store.tx_snapshot(aux1_tx_id).is_none(),
        "attached aux1 MVCC tx must be removed from txs"
    );
    assert!(
        aux2_mv_store.tx_snapshot(aux2_tx_id).is_none(),
        "attached aux2 MVCC tx must be removed from txs"
    );
    assert!(
        !aux1_pager.holds_read_lock(),
        "attached aux1 pager read lock must be released"
    );
    assert!(
        !aux2_pager.holds_read_lock(),
        "attached aux2 pager read lock must be released"
    );
}

// https://github.com/tursodatabase/turso/issues/6757

// https://github.com/tursodatabase/turso/issues/6755

/// Like the test above, but the writer pauses in `FinalizeCommit`, after it is
/// `Committed` and before it raises `last_committed_tx_ts`. The check after the
/// CAS must still see it.
#[test]
fn test_create_index_exclusive_acquire_rechecks_committed_tx_after_cas() {
    let db = MvccTestDbNoConn::new_with_random_db();

    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    setup.close().unwrap();

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES (2, 200)").unwrap();
    writer.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::BeforeGlobalHeaderUpdate.point(),
    ])));

    let ddl = db.connect();
    ddl.execute("BEGIN DEFERRED").unwrap();
    let mut read = ddl.prepare("SELECT COUNT(*) FROM t").unwrap();
    read.run_ignore_rows().unwrap();
    drop(read);

    let mvcc_store = db.get_mvcc_store();
    let ddl_tx_id = ddl
        .get_mv_tx_id()
        .expect("DDL connection should have an active MVCC transaction");
    let injector = CommitWriterOnExclusiveAcquireInjector::new(
        ExclusiveTxYieldPoint::AfterTimestampCheckBeforeCas.point(),
        ddl_tx_id,
        writer.clone(),
    );
    ddl.set_yield_injector(Some(injector.clone()));

    let result = ddl.execute("CREATE INDEX idx_v ON t(v)");
    let mut writer_commit = injector
        .take_writer_commit()
        .expect("writer COMMIT should pause before publishing last_committed_tx_ts");
    assert!(
        matches!(result, Err(crate::LimboError::Busy)),
        "DDL transaction older than an already-committed writer should return Busy: {result:?}"
    );
    assert!(!mvcc_store.is_exclusive_tx(&ddl_tx_id));
    ddl.set_yield_injector(None);
    ddl.execute("ROLLBACK").unwrap();

    writer.set_yield_injector(None);
    writer_commit.run_ignore_rows().unwrap();
    drop(writer_commit);

    let observer = db.connect();
    observer.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    let rows = get_rows(&observer, "SELECT id FROM t INDEXED BY idx_v WHERE v = 200");
    assert_eq!(rows.len(), 1);
}

/// Unlocking before `on_checkpoint_end` races writers and the next checkpoint.
#[test]
fn on_checkpoint_end_runs_before_blocking_checkpoint_unlock() {
    use crate::io::FileSyncType;
    use crate::mvcc;
    use crate::mvcc::database::{LogRecord, RowVersion};
    use crate::mvcc::persistent_storage::logical_log::{LogHeader, OnSerializationComplete};
    use crate::mvcc::persistent_storage::DurableStorage;
    use crate::storage::encryption::EncryptionContext;
    use crate::storage::sqlite3_ondisk::DatabaseHeader;
    use crate::storage::wal::{CheckpointMode, TursoRwLock};
    use crate::{CheckpointResult, File, Result, IO};

    #[derive(Debug)]
    struct ObserveCheckpointEndStorage {
        inner: Arc<dyn DurableStorage>,
        /// Set after open; probed from `on_checkpoint_end`.
        lock: Mutex<Option<Arc<TursoRwLock>>>,
        lock_held_during_end: AtomicBool,
        end_called: AtomicBool,
    }

    impl ObserveCheckpointEndStorage {
        fn new(inner: Arc<dyn DurableStorage>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                lock: Mutex::new(None),
                lock_held_during_end: AtomicBool::new(false),
                end_called: AtomicBool::new(false),
            })
        }

        fn set_lock(&self, lock: Arc<TursoRwLock>) {
            *self.lock.lock() = Some(lock);
        }
    }

    impl DurableStorage for ObserveCheckpointEndStorage {
        fn serialize_row_version(
            &self,
            log_record: &mut LogRecord,
            row_id: &RowID,
            row_version: &RowVersion,
            portable_extension: Option<&[u8]>,
        ) -> Result<()> {
            self.inner
                .serialize_row_version(log_record, row_id, row_version, portable_extension)
        }
        fn serialize_database_header(
            &self,
            log_record: &mut LogRecord,
            header: &DatabaseHeader,
        ) -> Result<()> {
            self.inner.serialize_database_header(log_record, header)
        }
        fn log_tx(
            &self,
            m: LogRecord,
            c: OnSerializationComplete<'_>,
        ) -> Result<(Completion, u64)> {
            self.inner.log_tx(m, c)
        }
        fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>> {
            self.inner.upgrade_header_for_log_tx(m)
        }
        fn sync(&self, t: FileSyncType) -> Result<Completion> {
            self.inner.sync(t)
        }
        fn update_header(&self) -> Result<Completion> {
            self.inner.update_header()
        }
        fn truncate(
            &self,
            checkpointed_through_ts: u64,
        ) -> Result<(
            Completion,
            crate::mvcc::persistent_storage::LogicalLogTruncateOutcome,
        )> {
            self.inner.truncate(checkpointed_through_ts)
        }
        fn reset_to_fresh_header(&self) -> Result<Completion> {
            self.inner.reset_to_fresh_header()
        }
        fn get_logical_log_file(&self) -> Arc<dyn File> {
            self.inner.get_logical_log_file()
        }
        fn logical_log_offset(&self) -> u64 {
            self.inner.logical_log_offset()
        }
        fn should_checkpoint(&self) -> bool {
            self.inner.should_checkpoint()
        }
        fn set_checkpoint_threshold(&self, t: i64) {
            self.inner.set_checkpoint_threshold(t)
        }
        fn checkpoint_threshold(&self) -> i64 {
            self.inner.checkpoint_threshold()
        }
        fn advance_logical_log_offset_after_success(&self, b: u64) -> Result<()> {
            self.inner.advance_logical_log_offset_after_success(b)
        }
        fn discard_pending_log_write(&self) -> Result<()> {
            self.inner.discard_pending_log_write()
        }
        fn restore_logical_log_state_after_recovery(&self, o: u64, c: u32) {
            self.inner.restore_logical_log_state_after_recovery(o, c)
        }
        fn set_header(&self, h: LogHeader) {
            self.inner.set_header(h)
        }
        fn on_checkpoint_start(&self) -> Result<()> {
            self.inner.on_checkpoint_start()
        }
        fn on_checkpoint_end(&self, r: Result<&CheckpointResult>) -> Result<()> {
            self.end_called.store(true, Ordering::Release);
            let held = match self.lock.lock().as_ref() {
                Some(lock) => {
                    // write() fails if the checkpoint still holds the lock.
                    if lock.write() {
                        lock.unlock();
                        false
                    } else {
                        true
                    }
                }
                None => false,
            };
            self.lock_held_during_end.store(held, Ordering::Release);
            self.inner.on_checkpoint_end(r)
        }
        fn encryption_ctx(&self) -> Option<EncryptionContext> {
            self.inner.encryption_ctx()
        }
    }

    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join(format!("test_{}.db", rand::random::<u64>()));
    let path_str = path.to_str().unwrap().to_string();
    {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            &path_str,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(EngineDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        remove_file_from_database_registry(&path_str);
    }

    let log_path = path.with_extension("db-log");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let log_file = io
        .open_file(log_path.to_str().unwrap(), OpenFlags::default(), false)
        .unwrap();
    let inner_storage: Arc<dyn DurableStorage> = Arc::new(mvcc::persistent_storage::Storage::new(
        log_file,
        io.clone(),
        None,
    ));
    let observe = ObserveCheckpointEndStorage::new(inner_storage);
    let db = Database::open(
        io,
        &path_str,
        crate::OpenOptions::new(Arc::new(EngineDialect))
            .durable_storage(observe.clone() as Arc<dyn DurableStorage>),
    )
    .unwrap();

    let mv_store = db.get_mv_store().clone().unwrap();
    observe.set_lock(mv_store.blocking_checkpoint_lock.clone());

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })
    .unwrap();

    assert!(
        observe.end_called.load(Ordering::Acquire),
        "on_checkpoint_end must run for a successful truncate checkpoint"
    );
    assert!(
        observe.lock_held_during_end.load(Ordering::Acquire),
        "blocking_checkpoint_lock must still be held during on_checkpoint_end"
    );
    assert!(
        mv_store.blocking_checkpoint_lock.write(),
        "blocking_checkpoint_lock must be released after checkpoint returns"
    );
    mv_store.blocking_checkpoint_lock.unlock();
}

/// What this test checks: commit-time conflict validation reports a
/// write-write conflict, instead of panicking, when it meets an in-flight
/// B-tree tombstone whose writer has already been evicted from the live
/// transaction map into `finalized_tx_states`.
/// Why this matters: eviction (`remove_tx`) runs concurrently with other
/// transactions' commit validation. Validation reads the tombstone's TxID
/// marker first and looks the writer up second, so the writer can move maps
/// in between; this used to panic with "tombstone end TxID not found in txn
/// map" and take the process down. Found by the FTS concurrent-writers fuzz
/// soak.
#[test]
fn commit_validation_reports_conflict_for_evicted_tombstone_writer() {
    let db = MvccTestDb::new();

    // T1 creates the row so a version chain exists.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .insert(tx1, generate_simple_string_row((-2).into(), 1, "original"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 updates the row, putting it into T2's write set so T2's commit
    // walks this version chain.
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    assert!(db
        .mvcc_store
        .update(tx2, generate_simple_string_row((-2).into(), 1, "updated"))
        .unwrap());

    // Recreate the state T2's validation observes mid-race: another
    // transaction deleted the B-tree-resident row, its tombstone still
    // carries the in-flight TxID marker, and the writer itself has already
    // committed and been evicted from `txs` into `finalized_tx_states`.
    let evicted_writer: TxID = 9999;
    db.mvcc_store
        .insert_finalized_tx_state(evicted_writer, 1000)
        .unwrap();
    let row_id = RowID {
        table_id: (-2).into(),
        row_id: RowKey::Int(1),
    };
    let entry = db.mvcc_store.rows.get(&row_id).unwrap();
    entry.value().write().push(RowVersion {
        id: 0,
        begin: PackedTs::pack(None),
        end: PackedTs::pack(Some(TxTimestampOrID::TxID(evicted_writer))),
        data: generate_simple_string_row((-2).into(), 1, "original").data,
        btree_resident: true,
        materialized_at: WalPos::ORIGIN,
    });
    drop(entry);

    // The evicted writer committed, so T2 must lose with a write-write
    // conflict — not a panic.
    assert!(matches!(
        commit_tx(db.mvcc_store.clone(), &conn2, tx2),
        Err(LimboError::WriteWriteConflict)
    ));
}

#[path = "group_commit_tests.rs"]
mod group_commit_tests;

#[path = "group_commit_sync_mode_tests.rs"]
mod group_commit_sync_mode_tests;

fn drive_statement_to_done(statement: &mut crate::Statement, pager_io: &Arc<dyn crate::IO>) {
    use crate::StepResult;
    for _ in 0..200_000 {
        match statement.step().unwrap() {
            StepResult::Done => return,
            StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
            other => panic!("unexpected checkpoint step: {other:?}"),
        }
    }
    panic!("checkpoint did not complete");
}

#[test]
fn update_survives_restart_when_prune_cannot_mark_its_key_again() {
    use crate::StepResult;

    let _ = tracing_subscriber::fmt::try_init();
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("prune-mark-again-oom.db")
        .to_str()
        .unwrap()
        .to_string();
    let opts = DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true);
    let fault = Arc::new(OneShotAllocFault::default());
    {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open(
            io,
            &path,
            crate::OpenOptions::new(Arc::new(EngineDialect))
                .db_opts(opts)
                .allocators(crate::DatabaseAllocators {
                    mv_store: crate::alloc::DynAllocator::new(OneShotFaultAlloc(fault.clone())),
                    fts: crate::alloc::DynAllocator::default(),
                }),
        )
        .unwrap();
        let checkpointer = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        checkpointer
            .execute("PRAGMA journal_mode = 'mvcc'")
            .unwrap();
        checkpointer
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        checkpointer
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        checkpointer
            .execute("CREATE TABLE driver(id INTEGER PRIMARY KEY)")
            .unwrap();
        checkpointer
            .execute("PRAGMA wal_checkpoint(PASSIVE)")
            .unwrap();
        checkpointer
            .execute("INSERT INTO t VALUES (1, 'old')")
            .unwrap();
        let mvstore = db.get_mv_store().clone().unwrap();
        let root_page = get_rows(
            &checkpointer,
            "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 't'",
        )[0][0]
            .as_int()
            .unwrap();
        let key = RowID::new(
            mvstore.get_table_id_from_root_page(root_page),
            RowKey::Int(1),
        );
        let dirty_stamp = |key: &RowID| {
            mvstore
                .checkpoint_dirty_table_keys
                .get(key)
                .map(|entry| entry.value().load(Ordering::Acquire))
        };
        assert!(dirty_stamp(&key).is_some());

        let writer = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        writer.execute("BEGIN CONCURRENT").unwrap();
        writer
            .execute("UPDATE t SET v = 'new' WHERE id = 1")
            .unwrap();
        let update_tx = writer.get_mv_tx_id().unwrap();

        let checkpoint_hooks = Arc::new(ParkAfterCollectThenArmFault {
            parked: AtomicBool::new(false),
            fault: fault.clone(),
        });
        checkpointer.set_yield_injector(Some(checkpoint_hooks.clone()));
        let pager_io = checkpointer.pager.load().io.clone();
        mvstore.set_checkpoint_threshold(0);
        let mut first_checkpoint = checkpointer
            .prepare("INSERT INTO driver VALUES (1)")
            .unwrap();
        loop {
            match first_checkpoint.step().unwrap() {
                StepResult::IO | StepResult::Yield
                    if checkpoint_hooks.parked.load(Ordering::SeqCst) =>
                {
                    break
                }
                StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
                other => {
                    panic!("the auto-checkpoint ended before it collected table rows: {other:?}")
                }
            }
        }
        mvstore.set_checkpoint_threshold(-1);

        let log_written =
            FixedYieldInjector::new([CommitYieldPoint::LogRecordMarkedWritten.point()]);
        writer.set_yield_injector(Some(log_written.clone()));
        let mut commit = writer.prepare("COMMIT").unwrap();
        loop {
            match commit.step().unwrap() {
                StepResult::IO | StepResult::Yield if log_written.is_empty() => break,
                StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
                other => panic!("COMMIT ended before LogRecordMarkedWritten: {other:?}"),
            }
        }
        let preparing_end_ts = || match mvstore.tx_snapshot(update_tx).map(|tx| tx.state) {
            Some(TransactionState::Preparing(end_ts)) => end_ts,
            other => panic!("the UPDATE must still be Preparing, got {other:?}"),
        };
        let update_end_ts = preparing_end_ts();
        assert_eq!(dirty_stamp(&key), Some(update_end_ts));

        drive_statement_to_done(&mut first_checkpoint, &pager_io);
        drop(first_checkpoint);
        checkpointer.set_yield_injector(None);
        fault.armed.store(false, Ordering::SeqCst);
        assert_eq!(fault.fired.load(Ordering::SeqCst), 1);
        let failed_allocation = fault.failed_allocation.lock().take().unwrap();
        assert!(
            failed_allocation.contains("prune_dirty_keys")
                && failed_allocation.contains("mark_checkpoint_dirty_key"),
            "the failed allocation must be the second mark in prune_dirty_keys:\n{failed_allocation}"
        );

        assert!(mvstore.checkpoint_snapshot_ts() < update_end_ts);
        checkpointer
            .execute("PRAGMA wal_checkpoint(PASSIVE)")
            .unwrap();
        assert_eq!(preparing_end_ts(), update_end_ts);

        drive_statement_to_done(&mut commit, &pager_io);
        drop(commit);
        writer.set_yield_injector(None);

        checkpointer
            .execute("PRAGMA wal_checkpoint(PASSIVE)")
            .unwrap();
    }

    remove_file_from_database_registry(&path);
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        &path,
        OpenFlags::default(),
        opts,
        None,
        Arc::new(EngineDialect),
    )
    .unwrap();
    let rows = get_rows(
        &db.connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap(),
        "SELECT v FROM t WHERE id = 1",
    );
    assert_eq!(
        rows[0][0].to_string(),
        "new",
        "the committed UPDATE was lost after restart"
    );
}

#[derive(Debug)]
struct ParkAfterCollectThenArmFault {
    parked: AtomicBool,
    fault: Arc<OneShotAllocFault>,
}

impl YieldInjector for ParkAfterCollectThenArmFault {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CheckpointYieldPoint::AfterCollectTableRows.point() {
            return !self.parked.swap(true, Ordering::SeqCst);
        }
        if point == CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point() {
            self.fault.armed.store(true, Ordering::SeqCst);
        }
        false
    }
}

#[derive(Debug, Default)]
struct OneShotAllocFault {
    armed: AtomicBool,
    fired: AtomicU64,
    failed_allocation: Mutex<Option<String>>,
}

#[derive(Clone, Debug)]
struct OneShotFaultAlloc(Arc<OneShotAllocFault>);

unsafe impl crate::alloc::ApiAllocator for OneShotFaultAlloc {
    fn allocate(
        &self,
        layout: crate::alloc::Layout,
    ) -> std::result::Result<std::ptr::NonNull<[u8]>, crate::alloc::AllocError> {
        if self.0.armed.swap(false, Ordering::SeqCst) {
            self.0.fired.fetch_add(1, Ordering::SeqCst);
            *self.0.failed_allocation.lock() =
                Some(std::backtrace::Backtrace::force_capture().to_string());
            return Err(crate::alloc::AllocError);
        }
        <crate::alloc::TursoAllocator as crate::alloc::ApiAllocator>::allocate(
            &crate::alloc::TursoAllocator,
            layout,
        )
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: crate::alloc::Layout) {
        unsafe {
            <crate::alloc::TursoAllocator as crate::alloc::ApiAllocator>::deallocate(
                &crate::alloc::TursoAllocator,
                ptr,
                layout,
            )
        }
    }
}

fn empty_version_chain() -> RowVersionChain<TursoAllocator> {
    <RowVersionChain<TursoAllocator> as TursoVecInExt<RowVersion, TursoAllocator>>::new_in(
        TursoAllocator,
    )
}
