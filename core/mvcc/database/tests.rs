use crate::dialect::cannot_parse::DialectThatCannotParse;
use rustc_hash::FxHashSet as HashSet;

use super::*;
use crate::alloc::{TursoAllocator, TursoIteratorExt, TursoTryWithCapacityExt};
use crate::io::IO;
use crate::mvcc::clock::MvccClock;
use crate::mvcc::cursor::{CursorYieldPoint, MvccCursorType};
use crate::mvcc::database::CommitYieldPoint;
#[cfg(feature = "conn_raw_api")]
use crate::mvcc::portable_logical::{PortableLogicalBuilder, PortableObjectMapEntry};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::Mutex;
use crate::sync::RwLock;
use crate::{Completion, ConnectOpts, DatabaseOpts, LimboError, OpenFlags};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

pub(crate) struct MvccTestDbNoConn {
    pub(crate) db: Option<Arc<Database>>,
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
}

impl YieldInjector for FixedYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        self.remaining.lock().remove(&point)
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
                sort_order: crate::ast::SortOrder::Asc,
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

/// `SortableIndexKey` compares two integers, and two texts under BINARY,
/// without decoding them into values. The order must be the order of
/// `compare_immutable` on the decoded values, for integers of every serial
/// type, for texts of one-byte and two-byte serial types, for values of
/// different kinds, for both sort orders, for another collation and for
/// keys of different lengths.
#[test]
fn index_key_order_matches_compare_immutable() {
    use crate::ast::SortOrder;
    use crate::translate::collate::CollationSeq;

    let integers = [
        0,
        1,
        -1,
        2,
        127,
        -128,
        128,
        32_767,
        -32_768,
        32_768,
        8_388_607,
        -8_388_608,
        8_388_608,
        2_147_483_647,
        -2_147_483_648,
        2_147_483_648,
        140_737_488_355_327,
        -140_737_488_355_328,
        140_737_488_355_328,
        i64::MAX,
        i64::MIN,
    ];
    let mut values: Vec<Value> = integers.iter().map(|&i| Value::from_i64(i)).collect();
    values.extend([
        Value::Null,
        Value::from_f64(-0.5),
        Value::from_f64(1.0),
        Value::from_f64(1.5),
        Value::from_f64(9.3e18),
        Value::build_text("1"),
        Value::build_text("a"),
        Value::build_text("A"),
        Value::build_text(""),
        Value::build_text("ab"),
        Value::build_text("b"),
        Value::build_text("é"),
        // Longer than 57 bytes, so the serial type takes two bytes.
        Value::build_text("x".repeat(100)),
        Value::build_text(format!("{}y", "x".repeat(99))),
        Value::from_slice(b"1").unwrap(),
    ]);

    let index_info = |sort_orders: &[SortOrder], collation: CollationSeq, num_cols: usize| {
        Arc::new(
            IndexInfo::new(
                sort_orders.iter().map(|&sort_order| crate::types::KeyInfo {
                    sort_order,
                    collation,
                    nulls_order: None,
                }),
                true,
                num_cols,
                false,
            )
            .unwrap(),
        )
    };
    let key = |values: &[Value], info: &Arc<IndexInfo>| {
        let record = ImmutableRecord::from_values(values, values.len()).unwrap();
        SortableIndexKey::new_from_payload_in(&record, info.clone(), TursoAllocator).unwrap()
    };
    let expected = |lhs: &SortableIndexKey, rhs: &SortableIndexKey| {
        let num_cols = lhs.metadata.num_cols.min(rhs.metadata.num_cols);
        let lhs_values = lhs.key.get_values().unwrap();
        let rhs_values = rhs.key.get_values().unwrap();
        compare_immutable(
            &lhs_values[..num_cols],
            &rhs_values[..num_cols],
            &lhs.metadata.key_info[..num_cols],
        )
    };

    for (first_order, second_order, collation) in [
        (SortOrder::Asc, SortOrder::Asc, CollationSeq::BINARY),
        (SortOrder::Asc, SortOrder::Desc, CollationSeq::BINARY),
        (SortOrder::Desc, SortOrder::Asc, CollationSeq::BINARY),
        (SortOrder::Desc, SortOrder::Desc, CollationSeq::BINARY),
        (SortOrder::Asc, SortOrder::Asc, CollationSeq::NOCASE),
        (SortOrder::Desc, SortOrder::Asc, CollationSeq::NOCASE),
    ] {
        let sort_orders = [first_order, second_order];
        let full = index_info(&sort_orders, collation, 2);
        let prefix = index_info(&sort_orders, collation, 1);
        for a in &values {
            for b in &values {
                for (a_second, b_second) in [(1, 1), (1, 2), (3, 2)] {
                    let lhs = key(&[a.clone(), Value::from_i64(a_second)], &full);
                    let rhs = key(&[b.clone(), Value::from_i64(b_second)], &full);
                    assert_eq!(lhs.cmp(&rhs), expected(&lhs, &rhs), "{a:?} {b:?}");
                    assert_eq!(lhs == rhs, expected(&lhs, &rhs).is_eq());
                }
                let lhs = key(&[a.clone()], &prefix);
                let rhs = key(&[b.clone(), Value::build_text("x")], &full);
                assert_eq!(lhs.cmp(&rhs), expected(&lhs, &rhs), "{a:?} {b:?}");
                assert_eq!(rhs.cmp(&lhs), expected(&rhs, &lhs), "{a:?} {b:?}");
            }
        }
    }
}

/// A TEXT that is not valid UTF-8 and an integer with missing bytes do not
/// take the fast paths of `SortableIndexKey::compare`: the comparison gives
/// the error of the general path, from both sides.
#[test]
fn index_key_compare_reports_corrupt_values() {
    use crate::ast::SortOrder;
    use crate::translate::collate::CollationSeq;

    let info = Arc::new(
        IndexInfo::new(
            [crate::types::KeyInfo {
                sort_order: SortOrder::Asc,
                collation: CollationSeq::BINARY,
                nulls_order: None,
            }],
            false,
            1,
            false,
        )
        .unwrap(),
    );
    let key = |payload: &[u8]| {
        SortableIndexKey::new_from_payload_in(payload, info.clone(), TursoAllocator).unwrap()
    };
    // Header size 2, then the serial type of a 2-byte TEXT, then 0xff 0xfe.
    let invalid_text = key(&[2, 17, 0xff, 0xfe]);
    // Header size 2, then the serial type of an 8-byte integer, then 3 bytes.
    let truncated_integer = key(&[2, 6, 0, 0, 1]);
    let text_record = ImmutableRecord::from_values(&[Value::build_text("ab")], 1).unwrap();
    let text = key(text_record.get_payload());
    let integer_record = ImmutableRecord::from_values(&[Value::from_i64(i64::MAX)], 1).unwrap();
    let integer = key(integer_record.get_payload());

    for (lhs, rhs) in [
        (&invalid_text, &text),
        (&text, &invalid_text),
        (&truncated_integer, &integer),
        (&integer, &truncated_integer),
    ] {
        assert!(
            matches!(lhs.compare(rhs), Err(LimboError::Corrupt(_))),
            "{:?}",
            lhs.compare(rhs)
        );
    }
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
        let db = Database::open(
            io,
            ":memory:",
            crate::OpenOptions::new(Arc::new(DialectThatCannotParse))
                .db_opts(DatabaseOpts::new().with_mvcc(true)),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let mvcc_store = db.get_mv_store().clone().unwrap();
        Self {
            mvcc_store,
            db,
            conn,
        }
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

    let store = empty_mv_store();
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

impl MvccTestDbNoConn {
    pub fn new() -> Self {
        Self::new_with_opts(DatabaseOpts::new().with_mvcc(true))
    }

    pub fn new_passive() -> Self {
        Self::new_with_opts(
            DatabaseOpts::new()
                .with_mvcc(true)
                .with_experimental_mvcc_passive_checkpoint(true),
        )
    }

    fn new_with_opts(opts: DatabaseOpts) -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::default(),
            opts,
            None,
            Arc::new(DialectThatCannotParse),
        )
        .unwrap();
        Self { db: Some(db) }
    }

    /// Asumes there is a database open
    pub fn get_db(&self) -> Arc<Database> {
        self.db.as_ref().unwrap().clone()
    }

    pub fn connect(&self) -> Arc<Connection> {
        self.get_db()
            .connect(ConnectOpts::new(SessionRole::Superuser))
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

/// `checkpoint_snapshot_ts` must clamp the published checkpoint boundary below any
/// in-flight (Preparing) commit. `last_committed_tx_ts` is a fetch_max high-water mark,
/// so a commit that assigned a LOWER end_ts and is still Preparing (commits finalize out
/// of timestamp order) sits below it; a checkpoint that published a boundary above that
/// end_ts and skipped the commit (not yet Committed) would lose it on reopen. Regression
/// for the boundary-straddle data-loss path (review finding #1).
#[test]
fn test_checkpoint_snapshot_ts_clamps_below_inflight_preparing() {
    let store = empty_mv_store();
    let inflight: TxID = 1;
    store.txs.insert(
        inflight,
        new_tx_in::<crate::alloc::DynAllocator>(inflight, 1, TransactionState::Active),
    );

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

use crate::mvcc::cursor::MvccLazyCursor;
use crate::mvcc::database::{MvStore, Row, RowID};
use crate::schema::IndexColumn;
use crate::security::roles::SessionRole;
use crate::types::Text;
use crate::Database;
use crate::MemoryIO;
use crate::Value;
// Simple atomic clock implementation for testing

fn setup_lazy_db(initial_keys: &[i64]) -> (MvccTestDb, u64, MVTableId, i64) {
    let db = MvccTestDb::new();
    let table_id = new_uncheckpointed_table_id(&db);
    let btree_root_page = -i64::from(table_id);

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
    let table_id = new_uncheckpointed_table_id(&db);
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
            -i64::from(table_id),
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

/// Allocates a table id the way CREATE TABLE does in MVCC mode: the table has
/// no B-tree until a checkpoint gives it one, and its root page in
/// sqlite_schema is the table id.
fn new_uncheckpointed_table_id(db: &MvccTestDb) -> MVTableId {
    MVTableId::from(db.mvcc_store.get_next_table_id())
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
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[5, 10, 15, 20, 30]);

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
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();

    let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    mvcc_store
        .insert(tx_id, generate_simple_string_row((-2).into(), 1, "1"))
        .unwrap();
    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::BeforeFinishCommittedTx.point(),
    ])));
    let mut commit = mvcc_store
        .commit_tx(tx_id, &conn, crate::MAIN_DB_ID)
        .unwrap();
    loop {
        match commit.step(&mvcc_store).unwrap() {
            IOResult::IO(io) if io.is_explicit_yield() => break,
            IOResult::IO(io) => io.wait(conn.db.io.as_ref()).unwrap(),
            IOResult::Done(_) => panic!("commit finished without the injected yield"),
        }
    }

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

    let store = empty_mv_store();
    let table_id = MVTableId::from(-999_i64);

    // Single-column ascending integer index key.
    let info = std::sync::Arc::new(
        crate::types::IndexInfo::new(
            crate::alloc::vec![crate::types::KeyInfo {
                sort_order: crate::ast::SortOrder::Asc,
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
        .btree_row_is_valid(
            &store,
            table_id,
            reader_id,
            &idx_key(10),
            crate::vdbe::metrics::CursorWorkRef::new(
                &crate::vdbe::metrics::new_cursor_work_handle()
            ),
        )
        .unwrap());
    // B-tree key 30: scan (at key 20) is behind → steps over the tombstone.
    // It must advance past it WITHOUT evaluating the shadow predicate.
    assert!(scan
        .btree_row_is_valid(
            &store,
            table_id,
            reader_id,
            &idx_key(30),
            crate::vdbe::metrics::CursorWorkRef::new(
                &crate::vdbe::metrics::new_cursor_work_handle()
            ),
        )
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
/// When the writer aborts, the reader's commit must fail with
/// CommitDependencyAborted (cascade abort via AbortNow).
///
/// Sequence:
///   1. Writer: begin → update row 1
///   2. Writer state manually set to Preparing(end_ts)
///   3. Reader thread: begin → read row 1 (speculative read → dependency) → insert
///   4. Main thread: rollback writer → cascade abort via AbortNow
///   5. Reader commit → CommitDependencyAborted
#[test]
fn test_commit_dep_threaded_abort_cascades() {
    let db = MvccTestDbNoConn::new();
    let mvcc_store = db.get_mvcc_store();
    commit_dep_insert_initial_row(&db);

    let writer_conn = db.connect();
    let writer_tx_id = commit_dep_begin_preparing_writer(&mvcc_store, &writer_conn, "modified");

    // Reader signals after speculative read so main thread can abort writer.
    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let mvcc_store = db_arc.get_mv_store().clone().unwrap();
        let reader_tx_id = mvcc_store
            .begin_tx(reader_conn.pager.load().clone())
            .unwrap();

        // The read is speculative: reader.begin_ts >= writer.end_ts
        // → Hekaton Table 1: visible, register commit dependency
        let row = mvcc_store
            .read(reader_tx_id, &commit_dep_row_id(1))
            .unwrap();

        // Write so the commit exercises the full commit state machine path.
        mvcc_store
            .insert(
                reader_tx_id,
                generate_simple_string_row(COMMIT_DEP_TABLE_ID.into(), 2, "reader_data"),
            )
            .unwrap();

        // Signal: speculative read done, dependency registered
        signal_tx.send(()).unwrap();

        // The commit waits in WaitForDependencies until the writer resolves.
        // Writer will abort → AbortNow set → CommitDependencyAborted.
        let commit_result = commit_tx(mvcc_store, &reader_conn, reader_tx_id);
        let _ = reader_conn.close();
        (row, commit_result)
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

    let (row, commit_result) = reader_handle.join().unwrap();

    // Reader saw the writer's modified value via speculative read
    assert_eq!(
        row,
        Some(commit_dep_row(1, "modified")),
        "reader should have speculatively read the Preparing writer's value"
    );

    // Reader's commit must fail: depended-on writer aborted
    assert!(
        matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
        "expected CommitDependencyAborted, got: {commit_result:?}",
    );

    // Verify database consistency: only the initial value remains, and the
    // reader's insert is not visible (cascade-aborted).
    assert_eq!(
        commit_dep_read_committed(&db, &[1, 2]),
        vec![Some(commit_dep_row(1, "initial")), None],
        "reader's write should not be visible after cascade abort"
    );
}

/// Hekaton §2.7: multiple readers depending on the same Preparing writer
/// all cascade-abort when the writer aborts.
#[test]
fn test_commit_dep_threaded_multiple_dependents_abort() {
    let db = MvccTestDbNoConn::new();
    let mvcc_store = db.get_mvcc_store();
    commit_dep_insert_initial_row(&db);

    let writer_conn = db.connect();
    let writer_tx_id = commit_dep_begin_preparing_writer(&mvcc_store, &writer_conn, "modified");

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
            let mvcc_store = db_arc.get_mv_store().clone().unwrap();
            let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();

            // Speculative read from Preparing writer
            let row = mvcc_store.read(tx_id, &commit_dep_row_id(1)).unwrap();

            // Each reader writes to a unique row (no conflicts)
            mvcc_store
                .insert(
                    tx_id,
                    generate_simple_string_row(
                        COMMIT_DEP_TABLE_ID.into(),
                        i as i64 + 10,
                        &format!("reader_{i}"),
                    ),
                )
                .unwrap();

            // Signal: all readers done with speculative reads
            barrier_clone.wait();

            let commit_result = commit_tx(mvcc_store, &conn, tx_id);
            let _ = conn.close();
            (row, commit_result)
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
        let (row, commit_result) = handle.join().unwrap();
        assert_eq!(row, Some(commit_dep_row(1, "modified")));
        assert!(
            matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
            "expected CommitDependencyAborted, got: {commit_result:?}",
        );
    }

    // All reader writes should be invisible — only the initial row remains
    assert_eq!(
        commit_dep_read_committed(&db, &[1, 10, 11, 12, 13]),
        vec![Some(commit_dep_row(1, "initial")), None, None, None, None]
    );
}

/// Hekaton §2.7 happy path: when a Preparing writer commits, the dependent
/// reader's CommitDepCounter is decremented and the reader can proceed.
#[test]
fn test_commit_dep_threaded_commit_resolves() {
    let db = MvccTestDbNoConn::new();
    let mvcc_store = db.get_mvcc_store();
    commit_dep_insert_initial_row(&db);

    let writer_conn = db.connect();
    let writer_tx_id = commit_dep_begin_preparing_writer(&mvcc_store, &writer_conn, "committed");

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let mvcc_store = db_arc.get_mv_store().clone().unwrap();
        let reader_tx_id = mvcc_store
            .begin_tx(reader_conn.pager.load().clone())
            .unwrap();

        let row = mvcc_store
            .read(reader_tx_id, &commit_dep_row_id(1))
            .unwrap();

        mvcc_store
            .insert(
                reader_tx_id,
                generate_simple_string_row(COMMIT_DEP_TABLE_ID.into(), 2, "reader_data"),
            )
            .unwrap();

        signal_tx.send(()).unwrap();

        // The commit waits in WaitForDependencies. Writer will commit →
        // counter decremented → reader proceeds.
        let commit_result = commit_tx(mvcc_store, &reader_conn, reader_tx_id);
        let _ = reader_conn.close();
        (row, commit_result)
    });

    signal_rx.recv().unwrap();

    // Complete the writer's commit manually (postprocessing):
    // 1. Convert TxID → Timestamp in row versions (Hekaton §3.3)
    // 2. Set state to Committed
    // 3. Drain CommitDepSet, decrement dependents' counters
    commit_dep_finish_writer_commit(&mvcc_store, writer_tx_id);

    let (row, commit_result) = reader_handle.join().unwrap();

    // Reader speculatively read the writer's value
    assert_eq!(row, Some(commit_dep_row(1, "committed")));

    // Reader's commit succeeds: dependency resolved by writer's commit
    assert!(
        commit_result.is_ok(),
        "expected reader commit to succeed, got: {commit_result:?}",
    );

    // Both writes are visible
    assert_eq!(
        commit_dep_read_committed(&db, &[1, 2]),
        vec![
            Some(commit_dep_row(1, "committed")),
            Some(commit_dep_row(2, "reader_data"))
        ]
    );
}

/// Regression: the write_set.is_empty() fast path used to commit read-only
/// transactions without checking commit dependencies. A read-only tx that
/// speculatively read from a Preparing writer must still honour AbortNow.
#[test]
fn test_commit_dep_threaded_readonly_abort_cascades() {
    let db = MvccTestDbNoConn::new();
    let mvcc_store = db.get_mvcc_store();
    commit_dep_insert_initial_row(&db);

    let writer_conn = db.connect();
    let writer_tx_id = commit_dep_begin_preparing_writer(&mvcc_store, &writer_conn, "modified");

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let mvcc_store = db_arc.get_mv_store().clone().unwrap();
        let reader_tx_id = mvcc_store
            .begin_tx(reader_conn.pager.load().clone())
            .unwrap();

        // Read-only: no writes, only a read → triggers speculative read
        let row = mvcc_store
            .read(reader_tx_id, &commit_dep_row_id(1))
            .unwrap();

        signal_tx.send(()).unwrap();

        // Committing a read-only tx hits the write_set.is_empty() fast path.
        // It must still check commit dependencies.
        let commit_result = commit_tx(mvcc_store, &reader_conn, reader_tx_id);
        let _ = reader_conn.close();
        (row, commit_result)
    });

    signal_rx.recv().unwrap();

    // Abort writer → cascade to read-only reader
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    let (row, commit_result) = reader_handle.join().unwrap();

    assert_eq!(row, Some(commit_dep_row(1, "modified")));

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

/// `MvccCursor::insert` reads the row and then calls `update_to_table_or_index`
/// without holding the row's version chain lock in between. If the version it
/// read belongs to a committing writer that aborts in between, the update finds
/// no visible version. It must fail with CommitDependencyAborted instead of
/// returning false, which made the cursor panic.
#[test]
fn test_update_fails_when_speculatively_read_writer_aborts() {
    // The writer either inserts row 1, or updates row 1 while it exists only in
    // the B-tree. After its rollback, neither leaves a version the updater can see.
    for btree_resident in [false, true] {
        let db = MvccTestDb::new();
        let table_id: MVTableId = (-2).into();
        let id = RowID::new(table_id, RowKey::Int(1));

        let writer_conn = db
            .db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let writer = db
            .mvcc_store
            .begin_tx(writer_conn.pager.load().clone())
            .unwrap();
        let written = generate_simple_string_row(table_id, 1, "writer");
        if btree_resident {
            db.mvcc_store
                .insert_btree_resident_to_table_or_index(writer, written, None)
                .unwrap();
        } else {
            db.mvcc_store.insert(writer, written).unwrap();
        }
        db.mvcc_store.get_commit_timestamp(|ts| {
            db.mvcc_store
                .txs
                .get(&writer)
                .unwrap()
                .value()
                .state
                .store(TransactionState::Preparing(ts));
        });

        let updater_conn = db
            .db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let updater = db
            .mvcc_store
            .begin_tx(updater_conn.pager.load().clone())
            .unwrap();
        let read = db.mvcc_store.read(updater, &id).unwrap();
        assert_eq!(
            read,
            Some(generate_simple_string_row(table_id, 1, "writer")),
            "the updater should read the Preparing writer's version"
        );

        db.mvcc_store.rollback_tx(
            writer,
            writer_conn.pager.load().clone(),
            &writer_conn,
            crate::MAIN_DB_ID,
        );

        let result = db
            .mvcc_store
            .update(updater, generate_simple_string_row(table_id, 1, "updater"));
        assert!(
            matches!(result, Err(LimboError::CommitDependencyAborted)),
            "btree_resident={btree_resident}: expected CommitDependencyAborted, got {result:?}"
        );
        let commit_result = commit_tx(db.mvcc_store.clone(), &updater_conn, updater);
        assert!(
            matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
            "btree_resident={btree_resident}: expected CommitDependencyAborted, got {commit_result:?}"
        );
    }
}

/// Test that read-only transactions with resolved dependencies do NOT advance
/// last_committed_tx_ts. A read-only tx going through WaitForDependencies →
/// CommitEnd would update last_committed_tx_ts, causing spurious Busy errors
/// from acquire_exclusive_tx.
#[test]
fn test_commit_dep_readonly_does_not_advance_timestamp() {
    let db = MvccTestDbNoConn::new();
    let mvcc_store = db.get_mvcc_store();
    commit_dep_insert_initial_row(&db);

    let ts_before = mvcc_store.last_committed_tx_ts.load(Ordering::Acquire);

    let writer_conn = db.connect();
    let writer_tx_id = commit_dep_begin_preparing_writer(&mvcc_store, &writer_conn, "modified");

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let mvcc_store = db_arc.get_mv_store().clone().unwrap();
        let reader_tx_id = mvcc_store
            .begin_tx(reader_conn.pager.load().clone())
            .unwrap();

        // Read-only: a read only → speculative read registers dependency
        let _row = mvcc_store
            .read(reader_tx_id, &commit_dep_row_id(1))
            .unwrap();

        signal_tx.send(()).unwrap();

        // Commit: read-only with dependency → WaitForDependencies
        let commit_result = commit_tx(mvcc_store, &reader_conn, reader_tx_id);
        let _ = reader_conn.close();
        commit_result
    });

    signal_rx.recv().unwrap();

    commit_dep_finish_writer_commit(&mvcc_store, writer_tx_id);

    let commit_result = reader_handle.join().unwrap();
    assert!(
        commit_result.is_ok(),
        "read-only tx with resolved dependency should commit: {commit_result:?}",
    );

    let ts_after = mvcc_store.last_committed_tx_ts.load(Ordering::Acquire);
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
    let db = MvccTestDbNoConn::new();
    let mvcc_store = db.get_mvcc_store();
    commit_dep_insert_initial_row(&db);

    let writer_conn = db.connect();
    let writer_tx_id = commit_dep_begin_preparing_writer(&mvcc_store, &writer_conn, "modified");

    // Start a tx that will try to get the exclusive lock later. Its begin_ts
    // must be assigned now, before the read-only tx commits.
    let exclusive_conn = db.connect();
    let exclusive_tx_id = mvcc_store
        .begin_tx(exclusive_conn.pager.load().clone())
        .unwrap();

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let mvcc_store = db_arc.get_mv_store().clone().unwrap();
        let reader_tx_id = mvcc_store
            .begin_tx(reader_conn.pager.load().clone())
            .unwrap();

        let _row = mvcc_store
            .read(reader_tx_id, &commit_dep_row_id(1))
            .unwrap();

        signal_tx.send(()).unwrap();

        let commit_result = commit_tx(mvcc_store, &reader_conn, reader_tx_id);
        let _ = reader_conn.close();
        commit_result
    });

    signal_rx.recv().unwrap();

    // Resolve the writer's commit (unblocks reader's WaitForDependencies)
    commit_dep_finish_writer_commit(&mvcc_store, writer_tx_id);

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

/// The table the commit dependency tests write to. It has no schema entry; the
/// tests read and write its rows through `MvStore`.
const COMMIT_DEP_TABLE_ID: i64 = -2;

fn commit_dep_row_id(id: i64) -> RowID {
    RowID::new(COMMIT_DEP_TABLE_ID.into(), RowKey::Int(id))
}

fn commit_dep_row(id: i64, value: &str) -> Row {
    generate_simple_string_row(COMMIT_DEP_TABLE_ID.into(), id, value)
}

/// Commits row 1 with the value "initial".
fn commit_dep_insert_initial_row(db: &MvccTestDbNoConn) {
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    mvcc_store
        .insert(tx_id, commit_dep_row(1, "initial"))
        .unwrap();
    commit_tx(mvcc_store, &conn, tx_id).unwrap();
    conn.close().unwrap();
}

/// Updates row 1 to `value` in a new transaction and moves that transaction to
/// Preparing, as if it were in the middle of its commit. Its end_ts comes from
/// the global clock, so a transaction that begins later has begin_ts >= end_ts,
/// which is the speculative read condition (Hekaton Table 1).
fn commit_dep_begin_preparing_writer(
    mvcc_store: &Arc<crate::MvStore>,
    conn: &Arc<Connection>,
    value: &str,
) -> TxID {
    let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    assert!(mvcc_store.update(tx_id, commit_dep_row(1, value)).unwrap());
    mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });
    tx_id
}

/// Completes the commit of a Preparing writer by hand: its row versions get
/// its end_ts, it becomes Committed, and its dependents' counters go down.
fn commit_dep_finish_writer_commit(mvcc_store: &Arc<crate::MvStore>, writer_tx_id: TxID) {
    let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
    let writer_tx = writer_tx.value();
    let TransactionState::Preparing(end_ts) = writer_tx.state.load() else {
        panic!("writer should be preparing");
    };
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

/// Reads the given rows in a new transaction on a new connection.
fn commit_dep_read_committed(db: &MvccTestDbNoConn, ids: &[i64]) -> Vec<Option<Row>> {
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    let rows = ids
        .iter()
        .map(|id| mvcc_store.read(tx_id, &commit_dep_row_id(*id)).unwrap())
        .collect();
    mvcc_store.rollback_tx(tx_id, conn.pager.load().clone(), &conn, crate::MAIN_DB_ID);
    conn.close().unwrap();
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

// ─── GC helpers ───────────────────────────────────────────────────────────

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
    let store = empty_mv_store();
    let table_id: MVTableId = (-2).into();
    let row_id = RowID::new(table_id, RowKey::Int(1));
    let tx: TxID = 1;
    store.txs.insert(
        tx,
        new_tx_in::<crate::alloc::DynAllocator>(tx, 1, TransactionState::Active),
    );
    store
        .insert(tx, generate_simple_string_row(table_id, 1, "version_0"))
        .unwrap();
    let entry = store.rows.get(&row_id).unwrap();
    let versions = entry.value().read();
    let chain_start = std::ptr::addr_of!(*versions) as usize;
    let chain = chain_start..chain_start + std::mem::size_of_val(&*versions);
    assert!(chain.contains(&(versions.as_ptr() as usize)));
}

/// An MVCC store that belongs to no database.
fn empty_mv_store() -> crate::MvStore {
    MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("empty-mv-store.db-log"),
        crate::alloc::DynAllocator::default(),
        false,
    )
    .unwrap()
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

// https://github.com/tursodatabase/turso/issues/6752

// https://github.com/tursodatabase/turso/issues/6757

// https://github.com/tursodatabase/turso/issues/6755

/// Repro for https://github.com/tursodatabase/turso/issues/8004.
///
/// When a passive checkpoint collects a newly-created negative-root table before
/// `DROP TABLE` starts, and `DROP TABLE` pauses at `LogRecordPrepared` until
/// after the checkpoint publishes the positive root page, committing the DROP
/// must preserve the dropped root in `dropped_root_pages` (and keep other
/// published positive roots on surviving tables) until a later checkpoint frees it.
#[test]
fn test_passive_checkpoint_preserves_drop_committed_after_root_publication() {
    use crate::StepResult;

    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let setup = db.connect();
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("CREATE TABLE driver(id INTEGER PRIMARY KEY)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1, 'live')").unwrap();
    let root_page = get_rows(
        &setup,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    )[0][0]
        .as_int()
        .unwrap();
    assert!(root_page < 0);

    let checkpointer = db.connect();
    checkpointer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let checkpoint_injector =
        FixedYieldInjector::new([CheckpointYieldPoint::AfterCollectTableRows.point()]);
    checkpointer.set_yield_injector(Some(checkpoint_injector.clone()));
    let mut checkpoint = checkpointer
        .prepare("INSERT INTO driver VALUES (1)")
        .unwrap();
    let io = checkpointer.pager.load().io.clone();
    let mut checkpoint_yielded = false;
    for _ in 0..200_000 {
        match checkpoint.step().unwrap() {
            StepResult::Yield if checkpoint_injector.is_empty() => {
                checkpoint_yielded = true;
                break;
            }
            StepResult::IO => io.step().unwrap(),
            StepResult::Yield => {}
            StepResult::Done => {
                panic!("checkpoint completed before AfterCollectTableRows yielded")
            }
            other => panic!("unexpected checkpoint result before yield: {other:?}"),
        }
    }
    assert!(checkpoint_yielded);

    let dropper = db.connect();
    let drop_injector = FixedYieldInjector::new([CommitYieldPoint::LogRecordPrepared.point()]);
    dropper.set_yield_injector(Some(drop_injector.clone()));
    let mut drop_table = dropper.prepare("DROP TABLE t").unwrap();
    assert!(matches!(drop_table.step().unwrap(), StepResult::Yield));
    assert!(drop_injector.is_empty());

    checkpointer.set_yield_injector(None);
    checkpoint.run_ignore_rows().unwrap();
    drop(checkpoint);

    dropper.set_yield_injector(None);
    drop_table.run_ignore_rows().unwrap();
    drop(drop_table);

    assert_integrity_ok(&dropper);

    let observer = db.connect();
    let table_rows = get_rows(
        &observer,
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    );
    assert!(table_rows.is_empty());
    assert_integrity_ok(&observer);

    observer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert!(observer.db.schema.lock().dropped_root_pages.is_empty());
    assert_integrity_ok(&observer);
}

#[test]
fn test_passive_checkpoint_preserves_drop_index_committed_after_root_publication() {
    use crate::StepResult;

    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let setup = db.connect();
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup.execute("CREATE INDEX idx ON t(v)").unwrap();
    setup
        .execute("CREATE TABLE driver(id INTEGER PRIMARY KEY)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1, 'live')").unwrap();
    let index_root_page = get_rows(
        &setup,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'index' AND name = 'idx'",
    )[0][0]
        .as_int()
        .unwrap();
    assert!(index_root_page < 0);

    let checkpointer = db.connect();
    checkpointer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let checkpoint_injector =
        FixedYieldInjector::new([CheckpointYieldPoint::AfterCollectTableRows.point()]);
    checkpointer.set_yield_injector(Some(checkpoint_injector.clone()));
    let mut checkpoint = checkpointer
        .prepare("INSERT INTO driver VALUES (1)")
        .unwrap();
    let io = checkpointer.pager.load().io.clone();
    let mut checkpoint_yielded = false;
    for _ in 0..200_000 {
        match checkpoint.step().unwrap() {
            StepResult::Yield if checkpoint_injector.is_empty() => {
                checkpoint_yielded = true;
                break;
            }
            StepResult::IO => io.step().unwrap(),
            StepResult::Yield => {}
            StepResult::Done => {
                panic!("checkpoint completed before AfterCollectTableRows yielded")
            }
            other => panic!("unexpected checkpoint result before yield: {other:?}"),
        }
    }
    assert!(checkpoint_yielded);

    let dropper = db.connect();
    let drop_injector = FixedYieldInjector::new([CommitYieldPoint::LogRecordPrepared.point()]);
    dropper.set_yield_injector(Some(drop_injector.clone()));
    let mut drop_index = dropper.prepare("DROP INDEX idx").unwrap();
    assert!(matches!(drop_index.step().unwrap(), StepResult::Yield));
    assert!(drop_injector.is_empty());

    checkpointer.set_yield_injector(None);
    checkpoint.run_ignore_rows().unwrap();
    drop(checkpoint);

    dropper.set_yield_injector(None);
    drop_index.run_ignore_rows().unwrap();
    drop(drop_index);

    assert_integrity_ok(&dropper);

    let observer = db.connect();
    let index_rows = get_rows(
        &observer,
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = 'idx'",
    );
    assert!(index_rows.is_empty());
    assert_integrity_ok(&observer);

    observer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert!(observer.db.schema.lock().dropped_root_pages.is_empty());
    assert_integrity_ok(&observer);
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
        fn finish_truncate(&self) {
            self.inner.finish_truncate()
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

    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let log_file = io
        .open_file("test.db-log", OpenFlags::default(), false)
        .unwrap();
    let inner_storage: Arc<dyn DurableStorage> = Arc::new(mvcc::persistent_storage::Storage::new(
        log_file,
        io.clone(),
        None,
    ));
    let observe = ObserveCheckpointEndStorage::new(inner_storage);
    let db = Database::open(
        io,
        ":memory:",
        crate::OpenOptions::new(Arc::new(DialectThatCannotParse))
            .db_opts(DatabaseOpts::new().with_mvcc(true))
            .durable_storage(observe.clone() as Arc<dyn DurableStorage>),
    )
    .unwrap();

    let mv_store = db.get_mv_store().clone().unwrap();
    observe.set_lock(mv_store.blocking_checkpoint_lock.clone());

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx_id = mv_store
        .begin_exclusive_tx(conn.pager.load().clone(), None, &conn, None)
        .unwrap();
    mv_store
        .with_header_mut(|header| header.user_version = 1.into(), Some(&tx_id))
        .unwrap();
    commit_tx(mv_store.clone(), &conn, tx_id).unwrap();
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

fn empty_version_chain() -> RowVersionChain<TursoAllocator> {
    <RowVersionChain<TursoAllocator> as TursoVecInExt<RowVersion, TursoAllocator>>::new_in(
        TursoAllocator,
    )
}
