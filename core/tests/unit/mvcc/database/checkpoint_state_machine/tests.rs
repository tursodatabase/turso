use super::*;
use crate::alloc::vec;
use crate::mvcc::database::tests::MvccTestDbNoConn;
use crate::mvcc::database::SortableIndexKey;
use crate::translate::collate::CollationSeq;
use crate::types::{IndexInfo, KeyInfo};
use turso_parser::ast::SortOrder;

fn sqlite_schema_row_version(
    rowid: i64,
    entry_type: &'static str,
    name: &'static str,
    table_name: &'static str,
    root_page: i64,
    begin: Option<u64>,
    end: Option<u64>,
) -> RowVersion {
    let record = ImmutableRecord::from_values(
        &[
            Value::build_text(entry_type),
            Value::build_text(name),
            Value::build_text(table_name),
            Value::from_i64(root_page),
            Value::build_text(format!("sql:{entry_type}:{name}:{root_page}")),
        ],
        5,
    )
    .unwrap();
    RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
        end: crate::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
        row: Row::new_table_row(
            RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(rowid)),
            record.as_blob(),
            5,
        )
        .unwrap(),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

#[test]
fn local_schema_record_shares_mvcc_payload() {
    let version = sqlite_schema_row_version(2, "table", "t", "t", -2, Some(1), None);
    let payload = version.row.data.as_ref().unwrap();
    let payload_ptr = payload.as_ptr();

    let record = ImmutableRecordRef::from_shared_record(payload.clone());

    assert_eq!(record.get_payload().as_ptr(), payload_ptr);
    assert_eq!(record.column_count(), SQLITE_SCHEMA_COLUMN_COUNT);
}

#[test]
fn sqlite_schema_identity_treats_index_sql_rewrite_as_same_object() {
    let old = sqlite_schema_row_version(3, "index", "idx_t_a", "t", 7, Some(1), Some(2));
    let new = sqlite_schema_row_version(3, "index", "idx_t_a", "t", 7, Some(2), None);

    assert_eq!(
        sqlite_schema_btree_identity(&old),
        Some(SqliteSchemaBtreeIdentity {
            kind: SqliteSchemaBtreeKind::Index,
            root_page: 7,
        })
    );
    assert!(sqlite_schema_versions_refer_to_btree(&old, &new));
    assert!(!is_schema_metadata_only_rewrite(&old, Some(&new)));
}

#[test]
fn sqlite_schema_identity_treats_table_sql_rewrite_as_same_object() {
    let old = sqlite_schema_row_version(2, "table", "t", "t", 5, Some(1), Some(2));
    let new = sqlite_schema_row_version(2, "table", "t", "t", 5, Some(2), None);

    assert!(sqlite_schema_versions_refer_to_btree(&old, &new));
    assert!(!is_schema_metadata_only_rewrite(&old, Some(&new)));
}

#[test]
fn sqlite_schema_identity_detects_drop_recreate_as_different_objects() {
    let dropped = sqlite_schema_row_version(3, "index", "idx_t_v", "t", -4, Some(1), Some(2));
    let recreated = sqlite_schema_row_version(3, "index", "idx_t_v", "t", -5, Some(2), None);

    assert!(!sqlite_schema_versions_refer_to_btree(&dropped, &recreated));
    assert!(is_schema_metadata_only_rewrite(&dropped, Some(&recreated)));
}

#[test]
fn sqlite_schema_identity_detects_drop_without_successor() {
    let dropped = sqlite_schema_row_version(3, "index", "idx_t_v", "t", 11, Some(1), Some(2));

    assert!(is_schema_metadata_only_rewrite(&dropped, None));
}

#[test]
fn sqlite_schema_identity_ignores_non_btree_schema_entries() {
    let trigger = sqlite_schema_row_version(9, "trigger", "trg_t", "t", 0, Some(1), Some(2));
    let rewritten_trigger = sqlite_schema_row_version(9, "trigger", "trg_t", "t", 0, Some(2), None);

    assert_eq!(sqlite_schema_btree_identity(&trigger), None);
    assert!(!sqlite_schema_versions_refer_to_btree(
        &trigger,
        &rewritten_trigger
    ));
    assert!(!is_schema_metadata_only_rewrite(
        &trigger,
        Some(&rewritten_trigger)
    ));
}

#[test]
fn sqlite_schema_identity_ignores_payloadless_tombstones() {
    let tombstone = RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(None),
        end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(2))),
        row: Row::new_table_row(
            RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(9)),
            &[],
            0,
        )
        .unwrap(),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    assert_eq!(sqlite_schema_btree_identity(&tombstone), None);
    assert!(!is_schema_metadata_only_rewrite(&tombstone, None));
}

fn index_row_version(
    index_id: MVTableId,
    key_text: &str,
    rowid: i64,
    version_id: u64,
    begin: Option<u64>,
    end: Option<u64>,
    btree_resident: bool,
) -> (Arc<SortableIndexKey>, RowVersion) {
    let index_info = Arc::new(
        IndexInfo::new(
            vec![
                KeyInfo {
                    sort_order: SortOrder::Asc,
                    collation: CollationSeq::Binary,
                    nulls_order: None,
                },
                KeyInfo {
                    sort_order: SortOrder::Asc,
                    collation: CollationSeq::Binary,
                    nulls_order: None,
                },
            ],
            true,
            2,
            true,
        )
        .unwrap(),
    );
    let key_record = ImmutableRecord::from_values(
        &[
            Value::Text(crate::types::Text::new(key_text.to_string())),
            Value::from_i64(rowid),
        ],
        2,
    )
    .unwrap();
    let sortable_key =
        SortableIndexKey::new_from_payload_in(&key_record, index_info, TursoAllocator).unwrap();
    let key_arc = Arc::new(sortable_key.clone());
    let row = Row::new_index_row(
        RowID::new(index_id, RowKey::Record(Arc::new(sortable_key))),
        2,
    );
    let row_version = RowVersion {
        id: version_id,
        begin: crate::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
        end: crate::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
        row,
        btree_resident,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    (key_arc, row_version)
}

#[test]
fn checkpoint_retry_does_not_replay_checkpointed_btree_resident_delete() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();
    let mvstore = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint = CheckpointStateMachine::new(
        pager,
        mvstore.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    checkpoint.durable_txid_max_old = std::num::NonZeroU64::new(10);
    checkpoint.durable_txid_max_new = 10;

    let index_id = MVTableId::from(-42);
    let (garbage_key, garbage_version) =
        index_row_version(index_id, "blue_river_906", 75, 1, None, None, true);
    let (_, tombstone_version) =
        index_row_version(index_id, "blue_river_906", 75, 2, None, Some(10), true);

    mvstore
        .insert_index_version(index_id, garbage_key, garbage_version)
        .unwrap();
    let entry = mvstore
        .index_rows
        .get(&index_id)
        .expect("index entry should exist after first insert");
    let tombstone_key = entry
        .value()
        .front()
        .expect("key bucket should exist after first insert")
        .key()
        .clone();
    mvstore
        .insert_index_version(index_id, tombstone_key, tombstone_version)
        .unwrap();

    while checkpoint.collect_index_rows().unwrap().is_some() {}

    assert!(
        checkpoint.index_write_set.is_empty(),
        "a retry checkpoint must not replay a delete whose btree_resident tombstone was already made durable"
    );
}

fn committed_table_row_version(table_id: MVTableId, rowid: i64) -> RowVersion {
    table_row_version(table_id, rowid, 1, Some(5), None, false)
}

fn table_row_version(
    table_id: MVTableId,
    rowid: i64,
    version_id: u64,
    begin: Option<u64>,
    end: Option<u64>,
    btree_resident: bool,
) -> RowVersion {
    let record = ImmutableRecord::from_values(&[Value::from_i64(rowid)], 1).unwrap();
    RowVersion {
        id: version_id,
        begin: crate::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
        end: crate::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
        row: Row::new_table_row(
            RowID::new(table_id, RowKey::Int(rowid)),
            record.as_blob(),
            1,
        )
        .unwrap(),
        btree_resident,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

fn checkpoint_for_collect_tests(
) -> CheckpointStateMachine<crate::mvcc::clock::MvccClock, crate::alloc::DynAllocator> {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();
    let mvstore = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint = CheckpointStateMachine::new(
        pager,
        mvstore,
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    checkpoint.durable_txid_max_old = NonZeroU64::new(2);
    checkpoint
}

#[test]
fn checkpoint_collection_uses_btree_marker_for_existence_but_writes_surviving_replacement() {
    let checkpoint = checkpoint_for_collect_tests();
    let table_id = MVTableId::from(-2);
    let btree_tombstone = table_row_version(table_id, 1, 1, None, Some(5), true);
    let replacement = table_row_version(table_id, 1, 2, Some(5), None, false);

    let checkpointable =
        checkpoint.maybe_get_checkpointable_versions(&[btree_tombstone, replacement], table_id);

    assert_eq!(checkpointable.len(), 1);
    assert_eq!(checkpointable[0].id, 2);
    assert_eq!(checkpointable[0].end(), None);
}

#[test]
fn checkpoint_collection_uses_btree_marker_for_later_delete_of_replacement() {
    let checkpoint = checkpoint_for_collect_tests();
    let table_id = MVTableId::from(-2);
    let btree_tombstone = table_row_version(table_id, 1, 1, None, Some(5), true);
    let deleted_replacement = table_row_version(table_id, 1, 2, Some(5), Some(6), false);

    let checkpointable = checkpoint
        .maybe_get_checkpointable_versions(&[btree_tombstone, deleted_replacement], table_id);

    assert_eq!(checkpointable.len(), 1);
    assert_eq!(checkpointable[0].id, 2);
    assert_eq!(checkpointable[0].end(), Some(TxTimestampOrID::Timestamp(6)));
}

#[test]
fn checkpoint_collection_skips_delete_of_never_checkpointed_replacement_without_btree_marker() {
    let checkpoint = checkpoint_for_collect_tests();
    let table_id = MVTableId::from(-2);
    let deleted_replacement = table_row_version(table_id, 1, 2, Some(5), Some(6), false);

    let checkpointable =
        checkpoint.maybe_get_checkpointable_versions(&[deleted_replacement], table_id);

    assert!(checkpointable.is_empty());
}

#[test]
fn collect_table_rows_preempts_on_large_scan() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();
    let mvstore = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint = CheckpointStateMachine::new(
        pager,
        mvstore.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    // More than one chunk worth of committed rows so collection must preempt.
    let table_id = MVTableId::from(-2);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        let version = committed_table_row_version(table_id, i);
        let mut versions =
            <crate::mvcc::database::RowVersionChain<crate::alloc::DynAllocator> as crate::alloc::TursoVecInExt<
                RowVersion,
                crate::alloc::DynAllocator,
            >>::new_in(crate::alloc::DynAllocator::default());
        versions.push(version);
        mvstore.rows.insert(
            RowID::new(table_id, RowKey::Int(i)),
            Arc::new(RwLock::new(versions)),
        );
    }

    // The first chunk fills up before the scan finishes, so it must yield.
    let first = checkpoint.collect_table_rows().unwrap();
    assert!(
        first.is_some_and(|io| io.is_explicit_yield()),
        "scanning more than COLLECT_PREEMPTION_THRESHOLD rows must preempt with an explicit yield"
    );

    // Resume from the cursor until the scan finishes; every row must still
    // be collected exactly once across the chunks.
    while checkpoint.collect_table_rows().unwrap().is_some() {}
    assert_eq!(checkpoint.write_set.len(), row_count);
}

#[test]
fn collect_index_rows_preempts_on_large_scan() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();
    let mvstore = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint = CheckpointStateMachine::new(
        pager,
        mvstore.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    let index_id = MVTableId::from(-7);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        let (key, version) = index_row_version(index_id, "k", i, 1, Some(5), None, false);
        mvstore
            .insert_index_version(index_id, key, version)
            .unwrap();
    }

    let first = checkpoint.collect_index_rows().unwrap();
    assert!(
        first.is_some_and(|io| io.is_explicit_yield()),
        "scanning more than COLLECT_PREEMPTION_THRESHOLD index rows must preempt with an explicit yield"
    );

    while checkpoint.collect_index_rows().unwrap().is_some() {}
    assert_eq!(checkpoint.index_write_set.len(), row_count);
}

#[test]
fn gc_checkpointed_table_versions_preempts_on_large_scan() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();
    let mvstore = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint = CheckpointStateMachine::new(
        pager,
        mvstore.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    checkpoint.lock_states.blocking_checkpoint_lock_held = true;
    checkpoint.durable_txid_max_new = 5;

    let table_id = MVTableId::from(-2);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        let version = committed_table_row_version(table_id, i);
        let row_id = RowID::new(table_id, RowKey::Int(i));
        let mut versions =
            <crate::mvcc::database::RowVersionChain<crate::alloc::DynAllocator> as crate::alloc::TursoVecInExt<
                RowVersion,
                crate::alloc::DynAllocator,
            >>::new_in(crate::alloc::DynAllocator::default());
        versions.push(version.clone());
        mvstore.rows.insert(row_id, Arc::new(RwLock::new(versions)));
        checkpoint.write_set.push((version, None));
        checkpoint.written_table_rowids.insert((table_id, i));
    }
    // The real run publishes `backfill_floor` from the checkpointed WAL right
    // before entering `GcTableRows`. Rule 3 needs every reader mark to have
    // reached the stamp, and `gc_floor_reader_mark` is clamped by this floor,
    // so a fixture that jumps straight into the state must publish it too.
    *mvstore.backfill_floor.write() = WalPos::from_pair(checkpoint.pager.wal_pos());
    checkpoint.state = CheckpointState::GcTableRows {
        next_index: 0,
        lwm: u64::MAX,
    };

    let first = checkpoint.gc_checkpointed_table_versions();
    assert!(
        first.is_some_and(|io| io.is_explicit_yield()),
        "GCing more than COLLECT_PREEMPTION_THRESHOLD rows must preempt with an explicit yield"
    );

    while checkpoint.gc_checkpointed_table_versions().is_some() {}
    // Write-set GC clears version chains but leaves empty SkipMap slots;
    // Truncate Finalize `_and_slots` unlinks them.
    let slots = mvstore
        .rows
        .iter()
        .filter(|entry| entry.key().table_id == table_id)
        .count();
    let versions: usize = mvstore
        .rows
        .iter()
        .filter(|entry| entry.key().table_id == table_id)
        .map(|entry| entry.value().read().len())
        .sum();
    assert_eq!(versions, 0, "write-set GC must reclaim version chains");
    assert_eq!(
        slots, row_count,
        "empty slots remain until Finalize `_and_slots`"
    );
    mvstore.drop_unused_row_versions_and_slots();
    let remaining = mvstore
        .rows
        .iter()
        .filter(|entry| entry.key().table_id == table_id)
        .count();
    assert_eq!(remaining, 0);
}

#[test]
fn gc_checkpointed_index_versions_preempts_on_large_scan() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();
    let mvstore = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint = CheckpointStateMachine::new(
        pager,
        mvstore.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    checkpoint.lock_states.blocking_checkpoint_lock_held = true;
    checkpoint.durable_txid_max_new = 5;

    let index_id = MVTableId::from(-7);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        let (key, version) = index_row_version(index_id, "k", i, 1, Some(5), None, false);
        mvstore
            .insert_index_version(index_id, key, version.clone())
            .unwrap();
        checkpoint.index_write_set.push((index_id, version, false));
        checkpoint.written_index_slots.insert(i as usize);
    }
    // See the table variant: publish the floor the real `GcIndexRows` entry
    // would have published, or Rule 3 keeps every stamped current.
    *mvstore.backfill_floor.write() = WalPos::from_pair(checkpoint.pager.wal_pos());
    checkpoint.state = CheckpointState::GcIndexRows {
        next_index: 0,
        lwm: u64::MAX,
    };

    let first = checkpoint.gc_checkpointed_index_versions();
    assert!(
        first.is_some_and(|io| io.is_explicit_yield()),
        "GCing more than COLLECT_PREEMPTION_THRESHOLD index rows must preempt with an explicit yield"
    );

    while checkpoint.gc_checkpointed_index_versions().is_some() {}
    // Write-set GC clears version chains but leaves empty SkipMap slots;
    // Truncate Finalize `_and_slots` unlinks them.
    let inner = mvstore
        .index_rows
        .get(&index_id)
        .expect("index map must exist");
    let slots = inner.value().len();
    let versions: usize = inner
        .value()
        .iter()
        .map(|entry| entry.value().read().len())
        .sum();
    assert_eq!(
        versions, 0,
        "write-set GC must reclaim index version chains"
    );
    assert_eq!(
        slots, row_count,
        "empty index slots remain until Finalize `_and_slots`"
    );
    mvstore.drop_unused_row_versions_and_slots();
    let remaining = mvstore
        .index_rows
        .get(&index_id)
        .map_or(0, |entry| entry.value().len());
    assert_eq!(remaining, 0);
}
