use crate::mvcc_store::database::MvccTestDbNoConn;
use std::num::NonZeroU64;
use turso_core::alloc::vec;
use turso_core::alloc::TursoAllocator;
use turso_core::mvcc::database::checkpoint_state_machine::*;
use turso_core::mvcc::database::MVTableId;
use turso_core::mvcc::database::MvStore;
use turso_core::mvcc::database::RowID;
use turso_core::mvcc::database::RowKey;
use turso_core::mvcc::database::RowVersion;
use turso_core::mvcc::database::TxTimestampOrID;
use turso_core::mvcc::database::WalPos;
use turso_core::mvcc::database::SQLITE_SCHEMA_MVCC_TABLE_ID;
use turso_core::mvcc::database::{Row, SortableIndexKey};
use turso_core::storage::wal::CheckpointMode;
use turso_core::sync::atomic::Ordering;
use turso_core::sync::Arc;
use turso_core::sync::RwLock;
use turso_core::translate::collate::CollationSeq;
use turso_core::types::IOCompletions;
use turso_core::types::ImmutableRecord;
use turso_core::types::ImmutableRecordRef;
use turso_core::types::{IndexInfo, KeyInfo};
use turso_core::Result;
use turso_core::Value;
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
        begin: turso_core::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
        end: turso_core::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
        data: Row::new_table_row(
            RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(rowid)),
            record.as_blob(),
        )
        .unwrap()
        .data,
        btree_resident: false,
        materialized_at: turso_core::mvcc::database::WalPos::ORIGIN,
    }
}

#[test]
fn local_schema_record_shares_mvcc_payload() {
    let version = sqlite_schema_row_version(2, "table", "t", "t", -2, Some(1), None);
    let payload = version.data.as_ref().unwrap();
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
        sqlite_schema_btree_identity(SQLITE_SCHEMA_MVCC_TABLE_ID, &old),
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

    assert_eq!(
        sqlite_schema_btree_identity(SQLITE_SCHEMA_MVCC_TABLE_ID, &trigger),
        None
    );
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
        begin: turso_core::mvcc::database::PackedTs::pack(None),
        end: turso_core::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(2))),
        data: Row::new_table_row(RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(9)), &[])
            .unwrap()
            .data,
        btree_resident: false,
        materialized_at: turso_core::mvcc::database::WalPos::ORIGIN,
    };

    assert_eq!(
        sqlite_schema_btree_identity(SQLITE_SCHEMA_MVCC_TABLE_ID, &tombstone),
        None
    );
    assert!(!is_schema_metadata_only_rewrite(&tombstone, None));
}

fn index_row_version(
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
                    collation: CollationSeq::BINARY,
                    nulls_order: None,
                },
                KeyInfo {
                    sort_order: SortOrder::Asc,
                    collation: CollationSeq::BINARY,
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
            Value::Text(turso_core::types::Text::new(key_text.to_string())),
            Value::from_i64(rowid),
        ],
        2,
    )
    .unwrap();
    let sortable_key =
        SortableIndexKey::new_from_payload_in(&key_record, index_info, TursoAllocator).unwrap();
    let key_arc = Arc::new(sortable_key);
    let row_version = RowVersion {
        id: version_id,
        begin: turso_core::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
        end: turso_core::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
        data: None,
        btree_resident,
        materialized_at: turso_core::mvcc::database::WalPos::ORIGIN,
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
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    checkpoint.durable_txid_max_old = std::num::NonZeroU64::new(10);
    checkpoint.durable_txid_max_new = 10;

    let index_id = MVTableId::from(-42);
    let (garbage_key, garbage_version) =
        index_row_version("blue_river_906", 75, 1, None, None, true);
    let (_, tombstone_version) = index_row_version("blue_river_906", 75, 2, None, Some(10), true);

    insert_dirty_index_version(&mvstore, index_id, garbage_key, garbage_version);
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
    insert_dirty_index_version(&mvstore, index_id, tombstone_key, tombstone_version);

    while checkpoint.collect_index_rows().unwrap().is_some() {}

    assert!(
        checkpoint.index_write_set.is_empty(),
        "a retry checkpoint must not replay a delete whose btree_resident tombstone was already made durable"
    );
}

fn committed_table_row_version(table_id: MVTableId, rowid: i64) -> (RowID, RowVersion) {
    table_row_version(table_id, rowid, 1, Some(5), None, false)
}

fn table_row_version(
    table_id: MVTableId,
    rowid: i64,
    version_id: u64,
    begin: Option<u64>,
    end: Option<u64>,
    btree_resident: bool,
) -> (RowID, RowVersion) {
    let record = ImmutableRecord::from_values(&[Value::from_i64(rowid)], 1).unwrap();
    let row =
        Row::new_table_row(RowID::new(table_id, RowKey::Int(rowid)), record.as_blob()).unwrap();
    let version = RowVersion {
        id: version_id,
        begin: turso_core::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
        end: turso_core::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
        data: row.data,
        btree_resident,
        materialized_at: turso_core::mvcc::database::WalPos::ORIGIN,
    };
    (row.id, version)
}

fn checkpoint_for_collect_tests(
) -> CheckpointStateMachine<turso_core::mvcc::clock::MvccClock, turso_core::alloc::DynAllocator> {
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
        turso_core::MAIN_DB_ID,
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
    let (_, btree_tombstone) = table_row_version(table_id, 1, 1, None, Some(5), true);
    let (_, replacement) = table_row_version(table_id, 1, 2, Some(5), None, false);

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
    let (_, btree_tombstone) = table_row_version(table_id, 1, 1, None, Some(5), true);
    let (_, deleted_replacement) = table_row_version(table_id, 1, 2, Some(5), Some(6), false);

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
    let (_, deleted_replacement) = table_row_version(table_id, 1, 2, Some(5), Some(6), false);

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
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    let table_id = MVTableId::from(-2);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        insert_row_version(&mvstore, committed_table_row_version(table_id, i));
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

fn insert_row_version(
    mvstore: &turso_core::sync::Arc<
        MvStore<turso_core::mvcc::clock::MvccClock, turso_core::alloc::DynAllocator>,
    >,
    (key, version): (RowID, RowVersion),
) {
    let mut versions =
        <turso_core::mvcc::database::RowVersionChain<turso_core::alloc::DynAllocator> as turso_core::alloc::TursoVecInExt<
            RowVersion,
            turso_core::alloc::DynAllocator,
        >>::new_in(turso_core::alloc::DynAllocator::default());
    let stamp = version_stamp(&version);
    versions.push(version);
    mvstore
        .rows
        .insert(key.clone(), Arc::new(RwLock::new(versions)));
    mvstore.mark_checkpoint_dirty_key(&key, stamp).unwrap();
}

fn version_stamp(version: &RowVersion) -> u64 {
    [version.begin(), version.end()]
        .into_iter()
        .filter_map(|stamp| match stamp {
            Some(TxTimestampOrID::Timestamp(ts)) => Some(ts),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

fn insert_clean_row_version(
    mvstore: &turso_core::sync::Arc<
        MvStore<turso_core::mvcc::clock::MvccClock, turso_core::alloc::DynAllocator>,
    >,
    (key, version): (RowID, RowVersion),
) {
    let mut versions =
        <turso_core::mvcc::database::RowVersionChain<turso_core::alloc::DynAllocator> as turso_core::alloc::TursoVecInExt<
            RowVersion,
            turso_core::alloc::DynAllocator,
        >>::new_in(turso_core::alloc::DynAllocator::default());
    versions.push(version);
    mvstore.rows.insert(key, Arc::new(RwLock::new(versions)));
}

fn insert_dirty_index_version(
    mvstore: &turso_core::sync::Arc<
        MvStore<turso_core::mvcc::clock::MvccClock, turso_core::alloc::DynAllocator>,
    >,
    index_id: MVTableId,
    key: Arc<SortableIndexKey>,
    version: RowVersion,
) {
    let stamp = version_stamp(&version);
    let (key, _, _) = mvstore
        .insert_index_version(index_id, key, version)
        .unwrap();
    mvstore
        .mark_checkpoint_dirty_key(&RowID::new(index_id, RowKey::Record(key)), stamp)
        .unwrap();
}

#[test]
fn collect_table_rows_skips_user_keys_after_schema_drop() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let dropped = MVTableId::from(-2);
    let kept = MVTableId::from(-3);
    insert_row_version(
        &mvstore,
        (
            RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(2)),
            sqlite_schema_row_version(2, "table", "t", "t", -2, Some(5), Some(6)),
        ),
    );
    insert_row_version(&mvstore, committed_table_row_version(dropped, 1));
    insert_row_version(&mvstore, committed_table_row_version(dropped, 2));
    insert_row_version(&mvstore, committed_table_row_version(kept, 1));

    while checkpoint.collect_table_rows().unwrap().is_some() {}

    assert!(
        checkpoint.destroyed_tables.contains(&dropped),
        "schema DROP must mark the table before user keys are visited"
    );
    assert!(
        checkpoint
            .write_set
            .iter()
            .all(|(row_id, _, _)| row_id.table_id != dropped),
        "user rows of a table dropped in the schema pass must not enter write_set"
    );
    assert!(
        checkpoint
            .write_set
            .iter()
            .any(|(row_id, _, _)| row_id.table_id == kept),
        "rows of a table that was not dropped must still be collected"
    );
}

#[test]
fn collect_table_rows_yields_in_schema_pass() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    let schema_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..schema_count as i64 {
        insert_row_version(
            &mvstore,
            (
                RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(i + 1)),
                sqlite_schema_row_version(i + 1, "table", "t", "t", -2, Some(5), None),
            ),
        );
    }
    insert_row_version(&mvstore, committed_table_row_version(table_id, 1));

    let first = checkpoint.collect_table_rows().unwrap();
    assert!(
        first.is_some_and(|io| io.is_explicit_yield()),
        "a schema pass larger than COLLECT_PREEMPTION_THRESHOLD must yield"
    );
    assert_eq!(checkpoint.collect_table_phase, CollectTablePhase::Schema);
    assert_eq!(
        checkpoint
            .collect_table_cursor
            .last_visited
            .as_ref()
            .map(|key| key.table_id),
        Some(SQLITE_SCHEMA_MVCC_TABLE_ID)
    );

    while checkpoint.collect_table_rows().unwrap().is_some() {}
    assert!(
        checkpoint
            .write_set
            .iter()
            .any(|(row_id, _, _)| row_id.table_id == table_id),
        "user rows after a schema-pass yield must still be collected"
    );
}

#[test]
fn collect_table_rows_yields_in_user_pass() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    insert_row_version(
        &mvstore,
        (
            RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(2)),
            sqlite_schema_row_version(2, "table", "t", "t", -2, Some(5), None),
        ),
    );
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        insert_row_version(&mvstore, committed_table_row_version(table_id, i));
    }

    let first = checkpoint.collect_table_rows().unwrap();
    assert!(
        first.is_some_and(|io| io.is_explicit_yield()),
        "a user pass larger than COLLECT_PREEMPTION_THRESHOLD must yield"
    );
    assert_eq!(checkpoint.collect_table_phase, CollectTablePhase::User);
    assert_eq!(
        checkpoint
            .collect_table_cursor
            .last_visited
            .as_ref()
            .map(|key| key.table_id),
        Some(table_id)
    );

    while checkpoint.collect_table_rows().unwrap().is_some() {}
    assert_eq!(
        checkpoint
            .write_set
            .iter()
            .filter(|(row_id, _, _)| row_id.table_id == table_id)
            .count(),
        row_count
    );
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
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    let index_id = MVTableId::from(-7);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        let (key, version) = index_row_version("k", i, 1, Some(5), None, false);
        insert_dirty_index_version(&mvstore, index_id, key, version);
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
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    checkpoint.lock_states.blocking_checkpoint_lock_held = true;
    checkpoint.durable_txid_max_new = 5;

    let table_id = MVTableId::from(-2);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        let (row_id, version) = committed_table_row_version(table_id, i);
        let mut versions = <turso_core::mvcc::database::RowVersionChain<
            turso_core::alloc::DynAllocator,
        > as turso_core::alloc::TursoVecInExt<RowVersion, turso_core::alloc::DynAllocator>>::new_in(
            turso_core::alloc::DynAllocator::default(),
        );
        versions.push(version.clone());
        mvstore
            .rows
            .insert(row_id.clone(), Arc::new(RwLock::new(versions)));
        checkpoint.write_set.push((row_id, version, None));
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
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    checkpoint.lock_states.blocking_checkpoint_lock_held = true;
    checkpoint.durable_txid_max_new = 5;

    let index_id = MVTableId::from(-7);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for i in 0..row_count as i64 {
        let (key, version) = index_row_version("k", i, 1, Some(5), None, false);
        let row_id = RowID::new(index_id, RowKey::Record(key.clone()));
        insert_dirty_index_version(&mvstore, index_id, key, version.clone());
        checkpoint.index_write_set.push((row_id, version, false));
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

fn dirty_table_key(
    mvstore: &turso_core::sync::Arc<
        MvStore<turso_core::mvcc::clock::MvccClock, turso_core::alloc::DynAllocator>,
    >,
    table_id: MVTableId,
    rowid: i64,
) -> bool {
    mvstore
        .checkpoint_dirty_table_keys
        .contains_key(&RowID::new(table_id, RowKey::Int(rowid)))
}

#[test]
fn collect_table_rows_visits_only_dirty_keys() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    for rowid in 1..=10 {
        insert_clean_row_version(
            &mvstore,
            table_row_version(table_id, rowid, 1, Some(1), None, false),
        );
    }
    insert_row_version(&mvstore, committed_table_row_version(table_id, 5));

    while checkpoint.collect_table_rows().unwrap().is_some() {}

    assert_eq!(checkpoint.write_set.len(), 1);
    assert_eq!(checkpoint.write_set[0].0.row_id, RowKey::Int(5));
}

#[test]
fn collect_table_rows_full_scan_visits_every_row() {
    let mut checkpoint = checkpoint_for_collect_tests();
    checkpoint.full_scan_generation = NonZeroU64::new(1);
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    for rowid in 1..=10 {
        insert_clean_row_version(
            &mvstore,
            table_row_version(table_id, rowid, 1, Some(1), None, false),
        );
    }
    insert_clean_row_version(&mvstore, committed_table_row_version(table_id, 5));

    while checkpoint.collect_table_rows().unwrap().is_some() {}

    assert_eq!(
        checkpoint.write_set.len(),
        1,
        "a full scan must find a checkpointable row that no commit marked"
    );
    assert_eq!(checkpoint.write_set[0].0.row_id, RowKey::Int(5));
}

#[test]
fn collect_table_rows_tolerates_dirty_key_without_chain() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    mvstore
        .mark_checkpoint_dirty_key(&RowID::new(table_id, RowKey::Int(7)), 5)
        .unwrap();
    checkpoint.snapshot_ts = 10;

    while checkpoint.collect_table_rows().unwrap().is_some() {}
    assert!(checkpoint.write_set.is_empty());

    assert!(checkpoint.prune_dirty_keys().is_none());
    assert!(!dirty_table_key(&mvstore, table_id, 7));
}

#[test]
fn prune_dirty_keys_keeps_key_marked_again_after_collect() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    let key = RowID::new(table_id, RowKey::Int(7));
    mvstore.mark_checkpoint_dirty_key(&key, 5).unwrap();
    checkpoint.snapshot_ts = 10;

    while checkpoint.collect_table_rows().unwrap().is_some() {}
    mvstore.mark_checkpoint_dirty_key(&key, 50).unwrap();

    assert!(checkpoint.prune_dirty_keys().is_none());
    let entry = mvstore
        .checkpoint_dirty_table_keys
        .get(&key)
        .expect("a key marked after collect must stay dirty");
    assert_eq!(entry.value().load(Ordering::Acquire), 50);
}

#[test]
fn collect_index_rows_visits_only_dirty_keys() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let index_id = MVTableId::from(-7);
    let (clean_key, clean_version) = index_row_version("k", 1, 1, Some(1), None, false);
    mvstore
        .insert_index_version(index_id, clean_key, clean_version)
        .unwrap();
    let (dirty_key, dirty_version) = index_row_version("k", 2, 1, Some(5), None, false);
    insert_dirty_index_version(&mvstore, index_id, dirty_key, dirty_version);

    while checkpoint.collect_index_rows().unwrap().is_some() {}

    assert_eq!(checkpoint.index_write_set.len(), 1);
    assert_eq!(checkpoint.index_write_set[0].1.id, 1);
    assert!(checkpoint.collect_dirty_index_cursor.last_visited.is_some());
    assert!(checkpoint.collect_index_key_cursor.is_none());
}

fn collect_while_rows_are_appended(
    mut collect: impl FnMut() -> Result<Option<IOCompletions>>,
    mut append: impl FnMut(),
) {
    let mut yields = 0;
    while collect().unwrap().is_some() {
        yields += 1;
        assert!(
            yields <= 4,
            "collect kept scanning rows committed after its snapshot"
        );
        append();
    }
}

fn append_table_rows(
    mvstore: &turso_core::sync::Arc<
        MvStore<turso_core::mvcc::clock::MvccClock, turso_core::alloc::DynAllocator>,
    >,
    table_id: MVTableId,
    next_rowid: &mut i64,
) {
    for _ in 0..COLLECT_PREEMPTION_THRESHOLD {
        insert_row_version(
            mvstore,
            table_row_version(table_id, *next_rowid, 1, Some(20), None, false),
        );
        *next_rowid += 1;
    }
}

fn append_index_rows(
    mvstore: &turso_core::sync::Arc<
        MvStore<turso_core::mvcc::clock::MvccClock, turso_core::alloc::DynAllocator>,
    >,
    index_id: MVTableId,
    next_rowid: &mut i64,
) {
    for _ in 0..COLLECT_PREEMPTION_THRESHOLD {
        let (key, version) = index_row_version("k", *next_rowid, 1, Some(20), None, false);
        insert_dirty_index_version(mvstore, index_id, key, version);
        *next_rowid += 1;
    }
}

#[test]
fn collect_table_rows_finishes_while_rows_are_appended() {
    for full_scan in [false, true] {
        let mut checkpoint = checkpoint_for_collect_tests();
        checkpoint.snapshot_ts = 10;
        checkpoint.full_scan_generation = full_scan.then(|| NonZeroU64::new(1).unwrap());
        let mvstore = checkpoint.mvstore.clone();
        let walked = MVTableId::from(-3);
        let other = MVTableId::from(-2);
        let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
        for rowid in 0..row_count as i64 {
            insert_row_version(&mvstore, committed_table_row_version(walked, rowid));
        }
        insert_row_version(&mvstore, committed_table_row_version(other, 1));

        let mut next_rowid = row_count as i64;
        collect_while_rows_are_appended(
            || checkpoint.collect_table_rows(),
            || append_table_rows(&mvstore, walked, &mut next_rowid),
        );

        assert_eq!(
            checkpoint.write_set.len(),
            row_count + 1,
            "full_scan={full_scan}: every row committed before the snapshot is collected once"
        );
    }
}

#[test]
fn collect_index_rows_finishes_while_index_rows_are_appended() {
    for full_scan in [false, true] {
        let mut checkpoint = checkpoint_for_collect_tests();
        checkpoint.snapshot_ts = 10;
        checkpoint.full_scan_generation = full_scan.then(|| NonZeroU64::new(1).unwrap());
        let mvstore = checkpoint.mvstore.clone();
        let walked = MVTableId::from(-8);
        let other = MVTableId::from(-7);
        let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
        for rowid in 0..row_count as i64 {
            let (key, version) = index_row_version("k", rowid, 1, Some(5), None, false);
            insert_dirty_index_version(&mvstore, walked, key, version);
        }
        let (key, version) = index_row_version("k", 1, 1, Some(5), None, false);
        insert_dirty_index_version(&mvstore, other, key, version);

        let mut next_rowid = row_count as i64;
        collect_while_rows_are_appended(
            || checkpoint.collect_index_rows(),
            || append_index_rows(&mvstore, walked, &mut next_rowid),
        );

        assert_eq!(
            checkpoint.index_write_set.len(),
            row_count + 1,
            "full_scan={full_scan}: every index row committed before the snapshot is collected once"
        );
    }
}

#[test]
fn prune_dirty_keys_keeps_keys_with_versions_past_the_snapshot() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    insert_row_version(&mvstore, committed_table_row_version(table_id, 1));
    insert_row_version(
        &mvstore,
        table_row_version(table_id, 2, 1, Some(50), None, false),
    );
    insert_row_version(
        &mvstore,
        table_row_version(table_id, 3, 1, Some(5), Some(50), false),
    );
    let index_id = MVTableId::from(-7);
    let (old_key, old_version) = index_row_version("k", 1, 1, Some(5), None, false);
    insert_dirty_index_version(&mvstore, index_id, old_key, old_version);
    let (new_key, new_version) = index_row_version("k", 2, 1, Some(50), None, false);
    insert_dirty_index_version(&mvstore, index_id, new_key, new_version);
    checkpoint.snapshot_ts = 10;
    while checkpoint.collect_table_rows().unwrap().is_some() {}
    while checkpoint.collect_index_rows().unwrap().is_some() {}

    assert!(checkpoint.prune_dirty_keys().is_none());

    assert!(!dirty_table_key(&mvstore, table_id, 1));
    assert!(dirty_table_key(&mvstore, table_id, 2));
    assert!(dirty_table_key(&mvstore, table_id, 3));
    let remaining: std::vec::Vec<_> = mvstore
        .checkpoint_dirty_index_keys
        .iter()
        .map(|entry| entry.key().clone())
        .collect();
    assert_eq!(
        remaining.len(),
        1,
        "only the index key with a newer version stays: {remaining:?}"
    );
    let index = mvstore.index_rows.get(&index_id).unwrap();
    let RowKey::Record(remaining_key) = &remaining[0].row_id else {
        panic!("index dirty keys carry Record keys");
    };
    let chain = index.value().get(remaining_key).unwrap();
    assert_eq!(
        chain.value().read()[0].begin(),
        Some(TxTimestampOrID::Timestamp(50))
    );
}

#[test]
fn prune_dirty_keys_preempts_on_large_map() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
    for rowid in 0..row_count as i64 {
        insert_row_version(&mvstore, committed_table_row_version(table_id, rowid));
    }
    checkpoint.snapshot_ts = 10;
    while checkpoint.collect_table_rows().unwrap().is_some() {}
    while checkpoint.collect_index_rows().unwrap().is_some() {}

    let first = checkpoint.prune_dirty_keys();
    assert!(
        first.is_some_and(|io| io.is_explicit_yield()),
        "pruning more than COLLECT_PREEMPTION_THRESHOLD keys must preempt with an explicit yield"
    );
    while checkpoint.prune_dirty_keys().is_some() {}
    assert!(mvstore.checkpoint_dirty_table_keys.is_empty());
}

#[test]
fn dirty_walk_finds_near_and_far_keys_and_skips_missing_chains() {
    let mut checkpoint = checkpoint_for_collect_tests();
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    for rowid in 1..=200 {
        insert_clean_row_version(
            &mvstore,
            table_row_version(table_id, rowid, 1, Some(1), None, false),
        );
    }
    for rowid in [1, 2, 3, 150, 200] {
        insert_row_version(&mvstore, committed_table_row_version(table_id, rowid));
    }
    mvstore
        .mark_checkpoint_dirty_key(&RowID::new(table_id, RowKey::Int(60)), 5)
        .unwrap();
    mvstore.rows.remove(&RowID::new(table_id, RowKey::Int(60)));

    while checkpoint.collect_table_rows().unwrap().is_some() {}

    let collected: std::vec::Vec<i64> = checkpoint
        .write_set
        .iter()
        .map(|(row_id, _, _)| row_id.row_id.to_int_or_panic())
        .collect();
    assert_eq!(collected, vec![1, 2, 3, 150, 200]);
}

#[test]
fn full_scan_prunes_dirty_keys_it_covered() {
    let mut checkpoint = checkpoint_for_collect_tests();
    checkpoint.full_scan_generation = NonZeroU64::new(1);
    checkpoint.snapshot_ts = 10;
    let mvstore = checkpoint.mvstore.clone();
    let table_id = MVTableId::from(-2);
    insert_row_version(&mvstore, committed_table_row_version(table_id, 1));
    insert_row_version(
        &mvstore,
        table_row_version(table_id, 2, 1, Some(50), None, false),
    );
    let index_id = MVTableId::from(-7);
    let (key, version) = index_row_version("k", 1, 1, Some(5), None, false);
    insert_dirty_index_version(&mvstore, index_id, key, version);

    while checkpoint.collect_table_rows().unwrap().is_some() {}
    while checkpoint.collect_index_rows().unwrap().is_some() {}
    assert!(checkpoint.prune_dirty_keys().is_none());

    assert!(!dirty_table_key(&mvstore, table_id, 1));
    assert!(dirty_table_key(&mvstore, table_id, 2));
    assert!(mvstore.checkpoint_dirty_index_keys.is_empty());
}
