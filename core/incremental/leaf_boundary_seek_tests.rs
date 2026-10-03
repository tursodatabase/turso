//! A DBSP state index seek must also reach an entry that is the first cell of the
//! next leaf page. A seek that stops short turns a weight update into a second state
//! row, drops the next MIN/MAX candidate, or cuts a join scan short.

use crate::incremental::aggregate_operator::{ScanState, AGG_TYPE_MINMAX, AGG_TYPE_REGULAR};
use crate::incremental::dbsp::{Hash128, HashableRow};
use crate::incremental::join_operator::{read_next_join_row, serialize_hashable_row};
use crate::incremental::operator::{
    create_dbsp_state_index, generate_storage_id, DbspStateCursors,
};
use crate::incremental::persistence::{seek_dbsp_index_key, LeafBoundarySeek, WriteRow};
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::types::{IOResult, SeekKey, SeekOp, SeekResult};
use crate::util::IOExt;
use crate::{Connection, Database, MemoryIO, Pager, SqliteDialect, Value, IO};
use rustc_hash::FxHashMap as HashMap;

/// Same key shape the aggregate operator uses: operator id, 16-byte group hash,
/// 16-byte element id.
fn keys(n: usize) -> Vec<[u8; 16]> {
    let mut out = Vec::with_capacity(n);
    let mut state: u64 = 0x243f_6a88_85a3_08d3;
    for _ in 0..n {
        let mut b = [0u8; 16];
        for chunk in b.chunks_mut(8) {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        out.push(b);
    }
    out
}

fn index_key(zset: &[u8; 16]) -> Vec<Value> {
    vec![
        Value::from_i64(131072),
        Value::from_slice(zset).unwrap(),
        Value::from_slice(&[0u8; 16]).unwrap(),
    ]
}

fn setup() -> (Arc<Connection>, Arc<Pager>, DbspStateCursors) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let pager = conn.pager.load().clone();
    let _ = pager.io.block(|| pager.allocate_page1());
    let table_root = pager
        .io
        .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
        .unwrap() as i64;
    let index_root = pager
        .io
        .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
        .unwrap() as i64;

    let table_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
    let index_def = create_dbsp_state_index(index_root);
    let index_cursor = BTreeCursor::new_index(pager.clone(), index_root, &index_def, 4).unwrap();
    (
        conn,
        pager,
        DbspStateCursors::new(table_cursor, index_cursor),
    )
}

fn write_state_row(
    pager: &Arc<Pager>,
    cursors: &mut DbspStateCursors,
    index_key: Vec<Value>,
    value: Value,
) {
    let mut record_values = index_key.clone();
    record_values.push(value);
    let mut wr = WriteRow::new();
    pager
        .io
        .block(|| wr.write_row(cursors, index_key.clone(), record_values.clone(), 1))
        .unwrap();
}

#[test]
fn write_row_updates_every_state_row_in_a_multi_page_index() {
    let (_conn, pager, mut cursors) = setup();

    // Enough keys that the index btree has more than one leaf. One page of
    // entries never reproduces this.
    let keys = keys(400);
    for zset in &keys {
        write_state_row(
            &pager,
            &mut cursors,
            index_key(zset),
            Value::from_slice(&[7u8; 24]).unwrap(),
        );
    }

    let mut index_entries = 0;
    pager.io.block(|| cursors.index_cursor.rewind()).unwrap();
    loop {
        let present = pager
            .io
            .block(|| {
                Ok(match cursors.index_cursor.record()? {
                    IOResult::Done(r) => IOResult::Done(r.is_some()),
                    IOResult::IO(io) => IOResult::IO(io),
                })
            })
            .unwrap();
        if !present {
            break;
        }
        index_entries += 1;
        pager.io.block(|| cursors.index_cursor.next()).unwrap();
    }
    assert_eq!(index_entries, keys.len(), "index lost entries");

    // Second write of every key: a lookup that misses inserts a duplicate
    // instead of bumping the weight.
    for zset in &keys {
        write_state_row(
            &pager,
            &mut cursors,
            index_key(zset),
            Value::from_slice(&[7u8; 24]).unwrap(),
        );
    }

    let mut table_rows = 0;
    let mut wrong_weight = 0;
    pager.io.block(|| cursors.table_cursor.rewind()).unwrap();
    while pager
        .io
        .block(|| cursors.table_cursor.rowid())
        .unwrap()
        .is_some()
    {
        table_rows += 1;
        let weight = pager
            .io
            .block(|| {
                Ok(match cursors.table_cursor.record()? {
                    IOResult::Done(r) => IOResult::Done(
                        r.map(|r| r.get_value(4).unwrap().to_owned().unwrap())
                            .unwrap_or(Value::Null),
                    ),
                    IOResult::IO(io) => IOResult::IO(io),
                })
            })
            .unwrap();
        if weight != Value::from_i64(2) {
            wrong_weight += 1;
        }
        pager.io.block(|| cursors.table_cursor.next()).unwrap();
    }

    assert_eq!(
        table_rows,
        keys.len(),
        "duplicate DBSP state rows: WriteRow::GetRecord failed to find keys that are \
         present (leaf-boundary eq_only seek)"
    );
    assert_eq!(
        wrong_weight, 0,
        "{wrong_weight} state rows did not accumulate their second write"
    );
}

/// The MIN/MAX scan walks the state index with GT (MIN) and LT (MAX) to find the
/// next extreme after the current one is retracted. Every step must cross a leaf
/// page boundary, or the group keeps a MIN/MAX that no longer exists.
#[test]
fn min_max_scan_steps_over_every_leaf_boundary() {
    let (_conn, pager, mut cursors) = setup();

    let storage_id = generate_storage_id(9, 0, AGG_TYPE_MINMAX);
    let zset_hash = Hash128::new(0x5eed, 0xf00d);
    // Enough values that the index btree has more than one leaf.
    let values: Vec<i64> = (1..=400).collect();
    for value in &values {
        write_state_row(
            &pager,
            &mut cursors,
            vec![
                Value::from_i64(storage_id),
                zset_hash.to_value().unwrap(),
                Value::from_i64(*value),
            ],
            Value::Null,
        );
    }

    let retracted = |value: i64| {
        let mut group_values: HashMap<(usize, HashableRow), isize> = HashMap::default();
        group_values.insert(
            (0usize, HashableRow::new(0, vec![Value::from_i64(value)])),
            0,
        );
        group_values
    };

    let mut missing_mins = Vec::new();
    for value in &values[..values.len() - 1] {
        let mut scan = ScanState::new_for_min(
            Some(Value::from_i64(*value)),
            "g".to_string(),
            0,
            storage_id,
            zset_hash,
            retracted(*value),
        );
        let found = pager
            .io
            .block(|| scan.find_new_value(&mut cursors))
            .unwrap();
        if found != Some(Value::from_i64(value + 1)) {
            missing_mins.push((*value, found));
        }
    }
    let mut missing_maxs = Vec::new();
    for value in &values[1..] {
        let mut scan = ScanState::new_for_max(
            Some(Value::from_i64(*value)),
            "g".to_string(),
            0,
            storage_id,
            zset_hash,
            retracted(*value),
        );
        let found = pager
            .io
            .block(|| scan.find_new_value(&mut cursors))
            .unwrap();
        if found != Some(Value::from_i64(value - 1)) {
            missing_maxs.push((*value, found));
        }
    }
    assert!(
        missing_mins.is_empty() && missing_maxs.is_empty(),
        "MIN scan missed {} and MAX scan missed {} of {} retracted values: {:?} {:?}",
        missing_mins.len(),
        missing_maxs.len(),
        values.len() - 1,
        &missing_mins[..missing_mins.len().min(4)],
        &missing_maxs[..missing_maxs.len().min(4)]
    );
}

/// The join reads all rows of one join key by repeated GT seeks on the state index.
/// A step that stops at a leaf page boundary makes the join lose the rest of the rows.
#[test]
fn join_lookup_reads_every_row_of_a_group_in_a_multi_page_index() {
    let (_conn, pager, mut cursors) = setup();

    let storage_id = generate_storage_id(11, 0, AGG_TYPE_REGULAR);
    let join_key = HashableRow::new(0, vec![Value::from_i64(42)]);
    let zset_hash = join_key.cached_hash();
    // Enough rows under one join key that the index btree has more than one leaf.
    let rows: Vec<HashableRow> = (1..=400)
        .map(|i| HashableRow::new(i, vec![Value::from_i64(42), Value::from_i64(i)]))
        .collect();
    for row in &rows {
        write_state_row(
            &pager,
            &mut cursors,
            vec![
                Value::from_i64(storage_id),
                zset_hash.to_value().unwrap(),
                row.cached_hash().to_value().unwrap(),
            ],
            Value::Blob(serialize_hashable_row(row).unwrap()),
        );
    }

    let mut read = 0;
    let mut last_element_hash = None;
    let mut seek = LeafBoundarySeek::default();
    while let Some((element_hash, _row, _weight)) = pager
        .io
        .block(|| {
            read_next_join_row(
                &mut seek,
                storage_id,
                &join_key,
                last_element_hash,
                &mut cursors,
            )
        })
        .unwrap()
    {
        read += 1;
        last_element_hash = Some(element_hash);
        assert!(read <= rows.len(), "join scan does not terminate");
    }

    assert_eq!(
        read,
        rows.len(),
        "the join scan stopped after {read} of {} rows of the join key",
        rows.len()
    );
}

/// The step to the next leaf page can yield for I/O. The seek must not run a second
/// time when the operation is polled again: the cursor is then already in the middle
/// of an advance.
#[test]
fn seek_runs_once_when_the_step_to_the_next_leaf_yields() {
    let mut cursor = CountingCursor::new();
    let mut seek = LeafBoundarySeek::default();
    let key = index_key(&[3u8; 16]);

    let yielded = seek_dbsp_index_key(&mut seek, &mut cursor, &key).unwrap();
    assert!(
        matches!(yielded, IOResult::IO(_)),
        "the advance was supposed to yield, got {yielded:?}"
    );

    let resumed = seek_dbsp_index_key(&mut seek, &mut cursor, &key).unwrap();
    assert!(
        matches!(resumed, IOResult::Done(false)),
        "expected the empty position after the advance, got {resumed:?}"
    );

    assert_eq!(
        cursor.seeks, 1,
        "the seek ran {} times: the advance state of the cursor is lost when the seek \
         is repeated after an I/O yield",
        cursor.seeks
    );
}

/// Answers TryAdvance to every seek and yields once in `next`, and counts both.
struct CountingCursor {
    seeks: usize,
    advances: usize,
    index_info: Arc<crate::types::IndexInfo>,
}

impl CountingCursor {
    fn new() -> Self {
        Self {
            seeks: 0,
            advances: 0,
            index_info: Arc::new(crate::types::IndexInfo::default()),
        }
    }
}

impl CursorTrait for CountingCursor {
    fn seek(&mut self, _key: SeekKey<'_>, _op: SeekOp) -> crate::types::IOResultOr<SeekResult> {
        self.seeks += 1;
        Ok(IOResult::Done(SeekResult::TryAdvance))
    }

    fn next(&mut self) -> crate::types::IOResultOr<()> {
        self.advances += 1;
        if self.advances == 1 {
            return Ok(IOResult::IO(crate::types::IOCompletions(
                crate::io::Completion::new_yield(),
            )));
        }
        Ok(IOResult::Done(()))
    }

    fn has_record(&self) -> bool {
        false
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn index_info(&self) -> Option<&Arc<crate::types::IndexInfo>> {
        Some(&self.index_info)
    }

    fn prev(&mut self) -> crate::types::IOResultOr<()> {
        unimplemented!()
    }

    fn last(&mut self) -> crate::types::IOResultOr<()> {
        unimplemented!()
    }

    fn rowid(&mut self) -> crate::types::IOResultOr<Option<i64>> {
        unimplemented!()
    }

    fn record(&mut self) -> crate::types::IOResultOr<Option<&crate::types::ImmutableRecord>> {
        unimplemented!()
    }

    fn seek_unpacked(
        &mut self,
        _registers: &[crate::vdbe::Register],
        _op: SeekOp,
    ) -> crate::types::IOResultOr<SeekResult> {
        unimplemented!()
    }

    fn insert(&mut self, _key: &BTreeKey) -> crate::types::IOResultOr<()> {
        unimplemented!()
    }

    fn delete(&mut self) -> crate::types::IOResultOr<()> {
        unimplemented!()
    }

    fn set_null_flag(&mut self, _flag: bool) {
        unimplemented!()
    }

    fn get_null_flag(&self) -> bool {
        unimplemented!()
    }

    fn exists(&mut self, _key: &Value) -> crate::types::IOResultOr<bool> {
        unimplemented!()
    }

    fn clear_btree(&mut self) -> crate::types::IOResultOr<Option<usize>> {
        unimplemented!()
    }

    fn btree_destroy(&mut self) -> crate::types::IOResultOr<Option<usize>> {
        unimplemented!()
    }

    fn count(&mut self) -> crate::types::IOResultOr<usize> {
        unimplemented!()
    }

    fn root_page(&self) -> i64 {
        unimplemented!()
    }

    fn rewind(&mut self) -> crate::types::IOResultOr<()> {
        unimplemented!()
    }

    fn set_has_record(&mut self, _has_record: bool) {
        unimplemented!()
    }

    fn seek_end(&mut self) -> crate::types::IOResultOr<()> {
        unimplemented!()
    }

    fn seek_to_last(&mut self) -> crate::types::IOResultOr<()> {
        unimplemented!()
    }

    fn invalidate_record(&mut self) {
        unimplemented!()
    }

    fn has_rowid(&self) -> bool {
        unimplemented!()
    }

    fn get_pager(&self) -> Arc<Pager> {
        unimplemented!()
    }

    fn get_skip_advance(&self) -> bool {
        false
    }
}
