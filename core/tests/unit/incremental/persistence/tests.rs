use super::*;
use crate::incremental::operator::{create_dbsp_state_index, DbspStateCursors};
use crate::incremental::yield_test_support::OneShotYieldInjector;
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::storage::btree::{
    BTreeCursor, BTreeWriteYieldPoint, CursorTrait, BTREE_WRITE_YIELD_FAMILY,
};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::util::IOExt;
use crate::{Connection, Database, MemoryIO, SqliteDialect, IO};

fn setup() -> (Arc<Connection>, Arc<crate::Pager>, i64, i64) {
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
    (conn, pager, table_root, index_root)
}

// ~1200-byte cells stay on the leaf, so enough of them overflow the page and trip
// AfterInsertOverflowCellBeforeBalance.
fn page_filling_record(op_id: i64, zset_id: i64, elem_id: i64) -> Vec<Value> {
    vec![
        Value::from_i64(op_id),
        Value::from_i64(zset_id),
        Value::from_i64(elem_id),
        Value::from_slice(&[0xcd_u8; 1200]).unwrap(),
    ]
}

/// If the table insert yields mid-balance, `WriteRow` must re-drive it before
/// advancing; advancing first strands the overflow cell and the row vanishes.
#[test]
fn write_row_completes_yielded_overflowing_table_insert() {
    let (conn, pager, table_root, index_root) = setup();

    let injector = OneShotYieldInjector::new(
        BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
        BTREE_WRITE_YIELD_FAMILY ^ table_root as u64,
    );
    conn.set_yield_injector(Some(injector.clone()));

    let mut table_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
    table_cursor.install_yield_context(&conn);
    let index_def = create_dbsp_state_index(index_root);
    let mut index_cursor =
        BTreeCursor::new_index(pager.clone(), index_root, &index_def, 4).unwrap();
    index_cursor.install_yield_context(&conn);
    let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);

    let (op_id, zset_id) = (1i64, 1i64);
    // rowid == elem_id here: ComputeNewRowId assigns last+1.
    let mut victim_rowid = None;
    for elem_id in 1i64..=200 {
        let index_key = vec![
            Value::from_i64(op_id),
            Value::from_i64(zset_id),
            Value::from_i64(elem_id),
        ];
        let record_values = page_filling_record(op_id, zset_id, elem_id);

        let mut wr = WriteRow::new();
        pager
            .io
            .block(|| wr.write_row(&mut cursors, index_key.clone(), record_values.clone(), 1))
            .unwrap();

        if injector.fired() {
            victim_rowid = Some(elem_id);
            break;
        }
    }
    let victim_rowid =
        victim_rowid.expect("no insert ever overflowed a page; test does not exercise the bug");
    conn.set_yield_injector(None);

    // Fresh cursor: the working one may be parked mid-balance.
    let mut verify_cursor = BTreeCursor::new_table(pager.clone(), table_root, 5);
    let found = pager
        .io
        .block(|| {
            verify_cursor.seek(
                SeekKey::TableRowId(victim_rowid),
                SeekOp::GE { eq_only: true },
            )
        })
        .unwrap();
    assert!(
        matches!(found, SeekResult::Found),
        "table row {victim_rowid} lost: WriteRow advanced past a yielded (mid-balance) insert"
    );
}
