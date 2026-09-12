use crate::SqliteDialect;
use rand::{rng, Rng};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};
use sorted_vec::SortedVec;
use test_log::test;

use super::*;
use crate::{
    io::{Buffer, MemoryIO, OpenFlags, IO},
    mvcc::{
        yield_hooks::YieldPointMarker,
        yield_points::{YieldInjector, YieldPoint},
    },
    schema::IndexColumn,
    storage::{
        database::DatabaseFile, page_cache::PageCache, pager::default_page1,
        sqlite3_ondisk::PageSize,
    },
    types::Text,
    vdbe::Register,
    BufferPool, Completion, Connection, IOContext, StepResult, Wal, WalAutoActions, WalFile,
    WalFileShared,
};
use arc_swap::ArcSwapOption;
use std::{
    mem::transmute,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tempfile::TempDir;

use crate::{
    storage::{
        btree::{compute_free_space, fill_cell_payload, payload_overflow_threshold_max},
        sqlite3_ondisk::{BTreeCell, PageContent, PageType, TableLeafCell},
    },
    types::Value,
    Database, Page, Pager, PlatformIO,
};

use super::{btree_init_page, defragment_page, drop_cell, insert_into_cell};

#[derive(Debug)]
struct TargetedYieldInjector {
    point: YieldPoint,
    selection_key: u64,
    fired: AtomicBool,
}

impl TargetedYieldInjector {
    fn new(point: YieldPoint, selection_key: u64) -> Arc<Self> {
        Arc::new(Self {
            point,
            selection_key,
            fired: AtomicBool::new(false),
        })
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

impl YieldInjector for TargetedYieldInjector {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        point == self.point
            && selection_key == self.selection_key
            && !self.fired.swap(true, Ordering::AcqRel)
    }
}

fn btree_write_selection_key(root_page: i64) -> u64 {
    BTREE_WRITE_YIELD_FAMILY ^ root_page as u64
}

fn query_single_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
        other => panic!("expected a row, got {other:?}"),
    }
}

fn query_single_text(conn: &Arc<Connection>, sql: &str) -> String {
    let mut stmt = conn.prepare(sql).unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => stmt.row().unwrap().get::<String>(0).unwrap(),
        other => panic!("expected a row, got {other:?}"),
    }
}

#[allow(clippy::arc_with_non_send_sync)]
fn get_page(id: usize) -> PageRef {
    let page = Arc::new(Page::new(id as i64));

    {
        let inner = page.get();
        inner.set_buffer(Arc::new(Buffer::new_temporary(4096)));
    }
    page.set_loaded();

    btree_init_page(&page, PageType::TableLeaf, 0, 4096);
    page
}

/// The returned `TempDir` deletes the database directory when it drops, so
/// callers must hold it for as long as they use the database.
#[allow(clippy::arc_with_non_send_sync)]
fn get_database() -> (Arc<Database>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "wal")
            .unwrap();
    }
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db =
        Database::open_file(io.clone(), path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();

    (db, temp_dir)
}

/// Deterministic coverage for the allocation-failure window created by routing
/// overflow-cell payloads through `TursoAllocator`: in
/// `OverwriteCellState::ClearOverflowPagesAndOverwrite`, `drop_cell` mutates the
/// page before `insert_into_cell`, whose overflow-cell branch can fail on
/// allocation. A fault there must not lose the row or corrupt the tree.
///
/// Nightly-only because on stable `crate::alloc::Vec` degrades to the global
/// allocator, so the injected fault can never fire.
#[cfg(all(nightly, feature = "allocation_metric"))]
mod allocation_fault_repro {
    use super::*;
    use crate::alloc::{
        current_allocation_site, AllocError, AllocationSite, ApiAllocator, BTreeAllocationSite,
        Global, Layout, TursoAllocBackend,
    };
    use std::cell::Cell as StdCell;
    use std::ptr::NonNull;
    use test_log::test;

    thread_local! {
        static FAIL_SITE: StdCell<Option<AllocationSite>> = const { StdCell::new(None) };
    }

    struct SiteFaultBackend;

    unsafe impl TursoAllocBackend for SiteFaultBackend {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            let fail = FAIL_SITE.with(|slot| {
                let target = slot.get();
                target.is_some() && target == current_allocation_site()
            });
            if fail {
                return Err(AllocError);
            }
            <Global as ApiAllocator>::allocate(&Global, layout)
        }

        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe { <Global as ApiAllocator>::deallocate(&Global, ptr, layout) }
        }
    }

    static BACKEND: SiteFaultBackend = SiteFaultBackend;

    fn arm_fault(site: BTreeAllocationSite) {
        // The backend passes through to Global unless a fault is armed on this
        // thread, so sharing it process-wide with other tests is safe.
        let _ = unsafe { crate::alloc::set_allocator(&BACKEND) };
        FAIL_SITE.with(|slot| slot.set(Some(AllocationSite::BTree(site))));
    }

    fn disarm_fault() {
        FAIL_SITE.with(|slot| slot.set(None));
    }

    #[test]
    fn overwrite_survives_overflow_cell_allocation_failure() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db =
            Database::open_file(io, "overflow-cell-fault.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        // Row 1 is the overwrite target; the filler rows pack the leaf page so
        // that growing row 1 cannot fit locally and must spill to an overflow
        // cell inside insert_into_cell.
        conn.execute(format!("INSERT INTO t VALUES (1, '{}')", "a".repeat(600)))
            .unwrap();
        for id in 2..=6 {
            conn.execute(format!(
                "INSERT INTO t VALUES ({id}, '{}')",
                "b".repeat(600)
            ))
            .unwrap();
        }
        let rows_before = query_single_i64(&conn, "SELECT count(*) FROM t");
        assert_eq!(rows_before, 6);

        arm_fault(BTreeAllocationSite::OverflowCell);
        let update = conn.execute(format!(
            "UPDATE t SET v = '{}' WHERE id = 1",
            "c".repeat(1400)
        ));
        disarm_fault();
        assert!(
            update.is_err(),
            "update should fail once the overflow-cell allocation is denied"
        );

        // The failed statement must not lose the row or corrupt the tree.
        assert_eq!(query_single_i64(&conn, "SELECT count(*) FROM t"), 6);
        assert_eq!(
            query_single_i64(&conn, "SELECT length(v) FROM t WHERE id = 1"),
            600
        );
        assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
    }

    /// Regression test: the overflow-read epilogue empties the payload held
    /// inside `read_overflow_state` before the fallible record allocations
    /// run. On failure it must clear the state — same invariant the adjacent
    /// corrupt-chain branch upholds — otherwise a later call resumes against
    /// the emptied buffer and silently completes with an empty record.
    #[test]
    fn overflow_read_epilogue_clears_state_on_allocation_failure() {
        let pager = setup_test_env(5);
        let mut cursor = BTreeCursor::new_table(pager.clone(), 1, 5);
        let usable = cursor.usable_space();
        let data_per_page = usable - 4;

        // Re-purpose pages 4 and 5 as a valid 2-page overflow chain: 4 -> 5 -> end.
        let load_and_fill = |id: i64, next: u32, fill: u8| {
            let (page, c) = cursor.read_page_blocking(id).unwrap();
            if let Some(c) = c {
                pager.io.wait_for_completion(c).unwrap();
            }
            while page.is_locked() {
                pager.io.step().unwrap();
            }
            let buf = page.get_contents().as_ptr();
            buf[0..4].copy_from_slice(&next.to_be_bytes());
            buf[4..usable].fill(fill);
        };
        load_and_fill(4, 5, b'A');
        load_and_fill(5, 0, b'B');

        let payload_size = (2 * data_per_page) as u64;
        let cursor_pager = cursor.pager.clone();

        // Fail the epilogue's record allocation, which runs after the chain
        // has been fully read and the accumulated payload was already moved
        // out of `read_overflow_state`.
        arm_fault(BTreeAllocationSite::RecordPayload);
        run_until_done(
            || cursor.process_overflow_read(b"", 4, payload_size),
            &cursor_pager,
        )
        .expect_err("record allocation failure should surface");
        disarm_fault();

        assert!(
            cursor.read_overflow_state.is_none(),
            "failed overflow read left resumable state behind"
        );

        // A retry must rebuild the record from scratch and see the full chain.
        run_until_done(
            || cursor.process_overflow_read(b"", 4, payload_size),
            &cursor_pager,
        )
        .unwrap();
        let rec_slot = cursor.get_immutable_record_or_create().unwrap();
        let bytes = rec_slot.as_ref().unwrap().get_payload();
        assert_eq!(bytes.len(), 2 * data_per_page);
        assert!(bytes[..data_per_page].iter().all(|&b| b == b'A'));
        assert!(bytes[data_per_page..].iter().all(|&b| b == b'B'));
    }
}

#[cfg(nightly)]
#[test]
fn btree_state_buffers_use_turso_allocator() {
    fn assert_alloc_vec<T>(_: &crate::alloc::Vec<T>) {}
    fn assert_cursor_buffers(cursor: &BTreeCursor) {
        assert_alloc_vec(&cursor.reusable_cell_payload);
        assert_alloc_vec(&cursor.blob_cache.overflow_pages);
    }
    fn assert_overflow_cell_buffers(page: &crate::storage::pager::PageInner, cell: &OverflowCell) {
        fn assert_alloc_payload(_: &Pin<crate::alloc::Vec<u8>>) {}

        assert_alloc_vec(&page.overflow_cells);
        assert_alloc_payload(&cell.payload);
    }

    let balance = BalanceState::default();
    assert_alloc_vec(&balance.reusable_divider_buffers[0]);
    assert_alloc_vec(&balance.reusable_cell_payloads);
    let integrity_check = IntegrityCheckState::new(0);
    assert_alloc_vec(&integrity_check.page_stack);
    let op_integrity_check = crate::vdbe::execute::OpIntegrityCheckState::CheckingBTreeStructure {
        errors: crate::alloc::vec![],
        current_root_idx: 0,
        current_dropped_idx: 0,
        state: IntegrityCheckState::new(0),
    };
    let crate::vdbe::execute::OpIntegrityCheckState::CheckingBTreeStructure { errors, .. } =
        op_integrity_check
    else {
        unreachable!()
    };
    assert_alloc_vec(&errors);
    let _ = assert_cursor_buffers;
    let _ = assert_overflow_cell_buffers;
}

#[test]
fn wal_reuses_freelist_leaf_after_abandoned_overflowing_insert() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db =
        Database::open_file(io, "freelist-stale-overflow.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute(
        "CREATE TABLE red_rain_308 (
                slow_desk_238 INTEGER NOT NULL,
                quiet_tree_398 TEXT NOT NULL,
                smart_dog_422 REAL UNIQUE,
                new_hill_140 TEXT,
                light_door_957 REAL,
                wild_dog_803 TEXT NOT NULL,
                new_flower_28 BLOB,
                old_fish_931 TEXT PRIMARY KEY
            )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE full_fish_194 (
                brave_star_722 TEXT PRIMARY KEY,
                hot_star_957 NUMERIC,
                empty_chair_834 REAL
            )",
    )
    .unwrap();

    conn.execute(
        "INSERT INTO red_rain_308 (
                slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                light_door_957, wild_dog_803, new_flower_28, old_fish_931
            ) VALUES (
                1, 'seed', 0.24, 'seed', 1.0, 'seed', zeroblob(3600), 'existing_unique'
            )",
    )
    .unwrap();
    for id in 2..260 {
        conn.execute(format!(
            "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                    1.5, 'wild_dog_{id}', zeroblob(3600), 'seed_{id}'
                )"
        ))
        .unwrap();
    }

    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM red_rain_308 WHERE old_fish_931 = 'seed_2'")
        .unwrap();
    conn.execute("SAVEPOINT sp_91").unwrap();
    conn.execute(
        "INSERT INTO full_fish_194 (brave_star_722, hot_star_957, empty_chair_834)
             VALUES ('sweet_fish_597', 328, 7.71)",
    )
    .unwrap();

    let red_rain_root_page = query_single_i64(
        &conn,
        "SELECT rootpage FROM sqlite_schema
             WHERE type = 'table' AND name = 'red_rain_308'",
    );
    let injector = TargetedYieldInjector::new(
        BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
        btree_write_selection_key(red_rain_root_page),
    );
    conn.set_yield_injector(Some(injector.clone()));
    let mut abandoned = conn
        .prepare(
            "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    926, 'dry_hill_411', 9.24, 'blue_hill_65',
                    7.50, 'happy_moon_474', zeroblob(7200), 'smart_rock_113'
                )",
        )
        .unwrap();
    assert!(matches!(abandoned.step().unwrap(), StepResult::Yield));
    assert!(injector.fired());
    abandoned.reset().unwrap();
    drop(abandoned);
    conn.set_yield_injector(None);

    conn.execute("DELETE FROM red_rain_308 WHERE slow_desk_238 BETWEEN 2 AND 240")
        .unwrap();
    conn.execute("RELEASE sp_91").unwrap();

    assert!(conn
        .execute(
            "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    927, 'dry_hill_412', 0.24, 'blue_hill_66',
                    7.50, 'happy_moon_475', x'736f66745f73746f6e655f313732', 'smart_rock_114'
                )"
        )
        .is_err());

    for id in 300..420 {
        conn.execute(format!(
            "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                    1.5, 'wild_dog_{id}', zeroblob(3600), 'reuse_{id}'
                )"
        ))
        .unwrap();
    }

    conn.execute("ROLLBACK").unwrap();
    assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
}

#[test]
fn wal_overflowing_insert_resumes_after_yield_before_balance() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, "resume-overflow-yield.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute(
        "CREATE TABLE red_rain_308 (
                slow_desk_238 INTEGER NOT NULL,
                quiet_tree_398 TEXT NOT NULL,
                smart_dog_422 REAL UNIQUE,
                new_hill_140 TEXT,
                light_door_957 REAL,
                wild_dog_803 TEXT NOT NULL,
                new_flower_28 BLOB,
                old_fish_931 TEXT PRIMARY KEY
            )",
    )
    .unwrap();

    for id in 1..260 {
        conn.execute(format!(
            "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                    1.5, 'wild_dog_{id}', zeroblob(3600), 'seed_{id}'
                )"
        ))
        .unwrap();
    }

    let red_rain_root_page = query_single_i64(
        &conn,
        "SELECT rootpage FROM sqlite_schema
             WHERE type = 'table' AND name = 'red_rain_308'",
    );
    let injector = TargetedYieldInjector::new(
        BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
        btree_write_selection_key(red_rain_root_page),
    );
    conn.set_yield_injector(Some(injector.clone()));
    let mut stmt = conn
        .prepare(
            "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    926, 'dry_hill_411', 9.24, 'blue_hill_65',
                    7.50, 'happy_moon_474', zeroblob(7200), 'smart_rock_113'
                )",
        )
        .unwrap();

    assert!(matches!(stmt.step().unwrap(), StepResult::Yield));
    assert!(injector.fired());
    assert!(matches!(stmt.step().unwrap(), StepResult::Done));
    drop(stmt);
    conn.set_yield_injector(None);

    assert_eq!(
        query_single_i64(
            &conn,
            "SELECT COUNT(*) FROM red_rain_308 WHERE old_fish_931 = 'smart_rock_113'",
        ),
        1
    );
    assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
}

fn ensure_cell(page: &mut PageContent, cell_idx: usize, payload: &[u8]) {
    let cell = page.cell_get_raw_region(cell_idx, 4096).unwrap();
    tracing::trace!("cell idx={} start={} len={}", cell_idx, cell.0, cell.1);
    let buf = &page.as_ptr()[cell.0..cell.0 + cell.1];
    assert_eq!(buf.len(), payload.len());
    assert_eq!(buf, payload);
}

fn add_record(
    id: usize,
    pos: usize,
    page: PageRef,
    record: ImmutableRecord,
    conn: &Arc<Connection>,
) -> crate::alloc::Vec<u8> {
    let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
    let mut fill_cell_payload_state = FillCellPayloadState::Start;
    run_until_done(
        || {
            fill_cell_payload(
                &PinGuard::new(page.clone()),
                Some(id as i64),
                &mut payload,
                pos,
                &record,
                4096,
                &conn.pager.load(),
                &mut fill_cell_payload_state,
            )
        },
        &conn.pager.load().clone(),
    )
    .unwrap();
    insert_into_cell(page.get_contents(), &payload, pos, 4096).unwrap();
    payload
}

fn insert_record(
    cursor: &mut BTreeCursor,
    pager: &Arc<Pager>,
    rowid: i64,
    val: Value,
) -> Result<(), LimboError> {
    let regs = &[Register::Value(val)];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

    run_until_done(
        || {
            let key = SeekKey::TableRowId(rowid);
            cursor.seek(key, SeekOp::GE { eq_only: true })
        },
        pager.deref(),
    )?;
    run_until_done(
        || cursor.insert(&BTreeKey::new_table_rowid(rowid, Some(&record))),
        pager.deref(),
    )?;
    Ok(())
}

fn assert_btree_empty(cursor: &mut BTreeCursor, pager: &Pager) -> Result<()> {
    let _c = cursor.move_to_root()?;
    run_until_done(|| cursor.next(), pager)?;
    let empty = !cursor.has_record;
    assert!(empty, "expected B-tree to be empty");
    Ok(())
}

#[test]
fn test_insert_cell() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();
    let page = get_page(2);

    let header_size = 8;
    let regs = &[Register::Value(Value::from_i64(1))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let payload = add_record(1, 0, page.clone(), record, &conn);
    let page_contents = page.get_contents();
    assert_eq!(page_contents.cell_count(), 1);
    let free = compute_free_space(page_contents, 4096).unwrap();
    assert_eq!(free, 4096 - payload.len() - 2 - header_size);

    let cell_idx = 0;
    ensure_cell(page_contents, cell_idx, &payload);
}

struct Cell {
    pos: usize,
    payload: crate::alloc::Vec<u8>,
}

#[test]
fn test_drop_1() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let header_size = 8;

    let mut total_size = 0;
    let mut cells = Vec::new();
    let usable_space = 4096;
    for i in 0..3 {
        let regs = &[Register::Value(Value::from_i64(i as i64))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(i, i, page.clone(), record, &conn);
        assert_eq!(page_contents.cell_count(), i + 1);
        let free = compute_free_space(page_contents, usable_space).unwrap();
        total_size += payload.len() + 2;
        assert_eq!(free, 4096 - total_size - header_size);
        cells.push(Cell { pos: i, payload });
    }

    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }
    cells.remove(1);
    drop_cell(page_contents, 1, usable_space).unwrap();

    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }
}

fn validate_btree(pager: Arc<Pager>, page_idx: i64) -> (usize, bool) {
    let num_columns = 5;
    let cursor = BTreeCursor::new_table(pager.clone(), page_idx, num_columns);
    let (page, _c) = cursor.read_page_blocking(page_idx).unwrap();
    while page.is_locked() {
        pager.io.step().unwrap();
    }

    // Pin page in order to not drop it in between
    page.set_dirty();
    let contents = page.get_contents();
    let mut previous_key = None;
    let mut valid = true;
    let mut depth = None;
    debug_validate_cells!(contents, pager.usable_space());
    let mut child_pages = Vec::new();
    for cell_idx in 0..contents.cell_count() {
        let cell = contents.cell_get(cell_idx, cursor.usable_space()).unwrap();
        let current_depth = match cell {
            BTreeCell::TableLeafCell(..) => 1,
            BTreeCell::TableInteriorCell(TableInteriorCell {
                left_child_page, ..
            }) => {
                let (child_page, _c) = cursor.read_page_blocking(left_child_page as i64).unwrap();
                while child_page.is_locked() {
                    pager.io.step().unwrap();
                }
                child_pages.push(child_page);
                if left_child_page == page.get().id() as u32 {
                    valid = false;
                    tracing::error!("left child page is the same as parent {}", left_child_page);
                    continue;
                }
                let (child_depth, child_valid) =
                    validate_btree(pager.clone(), left_child_page as i64);
                valid &= child_valid;
                child_depth
            }
            _ => panic!("unsupported btree cell: {cell:?}"),
        };
        if current_depth >= 100 {
            tracing::error!("depth is too big");
            page.clear_dirty();
            return (100, false);
        }
        depth = Some(depth.unwrap_or(current_depth + 1));
        if depth != Some(current_depth + 1) {
            tracing::error!("depth is different for child of page {}", page_idx);
            valid = false;
        }
        match cell {
            BTreeCell::TableInteriorCell(TableInteriorCell { rowid, .. })
            | BTreeCell::TableLeafCell(TableLeafCell { rowid, .. }) => {
                if previous_key.is_some() && previous_key.unwrap() >= rowid {
                    tracing::error!(
                        "keys are in bad order: prev={:?}, current={}",
                        previous_key,
                        rowid
                    );
                    valid = false;
                }
                previous_key = Some(rowid);
            }
            _ => panic!("unsupported btree cell: {cell:?}"),
        }
    }
    if let Some(right) = contents.rightmost_pointer().ok().flatten() {
        let (right_depth, right_valid) = validate_btree(pager.clone(), right as i64);
        valid &= right_valid;
        depth = Some(depth.unwrap_or(right_depth + 1));
        if depth != Some(right_depth + 1) {
            tracing::error!("depth is different for child of page {}", page_idx);
            valid = false;
        }
    }
    let first_page_type = child_pages.first_mut().map(|p| {
        if !p.is_loaded() {
            let (new_page, _c) = pager
                .io
                .block(|| pager.read_page(p.get().id() as i64))
                .unwrap();
            *p = new_page;
        }
        while p.is_locked() {
            pager.io.step().unwrap();
        }
        p.get_contents().page_type().ok()
    });
    if let Some(child_type) = first_page_type {
        for page in child_pages.iter_mut().skip(1) {
            if !page.is_loaded() {
                let (new_page, _c) = pager
                    .io
                    .block(|| pager.read_page(page.get().id() as i64))
                    .unwrap();
                *page = new_page;
            }
            while page.is_locked() {
                pager.io.step().unwrap();
            }
            if page.get_contents().page_type().ok() != child_type {
                tracing::error!("child pages have different types");
                valid = false;
            }
        }
    }
    if contents.rightmost_pointer().ok().flatten().is_none() && contents.cell_count() == 0 {
        valid = false;
    }
    page.clear_dirty();
    (depth.unwrap(), valid)
}

fn format_btree(pager: Arc<Pager>, page_idx: i64, depth: usize) -> String {
    let num_columns = 5;

    let cursor = BTreeCursor::new_table(pager.clone(), page_idx, num_columns);
    let (page, _c) = cursor.read_page_blocking(page_idx).unwrap();
    while page.is_locked() {
        pager.io.step().unwrap();
    }

    // Pin page in order to not drop it in between loading of different pages. If not contents will be a dangling reference.
    page.set_dirty();
    let contents = page.get_contents();
    let mut current = Vec::new();
    let mut child = Vec::new();
    for cell_idx in 0..contents.cell_count() {
        let cell = contents.cell_get(cell_idx, cursor.usable_space()).unwrap();
        match cell {
            BTreeCell::TableInteriorCell(cell) => {
                current.push(format!(
                    "node[rowid:{}, ptr(<=):{}]",
                    cell.rowid, cell.left_child_page
                ));
                child.push(format_btree(
                    pager.clone(),
                    cell.left_child_page as i64,
                    depth + 2,
                ));
            }
            BTreeCell::TableLeafCell(cell) => {
                current.push(format!(
                    "leaf[rowid:{}, len(payload):{}, overflow:{}]",
                    cell.rowid,
                    cell.payload.len(),
                    cell.first_overflow_page.is_some()
                ));
            }
            _ => panic!("unsupported btree cell: {cell:?}"),
        }
    }
    if let Some(rightmost) = contents.rightmost_pointer().ok().flatten() {
        child.push(format_btree(pager, rightmost as i64, depth + 2));
    }
    let current = format!(
        "{}-page:{}, ptr(right):{:?}\n{}+cells:{}",
        " ".repeat(depth),
        page_idx,
        contents.rightmost_pointer().ok().flatten(),
        " ".repeat(depth),
        current.join(", ")
    );
    page.clear_dirty();
    if child.is_empty() {
        current
    } else {
        current + "\n" + &child.join("\n")
    }
}

fn empty_btree() -> (Arc<Pager>, i64, Arc<Database>, Arc<Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let pager = conn.pager.load().clone();

    // FIXME: handle page cache is full

    // force allocate page1 with a transaction
    pager.begin_read_tx().unwrap();
    run_until_done(
        || pager.begin_write_tx(WalAutoActions::all_enabled()),
        &pager,
    )
    .unwrap();
    run_until_done(
        || pager.commit_tx(&conn, conn.get_sync_mode(), true),
        &pager,
    )
    .unwrap();

    let page2 = run_until_done(|| pager.allocate_page(), &pager).unwrap();
    btree_init_page(&page2, PageType::TableLeaf, 0, pager.usable_space());
    (pager, page2.get().id() as i64, db, conn)
}

#[test]
fn btree_with_virtual_page_1() -> Result<()> {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let pager = conn.pager.load().clone();

    let mut cursor = BTreeCursor::new(pager, 1, 5);
    let result = cursor.rewind()?;
    assert!(matches!(result, IOResult::Done(_)));
    let result = cursor.next()?;
    assert!(matches!(result, IOResult::Done(_)));
    assert!(!cursor.has_record);
    let result = cursor.record()?;
    assert!(matches!(result, IOResult::Done(record) if record.is_none()));
    Ok(())
}

#[test]
pub fn btree_test_overflow_pages_are_cleared_on_overwrite() {
    // Create a database with a table
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 5;

    // Get the maximum local payload size for table leaf pages
    let max_local = payload_overflow_threshold_max(PageType::TableLeaf, 4096);
    let usable_size = 4096;

    // Create a payload that is definitely larger than the maximum local size
    // This will force the creation of overflow pages
    let large_payload_size = max_local + usable_size * 2;
    let large_payload = crate::alloc::vec![b'X'; large_payload_size];

    // Create a record with the large payload
    let regs = &[Register::Value(Value::Blob(large_payload))];
    let large_record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

    // Create cursor for the table
    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
    let cursor = &mut cursor;

    let initial_pagecount = pager
        .io
        .block(|| pager.with_header(|header| header.database_size.get()))
        .unwrap();
    assert_eq!(
        initial_pagecount, 2,
        "Page count should be 2 after initial insert, was {initial_pagecount}"
    );

    // Insert the large record with rowid 1
    run_until_done(
        || {
            let key = SeekKey::TableRowId(1);
            cursor.seek(key, SeekOp::GE { eq_only: true })
        },
        pager.deref(),
    )
    .unwrap();
    let key = BTreeKey::new_table_rowid(1, Some(&large_record));
    run_until_done(|| cursor.insert(&key), pager.deref()).unwrap();

    // Verify that overflow pages were created by checking freelist count
    // The freelist count should be 0 initially, and after inserting a large record,
    // some pages should be allocated for overflow, but they won't be in freelist yet
    let freelist_after_insert = pager
        .io
        .block(|| pager.with_header(|header| header.freelist_pages.get()))
        .unwrap();
    assert_eq!(
        freelist_after_insert, 0,
        "Freelist count should be 0 after insert, was {freelist_after_insert}"
    );
    let pagecount_after_insert = pager
        .io
        .block(|| pager.with_header(|header| header.database_size.get()))
        .unwrap();
    const EXPECTED_OVERFLOW_PAGES: u32 = 3;
    assert_eq!(
        pagecount_after_insert,
        initial_pagecount + EXPECTED_OVERFLOW_PAGES,
        "Page count should be {} after insert, was {pagecount_after_insert}",
        initial_pagecount + EXPECTED_OVERFLOW_PAGES
    );

    // Create a smaller record to overwrite with
    let small_payload = crate::alloc::vec![b'Y'; 100]; // Much smaller payload
    let regs = &[Register::Value(Value::Blob(small_payload.clone()))];
    let small_record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

    // Seek to the existing record
    run_until_done(
        || {
            let key = SeekKey::TableRowId(1);
            cursor.seek(key, SeekOp::GE { eq_only: true })
        },
        pager.deref(),
    )
    .unwrap();

    // Overwrite the record with the same rowid
    let key = BTreeKey::new_table_rowid(1, Some(&small_record));
    run_until_done(|| cursor.insert(&key), pager.deref()).unwrap();

    // Check that the freelist count has increased, indicating overflow pages were cleared
    let freelist_after_overwrite = pager
        .io
        .block(|| pager.with_header(|header| header.freelist_pages.get()))
        .unwrap();
    assert_eq!(freelist_after_overwrite, EXPECTED_OVERFLOW_PAGES, "Freelist count should be {EXPECTED_OVERFLOW_PAGES} after overwrite, was {freelist_after_overwrite}");

    // Verify the record was actually overwritten by reading it back
    run_until_done(
        || {
            let key = SeekKey::TableRowId(1);
            cursor.seek(key, SeekOp::GE { eq_only: true })
        },
        pager.deref(),
    )
    .unwrap();

    let record = loop {
        match cursor.record().unwrap() {
            IOResult::Done(r) => break r,
            IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
        }
    };
    let record = record.unwrap();

    // The record should now contain the smaller payload
    let record_payload = record.get_payload();
    const RECORD_HEADER_SIZE: usize = 1;
    const ROWID_VARINT_SIZE: usize = 1;
    const ROWID_PAYLOAD_SIZE: usize = 0; // const int 1 doesn't take any space
    const BLOB_PAYLOAD_SIZE: usize = 1; // the size '100 bytes' can be expressed as 1 byte
    assert_eq!(
        record_payload.len(),
        RECORD_HEADER_SIZE
            + ROWID_VARINT_SIZE
            + ROWID_PAYLOAD_SIZE
            + BLOB_PAYLOAD_SIZE
            + small_payload.len(),
        "Record should now contain smaller payload after overwrite"
    );
}

#[test]
#[ignore]
pub fn btree_insert_fuzz_ex() {
    for sequence in [
        &[
            (777548915, 3364),
            (639157228, 3796),
            (709175417, 1214),
            (390824637, 210),
            (906124785, 1481),
            (197677875, 1305),
            (457946262, 3734),
            (956825466, 592),
            (835875722, 1334),
            (649214013, 1250),
            (531143011, 1788),
            (765057993, 2351),
            (510007766, 1349),
            (884516059, 822),
            (81604840, 2545),
        ]
        .as_slice(),
        &[
            (293471650, 2452),
            (163608869, 627),
            (544576229, 464),
            (705823748, 3441),
        ]
        .as_slice(),
        &[
            (987283511, 2924),
            (261851260, 1766),
            (343847101, 1657),
            (315844794, 572),
        ]
        .as_slice(),
        &[
            (987283511, 2924),
            (261851260, 1766),
            (343847101, 1657),
            (315844794, 572),
            (649272840, 1632),
            (723398505, 3140),
            (334416967, 3874),
        ]
        .as_slice(),
    ] {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        for (key, size) in sequence.iter() {
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(*key);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            let regs = &[Register::Value(Value::Blob(crate::alloc::vec![0; *size]))];
            let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            tracing::info!("insert key:{}", key);
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(*key, Some(&value))),
                pager.deref(),
            )
            .unwrap();
            tracing::info!(
                "=========== btree ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
        }
        for (key, _) in sequence.iter() {
            let seek_key = SeekKey::TableRowId(*key);
            assert!(
                matches!(
                    cursor.seek(seek_key, SeekOp::GE { eq_only: true }).unwrap(),
                    IOResult::Done(SeekResult::Found)
                ),
                "key {key} is not found"
            );
        }
    }
}

fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
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

fn btree_insert_fuzz_run(attempts: usize, inserts: usize, size: impl Fn(&mut ChaCha8Rng) -> usize) {
    const VALIDATE_INTERVAL: usize = 1000;
    let do_validate_btree =
        std::env::var("VALIDATE_BTREE").is_ok_and(|v| v.parse().expect("validate should be bool"));
    let (mut rng, seed) = rng_from_time_or_env();
    let mut seen = crate::HashSet::default();
    tracing::info!("super seed: {}", seed);
    let num_columns = 5;

    for _ in 0..attempts {
        let (pager, root_page, _db, conn) = empty_btree();
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let mut keys = SortedVec::new();
        tracing::info!("seed: {seed}");
        for insert_id in 0..inserts {
            let do_validate = do_validate_btree || (insert_id % VALIDATE_INTERVAL == 0);
            pager.begin_read_tx().unwrap();
            run_until_done(
                || pager.begin_write_tx(WalAutoActions::all_enabled()),
                &pager,
            )
            .unwrap();
            let size = size(&mut rng);
            let key = {
                let result;
                loop {
                    let key = (rng.next_u64() % (1 << 30)) as i64;
                    if seen.contains(&key) {
                        continue;
                    } else {
                        seen.insert(key);
                    }
                    result = key;
                    break;
                }
                result
            };
            keys.push(key);
            tracing::info!(
                "INSERT INTO t VALUES ({}, randomblob({})); -- {}",
                key,
                size,
                insert_id
            );
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(key);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            let regs = &[Register::Value(Value::Blob(crate::alloc::vec![0; size]))];
            let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let btree_before = if do_validate {
                format_btree(pager.clone(), root_page, 0)
            } else {
                "".to_string()
            };
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(key, Some(&value))),
                pager.deref(),
            )
            .unwrap();
            pager
                .io
                .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                .unwrap();
            pager.begin_read_tx().unwrap();
            // FIXME: add sorted vector instead, should be okay for small amounts of keys for now :P, too lazy to fix right now
            let _c = cursor.move_to_root().unwrap();
            let mut valid = true;
            if do_validate {
                let _c = cursor.move_to_root().unwrap();
                for key in keys.iter() {
                    tracing::trace!("seeking key: {}", key);
                    run_until_done(|| cursor.next(), pager.deref()).unwrap();
                    let cursor_rowid = run_until_done(|| cursor.rowid(), pager.deref())
                        .unwrap()
                        .unwrap();
                    if *key != cursor_rowid {
                        valid = false;
                        println!("key {key} is not found, got {cursor_rowid}");
                        break;
                    }
                }
            }
            // let's validate btree too so that we undertsand where the btree failed
            if do_validate
                && (!valid || matches!(validate_btree(pager.clone(), root_page), (_, false)))
            {
                let btree_after = format_btree(pager, root_page, 0);
                println!("btree before:\n{btree_before}");
                println!("btree after:\n{btree_after}");
                panic!("invalid btree");
            }
            pager.end_read_tx();
        }
        pager.begin_read_tx().unwrap();
        tracing::info!(
            "=========== btree ===========\n{}\n\n",
            format_btree(pager.clone(), root_page, 0)
        );
        if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
            panic!("invalid btree");
        }
        let _c = cursor.move_to_root().unwrap();
        for key in keys.iter() {
            tracing::trace!("seeking key: {}", key);
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            let cursor_rowid = run_until_done(|| cursor.rowid(), pager.deref())
                .unwrap()
                .unwrap();
            assert_eq!(
                *key, cursor_rowid,
                "key {key} is not found, got {cursor_rowid}"
            );
        }
        pager.end_read_tx();
    }
}

fn btree_index_insert_fuzz_run(attempts: usize, inserts: usize) {
    use crate::storage::pager::CreateBTreeFlags;

    let (mut rng, seed) = if std::env::var("SEED").is_ok() {
        let seed = std::env::var("SEED").unwrap();
        let seed = seed.parse::<u64>().unwrap();
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    } else {
        rng_from_time_or_env()
    };
    let mut seen = crate::HashSet::default();
    tracing::info!("super seed: {}", seed);
    for _ in 0..attempts {
        let (pager, _, _db, conn) = empty_btree();
        let index_root_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;
        let index_def = Index {
            name: "testindex".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many((0..10).map(|i| format!("test{i}"))),
            table_name: "test".to_string(),
            root_page: index_root_page,
            unique: false,
            ephemeral: false,
            has_rowid: false,
            index_method: None,
            on_conflict: None,
        };
        let num_columns = index_def.columns.len();
        let mut cursor =
            BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, num_columns)
                .unwrap();
        let mut keys = SortedVec::new();
        tracing::info!("seed: {seed}");
        for i in 0..inserts {
            pager.begin_read_tx().unwrap();
            pager
                .io
                .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
                .unwrap();
            let key = {
                let result;
                loop {
                    let cols = (0..num_columns)
                        .map(|_| (rng.next_u64() % (1 << 30)) as i64)
                        .collect::<Vec<_>>();
                    if seen.contains(&cols) {
                        continue;
                    } else {
                        seen.insert(cols.clone());
                    }
                    result = cols;
                    break;
                }
                result
            };
            tracing::info!("insert {}/{}: {:?}", i + 1, inserts, key);
            keys.push(key.clone());
            let regs = key
                .iter()
                .map(|col| Register::Value(Value::from_i64(*col)))
                .collect::<Vec<_>>();
            let value = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
            run_until_done(
                || {
                    let record = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
                    let key = SeekKey::IndexKey(record.as_record_ref());
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_index_key(value.as_record_ref())),
                pager.deref(),
            )
            .unwrap();
            let c = cursor.move_to_root().unwrap();
            if let Some(c) = c {
                pager.io.wait_for_completion(c).unwrap();
            }
            pager
                .io
                .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                .unwrap();
        }

        // Check that all keys can be found by seeking
        pager.begin_read_tx().unwrap();
        let _c = cursor.move_to_root().unwrap();
        for (i, key) in keys.iter().enumerate() {
            tracing::info!("seeking key {}/{}: {:?}", i + 1, keys.len(), key);
            let exists = run_until_done(
                || {
                    let regs = key
                        .iter()
                        .map(|col| Register::Value(Value::from_i64(*col)))
                        .collect::<Vec<_>>();
                    cursor.seek(
                        SeekKey::IndexKey(
                            ImmutableRecord::from_registers(&regs, regs.len())
                                .unwrap()
                                .as_record_ref(),
                        ),
                        SeekOp::GE { eq_only: true },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            let mut found = matches!(exists, SeekResult::Found);
            if matches!(exists, SeekResult::TryAdvance) {
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                found = cursor.has_record();
            }
            assert!(found, "key {key:?} is not found");
        }
        // Check that key count is right
        let _c = cursor.move_to_root().unwrap();
        let mut count = 0;
        while {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            cursor.has_record
        } {
            count += 1;
        }
        assert_eq!(
            count,
            keys.len(),
            "key count is not right, got {}, expected {}",
            count,
            keys.len()
        );
        // Check that all keys can be found in-order, by iterating the btree
        let _c = cursor.move_to_root().unwrap();
        let mut prev = None;
        for (i, key) in keys.iter().enumerate() {
            tracing::info!("iterating key {}/{}: {:?}", i + 1, keys.len(), key);
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            let record = loop {
                match cursor.record().unwrap() {
                    IOResult::Done(r) => break r,
                    IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
                }
            };
            let record = record.as_ref().unwrap();
            let cur = record
                .get_values()
                .unwrap()
                .iter()
                .map(|value| value.to_owned().expect(crate::alloc::ALLOC_ERR_MSG))
                .collect::<Vec<_>>();
            if let Some(prev) = prev {
                if prev >= cur {
                    println!("Seed: {seed}");
                }
                assert!(
                    prev < cur,
                    "keys are not in ascending order: {prev:?} < {cur:?}",
                );
            }
            prev = Some(cur);
        }
        pager.end_read_tx();
    }
}

fn btree_index_insert_delete_fuzz_run(
    attempts: usize,
    operations: usize,
    size: impl Fn(&mut ChaCha8Rng) -> usize,
    insert_chance: f64,
) {
    use crate::storage::pager::CreateBTreeFlags;

    let (mut rng, seed) = if std::env::var("SEED").is_ok() {
        let seed = std::env::var("SEED").unwrap();
        let seed = seed.parse::<u64>().unwrap();
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    } else {
        rng_from_time_or_env()
    };
    let mut seen = crate::HashSet::default();
    tracing::info!("super seed: {}", seed);

    for _ in 0..attempts {
        let (pager, _, _db, conn) = empty_btree();
        let index_root_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;
        let index_def = Index {
            name: "testindex".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many(vec!["testcol"]),
            table_name: "test".to_string(),
            root_page: index_root_page,
            unique: false,
            ephemeral: false,
            has_rowid: false,
            index_method: None,
            on_conflict: None,
        };
        let mut cursor =
            BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, 1).unwrap();

        // Track expected keys that should be present in the tree
        let mut expected_keys = Vec::new();

        tracing::info!("seed: {seed}");
        for i in 0..operations {
            let print_progress = i % 100 == 0;
            pager.begin_read_tx().unwrap();

            pager
                .io
                .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
                .unwrap();

            // Decide whether to insert or delete (80% chance of insert)
            let is_insert = rng.next_u64() % 100 < (insert_chance * 100.0) as u64;

            if is_insert {
                // Generate a unique key for insertion
                let key = {
                    let result;
                    loop {
                        let sizeof_blob = size(&mut rng);
                        let blob = (0..sizeof_blob)
                            .map(|_| (rng.next_u64() % 256) as u8)
                            .collect::<Vec<_>>();
                        if seen.contains(&blob) {
                            continue;
                        } else {
                            seen.insert(blob.clone());
                        }
                        result = blob;
                        break;
                    }
                    result
                };

                if print_progress {
                    tracing::info!("insert {}/{}, seed: {seed}", i + 1, operations);
                }
                expected_keys.push(key.clone());

                let regs = vec![Register::Value(
                    Value::from_slice(&key).expect(crate::alloc::ALLOC_ERR_MSG),
                )];
                let value = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();

                let seek_result = run_until_done(
                    || {
                        let record = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
                        let key = SeekKey::IndexKey(record.as_record_ref());
                        cursor.seek(key, SeekOp::GE { eq_only: true })
                    },
                    pager.deref(),
                )
                .unwrap();
                if let SeekResult::TryAdvance = seek_result {
                    run_until_done(|| cursor.next(), pager.deref()).unwrap();
                }
                run_until_done(
                    || cursor.insert(&BTreeKey::new_index_key(value.as_record_ref())),
                    pager.deref(),
                )
                .unwrap();
            } else {
                // Delete a random existing key
                if !expected_keys.is_empty() {
                    let delete_idx = rng.next_u64() as usize % expected_keys.len();
                    let key_to_delete = expected_keys[delete_idx].clone();

                    if print_progress {
                        tracing::info!("delete {}/{}, seed: {seed}", i + 1, operations);
                    }

                    let regs = vec![Register::Value(
                        Value::from_slice(&key_to_delete).expect(crate::alloc::ALLOC_ERR_MSG),
                    )];
                    let record = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();

                    // Seek to the key to delete
                    let seek_result = run_until_done(
                        || {
                            cursor.seek(
                                SeekKey::IndexKey(record.as_record_ref()),
                                SeekOp::GE { eq_only: true },
                            )
                        },
                        pager.deref(),
                    )
                    .unwrap();
                    let mut found = matches!(seek_result, SeekResult::Found);
                    if matches!(seek_result, SeekResult::TryAdvance) {
                        run_until_done(|| cursor.next(), pager.deref()).unwrap();
                        found = cursor.has_record()
                    }
                    assert!(found, "expected key {key_to_delete:?} is not found");

                    // Delete the key
                    run_until_done(|| cursor.delete(), pager.deref()).unwrap();

                    // Remove from expected keys
                    expected_keys.remove(delete_idx);
                }
            }

            let c = cursor.move_to_root().unwrap();
            if let Some(c) = c {
                pager.io.wait_for_completion(c).unwrap();
            }
            pager
                .io
                .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                .unwrap();
        }

        // Final validation
        let mut sorted_keys = expected_keys.clone();
        sorted_keys.sort();
        validate_expected_keys(&pager, &mut cursor, &sorted_keys, seed);

        pager.end_read_tx();
    }
}

fn validate_expected_keys(
    pager: &Arc<Pager>,
    cursor: &mut BTreeCursor,
    expected_keys: &[Vec<u8>],
    seed: u64,
) {
    // Check that all expected keys can be found by seeking
    pager.begin_read_tx().unwrap();
    let _c = cursor.move_to_root().unwrap();
    for (i, key) in expected_keys.iter().enumerate() {
        tracing::info!(
            "validating key {}/{}, seed: {seed}",
            i + 1,
            expected_keys.len()
        );
        let exists = run_until_done(
            || {
                let regs = vec![Register::Value(
                    Value::from_slice(key).expect(crate::alloc::ALLOC_ERR_MSG),
                )];
                cursor.seek(
                    SeekKey::IndexKey(
                        ImmutableRecord::from_registers(&regs, regs.len())
                            .unwrap()
                            .as_record_ref(),
                    ),
                    SeekOp::GE { eq_only: true },
                )
            },
            pager.deref(),
        )
        .unwrap();
        let mut found = matches!(exists, SeekResult::Found);
        if matches!(exists, SeekResult::TryAdvance) {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            found = cursor.has_record();
        }
        assert!(found, "expected key {key:?} is not found");
    }

    // Check key count
    let _c = cursor.move_to_root().unwrap();
    run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
    if !cursor.has_record() {
        panic!("no keys in tree");
    }
    let mut count = 1;
    loop {
        run_until_done(|| cursor.next(), pager.deref()).unwrap();
        if !cursor.has_record() {
            break;
        }
        count += 1;
    }
    assert_eq!(
        count,
        expected_keys.len(),
        "key count is not right, got {}, expected {}, seed: {seed}",
        count,
        expected_keys.len()
    );

    // Check that all keys can be found in-order, by iterating the btree
    let _c = cursor.move_to_root().unwrap();
    for (i, key) in expected_keys.iter().enumerate() {
        run_until_done(|| cursor.next(), pager.deref()).unwrap();
        tracing::info!(
            "iterating key {}/{}, cursor stack cur idx: {:?}, cursor stack depth: {:?}, seed: {seed}",
            i + 1,
            expected_keys.len(),
            cursor.stack.current_cell_index(),
            cursor.stack.current()
        );
        let record = loop {
            match cursor.record().unwrap() {
                IOResult::Done(r) => break r,
                IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
            }
        };
        let record = record.as_ref().unwrap();
        let cur = record.get_value(0).expect("expected at least one column");
        let ValueRef::Blob(ref cur) = cur else {
            panic!("expected blob, got {cur:?}");
        };
        assert_eq!(cur, key, "key {key:?} is not found, seed: {seed}");
    }
    pager.end_read_tx();
}

#[test]
pub fn test_drop_odd() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let header_size = 8;

    let mut total_size = 0;
    let mut cells = Vec::new();
    let usable_space = 4096;
    let total_cells = 10;
    for i in 0..total_cells {
        let regs = &[Register::Value(Value::from_i64(i as i64))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(i, i, page.clone(), record, &conn);
        assert_eq!(page_contents.cell_count(), i + 1);
        let free = compute_free_space(page_contents, usable_space).unwrap();
        total_size += payload.len() + 2;
        assert_eq!(free, 4096 - total_size - header_size);
        cells.push(Cell { pos: i, payload });
    }

    let mut removed = 0;
    let mut new_cells = Vec::new();
    for cell in cells {
        if cell.pos % 2 == 1 {
            drop_cell(page_contents, cell.pos - removed, usable_space).unwrap();
            removed += 1;
        } else {
            new_cells.push(cell);
        }
    }
    let cells = new_cells;
    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }

    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }
}

#[test]
pub fn btree_insert_fuzz_run_equal_size() {
    for size in 1..8 {
        tracing::info!("======= size:{} =======", size);
        btree_insert_fuzz_run(2, 1024, |_| size);
    }
}

#[test]
pub fn btree_index_insert_fuzz_run_equal_size() {
    btree_index_insert_fuzz_run(2, 1024);
}

#[test]
pub fn btree_index_insert_delete_fuzz_run_test() {
    btree_index_insert_delete_fuzz_run(
        2,
        2000,
        |rng| {
            let min: u32 = 4;
            let size = min + rng.next_u32() % (1024 - min);
            size as usize
        },
        0.65,
    );
}

#[test]
pub fn btree_insert_fuzz_run_random() {
    btree_insert_fuzz_run(128, 16, |rng| (rng.next_u32() % 4096) as usize);
}

#[test]
pub fn btree_insert_fuzz_run_small() {
    btree_insert_fuzz_run(1, 100, |rng| (rng.next_u32() % 128) as usize);
}

#[test]
pub fn btree_insert_fuzz_run_big() {
    btree_insert_fuzz_run(64, 32, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
}

#[test]
pub fn btree_insert_fuzz_run_overflow() {
    btree_insert_fuzz_run(64, 32, |rng| (rng.next_u32() % 32 * 1024) as usize);
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_equal_size() {
    for size in 1..8 {
        tracing::info!("======= size:{} =======", size);
        btree_insert_fuzz_run(2, 10_000, |_| size);
    }
}

#[test]
#[ignore]
pub fn fuzz_long_btree_index_insert_fuzz_run_equal_size() {
    btree_index_insert_fuzz_run(2, 10_000);
}

/// Re-insert an already-present index key the way FTS's RowInserter
/// does — seek `GE { eq_only: true }`, advance once on `TryAdvance`,
/// insert at the cursor — after delete churn that leaves stale interior
/// dividers. `insert` only checks the cursor's current cell for an
/// exact-key overwrite, so a caller that skips the advance lands one
/// leaf early and writes a second physical copy of the key (the whopper
/// duplicate-registry-row corruption). The full scan must see every key
/// exactly once.
#[test]
pub fn btree_index_reinsert_after_tryadvance_never_duplicates() {
    use crate::storage::pager::CreateBTreeFlags;
    let (mut rng, seed) = rng_from_time_or_env();
    tracing::info!("seed: {seed}");
    for attempt in 0..8 {
        let (pager, _, _db, conn) = empty_btree();
        let index_root_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;
        let index_def = Index {
            name: "testindex".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many(vec!["testcol"]),
            table_name: "test".to_string(),
            root_page: index_root_page,
            unique: false,
            ephemeral: false,
            has_rowid: false,
            index_method: None,
            on_conflict: None,
        };
        let mut cursor =
            BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, 1).unwrap();

        let key_bytes = |i: u64| -> Vec<u8> {
            let mut k = vec![0u8; 128];
            k[..8].copy_from_slice(&i.to_be_bytes());
            k
        };
        let record_for = |k: &[u8]| {
            let regs = vec![Register::Value(Value::from_slice(k).unwrap())];
            ImmutableRecord::from_registers(&regs, regs.len()).unwrap()
        };

        let mut live: Vec<u64> = Vec::new();
        pager.begin_read_tx().unwrap();
        pager
            .io
            .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
            .unwrap();
        for i in 0..600u64 {
            let record = record_for(&key_bytes(i));
            run_until_done(
                || {
                    cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_index_key(record.as_record_ref())),
                pager.deref(),
            )
            .unwrap();
            live.push(i);
        }
        // Delete a random half to churn interior dividers.
        for _ in 0..300 {
            let idx = rng.next_u64() as usize % live.len();
            let victim = live.swap_remove(idx);
            let record = record_for(&key_bytes(victim));
            let seek_result = run_until_done(
                || {
                    cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: true },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            if matches!(seek_result, SeekResult::TryAdvance) {
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
            }
            run_until_done(|| cursor.delete(), pager.deref()).unwrap();
        }
        // Re-insert EVERY surviving key with RowInserter's protocol:
        // seek GE eq_only, advance once on TryAdvance, insert.
        let mut tryadvance_reinserts = 0usize;
        for &survivor in &live {
            let record = record_for(&key_bytes(survivor));
            let seek_result = run_until_done(
                || {
                    cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: true },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            if matches!(seek_result, SeekResult::TryAdvance) {
                tryadvance_reinserts += 1;
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
            }
            run_until_done(
                || cursor.insert(&BTreeKey::new_index_key(record.as_record_ref())),
                pager.deref(),
            )
            .unwrap();
        }
        let c = cursor.move_to_root().unwrap();
        if let Some(c) = c {
            pager.io.wait_for_completion(c).unwrap();
        }
        pager
            .io
            .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
            .unwrap();

        // Full scan: every key must appear exactly once.
        pager.begin_read_tx().unwrap();
        let _c = cursor.move_to_root().unwrap();
        run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
        let mut seen: Vec<Vec<u8>> = Vec::new();
        while cursor.has_record() {
            let bytes = loop {
                match cursor.record().unwrap() {
                    IOResult::Done(record) => {
                        let record = record.unwrap();
                        break record
                            .get_value_opt(0)
                            .and_then(|v| match v {
                                crate::types::ValueRef::Blob(b) => Some(b.to_vec()),
                                _ => None,
                            })
                            .unwrap();
                    }
                    IOResult::IO(io) => {
                        while !io.finished() {
                            pager.io.step().unwrap();
                        }
                    }
                }
            };
            seen.push(bytes);
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
        }
        pager.end_read_tx();
        let total = seen.len();
        seen.dedup();
        assert_eq!(
            total,
            seen.len(),
            "attempt {attempt}: duplicate index keys after RowInserter-style \
                 re-inserts (seed {seed}, {tryadvance_reinserts} TryAdvance re-inserts)"
        );
        assert_eq!(total, live.len(), "attempt {attempt}: scan count mismatch");
        tracing::info!(
            "attempt {attempt}: {} keys, {} TryAdvance re-inserts, no duplicates",
            total,
            tryadvance_reinserts
        );
    }
}

#[test]
#[ignore]
pub fn fuzz_long_btree_index_insert_delete_fuzz_run() {
    btree_index_insert_delete_fuzz_run(
        2,
        10000,
        |rng| {
            let min: u32 = 4;
            let size = min + rng.next_u32() % (1024 - min);
            size as usize
        },
        0.65,
    );
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_random() {
    btree_insert_fuzz_run(2, 10_000, |rng| (rng.next_u32() % 4096) as usize);
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_small() {
    btree_insert_fuzz_run(2, 10_000, |rng| (rng.next_u32() % 128) as usize);
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_big() {
    btree_insert_fuzz_run(2, 10_000, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
}

#[test]
#[ignore]
pub fn fuzz_long_btree_insert_fuzz_run_overflow() {
    btree_insert_fuzz_run(2, 5_000, |rng| (rng.next_u32() % 32 * 1024) as usize);
}

#[allow(clippy::arc_with_non_send_sync)]
fn setup_test_env(database_size: u32) -> Arc<Pager> {
    let page_size = 512;

    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let buffer_pool = BufferPool::begin_init(&io, page_size * 128);

    let db_file = Arc::new(DatabaseFile::new(
        io.open_file(":memory:", OpenFlags::Create, false).unwrap(),
    ));

    let wal_file = io.open_file("test.wal", OpenFlags::Create, false).unwrap();
    let wal_shared = WalFileShared::new_shared(wal_file).unwrap();
    let last_checksum_and_max_frame = wal_shared.read().last_checksum_and_max_frame();
    let wal: Arc<dyn Wal> = Arc::new(WalFile::new(
        io.clone(),
        wal_shared,
        last_checksum_and_max_frame,
        buffer_pool.clone(),
    ));

    // For new empty databases, init_page_1 must be Some(page) so allocate_page1() can be called
    let init_page_1 = Arc::new(ArcSwapOption::new(Some(default_page1(None))));
    let pager = Arc::new(
        Pager::new(
            db_file,
            Some(wal),
            io,
            PageCache::new(10),
            buffer_pool,
            Arc::new(crate::sync::Mutex::new(())),
            init_page_1,
        )
        .unwrap(),
    );

    pager.io.step().unwrap();

    let _ = run_until_done(|| pager.allocate_page1(), &pager);
    for _ in 0..(database_size - 1) {
        let _res = pager.allocate_page().unwrap();
    }

    pager
        .io
        .block(|| {
            pager.with_header_mut(|header| {
                header.page_size = PageSize::new(page_size as u32).unwrap()
            })
        })
        .unwrap();

    pager
}

#[test]
pub fn test_clear_overflow_pages() -> Result<()> {
    let pager = setup_test_env(5);
    let num_columns = 5;

    let mut cursor = BTreeCursor::new_table(pager.clone(), 1, num_columns);

    let max_local = payload_overflow_threshold_max(PageType::TableLeaf, 4096);
    let usable_size = cursor.usable_space();

    // Create a large payload that will definitely trigger overflow
    let large_payload = vec![b'A'; max_local + usable_size];

    // Setup overflow pages (2, 3, 4) with linking
    let mut current_page = 2_usize;
    while current_page <= 4 {
        #[allow(clippy::arc_with_non_send_sync)]
        let buf = Arc::new(Buffer::new_temporary(
            pager
                .io
                .block(|| pager.with_header(|header| header.page_size))?
                .get() as usize,
        ));
        let _buf = buf.clone();
        let c = Completion::new_write(move |_| {
            let _ = _buf.clone();
        });
        let _c = pager
            .db_file
            .write_page(current_page, buf.clone(), &IOContext::default(), c)?;
        pager.io.step()?;

        let (page, _c) = cursor.read_page_blocking(current_page as i64)?;
        while page.is_locked() {
            cursor.pager.io.step()?;
        }

        {
            let contents = page.get_contents();

            let next_page = if current_page < 4 {
                current_page + 1
            } else {
                0
            };
            contents.write_u32_no_offset(0, next_page as u32); // Write pointer to next overflow page

            let buf = contents.as_ptr();
            buf[4..].fill(b'A');
        }

        current_page += 1;
    }
    pager.io.step()?;

    // Create leaf cell pointing to start of overflow chain
    let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
        rowid: 1,
        payload: unsafe { transmute::<&[u8], &'static [u8]>(large_payload.as_slice()) },
        first_overflow_page: Some(2), // Point to first overflow page
        payload_size: large_payload.len() as u64,
    });

    let initial_freelist_pages = pager
        .io
        .block(|| pager.with_header(|header| header.freelist_pages))?
        .get();
    // Clear overflow pages
    pager.io.block(|| cursor.clear_overflow_pages(&leaf_cell))?;
    let (freelist_pages, freelist_trunk_page) = pager
        .io
        .block(|| {
            pager.with_header(|header| {
                (
                    header.freelist_pages.get(),
                    header.freelist_trunk_page.get(),
                )
            })
        })
        .unwrap();

    // Verify proper number of pages were added to freelist
    assert_eq!(
        freelist_pages,
        initial_freelist_pages + 3,
        "Expected 3 pages to be added to freelist"
    );

    // If this is first trunk page
    let trunk_page_id = freelist_trunk_page;
    if trunk_page_id > 0 {
        // Verify trunk page structure
        let (trunk_page, _c) = cursor.read_page_blocking(trunk_page_id as i64)?;
        let contents = trunk_page.get_contents();
        // Read number of leaf pages in trunk
        let n_leaf = contents.read_u32_no_offset(4);
        assert!(n_leaf > 0, "Trunk page should have leaf entries");

        for i in 0..n_leaf {
            let leaf_page_id = contents.read_u32_no_offset(8 + (i as usize * 4));
            assert!(
                (2..=4).contains(&leaf_page_id),
                "Leaf page ID {leaf_page_id} should be in range 2-4"
            );
        }
    }

    Ok(())
}

#[test]
fn test_process_overflow_read_inconsistent_chain_returns_corrupt() -> Result<()> {
    let pager = setup_test_env(3);
    let mut cursor = BTreeCursor::new_table(pager.clone(), 1, 5);

    let (overflow_page, c) = cursor.read_page_blocking(2)?;
    if let Some(c) = c {
        pager.io.wait_for_completion(c)?;
    }
    while overflow_page.is_locked() {
        pager.io.step()?;
    }

    let overflow_contents = overflow_page.get_contents();
    overflow_contents.write_u32_no_offset(0, 0);
    overflow_contents.as_ptr()[4..].fill(b'Z');

    let local_payload: &'static [u8] = Box::leak(vec![b'Y'; 32].into_boxed_slice());
    let payload_size = local_payload.len() as u64 + ((cursor.usable_space() - 4) as u64 * 2);
    let cursor_pager = cursor.pager.clone();

    let err = run_until_done(
        || cursor.process_overflow_read(local_payload, 2, payload_size),
        &cursor_pager,
    )
    .expect_err("inconsistent overflow chain should fail with Corrupt");
    assert!(matches!(err, LimboError::Corrupt(_)));
    assert!(cursor.read_overflow_state.is_none());
    Ok(())
}

/// Forces a spill yield from `read_page(next-chain-page)` during
/// `process_overflow_read`'s loop and verifies the resulting record
/// holds the chain's bytes in order with no duplicates and no truncation.
#[test]
fn process_overflow_read_survives_spill_yield_from_next_chain_read() {
    let pager = setup_test_env(5);
    let mut cursor = BTreeCursor::new_table(pager.clone(), 1, 5);
    let usable = cursor.usable_space();
    let data_per_page = usable - 4;

    // Re-purpose pages 4 and 5 as a 2-page overflow chain: 4 -> 5 -> end.
    let load_and_fill = |id: i64, next: u32, fill: u8| {
        let (page, c) = cursor.read_page_blocking(id).unwrap();
        if let Some(c) = c {
            pager.io.wait_for_completion(c).unwrap();
        }
        while page.is_locked() {
            pager.io.step().unwrap();
        }
        let buf = page.get_contents().as_ptr();
        buf[0..4].copy_from_slice(&next.to_be_bytes());
        buf[4..usable].fill(fill);
    };
    load_and_fill(4, 5, b'A');
    load_and_fill(5, 0, b'B');

    // Arm `read_page(5)` to yield IO once
    pager.arm_spill_yield_on_read(5, 0);

    let payload_size = (2 * data_per_page) as u64;
    let cursor_pager = cursor.pager.clone();
    run_until_done(
        || cursor.process_overflow_read(b"", 4, payload_size),
        &cursor_pager,
    )
    .unwrap();

    let rec_slot = cursor.get_immutable_record_or_create().unwrap();
    let bytes = rec_slot.as_ref().unwrap().get_payload();
    assert_eq!(bytes.len(), 2 * data_per_page);
    assert!(bytes[..data_per_page].iter().all(|&b| b == b'A'));
    assert!(bytes[data_per_page..].iter().all(|&b| b == b'B'));
}

/// Forces a real spill yield from the finalization `move_to_root_nonblock`
/// at the end of `count`'s traversal and verifies the returned tally
/// equals the true cell count.
#[test]
fn count_survives_spill_yield_at_finalization() {
    let (pager, root_page, _db, _conn) = empty_btree();
    let n: u16 = 7;

    // `count()` only reads cell_count from the header; no real cells needed.
    let (root, _c) = pager.io.block(|| pager.read_page(root_page)).unwrap();
    while root.is_locked() {
        pager.io.step().unwrap();
    }
    root.get_contents().write_cell_count(n);

    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, 5);

    // `count` calls `move_to_root_nonblock` -> `read_page(root)` twice:
    // once in `Start` to push the root, and once at finalization. Skip
    // the first; yield on the second.
    pager.arm_spill_yield_on_read(root_page, 1);

    let final_count = run_until_done(|| cursor.count(), &pager).unwrap();
    assert_eq!(final_count, n as usize);
}

/// An oversized on-disk cell count must not panic the rowid binary search:
/// indexing the cell-pointer array off the page should surface `Corrupt`.
/// Regression test for https://github.com/tursodatabase/turso/issues/7473.
#[test]
fn table_leaf_oversized_cell_count_reads_corrupt_not_panic() {
    let (pager, root_page, _db, _conn) = empty_btree();

    let (root, _c) = pager.io.block(|| pager.read_page(root_page)).unwrap();
    while root.is_locked() {
        pager.io.step().unwrap();
    }

    // Forge an oversized cell count, as a corrupt file would.
    let contents = root.get_contents();
    contents.write_cell_count(0xFFFF);

    // In range per the forged count, but its array entry lies off the page.
    let oob_idx = pager.usable_space() / CELL_PTR_SIZE_BYTES;
    let result = contents.cell_table_leaf_read_rowid(oob_idx);
    assert!(
        matches!(result, Err(LimboError::Corrupt(_))),
        "expected Corrupt error, got {result:?}"
    );
}

#[test]
pub fn test_clear_overflow_pages_no_overflow() -> Result<()> {
    let pager = setup_test_env(5);
    let num_columns = 5;

    let mut cursor = BTreeCursor::new_table(pager.clone(), 1, num_columns);

    let small_payload = vec![b'A'; 10];

    // Create leaf cell with no overflow pages
    let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
        rowid: 1,
        payload: unsafe { transmute::<&[u8], &'static [u8]>(small_payload.as_slice()) },
        first_overflow_page: None,
        payload_size: small_payload.len() as u64,
    });

    let initial_freelist_pages = pager
        .io
        .block(|| pager.with_header(|header| header.freelist_pages))?
        .get() as usize;

    // Try to clear non-existent overflow pages
    pager.io.block(|| cursor.clear_overflow_pages(&leaf_cell))?;
    let (freelist_pages, freelist_trunk_page) = pager.io.block(|| {
        pager.with_header(|header| {
            (
                header.freelist_pages.get(),
                header.freelist_trunk_page.get(),
            )
        })
    })?;

    // Verify freelist was not modified
    assert_eq!(
        freelist_pages as usize, initial_freelist_pages,
        "Freelist should not change when no overflow pages exist"
    );

    // Verify trunk page wasn't created
    assert_eq!(
        freelist_trunk_page, 0,
        "No trunk page should be created when no overflow pages exist"
    );

    Ok(())
}

#[test]
fn test_btree_destroy() -> Result<()> {
    let initial_size = 1;
    let pager = setup_test_env(initial_size);
    let num_columns = 5;

    let mut cursor = BTreeCursor::new_table(pager.clone(), 2, num_columns);

    // Initialize page 2 as a root page (interior)
    let root_page = run_until_done(
        || cursor.allocate_page(PageType::TableInterior, 0),
        &cursor.pager,
    )?;

    // Allocate two leaf pages
    let page3 = run_until_done(
        || cursor.allocate_page(PageType::TableLeaf, 0),
        &cursor.pager,
    )?;
    let page4 = run_until_done(
        || cursor.allocate_page(PageType::TableLeaf, 0),
        &cursor.pager,
    )?;

    // Configure the root page to point to the two leaf pages
    {
        let contents = root_page.get_contents();

        // Set rightmost pointer to page4
        contents.write_rightmost_ptr(page4.get().id() as u32);

        // Create a cell with pointer to page3
        let cell_content = vec![
            // First 4 bytes: left child pointer (page3)
            (page3.get().id() >> 24) as u8,
            (page3.get().id() >> 16) as u8,
            (page3.get().id() >> 8) as u8,
            page3.get().id() as u8,
            // Next byte: rowid as varint (simple value 100)
            100,
        ];

        // Insert the cell
        insert_into_cell(contents, &cell_content, 0, 512)?;
    }

    // Add a simple record to each leaf page
    for page in [&page3, &page4] {
        let contents = page.get_contents();

        // Simple record with just a rowid and payload
        let record_bytes = vec![
            5,                     // Payload length (varint)
            page.get().id() as u8, // Rowid (varint)
            b'h',
            b'e',
            b'l',
            b'l',
            b'o', // Payload
        ];

        insert_into_cell(contents, &record_bytes, 0, 512)?;
    }

    // Verify structure before destruction
    assert_eq!(
        pager
            .io
            .block(|| pager.with_header(|header| header.database_size))?
            .get(),
        4, // We should have pages 1-4
        "Database should have 4 pages total"
    );

    // Track freelist state before destruction
    let initial_free_pages = pager
        .io
        .block(|| pager.with_header(|header| header.freelist_pages))?
        .get();
    assert_eq!(initial_free_pages, 0, "should start with no free pages");

    run_until_done(|| cursor.btree_destroy(), pager.deref())?;

    let pages_freed = pager
        .io
        .block(|| pager.with_header(|header| header.freelist_pages))?
        .get()
        - initial_free_pages;
    assert_eq!(pages_freed, 3, "should free 3 pages (root + 2 leaves)");

    Ok(())
}

#[test]
pub fn test_clear_btree_with_single_page() -> Result<()> {
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 5;
    let record_count = 10;

    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

    for rowid in 1..=record_count {
        insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
    }

    let page_count = pager
        .io
        .block(|| pager.with_header(|header| header.database_size.get()))?;
    assert_eq!(
        page_count, 2,
        "expected two pages (header + root), got {page_count}"
    );

    run_until_done(|| cursor.clear_btree(), &pager)?;

    assert_btree_empty(&mut cursor, pager.deref())
}

#[test]
pub fn test_clear_btree_with_multiple_pages() -> Result<()> {
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 5;
    let record_count = 1000;

    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

    for rowid in 1..=record_count {
        insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
    }

    // Ensure enough records were created so the tree spans multiple pages.
    let page_count = pager
        .io
        .block(|| pager.with_header(|header| header.database_size.get()))?;
    assert!(
        page_count > 2,
        "expected more pages than just header + root, got {page_count}"
    );

    run_until_done(|| cursor.clear_btree(), &pager)?;

    assert_btree_empty(&mut cursor, pager.deref())
}

#[test]
pub fn test_clear_btree_reinsertion() -> Result<()> {
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 5;
    let record_count = 1000;

    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

    for rowid in 1..=record_count {
        insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
    }

    run_until_done(|| cursor.clear_btree(), &pager)?;

    // Reinsert into cleared B-tree to ensure it’s still functional
    for rowid in 1..=record_count {
        insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
    }

    if let (_, false) = validate_btree(pager.clone(), root_page) {
        panic!("Invalid B-tree after reinsertion");
    }

    let _c = cursor.move_to_root()?;
    for i in 1..=record_count {
        run_until_done(|| cursor.next(), &pager)?;
        let exists = cursor.has_record();
        assert!(exists, "Record {i} not found");

        let record = loop {
            match cursor.record()? {
                IOResult::Done(r) => break r,
                IOResult::IO(io) => io.wait(&*pager.io)?,
            }
        }
        .unwrap();
        let value = record.get_value(0)?;
        assert_eq!(
            value,
            ValueRef::Numeric(Numeric::Integer(i)),
            "Unexpected value for record {i}",
        );
    }

    Ok(())
}

#[test]
pub fn test_clear_btree_multiple_cursors() -> Result<()> {
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 5;
    let record_count = 1000;

    let mut cursor1 = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
    let mut cursor2 = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

    // Use cursor1 to insert records
    for rowid in 1..=record_count {
        insert_record(&mut cursor1, &pager, rowid, Value::from_i64(rowid))?;
    }

    // Use cursor1 to clear the btree
    run_until_done(|| cursor1.clear_btree(), &pager)?;

    // Verify that cursor2 works correctly
    assert_btree_empty(&mut cursor2, pager.deref())?;

    // Insert using cursor2
    insert_record(&mut cursor1, &pager, 1, Value::from_i64(123))?;

    if let (_, false) = validate_btree(pager.clone(), root_page) {
        panic!("Invalid B-tree after insertion");
    }

    let key = Value::from_i64(1);
    let exists = run_until_done(|| cursor2.exists(&key), pager.deref())?;
    assert!(exists, "key not found {key}");

    Ok(())
}

/// Regression test: after clear_btree() on one cursor and invalidate_btree_cache()
/// on a sibling cursor sharing the same btree (e.g. OpenDup), the count cache must
/// be reset. Otherwise count() returns the stale value from before the clear.
///
/// This is the mechanism behind stale partition counts in window functions:
/// ResetSorter calls clear_btree on the main cursor and invalidate_btree_cache on
/// OpenDup cursors. If count_state/count are not reset, the Count instruction on the
/// dup cursor returns the previous partition's row count.
#[test]
pub fn test_clear_btree_resets_count_cache() -> Result<()> {
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 1;

    let mut cursor_main = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
    let mut cursor_dup = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

    // Insert 5 records (simulating partition 'a' with 5 rows)
    for rowid in 1..=5 {
        insert_record(&mut cursor_main, &pager, rowid, Value::from_i64(rowid))?;
    }

    // Count via the dup cursor -- should be 5 and caches the result
    let count1 = run_until_done(|| cursor_dup.count(), pager.deref())?;
    assert_eq!(count1, 5, "first count should be 5");

    // Simulate ResetSorter: clear the btree via the main cursor
    run_until_done(|| cursor_main.clear_btree(), &pager)?;
    // Invalidate sibling cursor's cache (as op_reset_sorter does)
    cursor_dup.invalidate_btree_cache();

    // Insert only 2 records (simulating partition 'b' with 2 rows)
    for rowid in 1..=2 {
        insert_record(&mut cursor_main, &pager, rowid, Value::from_i64(rowid + 10))?;
    }

    // Count via the dup cursor again -- must be 2, not the stale 5
    let count2 = run_until_done(|| cursor_dup.count(), pager.deref())?;
    assert_eq!(
        count2, 2,
        "count after clear + re-insert should be 2, got stale count if cache was not reset"
    );

    Ok(())
}

/// Verify that clear_btree() resets its own count cache, not just sibling cursors.
#[test]
pub fn test_clear_btree_resets_own_count_cache() -> Result<()> {
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 1;

    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

    // Insert 5 records and count
    for rowid in 1..=5 {
        insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
    }
    let count1 = run_until_done(|| cursor.count(), pager.deref())?;
    assert_eq!(count1, 5);

    // Clear and re-insert 3 records
    run_until_done(|| cursor.clear_btree(), &pager)?;
    for rowid in 1..=3 {
        insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid + 10))?;
    }

    // Count should reflect the new 3 records, not the stale 5
    let count2 = run_until_done(|| cursor.count(), pager.deref())?;
    assert_eq!(
        count2, 3,
        "count after clear_btree + re-insert should be 3, not stale 5"
    );

    Ok(())
}

/// Verify that insert() invalidates the count cache so a subsequent count()
/// re-traverses the btree instead of returning the stale cached value.
#[test]
pub fn test_insert_invalidates_count_cache() -> Result<()> {
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 1;

    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

    // Insert 3 records and count
    for rowid in 1..=3 {
        insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
    }
    let count1 = run_until_done(|| cursor.count(), pager.deref())?;
    assert_eq!(count1, 3, "initial count should be 3");

    // Insert 2 more records
    for rowid in 4..=5 {
        insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
    }

    // Count should reflect all 5 records, not the stale 3
    let count2 = run_until_done(|| cursor.count(), pager.deref())?;
    assert_eq!(
        count2, 5,
        "count after additional inserts should be 5, not stale 3"
    );

    Ok(())
}

#[test]
pub fn test_clear_btree_with_overflow_pages() -> Result<()> {
    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 5;
    let record_count = 100;

    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

    let initial_page_count = pager
        .io
        .block(|| pager.with_header(|header| header.database_size.get()))?;

    for rowid in 1..=record_count {
        let large_blob = crate::alloc::vec![b'A'; 8192];
        insert_record(&mut cursor, &pager, rowid, Value::Blob(large_blob))?;
    }

    let page_count_after_inserts = pager
        .io
        .block(|| pager.with_header(|header| header.database_size.get()))?;
    let created_pages = page_count_after_inserts - initial_page_count;
    assert!(
        created_pages > record_count as u32,
        "expected more pages to be created than records, got {created_pages}"
    );

    run_until_done(|| cursor.clear_btree(), &pager)?;

    assert_btree_empty(&mut cursor, pager.deref())
}

#[test]
pub fn test_defragment() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let header_size = 8;

    let mut total_size = 0;
    let mut cells = Vec::new();
    let usable_space = 4096;
    for i in 0..3 {
        let regs = &[Register::Value(Value::from_i64(i as i64))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(i, i, page.clone(), record, &conn);
        assert_eq!(page_contents.cell_count(), i + 1);
        let free = compute_free_space(page_contents, usable_space).unwrap();
        total_size += payload.len() + 2;
        assert_eq!(free, 4096 - total_size - header_size);
        cells.push(Cell { pos: i, payload });
    }

    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }
    cells.remove(1);
    drop_cell(page_contents, 1, usable_space).unwrap();

    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }

    defragment_page(page_contents, usable_space, 4).unwrap();

    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }
}

#[test]
pub fn test_drop_odd_with_defragment() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let header_size = 8;

    let mut total_size = 0;
    let mut cells = Vec::new();
    let usable_space = 4096;
    let total_cells = 10;
    for i in 0..total_cells {
        let regs = &[Register::Value(Value::from_i64(i as i64))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(i, i, page.clone(), record, &conn);
        assert_eq!(page_contents.cell_count(), i + 1);
        let free = compute_free_space(page_contents, usable_space).unwrap();
        total_size += payload.len() + 2;
        assert_eq!(free, 4096 - total_size - header_size);
        cells.push(Cell { pos: i, payload });
    }

    let mut removed = 0;
    let mut new_cells = Vec::new();
    for cell in cells {
        if cell.pos % 2 == 1 {
            drop_cell(page_contents, cell.pos - removed, usable_space).unwrap();
            removed += 1;
        } else {
            new_cells.push(cell);
        }
    }
    let cells = new_cells;
    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }

    defragment_page(page_contents, usable_space, 4).unwrap();

    for (i, cell) in cells.iter().enumerate() {
        ensure_cell(page_contents, i, &cell.payload);
    }
}

#[test]
pub fn test_fuzz_drop_defragment_insert() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let header_size = 8;

    let mut total_size = 0;
    let mut cells = Vec::new();
    let usable_space = 4096;
    let mut i = 100000;
    let seed = rng().random();
    tracing::info!("seed {}", seed);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    while i > 0 {
        i -= 1;
        match rng.next_u64() % 4 {
            0 => {
                // allow appends with extra place to insert
                let cell_idx = rng.next_u64() as usize % (page_contents.cell_count() + 1);
                let free = compute_free_space(page_contents, usable_space).unwrap();
                let regs = &[Register::Value(Value::from_i64(i as i64))];
                let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
                let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
                let mut fill_cell_payload_state = FillCellPayloadState::Start;
                run_until_done(
                    || {
                        fill_cell_payload(
                            &PinGuard::new(page.clone()),
                            Some(i as i64),
                            &mut payload,
                            cell_idx,
                            &record,
                            4096,
                            &conn.pager.load(),
                            &mut fill_cell_payload_state,
                        )
                    },
                    &conn.pager.load().clone(),
                )
                .unwrap();
                if (free as usize) < payload.len() + 2 {
                    // do not try to insert overflow pages because they require balancing
                    continue;
                }
                insert_into_cell(page_contents, &payload, cell_idx, 4096).unwrap();
                assert!(page_contents.overflow_cells.is_empty());
                total_size += payload.len() + 2;
                cells.insert(cell_idx, Cell { pos: i, payload });
            }
            1 => {
                if page_contents.cell_count() == 0 {
                    continue;
                }
                let cell_idx = rng.next_u64() as usize % page_contents.cell_count();
                let (_, len) = page_contents
                    .cell_get_raw_region(cell_idx, usable_space)
                    .unwrap();
                drop_cell(page_contents, cell_idx, usable_space).unwrap();
                total_size -= len + 2;
                cells.remove(cell_idx);
            }
            2 => {
                defragment_page(page_contents, usable_space, 4).unwrap();
            }
            3 => {
                // check cells
                for (i, cell) in cells.iter().enumerate() {
                    ensure_cell(page_contents, i, &cell.payload);
                }
                assert_eq!(page_contents.cell_count(), cells.len());
            }
            _ => unreachable!(),
        }
        let free = compute_free_space(page_contents, usable_space).unwrap();
        assert_eq!(free, 4096 - total_size - header_size);
    }
}

#[test]
pub fn test_fuzz_drop_defragment_insert_issue_1085() {
    // This test is used to demonstrate that issue at https://github.com/tursodatabase/turso/issues/1085
    // is FIXED.
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let header_size = 8;

    let mut total_size = 0;
    let usable_space = 4096;
    let mut i = 1000;
    for seed in [15292777653676891381, 9261043168681395159] {
        tracing::info!("seed {}", seed);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        while i > 0 {
            i -= 1;
            match rng.next_u64() % 3 {
                0 => {
                    // allow appends with extra place to insert
                    let cell_idx = rng.next_u64() as usize % (page_contents.cell_count() + 1);
                    let free = compute_free_space(page_contents, usable_space).unwrap();
                    let regs = &[Register::Value(Value::from_i64(i))];
                    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
                    let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
                    let mut fill_cell_payload_state = FillCellPayloadState::Start;
                    run_until_done(
                        || {
                            fill_cell_payload(
                                &PinGuard::new(page.clone()),
                                Some(i),
                                &mut payload,
                                cell_idx,
                                &record,
                                4096,
                                &conn.pager.load(),
                                &mut fill_cell_payload_state,
                            )
                        },
                        &conn.pager.load().clone(),
                    )
                    .unwrap();
                    if (free as usize) < payload.len() - 2 {
                        // do not try to insert overflow pages because they require balancing
                        continue;
                    }
                    insert_into_cell(page_contents, &payload, cell_idx, 4096).unwrap();
                    assert!(page_contents.overflow_cells.is_empty());
                    total_size += payload.len() + 2;
                }
                1 => {
                    if page_contents.cell_count() == 0 {
                        continue;
                    }
                    let cell_idx = rng.next_u64() as usize % page_contents.cell_count();
                    let (_, len) = page_contents
                        .cell_get_raw_region(cell_idx, usable_space)
                        .unwrap();
                    drop_cell(page_contents, cell_idx, usable_space).unwrap();
                    total_size -= len + 2;
                }
                2 => {
                    defragment_page(page_contents, usable_space, 4).unwrap();
                }
                _ => unreachable!(),
            }
            let free = compute_free_space(page_contents, usable_space).unwrap();
            assert_eq!(free, 4096 - total_size - header_size);
        }
    }
}

// this test will create a tree like this:
// -page:2, ptr(right):4
// +cells:node[rowid:14, ptr(<=):3]
//   -page:3, ptr(right):0
//   +cells:leaf[rowid:11, len(payload):137, overflow:false]
//   -page:4, ptr(right):0
//   +cells:
#[test]
pub fn test_drop_page_in_balancing_issue_1203() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let queries = vec![
"CREATE TABLE lustrous_petit (awesome_nomous TEXT,ambitious_amargi TEXT,fantastic_daniels BLOB,stupendous_highleyman TEXT,relaxed_crane TEXT,elegant_bromma INTEGER,proficient_castro BLOB,ambitious_liman TEXT,responsible_lusbert BLOB);",
"INSERT INTO lustrous_petit VALUES ('funny_sarambi', 'hardworking_naoumov', X'666561726C6573735F68696C6C', 'elegant_iafd', 'rousing_flag', 681399778772406122, X'706572736F6E61626C655F676F6477696E6772696D6D', 'insightful_anonymous', X'706F77657266756C5F726F636861'), ('personable_holmes', 'diligent_pera', X'686F6E6573745F64696D656E73696F6E', 'energetic_raskin', 'gleaming_federasyon', -2778469859573362611, X'656666696369656E745F6769617A', 'sensible_skirda', X'66616E7461737469635F6B656174696E67'), ('inquisitive_baedan', 'brave_sphinx', X'67656E65726F75735F6D6F6E7473656E79', 'inquisitive_syndicate', 'amiable_room', 6954857961525890638, X'7374756E6E696E675F6E6965747A73636865', 'glowing_coordinator', X'64617A7A6C696E675F7365766572696E65'), ('upbeat_foxtale', 'engaging_aktimon', X'63726561746976655F6875746368696E6773', 'ample_locura', 'creative_barrett', 6413352509911171593, X'6772697070696E675F6D696E7969', 'competitive_parissi', X'72656D61726B61626C655F77696E7374616E6C6579');",
"INSERT INTO lustrous_petit VALUES ('ambitious_berry', 'devoted_marshall', X'696E7175697369746976655F6C6172657661', 'flexible_pramen', 'outstanding_stauch', 6936508362673228293, X'6C6F76696E675F6261756572', 'charming_anonymous', X'68617264776F726B696E675F616E6E6973'), ('enchanting_cohen', 'engaging_rubel', X'686F6E6573745F70726F766F63617A696F6E65', 'humorous_robin', 'imaginative_shuzo', 4762266264295288131, X'726F7573696E675F6261796572', 'vivid_bolling', X'6F7267616E697A65645F7275696E73'), ('affectionate_resistance', 'gripping_rustamova', X'6B696E645F6C61726B696E', 'bright_boulanger', 'upbeat_ashirov', -1726815435854320541, X'61646570745F66646361', 'dazzling_tashjian', X'68617264776F726B696E675F6D6F72656C'), ('zestful_ewald', 'favorable_lewis', X'73747570656E646F75735F7368616C6966', 'bright_combustion', 'blithesome_harding', 8408539013935554176, X'62726176655F737079726F706F756C6F75', 'hilarious_finnegan', X'676976696E675F6F7267616E697A696E67'), ('blithesome_picqueray', 'sincere_william', X'636F75726167656F75735F6D69746368656C6C', 'rousing_atan', 'mirthful_katie', -429232313453215091, X'6C6F76656C795F776174616E616265', 'stupendous_mcmillan', X'666F63757365645F6B61666568'), ('incredible_kid', 'friendly_yvetot', X'706572666563745F617A697A', 'helpful_manhattan', 'shining_horrox', -4318061095860308846, X'616D626974696F75735F726F7765', 'twinkling_anarkiya', X'696D6167696E61746976655F73756D6E6572');",
"INSERT INTO lustrous_petit VALUES ('sleek_graeber', 'approachable_ghazzawi', X'62726176655F6865776974747768697465', 'adaptable_zimmer', 'polite_cohn', -5464225138957223865, X'68756D6F726F75735F736E72', 'adaptable_igualada', X'6C6F76656C795F7A686F75'), ('imaginative_rautiainen', 'magnificent_ellul', X'73706C656E6469645F726F6361', 'responsible_brown', 'upbeat_uruguaya', -1185340834321792223, X'616D706C655F6D6470', 'philosophical_kelly', X'676976696E675F6461676865726D6172676F7369616E'), ('blithesome_darkness', 'creative_newell', X'6C757374726F75735F61706174726973', 'engaging_kids', 'charming_wark', -1752453819873942466, X'76697669645F6162657273', 'independent_barricadas', X'676C697374656E696E675F64686F6E6474'), ('productive_chardronnet', 'optimistic_karnage', X'64696C6967656E745F666F72657374', 'engaging_beggar', 'sensible_wolke', 784341549042407442, X'656E676167696E675F6265726B6F7769637A', 'blithesome_zuzenko', X'6E6963655F70726F766F63617A696F6E65');",
"INSERT INTO lustrous_petit VALUES ('shining_sagris', 'considerate_mother', X'6F70656E5F6D696E6465645F72696F74', 'polite_laufer', 'patient_mink', 2240393952789100851, X'636F75726167656F75735F6D636D696C6C616E', 'glowing_robertson', X'68656C7066756C5F73796D6F6E6473'), ('dazzling_glug', 'stupendous_poznan', X'706572736F6E61626C655F6672616E6B73', 'open_minded_ruins', 'qualified_manes', 2937238916206423261, X'696E736967687466756C5F68616B69656C', 'passionate_borl', X'616D6961626C655F6B7570656E647561'), ('wondrous_parry', 'knowledgeable_giovanni', X'6D6F76696E675F77696E6E', 'shimmering_aberlin', 'affectionate_calhoun', 702116954493913499, X'7265736F7572636566756C5F62726F6D6D61', 'propitious_mezzagarcia', X'746563686E6F6C6F676963616C5F6E6973686974616E69');",
"INSERT INTO lustrous_petit VALUES ('kind_room', 'hilarious_crow', X'6F70656E5F6D696E6465645F6B6F74616E7969', 'hardworking_petit', 'adaptable_zarrow', 2491343172109894986, X'70726F647563746976655F646563616C6F677565', 'willing_sindikalis', X'62726561746874616B696E675F6A6F7264616E');",
"INSERT INTO lustrous_petit VALUES ('confident_etrebilal', 'agreeable_shifu', X'726F6D616E7469635F7363687765697A6572', 'loving_debs', 'gripping_spooner', -3136910055229112693, X'677265676172696F75735F736B726F7A6974736B79', 'ample_ontiveros', X'7175616C69666965645F726F6D616E69656E6B6F'), ('competitive_call', 'technological_egoumenides', X'6469706C6F6D617469635F6D6F6E616768616E', 'willing_stew', 'frank_neal', -5973720171570031332, X'6C6F76696E675F6465737461', 'dazzling_gambone', X'70726F647563746976655F6D656E64656C676C6565736F6E'), ('favorable_delesalle', 'sensible_atterbury', X'666169746866756C5F64617861', 'bountiful_aldred', 'marvelous_malgraith', 5330463874397264493, X'706572666563745F7765726265', 'lustrous_anti', X'6C6F79616C5F626F6F6B6368696E'), ('stellar_corlu', 'loyal_espana', X'6D6F76696E675F7A6167', 'efficient_nelson', 'qualified_shepard', 1015518116803600464, X'737061726B6C696E675F76616E6469766572', 'loving_scoffer', X'686F6E6573745F756C72696368'), ('adaptable_taylor', 'shining_yasushi', X'696D6167696E61746976655F776974746967', 'alluring_blackmore', 'zestful_coeurderoy', -7094136731216188999, X'696D6167696E61746976655F757A63617465677569', 'gleaming_hernandez', X'6672616E6B5F646F6D696E69636B'), ('competitive_luis', 'stellar_fredericks', X'616772656561626C655F6D696368656C', 'optimistic_navarro', 'funny_hamilton', 4003895682491323194, X'6F70656E5F6D696E6465645F62656C6D6173', 'incredible_thorndycraft', X'656C6567616E745F746F6C6B69656E'), ('remarkable_parsons', 'sparkling_ulrich', X'737061726B6C696E675F6D6172696E636561', 'technological_leighlais', 'warmhearted_konok', -5789111414354869563, X'676976696E675F68657272696E67', 'adept_dabtara', X'667269656E646C795F72617070');",
"INSERT INTO lustrous_petit VALUES ('hardworking_norberg', 'approachable_winter', X'62726176655F68617474696E6768', 'imaginative_james', 'open_minded_capital', -5950508516718821688, X'6C757374726F75735F72616E7473', 'warmhearted_limanov', X'696E736967687466756C5F646F637472696E65'), ('generous_shatz', 'generous_finley', X'726176697368696E675F6B757A6E6574736F76', 'stunning_arrigoni', 'favorable_volcano', -8442328990977069526, X'6D6972746866756C5F616C7467656C64', 'thoughtful_zurbrugg', X'6D6972746866756C5F6D6F6E726F65'), ('frank_kerr', 'splendid_swain', X'70617373696F6E6174655F6D6470', 'flexible_dubey', 'sensible_tj', 6352949260574274181, X'656666696369656E745F6B656D736B79', 'vibrant_ege', X'736C65656B5F6272696768746F6E'), ('organized_neal', 'glistening_sugar', X'656E676167696E675F6A6F72616D', 'romantic_krieger', 'qualified_corr', -4774868512022958085, X'706572666563745F6B6F7A6172656B', 'bountiful_zaikowska', X'74686F7567687466756C5F6C6F6767616E73'), ('excellent_lydiettcarrion', 'diligent_denslow', X'666162756C6F75735F6D616E68617474616E', 'confident_tomar', 'glistening_ligt', -1134906665439009896, X'7175616C69666965645F6F6E6B656E', 'remarkable_anarkiya', X'6C6F79616C5F696E64616261'), ('passionate_melis', 'loyal_xsilent', X'68617264776F726B696E675F73637564', 'lustrous_barnes', 'nice_sugako', -4097897163377829983, X'726F6D616E7469635F6461686572', 'bright_imrie', X'73656E7369626C655F6D61726B'), ('giving_mlb', 'breathtaking_fourier', X'736C65656B5F616E61726368697374', 'glittering_malet', 'brilliant_crew', 8791228049111405793, X'626F756E746966756C5F626576656E736565', 'lovely_swords', X'70726F706974696F75735F696E656469746173'), ('honest_wright', 'qualified_rabble', X'736C65656B5F6D6172656368616C', 'shimmering_marius', 'blithesome_mckelvie', -1330737263592370654, X'6F70656E5F6D696E6465645F736D616C6C', 'energetic_gorman', X'70726F706974696F75735F6B6F74616E7969');",
"DELETE FROM lustrous_petit WHERE (ambitious_liman > 'adept_dabtaqu');",
"INSERT INTO lustrous_petit VALUES ('technological_dewey', 'fabulous_st', X'6F7074696D69737469635F73687562', 'considerate_levy', 'adaptable_kernis', 4195134012457716562, X'61646570745F736F6C6964617269646164', 'vibrant_crump', X'6C6F79616C5F72796E6572'), ('super_marjan', 'awesome_gethin', X'736C65656B5F6F737465727765696C', 'diplomatic_loidl', 'qualified_bokani', -2822676417968234733, X'6272696768745F64756E6C6170', 'creative_en', X'6D6972746866756C5F656C6F6666'), ('philosophical_malet', 'unique_garcia', X'76697669645F6E6F7262657267', 'spellbinding_fire', 'faithful_barringtonbush', -7293711848773657758, X'6272696C6C69616E745F6F6B65656665', 'gripping_guillon', X'706572736F6E61626C655F6D61726C696E7370696B65'), ('thoughtful_morefus', 'lustrous_rodriguez', X'636F6E666964656E745F67726F73736D616E726F73686368696E', 'devoted_jackson', 'propitious_karnage', -7802999054396485709, X'63617061626C655F64', 'enchanting_orwell', X'7477696E6B6C696E675F64616C616B6F676C6F75'), ('alluring_guillon', 'brilliant_pinotnoir', X'706572736F6E61626C655F6A6165636B6C65', 'open_minded_azeez', 'courageous_romania', 2126962403055072268, X'746563686E6F6C6F676963616C5F6962616E657A', 'open_minded_rosa', X'6C757374726F75735F6575726F7065'), ('courageous_kolokotronis', 'inquisitive_gahman', X'677265676172696F75735F626172726574', 'ambitious_shakur', 'fantastic_apatris', -1232732971861520864, X'737061726B6C696E675F7761746368', 'captivating_clover', X'636F6E666964656E745F736574686E65737363617374726F'), ('charming_sullivan', 'focused_congress', X'7368696D6D6572696E675F636C7562', 'wondrous_skrbina', 'giving_mendanlioglu', -6837337053772308333, X'636861726D696E675F73616C696E6173', 'rousing_hedva', X'6469706C6F6D617469635F7061796E');",
    ];

    for query in queries {
        let mut stmt = conn.query(query).unwrap().unwrap();
        loop {
            let row = stmt.step().expect("step");
            match row {
                StepResult::Done => {
                    break;
                }
                _ => {
                    tracing::debug!("row {:?}", row);
                }
            }
        }
    }
}

// this test will create a tree like this:
// -page:2, ptr(right):3
// +cells:
//   -page:3, ptr(right):0
//   +cells:
#[test]
pub fn test_drop_page_in_balancing_issue_1203_2() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let queries = vec![
"CREATE TABLE super_becky (engrossing_berger BLOB,plucky_chai BLOB,mirthful_asbo REAL,bountiful_jon REAL,competitive_petit REAL,engrossing_rexroth REAL);",
"INSERT INTO super_becky VALUES (X'636861726D696E675F6261796572', X'70726F647563746976655F70617269737369', 6847793643.408741, 7330361375.924953, -6586051582.891455, -6921021872.711397), (X'657863656C6C656E745F6F7267616E697A696E67', X'6C757374726F75735F73696E64696B616C6973', 9905774996.48619, 570325205.2246342, 5852346465.53047, 728566012.1968269), (X'7570626561745F73656174746C65', X'62726176655F6661756E', -2202725836.424899, 5424554426.388281, 2625872085.917082, -6657362503.808359), (X'676C6F77696E675F6D617877656C6C', X'7761726D686561727465645F726F77616E', -9610936969.793116, 4886606277.093559, -3414536174.7928505, 6898267795.317778), (X'64796E616D69635F616D616E', X'7374656C6C61725F7374657073', 3918935692.153696, 151068445.947237, 4582065669.356403, -3312668220.4789667), (X'64696C6967656E745F64757272757469', X'7175616C69666965645F6D726163686E696B', 5527271629.262201, 6068855126.044355, 289904657.13490677, 2975774820.0877323), (X'6469706C6F6D617469635F726F76657363696F', X'616C6C7572696E675F626F7474696369', 9844748192.66119, -6180276383.305578, -4137330511.025565, -478754566.79494476), (X'776F6E64726F75735F6173686572', X'6465766F7465645F6176657273696F6E', 2310211470.114773, -6129166761.628184, -2865371645.3145514, 7542428654.8645935), (X'617070726F61636861626C655F6B686F6C61', X'6C757374726F75735F6C696E6E656C6C', -4993113161.458349, 7356727284.362968, -3228937035.568404, -1779334005.5067253);",
"INSERT INTO super_becky VALUES (X'74686F7567687466756C5F726576696577', X'617765736F6D655F63726F73736579', 9401977997.012783, 8428201961.643898, 2822821303.052643, 4555601220.718847), (X'73706563746163756C61725F6B686179617469', X'616772656561626C655F61646F6E696465', 7414547022.041355, 365016845.73330307, 50682963.055828094, -9258802584.962656), (X'6C6F79616C5F656D6572736F6E', X'676C6F77696E675F626174616C6F', -5522070106.765736, 2712536599.6384163, 6631385631.869345, 1242757880.7583427), (X'68617264776F726B696E675F6F6B656C6C79', X'666162756C6F75735F66696C697373', 6682622809.9778805, 4233900041.917185, 9017477903.795563, -756846353.6034946), (X'68617264776F726B696E675F626C61756D616368656E', X'616666656374696F6E6174655F6B6F736D616E', -1146438175.3174362, -7545123696.438596, -6799494012.403366, 5646913977.971333), (X'66616E7461737469635F726F77616E', X'74686F7567687466756C5F7465727269746F72696573', -4414529784.916277, -6209371635.279242, 4491104121.288605, 2590223842.117277);",
"INSERT INTO super_becky VALUES (X'676C697374656E696E675F706F72746572', X'696E7175697369746976655F656D', 2986144164.3676434, 3495899172.5935287, -849280584.9386635, 6869709150.2699375), (X'696D6167696E61746976655F6D65726C696E6F', X'676C6F77696E675F616B74696D6F6E', 8733490615.829357, 6782649864.719433, 6926744218.74107, 1532081022.4379768), (X'6E6963655F726F73736574', X'626C69746865736F6D655F66696C697373', -839304300.0706863, 6155504968.705227, -2951592321.950267, -6254186334.572437), (X'636F6E666964656E745F6C69626574', X'676C696D6D6572696E675F6B6F74616E7969', -5344675223.37533, -8703794729.211002, 3987472096.020382, -7678989974.961197), (X'696D6167696E61746976655F6B61726162756C7574', X'64796E616D69635F6D6367697272', 2028227065.6995697, -7435689525.030833, 7011220815.569796, 5526665697.213846), (X'696E7175697369746976655F636C61726B', X'616666656374696F6E6174655F636C6561766572', 3016598350.546356, -3686782925.383732, 9671422351.958004, 9099319829.078941), (X'63617061626C655F746174616E6B61', X'696E6372656469626C655F6F746F6E6F6D61', 6339989259.432795, -8888997534.102034, 6855868409.475763, -2565348887.290493), (X'676F7267656F75735F6265726E657269', X'65647563617465645F6F6D6F77616C69', 6992467657.527826, -3538089391.748543, -7103111660.146708, 4019283237.3740463), (X'616772656561626C655F63756C74757265', X'73706563746163756C61725F657370616E61', 189387871.06959534, 6211851191.361202, 1786455196.9768047, 7966404387.318119);",
"INSERT INTO super_becky VALUES (X'7068696C6F736F70686963616C5F6C656967686C616973', X'666162756C6F75735F73656D696E61746F7265', 8688321500.141502, -7855144036.024546, -5234949709.573349, -9937638367.366447), (X'617070726F61636861626C655F726F677565', X'676C65616D696E675F6D7574696E79', -5351540099.744092, -3614025150.9013805, -2327775310.276925, 2223379997.077526), (X'676C696D6D6572696E675F63617263686961', X'696D6167696E61746976655F61737379616E6E', 4104832554.8371887, -5531434716.627781, 1652773397.4099865, 3884980522.1830273);",
"DELETE FROM super_becky WHERE (plucky_chai != X'7761726D686561727465645F6877616E67' AND mirthful_asbo != 9537234687.183533 AND bountiful_jon = -3538089391.748543);",
"INSERT INTO super_becky VALUES (X'706C75636B795F6D617263616E74656C', X'696D6167696E61746976655F73696D73', 9535651632.375484, 92270815.0720501, 1299048084.6248207, 6460855331.572151), (X'726F6D616E7469635F706F746C61746368', X'68756D6F726F75735F63686165686F', 9345375719.265533, 7825332230.247925, -7133157299.39028, -6939677879.6597), (X'656666696369656E745F6261676E696E69', X'63726561746976655F67726168616D', -2615470560.1954746, 6790849074.977201, -8081732985.448849, -8133707792.312794), (X'677265676172696F75735F73637564', X'7368696E696E675F67726F7570', -7996394978.2610035, -9734939565.228964, 1108439333.8481388, -5420483517.169478), (X'6C696B61626C655F6B616E6176616C6368796B', X'636F75726167656F75735F7761726669656C64', -1959869609.656724, 4176668769.239971, -8423220404.063669, 9987687878.685959), (X'657863656C6C656E745F68696C6473646F74746572', X'676C6974746572696E675F7472616D7564616E61', -5220160777.908238, 3892402687.8826714, 9803857762.617172, -1065043714.0265541), (X'6D61676E69666963656E745F717565657273', X'73757065725F717565657273', -700932053.2006226, -4706306995.253335, -5286045811.046467, 1954345265.5250092), (X'676976696E675F6275636B65726D616E6E', X'667269656E646C795F70697A7A6F6C61746F', -2186859620.9089565, -6098492099.446075, -7456845586.405931, 8796967674.444252);",
"DELETE FROM super_becky WHERE TRUE;",
"INSERT INTO super_becky VALUES (X'6F7074696D69737469635F6368616E69616C', X'656E657267657469635F6E65677261', 1683345860.4208698, 4163199322.9289455, -4192968616.7868404, -7253371206.571701), (X'616C6C7572696E675F686176656C', X'7477696E6B6C696E675F626965627579636B', -9947019174.287437, 5975899640.893995, 3844707723.8570194, -9699970750.513876), (X'6F7074696D69737469635F7A686F75', X'616D626974696F75735F636F6E6772657373', 4143738484.1081524, -2138255286.170598, 9960750454.03466, 5840575852.80299), (X'73706563746163756C61725F6A6F6E67', X'73656E7369626C655F616269646F72', -1767611042.9716015, -7684260477.580351, 4570634429.188147, -9222640121.140202), (X'706F6C6974655F6B657272', X'696E736967687466756C5F63686F646F726B6F6666', -635016769.5123329, -4359901288.494518, -7531565119.905825, -1180410948.6572971), (X'666C657869626C655F636F6D756E69656C6C6F', X'6E6963655F6172636F73', 8708423014.802425, -6276712625.559328, -771680766.2485523, 8639486874.113342);",
"DELETE FROM super_becky WHERE (mirthful_asbo < 9730384310.536528 AND plucky_chai < X'6E6963655F61726370B2');",
"DELETE FROM super_becky WHERE (mirthful_asbo > 6248699554.426553 AND bountiful_jon > 4124481472.333034);",
"INSERT INTO super_becky VALUES (X'676C696D6D6572696E675F77656C7368', X'64696C6967656E745F636F7262696E', 8217054003.369003, 8745594518.77864, 1928172803.2261295, -8375115534.050233), (X'616772656561626C655F6463', X'6C6F76696E675F666F72656D616E', -5483889804.871533, -8264576639.127487, 4770567289.404846, -3409172927.2573576), (X'6D617276656C6F75735F6173696D616B6F706F756C6F73', X'746563686E6F6C6F676963616C5F6A61637175696572', 2694858779.206814, -1703227425.3442516, -4504989231.263319, -3097265869.5230227), (X'73747570656E646F75735F64757075697364657269', X'68696C6172696F75735F6D75697268656164', 568174708.66469, -4878260547.265669, -9579691520.956625, 73507727.8100338), (X'626C69746865736F6D655F626C6F6B', X'61646570745F6C65696572', 7772117077.916897, 4590608571.321514, -881713470.657032, -9158405774.647465);",
"INSERT INTO super_becky VALUES (X'6772697070696E675F6573736578', X'67656E65726F75735F636875726368696C6C', -4180431825.598956, 7277443000.677654, 2499796052.7878246, -2858339306.235305), (X'756E697175655F6D6172656368616C', X'62726561746874616B696E675F636875726368696C6C', 1401354536.7625294, -611427440.2796707, -4621650430.463729, 1531473111.7482872), (X'657863656C6C656E745F66696E6C6579', X'666169746866756C5F62726F636B', -4020697828.0073624, -2833530733.19637, -7766170050.654022, 8661820959.434689);",
"INSERT INTO super_becky VALUES (X'756E697175655F6C617061797265', X'6C6F76696E675F7374617465', 7063237787.258968, -5425712581.365798, -7750509440.0141945, -7570954710.892544), (X'62726561746874616B696E675F6E65616C', X'636F75726167656F75735F61727269676F6E69', 289862394.2028198, 9690362375.014446, -4712463267.033899, 2474917855.0973473), (X'7477696E6B6C696E675F7368616B7572', X'636F75726167656F75735F636F6D6D6974746565', 5449035403.229155, -2159678989.597906, 3625606019.1150894, -3752010405.4475393);",
"INSERT INTO super_becky VALUES (X'70617373696F6E6174655F73686970776179', X'686F6E6573745F7363687765697A6572', 4193384746.165228, -2232151704.896323, 8615245520.962444, -9789090953.995636);",
"INSERT INTO super_becky VALUES (X'6C696B61626C655F69', X'6661766F7261626C655F6D626168', 6581403690.769894, 3260059398.9544716, -407118859.046051, -3155853965.2700634), (X'73696E636572655F6F72', X'616772656561626C655F617070656C6261756D', 9402938544.308651, -7595112171.758331, -7005316716.211025, -8368210960.419411);",
"INSERT INTO super_becky VALUES (X'6D617276656C6F75735F6B61736864616E', X'6E6963655F636F7272', -5976459640.85817, -3177550476.2092276, 2073318650.736992, -1363247319.9978447);",
"INSERT INTO super_becky VALUES (X'73706C656E6469645F6C616D656E646F6C61', X'677265676172696F75735F766F6E6E65677574', 6898259773.050102, 8973519699.707073, -25070632.280548096, -1845922497.9676847), (X'617765736F6D655F7365766572', X'656E657267657469635F706F746C61746368', -8750678407.717808, 5130907533.668898, -6778425327.111566, 3718982135.202587);",
"INSERT INTO super_becky VALUES (X'70726F706974696F75735F6D616C617465737461', X'657863656C6C656E745F65766572657474', -8846855772.62094, -6168969732.697067, -8796372709.125793, 9983557891.544613), (X'73696E636572655F6C6177', X'696E7175697369746976655F73616E647374726F6D', -6366985697.975358, 3838628702.6652164, 3680621713.3371124, -786796486.8049564), (X'706F6C6974655F676C6561736F6E', X'706C75636B795F677579616E61', -3987946379.104308, -2119148244.413993, -1448660343.6888638, -1264195510.1611118), (X'676C6974746572696E675F6C6975', X'70657273697374656E745F6F6C6976696572', 6741779968.943846, -3239809989.227495, -1026074003.5506897, 4654600514.871752);",
"DELETE FROM super_becky WHERE (engrossing_berger < X'6566651A3C70278D4E200657551D8071A1' AND competitive_petit > 1236742147.9451914);",
"INSERT INTO super_becky VALUES (X'6661766F7261626C655F726569746D616E', X'64657465726D696E65645F726974746572', -7412553243.829927, -7572665195.290464, 7879603411.222157, 3706943306.5691853), (X'70657273697374656E745F6E6F6C616E', X'676C6974746572696E675F73686570617264', 7028261282.277422, -2064164782.3494844, -5244048504.507779, -2399526243.005843), (X'6B6E6F776C6564676561626C655F70617474656E', X'70726F66696369656E745F726F7365627261756768', 3713056763.583538, 3919834206.566164, -6306779387.430006, -9939464323.995546), (X'616461707461626C655F7172757A', X'696E7175697369746976655F68617261776179', 6519349690.299835, -9977624623.820414, 7500579325.440605, -8118341251.362242);",
"INSERT INTO super_becky VALUES (X'636F6E73696465726174655F756E696F6E', X'6E6963655F6573736578', -1497385534.8720198, 9957688503.242973, 9191804202.566128, -179015615.7117195), (X'666169746866756C5F626F776C656773', X'6361707469766174696E675F6D6367697272', 893707300.1576138, 3381656294.246702, 6884723724.381908, 6248331214.701559), (X'6B6E6F776C6564676561626C655F70656E6E61', X'6B696E645F616A697468', -3335162603.6574974, 1812878172.8505402, 5115606679.658335, -5690100280.808182), (X'617765736F6D655F77696E7374616E6C6579', X'70726F706974696F75735F6361726173736F', -7395576292.503981, 4956546102.029215, -1468521769.7486448, -2968223925.60355), (X'636F75726167656F75735F77617266617265', X'74686F7567687466756C5F7361707068697265', 7052982930.566017, -9806098174.104418, -6910398936.377775, -4041963031.766964), (X'657863656C6C656E745F6B62', X'626C69746865736F6D655F666F75747A6F706F756C6F73', 6142173202.994768, 5193126957.544125, -7522202722.983735, -1659088056.594862), (X'7374756E6E696E675F6E6576616461', X'626F756E746966756C5F627572746F6E', -3822097036.7628613, -3458840259.240303, 2544472236.86788, 6928890176.466003);",
"INSERT INTO super_becky VALUES (X'706572736F6E61626C655F646D69747269', X'776F6E64726F75735F6133796F', 2651932559.0077076, 811299402.3174248, -8271909238.671928, 6761098864.189909);",
"INSERT INTO super_becky VALUES (X'726F7573696E675F6B6C6166657461', X'64617A7A6C696E675F6B6E617070', 9370628891.439335, -5923332007.253168, -2763161830.5880013, -9156194881.875952), (X'656666696369656E745F6C6576656C6C6572', X'616C6C7572696E675F706561636F7474', 3102641409.8314342, 2838360181.628153, 2466271662.169607, 1015942181.844162), (X'6469706C6F6D617469635F7065726B696E73', X'726F7573696E675F6172616269', -1551071129.022499, -8079487600.186886, 7832984580.070087, -6785993247.895652), (X'626F756E746966756C5F6D656D62657273', X'706F77657266756C5F70617269737369', 9226031830.72445, 7012021503.536997, -2297349030.108919, -2738320055.4710903), (X'676F7267656F75735F616E6172636F7469636F', X'68656C7066756C5F7765696C616E64', -8394163480.676959, -2978605095.699134, -6439355448.021704, 9137308022.281273), (X'616666656374696F6E6174655F70726F6C65696E666F', X'706C75636B795F73616E7A', 3546758708.3524914, -1870964264.9353771, 338752565.3643894, -3908023657.299715), (X'66756E6E795F706F70756C61697265', X'6F75747374616E64696E675F626576696E67746F6E', -1533858145.408224, 6164225076.710373, 8419445987.622173, 584555253.6852646), (X'76697669645F6D7474', X'7368696D6D6572696E675F70616F6E65737361', 5512251366.193035, -8680583180.123213, -4445968638.153208, -3274009935.4229546);",
"INSERT INTO super_becky VALUES (X'7068696C6F736F70686963616C5F686F7264', X'657863656C6C656E745F67757373656C7370726F757473', -816909447.0240917, -3614686681.8786583, 7701617524.26067, -4541962047.183721), (X'616D6961626C655F69676E6174696576', X'6D61676E69666963656E745F70726F76696E6369616C69', -1318532883.847702, -4918966075.976474, -7601723171.33518, -3515747704.3847466), (X'70726F66696369656E745F32303137', X'66756E6E795F6E77', -1264540201.518032, 8227396547.578808, 6245093925.183641, -8368355328.110817);",
"INSERT INTO super_becky VALUES (X'77696C6C696E675F6E6F6B6B65', X'726F6D616E7469635F677579616E61', 6618610796.3707695, -3814565359.1524105, 1663106272.4565296, -4175107840.768817), (X'72656C617865645F7061766C6F76', X'64657465726D696E65645F63686F646F726B6F6666', -3350029338.034504, -3520837855.4619064, 3375167499.631817, -8866806483.714607), (X'616D706C655F67696464696E6773', X'667269656E646C795F6A6F686E', 1458864959.9942684, 1344208968.0486107, 9335156635.91314, -6180643697.918882), (X'72656C617865645F6C65726F79', X'636F75726167656F75735F6E6F72646772656E', -5164986537.499656, 8820065797.720875, 6146530425.891005, 6949241471.958189), (X'666F63757365645F656D6D61', X'696D6167696E61746976655F6C6F6E67', -9587619060.80035, 6128068142.184402, 6765196076.956905, 800226302.7983418);",
"INSERT INTO super_becky VALUES (X'616D626974696F75735F736F6E67', X'706572666563745F6761686D616E', 4989979180.706432, -9374266591.537058, 314459621.2820797, -3200029490.9553604), (X'666561726C6573735F626C6174', X'676C697374656E696E675F616374696F6E', -8512203612.903147, -7625581186.013805, -9711122307.234787, -301590929.32751083), (X'617765736F6D655F6669646573', X'666169746866756C5F63756E6E696E6768616D', -1428228887.9205084, 7669883854.400173, 5604446195.905277, -1509311057.9653416), (X'68756D6F726F75735F77697468647261776E', X'62726561746874616B696E675F7472617562656C', -7292778713.676636, -6728132503.529593, 2805341768.7252483, 330416975.2300949);",
"INSERT INTO super_becky VALUES (X'677265676172696F75735F696873616E', X'7374656C6C61725F686172746D616E', 8819210651.1988, 5298459883.813452, 7293544377.958424, 460475869.72971725), (X'696E736967687466756C5F62657765726E69747A', X'676C65616D696E675F64656E736C6F77', -6911957282.193239, 1754196756.2193146, -6316860403.693853, -3094020672.236368), (X'6D6972746866756C5F616D6265727261656B656C6C79', X'68756D6F726F75735F6772617665', 1785574023.0269203, -372056983.82761574, 4133719439.9538956, 9374053482.066044), (X'76697669645F736169747461', X'7761726D686561727465645F696E656469746173', 2787071361.6099434, 9663839418.553448, -5934098589.901047, -9774745509.608858), (X'61646570745F6F6375727279', X'6C696B61626C655F726569746D616E', -3098540915.1310825, 5460848322.672174, -6012867197.519758, 6769770087.661135), (X'696E646570656E64656E745F6F', X'656C6567616E745F726F6F726461', 1462542860.3143978, 3360904654.2464733, 5458876201.665213, -5522844849.529962), (X'72656D61726B61626C655F626F6B616E69', X'6F70656E5F6D696E6465645F686F72726F78', 7589481760.867031, 7970075121.546291, 7513467575.5213585, 9663061478.289227), (X'636F6E666964656E745F6C616479', X'70617373696F6E6174655F736B726F7A6974736B79', 8266917234.53915, -7172933478.625412, 309854059.94031143, -8309837814.497616);",
"DELETE FROM super_becky WHERE (competitive_petit != 8725256604.165474 OR engrossing_rexroth > -3607424615.7839313 OR plucky_chai < X'726F7573696E675F6216E20375');",
"INSERT INTO super_becky VALUES (X'7368696E696E675F736F6C69646169726573', X'666561726C6573735F63617264616E', -170727879.20838165, 2744601113.384678, 5676912434.941502, 6757573601.657997), (X'636F75726167656F75735F706C616E636865', X'696E646570656E64656E745F636172736F6E', -6271723086.761938, -180566679.7470188, -1285774632.134449, 1359665735.7842407), (X'677265676172696F75735F7374616D61746F76', X'7374756E6E696E675F77696C64726F6F7473', -6210238866.953484, 2492683045.8287067, -9688894361.68205, 5420275482.048567), (X'696E646570656E64656E745F6F7267616E697A6572', X'676C6974746572696E675F736F72656C', 9291163783.3073, -6843003475.769236, -1320245894.772686, -5023483808.044955), (X'676C6F77696E675F6E65736963', X'676C65616D696E675F746F726D6579', 829526382.8027191, 9365690945.1316, 4761505764.826195, -4149154965.0024815), (X'616C6C7572696E675F646F637472696E65', X'6E6963655F636C6561766572', 3896644979.981762, -288600448.8016701, 9462856570.130062, -909633752.5993862);",
    ];

    for query in queries {
        let mut stmt = conn.query(query).unwrap().unwrap();
        loop {
            let row = stmt.step().expect("step");
            match row {
                StepResult::Done => {
                    break;
                }
                _ => {
                    tracing::debug!("row {:?}", row);
                }
            }
        }
    }
}

#[test]
pub fn test_free_space() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();
    let page = get_page(2);

    let page_contents = page.get_contents();
    let header_size = 8;
    let usable_space = 4096;

    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let payload = add_record(0, 0, page.clone(), record, &conn);
    let free = compute_free_space(page_contents, usable_space).unwrap();
    assert_eq!(free, 4096 - payload.len() - 2 - header_size);
}

/// A cell smaller than MINIMUM_CELL_SIZE still takes MINIMUM_CELL_SIZE
/// bytes on the page. If the free-space check in insert_into_cell()
/// forgets that, a 3-byte cell inserted into a page with exactly 5 free
/// bytes passes the check (3 + 2 == 5) but the allocation takes 6, and
/// the cell content area slides one byte into the cell pointer array.
/// The cell must go to the overflow path instead.
#[test]
pub fn test_tiny_cell_insert_must_not_overlap_cell_pointer_array() {
    let page = get_page(2);
    btree_init_page(&page, PageType::IndexLeaf, 0, 4096);
    let contents = page.get_contents();
    let usable_space = 4096;

    // Fill the page so exactly 5 free bytes remain: the empty index leaf
    // has 4096 - 8 = 4088 free bytes, and 679 four-byte cells plus one
    // seven-byte cell consume 679 * (4 + 2) + (7 + 2) = 4083 of them.
    let four_byte_cell = [0x03, b'a', b'b', b'c'];
    for i in 0..679 {
        insert_into_cell(contents, &four_byte_cell, i, usable_space).unwrap();
    }
    let seven_byte_cell = [0x06, b'a', b'b', b'c', b'd', b'e', b'f'];
    insert_into_cell(contents, &seven_byte_cell, 679, usable_space).unwrap();
    assert_eq!(compute_free_space(contents, usable_space).unwrap(), 5);

    let three_byte_cell = [0x02, b'x', b'y'];
    insert_into_cell(contents, &three_byte_cell, 680, usable_space).unwrap();

    assert_eq!(
        contents.overflow_cells.len(),
        1,
        "a 3-byte cell needs MINIMUM_CELL_SIZE + 2 = 6 bytes, so it must overflow when only 5 are free"
    );
    let pointer_array_end = contents.offset() + contents.header_size() + 2 * contents.cell_count();
    assert!(
        contents.cell_content_area() as usize >= pointer_array_end,
        "cell content area ({}) overlaps the cell pointer array (ends at {})",
        contents.cell_content_area(),
        pointer_array_end
    );
}

#[test]
pub fn test_defragment_1() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let usable_space = 4096;

    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let payload = add_record(0, 0, page.clone(), record, &conn);

    assert_eq!(page_contents.cell_count(), 1);
    defragment_page(page_contents, usable_space, 4).unwrap();
    assert_eq!(page_contents.cell_count(), 1);
    let (start, len) = page_contents.cell_get_raw_region(0, usable_space).unwrap();
    let buf = page_contents.as_ptr();
    assert_eq!(&payload, &buf[start..start + len]);
}

#[test]
pub fn test_insert_drop_insert() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let usable_space = 4096;

    let regs = &[
        Register::Value(Value::from_i64(0)),
        Register::Value(Value::Text(Text::new("aaaaaaaa"))),
    ];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let _ = add_record(0, 0, page.clone(), record, &conn);

    assert_eq!(page_contents.cell_count(), 1);
    drop_cell(page_contents, 0, usable_space).unwrap();
    assert_eq!(page_contents.cell_count(), 0);

    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let payload = add_record(0, 0, page.clone(), record, &conn);
    assert_eq!(page_contents.cell_count(), 1);

    let (start, len) = page_contents.cell_get_raw_region(0, usable_space).unwrap();
    let buf = page_contents.as_ptr();
    assert_eq!(&payload, &buf[start..start + len]);
}

#[test]
pub fn test_insert_drop_insert_multiple() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let usable_space = 4096;

    let regs = &[
        Register::Value(Value::from_i64(0)),
        Register::Value(Value::Text(Text::new("aaaaaaaa"))),
    ];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let _ = add_record(0, 0, page.clone(), record, &conn);

    for _ in 0..100 {
        assert_eq!(page_contents.cell_count(), 1);
        drop_cell(page_contents, 0, usable_space).unwrap();
        assert_eq!(page_contents.cell_count(), 0);

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(0, 0, page.clone(), record, &conn);
        assert_eq!(page_contents.cell_count(), 1);

        let (start, len) = page_contents.cell_get_raw_region(0, usable_space).unwrap();
        let buf = page_contents.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }
}

#[test]
pub fn test_drop_a_few_insert() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let usable_space = 4096;

    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let payload = add_record(0, 0, page.clone(), record, &conn);
    let regs = &[Register::Value(Value::from_i64(1))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let _ = add_record(1, 1, page.clone(), record, &conn);
    let regs = &[Register::Value(Value::from_i64(2))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let _ = add_record(2, 2, page.clone(), record, &conn);

    drop_cell(page_contents, 1, usable_space).unwrap();
    drop_cell(page_contents, 1, usable_space).unwrap();

    ensure_cell(page_contents, 0, &payload);
}

#[test]
pub fn test_fuzz_victim_1() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);

    let page_contents = page.get_contents();
    let usable_space = 4096;

    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let _ = add_record(0, 0, page.clone(), record, &conn);

    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let _ = add_record(0, 0, page.clone(), record, &conn);
    drop_cell(page_contents, 0, usable_space).unwrap();

    defragment_page(page_contents, usable_space, 4).unwrap();

    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let _ = add_record(0, 1, page.clone(), record, &conn);

    drop_cell(page_contents, 0, usable_space).unwrap();

    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let _ = add_record(0, 1, page.clone(), record, &conn);
}

#[test]
pub fn test_fuzz_victim_2() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);
    let usable_space = 4096;
    let insert = |pos, page| {
        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(0, pos, page, record, &conn);
    };
    let drop = |pos, page| {
        drop_cell(page, pos, usable_space).unwrap();
    };
    let defragment = |page| {
        defragment_page(page, usable_space, 4).unwrap();
    };

    defragment(page.get_contents());
    defragment(page.get_contents());
    insert(0, page.clone());
    drop(0, page.get_contents());
    insert(0, page.clone());
    drop(0, page.get_contents());
    insert(0, page.clone());
    defragment(page.get_contents());
    defragment(page.get_contents());
    drop(0, page.get_contents());
    defragment(page.get_contents());
    insert(0, page.clone());
    drop(0, page.get_contents());
    insert(0, page.clone());
    insert(1, page.clone());
    insert(1, page.clone());
    insert(0, page.clone());
    drop(3, page.get_contents());
    drop(2, page.get_contents());
    compute_free_space(page.get_contents(), usable_space).unwrap();
}

#[test]
pub fn test_fuzz_victim_3() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);
    let usable_space = 4096;
    let insert = |pos, page| {
        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(0, pos, page, record, &conn);
    };
    let drop = |pos, page| {
        drop_cell(page, pos, usable_space).unwrap();
    };
    let defragment = |page| {
        defragment_page(page, usable_space, 4).unwrap();
    };
    let regs = &[Register::Value(Value::from_i64(0))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
    let mut fill_cell_payload_state = FillCellPayloadState::Start;
    run_until_done(
        || {
            fill_cell_payload(
                &PinGuard::new(page.clone()),
                Some(0),
                &mut payload,
                0,
                &record,
                4096,
                &conn.pager.load(),
                &mut fill_cell_payload_state,
            )
        },
        &conn.pager.load().clone(),
    )
    .unwrap();

    insert(0, page.clone());
    defragment(page.get_contents());
    insert(0, page.clone());
    defragment(page.get_contents());
    insert(0, page.clone());
    drop(2, page.get_contents());
    drop(0, page.get_contents());
    let free = compute_free_space(page.get_contents(), usable_space).unwrap();
    let total_size = payload.len() + 2;
    assert_eq!(
        free,
        usable_space - page.get_contents().header_size() - total_size
    );
    dbg!(free);
}

#[test]
pub fn btree_insert_sequential() {
    let (pager, root_page, _, _) = empty_btree();
    let mut keys = Vec::new();
    let num_columns = 5;

    for i in 0..10000 {
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        tracing::info!("INSERT INTO t VALUES ({});", i,);
        let regs = &[Register::Value(Value::from_i64(i))];
        let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        tracing::trace!("before insert {}", i);
        run_until_done(
            || {
                let key = SeekKey::TableRowId(i);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();
        run_until_done(
            || cursor.insert(&BTreeKey::new_table_rowid(i, Some(&value))),
            pager.deref(),
        )
        .unwrap();
        keys.push(i);
    }
    if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
        panic!("invalid btree");
    }
    tracing::trace!(
        "=========== btree ===========\n{}\n\n",
        format_btree(pager.clone(), root_page, 0)
    );
    for key in keys.iter() {
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let key = Value::from_i64(*key);
        let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
        assert!(exists, "key not found {key}");
    }
}

#[test]
pub fn test_big_payload_compute_free() {
    let (db, _temp_dir) = get_database();
    let conn = db.connect().unwrap();

    let page = get_page(2);
    let usable_space = 4096;
    let regs = &[Register::Value(Value::Blob(crate::alloc::vec![0; 3600]))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
    let mut fill_cell_payload_state = FillCellPayloadState::Start;
    run_until_done(
        || {
            fill_cell_payload(
                &PinGuard::new(page.clone()),
                Some(0),
                &mut payload,
                0,
                &record,
                4096,
                &conn.pager.load(),
                &mut fill_cell_payload_state,
            )
        },
        &conn.pager.load().clone(),
    )
    .unwrap();
    insert_into_cell(page.get_contents(), &payload, 0, 4096).unwrap();
    let free = compute_free_space(page.get_contents(), usable_space).unwrap();
    let total_size = payload.len() + 2;
    assert_eq!(
        free,
        usable_space - page.get_contents().header_size() - total_size
    );
    dbg!(free);
}

#[test]
pub fn test_delete_balancing() {
    // What does this test do:
    // 1. Insert 10,000 rows of ~15 byte payload each. This creates
    //    nearly 40 pages (10,000 * 15 / 4096) and 240 rows per page.
    // 2. Delete enough rows to create empty/ nearly empty pages to trigger balancing
    //    (verified this in SQLite).
    // 3. Verify validity/integrity of btree after deleting and also verify that these
    //    values are actually deleted.

    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 5;

    // Insert 10,000 records in to the BTree.
    for i in 1..=10000 {
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let regs = &[Register::Value(Value::Text(Text::new("hello world")))];
        let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

        run_until_done(
            || {
                let key = SeekKey::TableRowId(i);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();

        run_until_done(
            || cursor.insert(&BTreeKey::new_table_rowid(i, Some(&value))),
            pager.deref(),
        )
        .unwrap();
    }

    if let (_, false) = validate_btree(pager.clone(), root_page) {
        panic!("Invalid B-tree after insertion");
    }
    let num_columns = 5;

    // Delete records with 500 <= key <= 3500
    for i in 500..=3500 {
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let seek_key = SeekKey::TableRowId(i);

        let seek_result = run_until_done(
            || cursor.seek(seek_key.clone(), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();

        if matches!(seek_result, SeekResult::Found) {
            run_until_done(|| cursor.delete(), pager.deref()).unwrap();
        }
    }

    // Verify that records with key < 500 and key > 3500 still exist in the BTree.
    for i in 1..=10000 {
        if (500..=3500).contains(&i) {
            continue;
        }

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let key = Value::from_i64(i);
        let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
        assert!(exists, "Key {i} should exist but doesn't");
    }

    // Verify the deleted records don't exist.
    for i in 500..=3500 {
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let key = Value::from_i64(i);
        let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
        assert!(!exists, "Deleted key {i} still exists");
    }
}

#[test]
pub fn test_overflow_cells() {
    let iterations = 10_usize;
    let mut huge_texts = Vec::new();
    for i in 0..iterations {
        let mut huge_text = String::new();
        for _j in 0..8192 {
            huge_text.push((b'A' + i as u8) as char);
        }
        huge_texts.push(huge_text);
    }

    let (pager, root_page, _, _) = empty_btree();
    let num_columns = 5;

    for (i, huge_text) in huge_texts.iter().enumerate().take(iterations) {
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        tracing::info!("INSERT INTO t VALUES ({});", i,);
        let regs = &[Register::Value(Value::Text(Text::new(huge_text.clone())))];
        let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        tracing::trace!("before insert {}", i);
        tracing::debug!(
            "=========== btree before ===========\n{}\n\n",
            format_btree(pager.clone(), root_page, 0)
        );
        run_until_done(
            || {
                let key = SeekKey::TableRowId(i as i64);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();
        run_until_done(
            || cursor.insert(&BTreeKey::new_table_rowid(i as i64, Some(&value))),
            pager.deref(),
        )
        .unwrap();
        tracing::debug!(
            "=========== btree after ===========\n{}\n\n",
            format_btree(pager.clone(), root_page, 0)
        );
    }
    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
    let _c = cursor.move_to_root().unwrap();
    for i in 0..iterations {
        run_until_done(|| cursor.next(), pager.deref()).unwrap();
        let has_next = cursor.has_record();
        if !has_next {
            panic!("expected Some(rowid) but got {:?}", cursor.has_record());
        };
        let rowid = run_until_done(|| cursor.rowid(), pager.deref())
            .unwrap()
            .unwrap();
        assert_eq!(rowid, i as i64, "got!=expected");
    }
}

fn run_until_done<T>(action: impl FnMut() -> IOResultOr<T>, pager: &Pager) -> Result<T> {
    pager.io.block(action)
}

#[test]
fn test_free_array() {
    let (mut rng, seed) = rng_from_time_or_env();
    tracing::info!("seed={}", seed);

    const ITERATIONS: usize = 10000;
    for _ in 0..ITERATIONS {
        let mut cell_array = CellArray {
            cell_payloads: crate::alloc::vec![],
            cell_count_per_page_cumulative: [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
        };
        let mut cells_cloned = Vec::new();
        let (pager, _, _, _) = empty_btree();
        let page_type = PageType::TableLeaf;
        let page = run_until_done(|| pager.allocate_page(), &pager).unwrap();
        btree_init_page(&page, page_type, 0, pager.usable_space());

        let mut size = (rng.next_u64() % 100) as u16;
        let mut i = 0;
        // add a bunch of cells
        while compute_free_space(page.get_contents(), pager.usable_space()).unwrap()
            >= size as usize + 10
        {
            insert_cell(i, size, page.clone(), pager.clone());
            i += 1;
            size = (rng.next_u64() % 1024) as u16;
        }

        // Create cell array with references to cells inserted
        let contents = page.get_contents();
        for cell_idx in 0..contents.cell_count() {
            let buf = contents.as_ptr();
            let (start, len) = contents
                .cell_get_raw_region(cell_idx, pager.usable_space())
                .unwrap();
            cell_array
                .cell_payloads
                .push(to_static_buf(&mut buf[start..start + len]));
            cells_cloned.push(buf[start..start + len].to_vec());
        }

        debug_validate_cells!(contents, pager.usable_space());

        // now free a prefix or suffix of cells added
        let cells_before_free = contents.cell_count();
        let size = rng.next_u64() as usize % cells_before_free;
        let prefix = rng.next_u64() % 2 == 0;
        let start = if prefix {
            0
        } else {
            contents.cell_count() - size
        };
        let removed =
            page_free_array(contents, start, size, &cell_array, pager.usable_space()).unwrap();
        // shift if needed
        if prefix {
            shift_cells_left(contents, cells_before_free, removed);
        }

        assert_eq!(removed, size);
        assert_eq!(contents.cell_count(), cells_before_free - size);
        #[cfg(debug_assertions)]
        debug_validate_cells_core(contents, pager.usable_space());
        // check cells are correct
        let mut cell_idx_cloned = if prefix { size } else { 0 };
        for cell_idx in 0..contents.cell_count() {
            let buf = contents.as_ptr();
            let (start, len) = contents
                .cell_get_raw_region(cell_idx, pager.usable_space())
                .unwrap();
            let cell_in_page = &buf[start..start + len];
            let cell_in_array = &cells_cloned[cell_idx_cloned];
            assert_eq!(cell_in_page, cell_in_array);
            cell_idx_cloned += 1;
        }
    }
}

fn insert_cell(cell_idx: u64, size: u16, page: PageRef, pager: Arc<Pager>) {
    let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
    let regs = &[Register::Value(Value::Blob(
        crate::alloc::vec![0; size as usize],
    ))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
    let mut fill_cell_payload_state = FillCellPayloadState::Start;
    let contents = page.get_contents();
    run_until_done(
        || {
            fill_cell_payload(
                &PinGuard::new(page.clone()),
                Some(cell_idx as i64),
                &mut payload,
                cell_idx as usize,
                &record,
                pager.usable_space(),
                &pager,
                &mut fill_cell_payload_state,
            )
        },
        &pager,
    )
    .unwrap();
    insert_into_cell(contents, &payload, cell_idx as usize, pager.usable_space()).unwrap();
}

/// Direct red-first coverage for the pager-level cursor registry and
/// saveAllCursors port introduced in #7341 (register_with_pager,
/// has_peers toggling, drive_pending_peer_save inside insert/delete,
/// clear_btree's auto-invalidation of peers).
///
/// Pairs with the SQL-surface red-first cases in
/// sqlite/conformance/sqlite-sqltests/save-all-cursors.sqltest (window function
/// over triple self-joined source) and the pre-existing
/// sqlite/conformance/sqlite-sqltests/window-selfjoin-reset-sorter.sqltest.
/// Those exercise the same machinery end-to-end; the cases here
/// pin individual primitives (peer registration, save vs.
/// invalidate, restore semantics) so a regression bisects faster.
mod save_all_cursors {
    use super::*;
    use crate::storage::btree::SavePositionResult;
    use test_log::test;

    /// Boxed cursor pinned to its heap location for the duration of the
    /// test — register_cursor stores raw pointers, so the cursor must
    /// not be moved after registration.
    fn make_registered_cursor(
        pager: &Arc<Pager>,
        root_page: i64,
        num_columns: usize,
    ) -> Box<BTreeCursor> {
        let cursor = Box::new(BTreeCursor::new_table(
            pager.clone(),
            root_page,
            num_columns,
        ));
        (*cursor).register_with_pager();
        cursor
    }

    /// A peer's insert must invalidate this cursor's cached rightmost
    /// page id (move_to_rightmost's skip-a-seek optimization). Once the
    /// peer's appends split the rightmost leaf, a long-lived cursor's
    /// last() would otherwise trust the stale cache and return the old
    /// page's last cell instead of the true maximum.
    #[test]
    fn peer_write_invalidates_rightmost_page_cache() {
        let (pager, root_page, _db, _conn) = empty_btree();
        let mut writer = make_registered_cursor(&pager, root_page, 1);
        let mut reader = make_registered_cursor(&pager, root_page, 1);

        // Blob payloads so a handful of appends split the rightmost leaf.
        for rowid in 1..=8 {
            insert_record(
                &mut writer,
                &pager,
                rowid,
                Value::Blob(crate::alloc::vec![0u8; 1000]),
            )
            .unwrap();
        }

        // Positions the reader on the last row and caches the rightmost page id.
        run_until_done(|| reader.last(), pager.deref()).unwrap();
        let max1 = run_until_done(|| reader.rowid(), pager.deref()).unwrap();
        assert_eq!(max1, Some(8));

        // Peer appends: balance_quick allocates a new rightmost leaf.
        for rowid in 9..=16 {
            insert_record(
                &mut writer,
                &pager,
                rowid,
                Value::Blob(crate::alloc::vec![0u8; 1000]),
            )
            .unwrap();
        }

        run_until_done(|| reader.last(), pager.deref()).unwrap();
        let max2 = run_until_done(|| reader.rowid(), pager.deref()).unwrap();
        assert_eq!(
            max2,
            Some(16),
            "last() must see the true maximum after a peer split the rightmost leaf"
        );
    }

    /// A peer's insert must reset this cursor's memoized count():
    /// count_state stays at CountState::Finish after a full count, so
    /// without invalidation every later count() returns the stale value.
    #[test]
    fn peer_write_invalidates_count_cache() {
        let (pager, root_page, _db, _conn) = empty_btree();
        let mut writer = make_registered_cursor(&pager, root_page, 1);
        let mut reader = make_registered_cursor(&pager, root_page, 1);

        for rowid in 1..=5 {
            insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
        }
        let count1 = run_until_done(|| reader.count(), pager.deref()).unwrap();
        assert_eq!(count1, 5);

        insert_record(&mut writer, &pager, 6, Value::from_i64(6)).unwrap();
        let count2 = run_until_done(|| reader.count(), pager.deref()).unwrap();
        assert_eq!(
            count2, 6,
            "count() must not return the memoized pre-write value"
        );
    }

    #[test]
    fn registry_toggles_has_peers_flag() {
        let (pager, root_page, _db, _conn) = empty_btree();
        let cursor_a = make_registered_cursor(&pager, root_page, 1);
        assert!(
            !cursor_a
                .has_peers
                .load(crate::sync::atomic::Ordering::Relaxed),
            "single registered cursor must have has_peers=false"
        );

        let cursor_b = make_registered_cursor(&pager, root_page, 1);
        assert!(
            cursor_a
                .has_peers
                .load(crate::sync::atomic::Ordering::Relaxed),
            "registering a peer must set has_peers on the original"
        );
        assert!(
            cursor_b
                .has_peers
                .load(crate::sync::atomic::Ordering::Relaxed),
            "the newly registered cursor must also see has_peers=true"
        );

        drop(cursor_b);
        assert!(
            !cursor_a
                .has_peers
                .load(crate::sync::atomic::Ordering::Relaxed),
            "dropping the peer must clear has_peers on the survivor"
        );
    }

    #[test]
    fn registry_buckets_per_root_page() {
        // Cursors on different root pages must not see each other as
        // peers; saveAllCursors is per-root (SQLite btree.c:806).
        let (pager, root_a, _db, _conn) = empty_btree();
        let page_b = run_until_done(|| pager.allocate_page(), &pager).unwrap();
        btree_init_page(&page_b, PageType::TableLeaf, 0, pager.usable_space());
        let root_b = page_b.get().id() as i64;

        let cursor_a = make_registered_cursor(&pager, root_a, 1);
        let cursor_b = make_registered_cursor(&pager, root_b, 1);
        assert!(
            !cursor_a
                .has_peers
                .load(crate::sync::atomic::Ordering::Relaxed)
                && !cursor_b
                    .has_peers
                    .load(crate::sync::atomic::Ordering::Relaxed),
            "cursors on different roots must not be peers"
        );
    }

    #[test]
    fn try_save_position_table_returns_saved() {
        let (pager, root_page, _db, _conn) = empty_btree();
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, 1);
        for rowid in 1..=5 {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid)).unwrap();
        }
        run_until_done(
            || cursor.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();
        assert!(cursor.has_record());

        let outcome = run_until_done(
            || cursor.try_save_position_for_external_balance(),
            pager.deref(),
        )
        .unwrap();
        assert_eq!(outcome, SavePositionResult::Saved);
        assert_eq!(cursor.valid_state, CursorValidState::RequireSeek);
        assert!(cursor.context.is_some());
    }

    #[test]
    fn try_save_position_unpositioned_returns_saved_noop() {
        // valid_state=Valid + has_record=false ⇒ nothing to save and
        // nothing to invalidate (stack already in a re-navigable state).
        let (pager, root_page, _db, _conn) = empty_btree();
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, 1);
        let outcome = run_until_done(
            || cursor.try_save_position_for_external_balance(),
            pager.deref(),
        )
        .unwrap();
        assert_eq!(outcome, SavePositionResult::Saved);
        assert!(cursor.context.is_none());
    }

    #[test]
    fn clear_btree_auto_invalidates_peer() {
        // Without saveAllCursors, ResetSorter used to invalidate dup
        // cursors via pointer-equality from op_reset_sorter. Now
        // clear_btree itself does it via the pager registry.
        let (pager, root_page, _db, _conn) = empty_btree();
        let mut writer = make_registered_cursor(&pager, root_page, 1);
        let mut peer = make_registered_cursor(&pager, root_page, 1);

        for rowid in 1..=10 {
            insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
        }
        run_until_done(
            || peer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();
        assert!(peer.has_record());
        assert!(peer.stack.current_page >= 0);

        run_until_done(|| writer.clear_btree(), &pager).unwrap();

        assert!(
            !peer.has_record(),
            "peer must observe has_record=false after a peer's clear_btree"
        );
        assert_eq!(
            peer.stack.current_page, -1,
            "peer's page stack must be reset to the sentinel"
        );
    }

    #[test]
    fn delete_preserves_peer_logical_position() {
        // Cursor1 deletes rowid=3 while cursor2 sits on rowid=5. Without
        // the saveAllCursors port, cursor2's cached cell_idx points to
        // the cell that shifted left into rowid=5's slot (now rowid=6),
        // and rowid() silently returns 6. With the port, cursor2's
        // position is saved on entry to delete and restored on next
        // access — rowid() returns 5.
        let (pager, root_page, _db, _conn) = empty_btree();
        let mut writer = make_registered_cursor(&pager, root_page, 1);
        let mut peer = make_registered_cursor(&pager, root_page, 1);

        for rowid in 1..=10 {
            insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
        }
        run_until_done(
            || peer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();
        assert_eq!(
            run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
            Some(5)
        );

        run_until_done(
            || writer.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();
        run_until_done(|| writer.delete(), pager.deref()).unwrap();

        assert_eq!(
            run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
            Some(5),
            "peer must still observe its logical rowid after a peer delete"
        );
    }

    #[test]
    fn delete_of_peers_own_row_lands_on_next_greater() {
        // SQLite's CURSOR_SKIPNEXT (btree.c:915): when restore_context's
        // re-seek lands on NotFound, the cursor sets skip_advance so the
        // next() returns the cell the seek landed on instead of stepping
        // past it. Forward iteration continues correctly after a peer
        // deletes the row we were sitting on.
        let (pager, root_page, _db, _conn) = empty_btree();
        let mut writer = make_registered_cursor(&pager, root_page, 1);
        let mut peer = make_registered_cursor(&pager, root_page, 1);

        for rowid in 1..=10 {
            insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
        }
        run_until_done(
            || peer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();

        run_until_done(
            || writer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();
        run_until_done(|| writer.delete(), pager.deref()).unwrap();

        // First access after a peer wipe-out triggers restore_context's
        // NotFound branch and sets skip_advance.
        run_until_done(|| peer.next(), pager.deref()).unwrap();
        assert_eq!(
            run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
            Some(6),
            "next() after peer deletion of our row must land on the next-greater rowid"
        );
    }

    #[test]
    fn record_payload_restores_after_peer_insert() {
        for size in [16, 6000] {
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut writer = make_registered_cursor(&pager, root_page, 1);
            let mut reader = make_registered_cursor(&pager, root_page, 1);
            let value = Value::Blob(crate::alloc::vec![b'x'; size]);
            let expected =
                ImmutableRecord::from_registers(&[Register::Value(value.clone())], 1).unwrap();
            insert_record(&mut writer, &pager, 5, value).unwrap();
            run_until_done(|| reader.rewind(), pager.deref()).unwrap();

            for rowid in [1, 2] {
                insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
                assert!(reader.needs_restore());
                // The second read exercises the remembered location or overflow buffer.
                for _ in 0..2 {
                    let payload = run_until_done(
                        || {
                            Ok(reader
                                .record_payload()?
                                .map(|payload| payload.map(<[u8]>::to_vec)))
                        },
                        pager.deref(),
                    )
                    .unwrap()
                    .unwrap();
                    assert_eq!(payload, expected.get_payload());
                    assert!(!reader.needs_restore());
                }
            }

            insert_record(&mut writer, &pager, 3, Value::from_i64(3)).unwrap();
            assert!(reader.needs_restore());
            reader.set_null_flag(true);
            let restored = run_until_done(
                || {
                    Ok(reader
                        .record_payload()?
                        .map(|payload| payload.map(<[u8]>::to_vec)))
                },
                pager.deref(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(restored, expected.get_payload());
            assert!(!reader.needs_restore());
            assert!(!reader.get_null_flag());

            reader.set_null_flag(true);
            let missing = run_until_done(
                || Ok(reader.record_payload()?.map(|payload| payload.is_none())),
                pager.deref(),
            )
            .unwrap();
            assert!(missing);
            assert!(!reader.needs_restore());
        }
    }

    #[test]
    fn insert_preserves_peer_logical_position() {
        // Cursor2 sits on rowid=5; cursor1 inserts rowid=2 (causes
        // cells to shift right on the shared page). Without saveAllCursors,
        // cursor2's cell_idx now points to rowid=4. With the port, the
        // saved rowid=5 is restored on next access.
        let (pager, root_page, _db, _conn) = empty_btree();
        let mut writer = make_registered_cursor(&pager, root_page, 1);
        let mut peer = make_registered_cursor(&pager, root_page, 1);

        // Insert odd rowids so even slots are open for the peer-disrupting
        // insert below.
        for rowid in [1i64, 3, 5, 7, 9] {
            insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
        }
        run_until_done(
            || peer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();
        assert_eq!(
            run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
            Some(5)
        );

        insert_record(&mut writer, &pager, 2, Value::from_i64(2)).unwrap();

        assert_eq!(
            run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
            Some(5),
            "peer must observe its saved rowid after a left-of-it peer insert"
        );
    }
}

/// Strict property tests for page-level btree mutations.
///
/// These tests model expected cell bytes and check that every mutation
/// preserves both byte-level payload contents and page-layout invariants.
mod property_tests {
    use std::collections::HashSet;

    use quickcheck::{quickcheck, TestResult};

    use crate::storage::btree::{compute_free_space, defragment_page, drop_cell, insert_into_cell};
    use crate::storage::sqlite3_ondisk::{write_varint, PageContent, CELL_PTR_SIZE_BYTES};
    use crate::PageRef;

    use super::get_page;

    const PAGE_SIZE: usize = 4096;
    const MIN_INSERTED_CELLS: usize = 6;

    struct FillOutcome {
        expected: Vec<Vec<u8>>,
        had_middle_insert: bool,
    }

    /// Convert arbitrary fuzz bytes into bounded payload sizes that are small enough
    /// to produce many cells and varied freeblock behavior in a single page.
    fn normalize_sizes(raw: &[u8]) -> Vec<usize> {
        raw.iter()
            .take(300)
            .map(|v| ((*v as usize) % 220) + 1)
            .collect()
    }

    /// Validate strict page invariants after each mutation.
    ///
    /// Checks:
    /// - pointer array/header consistency:
    ///   `unallocated_region_start` must equal
    ///   `cell_pointer_array_offset + (cell_count * 2)`, so the header and pointer-array
    ///   metadata agree on where unallocated space begins.
    /// - structural bounds:
    ///   every cell and freeblock must lie fully within `[cell_content_area, usable_space)`.
    /// - pointer uniqueness:
    ///   no two cell-pointer entries may reference the same cell start offset.
    /// - interval non-overlap:
    ///   cell byte ranges and freeblock ranges must be disjoint; overlap means corruption.
    /// - freeblock chain validity:
    ///   the linked list must be strictly ascending by offset and must not contain cycles.
    /// - accounting equality:
    ///   independently computed free space from layout pieces must exactly equal
    ///   `compute_free_space(page, usable_space)`.
    /// - logical data preservation (optional):
    ///   when an expected model is provided, each on-page cell must match expected bytes
    ///   at the same logical index.
    fn strict_validate_page(
        page: &PageContent,
        usable_space: usize,
        expected_cells: Option<&[Vec<u8>]>,
    ) {
        let cell_count = page.cell_count();
        let cell_content_area = page.cell_content_area() as usize;
        let unallocated_start = page.unallocated_region_start();
        let ptr_start = page.cell_pointer_array_offset();
        let expected_unallocated_start = ptr_start + (cell_count * CELL_PTR_SIZE_BYTES);

        assert_eq!(
            unallocated_start, expected_unallocated_start,
            "unallocated region start inconsistent with cell pointer array"
        );
        assert!(
            unallocated_start <= cell_content_area,
            "cell pointer array overlaps cell content area"
        );
        assert!(
            cell_content_area <= usable_space,
            "cell content area beyond usable space"
        );
        assert!(
            page.num_frag_free_bytes() <= 60,
            "fragmented free bytes exceed SQLite limit"
        );

        let mut intervals = Vec::<(usize, usize, &'static str)>::new();
        let mut ptrs = HashSet::new();
        for i in 0..cell_count {
            let ptr_offset = ptr_start + (i * CELL_PTR_SIZE_BYTES);
            let raw_ptr = page.read_u16_no_offset(ptr_offset) as usize;
            let (start, len) = page.cell_get_raw_region(i, usable_space).unwrap();
            assert_eq!(
                raw_ptr, start,
                "cell pointer does not match parsed cell start"
            );
            assert!(len >= 2, "cell too small");
            assert!(
                start >= cell_content_area,
                "cell starts before cell content area"
            );
            assert!(
                start + len <= usable_space,
                "cell extends beyond usable space"
            );
            assert!(ptrs.insert(raw_ptr), "duplicate cell pointer");
            intervals.push((start, start + len, "cell"));
        }

        let mut freeblock_total = 0usize;
        let mut seen_freeblocks = HashSet::new();
        let mut cur = page.first_freeblock() as usize;
        let mut prev = 0usize;
        while cur != 0 {
            assert!(
                seen_freeblocks.insert(cur),
                "freeblock cycle detected at offset {cur}"
            );
            assert!(
                cur >= cell_content_area,
                "freeblock before cell content area"
            );
            assert!(cur + 4 <= usable_space, "freeblock header out of bounds");
            let (next, size_u16) = page.read_freeblock(cur as u16);
            let size = size_u16 as usize;
            assert!(size >= 4, "freeblock size too small");
            assert!(
                cur + size <= usable_space,
                "freeblock extends beyond usable space"
            );
            if prev != 0 {
                assert!(cur > prev, "freeblocks must be strictly ascending");
            }
            let next_usize = next as usize;
            if next_usize != 0 {
                assert!(next_usize > cur, "freeblock next pointer not ascending");
            }
            intervals.push((cur, cur + size, "freeblock"));
            freeblock_total += size;
            prev = cur;
            cur = next_usize;
        }

        intervals.sort_by_key(|(start, _, _)| *start);
        for pair in intervals.windows(2) {
            let (a_start, a_end, a_kind) = pair[0];
            let (b_start, _b_end, b_kind) = pair[1];
            assert!(
                a_end <= b_start,
                "interval overlap: {a_kind}@{a_start}..{a_end} overlaps {b_kind}@{b_start}"
            );
        }

        let computed = compute_free_space(page, usable_space).unwrap();
        let expected_free = (cell_content_area - unallocated_start)
            + page.num_frag_free_bytes() as usize
            + freeblock_total;
        assert_eq!(
            computed, expected_free,
            "compute_free_space mismatch: computed={computed}, expected={expected_free}"
        );

        if let Some(expected_cells) = expected_cells {
            assert_eq!(
                cell_count,
                expected_cells.len(),
                "cell count mismatch against expected model"
            );
            for (i, expected) in expected_cells.iter().enumerate() {
                let (start, len) = page.cell_get_raw_region(i, usable_space).unwrap();
                let actual = &page.as_ptr()[start..start + len];
                assert_eq!(
                    actual,
                    expected.as_slice(),
                    "cell bytes mismatch at idx {i}"
                );
            }
        }
    }

    /// Build a valid table-leaf cell:
    /// [payload_size varint][rowid varint][record(header + blob data)].
    ///
    /// The body is synthetic but stable, so byte-level equality checks are deterministic.
    fn make_table_leaf_cell(rowid: u64, data_size: usize) -> Vec<u8> {
        let mut cell = Vec::new();
        let serial_type = (data_size as u64) * 2 + 12;

        let mut header_buf = [0u8; 9];
        let mut serial_buf = [0u8; 9];
        let serial_len = write_varint(&mut serial_buf, serial_type);
        let header_size = 1 + serial_len;
        let header_size_len = write_varint(&mut header_buf, header_size as u64);

        let mut record = Vec::new();
        record.extend_from_slice(&header_buf[..header_size_len]);
        record.extend_from_slice(&serial_buf[..serial_len]);
        record.extend(vec![0xAB; data_size]);

        let payload_size = record.len() as u64;
        let mut payload_size_buf = [0u8; 9];
        let payload_size_len = write_varint(&mut payload_size_buf, payload_size);
        cell.extend_from_slice(&payload_size_buf[..payload_size_len]);

        let mut rowid_buf = [0u8; 9];
        let rowid_len = write_varint(&mut rowid_buf, rowid);
        cell.extend_from_slice(&rowid_buf[..rowid_len]);
        cell.extend_from_slice(&record);
        cell
    }

    /// Execute a modeled insertion workload against one page.
    ///
    /// For each insert that fits, mutate both:
    /// - the real page (via `insert_into_cell`), and
    /// - the expected model vector at the same index.
    ///
    /// `had_middle_insert` ensures we exercised pointer-shift paths, not only appends.
    fn fill_page_with_model(
        page: &PageRef,
        cell_sizes: &[usize],
        insert_hints: &[u8],
    ) -> FillOutcome {
        let mut expected = Vec::new();
        let mut had_middle_insert = false;
        let contents = page.get_contents();

        for (i, size) in cell_sizes.iter().copied().enumerate() {
            let cell = make_table_leaf_cell(i as u64, size);
            let free = compute_free_space(contents, PAGE_SIZE).unwrap();
            if cell.len() + CELL_PTR_SIZE_BYTES > free {
                continue;
            }

            let idx = if expected.is_empty() {
                0
            } else {
                insert_hints.get(i).copied().unwrap_or(i as u8) as usize % (expected.len() + 1)
            };
            if idx < expected.len() {
                had_middle_insert = true;
            }

            insert_into_cell(contents, &cell, idx, PAGE_SIZE).unwrap();
            expected.insert(idx, cell);
            strict_validate_page(contents, PAGE_SIZE, Some(&expected));
        }

        FillOutcome {
            expected,
            had_middle_insert,
        }
    }

    quickcheck! {
        // Invariant: arbitrary insert sequences (including middle inserts) preserve exact cell bytes
        // and keep page layout/accounting valid after every insertion.
        fn prop_insertions_preserve_exact_cell_bytes(
            raw_sizes: Vec<u8>,
            insert_hints: Vec<u8>
        ) -> TestResult {
            // Build many small payload sizes from random bytes so one page gets many edits.
            let cell_sizes = normalize_sizes(&raw_sizes);
            if cell_sizes.len() < MIN_INSERTED_CELLS {
                return TestResult::discard();
            }

            let page = get_page(2);
            // Mutate both the real page and the expected-model vector in lock-step.
            let outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
            // Require enough inserts and at least one middle insert (not append-only).
            if outcome.expected.len() < MIN_INSERTED_CELLS || !outcome.had_middle_insert {
                return TestResult::discard();
            }

            // Final strict check: metadata + free-space accounting + exact cell bytes.
            strict_validate_page(page.get_contents(), PAGE_SIZE, Some(&outcome.expected));
            TestResult::passed()
        }
    }

    quickcheck! {
        // Invariant: every drop operation removes exactly one modeled cell, never mutates surviving
        // cell bytes, and always preserves freeblock/pointer/free-space structural validity.
        fn prop_drop_sequence_preserves_model_and_layout(
            raw_sizes: Vec<u8>,
            insert_hints: Vec<u8>,
            drop_ops: Vec<u8>
        ) -> TestResult {
            if drop_ops.is_empty() {
                return TestResult::discard();
            }

            // Start from a non-trivial page state built with randomized inserts.
            let page = get_page(2);
            let cell_sizes = normalize_sizes(&raw_sizes);
            let mut outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
            if outcome.expected.len() < MIN_INSERTED_CELLS || !outcome.had_middle_insert {
                return TestResult::discard();
            }

            let contents = page.get_contents();
            let mut drops_executed = 0usize;
            for op in drop_ops.iter().take(200) {
                // Stop once the model is empty; there is nothing left to drop.
                if outcome.expected.is_empty() {
                    break;
                }
                // Drop same logical index in model and real page.
                let idx = (*op as usize) % outcome.expected.len();
                outcome.expected.remove(idx);
                drop_cell(contents, idx, PAGE_SIZE).unwrap();
                // After each mutation, validate structure and surviving bytes immediately.
                strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                drops_executed += 1;
            }

            if drops_executed == 0 {
                // Require at least one real mutation, otherwise this run is not informative.
                return TestResult::discard();
            }
            TestResult::passed()
        }
    }

    quickcheck! {
        // Invariant: after creating holes via drops, inserting new cells back into the page
        // preserves all existing bytes and keeps freeblock reuse/allocation safe.
        fn prop_insert_drop_insert_reuses_space_safely(
            raw_sizes: Vec<u8>,
            insert_hints: Vec<u8>,
            drop_ops: Vec<u8>,
            new_sizes: Vec<u8>,
            new_insert_hints: Vec<u8>
        ) -> TestResult {
            if drop_ops.is_empty() || new_sizes.is_empty() {
                return TestResult::discard();
            }

            let page = get_page(2);
            let cell_sizes = normalize_sizes(&raw_sizes);
            let mut outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
            if outcome.expected.len() < MIN_INSERTED_CELLS {
                return TestResult::discard();
            }

            let contents = page.get_contents();
            let mut drops_executed = 0usize;
            // Phase 1: create holes and freeblocks by dropping cells in random positions.
            for op in drop_ops.iter().take(16) {
                if outcome.expected.len() <= 2 {
                    break;
                }
                let idx = (*op as usize) % outcome.expected.len();
                outcome.expected.remove(idx);
                drop_cell(contents, idx, PAGE_SIZE).unwrap();
                strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                drops_executed += 1;
            }
            if drops_executed == 0 {
                return TestResult::discard();
            }

            let base_rowid = 1_000_000u64 + outcome.expected.len() as u64;
            let mut inserted = 0usize;
            // Phase 2: insert new cells back, forcing allocator/freeblock reuse paths.
            for (i, raw) in new_sizes.iter().take(32).enumerate() {
                let size = ((*raw as usize) % 220) + 1;
                let cell = make_table_leaf_cell(base_rowid + i as u64, size);
                let free = compute_free_space(contents, PAGE_SIZE).unwrap();
                if cell.len() + CELL_PTR_SIZE_BYTES > free {
                    continue;
                }
                let idx = if outcome.expected.is_empty() {
                    0
                } else {
                    new_insert_hints
                        .get(i)
                        .copied()
                        .unwrap_or(i as u8) as usize
                        % (outcome.expected.len() + 1)
                };
                insert_into_cell(contents, &cell, idx, PAGE_SIZE).unwrap();
                outcome.expected.insert(idx, cell);
                strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                inserted += 1;
            }

            if inserted == 0 {
                // Require at least one successful re-insert to exercise the target path.
                return TestResult::discard();
            }
            TestResult::passed()
        }
    }

    quickcheck! {
        // Invariant: full defragmentation is lossless (all live cell bytes unchanged), reaches canonical
        // no-freeblock/no-fragment state, and is idempotent when applied repeatedly.
        fn prop_defragment_is_lossless_and_idempotent(
            raw_sizes: Vec<u8>,
            insert_hints: Vec<u8>,
            drop_ops: Vec<u8>
        ) -> TestResult {
            if drop_ops.is_empty() {
                return TestResult::discard();
            }

            let page = get_page(2);
            let cell_sizes = normalize_sizes(&raw_sizes);
            let mut outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
            if outcome.expected.len() < MIN_INSERTED_CELLS {
                // Need enough cells so one drop still leaves a meaningful page state.
                return TestResult::discard();
            }

            let contents = page.get_contents();
            let mut drops_executed = 0usize;
            for op in drop_ops.iter().take(40) {
                if outcome.expected.len() <= 1 {
                    break;
                }
                // Create realistic holes/freeblocks before defragmenting.
                let idx = (*op as usize) % outcome.expected.len();
                outcome.expected.remove(idx);
                drop_cell(contents, idx, PAGE_SIZE).unwrap();
                strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                drops_executed += 1;
            }

            if drops_executed == 0 || outcome.expected.is_empty() {
                return TestResult::discard();
            }

            // First defrag: must preserve live cells and clean freeblock/fragment metadata.
            defragment_page(contents, PAGE_SIZE, -1).unwrap();
            strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
            assert_eq!(contents.first_freeblock(), 0, "freeblocks remain after defrag");
            assert_eq!(contents.num_frag_free_bytes(), 0, "fragments remain after defrag");

            // Second defrag should be a no-op on bytes (idempotence).
            let snapshot_after_first = contents.as_ptr().to_vec();
            defragment_page(contents, PAGE_SIZE, -1).unwrap();
            strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
            assert_eq!(
                contents.as_ptr().to_vec(),
                snapshot_after_first,
                "defragmentation is not idempotent"
            );
            TestResult::passed()
        }
    }

    quickcheck! {
        // Invariant: for simple freeblock layouts where fast-path is applicable, fast defrag and
        // full defrag produce the same logical page state and identical serialized cell bytes.
        fn prop_defragment_fast_matches_full(
            raw_sizes: Vec<u8>,
            insert_hints: Vec<u8>,
            drop_op: u8
        ) -> TestResult {
            let cell_sizes = normalize_sizes(&raw_sizes);
            if cell_sizes.len() < MIN_INSERTED_CELLS {
                return TestResult::discard();
            }

            let page_fast = get_page(2);
            let mut outcome = fill_page_with_model(&page_fast, &cell_sizes, &insert_hints);
            if outcome.expected.len() < MIN_INSERTED_CELLS {
                return TestResult::discard();
            }

            // Clone logical state to second page so both start identical.
            let page_full = get_page(3);
            let full_contents = page_full.get_contents();
            for (i, cell) in outcome.expected.iter().enumerate() {
                insert_into_cell(full_contents, cell, i, PAGE_SIZE).unwrap();
            }
            strict_validate_page(full_contents, PAGE_SIZE, Some(&outcome.expected));

            // Create a single hole => one freeblock, making fast-path eligibility likely.
            let idx = drop_op as usize % outcome.expected.len();
            let fast_contents = page_fast.get_contents();
            outcome.expected.remove(idx);
            drop_cell(fast_contents, idx, PAGE_SIZE).unwrap();
            drop_cell(full_contents, idx, PAGE_SIZE).unwrap();
            strict_validate_page(fast_contents, PAGE_SIZE, Some(&outcome.expected));
            strict_validate_page(full_contents, PAGE_SIZE, Some(&outcome.expected));

            // Try fast-path on one page and force full-path on the other.
            defragment_page(fast_contents, PAGE_SIZE, 4).unwrap();
            defragment_page(full_contents, PAGE_SIZE, -1).unwrap();

            // Both algorithms must preserve the exact same logical model.
            strict_validate_page(fast_contents, PAGE_SIZE, Some(&outcome.expected));
            strict_validate_page(full_contents, PAGE_SIZE, Some(&outcome.expected));
            assert_eq!(fast_contents.cell_count(), full_contents.cell_count());
            assert_eq!(
                compute_free_space(fast_contents, PAGE_SIZE).unwrap(),
                compute_free_space(full_contents, PAGE_SIZE).unwrap()
            );
            assert_eq!(fast_contents.first_freeblock(), full_contents.first_freeblock());
            assert_eq!(
                fast_contents.num_frag_free_bytes(),
                full_contents.num_frag_free_bytes()
            );

            for i in 0..fast_contents.cell_count() {
                let (s1, l1) = fast_contents.cell_get_raw_region(i, PAGE_SIZE).unwrap();
                let (s2, l2) = full_contents.cell_get_raw_region(i, PAGE_SIZE).unwrap();
                assert_eq!(l1, l2, "cell {i} length mismatch after defragmentation");
                assert_eq!(
                    &fast_contents.as_ptr()[s1..s1 + l1],
                    &full_contents.as_ptr()[s2..s2 + l2],
                    "cell {i} bytes mismatch between fast and full defrag"
                );
            }
            TestResult::passed()
        }
    }

    quickcheck! {
        // Invariant: dropping all cells and defragmenting returns the page to a canonical empty state
        // (zero cells, no fragments/freeblocks, content area at end, exact free-space accounting).
        fn prop_drop_all_then_defrag_returns_canonical_empty_page(
            raw_sizes: Vec<u8>,
            insert_hints: Vec<u8>
        ) -> TestResult {
            let page = get_page(2);
            let cell_sizes = normalize_sizes(&raw_sizes);
            let mut outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
            if outcome.expected.len() < MIN_INSERTED_CELLS {
                return TestResult::discard();
            }

            let contents = page.get_contents();
            while !outcome.expected.is_empty() {
                // Repeatedly drop from the logical front so model and page stay aligned.
                outcome.expected.remove(0);
                drop_cell(contents, 0, PAGE_SIZE).unwrap();
                strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
            }

            // After all cells are gone, defrag should normalize page to canonical empty form.
            defragment_page(contents, PAGE_SIZE, -1).unwrap();
            strict_validate_page(contents, PAGE_SIZE, Some(&[]));
            assert_eq!(contents.cell_count(), 0);
            assert_eq!(contents.first_freeblock(), 0);
            assert_eq!(contents.num_frag_free_bytes(), 0);
            assert_eq!(contents.cell_content_area() as usize, PAGE_SIZE);
            assert_eq!(
                compute_free_space(contents, PAGE_SIZE).unwrap(),
                PAGE_SIZE - contents.header_size(),
                "empty page must expose full free space minus header"
            );
            TestResult::passed()
        }
    }
}

/// Corruption-handling properties.
///
/// These tests verify that malformed on-page metadata is rejected with
/// corruption errors, instead of silently succeeding or panicking.
mod corruption_properties {
    use quickcheck::quickcheck;

    use crate::storage::btree::{compute_free_space, defragment_page, insert_into_cell};
    use crate::storage::sqlite3_ondisk::write_varint;

    use super::get_page;

    const PAGE_SIZE: usize = 4096;

    fn make_table_leaf_cell(rowid: u64, data_size: usize) -> Vec<u8> {
        let mut cell = Vec::new();
        let serial_type = (data_size as u64) * 2 + 12;

        let mut header_buf = [0u8; 9];
        let mut serial_buf = [0u8; 9];
        let serial_len = write_varint(&mut serial_buf, serial_type);
        let header_size = 1 + serial_len;
        let header_size_len = write_varint(&mut header_buf, header_size as u64);

        let mut record = Vec::new();
        record.extend_from_slice(&header_buf[..header_size_len]);
        record.extend_from_slice(&serial_buf[..serial_len]);
        record.extend(vec![0xCC; data_size]);

        let payload_size = record.len() as u64;
        let mut payload_size_buf = [0u8; 9];
        let payload_size_len = write_varint(&mut payload_size_buf, payload_size);
        cell.extend_from_slice(&payload_size_buf[..payload_size_len]);

        let mut rowid_buf = [0u8; 9];
        let rowid_len = write_varint(&mut rowid_buf, rowid);
        cell.extend_from_slice(&rowid_buf[..rowid_len]);
        cell.extend_from_slice(&record);
        cell
    }

    quickcheck! {
        // Desired invariant: malformed freeblock pointer values should return Corrupt errors.
        fn prop_compute_free_space_returns_err_when_first_freeblock_is_invalid(seed: u16) -> bool {
            let page = get_page(2);
            let contents = page.get_contents();
            let bad_ptr = ((seed as usize % (PAGE_SIZE - 1)) + 1) as u16; // 1..=4095, always < initial cell_content_area (4096)
            contents.write_first_freeblock(bad_ptr);
            compute_free_space(contents, PAGE_SIZE).is_err()
        }
    }

    quickcheck! {
        // Desired invariant: malformed freeblock chain ordering should return Corrupt errors.
        fn prop_compute_free_space_returns_err_on_malformed_freeblock_chain(seed: u16) -> bool {
            let page = get_page(2);
            let contents = page.get_contents();

            // Move content area left so freeblocks can exist "inside content area".
            contents.write_cell_content_area(64);

            // Create one freeblock whose "next" pointer violates ordering assumptions.
            let base = 128 + (seed as usize % (PAGE_SIZE - 256));
            let cur = base as u16;
            let next = (base + 1) as u16; // intentionally invalid relative to size constraints
            contents.write_first_freeblock(cur);
            contents.write_freeblock(cur, 8, Some(next));
            compute_free_space(contents, PAGE_SIZE).is_err()
        }
    }

    quickcheck! {
        // Desired invariant: malformed freeblock metadata should return Corrupt errors.
        fn prop_defragment_returns_err_on_malformed_freeblock_chain(seed: u8) -> bool {
            let page = get_page(2);
            let contents = page.get_contents();

            // Ensure page is non-empty so defragmentation doesn't early-return.
            let cell = make_table_leaf_cell(1, (seed as usize % 24) + 1);
            if insert_into_cell(contents, &cell, 0, PAGE_SIZE).is_err() {
                return true;
            }

            // Construct malformed chain: first freeblock points "backwards".
            contents.write_first_freeblock(100);
            contents.write_freeblock(100, 8, Some(90));
            defragment_page(contents, PAGE_SIZE, 4).is_err()
        }
    }
}

/// `next()` past the last row must leave the cursor exactly on the
/// append slot the MVCC checkpoint's sequential-write optimization
/// inserts into: end of the rightmost leaf, one past the last cell,
/// with no ancestor holding a child to the right. The tree is grown
/// past one leaf so the predicate has ancestors to check.
#[test]
fn next_past_the_last_row_lands_on_the_rightmost_leaf_append_slot() {
    let (pager, root_page, _db, _conn) = empty_btree();
    let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, 1);
    let cursor = &mut cursor;

    let payload = crate::alloc::vec![b'X'; 512];
    let regs = &[Register::Value(Value::Blob(payload))];
    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

    for rowid in 1..=64 {
        run_until_done(
            || cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }),
            pager.deref(),
        )
        .unwrap();
        let key = BTreeKey::new_table_rowid(rowid, Some(&record));
        run_until_done(|| cursor.insert(&key), pager.deref()).unwrap();
    }

    run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
    assert!(
        !cursor.is_at_end_of_rightmost_leaf(),
        "a cursor on the first row is not at the append slot"
    );

    let mut rows = 0;
    while cursor.has_record {
        rows += 1;
        run_until_done(|| cursor.next(), pager.deref()).unwrap();
    }
    assert_eq!(rows, 64);
    assert!(
        cursor.is_at_end_of_rightmost_leaf(),
        "next() past the last row must land on the append slot"
    );

    // Inserting the next consecutive rowid at that position must append.
    let key = BTreeKey::new_table_rowid(65, Some(&record));
    run_until_done(|| cursor.insert(&key), pager.deref()).unwrap();

    run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
    let mut rowids = crate::alloc::vec![];
    while cursor.has_record {
        let rowid = run_until_done(|| cursor.rowid(), pager.deref())
            .unwrap()
            .unwrap();
        rowids.push(rowid);
        run_until_done(|| cursor.next(), pager.deref()).unwrap();
    }
    assert_eq!(rowids, (1..=65).collect::<Vec<i64>>());
}
