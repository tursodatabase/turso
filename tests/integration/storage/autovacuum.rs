use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO};

fn open_autovacuum_db(db_path: &std::path::Path) -> Arc<Database> {
    let io = Arc::new(PlatformIO::new().unwrap()) as Arc<dyn turso_core::IO>;
    let opts = DatabaseOpts::new().with_autovacuum(true);
    Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
    )
    .unwrap()
}

/// Validate ptrmap correctness using SQLite's own integrity_check.
///
/// Why rusqlite, not Turso's `PRAGMA integrity_check`:
///   Turso's integrity_check (btree.rs::integrity_check) validates B-tree
///   structure — cell ordering, depth, overlap — but does not read or validate
///   the ptrmap. The ptrmap could be entirely wrong and Turso's check returns "ok".
///
///   rusqlite runs SQLite's checkTreePage which calls checkPtrmap for every
///   child pointer in every interior page. That is the only check that catches
///   the bug this PR fixes.
///
/// Why dropping Turso before opening rusqlite is safe:
///   Connection::drop does not checkpoint. The WAL file remains on disk with
///   committed frames. Turso uses standard SQLite WAL format (magic 0x377f0682),
///   so rusqlite opens the main db + reads the WAL directly in read-only mode,
///   seeing all committed data without needing to write a WAL index.
fn assert_ptrmap_ok_via_rusqlite(db_path: &std::path::Path) {
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("rusqlite could not open db for integrity check");

    let result: String = conn
        .pragma_query_value(None, "integrity_check", |row| row.get(0))
        .expect("integrity_check pragma failed");

    assert_eq!(
        result, "ok",
        "ptrmap corrupt (rusqlite integrity_check failed): {}",
        result
    );
}

/// Set up a fresh auto_vacuum=FULL WAL database via rusqlite.
///
/// rusqlite is used for setup because it correctly initialises the database
/// header fields that mark it as auto-vacuum (largest_root_page > 0).
/// Turso then respects this when opened with `with_autovacuum(true)`.
fn make_autovacuum_db(page_size: u32, extra_ddl: &str) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.pragma_update(None, "page_size", page_size).unwrap();
        conn.pragma_update(None, "auto_vacuum", "FULL").unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        if !extra_ddl.is_empty() {
            conn.execute_batch(extra_ddl).unwrap();
        }
    }
    (dir, path)
}

#[test]
fn test_autovacuum_readonly_behavior() {
    // (autovacuum_mode, enable_autovacuum_flag, expected_readonly)
    // TODO: Add encrypted case ("NONE", false, false) after fixing https://github.com/tursodatabase/turso/issues/4519
    let test_cases = [
        ("NONE", false, false),
        ("NONE", true, false),
        ("FULL", false, true),
        ("FULL", true, false),
        ("INCREMENTAL", false, true),
        ("INCREMENTAL", true, false),
    ];

    for (autovacuum_mode, enable_autovacuum_flag, expected_readonly) in test_cases {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "auto_vacuum", autovacuum_mode)
                .unwrap();
        }

        let io = Arc::new(PlatformIO::new().unwrap()) as Arc<dyn turso_core::IO>;
        let opts = DatabaseOpts::new().with_autovacuum(enable_autovacuum_flag);
        let db = Database::open_file_with_flags(
            io,
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
        )
        .unwrap();

        assert_eq!(
            db.is_readonly(),
            expected_readonly,
            "autovacuum={autovacuum_mode}, flag={enable_autovacuum_flag}: expected readonly={expected_readonly}"
        );
    }
}

// ─── ptrmap correctness tests ─────────────────────────────────────────────────
//
// Pattern for every test:
//   1. Insert rows via Turso to trigger a specific balance path.
//      Each do_allocate_page call in that path must be followed by a ptrmap_put
//      call — that is exactly what this PR adds.
//   2. Drop the Turso connection. WAL frames remain on disk (no checkpoint).
//   3. Open the same file with rusqlite (read-only) and run integrity_check.
//      SQLite's checkTreePage validates every ptrmap entry via checkPtrmap.
//      A missing or stale entry produces:
//        "Bad ptr map entry key=N expected=(5,P) got=(5,0)"
//
// Page size 512 means ~13 rows per leaf at 30-char payloads, so splits happen
// quickly and all balance paths fire within a small number of inserts.

/// balance_root: root page overflows → one new child page allocated.
///
/// SQLite (balance_deeper):
///   allocateBtreePage(pBt, &pChild, &pgnoChild, pRoot->pgno, 0)
///   ptrmapPut(pBt, pgnoChild, PTRMAP_BTREE, pRoot->pgno, &rc)
///
/// balance_root() captures child_id and root_id before any IO,
/// returns them to the dispatcher. The BalanceRootPtrmap sub-state calls
/// update_ptrmap(child_id, root_id) in a separate resumable step so an IO
/// yield cannot cause do_allocate_page to run a second time.
#[test]
fn test_ptrmap_updated_after_balance_root() {
    let (_dir, path) = make_autovacuum_db(512, "");
    {
        let db = open_autovacuum_db(&path);
        let conn = db.connect().unwrap();
        // Root splits at row ~13; 50 rows ensures balance_root fires.
        for i in 0..50i64 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'payload_{i:0>32}');"))
                .unwrap();
        }
    }
    assert_ptrmap_ok_via_rusqlite(&path);
}

/// balance_quick: rightmost leaf overflows → one new leaf allocated to the right.
///
/// SQLite (balance_quick):
///   allocateBtreePage(pBt, &pNew, &pgnoNew, 0, 0)
///   ptrmapPut(pBt, pgnoNew, PTRMAP_BTREE, pParent->pgno, &rc)
///
/// balance_quick() captures new_leaf_id and parent_id, sets
/// QuickPtrmap sub-state and returns. The dispatcher calls update_ptrmap,
/// then balance_quick_finish() does the cell work. This separates allocation
/// from the IO-capable ptrmap call, making re-entry safe.
///
/// Sequential ascending rowids always hit this path: the overflow cell is
/// always the last cell, which is the only condition balance_quick checks.
#[test]
fn test_ptrmap_updated_after_balance_quick() {
    let (_dir, path) = make_autovacuum_db(512, "");
    {
        let db = open_autovacuum_db(&path);
        let conn = db.connect().unwrap();
        // 200 sequential rows: balance_quick fires ~14 times.
        for i in 0..200i64 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');"
            ))
            .unwrap();
        }
    }
    assert_ptrmap_ok_via_rusqlite(&path);
}

/// balance_non_root: general rebalancing allocates new sibling pages.
///
/// SQLite (balance_nonroot, apNew[] allocation loop):
///   if( i >= nOld ) {
///     allocateBtreePage(pBt, &apNew[i], &pgno, 0, 0)
///     ptrmapPut(pBt, pgno, PTRMAP_BTREE, pParent->pgno, &rc)
///   }
///
/// NonRootDoBalancingAllocate stores the page in pages_to_balance_new[i]
/// BEFORE calling ptrmap_put. The slot_already_allocated guard (is_none() check)
/// prevents do_allocate_page from running again if ptrmap_put yields IO and
/// the state machine re-enters this arm.
///
/// Descending rowids bypass balance_quick (overflow cell is never the last)
/// and always take the balance_non_root path.
#[test]
fn test_ptrmap_updated_after_balance_non_root() {
    let (_dir, path) = make_autovacuum_db(512, "");
    {
        let db = open_autovacuum_db(&path);
        let conn = db.connect().unwrap();
        for i in (0..200i64).rev() {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');"
            ))
            .unwrap();
        }
    }
    assert_ptrmap_ok_via_rusqlite(&path);
}

/// balance_non_root on interior pages: sites 2 and 3.
///
/// When interior pages themselves split, ptrmap must be updated for:
///
///   Site 2 (SQLite balance_nonroot ): left-child of each moved interior cell
///     ptrmapPut(pBt, left_child, PTRMAP_BTREE, new_sibling->pgno, &rc)
///
///   Site 3 (SQLite balance_nonroot ): right-child of each new sibling
///     ptrmapPut(pBt, right_child, PTRMAP_BTREE, new_sibling->pgno, &rc)
///
/// ptrmap_cell_idx (site 2) and ptrmap_sibling_idx (site 3) are
/// resume cursors stored in BalanceContext. Each is advanced BEFORE the
/// ptrmap_put call so a mid-loop IO yield skips already-done entries on
/// re-entry rather than repeating them.
///
/// An index forces interior cells to carry key payload inline, so interior
/// pages fill quickly at page_size=512 and the interior level itself
/// triggers balance_non_root, exercising sites 2 and 3.
#[test]
fn test_ptrmap_updated_after_interior_page_rebalance() {
    let (_dir, path) = make_autovacuum_db(512, "CREATE INDEX t_val ON t(val);");
    {
        let db = open_autovacuum_db(&path);
        let conn = db.connect().unwrap();
        for i in (0..300i64).rev() {
            let val = format!("key_{i:0>40}_{}", "x".repeat(20));
            conn.execute(&format!("INSERT INTO t VALUES ({i}, '{val}');"))
                .unwrap();
        }
    }
    assert_ptrmap_ok_via_rusqlite(&path);
}

/// All three balance paths in one pass.
///
/// Ascending phase triggers balance_root (first root overflow) and balance_quick
/// (every subsequent rightmost overflow). Descending phase inserts into the gaps,
/// triggering balance_non_root for middle-of-tree splits. All five ptrmap write
/// sites are exercised in a single test.
#[test]
fn test_ptrmap_updated_after_mixed_inserts() {
    let (_dir, path) = make_autovacuum_db(512, "");
    {
        let db = open_autovacuum_db(&path);
        let conn = db.connect().unwrap();
        for i in 0..100i64 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'cccccccccccccccccccccccccccccccccccc');"
            ))
            .unwrap();
        }
        for i in (100..200i64).rev() {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'dddddddddddddddddddddddddddddddddddd');"
            ))
            .unwrap();
        }
    }
    assert_ptrmap_ok_via_rusqlite(&path);
}

/// Ptrmap entries survive a delete + rebalance cycle.
///
/// Deleting rows causes underflow which triggers balance_non_root to merge
/// pages. Freed pages move to the freelist with ptrmap type FreePage.
/// Surviving pages keep their BTreeNode entries with updated parent pointers.
/// rusqlite's integrity_check catches any entry that was not updated after
/// a merge reassigned a page to a different sibling.
#[test]
fn test_ptrmap_correct_after_delete_and_rebalance() {
    let (_dir, path) = make_autovacuum_db(512, "");
    {
        let db = open_autovacuum_db(&path);
        let conn = db.connect().unwrap();
        for i in 0..200i64 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee');"
            ))
            .unwrap();
        }
        for i in (0..200i64).step_by(2) {
            conn.execute(&format!("DELETE FROM t WHERE id = {i};"))
                .unwrap();
        }
    }
    assert_ptrmap_ok_via_rusqlite(&path);
}

