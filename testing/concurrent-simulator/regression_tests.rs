#![cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]

use std::path::Path;
use std::sync::Arc;
use turso_core::{Connection, Database, DatabaseOpts, IO, LimboError, OpenFlags};
use turso_whopper::multiprocess::{MultiprocessOpts, MultiprocessWhopper};
use turso_whopper::{
    multiprocess_platform_io,
    workloads::{
        BeginWorkload, CommitWorkload, CreateIndexWorkload, CreateSimpleTableWorkload,
        DeleteWorkload, DropIndexWorkload, IntegrityCheckWorkload, RollbackWorkload,
        SimpleInsertWorkload, SimpleSelectWorkload, UpdateWorkload, WalCheckpointWorkload,
    },
};

fn wait_for_file(path: &Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("timed out waiting for {}", path.display());
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn multiprocess_test_io() -> Arc<dyn IO> {
    multiprocess_platform_io().expect("multiprocess io")
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn multiprocess_wal_db_opts() -> DatabaseOpts {
    DatabaseOpts::new().with_multiprocess_wal(true)
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn flip_db_header_reserved_byte(path: &Path) {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open db file for header mutation");

    #[cfg(feature = "checksum")]
    {
        let mut page = vec![0u8; 4096];
        file.read_exact(&mut page).expect("read page 1");
        page[72] ^= 0x01;
        // Recompute the page checksum (last 8 bytes) so the
        // page-level integrity check passes and the test exercises
        // the tshm proof validation path rather than crashing early.
        let checksum = twox_hash::XxHash3_64::oneshot(&page[..4088]);
        page[4088..].copy_from_slice(&checksum.to_le_bytes());
        file.seek(SeekFrom::Start(0)).expect("seek to start");
        file.write_all(&page).expect("write mutated page 1");
    }

    #[cfg(not(feature = "checksum"))]
    {
        file.seek(SeekFrom::Start(72))
            .expect("seek to reserved header byte");
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte)
            .expect("read reserved header byte");
        byte[0] ^= 0x01;
        file.seek(SeekFrom::Start(72))
            .expect("seek to reserved header byte for write");
        file.write_all(&byte).expect("write mutated header byte");
    }

    file.sync_all().expect("sync db header mutation");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn configure_worker_exe() {
    unsafe {
        std::env::set_var(
            "TURSO_WHOPPER_WORKER_EXE",
            env!("CARGO_BIN_EXE_turso_whopper"),
        );
    }
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn create_multiprocess_whopper(max_connections: usize) -> MultiprocessWhopper {
    create_multiprocess_whopper_with_keep(max_connections, false)
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn create_multiprocess_whopper_with_shape(
    process_count: usize,
    connections_per_process: usize,
) -> MultiprocessWhopper {
    create_multiprocess_whopper_with_shape_and_keep(process_count, connections_per_process, false)
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn create_multiprocess_whopper_with_keep(
    max_connections: usize,
    keep_files: bool,
) -> MultiprocessWhopper {
    create_multiprocess_whopper_with_shape_and_keep(max_connections, 1, keep_files)
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn create_multiprocess_whopper_with_shape_and_keep(
    process_count: usize,
    connections_per_process: usize,
    keep_files: bool,
) -> MultiprocessWhopper {
    configure_worker_exe();
    MultiprocessWhopper::new(MultiprocessOpts {
        seed: Some(7),
        enable_mvcc: false,
        process_count,
        connections_per_process,
        max_steps: 0,
        elle_tables: vec![],
        workloads: vec![],
        properties: vec![],
        chaotic_profiles: vec![],
        kill_probability: 0.0,
        restart_probability: 0.0,
        history_output: None,
        keep_files,
    })
    .expect("create multiprocess whopper")
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn populate_blob_test_rows(whopper: &mut MultiprocessWhopper) {
    whopper
        .disable_auto_checkpoint_direct(0)
        .expect("disable auto checkpoint");
    whopper
        .execute_sql_direct(0, "create table test(id integer primary key, value blob)")
        .expect("create table")
        .expect("create table should succeed");
    whopper
        .execute_sql_direct(0, "begin immediate")
        .expect("begin write transaction")
        .expect("begin should succeed");
    for _ in 0..32 {
        whopper
            .execute_sql_direct(0, "insert into test(value) values (randomblob(2048))")
            .expect("insert blob row")
            .expect("insert should succeed");
    }
    whopper
        .execute_sql_direct(0, "commit")
        .expect("commit transaction")
        .expect("commit should succeed");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn create_partial_checkpoint_state(whopper: &mut MultiprocessWhopper) -> (u64, u64) {
    populate_blob_test_rows(whopper);

    let snapshot_before_checkpoint = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read shared WAL snapshot before checkpoint")
        .expect("shared WAL snapshot should be available");
    assert!(
        snapshot_before_checkpoint.max_frame > 1,
        "partial-checkpoint restart coverage requires more than one WAL frame before checkpointing"
    );

    whopper
        .passive_checkpoint_direct(0, Some(1))
        .expect("run partial checkpoint");

    let snapshot_after_checkpoint = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read shared WAL snapshot after checkpoint")
        .expect("shared WAL snapshot should be available");
    assert!(
        snapshot_after_checkpoint.nbackfills > 0,
        "partial-checkpoint restart coverage requires positive nbackfills"
    );
    assert!(
        snapshot_after_checkpoint.max_frame > snapshot_after_checkpoint.nbackfills,
        "partial-checkpoint restart coverage requires live WAL frames past the backfill point"
    );

    (
        snapshot_after_checkpoint.max_frame,
        snapshot_after_checkpoint.nbackfills,
    )
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn count_test_rows(whopper: &mut MultiprocessWhopper, worker_idx: usize) -> i64 {
    for _ in 0..32 {
        let rows = whopper
            .execute_sql_direct(worker_idx, "select count(*) from test")
            .expect("count rows");
        match rows {
            Ok(rows) => return rows[0][0].as_int().expect("count should be integer"),
            Err(
                LimboError::SchemaUpdated
                | LimboError::SchemaConflict
                | LimboError::Busy
                | LimboError::BusySnapshot
                | LimboError::TableLocked,
            ) => continue,
            Err(err) => panic!("count should succeed: {err}"),
        }
    }
    panic!("count rows did not stabilize after transient multiprocess errors");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn truncate_checkpoint_until_stable(whopper: &mut MultiprocessWhopper, connection_idx: usize) {
    for _ in 0..32 {
        let result = whopper
            .execute_sql_direct(connection_idx, "PRAGMA wal_checkpoint(TRUNCATE)")
            .expect("run TRUNCATE checkpoint");
        match result {
            Ok(_) => return,
            Err(
                LimboError::Busy
                | LimboError::BusySnapshot
                | LimboError::SchemaUpdated
                | LimboError::SchemaConflict
                | LimboError::TableLocked,
            ) => continue,
            Err(err) => panic!("TRUNCATE checkpoint should stabilize: {err}"),
        }
    }
    panic!("TRUNCATE checkpoint did not stabilize after transient multiprocess errors");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn assert_integrity_check_ok(whopper: &mut MultiprocessWhopper, worker_idx: usize) {
    let rows = whopper
        .execute_sql_direct(worker_idx, "PRAGMA integrity_check")
        .expect("run integrity_check")
        .expect("integrity_check should succeed");
    assert_eq!(rows.len(), 1, "integrity_check should return one row");
    assert_eq!(
        rows[0].len(),
        1,
        "integrity_check row should contain one column"
    );
    assert_eq!(
        rows[0][0].to_text(),
        Some("ok"),
        "integrity_check should report ok"
    );
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_same_process_sibling_reader_keeps_shared_snapshot_live_until_last_release() {
    let mut whopper = create_multiprocess_whopper_with_shape(2, 2);

    for connection_idx in 0..4 {
        whopper
            .disable_auto_checkpoint_direct(connection_idx)
            .expect("disable auto checkpoint");
    }

    whopper
        .execute_sql_direct(
            0,
            "create table test(id integer primary key, value text not null)",
        )
        .expect("create table")
        .expect("create table should succeed");
    whopper
        .execute_sql_direct(2, "insert into test(value) values ('base')")
        .expect("insert base row")
        .expect("base insert should succeed");

    let initial_snapshot = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read initial shared WAL snapshot")
        .expect("shared WAL snapshot should exist");
    assert!(
        initial_snapshot.max_frame > 0,
        "shared reader-slot regression requires visible WAL frames before read transactions begin"
    );

    for connection_idx in [0usize, 1usize] {
        whopper
            .execute_sql_direct(connection_idx, "begin")
            .expect("begin read transaction")
            .expect("begin should succeed");
        assert_eq!(
            count_test_rows(&mut whopper, connection_idx),
            1,
            "same-process sibling readers should share the initial snapshot before the second writer commits",
        );
    }

    whopper
        .execute_sql_direct(2, "insert into test(value) values ('new')")
        .expect("insert new row")
        .expect("second insert should succeed");

    let post_append_snapshot = whopper
        .shared_wal_snapshot_direct(2)
        .expect("read post-append shared WAL snapshot")
        .expect("shared WAL snapshot should exist after append");
    assert!(
        post_append_snapshot.max_frame > initial_snapshot.max_frame,
        "second writer should advance the authoritative WAL tail"
    );

    whopper
        .execute_sql_direct(0, "rollback")
        .expect("rollback first sibling reader")
        .expect("rollback should succeed");

    let truncate_while_reader_active = whopper
        .execute_sql_direct(3, "PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("attempt TRUNCATE checkpoint while sibling reader is active");
    match truncate_while_reader_active {
        Ok(_) => {}
        Err(
            LimboError::Busy
            | LimboError::BusySnapshot
            | LimboError::SchemaUpdated
            | LimboError::SchemaConflict
            | LimboError::TableLocked,
        ) => {}
        Err(err) => panic!("unexpected TRUNCATE result while sibling reader is active: {err}"),
    }

    let snapshot_while_reader_active = whopper
        .shared_wal_snapshot_direct(3)
        .expect("read shared WAL snapshot while sibling reader is active")
        .expect("shared WAL snapshot should exist while sibling reader is active");
    assert!(
        snapshot_while_reader_active.max_frame > 0,
        "one same-process sibling must keep the shared WAL generation live after the other releases",
    );
    assert_eq!(
        count_test_rows(&mut whopper, 1),
        1,
        "remaining same-process sibling reader must keep its original snapshot after the other connection rolls back",
    );

    whopper
        .execute_sql_direct(1, "rollback")
        .expect("rollback second sibling reader")
        .expect("rollback should succeed");

    truncate_checkpoint_until_stable(&mut whopper, 3);
    let snapshot_after_release = whopper
        .shared_wal_snapshot_direct(3)
        .expect("read shared WAL snapshot after both readers release")
        .expect("shared WAL snapshot should exist after both readers release");
    assert_eq!(
        snapshot_after_release.max_frame, 0,
        "TRUNCATE should only reset the WAL generation after the last same-process sibling reader releases",
    );
    assert_eq!(
        count_test_rows(&mut whopper, 2),
        2,
        "writer process should observe both committed rows after the shared reader snapshot is released",
    );

    whopper.finalize().expect("finalize multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn count_rows_in_table(conn: &Arc<Connection>, table_name: &str) -> i64 {
    let mut stmt = conn
        .prepare(format!("select count(*) from {table_name}"))
        .expect("prepare count");
    let mut count = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).expect("count column");
        Ok(())
    })
    .expect("run count");
    count
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn read_simple_kv_length(db_path: &Path, table_name: &str, key: &str) -> Option<i64> {
    let io: Arc<dyn IO> = multiprocess_test_io();
    let reopened = Database::open_file_with_flags(
        io,
        db_path.to_str().expect("db path utf8"),
        OpenFlags::ReadOnly,
        multiprocess_wal_db_opts(),
        None,
    )
    .expect("open observer database");
    let conn = reopened.connect().expect("connect observer db");
    let sql = format!("select length(value) from {table_name} where key='{key}'");
    for _ in 0..32 {
        let mut stmt = match conn.prepare(sql.clone()) {
            Ok(stmt) => stmt,
            Err(LimboError::SchemaUpdated | LimboError::SchemaConflict) => {
                conn.maybe_reparse_schema()
                    .expect("observer reparse after schema change");
                continue;
            }
            Err(LimboError::Busy | LimboError::BusySnapshot | LimboError::TableLocked) => {
                continue;
            }
            Err(err) => panic!("observer prepare should succeed: {err}"),
        };
        let mut result = None;
        match stmt.run_with_row_callback(|row| {
            result = Some(row.get::<i64>(0).expect("length column"));
            Ok(())
        }) {
            Ok(()) => return result,
            Err(LimboError::SchemaUpdated | LimboError::SchemaConflict) => {
                drop(stmt);
                conn.maybe_reparse_schema()
                    .expect("observer reparse after schema change");
                continue;
            }
            Err(LimboError::Busy | LimboError::BusySnapshot | LimboError::TableLocked) => {
                continue;
            }
            Err(err) => panic!("observer query should succeed: {err}"),
        }
    }
    panic!("observer query did not stabilize after transient multiprocess errors");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn probe_optional_int_via_fresh_worker(whopper: &MultiprocessWhopper, sql: String) -> Option<i64> {
    for _ in 0..8 {
        let (_startup, result) = whopper
            .execute_sql_via_fresh_worker(sql.clone())
            .expect("probe via fresh worker should succeed");
        match result {
            Ok(rows) => {
                return match rows.as_slice() {
                    [] => None,
                    [row] => Some(row[0].as_int().expect("probe column should be integer")),
                    _ => panic!("probe query should return at most one row, got {rows:?}"),
                };
            }
            Err(
                LimboError::SchemaUpdated
                | LimboError::SchemaConflict
                | LimboError::Busy
                | LimboError::BusySnapshot
                | LimboError::TableLocked,
            ) => continue,
            Err(err) => panic!("probe SQL should succeed: {err}"),
        }
    }
    panic!("fresh-worker probe did not stabilize after transient multiprocess errors");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn probe_table_rootpage_via_fresh_worker(
    whopper: &MultiprocessWhopper,
    table_name: &str,
) -> Option<i64> {
    probe_optional_int_via_fresh_worker(
        whopper,
        format!("select rootpage from sqlite_schema where type='table' and name='{table_name}'"),
    )
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn probe_simple_kv_length_via_fresh_worker(
    whopper: &MultiprocessWhopper,
    table_name: &str,
    key: &str,
) -> Option<i64> {
    probe_optional_int_via_fresh_worker(
        whopper,
        format!("select length(value) from {table_name} where key='{key}'"),
    )
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn advance_seeded_whopper_to_step(whopper: &mut MultiprocessWhopper, step_after_execution: usize) {
    while whopper.current_step < step_after_execution {
        whopper.step().expect("seeded whopper step");
    }
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_restart_reuses_persisted_tshm_without_disk_scan() {
    let mut whopper = create_multiprocess_whopper(2);

    whopper
        .execute_sql_direct(0, "create table test(id integer primary key, value text)")
        .expect("create table")
        .expect("create table should succeed");
    whopper
        .execute_sql_direct(0, "insert into test(value) values ('persisted')")
        .expect("insert row")
        .expect("insert should succeed");

    let tshm_path = whopper.db_path().with_extension("db-tshm");
    wait_for_file(&tshm_path);
    let tshm_len_before_restart = std::fs::metadata(&tshm_path)
        .expect("read tshm metadata before restart")
        .len();
    assert!(
        tshm_len_before_restart > 0,
        "restart coverage requires a persisted non-empty tshm file"
    );

    whopper
        .restart_all_workers_preserve_files()
        .expect("restart worker cohort");

    let startup = whopper
        .worker_startup_telemetries()
        .first()
        .copied()
        .expect("worker telemetry");
    assert!(
        !startup.loaded_from_disk_scan,
        "first reopened worker should reuse preserved tshm authority without a WAL disk scan"
    );
    assert_eq!(
        startup.reopened_nbackfills, 0,
        "clean restart should remain on the conservative nbackfills=0 reopen path"
    );

    let row_count = whopper
        .execute_sql_direct(1, "select count(*) from test")
        .expect("count rows")
        .expect("count should succeed");
    assert_eq!(row_count.len(), 1, "count query should return one row");
    assert_eq!(
        row_count[0].len(),
        1,
        "count query should return one column"
    );
    assert_eq!(
        row_count[0][0].as_int().expect("count should be integer"),
        1,
        "restart should preserve committed rows"
    );
    assert!(
        std::fs::metadata(&tshm_path)
            .expect("read tshm metadata after restart")
            .len()
            >= tshm_len_before_restart,
        "restart should preserve the persisted tshm file"
    );

    whopper.finalize().expect("finalize multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_finalize_after_restart_preserves_simple_kv_rows() {
    let mut whopper = create_multiprocess_whopper_with_keep(16, true);
    let db_path = whopper.db_path().to_path_buf();
    let table_name = "simple_kv_finalize_regression";

    whopper
        .execute_sql_direct(
            0,
            format!("create table {table_name}(key text primary key, value blob not null)"),
        )
        .expect("create table")
        .expect("create table should succeed");

    for (key, len) in [
        ("key_912", 2933usize),
        ("key_9544", 9956usize),
        ("key_6190", 16191usize),
        ("key_7791", 15328usize),
    ] {
        whopper
            .execute_sql_direct(
                0,
                format!(
                    "insert or replace into {table_name}(key, value) values ('{key}', zeroblob({len}))"
                ),
            )
            .expect("insert baseline row")
            .expect("baseline insert should succeed");
    }

    whopper
        .restart_all_workers_preserve_files()
        .expect("restart after baseline rows");

    whopper
        .execute_sql_direct(
            1,
            format!(
                "insert or replace into {table_name}(key, value) values ('key_5709', zeroblob(9410))"
            ),
        )
        .expect("insert post-restart row")
        .expect("post-restart insert should succeed");

    whopper
        .restart_all_workers_preserve_files()
        .expect("restart after post-restart insert");
    whopper.finalize().expect("finalize multiprocess whopper");

    let io: Arc<dyn IO> = multiprocess_test_io();
    let reopened = Database::open_file(io, db_path.to_str().expect("db path utf8"))
        .expect("reopen finalized database");
    let conn = reopened.connect().expect("connect reopened db");
    assert_eq!(
        count_rows_in_table(&conn, table_name),
        5,
        "restart + graceful shutdown must preserve every simple_kv row"
    );

    let db_str = db_path.to_str().expect("db path utf8");
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_str}-wal"));
    let _ = std::fs::remove_file(format!("{db_str}-tshm"));
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_seed_5724542806254236599_restart_then_finalize_preserves_key_684() {
    configure_worker_exe();
    let mut whopper = MultiprocessWhopper::new(MultiprocessOpts {
        seed: Some(5724542806254236599),
        enable_mvcc: false,
        process_count: 16,
        connections_per_process: 1,
        max_steps: 4951,
        elle_tables: vec![],
        workloads: vec![
            (10, Box::new(IntegrityCheckWorkload)),
            (5, Box::new(WalCheckpointWorkload)),
            (10, Box::new(CreateSimpleTableWorkload)),
            (20, Box::new(SimpleSelectWorkload)),
            (20, Box::new(SimpleInsertWorkload)),
            (15, Box::new(UpdateWorkload)),
            (15, Box::new(DeleteWorkload)),
            (2, Box::new(CreateIndexWorkload)),
            (2, Box::new(DropIndexWorkload)),
            (30, Box::new(BeginWorkload)),
            (10, Box::new(CommitWorkload)),
            (10, Box::new(RollbackWorkload)),
        ],
        properties: vec![],
        chaotic_profiles: vec![],
        kill_probability: 0.0,
        restart_probability: 0.05,
        history_output: None,
        keep_files: true,
    })
    .expect("create seeded multiprocess whopper");
    let db_path = whopper.db_path().to_path_buf();

    while whopper.current_step < 4950 {
        whopper.step().expect("seeded whopper step");
    }

    let rows_after_restart = whopper
        .execute_sql_direct(
            0,
            "select count(*), min(length(value)), max(length(value)) from simple_kv_19961 where key='key_684'",
        )
        .expect("query key_684 after restart")
        .expect("post-restart query should succeed");
    assert_eq!(
        rows_after_restart[0][0].as_int(),
        Some(1),
        "row should remain visible immediately after the step 4950 restart",
    );
    assert_eq!(rows_after_restart[0][1].as_int(), Some(6871));
    assert_eq!(rows_after_restart[0][2].as_int(), Some(6871));
    let snapshot_after_restart = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read shared WAL snapshot after restart")
        .expect("shared WAL snapshot should exist after restart");

    whopper.step().expect("run step 4951 after restart");
    let snapshot_after_integrity_check = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read shared WAL snapshot after integrity check")
        .expect("shared WAL snapshot should exist after integrity check");
    assert_eq!(
        snapshot_after_integrity_check.max_frame, snapshot_after_restart.max_frame,
        "integrity_check must not mutate authoritative max_frame",
    );
    assert_eq!(
        snapshot_after_integrity_check.nbackfills, snapshot_after_restart.nbackfills,
        "integrity_check must not publish checkpoint progress",
    );
    assert_eq!(
        snapshot_after_integrity_check.checkpoint_seq, snapshot_after_restart.checkpoint_seq,
        "integrity_check must not advance the WAL generation",
    );

    let pre_finalize_rows = whopper
        .execute_sql_direct(
            0,
            "select count(*), min(length(value)), max(length(value)) from simple_kv_19961 where key='key_684'",
        )
        .expect("query key_684 before finalize")
        .expect("pre-finalize query should succeed");
    assert_eq!(
        pre_finalize_rows[0][0].as_int(),
        Some(1),
        "row should still be visible after the step 4951 integrity check",
    );
    assert_eq!(pre_finalize_rows[0][1].as_int(), Some(6871));
    assert_eq!(pre_finalize_rows[0][2].as_int(), Some(6871));

    whopper
        .finalize()
        .expect("finalize seeded multiprocess whopper");

    let io: Arc<dyn IO> = multiprocess_test_io();
    let reopened = Database::open_file(io, db_path.to_str().expect("db path utf8"))
        .expect("reopen finalized seeded database");
    let conn = reopened.connect().expect("connect reopened seeded db");
    let mut stmt = conn
        .prepare("select count(*), min(length(value)), max(length(value)) from simple_kv_19961 where key='key_684'")
        .expect("prepare reopened seeded query");
    let mut reopened_rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        reopened_rows.push((
            row.get::<i64>(0).expect("count column"),
            row.get::<i64>(1).expect("min length column"),
            row.get::<i64>(2).expect("max length column"),
        ));
        Ok(())
    })
    .expect("run reopened seeded query");
    assert_eq!(reopened_rows, vec![(1, 6871, 6871)]);

    let db_str = db_path.to_str().expect("db path utf8");
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_str}-wal"));
    let _ = std::fs::remove_file(format!("{db_str}-tshm"));
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_seed_5724542806254236599_localizes_key_4994_loss() {
    configure_worker_exe();
    let mut whopper = MultiprocessWhopper::new(MultiprocessOpts {
        seed: Some(5724542806254236599),
        enable_mvcc: false,
        process_count: 16,
        connections_per_process: 1,
        max_steps: 317,
        elle_tables: vec![],
        workloads: vec![
            (10, Box::new(IntegrityCheckWorkload)),
            (5, Box::new(WalCheckpointWorkload)),
            (10, Box::new(CreateSimpleTableWorkload)),
            (20, Box::new(SimpleSelectWorkload)),
            (20, Box::new(SimpleInsertWorkload)),
            (15, Box::new(UpdateWorkload)),
            (15, Box::new(DeleteWorkload)),
            (2, Box::new(CreateIndexWorkload)),
            (2, Box::new(DropIndexWorkload)),
            (30, Box::new(BeginWorkload)),
            (10, Box::new(CommitWorkload)),
            (10, Box::new(RollbackWorkload)),
        ],
        properties: vec![],
        chaotic_profiles: vec![],
        kill_probability: 0.0,
        restart_probability: 0.05,
        history_output: None,
        keep_files: true,
    })
    .expect("create seeded multiprocess whopper");

    let db_path = whopper.db_path().to_path_buf();
    let table_name = "simple_kv_57904";
    let key = "key_4994";

    advance_seeded_whopper_to_step(&mut whopper, 267);
    let live_rows = whopper
        .execute_sql_direct(
            10,
            "select count(*), min(length(value)), max(length(value)) from simple_kv_57904 where key='key_4994'",
        )
        .expect("query key_4994 on live worker after insert")
        .expect("live worker query should succeed");
    eprintln!(
        "after step 266 insert: live_rows={:?} shared_snapshot={:?}",
        live_rows,
        whopper
            .shared_wal_snapshot_direct(10)
            .expect("read shared WAL snapshot after insert")
    );
    let observer_io: Arc<dyn IO> = multiprocess_test_io();
    let observer_db = Database::open_file_with_flags(
        observer_io,
        db_path.to_str().expect("db path utf8"),
        OpenFlags::ReadOnly,
        multiprocess_wal_db_opts(),
        None,
    )
    .expect("open observer database after insert");
    eprintln!(
        "after step 266 insert: observer_telemetry={:?} observer_local_max_frame={:?}",
        observer_db
            .shared_wal_open_telemetry()
            .expect("observer shared WAL telemetry"),
        observer_db.local_wal_max_frame_for_testing()
    );
    assert_eq!(
        read_simple_kv_length(&db_path, table_name, key),
        Some(5044),
        "row must be visible immediately after step 266 insert",
    );

    advance_seeded_whopper_to_step(&mut whopper, 283);
    eprintln!(
        "after step 282 restart: telemetries={:?}",
        whopper.worker_startup_telemetries()
    );
    assert_eq!(
        read_simple_kv_length(&db_path, table_name, key),
        Some(5044),
        "row disappeared during the step 282 restart",
    );

    advance_seeded_whopper_to_step(&mut whopper, 306);
    eprintln!(
        "after step 305 restart: telemetries={:?}",
        whopper.worker_startup_telemetries()
    );
    assert_eq!(
        read_simple_kv_length(&db_path, table_name, key),
        Some(5044),
        "row disappeared during the step 305 restart",
    );
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_seed_8849519299024683634_localizes_schema_loss_boundary() {
    configure_worker_exe();
    let mut whopper = MultiprocessWhopper::new(MultiprocessOpts {
        seed: Some(8849519299024683634),
        enable_mvcc: false,
        process_count: 16,
        connections_per_process: 1,
        max_steps: 72,
        elle_tables: vec![],
        workloads: vec![
            (10, Box::new(IntegrityCheckWorkload)),
            (5, Box::new(WalCheckpointWorkload)),
            (10, Box::new(CreateSimpleTableWorkload)),
            (20, Box::new(SimpleSelectWorkload)),
            (20, Box::new(SimpleInsertWorkload)),
            (15, Box::new(UpdateWorkload)),
            (15, Box::new(DeleteWorkload)),
            (2, Box::new(CreateIndexWorkload)),
            (2, Box::new(DropIndexWorkload)),
            (30, Box::new(BeginWorkload)),
            (10, Box::new(CommitWorkload)),
            (10, Box::new(RollbackWorkload)),
        ],
        properties: vec![],
        chaotic_profiles: vec![],
        kill_probability: 0.0,
        restart_probability: 0.05,
        history_output: None,
        keep_files: true,
    })
    .expect("create seeded multiprocess whopper");

    advance_seeded_whopper_to_step(&mut whopper, 67);
    assert!(
        probe_table_rootpage_via_fresh_worker(&whopper, "simple_kv_9842").is_some(),
        "simple_kv_9842 should still exist for a fresh opener immediately after the step 66 restart",
    );
    assert_eq!(
        probe_simple_kv_length_via_fresh_worker(&whopper, "simple_kv_9842", "key_6095"),
        Some(874),
        "baseline row should still be visible for a fresh opener immediately after the step 66 restart",
    );

    advance_seeded_whopper_to_step(&mut whopper, 70);
    assert!(
        probe_table_rootpage_via_fresh_worker(&whopper, "simple_kv_9842").is_some(),
        "simple_kv_9842 disappeared from sqlite_schema by the step 70 restart; cohort_telemetries={:?}",
        whopper.worker_startup_telemetries(),
    );

    whopper
        .finalize()
        .expect("finalize seeded multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_truncate_generation_survives_foreign_first_append_and_restart() {
    let mut whopper = create_multiprocess_whopper(2);
    let table_name = "simple_kv_85275";

    whopper
        .execute_sql_direct(
            0,
            format!("create table {table_name}(key text primary key, value text not null)"),
        )
        .expect("create table")
        .expect("create table should succeed");

    let snapshot_before_truncate = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read shared WAL snapshot before TRUNCATE")
        .expect("shared WAL snapshot should be available");
    assert!(
        snapshot_before_truncate.max_frame > 0,
        "TRUNCATE regression requires schema changes to create WAL content before checkpoint"
    );

    whopper
        .execute_sql_direct(1, "PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("run TRUNCATE checkpoint")
        .expect("TRUNCATE checkpoint should succeed");

    let snapshot_after_truncate = whopper
        .shared_wal_snapshot_direct(1)
        .expect("read shared WAL snapshot after TRUNCATE")
        .expect("shared WAL snapshot should be available");
    assert_eq!(
        snapshot_after_truncate.max_frame, 0,
        "TRUNCATE should reset the authoritative max_frame for the new generation"
    );
    assert_eq!(
        snapshot_after_truncate.nbackfills, 0,
        "TRUNCATE should clear published backfill progress before the next append"
    );
    assert_ne!(
        snapshot_after_truncate.checkpoint_seq, snapshot_before_truncate.checkpoint_seq,
        "TRUNCATE should advance the checkpoint generation"
    );

    whopper
        .execute_sql_direct(
            0,
            format!("insert or replace into {table_name}(key, value) values ('hello', 'world')"),
        )
        .expect("append first row after TRUNCATE")
        .expect("first post-TRUNCATE append should succeed");

    let snapshot_after_append = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read shared WAL snapshot after first post-TRUNCATE append")
        .expect("shared WAL snapshot should be available");
    assert!(
        snapshot_after_append.max_frame > 0,
        "first post-TRUNCATE append should publish frames in the replacement generation"
    );
    assert_eq!(
        snapshot_after_append.checkpoint_seq, snapshot_after_truncate.checkpoint_seq,
        "first post-TRUNCATE append must remain in the TRUNCATE generation"
    );

    whopper
        .restart_all_workers_preserve_files()
        .expect("restart worker cohort");

    for (worker_idx, startup) in whopper.worker_startup_telemetries().iter().enumerate() {
        assert_eq!(
            startup.reopened_checkpoint_seq, snapshot_after_append.checkpoint_seq,
            "worker {worker_idx} reopened the wrong WAL generation after TRUNCATE"
        );
    }

    let row_count = whopper
        .execute_sql_direct(1, format!("select count(*) from {table_name}"))
        .expect("count rows after restart")
        .expect("count should succeed");
    assert_eq!(row_count[0][0].as_int(), Some(1));
    assert_integrity_check_ok(&mut whopper, 1);

    whopper.finalize().expect("finalize multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_restart_rebuilds_from_disk_when_partial_checkpoint_publishes_positive_nbackfills() {
    let mut whopper = create_multiprocess_whopper(1);
    let (_max_frame_before_restart, nbackfills_before_restart) =
        create_partial_checkpoint_state(&mut whopper);
    assert!(
        nbackfills_before_restart > 0,
        "restart coverage requires published positive nbackfills before reopen"
    );

    whopper
        .restart_all_workers_preserve_files()
        .expect("restart worker cohort");

    let startup = whopper
        .worker_startup_telemetries()
        .first()
        .copied()
        .expect("worker telemetry");
    assert!(
        startup.loaded_from_disk_scan,
        "reopen must rebuild from disk when positive nbackfills cannot be proven safe on restart"
    );
    assert_eq!(
        startup.reopened_nbackfills, 0,
        "disk-scan reopen must not resurrect positive nbackfills from tshm authority state"
    );

    let row_count = whopper
        .execute_sql_direct(0, "select count(*) from test")
        .expect("count rows")
        .expect("count should succeed");
    assert_eq!(
        row_count[0][0].as_int().expect("count should be integer"),
        32,
        "conservative reopen after a positive-nbackfills checkpoint must preserve committed rows"
    );

    whopper.finalize().expect("finalize multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_restart_rebuilds_from_disk_after_wal_append_invalidates_partial_checkpoint_proof() {
    let mut whopper = create_multiprocess_whopper(1);
    let (max_frame_after_checkpoint, _nbackfills_after_checkpoint) =
        create_partial_checkpoint_state(&mut whopper);

    whopper
        .execute_sql_direct(0, "insert into test(value) values (randomblob(2048))")
        .expect("append row after checkpoint")
        .expect("append should succeed");

    let snapshot_after_append = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read shared WAL snapshot after append")
        .expect("shared WAL snapshot should be available");
    assert!(
        snapshot_after_append.max_frame > max_frame_after_checkpoint,
        "stale-proof restart coverage requires a WAL append after proof installation"
    );

    whopper
        .restart_all_workers_preserve_files()
        .expect("restart worker cohort");

    let startup = whopper
        .worker_startup_telemetries()
        .first()
        .copied()
        .expect("worker telemetry");
    assert!(
        startup.loaded_from_disk_scan,
        "reopen must rebuild from disk after a WAL append invalidates the tshm backfill proof"
    );

    let row_count = whopper
        .execute_sql_direct(0, "select count(*) from test")
        .expect("count rows")
        .expect("count should succeed");
    assert_eq!(
        row_count[0][0].as_int().expect("count should be integer"),
        33,
        "stale-proof restart should preserve rows committed after the partial checkpoint"
    );

    whopper.finalize().expect("finalize multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_restart_rebuilds_from_disk_after_partial_checkpoint_proof_is_cleared() {
    let mut whopper = create_multiprocess_whopper(1);
    create_partial_checkpoint_state(&mut whopper);

    whopper
        .clear_backfill_proof_direct(0)
        .expect("clear backfill proof");
    whopper
        .restart_all_workers_preserve_files()
        .expect("restart worker cohort");

    let startup = whopper
        .worker_startup_telemetries()
        .first()
        .copied()
        .expect("worker telemetry");
    assert!(
        startup.loaded_from_disk_scan,
        "reopen must rebuild from disk when positive nbackfills are not durably provable"
    );
    assert_eq!(
        count_test_rows(&mut whopper, 0),
        32,
        "disk-scan reopen should preserve committed rows after proof removal"
    );

    whopper.finalize().expect("finalize multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_restart_rebuilds_from_disk_after_db_header_mismatch_invalidates_partial_checkpoint_proof()
 {
    let mut whopper = create_multiprocess_whopper(1);
    create_partial_checkpoint_state(&mut whopper);

    whopper
        .mutate_on_disk_and_restart(|db_path| {
            flip_db_header_reserved_byte(db_path);
            Ok(())
        })
        .expect("mutate db header and restart");

    let startup = whopper
        .worker_startup_telemetries()
        .first()
        .copied()
        .expect("worker telemetry");
    assert!(
        startup.loaded_from_disk_scan,
        "reopen must rebuild from disk when the DB header fingerprint invalidates the tshm proof"
    );
    assert_eq!(
        count_test_rows(&mut whopper, 0),
        32,
        "DB-header-mismatch restart should preserve committed rows after conservative reopen"
    );

    whopper.finalize().expect("finalize multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_restart_stays_conservative_after_unpublished_backfill_proof_install() {
    let mut whopper = create_multiprocess_whopper(1);
    populate_blob_test_rows(&mut whopper);

    whopper
        .install_unpublished_backfill_proof_direct(0, 1)
        .expect("install unpublished backfill proof");

    let snapshot_before_restart = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read shared WAL snapshot before restart")
        .expect("shared WAL snapshot should be available");
    assert_eq!(
        snapshot_before_restart.nbackfills, 0,
        "proof-installed-but-unpublished coverage requires nbackfills to remain unpublished"
    );

    whopper
        .restart_all_workers_preserve_files()
        .expect("restart worker cohort");

    let startup = whopper
        .worker_startup_telemetries()
        .first()
        .copied()
        .expect("worker telemetry");
    assert!(
        !startup.loaded_from_disk_scan,
        "reopen after proof install without publication should stay on the conservative authority path"
    );
    assert_eq!(
        startup.reopened_nbackfills, 0,
        "reopen after proof install without publication must keep backfill unpublished"
    );
    assert_eq!(
        count_test_rows(&mut whopper, 0),
        32,
        "reopen after proof install without publication must preserve committed rows"
    );

    whopper.finalize().expect("finalize multiprocess whopper");
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
#[test]
fn multiprocess_restart_rebuilds_from_disk_after_restart_checkpoint_changes_generation() {
    let mut whopper = create_multiprocess_whopper(1);
    create_partial_checkpoint_state(&mut whopper);

    let snapshot_before_restart_checkpoint = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read snapshot before RESTART checkpoint")
        .expect("shared WAL snapshot should be available");

    whopper
        .execute_sql_direct(0, "PRAGMA wal_checkpoint(RESTART)")
        .expect("run RESTART checkpoint")
        .expect("RESTART checkpoint should succeed");

    let snapshot_after_restart_checkpoint = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read snapshot after RESTART checkpoint")
        .expect("shared WAL snapshot should be available");
    assert_eq!(
        snapshot_after_restart_checkpoint.max_frame, 0,
        "RESTART should reset the authoritative max_frame for the new WAL generation"
    );
    assert_eq!(
        snapshot_after_restart_checkpoint.nbackfills, 0,
        "RESTART should clear published backfill progress before the replacement generation begins"
    );
    assert_ne!(
        snapshot_after_restart_checkpoint.checkpoint_seq,
        snapshot_before_restart_checkpoint.checkpoint_seq,
        "RESTART should advance the checkpoint generation"
    );

    whopper
        .execute_sql_direct(0, "begin immediate")
        .expect("begin replacement-generation write transaction")
        .expect("begin should succeed");
    for _ in 0..32 {
        whopper
            .execute_sql_direct(0, "insert into test(value) values (randomblob(2048))")
            .expect("insert replacement-generation row")
            .expect("replacement-generation insert should succeed");
    }
    whopper
        .execute_sql_direct(0, "commit")
        .expect("commit replacement-generation transaction")
        .expect("commit should succeed");
    whopper
        .passive_checkpoint_direct(0, Some(1))
        .expect("run replacement partial checkpoint");

    let snapshot_after_replacement = whopper
        .shared_wal_snapshot_direct(0)
        .expect("read snapshot after replacement proof install")
        .expect("shared WAL snapshot should be available");
    assert!(
        snapshot_after_replacement.nbackfills > 0,
        "replacement generation should install a positive nbackfills proof"
    );
    assert!(
        snapshot_after_replacement.max_frame > snapshot_after_replacement.nbackfills,
        "replacement generation should retain live WAL frames beyond the backfill point"
    );
    assert_ne!(
        snapshot_after_replacement.checkpoint_seq,
        snapshot_before_restart_checkpoint.checkpoint_seq,
        "replacement proof must belong to the restarted WAL generation"
    );

    whopper
        .restart_all_workers_preserve_files()
        .expect("restart worker cohort");

    let startup = whopper
        .worker_startup_telemetries()
        .first()
        .copied()
        .expect("worker telemetry");
    assert!(
        startup.loaded_from_disk_scan,
        "replacement-generation reopen must stay conservative when restart leaves positive nbackfills"
    );
    assert_eq!(
        startup.reopened_nbackfills, 0,
        "replacement-generation reopen must not revive positive nbackfills from tshm"
    );
    assert_eq!(
        startup.reopened_checkpoint_seq, snapshot_after_replacement.checkpoint_seq,
        "conservative reopen after RESTART must still bind to the replacement WAL generation"
    );
    assert_ne!(
        startup.reopened_checkpoint_seq, snapshot_before_restart_checkpoint.checkpoint_seq,
        "reopen after RESTART must not reuse state from the previous generation"
    );
    assert_eq!(
        count_test_rows(&mut whopper, 0),
        64,
        "conservative reopen after RESTART-generation checkpoint churn should preserve both generations of rows"
    );

    whopper.finalize().expect("finalize multiprocess whopper");
}
