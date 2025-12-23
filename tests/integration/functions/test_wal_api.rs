use std::{collections::HashSet, path::PathBuf, sync::Arc};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tempfile::TempDir;
use turso_core::{
    types::{WalFrameInfo, WalState},
    CheckpointMode, LimboError, StepResult,
};

use crate::common::{rng_from_time, ExecRows, TempDatabase};

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_count(db: TempDatabase) {
    let conn = db.connect_limbo();
    assert_eq!(conn.wal_state().unwrap().max_frame, 0);
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    assert_eq!(conn.wal_state().unwrap().max_frame, 2);
    conn.execute("INSERT INTO t VALUES (10, 10), (5, 1)")
        .unwrap();
    assert_eq!(conn.wal_state().unwrap().max_frame, 3);
    conn.execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn.wal_state().unwrap().max_frame, 15);
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_transfer_no_schema_changes(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);
    let db1 = builder.clone().build();
    let conn1 = db1.connect_limbo();
    let db2 = builder.build();
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (10, 10), (5, 1)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_state().unwrap().max_frame, 15);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    let frames_count = conn1.wal_state().unwrap().max_frame;
    for frame_id in 1..=frames_count {
        conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        conn2.wal_insert_frame(frame_id, &frame).unwrap();
    }

    conn2.wal_insert_end(false).unwrap();
    assert_eq!(conn2.wal_state().unwrap().max_frame, 15);
    let rows: Vec<(i64, i64)> = conn2.exec_rows("SELECT x, length(y) FROM t");
    assert_eq!(rows, vec![(5, 1), (10, 2), (1024, 40960)]);
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_transfer_various_schema_changes(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    let db1 = builder.clone().build();
    let conn1 = db1.connect_limbo();
    let db2 = builder.build();
    let conn2 = db2.connect_limbo();
    let conn3 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    let mut frame = [0u8; 24 + 4096];
    let mut synced_frame = 0;
    let mut sync = || {
        let last_frame = conn1.wal_state().unwrap().max_frame;
        conn2.wal_insert_begin().unwrap();
        for frame_id in (synced_frame + 1)..=last_frame {
            conn1.wal_get_frame(frame_id, &mut frame).unwrap();
            conn2.wal_insert_frame(frame_id, &frame).unwrap();
        }
        conn2.wal_insert_end(false).unwrap();
        synced_frame = last_frame;
    };

    sync();
    let rows: Vec<(i64, i64)> = conn2.exec_rows("SELECT * FROM t");
    assert!(rows.is_empty());
    let rows: Vec<(i64, i64)> = conn3.exec_rows("SELECT * FROM t");
    assert!(rows.is_empty());

    conn1.execute("DROP TABLE t").unwrap();

    sync();
    assert!(matches!(
        conn2.prepare("SELECT * FROM t").err().unwrap(),
        turso_core::LimboError::ParseError(error) if error == "no such table: t"
    ));
    assert!(matches!(
        conn3.prepare("SELECT * FROM t").err().unwrap(),
        turso_core::LimboError::ParseError(error) if error == "no such table: t"
    ));

    conn1.execute("CREATE TABLE a(x)").unwrap();
    conn1.execute("CREATE TABLE b(x, y)").unwrap();

    sync();
    let rows: Vec<(i64,)> = conn2.exec_rows("SELECT 1 FROM a UNION ALL SELECT 1 FROM b");
    assert!(rows.is_empty());
    let rows: Vec<(i64,)> = conn3.exec_rows("SELECT 1 FROM a UNION ALL SELECT 1 FROM b");
    assert!(rows.is_empty());
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_transfer_schema_changes(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    let db1 = builder.clone().build();
    let conn1 = db1.connect_limbo();
    let db2 = builder.build();
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (10, 10), (5, 1)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_state().unwrap().max_frame, 15);
    let mut frame = [0u8; 24 + 4096];
    let mut commits = 0;
    conn2.wal_insert_begin().unwrap();
    for frame_id in 1..=conn1.wal_state().unwrap().max_frame {
        conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        let info = conn2.wal_insert_frame(frame_id, &frame).unwrap();
        if info.is_commit_frame() {
            commits += 1;
        }
    }
    conn2.wal_insert_end(false).unwrap();
    assert_eq!(commits, 3);
    assert_eq!(conn2.wal_state().unwrap().max_frame, 15);
    let rows: Vec<(i64, i64)> = conn2.exec_rows("SELECT x, length(y) FROM t");
    assert_eq!(rows, vec![(5, 1), (10, 2), (1024, 40960)]);
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_transfer_no_schema_changes_rollback(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    let db1 = builder.clone().build();
    let conn1 = db1.connect_limbo();
    let db2 = builder.build();
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_state().unwrap().max_frame, 14);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    // Intentionally leave out the final commit frame, so the big randomblob is not committed and should not be visible to transactions.
    for frame_id in 1..=(conn1.wal_state().unwrap().max_frame - 1) {
        conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        conn2.wal_insert_frame(frame_id, &frame).unwrap();
    }
    conn2.wal_insert_end(false).unwrap();
    assert_eq!(conn2.wal_state().unwrap().max_frame, 2);
    let rows: Vec<(i64, i64)> = conn2.exec_rows("SELECT x, length(y) FROM t");
    assert!(rows.is_empty());
    conn2.execute("CREATE TABLE q(x)").unwrap();
    conn2
        .execute("INSERT INTO q VALUES (randomblob(4096 * 10))")
        .unwrap();
    let rows: Vec<(i64, i64)> = conn2.exec_rows("SELECT x, LENGTH(y) FROM t");
    assert!(rows.is_empty());
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_transfer_schema_changes_rollback(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    let db1 = builder.clone().build();
    let conn1 = db1.connect_limbo();
    let db2 = builder.build();
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_state().unwrap().max_frame, 14);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    for frame_id in 1..=(conn1.wal_state().unwrap().max_frame - 1) {
        conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        conn2.wal_insert_frame(frame_id, &frame).unwrap();
    }
    conn2.wal_insert_end(false).unwrap();
    assert_eq!(conn2.wal_state().unwrap().max_frame, 2);
    let rows: Vec<(i64, i64)> = conn2.exec_rows("SELECT x, length(y) FROM t");
    assert!(rows.is_empty());
    conn2.execute("CREATE TABLE q(x)").unwrap();
    conn2
        .execute("INSERT INTO q VALUES (randomblob(4096 * 10))")
        .unwrap();
    let rows: Vec<(i64, i64)> = conn2.exec_rows("SELECT x, LENGTH(y) FROM t");
    assert!(rows.is_empty());
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_conflict(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    let db1 = builder.clone().build();
    let conn1 = db1.connect_limbo();
    let db2 = builder.build();
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE q(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    assert_eq!(conn1.wal_state().unwrap().max_frame, 2);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    conn1.wal_get_frame(1, &mut frame).unwrap();
    assert!(conn2.wal_insert_frame(1, &frame).is_err());
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_far_away_write(db: TempDatabase) {
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    let db1 = builder.clone().build();
    let conn1 = db1.connect_limbo();
    let db2 = builder.clone().build();
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_state().unwrap().max_frame, 14);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();

    conn1.wal_get_frame(3, &mut frame).unwrap();
    conn2.wal_insert_frame(3, &frame).unwrap();

    conn1.wal_get_frame(5, &mut frame).unwrap();
    assert!(conn2.wal_insert_frame(5, &frame).is_err());
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_frame_api_no_schema_changes_fuzz(db: TempDatabase) {
    let (mut rng, _) = rng_from_time();

    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    for _ in 0..4 {
        let db1 = builder.clone().build();
        let conn1 = db1.connect_limbo();
        let db2 = builder.clone().build();
        let conn2 = db2.connect_limbo();
        conn1
            .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
            .unwrap();
        conn2
            .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
            .unwrap();

        let seed = rng.next_u64();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        println!("SEED: {seed}");

        let (mut size, mut synced_frame) = (0, conn2.wal_state().unwrap().max_frame);
        let mut commit_frames = vec![conn1.wal_state().unwrap().max_frame];
        for _ in 0..256 {
            if rng.next_u32() % 10 != 0 {
                let key = rng.next_u32();
                let length = rng.next_u32() % (4 * 4096);
                let query = format!("INSERT INTO t VALUES ({key}, randomblob({length}))");
                conn1.execute(&query).unwrap();
                commit_frames.push(conn1.wal_state().unwrap().max_frame);
            } else {
                let last_frame = conn1.wal_state().unwrap().max_frame;
                let next_frame =
                    synced_frame + (rng.next_u32() as u64 % (last_frame - synced_frame + 1));
                let mut frame = [0u8; 24 + 4096];
                conn2.wal_insert_begin().unwrap();
                for frame_no in (synced_frame + 1)..=next_frame {
                    conn1.wal_get_frame(frame_no, &mut frame).unwrap();
                    conn2.wal_insert_frame(frame_no, &frame[..]).unwrap();
                }
                conn2.wal_insert_end(false).unwrap();
                for (i, committed) in commit_frames.iter().enumerate() {
                    if *committed <= next_frame {
                        size = size.max(i);
                        synced_frame = *committed;
                    }
                }
                if rng.next_u32() % 10 == 0 {
                    synced_frame = rng.next_u32() as u64 % synced_frame;
                }
                let rows: Vec<(i64,)> = conn2.exec_rows("SELECT COUNT(*) FROM t");
                assert_eq!(rows, vec![(size as i64,)]);
            }
        }
    }
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_api_changed_pages(db1: TempDatabase) {
    let conn1 = db1.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("CREATE TABLE q(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    assert_eq!(
        conn1
            .wal_changed_pages_after(0)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([1, 2, 3])
    );
    let frames = conn1.wal_state().unwrap().max_frame;
    conn1.execute("INSERT INTO t VALUES (1, 2)").unwrap();
    conn1.execute("INSERT INTO t VALUES (3, 4)").unwrap();
    assert_eq!(
        conn1
            .wal_changed_pages_after(frames)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([2])
    );
    let frames = conn1.wal_state().unwrap().max_frame;
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 2))")
        .unwrap();
    assert_eq!(
        conn1
            .wal_changed_pages_after(frames)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([1, 2, 4, 5])
    );
}

fn revert_to(conn: &Arc<turso_core::Connection>, frame_watermark: u64) -> turso_core::Result<()> {
    let mut frame = [0u8; 4096 + 24];
    let frame_watermark_info = conn.wal_get_frame(frame_watermark, &mut frame)?;

    let changed_pages = conn.wal_changed_pages_after(frame_watermark)?;

    conn.wal_insert_begin()?;
    let mut frames = Vec::new();
    for page_id in changed_pages {
        let has_page =
            conn.try_wal_watermark_read_page(page_id, &mut frame[24..], Some(frame_watermark))?;
        if !has_page {
            continue;
        }
        frames.push((page_id, frame));
    }

    let mut frame_no = conn.wal_state().unwrap().max_frame;
    for (i, (page_id, mut frame)) in frames.iter().enumerate() {
        let info = WalFrameInfo {
            db_size: if i == frames.len() - 1 {
                frame_watermark_info.db_size
            } else {
                0
            },
            page_no: *page_id,
        };
        info.put_to_frame_header(&mut frame);
        frame_no += 1;
        conn.wal_insert_frame(frame_no, &frame)?;
    }
    conn.wal_insert_end(false)?;

    Ok(())
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_api_revert_pages(db1: TempDatabase) {
    let conn1 = db1.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    let watermark1 = conn1.wal_state().unwrap().max_frame;
    conn1
        .execute("INSERT INTO t VALUES (1, randomblob(10))")
        .unwrap();
    let watermark2 = conn1.wal_state().unwrap().max_frame;

    conn1
        .execute("INSERT INTO t VALUES (3, randomblob(20))")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 2))")
        .unwrap();

    let rows: Vec<(i64, i64)> = conn1.exec_rows("SELECT x, length(y) FROM t");
    assert_eq!(rows, vec![(1, 10), (3, 20), (1024, 4096 * 2)]);

    revert_to(&conn1, watermark2).unwrap();

    let rows: Vec<(i64, i64)> = conn1.exec_rows("SELECT x, length(y) FROM t");
    assert_eq!(rows, vec![(1, 10)]);

    revert_to(&conn1, watermark1).unwrap();

    let rows: Vec<(i64, i64)> = conn1.exec_rows("SELECT x, length(y) FROM t");
    assert!(rows.is_empty());
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_upper_bound_passive(db: TempDatabase) {
    let writer = db.connect_limbo();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    let watermark0 = writer.wal_state().unwrap().max_frame;
    writer
        .execute("insert into test values (1, 'hello')")
        .unwrap();
    let watermark1 = writer.wal_state().unwrap().max_frame;
    writer
        .execute("insert into test values (2, 'turso')")
        .unwrap();
    let watermark2 = writer.wal_state().unwrap().max_frame;
    let expected = [
        vec![
            turso_core::types::Value::Integer(1),
            turso_core::types::Value::Text(turso_core::types::Text::new("hello")),
        ],
        vec![
            turso_core::types::Value::Integer(2),
            turso_core::types::Value::Text(turso_core::types::Text::new("turso")),
        ],
    ];

    for (prefix, watermark) in [(0, watermark0), (1, watermark1), (2, watermark2)] {
        let mode = CheckpointMode::Passive {
            upper_bound_inclusive: Some(watermark),
        };
        writer.checkpoint(mode).unwrap();

        let db_path_copy = format!("{}-{}-copy", db.path.to_str().unwrap(), watermark);
        std::fs::copy(&db.path, db_path_copy.clone()).unwrap();
        let db_copy = TempDatabase::new_with_existent(&PathBuf::from(db_path_copy));
        let conn = db_copy.connect_limbo();
        let mut stmt = conn.prepare("select * from test").unwrap();
        let mut rows: Vec<Vec<turso_core::types::Value>> = Vec::new();
        loop {
            let result = stmt.step();
            match result {
                Ok(StepResult::Row) => {
                    rows.push(stmt.row().unwrap().get_values().cloned().collect())
                }
                Ok(StepResult::IO) => db_copy.io.step().unwrap(),
                Ok(StepResult::Done) => break,
                result => panic!("unexpected step result: {result:?}"),
            }
        }
        assert_eq!(rows, expected[0..prefix]);
    }
}

// TODO: enable MVCC
#[turso_macros::test()]
fn test_wal_upper_bound_truncate(db: TempDatabase) {
    let writer = db.connect_limbo();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    writer
        .execute("insert into test values (1, 'hello')")
        .unwrap();
    let watermark = writer.wal_state().unwrap().max_frame;
    writer
        .execute("insert into test values (2, 'turso')")
        .unwrap();

    let mode = CheckpointMode::Truncate {
        upper_bound_inclusive: Some(watermark),
    };
    assert!(matches!(
        writer.checkpoint(mode).err().unwrap(),
        LimboError::Busy
    ));
    writer
        .execute("insert into test values (3, 'final')")
        .unwrap();
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_state_checkpoint_seq(db: TempDatabase) {
    let writer = db.connect_limbo();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    writer
        .execute("insert into test values (1, 'hello')")
        .unwrap();
    writer
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    writer
        .execute("insert into test values (2, 'turso')")
        .unwrap();
    writer
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    writer
        .execute("insert into test values (3, 'limbo')")
        .unwrap();

    let state = writer.wal_state().unwrap();
    assert_eq!(
        state,
        WalState {
            checkpoint_seq_no: 2,
            max_frame: 1
        }
    );
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_checkpoint_no_work(db: TempDatabase) {
    let writer = db.connect_limbo();
    let reader = db.connect_limbo();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    // open read txn in order to hold early WAL frames and prevent them from checkpoint
    reader.execute("BEGIN").unwrap();
    reader.execute("SELECT * FROM test").unwrap();

    writer
        .execute("insert into test values (1, 'hello')")
        .unwrap();

    writer
        .execute("insert into test values (2, 'turso')")
        .unwrap();
    writer
        .checkpoint(CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();
    assert!(writer
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .is_err());
    writer
        .execute("insert into test values (3, 'limbo')")
        .unwrap();

    let state = writer.wal_state().unwrap();
    assert_eq!(
        state,
        WalState {
            checkpoint_seq_no: 0,
            max_frame: 5
        }
    );
    reader.execute("SELECT * FROM test").unwrap();
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_revert_change_db_size(db: TempDatabase) {
    let writer = db.connect_limbo();

    writer.execute("create table t(x, y)").unwrap();
    let watermark = writer.wal_state().unwrap().max_frame;
    writer
        .execute("insert into t values (1, randomblob(10 * 4096))")
        .unwrap();
    writer
        .execute("insert into t values (2, randomblob(20 * 4096))")
        .unwrap();
    let mut changed = writer.wal_changed_pages_after(watermark).unwrap();
    changed.sort();

    let mut frame = [0u8; 4096 + 24];

    writer.wal_insert_begin().unwrap();
    let mut frames_count = writer.wal_state().unwrap().max_frame;
    for page_no in changed {
        let page = &mut frame[24..];
        if !writer
            .try_wal_watermark_read_page(page_no, page, Some(watermark))
            .unwrap()
        {
            continue;
        }
        let info = WalFrameInfo {
            page_no,
            db_size: if page_no == 2 { 2 } else { 0 },
        };
        info.put_to_frame_header(&mut frame);
        frames_count += 1;
        writer.wal_insert_frame(frames_count, &frame).unwrap();
    }
    writer.wal_insert_end(false).unwrap();

    writer
        .execute("insert into t values (3, randomblob(30 * 4096))")
        .unwrap();
    let rows: Vec<(i64, i64)> = writer.exec_rows("SELECT x, length(y) FROM t");
    assert_eq!(rows, vec![(3, 30 * 4096)]);
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_api_exec_commit(db: TempDatabase) {
    let writer = db.connect_limbo();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();

    writer.wal_insert_begin().unwrap();

    writer
        .execute("insert into test values (1, 'hello')")
        .unwrap();
    writer
        .execute("insert into test values (2, 'turso')")
        .unwrap();

    writer.wal_insert_end(true).unwrap();

    let mut stmt = writer.prepare("select * from test").unwrap();
    let mut rows: Vec<Vec<turso_core::types::Value>> = Vec::new();
    loop {
        let result = stmt.step();
        match result {
            Ok(StepResult::Row) => rows.push(stmt.row().unwrap().get_values().cloned().collect()),
            Ok(StepResult::IO) => db.io.step().unwrap(),
            Ok(StepResult::Done) => break,
            result => panic!("unexpected step result: {result:?}"),
        }
    }
    tracing::info!("rows: {:?}", rows);
    assert_eq!(
        rows,
        vec![
            vec![
                turso_core::types::Value::Integer(1),
                turso_core::types::Value::Text(turso_core::types::Text::new("hello")),
            ],
            vec![
                turso_core::types::Value::Integer(2),
                turso_core::types::Value::Text(turso_core::types::Text::new("turso")),
            ],
        ]
    );
}

#[turso_macros::test(mvcc)]
fn test_wal_api_exec_rollback(db: TempDatabase) {
    let writer = db.connect_limbo();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();

    writer.wal_insert_begin().unwrap();

    writer
        .execute("insert into test values (1, 'hello')")
        .unwrap();
    writer
        .execute("insert into test values (2, 'turso')")
        .unwrap();

    writer.wal_insert_end(false).unwrap();

    let mut stmt = writer.prepare("select * from test").unwrap();
    let mut rows: Vec<Vec<turso_core::types::Value>> = Vec::new();
    loop {
        let result = stmt.step();
        match result {
            Ok(StepResult::Row) => rows.push(stmt.row().unwrap().get_values().cloned().collect()),
            Ok(StepResult::IO) => db.io.step().unwrap(),
            Ok(StepResult::Done) => break,
            result => panic!("unexpected step result: {result:?}"),
        }
    }
    tracing::info!("rows: {:?}", rows);
    assert_eq!(rows, vec![] as Vec<Vec<turso_core::types::Value>>);
}

// TODO: mvcc
#[turso_macros::test()]
fn test_wal_api_insert_exec_mix(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("create table a(x, y)").unwrap();
    conn.execute("insert into a values (1, randomblob(1 * 4096))")
        .unwrap();
    let watermark = conn.wal_state().unwrap().max_frame;
    conn.execute("create table b(x, y)").unwrap();
    conn.execute("insert into b values (2, randomblob(2 * 4096))")
        .unwrap();

    let pages = conn.wal_changed_pages_after(watermark).unwrap();
    let mut frames = Vec::new();
    let mut frame = [0u8; 4096 + 24];
    for page_no in pages {
        let page = &mut frame[24..];
        if !conn
            .try_wal_watermark_read_page(page_no, page, Some(watermark))
            .unwrap()
        {
            continue;
        }
        let info = WalFrameInfo {
            db_size: 0,
            page_no,
        };
        info.put_to_frame_header(&mut frame);
        frames.push(frame);
    }

    let schema_version = conn.read_schema_version().unwrap();
    conn.wal_insert_begin().unwrap();

    let frames_cnt = conn.wal_state().unwrap().max_frame;
    for (i, frame) in frames.iter().enumerate() {
        conn.wal_insert_frame(frames_cnt + i as u64 + 1, frame)
            .unwrap();
    }
    conn.write_schema_version(schema_version + 1).unwrap();
    conn.execute("insert into a values (3, randomblob(3 * 4096))")
        .unwrap();
    conn.execute("create table b(x, y)").unwrap();
    conn.execute("insert into b values (4, randomblob(4 * 4096))")
        .unwrap();

    conn.wal_insert_end(true).unwrap();

    let mut stmt = conn.prepare("select x, length(y) from a").unwrap();
    let mut rows: Vec<Vec<turso_core::types::Value>> = Vec::new();
    loop {
        let result = stmt.step();
        match result {
            Ok(StepResult::Row) => rows.push(stmt.row().unwrap().get_values().cloned().collect()),
            Ok(StepResult::IO) => db.io.step().unwrap(),
            Ok(StepResult::Done) => break,
            result => panic!("unexpected step result: {result:?}"),
        }
    }
    tracing::info!("rows: {:?}", rows);
    assert_eq!(
        rows,
        vec![
            vec![
                turso_core::types::Value::Integer(1),
                turso_core::types::Value::Integer(4096),
            ],
            vec![
                turso_core::types::Value::Integer(3),
                turso_core::types::Value::Integer(3 * 4096),
            ],
        ]
    );

    let mut stmt = conn.prepare("select x, length(y) from b").unwrap();
    let mut rows: Vec<Vec<turso_core::types::Value>> = Vec::new();
    loop {
        let result = stmt.step();
        match result {
            Ok(StepResult::Row) => rows.push(stmt.row().unwrap().get_values().cloned().collect()),
            Ok(StepResult::IO) => db.io.step().unwrap(),
            Ok(StepResult::Done) => break,
            result => panic!("unexpected step result: {result:?}"),
        }
    }
    tracing::info!("rows: {:?}", rows);
    assert_eq!(
        rows,
        vec![vec![
            turso_core::types::Value::Integer(4),
            turso_core::types::Value::Integer(4 * 4096),
        ]]
    );
}

// TODO: see later how this test should work with mvcc
#[test]
fn test_db_share_same_file() {
    let mut path = TempDir::new().unwrap().keep();
    let (mut rng, _) = rng_from_time();
    path.push(format!("test-{}.db", rng.next_u32()));

    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db_file = io
        .open_file(path.to_str().unwrap(), turso_core::OpenFlags::Create, false)
        .unwrap();
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(db_file));
    let db1 = turso_core::Database::open_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        db_file.clone(),
        turso_core::OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn1 = db1.connect().unwrap();
    conn1.wal_auto_checkpoint_disable();

    conn1.execute("create table a(x, y)").unwrap();
    conn1
        .execute("insert into a values (1, randomblob(1 * 4096))")
        .unwrap();
    conn1
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();

    conn1
        .execute("insert into a values (2, randomblob(2 * 4096))")
        .unwrap();

    let db2 = turso_core::Database::open_with_flags_bypass_registry(
        io.clone(),
        path.to_str().unwrap(),
        &format!("{}-wal-copy", path.to_str().unwrap()),
        db_file.clone(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn2 = db2.connect().unwrap();
    conn2.wal_auto_checkpoint_disable();

    let mut stmt = conn2.prepare("select x, length(y) from a").unwrap();
    let mut rows: Vec<Vec<turso_core::types::Value>> = Vec::new();
    loop {
        let result = stmt.step();
        match result {
            Ok(StepResult::Row) => rows.push(stmt.row().unwrap().get_values().cloned().collect()),
            Ok(StepResult::IO) => db2.io.step().unwrap(),
            Ok(StepResult::Done) => break,
            result => panic!("unexpected step result: {result:?}"),
        }
    }
    tracing::info!("rows: {:?}", rows);
    assert_eq!(
        rows,
        vec![vec![
            turso_core::types::Value::Integer(1),
            turso_core::types::Value::Integer(4096),
        ]]
    );
}

#[turso_macros::test(mvcc)]
fn test_wal_api_simulate_spilled_frames(db: TempDatabase) {
    let (mut rng, _) = rng_from_time();
    let opts = db.db_opts;
    let flags = db.db_flags;
    let builder = TempDatabase::builder().with_flags(flags).with_opts(opts);

    let db1 = builder.clone().build();
    let conn1 = db1.connect_limbo();
    let db2 = builder.build();
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    let watermark = conn1.wal_state().unwrap().max_frame;
    for _ in 0..128 {
        let key = rng.next_u32();
        let length = rng.next_u32() % 4096 + 1;
        conn1
            .execute(format!(
                "INSERT INTO t VALUES ({key}, randomblob({length}))"
            ))
            .unwrap();
    }
    let mut frame = [0u8; 24 + 4096];
    conn2
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    conn2.wal_insert_begin().unwrap();
    let frames_count = conn1.wal_state().unwrap().max_frame;
    for frame_id in watermark + 1..=frames_count {
        let mut info = conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        info.db_size = 0;
        info.put_to_frame_header(&mut frame);
        conn2
            .wal_insert_frame(frame_id - watermark, &frame)
            .unwrap();
    }
    for _ in 0..128 {
        let key = rng.next_u32();
        let length = rng.next_u32() % 4096 + 1;
        conn2
            .execute(format!(
                "INSERT INTO t VALUES ({key}, randomblob({length}))"
            ))
            .unwrap();
    }
}
