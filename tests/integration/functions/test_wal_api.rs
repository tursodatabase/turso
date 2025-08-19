use std::{collections::HashSet, path::PathBuf, sync::Arc};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rusqlite::types::Value;
use turso_core::{types::WalFrameInfo, CheckpointMode, LimboError, StepResult};

use crate::common::{limbo_exec_rows, rng_from_time, TempDatabase};

#[test]
fn test_wal_frame_count() {
    let db = TempDatabase::new_empty(false);
    let conn = db.connect_limbo();
    assert_eq!(conn.wal_frame_count().unwrap(), 0);
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    assert_eq!(conn.wal_frame_count().unwrap(), 2);
    conn.execute("INSERT INTO t VALUES (10, 10), (5, 1)")
        .unwrap();
    assert_eq!(conn.wal_frame_count().unwrap(), 3);
    conn.execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn.wal_frame_count().unwrap(), 15);
}

#[test]
fn test_wal_frame_transfer_no_schema_changes() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
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
    assert_eq!(conn1.wal_frame_count().unwrap(), 15);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    let frames_count = conn1.wal_frame_count().unwrap();
    for frame_id in 1..=frames_count {
        conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        conn2.wal_insert_frame(frame_id, &frame).unwrap();
    }

    conn2.wal_insert_end().unwrap();
    assert_eq!(conn2.wal_frame_count().unwrap(), 15);
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, length(y) FROM t"),
        vec![
            vec![Value::Integer(5), Value::Integer(1)],
            vec![Value::Integer(10), Value::Integer(2)],
            vec![Value::Integer(1024), Value::Integer(40960)],
        ]
    );
}

#[test]
fn test_wal_frame_transfer_various_schema_changes() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
    let conn2 = db2.connect_limbo();
    let conn3 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    let mut frame = [0u8; 24 + 4096];
    let mut synced_frame = 0;
    let mut sync = || {
        let last_frame = conn1.wal_frame_count().unwrap();
        conn2.wal_insert_begin().unwrap();
        for frame_id in (synced_frame + 1)..=last_frame {
            conn1.wal_get_frame(frame_id, &mut frame).unwrap();
            conn2.wal_insert_frame(frame_id, &frame).unwrap();
        }
        conn2.wal_insert_end().unwrap();
        synced_frame = last_frame;
    };

    sync();
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT * FROM t"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
    assert_eq!(
        limbo_exec_rows(&db2, &conn3, "SELECT * FROM t"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );

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
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT 1 FROM a UNION ALL SELECT 1 FROM b"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
    assert_eq!(
        limbo_exec_rows(&db2, &conn3, "SELECT 1 FROM a UNION ALL SELECT 1 FROM b"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
}

#[test]
fn test_wal_frame_transfer_schema_changes() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
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
    assert_eq!(conn1.wal_frame_count().unwrap(), 15);
    let mut frame = [0u8; 24 + 4096];
    let mut commits = 0;
    conn2.wal_insert_begin().unwrap();
    for frame_id in 1..=conn1.wal_frame_count().unwrap() {
        conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        let info = conn2.wal_insert_frame(frame_id, &frame).unwrap();
        if info.is_commit_frame() {
            commits += 1;
        }
    }
    conn2.wal_insert_end().unwrap();
    assert_eq!(commits, 3);
    assert_eq!(conn2.wal_frame_count().unwrap(), 15);
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, length(y) FROM t"),
        vec![
            vec![Value::Integer(5), Value::Integer(1)],
            vec![Value::Integer(10), Value::Integer(2)],
            vec![Value::Integer(1024), Value::Integer(40960)],
        ]
    );
}

#[test]
fn test_wal_frame_transfer_no_schema_changes_rollback() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
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
    assert_eq!(conn1.wal_frame_count().unwrap(), 14);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    // Intentionally leave out the final commit frame, so the big randomblob is not committed and should not be visible to transactions.
    for frame_id in 1..=(conn1.wal_frame_count().unwrap() - 1) {
        conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        conn2.wal_insert_frame(frame_id, &frame).unwrap();
    }
    conn2.wal_insert_end().unwrap();
    assert_eq!(conn2.wal_frame_count().unwrap(), 2);
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, length(y) FROM t"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
    conn2.execute("CREATE TABLE q(x)").unwrap();
    conn2
        .execute("INSERT INTO q VALUES (randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, LENGTH(y) FROM t"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
}

#[test]
fn test_wal_frame_transfer_schema_changes_rollback() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_frame_count().unwrap(), 14);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    for frame_id in 1..=(conn1.wal_frame_count().unwrap() - 1) {
        conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        conn2.wal_insert_frame(frame_id, &frame).unwrap();
    }
    conn2.wal_insert_end().unwrap();
    assert_eq!(conn2.wal_frame_count().unwrap(), 2);
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, length(y) FROM t"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
    conn2.execute("CREATE TABLE q(x)").unwrap();
    conn2
        .execute("INSERT INTO q VALUES (randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, LENGTH(y) FROM t"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
}

#[test]
fn test_wal_frame_conflict() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE q(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    assert_eq!(conn1.wal_frame_count().unwrap(), 2);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    conn1.wal_get_frame(1, &mut frame).unwrap();
    assert!(conn2.wal_insert_frame(1, &frame).is_err());
}

#[test]
fn test_wal_frame_far_away_write() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
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
    assert_eq!(conn1.wal_frame_count().unwrap(), 14);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();

    conn1.wal_get_frame(3, &mut frame).unwrap();
    conn2.wal_insert_frame(3, &frame).unwrap();

    conn1.wal_get_frame(5, &mut frame).unwrap();
    assert!(conn2.wal_insert_frame(5, &frame).is_err());
}

#[test]
fn test_wal_frame_api_no_schema_changes_fuzz() {
    let (mut rng, _) = rng_from_time();
    for _ in 0..4 {
        let db1 = TempDatabase::new_empty(false);
        let conn1 = db1.connect_limbo();
        let db2 = TempDatabase::new_empty(false);
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

        let (mut size, mut synced_frame) = (0, conn2.wal_frame_count().unwrap());
        let mut commit_frames = vec![conn1.wal_frame_count().unwrap()];
        for _ in 0..256 {
            if rng.next_u32() % 10 != 0 {
                let key = rng.next_u32();
                let length = rng.next_u32() % (4 * 4096);
                let query = format!("INSERT INTO t VALUES ({key}, randomblob({length}))");
                conn1.execute(&query).unwrap();
                commit_frames.push(conn1.wal_frame_count().unwrap());
            } else {
                let last_frame = conn1.wal_frame_count().unwrap();
                let next_frame =
                    synced_frame + (rng.next_u32() as u64 % (last_frame - synced_frame + 1));
                let mut frame = [0u8; 24 + 4096];
                conn2.wal_insert_begin().unwrap();
                for frame_no in (synced_frame + 1)..=next_frame {
                    conn1.wal_get_frame(frame_no, &mut frame).unwrap();
                    conn2.wal_insert_frame(frame_no, &frame[..]).unwrap();
                }
                conn2.wal_insert_end().unwrap();
                for (i, committed) in commit_frames.iter().enumerate() {
                    if *committed <= next_frame {
                        size = size.max(i);
                        synced_frame = *committed;
                    }
                }
                if rng.next_u32() % 10 == 0 {
                    synced_frame = rng.next_u32() as u64 % synced_frame;
                }
                assert_eq!(
                    limbo_exec_rows(&db2, &conn2, "SELECT COUNT(*) FROM t"),
                    vec![vec![Value::Integer(size as i64)]]
                );
            }
        }
    }
}

#[test]
fn test_wal_api_changed_pages() {
    let db1 = TempDatabase::new_empty(false);
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
    let frames = conn1.wal_frame_count().unwrap();
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
    let frames = conn1.wal_frame_count().unwrap();
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

    let mut frame_no = conn.wal_frame_count().unwrap();
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
    conn.wal_insert_end()?;

    Ok(())
}

#[test]
fn test_wal_api_revert_pages() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    let watermark1 = conn1.wal_frame_count().unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1, randomblob(10))")
        .unwrap();
    let watermark2 = conn1.wal_frame_count().unwrap();

    conn1
        .execute("INSERT INTO t VALUES (3, randomblob(20))")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 2))")
        .unwrap();

    assert_eq!(
        limbo_exec_rows(&db1, &conn1, "SELECT x, length(y) FROM t"),
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(3), Value::Integer(20)],
            vec![Value::Integer(1024), Value::Integer(4096 * 2)],
        ]
    );

    revert_to(&conn1, watermark2).unwrap();

    assert_eq!(
        limbo_exec_rows(&db1, &conn1, "SELECT x, length(y) FROM t"),
        vec![vec![Value::Integer(1), Value::Integer(10)],]
    );

    revert_to(&conn1, watermark1).unwrap();

    assert_eq!(
        limbo_exec_rows(&db1, &conn1, "SELECT x, length(y) FROM t"),
        vec![] as Vec<Vec<Value>>,
    );
}

#[test]
fn test_wal_upper_bound_passive() {
    let db = TempDatabase::new_empty(false);
    let writer = db.connect_limbo();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    let watermark0 = writer.wal_frame_count().unwrap();
    writer
        .execute("insert into test values (1, 'hello')")
        .unwrap();
    let watermark1 = writer.wal_frame_count().unwrap();
    writer
        .execute("insert into test values (2, 'turso')")
        .unwrap();
    let watermark2 = writer.wal_frame_count().unwrap();
    let expected = vec![
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
        let db_copy = TempDatabase::new_with_existent(&PathBuf::from(db_path_copy), false);
        let conn = db_copy.connect_limbo();
        let mut stmt = conn.prepare("select * from test").unwrap();
        let mut rows: Vec<Vec<turso_core::types::Value>> = Vec::new();
        loop {
            let result = stmt.step();
            match result {
                Ok(StepResult::Row) => {
                    rows.push(stmt.row().unwrap().get_values().cloned().collect())
                }
                Ok(StepResult::IO) => db_copy.io.run_once().unwrap(),
                Ok(StepResult::Done) => break,
                result => panic!("unexpected step result: {result:?}"),
            }
        }
        assert_eq!(rows, expected[0..prefix]);
    }
}

#[test]
fn test_wal_upper_bound_truncate() {
    let db = TempDatabase::new_empty(false);
    let writer = db.connect_limbo();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    writer
        .execute("insert into test values (1, 'hello')")
        .unwrap();
    let watermark = writer.wal_frame_count().unwrap();
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
}
