use std::sync::Arc;

use anyhow::Result;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, IO, OpenFlags, SqliteDialect, StepResult,
};

use crate::runner::memory::io::MemorySimIO;

fn make_io(seed: u64, latency_probability: u8) -> Arc<MemorySimIO> {
    Arc::new(MemorySimIO::new(seed, 4096, latency_probability, 1, 4))
}

fn open_conn(io: Arc<MemorySimIO>, path: &str) -> Result<Arc<Connection>> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    Ok(db.connect(ConnectOpts::new())?)
}

fn step_done(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {}
            StepResult::Done => return Ok(()),
            other => panic!("unexpected step result for `{sql}`: {other:?}"),
        }
    }
}

fn query_pairs(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push((row.get::<i64>(0)?, row.get::<String>(1)?));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result for `{sql}`: {other:?}"),
        }
    }
}

fn assert_integrity_ok(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let mut messages = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("integrity_check row");
                messages.push(row.get::<String>(0)?);
            }
            StepResult::Done => break,
            other => panic!("unexpected step result for integrity_check: {other:?}"),
        }
    }
    assert_eq!(messages, vec!["ok".to_string()]);
    Ok(())
}

/// Regression test for #8067: checkpointed MVCC rows updated through
/// `ON CONFLICT(v) DO UPDATE SET id = excluded.id` must re-seek the underlying
/// B-tree cursor after `NotExists` misses on the new rowid (moving upward,
/// moving downward, and moving onto a rowid whose B-tree entry was shadowed by
/// an MVCC delete).
#[test]
fn mvcc_checkpointed_upsert_changing_ipk_repro() -> Result<()> {
    let io = make_io(8067, 0);
    let path = "mvcc_upsert_ipk.db";
    let conn = open_conn(io.clone(), path)?;

    step_done(&conn, &io, "PRAGMA journal_mode = 'mvcc'")?;
    step_done(
        &conn,
        &io,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
    )?;
    step_done(&conn, &io, "INSERT INTO t VALUES (1, 'one'), (2, 'two')")?;
    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;

    // Move rowid above existing B-tree max (btree_cursor.exists(3) lands at EOF).
    step_done(
        &conn,
        &io,
        "INSERT INTO t VALUES (3, 'one') ON CONFLICT(v) DO UPDATE SET id = excluded.id",
    )?;
    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn, &io)?;

    assert_eq!(
        query_pairs(&conn, &io, "SELECT id, v FROM t ORDER BY id")?,
        vec![(2, "two".to_string()), (3, "one".to_string())]
    );
    assert_eq!(
        query_pairs(
            &conn,
            &io,
            "SELECT id, v FROM t INDEXED BY sqlite_autoindex_t_1 ORDER BY v"
        )?,
        vec![(3, "one".to_string()), (2, "two".to_string())]
    );

    // Move rowid below existing B-tree min, and also onto a rowid deleted in MVCC.
    step_done(
        &conn,
        &io,
        "INSERT INTO t VALUES (0, 'one') ON CONFLICT(v) DO UPDATE SET id = excluded.id",
    )?;
    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    step_done(&conn, &io, "DELETE FROM t WHERE id = 2")?;
    step_done(
        &conn,
        &io,
        "INSERT INTO t VALUES (2, 'one') ON CONFLICT(v) DO UPDATE SET id = excluded.id",
    )?;
    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn, &io)?;

    conn.close()?;
    let reopened = open_conn(io.clone(), path)?;
    assert_integrity_ok(&reopened, &io)?;
    assert_eq!(
        query_pairs(&reopened, &io, "SELECT id, v FROM t ORDER BY id")?,
        vec![(2, "one".to_string())]
    );
    Ok(())
}

#[test]
fn mvcc_checkpointed_upsert_changing_ipk_under_io_latency() -> Result<()> {
    for seed in [11_u64, 42, 8067] {
        let io = make_io(seed, 55);
        let path = format!("mvcc_upsert_ipk_{seed}.db");
        let conn = open_conn(io.clone(), &path)?;
        step_done(&conn, &io, "PRAGMA journal_mode = 'mvcc'")?;
        step_done(
            &conn,
            &io,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE, pad BLOB)",
        )?;
        for slot in 0..6_i64 {
            let sql = format!("INSERT INTO t VALUES ({slot}, 'k_{slot}', zeroblob(512))");
            step_done(&conn, &io, &sql)?;
        }
        step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;

        for round in 1..=6_i64 {
            let slot = round % 6;
            let new_id = round * 16 + slot;
            let sql = format!(
                "INSERT INTO t VALUES ({new_id}, 'k_{slot}', zeroblob(512)) \
                 ON CONFLICT(v) DO UPDATE SET id = excluded.id"
            );
            step_done(&conn, &io, &sql)?;
            if round % 2 == 0 {
                step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
                assert_integrity_ok(&conn, &io)?;
            }
        }
    }
    Ok(())
}
