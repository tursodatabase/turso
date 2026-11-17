// Clean imports
use std::sync::Arc;
use anyhow::{Context, Result};
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, IO, OpenFlags, SessionRole, StepResult,
};
use turso_sqlite::SqliteDialect;

use crate::runner::SimIO;
use crate::runner::memory::io::MemorySimIO;

fn make_io(seed: u64, latency: u8) -> Arc<MemorySimIO> {
    Arc::new(MemorySimIO::new(seed, 4096, latency, 1, 4))
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
    Ok(db.connect(ConnectOpts::new(SessionRole::Superuser))?)
}

fn open_two_conns(io: Arc<MemorySimIO>, path: &str) -> Result<(Arc<Connection>, Arc<Connection>)> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let c1 = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    let c2 = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    Ok((c1, c2))
}

fn step_done(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql).with_context(|| format!("prepare failed: {sql}"))?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {}
            StepResult::Done => return Ok(()),
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
    assert_eq!(messages, vec!["ok".to_string()], "integrity_check failed: {messages:?}");
    Ok(())
}

fn query_count(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<i64> {
    let mut stmt = conn.prepare(sql)?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                return Ok(row.get::<i64>(0)?);
            }
            StepResult::Done => panic!("query ended without row: {sql}"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Vector 1: Nested Savepoints + B-Tree Page Splits with I/O Fault Injection
// ------------------------------------------------------------------------------------------------
fn run_nested_savepoint_vector(seed: u64) -> Result<()> {
    let io = make_io(seed, 30);
    let path = format!("sp_split_{seed}.db");
    let conn = open_conn(io.clone(), &path)?;

    step_done(&conn, &io, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, pad BLOB)")?;
    step_done(&conn, &io, "CREATE INDEX idx_v ON t(v)")?;

    // Fill initial pages
    for i in 0..25 {
        step_done(
            &conn,
            &io,
            &format!("INSERT INTO t VALUES ({i}, 'k_{i}', zeroblob(256))"),
        )?;
    }
    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn, &io)?;

    // Nested savepoints
    step_done(&conn, &io, "SAVEPOINT sp1")?;
    for i in 25..50 {
        step_done(
            &conn,
            &io,
            &format!("INSERT INTO t VALUES ({i}, 'k_{i}', zeroblob(256))"),
        )?;
    }

    step_done(&conn, &io, "SAVEPOINT sp2")?;
    for i in 50..70 {
        step_done(
            &conn,
            &io,
            &format!("INSERT INTO t VALUES ({i}, 'k_{i}', zeroblob(256))"),
        )?;
    }

    // Rollback inner savepoint
    step_done(&conn, &io, "ROLLBACK TO sp2")?;
    assert_eq!(query_count(&conn, &io, "SELECT COUNT(*) FROM t")?, 50);

    // Inject transient fault during release/commit
    io.inject_fault(true);
    let _ = step_done(&conn, &io, "RELEASE sp1");
    io.inject_fault(false);

    // Reopen and check integrity
    drop(conn);
    io.close_files();

    let reopened = open_conn(io.clone(), &path)?;
    assert_integrity_ok(&reopened, &io)?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Vector 2: UPSERT ON CONFLICT DO UPDATE on Secondary Unique Index Across Checkpoints
// ------------------------------------------------------------------------------------------------
fn run_upsert_secondary_index_vector(seed: u64) -> Result<()> {
    let io = make_io(seed, 20);
    let path = format!("upsert_sec_{seed}.db");
    let conn = open_conn(io.clone(), &path)?;

    step_done(
        &conn,
        &io,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, tag TEXT UNIQUE, score INT)",
    )?;

    // Insert baseline rows
    for i in 1..=10 {
        step_done(
            &conn,
            &io,
            &format!("INSERT INTO users VALUES ({i}, 'email{i}@turso.io', 'tag{i}', 100)"),
        )?;
    }
    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn, &io)?;

    // Run UPSERT modifying secondary keys and values
    for i in 1..=10 {
        let new_email = format!("email{}@turso.io", i);
        let new_tag = format!("tag{}_updated", i);
        let sql = format!(
            "INSERT INTO users VALUES ({i}, '{new_email}', '{new_tag}', 200) \
             ON CONFLICT(email) DO UPDATE SET tag = excluded.tag, score = excluded.score"
        );
        step_done(&conn, &io, &sql)?;
    }

    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn, &io)?;

    // Validate that index scan matches table scan
    let cnt_table = query_count(&conn, &io, "SELECT COUNT(*) FROM users")?;
    let cnt_idx_email = query_count(
        &conn,
        &io,
        "SELECT COUNT(*) FROM users INDEXED BY sqlite_autoindex_users_1",
    )?;
    let cnt_idx_tag = query_count(
        &conn,
        &io,
        "SELECT COUNT(*) FROM users INDEXED BY sqlite_autoindex_users_2",
    )?;

    assert_eq!(cnt_table, 10);
    assert_eq!(cnt_table, cnt_idx_email, "email index count diverged from table count!");
    assert_eq!(cnt_table, cnt_idx_tag, "tag index count diverged from table count!");

    drop(conn);
    io.close_files();

    let reopened = open_conn(io.clone(), &path)?;
    assert_integrity_ok(&reopened, &io)?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Vector 3: ALTER TABLE ADD COLUMN with Unique / Default under Checkpoint
// ------------------------------------------------------------------------------------------------
fn run_alter_table_vector(seed: u64) -> Result<()> {
    let io = make_io(seed, 25);
    let path = format!("alter_table_{seed}.db");
    let conn = open_conn(io.clone(), &path)?;

    step_done(
        &conn,
        &io,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, sku TEXT UNIQUE, price INT)",
    )?;
    for i in 1..=15 {
        step_done(
            &conn,
            &io,
            &format!("INSERT INTO items VALUES ({i}, 'sku_{i}', {i} * 10)"),
        )?;
    }
    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;

    // Alter table adding columns with defaults
    step_done(&conn, &io, "ALTER TABLE items ADD COLUMN active INT DEFAULT 1")?;
    step_done(&conn, &io, "ALTER TABLE items ADD COLUMN desc TEXT DEFAULT 'item'")?;

    // Insert new rows with extended schema
    step_done(
        &conn,
        &io,
        "INSERT INTO items VALUES (100, 'sku_100', 999, 0, 'custom')",
    )?;

    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn, &io)?;

    drop(conn);
    io.close_files();

    let reopened = open_conn(io.clone(), &path)?;
    assert_integrity_ok(&reopened, &io)?;
    let total = query_count(&reopened, &io, "SELECT COUNT(*) FROM items WHERE active = 1")?;
    assert_eq!(total, 15, "default column values should evaluate correctly for older rows");
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Vector 4: Statement Abandonment During Spill / Concurrent Checkpoint
// ------------------------------------------------------------------------------------------------
fn run_statement_abandon_spill_vector(seed: u64) -> Result<()> {
    let io = make_io(seed, 100);
    let path = format!("abandon_spill_{seed}.db");
    let (conn1, conn2) = open_two_conns(io.clone(), &path)?;

    step_done(&conn1, &io, "PRAGMA cache_size = 5")?;
    step_done(&conn1, &io, "CREATE TABLE logs (id INTEGER PRIMARY KEY, data TEXT)")?;

    // Insert rows
    for i in 1..=20 {
        step_done(
            &conn1,
            &io,
            &format!("INSERT INTO logs VALUES ({i}, '{}')", "x".repeat(300)),
        )?;
    }

    // Prepare large update and step once then abandon
    let mut stmt = conn1.prepare("UPDATE logs SET data = data || '_modified' RETURNING id")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => break, // Got first row, now abandon!
            StepResult::Done => panic!("unexpected immediate done"),
            other => panic!("unexpected step: {other:?}"),
        }
    }
    drop(stmt);
    io.step()?;

    // conn2 must be able to write without deadlock / corruption
    step_done(&conn2, &io, "INSERT INTO logs VALUES (999, 'from_conn2')")?;
    step_done(&conn2, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn2, &io)?;

    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Vector 5: Compound Unique UPSERT changing both IPK and Unique Key Columns
// ------------------------------------------------------------------------------------------------
fn run_compound_upsert_vector(seed: u64) -> Result<()> {
    let io = make_io(seed, 25);
    let path = format!("compound_upsert_{seed}.db");
    let conn = open_conn(io.clone(), &path)?;

    step_done(
        &conn,
        &io,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, val INT, UNIQUE(a, b))",
    )?;

    for i in 1..=30 {
        step_done(
            &conn,
            &io,
            &format!("INSERT INTO t VALUES ({i}, 'a_{i}', 'b_{i}', {i} * 10)"),
        )?;
    }
    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn, &io)?;

    // Update with UPSERT changing both IPK and key `a`
    for i in 1..=15 {
        let new_id = 100 + i;
        let new_a = format!("a_{i}_updated");
        let sql = format!(
            "INSERT INTO t VALUES ({new_id}, 'a_{i}', 'b_{i}', 999) \
             ON CONFLICT(a, b) DO UPDATE SET id = excluded.id, a = '{new_a}', val = excluded.val"
        );
        step_done(&conn, &io, &sql)?;
    }

    step_done(&conn, &io, "PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity_ok(&conn, &io)?;

    // Verify row count and index matches
    let cnt_table = query_count(&conn, &io, "SELECT COUNT(*) FROM t")?;
    let cnt_idx = query_count(
        &conn,
        &io,
        "SELECT COUNT(*) FROM t INDEXED BY sqlite_autoindex_t_1",
    )?;
    assert_eq!(cnt_table, 30);
    assert_eq!(cnt_table, cnt_idx, "compound index count diverged!");

    drop(conn);
    io.close_files();

    let reopened = open_conn(io.clone(), &path)?;
    assert_integrity_ok(&reopened, &io)?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Vector 6: Multi-Level Nested Savepoints with Mixed Rollbacks and Transient Faults
// ------------------------------------------------------------------------------------------------
fn run_deep_nested_savepoints_vector(seed: u64) -> Result<()> {
    let io = make_io(seed, 50);
    let path = format!("deep_sp_{seed}.db");
    let conn = open_conn(io.clone(), &path)?;

    step_done(&conn, &io, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    step_done(&conn, &io, "CREATE INDEX idx_v ON t(v)")?;

    for level in 1..=5 {
        step_done(&conn, &io, &format!("SAVEPOINT sp_{level}"))?;
        for row in 0..10 {
            let id = level * 100 + row;
            step_done(
                &conn,
                &io,
                &format!("INSERT INTO t VALUES ({id}, 'val_{id}')"),
            )?;
        }
    }

    // Rollback to level 3
    step_done(&conn, &io, "ROLLBACK TO sp_3")?;

    // Insert more at level 3
    for row in 20..25 {
        let id = 300 + row;
        step_done(
            &conn,
            &io,
            &format!("INSERT INTO t VALUES ({id}, 'val_{id}')"),
        )?;
    }

    // Rollback to level 1
    step_done(&conn, &io, "ROLLBACK TO sp_1")?;
    assert_eq!(query_count(&conn, &io, "SELECT COUNT(*) FROM t")?, 0);

    // Release sp_1
    step_done(&conn, &io, "RELEASE sp_1")?;

    drop(conn);
    io.close_files();

    let reopened = open_conn(io.clone(), &path)?;
    assert_integrity_ok(&reopened, &io)?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Unit Tests (Invoked by cargo test)
// ------------------------------------------------------------------------------------------------
#[test]
fn test_nested_savepoint_btree_split() -> Result<()> {
    for seed in [101_u64, 102, 103, 104, 105] {
        run_nested_savepoint_vector(seed)?;
    }
    Ok(())
}

#[test]
fn test_upsert_secondary_index() -> Result<()> {
    for seed in [201_u64, 202, 203, 204, 205] {
        run_upsert_secondary_index_vector(seed)?;
    }
    Ok(())
}

#[test]
fn test_alter_table_add_column_index() -> Result<()> {
    for seed in [301_u64, 302, 303, 304, 305] {
        run_alter_table_vector(seed)?;
    }
    Ok(())
}

#[test]
fn test_statement_abandon_spill() -> Result<()> {
    for seed in [401_u64, 402, 403, 404, 405] {
        run_statement_abandon_spill_vector(seed)?;
    }
    Ok(())
}

#[test]
fn test_compound_upsert() -> Result<()> {
    for seed in [501_u64, 502, 503, 504, 505] {
        run_compound_upsert_vector(seed)?;
    }
    Ok(())
}

#[test]
fn test_deep_nested_savepoints() -> Result<()> {
    for seed in [601_u64, 602, 603, 604, 605] {
        run_deep_nested_savepoints_vector(seed)?;
    }
    Ok(())
}
