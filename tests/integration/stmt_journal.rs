/// Tests that the `needs_stmt_subtransactions` flag is correctly set based on
/// the is_multi_write and may_abort analysis during statement compilation.
///
/// These tests mirror SQLite's usesStmtJournal = isMultiWrite && mayAbort (vdbeaux.c:2685).
use crate::common::TempDatabase;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Returns true if the statement will need a statement subtransaction
/// if the statement is part of an interactive transaction.
fn needs_stmt_journal(conn: &Arc<turso_core::Connection>, sql: &str) -> bool {
    let stmt = conn.prepare(sql).unwrap();
    stmt.get_program()
        .prepared()
        .needs_stmt_subtransactions
        .load(Ordering::Relaxed)
}

// ──────────────────────────────────────────────────────────
// INSERT
// ──────────────────────────────────────────────────────────

/// Single-row INSERT into a table with no constraints, no triggers, no FKs.
/// Neither multi-write nor may-abort.
#[turso_macros::test(init_sql = "CREATE TABLE t (a, b, c);")]
fn insert_single_row_no_constraints(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(&conn, "INSERT INTO t VALUES (1, 2, 3)"));
    Ok(())
}

/// Multi-row INSERT (INSERT ... VALUES (...), (...)) is multi-write.
#[turso_macros::test(init_sql = "CREATE TABLE t (a, b, c);")]
fn insert_multi_row_no_constraints(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    // Multi-row but no may_abort → still no stmt journal.
    assert!(!needs_stmt_journal(
        &conn,
        "INSERT INTO t VALUES (1,2,3),(4,5,6)"
    ));
    Ok(())
}

/// INSERT with NOT NULL constraint and default (Abort) conflict resolution → may_abort.
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b);")]
fn insert_single_row_notnull(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    // Single-row (not multi-write) but may_abort.
    // needs_stmt = multi_write && may_abort = false && true = false.
    assert!(!needs_stmt_journal(&conn, "INSERT INTO t VALUES (1, 2)"));
    Ok(())
}

/// Multi-row INSERT with NOT NULL → multi-write AND may-abort → needs stmt journal.
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b);")]
fn insert_multi_row_notnull(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(needs_stmt_journal(
        &conn,
        "INSERT INTO t VALUES (1, 2), (3, 4)"
    ));
    Ok(())
}

/// INSERT with UNIQUE constraint → may_abort.
#[turso_macros::test(init_sql = "CREATE TABLE t (a UNIQUE, b);")]
fn insert_multi_row_unique(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(needs_stmt_journal(
        &conn,
        "INSERT INTO t VALUES (1, 2), (3, 4)"
    ));
    Ok(())
}

/// INSERT OR IGNORE with UNIQUE → may_abort is false (not OE_Abort).
#[turso_macros::test(init_sql = "CREATE TABLE t (a UNIQUE, b);")]
fn insert_or_ignore_multi_row_unique(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(
        &conn,
        "INSERT OR IGNORE INTO t VALUES (1, 2), (3, 4)"
    ));
    Ok(())
}

/// INSERT OR REPLACE is multi-write (REPLACE may delete conflicting rows).
/// But may_abort=false (conflict resolution is not OE_Abort).
/// needs_stmt = true && false = false.
#[turso_macros::test(init_sql = "CREATE TABLE t (a UNIQUE, b);")]
fn insert_or_replace_single_row(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(
        &conn,
        "INSERT OR REPLACE INTO t VALUES (1, 2)"
    ));
    Ok(())
}

/// AUTOINCREMENT is multi-write (writes to sqlite_sequence).
/// No constraints → may_abort=false. needs_stmt = true && false = false.
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v);")]
fn insert_autoincrement_single_row(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(&conn, "INSERT INTO t (v) VALUES (1)"));
    Ok(())
}

/// AUTOINCREMENT + NOT NULL → multi-write AND may-abort.
#[turso_macros::test(
    init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v NOT NULL);"
)]
fn insert_autoincrement_notnull(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(needs_stmt_journal(&conn, "INSERT INTO t (v) VALUES (1)"));
    Ok(())
}

/// INSERT with SELECT as source is multi-row.
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL);")]
fn insert_select_source(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE s (x)")?;
    assert!(needs_stmt_journal(&conn, "INSERT INTO t SELECT x FROM s"));
    Ok(())
}

// ──────────────────────────────────────────────────────────
// UPDATE
// ──────────────────────────────────────────────────────────

/// UPDATE with no WHERE (table scan) is multi-write.
#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn update_no_where(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    // Multi-write (table scan), but no constraints → may_abort=false.
    assert!(!needs_stmt_journal(&conn, "UPDATE t SET a = 1"));
    Ok(())
}

/// UPDATE with no WHERE + NOT NULL → multi-write AND may-abort.
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b);")]
fn update_no_where_notnull(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(needs_stmt_journal(&conn, "UPDATE t SET a = 1"));
    Ok(())
}

/// UPDATE WHERE rowid = ? → single-row, not multi-write.
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b);")]
fn update_by_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    // Single-row → not multi-write. may_abort=true (NOT NULL).
    // needs_stmt = false && true = false.
    assert!(!needs_stmt_journal(
        &conn,
        "UPDATE t SET a = 1 WHERE rowid = 5"
    ));
    Ok(())
}

/// UPDATE WHERE id = ? with id as rowid alias → single-row.
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, a NOT NULL, b);")]
fn update_by_rowid_alias(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(
        &conn,
        "UPDATE t SET a = 1 WHERE id = 5"
    ));
    Ok(())
}

/// UPDATE WHERE unique_col = ? → single-row (unique index equality seek).
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b UNIQUE);")]
fn update_by_unique_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(&conn, "UPDATE t SET a = 1 WHERE b = 5"));
    Ok(())
}

/// UPDATE WHERE non_unique_col = ? → multi-write (scan, not a point lookup).
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b);")]
fn update_by_non_unique_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE INDEX idx_b ON t(b)")?;
    assert!(needs_stmt_journal(&conn, "UPDATE t SET a = 1 WHERE b = 5"));
    Ok(())
}

/// UPDATE with no index on WHERE column → table scan → multi-write.
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b);")]
fn update_unindexed_where(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(needs_stmt_journal(&conn, "UPDATE t SET a = 1 WHERE b = 5"));
    Ok(())
}

/// UPDATE OR REPLACE → always multi-write (can delete conflicting rows).
/// But may_abort = false (conflict resolution is not OE_Abort).
/// needs_stmt = true && false = false.
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, a UNIQUE);")]
fn update_or_replace_by_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(
        &conn,
        "UPDATE OR REPLACE t SET a = 1 WHERE id = 5"
    ));
    Ok(())
}

/// UPDATE with composite unique index — all columns constrained → single-row.
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b, c);")]
fn update_composite_unique_all_cols(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE UNIQUE INDEX idx_bc ON t(b, c)")?;
    assert!(!needs_stmt_journal(
        &conn,
        "UPDATE t SET a = 1 WHERE b = 1 AND c = 2"
    ));
    Ok(())
}

/// UPDATE with composite unique index — only partial columns constrained → multi-row.
#[turso_macros::test(init_sql = "CREATE TABLE t (a NOT NULL, b, c);")]
fn update_composite_unique_partial_cols(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE UNIQUE INDEX idx_bc ON t(b, c)")?;
    assert!(needs_stmt_journal(&conn, "UPDATE t SET a = 1 WHERE b = 1"));
    Ok(())
}

// ──────────────────────────────────────────────────────────
// DELETE
// ──────────────────────────────────────────────────────────

/// DELETE with no WHERE (table scan) + no constraints → multi-write, no may-abort.
#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn delete_no_where(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(&conn, "DELETE FROM t"));
    Ok(())
}

/// DELETE WHERE rowid = ? → single-row.
#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn delete_by_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(&conn, "DELETE FROM t WHERE rowid = 5"));
    Ok(())
}

/// DELETE WHERE unique_col = ? → single-row.
#[turso_macros::test(init_sql = "CREATE TABLE t (a, b UNIQUE);")]
fn delete_by_unique_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(&conn, "DELETE FROM t WHERE b = 5"));
    Ok(())
}

/// DELETE with no WHERE on table with FK → may-abort.
#[turso_macros::test(init_sql = "CREATE TABLE parent (id INTEGER PRIMARY KEY);")]
fn delete_with_fk(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON")?;
    conn.execute("CREATE TABLE child (pid REFERENCES parent(id))")?;
    // multi-write (table scan) && may_abort (FK) → true.
    assert!(needs_stmt_journal(&conn, "DELETE FROM parent"));
    Ok(())
}

/// DELETE WHERE rowid = ? on table with FK → still multi-write because FK counter
/// modifications need statement journal protection. Mirrors SQLite's sqlite3VdbeMultiWrite
/// (fkey.c:452-453): always multi-write when FKs are active.
#[turso_macros::test(init_sql = "CREATE TABLE parent (id INTEGER PRIMARY KEY);")]
fn delete_by_rowid_with_fk(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON")?;
    conn.execute("CREATE TABLE child (pid REFERENCES parent(id))")?;
    // FK constraint checks modify deferred violation counter before the statement can abort,
    // so multi_write stays true. needs_stmt = true && true = true.
    assert!(needs_stmt_journal(&conn, "DELETE FROM parent WHERE id = 5"));
    Ok(())
}

// ──────────────────────────────────────────────────────────
// SELECT (read-only) — never needs stmt journal
// ──────────────────────────────────────────────────────────

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn select_never_needs_stmt_journal(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    assert!(!needs_stmt_journal(&conn, "SELECT * FROM t"));
    assert!(!needs_stmt_journal(&conn, "SELECT * FROM t WHERE a = 1"));
    Ok(())
}

// ──────────────────────────────────────────────────────────
// UPDATE with ephemeral table (key mutation) + FK
// ──────────────────────────────────────────────────────────

/// UPDATE on a parent FK table that mutates the PK (key mutation → ephemeral table).
/// The ephemeral table rewrite must NOT hide the FK relationship from the stmt journal check.
#[turso_macros::test(init_sql = "CREATE TABLE p (id INTEGER PRIMARY KEY, val TEXT);")]
fn update_fk_parent_key_mutation_ephemeral(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON")?;
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)")?;
    // Changing the PK triggers ephemeral table creation (KeyMutation safety).
    // But p is referenced by c, so has_fks = true → needs stmt journal.
    assert!(needs_stmt_journal(
        &conn,
        "UPDATE p SET id = 99 WHERE id = 1"
    ));
    Ok(())
}

/// UPDATE on a child FK table that mutates the PK (key mutation → ephemeral table).
#[turso_macros::test(init_sql = "CREATE TABLE p (id INTEGER PRIMARY KEY, val TEXT);")]
fn update_fk_child_key_mutation_ephemeral(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON")?;
    conn.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)")?;
    // Changing c's PK triggers ephemeral table. c has child FKs → needs stmt journal.
    assert!(needs_stmt_journal(
        &conn,
        "UPDATE c SET id = 99 WHERE id = 1"
    ));
    Ok(())
}
