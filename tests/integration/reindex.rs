use crate::queued_io::{QueuedIo, QueuedIoOpKind};
use std::sync::{atomic::Ordering, Arc};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, StepResult};

/// Opens a database on `QueuedIo` so tests can control when pending writes complete.
fn open_queued_db(io: Arc<QueuedIo>, path: &str) -> anyhow::Result<Arc<Database>> {
    Ok(Database::open_file_with_flags(
        io,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?)
}

/// Executes a query and renders rows as pipe-separated values for compact assertions.
fn query_rows(conn: &Arc<Connection>, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values: Vec<String> = row.get_values().map(|value| format!("{value}")).collect();
        rows.push(values.join("|"));
        Ok(())
    })?;
    Ok(rows)
}

/// Returns whether compiling `sql` marked the program as needing statement rollback.
fn needs_stmt_journal(conn: &Arc<Connection>, sql: &str) -> bool {
    let stmt = conn.prepare(sql).unwrap();
    stmt.get_program()
        .prepared()
        .needs_stmt_subtransactions
        .load(Ordering::Relaxed)
}

/// Creates a table with enough indexed data to force REINDEX through sorter and pager I/O.
fn populate_reindex_fixture(conn: &Arc<Connection>, rows: usize) -> anyhow::Result<()> {
    conn.execute("PRAGMA page_size = 512")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, c TEXT)")?;
    conn.execute("CREATE INDEX idx_a ON t(a COLLATE NOCASE)")?;
    conn.execute("CREATE INDEX idx_b_partial ON t(b) WHERE b % 2 = 0")?;
    conn.execute("CREATE INDEX idx_expr ON t(substr(c, 1, 2), id DESC)")?;
    for i in 0..rows {
        let rev = rows - i;
        let a = format!("value{rev:04}");
        let b = (i % 97) as i64 - 48;
        let c = format!("payload{i:04}");
        conn.execute(format!("INSERT INTO t VALUES({i}, '{a}', {b}, '{c}')"))?;
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    Ok(())
}

/// Verifies indexed lookups and full integrity after an interrupted or failed REINDEX.
fn assert_reindex_fixture_intact(conn: &Arc<Connection>, rows: usize) -> anyhow::Result<()> {
    assert_eq!(
        query_rows(conn, "SELECT COUNT(*) FROM t")?,
        vec![rows.to_string()]
    );
    assert_eq!(
        query_rows(
            conn,
            "SELECT id, a FROM t INDEXED BY idx_a WHERE a >= '' ORDER BY a COLLATE NOCASE LIMIT 3"
        )?,
        vec![
            format!("{}|value0001", rows - 1),
            format!("{}|value0002", rows - 2),
            format!("{}|value0003", rows - 3),
        ]
    );
    assert_eq!(query_rows(conn, "PRAGMA integrity_check")?, vec!["ok"]);
    Ok(())
}

/// REINDEX clears and refills existing index roots, so every target form must
/// run under statement subtransactions when compiled in write mode.
#[turso_macros::test]
fn reindex_requires_stmt_journal_for_all_target_forms(
    tmp_db: crate::common::TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, b INT)")?;
    conn.execute("CREATE INDEX idx_a ON t(a)")?;
    conn.execute("CREATE INDEX idx_b ON t(b)")?;

    assert!(needs_stmt_journal(&conn, "REINDEX"));
    assert!(needs_stmt_journal(&conn, "REINDEX t"));
    assert!(needs_stmt_journal(&conn, "REINDEX idx_a"));
    assert!(needs_stmt_journal(&conn, "REINDEX nocase"));
    Ok(())
}

/// SQLite allows REINDEX under query_only when target resolution finds no
/// indexes to rebuild, because no write is attempted.
#[turso_macros::test]
fn reindex_query_only_allows_empty_work(tmp_db: crate::common::TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t_without_indexes(a)")?;

    conn.execute("PRAGMA query_only = 1")?;
    conn.execute("REINDEX")?;
    conn.execute("REINDEX t_without_indexes")?;
    conn.execute("PRAGMA query_only = 0")?;

    Ok(())
}

/// Resetting a REINDEX statement while writes are pending must roll back the
/// partially executed statement instead of leaving the target index empty.
#[test]
fn reindex_reset_during_pending_io_preserves_original_indexes() -> anyhow::Result<()> {
    let io = Arc::new(QueuedIo::new());
    let db = open_queued_db(io.clone(), "queued-reindex-reset.db")?;
    let conn = db.connect()?;
    let rows = 700;
    populate_reindex_fixture(&conn, rows)?;

    conn.execute("BEGIN")?;
    let mut stmt = conn.prepare("REINDEX idx_a")?;
    loop {
        match stmt.step()? {
            StepResult::Done => anyhow::bail!("REINDEX finished before yielding I/O"),
            StepResult::Row => continue,
            StepResult::Busy | StepResult::Interrupt => {
                anyhow::bail!("unexpected REINDEX step result before reset")
            }
            StepResult::IO => break,
        }
    }

    stmt.reset()?;
    assert_reindex_fixture_intact(&conn, rows)?;
    Ok(())
}

/// A write fault during the clear-plus-refill phase must preserve the original index contents.
#[test]
fn reindex_write_fault_rolls_back_without_emptying_index() -> anyhow::Result<()> {
    let io = Arc::new(QueuedIo::new());
    let db_path = "queued-reindex-fault.db";
    let wal_path = format!("{db_path}-wal");
    let db = open_queued_db(io.clone(), db_path)?;
    let conn = db.connect()?;
    let rows = 700;
    populate_reindex_fixture(&conn, rows)?;

    io.fault_after(wal_path, QueuedIoOpKind::Pwritev, 0);
    let reindex_result = conn.execute("REINDEX idx_a");
    assert!(
        reindex_result.is_err(),
        "REINDEX should fail under injected WAL write fault"
    );
    io.clear_fault();

    assert_reindex_fixture_intact(&conn, rows)?;
    Ok(())
}
