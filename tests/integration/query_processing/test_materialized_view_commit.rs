use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};

/// A COMMIT whose commit-time materialized-view delta merge needs I/O must not
/// destroy the transaction it is committing.
///
/// `Program::commit_txn` runs `apply_view_deltas` before `commit_state` ever
/// leaves `Ready`, so an I/O yield there re-enters `op_transaction` with
/// `auto_commit` already flipped to true and no in-flight marker set. The
/// re-entry was then classified as a fresh COMMIT outside a transaction and
/// rejected, silently discarding the write. The merge only yields when the
/// view's persisted state is cold, so the view must be created by one database
/// handle and the transaction run through another.
#[turso_macros::test(views)]
fn test_commit_survives_view_delta_io_yield(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (1, 5), (2, 20)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::builder()
        .with_db_path(&path)
        .with_views(true)
        .build();
    let conn = reopened.connect_limbo();

    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (3, 7)")?;

    // The row is visible inside the transaction ...
    let in_txn: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(in_txn, vec![(4,)], "row must be visible inside the txn");

    // ... so COMMIT must not claim there is no transaction to commit.
    conn.execute("COMMIT")?;

    let after_commit: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(
        after_commit,
        vec![(4,)],
        "committed row must survive the commit"
    );
    drop(conn);
    drop(reopened);

    // ... and must still be on disk, with the view updated to match.
    let after = TempDatabase::builder()
        .with_db_path(&path)
        .with_views(true)
        .build();
    let conn = after.connect_limbo();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(4,)], "committed row must survive reopen");
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 15), (2, 20), (3, 7)]);

    Ok(())
}

/// The creating connection hits the same I/O yield once other writes have
/// pushed the view's state pages out of the page cache.
#[turso_macros::test(views)]
fn test_commit_survives_view_delta_io_yield_after_cache_eviction(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA cache_size = 200")?;
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("CREATE TABLE filler (x BLOB)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    conn.execute("INSERT INTO t VALUES (1, 1), (2, 1)")?;
    conn.execute("INSERT INTO filler SELECT zeroblob(3000) FROM generate_series(1, 1000)")?;

    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (1, 1)")?;
    conn.execute("COMMIT")?;

    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 2), (2, 1)]);

    Ok(())
}

/// The rollback of a failed statement also empties the page cache.
#[turso_macros::test(views)]
fn test_commit_survives_view_delta_io_yield_after_failed_statement(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT NOT NULL)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t")?;
    let err = conn.execute("INSERT INTO t VALUES (1, NULL)").unwrap_err();
    assert!(
        err.to_string().contains("NOT NULL constraint failed"),
        "{err}"
    );

    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (2, 'x')")?;
    conn.execute("COMMIT")?;

    let table: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM t ORDER BY a");
    let view: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM mv ORDER BY a");
    assert_eq!(table, vec![(2, "x".to_string())]);
    assert_eq!(view, table);

    Ok(())
}

/// Controls for `test_commit_survives_view_delta_io_yield`: the same write must
/// keep working on the paths whose commit-time merge never yields.
#[turso_macros::test(views)]
fn test_commit_with_view_controls(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("CREATE TABLE u (x INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (1, 5), (2, 20)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;

    // Matview created by this same handle: txn DML commits.
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (3, 7)")?;
    conn.execute("COMMIT")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(4,)]);

    // Autocommit DML on a matview'd table.
    conn.execute("INSERT INTO t VALUES (4, 1)")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(5,)]);

    // Txn DML on a table no matview depends on.
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO u VALUES (9)")?;
    conn.execute("COMMIT")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM u");
    assert_eq!(rows, vec![(1,)]);

    // A read-only txn still commits cleanly.
    conn.execute("BEGIN")?;
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    conn.execute("COMMIT")?;

    // ROLLBACK of txn DML on a matview'd table leaves both table and view intact.
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (5, 3)")?;
    conn.execute("ROLLBACK")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(5,)]);
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 15), (2, 20), (3, 7), (4, 1)]);

    Ok(())
}

/// The ordinary-VIEW counterpart of the reopen shape: no matview machinery is
/// involved, so this must pass regardless.
#[turso_macros::test(views)]
fn test_commit_after_reopen_with_plain_view(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1, 10)")?;
    conn.execute("CREATE VIEW ov AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::builder()
        .with_db_path(&path)
        .with_views(true)
        .build();
    let conn = reopened.connect_limbo();
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (3, 7)")?;
    conn.execute("COMMIT")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(2,)]);

    Ok(())
}

/// Regression test for #10056: `CREATE INDEX` on a materialized view's own
/// storage used to succeed, but nothing in the view's maintenance path keeps
/// that index up to date on later writes, so it silently went stale and
/// reopening the database failed with "sqlite_schema contains index for
/// missing table" (populate_indices ran before the view's backing table was
/// registered). CREATE INDEX on a materialized view must be rejected, the
/// same way it already is for a plain view, and the attempt must leave the
/// database in a state that still reopens cleanly.
#[turso_macros::test(views)]
fn create_index_on_materialized_view_is_rejected(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, type TEXT, amt INTEGER)")?;
    conn.execute(
        "CREATE MATERIALIZED VIEW tm AS SELECT type, sum(amt) AS totamt FROM t GROUP BY type",
    )?;
    conn.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)")?;

    let err = conn
        .execute("CREATE UNIQUE INDEX tm_type ON tm (type)")
        .unwrap_err();
    assert!(
        err.to_string().contains("views may not be indexed"),
        "{err}"
    );
    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::builder()
        .with_db_path(&path)
        .with_views(true)
        .build();
    let conn = reopened.connect_limbo();
    let view: Vec<(String, i64)> = conn.exec_rows("SELECT type, totamt FROM tm ORDER BY type");
    assert_eq!(view, vec![("a".to_string(), 10), ("b".to_string(), 20)]);

    Ok(())
}

/// Regression test for #9325: a cursor left open on a materialized view must
/// keep seeing every row even if a write on the same connection, through the
/// same transaction, causes a page split in the view's btree while the
/// cursor sits on an older page. The materialized-view cursor's inner btree
/// cursor was never registered with the pager's cursor registry (unlike an
/// ordinary table cursor opened via `Cursor::new_btree`/`new_btree_dyn`), so
/// it never took part in the save/restore dance that ordinary cursors get
/// when a peer write rebalances the tree, and could silently skip rows that
/// moved to a new page.
#[turso_macros::test(views)]
fn open_materialized_view_cursor_survives_page_split_from_same_connection_write(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    use turso_core::StepResult;

    fn collect_v(stmt: &mut turso_core::Statement) -> anyhow::Result<Vec<i64>> {
        let mut out = Vec::new();
        loop {
            match stmt.step()? {
                StepResult::Row => out.push(stmt.row().unwrap().get::<i64>(1)?),
                StepResult::Done => break,
                StepResult::IO => stmt.get_pager().io.step()?,
                other => panic!("unexpected step result: {other:?}"),
            }
        }
        Ok(out)
    }

    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM t")?;
    conn.execute("INSERT INTO t SELECT value * 2, value * 2 FROM generate_series(1, 400)")?;
    conn.execute("CREATE TABLE s(id INTEGER PRIMARY KEY, x INTEGER)")?;
    conn.execute("INSERT INTO s VALUES (1, 800), (2, 798), (3, 2)")?;

    let sql = "SELECT s.x, mv.v FROM s CROSS JOIN mv ON mv.rowid = s.x";
    let mut stmt = conn.prepare(sql)?;
    let mut open = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::Row => {
                open.push(stmt.row().unwrap().get::<i64>(1)?);
                break;
            }
            StepResult::IO => stmt.get_pager().io.step()?,
            other => panic!("unexpected step result: {other:?}"),
        }
    }

    // A write on the same connection, while `stmt`'s cursor over `mv` is
    // still open, must not cause it to miss rows that move pages.
    conn.execute("INSERT INTO t SELECT value * 2 - 1, value * 2 - 1 FROM generate_series(1, 400)")?;

    open.extend(collect_v(&mut stmt)?);
    drop(stmt);

    let mut fresh_stmt = conn.prepare(sql)?;
    let fresh = collect_v(&mut fresh_stmt)?;

    assert_eq!(
        open,
        vec![800, 798, 2],
        "open cursor must not miss rows across a same-connection page split"
    );
    assert_eq!(fresh, vec![800, 798, 2]);

    Ok(())
}

/// Regression test for #9326: an open cursor over a materialized view must
/// not keep returning a row after a DELETE on the same connection commits
/// and removes it. The cursor held a reference to the per-statement view
/// change-log state at open time; a later commit cleared the connection-wide
/// map but the open cursor kept its stale reference, and the subsequent seek
/// only matched rows by rowid, ignoring that the delta's weight had gone
/// negative (retracted).
#[turso_macros::test(views)]
fn open_materialized_view_cursor_does_not_return_row_deleted_after_open(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    use turso_core::StepResult;

    fn collect_pairs(stmt: &mut turso_core::Statement) -> anyhow::Result<Vec<(i64, i64)>> {
        let mut out = Vec::new();
        loop {
            match stmt.step()? {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    out.push((row.get::<i64>(0)?, row.get::<i64>(1)?));
                }
                StepResult::Done => break,
                StepResult::IO => stmt.get_pager().io.step()?,
                other => panic!("unexpected step result: {other:?}"),
            }
        }
        Ok(out)
    }

    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM t")?;
    conn.execute("CREATE TABLE s(id INTEGER PRIMARY KEY, x INTEGER)")?;
    conn.execute("INSERT INTO s VALUES (1, 1), (2, 3)")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")?;

    let sql = "SELECT s.x, mv.v FROM s CROSS JOIN mv ON mv.rowid = s.x";
    let mut stmt = conn.prepare(sql)?;
    let mut open = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                open.push((row.get::<i64>(0)?, row.get::<i64>(1)?));
                break;
            }
            StepResult::IO => stmt.get_pager().io.step()?,
            other => panic!("unexpected step result: {other:?}"),
        }
    }

    // This commits on the same connection while `stmt` is still open on `mv`.
    conn.execute("DELETE FROM t WHERE id = 3")?;

    open.extend(collect_pairs(&mut stmt)?);
    drop(stmt);

    let mut fresh_stmt = conn.prepare(sql)?;
    let fresh = collect_pairs(&mut fresh_stmt)?;

    assert_eq!(
        open,
        vec![(1, 10)],
        "open cursor must not return a row removed by a DELETE that committed after it opened"
    );
    assert_eq!(fresh, vec![(1, 10)]);

    Ok(())
}

/// Regression test for #6771: when an autocommit `INSERT ... SELECT` aborts
/// mid-execution on a constraint violation, the per-row deltas it already
/// wrote to the connection's view change-log buffer must be discarded along
/// with the base-table writes. `Program::abort` rolled back the base-table
/// statement savepoint but left the view buffer populated, so the next
/// successful commit folded the aborted statement's stale deltas into the
/// view.
#[turso_macros::test(views)]
fn aborted_insert_select_does_not_leak_view_deltas(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT v, COUNT(*) AS cnt FROM t GROUP BY v")?;
    conn.execute("INSERT INTO t VALUES (1, 10)")?;
    conn.execute("INSERT INTO t VALUES (2, 20)")?;
    // The first branch writes (3, 30) and its view delta; the second branch
    // hits the PK on id=1 and aborts the whole statement.
    let err = conn
        .execute("INSERT INTO t SELECT 3 AS id, 30 AS v UNION ALL SELECT 1, 99")
        .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed"),
        "{err}"
    );
    conn.execute("INSERT INTO t VALUES (4, 40)")?;

    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT v, cnt FROM mv ORDER BY v");
    assert_eq!(
        view,
        vec![(10, 1), (20, 1), (40, 1)],
        "the aborted statement's (30, 1) delta must not reach the view"
    );

    Ok(())
}

/// A statement prepared while the schema has no materialized views must still
/// maintain a view created afterward on the same connection, once it steps.
/// `op_insert`/`op_delete` skip change-log routing entirely for a program
/// built against a materialized-view-free schema (see
/// `PreparedProgram::has_materialized_views`); this only stays correct
/// because `CREATE MATERIALIZED VIEW` bumps the schema cookie, which forces
/// this already-prepared INSERT to reprepare (picking up the fresh flag)
/// before it runs.
#[turso_macros::test(views)]
fn prepared_insert_sees_view_created_after_prepare_same_connection(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;

    // Prepared while the schema has no materialized views at all.
    let mut stmt = conn.prepare("INSERT INTO t VALUES (1, 10)")?;

    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, b FROM t")?;

    // Must reprepare against the new schema before running, and route the
    // insert's change through to the view.
    stmt.run_ignore_rows()?;
    drop(stmt);

    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, b FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 10)]);
    Ok(())
}

/// Same as above, but the materialized view is created by a second
/// connection after the first connection already prepared its INSERT. The
/// first connection must notice the on-disk schema cookie change and
/// reprepare before stepping, the same as it would for any other concurrent
/// schema change.
#[turso_macros::test(views)]
fn prepared_insert_sees_view_created_after_prepare_other_connection(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn_a = tmp_db.connect_limbo();
    conn_a.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;

    // Prepared on connection A while the schema has no materialized views.
    let mut stmt = conn_a.prepare("INSERT INTO t VALUES (1, 10)")?;

    // Connection B creates the view and commits it to disk.
    let conn_b = tmp_db.connect_limbo();
    conn_b.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, b FROM t")?;
    drop(conn_b);

    stmt.run_ignore_rows()?;
    drop(stmt);

    let view: Vec<(i64, i64)> = conn_a.exec_rows("SELECT a, b FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 10)]);
    Ok(())
}

fn count_orders(conn: &std::sync::Arc<turso_core::Connection>) -> i64 {
    match limbo_exec_rows(conn, "SELECT COUNT(*) FROM orders")[0][0] {
        rusqlite::types::Value::Integer(n) => n,
        ref other => panic!("unexpected count value: {other:?}"),
    }
}

/// A COMMIT that fails partway through must leave no partial effects behind,
/// even when a materialized view is maintained on the table being written.
/// The repro drives enough INSERT OR REPLACE batches that some COMMIT in the
/// run hits an induced failure; once that happens, the row count on the same
/// connection and on a fresh connection must agree with the number of batches
/// that actually reported success.
#[turso_macros::test(views)]
fn failed_commit_with_materialized_view_leaves_no_rows_behind(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute(
        "CREATE TABLE orders (collection TEXT NOT NULL, key TEXT NOT NULL, \
         value TEXT NOT NULL, PRIMARY KEY (collection, key))",
    )
    .unwrap();
    conn.execute(
        "CREATE MATERIALIZED VIEW mv AS SELECT json_extract(value,'$.customer') AS grp, \
         SUM(json_extract(value,'$.amount')) AS agg FROM orders \
         WHERE collection='o' GROUP BY grp",
    )
    .unwrap();

    let mut committed = 0i64;
    let mut commit_error = None;
    for start in (0..60_000).step_by(1000) {
        conn.execute("BEGIN").unwrap();
        for i in start..start + 1000 {
            let key = format!("order-{i:05}");
            let value = format!(
                r#"{{"customer":"c{}","amount":{}}}"#,
                i % 316,
                (i % 97) as f64 + 0.5
            );
            let mut stmt = conn
                .prepare(
                    "INSERT OR REPLACE INTO orders (collection, key, value) VALUES ('o', ?, ?)",
                )
                .unwrap();
            stmt.bind_at(1.try_into().unwrap(), turso_core::Value::build_text(key))
                .unwrap();
            stmt.bind_at(2.try_into().unwrap(), turso_core::Value::build_text(value))
                .unwrap();
            stmt.run_ignore_rows().unwrap();
        }
        match conn.execute("COMMIT") {
            Ok(()) => committed += 1000,
            Err(e) => {
                commit_error = Some(e.to_string());
                break;
            }
        }
    }

    let Some(commit_error) = commit_error else {
        return Ok(());
    };

    let same_conn_rows = count_orders(&conn);
    let fresh = tmp_db.connect_limbo();
    let fresh_rows = count_orders(&fresh);
    assert_eq!(
        (same_conn_rows, fresh_rows),
        (committed, committed),
        "rows from the transaction whose COMMIT failed ({commit_error}) are still visible \
         (same connection, fresh connection)"
    );

    Ok(())
}

/// A failed statement inside an explicit transaction must undo its own writes
/// even when recording its row changes for a view spilled the change log to
/// disk. The 300 KB row pushes the log over its in-memory limit, so recording
/// the deletion starts a spill; the trigger then fails the DELETE. The
/// statement rollback used to fail on the unfinished spill and skip undoing
/// the DELETE, so COMMIT made the deletion permanent.
#[turso_macros::test(views)]
fn failed_statement_after_change_log_spill_is_rolled_back(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB)")?;
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT a, length(b) AS n FROM t")?;
    conn.execute("INSERT INTO t VALUES (1, zeroblob(300000))")?;
    conn.execute("CREATE TRIGGER tr AFTER DELETE ON t BEGIN SELECT RAISE(ABORT, 'nope'); END")?;

    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (2, x'00')")?;
    let err = conn
        .execute("DELETE FROM t WHERE a = 1")
        .expect_err("the trigger must fail the DELETE");
    assert!(err.to_string().contains("nope"), "unexpected error: {err}");
    conn.execute("COMMIT")?;

    let base: Vec<(i64, i64)> = conn.exec_rows("SELECT a, length(b) FROM t ORDER BY a");
    assert_eq!(base, vec![(1, 300000), (2, 1)]);
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, n FROM v ORDER BY a");
    assert_eq!(view, vec![(1, 300000), (2, 1)]);
    Ok(())
}

/// Assert that both the base table `t(id, u)` and its row-for-row
/// materialized view `v` hold exactly `expected`, and that the database is
/// structurally sound.
fn assert_table_and_view(conn: &std::sync::Arc<turso_core::Connection>, expected: &[(i64, i64)]) {
    let base: Vec<(i64, i64)> = conn.exec_rows("SELECT id, u FROM t ORDER BY id");
    assert_eq!(base, expected, "base table");
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT id, u FROM v ORDER BY id");
    assert_eq!(view, expected, "materialized view");
    assert_eq!(run_integrity_check(conn), "ok");
}

/// INSERT OR FAIL keeps the rows it wrote before the conflict. Those rows
/// must reach the view when the statement ends, like a successful statement.
/// The FAIL statement used to end without maintaining the view, so its
/// captured row stayed in the change log. The SAVEPOINT then recorded a
/// change-log position past it, the next INSERT maintained the view and
/// emptied the log, and ROLLBACK TO failed with "change-log mark 1 is ahead
/// of current length 0", which threw away the whole transaction.
#[turso_macros::test(views)]
fn insert_or_fail_rows_reach_view_before_savepoint_rollback(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE)")?;
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT id, u FROM t")?;
    conn.execute("INSERT INTO t VALUES (100, 100)")?;

    conn.execute("BEGIN")?;
    let err = conn
        .execute("INSERT OR FAIL INTO t VALUES (1, 1), (2, 100)")
        .expect_err("the second row conflicts on u");
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    conn.execute("SAVEPOINT a")?;
    conn.execute("INSERT INTO t VALUES (3, 3)")?;
    conn.execute("ROLLBACK TO a")?;
    conn.execute("COMMIT")?;

    assert_table_and_view(&conn, &[(1, 1), (100, 100)]);
    Ok(())
}

/// Same as above for UPDATE OR FAIL: the rows it updated before the conflict
/// are kept, and the view must show them after a later savepoint rollback.
#[turso_macros::test(views)]
fn update_or_fail_rows_reach_view_before_savepoint_rollback(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE)")?;
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT id, u FROM t")?;
    conn.execute("INSERT INTO t VALUES (1, 1), (2, 2), (100, 100)")?;

    conn.execute("BEGIN")?;
    // Rows are visited in rowid order: id=1 becomes 11, id=2 conflicts.
    let err = conn
        .execute("UPDATE OR FAIL t SET u = CASE id WHEN 1 THEN 11 ELSE 100 END WHERE id < 100")
        .expect_err("the second row conflicts on u");
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    conn.execute("SAVEPOINT a")?;
    conn.execute("INSERT INTO t VALUES (3, 3)")?;
    conn.execute("ROLLBACK TO a")?;
    conn.execute("COMMIT")?;

    assert_table_and_view(&conn, &[(1, 11), (2, 2), (100, 100)]);
    Ok(())
}

/// The kept INSERT OR FAIL row must reach the view even when the next
/// statement reads the view partway through (here from a trigger) and then
/// aborts. The mid-statement read maintained the view and emptied the change
/// log, including the FAIL statement's row, so rolling back the aborted
/// statement could not restore the log: the view kept the aborted
/// statement's rows and lost the FAIL row.
#[turso_macros::test(views)]
fn insert_or_fail_rows_survive_next_statement_abort_after_view_read(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE)")?;
    conn.execute("CREATE TABLE lg(x)")?;
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT id, u FROM t")?;
    conn.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t WHEN new.id = 3 \
         BEGIN INSERT INTO lg SELECT count(*) FROM v; END",
    )?;
    conn.execute("INSERT INTO t VALUES (100, 100)")?;

    conn.execute("BEGIN")?;
    let err = conn
        .execute("INSERT OR FAIL INTO t VALUES (1, 1), (2, 100)")
        .expect_err("the second row conflicts on u");
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    let err = conn
        .execute("INSERT INTO t VALUES (3, 3), (4, 4), (5, 100)")
        .expect_err("the third row conflicts on u");
    assert!(err.to_string().contains("UNIQUE"), "{err}");
    conn.execute("COMMIT")?;

    assert_table_and_view(&conn, &[(1, 1), (100, 100)]);
    let lg: Vec<(i64,)> = conn.exec_rows("SELECT x FROM lg");
    assert!(
        lg.is_empty(),
        "the aborted statement's trigger write: {lg:?}"
    );
    Ok(())
}

/// RAISE(FAIL) in a trigger keeps the rows written before it, like INSERT OR
/// FAIL. Those rows must reach the view when the statement ends, so a later
/// statement that reads the view and then aborts cannot lose them.
#[turso_macros::test(views)]
fn raise_fail_rows_survive_next_statement_abort_after_view_read(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u INT)")?;
    conn.execute("CREATE TABLE lg(x)")?;
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT id, u FROM t")?;
    conn.execute(
        "CREATE TRIGGER f BEFORE INSERT ON t WHEN new.u < 0 BEGIN \
         SELECT RAISE(FAIL, 'neg') WHERE new.u = -1; \
         SELECT RAISE(ABORT, 'abort') WHERE new.u = -5; END",
    )?;
    conn.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t WHEN new.id = 3 \
         BEGIN INSERT INTO lg SELECT count(*) FROM v; END",
    )?;

    conn.execute("BEGIN")?;
    let err = conn
        .execute("INSERT INTO t VALUES (1, 1), (2, -1)")
        .expect_err("RAISE(FAIL) on the second row");
    assert!(err.to_string().contains("neg"), "{err}");
    let err = conn
        .execute("INSERT INTO t VALUES (3, 3), (4, 4), (5, -5)")
        .expect_err("RAISE(ABORT) on the third row");
    assert!(err.to_string().contains("abort"), "{err}");
    conn.execute("COMMIT")?;

    assert_table_and_view(&conn, &[(1, 1)]);
    let lg: Vec<(i64,)> = conn.exec_rows("SELECT x FROM lg");
    assert!(
        lg.is_empty(),
        "the aborted statement's trigger write: {lg:?}"
    );
    Ok(())
}

/// A write statement that reads a pragma table-valued function still has
/// that function's helper statement open when it reaches Halt, so the
/// connection looked like it was running a nested statement and Halt skipped
/// view maintenance. The rows stayed in the change log, and a later ROLLBACK
/// TO failed with "change-log mark 2 is ahead of current length 0".
#[turso_macros::test(views)]
fn insert_from_pragma_function_reaches_view_before_savepoint_rollback(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u INT)")?;
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT id, u FROM t")?;

    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t SELECT cid + 10, cid FROM pragma_table_info('t')")?;
    conn.execute("SAVEPOINT a")?;
    conn.execute("INSERT INTO t VALUES (3, 3)")?;
    conn.execute("ROLLBACK TO a")?;
    conn.execute("COMMIT")?;

    assert_table_and_view(&conn, &[(10, 0), (11, 1)]);
    Ok(())
}

/// Run PRAGMA integrity_check and join every result row into one string
/// ("ok" on a healthy database, or the newline-joined list of problems).
fn run_integrity_check(conn: &std::sync::Arc<turso_core::Connection>) -> String {
    let rows = conn
        .pragma_query("integrity_check")
        .expect("integrity_check should succeed");
    rows.into_iter()
        .filter_map(|row| {
            row.into_iter().next().and_then(|v| match v {
                turso_core::Value::Text(text) => Some(text.as_str().to_string()),
                _ => None,
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A materialized view created by an older Turso build (DBSP storage
/// version 1, the pre-rewrite interpreter engine) owns a real view btree
/// plus hidden state-table btrees and their primary-key indexes. This build
/// refuses to read or write through that old storage (see IVM_COMPAT.md
/// "Views from older versions"), but it must still know every page those
/// objects own: `PRAGMA integrity_check` must not call them "never used",
/// and `DROP VIEW` must free all of them rather than leaking pages.
///
/// The fixture was generated by a pre-fix tursodb (DBSP_CIRCUIT_VERSION 1)
/// running:
///   CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER);
///   INSERT INTO t VALUES (1,10),(2,20);
///   CREATE MATERIALIZED VIEW v AS SELECT a, b*2 AS c FROM t WHERE b > 5;
///   CREATE MATERIALIZED VIEW g AS SELECT count(*) AS n, sum(b) AS s FROM t;
#[test]
fn test_incompatible_materialized_view_storage_is_fully_tracked() -> anyhow::Result<()> {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sqlite/conformance/database/testing_legacy_materialized_view_v1.db");
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("legacy_materialized_view.db");
    std::fs::copy(&fixture, &db_path)?;

    let db = TempDatabase::builder()
        .with_db_path(&db_path)
        .with_views(true)
        .build();
    let conn = db.connect_limbo();

    // The old-version views' own btrees and hidden state-table indexes are
    // real, used pages: integrity_check must report "ok", not "never used".
    assert_eq!(run_integrity_check(&conn), "ok");

    // Reads and writes through the old storage are still rejected.
    let err = conn.execute("SELECT * FROM v").unwrap_err();
    assert!(err.to_string().contains("incompatible version"), "{err}");
    let err = conn.execute("INSERT INTO t VALUES (3, 30)").unwrap_err();
    assert!(err.to_string().contains("incompatible"), "{err}");

    // DROP VIEW must free every btree the old view owned: its own btree,
    // its hidden state tables, and their indexes.
    conn.execute("DROP VIEW v")?;
    conn.execute("DROP VIEW g")?;
    assert_eq!(run_integrity_check(&conn), "ok");

    // The name is free again and maintenance works normally going forward.
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT a, b*2 AS c FROM t WHERE b > 5")?;
    conn.execute("CREATE MATERIALIZED VIEW g AS SELECT count(*) AS n, sum(b) AS s FROM t")?;
    conn.execute("INSERT INTO t VALUES (3, 30)")?;
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, c FROM v ORDER BY a");
    assert_eq!(view, vec![(1, 20), (2, 40), (3, 60)]);
    let agg: Vec<(i64, i64)> = conn.exec_rows("SELECT n, s FROM g");
    assert_eq!(agg, vec![(3, 60)]);
    assert_eq!(run_integrity_check(&conn), "ok");

    Ok(())
}

/// UPDATE always emits an explicit Delete before the Insert under MVCC, even
/// when the row's rowid does not change. The view must equal a fresh query
/// both before and after the update, including across a checkpoint and a
/// second update.
#[turso_macros::test(views, mvcc)]
fn mvcc_matview_update_without_rowid_change_matches_query(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g INTEGER, v INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT g, sum(v) AS s FROM t GROUP BY g")?;
    conn.execute("INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 5)")?;

    conn.execute("UPDATE t SET v = 100 WHERE id = 1")?;
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT g, s FROM mv ORDER BY g");
    let query: Vec<(i64, i64)> = conn.exec_rows("SELECT g, sum(v) FROM t GROUP BY g ORDER BY g");
    assert_eq!(view, query);
    assert_eq!(view, vec![(1, 120), (2, 5)]);

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    conn.execute("UPDATE t SET v = 7 WHERE id = 2")?;
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT g, s FROM mv ORDER BY g");
    let query: Vec<(i64, i64)> = conn.exec_rows("SELECT g, sum(v) FROM t GROUP BY g ORDER BY g");
    assert_eq!(view, query);
    assert_eq!(view, vec![(1, 107), (2, 5)]);
    assert_eq!(run_integrity_check(&conn), "ok");
    Ok(())
}

/// Same UPDATE-without-rowid-change hazard, but through a view that feeds
/// another materialized view: the upstream write must cascade correctly
/// through both maintenance programs.
#[turso_macros::test(views, mvcc)]
fn mvcc_matview_view_on_view_with_update(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g INTEGER, v INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW v1 AS SELECT g, sum(v) AS s FROM t GROUP BY g")?;
    conn.execute("CREATE MATERIALIZED VIEW v2 AS SELECT g, s * 2 AS doubled FROM v1")?;
    conn.execute("INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 5)")?;

    conn.execute("UPDATE t SET v = 100 WHERE id = 1")?;
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT g, doubled FROM v2 ORDER BY g");
    assert_eq!(view, vec![(1, 240), (2, 10)]);
    Ok(())
}

/// A materialized view maintained inside a plain (non-concurrent) MVCC
/// transaction must behave exactly like the WAL-mode case: writes inside the
/// transaction are visible to the view, SAVEPOINT rolls back only its own
/// writes, and ROLLBACK undoes the whole transaction's writes.
#[turso_macros::test(views, mvcc)]
fn mvcc_matview_write_rollback_and_savepoint(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 5)")?;

    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (1, 100)")?;
    conn.execute("SAVEPOINT sp")?;
    conn.execute("INSERT INTO t VALUES (3, 1)")?;
    conn.execute("ROLLBACK TO sp")?;
    conn.execute("COMMIT")?;

    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 130), (2, 5)]);

    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (4, 1000)")?;
    conn.execute("ROLLBACK")?;

    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 130), (2, 5)]);
    Ok(())
}

/// A materialized view built on top of another materialized view must stay
/// consistent as the base table changes, and DROP VIEW must refuse to drop a
/// view that another materialized view still depends on, then clean up every
/// hidden table once the dependent is gone.
#[turso_macros::test(views, mvcc)]
fn mvcc_matview_view_on_view_and_drop(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW v1 AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    conn.execute("CREATE MATERIALIZED VIEW v2 AS SELECT a, s * 2 AS doubled FROM v1")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")?;

    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, doubled FROM v2 ORDER BY a");
    assert_eq!(view, vec![(1, 20), (2, 40)]);

    let err = conn.execute("DROP VIEW v1").unwrap_err();
    assert!(
        err.to_string().contains("dependent materialized view"),
        "{err}"
    );

    conn.execute("DROP VIEW v2")?;
    conn.execute("DROP VIEW v1")?;

    let names: Vec<(String,)> = conn.exec_rows("SELECT name FROM sqlite_schema ORDER BY name");
    assert_eq!(
        names
            .into_iter()
            .filter(|(name,)| name != "__turso_internal_mvcc_meta")
            .collect::<Vec<_>>(),
        vec![("t".to_string(),)],
        "leftover schema rows"
    );
    assert_eq!(run_integrity_check(&conn), "ok");
    Ok(())
}

/// A materialized view must keep working across a close and reopen once its
/// writes have gone through `PRAGMA wal_checkpoint(TRUNCATE)`: the view's own
/// storage and its hidden state table must both load from the B-tree with
/// real root pages, and further writes must keep maintaining it correctly.
#[test]
fn mvcc_matview_reopen_after_checkpoint() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::builder()
        .with_views(true)
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::builder()
        .with_db_path(&path)
        .with_views(true)
        .with_mvcc(true)
        .build();
    let conn = reopened.connect_limbo();
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 10), (2, 20)]);

    conn.execute("INSERT INTO t VALUES (1, 5)")?;
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 15), (2, 20)]);
    assert_eq!(run_integrity_check(&conn), "ok");
    Ok(())
}

/// The same reopen must also work when the database closes with its writes
/// still only in the MVCC logical log (no checkpoint): reopening replays the
/// log, which must recognize the view's own schema row (type `view`, a real
/// root page) the same way it recognizes a table's.
#[test]
fn mvcc_matview_reopen_without_checkpoint() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::builder()
        .with_views(true)
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")?;
    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::builder()
        .with_db_path(&path)
        .with_views(true)
        .with_mvcc(true)
        .build();
    let conn = reopened.connect_limbo();
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 10), (2, 20)]);

    conn.execute("INSERT INTO t VALUES (3, 1)")?;
    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 10), (2, 20), (3, 1)]);
    assert_eq!(run_integrity_check(&conn), "ok");
    Ok(())
}

/// DROP VIEW on a materialized view that was already checkpointed destroys
/// its own btree plus its hidden state tables and their indexes directly
/// (translate_drop_view), not through op_drop_table, so under MVCC those
/// root pages must be tracked the same way op_drop_table tracks a dropped
/// table's roots: integrity_check must not report them as "never used"
/// freed pages, both right after the DROP and after a real reopen.
#[test]
fn mvcc_matview_drop_after_checkpoint_keeps_integrity_check_clean() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::builder()
        .with_views(true)
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute(
        "CREATE MATERIALIZED VIEW mv AS SELECT a, min(b) AS mn, count(DISTINCT b) AS cnt FROM t GROUP BY a",
    )?;
    conn.execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 5)")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    conn.execute("DROP VIEW mv")?;
    assert_eq!(run_integrity_check(&conn), "ok");

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::builder()
        .with_db_path(&path)
        .with_views(true)
        .with_mvcc(true)
        .build();
    let conn = reopened.connect_limbo();
    assert_eq!(run_integrity_check(&conn), "ok");
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(run_integrity_check(&conn), "ok");
    Ok(())
}

/// A trigger on a table with no materialized view can still write a table a
/// materialized view depends on. The write must be rejected the same way a
/// direct one would, and the statement that fired the trigger must leave no
/// row behind, even though its own target table has no view.
#[turso_macros::test(views)]
fn trigger_write_to_viewed_table_is_rejected_under_begin_concurrent(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'mvcc'")?;
    conn.execute("CREATE TABLE src (x INTEGER)")?;
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    conn.execute(
        "CREATE TRIGGER trg AFTER INSERT ON src BEGIN INSERT INTO t VALUES (new.x, new.x); END",
    )?;

    conn.execute("BEGIN CONCURRENT")?;
    let err = conn.execute("INSERT INTO src VALUES (1), (2)").unwrap_err();
    assert!(
        err.to_string()
            .contains("cannot modify table \"t\" in a BEGIN CONCURRENT transaction"),
        "unexpected error: {err}"
    );

    let src_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM src");
    assert!(
        src_rows.is_empty(),
        "the triggering row must not survive: {src_rows:?}"
    );
    Ok(())
}

/// A table a materialized view depends on may still be written inside BEGIN
/// CONCURRENT once CREATE MATERIALIZED VIEW (a DDL write) has already
/// upgraded the transaction to the exclusive writer slot: at that point no
/// other transaction can be concurrently maintaining the same view.
#[turso_macros::test(views)]
fn begin_concurrent_allows_write_after_create_materialized_view_upgrades_it(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'mvcc'")?;
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;

    conn.execute("BEGIN CONCURRENT")?;
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT a, sum(b) AS s FROM t GROUP BY a")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")?;
    conn.execute("COMMIT")?;

    let view: Vec<(i64, i64)> = conn.exec_rows("SELECT a, s FROM mv ORDER BY a");
    assert_eq!(view, vec![(1, 10), (2, 20)]);
    Ok(())
}
