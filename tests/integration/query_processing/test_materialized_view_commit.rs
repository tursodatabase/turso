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
    drop(conn);

    let reopened = TempDatabase::builder()
        .with_db_path(&tmp_db.path)
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

    // ... and must still be on disk, with the view updated to match.
    let after = TempDatabase::builder()
        .with_db_path(&tmp_db.path)
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
    drop(conn);

    let reopened = TempDatabase::builder()
        .with_db_path(&tmp_db.path)
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
    drop(conn);

    let reopened = TempDatabase::builder()
        .with_db_path(&tmp_db.path)
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
