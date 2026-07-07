use crate::queued_io::QueuedIo;
use std::sync::Arc;
use turso_core::vdbe::StepResult;
use turso_core::{Connection, Database, Result};

fn step_blocking(stmt: &mut turso_core::Statement) -> Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Yield => continue,
            other => return Ok(other),
        }
    }
}

fn exec_ints(conn: &Arc<Connection>, sql: &str) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare(sql)?;
    let mut out = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Yield => continue,
            StepResult::Row => {
                let row = stmt.row().unwrap();
                for value in row.get_values() {
                    if let turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) = value {
                        out.push(*i);
                    }
                }
            }
            StepResult::Done => break,
            r => panic!("unexpected step result: {r:?}"),
        }
    }
    Ok(out)
}

fn exec(conn: &Arc<Connection>, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    match step_blocking(&mut stmt)? {
        StepResult::Done | StepResult::Row => Ok(()),
        r => panic!("unexpected step result for `{sql}`: {r:?}"),
    }
}

fn try_exec_step(conn: &Arc<Connection>, sql: &str) -> Result<StepResult> {
    let mut stmt = conn.prepare(sql)?;
    step_blocking(&mut stmt)
}

const BATCH1: usize = 500;
const BATCH2: usize = 600;

struct Setup {
    _io: Arc<QueuedIo>,
    db: Arc<Database>,
    a: Arc<Connection>,
    b: Arc<Connection>,
    commit_stmt: turso_core::Statement,
    reader_stmt: Option<turso_core::Statement>,
    a_done: bool,
}

fn setup(park: usize) -> Setup {
    let io = Arc::new(QueuedIo::new());
    let db = Database::open_file(io.clone(), "publish-backfill-race.db").unwrap();
    let a = db.connect().unwrap();
    let b = db.connect().unwrap();

    exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH1 {
        exec(&a, &format!("INSERT INTO t VALUES ({i}, zeroblob(3000))")).unwrap();
    }
    exec(&a, "COMMIT").unwrap();

    let mut reader = b.prepare("SELECT id FROM t").unwrap();
    loop {
        match reader.step().unwrap() {
            StepResult::IO => reader._io().step().unwrap(),
            StepResult::Yield => continue,
            StepResult::Row => break,
            r => panic!("unexpected step result while parking reader: {r:?}"),
        }
    }

    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH2 {
        exec(
            &a,
            &format!("INSERT INTO t VALUES ({}, zeroblob(3000))", BATCH1 + i),
        )
        .unwrap();
    }

    let mut commit_stmt = a.prepare("COMMIT").unwrap();
    let mut pumps = 0;
    let mut a_done = false;
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 10_000_000, "commit statement seems stuck");
        match commit_stmt.step().unwrap() {
            StepResult::IO => {
                if pumps >= park {
                    break;
                }
                pumps += 1;
                commit_stmt._io().step().unwrap();
            }
            StepResult::Row => {}
            StepResult::Yield => {}
            StepResult::Done => {
                a_done = true;
                break;
            }
            r => panic!("unexpected step result: {r:?}"),
        }
    }

    Setup {
        _io: io,
        db,
        a,
        b,
        commit_stmt,
        reader_stmt: Some(reader),
        a_done,
    }
}

fn finish(stmt: &mut turso_core::Statement) {
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 10_000_000, "statement seems stuck");
        match stmt.step().unwrap() {
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Row => {}
            StepResult::Yield => {}
            StepResult::Done => break,
            r => panic!("unexpected step result: {r:?}"),
        }
    }
}

fn assert_integrity_ok(conn: &Arc<Connection>, context: &str) {
    let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
    let mut rows = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Yield => continue,
            StepResult::Row => {
                rows.push(stmt.row().unwrap().get::<String>(0).unwrap());
            }
            StepResult::Done => break,
            r => panic!("unexpected integrity_check step result: {r:?}"),
        }
    }
    assert_eq!(rows, vec!["ok"], "integrity_check failed {context}");
}

fn setup_hidden_root_reuse(park: usize) -> Setup {
    let io = Arc::new(QueuedIo::new());
    let db = Database::open_file(io.clone(), "publish-backfill-hidden-root-race.db").unwrap();
    let a = db.connect().unwrap();
    let b = db.connect().unwrap();

    exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    exec(&a, "CREATE TABLE hidden(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    exec(&a, "INSERT INTO hidden VALUES(1, zeroblob(3000))").unwrap();
    exec(&a, "CREATE TABLE small(id INTEGER PRIMARY KEY, k INTEGER)").unwrap();
    exec(&a, "INSERT INTO small VALUES(1, 1)").unwrap();
    let _ = exec_ints(&a, "PRAGMA wal_checkpoint").unwrap();

    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH1 {
        exec(&a, &format!("INSERT INTO t VALUES ({i}, zeroblob(3000))")).unwrap();
    }
    exec(&a, "COMMIT").unwrap();

    let mut reader = b.prepare("SELECT id FROM t").unwrap();
    loop {
        match reader.step().unwrap() {
            StepResult::IO => reader._io().step().unwrap(),
            StepResult::Yield => continue,
            StepResult::Row => break,
            r => panic!("unexpected step result while parking reader: {r:?}"),
        }
    }

    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH2 {
        exec(
            &a,
            &format!("INSERT INTO t VALUES ({}, zeroblob(3000))", BATCH1 + i),
        )
        .unwrap();
    }

    let mut commit_stmt = a.prepare("COMMIT").unwrap();
    let mut pumps = 0;
    let mut a_done = false;
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 10_000_000, "commit statement seems stuck");
        match commit_stmt.step().unwrap() {
            StepResult::IO => {
                if pumps >= park {
                    break;
                }
                pumps += 1;
                commit_stmt._io().step().unwrap();
            }
            StepResult::Row => {}
            StepResult::Yield => {}
            StepResult::Done => {
                a_done = true;
                break;
            }
            r => panic!("unexpected step result: {r:?}"),
        }
    }

    Setup {
        _io: io,
        db,
        a,
        b,
        commit_stmt,
        reader_stmt: Some(reader),
        a_done,
    }
}

fn total_commit_io_pumps_hidden_root_reuse() -> usize {
    let io = Arc::new(QueuedIo::new());
    let db = Database::open_file(io.clone(), "publish-backfill-hidden-root-race-dry.db").unwrap();
    let a = db.connect().unwrap();
    let b = db.connect().unwrap();

    exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    exec(&a, "CREATE TABLE hidden(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    exec(&a, "INSERT INTO hidden VALUES(1, zeroblob(3000))").unwrap();
    exec(&a, "CREATE TABLE small(id INTEGER PRIMARY KEY, k INTEGER)").unwrap();
    exec(&a, "INSERT INTO small VALUES(1, 1)").unwrap();
    let _ = exec_ints(&a, "PRAGMA wal_checkpoint").unwrap();

    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH1 {
        exec(&a, &format!("INSERT INTO t VALUES ({i}, zeroblob(3000))")).unwrap();
    }
    exec(&a, "COMMIT").unwrap();

    let mut reader = b.prepare("SELECT id FROM t").unwrap();
    loop {
        match reader.step().unwrap() {
            StepResult::IO => reader._io().step().unwrap(),
            StepResult::Yield => continue,
            StepResult::Row => break,
            r => panic!("unexpected: {r:?}"),
        }
    }

    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH2 {
        exec(
            &a,
            &format!("INSERT INTO t VALUES ({}, zeroblob(3000))", BATCH1 + i),
        )
        .unwrap();
    }

    let mut commit_stmt = a.prepare("COMMIT").unwrap();
    let mut pumps = 0usize;
    loop {
        match commit_stmt.step().unwrap() {
            StepResult::IO => {
                pumps += 1;
                commit_stmt._io().step().unwrap();
            }
            StepResult::Row | StepResult::Yield => {}
            StepResult::Done => break,
            r => panic!("unexpected: {r:?}"),
        }
    }
    pumps
}

#[test]
fn wal_stale_publish_backfill_hides_committed_rows() {
    let total_yields = {
        let io = Arc::new(QueuedIo::new());
        let db = Database::open_file(io.clone(), "publish-backfill-race-dry.db").unwrap();
        let a = db.connect().unwrap();
        let b = db.connect().unwrap();

        exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
        exec(&a, "BEGIN").unwrap();
        for i in 0..BATCH1 {
            exec(&a, &format!("INSERT INTO t VALUES ({i}, zeroblob(3000))")).unwrap();
        }
        exec(&a, "COMMIT").unwrap();

        let mut reader = b.prepare("SELECT id FROM t").unwrap();
        loop {
            match reader.step().unwrap() {
                StepResult::IO => reader._io().step().unwrap(),
                StepResult::Yield => continue,
                StepResult::Row => break,
                r => panic!("unexpected: {r:?}"),
            }
        }

        exec(&a, "BEGIN").unwrap();
        for i in 0..BATCH2 {
            exec(
                &a,
                &format!("INSERT INTO t VALUES ({}, zeroblob(3000))", BATCH1 + i),
            )
            .unwrap();
        }

        let mut commit_stmt = a.prepare("COMMIT").unwrap();
        let mut pumps = 0usize;
        loop {
            match commit_stmt.step().unwrap() {
                StepResult::IO => {
                    pumps += 1;
                    commit_stmt._io().step().unwrap();
                }
                StepResult::Row | StepResult::Yield => {}
                StepResult::Done => break,
                r => panic!("unexpected: {r:?}"),
            }
        }
        pumps
    };

    let from = total_yields.saturating_sub(8);
    for park in (from..total_yields).rev() {
        let mut s = setup(park);
        if s.a_done {
            continue;
        }

        drop(s.reader_stmt.take());

        let ck = exec_ints(&s.b, "PRAGMA wal_checkpoint").unwrap();
        if ck.first().copied() != Some(0) {
            finish(&mut s.commit_stmt);
            continue;
        }

        if matches!(
            try_exec_step(&s.b, "INSERT INTO t VALUES (1000001, zeroblob(3000))").unwrap(),
            StepResult::Busy
        ) {
            finish(&mut s.commit_stmt);
            exec(&s.b, "INSERT INTO t VALUES (1000001, zeroblob(3000))").unwrap();
            exec(&s.b, "INSERT INTO t VALUES (1000002, zeroblob(3000))").unwrap();
            exec(&s.b, "INSERT INTO t VALUES (1000003, zeroblob(3000))").unwrap();
            let got = exec_ints(&s.b, "SELECT id FROM t WHERE id >= 1000000 ORDER BY id").unwrap();
            assert_eq!(got, vec![1000001, 1000002, 1000003]);
            assert_integrity_ok(&s.b, "after guarded stale publish race");
            return;
        }

        exec(&s.b, "INSERT INTO t VALUES (1000002, zeroblob(3000))").unwrap();
        exec(&s.b, "INSERT INTO t VALUES (1000003, zeroblob(3000))").unwrap();

        finish(&mut s.commit_stmt);

        let got = exec_ints(&s.b, "SELECT id FROM t WHERE id >= 1000000 ORDER BY id").unwrap();
        if got == vec![1000001, 1000002, 1000003] {
            continue;
        }

        let dup = exec(&s.b, "INSERT INTO t VALUES (1000001, zeroblob(10))");
        let integrity_after_reopen = {
            let Setup {
                _io: io,
                db,
                a,
                b,
                commit_stmt,
                reader_stmt,
                ..
            } = s;
            drop(commit_stmt);
            drop(reader_stmt);
            drop(a);
            drop(b);
            drop(db);
            turso_core::clear_database_registry();

            let db2 = Database::open_file(io, "publish-backfill-race.db").unwrap();
            let c = db2.connect().unwrap();
            let mut stmt = c.prepare("PRAGMA integrity_check").unwrap();
            let mut msgs = Vec::new();
            loop {
                match stmt.step().unwrap() {
                    StepResult::IO => stmt._io().step().unwrap(),
                    StepResult::Yield => continue,
                    StepResult::Row => {
                        let row = stmt.row().unwrap();
                        msgs.push(format!("{:?}", row.get_values().collect::<Vec<_>>()));
                    }
                    StepResult::Done => break,
                    r => panic!("unexpected: {r:?}"),
                }
            }
            msgs
        };

        panic!(
            "WAL generation poisoned by stale publish_backfill (COMMIT parked after \
             {park}/{total_yields} IO pumps):\n\
             - rows committed by connection B vanished: got {got:?}, expected [1000001, 1000002, 1000003]\n\
             - re-INSERT of invisible committed PRIMARY KEY 1000001: {dup:?} (should be constraint error)\n\
             - integrity_check after reopen+recovery: {integrity_after_reopen:?}"
        );
    }
}

// This test drives the stale backfill-publish race into the same page-type
// assertion seen under stress.
//
// The important setup is:
// - `hidden` is a normal rowid table with root page H.
// - `small` is another table that can later get an index.
// - connection A writes enough large rows to trigger an auto-checkpoint during
//   COMMIT.
// - connection B keeps a read cursor open, so A's checkpoint can backfill only
//   part of the WAL.
//
// `total_commit_io_pumps_hidden_root_reuse()` first performs the same COMMIT
// without any interleaving and counts how many `StepResult::IO` completions it
// needs. The stale-publish window is near the end of that COMMIT, after WAL
// checkpoint finalization but before the pager publishes the synced backfill
// count. The loop below retries the scenario while parking A's COMMIT at each
// of the last few IO boundaries:
//
//   total_yields = number of IO completions needed by COMMIT
//   park = IO completion count where this test stops A's COMMIT
//
// Trying the last few park points keeps the test independent of small changes
// in the exact number of IO operations while still making the interleaving
// deterministic.
//
// On the buggy path:
// 1. A is parked before publishing its old partial backfill.
// 2. B finishes checkpointing, drops `hidden`, and creates `b_idx`, reusing
//    hidden's old root page H as an index root.
// 3. A resumes and publishes the old backfill count into the new WAL
//    generation.
// 4. A stale prepared schema statement, compiled while `hidden` still existed,
//    writes schema state that makes `hidden` visible again.
// 5. After reopen, sqlite_schema says `hidden` is a table rooted at H, but H now
//    contains an index page. Reading `hidden` uses a table cursor, reaches H,
//    and trips `matches!(self.page_type(), Ok(PageType::TableLeaf))`.
//
// With the fix, B's `DROP TABLE hidden` is Busy while A's checkpoint guard is
// still held. The WAL cannot restart under A's pending publish, so the page H
// table/index mismatch never forms.
#[test]
fn wal_stale_publish_backfill_can_point_table_at_restarted_index_root() {
    let total_yields = total_commit_io_pumps_hidden_root_reuse();

    let from = total_yields.saturating_sub(12);
    for park in (from..total_yields).rev() {
        let mut s = setup_hidden_root_reuse(park);
        if s.a_done {
            continue;
        }

        let schema_writer = s.db.connect().unwrap();
        let mut stale_schema_stmt = schema_writer
            .prepare("CREATE TABLE stale_keep(id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();

        drop(s.reader_stmt.take());

        let ck = exec_ints(&s.b, "PRAGMA wal_checkpoint").unwrap();
        if ck.first().copied() != Some(0) {
            finish(&mut s.commit_stmt);
            continue;
        }

        // Without the fix, this write restarts the WAL while connection A's
        // older partial checkpoint is parked before publishing nBackfill.
        if matches!(
            try_exec_step(&s.b, "DROP TABLE hidden").unwrap(),
            StepResult::Busy
        ) {
            finish(&mut s.commit_stmt);
            exec(&s.b, "DROP TABLE hidden").unwrap();
            exec(&s.b, "CREATE INDEX b_idx ON small(k)").unwrap();
            let _ = step_blocking(&mut stale_schema_stmt);
            assert_eq!(
                exec_ints(
                    &s.b,
                    "SELECT count(*) FROM sqlite_schema WHERE name = 'hidden'"
                )
                .unwrap(),
                vec![0]
            );
            assert_integrity_ok(&s.b, "after guarded table root reuse race");
            return;
        }
        exec(&s.b, "CREATE INDEX b_idx ON small(k)").unwrap();

        finish(&mut s.commit_stmt);

        // This prepared statement was compiled against the old schema where
        // `hidden` still exists. On the buggy path, it republishes that schema
        // after the restarted generation has reused hidden's root as b_idx.
        let _ = step_blocking(&mut stale_schema_stmt);

        let Setup {
            _io: io,
            db,
            a,
            b,
            commit_stmt,
            reader_stmt,
            ..
        } = s;
        drop(stale_schema_stmt);
        drop(schema_writer);
        drop(commit_stmt);
        drop(reader_stmt);
        drop(a);
        drop(b);
        drop(db);
        turso_core::clear_database_registry();

        let db2 = Database::open_file(io, "publish-backfill-hidden-root-race.db").unwrap();
        let c = db2.connect().unwrap();
        if exec_ints(
            &c,
            "SELECT count(*) FROM sqlite_schema WHERE name = 'hidden'",
        )
        .unwrap()
            != vec![1]
        {
            continue;
        }

        let rows = exec_ints(&c, "SELECT id FROM hidden WHERE id >= 0 ORDER BY id").unwrap();
        assert_eq!(rows, vec![1]);
        assert_integrity_ok(&c, "after stale schema points at recovered table root");
        return;
    }

    panic!("no parking point exercised stale schema/index root WAL race");
}
