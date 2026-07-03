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
