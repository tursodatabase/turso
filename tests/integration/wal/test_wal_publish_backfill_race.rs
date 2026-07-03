use crate::queued_io::QueuedIo;
use std::sync::Arc;
use turso_core::vdbe::StepResult;
use turso_core::{Connection, Database, Result};

fn exec(conn: &Arc<Connection>, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    finish(&mut stmt)
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

fn finish(stmt: &mut turso_core::Statement) -> Result<()> {
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 1_000_000, "statement seems stuck");
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Row | StepResult::Yield => {}
            StepResult::Done => return Ok(()),
            r => panic!("unexpected step result: {r:?}"),
        }
    }
}

#[test]
fn wal_stale_publish_backfill_hides_committed_rows() {
    'sweep: for park in 0.. {
        let io = Arc::new(QueuedIo::new());
        let db = Database::open_file(io.clone(), "publish-backfill-race.db").unwrap();
        let a = db.connect().unwrap();
        let b = db.connect().unwrap();

        exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
        for i in 0..24 {
            exec(&a, &format!("INSERT INTO t VALUES ({i})")).unwrap();
        }

        let mut reader = b.prepare("SELECT id FROM t").unwrap();
        loop {
            match reader.step().unwrap() {
                StepResult::IO => reader._io().step().unwrap(),
                StepResult::Yield => continue,
                StepResult::Row => break,
                r => panic!("unexpected step result while parking reader: {r:?}"),
            }
        }

        for i in 24..28 {
            exec(&a, &format!("INSERT INTO t VALUES ({i})")).unwrap();
        }

        let mut ckpt = a.prepare("PRAGMA wal_checkpoint").unwrap();
        let mut pumps = 0;
        loop {
            match ckpt.step().unwrap() {
                StepResult::IO => {
                    if pumps >= park {
                        break;
                    }
                    pumps += 1;
                    ckpt._io().step().unwrap();
                }
                StepResult::Row | StepResult::Yield => {}
                StepResult::Done => return,
                r => panic!("unexpected step result: {r:?}"),
            }
        }

        drop(reader);
        let ck = exec_ints(&b, "PRAGMA wal_checkpoint").unwrap();
        if ck.first().copied() != Some(0) {
            finish(&mut ckpt).unwrap();
            continue 'sweep;
        }
        for id in [9001, 9002, 9003] {
            exec(&b, &format!("INSERT INTO t VALUES ({id})")).unwrap();
        }

        finish(&mut ckpt).unwrap();
        drop(ckpt);

        let got = exec_ints(&b, "SELECT id FROM t WHERE id >= 9000").unwrap();
        if got == [9001, 9002, 9003] {
            continue 'sweep;
        }

        let dup = exec(&b, "INSERT INTO t VALUES (9001)");
        drop(a);
        drop(b);
        drop(db);
        turso_core::clear_database_registry();
        let db = Database::open_file(io, "publish-backfill-race.db").unwrap();
        let c = db.connect().unwrap();
        let after_reopen = exec_ints(&c, "SELECT id FROM t WHERE id >= 9000").unwrap();

        panic!(
            "WAL generation poisoned by stale publish_backfill (checkpoint parked after \
             {park} IO pumps):\n\
             - rows committed by connection B vanished: got {got:?}, expected [9001, 9002, 9003]\n\
             - re-INSERT of invisible committed PRIMARY KEY 9001: {dup:?} (expected constraint error)\n\
             - after reopen + WAL recovery: {after_reopen:?} — committed rows 9002/9003 were \
             permanently destroyed by the stale-view write"
        );
    }
}
