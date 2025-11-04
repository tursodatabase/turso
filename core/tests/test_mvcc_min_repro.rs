use std::sync::Arc;
use turso_core::{Database, MemoryIO, Result, StepResult};

// Minimal, fast reproducer for MVCC inconsistency under concurrent transactions.
// Expected: final value equals number of successful commits.
// Actual: panics inside MVCC rollback or value != committed count.
#[test]
fn test_concurrent_transactions_min_repro() -> Result<()> {
    const NUM_THREADS: usize = 8;
    const ITERS: usize = 30;

    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", true, false)?;

    {
        let conn = db.connect()?;
        conn.execute("CREATE TABLE ct (id INTEGER PRIMARY KEY, v INTEGER)")?;
        conn.execute("INSERT INTO ct (id, v) VALUES (0, 0)")?;
    }

    let mut handles: Vec<std::thread::JoinHandle<usize>> = vec![];
    for _t in 0..NUM_THREADS {
        let db = db.clone();
        let h = std::thread::spawn(move || {
            let conn = db.connect().expect("connect");
            let mut ok = 0usize;
            for _ in 0..ITERS {
                let _ = conn.execute("BEGIN CONCURRENT");
                let res = conn.execute("UPDATE ct SET v = v + 1 WHERE id = 0");
                if res.is_ok() {
                    if conn.execute("COMMIT").is_ok() {
                        ok += 1;
                    } else {
                        let _ = conn.execute("ROLLBACK");
                    }
                } else {
                    let _ = conn.execute("ROLLBACK");
                }
            }
            ok
        });
        handles.push(h);
    }

    let mut committed = 0usize;
    for h in handles {
        committed += h.join().map_err(|_| turso_core::LimboError::InternalError("panic".into()))?;
    }

    let conn = db.connect()?;
    let mut stmt = conn.prepare("SELECT v FROM ct WHERE id = 0")?;
    if let StepResult::Row = stmt.step()? {
        let v = stmt.row().unwrap().get::<i64>(0)? as usize;
        println!("min-repro committed={}, observed={}", committed, v);
        assert_eq!(v, committed, "MVCC inconsistency: value != committed updates");
    }

    Ok(())
}
