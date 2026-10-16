#![cfg(feature = "io_memory_yield")]

use std::sync::Arc;

use tempfile::tempdir;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, SqliteDialect, StepResult, Value,
    IO,
};

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut out = Vec::new();
    stmt.run_with_row_callback(|row| {
        out.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })
    .unwrap();
    out
}

fn ints(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    rows(conn, sql)
        .into_iter()
        .map(|r| match r[0] {
            Value::Numeric(turso_core::Numeric::Integer(i)) => i,
            ref other => panic!("not an integer: {other:?}"),
        })
        .collect()
}

fn make_io() -> Arc<dyn IO> {
    #[cfg(feature = "io_memory_yield")]
    {
        Arc::new(turso_core::MemoryYieldIO::new())
    }
    #[cfg(not(feature = "io_memory_yield"))]
    {
        Arc::new(turso_core::PlatformIO::new().unwrap())
    }
}

fn open_db(mvcc: bool) -> (tempfile::TempDir, Arc<Database>, Arc<dyn IO>) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("f2.db");
    let io = make_io();
    let opts = if mvcc {
        DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true)
    } else {
        DatabaseOpts::new()
    };
    let db = Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let setup = db.connect(ConnectOpts::new()).unwrap();
    if mvcc {
        rows(&setup, "PRAGMA journal_mode = mvcc");
        setup
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
    } else {
        rows(&setup, "PRAGMA journal_mode = wal");
    }
    setup
        .execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("CREATE TABLE child(pid INTEGER REFERENCES parent(id))")
        .unwrap();
    setup
        .execute("CREATE TABLE big(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("CREATE TABLE ctbl(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("INSERT INTO parent VALUES (1, 'a'), (100, 'z')")
        .unwrap();
    setup.execute("INSERT INTO big VALUES (1, 'b')").unwrap();
    setup.execute("INSERT INTO ctbl VALUES (1, 'c')").unwrap();
    rows(&setup, "PRAGMA wal_checkpoint(TRUNCATE)");
    setup.close().unwrap();
    (dir, db, io)
}

struct Outcome {
    yields: usize,
    probed: bool,
    visible: Vec<i64>,
    fk: turso_core::Result<()>,
    dup: turso_core::Result<()>,
}

const PARENT_PROBE: &str = "INSERT INTO parent VALUES (5000, 'p')";
const BIG_PROBE: &str = "INSERT INTO big VALUES (5000, 'p')";

fn run_scenario(probe_at_yield: Option<usize>, sibling_sql: &str) -> Outcome {
    let (_dir, db, io) = open_db(true);
    let victim = db.connect(ConnectOpts::new()).unwrap();
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let checkpointer = db.connect(ConnectOpts::new()).unwrap();
    for conn in [&victim, &writer, &checkpointer] {
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
    }
    victim.execute("PRAGMA foreign_keys = ON").unwrap();

    victim.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(ints(&victim, "SELECT id FROM parent WHERE id = 100"), [100]);

    writer.execute("DELETE FROM parent WHERE v = 'z'").unwrap();
    writer.execute("INSERT INTO big VALUES (500, 'x')").unwrap();

    checkpointer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    let mut s1 = checkpointer
        .prepare("INSERT INTO ctbl VALUES (2, 'c2')")
        .unwrap();
    let mut yields = 0usize;
    let mut probed = false;
    loop {
        match s1.step().unwrap() {
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield => {
                if Some(yields) == probe_at_yield && !probed {
                    match checkpointer.prepare(sibling_sql) {
                        Ok(mut s2) => match s2.run_ignore_rows() {
                            Ok(()) => probed = true,
                            Err(err) => println!("k={yields}: sibling run failed: {err}"),
                        },
                        Err(err) => println!("k={yields}: sibling prepare failed: {err}"),
                    }
                }
                yields += 1;
                io.step().unwrap();
            }
            StepResult::Row => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    drop(s1);
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();

    let visible = ints(&victim, "SELECT id FROM parent WHERE id = 100");
    let fk = victim.execute("INSERT INTO child(pid) VALUES (100)");
    let dup = victim.execute("INSERT INTO parent VALUES (100, 'dup')");
    Outcome {
        yields,
        probed,
        visible,
        fk,
        dup,
    }
}

#[test]
fn non_mvcc_baseline_fk_ok_and_dup_rejected() {
    let (_dir, db, _io) = open_db(false);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(ints(&conn, "SELECT id FROM parent WHERE id = 100"), [100]);
    let fk = conn.execute("INSERT INTO child(pid) VALUES (100)");
    let dup = conn.execute("INSERT INTO parent VALUES (100, 'dup')");
    println!("non-mvcc: fk={fk:?} dup={dup:?}");
    assert!(fk.is_ok(), "fk: {fk:?}");
    assert!(dup.is_err(), "dup: {dup:?}");
}

#[test]
fn mvcc_control_without_sibling_probe() {
    let out = run_scenario(None, PARENT_PROBE);
    println!(
        "control: yields={} probed={} visible={:?} fk={:?} dup={:?}",
        out.yields, out.probed, out.visible, out.fk, out.dup
    );
    assert_eq!(out.visible, [100]);
    assert!(out.fk.is_ok(), "fk: {:?}", out.fk);
    assert!(out.dup.is_err(), "dup: {:?}", out.dup);
}

fn sweep(sibling_sql: &str) -> (usize, Vec<(usize, Vec<i64>, String, String)>) {
    let total_yields = run_scenario(None, sibling_sql).yields;
    println!("total yields in control run: {total_yields}");
    let mut failures = Vec::new();
    let mut probed_count = 0usize;
    for k in 0..total_yields {
        let out = match std::panic::catch_unwind(|| run_scenario(Some(k), sibling_sql)) {
            Ok(out) => out,
            Err(_) => {
                println!("k={k}: engine panicked");
                continue;
            }
        };
        println!(
            "k={k}: yields={} probed={} visible={:?} fk_ok={} dup_err={}",
            out.yields,
            out.probed,
            out.visible,
            out.fk.is_ok(),
            out.dup.is_err()
        );
        if !out.probed {
            continue;
        }
        probed_count += 1;
        if out.visible != [100] || out.fk.is_err() || out.dup.is_ok() {
            failures.push((
                k,
                out.visible.clone(),
                format!("{:?}", out.fk),
                format!("{:?}", out.dup),
            ));
        }
    }
    (probed_count, failures)
}

#[test]
fn sibling_probe_during_parked_auto_checkpoint_poisons_pinned_reader() {
    let (probed_count, failures) = sweep(PARENT_PROBE);
    assert!(probed_count > 0, "the sibling probe never ran");
    assert!(
        failures.is_empty(),
        "victim pinned before the checkpointed delete got wrong NotExists answers: {failures:#?}"
    );
}

#[test]
fn sibling_probe_on_other_table_leaves_pinned_reader_correct() {
    let (probed_count, failures) = sweep(BIG_PROBE);
    assert!(probed_count > 0, "the sibling probe never ran");
    assert!(failures.is_empty(), "unexpected failures: {failures:#?}");
}
