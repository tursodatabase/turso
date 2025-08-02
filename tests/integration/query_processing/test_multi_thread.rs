use std::sync::{atomic::AtomicUsize, Arc};

use turso_core::StepResult;

use crate::common::{maybe_setup_tracing, TempDatabase};

#[test]
fn test_schema_reprepare() {
    let tmp_db = TempDatabase::new_empty(false);
    let conn1 = tmp_db.connect_limbo();
    conn1.execute("CREATE TABLE t(x, y, z)").unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1, 2, 3), (10, 20, 30)")
        .unwrap();
    let conn2 = tmp_db.connect_limbo();
    let mut stmt = conn2.prepare("SELECT y, z FROM t").unwrap();
    let mut stmt2 = conn2.prepare("SELECT x, z FROM t").unwrap();
    conn1.execute("ALTER TABLE t DROP COLUMN x").unwrap();
    assert_eq!(
        stmt2.step().unwrap_err().to_string(),
        "Parse error: no such column: x"
    );

    let mut rows = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Done => {
                break;
            }
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                rows.push((row.get::<i64>(0).unwrap(), row.get::<i64>(1).unwrap()));
            }
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            step => panic!("unexpected step result {step:?}"),
        }
    }
    let row = rows[0];
    assert_eq!(row, (2, 3));
    let row = rows[1];
    assert_eq!(row, (20, 30));
}

#[test]
#[ignore]
fn test_create_multiple_connections() -> anyhow::Result<()> {
    maybe_setup_tracing();
    let tries = 1;
    for _ in 0..tries {
        let tmp_db = Arc::new(TempDatabase::new_empty(false));
        {
            let conn = tmp_db.connect_limbo();
            conn.execute("CREATE TABLE t (x)").unwrap();
        }

        let mut threads = Vec::new();
        for i in 0..10 {
            let tmp_db_ = tmp_db.clone();
            threads.push(std::thread::spawn(move || {
                let conn = tmp_db_.connect_limbo();
                'outer: loop {
                    let mut stmt = conn
                        .prepare(format!("INSERT INTO t VALUES ({i})").as_str())
                        .unwrap();
                    tracing::info!("inserting row {}", i);
                    loop {
                        match stmt.step().unwrap() {
                            StepResult::Row => {
                                panic!("unexpected row result");
                            }
                            StepResult::IO => {
                                stmt.run_once().unwrap();
                            }
                            StepResult::Done => {
                                tracing::info!("inserted row {}", i);
                                break 'outer;
                            }
                            StepResult::Interrupt => {
                                panic!("unexpected step result");
                            }
                            StepResult::Busy => {
                                // repeat until we can insert it
                                tracing::info!("busy {}, repeating", i);
                                break;
                            }
                        }
                    }
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }

        let conn = tmp_db.connect_limbo();
        let mut stmt = conn.prepare("SELECT * FROM t").unwrap();
        let mut rows = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    rows.push(row.get::<i64>(0).unwrap());
                }
                StepResult::IO => {
                    stmt.run_once().unwrap();
                }
                StepResult::Done => {
                    break;
                }
                StepResult::Interrupt => {
                    panic!("unexpected step result");
                }
                StepResult::Busy => {
                    panic!("unexpected busy result on select");
                }
            }
        }
        rows.sort();
        assert_eq!(rows, (0..10).collect::<Vec<_>>());
    }
    Ok(())
}

#[test]
#[ignore]
fn test_reader_writer() -> anyhow::Result<()> {
    let tries = 10;
    for _ in 0..tries {
        let tmp_db = Arc::new(TempDatabase::new_empty(false));
        {
            let conn = tmp_db.connect_limbo();
            conn.execute("CREATE TABLE t (x)").unwrap();
        }

        let mut threads = Vec::new();
        let number_of_writers = 100;
        let current_written_rows = Arc::new(AtomicUsize::new(0));
        {
            let tmp_db = tmp_db.clone();
            let current_written_rows = current_written_rows.clone();
            threads.push(std::thread::spawn(move || {
                let conn = tmp_db.connect_limbo();
                for i in 0..number_of_writers {
                    conn.execute(format!("INSERT INTO t VALUES ({i})").as_str())
                        .unwrap();
                    current_written_rows.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }));
        }
        {
            let current_written_rows = current_written_rows.clone();
            threads.push(std::thread::spawn(move || {
                let conn = tmp_db.connect_limbo();
                loop {
                    let current_written_rows =
                        current_written_rows.load(std::sync::atomic::Ordering::Relaxed);
                    if current_written_rows == number_of_writers {
                        break;
                    }
                    let mut stmt = conn.prepare("SELECT * FROM t").unwrap();
                    let mut rows = Vec::new();
                    loop {
                        match stmt.step().unwrap() {
                            StepResult::Row => {
                                let row = stmt.row().unwrap();
                                let x = row.get::<i64>(0).unwrap();
                                rows.push(x);
                            }
                            StepResult::IO => {
                                stmt.run_once().unwrap();
                            }
                            StepResult::Done => {
                                rows.sort();
                                for i in 0..current_written_rows {
                                    let i = i as i64;
                                    assert!(
                                        rows.contains(&i),
                                        "row {i} not found in {rows:?}. current_written_rows: {current_written_rows}",
                                    );
                                }
                                break;
                            }
                            StepResult::Interrupt | StepResult::Busy => {
                                panic!("unexpected step result");
                            }
                        }
                    }
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }
    Ok(())
}
