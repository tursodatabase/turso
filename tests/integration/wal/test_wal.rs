use crate::common::{do_flush, maybe_setup_tracing, TempDatabase};
use limbo_core::{Connection, LimboError, Result, StepResult};
use std::cell::RefCell;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[allow(clippy::arc_with_non_send_sync)]
#[test]
fn test_wal_checkpoint_result() -> Result<()> {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal.db");
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t1 (id text);")?;

    let res = execute_and_get_strings(&tmp_db, &conn, "pragma journal_mode;")?;
    assert_eq!(res, vec!["wal"]);

    conn.execute("insert into t1(id) values (1), (2);")?;
    do_flush(&conn, &tmp_db).unwrap();
    conn.execute("select * from t1;")?;
    do_flush(&conn, &tmp_db).unwrap();

    // checkpoint result should return > 0 num pages now as database has data
    let res = execute_and_get_ints(&tmp_db, &conn, "pragma wal_checkpoint;")?;
    println!("'pragma wal_checkpoint;' returns: {res:?}");
    assert_eq!(res.len(), 3);
    assert_eq!(res[0], 0); // checkpoint successfully
    assert!(res[1] > 0); // num pages in wal
    assert!(res[2] > 0); // num pages checkpointed successfully

    Ok(())
}

#[test]
#[ignore = "ignored for now because it's flaky"]
fn test_wal_1_writer_1_reader() -> Result<()> {
    maybe_setup_tracing();
    let tmp_db = Arc::new(Mutex::new(TempDatabase::new("test_wal.db")));
    let db = tmp_db.lock().unwrap().limbo_database();

    {
        let conn = db.connect().unwrap();
        match conn.query("CREATE TABLE t (id)")? {
            Some(ref mut rows) => loop {
                match rows.step().unwrap() {
                    StepResult::Row => {}
                    StepResult::IO => {
                        tmp_db.lock().unwrap().io.run_once().unwrap();
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => unreachable!(),
                }
            },
            None => todo!(),
        }
        do_flush(&conn, tmp_db.lock().unwrap().deref()).unwrap();
    }
    let rows = Arc::new(std::sync::Mutex::new(0));
    let rows_ = rows.clone();
    const ROWS_WRITE: usize = 1000;
    let tmp_db_w = db.clone();
    let writer_thread = std::thread::spawn(move || {
        let conn = tmp_db_w.connect().unwrap();
        for i in 0..ROWS_WRITE {
            conn.execute(format!("INSERT INTO t values({})", i).as_str())
                .unwrap();
            let mut rows = rows_.lock().unwrap();
            *rows += 1;
        }
    });
    let rows_ = rows.clone();
    let reader_thread = std::thread::spawn(move || {
        let conn = db.connect().unwrap();
        loop {
            let rows = *rows_.lock().unwrap();
            let mut i = 0;
            match conn.query("SELECT * FROM t") {
                Ok(Some(ref mut rows)) => loop {
                    match rows.step().unwrap() {
                        StepResult::Row => {
                            let row = rows.row().unwrap();
                            let id = row.get::<i64>(0).unwrap();
                            assert_eq!(id, i);
                            i += 1;
                        }
                        StepResult::IO => {
                            tmp_db.lock().unwrap().io.run_once().unwrap();
                        }
                        StepResult::Interrupt => break,
                        StepResult::Done => break,
                        StepResult::Busy => unreachable!(),
                    }
                },
                Ok(None) => {}
                Err(err) => {
                    eprintln!("{}", err);
                }
            }
            if rows == ROWS_WRITE {
                break;
            }
        }
    });

    writer_thread.join().unwrap();
    reader_thread.join().unwrap();
    Ok(())
}

/// Execute a statement and get strings result
pub(crate) fn execute_and_get_strings(
    tmp_db: &TempDatabase,
    conn: &Rc<Connection>,
    sql: &str,
) -> Result<Vec<String>> {
    let statement = conn.prepare(sql)?;
    let stmt = Rc::new(RefCell::new(statement));
    let mut result = Vec::new();

    let mut stmt = stmt.borrow_mut();
    while let Ok(step_result) = stmt.step() {
        match step_result {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                for el in row.get_values() {
                    result.push(format!("{el}"));
                }
            }
            StepResult::Done => break,
            StepResult::Interrupt => break,
            StepResult::IO => tmp_db.io.run_once()?,
            StepResult::Busy => tmp_db.io.run_once()?,
        }
    }
    Ok(result)
}

/// Execute a statement and get integers
pub(crate) fn execute_and_get_ints(
    tmp_db: &TempDatabase,
    conn: &Rc<Connection>,
    sql: &str,
) -> Result<Vec<i64>> {
    let statement = conn.prepare(sql)?;
    let stmt = Rc::new(RefCell::new(statement));
    let mut result = Vec::new();

    let mut stmt = stmt.borrow_mut();
    while let Ok(step_result) = stmt.step() {
        match step_result {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                for value in row.get_values() {
                    let out = match value {
                        limbo_core::Value::Integer(i) => i,
                        _ => {
                            return Err(LimboError::ConversionError(format!(
                                "cannot convert {value} to int"
                            )))
                        }
                    };
                    result.push(*out);
                }
            }
            StepResult::Done => break,
            StepResult::Interrupt => break,
            StepResult::IO => tmp_db.io.run_once()?,
            StepResult::Busy => tmp_db.io.run_once()?,
        }
    }
    Ok(result)
}
