use turso_core::{LimboError, Result, StepResult, Value};

use crate::common::TempDatabase;

#[test]
fn test_txn_error_doesnt_rollback_txn() -> Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("create table t (x);", false);
    let conn = tmp_db.connect_limbo();

    conn.execute("begin")?;
    conn.execute("insert into t values (1)")?;
    // should fail
    assert!(conn
        .execute("begin")
        .inspect_err(|e| assert!(matches!(e, LimboError::TxError(_))))
        .is_err());
    conn.execute("insert into t values (1)")?;
    conn.execute("commit")?;
    let mut stmt = conn.query("select sum(x) from t")?.unwrap();
    if let StepResult::Row = stmt.step()? {
        let row = stmt.row().unwrap();
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(2));
    }

    Ok(())
}

// https://github.com/tursodatabase/turso/issues/2713
#[test]
fn test_constraint_error_doesnt_rollback_txn() -> Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("create table t (x integer primary key);", false);
    let conn = tmp_db.connect_limbo();
    conn.execute("insert into t values (1)")?;

    conn.execute("begin")?;
    conn.execute("create table t2(x integer primary key)")?;

    // should fail
    assert!(conn
        .execute("insert into t values (1)")
        .inspect_err(|e| assert!(matches!(e, LimboError::Constraint(_))))
        .is_err());

    conn.execute("commit")?;
    let mut stmt = conn.query("select count(*) from sqlite_schema")?.unwrap();
    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                // should be 2 tables => t, t1
                assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(2));
            }
            StepResult::Done => break,
            StepResult::IO => {
                stmt.run_once().unwrap();
            }
            step => panic!("unexpected step result {step:?}"),
        }
    }

    Ok(())
}
