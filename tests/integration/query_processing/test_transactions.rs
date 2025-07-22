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
