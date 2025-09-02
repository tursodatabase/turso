use turso_core::{Result, StepResult, TursoError, Value};

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
        .inspect_err(|e| assert!(matches!(e, TursoError::TxError(_))))
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
