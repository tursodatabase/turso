use crate::common::{ExecRows, TempDatabase};
use std::sync::Arc;
use turso_core::{Database, SqliteDialect};

#[turso_macros::test]
fn test_mvcc_dropped_trigger_stays_dropped_after_restart_and_checkpoint(
    db: TempDatabase,
) -> anyhow::Result<()> {
    let path = db.path.clone();
    let io = db.io.clone();
    {
        let conn = db.connect_limbo();
        conn.pragma_update("journal_mode", "'mvcc'")?;
        conn.execute("CREATE TABLE t(x)")?;
        conn.execute("CREATE TRIGGER tr1 AFTER INSERT ON t BEGIN SELECT 1; END")?;
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        conn.execute("DROP TRIGGER tr1")?;
    }
    drop(db);

    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect))?;
    let conn = db.connect()?;
    conn.execute("CREATE TRIGGER tr2 AFTER INSERT ON t BEGIN SELECT 2; END")?;
    conn.execute("DROP TRIGGER tr2")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let rows: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE type='trigger'");
    assert_eq!(rows, Vec::<(String,)>::new());
    Ok(())
}
