use crate::common::{ExecRows, TempDatabase};

#[turso_macros::test]
fn test_delete_with_self_referencing_set_null_deletes_every_row(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA foreign_keys=ON")?;
    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES t(id) ON DELETE SET NULL)",
    )?;
    conn.execute("INSERT INTO t VALUES (1, NULL), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5)")?;
    conn.execute("DELETE FROM t WHERE id > 1")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t ORDER BY id");
    assert_eq!(rows, vec![(1,)]);
    Ok(())
}

#[turso_macros::test]
fn test_delete_with_fk_update_write_back_deletes_every_row(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA foreign_keys=ON")?;
    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, cref INTEGER REFERENCES c(x) ON UPDATE SET NULL)",
    )?;
    conn.execute(
        "CREATE TABLE c(id INTEGER PRIMARY KEY, x INTEGER UNIQUE REFERENCES t(id) ON DELETE SET NULL)",
    )?;
    conn.execute(
        "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL), (4, NULL), (5, NULL), (6, NULL)",
    )?;
    conn.execute("INSERT INTO c(x) VALUES (2), (3), (4), (5), (6)")?;
    conn.execute("INSERT INTO t VALUES (1002, 2), (1003, 3), (1004, 4), (1005, 5), (1006, 6)")?;
    conn.execute("DELETE FROM t WHERE id BETWEEN 2 AND 6")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![(1,), (1002,), (1003,), (1004,), (1005,), (1006,)]
    );
    Ok(())
}
