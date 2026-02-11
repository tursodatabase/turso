use crate::common::{do_flush, TempDatabase};

#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE test_rowid (id INTEGER PRIMARY KEY, val TEXT);"
)]
fn test_last_insert_rowid_basic(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Simple insert
    conn.execute("INSERT INTO test_rowid (id, val) VALUES (NULL, 'test1')")?;

    // Check last_insert_rowid separately
    let mut select_query = conn.query("SELECT last_insert_rowid()")?;
    if let Some(ref mut rows) = select_query {
        rows.run_with_row_callback(|row| {
            if let turso_core::Value::Numeric(turso_core::Numeric::Integer(id)) = row.get_value(0) {
                assert_eq!(*id, 1, "First insert should have rowid 1");
            }
            Ok(())
        })?;
    }

    // Test explicit rowid
    conn.execute("INSERT INTO test_rowid (id, val) VALUES (5, 'test2')")?;

    // Check last_insert_rowid after explicit id
    let mut last_id = 0;
    match conn.query("SELECT last_insert_rowid()") {
        Ok(Some(ref mut rows)) => {
            rows.run_with_row_callback(|row| {
                if let turso_core::Value::Numeric(turso_core::Numeric::Integer(id)) =
                    row.get_value(0)
                {
                    last_id = *id;
                }
                Ok(())
            })?;
        }
        Ok(None) => {}
        Err(err) => eprintln!("{err}"),
    };
    assert_eq!(last_id, 5, "Explicit insert should have rowid 5");
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[turso_macros::test(mvcc, init_sql = "CREATE TABLE test_rowid (id INTEGER PRIMARY KEY);")]
fn test_integer_primary_key(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    for query in &[
        "INSERT INTO test_rowid VALUES (-1)",
        "INSERT INTO test_rowid VALUES (NULL)",
    ] {
        conn.execute(query)?;
    }
    let mut rowids = Vec::new();
    let mut select_query = conn.query("SELECT * FROM test_rowid")?.unwrap();
    select_query.run_with_row_callback(|row| {
        if let turso_core::Value::Numeric(turso_core::Numeric::Integer(id)) = row.get_value(0) {
            rowids.push(*id);
        }
        Ok(())
    })?;

    assert_eq!(rowids.len(), 2);
    assert!(rowids[0] == -1);
    assert!(rowids[1] == 0);
    Ok(())
}
