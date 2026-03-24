use crate::common::{ExecRows, TempDatabase};
use std::io::Write;
use tempfile::TempDir;

#[turso_macros::test]
fn test_create_server_only(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE SERVER csv_files OPTIONS (driver 'csv')")?;
    Ok(())
}

#[turso_macros::test]
fn test_create_server_and_foreign_table(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // Create a CSV file
    let csv_path = tmp_db.path.parent().unwrap().join("test_employees.csv");
    let mut f = std::fs::File::create(&csv_path)?;
    writeln!(f, "name,age,city")?;
    writeln!(f, "alice,30,berlin")?;
    writeln!(f, "bob,25,munich")?;
    writeln!(f, "carol,35,hamburg")?;
    drop(f);

    // CREATE SERVER
    conn.execute("CREATE SERVER csv_files OPTIONS (driver 'csv')")?;

    // CREATE FOREIGN TABLE
    conn.execute(&format!(
        "CREATE FOREIGN TABLE employees (name TEXT, age TEXT, city TEXT) \
         SERVER csv_files OPTIONS (path '{}', skip_header 'true')",
        csv_path.display()
    ))?;

    // Query the foreign table
    let rows: Vec<(String, String, String)> =
        conn.exec_rows("SELECT name, age, city FROM employees");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        ("alice".to_string(), "30".to_string(), "berlin".to_string())
    );
    assert_eq!(
        rows[1],
        ("bob".to_string(), "25".to_string(), "munich".to_string())
    );
    assert_eq!(
        rows[2],
        ("carol".to_string(), "35".to_string(), "hamburg".to_string())
    );

    Ok(())
}

#[turso_macros::test]
fn test_foreign_table_error_no_server(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let result = conn
        .execute("CREATE FOREIGN TABLE t (a TEXT) SERVER nonexistent OPTIONS (path '/tmp/x.csv')");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("no such server"), "Got: {err}");

    Ok(())
}

#[turso_macros::test]
fn test_foreign_table_error_no_driver(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let result = conn.execute("CREATE SERVER s OPTIONS (driver 'nonexistent')");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("no such foreign driver"), "Got: {err}");

    Ok(())
}

#[turso_macros::test]
fn test_foreign_table_csv_missing_path(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE SERVER csv_files OPTIONS (driver 'csv')")?;
    let result = conn
        .execute("CREATE FOREIGN TABLE t (a TEXT) SERVER csv_files OPTIONS (skip_header 'true')");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("path"), "Got: {err}");

    Ok(())
}

#[turso_macros::test]
fn test_create_server_if_not_exists(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE SERVER csv_files OPTIONS (driver 'csv')")?;
    // Should not error
    conn.execute("CREATE SERVER IF NOT EXISTS csv_files OPTIONS (driver 'csv')")?;
    // Without IF NOT EXISTS should error
    let result = conn.execute("CREATE SERVER csv_files OPTIONS (driver 'csv')");
    assert!(result.is_err());

    Ok(())
}

#[turso_macros::test]
fn test_drop_server_if_exists(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // Should not error
    conn.execute("DROP SERVER IF EXISTS nonexistent")?;
    // Without IF EXISTS should error
    let result = conn.execute("DROP SERVER nonexistent");
    assert!(result.is_err());

    Ok(())
}

#[test]
fn test_foreign_table_persists_across_reopen() {
    let dir = TempDir::new().unwrap().keep();
    let db_path = dir.join("foreign_reopen.db");
    let csv_path = dir.join("reopen_data.csv");
    {
        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "x,y").unwrap();
        writeln!(f, "hello,world").unwrap();
        writeln!(f, "foo,bar").unwrap();
    }

    // Session 1: create server + foreign table, query, close
    {
        let db = TempDatabase::new_with_existent(&db_path);
        let conn = db.connect_limbo();
        conn.execute("CREATE SERVER csv_srv OPTIONS (driver 'csv')")
            .unwrap();
        conn.execute(&format!(
            "CREATE FOREIGN TABLE ft (x TEXT, y TEXT) \
             SERVER csv_srv OPTIONS (path '{}', skip_header 'true')",
            csv_path.display()
        ))
        .unwrap();
        let rows: Vec<(String, String)> = conn.exec_rows("SELECT x, y FROM ft");
        assert_eq!(rows.len(), 2);
        conn.close().unwrap();
    }

    // Session 2: reopen, query must still work
    {
        let db = TempDatabase::new_with_existent(&db_path);
        let conn = db.connect_limbo();
        let rows: Vec<(String, String)> = conn.exec_rows("SELECT x, y FROM ft");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], ("hello".to_string(), "world".to_string()));
        assert_eq!(rows[1], ("foo".to_string(), "bar".to_string()));
        conn.close().unwrap();
    }
}

// Note: DROP TABLE on foreign tables (virtual tables) requires VDestroy which
// calls conn.syms.write(). Connection::execute() holds conn.syms.read() for
// the duration, causing a deadlock. This is a pre-existing limitation of the
// execute() path (not specific to foreign tables). The CLI uses step-based
// execution which doesn't hold the read lock. DROP TABLE tests for foreign
// tables should be done via the CLI or sqltest runner.

#[turso_macros::test]
fn test_drop_server_with_dependent_table_errors(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let csv_path = tmp_db.path.parent().unwrap().join("dep_test.csv");
    {
        let mut f = std::fs::File::create(&csv_path)?;
        writeln!(f, "a")?;
        writeln!(f, "1")?;
    }

    conn.execute("CREATE SERVER s OPTIONS (driver 'csv')")?;
    conn.execute(&format!(
        "CREATE FOREIGN TABLE ft (a TEXT) SERVER s OPTIONS (path '{}', skip_header 'true')",
        csv_path.display()
    ))?;

    // DROP SERVER should fail — ft still depends on it
    let result = conn.execute("DROP SERVER s");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("foreign table") || err.contains("dependent"),
        "Expected dependency error, got: {err}"
    );

    Ok(())
}
