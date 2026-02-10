use crate::common::{ExecRows, TempDatabase};
use tempfile::TempDir;

/// Test that ATTACH works correctly with (MVCC) databases.
#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE source_data (id INTEGER PRIMARY KEY, name TEXT);"
)]
fn test_attach_database_read(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let source_conn = tmp_db.connect_limbo();

    source_conn.execute("INSERT INTO source_data VALUES (1, 'alice')")?;
    source_conn.execute("INSERT INTO source_data VALUES (2, 'bob')")?;
    source_conn.execute("INSERT INTO source_data VALUES (3, 'charlie')")?;
    let source_rows: Vec<(i64, String)> = source_conn.exec_rows("SELECT * FROM source_data");
    assert_eq!(
        source_rows,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "charlie".to_string())
        ],
        "Source database should have 3 rows"
    );

    // create a new database and attach the source
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("dest.db");
    let dest_db = TempDatabase::builder().with_db_path(&dest_path).build();
    let dest_conn = dest_db.connect_limbo();

    // attach the source MVCC database
    let source_path = tmp_db.path.to_str().unwrap();
    dest_conn.execute(&format!("ATTACH DATABASE '{}' AS source_db", source_path))?;

    // read from the attached MVCC database
    let attached_rows: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT * FROM source_db.source_data");
    assert_eq!(
        attached_rows,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "charlie".to_string())
        ],
        "Reading from attached MVCC database should return all rows"
    );

    dest_conn.execute("DETACH DATABASE source_db")?;

    Ok(())
}

/// Test MVCC source attached to MVCC destination.
/// Both databases use MVCC mode and should work correctly.
#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE source_data (id INTEGER PRIMARY KEY, name TEXT);"
)]
fn test_attach_mvcc_to_mvcc(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let source_conn = tmp_db.connect_limbo();

    // Insert data into source MVCC database
    source_conn.execute("INSERT INTO source_data VALUES (1, 'alice')")?;
    source_conn.execute("INSERT INTO source_data VALUES (2, 'bob')")?;

    // Verify source has data
    let source_rows: Vec<(i64, String)> = source_conn.exec_rows("SELECT * FROM source_data");
    assert_eq!(source_rows.len(), 2, "Source database should have 2 rows");

    // Create an MVCC destination database
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("dest.db");
    let dest_db = TempDatabase::builder()
        .with_db_path(&dest_path)
        .with_mvcc(true)
        .build();
    let dest_conn = dest_db.connect_limbo();

    // Attach the source MVCC database to the MVCC destination
    let source_path = tmp_db.path.to_str().unwrap();
    dest_conn.execute(&format!("ATTACH DATABASE '{}' AS source_db", source_path))?;

    // Read from the attached MVCC database
    let attached_rows: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT * FROM source_db.source_data");

    assert_eq!(
        attached_rows,
        vec![(1, "alice".to_string()), (2, "bob".to_string())],
        "Reading from attached MVCC database should return all rows"
    );

    dest_conn.execute("DETACH DATABASE source_db")?;

    Ok(())
}

/// Test that ATTACH works correctly with non-MVCC (WAL mode) databases.
/// This should work because WAL mode doesn't require mv_store.
#[turso_macros::test(init_sql = "CREATE TABLE source_data (id INTEGER PRIMARY KEY, name TEXT);")]
fn test_attach_wal_database_read(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let source_conn = tmp_db.connect_limbo();

    // Insert data into source WAL database
    source_conn.execute("INSERT INTO source_data VALUES (1, 'alice')")?;
    source_conn.execute("INSERT INTO source_data VALUES (2, 'bob')")?;
    source_conn.execute("INSERT INTO source_data VALUES (3, 'charlie')")?;

    // Verify source has data
    let source_rows: Vec<(i64, String)> = source_conn.exec_rows("SELECT * FROM source_data");
    assert_eq!(source_rows.len(), 3, "Source database should have 3 rows");

    // Create a new database and attach the source
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("dest.db");
    let dest_db = TempDatabase::builder().with_db_path(&dest_path).build();
    let dest_conn = dest_db.connect_limbo();

    // Attach the source WAL database
    let source_path = tmp_db.path.to_str().unwrap();
    dest_conn.execute(&format!("ATTACH DATABASE '{}' AS source_db", source_path))?;

    // Read from the attached WAL database - this should work
    let attached_rows: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT * FROM source_db.source_data");

    assert_eq!(
        attached_rows,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "charlie".to_string())
        ],
        "Reading from attached WAL database should return all rows"
    );

    dest_conn.execute("DETACH DATABASE source_db")?;

    Ok(())
}

/// Test INSERT INTO ... SELECT FROM attached WAL database works.
#[turso_macros::test(init_sql = "CREATE TABLE source_data (id INTEGER PRIMARY KEY, name TEXT);")]
fn test_attach_insert_select_wal(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let source_conn = tmp_db.connect_limbo();

    // Insert data into source
    source_conn.execute("INSERT INTO source_data VALUES (1, 'alice')")?;
    source_conn.execute("INSERT INTO source_data VALUES (2, 'bob')")?;

    // Create destination database with same schema
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("dest.db");
    let dest_db = TempDatabase::builder().with_db_path(&dest_path).build();
    let dest_conn = dest_db.connect_limbo();
    dest_conn.execute("CREATE TABLE source_data (id INTEGER PRIMARY KEY, name TEXT)")?;

    // Attach source and bulk copy
    let source_path = tmp_db.path.to_str().unwrap();
    dest_conn.execute(&format!("ATTACH DATABASE '{}' AS source_db", source_path))?;
    dest_conn.execute("INSERT INTO source_data SELECT * FROM source_db.source_data")?;
    dest_conn.execute("DETACH DATABASE source_db")?;

    // Verify data was copied
    let dest_rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT * FROM source_data");
    assert_eq!(
        dest_rows,
        vec![(1, "alice".to_string()), (2, "bob".to_string())],
        "Data should be copied to destination"
    );

    Ok(())
}
