use super::*;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_database_persistence() -> Result<()> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    // First, create the database, a table, and insert some data
    {
        let db = Builder::new_local(db_path).build().await?;
        let conn = db.connect()?;
        conn.execute(
            "CREATE TABLE test_persistence (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
            (),
        )
        .await?;
        conn.execute("INSERT INTO test_persistence (name) VALUES ('Alice');", ())
            .await?;
        conn.execute("INSERT INTO test_persistence (name) VALUES ('Bob');", ())
            .await?;
    } // db and conn are dropped here, simulating closing

    // Now, re-open the database and check if the data is still there
    let db = Builder::new_local(db_path).build().await?;
    let conn = db.connect()?;

    let mut rows = conn
        .query("SELECT name FROM test_persistence ORDER BY id;", ())
        .await?;

    let row1 = rows.next().await?.expect("Expected first row");
    assert_eq!(row1.get_value(0)?, Value::Text("Alice".to_string()));

    let row2 = rows.next().await?.expect("Expected second row");
    assert_eq!(row2.get_value(0)?, Value::Text("Bob".to_string()));

    assert!(rows.next().await?.is_none(), "Expected no more rows");

    Ok(())
}

#[tokio::test]
async fn test_database_persistence_many_frames() -> Result<()> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    const NUM_INSERTS: usize = 100;
    const TARGET_STRING_LEN: usize = 1024; // 1KB

    let mut original_data = Vec::with_capacity(NUM_INSERTS);
    for i in 0..NUM_INSERTS {
        let prefix = format!("test_string_{i:04}_");
        let padding_len = TARGET_STRING_LEN.saturating_sub(prefix.len());
        let padding: String = "A".repeat(padding_len);
        original_data.push(format!("{prefix}{padding}"));
    }

    // First, create the database, a table, and insert many large strings
    {
        let db = Builder::new_local(db_path).build().await?;
        let conn = db.connect()?;
        conn.execute(
            "CREATE TABLE test_large_persistence (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL);",
            (),
        )
        .await?;

        for data_val in &original_data {
            conn.execute(
                "INSERT INTO test_large_persistence (data) VALUES (?);",
                params::Params::Positional(vec![Value::Text(data_val.clone())]),
            )
            .await?;
        }
    } // db and conn are dropped here, simulating closing

    {
        // Now, re-open the database and check if the data is still there
        let db = Builder::new_local(db_path).build().await?;
        let conn = db.connect()?;

        let mut rows = conn
            .query("SELECT data FROM test_large_persistence ORDER BY id;", ())
            .await?;

        for (i, value) in original_data.iter().enumerate().take(NUM_INSERTS) {
            let row = rows
                .next()
                .await?
                .unwrap_or_else(|| panic!("Expected row {i} but found None"));
            assert_eq!(
                row.get_value(0)?,
                Value::Text(value.clone()),
                "Mismatch in retrieved data for row {i}"
            );
        }

        assert!(
            rows.next().await?.is_none(),
            "Expected no more rows after retrieving all inserted data"
        );

        // Delete the WAL file only and try to re-open and query
        let wal_path = format!("{db_path}-wal");
        std::fs::remove_file(&wal_path)
            .map_err(|e| eprintln!("Warning: Failed to delete WAL file for test: {e}"))
            .unwrap();
    }

    // Attempt to re-open the database after deleting WAL and assert that table is missing.
    let db_after_wal_delete = Builder::new_local(db_path).build().await?;
    let conn_after_wal_delete = db_after_wal_delete.connect()?;

    let query_result_after_wal_delete = conn_after_wal_delete
        .query("SELECT data FROM test_large_persistence ORDER BY id;", ())
        .await;

    match query_result_after_wal_delete {
        Ok(_) => panic!(
            "Query succeeded after WAL deletion and DB reopen, but was expected to fail because the table definition should have been in the WAL."
        ),
        Err(Error::Error(msg)) => {
            assert!(
                msg.contains("no such table: test_large_persistence"),
                "Expected 'test_large_persistence not found' error, but got: {msg}"
            );
        }
        Err(e) => panic!(
            "Expected SqlExecutionFailure for 'no such table', but got a different error: {e:?}"
        ),
    }

    Ok(())
}

#[tokio::test]
async fn test_rows_column_names() -> Result<()> {
    let db = Builder::new_local(":memory:").build().await?;
    let conn = db.connect()?;
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
        (),
    )
    .await?;
    conn.execute(
        "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.org');",
        (),
    )
    .await?;

    let rows = conn.query("SELECT id, name, email FROM users;", ()).await?;

    // columns()
    let columns = rows.columns();
    let names: Vec<&str> = columns.iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["id", "name", "email"]);

    // column_count()
    assert_eq!(rows.column_count(), 3);

    // column_name()
    assert_eq!(rows.column_name(0)?, "id");
    assert_eq!(rows.column_name(1)?, "name");
    assert_eq!(rows.column_name(2)?, "email");
    assert!(rows.column_name(3).is_err());

    // column_names()
    assert_eq!(rows.column_names(), vec!["id", "name", "email"]);

    // column_index()
    assert_eq!(rows.column_index("id")?, 0);
    assert_eq!(rows.column_index("name")?, 1);
    assert_eq!(rows.column_index("email")?, 2);
    assert_eq!(rows.column_index("EMAIL")?, 2); // case-insensitive
    assert!(rows.column_index("nonexistent").is_err());

    Ok(())
}

#[tokio::test]
async fn test_database_persistence_write_one_frame_many_times() -> Result<()> {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();

    for i in 0..100 {
        {
            let db = Builder::new_local(db_path).build().await?;
            let conn = db.connect()?;

            conn.execute("CREATE TABLE IF NOT EXISTS test_persistence (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", ()).await?;
            conn.execute("INSERT INTO test_persistence (name) VALUES ('Alice');", ())
                .await?;
        }
        {
            let db = Builder::new_local(db_path).build().await?;
            let conn = db.connect()?;

            let mut rows_iter = conn
                .query("SELECT count(*) FROM test_persistence;", ())
                .await?;
            let rows = rows_iter.next().await?.unwrap();
            assert_eq!(rows.get_value(0)?, Value::Integer(i as i64 + 1));
            assert!(rows_iter.next().await?.is_none());
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_parallel_writes_and_wal_size() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    let db = Builder::new_local(db_path_str).build().await?;
    let conn = db.connect()?;
    conn.execute(
        "CREATE TABLE test_data (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);",
        (),
    )
    .await?;

    // Generate a ~200KB payload
    let payload = "X".repeat(200 * 1024);

    // Parallel writes: spawn 8 connections, each inserting 5 rows
    let mut handles = Vec::new();
    for conn_id in 0..8u32 {
        let db = db.clone();
        let payload = payload.clone();
        handles.push(tokio::spawn(async move {
            let conn = db.connect().unwrap();
            for row_id in 0..5u32 {
                let tag = format!("conn{conn_id}_row{row_id}");
                let data = format!("{tag}_{payload}");
                loop {
                    match conn
                        .execute(
                            "INSERT INTO test_data (payload) VALUES (?);",
                            params::Params::Positional(vec![Value::Text(data.clone())]),
                        )
                        .await
                    {
                        Ok(_) => break,
                        Err(Error::Busy(_)) => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                        Err(e) => panic!("Insert failed: {e:?}"),
                    }
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Sequential writes: 3 more large inserts
    for i in 0..3 {
        let data = format!("sequential_{i}_{payload}");
        conn.execute(
            "INSERT INTO test_data (payload) VALUES (?);",
            params::Params::Positional(vec![Value::Text(data)]),
        )
        .await?;
    }

    // Verify row count: 8*5 + 3 = 43
    let mut rows = conn.query("SELECT count(*) FROM test_data;", ()).await?;
    let row = rows.next().await?.unwrap();
    assert_eq!(row.get_value(0)?, Value::Integer(43));

    // Report WAL size
    let wal_path = format!("{db_path_str}-wal");
    let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    eprintln!(
        "WAL size after all writes: {} bytes ({:.2} KB)",
        wal_size,
        wal_size as f64 / 1024.0
    );
    assert!(wal_size > 0, "WAL file should exist and be non-empty");

    Ok(())
}
