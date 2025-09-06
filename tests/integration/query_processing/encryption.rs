use crate::common::{do_flush, TempDatabase};
use crate::query_processing::test_write_path::{run_query, run_query_on_row};
use rand::{rng, RngCore};
use std::panic;
use turso_core::Row;

#[test]
fn test_per_page_encryption() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_name = format!("test-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name, false);
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        run_query(
            &tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Hello, World!')",
        )?;
        let mut row_count = 0;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
            row_count += 1;
        })?;
        assert_eq!(row_count, 1);
        do_flush(&conn, &tmp_db)?;
    }

    {
        // this should panik because we should not be able to access the encrypted database
        // without the key
        let conn = tmp_db.connect_limbo();
        let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_: &Row| {}).unwrap();
        }));
        assert!(
            should_panic.is_err(),
            "should panic when accessing encrypted DB without key"
        );

        // it should also panic if we specify either only key or cipher
        let conn = tmp_db.connect_limbo();
        let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';").unwrap();
            run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_: &Row| {}).unwrap();
        }));
        assert!(
            should_panic.is_err(),
            "should panic when accessing encrypted DB without key"
        );

        let conn = tmp_db.connect_limbo();
        let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            run_query(
                &tmp_db,
                &conn,
                "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
            ).unwrap();
            run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_: &Row| {}).unwrap();
        }));
        assert!(
            should_panic.is_err(),
            "should panic when accessing encrypted DB without cipher name"
        );

        // it should panic if we specify wrong cipher or key
        let conn = tmp_db.connect_limbo();
        let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            run_query(
                &tmp_db,
                &conn,
                "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
            ).unwrap();
            run_query(&tmp_db, &conn, "PRAGMA cipher = 'aes256gcm';").unwrap();
            run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_: &Row| {}).unwrap();
        }));
        assert!(
            should_panic.is_err(),
            "should panic when accessing encrypted DB with incorrect cipher"
        );

        let conn = tmp_db.connect_limbo();
        let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';").unwrap();
            run_query(
                &tmp_db,
                &conn,
                "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76377';",
            ).unwrap();
            run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_: &Row| {}).unwrap();
        }));
        assert!(
            should_panic.is_err(),
            "should panic when accessing encrypted DB with incorrect key"
        );
    }

    {
        // let's test the existing db with the key
        let existing_db = TempDatabase::new_with_existent(&db_path, false);
        let conn = existing_db.connect_limbo();
        run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
        run_query(
            &existing_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query_on_row(&existing_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
        })?;
    }

    {
        // let's test connecting to the encrypted db using URI
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );
        let (_io, conn) = turso_core::Connection::from_uri(&uri, true, false, false, false)?;
        let mut row_count = 0;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
            row_count += 1;
        })?;
        assert_eq!(row_count, 1);
    }

    Ok(())
}

#[test]
fn test_non_4k_page_size_encryption() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_name = format!("test-8k-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name, false);
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
        // Set page size to 8k (8192 bytes) and test encryption. Default page size is 4k.
        run_query(&tmp_db, &conn, "PRAGMA page_size = 8192;")?;
        run_query(
            &tmp_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        run_query(
            &tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Hello, World!')",
        )?;
        let mut row_count = 0;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
            row_count += 1;
        })?;

        assert_eq!(row_count, 1);
        do_flush(&conn, &tmp_db)?;
    }

    {
        // Reopen the existing db with 8k page size and test encryption
        let existing_db = TempDatabase::new_with_existent(&db_path, false);
        let conn = existing_db.connect_limbo();
        run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
        run_query(
            &existing_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query_on_row(&existing_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
        })?;
    }

    Ok(())
}
