use crate::common::{do_flush, TempDatabase};
use crate::query_processing::test_write_path::{run_query, run_query_on_row};
use rand::{rng, RngCore};
use std::panic;
use turso_core::{Connection, Row};

#[test]
fn test_encryption_from_uri() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let db_name = format!("test-uri-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name, false);
    let db_path = tmp_db.path.clone();

    let key = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let cipher = "aegis256";
    let uri = format!(
        "file:{}?cipher={}&hexkey={}",
        db_path.to_str().unwrap(),
        cipher,
        key
    );

    {
        let (_io, conn) = Connection::from_uri(&uri, true, false, false)?;

        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        run_query(
            &tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Hello, from URI!')",
        )?;

        let mut row_count = 0;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, from URI!");
            row_count += 1;
        })?;
        assert_eq!(row_count, 1);

        do_flush(&conn, &tmp_db)?;
    }

    //Panic and fail
    {
        let conn_no_key = tmp_db.connect_limbo();

        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            run_query_on_row(&tmp_db, &conn_no_key, "SELECT * FROM test", |_: &Row| {}).unwrap();
        }));

        assert!(
            result.is_err(),
            "Expected panic when accessing encrypted DB without key"
        );
    }

    //reopen encrypted db with correct key and should not panic.
    {
        let (_io, conn) = Connection::from_uri(&uri, true, false, false)?;
        let mut row_count = 0;

        run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, from URI!");
            row_count += 1;
        })?;
        assert_eq!(row_count, 1);
    }

    Ok(())
}

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

    Ok(())
}
