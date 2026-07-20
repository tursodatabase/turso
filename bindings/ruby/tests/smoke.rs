use turso_sdk_kit::rsapi::{TursoDatabase, TursoDatabaseConfig, TursoStatusCode};
use turso_sdk_kit::IoBackend;

#[test]
fn test_smoke_create_and_query() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        readonly: false,
        file_must_exist: false,
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());
    let conn = db.connect().unwrap();

    // Create table
    let mut stmt = conn
        .prepare_single("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    assert_eq!(stmt.execute(None).unwrap().status, TursoStatusCode::Done);
    stmt.finalize(None).unwrap();

    // Insert data
    let mut stmt = conn
        .prepare_single("INSERT INTO test (name) VALUES ('hello')")
        .unwrap();
    assert_eq!(stmt.execute(None).unwrap().status, TursoStatusCode::Done);
    assert_eq!(stmt.n_change(), 1);
    stmt.finalize(None).unwrap();

    // Query
    let mut stmt = conn
        .prepare_single("SELECT name FROM test WHERE id = 1")
        .unwrap();
    assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Row);
    let name = stmt.row_value(0).unwrap();
    assert_eq!(name.to_text(), Some("hello"));

    // No more rows
    assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Done);
    stmt.finalize(None).unwrap();
}

#[test]
fn test_smoke_bind_positional() {
    let db = TursoDatabase::new(TursoDatabaseConfig {
        path: ":memory:".to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        readonly: false,
        file_must_exist: false,
    });
    let result = db.open().unwrap();
    assert!(!result.is_io());
    let conn = db.connect().unwrap();

    let mut stmt = conn.prepare_single("SELECT ?1 + ?2").unwrap();
    stmt.bind_positional(1, turso_sdk_kit::rsapi::Value::from_i64(3))
        .unwrap();
    stmt.bind_positional(2, turso_sdk_kit::rsapi::Value::from_i64(4))
        .unwrap();
    assert_eq!(stmt.step(None).unwrap(), TursoStatusCode::Row);
    let val = stmt.row_value(0).unwrap();
    assert_eq!(val.as_int(), Some(7));
    stmt.finalize(None).unwrap();
}
