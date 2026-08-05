// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Session isolation over the wire protocol: every client connection must
//! get its own session, so concurrent transactions and session state do
//! not interleave. PostgreSQL gives every connection its own backend;
//! sharing one session between clients silently merges their transactions.

use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_pg_client::{error_message, BackendEvent, ConnParams, PgConn};
use turso_pg_server::TursoPgServer;

/// Starts a tursopg server on a free port over a fresh temp database and
/// returns connection parameters for it. The server thread runs until the
/// test process exits.
fn start_server() -> (ConnParams, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_file = dir.path().join("sessions.db").to_string_lossy().to_string();

    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let address = format!("127.0.0.1:{port}");

    let (_io, db) = turso_pg::open_database(
        &db_file,
        None,
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new()
            .with_views(true)
            .with_custom_types(true)
            .with_attach(true),
    )
    .unwrap();

    let server = TursoPgServer::new(address.clone(), db_file, db, Arc::new(AtomicUsize::new(0)));
    std::thread::spawn(move || {
        let _ = server.run();
    });

    // Wait for the listener to come up.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect(&address) {
            Ok(_) => break,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("server did not come up on {address}: {e}"),
        }
    }

    let params = ConnParams {
        host: "127.0.0.1".to_string(),
        port,
        user: "turso".to_string(),
        password: None,
        database: "regression".to_string(),
    };
    (params, dir)
}

/// Runs a statement and panics on any error response.
fn exec(conn: &mut PgConn, sql: &str) {
    for event in conn.simple_query(sql).unwrap() {
        if let BackendEvent::ErrorResponse(fields) = event {
            panic!("{sql} failed: {}", error_message(&fields));
        }
    }
}

/// Runs a query returning a single integer.
fn query_int(conn: &mut PgConn, sql: &str) -> i64 {
    let mut result = None;
    for event in conn.simple_query(sql).unwrap() {
        match event {
            BackendEvent::ErrorResponse(fields) => {
                panic!("{sql} failed: {}", error_message(&fields))
            }
            BackendEvent::DataRow(row) => result = Some(row[0].clone().unwrap().parse().unwrap()),
            _ => {}
        }
    }
    result.unwrap_or_else(|| panic!("no row returned for: {sql}"))
}

#[test]
fn concurrent_sessions_have_independent_transactions() {
    let (params, _dir) = start_server();
    let mut alice = PgConn::connect(&params, &[]).unwrap();
    let mut bob = PgConn::connect(&params, &[]).unwrap();

    exec(&mut alice, "CREATE TABLE session_test (x int)");

    // Both sessions must be able to open their own transaction. With one
    // shared session the second BEGIN lands inside the first transaction
    // and errors (or silently merges the two).
    exec(&mut alice, "BEGIN");
    exec(&mut bob, "BEGIN");

    // Alice's uncommitted insert must not be visible to Bob.
    exec(&mut alice, "INSERT INTO session_test VALUES (1)");
    assert_eq!(
        query_int(&mut bob, "SELECT count(*) FROM session_test"),
        0,
        "uncommitted row from another session must not be visible"
    );

    exec(&mut bob, "COMMIT");
    exec(&mut alice, "COMMIT");
    assert_eq!(query_int(&mut bob, "SELECT count(*) FROM session_test"), 1);
}

#[test]
fn rollback_in_one_session_does_not_touch_another() {
    let (params, _dir) = start_server();
    let mut alice = PgConn::connect(&params, &[]).unwrap();
    let mut bob = PgConn::connect(&params, &[]).unwrap();

    exec(&mut alice, "CREATE TABLE rollback_test (x int)");
    exec(&mut alice, "INSERT INTO rollback_test VALUES (1)");

    exec(&mut bob, "BEGIN");
    exec(&mut bob, "INSERT INTO rollback_test VALUES (2)");
    exec(&mut bob, "ROLLBACK");

    // Bob's rollback must only undo Bob's insert.
    assert_eq!(
        query_int(&mut alice, "SELECT count(*) FROM rollback_test"),
        1
    );
}
