// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Shared harness for wire-protocol tests: starts a tursopg server over a
//! fresh temp database and drives it with the minimal PostgreSQL client.

use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_pg_client::{error_message, BackendEvent, ConnParams, ErrorFields, PgConn};
use turso_pg_server::TursoPgServer;

/// Starts a tursopg server on a free port over a fresh temp database and
/// returns connection parameters for it. The server thread runs until the
/// test process exits.
pub fn start_server() -> (ConnParams, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_file = dir.path().join("wire.db").to_string_lossy().to_string();

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
pub fn exec(conn: &mut PgConn, sql: &str) {
    for event in conn.simple_query(sql).unwrap() {
        if let BackendEvent::ErrorResponse(fields) = event {
            panic!("{sql} failed: {}", error_message(&fields));
        }
    }
}

/// Runs a query returning a single integer.
pub fn query_int(conn: &mut PgConn, sql: &str) -> i64 {
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

/// Runs a statement that must fail and returns the error fields.
pub fn expect_error(conn: &mut PgConn, sql: &str) -> ErrorFields {
    let mut error = None;
    for event in conn.simple_query(sql).unwrap() {
        if let BackendEvent::ErrorResponse(fields) = event {
            error.get_or_insert(fields);
        }
    }
    error.unwrap_or_else(|| panic!("expected an error for: {sql}"))
}
