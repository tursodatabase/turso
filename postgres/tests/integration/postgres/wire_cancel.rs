// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! CancelRequest handling. A client cancels a running query by opening a
//! dedicated connection and sending the (pid, secret) pair from the
//! session's BackendKeyData; the running statement fails with SQLSTATE
//! 57014 and the session stays usable. Drivers surface this as query
//! timeouts (pgJDBC setQueryTimeout) and Ctrl-C (psql), so it is
//! load-bearing far beyond interactive use.

use std::time::Duration;

use super::wire::{exec, query_int, start_server};
use turso_pg_client::{BackendEvent, PgConn};

#[test]
fn cancel_request_interrupts_a_running_query() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    let (pid, secret) = conn.backend_key().expect("server must send BackendKeyData");

    exec(&mut conn, "CREATE TABLE cancel_t (v int)");
    let values: Vec<String> = (1..=64).map(|i| format!("({i})")).collect();
    exec(
        &mut conn,
        &format!("INSERT INTO cancel_t VALUES {}", values.join(", ")),
    );

    // A four-way cross join over 64 rows is ~17M combinations — far more
    // work than fits in the 300ms window before the cancel arrives.
    conn.send_query(
        "SELECT count(*) FROM cancel_t a, cancel_t b, cancel_t c, cancel_t d \
         WHERE a.v + b.v + c.v + d.v < 0",
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    PgConn::cancel_request(&params, pid, secret).unwrap();

    let mut error = None;
    loop {
        match conn.read_event().unwrap() {
            BackendEvent::ErrorResponse(fields) => {
                error.get_or_insert(fields);
            }
            BackendEvent::ReadyForQuery(_) => break,
            _ => {}
        }
    }
    let fields = error.expect("the cancelled query must fail");
    assert_eq!(
        fields.get(&b'C').map(String::as_str),
        Some("57014"),
        "cancellation SQLSTATE"
    );

    // The session survives the cancellation.
    assert_eq!(query_int(&mut conn, "SELECT count(*) FROM cancel_t"), 64);
}

#[test]
fn cancel_with_wrong_secret_is_ignored() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();
    let (pid, secret) = conn.backend_key().expect("server must send BackendKeyData");

    // PostgreSQL silently ignores a cancel whose secret does not match;
    // the session must be unaffected.
    PgConn::cancel_request(&params, pid, secret ^ 0x5eed).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(query_int(&mut conn, "SELECT 1"), 1);
}

#[test]
fn backend_key_is_unique_per_session() {
    let (params, _dir) = start_server();
    let a = PgConn::connect(&params, &[]).unwrap();
    let b = PgConn::connect(&params, &[]).unwrap();
    assert_ne!(
        a.backend_key().expect("BackendKeyData"),
        b.backend_key().expect("BackendKeyData"),
        "two sessions must not share a cancellation keypair"
    );
}
