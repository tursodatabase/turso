// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Session isolation over the wire protocol: every client connection must
//! get its own session, so concurrent transactions and session state do
//! not interleave. PostgreSQL gives every connection its own backend;
//! sharing one session between clients silently merges their transactions.

use super::wire::{exec, query_int, start_server};
use turso_pg_client::PgConn;

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
