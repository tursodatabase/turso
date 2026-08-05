// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! The COPY sub-protocol over the wire: `COPY ... FROM STDIN` receives data
//! frames from the client, `COPY ... TO STDOUT` streams them back. The
//! regress corpus loads most of its fixture data through inline COPY, and
//! psql's `\copy` is nothing else.

use super::wire::{exec, query_int, start_server};
use turso_pg_client::PgConn;

/// Drives one `COPY ... FROM STDIN`: sends the statement, waits for
/// CopyInResponse, pushes `data`, and returns the CommandComplete tag.
fn copy_in(conn: &mut PgConn, sql: &str, data: &[u8]) -> String {
    conn.send_query(sql).unwrap();
    let (tag, _) = conn.read_message().unwrap();
    assert_eq!(tag, b'G', "expected CopyInResponse for {sql}");
    conn.send_copy_data(data).unwrap();
    conn.send_copy_done().unwrap();

    let mut command_tag = None;
    loop {
        let (tag, body) = conn.read_message().unwrap();
        match tag {
            b'C' => {
                command_tag = Some(String::from_utf8_lossy(&body[..body.len() - 1]).into_owned())
            }
            b'E' => panic!("COPY failed: {}", String::from_utf8_lossy(&body)),
            b'Z' => return command_tag.expect("no CommandComplete before ReadyForQuery"),
            _ => {}
        }
    }
}

/// Drives one `COPY ... TO STDOUT` and returns the streamed data.
fn copy_out(conn: &mut PgConn, sql: &str) -> (String, String) {
    conn.send_query(sql).unwrap();
    let (tag, _) = conn.read_message().unwrap();
    assert_eq!(tag, b'H', "expected CopyOutResponse for {sql}");

    let mut data = Vec::new();
    let mut command_tag = None;
    loop {
        let (tag, body) = conn.read_message().unwrap();
        match tag {
            b'd' => data.extend_from_slice(&body),
            b'c' => {}
            b'C' => {
                command_tag = Some(String::from_utf8_lossy(&body[..body.len() - 1]).into_owned())
            }
            b'E' => panic!("COPY failed: {}", String::from_utf8_lossy(&body)),
            b'Z' => {
                return (
                    String::from_utf8(data).unwrap(),
                    command_tag.expect("no CommandComplete"),
                )
            }
            _ => {}
        }
    }
}

#[test]
fn copy_from_stdin_loads_rows() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE copy_in_t (a int, b text)");
    let tag = copy_in(
        &mut conn,
        "COPY copy_in_t FROM STDIN",
        b"1\tone\n2\ttwo\n3\t\\N\n",
    );
    assert_eq!(tag, "COPY 3");

    assert_eq!(query_int(&mut conn, "SELECT count(*) FROM copy_in_t"), 3);
    assert_eq!(
        query_int(&mut conn, "SELECT count(*) FROM copy_in_t WHERE b IS NULL"),
        1
    );
    assert_eq!(
        query_int(&mut conn, "SELECT a FROM copy_in_t WHERE b = 'two'"),
        2
    );
}

#[test]
fn copy_to_stdout_streams_rows() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE copy_out_t (a int, b text)");
    exec(
        &mut conn,
        "INSERT INTO copy_out_t VALUES (1, 'one'), (2, NULL)",
    );

    let (data, tag) = copy_out(&mut conn, "COPY copy_out_t TO STDOUT");
    assert_eq!(data, "1\tone\n2\t\\N\n");
    assert_eq!(tag, "COPY 2");

    // The query form streams the same way.
    let (data, tag) = copy_out(
        &mut conn,
        "COPY (SELECT b FROM copy_out_t WHERE a = 1) TO STDOUT",
    );
    assert_eq!(data, "one\n");
    assert_eq!(tag, "COPY 1");
}

#[test]
fn copy_round_trip_preserves_escaped_fields() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE copy_esc (a int, b text)");
    // A field containing a tab and a backslash must survive a round trip.
    copy_in(
        &mut conn,
        "COPY copy_esc FROM STDIN",
        b"1\thas\\ttab and \\\\backslash\n",
    );
    let (data, _) = copy_out(&mut conn, "COPY copy_esc TO STDOUT");
    assert_eq!(data, "1\thas\\ttab and \\\\backslash\n");
}
