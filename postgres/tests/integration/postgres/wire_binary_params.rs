// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Binary-format Bind parameters over the extended query protocol. Drivers
//! switch to binary for prepared statements (JDBC, asyncpg); a server that
//! misreads binary bytes as text breaks them on their first parameterized
//! query.

use super::wire::{exec, query_int, start_server};
use turso_pg_client::{error_message, BackendEvent, PgConn};

const OID_BOOL: u32 = 16;
const OID_INT8: u32 = 20;
const OID_TEXT: u32 = 25;
const OID_FLOAT8: u32 = 701;

/// Runs an extended query and panics on any error event.
fn extended_ok(
    conn: &mut PgConn,
    sql: &str,
    types: &[u32],
    params: &[Option<Vec<u8>>],
    binary: bool,
) -> Vec<BackendEvent> {
    let events = conn.extended_query(sql, types, params, binary).unwrap();
    for event in &events {
        if let BackendEvent::ErrorResponse(fields) = event {
            panic!("{sql} failed: {}", error_message(fields));
        }
    }
    events
}

#[test]
fn binary_bind_parameters_round_trip() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(
        &mut conn,
        "CREATE TABLE bin_params (a bigint, b double precision, c text, d boolean)",
    );

    extended_ok(
        &mut conn,
        "INSERT INTO bin_params VALUES ($1, $2, $3, $4)",
        &[OID_INT8, OID_FLOAT8, OID_TEXT, OID_BOOL],
        &[
            Some(1234567890123i64.to_be_bytes().to_vec()),
            Some(2.5f64.to_be_bytes().to_vec()),
            Some("héllo".as_bytes().to_vec()),
            Some(vec![1]),
        ],
        true,
    );

    let row = conn
        .simple_query("SELECT a, b, c, d FROM bin_params")
        .unwrap()
        .into_iter()
        .find_map(|e| match e {
            BackendEvent::DataRow(row) => Some(row),
            _ => None,
        })
        .expect("no row returned");
    assert_eq!(row[0].as_deref(), Some("1234567890123"));
    assert_eq!(row[1].as_deref(), Some("2.5"));
    assert_eq!(row[2].as_deref(), Some("héllo"));

    // A binary parameter in a WHERE clause must match the stored value.
    let events = extended_ok(
        &mut conn,
        "SELECT count(*) FROM bin_params WHERE a = $1",
        &[OID_INT8],
        &[Some(1234567890123i64.to_be_bytes().to_vec())],
        true,
    );
    let count = events
        .iter()
        .find_map(|e| match e {
            BackendEvent::DataRow(row) => row[0].clone(),
            _ => None,
        })
        .expect("no count returned");
    assert_eq!(count, "1");
}

#[test]
fn binary_null_and_text_parameters_still_work() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE bin_nulls (a bigint, b text)");

    // NULL travels as length -1 regardless of format.
    extended_ok(
        &mut conn,
        "INSERT INTO bin_nulls VALUES ($1, $2)",
        &[OID_INT8, OID_TEXT],
        &[None, Some("x".as_bytes().to_vec())],
        true,
    );
    assert_eq!(
        query_int(&mut conn, "SELECT count(*) FROM bin_nulls WHERE a IS NULL"),
        1
    );

    // Text format through the same extended-protocol path keeps working.
    extended_ok(
        &mut conn,
        "INSERT INTO bin_nulls VALUES ($1, $2)",
        &[OID_INT8, OID_TEXT],
        &[Some(b"42".to_vec()), Some(b"y".to_vec())],
        false,
    );
    assert_eq!(
        query_int(&mut conn, "SELECT count(*) FROM bin_nulls WHERE a = 42"),
        1
    );
}
