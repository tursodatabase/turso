// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Binary result formats. JDBC and asyncpg bind portals with binary result
//! columns, so every common type must encode PostgreSQL's documented binary
//! wire representation: big-endian fixed-width integers and floats, days or
//! microseconds since 2000-01-01 for date/time types, raw bytes for text
//! and bytea, a version byte plus text for jsonb, and base-10000 digit
//! groups for numeric.

use super::wire::{exec, start_server};
use turso_pg_client::PgConn;

fn one_row(conn: &mut PgConn, sql: &str) -> Vec<Option<Vec<u8>>> {
    let rows = conn.extended_query_binary_results(sql, &[]).unwrap();
    assert_eq!(rows.len(), 1, "expected one row for {sql}");
    rows.into_iter().next().unwrap()
}

#[test]
fn binary_results_encode_postgres_wire_representations() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(
        &mut conn,
        "CREATE TABLE bin_t (a int, b bigint, c double precision, d boolean, \
         e text, f bytea, g date, h timestamp, i uuid, j numeric(10,2), k text)",
    );
    exec(
        &mut conn,
        "INSERT INTO bin_t VALUES (42, 5000000000, 2.5, true, 'hello', \
         decode('deadbeef', 'hex'), '2024-01-15', '2024-01-15 12:00:00', \
         '550e8400-e29b-41d4-a716-446655440000', 123.45, NULL)",
    );

    let row = one_row(
        &mut conn,
        "SELECT a, b, c, d, e, f, g, h, i, j, k FROM bin_t",
    );
    assert_eq!(row[0].as_deref(), Some(&42i32.to_be_bytes()[..]), "int4");
    assert_eq!(
        row[1].as_deref(),
        Some(&5_000_000_000i64.to_be_bytes()[..]),
        "int8"
    );
    assert_eq!(row[2].as_deref(), Some(&2.5f64.to_be_bytes()[..]), "float8");
    assert_eq!(row[3].as_deref(), Some(&[1u8][..]), "bool");
    assert_eq!(row[4].as_deref(), Some(&b"hello"[..]), "text");
    assert_eq!(
        row[5].as_deref(),
        Some(&[0xde, 0xad, 0xbe, 0xef][..]),
        "bytea"
    );
    // 2024-01-15 is 8780 days after PostgreSQL's 2000-01-01 epoch.
    assert_eq!(row[6].as_deref(), Some(&8780i32.to_be_bytes()[..]), "date");
    // (8780 days * 86400 + 12 hours) in microseconds since 2000-01-01.
    assert_eq!(
        row[7].as_deref(),
        Some(&758_635_200_000_000i64.to_be_bytes()[..]),
        "timestamp"
    );
    assert_eq!(
        row[8].as_deref(),
        Some(
            &[
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00
            ][..]
        ),
        "uuid"
    );
    // The engine represents numeric as float8 end-to-end today (real
    // numeric is Phase 3 work), so the wire type and binary encoding are
    // float8's; numeric_wire_bytes has unit coverage for when that flips.
    assert_eq!(
        row[9].as_deref(),
        Some(&123.45f64.to_be_bytes()[..]),
        "numeric"
    );
    assert_eq!(row[10], None, "null");
}

#[test]
fn binary_results_cover_expressions_and_negatives() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    let row = one_row(&mut conn, "SELECT 1, -2.5::float8");
    assert_eq!(row[0].as_deref(), Some(&1i32.to_be_bytes()[..]));
    assert_eq!(row[1].as_deref(), Some(&(-2.5f64).to_be_bytes()[..]));

    // Bool literals type as integer in expressions (an engine typing gap),
    // so false goes through a boolean column.
    exec(&mut conn, "CREATE TABLE bin_b (v boolean)");
    exec(&mut conn, "INSERT INTO bin_b VALUES (false)");
    let row = one_row(&mut conn, "SELECT v FROM bin_b");
    assert_eq!(row[0].as_deref(), Some(&[0u8][..]));
}

#[test]
fn binary_results_encode_jsonb_with_version_byte() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE bin_j (a jsonb)");
    exec(&mut conn, "INSERT INTO bin_j VALUES ('{\"a\":1}')");

    let row = one_row(&mut conn, "SELECT a FROM bin_j");
    let bytes = row[0].as_deref().expect("jsonb value");
    assert_eq!(bytes[0], 1, "jsonb binary format starts with version 1");
    assert!(
        std::str::from_utf8(&bytes[1..]).unwrap().contains("\"a\""),
        "jsonb payload is the json text"
    );
}
