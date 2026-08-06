// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Portal suspension: an Execute with a row limit returns at most that
//! many rows and ends with PortalSuspended when more remain; the next
//! Execute on the same portal resumes where it stopped, and the final one
//! ends with CommandComplete counting the rows of that phase. Cursor-style
//! drivers (JDBC setFetchSize, asyncpg cursors) depend on this.

use super::wire::{exec, start_server};
use turso_pg_client::{BackendEvent, PgConn};

const PORTAL_SUSPENDED: u8 = b's';

/// Compresses an event stream into a per-Execute summary: the data-row
/// values of each phase and how the phase ended.
fn phases(events: &[BackendEvent]) -> Vec<(Vec<String>, String)> {
    let mut out = Vec::new();
    let mut rows = Vec::new();
    for event in events {
        match event {
            BackendEvent::DataRow(row) => {
                rows.push(row[0].clone().unwrap_or_default());
            }
            BackendEvent::Other(PORTAL_SUSPENDED) => {
                out.push((std::mem::take(&mut rows), "suspended".to_string()));
            }
            BackendEvent::CommandComplete(tag) => {
                out.push((std::mem::take(&mut rows), tag.clone()));
            }
            BackendEvent::ErrorResponse(fields) => {
                panic!("error: {}", turso_pg_client::error_message(fields));
            }
            _ => {}
        }
    }
    out
}

#[test]
fn execute_row_limit_suspends_and_resumes_the_portal() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE susp_t (v int)");
    exec(
        &mut conn,
        "INSERT INTO susp_t VALUES (1), (2), (3), (4), (5)",
    );

    let events = conn
        .extended_query_row_limits("SELECT v FROM susp_t ORDER BY v", &[2, 2, 0])
        .unwrap();
    let phases = phases(&events);
    assert_eq!(
        phases,
        vec![
            (vec!["1".into(), "2".into()], "suspended".to_string()),
            (vec!["3".into(), "4".into()], "suspended".to_string()),
            (vec!["5".into()], "SELECT 1".to_string()),
        ]
    );
}

#[test]
fn exhausting_a_portal_exactly_at_the_limit_still_suspends() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE susp_e (v int)");
    exec(&mut conn, "INSERT INTO susp_e VALUES (1), (2)");

    // PostgreSQL suspends when the limit is reached even if no rows
    // remain; the next Execute returns zero rows and completes.
    let events = conn
        .extended_query_row_limits("SELECT v FROM susp_e ORDER BY v", &[2, 0])
        .unwrap();
    let phases = phases(&events);
    assert_eq!(
        phases,
        vec![
            (vec!["1".into(), "2".into()], "suspended".to_string()),
            (vec![], "SELECT 0".to_string()),
        ]
    );
}

#[test]
fn unlimited_execute_is_unaffected() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE susp_u (v int)");
    exec(&mut conn, "INSERT INTO susp_u VALUES (1), (2), (3)");

    let events = conn
        .extended_query_row_limits("SELECT v FROM susp_u ORDER BY v", &[0])
        .unwrap();
    let phases = phases(&events);
    assert_eq!(
        phases,
        vec![(
            vec!["1".into(), "2".into(), "3".into()],
            "SELECT 3".to_string()
        )]
    );
}
