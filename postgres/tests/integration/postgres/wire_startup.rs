// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Startup-parameter initialization. Run-time parameters the client sends
//! in the StartupMessage (psql sends application_name, JDBC sends
//! extra_float_digits, poolers send `options` with `-c name=value` pairs)
//! must configure the session before its first query, and RESET must
//! restore the startup-supplied value, not the built-in default, the way
//! PostgreSQL treats client-sourced settings.

use super::wire::start_server;
use turso_pg_client::{error_message, BackendEvent, PgConn};

/// Runs a query returning the first column of the single row as text.
fn query_text(conn: &mut PgConn, sql: &str) -> String {
    let mut result = None;
    for event in conn.simple_query(sql).unwrap() {
        match event {
            BackendEvent::ErrorResponse(fields) => {
                panic!("{sql} failed: {}", error_message(&fields))
            }
            BackendEvent::DataRow(row) => result = Some(row[0].clone().unwrap_or_default()),
            _ => {}
        }
    }
    result.unwrap_or_else(|| panic!("no row returned for: {sql}"))
}

#[test]
fn startup_parameters_configure_the_session() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(
        &params,
        &[
            ("application_name", "wire_startup_test"),
            ("extra_float_digits", "0"),
            ("myapp.tenant", "acme"),
        ],
    )
    .unwrap();

    assert_eq!(
        query_text(&mut conn, "SHOW application_name"),
        "wire_startup_test"
    );
    assert_eq!(
        query_text(&mut conn, "SELECT current_setting('myapp.tenant')"),
        "acme"
    );
    // The startup extra_float_digits actually drives float output.
    assert_eq!(
        query_text(&mut conn, "SELECT sqrt(0.5)"),
        "0.707106781186548"
    );

    // Startup parameters are client-sourced: pg_settings reports them so,
    // and RESET restores them rather than the built-in default.
    assert_eq!(
        query_text(
            &mut conn,
            "SELECT source FROM pg_settings WHERE name = 'application_name'"
        ),
        "client"
    );
    query_text(
        &mut conn,
        "SELECT set_config('application_name', 'changed', false)",
    );
    assert_eq!(query_text(&mut conn, "SHOW application_name"), "changed");
    for event in conn.simple_query("RESET application_name").unwrap() {
        if let BackendEvent::ErrorResponse(fields) = event {
            panic!("RESET failed: {}", error_message(&fields));
        }
    }
    assert_eq!(
        query_text(&mut conn, "SHOW application_name"),
        "wire_startup_test"
    );
}

#[test]
fn startup_options_parameter_carries_dash_c_settings() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(
        &params,
        &[(
            "options",
            "-c myapp.owner=joe -cextra_float_digits=2 --application_name=via_options",
        )],
    )
    .unwrap();

    assert_eq!(
        query_text(&mut conn, "SELECT current_setting('myapp.owner')"),
        "joe"
    );
    assert_eq!(query_text(&mut conn, "SHOW extra_float_digits"), "2");
    assert_eq!(
        query_text(&mut conn, "SHOW application_name"),
        "via_options"
    );
}

#[test]
fn connection_establishment_keys_are_not_treated_as_settings() {
    let (params, _dir) = start_server();
    // `user` and `database` always arrive in the startup packet; they must
    // not leak into the configuration namespace.
    let mut conn = PgConn::connect(&params, &[]).unwrap();
    assert_eq!(
        query_text(
            &mut conn,
            "SELECT count(*) FROM pg_settings WHERE name IN ('user', 'database')"
        ),
        "0"
    );
}
