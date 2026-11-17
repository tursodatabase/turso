use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use turso_core::{ConnectOpts, SessionRole};
use turso_pg_client::{error_message, BackendEvent, ConnParams, PgConn};
use turso_pg_server::TursoPgServer;

#[test]
fn transaction_of_one_client_is_not_visible_to_another() {
    let server = TestServer::start();
    let mut client1 = server.connect();
    let mut client2 = server.connect();
    query(&mut client1, "CREATE TABLE t (x int)");

    query(&mut client1, "BEGIN");
    query(&mut client1, "INSERT INTO t VALUES (1)");

    assert_eq!(query(&mut client2, "SELECT count(*) FROM t"), vec!["0"]);
    // COMMIT outside a transaction block succeeds like in PostgreSQL, and
    // must not end the other client's open transaction.
    query(&mut client2, "COMMIT");
    assert_eq!(query(&mut client2, "SELECT count(*) FROM t"), vec!["0"]);
    query(&mut client1, "COMMIT");
    assert_eq!(query(&mut client2, "SELECT count(*) FROM t"), vec!["1"]);
}

#[test]
fn search_path_of_one_client_does_not_change_another() {
    let server = TestServer::start();
    let mut client1 = server.connect();
    let mut client2 = server.connect();
    query(&mut client1, "CREATE TABLE t (x text)");
    query(&mut client1, "INSERT INTO t VALUES ('public')");
    query(&mut client1, "CREATE SCHEMA s");
    query(&mut client1, "CREATE TABLE s.t (x text)");
    query(&mut client1, "INSERT INTO s.t VALUES ('s')");

    query(&mut client1, "SET search_path TO s");

    assert_eq!(query(&mut client1, "SELECT x FROM t"), vec!["s"]);
    assert_eq!(query(&mut client2, "SELECT x FROM t"), vec!["public"]);
}

#[test]
fn schema_created_by_one_client_is_visible_to_another() {
    let server = TestServer::start();
    let mut client1 = server.connect();
    let mut client2 = server.connect();
    query(&mut client2, "SELECT 1");

    query(&mut client1, "CREATE SCHEMA s");
    query(&mut client1, "CREATE TABLE s.t (x int)");
    query(&mut client1, "INSERT INTO s.t VALUES (1)");

    assert_eq!(query(&mut client2, "SELECT x FROM s.t"), vec!["1"]);
}

#[test]
fn schema_dropped_by_one_client_is_gone_for_another() {
    let server = TestServer::start();
    let mut client1 = server.connect();
    let mut client2 = server.connect();
    query(&mut client1, "CREATE SCHEMA s");
    query(&mut client1, "CREATE TABLE s.t (x int)");
    assert_eq!(query(&mut client2, "SELECT count(*) FROM s.t"), vec!["0"]);

    query(&mut client1, "DROP SCHEMA s CASCADE");

    query_error(&mut client2, "SELECT count(*) FROM s.t");
}

#[test]
fn startup_parameters_of_the_jdbc_driver_are_accepted() {
    let server = TestServer::start();
    // The parameters that the PostgreSQL JDBC driver sends when the JVM's
    // time zone is UTC.
    let mut client = server.connect_with(&[
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO"),
        ("TimeZone", "UTC"),
        ("extra_float_digits", "3"),
        ("application_name", "PostgreSQL JDBC Driver"),
    ]);
    assert_eq!(
        query(&mut client, "SELECT TIMESTAMPTZ '2024-01-05 10:00:00+02'"),
        vec!["2024-01-05 08:00:00+00"]
    );
}

#[test]
fn startup_parameter_that_cannot_be_set_ends_the_connection() {
    let server = TestServer::start();
    for (name, value) in [
        ("TimeZone", "Europe/Helsinki"),
        ("DateStyle", "German"),
        ("IntervalStyle", "sql_standard"),
        ("search_path", "app"),
        ("role", "postgres"),
        ("app.user_id", "42"),
        ("no_such_parameter", "1"),
        ("options", "-c TimeZone=UTC"),
        ("_pq_.option", "1"),
    ] {
        let reply = startup_reply(server.port, name, value);
        assert!(
            reply.starts_with('E') && reply.contains("SFATAL\0") && reply.contains("C0A000\0"),
            "{name}={value}: {reply}"
        );
    }
}

#[test]
fn startup_time_zone_is_the_session_time_zone() {
    let server = TestServer::start();
    let mut client = server.connect_with(&[("TimeZone", "utc")]);
    assert_eq!(query(&mut client, "SHOW TimeZone"), vec!["UTC"]);
    query(&mut client, "RESET TimeZone");
    assert_eq!(query(&mut client, "SHOW TimeZone"), vec!["UTC"]);
    assert_eq!(
        query_error(&mut client, "SET TimeZone = 'Etc/UTC'"),
        "time zone \"Etc/UTC\" is not supported; the session's time zone is \"UTC\" and cannot be changed"
    );
    let mut client = server.connect();
    assert_eq!(query(&mut client, "SHOW TimeZone"), vec!["Etc/UTC"]);
    // The time zone in the ParameterStatus message sent at startup.
    let reply = startup_reply(server.port, "TimeZone", "utc");
    assert!(reply.contains("TimeZone\0UTC\0"), "{reply}");
    let reply = startup_reply(server.port, "application_name", "test");
    assert!(reply.contains("TimeZone\0Etc/UTC\0"), "{reply}");
}

/// Sends a startup message with `name=value` and returns what the server
/// sends until it closes the connection or waits for a query.
fn startup_reply(port: u16, name: &str, value: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut body = b"\0\x03\0\0user\0postgres\0".to_vec();
    for text in [name, value] {
        body.extend_from_slice(text.as_bytes());
        body.push(0);
    }
    body.push(0);
    stream
        .write_all(&(body.len() as u32 + 4).to_be_bytes())
        .unwrap();
    stream.write_all(&body).unwrap();
    let mut reply = Vec::new();
    let mut buffer = [0; 1024];
    // ReadyForQuery with an idle transaction status.
    while !reply.ends_with(b"Z\0\0\0\x05I") {
        match stream.read(&mut buffer).unwrap() {
            0 => break,
            read => reply.extend_from_slice(&buffer[..read]),
        }
    }
    String::from_utf8_lossy(&reply).into_owned()
}

struct TestServer {
    port: u16,
    _dir: TempDir,
}

impl TestServer {
    fn start() -> Self {
        let dir = TempDir::new().unwrap();
        let db_file = dir.path().join("test.db").to_string_lossy().to_string();
        let (_io, db) = turso_pg::open_database(
            &db_file,
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let server = TursoPgServer::new(
            "127.0.0.1:0".to_string(),
            db_file,
            db,
            ConnectOpts::new(SessionRole::Superuser),
            Arc::new(AtomicUsize::new(0)),
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || runtime.block_on(server.serve(listener)));
        Self { port, _dir: dir }
    }

    fn connect(&self) -> PgConn {
        self.connect_with(&[])
    }

    fn connect_with(&self, startup: &[(&str, &str)]) -> PgConn {
        let params = ConnParams {
            host: "127.0.0.1".to_string(),
            port: self.port,
            user: "postgres".to_string(),
            password: None,
            database: "main".to_string(),
        };
        PgConn::connect(&params, startup).unwrap()
    }
}

/// Runs `sql` and returns the first column of each row.
fn query(client: &mut PgConn, sql: &str) -> Vec<String> {
    let mut values = Vec::new();
    for event in client.simple_query(sql).unwrap() {
        match event {
            BackendEvent::DataRow(columns) => {
                values.push(columns[0].clone().unwrap_or_default());
            }
            BackendEvent::ErrorResponse(fields) => {
                panic!("{sql}: {}", error_message(&fields));
            }
            _ => {}
        }
    }
    values
}

/// Runs `sql`, which must fail, and returns the error message.
fn query_error(client: &mut PgConn, sql: &str) -> String {
    for event in client.simple_query(sql).unwrap() {
        if let BackendEvent::ErrorResponse(fields) = event {
            return error_message(&fields).to_string();
        }
    }
    panic!("{sql}: expected an error");
}
