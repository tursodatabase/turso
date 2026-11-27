use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

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
fn time_zone_other_than_utc_is_refused_at_connection() {
    let server = TestServer::start();
    for startup in [
        vec![("TimeZone", "America/New_York")],
        vec![("options", "-c TimeZone=America/New_York")],
        vec![("options", "--TimeZone=America/New_York")],
        // PostgreSQL would run this one in New York.
        vec![
            ("TimeZone", "America/New_York"),
            ("options", "-c TimeZone=UTC"),
        ],
    ] {
        let error = server.connect_error(&startup);
        assert!(error.contains("0A000"), "{startup:?}: {error}");
        assert!(error.contains("America/New_York"), "{startup:?}: {error}");
    }
    for startup in [("TimeZone", "UTC"), ("options", "-c TimeZone=Etc/UTC")] {
        let mut client = server.connect_with(&[startup]);
        assert_eq!(query(&mut client, "SELECT 1"), ["1"]);
    }
}

/// A refusal reaches the client with a workaround in the HINT field of the
/// ErrorResponse, whether the translator or the frontend refused the
/// statement.
#[test]
fn refusal_hint_reaches_the_client() {
    let server = TestServer::start();
    let mut client = server.connect();
    for (sql, hint) in [
        (
            // Refused by the translator.
            "CREATE OR REPLACE VIEW v AS SELECT 1 AS a",
            "Run DROP VIEW and CREATE VIEW in one transaction.",
        ),
        (
            // Refused by the frontend, after translation.
            "SELECT DATE '2020-02-03' + INTERVAL '1 day'",
            "Cast the date to timestamp first: d::timestamp + interval.",
        ),
    ] {
        let fields = error_fields(&mut client, sql);
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("0A000"),
            "{sql}"
        );
        let sent = fields
            .get(&b'H')
            .unwrap_or_else(|| panic!("{sql}: no hint"));
        assert!(sent.starts_with(hint), "{sql}: {sent}");
    }
}

/// A computed bytea value is described as bytea, at the top level and when
/// an outer query reads it from a subquery.
#[test]
fn computed_bytea_is_described_as_bytea() {
    const BYTEA_OID: u32 = 17;
    let server = TestServer::start();
    let mut client = server.connect();
    query(
        &mut client,
        "CREATE TABLE t1 (id int PRIMARY KEY, v varchar(5))",
    );
    assert_eq!(
        column_type_oids(&mut client, "SELECT v::bytea AS x FROM t1"),
        vec![BYTEA_OID]
    );
    assert_eq!(
        column_type_oids(
            &mut client,
            "SELECT x FROM (SELECT v::bytea AS x FROM t1) s"
        ),
        vec![BYTEA_OID]
    );
    assert_eq!(
        column_type_oids(&mut client, "SELECT v::bytea || v::bytea AS x FROM t1"),
        vec![BYTEA_OID]
    );
    assert_eq!(
        column_type_oids(
            &mut client,
            "SELECT x FROM (SELECT v::bytea || v::bytea AS x FROM t1) s"
        ),
        vec![BYTEA_OID]
    );
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
        self.try_connect(startup).unwrap()
    }

    /// The error of a connection that the server refuses, with its SQLSTATE.
    fn connect_error(&self, startup: &[(&str, &str)]) -> String {
        format!(
            "{:?}",
            self.try_connect(startup).err().expect("expected a refusal")
        )
    }

    fn try_connect(&self, startup: &[(&str, &str)]) -> turso_pg_client::Result<PgConn> {
        let params = ConnParams {
            host: "127.0.0.1".to_string(),
            port: self.port,
            user: "postgres".to_string(),
            password: None,
            database: "main".to_string(),
        };
        PgConn::connect(&params, startup)
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

/// Runs `sql`, which must fail, and returns the fields of its ErrorResponse.
fn error_fields(client: &mut PgConn, sql: &str) -> turso_pg_client::ErrorFields {
    for event in client.simple_query(sql).unwrap() {
        if let BackendEvent::ErrorResponse(fields) = event {
            return fields;
        }
    }
    panic!("{sql}: expected an error");
}

/// Runs `sql` and returns the type OIDs of its RowDescription.
fn column_type_oids(client: &mut PgConn, sql: &str) -> Vec<u32> {
    for event in client.simple_query(sql).unwrap() {
        match event {
            BackendEvent::RowDescription(columns) => {
                return columns.iter().map(|column| column.type_oid).collect();
            }
            BackendEvent::ErrorResponse(fields) => {
                panic!("{sql}: {}", error_message(&fields));
            }
            _ => {}
        }
    }
    panic!("{sql}: no RowDescription");
}
