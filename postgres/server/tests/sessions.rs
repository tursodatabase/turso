use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use tempfile::TempDir;
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
    assert!(query_error(&mut client2, "COMMIT").contains("no transaction is active"));
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
            turso_pg::DatabaseOpts::new().with_attach(true),
        )
        .unwrap();
        let conn = turso_pg::Connection::new(db.connect().unwrap());
        let server = TursoPgServer::new(
            "127.0.0.1:0".to_string(),
            db_file,
            conn,
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
        let params = ConnParams {
            host: "127.0.0.1".to_string(),
            port: self.port,
            user: "postgres".to_string(),
            password: None,
            database: "main".to_string(),
        };
        PgConn::connect(&params, &[]).unwrap()
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
