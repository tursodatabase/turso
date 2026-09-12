use super::*;

fn memory_server() -> TursoMcpServer {
    let (_io, conn) =
        Connection::from_uri(":memory:", DatabaseOpts::default(), Arc::new(SqliteDialect))
            .expect("open memory database");
    TursoMcpServer::new(conn, Arc::new(AtomicUsize::new(0)))
}

fn query_arg(sql: &str) -> Option<Value> {
    Some(json!({ "query": sql }))
}

fn seed_bench_orders(server: &TursoMcpServer) {
    let conn = server.conn.lock().unwrap().clone();
    conn.execute(
        "CREATE TABLE bench_orders (
                order_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL
            )",
    )
    .unwrap();
    conn.execute("INSERT INTO bench_orders VALUES (1, 'READY', 1), (2, 'HOLD', 2)")
        .unwrap();
}

fn orders_dump(server: &TursoMcpServer) -> String {
    server.execute_query(&query_arg(
        "SELECT order_id, status, priority FROM bench_orders ORDER BY order_id",
    ))
}

#[test]
fn update_data_rejects_trailing_delete() {
    let server = memory_server();
    seed_bench_orders(&server);

    let result = server.update_data(&query_arg(
        "UPDATE bench_orders SET status='DONE' WHERE order_id=1; DELETE FROM bench_orders WHERE order_id=2",
    ));

    assert!(
        result.contains("Only a single UPDATE statement is allowed"),
        "expected single-statement rejection, got: {result}"
    );

    let dump = orders_dump(&server);
    assert!(
        dump.contains("1 | READY | 1"),
        "UPDATE must not run: {dump}"
    );
    assert!(
        dump.contains("2 | HOLD | 2"),
        "trailing DELETE must not run: {dump}"
    );
}

#[test]
fn update_data_allows_semicolon_inside_string() {
    let server = memory_server();
    seed_bench_orders(&server);

    let result = server.update_data(&query_arg(
        "UPDATE bench_orders SET status='DONE; DELETE' WHERE order_id=1",
    ));
    assert_eq!(result, "UPDATE successful.");

    let dump = orders_dump(&server);
    assert!(dump.contains("1 | DONE; DELETE | 1"), "{dump}");
    assert!(dump.contains("2 | HOLD | 2"), "{dump}");
}

#[test]
fn insert_data_rejects_trailing_delete() {
    let server = memory_server();
    seed_bench_orders(&server);

    let result = server.insert_data(&query_arg(
        "INSERT INTO bench_orders VALUES (3, 'NEW', 3); DELETE FROM bench_orders WHERE order_id=2",
    ));

    assert!(
        result.contains("Only a single INSERT statement is allowed"),
        "expected single-statement rejection, got: {result}"
    );
    assert!(!orders_dump(&server).contains("3 | NEW | 3"));
    assert!(orders_dump(&server).contains("2 | HOLD | 2"));
}

#[test]
fn delete_data_rejects_trailing_drop() {
    let server = memory_server();
    seed_bench_orders(&server);

    let result = server.delete_data(&query_arg(
        "DELETE FROM bench_orders WHERE order_id=1; DROP TABLE bench_orders",
    ));

    assert!(
        result.contains("Only a single DELETE statement is allowed"),
        "expected single-statement rejection, got: {result}"
    );
    let dump = orders_dump(&server);
    assert!(dump.contains("1 | READY | 1"), "{dump}");
    assert!(dump.contains("2 | HOLD | 2"), "{dump}");
}

#[test]
fn schema_change_rejects_trailing_delete() {
    let server = memory_server();
    seed_bench_orders(&server);

    let result = server.schema_change(&query_arg(
        "CREATE TABLE extra (id INTEGER); DELETE FROM bench_orders",
    ));

    assert!(
        result.contains("Only a single schema modification statement is allowed"),
        "expected single-statement rejection, got: {result}"
    );
    assert!(orders_dump(&server).contains("1 | READY | 1"));
    assert!(orders_dump(&server).contains("2 | HOLD | 2"));
}

#[test]
fn execute_query_rejects_trailing_delete() {
    let server = memory_server();
    seed_bench_orders(&server);

    let result = server.execute_query(&query_arg(
        "SELECT order_id FROM bench_orders WHERE order_id=1; DELETE FROM bench_orders WHERE order_id=2",
    ));

    assert!(
        result.contains("Only a single SELECT query is allowed"),
        "expected single-statement rejection, got: {result}"
    );
    assert!(orders_dump(&server).contains("2 | HOLD | 2"));
}

#[test]
fn update_data_accepts_single_update() {
    let server = memory_server();
    seed_bench_orders(&server);

    let result = server.update_data(&query_arg(
        "UPDATE bench_orders SET status='DONE' WHERE order_id=1",
    ));
    assert_eq!(result, "UPDATE successful.");
    assert!(orders_dump(&server).contains("1 | DONE | 1"));
    assert!(orders_dump(&server).contains("2 | HOLD | 2"));
}
