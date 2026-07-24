// Property-based differential tests for the Rust turso drivers.
//
// Compares turso (embedded driver, in-memory database) against
// turso_serverless (SQL over HTTP driver) running against a live Turso
// Cloud database. Random operation sequences generated from the shared
// spec (serverless/conformance/differential/spec/ops.json) run against
// both drivers, and every result must match structurally and by value.
//
// Configuration comes from the environment; the tests skip when it is
// missing. See serverless/conformance/differential/README.md for setup:
//   TURSO_DATABASE_URL   Turso Cloud database URL (use a scratch database)
//   TURSO_AUTH_TOKEN     auth token for the database
//   HEGEL_NUM_RUNS       property test iterations per test (default 10)

use hegel::generators as gs;
use hegel::TestCase;

use turso_serverless_conformance::{config, TestConfig};
use turso_serverless_differential::{
    create_table_sql, gen_error_sql, gen_ops, spec_num_tables, Op, Val,
};

// ---------------------------------------------------------------------------
// Configuration. Property test iterations default to 10, sized for a remote
// database over the network; crank HEGEL_NUM_RUNS up for a thorough run.
// ---------------------------------------------------------------------------

fn settings() -> hegel::Settings {
    let runs = std::env::var("HEGEL_NUM_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    // Every test case issues dozens of HTTP round trips, so wall clock time
    // is dominated by network latency, not generation.
    hegel::Settings::new()
        .test_cases(runs)
        .suppress_health_check([hegel::HealthCheck::TooSlow])
}

fn config_or_skip() -> Option<TestConfig> {
    let config = config();
    if config.is_none() {
        static NOTICE: std::sync::Once = std::sync::Once::new();
        NOTICE.call_once(|| {
            eprintln!(
                "skipping: set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN to run the differential tests"
            );
        });
    }
    config
}

// ---------------------------------------------------------------------------
// Normalized values — compared across drivers with epsilon tolerance
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum NormalizedValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

fn values_match(a: &NormalizedValue, b: &NormalizedValue) -> bool {
    match (a, b) {
        (NormalizedValue::Null, NormalizedValue::Null) => true,
        (NormalizedValue::Integer(x), NormalizedValue::Integer(y)) => x == y,
        (NormalizedValue::Real(x), NormalizedValue::Real(y)) => {
            let max = x.abs().max(y.abs());
            if max == 0.0 {
                true
            } else {
                (x - y).abs() / max < 1e-12
            }
        }
        // Integer/Real crossover: SQLite may coerce Integer(1) <-> Real(1.0)
        (NormalizedValue::Integer(x), NormalizedValue::Real(y)) => {
            let xf = *x as f64;
            (xf - y).abs() < 1e-12
        }
        (NormalizedValue::Real(x), NormalizedValue::Integer(y)) => {
            let yf = *y as f64;
            (x - yf).abs() < 1e-12
        }
        (NormalizedValue::Text(x), NormalizedValue::Text(y)) => x == y,
        (NormalizedValue::Blob(x), NormalizedValue::Blob(y)) => x == y,
        _ => false,
    }
}

fn normalize_local(v: &turso::Value) -> NormalizedValue {
    match v {
        turso::Value::Null => NormalizedValue::Null,
        turso::Value::Integer(n) => NormalizedValue::Integer(*n),
        turso::Value::Real(f) => NormalizedValue::Real(*f),
        turso::Value::Text(s) => NormalizedValue::Text(s.clone()),
        turso::Value::Blob(b) => NormalizedValue::Blob(b.clone()),
    }
}

fn normalize_remote(v: &turso_serverless::Value) -> NormalizedValue {
    match v {
        turso_serverless::Value::Null => NormalizedValue::Null,
        turso_serverless::Value::Integer(n) => NormalizedValue::Integer(*n),
        turso_serverless::Value::Real(f) => NormalizedValue::Real(*f),
        turso_serverless::Value::Text(s) => NormalizedValue::Text(s.clone()),
        turso_serverless::Value::Blob(b) => NormalizedValue::Blob(b.clone()),
    }
}

fn value_type_tag(v: &NormalizedValue) -> &'static str {
    match v {
        NormalizedValue::Null => "null",
        NormalizedValue::Integer(_) => "integer",
        NormalizedValue::Real(_) => "real",
        NormalizedValue::Text(_) => "text",
        NormalizedValue::Blob(_) => "blob",
    }
}

// ---------------------------------------------------------------------------
// Structural result — compared across drivers
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct OpResult {
    success: bool,
    column_count: Option<usize>,
    column_names: Option<Vec<String>>,
    row_count: Option<usize>,
    value_types: Option<Vec<Vec<String>>>,
    values: Option<Vec<Vec<NormalizedValue>>>,
    column_decltypes: Option<Vec<Option<String>>>,
    affected_rows: Option<u64>,
    last_insert_rowid: Option<i64>,
}

fn fail_result() -> OpResult {
    OpResult::default()
}

fn success_result() -> OpResult {
    OpResult {
        success: true,
        ..OpResult::default()
    }
}

fn exec_ok() -> OpResult {
    OpResult {
        success: true,
        row_count: Some(0),
        ..OpResult::default()
    }
}

fn rows_result(
    column_names: Vec<String>,
    column_decltypes: Vec<Option<String>>,
    values: Vec<Vec<NormalizedValue>>,
) -> OpResult {
    OpResult {
        success: true,
        column_count: Some(column_names.len()),
        column_names: Some(column_names),
        row_count: Some(values.len()),
        value_types: Some(
            values
                .iter()
                .map(|row| row.iter().map(|v| value_type_tag(v).to_string()).collect())
                .collect(),
        ),
        values: Some(values),
        column_decltypes: Some(column_decltypes),
        ..OpResult::default()
    }
}

fn affected_result(n: u64) -> OpResult {
    OpResult {
        success: true,
        affected_rows: Some(n),
        ..OpResult::default()
    }
}

// ---------------------------------------------------------------------------
// Value conversion helpers
// ---------------------------------------------------------------------------

fn val_to_local(v: &Val) -> turso::Value {
    match v {
        Val::Null => turso::Value::Null,
        Val::Int(n) => turso::Value::Integer(*n),
        Val::Float(f) => turso::Value::Real(*f),
        Val::Text(s) => turso::Value::Text(s.clone()),
        Val::Blob(b) => turso::Value::Blob(b.clone()),
    }
}

fn val_to_remote(v: &Val) -> turso_serverless::Value {
    match v {
        Val::Null => turso_serverless::Value::Null,
        Val::Int(n) => turso_serverless::Value::Integer(*n),
        Val::Float(f) => turso_serverless::Value::Real(*f),
        Val::Text(s) => turso_serverless::Value::Text(s.clone()),
        Val::Blob(b) => turso_serverless::Value::Blob(b.clone()),
    }
}

// ---------------------------------------------------------------------------
// Query helpers — normalize both drivers to the same result shape
// ---------------------------------------------------------------------------

async fn query_local(conn: &turso::Connection, sql: &str, params: Vec<turso::Value>) -> OpResult {
    match conn.query(sql, params).await {
        Ok(mut rows) => {
            let col_names = rows.column_names();
            let col_count = col_names.len();
            let col_decltypes: Vec<Option<String>> = rows
                .columns()
                .iter()
                .map(|c| c.decl_type().map(|s| s.to_string()))
                .collect();
            let mut row_values = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                let vals: Vec<NormalizedValue> = (0..col_count)
                    .map(|i| match row.get_value(i) {
                        Ok(v) => normalize_local(&v),
                        Err(_) => NormalizedValue::Null,
                    })
                    .collect();
                row_values.push(vals);
            }
            rows_result(col_names, col_decltypes, row_values)
        }
        Err(_) => fail_result(),
    }
}

async fn query_remote(
    conn: &turso_serverless::Connection,
    sql: &str,
    params: Vec<turso_serverless::Value>,
) -> OpResult {
    match conn.query(sql, params).await {
        Ok(mut rows) => {
            let col_names = rows.column_names();
            let col_count = col_names.len();
            let col_decltypes: Vec<Option<String>> = rows
                .columns()
                .iter()
                .map(|c| c.decl_type().map(|s| s.to_string()))
                .collect();
            let mut row_values = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                let vals: Vec<NormalizedValue> = (0..col_count)
                    .map(|i| match row.get_value(i) {
                        Ok(v) => normalize_remote(&v),
                        Err(_) => NormalizedValue::Null,
                    })
                    .collect();
                row_values.push(vals);
            }
            rows_result(col_names, col_decltypes, row_values)
        }
        Err(_) => fail_result(),
    }
}

// ---------------------------------------------------------------------------
// Execute operations against each driver
// ---------------------------------------------------------------------------

fn execute_op_local<'a>(
    conn: &'a turso::Connection,
    op: &'a Op,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = OpResult> + 'a>> {
    Box::pin(execute_op_local_inner(conn, op))
}

async fn execute_op_local_inner(conn: &turso::Connection, op: &Op) -> OpResult {
    match op {
        Op::CreateTable { name, cols } | Op::CreateTableDynamic { name, cols } => {
            let sql = create_table_sql(name, cols);
            match conn.execute(&sql, ()).await {
                Ok(_) => exec_ok(),
                Err(_) => fail_result(),
            }
        }
        Op::Insert { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso::Value> = values.iter().map(val_to_local).collect();
            match conn.execute(&sql, params).await {
                Ok(n) => OpResult {
                    success: true,
                    row_count: Some(n as usize),
                    ..OpResult::default()
                },
                Err(_) => fail_result(),
            }
        }
        Op::InsertReturning { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!(
                "INSERT INTO {} VALUES ({}) RETURNING *",
                table,
                placeholders.join(", ")
            );
            let params: Vec<turso::Value> = values.iter().map(val_to_local).collect();
            query_local(conn, &sql, params).await
        }
        Op::DeleteReturning { table } => {
            query_local(conn, &format!("DELETE FROM {table} RETURNING *"), vec![]).await
        }
        Op::UpdateReturning { table, value } => {
            let sql = format!("UPDATE {table} SET a = ? RETURNING *");
            query_local(conn, &sql, vec![val_to_local(value)]).await
        }
        Op::Select { table } => query_local(conn, &format!("SELECT * FROM {table}"), vec![]).await,
        Op::SelectValue { expr } => query_local(conn, &format!("SELECT {expr}"), vec![]).await,
        Op::BeginTx => match conn.execute("BEGIN", ()).await {
            Ok(_) => exec_ok(),
            Err(_) => fail_result(),
        },
        Op::CommitTx => match conn.execute("COMMIT", ()).await {
            Ok(_) => exec_ok(),
            Err(_) => fail_result(),
        },
        Op::RollbackTx => match conn.execute("ROLLBACK", ()).await {
            Ok(_) => exec_ok(),
            Err(_) => fail_result(),
        },
        Op::InvalidSQL { sql } => match conn.query(sql, ()).await {
            Ok(_) => success_result(),
            Err(_) => fail_result(),
        },
        Op::Param { sql, params } => {
            let p: Vec<turso::Value> = params.iter().map(val_to_local).collect();
            query_local(conn, sql, p).await
        }
        Op::NamedParam { values } => {
            let cols: Vec<String> = values.iter().map(|(n, _)| format!(":{n}")).collect();
            let sql = format!("SELECT {}", cols.join(", "));
            let named: Vec<(String, turso::Value)> = values
                .iter()
                .map(|(n, v)| (format!(":{n}"), val_to_local(v)))
                .collect();
            match conn.query(&sql, named).await {
                Ok(mut rows) => {
                    let col_names = rows.column_names();
                    let col_count = col_names.len();
                    let col_decltypes: Vec<Option<String>> = rows
                        .columns()
                        .iter()
                        .map(|c| c.decl_type().map(|s| s.to_string()))
                        .collect();
                    let mut row_values = Vec::new();
                    while let Ok(Some(row)) = rows.next().await {
                        let vals: Vec<NormalizedValue> = (0..col_count)
                            .map(|i| match row.get_value(i) {
                                Ok(v) => normalize_local(&v),
                                Err(_) => NormalizedValue::Null,
                            })
                            .collect();
                        row_values.push(vals);
                    }
                    rows_result(col_names, col_decltypes, row_values)
                }
                Err(_) => fail_result(),
            }
        }
        Op::NumberedParam { values } => {
            let cols: Vec<String> = (1..=values.len()).map(|i| format!("?{i}")).collect();
            let sql = format!("SELECT {}", cols.join(", "));
            let p: Vec<turso::Value> = values.iter().map(val_to_local).collect();
            query_local(conn, &sql, p).await
        }
        Op::InsertAffected { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso::Value> = values.iter().map(val_to_local).collect();
            match conn.execute(&sql, params).await {
                Ok(n) => affected_result(n),
                Err(_) => fail_result(),
            }
        }
        Op::DeleteAffected { table } => {
            match conn.execute(&format!("DELETE FROM {table}"), ()).await {
                Ok(n) => affected_result(n),
                Err(_) => fail_result(),
            }
        }
        Op::UpdateAffected { table, value } => {
            let sql = format!("UPDATE {table} SET a = ?");
            match conn.execute(&sql, vec![val_to_local(value)]).await {
                Ok(n) => affected_result(n),
                Err(_) => fail_result(),
            }
        }
        Op::InsertRowid { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso::Value> = values.iter().map(val_to_local).collect();
            match conn.execute(&sql, params).await {
                Ok(_) => OpResult {
                    success: true,
                    last_insert_rowid: Some(conn.last_insert_rowid()),
                    ..OpResult::default()
                },
                Err(_) => fail_result(),
            }
        }
        Op::Batch { sql } => match conn.execute_batch(sql).await {
            Ok(()) => exec_ok(),
            Err(_) => fail_result(),
        },
        Op::SelectLimit { table } => {
            query_local(conn, &format!("SELECT * FROM {table} LIMIT 1"), vec![]).await
        }
        Op::SelectCount { table } => {
            let sql = format!("SELECT COUNT(*), SUM(a) FROM {table}");
            query_local(conn, &sql, vec![]).await
        }
        Op::SelectExpr { param } => {
            let sql = "SELECT 1+1, 'hello'||'world', NULL, CAST(3.14 AS INTEGER), typeof(?)";
            query_local(conn, sql, vec![val_to_local(param)]).await
        }
        Op::ErrorCheck { sql } => OpResult {
            success: conn.query(sql, ()).await.is_ok(),
            ..OpResult::default()
        },
        Op::PreparedReuse { params_sets } => match conn.prepare("SELECT ?, ?").await {
            Ok(mut stmt) => {
                let mut all_values = Vec::new();
                for params in params_sets {
                    let p: Vec<turso::Value> = params.iter().map(val_to_local).collect();
                    match stmt.query(p).await {
                        Ok(mut rows) => {
                            let col_count = rows.column_count();
                            while let Ok(Some(row)) = rows.next().await {
                                let vals: Vec<NormalizedValue> = (0..col_count)
                                    .map(|i| match row.get_value(i) {
                                        Ok(v) => normalize_local(&v),
                                        Err(_) => NormalizedValue::Null,
                                    })
                                    .collect();
                                all_values.push(vals);
                            }
                        }
                        Err(_) => return fail_result(),
                    }
                }
                rows_result(vec!["?".into(), "?".into()], vec![None, None], all_values)
            }
            Err(_) => fail_result(),
        },
        Op::CreateTrigger { table } => {
            let audit = format!("{table}_audit");
            let _ = conn
                .execute(
                    &format!("CREATE TABLE IF NOT EXISTS {audit} (src TEXT, val)"),
                    (),
                )
                .await;
            let trigger_sql = format!(
                "CREATE TRIGGER IF NOT EXISTS tr_{table}_ins AFTER INSERT ON {table} \
                 BEGIN \
                 INSERT INTO {audit} VALUES ('{table}', NEW.a); \
                 INSERT INTO {audit} VALUES ('{table}', NEW.a * 2); \
                 END"
            );
            match conn.execute(&trigger_sql, ()).await {
                Ok(_) => {
                    let _ = conn
                        .execute(
                            &format!("INSERT INTO {table} VALUES (42, 'trigger_test')"),
                            (),
                        )
                        .await;
                    query_local(
                        conn,
                        &format!("SELECT * FROM {audit} ORDER BY rowid"),
                        vec![],
                    )
                    .await
                }
                Err(_) => fail_result(),
            }
        }
        Op::TransactionWorkflow { inner_ops, commit } => {
            if conn.execute("BEGIN", ()).await.is_err() {
                return fail_result();
            }
            for inner in inner_ops {
                let _ = execute_op_local(conn, inner).await;
            }
            let end_sql = if *commit { "COMMIT" } else { "ROLLBACK" };
            match conn.execute(end_sql, ()).await {
                Ok(_) => exec_ok(),
                Err(_) => fail_result(),
            }
        }
        Op::ErrorInTransaction {
            good_op,
            bad_sql,
            recovery_op,
        } => {
            if conn.execute("BEGIN", ()).await.is_err() {
                return fail_result();
            }
            let _ = execute_op_local(conn, good_op).await;
            let _ = conn.execute(bad_sql, ()).await;
            let _ = execute_op_local(conn, recovery_op).await;
            match conn.execute("ROLLBACK", ()).await {
                Ok(_) => exec_ok(),
                Err(_) => fail_result(),
            }
        }
    }
}

fn execute_op_remote<'a>(
    conn: &'a turso_serverless::Connection,
    op: &'a Op,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = OpResult> + 'a>> {
    Box::pin(execute_op_remote_inner(conn, op))
}

async fn execute_op_remote_inner(conn: &turso_serverless::Connection, op: &Op) -> OpResult {
    match op {
        Op::CreateTable { name, cols } | Op::CreateTableDynamic { name, cols } => {
            let sql = create_table_sql(name, cols);
            match conn.execute(&sql, ()).await {
                Ok(_) => exec_ok(),
                Err(_) => fail_result(),
            }
        }
        Op::Insert { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            match conn.execute(&sql, params).await {
                Ok(n) => OpResult {
                    success: true,
                    row_count: Some(n as usize),
                    ..OpResult::default()
                },
                Err(_) => fail_result(),
            }
        }
        Op::InsertReturning { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!(
                "INSERT INTO {} VALUES ({}) RETURNING *",
                table,
                placeholders.join(", ")
            );
            let params: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            query_remote(conn, &sql, params).await
        }
        Op::DeleteReturning { table } => {
            query_remote(conn, &format!("DELETE FROM {table} RETURNING *"), vec![]).await
        }
        Op::UpdateReturning { table, value } => {
            let sql = format!("UPDATE {table} SET a = ? RETURNING *");
            query_remote(conn, &sql, vec![val_to_remote(value)]).await
        }
        Op::Select { table } => query_remote(conn, &format!("SELECT * FROM {table}"), vec![]).await,
        Op::SelectValue { expr } => query_remote(conn, &format!("SELECT {expr}"), vec![]).await,
        Op::BeginTx => match conn.execute("BEGIN", ()).await {
            Ok(_) => exec_ok(),
            Err(_) => fail_result(),
        },
        Op::CommitTx => match conn.execute("COMMIT", ()).await {
            Ok(_) => exec_ok(),
            Err(_) => fail_result(),
        },
        Op::RollbackTx => match conn.execute("ROLLBACK", ()).await {
            Ok(_) => exec_ok(),
            Err(_) => fail_result(),
        },
        Op::InvalidSQL { sql } => match conn.query(sql, ()).await {
            Ok(_) => success_result(),
            Err(_) => fail_result(),
        },
        Op::Param { sql, params } => {
            let p: Vec<turso_serverless::Value> = params.iter().map(val_to_remote).collect();
            query_remote(conn, sql, p).await
        }
        Op::NamedParam { values } => {
            let cols: Vec<String> = values.iter().map(|(n, _)| format!(":{n}")).collect();
            let sql = format!("SELECT {}", cols.join(", "));
            let named: Vec<(String, turso_serverless::Value)> = values
                .iter()
                .map(|(n, v)| (format!(":{n}"), val_to_remote(v)))
                .collect();
            match conn.query(&sql, named).await {
                Ok(mut rows) => {
                    let col_names = rows.column_names();
                    let col_count = col_names.len();
                    let col_decltypes: Vec<Option<String>> = rows
                        .columns()
                        .iter()
                        .map(|c| c.decl_type().map(|s| s.to_string()))
                        .collect();
                    let mut row_values = Vec::new();
                    while let Ok(Some(row)) = rows.next().await {
                        let vals: Vec<NormalizedValue> = (0..col_count)
                            .map(|i| match row.get_value(i) {
                                Ok(v) => normalize_remote(&v),
                                Err(_) => NormalizedValue::Null,
                            })
                            .collect();
                        row_values.push(vals);
                    }
                    rows_result(col_names, col_decltypes, row_values)
                }
                Err(_) => fail_result(),
            }
        }
        Op::NumberedParam { values } => {
            let cols: Vec<String> = (1..=values.len()).map(|i| format!("?{i}")).collect();
            let sql = format!("SELECT {}", cols.join(", "));
            let p: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            query_remote(conn, &sql, p).await
        }
        Op::InsertAffected { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            match conn.execute(&sql, params).await {
                Ok(n) => affected_result(n),
                Err(_) => fail_result(),
            }
        }
        Op::DeleteAffected { table } => {
            match conn.execute(&format!("DELETE FROM {table}"), ()).await {
                Ok(n) => affected_result(n),
                Err(_) => fail_result(),
            }
        }
        Op::UpdateAffected { table, value } => {
            let sql = format!("UPDATE {table} SET a = ?");
            match conn.execute(&sql, vec![val_to_remote(value)]).await {
                Ok(n) => affected_result(n),
                Err(_) => fail_result(),
            }
        }
        Op::InsertRowid { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            match conn.execute(&sql, params).await {
                Ok(_) => OpResult {
                    success: true,
                    last_insert_rowid: Some(conn.last_insert_rowid()),
                    ..OpResult::default()
                },
                Err(_) => fail_result(),
            }
        }
        Op::Batch { sql } => match conn.execute_batch(sql).await {
            Ok(()) => exec_ok(),
            Err(_) => fail_result(),
        },
        Op::SelectLimit { table } => {
            query_remote(conn, &format!("SELECT * FROM {table} LIMIT 1"), vec![]).await
        }
        Op::SelectCount { table } => {
            let sql = format!("SELECT COUNT(*), SUM(a) FROM {table}");
            query_remote(conn, &sql, vec![]).await
        }
        Op::SelectExpr { param } => {
            let sql = "SELECT 1+1, 'hello'||'world', NULL, CAST(3.14 AS INTEGER), typeof(?)";
            query_remote(conn, sql, vec![val_to_remote(param)]).await
        }
        Op::ErrorCheck { sql } => OpResult {
            success: conn.query(sql, ()).await.is_ok(),
            ..OpResult::default()
        },
        Op::PreparedReuse { params_sets } => match conn.prepare("SELECT ?, ?").await {
            Ok(mut stmt) => {
                let mut all_values = Vec::new();
                for params in params_sets {
                    let p: Vec<turso_serverless::Value> =
                        params.iter().map(val_to_remote).collect();
                    match stmt.query(p).await {
                        Ok(mut rows) => {
                            let col_count = rows.column_count();
                            while let Ok(Some(row)) = rows.next().await {
                                let vals: Vec<NormalizedValue> = (0..col_count)
                                    .map(|i| match row.get_value(i) {
                                        Ok(v) => normalize_remote(&v),
                                        Err(_) => NormalizedValue::Null,
                                    })
                                    .collect();
                                all_values.push(vals);
                            }
                        }
                        Err(_) => return fail_result(),
                    }
                }
                rows_result(vec!["?".into(), "?".into()], vec![None, None], all_values)
            }
            Err(_) => fail_result(),
        },
        Op::CreateTrigger { table } => {
            let audit = format!("{table}_audit");
            let _ = conn
                .execute(
                    &format!("CREATE TABLE IF NOT EXISTS {audit} (src TEXT, val)"),
                    (),
                )
                .await;
            let trigger_sql = format!(
                "CREATE TRIGGER IF NOT EXISTS tr_{table}_ins AFTER INSERT ON {table} \
                 BEGIN \
                 INSERT INTO {audit} VALUES ('{table}', NEW.a); \
                 INSERT INTO {audit} VALUES ('{table}', NEW.a * 2); \
                 END"
            );
            match conn.execute(&trigger_sql, ()).await {
                Ok(_) => {
                    let _ = conn
                        .execute(
                            &format!("INSERT INTO {table} VALUES (42, 'trigger_test')"),
                            (),
                        )
                        .await;
                    query_remote(
                        conn,
                        &format!("SELECT * FROM {audit} ORDER BY rowid"),
                        vec![],
                    )
                    .await
                }
                Err(_) => fail_result(),
            }
        }
        Op::TransactionWorkflow { inner_ops, commit } => {
            if conn.execute("BEGIN", ()).await.is_err() {
                return fail_result();
            }
            for inner in inner_ops {
                let _ = execute_op_remote(conn, inner).await;
            }
            let end_sql = if *commit { "COMMIT" } else { "ROLLBACK" };
            match conn.execute(end_sql, ()).await {
                Ok(_) => exec_ok(),
                Err(_) => fail_result(),
            }
        }
        Op::ErrorInTransaction {
            good_op,
            bad_sql,
            recovery_op,
        } => {
            if conn.execute("BEGIN", ()).await.is_err() {
                return fail_result();
            }
            let _ = execute_op_remote(conn, good_op).await;
            let _ = conn.execute(bad_sql, ()).await;
            let _ = execute_op_remote(conn, recovery_op).await;
            match conn.execute("ROLLBACK", ()).await {
                Ok(_) => exec_ok(),
                Err(_) => fail_result(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Connection helpers
// ---------------------------------------------------------------------------

async fn connect_local() -> turso::Connection {
    let db = turso::Builder::new_local(":memory:").build().await.unwrap();
    db.connect().unwrap()
}

async fn connect_remote(config: &TestConfig) -> turso_serverless::Connection {
    turso_serverless::Builder::new_remote(&config.url)
        .with_auth_token(&config.auth_token)
        .build()
        .await
        .unwrap()
        .connect()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Value comparison helper
// ---------------------------------------------------------------------------

fn assert_values_match(
    local_vals: &Option<Vec<Vec<NormalizedValue>>>,
    remote_vals: &Option<Vec<Vec<NormalizedValue>>>,
    i: usize,
    op: &Op,
) {
    match (local_vals, remote_vals) {
        (Some(lv), Some(rv)) => {
            assert_eq!(
                lv.len(),
                rv.len(),
                "values row count mismatch on op #{i} {op:?}"
            );
            for (r, (lr, rr)) in lv.iter().zip(rv.iter()).enumerate() {
                assert_eq!(
                    lr.len(),
                    rr.len(),
                    "values col count mismatch on op #{i} row {r} {op:?}"
                );
                for (c, (lc, rc)) in lr.iter().zip(rr.iter()).enumerate() {
                    assert!(
                        values_match(lc, rc),
                        "value mismatch on op #{i} row {r} col {c} {op:?}\n  local:  {lc:?}\n  remote: {rc:?}"
                    );
                }
            }
        }
        (None, None) => {}
        _ => panic!(
            "values presence mismatch on op #{i} {op:?}\n  local:  {local_vals:?}\n  remote: {remote_vals:?}"
        ),
    }
}

// ---------------------------------------------------------------------------
// The property test
// ---------------------------------------------------------------------------

#[hegel::test(settings())]
fn api_parity(tc: TestCase) {
    let Some(config) = config_or_skip() else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Fresh connections per test case — no stale transaction state.
        let local = connect_local().await;
        let remote = connect_remote(&config).await;

        // Unique table prefix per test case — deterministic via hegel draw.
        // api_parity uses the prefix range [0, 65535] from the spec.
        let prefix: u32 = tc.draw(gs::integers::<u16>()) as u32;

        // Drop any leftover tables for this prefix (from prior runs or hegel replays).
        for i in 0..spec_num_tables() {
            let _ = remote
                .execute(&format!("DROP TABLE IF EXISTS t_{prefix}_{i}"), ())
                .await;
            let _ = remote
                .execute(&format!("DROP TABLE IF EXISTS t_{prefix}_{i}_audit"), ())
                .await;
            let _ = remote
                .execute(&format!("DROP TRIGGER IF EXISTS tr_t_{prefix}_{i}_ins"), ())
                .await;
        }

        let ops = gen_ops(&tc, prefix);

        // Accumulate an operation trace so failures show the full history.
        let mut trace: Vec<String> = Vec::new();

        for (i, op) in ops.iter().enumerate() {
            let local_result = execute_op_local(&local, op).await;
            let remote_result = execute_op_remote(&remote, op).await;

            trace.push(format!(
                "  op[{i}]: {op:?}\n    local:  ok={} cols={:?} rows={:?} affected={:?}\n    remote: ok={} cols={:?} rows={:?} affected={:?}",
                local_result.success, local_result.column_count, local_result.row_count, local_result.affected_rows,
                remote_result.success, remote_result.column_count, remote_result.row_count, remote_result.affected_rows,
            ));
            let trace_dump = || trace.join("\n");

            // ErrorCheck only compares success/failure — error messages legitimately differ.
            if matches!(op, Op::ErrorCheck { .. }) {
                assert_eq!(
                    local_result.success, remote_result.success,
                    "success mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
                );
                continue;
            }

            assert_eq!(
                local_result.success, remote_result.success,
                "success mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
            );

            // If both failed, stop the sequence — continuing with diverged
            // implicit transaction state leads to false positives.
            if !local_result.success {
                break;
            }

            assert_eq!(
                local_result.column_count, remote_result.column_count,
                "column_count mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
            );
            assert_eq!(
                local_result.column_names, remote_result.column_names,
                "column_names mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
            );
            assert_eq!(
                local_result.row_count, remote_result.row_count,
                "row_count mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
            );
            assert_eq!(
                local_result.value_types, remote_result.value_types,
                "value_types mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
            );
            assert_values_match(&local_result.values, &remote_result.values, i, op);
            assert_eq!(
                local_result.column_decltypes, remote_result.column_decltypes,
                "column_decltypes mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
            );
            assert_eq!(
                local_result.affected_rows, remote_result.affected_rows,
                "affected_rows mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
            );
            assert_eq!(
                local_result.last_insert_rowid, remote_result.last_insert_rowid,
                "last_insert_rowid mismatch on op #{i} {op:?}\n  local:  {local_result:?}\n  remote: {remote_result:?}\n\nFull trace (prefix={prefix}):\n{}", trace_dump()
            );
        }
    });
}

// ---------------------------------------------------------------------------
// DDL visibility in transactions: CREATE TABLE must be visible to subsequent
// statements within the same transaction.
// ---------------------------------------------------------------------------

#[hegel::test(settings())]
fn ddl_in_transaction(tc: TestCase) {
    let Some(config) = config_or_skip() else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let local = connect_local().await;
        let remote = connect_remote(&config).await;

        let prefix: u32 = tc.draw(gs::integers::<u16>()) as u32 + 100_000;
        let table_idx: u8 = tc.draw(gs::integers::<u8>());
        let table_idx = table_idx % spec_num_tables();
        let table = format!("t_{prefix}_{table_idx}");
        let val: i64 = tc.draw(gs::integers::<i64>());

        // Drop any leftover from prior runs so the INSERT produces exactly 1 row.
        let _ = remote
            .execute(&format!("DROP TABLE IF EXISTS {table}"), ())
            .await;

        let create_sql = format!("CREATE TABLE IF NOT EXISTS {table} (a INTEGER, b TEXT)");
        let insert_sql = format!("INSERT INTO {table} VALUES (?, 'txn_ddl')");
        let select_sql = format!("SELECT a FROM {table}");

        // Local: BEGIN -> CREATE -> INSERT -> SELECT -> COMMIT
        local
            .execute("BEGIN", ())
            .await
            .expect("local: BEGIN failed");
        local
            .execute(&create_sql, ())
            .await
            .expect("local: CREATE TABLE failed");
        local
            .execute(&insert_sql, vec![turso::Value::Integer(val)])
            .await
            .expect("local: INSERT failed");
        let local_inner = query_local(&local, &select_sql, vec![]).await;
        assert!(
            local_inner.success && local_inner.row_count == Some(1),
            "local: SELECT inside txn failed after CREATE+INSERT: {local_inner:?}"
        );
        local
            .execute("COMMIT", ())
            .await
            .expect("local: COMMIT failed");

        // Remote: BEGIN -> CREATE -> INSERT -> SELECT -> COMMIT
        remote
            .execute("BEGIN", ())
            .await
            .expect("remote: BEGIN failed");
        remote
            .execute(&create_sql, ())
            .await
            .expect("remote: CREATE TABLE failed");
        remote
            .execute(&insert_sql, vec![turso_serverless::Value::Integer(val)])
            .await
            .expect("remote: INSERT failed");
        let remote_inner = query_remote(&remote, &select_sql, vec![]).await;
        assert!(
            remote_inner.success && remote_inner.row_count == Some(1),
            "remote: SELECT inside txn failed after CREATE+INSERT: {remote_inner:?}"
        );
        remote
            .execute("COMMIT", ())
            .await
            .expect("remote: COMMIT failed");
    });
}

// ---------------------------------------------------------------------------
// DDL + prepare() in transactions: prepare() involves a server round trip
// which must see tables created earlier in the same transaction. This
// catches bugs where describe opens a new session without transaction
// context (issue #6562).
// ---------------------------------------------------------------------------

#[hegel::test(settings())]
fn ddl_prepare_in_transaction(tc: TestCase) {
    let Some(config) = config_or_skip() else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let local = connect_local().await;
        let remote = connect_remote(&config).await;

        let prefix: u32 = tc.draw(gs::integers::<u16>()) as u32 + 200_000;
        let table_idx: u8 = tc.draw(gs::integers::<u8>());
        let table_idx = table_idx % spec_num_tables();
        let table = format!("t_{prefix}_{table_idx}");
        let val: i64 = tc.draw(gs::integers::<i64>());

        // Drop any leftover from prior runs so the INSERT produces exactly 1 row.
        let _ = remote
            .execute(&format!("DROP TABLE IF EXISTS {table}"), ())
            .await;

        let create_sql = format!("CREATE TABLE IF NOT EXISTS {table} (a INTEGER, b TEXT)");
        let insert_sql = format!("INSERT INTO {table} VALUES (?, 'txn_ddl')");
        let select_sql = format!("SELECT a FROM {table}");

        // Local: BEGIN -> CREATE -> prepare(INSERT) -> execute -> SELECT -> COMMIT
        local
            .execute("BEGIN", ())
            .await
            .expect("local: BEGIN failed");
        local
            .execute(&create_sql, ())
            .await
            .expect("local: CREATE TABLE failed");
        let mut local_stmt = local
            .prepare(&insert_sql)
            .await
            .expect("local: prepare(INSERT) failed after CREATE in same txn");
        local_stmt
            .execute(vec![turso::Value::Integer(val)])
            .await
            .expect("local: prepared INSERT execute failed");
        let local_inner = query_local(&local, &select_sql, vec![]).await;
        assert!(
            local_inner.success && local_inner.row_count == Some(1),
            "local: SELECT inside txn failed after CREATE+prepare(INSERT): {local_inner:?}"
        );
        local
            .execute("COMMIT", ())
            .await
            .expect("local: COMMIT failed");

        // Remote: BEGIN -> CREATE -> prepare(INSERT) -> execute -> SELECT -> COMMIT
        remote
            .execute("BEGIN", ())
            .await
            .expect("remote: BEGIN failed");
        remote
            .execute(&create_sql, ())
            .await
            .expect("remote: CREATE TABLE failed");
        let mut remote_stmt = remote
            .prepare(&insert_sql)
            .await
            .expect("remote: prepare(INSERT) failed after CREATE in same txn");
        remote_stmt
            .execute(vec![turso_serverless::Value::Integer(val)])
            .await
            .expect("remote: prepared INSERT execute failed");
        let remote_inner = query_remote(&remote, &select_sql, vec![]).await;
        assert!(
            remote_inner.success && remote_inner.row_count == Some(1),
            "remote: SELECT inside txn failed after CREATE+prepare(INSERT): {remote_inner:?}"
        );
        remote
            .execute("COMMIT", ())
            .await
            .expect("remote: COMMIT failed");
    });
}

// ---------------------------------------------------------------------------
// Error recovery property: errors must never prevent subsequent commands
// ---------------------------------------------------------------------------

#[hegel::test(settings())]
fn error_recovery(tc: TestCase) {
    let Some(config) = config_or_skip() else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let remote = connect_remote(&config).await;

        let prefix: u32 = tc.draw(gs::integers::<u16>()) as u32 + 300_000;
        let error_sql = gen_error_sql(&tc, prefix);

        // Send the error-inducing SQL (expected to fail server-side).
        let _ = remote.execute(&error_sql, ()).await;

        // The critical assertion: SELECT 1 must succeed afterward.
        let mut rows = remote
            .query("SELECT 1", ())
            .await
            .unwrap_or_else(|e| panic!("SELECT 1 failed after error SQL {error_sql:?}: {e}"));
        let row = rows
            .next()
            .await
            .expect("row should be Ok")
            .expect("should have a row");
        let val = row.get_value(0).expect("should get column 0");
        match val {
            turso_serverless::Value::Integer(1) => {}
            other => panic!("SELECT 1 returned {other:?} after error SQL {error_sql:?}"),
        }
    });
}
