// Hegel property-based parity tests for turso (local) vs turso_serverless.
//
// Generates random sequences of database operations and asserts that both
// drivers produce structurally identical results: same success/failure,
// same column counts/names, same row counts, same value types, and same
// actual cell values.
//
// Requires a running libsql-server for the remote driver (throttling disabled
// so property tests don't hit 429s):
//   docker run -d -p 8080:8080 \
//     -e SQLD_MAX_CONCURRENT_REQUESTS=1024 \
//     -e SQLD_MAX_CONCURRENT_CONNECTIONS=1024 \
//     -e SQLD_DISABLE_INTELLIGENT_THROTTLING=true \
//     ghcr.io/tursodatabase/libsql-server:latest

use hegel::generators as gs;
use hegel::TestCase;

use turso_hegel_parity::{
    auth_token, create_table_sql, gen_error_sql, gen_ops, server_url, spec_num_tables, Op, Val,
};

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

fn normalize_turso(v: &turso::Value) -> NormalizedValue {
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

// ---------------------------------------------------------------------------
// Structural result — compared across drivers
// ---------------------------------------------------------------------------

#[derive(Debug)]
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
    OpResult {
        success: false,
        column_count: None,
        column_names: None,
        row_count: None,
        value_types: None,
        values: None,
        column_decltypes: None,
        affected_rows: None,
        last_insert_rowid: None,
    }
}

// ---------------------------------------------------------------------------
// Value conversion helpers
// ---------------------------------------------------------------------------

fn val_to_turso(v: &Val) -> turso::Value {
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

fn value_type_tag(v: &turso::Value) -> String {
    match v {
        turso::Value::Null => "null".into(),
        turso::Value::Integer(_) => "integer".into(),
        turso::Value::Real(_) => "real".into(),
        turso::Value::Text(_) => "text".into(),
        turso::Value::Blob(_) => "blob".into(),
    }
}

fn remote_value_type_tag(v: &turso_serverless::Value) -> String {
    match v {
        turso_serverless::Value::Null => "null".into(),
        turso_serverless::Value::Integer(_) => "integer".into(),
        turso_serverless::Value::Real(_) => "real".into(),
        turso_serverless::Value::Text(_) => "text".into(),
        turso_serverless::Value::Blob(_) => "blob".into(),
    }
}

// ---------------------------------------------------------------------------
// Local query helper — reduces boilerplate for query-returning ops
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
            let mut row_types = Vec::new();
            let mut row_values = Vec::new();
            let mut row_count = 0usize;
            while let Ok(Some(row)) = rows.next().await {
                let mut types = Vec::new();
                let mut vals = Vec::new();
                for i in 0..col_count {
                    match row.get_value(i) {
                        Ok(v) => {
                            types.push(value_type_tag(&v));
                            vals.push(normalize_turso(&v));
                        }
                        Err(_) => {
                            types.push("unknown".into());
                            vals.push(NormalizedValue::Null);
                        }
                    }
                }
                row_types.push(types);
                row_values.push(vals);
                row_count += 1;
            }
            OpResult {
                success: true,
                column_count: Some(col_count),
                column_names: Some(col_names),
                row_count: Some(row_count),
                value_types: Some(row_types),
                values: Some(row_values),
                column_decltypes: Some(col_decltypes),
                affected_rows: None,
                last_insert_rowid: None,
            }
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
            let mut row_types = Vec::new();
            let mut row_values = Vec::new();
            let mut row_count = 0usize;
            while let Ok(Some(row)) = rows.next().await {
                let mut types = Vec::new();
                let mut vals = Vec::new();
                for i in 0..col_count {
                    match row.get_value(i) {
                        Ok(v) => {
                            types.push(remote_value_type_tag(&v));
                            vals.push(normalize_remote(&v));
                        }
                        Err(_) => {
                            types.push("unknown".into());
                            vals.push(NormalizedValue::Null);
                        }
                    }
                }
                row_types.push(types);
                row_values.push(vals);
                row_count += 1;
            }
            OpResult {
                success: true,
                column_count: Some(col_count),
                column_names: Some(col_names),
                row_count: Some(row_count),
                value_types: Some(row_types),
                values: Some(row_values),
                column_decltypes: Some(col_decltypes),
                affected_rows: None,
                last_insert_rowid: None,
            }
        }
        Err(_) => fail_result(),
    }
}

// ---------------------------------------------------------------------------
// Execute operations against each driver
// ---------------------------------------------------------------------------

fn exec_ok(affected: Option<u64>) -> OpResult {
    OpResult {
        success: true,
        column_count: None,
        column_names: None,
        row_count: Some(0),
        value_types: None,
        values: None,
        column_decltypes: None,
        affected_rows: affected,
        last_insert_rowid: None,
    }
}

fn execute_op_local<'a>(
    conn: &'a turso::Connection,
    op: &'a Op,
    in_tx: &'a mut bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = OpResult> + 'a>> {
    Box::pin(execute_op_local_inner(conn, op, in_tx))
}

async fn execute_op_local_inner(conn: &turso::Connection, op: &Op, in_tx: &mut bool) -> OpResult {
    match op {
        Op::CreateTable { name, cols } | Op::CreateTableDynamic { name, cols } => {
            let sql = create_table_sql(name, cols);
            match conn.execute(&sql, ()).await {
                Ok(_) => exec_ok(None),
                Err(_) => fail_result(),
            }
        }
        Op::Insert { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso::Value> = values.iter().map(val_to_turso).collect();
            match conn.execute(&sql, params).await {
                Ok(n) => OpResult {
                    success: true,
                    column_count: None,
                    column_names: None,
                    row_count: Some(n as usize),
                    value_types: None,
                    values: None,
                    column_decltypes: None,
                    affected_rows: None,
                    last_insert_rowid: None,
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
            let params: Vec<turso::Value> = values.iter().map(val_to_turso).collect();
            query_local(conn, &sql, params).await
        }
        Op::DeleteReturning { table } => {
            let sql = format!("DELETE FROM {table} RETURNING *");
            query_local(conn, &sql, vec![]).await
        }
        Op::UpdateReturning { table, value } => {
            let sql = format!("UPDATE {table} SET a = ? RETURNING *");
            query_local(conn, &sql, vec![val_to_turso(value)]).await
        }
        Op::Select { table } => {
            let sql = format!("SELECT * FROM {table}");
            query_local(conn, &sql, vec![]).await
        }
        Op::SelectValue { expr } => {
            let sql = format!("SELECT {expr}");
            query_local(conn, &sql, vec![]).await
        }
        Op::BeginTx => match conn.execute("BEGIN", ()).await {
            Ok(_) => {
                *in_tx = true;
                exec_ok(None)
            }
            Err(_) => fail_result(),
        },
        Op::CommitTx => match conn.execute("COMMIT", ()).await {
            Ok(_) => {
                *in_tx = false;
                exec_ok(None)
            }
            Err(_) => fail_result(),
        },
        Op::RollbackTx => match conn.execute("ROLLBACK", ()).await {
            Ok(_) => {
                *in_tx = false;
                exec_ok(None)
            }
            Err(_) => fail_result(),
        },
        Op::InvalidSQL { sql } => match conn.query(sql, ()).await {
            Ok(_) => OpResult {
                success: true,
                column_count: None,
                column_names: None,
                row_count: None,
                value_types: None,
                values: None,
                column_decltypes: None,
                affected_rows: None,
                last_insert_rowid: None,
            },
            Err(_) => fail_result(),
        },
        Op::Param { sql, params } => {
            let p: Vec<turso::Value> = params.iter().map(val_to_turso).collect();
            query_local(conn, sql, p).await
        }
        Op::NamedParam { values } => {
            let cols: Vec<String> = values.iter().map(|(n, _)| format!(":{n}")).collect();
            let sql = format!("SELECT {}", cols.join(", "));
            let named: Vec<(String, turso::Value)> = values
                .iter()
                .map(|(n, v)| (format!(":{n}"), val_to_turso(v)))
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
                    let mut row_types = Vec::new();
                    let mut row_values = Vec::new();
                    let mut row_count = 0usize;
                    while let Ok(Some(row)) = rows.next().await {
                        let mut types = Vec::new();
                        let mut vals = Vec::new();
                        for i in 0..col_count {
                            match row.get_value(i) {
                                Ok(v) => {
                                    types.push(value_type_tag(&v));
                                    vals.push(normalize_turso(&v));
                                }
                                Err(_) => {
                                    types.push("unknown".into());
                                    vals.push(NormalizedValue::Null);
                                }
                            }
                        }
                        row_types.push(types);
                        row_values.push(vals);
                        row_count += 1;
                    }
                    OpResult {
                        success: true,
                        column_count: Some(col_count),
                        column_names: Some(col_names),
                        row_count: Some(row_count),
                        value_types: Some(row_types),
                        values: Some(row_values),
                        column_decltypes: Some(col_decltypes),
                        affected_rows: None,
                        last_insert_rowid: None,
                    }
                }
                Err(_) => fail_result(),
            }
        }
        Op::NumberedParam { values } => {
            let cols: Vec<String> = (1..=values.len()).map(|i| format!("?{i}")).collect();
            let sql = format!("SELECT {}", cols.join(", "));
            let p: Vec<turso::Value> = values.iter().map(val_to_turso).collect();
            query_local(conn, &sql, p).await
        }
        Op::InsertAffected { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso::Value> = values.iter().map(val_to_turso).collect();
            match conn.execute(&sql, params).await {
                Ok(n) => OpResult {
                    success: true,
                    column_count: None,
                    column_names: None,
                    row_count: None,
                    value_types: None,
                    values: None,
                    column_decltypes: None,
                    affected_rows: Some(n),
                    last_insert_rowid: None,
                },
                Err(_) => fail_result(),
            }
        }
        Op::DeleteAffected { table } => {
            let sql = format!("DELETE FROM {table}");
            match conn.execute(&sql, ()).await {
                Ok(n) => OpResult {
                    success: true,
                    column_count: None,
                    column_names: None,
                    row_count: None,
                    value_types: None,
                    values: None,
                    column_decltypes: None,
                    affected_rows: Some(n),
                    last_insert_rowid: None,
                },
                Err(_) => fail_result(),
            }
        }
        Op::UpdateAffected { table, value } => {
            let sql = format!("UPDATE {table} SET a = ?");
            let params = vec![val_to_turso(value)];
            match conn.execute(&sql, params).await {
                Ok(n) => OpResult {
                    success: true,
                    column_count: None,
                    column_names: None,
                    row_count: None,
                    value_types: None,
                    values: None,
                    column_decltypes: None,
                    affected_rows: Some(n),
                    last_insert_rowid: None,
                },
                Err(_) => fail_result(),
            }
        }
        Op::InsertRowid { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso::Value> = values.iter().map(val_to_turso).collect();
            match conn.execute(&sql, params).await {
                Ok(_) => match conn.query("SELECT last_insert_rowid()", ()).await {
                    Ok(mut rows) => {
                        let rowid = if let Ok(Some(row)) = rows.next().await {
                            match row.get_value(0) {
                                Ok(turso::Value::Integer(n)) => Some(n),
                                _ => None,
                            }
                        } else {
                            None
                        };
                        OpResult {
                            success: true,
                            column_count: None,
                            column_names: None,
                            row_count: None,
                            value_types: None,
                            values: None,
                            column_decltypes: None,
                            affected_rows: None,
                            last_insert_rowid: rowid,
                        }
                    }
                    Err(_) => fail_result(),
                },
                Err(_) => fail_result(),
            }
        }
        Op::Batch { sql } => match conn.execute_batch(sql).await {
            Ok(()) => exec_ok(None),
            Err(_) => fail_result(),
        },
        Op::SelectLimit { table } => {
            let sql = format!("SELECT * FROM {table} LIMIT 1");
            query_local(conn, &sql, vec![]).await
        }
        Op::SelectCount { table } => {
            let sql = format!("SELECT COUNT(*), SUM(a) FROM {table}");
            query_local(conn, &sql, vec![]).await
        }
        Op::SelectExpr { param } => {
            let sql = "SELECT 1+1, 'hello'||'world', NULL, CAST(3.14 AS INTEGER), typeof(?)";
            query_local(conn, sql, vec![val_to_turso(param)]).await
        }
        Op::ErrorCheck { sql } => {
            let success = conn.query(sql, ()).await.is_ok();
            OpResult {
                success,
                column_count: None,
                column_names: None,
                row_count: None,
                value_types: None,
                values: None,
                column_decltypes: None,
                affected_rows: None,
                last_insert_rowid: None,
            }
        }
        Op::PreparedReuse { params_sets } => {
            let sql = "SELECT ?, ?";
            match conn.prepare(sql).await {
                Ok(mut stmt) => {
                    let mut all_values = Vec::new();
                    let mut all_types = Vec::new();
                    for params in params_sets {
                        let p: Vec<turso::Value> = params.iter().map(val_to_turso).collect();
                        match stmt.query(p).await {
                            Ok(mut rows) => {
                                let col_count = rows.column_count();
                                while let Ok(Some(row)) = rows.next().await {
                                    let mut types = Vec::new();
                                    let mut vals = Vec::new();
                                    for i in 0..col_count {
                                        match row.get_value(i) {
                                            Ok(v) => {
                                                types.push(value_type_tag(&v));
                                                vals.push(normalize_turso(&v));
                                            }
                                            Err(_) => {
                                                types.push("unknown".into());
                                                vals.push(NormalizedValue::Null);
                                            }
                                        }
                                    }
                                    all_types.push(types);
                                    all_values.push(vals);
                                }
                            }
                            Err(_) => return fail_result(),
                        }
                    }
                    OpResult {
                        success: true,
                        column_count: Some(2),
                        column_names: Some(vec!["?".into(), "?".into()]),
                        row_count: Some(all_values.len()),
                        value_types: Some(all_types),
                        values: Some(all_values),
                        column_decltypes: Some(vec![None, None]),
                        affected_rows: None,
                        last_insert_rowid: None,
                    }
                }
                Err(_) => fail_result(),
            }
        }
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
            *in_tx = true;
            for inner in inner_ops {
                let _ = execute_op_local(conn, inner, in_tx).await;
            }
            let end_sql = if *commit { "COMMIT" } else { "ROLLBACK" };
            match conn.execute(end_sql, ()).await {
                Ok(_) => {
                    *in_tx = false;
                    exec_ok(None)
                }
                Err(_) => {
                    *in_tx = false;
                    fail_result()
                }
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
            *in_tx = true;
            let _ = execute_op_local(conn, good_op, in_tx).await;
            let _ = conn.execute(bad_sql, ()).await;
            let _ = execute_op_local(conn, recovery_op, in_tx).await;
            match conn.execute("ROLLBACK", ()).await {
                Ok(_) => {
                    *in_tx = false;
                    exec_ok(None)
                }
                Err(_) => {
                    *in_tx = false;
                    fail_result()
                }
            }
        }
    }
}

fn execute_op_remote<'a>(
    conn: &'a turso_serverless::Connection,
    op: &'a Op,
    in_tx: &'a mut bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = OpResult> + 'a>> {
    Box::pin(execute_op_remote_inner(conn, op, in_tx))
}

async fn execute_op_remote_inner(
    conn: &turso_serverless::Connection,
    op: &Op,
    in_tx: &mut bool,
) -> OpResult {
    match op {
        Op::CreateTable { name, cols } | Op::CreateTableDynamic { name, cols } => {
            let sql = create_table_sql(name, cols);
            match conn.execute(&sql, ()).await {
                Ok(_) => exec_ok(None),
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
                    column_count: None,
                    column_names: None,
                    row_count: Some(n as usize),
                    value_types: None,
                    values: None,
                    column_decltypes: None,
                    affected_rows: None,
                    last_insert_rowid: None,
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
            let sql = format!("DELETE FROM {table} RETURNING *");
            query_remote(conn, &sql, vec![]).await
        }
        Op::UpdateReturning { table, value } => {
            let sql = format!("UPDATE {table} SET a = ? RETURNING *");
            query_remote(conn, &sql, vec![val_to_remote(value)]).await
        }
        Op::Select { table } => {
            let sql = format!("SELECT * FROM {table}");
            query_remote(conn, &sql, vec![]).await
        }
        Op::SelectValue { expr } => {
            let sql = format!("SELECT {expr}");
            query_remote(conn, &sql, vec![]).await
        }
        Op::BeginTx => match conn.execute("BEGIN", ()).await {
            Ok(_) => {
                *in_tx = true;
                exec_ok(None)
            }
            Err(_) => fail_result(),
        },
        Op::CommitTx => match conn.execute("COMMIT", ()).await {
            Ok(_) => {
                *in_tx = false;
                exec_ok(None)
            }
            Err(_) => fail_result(),
        },
        Op::RollbackTx => match conn.execute("ROLLBACK", ()).await {
            Ok(_) => {
                *in_tx = false;
                exec_ok(None)
            }
            Err(_) => fail_result(),
        },
        Op::InvalidSQL { sql } => match conn.query(sql, ()).await {
            Ok(_) => OpResult {
                success: true,
                column_count: None,
                column_names: None,
                row_count: None,
                value_types: None,
                values: None,
                column_decltypes: None,
                affected_rows: None,
                last_insert_rowid: None,
            },
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
                    let mut row_types = Vec::new();
                    let mut row_values = Vec::new();
                    let mut row_count = 0usize;
                    while let Ok(Some(row)) = rows.next().await {
                        let mut types = Vec::new();
                        let mut vals = Vec::new();
                        for i in 0..col_count {
                            match row.get_value(i) {
                                Ok(v) => {
                                    types.push(remote_value_type_tag(&v));
                                    vals.push(normalize_remote(&v));
                                }
                                Err(_) => {
                                    types.push("unknown".into());
                                    vals.push(NormalizedValue::Null);
                                }
                            }
                        }
                        row_types.push(types);
                        row_values.push(vals);
                        row_count += 1;
                    }
                    OpResult {
                        success: true,
                        column_count: Some(col_count),
                        column_names: Some(col_names),
                        row_count: Some(row_count),
                        value_types: Some(row_types),
                        values: Some(row_values),
                        column_decltypes: Some(col_decltypes),
                        affected_rows: None,
                        last_insert_rowid: None,
                    }
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
                Ok(n) => OpResult {
                    success: true,
                    column_count: None,
                    column_names: None,
                    row_count: None,
                    value_types: None,
                    values: None,
                    column_decltypes: None,
                    affected_rows: Some(n),
                    last_insert_rowid: None,
                },
                Err(_) => fail_result(),
            }
        }
        Op::DeleteAffected { table } => {
            let sql = format!("DELETE FROM {table}");
            match conn.execute(&sql, ()).await {
                Ok(n) => OpResult {
                    success: true,
                    column_count: None,
                    column_names: None,
                    row_count: None,
                    value_types: None,
                    values: None,
                    column_decltypes: None,
                    affected_rows: Some(n),
                    last_insert_rowid: None,
                },
                Err(_) => fail_result(),
            }
        }
        Op::UpdateAffected { table, value } => {
            let sql = format!("UPDATE {table} SET a = ?");
            let params = vec![val_to_remote(value)];
            match conn.execute(&sql, params).await {
                Ok(n) => OpResult {
                    success: true,
                    column_count: None,
                    column_names: None,
                    row_count: None,
                    value_types: None,
                    values: None,
                    column_decltypes: None,
                    affected_rows: Some(n),
                    last_insert_rowid: None,
                },
                Err(_) => fail_result(),
            }
        }
        Op::InsertRowid { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            match conn.execute(&sql, params).await {
                Ok(_) => match conn.query("SELECT last_insert_rowid()", ()).await {
                    Ok(mut rows) => {
                        let rowid = if let Ok(Some(row)) = rows.next().await {
                            match row.get_value(0) {
                                Ok(turso_serverless::Value::Integer(n)) => Some(n),
                                _ => None,
                            }
                        } else {
                            None
                        };
                        OpResult {
                            success: true,
                            column_count: None,
                            column_names: None,
                            row_count: None,
                            value_types: None,
                            values: None,
                            column_decltypes: None,
                            affected_rows: None,
                            last_insert_rowid: rowid,
                        }
                    }
                    Err(_) => fail_result(),
                },
                Err(_) => fail_result(),
            }
        }
        Op::Batch { sql } => match conn.execute_batch(sql).await {
            Ok(()) => exec_ok(None),
            Err(_) => fail_result(),
        },
        Op::SelectLimit { table } => {
            let sql = format!("SELECT * FROM {table} LIMIT 1");
            query_remote(conn, &sql, vec![]).await
        }
        Op::SelectCount { table } => {
            let sql = format!("SELECT COUNT(*), SUM(a) FROM {table}");
            query_remote(conn, &sql, vec![]).await
        }
        Op::SelectExpr { param } => {
            let sql = "SELECT 1+1, 'hello'||'world', NULL, CAST(3.14 AS INTEGER), typeof(?)";
            query_remote(conn, sql, vec![val_to_remote(param)]).await
        }
        Op::ErrorCheck { sql } => {
            let success = conn.query(sql, ()).await.is_ok();
            OpResult {
                success,
                column_count: None,
                column_names: None,
                row_count: None,
                value_types: None,
                values: None,
                column_decltypes: None,
                affected_rows: None,
                last_insert_rowid: None,
            }
        }
        Op::PreparedReuse { params_sets } => {
            let sql = "SELECT ?, ?";
            match conn.prepare(sql).await {
                Ok(mut stmt) => {
                    let mut all_values = Vec::new();
                    let mut all_types = Vec::new();
                    for params in params_sets {
                        let p: Vec<turso_serverless::Value> =
                            params.iter().map(val_to_remote).collect();
                        match stmt.query(p).await {
                            Ok(mut rows) => {
                                let col_count = rows.column_count();
                                while let Ok(Some(row)) = rows.next().await {
                                    let mut types = Vec::new();
                                    let mut vals = Vec::new();
                                    for i in 0..col_count {
                                        match row.get_value(i) {
                                            Ok(v) => {
                                                types.push(remote_value_type_tag(&v));
                                                vals.push(normalize_remote(&v));
                                            }
                                            Err(_) => {
                                                types.push("unknown".into());
                                                vals.push(NormalizedValue::Null);
                                            }
                                        }
                                    }
                                    all_types.push(types);
                                    all_values.push(vals);
                                }
                            }
                            Err(_) => return fail_result(),
                        }
                    }
                    OpResult {
                        success: true,
                        column_count: Some(2),
                        column_names: Some(vec!["?".into(), "?".into()]),
                        row_count: Some(all_values.len()),
                        value_types: Some(all_types),
                        values: Some(all_values),
                        column_decltypes: Some(vec![None, None]),
                        affected_rows: None,
                        last_insert_rowid: None,
                    }
                }
                Err(_) => fail_result(),
            }
        }
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
            *in_tx = true;
            for inner in inner_ops {
                let _ = execute_op_remote(conn, inner, in_tx).await;
            }
            let end_sql = if *commit { "COMMIT" } else { "ROLLBACK" };
            match conn.execute(end_sql, ()).await {
                Ok(_) => {
                    *in_tx = false;
                    exec_ok(None)
                }
                Err(_) => {
                    *in_tx = false;
                    fail_result()
                }
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
            *in_tx = true;
            let _ = execute_op_remote(conn, good_op, in_tx).await;
            let _ = conn.execute(bad_sql, ()).await;
            let _ = execute_op_remote(conn, recovery_op, in_tx).await;
            match conn.execute("ROLLBACK", ()).await {
                Ok(_) => {
                    *in_tx = false;
                    exec_ok(None)
                }
                Err(_) => {
                    *in_tx = false;
                    fail_result()
                }
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

async fn connect_remote() -> turso_serverless::Connection {
    let mut builder = turso_serverless::Builder::new_remote(server_url());
    if let Some(token) = auth_token() {
        builder = builder.with_auth_token(token);
    }
    let db = builder.build().await.unwrap();
    db.connect().unwrap()
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

#[hegel::test(database = None)]
fn api_parity(tc: TestCase) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Fresh connections per test case — no stale transaction state.
        let local = connect_local().await;
        let remote = connect_remote().await;

        // Unique table prefix per test case — deterministic via Hegel draw.
        // Offset by 0 so api_parity uses range [0, 65535].
        let prefix: u32 = tc.draw(gs::integers::<u16>()) as u32;

        // Drop any leftover tables for this prefix (from prior runs or Hegel replays).
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
        let mut local_in_tx = false;
        let mut remote_in_tx = false;

        // Accumulate an operation trace so failures show the full history.
        let mut trace: Vec<String> = Vec::new();

        for (i, op) in ops.iter().enumerate() {
            let local_result = execute_op_local(&local, op, &mut local_in_tx).await;
            let remote_result = execute_op_remote(&remote, op, &mut remote_in_tx).await;

            let trace_line = format!(
                "  op[{i}]: {op:?}\n    local:  ok={} cols={:?} rows={:?} affected={:?}\n    remote: ok={} cols={:?} rows={:?} affected={:?}",
                local_result.success, local_result.column_count, local_result.row_count, local_result.affected_rows,
                remote_result.success, remote_result.column_count, remote_result.row_count, remote_result.affected_rows,
            );
            trace.push(trace_line);

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

#[hegel::test(database = None)]
fn ddl_in_transaction(tc: TestCase) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let local = connect_local().await;
        let remote = connect_remote().await;

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
            local_inner.success,
            "local: SELECT inside txn failed after CREATE+INSERT"
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
            remote_inner.success,
            "remote: SELECT inside txn failed after CREATE+INSERT"
        );
        remote
            .execute("COMMIT", ())
            .await
            .expect("remote: COMMIT failed");
    });
}

// ---------------------------------------------------------------------------
// DDL + prepare() in transactions: prepare() calls describe which must see
// tables created earlier in the same transaction.
// ---------------------------------------------------------------------------

#[hegel::test(database = None)]
fn ddl_prepare_in_transaction(tc: TestCase) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let local = connect_local().await;
        let remote = connect_remote().await;

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
            local_inner.success,
            "local: SELECT inside txn failed after CREATE+prepare(INSERT)"
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
            remote_inner.success,
            "remote: SELECT inside txn failed after CREATE+prepare(INSERT)"
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

#[hegel::test(database = None)]
fn error_recovery(tc: TestCase) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let remote = connect_remote().await;

        let prefix: u32 = tc.draw(gs::integers::<u16>()) as u32 + 300_000;
        let error_sql = gen_error_sql(&tc, prefix);

        // Send the error-inducing SQL (expected to fail)
        let _ = remote.execute(&error_sql, ()).await;

        // The critical assertion: SELECT 1 must succeed afterward
        let mut rows = remote
            .query("SELECT 1", ())
            .await
            .unwrap_or_else(|e| panic!("SELECT 1 failed after error SQL {:?}: {e}", error_sql));
        let row = rows
            .next()
            .await
            .expect("should have a row")
            .expect("row should be Ok");
        let val = row.get_value(0).expect("should get column 0");
        match val {
            turso_serverless::Value::Integer(1) => {} // OK
            other => panic!(
                "SELECT 1 returned {other:?} after error SQL {:?}",
                error_sql
            ),
        }
    });
}
