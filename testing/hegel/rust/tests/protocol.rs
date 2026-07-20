// Protocol property test: runs random operation sequences against a remote
// driver (typically through the hrana-proxy) and checks that no protocol
// violations occur.
//
// Unlike the parity test, this does NOT compare against a local driver.
// It only asserts that operations either succeed or fail with a normal SQL
// error — never with a proxy protocol violation.
//
// Usage:
//   # Start the proxy in front of libsql-server:
//   cargo run -p hrana-proxy -- --upstream http://localhost:8080 --listen 0.0.0.0:9090
//
//   # Run this test through the proxy:
//   TURSO_DATABASE_URL=http://localhost:9090 cargo test --test protocol -p turso_hegel_parity -- --nocapture --test-threads=1

use hegel::generators as gs;
use hegel::TestCase;

use turso_hegel_parity::{
    auth_token, create_table_sql, gen_ops, server_url, spec_num_tables, Op, Val,
};

// ---------------------------------------------------------------------------
// Value conversion
// ---------------------------------------------------------------------------

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
// Connection helper
// ---------------------------------------------------------------------------

async fn connect_remote() -> turso_serverless::Connection {
    let mut builder = turso_serverless::Builder::new_remote(server_url());
    if let Some(token) = auth_token() {
        builder = builder.with_auth_token(token);
    }
    let db = builder.build().await.unwrap();
    db.connect().unwrap()
}

// ---------------------------------------------------------------------------
// Execute an op against the remote driver, checking for proxy violations.
// Normal SQL errors are fine — proxy_error responses are not.
// ---------------------------------------------------------------------------

fn is_proxy_error(err: &turso_serverless::Error) -> bool {
    let msg = format!("{err}");
    msg.contains("proxy_error") || msg.contains("proxy error")
}

fn execute_op_remote<'a>(
    conn: &'a turso_serverless::Connection,
    op: &'a Op,
    in_tx: &'a mut bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + 'a>> {
    Box::pin(execute_op_remote_inner(conn, op, in_tx))
}

async fn execute_op_remote_inner(
    conn: &turso_serverless::Connection,
    op: &Op,
    in_tx: &mut bool,
) -> Result<(), String> {
    match op {
        Op::CreateTable { name, cols } | Op::CreateTableDynamic { name, cols } => {
            let sql = create_table_sql(name, cols);
            check(conn.execute(&sql, ()).await)
        }
        Op::Insert { table, values }
        | Op::InsertAffected { table, values }
        | Op::InsertRowid { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!("INSERT INTO {} VALUES ({})", table, placeholders.join(", "));
            let params: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            check(conn.execute(&sql, params).await)
        }
        Op::InsertReturning { table, values } => {
            let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
            let sql = format!(
                "INSERT INTO {} VALUES ({}) RETURNING *",
                table,
                placeholders.join(", ")
            );
            let params: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            check(conn.query(&sql, params).await)
        }
        Op::DeleteReturning { table } => check(
            conn.query(&format!("DELETE FROM {table} RETURNING *"), ())
                .await,
        ),
        Op::UpdateReturning { table, value } => {
            let sql = format!("UPDATE {table} SET a = ? RETURNING *");
            check(conn.query(&sql, vec![val_to_remote(value)]).await)
        }
        Op::Select { table } => check(conn.query(&format!("SELECT * FROM {table}"), ()).await),
        Op::SelectValue { expr } => check(conn.query(&format!("SELECT {expr}"), ()).await),
        Op::BeginTx => {
            let r = check(conn.execute("BEGIN", ()).await);
            if r.is_ok() {
                *in_tx = true;
            }
            r
        }
        Op::CommitTx => {
            let r = check(conn.execute("COMMIT", ()).await);
            if r.is_ok() {
                *in_tx = false;
            }
            r
        }
        Op::RollbackTx => {
            let r = check(conn.execute("ROLLBACK", ()).await);
            if r.is_ok() {
                *in_tx = false;
            }
            r
        }
        Op::InvalidSQL { sql } => check(conn.query(sql, ()).await),
        Op::Param { sql, params } => {
            let p: Vec<turso_serverless::Value> = params.iter().map(val_to_remote).collect();
            check(conn.query(sql, p).await)
        }
        Op::NamedParam { values } => {
            let cols: Vec<String> = values.iter().map(|(n, _)| format!(":{n}")).collect();
            let sql = format!("SELECT {}", cols.join(", "));
            let named: Vec<(String, turso_serverless::Value)> = values
                .iter()
                .map(|(n, v)| (format!(":{n}"), val_to_remote(v)))
                .collect();
            check(conn.query(&sql, named).await)
        }
        Op::NumberedParam { values } => {
            let cols: Vec<String> = (1..=values.len()).map(|i| format!("?{i}")).collect();
            let sql = format!("SELECT {}", cols.join(", "));
            let p: Vec<turso_serverless::Value> = values.iter().map(val_to_remote).collect();
            check(conn.query(&sql, p).await)
        }
        Op::DeleteAffected { table } => {
            check(conn.execute(&format!("DELETE FROM {table}"), ()).await)
        }
        Op::UpdateAffected { table, value } => {
            let sql = format!("UPDATE {table} SET a = ?");
            check(conn.execute(&sql, vec![val_to_remote(value)]).await)
        }
        Op::Batch { sql } => check(conn.execute_batch(sql).await),
        Op::SelectLimit { table } => check(
            conn.query(&format!("SELECT * FROM {table} LIMIT 1"), ())
                .await,
        ),
        Op::SelectCount { table } => check(
            conn.query(&format!("SELECT COUNT(*), SUM(a) FROM {table}"), ())
                .await,
        ),
        Op::SelectExpr { param } => {
            let sql = "SELECT 1+1, 'hello'||'world', NULL, CAST(3.14 AS INTEGER), typeof(?)";
            check(conn.query(sql, vec![val_to_remote(param)]).await)
        }
        Op::ErrorCheck { sql } => check(conn.query(sql, ()).await),
        Op::PreparedReuse { params_sets } => {
            let sql = "SELECT ?, ?";
            match conn.prepare(sql).await {
                Ok(mut stmt) => {
                    for params in params_sets {
                        let p: Vec<turso_serverless::Value> =
                            params.iter().map(val_to_remote).collect();
                        if let Err(e) = stmt.query(p).await {
                            if is_proxy_error(&e) {
                                return Err(format!("proxy error: {e}"));
                            }
                        }
                    }
                    Ok(())
                }
                Err(e) => {
                    if is_proxy_error(&e) {
                        Err(format!("proxy error: {e}"))
                    } else {
                        Ok(())
                    }
                }
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
            let r = check(conn.execute(&trigger_sql, ()).await);
            if r.is_ok() {
                let _ = conn
                    .execute(
                        &format!("INSERT INTO {table} VALUES (42, 'trigger_test')"),
                        (),
                    )
                    .await;
                check(
                    conn.query(&format!("SELECT * FROM {audit} ORDER BY rowid"), ())
                        .await,
                )
            } else {
                r
            }
        }
        Op::TransactionWorkflow { inner_ops, commit } => {
            if let Err(e) = check::<u64>(conn.execute("BEGIN", ()).await) {
                return Err(e);
            }
            *in_tx = true;
            for inner in inner_ops {
                // Ignore normal SQL errors inside the workflow
                let _ = execute_op_remote(conn, inner, in_tx).await;
            }
            let end_sql = if *commit { "COMMIT" } else { "ROLLBACK" };
            let r = check(conn.execute(end_sql, ()).await);
            *in_tx = false;
            r
        }
        Op::ErrorInTransaction {
            good_op,
            bad_sql,
            recovery_op,
        } => {
            if let Err(e) = check::<u64>(conn.execute("BEGIN", ()).await) {
                return Err(e);
            }
            *in_tx = true;
            let _ = execute_op_remote(conn, good_op, in_tx).await;
            let _ = conn.execute(bad_sql, ()).await;
            let _ = execute_op_remote(conn, recovery_op, in_tx).await;
            let r = check(conn.execute("ROLLBACK", ()).await);
            *in_tx = false;
            r
        }
    }
}

/// Check a result for proxy errors. Normal SQL errors are OK.
fn check<T>(result: Result<T, turso_serverless::Error>) -> Result<(), String> {
    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            if is_proxy_error(&e) {
                Err(format!("proxy protocol violation: {e}"))
            } else {
                // Normal SQL error — expected and fine.
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Protocol properties test
// ---------------------------------------------------------------------------

#[hegel::test(database = None)]
fn protocol_properties(tc: TestCase) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let remote = connect_remote().await;

        let prefix: u32 = tc.draw(gs::integers::<u16>()) as u32 + 400_000;

        // Drop any leftover tables for this prefix.
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
        let mut in_tx = false;
        let mut trace: Vec<String> = Vec::new();

        for (i, op) in ops.iter().enumerate() {
            let result = execute_op_remote(&remote, op, &mut in_tx).await;

            trace.push(format!("  op[{i}]: {op:?} -> {result:?}"));

            if let Err(msg) = result {
                panic!(
                    "Protocol violation on op #{i}:\n  {msg}\n\nFull trace (prefix={prefix}):\n{}",
                    trace.join("\n")
                );
            }
        }
    });
}
