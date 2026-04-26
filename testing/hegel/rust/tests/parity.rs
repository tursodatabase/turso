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

use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use hegel::generators as gs;
use hegel::TestCase;
use serde_json::Value as JsonValue;

// ---------------------------------------------------------------------------
// Load spec from ops.json at startup
// ---------------------------------------------------------------------------

static SPEC: LazyLock<JsonValue> = LazyLock::new(|| {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let path = Path::new(manifest).join("../spec/ops.json");
    let data = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read ops.json at {}: {e}", path.display()));
    serde_json::from_str(&data).expect("invalid ops.json")
});

fn spec_num_tables() -> u8 {
    SPEC["constants"]["num_tables"].as_u64().unwrap() as u8
}
fn spec_max_ops() -> u8 {
    SPEC["constants"]["max_ops_per_case"].as_u64().unwrap() as u8
}
fn spec_max_dynamic_cols() -> u8 {
    SPEC["constants"]["max_dynamic_cols"].as_u64().unwrap() as u8
}
fn spec_col_types() -> Vec<&'static str> {
    SPEC["constants"]["col_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            // SAFETY: SPEC is 'static so the str references are 'static
            let s: &str = v.as_str().unwrap();
            unsafe { std::mem::transmute::<&str, &'static str>(s) }
        })
        .collect()
}
fn spec_num_values() -> u8 {
    SPEC["values"].as_array().unwrap().len() as u8
}
fn spec_num_ops() -> u8 {
    SPEC["ops"].as_array().unwrap().len() as u8
}
fn spec_error_sqls() -> &'static Vec<JsonValue> {
    // SAFETY: SPEC is 'static LazyLock
    unsafe {
        std::mem::transmute::<&Vec<JsonValue>, &'static Vec<JsonValue>>(
            SPEC["constants"]["error_sqls"].as_array().unwrap(),
        )
    }
}
fn spec_unicode_options() -> &'static Vec<JsonValue> {
    unsafe {
        std::mem::transmute::<&Vec<JsonValue>, &'static Vec<JsonValue>>(
            SPEC["unicode_options"].as_array().unwrap(),
        )
    }
}

// ---------------------------------------------------------------------------
// Operation vocabulary
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    CreateTable {
        name: String,
        cols: Vec<(String, String)>,
    },
    CreateTableDynamic {
        name: String,
        cols: Vec<(String, String)>,
    },
    Insert {
        table: String,
        values: Vec<Val>,
    },
    InsertReturning {
        table: String,
        values: Vec<Val>,
    },
    DeleteReturning {
        table: String,
    },
    UpdateReturning {
        table: String,
        value: Val,
    },
    Select {
        table: String,
    },
    SelectValue {
        expr: String,
    },
    BeginTx,
    CommitTx,
    RollbackTx,
    InvalidSQL {
        sql: String,
    },
    Param {
        sql: String,
        params: Vec<Val>,
    },
    NamedParam {
        values: Vec<(String, Val)>,
    },
    NumberedParam {
        values: Vec<Val>,
    },
    InsertAffected {
        table: String,
        values: Vec<Val>,
    },
    DeleteAffected {
        table: String,
    },
    UpdateAffected {
        table: String,
        value: Val,
    },
    InsertRowid {
        table: String,
        values: Vec<Val>,
    },
    Batch {
        sql: String,
    },
    SelectLimit {
        table: String,
    },
    SelectCount {
        table: String,
    },
    SelectExpr {
        param: Val,
    },
    ErrorCheck {
        sql: String,
    },
    PreparedReuse {
        params_sets: Vec<Vec<Val>>,
    },
    /// Create a trigger with semicolons inside BEGIN...END, then fire it.
    /// Stresses statement splitting in drivers that split on ';'.
    CreateTrigger {
        /// Source table the trigger fires on
        table: String,
    },
}

#[derive(Debug, Clone)]
enum Val {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
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
// Generators
// ---------------------------------------------------------------------------

fn table_name(tc: &TestCase, prefix: u32) -> String {
    let idx: u8 = tc.draw(gs::integers::<u8>());
    let idx = idx % spec_num_tables();
    format!("t_{prefix}_{idx}")
}

fn gen_value(tc: &TestCase) -> Val {
    let variant: u8 = tc.draw(gs::integers::<u8>());
    let values = SPEC["values"].as_array().unwrap();
    let entry = &values[(variant % spec_num_values()) as usize];
    let id = entry["id"].as_str().unwrap();

    match id {
        "null" => Val::Null,
        "integer" => {
            let r = &entry["random"];
            let min = r["min"].as_i64().unwrap();
            let max = r["max"].as_i64().unwrap();
            let v: i64 = tc.draw(gs::integers::<i64>());
            let range = max - min + 1;
            Val::Int(min + (v.rem_euclid(range)))
        }
        "float" => {
            let r = &entry["random_scaled"];
            let min = r["min"].as_i64().unwrap() as i32;
            let max = r["max"].as_i64().unwrap() as i32;
            let divisor = r["divisor"].as_f64().unwrap();
            let v: i32 = tc.draw(gs::integers::<i32>());
            let range = max - min + 1;
            Val::Float((min + (v.rem_euclid(range))) as f64 / divisor)
        }
        "ascii" => {
            let max_len = entry["random_string"]["max_len"].as_u64().unwrap() as usize;
            let t: String = tc.draw(gs::text());
            Val::Text(t.chars().take(max_len).collect())
        }
        "blob" => {
            let max_len = entry["random_bytes"]["max_len"].as_u64().unwrap() as usize;
            let b: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()));
            Val::Blob(b.into_iter().take(max_len).collect())
        }
        "int_extreme" => {
            let opts = entry["oneof"].as_array().unwrap();
            let pick: u8 = tc.draw(gs::integers::<u8>());
            Val::Int(opts[(pick as usize) % opts.len()].as_i64().unwrap())
        }
        "float_extreme" => {
            let opts = entry["oneof_float"].as_array().unwrap();
            let pick: u8 = tc.draw(gs::integers::<u8>());
            Val::Float(opts[(pick as usize) % opts.len()].as_f64().unwrap())
        }
        "empty_string" => Val::Text(entry["literal"].as_str().unwrap().into()),
        "empty_blob" => Val::Blob(vec![]),
        "large_or_unicode" => {
            let pick: u8 = tc.draw(gs::integers::<u8>());
            if pick % 2 == 0 {
                Val::Blob(vec![0xAB; 256])
            } else {
                let options = spec_unicode_options();
                let idx = (pick / 2) as usize % options.len();
                Val::Text(options[idx].as_str().unwrap().into())
            }
        }
        "negative_zero" => Val::Float(entry["literal_float"].as_f64().unwrap()),
        "zero_blob" => {
            let fb = &entry["fill_bytes"];
            let byte = fb["byte"].as_u64().unwrap() as u8;
            let len = fb["len"].as_u64().unwrap() as usize;
            Val::Blob(vec![byte; len])
        }
        "high_bit_blob" => {
            let fb = &entry["fill_bytes"];
            let byte = fb["byte"].as_u64().unwrap() as u8;
            let len = fb["len"].as_u64().unwrap() as usize;
            Val::Blob(vec![byte; len])
        }
        "large_string" => {
            let rc = &entry["repeat_char"];
            let ch = rc["char"].as_str().unwrap();
            let len = rc["len"].as_u64().unwrap() as usize;
            Val::Text(ch.repeat(len))
        }
        "large_blob" => {
            let fb = &entry["fill_bytes"];
            let byte = fb["byte"].as_u64().unwrap() as u8;
            let len = fb["len"].as_u64().unwrap() as usize;
            Val::Blob(vec![byte; len])
        }
        // literal string values (null_bytes_string, sql_chars_string, etc.)
        _ if entry.get("literal").is_some() => {
            Val::Text(entry["literal"].as_str().unwrap().into())
        }
        _ if entry.get("literal_bytes").is_some() => Val::Blob(vec![]),
        other => panic!("unknown value id in ops.json: {other}"),
    }
}

fn gen_dynamic_cols(tc: &TestCase) -> Vec<(String, String)> {
    let count: u8 = tc.draw(gs::integers::<u8>());
    let count = (count % spec_max_dynamic_cols()) + 1;
    let col_types = spec_col_types();
    (0..count)
        .map(|i| {
            let type_idx: u8 = tc.draw(gs::integers::<u8>());
            let col_type = col_types[type_idx as usize % col_types.len()];
            (format!("c{}", i), col_type.to_string())
        })
        .collect()
}

/// Generate a batch SQL string with literal values (no params).
fn gen_batch_sql(tc: &TestCase, prefix: u32) -> String {
    let tbl: u8 = tc.draw(gs::integers::<u8>());
    let tbl = tbl % spec_num_tables();
    let a: i32 = tc.draw(gs::integers::<i32>());
    let a = a % 1001;
    format!(
        "CREATE TABLE IF NOT EXISTS t_{prefix}_{tbl} (a INTEGER, b TEXT); INSERT INTO t_{prefix}_{tbl} VALUES ({a}, 'batch')"
    )
}

/// Generate values matching a table's column count (default 2 if unknown).
/// Always draws 5 values to keep the hegel draw sequence deterministic, then truncates.
fn gen_values_for_table(tc: &TestCase, table: &str, table_cols: &HashMap<String, usize>) -> Vec<Val> {
    let all: Vec<Val> = (0..5).map(|_| gen_value(tc)).collect();
    let n = table_cols.get(table).copied().unwrap_or(2);
    all.into_iter().take(n).collect()
}

fn gen_error_sql(tc: &TestCase, prefix: u32) -> String {
    let error_sqls = spec_error_sqls();
    let pick: u8 = tc.draw(gs::integers::<u8>());
    let sql = error_sqls[(pick as usize) % error_sqls.len()]
        .as_str()
        .unwrap();
    sql.replace("{prefix}", &prefix.to_string())
}

fn gen_op(tc: &TestCase, table_cols: &mut HashMap<String, usize>, prefix: u32) -> Op {
    let variant: u8 = tc.draw(gs::integers::<u8>());
    let ops = SPEC["ops"].as_array().unwrap();
    let op_spec = &ops[(variant % spec_num_ops()) as usize];
    let id = op_spec["id"].as_str().unwrap();

    match id {
        "create" => {
            let name = table_name(tc, prefix);
            table_cols.insert(name.clone(), 2);
            Op::CreateTable {
                name,
                cols: vec![("a".into(), "INTEGER".into()), ("b".into(), "TEXT".into())],
            }
        }
        "insert" => {
            let table = table_name(tc, prefix);
            let values = gen_values_for_table(tc, &table, table_cols);
            Op::Insert { table, values }
        }
        "select" => Op::Select {
            table: table_name(tc, prefix),
        },
        "select_value" => {
            let v: i32 = tc.draw(gs::integers::<i32>());
            let val = v % 1001;
            Op::SelectValue {
                expr: format!("{val}"),
            }
        }
        "begin" => Op::BeginTx,
        "commit" => Op::CommitTx,
        "rollback" => Op::RollbackTx,
        "invalid" => Op::InvalidSQL {
            sql: op_spec["sql"].as_str().unwrap().into(),
        },
        "param" => Op::Param {
            sql: op_spec["sql"].as_str().unwrap().into(),
            params: vec![gen_value(tc), gen_value(tc)],
        },
        "create_dynamic" => {
            let cols = gen_dynamic_cols(tc);
            let name = table_name(tc, prefix);
            table_cols.insert(name.clone(), cols.len());
            Op::CreateTableDynamic { name, cols }
        }
        "insert_returning" => {
            let table = table_name(tc, prefix);
            let values = gen_values_for_table(tc, &table, table_cols);
            Op::InsertReturning { table, values }
        }
        "delete_returning" => Op::DeleteReturning {
            table: table_name(tc, prefix),
        },
        "update_returning" => Op::UpdateReturning {
            table: table_name(tc, prefix),
            value: gen_value(tc),
        },
        "named_param" => {
            let names = op_spec["named_params"].as_array().unwrap();
            let values: Vec<(String, Val)> = names
                .iter()
                .map(|n| (n.as_str().unwrap().into(), gen_value(tc)))
                .collect();
            Op::NamedParam { values }
        }
        "numbered_param" => Op::NumberedParam {
            values: vec![gen_value(tc), gen_value(tc)],
        },
        "insert_affected" => {
            let table = table_name(tc, prefix);
            let values = gen_values_for_table(tc, &table, table_cols);
            Op::InsertAffected { table, values }
        }
        "delete_affected" => Op::DeleteAffected {
            table: table_name(tc, prefix),
        },
        "update_affected" => Op::UpdateAffected {
            table: table_name(tc, prefix),
            value: gen_value(tc),
        },
        "insert_rowid" => {
            let table = table_name(tc, prefix);
            let values = gen_values_for_table(tc, &table, table_cols);
            Op::InsertRowid { table, values }
        }
        "batch" => Op::Batch {
            sql: gen_batch_sql(tc, prefix),
        },
        "select_limit" => Op::SelectLimit {
            table: table_name(tc, prefix),
        },
        "select_count" => Op::SelectCount {
            table: table_name(tc, prefix),
        },
        "select_expr" => Op::SelectExpr {
            param: gen_value(tc),
        },
        "error_check" => Op::ErrorCheck {
            sql: gen_error_sql(tc, prefix),
        },
        "prepared_reuse" => Op::PreparedReuse {
            params_sets: vec![
                vec![gen_value(tc), gen_value(tc)],
                vec![gen_value(tc), gen_value(tc)],
                vec![gen_value(tc), gen_value(tc)],
            ],
        },
        "create_trigger" => Op::CreateTrigger {
            table: table_name(tc, prefix),
        },
        other => panic!("unknown op id in ops.json: {other}"),
    }
}

fn gen_ops(tc: &TestCase, prefix: u32) -> Vec<Op> {
    let count: u8 = tc.draw(gs::integers::<u8>());
    let count = (count % spec_max_ops()) + 1;
    let mut table_cols = HashMap::new();
    (0..count).map(|_| gen_op(tc, &mut table_cols, prefix)).collect()
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

fn create_table_sql(name: &str, cols: &[(String, String)]) -> String {
    let col_defs: Vec<String> = cols.iter().map(|(n, t)| format!("{n} {t}")).collect();
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        name,
        col_defs.join(", ")
    )
}

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

async fn execute_op_local(conn: &turso::Connection, op: &Op, in_tx: &mut bool) -> OpResult {
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
                Ok(_) => {
                    match conn.query("SELECT last_insert_rowid()", ()).await {
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
                    }
                }
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
            // Create the audit table, create a trigger with BEGIN...END (internal semicolons),
            // fire the trigger via INSERT, then read back the audit log.
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
                    // Fire the trigger
                    let _ = conn
                        .execute(&format!("INSERT INTO {table} VALUES (42, 'trigger_test')"), ())
                        .await;
                    // Read the audit log
                    query_local(conn, &format!("SELECT * FROM {audit} ORDER BY rowid"), vec![])
                        .await
                }
                Err(_) => fail_result(),
            }
        }
    }
}

async fn execute_op_remote(
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
                Ok(_) => {
                    match conn.query("SELECT last_insert_rowid()", ()).await {
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
                    }
                }
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
                        .execute(&format!("INSERT INTO {table} VALUES (42, 'trigger_test')"), ())
                        .await;
                    query_remote(conn, &format!("SELECT * FROM {audit} ORDER BY rowid"), vec![])
                        .await
                }
                Err(_) => fail_result(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Connection helpers
// ---------------------------------------------------------------------------

fn server_url() -> String {
    std::env::var("TURSO_DATABASE_URL").unwrap_or_else(|_| "http://localhost:8080".into())
}

fn auth_token() -> Option<String> {
    std::env::var("TURSO_AUTH_TOKEN").ok()
}

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

        let create_sql =
            format!("CREATE TABLE IF NOT EXISTS {table} (a INTEGER, b TEXT)");
        let insert_sql = format!("INSERT INTO {table} VALUES (?, 'txn_ddl')");
        let select_sql = format!("SELECT a FROM {table}");

        // Local: BEGIN → CREATE → INSERT → SELECT → COMMIT
        local.execute("BEGIN", ()).await.expect("local: BEGIN failed");
        local.execute(&create_sql, ()).await.expect("local: CREATE TABLE failed");
        local
            .execute(&insert_sql, vec![turso::Value::Integer(val)])
            .await
            .expect("local: INSERT failed");
        let local_inner = query_local(&local, &select_sql, vec![]).await;
        assert!(
            local_inner.success,
            "local: SELECT inside txn failed after CREATE+INSERT"
        );
        local.execute("COMMIT", ()).await.expect("local: COMMIT failed");

        // Remote: BEGIN → CREATE → INSERT → SELECT → COMMIT
        remote.execute("BEGIN", ()).await.expect("remote: BEGIN failed");
        remote.execute(&create_sql, ()).await.expect("remote: CREATE TABLE failed");
        remote
            .execute(&insert_sql, vec![turso_serverless::Value::Integer(val)])
            .await
            .expect("remote: INSERT failed");
        let remote_inner = query_remote(&remote, &select_sql, vec![]).await;
        assert!(
            remote_inner.success,
            "remote: SELECT inside txn failed after CREATE+INSERT"
        );
        remote.execute("COMMIT", ()).await.expect("remote: COMMIT failed");
    });
}

// ---------------------------------------------------------------------------
// DDL + prepare() in transactions: prepare() calls describe which must see
// tables created earlier in the same transaction. This catches bugs where
// describe opens a new session without transaction context (issue #6562).
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

        let create_sql =
            format!("CREATE TABLE IF NOT EXISTS {table} (a INTEGER, b TEXT)");
        let insert_sql = format!("INSERT INTO {table} VALUES (?, 'txn_ddl')");
        let select_sql = format!("SELECT a FROM {table}");

        // Local: BEGIN → CREATE → prepare(INSERT) → execute → SELECT → COMMIT
        local.execute("BEGIN", ()).await.expect("local: BEGIN failed");
        local.execute(&create_sql, ()).await.expect("local: CREATE TABLE failed");
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
        local.execute("COMMIT", ()).await.expect("local: COMMIT failed");

        // Remote: BEGIN → CREATE → prepare(INSERT) → execute → SELECT → COMMIT
        remote.execute("BEGIN", ()).await.expect("remote: BEGIN failed");
        remote.execute(&create_sql, ()).await.expect("remote: CREATE TABLE failed");
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
        remote.execute("COMMIT", ()).await.expect("remote: COMMIT failed");
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
            .unwrap_or_else(|e| {
                panic!(
                    "SELECT 1 failed after error SQL {:?}: {e}",
                    error_sql
                )
            });
        let row = rows.next().await.expect("should have a row").expect("row should be Ok");
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
