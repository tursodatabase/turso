// Shared test infrastructure for hegel parity and protocol tests.
//
// Contains operation vocabulary, value types, spec loading, and generators
// that are reused across multiple test files.

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

pub fn spec_num_tables() -> u8 {
    SPEC["constants"]["num_tables"].as_u64().unwrap() as u8
}
pub fn spec_max_ops() -> u8 {
    SPEC["constants"]["max_ops_per_case"].as_u64().unwrap() as u8
}
pub fn spec_max_dynamic_cols() -> u8 {
    SPEC["constants"]["max_dynamic_cols"].as_u64().unwrap() as u8
}
pub fn spec_col_types() -> Vec<&'static str> {
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
pub fn spec_num_values() -> u8 {
    SPEC["values"].as_array().unwrap().len() as u8
}
pub fn spec_num_ops() -> u8 {
    SPEC["ops"].as_array().unwrap().len() as u8
}
pub fn spec_error_sqls() -> &'static Vec<JsonValue> {
    // SAFETY: SPEC is 'static LazyLock
    unsafe {
        std::mem::transmute::<&Vec<JsonValue>, &'static Vec<JsonValue>>(
            SPEC["constants"]["error_sqls"].as_array().unwrap(),
        )
    }
}
pub fn spec_unicode_options() -> &'static Vec<JsonValue> {
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
pub enum Op {
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
    CreateTrigger {
        table: String,
    },
    /// Structured transaction workflow: BEGIN → inner ops → COMMIT/ROLLBACK.
    TransactionWorkflow {
        inner_ops: Vec<Op>,
        commit: bool,
    },
    /// Error recovery in transaction: BEGIN → good → bad SQL → recovery → ROLLBACK.
    ErrorInTransaction {
        good_op: Box<Op>,
        bad_sql: String,
        recovery_op: Box<Op>,
    },
}

#[derive(Debug, Clone)]
pub enum Val {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

pub fn table_name(tc: &TestCase, prefix: u32) -> String {
    let idx: u8 = tc.draw(gs::integers::<u8>());
    let idx = idx % spec_num_tables();
    format!("t_{prefix}_{idx}")
}

pub fn gen_value(tc: &TestCase) -> Val {
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
        _ if entry.get("literal").is_some() => Val::Text(entry["literal"].as_str().unwrap().into()),
        _ if entry.get("literal_bytes").is_some() => Val::Blob(vec![]),
        other => panic!("unknown value id in ops.json: {other}"),
    }
}

pub fn gen_dynamic_cols(tc: &TestCase) -> Vec<(String, String)> {
    let count: u8 = tc.draw(gs::integers::<u8>());
    let count = (count % spec_max_dynamic_cols()) + 1;
    let col_types = spec_col_types();
    (0..count)
        .map(|i| {
            let type_idx: u8 = tc.draw(gs::integers::<u8>());
            let col_type = col_types[type_idx as usize % col_types.len()];
            (format!("c{i}"), col_type.to_string())
        })
        .collect()
}

/// Generate a batch SQL string with literal values (no params).
pub fn gen_batch_sql(tc: &TestCase, prefix: u32) -> String {
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
pub fn gen_values_for_table(
    tc: &TestCase,
    table: &str,
    table_cols: &HashMap<String, usize>,
) -> Vec<Val> {
    let all: Vec<Val> = (0..5).map(|_| gen_value(tc)).collect();
    let n = table_cols.get(table).copied().unwrap_or(2);
    all.into_iter().take(n).collect()
}

pub fn gen_error_sql(tc: &TestCase, prefix: u32) -> String {
    let error_sqls = spec_error_sqls();
    let pick: u8 = tc.draw(gs::integers::<u8>());
    let sql = error_sqls[(pick as usize) % error_sqls.len()]
        .as_str()
        .unwrap();
    sql.replace("{prefix}", &prefix.to_string())
}

pub fn gen_op(tc: &TestCase, table_cols: &mut HashMap<String, usize>, prefix: u32) -> Op {
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
        "transaction_workflow" => gen_transaction_workflow(tc, table_cols, prefix),
        "error_in_transaction" => gen_error_in_transaction(tc, table_cols, prefix),
        other => panic!("unknown op id in ops.json: {other}"),
    }
}

pub fn gen_ops(tc: &TestCase, prefix: u32) -> Vec<Op> {
    let count: u8 = tc.draw(gs::integers::<u8>());
    let count = (count % spec_max_ops()) + 1;
    let mut table_cols = HashMap::new();
    (0..count)
        .map(|_| gen_op(tc, &mut table_cols, prefix))
        .collect()
}

// ---------------------------------------------------------------------------
// Structured transaction workflow generators
// ---------------------------------------------------------------------------

/// Generate a DML/query op suitable for inside a transaction (no BEGIN/COMMIT/ROLLBACK).
fn gen_inner_op(tc: &TestCase, table_cols: &mut HashMap<String, usize>, prefix: u32) -> Op {
    // Draw from a restricted set: only DML and query ops
    let dml_ops = [
        "create",
        "insert",
        "select",
        "select_value",
        "insert_returning",
        "delete_returning",
        "update_returning",
        "select_limit",
        "select_count",
        "select_expr",
        "insert_affected",
        "delete_affected",
        "update_affected",
    ];
    let pick: u8 = tc.draw(gs::integers::<u8>());
    let id = dml_ops[(pick as usize) % dml_ops.len()];

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
        "select_limit" => Op::SelectLimit {
            table: table_name(tc, prefix),
        },
        "select_count" => Op::SelectCount {
            table: table_name(tc, prefix),
        },
        "select_expr" => Op::SelectExpr {
            param: gen_value(tc),
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
        _ => unreachable!(),
    }
}

fn gen_transaction_workflow(
    tc: &TestCase,
    table_cols: &mut HashMap<String, usize>,
    prefix: u32,
) -> Op {
    let count: u8 = tc.draw(gs::integers::<u8>());
    let count = ((count % 5) + 1) as usize;
    let inner_ops: Vec<Op> = (0..count)
        .map(|_| gen_inner_op(tc, table_cols, prefix))
        .collect();
    let commit_byte: u8 = tc.draw(gs::integers::<u8>());
    Op::TransactionWorkflow {
        inner_ops,
        commit: commit_byte % 2 == 0,
    }
}

fn gen_error_in_transaction(
    tc: &TestCase,
    table_cols: &mut HashMap<String, usize>,
    prefix: u32,
) -> Op {
    let good_op = gen_inner_op(tc, table_cols, prefix);
    let bad_sql = gen_error_sql(tc, prefix);
    let recovery_op = gen_inner_op(tc, table_cols, prefix);
    Op::ErrorInTransaction {
        good_op: Box::new(good_op),
        bad_sql,
        recovery_op: Box::new(recovery_op),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn create_table_sql(name: &str, cols: &[(String, String)]) -> String {
    let col_defs: Vec<String> = cols.iter().map(|(n, t)| format!("{n} {t}")).collect();
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        name,
        col_defs.join(", ")
    )
}

pub fn server_url() -> String {
    std::env::var("TURSO_DATABASE_URL").unwrap_or_else(|_| "http://localhost:8080".into())
}

pub fn auth_token() -> Option<String> {
    std::env::var("TURSO_AUTH_TOKEN").ok()
}
