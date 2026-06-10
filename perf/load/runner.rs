use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use turso::Connection;

use crate::generators::{GenState, Generator, Val};
use crate::stats::Stats;
use crate::workload::{OperationSpec, RowsExpect, StatementSpec, Workload};

pub struct CompiledStatement {
    pub sql: String,
    /// Placeholder names appearing in the SQL, in order of first appearance.
    pub params: Vec<String>,
    pub bind: Vec<(String, Generator)>,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum OpKind {
    Sql,
    Transaction,
    Batch,
}

pub struct CompiledOp {
    pub name: String,
    pub cumulative_weight: f64,
    pub kind: OpKind,
    pub concurrent: bool,
    pub bind: Vec<(String, Generator)>,
    pub statements: Vec<CompiledStatement>,
    /// (pool key, bind name) the generated key value is registered under.
    pub produces: Option<(String, String)>,
    pub requires: Option<(String, usize)>,
    pub expect_rows: Option<(i64, i64)>,
    pub expect_errors: Vec<String>,
}

/// Extract `:name` placeholders from a SQL statement, skipping quoted strings.
fn extract_params(sql: &str) -> Vec<String> {
    let mut params = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'\'' | b'"') => {
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    i += 1;
                }
                i += 1;
            }
            b':' => {
                let start = i + 1;
                let mut end = start;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
                {
                    end += 1;
                }
                if end > start {
                    let name = sql[start..end].to_string();
                    if !params.contains(&name) {
                        params.push(name);
                    }
                }
                i = end;
            }
            _ => i += 1,
        }
    }
    params
}

fn compile_binds(binds: &serde_yaml::Mapping, op_name: &str) -> Result<Vec<(String, Generator)>> {
    let mut out = Vec::new();
    for (name, spec) in binds {
        let name = name
            .as_str()
            .with_context(|| format!("operation {op_name}: bind name must be a string"))?;
        let gen =
            Generator::parse(spec).with_context(|| format!("operation {op_name}: bind {name}"))?;
        out.push((name.to_string(), gen));
    }
    Ok(out)
}

fn parse_expect_rows(rows: &RowsExpect) -> Result<(i64, i64)> {
    match rows {
        RowsExpect::Exact(n) => Ok((*n, *n)),
        RowsExpect::Range(s) => {
            let (min, max) = s
                .split_once("..")
                .with_context(|| format!("invalid expect.rows: \"{s}\" (use a number or n..m)"))?;
            Ok((min.trim().parse()?, max.trim().parse()?))
        }
    }
}

fn compile_op(spec: &OperationSpec, cumulative_weight: f64) -> Result<CompiledOp> {
    let (kind, sources): (OpKind, Vec<&StatementSpec>) = if spec.sql.is_some() {
        (OpKind::Sql, Vec::new())
    } else if let Some(transaction) = &spec.transaction {
        (OpKind::Transaction, transaction.iter().collect())
    } else {
        (OpKind::Batch, spec.batch.iter().flatten().collect())
    };

    let bind = compile_binds(&spec.bind, &spec.name)?;
    let mut statements = Vec::new();
    if let Some(sql) = &spec.sql {
        statements.push(CompiledStatement {
            sql: sql.clone(),
            params: extract_params(sql),
            bind: Vec::new(),
        });
    }
    for stmt in sources {
        statements.push(CompiledStatement {
            sql: stmt.sql.clone(),
            params: extract_params(&stmt.sql),
            bind: compile_binds(&stmt.bind, &spec.name)?,
        });
    }

    // Every placeholder must have a generator, at the operation or statement level.
    for stmt in &statements {
        for param in &stmt.params {
            if !bind.iter().any(|(n, _)| n == param) && !stmt.bind.iter().any(|(n, _)| n == param) {
                bail!("operation {}: no bind for placeholder :{param}", spec.name);
            }
        }
    }

    let produces = match &spec.produces {
        Some(produces) => {
            let (_, bind_name) = produces.split_once('.').with_context(|| {
                format!("operation {}: produces must be table.column", spec.name)
            })?;
            Some((produces.clone(), bind_name.to_string()))
        }
        None => None,
    };

    Ok(CompiledOp {
        name: spec.name.clone(),
        cumulative_weight,
        kind,
        concurrent: spec.concurrent,
        bind,
        statements,
        produces,
        requires: spec.requires.as_ref().map(|r| (r.pool.clone(), r.min)),
        expect_rows: spec
            .expect
            .as_ref()
            .and_then(|e| e.rows.as_ref())
            .map(parse_expect_rows)
            .transpose()
            .with_context(|| format!("operation {}", spec.name))?,
        expect_errors: spec
            .expect
            .iter()
            .flat_map(|e| &e.errors)
            .map(|e| match e {
                serde_yaml::Value::String(s) => s.clone(),
                other => serde_yaml::to_string(other)
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
            })
            .collect(),
    })
}

pub fn compile_ops(workload: &Workload) -> Result<(Vec<CompiledOp>, f64)> {
    let mut ops = Vec::new();
    let mut cumulative = 0.0;
    for spec in &workload.operations {
        cumulative += spec.weight;
        ops.push(compile_op(spec, cumulative)?);
    }
    if ops.is_empty() {
        bail!("workload \"{}\" has no operations to bench", workload.name);
    }
    Ok((ops, cumulative))
}

fn error_is_expected(text: &str, expect: &[String]) -> bool {
    expect.iter().any(|e| {
        text.contains(e.as_str())
            || (e.eq_ignore_ascii_case("SQLITE_BUSY") && text.to_lowercase().contains("busy"))
    })
}

enum Iteration {
    Skip,
    Run {
        /// Operation-level binds (used for `produces` registration).
        op_binds: HashMap<String, Val>,
        /// Full bind set per statement (op-level merged with statement-level).
        per_statement: Vec<HashMap<String, Val>>,
    },
}

/// Pick an operation and evaluate every bind for it under one lock, so a skip
/// never happens halfway through a transaction.
fn prepare_iteration(
    ops: &[CompiledOp],
    total_weight: f64,
    gen: &mut GenState,
) -> (usize, Iteration) {
    let r = gen.rng.f64() * total_weight;
    let idx = ops
        .iter()
        .position(|op| r < op.cumulative_weight)
        .unwrap_or(ops.len() - 1);
    let op = &ops[idx];

    if let Some((pool, min)) = &op.requires {
        if gen.pools.size(pool) < *min {
            return (idx, Iteration::Skip);
        }
    }

    let mut op_binds = HashMap::new();
    for (name, generator) in &op.bind {
        let Some(value) = gen.eval(generator, &format!("op:{}:{name}", op.name)) else {
            return (idx, Iteration::Skip);
        };
        op_binds.insert(name.clone(), value);
    }

    let mut per_statement = Vec::with_capacity(op.statements.len());
    for stmt in &op.statements {
        let mut binds = op_binds.clone();
        for (name, generator) in &stmt.bind {
            let Some(value) = gen.eval(generator, &format!("op:{}:{name}", op.name)) else {
                return (idx, Iteration::Skip);
            };
            binds.insert(name.clone(), value);
        }
        per_statement.push(binds);
    }

    (
        idx,
        Iteration::Run {
            op_binds,
            per_statement,
        },
    )
}

async fn execute_statement(
    conn: &Connection,
    stmt: &CompiledStatement,
    binds: &HashMap<String, Val>,
) -> turso::Result<usize> {
    let mut prepared = conn.prepare_cached(&stmt.sql).await?;
    let params: Vec<(String, turso::Value)> = stmt
        .params
        .iter()
        .map(|p| (format!(":{p}"), (&binds[p]).into()))
        .collect();
    let mut rows = if params.is_empty() {
        prepared.query(()).await?
    } else {
        prepared.query(params).await?
    };
    let mut n = 0;
    while rows.next().await?.is_some() {
        n += 1;
    }
    Ok(n)
}

static CONCURRENT_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);

async fn run_op(
    conn: &Connection,
    op: &CompiledOp,
    per_statement: &[HashMap<String, Val>],
    concurrent_ok: &AtomicBool,
) -> turso::Result<usize> {
    let mut last_rows = 0;
    if op.kind == OpKind::Transaction {
        let concurrent = op.concurrent && concurrent_ok.load(Ordering::Relaxed);
        let begin = if concurrent {
            "BEGIN CONCURRENT"
        } else {
            "BEGIN"
        };
        if let Err(err) = conn.execute(begin, ()).await {
            if concurrent {
                concurrent_ok.store(false, Ordering::Relaxed);
                if !CONCURRENT_FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                    eprintln!(
                        "warning: BEGIN CONCURRENT rejected; falling back to plain BEGIN (run with --mvcc to enable it)"
                    );
                }
            }
            return Err(err);
        }
        for (stmt, binds) in op.statements.iter().zip(per_statement) {
            match execute_statement(conn, stmt, binds).await {
                Ok(n) => last_rows = n,
                Err(err) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(err);
                }
            }
        }
        if let Err(err) = conn.execute("COMMIT", ()).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(err);
        }
    } else {
        // A single statement, or a batch run back to back without
        // transaction semantics.
        for (stmt, binds) in op.statements.iter().zip(per_statement) {
            last_rows = execute_statement(conn, stmt, binds).await?;
        }
    }
    Ok(last_rows)
}

pub struct WorkerCfg {
    /// Target ops/sec per connection (open-loop); None = saturate.
    pub rate: Option<f64>,
    pub verbose: bool,
}

#[allow(clippy::too_many_arguments)]
pub async fn worker(
    conn: Connection,
    ops: Arc<Vec<CompiledOp>>,
    total_weight: f64,
    gen: Arc<Mutex<GenState>>,
    concurrent_ok: Arc<AtomicBool>,
    cfg: Arc<WorkerCfg>,
    measure_from: Instant,
    deadline: Instant,
) -> Stats {
    let mut stats = Stats::new();
    let interval = cfg.rate.map(|r| Duration::from_secs_f64(1.0 / r));

    while Instant::now() < deadline {
        let iter_start = Instant::now();
        let (op_idx, iteration) = {
            let mut gen = gen.lock().unwrap();
            prepare_iteration(&ops, total_weight, &mut gen)
        };
        let op = &ops[op_idx];
        let measured = iter_start >= measure_from;
        let at_sec = if measured {
            (iter_start - measure_from).as_secs_f64()
        } else {
            0.0
        };

        match iteration {
            Iteration::Skip => {
                if measured {
                    stats.record_skip(&op.name);
                }
                // An all-skip loop (e.g. every pool empty) must not spin.
                tokio::task::yield_now().await;
            }
            Iteration::Run {
                op_binds,
                per_statement,
            } => {
                let result = run_op(&conn, op, &per_statement, &concurrent_ok).await;
                let result =
                    result
                        .map_err(|e| e.to_string())
                        .and_then(|rows| match op.expect_rows {
                            Some((min, max)) if (rows as i64) < min || (rows as i64) > max => Err(
                                format!("{}: expected {min}..{max} rows, got {rows}", op.name),
                            ),
                            _ => Ok(rows),
                        });
                match result {
                    Ok(_) => {
                        if let Some((pool, bind_name)) = &op.produces {
                            if let Some(value) = op_binds.get(bind_name) {
                                gen.lock().unwrap().pools.add(pool, value.clone());
                            }
                        }
                        if measured {
                            stats.record_ok(
                                &op.name,
                                iter_start.elapsed().as_secs_f64() * 1000.0,
                                at_sec,
                            );
                        }
                    }
                    Err(text) => {
                        if error_is_expected(&text, &op.expect_errors) {
                            if measured {
                                stats.record_ok(
                                    &op.name,
                                    iter_start.elapsed().as_secs_f64() * 1000.0,
                                    at_sec,
                                );
                            }
                        } else {
                            if measured {
                                stats.record_error(&op.name);
                            }
                            if cfg.verbose {
                                eprintln!("[{}] {text}", op.name);
                            }
                        }
                    }
                }
            }
        }

        if let Some(interval) = interval {
            let elapsed = iter_start.elapsed();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_extraction() {
        assert_eq!(
            extract_params("UPDATE t SET a = :ts, b = :id WHERE id = :id"),
            vec!["ts".to_string(), "id".to_string()]
        );
        assert_eq!(
            extract_params("SELECT ':not_a_param', \":also_not\" FROM t WHERE x = :real"),
            vec!["real".to_string()]
        );
        assert!(extract_params("SELECT 1").is_empty());
    }

    #[test]
    fn expect_rows_parsing() {
        assert_eq!(parse_expect_rows(&RowsExpect::Exact(3)).unwrap(), (3, 3));
        assert_eq!(
            parse_expect_rows(&RowsExpect::Range("0..100".to_string())).unwrap(),
            (0, 100)
        );
        assert!(parse_expect_rows(&RowsExpect::Range("nope".to_string())).is_err());
    }

    #[test]
    fn busy_errors_match() {
        assert!(error_is_expected(
            "database is Busy",
            &["SQLITE_BUSY".to_string()]
        ));
        assert!(!error_is_expected(
            "UNIQUE constraint failed",
            &["SQLITE_BUSY".to_string()]
        ));
    }
}
