//! MVCC Concurrent Transaction REPL
//!
//! Utility for interactive testing of concurrent MVCC transactions, with
//! built-in fault-injection driving the existing yield/failure injection
//! points in `core/mvcc/`.
//!
//! # Overview
//!
//! Drives multiple in-process MVCC connections from a single REPL. Connections
//! are created lazily on first use.
//!
//! # Invocation
//!
//! ```bash
//! cargo run --bin tursodb --features mvcc_repl -- --mvcc [path]
//! ```
//!
//! # SQL syntax
//!
//! ```text
//! mvcc> conn1 CREATE TABLE t(x INT)
//! mvcc> conn1 BEGIN CONCURRENT
//! mvcc> conn2 INSERT INTO t VALUES (1)
//! ```
//!
//! # Fault-injection syntax
//!
//! ```text
//! mvcc> .fault list
//!       # print every yield point the engine exposes
//!
//! mvcc> .fault conn1 fail commit.validation [busy|internal|txterm|wwconflict|schemaconflict]
//!       # next time conn1's commit reaches `commit.validation`, return that error
//!       # (single-shot: the fault is consumed on first hit; default error is `internal`)
//!
//! mvcc> .fault conn1 yield cursor.next_start
//!       # next time conn1's cursor reaches `cursor.next_start`, synthesize an I/O yield
//!
//! mvcc> .fault conn1 status
//!       # show pending faults on conn1
//!
//! mvcc> .fault conn1 clear
//!       # drop all pending faults on conn1
//! ```
use anyhow::{anyhow, Context as _};
use rustyline::DefaultEditor;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use turso_core::mvcc::yield_points::{
    all_yield_points, yield_point_by_name, FailureInjector, YieldInjector, YieldPoint,
};
use turso_core::{Connection, Database, DatabaseOpts, LimboError, OpenFlags, Value};

/// Per-connection REPL state. The injectors are installed lazily on the first
/// `.fault` referencing this connection so that connections used only for SQL
/// don't pay the cost of a no-op injector.
struct ConnState {
    conn: Arc<Connection>,
    failure: Arc<ReplFailureInjector>,
    yields: Arc<ReplYieldInjector>,
    injectors_installed: bool,
}

impl ConnState {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            failure: Arc::new(ReplFailureInjector::default()),
            yields: Arc::new(ReplYieldInjector::default()),
            injectors_installed: false,
        }
    }

    /// Install both injectors on the connection on first use. Connection's
    /// setters assert the slot was empty, so this must only run once per
    /// connection lifetime.
    fn ensure_injectors_installed(&mut self) {
        if self.injectors_installed {
            return;
        }
        self.conn
            .set_failure_injector(Some(self.failure.clone() as Arc<dyn FailureInjector>));
        self.conn
            .set_yield_injector(Some(self.yields.clone() as Arc<dyn YieldInjector>));
        self.injectors_installed = true;
    }
}

#[derive(Default, Debug)]
struct ReplFailureInjector {
    /// Pending one-shot failures keyed by yield point. The trait-method
    /// signature consumes the entry on first matching hit.
    pending: Mutex<HashMap<YieldPoint, FaultErrorKind>>,
}

impl FailureInjector for ReplFailureInjector {
    fn should_fail(
        &self,
        _instance_id: u64,
        _selection_key: u64,
        point: YieldPoint,
    ) -> Option<LimboError> {
        self.pending
            .lock()
            .unwrap()
            .remove(&point)
            .map(|kind| kind.into_err())
    }
}

#[derive(Default, Debug)]
struct ReplYieldInjector {
    pending: Mutex<HashSet<YieldPoint>>,
}

impl YieldInjector for ReplYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        self.pending.lock().unwrap().remove(&point)
    }
}

#[derive(Clone, Copy, Debug)]
enum FaultErrorKind {
    Internal,
    Busy,
    TxTerminated,
    WriteWriteConflict,
    SchemaConflict,
}

impl FaultErrorKind {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "internal" => Some(Self::Internal),
            "busy" => Some(Self::Busy),
            "txterm" | "tx_terminated" | "txterminated" => Some(Self::TxTerminated),
            "wwconflict" | "ww_conflict" | "writewriteconflict" => Some(Self::WriteWriteConflict),
            "schemaconflict" | "schema_conflict" => Some(Self::SchemaConflict),
            _ => None,
        }
    }

    fn into_err(self) -> LimboError {
        match self {
            Self::Internal => LimboError::InternalError("fault-injected".to_string()),
            Self::Busy => LimboError::Busy,
            Self::TxTerminated => LimboError::TxTerminated,
            Self::WriteWriteConflict => LimboError::WriteWriteConflict,
            Self::SchemaConflict => LimboError::SchemaConflict,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Internal => "internal",
            Self::Busy => "busy",
            Self::TxTerminated => "txterm",
            Self::WriteWriteConflict => "wwconflict",
            Self::SchemaConflict => "schemaconflict",
        }
    }
}

pub fn run(path: &str) -> anyhow::Result<()> {
    let interactive_stdin = std::io::IsTerminal::is_terminal(&std::io::stdin());
    if interactive_stdin {
        println!("MVCC REPL");
        println!("  connN SQL          run SQL on connection N (connections auto-created)");
        println!("  .fault ...         install/inspect fault injections; `.fault help`");
        println!("  .quit              exit");
        println!();
    }

    let db = open_mvcc_db(path)?;
    let mut connections: HashMap<String, ConnState> = HashMap::new();
    let mut rl = DefaultEditor::new()?;

    loop {
        match rl.readline("mvcc> ") {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if line == ".quit" {
                    break;
                }

                if let Some(rest) = line.strip_prefix(".fault") {
                    if let Err(e) = handle_fault_command(rest.trim(), &mut connections, &db) {
                        eprintln!("fault: {e}");
                    }
                    continue;
                }

                if let Some((conn_name, sql)) = parse_conn_input(line) {
                    let state = connections.entry(conn_name.clone()).or_insert_with(|| {
                        ConnState::new(db.connect().expect("failed to create connection"))
                    });

                    match execute_and_display(&state.conn, sql, &conn_name) {
                        Ok(()) => {}
                        Err(e) => {
                            eprintln!("[{conn_name}] ERROR: {e}");
                        }
                    }
                } else {
                    eprintln!("Error: format is `connN SQL` or `.fault ...`");
                }
            }
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!();
                continue;
            }
            Err(e) => anyhow::bail!(e),
        }
    }

    Ok(())
}

fn handle_fault_command(
    args: &str,
    connections: &mut HashMap<String, ConnState>,
    db: &Arc<Database>,
) -> anyhow::Result<()> {
    if args.is_empty() || args == "help" {
        print_fault_help();
        return Ok(());
    }

    if args == "list" {
        print_fault_list();
        return Ok(());
    }

    let mut tokens = args.split_whitespace();
    let first = tokens
        .next()
        .ok_or_else(|| anyhow!("missing connection name"))?;

    if !is_conn_name(first) {
        return Err(anyhow!(
            "first arg must be a connection name (e.g. `conn1`) or `list`/`help`, got `{first}`"
        ));
    }
    let conn_name = first.to_string();
    let verb = tokens
        .next()
        .ok_or_else(|| anyhow!("missing verb (fail|yield|status|clear)"))?;

    let state = connections
        .entry(conn_name.clone())
        .or_insert_with(|| ConnState::new(db.connect().expect("failed to create connection")));

    match verb {
        "fail" => {
            let point_name = tokens
                .next()
                .ok_or_else(|| anyhow!("usage: .fault {conn_name} fail <POINT> [ERROR_KIND]"))?;
            let point = yield_point_by_name(point_name)
                .ok_or_else(|| anyhow!("unknown yield point `{point_name}` (try `.fault list`)"))?;
            let kind = match tokens.next() {
                Some(s) => FaultErrorKind::parse(s)
                    .ok_or_else(|| anyhow!("unknown error kind `{s}` (try `.fault help`)"))?,
                None => FaultErrorKind::Internal,
            };
            state.ensure_injectors_installed();
            state.failure.pending.lock().unwrap().insert(point, kind);
            println!(
                "[{conn_name}] armed: fail at `{point_name}` -> {}",
                kind.label()
            );
        }
        "yield" => {
            let point_name = tokens
                .next()
                .ok_or_else(|| anyhow!("usage: .fault {conn_name} yield <POINT>"))?;
            let point = yield_point_by_name(point_name)
                .ok_or_else(|| anyhow!("unknown yield point `{point_name}` (try `.fault list`)"))?;
            state.ensure_injectors_installed();
            state.yields.pending.lock().unwrap().insert(point);
            println!("[{conn_name}] armed: yield at `{point_name}`");
        }
        "clear" => {
            // Drop pending faults but leave the injectors installed; the
            // Connection's setters assert-fail on double-install/uninstall,
            // and we want subsequent `.fault` calls on this conn to work.
            state.failure.pending.lock().unwrap().clear();
            state.yields.pending.lock().unwrap().clear();
            println!("[{conn_name}] all pending faults cleared");
        }
        "status" => {
            let fails = state.failure.pending.lock().unwrap();
            let yields = state.yields.pending.lock().unwrap();
            if fails.is_empty() && yields.is_empty() {
                println!("[{conn_name}] no pending faults");
            } else {
                for (point, kind) in fails.iter() {
                    println!(
                        "[{conn_name}] pending: fail at {} -> {}",
                        name_for_point(*point).unwrap_or("?"),
                        kind.label()
                    );
                }
                for point in yields.iter() {
                    println!(
                        "[{conn_name}] pending: yield at {}",
                        name_for_point(*point).unwrap_or("?")
                    );
                }
            }
        }
        other => {
            return Err(anyhow!(
                "unknown verb `{other}`; expected fail|yield|clear|status"
            ));
        }
    }
    Ok(())
}

fn print_fault_help() {
    println!("Fault injection commands:");
    println!("  .fault list");
    println!("      list every named yield point in the engine");
    println!("  .fault conn<N> fail <POINT> [busy|internal|txterm|wwconflict|schemaconflict]");
    println!("      next time conn<N> reaches <POINT>, return that error (default: internal)");
    println!("  .fault conn<N> yield <POINT>");
    println!("      next time conn<N> reaches <POINT>, synthesize an I/O yield");
    println!("  .fault conn<N> status");
    println!("      show pending faults armed on conn<N>");
    println!("  .fault conn<N> clear");
    println!("      drop all pending faults on conn<N>");
}

fn print_fault_list() {
    let points = all_yield_points();
    println!("Available yield points ({} total):", points.len());
    let mut prefix = "";
    for (name, _) in points {
        let new_prefix = name.split_once('.').map(|(p, _)| p).unwrap_or("");
        if new_prefix != prefix {
            if !prefix.is_empty() {
                println!();
            }
            println!("  [{new_prefix}]");
            prefix = new_prefix;
        }
        println!("    {name}");
    }
}

fn name_for_point(point: YieldPoint) -> Option<&'static str> {
    all_yield_points()
        .iter()
        .find_map(|(name, p)| (*p == point).then_some(*name))
}

fn is_conn_name(s: &str) -> bool {
    s.strip_prefix("conn")
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()))
}

fn open_mvcc_db(path: &str) -> anyhow::Result<Arc<Database>> {
    let (_, db) = Database::open_new::<&str>(
        path,
        None,
        OpenFlags::default(),
        DatabaseOpts::default(),
        None,
    )
    .context("failed to open database")?;

    let boot = db.connect()?;
    boot.execute("PRAGMA journal_mode = 'mvcc'")?;
    boot.close()?;

    Ok(db)
}

fn parse_conn_input(line: &str) -> Option<(String, &str)> {
    let (first, rest) = line.split_once(char::is_whitespace)?;
    if !is_conn_name(first) {
        return None;
    }
    Some((first.to_string(), rest.trim()))
}

fn execute_and_display(conn: &Arc<Connection>, sql: &str, conn_name: &str) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(sql).map_err(|e| anyhow!("{}", e))?;

    match stmt.run_collect_rows() {
        Ok(rows) => {
            if rows.is_empty() {
                println!("[{conn_name}] OK");
            } else {
                for row in rows {
                    let formatted: Vec<String> = row.iter().map(fmt_value).collect();
                    println!("[{conn_name}] {}", formatted.join(" | "));
                }
            }
            Ok(())
        }
        Err(LimboError::WriteWriteConflict) => {
            Err(anyhow!("write-write conflict (transaction rolled back)"))
        }
        Err(e) => Err(anyhow!("{e}")),
    }
}

fn fmt_value(v: &Value) -> String {
    use turso_core::Numeric;
    match v {
        Value::Null => "NULL".to_string(),
        Value::Numeric(Numeric::Integer(i)) => i.to_string(),
        Value::Numeric(Numeric::Float(f)) => {
            let fval: f64 = (*f).into();
            // Format floats without trailing zeros for cleaner display
            if fval.fract() == 0.0 && fval.abs() < 1e10 {
                format!("{fval:.1}")
            } else {
                format!("{fval}")
            }
        }
        Value::Text(s) => s.to_string(),
        Value::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}
