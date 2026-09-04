// Runs a `.sql` file through turso_core directly so we can reproduce and
// observe hangs/errors without any network or serverless-client layer.
//
// Run: cargo run --release -- <path-to-repro.sql> [options]
//
// Designed to mirror stress-ts/repro.ts: loads the SQL, splits it into
// statements (handling CREATE TRIGGER BEGIN..END blocks that span multiple
// semicolons), executes each, prints per-statement timing, and runs a pair
// of verification queries at the end.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::{Connection, Database, DatabaseOpts, MemoryIO, OpenFlags, StepResult, Value, IO};

#[derive(Debug)]
struct Args {
    sql_path: PathBuf,
    db_path: String,
    timeout: Duration,
    keep_going: bool,
    reset: bool,
    skip_verify: bool,
    generated_columns: bool,
    views: bool,
    attach: bool,
    mvcc: bool,
}

fn parse_args() -> Args {
    let mut sql_path: Option<PathBuf> = None;
    let mut db_path = ":memory:".to_string();
    let mut timeout_secs: u64 = 30;
    let mut keep_going = false;
    let mut reset = false;
    let mut skip_verify = false;
    let mut generated_columns = true;
    let mut views = false;
    let mut attach = false;
    let mut mvcc = false;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--db" => {
                db_path = it.next().expect("--db requires a path");
            }
            "-t" | "--timeout" => {
                timeout_secs = it
                    .next()
                    .expect("--timeout requires seconds")
                    .parse()
                    .expect("timeout must be integer seconds");
            }
            "--keep-going" => keep_going = true,
            "--reset" => reset = true,
            "--no-verify" => skip_verify = true,
            "--no-generated-columns" => generated_columns = false,
            "--views" => views = true,
            "--attach" => attach = true,
            "--mvcc" => mvcc = true,
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            _ if arg.starts_with('-') && arg.as_str() != "-" => {
                eprintln!("unknown flag: {arg}");
                print_help();
                std::process::exit(2);
            }
            _ => {
                if sql_path.is_some() {
                    eprintln!("only one SQL path can be supplied");
                    std::process::exit(2);
                }
                sql_path = Some(PathBuf::from(arg));
            }
        }
    }

    let sql_path = sql_path.unwrap_or_else(|| {
        print_help();
        std::process::exit(2);
    });

    Args {
        sql_path,
        db_path,
        timeout: Duration::from_secs(timeout_secs),
        keep_going,
        reset,
        skip_verify,
        generated_columns,
        views,
        attach,
        mvcc,
    }
}

fn print_help() {
    println!(
        "Usage: turso-repro <sql-file|-> [options]\n\
         \n\
         Pass `-` as the path to read SQL from stdin:\n  \
           turso-repro -             # then paste, Ctrl-D to finish\n  \
           pbpaste | turso-repro -\n\
         \n\
         Options:\n  \
           --db <path>         Database path (default :memory:)\n  \
           -t, --timeout <s>   Per-statement timeout seconds (default 30)\n  \
           --keep-going        Continue past statement errors/timeouts\n  \
           --reset             DROP TABLE known repro tables before executing\n  \
           --no-verify         Skip the trailing verification queries\n  \
           --no-generated-columns  Disable generated-columns experimental flag (on by default)\n  \
           --views             Enable experimental views\n  \
           --attach            Enable experimental ATTACH\n  \
           --mvcc              Enable experimental MVCC\n  \
           -h, --help          Show this help"
    );
}

const RESET_TABLES: &[&str] = &[
    "comments",
    "comment_threads",
    "subscriptions",
    "select_list_options",
    "external_dependencies",
    "at_refs",
    "core_denormalized_scalars",
    "row_activity",
    "op_log",
    "_transaction_trigger_gate",
    "column_metadata",
    "metadata",
    "core",
];

fn main() {
    let args = parse_args();
    match run(&args) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("\x1b[31mrepro failed:\x1b[0m {e}");
            std::process::exit(1);
        }
    }
}

fn run(args: &Args) -> Result<(), String> {
    let sql = if args.sql_path.as_os_str() == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| format!("reading stdin: {e}"))?;
        buf
    } else {
        std::fs::read_to_string(&args.sql_path)
            .map_err(|e| format!("reading {}: {e}", args.sql_path.display()))?
    };
    let statements = split_statements(&sql);

    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut opts = DatabaseOpts::new().with_generated_columns(args.generated_columns);
    if args.views {
        opts = opts.with_views(true);
    }
    if args.attach {
        opts = opts.with_attach(true);
    }
    // NOTE: MVCC is not toggled through DatabaseOpts in this API — BEGIN CONCURRENT
    // is handled at statement level. The --mvcc flag is currently informational.
    let _ = args.mvcc;
    let db = Database::open_file_with_flags(io, &args.db_path, OpenFlags::default(), opts, None)
        .map_err(|e| format!("open db: {e}"))?;
    let conn = db.connect().map_err(|e| format!("connect: {e}"))?;

    println!();
    println!("\x1b[1mturso-repro\x1b[0m");
    println!("  SQL file:    {}", args.sql_path.display());
    println!("  DB:          {}", args.db_path);
    println!("  Statements:  {}", statements.len());
    println!("  Timeout:     {}s", args.timeout.as_secs());
    println!("  Keep-going:  {}", args.keep_going);
    println!();

    if args.reset {
        println!("resetting schema...");
        for tbl in RESET_TABLES {
            let sql = format!("DROP TABLE IF EXISTS {tbl}");
            if let Err(e) = execute_with_timeout(&conn, &sql, args.timeout) {
                return Err(format!("reset failed on {tbl}: {e}"));
            }
        }
        println!();
    }

    let mut failures = 0usize;
    let start = Instant::now();
    for (i, stmt) in statements.iter().enumerate() {
        let snippet = snippet(stmt);
        let t0 = Instant::now();
        match execute_with_timeout(&conn, stmt, args.timeout) {
            Ok(rows) => {
                let elapsed = t0.elapsed();
                let tag = if rows > 0 {
                    format!(" {rows} row{}", if rows == 1 { "" } else { "s" })
                } else {
                    String::new()
                };
                println!(
                    "[{:>3}/{}] {:>7.1}ms OK{}   {}",
                    i + 1,
                    statements.len(),
                    elapsed.as_secs_f64() * 1000.0,
                    tag,
                    snippet
                );
            }
            Err(e) => {
                let elapsed = t0.elapsed();
                println!(
                    "[{:>3}/{}] {:>7.1}ms \x1b[31mFAIL\x1b[0m   {}\n          -> {}",
                    i + 1,
                    statements.len(),
                    elapsed.as_secs_f64() * 1000.0,
                    snippet,
                    e
                );
                failures += 1;
                if !args.keep_going {
                    return Err(format!("statement {} failed: {e}", i + 1));
                }
            }
        }
    }

    println!();
    println!(
        "all statements done in {:.2}s ({} failures)",
        start.elapsed().as_secs_f64(),
        failures
    );

    if !args.skip_verify {
        println!();
        println!("\x1b[1mverification\x1b[0m");
        run_verification(&conn, args.timeout)?;
    }

    Ok(())
}

fn execute_with_timeout(
    conn: &Arc<Connection>,
    sql: &str,
    timeout: Duration,
) -> Result<usize, String> {
    // Watchdog: another thread sleeps for the timeout and calls conn.interrupt()
    // if the statement is still running. The `done` flag lets us cancel the
    // watchdog cleanly when the statement finishes first.
    let done = Arc::new(AtomicBool::new(false));
    let watchdog = {
        let conn = Arc::clone(conn);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let start = Instant::now();
            while start.elapsed() < timeout {
                if done.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if !done.load(Ordering::Relaxed) {
                conn.interrupt();
            }
        })
    };

    let result = run_once(conn, sql);
    done.store(true, Ordering::Relaxed);
    let _ = watchdog.join();
    result
}

fn run_once(conn: &Arc<Connection>, sql: &str) -> Result<usize, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| format!("prepare: {e}"))?;
    let mut rows = 0usize;
    loop {
        match stmt.step().map_err(|e| format!("step: {e}"))? {
            StepResult::Done => return Ok(rows),
            StepResult::Row => {
                rows += 1;
            }
            StepResult::IO => {
                stmt.get_pager()
                    .io
                    .step()
                    .map_err(|e| format!("io step: {e}"))?;
            }
            StepResult::Interrupt => {
                return Err("interrupted (timeout)".to_string());
            }
            StepResult::Busy => {
                return Err("busy".to_string());
            }
        }
    }
}

fn run_verification(conn: &Arc<Connection>, timeout: Duration) -> Result<(), String> {
    let metadata_row = fetch_one(
        conn,
        "SELECT latest_op_version, max_row_number, coda_document_id FROM metadata LIMIT 1",
        timeout,
    )?;
    let core_row = fetch_one(
        conn,
        "SELECT id, row_number, system_search_representation FROM core WHERE id = 'row-example-id'",
        timeout,
    )?;

    match metadata_row.as_ref() {
        Some(r) => {
            println!(
                "  metadata: latest_op_version={}  max_row_number={}  coda_document_id={}",
                fmt(&r[0]),
                fmt(&r[1]),
                fmt(&r[2])
            );
        }
        None => println!("  metadata: \x1b[31mMISSING\x1b[0m"),
    }
    match core_row.as_ref() {
        Some(r) => {
            println!(
                "  core:     id={}  row_number={}  system_search_representation={}",
                fmt(&r[0]),
                fmt(&r[1]),
                fmt(&r[2])
            );
        }
        None => println!("  core:     \x1b[31mMISSING\x1b[0m"),
    }

    Ok(())
}

fn fetch_one(
    conn: &Arc<Connection>,
    sql: &str,
    timeout: Duration,
) -> Result<Option<Vec<Value>>, String> {
    let done = Arc::new(AtomicBool::new(false));
    let watchdog = {
        let conn = Arc::clone(conn);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let start = Instant::now();
            while start.elapsed() < timeout {
                if done.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if !done.load(Ordering::Relaxed) {
                conn.interrupt();
            }
        })
    };

    let result = (|| {
        let mut stmt = conn.prepare(sql).map_err(|e| format!("prepare: {e}"))?;
        loop {
            match stmt.step().map_err(|e| format!("step: {e}"))? {
                StepResult::Done => return Ok(None),
                StepResult::Row => {
                    let row = stmt.row().ok_or("Row reported but no row")?;
                    let values: Vec<Value> = row.get_values().cloned().collect();
                    return Ok(Some(values));
                }
                StepResult::IO => {
                    stmt.get_pager()
                        .io
                        .step()
                        .map_err(|e| format!("io step: {e}"))?;
                }
                StepResult::Interrupt => return Err("interrupted (timeout)".to_string()),
                StepResult::Busy => return Err("busy".to_string()),
            }
        }
    })();

    done.store(true, Ordering::Relaxed);
    let _ = watchdog.join();
    result
}

fn fmt(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Numeric(n) => format!("{n:?}"),
        Value::Text(t) => format!("\"{}\"", t.as_str()),
        Value::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}

fn snippet(stmt: &str) -> String {
    let one_line: String = stmt.split_whitespace().collect::<Vec<_>>().join(" ");
    if one_line.len() <= 120 {
        one_line
    } else {
        format!("{}...", &one_line[..117])
    }
}

// Statement splitter: handles --line and /* block */ comments, '…' and "…"
// string literals (with doubled-quote escape), and CREATE TRIGGER bodies that
// contain their own BEGIN…END; block.
fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut token = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut in_trigger = false;
    let mut trigger_depth: i32 = 0;

    let bytes: Vec<char> = sql.chars().collect();
    let mut i = 0;

    let flush_token = |current: &str,
                       token: &mut String,
                       in_trigger: &mut bool,
                       trigger_depth: &mut i32| {
        if token.is_empty() {
            return;
        }
        if !*in_trigger {
            let stripped = strip_leading_noise(current);
            if stripped.to_ascii_uppercase().starts_with("CREATE TRIGGER")
                || stripped.to_ascii_uppercase().starts_with("CREATE TEMP TRIGGER")
                || stripped
                    .to_ascii_uppercase()
                    .starts_with("CREATE TEMPORARY TRIGGER")
            {
                *in_trigger = true;
            }
        }
        if *in_trigger {
            let upper = token.to_ascii_uppercase();
            if upper == "BEGIN" || upper == "CASE" {
                *trigger_depth += 1;
            } else if upper == "END" && *trigger_depth > 0 {
                *trigger_depth -= 1;
            }
        }
        token.clear();
    };

    let push_current = |statements: &mut Vec<String>,
                        current: &mut String,
                        token: &mut String,
                        in_trigger: &mut bool,
                        trigger_depth: &mut i32| {
        let trimmed = current.trim().to_string();
        if !trimmed.is_empty() {
            statements.push(trimmed);
        }
        current.clear();
        token.clear();
        *in_trigger = false;
        *trigger_depth = 0;
    };

    while i < bytes.len() {
        let c = bytes[i];
        let next = bytes.get(i + 1).copied();
        current.push(c);

        if in_line_comment {
            if c == '\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if c == '*' && next == Some('/') {
                current.push('/');
                i += 2;
                in_block_comment = false;
                continue;
            }
            i += 1;
            continue;
        }
        if in_single {
            if c == '\'' && next == Some('\'') {
                current.push('\'');
                i += 2;
                continue;
            }
            if c == '\'' {
                in_single = false;
            }
            i += 1;
            continue;
        }
        if in_double {
            if c == '"' && next == Some('"') {
                current.push('"');
                i += 2;
                continue;
            }
            if c == '"' {
                in_double = false;
            }
            i += 1;
            continue;
        }

        if c == '-' && next == Some('-') {
            current.push('-');
            i += 2;
            flush_token(&current, &mut token, &mut in_trigger, &mut trigger_depth);
            in_line_comment = true;
            continue;
        }
        if c == '/' && next == Some('*') {
            current.push('*');
            i += 2;
            flush_token(&current, &mut token, &mut in_trigger, &mut trigger_depth);
            in_block_comment = true;
            continue;
        }
        if c == '\'' {
            flush_token(&current, &mut token, &mut in_trigger, &mut trigger_depth);
            in_single = true;
            i += 1;
            continue;
        }
        if c == '"' {
            flush_token(&current, &mut token, &mut in_trigger, &mut trigger_depth);
            in_double = true;
            i += 1;
            continue;
        }
        if c.is_alphabetic() || c == '_' {
            token.push(c);
            i += 1;
            continue;
        }

        flush_token(&current, &mut token, &mut in_trigger, &mut trigger_depth);
        if c == ';' && (!in_trigger || trigger_depth == 0) {
            push_current(
                &mut statements,
                &mut current,
                &mut token,
                &mut in_trigger,
                &mut trigger_depth,
            );
        }
        i += 1;
    }

    flush_token(&current, &mut token, &mut in_trigger, &mut trigger_depth);
    push_current(
        &mut statements,
        &mut current,
        &mut token,
        &mut in_trigger,
        &mut trigger_depth,
    );
    statements
}

fn strip_leading_noise(s: &str) -> &str {
    let mut bytes = s.as_bytes();
    loop {
        // Skip whitespace
        while let Some((&b, rest)) = bytes.split_first() {
            if b.is_ascii_whitespace() {
                bytes = rest;
            } else {
                break;
            }
        }
        // Skip line comment
        if bytes.starts_with(b"--") {
            if let Some(nl) = bytes.iter().position(|&b| b == b'\n') {
                bytes = &bytes[nl + 1..];
            } else {
                bytes = &[];
            }
            continue;
        }
        // Skip block comment
        if bytes.starts_with(b"/*") {
            if let Some(end) = find_subslice(bytes, b"*/") {
                bytes = &bytes[end + 2..];
            } else {
                bytes = &[];
            }
            continue;
        }
        break;
    }
    std::str::from_utf8(bytes).unwrap_or("")
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}
