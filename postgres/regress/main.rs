// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Runner for the upstream PostgreSQL regression tests.
//!
//! The test corpus lives in `postgres/conformance/upstream/` and is imported
//! verbatim from the PostgreSQL source tree (`src/test/regress/`): each
//! `<name>.sql` script is paired with the expected psql transcript
//! `<name>.out`. The corpus is pristine — only this runner is ours.
//!
//! Unlike upstream's pg_regress, this runner does not shell out to `psql`.
//! It speaks the PostgreSQL wire protocol directly (simple query protocol
//! over TCP) and reproduces psql's transcript output — echoed input, aligned
//! result tables, error reports with `LINE n:` position markers — so the
//! combined output byte-compares against the expected transcript. Start the
//! server first, then point the runner at it:
//!
//! ```text
//! tursopg --server 127.0.0.1:5432 &
//! cargo run -p turso_pg_regress -- --dsn 'postgres://127.0.0.1:5432/regression'
//! ```
//!
//! The transcript emulation mirrors what `pg_regress` gets out of
//! `psql -X -a -q` with pg_regress's pinned environment (the equivalent
//! settings are sent as startup parameters). If every test starts failing
//! with uniform, systematic diffs, suspect a drift between this emulation
//! and psql before suspecting the server.
//!
//! The meta-commands the corpus uses are interpreted: psql variables
//! (`\set`, `\getenv`, interpolation of `:name`, `:'name'`, `:"name"`),
//! conditionals (`\if`/`\elif`/`\else`/`\endif`), buffer send commands
//! (`\g`, `\gset`, `\gexec`), output control (`\o`, `\echo`, `\qecho`,
//! `\x`, `\pset null`), client-side `\copy`, inline `COPY ... FROM stdin`
//! data, reconnects (`\c`), `\quit`, and the describe family (`\d`, `\d+`,
//! `\dD`, `\dT+`, `\sv`, `\sf` — see `describe.rs`).
//!
//! Known emulation gaps, to be filled as the corpus grows to need them:
//! multi-line field values are not rendered with `+` continuation markers,
//! long error-position lines are not clipped with `...`, and column widths
//! count characters rather than terminal display width.

mod describe;

use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read as _, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use clap::Parser;

/// Default endpoint when neither `--dsn` nor `PGREGRESS_DSN` is set.
const DEFAULT_DSN: &str = "postgres://127.0.0.1:5432/regression";

#[derive(Parser, Debug)]
#[command(
    name = "pgregress",
    about = "Run upstream PostgreSQL regression tests over the wire"
)]
struct Args {
    /// Test scripts or directories to run (directories are searched for `*.sql`).
    #[arg(default_value = "postgres/conformance/upstream")]
    paths: Vec<PathBuf>,

    /// Connection string. Falls back to $PGREGRESS_DSN, then a localhost default.
    #[arg(long)]
    dsn: Option<String>,

    /// Directory for actual output and diffs of failed tests.
    #[arg(long, default_value = "postgres/regress/results")]
    results: PathBuf,

    /// Maximum diff lines to print per failed test (0 = unlimited).
    #[arg(long, default_value_t = 40)]
    max_diff_lines: usize,
}

/// Exit codes: 0 all passed, 1 output mismatches, 2 harness error,
/// 3 at least one transport failure (server died or stopped responding —
/// the driver should restart it before the next test).
fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(RunResult {
            failed: 0,
            transport_failures: 0,
        }) => ExitCode::SUCCESS,
        Ok(RunResult {
            transport_failures: 0,
            ..
        }) => ExitCode::FAILURE,
        Ok(_) => ExitCode::from(3),
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

#[derive(Default)]
struct RunResult {
    failed: usize,
    transport_failures: usize,
}

fn run(args: &Args) -> Result<RunResult> {
    let dsn = args
        .dsn
        .clone()
        .or_else(|| std::env::var("PGREGRESS_DSN").ok())
        .unwrap_or_else(|| DEFAULT_DSN.to_string());
    let params = ConnParams::parse(&dsn)?;

    let mut scripts = Vec::new();
    for path in &args.paths {
        collect_scripts(path, &mut scripts)
            .with_context(|| format!("discovering tests under {}", path.display()))?;
    }
    if scripts.is_empty() {
        bail!(
            "no .sql files found in: {}",
            args.paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    std::fs::create_dir_all(&args.results)
        .with_context(|| format!("creating results dir {}", args.results.display()))?;

    println!("running {} test(s) against {dsn}\n", scripts.len());

    let mut result = RunResult::default();
    for script in &scripts {
        match run_script(args, &params, script)? {
            TestResult::Passed => {}
            TestResult::Failed => result.failed += 1,
            TestResult::TransportError => {
                result.failed += 1;
                result.transport_failures += 1;
            }
        }
    }

    let passed = scripts.len() - result.failed;
    println!("\n{passed} of {} test(s) passed", scripts.len());
    Ok(result)
}

enum TestResult {
    Passed,
    Failed,
    /// The connection died or timed out; the server may be wedged.
    TransportError,
}

/// Collects `.sql` scripts from a file or directory (non-recursive). A
/// directory containing a `schedule` file runs in schedule order — later
/// tests use fixtures created by earlier ones (`test_setup` above all), so
/// alphabetical order would fail them spuriously.
fn collect_scripts(path: &Path, scripts: &mut Vec<PathBuf>) -> Result<()> {
    let meta = std::fs::metadata(path)
        .with_context(|| format!("no such file or directory: {}", path.display()))?;
    if meta.is_file() {
        scripts.push(path.to_path_buf());
        return Ok(());
    }
    let schedule = path.join("schedule");
    if schedule.is_file() {
        for line in std::fs::read_to_string(&schedule)?.lines() {
            let Some(names) = line.strip_prefix("test:") else {
                continue;
            };
            for name in names.split_whitespace() {
                let p = path.join(format!("{name}.sql"));
                if !p.is_file() {
                    bail!("schedule lists {name} but {} does not exist", p.display());
                }
                scripts.push(p);
            }
        }
        return Ok(());
    }
    let mut found = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() && p.extension().is_some_and(|e| e == "sql") {
            found.push(p);
        }
    }
    found.sort();
    scripts.extend(found);
    Ok(())
}

/// Runs one script and compares against its expected transcript.
fn run_script(args: &Args, params: &ConnParams, script: &Path) -> Result<TestResult> {
    let name = script
        .file_stem()
        .and_then(|s| s.to_str())
        .with_context(|| format!("bad test file name: {}", script.display()))?;
    let expected_path = script.with_extension("out");
    let expected = std::fs::read_to_string(&expected_path).with_context(|| {
        format!(
            "missing expected output {} for {}",
            expected_path.display(),
            script.display()
        )
    })?;

    print!("{name} ... ");
    std::io::stdout().flush()?;

    let source =
        std::fs::read_to_string(script).with_context(|| format!("reading {}", script.display()))?;

    let start = Instant::now();
    // A transport failure (typically the server dying or hanging mid-script
    // — a known engine gap) fails this test but does not abort the run.
    let actual = match run_transcript(params, name, &source) {
        Ok(actual) => actual,
        Err(e) => {
            println!("FAILED (error: {e:#})");
            return Ok(TestResult::TransportError);
        }
    };
    let elapsed = start.elapsed();

    let actual_path = args.results.join(format!("{name}.out"));
    std::fs::write(&actual_path, &actual)
        .with_context(|| format!("writing {}", actual_path.display()))?;

    if actual == expected {
        println!("ok ({} ms)", elapsed.as_millis());
        return Ok(TestResult::Passed);
    }

    let diff = similar::TextDiff::from_lines(&expected, &actual)
        .unified_diff()
        .header(
            &expected_path.display().to_string(),
            &actual_path.display().to_string(),
        )
        .to_string();
    let diff_path = args.results.join(format!("{name}.diff"));
    std::fs::write(&diff_path, &diff)?;

    let lines: Vec<&str> = diff.lines().collect();
    println!(
        "FAILED ({} ms, {} diff lines)",
        elapsed.as_millis(),
        lines.len()
    );
    let shown = if args.max_diff_lines == 0 {
        lines.len()
    } else {
        args.max_diff_lines.min(lines.len())
    };
    for line in &lines[..shown] {
        println!("  {line}");
    }
    if shown < lines.len() {
        println!(
            "  ... {} more lines in {}",
            lines.len() - shown,
            diff_path.display()
        );
    }
    Ok(TestResult::Failed)
}

/// Executes one script on a fresh connection (pg_regress runs one psql per
/// script) and returns the psql-style transcript.
fn run_transcript(params: &ConnParams, name: &str, source: &str) -> Result<String> {
    let mut session = Session::new(PgConn::connect(params, name)?, params.clone(), name);
    let mut out = String::new();
    let mut scanner = Scanner::default();

    let mut lines = source.split_inclusive('\n');
    while let Some(raw) = lines.next() {
        if session.quit {
            break;
        }
        let line = raw.strip_suffix('\n').unwrap_or(raw);
        // psql ignores empty input lines entirely — no echo, no newline in
        // the query buffer — unless they continue a quoted string.
        if line.is_empty() && !scanner.in_quote() {
            continue;
        }
        // psql's echo-all prints each input line as it is read, before any
        // statement completed on that line executes. Lines inside a false
        // `\if` branch still echo; only execution is skipped.
        if session.echo_input {
            out.push_str(line);
            out.push('\n');
        }
        for item in scanner.feed_line(line, &session.vars)? {
            match item {
                Item::Statement(stmt) => {
                    if !session.active() {
                        continue;
                    }
                    if session.execute(&stmt, &mut out)? == Outcome::CopyIn {
                        session.feed_copy_data(&mut lines, &mut out)?;
                    }
                }
                Item::MetaCommand(cmd) => {
                    session.meta_command(&cmd, &mut scanner, &mut lines, &mut out)?
                }
            }
        }
    }
    // psql executes whatever is left in the query buffer at EOF even without
    // a terminating semicolon.
    if !session.quit {
        if let Some(stmt) = scanner.take_rest() {
            if session.active() {
                session.execute(&stmt, &mut out)?;
            }
        }
    }
    Ok(out)
}

/// Rendering state mutable via psql meta-commands.
#[derive(Default)]
struct RenderOpts {
    /// `\pset null` — the display string for NULL values (default empty).
    null_display: String,
    /// `\x` — expanded display (one `-[ RECORD n ]` block per row).
    expanded: bool,
    /// `\set SHOW_CONTEXT` — when to render error CONTEXT fields.
    show_context: ShowContext,
}

#[derive(Default, Clone, Copy, PartialEq)]
enum ShowContext {
    Never,
    /// The psql default: CONTEXT only on messages of ERROR severity or worse.
    #[default]
    Errors,
    Always,
}

/// One `\if` nesting level.
struct CondFrame {
    /// Whether this branch executes (requires every enclosing branch too).
    active: bool,
    /// Whether any branch of this `\if` chain has been taken yet.
    taken: bool,
    /// Whether the surrounding scope was active when `\if` was read.
    parent_active: bool,
}

#[derive(PartialEq)]
enum Outcome {
    Done,
    /// The server entered COPY FROM STDIN; data lines follow in the script.
    CopyIn,
}

/// Per-script interpreter state: the connection plus everything psql
/// meta-commands mutate.
struct Session {
    conn: PgConn,
    params: ConnParams,
    test_name: String,
    /// psql variables (`\set`, `\getenv`, `\gset`), interpolated as `:name`.
    vars: HashMap<String, String>,
    /// `\set ECHO none` suppresses input echo (pg_regress default is all).
    echo_input: bool,
    cond: Vec<CondFrame>,
    /// `\o` — query output redirect; results go here, errors stay in the
    /// transcript (psql sends them to stderr, which pg_regress captures).
    out_file: Option<std::fs::File>,
    opts: RenderOpts,
    quit: bool,
}

impl Session {
    fn new(conn: PgConn, params: ConnParams, test_name: &str) -> Session {
        Session {
            conn,
            params,
            test_name: test_name.to_string(),
            vars: HashMap::new(),
            echo_input: true,
            cond: Vec::new(),
            out_file: None,
            opts: RenderOpts::default(),
            quit: false,
        }
    }

    /// Whether statements execute right now (no enclosing false `\if`).
    fn active(&self) -> bool {
        self.cond.iter().all(|f| f.active)
    }

    /// Appends query output, honoring an `\o` redirect.
    fn result_out(&mut self, text: &str, out: &mut String) {
        match &mut self.out_file {
            Some(f) => {
                let _ = f.write_all(text.as_bytes());
            }
            None => out.push_str(text),
        }
    }

    /// Sends one statement and appends its psql-rendered results.
    fn execute(&mut self, stmt: &str, out: &mut String) -> Result<Outcome> {
        self.conn.send_query(stmt)?;
        let mut table: Option<Table> = None;
        loop {
            let (tag, body) = self.conn.read_message()?;
            match tag {
                b'T' => table = Some(Table::from_row_description(&body)?),
                b'D' => {
                    let table = table
                        .as_mut()
                        .context("DataRow received without RowDescription")?;
                    table.rows.push(parse_data_row(&body)?);
                }
                b'C' => {
                    // Quiet mode: only row-returning statements produce output;
                    // command tags (CREATE TABLE, INSERT 0 1, ...) are suppressed.
                    if let Some(table) = table.take() {
                        let mut text = String::new();
                        table.render(&self.opts, &mut text);
                        self.result_out(&text, out);
                    }
                }
                b'E' => {
                    table = None;
                    format_error(&parse_error_fields(&body)?, stmt, &self.opts, out);
                }
                b'N' => format_error(&parse_error_fields(&body)?, stmt, &self.opts, out),
                b'G' => return Ok(Outcome::CopyIn),
                b'H' => self.receive_copy_out(out)?,
                b'd' | b'c' => {} // stray CopyData/CopyDone outside COPY OUT
                b'I' => {}        // EmptyQueryResponse
                b'Z' => return Ok(Outcome::Done),
                b'S' | b'K' | b'A' => {} // ParameterStatus, BackendKeyData, notifications
                other => bail!(
                    "unexpected message {:?} while executing query",
                    other as char
                ),
            }
        }
    }

    /// Streams COPY TO STDOUT data into the query output channel. Leaves the
    /// protocol after CopyDone; CommandComplete and ReadyForQuery are read by
    /// the caller's loop.
    fn receive_copy_out(&mut self, out: &mut String) -> Result<()> {
        loop {
            let (tag, body) = self.conn.read_message()?;
            match tag {
                b'd' => {
                    let text = String::from_utf8_lossy(&body).into_owned();
                    self.result_out(&text, out);
                }
                b'c' => return Ok(()),
                b'E' => {
                    format_error(&parse_error_fields(&body)?, "", &self.opts, out);
                    return Ok(());
                }
                other => bail!("unexpected message {:?} during COPY OUT", other as char),
            }
        }
    }

    /// Feeds inline `COPY ... FROM stdin` data: script lines up to `\.` are
    /// sent verbatim as copy data. psql does not echo copy data lines.
    fn feed_copy_data<'a>(
        &mut self,
        lines: &mut impl Iterator<Item = &'a str>,
        out: &mut String,
    ) -> Result<()> {
        for raw in lines {
            if raw.strip_suffix('\n').unwrap_or(raw).trim_end() == "\\." {
                return self.finish_copy_in(out);
            }
            let mut data = Vec::from(raw.as_bytes());
            if !data.ends_with(b"\n") {
                data.push(b'\n');
            }
            self.conn.write_message(b'd', &data)?;
        }
        // EOF before `\.` terminates the copy, matching psql.
        self.finish_copy_in(out)
    }

    /// Sends CopyDone and drains the protocol to ReadyForQuery.
    fn finish_copy_in(&mut self, out: &mut String) -> Result<()> {
        self.conn.write_message(b'c', &[])?;
        loop {
            let (tag, body) = self.conn.read_message()?;
            match tag {
                b'C' | b'S' | b'K' | b'A' => {}
                b'E' => format_error(&parse_error_fields(&body)?, "", &self.opts, out),
                b'N' => format_error(&parse_error_fields(&body)?, "", &self.opts, out),
                b'Z' => return Ok(()),
                other => bail!("unexpected message {:?} finishing COPY IN", other as char),
            }
        }
    }

    /// Runs a query and collects its result table instead of rendering it.
    /// Server errors and notices are rendered into the transcript; an error
    /// yields `Ok(None)` so callers skip their command, the way psql does.
    fn collect(&mut self, sql: &str, out: &mut String) -> Result<Option<Table>> {
        self.conn.send_query(sql)?;
        let mut table: Option<Table> = None;
        let mut failed = false;
        loop {
            let (tag, body) = self.conn.read_message()?;
            match tag {
                b'T' => table = Some(Table::from_row_description(&body)?),
                b'D' => {
                    if let Some(t) = table.as_mut() {
                        t.rows.push(parse_data_row(&body)?);
                    }
                }
                b'E' => {
                    failed = true;
                    table = None;
                    format_error(&parse_error_fields(&body)?, sql, &self.opts, out);
                }
                b'N' => format_error(&parse_error_fields(&body)?, sql, &self.opts, out),
                b'C' | b'I' | b'S' | b'K' | b'A' => {}
                b'Z' => return Ok(if failed { None } else { table }),
                other => bail!("unexpected message {:?} collecting query", other as char),
            }
        }
    }

    /// Interprets one backslash command. `scanner` provides the query buffer
    /// for the send commands (`\g`, `\gset`, `\gexec`); `lines` provides data
    /// for a `\copy ... from stdin`. Unknown commands render psql's
    /// `invalid command` line and continue, so one emulation gap cannot abort
    /// a whole script.
    fn meta_command<'a>(
        &mut self,
        cmd: &str,
        scanner: &mut Scanner,
        lines: &mut impl Iterator<Item = &'a str>,
        out: &mut String,
    ) -> Result<()> {
        let rest = cmd.strip_prefix('\\').unwrap_or(cmd);
        let (name, rest) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));

        // Inside a false branch only the conditional commands execute; psql
        // parses and discards everything else, including \quit.
        if !self.active() && !matches!(name, "if" | "elif" | "else" | "endif") {
            return Ok(());
        }

        match name {
            "if" => {
                let parent = self.active();
                let arg = self.meta_args(rest).join(" ");
                let value = parse_bool(&arg).unwrap_or_else(|| {
                    out.push_str(&format!(
                        "unrecognized value \"{arg}\" for \"\\if expression\": Boolean expected\n"
                    ));
                    false
                });
                self.cond.push(CondFrame {
                    active: parent && value,
                    taken: value,
                    parent_active: parent,
                });
            }
            "elif" => {
                let value = parse_bool(&self.meta_args(rest).join(" ")).unwrap_or(false);
                match self.cond.last_mut() {
                    Some(f) => {
                        f.active = f.parent_active && !f.taken && value;
                        f.taken |= value;
                    }
                    None => out.push_str("\\elif: no matching \\if\n"),
                }
            }
            "else" => match self.cond.last_mut() {
                Some(f) => {
                    f.active = f.parent_active && !f.taken;
                    f.taken = true;
                }
                None => out.push_str("\\else: no matching \\if\n"),
            },
            "endif" => {
                if self.cond.pop().is_none() {
                    out.push_str("\\endif: no matching \\if\n");
                }
            }
            "quit" | "q" => self.quit = true,
            "set" => {
                let args = self.meta_args(rest);
                if let Some((name, parts)) = args.split_first() {
                    // psql concatenates multiple value arguments directly.
                    let value = parts.concat();
                    self.set_var(name.clone(), value);
                }
            }
            "unset" => {
                for name in self.meta_args(rest) {
                    self.vars.remove(&name);
                }
            }
            "getenv" => {
                let args = self.meta_args(rest);
                if let [var, env] = args.as_slice() {
                    if let Ok(value) = std::env::var(env) {
                        self.set_var(var.clone(), value);
                    }
                }
            }
            "echo" => {
                out.push_str(&self.meta_args(rest).join(" "));
                out.push('\n');
            }
            "qecho" => {
                let text = format!("{}\n", self.meta_args(rest).join(" "));
                self.result_out(&text, out);
            }
            "o" | "out" => {
                let args = self.meta_args(rest);
                self.out_file = match args.first() {
                    Some(path) => {
                        if let Some(dir) = Path::new(path).parent() {
                            let _ = std::fs::create_dir_all(dir);
                        }
                        Some(std::fs::File::create(path).with_context(|| format!("\\o {path}"))?)
                    }
                    None => None,
                };
            }
            "x" => self.opts.expanded = !self.opts.expanded,
            "pset" => {
                let args = self.meta_args(rest);
                match args.as_slice() {
                    [option, value] if option == "null" => {
                        self.opts.null_display.clone_from(value);
                    }
                    _ => out.push_str(&format!("\\pset: unsupported option: {rest}\n")),
                }
            }
            "c" | "connect" => {
                // `\c` / `\c -`: reconnect to the same database. Variables
                // and display options survive, connection state resets.
                self.conn = PgConn::connect(&self.params, &self.test_name)?;
            }
            "g" => {
                let stmt = scanner.take_buffer();
                self.execute(&stmt, out)?;
            }
            "gset" => {
                let prefix = self.meta_args(rest).into_iter().next().unwrap_or_default();
                let stmt = scanner.take_buffer();
                match self.collect(&stmt, out)? {
                    Some(t) if t.rows.len() == 1 => {
                        for (col, value) in t.columns.iter().zip(&t.rows[0]) {
                            let name = format!("{prefix}{}", col.name);
                            match value {
                                // A NULL result unsets the variable.
                                Some(v) => self.set_var(name, v.clone()),
                                None => {
                                    self.vars.remove(&name);
                                }
                            }
                        }
                    }
                    Some(t) if t.rows.is_empty() => {
                        out.push_str("no rows returned for \\gset\n");
                    }
                    Some(_) => out.push_str("more than one row returned for \\gset\n"),
                    None => {} // error already rendered
                }
            }
            "gexec" => {
                let stmt = scanner.take_buffer();
                if let Some(t) = self.collect(&stmt, out)? {
                    for row in &t.rows {
                        for cell in row.iter().flatten() {
                            self.execute(cell, out)?;
                        }
                    }
                }
            }
            "copy" => self.client_copy(rest, lines, out)?,
            "." => {} // stray copy terminator (COPY failed to start); ignore
            _ if name.starts_with('d') || name == "sv" || name == "sf" => {
                let args = self.meta_args(rest);
                describe::run(self, name, &args, out)?;
            }
            _ => out.push_str(&format!("invalid command \\{name}\n")),
        }
        Ok(())
    }

    /// Stores a variable, applying the side effects of psql's special ones.
    fn set_var(&mut self, name: String, value: String) {
        match name.as_str() {
            "ECHO" => self.echo_input = value != "none",
            "SHOW_CONTEXT" => {
                self.opts.show_context = match value.as_str() {
                    "always" => ShowContext::Always,
                    "never" => ShowContext::Never,
                    _ => ShowContext::Errors,
                }
            }
            _ => {}
        }
        self.vars.insert(name, value);
    }

    /// Tokenizes meta-command arguments: whitespace-separated, single quotes
    /// group (with backslash escapes), `:name` and `:'name'` interpolate.
    fn meta_args(&self, rest: &str) -> Vec<String> {
        split_meta_args(rest, &self.vars)
    }

    /// `\copy`: rewrites the client-side spec into a server COPY via STDIN /
    /// STDOUT and shuttles the data over the wire. `\copy ... from <file>`
    /// streams the file; `from stdin` consumes script lines like inline COPY.
    fn client_copy<'a>(
        &mut self,
        spec: &str,
        lines: &mut impl Iterator<Item = &'a str>,
        out: &mut String,
    ) -> Result<()> {
        let Some((before, direction, target, options)) = split_copy_spec(spec) else {
            out.push_str("\\copy: parse error\n");
            return Ok(());
        };
        let to_server = direction.eq_ignore_ascii_case("from");
        let stream = if to_server { "STDIN" } else { "STDOUT" };
        let sql = format!("COPY {before} {direction} {stream} {options}");
        match self.execute(sql.trim_end(), out)? {
            Outcome::Done => Ok(()), // COPY OUT (or an error) fully handled
            Outcome::CopyIn => {
                let is_file = !matches!(target.to_ascii_lowercase().as_str(), "stdin" | "pstdin");
                if is_file {
                    match std::fs::read(&target) {
                        Ok(data) => {
                            self.conn.write_message(b'd', &data)?;
                            self.finish_copy_in(out)
                        }
                        Err(e) => {
                            out.push_str(&format!("{target}: {e}\n"));
                            self.finish_copy_in(out)
                        }
                    }
                } else {
                    self.feed_copy_data(lines, out)
                }
            }
        }
    }
}

/// psql's boolean variable parsing: unique prefixes of true/false/yes/no,
/// on/off, and 1/0.
fn parse_bool(value: &str) -> Option<bool> {
    let v = value.trim().to_ascii_lowercase();
    if v.is_empty() {
        return None;
    }
    for (word, result) in [
        ("true", true),
        ("false", false),
        ("yes", true),
        ("no", false),
    ] {
        if word.starts_with(&v) {
            return Some(result);
        }
    }
    match v.as_str() {
        "on" => Some(true),
        "off" | "of" => Some(false),
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

/// Splits meta-command arguments the way psql's lexer does: whitespace
/// separates tokens, single-quoted spans group (backslash escapes a quote or
/// backslash), double-quoted spans group and keep their quotes, and `:name` /
/// `:'name'` interpolate variables. Unknown variables stay literal.
fn split_meta_args(rest: &str, vars: &HashMap<String, String>) -> Vec<String> {
    let chars: Vec<char> = rest.chars().collect();
    let mut args = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        let mut token = String::new();
        while i < chars.len() && !chars[i].is_whitespace() {
            match chars[i] {
                '\'' => {
                    i += 1;
                    while i < chars.len() && chars[i] != '\'' {
                        if chars[i] == '\\' && i + 1 < chars.len() {
                            match chars[i + 1] {
                                '\\' | '\'' => {
                                    token.push(chars[i + 1]);
                                    i += 2;
                                    continue;
                                }
                                'n' => {
                                    token.push('\n');
                                    i += 2;
                                    continue;
                                }
                                't' => {
                                    token.push('\t');
                                    i += 2;
                                    continue;
                                }
                                _ => {}
                            }
                        }
                        token.push(chars[i]);
                        i += 1;
                    }
                    i += 1; // closing quote
                }
                '"' => {
                    // Double quotes group and are kept (SQL identifiers).
                    token.push('"');
                    i += 1;
                    while i < chars.len() && chars[i] != '"' {
                        token.push(chars[i]);
                        i += 1;
                    }
                    token.push('"');
                    i += 1;
                }
                ':' => {
                    if let Some((text, consumed)) = interpolate_var(&chars[i..], vars) {
                        token.push_str(&text);
                        i += consumed;
                    } else {
                        token.push(':');
                        i += 1;
                    }
                }
                c => {
                    token.push(c);
                    i += 1;
                }
            }
        }
        args.push(token);
    }
    args
}

/// Parses a `:name`, `:'name'`, or `:"name"` reference at `chars[0] == ':'`.
/// Returns the substituted text and the chars consumed, or None if it is not
/// a reference to a defined variable.
fn interpolate_var(chars: &[char], vars: &HashMap<String, String>) -> Option<(String, usize)> {
    debug_assert_eq!(chars[0], ':');
    let (quote, start) = match chars.get(1) {
        Some(&q @ ('\'' | '"')) => (Some(q), 2),
        _ => (None, 1),
    };
    let mut name = String::new();
    let mut i = start;
    while i < chars.len() {
        let c = chars[i];
        match quote {
            Some(q) if c == q => break,
            Some(_) => name.push(c),
            None if c.is_alphanumeric() || c == '_' => name.push(c),
            None => break,
        }
        i += 1;
    }
    if quote.is_some() {
        if i >= chars.len() {
            return None; // unterminated
        }
        i += 1; // closing quote
    }
    if name.is_empty() {
        return None;
    }
    let value = vars.get(&name)?;
    let text = match quote {
        // :'name' — value as an escaped SQL string literal.
        Some('\'') => format!("'{}'", value.replace('\'', "''")),
        // :"name" — value as a quoted SQL identifier.
        Some('"') => format!("\"{}\"", value.replace('"', "\"\"")),
        _ => value.clone(),
    };
    Some((text, i))
}

/// Splits a `\copy` spec into (table-or-query, direction keyword, target,
/// trailing options). The direction is the first top-level `to`/`from`
/// outside parentheses and quotes.
fn split_copy_spec(spec: &str) -> Option<(String, String, String, String)> {
    let chars: Vec<char> = spec.chars().collect();
    let mut depth = 0u32;
    let mut in_quote = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if in_quote {
            if c == '\'' {
                in_quote = false;
            }
        } else {
            match c {
                '\'' => in_quote = true,
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                _ if depth == 0 => {
                    let rest: String = chars[i..].iter().collect();
                    let lower = rest.to_ascii_lowercase();
                    for dir in ["to", "from"] {
                        let boundary_before = i == 0 || chars[i - 1].is_whitespace();
                        if boundary_before
                            && lower.starts_with(dir)
                            && rest[dir.len()..].starts_with(char::is_whitespace)
                        {
                            let before: String = chars[..i].iter().collect();
                            let after = rest[dir.len()..].trim_start();
                            let (target, options) = match after.split_once(char::is_whitespace) {
                                Some((t, o)) => (t.to_string(), o.trim().to_string()),
                                None => (after.to_string(), String::new()),
                            };
                            // Strip quotes from a file target.
                            let target = target
                                .strip_prefix('\'')
                                .and_then(|t| t.strip_suffix('\''))
                                .unwrap_or(&target)
                                .to_string();
                            return Some((
                                before.trim().to_string(),
                                rest[..dir.len()].to_string(),
                                target,
                                options,
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

// ---------------------------------------------------------------------------
// Script scanning: psql's input loop
// ---------------------------------------------------------------------------

/// Lexer state carried across input lines.
#[derive(Default, Clone, Copy, PartialEq)]
enum LexState {
    #[default]
    Normal,
    SingleQuote,
    /// `E'...'` string, where backslash escapes a following quote.
    EscapeQuote,
    DoubleQuote,
    BlockComment(u32),
}

/// One unit of work produced by the scanner.
enum Item {
    /// A complete SQL statement to send to the server.
    Statement(String),
    /// A whole-line psql meta-command (backslash command).
    MetaCommand(String),
}

/// Splits script input into statements the way psql's scanner does: `--`
/// comments are stripped (they are never sent to the server, which is why
/// upstream transcripts report `LINE 1:` for statements preceded by comment
/// lines), leading whitespace of a fresh statement is skipped, and newlines
/// inside a statement are preserved so error positions map to the right line.
#[derive(Default)]
struct Scanner {
    buf: String,
    state: LexState,
    dollar_tag: Option<String>,
}

impl Scanner {
    /// Feeds one input line (without its newline) and returns the statements
    /// and meta-commands it completed, in order. `vars` drives psql variable
    /// interpolation (`:name`, `:'name'`, `:"name"`) outside quotes.
    fn feed_line(&mut self, line: &str, vars: &HashMap<String, String>) -> Result<Vec<Item>> {
        let mut completed = Vec::new();
        let chars: Vec<char> = line.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            let next = chars.get(i + 1).copied();
            match self.state {
                LexState::Normal => {
                    if let Some(tag) = self.dollar_tag.clone() {
                        // Inside $tag$ ... $tag$: look for the closing tag.
                        if c == '$' && line[byte_at(line, i)..].starts_with(&tag) {
                            self.buf.push_str(&tag);
                            i += tag.chars().count();
                            self.dollar_tag = None;
                            continue;
                        }
                        self.buf.push(c);
                    } else if c == '-' && next == Some('-') {
                        break; // line comment: strip to end of line
                    } else if c == '/' && next == Some('*') {
                        self.buf.push_str("/*");
                        self.state = LexState::BlockComment(1);
                        i += 2;
                        continue;
                    } else if c == '\'' {
                        let escape_string = self.buf.chars().last().is_some_and(|p| {
                            (p == 'e' || p == 'E')
                                && !self
                                    .buf
                                    .chars()
                                    .rev()
                                    .nth(1)
                                    .is_some_and(|q| q.is_alphanumeric() || q == '_')
                        });
                        self.buf.push(c);
                        self.state = if escape_string {
                            LexState::EscapeQuote
                        } else {
                            LexState::SingleQuote
                        };
                    } else if c == '"' {
                        self.buf.push(c);
                        self.state = LexState::DoubleQuote;
                    } else if c == '$' {
                        // Possible dollar-quote opener: $tag$ where tag is
                        // empty or an identifier.
                        if let Some(tag) = scan_dollar_tag(&chars[i..]) {
                            self.buf.push_str(&tag);
                            i += tag.chars().count();
                            self.dollar_tag = Some(tag);
                            continue;
                        }
                        self.buf.push(c);
                    } else if c == '\\' {
                        // A backslash outside quotes starts a meta-command
                        // that runs to end of line; the query buffer stays
                        // for send commands like `SELECT ... \gset`.
                        let rest: String = chars[i..].iter().collect();
                        completed.push(Item::MetaCommand(rest.trim().to_string()));
                        return Ok(completed);
                    } else if c == ':' && !self.buf.ends_with(':') {
                        // psql variable interpolation, skipping `::` casts.
                        if let Some((text, consumed)) = interpolate_var(&chars[i..], vars) {
                            self.buf.push_str(&text);
                            i += consumed;
                            continue;
                        }
                        self.buf.push(c);
                    } else if c == ';' {
                        self.buf.push(c);
                        completed.push(Item::Statement(std::mem::take(&mut self.buf)));
                    } else if c.is_whitespace() && self.buf.is_empty() {
                        // psql suppresses whitespace at the start of a statement.
                    } else {
                        self.buf.push(c);
                    }
                }
                LexState::SingleQuote => {
                    self.buf.push(c);
                    if c == '\'' {
                        if next == Some('\'') {
                            self.buf.push('\'');
                            i += 2;
                            continue;
                        }
                        self.state = LexState::Normal;
                    }
                }
                LexState::EscapeQuote => {
                    self.buf.push(c);
                    if c == '\\' {
                        if let Some(n) = next {
                            self.buf.push(n);
                            i += 2;
                            continue;
                        }
                    } else if c == '\'' {
                        if next == Some('\'') {
                            self.buf.push('\'');
                            i += 2;
                            continue;
                        }
                        self.state = LexState::Normal;
                    }
                }
                LexState::DoubleQuote => {
                    self.buf.push(c);
                    if c == '"' {
                        if next == Some('"') {
                            self.buf.push('"');
                            i += 2;
                            continue;
                        }
                        self.state = LexState::Normal;
                    }
                }
                LexState::BlockComment(depth) => {
                    if c == '*' && next == Some('/') {
                        self.buf.push_str("*/");
                        i += 2;
                        self.state = if depth == 1 {
                            LexState::Normal
                        } else {
                            LexState::BlockComment(depth - 1)
                        };
                        continue;
                    }
                    if c == '/' && next == Some('*') {
                        self.buf.push_str("/*");
                        i += 2;
                        self.state = LexState::BlockComment(depth + 1);
                        continue;
                    }
                    self.buf.push(c);
                }
            }
            i += 1;
        }
        if !self.buf.is_empty() {
            self.buf.push('\n');
        }
        Ok(completed)
    }

    /// Whether the scanner is inside a quoted string (psql_scan_in_quote).
    fn in_quote(&self) -> bool {
        self.dollar_tag.is_some()
            || matches!(
                self.state,
                LexState::SingleQuote | LexState::EscapeQuote | LexState::DoubleQuote
            )
    }

    /// Returns the unterminated trailing statement at EOF, if any. A buffer
    /// holding only block comments is not a statement — psql does not send
    /// it, so a script ending in `/* ... */` produces no output.
    fn take_rest(&mut self) -> Option<String> {
        let rest = std::mem::take(&mut self.buf);
        let trimmed = rest.trim();
        (!trimmed.is_empty() && !is_only_comments(trimmed)).then(|| trimmed.to_string())
    }

    /// Takes the current query buffer for a send command (`\g`, `\gset`).
    fn take_buffer(&mut self) -> String {
        std::mem::take(&mut self.buf).trim().to_string()
    }
}

/// Whether the text is nothing but block comments and whitespace.
fn is_only_comments(text: &str) -> bool {
    let mut rest = text.trim_start();
    while let Some(after_open) = rest.strip_prefix("/*") {
        let mut depth = 1u32;
        let mut chars = after_open.char_indices().peekable();
        let mut end = None;
        while let Some((i, c)) = chars.next() {
            let next = chars.peek().map(|&(_, c)| c);
            if c == '/' && next == Some('*') {
                depth += 1;
                chars.next();
            } else if c == '*' && next == Some('/') {
                depth -= 1;
                chars.next();
                if depth == 0 {
                    end = Some(i + 2);
                    break;
                }
            }
        }
        match end {
            Some(end) => rest = after_open[end..].trim_start(),
            None => return true, // unterminated comment swallows the rest
        }
    }
    rest.is_empty()
}

/// Byte offset of the `i`-th character of `s`.
fn byte_at(s: &str, i: usize) -> usize {
    s.char_indices()
        .nth(i)
        .map(|(b, _)| b)
        .unwrap_or_else(|| s.len())
}

/// Scans a dollar-quote delimiter (`$`, optional identifier tag, `$`)
/// starting at `chars[0] == '$'`. Returns the full delimiter text.
fn scan_dollar_tag(chars: &[char]) -> Option<String> {
    debug_assert_eq!(chars[0], '$');
    let mut tag = String::from("$");
    for &c in &chars[1..] {
        if c == '$' {
            tag.push('$');
            return Some(tag);
        }
        if c.is_alphanumeric() || c == '_' {
            tag.push(c);
        } else {
            return None;
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Result rendering: psql's aligned output format
// ---------------------------------------------------------------------------

struct Column {
    name: String,
    type_oid: u32,
}

struct Table {
    columns: Vec<Column>,
    rows: Vec<Vec<Option<String>>>,
}

impl Table {
    fn from_row_description(body: &[u8]) -> Result<Table> {
        let mut r = Reader::new(body);
        let nfields = r.u16()? as usize;
        let mut columns = Vec::with_capacity(nfields);
        for _ in 0..nfields {
            let name = r.cstring()?;
            r.skip(4 + 2)?; // table oid, attnum
            let type_oid = r.u32()?;
            r.skip(2 + 4 + 2)?; // typlen, typmod, format
            columns.push(Column { name, type_oid });
        }
        Ok(Table {
            columns,
            rows: Vec::new(),
        })
    }

    /// Renders in psql's default aligned format with border 1: centered
    /// headers, a dashed separator, per-type value alignment, a row-count
    /// footer, and a trailing blank line. Data lines are right-trimmed;
    /// header and separator lines are not (matching psql exactly).
    fn render(&self, opts: &RenderOpts, out: &mut String) {
        if opts.expanded {
            return self.render_expanded(opts, out);
        }
        let display = |value: &Option<String>| -> String {
            value.clone().unwrap_or_else(|| opts.null_display.clone())
        };
        let widths: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                self.rows
                    .iter()
                    .map(|row| display(&row[i]).chars().count())
                    .max()
                    .unwrap_or(0)
                    .max(col.name.chars().count())
            })
            .collect();

        let header: Vec<String> = self
            .columns
            .iter()
            .zip(&widths)
            .map(|(col, &w)| {
                let pad = w - col.name.chars().count();
                format!(
                    " {}{}{} ",
                    " ".repeat(pad / 2),
                    col.name,
                    " ".repeat(pad - pad / 2)
                )
            })
            .collect();
        out.push_str(&header.join("|"));
        out.push('\n');

        let sep: Vec<String> = widths.iter().map(|&w| "-".repeat(w + 2)).collect();
        out.push_str(&sep.join("+"));
        out.push('\n');

        for row in &self.rows {
            let cells: Vec<String> = row
                .iter()
                .zip(&self.columns)
                .zip(&widths)
                .map(|((value, col), &w)| {
                    let v = display(value);
                    let pad = " ".repeat(w - v.chars().count());
                    if right_aligned(col.type_oid) {
                        format!(" {pad}{v} ")
                    } else {
                        format!(" {v}{pad} ")
                    }
                })
                .collect();
            let line = cells.join("|");
            out.push_str(line.trim_end());
            out.push('\n');
        }

        let n = self.rows.len();
        out.push_str(&format!("({n} row{})\n\n", if n == 1 { "" } else { "s" }));
    }

    /// `\x` expanded format: one `-[ RECORD n ]` block per row, field names
    /// down the left. Data lines keep psql's `name | value` shape untrimmed.
    fn render_expanded(&self, opts: &RenderOpts, out: &mut String) {
        if self.rows.is_empty() {
            out.push_str("(0 rows)\n\n");
            return;
        }
        let name_width = self
            .columns
            .iter()
            .map(|c| c.name.chars().count())
            .max()
            .unwrap_or(0);
        let value_width = self
            .rows
            .iter()
            .flatten()
            .map(|v| v.as_deref().unwrap_or(&opts.null_display).chars().count())
            .max()
            .unwrap_or(0);
        for (n, row) in self.rows.iter().enumerate() {
            let label = format!("-[ RECORD {} ]", n + 1);
            let left_pad = (name_width + 1).saturating_sub(label.chars().count());
            out.push_str(&format!(
                "{label}{}+{}\n",
                "-".repeat(left_pad),
                "-".repeat(value_width + 1)
            ));
            for (col, value) in self.columns.iter().zip(row) {
                let v = value.as_deref().unwrap_or(&opts.null_display);
                let pad = " ".repeat(name_width - col.name.chars().count());
                out.push_str(&format!("{}{pad} | {v}\n", col.name));
            }
        }
        out.push('\n');
    }
}

/// psql right-aligns numeric types (see `column_type_alignment` in
/// `fe_utils/print.c`); everything else, including bool, is left-aligned.
fn right_aligned(type_oid: u32) -> bool {
    matches!(
        type_oid,
        20 | 21 | 23 // int8, int2, int4
        | 26 | 28 | 29 | 5069 // oid, xid, cid, xid8
        | 700 | 701 // float4, float8
        | 790 // money
        | 1700 // numeric
    )
}

fn parse_data_row(body: &[u8]) -> Result<Vec<Option<String>>> {
    let mut r = Reader::new(body);
    let ncols = r.u16()? as usize;
    let mut row = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let len = r.i32()?;
        if len < 0 {
            row.push(None);
        } else {
            let bytes = r.bytes(len as usize)?;
            row.push(Some(String::from_utf8_lossy(bytes).into_owned()));
        }
    }
    Ok(row)
}

/// Renders an ErrorResponse/NoticeResponse the way psql does in its default
/// verbosity: `SEVERITY:  message`, then the `LINE n:` excerpt with a caret
/// when the server reported a statement position, then detail/hint/context.
/// CONTEXT rendering follows `\set SHOW_CONTEXT`: by default only messages
/// of ERROR severity or worse include it.
fn format_error(fields: &HashMap<u8, String>, query: &str, opts: &RenderOpts, out: &mut String) {
    let severity = fields.get(&b'S').map(String::as_str).unwrap_or("ERROR");
    let message = fields.get(&b'M').map(String::as_str).unwrap_or("");
    out.push_str(&format!("{severity}:  {message}\n"));

    if let Some(pos) = fields.get(&b'P').and_then(|p| p.parse::<usize>().ok()) {
        let chars: Vec<char> = query.chars().collect();
        let pos0 = pos.saturating_sub(1).min(chars.len());
        let mut line_no = 1;
        let mut line_start = 0;
        for (i, &c) in chars[..pos0].iter().enumerate() {
            if c == '\n' {
                line_no += 1;
                line_start = i + 1;
            }
        }
        let line: String = chars[line_start..]
            .iter()
            .take_while(|&&c| c != '\n')
            .collect();
        let prefix = format!("LINE {line_no}: ");
        let caret_col = prefix.chars().count() + (pos0 - line_start);
        out.push_str(&format!("{prefix}{line}\n"));
        out.push_str(&format!("{}^\n", " ".repeat(caret_col)));
    }

    let show_context = match opts.show_context {
        ShowContext::Always => true,
        ShowContext::Errors => matches!(severity, "ERROR" | "FATAL" | "PANIC"),
        ShowContext::Never => false,
    };
    for (field, label) in [(b'D', "DETAIL"), (b'H', "HINT"), (b'W', "CONTEXT")] {
        if field == b'W' && !show_context {
            continue;
        }
        if let Some(text) = fields.get(&field) {
            out.push_str(&format!("{label}:  {text}\n"));
        }
    }
}

fn parse_error_fields(body: &[u8]) -> Result<HashMap<u8, String>> {
    let mut r = Reader::new(body);
    let mut fields = HashMap::new();
    loop {
        let code = r.u8()?;
        if code == 0 {
            return Ok(fields);
        }
        fields.insert(code, r.cstring()?);
    }
}

// ---------------------------------------------------------------------------
// Wire protocol client
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ConnParams {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    database: String,
}

impl ConnParams {
    /// Parses a `postgres://[user[:password]@]host[:port][/database]` DSN.
    fn parse(dsn: &str) -> Result<ConnParams> {
        let rest = dsn
            .strip_prefix("postgres://")
            .or_else(|| dsn.strip_prefix("postgresql://"))
            .with_context(|| format!("DSN must start with postgres://, got {dsn}"))?;
        let (authority, database) = match rest.split_once('/') {
            Some((a, db)) => (a, db.split('?').next().unwrap_or(db)),
            None => (rest, "regression"),
        };
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, authority),
        };
        let (user, password) = match userinfo {
            Some(u) => match u.split_once(':') {
                Some((user, pass)) => (user.to_string(), Some(pass.to_string())),
                None => (u.to_string(), None),
            },
            None => (
                std::env::var("USER").unwrap_or_else(|_| "postgres".to_string()),
                None,
            ),
        };
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.parse().with_context(|| format!("bad port in DSN: {p}"))?,
            ),
            None => (hostport.to_string(), 5432),
        };
        Ok(ConnParams {
            host,
            port,
            user,
            password,
            database: database.to_string(),
        })
    }
}

struct PgConn {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl PgConn {
    /// Connects and performs the startup handshake. The startup parameters
    /// replicate pg_regress's psql environment (PGTZ, PGDATESTYLE, PGOPTIONS,
    /// PGAPPNAME) so transcripts are byte-reproducible.
    fn connect(params: &ConnParams, test_name: &str) -> Result<PgConn> {
        let stream = TcpStream::connect((params.host.as_str(), params.port))
            .with_context(|| format!("connecting to {}:{}", params.host, params.port))?;
        // A statement that gets no reply within this window means the server
        // is wedged (e.g. a panic poisoned its connection state). Fail the
        // test instead of hanging the whole run; regress statements finish
        // in milliseconds, so this is generous while keeping the worst-case
        // cost of a wedged server bounded.
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(15)))
            .context("setting read timeout")?;
        let reader = BufReader::new(stream.try_clone().context("cloning stream")?);
        let writer = BufWriter::new(stream);
        let mut conn = PgConn { reader, writer };

        let mut startup = Vec::new();
        startup.extend_from_slice(&196608u32.to_be_bytes()); // protocol 3.0
        for (k, v) in [
            ("user", params.user.as_str()),
            ("database", params.database.as_str()),
            ("application_name", &format!("pg_regress/{test_name}")),
            ("options", "-c intervalstyle=postgres_verbose"),
            ("datestyle", "Postgres, MDY"),
            ("timezone", "America/Los_Angeles"),
        ] {
            startup.extend_from_slice(k.as_bytes());
            startup.push(0);
            startup.extend_from_slice(v.as_bytes());
            startup.push(0);
        }
        startup.push(0);
        conn.writer
            .write_all(&((startup.len() as u32 + 4).to_be_bytes()))?;
        conn.writer.write_all(&startup)?;
        conn.writer.flush()?;

        loop {
            let (tag, body) = conn.read_message()?;
            match tag {
                b'R' => {
                    let mut r = Reader::new(&body);
                    match r.u32()? {
                        0 => {} // AuthenticationOk
                        3 => {
                            // Cleartext password.
                            let password = params
                                .password
                                .as_deref()
                                .context("server requested a password but DSN has none")?;
                            let mut msg = Vec::from(password.as_bytes());
                            msg.push(0);
                            conn.write_message(b'p', &msg)?;
                        }
                        method => bail!("unsupported authentication method {method}"),
                    }
                }
                b'S' | b'K' | b'N' => {} // ParameterStatus, BackendKeyData, notices
                b'Z' => return Ok(conn),
                b'E' => {
                    let fields = parse_error_fields(&body)?;
                    bail!(
                        "server rejected connection: {}",
                        fields.get(&b'M').map(String::as_str).unwrap_or("unknown")
                    );
                }
                other => bail!("unexpected message {:?} during startup", other as char),
            }
        }
    }

    fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut body = Vec::from(sql.as_bytes());
        body.push(0);
        self.write_message(b'Q', &body)
    }

    fn write_message(&mut self, tag: u8, body: &[u8]) -> Result<()> {
        self.writer.write_all(&[tag])?;
        self.writer
            .write_all(&((body.len() as u32 + 4).to_be_bytes()))?;
        self.writer.write_all(body)?;
        self.writer.flush()?;
        Ok(())
    }

    fn read_message(&mut self) -> Result<(u8, Vec<u8>)> {
        let mut header = [0u8; 5];
        self.reader
            .read_exact(&mut header)
            .context("reading message header (server closed the connection?)")?;
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        if len < 4 {
            bail!("invalid message length {len}");
        }
        let mut body = vec![0u8; len - 4];
        self.reader
            .read_exact(&mut body)
            .context("reading message body")?;
        Ok((header[0], body))
    }
}

/// Cursor over a message body.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.buf.len());
        let end = end.context("truncated message")?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn skip(&mut self, n: usize) -> Result<()> {
        self.bytes(n).map(|_| ())
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn cstring(&mut self) -> Result<String> {
        let start = self.pos;
        let nul = self.buf[start..]
            .iter()
            .position(|&b| b == 0)
            .context("unterminated string in message")?;
        let s = String::from_utf8_lossy(&self.buf[start..start + nul]).into_owned();
        self.pos = start + nul + 1;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn feed(scanner: &mut Scanner, line: &str, vars: &HashMap<String, String>) -> Vec<Item> {
        scanner.feed_line(line, vars).unwrap()
    }

    #[test]
    fn statement_interpolates_plain_quoted_and_identifier_variables() {
        let vars = vars(&[("filename", "/tmp/agg.data"), ("col", "f1")]);
        let mut scanner = Scanner::default();
        let items = feed(&mut scanner, "COPY onek FROM :'filename';", &vars);
        match items.as_slice() {
            [Item::Statement(s)] => assert_eq!(s, "COPY onek FROM '/tmp/agg.data';"),
            _ => panic!("expected one statement"),
        }
        let items = feed(&mut scanner, "SELECT :\"col\" FROM t;", &vars);
        match items.as_slice() {
            [Item::Statement(s)] => assert_eq!(s, "SELECT \"f1\" FROM t;"),
            _ => panic!("expected one statement"),
        }
    }

    #[test]
    fn double_colon_casts_and_unknown_variables_stay_literal() {
        let vars = vars(&[("int", "BOOM")]);
        let mut scanner = Scanner::default();
        let items = feed(&mut scanner, "SELECT 1::int, :missing;", &vars);
        match items.as_slice() {
            [Item::Statement(s)] => assert_eq!(s, "SELECT 1::int, :missing;"),
            _ => panic!("expected one statement"),
        }
    }

    #[test]
    fn variables_inside_string_literals_are_not_interpolated() {
        let vars = vars(&[("x", "BOOM")]);
        let mut scanner = Scanner::default();
        let items = feed(&mut scanner, "SELECT ':x';", &vars);
        match items.as_slice() {
            [Item::Statement(s)] => assert_eq!(s, "SELECT ':x';"),
            _ => panic!("expected one statement"),
        }
    }

    #[test]
    fn trailing_gset_becomes_meta_command_and_keeps_the_buffer() {
        let vars = HashMap::new();
        let mut scanner = Scanner::default();
        let items = feed(&mut scanner, "SELECT 1 AS x \\gset", &vars);
        match items.as_slice() {
            [Item::MetaCommand(cmd)] => assert_eq!(cmd, "\\gset"),
            _ => panic!("expected one meta-command"),
        }
        assert_eq!(scanner.take_buffer(), "SELECT 1 AS x");
    }

    #[test]
    fn backslash_inside_quotes_does_not_start_a_meta_command() {
        let vars = HashMap::new();
        let mut scanner = Scanner::default();
        let items = feed(&mut scanner, "SELECT E'a\\n', '\\x';", &vars);
        match items.as_slice() {
            [Item::Statement(s)] => assert_eq!(s, "SELECT E'a\\n', '\\x';"),
            _ => panic!("expected one statement"),
        }
    }

    #[test]
    fn set_concatenates_value_arguments_without_separator() {
        let vars = vars(&[("libdir", "/lib"), ("dlsuffix", ".so")]);
        let args = split_meta_args(":libdir '/regress' :dlsuffix", &vars);
        assert_eq!(args, ["/lib", "/regress", ".so"]);
        assert_eq!(args.concat(), "/lib/regress.so");
    }

    #[test]
    fn meta_args_unescape_single_quoted_strings() {
        let none = HashMap::new();
        assert_eq!(split_meta_args("null '\\\\N'", &none), ["null", "\\N"]);
        assert_eq!(split_meta_args("null ''", &none), ["null", ""]);
        assert_eq!(split_meta_args("null NULL", &none), ["null", "NULL"]);
    }

    #[test]
    fn trailing_comment_only_buffer_is_not_sent_at_eof() {
        let vars = HashMap::new();
        let mut scanner = Scanner::default();
        feed(&mut scanner, "/* and this is", &vars);
        feed(&mut scanner, "the end of the file */", &vars);
        assert_eq!(scanner.take_rest(), None);

        let mut scanner = Scanner::default();
        feed(&mut scanner, "SELECT 1 /* trailing */", &vars);
        assert_eq!(
            scanner.take_rest().as_deref(),
            Some("SELECT 1 /* trailing */")
        );
    }

    #[test]
    fn parse_bool_accepts_psql_prefixes() {
        assert_eq!(parse_bool("t"), Some(true));
        assert_eq!(parse_bool("f"), Some(false));
        assert_eq!(parse_bool("on"), Some(true));
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("yes"), Some(true));
        assert_eq!(parse_bool(":unset_var"), None);
    }

    #[test]
    fn copy_spec_splits_table_query_and_options() {
        let (before, dir, target, options) =
            split_copy_spec("y TO stdout (FORMAT CSV, DELIMITER '|')").unwrap();
        assert_eq!((before.as_str(), dir.as_str()), ("y", "TO"));
        assert_eq!(target, "stdout");
        assert_eq!(options, "(FORMAT CSV, DELIMITER '|')");

        let (before, dir, target, _) =
            split_copy_spec("(select * from t where 'x from y' <> a) to stdout").unwrap();
        assert_eq!(before, "(select * from t where 'x from y' <> a)");
        assert_eq!(dir, "to");
        assert_eq!(target, "stdout");

        let (_, dir, target, _) = split_copy_spec("t from '/tmp/data.csv' with csv").unwrap();
        assert_eq!(dir, "from");
        assert_eq!(target, "/tmp/data.csv");
    }
}
