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
//! Known emulation gaps, to be filled as the corpus grows to need them:
//! psql meta-commands other than `\pset null` are rejected rather than
//! interpreted,
//! multi-line field values are not rendered with `+` continuation markers,
//! long error-position lines are not clipped with `...`, and column widths
//! count characters rather than terminal display width.

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

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

fn run(args: &Args) -> Result<bool> {
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
    scripts.sort();
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

    let mut failed = 0usize;
    for script in &scripts {
        if !run_script(args, &params, script)? {
            failed += 1;
        }
    }

    let passed = scripts.len() - failed;
    println!("\n{passed} of {} test(s) passed", scripts.len());
    Ok(failed == 0)
}

/// Collects `.sql` scripts from a file or directory (non-recursive).
fn collect_scripts(path: &Path, scripts: &mut Vec<PathBuf>) -> Result<()> {
    let meta = std::fs::metadata(path)
        .with_context(|| format!("no such file or directory: {}", path.display()))?;
    if meta.is_file() {
        scripts.push(path.to_path_buf());
        return Ok(());
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() && p.extension().is_some_and(|e| e == "sql") {
            scripts.push(p);
        }
    }
    Ok(())
}

/// Runs one script and compares against its expected transcript.
/// Returns whether the test passed.
fn run_script(args: &Args, params: &ConnParams, script: &Path) -> Result<bool> {
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
    let actual = run_transcript(params, name, &source)
        .with_context(|| format!("running {}", script.display()))?;
    let elapsed = start.elapsed();

    let actual_path = args.results.join(format!("{name}.out"));
    std::fs::write(&actual_path, &actual)
        .with_context(|| format!("writing {}", actual_path.display()))?;

    if actual == expected {
        println!("ok ({} ms)", elapsed.as_millis());
        return Ok(true);
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
    Ok(false)
}

/// Executes one script on a fresh connection (pg_regress runs one psql per
/// script) and returns the psql-style transcript.
fn run_transcript(params: &ConnParams, name: &str, source: &str) -> Result<String> {
    let mut conn = PgConn::connect(params, name)?;
    let mut out = String::new();
    let mut scanner = Scanner::default();
    let mut opts = RenderOpts::default();

    for line in source.split_inclusive('\n') {
        // psql ignores empty input lines entirely — no echo, no newline in
        // the query buffer — unless they continue a quoted string.
        if line.strip_suffix('\n').unwrap_or(line).is_empty() && !scanner.in_quote() {
            continue;
        }
        // psql's echo-all prints each input line as it is read, before any
        // statement completed on that line executes.
        out.push_str(line);
        if !line.ends_with('\n') {
            out.push('\n');
        }
        for item in scanner.feed_line(line.strip_suffix('\n').unwrap_or(line))? {
            match item {
                Item::Statement(stmt) => execute(&mut conn, &stmt, &opts, &mut out)?,
                Item::MetaCommand(cmd) => apply_meta_command(&cmd, &mut opts)?,
            }
        }
    }
    // psql executes whatever is left in the query buffer at EOF even without
    // a terminating semicolon.
    if let Some(stmt) = scanner.take_rest() {
        execute(&mut conn, &stmt, &opts, &mut out)?;
    }
    Ok(out)
}

/// Rendering state mutable via psql meta-commands.
#[derive(Default)]
struct RenderOpts {
    /// `\pset null` — the display string for NULL values (default empty).
    null_display: String,
}

/// Interprets the psql meta-commands the corpus uses. `\pset` produces no
/// output in quiet mode, so successful application only mutates `opts`.
fn apply_meta_command(cmd: &str, opts: &mut RenderOpts) -> Result<()> {
    let args: Vec<&str> = cmd.split_whitespace().collect();
    match args.as_slice() {
        ["\\pset", "null", value] => {
            opts.null_display = value
                .strip_prefix('\'')
                .and_then(|v| v.strip_suffix('\''))
                .unwrap_or(value)
                .to_string();
            Ok(())
        }
        _ => bail!("unsupported psql meta-command: {cmd}"),
    }
}

/// Sends one statement and appends its psql-rendered results to `out`.
fn execute(conn: &mut PgConn, stmt: &str, opts: &RenderOpts, out: &mut String) -> Result<()> {
    conn.send_query(stmt)?;
    let mut table: Option<Table> = None;
    loop {
        let (tag, body) = conn.read_message()?;
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
                    table.render(opts, out);
                }
            }
            b'E' => {
                table = None;
                format_error(&parse_error_fields(&body)?, stmt, out);
            }
            b'N' => format_error(&parse_error_fields(&body)?, stmt, out),
            b'I' => {} // EmptyQueryResponse
            b'Z' => return Ok(()),
            b'S' | b'K' | b'A' => {} // ParameterStatus, BackendKeyData, notifications
            other => bail!(
                "unexpected message {:?} while executing query",
                other as char
            ),
        }
    }
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
    /// and meta-commands it completed, in order.
    fn feed_line(&mut self, line: &str) -> Result<Vec<Item>> {
        if self.state == LexState::Normal
            && self.dollar_tag.is_none()
            && self.buf.trim().is_empty()
            && line.trim_start().starts_with('\\')
        {
            self.buf.clear();
            return Ok(vec![Item::MetaCommand(line.trim().to_string())]);
        }

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

    /// Returns the unterminated trailing statement at EOF, if any.
    fn take_rest(&mut self) -> Option<String> {
        let rest = std::mem::take(&mut self.buf);
        let trimmed = rest.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }
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
fn format_error(fields: &HashMap<u8, String>, query: &str, out: &mut String) {
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

    for (field, label) in [(b'D', "DETAIL"), (b'H', "HINT"), (b'W', "CONTEXT")] {
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
