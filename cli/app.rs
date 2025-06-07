use crate::{
    commands::{
        args::{EchoMode, TimerMode},
        import::ImportFile,
        Command, CommandParser,
    },
    config::Config,
    helper::LimboHelper,
    input::{get_io, get_writer, DbLocation, OutputMode, Settings},
    opcodes_dictionary::OPCODE_DESCRIPTIONS,
    HISTORY_FILE,
};
use comfy_table::{Attribute, Cell, CellAlignment, ContentArrangement, Row, Table};
use limbo_core::{Database, LimboError, Statement, StepResult, Value};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use clap::Parser;
use rustyline::{error::ReadlineError, history::DefaultHistory, Editor};
use std::{
    fmt,
    io::{self, BufRead as _, Write},
    path::PathBuf,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[derive(Parser)]
#[command(name = "limbo")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    #[clap(index = 1, help = "SQLite database file", default_value = ":memory:")]
    pub database: Option<PathBuf>,
    #[clap(index = 2, help = "Optional SQL command to execute")]
    pub sql: Option<String>,
    #[clap(short = 'm', long, default_value_t = OutputMode::Pretty)]
    pub output_mode: OutputMode,
    #[clap(short, long, default_value = "")]
    pub output: String,
    #[clap(
        short,
        long,
        help = "don't display program information on start",
        default_value_t = false
    )]
    pub quiet: bool,
    #[clap(short, long, help = "Print commands before execution")]
    pub echo: bool,
    #[clap(
        short = 'v',
        long,
        help = "Select VFS. options are io_uring (if feature enabled), memory, and syscall"
    )]
    pub vfs: Option<String>,
    #[clap(long, help = "Enable experimental MVCC feature")]
    pub experimental_mvcc: bool,
    #[clap(short = 't', long, help = "specify output file for log traces")]
    pub tracing_output: Option<String>,
}

const PROMPT: &str = "limbo> ";

pub struct Limbo {
    pub prompt: String,
    io: Arc<dyn limbo_core::IO>,
    writer: Box<dyn Write>,
    conn: Rc<limbo_core::Connection>,
    pub interrupt_count: Arc<AtomicUsize>,
    input_buff: String,
    opts: Settings,
    pub rl: Option<Editor<LimboHelper, DefaultHistory>>,
    config: Option<Config>,
}

struct QueryStatistics {
    io_time_elapsed_samples: Vec<Duration>,
    execute_time_elapsed_samples: Vec<Duration>,
}

macro_rules! query_internal {
    ($self:expr, $query:expr, $body:expr) => {{
        let rows = $self.conn.query($query)?;
        if let Some(mut rows) = rows {
            loop {
                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        $body(row)?;
                    }
                    StepResult::IO => {
                        $self.io.run_once()?;
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => {
                        Err(LimboError::InternalError("database is busy".into()))?;
                    }
                }
            }
        }
        Ok::<(), LimboError>(())
    }};
}

impl Limbo {
    pub fn new() -> anyhow::Result<Self> {
        let opts = Opts::parse();
        let db_file = opts
            .database
            .as_ref()
            .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string());
        let (io, db) = if let Some(ref vfs) = opts.vfs {
            Database::open_new(&db_file, vfs)?
        } else {
            let io = {
                match db_file.as_str() {
                    ":memory:" => get_io(
                        DbLocation::Memory,
                        opts.vfs.as_ref().map_or("", |s| s.as_str()),
                    )?,
                    _path => get_io(
                        DbLocation::Path,
                        opts.vfs.as_ref().map_or("", |s| s.as_str()),
                    )?,
                }
            };
            (
                io.clone(),
                Database::open_file(io.clone(), &db_file, opts.experimental_mvcc)?,
            )
        };
        let conn = db.connect()?;
        let interrupt_count = Arc::new(AtomicUsize::new(0));
        {
            let interrupt_count: Arc<AtomicUsize> = Arc::clone(&interrupt_count);
            ctrlc::set_handler(move || {
                // Increment the interrupt count on Ctrl-C
                interrupt_count.fetch_add(1, Ordering::SeqCst);
            })
            .expect("Error setting Ctrl-C handler");
        }
        let sql = opts.sql.clone();
        let quiet = opts.quiet;
        let mut app = Self {
            prompt: PROMPT.to_string(),
            io,
            writer: get_writer(&opts.output),
            conn,
            interrupt_count,
            input_buff: String::new(),
            opts: Settings::from(opts),
            rl: None,
            config: Some(Config::default()),
        };
        app.first_run(sql, quiet)?;
        Ok(app)
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_readline(mut self, mut rl: Editor<LimboHelper, DefaultHistory>) -> Self {
        let h = LimboHelper::new(
            self.conn.clone(),
            self.io.clone(),
            self.config.as_ref().map(|c| c.highlight.clone()),
        );
        rl.set_helper(Some(h));
        self.rl = Some(rl);
        self
    }

    fn first_run(&mut self, sql: Option<String>, quiet: bool) -> Result<(), LimboError> {
        if let Some(sql) = sql {
            self.handle_first_input(&sql)?;
        }
        if !quiet {
            self.write_fmt(format_args!("Limbo v{}", env!("CARGO_PKG_VERSION")))?;
            self.writeln("Enter \".help\" for usage hints.")?;
            self.display_in_memory()?;
        }
        Ok(())
    }

    fn handle_first_input(&mut self, cmd: &str) -> Result<(), LimboError> {
        if cmd.trim().starts_with('.') {
            self.handle_dot_command(&cmd[1..]);
        } else {
            self.run_query(cmd);
        }
        self.close_conn()?;
        std::process::exit(0);
    }

    fn set_multiline_prompt(&mut self) {
        self.prompt = match self.input_buff.chars().fold(0, |acc, c| match c {
            '(' => acc + 1,
            ')' => acc - 1,
            _ => acc,
        }) {
            n if n < 0 => String::from(")x!...>"),
            0 => String::from("   ...> "),
            n if n < 10 => format!("(x{}...> ", n),
            _ => String::from("(.....> "),
        };
    }

    #[cfg(not(target_family = "wasm"))]
    fn handle_load_extension(&mut self, path: &str) -> Result<(), String> {
        let ext_path = limbo_core::resolve_ext_path(path).map_err(|e| e.to_string())?;
        self.conn
            .load_extension(ext_path)
            .map_err(|e| e.to_string())
    }

    fn dump_table(&mut self, name: &str) -> Result<(), LimboError> {
        let query = format!("pragma table_info={}", name);
        let mut cols = vec![];
        let mut value_types = vec![];
        query_internal!(
            self,
            query,
            |row: &limbo_core::Row| -> Result<(), LimboError> {
                let name: &str = row.get::<&str>(1)?;
                cols.push(name.to_string());
                let value_type: &str = row.get::<&str>(2)?;
                value_types.push(value_type.to_string());
                Ok(())
            }
        )?;
        // FIXME: sqlite has logic to check rowid and optionally preserve
        // it, but it requires pragma index_list, and it seems to be relevant
        // only for indexes.
        let cols_str = cols.join(", ");
        let select = format!("select {} from {}", cols_str, name);
        query_internal!(
            self,
            select,
            |row: &limbo_core::Row| -> Result<(), LimboError> {
                let values = row
                    .get_values()
                    .zip(value_types.iter())
                    .map(|(value, value_type)| {
                        // If the type affinity is TEXT, replace each single
                        // quotation mark with two single quotation marks, and
                        // wrap it with single quotation marks.
                        if value_type.contains("CHAR")
                            || value_type.contains("CLOB")
                            || value_type.contains("TEXT")
                        {
                            format!("'{}'", value.to_string().replace("'", "''"))
                        } else if value_type.contains("BLOB") {
                            let blob = value.to_blob().unwrap_or(&[]);
                            let hex_string: String =
                                blob.iter().fold(String::new(), |mut output, b| {
                                    let _ =
                                        fmt::Write::write_fmt(&mut output, format_args!("{b:02x}"));
                                    output
                                });
                            format!("X'{}'", hex_string)
                        } else {
                            value.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                self.write_fmt(format_args!("INSERT INTO {} VALUES({});", name, values))?;
                Ok(())
            }
        )?;
        Ok(())
    }

    fn dump_database(&mut self) -> anyhow::Result<()> {
        self.writeln("PRAGMA foreign_keys=OFF;")?;
        self.writeln("BEGIN TRANSACTION;")?;
        // FIXME: At this point, SQLite executes the following:
        // sqlite3_exec(p->db, "SAVEPOINT dump; PRAGMA writable_schema=ON", 0, 0, 0);
        // we don't have those yet, so don't.
        let query = r#"
    SELECT name, type, sql
    FROM sqlite_schema AS o
    WHERE type == 'table'
        AND sql NOT NULL
    ORDER BY tbl_name = 'sqlite_sequence', rowid"#;

        let res = query_internal!(
            self,
            query,
            |row: &limbo_core::Row| -> Result<(), LimboError> {
                let sql: &str = row.get::<&str>(2)?;
                let name: &str = row.get::<&str>(0)?;
                self.write_fmt(format_args!("{};", sql))?;
                self.dump_table(name)
            }
        );

        match res {
            Ok(_) => Ok(()),
            Err(LimboError::Corrupt(x)) => {
                // FIXME: SQLite at this point retry the query with a different
                // order by, but for simplicity we are just ignoring for now
                self.writeln("/****** CORRUPTION ERROR *******/")?;
                Err(LimboError::Corrupt(x))
            }
            Err(x) => Err(x),
        }?;

        self.conn.close()?;
        self.writeln("COMMIT;")?;
        Ok(())
    }

    fn display_in_memory(&mut self) -> io::Result<()> {
        if self.opts.db_file == ":memory:" {
            self.writeln("Connected to a transient in-memory database.")?;
            self.writeln("Use \".open FILENAME\" to reopen on a persistent database")?;
        }
        Ok(())
    }

    fn show_info(&mut self) -> io::Result<()> {
        let opts = format!("{}", self.opts);
        self.writeln(opts)
    }

    pub fn reset_input(&mut self) {
        self.prompt = PROMPT.to_string();
        self.input_buff.clear();
    }

    pub fn close_conn(&mut self) -> Result<(), LimboError> {
        self.conn.close()
    }

    fn toggle_echo(&mut self, arg: EchoMode) {
        match arg {
            EchoMode::On => self.opts.echo = true,
            EchoMode::Off => self.opts.echo = false,
        }
    }

    fn open_db(&mut self, path: &str, vfs_name: Option<&str>) -> anyhow::Result<()> {
        self.conn.close()?;
        let (io, db) = if let Some(vfs_name) = vfs_name {
            self.conn.open_new(path, vfs_name)?
        } else {
            let io = {
                match path {
                    ":memory:" => get_io(DbLocation::Memory, &self.opts.io.to_string())?,
                    _path => get_io(DbLocation::Path, &self.opts.io.to_string())?,
                }
            };
            (io.clone(), Database::open_file(io.clone(), path, false)?)
        };
        self.io = io;
        self.conn = db.connect()?;
        self.opts.db_file = path.to_string();
        Ok(())
    }

    fn set_output_file(&mut self, path: &str) -> Result<(), String> {
        if path.is_empty() || path.trim().eq_ignore_ascii_case("stdout") {
            self.set_output_stdout();
            return Ok(());
        }
        match std::fs::File::create(path) {
            Ok(file) => {
                self.writer = Box::new(file);
                self.opts.is_stdout = false;
                self.opts.output_mode = OutputMode::List;
                self.opts.output_filename = path.to_string();
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn set_output_stdout(&mut self) {
        let _ = self.writer.flush();
        self.writer = Box::new(io::stdout());
        self.opts.is_stdout = true;
    }

    fn set_mode(&mut self, mode: OutputMode) -> Result<(), String> {
        if mode == OutputMode::Pretty && !self.opts.is_stdout {
            Err("pretty output can only be written to a tty".to_string())
        } else {
            self.opts.output_mode = mode;
            Ok(())
        }
    }

    fn write_fmt(&mut self, fmt: std::fmt::Arguments) -> io::Result<()> {
        let _ = self.writer.write_fmt(fmt);
        self.writer.write_all(b"\n")
    }

    fn writeln<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.write_all(data.as_ref())?;
        self.writer.write_all(b"\n")
    }

    fn buffer_input(&mut self, line: &str) {
        self.input_buff.push_str(line);
        self.input_buff.push(' ');
    }

    fn run_query(&mut self, input: &str) {
        let echo = self.opts.echo;
        if echo {
            let _ = self.writeln(input);
        }

        let start = Instant::now();
        let mut stats = QueryStatistics {
            io_time_elapsed_samples: vec![],
            execute_time_elapsed_samples: vec![],
        };
        // TODO this is a quickfix. Some ideas to do case insensitive comparisons is to use
        // Uncased or Unicase.
        let temp = input.to_lowercase();
        if temp.trim_start().starts_with("explain") {
            match self.conn.query(input) {
                Ok(Some(stmt)) => {
                    let _ = self.writeln(stmt.explain().as_bytes());
                }
                Err(e) => {
                    let _ = self.writeln(e.to_string());
                }
                _ => {}
            }
        } else {
            let conn = self.conn.clone();
            let runner = conn.query_runner(input.as_bytes());
            for output in runner {
                if self
                    .print_query_result(input, output, Some(&mut stats))
                    .is_err()
                {
                    break;
                }
            }
        }
        self.print_query_performance_stats(start, stats);
        self.reset_input();
    }

    fn print_query_performance_stats(&mut self, start: Instant, stats: QueryStatistics) {
        let elapsed_as_str = |duration: Duration| {
            if duration.as_secs() >= 1 {
                format!("{} s", duration.as_secs_f64())
            } else if duration.as_millis() >= 1 {
                format!("{} ms", duration.as_millis() as f64)
            } else if duration.as_micros() >= 1 {
                format!("{} us", duration.as_micros() as f64)
            } else {
                format!("{} ns", duration.as_nanos())
            }
        };
        let sample_stats_as_str = |name: &str, samples: Vec<Duration>| {
            if samples.is_empty() {
                return format!("{}: No samples available", name);
            }
            let avg_time_spent = samples.iter().sum::<Duration>() / samples.len() as u32;
            let total_time = samples.iter().fold(Duration::ZERO, |acc, x| acc + *x);
            format!(
                "{}: avg={}, total={}",
                name,
                elapsed_as_str(avg_time_spent),
                elapsed_as_str(total_time),
            )
        };
        if self.opts.timer {
            let _ = self.writeln("Command stats:\n----------------------------");
            let _ = self.writeln(format!(
                "total: {} (this includes parsing/coloring of cli app)\n",
                elapsed_as_str(start.elapsed())
            ));

            let _ = self.writeln("query execution stats:\n----------------------------");
            let _ = self.writeln(sample_stats_as_str(
                "Execution",
                stats.execute_time_elapsed_samples,
            ));
            let _ = self.writeln(sample_stats_as_str("I/O", stats.io_time_elapsed_samples));
        }
    }

    fn reset_line(&mut self, _line: &str) -> rustyline::Result<()> {
        // Entry is auto added to history
        // self.rl.add_history_entry(line.to_owned())?;
        self.interrupt_count.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub fn handle_input_line(&mut self, line: &str) -> anyhow::Result<()> {
        if self.input_buff.is_empty() {
            if line.is_empty() {
                return Ok(());
            }
            if line.starts_with('.') {
                self.handle_dot_command(&line[1..]);
                let _ = self.reset_line(line);
                return Ok(());
            }
        }
        if line.trim_start().starts_with("--") {
            if let Some(remaining) = line.split_once('\n') {
                let after_comment = remaining.1.trim();
                if !after_comment.is_empty() {
                    if after_comment.ends_with(';') {
                        self.run_query(after_comment);
                        if self.opts.echo {
                            let _ = self.writeln(after_comment);
                        }
                        let conn = self.conn.clone();
                        let runner = conn.query_runner(after_comment.as_bytes());
                        for output in runner {
                            if let Err(e) = self.print_query_result(after_comment, output, None) {
                                let _ = self.writeln(e.to_string());
                            }
                        }
                        self.reset_input();
                        return self.handle_input_line(after_comment);
                    } else {
                        self.set_multiline_prompt();
                        let _ = self.reset_line(line);
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }
        if line.ends_with(';') {
            self.buffer_input(line);
            let buff = self.input_buff.clone();
            self.run_query(buff.as_str());
        } else {
            self.buffer_input(format!("{}\n", line).as_str());
            self.set_multiline_prompt();
        }
        self.reset_line(line)?;
        Ok(())
    }

    pub fn handle_dot_command(&mut self, line: &str) {
        let args: Vec<&str> = line.split_whitespace().collect();
        if args.is_empty() {
            return;
        }
        match CommandParser::try_parse_from(args) {
            Err(err) => {
                // Let clap print with Styled Colors instead
                let _ = err.print();
            }
            Ok(cmd) => match cmd.command {
                Command::Exit(args) => {
                    self.save_history();
                    std::process::exit(args.code);
                }
                Command::Quit => {
                    let _ = self.writeln("Exiting Limbo SQL Shell.");
                    let _ = self.close_conn();
                    self.save_history();
                    std::process::exit(0)
                }
                Command::Open(args) => {
                    if self.open_db(&args.path, args.vfs_name.as_deref()).is_err() {
                        let _ = self.writeln("Error: Unable to open database file.");
                    }
                }
                Command::Schema(args) => {
                    if let Err(e) = self.display_schema(args.table_name.as_deref()) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Tables(args) => {
                    if let Err(e) = self.display_tables(args.pattern.as_deref()) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Opcodes(args) => {
                    if let Some(opcode) = args.opcode {
                        for op in &OPCODE_DESCRIPTIONS {
                            if op.name.eq_ignore_ascii_case(opcode.trim()) {
                                let _ = self.write_fmt(format_args!("{}", op));
                            }
                        }
                    } else {
                        for op in &OPCODE_DESCRIPTIONS {
                            let _ = self.write_fmt(format_args!("{}\n", op));
                        }
                    }
                }
                Command::NullValue(args) => {
                    self.opts.null_value = args.value;
                }
                Command::OutputMode(args) => {
                    if let Err(e) = self.set_mode(args.mode) {
                        let _ = self.write_fmt(format_args!("Error: {}", e));
                    }
                }
                Command::SetOutput(args) => {
                    if let Some(path) = args.path {
                        if let Err(e) = self.set_output_file(&path) {
                            let _ = self.write_fmt(format_args!("Error: {}", e));
                        }
                    } else {
                        self.set_output_stdout();
                    }
                }
                Command::Echo(args) => {
                    self.toggle_echo(args.mode);
                }
                Command::Cwd(args) => {
                    let _ = std::env::set_current_dir(args.directory);
                }
                Command::ShowInfo => {
                    let _ = self.show_info();
                }
                Command::Import(args) => {
                    let mut import_file =
                        ImportFile::new(self.conn.clone(), self.io.clone(), &mut self.writer);
                    import_file.import(args)
                }
                Command::LoadExtension(args) => {
                    #[cfg(not(target_family = "wasm"))]
                    if let Err(e) = self.handle_load_extension(&args.path) {
                        let _ = self.writeln(&e);
                    }
                }
                Command::Dump => {
                    if let Err(e) = self.dump_database() {
                        let _ = self.write_fmt(format_args!("/****** ERROR: {} ******/", e));
                    }
                }
                Command::ListVfs => {
                    let _ = self.writeln("Available VFS modules:");
                    self.conn.list_vfs().iter().for_each(|v| {
                        let _ = self.writeln(v);
                    });
                }
                Command::ListIndexes(args) => {
                    if let Err(e) = self.display_indexes(args.tbl_name) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Timer(timer_mode) => {
                    self.opts.timer = match timer_mode.mode {
                        TimerMode::On => true,
                        TimerMode::Off => false,
                    };
                }
            },
        }
    }

    fn print_query_result(
        &mut self,
        sql: &str,
        mut output: Result<Option<Statement>, LimboError>,
        mut statistics: Option<&mut QueryStatistics>,
    ) -> anyhow::Result<()> {
        match output {
            Ok(Some(ref mut rows)) => match self.opts.output_mode {
                OutputMode::List => loop {
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                        println!("Query interrupted.");
                        return Ok(());
                    }

                    let start = Instant::now();

                    match rows.step() {
                        Ok(StepResult::Row) => {
                            if let Some(ref mut stats) = statistics {
                                stats.execute_time_elapsed_samples.push(start.elapsed());
                            }
                            let row = rows.row().unwrap();
                            for (i, value) in row.get_values().enumerate() {
                                if i > 0 {
                                    let _ = self.writer.write(b"|");
                                }
                                if matches!(value, Value::Null) {
                                    let _ = self.writer.write(self.opts.null_value.as_bytes())?;
                                } else {
                                    let _ = self.writer.write(format!("{}", value).as_bytes())?;
                                }
                            }
                            let _ = self.writeln("");
                        }
                        Ok(StepResult::IO) => {
                            let start = Instant::now();
                            self.io.run_once()?;
                            if let Some(ref mut stats) = statistics {
                                stats.io_time_elapsed_samples.push(start.elapsed());
                            }
                        }
                        Ok(StepResult::Interrupt) => break,
                        Ok(StepResult::Done) => {
                            if let Some(ref mut stats) = statistics {
                                stats.execute_time_elapsed_samples.push(start.elapsed());
                            }
                            break;
                        }
                        Ok(StepResult::Busy) => {
                            if let Some(ref mut stats) = statistics {
                                stats.execute_time_elapsed_samples.push(start.elapsed());
                            }
                            let _ = self.writeln("database is busy");
                            break;
                        }
                        Err(err) => {
                            if let Some(ref mut stats) = statistics {
                                stats.execute_time_elapsed_samples.push(start.elapsed());
                            }
                            let _ = self.writeln(err.to_string());
                            break;
                        }
                    }
                },
                OutputMode::Pretty => {
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                        println!("Query interrupted.");
                        return Ok(());
                    }
                    let config = self.config.as_ref().unwrap();
                    let mut table = Table::new();
                    table
                        .set_content_arrangement(ContentArrangement::Dynamic)
                        .set_truncation_indicator("…")
                        .apply_modifier("││──├─┼┤│─┼├┤┬┴┌┐└┘");
                    if rows.num_columns() > 0 {
                        let header = (0..rows.num_columns())
                            .map(|i| {
                                let name = rows.get_column_name(i);
                                Cell::new(name)
                                    .add_attribute(Attribute::Bold)
                                    .fg(config.table.header_color.into_comfy_table_color())
                            })
                            .collect::<Vec<_>>();
                        table.set_header(header);
                    }
                    loop {
                        let start = Instant::now();
                        match rows.step() {
                            Ok(StepResult::Row) => {
                                if let Some(ref mut stats) = statistics {
                                    stats.execute_time_elapsed_samples.push(start.elapsed());
                                }
                                let record = rows.row().unwrap();
                                let mut row = Row::new();
                                row.max_height(1);
                                for (idx, value) in record.get_values().enumerate() {
                                    let (content, alignment) = match value {
                                        Value::Null => {
                                            (self.opts.null_value.clone(), CellAlignment::Left)
                                        }
                                        Value::Integer(_) => {
                                            (format!("{}", value), CellAlignment::Right)
                                        }
                                        Value::Float(_) => {
                                            (format!("{}", value), CellAlignment::Right)
                                        }
                                        Value::Text(_) => {
                                            (format!("{}", value), CellAlignment::Left)
                                        }
                                        Value::Blob(_) => {
                                            (format!("{}", value), CellAlignment::Left)
                                        }
                                    };
                                    row.add_cell(
                                        Cell::new(content)
                                            .set_alignment(alignment)
                                            .fg(config.table.column_colors
                                                [idx % config.table.column_colors.len()]
                                            .into_comfy_table_color()),
                                    );
                                }
                                table.add_row(row);
                            }
                            Ok(StepResult::IO) => {
                                let start = Instant::now();
                                self.io.run_once()?;
                                if let Some(ref mut stats) = statistics {
                                    stats.io_time_elapsed_samples.push(start.elapsed());
                                }
                            }
                            Ok(StepResult::Interrupt) => {
                                if let Some(ref mut stats) = statistics {
                                    stats.execute_time_elapsed_samples.push(start.elapsed());
                                }
                                break;
                            }
                            Ok(StepResult::Done) => {
                                if let Some(ref mut stats) = statistics {
                                    stats.execute_time_elapsed_samples.push(start.elapsed());
                                }
                                break;
                            }
                            Ok(StepResult::Busy) => {
                                if let Some(ref mut stats) = statistics {
                                    stats.execute_time_elapsed_samples.push(start.elapsed());
                                }
                                let _ = self.writeln("database is busy");
                                break;
                            }
                            Err(err) => {
                                if let Some(ref mut stats) = statistics {
                                    stats.execute_time_elapsed_samples.push(start.elapsed());
                                }
                                let report =
                                    miette::Error::from(err).with_source_code(sql.to_owned());
                                let _ = self.write_fmt(format_args!("{:?}", report));
                                break;
                            }
                        }
                    }

                    if !table.is_empty() {
                        let _ = self.write_fmt(format_args!("{}", table));
                    }
                }
            },
            Ok(None) => {}
            Err(err) => {
                let report = miette::Error::from(err).with_source_code(sql.to_owned());
                let _ = self.write_fmt(format_args!("{:?}", report));
                anyhow::bail!("We have to throw here, even if we printed error");
            }
        }
        // for now let's cache flush always
        self.conn.cacheflush()?;
        Ok(())
    }

    pub fn init_tracing(&mut self) -> Result<WorkerGuard, std::io::Error> {
        let ((non_blocking, guard), should_emit_ansi) =
            if let Some(file) = &self.opts.tracing_output {
                (
                    tracing_appender::non_blocking(
                        std::fs::File::options()
                            .append(true)
                            .create(true)
                            .open(file)?,
                    ),
                    false,
                )
            } else {
                (tracing_appender::non_blocking(std::io::stderr()), true)
            };
        // Disable rustyline traces
        if let Err(e) = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_line_number(true)
                    .with_thread_ids(true)
                    .with_ansi(should_emit_ansi),
            )
            .with(EnvFilter::from_default_env().add_directive("rustyline=off".parse().unwrap()))
            .try_init()
        {
            println!("Unable to setup tracing appender: {:?}", e);
        }
        Ok(guard)
    }

    fn display_schema(&mut self, table: Option<&str>) -> anyhow::Result<()> {
        let sql = match table {
        Some(table_name) => format!(
            "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND tbl_name = '{}' AND name NOT LIKE 'sqlite_%'",
            table_name
        ),
        None => String::from(
            "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%'"
        ),
    };

        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => {
                let mut found = false;
                loop {
                    match rows.step()? {
                        StepResult::Row => {
                            let row = rows.row().unwrap();
                            if let Ok(Value::Text(schema)) = row.get::<&Value>(0) {
                                let _ = self.write_fmt(format_args!("{};", schema.as_str()));
                                found = true;
                            }
                        }
                        StepResult::IO => {
                            self.io.run_once()?;
                        }
                        StepResult::Interrupt => break,
                        StepResult::Done => break,
                        StepResult::Busy => {
                            let _ = self.writeln("database is busy");
                            break;
                        }
                    }
                }
                if !found {
                    if let Some(table_name) = table {
                        let _ = self
                            .write_fmt(format_args!("-- Error: Table '{}' not found.", table_name));
                    } else {
                        let _ = self.writeln("-- No tables or indexes found in the database.");
                    }
                }
            }
            Ok(None) => {
                let _ = self.writeln("No results returned from the query.");
            }
            Err(err) => {
                if err.to_string().contains("no such table: sqlite_schema") {
                    return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
                } else {
                    return Err(anyhow::anyhow!("Error querying schema: {}", err));
                }
            }
        }

        Ok(())
    }

    fn display_indexes(&mut self, maybe_table: Option<String>) -> anyhow::Result<()> {
        let sql = match maybe_table {
            Some(ref tbl_name) => format!(
                "SELECT name FROM sqlite_schema WHERE type='index' AND tbl_name = '{}' ORDER BY 1",
                tbl_name
            ),
            None => String::from("SELECT name FROM sqlite_schema WHERE type='index' ORDER BY 1"),
        };

        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => {
                let mut indexes = String::new();
                loop {
                    match rows.step()? {
                        StepResult::Row => {
                            let row = rows.row().unwrap();
                            if let Ok(Value::Text(idx)) = row.get::<&Value>(0) {
                                indexes.push_str(idx.as_str());
                                indexes.push(' ');
                            }
                        }
                        StepResult::IO => {
                            self.io.run_once()?;
                        }
                        StepResult::Interrupt => break,
                        StepResult::Done => break,
                        StepResult::Busy => {
                            let _ = self.writeln("database is busy");
                            break;
                        }
                    }
                }
                if !indexes.is_empty() {
                    let _ = self.writeln(indexes.trim_end());
                }
            }
            Err(err) => {
                if err.to_string().contains("no such table: sqlite_schema") {
                    return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
                } else {
                    return Err(anyhow::anyhow!("Error querying schema: {}", err));
                }
            }
            Ok(None) => {}
        }

        Ok(())
    }

    fn display_tables(&mut self, pattern: Option<&str>) -> anyhow::Result<()> {
        let sql = match pattern {
            Some(pattern) => format!(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name LIKE '{}' ORDER BY 1",
                pattern
            ),
            None => String::from(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1"
            ),
        };

        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => {
                let mut tables = String::new();
                loop {
                    match rows.step()? {
                        StepResult::Row => {
                            let row = rows.row().unwrap();
                            if let Ok(Value::Text(table)) = row.get::<&Value>(0) {
                                tables.push_str(table.as_str());
                                tables.push(' ');
                            }
                        }
                        StepResult::IO => {
                            self.io.run_once()?;
                        }
                        StepResult::Interrupt => break,
                        StepResult::Done => break,
                        StepResult::Busy => {
                            let _ = self.writeln("database is busy");
                            break;
                        }
                    }
                }

                if !tables.is_empty() {
                    let _ = self.writeln(tables.trim_end());
                } else if let Some(pattern) = pattern {
                    let _ = self.write_fmt(format_args!(
                        "Error: Tables with pattern '{}' not found.",
                        pattern
                    ));
                } else {
                    let _ = self.writeln("No tables found in the database.");
                }
            }
            Ok(None) => {
                let _ = self.writeln("No results returned from the query.");
            }
            Err(err) => {
                if err.to_string().contains("no such table: sqlite_schema") {
                    return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
                } else {
                    return Err(anyhow::anyhow!("Error querying schema: {}", err));
                }
            }
        }

        Ok(())
    }

    pub fn handle_remaining_input(&mut self) {
        if self.input_buff.is_empty() {
            return;
        }

        let buff = self.input_buff.clone();
        self.run_query(buff.as_str());
        self.reset_input();
    }

    pub fn readline(&mut self) -> Result<String, ReadlineError> {
        if let Some(rl) = &mut self.rl {
            Ok(rl.readline(&self.prompt)?)
        } else {
            let mut input = String::new();
            println!("");
            let mut reader = std::io::stdin().lock();
            if reader.read_line(&mut input)? == 0 {
                return Err(ReadlineError::Eof.into());
            }
            // Remove trailing newline
            if input.ends_with('\n') {
                input.pop();
                if input.ends_with('\r') {
                    input.pop();
                }
            }

            Ok(input)
        }
    }

    fn save_history(&mut self) {
        if let Some(rl) = &mut self.rl {
            let _ = rl.save_history(HISTORY_FILE.as_path());
        }
    }
}

impl Drop for Limbo {
    fn drop(&mut self) {
        self.save_history()
    }
}
