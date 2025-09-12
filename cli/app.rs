use crate::{
    commands::{
        args::{EchoMode, HeadersMode, TimerMode},
        import::ImportFile,
        Command, CommandParser,
    },
    config::Config,
    helper::LimboHelper,
    input::{
        get_io, get_writer, ApplyWriter, DbLocation, NoopProgress, OutputMode, ProgressSink,
        Settings, StderrProgress,
    },
    opcodes_dictionary::OPCODE_DESCRIPTIONS,
    HISTORY_FILE,
};
use anyhow::anyhow;
use clap::Parser;
use comfy_table::{Attribute, Cell, CellAlignment, ContentArrangement, Row, Table};
use rustyline::{error::ReadlineError, history::DefaultHistory, Editor};
use std::{
    io::{self, BufRead as _, IsTerminal, Write},
    mem::{forget, ManuallyDrop},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use turso_core::{
    Connection, Database, LimboError, OpenFlags, QueryMode, Statement, StepResult, Value,
};

#[derive(Parser, Debug)]
#[command(name = "Turso")]
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
    #[clap(long, help = "Open the database in read-only mode")]
    pub readonly: bool,
    #[clap(long, help = "Enable experimental MVCC feature")]
    pub experimental_mvcc: bool,
    #[clap(long, help = "Enable experimental views feature")]
    pub experimental_views: bool,
    #[clap(long, help = "Enable experimental indexing feature")]
    pub experimental_indexes: Option<bool>,
    #[clap(long, help = "Enable experimental strict schema mode")]
    pub experimental_strict: bool,
    #[clap(short = 't', long, help = "specify output file for log traces")]
    pub tracing_output: Option<String>,
    #[clap(long, help = "Start MCP server instead of interactive shell")]
    pub mcp: bool,
}

const PROMPT: &str = "turso> ";

pub struct Limbo {
    pub prompt: String,
    io: Arc<dyn turso_core::IO>,
    writer: Option<Box<dyn Write>>,
    conn: Arc<turso_core::Connection>,
    pub interrupt_count: Arc<AtomicUsize>,
    input_buff: ManuallyDrop<String>,
    opts: Settings,
    pub rl: Option<Editor<LimboHelper, DefaultHistory>>,
    config: Option<Config>,
}

struct QueryStatistics {
    io_time_elapsed_samples: Vec<Duration>,
    execute_time_elapsed_samples: Vec<Duration>,
}

macro_rules! row_step_result_query {
    ($app:expr, $sql:expr, $rows:expr, $stats:expr, $row_handle:expr) => {
        if $app.interrupt_count.load(Ordering::Acquire) > 0 {
            println!("Query interrupted.");
            return Ok(());
        }

        let start = Instant::now();
        match $rows.step() {
            Ok(StepResult::Row) => {
                if let Some(ref mut stats) = $stats {
                    stats.execute_time_elapsed_samples.push(start.elapsed());
                }

                $row_handle
            }
            Ok(StepResult::IO) => {
                let start = Instant::now();
                $rows.run_once()?;
                if let Some(ref mut stats) = $stats {
                    stats.io_time_elapsed_samples.push(start.elapsed());
                }
            }
            Ok(StepResult::Interrupt) => {
                if let Some(ref mut stats) = $stats {
                    stats.execute_time_elapsed_samples.push(start.elapsed());
                }
                break;
            }
            Ok(StepResult::Done) => {
                if let Some(ref mut stats) = $stats {
                    stats.execute_time_elapsed_samples.push(start.elapsed());
                }
                break;
            }
            Ok(StepResult::Busy) => {
                if let Some(ref mut stats) = $stats {
                    stats.execute_time_elapsed_samples.push(start.elapsed());
                }
                let _ = $app.writeln("database is busy");
                break;
            }
            Err(err) => {
                if let Some(ref mut stats) = $stats {
                    stats.execute_time_elapsed_samples.push(start.elapsed());
                }
                let report = miette::Error::from(err).with_source_code($sql.to_owned());
                let _ = $app.writeln_fmt(format_args!("{report:?}"));
                break;
            }
        }
    };
}

impl Limbo {
    pub fn new() -> anyhow::Result<(Self, WorkerGuard)> {
        let mut opts = Opts::parse();
        let guard = Self::init_tracing(&opts)?;

        let db_file = opts
            .database
            .as_ref()
            .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string());
        let indexes_enabled = opts.experimental_indexes.unwrap_or(true);
        let (io, conn) = if db_file.contains([':', '?', '&', '#']) {
            Connection::from_uri(
                &db_file,
                indexes_enabled,
                opts.experimental_mvcc,
                opts.experimental_views,
                opts.experimental_strict,
            )?
        } else {
            let flags = if opts.readonly {
                OpenFlags::ReadOnly
            } else {
                OpenFlags::default()
            };
            let (io, db) = Database::open_new(
                &db_file,
                opts.vfs.as_ref(),
                flags,
                turso_core::DatabaseOpts::new()
                    .with_mvcc(opts.experimental_mvcc)
                    .with_indexes(indexes_enabled)
                    .with_views(opts.experimental_views)
                    .with_strict(opts.experimental_strict),
            )?;
            let conn = db.connect()?;
            (io, conn)
        };
        unsafe {
            let mut ext_api = conn._build_turso_ext();
            if !limbo_completion::register_extension_static(&mut ext_api).is_ok() {
                return Err(anyhow!(
                    "Failed to register completion extension".to_string()
                ));
            }
            conn._free_extension_ctx(ext_api);
        }
        let interrupt_count = Arc::new(AtomicUsize::new(0));
        {
            let interrupt_count: Arc<AtomicUsize> = Arc::clone(&interrupt_count);
            ctrlc::set_handler(move || {
                // Increment the interrupt count on Ctrl-C
                interrupt_count.fetch_add(1, Ordering::Release);
            })
            .expect("Error setting Ctrl-C handler");
        }
        let sql = opts.sql.take();
        let has_sql = sql.is_some();
        let quiet = opts.quiet;
        let config = Config::for_output_mode(opts.output_mode);
        let mut app = Self {
            prompt: PROMPT.to_string(),
            io,
            writer: Some(get_writer(&opts.output)),
            conn,
            interrupt_count,
            input_buff: ManuallyDrop::new(sql.unwrap_or_default()),
            opts: Settings::from(opts),
            rl: None,
            config: Some(config),
        };
        app.first_run(has_sql, quiet)?;
        Ok((app, guard))
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_readline(mut self, mut rl: Editor<LimboHelper, DefaultHistory>) -> Self {
        let h = LimboHelper::new(
            self.conn.clone(),
            self.config.as_ref().map(|c| c.highlight.clone()),
        );
        rl.set_helper(Some(h));
        self.rl = Some(rl);
        self
    }

    fn first_run(&mut self, has_sql: bool, quiet: bool) -> Result<(), LimboError> {
        // Skip startup messages and SQL execution in MCP mode
        if self.is_mcp_mode() {
            return Ok(());
        }

        if has_sql {
            self.handle_first_input()?;
        }
        if !quiet {
            self.writeln_fmt(format_args!("Turso v{}", env!("CARGO_PKG_VERSION")))?;
            self.writeln("Enter \".help\" for usage hints.")?;
            self.writeln(
                "This software is ALPHA, only use for development, testing, and experimentation.",
            )?;
            self.display_in_memory()?;
        }
        Ok(())
    }

    fn handle_first_input(&mut self) -> Result<(), LimboError> {
        self.consume(true);
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
            n if n < 10 => format!("(x{n}...> "),
            _ => String::from("(.....> "),
        };
    }

    #[cfg(not(target_family = "wasm"))]
    fn handle_load_extension(&mut self, path: &str) -> Result<(), String> {
        let ext_path = turso_core::resolve_ext_path(path).map_err(|e| e.to_string())?;
        self.conn
            .load_extension(ext_path)
            .map_err(|e| e.to_string())
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

    fn display_stats(&mut self, args: crate::commands::args::StatsArgs) -> io::Result<()> {
        use crate::commands::args::StatsToggle;

        // Handle on/off toggle
        if let Some(toggle) = args.toggle {
            match toggle {
                StatsToggle::On => {
                    self.opts.stats = true;
                    self.writeln("Stats display enabled.")?;
                }
                StatsToggle::Off => {
                    self.opts.stats = false;
                    self.writeln("Stats display disabled.")?;
                }
            }
            return Ok(());
        }

        // Display all metrics
        let output = {
            let metrics = self.conn.metrics.borrow();
            format!("{metrics}")
        };

        self.writeln(output)?;

        if args.reset {
            self.conn.metrics.borrow_mut().reset();
            self.writeln("Statistics reset.")?;
        }

        Ok(())
    }

    pub fn reset_input(&mut self) {
        self.prompt = PROMPT.to_string();
        self.input_buff.clear();
    }

    pub fn close_conn(&mut self) -> Result<(), LimboError> {
        self.conn.close()
    }

    pub fn get_connection(&self) -> Arc<turso_core::Connection> {
        self.conn.clone()
    }

    pub fn is_mcp_mode(&self) -> bool {
        self.opts.mcp
    }

    pub fn get_interrupt_count(&self) -> Arc<AtomicUsize> {
        self.interrupt_count.clone()
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
            (
                io.clone(),
                Database::open_file(io.clone(), path, false, false)?,
            )
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
                self.writer = Some(Box::new(file));
                self.opts.is_stdout = false;
                self.opts.output_mode = OutputMode::List;
                self.opts.output_filename = path.to_string();
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn set_output_stdout(&mut self) {
        let _ = self.writer.as_mut().unwrap().flush();
        self.writer = Some(Box::new(io::stdout()));
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
        self.writer.as_mut().unwrap().write_fmt(fmt)
    }

    fn writeln_fmt(&mut self, fmt: std::fmt::Arguments) -> io::Result<()> {
        self.writer.as_mut().unwrap().write_fmt(fmt)?;
        self.writer.as_mut().unwrap().write_all(b"\n")
    }

    fn write<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.as_mut().unwrap().write_all(data.as_ref())
    }

    fn writeln<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.as_mut().unwrap().write_all(data.as_ref())?;
        self.writer.as_mut().unwrap().write_all(b"\n")
    }

    fn write_null(&mut self) -> io::Result<()> {
        self.writer
            .as_mut()
            .unwrap()
            .write_all(self.opts.null_value.as_bytes())
    }
    fn run_query(&mut self, input: &str) {
        let echo = self.opts.echo;
        if echo {
            let _ = self.writeln(input);
        }

        let start = Instant::now();
        let mut stats = if self.opts.timer {
            Some(QueryStatistics {
                io_time_elapsed_samples: vec![],
                execute_time_elapsed_samples: vec![],
            })
        } else {
            None
        };

        let conn = self.conn.clone();
        let runner = conn.query_runner(input.as_bytes());
        for output in runner {
            if self
                .print_query_result(input, output, stats.as_mut())
                .is_err()
            {
                break;
            }
        }

        self.print_query_performance_stats(start, stats.as_ref());

        // Display stats if enabled
        if self.opts.stats {
            let stats_output = {
                let metrics = self.conn.metrics.borrow();
                metrics
                    .last_statement
                    .as_ref()
                    .map(|last| format!("\n{last}"))
            };
            if let Some(output) = stats_output {
                let _ = self.writeln(output);
            }
        }
    }

    fn print_query_performance_stats(&mut self, start: Instant, stats: Option<&QueryStatistics>) {
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
        let sample_stats_as_str = |name: &str, samples: &Vec<Duration>| {
            if samples.is_empty() {
                return format!("{name}: No samples available");
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
            if let Some(stats) = stats {
                let _ = self.writeln("Command stats:\n----------------------------");
                let _ = self.writeln(format!(
                    "total: {} (this includes parsing/coloring of cli app)\n",
                    elapsed_as_str(start.elapsed())
                ));

                let _ = self.writeln("query execution stats:\n----------------------------");
                let _ = self.writeln(sample_stats_as_str(
                    "Execution",
                    &stats.execute_time_elapsed_samples,
                ));
                let _ = self.writeln(sample_stats_as_str("I/O", &stats.io_time_elapsed_samples));
            }
        }
    }

    fn reset_line(&mut self) {
        // Entry is auto added to history
        // self.rl.add_history_entry(line.to_owned())?;
        self.interrupt_count.store(0, Ordering::Release);
    }

    // consume will consume `input_buff`
    pub fn consume(&mut self, flush: bool) {
        if self.input_buff.trim().is_empty() {
            return;
        }

        self.reset_line();

        // we are taking ownership of input_buff here
        // its always safe because we split the string in two parts
        fn take_usable_part(app: &mut Limbo) -> (String, usize) {
            let ptr = app.input_buff.as_mut_ptr();
            let (len, cap) = (app.input_buff.len(), app.input_buff.capacity());
            app.input_buff =
                ManuallyDrop::new(unsafe { String::from_raw_parts(ptr.add(len), 0, cap - len) });
            (unsafe { String::from_raw_parts(ptr, len, len) }, unsafe {
                ptr.add(len).addr()
            })
        }

        fn concat_usable_part(app: &mut Limbo, mut part: String, old_address: usize) {
            let ptr = app.input_buff.as_mut_ptr();
            let (len, cap) = (app.input_buff.len(), app.input_buff.capacity());

            // if the address is not the same, meaning the string has been reallocated
            // so we just drop the part we took earlier
            if ptr.addr() != old_address || !app.input_buff.is_empty() {
                return;
            }

            let head_ptr = part.as_mut_ptr();
            let (head_len, head_cap) = (part.len(), part.capacity());
            forget(part); // move this part into `input_buff`
            app.input_buff = ManuallyDrop::new(unsafe {
                String::from_raw_parts(head_ptr, head_len + len, head_cap + cap)
            });
        }

        let value = self.input_buff.trim();
        match (value.starts_with('.'), value.ends_with(';')) {
            (true, _) => {
                let (owned_value, old_address) = take_usable_part(self);
                self.handle_dot_command(owned_value.trim().strip_prefix('.').unwrap());
                concat_usable_part(self, owned_value, old_address);
                self.reset_input();
            }
            (false, true) => {
                let (owned_value, old_address) = take_usable_part(self);
                self.run_query(owned_value.trim());
                concat_usable_part(self, owned_value, old_address);
                self.reset_input();
            }
            (false, false) if flush => {
                let (owned_value, old_address) = take_usable_part(self);
                self.run_query(owned_value.trim());
                concat_usable_part(self, owned_value, old_address);
                self.reset_input();
            }
            (false, false) => {
                self.set_multiline_prompt();
            }
        }
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
                    let _ = self.writeln("Exiting Turso SQL Shell.");
                    let _ = self.close_conn();
                    self.save_history();
                    std::process::exit(0)
                }
                Command::Open(args) => {
                    if let Err(e) = self.open_db(&args.path, args.vfs_name.as_deref()) {
                        let _ = self.writeln(e.to_string());
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
                Command::Databases => {
                    if let Err(e) = self.display_databases() {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Opcodes(args) => {
                    if let Some(opcode) = args.opcode {
                        for op in &OPCODE_DESCRIPTIONS {
                            if op.name.eq_ignore_ascii_case(opcode.trim()) {
                                let _ = self.writeln_fmt(format_args!("{op}"));
                            }
                        }
                    } else {
                        for op in &OPCODE_DESCRIPTIONS {
                            let _ = self.writeln_fmt(format_args!("{op}\n"));
                        }
                    }
                }
                Command::NullValue(args) => {
                    self.opts.null_value = args.value;
                }
                Command::OutputMode(args) => {
                    if let Err(e) = self.set_mode(args.mode) {
                        let _ = self.writeln_fmt(format_args!("Error: {e}"));
                    }
                }
                Command::SetOutput(args) => {
                    if let Some(path) = args.path {
                        if let Err(e) = self.set_output_file(&path) {
                            let _ = self.writeln_fmt(format_args!("Error: {e}"));
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
                Command::Stats(args) => {
                    if let Err(e) = self.display_stats(args) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Import(args) => {
                    let w = self.writer.as_mut().unwrap();
                    let mut import_file = ImportFile::new(self.conn.clone(), w);
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
                        let _ = self.writeln_fmt(format_args!("/****** ERROR: {e} ******/"));
                    }
                }
                Command::DbConfig(_args) => {
                    let _ = self.writeln("dbconfig currently ignored");
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
                Command::Headers(headers_mode) => {
                    self.opts.headers = match headers_mode.mode {
                        HeadersMode::On => true,
                        HeadersMode::Off => false,
                    };
                }
                Command::Clone(args) => {
                    if let Err(e) = self.clone_database(&args.output_file) {
                        let _ = self.writeln(e.to_string());
                    }
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
            Ok(Some(ref mut rows)) => match (self.opts.output_mode, rows.get_query_mode()) {
                (_, QueryMode::ExplainQueryPlan) => {
                    struct Entry {
                        id: usize,
                        detail: String,
                        child_prefix: String,
                        children: Vec<Entry>,
                    }

                    let mut root = Entry {
                        id: 0,
                        detail: "QUERY PLAN".to_owned(),
                        child_prefix: "".to_owned(),
                        children: vec![],
                    };

                    fn add_children(
                        id: usize,
                        parent_id: usize,
                        detail: String,
                        current: &mut Entry,
                    ) -> bool {
                        if current.id == parent_id {
                            current.children.push(Entry {
                                id,
                                detail,
                                child_prefix: current.child_prefix.clone() + "   ",
                                children: vec![],
                            });
                            if current.children.len() > 1 {
                                let idx = current.children.len() - 2;
                                current.children[idx].child_prefix =
                                    current.child_prefix.clone() + "|  ";
                            }

                            return false;
                        }

                        for child in &mut current.children {
                            if !add_children(id, parent_id, detail.clone(), child) {
                                return false;
                            }
                        }

                        true
                    }

                    fn print_entry(app: &mut Limbo, entry: &Entry, prefix: &str) {
                        writeln!(app, "{}{}", prefix, entry.detail).unwrap();
                        for (i, child) in entry.children.iter().enumerate() {
                            let is_last = i == entry.children.len() - 1;
                            let child_prefix = format!(
                                "{}{}",
                                entry.child_prefix,
                                if is_last { "`--" } else { "|--" }
                            );
                            print_entry(app, child, child_prefix.as_str());
                        }
                    }

                    loop {
                        row_step_result_query!(self, sql, rows, statistics, {
                            let row = rows.row().unwrap();
                            let id: usize = row.get_value(0).as_uint() as usize;
                            let parent_id: usize = row.get_value(1).as_uint() as usize;
                            let detail = row.get_value(3).to_string();
                            add_children(id, parent_id, detail, &mut root);
                        });
                    }

                    print_entry(self, &root, "");
                }
                (_, QueryMode::Explain) => {
                    fn get_explain_indent(
                        indent_count: usize,
                        curr_insn: &str,
                        prev_insn: &str,
                    ) -> usize {
                        let indent_count = match prev_insn {
                            "Rewind" | "Last" | "SorterSort" | "SeekGE" | "SeekGT" | "SeekLE"
                            | "SeekLT" => indent_count + 1,
                            _ => indent_count,
                        };

                        match curr_insn {
                            "Next" | "SorterNext" | "Prev" => indent_count - 1,
                            _ => indent_count,
                        }
                    }

                    let _ = self.writeln(
                        "addr  opcode             p1    p2    p3    p4             p5  comment",
                    );
                    let _ = self.writeln(
                        "----  -----------------  ----  ----  ----  -------------  --  -------",
                    );

                    let mut prev_insn: String = "".to_string();
                    let mut indent_count = 0;
                    let indent = "  ";
                    loop {
                        row_step_result_query!(self, sql, rows, statistics, {
                            let row = rows.row().unwrap();
                            let insn = row.get_value(1).to_string();
                            indent_count = get_explain_indent(indent_count, &insn, &prev_insn);
                            let _ = self.writeln(format!(
                                "{:<4}  {:<17}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
                                row.get_value(0).to_string(),
                                &(indent.repeat(indent_count) + &insn),
                                row.get_value(2).to_string(),
                                row.get_value(3).to_string(),
                                row.get_value(4).to_string(),
                                row.get_value(5).to_string(),
                                row.get_value(6).to_string(),
                                row.get_value(7),
                            ));
                            prev_insn = insn;
                        });
                    }
                }
                (OutputMode::List, _) => {
                    let mut headers_printed = false;
                    loop {
                        row_step_result_query!(self, sql, rows, statistics, {
                            // Print headers if enabled and not already printed
                            if self.opts.headers && !headers_printed {
                                for i in 0..rows.num_columns() {
                                    if i > 0 {
                                        let _ = self.write(b"|");
                                    }
                                    let _ = self.write(rows.get_column_name(i).as_bytes());
                                }
                                let _ = self.writeln("");
                                headers_printed = true;
                            }

                            let row = rows.row().unwrap();
                            for (i, value) in row.get_values().enumerate() {
                                if i > 0 {
                                    let _ = self.write(b"|");
                                }
                                if matches!(value, Value::Null) {
                                    self.write_null()?;
                                } else {
                                    write!(self, "{value}")?;
                                }
                            }
                            let _ = self.writeln("");
                        });
                    }
                }
                (OutputMode::Pretty, _) => {
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
                                    .fg(config.table.header_color.as_comfy_table_color())
                            })
                            .collect::<Vec<_>>();
                        table.set_header(header);
                    }
                    loop {
                        row_step_result_query!(self, sql, rows, statistics, {
                            let record = rows.row().unwrap();
                            let mut row = Row::new();
                            row.max_height(1);
                            for (idx, value) in record.get_values().enumerate() {
                                let (content, alignment) = match value {
                                    Value::Null => {
                                        (self.opts.null_value.clone(), CellAlignment::Left)
                                    }
                                    Value::Integer(_) => (format!("{value}"), CellAlignment::Right),
                                    Value::Float(_) => (format!("{value}"), CellAlignment::Right),
                                    Value::Text(_) => (format!("{value}"), CellAlignment::Left),
                                    Value::Blob(_) => (format!("{value}"), CellAlignment::Left),
                                };
                                row.add_cell(
                                    Cell::new(content)
                                        .set_alignment(alignment)
                                        .fg(config.table.column_colors
                                            [idx % config.table.column_colors.len()]
                                        .as_comfy_table_color()),
                                );
                            }
                            table.add_row(row);
                        });
                    }

                    if !table.is_empty() {
                        writeln!(self, "{table}")?;
                    }
                }
                (OutputMode::Line, _) => {
                    let mut first_row_printed = false;

                    let max_width = (0..rows.num_columns())
                        .map(|i| rows.get_column_name(i).len())
                        .max()
                        .unwrap_or(0);

                    let formatted_columns: Vec<String> = (0..rows.num_columns())
                        .map(|i| format!("{:>width$}", rows.get_column_name(i), width = max_width))
                        .collect();

                    loop {
                        row_step_result_query!(self, sql, rows, statistics, {
                            let record = rows.row().unwrap();

                            if !first_row_printed {
                                first_row_printed = true;
                            } else {
                                self.writeln("")?;
                            }

                            for (i, value) in record.get_values().enumerate() {
                                self.write(&formatted_columns[i])?;
                                self.write(b" = ")?;
                                if matches!(value, Value::Null) {
                                    self.write_null()?;
                                } else {
                                    write!(self, "{value}")?;
                                }
                                self.writeln("")?;
                            }
                        });
                    }
                }
            },
            Ok(None) => {}
            Err(err) => {
                let report = miette::Error::from(err).with_source_code(sql.to_owned());
                let _ = self.writeln_fmt(format_args!("{report:?}"));
                anyhow::bail!("We have to throw here, even if we printed error");
            }
        }
        Ok(())
    }

    pub fn init_tracing(opts: &Opts) -> Result<WorkerGuard, std::io::Error> {
        let ((non_blocking, guard), should_emit_ansi) = if let Some(file) = &opts.tracing_output {
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
            (
                tracing_appender::non_blocking(std::io::stderr()),
                IsTerminal::is_terminal(&std::io::stderr()),
            )
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
            println!("Unable to setup tracing appender: {e:?}");
        }
        Ok(guard)
    }

    fn print_schema_entry(&mut self, db_display_name: &str, row: &turso_core::Row) -> bool {
        if let (Ok(Value::Text(schema)), Ok(Value::Text(obj_type)), Ok(Value::Text(obj_name))) = (
            row.get::<&Value>(0),
            row.get::<&Value>(1),
            row.get::<&Value>(2),
        ) {
            let modified_schema = if db_display_name == "main" {
                schema.as_str().to_string()
            } else {
                // We need to modify the SQL to include the database prefix in table names
                // This is a simple approach - for CREATE TABLE statements, insert db name after "TABLE "
                // For CREATE INDEX statements, insert db name after "ON "
                let schema_str = schema.as_str();
                if schema_str.to_uppercase().contains("CREATE TABLE ") {
                    // Find "CREATE TABLE " and insert database name after it
                    if let Some(pos) = schema_str.to_uppercase().find("CREATE TABLE ") {
                        let before = &schema_str[..pos + "CREATE TABLE ".len()];
                        let after = &schema_str[pos + "CREATE TABLE ".len()..];
                        format!("{before}{db_display_name}.{after}")
                    } else {
                        schema_str.to_string()
                    }
                } else if schema_str.to_uppercase().contains(" ON ") {
                    // For indexes, find " ON " and insert database name after it
                    if let Some(pos) = schema_str.to_uppercase().find(" ON ") {
                        let before = &schema_str[..pos + " ON ".len()];
                        let after = &schema_str[pos + " ON ".len()..];
                        format!("{before}{db_display_name}.{after}")
                    } else {
                        schema_str.to_string()
                    }
                } else {
                    schema_str.to_string()
                }
            };
            let _ = self.writeln_fmt(format_args!("{modified_schema};"));
            // For views, add the column comment like SQLite does
            if obj_type.as_str() == "view" {
                let columns = self
                    .get_view_columns(obj_name.as_str())
                    .unwrap_or_else(|_| "x".to_string());
                let _ = self.writeln_fmt(format_args!("/* {}({}) */", obj_name.as_str(), columns));
            }
            true
        } else {
            false
        }
    }

    /// Get column names for a view to generate the SQLite-compatible comment
    fn get_view_columns(&mut self, view_name: &str) -> anyhow::Result<String> {
        // Get column information using PRAGMA table_info
        let pragma_sql = format!("PRAGMA table_info({view_name})");

        let mut columns = Vec::new();
        let handler = |row: &turso_core::Row| -> anyhow::Result<()> {
            // Column name is in the second column (index 1) of PRAGMA table_info
            if let Ok(Value::Text(col_name)) = row.get::<&Value>(1) {
                columns.push(col_name.as_str().to_string());
            }
            Ok(())
        };
        if let Err(err) = self.handle_row(&pragma_sql, handler) {
            return Err(anyhow::anyhow!(
                "Error retrieving columns for view '{}': {}",
                view_name,
                err
            ));
        }
        if columns.is_empty() {
            anyhow::bail!("PRAGMA table_info returned no columns for view '{}'. The view may be corrupted or the database schema is invalid.", view_name);
        }
        Ok(columns.join(","))
    }

    fn query_one_table_schema(
        &mut self,
        db_prefix: &str,
        db_display_name: &str,
        table_name: &str,
    ) -> anyhow::Result<bool> {
        let sql = format!(
            "SELECT sql, type, name FROM {db_prefix}.sqlite_schema WHERE type IN ('table', 'index', 'view') AND (tbl_name = '{table_name}' OR name = '{table_name}') AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 1 WHEN 'view' THEN 2 WHEN 'index' THEN 3 END, rowid"
        );

        let mut found = false;
        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        found |= self.print_schema_entry(db_display_name, row);
                    }
                    StepResult::IO => {
                        rows.run_once()?;
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => {
                        let _ = self.writeln("database is busy");
                        break;
                    }
                }
            },
            Ok(None) => {}
            Err(_) => {} // Table not found in this database
        }
        Ok(found)
    }

    fn query_all_tables_schema(
        &mut self,
        db_prefix: &str,
        db_display_name: &str,
    ) -> anyhow::Result<()> {
        let sql = format!("SELECT sql, type, name FROM {db_prefix}.sqlite_schema WHERE type IN ('table', 'index', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 1 WHEN 'view' THEN 2 WHEN 'index' THEN 3 END, rowid");

        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        self.print_schema_entry(db_display_name, row);
                    }
                    StepResult::IO => {
                        rows.run_once()?;
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => {
                        let _ = self.writeln("database is busy");
                        break;
                    }
                }
            },
            Ok(None) => {}
            Err(err) => {
                // If we can't access this database's schema, just skip it
                if !err.to_string().contains("no such table") {
                    eprintln!(
                        "Warning: Could not query schema for database '{db_display_name}': {err}"
                    );
                }
            }
        }
        Ok(())
    }

    fn display_schema(&mut self, table: Option<&str>) -> anyhow::Result<()> {
        match table {
            Some(table_spec) => {
                // Parse table name to handle database prefixes (e.g., "db.table")
                let clean_table_spec = table_spec.trim_end_matches(';');

                let (target_db, table_name) =
                    if let Some((db, tbl)) = clean_table_spec.split_once('.') {
                        (db, tbl)
                    } else {
                        ("main", clean_table_spec)
                    };

                // Query only the specific table in the specific database
                let found = if target_db == "main" {
                    self.query_one_table_schema("main", "main", table_name)?
                } else {
                    // Check if the database is attached
                    let attached_databases = self.conn.list_attached_databases();
                    if attached_databases.contains(&target_db.to_string()) {
                        self.query_one_table_schema(target_db, target_db, table_name)?
                    } else {
                        false
                    }
                };

                if !found {
                    let table_display = if target_db == "main" {
                        table_name.to_string()
                    } else {
                        format!("{target_db}.{table_name}")
                    };
                    let _ = self
                        .writeln_fmt(format_args!("-- Error: Table '{table_display}' not found."));
                }
            }
            None => {
                // Show schema for all tables in all databases
                let attached_databases = self.conn.list_attached_databases();

                // Query main database first
                self.query_all_tables_schema("main", "main")?;

                // Query all attached databases
                for db_name in attached_databases {
                    self.query_all_tables_schema(&db_name, &db_name)?;
                }
            }
        }

        Ok(())
    }

    fn display_indexes(&mut self, maybe_table: Option<String>) -> anyhow::Result<()> {
        let sql = match maybe_table {
            Some(ref tbl_name) => format!(
                "SELECT name FROM sqlite_schema WHERE type='index' AND tbl_name = '{tbl_name}' ORDER BY 1"
            ),
            None => String::from("SELECT name FROM sqlite_schema WHERE type='index' ORDER BY 1"),
        };

        let mut indexes = String::new();
        let handler = |row: &turso_core::Row| -> anyhow::Result<()> {
            if let Ok(Value::Text(idx)) = row.get::<&Value>(0) {
                indexes.push_str(idx.as_str());
                indexes.push(' ');
            }
            Ok(())
        };
        if let Err(err) = self.handle_row(&sql, handler) {
            if err.to_string().contains("no such table: sqlite_schema") {
                return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
            } else {
                return Err(anyhow::anyhow!("Error querying schema: {}", err));
            }
        }
        if !indexes.is_empty() {
            let _ = self.writeln(indexes.trim_end().as_bytes());
        }
        Ok(())
    }

    fn display_tables(&mut self, pattern: Option<&str>) -> anyhow::Result<()> {
        let sql = match pattern {
            Some(pattern) => format!(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name LIKE '{pattern}' ORDER BY 1"
            ),
            None => String::from(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1"
            ),
        };

        let mut tables = String::new();
        let handler = |row: &turso_core::Row| -> anyhow::Result<()> {
            if let Ok(Value::Text(table)) = row.get::<&Value>(0) {
                tables.push_str(table.as_str());
                tables.push(' ');
            }
            Ok(())
        };
        if let Err(e) = self.handle_row(&sql, handler) {
            if e.to_string().contains("no such table: sqlite_schema") {
                return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
            } else {
                return Err(anyhow::anyhow!("Error querying schema: {}", e));
            }
        }
        if !tables.is_empty() {
            let _ = self.writeln(tables.trim_end().as_bytes());
        } else if let Some(pattern) = pattern {
            let _ = self.writeln_fmt(format_args!(
                "Error: Tables with pattern '{pattern}' not found."
            ));
        } else {
            let _ = self.writeln(b"No tables found in the database.");
        }
        Ok(())
    }

    fn handle_row<F>(&mut self, sql: &str, mut handler: F) -> anyhow::Result<()>
    where
        F: FnMut(&turso_core::Row) -> anyhow::Result<()>,
    {
        match self.conn.query(sql) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        handler(row)?;
                    }
                    StepResult::IO => {
                        rows.run_once()?;
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => {
                        let _ = self.writeln("database is busy");
                        break;
                    }
                }
            },
            Ok(None) => {
                let _ = self.writeln("No results returned from the query.");
            }
            Err(err) => {
                return Err(anyhow::anyhow!("Error querying database: {}", err));
            }
        }
        Ok(())
    }

    fn display_databases(&mut self) -> anyhow::Result<()> {
        let sql = "PRAGMA database_list";

        match self.conn.query(sql) {
            Ok(Some(ref mut rows)) => {
                loop {
                    match rows.step()? {
                        StepResult::Row => {
                            let row = rows.row().unwrap();
                            if let (
                                Ok(Value::Integer(seq)),
                                Ok(Value::Text(name)),
                                Ok(file_value),
                            ) = (
                                row.get::<&Value>(0),
                                row.get::<&Value>(1),
                                row.get::<&Value>(2),
                            ) {
                                let file = match file_value {
                                    Value::Text(path) => path.as_str(),
                                    Value::Null => "",
                                    _ => "",
                                };

                                // Format like SQLite: "main: /path/to/file r/w"
                                let file_display = if file.is_empty() {
                                    "\"\"".to_string()
                                } else {
                                    file.to_string()
                                };

                                // Detect readonly mode from connection
                                let mode = if self.conn.is_readonly(*seq as usize) {
                                    "r/o"
                                } else {
                                    "r/w"
                                };

                                let _ = self.writeln(format!(
                                    "{}: {} {}",
                                    name.as_str(),
                                    file_display,
                                    mode
                                ));
                            }
                        }
                        StepResult::IO => {
                            rows.run_once()?;
                        }
                        StepResult::Interrupt => break,
                        StepResult::Done => break,
                        StepResult::Busy => {
                            let _ = self.writeln("database is busy");
                            break;
                        }
                    }
                }
            }
            Ok(None) => {
                let _ = self.writeln("No results returned from the query.");
            }
            Err(err) => {
                return Err(anyhow::anyhow!("Error querying database list: {}", err));
            }
        }

        Ok(())
    }

    // readline will read inputs from rustyline or stdin
    // and write it to input_buff.
    pub fn readline(&mut self) -> Result<(), ReadlineError> {
        use std::fmt::Write;

        if let Some(rl) = &mut self.rl {
            let result = rl.readline(&self.prompt)?;
            let _ = self.input_buff.write_str(result.as_str());
        } else {
            let mut reader = std::io::stdin().lock();
            if reader.read_line(&mut self.input_buff)? == 0 {
                return Err(ReadlineError::Eof);
            }
        }

        let _ = self.input_buff.write_char(' ');
        Ok(())
    }

    pub fn dump_database_from_conn<W: Write, P: ProgressSink>(
        fk: bool,
        conn: Arc<Connection>,
        out: &mut W,
        mut progress: P,
    ) -> anyhow::Result<()> {
        // Snapshot for consistency
        Self::exec_all_conn(&conn, "BEGIN")?;
        // FIXME: we don't yet support PRAGMA foreign_keys=OFF internally,
        // so for now this hacky boolean that decides not to emit it when cloning
        if fk {
            writeln!(out, "PRAGMA foreign_keys=OFF;")?;
        }
        writeln!(out, "BEGIN TRANSACTION;")?;
        // FIXME: At this point, SQLite executes the following:
        // sqlite3_exec(p->db, "SAVEPOINT dump; PRAGMA writable_schema=ON", 0, 0, 0);
        // we don't have those yet, so don't.
        let q_tables = r#"
        SELECT name, sql
        FROM sqlite_schema
        WHERE type='table' AND sql NOT NULL
        ORDER BY tbl_name = 'sqlite_sequence', rowid
    "#;
        if let Some(mut rows) = conn.query(q_tables)? {
            loop {
                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let name: &str = row.get::<&str>(0)?;
                        let ddl: &str = row.get::<&str>(1)?;
                        writeln!(out, "{ddl};")?;
                        Self::dump_table_from_conn(&conn, out, name, &mut progress)?;
                        progress.on(name);
                    }
                    StepResult::IO => rows.run_once()?,
                    StepResult::Done | StepResult::Interrupt => break,
                    StepResult::Busy => anyhow::bail!("database is busy"),
                }
            }
        }
        Self::dump_sqlite_sequence(&conn, out)?;
        Self::dump_schema_objects(&conn, out, &mut progress)?;
        Self::exec_all_conn(&conn, "COMMIT")?;
        writeln!(out, "COMMIT;")?;
        Ok(())
    }

    fn exec_all_conn(conn: &Arc<Connection>, sql: &str) -> anyhow::Result<()> {
        if let Some(mut rows) = conn.query(sql)? {
            loop {
                match rows.step()? {
                    StepResult::IO => rows.run_once()?,
                    StepResult::Done | StepResult::Interrupt => break,
                    StepResult::Busy => anyhow::bail!("database is busy"),
                    StepResult::Row => {}
                }
            }
        }
        Ok(())
    }

    fn dump_table_from_conn<W: Write, P: ProgressSink>(
        conn: &Arc<Connection>,
        out: &mut W,
        table_name: &str,
        progress: &mut P,
    ) -> anyhow::Result<()> {
        let pragma = format!("PRAGMA table_info({})", quote_ident(table_name));
        let (mut cols, mut types) = (Vec::new(), Vec::new());

        if let Some(mut rows) = conn.query(pragma)? {
            loop {
                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let ty = row.get::<&str>(2)?.to_string();
                        let name = row.get::<&str>(1)?.to_string();
                        match ty.as_str() {
                            "index" => progress.on(&name),
                            "view" => progress.on(&name),
                            "trigger" => progress.on(&name),
                            _ => {}
                        }
                        cols.push(name);
                        types.push(ty);
                    }
                    StepResult::IO => rows.run_once()?,
                    StepResult::Done | StepResult::Interrupt => break,
                    StepResult::Busy => anyhow::bail!("database is busy"),
                }
            }
        }
        // FIXME: sqlite has logic to check rowid and optionally preserve it, but it requires
        // pragma index_list, and it seems to be relevant only for indexes.
        let cols_str = cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let select = format!("SELECT {cols_str} FROM {}", quote_ident(table_name));
        if let Some(mut rows) = conn.query(select)? {
            loop {
                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        write!(out, "INSERT INTO {} VALUES(", quote_ident(table_name))?;
                        for i in 0..cols.len() {
                            if i > 0 {
                                out.write_all(b",")?;
                            }
                            let v = row.get::<&Value>(i)?;
                            Self::write_sql_value_from_value(out, v)?;
                        }
                        out.write_all(b");\n")?;
                    }
                    StepResult::IO => rows.run_once()?,
                    StepResult::Done | StepResult::Interrupt => break,
                    StepResult::Busy => anyhow::bail!("database is busy"),
                }
            }
        }
        Ok(())
    }

    fn dump_sqlite_sequence<W: Write>(conn: &Arc<Connection>, out: &mut W) -> anyhow::Result<()> {
        let mut has_seq = false;
        if let Some(mut rows) =
            conn.query("SELECT 1 FROM sqlite_schema WHERE name='sqlite_sequence' AND type='table'")?
        {
            loop {
                match rows.step()? {
                    StepResult::Row => {
                        has_seq = true;
                    }
                    StepResult::IO => rows.run_once()?,
                    StepResult::Done | StepResult::Interrupt => break,
                    StepResult::Busy => anyhow::bail!("database is busy"),
                }
            }
        }
        if !has_seq {
            return Ok(());
        }

        writeln!(out, "DELETE FROM sqlite_sequence;")?;
        if let Some(mut rows) = conn.query("SELECT name, seq FROM sqlite_sequence")? {
            loop {
                match rows.step()? {
                    StepResult::Row => {
                        let r = rows.row().unwrap();
                        let name = r.get::<&str>(0)?;
                        let seq = r.get::<i64>(1)?;
                        writeln!(
                            out,
                            "INSERT INTO sqlite_sequence(name,seq) VALUES({},{});",
                            sql_quote_string(name),
                            seq
                        )?;
                    }
                    StepResult::IO => rows.run_once()?,
                    StepResult::Done | StepResult::Interrupt => break,
                    StepResult::Busy => anyhow::bail!("database is busy"),
                }
            }
        }
        Ok(())
    }

    fn dump_schema_objects<W: Write, P: ProgressSink>(
        conn: &Arc<Connection>,
        out: &mut W,
        progress: &mut P,
    ) -> anyhow::Result<()> {
        // SQLite’s shell usually emits views after tables.
        // Emit only user objects: sql NOT NULL and name NOT LIKE 'sqlite_%'
        let sql = r#"
            SELECT name, sql FROM sqlite_schema
            WHERE sql NOT NULL
              AND name NOT LIKE 'sqlite_%'
              AND type IN ('index','trigger','view')
            ORDER BY CASE type WHEN 'view' THEN 1 WHEN 'index' THEN 2 WHEN 'trigger' THEN 3 END, rowid
        "#;
        if let Some(mut rows) = conn.query(sql)? {
            loop {
                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let ddl: &str = row.get::<&str>(1)?;
                        let name: &str = row.get::<&str>(0)?;
                        progress.on(name);
                        writeln!(out, "{ddl};")?;
                    }
                    StepResult::IO => rows.run_once()?,
                    StepResult::Done | StepResult::Interrupt => break,
                    StepResult::Busy => anyhow::bail!("database is busy"),
                }
            }
        }
        Ok(())
    }

    fn write_sql_value_from_value<W: Write>(out: &mut W, v: &Value) -> io::Result<()> {
        match v {
            Value::Null => out.write_all(b"NULL"),
            Value::Integer(i) => out.write_all(format!("{i}").as_bytes()),
            Value::Float(f) => write!(out, "{f}").map(|_| ()),
            Value::Text(s) => {
                out.write_all(b"'")?;
                let bytes = &s.value;
                let mut i = 0;
                while i < bytes.len() {
                    let b = bytes[i];
                    if b == b'\'' {
                        out.write_all(b"''")?;
                    } else {
                        out.write_all(&[b])?;
                    }
                    i += 1;
                }
                out.write_all(b"'")
            }
            Value::Blob(b) => {
                out.write_all(b"X'")?;
                const HEX: &[u8; 16] = b"0123456789abcdef";
                for &byte in b {
                    out.write_all(&[HEX[(byte >> 4) as usize], HEX[(byte & 0x0F) as usize]])?;
                }
                out.write_all(b"'")
            }
        }
    }

    fn dump_database(&mut self) -> anyhow::Result<()> {
        // Move writer out so we don’t hold a field-borrow of self during the call.
        let mut out = std::mem::take(&mut self.writer).unwrap();
        let conn = self.conn.clone();
        // dont print progress because it would interfere with piping output of .dump
        let res = Self::dump_database_from_conn(true, conn, &mut out, NoopProgress);
        // Put writer back
        self.writer = Some(out);
        res
    }

    fn clone_database(&mut self, output_file: &str) -> anyhow::Result<()> {
        use std::path::Path;
        if Path::new(output_file).exists() {
            anyhow::bail!("Refusing to overwrite existing file: {output_file}");
        }
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new()?);
        let db = Database::open_file(io.clone(), output_file, false, true)?;
        let target = db.connect()?;

        let mut applier = ApplyWriter::new(&target);
        Self::dump_database_from_conn(false, self.conn.clone(), &mut applier, StderrProgress)?;
        applier.finish()?;
        Ok(())
    }

    fn save_history(&mut self) {
        if let Some(rl) = &mut self.rl {
            let _ = rl.save_history(HISTORY_FILE.as_path());
        }
    }
}

fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

fn sql_quote_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push('\'');
        } // escape as ''
        out.push(ch);
    }
    out.push('\'');
    out
}
impl Drop for Limbo {
    fn drop(&mut self) {
        self.save_history();
        unsafe {
            ManuallyDrop::drop(&mut self.input_buff);
        }
    }
}
