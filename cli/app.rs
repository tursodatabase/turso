use crate::{
    helper::LimboHelper,
    import::{ImportFile, IMPORT_HELP},
    input::{get_io, get_writer, DbLocation, OutputMode, Settings, HELP_MSG},
    opcodes_dictionary::OPCODE_DESCRIPTIONS,
};
use comfy_table::{Attribute, Cell, CellAlignment, Color, ContentArrangement, Row, Table};
use limbo_core::{Database, LimboError, OwnedValue, RefValue, Statement, StepResult};

use clap::{Parser, ValueEnum};
use rustyline::{history::DefaultHistory, Editor};
use std::{
    fmt,
    io::{self, Write},
    path::PathBuf,
    rc::Rc,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
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
}

#[derive(Debug, Clone)]
pub enum Command {
    /// Exit this program with return-code CODE
    Exit,
    /// Quit the shell
    Quit,
    /// Open a database file
    Open,
    /// Display help message
    Help,
    /// Display schema for a table
    Schema,
    /// Set output file (or stdout if empty)
    SetOutput,
    /// Set output display mode
    OutputMode,
    /// Show vdbe opcodes
    Opcodes,
    /// Change the current working directory
    Cwd,
    /// Display information about settings
    ShowInfo,
    /// Set the value of NULL to be printed in 'list' mode
    NullValue,
    /// Toggle 'echo' mode to repeat commands before execution
    Echo,
    /// Display tables
    Tables,
    /// Import data from FILE into TABLE
    Import,
    /// Loads an extension library
    LoadExtension,
    /// Dump the current database as a list of SQL statements
    Dump,
    /// List vfs modules available
    ListVfs,
}

impl Command {
    fn min_args(&self) -> usize {
        1 + match self {
            Self::Exit
            | Self::Quit
            | Self::Schema
            | Self::Help
            | Self::Opcodes
            | Self::ShowInfo
            | Self::Tables
            | Self::SetOutput
            | Self::ListVfs
            | Self::Dump => 0,
            Self::Open
            | Self::OutputMode
            | Self::Cwd
            | Self::Echo
            | Self::NullValue
            | Self::LoadExtension => 1,
            Self::Import => 2,
        }
    }

    fn usage(&self) -> &str {
        match self {
            Self::Exit => ".exit ?<CODE>",
            Self::Quit => ".quit",
            Self::Open => ".open <file>",
            Self::Help => ".help",
            Self::Schema => ".schema ?<table>?",
            Self::Opcodes => ".opcodes",
            Self::OutputMode => ".mode list|pretty",
            Self::SetOutput => ".output ?file?",
            Self::Cwd => ".cd <directory>",
            Self::ShowInfo => ".show",
            Self::NullValue => ".nullvalue <string>",
            Self::Echo => ".echo on|off",
            Self::Tables => ".tables",
            Self::LoadExtension => ".load",
            Self::Dump => ".dump",
            Self::Import => &IMPORT_HELP,
            Self::ListVfs => ".vfslist",
        }
    }
}

impl FromStr for Command {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ".exit" => Ok(Self::Exit),
            ".quit" => Ok(Self::Quit),
            ".open" => Ok(Self::Open),
            ".help" => Ok(Self::Help),
            ".schema" => Ok(Self::Schema),
            ".tables" => Ok(Self::Tables),
            ".opcodes" => Ok(Self::Opcodes),
            ".mode" => Ok(Self::OutputMode),
            ".output" => Ok(Self::SetOutput),
            ".cd" => Ok(Self::Cwd),
            ".show" => Ok(Self::ShowInfo),
            ".nullvalue" => Ok(Self::NullValue),
            ".echo" => Ok(Self::Echo),
            ".import" => Ok(Self::Import),
            ".load" => Ok(Self::LoadExtension),
            ".dump" => Ok(Self::Dump),
            ".vfslist" => Ok(Self::ListVfs),
            _ => Err("Unknown command".to_string()),
        }
    }
}

const PROMPT: &str = "limbo> ";

pub struct Limbo<'a> {
    pub prompt: String,
    io: Arc<dyn limbo_core::IO>,
    writer: Box<dyn Write>,
    conn: Rc<limbo_core::Connection>,
    pub interrupt_count: Arc<AtomicUsize>,
    input_buff: String,
    opts: Settings,
    pub rl: &'a mut Editor<LimboHelper, DefaultHistory>,
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

static COLORS: &[Color] = &[Color::Green, Color::Black, Color::Grey];

impl<'a> Limbo<'a> {
    pub fn new(rl: &'a mut rustyline::Editor<LimboHelper, DefaultHistory>) -> anyhow::Result<Self> {
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
        let h = LimboHelper::new(conn.clone(), io.clone());
        rl.set_helper(Some(h));
        let interrupt_count = Arc::new(AtomicUsize::new(0));
        {
            let interrupt_count: Arc<AtomicUsize> = Arc::clone(&interrupt_count);
            ctrlc::set_handler(move || {
                // Increment the interrupt count on Ctrl-C
                interrupt_count.fetch_add(1, Ordering::SeqCst);
            })
            .expect("Error setting Ctrl-C handler");
        }
        let mut app = Self {
            prompt: PROMPT.to_string(),
            io,
            writer: get_writer(&opts.output),
            conn,
            interrupt_count,
            input_buff: String::new(),
            opts: Settings::from(&opts),
            rl,
        };

        if opts.sql.is_some() {
            app.handle_first_input(opts.sql.as_ref().unwrap());
        }
        if !opts.quiet {
            app.write_fmt(format_args!("Limbo v{}", env!("CARGO_PKG_VERSION")))?;
            app.writeln("Enter \".help\" for usage hints.")?;
            app.display_in_memory()?;
        }
        Ok(app)
    }

    fn handle_first_input(&mut self, cmd: &str) {
        if cmd.trim().starts_with('.') {
            self.handle_dot_command(cmd);
        } else {
            self.run_query(cmd);
        }
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
                    .into_iter()
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
                let _ = self.write_fmt(format_args!("INSERT INTO {} VALUES({});", name, values))?;
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
                let _ = self.write_fmt(format_args!("{};", sql))?;
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

    fn toggle_echo(&mut self, arg: &str) {
        match arg.trim().to_lowercase().as_str() {
            "on" => self.opts.echo = true,
            "off" => self.opts.echo = false,
            _ => {}
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
            let _ = self.writeln(&input);
        }
        let conn = self.conn.clone();
        let runner = conn.query_runner(input.as_bytes());
        for output in runner {
            if let Err(_) = self.print_query_result(&input, output) {
                break;
            }
        }
        self.reset_input();
    }

    fn reset_line(&mut self, line: &str) -> rustyline::Result<()> {
        self.rl.add_history_entry(line.to_owned())?;
        self.interrupt_count.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub fn handle_input_line(&mut self, line: &str) -> anyhow::Result<()> {
        if self.input_buff.is_empty() {
            if line.is_empty() {
                return Ok(());
            }
            if line.starts_with('.') {
                self.handle_dot_command(line);
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
                            if let Err(e) = self.print_query_result(after_comment, output) {
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

        if let Some(comment_pos) = line.find("--") {
            let before_comment = line[..comment_pos].trim();
            if !before_comment.is_empty() {
                return self.handle_input_line(before_comment);
            }
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
        if let Ok(ref cmd) = Command::from_str(args[0]) {
            if args.len() < cmd.min_args() {
                let _ = self.write_fmt(format_args!(
                    "Insufficient arguments: USAGE: {}",
                    cmd.usage()
                ));
                return;
            }
            match cmd {
                Command::Exit => {
                    let code = args.get(1).and_then(|c| c.parse::<i32>().ok()).unwrap_or(0);
                    std::process::exit(code);
                }
                Command::Quit => {
                    let _ = self.writeln("Exiting Limbo SQL Shell.");
                    let _ = self.close_conn();
                    std::process::exit(0)
                }
                Command::Open => {
                    let vfs = args.get(2).map(|s| &**s);
                    if self.open_db(args[1], vfs).is_err() {
                        let _ = self.writeln("Error: Unable to open database file.");
                    }
                }
                Command::Schema => {
                    let table_name = args.get(1).copied();
                    if let Err(e) = self.display_schema(table_name) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Tables => {
                    let pattern = args.get(1).copied();
                    if let Err(e) = self.display_tables(pattern) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Opcodes => {
                    if args.len() > 1 {
                        for op in &OPCODE_DESCRIPTIONS {
                            if op.name.eq_ignore_ascii_case(args.get(1).unwrap().trim()) {
                                let _ = self.write_fmt(format_args!("{}", op));
                            }
                        }
                    } else {
                        for op in &OPCODE_DESCRIPTIONS {
                            let _ = self.write_fmt(format_args!("{}\n", op));
                        }
                    }
                }
                Command::NullValue => {
                    self.opts.null_value = args[1].to_string();
                }
                Command::OutputMode => match OutputMode::from_str(args[1], true) {
                    Ok(mode) => {
                        if let Err(e) = self.set_mode(mode) {
                            let _ = self.write_fmt(format_args!("Error: {}", e));
                        }
                    }
                    Err(e) => {
                        let _ = self.writeln(e);
                    }
                },
                Command::SetOutput => {
                    if args.len() == 2 {
                        if let Err(e) = self.set_output_file(args[1]) {
                            let _ = self.write_fmt(format_args!("Error: {}", e));
                        }
                    } else {
                        self.set_output_stdout();
                    }
                }
                Command::Echo => {
                    self.toggle_echo(args[1]);
                }
                Command::Cwd => {
                    let _ = std::env::set_current_dir(args[1]);
                }
                Command::ShowInfo => {
                    let _ = self.show_info();
                }
                Command::Help => {
                    let _ = self.writeln(HELP_MSG);
                }
                Command::Import => {
                    let mut import_file =
                        ImportFile::new(self.conn.clone(), self.io.clone(), &mut self.writer);
                    if let Err(e) = import_file.import(&args) {
                        let _ = self.writeln(e.to_string());
                    };
                }
                Command::LoadExtension =>
                {
                    #[cfg(not(target_family = "wasm"))]
                    if let Err(e) = self.handle_load_extension(args[1]) {
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
            }
        } else {
            let _ = self.write_fmt(format_args!(
                "Unknown command: {}\nenter: .help for all available commands",
                args[0]
            ));
        }
    }

    fn print_query_result(
        &mut self,
        sql: &str,
        mut output: Result<Option<Statement>, LimboError>,
    ) -> anyhow::Result<()> {
        match output {
            Ok(Some(ref mut rows)) => match self.opts.output_mode {
                OutputMode::List => loop {
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                        println!("Query interrupted.");
                        return Ok(());
                    }

                    match rows.step() {
                        Ok(StepResult::Row) => {
                            let row = rows.row().unwrap();
                            for (i, value) in row.get_values().iter().enumerate() {
                                if i > 0 {
                                    let _ = self.writer.write(b"|");
                                }
                                let _ = self.writer.write(
                                    match value {
                                        RefValue::Null => self.opts.null_value.clone(),
                                        RefValue::Integer(i) => format!("{}", i),
                                        RefValue::Float(f) => format!("{:?}", f),
                                        RefValue::Text(s) => s.to_string(),
                                        RefValue::Blob(b) => {
                                            format!("{}", String::from_utf8_lossy(b.to_slice()))
                                        }
                                    }
                                    .as_bytes(),
                                )?;
                            }
                            let _ = self.writeln("");
                        }
                        Ok(StepResult::IO) => {
                            self.io.run_once()?;
                        }
                        Ok(StepResult::Interrupt) => break,
                        Ok(StepResult::Done) => {
                            break;
                        }
                        Ok(StepResult::Busy) => {
                            let _ = self.writeln("database is busy");
                            break;
                        }
                        Err(err) => {
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
                                    .fg(Color::White)
                            })
                            .collect::<Vec<_>>();
                        table.set_header(header);
                    }
                    loop {
                        match rows.step() {
                            Ok(StepResult::Row) => {
                                let record = rows.row().unwrap();
                                let mut row = Row::new();
                                row.max_height(1);
                                for (idx, value) in record.get_values().iter().enumerate() {
                                    let (content, alignment) = match value {
                                        RefValue::Null => {
                                            (self.opts.null_value.clone(), CellAlignment::Left)
                                        }
                                        RefValue::Integer(i) => {
                                            (i.to_string(), CellAlignment::Right)
                                        }
                                        RefValue::Float(f) => (f.to_string(), CellAlignment::Right),
                                        RefValue::Text(s) => (s.to_string(), CellAlignment::Left),
                                        RefValue::Blob(b) => (
                                            String::from_utf8_lossy(b.to_slice()).to_string(),
                                            CellAlignment::Left,
                                        ),
                                    };
                                    row.add_cell(
                                        Cell::new(content)
                                            .set_alignment(alignment)
                                            .fg(COLORS[idx % COLORS.len()]),
                                    );
                                }
                                table.add_row(row);
                            }
                            Ok(StepResult::IO) => {
                                self.io.run_once()?;
                            }
                            Ok(StepResult::Interrupt) => break,
                            Ok(StepResult::Done) => break,
                            Ok(StepResult::Busy) => {
                                let _ = self.writeln("database is busy");
                                break;
                            }
                            Err(err) => {
                                let _ = self.write_fmt(format_args!(
                                    "{:?}",
                                    miette::Error::from(err).with_source_code(sql.to_owned())
                                ));
                                break;
                            }
                        }
                    }

                    if table.header().is_some() {
                        let _ = self.write_fmt(format_args!("{}", table));
                    }
                }
            },
            Ok(None) => {}
            Err(err) => {
                let _ = self.write_fmt(format_args!(
                    "{:?}",
                    miette::Error::from(err).with_source_code(sql.to_owned())
                ));
                anyhow::bail!("We have to throw here, even if we printed error");
            }
        }
        // for now let's cache flush always
        self.conn.cacheflush()?;
        Ok(())
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
                            if let Some(RefValue::Text(schema)) = row.get_values().first() {
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
                            if let Some(RefValue::Text(table)) = row.get_values().first() {
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
}
