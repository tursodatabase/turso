use clap::Args;
use clap_complete::{ArgValueCompleter, PathCompleter};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::File,
    io::{self, Read, Write},
    num::NonZero,
    path::{Path, PathBuf},
    string::FromUtf8Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use turso_core::{Connection, LimboError, Statement, Value};

#[derive(Debug, Clone, Args)]
pub struct ImportArgs {
    /// Use , and \n as column and row separators
    #[arg(long, default_value = "true")]
    csv: bool,
    /// Roll back this import if any row fails
    #[arg(long, default_value = "false")]
    atomic: bool,
    /// "Verbose" - increase auxiliary output
    #[arg(short, default_value = "false")]
    verbose: bool,
    /// Skip the first N rows of input
    #[arg(long, default_value = "0")]
    skip: u64,
    #[arg(add = ArgValueCompleter::new(PathCompleter::file()))]
    file: PathBuf,
    table: String,
}

pub struct ImportFile<'a> {
    conn: Arc<Connection>,
    out: &'a mut dyn Write,
    err: &'a mut dyn Write,
    interrupt_count: Option<Arc<AtomicUsize>>,
}

#[derive(Debug, Clone, Copy)]
enum ImportTransaction {
    Unmanaged,
    Managed,
    AtomicSavepoint,
}

enum RelationKind {
    Table,
    View,
}

impl RelationKind {
    fn from_sqlite_master_type(value: &str) -> Result<Self, LimboError> {
        match value {
            "table" => Ok(Self::Table),
            "view" => Ok(Self::View),
            _ => Err(LimboError::InternalError(format!(
                "unexpected sqlite_master.type for import target: {value}"
            ))),
        }
    }
}

/// `.import` emits its own diagnostics as it runs; `failed` only tells the
/// shell whether the command should count as a query error.
#[derive(Debug, Default)]
struct ImportProgress {
    failed: bool,
    success_rows: u64,
    failed_rows: u64,
}

impl ImportProgress {
    fn record_failure(&mut self) {
        self.failed_rows += 1;
        self.failed = true;
    }
}

#[derive(Debug)]
enum ImportAbort {
    Message(String),
    RowError(String),
    Interrupted,
}

impl ImportAbort {
    fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    fn read_row(error: CsvReadError) -> Self {
        match error {
            CsvReadError::Interrupted => Self::Interrupted,
            CsvReadError::InvalidUtf8 { .. } => {
                Self::RowError(format!("Error reading row: {error}"))
            }
            CsvReadError::Io(_) => Self::Message(format!("Error reading row: {error}")),
        }
    }
}

impl<'a> ImportFile<'a> {
    pub fn new(conn: Arc<Connection>, out: &'a mut dyn Write, err: &'a mut dyn Write) -> Self {
        Self {
            conn,
            out,
            err,
            interrupt_count: None,
        }
    }

    pub fn with_interrupt_count(mut self, interrupt_count: Arc<AtomicUsize>) -> Self {
        self.interrupt_count = Some(interrupt_count);
        self
    }

    /// Runs `.import`, emitting its own diagnostics. Returns true when the
    /// command should count as a query error.
    pub(crate) fn import(&mut self, args: ImportArgs) -> bool {
        self.import_csv(args)
    }

    pub(crate) fn import_csv(&mut self, args: ImportArgs) -> bool {
        match self.import_csv_inner(args) {
            Ok(failed) => failed,
            Err(abort) => {
                self.emit_abort(abort);
                true
            }
        }
    }

    fn import_csv_inner(&mut self, args: ImportArgs) -> Result<bool, ImportAbort> {
        let relation_kind = match self.relation_kind(&args.table) {
            Ok(kind) => kind,
            Err(e) => {
                return Err(ImportAbort::message(format!(
                    "Error checking table existence: {e}"
                )));
            }
        };

        let mut csv_reader = self.open_csv_reader(&args)?;
        self.skip_import_records(&args, &mut csv_reader)?;

        let mut transaction = ImportTransaction::Unmanaged;
        match relation_kind {
            None => {
                let header = self.read_import_header(&args, &mut csv_reader)?;
                let auto_columns = auto_column_names_with_renames(&header);
                self.emit_column_rename_notice(&args.file, &auto_columns.renames);

                if args.atomic {
                    transaction = self.begin_import_transaction(true).map_err(|e| {
                        ImportAbort::message(format!("Error beginning import transaction: {e}"))
                    })?;
                }

                if let Err(abort) = self.create_import_table(&args, &auto_columns.columns) {
                    self.rollback_atomic_import(transaction);
                    return Err(abort);
                }
            }
            Some(RelationKind::Table) => {}
            Some(RelationKind::View) => {
                return Err(ImportAbort::message(format!(
                    "Error: cannot modify {} because it is a view",
                    args.table
                )));
            }
        };

        let column_count = match self.table_column_count(&args.table) {
            Ok(column_count) => column_count,
            Err(e) => {
                self.rollback_atomic_import(transaction);
                return Err(ImportAbort::message(format!(
                    "Error reading table columns: {e}"
                )));
            }
        };
        if column_count == 0 {
            let mut progress = ImportProgress::default();
            self.complete_import_transaction(transaction, args.atomic, &mut progress);
            return Ok(progress.failed);
        }

        let insert_sql = insert_statement_sql(&args.table, column_count);
        let mut insert_stmt = match self.conn.prepare(&insert_sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                self.rollback_atomic_import(transaction);
                return Err(ImportAbort::message(format!("Error preparing insert: {e}")));
            }
        };
        transaction = self.ensure_import_transaction_started(args.atomic, transaction)?;

        let mut progress =
            self.import_records(&args, &mut csv_reader, &mut insert_stmt, column_count);

        drop(insert_stmt);
        self.complete_import_transaction(transaction, args.atomic, &mut progress);
        self.emit_verbose_summary(&args, &csv_reader, &progress);
        Ok(progress.failed)
    }

    fn open_csv_reader(&self, args: &ImportArgs) -> Result<CsvImportReader<File>, ImportAbort> {
        let file = File::open(&args.file).map_err(|_| {
            ImportAbort::message(format!("Error: cannot open \"{}\"", args.file.display()))
        })?;
        let mut csv_reader = CsvImportReader::new(file);
        if let Some(interrupt_count) = &self.interrupt_count {
            csv_reader = csv_reader.with_interrupt_count(interrupt_count.clone());
        }
        Ok(csv_reader)
    }

    fn skip_import_records<R: Read>(
        &mut self,
        args: &ImportArgs,
        csv_reader: &mut CsvImportReader<R>,
    ) -> Result<(), ImportAbort> {
        for _ in 0..args.skip {
            if self
                .read_raw_import_record(csv_reader, &args.file)?
                .is_none()
            {
                break;
            }
        }
        Ok(())
    }

    fn read_import_header<R: Read>(
        &mut self,
        args: &ImportArgs,
        csv_reader: &mut CsvImportReader<R>,
    ) -> Result<Vec<String>, ImportAbort> {
        match self.read_import_record(csv_reader, &args.file)? {
            Some(record) => Ok(record.fields),
            None => Err(ImportAbort::message(format!(
                "{}: empty file",
                args.file.display()
            ))),
        }
    }

    fn create_import_table(
        &mut self,
        args: &ImportArgs,
        columns: &[String],
    ) -> Result<(), ImportAbort> {
        let column_defs = columns
            .iter()
            .map(|column| format!("{} ANY", quote_ident(column)))
            .collect::<Vec<_>>()
            .join(", ");
        let create_table = format!(
            "CREATE TABLE {} ({});",
            quote_ident(&args.table),
            column_defs
        );
        self.run_statement(&create_table)
            .map_err(|e| ImportAbort::message(format!("Error creating table: {e}")))
    }

    fn ensure_import_transaction_started(
        &mut self,
        atomic: bool,
        transaction: ImportTransaction,
    ) -> Result<ImportTransaction, ImportAbort> {
        match transaction {
            ImportTransaction::Unmanaged => self.begin_import_transaction(atomic).map_err(|e| {
                ImportAbort::message(format!("Error beginning import transaction: {e}"))
            }),
            _ => Ok(transaction),
        }
    }

    fn import_records<R: Read>(
        &mut self,
        args: &ImportArgs,
        csv_reader: &mut CsvImportReader<R>,
        insert_stmt: &mut Statement,
        column_count: usize,
    ) -> ImportProgress {
        let mut progress = ImportProgress::default();
        loop {
            let record = match self.read_import_record(csv_reader, &args.file) {
                Ok(Some(record)) => record,
                Ok(None) => break,
                Err(ImportAbort::Interrupted) => {
                    progress.failed = true;
                    self.emit_interrupt();
                    break;
                }
                Err(ImportAbort::RowError(message)) => {
                    progress.record_failure();
                    let _ = writeln!(self.err, "{message}");
                    continue;
                }
                Err(ImportAbort::Message(message)) => {
                    progress.failed = true;
                    let _ = writeln!(self.err, "{message}");
                    break;
                }
            };

            let start_line = record.start_line;
            let trailing_empty_field_at_eof = record.trailing_empty_field_at_eof;
            let field_count = record.fields.len() + usize::from(trailing_empty_field_at_eof);
            self.emit_column_count_warning(args, start_line, column_count, field_count);

            if let Err(e) = bind_import_row(
                insert_stmt,
                record.fields,
                column_count,
                trailing_empty_field_at_eof,
            ) {
                progress.record_failure();
                insert_stmt.clear_bindings();
                insert_stmt.reset_best_effort();
                let message = import_insert_error_message(&e);
                self.emit_insert_error(&args.file, start_line, &message);
                continue;
            }

            match run_import_insert(insert_stmt) {
                Ok(()) => {
                    progress.success_rows += 1;
                }
                Err(LimboError::Interrupt) => {
                    self.emit_interrupt();
                    progress.failed = true;
                    break;
                }
                Err(e) => {
                    progress.record_failure();
                    let message = import_insert_error_message(&e);
                    self.emit_insert_error(&args.file, start_line, &message);
                }
            }
        }
        progress
    }

    fn emit_column_count_warning(
        &mut self,
        args: &ImportArgs,
        line: u64,
        column_count: usize,
        field_count: usize,
    ) {
        if field_count < column_count {
            let _ = writeln!(
                self.err,
                "{}:{}: expected {} columns but found {} - filling the rest with NULL",
                args.file.display(),
                line,
                column_count,
                field_count
            );
        } else if field_count > column_count {
            let _ = writeln!(
                self.err,
                "{}:{}: expected {} columns but found {} - extras ignored",
                args.file.display(),
                line,
                column_count,
                field_count
            );
        }
    }

    fn complete_import_transaction(
        &mut self,
        transaction: ImportTransaction,
        atomic: bool,
        progress: &mut ImportProgress,
    ) {
        // SQLite commits the successful prefix after row errors or Ctrl-C.
        // --atomic is the explicit opt-in to rollback instead.
        if progress.failed && atomic {
            self.rollback_atomic_import(transaction);
        } else if let Err(e) = self.finish_import_transaction(transaction) {
            let _ = writeln!(self.err, "Error committing import transaction: {e}");
            if atomic {
                self.rollback_atomic_import(transaction);
            }
            progress.failed = true;
        }
    }

    fn emit_verbose_summary<R: Read>(
        &mut self,
        args: &ImportArgs,
        csv_reader: &CsvImportReader<R>,
        progress: &ImportProgress,
    ) {
        if args.verbose {
            let _ = writeln!(self.out, "Column separator \",\", row separator \"\\n\"");
            let input_lines = csv_reader.lines_read_for_summary();
            let _ = writeln!(
                self.out,
                "Added {} rows with {} errors using {input_lines} lines of input",
                progress.success_rows, progress.failed_rows
            );
        }
    }

    fn emit_csv_warnings(&mut self, file: &Path, warnings: &[CsvWarning]) {
        for warning in warnings {
            let message = match warning.kind {
                CsvWarningKind::UnescapedQuote => "unescaped \" character",
                CsvWarningKind::UnterminatedQuote => "unterminated \"-quoted field",
            };
            let _ = writeln!(self.err, "{}:{}: {message}", file.display(), warning.line);
        }
    }

    fn emit_abort(&mut self, abort: ImportAbort) {
        match abort {
            ImportAbort::Message(message) | ImportAbort::RowError(message) => {
                let _ = writeln!(self.err, "{message}");
            }
            ImportAbort::Interrupted => self.emit_interrupt(),
        }
    }

    fn read_import_record<R: Read>(
        &mut self,
        csv_reader: &mut CsvImportReader<R>,
        file: &Path,
    ) -> Result<Option<CsvRecord>, ImportAbort> {
        self.read_raw_import_record(csv_reader, file)?
            .map(|record| record.into_record().map_err(ImportAbort::read_row))
            .transpose()
    }

    fn read_raw_import_record<R: Read>(
        &mut self,
        csv_reader: &mut CsvImportReader<R>,
        file: &Path,
    ) -> Result<Option<CsvRawRecord>, ImportAbort> {
        let record = csv_reader
            .read_raw_record()
            .map_err(ImportAbort::read_row)?;
        if let Some(record) = &record {
            self.emit_csv_warnings(file, &record.warnings);
        }
        Ok(record)
    }

    fn emit_interrupt(&mut self) {
        let _ = writeln!(self.err, "interrupt");
    }

    fn rollback_managed_import(&mut self, context: &str) {
        if let Err(e) = self.run_statement("ROLLBACK") {
            let _ = writeln!(
                self.err,
                "Error rolling back import transaction {context}: {e}"
            );
        }
    }

    fn begin_import_transaction(&mut self, atomic: bool) -> Result<ImportTransaction, LimboError> {
        if self.conn.get_auto_commit() {
            self.run_statement("BEGIN")?;
            Ok(ImportTransaction::Managed)
        } else if atomic {
            // A savepoint keeps --atomic scoped to this import when the caller
            // already owns the surrounding transaction.
            self.run_statement("SAVEPOINT \"__turso_import_atomic\"")?;
            Ok(ImportTransaction::AtomicSavepoint)
        } else {
            Ok(ImportTransaction::Unmanaged)
        }
    }

    fn finish_import_transaction(
        &mut self,
        transaction: ImportTransaction,
    ) -> Result<(), LimboError> {
        match transaction {
            ImportTransaction::Unmanaged => Ok(()),
            ImportTransaction::Managed => self.run_statement("COMMIT"),
            ImportTransaction::AtomicSavepoint => {
                self.run_statement("RELEASE \"__turso_import_atomic\"")
            }
        }
    }

    fn rollback_atomic_import(&mut self, transaction: ImportTransaction) {
        match transaction {
            ImportTransaction::Unmanaged => {}
            ImportTransaction::Managed => {
                self.rollback_managed_import("after failed atomic import")
            }
            ImportTransaction::AtomicSavepoint => {
                if let Err(e) = self.run_statement("ROLLBACK TO \"__turso_import_atomic\"") {
                    let _ = writeln!(
                        self.err,
                        "Error rolling back import transaction after failed atomic import: {e}"
                    );
                }
                if let Err(e) = self.run_statement("RELEASE \"__turso_import_atomic\"") {
                    let _ = writeln!(
                        self.err,
                        "Error releasing import savepoint after failed atomic import: {e}"
                    );
                }
            }
        }
    }

    fn emit_insert_error(&mut self, file: &Path, line: u64, message: &str) {
        let _ = writeln!(
            self.err,
            "{}:{line}: INSERT failed: {message}",
            file.display()
        );
    }

    fn emit_column_rename_notice(&mut self, file: &Path, renamed_columns: &[(String, String)]) {
        if renamed_columns.is_empty() {
            return;
        }

        let renames = renamed_columns
            .iter()
            .map(|(from, to)| format!("{} to {}", quote_ident(from), quote_ident(to)))
            .collect::<Vec<_>>()
            .join(",\n");
        let _ = writeln!(
            self.err,
            "Columns renamed during .import {} due to duplicates:\n{renames}",
            file.display()
        );
    }

    fn relation_kind(&mut self, table: &str) -> Result<Option<RelationKind>, LimboError> {
        let sql = format!(
            "SELECT type FROM sqlite_master WHERE name={} COLLATE NOCASE AND type IN ('table','view') LIMIT 1",
            sql_quote_string_literal(table)
        );
        let mut kind = None;
        if let Some(mut rows) = self.conn.query(sql)? {
            rows.run_with_row_callback(|row| {
                let value = row.get::<&Value>(0)?;
                let Value::Text(text) = value else {
                    return Err(LimboError::InternalError(
                        "sqlite_master.type must be text".to_string(),
                    ));
                };
                kind = Some(RelationKind::from_sqlite_master_type(text.as_str())?);
                Ok(())
            })?;
        }
        Ok(kind)
    }

    fn table_column_count(&mut self, table: &str) -> Result<usize, LimboError> {
        let sql = format!("PRAGMA table_info({})", quote_ident(table));
        let mut count = 0usize;
        if let Some(mut rows) = self.conn.query(sql)? {
            rows.run_with_row_callback(|_| {
                count += 1;
                Ok(())
            })?;
        }
        Ok(count)
    }

    fn run_statement(&mut self, sql: &str) -> Result<(), LimboError> {
        let mut stmt = self.conn.prepare(sql)?;
        let result = stmt.run_with_row_callback(|_| {
            Err(LimboError::InternalError(format!(
                "unexpected row from statement: {sql}"
            )))
        });
        match result {
            Ok(()) => stmt.reset(),
            Err(e) => {
                stmt.reset_best_effort();
                Err(e)
            }
        }
    }
}

#[derive(Debug)]
struct CsvRecord {
    fields: Vec<String>,
    start_line: u64,
    trailing_empty_field_at_eof: bool,
}

#[derive(Debug)]
struct CsvRawRecord {
    fields: Vec<Vec<u8>>,
    start_line: u64,
    warnings: Vec<CsvWarning>,
    trailing_empty_field_at_eof: bool,
}

impl CsvRawRecord {
    fn into_record(self) -> Result<CsvRecord, CsvReadError> {
        let start_line = self.start_line;
        let fields = self
            .fields
            .into_iter()
            .map(String::from_utf8)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| CsvReadError::InvalidUtf8 {
                line: start_line,
                source,
            })?;
        Ok(CsvRecord {
            fields,
            start_line,
            trailing_empty_field_at_eof: self.trailing_empty_field_at_eof,
        })
    }
}

/// Errors produced while reading one CSV record.
///
/// `InvalidUtf8` is recoverable per row: the raw record was fully consumed, so
/// the reader is already positioned at the next record. Turso values require
/// valid UTF-8 text while SQLite's shell can bind arbitrary bytes, making this
/// a deliberate CLI compatibility gap rather than a parser synchronization
/// error.
#[derive(Debug)]
enum CsvReadError {
    Interrupted,
    InvalidUtf8 { line: u64, source: FromUtf8Error },
    Io(io::Error),
}

impl fmt::Display for CsvReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Interrupted => write!(f, "interrupt"),
            Self::InvalidUtf8 { line, source } => {
                write!(f, "line {line}: invalid UTF-8: {source}")
            }
            Self::Io(error) => error.fmt(f),
        }
    }
}

impl From<io::Error> for CsvReadError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CsvWarning {
    line: u64,
    kind: CsvWarningKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CsvWarningKind {
    UnescapedQuote,
    UnterminatedQuote,
}

#[derive(Debug)]
struct CsvField {
    bytes: Vec<u8>,
    terminator: FieldTerminator,
    warnings: Vec<CsvWarning>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldTerminator {
    Column,
    Row,
    Eof,
}

const CSV_IMPORT_BUFFER_SIZE: usize = 8 * 1024;

/// CSV reader for `.import --csv`.
///
/// This intentionally follows SQLite shell import behavior instead of RFC 4180:
/// physical blank rows are one empty field, EOF can terminate a final record,
/// trailing column separators add an empty field, and line counts are physical
/// input lines used for diagnostics and the verbose summary.
struct CsvImportReader<R> {
    reader: R,
    buffer: [u8; CSV_IMPORT_BUFFER_SIZE],
    buffer_pos: usize,
    buffer_len: usize,
    pushed_byte: Option<u8>,
    line: u64,
    seen_any: bool,
    interrupt_count: Option<Arc<AtomicUsize>>,
}

impl<R: Read> CsvImportReader<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: [0; CSV_IMPORT_BUFFER_SIZE],
            buffer_pos: 0,
            buffer_len: 0,
            pushed_byte: None,
            line: 1,
            seen_any: false,
            interrupt_count: None,
        }
    }

    fn with_interrupt_count(mut self, interrupt_count: Arc<AtomicUsize>) -> Self {
        self.interrupt_count = Some(interrupt_count);
        self
    }

    fn read_raw_record(&mut self) -> Result<Option<CsvRawRecord>, CsvReadError> {
        let start_line = self.line;
        let mut fields = Vec::new();
        let mut warnings = Vec::new();
        let mut needs_trailing_empty_field = false;
        loop {
            let Some(field) = self.read_field()? else {
                if fields.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(CsvRawRecord {
                    fields,
                    start_line,
                    warnings,
                    trailing_empty_field_at_eof: needs_trailing_empty_field,
                }));
            };
            warnings.extend(field.warnings);
            fields.push(field.bytes);
            match field.terminator {
                FieldTerminator::Column => {
                    needs_trailing_empty_field = true;
                }
                FieldTerminator::Row | FieldTerminator::Eof => {
                    return Ok(Some(CsvRawRecord {
                        fields,
                        start_line,
                        warnings,
                        trailing_empty_field_at_eof: false,
                    }));
                }
            }
        }
    }

    fn read_field(&mut self) -> Result<Option<CsvField>, CsvReadError> {
        if self.is_interrupted() {
            return Err(CsvReadError::Interrupted);
        }
        let Some(mut c) = self.next_byte()? else {
            return Ok(None);
        };
        let mut field_prefix = Vec::new();

        if !self.seen_any && c == 0xef {
            let Some(second) = self.next_byte()? else {
                self.seen_any = true;
                return Ok(Some(CsvField {
                    bytes: vec![c],
                    terminator: FieldTerminator::Eof,
                    warnings: Vec::new(),
                }));
            };
            field_prefix.push(c);
            if second == 0xbb {
                let Some(third) = self.next_byte()? else {
                    self.seen_any = true;
                    field_prefix.push(second);
                    return Ok(Some(CsvField {
                        bytes: field_prefix,
                        terminator: FieldTerminator::Eof,
                        warnings: Vec::new(),
                    }));
                };
                if third == 0xbf {
                    self.seen_any = true;
                    return self.read_field();
                }
                field_prefix.push(second);
                c = third;
            } else {
                c = second;
            }
        }

        self.seen_any = true;
        if field_prefix.is_empty() && c == b'"' {
            return self.read_quoted_field(self.line).map(Some);
        }

        let mut field = field_prefix;
        loop {
            match c {
                b',' => {
                    return Ok(Some(CsvField {
                        bytes: field,
                        terminator: FieldTerminator::Column,
                        warnings: Vec::new(),
                    }));
                }
                b'\n' => {
                    self.line += 1;
                    if field.last() == Some(&b'\r') {
                        field.pop();
                    }
                    return Ok(Some(CsvField {
                        bytes: field,
                        terminator: FieldTerminator::Row,
                        warnings: Vec::new(),
                    }));
                }
                _ => field.push(c),
            }

            let Some(next) = self.next_byte()? else {
                return Ok(Some(CsvField {
                    bytes: field,
                    terminator: FieldTerminator::Eof,
                    warnings: Vec::new(),
                }));
            };
            c = next;
        }
    }

    fn read_quoted_field(&mut self, start_line: u64) -> Result<CsvField, CsvReadError> {
        let mut field = Vec::new();
        let mut warnings = Vec::new();
        loop {
            let Some(c) = self.next_byte()? else {
                warnings.push(CsvWarning {
                    line: start_line,
                    kind: CsvWarningKind::UnterminatedQuote,
                });
                return Ok(CsvField {
                    bytes: field,
                    terminator: FieldTerminator::Eof,
                    warnings,
                });
            };
            match c {
                b'"' => {
                    let Some(next) = self.next_byte()? else {
                        return Ok(CsvField {
                            bytes: field,
                            terminator: FieldTerminator::Eof,
                            warnings,
                        });
                    };
                    match next {
                        b'"' => field.push(b'"'),
                        b',' => {
                            return Ok(CsvField {
                                bytes: field,
                                terminator: FieldTerminator::Column,
                                warnings,
                            });
                        }
                        b'\n' => {
                            self.line += 1;
                            return Ok(CsvField {
                                bytes: field,
                                terminator: FieldTerminator::Row,
                                warnings,
                            });
                        }
                        b'\r' => {
                            let following = self.next_byte()?;
                            if following == Some(b'\n') {
                                self.line += 1;
                                return Ok(CsvField {
                                    bytes: field,
                                    terminator: FieldTerminator::Row,
                                    warnings,
                                });
                            }
                            if let Some(following) = following {
                                self.push_byte(following);
                            }
                            field.push(b'"');
                            field.push(b'\r');
                        }
                        other => {
                            warnings.push(CsvWarning {
                                line: self.line,
                                kind: CsvWarningKind::UnescapedQuote,
                            });
                            field.push(b'"');
                            field.push(other);
                        }
                    }
                }
                b'\n' => {
                    self.line += 1;
                    field.push(c);
                }
                _ => field.push(c),
            }
        }
    }

    fn next_byte(&mut self) -> io::Result<Option<u8>> {
        if let Some(byte) = self.pushed_byte.take() {
            return Ok(Some(byte));
        }

        if self.buffer_pos == self.buffer_len {
            self.refill_buffer()?;
        }
        if self.buffer_pos == self.buffer_len {
            Ok(None)
        } else {
            let byte = self.buffer[self.buffer_pos];
            self.buffer_pos += 1;
            Ok(Some(byte))
        }
    }

    fn refill_buffer(&mut self) -> io::Result<()> {
        self.buffer_pos = 0;
        match self.reader.read(&mut self.buffer) {
            Ok(bytes_read) => {
                self.buffer_len = bytes_read;
                Ok(())
            }
            Err(error) => {
                self.buffer_len = 0;
                Err(error)
            }
        }
    }

    fn push_byte(&mut self, byte: u8) {
        assert!(
            self.pushed_byte.replace(byte).is_none(),
            "CsvImportReader supports one byte of lookahead"
        );
    }

    fn lines_read_for_summary(&self) -> u64 {
        self.line.saturating_sub(1)
    }

    fn is_interrupted(&self) -> bool {
        self.interrupt_count
            .as_ref()
            .is_some_and(|count| count.load(Ordering::Acquire) > 0)
    }
}

fn bind_import_row(
    stmt: &mut Statement,
    fields: Vec<String>,
    column_count: usize,
    trailing_empty_field_at_eof: bool,
) -> Result<(), LimboError> {
    let field_count = fields.len();
    let mut fields = fields.into_iter();
    for i in 0..column_count {
        let value = fields.next().map_or_else(
            || {
                if trailing_empty_field_at_eof
                    && field_count + 1 == column_count
                    && i + 1 == column_count
                {
                    Value::from_text("")
                } else {
                    Value::Null
                }
            },
            Value::from_text,
        );
        let index = NonZero::new(i + 1).expect("bind indexes are 1-based");
        stmt.bind_at(index, value)?;
    }
    Ok(())
}

fn run_import_insert(stmt: &mut Statement) -> Result<(), LimboError> {
    let result = stmt.run_with_row_callback(|_| {
        Err(LimboError::InternalError(
            "unexpected row from import INSERT".to_string(),
        ))
    });
    let reset = stmt.reset();
    match (result, reset) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), Ok(())) | (Ok(()), Err(e)) => Err(e),
        (Err(e), Err(_)) => Err(e),
    }
}

fn import_insert_error_message(error: &LimboError) -> String {
    match error {
        LimboError::Constraint(message)
        | LimboError::ForeignKeyConstraint(message)
        | LimboError::Raise(_, message) => message.to_string(),
        LimboError::Busy => "database is busy".to_string(),
        _ => error.to_string(),
    }
}

fn insert_statement_sql(table: &str, column_count: usize) -> String {
    format!(
        "INSERT INTO {} VALUES({})",
        quote_ident(table),
        vec!["?"; column_count].join(",")
    )
}

#[cfg(test)]
fn auto_column_names(headers: &[String]) -> Vec<String> {
    auto_column_names_with_renames(headers).columns
}

#[derive(Debug, PartialEq, Eq)]
struct AutoColumnNames {
    columns: Vec<String>,
    renames: Vec<(String, String)>,
}

fn auto_column_names_with_renames(headers: &[String]) -> AutoColumnNames {
    let base_names = headers
        .iter()
        .map(|name| {
            if name.is_empty() {
                "?".to_string()
            } else {
                name.to_string()
            }
        })
        .collect::<Vec<_>>();

    let mut counts = HashMap::<String, usize>::new();
    for name in &base_names {
        *counts.entry(name.to_ascii_lowercase()).or_default() += 1;
    }
    let repeated = counts
        .into_iter()
        .filter_map(|(name, count)| (count > 1).then_some(name))
        .collect::<HashSet<_>>();
    if repeated.is_empty() {
        return AutoColumnNames {
            columns: base_names,
            renames: Vec::new(),
        };
    }

    // IMPORTANT: SQLite checks for collisions using candidates padded to
    // the digit count of the column total, but emits names that start
    // unpadded, so past 9 columns the name it checks is not the name it
    // creates and CREATE TABLE can fail on a clash its check never saw:
    //
    //   printf 'X,X,c3,c4,c5,c6,c7,c8,c9,c10,c11,x_1\n' > /tmp/dup12.csv
    //   sqlite3 :memory: '.import --csv /tmp/dup12.csv t'
    //   => duplicate column name: x_1   (checked "X_01" but emitted "X_1")
    //
    // To avoid this we test the exact names we will emit, widening until
    // they are unique.
    let max_width = base_names
        .iter()
        .map(String::len)
        .max()
        .unwrap_or(0)
        .saturating_add(headers.len().to_string().len())
        .saturating_add(1);
    for width in 1..=max_width {
        let candidate_names = base_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                if repeated.contains(&name.to_ascii_lowercase()) {
                    format!("{name}_{:0width$}", i + 1)
                } else {
                    name.to_string()
                }
            })
            .collect::<Vec<_>>();
        let mut seen = HashSet::with_capacity(candidate_names.len());
        if candidate_names
            .iter()
            .all(|name| seen.insert(name.to_ascii_lowercase()))
        {
            let renames = base_names
                .iter()
                .zip(candidate_names.iter())
                .filter(|(from, to)| from != to)
                .map(|(from, to)| (from.clone(), to.clone()))
                .collect();
            return AutoColumnNames {
                columns: candidate_names,
                renames,
            };
        }
    }

    unreachable!(
        "suffix width exceeds every literal header length, so generated import column names cannot collide"
    )
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn sql_quote_string_literal(literal: &str) -> String {
    format!("'{}'", literal.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    };
    use turso_core::{Database, MemoryIO, Numeric};

    impl<R: Read> CsvImportReader<R> {
        fn read_record(&mut self) -> Result<Option<CsvRecord>, CsvReadError> {
            self.read_raw_record()?
                .map(CsvRawRecord::into_record)
                .transpose()
        }
    }

    #[test]
    fn auto_column_names_grows_suffix_width_on_collision() {
        let headers = vec!["X".to_string(), "X".to_string(), "x_1".to_string()];
        assert_eq!(auto_column_names(&headers), vec!["X_01", "X_02", "x_1"]);
    }

    #[test]
    fn auto_column_names_does_not_panic_when_width_exceeds_usize_digits() {
        let mut headers = Vec::new();
        headers.push("X_22".to_string());
        for width in 3..=20 {
            headers.push(format!("X_{:0width$}", 22));
        }
        headers.push("Y".to_string());
        headers.push("X".to_string());
        headers.push("X".to_string());

        let columns = auto_column_names(&headers);
        assert_eq!(columns[20], format!("X_{:021}", 21));
        assert_eq!(columns[21], format!("X_{:021}", 22));
    }

    #[test]
    fn auto_column_names_reports_renamed_columns() {
        let headers = vec!["id".to_string(), "id".to_string(), "x_1".to_string()];
        let auto_columns = auto_column_names_with_renames(&headers);
        assert_eq!(auto_columns.columns, vec!["id_1", "id_2", "x_1"]);
        assert_eq!(
            auto_columns.renames,
            vec![
                ("id".to_string(), "id_1".to_string()),
                ("id".to_string(), "id_2".to_string()),
            ]
        );
    }

    #[test]
    fn auto_column_names_uses_question_mark_for_empty_header() {
        let headers = vec!["".to_string(), "".to_string()];
        assert_eq!(auto_column_names(&headers), vec!["?_1", "?_2"]);
    }

    #[test]
    fn csv_import_reader_preserves_blank_lines_as_empty_field_records() {
        let mut reader = CsvImportReader::new("a,b\n\n1,2\n".as_bytes());
        assert_eq!(
            reader.read_record().unwrap().unwrap().fields,
            vec!["a", "b"]
        );
        assert_eq!(reader.read_record().unwrap().unwrap().fields, vec![""]);
        assert_eq!(
            reader.read_record().unwrap().unwrap().fields,
            vec!["1", "2"]
        );
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn csv_import_reader_handles_multiline_quoted_fields() {
        let mut reader = CsvImportReader::new("\"a\nb\",c\n".as_bytes());
        let record = reader.read_record().unwrap().unwrap();
        assert_eq!(record.start_line, 1);
        assert_eq!(record.fields, vec!["a\nb", "c"]);
        assert_eq!(reader.line, 3);
    }

    #[test]
    fn csv_import_reader_reports_unescaped_and_unterminated_quotes() {
        let mut reader = CsvImportReader::new("a,\"b\"c\n".as_bytes());
        let record = reader.read_raw_record().unwrap().unwrap();
        assert_eq!(record.fields, vec![b"a".to_vec(), b"b\"c\n".to_vec()]);
        assert_eq!(
            record.warnings,
            vec![
                CsvWarning {
                    line: 1,
                    kind: CsvWarningKind::UnescapedQuote,
                },
                CsvWarning {
                    line: 1,
                    kind: CsvWarningKind::UnterminatedQuote,
                },
            ]
        );
    }

    #[test]
    fn csv_import_reader_reports_unescaped_quote_on_current_line() {
        let mut reader = CsvImportReader::new("\"a\nb\"c".as_bytes());
        let record = reader.read_raw_record().unwrap().unwrap();
        assert_eq!(
            record.warnings,
            vec![
                CsvWarning {
                    line: 2,
                    kind: CsvWarningKind::UnescapedQuote,
                },
                CsvWarning {
                    line: 1,
                    kind: CsvWarningKind::UnterminatedQuote,
                },
            ]
        );
    }

    #[test]
    fn csv_import_reader_refills_before_pushed_byte() {
        let field = "a".repeat(CSV_IMPORT_BUFFER_SIZE - 3);
        let input = format!("\"{field}\"\rb\",c\n");
        let mut reader = CsvImportReader::new(input.as_bytes());

        let record = reader.read_record().unwrap().unwrap();
        assert_eq!(
            record.fields,
            vec![format!("{field}\"\rb"), "c".to_string()]
        );
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn csv_import_reader_consumes_record_before_utf8_error() {
        let cases: &[(&[u8], u64)] = &[
            (b"\xff,a\nb,c\n", 2),
            (b"\xef\nb,c\n", 2),
            (b"a,\xff\nb,c\n", 2),
            (b"\"a\n\xff\",z\nb,c\n", 3),
        ];

        for (input, next_record_line) in cases {
            let mut reader = CsvImportReader::new(*input);
            let err = reader.read_record().unwrap_err();
            assert!(err.to_string().contains("line 1: invalid UTF-8"));

            let record = reader.read_record().unwrap().unwrap();
            assert_eq!(record.start_line, *next_record_line);
            assert_eq!(record.fields, vec!["b", "c"]);
        }
    }

    #[test]
    fn csv_import_reader_checks_separator_after_partial_bom_prefix() {
        let mut reader = CsvImportReader::new(b"\xef,a\nb,c\n".as_slice());
        let record = reader.read_raw_record().unwrap().unwrap();
        assert_eq!(record.fields, vec![vec![0xef], b"a".to_vec()]);

        let record = reader.read_record().unwrap().unwrap();
        assert_eq!(record.start_line, 2);
        assert_eq!(record.fields, vec!["b", "c"]);
    }

    #[test]
    fn csv_import_reader_skips_utf8_bom() {
        let mut reader = CsvImportReader::new(b"\xef\xbb\xbfa,b\n".as_slice());
        let record = reader.read_record().unwrap().unwrap();
        assert_eq!(record.start_line, 1);
        assert_eq!(record.fields, vec!["a", "b"]);
    }

    #[test]
    fn csv_import_reader_observes_interrupt_before_next_field() {
        let interrupt_count = Arc::new(AtomicUsize::new(1));
        let mut reader =
            CsvImportReader::new("a,b\n".as_bytes()).with_interrupt_count(interrupt_count);

        let err = reader.read_record().unwrap_err();
        assert!(matches!(err, CsvReadError::Interrupted));
    }

    #[test]
    fn import_read_row_keeps_only_decode_errors_recoverable() {
        let invalid_utf8 = ImportAbort::read_row(CsvReadError::InvalidUtf8 {
            line: 1,
            source: String::from_utf8(vec![0xff]).unwrap_err(),
        });
        assert!(matches!(invalid_utf8, ImportAbort::RowError(_)));

        let source_error = ImportAbort::read_row(CsvReadError::Io(io::Error::other(
            "persistent read failure",
        )));
        assert!(matches!(source_error, ImportAbort::Message(_)));
    }

    #[test]
    fn import_insert_error_message_uses_raw_constraint_message() {
        let error = LimboError::Constraint("UNIQUE constraint failed: t.x (19)".to_string());
        assert_eq!(
            import_insert_error_message(&error),
            "UNIQUE constraint failed: t.x (19)"
        );
    }

    #[test]
    fn import_connection_interrupt_handles_default_and_atomic_modes() {
        let (failed, error, auto_commit, count) = run_progress_interrupted_import(false);

        assert!(failed);
        assert!(error.contains("interrupt"));
        assert!(auto_commit);
        assert!(
            count > 0,
            "default import should preserve rows inserted before interrupt"
        );
        assert!(count < 200, "interrupt should stop before all rows import");

        let (failed, error, auto_commit, count) = run_progress_interrupted_import(true);

        assert!(failed);
        assert!(error.contains("interrupt"));
        assert!(auto_commit);
        assert_eq!(count, 0);
    }

    #[test]
    fn import_atomic_auto_create_rolls_back_created_table_on_read_error() {
        let io: Arc<dyn turso_core::IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:import-atomic-auto-create").unwrap();
        let conn = db.connect().unwrap();

        let path = std::env::temp_dir().join(format!(
            "turso-import-atomic-auto-create-{}-{}.csv",
            std::process::id(),
            unique_test_id()
        ));
        fs::write(&path, b"x\n\xff\n").unwrap();

        let mut output = Vec::new();
        let mut error = Vec::new();
        let mut importer = ImportFile::new(conn.clone(), &mut output, &mut error);
        let failed = importer.import_csv(ImportArgs {
            csv: true,
            atomic: true,
            verbose: false,
            skip: 0,
            file: path.clone(),
            table: "auto_atomic".to_string(),
        });
        let _ = fs::remove_file(path);

        assert!(failed);
        assert!(conn.get_auto_commit());
        assert_eq!(
            select_count(
                &conn,
                "SELECT count(*) FROM sqlite_master WHERE name='auto_atomic'"
            ),
            0
        );
        assert!(String::from_utf8(error)
            .unwrap()
            .contains("Error reading row: line 2: invalid UTF-8"));
    }

    fn run_progress_interrupted_import(atomic: bool) -> (bool, String, bool, i64) {
        let io: Arc<dyn turso_core::IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:import-interrupt").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x)").unwrap();

        let path = std::env::temp_dir().join(format!(
            "turso-import-interrupt-{}-{}.csv",
            std::process::id(),
            unique_test_id()
        ));
        let mut contents = String::new();
        for i in 0..200 {
            contents.push_str(&format!("{i}\n"));
        }
        fs::write(&path, contents).unwrap();

        let fired = Arc::new(AtomicBool::new(false));
        let fired_for_handler = fired.clone();
        let steps = Arc::new(AtomicUsize::new(0));
        let steps_for_handler = steps.clone();
        let conn_for_handler = conn.clone();
        conn.set_progress_handler(
            1,
            Some(Box::new(move || {
                if steps_for_handler.fetch_add(1, Ordering::SeqCst) > 100
                    && !fired_for_handler.swap(true, Ordering::SeqCst)
                {
                    conn_for_handler.interrupt();
                }
                false
            })),
        );

        let mut output = Vec::new();
        let mut error = Vec::new();
        let mut importer = ImportFile::new(conn.clone(), &mut output, &mut error);
        let failed = importer.import_csv(ImportArgs {
            csv: true,
            atomic,
            verbose: false,
            skip: 0,
            file: path.clone(),
            table: "t".to_string(),
        });
        conn.set_progress_handler(0, None);
        let _ = fs::remove_file(path);

        let auto_commit = conn.get_auto_commit();
        let count = select_count(&conn, "SELECT count(*) FROM t");
        (
            failed,
            String::from_utf8(error).unwrap(),
            auto_commit,
            count,
        )
    }

    #[test]
    fn import_commit_failure_handles_default_and_atomic_modes() {
        let (conn, failed, error) = run_deferred_fk_import(false);

        assert!(failed);
        assert!(error.contains("Error committing import transaction"));
        assert!(!conn.get_auto_commit());
        assert_eq!(select_count(&conn, "SELECT count(*) FROM child"), 1);
        conn.execute("ROLLBACK").unwrap();
        assert!(conn.get_auto_commit());

        let (conn, failed, error) = run_deferred_fk_import(true);

        assert!(failed);
        assert!(error.contains("Error committing import transaction"));
        assert!(conn.get_auto_commit());
        assert_eq!(select_count(&conn, "SELECT count(*) FROM child"), 0);
    }

    #[test]
    fn import_atomic_inside_existing_transaction_rolls_back_to_savepoint() {
        let io: Arc<dyn turso_core::IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:import-savepoint").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x UNIQUE)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES('outer')").unwrap();

        let path = std::env::temp_dir().join(format!(
            "turso-import-savepoint-{}-{}.csv",
            std::process::id(),
            unique_test_id()
        ));
        fs::write(&path, "a\na\nb\n").unwrap();

        let mut output = Vec::new();
        let mut error = Vec::new();
        let mut importer = ImportFile::new(conn.clone(), &mut output, &mut error);
        let failed = importer.import_csv(ImportArgs {
            csv: true,
            atomic: true,
            verbose: false,
            skip: 0,
            file: path.clone(),
            table: "t".to_string(),
        });
        let _ = fs::remove_file(path);

        assert!(failed);
        assert!(!conn.get_auto_commit());
        assert_eq!(select_count(&conn, "SELECT count(*) FROM t"), 1);
        conn.execute("COMMIT").unwrap();
        assert_eq!(select_count(&conn, "SELECT count(*) FROM t"), 1);
        assert!(String::from_utf8(error)
            .unwrap()
            .contains("UNIQUE constraint failed: t.x"));
    }

    fn run_deferred_fk_import(atomic: bool) -> (Arc<Connection>, bool, String) {
        let io: Arc<dyn turso_core::IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:import-commit-failure").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA foreign_keys = ON").unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute(
            "CREATE TABLE child(parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        )
        .unwrap();

        let path = std::env::temp_dir().join(format!(
            "turso-import-deferred-fk-{}-{}.csv",
            std::process::id(),
            unique_test_id()
        ));
        fs::write(&path, "1\n").unwrap();

        let mut output = Vec::new();
        let mut error = Vec::new();
        let mut importer = ImportFile::new(conn.clone(), &mut output, &mut error);
        let failed = importer.import_csv(ImportArgs {
            csv: true,
            atomic,
            verbose: false,
            skip: 0,
            file: path.clone(),
            table: "child".to_string(),
        });
        let _ = fs::remove_file(path);

        (conn, failed, String::from_utf8(error).unwrap())
    }

    fn select_count(conn: &Arc<Connection>, sql: &str) -> i64 {
        let mut count = None;
        let mut stmt = conn.query(sql).unwrap().unwrap();
        stmt.run_with_row_callback(|row| {
            let value = row.get::<&Value>(0)?;
            count = numeric_integer(value);
            Ok(())
        })
        .unwrap();
        count.unwrap()
    }

    fn numeric_integer(value: &Value) -> Option<i64> {
        match value {
            Value::Numeric(Numeric::Integer(i)) => Some(*i),
            _ => None,
        }
    }

    fn unique_test_id() -> usize {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        NEXT_ID.fetch_add(1, Ordering::SeqCst)
    }
}
