use chumsky::{error::EmptyErr, prelude::*};
use std::borrow::Cow;
use std::num::NonZero;
use std::str;
use std::sync::Arc;
use std::task::Waker;

use crate::catalog::{self, PostgresDialect};
use crate::error::{in_failed_sql_transaction_error, sqlstate_error};
use crate::functions::{find_parameter, ConfigParameter, Parameter};
use crate::settings::{ConnectionSettings, PgSessionState, SettingScope};
use crate::statement::{PgTxState, PostRunFn, TxRole};
use turso_core::schema::{Table, SEQ_BACKING_TABLE_PREFIX};
use turso_core::{
    Connection, IOResult, LimboError, Qualifier, Result, Statement, StepResult, Value,
};
use turso_parser::ast::{self};
use turso_pg_parser::foreign_key_tables;
use turso_pg_parser::sqlstate::{
    SQLSTATE_ACTIVE_SQL_TRANSACTION, SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INSUFFICIENT_PRIVILEGE, SQLSTATE_INVALID_NAME,
    SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_INVALID_SCHEMA_NAME,
    SQLSTATE_OBJECT_NOT_IN_PREREQUISITE_STATE, SQLSTATE_RESERVED_NAME, SQLSTATE_WRONG_OBJECT_TYPE,
};
use turso_pg_parser::translator::{
    execution_takes_snapshot, is_drop_matview, is_tx_exit_stmt, session_stmt,
    set_transaction_access_mode, unnamed_index_name, unqualified_create_table_name_location,
    PgCopyFromStmt, PgCreateSchemaStmt, PgDropSchemaStmt, PgSetStmt, PgSetTo, PgSetValue,
    PgShowStmt, PostgreSQLTranslator, SessionStmt,
};
use turso_pg_parser::ParseError;

use crate::copy::parse_copy_text_format;

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

struct PgConnectionInner {
    conn: Arc<Connection>,
    tx: Arc<PgTxState>,
}

/// The transaction state of a session as PostgreSQL reports it in
/// ReadyForQuery: idle, inside a transaction block, or inside a failed
/// transaction block.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TransactionState {
    Idle,
    Active,
    Aborted,
}

const SEARCH_PATH_SETTING: &str = "search_path";

pub(crate) const DEFAULT_SEARCH_PATH: &str = "\"$user\", public";

/// Open a database with the PostgreSQL schema dialect, resolving the IO
/// backend from `vfs` or the path like [`turso_core::Database::open_new`].
pub fn open_database(
    path: &str,
    vfs: Option<&str>,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> crate::Result<(Arc<dyn turso_core::IO>, Arc<turso_core::Database>)> {
    let io = match vfs {
        Some(vfs) => turso_core::Database::io_for_vfs(vfs)?,
        None => turso_core::Database::io_for_path(path)?,
    };
    let db = open_database_with_io(io.clone(), path, flags, opts)?;
    Ok((io, db))
}

/// Open a database with the PostgreSQL schema dialect on an existing IO
/// backend.
pub fn open_database_with_io(
    io: Arc<dyn turso_core::IO>,
    path: &str,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> crate::Result<Arc<turso_core::Database>> {
    let is_empty =
        turso_core::is_memory_like(path) || io.open_file(path, flags, true)?.size()? == 0;
    if !is_empty {
        refuse_a_file_with_tables_in_no_schema(&io, path, flags, opts)?;
    }
    let file = io.open_file(path, flags, true)?;
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    turso_core::Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(PostgresDialect))
            .storage(db_file)
            .flags(flags)
            .db_opts(opts.with_mvcc(true)),
    )
    .map_err(Into::into)
}

/// Refuses an existing file whose tables are in no schema before it is
/// opened with MVCC, which converts a file that is not in MVCC mode yet:
/// looking at it read-only and without MVCC first leaves a refused file as
/// it was. A file in MVCC mode is not converted, so connecting checks it.
fn refuse_a_file_with_tables_in_no_schema(
    io: &Arc<dyn turso_core::IO>,
    path: &str,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> crate::Result<()> {
    let db = turso_core::Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(PostgresDialect))
            .flags(flags | turso_core::OpenFlags::ReadOnly)
            .db_opts(opts.with_mvcc(false)),
    )?;
    if db.mvcc_enabled() {
        return Ok(());
    }
    let conn = db.connect(turso_core::ConnectOpts::new())?;
    refuse_tables_in_no_schema(&conn)?;
    Ok(())
}

impl PgConnection {
    pub fn connect(db: &Arc<turso_core::Database>) -> crate::Result<Self> {
        Self::connect_with(db, turso_core::ConnectOpts::new())
    }

    /// Opens a session on `db`, connecting with `opts`, such as the
    /// encryption key. The session has its own transaction and settings.
    pub fn connect_with(
        db: &Arc<turso_core::Database>,
        opts: turso_core::ConnectOpts,
    ) -> crate::Result<Self> {
        let conn = db.connect_with_hooks(opts, Arc::new(PgSessionState::default()))?;
        refuse_tables_in_no_schema(&conn)?;
        set_search_path(&conn, None, SettingScope::Session)?;
        Ok(Self {
            inner: Arc::new(PgConnectionInner {
                conn,
                tx: Arc::new(PgTxState::default()),
            }),
        })
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.inner.conn
    }

    /// Record an error the caller raised on this session's behalf before
    /// reaching prepare — the wire server failing to parse a query string,
    /// for example. In PostgreSQL any error inside a transaction block
    /// aborts the block.
    pub fn abort_transaction_block_on_error(&self) {
        if !self.inner.conn.get_auto_commit() {
            self.inner.tx.abort();
        }
    }

    pub fn transaction_state(&self) -> TransactionState {
        if self.inner.tx.is_aborted() {
            TransactionState::Aborted
        } else if !self.inner.conn.get_auto_commit() {
            TransactionState::Active
        } else {
            TransactionState::Idle
        }
    }

    pub fn prepare(&self, sql: impl AsRef<str>) -> crate::Result<crate::Statement> {
        prepare_statement(&self.inner, sql.as_ref()).map_err(Into::into)
    }

    pub fn prepare_copy_to_stdout(
        &self,
        spec: &turso_pg_parser::copy::CopyToStdout,
    ) -> crate::Result<crate::Statement> {
        // Same aborted-block rules as prepare_statement: COPY is rejected
        // with 25P02 while the block is aborted, and a prepare error inside
        // a transaction block aborts it.
        if self.inner.tx.is_aborted() {
            return Err(in_failed_sql_transaction_error().into());
        }
        let result = crate::copy::prepare_copy_to_stdout(&self.inner.conn, spec);
        if result.is_err() && !self.inner.conn.get_auto_commit() {
            self.inner.tx.abort();
            self.inner.tx.mark_query_started();
        }
        result
            .map(|stmt| {
                crate::Statement::new(stmt)
                    .with_tx_tracking(
                        Arc::clone(&self.inner.conn),
                        Arc::clone(&self.inner.tx),
                        TxRole::Other,
                    )
                    .with_query_start_marking(true)
            })
            .map_err(Into::into)
    }

    pub fn query(&self, sql: impl AsRef<str>) -> crate::Result<Option<crate::Statement>> {
        let sql = sql.as_ref().trim();
        if sql.is_empty() {
            return Ok(None);
        }
        self.prepare(sql).map(Some)
    }

    pub fn execute(&self, sql: impl AsRef<str>) -> crate::Result<()> {
        for stmt in self.query_runner(sql.as_ref().as_bytes()) {
            if let Some(mut stmt) = stmt? {
                stmt.run_ignore_rows()?;
            }
        }
        Ok(())
    }

    pub fn close(&self) -> crate::Result<()> {
        self.inner.conn.close().map_err(Into::into)
    }

    pub fn pragma_update(&self, name: &str, value: impl std::fmt::Display) -> Result<()> {
        self.inner.conn.pragma_update(name, value)?;
        Ok(())
    }

    pub fn query_runner<'a>(&'a self, sql: &'a [u8]) -> PgQueryRunner<'a> {
        PgQueryRunner::new(&self.inner, sql)
    }
}

pub struct PgQueryRunner<'a> {
    conn: &'a Arc<PgConnectionInner>,
    stmts: Vec<String>,
    index: usize,
}

impl<'a> PgQueryRunner<'a> {
    fn new(conn: &'a Arc<PgConnectionInner>, sql: &'a [u8]) -> Self {
        let sql = str::from_utf8(sql).unwrap_or("");
        Self {
            conn,
            stmts: split_statements(sql)
                .unwrap_or_else(|_| vec![sql.trim().to_string()])
                .into_iter()
                .filter(|stmt| !stmt.trim().is_empty())
                .collect(),
            index: 0,
        }
    }
}

impl Iterator for PgQueryRunner<'_> {
    type Item = crate::Result<Option<crate::Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.stmts.len() {
            return None;
        }

        let sql = &self.stmts[self.index];
        self.index += 1;
        let res = prepare_statement(self.conn, sql)
            .map(Some)
            .map_err(Into::into);
        Some(res)
    }
}

pub fn split_statements(sql: &str) -> crate::Result<Vec<String>> {
    match turso_pg_parser::split_statements(sql) {
        Ok(stmts) if stmts.is_empty() && !sql.trim().is_empty() => Ok(vec![sql.trim().to_string()]),
        Ok(stmts) => Ok(stmts),
        Err(_) => Ok(vec![sql.trim().to_string()]),
    }
}

/// A database whose tables are in the root namespace, in no schema, was
/// made by an earlier version of tursopg or by SQLite. This version keeps
/// every table in a schema and would hide those, so it refuses the
/// database instead.
fn refuse_tables_in_no_schema(conn: &Arc<Connection>) -> Result<()> {
    let schema = conn.current_schema();
    let has_tables_in_no_schema = schema
        .tables(turso_core::NamespaceId::ROOT)
        .any(|table| table.btree().is_some() && !catalog::is_system_table_name(table.get_name()))
        || schema.views(turso_core::NamespaceId::ROOT).next().is_some();
    if has_tables_in_no_schema {
        return Err(LimboError::ParseError(
            "the database keeps its tables in no schema, as an earlier version of tursopg or \
             SQLite made it; this version does not open it"
                .to_string(),
        ));
    }
    Ok(())
}

fn apply_tx_access_mode(
    conn: &Arc<Connection>,
    tx: &PgTxState,
    read_only: bool,
) -> crate::Result<()> {
    if !read_only && conn.is_tx_read_only() {
        if tx.inside_savepoint() {
            return Err(sqlstate_error(
                SQLSTATE_ACTIVE_SQL_TRANSACTION,
                "cannot set transaction read-write mode inside a read-only transaction",
            )
            .into());
        }
        if tx.query_started() {
            return Err(sqlstate_error(
                SQLSTATE_ACTIVE_SQL_TRANSACTION,
                "transaction read-write mode must be set before any query",
            )
            .into());
        }
    }
    conn.set_tx_read_only(read_only);
    Ok(())
}

fn is_no_active_tx_error(e: &crate::Error) -> bool {
    matches!(e.core_error(), LimboError::NoActiveTransaction { .. })
}

fn is_nested_begin_error(e: &crate::Error) -> bool {
    matches!(e.core_error(), LimboError::TransactionAlreadyActive)
}

fn prepare_statement(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<crate::Statement> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(LimboError::InvalidArgument(
            "The supplied SQL string contains no statements".to_string(),
        ));
    }
    let was_aborted = pg_conn.tx.is_aborted();
    let result = prepare_statement_inner(pg_conn, sql);
    // In PostgreSQL any error inside a transaction block aborts the block,
    // including errors raised before execution: syntax errors, unknown
    // objects, unsupported statements. A snapshot-taking statement that
    // parses but fails later, like one naming an unknown table, has already
    // taken its snapshot in PostgreSQL, so it also counts as the
    // transaction's first query — unless the block was already aborted, in
    // which case PostgreSQL rejects it before doing anything.
    if result.is_err() && !pg_conn.conn.get_auto_commit() {
        pg_conn.tx.abort();
        if !was_aborted
            && turso_pg_parser::parse(sql).is_ok_and(|parsed| execution_takes_snapshot(&parsed))
        {
            pg_conn.tx.mark_query_started();
        }
    }
    result
}

fn prepare_statement_inner(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
) -> Result<crate::Statement> {
    reject_sqlite_catalog_access(sql)?;

    let parse_result =
        turso_pg_parser::parse(sql).map_err(|e| LimboError::ParseError(e.to_string()))?;

    // In an aborted transaction block PostgreSQL accepts only the statements
    // that end it and rejects everything else with 25P02.
    if pg_conn.tx.is_aborted() && !is_tx_exit_stmt(&parse_result) {
        return Err(in_failed_sql_transaction_error());
    }

    // The single definition of what counts as the transaction's first
    // query, for every kind of statement this function returns.
    let marks_query_start = execution_takes_snapshot(&parse_result);

    if let Some(stmt) = try_prepare_special(pg_conn, &parse_result)? {
        return Ok(stmt
            .with_tx_tracking(
                Arc::clone(&pg_conn.conn),
                Arc::clone(&pg_conn.tx),
                TxRole::Other,
            )
            .with_query_start_marking(marks_query_start));
    }

    let rewritten_sql = if let Some((location, schema)) =
        create_table_schema_from_search_path(pg_conn, &parse_result)?
    {
        Some(turso_pg_parser::qualify_create_table_name(
            sql, location, &schema,
        ))
    } else {
        unnamed_index_name(sql, &parse_result, |schema, table, name| {
            relation_name_is_taken(&pg_conn.conn, schema, table, name)
        })?
        .map(|(location, name)| turso_pg_parser::name_create_index(sql, location, &name))
    };
    let (sql, parse_result) = match rewritten_sql {
        Some(sql) => {
            let parse_result =
                turso_pg_parser::parse(&sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
            (Cow::Owned(sql), parse_result)
        }
        None => (Cow::Borrowed(sql), parse_result),
    };
    let (sql, parse_result) = name_foreign_key_tables(&pg_conn.conn, sql, parse_result)?;
    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(&parse_result)
        .map_err(|e| translation_error(&pg_conn.conn, e))?;
    reject_catalog_dml(translated.cmd.stmt())?;
    if let ast::Stmt::DropView { view_name, .. } = translated.cmd.stmt() {
        reject_drop_of_other_view_kind(&pg_conn.conn, view_name, is_drop_matview(&parse_result))?;
    }

    let conn = Arc::clone(&pg_conn.conn);
    let tx = Arc::clone(&pg_conn.tx);
    let role = tx_role(translated.cmd.stmt());
    let post_run = tx_state_post_run(pg_conn, translated.cmd.stmt(), role);

    if translated.prereqs.is_empty() {
        return pg_conn
            .conn
            .prepare_translated_cmd(translated.cmd, &sql)
            .map_err(schema_error)
            .map(|s| {
                crate::Statement::new(s)
                    .with_tx_tracking(conn, tx, role)
                    .with_query_start_marking(marks_query_start)
                    .with_post_run(post_run)
            });
    }

    // Validate prerequisites and the target command during prepare without
    // executing any writes. Side-effecting prerequisites run atomically with
    // the command only when the returned Statement is actually executed.
    for prereq in &translated.prereqs {
        let input = prereq.to_string();
        let _ = pg_conn
            .conn
            .prepare_translated_stmt(prereq.clone(), &input)
            .map_err(schema_error)?;
    }
    let cmd = translated.cmd;
    let inner = pg_conn
        .conn
        .prepare_translated_cmd(cmd.clone(), &sql)
        .map_err(schema_error)?;
    Ok(crate::Statement::with_deferred(
        inner,
        DeferredExecution::new(
            pg_conn,
            DeferredOperation::Translated {
                prereqs: translated.prereqs,
                cmd: Box::new(cmd),
                sql: sql.into_owned(),
            },
        ),
    )
    .with_tx_tracking(conn, tx, role)
    .with_query_start_marking(marks_query_start)
    .with_post_run(post_run))
}

/// Connection and session state changes that must follow a statement's
/// execute-time outcome, and error replacements that depend on it.
///
/// A non-nested BEGIN applies its access mode in the VDBE, so its post_run
/// only handles the nested case: PostgreSQL warns, keeps the transaction,
/// and still applies the access mode, which replaces the core error. A
/// COMMIT or ROLLBACK outside a transaction block warns and succeeds in
/// PostgreSQL, so the core error is dropped. Savepoint statements maintain
/// the per-savepoint access modes, which PostgreSQL restores when the
/// subtransaction ends.
fn tx_state_post_run(
    pg_conn: &Arc<PgConnectionInner>,
    stmt: &ast::Stmt,
    role: TxRole,
) -> PostRunFn {
    let conn = Arc::clone(&pg_conn.conn);
    let tx = Arc::clone(&pg_conn.tx);
    match role {
        TxRole::Begin => {
            let access_mode = match stmt {
                ast::Stmt::Begin { access_mode, .. } => *access_mode,
                _ => None,
            };
            Arc::new(move |result: crate::Result<()>| match result {
                Ok(()) => {
                    tx.transaction_began();
                    Ok(())
                }
                Err(ref e) if is_nested_begin_error(e) => {
                    if let Some(mode) = access_mode {
                        apply_tx_access_mode(&conn, &tx, mode == ast::AccessMode::ReadOnly)?;
                    }
                    Ok(())
                }
                other => other,
            })
        }
        TxRole::Commit | TxRole::Rollback => Arc::new(|result: crate::Result<()>| match result {
            Err(ref e) if is_no_active_tx_error(e) => Ok(()),
            other => other,
        }),
        TxRole::RollbackToSavepoint => {
            let name = savepoint_name(stmt);
            Arc::new(move |result: crate::Result<()>| {
                result?;
                if let Some(name) = &name {
                    tx.savepoint_rolled_back(&conn, name);
                }
                Ok(())
            })
        }
        TxRole::Other => match stmt {
            ast::Stmt::Savepoint { name } => {
                let name = name.as_str().to_string();
                Arc::new(move |result: crate::Result<()>| {
                    result?;
                    tx.savepoint_created(&conn, &name);
                    Ok(())
                })
            }
            ast::Stmt::Release { name } => {
                let name = name.as_str().to_string();
                Arc::new(move |result: crate::Result<()>| {
                    result?;
                    tx.savepoint_released(&conn, &name);
                    Ok(())
                })
            }
            _ => Arc::new(|result| result),
        },
    }
}

fn savepoint_name(stmt: &ast::Stmt) -> Option<String> {
    match stmt {
        ast::Stmt::Rollback {
            savepoint_name: Some(name),
            ..
        } => Some(name.as_str().to_string()),
        _ => None,
    }
}

fn tx_role(stmt: &ast::Stmt) -> TxRole {
    match stmt {
        ast::Stmt::Begin { .. } => TxRole::Begin,
        ast::Stmt::Commit { .. } => TxRole::Commit,
        ast::Stmt::Rollback {
            savepoint_name: Some(_),
            ..
        } => TxRole::RollbackToSavepoint,
        ast::Stmt::Rollback { .. } => TxRole::Rollback,
        _ => TxRole::Other,
    }
}

const PG_STMT_SAVEPOINT: &str = "__turso_pg_stmt";

#[derive(Clone, Copy, PartialEq, Eq)]
enum DeferredPhase {
    Start,
    CheckTarget,
    CopyColumns,
    Savepoint,
    Statement(usize),
    CopyRow(usize),
    Release,
    Rollback,
    RollbackRelease,
    Done,
}

enum DeferredOperation {
    Translated {
        prereqs: Vec<ast::Stmt>,
        cmd: Box<ast::Cmd>,
        sql: String,
    },
    Session(SessionStmt),
}

enum DeferredCommand {
    Translated(ast::Cmd, String),
    Namespace(ast::Stmt),
    Postgres(String),
    Sqlite(String),
}

pub(crate) struct DeferredExecution {
    pg_conn: Arc<PgConnectionInner>,
    operation: DeferredOperation,
    statements: Vec<DeferredCommand>,
    phase: DeferredPhase,
    active: Option<crate::Statement>,
    pub(crate) io_completions: Option<turso_core::types::IOCompletions>,
    pub(crate) was_in_tx: Option<bool>,
    savepoint_open: bool,
    target_exists: bool,
    copy_columns: usize,
    copy_rows: Vec<Vec<Option<String>>>,
    copy_sql: String,
    error: Option<LimboError>,
    pub(crate) interrupted: bool,
}

impl DeferredExecution {
    fn new(pg_conn: &Arc<PgConnectionInner>, operation: DeferredOperation) -> Self {
        Self {
            pg_conn: Arc::clone(pg_conn),
            operation,
            statements: Vec::new(),
            phase: DeferredPhase::Start,
            active: None,
            io_completions: None,
            was_in_tx: None,
            savepoint_open: false,
            target_exists: false,
            copy_columns: 0,
            copy_rows: Vec::new(),
            copy_sql: String::new(),
            error: None,
            interrupted: false,
        }
    }

    pub(crate) fn step(
        &mut self,
        inner: &mut Statement,
        waker: Option<&Waker>,
    ) -> Result<StepResult> {
        self.io_completions = None;
        loop {
            if self.phase == DeferredPhase::Done {
                return self.error.clone().map_or(Ok(StepResult::Done), Err);
            }
            match self.run_phase_nonblock(inner) {
                Ok(IOResult::Done(())) => continue,
                Ok(IOResult::IO(io)) => {
                    io.set_waker(waker);
                    let result = if io.is_explicit_yield() {
                        StepResult::Yield
                    } else {
                        StepResult::IO
                    };
                    self.io_completions = Some(io);
                    return Ok(result);
                }
                Err(error) => self.fail(error),
            }
        }
    }

    fn run_phase_nonblock(&mut self, inner: &mut Statement) -> Result<IOResult<()>> {
        let conn = Arc::clone(&self.pg_conn.conn);
        let mut saved_search_path = None;
        if let (
            DeferredOperation::Session(SessionStmt::CreateSchema(stmt)),
            DeferredPhase::Statement(index),
        ) = (&self.operation, self.phase)
        {
            if index > 0 {
                let setting = search_path_setting(&conn);
                let mut path = turso_pg_parser::quote_identifier(&stmt.name.to_lowercase());
                let rest = setting.as_deref().unwrap_or(DEFAULT_SEARCH_PATH).trim();
                if !rest.is_empty() {
                    path = format!("{path}, {rest}");
                }
                set_search_path(&conn, Some(&path), SettingScope::Session)?;
                saved_search_path = Some(setting);
            }
        }
        let disable_foreign_keys = conn.foreign_keys_enabled()
            && matches!(
                self.operation,
                DeferredOperation::Session(SessionStmt::DropSchema(_))
            )
            && matches!(self.phase, DeferredPhase::Statement(_));
        if disable_foreign_keys {
            conn.set_foreign_keys_enabled(false);
        }
        let result = self.run_phase(inner);
        if disable_foreign_keys {
            conn.set_foreign_keys_enabled(true);
        }
        if let Some(setting) = saved_search_path {
            set_search_path(&conn, setting.as_deref(), SettingScope::Session)?;
        }
        result
    }

    fn run_phase(&mut self, inner: &mut Statement) -> Result<IOResult<()>> {
        use DeferredPhase::*;

        if self.phase == Start {
            self.start()?;
            return Ok(IOResult::Done(()));
        }
        if self.active.is_none() {
            self.prepare_active()?;
        }
        let Some(active) = self.active.as_mut() else {
            return Ok(IOResult::Done(()));
        };
        if matches!(self.phase, Rollback | RollbackRelease) {
            active.set_query_timeout_override(Some(None));
        } else if self.interrupted {
            active.interrupt();
        }
        let result = match self.phase {
            CheckTarget => active.run_with_row_callback_nonblock(|row| {
                self.target_exists = row.get_value(0) == &Value::from_i64(1);
                Ok(())
            }),
            CopyColumns => active.run_with_row_callback_nonblock(|_| {
                self.copy_columns += 1;
                Ok(())
            }),
            _ => active.run_ignore_rows_nonblock(),
        }
        .map_err(crate::Error::into_core_error)?;
        if let IOResult::IO(io) = result {
            return Ok(IOResult::IO(io));
        }
        if self.interrupted && !matches!(self.phase, Release | Rollback | RollbackRelease) {
            if self.phase == Savepoint {
                self.savepoint_open = true;
            }
            return Err(LimboError::Interrupt);
        }
        self.finish_phase(inner)?;
        Ok(IOResult::Done(()))
    }

    fn prepare_active(&mut self) -> Result<()> {
        use DeferredPhase::*;

        let conn = &self.pg_conn.conn;
        let sql = self.phase_sql();
        self.active = Some(match self.phase {
            Statement(index) => match &self.statements[index] {
                DeferredCommand::Translated(cmd, _) => crate::Statement::new(
                    conn.prepare_translated_cmd(cmd.clone(), &sql)
                        .map_err(schema_error)?,
                ),
                DeferredCommand::Namespace(stmt) => {
                    crate::Statement::new(conn.prepare_namespace_stmt(stmt.clone())?)
                }
                DeferredCommand::Postgres(_) => {
                    prepare_statement_inner(&self.pg_conn, &sql)?.without_tx_tracking()
                }
                DeferredCommand::Sqlite(_) => crate::Statement::new(conn.prepare_sqlite(&sql)?),
            },
            CopyRow(index) => {
                let mut stmt = crate::Statement::new(conn.prepare_sqlite(&sql)?);
                if self.copy_rows.is_empty() {
                    self.phase = Release;
                    return Ok(());
                }
                bind_copy_row(&mut stmt, &self.copy_rows[index])?;
                stmt
            }
            Savepoint | Release | Rollback | RollbackRelease => {
                crate::Statement::new(conn.prepare_sqlite(&sql)?)
            }
            Start | CheckTarget | CopyColumns | Done => unreachable!(),
        });
        Ok(())
    }

    fn finish_phase(&mut self, inner: &mut Statement) -> Result<()> {
        use DeferredPhase::*;

        self.phase = match self.phase {
            CheckTarget => {
                self.active = None;
                if self.target_exists {
                    Statement(self.statements.len() - 1)
                } else {
                    Savepoint
                }
            }
            CopyColumns => {
                self.active = None;
                let DeferredOperation::Session(SessionStmt::CopyFrom(stmt)) = &self.operation
                else {
                    unreachable!()
                };
                let (sql, rows) = prepare_copy_rows(stmt, self.copy_columns)?;
                self.copy_sql = sql;
                self.copy_rows = rows;
                if self.was_in_tx == Some(true) {
                    return Err(LimboError::TransactionAlreadyActive);
                }
                Savepoint
            }
            Savepoint => {
                self.active = None;
                self.savepoint_open = true;
                if matches!(
                    self.operation,
                    DeferredOperation::Session(SessionStmt::CopyFrom(_))
                ) {
                    CopyRow(0)
                } else {
                    Statement(0)
                }
            }
            Statement(index) if index + 1 < self.statements.len() => {
                self.active = None;
                Statement(index + 1)
            }
            Statement(_) => {
                let active = self.active.take().unwrap();
                if matches!(self.operation, DeferredOperation::Translated { .. }) {
                    *inner = active.inner;
                }
                if self.savepoint_open {
                    Release
                } else {
                    Done
                }
            }
            CopyRow(index) if index + 1 < self.copy_rows.len() => {
                let active = self.active.as_mut().unwrap();
                active.reset().map_err(crate::Error::into_core_error)?;
                active.clear_bindings();
                bind_copy_row(active, &self.copy_rows[index + 1])?;
                CopyRow(index + 1)
            }
            CopyRow(_) => {
                self.active = None;
                Release
            }
            Rollback if self.was_in_tx == Some(true) => {
                self.active = None;
                RollbackRelease
            }
            Release | Rollback | RollbackRelease => {
                self.active = None;
                self.savepoint_open = false;
                if self.phase == Release
                    && matches!(
                        self.operation,
                        DeferredOperation::Session(SessionStmt::CopyFrom(_))
                    )
                {
                    inner.set_n_change(self.copy_rows.len() as i64);
                }
                Done
            }
            Start | Done => unreachable!(),
        };
        Ok(())
    }

    fn start(&mut self) -> Result<()> {
        let conn = &self.pg_conn.conn;
        self.was_in_tx = Some(!conn.get_auto_commit());
        if self.interrupted {
            return Err(LimboError::Interrupt);
        }
        self.phase = DeferredPhase::Savepoint;
        match &self.operation {
            DeferredOperation::Translated { prereqs, cmd, sql } => {
                self.statements = prereqs
                    .iter()
                    .map(|stmt| {
                        DeferredCommand::Translated(ast::Cmd::Stmt(stmt.clone()), stmt.to_string())
                    })
                    .collect();
                self.statements
                    .push(DeferredCommand::Translated((**cmd).clone(), sql.clone()));
                self.active =
                    prepare_create_table_target_check(conn, cmd)?.map(crate::Statement::new);
                if self.active.is_some() {
                    self.phase = DeferredPhase::CheckTarget;
                }
            }
            DeferredOperation::Session(stmt) => match stmt {
                SessionStmt::ResetAll | SessionStmt::DiscardAll => {
                    if matches!(stmt, SessionStmt::DiscardAll) {
                        discard_all(conn)?;
                    } else {
                        reset_all_settings(conn)?;
                    }
                    self.phase = DeferredPhase::Done;
                }
                SessionStmt::CreateSchema(stmt) => {
                    if conn.is_tx_read_only() {
                        return Err(LimboError::ReadOnly);
                    }
                    self.statements = create_schema_commands(conn, stmt)?;
                    if self.statements.is_empty() {
                        self.phase = DeferredPhase::Done;
                    }
                }
                SessionStmt::DropSchema(stmt) => {
                    if conn.is_tx_read_only() {
                        return Err(LimboError::ReadOnly);
                    }
                    self.statements = drop_schema_commands(conn, stmt)?;
                    if self.statements.is_empty() {
                        self.phase = DeferredPhase::Done;
                    }
                }
                SessionStmt::CopyFrom(stmt) => {
                    self.active = Some(crate::Statement::new(prepare_copy_columns(
                        conn,
                        &stmt.table_name,
                        stmt.schema_name.as_deref(),
                    )?));
                    self.phase = DeferredPhase::CopyColumns;
                }
                SessionStmt::RefreshMatview | SessionStmt::CommentOn => {
                    if conn.is_tx_read_only() {
                        return Err(LimboError::ReadOnly);
                    }
                    self.phase = DeferredPhase::Done;
                }
                SessionStmt::Set(_) | SessionStmt::SetTransaction | SessionStmt::Show(_) => {
                    unreachable!()
                }
            },
        }
        Ok(())
    }

    fn phase_sql(&self) -> Cow<'_, str> {
        use DeferredPhase::*;

        match self.phase {
            Savepoint => format!("SAVEPOINT \"{PG_STMT_SAVEPOINT}\"").into(),
            Statement(index) => match &self.statements[index] {
                DeferredCommand::Translated(_, sql)
                | DeferredCommand::Sqlite(sql)
                | DeferredCommand::Postgres(sql) => Cow::Borrowed(sql),
                DeferredCommand::Namespace(stmt) => stmt.to_string().into(),
            },
            CopyRow(_) => Cow::Borrowed(&self.copy_sql),
            Release | RollbackRelease => {
                format!("RELEASE SAVEPOINT \"{PG_STMT_SAVEPOINT}\"").into()
            }
            Rollback if self.was_in_tx == Some(true) => {
                format!("ROLLBACK TO SAVEPOINT \"{PG_STMT_SAVEPOINT}\"").into()
            }
            Rollback => Cow::Borrowed("ROLLBACK"),
            Start | CheckTarget | CopyColumns | Done => unreachable!(),
        }
    }

    fn fail(&mut self, error: LimboError) {
        let cleaning_up = matches!(
            self.phase,
            DeferredPhase::Rollback | DeferredPhase::RollbackRelease
        );
        self.active = None;
        self.error = Some(error);
        if self.pg_conn.conn.get_auto_commit() {
            self.savepoint_open = false;
        }
        self.phase = if self.savepoint_open && !cleaning_up {
            DeferredPhase::Rollback
        } else {
            if self.savepoint_open {
                self.pg_conn.tx.abort();
            }
            DeferredPhase::Done
        };
    }

    pub(crate) fn reset(&mut self) -> Result<()> {
        let io_result = match self.io_completions.take() {
            Some(io) if !io.0.is_wait() => io.wait(self.active.as_ref().unwrap()._io()),
            _ => Ok(()),
        };
        let active_result = self
            .active
            .as_mut()
            .map_or(Ok(()), crate::Statement::reset)
            .map_err(crate::Error::into_core_error);
        self.active = None;
        if self.savepoint_open && !self.pg_conn.conn.get_auto_commit() {
            let conn = &self.pg_conn.conn;
            let result = if self.was_in_tx == Some(true) {
                execute_sqlite(
                    conn,
                    format!("ROLLBACK TO SAVEPOINT \"{PG_STMT_SAVEPOINT}\""),
                )
                .and_then(|()| {
                    execute_sqlite(conn, format!("RELEASE SAVEPOINT \"{PG_STMT_SAVEPOINT}\""))
                })
            } else {
                execute_sqlite(conn, "ROLLBACK")
            };
            if result.is_err() {
                self.pg_conn.tx.abort();
            }
            result?;
        }
        self.phase = DeferredPhase::Start;
        self.statements.clear();
        self.copy_columns = 0;
        self.copy_rows.clear();
        self.copy_sql.clear();
        self.was_in_tx = None;
        self.savepoint_open = false;
        self.target_exists = false;
        self.error = None;
        self.interrupted = false;
        io_result.and(active_result)
    }
}

impl Drop for DeferredExecution {
    fn drop(&mut self) {
        if let Err(error) = self.reset() {
            self.pg_conn.tx.abort();
            tracing::error!("Failed to clean up PostgreSQL deferred statement: {error}");
        }
    }
}

fn prepare_create_table_target_check(
    conn: &Arc<Connection>,
    cmd: &ast::Cmd,
) -> Result<Option<Statement>> {
    let ast::Cmd::Stmt(ast::Stmt::CreateTable {
        if_not_exists: true,
        tbl_name,
        ..
    }) = cmd
    else {
        return Ok(None);
    };
    let default_schema;
    let schema = match &tbl_name.namespace {
        Some(ns) => Some(ns.as_str()),
        None => {
            default_schema = current_schema(conn);
            default_schema.as_deref()
        }
    };
    let Some(namespace) = relation_lookup_namespaces(conn, schema).into_iter().next() else {
        return Ok(None);
    };
    let database = database_of(conn, &namespace);
    let mut stmt = conn.prepare_sqlite(format!(
        "SELECT EXISTS (SELECT 1 FROM \"{database}\".sqlite_schema \
         WHERE type IN ('table', 'view') AND name COLLATE NOCASE = ?1)"
    ))?;
    stmt.bind_at(
        NonZero::new(1).unwrap(),
        Value::build_text(namespace.mangle_name(tbl_name.name.as_str())),
    )?;
    Ok(Some(stmt))
}

/// `sql` with each table that a foreign key names replaced by the name the
/// table is stored under, so that the stored text of the statement names
/// the table as it is stored, which is how the text is read when the
/// schema loads. A table named with a schema is in that schema. A table
/// named without one is in the first schema of the search path that has
/// it, as in PostgreSQL, or else in the schema the statement creates its
/// table in, so a table can reference itself.
fn name_foreign_key_tables<'a>(
    conn: &Arc<Connection>,
    sql: Cow<'a, str>,
    parse_result: turso_pg_parser::ParseResult,
) -> Result<(Cow<'a, str>, turso_pg_parser::ParseResult)> {
    let tables = foreign_key_tables(&parse_result);
    if tables.is_empty() {
        return Ok((sql, parse_result));
    }
    let created = created_table(conn, &parse_result);
    let mut sql = sql.into_owned();
    for table in tables.iter().rev() {
        let namespace = match table.schema.as_deref() {
            Some(schema) => conn.qualifier(&schema.to_lowercase()).ok_or_else(|| {
                LimboError::ParseError(format!("schema \"{schema}\" does not exist"))
            })?,
            None => foreign_key_table_namespace(conn, &table.name, created.as_ref()).ok_or_else(
                || LimboError::ParseError("no schema has been selected to create in".to_string()),
            )?,
        };
        sql = turso_pg_parser::name_at(&sql, table.location, &namespace.mangle_name(&table.name))
            .map_err(|e| LimboError::ParseError(e.to_string()))?;
    }
    let parse_result =
        turso_pg_parser::parse(&sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
    Ok((Cow::Owned(sql), parse_result))
}

/// The schema and name of the table that a CREATE TABLE statement creates.
fn created_table(
    conn: &Arc<Connection>,
    parse_result: &turso_pg_parser::ParseResult,
) -> Option<(Qualifier, String)> {
    let (schema, name) = turso_pg_parser::create_table_name(parse_result)?;
    let schema = match schema {
        Some(schema) => schema.to_lowercase(),
        None => current_schema(conn)?,
    };
    Some((conn.qualifier(&schema)?, name))
}

/// The schema that a foreign key target named without a schema is in: the
/// first schema of the search path that has a table of that name, counting
/// the table the statement creates, which PostgreSQL makes before its
/// foreign keys; or else the first schema of the search path.
fn foreign_key_table_namespace(
    conn: &Arc<Connection>,
    name: &str,
    created: Option<&(Qualifier, String)>,
) -> Option<Qualifier> {
    let schema = conn.current_schema();
    let candidates: Vec<Qualifier> = conn
        .search_order()
        .into_iter()
        .filter(|namespace| namespace.database_id() == turso_core::MAIN_DB_ID)
        .collect();
    candidates
        .iter()
        .find(|namespace| {
            schema.has_table(namespace.target.id, name)
                || created.is_some_and(|(created_namespace, created_name)| {
                    created_namespace == *namespace && created_name == name
                })
        })
        .or_else(|| candidates.first())
        .cloned()
}

/// Core reports a name whose namespace does not exist as a missing
/// database; in PostgreSQL that is a missing schema.
fn schema_error(error: LimboError) -> LimboError {
    let LimboError::InvalidArgument(message) = &error else {
        return error;
    };
    match message.strip_prefix("no such database: ") {
        Some(schema) => LimboError::ParseError(format!("schema \"{schema}\" does not exist")),
        None => error,
    }
}

/// The schema that a CREATE TABLE statement naming its table without a
/// schema creates the table in, with the byte offset of the table name: the
/// first schema of `search_path` that exists, like PostgreSQL. None when the
/// statement is not such a CREATE TABLE.
fn create_table_schema_from_search_path(
    pg_conn: &Arc<PgConnectionInner>,
    parse_result: &turso_pg_parser::ParseResult,
) -> Result<Option<(usize, String)>> {
    let Some(location) = unqualified_create_table_name_location(parse_result) else {
        return Ok(None);
    };
    let schema = current_schema(&pg_conn.conn).ok_or_else(|| {
        sqlstate_error(
            SQLSTATE_INVALID_SCHEMA_NAME,
            "no schema has been selected to create in",
        )
    })?;
    Ok(Some((location, schema)))
}

/// Whether the schema of table `table` already has a table, index, view or
/// sequence named `name`, which makes PostgreSQL choose another name for a
/// new index on the table. A table named with a schema is in that schema.
/// Otherwise the table is looked up in the same order as core does: temp
/// first, then the namespaces of the search path.
fn relation_name_is_taken(
    conn: &Arc<Connection>,
    schema: Option<&str>,
    table: &str,
    name: &str,
) -> Result<bool> {
    let namespaces = relation_lookup_namespaces(conn, schema);
    let mut taken = false;
    for namespace in namespaces {
        let database = database_of(conn, &namespace);
        let mut stmt = conn.prepare_sqlite(format!(
            "SELECT \
             EXISTS (SELECT 1 FROM \"{database}\".sqlite_schema \
                     WHERE type = 'table' AND name COLLATE NOCASE = ?1), \
             EXISTS (SELECT 1 FROM \"{database}\".sqlite_schema \
                     WHERE type IN ('table', 'index', 'view') \
                     AND name COLLATE NOCASE IN (?2, '{SEQ_BACKING_TABLE_PREFIX}' || ?2))"
        ))?;
        stmt.bind_at(
            NonZero::new(1).unwrap(),
            Value::build_text(namespace.mangle_name(table)),
        )?;
        stmt.bind_at(
            NonZero::new(2).unwrap(),
            Value::build_text(namespace.mangle_name(name)),
        )?;
        let row = &stmt.run_collect_rows()?[0];
        taken = row[1] == Value::from_i64(1);
        if row[0] == Value::from_i64(1) {
            break;
        }
    }
    Ok(taken)
}

/// PostgreSQL refuses DROP VIEW and DROP MATERIALIZED VIEW on a relation of
/// another kind, but core's DROP VIEW drops both kinds of view. The relation
/// is the first table or view named `view_name` in the databases that core
/// looks it up in. A view is materialized when it has a root page.
fn reject_drop_of_other_view_kind(
    conn: &Arc<Connection>,
    view_name: &ast::QualifiedName,
    materialized: bool,
) -> Result<()> {
    let expected_kind = if materialized {
        "materialized view"
    } else {
        "view"
    };
    let schema = view_name.namespace.as_ref().map(|name| name.as_str());
    for namespace in relation_lookup_namespaces(conn, schema) {
        let database = database_of(conn, &namespace);
        let mut stmt = conn.prepare_sqlite(format!(
            "SELECT CASE WHEN type = 'table' THEN 'table' \
                         WHEN rootpage = 0 THEN 'view' \
                         ELSE 'materialized view' END \
             FROM \"{database}\".sqlite_schema \
             WHERE type IN ('table', 'view') AND name COLLATE NOCASE = ?1"
        ))?;
        stmt.bind_at(
            NonZero::new(1).unwrap(),
            Value::build_text(namespace.mangle_name(view_name.name.as_str())),
        )?;
        let Some(row) = stmt.run_collect_rows()?.into_iter().next() else {
            continue;
        };
        if row[0] != Value::build_text(expected_kind) {
            return Err(sqlstate_error(
                SQLSTATE_WRONG_OBJECT_TYPE,
                format!("\"{}\" is not a {expected_kind}", view_name.name.as_str()),
            ));
        }
        return Ok(());
    }
    Ok(())
}

/// The namespaces that a relation is looked up in, in order. A relation
/// named with a schema is in that schema. Otherwise it is looked up in the
/// same order as core does: temp first, then the namespaces of the search
/// path.
pub(crate) fn relation_lookup_namespaces(
    conn: &Arc<Connection>,
    schema: Option<&str>,
) -> Vec<Qualifier> {
    match schema {
        Some(schema) => conn.qualifier(&schema.to_lowercase()).into_iter().collect(),
        None => {
            let has_temp = conn
                .list_all_databases()
                .iter()
                .any(|(id, _, _)| *id == turso_core::TEMP_DB_ID);
            has_temp
                .then(|| Qualifier::database("temp", turso_core::TEMP_DB_ID))
                .into_iter()
                .chain(conn.search_order())
                .collect()
        }
    }
}

/// The name of the database that has the objects of `namespace`, for
/// reading its sqlite_schema.
pub(crate) fn database_of(conn: &Connection, namespace: &Qualifier) -> String {
    conn.namespace_name(namespace.database_id())
        .expect("the database of a namespace has a name")
}

/// The first schema of `search_path` that exists, which is what PostgreSQL's
/// current_schema() returns. None when no schema of `search_path` exists.
/// The default search_path, "$user", public, gives public, since there is no
/// schema named after a user.
pub(crate) fn current_schema(conn: &Connection) -> Option<String> {
    conn.search_order()
        .into_iter()
        .next()
        .map(|namespace| namespace.name)
}

pub(crate) fn current_schemas(conn: &Connection, include_implicit: bool) -> Vec<String> {
    let mut schemas = Vec::new();
    for name in conn
        .search_path()
        .unwrap_or_else(|| vec!["public".to_owned()])
    {
        if catalog::namespace_exists(conn, &name) && !schemas.contains(&name) {
            schemas.push(name);
        }
    }
    if include_implicit && !schemas.iter().any(|name| name == "pg_catalog") {
        schemas.insert(0, "pg_catalog".to_owned());
    }
    schemas
}

fn reject_catalog_dml(stmt: &ast::Stmt) -> Result<()> {
    let table_name = match stmt {
        ast::Stmt::Insert { tbl_name, .. } => Some(tbl_name.name.as_str()),
        ast::Stmt::Delete { tbl_name, .. } => Some(tbl_name.name.as_str()),
        ast::Stmt::Update(update) => Some(update.tbl_name.name.as_str()),
        _ => None,
    };

    let Some(table_name) = table_name else {
        return Ok(());
    };

    if !catalog::is_catalog_table_name(table_name) {
        return Ok(());
    }

    let verb = match stmt {
        ast::Stmt::Insert { .. } => "insert into",
        ast::Stmt::Delete { .. } => "delete from",
        ast::Stmt::Update { .. } => "update",
        _ => unreachable!(),
    };
    if catalog::is_catalog_view_name(table_name) {
        return Err(sqlstate_error(
            SQLSTATE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            format!("cannot {verb} view \"{table_name}\""),
        ));
    }
    Err(sqlstate_error(
        SQLSTATE_INSUFFICIENT_PRIVILEGE,
        format!("cannot {verb} pg_catalog table \"{table_name}\""),
    ))
}

fn reject_sqlite_catalog_access(sql: &str) -> Result<()> {
    let lower = sql.to_ascii_lowercase();
    for table_name in ["sqlite_master", "sqlite_schema"] {
        if lower.contains(table_name) {
            return Err(LimboError::ParseError(format!(
                "no such table: {table_name}"
            )));
        }
    }
    Ok(())
}

fn try_prepare_special(
    pg_conn: &Arc<PgConnectionInner>,
    parse_result: &turso_pg_parser::ParseResult,
) -> Result<Option<crate::Statement>> {
    let Some(stmt) = session_stmt(parse_result).map_err(|e| translation_error(&pg_conn.conn, e))?
    else {
        return Ok(None);
    };
    let stmt = match stmt {
        SessionStmt::Set(set_stmt) => crate::Statement::new(handle_pg_set(pg_conn, &set_stmt)?),
        SessionStmt::SetTransaction => prepare_set_transaction(pg_conn, parse_result)?,
        SessionStmt::Show(show_stmt) => crate::Statement::new(handle_pg_show(pg_conn, &show_stmt)?),
        stmt @ (SessionStmt::ResetAll
        | SessionStmt::DiscardAll
        | SessionStmt::CreateSchema(_)
        | SessionStmt::DropSchema(_)
        | SessionStmt::CopyFrom(_)
        | SessionStmt::RefreshMatview
        | SessionStmt::CommentOn) => crate::Statement::with_deferred(
            noop_statement(&pg_conn.conn)?,
            DeferredExecution::new(pg_conn, DeferredOperation::Session(stmt)),
        ),
    };
    Ok(Some(stmt))
}

/// The engine error for an error of the translator.
fn translation_error(conn: &Arc<Connection>, error: ParseError) -> LimboError {
    match error {
        ParseError::ParseError(message) => LimboError::ParseError(message),
        ParseError::Sqlstate { sqlstate, message } => sqlstate_error(sqlstate, message),
        ParseError::SchemaQualifiedName { ref schema, .. }
            if !catalog::namespace_exists(conn, schema) =>
        {
            sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                format!("schema \"{schema}\" does not exist"),
            )
        }
        ParseError::SchemaQualifiedName { .. } => {
            sqlstate_error(SQLSTATE_FEATURE_NOT_SUPPORTED, error.to_string())
        }
    }
}

/// PostgreSQL applies SET TRANSACTION only inside a transaction block;
/// outside one it warns and changes nothing.
fn prepare_set_transaction(
    pg_conn: &Arc<PgConnectionInner>,
    parse_result: &turso_pg_parser::ParseResult,
) -> Result<crate::Statement> {
    let access_mode = set_transaction_access_mode(parse_result);
    let inner = noop_statement(&pg_conn.conn)?;
    let conn = pg_conn.conn.clone();
    let tx = Arc::clone(&pg_conn.tx);
    let post_run = std::sync::Arc::new(move |result: crate::Result<()>| {
        result?;
        if conn.get_auto_commit() {
            return Ok(());
        }
        if let Some(read_only) = access_mode {
            apply_tx_access_mode(&conn, &tx, read_only)?;
        }
        Ok(())
    });
    Ok(crate::Statement::new_with_post_run(inner, post_run))
}

fn noop_statement(conn: &Arc<Connection>) -> Result<Statement> {
    conn.prepare("SELECT 0 WHERE 0")
}

fn execute_sqlite(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    let mut stmt = conn.prepare_sqlite(sql)?;
    stmt.run_ignore_rows()
}

fn handle_pg_set(pg_conn: &Arc<PgConnectionInner>, set_stmt: &PgSetStmt) -> Result<Statement> {
    let name = &set_stmt.name;
    let quote = |text: &str| format!("'{}'", text.replace('\'', "''"));
    let value = match (find_parameter(name), &set_stmt.to) {
        // A NULL value sets the default, as RESET does.
        (Some(_), PgSetTo::Default) => "NULL".to_string(),
        (Some(_), PgSetTo::Current) => format!("current_setting({})", quote(name)),
        (Some(Parameter::BuiltIn(ConfigParameter::SearchPath)), PgSetTo::Values(values)) => quote(
            &values
                .iter()
                .map(|value| {
                    value
                        .as_search_path_name()
                        .map(turso_pg_parser::quote_identifier)
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| LimboError::ParseError("incorrect format".to_string()))?
                .join(", "),
        ),
        (Some(Parameter::Custom(_)), PgSetTo::Values(values)) => match values.as_slice() {
            [PgSetValue::Identifier(value)
            | PgSetValue::StringLiteral(value)
            | PgSetValue::Number(value)] => quote(value),
            [_] => return Err(LimboError::ParseError("incorrect format".to_string())),
            _ => {
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_PARAMETER_VALUE,
                    format!("SET {name} takes only one argument"),
                ))
            }
        },
        (None, PgSetTo::Values(values)) if set_value_already_in_effect(name, values) => {
            return noop_statement(&pg_conn.conn);
        }
        (None, to) => {
            let command = match to {
                PgSetTo::Default => "RESET",
                PgSetTo::Values(_) | PgSetTo::Current => "SET",
            };
            return Err(LimboError::ParseError(format!(
                "{command} {name} is not supported"
            )));
        }
    };
    // Change the setting when the statement runs, not when it is prepared.
    // set_config returns text, so IS NULL prevents SET from returning rows.
    pg_conn.conn.prepare(format!(
        "SELECT 0 WHERE set_config({}, {value}, {}) IS NULL",
        quote(name),
        set_stmt.is_local
    ))
}

fn set_value_already_in_effect(name: &str, values: &[PgSetValue]) -> bool {
    let Some(values) = values
        .iter()
        .map(|value| match value {
            PgSetValue::Identifier(s) | PgSetValue::StringLiteral(s) | PgSetValue::Number(s) => {
                Some(s.to_ascii_lowercase())
            }
            PgSetValue::Bool(b) => Some(if *b { "on" } else { "off" }.to_string()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let value = values.join(", ");
    let is_on = matches!(value.as_str(), "on" | "true" | "1");
    let is_off = matches!(value.as_str(), "off" | "false" | "0");
    match name.to_ascii_lowercase().as_str() {
        "statement_timeout" | "lock_timeout" | "idle_in_transaction_session_timeout" => {
            value == "0"
        }
        "datestyle" => matches!(value.as_str(), "iso" | "iso, mdy" | "iso, ymd" | "iso, dmy"),
        "intervalstyle" => value == "postgres",
        "client_encoding" => matches!(value.as_str(), "utf8" | "utf-8" | "unicode"),
        "extra_float_digits" => matches!(value.as_str(), "1" | "2" | "3"),
        "standard_conforming_strings" => is_on,
        "row_security" | "quote_all_identifiers" => is_off,
        "check_function_bodies" | "synchronize_seqscans" => is_on || is_off,
        "default_tablespace" => value.is_empty(),
        "default_table_access_method" => value == "heap",
        "transaction_timeout" => value == "0",
        "xmloption" => value == "content",
        "application_name" => true,
        "client_min_messages" => matches!(
            value.as_str(),
            "debug5"
                | "debug4"
                | "debug3"
                | "debug2"
                | "debug1"
                | "debug"
                | "log"
                | "notice"
                | "warning"
                | "error"
        ),
        _ => false,
    }
}

fn handle_pg_show(pg_conn: &Arc<PgConnectionInner>, show_stmt: &PgShowStmt) -> Result<Statement> {
    let name = &show_stmt.name;
    // PostgreSQL names the column after the parameter, not after the name
    // SHOW was given.
    let column = match find_parameter(name) {
        Some(Parameter::BuiltIn(ConfigParameter::SearchPath)) => SEARCH_PATH_SETTING.to_owned(),
        Some(Parameter::Custom(_)) => session_state(&pg_conn.conn)?
            .name_as_defined(name)
            .unwrap_or_else(|| name.clone()),
        None => {
            return Err(LimboError::ParseError(format!(
                "SHOW {name} is not supported"
            )))
        }
    };
    pg_conn.conn.prepare(format!(
        "SELECT current_setting('{}') AS {}",
        name.replace('\'', "''"),
        turso_pg_parser::quote_identifier(&column)
    ))
}

/// Sets every parameter to its default value, as RESET ALL does.
fn reset_all_settings(conn: &Connection) -> Result<()> {
    session_state(conn)?.mutate(conn, ConnectionSettings::reset_all);
    set_search_path(conn, None, SettingScope::Session)?;
    Ok(())
}

/// Resets the session as DISCARD ALL does. tursopg has no temporary
/// tables, SQL-level prepared statements or cached plans to drop. A server
/// client's protocol-level prepared statements are not dropped.
fn discard_all(conn: &Connection) -> Result<()> {
    if !conn.get_auto_commit() {
        return Err(sqlstate_error(
            SQLSTATE_ACTIVE_SQL_TRANSACTION,
            "DISCARD ALL cannot run inside a transaction block",
        ));
    }
    reset_all_settings(conn)?;
    conn.clear_sequence_currvals();
    Ok(())
}

/// `$user` stands for a schema named after the session user, which does
/// not exist, so it is not a name that core looks up.
pub(crate) fn set_search_path(
    conn: &Connection,
    value: Option<&str>,
    scope: SettingScope,
) -> Result<String> {
    let mut path = parse_search_path(value.unwrap_or(DEFAULT_SEARCH_PATH))?;
    path.retain(|name| name != "$user" && !is_core_database_name(name));
    session_state(conn)?.mutate(conn, |settings| {
        settings.set_search_path(Some(path.clone()), scope);
        settings.set(SEARCH_PATH_SETTING, value.map(str::to_owned), scope);
    });
    conn.set_search_path(Some(path));
    Ok(value.unwrap_or(DEFAULT_SEARCH_PATH).to_owned())
}

/// `main` and `temp` name databases in core, not PostgreSQL schemas, so a
/// search_path that lists them finds nothing there, as in PostgreSQL.
fn is_core_database_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("main") || name.eq_ignore_ascii_case("temp")
}

pub(crate) fn search_path_setting(conn: &Connection) -> Option<String> {
    conn.context::<PgSessionState>()?
        .setting(SEARCH_PATH_SETTING)
}

pub(crate) fn set_custom_parameter(
    conn: &Connection,
    name: &str,
    value: Option<&str>,
    scope: SettingScope,
) -> Result<String> {
    if !is_valid_custom_parameter_name(name) {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_NAME,
            format!("invalid configuration parameter name \"{name}\""),
        ));
    }
    session_state(conn)?.mutate(conn, |settings| {
        settings.set(name, value.map(str::to_owned), scope)
    });
    Ok(value.unwrap_or_default().to_owned())
}

pub(crate) fn custom_parameter(conn: &Connection, name: &str) -> Option<String> {
    let state = conn.context::<PgSessionState>()?;
    let name = name.to_ascii_lowercase();
    state
        .has_setting(&name)
        .then(|| state.setting(&name).unwrap_or_default())
}

fn session_state(conn: &Connection) -> Result<&PgSessionState> {
    conn.context::<PgSessionState>().ok_or_else(|| {
        LimboError::InvalidArgument("PostgreSQL session state is not initialized".to_string())
    })
}

fn is_valid_custom_parameter_name(name: &str) -> bool {
    name.contains('.') && name.split('.').all(is_simple_identifier)
}

fn is_simple_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_' || !first.is_ascii())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$' || !c.is_ascii())
}

fn parse_search_path(value: &str) -> Result<Vec<String>> {
    search_path_parser()
        .parse(value)
        .into_result()
        .map_err(|_| {
            LimboError::ParseError("invalid value for parameter \"search_path\"".to_string())
        })
}

fn search_path_parser<'src>() -> impl Parser<'src, &'src str, Vec<String>, extra::Err<EmptyErr>> {
    let whitespace = any().filter(char::is_ascii_whitespace).repeated().ignored();
    // Parse double-quoted names such as "My, Schema" or "a""b".
    // Preserve case, commas, and spaces. Turn doubled quotes into one quote.
    let quoted = just("\"\"")
        .to('"')
        .or(none_of('"'))
        .repeated()
        .collect::<String>()
        .delimited_by(just('"'), just('"'));
    // Parse names such as PUBLIC or $user, up to a comma or ASCII whitespace.
    // Require at least one character, reject a leading quote, and lowercase ASCII letters.
    let unquoted = any()
        .filter(|c: &char| *c != ',' && !c.is_ascii_whitespace())
        .repeated()
        .at_least(1)
        .to_slice()
        .filter(|name: &&str| !name.starts_with('"'))
        .map(str::to_ascii_lowercase);

    quoted
        .or(unquoted)
        .separated_by(just(',').padded_by(whitespace))
        .collect::<Vec<_>>()
        .padded_by(whitespace)
}

fn create_schema_commands(
    conn: &Arc<Connection>,
    stmt: &PgCreateSchemaStmt,
) -> Result<Vec<DeferredCommand>> {
    let name = stmt.name.to_lowercase();
    if conn.qualifier(&name).is_some() {
        if stmt.if_not_exists {
            return Ok(Vec::new());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        )));
    }
    if name.starts_with("pg_") {
        return Err(sqlstate_error(
            SQLSTATE_RESERVED_NAME,
            format!(
                "unacceptable schema name \"{name}\": the prefix \"pg_\" is reserved for system schemas"
            ),
        ));
    }

    if !stmt.elements.is_empty() && !conn.get_auto_commit() {
        return Err(LimboError::ParseError(
            "CREATE SCHEMA with schema elements cannot run inside a transaction block".to_string(),
        ));
    }

    let mut commands = vec![DeferredCommand::Namespace(ast::Stmt::CreateSchema {
        name: ast::Name::exact(name),
    })];
    commands.extend(stmt.elements.iter().cloned().map(DeferredCommand::Postgres));
    Ok(commands)
}

fn drop_schema_commands(
    conn: &Arc<Connection>,
    stmt: &PgDropSchemaStmt,
) -> Result<Vec<DeferredCommand>> {
    let name = stmt.name.to_lowercase();
    if !schema_exists(conn, &name) {
        if stmt.if_exists {
            return Ok(Vec::new());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" does not exist"
        )));
    }

    let objects = schema_objects(conn, &name);
    if !stmt.cascade && !objects.is_empty() {
        return Err(sqlstate_error(
            SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
            format!("cannot drop schema \"{name}\" because other objects depend on it"),
        ));
    }

    let mut commands: Vec<_> = objects
        .iter()
        .map(|object| DeferredCommand::Sqlite(object.drop_sql(&name)))
        .collect();
    commands.push(DeferredCommand::Namespace(ast::Stmt::DropSchema {
        name: ast::Name::exact(name),
    }));
    Ok(commands)
}

/// An object of a schema, which DROP SCHEMA ... CASCADE drops.
enum SchemaObject {
    View(String),
    Table(String),
    Sequence(String),
}

impl SchemaObject {
    /// The statement that drops the object, named with its schema so that
    /// the search path does not change what it names.
    fn drop_sql(&self, schema: &str) -> String {
        let kind = match self {
            SchemaObject::View(_) => "VIEW",
            SchemaObject::Table(_) => "TABLE",
            SchemaObject::Sequence(_) => "SEQUENCE",
        };
        format!(
            "DROP {kind} \"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            self.name().replace('"', "\"\"")
        )
    }

    fn name(&self) -> &str {
        match self {
            SchemaObject::View(name) | SchemaObject::Table(name) | SchemaObject::Sequence(name) => {
                name
            }
        }
    }
}

/// The objects of schema `schema`, in the order that DROP SCHEMA drops
/// them: views first, because they depend on tables. Indexes and triggers
/// go with their tables.
fn schema_objects(conn: &Arc<Connection>, schema: &str) -> Vec<SchemaObject> {
    let Some(namespace) = conn.qualifier(schema) else {
        return Vec::new();
    };
    let namespace = namespace.target.id;
    let current = conn.current_schema();
    let is_user_object =
        |name: &str| !name.starts_with("sqlite_") && !name.starts_with("__turso_internal_");
    let mut views: Vec<String> = current
        .views(namespace)
        .map(|(name, _)| name.to_string())
        .collect();
    let mut tables: Vec<String> = Vec::new();
    for table in current.tables(namespace) {
        let name = table.get_name();
        if !is_user_object(name) || !matches!(table.as_ref(), Table::BTree(_)) {
            continue;
        }
        if current.is_materialized_view(namespace, name) {
            views.push(name.to_string());
        } else {
            tables.push(name.to_string());
        }
    }
    let mut sequences: Vec<String> = current
        .sequences_by_name(namespace)
        .keys()
        .filter(|name| is_user_object(name))
        .cloned()
        .collect();
    views.sort();
    tables.sort();
    sequences.sort();
    views
        .into_iter()
        .map(SchemaObject::View)
        .chain(tables.into_iter().map(SchemaObject::Table))
        .chain(sequences.into_iter().map(SchemaObject::Sequence))
        .collect()
}

/// Whether `schema_name` is a schema: a namespace stored in the main
/// database. `main` and `temp` name databases, not schemas.
fn schema_exists(conn: &Connection, schema_name: &str) -> bool {
    conn.qualifier(schema_name)
        .is_some_and(|namespace| !namespace.is_database())
}

fn prepare_copy_rows(
    stmt: &PgCopyFromStmt,
    column_count: usize,
) -> Result<(String, Vec<Vec<Option<String>>>)> {
    let data = std::fs::read_to_string(&stmt.filename).map_err(|e| {
        LimboError::ParseError(format!("COPY FROM: cannot read '{}': {}", stmt.filename, e))
    })?;

    let table_name = match &stmt.schema_name {
        Some(schema) => format!("\"{schema}\".\"{}\"", stmt.table_name),
        None => format!("\"{}\"", stmt.table_name),
    };
    if column_count == 0 {
        return Err(LimboError::ParseError(format!(
            "COPY FROM: table '{}' not found or has no columns",
            stmt.table_name
        )));
    }

    let (insert_cols, num_columns) = match &stmt.columns {
        Some(cols) => {
            let col_list = cols
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            (format!(" ({col_list})"), cols.len())
        }
        None => (String::new(), column_count),
    };

    let placeholders = (0..num_columns).map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = format!("INSERT INTO {table_name}{insert_cols} VALUES ({placeholders})");

    let delimiter = stmt
        .delimiter
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or('\t');
    let null_string = stmt.null_string.as_deref().unwrap_or("\\N");

    let rows = parse_copy_text_format(&data, delimiter, null_string, num_columns, stmt.header)?;
    Ok((insert_sql, rows))
}

fn bind_copy_row(stmt: &mut crate::Statement, row: &[Option<String>]) -> Result<()> {
    for (index, value) in row.iter().enumerate() {
        let value = value
            .as_ref()
            .map_or(Value::Null, |s| Value::build_text(s.clone()));
        stmt.bind_at(NonZero::new(index + 1).unwrap(), value)
            .map_err(crate::Error::into_core_error)?;
    }
    Ok(())
}

fn prepare_copy_columns(
    conn: &Arc<Connection>,
    table_name: &str,
    schema_name: Option<&str>,
) -> Result<Statement> {
    // The pragma runs as an engine statement, without the search path, so
    // a table named without a schema is looked up here, as a query would.
    let schema = match schema_name {
        Some(schema) => Some(schema.to_string()),
        None => relation_lookup_namespaces(conn, None)
            .into_iter()
            .find(|namespace| {
                namespace.database_id() == turso_core::MAIN_DB_ID
                    && conn
                        .current_schema()
                        .has_table(namespace.target.id, &table_name.to_lowercase())
            })
            .map(|namespace| namespace.name),
    };
    let sql = match schema {
        Some(schema) => format!("PRAGMA \"{schema}\".table_info('{table_name}')"),
        None => format!("PRAGMA table_info('{table_name}')"),
    };
    conn.prepare_internal(&sql)
}

#[cfg(test)]
mod tests {
    use super::{open_database_with_io, search_path_parser, PgConnection};
    use crate::PostgresDialect;
    use chumsky::Parser;
    use std::sync::Arc;
    use turso_core::{Database, DatabaseOpts, MemoryIO, OpenFlags, OpenOptions, Value};

    #[test]
    fn a_materialized_view_of_a_schema_is_maintained_and_dropped_with_the_schema() {
        let db = Database::open(
            Arc::new(MemoryIO::new()),
            ":memory:",
            OpenOptions::new(Arc::new(PostgresDialect))
                .db_opts(DatabaseOpts::new().with_views(true)),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute(
            "CREATE SCHEMA app; \
             CREATE TABLE t (x int); \
             CREATE TABLE app.t (x int); \
             CREATE MATERIALIZED VIEW mv AS SELECT count(*) AS n FROM t; \
             CREATE MATERIALIZED VIEW app.mv AS SELECT count(*) AS n FROM t; \
             INSERT INTO t VALUES (1); \
             INSERT INTO app.t VALUES (1), (2);",
        )
        .unwrap();
        let count = |sql: &str| -> i64 {
            let mut rows = conn.query(sql).unwrap().unwrap();
            let turso_core::StepResult::Row = rows.step().unwrap() else {
                panic!("expected a row");
            };
            let turso_core::Value::Numeric(turso_core::Numeric::Integer(n)) =
                rows.row().unwrap().get_value(0)
            else {
                panic!("expected an integer");
            };
            *n
        };
        assert_eq!(count("SELECT n FROM mv"), 1);
        assert_eq!(count("SELECT n FROM app.mv"), 2);
        conn.execute("DROP SCHEMA app CASCADE").unwrap();
        assert!(conn.query("SELECT n FROM app.mv").is_err());
        assert_eq!(count("SELECT n FROM mv"), 1);
    }

    #[test]
    fn a_view_of_a_schema_reads_catalog_tables_with_aliases_and_using() {
        let db = Database::open(
            Arc::new(MemoryIO::new()),
            ":memory:",
            OpenOptions::new(Arc::new(PostgresDialect))
                .db_opts(DatabaseOpts::new().with_views(true)),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute(
            "CREATE VIEW aliased AS SELECT a.nspname FROM pg_namespace a JOIN pg_namespace b ON a.oid = b.oid; \
             CREATE VIEW joined AS WITH c AS (SELECT * FROM pg_namespace) SELECT c.nspname FROM c JOIN pg_namespace USING (oid); \
             CREATE SCHEMA app; \
             CREATE VIEW app.joined AS WITH c AS (SELECT * FROM pg_namespace) SELECT c.nspname FROM c JOIN pg_namespace USING (oid);",
        )
        .unwrap();
        for view in ["aliased", "joined", "app.joined"] {
            let mut rows = conn
                .query(format!("SELECT count(*) FROM {view}"))
                .unwrap()
                .unwrap();
            let turso_core::StepResult::Row = rows.step().unwrap() else {
                panic!("expected a row from {view}");
            };
            assert_eq!(
                *rows.row().unwrap().get_value(0),
                turso_core::Value::from_i64(4),
                "{view}"
            );
        }
    }

    #[test]
    fn drop_view_refuses_a_relation_of_another_kind() {
        let db = Database::open(
            Arc::new(MemoryIO::new()),
            ":memory:",
            OpenOptions::new(Arc::new(PostgresDialect))
                .db_opts(DatabaseOpts::new().with_views(true)),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute(
            "CREATE TABLE t (a int); \
             CREATE VIEW v AS SELECT * FROM t; \
             CREATE MATERIALIZED VIEW m AS SELECT * FROM t;",
        )
        .unwrap();
        for (sql, error) in [
            (
                "DROP MATERIALIZED VIEW v",
                "\"v\" is not a materialized view",
            ),
            (
                "DROP MATERIALIZED VIEW IF EXISTS public.v",
                "\"v\" is not a materialized view",
            ),
            (
                "DROP MATERIALIZED VIEW t",
                "\"t\" is not a materialized view",
            ),
            ("DROP VIEW m", "\"m\" is not a view"),
            ("DROP VIEW IF EXISTS public.m", "\"m\" is not a view"),
            ("DROP VIEW t", "\"t\" is not a view"),
        ] {
            let err = conn.execute(sql).unwrap_err();
            assert_eq!(err.to_string(), error, "{sql}");
            assert_eq!(err.sqlstate, "42809", "{sql}");
        }
        conn.execute("SELECT * FROM v; SELECT * FROM m; SELECT * FROM t;")
            .unwrap();
        conn.execute("DROP VIEW v; DROP MATERIALIZED VIEW m;")
            .unwrap();
        for sql in ["SELECT * FROM v", "SELECT * FROM m"] {
            assert!(conn.execute(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn create_schema_on_an_in_memory_database_stores_the_schema_in_it() {
        for path in [":memory:", ":memory:named", "file::memory:"] {
            let db = Database::open(
                Arc::new(MemoryIO::new()),
                path,
                OpenOptions::new(Arc::new(PostgresDialect))
                    .db_opts(DatabaseOpts::new().with_attach(true)),
            )
            .unwrap();
            let conn = PgConnection::connect(&db).unwrap();
            conn.execute("CREATE SCHEMA s").unwrap();
            assert!(
                conn.inner()
                    .qualifier("s")
                    .is_some_and(|namespace| !namespace.is_database()),
                "{path}"
            );
            let mut stmt = conn
                .inner()
                .prepare_internal("SELECT name FROM pragma_database_list")
                .unwrap();
            assert_eq!(
                stmt.run_collect_rows().unwrap(),
                vec![vec![Value::build_text("main")]],
                "{path}"
            );
        }
    }

    #[test]
    fn a_dropped_public_schema_stays_dropped_for_later_connections() {
        let db = open_database_with_io(
            Arc::new(MemoryIO::new()),
            ":memory:",
            OpenFlags::default(),
            DatabaseOpts::new(),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute("CREATE TABLE t (x int)").unwrap();
        conn.execute("DROP SCHEMA public CASCADE").unwrap();

        let later = PgConnection::connect(&db).unwrap();
        assert!(later.inner().qualifier("public").is_none());
        let error = later
            .execute("CREATE TABLE t (x int)")
            .unwrap_err()
            .to_string();
        assert!(error.contains("no schema has been selected"), "{error}");
        later.execute("CREATE SCHEMA public").unwrap();
        later.execute("CREATE TABLE t (x int)").unwrap();
    }

    #[test]
    fn a_database_with_tables_in_no_schema_is_refused() {
        let db = Database::open(
            Arc::new(MemoryIO::new()),
            ":memory:",
            OpenOptions::new(Arc::new(PostgresDialect)),
        )
        .unwrap();
        db.connect(turso_core::ConnectOpts::new())
            .unwrap()
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
            .unwrap();
        let error = match PgConnection::connect(&db) {
            Ok(_) => panic!("expected the database to be refused"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("in no schema"), "{error}");
    }

    #[test]
    fn search_path_parser_accepts_identifier_lists() {
        for (input, expected) in [
            ("", vec![]),
            (" \t\n\r\u{c}", vec![]),
            (" PUBLIC , \"MiXeD\" ", vec!["public", "MiXeD"]),
            ("\"a,b\", \"a\"\"b\", $USER", vec!["a,b", "a\"b", "$user"]),
            ("\"\"\"\"", vec!["\""]),
            ("\"\", public", vec!["", "public"]),
            ("public\"extra", vec!["public\"extra"]),
            ("\u{b}PUBLIC\u{a0}", vec!["\u{b}public\u{a0}"]),
        ] {
            assert_eq!(
                search_path_parser().parse(input).into_result().unwrap(),
                expected,
                "{input:?}"
            );
        }
    }

    #[test]
    fn search_path_parser_rejects_invalid_identifier_lists() {
        for input in [
            ",public",
            "public,",
            "public, ",
            "public,,main",
            "one two",
            "\"\"\"",
            "\"unclosed",
            "\"a\"b",
            "\"a\" \"b\"",
        ] {
            assert!(search_path_parser().parse(input).has_errors(), "{input:?}");
        }
    }

    fn test_connection() -> super::PgConnection {
        let (io, db) = super::open_database(
            ":memory:",
            None,
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
        )
        .unwrap();
        drop(io);
        super::PgConnection::connect(&db).unwrap()
    }

    #[test]
    fn set_transaction_read_only_applies_inside_transaction() {
        let conn = test_connection();
        conn.execute("BEGIN").unwrap();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "SET TRANSACTION READ ONLY inside a transaction must set tx read-only mode"
        );
        conn.execute("SET TRANSACTION READ WRITE").unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "SET TRANSACTION READ WRITE inside a transaction must clear tx read-only mode"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn set_transaction_read_only_outside_transaction_is_ignored() {
        let conn = test_connection();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "SET TRANSACTION outside a transaction block must change nothing"
        );
    }

    #[test]
    fn set_transaction_read_write_after_first_query_is_rejected() {
        let conn = test_connection();
        conn.execute("BEGIN READ ONLY").unwrap();
        conn.execute("SELECT 1").unwrap();
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(err.sqlstate, "25001");
        assert_eq!(
            err.message,
            "transaction read-write mode must be set before any query"
        );
        assert!(
            conn.inner().is_tx_read_only(),
            "the rejected access-mode change must leave the transaction read-only"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn set_transaction_read_only_after_first_query_is_allowed() {
        let conn = test_connection();
        conn.execute("BEGIN").unwrap();
        conn.execute("SELECT 1").unwrap();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "downgrading to READ ONLY after the first query must be allowed"
        );
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(
            err.sqlstate, "25001",
            "upgrading back to READ WRITE after the first query must be rejected"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn set_transaction_same_mode_after_first_query_is_allowed() {
        let conn = test_connection();
        conn.execute("BEGIN READ ONLY").unwrap();
        conn.execute("SELECT 1").unwrap();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        conn.execute("ROLLBACK").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("SELECT 1").unwrap();
        conn.execute("SET TRANSACTION READ WRITE").unwrap();
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn set_transaction_does_not_count_as_a_query() {
        let conn = test_connection();
        conn.execute("BEGIN").unwrap();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        conn.execute("SET TRANSACTION READ WRITE").unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "SET TRANSACTION itself must not block later access-mode changes"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn set_transaction_prepared_but_never_run_changes_nothing() {
        let conn = test_connection();
        conn.execute("BEGIN").unwrap();
        let mut stmt = conn.prepare("SET TRANSACTION READ ONLY").unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "preparing SET TRANSACTION must not apply the access mode"
        );
        stmt.run_ignore_rows().unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "running the prepared SET TRANSACTION must apply the access mode"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn set_transaction_prepared_inside_transaction_runs_against_execute_time_state() {
        let conn = test_connection();
        conn.execute("BEGIN").unwrap();
        let mut stmt = conn.prepare("SET TRANSACTION READ ONLY").unwrap();
        conn.execute("ROLLBACK").unwrap();
        stmt.run_ignore_rows().unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "SET TRANSACTION run outside a transaction block must change nothing, \
             even when it was prepared inside one"
        );
    }

    #[test]
    fn set_transaction_applies_last_access_mode_option() {
        let conn = test_connection();
        conn.execute("BEGIN").unwrap();
        conn.execute("SET TRANSACTION READ WRITE, READ ONLY")
            .unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "the last access-mode option must win"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn nested_begin_keeps_transaction_and_applies_access_mode() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        assert!(
            !conn.inner().get_auto_commit(),
            "nested BEGIN must keep the open transaction"
        );
        assert!(
            conn.inner().is_tx_read_only(),
            "nested BEGIN READ ONLY must apply its access mode"
        );
        let err = conn.execute("BEGIN READ WRITE").unwrap_err();
        assert_eq!(
            err.sqlstate, "25001",
            "nested BEGIN READ WRITE after the first query of a read-only \
             transaction must be rejected like SET TRANSACTION READ WRITE"
        );
        assert!(
            conn.inner().is_tx_read_only(),
            "the rejected access-mode change must leave the transaction read-only"
        );
        conn.execute("ROLLBACK").unwrap();
        let mut stmt = conn.query("SELECT count(*) FROM t").unwrap().unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows, vec![vec![turso_core::Value::from_i64(0)]]);
    }

    #[test]
    fn commit_and_rollback_outside_transaction_succeed() {
        let conn = test_connection();
        conn.execute("COMMIT").unwrap();
        conn.execute("ROLLBACK").unwrap();
        conn.execute("END").unwrap();
        conn.execute("ABORT").unwrap();
    }

    #[test]
    fn error_inside_read_only_transaction_does_not_leak_read_only_mode() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        assert!(conn.inner().is_tx_read_only());
        conn.execute("SELECT 1/0").unwrap_err();
        conn.execute("ROLLBACK").unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "read-only mode must end with the transaction, even after an error aborted it"
        );
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
    }

    #[test]
    fn savepoint_survives_runtime_error_inside_transaction() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("SAVEPOINT s").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("SELECT 1/0").unwrap_err();
        assert!(
            !conn.inner().get_auto_commit(),
            "a failed read statement must not roll back the transaction"
        );
        conn.execute("ROLLBACK TO s").unwrap();
        conn.execute("COMMIT").unwrap();
        let mut stmt = conn.query("SELECT a FROM t").unwrap().unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows, vec![vec![turso_core::Value::from_i64(1)]]);
    }

    #[test]
    fn savepoint_end_restores_the_access_mode_it_was_created_with() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        for end in ["ROLLBACK TO a", "RELEASE a"] {
            conn.execute("BEGIN").unwrap();
            conn.execute("SAVEPOINT a").unwrap();
            conn.execute("SET TRANSACTION READ ONLY").unwrap();
            conn.execute(end).unwrap();
            assert!(
                !conn.inner().is_tx_read_only(),
                "{end} must restore the access mode the savepoint was created with"
            );
            conn.execute("INSERT INTO t VALUES (3)").unwrap();
            conn.execute("COMMIT").unwrap();
        }
        let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows, vec![vec![turso_core::Value::from_i64(2)]]);
    }

    #[test]
    fn rollback_to_savepoint_in_a_read_only_transaction_stays_read_only() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        conn.execute("SAVEPOINT sp").unwrap();
        conn.execute("ROLLBACK TO sp").unwrap();
        assert!(conn.inner().is_tx_read_only());
        let err = conn.execute("INSERT INTO t VALUES (1)").unwrap_err();
        assert_eq!(err.sqlstate, "25006");
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn savepoint_release_and_lock_do_not_count_as_the_first_query() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        for stmts in [
            &["SAVEPOINT s", "RELEASE s"][..],
            &["LOCK TABLE t IN ACCESS SHARE MODE"][..],
        ] {
            conn.execute("BEGIN READ ONLY").unwrap();
            for stmt in stmts {
                conn.execute(*stmt).unwrap();
            }
            conn.execute("SET TRANSACTION READ WRITE")
                .unwrap_or_else(|e| panic!("switch after {stmts:?} must be allowed: {e}"));
            conn.execute("INSERT INTO t VALUES (1)").unwrap();
            conn.execute("COMMIT").unwrap();
        }
    }

    #[test]
    fn sequence_writes_are_rejected_in_a_read_only_transaction() {
        let conn = test_connection();
        conn.execute("CREATE SEQUENCE seq1").unwrap();
        for stmt in ["SELECT nextval('seq1')", "SELECT setval('seq1', 100)"] {
            conn.execute("BEGIN READ ONLY").unwrap();
            let err = conn.execute(stmt).unwrap_err();
            assert_eq!(err.sqlstate, "25006", "{stmt} must be rejected");
            conn.execute("ROLLBACK").unwrap();
        }
        let mut stmt = conn.prepare("SELECT nextval('seq1')").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(
            rows,
            vec![vec![turso_core::Value::from_i64(1)]],
            "the rejected calls must not have advanced the sequence"
        );
    }

    #[test]
    fn read_write_switch_inside_savepoint_of_read_only_transaction_is_rejected() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        for switch in ["SET TRANSACTION READ WRITE", "BEGIN READ WRITE"] {
            conn.execute("BEGIN READ ONLY").unwrap();
            conn.execute("SAVEPOINT a").unwrap();
            let err = conn.execute(switch).unwrap_err();
            assert_eq!(err.sqlstate, "25001", "{switch} must be rejected");
            assert_eq!(
                err.message,
                "cannot set transaction read-write mode inside a read-only transaction"
            );
            conn.execute("ROLLBACK").unwrap();
        }
        let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![turso_core::Value::from_i64(0)]]
        );
    }

    #[test]
    fn noop_write_statements_are_rejected_in_a_read_only_transaction() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        for stmt in [
            "COMMENT ON TABLE t IS 'x'",
            "REFRESH MATERIALIZED VIEW some_view",
        ] {
            conn.execute("BEGIN READ ONLY").unwrap();
            let err = conn.execute(stmt).unwrap_err();
            assert_eq!(err.sqlstate, "25006", "{stmt} must be rejected");
            conn.execute("ROLLBACK").unwrap();
        }
    }

    #[test]
    fn noop_write_statements_count_as_the_first_query() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("COMMENT ON TABLE t IS 'x'").unwrap();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(err.sqlstate, "25001");
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn copy_to_stdout_counts_as_the_first_query() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        let spec = turso_pg_parser::copy::parse_copy_to_stdout("COPY t TO STDOUT")
            .unwrap()
            .unwrap();
        let mut stmt = conn.prepare_copy_to_stdout(&spec).unwrap();
        stmt.run_ignore_rows().unwrap();
        drop(stmt);
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(err.sqlstate, "25001");
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn statement_that_fails_at_prepare_counts_as_the_first_query() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        conn.execute("SAVEPOINT s").unwrap();
        conn.execute("SELECT * FROM nosuch_tbl").unwrap_err();
        conn.execute("ROLLBACK TO s").unwrap();
        conn.execute("RELEASE s").unwrap();
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(
            err.sqlstate, "25001",
            "the failed statement took a snapshot in PostgreSQL, so the switch must be rejected"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn failed_utility_statement_does_not_count_as_the_first_query() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        conn.execute("SAVEPOINT s").unwrap();
        conn.execute("SET nosuch_param = 1").unwrap_err();
        conn.execute("ROLLBACK TO s").unwrap();
        conn.execute("RELEASE s").unwrap();
        conn.execute("SET TRANSACTION READ WRITE")
            .expect("a failed SET takes no snapshot in PostgreSQL, so the switch must be allowed");
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("COMMIT").unwrap();
        let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![turso_core::Value::from_i64(1)]]
        );
    }

    #[test]
    fn explain_of_a_write_is_allowed_in_a_read_only_transaction() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("CREATE SEQUENCE s").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        for sql in [
            "EXPLAIN INSERT INTO t VALUES (1)",
            "EXPLAIN SELECT nextval('s')",
        ] {
            let mut stmt = conn.prepare(sql).unwrap();
            stmt.run_collect_rows()
                .unwrap_or_else(|e| panic!("{sql} must not error in a read-only transaction: {e}"));
        }
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn unevaluated_sequence_writes_are_allowed_in_a_read_only_transaction() {
        let conn = test_connection();
        conn.execute("CREATE SEQUENCE s").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        let mut stmt = conn.prepare("SELECT nextval('s') WHERE false").unwrap();
        assert_eq!(stmt.run_collect_rows().unwrap(), Vec::<Vec<_>>::new());
        let mut stmt = conn
            .prepare("SELECT CASE WHEN false THEN nextval('s') END")
            .unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![turso_core::Value::Null]]
        );
        let err = conn.execute("SELECT nextval('s')").unwrap_err();
        assert_eq!(
            err.sqlstate, "25006",
            "an evaluated nextval must still fail"
        );
        conn.execute("ROLLBACK").unwrap();
        let mut stmt = conn.prepare("SELECT nextval('s')").unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![turso_core::Value::from_i64(1)]],
            "nothing in the read-only transaction may have advanced the sequence"
        );
    }

    #[test]
    fn failed_ddl_counts_as_the_first_query() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        conn.execute("SAVEPOINT s").unwrap();
        conn.execute("CREATE TABLE x (a int)").unwrap_err();
        conn.execute("ROLLBACK TO s").unwrap();
        conn.execute("RELEASE s").unwrap();
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(
            err.sqlstate, "25001",
            "PostgreSQL snapshots every non-exempt utility statement, so the failed              CREATE TABLE counts as the first query"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn statement_rejected_in_an_aborted_block_does_not_count_as_the_first_query() {
        let conn = test_connection();
        conn.execute("CREATE TABLE r1 (a int)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        conn.execute("SAVEPOINT s").unwrap();
        conn.execute("SET nosuch_param = 1").unwrap_err();
        let err = conn.execute("SELECT 1").unwrap_err();
        assert_eq!(err.sqlstate, "25P02");
        conn.execute("ROLLBACK TO s").unwrap();
        conn.execute("RELEASE s").unwrap();
        conn.execute("SET TRANSACTION READ WRITE")
            .expect("PostgreSQL never analyzed the rejected SELECT, so the switch must be allowed");
        conn.execute("INSERT INTO r1 VALUES (1)").unwrap();
        conn.execute("COMMIT").unwrap();
        let mut stmt = conn.prepare("SELECT count(*) FROM r1").unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![turso_core::Value::from_i64(1)]]
        );
    }

    #[test]
    fn writes_on_missing_tables_report_the_name_error_in_a_read_only_transaction() {
        let conn = test_connection();
        conn.execute("BEGIN READ ONLY").unwrap();
        let err = conn
            .execute("INSERT INTO nosuch_tbl VALUES (1)")
            .unwrap_err();
        assert_eq!(
            err.sqlstate, "42P01",
            "name resolution comes before the read-only check, as in PostgreSQL"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn show_does_not_count_as_the_first_query() {
        let conn = test_connection();
        conn.execute("BEGIN READ ONLY").unwrap();
        conn.execute("SHOW search_path").unwrap();
        conn.execute("SET TRANSACTION READ WRITE").unwrap();
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn rejected_access_mode_change_aborts_the_block() {
        let conn = test_connection();
        conn.execute("BEGIN READ ONLY").unwrap();
        conn.execute("SELECT 1").unwrap();
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(err.sqlstate, "25001");
        let err = conn.execute("SELECT 1").unwrap_err();
        assert_eq!(
            err.sqlstate, "25P02",
            "an error inside a transaction block must abort the block"
        );
        conn.execute("ROLLBACK").unwrap();
        conn.execute("SELECT 1").unwrap();
    }

    #[test]
    fn partially_consumed_query_blocks_switch_to_read_write() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (v int)").unwrap();
        conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("SET TRANSACTION READ ONLY").unwrap();
        let mut stmt = conn.prepare("SELECT v FROM t").unwrap();
        assert!(stmt
            .run_one_step_blocking(|| Ok(()), || Ok(()))
            .unwrap()
            .is_some());
        stmt.reset().unwrap();
        drop(stmt);
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(err.sqlstate, "25001");
        conn.execute("INSERT INTO t VALUES (3)").unwrap_err();
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn abandoned_query_blocks_switch_to_read_write() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (v int)").unwrap();
        conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        let mut stmt = conn.prepare("SELECT v FROM t").unwrap();
        assert!(stmt
            .run_one_step_blocking(|| Ok(()), || Ok(()))
            .unwrap()
            .is_some());
        drop(stmt);
        let err = conn.execute("SET TRANSACTION READ WRITE").unwrap_err();
        assert_eq!(err.sqlstate, "25001");
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn create_and_drop_schema_are_rejected_in_a_read_only_transaction() {
        let conn = test_connection();
        conn.execute("CREATE SCHEMA pre_existing").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        let err = conn.execute("CREATE SCHEMA readonly_schema").unwrap_err();
        assert_eq!(err.sqlstate, "25006");
        conn.execute("ROLLBACK").unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT count(*) FROM information_schema.schemata \
                 WHERE schema_name = 'readonly_schema'",
            )
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(
            rows,
            vec![vec![turso_core::Value::from_i64(0)]],
            "CREATE SCHEMA in a READ ONLY transaction must create nothing"
        );
        conn.execute("BEGIN READ ONLY").unwrap();
        let err = conn.execute("DROP SCHEMA pre_existing").unwrap_err();
        assert_eq!(err.sqlstate, "25006");
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn statement_prepared_before_begin_read_only_cannot_write() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        let mut insert = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        insert.run_ignore_rows().unwrap_err();
        conn.execute("ROLLBACK").unwrap();
        insert.run_ignore_rows().unwrap();
    }

    #[test]
    fn prepare_applies_no_transaction_state_before_execute() {
        let conn = test_connection();
        let _begin = conn.prepare("BEGIN READ ONLY").unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "prepare(BEGIN READ ONLY) must not set read-only mode before execute"
        );
        conn.execute("BEGIN READ ONLY").unwrap();
        let _commit = conn.prepare("COMMIT").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "prepare(COMMIT) must not clear read-only mode before execute"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn prepared_commit_actually_commits_when_run_in_transaction() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (v INT)").unwrap();
        // Prepare COMMIT while idle — must not bake in a noop.
        let mut commit = conn.prepare("COMMIT").unwrap();
        // Prepare ROLLBACK while idle — same requirement.
        let mut rollback = conn.prepare("ROLLBACK").unwrap();

        // Run the prepared COMMIT while still idle — must succeed without error.
        commit.run_ignore_rows().unwrap();

        // Now start a transaction, insert a row, and commit with the prepared statement.
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        commit.run_ignore_rows().unwrap();
        let rows = conn
            .prepare("SELECT count(*) FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![turso_core::Value::from_i64(1)]],
            "INSERT must be committed"
        );

        // Now start another transaction, insert, and roll back with the prepared statement.
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        rollback.run_ignore_rows().unwrap();
        let rows = conn
            .prepare("SELECT count(*) FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![turso_core::Value::from_i64(1)]],
            "INSERT must be rolled back"
        );

        // Run ROLLBACK while idle — must succeed without error.
        rollback.run_ignore_rows().unwrap();
    }

    #[test]
    fn prepared_begin_read_only_run_inside_transaction_keeps_transaction_and_applies_mode() {
        let conn = test_connection();
        // Prepare BEGIN READ ONLY while idle.
        let mut begin_ro = conn.prepare("BEGIN READ ONLY").unwrap();
        conn.execute("BEGIN").unwrap();
        // Run the prepared BEGIN READ ONLY while already in a transaction.
        // Must succeed (warning-level, like PostgreSQL), keep the transaction
        // open, and apply the READ ONLY access mode.
        begin_ro.run_ignore_rows().unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "nested prepared BEGIN READ ONLY must apply read-only mode"
        );
        assert!(
            !conn.inner().get_auto_commit(),
            "nested prepared BEGIN READ ONLY must keep the transaction open"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn prepared_begin_inside_transaction_starts_new_transaction_after_commit() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (v INT)").unwrap();

        conn.execute("BEGIN").unwrap();
        // Prepare BEGIN while already inside a transaction. The returned statement
        // must compile the real BEGIN, not a noop — otherwise re-using it after
        // this transaction ends would silently skip starting a new transaction.
        let mut begin = conn.prepare("BEGIN").unwrap();
        conn.execute("COMMIT").unwrap();

        // Execute the prepared BEGIN: must start a real transaction.
        begin.run_ignore_rows().unwrap();
        conn.execute("INSERT INTO t VALUES (42)").unwrap();
        conn.execute("ROLLBACK").unwrap();

        // If BEGIN was a noop, the INSERT auto-committed and count would be 1.
        let rows = conn
            .prepare("SELECT count(*) FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![turso_core::Value::from_i64(0)]],
            "ROLLBACK must have undone the INSERT (prepared BEGIN must start a real transaction)"
        );
    }

    #[test]
    fn prepared_commit_does_not_clear_read_only_mode_before_it_runs() {
        let conn = test_connection();
        conn.execute("CREATE TABLE t (v INT)").unwrap();

        conn.execute("BEGIN READ ONLY").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "BEGIN READ ONLY must set tx read-only mode"
        );
        // Preparing COMMIT must not immediately clear tx read-only: the transaction
        // is still open and write attempts before running COMMIT must be rejected.
        let mut commit = conn.prepare("COMMIT").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "prepare(COMMIT) must not clear tx read-only mode before it runs"
        );
        // Running the prepared COMMIT must clear tx read-only.
        commit.run_ignore_rows().unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "running the prepared COMMIT must clear tx read-only mode"
        );
        // Writes must now be allowed.
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
    }
}
