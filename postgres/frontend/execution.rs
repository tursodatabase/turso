use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use turso_core::{Completion, IOResult, LimboError, Statement as CoreStatement};
use turso_parser::ast;
use turso_pg_parser::translator::{PgCopyFromStmt, PgCreateSchemaStmt, PgDropSchemaStmt};

use crate::copy::parse_copy_text_format;
use crate::session::{self, PgConnectionInner};
use crate::settings::SettingScope;
use crate::{Result, Statement};

pub(crate) enum Plan {
    Prerequisites {
        statements: Vec<ast::Stmt>,
        main: Box<ast::Cmd>,
        input: String,
    },
    CreateSchema(PgCreateSchemaStmt),
    DropSchema(PgDropSchemaStmt),
    Copy(PgCopyFromStmt),
}

pub(crate) struct Execution {
    conn: Arc<PgConnectionInner>,
    plan: Plan,
    operation: Option<Operation>,
    interrupted: bool,
    timeout: Option<Option<Duration>>,
}

impl Execution {
    pub(crate) fn new(conn: Arc<PgConnectionInner>, plan: Plan) -> Self {
        Self {
            conn,
            plan,
            operation: None,
            interrupted: false,
            timeout: None,
        }
    }

    pub(crate) fn step(&mut self, inner: &mut CoreStatement) -> Result<IOResult<()>> {
        if self.operation.is_none() {
            if self.interrupted {
                return Err(LimboError::Interrupt.into());
            }
            if matches!(self.plan, Plan::CreateSchema(_) | Plan::DropSchema(_))
                && self.conn.conn.is_tx_read_only()
            {
                return Err(LimboError::ReadOnly.into());
            }
            self.operation = Some(match &self.plan {
                Plan::Prerequisites {
                    statements,
                    main,
                    input,
                } => {
                    let mut steps = VecDeque::new();
                    if !session::create_table_if_not_exists_target_present(&self.conn.conn, main)? {
                        steps.extend(statements.iter().cloned().map(Sql::Translated));
                    }
                    steps.push_back(Sql::Main((**main).clone(), input.clone()));
                    Operation::Batch(Batch::new(self.conn.clone(), steps, true))
                }
                Plan::CreateSchema(spec) => {
                    Operation::Batch(session::create_schema_steps(&self.conn, spec)?)
                }
                Plan::DropSchema(spec) => {
                    Operation::Batch(session::drop_schema_steps(&self.conn, spec)?)
                }
                Plan::Copy(spec) => Operation::Copy(Copy {
                    spec: spec.clone(),
                    state: CopyState::Columns {
                        query: Box::new(session::prepare_table_columns(
                            &self.conn.conn,
                            &spec.table_name,
                            spec.schema_name.as_deref(),
                        )?),
                        columns: 0,
                    },
                }),
            });
        }
        match self.operation.as_mut().unwrap() {
            Operation::Batch(batch) => batch.step(inner, self.timeout, self.interrupted),
            Operation::Copy(copy) => copy.step(&self.conn, inner, self.timeout, self.interrupted),
        }
    }

    pub(crate) fn interrupt(&mut self) {
        self.interrupted = true;
        if let Some(Operation::Batch(batch)) = &mut self.operation {
            if let Some(current) = &mut batch.current {
                current.interrupt();
            }
        }
    }

    pub(crate) fn set_timeout(&mut self, timeout: Option<Option<Duration>>) {
        self.timeout = timeout;
    }

    pub(crate) fn reset(&mut self) {
        self.operation = None;
        self.interrupted = false;
        self.timeout = None;
    }
}

enum Operation {
    Batch(Batch),
    Copy(Copy),
}

pub(crate) enum Sql {
    Sqlite(String),
    Translated(ast::Stmt),
    Namespace(ast::Stmt),
    Main(ast::Cmd, String),
    Frontend(String),
    DropObject(String),
    SearchPath(String),
    Begin,
    Release,
}

pub(crate) struct Batch {
    conn: Arc<PgConnectionInner>,
    steps: VecDeque<Sql>,
    current: Option<ActiveStatement>,
    savepoint: bool,
    started_in_autocommit: bool,
    restore_search_path: Option<Option<String>>,
    error: Option<crate::Error>,
}

enum ActiveStatement {
    Core(Box<CoreStatement>),
    Frontend(Box<Statement>),
}

impl Batch {
    pub(crate) fn new(
        conn: Arc<PgConnectionInner>,
        mut steps: VecDeque<Sql>,
        atomic: bool,
    ) -> Self {
        if atomic {
            steps.push_front(Sql::Begin);
            steps.push_back(Sql::Release);
        }
        Self {
            started_in_autocommit: conn.conn.get_auto_commit(),
            conn,
            steps,
            current: None,
            savepoint: false,
            restore_search_path: None,
            error: None,
        }
    }

    fn step(
        &mut self,
        inner: &mut CoreStatement,
        timeout: Option<Option<Duration>>,
        interrupted: bool,
    ) -> Result<IOResult<()>> {
        loop {
            let result = self.step_current(inner, timeout, interrupted && self.error.is_none());
            match result {
                Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                Ok(IOResult::Done(())) => {
                    if !self.steps.is_empty() {
                        continue;
                    }
                    self.restore_search_path()?;
                    return match self.error.take() {
                        Some(error) => Err(error),
                        None => Ok(IOResult::Done(())),
                    };
                }
                Err(error) => {
                    self.current = None;
                    self.steps.clear();
                    if self.error.is_some() || !self.savepoint || self.conn.conn.get_auto_commit() {
                        self.savepoint = false;
                        self.restore_search_path()?;
                        return Err(error);
                    }
                    self.error = Some(error);
                    self.steps = self.rollback_steps();
                }
            }
        }
    }

    fn step_current(
        &mut self,
        inner: &mut CoreStatement,
        timeout: Option<Option<Duration>>,
        interrupted: bool,
    ) -> Result<IOResult<()>> {
        let Some(sql) = self.steps.front() else {
            return Ok(IOResult::Done(()));
        };
        if interrupted && self.current.is_none() {
            return Err(LimboError::Interrupt.into());
        }
        let conn = &self.conn.conn;
        if self.current.is_none() {
            self.current = Some(match sql {
                Sql::Sqlite(sql) | Sql::DropObject(sql) => {
                    ActiveStatement::Core(Box::new(conn.prepare_sqlite(sql)?))
                }
                Sql::Translated(stmt) => ActiveStatement::Core(Box::new(
                    conn.prepare_translated_stmt(stmt.clone(), &stmt.to_string())?,
                )),
                Sql::Namespace(stmt) => {
                    ActiveStatement::Core(Box::new(conn.prepare_namespace_stmt(stmt.clone())?))
                }
                Sql::Main(cmd, input) => ActiveStatement::Core(Box::new(
                    conn.prepare_translated_cmd(cmd.clone(), input)?,
                )),
                Sql::Frontend(sql) => ActiveStatement::Frontend(Box::new(
                    session::prepare_statement_inner(&self.conn, sql)?.without_tx_tracking(),
                )),
                Sql::Begin => ActiveStatement::Core(Box::new(
                    conn.prepare_sqlite("SAVEPOINT \"__turso_pg_stmt\"")?,
                )),
                Sql::Release => ActiveStatement::Core(Box::new(
                    conn.prepare_sqlite("RELEASE SAVEPOINT \"__turso_pg_stmt\"")?,
                )),
                Sql::SearchPath(path) => {
                    self.restore_search_path = Some(session::search_path_setting(conn));
                    session::set_search_path(conn, Some(path), SettingScope::Session)?;
                    self.steps.pop_front();
                    return Ok(IOResult::Done(()));
                }
            });
        }
        let disable_foreign_keys = matches!(sql, Sql::DropObject(_)) && conn.foreign_keys_enabled();
        if disable_foreign_keys {
            conn.set_foreign_keys_enabled(false);
        }
        let result = self.current.as_mut().unwrap().step(timeout, interrupted);
        if disable_foreign_keys {
            conn.set_foreign_keys_enabled(true);
        }
        if let IOResult::IO(io) = result? {
            return Ok(IOResult::IO(io));
        }
        let current = self.current.take().unwrap();
        match self.steps.pop_front().unwrap() {
            Sql::Begin => self.savepoint = true,
            Sql::Release => self.savepoint = false,
            Sql::Main(_, _) => {
                let ActiveStatement::Core(stmt) = current else {
                    unreachable!();
                };
                *inner = *stmt;
            }
            _ => {}
        }
        Ok(IOResult::Done(()))
    }

    fn rollback_steps(&self) -> VecDeque<Sql> {
        if self.started_in_autocommit {
            VecDeque::from([Sql::Sqlite("ROLLBACK".to_owned())])
        } else {
            VecDeque::from([
                Sql::Sqlite("ROLLBACK TO SAVEPOINT \"__turso_pg_stmt\"".to_owned()),
                Sql::Release,
            ])
        }
    }

    fn restore_search_path(&mut self) -> Result<()> {
        if let Some(setting) = self.restore_search_path.take() {
            session::set_search_path(&self.conn.conn, setting.as_deref(), SettingScope::Session)?;
        }
        Ok(())
    }
}

impl ActiveStatement {
    fn step(
        &mut self,
        timeout: Option<Option<Duration>>,
        interrupted: bool,
    ) -> Result<IOResult<()>> {
        match self {
            Self::Core(stmt) => {
                stmt.set_query_timeout_override(timeout);
                if interrupted {
                    stmt.interrupt();
                }
                stmt.run_ignore_rows_nonblock()
                    .map_err(|error| (*error).into())
            }
            Self::Frontend(stmt) => {
                stmt.set_query_timeout_override(timeout);
                if interrupted {
                    stmt.interrupt();
                }
                stmt.run_ignore_rows_nonblock()
            }
        }
    }

    fn interrupt(&mut self) {
        match self {
            Self::Core(stmt) => stmt.interrupt(),
            Self::Frontend(stmt) => stmt.interrupt(),
        }
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        self.current = None;
        if self.savepoint && !self.conn.conn.get_auto_commit() {
            for sql in self.rollback_steps() {
                let sql = match sql {
                    Sql::Sqlite(sql) => sql,
                    Sql::Release => "RELEASE SAVEPOINT \"__turso_pg_stmt\"".to_owned(),
                    _ => unreachable!(),
                };
                if let Err(error) = self
                    .conn
                    .conn
                    .prepare_sqlite(sql)
                    .and_then(|mut stmt| stmt.run_ignore_rows())
                {
                    tracing::error!("Failed to roll back frontend statement: {error}");
                }
            }
        }
        if let Err(error) = self.restore_search_path() {
            tracing::error!("Failed to restore search_path: {error}");
        }
    }
}

struct Copy {
    spec: PgCopyFromStmt,
    state: CopyState,
}

enum CopyState {
    Columns {
        query: Box<CoreStatement>,
        columns: usize,
    },
    Read {
        columns: usize,
        data: Arc<OnceLock<std::io::Result<String>>>,
        completion: Completion,
    },
    Insert,
}

impl Copy {
    fn step(
        &mut self,
        conn: &PgConnectionInner,
        inner: &mut CoreStatement,
        timeout: Option<Option<Duration>>,
        interrupted: bool,
    ) -> Result<IOResult<()>> {
        loop {
            match &mut self.state {
                CopyState::Columns { query, columns } => {
                    query.set_query_timeout_override(timeout);
                    if interrupted {
                        query.interrupt();
                    }
                    if let IOResult::IO(io) = query
                        .run_with_row_callback_nonblock(|_| {
                            *columns += 1;
                            Ok(())
                        })
                        .map_err(|error| *error)?
                    {
                        return Ok(IOResult::IO(io));
                    }
                    if *columns == 0 {
                        return Err(LimboError::ParseError(format!(
                            "COPY FROM: table '{}' not found or has no columns",
                            self.spec.table_name
                        ))
                        .into());
                    }
                    let data = Arc::new(OnceLock::new());
                    let completion = Completion::new_wait();
                    let filename = self.spec.filename.clone();
                    let output = data.clone();
                    let done = completion.clone();
                    std::thread::Builder::new()
                        .spawn(move || {
                            output
                                .set(std::fs::read_to_string(filename))
                                .expect("COPY input is published once");
                            done.complete(0);
                        })
                        .map_err(|error| {
                            LimboError::ParseError(format!(
                                "COPY FROM: cannot start file read: {error}"
                            ))
                        })?;
                    self.state = CopyState::Read {
                        columns: self.spec.columns.as_ref().map_or(*columns, Vec::len),
                        data,
                        completion: completion.clone(),
                    };
                    return Ok(IOResult::IO(turso_core::types::IOCompletions(completion)));
                }
                CopyState::Read {
                    columns,
                    data,
                    completion,
                } => {
                    if interrupted {
                        return Err(LimboError::Interrupt.into());
                    }
                    if !completion.finished() {
                        return Ok(IOResult::IO(turso_core::types::IOCompletions(
                            completion.clone(),
                        )));
                    }
                    let data = data
                        .get()
                        .expect("COPY input is available after completion")
                        .as_ref()
                        .map_err(|error| {
                            LimboError::ParseError(format!(
                                "COPY FROM: cannot read '{}': {error}",
                                self.spec.filename
                            ))
                        })?;
                    let rows = parse_copy_text_format(
                        data,
                        self.spec
                            .delimiter
                            .as_ref()
                            .and_then(|d| d.chars().next())
                            .unwrap_or('\t'),
                        self.spec.null_string.as_deref().unwrap_or("\\N"),
                        *columns,
                        self.spec.header,
                    )?;
                    if rows.is_empty() {
                        inner.set_n_change(0);
                        return Ok(IOResult::Done(()));
                    }
                    let quote = turso_pg_parser::quote_identifier;
                    let table = match &self.spec.schema_name {
                        Some(schema) => {
                            format!("{}.{}", quote(schema), quote(&self.spec.table_name))
                        }
                        None => quote(&self.spec.table_name),
                    };
                    let columns = self
                        .spec
                        .columns
                        .as_ref()
                        .map(|columns| {
                            format!(
                                " ({})",
                                columns
                                    .iter()
                                    .map(|c| quote(c))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        })
                        .unwrap_or_default();
                    let values = rows
                        .iter()
                        .map(|row| {
                            format!(
                                "({})",
                                row.iter()
                                    .map(|value| match value {
                                        Some(value) => format!("'{}'", value.replace('\'', "''")),
                                        None => "NULL".to_owned(),
                                    })
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    *inner = conn
                        .conn
                        .prepare_sqlite(format!("INSERT INTO {table}{columns} VALUES {values}"))?;
                    self.state = CopyState::Insert;
                }
                CopyState::Insert => {
                    inner.set_query_timeout_override(timeout);
                    if interrupted {
                        inner.interrupt();
                    }
                    return inner
                        .run_ignore_rows_nonblock()
                        .map_err(|error| (*error).into());
                }
            }
        }
    }
}
