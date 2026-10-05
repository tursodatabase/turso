use std::str;
use std::sync::{Arc, Mutex};

use crate::aliases;
use crate::catalog::{self, PostgresDialect};
use crate::statement::{with_execution, Plan, Statement};
use turso_core::{Connection, LimboError, PrepareOptions, Result};
use turso_parser::ast::{self};
use turso_pg_parser::translator::{
    is_comment_on, is_refresh_matview, try_extract_copy_from, try_extract_create_schema,
    try_extract_drop_schema, try_extract_set, try_extract_show, PgSetStmt, PostgreSQLTranslator,
};

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

pub(crate) struct PgConnectionInner {
    pub(crate) conn: Arc<Connection>,
    session_state: Mutex<SessionState>,
}

impl PgConnectionInner {
    pub(crate) fn set_search_path(&self, path: Vec<String>) {
        let mut state = self.session_state.lock().unwrap();
        state.search_path = path;
    }
}

#[derive(Default)]
struct SessionState {
    search_path: Vec<String>,
}

/// Open a database with the PostgreSQL schema dialect, resolving the IO
/// backend from `vfs` or the path like [`turso_core::Database::open_new`].
pub fn open_database(
    path: &str,
    vfs: Option<&str>,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> Result<(Arc<dyn turso_core::IO>, Arc<turso_core::Database>)> {
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
) -> Result<Arc<turso_core::Database>> {
    let file = io.open_file(path, flags, true)?;
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    turso_core::Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(PostgresDialect))
            .storage(db_file)
            .flags(flags)
            .db_opts(opts),
    )
}

impl PgConnection {
    pub fn new(conn: Arc<Connection>) -> Self {
        aliases::install(&conn);
        Self {
            inner: Arc::new(PgConnectionInner {
                conn,
                session_state: Mutex::new(SessionState::default()),
            }),
        }
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.inner.conn
    }

    pub fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        prepare_statement(&self.inner, sql.as_ref())
    }

    pub fn query(&self, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        let sql = sql.as_ref().trim();
        if sql.is_empty() {
            return Ok(None);
        }
        self.prepare(sql).map(Some)
    }

    pub fn execute(&self, sql: impl AsRef<str>) -> Result<()> {
        for stmt in self.query_runner(sql.as_ref().as_bytes()) {
            if let Some(mut stmt) = stmt? {
                stmt.run_ignore_rows()?;
            }
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.inner.conn.close()
    }

    pub fn pragma_update(&self, name: &str, value: impl std::fmt::Display) -> Result<()> {
        let sql = format!("PRAGMA {name} = {value}");
        let mut stmt = self.inner.conn.prepare_sqlite(sql)?;
        stmt.run_ignore_rows()
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
    type Item = Result<Option<Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.stmts.len() {
            return None;
        }

        let sql = &self.stmts[self.index];
        self.index += 1;
        Some(prepare_statement(self.conn, sql).map(Some))
    }
}

pub fn split_statements(sql: &str) -> Result<Vec<String>> {
    match turso_pg_parser::split_statements(sql) {
        Ok(stmts) if stmts.is_empty() && !sql.trim().is_empty() => Ok(vec![sql.trim().to_string()]),
        Ok(stmts) => Ok(stmts),
        Err(_) => Ok(vec![sql.trim().to_string()]),
    }
}

fn prepare_statement(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<Statement> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(LimboError::InvalidArgument(
            "The supplied SQL string contains no statements".to_string(),
        ));
    }

    reject_sqlite_catalog_access(sql)?;

    if let Some(stmt) = try_prepare_special(pg_conn, sql)? {
        return Ok(stmt);
    }

    let parse_result =
        turso_pg_parser::parse(sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    reject_catalog_dml(translated.cmd.stmt())?;

    let options = {
        let state = pg_conn.session_state.lock().unwrap();
        let path = state.search_path.clone();
        PrepareOptions {
            unqualified_database_search_path: if path.is_empty() { None } else { Some(path) },
        }
    };
    let plan = (!translated.prereqs.is_empty() && matches!(translated.cmd, ast::Cmd::Stmt(_)))
        .then(|| Plan::Prerequisites {
            statements: translated.prereqs,
            options: PrepareOptions {
                unqualified_database_search_path: options.unqualified_database_search_path.clone(),
            },
            main: Box::new(translated.cmd.clone()),
            input: sql.to_string(),
        });
    let stmt = pg_conn
        .conn
        .prepare_translated_cmd_with_options(translated.cmd, sql, &options)?;
    Ok(with_execution(pg_conn.clone(), stmt, plan))
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
    Err(LimboError::ParseError(format!(
        "cannot {verb} pg_catalog table \"{table_name}\""
    )))
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

fn try_prepare_special(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<Option<Statement>> {
    let parse_result = match turso_pg_parser::parse(sql) {
        Ok(result) => result,
        Err(_) => return Ok(None),
    };

    if let Some(set_stmt) = try_extract_set(&parse_result) {
        let stmt = handle_pg_set(pg_conn, &set_stmt)?;
        return Ok(Some(stmt));
    }

    if let Some(show_stmt) = try_extract_show(&parse_result) {
        let pragma_sql = format!("PRAGMA {}", show_stmt.name);
        return Ok(Some(with_execution(
            pg_conn.clone(),
            pg_conn.conn.prepare(&pragma_sql)?,
            None,
        )));
    }

    if let Some(stmt) = try_extract_create_schema(&parse_result) {
        return Ok(Some(noop_statement(
            pg_conn,
            Some(Plan::CreateSchema(stmt)),
        )?));
    }

    if let Some(stmt) = try_extract_drop_schema(&parse_result) {
        return Ok(Some(noop_statement(pg_conn, Some(Plan::DropSchema(stmt)))?));
    }

    if is_refresh_matview(&parse_result) {
        return Ok(Some(noop_statement(pg_conn, None)?));
    }

    if is_comment_on(&parse_result) {
        return Ok(Some(noop_statement(pg_conn, None)?));
    }

    if let Some(stmt) = try_extract_copy_from(&parse_result) {
        return Ok(Some(noop_statement(pg_conn, Some(Plan::Copy(stmt)))?));
    }

    Ok(None)
}

fn noop_statement(pg_conn: &Arc<PgConnectionInner>, plan: Option<Plan>) -> Result<Statement> {
    Ok(with_execution(
        pg_conn.clone(),
        pg_conn.conn.prepare("SELECT 0 WHERE 0")?,
        plan,
    ))
}

fn handle_pg_set(pg_conn: &Arc<PgConnectionInner>, set_stmt: &PgSetStmt) -> Result<Statement> {
    if set_stmt.name == "search_path" {
        let path = set_stmt
            .values
            .iter()
            .map(|value| value.as_search_path_name().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| LimboError::ParseError("incorrect format".to_string()))?;
        return noop_statement(pg_conn, Some(Plan::SearchPath(path)));
    }
    let value = set_stmt.values.first().ok_or_else(|| {
        LimboError::ParseError(format!("SET {}: no value provided", set_stmt.name))
    })?;
    let pragma_sql = format!("PRAGMA {} = {}", set_stmt.name, value.to_sql_string());
    Ok(with_execution(
        pg_conn.clone(),
        pg_conn.conn.prepare(&pragma_sql)?,
        None,
    ))
}
