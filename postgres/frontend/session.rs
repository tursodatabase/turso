use chumsky::{error::EmptyErr, prelude::*};
use std::str;
use std::sync::{Arc, Mutex};

use crate::catalog::{self, PostgresDialect};
use crate::statement::{with_execution, Plan, Statement};
use turso_core::{Connection, LimboError, PrepareOptions, Result, Value};
use turso_parser::ast::{self};
use turso_pg_parser::copy::CopyToStdout;
use turso_pg_parser::translator::{
    is_comment_on, is_refresh_matview, is_set_transaction, try_extract_copy_from,
    try_extract_create_schema, try_extract_drop_schema, try_extract_set, try_extract_show,
    PgSetStmt, PostgreSQLTranslator,
};

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

pub(crate) struct PgConnectionInner {
    pub(crate) conn: Arc<Connection>,
}

#[derive(Default)]
struct SessionState {
    search_path: Option<Vec<String>>,
    search_path_setting: Option<String>,
}

type PgSessionState = Mutex<SessionState>;

pub(crate) const DEFAULT_SEARCH_PATH: &str = "\"$user\", public";

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
    pub fn connect(db: &Arc<turso_core::Database>) -> Result<Self> {
        let context = Arc::new(PgSessionState::default());
        let conn = db.connect_with_context(context)?;
        Ok(Self {
            inner: Arc::new(PgConnectionInner { conn }),
        })
    }

    pub fn connect_async(
        db: &Arc<turso_core::Database>,
        opts: turso_core::ConnectOpts,
        state: &mut turso_core::ConnectAsyncState,
    ) -> turso_core::types::IOResultOr<Self> {
        let opts = opts.with_context(Arc::new(PgSessionState::default()));
        db.connect_async(opts, state).map(|result| {
            result.map(|conn| Self {
                inner: Arc::new(PgConnectionInner { conn }),
            })
        })
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.inner.conn
    }

    pub fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        prepare_statement(&self.inner, sql.as_ref())
    }

    pub fn prepare_copy_to_stdout(&self, copy: &CopyToStdout) -> Result<Statement> {
        noop_statement(
            &self.inner,
            Some(Plan::CopyTo {
                spec: copy.clone(),
                options: prepare_options(&self.inner.conn),
            }),
        )
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

#[derive(Debug)]
pub enum StatementKind {
    Sql(Box<ast::Cmd>),
    Session,
    Schema,
    ExternalStorage,
    CopyOut,
}

pub fn inspect(sql: &str) -> Result<Option<StatementKind>> {
    let parsed =
        turso_pg_parser::parse(sql).map_err(|error| LimboError::ParseError(error.to_string()))?;
    match parsed.protobuf.stmts.len() {
        0 => return Ok(None),
        1 => {}
        _ => return Err(LimboError::InvalidArgument("expected one statement".into())),
    }
    reject_sqlite_catalog_access(sql)?;
    if let Some(set) = try_extract_set(&parsed) {
        if set.name == "search_path" {
            return Ok(Some(StatementKind::Session));
        }
        return Err(LimboError::InvalidArgument(format!(
            "setting '{}' requires host configuration access",
            set.name
        )));
    }
    if let Some(show) = try_extract_show(&parsed) {
        let (cmd, _) = turso_core::dialect::sqlite::parse(&format!("PRAGMA {}", show.name))?;
        return Ok(cmd.map(|cmd| StatementKind::Sql(Box::new(cmd))));
    }
    if is_set_transaction(&parsed) {
        return Ok(Some(StatementKind::Session));
    }
    if let Some(schema) = try_extract_create_schema(&parsed) {
        return Ok(Some(if schema.name.eq_ignore_ascii_case("public") {
            StatementKind::Schema
        } else {
            StatementKind::ExternalStorage
        }));
    }
    if let Some(schema) = try_extract_drop_schema(&parsed) {
        return Ok(Some(if schema.name.eq_ignore_ascii_case("public") {
            StatementKind::Schema
        } else {
            StatementKind::ExternalStorage
        }));
    }
    if try_extract_copy_from(&parsed).is_some() {
        return Ok(Some(StatementKind::ExternalStorage));
    }
    if turso_pg_parser::copy::parse_copy_to_stdout(sql)
        .map_err(|error| LimboError::ParseError(error.to_string()))?
        .is_some()
    {
        return Ok(Some(StatementKind::CopyOut));
    }
    if is_comment_on(&parsed) || is_refresh_matview(&parsed) {
        return Ok(Some(StatementKind::Schema));
    }
    let translated = PostgreSQLTranslator::new()
        .translate_with_prereqs(&parsed)
        .map_err(|error| LimboError::ParseError(error.to_string()))?;
    reject_catalog_dml(translated.cmd.stmt())?;
    Ok(Some(StatementKind::Sql(Box::new(translated.cmd))))
}

fn prepare_statement(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<Statement> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(LimboError::InvalidArgument(
            "The supplied SQL string contains no statements".to_string(),
        ));
    }

    reject_sqlite_catalog_access(sql)?;

    if let Some(mut stmt) = try_prepare_special(pg_conn, sql)? {
        sql.clone_into(&mut stmt.sql);
        return Ok(stmt);
    }

    let parse_result =
        turso_pg_parser::parse(sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    reject_catalog_dml(translated.cmd.stmt())?;

    let options = prepare_options(&pg_conn.conn);
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

fn prepare_options(conn: &Connection) -> PrepareOptions {
    let state = conn
        .context::<PgSessionState>()
        .expect("PostgreSQL connections have session state")
        .lock()
        .unwrap();
    PrepareOptions {
        unqualified_database_search_path: state.search_path.clone(),
    }
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

    if is_set_transaction(&parse_result) {
        return Ok(Some(noop_statement(pg_conn, None)?));
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
            .map(|value| {
                value
                    .as_search_path_name()
                    .map(turso_pg_parser::quote_identifier)
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| LimboError::ParseError("incorrect format".to_string()))?
            .join(", ");
        // Change the setting when the statement runs, not when it is prepared.
        // set_config returns text, so IS NULL prevents SET from returning rows.
        return Ok(with_execution(
            pg_conn.clone(),
            pg_conn.conn.prepare(format!(
                "SELECT 0 WHERE set_config('search_path', '{}', {}) IS NULL",
                path.replace('\'', "''"),
                set_stmt.is_local
            ))?,
            None,
        ));
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

pub(crate) fn set_search_path(conn: &Connection, value: Option<&str>) -> Result<Value> {
    let path = value.map(parse_search_path).transpose()?;
    let state = conn.context::<PgSessionState>().ok_or_else(|| {
        LimboError::InvalidArgument("PostgreSQL session state is not initialized".to_string())
    })?;
    let mut state = state.lock().unwrap();
    state.search_path = path;
    state.search_path_setting = value.map(str::to_owned);
    Ok(Value::build_text(
        value.unwrap_or(DEFAULT_SEARCH_PATH).to_owned(),
    ))
}

pub(crate) fn search_path_setting(conn: &Connection) -> Option<String> {
    conn.context::<PgSessionState>()
        .and_then(|state| state.lock().unwrap().search_path_setting.clone())
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

#[cfg(test)]
mod tests {
    use super::{search_path_parser, PgSessionState, SessionState};
    use chumsky::Parser;
    use std::any::Any;
    use std::sync::{Arc, Mutex};

    #[test]
    fn session_state_alias_matches_injected_type() {
        let state: Arc<dyn Any + Send + Sync> = Arc::new(Mutex::new(SessionState::default()));
        assert!(state.downcast_ref::<PgSessionState>().is_some());
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
}
