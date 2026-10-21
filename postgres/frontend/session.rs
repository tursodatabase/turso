use chumsky::{error::EmptyErr, prelude::*};
use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZero;
use std::str;
use std::sync::{Arc, Mutex};

use crate::catalog::{self, PostgresDialect};
use crate::functions::ConfigParameter;
use turso_core::{Connection, LimboError, Result, Statement, Value};
use turso_parser::ast::{self};
use turso_pg_parser::translator::{
    check_set_transaction, is_comment_on, is_refresh_matview, try_extract_copy_from,
    try_extract_create_schema, try_extract_drop_schema, try_extract_set, try_extract_show,
    unnamed_index_name, unqualified_create_table_name_location, PgCopyFromStmt, PgCreateSchemaStmt,
    PgDropSchemaStmt, PgSetStmt, PgSetValue, PgShowStmt, PostgreSQLTranslator,
};

use crate::copy::parse_copy_text_format;

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

struct PgConnectionInner {
    conn: Arc<Connection>,
}

/// The text that `search_path` was set to, for SHOW. The search path itself
/// is on the connection.
#[derive(Default)]
struct SessionState {
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
            .db_opts(opts.with_mvcc(true)),
    )
}

impl PgConnection {
    pub fn connect(db: &Arc<turso_core::Database>) -> Result<Self> {
        let context = Arc::new(PgSessionState::default());
        let conn = db.connect_with_context(context)?;
        conn.add_namespace(
            "public",
            turso_core::Namespace::database(turso_core::MAIN_DB_ID),
        );
        set_search_path(&conn, None)?;
        Ok(Self {
            inner: Arc::new(PgConnectionInner { conn }),
        })
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
    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    reject_catalog_dml(translated.cmd.stmt())?;

    for prereq in translated.prereqs {
        let input = prereq.to_string();
        let mut stmt = pg_conn.conn.prepare_translated_stmt(prereq, &input)?;
        stmt.run_ignore_rows()?;
    }

    pg_conn.conn.prepare_translated_cmd(translated.cmd, &sql)
}

/// The schema that a CREATE TABLE statement naming its table without a
/// schema creates the table in, with the byte offset of the table name: the
/// first schema of `search_path` that exists, like PostgreSQL. None when the
/// statement is not such a CREATE TABLE or when the table goes to public,
/// which is where core puts it anyway.
fn create_table_schema_from_search_path(
    pg_conn: &Arc<PgConnectionInner>,
    parse_result: &turso_pg_parser::ParseResult,
) -> Result<Option<(usize, String)>> {
    let Some(location) = unqualified_create_table_name_location(parse_result) else {
        return Ok(None);
    };
    let schema = current_schema(&pg_conn.conn).ok_or_else(|| {
        LimboError::ParseError("no schema has been selected to create in".to_string())
    })?;
    if schema.eq_ignore_ascii_case("public") {
        return Ok(None);
    }
    Ok(Some((location, schema)))
}

/// Whether the schema of table `table` already has a table, index, view or
/// sequence named `name`, which makes PostgreSQL choose another name for a
/// new index on the table. A table named with a schema is in that schema.
/// Otherwise the table is looked up in the same order as core does: temp
/// first, then the databases of the search path. Main when no database has
/// the table.
fn relation_name_is_taken(
    conn: &Arc<Connection>,
    schema: Option<&str>,
    table: &str,
    name: &str,
) -> Result<bool> {
    let database_names: HashMap<usize, String> = conn
        .list_all_databases()
        .into_iter()
        .map(|(id, database, _)| (id, database))
        .collect();
    let databases: Vec<String> = match schema {
        Some(schema) if schema.eq_ignore_ascii_case("public") => vec!["main".to_string()],
        Some(schema) => database_names
            .values()
            .filter(|database| database.eq_ignore_ascii_case(schema))
            .cloned()
            .collect(),
        None => database_names
            .contains_key(&turso_core::TEMP_DB_ID)
            .then_some(turso_core::TEMP_DB_ID)
            .into_iter()
            .chain(conn.search_order())
            .chain(std::iter::once(turso_core::MAIN_DB_ID))
            .map(|id| database_names[&id].clone())
            .collect(),
    };
    let mut taken = false;
    for database in databases {
        let mut stmt = conn.prepare_sqlite(format!(
            "SELECT \
             EXISTS (SELECT 1 FROM \"{database}\".sqlite_schema \
                     WHERE type = 'table' AND name COLLATE NOCASE = ?1), \
             EXISTS (SELECT 1 FROM \"{database}\".sqlite_schema \
                     WHERE type IN ('table', 'index', 'view') \
                     AND name COLLATE NOCASE IN (?2, '{SEQUENCE_TABLE_PREFIX}' || ?2))"
        ))?;
        stmt.bind_at(
            NonZero::new(1).unwrap(),
            Value::build_text(table.to_string()),
        )?;
        stmt.bind_at(
            NonZero::new(2).unwrap(),
            Value::build_text(name.to_string()),
        )?;
        let row = &stmt.run_collect_rows()?[0];
        taken = row[1] == Value::from_i64(1);
        if row[0] == Value::from_i64(1) {
            break;
        }
    }
    Ok(taken)
}

/// The first schema of `search_path` that exists, which is what PostgreSQL's
/// current_schema() returns. None when no schema of `search_path` exists.
/// The default search_path, "$user", public, gives public, since there is no
/// schema named after a user.
pub(crate) fn current_schema(conn: &Connection) -> Option<String> {
    let database_id = *conn.search_order().first()?;
    if database_id == turso_core::MAIN_DB_ID {
        return Some("public".to_owned());
    }
    conn.namespace_name(database_id)
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

    if check_set_transaction(&parse_result).map_err(|e| LimboError::ParseError(e.to_string()))? {
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(show_stmt) = try_extract_show(&parse_result) {
        return Ok(Some(handle_pg_show(pg_conn, &show_stmt)?));
    }

    if let Some(stmt) = try_extract_create_schema(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?
    {
        handle_pg_create_schema(pg_conn, &stmt)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(stmt) = try_extract_drop_schema(&parse_result) {
        handle_pg_drop_schema(&pg_conn.conn, &stmt)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if is_refresh_matview(&parse_result).map_err(|e| LimboError::ParseError(e.to_string()))? {
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if is_comment_on(&parse_result) {
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(stmt) =
        try_extract_copy_from(&parse_result).map_err(|e| LimboError::ParseError(e.to_string()))?
    {
        let rows_inserted = handle_pg_copy_from(&pg_conn.conn, &stmt)?;
        let stmt = noop_statement(&pg_conn.conn)?;
        stmt.set_n_change(rows_inserted as i64);
        return Ok(Some(stmt));
    }

    Ok(None)
}

fn noop_statement(conn: &Arc<Connection>) -> Result<Statement> {
    conn.prepare("SELECT 0 WHERE 0")
}

fn execute_sqlite(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    let mut stmt = conn.prepare_sqlite(sql)?;
    stmt.run_ignore_rows()
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
        return pg_conn.conn.prepare(format!(
            "SELECT 0 WHERE set_config('search_path', '{}', {}) IS NULL",
            path.replace('\'', "''"),
            set_stmt.is_local
        ));
    }
    if set_value_already_in_effect(&set_stmt.name, &set_stmt.values) {
        return noop_statement(&pg_conn.conn);
    }
    Err(LimboError::ParseError(format!(
        "SET {} is not supported",
        set_stmt.name
    )))
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
    match show_stmt.name.parse::<ConfigParameter>() {
        Ok(ConfigParameter::SearchPath) => pg_conn
            .conn
            .prepare("SELECT current_setting('search_path') AS search_path"),
        Err(_) => Err(LimboError::ParseError(format!(
            "SHOW {} is not supported",
            show_stmt.name
        ))),
    }
}

/// `$user` stands for a schema named after the session user, which does
/// not exist, so it is not a name that core looks up.
pub(crate) fn set_search_path(conn: &Connection, value: Option<&str>) -> Result<Value> {
    let mut path = parse_search_path(value.unwrap_or(DEFAULT_SEARCH_PATH))?;
    path.retain(|name| name != "$user" && !is_core_database_name(name));
    let state = conn.context::<PgSessionState>().ok_or_else(|| {
        LimboError::InvalidArgument("PostgreSQL session state is not initialized".to_string())
    })?;
    state.lock().unwrap().search_path_setting = value.map(str::to_owned);
    conn.set_search_path(Some(path));
    Ok(Value::build_text(
        value.unwrap_or(DEFAULT_SEARCH_PATH).to_owned(),
    ))
}

/// `main` and `temp` name databases in core, not PostgreSQL schemas, so a
/// search_path that lists them finds nothing there, as in PostgreSQL.
fn is_core_database_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("main") || name.eq_ignore_ascii_case("temp")
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

fn handle_pg_create_schema(
    pg_conn: &Arc<PgConnectionInner>,
    stmt: &PgCreateSchemaStmt,
) -> Result<()> {
    let conn = &pg_conn.conn;
    let name = stmt.name.to_lowercase();
    if name == "public" {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        )));
    }

    if schema_exists(conn, &name)? {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        )));
    }
    if name.starts_with("pg_") {
        return Err(LimboError::ParseError(format!(
            "unacceptable schema name \"{name}\": the prefix \"pg_\" is reserved for system schemas"
        )));
    }

    // A failed element is undone by dropping the schema, and DETACH cannot
    // run inside a transaction.
    if !stmt.elements.is_empty() && !conn.get_auto_commit() {
        return Err(LimboError::ParseError(
            "CREATE SCHEMA with schema elements cannot run inside a transaction block".to_string(),
        ));
    }

    let path = schema_file_path(conn, &name);
    execute_sqlite(
        conn,
        format!("ATTACH '{}' AS \"{}\"", path.replace('\'', "''"), name),
    )?;
    if stmt.elements.is_empty() {
        return Ok(());
    }
    let setting = search_path_setting(conn);
    let mut search_path = turso_pg_parser::quote_identifier(&name);
    let rest = setting.as_deref().unwrap_or(DEFAULT_SEARCH_PATH).trim();
    if !rest.is_empty() {
        search_path = format!("{search_path}, {rest}");
    }
    set_search_path(conn, Some(&search_path))?;
    let result = stmt
        .elements
        .iter()
        .try_for_each(|sql| prepare_statement(pg_conn, sql)?.run_ignore_rows());
    set_search_path(conn, setting.as_deref())?;
    if let Err(err) = result {
        handle_pg_drop_schema(
            conn,
            &PgDropSchemaStmt {
                name,
                if_exists: false,
                cascade: true,
            },
        )?;
        return Err(err);
    }
    Ok(())
}

fn schema_file_path(conn: &Connection, schema_name: &str) -> String {
    let main_path = conn.db_file_path();
    let filename = format!("turso-postgres-schema-{schema_name}.db");
    if main_path == ":memory:" {
        filename
    } else {
        let parent = std::path::Path::new(&main_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        parent.join(&filename).to_string_lossy().to_string()
    }
}

fn handle_pg_drop_schema(conn: &Arc<Connection>, stmt: &PgDropSchemaStmt) -> Result<()> {
    let name = stmt.name.to_lowercase();
    if name != "public" && !schema_exists(conn, &name)? {
        if stmt.if_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" does not exist"
        )));
    }

    let database = if name == "public" { "main" } else { &name };
    let objects = schema_objects(conn, database)?;
    if !stmt.cascade && !objects.is_empty() {
        return Err(LimboError::ParseError(format!(
            "cannot drop schema \"{name}\" because other objects depend on it"
        )));
    }

    without_foreign_keys(conn, || {
        for object in &objects {
            execute_sqlite(conn, object.drop_sql(database))?;
        }
        Ok(())
    })?;
    // PostgreSQL drops public like any other schema, but turso keeps it,
    // because unqualified names of the main database are its objects.
    if name == "public" {
        return Ok(());
    }
    execute_sqlite(conn, format!("DETACH \"{name}\""))
}

/// Runs `f` with foreign keys not enforced. DROP SCHEMA ... CASCADE drops
/// the foreign keys of the tables that it drops along with the tables, so
/// they must not stop the drops or delete rows.
fn without_foreign_keys(conn: &Arc<Connection>, f: impl FnOnce() -> Result<()>) -> Result<()> {
    let enabled = conn.foreign_keys_enabled();
    conn.set_foreign_keys_enabled(false);
    let result = f();
    conn.set_foreign_keys_enabled(enabled);
    result
}

/// An object of a schema, which DROP SCHEMA ... CASCADE drops.
enum SchemaObject {
    View(String),
    Table(String),
    Sequence(String),
}

impl SchemaObject {
    fn drop_sql(&self, database: &str) -> String {
        let kind = match self {
            SchemaObject::View(_) => "VIEW",
            SchemaObject::Table(_) => "TABLE",
            SchemaObject::Sequence(_) => "SEQUENCE",
        };
        format!(
            "DROP {kind} \"{database}\".\"{}\"",
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

/// The table in which core stores sequence `s` is named
/// `__turso_internal_seq_s`.
const SEQUENCE_TABLE_PREFIX: &str = "__turso_internal_seq_";

/// The objects of the schema that database `database` holds, in the order
/// that DROP SCHEMA drops them: views first, because they depend on tables.
/// Indexes and triggers go with their tables.
fn schema_objects(conn: &Arc<Connection>, database: &str) -> Result<Vec<SchemaObject>> {
    let mut stmt = conn.prepare_sqlite(format!(
        "SELECT type, name FROM \"{database}\".sqlite_schema \
         WHERE type IN ('view', 'table') ORDER BY name"
    ))?;
    let mut views = Vec::new();
    let mut tables = Vec::new();
    let mut sequences = Vec::new();
    for row in stmt.run_collect_rows()? {
        let (Some(Value::Text(kind)), Some(Value::Text(name))) = (row.first(), row.get(1)) else {
            continue;
        };
        let name = name.as_str().to_string();
        if kind.as_str() == "view" {
            views.push(SchemaObject::View(name));
        } else if let Some(sequence) = name.strip_prefix(SEQUENCE_TABLE_PREFIX) {
            if !sequence.starts_with("__turso_internal_") {
                sequences.push(SchemaObject::Sequence(sequence.to_string()));
            }
        } else if !name.starts_with("sqlite_") && !name.starts_with("__turso_internal_") {
            tables.push(SchemaObject::Table(name));
        }
    }
    Ok(views.into_iter().chain(tables).chain(sequences).collect())
}

fn handle_pg_copy_from(conn: &Arc<Connection>, stmt: &PgCopyFromStmt) -> Result<usize> {
    let data = std::fs::read_to_string(&stmt.filename).map_err(|e| {
        LimboError::ParseError(format!("COPY FROM: cannot read '{}': {}", stmt.filename, e))
    })?;

    let table_name = match &stmt.schema_name {
        Some(schema) => format!("\"{schema}\".\"{}\"", stmt.table_name),
        None => format!("\"{}\"", stmt.table_name),
    };
    let column_names = get_table_columns(conn, &stmt.table_name, stmt.schema_name.as_deref())?;
    if column_names.is_empty() {
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
        None => (String::new(), column_names.len()),
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

    let rows_inserted = rows.len();
    let mut begin = conn.prepare_sqlite("BEGIN CONCURRENT")?;
    begin.run_ignore_rows()?;

    let result = (|| {
        let mut insert_stmt = conn.prepare_sqlite(&insert_sql)?;
        for row in &rows {
            for (i, val) in row.iter().enumerate() {
                let index = NonZero::new(i + 1).unwrap();
                match val {
                    Some(s) => insert_stmt.bind_at(index, Value::build_text(s.clone()))?,
                    None => insert_stmt.bind_at(index, Value::Null)?,
                }
            }
            insert_stmt.run_ignore_rows()?;
            insert_stmt.reset()?;
            insert_stmt.clear_bindings();
        }

        let mut commit = conn.prepare_sqlite("COMMIT")?;
        commit.run_ignore_rows()?;
        Ok(rows_inserted)
    })();

    if result.is_err() {
        if let Ok(mut rollback) = conn.prepare_sqlite("ROLLBACK") {
            let _ = rollback.run_ignore_rows();
        }
    }

    result
}

fn get_table_columns(
    conn: &Arc<Connection>,
    table_name: &str,
    schema_name: Option<&str>,
) -> Result<Vec<String>> {
    let sql = match schema_name {
        Some(schema) => format!("PRAGMA \"{schema}\".table_info('{table_name}')"),
        None => format!("PRAGMA table_info('{table_name}')"),
    };
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::Text(t)) => Some(t.as_str().to_string()),
            _ => None,
        })
        .collect())
}

fn schema_exists(conn: &Arc<Connection>, schema_name: &str) -> Result<bool> {
    let sql = format!(
        "SELECT 1 FROM pragma_database_list WHERE name = '{}'",
        schema_name.replace('\'', "''")
    );
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(!rows.is_empty())
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
