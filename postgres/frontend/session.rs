use chumsky::{error::EmptyErr, prelude::*};
use std::num::NonZero;
use std::str;
use std::sync::{Arc, Mutex};

use crate::catalog::{self, PostgresDialect};
use turso_core::{Connection, LimboError, Result, Statement, Value};
use turso_parser::ast::{self};
use turso_pg_parser::translator::{
    is_comment_on, is_refresh_matview, is_set_transaction, split_relation_table_name,
    try_extract_copy_from, try_extract_create_schema, try_extract_drop_schema, try_extract_set,
    try_extract_show, ObjectKind, PgCopyFromStmt, PgCreateSchemaStmt, PgDropSchemaStmt, PgSetStmt,
    PostgreSQLTranslator, SchemaLookup,
};
use turso_pg_parser::ParseError;

use crate::copy::parse_copy_text_format;

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

struct PgConnectionInner {
    conn: Arc<Connection>,
}

#[derive(Default)]
struct SessionState {
    search_path: Option<Vec<String>>,
    search_path_setting: Option<String>,
}

type PgSessionState = Mutex<SessionState>;

pub(crate) const DEFAULT_SEARCH_PATH: &str = "\"$user\", public";

/// The user that every session runs as, which `$user` in `search_path`
/// stands for.
const SESSION_USER: &str = "turso";

/// The table that lists the schemas other than public. A table `t` of
/// schema `s` is stored in the main database as table `s.t`, and an empty
/// schema has no table, so the schemas are listed here. The `pg_` prefix
/// hides it from the catalog like the other system tables.
pub(crate) const SCHEMAS_TABLE: &str = "pg_turso_schemas";

/// The OID of the first schema created. Schemas get the next free OID from
/// here, above the OIDs that the catalog gives to tables and types.
const FIRST_SCHEMA_OID: i64 = 1_000_000;

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
        let mut stmt = self.inner.conn.prepare_internal(sql)?;
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

    let mut parse_result =
        turso_pg_parser::parse(sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
    let lookup = SessionSchemaLookup::new(&pg_conn.conn);
    let translator = PostgreSQLTranslator::with_lookup(&lookup);
    let create_table_sql = translator
        .write_schemas_into_create_table(&mut parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    let translated = translator
        .translate_with_prereqs(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    reject_catalog_dml(translated.cmd.stmt())?;

    for prereq in translated.prereqs {
        let input = prereq.to_string();
        let mut stmt = pg_conn.conn.prepare_translated_stmt(prereq, &input)?;
        stmt.run_ignore_rows()?;
    }

    let sql = create_table_sql.as_deref().unwrap_or(sql);
    pg_conn.conn.prepare_translated_cmd(translated.cmd, sql)
}

/// Looks up schemas and objects for the translator through the
/// `search_path` of a session.
struct SessionSchemaLookup<'c> {
    conn: &'c Arc<Connection>,
    schema: Arc<turso_core::schema::Schema>,
    /// The schemas other than public, read when first needed.
    schemas: std::cell::OnceCell<Vec<String>>,
}

impl<'c> SessionSchemaLookup<'c> {
    fn new(conn: &'c Arc<Connection>) -> Self {
        conn.maybe_update_schema();
        Self {
            conn,
            schema: conn.current_schema(),
            schemas: std::cell::OnceCell::new(),
        }
    }
}

impl SchemaLookup for SessionSchemaLookup<'_> {
    fn search_path(&self) -> Vec<String> {
        search_path(self.conn)
    }

    fn schema_exists(&self, schema: &str) -> std::result::Result<bool, ParseError> {
        if schema == "public" {
            return Ok(true);
        }
        let schemas = match self.schemas.get() {
            Some(schemas) => schemas,
            None => {
                let schemas =
                    list_schemas(self.conn).map_err(|e| ParseError::ParseError(e.to_string()))?;
                self.schemas.get_or_init(|| schemas)
            }
        };
        Ok(schemas.iter().any(|name| name == schema))
    }

    fn object_exists(&self, kind: ObjectKind, table_name: &str) -> bool {
        match kind {
            ObjectKind::Relation => {
                self.schema.get_table(table_name).is_some()
                    || self.schema.get_view(table_name).is_some()
                    || self.schema.get_materialized_view(table_name).is_some()
            }
            ObjectKind::Index => self
                .schema
                .indexes
                .values()
                .flatten()
                .any(|index| index.name.eq_ignore_ascii_case(table_name)),
            ObjectKind::Sequence => self.schema.get_sequence(table_name).is_some(),
        }
    }
}

pub(crate) fn search_path(conn: &Connection) -> Vec<String> {
    let state = conn
        .context::<PgSessionState>()
        .expect("PostgreSQL connections have session state")
        .lock()
        .unwrap();
    match &state.search_path {
        Some(search_path) => search_path
            .iter()
            .map(|schema| match schema.as_str() {
                "$user" => SESSION_USER.to_string(),
                _ => schema.to_lowercase(),
            })
            .collect(),
        None => vec![SESSION_USER.to_string(), "public".to_string()],
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
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(show_stmt) = try_extract_show(&parse_result) {
        let pragma_sql = format!("PRAGMA {}", show_stmt.name);
        return Ok(Some(pg_conn.conn.prepare(&pragma_sql)?));
    }

    if let Some(stmt) = try_extract_create_schema(&parse_result) {
        handle_pg_create_schema(&pg_conn.conn, &stmt)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(stmt) = try_extract_drop_schema(&parse_result) {
        handle_pg_drop_schema(&pg_conn.conn, &stmt)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if is_refresh_matview(&parse_result) {
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if is_comment_on(&parse_result) {
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(stmt) = try_extract_copy_from(&parse_result) {
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
    let value = set_stmt.values.first().ok_or_else(|| {
        LimboError::ParseError(format!("SET {}: no value provided", set_stmt.name))
    })?;
    let pragma_sql = format!("PRAGMA {} = {}", set_stmt.name, value.to_sql_string());
    pg_conn.conn.prepare(&pragma_sql)
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

fn handle_pg_create_schema(conn: &Arc<Connection>, stmt: &PgCreateSchemaStmt) -> Result<()> {
    let name = stmt.name.to_lowercase();
    if name == "public" || schema_exists(conn, &name)? {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        )));
    }
    if name == "main" || name == "temp" {
        return Err(LimboError::ParseError(format!(
            "unacceptable schema name \"{name}\": it is the name of a database"
        )));
    }
    if name.starts_with("pg_") {
        return Err(LimboError::ParseError(format!(
            "unacceptable schema name \"{name}\": the prefix \"pg_\" is reserved for system schemas"
        )));
    }
    if name.contains('.') {
        return Err(LimboError::ParseError(format!(
            "names containing \".\" are not supported: \"{name}\""
        )));
    }

    in_transaction(conn, || {
        execute_sqlite(
            conn,
            format!(
                "CREATE TABLE IF NOT EXISTS {SCHEMAS_TABLE} \
                 (oid INTEGER PRIMARY KEY, nspname TEXT NOT NULL UNIQUE)"
            ),
        )?;
        execute_sqlite(
            conn,
            format!(
                "INSERT INTO {SCHEMAS_TABLE} (oid, nspname) \
                 SELECT coalesce(max(oid) + 1, {FIRST_SCHEMA_OID}), '{}' FROM {SCHEMAS_TABLE}",
                name.replace('\'', "''")
            ),
        )
    })
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

    let objects = schema_objects(&conn.current_schema(), &name);
    if !stmt.cascade && !objects.is_empty() {
        return Err(LimboError::ParseError(format!(
            "cannot drop schema \"{name}\" because other objects depend on it"
        )));
    }

    in_transaction(conn, || {
        for object in &objects {
            execute_sqlite(conn, object.drop_sql())?;
        }
        // PostgreSQL drops public like any other schema, but turso keeps it,
        // because unqualified names of the main database are its objects.
        if name == "public" {
            return Ok(());
        }
        execute_sqlite(
            conn,
            format!(
                "DELETE FROM {SCHEMAS_TABLE} WHERE nspname = '{}'",
                name.replace('\'', "''")
            ),
        )
    })
}

/// An object of a schema, which DROP SCHEMA ... CASCADE drops.
enum SchemaObject {
    View(String),
    Table(String),
    Sequence(String),
}

impl SchemaObject {
    fn drop_sql(&self) -> String {
        let (kind, name) = match self {
            SchemaObject::View(name) => ("VIEW", name),
            SchemaObject::Table(name) => ("TABLE", name),
            SchemaObject::Sequence(name) => ("SEQUENCE", name),
        };
        format!("DROP {kind} \"{}\"", name.replace('"', "\"\""))
    }
}

/// The objects of schema `schema_name`, in the order that DROP SCHEMA drops
/// them: views first, because they depend on tables. Indexes and triggers go
/// with their tables.
fn schema_objects(schema: &turso_core::schema::Schema, schema_name: &str) -> Vec<SchemaObject> {
    let in_schema = |table_name: &str| {
        split_relation_table_name(table_name).0 == schema_name
            && !catalog::is_system_table_name(table_name)
    };
    let mut views: Vec<String> = schema
        .views
        .keys()
        .chain(schema.materialized_view_names.iter())
        .filter(|name| in_schema(name))
        .cloned()
        .collect();
    let mut tables: Vec<String> = schema
        .tables
        .iter()
        .filter(|(name, table)| {
            in_schema(name)
                && matches!(table.as_ref(), turso_core::schema::Table::BTree(_))
                && !schema.materialized_view_names.contains(*name)
        })
        .map(|(name, _)| name.clone())
        .collect();
    let mut sequences: Vec<String> = schema
        .sequences
        .keys()
        .filter(|name| in_schema(name))
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

/// Runs `f` in a transaction: the current one, or a new one that is
/// committed if `f` succeeds and rolled back if it fails.
fn in_transaction(conn: &Arc<Connection>, f: impl FnOnce() -> Result<()>) -> Result<()> {
    if !conn.get_auto_commit() {
        return f();
    }
    execute_sqlite(conn, "BEGIN")?;
    match f() {
        Ok(()) => execute_sqlite(conn, "COMMIT"),
        Err(e) => {
            execute_sqlite(conn, "ROLLBACK")?;
            Err(e)
        }
    }
}

fn handle_pg_copy_from(conn: &Arc<Connection>, stmt: &PgCopyFromStmt) -> Result<usize> {
    let data = std::fs::read_to_string(&stmt.filename).map_err(|e| {
        LimboError::ParseError(format!("COPY FROM: cannot read '{}': {}", stmt.filename, e))
    })?;

    let lookup = SessionSchemaLookup::new(conn);
    let table_name = PostgreSQLTranslator::with_lookup(&lookup)
        .resolve_relation_name(stmt.schema_name.as_deref().unwrap_or(""), &stmt.table_name)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    let column_names = get_table_columns(conn, &table_name)?;
    let table_name = format!("\"{}\"", table_name.replace('"', "\"\""));
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

    let mut rows = parse_copy_text_format(&data, delimiter, null_string, num_columns)?;
    if stmt.header && !rows.is_empty() {
        rows.remove(0);
    }

    let rows_inserted = rows.len();
    let mut begin = conn.prepare_sqlite("BEGIN")?;
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

fn get_table_columns(conn: &Arc<Connection>, table_name: &str) -> Result<Vec<String>> {
    let sql = format!("PRAGMA table_info('{}')", table_name.replace('\'', "''"));
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

/// The schemas other than public.
pub(crate) fn list_schemas(conn: &Arc<Connection>) -> Result<Vec<String>> {
    if conn.current_schema().get_table(SCHEMAS_TABLE).is_none() {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare_sqlite(format!(
        "SELECT nspname FROM {SCHEMAS_TABLE} ORDER BY nspname"
    ))?;
    Ok(stmt
        .run_collect_rows()?
        .into_iter()
        .filter_map(|row| match row.into_iter().next() {
            Some(Value::Text(name)) => Some(name.as_str().to_string()),
            _ => None,
        })
        .collect())
}

fn schema_exists(conn: &Arc<Connection>, schema_name: &str) -> Result<bool> {
    Ok(list_schemas(conn)?.iter().any(|name| name == schema_name))
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
