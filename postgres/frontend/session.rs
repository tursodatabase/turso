use chumsky::{error::EmptyErr, prelude::*};
use std::collections::BTreeMap;
use std::num::NonZero;
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::catalog::{self, PostgresDialect};
use turso_core::{
    ConnectOpts, Connection, Database, LimboError, PrepareOptions, Result, Statement, Value,
};
use turso_parser::ast::{self};
use turso_pg_parser::translator::{
    is_comment_on, is_refresh_matview, is_set_transaction, try_extract_copy_from,
    try_extract_create_schema, try_extract_drop_schema, try_extract_set, try_extract_show,
    PgCopyFromStmt, PgCreateSchemaStmt, PgDropSchemaStmt, PgSetStmt, PostgreSQLTranslator,
};

use crate::copy::parse_copy_text_format;

/// A database opened with the PostgreSQL frontend. Each connection to it is
/// a session with its own transaction and settings, and sees the schemas
/// that other sessions create or drop.
#[derive(Clone)]
pub struct PgDatabase {
    db: Arc<Database>,
    schemas: Arc<SharedSchemas>,
}

impl PgDatabase {
    pub fn new(db: Arc<Database>) -> Self {
        let files_on_host_file_system = db.io.uses_host_file_system() && db.path != ":memory:";
        Self {
            db,
            schemas: Arc::new(SharedSchemas {
                list: Mutex::new(SchemaList::default()),
                version: AtomicU64::new(0),
                files_on_host_file_system,
            }),
        }
    }

    /// Opens a session, connecting with `opts`.
    pub fn connect(&self, opts: ConnectOpts) -> Result<PgConnection> {
        let session_state = Arc::new(PgSessionState::default());
        let conn = self.db.connect(opts.with_context(session_state.clone()))?;
        let schemas = self.schemas.clone();
        conn.set_attached_write_check(Arc::new(move |name| {
            check_schema_is_not_dropped(&schemas, &session_state, name)
        }));
        Ok(PgConnection::with_schemas(conn, self.schemas.clone()))
    }
}

/// Fails a write to schema `name` if another session dropped the schema
/// that this session has attached as `name`. Runs after the write took the
/// write lock of the schema, and DROP SCHEMA takes that lock too, so the
/// schema cannot be dropped between this check and the write.
fn check_schema_is_not_dropped(
    schemas: &SharedSchemas,
    session_state: &PgSessionState,
    name: &str,
) -> Result<()> {
    let (attached_id, dropping_schema) = {
        let state = session_state.lock().unwrap();
        (
            state.attached_schemas.get(name).copied(),
            state.dropping_schema,
        )
    };
    let Some(attached_id) = attached_id else {
        return Ok(());
    };
    if dropping_schema {
        return Ok(());
    }
    let list = schemas.list.lock().unwrap();
    if list.schemas.get(name).map(|schema| schema.id) != Some(attached_id) {
        return Err(LimboError::TxError(format!(
            "schema \"{name}\" was dropped by another session"
        )));
    }
    Ok(())
}

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

struct PgConnectionInner {
    conn: Arc<Connection>,
    schemas: Arc<SharedSchemas>,
}

impl PgConnectionInner {
    fn session_state(&self) -> &PgSessionState {
        self.conn
            .context::<PgSessionState>()
            .expect("PostgreSQL connections have session state")
    }

    /// Attaches the database file at `path` as schema `name`, in this session
    /// and in every other session of the database. Returns false, without
    /// attaching anything, if another session already added schema `name`.
    /// The list of schemas stays locked while attaching, so two sessions
    /// cannot both add the same name.
    fn add_schema(&self, name: &str, path: &str) -> Result<bool> {
        let mut list = self.schemas.list.lock().unwrap();
        if list.schemas.contains_key(name) {
            return Ok(false);
        }
        attach_database(&self.conn, name, path)?;
        let id = self.schemas.version.fetch_add(1, Ordering::AcqRel) + 1;
        list.schemas.insert(
            name.to_string(),
            SharedSchema {
                path: path.to_string(),
                id,
            },
        );
        list.attached(id);
        self.session_state()
            .lock()
            .unwrap()
            .attached_schemas
            .insert(name.to_string(), id);
        Ok(true)
    }

    /// Drops schema `name`, in this session and in every other session of the
    /// database, and with `cascade` also drops its tables. The list of schemas
    /// stays locked, so no session can attach the schema or start using it
    /// while it is dropped. If the schema files are on the host file system,
    /// the file of the schema is deleted once no session has it attached.
    /// Returns false, without dropping anything, if another session dropped
    /// the schema that this session has attached as `name`.
    fn drop_schema(&self, name: &str, cascade: bool) -> Result<bool> {
        let mut list = self.schemas.list.lock().unwrap();
        let attached_id = self
            .session_state()
            .lock()
            .unwrap()
            .attached_schemas
            .get(name)
            .copied();
        if list.schemas.get(name).map(|schema| schema.id) != attached_id {
            return Ok(false);
        }
        self.schemas.version.fetch_add(1, Ordering::AcqRel);
        self.session_state().lock().unwrap().dropping_schema = true;
        let emptied = empty_schema(&self.conn, name, cascade);
        self.session_state().lock().unwrap().dropping_schema = false;
        emptied?;
        execute_sqlite_internal(&self.conn, format!("DETACH \"{name}\""))?;
        {
            let mut state = self.session_state().lock().unwrap();
            state.attached_schemas.remove(name);
            // If moving the file fails, the schema stays in the list, and the
            // session attaches it again before its next statement.
            state.schemas_version = 0;
        }
        let schema = list.schemas[name].clone();
        list.detached(schema.id);
        if self.schemas.files_on_host_file_system {
            list.delete_file_when_unused(&schema)?;
        }
        list.schemas.remove(name);
        Ok(true)
    }
}

impl Drop for PgConnectionInner {
    fn drop(&mut self) {
        let mut list = self.schemas.list.lock().unwrap();
        let state = self.session_state().lock().unwrap();
        for id in state.attached_schemas.values() {
            list.detached(*id);
        }
    }
}

#[derive(Default)]
struct SessionState {
    search_path: Option<Vec<String>>,
    search_path_setting: Option<String>,
    /// The `SharedSchemas::version` that this session's attached schemas
    /// match.
    schemas_version: u64,
    /// Name to `SharedSchema::id` of each schema attached to this session.
    attached_schemas: BTreeMap<String, u64>,
    /// True while this session drops a schema. The session then holds the
    /// list of schemas locked, so its writes to the schema skip
    /// `check_schema_is_not_dropped`.
    dropping_schema: bool,
}

/// The schemas of a database, shared by all of its sessions. A schema is a
/// database file attached to a connection, and an attachment belongs to one
/// connection only. Before running a statement, each session attaches the
/// schemas that other sessions created and detaches the ones they dropped.
struct SharedSchemas {
    list: Mutex<SchemaList>,
    /// Incremented whenever the schemas in `list` change.
    version: AtomicU64,
    /// Whether the schema files are on the host file system, so that the
    /// file of a dropped schema can be moved and deleted with `std::fs`.
    files_on_host_file_system: bool,
}

#[derive(Default)]
struct SchemaList {
    /// Schema name to its database file.
    schemas: BTreeMap<String, SharedSchema>,
    /// `SharedSchema::id` to the number of sessions that have the schema
    /// attached, for schemas in `schemas` and for dropped schemas.
    sessions: BTreeMap<u64, usize>,
    /// `SharedSchema::id` of a dropped schema that sessions still have
    /// attached, to the path its file was moved to. The file is deleted when
    /// the last of those sessions detaches the schema.
    dropped_files: BTreeMap<u64, String>,
}

#[derive(Clone)]
struct SharedSchema {
    path: String,
    /// Tells a dropped schema apart from a schema created later with the
    /// same name and path. DROP SCHEMA moves the file of the dropped schema
    /// away, so a session that still has it attached must attach the new
    /// file.
    id: u64,
}

impl SchemaList {
    fn attached(&mut self, id: u64) {
        *self.sessions.entry(id).or_insert(0) += 1;
    }

    /// Records that a session detached the schema with `SharedSchema::id`
    /// `id`, and deletes the file of a dropped schema that no session has
    /// attached anymore.
    fn detached(&mut self, id: u64) {
        let sessions = self
            .sessions
            .get_mut(&id)
            .expect("a schema attached to a session has a session count");
        *sessions -= 1;
        if *sessions > 0 {
            return;
        }
        self.sessions.remove(&id);
        if let Some(path) = self.dropped_files.remove(&id) {
            delete_database_files(&path);
        }
    }

    /// Deletes the file of dropped schema `schema` if no session has it
    /// attached. Otherwise moves the file away, so that the sessions that
    /// still have it attached can keep using it until they notice the drop,
    /// and a schema created later with the same name gets a new file. If a
    /// file cannot be moved, moves back the files that were moved and
    /// returns an error.
    fn delete_file_when_unused(&mut self, schema: &SharedSchema) -> Result<()> {
        if !self.sessions.contains_key(&schema.id) {
            delete_database_files(&schema.path);
            return Ok(());
        }
        let dropped_path = format!("{}.dropped-{}", schema.path, schema.id);
        let mut moved = Vec::new();
        for suffix in DATABASE_FILE_SUFFIXES {
            let from = format!("{}{suffix}", schema.path);
            let to = format!("{dropped_path}{suffix}");
            match std::fs::rename(&from, &to) {
                Ok(()) => moved.push((from, to)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && !suffix.is_empty() => {}
                Err(e) => {
                    for (from, to) in moved {
                        if let Err(e) = std::fs::rename(&to, &from) {
                            tracing::warn!("Failed to move {to} back to {from}: {e}");
                        }
                    }
                    return Err(LimboError::InternalError(format!(
                        "cannot move the file of dropped schema from {from} to {to}: {e}"
                    )));
                }
            }
        }
        self.dropped_files.insert(schema.id, dropped_path);
        Ok(())
    }
}

/// Suffixes of the database file and of the files next to it: the WAL, the
/// shared memory file, and the MVCC log.
const DATABASE_FILE_SUFFIXES: [&str; 4] = ["", "-wal", "-shm", "-log"];

fn delete_database_files(path: &str) {
    for suffix in DATABASE_FILE_SUFFIXES {
        let file = format!("{path}{suffix}");
        match std::fs::remove_file(&file) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!("Failed to delete {file}: {e}"),
        }
    }
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
    /// Opens a session that does not share schemas with other sessions.
    /// Use [`PgDatabase::connect`] to open sessions that do.
    pub fn connect(db: &Arc<Database>) -> Result<Self> {
        PgDatabase::new(db.clone()).connect(ConnectOpts::new())
    }

    fn with_schemas(conn: Arc<Connection>, schemas: Arc<SharedSchemas>) -> Self {
        Self {
            inner: Arc::new(PgConnectionInner { conn, schemas }),
        }
    }

    /// Attaches an existing schema database file as schema `name`, in this
    /// session and in every other session of the database.
    pub fn attach_schema(&self, name: &str, path: &str) -> Result<()> {
        let name = name.to_lowercase();
        if !self.inner.add_schema(&name, path)? {
            return Err(schema_already_exists(&name));
        }
        Ok(())
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
    attach_schemas_of_other_sessions(pg_conn, sql)?;

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
        let state = pg_conn.session_state().lock().unwrap();
        PrepareOptions {
            unqualified_database_search_path: state.search_path.clone(),
        }
    };
    for prereq in translated.prereqs {
        let input = prereq.to_string();
        let mut stmt = pg_conn
            .conn
            .prepare_translated_stmt_with_options(prereq, &input, &options)?;
        stmt.run_ignore_rows()?;
    }

    pg_conn
        .conn
        .prepare_translated_cmd_with_options(translated.cmd, sql, &options)
}

/// Attaches the schemas that other sessions created and detaches the ones
/// they dropped, before running `sql`. A database cannot be attached or
/// detached inside a transaction, so a session inside a transaction keeps
/// its schemas until the transaction ends. If another session dropped one
/// of them, its file is deleted, and writes to it would be lost, so the
/// session can only end the transaction.
fn attach_schemas_of_other_sessions(pg_conn: &PgConnectionInner, sql: &str) -> Result<()> {
    let version = pg_conn.schemas.version.load(Ordering::Acquire);
    if pg_conn.session_state().lock().unwrap().schemas_version == version {
        return Ok(());
    }
    let mut list = pg_conn.schemas.list.lock().unwrap();
    let version = pg_conn.schemas.version.load(Ordering::Acquire);
    let mut state = pg_conn.session_state().lock().unwrap();
    let wanted = list.schemas.clone();
    let dropped: Vec<String> = state
        .attached_schemas
        .iter()
        .filter(|(name, id)| wanted.get(*name).map(|schema| schema.id) != Some(**id))
        .map(|(name, _)| name.clone())
        .collect();
    if !pg_conn.conn.get_auto_commit() {
        if let Some(name) = dropped.first() {
            if !ends_transaction(sql) {
                return Err(LimboError::TxError(format!(
                    "schema \"{name}\" was dropped by another session, roll back the transaction"
                )));
            }
        }
        return Ok(());
    }
    for name in &dropped {
        execute_sqlite_internal(&pg_conn.conn, format!("DETACH \"{name}\""))?;
        let id = state
            .attached_schemas
            .remove(name)
            .expect("a dropped schema is attached to the session");
        list.detached(id);
    }
    for (name, schema) in &wanted {
        if !state.attached_schemas.contains_key(name) {
            attach_database(&pg_conn.conn, name, &schema.path)?;
            state.attached_schemas.insert(name.clone(), schema.id);
            list.attached(schema.id);
        }
    }
    state.schemas_version = version;
    Ok(())
}

fn ends_transaction(sql: &str) -> bool {
    let Ok(parse_result) = turso_pg_parser::parse(sql) else {
        return false;
    };
    let Ok(translated) = PostgreSQLTranslator::new().translate_with_prereqs(&parse_result) else {
        return false;
    };
    matches!(
        translated.cmd.stmt(),
        ast::Stmt::Commit { .. }
            | ast::Stmt::Rollback {
                savepoint_name: None,
                ..
            }
    )
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
        handle_pg_create_schema(pg_conn, &stmt)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(stmt) = try_extract_drop_schema(&parse_result) {
        handle_pg_drop_schema(pg_conn, &stmt)?;
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

fn execute_sqlite_internal(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    let mut stmt = conn.prepare_internal(sql)?;
    stmt.run_ignore_rows()
}

fn attach_database(conn: &Arc<Connection>, name: &str, path: &str) -> Result<()> {
    execute_sqlite_internal(
        conn,
        format!("ATTACH '{}' AS \"{}\"", path.replace('\'', "''"), name),
    )
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

fn handle_pg_create_schema(pg_conn: &PgConnectionInner, stmt: &PgCreateSchemaStmt) -> Result<()> {
    let conn = &pg_conn.conn;
    let name = stmt.name.to_lowercase();
    if name == "public" {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(schema_already_exists(&name));
    }

    let path = schema_file_path(conn, &name);
    if schema_exists(conn, &name)? || !pg_conn.add_schema(&name, &path)? {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(schema_already_exists(&name));
    }
    Ok(())
}

fn schema_already_exists(name: &str) -> LimboError {
    LimboError::ParseError(format!("schema \"{name}\" already exists"))
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

fn handle_pg_drop_schema(pg_conn: &PgConnectionInner, stmt: &PgDropSchemaStmt) -> Result<()> {
    let conn = &pg_conn.conn;
    let name = stmt.name.to_lowercase();
    if name == "public" {
        return handle_pg_drop_schema_public(conn, stmt.cascade);
    }

    if !schema_exists(conn, &name)? || !pg_conn.drop_schema(&name, stmt.cascade)? {
        if stmt.if_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" does not exist"
        )));
    }
    Ok(())
}

/// With `cascade`, drops the tables of schema `name`. Runs in a write
/// transaction that also writes the user version of the schema, unchanged,
/// so that it always takes the write lock of the schema, and fails with
/// "database is busy" if another session has uncommitted writes to the
/// schema. Without the lock, those writes would be committed to the file of
/// the dropped schema.
fn empty_schema(conn: &Arc<Connection>, name: &str, cascade: bool) -> Result<()> {
    let execute = |sql: String| conn.prepare_sqlite(sql)?.run_ignore_rows();
    execute("BEGIN".to_string())?;
    let result = (|| {
        let rows = conn
            .prepare_sqlite(format!("PRAGMA \"{name}\".user_version"))?
            .run_collect_rows()?;
        let user_version = rows[0][0].to_string();
        execute(format!("PRAGMA \"{name}\".user_version = {user_version}"))?;
        if cascade {
            drop_all_tables_in_schema(conn, name)?;
        }
        execute("COMMIT".to_string())
    })();
    if result.is_err() {
        execute("ROLLBACK".to_string())?;
    }
    result
}

fn handle_pg_drop_schema_public(conn: &Arc<Connection>, cascade: bool) -> Result<()> {
    let table_names = list_user_tables(conn, None)?;
    if !cascade && !table_names.is_empty() {
        return Err(LimboError::ParseError(
            "cannot drop schema \"public\" because other objects depend on it".to_string(),
        ));
    }

    for table_name in table_names {
        let mut stmt = conn.prepare(format!("DROP TABLE \"{table_name}\""))?;
        stmt.run_ignore_rows()?;
    }
    Ok(())
}

fn drop_all_tables_in_schema(conn: &Arc<Connection>, schema_name: &str) -> Result<()> {
    for table_name in list_user_tables(conn, Some(schema_name))? {
        let mut stmt = conn.prepare(format!("DROP TABLE \"{schema_name}\".\"{table_name}\"",))?;
        stmt.run_ignore_rows()?;
    }
    Ok(())
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

fn list_user_tables(conn: &Arc<Connection>, schema_name: Option<&str>) -> Result<Vec<String>> {
    let filter = "type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%'";
    let sql = match schema_name {
        Some(name) => format!("SELECT name FROM \"{name}\".sqlite_schema WHERE {filter}"),
        None => format!("SELECT name FROM sqlite_schema WHERE {filter}"),
    };
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.first() {
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
