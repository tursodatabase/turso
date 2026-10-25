use chumsky::{error::EmptyErr, prelude::*};
use std::borrow::Cow;
use std::num::NonZero;
use std::str;
use std::sync::Arc;

use crate::catalog::{self, PostgresDialect};
use crate::error::sqlstate_error;
use crate::functions::{find_parameter, ConfigParameter, Parameter};
use crate::settings::{ConnectionSettings, PgSessionState, SettingScope};
use turso_core::schema::{Table, SEQ_BACKING_TABLE_PREFIX};
use turso_core::{Connection, LimboError, Qualifier, Result, Statement, Value};
use turso_parser::ast::{self};
use turso_pg_parser::foreign_key_tables;
use turso_pg_parser::sqlstate::{
    SQLSTATE_ACTIVE_SQL_TRANSACTION, SQLSTATE_DEPENDENT_OBJECTS_STILL_EXIST,
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INSUFFICIENT_PRIVILEGE, SQLSTATE_INVALID_NAME,
    SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_INVALID_SCHEMA_NAME,
    SQLSTATE_OBJECT_NOT_IN_PREREQUISITE_STATE, SQLSTATE_RESERVED_NAME, SQLSTATE_WRONG_OBJECT_TYPE,
};
use turso_pg_parser::translator::{
    check_set_transaction, is_comment_on, is_discard_all, is_drop_matview, is_refresh_matview,
    is_reset_all, try_extract_copy_from, try_extract_create_schema, try_extract_drop_schema,
    try_extract_set, try_extract_show, unnamed_index_name, unqualified_create_table_name_location,
    PgCopyFromStmt, PgCreateSchemaStmt, PgDropSchemaStmt, PgSetStmt, PgSetTo, PgSetValue,
    PgShowStmt, PostgreSQLTranslator,
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
    let is_new = turso_core::is_memory_like(path) || io.open_file(path, flags, true)?.size()? == 0;
    if !is_new {
        refuse_a_file_with_tables_in_no_schema(&io, path, flags, opts)?;
    }
    let file = io.open_file(path, flags, true)?;
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    let db = turso_core::Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(PostgresDialect))
            .storage(db_file)
            .flags(flags)
            .db_opts(opts.with_mvcc(true)),
    )?;
    if is_new {
        set_up_new_database(&db)?;
    }
    Ok(db)
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

/// Sets up a database that was just made: creates the schema `public`, as
/// PostgreSQL does when a database is created. `public` is a stored
/// namespace like any other schema, so once dropped it stays dropped, and
/// the root namespace holds only the engine's own tables. A read-only
/// database is left as it is.
pub fn set_up_new_database(db: &Arc<turso_core::Database>) -> crate::Result<()> {
    let conn = db.connect(turso_core::ConnectOpts::new())?;
    if conn.qualifier("public").is_some() || conn.is_readonly(turso_core::MAIN_DB_ID) {
        return Ok(());
    }
    match conn.create_namespace("public") {
        Ok(_) => Ok(()),
        Err(_) if conn.qualifier("public").is_some() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

impl PgConnection {
    pub fn connect(db: &Arc<turso_core::Database>) -> crate::Result<Self> {
        let conn = db.connect_with_hooks(Arc::new(PgSessionState::default()))?;
        refuse_tables_in_no_schema(&conn)?;
        set_search_path(&conn, None, SettingScope::Session)?;
        Ok(Self {
            inner: Arc::new(PgConnectionInner { conn }),
        })
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.inner.conn
    }

    pub fn prepare(&self, sql: impl AsRef<str>) -> crate::Result<crate::Statement> {
        prepare_statement(&self.inner, sql.as_ref()).map_err(Into::into)
    }

    pub fn prepare_copy_to_stdout(
        &self,
        spec: &turso_pg_parser::copy::CopyToStdout,
    ) -> crate::Result<crate::Statement> {
        crate::copy::prepare_copy_to_stdout(&self.inner.conn, spec)
            .map(crate::Statement::new)
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
/// database instead. A database that has a schema, or none and no tables,
/// is opened as it is: a dropped `public` is not recreated.
fn refuse_tables_in_no_schema(conn: &Arc<Connection>) -> Result<()> {
    if conn.qualifier("public").is_some() {
        return Ok(());
    }
    let schema = conn.current_schema();
    if schema.namespaces().next().is_some() {
        return Ok(());
    }
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

fn prepare_statement(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<crate::Statement> {
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
    let (sql, parse_result) = name_foreign_key_tables(&pg_conn.conn, sql, parse_result)?;
    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(&parse_result)
        .map_err(|e| translation_error(&pg_conn.conn, e))?;
    reject_catalog_dml(translated.cmd.stmt())?;
    if let ast::Stmt::DropView { view_name, .. } = translated.cmd.stmt() {
        reject_drop_of_other_view_kind(&pg_conn.conn, view_name, is_drop_matview(&parse_result))?;
    }

    if translated.prereqs.is_empty() {
        return pg_conn
            .conn
            .prepare_translated_cmd(translated.cmd, &sql)
            .map_err(schema_error)
            .map(crate::Statement::new);
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
    let pg_conn = Arc::clone(pg_conn);
    let prereqs = translated.prereqs;
    let sql_owned = sql.into_owned();
    Ok(crate::Statement::with_deferred(
        inner,
        Box::new(move |inner| {
            execute_cmd_with_prereqs(&pg_conn, &prereqs, &cmd, &sql_owned, inner)
        }),
    ))
}

const PG_STMT_SAVEPOINT: &str = "__turso_pg_stmt";

fn execute_cmd_with_prereqs(
    pg_conn: &Arc<PgConnectionInner>,
    prereqs: &[ast::Stmt],
    cmd: &ast::Cmd,
    sql: &str,
    inner: &mut Statement,
) -> Result<()> {
    let conn = &pg_conn.conn;
    if create_table_if_not_exists_target_present(conn, cmd)? {
        *inner = conn
            .prepare_translated_cmd(cmd.clone(), sql)
            .map_err(schema_error)?;
        return inner.run_ignore_rows();
    }

    let started_in_autocommit = conn.get_auto_commit();
    let begin_sp = format!("SAVEPOINT \"{PG_STMT_SAVEPOINT}\"");
    let release_sp = format!("RELEASE SAVEPOINT \"{PG_STMT_SAVEPOINT}\"");
    execute_sqlite(conn, begin_sp)?;
    let result = (|| {
        for prereq in prereqs {
            let input = prereq.to_string();
            let mut stmt = conn
                .prepare_translated_stmt(prereq.clone(), &input)
                .map_err(schema_error)?;
            stmt.run_ignore_rows()?;
        }
        *inner = conn
            .prepare_translated_cmd(cmd.clone(), sql)
            .map_err(schema_error)?;
        inner.run_ignore_rows()
    })();

    match result {
        Ok(()) => execute_sqlite(conn, release_sp),
        Err(err) => {
            if started_in_autocommit {
                let _ = execute_sqlite(conn, "ROLLBACK");
            } else {
                let rollback_sp = format!("ROLLBACK TO SAVEPOINT \"{PG_STMT_SAVEPOINT}\"");
                let _ = execute_sqlite(conn, rollback_sp);
                let _ = execute_sqlite(conn, release_sp);
            }
            Err(err)
        }
    }
}

fn create_table_if_not_exists_target_present(
    conn: &Arc<Connection>,
    cmd: &ast::Cmd,
) -> Result<bool> {
    let ast::Cmd::Stmt(ast::Stmt::CreateTable {
        if_not_exists: true,
        tbl_name,
        ..
    }) = cmd
    else {
        return Ok(false);
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
        return Ok(false);
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
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .first()
        .and_then(|row| row.first())
        .is_some_and(|val| *val == Value::from_i64(1)))
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

/// Keeps the connection's internal `__turso_internal_*` bookkeeping tables
/// visible to schema scans (PRAGMA show_internal_schema) for as long as it
/// lives, then restores the previous setting. The Postgres layer needs them
/// visible to find sequence backing tables while the ordinary scans hide them.
struct InternalSchemaVisible<'a> {
    conn: &'a Arc<Connection>,
    previous: bool,
}

impl<'a> InternalSchemaVisible<'a> {
    fn new(conn: &'a Arc<Connection>) -> Self {
        let previous = conn.get_show_internal_schema();
        conn.set_show_internal_schema(true);
        Self { conn, previous }
    }
}

impl Drop for InternalSchemaVisible<'_> {
    fn drop(&mut self) {
        self.conn.set_show_internal_schema(self.previous);
    }
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
    // A chosen name also collides with the `__turso_internal_seq_*` backing
    // table of a sequence, which is hidden from ordinary schema scans.
    let _internal = InternalSchemaVisible::new(conn);
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
    sql: &str,
) -> Result<Option<crate::Statement>> {
    let parse_result = match turso_pg_parser::parse(sql) {
        Ok(result) => result,
        Err(_) => return Ok(None),
    };

    if let Some(set_stmt) = try_extract_set(&parse_result) {
        let stmt = handle_pg_set(pg_conn, &set_stmt)?;
        return Ok(Some(crate::Statement::new(stmt)));
    }

    if check_set_transaction(&parse_result).map_err(|e| translation_error(&pg_conn.conn, e))? {
        return Ok(Some(crate::Statement::new(noop_statement(&pg_conn.conn)?)));
    }

    if let Some(show_stmt) = try_extract_show(&parse_result) {
        let stmt = handle_pg_show(pg_conn, &show_stmt)?;
        return Ok(Some(crate::Statement::new(stmt)));
    }

    if is_reset_all(&parse_result) {
        let inner = noop_statement(&pg_conn.conn)?;
        let conn = Arc::clone(&pg_conn.conn);
        return Ok(Some(crate::Statement::with_deferred(
            inner,
            Box::new(move |_inner| reset_all_settings(&conn)),
        )));
    }

    if is_discard_all(&parse_result) {
        let inner = noop_statement(&pg_conn.conn)?;
        let conn = Arc::clone(&pg_conn.conn);
        return Ok(Some(crate::Statement::with_deferred(
            inner,
            Box::new(move |_inner| discard_all(&conn)),
        )));
    }

    if let Some(stmt) =
        try_extract_create_schema(&parse_result).map_err(|e| translation_error(&pg_conn.conn, e))?
    {
        let inner = noop_statement(&pg_conn.conn)?;
        let pg_conn = Arc::clone(pg_conn);
        return Ok(Some(crate::Statement::with_deferred(
            inner,
            Box::new(move |_inner| handle_pg_create_schema(&pg_conn, &stmt)),
        )));
    }

    if let Some(stmt) = try_extract_drop_schema(&parse_result) {
        let inner = noop_statement(&pg_conn.conn)?;
        let conn = Arc::clone(&pg_conn.conn);
        return Ok(Some(crate::Statement::with_deferred(
            inner,
            Box::new(move |_inner| handle_pg_drop_schema(&conn, &stmt)),
        )));
    }

    if is_refresh_matview(&parse_result).map_err(|e| translation_error(&pg_conn.conn, e))? {
        return Ok(Some(crate::Statement::new(noop_statement(&pg_conn.conn)?)));
    }

    if is_comment_on(&parse_result) {
        return Ok(Some(crate::Statement::new(noop_statement(&pg_conn.conn)?)));
    }

    if let Some(stmt) =
        try_extract_copy_from(&parse_result).map_err(|e| translation_error(&pg_conn.conn, e))?
    {
        let inner = noop_statement(&pg_conn.conn)?;
        let conn = Arc::clone(&pg_conn.conn);
        return Ok(Some(crate::Statement::with_deferred(
            inner,
            Box::new(move |inner| {
                let rows_inserted = handle_pg_copy_from(&conn, &stmt)?;
                inner.set_n_change(rows_inserted as i64);
                Ok(())
            }),
        )));
    }

    Ok(None)
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
) -> Result<Value> {
    let mut path = parse_search_path(value.unwrap_or(DEFAULT_SEARCH_PATH))?;
    path.retain(|name| name != "$user" && !is_core_database_name(name));
    session_state(conn)?.mutate(conn, |settings| {
        settings.set_search_path(Some(path.clone()), scope);
        settings.set(SEARCH_PATH_SETTING, value.map(str::to_owned), scope);
    });
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
    conn.context::<PgSessionState>()?
        .setting(SEARCH_PATH_SETTING)
}

pub(crate) fn set_custom_parameter(
    conn: &Connection,
    name: &str,
    value: Option<&str>,
    scope: SettingScope,
) -> Result<Value> {
    if !is_valid_custom_parameter_name(name) {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_NAME,
            format!("invalid configuration parameter name \"{name}\""),
        ));
    }
    session_state(conn)?.mutate(conn, |settings| {
        settings.set(name, value.map(str::to_owned), scope)
    });
    Ok(Value::build_text(value.unwrap_or_default().to_owned()))
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

fn handle_pg_create_schema(
    pg_conn: &Arc<PgConnectionInner>,
    stmt: &PgCreateSchemaStmt,
) -> Result<()> {
    let conn = &pg_conn.conn;
    let name = stmt.name.to_lowercase();
    if conn.qualifier(&name).is_some() {
        if stmt.if_not_exists {
            return Ok(());
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

    conn.create_namespace(&name)?;
    if stmt.elements.is_empty() {
        return Ok(());
    }
    let setting = search_path_setting(conn);
    let mut search_path = turso_pg_parser::quote_identifier(&name);
    let rest = setting.as_deref().unwrap_or(DEFAULT_SEARCH_PATH).trim();
    if !rest.is_empty() {
        search_path = format!("{search_path}, {rest}");
    }
    set_search_path(conn, Some(&search_path), SettingScope::Session)?;
    let result = stmt.elements.iter().try_for_each(|sql| {
        prepare_statement(pg_conn, sql)?
            .run_ignore_rows()
            .map_err(crate::Error::into_core_error)
    });
    set_search_path(conn, setting.as_deref(), SettingScope::Session)?;
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

fn handle_pg_drop_schema(conn: &Arc<Connection>, stmt: &PgDropSchemaStmt) -> Result<()> {
    let name = stmt.name.to_lowercase();
    if !schema_exists(conn, &name) {
        if stmt.if_exists {
            return Ok(());
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

    without_foreign_keys(conn, || {
        for object in &objects {
            execute_sqlite(conn, object.drop_sql(&name))?;
        }
        Ok(())
    })?;
    conn.drop_namespace(&name)
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

#[cfg(test)]
mod tests {
    use super::{open_database_with_io, search_path_parser, PgConnection};
    use crate::session::set_up_new_database;
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
        set_up_new_database(&db).unwrap();
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
        set_up_new_database(&db).unwrap();
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
        set_up_new_database(&db).unwrap();
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
            set_up_new_database(&db).unwrap();
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
}
