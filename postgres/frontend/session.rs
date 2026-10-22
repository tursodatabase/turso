use chumsky::{error::EmptyErr, prelude::*};
use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZero;
use std::str;
use std::sync::Arc;

use crate::catalog::{self, PostgresDialect};
use crate::functions::ConfigParameter;
use crate::settings::{PgSessionState, SettingScope};
use turso_core::{Connection, LimboError, Result, Statement, Value};
use turso_parser::ast::{self};
use turso_pg_parser::translator::{
    begin_access_mode, check_set_transaction, is_begin, is_comment_on, is_commit_or_rollback,
    is_drop_matview, is_refresh_matview, set_transaction_access_mode, try_extract_copy_from,
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

impl PgConnection {
    pub fn connect(db: &Arc<turso_core::Database>) -> crate::Result<Self> {
        let conn = db.connect_with_hooks(Arc::new(PgSessionState::default()))?;
        conn.add_namespace("public", turso_core::MAIN_DB_ID);
        set_search_path(&conn, None, SettingScope::Session)?;
        Ok(Self {
            inner: Arc::new(PgConnectionInner { conn }),
        })
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.inner.conn
    }

    pub fn prepare(&self, sql: impl AsRef<str>) -> crate::Result<crate::Statement> {
        prepare_pg_statement(&self.inner, sql.as_ref())
    }

    pub fn prepare_for_describe(&self, sql: impl AsRef<str>) -> crate::Result<crate::Statement> {
        let pg_conn = &self.inner;
        let sql = sql.as_ref().trim();
        if let Ok(parse_result) = turso_pg_parser::parse(sql) {
            let has_state_change = is_begin(&parse_result)
                || is_commit_or_rollback(&parse_result)
                || check_set_transaction(&parse_result).unwrap_or(false);
            if has_state_change {
                return noop_statement(&pg_conn.conn)
                    .map(crate::Statement::new)
                    .map_err(Into::into);
            }
        }
        prepare_statement(pg_conn, sql).map_err(Into::into)
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
        Some(prepare_pg_statement(self.conn, sql).map(Some))
    }
}

fn prepare_pg_statement(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
) -> crate::Result<crate::Statement> {
    let sql = sql.trim();
    if let Ok(parse_result) = turso_pg_parser::parse(sql) {
        if is_commit_or_rollback(&parse_result) {
            return prepare_tx_terminal(pg_conn, sql, &parse_result);
        }
        if is_begin(&parse_result) {
            return prepare_begin(pg_conn, sql, &parse_result);
        }
    }
    prepare_statement(pg_conn, sql).map_err(Into::into)
}

pub fn split_statements(sql: &str) -> crate::Result<Vec<String>> {
    match turso_pg_parser::split_statements(sql) {
        Ok(stmts) if stmts.is_empty() && !sql.trim().is_empty() => Ok(vec![sql.trim().to_string()]),
        Ok(stmts) => Ok(stmts),
        Err(_) => Ok(vec![sql.trim().to_string()]),
    }
}

fn prepare_tx_terminal(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    parse_result: &turso_pg_parser::ParseResult,
) -> crate::Result<crate::Statement> {
    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    debug_assert!(
        translated.prereqs.is_empty(),
        "COMMIT/ROLLBACK should never produce prerequisite statements"
    );
    let inner = pg_conn
        .conn
        .prepare_translated_cmd(translated.cmd, sql)
        .map_err(crate::Error::from)?;
    let post_run = std::sync::Arc::new(move |result: crate::Result<()>| match result {
        Err(ref e) if is_no_active_tx_error(e) => Ok(()),
        other => other,
    });
    Ok(crate::Statement::new_with_post_run(inner, post_run))
}

fn prepare_begin(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    parse_result: &turso_pg_parser::ParseResult,
) -> crate::Result<crate::Statement> {
    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    debug_assert!(
        translated.prereqs.is_empty(),
        "BEGIN should never produce prerequisite statements"
    );
    let inner = pg_conn
        .conn
        .prepare_translated_cmd(translated.cmd, sql)
        .map_err(crate::Error::from)?;
    // For a non-nested BEGIN: the VDBE sets tx_read_only when the instruction
    // executes, so post_run has nothing to do on success.
    //
    // For a nested BEGIN (PostgreSQL warns but keeps the transaction): the VDBE
    // errors with "cannot start a transaction within a transaction" and never
    // runs, so we apply the access mode directly here.
    let access_mode = begin_access_mode(parse_result);
    let conn = pg_conn.conn.clone();
    let post_run = std::sync::Arc::new(move |result: crate::Result<()>| match result {
        Ok(()) => Ok(()),
        Err(ref e) if is_nested_begin_error(e) => {
            if let Some(ro) = access_mode {
                apply_tx_access_mode(&conn, ro)?;
            }
            Ok(())
        }
        other => other,
    });
    Ok(crate::Statement::new_with_post_run(inner, post_run))
}

fn apply_tx_access_mode(conn: &Arc<Connection>, read_only: bool) -> crate::Result<()> {
    if !read_only && conn.is_tx_read_only() && conn.tx_has_started() {
        return Err(crate::Error::read_write_mode_after_first_query());
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
    if let ast::Stmt::DropView { view_name, .. } = translated.cmd.stmt() {
        reject_drop_of_other_view_kind(&pg_conn.conn, view_name, is_drop_matview(&parse_result))?;
    }

    if translated.prereqs.is_empty() {
        let inner = pg_conn.conn.prepare_translated_cmd(translated.cmd, &sql)?;
        let conn = pg_conn.conn.clone();
        let post_run = std::sync::Arc::new(move |result: crate::Result<()>| {
            if !conn.get_auto_commit() {
                conn.set_tx_has_started(true);
            }
            result
        });
        return Ok(crate::Statement::new_with_post_run(inner, post_run));
    }

    // Validate prerequisites and the target command during prepare without
    // executing any writes. Side-effecting prerequisites run atomically with
    // the command only when the returned Statement is actually executed.
    for prereq in &translated.prereqs {
        let input = prereq.to_string();
        let _ = pg_conn
            .conn
            .prepare_translated_stmt(prereq.clone(), &input)?;
    }
    let cmd = translated.cmd;
    let inner = pg_conn.conn.prepare_translated_cmd(cmd.clone(), &sql)?;
    let pg_conn = Arc::clone(pg_conn);
    let prereqs = translated.prereqs;
    let sql_owned = sql.into_owned();
    Ok(crate::Statement::with_deferred(
        inner,
        Box::new(move |inner| {
            let result = execute_cmd_with_prereqs(&pg_conn, &prereqs, &cmd, &sql_owned, inner);
            if !pg_conn.conn.get_auto_commit() {
                pg_conn.conn.set_tx_has_started(true);
            }
            result
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
        *inner = conn.prepare_translated_cmd(cmd.clone(), sql)?;
        return inner.run_ignore_rows();
    }

    let started_in_autocommit = conn.get_auto_commit();
    let begin_sp = format!("SAVEPOINT \"{PG_STMT_SAVEPOINT}\"");
    let release_sp = format!("RELEASE SAVEPOINT \"{PG_STMT_SAVEPOINT}\"");
    execute_sqlite(conn, begin_sp)?;
    let result = (|| {
        for prereq in prereqs {
            let input = prereq.to_string();
            let mut stmt = conn.prepare_translated_stmt(prereq.clone(), &input)?;
            stmt.run_ignore_rows()?;
        }
        *inner = conn.prepare_translated_cmd(cmd.clone(), sql)?;
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
    let Some(database) = relation_lookup_databases(conn, schema).into_iter().next() else {
        return Ok(false);
    };
    let mut stmt = conn.prepare_sqlite(format!(
        "SELECT EXISTS (SELECT 1 FROM \"{database}\".sqlite_schema \
         WHERE type IN ('table', 'view') AND name COLLATE NOCASE = ?1)"
    ))?;
    stmt.bind_at(
        NonZero::new(1).unwrap(),
        Value::build_text(tbl_name.name.as_str().to_string()),
    )?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .first()
        .and_then(|row| row.first())
        .is_some_and(|val| *val == Value::from_i64(1)))
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
/// new index on the table. Main when no database has the table.
fn relation_name_is_taken(
    conn: &Arc<Connection>,
    schema: Option<&str>,
    table: &str,
    name: &str,
) -> Result<bool> {
    let mut databases = relation_lookup_databases(conn, schema);
    if schema.is_none() {
        databases.push("main".to_string());
    }
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
    for database in relation_lookup_databases(conn, schema) {
        let mut stmt = conn.prepare_sqlite(format!(
            "SELECT CASE WHEN type = 'table' THEN 'table' \
                         WHEN rootpage = 0 THEN 'view' \
                         ELSE 'materialized view' END \
             FROM \"{database}\".sqlite_schema \
             WHERE type IN ('table', 'view') AND name COLLATE NOCASE = ?1"
        ))?;
        stmt.bind_at(
            NonZero::new(1).unwrap(),
            Value::build_text(view_name.name.as_str().to_string()),
        )?;
        let Some(row) = stmt.run_collect_rows()?.into_iter().next() else {
            continue;
        };
        if row[0] != Value::build_text(expected_kind) {
            return Err(LimboError::ParseError(format!(
                "\"{}\" is not a {expected_kind}",
                view_name.name.as_str()
            )));
        }
        return Ok(());
    }
    Ok(())
}

/// The databases that a relation is looked up in, in order. A relation
/// named with a schema is in that schema. Otherwise it is looked up in the
/// same order as core does: temp first, then the databases of the search
/// path.
fn relation_lookup_databases(conn: &Arc<Connection>, schema: Option<&str>) -> Vec<String> {
    let database_names: HashMap<usize, String> = conn
        .list_all_databases()
        .into_iter()
        .map(|(id, database, _)| (id, database))
        .collect();
    match schema {
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
            .map(|id| database_names[&id].clone())
            .collect(),
    }
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

    if check_set_transaction(&parse_result).map_err(|e| LimboError::ParseError(e.to_string()))? {
        return Ok(Some(prepare_set_transaction(pg_conn, &parse_result)?));
    }

    if let Some(show_stmt) = try_extract_show(&parse_result) {
        let stmt = handle_pg_show(pg_conn, &show_stmt)?;
        return Ok(Some(crate::Statement::new(stmt)));
    }

    if let Some(stmt) = try_extract_create_schema(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?
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

    if is_refresh_matview(&parse_result).map_err(|e| LimboError::ParseError(e.to_string()))? {
        return Ok(Some(crate::Statement::new(noop_statement(&pg_conn.conn)?)));
    }

    if is_comment_on(&parse_result) {
        return Ok(Some(crate::Statement::new(noop_statement(&pg_conn.conn)?)));
    }

    if let Some(stmt) =
        try_extract_copy_from(&parse_result).map_err(|e| LimboError::ParseError(e.to_string()))?
    {
        let inner = noop_statement(&pg_conn.conn)?;
        let conn = Arc::clone(&pg_conn.conn);
        return Ok(Some(crate::Statement::with_deferred(
            inner,
            Box::new(move |inner| {
                let rows_inserted = handle_pg_copy_from(&conn, &stmt)?;
                if !conn.get_auto_commit() {
                    conn.set_tx_has_started(true);
                }
                inner.set_n_change(rows_inserted as i64);
                Ok(())
            }),
        )));
    }

    Ok(None)
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
    let post_run = std::sync::Arc::new(move |result: crate::Result<()>| {
        result?;
        if conn.get_auto_commit() {
            return Ok(());
        }
        if let Some(read_only) = access_mode {
            apply_tx_access_mode(&conn, read_only)?;
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
        return Err(LimboError::InvalidArgument(format!(
            "invalid configuration parameter name \"{name}\""
        )));
    }
    session_state(conn)?.mutate(conn, |settings| {
        settings.set(&name.to_ascii_lowercase(), value.map(str::to_owned), scope)
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
    use super::{search_path_parser, PgConnection};
    use crate::PostgresDialect;
    use chumsky::Parser;
    use std::sync::Arc;
    use turso_core::{Database, DatabaseOpts, MemoryIO, OpenOptions};

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
            assert_eq!(
                conn.execute(sql).unwrap_err().to_string(),
                format!("Parse error: {error}"),
                "{sql}"
            );
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
    fn prepare_for_describe_does_not_change_tx_read_only() {
        let (io, db) = super::open_database(
            ":memory:",
            None,
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
        )
        .unwrap();
        drop(io);
        let conn = super::PgConnection::connect(&db).unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "BEGIN READ ONLY must set tx read-only mode"
        );
        let _stmt = conn.prepare_for_describe("COMMIT").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "prepare_for_describe(COMMIT) must not clear tx read-only mode"
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
