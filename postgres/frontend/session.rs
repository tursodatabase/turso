use std::num::NonZero;
use std::str;
use std::sync::{Arc, Mutex};

use crate::aliases;
use crate::catalog::{self, PostgresDialect};
use crate::datestyle::DateStyle;
use turso_core::{Connection, LimboError, PrepareOptions, Result, Statement, Value};
use turso_parser::ast::{self};
use turso_pg_parser::translator::{
    is_comment_on, is_refresh_matview, try_extract_copy_from, try_extract_copy_stdin,
    try_extract_copy_to_stdout, try_extract_create_schema, try_extract_create_table_like,
    try_extract_drop_schema, try_extract_multi_drop, try_extract_multi_truncate, try_extract_set,
    try_extract_show, PgCopyFromStmt, PgCreateSchemaStmt, PgCreateTableLike, PgDropSchemaStmt,
    PgSetKind, PgSetStmt, PgSetValue, PostgreSQLTranslator,
};

use crate::copy::parse_copy_text_format;

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

struct PgConnectionInner {
    conn: Arc<Connection>,
    session_state: Arc<Mutex<SessionState>>,
}

impl PgConnectionInner {
    fn set_search_path(&self, path: Vec<String>) {
        let mut state = self.session_state.lock().unwrap();
        state.search_path = path;
    }
}

#[derive(Default)]
pub(crate) struct SessionState {
    search_path: Vec<String>,
    /// Session GUCs set via `SET`, keyed by lowercased name. Values fall
    /// back to [`GUC_DEFAULTS`] when unset.
    gucs: std::collections::HashMap<String, String>,
    /// GUCs set via `SET LOCAL` or `set_config(..., is_local => true)`,
    /// layered over `gucs` until the transaction ends. The frontend has no
    /// commit hook, so readers sweep this lazily: every GUC access that can
    /// see the connection clears it once the connection is back in
    /// autocommit (see [`SessionState::clear_txn_gucs`] call sites).
    txn_gucs: std::collections::HashMap<String, String>,
    /// GUCs the client supplied in the wire StartupMessage. A layer under
    /// `gucs` rather than an insert into it because PostgreSQL treats
    /// client-sourced values as the session's reset point: RESET (and
    /// RESET ALL) restores these, not the built-in defaults, and
    /// pg_settings reports their source as "client".
    startup_gucs: std::collections::HashMap<String, String>,
}

impl SessionState {
    /// Value of a configuration parameter the way SHOW displays it, or
    /// None when the name is neither set in this session nor a built-in.
    pub(crate) fn guc_value(&self, lower_name: &str) -> Option<String> {
        if lower_name == "search_path" {
            return Some(if self.search_path.is_empty() {
                "\"$user\", public".to_string()
            } else {
                self.search_path.join(", ")
            });
        }
        if let Some(value) = self.txn_gucs.get(lower_name) {
            return Some(value.clone());
        }
        if let Some(value) = self.gucs.get(lower_name) {
            return Some(value.clone());
        }
        if let Some(value) = self.startup_gucs.get(lower_name) {
            return Some(value.clone());
        }
        guc_default(lower_name).map(|(_, default)| default.to_string())
    }

    /// Set (Some) or reset (None) a configuration parameter, returning the
    /// value SHOW displays afterwards. Mirrors the frontend's `SET`/`RESET`
    /// handling, including for `search_path`, which lives outside the GUC
    /// map.
    pub(crate) fn set_guc(&mut self, lower_name: &str, value: Option<String>) -> Result<String> {
        if lower_name == "search_path" {
            self.search_path = match value {
                Some(v) => v
                    .split(',')
                    .map(|part| part.trim().to_string())
                    .filter(|part| !part.is_empty())
                    .collect(),
                None => Vec::new(),
            };
            return Ok(self
                .guc_value(lower_name)
                .expect("search_path always resolves"));
        }
        // A plain SET or RESET takes effect immediately even after an
        // earlier SET LOCAL of the same name, the way PostgreSQL behaves.
        self.txn_gucs.remove(lower_name);
        match value {
            Some(v) => {
                let v = self.canonical_guc_value(lower_name, v)?;
                self.gucs.insert(lower_name.to_string(), v.clone());
                Ok(v)
            }
            None => {
                self.gucs.remove(lower_name);
                Ok(self.guc_value(lower_name).unwrap_or_default())
            }
        }
    }

    /// Set (Some) or reset-to-default (None) a parameter for the rest of
    /// the current transaction, returning the value SHOW displays. The
    /// session value underneath is untouched, so dropping the overlay at
    /// transaction end restores it for commit and rollback alike.
    pub(crate) fn set_local_guc(
        &mut self,
        lower_name: &str,
        value: Option<String>,
    ) -> Result<String> {
        match value {
            Some(v) => {
                let v = self.canonical_guc_value(lower_name, v)?;
                self.txn_gucs.insert(lower_name.to_string(), v.clone());
                Ok(v)
            }
            None => match guc_default(lower_name) {
                Some((_, default)) => {
                    self.txn_gucs
                        .insert(lower_name.to_string(), default.to_string());
                    Ok(default.to_string())
                }
                None => {
                    self.txn_gucs.remove(lower_name);
                    Ok(self.guc_value(lower_name).unwrap_or_default())
                }
            },
        }
    }

    /// Drop every SET LOCAL value. Called by GUC readers once the
    /// connection is back in autocommit, which is how transaction end is
    /// observed.
    pub(crate) fn clear_txn_gucs(&mut self) {
        self.txn_gucs.clear();
    }

    /// Record one client-supplied StartupMessage parameter. The live
    /// search path is seeded directly (it has no reset point to preserve);
    /// everything else becomes the session's client-sourced reset value.
    pub(crate) fn set_startup_guc(&mut self, lower_name: &str, value: String) {
        if lower_name == "search_path" {
            self.search_path = value
                .split(',')
                .map(|part| part.trim().to_string())
                .filter(|part| !part.is_empty())
                .collect();
            return;
        }
        // PostgreSQL refuses the connection outright when a startup
        // parameter is invalid. There is no error channel here, so the
        // session keeps the default for that parameter instead of taking a
        // value it would reject from SET.
        match self.canonical_guc_value(lower_name, value) {
            Ok(value) => {
                self.startup_gucs.insert(lower_name.to_string(), value);
            }
            Err(e) => tracing::warn!("ignoring startup parameter {lower_name}: {e}"),
        }
    }

    /// The session's DateStyle. Stored values are canonical, so parsing one
    /// back always succeeds.
    pub(crate) fn date_style(&self) -> DateStyle {
        self.guc_value("datestyle")
            .and_then(|value| DateStyle::parse(DateStyle::default(), &value))
            .unwrap_or_default()
    }

    /// The value a parameter displays as after being set to `value`, or
    /// PostgreSQL's error if the value is not one the parameter accepts.
    pub(crate) fn canonical_guc_value(&self, lower_name: &str, value: String) -> Result<String> {
        if lower_name == "datestyle" {
            let Some(style) = DateStyle::parse(self.date_style(), &value) else {
                return Err(LimboError::ParseError(format!(
                    "invalid value for parameter \"DateStyle\": \"{value}\""
                )));
            };
            return Ok(style.canonical());
        }
        Ok(canonicalize_guc(lower_name, value))
    }
}

/// The session settings a value's text representation depends on:
/// `extra_float_digits` for float8, `DateStyle` for dates and timestamps.
#[derive(Clone, Copy, Debug)]
pub struct TextOutputSettings {
    /// PostgreSQL's default of 1 means shortest-roundtrip float formatting.
    pub extra_float_digits: i32,
    pub date_style: DateStyle,
}

/// The session-dependent fields of one pg_settings row.
pub(crate) struct PgSettingRow {
    pub(crate) name: String,
    pub(crate) setting: String,
    pub(crate) context: &'static str,
    pub(crate) vartype: &'static str,
    pub(crate) source: &'static str,
    pub(crate) min_val: Option<&'static str>,
    pub(crate) max_val: Option<&'static str>,
    pub(crate) boot_val: Option<String>,
    pub(crate) reset_val: String,
}

impl SessionState {
    /// Snapshot for the pg_settings catalog table: every built-in with its
    /// current value and source, the live search path, and the customized
    /// (dotted) parameters this session set, sorted by name.
    pub(crate) fn settings_snapshot(&self) -> Vec<PgSettingRow> {
        let mut rows: Vec<PgSettingRow> = GUC_DEFAULTS
            .iter()
            .map(|def| {
                let session_value = self
                    .txn_gucs
                    .get(def.name)
                    .or_else(|| self.gucs.get(def.name));
                let startup_value = self.startup_gucs.get(def.name);
                PgSettingRow {
                    name: def.display_name.to_string(),
                    setting: session_value
                        .or(startup_value)
                        .cloned()
                        .unwrap_or_else(|| def.default.to_string()),
                    context: def.context,
                    vartype: def.vartype,
                    source: if session_value.is_some() {
                        "session"
                    } else if startup_value.is_some() {
                        "client"
                    } else {
                        "default"
                    },
                    min_val: def.min_val,
                    max_val: def.max_val,
                    boot_val: Some(def.default.to_string()),
                    reset_val: startup_value
                        .cloned()
                        .unwrap_or_else(|| def.default.to_string()),
                }
            })
            .collect();
        rows.push(PgSettingRow {
            name: "search_path".to_string(),
            setting: self
                .guc_value("search_path")
                .expect("search_path always resolves"),
            context: "user",
            vartype: "string",
            source: if self.search_path.is_empty() {
                "default"
            } else {
                "session"
            },
            min_val: None,
            max_val: None,
            boot_val: Some("\"$user\", public".to_string()),
            reset_val: "\"$user\", public".to_string(),
        });
        let custom_names: std::collections::BTreeSet<&String> = self
            .gucs
            .keys()
            .chain(self.txn_gucs.keys())
            .chain(self.startup_gucs.keys())
            .filter(|name| guc_default(name).is_none())
            .collect();
        for name in custom_names {
            let session_value = self.txn_gucs.get(name).or_else(|| self.gucs.get(name));
            let startup_value = self.startup_gucs.get(name);
            let value = session_value
                .or(startup_value)
                .expect("custom names come from these maps");
            // Customized (dotted) parameters exist only once set;
            // PostgreSQL presents them as user-context strings.
            rows.push(PgSettingRow {
                name: name.clone(),
                setting: value.clone(),
                context: "user",
                vartype: "string",
                source: if session_value.is_some() {
                    "session"
                } else {
                    "client"
                },
                min_val: None,
                max_val: None,
                boot_val: None,
                reset_val: startup_value.unwrap_or(value).clone(),
            });
        }
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        rows
    }
}

/// Extracts `name=value` settings from a StartupMessage `options`
/// parameter, which uses server command-line syntax: `-c name=value`
/// (space after -c optional) and `--name=value`. Tokens that are not
/// settings are skipped; a connection must not fail over an option we
/// would ignore at runtime anyway.
fn parse_startup_options(options: &str) -> Vec<(String, String)> {
    let mut settings = Vec::new();
    let mut tokens = options.split_whitespace();
    while let Some(token) = tokens.next() {
        let assignment = if token == "-c" {
            tokens.next()
        } else {
            token
                .strip_prefix("--")
                .or_else(|| token.strip_prefix("-c"))
        };
        if let Some((name, value)) = assignment.and_then(|a| a.split_once('=')) {
            settings.push((name.to_lowercase(), value.to_string()));
        }
    }
    settings
}

/// The PostgreSQL session state attached to a core connection, present on
/// every connection opened through [`PgConnection::new`]. Dialect scalar
/// functions (`current_setting`, `set_config`) reach the session's GUCs
/// through this.
pub(crate) fn session_state_of(conn: &Connection) -> Option<Arc<Mutex<SessionState>>> {
    conn.frontend_state()?
        .downcast::<Mutex<SessionState>>()
        .ok()
}

/// A built-in configuration parameter: its lowercased key, the canonical
/// display name (SHOW's column header and pg_settings preserve
/// PostgreSQL's casing), the session default, and the pg_settings
/// metadata real tools filter on.
pub(crate) struct GucDef {
    pub(crate) name: &'static str,
    pub(crate) display_name: &'static str,
    pub(crate) default: &'static str,
    pub(crate) vartype: &'static str,
    pub(crate) context: &'static str,
    pub(crate) min_val: Option<&'static str>,
    pub(crate) max_val: Option<&'static str>,
}

/// A user-settable GUC with no numeric range, the common case.
const fn guc_user(
    name: &'static str,
    display_name: &'static str,
    default: &'static str,
    vartype: &'static str,
) -> GucDef {
    GucDef {
        name,
        display_name,
        default,
        vartype,
        context: "user",
        min_val: None,
        max_val: None,
    }
}

/// Built-in GUCs with their session defaults, matching PostgreSQL's
/// metadata for each. `search_path` is special-cased against the
/// session's live search path instead.
pub(crate) const GUC_DEFAULTS: &[GucDef] = &[
    guc_user("application_name", "application_name", "", "string"),
    guc_user("client_encoding", "client_encoding", "UTF8", "string"),
    guc_user(
        "client_min_messages",
        "client_min_messages",
        "notice",
        "enum",
    ),
    guc_user("datestyle", "DateStyle", "ISO, MDY", "string"),
    GucDef {
        name: "extra_float_digits",
        display_name: "extra_float_digits",
        default: "1",
        vartype: "integer",
        context: "user",
        min_val: Some("-15"),
        max_val: Some("3"),
    },
    guc_user("intervalstyle", "IntervalStyle", "postgres", "enum"),
    GucDef {
        name: "max_index_keys",
        display_name: "max_index_keys",
        default: "32",
        vartype: "integer",
        context: "internal",
        min_val: Some("32"),
        max_val: Some("32"),
    },
    GucDef {
        name: "server_encoding",
        display_name: "server_encoding",
        default: "UTF8",
        vartype: "string",
        context: "internal",
        min_val: None,
        max_val: None,
    },
    GucDef {
        name: "server_version",
        display_name: "server_version",
        default: "17.0",
        vartype: "string",
        context: "internal",
        min_val: None,
        max_val: None,
    },
    guc_user(
        "standard_conforming_strings",
        "standard_conforming_strings",
        "on",
        "bool",
    ),
    guc_user("synchronous_commit", "synchronous_commit", "on", "enum"),
    guc_user("timezone", "TimeZone", "UTC", "string"),
    guc_user(
        "transaction_isolation",
        "transaction_isolation",
        "read committed",
        "enum",
    ),
];

/// Canonical display name and default value of a built-in GUC.
fn guc_default(lower_name: &str) -> Option<(&'static str, &'static str)> {
    GUC_DEFAULTS
        .iter()
        .find(|def| def.name == lower_name)
        .map(|def| (def.display_name, def.default))
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

/// Attaches every `turso-postgres-schema-<name>.db` file next to the main
/// database as schema `<name>`. ATTACH is per-connection state in Turso, so
/// every new session must do this to see previously created schemas.
pub fn auto_attach_schemas(conn: &PgConnection, db_file: &str) {
    if db_file == ":memory:" {
        return;
    }
    let dir = std::path::Path::new(db_file)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(schema) = name
            .strip_prefix("turso-postgres-schema-")
            .and_then(|s| s.strip_suffix(".db"))
        else {
            continue;
        };
        let path = entry.path().to_string_lossy().to_string();
        let sql = format!("ATTACH '{path}' AS \"{schema}\"");
        tracing::info!("Auto-attaching PG schema '{}' from {}", schema, path);
        if let Err(e) = conn.inner().execute(&sql) {
            tracing::warn!("Failed to attach schema '{}': {}", schema, e);
        }
    }
}

impl PgConnection {
    pub fn new(conn: Arc<Connection>) -> Self {
        aliases::install(&conn);
        // PostgreSQL always enforces foreign keys; the engine's SQLite-style
        // default is off, so every session turns it on.
        conn.set_foreign_keys_enabled(true);
        let session_state = Arc::new(Mutex::new(SessionState::default()));
        conn.set_frontend_state(session_state.clone());
        Self {
            inner: Arc::new(PgConnectionInner {
                conn,
                session_state,
            }),
        }
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.inner.conn
    }

    /// The session settings that shape how result values print in text
    /// format, read in one pass so a row's values all agree.
    pub fn text_output_settings(&self) -> TextOutputSettings {
        let mut state = self.inner.session_state.lock().unwrap();
        if self.inner.conn.get_auto_commit() {
            state.clear_txn_gucs();
        }
        TextOutputSettings {
            extra_float_digits: state
                .guc_value("extra_float_digits")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
            date_style: state.date_style(),
        }
    }

    /// Maps an engine error to its PostgreSQL SQLSTATE and wording, using
    /// this session's schema for what the message alone cannot say. `sql` is
    /// the statement that failed.
    pub fn pg_error(&self, e: &LimboError, sql: &str) -> crate::errors::PgErrorInfo {
        // A unique violation names columns in the engine and a constraint in
        // PostgreSQL, so the schema has to bridge the two.
        if let Some(violation) = crate::errors::unique_violation(e) {
            if let Some(name) = self.unique_constraint_name(&violation) {
                return crate::errors::PgErrorInfo::user_error(
                    "23505",
                    crate::errors::unique_violation_message(&name),
                );
            }
        }
        if let Some(description) = crate::errors::check_violation(e) {
            if let Some((table, name)) = self.check_constraint_name(description) {
                return crate::errors::PgErrorInfo::user_error(
                    "23514",
                    crate::errors::check_violation_message(&table, &name),
                );
            }
        }
        crate::errors::pg_error(e, sql)
    }

    /// The table and constraint name behind a failed CHECK, found by the
    /// description the engine put in the message — the constraint's name, or
    /// its expression when it has none. Returns None unless exactly one
    /// constraint in the schema describes itself that way, because naming
    /// the wrong table is worse than keeping the engine's wording.
    fn check_constraint_name(&self, description: &str) -> Option<(String, String)> {
        let schema = self.inner.conn.current_schema();
        let mut found = None;
        for (table_name, table) in schema.tables.iter() {
            let Some(table) = table.btree() else { continue };
            for check in &table.check_constraints {
                // Mirrors how the engine describes a check constraint when
                // it emits the failure (translate/emitter: the name, else
                // the expression).
                let describes_itself_as = match &check.name {
                    Some(name) => name.clone(),
                    None => format!("{}", check.expr),
                };
                if describes_itself_as != description {
                    continue;
                }
                if found.is_some() {
                    return None;
                }
                // PostgreSQL names an unnamed check after the column it
                // guards, or after the table for a table-level one.
                let name = match (&check.name, &check.column) {
                    (Some(name), _) => name.clone(),
                    (None, Some(column)) => format!("{table_name}_{column}_check"),
                    (None, None) => format!("{table_name}_check"),
                };
                found = Some((table_name.clone(), name));
            }
        }
        found
    }

    /// The name PostgreSQL would give the constraint covering exactly these
    /// columns, following the same rules `pg_constraint` uses: `<table>_pkey`
    /// for a primary key, `<table>_<columns>_key` for a unique constraint,
    /// and its own name for a standalone unique index. None when no
    /// constraint matches, which leaves the engine wording in place rather
    /// than inventing a name.
    fn unique_constraint_name(
        &self,
        violation: &crate::errors::UniqueViolation<'_>,
    ) -> Option<String> {
        let schema = self.inner.conn.current_schema();
        let table = schema.get_table(violation.table)?;
        let table = table.btree()?;
        let matches = |columns: &[&str]| -> bool {
            columns.len() == violation.columns.len()
                && columns
                    .iter()
                    .zip(&violation.columns)
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
        };

        for set in &table.unique_sets {
            let columns: Vec<&str> = set.columns.iter().map(|c| c.name.as_str()).collect();
            if !matches(&columns) {
                continue;
            }
            return Some(if set.is_primary_key {
                format!("{}_pkey", violation.table)
            } else {
                format!("{}_{}_key", violation.table, columns.join("_"))
            });
        }
        // A rowid-alias primary key (`i int PRIMARY KEY`) has no unique set.
        let pk: Vec<&str> = table
            .primary_key_columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        if !pk.is_empty() && matches(&pk) {
            return Some(format!("{}_pkey", violation.table));
        }
        // A standalone CREATE UNIQUE INDEX carries its own name, which is
        // what PostgreSQL reports for one.
        let index_name = schema
            .get_indices(violation.table)
            .filter(|index| index.unique && !index.ephemeral)
            .find(|index| {
                let columns: Vec<&str> = index.columns.iter().map(|c| c.name.as_str()).collect();
                matches(&columns)
            })
            .map(|index| index.name.clone());
        index_name
    }

    /// The value `SHOW name` would display, for a parameter name already in
    /// lower case. None for a parameter this session does not know.
    pub fn guc(&self, lower_name: &str) -> Option<String> {
        self.inner
            .session_state
            .lock()
            .unwrap()
            .guc_value(lower_name)
    }

    /// Seeds the session from the client's StartupMessage run-time
    /// parameters. Everything except the connection-establishment keys is
    /// a configuration parameter; `options` carries command-line style
    /// `-c name=value` pairs.
    pub fn init_startup_parameters<'a>(
        &self,
        params: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) {
        let mut state = self.inner.session_state.lock().unwrap();
        for (name, value) in params {
            let lower = name.to_lowercase();
            match lower.as_str() {
                "user" | "database" | "replication" => {}
                "options" => {
                    for (opt_name, opt_value) in parse_startup_options(value) {
                        state.set_startup_guc(&opt_name, opt_value);
                    }
                }
                _ => state.set_startup_guc(&lower, value.to_string()),
            }
        }
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

    /// Returns the COPY descriptor when `sql` is a `COPY ... FROM STDIN`
    /// statement, whose data arrives over the wire protocol.
    pub fn parse_copy_stdin(&self, sql: &str) -> Option<PgCopyFromStmt> {
        let parse_result = turso_pg_parser::parse(sql).ok()?;
        try_extract_copy_stdin(&parse_result)
    }

    /// Inserts the accumulated wire data of a `COPY ... FROM STDIN`. A
    /// trailing `\.` end-of-data marker is accepted and ignored.
    pub fn copy_stdin_finish(&self, stmt: &PgCopyFromStmt, data: &str) -> Result<usize> {
        let data = data
            .strip_suffix("\\.\n")
            .or_else(|| data.strip_suffix("\\."))
            .unwrap_or(data);
        copy_rows_into(&self.inner.conn, stmt, data)
    }

    /// Runs a `COPY ... TO STDOUT` statement, returning the formatted text
    /// rows to stream to the client, or None when `sql` is not one.
    pub fn copy_to_stdout(&self, sql: &str) -> Result<Option<Vec<String>>> {
        let Ok(parse_result) = turso_pg_parser::parse(sql) else {
            return Ok(None);
        };
        let Some(stmt) = try_extract_copy_to_stdout(&parse_result) else {
            return Ok(None);
        };

        let select = if stmt.has_query {
            copy_query_text(sql).ok_or_else(|| {
                LimboError::ParseError("COPY: cannot extract query text".to_string())
            })?
        } else {
            let table = stmt
                .table_name
                .as_ref()
                .expect("relation-form COPY always has a table");
            let qualified = match &stmt.schema_name {
                Some(schema) => format!("\"{schema}\".\"{table}\""),
                None => format!("\"{table}\""),
            };
            let cols = match &stmt.columns {
                Some(cols) => cols
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", "),
                None => "*".to_string(),
            };
            format!("SELECT {cols} FROM {qualified}")
        };

        let delimiter = stmt
            .delimiter
            .as_ref()
            .and_then(|d| d.chars().next())
            .unwrap_or('\t');
        let null_string = stmt.null_string.as_deref().unwrap_or("\\N");

        let mut rows = prepare_statement(&self.inner, &select)?;
        let mut lines = Vec::new();
        loop {
            match rows.step()? {
                turso_core::StepResult::Row => {
                    let row = rows.row().ok_or_else(|| {
                        LimboError::InternalError("row expected after StepResult::Row".to_string())
                    })?;
                    let fields: Vec<String> = row
                        .get_values()
                        .map(|value| match value {
                            Value::Null => null_string.to_string(),
                            other => escape_copy_field(&other.to_string()),
                        })
                        .collect();
                    lines.push(fields.join(&delimiter.to_string()));
                }
                turso_core::StepResult::Done => break,
                // The driver may yield IO mid-program; keep stepping.
                turso_core::StepResult::IO => continue,
                other => {
                    return Err(LimboError::InternalError(format!(
                        "unexpected step result during COPY: {other:?}"
                    )))
                }
            }
        }
        Ok(Some(lines))
    }
}

/// Extracts the parenthesized query text of a `COPY (query) TO STDOUT`,
/// scanning for the matching close paren outside string literals.
fn copy_query_text(sql: &str) -> Option<String> {
    let start = sql.find('(')?;
    let mut depth = 0usize;
    let mut in_string = false;
    for (i, c) in sql[start..].char_indices() {
        match c {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(sql[start + 1..start + i].trim().to_string());
                }
            }
            _ => {}
        }
    }
    None
}

/// Escapes one field for PostgreSQL's COPY text format: backslash, tab,
/// newline, and carriage return must not read back as delimiters.
fn escape_copy_field(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    for c in field.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
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

    // Autocommit means any transaction that held SET LOCAL values has
    // ended; drop them before this statement can observe them.
    if pg_conn.conn.get_auto_commit() {
        pg_conn.session_state.lock().unwrap().clear_txn_gucs();
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
    reject_catalog_dml(&translated.stmt)?;

    let options = {
        let state = pg_conn.session_state.lock().unwrap();
        let path = state.search_path.clone();
        PrepareOptions {
            unqualified_database_search_path: if path.is_empty() { None } else { Some(path) },
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
        .prepare_translated_stmt_with_options(translated.stmt, sql, &options)
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
        return Ok(Some(handle_pg_show(pg_conn, &show_stmt.name)?));
    }

    if let Some(stmt) = try_extract_create_table_like(&parse_result) {
        return Ok(Some(handle_pg_create_table_like(pg_conn, &stmt)?));
    }

    // DROP a, b, c and TRUNCATE a, b, c used to act on the first object
    // only; expand them into one statement per object, atomically.
    let multi =
        try_extract_multi_drop(&parse_result).or_else(|| try_extract_multi_truncate(&parse_result));
    if let Some(multi) = multi {
        handle_pg_multi_statement(pg_conn, &multi.statements)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
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

fn execute_sqlite_internal(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    let mut stmt = conn.prepare_internal(sql)?;
    stmt.run_ignore_rows()
}

fn handle_pg_set(pg_conn: &Arc<PgConnectionInner>, set_stmt: &PgSetStmt) -> Result<Statement> {
    let name = set_stmt.name.to_lowercase();
    if set_stmt.local {
        return handle_pg_set_local(pg_conn, set_stmt, &name);
    }
    match set_stmt.kind {
        PgSetKind::Value if name == "search_path" => {
            let path = set_stmt
                .values
                .iter()
                .map(|value| value.as_search_path_name().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| LimboError::ParseError("incorrect format".to_string()))?;
            pg_conn.set_search_path(path);
        }
        PgSetKind::Value => {
            if set_stmt.values.is_empty() {
                return Err(LimboError::ParseError(format!(
                    "SET {}: no value provided",
                    set_stmt.name
                )));
            }
            // Multi-part values display comma-separated, the way SHOW
            // renders them (SET datestyle = ISO, MDY -> "ISO, MDY").
            let value = set_stmt
                .values
                .iter()
                .map(guc_value_text)
                .collect::<Vec<_>>()
                .join(", ");
            let mut state = pg_conn.session_state.lock().unwrap();
            state.set_guc(&name, Some(value))?;
        }
        PgSetKind::Reset => {
            let mut state = pg_conn.session_state.lock().unwrap();
            state.set_guc(&name, None)?;
        }
        PgSetKind::ResetAll => {
            let mut state = pg_conn.session_state.lock().unwrap();
            state.gucs.clear();
        }
    }
    noop_statement(&pg_conn.conn)
}

/// SET LOCAL: the setting lasts until the end of the current transaction.
/// Outside a transaction block PostgreSQL warns and applies nothing, so
/// this is a no-op there. The live search path has no transaction-local
/// overlay, so SET LOCAL search_path errors instead of silently acting
/// session-wide.
fn handle_pg_set_local(
    pg_conn: &Arc<PgConnectionInner>,
    set_stmt: &PgSetStmt,
    lower_name: &str,
) -> Result<Statement> {
    if lower_name == "search_path" {
        return Err(LimboError::ParseError(
            "SET LOCAL search_path is not supported".to_string(),
        ));
    }
    if !pg_conn.conn.get_auto_commit() {
        let mut state = pg_conn.session_state.lock().unwrap();
        match set_stmt.kind {
            PgSetKind::Value => {
                if set_stmt.values.is_empty() {
                    return Err(LimboError::ParseError(format!(
                        "SET {}: no value provided",
                        set_stmt.name
                    )));
                }
                let value = set_stmt
                    .values
                    .iter()
                    .map(guc_value_text)
                    .collect::<Vec<_>>()
                    .join(", ");
                state.set_local_guc(lower_name, Some(value))?;
            }
            PgSetKind::Reset => {
                state.set_local_guc(lower_name, None)?;
            }
            // The grammar has no LOCAL form of RESET ALL.
            PgSetKind::ResetAll => {
                state.clear_txn_gucs();
            }
        }
    }
    noop_statement(&pg_conn.conn)
}

/// GUCs whose values SHOW displays in canonical PostgreSQL form. Booleans
/// display as on/off however they were spelled. DateStyle needs the current
/// setting to canonicalize against, so it lives in
/// [`SessionState::canonical_guc_value`] instead.
fn canonicalize_guc(lower_name: &str, value: String) -> String {
    const BOOL_GUCS: &[&str] = &["standard_conforming_strings", "synchronous_commit"];
    if BOOL_GUCS.contains(&lower_name) {
        return match value.to_lowercase().as_str() {
            "on" | "true" | "yes" | "1" => "on".to_string(),
            "off" | "false" | "no" | "0" => "off".to_string(),
            _ => value,
        };
    }
    // IntervalStyle values (postgres, postgres_verbose, sql_standard,
    // iso_8601) display lowercase, which they already are.
    value
}

/// How a SET argument reads back through SHOW.
fn guc_value_text(value: &PgSetValue) -> String {
    match value {
        PgSetValue::Identifier(s) | PgSetValue::StringLiteral(s) | PgSetValue::Number(s) => {
            s.clone()
        }
        PgSetValue::Bool(true) => "on".to_string(),
        PgSetValue::Bool(false) => "off".to_string(),
        PgSetValue::RawSql(s) => s.clone(),
        PgSetValue::Null => String::new(),
    }
}

/// Resolves a GUC for SHOW: session value, then built-in default. Unknown
/// parameters error the way PostgreSQL does.
fn handle_pg_show(pg_conn: &Arc<PgConnectionInner>, name: &str) -> Result<Statement> {
    let lower = name.to_lowercase();
    let state = pg_conn.session_state.lock().unwrap();
    let Some(value) = state.guc_value(&lower) else {
        return Err(LimboError::ParseError(format!(
            "unrecognized configuration parameter \"{lower}\""
        )));
    };
    drop(state);
    let column = if lower == "search_path" {
        "search_path"
    } else {
        guc_default(&lower)
            .map(|(canonical, _)| canonical)
            .unwrap_or(&lower)
    };
    let sql = format!(
        "SELECT '{}' AS \"{}\"",
        value.replace('\'', "''"),
        column.replace('"', "\"\"")
    );
    pg_conn.conn.prepare(&sql)
}

fn handle_pg_create_schema(conn: &Arc<Connection>, stmt: &PgCreateSchemaStmt) -> Result<()> {
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

    let path = schema_file_path(conn, &name);
    execute_sqlite_internal(
        conn,
        format!("ATTACH '{}' AS \"{}\"", path.replace('\'', "''"), name),
    )?;
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
    if name == "public" {
        return handle_pg_drop_schema_public(conn, stmt.cascade);
    }

    if !schema_exists(conn, &name)? {
        if stmt.if_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" does not exist"
        )));
    }

    if stmt.cascade {
        drop_all_tables_in_schema(conn, &name)?;
    }

    execute_sqlite_internal(conn, format!("DETACH \"{name}\""))?;
    Ok(())
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
    copy_rows_into(conn, stmt, &data)
}

/// Parses COPY text data and inserts the rows — the shared tail of the
/// file-based and wire-protocol (STDIN) COPY FROM paths.
fn copy_rows_into(conn: &Arc<Connection>, stmt: &PgCopyFromStmt, data: &str) -> Result<usize> {
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

    let mut rows = parse_copy_text_format(data, delimiter, null_string, num_columns)?;
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

/// Executes the expanded statements of a multi-object DROP or TRUNCATE.
/// PostgreSQL treats the original as one statement, so when the session is
/// not already inside a transaction the parts run in one of our own and a
/// failure undoes the statements that already ran.
fn handle_pg_multi_statement(
    pg_conn: &Arc<PgConnectionInner>,
    statements: &[String],
) -> Result<()> {
    let conn = &pg_conn.conn;
    // Inside an explicit transaction BEGIN would error; the outer
    // transaction then provides the rollback scope.
    let own_txn = conn
        .prepare_sqlite("BEGIN")
        .and_then(|mut stmt| stmt.run_ignore_rows())
        .is_ok();

    let result = (|| {
        for sql in statements {
            let mut stmt = prepare_statement(pg_conn, sql)?;
            stmt.run_ignore_rows()?;
        }
        Ok(())
    })();

    if own_txn {
        let end = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
        if let Ok(mut stmt) = conn.prepare_sqlite(end) {
            let _ = stmt.run_ignore_rows();
        }
    }
    result
}

/// Expands `CREATE TABLE name (LIKE source)` using the source table's live
/// schema. PostgreSQL's bare LIKE copies column names, types, and not-null
/// constraints; the INCLUDING forms (defaults, constraints, indexes) are
/// not supported and fail rather than silently copying less than asked.
fn handle_pg_create_table_like(
    pg_conn: &Arc<PgConnectionInner>,
    stmt: &PgCreateTableLike,
) -> Result<Statement> {
    if stmt.has_options {
        return Err(LimboError::ParseError(
            "CREATE TABLE ... (LIKE ... INCLUDING ...) is not supported".to_string(),
        ));
    }

    let table_info_sql = match &stmt.source_schema {
        Some(schema) => format!("PRAGMA \"{schema}\".table_info('{}')", stmt.source_table),
        None => format!("PRAGMA table_info('{}')", stmt.source_table),
    };
    let mut info = pg_conn.conn.prepare_internal(&table_info_sql)?;
    let rows = info.run_collect_rows()?;
    if rows.is_empty() {
        return Err(LimboError::ParseError(format!(
            "no such table: {}",
            stmt.source_table
        )));
    }

    let mut columns = Vec::new();
    for row in rows {
        let Some(Value::Text(name)) = row.get(1) else {
            continue;
        };
        let column_type = match row.get(2) {
            Some(Value::Text(t)) if !t.as_str().is_empty() => format!(" {}", t.as_str()),
            _ => String::new(),
        };
        let not_null = match row.get(3) {
            Some(value) if value.as_int() == Some(1) => " NOT NULL",
            _ => "",
        };
        columns.push(format!("\"{}\"{column_type}{not_null}", name.as_str()));
    }

    let qualified = match &stmt.schema_name {
        Some(schema) => format!("\"{schema}\".\"{}\"", stmt.table_name),
        None => format!("\"{}\"", stmt.table_name),
    };
    let if_not_exists = if stmt.if_not_exists {
        "IF NOT EXISTS "
    } else {
        ""
    };
    let create_sql = format!(
        "CREATE TABLE {if_not_exists}{qualified} ({})",
        columns.join(", ")
    );
    pg_conn.conn.prepare(&create_sql)
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
