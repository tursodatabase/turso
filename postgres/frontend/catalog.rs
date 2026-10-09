use crate::functions::validate_pg_input;
use rustc_hash::FxHashMap as HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use turso_core::{
    native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule},
    schema::{is_system_table, BTreeTable, Schema, Sequence, Table, View},
    Connection, Dialect, Func, IOResult, LimboError, OpenOptions, Result, Statement, Value,
};
use turso_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabKind};
use turso_parser::ast::{self, RefAct};
use turso_pg_parser::quote_identifier;

pub use turso_pg_parser::translator::is_catalog_table_name;

/// Starting OID for user tables (matches PostgreSQL convention)
const USER_TABLE_OID_START: i64 = 16384;
const PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX: &str = "sqlite_autoindex_";
const STORED_PG_SCHEMA_PREFIX: &str = "/* turso_frontend:postgres */ ";

#[derive(Debug)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn parse(&self, sql: &str) -> Result<(Option<turso_parser::ast::Cmd>, usize)> {
        // Engine-generated helper statements and pragmas are canonical SQLite
        // text that pg_query rejects, so anything the PostgreSQL parser cannot
        // handle falls back to SQLite parsing.
        let Ok(parse_result) = turso_pg_parser::parse(sql) else {
            return turso_core::dialect::sqlite::parse(sql);
        };
        let stmts = &parse_result.protobuf.stmts;
        if stmts.is_empty() {
            return Ok((None, sql.len()));
        }
        // The translator consumes the first statement only; report how many
        // input bytes it covers so multi-statement iteration can resume after
        // it. pg_query records where the next statement starts, which also
        // accounts for the semicolon and any trailing whitespace in between.
        let consumed = match stmts.get(1) {
            Some(next) => next.stmt_location as usize,
            None => sql.len(),
        };
        let translator = turso_pg_parser::translator::PostgreSQLTranslator::new();
        match translator.translate(&parse_result) {
            Ok(stmt) => Ok((Some(turso_parser::ast::Cmd::Stmt(stmt)), consumed)),
            Err(_) => turso_core::dialect::sqlite::parse(sql),
        }
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        // Schema rows written by internal SQLite paths (e.g. sqlite_sequence)
        // carry no frontend marker and are plain SQLite SQL.
        let Some(raw_sql) = decode_stored_pg_schema_sql(sql) else {
            return BTreeTable::from_sql(sql, root_page);
        };

        let parse_result =
            turso_pg_parser::parse(raw_sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
        let translator = turso_pg_parser::translator::PostgreSQLTranslator::new();
        let stmt = translator
            .translate(&parse_result)
            .map_err(|e| LimboError::ParseError(e.to_string()))?;
        match stmt {
            turso_parser::ast::Stmt::CreateTable { tbl_name, body, .. } => {
                BTreeTable::from_create_table_ast(&tbl_name, &body, root_page)
            }
            _ => Err(LimboError::ParseError(
                "expected CREATE TABLE statement".to_string(),
            )),
        }
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<turso_parser::ast::Stmt> {
        // Schema rows written by internal SQLite paths (e.g. sqlite_sequence)
        // carry no frontend marker and are plain SQLite SQL.
        let Some(raw_sql) = decode_stored_pg_schema_sql(sql) else {
            return turso_core::dialect::sqlite::parse_table_sql_ast(sql);
        };

        let parse_result =
            turso_pg_parser::parse(raw_sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
        let translator = turso_pg_parser::translator::PostgreSQLTranslator::new();
        let stmt = translator
            .translate(&parse_result)
            .map_err(|e| LimboError::ParseError(e.to_string()))?;
        match stmt {
            stmt @ turso_parser::ast::Stmt::CreateTable { .. } => Ok(stmt),
            _ => Err(LimboError::ParseError(
                "expected CREATE TABLE statement".to_string(),
            )),
        }
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        let Some(raw_sql) = decode_stored_pg_schema_sql(sql) else {
            return turso_core::dialect::sqlite::table_sql_for_replay(sql);
        };

        let stmt = self.parse_table_sql_ast(sql)?;
        let turso_parser::ast::Stmt::CreateTable {
            mut tbl_name,
            temporary,
            if_not_exists,
            body,
        } = stmt
        else {
            unreachable!("parse_table_sql_ast returned a non-CREATE TABLE statement");
        };

        // Unqualified statements replay as the original PostgreSQL DDL. A
        // schema-qualified statement targets an attached database the replay
        // destination does not have, so re-render the translated AST without
        // the qualifier; the canonical text round-trips through the SQLite
        // fallback in `Dialect::parse`.
        if tbl_name.db_name.take().is_none() {
            return Ok(raw_sql.to_string());
        }

        Ok(turso_parser::ast::Stmt::CreateTable {
            tbl_name,
            temporary,
            if_not_exists,
            body,
        }
        .to_string())
    }

    fn format_table_sql(
        &self,
        input: &str,
        _tbl_name: &turso_parser::ast::QualifiedName,
        _body: &turso_parser::ast::CreateTableBody,
    ) -> Result<String> {
        Ok(encode_pg_schema_sql(input))
    }

    fn format_rewritten_table_sql(&self, stmt: &ast::Stmt) -> Result<String> {
        if !matches!(stmt, ast::Stmt::CreateTable { .. }) {
            return Err(LimboError::InternalError(
                "format_rewritten_table_sql requires CREATE TABLE".to_string(),
            ));
        }
        Ok(stmt.to_string())
    }

    fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
        turso_core::dialect::sqlite::register_builtin_catalog(schema, enable_custom_types)
    }

    fn register_native_extensions(&self, options: OpenOptions) -> OpenOptions {
        register_catalog_modules(crate::functions::register_functions(options))
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        turso_core::dialect::sqlite::resolve_builtin_function(name, arg_count)
    }

    fn requires_custom_types(&self) -> bool {
        true
    }
}

pub fn encode_pg_schema_sql(sql: &str) -> String {
    format!("{STORED_PG_SCHEMA_PREFIX}{sql}")
}

pub fn decode_stored_pg_schema_sql(sql: &str) -> Option<&str> {
    sql.strip_prefix(STORED_PG_SCHEMA_PREFIX)
}

/// Map a SQLite type string to a PostgreSQL type OID.
/// Strips parenthesized parameters (e.g. `varchar(100)` -> `VARCHAR`) before matching.
fn sqlite_type_to_pg_oid(ty_str: &str) -> i64 {
    let base = match ty_str.find('(') {
        Some(pos) => &ty_str[..pos],
        None => ty_str,
    };
    match base.to_uppercase().as_str() {
        "INTEGER" | "INT" | "INT4" => 23,
        "SMALLINT" | "INT2" => 21,
        "BIGINT" | "INT8" => 20,
        "TINYINT" | "MEDIUMINT" => 23,
        "TEXT" => 25,
        "VARCHAR" | "CHAR" | "CLOB" | "NCHAR" | "NVARCHAR" | "CHARACTER VARYING" => 1043,
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" | "FLOAT8" => 701,
        "FLOAT4" => 700,
        "BLOB" | "BYTEA" => 17,
        "NUMERIC" | "DECIMAL" => 1700,
        "BOOLEAN" | "BOOL" => 16,
        "UUID" => 2950,
        "JSON" => 114,
        "JSONB" => 3802,
        "DATE" => 1082,
        "TIME" => 1083,
        "TIMESTAMP" => 1114,
        "TIMESTAMPTZ" => 1184,
        "INET" => 869,
        "CIDR" => 650,
        "MACADDR" => 829,
        "OID" => 26,
        _ => 25, // default to text
    }
}

/// Convert a RefAct to its PostgreSQL single-character representation.
fn ref_act_to_char(act: &RefAct) -> &'static str {
    match act {
        RefAct::NoAction => "a",
        RefAct::Restrict => "r",
        RefAct::Cascade => "c",
        RefAct::SetNull => "n",
        RefAct::SetDefault => "d",
    }
}

/// Virtual table implementation for pg_catalog.pg_class
/// Maps SQLite's sqlite_master to PostgreSQL's pg_class system table
#[derive(Debug)]
struct PgClassTable;

impl SnapshotRows for PgClassTable {
    // PostgreSQL pg_class columns (simplified subset)
    const SCHEMA: &'static str = "CREATE TABLE pg_class (
            oid INTEGER,
            relname TEXT,
            relnamespace INTEGER,
            reltype INTEGER,
            reloftype INTEGER,
            relowner INTEGER,
            relam INTEGER,
            relfilenode INTEGER,
            reltablespace INTEGER,
            relpages INTEGER,
            reltuples REAL,
            relallvisible INTEGER,
            reltoastrelid INTEGER,
            relhasindex BOOLEAN,
            relisshared BOOLEAN,
            relpersistence TEXT,
            relkind TEXT,
            relnatts INTEGER,
            relchecks INTEGER,
            relhasrules BOOLEAN,
            relhastriggers BOOLEAN,
            relhassubclass BOOLEAN,
            relrowsecurity BOOLEAN,
            relforcerowsecurity BOOLEAN,
            relispopulated BOOLEAN,
            relreplident TEXT,
            relispartition BOOLEAN,
            relrewrite INTEGER,
            relfrozenxid INTEGER,
            relminmxid INTEGER,
            relacl TEXT,
            reloptions TEXT,
            relpartbound TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(1259);
    const ESTIMATED_COST: f64 = 1000.0;
    const ESTIMATED_ROWS: u32 = 100;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        let relations = catalog_relations(conn);
        let indexes = catalog_indexes(&relations);

        for relation in &relations {
            let (kind, relnatts, relchecks, access_method) = match &relation.kind {
                CatalogRelationKind::Table(table) => {
                    ("r", table.columns().len(), table.check_constraints.len(), 2)
                }
                CatalogRelationKind::View(view) => ("v", view.columns.len(), 0, 0),
                CatalogRelationKind::Sequence(_) => ("S", 3, 0, 0),
            };
            let relhasindex = if indexes.iter().any(|index| index.table_oid == relation.oid) {
                1i64
            } else {
                0
            };

            rows.push(vec![
                Value::from_i64(relation.oid),
                Value::build_text(relation.name.clone()),
                Value::from_i64(relation.namespace_oid),
                Value::from_i64(0),  // reltype
                Value::from_i64(0),  // reloftype
                Value::from_i64(10), // relowner
                Value::from_i64(access_method),
                Value::from_i64(0),           // relfilenode
                Value::from_i64(0),           // reltablespace
                Value::from_i64(1),           // relpages
                Value::from_f64(0.0),         // reltuples
                Value::from_i64(0),           // relallvisible
                Value::from_i64(0),           // reltoastrelid
                Value::from_i64(relhasindex), // relhasindex
                Value::from_i64(0),           // relisshared
                Value::Text("p".into()),      // relpersistence (permanent)
                Value::build_text(kind),
                Value::from_i64(relnatts as i64),
                Value::from_i64(relchecks as i64),
                Value::from_i64(0),      // relhasrules
                Value::from_i64(i64::from(matches!(&relation.kind, CatalogRelationKind::Table(table) if !table.foreign_keys.is_empty()))),
                Value::from_i64(0),      // relhassubclass
                Value::from_i64(0),      // relrowsecurity
                Value::from_i64(0),      // relforcerowsecurity
                Value::from_i64(1),      // relispopulated
                Value::Text("d".into()), // relreplident
                Value::from_i64(0),      // relispartition
                Value::from_i64(0),      // relrewrite
                Value::from_i64(0),      // relfrozenxid
                Value::from_i64(0),      // relminmxid
                Value::Null,             // relacl
                Value::Null,             // reloptions
                Value::Null,             // relpartbound
            ]);
        }

        // Add index rows (relkind='i')
        for index in indexes {
            let indnatts = index.columns.len() as i64;
            rows.push(vec![
                Value::from_i64(index.oid),     // oid
                Value::Text(index.name.into()), // relname
                Value::from_i64(index.namespace_oid),
                Value::from_i64(0),        // reltype
                Value::from_i64(0),        // reloftype
                Value::from_i64(10),       // relowner
                Value::from_i64(403),      // relam (btree)
                Value::from_i64(0),        // relfilenode
                Value::from_i64(0),        // reltablespace
                Value::from_i64(1),        // relpages
                Value::from_f64(0.0),      // reltuples
                Value::from_i64(0),        // relallvisible
                Value::from_i64(0),        // reltoastrelid
                Value::from_i64(0),        // relhasindex
                Value::from_i64(0),        // relisshared
                Value::Text("p".into()),   // relpersistence
                Value::Text("i".into()),   // relkind (index)
                Value::from_i64(indnatts), // relnatts
                Value::from_i64(0),        // relchecks
                Value::from_i64(0),        // relhasrules
                Value::from_i64(0),        // relhastriggers
                Value::from_i64(0),        // relhassubclass
                Value::from_i64(0),        // relrowsecurity
                Value::from_i64(0),        // relforcerowsecurity
                Value::from_i64(1),        // relispopulated
                Value::Text("d".into()),   // relreplident
                Value::from_i64(0),        // relispartition
                Value::from_i64(0),        // relrewrite
                Value::from_i64(0),        // relfrozenxid
                Value::from_i64(0),        // relminmxid
                Value::Null,               // relacl
                Value::Null,               // reloptions
                Value::Null,               // relpartbound
            ]);
        }

        rows
    }
}

/// Virtual table implementation for pg_catalog.pg_namespace
/// Maps schema information to PostgreSQL's pg_namespace
#[derive(Debug)]
struct PgNamespaceTable;

impl SnapshotRows for PgNamespaceTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_namespace (
            oid INTEGER,
            nspname TEXT,
            nspowner INTEGER,
            nspacl TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(2615);
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 5;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        // PostgreSQL standard namespaces
        let mut rows = vec![
            vec![
                Value::from_i64(11),              // oid
                Value::Text("pg_catalog".into()), // nspname
                Value::from_i64(10),              // nspowner
                Value::Null,                      // nspacl
            ],
            vec![
                Value::from_i64(2200),        // oid
                Value::Text("public".into()), // nspname
                Value::from_i64(10),          // nspowner
                Value::Null,                  // nspacl
            ],
            vec![
                Value::from_i64(11394),                   // oid
                Value::Text("information_schema".into()), // nspname
                Value::from_i64(10),                      // nspowner
                Value::Null,                              // nspacl
            ],
        ];

        // Add attached schemas (CREATE SCHEMA creates attached databases)
        let mut schema_names = conn.attached_database_names();
        schema_names.sort();
        let mut oid = 16384i64;
        for name in schema_names {
            rows.push(vec![
                Value::from_i64(oid),
                Value::build_text(name),
                Value::from_i64(10), // nspowner (bootstrap superuser)
                Value::Null,         // nspacl
            ]);
            oid += 1;
        }
        rows
    }
}

/// Virtual table implementation for pg_catalog.pg_attribute
/// Maps column information to PostgreSQL's pg_attribute
#[derive(Debug)]
struct PgAttributeTable;

impl SnapshotRows for PgAttributeTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_attribute (
            attrelid INTEGER,
            attname TEXT,
            atttypid INTEGER,
            attstattarget INTEGER,
            attlen INTEGER,
            attnum INTEGER,
            attndims INTEGER,
            attcacheoff INTEGER,
            atttypmod INTEGER,
            attbyval BOOLEAN,
            attstorage TEXT,
            attalign TEXT,
            attnotnull BOOLEAN,
            atthasdef BOOLEAN,
            atthasmissing BOOLEAN,
            attidentity TEXT,
            attgenerated TEXT,
            attisdropped BOOLEAN,
            attislocal BOOLEAN,
            attinhcount INTEGER,
            attcollation INTEGER,
            attacl TEXT,
            attoptions TEXT,
            attfdwoptions TEXT,
            attmissingval TEXT,
            attcompression TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(1249);
    const ESTIMATED_COST: f64 = 1000.0;
    const ESTIMATED_ROWS: u32 = 1000;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();

        for relation in catalog_relations(conn) {
            let columns = match &relation.kind {
                CatalogRelationKind::Table(table) => table.columns(),
                CatalogRelationKind::View(view) => &view.columns,
                CatalogRelationKind::Sequence(_) => continue,
            };
            for (i, col) in columns.iter().enumerate() {
                let col_name = col.name.clone().unwrap_or_default();
                let type_oid = sqlite_type_to_pg_oid(&col.ty_str);
                let type_info = PG_BASE_TYPES
                    .iter()
                    .find(|t| t.oid == type_oid)
                    .expect("column type OIDs refer to PostgreSQL base types");
                let array = col.array_dimensions() > 0;
                let type_oid = if array { type_info.typarray } else { type_oid };
                let attnum = (i + 1) as i64; // 1-based
                let primary_key = match &relation.kind {
                    CatalogRelationKind::Table(table) => table
                        .primary_key_columns
                        .iter()
                        .any(|(name, _)| col.name.as_ref() == Some(name)),
                    _ => false,
                };
                let notnull = i64::from(col.notnull() || primary_key);
                let has_def = if col.default.is_some() { 1i64 } else { 0i64 };

                rows.push(vec![
                    Value::from_i64(relation.oid),
                    Value::Text(col_name.into()), // attname
                    Value::from_i64(type_oid),    // atttypid
                    Value::from_i64(-1),          // attstattarget
                    Value::from_i64(if array { -1 } else { type_info.typlen }),
                    Value::from_i64(attnum),                        // attnum
                    Value::from_i64(col.array_dimensions() as i64), // attndims
                    Value::from_i64(-1),                            // attcacheoff
                    Value::from_i64(-1),                            // atttypmod
                    Value::from_i64(i64::from(!array && type_info.typbyval)),
                    Value::build_text(if array { "x" } else { type_info.typstorage }),
                    Value::build_text(if array { "i" } else { type_info.typalign }),
                    Value::from_i64(notnull), // attnotnull
                    Value::from_i64(has_def), // atthasdef
                    Value::from_i64(0),       // atthasmissing
                    Value::Text("".into()),   // attidentity
                    Value::Text("".into()),   // attgenerated
                    Value::from_i64(0),       // attisdropped
                    Value::from_i64(1),       // attislocal
                    Value::from_i64(0),       // attinhcount
                    Value::from_i64(0),       // attcollation
                    Value::Null,              // attacl
                    Value::Null,              // attoptions
                    Value::Null,              // attfdwoptions
                    Value::Null,              // attmissingval
                    Value::build_text(""),
                ]);
            }
        }

        rows
    }
}

/// Virtual table implementation for pg_catalog.pg_roles
/// Stub: returns a single hardcoded "turso" superuser role.
/// TODO: replace with real role data when authentication is implemented.
#[derive(Debug)]
struct PgRolesTable;

impl SnapshotRows for PgRolesTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_roles (
            oid INTEGER,
            rolname TEXT,
            rolsuper INTEGER,
            rolinherit INTEGER,
            rolcreaterole INTEGER,
            rolcreatedb INTEGER,
            rolcanlogin INTEGER,
            rolreplication INTEGER,
            rolconnlimit INTEGER,
            rolpassword TEXT,
            rolvaliduntil TEXT,
            rolbypassrls INTEGER,
            rolconfig TEXT
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 1;

    /// Stub: returns a single default superuser role.
    /// Replace this method with real role lookup when auth is implemented.
    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        vec![vec![
            Value::from_i64(10),        // oid
            Value::build_text("turso"), // rolname
            Value::from_i64(1),         // rolsuper
            Value::from_i64(1),         // rolinherit
            Value::from_i64(1),         // rolcreaterole
            Value::from_i64(1),         // rolcreatedb
            Value::from_i64(1),         // rolcanlogin
            Value::from_i64(1),         // rolreplication
            Value::from_i64(-1),        // rolconnlimit (-1 = no limit)
            Value::Null,                // rolpassword (never exposed)
            Value::Null,                // rolvaliduntil
            Value::from_i64(1),         // rolbypassrls
            Value::Null,                // rolconfig
        ]]
    }
}

/// Virtual table implementation for pg_catalog.pg_proc
/// Populated from the same function registry as PRAGMA function_list.
#[derive(Debug)]
struct PgProcTable;

impl SnapshotRows for PgProcTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_proc (
            oid INTEGER,
            proname TEXT,
            pronamespace INTEGER,
            proowner INTEGER,
            prolang INTEGER,
            procost REAL,
            prorows REAL,
            provariadic INTEGER,
            prokind TEXT,
            prosecdef BOOLEAN,
            proleakproof BOOLEAN,
            proisstrict BOOLEAN,
            proretset BOOLEAN,
            provolatile TEXT,
            proparallel TEXT,
            pronargs INTEGER,
            pronargdefaults INTEGER,
            prorettype INTEGER,
            proargtypes TEXT,
            proallargtypes TEXT,
            proargmodes TEXT,
            proargnames TEXT,
            proargdefaults TEXT,
            protrftypes TEXT,
            prosrc TEXT,
            probin TEXT,
            prosqlbody TEXT,
            proconfig TEXT,
            proacl TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(1255);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 100;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        use crate::Func;

        let mut rows = Vec::new();
        let mut oid = 1i64;

        // Built-in functions from the same registry as PRAGMA function_list
        for entry in Func::builtin_function_list() {
            let prokind = match entry.func_type {
                "a" => "a", // aggregate
                "w" => "w", // window
                _ => "f",   // function
            };
            let provolatile = if entry.deterministic { "i" } else { "v" };

            rows.push(vec![
                Value::from_i64(oid),               // oid
                Value::build_text(entry.name),      // proname
                Value::from_i64(11),                // pronamespace (pg_catalog)
                Value::from_i64(10),                // proowner
                Value::from_i64(12),                // prolang (internal)
                Value::from_f64(1.0),               // procost
                Value::from_f64(0.0),               // prorows
                Value::from_i64(0),                 // provariadic
                Value::build_text(prokind),         // prokind
                Value::from_i64(0),                 // prosecdef
                Value::from_i64(0),                 // proleakproof
                Value::from_i64(0),                 // proisstrict
                Value::from_i64(0),                 // proretset
                Value::build_text(provolatile),     // provolatile
                Value::build_text("u"),             // proparallel (unsafe)
                Value::from_i64(entry.narg as i64), // pronargs
                Value::from_i64(0),                 // pronargdefaults
                Value::from_i64(0),                 // prorettype
                Value::Null,                        // proargtypes
                Value::Null,                        // proallargtypes
                Value::Null,                        // proargmodes
                Value::Null,                        // proargnames
                Value::Null,                        // proargdefaults
                Value::Null,                        // protrftypes
                Value::Null,                        // prosrc
                Value::Null,                        // probin
                Value::Null,                        // prosqlbody
                Value::Null,                        // proconfig
                Value::Null,                        // proacl
            ]);
            oid += 1;
        }

        // Extension functions
        for (name, is_agg, argc, deterministic) in conn.get_syms_functions() {
            let prokind = if is_agg { "a" } else { "f" };
            let provolatile = if deterministic { "i" } else { "v" };

            rows.push(vec![
                Value::from_i64(oid),       // oid
                Value::build_text(name),    // proname
                Value::from_i64(11),        // pronamespace (pg_catalog)
                Value::from_i64(10),        // proowner
                Value::from_i64(13),        // prolang (C)
                Value::from_f64(1.0),       // procost
                Value::from_f64(0.0),       // prorows
                Value::from_i64(0),         // provariadic
                Value::build_text(prokind), // prokind
                Value::from_i64(0),         // prosecdef
                Value::from_i64(0),         // proleakproof
                Value::from_i64(0),         // proisstrict
                Value::from_i64(0),         // proretset
                Value::build_text(provolatile),
                Value::build_text("u"),       // proparallel
                Value::from_i64(argc as i64), // pronargs
                Value::from_i64(0),           // pronargdefaults
                Value::from_i64(0),           // prorettype
                Value::Null,                  // proargtypes
                Value::Null,                  // proallargtypes
                Value::Null,                  // proargmodes
                Value::Null,                  // proargnames
                Value::Null,                  // proargdefaults
                Value::Null,                  // protrftypes
                Value::Null,                  // prosrc
                Value::Null,                  // probin
                Value::Null,                  // prosqlbody
                Value::Null,                  // proconfig
                Value::Null,                  // proacl
            ]);
            oid += 1;
        }
        rows
    }
}

/// Virtual table implementation for pg_catalog.pg_database
/// Returns one row per database, deriving the name from the database file path.
#[derive(Debug)]
struct PgDatabaseTable;

impl SnapshotRows for PgDatabaseTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_database (
            oid INTEGER,
            datname TEXT,
            datdba INTEGER,
            encoding INTEGER,
            datlocprovider TEXT,
            datistemplate BOOLEAN,
            datallowconn BOOLEAN,
            datconnlimit INTEGER,
            datfrozenxid INTEGER,
            datminmxid INTEGER,
            dattablespace INTEGER,
            datcollate TEXT,
            datctype TEXT,
            daticulocale TEXT,
            daticurules TEXT,
            datacl TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(1262);
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 1;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let db_name = db_name_from_path(conn.db_file_path());
        vec![vec![
            Value::from_i64(16384),           // oid
            Value::build_text(db_name),       // datname
            Value::from_i64(10),              // datdba (bootstrap superuser OID)
            Value::from_i64(6),               // encoding (UTF8)
            Value::build_text("c"),           // datlocprovider (libc)
            Value::from_i64(0),               // datistemplate
            Value::from_i64(1),               // datallowconn
            Value::from_i64(-1),              // datconnlimit (unlimited)
            Value::from_i64(0),               // datfrozenxid
            Value::from_i64(0),               // datminmxid
            Value::from_i64(1663),            // dattablespace (pg_default)
            Value::build_text("en_US.UTF-8"), // datcollate
            Value::build_text("en_US.UTF-8"), // datctype
            Value::Null,                      // daticulocale
            Value::Null,                      // daticurules
            Value::Null,                      // datacl
        ]]
    }
}

/// Shared by the pg_database virtual table and current_database() so both
/// report the same database name.
pub(crate) fn db_name_from_path(path: &str) -> String {
    std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
        .to_string()
}

/// Virtual table implementation for pg_catalog.pg_am
/// Stub: returns two access methods (heap and btree).
#[derive(Debug)]
struct PgAmTable;

impl SnapshotRows for PgAmTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_am (
            oid INTEGER,
            amname TEXT,
            amhandler TEXT,
            amtype TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(2601);
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 2;

    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        vec![
            vec![
                Value::from_i64(2),                        // oid
                Value::build_text("heap"),                 // amname
                Value::build_text("heap_tableam_handler"), // amhandler
                Value::build_text("t"),                    // amtype (table)
            ],
            vec![
                Value::from_i64(403),           // oid
                Value::build_text("btree"),     // amname
                Value::build_text("bthandler"), // amhandler
                Value::build_text("i"),         // amtype (index)
            ],
        ]
    }
}

/// Generic empty PG catalog table — always returns no rows.
/// Used for catalog tables psql queries but we don't yet need real data for.
#[derive(Debug, Clone)]
struct EmptyPgCatalogTable {
    create_sql: String,
}

impl VirtualTableModule for EmptyPgCatalogTable {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok(self.create_sql.clone())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(self.clone())
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for EmptyPgCatalogTable {
    type Cursor = EmptyPgCatalogCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(EmptyPgCatalogCursor)
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let constraint_usages = constraints
            .iter()
            .map(|_| turso_ext::ConstraintUsage {
                argv_index: None,
                omit: false,
            })
            .collect();
        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 10.0,
            estimated_rows: 0,
            constraint_usages,
        })
    }
}

struct EmptyPgCatalogCursor;

impl VirtualTableCursor for EmptyPgCatalogCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        Ok(IOResult::Done(false))
    }
    fn rowid(&self) -> i64 {
        0
    }
    fn column(&mut self, _column: usize) -> turso_core::types::IOResultOr<Value> {
        Ok(IOResult::Done(Value::Null))
    }
    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        Ok(IOResult::Done(false))
    }
}

/// Virtual table implementation for pg_tables
/// Maps user tables to PostgreSQL's pg_tables view
#[derive(Debug)]
struct PgTablesTable;

impl SnapshotRows for PgTablesTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_tables (
            schemaname TEXT,
            tablename TEXT,
            tableowner TEXT,
            tablespace TEXT,
            hasindexes INTEGER,
            hasrules INTEGER,
            hastriggers INTEGER,
            rowsecurity INTEGER
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 1000.0;
    const ESTIMATED_ROWS: u32 = 100;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        let relations = catalog_relations(conn);
        let indexes = catalog_indexes(&relations);
        for relation in relations {
            if !matches!(relation.kind, CatalogRelationKind::Table(_)) {
                continue;
            }
            rows.push(vec![
                Value::build_text(relation.namespace),
                Value::build_text(relation.name),
                Value::Text("turso".into()), // tableowner
                Value::Null,                 // tablespace
                Value::from_i64(i64::from(
                    indexes.iter().any(|index| index.table_oid == relation.oid),
                )),
                Value::from_i64(0), // hasrules
                Value::from_i64(0), // hastriggers
                Value::from_i64(0), // rowsecurity
            ]);
        }

        rows
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_type
// ──────────────────────────────────────────────────────────────────────

struct PgTypeInfo {
    oid: i64,
    name: &'static str,
    typtype: &'static str,
    typcategory: &'static str,
    typlen: i64,
    typarray: i64,
    typelem: i64,
    typbyval: bool,
    typalign: &'static str,
    typstorage: &'static str,
}

const PG_BASE_TYPES: &[PgTypeInfo] = &[
    PgTypeInfo {
        oid: 16,
        name: "bool",
        typtype: "b",
        typcategory: "B",
        typlen: 1,
        typarray: 1000,
        typelem: 0,
        typbyval: true,
        typalign: "c",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 17,
        name: "bytea",
        typtype: "b",
        typcategory: "U",
        typlen: -1,
        typarray: 1001,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
    PgTypeInfo {
        oid: 20,
        name: "int8",
        typtype: "b",
        typcategory: "N",
        typlen: 8,
        typarray: 1016,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 21,
        name: "int2",
        typtype: "b",
        typcategory: "N",
        typlen: 2,
        typarray: 1005,
        typelem: 0,
        typbyval: true,
        typalign: "s",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 23,
        name: "int4",
        typtype: "b",
        typcategory: "N",
        typlen: 4,
        typarray: 1007,
        typelem: 0,
        typbyval: true,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 25,
        name: "text",
        typtype: "b",
        typcategory: "S",
        typlen: -1,
        typarray: 1009,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
    PgTypeInfo {
        oid: 26,
        name: "oid",
        typtype: "b",
        typcategory: "N",
        typlen: 4,
        typarray: 1028,
        typelem: 0,
        typbyval: true,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 114,
        name: "json",
        typtype: "b",
        typcategory: "U",
        typlen: -1,
        typarray: 199,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
    PgTypeInfo {
        oid: 650,
        name: "cidr",
        typtype: "b",
        typcategory: "I",
        typlen: -1,
        typarray: 651,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "m",
    },
    PgTypeInfo {
        oid: 700,
        name: "float4",
        typtype: "b",
        typcategory: "N",
        typlen: 4,
        typarray: 1021,
        typelem: 0,
        typbyval: true,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 701,
        name: "float8",
        typtype: "b",
        typcategory: "N",
        typlen: 8,
        typarray: 1022,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 829,
        name: "macaddr",
        typtype: "b",
        typcategory: "U",
        typlen: 6,
        typarray: 1040,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 869,
        name: "inet",
        typtype: "b",
        typcategory: "I",
        typlen: -1,
        typarray: 1041,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "m",
    },
    PgTypeInfo {
        oid: 1043,
        name: "varchar",
        typtype: "b",
        typcategory: "S",
        typlen: -1,
        typarray: 1015,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
    PgTypeInfo {
        oid: 1082,
        name: "date",
        typtype: "b",
        typcategory: "D",
        typlen: 4,
        typarray: 1182,
        typelem: 0,
        typbyval: true,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 1083,
        name: "time",
        typtype: "b",
        typcategory: "D",
        typlen: 8,
        typarray: 1183,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 1114,
        name: "timestamp",
        typtype: "b",
        typcategory: "D",
        typlen: 8,
        typarray: 1115,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 1184,
        name: "timestamptz",
        typtype: "b",
        typcategory: "D",
        typlen: 8,
        typarray: 1185,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 1700,
        name: "numeric",
        typtype: "b",
        typcategory: "N",
        typlen: -1,
        typarray: 1231,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "m",
    },
    PgTypeInfo {
        oid: 2950,
        name: "uuid",
        typtype: "b",
        typcategory: "U",
        typlen: 16,
        typarray: 2951,
        typelem: 0,
        typbyval: false,
        typalign: "c",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 3802,
        name: "jsonb",
        typtype: "b",
        typcategory: "U",
        typlen: -1,
        typarray: 3807,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
];

/// Static PG array type definitions:
/// (oid, name, typelem)
const PG_ARRAY_TYPES: &[(i64, &str, i64)] = &[
    (199, "_json", 114),
    (651, "_cidr", 650),
    (1000, "_bool", 16),
    (1001, "_bytea", 17),
    (1005, "_int2", 21),
    (1007, "_int4", 23),
    (1009, "_text", 25),
    (1015, "_varchar", 1043),
    (1016, "_int8", 20),
    (1021, "_float4", 700),
    (1022, "_float8", 701),
    (1028, "_oid", 26),
    (1040, "_macaddr", 829),
    (1041, "_inet", 869),
    (1115, "_timestamp", 1114),
    (1182, "_date", 1082),
    (1183, "_time", 1083),
    (1185, "_timestamptz", 1184),
    (1231, "_numeric", 1700),
    (2951, "_uuid", 2950),
    (3807, "_jsonb", 3802),
];

const PG_TYPE_SQL: &str = "CREATE TABLE pg_type (oid INTEGER, typname TEXT, typnamespace INTEGER, typowner INTEGER, typlen INTEGER, typbyval BOOLEAN, typtype TEXT, typcategory TEXT, typispreferred BOOLEAN, typisdefined BOOLEAN, typdelim TEXT, typrelid INTEGER, typsubscript TEXT, typelem INTEGER, typarray INTEGER, typinput TEXT, typoutput TEXT, typreceive TEXT, typsend TEXT, typmodin TEXT, typmodout TEXT, typanalyze TEXT, typalign TEXT, typstorage TEXT, typnotnull BOOLEAN, typbasetype INTEGER, typtypmod INTEGER, typndims INTEGER, typcollation INTEGER, typdefaultbin TEXT, typdefault TEXT, typacl TEXT, tableoid INTEGER HIDDEN)";

#[derive(Debug)]
struct PgTypeTable;

impl SnapshotRows for PgTypeTable {
    const SCHEMA: &'static str = PG_TYPE_SQL;
    const TABLE_OID: Option<i64> = Some(1247);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();

        // Static base types
        for t in PG_BASE_TYPES {
            rows.push(make_type_row(t));
        }

        // Static array types
        for &(oid, name, typelem) in PG_ARRAY_TYPES {
            rows.push(make_type_row(&PgTypeInfo {
                oid,
                name,
                typtype: "b",
                typcategory: "A",
                typlen: -1,
                typarray: 0,
                typelem,
                typbyval: false,
                typalign: "i",
                typstorage: "x",
            }));
        }

        // Dynamic: user-defined enum types from type_registry
        let schema = conn.current_schema();
        for (name, td) in &schema.type_registry {
            if td.is_builtin {
                continue;
            }
            // User-defined enums: typtype='e', typcategory='E'
            let enum_oid = 50000
                + (name
                    .as_bytes()
                    .iter()
                    .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64))
                    % 10000) as i64;
            rows.push(vec![
                Value::from_i64(enum_oid),        // oid
                Value::Text(name.clone().into()), // typname
                Value::from_i64(11),              // typnamespace (pg_catalog)
                Value::from_i64(10),              // typowner
                Value::from_i64(4),               // typlen
                Value::from_i64(1),               // typbyval
                Value::build_text("e"),           // typtype (enum)
                Value::build_text("E"),           // typcategory (enum)
                Value::from_i64(0),               // typispreferred
                Value::from_i64(1),               // typisdefined
                Value::build_text(","),           // typdelim
                Value::from_i64(0),               // typrelid
                Value::Null,                      // typsubscript
                Value::from_i64(0),               // typelem
                Value::from_i64(0),               // typarray
                Value::Null,                      // typinput
                Value::Null,                      // typoutput
                Value::Null,                      // typreceive
                Value::Null,                      // typsend
                Value::Null,                      // typmodin
                Value::Null,                      // typmodout
                Value::Null,                      // typanalyze
                Value::build_text("i"),           // typalign
                Value::build_text("p"),           // typstorage
                Value::from_i64(0),               // typnotnull
                Value::from_i64(0),               // typbasetype
                Value::from_i64(-1),              // typtypmod
                Value::from_i64(0),               // typndims
                Value::from_i64(0),               // typcollation
                Value::Null,                      // typdefaultbin
                Value::Null,                      // typdefault
                Value::Null,                      // typacl
            ]);
        }
        rows
    }
}

fn make_type_row(t: &PgTypeInfo) -> Vec<Value> {
    vec![
        Value::from_i64(t.oid),                 // oid
        Value::build_text(t.name),              // typname
        Value::from_i64(11),                    // typnamespace (pg_catalog)
        Value::from_i64(10),                    // typowner
        Value::from_i64(t.typlen),              // typlen
        Value::from_i64(i64::from(t.typbyval)), // typbyval
        Value::build_text(t.typtype),           // typtype
        Value::build_text(t.typcategory),       // typcategory
        Value::from_i64(0),                     // typispreferred
        Value::from_i64(1),                     // typisdefined
        Value::build_text(","),                 // typdelim
        Value::from_i64(0),                     // typrelid
        Value::Null,                            // typsubscript
        Value::from_i64(t.typelem),             // typelem
        Value::from_i64(t.typarray),            // typarray
        Value::Null,                            // typinput
        Value::Null,                            // typoutput
        Value::Null,                            // typreceive
        Value::Null,                            // typsend
        Value::Null,                            // typmodin
        Value::Null,                            // typmodout
        Value::Null,                            // typanalyze
        Value::build_text(t.typalign),          // typalign
        Value::build_text(t.typstorage),        // typstorage
        Value::from_i64(0),                     // typnotnull
        Value::from_i64(0),                     // typbasetype
        Value::from_i64(-1),                    // typtypmod
        Value::from_i64(0),                     // typndims
        Value::from_i64(0),                     // typcollation
        Value::Null,                            // typdefaultbin
        Value::Null,                            // typdefault
        Value::Null,                            // typacl
    ]
}

// ──────────────────────────────────────────────────────────────────────
// pg_index
// ──────────────────────────────────────────────────────────────────────

const PG_INDEX_SQL: &str = "CREATE TABLE pg_index (indexrelid INTEGER, indrelid INTEGER, indnatts INTEGER, indnkeyatts INTEGER, indisunique BOOLEAN, indisprimary BOOLEAN, indisexclusion BOOLEAN, indimmediate BOOLEAN, indisclustered BOOLEAN, indisvalid BOOLEAN, indcheckxmin BOOLEAN, indisready BOOLEAN, indislive BOOLEAN, indisreplident BOOLEAN, indkey TEXT, indcollation TEXT, indclass TEXT, indoption TEXT, indexprs TEXT, indpred TEXT, indnullsnotdistinct BOOLEAN, tableoid INTEGER HIDDEN)";

#[derive(Debug)]
struct PgIndexTable;

impl SnapshotRows for PgIndexTable {
    const SCHEMA: &'static str = PG_INDEX_SQL;
    const TABLE_OID: Option<i64> = Some(2610);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();

        for index in catalog_indexes(&catalog_relations(conn)) {
            let indnatts = index.columns.len() as i64;
            let indkey: String = index
                .columns
                .iter()
                .map(|(_, position, expression)| {
                    if expression.is_some() {
                        "0".to_string()
                    } else {
                        (position + 1).to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");

            let indpred = index
                .where_clause
                .as_ref()
                .map(|e| Value::build_text(e.clone()))
                .unwrap_or(Value::Null);

            let indexprs = if index.columns.iter().any(|(_, _, e)| e.is_some()) {
                let exprs: Vec<String> = index
                    .columns
                    .iter()
                    .filter_map(|(_, _, e)| e.clone())
                    .collect();
                Value::build_text(exprs.join(", "))
            } else {
                Value::Null
            };

            rows.push(vec![
                Value::from_i64(index.oid),                // indexrelid
                Value::from_i64(index.table_oid),          // indrelid
                Value::from_i64(indnatts),                 // indnatts
                Value::from_i64(indnatts),                 // indnkeyatts
                Value::from_i64(i64::from(index.unique)),  // indisunique
                Value::from_i64(i64::from(index.primary)), // indisprimary
                Value::from_i64(0),                        // indisexclusion
                Value::from_i64(1),                        // indimmediate
                Value::from_i64(0),                        // indisclustered
                Value::from_i64(1),                        // indisvalid
                Value::from_i64(0),                        // indcheckxmin
                Value::from_i64(1),                        // indisready
                Value::from_i64(1),                        // indislive
                Value::from_i64(0),                        // indisreplident
                Value::build_text(indkey),                 // indkey
                Value::Null,                               // indcollation
                Value::Null,                               // indclass
                Value::Null,                               // indoption
                indexprs,                                  // indexprs
                indpred,                                   // indpred
                Value::from_i64(0),
            ]);
        }
        rows
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_constraint
// ──────────────────────────────────────────────────────────────────────

const PG_CONSTRAINT_SQL: &str = "CREATE TABLE pg_constraint (oid INTEGER, conname TEXT, connamespace INTEGER, contype TEXT, condeferrable BOOLEAN, condeferred BOOLEAN, convalidated BOOLEAN, conrelid INTEGER, contypid INTEGER, conindid INTEGER, conparentid INTEGER, confrelid INTEGER, confupdtype TEXT, confdeltype TEXT, confmatchtype TEXT, conislocal BOOLEAN, coninhcount INTEGER, connoinherit BOOLEAN, conkey TEXT, confkey TEXT, conpfeqop TEXT, conppeqop TEXT, conffeqop TEXT, conexclop TEXT, conbin TEXT, tableoid INTEGER HIDDEN)";

#[derive(Debug)]
struct PgConstraintTable;

impl SnapshotRows for PgConstraintTable {
    const SCHEMA: &'static str = PG_CONSTRAINT_SQL;
    const TABLE_OID: Option<i64> = Some(2606);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();

        let relations = catalog_relations(conn);
        let indexes = catalog_indexes(&relations);
        let mut constraint_oid = indexes
            .last()
            .map_or(USER_TABLE_OID_START + relations.len() as i64, |index| {
                index.oid + 1
            });

        for relation in &relations {
            let btree = match &relation.kind {
                CatalogRelationKind::Table(table) => table,
                _ => continue,
            };
            let table_name = &relation.name;
            let table_oid = relation.oid;

            // Synthesize PK constraint for rowid-alias tables when unique_sets has no PK
            let has_pk_in_unique_sets = btree.unique_sets.iter().any(|us| us.is_primary_key);
            if !has_pk_in_unique_sets && btree.get_rowid_alias_column().is_some() {
                let conname = btree
                    .primary_key_name
                    .clone()
                    .unwrap_or_else(|| format!("{table_name}_pkey"));
                let conkey: String = btree
                    .primary_key_columns
                    .iter()
                    .map(|(name, _)| {
                        btree
                            .get_column(name)
                            .map(|(pos, _)| (pos + 1).to_string())
                            .unwrap_or_else(|| "0".to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                rows.push(vec![
                    Value::from_i64(constraint_oid),
                    Value::build_text(conname),
                    Value::from_i64(relation.namespace_oid),
                    Value::build_text("p"),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(1),
                    Value::from_i64(table_oid),
                    Value::from_i64(0),
                    Value::from_i64(
                        indexes
                            .iter()
                            .find(|index| index.table_oid == table_oid && index.primary)
                            .expect("rowid primary keys have a catalog index")
                            .oid,
                    ),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::from_i64(1),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::build_text(conkey),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]);
                constraint_oid += 1;
            }

            // PK / UNIQUE constraints from unique_sets
            for us in &btree.unique_sets {
                let contype = if us.is_primary_key { "p" } else { "u" };
                let col_names: Vec<&str> = us.columns.iter().map(|c| c.name.as_str()).collect();
                let conname = us.name.clone().unwrap_or_else(|| {
                    if us.is_primary_key {
                        format!("{table_name}_pkey")
                    } else {
                        let cols_str = col_names.join("_");
                        format!("{table_name}_{cols_str}_key")
                    }
                });

                // Build conkey: space-separated 1-based attnums
                let conkey: String = col_names
                    .iter()
                    .map(|name| {
                        btree
                            .get_column(name)
                            .map(|(pos, _)| (pos + 1).to_string())
                            .unwrap_or_else(|| "0".to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let positions: Vec<usize> = col_names
                    .iter()
                    .map(|name| btree.get_column(name).expect("constraint columns exist").0)
                    .collect();
                let conindid = indexes
                    .iter()
                    .find(|index| {
                        index.table_oid == table_oid
                            && index.unique
                            && index
                                .name
                                .starts_with(PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX)
                            && index.where_clause.is_none()
                            && index
                                .columns
                                .iter()
                                .all(|(_, _, expression)| expression.is_none())
                            && index.primary == us.is_primary_key
                            && index
                                .columns
                                .iter()
                                .map(|(_, pos, _)| *pos)
                                .eq(positions.iter().copied())
                    })
                    .expect("unique constraints have a catalog index")
                    .oid;

                rows.push(vec![
                    Value::from_i64(constraint_oid), // oid
                    Value::build_text(conname),      // conname
                    Value::from_i64(relation.namespace_oid),
                    Value::build_text(contype), // contype
                    Value::from_i64(0),         // condeferrable
                    Value::from_i64(0),         // condeferred
                    Value::from_i64(1),         // convalidated
                    Value::from_i64(table_oid), // conrelid
                    Value::from_i64(0),         // contypid
                    Value::from_i64(conindid),  // conindid
                    Value::from_i64(0),         // conparentid
                    Value::from_i64(0),         // confrelid
                    Value::Null,                // confupdtype
                    Value::Null,                // confdeltype
                    Value::Null,                // confmatchtype
                    Value::from_i64(1),         // conislocal
                    Value::from_i64(0),         // coninhcount
                    Value::from_i64(0),         // connoinherit
                    Value::build_text(conkey),  // conkey
                    Value::Null,                // confkey
                    Value::Null,                // conpfeqop
                    Value::Null,                // conppeqop
                    Value::Null,                // conffeqop
                    Value::Null,                // conexclop
                    Value::Null,                // conbin
                ]);
                constraint_oid += 1;
            }

            // FK constraints from foreign_keys
            for fk in &btree.foreign_keys {
                let child_cols = fk.child_columns.join("_");
                let conname = fk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{table_name}_{child_cols}_fkey"));

                let conkey: String = fk
                    .child_columns
                    .iter()
                    .map(|name| {
                        btree
                            .get_column(name)
                            .map(|(pos, _)| (pos + 1).to_string())
                            .unwrap_or_else(|| "0".to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let confrelid = relations
                    .iter()
                    .find(|parent| {
                        parent.namespace == relation.namespace && parent.name == fk.parent_table
                    })
                    .map_or(0, |parent| parent.oid);

                let confkey: String = fk
                    .parent_columns
                    .iter()
                    .map(|name| {
                        relation
                            .schema
                            .get_btree_table(&fk.parent_table)
                            .and_then(|parent_bt| {
                                parent_bt
                                    .get_column(name)
                                    .map(|(pos, _)| (pos + 1).to_string())
                            })
                            .unwrap_or_else(|| "0".to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                rows.push(vec![
                    Value::from_i64(constraint_oid), // oid
                    Value::build_text(conname),      // conname
                    Value::from_i64(relation.namespace_oid),
                    Value::build_text("f"),                  // contype
                    Value::from_i64(i64::from(fk.deferred)), // condeferrable
                    Value::from_i64(i64::from(fk.deferred)), // condeferred
                    Value::from_i64(1),                      // convalidated
                    Value::from_i64(table_oid),              // conrelid
                    Value::from_i64(0),                      // contypid
                    Value::from_i64(0),                      // conindid
                    Value::from_i64(0),                      // conparentid
                    Value::from_i64(confrelid),              // confrelid
                    Value::build_text(ref_act_to_char(&fk.on_update)), // confupdtype
                    Value::build_text(ref_act_to_char(&fk.on_delete)), // confdeltype
                    Value::build_text("s"),                  // confmatchtype (simple)
                    Value::from_i64(1),                      // conislocal
                    Value::from_i64(0),                      // coninhcount
                    Value::from_i64(0),                      // connoinherit
                    Value::build_text(conkey),               // conkey
                    Value::build_text(confkey),              // confkey
                    Value::Null,                             // conpfeqop
                    Value::Null,                             // conppeqop
                    Value::Null,                             // conffeqop
                    Value::Null,                             // conexclop
                    Value::Null,                             // conbin
                ]);
                constraint_oid += 1;
            }

            // CHECK constraints from check_constraints
            for chk in &btree.check_constraints {
                let conname = chk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{table_name}_check"));

                let conkey = chk
                    .column
                    .as_ref()
                    .and_then(|col_name| {
                        btree
                            .get_column(col_name)
                            .map(|(pos, _)| (pos + 1).to_string())
                    })
                    .unwrap_or_default();

                rows.push(vec![
                    Value::from_i64(constraint_oid), // oid
                    Value::build_text(conname),      // conname
                    Value::from_i64(relation.namespace_oid),
                    Value::build_text("c"),     // contype
                    Value::from_i64(0),         // condeferrable
                    Value::from_i64(0),         // condeferred
                    Value::from_i64(1),         // convalidated
                    Value::from_i64(table_oid), // conrelid
                    Value::from_i64(0),         // contypid
                    Value::from_i64(0),         // conindid
                    Value::from_i64(0),         // conparentid
                    Value::from_i64(0),         // confrelid
                    Value::Null,                // confupdtype
                    Value::Null,                // confdeltype
                    Value::Null,                // confmatchtype
                    Value::from_i64(1),         // conislocal
                    Value::from_i64(0),         // coninhcount
                    Value::from_i64(0),         // connoinherit
                    if conkey.is_empty() {
                        Value::Null
                    } else {
                        Value::build_text(conkey)
                    }, // conkey
                    Value::Null,                // confkey
                    Value::Null,                // conpfeqop
                    Value::Null,                // conppeqop
                    Value::Null,                // conffeqop
                    Value::Null,                // conexclop
                    Value::build_text(chk.expr.to_string()), // conbin
                ]);
                constraint_oid += 1;
            }
        }
        rows
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_attrdef
// ──────────────────────────────────────────────────────────────────────

const PG_ATTRDEF_SQL: &str = "CREATE TABLE pg_attrdef (oid INTEGER, adrelid INTEGER, adnum INTEGER, adbin TEXT, tableoid INTEGER HIDDEN)";

#[derive(Debug)]
struct PgAttrdefTable;

impl SnapshotRows for PgAttrdefTable {
    const SCHEMA: &'static str = PG_ATTRDEF_SQL;
    const TABLE_OID: Option<i64> = Some(2604);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();

        // OID counter for pg_attrdef rows — start after constraint OIDs
        // Use a high base to avoid collisions
        let mut attrdef_oid: i64 = 50000;

        for relation in catalog_relations(conn) {
            let btree = match &relation.kind {
                CatalogRelationKind::Table(table) => table,
                _ => continue,
            };

            for (col_idx, col) in btree.columns().iter().enumerate() {
                if let Some(default_expr) = &col.default {
                    rows.push(vec![
                        Value::from_i64(attrdef_oid), // oid
                        Value::from_i64(relation.oid),
                        Value::from_i64(col_idx as i64 + 1), // adnum (1-based)
                        Value::build_text(default_expr.to_string()), // adbin
                    ]);
                    attrdef_oid += 1;
                }
            }
        }
        rows
    }
}

#[derive(Debug)]
struct PgSequenceTable;

impl SnapshotRows for PgSequenceTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_sequence (
        seqrelid INTEGER, seqtypid INTEGER, seqstart INTEGER, seqincrement INTEGER,
        seqmax INTEGER, seqmin INTEGER, seqcache INTEGER, seqcycle BOOLEAN,
        tableoid INTEGER HIDDEN
    )";
    const TABLE_OID: Option<i64> = Some(2224);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 10;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        catalog_relations(conn)
            .into_iter()
            .filter_map(|relation| {
                let CatalogRelationKind::Sequence(sequence) = relation.kind else {
                    return None;
                };
                Some(vec![
                    Value::from_i64(relation.oid),
                    Value::from_i64(20),
                    Value::from_i64(sequence.start_value),
                    Value::from_i64(sequence.increment_by),
                    Value::from_i64(sequence.max_value),
                    Value::from_i64(sequence.min_value),
                    Value::from_i64(1),
                    Value::from_i64(i64::from(sequence.cycle)),
                ])
            })
            .collect()
    }
}

/// Virtual table implementation for pg_catalog.pg_sequences
/// Reads sequence metadata from Schema.sequences at scan time.
#[derive(Debug)]
struct PgSequencesTable;

impl SnapshotRows for PgSequencesTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_sequences (
            schemaname TEXT,
            sequencename TEXT,
            sequenceowner TEXT,
            data_type TEXT,
            start_value INTEGER,
            min_value INTEGER,
            max_value INTEGER,
            increment_by INTEGER,
            cycle INTEGER,
            cache_size INTEGER,
            last_value INTEGER
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 10;

    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn read_sql(conn: &Connection) -> Option<String> {
        let selects: Vec<_> = catalog_relations(conn)
            .into_iter()
            .filter_map(|relation| {
                let CatalogRelationKind::Sequence(sequence) = &relation.kind else {
                    return None;
                };
                let state = sequence_state_sql(&relation.namespace, sequence);
                Some(format!(
                    "SELECT '{}', '{}', 'turso', 'bigint', {}, {}, {}, {}, {}, 1,
                (SELECT CASE WHEN is_called THEN last_value ELSE NULL END FROM ({state}))",
                    relation.namespace.replace('\'', "''"),
                    sequence.name.replace('\'', "''"),
                    sequence.start_value,
                    sequence.min_value,
                    sequence.max_value,
                    sequence.increment_by,
                    i64::from(sequence.cycle)
                ))
            })
            .collect();
        (!selects.is_empty()).then(|| selects.join(" UNION ALL "))
    }
}

pub(crate) fn register_catalog_modules(mut options: OpenOptions) -> OpenOptions {
    options = options.native_module(
        "pg_class",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgClassTable>(PhantomData),
    );
    options = options.native_module(
        "pg_namespace",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgNamespaceTable>(PhantomData),
    );
    options = options.native_module(
        "pg_attribute",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgAttributeTable>(PhantomData),
    );
    options = options.native_module(
        "pg_roles",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgRolesTable>(PhantomData),
    );
    options = options.native_module(
        "pg_am",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgAmTable>(PhantomData),
    );
    options = options.native_module(
        "pg_proc",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgProcTable>(PhantomData),
    );
    options = options.native_module(
        "pg_database",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgDatabaseTable>(PhantomData),
    );
    options = options.native_module(
        "pg_tables",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgTablesTable>(PhantomData),
    );
    options = options.native_module(
        "pg_get_tabledef",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgGetTableDefTable::schema,
            create: PgGetTableDefTable::new,
        },
    );
    options = options.native_module(
        "pg_index",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgIndexTable>(PhantomData),
    );
    options = options.native_module(
        "pg_constraint",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgConstraintTable>(PhantomData),
    );
    options = options.native_module(
        "pg_type",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgTypeTable>(PhantomData),
    );
    options = options.native_module(
        "pg_attrdef",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgAttrdefTable>(PhantomData),
    );
    options = options.native_module(
        "pg_input_error_info",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgInputErrorInfoTable::schema,
            create: PgInputErrorInfoTable::new,
        },
    );
    options = options.native_module(
        "pg_sequence",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgSequenceTable>(PhantomData),
    );
    options = options.native_module(
        "pg_sequences",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgSequencesTable>(PhantomData),
    );
    options = options.native_module(
        "pg_settings",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgSettingsTable>(PhantomData),
    );
    options = options.native_module(
        "pg_extension",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_extension (
                oid INTEGER, extname TEXT, extowner INTEGER, extnamespace INTEGER,
                extrelocatable BOOLEAN, extversion TEXT, extconfig INTEGER[], extcondition TEXT[],
                tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );
    options = options.native_module(
        "pg_policy",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_policy (oid INTEGER, polname TEXT, polpermissive TEXT, polroles TEXT, polcmd TEXT, polqual TEXT, polwithcheck TEXT, polrelid INTEGER, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_trigger",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_trigger (
                oid INTEGER, tgrelid INTEGER, tgparentid INTEGER NOT NULL DEFAULT 0,
                tgname TEXT, tgfoid INTEGER, tgtype INTEGER, tgenabled TEXT,
                tgisinternal INTEGER, tgconstrrelid INTEGER, tgconstrindid INTEGER,
                tgconstraint INTEGER, tgdeferrable INTEGER, tginitdeferred INTEGER,
                tgnargs INTEGER, tgattr TEXT, tgargs TEXT, tgqual TEXT, tgoldtable TEXT,
                tgnewtable TEXT, tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );
    options = options.native_module(
        "pg_statistic_ext",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_statistic_ext (oid INTEGER, stxrelid INTEGER, stxname TEXT, stxnamespace INTEGER, stxowner INTEGER, stxstattarget INTEGER, stxkeys TEXT, stxkind TEXT, stxexprs TEXT, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_inherits",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_inherits (inhrelid INTEGER, inhparent INTEGER, inhseqno INTEGER, inhdetachpending INTEGER, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_rewrite",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_rewrite (oid INTEGER, rulename TEXT, ev_class INTEGER, ev_type TEXT, ev_enabled TEXT, is_instead INTEGER, ev_qual TEXT, ev_action TEXT, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_foreign_table",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql:
                "CREATE TABLE pg_foreign_table (ftrelid INTEGER, ftserver INTEGER, ftoptions TEXT, tableoid INTEGER HIDDEN)"
                    .to_string(),
        },
    );
    options = options.native_module(
        "pg_partitioned_table",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_partitioned_table (partrelid INTEGER, partstrat TEXT, partnatts INTEGER, partdefid INTEGER, partattrs TEXT, partclass TEXT, partcollation TEXT, partexprs TEXT, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_collation",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_collation (oid INTEGER, collname TEXT, collnamespace INTEGER, collowner INTEGER, collprovider TEXT, collisdeterministic INTEGER, collencoding INTEGER, collcollate TEXT, collctype TEXT, colliculocale TEXT, collicurules TEXT, collversion TEXT, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_description",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_description (objoid INTEGER, classoid INTEGER, objsubid INTEGER, description TEXT, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_publication",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication (oid INTEGER, pubname TEXT, pubowner INTEGER, puballtables INTEGER, pubinsert INTEGER, pubupdate INTEGER, pubdelete INTEGER, pubtruncate INTEGER, pubviaroot INTEGER, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_publication_namespace",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication_namespace (oid INTEGER, pnpubid INTEGER, pnnspid INTEGER, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_publication_rel",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication_rel (oid INTEGER, prpubid INTEGER, prrelid INTEGER, prqual TEXT, prattrs TEXT, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_depend",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgDependTable>(PhantomData),
    );
    options = options.native_module(
        "pg_tablespace",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_tablespace (
                oid INTEGER, spcname TEXT, spcowner INTEGER, spcacl TEXT[], spcoptions TEXT[],
                tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );
    options = options.native_module(
        "pg_init_privs",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_init_privs (
                objoid INTEGER, classoid INTEGER, objsubid INTEGER, privtype TEXT,
                initprivs TEXT[], tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );
    options = options.native_module(
        "pg_cast",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_cast (
                oid INTEGER, castsource INTEGER, casttarget INTEGER, castfunc INTEGER,
                castcontext TEXT, castmethod TEXT, tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );
    options = options.native_module(
        "pg_transform",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_transform (
                oid INTEGER, trftype INTEGER, trflang INTEGER, trffromsql INTEGER,
                trftosql INTEGER, tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );
    options = options.native_module(
        "pg_language",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<PgLanguageTable>(PhantomData),
    );
    options = options.native_module(
        "pg_options_to_table",
        VTabKind::TableValuedFunction,
        PgOptionsToTable,
    );
    options = options.native_module("unnest", VTabKind::TableValuedFunction, PgUnnest);
    options = options.native_module(
        "pg_generate_series",
        VTabKind::TableValuedFunction,
        PgGenerateSeries,
    );
    for (name, create_sql) in [
        (
            "pg_operator",
            "CREATE TABLE pg_operator (
            oid INTEGER, oprname TEXT, oprnamespace INTEGER, oprowner INTEGER,
            oprkind TEXT, oprcanmerge BOOLEAN, oprcanhash BOOLEAN, oprleft INTEGER,
            oprright INTEGER, oprresult INTEGER, oprcom INTEGER, oprnegate INTEGER,
            oprcode INTEGER, oprrest INTEGER, oprjoin INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_opclass",
            "CREATE TABLE pg_opclass (
            oid INTEGER, opcmethod INTEGER, opcname TEXT, opcnamespace INTEGER,
            opcowner INTEGER, opcfamily INTEGER, opcintype INTEGER, opcdefault BOOLEAN,
            opckeytype INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_opfamily",
            "CREATE TABLE pg_opfamily (
            oid INTEGER, opfmethod INTEGER, opfname TEXT, opfnamespace INTEGER,
            opfowner INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_ts_parser",
            "CREATE TABLE pg_ts_parser (
            oid INTEGER, prsname TEXT, prsnamespace INTEGER, prsstart INTEGER,
            prstoken INTEGER, prsend INTEGER, prsheadline INTEGER, prslextype INTEGER,
            tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_ts_template",
            "CREATE TABLE pg_ts_template (
            oid INTEGER, tmplname TEXT, tmplnamespace INTEGER, tmplinit INTEGER,
            tmpllexize INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_ts_dict",
            "CREATE TABLE pg_ts_dict (
            oid INTEGER, dictname TEXT, dictnamespace INTEGER, dictowner INTEGER,
            dicttemplate INTEGER, dictinitoption TEXT, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_ts_config",
            "CREATE TABLE pg_ts_config (
            oid INTEGER, cfgname TEXT, cfgnamespace INTEGER, cfgowner INTEGER,
            cfgparser INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_foreign_data_wrapper",
            "CREATE TABLE pg_foreign_data_wrapper (
            oid INTEGER, fdwname TEXT, fdwowner INTEGER, fdwhandler INTEGER,
            fdwvalidator INTEGER, fdwacl TEXT[], fdwoptions TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_foreign_server",
            "CREATE TABLE pg_foreign_server (
            oid INTEGER, srvname TEXT, srvowner INTEGER, srvfdw INTEGER, srvtype TEXT,
            srvversion TEXT, srvacl TEXT[], srvoptions TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_default_acl",
            "CREATE TABLE pg_default_acl (
            oid INTEGER, defaclrole INTEGER, defaclnamespace INTEGER, defaclobjtype TEXT,
            defaclacl TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_conversion",
            "CREATE TABLE pg_conversion (
            oid INTEGER, conname TEXT, connamespace INTEGER, conowner INTEGER,
            conforencoding INTEGER, contoencoding INTEGER, conproc INTEGER,
            condefault BOOLEAN, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_range",
            "CREATE TABLE pg_range (
            rngtypid INTEGER, rngsubtype INTEGER, rngmultitypid INTEGER,
            rngcollation INTEGER, rngsubopc INTEGER, rngcanonical INTEGER,
            rngsubdiff INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_event_trigger",
            "CREATE TABLE pg_event_trigger (
            oid INTEGER, evtname TEXT, evtevent TEXT, evtowner INTEGER, evtfoid INTEGER,
            evtenabled TEXT, evttags TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_subscription",
            "CREATE TABLE pg_subscription (
            oid INTEGER, subdbid INTEGER, subskiplsn TEXT, subname TEXT, subowner INTEGER,
            subenabled BOOLEAN, subbinary BOOLEAN, substream TEXT, subtwophasestate TEXT,
            subdisableonerr BOOLEAN, subpasswordrequired BOOLEAN, subrunasowner BOOLEAN,
            subconninfo TEXT, subslotname TEXT, subsynccommit TEXT, subpublications TEXT[],
            suborigin TEXT, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_largeobject_metadata",
            "CREATE TABLE pg_largeobject_metadata (
            oid INTEGER, lomowner INTEGER, lomacl TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_amop",
            "CREATE TABLE pg_amop (
            oid INTEGER, amopfamily INTEGER, amoplefttype INTEGER, amoprighttype INTEGER,
            amopstrategy INTEGER, amoppurpose TEXT, amopopr INTEGER, amopmethod INTEGER,
            amopsortfamily INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_amproc",
            "CREATE TABLE pg_amproc (
            oid INTEGER, amprocfamily INTEGER, amproclefttype INTEGER, amprocrighttype INTEGER,
            amprocnum INTEGER, amproc INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_seclabels",
            "CREATE TABLE pg_seclabels (
            objoid INTEGER, classoid INTEGER, objsubid INTEGER, objtype TEXT,
            objnamespace INTEGER, objname TEXT, provider TEXT, label TEXT
        )",
        ),
    ] {
        options = options.native_module(
            name,
            VTabKind::TableValuedFunction,
            EmptyPgCatalogTable {
                create_sql: create_sql.to_owned(),
            },
        );
    }
    options
}

#[derive(Debug)]
struct PgGenerateSeries;

impl VirtualTableModule for PgGenerateSeries {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE pg_generate_series (generate_series INTEGER, start INTEGER HIDDEN, stop INTEGER HIDDEN, step INTEGER HIDDEN)".to_owned())
    }

    fn create(&self, _args: &[Value]) -> Result<Self> {
        Ok(Self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for PgGenerateSeries {
    type Cursor = PgGenerateSeriesCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgGenerateSeriesCursor {
            start: 0,
            current: 0,
            stop: 0,
            step: 1,
            rowid: 0,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        use turso_ext::{ConstraintOp, ConstraintUsage};
        let mut inputs = [None; 3];
        for (index, constraint) in constraints.iter().enumerate() {
            if (1..=3).contains(&constraint.column_index) && constraint.op == ConstraintOp::Eq {
                if !constraint.usable {
                    return Err(ResultCode::ConstraintViolation);
                }
                inputs[constraint.column_index as usize - 1] = Some(index);
            }
        }
        if inputs[0].is_none() || inputs[1].is_none() {
            return Err(ResultCode::InvalidArgs);
        }
        let mut next_arg = 1;
        let mut usages = vec![
            ConstraintUsage {
                argv_index: None,
                omit: false,
            };
            constraints.len()
        ];
        for input in inputs.into_iter().flatten() {
            usages[input] = ConstraintUsage {
                argv_index: Some(next_arg),
                omit: true,
            };
            next_arg += 1;
        }
        Ok(IndexInfo {
            constraint_usages: usages,
            estimated_cost: 1.0,
            estimated_rows: 1000,
            ..Default::default()
        })
    }
}

struct PgGenerateSeriesCursor {
    start: i64,
    current: i64,
    stop: i64,
    step: i64,
    rowid: i64,
}

impl VirtualTableCursor for PgGenerateSeriesCursor {
    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        if args.iter().any(|arg| matches!(arg, Value::Null)) {
            return Ok(IOResult::Done(false));
        }
        self.current = args[0].as_int().ok_or_else(|| {
            LimboError::InvalidArgument("generate_series requires integer arguments".to_owned())
        })?;
        self.start = self.current;
        self.stop = args[1].as_int().ok_or_else(|| {
            LimboError::InvalidArgument("generate_series requires integer arguments".to_owned())
        })?;
        self.step = match args.get(2) {
            Some(value) => value.as_int().ok_or_else(|| {
                LimboError::InvalidArgument("generate_series requires integer arguments".to_owned())
            })?,
            None => 1,
        };
        if self.step == 0 {
            return Err(
                LimboError::InvalidArgument("step size cannot equal zero".to_owned()).into(),
            );
        }
        self.rowid = 0;
        Ok(IOResult::Done(self.in_bounds()))
    }

    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.rowid += 1;
        let Some(next) = self.current.checked_add(self.step) else {
            return Ok(IOResult::Done(false));
        };
        self.current = next;
        Ok(IOResult::Done(self.in_bounds()))
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        let value = match column {
            0 => self.current,
            1 => self.start,
            2 => self.stop,
            3 => self.step,
            _ => unreachable!(),
        };
        Ok(IOResult::Done(Value::from_i64(value)))
    }

    fn rowid(&self) -> i64 {
        self.rowid
    }
}

impl PgGenerateSeriesCursor {
    fn in_bounds(&self) -> bool {
        if self.step > 0 {
            self.current <= self.stop
        } else {
            self.current >= self.stop
        }
    }
}

#[derive(Debug)]
struct PgUnnest;

impl VirtualTableModule for PgUnnest {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE unnest (unnest, input BLOB HIDDEN)".to_owned())
    }

    fn create(&self, _args: &[Value]) -> Result<Self> {
        Ok(Self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for PgUnnest {
    type Cursor = PgUnnestCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgUnnestCursor {
            values: vec![],
            input: Value::Null,
            index: 0,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        use turso_ext::{ConstraintOp, ConstraintUsage};
        let input = constraints
            .iter()
            .position(|c| c.column_index == 1 && c.op == ConstraintOp::Eq);
        let Some(input) = input else {
            return Err(ResultCode::InvalidArgs);
        };
        if !constraints[input].usable {
            return Err(ResultCode::ConstraintViolation);
        }
        Ok(IndexInfo {
            constraint_usages: constraints
                .iter()
                .enumerate()
                .map(|(i, _)| ConstraintUsage {
                    argv_index: (i == input).then_some(1),
                    omit: i == input,
                })
                .collect(),
            estimated_cost: 1.0,
            estimated_rows: 10,
            ..Default::default()
        })
    }
}

struct PgUnnestCursor {
    values: Vec<Value>,
    input: Value,
    index: usize,
}

impl VirtualTableCursor for PgUnnestCursor {
    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        self.index = 0;
        self.input = args[0].clone();
        self.values = turso_core::array_values_from_any(&self.input)
            .ok_or_else(|| LimboError::InvalidArgument("unnest requires an array".to_owned()))?;
        Ok(IOResult::Done(!self.values.is_empty()))
    }

    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.index += 1;
        Ok(IOResult::Done(self.index < self.values.len()))
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        Ok(IOResult::Done(if column == 1 {
            self.input.clone()
        } else {
            self.values[self.index].clone()
        }))
    }

    fn rowid(&self) -> i64 {
        self.index as i64
    }
}

#[derive(Debug)]
struct PgOptionsToTable;

impl VirtualTableModule for PgOptionsToTable {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE pg_options_to_table (
            option_name TEXT, option_value TEXT, options BLOB HIDDEN
        )"
        .to_owned())
    }

    fn create(&self, _args: &[Value]) -> Result<Self> {
        Ok(Self)
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for PgOptionsToTable {
    type Cursor = PgOptionsCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgOptionsCursor {
            rows: vec![],
            input: Value::Null,
            index: 0,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        use turso_ext::{ConstraintOp, ConstraintUsage};

        let input = constraints
            .iter()
            .position(|c| c.column_index == 2 && c.op == ConstraintOp::Eq);
        let Some(input) = input else {
            return Err(ResultCode::InvalidArgs);
        };
        if !constraints[input].usable {
            return Err(ResultCode::ConstraintViolation);
        }
        Ok(IndexInfo {
            constraint_usages: constraints
                .iter()
                .enumerate()
                .map(|(i, _)| ConstraintUsage {
                    argv_index: (i == input).then_some(1),
                    omit: i == input,
                })
                .collect(),
            estimated_cost: 1.0,
            estimated_rows: 10,
            ..Default::default()
        })
    }
}

struct PgOptionsCursor {
    rows: Vec<[Value; 2]>,
    input: Value,
    index: usize,
}

impl VirtualTableCursor for PgOptionsCursor {
    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        self.index = 0;
        self.rows.clear();
        self.input = args[0].clone();
        let values = turso_core::array_values_from_any(&self.input).ok_or_else(|| {
            LimboError::InvalidArgument("pg_options_to_table requires a text array".to_owned())
        })?;
        for value in values {
            let Value::Text(text) = value else {
                return Err(LimboError::InvalidArgument(
                    "pg_options_to_table requires non-null text elements".to_owned(),
                )
                .into());
            };
            let (name, value) = match text.as_str().split_once('=') {
                Some((name, value)) => (name, Value::build_text(value.to_owned())),
                None => (text.as_str(), Value::Null),
            };
            self.rows.push([Value::build_text(name.to_owned()), value]);
        }
        Ok(IOResult::Done(!self.rows.is_empty()))
    }

    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.index += 1;
        Ok(IOResult::Done(self.index < self.rows.len()))
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        Ok(IOResult::Done(if column == 2 {
            self.input.clone()
        } else {
            self.rows[self.index][column].clone()
        }))
    }

    fn rowid(&self) -> i64 {
        self.index as i64
    }
}

#[derive(Debug)]
struct PgLanguageTable;

impl SnapshotRows for PgLanguageTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_language (
        oid INTEGER, lanname TEXT, lanowner INTEGER, lanispl BOOLEAN,
        lanpltrusted BOOLEAN, lanplcallfoid INTEGER, laninline INTEGER,
        lanvalidator INTEGER, lanacl TEXT[], tableoid INTEGER HIDDEN
    )";
    const TABLE_OID: Option<i64> = Some(2612);
    const ESTIMATED_COST: f64 = 1.0;
    const ESTIMATED_ROWS: u32 = 2;

    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        [(12, "internal"), (13, "c")]
            .into_iter()
            .map(|(oid, name)| {
                vec![
                    Value::from_i64(oid),
                    Value::build_text(name),
                    Value::from_i64(10),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::Null,
                ]
            })
            .collect()
    }
}

#[derive(Debug)]
struct PgSettingsTable;

impl SnapshotRows for PgSettingsTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_settings (
        name TEXT,
        setting TEXT,
        unit TEXT,
        category TEXT,
        short_desc TEXT,
        extra_desc TEXT,
        context TEXT,
        vartype TEXT,
        source TEXT,
        min_val TEXT,
        max_val TEXT,
        enumvals TEXT[],
        boot_val TEXT,
        reset_val TEXT,
        sourcefile TEXT,
        sourceline INTEGER,
        pending_restart BOOLEAN
    )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 1.0;
    const ESTIMATED_ROWS: u32 = 1;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        use crate::session::{search_path_setting, DEFAULT_SEARCH_PATH};

        let setting = search_path_setting(conn);
        let source = if setting.is_some() {
            "session"
        } else {
            "default"
        };
        vec![vec![
            Value::build_text("search_path"),
            Value::build_text(setting.unwrap_or_else(|| DEFAULT_SEARCH_PATH.to_owned())),
            Value::Null,
            Value::build_text("Client Connection Defaults / Statement Behavior"),
            Value::build_text(
                "Sets the schema search order for names that are not schema-qualified.",
            ),
            Value::Null,
            Value::build_text("user"),
            Value::build_text("string"),
            Value::build_text(source),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::build_text(DEFAULT_SEARCH_PATH),
            Value::build_text(DEFAULT_SEARCH_PATH),
            Value::Null,
            Value::Null,
            Value::from_i64(0),
        ]]
    }
}

#[derive(Debug)]
struct PgDependTable;

impl SnapshotRows for PgDependTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_depend (
        classid INTEGER, objid INTEGER, objsubid INTEGER,
        refclassid INTEGER, refobjid INTEGER, refobjsubid INTEGER, deptype TEXT,
        tableoid INTEGER HIDDEN
    )";
    const TABLE_OID: Option<i64> = Some(2608);
    const ESTIMATED_COST: f64 = 1000.0;
    const ESTIMATED_ROWS: u32 = 100;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        let constraints = PgConstraintTable::load_rows(conn);
        let indexes = PgIndexTable::load_rows(conn);
        let relations = PgClassTable::load_rows(conn);

        for relation in &relations {
            if !matches!(&relation[16], Value::Text(kind) if matches!(kind.as_str(), "r" | "v" | "S"))
            {
                continue;
            }
            let oid = relation[0].as_int().expect("catalog OIDs are integers");
            let namespace = relation[2].as_int().expect("catalog OIDs are integers");
            rows.push(dependency_row([1259, oid, 0], [2615, namespace, 0], "n"));
            let access_method = relation[6].as_int().expect("catalog OIDs are integers");
            if access_method != 0 {
                rows.push(dependency_row(
                    [1259, oid, 0],
                    [2601, access_method, 0],
                    "n",
                ));
            }
        }

        let objects = catalog_relations(conn);
        for object in &objects {
            if matches!(object.kind, CatalogRelationKind::View(_)) {
                let (_, dependencies) = qualified_view_select(object, &objects)
                    .expect("stored views have valid relation names");
                for referenced_oid in dependencies {
                    rows.push(dependency_row(
                        [1259, object.oid, 0],
                        [1259, referenced_oid, 0],
                        "n",
                    ));
                }
            }
        }

        for index in &indexes {
            let oid = index[0].as_int().expect("catalog OIDs are integers");
            let table = index[1].as_int().expect("catalog OIDs are integers");
            let columns = catalog_column_numbers(&index[14]);
            let constraint = constraints.iter().find(|constraint| {
                matches!(&constraint[3], Value::Text(kind) if matches!(kind.as_str(), "p" | "u"))
                    && constraint[9].as_int() == Some(oid)
            });
            if let Some(constraint) = constraint {
                let constraint_oid = constraint[0].as_int().expect("catalog OIDs are integers");
                rows.push(dependency_row(
                    [1259, oid, 0],
                    [2606, constraint_oid, 0],
                    "i",
                ));
            } else {
                for column in columns {
                    rows.push(dependency_row([1259, oid, 0], [1259, table, column], "a"));
                }
            }
        }

        for constraint in &constraints {
            let oid = constraint[0].as_int().expect("catalog OIDs are integers");
            let table = constraint[7].as_int().expect("catalog OIDs are integers");
            for column in catalog_column_numbers(&constraint[18]) {
                rows.push(dependency_row([2606, oid, 0], [1259, table, column], "a"));
            }
            let referenced_table = constraint[11].as_int().expect("catalog OIDs are integers");
            if referenced_table != 0 {
                let referenced_columns = catalog_column_numbers(&constraint[19]);
                for &column in &referenced_columns {
                    rows.push(dependency_row(
                        [2606, oid, 0],
                        [1259, referenced_table, column],
                        "n",
                    ));
                }
                if let Some(index) = indexes.iter().find(|index| {
                    index[1].as_int() == Some(referenced_table)
                        && index[4].as_int() == Some(1)
                        && catalog_column_numbers(&index[14]) == referenced_columns
                }) {
                    let index_oid = index[0].as_int().expect("catalog OIDs are integers");
                    rows.push(dependency_row([2606, oid, 0], [1259, index_oid, 0], "n"));
                }
            }
        }

        for default in PgAttrdefTable::load_rows(conn) {
            let oid = default[0].as_int().expect("catalog OIDs are integers");
            let table = default[1].as_int().expect("catalog OIDs are integers");
            let column = default[2]
                .as_int()
                .expect("catalog column numbers are integers");
            rows.push(dependency_row([2604, oid, 0], [1259, table, column], "a"));
        }
        rows
    }
}

fn catalog_column_numbers(value: &Value) -> Vec<i64> {
    match value {
        Value::Null => vec![0],
        Value::Text(text) => {
            let mut columns: Vec<i64> = text
                .as_str()
                .split_ascii_whitespace()
                .map(|column| column.parse().expect("catalog column numbers are integers"))
                .collect();
            if columns.is_empty() {
                columns.push(0);
            }
            columns.sort_unstable();
            columns.dedup();
            columns
        }
        _ => unreachable!("catalog column lists are text or null"),
    }
}

fn dependency_row(dependent: [i64; 3], referenced: [i64; 3], kind: &'static str) -> Vec<Value> {
    dependent
        .into_iter()
        .chain(referenced)
        .map(Value::from_i64)
        .chain(std::iter::once(Value::build_text(kind)))
        .collect()
}

trait SnapshotRows: Debug + Send + Sync + 'static {
    const SCHEMA: &'static str;
    const TABLE_OID: Option<i64>;
    const ESTIMATED_COST: f64;
    const ESTIMATED_ROWS: u32;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>>;

    fn read_sql(_conn: &Connection) -> Option<String> {
        None
    }
}

#[derive(Debug)]
struct SnapshotCatalog<T>(PhantomData<T>);

impl<T: SnapshotRows> VirtualTableModule for SnapshotCatalog<T> {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok(T::SCHEMA.to_string())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(Self(PhantomData))
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl<T: SnapshotRows> VirtualTable for SnapshotCatalog<T> {
    type Cursor = SnapshotCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(SnapshotCursor {
            conn,
            load_rows: T::load_rows,
            read_sql: T::read_sql,
            statement: None,
            table_oid: T::TABLE_OID,
            rows: Vec::new(),
            current_row: 0,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let constraint_usages = constraints
            .iter()
            .map(|_| turso_ext::ConstraintUsage {
                argv_index: None,
                omit: false,
            })
            .collect();

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: T::ESTIMATED_COST,
            estimated_rows: T::ESTIMATED_ROWS,
            constraint_usages,
        })
    }
}

struct SnapshotCursor {
    conn: Arc<Connection>,
    load_rows: fn(&Connection) -> Vec<Vec<Value>>,
    read_sql: fn(&Connection) -> Option<String>,
    statement: Option<Statement>,
    table_oid: Option<i64>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl VirtualTableCursor for SnapshotCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() {
            if column < self.rows[self.current_row].len() {
                return Ok(IOResult::Done(self.rows[self.current_row][column].clone()));
            }
            if column == self.rows[self.current_row].len() {
                if let Some(table_oid) = self.table_oid {
                    return Ok(IOResult::Done(Value::from_i64(table_oid)));
                }
            }
        }
        Ok(IOResult::Done(Value::Null))
    }

    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        if self.statement.is_none() {
            self.current_row = 0;
            self.rows.clear();
            if let Some(sql) = (self.read_sql)(&self.conn) {
                self.statement = Some(self.conn.prepare_internal(sql)?);
            } else {
                self.rows = (self.load_rows)(&self.conn);
            }
        }
        if let Some(statement) = &mut self.statement {
            let rows = &mut self.rows;
            if let IOResult::IO(completions) = statement.run_with_row_callback_nonblock(|row| {
                rows.push(row.get_values().cloned().collect());
                Ok(())
            })? {
                return Ok(IOResult::IO(completions));
            }
            self.statement = None;
        }
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

#[derive(Debug)]
struct CatalogModule<T> {
    schema: fn() -> String,
    create: fn() -> T,
}

impl<T> VirtualTableModule for CatalogModule<T>
where
    T: VirtualTable + 'static,
{
    type Table = T;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok((self.schema)())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok((self.create)())
    }

    fn innocuous(&self) -> bool {
        true
    }
}

/// Table-valued function: `pg_input_error_info(input TEXT, type TEXT)`
///
/// Returns one row with columns (message, detail, hint, sql_error_code).
/// If the input is valid for the given type, all columns are NULL.
/// If invalid, message and sql_error_code describe the error.
#[derive(Debug)]
struct PgInputErrorInfoTable;

impl PgInputErrorInfoTable {
    fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_input_error_info (
            message TEXT,
            detail TEXT,
            hint TEXT,
            sql_error_code TEXT,
            input TEXT HIDDEN,
            type_name TEXT HIDDEN
        )"
        .to_string()
    }
}

struct PgInputErrorInfoCursor {
    row: Option<[Value; 4]>,
    returned: bool,
}

impl PgInputErrorInfoCursor {
    fn new() -> Self {
        Self {
            row: None,
            returned: false,
        }
    }
}

impl VirtualTableCursor for PgInputErrorInfoCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.returned = true;
        Ok(IOResult::Done(false))
    }

    fn rowid(&self) -> i64 {
        0
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        match &self.row {
            Some(row) if column < 4 => Ok(IOResult::Done(row[column].clone())),
            _ => Ok(IOResult::Done(Value::Null)),
        }
    }

    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        self.returned = false;

        if args.len() < 2 {
            // Not enough arguments — return one row of NULLs
            self.row = Some([Value::Null, Value::Null, Value::Null, Value::Null]);
            return Ok(IOResult::Done(true));
        }

        let input = match &args[0] {
            Value::Text(t) => t.as_str().to_string(),
            Value::Null => {
                self.row = Some([Value::Null, Value::Null, Value::Null, Value::Null]);
                return Ok(IOResult::Done(true));
            }
            v => v.to_string(),
        };

        let type_name = match &args[1] {
            Value::Text(t) => t.as_str().to_string(),
            _ => {
                self.row = Some([Value::Null, Value::Null, Value::Null, Value::Null]);
                return Ok(IOResult::Done(true));
            }
        };

        self.row = Some(match validate_pg_input(&input, &type_name) {
            Some((message, code)) => [
                Value::build_text(message),
                Value::Null,
                Value::Null,
                Value::build_text(code),
            ],
            None => [Value::Null, Value::Null, Value::Null, Value::Null],
        });

        Ok(IOResult::Done(true))
    }
}

impl VirtualTable for PgInputErrorInfoTable {
    type Cursor = PgInputErrorInfoCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgInputErrorInfoCursor::new())
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        use turso_ext::{ConstraintOp, ConstraintUsage};

        let mut usages = vec![
            ConstraintUsage {
                argv_index: None,
                omit: false,
            };
            constraints.len()
        ];

        // Hidden columns: input (col 4) and type_name (col 5)
        let mut input_idx = None;
        let mut type_idx = None;
        for (i, c) in constraints.iter().enumerate() {
            if c.op != ConstraintOp::Eq || !c.usable {
                continue;
            }
            match c.column_index as usize {
                4 => input_idx = Some(i),
                5 => type_idx = Some(i),
                _ => {}
            }
        }

        if let Some(i) = input_idx {
            usages[i] = ConstraintUsage {
                argv_index: Some(1),
                omit: true,
            };
        }
        if let Some(i) = type_idx {
            usages[i] = ConstraintUsage {
                argv_index: Some(2),
                omit: true,
            };
        }

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 1,
            constraint_usages: usages,
        })
    }
}

/// Virtual table for getting PostgreSQL-compatible CREATE TABLE statements
#[derive(Debug)]
struct PgGetTableDefTable;

impl PgGetTableDefTable {
    fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_get_tabledef (
            schema_name TEXT,
            table_name TEXT,
            ddl TEXT
        )"
        .to_string()
    }
}

struct PgGetTableDefCursor {
    conn: Arc<Connection>,
    statement: Option<Statement>,
    sql_map: HashMap<(String, String), String>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
    row_count: usize,
}

impl PgGetTableDefCursor {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            statement: None,
            sql_map: HashMap::default(),
            rows: Vec::new(),
            current_row: 0,
            row_count: 0,
        }
    }

    fn load_table_defs(&mut self) {
        self.rows.clear();

        for relation in catalog_relations(&self.conn) {
            let CatalogRelationKind::Table(btree_table) = &relation.kind else {
                continue;
            };

            let postgres_ddl = match self
                .sql_map
                .get(&(relation.namespace.clone(), relation.name.clone()))
            {
                Some(schema_sql) => decode_stored_pg_schema_sql(schema_sql)
                    .map(str::to_string)
                    .unwrap_or_else(|| self.convert_to_postgres_ddl(schema_sql)),
                None => self.convert_to_postgres_ddl(&btree_table.to_sql()),
            };

            self.rows.push(vec![
                Value::build_text(relation.namespace),
                Value::build_text(relation.name),
                Value::Text(postgres_ddl.into()),
            ]);
        }
    }

    fn convert_to_postgres_ddl(&self, sqlite_ddl: &str) -> String {
        let mut postgres_ddl = sqlite_ddl.to_string();

        // Basic SQLite to PostgreSQL type conversions
        // Handle INTEGER PRIMARY KEY specially for SERIAL
        postgres_ddl = postgres_ddl.replace(" INTEGER PRIMARY KEY", " SERIAL PRIMARY KEY");
        postgres_ddl = postgres_ddl.replace(" AUTOINCREMENT", "");

        // Type conversions - use lowercase for PostgreSQL standard
        // Use regex-like replacements to handle case-insensitive matches
        let type_replacements = [
            (" INTEGER", " integer"),
            (" intEgEr", " integer"),
            (" REAL", " double precision"),
            (" real", " double precision"),
            (" TEXT", " text"),
            (" text", " text"),
            (" BLOB", " bytea"),
            (" blob", " bytea"),
            (" DATETIME", " timestamp"),
            (" datetime", " timestamp"),
        ];

        for (from, to) in &type_replacements {
            postgres_ddl = postgres_ddl.replace(from, to);
        }

        // Remove SQLite-specific features
        postgres_ddl = postgres_ddl.replace(" WITHOUT ROWID", "");

        postgres_ddl
    }
}

impl VirtualTableCursor for PgGetTableDefCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.row_count))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < 3 {
            Ok(IOResult::Done(self.rows[self.current_row][column].clone()))
        } else {
            Ok(IOResult::Done(Value::Null))
        }
    }

    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        if self.statement.is_none() {
            let mut databases = vec!["main".to_string()];
            let mut attached = self.conn.attached_database_names();
            attached.sort();
            databases.extend(attached);
            let sql = databases
                .iter()
                .map(|name| {
                    let namespace = if name == "main" { "public" } else { name };
                    format!(
                        "SELECT '{}', name, sql FROM {}.sqlite_schema WHERE type = 'table'",
                        namespace.replace('\'', "''"),
                        quote_identifier(name)
                    )
                })
                .collect::<Vec<_>>()
                .join(" UNION ALL ");
            self.statement = Some(self.conn.prepare_internal(sql)?);
            self.sql_map.clear();
            self.rows.clear();
            self.current_row = 0;
            self.row_count = 0;
        }
        let sql_map = &mut self.sql_map;
        if let IOResult::IO(completions) = self
            .statement
            .as_mut()
            .unwrap()
            .run_with_row_callback_nonblock(|row| {
                if let (Value::Text(namespace), Value::Text(name), Value::Text(sql)) =
                    (row.get_value(0), row.get_value(1), row.get_value(2))
                {
                    sql_map.insert(
                        (namespace.as_str().to_string(), name.as_str().to_string()),
                        sql.as_str().to_string(),
                    );
                }
                Ok(())
            })?
        {
            return Ok(IOResult::IO(completions));
        }
        self.statement = None;
        self.load_table_defs();
        self.sql_map.clear();
        self.row_count = self.rows.len();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

impl VirtualTable for PgGetTableDefTable {
    type Cursor = PgGetTableDefCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgGetTableDefCursor::new(conn))
    }

    fn best_index(
        &self,
        _constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 100.0,
            estimated_rows: 20,
            constraint_usages: vec![],
        })
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_get_constraintdef / pg_get_indexdef helper functions
// ──────────────────────────────────────────────────────────────────────

/// Format a referential action character code to SQL clause text.
fn ref_act_to_sql(code: &str) -> &'static str {
    match code {
        "c" => "CASCADE",
        "n" => "SET NULL",
        "d" => "SET DEFAULT",
        "r" => "RESTRICT",
        _ => "NO ACTION",
    }
}

/// Look up a constraint by OID and return its definition string.
/// Uses the same OID assignment as [PgConstraintTable].
pub(crate) fn pg_get_constraintdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let relations = catalog_relations(conn);
    let indexes = catalog_indexes(&relations);
    let mut constraint_oid = indexes
        .last()
        .map_or(USER_TABLE_OID_START + relations.len() as i64, |index| {
            index.oid + 1
        });

    for relation in &relations {
        let btree = match &relation.kind {
            CatalogRelationKind::Table(table) => table,
            _ => continue,
        };

        // Synthesized PK for rowid-alias tables
        let has_pk_in_unique_sets = btree.unique_sets.iter().any(|us| us.is_primary_key);
        if !has_pk_in_unique_sets && btree.get_rowid_alias_column().is_some() {
            if constraint_oid == target_oid {
                let cols: Vec<String> = btree
                    .primary_key_columns
                    .iter()
                    .map(|(name, _)| quote_identifier(name))
                    .collect();
                return Some(format!("PRIMARY KEY ({})", cols.join(", ")));
            }
            constraint_oid += 1;
        }

        // PK / UNIQUE from unique_sets
        for us in &btree.unique_sets {
            if constraint_oid == target_oid {
                let col_names: Vec<String> = us
                    .columns
                    .iter()
                    .map(|column| quote_identifier(&column.name))
                    .collect();
                let kw = if us.is_primary_key {
                    "PRIMARY KEY"
                } else {
                    "UNIQUE"
                };
                return Some(format!("{kw} ({})", col_names.join(", ")));
            }
            constraint_oid += 1;
        }

        // FK constraints
        for fk in &btree.foreign_keys {
            if constraint_oid == target_oid {
                let child_cols = fk
                    .child_columns
                    .iter()
                    .map(|column| quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ");
                let parent_cols = fk
                    .parent_columns
                    .iter()
                    .map(|column| quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut def = format!(
                    "FOREIGN KEY ({child_cols}) REFERENCES {}.{}({parent_cols})",
                    quote_identifier(&relation.namespace),
                    quote_identifier(&fk.parent_table)
                );
                let on_update = ref_act_to_char(&fk.on_update);
                let on_delete = ref_act_to_char(&fk.on_delete);
                if on_update != "a" {
                    def.push_str(&format!(" ON UPDATE {}", ref_act_to_sql(on_update)));
                }
                if on_delete != "a" {
                    def.push_str(&format!(" ON DELETE {}", ref_act_to_sql(on_delete)));
                }
                return Some(def);
            }
            constraint_oid += 1;
        }

        // CHECK constraints
        for chk in &btree.check_constraints {
            if constraint_oid == target_oid {
                return Some(format!("CHECK ({})", chk.expr));
            }
            constraint_oid += 1;
        }
    }

    None
}

/// Look up an index by OID and return its definition (CREATE INDEX ...).
/// Uses the same OID assignment as [PgIndexTable] / [PgClassTable].
pub(crate) fn pg_get_indexdef(conn: &Connection, target_oid: i64) -> Option<String> {
    for index in catalog_indexes(&catalog_relations(conn)) {
        if index.oid == target_oid {
            let unique = if index.unique { "UNIQUE " } else { "" };
            let cols: Vec<String> = index
                .columns
                .iter()
                .map(|(name, _, expression)| {
                    if let Some(expression) = expression {
                        expression.clone()
                    } else {
                        quote_identifier(name)
                    }
                })
                .collect();
            let mut def = format!(
                "CREATE {unique}INDEX {} ON {}.{} USING btree ({})",
                quote_identifier(&index.name),
                quote_identifier(&index.namespace),
                quote_identifier(&index.table_name),
                cols.join(", ")
            );
            if let Some(where_clause) = &index.where_clause {
                def.push_str(&format!(" WHERE {where_clause}"));
            }
            return Some(def);
        }
    }

    None
}

pub(crate) fn pg_get_viewdef(conn: &Connection, target_oid: i64) -> Result<Option<String>> {
    let relations = catalog_relations(conn);
    let Some(relation) = relations.iter().find(|relation| {
        relation.oid == target_oid && matches!(relation.kind, CatalogRelationKind::View(_))
    }) else {
        return Ok(None);
    };
    let (select, _) = qualified_view_select(relation, &relations)?;
    Ok(Some(format!("{select};")))
}

fn qualified_view_select(
    relation: &CatalogRelation,
    relations: &[CatalogRelation],
) -> Result<(ast::Select, Vec<i64>)> {
    let CatalogRelationKind::View(view) = &relation.kind else {
        unreachable!()
    };
    let mut select = view.select_stmt.clone();
    let mut dependencies = Vec::new();
    visit_select_relations(
        &mut select,
        &HashSet::new(),
        &mut |table, ctes| {
            let name = match table {
                ast::SelectTable::Table(name, _, _) => name,
                ast::SelectTable::TableCall(name, _, _, _) => {
                    if name.name.as_str() == "pg_generate_series" {
                        name.name = ast::Name::exact("generate_series".to_string());
                    }
                    if name
                        .db_name
                        .as_ref()
                        .is_some_and(|name| name.as_str() == "main")
                    {
                        name.db_name = Some(ast::Name::exact("pg_catalog".to_string()));
                    }
                    return Ok(());
                }
                _ => return Ok(()),
            };
            if name.db_name.is_none() && ctes.contains(name.name.as_str()) {
                return Ok(());
            }
            let namespace = match name.db_name.as_ref().map(ast::Name::as_str) {
                Some("main") => "public",
                Some(namespace) => namespace,
                None if is_catalog_table_name(name.name.as_str()) => "pg_catalog",
                None => &relation.namespace,
            };
            if let Some(referenced) = relations.iter().find(|candidate| {
                candidate.namespace == namespace && candidate.name == name.name.as_str()
            }) {
                if !dependencies.contains(&referenced.oid) {
                    dependencies.push(referenced.oid);
                }
            }
            name.db_name = Some(ast::Name::exact(namespace.to_string()));
            Ok(())
        },
        postgres_view_expr,
    )?;
    Ok((select, dependencies))
}

pub(crate) fn rewrite_sequence_reads(
    conn: &Connection,
    cmd: &mut ast::Cmd,
    options: &turso_core::PrepareOptions,
) -> Result<bool> {
    let stmt = match cmd {
        ast::Cmd::Stmt(stmt)
        | ast::Cmd::Explain(stmt)
        | ast::Cmd::ExplainQueryPlan { stmt, .. } => stmt,
    };
    let ast::Stmt::Select(select) = stmt else {
        return Ok(false);
    };
    let relations = catalog_relations(conn);
    let mut changed = false;
    visit_select_relations(
        select,
        &HashSet::new(),
        &mut |table, ctes| {
            let ast::SelectTable::Table(name, alias, _) = table else {
                return Ok(());
            };
            if name.db_name.is_none() && ctes.contains(name.name.as_str()) {
                return Ok(());
            }
            let search_path = match name.db_name.as_ref() {
                Some(database) => vec![if database.as_str() == "main" {
                    "public".to_string()
                } else {
                    database.as_str().to_string()
                }],
                None => options
                    .unqualified_database_search_path
                    .clone()
                    .unwrap_or_else(|| vec!["public".to_string()]),
            };
            let relation = search_path.iter().find_map(|namespace| {
                relations.iter().find(|relation| {
                    relation.namespace == *namespace && relation.name == name.name.as_str()
                })
            });
            let Some(CatalogRelation {
                namespace,
                kind: CatalogRelationKind::Sequence(sequence),
                ..
            }) = relation
            else {
                return Ok(());
            };
            let sql = sequence_state_sql(namespace, sequence);
            let (Some(ast::Cmd::Stmt(ast::Stmt::Select(state))), _) = conn.dialect().parse(&sql)?
            else {
                unreachable!("sequence state queries are SELECT statements");
            };
            let alias = alias
                .clone()
                .or_else(|| Some(ast::As::As(name.name.clone())));
            *table = ast::SelectTable::Select(state, alias);
            changed = true;
            Ok(())
        },
        |_| {},
    )?;
    Ok(changed)
}

fn sequence_state_sql(namespace: &str, sequence: &Sequence) -> String {
    let database = if namespace == "public" {
        "main"
    } else {
        namespace
    };
    let backing_table = format!("__turso_internal_seq_{}", sequence.name);
    let order = if sequence.increment_by > 0 {
        "DESC"
    } else {
        "ASC"
    };
    format!("SELECT CAST(value AS BIGINT) AS last_value, 0 AS log_cnt, CAST(is_called AS BOOLEAN) AS is_called
        FROM {}.{} ORDER BY value {order} LIMIT 1", quote_identifier(database), quote_identifier(&backing_table))
}

fn visit_select_relations<F>(
    select: &mut ast::Select,
    ctes: &HashSet<String>,
    visit: &mut F,
    expr_visit: fn(&mut ast::Expr),
) -> Result<()>
where
    F: FnMut(&mut ast::SelectTable, &HashSet<String>) -> Result<()>,
{
    let mut ctes = ctes.clone();
    if let Some(with) = &mut select.with {
        if with.recursive {
            ctes.extend(
                with.ctes
                    .iter()
                    .map(|cte| cte.tbl_name.as_str().to_string()),
            );
        }
        for cte in &mut with.ctes {
            visit_select_relations(&mut cte.select, &ctes, visit, expr_visit)?;
            ctes.insert(cte.tbl_name.as_str().to_string());
        }
    }
    for body in std::iter::once(&mut select.body.select).chain(
        select
            .body
            .compounds
            .iter_mut()
            .map(|compound| &mut compound.select),
    ) {
        match body {
            ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
                ..
            } => {
                for column in columns {
                    if let ast::ResultColumn::Expr(expr, _) = column {
                        visit_relation_expr(expr, &ctes, visit, expr_visit)?;
                    }
                }
                if let Some(from) = from {
                    visit_relation_from(from, &ctes, visit, expr_visit)?;
                }
                if let Some(expr) = where_clause {
                    visit_relation_expr(expr, &ctes, visit, expr_visit)?;
                }
                if let Some(group) = group_by {
                    for expr in &mut group.exprs {
                        visit_relation_expr(expr, &ctes, visit, expr_visit)?;
                    }
                    if let Some(expr) = &mut group.having {
                        visit_relation_expr(expr, &ctes, visit, expr_visit)?;
                    }
                }
                for window in window_clause {
                    for expr in &mut window.window.partition_by {
                        visit_relation_expr(expr, &ctes, visit, expr_visit)?;
                    }
                    for column in &mut window.window.order_by {
                        visit_relation_expr(&mut column.expr, &ctes, visit, expr_visit)?;
                    }
                    if let Some(frame) = &mut window.window.frame_clause {
                        for bound in std::iter::once(&mut frame.start).chain(frame.end.iter_mut()) {
                            if let ast::FrameBound::Preceding(expr)
                            | ast::FrameBound::Following(expr) = bound
                            {
                                visit_relation_expr(expr, &ctes, visit, expr_visit)?;
                            }
                        }
                    }
                }
            }
            ast::OneSelect::Values(rows) => {
                for expr in rows.iter_mut().flatten() {
                    visit_relation_expr(expr, &ctes, visit, expr_visit)?;
                }
            }
        }
    }
    for column in &mut select.order_by {
        visit_relation_expr(&mut column.expr, &ctes, visit, expr_visit)?;
    }
    if let Some(limit) = &mut select.limit {
        visit_relation_expr(&mut limit.expr, &ctes, visit, expr_visit)?;
        if let Some(expr) = &mut limit.offset {
            visit_relation_expr(expr, &ctes, visit, expr_visit)?;
        }
    }
    Ok(())
}

fn visit_relation_from<F>(
    from: &mut ast::FromClause,
    ctes: &HashSet<String>,
    visit: &mut F,
    expr_visit: fn(&mut ast::Expr),
) -> Result<()>
where
    F: FnMut(&mut ast::SelectTable, &HashSet<String>) -> Result<()>,
{
    visit_relation_table(&mut from.select, ctes, visit, expr_visit)?;
    for join in &mut from.joins {
        visit_relation_table(&mut join.table, ctes, visit, expr_visit)?;
        if let Some(ast::JoinConstraint::On(expr)) = &mut join.constraint {
            visit_relation_expr(expr, ctes, visit, expr_visit)?;
        }
    }
    Ok(())
}

fn visit_relation_table<F>(
    table: &mut ast::SelectTable,
    ctes: &HashSet<String>,
    visit: &mut F,
    expr_visit: fn(&mut ast::Expr),
) -> Result<()>
where
    F: FnMut(&mut ast::SelectTable, &HashSet<String>) -> Result<()>,
{
    match table {
        ast::SelectTable::Select(select, _) => {
            visit_select_relations(select, ctes, visit, expr_visit)?
        }
        ast::SelectTable::Sub(from, _) => visit_relation_from(from, ctes, visit, expr_visit)?,
        ast::SelectTable::TableCall(_, args, _, _) => {
            for expr in args {
                visit_relation_expr(expr, ctes, visit, expr_visit)?;
            }
        }
        ast::SelectTable::Table(_, _, _) => (),
    }
    visit(table, ctes)
}

fn visit_relation_expr<F>(
    expr: &mut ast::Expr,
    ctes: &HashSet<String>,
    visit: &mut F,
    expr_visit: fn(&mut ast::Expr),
) -> Result<()>
where
    F: FnMut(&mut ast::SelectTable, &HashSet<String>) -> Result<()>,
{
    turso_core::walk_expr_mut(expr, &mut |expr| {
        expr_visit(expr);
        match expr {
            ast::Expr::Array { elements } => {
                for element in elements {
                    visit_relation_expr(element, ctes, visit, expr_visit)?;
                }
                return Ok(turso_core::WalkControl::SkipChildren);
            }
            ast::Expr::Subscript { base, index } => {
                visit_relation_expr(base, ctes, visit, expr_visit)?;
                visit_relation_expr(index, ctes, visit, expr_visit)?;
                return Ok(turso_core::WalkControl::SkipChildren);
            }
            ast::Expr::Subquery(select)
            | ast::Expr::Exists(select)
            | ast::Expr::InSelect { rhs: select, .. } => {
                visit_select_relations(select, ctes, visit, expr_visit)?
            }
            _ => (),
        }
        Ok(turso_core::WalkControl::Continue)
    })?;
    Ok(())
}

fn postgres_view_expr(expr: &mut ast::Expr) {
    if let ast::Expr::FunctionCall { name, args, .. } = expr {
        match name.as_str() {
            "array" => {
                *expr = ast::Expr::Array {
                    elements: std::mem::take(args),
                }
            }
            "array_element" if args.len() == 2 => {
                let index = args.pop().unwrap();
                let base = args.pop().unwrap();
                *expr = ast::Expr::Subscript { base, index };
            }
            _ => (),
        }
    }
}

struct CatalogIndex {
    oid: i64,
    table_oid: i64,
    table_name: String,
    namespace: String,
    namespace_oid: i64,
    name: String,
    columns: Vec<(String, usize, Option<String>)>,
    unique: bool,
    primary: bool,
    where_clause: Option<String>,
}

fn catalog_indexes(relations: &[CatalogRelation]) -> Vec<CatalogIndex> {
    let mut oid = USER_TABLE_OID_START + relations.len() as i64;
    let mut indexes = Vec::new();
    for relation in relations {
        let CatalogRelationKind::Table(btree) = &relation.kind else {
            continue;
        };
        let table_name = &relation.name;
        let table_oid = relation.oid;
        if let Some((position, column)) = btree.get_rowid_alias_column() {
            indexes.push(CatalogIndex {
                oid,
                table_oid,
                table_name: table_name.clone(),
                namespace: relation.namespace.clone(),
                namespace_oid: relation.namespace_oid,
                name: btree
                    .primary_key_name
                    .clone()
                    .unwrap_or_else(|| format!("{table_name}_pkey")),
                columns: vec![(
                    column.name.clone().expect("primary key columns have names"),
                    position,
                    None,
                )],
                unique: true,
                primary: true,
                where_clause: None,
            });
            oid += 1;
        }
        for index in relation
            .schema
            .get_indices(table_name)
            .filter(|index| !index.ephemeral)
        {
            let positions: Vec<usize> = index
                .columns
                .iter()
                .map(|column| column.pos_in_table)
                .collect();
            let primary = index.unique
                && index
                    .name
                    .starts_with(PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX)
                && index.where_clause.is_none()
                && index.columns.iter().all(|column| column.expr.is_none())
                && btree.unique_sets.iter().any(|set| {
                    set.is_primary_key
                        && set
                            .columns
                            .iter()
                            .map(|column| {
                                btree
                                    .get_column(&column.name)
                                    .expect("primary key columns exist")
                                    .0
                            })
                            .eq(positions.iter().copied())
                });
            indexes.push(CatalogIndex {
                oid,
                table_oid,
                table_name: table_name.clone(),
                namespace: relation.namespace.clone(),
                namespace_oid: relation.namespace_oid,
                name: index.name.clone(),
                columns: index
                    .columns
                    .iter()
                    .map(|column| {
                        (
                            column.name.clone(),
                            column.pos_in_table,
                            column.expr.as_ref().map(ToString::to_string),
                        )
                    })
                    .collect(),
                unique: index.unique,
                primary,
                where_clause: index.where_clause.as_ref().map(ToString::to_string),
            });
            oid += 1;
        }
    }
    indexes
}

struct CatalogRelation {
    oid: i64,
    name: String,
    namespace: String,
    namespace_oid: i64,
    schema: Arc<Schema>,
    kind: CatalogRelationKind,
}

enum CatalogRelationKind {
    Table(Arc<BTreeTable>),
    View(Arc<View>),
    Sequence(Arc<Sequence>),
}

fn catalog_relations(conn: &Connection) -> Vec<CatalogRelation> {
    let mut schemas = vec![("public".to_string(), 2200, conn.current_schema())];
    let mut attached = conn.attached_database_names();
    attached.sort();
    for (i, name) in attached.into_iter().enumerate() {
        let schema = conn
            .schema_for_database(&name)
            .expect("attached databases have a schema");
        schemas.push((name, USER_TABLE_OID_START + i as i64, schema));
    }
    let mut relations = Vec::new();
    for (namespace, namespace_oid, schema) in schemas {
        let mut objects = Vec::new();
        for (name, table) in &schema.tables {
            if !is_system_table(name) {
                if let Table::BTree(table) = table.as_ref() {
                    objects.push((name.clone(), CatalogRelationKind::Table(table.clone())));
                }
            }
        }
        for (name, view) in &schema.views {
            if !is_system_table(name) {
                objects.push((name.clone(), CatalogRelationKind::View(view.clone())));
            }
        }
        for (name, sequence) in &schema.sequences {
            if !is_system_table(name) {
                objects.push((
                    name.clone(),
                    CatalogRelationKind::Sequence(sequence.clone()),
                ));
            }
        }
        objects.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (name, kind) in objects {
            relations.push(CatalogRelation {
                oid: USER_TABLE_OID_START + relations.len() as i64,
                name,
                namespace: namespace.clone(),
                namespace_oid,
                schema: schema.clone(),
                kind,
            });
        }
    }
    relations
}

// TODO: Fix tests to use correct API
#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use crate::{Database, Numeric, PlatformIO, StepResult};
    use tempfile::tempdir;

    #[test]
    fn catalogs_registered_at_open_survive_schema_refresh() {
        let catalog_names = [
            "pg_class",
            "pg_namespace",
            "pg_attribute",
            "pg_roles",
            "pg_am",
            "pg_proc",
            "pg_database",
            "pg_tables",
            "pg_get_tabledef",
            "pg_index",
            "pg_constraint",
            "pg_type",
            "pg_attrdef",
            "pg_input_error_info",
            "pg_sequences",
            "pg_settings",
            "pg_extension",
            "pg_policy",
            "pg_trigger",
            "pg_statistic_ext",
            "pg_inherits",
            "pg_rewrite",
            "pg_foreign_table",
            "pg_partitioned_table",
            "pg_collation",
            "pg_description",
            "pg_publication",
            "pg_publication_namespace",
            "pg_publication_rel",
        ];
        for mvcc in [false, true] {
            let db = crate::session::open_database_with_io(
                Arc::new(turso_core::MemoryIO::new()),
                "catalog.db",
                crate::OpenFlags::default(),
                crate::DatabaseOpts::new(),
            )
            .unwrap();
            let conn = db.connect().unwrap();
            if mvcc {
                conn.pragma_update("journal_mode", "'mvcc'").unwrap();
            }
            for name in catalog_names {
                assert!(
                    matches!(
                        conn.current_schema().get_table(name).as_deref(),
                        Some(Table::Virtual(_))
                    ),
                    "{name}"
                );
            }
            assert_eq!(
                conn.prepare("SELECT setting FROM pg_settings WHERE name = 'search_path'")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap(),
                vec![vec![Value::build_text("\"$user\", public")]]
            );
            let mut namespaces = conn
                .prepare("SELECT nspname FROM pg_namespace() ORDER BY nspname")
                .unwrap();
            assert_eq!(
                namespaces.run_collect_rows().unwrap(),
                vec![
                    vec![Value::build_text("information_schema")],
                    vec![Value::build_text("pg_catalog")],
                    vec![Value::build_text("public")],
                ]
            );
            let mut tables = conn
                .prepare("SELECT relname FROM pg_class WHERE relname = 'second_connection'")
                .unwrap();
            assert!(tables.run_collect_rows().unwrap().is_empty());
            let other = db.connect().unwrap();
            other
                .execute("CREATE TABLE second_connection (v INT)")
                .unwrap();
            tables.reset().unwrap();
            assert_eq!(
                tables.run_collect_rows().unwrap(),
                vec![vec![Value::build_text("second_connection")]]
            );
            conn.force_reparse_schema().unwrap();
            for name in catalog_names {
                assert!(
                    matches!(
                        conn.current_schema().get_table(name).as_deref(),
                        Some(Table::Virtual(_))
                    ),
                    "{name}"
                );
            }
            let mut error_info = conn
                .prepare("SELECT sql_error_code FROM pg_input_error_info('abc', 'integer')")
                .unwrap();
            assert_eq!(
                error_info.run_collect_rows().unwrap(),
                vec![vec![Value::build_text("22P02")]]
            );
            conn.execute("CREATE TABLE namespace_counts (n INT)")
                .unwrap();
            conn.execute("CREATE TRIGGER catalog_check AFTER INSERT ON second_connection BEGIN INSERT INTO namespace_counts SELECT COUNT(*) FROM pg_namespace LEFT JOIN pg_policy ON 1; END").unwrap();
            conn.execute("INSERT INTO second_connection VALUES (7)")
                .unwrap();
            let mut counts = conn.prepare("SELECT n FROM namespace_counts").unwrap();
            assert_eq!(
                counts.run_collect_rows().unwrap(),
                vec![vec![Value::from_i64(3)]]
            );
        }
    }

    #[test]
    fn table_definitions_resume_after_io_and_statement_reset() {
        use turso_core::{MemoryYieldIO, IO};

        let io = Arc::new(MemoryYieldIO::new());
        let db = crate::session::open_database_with_io(
            io.clone(),
            "tabledefs.db",
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        let mut expected = Vec::new();
        for i in 0..12 {
            let name = format!("catalog_{i:02}");
            let sql = format!(
                "CREATE TABLE {name} (item TEXT DEFAULT '{}', n INT DEFAULT {i})",
                "x".repeat(2048 + i * 31)
            );
            conn.execute(&sql).unwrap();
            expected.push(vec![
                Value::build_text("public"),
                Value::build_text(name),
                Value::build_text(sql),
            ]);
        }

        conn.execute("BEGIN").unwrap();
        conn.get_pager().clear_page_cache(false);
        let mut cursor = PgGetTableDefTable::new().open(conn.clone()).unwrap();
        let mut yields = 0;
        loop {
            match cursor.filter(&[], None, 0).unwrap() {
                IOResult::IO(completions) => {
                    yields += 1;
                    assert!(!completions.finished());
                    io.step().unwrap();
                    assert!(completions.finished());
                }
                IOResult::Done(has_rows) => {
                    assert!(has_rows);
                    break;
                }
            }
        }
        assert!(yields > 1, "schema scan should yield on multiple pages");
        cursor.rows.sort_by_key(|row| row[1].to_string());
        assert_eq!(cursor.rows, expected);
        drop(cursor);
        conn.execute("ROLLBACK").unwrap();

        let mut stmt = conn
            .prepare("SELECT schema_name, table_name, ddl FROM pg_get_tabledef ORDER BY table_name")
            .unwrap();
        conn.get_pager().clear_page_cache(false);
        conn.execute("SELECT COUNT(*) FROM pg_namespace").unwrap();
        assert!(matches!(stmt.step().unwrap(), StepResult::IO));
        assert!(matches!(stmt.step().unwrap(), StepResult::IO));
        stmt.reset().unwrap();
        conn.get_pager().clear_page_cache(false);
        conn.execute("SELECT COUNT(*) FROM pg_namespace").unwrap();
        let mut rows = Vec::new();
        let mut statement_yields = 0;
        loop {
            match stmt.step().unwrap() {
                StepResult::IO => {
                    statement_yields += 1;
                    let completions = stmt
                        .take_io_completions()
                        .expect("catalog I/O must reach its caller");
                    io.step().unwrap();
                    assert!(completions.finished());
                }
                StepResult::Row => rows.push(
                    stmt.row()
                        .unwrap()
                        .get_values()
                        .cloned()
                        .collect::<Vec<_>>(),
                ),
                StepResult::Done => break,
                other => panic!("unexpected catalog step: {other:?}"),
            }
        }
        assert!(statement_yields > 1);
        assert_eq!(rows, expected);
        stmt.reset().unwrap();
        assert_eq!(stmt.run_collect_rows().unwrap(), expected);
        conn.execute("CREATE TABLE after_catalog (v INT)").unwrap();
    }

    #[test]
    fn catalog_view_columns_survive_schema_refresh() {
        let expected = ["oid", "nspname", "nspowner", "nspacl"]
            .map(Value::build_text)
            .map(|value| vec![value])
            .to_vec();
        let columns = |conn: &Arc<Connection>| {
            conn.prepare("SELECT name FROM pragma_table_info('catalog_names') ORDER BY cid")
                .unwrap()
                .run_collect_rows()
                .unwrap()
        };
        let dir = tempdir().unwrap();
        for mvcc in [false, true] {
            let path = dir.path().join(format!("catalog-view-{mvcc}.db"));
            let io = Arc::new(PlatformIO::new().unwrap());
            let opts = crate::DatabaseOpts::new()
                .with_views(true)
                .with_experimental_mvcc_passive_checkpoint(true);
            {
                let db = crate::session::open_database_with_io(
                    io.clone(),
                    path.to_str().unwrap(),
                    crate::OpenFlags::default(),
                    opts,
                )
                .unwrap();
                let conn = db.connect().unwrap();
                if mvcc {
                    conn.pragma_update("journal_mode", "'mvcc'").unwrap();
                }
                conn.execute("CREATE VIEW catalog_names AS SELECT * FROM pg_namespace")
                    .unwrap();
                conn.execute("CREATE VIEW catalog_join AS WITH namespaces AS (SELECT * FROM pg_namespace) SELECT namespaces.nspname FROM namespaces JOIN pg_namespace USING (oid)").unwrap();
                assert_eq!(columns(&conn), expected);
                conn.force_reparse_schema().unwrap();
                assert_eq!(columns(&conn), expected);
            }
            let db = crate::session::open_database_with_io(
                io,
                path.to_str().unwrap(),
                crate::OpenFlags::default(),
                opts,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            assert_eq!(columns(&conn), expected);
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            conn.force_reparse_schema().unwrap();
            assert_eq!(columns(&conn), expected);
            assert_eq!(
                conn.prepare("SELECT nspname FROM catalog_join ORDER BY nspname")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap(),
                vec![
                    vec![Value::build_text("information_schema")],
                    vec![Value::build_text("pg_catalog")],
                    vec![Value::build_text("public")],
                ]
            );
        }
    }

    #[test]
    fn catalogs_available_in_secondary_databases() {
        let dir = tempdir().unwrap();
        let attached_path = dir.path().join("attached.db");
        let output_path = dir.path().join("vacuum.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let opts = crate::DatabaseOpts::new()
            .with_attach(true)
            .with_vacuum(true)
            .with_views(true);
        let db = crate::session::open_database_with_io(
            io.clone(),
            dir.path().join("main.db").to_str().unwrap(),
            crate::OpenFlags::default(),
            opts,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute(format!("ATTACH '{}' AS aux", attached_path.display()))
            .unwrap();
        conn.execute("CREATE TABLE aux.data (v INT)").unwrap();
        conn.execute("CREATE TABLE aux.counts (n INT)").unwrap();
        assert_eq!(
            conn.prepare("SELECT COUNT(*) FROM aux.pg_namespace")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(4)]]
        );
        conn.execute("CREATE TRIGGER aux.catalog_check AFTER INSERT ON data BEGIN INSERT INTO counts SELECT COUNT(*) FROM pg_namespace; END").unwrap();
        conn.execute("INSERT INTO aux.data VALUES (5)").unwrap();
        assert_eq!(
            conn.prepare("SELECT n FROM aux.counts")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(4)]]
        );
        let attached = crate::session::open_database_with_io(
            io.clone(),
            attached_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            opts,
        )
        .unwrap();
        assert_eq!(
            attached
                .connect()
                .unwrap()
                .prepare("SELECT COUNT(*) FROM pg_namespace")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(3)]]
        );
        for temp_store in ["MEMORY", "FILE"] {
            let temp_conn = db.connect().unwrap();
            temp_conn
                .execute(format!("PRAGMA temp_store = {temp_store}"))
                .unwrap();
            temp_conn.execute("BEGIN").unwrap();
            temp_conn
                .execute("CREATE TEMP TABLE temp_data (v INT)")
                .unwrap();
            temp_conn.execute("ROLLBACK").unwrap();
            assert_eq!(
                temp_conn
                    .prepare("SELECT COUNT(*) FROM temp.pg_namespace")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap(),
                vec![vec![Value::from_i64(3)]]
            );
        }
        conn.execute("CREATE VIEW catalog_names AS SELECT * FROM pg_namespace")
            .unwrap();
        conn.execute(format!("VACUUM INTO '{}'", output_path.display()))
            .unwrap();
        let output = crate::session::open_database_with_io(
            io,
            output_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            opts,
        )
        .unwrap();
        assert_eq!(
            output
                .connect()
                .unwrap()
                .prepare("SELECT COUNT(*) FROM catalog_names")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(3)]]
        );
    }

    #[test]

    fn test_pg_namespace_query() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = crate::Connection::connect(&db).unwrap();

        // Query pg_namespace
        let mut stmt = conn.prepare("SELECT * FROM pg_namespace").unwrap();

        let mut found_pg_catalog = false;
        let mut found_public = false;
        let mut found_information_schema = false;

        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let Value::Text(nspname) = row.get_value(1) {
                        match nspname.value.as_ref() {
                            "pg_catalog" => found_pg_catalog = true,
                            "public" => found_public = true,
                            "information_schema" => found_information_schema = true,
                            _ => {}
                        }
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }

        assert!(found_pg_catalog, "pg_catalog namespace not found");
        assert!(found_public, "public namespace not found");
        assert!(
            found_information_schema,
            "information_schema namespace not found"
        );
    }

    #[test]

    fn test_pg_class_lists_user_tables() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        // Create test tables in SQLite mode (default)
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE products (id INTEGER, title TEXT, price REAL)")
            .unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Query pg_class for regular tables
        let mut stmt = conn
            .prepare("SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = 2200")
            .unwrap();

        let mut tables = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let Value::Text(relname) = row.get_value(0) {
                        tables.push(relname.to_string());
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }

        // Should find our three tables
        assert!(
            tables.contains(&"users".to_string()),
            "users table not found"
        );
        assert!(
            tables.contains(&"products".to_string()),
            "products table not found"
        );
        assert!(
            tables.contains(&"orders".to_string()),
            "orders table not found"
        );
        assert_eq!(tables.len(), 3, "Expected exactly 3 tables");
    }

    #[test]

    fn test_pg_class_table_details() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        // Create a test table with known columns
        conn.execute("CREATE TABLE test_table (id INTEGER, name TEXT, value REAL)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Query pg_class for table details
        let mut stmt = conn
            .prepare(
                "SELECT oid, relname, relkind, relnatts
             FROM pg_class
             WHERE relname = 'test_table'",
            )
            .unwrap();

        if let StepResult::Row = stmt.step().unwrap() {
            let row = stmt.row().unwrap();
            let oid = if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                *v
            } else {
                panic!("Expected OID")
            };
            let relname = if let Value::Text(v) = row.get_value(1) {
                v
            } else {
                panic!("Expected relname")
            };
            let relkind = if let Value::Text(v) = row.get_value(2) {
                v
            } else {
                panic!("Expected relkind")
            };
            let relnatts = if let Value::Numeric(Numeric::Integer(v)) = row.get_value(3) {
                *v
            } else {
                panic!("Expected relnatts")
            };

            assert!(oid >= 16384, "OID should be >= 16384 for user tables");
            assert_eq!(relname.value, "test_table", "Table name should match");
            assert_eq!(
                relkind.value, "r",
                "relkind should be 'r' for regular table"
            );
            assert_eq!(relnatts, 3, "Table should have 3 columns");
        } else {
            panic!("test_table not found in pg_class");
        }
    }

    #[test]

    fn test_sqlite_tables_hidden_in_postgres_mode() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        // Create a test table
        conn.execute("CREATE TABLE test_table (id INTEGER)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Try to query sqlite_master - should fail
        let result = conn.prepare("SELECT * FROM sqlite_master");
        assert!(
            result.is_err(),
            "sqlite_master should not be accessible in PostgreSQL mode"
        );

        // Try to query sqlite_schema - should also fail
        let result = conn.prepare("SELECT * FROM sqlite_schema");
        assert!(
            result.is_err(),
            "sqlite_schema should not be accessible in PostgreSQL mode"
        );
    }

    #[test]

    fn test_postgres_tables_hidden_in_sqlite_mode() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        // Open with the SQLite dialect, whose catalog has no pg_* tables.
        let db = Database::open_file_with_flags(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
            None,
            Arc::new(turso_core::SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        // Try to query pg_class - should fail
        let result = conn.prepare("SELECT * FROM pg_class");
        assert!(
            result.is_err(),
            "pg_class should not be accessible in SQLite mode"
        );

        // Try to query pg_namespace - should fail
        let result = conn.prepare("SELECT * FROM pg_namespace");
        assert!(
            result.is_err(),
            "pg_namespace should not be accessible in SQLite mode"
        );

        // sqlite_master should work
        let result = conn.prepare("SELECT * FROM sqlite_master");
        assert!(
            result.is_ok(),
            "sqlite_master should be accessible in SQLite mode"
        );
    }

    /// User-created tables are visible through both the SQLite-side catalog
    /// (`sqlite_master`) on a SQLite-mode connection and the PG-side catalog
    /// (`pg_class`) on a PostgreSQL-mode connection. Each direction uses its
    /// own connection — dialect is fixed per connection and we don't
    /// hot-swap at runtime.
    #[test]
    fn user_table_is_listed_in_dialect_specific_catalog() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();

        // SQLite-mode connection: sqlite_master sees the user table.
        let sqlite_conn = db.connect().unwrap();
        sqlite_conn
            .execute("CREATE TABLE users (id INTEGER, name TEXT)")
            .unwrap();
        let mut stmt = sqlite_conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .unwrap();
        let mut found = false;
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    if let Value::Text(name) = stmt.row().unwrap().get_value(0) {
                        if name.value == "users" {
                            found = true;
                        }
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }
        assert!(found, "users table not found in sqlite_master");

        // PostgreSQL-mode connection: pg_class sees the user table.
        let pg_conn = crate::Connection::connect(&db).unwrap();
        let mut stmt = pg_conn
            .prepare("SELECT relname FROM pg_class WHERE relkind = 'r'")
            .unwrap();
        let mut found = false;
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    if let Value::Text(name) = stmt.row().unwrap().get_value(0) {
                        if name.value == "users" {
                            found = true;
                        }
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }
        assert!(found, "users table not found in pg_class");
    }

    #[test]

    fn test_pg_class_with_where_constraints() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        // Create multiple tables
        conn.execute("CREATE TABLE table1 (id INTEGER)").unwrap();
        conn.execute("CREATE TABLE table2 (id INTEGER, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE table3 (id INTEGER, name TEXT, value REAL)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Test various WHERE clause combinations

        // Test 1: Filter by relkind = 'r'
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM pg_class WHERE relkind = 'r'")
            .unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) {
                    assert_eq!(*count, 3, "Should have 3 regular tables");
                }
            }
            _ => panic!("Expected row from COUNT query"),
        }

        // Test 2: Filter by relnamespace = 2200 (public schema)
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM pg_class WHERE relnamespace = 2200")
            .unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) {
                    assert_eq!(*count, 3, "Should have 3 tables in public schema");
                }
            }
            _ => panic!("Expected row from COUNT query"),
        }

        // Test 3: Combined filters
        let mut stmt = conn.prepare("SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = 2200 ORDER BY relname").unwrap();
        let mut tables = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let Value::Text(name) = row.get_value(0) {
                        tables.push(name.to_string());
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }
        assert_eq!(tables, vec!["table1", "table2", "table3"]);
    }

    #[test]
    fn test_pg_tables_lists_user_tables() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        // Create test tables
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Query pg_tables
        let mut stmt = conn
            .prepare("SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public'")
            .unwrap();

        let mut tables = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let (Value::Text(schema), Value::Text(name)) =
                        (row.get_value(0), row.get_value(1))
                    {
                        assert_eq!(schema.as_str(), "public");
                        tables.push(name.to_string());
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }

        tables.sort();
        assert_eq!(tables, vec!["orders", "users"]);
    }

    #[test]
    fn test_pg_tables_excludes_internal_tables() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE mydata (id INTEGER PRIMARY KEY)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        let mut stmt = conn.prepare("SELECT tablename FROM pg_tables").unwrap();

        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let Value::Text(name) = row.get_value(0) {
                        assert!(
                            !name.as_str().starts_with("sqlite_"),
                            "internal table {} should not appear in pg_tables",
                            name.as_str()
                        );
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }
    }
}
