use crate::functions::validate_pg_input;
use rustc_hash::FxHashMap as HashMap;
use std::sync::Arc;
use turso_core::{
    native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule},
    schema::{BTreeTable, Schema, Table},
    Connection, Dialect, Func, IOResult, LimboError, OpenOptions, Result, Statement, Value,
};
use turso_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabKind};
use turso_parser::ast::RefAct;

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

    fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
        turso_core::dialect::sqlite::register_builtin_catalog(schema, enable_custom_types)
    }

    fn register_native_extensions(&self, options: OpenOptions) -> OpenOptions {
        register_catalog_modules(options)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        if crate::functions::resolve_scalar(name, arg_count) {
            return Ok(Some(Func::Dialect(name.to_string())));
        }
        turso_core::dialect::sqlite::resolve_builtin_function(name, arg_count)
    }

    fn exec_scalar_function(&self, conn: &Connection, name: &str, args: &[Value]) -> Result<Value> {
        crate::functions::exec_scalar(conn, name, args)
    }

    fn requires_custom_types(&self) -> bool {
        true
    }
}

pub fn is_catalog_table_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "pg_class"
            | "pg_namespace"
            | "pg_attribute"
            | "pg_roles"
            | "pg_proc"
            | "pg_database"
            | "pg_am"
            | "pg_type"
            | "pg_collation"
            | "pg_attrdef"
            | "pg_description"
            | "pg_publication"
            | "pg_publication_namespace"
            | "pg_publication_rel"
            | "pg_sequences"
            | "pg_constraint"
            | "pg_index"
            | "pg_inherits"
            | "pg_rewrite"
            | "pg_foreign_table"
            | "pg_partitioned_table"
            | "pg_trigger"
            | "pg_policy"
            | "pg_input_error_info"
            | "pg_get_tabledef"
            | "pg_tables"
    )
}

pub fn encode_pg_schema_sql(sql: &str) -> String {
    format!("{STORED_PG_SCHEMA_PREFIX}{sql}")
}

pub fn decode_stored_pg_schema_sql(sql: &str) -> Option<&str> {
    sql.strip_prefix(STORED_PG_SCHEMA_PREFIX)
}

/// Returns an iterator of (table_name, table_ref) for user tables in deterministic order.
/// Both pg_class and pg_attribute must use this function to ensure consistent OID assignment.
fn user_tables_sorted(schema: &Schema) -> Vec<(&String, &Arc<Table>)> {
    let mut tables: Vec<_> = schema
        .tables
        .iter()
        .filter(|(name, table)| {
            // Skip system tables
            if name.starts_with("sqlite_")
                || name.starts_with("pg_")
                || name.starts_with("pragma_")
                || name.starts_with("json_")
            {
                return false;
            }
            // Skip virtual tables and subqueries
            matches!(table.as_ref(), Table::BTree(_))
        })
        .collect();
    tables.sort_by_key(|(name, _)| *name);
    tables
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

/// Build a mapping from table name to OID for all user tables.
fn table_oid_map(schema: &Schema) -> HashMap<String, i64> {
    let tables = user_tables_sorted(schema);
    let mut map = HashMap::default();
    for (i, (name, _)) in tables.iter().enumerate() {
        map.insert((*name).clone(), USER_TABLE_OID_START + i as i64);
    }
    map
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
pub struct PgClassTable;

impl PgClassTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        // PostgreSQL pg_class columns (simplified subset)
        "CREATE TABLE pg_class (
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
            relhasindex INTEGER,
            relisshared INTEGER,
            relpersistence TEXT,
            relkind TEXT,
            relnatts INTEGER,
            relchecks INTEGER,
            relhasrules INTEGER,
            relhastriggers INTEGER,
            relhassubclass INTEGER,
            relrowsecurity INTEGER,
            relforcerowsecurity INTEGER,
            relispopulated INTEGER,
            relreplident TEXT,
            relispartition INTEGER,
            relrewrite INTEGER,
            relfrozenxid INTEGER,
            relminmxid INTEGER,
            relacl TEXT,
            reloptions TEXT,
            relpartbound TEXT
        )"
        .to_string()
    }
}

impl VirtualTable for PgClassTable {
    type Cursor = PgClassCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgClassCursor::new(conn))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        // Create constraint usages for each constraint
        let constraint_usages = constraints
            .iter()
            .map(|_constraint| turso_ext::ConstraintUsage {
                argv_index: None, // We'll handle filtering ourselves
                omit: false,
            })
            .collect();

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1000.0,
            estimated_rows: 100,
            constraint_usages,
        })
    }
}

pub struct PgClassCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgClassCursor {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            rows: Vec::new(),
            current_row: 0,
        }
    }

    fn load_from_sqlite_master(&mut self) -> Result<(), LimboError> {
        let schema = self.conn.current_schema();
        self.rows.clear();

        let tables = user_tables_sorted(&schema);
        let num_tables = tables.len() as i64;

        for (i, (table_name, table)) in tables.iter().enumerate() {
            let btree = match table.as_ref() {
                Table::BTree(bt) => bt,
                _ => continue,
            };
            let table_oid = USER_TABLE_OID_START + i as i64;
            let relnatts = btree.columns().len() as i64;
            let relhasindex = if schema.get_indices(table_name).next().is_some() {
                1i64
            } else {
                0
            };
            let relchecks = btree.check_constraints.len() as i64;

            self.rows.push(vec![
                Value::from_i64(table_oid),                // oid
                Value::Text((*table_name).clone().into()), // relname
                Value::from_i64(2200),                     // relnamespace (public schema)
                Value::from_i64(0),                        // reltype
                Value::from_i64(0),                        // reloftype
                Value::from_i64(10),                       // relowner
                Value::from_i64(2),                        // relam (heap)
                Value::from_i64(0),                        // relfilenode
                Value::from_i64(0),                        // reltablespace
                Value::from_i64(1),                        // relpages
                Value::from_f64(0.0),                      // reltuples
                Value::from_i64(0),                        // relallvisible
                Value::from_i64(0),                        // reltoastrelid
                Value::from_i64(relhasindex),              // relhasindex
                Value::from_i64(0),                        // relisshared
                Value::Text("p".into()),                   // relpersistence (permanent)
                Value::Text("r".into()),                   // relkind (regular table)
                Value::from_i64(relnatts),                 // relnatts
                Value::from_i64(relchecks),                // relchecks
                Value::from_i64(0),                        // relhasrules
                Value::from_i64(0),                        // relhastriggers
                Value::from_i64(0),                        // relhassubclass
                Value::from_i64(0),                        // relrowsecurity
                Value::from_i64(0),                        // relforcerowsecurity
                Value::from_i64(1),                        // relispopulated
                Value::Text("d".into()),                   // relreplident
                Value::from_i64(0),                        // relispartition
                Value::from_i64(0),                        // relrewrite
                Value::from_i64(0),                        // relfrozenxid
                Value::from_i64(0),                        // relminmxid
                Value::Null,                               // relacl
                Value::Null,                               // reloptions
                Value::Null,                               // relpartbound
            ]);
        }

        // Add index rows (relkind='i')
        let mut index_oid = USER_TABLE_OID_START + num_tables;
        for (table_name, _) in &tables {
            for idx in schema.get_indices(table_name) {
                if idx.ephemeral {
                    continue;
                }
                let indnatts = idx.columns.len() as i64;
                self.rows.push(vec![
                    Value::from_i64(index_oid),           // oid
                    Value::Text(idx.name.clone().into()), // relname
                    Value::from_i64(2200),                // relnamespace (public)
                    Value::from_i64(0),                   // reltype
                    Value::from_i64(0),                   // reloftype
                    Value::from_i64(10),                  // relowner
                    Value::from_i64(403),                 // relam (btree)
                    Value::from_i64(0),                   // relfilenode
                    Value::from_i64(0),                   // reltablespace
                    Value::from_i64(1),                   // relpages
                    Value::from_f64(0.0),                 // reltuples
                    Value::from_i64(0),                   // relallvisible
                    Value::from_i64(0),                   // reltoastrelid
                    Value::from_i64(0),                   // relhasindex
                    Value::from_i64(0),                   // relisshared
                    Value::Text("p".into()),              // relpersistence
                    Value::Text("i".into()),              // relkind (index)
                    Value::from_i64(indnatts),            // relnatts
                    Value::from_i64(0),                   // relchecks
                    Value::from_i64(0),                   // relhasrules
                    Value::from_i64(0),                   // relhastriggers
                    Value::from_i64(0),                   // relhassubclass
                    Value::from_i64(0),                   // relrowsecurity
                    Value::from_i64(0),                   // relforcerowsecurity
                    Value::from_i64(1),                   // relispopulated
                    Value::Text("d".into()),              // relreplident
                    Value::from_i64(0),                   // relispartition
                    Value::from_i64(0),                   // relrewrite
                    Value::from_i64(0),                   // relfrozenxid
                    Value::from_i64(0),                   // relminmxid
                    Value::Null,                          // relacl
                    Value::Null,                          // reloptions
                    Value::Null,                          // relpartbound
                ]);
                index_oid += 1;
            }
        }

        Ok(())
    }
}

impl VirtualTableCursor for PgClassCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        // Reset cursor and load data
        self.current_row = 0;
        self.rows.clear();
        self.load_from_sqlite_master()?;

        // Return true if we have any rows
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

/// Virtual table implementation for pg_catalog.pg_namespace
/// Maps schema information to PostgreSQL's pg_namespace
#[derive(Debug)]
pub struct PgNamespaceTable;

impl PgNamespaceTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_namespace (
            oid INTEGER,
            nspname TEXT,
            nspowner INTEGER,
            nspacl TEXT
        )"
        .to_string()
    }
}

impl VirtualTable for PgNamespaceTable {
    type Cursor = PgNamespaceCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgNamespaceCursor::new(conn))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let constraint_usages = constraints
            .iter()
            .map(|_constraint| turso_ext::ConstraintUsage {
                argv_index: None, // We'll handle filtering ourselves
                omit: false,
            })
            .collect();

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 10.0,
            estimated_rows: 5,
            constraint_usages,
        })
    }
}

pub struct PgNamespaceCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgNamespaceCursor {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            rows: Vec::new(),
            current_row: 0,
        }
    }

    fn load_namespaces(&mut self) -> Result<(), LimboError> {
        // PostgreSQL standard namespaces
        self.rows = vec![
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
        let schema_names = self.conn.attached_database_names();
        let mut oid = 16384i64;
        for name in schema_names {
            self.rows.push(vec![
                Value::from_i64(oid),
                Value::build_text(name),
                Value::from_i64(10), // nspowner (bootstrap superuser)
                Value::Null,         // nspacl
            ]);
            oid += 1;
        }
        Ok(())
    }
}

impl VirtualTableCursor for PgNamespaceCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.rows.clear();
        self.load_namespaces()?;
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

/// Virtual table implementation for pg_catalog.pg_attribute
/// Maps column information to PostgreSQL's pg_attribute
#[derive(Debug)]
pub struct PgAttributeTable;

impl PgAttributeTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_attribute (
            attrelid INTEGER,
            attname TEXT,
            atttypid INTEGER,
            attstattarget INTEGER,
            attlen INTEGER,
            attnum INTEGER,
            attndims INTEGER,
            attcacheoff INTEGER,
            atttypmod INTEGER,
            attbyval INTEGER,
            attstorage TEXT,
            attalign TEXT,
            attnotnull INTEGER,
            atthasdef INTEGER,
            atthasmissing INTEGER,
            attidentity TEXT,
            attgenerated TEXT,
            attisdropped INTEGER,
            attislocal INTEGER,
            attinhcount INTEGER,
            attcollation INTEGER,
            attacl TEXT,
            attoptions TEXT,
            attfdwoptions TEXT,
            attmissingval TEXT
        )"
        .to_string()
    }
}

impl VirtualTable for PgAttributeTable {
    type Cursor = PgAttributeCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgAttributeCursor::new(conn))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let constraint_usages = constraints
            .iter()
            .map(|_constraint| turso_ext::ConstraintUsage {
                argv_index: None, // We'll handle filtering ourselves
                omit: false,
            })
            .collect();

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1000.0,
            estimated_rows: 1000,
            constraint_usages,
        })
    }
}

pub struct PgAttributeCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgAttributeCursor {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            rows: Vec::new(),
            current_row: 0,
        }
    }

    fn load_attributes(&mut self) -> Result<(), LimboError> {
        let schema = self.conn.current_schema();
        self.rows.clear();

        let mut oid_counter = USER_TABLE_OID_START;

        for (_, table) in user_tables_sorted(&schema) {
            let table_oid = oid_counter;
            oid_counter += 1;

            let columns = table.columns();
            for (i, col) in columns.iter().enumerate() {
                let col_name = col.name.clone().unwrap_or_default();
                let type_oid = sqlite_type_to_pg_oid(&col.ty_str);
                let attnum = (i + 1) as i64; // 1-based
                let notnull = if col.notnull() { 1i64 } else { 0i64 };
                let has_def = if col.default.is_some() { 1i64 } else { 0i64 };

                self.rows.push(vec![
                    Value::from_i64(table_oid),   // attrelid
                    Value::Text(col_name.into()), // attname
                    Value::from_i64(type_oid),    // atttypid
                    Value::from_i64(-1),          // attstattarget
                    Value::from_i64(-1),          // attlen
                    Value::from_i64(attnum),      // attnum
                    Value::from_i64(0),           // attndims
                    Value::from_i64(-1),          // attcacheoff
                    Value::from_i64(-1),          // atttypmod
                    Value::from_i64(1),           // attbyval
                    Value::Text("p".into()),      // attstorage (plain)
                    Value::Text("i".into()),      // attalign (int)
                    Value::from_i64(notnull),     // attnotnull
                    Value::from_i64(has_def),     // atthasdef
                    Value::from_i64(0),           // atthasmissing
                    Value::Text("".into()),       // attidentity
                    Value::Text("".into()),       // attgenerated
                    Value::from_i64(0),           // attisdropped
                    Value::from_i64(1),           // attislocal
                    Value::from_i64(0),           // attinhcount
                    Value::from_i64(0),           // attcollation
                    Value::Null,                  // attacl
                    Value::Null,                  // attoptions
                    Value::Null,                  // attfdwoptions
                    Value::Null,                  // attmissingval
                ]);
            }
        }

        Ok(())
    }
}

impl VirtualTableCursor for PgAttributeCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.rows.clear();
        self.load_attributes()?;
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

/// Virtual table implementation for pg_catalog.pg_roles
/// Stub: returns a single hardcoded "turso" superuser role.
/// TODO: replace with real role data when authentication is implemented.
#[derive(Debug)]
pub struct PgRolesTable;

impl PgRolesTable {
    pub fn new() -> Self {
        Self
    }

    /// Stub: returns a single default superuser role.
    /// Replace this method with real role lookup when auth is implemented.
    fn roles() -> Vec<Vec<Value>> {
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

    fn schema() -> String {
        "CREATE TABLE pg_roles (
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
        )"
        .to_string()
    }
}

impl VirtualTable for PgRolesTable {
    type Cursor = PgRolesCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgRolesCursor {
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
            estimated_cost: 10.0,
            estimated_rows: 1,
            constraint_usages,
        })
    }
}

pub struct PgRolesCursor {
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl VirtualTableCursor for PgRolesCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.rows = PgRolesTable::roles();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

/// Virtual table implementation for pg_catalog.pg_proc
/// Populated from the same function registry as PRAGMA function_list.
#[derive(Debug)]
struct PgProcTable;

impl PgProcTable {
    fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_proc (
            oid INTEGER,
            proname TEXT,
            pronamespace INTEGER,
            proowner INTEGER,
            prolang INTEGER,
            procost REAL,
            prorows REAL,
            provariadic INTEGER,
            prokind TEXT,
            prosecdef INTEGER,
            proleakproof INTEGER,
            proisstrict INTEGER,
            proretset INTEGER,
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
            proacl TEXT
        )"
        .to_string()
    }
}

impl VirtualTable for PgProcTable {
    type Cursor = PgProcCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgProcCursor {
            conn,
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
            estimated_cost: 100.0,
            estimated_rows: 100,
            constraint_usages,
        })
    }
}

struct PgProcCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgProcCursor {
    fn load_functions(&mut self) {
        use crate::Func;

        self.rows.clear();
        let mut oid = 1i64;

        // Built-in functions from the same registry as PRAGMA function_list
        for entry in Func::builtin_function_list() {
            let prokind = match entry.func_type {
                "a" => "a", // aggregate
                "w" => "w", // window
                _ => "f",   // function
            };
            let provolatile = if entry.deterministic { "i" } else { "v" };

            self.rows.push(vec![
                Value::from_i64(oid),               // oid
                Value::build_text(entry.name),      // proname
                Value::from_i64(2200),              // pronamespace (public)
                Value::from_i64(10),                // proowner
                Value::from_i64(14),                // prolang (SQL)
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
        for (name, is_agg, argc, _deterministic) in self.conn.get_syms_functions() {
            let prokind = if is_agg { "a" } else { "f" };

            self.rows.push(vec![
                Value::from_i64(oid),         // oid
                Value::build_text(name),      // proname
                Value::from_i64(2200),        // pronamespace (public)
                Value::from_i64(10),          // proowner
                Value::from_i64(13),          // prolang (C)
                Value::from_f64(1.0),         // procost
                Value::from_f64(0.0),         // prorows
                Value::from_i64(0),           // provariadic
                Value::build_text(prokind),   // prokind
                Value::from_i64(0),           // prosecdef
                Value::from_i64(0),           // proleakproof
                Value::from_i64(0),           // proisstrict
                Value::from_i64(0),           // proretset
                Value::build_text("v"),       // provolatile (volatile)
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
    }
}

impl VirtualTableCursor for PgProcCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.load_functions();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

/// Virtual table implementation for pg_catalog.pg_database
/// Returns one row per database, deriving the name from the database file path.
#[derive(Debug)]
struct PgDatabaseTable;

impl PgDatabaseTable {
    fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_database (
            oid INTEGER,
            datname TEXT,
            datdba INTEGER,
            encoding INTEGER,
            datlocprovider TEXT,
            datistemplate INTEGER,
            datallowconn INTEGER,
            datconnlimit INTEGER,
            datfrozenxid INTEGER,
            datminmxid INTEGER,
            dattablespace INTEGER,
            datcollate TEXT,
            datctype TEXT,
            daticulocale TEXT,
            daticurules TEXT,
            datacl TEXT
        )"
        .to_string()
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

impl VirtualTable for PgDatabaseTable {
    type Cursor = PgDatabaseCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgDatabaseCursor {
            conn,
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
            estimated_cost: 10.0,
            estimated_rows: 1,
            constraint_usages,
        })
    }
}

struct PgDatabaseCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl VirtualTableCursor for PgDatabaseCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        let db_name = db_name_from_path(self.conn.db_file_path());
        self.rows = vec![vec![
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
        ]];
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

/// Virtual table implementation for pg_catalog.pg_am
/// Stub: returns two access methods (heap and btree).
#[derive(Debug)]
pub struct PgAmTable;

impl PgAmTable {
    pub fn new() -> Self {
        Self
    }

    fn rows() -> Vec<Vec<Value>> {
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

    fn schema() -> String {
        "CREATE TABLE pg_am (
            oid INTEGER,
            amname TEXT,
            amhandler TEXT,
            amtype TEXT
        )"
        .to_string()
    }
}

impl VirtualTable for PgAmTable {
    type Cursor = PgAmCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgAmCursor {
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
            estimated_cost: 10.0,
            estimated_rows: 2,
            constraint_usages,
        })
    }
}

pub struct PgAmCursor {
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl VirtualTableCursor for PgAmCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.rows = PgAmTable::rows();
        Ok(IOResult::Done(!self.rows.is_empty()))
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

    fn allows_reads_in_triggers(&self) -> bool {
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
pub struct PgTablesTable;

impl PgTablesTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_tables (
            schemaname TEXT,
            tablename TEXT,
            tableowner TEXT,
            tablespace TEXT,
            hasindexes INTEGER,
            hasrules INTEGER,
            hastriggers INTEGER,
            rowsecurity INTEGER
        )"
        .to_string()
    }
}

impl VirtualTable for PgTablesTable {
    type Cursor = PgTablesCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgTablesCursor::new(conn))
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
            estimated_cost: 1000.0,
            estimated_rows: 100,
            constraint_usages,
        })
    }
}

pub struct PgTablesCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgTablesCursor {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            rows: Vec::new(),
            current_row: 0,
        }
    }

    fn load_tables(&mut self) -> Result<(), LimboError> {
        let schema = self.conn.current_schema();
        self.rows.clear();

        for (table_name, _) in user_tables_sorted(&schema) {
            self.rows.push(vec![
                Value::Text("public".into()),           // schemaname
                Value::Text(table_name.clone().into()), // tablename
                Value::Text("turso".into()),            // tableowner
                Value::Null,                            // tablespace
                Value::from_i64(0),                     // hasindexes
                Value::from_i64(0),                     // hasrules
                Value::from_i64(0),                     // hastriggers
                Value::from_i64(0),                     // rowsecurity
            ]);
        }

        Ok(())
    }
}

impl VirtualTableCursor for PgTablesCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.rows.clear();
        self.load_tables()?;
        Ok(IOResult::Done(!self.rows.is_empty()))
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

const PG_TYPE_SQL: &str = "CREATE TABLE pg_type (oid INTEGER, typname TEXT, typnamespace INTEGER, typowner INTEGER, typlen INTEGER, typbyval INTEGER, typtype TEXT, typcategory TEXT, typispreferred INTEGER, typisdefined INTEGER, typdelim TEXT, typrelid INTEGER, typsubscript TEXT, typelem INTEGER, typarray INTEGER, typinput TEXT, typoutput TEXT, typreceive TEXT, typsend TEXT, typmodin TEXT, typmodout TEXT, typanalyze TEXT, typalign TEXT, typstorage TEXT, typnotnull INTEGER, typbasetype INTEGER, typtypmod INTEGER, typndims INTEGER, typcollation INTEGER, typdefaultbin TEXT, typdefault TEXT, typacl TEXT)";

#[derive(Debug)]
pub struct PgTypeTable;

impl PgTypeTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        PG_TYPE_SQL.to_string()
    }
}

impl VirtualTable for PgTypeTable {
    type Cursor = PgTypeCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgTypeCursor {
            conn,
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
            estimated_cost: 100.0,
            estimated_rows: 50,
            constraint_usages,
        })
    }
}

pub struct PgTypeCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgTypeCursor {
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

    fn load_types(&mut self) {
        self.rows.clear();

        // Static base types
        for t in PG_BASE_TYPES {
            self.rows.push(Self::make_type_row(t));
        }

        // Static array types
        for &(oid, name, typelem) in PG_ARRAY_TYPES {
            self.rows.push(Self::make_type_row(&PgTypeInfo {
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
        let schema = self.conn.current_schema();
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
            self.rows.push(vec![
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
    }
}

impl VirtualTableCursor for PgTypeCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.load_types();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_index
// ──────────────────────────────────────────────────────────────────────

const PG_INDEX_SQL: &str = "CREATE TABLE pg_index (indexrelid INTEGER, indrelid INTEGER, indnatts INTEGER, indnkeyatts INTEGER, indisunique INTEGER, indisprimary INTEGER, indisexclusion INTEGER, indimmediate INTEGER, indisclustered INTEGER, indisvalid INTEGER, indcheckxmin INTEGER, indisready INTEGER, indislive INTEGER, indisreplident INTEGER, indkey TEXT, indcollation TEXT, indclass TEXT, indoption TEXT, indexprs TEXT, indpred TEXT)";

#[derive(Debug)]
pub struct PgIndexTable;

impl PgIndexTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        PG_INDEX_SQL.to_string()
    }
}

impl VirtualTable for PgIndexTable {
    type Cursor = PgIndexCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgIndexCursor {
            conn,
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
            estimated_cost: 100.0,
            estimated_rows: 50,
            constraint_usages,
        })
    }
}

pub struct PgIndexCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgIndexCursor {
    fn load_indexes(&mut self) {
        let schema = self.conn.current_schema();
        self.rows.clear();

        let tables = user_tables_sorted(&schema);
        let num_tables = tables.len() as i64;
        let tbl_oid_map = table_oid_map(&schema);

        let mut index_oid = USER_TABLE_OID_START + num_tables;
        for (table_name, _) in &tables {
            let table_oid = tbl_oid_map.get(*table_name).copied().unwrap_or(0);
            for idx in schema.get_indices(table_name) {
                if idx.ephemeral {
                    continue;
                }
                let indnatts = idx.columns.len() as i64;
                let indisunique = i64::from(idx.unique);
                let indisprimary = i64::from(
                    idx.name
                        .starts_with(PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX)
                        && idx.unique,
                );

                // Build indkey: space-separated 1-based column positions (0 for expression cols)
                let indkey: String = idx
                    .columns
                    .iter()
                    .map(|col| {
                        if col.expr.is_some() {
                            "0".to_string()
                        } else {
                            (col.pos_in_table + 1).to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let indpred = idx
                    .where_clause
                    .as_ref()
                    .map(|e| Value::build_text(e.to_string()))
                    .unwrap_or(Value::Null);

                let indexprs = if idx.columns.iter().any(|c| c.expr.is_some()) {
                    let exprs: Vec<String> = idx
                        .columns
                        .iter()
                        .filter_map(|c| c.expr.as_ref().map(|e| e.to_string()))
                        .collect();
                    Value::build_text(exprs.join(", "))
                } else {
                    Value::Null
                };

                self.rows.push(vec![
                    Value::from_i64(index_oid),    // indexrelid
                    Value::from_i64(table_oid),    // indrelid
                    Value::from_i64(indnatts),     // indnatts
                    Value::from_i64(indnatts),     // indnkeyatts
                    Value::from_i64(indisunique),  // indisunique
                    Value::from_i64(indisprimary), // indisprimary
                    Value::from_i64(0),            // indisexclusion
                    Value::from_i64(1),            // indimmediate
                    Value::from_i64(0),            // indisclustered
                    Value::from_i64(1),            // indisvalid
                    Value::from_i64(0),            // indcheckxmin
                    Value::from_i64(1),            // indisready
                    Value::from_i64(1),            // indislive
                    Value::from_i64(0),            // indisreplident
                    Value::build_text(indkey),     // indkey
                    Value::Null,                   // indcollation
                    Value::Null,                   // indclass
                    Value::Null,                   // indoption
                    indexprs,                      // indexprs
                    indpred,                       // indpred
                ]);
                index_oid += 1;
            }
        }
    }
}

impl VirtualTableCursor for PgIndexCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.load_indexes();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_constraint
// ──────────────────────────────────────────────────────────────────────

const PG_CONSTRAINT_SQL: &str = "CREATE TABLE pg_constraint (oid INTEGER, conname TEXT, connamespace INTEGER, contype TEXT, condeferrable INTEGER, condeferred INTEGER, convalidated INTEGER, conrelid INTEGER, contypid INTEGER, conindid INTEGER, conparentid INTEGER, confrelid INTEGER, confupdtype TEXT, confdeltype TEXT, confmatchtype TEXT, conislocal INTEGER, coninhcount INTEGER, connoinherit INTEGER, conkey TEXT, confkey TEXT, conpfeqop TEXT, conppeqop TEXT, conffeqop TEXT, conexclop TEXT, conbin TEXT)";

#[derive(Debug)]
pub struct PgConstraintTable;

impl PgConstraintTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        PG_CONSTRAINT_SQL.to_string()
    }
}

impl VirtualTable for PgConstraintTable {
    type Cursor = PgConstraintCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgConstraintCursor {
            conn,
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
            estimated_cost: 100.0,
            estimated_rows: 50,
            constraint_usages,
        })
    }
}

pub struct PgConstraintCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgConstraintCursor {
    fn load_constraints(&mut self) {
        let schema = self.conn.current_schema();
        self.rows.clear();

        let tables = user_tables_sorted(&schema);
        let num_tables = tables.len() as i64;
        let tbl_oid_map = table_oid_map(&schema);

        // Build index_name -> index_oid map (same OID assignment as pg_class/pg_index)
        let mut index_oid_map: HashMap<String, i64> = HashMap::default();
        let mut next_index_oid = USER_TABLE_OID_START + num_tables;
        for (table_name, _) in &tables {
            for idx in schema.get_indices(table_name) {
                if idx.ephemeral {
                    continue;
                }
                index_oid_map.insert(idx.name.clone(), next_index_oid);
                next_index_oid += 1;
            }
        }

        let mut constraint_oid = next_index_oid;

        for (table_name, table) in &tables {
            let btree = match table.as_ref() {
                Table::BTree(bt) => bt,
                _ => continue,
            };
            let table_oid = tbl_oid_map.get(*table_name).copied().unwrap_or(0);

            // Synthesize PK constraint for rowid-alias tables when unique_sets has no PK
            let has_pk_in_unique_sets = btree.unique_sets.iter().any(|us| us.is_primary_key);
            if !has_pk_in_unique_sets && !btree.primary_key_columns.is_empty() {
                let conname = format!("{table_name}_pkey");
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
                self.rows.push(vec![
                    Value::from_i64(constraint_oid),
                    Value::build_text(conname),
                    Value::from_i64(2200),
                    Value::build_text("p"),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(1),
                    Value::from_i64(table_oid),
                    Value::from_i64(0),
                    Value::from_i64(0),
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
                let conname = if us.is_primary_key {
                    format!("{table_name}_pkey")
                } else {
                    let cols_str = col_names.join("_");
                    format!("{table_name}_{cols_str}_key")
                };

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

                // Find matching index OID
                let conindid = if us.is_primary_key {
                    // PK auto-index name: sqlite_autoindex_{table}_{N}
                    index_oid_map
                        .iter()
                        .find(|(k, _)| {
                            k.starts_with(&format!(
                                "{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{table_name}"
                            ))
                        })
                        .map(|(_, &v)| v)
                        .unwrap_or(0)
                } else {
                    index_oid_map
                        .iter()
                        .find(|(k, _)| k.contains(&col_names.join("_")))
                        .map(|(_, &v)| v)
                        .unwrap_or(0)
                };

                self.rows.push(vec![
                    Value::from_i64(constraint_oid), // oid
                    Value::build_text(conname),      // conname
                    Value::from_i64(2200),           // connamespace (public)
                    Value::build_text(contype),      // contype
                    Value::from_i64(0),              // condeferrable
                    Value::from_i64(0),              // condeferred
                    Value::from_i64(1),              // convalidated
                    Value::from_i64(table_oid),      // conrelid
                    Value::from_i64(0),              // contypid
                    Value::from_i64(conindid),       // conindid
                    Value::from_i64(0),              // conparentid
                    Value::from_i64(0),              // confrelid
                    Value::Null,                     // confupdtype
                    Value::Null,                     // confdeltype
                    Value::Null,                     // confmatchtype
                    Value::from_i64(1),              // conislocal
                    Value::from_i64(0),              // coninhcount
                    Value::from_i64(0),              // connoinherit
                    Value::build_text(conkey),       // conkey
                    Value::Null,                     // confkey
                    Value::Null,                     // conpfeqop
                    Value::Null,                     // conppeqop
                    Value::Null,                     // conffeqop
                    Value::Null,                     // conexclop
                    Value::Null,                     // conbin
                ]);
                constraint_oid += 1;
            }

            // FK constraints from foreign_keys
            for fk in &btree.foreign_keys {
                let child_cols = fk.child_columns.join("_");
                let conname = format!("{table_name}_{child_cols}_fkey");

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

                let confrelid = tbl_oid_map.get(&fk.parent_table).copied().unwrap_or(0);

                let confkey: String = fk
                    .parent_columns
                    .iter()
                    .map(|name| {
                        schema
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

                self.rows.push(vec![
                    Value::from_i64(constraint_oid),                   // oid
                    Value::build_text(conname),                        // conname
                    Value::from_i64(2200),                             // connamespace
                    Value::build_text("f"),                            // contype
                    Value::from_i64(i64::from(fk.deferred)),           // condeferrable
                    Value::from_i64(i64::from(fk.deferred)),           // condeferred
                    Value::from_i64(1),                                // convalidated
                    Value::from_i64(table_oid),                        // conrelid
                    Value::from_i64(0),                                // contypid
                    Value::from_i64(0),                                // conindid
                    Value::from_i64(0),                                // conparentid
                    Value::from_i64(confrelid),                        // confrelid
                    Value::build_text(ref_act_to_char(&fk.on_update)), // confupdtype
                    Value::build_text(ref_act_to_char(&fk.on_delete)), // confdeltype
                    Value::build_text("s"),                            // confmatchtype (simple)
                    Value::from_i64(1),                                // conislocal
                    Value::from_i64(0),                                // coninhcount
                    Value::from_i64(0),                                // connoinherit
                    Value::build_text(conkey),                         // conkey
                    Value::build_text(confkey),                        // confkey
                    Value::Null,                                       // conpfeqop
                    Value::Null,                                       // conppeqop
                    Value::Null,                                       // conffeqop
                    Value::Null,                                       // conexclop
                    Value::Null,                                       // conbin
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

                self.rows.push(vec![
                    Value::from_i64(constraint_oid), // oid
                    Value::build_text(conname),      // conname
                    Value::from_i64(2200),           // connamespace
                    Value::build_text("c"),          // contype
                    Value::from_i64(0),              // condeferrable
                    Value::from_i64(0),              // condeferred
                    Value::from_i64(1),              // convalidated
                    Value::from_i64(table_oid),      // conrelid
                    Value::from_i64(0),              // contypid
                    Value::from_i64(0),              // conindid
                    Value::from_i64(0),              // conparentid
                    Value::from_i64(0),              // confrelid
                    Value::Null,                     // confupdtype
                    Value::Null,                     // confdeltype
                    Value::Null,                     // confmatchtype
                    Value::from_i64(1),              // conislocal
                    Value::from_i64(0),              // coninhcount
                    Value::from_i64(0),              // connoinherit
                    if conkey.is_empty() {
                        Value::Null
                    } else {
                        Value::build_text(conkey)
                    }, // conkey
                    Value::Null,                     // confkey
                    Value::Null,                     // conpfeqop
                    Value::Null,                     // conppeqop
                    Value::Null,                     // conffeqop
                    Value::Null,                     // conexclop
                    Value::build_text(chk.expr.to_string()), // conbin
                ]);
                constraint_oid += 1;
            }
        }
    }
}

impl VirtualTableCursor for PgConstraintCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.load_constraints();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_attrdef
// ──────────────────────────────────────────────────────────────────────

const PG_ATTRDEF_SQL: &str =
    "CREATE TABLE pg_attrdef (oid INTEGER, adrelid INTEGER, adnum INTEGER, adbin TEXT)";

#[derive(Debug)]
pub struct PgAttrdefTable;

impl PgAttrdefTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        PG_ATTRDEF_SQL.to_string()
    }
}

impl VirtualTable for PgAttrdefTable {
    type Cursor = PgAttrdefCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgAttrdefCursor {
            conn,
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
            estimated_cost: 100.0,
            estimated_rows: 50,
            constraint_usages,
        })
    }
}

pub struct PgAttrdefCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgAttrdefCursor {
    fn load_defaults(&mut self) {
        let schema = self.conn.current_schema();
        self.rows.clear();

        let tables = user_tables_sorted(&schema);
        let tbl_oid_map = table_oid_map(&schema);

        // OID counter for pg_attrdef rows — start after constraint OIDs
        // Use a high base to avoid collisions
        let mut attrdef_oid: i64 = 50000;

        for (table_name, table) in &tables {
            let btree = match table.as_ref() {
                Table::BTree(bt) => bt,
                _ => continue,
            };
            let table_oid = tbl_oid_map.get(*table_name).copied().unwrap_or(0);

            for (col_idx, col) in btree.columns().iter().enumerate() {
                if let Some(default_expr) = &col.default {
                    self.rows.push(vec![
                        Value::from_i64(attrdef_oid),                // oid
                        Value::from_i64(table_oid),                  // adrelid
                        Value::from_i64(col_idx as i64 + 1),         // adnum (1-based)
                        Value::build_text(default_expr.to_string()), // adbin
                    ]);
                    attrdef_oid += 1;
                }
            }
        }
    }
}

impl VirtualTableCursor for PgAttrdefCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.load_defaults();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

/// Virtual table implementation for pg_catalog.pg_sequences
/// Reads sequence metadata from Schema.sequences at scan time.
#[derive(Debug)]
pub struct PgSequencesTable;

impl PgSequencesTable {
    pub fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_sequences (
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
        )"
        .to_string()
    }
}

impl VirtualTable for PgSequencesTable {
    type Cursor = PgSequencesCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgSequencesCursor {
            conn,
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
            estimated_cost: 100.0,
            estimated_rows: 10,
            constraint_usages,
        })
    }
}

pub struct PgSequencesCursor {
    conn: Arc<Connection>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl PgSequencesCursor {
    fn load_sequences(&mut self) {
        self.rows.clear();
        let schema = self.conn.current_schema();
        let mut names: Vec<_> = schema.sequences.keys().cloned().collect();
        names.sort();
        for name in names {
            if let Some(seq) = schema.sequences.get(&name) {
                let seq_name = seq.name.clone();
                // currval is per-connection; expose this connection's last
                // value via the connection currval map, falling back to start.
                let last_val = self
                    .conn
                    .get_sequence_currval(&seq_name)
                    .unwrap_or(seq.start_value);
                self.rows.push(vec![
                    Value::build_text("public"),           // schemaname
                    Value::build_text(seq_name),           // sequencename
                    Value::build_text("turso"),            // sequenceowner
                    Value::build_text("bigint"),           // data_type
                    Value::from_i64(seq.start_value),      // start_value
                    Value::from_i64(seq.min_value),        // min_value
                    Value::from_i64(seq.max_value),        // max_value
                    Value::from_i64(seq.increment_by),     // increment_by
                    Value::from_i64(i64::from(seq.cycle)), // cycle
                    Value::from_i64(1), // cache_size (PG default; Turso doesn't cache)
                    Value::from_i64(last_val), // last_value
                ]);
            }
        }
    }
}

impl VirtualTableCursor for PgSequencesCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < self.rows[self.current_row].len() {
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
        self.current_row = 0;
        self.load_sequences();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

pub(crate) fn register_catalog_modules(mut options: OpenOptions) -> OpenOptions {
    options = options.native_module(
        "pg_class",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgClassTable::schema,
            create: PgClassTable::new,
        },
    );
    options = options.native_module(
        "pg_namespace",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgNamespaceTable::schema,
            create: PgNamespaceTable::new,
        },
    );
    options = options.native_module(
        "pg_attribute",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgAttributeTable::schema,
            create: PgAttributeTable::new,
        },
    );
    options = options.native_module(
        "pg_roles",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgRolesTable::schema,
            create: PgRolesTable::new,
        },
    );
    options = options.native_module(
        "pg_am",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgAmTable::schema,
            create: PgAmTable::new,
        },
    );
    options = options.native_module(
        "pg_proc",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgProcTable::schema,
            create: PgProcTable::new,
        },
    );
    options = options.native_module(
        "pg_database",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgDatabaseTable::schema,
            create: PgDatabaseTable::new,
        },
    );
    options = options.native_module(
        "pg_tables",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgTablesTable::schema,
            create: PgTablesTable::new,
        },
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
        CatalogModule {
            schema: PgIndexTable::schema,
            create: PgIndexTable::new,
        },
    );
    options = options.native_module(
        "pg_constraint",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgConstraintTable::schema,
            create: PgConstraintTable::new,
        },
    );
    options = options.native_module(
        "pg_type",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgTypeTable::schema,
            create: PgTypeTable::new,
        },
    );
    options = options.native_module(
        "pg_attrdef",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgAttrdefTable::schema,
            create: PgAttrdefTable::new,
        },
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
        "pg_sequences",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgSequencesTable::schema,
            create: PgSequencesTable::new,
        },
    );
    options = options.native_module(
        "pg_policy",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_policy (oid INTEGER, polname TEXT, polpermissive TEXT, polroles TEXT, polcmd TEXT, polqual TEXT, polwithcheck TEXT, polrelid INTEGER)".to_string() },
    );
    options = options.native_module(
        "pg_trigger",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_trigger (oid INTEGER, tgrelid INTEGER, tgname TEXT, tgfoid INTEGER, tgtype INTEGER, tgenabled TEXT, tgisinternal INTEGER, tgconstrrelid INTEGER, tgconstrindid INTEGER, tgconstraint INTEGER, tgdeferrable INTEGER, tginitdeferred INTEGER, tgnargs INTEGER, tgattr TEXT, tgargs TEXT, tgqual TEXT, tgoldtable TEXT, tgnewtable TEXT)".to_string() },
    );
    options = options.native_module(
        "pg_statistic_ext",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_statistic_ext (oid INTEGER, stxrelid INTEGER, stxname TEXT, stxnamespace INTEGER, stxowner INTEGER, stxstattarget INTEGER, stxkeys TEXT, stxkind TEXT, stxexprs TEXT)".to_string() },
    );
    options = options.native_module(
        "pg_inherits",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_inherits (inhrelid INTEGER, inhparent INTEGER, inhseqno INTEGER, inhdetachpending INTEGER)".to_string() },
    );
    options = options.native_module(
        "pg_rewrite",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_rewrite (oid INTEGER, rulename TEXT, ev_class INTEGER, ev_type TEXT, ev_enabled TEXT, is_instead INTEGER, ev_qual TEXT, ev_action TEXT)".to_string() },
    );
    options = options.native_module(
        "pg_foreign_table",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql:
                "CREATE TABLE pg_foreign_table (ftrelid INTEGER, ftserver INTEGER, ftoptions TEXT)"
                    .to_string(),
        },
    );
    options = options.native_module(
        "pg_partitioned_table",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_partitioned_table (partrelid INTEGER, partstrat TEXT, partnatts INTEGER, partdefid INTEGER, partattrs TEXT, partclass TEXT, partcollation TEXT, partexprs TEXT)".to_string() },
    );
    options = options.native_module(
        "pg_collation",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_collation (oid INTEGER, collname TEXT, collnamespace INTEGER, collowner INTEGER, collprovider TEXT, collisdeterministic INTEGER, collencoding INTEGER, collcollate TEXT, collctype TEXT, colliculocale TEXT, collicurules TEXT, collversion TEXT)".to_string() },
    );
    options = options.native_module(
        "pg_description",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_description (objoid INTEGER, classoid INTEGER, objsubid INTEGER, description TEXT)".to_string() },
    );
    options = options.native_module(
        "pg_publication",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication (oid INTEGER, pubname TEXT, pubowner INTEGER, puballtables INTEGER, pubinsert INTEGER, pubupdate INTEGER, pubdelete INTEGER, pubtruncate INTEGER, pubviaroot INTEGER)".to_string() },
    );
    options = options.native_module(
        "pg_publication_namespace",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication_namespace (oid INTEGER, pnpubid INTEGER, pnnspid INTEGER)".to_string() },
    );
    options = options.native_module(
        "pg_publication_rel",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication_rel (oid INTEGER, prpubid INTEGER, prrelid INTEGER, prqual TEXT, prattrs TEXT)".to_string() },
    );
    options
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

    fn allows_reads_in_triggers(&self) -> bool {
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
    sql_map: HashMap<String, String>,
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
        let schema = self.conn.current_schema();
        self.rows.clear();

        for (table_name, table) in &schema.tables {
            // Skip system tables
            if table_name.starts_with("sqlite_")
                || table_name == "sqlite_master"
                || table_name == "sqlite_schema"
            {
                continue;
            }

            // Skip virtual tables and subqueries
            let Table::BTree(btree_table) = table.as_ref() else {
                continue;
            };

            let postgres_ddl = match self.sql_map.get(table_name) {
                Some(schema_sql) => decode_stored_pg_schema_sql(schema_sql)
                    .map(str::to_string)
                    .unwrap_or_else(|| self.convert_to_postgres_ddl(schema_sql)),
                None => self.convert_to_postgres_ddl(&btree_table.to_sql()),
            };

            self.rows.push(vec![
                Value::Text("public".into()),
                Value::Text(table_name.clone().into()),
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
            self.statement = Some(
                self.conn
                    .prepare_internal("SELECT name, sql FROM sqlite_schema WHERE type = 'table'")?,
            );
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
                if let (Value::Text(name), Value::Text(sql)) = (row.get_value(0), row.get_value(1))
                {
                    sql_map.insert(name.as_str().to_string(), sql.as_str().to_string());
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
#[allow(dead_code)]
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
/// Uses the same OID assignment as PgConstraintCursor::load_constraints.
#[allow(dead_code)]
pub fn pg_get_constraintdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    let tables = user_tables_sorted(&schema);
    let num_tables = tables.len() as i64;

    // Build index_name -> index_oid map (same as pg_constraint)
    let mut next_index_oid = USER_TABLE_OID_START + num_tables;
    for (table_name, _) in &tables {
        for idx in schema.get_indices(table_name) {
            if !idx.ephemeral {
                next_index_oid += 1;
            }
        }
    }

    let mut constraint_oid = next_index_oid;

    for (_, table) in &tables {
        let btree = match table.as_ref() {
            Table::BTree(bt) => bt,
            _ => continue,
        };

        // Synthesized PK for rowid-alias tables
        let has_pk_in_unique_sets = btree.unique_sets.iter().any(|us| us.is_primary_key);
        if !has_pk_in_unique_sets && !btree.primary_key_columns.is_empty() {
            if constraint_oid == target_oid {
                let cols: Vec<String> = btree
                    .primary_key_columns
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect();
                return Some(format!("PRIMARY KEY ({})", cols.join(", ")));
            }
            constraint_oid += 1;
        }

        // PK / UNIQUE from unique_sets
        for us in &btree.unique_sets {
            if constraint_oid == target_oid {
                let col_names: Vec<&str> = us.columns.iter().map(|c| c.name.as_str()).collect();
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
                let child_cols = fk.child_columns.join(", ");
                let parent_cols = fk.parent_columns.join(", ");
                let mut def = format!(
                    "FOREIGN KEY ({child_cols}) REFERENCES {}({parent_cols})",
                    fk.parent_table
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
/// Uses the same OID assignment as PgIndexCursor::load_indexes / PgClassTable.
#[allow(dead_code)]
pub fn pg_get_indexdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    let tables = user_tables_sorted(&schema);
    let num_tables = tables.len() as i64;

    let mut index_oid = USER_TABLE_OID_START + num_tables;
    for (table_name, _) in &tables {
        for idx in schema.get_indices(table_name) {
            if idx.ephemeral {
                continue;
            }
            if index_oid == target_oid {
                let unique = if idx.unique { "UNIQUE " } else { "" };
                let cols: Vec<String> = idx
                    .columns
                    .iter()
                    .map(|col| {
                        if let Some(expr) = &col.expr {
                            expr.to_string()
                        } else {
                            col.name.clone()
                        }
                    })
                    .collect();
                let mut def = format!(
                    "CREATE {unique}INDEX {} ON {table_name} USING btree ({})",
                    idx.name,
                    cols.join(", ")
                );
                if let Some(where_clause) = &idx.where_clause {
                    def.push_str(&format!(" WHERE {where_clause}"));
                }
                return Some(def);
            }
            index_oid += 1;
        }
    }

    None
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
    fn suspended_catalog_does_not_change_sibling_writer_accounting() {
        use turso_core::IO;

        let io = Arc::new(turso_core::MemoryYieldIO::new());
        let db = crate::session::open_database_with_io(
            io.clone(),
            "suspended-catalog.db",
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE writes (v INT)").unwrap();
        for i in 0..8 {
            conn.execute(format!(
                "CREATE TABLE catalog_{i} (v TEXT DEFAULT '{}')",
                "x".repeat(4096)
            ))
            .unwrap();
        }
        let mut catalog = conn
            .prepare("SELECT table_name FROM pg_get_tabledef")
            .unwrap();
        conn.get_pager().clear_page_cache(false);
        conn.execute("SELECT COUNT(*) FROM pg_namespace").unwrap();
        assert!(matches!(catalog.step().unwrap(), StepResult::IO));
        assert!(!conn.is_nested_stmt());

        let mut writer = conn
            .prepare("INSERT INTO writes VALUES (7) RETURNING v")
            .unwrap();
        loop {
            match writer.step().unwrap() {
                StepResult::IO => io.step().unwrap(),
                StepResult::Row => break,
                other => panic!("unexpected writer step: {other:?}"),
            }
        }
        assert_eq!(writer.row().unwrap().get_value(0), &Value::from_i64(7));
        let mut sibling = conn.prepare("INSERT INTO writes SELECT 9").unwrap();
        let result = loop {
            match sibling.step() {
                Ok(StepResult::IO) => io.step().unwrap(),
                result => break result,
            }
        };
        assert!(
            matches!(
                result,
                Err(LimboError::StatementsInProgress(
                    "cannot start a write statement"
                ))
            ),
            "unexpected sibling result: {result:?}"
        );
        drop(sibling);
        catalog.reset().unwrap();
        assert!(!conn.is_nested_stmt());
        writer.run_ignore_rows().unwrap();
        drop(writer);
        conn.execute("INSERT INTO writes VALUES (11)").unwrap();
        let observer = db.connect().unwrap();
        assert_eq!(
            observer
                .prepare("SELECT v FROM writes ORDER BY v")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(7)], vec![Value::from_i64(11)]]
        );
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
        let conn = crate::Connection::new(db.connect().unwrap());

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

        let conn = crate::Connection::new(conn);

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

        let conn = crate::Connection::new(conn);

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

        let conn = crate::Connection::new(conn);

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
        let pg_conn = crate::Connection::new(db.connect().unwrap());
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

        let conn = crate::Connection::new(conn);

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

        let conn = crate::Connection::new(conn);

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

        let conn = crate::Connection::new(conn);

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
