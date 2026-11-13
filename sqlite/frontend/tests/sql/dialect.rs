use std::sync::Arc;
use turso_core::dialect::*;
use turso_core::function::ScalarFunc;
use turso_core::functions::builtin::call_builtin_scalar;
use turso_core::functions::builtin::resolve_builtin_function;
use turso_core::native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule};
use turso_core::schema::BTreeTable;
use turso_core::schema::Schema;
use turso_core::security::roles::SessionRole;
use turso_core::storage::database::DatabaseFile;
use turso_core::sync::atomic::{AtomicUsize, Ordering};
use turso_core::types::IOResultOr;
use turso_core::vdbe::Register;
use turso_core::Connection;
use turso_core::DatabaseOpts;
use turso_core::Func;
use turso_core::LimboError;
use turso_core::OpenOptions;
use turso_core::PlatformIO;
use turso_core::Result;
use turso_core::Value;
use turso_core::{
    ConnectOpts, Database, IOResult, MemoryIO, Numeric, OpenFlags, StatementStatusCounter, IO,
};
use turso_ext::{ConstraintInfo, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode, VTabKind};
use turso_parser::ast;
use turso_parser::ast::Cmd;
use turso_parser::ast::CreateTableBody;
use turso_parser::ast::QualifiedName;
use turso_parser::ast::Stmt;
use turso_sqlite::SqliteDialect;

/// A dialect that counts schema-row parses and strips a `/* test */ `
/// marker before delegating to SQLite parsing, mirroring how a frontend
/// dialect recognizes its own stored text and falls back to SQLite for
/// unmarked rows.
#[derive(Default)]
struct TestDialect {
    parse_calls: AtomicUsize,
    statement_parse_calls: AtomicUsize,
}

impl Dialect for TestDialect {
    fn name(&self) -> &'static str {
        "test"
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        self.statement_parse_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(sql) = sql.strip_prefix("test: ") {
            let (cmd, offset) = SqliteDialect.parse_internal(sql)?;
            Ok((cmd, "test: ".len() + offset))
        } else {
            SqliteDialect.parse_internal(sql)
        }
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(&self, sql: &str) -> Result<ast::ColumnDefinition> {
        SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        self.parse_calls.fetch_add(1, Ordering::SeqCst);
        let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
        crate::table_from_sql(sql, root_page)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
        parse_table_sql_ast(&SqliteDialect, sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
        table_sql_for_replay(&SqliteDialect, sql)
    }

    fn format_table_sql(
        &self,
        input: &str,
        _tbl_name: &QualifiedName,
        _body: &CreateTableBody,
    ) -> Result<String> {
        Ok(format!("/* test */ {input}"))
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        if name.eq_ignore_ascii_case("nvl") {
            return resolve_builtin_function("coalesce", arg_count);
        }
        if name.eq_ignore_ascii_case("test_add_one") && arg_count == 1 {
            return Ok(Some(Func::Dialect("test_add_one".to_string())));
        }
        resolve_builtin_function(name, arg_count)
    }

    fn exec_scalar_function(
        &self,
        _conn: &Connection,
        name: &str,
        args: &[Value],
    ) -> Result<Value> {
        assert_eq!(name, "test_add_one");
        let Value::Numeric(Numeric::Integer(v)) = args[0] else {
            return Err(LimboError::InvalidArgument(
                "test_add_one expects an integer".to_string(),
            ));
        };
        Ok(Value::Numeric(Numeric::Integer(v + 1)))
    }

    fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
        schema.register_builtin_catalog(self, enable_custom_types)
    }

    fn register_native_extensions(&self, options: OpenOptions) -> OpenOptions {
        options.native_module(
            "test_catalog",
            VTabKind::TableValuedFunction,
            TestCatalogTable,
        )
    }
}

/// Stores table definitions in syntax that SQLite cannot parse and always
/// adds its marker, so replay tests detect repeated storage formatting.
struct StrictTestDialect;

impl StrictTestDialect {
    const PREFIX: &'static str = "strict: ";
}

impl Dialect for StrictTestDialect {
    fn name(&self) -> &'static str {
        "strict-test"
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(&self, sql: &str) -> Result<ast::ColumnDefinition> {
        SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
        crate::table_from_sql(sql, root_page)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
        parse_table_sql_ast(&SqliteDialect, sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
        table_sql_for_replay(&SqliteDialect, sql)
    }

    fn format_table_sql(
        &self,
        input: &str,
        _tbl_name: &QualifiedName,
        _body: &CreateTableBody,
    ) -> Result<String> {
        Ok(format!("{}{input}", Self::PREFIX))
    }

    fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
        schema.register_builtin_catalog(self, enable_custom_types)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        resolve_builtin_function(name, arg_count)
    }
}

/// A one-row catalog table installed by [`TestDialect`],
/// standing in for a frontend catalog surface like `pg_class`.
#[derive(Debug)]
struct TestCatalogTable;

impl VirtualTableModule for TestCatalogTable {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE test_catalog (value INTEGER)".to_string())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(Self)
    }
}

impl VirtualTable for TestCatalogTable {
    type Cursor = TestCatalogCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(TestCatalogCursor { row: 0 })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 1,
            constraint_usages: constraints
                .iter()
                .map(|_| ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
        })
    }
}

struct TestCatalogCursor {
    row: usize,
}

impl VirtualTableCursor for TestCatalogCursor {
    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> IOResultOr<bool> {
        self.row = 0;
        Ok(IOResult::Done(true))
    }

    fn next(&mut self) -> IOResultOr<bool> {
        self.row += 1;
        Ok(IOResult::Done(self.row < 1))
    }

    fn rowid(&self) -> i64 {
        self.row as i64
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        Ok(IOResult::Done(match column {
            0 => Value::from_i64(42),
            _ => Value::Null,
        }))
    }
}

fn open_db(io: &Arc<dyn IO>, path: &str, dialect: Arc<dyn Dialect>) -> Result<Arc<Database>> {
    let file = io.open_file(path, OpenFlags::Create, true)?;
    let db_file = Arc::new(DatabaseFile::new(file));
    Database::open(io.clone(), path, OpenOptions::new(dialect).storage(db_file))
}

#[test]
fn schema_load_routes_through_dialect() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    {
        let db = open_db(&io, "dialect-load.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.close().unwrap();
    }

    // Reopening the database parses the stored schema row for `t`
    // through the dialect.
    let dialect = Arc::new(TestDialect::default());
    let db = open_db(&io, "dialect-load.db", dialect.clone()).unwrap();
    assert!(dialect.parse_calls.load(Ordering::SeqCst) >= 1);

    // DDL reparses the schema via the ParseSchema opcode, again through
    // the dialect.
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let before = dialect.parse_calls.load(Ordering::SeqCst);
    conn.execute("CREATE TABLE u (y INTEGER)").unwrap();
    assert!(dialect.parse_calls.load(Ordering::SeqCst) > before);

    // Both tables are usable under the dialect.
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("INSERT INTO u VALUES (2)").unwrap();
    conn.close().unwrap();
}

#[test]
fn dialect_parser_is_used_for_reprepare() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let dialect = Arc::new(TestDialect::default());
    let db = open_db(&io, "dialect-reprepare.db", dialect.clone()).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let mut stmt = conn.prepare("test: SELECT 42").unwrap();
    conn.set_full_column_names(true);
    let rows = stmt.run_collect_rows().unwrap();

    assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 1);
    assert_eq!(dialect.statement_parse_calls.load(Ordering::SeqCst), 2);
    conn.close().unwrap();
}

#[test]
fn consume_stmt_reports_parse_error() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "consume-stmt-parse-error.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    assert!(conn.consume_stmt("SELECT * FROM").is_err());
    conn.close().unwrap();
}

#[test]
fn dialect_catalog_available_on_every_schema_and_rebuild() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-catalog.db", Arc::new(TestDialect::default())).unwrap();

    let query_catalog = |conn: &Arc<Connection>| -> Vec<Vec<Value>> {
        conn.prepare("SELECT value FROM test_catalog")
            .unwrap()
            .run_collect_rows()
            .unwrap()
    };

    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(
        query_catalog(&conn1),
        vec![vec![Value::Numeric(Numeric::Integer(42))]]
    );
    assert_eq!(
        query_catalog(&conn2),
        vec![vec![Value::Numeric(Numeric::Integer(42))]]
    );

    // DDL on another connection forces conn1 to rebuild its schema from
    // sqlite_schema; the catalog table must survive because schema
    // construction re-registers it.
    conn2.execute("CREATE TABLE t (x INTEGER)").unwrap();
    assert_eq!(
        query_catalog(&conn1),
        vec![vec![Value::Numeric(Numeric::Integer(42))]]
    );

    conn1.close().unwrap();
    conn2.close().unwrap();
}

#[test]
fn dialect_catalog_cannot_be_dropped() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(
        &io,
        "dialect-catalog-drop.db",
        Arc::new(TestDialect::default()),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let error = conn.execute("DROP TABLE test_catalog").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("table test_catalog may not be dropped"),
        "unexpected error: {error}"
    );

    let new_conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    for catalog_conn in [&conn, &new_conn] {
        let rows = catalog_conn
            .prepare("SELECT value FROM test_catalog")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
    }
    conn.close().unwrap();
    new_conn.close().unwrap();
}

#[test]
fn dialect_catalog_survives_mvcc_recovery() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let path = "dialect-catalog-mvcc-recovery.db";

    {
        let db = open_db(&io, path, Arc::new(TestDialect::default())).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("PRAGMA journal_mode = mvcc").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.close().unwrap();
    }

    let db = open_db(&io, path, Arc::new(TestDialect::default())).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let rows = conn
        .prepare("SELECT value FROM test_catalog")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
    conn.close().unwrap();
}

#[test]
fn dialect_catalog_available_in_initialized_temp_schema() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(
        &io,
        "dialect-catalog-temp.db",
        Arc::new(TestDialect::default()),
    )
    .unwrap();

    for temp_store in ["MEMORY", "FILE"] {
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute(format!("PRAGMA temp_store = {temp_store}"))
            .unwrap();
        conn.execute("CREATE TEMP TABLE t (x INTEGER)").unwrap();
        let rows = conn
            .prepare("SELECT value FROM temp.test_catalog")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
        conn.close().unwrap();
    }
}

#[test]
fn create_table_stores_dialect_formatted_sql() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    {
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-store.db", dialect).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        // A frontend prepares its translated AST while supplying the
        // original statement text.
        let input = "CREATE TABLE t (x INTEGER)";
        let stmt = match parse_statement(&SqliteDialect, input).unwrap().unwrap() {
            Cmd::Stmt(stmt) => stmt,
            other => panic!("unexpected command: {other:?}"),
        };
        conn.prepare_translated_stmt(stmt, input)
            .unwrap()
            .run_ignore_rows()
            .unwrap();

        // The stored schema row carries the dialect marker and the
        // original text.
        let rows = conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 1);
        let stored = rows[0][0].to_string();
        assert_eq!(stored.trim_matches('\''), format!("/* test */ {input}"));
        conn.close().unwrap();
    }

    // Round-trip: reopening parses the marked row back through the
    // dialect and the table stays usable.
    let dialect = Arc::new(TestDialect::default());
    let db = open_db(&io, "dialect-store.db", dialect.clone()).unwrap();
    assert!(dialect.parse_calls.load(Ordering::SeqCst) >= 1);
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let rows = conn
        .prepare("SELECT x FROM t")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
    conn.close().unwrap();
}

/// A dialect that resolves no functions at all, proving the dialect —
/// not the engine — owns the function name surface of user SQL.
struct NoFunctionsDialect;

impl Dialect for NoFunctionsDialect {
    fn name(&self) -> &'static str {
        "nofuncs"
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(&self, sql: &str) -> Result<ast::ColumnDefinition> {
        SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        crate::table_from_sql(sql, root_page)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        parse_table_sql_ast(&SqliteDialect, sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        table_sql_for_replay(&SqliteDialect, sql)
    }

    fn format_table_sql(
        &self,
        _input: &str,
        tbl_name: &QualifiedName,
        body: &CreateTableBody,
    ) -> Result<String> {
        Ok(format!(
            "CREATE TABLE {} {}",
            tbl_name.name.as_ident(),
            body
        ))
    }

    fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
        schema.register_builtin_catalog(self, enable_custom_types)
    }

    fn resolve_function(&self, _name: &str, _arg_count: usize) -> Result<Option<Func>> {
        Ok(None)
    }
}

/// A dialect that resolves the built-in names but runs `upper` its own
/// way, as a frontend does for a built-in whose behavior differs in its
/// dialect.
struct ShoutingUpperDialect;

impl Dialect for ShoutingUpperDialect {
    fn name(&self) -> &'static str {
        "shouting"
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(&self, sql: &str) -> Result<ast::ColumnDefinition> {
        SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        crate::table_from_sql(sql, root_page)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        parse_table_sql_ast(&SqliteDialect, sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        table_sql_for_replay(&SqliteDialect, sql)
    }

    fn format_table_sql(
        &self,
        _input: &str,
        tbl_name: &QualifiedName,
        body: &CreateTableBody,
    ) -> Result<String> {
        format_table_sql(tbl_name, body)
    }

    fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
        schema.register_builtin_catalog(self, enable_custom_types)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        resolve_builtin_function(name, arg_count)
    }

    fn call_scalar(
        &self,
        context: &ScalarFuncContext<'_>,
        func: &ScalarFunc,
        args: &[Register],
    ) -> Result<Value> {
        let value = call_builtin_scalar(context, func, args)?;
        if !matches!(func, ScalarFunc::Upper) {
            return Ok(value);
        }
        Ok(Value::build_text(format!("{value}!")))
    }
}

#[test]
fn dialect_owns_builtin_scalar_function_behavior() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-shouting.db", Arc::new(ShoutingUpperDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t (s TEXT)").unwrap();
    conn.execute("INSERT INTO t VALUES ('hi')").unwrap();
    let rows = conn
        .prepare("SELECT upper(s), lower('ABC'), abs(-7) FROM t")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::build_text("HI!"),
            Value::build_text("abc"),
            Value::from_i64(7),
        ]]
    );
    conn.close().unwrap();
}

#[test]
fn dialect_scalar_function_resolves_and_executes() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-funcs.db", Arc::new(TestDialect::default())).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // Dialect-provided scalar executes through exec_scalar_function.
    let rows = conn
        .prepare("SELECT test_add_one(41)")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::Numeric(Numeric::Integer(42))]]);

    // Built-ins still resolve because the dialect composes with the
    // shared table.
    let rows = conn
        .prepare("SELECT abs(-7)")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::Numeric(Numeric::Integer(7))]]);

    // Unknown names still error.
    let err = conn.prepare("SELECT no_such_function(1)").unwrap_err();
    assert!(err.to_string().contains("no such function"));
    conn.close().unwrap();
}

#[test]
fn dialect_function_alias_preserves_outer_join() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(
        &io,
        "dialect-outer-join.db",
        Arc::new(TestDialect::default()),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE lhs (id INTEGER)").unwrap();
    conn.execute("CREATE TABLE rhs (id INTEGER, value INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO lhs VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO rhs VALUES (1, 0)").unwrap();

    let rows = conn
        .prepare(
            "SELECT lhs.id FROM lhs LEFT JOIN rhs ON rhs.id = lhs.id \
             WHERE nvl(rhs.value, 1) = 1 ORDER BY lhs.id",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(2)]]);
    conn.close().unwrap();
}

#[test]
fn dialect_owns_the_function_surface() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-nofuncs.db", Arc::new(NoFunctionsDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // Function-free SQL works, including DDL (whose internal helper
    // statements resolve with SQLite semantics regardless of dialect).
    conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (-7)").unwrap();

    // A SQLite built-in is not part of this dialect's surface.
    let err = conn.prepare("SELECT abs(x) FROM t").unwrap_err();
    assert!(
        err.to_string().contains("no such function"),
        "unexpected error: {err}"
    );
    conn.close().unwrap();
}

#[test]
fn cdc_generated_functions_bypass_the_dialect() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-cdc.db", Arc::new(NoFunctionsDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (7)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (8)").unwrap();
    conn.execute("COMMIT").unwrap();

    let rows = conn
        .prepare(
            "SELECT change_type, table_name, id, change_txn_id \
             FROM turso_cdc ORDER BY change_id",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], Value::from_i64(1));
    assert_eq!(rows[0][1], Value::build_text("t"));
    assert_eq!(rows[0][2], Value::from_i64(1));
    assert_eq!(rows[1][0], Value::from_i64(2));
    assert_eq!(rows[1][1], Value::Null);
    assert_eq!(rows[1][2], Value::Null);
    assert_eq!(rows[0][3], rows[1][3]);
    assert_eq!(rows[2][0], Value::from_i64(1));
    assert_eq!(rows[2][1], Value::build_text("t"));
    assert_eq!(rows[2][2], Value::from_i64(2));
    assert_eq!(rows[3][0], Value::from_i64(2));
    assert_eq!(rows[3][1], Value::Null);
    assert_eq!(rows[3][2], Value::Null);
    assert_eq!(rows[2][3], rows[3][3]);
    conn.close().unwrap();
}

#[test]
fn alter_table_rewrites_dialect_formatted_sql() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-alter-table.db", Arc::new(StrictTestDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
    conn.execute("ALTER TABLE t RENAME TO u").unwrap();

    let rows = conn
        .prepare("SELECT sql FROM sqlite_schema WHERE name = 'u'")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].to_string().trim_matches('\''),
        "strict: CREATE TABLE u (x INTEGER)"
    );
    conn.execute("INSERT INTO u VALUES (1)").unwrap();
    conn.close().unwrap();
}

#[test]
fn alter_table_rename_column_decodes_dialect_formatted_sql() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-alter-column.db", Arc::new(StrictTestDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
    conn.execute("ALTER TABLE t RENAME COLUMN x TO y").unwrap();

    let rows = conn
        .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].to_string().trim_matches('\''),
        "strict: CREATE TABLE t (y INTEGER)"
    );
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(
        conn.prepare("SELECT y FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(1)]]
    );
    conn.close().unwrap();
}

#[test]
fn registry_rejects_dialect_mismatch() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let _db = open_db(&io, "dialect-mismatch.db", Arc::new(SqliteDialect)).unwrap();

    let err = open_db(&io, "dialect-mismatch.db", Arc::new(TestDialect::default())).unwrap_err();
    assert!(
        err.to_string().contains("already open with dialect"),
        "unexpected error: {err}"
    );
}

#[test]
fn registry_rejects_default_open_of_dialect_database() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let _db = open_db(
        &io,
        "dialect-mismatch-reverse.db",
        Arc::new(TestDialect::default()),
    )
    .unwrap();

    let err = open_db(&io, "dialect-mismatch-reverse.db", Arc::new(SqliteDialect)).unwrap_err();
    assert!(
        err.to_string().contains("already open with dialect"),
        "unexpected error: {err}"
    );
}

#[test]
fn shared_memory_registry_rejects_dialect_mismatch() {
    let name = "dialect-shared-memory-mismatch";
    let _db = Database::open_shared_memory(name, Arc::new(SqliteDialect)).unwrap();

    let err = Database::open_shared_memory(name, Arc::new(TestDialect::default())).unwrap_err();
    assert!(
        err.to_string().contains("already open with dialect"),
        "unexpected error: {err}"
    );
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn vacuum_into_replays_schema_with_source_dialect() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.db");
    let output_path = dir.path().join("output.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io.clone(),
        source_path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(StrictTestDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (42)").unwrap();

    conn.execute(format!("VACUUM INTO '{}'", output_path.display()))
        .unwrap();

    let output_db = Database::open_file(
        io,
        output_path.to_str().unwrap(),
        Arc::new(StrictTestDialect),
    )
    .unwrap();
    let output_conn = output_db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let schema_rows = output_conn
        .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(schema_rows.len(), 1);
    assert_eq!(
        schema_rows[0][0].to_string().trim_matches('\''),
        "strict: CREATE TABLE t(x INTEGER)"
    );
    assert_eq!(
        output_conn
            .prepare("SELECT x FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(42)]]
    );
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn vacuum_attached_database_strips_source_schema_from_replay() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.db");
    let attached_path = dir.path().join("attached.db");
    let output_path = dir.path().join("output.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io.clone(),
        source_path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(StrictTestDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute(format!(
        "ATTACH DATABASE '{}' AS aux",
        attached_path.display()
    ))
    .unwrap();
    conn.execute("CREATE TABLE aux.t (x INTEGER)").unwrap();
    conn.execute("INSERT INTO aux.t VALUES (42)").unwrap();

    conn.execute(format!("VACUUM aux INTO '{}'", output_path.display()))
        .unwrap();

    let output_db = Database::open_file(
        io,
        output_path.to_str().unwrap(),
        Arc::new(StrictTestDialect),
    )
    .unwrap();
    let output_conn = output_db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(
        output_conn
            .prepare("SELECT x FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(42)]]
    );
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn in_place_vacuum_replays_schema_with_source_dialect() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_vacuum(true),
        None,
        Arc::new(StrictTestDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (42)").unwrap();

    conn.execute("VACUUM").unwrap();

    let schema_rows = conn
        .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(schema_rows.len(), 1);
    assert_eq!(
        schema_rows[0][0].to_string().trim_matches('\''),
        "strict: CREATE TABLE t(x INTEGER)"
    );
    assert_eq!(
        conn.prepare("SELECT x FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(42)]]
    );
}
