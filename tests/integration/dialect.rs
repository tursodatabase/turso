use std::sync::Arc;
use turso_core::ast::Cmd;
use turso_core::ast::CreateTableBody;
use turso_core::ast::QualifiedName;
use turso_core::ast::Stmt;
use turso_core::dialect::*;
use turso_core::function::ScalarFunc;
use turso_core::io::PlatformIO;
use turso_core::native_ext::VirtualTable;
use turso_core::native_ext::VirtualTableCursor;
use turso_core::native_ext::VirtualTableModule;
use turso_core::schema::BTreeTable;
use turso_core::security::roles::SessionRole;
use turso_core::storage::database::DatabaseFile;
use turso_core::sync::atomic::AtomicUsize;
use turso_core::sync::atomic::Ordering;
use turso_core::types::Conversion;
use turso_core::types::IOResultOr;
use turso_core::types::Type;
use turso_core::vdbe::Register;
use turso_core::ConnectOpts;
use turso_core::Connection;
use turso_core::Database;
use turso_core::DatabaseOpts;
use turso_core::Func;
use turso_core::IOResult;
use turso_core::LimboError;
use turso_core::MemoryIO;
use turso_core::Numeric;
use turso_core::OpenFlags;
use turso_core::OpenOptions;
use turso_core::Result;
use turso_core::StatementStatusCounter;
use turso_core::Value;
use turso_core::IO;
use turso_ext::ConstraintInfo;
use turso_ext::ConstraintUsage;
use turso_ext::IndexInfo;
use turso_ext::OrderByInfo;
use turso_ext::ResultCode;
use turso_ext::VTabKind;
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

    fn parse_internal(
        &self,
        sql: &str,
    ) -> turso_core::Result<(Option<turso_core::ast::Cmd>, usize)> {
        turso_sqlite::SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> turso_core::Result<turso_core::ast::ColumnDefinition> {
        turso_sqlite::SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parameter_marker_ranges(
        &self,
        sql: &str,
    ) -> turso_core::Result<Vec<std::ops::Range<usize>>> {
        turso_sqlite::SqliteDialect.parameter_marker_ranges(sql)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
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

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        self.parse_calls.fetch_add(1, Ordering::SeqCst);
        let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
        turso_core::schema::BTreeTable::from_stored_sql(self, sql, root_page, false)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
        turso_core::dialect::parse_table_sql_ast(self, sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
        turso_core::dialect::table_sql_for_replay(self, sql)
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
            return turso_core::dialect::resolve_builtin_function("coalesce", arg_count);
        }
        if name.eq_ignore_ascii_case("test_add_one") && arg_count == 1 {
            return Ok(Some(Func::Dialect("test_add_one".to_string())));
        }
        turso_core::dialect::resolve_builtin_function(name, arg_count)
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

    fn parse_internal(
        &self,
        sql: &str,
    ) -> turso_core::Result<(Option<turso_core::ast::Cmd>, usize)> {
        turso_sqlite::SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> turso_core::Result<turso_core::ast::ColumnDefinition> {
        turso_sqlite::SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parameter_marker_ranges(
        &self,
        sql: &str,
    ) -> turso_core::Result<Vec<std::ops::Range<usize>>> {
        turso_sqlite::SqliteDialect.parameter_marker_ranges(sql)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
        turso_core::schema::BTreeTable::from_stored_sql(self, sql, root_page, false)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
        turso_core::dialect::parse_table_sql_ast(self, sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
        turso_core::dialect::table_sql_for_replay(self, sql)
    }

    fn format_table_sql(
        &self,
        input: &str,
        _tbl_name: &QualifiedName,
        _body: &CreateTableBody,
    ) -> Result<String> {
        Ok(format!("{}{input}", Self::PREFIX))
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        turso_core::dialect::resolve_builtin_function(name, arg_count)
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
        let stmt = match turso_core::dialect::parse_statement(&SqliteDialect, input)
            .unwrap()
            .unwrap()
        {
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

    fn parse_internal(
        &self,
        sql: &str,
    ) -> turso_core::Result<(Option<turso_core::ast::Cmd>, usize)> {
        turso_sqlite::SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> turso_core::Result<turso_core::ast::ColumnDefinition> {
        turso_sqlite::SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parameter_marker_ranges(
        &self,
        sql: &str,
    ) -> turso_core::Result<Vec<std::ops::Range<usize>>> {
        turso_sqlite::SqliteDialect.parameter_marker_ranges(sql)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        turso_core::schema::BTreeTable::from_stored_sql(self, sql, root_page, false)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        turso_core::dialect::parse_table_sql_ast(self, sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        turso_core::dialect::table_sql_for_replay(self, sql)
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

    fn parse_internal(
        &self,
        sql: &str,
    ) -> turso_core::Result<(Option<turso_core::ast::Cmd>, usize)> {
        turso_sqlite::SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> turso_core::Result<turso_core::ast::ColumnDefinition> {
        turso_sqlite::SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parameter_marker_ranges(
        &self,
        sql: &str,
    ) -> turso_core::Result<Vec<std::ops::Range<usize>>> {
        turso_sqlite::SqliteDialect.parameter_marker_ranges(sql)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        turso_core::schema::BTreeTable::from_stored_sql(self, sql, root_page, false)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        turso_core::dialect::parse_table_sql_ast(self, sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        turso_core::dialect::table_sql_for_replay(self, sql)
    }

    fn format_table_sql(
        &self,
        _input: &str,
        tbl_name: &QualifiedName,
        body: &CreateTableBody,
    ) -> Result<String> {
        turso_core::dialect::format_table_sql(tbl_name, body)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        turso_core::dialect::resolve_builtin_function(name, arg_count)
    }

    fn call_scalar(
        &self,
        context: &ScalarFuncContext<'_>,
        func: &ScalarFunc,
        args: &[Register],
    ) -> Result<Value> {
        let value = turso_core::dialect::call_builtin_scalar(context, func, args)?;
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

#[cfg(feature = "fs")]
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

#[cfg(all(feature = "fs", not(target_family = "wasm")))]
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

#[cfg(all(feature = "fs", not(target_family = "wasm")))]
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

#[cfg(all(feature = "fs", not(target_family = "wasm")))]
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

#[test]
fn test_sql_is_create_virtual_table() {
    assert!(sql_is_create_virtual_table(
        &SqliteDialect,
        "CREATE VIRTUAL TABLE x USING y;"
    ));
    assert!(sql_is_create_virtual_table(
        &SqliteDialect,
        "create virtual table x using y"
    ));
    assert!(sql_is_create_virtual_table(
        &SqliteDialect,
        "  \n\tCREATE  VIRTUAL TABLE x USING y"
    ));
    assert!(sql_is_create_virtual_table(
        &SqliteDialect,
        "-- comment\nCREATE /* c */ VIRTUAL TABLE x USING y"
    ));
    assert!(sql_is_create_virtual_table(
        &SqliteDialect,
        "CREATE VIRTUAL TABLE IF NOT EXISTS x USING y(a, b)"
    ));
    // Quoted identifiers must not confuse the classifier either way.
    assert!(sql_is_create_virtual_table(
        &SqliteDialect,
        "CREATE VIRTUAL TABLE \"create table\" USING y"
    ));
    // Regular table whose SQL merely contains the text must not match.
    assert!(!sql_is_create_virtual_table(
        &SqliteDialect,
        "CREATE TABLE t(x TEXT DEFAULT 'create virtual')"
    ));
    assert!(!sql_is_create_virtual_table(
        &SqliteDialect,
        "CREATE TABLE \"create virtual\"(x)"
    ));
    assert!(!sql_is_create_virtual_table(
        &SqliteDialect,
        "CREATE TABLE t(x)"
    ));
    assert!(!sql_is_create_virtual_table(
        &SqliteDialect,
        "CREATE VIRTUALX TABLE t(x)"
    ));
    assert!(!sql_is_create_virtual_table(
        &SqliteDialect,
        "CREATE VIRTUAL"
    ));
    assert!(!sql_is_create_virtual_table(&SqliteDialect, ""));
}

#[test]
fn schema_of_a_dialect_that_adds_to_the_builder_keeps_its_namespace_support() {
    let schema = <dyn Dialect>::new_schema(&NamespacesAndOwnBuilderDialect, false).unwrap();
    assert!(schema.supports_namespaces());
}

/// A dialect with namespaces that overrides `schema_builder`.
struct NamespacesAndOwnBuilderDialect;

impl Dialect for NamespacesAndOwnBuilderDialect {
    fn name(&self) -> &'static str {
        "namespaces-and-own-builder"
    }

    fn parse_internal(
        &self,
        sql: &str,
    ) -> turso_core::Result<(Option<turso_core::ast::Cmd>, usize)> {
        turso_sqlite::SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> turso_core::Result<turso_core::ast::ColumnDefinition> {
        turso_sqlite::SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parameter_marker_ranges(
        &self,
        sql: &str,
    ) -> turso_core::Result<Vec<std::ops::Range<usize>>> {
        turso_sqlite::SqliteDialect.parameter_marker_ranges(sql)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn supports_namespaces(&self) -> bool {
        true
    }

    fn schema_builder(
        &self,
        enable_custom_types: bool,
    ) -> Result<turso_core::schema::SchemaBuilder> {
        turso_core::schema::builtin::schema_builder(enable_custom_types)
    }
}

#[test]
fn dialect_registers_a_type_written_in_rust() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-rust-type.db", Arc::new(RustTypesDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // `bool` is another name of the boolean type of core.
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s loud, b boolean, b2 bool) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'Hello', 'si', 'no'), (2, NULL, NULL, NULL)")
        .unwrap();
    let rows = conn
        .prepare("SELECT s, b, b2 FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            // ENCODE stored HELLO, and DECODE added the question mark.
            vec![
                Value::build_text("HELLO?"),
                Value::from_i64(1),
                Value::from_i64(0)
            ],
            // Neither runs on NULL.
            vec![Value::Null, Value::Null, Value::Null],
        ]
    );

    // The boolean type of the dialect replaced the type of core, which
    // reads `yes` as true, under both of its names.
    for values in ["(3, 'x', 'yes', 'no')", "(3, 'x', 'si', 'yes')"] {
        let error = conn
            .execute(format!("INSERT INTO t VALUES {values}"))
            .unwrap_err();
        assert!(error.to_string().contains("not si or no"), "{error}");
    }
    conn.close().unwrap();
}

/// A type with a cast conversion takes values in a CAST that a column of the
/// type refuses.
#[test]
fn cast_conversion_of_a_type_runs_in_a_cast_and_not_in_a_column() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_db(&io, "dialect-type-cast.db", Arc::new(RustTypesDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // The conversion turns 5 into `si`, which ENCODE reads. It leaves text
    // to ENCODE, and it does not run on NULL.
    conn.execute("CREATE DOMAIN flag AS boolean").unwrap();
    let rows = conn
        .prepare(
            "SELECT CAST(5 AS boolean), CAST(0 AS boolean), CAST('si' AS boolean), \
             CAST(NULL AS boolean), CAST(5 AS flag)",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::from_i64(1),
            Value::from_i64(0),
            Value::from_i64(1),
            Value::Null,
            // A domain converts as the type it is based on.
            Value::from_i64(1),
        ]]
    );

    // Text that ENCODE refuses fails in a CAST too.
    let error = conn
        .prepare("SELECT CAST('yes' AS boolean)")
        .unwrap()
        .run_collect_rows()
        .unwrap_err();
    assert!(error.to_string().contains("yes is not si or no"), "{error}");

    // A column of the type does not run the conversion.
    conn.execute("CREATE TABLE t (b boolean) STRICT").unwrap();
    let error = conn.execute("INSERT INTO t VALUES (5)").unwrap_err();
    assert!(error.to_string().contains("5 is not si or no"), "{error}");
    conn.close().unwrap();
}

/// A dialect that registers two types written in Rust: `loud`, and a
/// `boolean` that replaces the type of core.
struct RustTypesDialect;

impl Dialect for RustTypesDialect {
    fn name(&self) -> &'static str {
        "rust-types"
    }

    fn parse_internal(
        &self,
        sql: &str,
    ) -> turso_core::Result<(Option<turso_core::ast::Cmd>, usize)> {
        turso_sqlite::SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> turso_core::Result<turso_core::ast::ColumnDefinition> {
        turso_sqlite::SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parameter_marker_ranges(
        &self,
        sql: &str,
    ) -> turso_core::Result<Vec<std::ops::Range<usize>>> {
        turso_sqlite::SqliteDialect.parameter_marker_ranges(sql)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn requires_custom_types(&self) -> bool {
        true
    }

    fn schema_builder(
        &self,
        enable_custom_types: bool,
    ) -> Result<turso_core::schema::SchemaBuilder> {
        turso_core::schema::builtin::schema_builder(enable_custom_types)?
            .builtin_type(&Loud)?
            .builtin_type(&SiNoBoolean)
    }
}

/// Text that is stored in upper case and read with a question mark after it.
#[derive(Debug)]
struct Loud;

impl Type for Loud {
    fn name(&self) -> &str {
        "loud"
    }

    fn params(&self) -> Vec<turso_core::ast::TypeParam> {
        vec![value_param("text")]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(|value, _params| {
            Ok(Value::build_text(value.to_string().to_uppercase()))
        }))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(|value, _params| {
            Ok(Value::build_text(format!("{value}?")))
        }))
    }

    fn reads_as(&self) -> Option<turso_core::types::StorageClass> {
        Some(turso_core::types::StorageClass::Text)
    }
}

/// A boolean that reads `si` as true and `no` as false. A CAST to it also
/// takes an integer.
#[derive(Debug)]
struct SiNoBoolean;

impl Type for SiNoBoolean {
    fn name(&self) -> &str {
        "boolean"
    }

    fn params(&self) -> Vec<turso_core::ast::TypeParam> {
        vec![value_param("any")]
    }

    fn base(&self) -> &str {
        "integer"
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(|value, _params| {
            match value.to_string().as_str() {
                "si" => Ok(Value::from_i64(1)),
                "no" => Ok(Value::from_i64(0)),
                other => Err(LimboError::Constraint(format!("{other} is not si or no"))),
            }
        }))
    }

    fn cast(&self) -> Option<Conversion> {
        Some(Conversion::Function(|value, _params| match value {
            Value::Numeric(Numeric::Integer(0)) => Ok(Value::build_text("no")),
            Value::Numeric(Numeric::Integer(_)) => Ok(Value::build_text("si")),
            other => Ok(other.clone()),
        }))
    }
}

fn value_param(ty: &str) -> turso_core::ast::TypeParam {
    turso_core::ast::TypeParam {
        name: "value".to_string(),
        ty: Some(ty.to_string()),
    }
}

/// A dialect that is SQLite's with catalog `C`.
struct DialectWithCatalog<C> {
    catalog: C,
}

impl<C: turso_core::Catalog + 'static> Dialect for DialectWithCatalog<C> {
    fn name(&self) -> &'static str {
        "catalog-test"
    }

    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse(sql)
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> Result<turso_core::ast::ColumnDefinition> {
        SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parameter_marker_ranges(&self, sql: &str) -> Result<Vec<std::ops::Range<usize>>> {
        SqliteDialect.parameter_marker_ranges(sql)
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect)
    }

    fn catalog(&self) -> &dyn turso_core::Catalog {
        &self.catalog
    }
}

struct CatalogWithAStep {
    /// One entry for each run of the step: did the schema have table `t`?
    table_seen_by_each_load: Arc<std::sync::Mutex<Vec<bool>>>,
}

impl turso_core::Catalog for CatalogWithAStep {
    fn emit(
        &self,
        program: &mut turso_core::ProgramBuilder,
        ctx: &turso_core::CmdContext<'_>,
        cmd: &turso_core::CatalogCmd<'_>,
    ) -> Result<()> {
        turso_core::SqliteCatalog.emit(program, ctx, cmd)
    }

    fn load_steps(&self) -> Vec<Box<dyn turso_core::LoadStep>> {
        vec![Box::new(LooksForTheTable {
            queried: false,
            table_seen_by_each_load: self.table_seen_by_each_load.clone(),
        })]
    }
}

/// A step that reads one row and notes whether table `t` is in the schema
/// being loaded.
struct LooksForTheTable {
    queried: bool,
    table_seen_by_each_load: Arc<std::sync::Mutex<Vec<bool>>>,
}

impl turso_core::LoadStep for LooksForTheTable {
    fn next_query(
        &mut self,
        _load: &mut turso_core::SchemaLoad,
    ) -> Result<Option<turso_core::LoadQuery>> {
        let query = (!self.queried).then(|| {
            turso_core::LoadQuery::engine_metadata_read("SELECT count(*) FROM sqlite_schema")
        });
        self.queried = true;
        Ok(query)
    }

    fn row(&mut self, load: &mut turso_core::SchemaLoad, _row: &turso_core::Row) -> Result<()> {
        self.table_seen_by_each_load
            .lock()
            .unwrap()
            .push(load.schema().has_table(turso_core::NamespaceId::ROOT, "t"));
        Ok(())
    }
}

/// A catalog's step runs after the engine's steps, so it can see the tables
/// that they loaded.
#[test]
fn steps_of_the_catalog_run_after_the_engine_steps_at_open_and_at_reload() {
    let table_seen_by_each_load = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dialect = Arc::new(DialectWithCatalog {
        catalog: CatalogWithAStep {
            table_seen_by_each_load: table_seen_by_each_load.clone(),
        },
    });
    let db = Database::open_file(Arc::new(MemoryIO::new()), ":memory:", dialect).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(*table_seen_by_each_load.lock().unwrap(), [false]);
    conn.execute("CREATE TABLE t (x)").unwrap();

    conn.force_reparse_schema_without_publish().unwrap();

    assert_eq!(*table_seen_by_each_load.lock().unwrap(), [false, true]);
}

/// A catalog that stores what SQLite's stores and notes the commands about
/// triggers, virtual tables and types that it gets.
struct NotesCommands {
    commands: Arc<std::sync::Mutex<Vec<String>>>,
}

impl turso_core::Catalog for NotesCommands {
    fn emit(
        &self,
        program: &mut turso_core::ProgramBuilder,
        ctx: &turso_core::CmdContext<'_>,
        cmd: &turso_core::CatalogCmd<'_>,
    ) -> Result<()> {
        match cmd {
            turso_core::CatalogCmd::CreateTrigger(trigger) => {
                self.commands.lock().unwrap().push(format!(
                    "create {} on {}: {}",
                    trigger.name, trigger.table, trigger.sql
                ));
            }
            turso_core::CatalogCmd::DropTrigger(trigger) => {
                self.commands
                    .lock()
                    .unwrap()
                    .push(format!("drop {}", trigger.name));
            }
            turso_core::CatalogCmd::CreateVirtualTable(table) => {
                self.commands
                    .lock()
                    .unwrap()
                    .push(format!("create virtual table {}", table.name));
            }
            turso_core::CatalogCmd::CreateType(ty) => {
                self.commands
                    .lock()
                    .unwrap()
                    .push(format!("create type {}: {}", ty.name, ty.sql));
            }
            turso_core::CatalogCmd::DropType(ty) => {
                self.commands
                    .lock()
                    .unwrap()
                    .push(format!("drop type {}", ty.name));
            }
            _ => {}
        }
        turso_core::SqliteCatalog.emit(program, ctx, cmd)
    }
}

#[test]
fn the_catalog_gets_a_new_trigger_and_a_dropped_trigger() {
    let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dialect = Arc::new(DialectWithCatalog {
        catalog: NotesCommands {
            commands: commands.clone(),
        },
    });
    let db = Database::open_file(Arc::new(MemoryIO::new()), ":memory:", dialect).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t (x)").unwrap();

    conn.execute("CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END")
        .unwrap();
    conn.execute("DROP TRIGGER tr").unwrap();

    assert_eq!(
        *commands.lock().unwrap(),
        [
            "create tr on t: CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
            "drop tr"
        ]
    );
}

#[test]
fn the_catalog_gets_a_new_virtual_table() {
    let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dialect = Arc::new(DialectWithCatalog {
        catalog: NotesCommands {
            commands: commands.clone(),
        },
    });
    let options = OpenOptions::new(dialect).native_module(
        "one_row",
        turso_ext::VTabKind::VirtualTable,
        TestCatalogTable,
    );
    let db = Database::open(Arc::new(MemoryIO::new()), ":memory:", options).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE VIRTUAL TABLE v USING one_row")
        .unwrap();

    assert_eq!(*commands.lock().unwrap(), ["create virtual table v"]);
}

#[test]
fn the_catalog_gets_new_and_dropped_types_and_domains() {
    let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dialect = Arc::new(DialectWithCatalog {
        catalog: NotesCommands {
            commands: commands.clone(),
        },
    });
    let options = OpenOptions::new(dialect).db_opts(DatabaseOpts::new().with_custom_types(true));
    let db = Database::open(Arc::new(MemoryIO::new()), ":memory:", options).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")
        .unwrap();
    conn.execute("CREATE DOMAIN positive AS integer CHECK (value > 0)")
        .unwrap();
    conn.execute("DROP TYPE cents").unwrap();
    conn.execute("DROP DOMAIN positive").unwrap();

    let commands = commands.lock().unwrap();
    let names: Vec<&str> = commands
        .iter()
        .map(|command| command.split(':').next().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "create type cents",
            "create type positive",
            "drop type cents",
            "drop type positive"
        ]
    );
    assert!(commands[0].contains("CREATE TYPE cents"), "{}", commands[0]);
    assert!(
        commands[1].contains("CREATE DOMAIN positive"),
        "{}",
        commands[1]
    );
}
