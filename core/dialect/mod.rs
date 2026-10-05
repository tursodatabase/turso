//! SQL dialects.
//!
//! The [`Dialect`] trait is the boundary between the engine and the SQL
//! dialect a frontend speaks. The engine owns the mechanics — pages,
//! B-trees, the `sqlite_schema` table itself, bytecode — and consults the
//! dialect wherever the meaning of SQL text is dialect-specific: parsing
//! statements into the engine AST and interpreting persisted schema text.
//! The [`sqlite`] module owns [`SqliteDialect`], the SQLite
//! implementation, and the catalog tables that ship with every Turso
//! build (`pragma_*`, `json_each`/`json_tree`, `sqlite_dbpage`,
//! `btree_dump`, `sqlite_turso_types`).

pub mod sqlite;

pub use sqlite::SqliteDialect;

/// SQL dialect layered on top of the engine.
///
/// Every [`crate::Database`] carries a dialect, supplied explicitly by
/// every open path and fixed for the lifetime of the database;
/// SQLite-compatible callers pass [`SqliteDialect`]. Initial statement
/// preparation, re-preparation, and every schema load go through this
/// interface.
pub trait Dialect: Send + Sync + 'static {
    /// Stable identifier for this dialect (e.g. "sqlite", "postgres").
    ///
    /// A database file must always be opened with the same dialect it was
    /// created with; the process-wide database registry uses this name to
    /// reject an open whose dialect differs from the already-open instance.
    fn name(&self) -> &'static str;

    /// Parse the first statement in `sql` into the engine AST.
    ///
    /// Returns the parsed command, if any, and the number of input bytes
    /// consumed. The engine uses the same method for initial preparation and
    /// re-preparation, so dialect-specific SQL remains valid after schema or
    /// connection compilation state changes. Implementations must accept the
    /// canonical SQLite text produced by the engine AST formatter because
    /// engine-generated and AST-only statements use that representation.
    fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)>;

    /// Parse a `sqlite_schema` `type='table'` row's SQL into a table
    /// definition.
    ///
    /// Rows written by internal engine paths (sequence backing tables,
    /// `sqlite_sequence`) are plain SQLite text and carry no frontend
    /// marker, so every implementation must fall back to SQLite parsing
    /// for text it does not recognize as its own.
    fn parse_table_sql(
        &self,
        sql: &str,
        root_page: i64,
    ) -> crate::Result<crate::schema::BTreeTable>;

    /// Decode a storage-backed table's persisted SQL into its `CREATE TABLE`
    /// AST.
    ///
    /// Unlike [`Dialect::parse`], this method receives SQL read from
    /// `sqlite_schema` and must recognize the representation produced by
    /// [`Dialect::format_table_sql`] and
    /// [`Dialect::format_rewritten_table_sql`]. Internal engine tables use
    /// plain SQLite text, so implementations must retain the same SQLite
    /// fallback required by [`Dialect::parse_table_sql`].
    fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt>;

    /// Recover SQL that can be prepared to recreate a persisted table.
    ///
    /// Dialects that wrap original frontend DDL in their stored representation
    /// must unwrap it here so replay preserves that DDL. The returned statement
    /// must create the table in the connection's main schema, even when the
    /// persisted statement originally qualified the source database. Unmarked
    /// internal engine tables must retain the SQLite fallback used by the
    /// schema parsing methods.
    fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String>;

    /// Produce the SQL text to store in `sqlite_schema` for a
    /// `CREATE TABLE`.
    ///
    /// `input` is the original statement text as the user wrote it, in the
    /// frontend's dialect; `tbl_name` and `body` are the translated AST.
    /// The SQLite dialect formats canonical SQLite text from the AST; a
    /// frontend dialect typically stores `input` with a marker it can
    /// recognize in [`Dialect::parse_table_sql`].
    fn format_table_sql(
        &self,
        input: &str,
        tbl_name: &turso_parser::ast::QualifiedName,
        body: &turso_parser::ast::CreateTableBody,
    ) -> crate::Result<String>;

    /// Produce stored SQL after the engine rewrites a `CREATE TABLE` AST.
    ///
    /// Schema rewrites cannot reuse the original frontend text because it no
    /// longer describes the rewritten table. Dialects that need syntax beyond
    /// a marker around canonical SQL can override this to render their native
    /// table definition from the rewritten AST.
    fn format_rewritten_table_sql(&self, stmt: &turso_parser::ast::Stmt) -> crate::Result<String> {
        let turso_parser::ast::Stmt::CreateTable { tbl_name, body, .. } = stmt else {
            return Err(crate::LimboError::InternalError(
                "format_rewritten_table_sql requires CREATE TABLE".to_string(),
            ));
        };
        self.format_table_sql(&stmt.to_string(), tbl_name, body)
    }

    /// Install the dialect's catalog tables into a freshly constructed
    /// schema.
    ///
    /// Called by [`crate::schema::Schema::with_options`] on every schema
    /// construction and rebuild, so catalog tables survive rebuilds
    /// structurally instead of being re-registered by hand. The SQLite
    /// dialect registers the standard built-in catalog here; other
    /// dialects typically compose with it via
    /// [`sqlite::register_builtin_catalog`] and then add their own tables
    /// (constructed with [`crate::VirtualTable::new_internal`], which
    /// requires no connection).
    fn register_catalog(
        &self,
        schema: &mut crate::schema::Schema,
        enable_custom_types: bool,
    ) -> crate::Result<()>;

    /// Resolve a function name in user SQL to the engine's function IR.
    ///
    /// The dialect owns its scalar function surface: the SQLite dialect
    /// resolves the built-in set, another dialect resolves its own —
    /// mapping names onto engine primitives where it wants them (usually
    /// by composing with [`sqlite::resolve_builtin_function`]) and onto
    /// [`crate::Func::Dialect`] for Rust scalar functions. Consulted
    /// before extension functions; engine-generated helper statements
    /// always resolve with SQLite semantics instead.
    fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<crate::Func>>;

    fn function_list(&self) -> Vec<crate::FunctionListEntry> {
        Vec::new()
    }

    /// Whether this dialect needs the custom-type machinery (DECODE/ENCODE,
    /// affinity metadata) regardless of the experimental database flag.
    /// A dialect whose type system leans on custom types (e.g. PostgreSQL)
    /// returns true so its databases never open with the machinery off.
    fn requires_custom_types(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::BTreeTable;
    use crate::storage::database::DatabaseFile;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::sync::Mutex;
    use crate::{Database, DatabaseOpts, IOResult, MemoryIO, OpenFlags, IO};
    use std::sync::Arc;

    /// A dialect that counts schema-row parses and strips a `/* test */ `
    /// marker before delegating to SQLite parsing, mirroring how a frontend
    /// dialect recognizes its own stored text and falls back to SQLite for
    /// unmarked rows.
    #[derive(Default)]
    struct TestDialect {
        parse_calls: AtomicUsize,
        statement_parse_calls: AtomicUsize,
        scalar_calls: Arc<AtomicUsize>,
        scalar_completions: Arc<Mutex<Vec<crate::Completion>>>,
    }

    impl Dialect for TestDialect {
        fn name(&self) -> &'static str {
            "test"
        }

        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            self.statement_parse_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(sql) = sql.strip_prefix("test: ") {
                let (cmd, offset) = sqlite::parse(sql)?;
                Ok((cmd, "test: ".len() + offset))
            } else {
                sqlite::parse(sql)
            }
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            self.parse_calls.fetch_add(1, Ordering::SeqCst);
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            sqlite::parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            sqlite::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(format!("/* test */ {input}"))
        }

        fn resolve_function(
            &self,
            name: &str,
            arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            if name.eq_ignore_ascii_case("nvl") {
                return sqlite::resolve_builtin_function("coalesce", arg_count);
            }
            let function: Arc<dyn crate::ScalarFunction> =
                if name.eq_ignore_ascii_case("test_add_one") {
                    Arc::new(AddOne)
                } else if name.eq_ignore_ascii_case("test_counter") {
                    Arc::new(Counter {
                        calls: self.scalar_calls.clone(),
                    })
                } else if name.eq_ignore_ascii_case("test_null_to_nine") {
                    Arc::new(NullToNine)
                } else if name.eq_ignore_ascii_case("test_nested_sum") {
                    Arc::new(NestedSum {
                        calls: self.scalar_calls.clone(),
                        completions: self.scalar_completions.clone(),
                    })
                } else {
                    return sqlite::resolve_builtin_function(name, arg_count);
                };
            Ok(function
                .arity()
                .accepts(arg_count)
                .then_some(crate::function::Func::Dialect(function)))
        }

        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)?;
            let vtab = crate::VirtualTable::new_internal(
                "test_catalog".to_string(),
                "CREATE TABLE test_catalog (value INTEGER)".to_string(),
                turso_ext::VTabKind::VirtualTable,
                Arc::new(crate::sync::RwLock::new(TestCatalogTable)),
            )?;
            schema.add_virtual_table(Arc::new(vtab))
        }
    }

    #[derive(Debug)]
    struct AddOne;

    impl crate::ScalarFunction for AddOne {
        fn name(&self) -> &str {
            "test_add_one"
        }

        fn arity(&self) -> crate::FunctionArity {
            crate::FunctionArity::Exact(1)
        }

        fn is_deterministic(&self) -> bool {
            true
        }

        fn call(
            &self,
            _conn: &Arc<crate::Connection>,
            args: &[crate::Register],
            _state: &mut crate::ScalarFunctionState,
        ) -> crate::types::IOResultOr<crate::Value> {
            let Some(v) = args[0].get_value().as_int() else {
                return Err(crate::LimboError::InvalidArgument(
                    "test_add_one expects an integer".to_string(),
                )
                .into());
            };
            Ok(crate::IOResult::Done(crate::Value::from_i64(v + 1)))
        }
    }

    #[derive(Debug)]
    struct Counter {
        calls: Arc<AtomicUsize>,
    }

    impl crate::ScalarFunction for Counter {
        fn name(&self) -> &str {
            "test_counter"
        }

        fn arity(&self) -> crate::FunctionArity {
            crate::FunctionArity::Exact(0)
        }

        fn call(
            &self,
            _conn: &Arc<crate::Connection>,
            _args: &[crate::Register],
            _state: &mut crate::ScalarFunctionState,
        ) -> crate::types::IOResultOr<crate::Value> {
            Ok(crate::IOResult::Done(crate::Value::from_i64(
                self.calls.fetch_add(1, Ordering::SeqCst) as i64 + 1,
            )))
        }
    }

    #[derive(Debug)]
    struct NullToNine;

    impl crate::ScalarFunction for NullToNine {
        fn name(&self) -> &str {
            "test_null_to_nine"
        }

        fn arity(&self) -> crate::FunctionArity {
            crate::FunctionArity::Exact(1)
        }

        fn call(
            &self,
            _conn: &Arc<crate::Connection>,
            args: &[crate::Register],
            _state: &mut crate::ScalarFunctionState,
        ) -> crate::types::IOResultOr<crate::Value> {
            Ok(crate::IOResult::Done(match args[0].get_value() {
                crate::Value::Null => crate::Value::from_i64(9),
                value => value.clone(),
            }))
        }
    }

    #[derive(Debug)]
    struct NestedSum {
        calls: Arc<AtomicUsize>,
        completions: Arc<Mutex<Vec<crate::Completion>>>,
    }

    impl crate::ScalarFunction for NestedSum {
        fn name(&self) -> &str {
            "test_nested_sum"
        }

        fn arity(&self) -> crate::FunctionArity {
            crate::FunctionArity::Exact(1)
        }

        fn call(
            &self,
            conn: &Arc<crate::Connection>,
            args: &[crate::Register],
            state: &mut crate::ScalarFunctionState,
        ) -> crate::types::IOResultOr<crate::Value> {
            let state = state.get_or_init::<NestedSumState>();
            if state.statement.is_none() {
                state.statement = Some(conn.prepare_internal("SELECT x FROM t")?);
                let completion = crate::Completion::new_write(|_| {});
                self.completions.lock().push(completion.clone());
                state.completion = Some(completion);
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
            let completion = state.completion.as_ref().unwrap();
            if !completion.finished() {
                return Ok(crate::IOResult::IO(crate::types::IOCompletions(
                    completion.clone(),
                )));
            }
            let statement = state.statement.as_mut().unwrap();
            crate::return_if_io!(statement.run_with_row_callback_nonblock(|row| {
                state.sum += row.get_value(0).as_int().unwrap();
                Ok(())
            }));
            let offset = args[0].get_value().as_int().unwrap();
            if offset < 0 {
                return Err(crate::LimboError::InvalidArgument(
                    "test_nested_sum expects a nonnegative offset".to_string(),
                )
                .into());
            }
            Ok(crate::IOResult::Done(crate::Value::from_i64(
                state.sum + offset,
            )))
        }
    }

    #[derive(Default)]
    struct NestedSumState {
        statement: Option<crate::Statement>,
        sum: i64,
        completion: Option<crate::Completion>,
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

        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            sqlite::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            sqlite::parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            sqlite::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(format!("{}{input}", Self::PREFIX))
        }

        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }

        fn resolve_function(
            &self,
            name: &str,
            arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            sqlite::resolve_builtin_function(name, arg_count)
        }
    }

    /// A one-row catalog table installed by [`TestDialect`],
    /// standing in for a frontend catalog surface like `pg_class`.
    #[derive(Debug)]
    struct TestCatalogTable;

    impl crate::InternalVirtualTable for TestCatalogTable {
        fn name(&self) -> String {
            "test_catalog".to_string()
        }

        fn sql(&self) -> String {
            "CREATE TABLE test_catalog (value INTEGER)".to_string()
        }

        fn open(
            &self,
            _conn: Arc<crate::Connection>,
        ) -> crate::Result<Arc<crate::sync::RwLock<dyn crate::InternalVirtualTableCursor>>>
        {
            Ok(Arc::new(crate::sync::RwLock::new(TestCatalogCursor {
                row: 0,
            })))
        }

        fn best_index(
            &self,
            constraints: &[turso_ext::ConstraintInfo],
            _order_by: &[turso_ext::OrderByInfo],
        ) -> std::result::Result<turso_ext::IndexInfo, turso_ext::ResultCode> {
            Ok(turso_ext::IndexInfo {
                idx_num: 0,
                idx_str: None,
                order_by_consumed: false,
                estimated_cost: 1.0,
                estimated_rows: 1,
                constraint_usages: constraints
                    .iter()
                    .map(|_| turso_ext::ConstraintUsage {
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

    impl crate::InternalVirtualTableCursor for TestCatalogCursor {
        fn filter(
            &mut self,
            _args: &[crate::Value],
            _idx_str: Option<String>,
            _idx_num: i32,
        ) -> crate::Result<bool> {
            self.row = 0;
            Ok(true)
        }

        fn next(&mut self) -> crate::Result<bool> {
            self.row += 1;
            Ok(self.row < 1)
        }

        fn rowid(&self) -> i64 {
            self.row as i64
        }

        fn column(&self, column: usize) -> crate::Result<crate::Value> {
            match column {
                0 => Ok(crate::Value::Numeric(crate::numeric::Numeric::Integer(42))),
                _ => Ok(crate::Value::Null),
            }
        }
    }

    fn open_db(
        io: &Arc<dyn IO>,
        path: &str,
        dialect: Arc<dyn Dialect>,
    ) -> crate::Result<Arc<Database>> {
        let file = io.open_file(path, OpenFlags::Create, true)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Database::open(
            io.clone(),
            path,
            crate::OpenOptions::new(dialect).storage(db_file),
        )
    }

    #[test]
    fn schema_load_routes_through_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let db = open_db(&io, "dialect-load.db", Arc::new(SqliteDialect)).unwrap();
            let conn = db.connect().unwrap();
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
        let conn = db.connect().unwrap();
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
        let conn = db.connect().unwrap();

        let mut stmt = conn.prepare("test: SELECT 42").unwrap();
        conn.set_full_column_names(true);
        let rows = stmt.run_collect_rows().unwrap();

        assert_eq!(rows, vec![vec![crate::Value::from_i64(42)]]);
        assert_eq!(
            stmt.stmt_status(crate::StatementStatusCounter::Reprepare),
            1
        );
        assert_eq!(dialect.statement_parse_calls.load(Ordering::SeqCst), 2);
        conn.close().unwrap();
    }

    #[test]
    fn query_runner_reports_invalid_utf8_once() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "query-runner-invalid-utf8.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let mut runner = conn.query_runner(b"SELECT 1;\xff");

        let Some(Err(crate::LimboError::ParseError(message))) = runner.next() else {
            panic!("invalid UTF-8 must produce a parse error");
        };
        assert!(message.contains("invalid UTF-8"));
        assert!(runner.next().is_none());
        conn.close().unwrap();
    }

    #[test]
    fn query_runner_reports_parse_error_once() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "query-runner-parse-error.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let mut runner = conn.query_runner(b"SELECT * FROM");

        assert!(runner.next().is_some_and(|result| result.is_err()));
        assert!(runner.next().is_none());
        conn.close().unwrap();
    }

    #[test]
    fn dialect_catalog_available_on_every_schema_and_rebuild() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-catalog.db", Arc::new(TestDialect::default())).unwrap();

        let query_catalog = |conn: &Arc<crate::Connection>| -> Vec<Vec<crate::Value>> {
            conn.prepare("SELECT value FROM test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap()
        };

        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();
        assert_eq!(
            query_catalog(&conn1),
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(42)
            )]]
        );
        assert_eq!(
            query_catalog(&conn2),
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(42)
            )]]
        );

        // DDL on another connection forces conn1 to rebuild its schema from
        // sqlite_schema; the catalog table must survive because schema
        // construction re-registers it.
        conn2.execute("CREATE TABLE t (x INTEGER)").unwrap();
        assert_eq!(
            query_catalog(&conn1),
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(42)
            )]]
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
        let conn = db.connect().unwrap();

        let error = conn.execute("DROP TABLE test_catalog").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("table test_catalog may not be dropped"),
            "unexpected error: {error}"
        );

        let new_conn = db.connect().unwrap();
        for catalog_conn in [&conn, &new_conn] {
            let rows = catalog_conn
                .prepare("SELECT value FROM test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows, vec![vec![crate::Value::from_i64(42)]]);
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
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA journal_mode = mvcc").unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.close().unwrap();
        }

        let db = open_db(&io, path, Arc::new(TestDialect::default())).unwrap();
        let conn = db.connect().unwrap();
        let rows = conn
            .prepare("SELECT value FROM test_catalog")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![crate::Value::from_i64(42)]]);
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
            let conn = db.connect().unwrap();
            conn.execute(format!("PRAGMA temp_store = {temp_store}"))
                .unwrap();
            conn.execute("CREATE TEMP TABLE t (x INTEGER)").unwrap();
            let rows = conn
                .prepare("SELECT value FROM temp.test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows, vec![vec![crate::Value::from_i64(42)]]);
            conn.close().unwrap();
        }
    }

    #[test]
    fn create_table_stores_dialect_formatted_sql() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let dialect = Arc::new(TestDialect::default());
            let db = open_db(&io, "dialect-store.db", dialect).unwrap();
            let conn = db.connect().unwrap();

            // A frontend prepares its translated AST while supplying the
            // original statement text.
            let input = "CREATE TABLE t (x INTEGER)";
            let stmt = match turso_parser::parser::Parser::new(input.as_bytes())
                .next_cmd()
                .unwrap()
                .unwrap()
            {
                turso_parser::ast::Cmd::Stmt(stmt) => stmt,
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
        let conn = db.connect().unwrap();
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

        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            sqlite::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            sqlite::parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            sqlite::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            _input: &str,
            tbl_name: &turso_parser::ast::QualifiedName,
            body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(format!(
                "CREATE TABLE {} {}",
                tbl_name.name.as_ident(),
                body
            ))
        }

        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }

        fn resolve_function(
            &self,
            _name: &str,
            _arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            Ok(None)
        }
    }

    #[test]
    fn dialect_scalar_function_resolves_and_executes() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-funcs.db", Arc::new(TestDialect::default())).unwrap();
        let conn = db.connect().unwrap();

        let rows = conn
            .prepare("SELECT test_add_one(41)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(42)
            )]]
        );

        // Built-ins still resolve because the dialect composes with the
        // shared table.
        let rows = conn
            .prepare("SELECT abs(-7)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(7)
            )]]
        );

        // Unknown names still error.
        let err = conn.prepare("SELECT no_such_function(1)").unwrap_err();
        assert!(err.to_string().contains("no such function"));

        let err = conn.prepare("SELECT test_add_one()").unwrap_err();
        assert!(err.to_string().contains("no such function"));
        let err = conn.prepare("SELECT test_add_one(1, 2)").unwrap_err();
        assert!(err.to_string().contains("no such function"));
        let err = conn
            .prepare("SELECT test_add_one(NULL)")
            .unwrap()
            .run_collect_rows()
            .unwrap_err();
        assert!(err.to_string().contains("test_add_one expects an integer"));
        conn.close().unwrap();
    }

    #[test]
    fn scalar_function_arity_checks_exact_counts_and_overloads() {
        use crate::FunctionArity;

        for (arity, accepted) in [
            (FunctionArity::Exact(0), &[0][..]),
            (FunctionArity::Exact(2), &[2][..]),
            (FunctionArity::OneOf(&[1, 3]), &[1, 3][..]),
        ] {
            for count in 0..=4 {
                assert_eq!(arity.accepts(count), accepted.contains(&count));
            }
        }
        assert!(FunctionArity::Variadic.accepts(0));
        assert!(FunctionArity::Variadic.accepts(100));
    }

    #[test]
    fn dialect_scalar_function_defaults_to_nondeterministic() {
        use crate::function::Deterministic;

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let dialect = Arc::new(TestDialect::default());
        assert!(!dialect
            .resolve_function("test_counter", 0)
            .unwrap()
            .unwrap()
            .is_deterministic());
        assert!(dialect
            .resolve_function("test_add_one", 1)
            .unwrap()
            .unwrap()
            .is_deterministic());

        let db = open_db(&io, "dialect-counter.db", dialect.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (3), (8), (12)").unwrap();
        let rows = conn
            .prepare("SELECT x, test_counter(), test_counter() FROM t ORDER BY x")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![
                    crate::Value::from_i64(3),
                    crate::Value::from_i64(1),
                    crate::Value::from_i64(2)
                ],
                vec![
                    crate::Value::from_i64(8),
                    crate::Value::from_i64(3),
                    crate::Value::from_i64(4)
                ],
                vec![
                    crate::Value::from_i64(12),
                    crate::Value::from_i64(5),
                    crate::Value::from_i64(6)
                ],
            ]
        );
        assert_eq!(dialect.scalar_calls.load(Ordering::SeqCst), 6);
        conn.close().unwrap();
    }

    #[test]
    fn dialect_scalar_function_resumes_and_clears_each_call_state() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-scalar-io.db", dialect.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (2), (11)").unwrap();
        let mut statement = conn
            .prepare("SELECT x, test_nested_sum(x), test_nested_sum(x + 10) FROM t ORDER BY x")
            .unwrap();

        for execution in 1..=2 {
            let (rows, io_count) = run_scalar_statement(&mut statement, &dialect, &io);
            assert_eq!(io_count, 4);
            assert_eq!(
                rows,
                vec![
                    vec![
                        crate::Value::from_i64(2),
                        crate::Value::from_i64(15),
                        crate::Value::from_i64(25),
                    ],
                    vec![
                        crate::Value::from_i64(11),
                        crate::Value::from_i64(24),
                        crate::Value::from_i64(34),
                    ],
                ]
            );
            assert_eq!(dialect.scalar_calls.load(Ordering::SeqCst), execution * 4);
            assert!(!conn.is_nested_stmt());
            statement.reset().unwrap();
        }
        conn.close().unwrap();
    }

    #[test]
    fn dialect_scalar_function_releases_helpers_before_rollback() {
        for journal_mode in ["wal", "mvcc"] {
            for cancellation in ["reset", "drop", "error", "io_error", "interrupt"] {
                let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
                let dialect = Arc::new(TestDialect::default());
                let db = open_db(
                    &io,
                    &format!("dialect-scalar-{journal_mode}-{cancellation}.db"),
                    dialect.clone(),
                )
                .unwrap();
                let conn = db.connect().unwrap();
                conn.execute(format!("PRAGMA journal_mode = {journal_mode}"))
                    .unwrap();
                conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
                conn.execute("INSERT INTO t VALUES (2), (11)").unwrap();
                conn.execute("CREATE TABLE output (x INTEGER)").unwrap();
                if cancellation == "interrupt" {
                    conn.set_progress_handler(1, Some(Box::new(|| false)));
                }
                let offset = if cancellation == "error" { -1 } else { 3 };
                let mut statement = conn
                    .prepare(format!(
                        "INSERT INTO output SELECT 100 UNION ALL SELECT test_nested_sum({offset})"
                    ))
                    .unwrap();
                assert!(matches!(statement.step().unwrap(), crate::StepResult::IO));
                let completion = dialect.scalar_completions.lock().pop().unwrap();
                if cancellation == "io_error" {
                    completion.error(crate::error::CompletionError::IOError(
                        std::io::ErrorKind::Other,
                        "test scalar I/O",
                    ));
                } else {
                    completion.complete(0);
                }
                assert_eq!(statement.n_change(), 1);
                assert!(conn.is_nested_stmt());

                match cancellation {
                    "reset" => statement.reset().unwrap(),
                    "drop" => drop(statement),
                    "error" => {
                        let error = statement.step().unwrap_err();
                        assert!(error.to_string().contains("nonnegative offset"));
                    }
                    "io_error" => {
                        let error = statement.step().unwrap_err();
                        assert!(error.to_string().contains("test scalar I/O"));
                    }
                    "interrupt" => {
                        conn.interrupt();
                        assert!(matches!(
                            statement.step().unwrap(),
                            crate::StepResult::Interrupt
                        ));
                        assert!(!conn.is_nested_stmt());
                        statement.reset().unwrap();
                    }
                    _ => unreachable!(),
                }
                assert!(!conn.is_nested_stmt(), "{journal_mode}: {cancellation}");
                assert_eq!(
                    conn.prepare("SELECT count(*) FROM output")
                        .unwrap()
                        .run_collect_rows()
                        .unwrap(),
                    vec![vec![crate::Value::from_i64(0)]],
                    "{journal_mode}: {cancellation}"
                );
                conn.execute("INSERT INTO output VALUES (29)").unwrap();
                conn.close().unwrap();
                let reopened = db.connect().unwrap();
                assert_eq!(
                    reopened
                        .prepare("SELECT x FROM output")
                        .unwrap()
                        .run_collect_rows()
                        .unwrap(),
                    vec![vec![crate::Value::from_i64(29)]],
                    "{journal_mode}: {cancellation}"
                );
                reopened.close().unwrap();
            }
        }
    }

    #[test]
    fn dialect_scalar_function_in_trigger_releases_helpers_before_rollback() {
        for journal_mode in ["wal", "mvcc"] {
            for nested_trigger in [false, true] {
                for explicit_transaction in [false, true] {
                    for cancellation in [
                        "reset",
                        "drop",
                        "error",
                        "io_error",
                        "reset_io_error",
                        "drop_io_error",
                        "interrupt",
                    ] {
                        let case = format!(
                            "{journal_mode}: {nested_trigger}: {explicit_transaction}: {cancellation}"
                        );
                        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
                        let dialect = Arc::new(TestDialect::default());
                        let db =
                            open_db(&io, "dialect-trigger-cleanup.db", dialect.clone()).unwrap();
                        let conn = db.connect().unwrap();
                        conn.execute(format!("PRAGMA journal_mode = {journal_mode}"))
                            .unwrap();
                        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
                        conn.execute("INSERT INTO t VALUES (2), (11)").unwrap();
                        conn.execute("CREATE TABLE output (x INTEGER PRIMARY KEY)")
                            .unwrap();
                        conn.execute("CREATE TABLE trigger_output (x INTEGER PRIMARY KEY)")
                            .unwrap();
                        let offset = if cancellation == "error" { -1 } else { 3 };
                        if nested_trigger {
                            conn.execute(format!(
                                "CREATE TRIGGER inner_trigger AFTER INSERT ON trigger_output BEGIN \
                                    SELECT test_nested_sum({offset}); \
                                END"
                            ))
                            .unwrap();
                        }
                        let scalar_call = if nested_trigger {
                            String::new()
                        } else {
                            format!("SELECT test_nested_sum({offset});")
                        };
                        conn.execute(format!(
                            "CREATE TRIGGER outer_trigger AFTER INSERT ON output \
                                WHEN new.x = 100 BEGIN \
                                    INSERT INTO trigger_output VALUES (200); \
                                    {scalar_call} \
                                END"
                        ))
                        .unwrap();
                        if explicit_transaction {
                            conn.execute("BEGIN").unwrap();
                        }
                        conn.execute("INSERT INTO output VALUES (7), (19)").unwrap();
                        let total_changes_before = conn.total_changes();
                        if cancellation == "interrupt" {
                            conn.set_progress_handler(1, Some(Box::new(|| false)));
                        }
                        let mut statement =
                            conn.prepare("INSERT INTO output VALUES (100)").unwrap();
                        assert!(matches!(statement.step().unwrap(), crate::StepResult::IO));
                        let completion = dialect.scalar_completions.lock().pop().unwrap();
                        if matches!(
                            cancellation,
                            "io_error" | "reset_io_error" | "drop_io_error"
                        ) {
                            completion.error(crate::error::CompletionError::IOError(
                                std::io::ErrorKind::Other,
                                "test scalar I/O",
                            ));
                        } else {
                            completion.complete(0);
                        }
                        assert_eq!(statement.n_change(), 1);
                        assert!(conn.is_nested_stmt());

                        match cancellation {
                            "reset" => statement.reset().unwrap(),
                            "drop" | "drop_io_error" => drop(statement),
                            "reset_io_error" => {
                                let error = statement.reset().unwrap_err();
                                assert!(error.to_string().contains("test scalar I/O"));
                            }
                            "error" => {
                                let error = statement.step().unwrap_err();
                                assert!(error.to_string().contains("nonnegative offset"));
                            }
                            "io_error" => {
                                let error = statement.step().unwrap_err();
                                assert!(error.to_string().contains("test scalar I/O"));
                            }
                            "interrupt" => {
                                conn.interrupt();
                                assert!(matches!(
                                    statement.step().unwrap(),
                                    crate::StepResult::Interrupt
                                ));
                                assert!(!conn.is_nested_stmt());
                                statement.reset().unwrap();
                            }
                            _ => unreachable!(),
                        }
                        assert!(!conn.is_nested_stmt(), "{case}");
                        assert!(conn.executing_triggers.read().is_empty(), "{case}");
                        assert_eq!(conn.last_insert_rowid(), 100, "{case}");
                        assert_eq!(conn.changes(), 2, "{case}");
                        assert_eq!(conn.total_changes() - total_changes_before, 1, "{case}");
                        assert_eq!(conn.get_auto_commit(), !explicit_transaction, "{case}");
                        let counts = conn
                            .prepare(
                                "SELECT count(*) FROM output \
                                    UNION ALL SELECT count(*) FROM trigger_output",
                            )
                            .unwrap()
                            .run_collect_rows()
                            .unwrap();
                        assert_eq!(
                            counts,
                            vec![
                                vec![crate::Value::from_i64(2)],
                                vec![crate::Value::from_i64(0)]
                            ],
                            "{case}"
                        );
                        if explicit_transaction {
                            conn.execute("COMMIT").unwrap();
                        }
                        conn.execute("INSERT INTO output VALUES (29)").unwrap();
                        conn.close().unwrap();
                        let reopened = db.connect().unwrap();
                        assert_eq!(
                            reopened
                                .prepare("SELECT x FROM output ORDER BY x")
                                .unwrap()
                                .run_collect_rows()
                                .unwrap(),
                            vec![
                                vec![crate::Value::from_i64(7)],
                                vec![crate::Value::from_i64(19)],
                                vec![crate::Value::from_i64(29)],
                            ],
                            "{case}"
                        );
                        reopened.close().unwrap();
                    }
                }
            }
        }
    }

    #[cfg(feature = "io_memory_yield")]
    #[test]
    fn dialect_scalar_function_propagates_nested_statement_io() {
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryYieldIO::new());
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-scalar-nested-io.db", dialect.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t (x INTEGER, padding BLOB)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, zeroblob(3000)), (11, zeroblob(3000))")
            .unwrap();
        conn.get_pager().clear_page_cache(false);

        let mut statement = conn.prepare("SELECT test_nested_sum(3)").unwrap();
        assert!(matches!(statement.step().unwrap(), crate::StepResult::IO));
        dialect.scalar_completions.lock().pop().unwrap().complete(0);
        let (rows, io_count) = run_scalar_statement(&mut statement, &dialect, &io);
        assert!(
            io_count >= 3,
            "expected reads of the root and two leaf pages"
        );
        assert_eq!(rows, vec![vec![crate::Value::from_i64(16)]]);
        assert_eq!(dialect.scalar_calls.load(Ordering::SeqCst), 1);
        assert!(!conn.is_nested_stmt());
        conn.close().unwrap();
    }

    fn run_scalar_statement(
        statement: &mut crate::Statement,
        dialect: &TestDialect,
        io: &Arc<dyn IO>,
    ) -> (Vec<Vec<crate::Value>>, usize) {
        let mut rows = Vec::new();
        let mut io_count = 0;
        loop {
            match statement.step().unwrap() {
                crate::StepResult::IO => {
                    io_count += 1;
                    let calls = dialect.scalar_calls.load(Ordering::SeqCst);
                    assert!(matches!(statement.step().unwrap(), crate::StepResult::IO));
                    assert_eq!(dialect.scalar_calls.load(Ordering::SeqCst), calls);
                    let pending = statement.take_io_completions().unwrap();
                    assert!(!pending.finished());
                    if let Some(completion) = dialect.scalar_completions.lock().pop() {
                        completion.complete(0);
                    }
                    io.step().unwrap();
                    assert!(pending.finished());
                }
                crate::StepResult::Row => {
                    rows.push(statement.row().unwrap().get_values().cloned().collect());
                }
                crate::StepResult::Done => return (rows, io_count),
                other => panic!("unexpected scalar statement result: {other:?}"),
            }
        }
    }

    #[test]
    fn dialect_scalar_function_can_return_a_value_for_null() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-null.db", Arc::new(TestDialect::default())).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE lhs (id INTEGER)").unwrap();
        conn.execute("CREATE TABLE rhs (id INTEGER, value INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO lhs VALUES (1), (2)").unwrap();
        conn.execute("INSERT INTO rhs VALUES (1, 0)").unwrap();

        let rows = conn
            .prepare(
                "SELECT lhs.id FROM lhs LEFT JOIN rhs ON rhs.id = lhs.id \
                 WHERE test_null_to_nine(rhs.value) = 9 ORDER BY lhs.id",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![crate::Value::from_i64(2)]]);
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
        let conn = db.connect().unwrap();

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
        assert_eq!(rows, vec![vec![crate::Value::from_i64(2)]]);
        conn.close().unwrap();
    }

    #[test]
    fn dialect_owns_the_function_surface() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-nofuncs.db", Arc::new(NoFunctionsDialect)).unwrap();
        let conn = db.connect().unwrap();

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
        let conn = db.connect().unwrap();

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
        assert_eq!(rows[0][0], crate::Value::from_i64(1));
        assert_eq!(rows[0][1], crate::Value::build_text("t"));
        assert_eq!(rows[0][2], crate::Value::from_i64(1));
        assert_eq!(rows[1][0], crate::Value::from_i64(2));
        assert_eq!(rows[1][1], crate::Value::Null);
        assert_eq!(rows[1][2], crate::Value::Null);
        assert_eq!(rows[0][3], rows[1][3]);
        assert_eq!(rows[2][0], crate::Value::from_i64(1));
        assert_eq!(rows[2][1], crate::Value::build_text("t"));
        assert_eq!(rows[2][2], crate::Value::from_i64(2));
        assert_eq!(rows[3][0], crate::Value::from_i64(2));
        assert_eq!(rows[3][1], crate::Value::Null);
        assert_eq!(rows[3][2], crate::Value::Null);
        assert_eq!(rows[2][3], rows[3][3]);
        conn.close().unwrap();
    }

    #[test]
    fn alter_table_rewrites_dialect_formatted_sql() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-alter-table.db", Arc::new(StrictTestDialect)).unwrap();
        let conn = db.connect().unwrap();

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
        let conn = db.connect().unwrap();

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
            vec![vec![crate::Value::from_i64(1)]]
        );
        conn.close().unwrap();
    }

    #[test]
    fn registry_rejects_dialect_mismatch() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let _db = open_db(&io, "dialect-mismatch.db", Arc::new(SqliteDialect)).unwrap();

        let err =
            open_db(&io, "dialect-mismatch.db", Arc::new(TestDialect::default())).unwrap_err();
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
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            source_path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
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
        let output_conn = output_db.connect().unwrap();
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
            vec![vec![crate::Value::from_i64(42)]]
        );
    }

    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn vacuum_attached_database_strips_source_schema_from_replay() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.db");
        let attached_path = dir.path().join("attached.db");
        let output_path = dir.path().join("output.db");
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            source_path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new().with_attach(true),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
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
        let output_conn = output_db.connect().unwrap();
        assert_eq!(
            output_conn
                .prepare("SELECT x FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![crate::Value::from_i64(42)]]
        );
    }

    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn in_place_vacuum_replays_schema_with_source_dialect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.db");
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new().with_vacuum(true),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
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
            vec![vec![crate::Value::from_i64(42)]]
        );
    }
}
