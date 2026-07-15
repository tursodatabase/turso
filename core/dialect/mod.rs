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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::BTreeTable;
    use crate::storage::database::DatabaseFile;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::{Database, DatabaseOpts, MemoryIO, OpenFlags, IO};
    use std::sync::Arc;

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
        Database::open_with_flags_with_allocator(
            io.clone(),
            path,
            db_file,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            None,
            crate::alloc::DynAllocator::default(),
            dialect,
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
