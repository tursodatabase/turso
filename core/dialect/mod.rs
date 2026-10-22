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

/// Prefix the PostgreSQL frontend puts before the `CREATE TABLE` text it
/// stores in `sqlite_schema`.
pub const POSTGRES_TABLE_SQL_PREFIX: &str = "/* turso_frontend:postgres */ ";

/// The functions that `CREATE FUNCTION` stored, as a statement finds them.
pub trait FunctionCatalog {
    fn find_function(
        &self,
        call_name: &str,
        arg_count: usize,
    ) -> Option<crate::sync::Arc<crate::schema::FunctionDef>>;
}

/// The tables and views that the statements of a dialect trigger body can
/// name. A name resolves as in those statements.
pub trait TableCatalog {
    /// The columns that `SELECT *` gives for the table or view `name`, or
    /// `None` when no table or view has the name.
    fn column_names(&self, name: &turso_parser::ast::QualifiedName) -> Option<Vec<String>>;
}

/// The statements that a dialect compiled for one trigger and one event.
/// Unlike the statements of a SQLite trigger, they can name tables of other
/// databases, and unqualified names resolve as in other statements.
pub struct TriggerBody {
    pub statements: Vec<turso_parser::ast::Stmt>,
    /// The columns that a `BEFORE INSERT` or `BEFORE UPDATE` trigger changes
    /// in the row that the statement writes. After the statements, core
    /// calculates each value and writes it into `NEW`. The next trigger and
    /// the statement see the changed row.
    pub changed_columns: Vec<ChangedColumn>,
}

/// A column that a trigger changes, and the new value. The value can read
/// `NEW` and `OLD`, as the statements of the trigger can.
pub struct ChangedColumn {
    pub name: String,
    pub value: turso_parser::ast::Expr,
}

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

    fn register_native_extensions(&self, options: crate::OpenOptions) -> crate::OpenOptions {
        options
    }

    /// Create the namespaces of a new connection. SQLite has one namespace
    /// per database: `main`, `temp` and each attached database.
    fn new_namespaces(&self) -> crate::sync::Arc<dyn crate::NamespaceCatalog> {
        crate::sync::Arc::new(sqlite::attach::AttachedNamespaces::default())
    }

    /// Resolve a function name in user SQL to the engine's function IR.
    ///
    /// The dialect owns its scalar function surface: the SQLite dialect
    /// resolves the built-in set, another dialect resolves its own —
    /// mapping names onto engine primitives where it wants them (usually
    /// by composing with [`sqlite::resolve_builtin_function`]) and onto
    /// [`crate::Func::Dialect`] for functions it executes
    /// itself via [`Dialect::exec_scalar_function`]. Consulted
    /// before extension functions; engine-generated helper statements
    /// always resolve with SQLite semantics instead.
    fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<crate::Func>>;

    /// Execute a dialect scalar function at runtime.
    ///
    /// Receives the connection — unlike extension functions — because
    /// catalog functions (e.g. `pg_get_tabledef`) need to inspect the
    /// schema. Only reached through [`crate::Func::Dialect`],
    /// so a dialect that never resolves to that variant can keep the
    /// default "no such function" error.
    fn exec_scalar_function(
        &self,
        _conn: &crate::Connection,
        name: &str,
        _args: &[crate::Value],
    ) -> crate::Result<crate::Value> {
        Err(crate::LimboError::ParseError(format!(
            "no such function: {name}"
        )))
    }

    /// Compile a call to a function that `CREATE FUNCTION` stored into the
    /// expression that the engine evaluates in place of the call. `functions`
    /// finds the other stored functions that the statement can call.
    fn compile_function_call(
        &self,
        function: &crate::schema::FunctionDef,
        _args: &[Box<turso_parser::ast::Expr>],
        _functions: &dyn FunctionCatalog,
    ) -> crate::Result<turso_parser::ast::Expr> {
        Err(crate::LimboError::ParseError(format!(
            "cannot execute function {}()",
            function.name
        )))
    }

    /// The statements that run when `trigger` fires on a row of `table`, for
    /// a trigger whose body the dialect owns. `None` runs the statements
    /// stored with the trigger. `tables` gives the columns of the tables that
    /// the statements can name.
    fn trigger_body(
        &self,
        _trigger: &crate::schema::Trigger,
        _table: &crate::schema::BTreeTable,
        _functions: &dyn FunctionCatalog,
        _tables: &dyn TableCatalog,
    ) -> crate::Result<Option<TriggerBody>> {
        Ok(None)
    }

    /// The WHEN condition of `trigger` on `table`, for a trigger whose
    /// condition the dialect owns. `None` uses the condition stored with the
    /// trigger. The condition can read `NEW` and `OLD`.
    fn trigger_when_clause(
        &self,
        _trigger: &crate::schema::Trigger,
        _table: &crate::schema::BTreeTable,
    ) -> crate::Result<Option<turso_parser::ast::Expr>> {
        Ok(None)
    }

    /// Put the triggers that fire for one event at one time in the order in
    /// which they fire. The default keeps the order of SQLite.
    fn order_triggers(&self, _triggers: &mut [crate::sync::Arc<crate::schema::Trigger>]) {}

    /// Whether INSERT gives a new row its rowid before the BEFORE INSERT
    /// triggers, so that NEW has the rowid. SQLite gives the rowid after the
    /// triggers, and NEW has -1. PostgreSQL calculates the default of a key,
    /// such as the next value of a serial column, before the triggers.
    fn rowid_before_insert_triggers(&self) -> bool {
        false
    }

    /// Whether this dialect needs the custom-type machinery (DECODE/ENCODE,
    /// affinity metadata) regardless of the experimental database flag.
    /// A dialect whose type system leans on custom types (e.g. PostgreSQL)
    /// returns true so its databases never open with the machinery off.
    fn requires_custom_types(&self) -> bool {
        false
    }

    /// Whether `t.c` reads only the nearest query that has a table named `t`.
    ///
    /// SQLite keeps searching outer queries when that table has no column
    /// `c`. PostgreSQL stops there and reports that the column does not
    /// exist.
    fn qualified_column_uses_nearest_table_only(&self) -> bool {
        false
    }

    /// Whether the function name `s.f` names the function `f` of the schema
    /// `s`. A SQLite function name has no schema, so `"s.f"` is one name.
    fn qualified_function_names(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::BTreeTable;
    use crate::storage::database::DatabaseFile;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, IO};
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

        fn resolve_function(
            &self,
            name: &str,
            arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            if name.eq_ignore_ascii_case("nvl") {
                return sqlite::resolve_builtin_function("coalesce", arg_count);
            }
            if name.eq_ignore_ascii_case("test_add_one") && arg_count == 1 {
                return Ok(Some(crate::function::Func::Dialect(
                    "test_add_one".to_string(),
                )));
            }
            sqlite::resolve_builtin_function(name, arg_count)
        }

        fn exec_scalar_function(
            &self,
            _conn: &crate::Connection,
            name: &str,
            args: &[crate::Value],
        ) -> crate::Result<crate::Value> {
            assert_eq!(name, "test_add_one");
            let crate::Value::Numeric(crate::numeric::Numeric::Integer(v)) = args[0] else {
                return Err(crate::LimboError::InvalidArgument(
                    "test_add_one expects an integer".to_string(),
                ));
            };
            Ok(crate::Value::Numeric(crate::numeric::Numeric::Integer(
                v + 1,
            )))
        }

        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }

        fn register_native_extensions(&self, options: crate::OpenOptions) -> crate::OpenOptions {
            options.native_module(
                "test_catalog",
                turso_ext::VTabKind::TableValuedFunction,
                TestCatalogTable,
            )
        }

        fn trigger_body(
            &self,
            trigger: &crate::schema::Trigger,
            _table: &BTreeTable,
            _functions: &dyn FunctionCatalog,
            tables: &dyn TableCatalog,
        ) -> crate::Result<Option<TriggerBody>> {
            use turso_parser::ast::{
                Cmd, Expr, Literal, Name, OneSelect, QualifiedName, ResultColumn, Stmt, TriggerCmd,
            };
            let [TriggerCmd::Select(select)] = trigger.commands.as_slice() else {
                return Ok(None);
            };
            let OneSelect::Select { columns, .. } = &select.body.select else {
                return Ok(None);
            };
            let [ResultColumn::Expr(call, _)] = columns.as_slice() else {
                return Ok(None);
            };
            let Expr::FunctionCall { name, args, .. } = call.as_ref() else {
                return Ok(None);
            };
            let text = |arg: &Expr| {
                let Expr::Literal(Literal::String(text)) = arg else {
                    panic!("test trigger bodies are text");
                };
                text[1..text.len() - 1].replace("''", "'")
            };
            if name.as_str() == "test_trigger_columns" {
                let [table] = args.as_slice() else {
                    panic!("test_trigger_columns names one table");
                };
                let table = text(table);
                let table = match table.split_once('.') {
                    Some((database, table)) => QualifiedName::fullname(
                        Name::exact(database.to_string()),
                        Name::exact(table.to_string()),
                    ),
                    None => QualifiedName::single(Name::exact(table)),
                };
                let columns = tables
                    .column_names(&table)
                    .map_or("none".to_string(), |columns| columns.join(","));
                let (Some(Cmd::Stmt(stmt)), _) =
                    sqlite::parse(&format!("INSERT INTO main.log VALUES ('{columns}')"))?
                else {
                    panic!("the log statement is a statement");
                };
                return Ok(Some(TriggerBody {
                    statements: vec![stmt],
                    changed_columns: Vec::new(),
                }));
            }
            if name.as_str() != "test_trigger_body" {
                return Ok(None);
            }
            let [body, changes @ ..] = args.as_slice() else {
                panic!("test trigger bodies have a statement");
            };
            let body = text(body);
            let statements = if body.is_empty() {
                Vec::new()
            } else {
                let (Some(Cmd::Stmt(stmt)), _) = sqlite::parse(&body)? else {
                    panic!("test trigger bodies are statements");
                };
                vec![stmt]
            };
            let changed_columns = changes
                .chunks(2)
                .map(|change| {
                    let [column, value] = change else {
                        panic!("each changed column has a value");
                    };
                    let (Some(Cmd::Stmt(Stmt::Select(select))), _) =
                        sqlite::parse(&format!("SELECT {}", text(value)))?
                    else {
                        panic!("changed values are expressions");
                    };
                    let OneSelect::Select { mut columns, .. } = select.body.select else {
                        panic!("changed values are expressions");
                    };
                    let ResultColumn::Expr(value, _) = columns.remove(0) else {
                        panic!("changed values are expressions");
                    };
                    Ok(ChangedColumn {
                        name: text(column),
                        value: *value,
                    })
                })
                .collect::<crate::Result<_>>()?;
            Ok(Some(TriggerBody {
                statements,
                changed_columns,
            }))
        }

        fn trigger_when_clause(
            &self,
            trigger: &crate::schema::Trigger,
            _table: &BTreeTable,
        ) -> crate::Result<Option<turso_parser::ast::Expr>> {
            if !trigger.name.ends_with("_dialect_when") {
                return Ok(None);
            }
            let (Some(turso_parser::ast::Cmd::Stmt(turso_parser::ast::Stmt::Select(select))), _) =
                sqlite::parse("SELECT new.a = 1")?
            else {
                panic!("the test condition is a SELECT");
            };
            let turso_parser::ast::OneSelect::Select { mut columns, .. } = select.body.select
            else {
                panic!("the test condition is one SELECT");
            };
            let Some(turso_parser::ast::ResultColumn::Expr(condition, _)) = columns.pop() else {
                panic!("the test condition is one column");
            };
            Ok(Some(*condition))
        }

        fn order_triggers(&self, triggers: &mut [Arc<crate::schema::Trigger>]) {
            triggers.sort_by(|left, right| left.name.cmp(&right.name));
        }

        fn compile_function_call(
            &self,
            function: &crate::schema::FunctionDef,
            args: &[Box<turso_parser::ast::Expr>],
            functions: &dyn FunctionCatalog,
        ) -> crate::Result<turso_parser::ast::Expr> {
            if let Some(other) = function.definition.strip_prefix("exists:") {
                let exists = functions.find_function(other, 1).is_some();
                return Ok(turso_parser::ast::Expr::Literal(
                    turso_parser::ast::Literal::Numeric(i64::from(exists).to_string()),
                ));
            }
            let addend = turso_parser::ast::Expr::Literal(turso_parser::ast::Literal::Numeric(
                function.definition.clone(),
            ));
            Ok(turso_parser::ast::Expr::Binary(
                args[0].clone(),
                turso_parser::ast::Operator::Add,
                Box::new(addend),
            ))
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

        fn resolve_function(
            &self,
            name: &str,
            arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            sqlite::resolve_builtin_function(name, arg_count)
        }
    }

    /// Namespaces where CREATE needs a namespace, so a connection that uses
    /// them cannot create an object without one.
    #[derive(Default)]
    struct CreateNeedsNamespace(sqlite::attach::AttachedNamespaces);

    impl crate::NamespaceCatalog for CreateNeedsNamespace {
        fn database_id(&self, name: &str) -> Option<usize> {
            self.0.database_id(name)
        }

        fn name(&self, database_id: usize) -> Option<String> {
            self.0.name(database_id)
        }

        fn others(&self) -> Vec<(usize, String)> {
            self.0.others()
        }

        fn add(&self, name: &str, database_id: usize) {
            self.0.add(name, database_id)
        }

        fn remove(&self, name: &str) -> Option<usize> {
            self.0.remove(name)
        }

        fn find_in_search_order(&self, contains: &mut dyn FnMut(usize) -> bool) -> Option<usize> {
            self.0.find_in_search_order(contains)
        }

        fn database_for_create(&self) -> crate::Result<usize> {
            Err(crate::LimboError::ParseError(
                "no namespace for create".to_string(),
            ))
        }
    }

    /// A dialect whose connections use [`CreateNeedsNamespace`].
    struct CreateNeedsNamespaceDialect;

    impl Dialect for CreateNeedsNamespaceDialect {
        fn name(&self) -> &'static str {
            "create-needs-namespace"
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
            input: &str,
            tbl_name: &turso_parser::ast::QualifiedName,
            body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            SqliteDialect.format_table_sql(input, tbl_name, body)
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

        fn new_namespaces(&self) -> Arc<dyn crate::NamespaceCatalog> {
            Arc::new(CreateNeedsNamespace::default())
        }
    }

    /// A one-row catalog table installed by [`TestDialect`],
    /// standing in for a frontend catalog surface like `pg_class`.
    #[derive(Debug)]
    struct TestCatalogTable;

    impl crate::native_ext::VirtualTableModule for TestCatalogTable {
        type Table = Self;

        fn schema(&self, _args: &[crate::Value]) -> crate::Result<String> {
            Ok("CREATE TABLE test_catalog (value INTEGER)".to_string())
        }

        fn create(&self, _args: &[crate::Value]) -> crate::Result<Self::Table> {
            Ok(Self)
        }
    }

    impl crate::native_ext::VirtualTable for TestCatalogTable {
        type Cursor = TestCatalogCursor;

        fn open(&self, _conn: Arc<crate::Connection>) -> crate::Result<Self::Cursor> {
            Ok(TestCatalogCursor { row: 0 })
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

    impl crate::native_ext::VirtualTableCursor for TestCatalogCursor {
        fn filter(
            &mut self,
            _args: &[crate::Value],
            _idx_str: Option<&str>,
            _idx_num: i32,
        ) -> crate::types::IOResultOr<bool> {
            self.row = 0;
            Ok(crate::IOResult::Done(true))
        }

        fn next(&mut self) -> crate::types::IOResultOr<bool> {
            self.row += 1;
            Ok(crate::IOResult::Done(self.row < 1))
        }

        fn rowid(&self) -> i64 {
            self.row as i64
        }

        fn column(&mut self, column: usize) -> crate::types::IOResultOr<crate::Value> {
            Ok(crate::IOResult::Done(match column {
                0 => crate::Value::from_i64(42),
                _ => crate::Value::Null,
            }))
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

    fn create_function(
        conn: &Arc<crate::Connection>,
        or_replace: bool,
        definition: &str,
    ) -> crate::Result<()> {
        create_named_function(conn, "add_n", or_replace, definition)
    }

    fn create_named_function(
        conn: &Arc<crate::Connection>,
        name: &str,
        or_replace: bool,
        definition: &str,
    ) -> crate::Result<()> {
        let stmt = turso_parser::ast::Stmt::CreateFunction {
            or_replace,
            func_name: turso_parser::ast::QualifiedName::single(
                turso_parser::ast::Name::from_string(name),
            ),
            arg_count: 1,
            definition: definition.to_string(),
        };
        conn.prepare_translated_stmt(stmt, "test: create add_n")?
            .run_ignore_rows()
    }

    fn drop_function(conn: &Arc<crate::Connection>, if_exists: bool) -> crate::Result<()> {
        let stmt = turso_parser::ast::Stmt::DropFunction {
            if_exists,
            func_name: turso_parser::ast::QualifiedName::single(
                turso_parser::ast::Name::from_string("add_n"),
            ),
            arg_count: Some(1),
        };
        conn.prepare_translated_stmt(stmt, "test: drop add_n")?
            .run_ignore_rows()
    }

    fn stored_definition(conn: &Arc<crate::Connection>) -> Option<String> {
        conn.prepare("SELECT 1").unwrap();
        conn.current_schema()
            .get_function("add_n", 1)
            .map(|function| function.definition.clone())
    }

    #[test]
    fn stored_function_survives_reopen() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let db = open_db(&io, "dialect-function.db", Arc::new(TestDialect::default())).unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            create_function(&conn, false, "1").unwrap();
            assert_eq!(stored_definition(&conn).as_deref(), Some("1"));
            conn.close().unwrap();
        }

        let db = open_db(&io, "dialect-function.db", Arc::new(TestDialect::default())).unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        assert_eq!(stored_definition(&conn).as_deref(), Some("1"));
        let error = create_function(&conn, false, "2").unwrap_err();
        assert_eq!(
            error.to_string(),
            "Parse error: function \"add_n\" already exists with same argument types"
        );
        assert_eq!(stored_definition(&conn).as_deref(), Some("1"));
        conn.close().unwrap();
    }

    #[test]
    fn replaced_and_dropped_functions_reach_other_connections() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-function-replace.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        let other = db.connect(ConnectOpts::new()).unwrap();
        create_function(&conn, false, "1").unwrap();
        assert_eq!(stored_definition(&other).as_deref(), Some("1"));

        create_function(&conn, true, "2").unwrap();
        assert_eq!(stored_definition(&conn).as_deref(), Some("2"));
        assert_eq!(stored_definition(&other).as_deref(), Some("2"));

        drop_function(&conn, false).unwrap();
        assert_eq!(stored_definition(&conn), None);
        assert_eq!(stored_definition(&other), None);
        drop_function(&conn, true).unwrap();
        let error = drop_function(&conn, false).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Parse error: function add_n does not exist"
        );
        conn.close().unwrap();
        other.close().unwrap();
    }

    #[test]
    fn rolled_back_function_is_not_stored() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-function-rollback.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("BEGIN").unwrap();
        create_function(&conn, false, "1").unwrap();
        assert_eq!(stored_definition(&conn).as_deref(), Some("1"));
        conn.execute("ROLLBACK").unwrap();
        assert_eq!(stored_definition(&conn), None);
        conn.close().unwrap();
    }

    fn select_add_n(conn: &Arc<crate::Connection>) -> crate::Result<Vec<Vec<crate::Value>>> {
        conn.prepare("SELECT add_n(41)")?.run_collect_rows()
    }

    #[test]
    fn stored_function_call_is_compiled_by_the_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-function-call.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        create_function(&conn, false, "1").unwrap();
        assert_eq!(
            select_add_n(&conn).unwrap(),
            vec![vec![crate::Value::from_i64(42)]]
        );
        let error = conn.prepare("SELECT add_n(1, 2)").unwrap_err();
        assert!(
            error.to_string().contains("no such function: add_n"),
            "unexpected error: {error}"
        );
        conn.close().unwrap();
    }

    #[test]
    fn dialect_finds_other_stored_functions() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-function-catalog.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        create_named_function(&conn, "has_add_n", false, "exists:add_n").unwrap();
        let has_add_n = |conn: &Arc<crate::Connection>| {
            conn.prepare("SELECT has_add_n(0)")
                .unwrap()
                .run_collect_rows()
                .unwrap()
        };
        assert_eq!(has_add_n(&conn), vec![vec![crate::Value::from_i64(0)]]);
        create_function(&conn, false, "1").unwrap();
        assert_eq!(has_add_n(&conn), vec![vec![crate::Value::from_i64(1)]]);
        conn.close().unwrap();
    }

    #[test]
    fn dialect_trigger_body_names_tables_of_other_databases() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let file = io
            .open_file("dialect-trigger-body.db", OpenFlags::Create, true)
            .unwrap();
        let db = Database::open(
            io.clone(),
            "dialect-trigger-body.db",
            crate::OpenOptions::new(Arc::new(TestDialect::default()))
                .storage(Arc::new(DatabaseFile::new(file)))
                .db_opts(DatabaseOpts::new().with_attach(true)),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("ATTACH DATABASE ':memory:' AS aux").unwrap();
        conn.execute("CREATE TABLE aux.t (x INTEGER)").unwrap();
        conn.execute("CREATE TABLE log (x INTEGER, earlier INTEGER)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER aux.tr AFTER INSERT ON t BEGIN \
             SELECT test_trigger_body('INSERT INTO main.log SELECT new.x, count(*) FROM main.log'); \
             END",
        )
        .unwrap();

        let mut insert = conn.prepare("INSERT INTO aux.t VALUES (1), (2)").unwrap();
        insert.run_ignore_rows().unwrap();
        assert_eq!(insert.n_change(), 2);
        let rows = conn
            .prepare("SELECT x, earlier FROM log ORDER BY x")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                vec![crate::Value::from_i64(1), crate::Value::from_i64(0)],
                vec![crate::Value::from_i64(2), crate::Value::from_i64(1)],
            ]
        );
        conn.close().unwrap();
    }

    #[test]
    fn before_insert_trigger_changes_the_new_row() {
        let conn = open_trigger_db("trigger-changes-inserted-row.db");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap();
        conn.execute("CREATE INDEX t_b ON t (b)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN \
             SELECT test_trigger_body('', 'b', 'CAST(new.a * 10 AS TEXT)'); \
             END",
        )
        .unwrap();

        conn.execute("INSERT INTO t (id, a) VALUES (1, 2), (2, 3)")
            .unwrap();

        assert_eq!(
            rows(&conn, "SELECT id, a, b, typeof(b) FROM t ORDER BY id"),
            vec![
                vec![int(1), int(2), int(20), text("integer")],
                vec![int(2), int(3), int(30), text("integer")],
            ]
        );
        assert_eq!(
            rows(&conn, "SELECT id FROM t INDEXED BY t_b WHERE b = 30"),
            vec![vec![int(2)]]
        );
        assert_eq!(
            rows(&conn, "PRAGMA integrity_check"),
            vec![vec![text("ok")]]
        );
    }

    #[test]
    fn before_update_trigger_changes_a_column_that_set_does_not_name() {
        let conn = open_trigger_db("trigger-changes-updated-row.db");
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, g INTEGER AS (b + 1))",
        )
        .unwrap();
        conn.execute("CREATE INDEX t_b ON t (b)").unwrap();
        conn.execute("CREATE INDEX t_g ON t (g)").unwrap();
        conn.execute("INSERT INTO t (id, a, b) VALUES (1, 1, 10)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN \
             SELECT test_trigger_body('', 'b', 'new.a * 10 + old.b'); \
             END",
        )
        .unwrap();

        conn.execute("UPDATE t SET a = 2").unwrap();

        assert_eq!(
            rows(&conn, "SELECT id, a, b, g FROM t"),
            vec![vec![int(1), int(2), int(30), int(31)]]
        );
        assert_eq!(
            rows(&conn, "SELECT id FROM t INDEXED BY t_b WHERE b = 30"),
            vec![vec![int(1)]]
        );
        assert_eq!(
            rows(&conn, "SELECT id FROM t INDEXED BY t_g WHERE g = 31"),
            vec![vec![int(1)]]
        );
        assert_eq!(
            rows(&conn, "PRAGMA integrity_check"),
            vec![vec![text("ok")]]
        );
    }

    #[test]
    fn before_update_trigger_changes_a_column_that_set_names() {
        let conn = open_trigger_db("trigger-changes-set-column.db");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, b INTEGER NOT NULL)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN \
             SELECT test_trigger_body('', 'b', 'nullif(new.b + 1, 100)'); \
             END",
        )
        .unwrap();

        conn.execute("UPDATE t SET b = 5").unwrap();
        assert_eq!(rows(&conn, "SELECT b FROM t"), vec![vec![int(6)]]);

        let error = conn.execute("UPDATE t SET b = 99").unwrap_err();
        assert!(error
            .to_string()
            .contains("NOT NULL constraint failed: t.b"));
        assert_eq!(rows(&conn, "SELECT b FROM t"), vec![vec![int(6)]]);
    }

    #[test]
    fn update_of_trigger_does_not_fire_for_a_column_that_a_trigger_changes() {
        let conn = open_trigger_db("trigger-changes-update-of.db");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE log (x TEXT)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1, 10)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN \
             SELECT test_trigger_body('', 'b', 'new.b + 1'); \
             END",
        )
        .unwrap();
        for column in ["a", "b"] {
            conn.execute(format!(
                "CREATE TRIGGER of_{column} AFTER UPDATE OF {column} ON t BEGIN \
                 INSERT INTO log VALUES ('{column}'); \
                 END"
            ))
            .unwrap();
        }

        conn.execute("UPDATE t SET a = 2").unwrap();

        assert_eq!(rows(&conn, "SELECT b FROM t"), vec![vec![int(11)]]);
        assert_eq!(rows(&conn, "SELECT x FROM log"), vec![vec![text("a")]]);
    }

    #[test]
    fn next_before_trigger_sees_the_changed_row() {
        let conn = open_trigger_db("trigger-changes-next-trigger.db");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER fires_second BEFORE INSERT ON t BEGIN \
             SELECT test_trigger_body('', 'c', 'new.b * 2'); \
             END",
        )
        .unwrap();
        conn.execute(
            "CREATE TRIGGER fires_first BEFORE INSERT ON t BEGIN \
             SELECT test_trigger_body('', 'b', 'new.a + 1'); \
             END",
        )
        .unwrap();

        conn.execute("INSERT INTO t (id, a) VALUES (1, 1)").unwrap();

        assert_eq!(
            rows(&conn, "SELECT a, b, c FROM t"),
            vec![vec![int(1), int(2), int(4)]]
        );
    }

    #[test]
    fn before_update_trigger_changes_the_row_of_an_upsert() {
        let conn = open_trigger_db("trigger-changes-upsert.db");
        conn.execute(
            "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100 OPERATOR '<'",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER NOT NULL, price cents) STRICT",
        )
        .unwrap();
        conn.execute("CREATE TABLE log (x TEXT)").unwrap();
        conn.execute("CREATE INDEX t_b ON t (b)").unwrap();
        conn.execute("CREATE INDEX t_price ON t (price)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1, 10, 5)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN \
             SELECT test_trigger_body('', 'b', 'nullif(new.a * 10 + old.b, 1020)', 'price', 'new.price + 1'); \
             END",
        )
        .unwrap();
        conn.execute(
            "CREATE TRIGGER of_b AFTER UPDATE OF b ON t BEGIN INSERT INTO log VALUES ('b'); END",
        )
        .unwrap();

        conn.execute(
            "INSERT INTO t VALUES (1, 2, 0, 0) ON CONFLICT (id) DO UPDATE SET a = excluded.a",
        )
        .unwrap();

        assert_eq!(
            rows(&conn, "SELECT id, a, b, price FROM t"),
            vec![vec![int(1), int(2), int(30), int(6)]]
        );
        assert_eq!(
            rows(&conn, "SELECT id FROM t INDEXED BY t_b WHERE b = 30"),
            vec![vec![int(1)]]
        );
        assert_eq!(
            rows(&conn, "SELECT id FROM t INDEXED BY t_price WHERE price = 6"),
            vec![vec![int(1)]]
        );
        assert_eq!(rows(&conn, "SELECT count(*) FROM log"), vec![vec![int(0)]]);
        assert_eq!(
            rows(&conn, "PRAGMA integrity_check"),
            vec![vec![text("ok")]]
        );

        let error = conn
            .execute("INSERT INTO t VALUES (1, 99, 0, 0) ON CONFLICT (id) DO UPDATE SET a = 99")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("NOT NULL constraint failed: t.b"));
    }

    #[test]
    fn triggers_fire_in_the_order_of_the_dialect() {
        let conn = open_trigger_db("trigger-order.db");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c TEXT)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER a_first BEFORE INSERT ON t BEGIN \
             SELECT test_trigger_body('', 'c', '''a'''); \
             END",
        )
        .unwrap();
        conn.execute(
            "CREATE TRIGGER b_second BEFORE INSERT ON t BEGIN \
             SELECT test_trigger_body('', 'c', 'new.c || ''b'''); \
             END",
        )
        .unwrap();

        conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

        assert_eq!(rows(&conn, "SELECT c FROM t"), vec![vec![text("ab")]]);
    }

    #[test]
    fn trigger_changes_a_custom_type_column() {
        let conn = open_trigger_db("trigger-changes-custom-type.db");
        conn.execute(
            "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100 OPERATOR '<'",
        )
        .unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, price cents) STRICT")
            .unwrap();
        conn.execute("CREATE INDEX t_price ON t (price)").unwrap();
        for event in ["INSERT", "UPDATE"] {
            conn.execute(format!(
                "CREATE TRIGGER on_{event} BEFORE {event} ON t BEGIN \
                 SELECT test_trigger_body('', 'price', 'new.a'); \
                 END"
            ))
            .unwrap();
        }

        conn.execute("INSERT INTO t (id, a) VALUES (1, 7)").unwrap();
        assert_eq!(rows(&conn, "SELECT price FROM t"), vec![vec![int(7)]]);
        conn.execute("UPDATE t SET a = 8").unwrap();

        assert_eq!(rows(&conn, "SELECT price FROM t"), vec![vec![int(8)]]);
        assert_eq!(
            rows(&conn, "SELECT id FROM t INDEXED BY t_price WHERE price = 8"),
            vec![vec![int(1)]]
        );
        assert_eq!(
            rows(&conn, "PRAGMA integrity_check"),
            vec![vec![text("ok")]]
        );
    }

    #[test]
    fn cdc_records_the_values_that_a_trigger_writes() {
        let conn = open_trigger_db("trigger-changes-cdc.db");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1, 10)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE UPDATE ON t BEGIN \
             SELECT test_trigger_body('', 'a', 'new.a + 1', 'b', 'new.a * 10'); \
             END",
        )
        .unwrap();
        conn.execute("PRAGMA capture_data_changes_conn('full')")
            .unwrap();

        conn.execute("UPDATE t SET a = 2").unwrap();

        assert_eq!(
            rows(
                &conn,
                "SELECT bin_record_json_object('[\"id?\",\"a?\",\"b?\",\"id\",\"a\",\"b\"]', updates) \
                 FROM turso_cdc WHERE table_name = 't'"
            ),
            vec![vec![text(r#"{"id?":0,"a?":1,"b?":1,"id":null,"a":3,"b":20}"#)]]
        );
    }

    #[test]
    fn trigger_cannot_change_the_rowid() {
        let conn = open_trigger_db("trigger-changes-rowid.db");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN \
             SELECT test_trigger_body('', 'id', '5'); \
             END",
        )
        .unwrap();

        let error = conn.execute("INSERT INTO t VALUES (1, 1)").unwrap_err();

        assert!(error
            .to_string()
            .contains("trigger tr cannot change column id"));
    }

    #[test]
    fn after_trigger_cannot_change_the_row() {
        let conn = open_trigger_db("trigger-changes-after.db");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
             SELECT test_trigger_body('', 'a', '5'); \
             END",
        )
        .unwrap();

        let error = conn.execute("INSERT INTO t VALUES (1, 1)").unwrap_err();

        assert!(error
            .to_string()
            .contains("trigger tr cannot change the row in this statement"));
    }

    #[test]
    fn trigger_when_clause_of_the_dialect_replaces_the_stored_condition() {
        let conn = open_trigger_db("trigger-dialect-when.db");
        conn.execute("CREATE TABLE t (a INTEGER)").unwrap();
        conn.execute("CREATE TABLE log (a INTEGER)").unwrap();
        conn.execute(
            "CREATE TRIGGER t_dialect_when AFTER INSERT ON t WHEN 0 BEGIN \
             INSERT INTO log VALUES (new.a); \
             END",
        )
        .unwrap();

        conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();

        assert_eq!(rows(&conn, "SELECT a FROM log"), vec![vec![int(1)]]);
    }

    #[test]
    fn trigger_body_finds_the_columns_of_the_tables_that_it_names() {
        let conn = open_trigger_db("trigger-body-table-columns.db");
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();
        conn.execute("CREATE VIEW v AS SELECT b AS c FROM t")
            .unwrap();
        conn.execute("CREATE TABLE log (columns TEXT)").unwrap();
        for (trigger, table) in [
            ("t1", "t"),
            ("t2", "main.t"),
            ("t3", "v"),
            ("t4", "missing"),
        ] {
            conn.execute(format!(
                "CREATE TRIGGER {trigger} AFTER INSERT ON t BEGIN \
                 SELECT test_trigger_columns('{table}'); \
                 END"
            ))
            .unwrap();
        }

        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();

        assert_eq!(
            rows(&conn, "SELECT columns FROM log ORDER BY columns"),
            vec![
                vec![text("a,b")],
                vec![text("a,b")],
                vec![text("c")],
                vec![text("none")],
            ]
        );
    }

    fn open_trigger_db(path: &str) -> Arc<crate::Connection> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let file = io.open_file(path, OpenFlags::Create, true).unwrap();
        let db = Database::open(
            io.clone(),
            path,
            crate::OpenOptions::new(Arc::new(TestDialect::default()))
                .storage(Arc::new(DatabaseFile::new(file)))
                .db_opts(
                    DatabaseOpts::new()
                        .with_custom_types(true)
                        .with_generated_columns(true),
                ),
        )
        .unwrap();
        db.connect(ConnectOpts::new()).unwrap()
    }

    fn rows(conn: &Arc<crate::Connection>, sql: &str) -> Vec<Vec<crate::Value>> {
        conn.prepare(sql).unwrap().run_collect_rows().unwrap()
    }

    fn int(value: i64) -> crate::Value {
        crate::Value::from_i64(value)
    }

    fn text(value: &str) -> crate::Value {
        crate::Value::build_text(value.to_string())
    }

    #[test]
    fn replaced_function_reprepares_statements_of_other_connections() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-function-reprepare.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        let other = db.connect(ConnectOpts::new()).unwrap();
        create_function(&conn, false, "1").unwrap();
        let mut stmt = other.prepare("SELECT add_n(41)").unwrap();

        create_function(&conn, true, "2").unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![crate::Value::from_i64(43)]]
        );

        drop_function(&conn, false).unwrap();
        stmt.reset().unwrap();
        let error = stmt.run_collect_rows().unwrap_err();
        assert!(
            error.to_string().contains("no such function: add_n"),
            "unexpected error: {error}"
        );
        conn.close().unwrap();
        other.close().unwrap();
    }

    #[test]
    fn schema_load_routes_through_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let db = open_db(&io, "dialect-load.db", Arc::new(SqliteDialect)).unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
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
        let conn = db.connect(ConnectOpts::new()).unwrap();
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
        let conn = db.connect(ConnectOpts::new()).unwrap();

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
    fn connections_use_the_namespaces_of_the_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-namespaces.db",
            Arc::new(CreateNeedsNamespaceDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();

        let Err(crate::LimboError::ParseError(message)) = conn.execute("CREATE TABLE t (x)") else {
            panic!("CREATE without a namespace must use the dialect's namespaces");
        };
        assert_eq!(message, "no namespace for create");
        conn.execute("CREATE TABLE main.t (x)").unwrap();
        conn.close().unwrap();
    }

    #[test]
    fn query_runner_reports_invalid_utf8_once() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "query-runner-invalid-utf8.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
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
        let conn = db.connect(ConnectOpts::new()).unwrap();
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

        let conn1 = db.connect(ConnectOpts::new()).unwrap();
        let conn2 = db.connect(ConnectOpts::new()).unwrap();
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
        let conn = db.connect(ConnectOpts::new()).unwrap();

        let error = conn.execute("DROP TABLE test_catalog").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("table test_catalog may not be dropped"),
            "unexpected error: {error}"
        );

        let new_conn = db.connect(ConnectOpts::new()).unwrap();
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
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.execute("PRAGMA journal_mode = mvcc").unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.close().unwrap();
        }

        let db = open_db(&io, path, Arc::new(TestDialect::default())).unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
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
            let conn = db.connect(ConnectOpts::new()).unwrap();
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
            let conn = db.connect(ConnectOpts::new()).unwrap();

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
        let conn = db.connect(ConnectOpts::new()).unwrap();
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
        let conn = db.connect(ConnectOpts::new()).unwrap();

        // Dialect-provided scalar executes through exec_scalar_function.
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
        let conn = db.connect(ConnectOpts::new()).unwrap();

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
        let conn = db.connect(ConnectOpts::new()).unwrap();

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
        let conn = db.connect(ConnectOpts::new()).unwrap();

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
        let conn = db.connect(ConnectOpts::new()).unwrap();

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
        let conn = db.connect(ConnectOpts::new()).unwrap();

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
        let conn = db.connect(ConnectOpts::new()).unwrap();
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
        let output_conn = output_db.connect(ConnectOpts::new()).unwrap();
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
        let conn = db.connect(ConnectOpts::new()).unwrap();
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
        let output_conn = output_db.connect(ConnectOpts::new()).unwrap();
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
        let conn = db.connect(ConnectOpts::new()).unwrap();
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
