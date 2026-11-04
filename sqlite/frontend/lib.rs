//! The SQLite frontend.
//!
//! [`SqliteDialect`] is the [`Dialect`] for SQLite: statements are SQLite
//! SQL and `sqlite_schema` rows are canonical SQLite text, parsed with the
//! SQLite parser.

use turso_core::dialect::engine;
use turso_core::schema::{BTreeTable, Schema};
use turso_core::Dialect;
use turso_core::Func;
use turso_parser::ast;

pub use turso_core::dialect::engine::{SQLITE_VERSION, SQLITE_VERSION_NUMBER};

/// The SQLite dialect.
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn parse(&self, sql: &str) -> turso_core::Result<(Option<ast::Cmd>, usize)> {
        engine::parse(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> turso_core::Result<BTreeTable> {
        engine::reject_postgres_table_sql(sql)?;
        BTreeTable::from_sql(sql, root_page)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> turso_core::Result<ast::Stmt> {
        engine::reject_postgres_table_sql(sql)?;
        engine::parse_table_sql_ast(sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> turso_core::Result<String> {
        engine::reject_postgres_table_sql(sql)?;
        engine::table_sql_for_replay(sql)
    }

    fn format_table_sql(
        &self,
        _input: &str,
        tbl_name: &ast::QualifiedName,
        body: &ast::CreateTableBody,
    ) -> turso_core::Result<String> {
        engine::format_table_sql(tbl_name, body)
    }

    fn register_catalog(
        &self,
        schema: &mut Schema,
        enable_custom_types: bool,
    ) -> turso_core::Result<()> {
        engine::register_builtin_catalog(schema, enable_custom_types)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> turso_core::Result<Option<Func>> {
        engine::resolve_builtin_function(name, arg_count)
    }
}
