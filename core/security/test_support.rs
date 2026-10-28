//! A dialect with roles for core tests. The SQLite dialect has no roles, and
//! the PostgreSQL dialect lives outside core.

use turso_parser::ast;

use crate::dialect::sqlite::SqliteDialect;
use crate::dialect::Dialect;
use crate::schema::{BTreeTable, Schema};
use crate::Func;

/// The SQLite dialect, with roles.
pub(crate) struct SqliteWithRoles;

impl Dialect for SqliteWithRoles {
    fn name(&self) -> &'static str {
        "sqlite_with_roles"
    }

    fn parse(&self, sql: &str) -> crate::Result<(Option<ast::Cmd>, usize)> {
        SqliteDialect.parse(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
        SqliteDialect.parse_table_sql(sql, root_page)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<ast::Stmt> {
        SqliteDialect.parse_table_sql_ast(sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
        SqliteDialect.table_sql_for_replay(sql)
    }

    fn format_table_sql(
        &self,
        input: &str,
        tbl_name: &ast::QualifiedName,
        body: &ast::CreateTableBody,
    ) -> crate::Result<String> {
        SqliteDialect.format_table_sql(input, tbl_name, body)
    }

    fn register_catalog(
        &self,
        schema: &mut Schema,
        enable_custom_types: bool,
    ) -> crate::Result<()> {
        SqliteDialect.register_catalog(schema, enable_custom_types)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<Func>> {
        SqliteDialect.resolve_function(name, arg_count)
    }

    fn supports_roles(&self) -> bool {
        true
    }
}
