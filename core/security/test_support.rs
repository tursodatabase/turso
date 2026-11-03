//! A dialect with roles for core tests. The SQLite dialect has no roles, and
//! the PostgreSQL dialect lives outside core.

use std::sync::Arc;

use turso_parser::ast;

use crate::dialect::sqlite::SqliteDialect;
use crate::dialect::Dialect;
use crate::schema::{BTreeTable, Schema};
use crate::security::roles::RoleId;
use crate::{ConnectOpts, Connection, Database, Func, MemoryIO, OpenOptions, SessionRole};

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

/// Opens an in-memory database with roles and connects as the superuser.
pub(crate) fn connect_with_roles() -> Arc<Connection> {
    connect_with_roles_and(OpenOptions::new(Arc::new(SqliteWithRoles)))
}

/// Like [connect_with_roles], with `options`, whose dialect must be
/// [SqliteWithRoles].
pub(crate) fn connect_with_roles_and(options: OpenOptions) -> Arc<Connection> {
    Database::open(Arc::new(MemoryIO::new()), ":memory:", options)
        .unwrap()
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap()
}

/// Creates a role that is not a superuser and makes it the current role.
pub(crate) fn become_new_role(conn: &Arc<Connection>, name: &str) -> RoleId {
    let create_role = ast::Cmd::Stmt(ast::Stmt::CreateRole {
        role_name: name.to_string(),
    });
    conn.prepare_translated_cmd(create_role, &format!("CREATE ROLE {name}"))
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    let role = conn.role_catalog().get_by_name(name).unwrap().id;
    conn.set_current_role(role);
    role
}
