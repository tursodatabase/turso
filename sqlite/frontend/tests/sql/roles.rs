use std::sync::Arc;
use turso_core::security::roles::SessionRole;
use turso_sqlite::SqliteDialect;

use turso_parser::ast;

use turso_core::{Connection, Database, MemoryIO};

#[test]
fn create_role_fails_under_a_dialect_without_roles() {
    let db = Database::open_file(
        Arc::new(MemoryIO::new()),
        ":memory:",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let error = create_role(&conn, "alice").unwrap_err();

    assert!(
        error.to_string().contains("CREATE ROLE is not supported"),
        "{error}"
    );
}

fn create_role(conn: &Arc<Connection>, name: &str) -> turso_core::Result<()> {
    let create_role = ast::Cmd::Stmt(ast::Stmt::CreateRole {
        role_name: name.to_string(),
        options: ast::RoleOptions::default(),
    });
    conn.prepare_translated_cmd(create_role, &format!("CREATE ROLE {name}"))?
        .run_ignore_rows()
}
