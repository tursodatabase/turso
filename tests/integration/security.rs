use std::sync::Arc;
use turso_core::security::roles::SessionRole;
use turso_core::Connection;
use turso_core::Database;
use turso_core::MemoryIO;
use turso_parser::ast;
use turso_sqlite::SqliteDialect;

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

/// A materialized view stores the rows of its query when it is refreshed,
/// so policies on it could not limit them.
#[test]
fn row_security_statements_reject_a_materialized_view() {
    let db = Database::open_file_with_flags(
        Arc::new(MemoryIO::new()),
        "row-security-materialized-view.db",
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_views(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t (x)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW mv AS SELECT x FROM t")
        .unwrap();
    let mv = || ast::QualifiedName::single(ast::Name::exact("mv".to_string()));
    let statements = [
        ast::Stmt::AlterTable(ast::AlterTable {
            name: mv(),
            body: ast::AlterTableBody::EnableRowSecurity,
        }),
        ast::Stmt::CreatePolicy(Box::new(ast::CreatePolicy {
            policy_name: ast::Name::exact("p".to_string()),
            tbl_name: mv(),
            permissive: true,
            command: ast::PolicyCommand::All,
            roles: vec![ast::Grantee::Public],
            using_expr: None,
            check_expr: None,
        })),
    ];

    for statement in statements {
        let sql = statement.to_string();
        let error = conn
            .prepare_translated_cmd(ast::Cmd::Stmt(statement), &sql)
            .unwrap_err();

        assert!(
            error.to_string().contains("\"mv\" is not a table"),
            "{error}"
        );
    }
}

#[test]
fn enabling_row_security_fails_under_a_dialect_without_it() {
    let db = Database::open_file(
        Arc::new(MemoryIO::new()),
        ":memory:",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t (x)").unwrap();
    let enable = ast::Cmd::Stmt(ast::Stmt::AlterTable(ast::AlterTable {
        name: ast::QualifiedName::single(ast::Name::exact("t".to_string())),
        body: ast::AlterTableBody::EnableRowSecurity,
    }));

    let error = conn
        .prepare_translated_cmd(enable, "ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("row-level security is not supported"),
        "{error}"
    );
}
