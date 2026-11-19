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

#[test]
fn drop_role_fails_under_a_dialect_without_roles() {
    let db = Database::open_file(
        Arc::new(MemoryIO::new()),
        ":memory:",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let drop_role = ast::Cmd::Stmt(ast::Stmt::DropRole {
        if_exists: true,
        roles: vec![ast::Grantee::Role(ast::RoleSpec::Name("alice".to_string()))],
    });

    let error = conn
        .prepare_translated_cmd(drop_role, "DROP ROLE IF EXISTS alice")
        .and_then(|mut stmt| stmt.run_ignore_rows())
        .unwrap_err();

    assert!(
        error.to_string().contains("DROP ROLE is not supported"),
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
            using_sql: None,
            check_sql: None,
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

/// The SQLite parser has no syntax for these statements, so the test
/// builds the statements a frontend with roles would build.
#[test]
fn dialect_without_roles_refuses_role_owner_privilege_and_row_security_statements() {
    let db = Database::open_file(
        Arc::new(MemoryIO::new()),
        ":memory:",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(a)").unwrap();
    conn.execute("CREATE SEQUENCE s").unwrap();

    let table = || ast::QualifiedName::single(ast::Name::exact("t".to_string()));
    let role = || ast::RoleSpec::Name("r".to_string());
    let alter_table = |body| {
        ast::Stmt::AlterTable(ast::AlterTable {
            name: table(),
            body,
        })
    };
    let grant = |revoke, objects| {
        ast::Stmt::Grant(ast::Grant {
            revoke,
            objects,
            grantees: vec![ast::Grantee::Role(role())],
        })
    };
    let privileges_on_table = || ast::GrantObjects::Relations {
        privileges: vec![ast::Privilege::Select],
        on_sequences: false,
        relations: vec![table()],
    };
    let member_roles = || ast::GrantObjects::Roles(vec!["admin".to_string()]);

    let statements = [
        (
            grant(false, privileges_on_table()),
            "GRANT is not supported",
        ),
        (
            grant(true, privileges_on_table()),
            "REVOKE is not supported",
        ),
        (
            ast::Stmt::CreateRole {
                role_name: "r".to_string(),
                options: ast::RoleOptions::default(),
            },
            "CREATE ROLE is not supported",
        ),
        (
            grant(false, member_roles()),
            "GRANT of role membership is not supported",
        ),
        (
            grant(true, member_roles()),
            "REVOKE of role membership is not supported",
        ),
        (
            alter_table(ast::AlterTableBody::OwnerTo {
                kind: ast::RelationKind::Table,
                role: role(),
                if_exists: false,
            }),
            "ALTER ... OWNER TO is not supported",
        ),
        (
            ast::Stmt::AlterSequenceOwnedBy {
                seq_name: ast::QualifiedName::single(ast::Name::exact("s".to_string())),
                owner: Some((table(), ast::Name::exact("a".to_string()))),
            },
            "ALTER SEQUENCE ... OWNED BY is not supported",
        ),
        (
            alter_table(ast::AlterTableBody::EnableRowSecurity),
            "row-level security is not supported",
        ),
        (
            alter_table(ast::AlterTableBody::DisableRowSecurity),
            "row-level security is not supported",
        ),
        (
            ast::Stmt::CreatePolicy(Box::new(ast::CreatePolicy {
                policy_name: ast::Name::exact("p".to_string()),
                tbl_name: table(),
                permissive: true,
                command: ast::PolicyCommand::All,
                roles: vec![ast::Grantee::Public],
                using_expr: None,
                check_expr: None,
                using_sql: None,
                check_sql: None,
            })),
            "row-level security is not supported",
        ),
        (
            ast::Stmt::DropPolicy {
                if_exists: false,
                policy_name: ast::Name::exact("p".to_string()),
                tbl_name: table(),
            },
            "row-level security is not supported",
        ),
    ];

    for (stmt, expected) in statements {
        let description = format!("{stmt:?}");
        match conn.prepare_ast_stmt(stmt) {
            Err(turso_core::LimboError::ParseError(message)) => {
                assert_eq!(message, expected, "{description}")
            }
            Err(other) => panic!("{description}: expected a parse error, got {other:?}"),
            Ok(_) => panic!("{description}: expected an error"),
        }
    }
}
