use std::sync::Arc;
use turso_core::security::roles::SessionRole;
use turso_sqlite::SqliteDialect;

use rustc_hash::FxHashMap;
use turso_parser::ast;

use turso_core::dialect::Dialect;
use turso_core::function::Func;
use turso_core::namespace::{NamespaceId, NamespaceRef};
use turso_core::schema::{BTreeTable, Schema};
use turso_core::security::policy::RowPolicy;
use turso_core::security::roles::RoleId;
use turso_core::translate::emitter::{DoubleQuotedDml, Resolver};
use turso_core::translate::row_security::{
    conditions_of, policy, using_condition, with_check_condition, Command, Condition, Policy,
};
use turso_core::{Database, DatabaseCatalog, MemoryIO, Qualifiers, RwLock, SymbolTable};

/// The engine dialect, except that every table of a schema with dialect
/// schema state has row-level security, and table `documents` has a
/// policy that lets every role access every row.
struct RowSecurityInSchemasWithState;

static ACCESS_EVERY_ROW: std::sync::LazyLock<Vec<RowPolicy>> = std::sync::LazyLock::new(|| {
    vec![RowPolicy {
        name: "everything".to_string(),
        permissive: true,
        command: ast::PolicyCommand::All,
        roles: vec![RoleId::PUBLIC],
        using_expr: Some(ast::Expr::Literal(ast::Literal::True)),
        check_expr: Some(ast::Expr::Literal(ast::Literal::True)),
    }]
});

impl Dialect for RowSecurityInSchemasWithState {
    fn name(&self) -> &'static str {
        SqliteDialect.name()
    }

    fn parse(&self, sql: &str) -> turso_core::Result<(Option<ast::Cmd>, usize)> {
        SqliteDialect.parse(sql)
    }

    fn parse_internal(&self, sql: &str) -> turso_core::Result<(Option<ast::Cmd>, usize)> {
        SqliteDialect.parse_internal(sql)
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> turso_core::Result<ast::ColumnDefinition> {
        SqliteDialect.parse_internal_column_definition(sql)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> turso_core::Result<BTreeTable> {
        SqliteDialect.parse_table_sql(sql, root_page)
    }

    fn parse_table_sql_ast(&self, sql: &str) -> turso_core::Result<ast::Stmt> {
        SqliteDialect.parse_table_sql_ast(sql)
    }

    fn table_sql_for_replay(&self, sql: &str) -> turso_core::Result<String> {
        SqliteDialect.table_sql_for_replay(sql)
    }

    fn format_table_sql(
        &self,
        input: &str,
        tbl_name: &ast::QualifiedName,
        body: &ast::CreateTableBody,
    ) -> turso_core::Result<String> {
        SqliteDialect.format_table_sql(input, tbl_name, body)
    }

    fn register_catalog(
        &self,
        schema: &mut Schema,
        enable_custom_types: bool,
    ) -> turso_core::Result<()> {
        SqliteDialect.register_catalog(schema, enable_custom_types)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> turso_core::Result<Option<Func>> {
        SqliteDialect.resolve_function(name, arg_count)
    }

    fn table_rls_enabled(&self, schema: &Schema, _namespace: NamespaceId, _name: &str) -> bool {
        schema.dialect_schema.is_some()
    }

    fn row_security_policies<'a>(
        &self,
        _schema: &'a Schema,
        _namespace: NamespaceId,
        name: &str,
    ) -> &'a [RowPolicy] {
        if name == "documents" {
            &ACCESS_EVERY_ROW
        } else {
            &[]
        }
    }
}

/// The permissive policies' conditions are one condition, combined with
/// OR, and each restrictive policy's condition is one more, named after
/// the policy, as in PostgreSQL.
#[test]
fn conditions_combine_permissive_policies_and_keep_restrictive_ones_apart() {
    let policies = [
        row_policy("a", true, Some("a = 1"), None),
        row_policy("b", true, Some("b = 1"), Some("b = 2")),
        row_policy("r", false, Some("r = 1"), None),
    ];

    assert_eq!(
        texts(&conditions_of(&policies, using_condition)),
        vec![
            (None, "(a = 1) OR (b = 1)".to_string()),
            (Some("r".to_string()), "r = 1".to_string()),
        ]
    );
    assert_eq!(
        texts(&conditions_of(&policies, with_check_condition)),
        vec![
            (None, "(a = 1) OR (b = 2)".to_string()),
            (Some("r".to_string()), "r = 1".to_string()),
        ]
    );
}

/// Without a permissive policy that has the condition, no row passes,
/// whatever the restrictive policies say.
#[test]
fn conditions_without_a_permissive_policy_pass_no_row() {
    let policies = [
        row_policy("insert_only", true, None, Some("a = 1")),
        row_policy("r", false, Some("r = 1"), None),
    ];

    assert_eq!(
        texts(&conditions_of(&policies, using_condition)),
        vec![(None, "0".to_string())]
    );
    assert_eq!(
        texts(&conditions_of(&[], with_check_condition)),
        vec![(None, "0".to_string())]
    );
}

fn row_policy(
    name: &str,
    permissive: bool,
    using_expr: Option<&str>,
    check_expr: Option<&str>,
) -> RowPolicy {
    let expr = |sql: &str| {
        let Some(ast::Cmd::Stmt(ast::Stmt::Select(select))) =
            SqliteDialect.parse(&format!("SELECT {sql}")).unwrap().0
        else {
            panic!("{sql} is not an expression");
        };
        let ast::OneSelect::Select { columns, .. } = select.body.select else {
            panic!("{sql} is not an expression");
        };
        let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
            panic!("{sql} is not an expression");
        };
        *expr.clone()
    };
    RowPolicy {
        name: name.to_string(),
        permissive,
        command: ast::PolicyCommand::All,
        roles: vec![RoleId::PUBLIC],
        using_expr: using_expr.map(expr),
        check_expr: check_expr.map(expr),
    }
}

fn texts(conditions: &[Condition]) -> Vec<(Option<String>, String)> {
    conditions
        .iter()
        .map(|condition| (condition.policy.clone(), condition.expr.to_string()))
        .collect()
}

/// Foreign-key actions see every row, as in PostgreSQL, so a cascading
/// delete also removes the rows that row-level security hides.
#[test]
fn row_security_limits_nothing_compiled_as_superuser() {
    let (main, alice) = main_schema_with_role_alice();
    with_resolver(&main, FxHashMap::default(), alice, |resolver| {
        let root = NamespaceRef::root_of(turso_core::MAIN_DB_ID);

        let inside =
            resolver.as_superuser(|resolver| policy(resolver, root, "documents", Command::Delete));

        assert_eq!(inside, Policy::Allow);
        assert_eq!(
            policy(resolver, root, "documents", Command::Delete),
            Policy::Apply(ACCESS_EVERY_ROW.clone())
        );
    });
}

/// Whether a table of an attached database has row-level security is a
/// question for the attached database, not for the main database.
#[test]
fn table_of_an_attached_database_with_row_security_denies_a_role() {
    let (mut main, alice) = main_schema_with_role_alice();
    main.dialect_schema = None;
    let mut attached = Schema::with_options(true, &SqliteDialect).unwrap();
    attached.dialect_schema = Some(Arc::new(()));
    let attached_database_id = 2;
    let mut database_schemas = FxHashMap::default();
    database_schemas.insert(attached_database_id, Arc::new(attached));

    let policy = with_resolver(&main, database_schemas, alice, |resolver| {
        policy(
            resolver,
            NamespaceRef::root_of(attached_database_id),
            "t",
            Command::Select,
        )
    });

    assert_eq!(policy, Policy::Apply(Vec::new()));
}

/// A main database whose tables have row-level security, and the role
/// alice, who is not a superuser.
fn main_schema_with_role_alice() -> (Schema, RoleId) {
    let alice = RoleId::from_stored(1);
    let mut main = Schema::with_options(true, &SqliteDialect).unwrap();
    main.dialect_schema = Some(Arc::new(()));
    (main, alice)
}

/// Calls `f` with a resolver that compiles statements for `role` under
/// [`RowSecurityInSchemasWithState`].
fn with_resolver<T>(
    main: &Schema,
    attached_schemas: FxHashMap<usize, Arc<Schema>>,
    role: RoleId,
    f: impl FnOnce(&mut Resolver) -> T,
) -> T {
    let database_schemas = RwLock::new(attached_schemas);
    let temp_database = RwLock::new(None);
    let attached_databases = RwLock::new(DatabaseCatalog::new());
    let qualifiers = Qualifiers::default();
    let syms = SymbolTable::new();
    let mut resolver = Resolver::new(
        main,
        &database_schemas,
        &temp_database,
        &attached_databases,
        &qualifiers,
        &syms,
        true,
        DoubleQuotedDml::Enabled,
        Arc::new(RowSecurityInSchemasWithState),
        None,
        role,
    );
    f(&mut resolver)
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
