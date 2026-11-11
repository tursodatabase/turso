//! Row-level security limits which rows of a table a role may access.
//!
//! The statements that read and write rows call the hooks of this module,
//! which decide which rows of a table the role may access from whether the
//! table has row-level security, [`crate::Dialect::table_rls_enabled`], and
//! who owns it, and limit the statement to those rows. The role is the one
//! the statement is compiled for, [`Resolver::row_security_role`].
//!
//! Which tables have row-level security is the dialect's schema state, like
//! the owner of a table. The module compiles the statements that enable and
//! disable it into a [`RelationChange`] for the dialect to record.

use std::sync::Arc;

use turso_parser::ast;

use crate::{
    bail_parse_error,
    dialect::RelationChange,
    namespace::{NamespaceId, NamespaceRef},
    schema::{BTreeTable, Schema, Table},
    security::permission::{Action, Object, PermissionDenied},
    security::policy::{Command, Policy},
    security::roles::{RoleCatalog, RoleId},
    translate::{
        emitter::Resolver,
        plan::{JoinOrigin, TableReferences, WhereTerm, WhereTermOrigin},
        roles,
        schema::emit_schema_cookie,
    },
    vdbe::builder::ProgramBuilder,
    vtab::{VirtualTable, VirtualTableType},
    Connection, Dialect, LimboError, Result, MAIN_DB_ID,
};

/// Limits the rows of the tables in `table_references` that the statement
/// reads, updates or deletes to the rows the role may access, by adding a
/// filter to `where_clause` for each table that the role may not access in
/// full.
///
/// For the right side of a LEFT JOIN the filter is part of the join
/// condition, so a hidden row reads as NULLs like a row that does not exist.
/// The filters go before the statement's own conditions.
pub(crate) fn filter_rows(
    command: Command,
    table_references: &TableReferences,
    where_clause: &mut Vec<WhereTerm>,
    resolver: &Resolver,
) -> Result<()> {
    let mut filters = Vec::new();
    for table in table_references.joined_tables() {
        let Table::BTree(btree) = &table.table else {
            continue;
        };
        match policy(resolver, table.namespace, &btree.declared_name, command) {
            Policy::Allow => continue,
            Policy::Deny => {}
        }
        reject_joins_that_keep_right_rows(table_references, &btree.declared_name)?;
        let is_outer = table
            .join_info
            .as_ref()
            .is_some_and(|join_info| join_info.is_outer());
        filters.push(WhereTerm {
            expr: no_rows(),
            origin: if is_outer {
                WhereTermOrigin::Join(JoinOrigin::Outer(table.internal_id))
            } else {
                WhereTermOrigin::Where
            },
            consumed: false,
        });
    }
    where_clause.splice(0..0, filters);
    Ok(())
}

/// Checks that the role may insert rows into `table`.
pub(crate) fn check_insert(
    table: &BTreeTable,
    namespace: NamespaceRef,
    resolver: &Resolver,
) -> Result<()> {
    match policy(resolver, namespace, &table.declared_name, Command::Insert) {
        Policy::Allow => Ok(()),
        Policy::Deny => Err(LimboError::PermissionDenied(Box::new(
            PermissionDenied::policy(Object::Table(table.declared_name.clone())),
        ))),
    }
}

/// Checks that the role may read the virtual table. A virtual table that
/// shows the database file would show the rows that row-level security
/// hides, so only a superuser may read it, like a PRAGMA.
pub(crate) fn check_virtual_table(table: &VirtualTable, resolver: &Resolver) -> Result<()> {
    let superuser_only = matches!(
        &table.vtab_type,
        VirtualTableType::Internal(internal) if internal.read().superuser_only()
    );
    if superuser_only && resolver.role_is_not_superuser {
        return Err(LimboError::PermissionDenied(Box::new(
            PermissionDenied::privilege(Action::Read, Object::Table(table.name.clone())),
        )));
    }
    Ok(())
}

/// Checks that the current role may open a BLOB handle on a row of `table`
/// of the root namespace, which reads and writes the row without a
/// statement.
pub(crate) fn check_blob_open(
    connection: &Arc<Connection>,
    table_name: &str,
    read_write: bool,
) -> Result<()> {
    let (command, action) = if read_write {
        (Command::Update, Action::Write)
    } else {
        (Command::Select, Action::Read)
    };
    let dialect = connection.dialect();
    let policy = connection.with_schema(MAIN_DB_ID, |schema| {
        policy_for(
            dialect.as_ref(),
            &schema.roles,
            schema,
            connection.current_role(),
            NamespaceId::ROOT,
            table_name,
            command,
        )
    });
    match policy {
        Policy::Allow => Ok(()),
        Policy::Deny => Err(LimboError::PermissionDenied(Box::new(
            PermissionDenied::privilege(action, Object::Table(table_name.to_string())),
        ))),
    }
}

/// Which rows of table `table_name` of `namespace` the role may access for
/// `command`. Inside a view, the view's owner reads the table, as for
/// privileges. Whether the table has row-level security is a question for
/// the table's own database, so a table of an attached database with
/// row-level security is limited too. The roles are those of the main
/// database, which the role was set from.
fn policy(
    resolver: &Resolver,
    namespace: NamespaceRef,
    table_name: &str,
    command: Command,
) -> Policy {
    let roles = resolver.schema().roles.clone();
    // A statement the engine compiles as the superuser stays unlimited
    // inside a view.
    let role = if roles.is_superuser(resolver.row_security_role) {
        resolver.row_security_role
    } else {
        roles::table_reader(resolver, resolver.row_security_role)
    };
    resolver.with_schema(namespace.database_id, |schema| {
        policy_for(
            resolver.catalog.dialect.as_ref(),
            &roles,
            schema,
            role,
            namespace.id,
            table_name,
            command,
        )
    })
}

/// Which rows of table `table_name` of `namespace` in `schema` `role` may
/// access for `command`. As in PostgreSQL's `check_enable_rls`, row-level
/// security does not apply to superusers or to the table's owner.
fn policy_for(
    dialect: &dyn Dialect,
    roles: &RoleCatalog,
    schema: &Schema,
    role: RoleId,
    namespace: NamespaceId,
    table_name: &str,
    command: Command,
) -> Policy {
    let table_name = crate::util::normalize_ident(table_name);
    if roles.is_superuser(role)
        || !dialect.table_rls_enabled(schema, namespace, &table_name)
        || dialect.relation_owner(schema, namespace, &table_name) == Some(role)
    {
        return Policy::Allow;
    }
    // Policies cannot be created yet, and a table with row-level security and
    // no policy for the command hides every row, as in PostgreSQL.
    match command {
        Command::Select | Command::Insert | Command::Update | Command::Delete => Policy::Deny,
        // As in PostgreSQL, row-level security does not apply to TRUNCATE.
        Command::Truncate => Policy::Allow,
    }
}

/// A RIGHT or FULL JOIN emits the unmatched rows of its right side after the
/// join loop, without the filters of the tables on its left. A filter on
/// either side would then hide too many rows or too few.
fn reject_joins_that_keep_right_rows(
    table_references: &TableReferences,
    table_name: &str,
) -> Result<()> {
    for join_info in table_references
        .joined_tables()
        .iter()
        .filter_map(|table| table.join_info.as_ref())
    {
        if join_info.is_full_outer() {
            bail_parse_error!(
                "FULL JOIN with table \"{table_name}\" that has row-level security is not supported"
            );
        }
        if join_info.keeps_right_rows() {
            bail_parse_error!(
                "RIGHT JOIN with table \"{table_name}\" that has row-level security is not supported"
            );
        }
    }
    Ok(())
}

/// A condition that no row passes.
fn no_rows() -> ast::Expr {
    ast::Expr::Literal(ast::Literal::Numeric("0".to_string()))
}

/// ALTER TABLE ... ENABLE ROW LEVEL SECURITY, or DISABLE when `enabled` is
/// false. The dialect records it with the table, so it follows the table
/// when the table is renamed and goes away when the table is dropped.
/// [`crate::translate::roles::check_statement_privileges`] lets the table's
/// owner and superusers run it.
pub(crate) fn translate_set_row_security(
    qualified_name: &ast::QualifiedName,
    enabled: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let namespace = resolver.resolve_existing_table(qualified_name)?;
    let table_name = crate::util::normalize_ident(qualified_name.name.as_str());
    if namespace.database_id != MAIN_DB_ID {
        bail_parse_error!("row-level security is only supported for tables in the main database");
    }
    let schema = resolver.schema();
    if schema.get_btree_table(namespace.id, &table_name).is_none() {
        if schema.get_table(namespace.id, &table_name).is_some()
            || schema.get_view(namespace.id, &table_name).is_some()
            || schema
                .get_materialized_view(namespace.id, &table_name)
                .is_some()
        {
            bail_parse_error!("\"{table_name}\" is not a table");
        }
        bail_parse_error!(
            "no such table: {}",
            crate::util::table_name_for_error(qualified_name)
        );
    }
    let changes = [RelationChange::SetRowSecurity {
        namespace: namespace.id,
        name: table_name,
        enabled,
    }];
    program.begin_write_on_database(MAIN_DB_ID, schema.schema_version)?;
    resolver
        .catalog
        .dialect
        .translate_relation_changes(program, schema, connection, &changes)?;
    emit_schema_cookie(resolver, program);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustc_hash::FxHashMap;
    use turso_parser::ast;

    use super::{policy, Command, Policy};
    use crate::dialect::{Dialect, EngineDialect};
    use crate::function::Func;
    use crate::namespace::{NamespaceId, NamespaceRef};
    use crate::schema::{BTreeTable, Schema};
    use crate::security::roles::RoleCatalog;
    use crate::translate::emitter::{DoubleQuotedDml, Resolver};
    use crate::{Database, DatabaseCatalog, MemoryIO, Qualifiers, RwLock, SymbolTable, Value};

    /// The engine dialect, except that every table of a schema with dialect
    /// schema state has row-level security.
    struct RowSecurityInSchemasWithState;

    impl Dialect for RowSecurityInSchemasWithState {
        fn name(&self) -> &'static str {
            EngineDialect.name()
        }

        fn parse(&self, sql: &str) -> crate::Result<(Option<ast::Cmd>, usize)> {
            EngineDialect.parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            EngineDialect.parse_table_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<ast::Stmt> {
            EngineDialect.parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            EngineDialect.table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            input: &str,
            tbl_name: &ast::QualifiedName,
            body: &ast::CreateTableBody,
        ) -> crate::Result<String> {
            EngineDialect.format_table_sql(input, tbl_name, body)
        }

        fn register_catalog(
            &self,
            schema: &mut Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            EngineDialect.register_catalog(schema, enable_custom_types)
        }

        fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<Func>> {
            EngineDialect.resolve_function(name, arg_count)
        }

        fn table_rls_enabled(&self, schema: &Schema, _namespace: NamespaceId, _name: &str) -> bool {
            schema.dialect_schema.is_some()
        }
    }

    /// Whether a table of an attached database has row-level security is a
    /// question for the attached database, not for the main database.
    #[test]
    fn table_of_an_attached_database_with_row_security_denies_a_role() {
        let roles = RoleCatalog::new(&[vec![
            Value::from_i64(1),
            Value::build_text("alice"),
            Value::from_i64(0),
        ]])
        .unwrap();
        let alice = roles.get_by_name("alice").unwrap().id;
        let mut main = Schema::default();
        main.roles = Arc::new(roles);
        let mut attached = Schema::default();
        attached.dialect_schema = Some(Arc::new(()));
        let attached_database_id = 2;
        let mut database_schemas = FxHashMap::default();
        database_schemas.insert(attached_database_id, Arc::new(attached));
        let database_schemas = RwLock::new(database_schemas);
        let temp_database = RwLock::new(None);
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let qualifiers = Qualifiers::default();
        let syms = SymbolTable::new();
        let resolver = Resolver::new(
            &main,
            &database_schemas,
            &temp_database,
            &attached_databases,
            &qualifiers,
            &syms,
            true,
            DoubleQuotedDml::Enabled,
            Arc::new(RowSecurityInSchemasWithState),
            None,
            alice,
        );

        let policy = policy(
            &resolver,
            NamespaceRef::root_of(attached_database_id),
            "t",
            Command::Select,
        );

        assert_eq!(policy, Policy::Deny);
    }

    #[test]
    fn enabling_row_security_fails_under_a_dialect_without_it() {
        let db = Database::open_file(
            Arc::new(MemoryIO::new()),
            ":memory:",
            Arc::new(EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
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
}
