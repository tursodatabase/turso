//! Row-level security limits which rows of a table a role may access, and
//! is turned on per table.
//!
//! Which tables have row-level security is the dialect's schema state, like
//! the owner of a table: [`crate::Dialect::table_rls_enabled`] says which
//! tables have it. The module compiles the statements that enable and
//! disable it into a [`RelationChange`] for the dialect to record. Nothing
//! limits rows yet.

use std::sync::Arc;

use turso_parser::ast;

use crate::{
    bail_parse_error,
    dialect::RelationChange,
    translate::{emitter::Resolver, schema::emit_schema_cookie},
    vdbe::builder::ProgramBuilder,
    Connection, Result, MAIN_DB_ID,
};

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

    use turso_parser::ast;

    use crate::dialect::EngineDialect;
    use crate::{Database, MemoryIO};

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
