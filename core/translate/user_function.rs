//! `CREATE FUNCTION` and `DROP FUNCTION` of functions that users define. The
//! engine does not store such functions: the dialect's catalog does, and a
//! catalog that stores none refuses the statements.

use std::sync::Arc;

use crate::ast;
use crate::{
    dialect::{CatalogCmd, CmdContext, CreateFunction, DropFunction},
    translate::{emitter::Resolver, schema::emit_schema_cookie},
    vdbe::builder::ProgramBuilder,
    Connection, Result, MAIN_DB_ID,
};

/// `CREATE [OR REPLACE] FUNCTION`.
pub fn translate_create_function(
    or_replace: bool,
    name: &ast::QualifiedName,
    argument_count: usize,
    sql: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let cmd = CatalogCmd::CreateFunction(CreateFunction {
        or_replace,
        name,
        argument_count,
        sql,
    });
    translate_function_cmd(&cmd, resolver, program, connection)
}

/// `DROP FUNCTION`.
pub fn translate_drop_function(
    if_exists: bool,
    functions: &[ast::FunctionSignature],
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let cmd = CatalogCmd::DropFunction(DropFunction {
        if_exists,
        functions,
    });
    translate_function_cmd(&cmd, resolver, program, connection)
}

/// Functions are objects of the main database, which the dialect's catalog
/// stores in the statement's transaction.
fn translate_function_cmd(
    cmd: &CatalogCmd<'_>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let schema = resolver.schema();
    program.begin_write_on_database(MAIN_DB_ID, schema.schema_version)?;
    let cmd_context = CmdContext::new(connection, resolver, MAIN_DB_ID);
    resolver
        .catalog
        .dialect
        .catalog()
        .emit(program, &cmd_context, cmd)?;
    emit_schema_cookie(resolver, program);
    Ok(())
}
