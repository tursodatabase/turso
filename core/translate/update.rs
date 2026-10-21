//! The UPDATE of the schema table that ALTER TABLE generates.

use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::vdbe::builder::ProgramBuilder;
use turso_parser::ast;

/// Prepare the UPDATE of the schema table that ALTER TABLE generates, then
/// run `after` on the program. Like legacy, it may write the schema table,
/// and change data capture records `ddl_query`, the ALTER statement, as the
/// new `sql` value.
pub fn translate_update_for_schema_change(
    body: ast::Update,
    _resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    ddl_query: &str,
    hir: crate::translate::HirContext<'_>,
    after: impl FnOnce(&mut ProgramBuilder),
) -> crate::Result<()> {
    let (schema, settings) = hir;
    let statement = ast::Stmt::Update(body);
    super::semantic_lowering::translate_schema_update(
        &statement, schema, settings, program, connection, ddl_query,
    )?;
    after(program);
    Ok(())
}
