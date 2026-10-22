//! The UPDATE of the schema table that ALTER TABLE generates.

use crate::sync::Arc;
use crate::translate::semantic::context::SemanticContext;
use crate::vdbe::builder::ProgramBuilder;
use turso_parser::ast;

/// Prepare the UPDATE of the schema table that ALTER TABLE generates, then
/// run `after` on the program. It may write the schema table, and change
/// data capture records `ddl_query`, the ALTER statement, as the
/// new `sql` value.
pub fn translate_update_for_schema_change(
    body: ast::Update,
    _resolver: &SemanticContext<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    ddl_query: &str,
    catalog: &SemanticContext<'_>,
    after: impl FnOnce(&mut ProgramBuilder),
) -> crate::Result<()> {
    let statement = ast::Stmt::Update(body);
    super::semantic_lowering::translate_schema_update(
        &statement, catalog, program, connection, ddl_query,
    )?;
    after(program);
    Ok(())
}
