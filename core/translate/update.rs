//! Internal schema UPDATE orchestration.
//!
//! User and nested DML roots enter through `translate_semantic_root`. ALTER
//! TABLE needs one extra hook after its generated sqlite_schema UPDATE, but it
//! otherwise uses the exact same semantic and physical layers.

use crate::{
    sync::Arc,
    translate::{
        emitter::Resolver,
        physical::{emit_root_update_with_context_and_after, PhysicalPlan, RootRuntimeInputs},
        semantic::{analyze, context::DmlPolicy, context::SemanticContext, AnalyzeInput},
    },
    vdbe::builder::ProgramBuilder,
    Connection, Result,
};
use turso_parser::ast;

pub fn translate_update_for_schema_change(
    body: ast::Update,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    ddl_query: &str,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    let statement = ast::Stmt::Update(body);
    let context = SemanticContext::new(
        resolver.schema(),
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        resolver.symbol_table,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        connection.dialect(),
    );
    let context = context.with_dml_policy(DmlPolicy::new(
        connection.is_nested_stmt(),
        connection.is_mvcc_bootstrap_connection(),
        true,
        connection.check_constraints_ignored(),
        connection.foreign_keys_enabled(),
    ));
    let context = context.with_capture_data_changes(program.capture_data_changes_info().clone());
    let context = context.with_internal_schema_change_sql(ddl_query);
    let document = analyze(&context, AnalyzeInput::Statement(&statement))?;

    super::set_semantic_conflict_policy(program, &document);
    super::set_semantic_statement_journal_flags(program, &document)?;
    super::set_semantic_transactions(program, &document, true)?;
    let triggers =
        super::semantic_prepare::prepare_triggers(&context, &document, program, connection)?;
    let plan = PhysicalPlan::new(&document)
        .map_err(|error| crate::LimboError::InternalError(error.to_string()))?;
    emit_root_update_with_context_and_after(
        &plan,
        program,
        &RootRuntimeInputs::default(),
        &triggers,
        after,
    )
    .map_err(|error| crate::LimboError::InternalError(error.to_string()))
}
