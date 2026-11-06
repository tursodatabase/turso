//! Translation of VACUUM statements to VDBE bytecode.

use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    bind_and_rewrite_expr, expr_vector_size, translate_expr, BindingBehavior,
};
use crate::translate::plan::{EvalAt, TableReferences};
use crate::translate::subquery::{
    emit_non_from_clause_subquery, plan_subqueries_in_expr_without_from_clause,
};
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{bail_parse_error, Connection, Result, TEMP_DB_ID};
use crate::{sync::Arc, LimboError};
use turso_parser::ast::{Expr, Name, SubqueryType};

/// Translate a VACUUM statement into VDBE bytecode.
///
/// We have `VACUUM INTO`. The in-place `VACUUM` is experimental and gated
/// behind the experimental vacuum feature flag.
///
/// # Arguments
/// * `program` - The program builder to emit instructions to
/// * `resolver` - Resolves names in the VACUUM INTO path expression
/// * `schema_name` - Optional schema/database name to vacuum (defaults to "main" if None)
/// * `into` - Optional destination path for VACUUM INTO
///
/// # Returns
/// The modified program builder on success
pub fn translate_vacuum(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    schema_name: Option<&Name>,
    into: Option<&Expr>,
    connection: Arc<Connection>,
) -> Result<()> {
    let schema_name = schema_name.map_or_else(|| "main".to_string(), |n| n.as_str().to_string());
    match into {
        Some(dest_expr) => {
            if connection.get_database_id_by_name(&schema_name)? == TEMP_DB_ID {
                return Ok(());
            }
            let mut dest_expr = dest_expr.clone();
            bind_and_rewrite_expr(
                &mut dest_expr,
                Some(&mut TableReferences::new(vec![], vec![])),
                None,
                resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
            let mut subqueries = Vec::new();
            plan_subqueries_in_expr_without_from_clause(
                program,
                &mut subqueries,
                &mut dest_expr,
                resolver,
                &connection,
            )?;
            match &dest_expr {
                Expr::SubqueryResult {
                    query_type: SubqueryType::RowValue { num_regs, .. },
                    ..
                } if *num_regs != 1 => {
                    bail_parse_error!("sub-select returns {num_regs} columns - expected 1");
                }
                _ if expr_vector_size(&dest_expr)? != 1 => {
                    bail_parse_error!("row value misused");
                }
                _ => {}
            }
            for subquery in &mut subqueries {
                let plan = subquery.consume_plan(EvalAt::BeforeLoop);
                emit_non_from_clause_subquery(
                    program,
                    resolver,
                    *plan,
                    &subquery.query_type,
                    false,
                )?;
            }
            let dest_reg = program.alloc_register();
            translate_expr(program, None, &dest_expr, dest_reg, resolver)?;
            program.emit_insn(Insn::VacuumInto {
                schema_name,
                dest_reg,
            });
            Ok(())
        }
        None => {
            if !connection.experimental_vacuum_enabled() {
                return Err(LimboError::ParseError(
                    "VACUUM is an experimental feature. Enable with --experimental-vacuum flag"
                        .to_string(),
                ));
            }
            if connection.experimental_multiprocess_wal_enabled() {
                return Err(LimboError::ParseError(
                    "VACUUM is incompatible with experimental multiprocess WAL".to_string(),
                ));
            }

            // Schema-qualified VACUUM is not supported yet.
            if schema_name != "main" {
                bail_parse_error!(
                    "VACUUM is only supported for the main database; schema '{}' is not supported yet",
                    schema_name
                );
            }
            program.emit_insn(Insn::Vacuum {
                db: crate::MAIN_DB_ID,
            });
            Ok(())
        }
    }
}
