use crate::schema::{BTreeTable, Table};
use crate::translate::{
    expr::{translate_plan_expr_no_constant_opt, NoConstantOptReason},
    plan::{JoinedTable, PlanRuntimeBindings, RuntimeRowBinding, RuntimeValueBinding},
    plan_expr::{
        lower_hir_expr, plan_expr_dependencies, PlanColumnUse, PlanExpr, PlanIdentityMap,
        PlanSourceId,
    },
    semantic::schema_expr::analyze_schema_exprs,
};
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::DmlColumnContext;
use crate::{Arc, LimboError, Result};

use super::{ProgramBuilder, Resolver};

/// Compute virtual columns from the expressions planned for this exact target source.
#[turso_macros::trace_stack]
pub(crate) fn compute_planned_virtual_columns(
    program: &mut ProgramBuilder,
    target: &JoinedTable,
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
) -> Result<()> {
    let table = target.table.btree().ok_or_else(|| {
        LimboError::InternalError("generated-column target is not a B-tree table".to_string())
    })?;
    emit_virtual_columns(
        program,
        &table,
        target.internal_id,
        &target.read_programs.generated_expressions,
        dml_ctx,
        resolver,
    )
}

/// Compute virtual columns for a standalone schema path that has no semantic HIR source.
#[turso_macros::trace_stack]
pub(crate) fn compute_virtual_columns_from_schema(
    program: &mut ProgramBuilder,
    table: &Arc<BTreeTable>,
    database_id: usize,
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
) -> Result<()> {
    let columns = table.columns_topo_sort()?;
    let generated: Vec<_> = columns
        .iter()
        .filter_map(|(column_index, column)| {
            column
                .generated_expr()
                .map(|expression| (column_index, column, expression))
        })
        .collect();
    if generated.is_empty() {
        return Ok(());
    }
    let expressions = generated
        .iter()
        .map(|(_, _, expression)| expression.as_valid())
        .collect::<Result<Vec<_>>>()?;
    let source = program.next_plan_source_id();
    let context = resolver.semantic_context();
    let analyzed = analyze_schema_exprs(
        &context,
        database_id,
        Arc::new(Table::BTree(Arc::clone(table))),
        &expressions,
    )?;
    let mut identities = PlanIdentityMap::new();
    identities.bind_source_definition(&analyzed.source, source);
    let mut generated_expressions = vec![None; table.columns().len()];
    for ((column_index, _, _), expression) in generated.iter().zip(&analyzed.expressions) {
        generated_expressions[*column_index] =
            Some(lower_hir_expr(expression, &identities).map_err(|error| {
                LimboError::InternalError(format!(
                    "failed to lower generated column expression: {error}"
                ))
            })?);
    }

    emit_virtual_columns(
        program,
        table,
        source,
        &generated_expressions,
        dml_ctx,
        resolver,
    )
}

fn emit_virtual_columns(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    source: PlanSourceId,
    generated_expressions: &[Option<PlanExpr>],
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
) -> Result<()> {
    if generated_expressions.len() != table.columns().len() {
        return Err(LimboError::InternalError(format!(
            "planned generated-column count {} does not match table column count {}",
            generated_expressions.len(),
            table.columns().len()
        )));
    }

    let generated = planned_generated_column_order(table, source, generated_expressions)?;
    if generated.is_empty() {
        return Ok(());
    }

    let bindings = generated_column_bindings(source, dml_ctx, table);

    resolver.with_plan_runtime_bindings(bindings, |resolver| {
        for column_index in generated {
            let column = &table.columns()[column_index];
            let expression = generated_expressions[column_index]
                .as_ref()
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "virtual generated column {}.{} has no planned expression",
                        table.name, column_index
                    ))
                })?;
            let target_reg = dml_ctx.to_column_reg(column_index);
            translate_plan_expr_no_constant_opt(
                program,
                None,
                expression,
                target_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            if column.affinity() != Affinity::Blob {
                program.emit_column_affinity(target_reg, column.affinity());
            }
        }
        Ok(())
    })
}

/// Order generated columns from the programs owned by this plan source.
///
/// A catalog expression can remain unresolved after a lenient schema reload.
/// Once semantic analysis has produced these `PlanExpr`s, reopening that
/// catalog expression would discard the resolved custom-type identities the
/// plan owns and can replay a stale resolution error.
fn planned_generated_column_order(
    table: &BTreeTable,
    source: PlanSourceId,
    generated_expressions: &[Option<PlanExpr>],
) -> Result<Vec<usize>> {
    let column_count = table.columns().len();
    let mut dependents = vec![Vec::new(); column_count];
    let mut in_degree = vec![0usize; column_count];

    for (column_index, column) in table.columns().iter().enumerate() {
        if !column.is_virtual_generated() {
            continue;
        }
        let expression = generated_expressions[column_index]
            .as_ref()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "virtual generated column {}.{} has no planned expression",
                    table.name, column_index
                ))
            })?;
        for (dependency_source, dependency) in plan_expr_dependencies(expression)?.source_uses {
            if dependency_source != source {
                continue;
            }
            let PlanColumnUse::Column(dependency_column) = dependency else {
                continue;
            };
            if dependency_column >= column_count {
                return Err(LimboError::InternalError(format!(
                    "generated expression {}.{} references missing column {}",
                    table.name, column_index, dependency_column
                )));
            }
            dependents[dependency_column].push(column_index);
            in_degree[column_index] += 1;
        }
    }

    let mut ready = (0..column_count)
        .filter(|column| in_degree[*column] == 0)
        .collect::<Vec<_>>();
    let mut order = Vec::with_capacity(column_count);
    while let Some(column) = ready.pop() {
        order.push(column);
        for dependent in &dependents[column] {
            in_degree[*dependent] -= 1;
            if in_degree[*dependent] == 0 {
                ready.push(*dependent);
            }
        }
    }

    if order.len() != column_count {
        let columns = (0..column_count)
            .filter(|column| in_degree[*column] > 0)
            .filter_map(|column| table.columns()[column].name.as_deref())
            .collect::<Vec<_>>()
            .join(", ");
        crate::bail_parse_error!("circular dependency in generated columns: {columns}");
    }

    order.retain(|column| table.columns()[*column].is_virtual_generated());
    Ok(order)
}

fn generated_column_bindings(
    source: PlanSourceId,
    dml_ctx: &DmlColumnContext,
    table: &BTreeTable,
) -> PlanRuntimeBindings {
    let mut bindings = PlanRuntimeBindings::default();
    bindings.bind_row(
        source,
        RuntimeRowBinding {
            columns: (0..table.columns().len())
                .map(|column| RuntimeValueBinding::Register {
                    register: dml_ctx.to_column_reg(column),
                    needs_decode: false,
                })
                .collect(),
            rowid: None,
            read_programs: None,
        },
    );
    bindings
}
