use super::emitter::{emit_program, TranslateCtx};
use super::plan::{InSeekSource, Operation, Search};
use crate::schema::Table;
use crate::sync::Arc;
use crate::translate::emitter::{OperationMode, Resolver};
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{Plan, SelectPlan, SubqueryState};
use crate::translate::result_row::emit_select_result;
use crate::vdbe::builder::ProgramBuilderOpts;
use crate::vdbe::insn::Insn;
use crate::{vdbe::builder::ProgramBuilder, Result};

/// Optimize and emit bytecode for an already-prepared select plan.
#[turso_macros::trace_stack]
pub fn emit_select_plan(
    mut plan: Plan,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<usize> {
    optimize_plan(program, &mut plan, resolver)?;
    let num_result_cols;
    let opts = match &plan {
        Plan::Select(select) => {
            num_result_cols = select.result_columns.len();
            ProgramBuilderOpts {
                num_cursors: count_required_cursors_for_simple_select(select),
                approx_num_insns: estimate_num_instructions_for_simple_select(select),
                approx_num_labels: estimate_num_labels_for_simple_select(select),
            }
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            // Compound Selects must return the same number of columns
            num_result_cols = right_most.result_columns.len();

            ProgramBuilderOpts {
                num_cursors: count_required_cursors_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| count_required_cursors_for_simple_select(plan))
                        .sum::<usize>(),
                approx_num_insns: estimate_num_instructions_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| estimate_num_instructions_for_simple_select(plan))
                        .sum::<usize>(),
                approx_num_labels: estimate_num_labels_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| estimate_num_labels_for_simple_select(plan))
                        .sum::<usize>(),
            }
        }
        Plan::RecursiveCte(recursive_cte) => {
            num_result_cols = recursive_cte.result_columns.len();
            ProgramBuilderOpts {
                num_cursors: count_required_cursors_for_plan(&plan),
                approx_num_insns: estimate_num_instructions_for_plan(&plan),
                approx_num_labels: estimate_num_labels_for_plan(&plan),
            }
        }
        _ => crate::bail_parse_error!("emit_select_plan called with non-SELECT plan"),
    };

    program.extend(&opts);
    emit_program(connection, resolver, program, plan, |_| {})?;
    Ok(num_result_cols)
}

pub(crate) fn plan_first_virtual_table_name(plan: &Plan) -> Option<String> {
    match plan {
        Plan::Select(select_plan) => select_plan_first_virtual_table_name(select_plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => select_plan_first_virtual_table_name(right_most).or_else(|| {
            left.iter()
                .find_map(|(plan, _)| select_plan_first_virtual_table_name(plan))
        }),
        Plan::RecursiveCte(recursive_cte) => {
            plan_first_virtual_table_name(&recursive_cte.initial_query)
                .or_else(|| plan_first_virtual_table_name(&recursive_cte.recursive_query))
        }
        Plan::Delete(_) | Plan::Update(_) => None,
    }
}

fn select_plan_first_virtual_table_name(select_plan: &SelectPlan) -> Option<String> {
    for joined_table in select_plan.joined_tables() {
        match &joined_table.table {
            Table::Virtual(virtual_table) if !virtual_table.innocuous => {
                return Some(virtual_table.name.clone())
            }
            Table::FromClauseSubquery(from_clause_subquery) => {
                if let Some(name) = plan_first_virtual_table_name(&from_clause_subquery.plan) {
                    return Some(name);
                }
            }
            _ => {}
        }
    }
    for subquery in &select_plan.non_from_clause_subqueries {
        if let SubqueryState::Unevaluated { plan: Some(plan) } = &subquery.state {
            if let Plan::Select(plan) = plan.as_ref() {
                if let Some(name) = select_plan_first_virtual_table_name(plan) {
                    return Some(name);
                }
            }
        }
    }
    None
}

/// Counts cursors needed to emit a query plan.
fn count_required_cursors_for_plan(plan: &Plan) -> usize {
    fold_query_plan(plan.into(), 0, 2, 0, count_required_cursors_for_one_select)
}

fn count_required_cursors_for_simple_select(plan: &SelectPlan) -> usize {
    fold_query_plan(plan.into(), 0, 2, 0, count_required_cursors_for_one_select)
}

fn count_required_cursors_for_one_select(plan: &SelectPlan) -> usize {
    let num_table_cursors: usize = plan
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 1,
            Operation::Search(search) => match search {
                Search::RowidEq { .. } => 1,
                Search::Seek { index, .. } => 1 + index.is_some() as usize,
                Search::InSeek { index, source } => match source {
                    // table cursor + new ephemeral cursor + optional index cursor
                    InSeekSource::LiteralList { .. } => 2 + index.is_some() as usize,
                    // table cursor + optional index cursor (ephemeral already counted)
                    InSeekSource::Subquery { .. } => 1 + index.is_some() as usize,
                },
            },
            Operation::IndexMethodQuery(_) => 1,
            Operation::HashJoin(_) => 2,
            // One table cursor + one cursor per index branch
            Operation::MultiIndexScan(multi_idx) => 1 + multi_idx.branches.len(),
        })
        .sum();
    let has_group_by_with_exprs = plan
        .group_by
        .as_ref()
        .is_some_and(|gb| !gb.exprs.is_empty());
    let num_sorter_cursors = has_group_by_with_exprs as usize + !plan.order_by.is_empty() as usize;
    let num_pseudo_cursors = has_group_by_with_exprs as usize + !plan.order_by.is_empty() as usize;

    num_table_cursors + num_sorter_cursors + num_pseudo_cursors
}

/// Estimates bytecode instructions needed to emit a query plan.
fn estimate_num_instructions_for_plan(plan: &Plan) -> usize {
    fold_query_plan(
        plan.into(),
        20,
        32,
        10,
        estimate_num_instructions_for_one_select,
    )
}

fn estimate_num_instructions_for_simple_select(select: &SelectPlan) -> usize {
    fold_query_plan(
        select.into(),
        20,
        32,
        10,
        estimate_num_instructions_for_one_select,
    )
}

fn estimate_num_instructions_for_one_select(select: &SelectPlan) -> usize {
    let table_instructions: usize = select
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 10,
            Operation::Search(_) => 15,
            Operation::IndexMethodQuery(_) => 15,
            Operation::HashJoin(_) => 20,
            // Multi-index scan: scan overhead per branch + deduplication + final rowid fetch
            Operation::MultiIndexScan(multi_idx) => 15 * multi_idx.branches.len() + 10,
        })
        .sum();

    let group_by_instructions = select.group_by.is_some() as usize * 10;
    let order_by_instructions = !select.order_by.is_empty() as usize * 10;
    let condition_instructions = select.where_clause.len() * 3;

    20 + table_instructions + group_by_instructions + order_by_instructions + condition_instructions
}

/// Estimates jump labels needed to emit a query plan.
fn estimate_num_labels_for_plan(plan: &Plan) -> usize {
    fold_query_plan(plan.into(), 10, 4, 3, estimate_num_labels_for_one_select)
}

fn estimate_num_labels_for_simple_select(select: &SelectPlan) -> usize {
    fold_query_plan(select.into(), 10, 4, 3, estimate_num_labels_for_one_select)
}

fn estimate_num_labels_for_one_select(select: &SelectPlan) -> usize {
    let init_halt_labels = 2;
    // 3 loop labels for each table in main loop + 1 to signify end of main loop
    let table_labels = select
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 3,
            Operation::Search(_) => 3,
            Operation::IndexMethodQuery(_) => 3,
            Operation::HashJoin(_) => 3,
            // Multi-index scan needs extra labels for each branch + rowset loop
            Operation::MultiIndexScan(multi_idx) => 3 + multi_idx.branches.len() * 2,
        })
        .sum::<usize>()
        + 1;

    let group_by_labels = select.group_by.is_some() as usize * 10;
    let order_by_labels = !select.order_by.is_empty() as usize * 10;
    let condition_labels = select.where_clause.len() * 2;

    init_halt_labels + table_labels + group_by_labels + order_by_labels + condition_labels
}

enum QueryPlanRef<'a> {
    Plan(&'a Plan),
    Select(&'a SelectPlan),
}

impl<'a> From<&'a Plan> for QueryPlanRef<'a> {
    fn from(plan: &'a Plan) -> Self {
        Self::Plan(plan)
    }
}

impl<'a> From<&'a SelectPlan> for QueryPlanRef<'a> {
    fn from(plan: &'a SelectPlan) -> Self {
        Self::Select(plan)
    }
}

/// Fold a query plan without retaining one Rust call frame per nested derived
/// table or CTE. Deep CTE chains are valid SQL, so sizing the bytecode builder
/// must use heap-backed traversal state.
fn fold_query_plan<'a>(
    root: QueryPlanRef<'a>,
    compound_overhead: usize,
    recursive_cte_overhead: usize,
    nested_select_overhead: usize,
    mut select_value: impl FnMut(&SelectPlan) -> usize,
) -> usize {
    let mut total = 0;
    let mut pending = vec![root];
    while let Some(node) = pending.pop() {
        let select = match node {
            QueryPlanRef::Plan(plan) => match plan {
                Plan::Select(select) => select.as_ref(),
                Plan::CompoundSelect {
                    left, right_most, ..
                } => {
                    total += compound_overhead;
                    pending.extend(left.iter().map(|(select, _)| QueryPlanRef::Select(select)));
                    pending.push(QueryPlanRef::Select(right_most));
                    continue;
                }
                Plan::RecursiveCte(recursive_cte) => {
                    total += recursive_cte_overhead;
                    pending.push(QueryPlanRef::Plan(&recursive_cte.initial_query));
                    pending.push(QueryPlanRef::Plan(&recursive_cte.recursive_query));
                    continue;
                }
                Plan::Delete(_) | Plan::Update(_) => continue,
            },
            QueryPlanRef::Select(select) => select,
        };

        total += select_value(select);
        for table in select.joined_tables() {
            if let Table::FromClauseSubquery(from_clause_subquery) = &table.table {
                total += nested_select_overhead;
                pending.push(QueryPlanRef::Plan(&from_clause_subquery.plan));
            }
        }
    }
    total
}

pub fn emit_simple_count(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<bool> {
    let cursors = plan
        .joined_tables()
        .first()
        .unwrap()
        .resolve_cursors(program, OperationMode::SELECT)?;

    let cursor_id = {
        match cursors {
            (_, Some(cursor_id)) | (Some(cursor_id), None) => cursor_id,
            _ => return Ok(false),
        }
    };

    // Count opcode only works on BTree cursors. Materialized view trigger
    // queries may have pseudo cursors — fall back to normal aggregation.
    if !program.cursor_is_btree(cursor_id) {
        return Ok(false);
    }

    let target_reg = program.alloc_register();

    program.emit_insn(Insn::Count {
        cursor_id,
        target_reg,
        exact: true,
    });

    program.emit_insn(Insn::Close { cursor_id });

    let agg = plan
        .aggregates
        .first()
        .expect("simple count requires exactly one aggregate");
    t_ctx
        .resolver
        .cache_plan_expr_reg(agg.original_expr.clone(), target_reg, false, None);
    t_ctx.resolver.enable_expr_to_reg_cache();

    emit_select_result(
        program,
        &t_ctx.resolver,
        plan,
        None,
        None,
        None,
        None,
        t_ctx.reg_result_cols_start.unwrap(),
        t_ctx.limit_ctx,
    )?;
    Ok(true)
}
