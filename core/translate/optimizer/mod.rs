use std::{cell::RefCell, cmp::Ordering, collections::HashMap, sync::Arc};

use constraints::{
    constraints_from_where_clause, usable_constraints_for_join_order, Constraint, ConstraintRef,
};
use cost::Cost;
use join::{compute_best_join_order, BestJoinOrderResult};
use lift_common_subexpressions::lift_common_subexpressions_from_binary_or_terms;
use order::{compute_order_target, plan_satisfies_order_target, EliminatesSortBy};
use turso_sqlite3_parser::{
    ast::{self, Expr, SortOrder},
    to_sql_string::ToSqlString as _,
};

use crate::{
    parameters::PARAM_PREFIX,
    schema::{Index, IndexColumn, Schema, Table},
    translate::{expr::walk_expr_mut, plan::TerminationKey},
    types::SeekOp,
    Result,
};

use super::{
    emitter::Resolver,
    plan::{
        DeletePlan, GroupBy, IterationDirection, JoinOrderMember, JoinedTable, Operation, Plan,
        Search, SeekDef, SeekKey, SelectPlan, TableReferences, UpdatePlan, WhereTerm,
    },
};

pub(crate) mod access_method;
pub(crate) mod constraints;
pub(crate) mod cost;
pub(crate) mod join;
pub(crate) mod lift_common_subexpressions;
pub(crate) mod order;

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
pub fn optimize_plan(plan: &mut Plan, schema: &Schema) -> Result<()> {
    match plan {
        Plan::Select(plan) => optimize_select_plan(plan, schema)?,
        Plan::Delete(plan) => optimize_delete_plan(plan, schema)?,
        Plan::Update(plan) => optimize_update_plan(plan, schema)?,
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            optimize_select_plan(right_most, schema)?;
            for (plan, _) in left {
                optimize_select_plan(plan, schema)?;
            }
        }
    }
    // When debug tracing is enabled, print the optimized plan as a SQL string for debugging
    tracing::debug!(plan_sql = plan.to_sql_string(&crate::translate::display::PlanContext(&[])));
    Ok(())
}

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
pub fn optimize_select_plan(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    optimize_subqueries(plan, schema)?;
    rewrite_exprs_select(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    let best_join_order = optimize_table_access(
        schema,
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut plan.group_by,
    )?;

    if let Some(best_join_order) = best_join_order {
        plan.join_order = best_join_order;
    }

    Ok(())
}

fn optimize_delete_plan(plan: &mut DeletePlan, _schema: &Schema) -> Result<()> {
    rewrite_exprs_delete(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    // FIXME: don't use indexes for delete right now because it's buggy. See for example:
    // https://github.com/tursodatabase/turso/issues/1714
    // let _ = optimize_table_access(
    //     &mut plan.table_references,
    //     &schema.indexes,
    //     &mut plan.where_clause,
    //     &mut plan.order_by,
    //     &mut None,
    // )?;

    Ok(())
}

fn optimize_update_plan(plan: &mut UpdatePlan, schema: &Schema) -> Result<()> {
    rewrite_exprs_update(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }
    let _ = optimize_table_access(
        schema,
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut None,
    )?;
    Ok(())
}

fn optimize_subqueries(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    for table in plan.table_references.joined_tables_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table.table {
            optimize_select_plan(&mut from_clause_subquery.plan, schema)?;
        }
    }

    Ok(())
}

/// Optimize the join order and index selection for a query.
///
/// This function does the following:
/// - Computes a set of [Constraint]s for each table.
/// - Using those constraints, computes the best join order for the list of [TableReference]s
///   and selects the best [crate::translate::optimizer::access_method::AccessMethod] for each table in the join order.
/// - Mutates the [Operation]s in `joined_tables` to use the selected access methods.
/// - Removes predicates from the `where_clause` that are now redundant due to the selected access methods.
/// - Removes sorting operations if the selected join order and access methods satisfy the [crate::translate::optimizer::order::OrderTarget].
///
/// Returns the join order if it was optimized, or None if the default join order was considered best.
fn optimize_table_access(
    schema: &Schema,
    table_references: &mut TableReferences,
    available_indexes: &HashMap<String, Vec<Arc<Index>>>,
    where_clause: &mut [WhereTerm],
    order_by: &mut Option<Vec<(ast::Expr, SortOrder)>>,
    group_by: &mut Option<GroupBy>,
) -> Result<Option<Vec<JoinOrderMember>>> {
    let access_methods_arena = RefCell::new(Vec::new());
    let maybe_order_target = compute_order_target(order_by, group_by.as_mut());
    let constraints_per_table =
        constraints_from_where_clause(where_clause, table_references, available_indexes)?;
    let Some(best_join_order_result) = compute_best_join_order(
        table_references.joined_tables_mut(),
        maybe_order_target.as_ref(),
        &constraints_per_table,
        &access_methods_arena,
    )?
    else {
        return Ok(None);
    };

    let BestJoinOrderResult {
        best_plan,
        best_ordered_plan,
    } = best_join_order_result;

    let joined_tables = table_references.joined_tables_mut();

    // See if best_ordered_plan is better than the overall best_plan if we add a sorting penalty
    // to the unordered plan's cost.
    let best_plan = if let Some(best_ordered_plan) = best_ordered_plan {
        let best_unordered_plan_cost = best_plan.cost;
        let best_ordered_plan_cost = best_ordered_plan.cost;
        const SORT_COST_PER_ROW_MULTIPLIER: f64 = 0.001;
        let sorting_penalty =
            Cost(best_plan.output_cardinality as f64 * SORT_COST_PER_ROW_MULTIPLIER);
        if best_unordered_plan_cost + sorting_penalty > best_ordered_plan_cost {
            best_ordered_plan
        } else {
            best_plan
        }
    } else {
        best_plan
    };

    // Eliminate sorting if possible.
    if let Some(order_target) = maybe_order_target {
        let satisfies_order_target = plan_satisfies_order_target(
            &best_plan,
            &access_methods_arena,
            joined_tables,
            &order_target,
        );
        if satisfies_order_target {
            match order_target.1 {
                EliminatesSortBy::Group => {
                    let _ = group_by.as_mut().and_then(|g| g.sort_order.take());
                }
                EliminatesSortBy::Order => {
                    let _ = order_by.take();
                }
                EliminatesSortBy::GroupByAndOrder => {
                    let _ = group_by.as_mut().and_then(|g| g.sort_order.take());
                    let _ = order_by.take();
                }
            }
        }
    }

    let (best_access_methods, best_table_numbers) = (
        best_plan.best_access_methods().collect::<Vec<_>>(),
        best_plan.table_numbers().collect::<Vec<_>>(),
    );

    let best_join_order: Vec<JoinOrderMember> = best_table_numbers
        .into_iter()
        .map(|table_number| JoinOrderMember {
            table_id: joined_tables[table_number].internal_id,
            original_idx: table_number,
            is_outer: joined_tables[table_number]
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.outer),
        })
        .collect();

    // Mutate the Operations in `joined_tables` to use the selected access methods.
    for (i, join_order_member) in best_join_order.iter().enumerate() {
        let table_idx = join_order_member.original_idx;
        let access_method = &access_methods_arena.borrow()[best_access_methods[i]];
        if access_method.is_scan() {
            let try_to_build_ephemeral_index = if schema.indexes_enabled() {
                let is_leftmost_table = i == 0;
                let uses_index = access_method.index.is_some();
                let source_table_is_from_clause_subquery = matches!(
                    &joined_tables[table_idx].table,
                    Table::FromClauseSubquery(_)
                );
                !is_leftmost_table && !uses_index && !source_table_is_from_clause_subquery
            } else {
                false
            };

            if !try_to_build_ephemeral_index {
                joined_tables[table_idx].op = Operation::Scan {
                    iter_dir: access_method.iter_dir,
                    index: access_method.index.clone(),
                };
                continue;
            }
            // This branch means we have a full table scan for a non-outermost table.
            // Try to construct an ephemeral index since it's going to be better than a scan.
            let table_constraints = constraints_per_table
                .iter()
                .find(|c| c.table_id == join_order_member.table_id);
            let Some(table_constraints) = table_constraints else {
                joined_tables[table_idx].op = Operation::Scan {
                    iter_dir: access_method.iter_dir,
                    index: access_method.index.clone(),
                };
                continue;
            };
            let temp_constraint_refs = (0..table_constraints.constraints.len())
                .map(|i| ConstraintRef {
                    constraint_vec_pos: i,
                    index_col_pos: table_constraints.constraints[i].table_col_pos,
                    sort_order: SortOrder::Asc,
                })
                .collect::<Vec<_>>();
            let usable_constraint_refs = usable_constraints_for_join_order(
                &table_constraints.constraints,
                &temp_constraint_refs,
                &best_join_order[..=i],
            );
            if usable_constraint_refs.is_empty() {
                joined_tables[table_idx].op = Operation::Scan {
                    iter_dir: access_method.iter_dir,
                    index: access_method.index.clone(),
                };
                continue;
            }
            let ephemeral_index = ephemeral_index_build(
                &joined_tables[table_idx],
                &table_constraints.constraints,
                usable_constraint_refs,
            );
            let ephemeral_index = Arc::new(ephemeral_index);
            joined_tables[table_idx].op = Operation::Search(Search::Seek {
                index: Some(ephemeral_index),
                seek_def: build_seek_def_from_constraints(
                    &table_constraints.constraints,
                    usable_constraint_refs,
                    access_method.iter_dir,
                    where_clause,
                )?,
            });
        } else {
            let constraint_refs = access_method.constraint_refs;
            assert!(!constraint_refs.is_empty());
            for cref in constraint_refs.iter() {
                let constraint =
                    &constraints_per_table[table_idx].constraints[cref.constraint_vec_pos];
                assert!(
                    !where_clause[constraint.where_clause_pos.0].consumed.get(),
                    "trying to consume a where clause term twice: {:?}",
                    where_clause[constraint.where_clause_pos.0]
                );
                where_clause[constraint.where_clause_pos.0]
                    .consumed
                    .set(true);
            }
            if let Some(index) = &access_method.index {
                joined_tables[table_idx].op = Operation::Search(Search::Seek {
                    index: Some(index.clone()),
                    seek_def: build_seek_def_from_constraints(
                        &constraints_per_table[table_idx].constraints,
                        constraint_refs,
                        access_method.iter_dir,
                        where_clause,
                    )?,
                });
                continue;
            }
            assert!(
                constraint_refs.len() == 1,
                "expected exactly one constraint for rowid seek, got {constraint_refs:?}"
            );
            let constraint = &constraints_per_table[table_idx].constraints
                [constraint_refs[0].constraint_vec_pos];
            joined_tables[table_idx].op = match constraint.operator {
                ast::Operator::Equals => Operation::Search(Search::RowidEq {
                    cmp_expr: constraint.get_constraining_expr(where_clause),
                }),
                _ => Operation::Search(Search::Seek {
                    index: None,
                    seek_def: build_seek_def_from_constraints(
                        &constraints_per_table[table_idx].constraints,
                        constraint_refs,
                        access_method.iter_dir,
                        where_clause,
                    )?,
                }),
            };
        }
    }

    Ok(Some(best_join_order))
}

#[derive(Debug, PartialEq, Clone)]
enum ConstantConditionEliminationResult {
    Continue,
    ImpossibleCondition,
}

/// Removes predicates that are always true.
/// Returns a ConstantEliminationResult indicating whether any predicates are always false.
/// This is used to determine whether the query can be aborted early.
fn eliminate_constant_conditions(
    where_clause: &mut [WhereTerm],
) -> Result<ConstantConditionEliminationResult> {
    let mut i = 0;
    while i < where_clause.len() {
        let predicate = &where_clause[i];
        if predicate.expr.is_always_true()? {
            // true predicates can be removed since they don't affect the result
            where_clause[i].consumed.set(true);
            i += 1;
        } else if predicate.expr.is_always_false()? {
            // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false,
            // except an outer join condition, because that just results in NULLs, not skipping the whole loop
            if predicate.from_outer_join.is_some() {
                i += 1;
                continue;
            }
            where_clause
                .iter_mut()
                .for_each(|term| term.consumed.set(true));
            return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
        } else {
            i += 1;
        }
    }

    Ok(ConstantConditionEliminationResult::Continue)
}

fn rewrite_exprs_select(plan: &mut SelectPlan) -> Result<()> {
    let mut param_count = 1;
    for rc in plan.result_columns.iter_mut() {
        rewrite_expr(&mut rc.expr, &mut param_count)?;
    }
    for agg in plan.aggregates.iter_mut() {
        rewrite_expr(&mut agg.original_expr, &mut param_count)?;
    }
    lift_common_subexpressions_from_binary_or_terms(&mut plan.where_clause)?;
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr, &mut param_count)?;
    }
    if let Some(group_by) = &mut plan.group_by {
        for expr in group_by.exprs.iter_mut() {
            rewrite_expr(expr, &mut param_count)?;
        }
    }
    if let Some(order_by) = &mut plan.order_by {
        for (expr, _) in order_by.iter_mut() {
            rewrite_expr(expr, &mut param_count)?;
        }
    }

    Ok(())
}

fn rewrite_exprs_delete(plan: &mut DeletePlan) -> Result<()> {
    let mut param_idx = 1;
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr, &mut param_idx)?;
    }
    Ok(())
}

fn rewrite_exprs_update(plan: &mut UpdatePlan) -> Result<()> {
    let mut param_idx = 1;
    for (_, expr) in plan.set_clauses.iter_mut() {
        rewrite_expr(expr, &mut param_idx)?;
    }
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr, &mut param_idx)?;
    }
    if let Some(order_by) = &mut plan.order_by {
        for (expr, _) in order_by.iter_mut() {
            rewrite_expr(expr, &mut param_idx)?;
        }
    }
    if let Some(rc) = plan.returning.as_mut() {
        for rc in rc.iter_mut() {
            rewrite_expr(&mut rc.expr, &mut param_idx)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlwaysTrueOrFalse {
    AlwaysTrue,
    AlwaysFalse,
}

/**
  Helper trait for expressions that can be optimized
  Implemented for ast::Expr
*/
pub trait Optimizable {
    // if the expression is a constant expression that, when evaluated as a condition, is always true or false
    // return a [ConstantPredicate].
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self.check_always_true_or_false()? == Some(AlwaysTrueOrFalse::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self.check_always_true_or_false()? == Some(AlwaysTrueOrFalse::AlwaysFalse))
    }
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool;
    fn is_nonnull(&self, tables: &TableReferences) -> bool;
}

impl Optimizable for ast::Expr {
    /// Returns true if the expressions is (verifiably) non-NULL.
    /// It might still be non-NULL even if we return false; we just
    /// weren't able to prove it.
    /// This function is currently very conservative, and will return false
    /// for any expression where we aren't sure and didn't bother to find out
    /// by writing more complex code.
    fn is_nonnull(&self, tables: &TableReferences) -> bool {
        match self {
            Expr::Between {
                lhs, start, end, ..
            } => lhs.is_nonnull(tables) && start.is_nonnull(tables) && end.is_nonnull(tables),
            Expr::Binary(expr, _, expr1) => expr.is_nonnull(tables) && expr1.is_nonnull(tables),
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
                ..
            } => {
                base.as_ref().is_none_or(|base| base.is_nonnull(tables))
                    && when_then_pairs
                        .iter()
                        .all(|(_, then)| then.is_nonnull(tables))
                    && else_expr
                        .as_ref()
                        .is_none_or(|else_expr| else_expr.is_nonnull(tables))
            }
            Expr::Cast { expr, .. } => expr.is_nonnull(tables),
            Expr::Collate(expr, _) => expr.is_nonnull(tables),
            Expr::DoublyQualified(..) => {
                panic!("Do not call is_nonnull before DoublyQualified has been rewritten as Column")
            }
            Expr::Exists(..) => false,
            Expr::FunctionCall { .. } => false,
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(..) => panic!("Do not call is_nonnull before Id has been rewritten as Column"),
            Expr::Column {
                table,
                column,
                is_rowid_alias,
                ..
            } => {
                if *is_rowid_alias {
                    return true;
                }

                let table_ref = tables.find_joined_table_by_internal_id(*table).unwrap();
                let columns = table_ref.columns();
                let column = &columns[*column];
                column.primary_key || column.notnull
            }
            Expr::RowId { .. } => true,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_nonnull(tables)
                    && rhs
                        .as_ref()
                        .is_none_or(|rhs| rhs.iter().all(|rhs| rhs.is_nonnull(tables)))
            }
            Expr::InSelect { .. } => false,
            Expr::InTable { .. } => false,
            Expr::IsNull(..) => true,
            Expr::Like { lhs, rhs, .. } => lhs.is_nonnull(tables) && rhs.is_nonnull(tables),
            Expr::Literal(literal) => match literal {
                ast::Literal::Numeric(_) => true,
                ast::Literal::String(_) => true,
                ast::Literal::Blob(_) => true,
                ast::Literal::Keyword(_) => true,
                ast::Literal::Null => false,
                ast::Literal::CurrentDate => true,
                ast::Literal::CurrentTime => true,
                ast::Literal::CurrentTimestamp => true,
            },
            Expr::Name(..) => false,
            Expr::NotNull(..) => true,
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_nonnull(tables)),
            Expr::Qualified(..) => {
                panic!("Do not call is_nonnull before Qualified has been rewritten as Column")
            }
            Expr::Raise(..) => false,
            Expr::Subquery(..) => false,
            Expr::Unary(_, expr) => expr.is_nonnull(tables),
            Expr::Variable(..) => false,
        }
    }
    /// Returns true if the expression is a constant i.e. does not depend on variables or columns etc.
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool {
        match self {
            Expr::Between {
                lhs, start, end, ..
            } => {
                lhs.is_constant(resolver)
                    && start.is_constant(resolver)
                    && end.is_constant(resolver)
            }
            Expr::Binary(expr, _, expr1) => {
                expr.is_constant(resolver) && expr1.is_constant(resolver)
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                base.as_ref().is_none_or(|base| base.is_constant(resolver))
                    && when_then_pairs.iter().all(|(when, then)| {
                        when.is_constant(resolver) && then.is_constant(resolver)
                    })
                    && else_expr
                        .as_ref()
                        .is_none_or(|else_expr| else_expr.is_constant(resolver))
            }
            Expr::Cast { expr, .. } => expr.is_constant(resolver),
            Expr::Collate(expr, _) => expr.is_constant(resolver),
            Expr::DoublyQualified(_, _, _) => {
                panic!("DoublyQualified should have been rewritten as Column")
            }
            Expr::Exists(_) => false,
            Expr::FunctionCall { args, name, .. } => {
                let Some(func) =
                    resolver.resolve_function(&name.0, args.as_ref().map_or(0, |args| args.len()))
                else {
                    return false;
                };
                func.is_deterministic()
                    && args
                        .as_ref()
                        .is_none_or(|args| args.iter().all(|arg| arg.is_constant(resolver)))
            }
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(_) => panic!("Id should have been rewritten as Column"),
            Expr::Column { .. } => false,
            Expr::RowId { .. } => false,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_constant(resolver)
                    && rhs
                        .as_ref()
                        .is_none_or(|rhs| rhs.iter().all(|rhs| rhs.is_constant(resolver)))
            }
            Expr::InSelect { .. } => {
                false // might be constant, too annoying to check subqueries etc. implement later
            }
            Expr::InTable { .. } => false,
            Expr::IsNull(expr) => expr.is_constant(resolver),
            Expr::Like {
                lhs, rhs, escape, ..
            } => {
                lhs.is_constant(resolver)
                    && rhs.is_constant(resolver)
                    && escape
                        .as_ref()
                        .is_none_or(|escape| escape.is_constant(resolver))
            }
            Expr::Literal(_) => true,
            Expr::Name(_) => false,
            Expr::NotNull(expr) => expr.is_constant(resolver),
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_constant(resolver)),
            Expr::Qualified(_, _) => {
                panic!("Qualified should have been rewritten as Column")
            }
            Expr::Raise(_, expr) => expr.as_ref().is_none_or(|expr| expr.is_constant(resolver)),
            Expr::Subquery(_) => false,
            Expr::Unary(_, expr) => expr.is_constant(resolver),
            Expr::Variable(_) => false,
        }
    }
    /// Returns true if the expression is a constant expression that, when evaluated as a condition, is always true or false
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>> {
        match self {
            Self::Literal(lit) => match lit {
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    let without_quotes = s.trim_matches('\'');
                    if let Ok(int_value) = without_quotes.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    if let Ok(float_value) = without_quotes.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(Some(AlwaysTrueOrFalse::AlwaysFalse))
                }
                _ => Ok(None),
            },
            Self::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial.map(|t| match t {
                        AlwaysTrueOrFalse::AlwaysTrue => AlwaysTrueOrFalse::AlwaysFalse,
                        AlwaysTrueOrFalse::AlwaysFalse => AlwaysTrueOrFalse::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            Self::InList { lhs: _, not, rhs } => {
                if rhs.is_none() {
                    return Ok(Some(if *not {
                        AlwaysTrueOrFalse::AlwaysTrue
                    } else {
                        AlwaysTrueOrFalse::AlwaysFalse
                    }));
                }
                let rhs = rhs.as_ref().unwrap();
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        AlwaysTrueOrFalse::AlwaysTrue
                    } else {
                        AlwaysTrueOrFalse::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_always_true_or_false()?;
                let rhs_trivial = rhs.check_always_true_or_false()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }

                        Ok(None)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

fn ephemeral_index_build(
    table_reference: &JoinedTable,
    constraints: &[Constraint],
    constraint_refs: &[ConstraintRef],
) -> Index {
    let mut ephemeral_columns: Vec<IndexColumn> = table_reference
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| IndexColumn {
            name: c.name.clone().unwrap(),
            order: SortOrder::Asc,
            pos_in_table: i,
            collation: c.collation,
            default: c.default.clone(),
        })
        // only include columns that are used in the query
        .filter(|c| table_reference.column_is_used(c.pos_in_table))
        .collect();
    // sort so that constraints first, then rest in whatever order they were in in the table
    ephemeral_columns.sort_by(|a, b| {
        let a_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| constraints[c.constraint_vec_pos].table_col_pos == a.pos_in_table);
        let b_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| constraints[c.constraint_vec_pos].table_col_pos == b.pos_in_table);
        match (a_constraint, b_constraint) {
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((a_idx, _)), Some((b_idx, _))) => a_idx.cmp(&b_idx),
            (None, None) => Ordering::Equal,
        }
    });
    let ephemeral_index = Index {
        name: format!(
            "ephemeral_{}_{}",
            table_reference.table.get_name(),
            table_reference.internal_id
        ),
        columns: ephemeral_columns,
        unique: false,
        ephemeral: true,
        table_name: table_reference.table.get_name().to_string(),
        root_page: 0,
        has_rowid: table_reference
            .table
            .btree()
            .is_some_and(|btree| btree.has_rowid),
    };

    ephemeral_index
}

/// Build a [SeekDef] for a given list of [Constraint]s
pub fn build_seek_def_from_constraints(
    constraints: &[Constraint],
    constraint_refs: &[ConstraintRef],
    iter_dir: IterationDirection,
    where_clause: &[WhereTerm],
) -> Result<SeekDef> {
    assert!(
        !constraint_refs.is_empty(),
        "cannot build seek def from empty list of constraint refs"
    );
    // Extract the key values and operators
    let key = constraint_refs
        .iter()
        .map(|cref| cref.as_seek_key_column(constraints, where_clause))
        .collect();

    // We know all but potentially the last term is an equality, so we can use the operator of the last term
    // to form the SeekOp
    let op = constraints[constraint_refs.last().unwrap().constraint_vec_pos].operator;

    let seek_def = build_seek_def(op, iter_dir, key)?;
    Ok(seek_def)
}

/// Build a [SeekDef] for a given comparison operator and index key.
/// To be usable as a seek key, all but potentially the last term must be equalities.
/// The last term can be a nonequality.
/// The comparison operator referred to by `op` is the operator of the last term.
///
/// There are two parts to the seek definition:
/// 1. The [SeekKey], which specifies the key that we will use to seek to the first row that matches the index key.
/// 2. The [TerminationKey], which specifies the key that we will use to terminate the index scan that follows the seek.
///
/// There are some nuances to how, and which parts of, the index key can be used in the [SeekKey] and [TerminationKey],
/// depending on the operator and iteration order. This function explains those nuances inline when dealing with
/// each case.
///
/// But to illustrate the general idea, consider the following examples:
///
/// 1. For example, having two conditions like (x>10 AND y>20) cannot be used as a valid [SeekKey] GT(x:10, y:20)
///    because the first row greater than (x:10, y:20) might be (x:10, y:21), which does not satisfy the where clause.
///    In this case, only GT(x:10) must be used as the [SeekKey], and rows with y <= 20 must be filtered as a regular condition expression for each value of x.
///
/// 2. In contrast, having (x=10 AND y>20) forms a valid index key GT(x:10, y:20) because after the seek, we can simply terminate as soon as x > 10,
///    i.e. use GT(x:10, y:20) as the [SeekKey] and GT(x:10) as the [TerminationKey].
///
/// The preceding examples are for an ascending index. The logic is similar for descending indexes, but an important distinction is that
/// since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
/// So when you see e.g. a SeekOp::GT below for a descending index, it actually means that we are seeking the first row where the index key is LESS than the seek key.
///
fn build_seek_def(
    op: ast::Operator,
    iter_dir: IterationDirection,
    key: Vec<(ast::Expr, SortOrder)>,
) -> Result<SeekDef> {
    let key_len = key.len();
    let sort_order_of_last_key = key.last().unwrap().1;

    // For the commented examples below, keep in mind that since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
    // Also keep in mind that index keys are compared based on the number of columns given, so for example:
    // - if key is GT(x:10), then (x=10, y=usize::MAX) is not GT because only X is compared. (x=11, y=<any>) is GT.
    // - if key is GT(x:10, y:20), then (x=10, y=21) is GT because both X and Y are compared.
    // - if key is GT(x:10, y:NULL), then (x=10, y=0) is GT because NULL is always LT in index key comparisons.
    Ok(match (iter_dir, op) {
        // Forwards, EQ:
        // Example: (x=10 AND y=20)
        // Seek key: start from the first GE(x:10, y:20)
        // Termination key: end at the first GT(x:10, y:20)
        // Ascending vs descending doesn't matter because all the comparisons are equalities.
        (IterationDirection::Forwards, ast::Operator::Equals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len,
                null_pad: false,
                op: SeekOp::GE { eq_only: true },
            }),
            termination: Some(TerminationKey {
                len: key_len,
                null_pad: false,
                op: SeekOp::GT,
            }),
        },
        // Forwards, GT:
        // Ascending index example: (x=10 AND y>20)
        // Seek key: start from the first GT(x:10, y:20), e.g. (x=10, y=21)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0)
        //
        // Descending index example: (x=10 AND y>20)
        // Seek key: start from the first LE(x:10), e.g. (x=10, y=usize::MAX), so reversed -> GE(x:10)
        // Termination key: end at the first LE(x:10, y:20), e.g. (x=10, y=20) so reversed -> GE(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::Greater) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len, key_len - 1, SeekOp::GT, SeekOp::GT)
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::LE { eq_only: false }.reverse(),
                        SeekOp::LE { eq_only: false }.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, GE:
        // Ascending index example: (x=10 AND y>=20)
        // Seek key: start from the first GE(x:10, y:20), e.g. (x=10, y=20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0)
        //
        // Descending index example: (x=10 AND y>=20)
        // Seek key: start from the first LE(x:10), e.g. (x=10, y=usize::MAX), so reversed -> GE(x:10)
        // Termination key: end at the first LT(x:10, y:20), e.g. (x=10, y=19), so reversed -> GT(x:10, y:20)
        (IterationDirection::Forwards, ast::Operator::GreaterEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::GE { eq_only: false },
                        SeekOp::GT,
                    )
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::LE { eq_only: false }.reverse(),
                        SeekOp::LT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, LT:
        // Ascending index example: (x=10 AND y<20)
        // Seek key: start from the first GT(x:10, y: NULL), e.g. (x=10, y=0)
        // Termination key: end at the first GE(x:10, y:20), e.g. (x=10, y=20)
        //
        // Descending index example: (x=10 AND y<20)
        // Seek key: start from the first LT(x:10, y:20), e.g. (x=10, y=19) so reversed -> GT(x:10, y:20)
        // Termination key: end at the first LT(x:10), e.g. (x=9, y=usize::MAX), so reversed -> GE(x:10, NULL); i.e. GE the smallest possible (x=10, y) combination (NULL is always LT)
        (IterationDirection::Forwards, ast::Operator::Less) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::GT,
                        SeekOp::GE { eq_only: false },
                    )
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::GT,
                        SeekOp::GE { eq_only: false },
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
            }
        }
        // Forwards, LE:
        // Ascending index example: (x=10 AND y<=20)
        // Seek key: start from the first GE(x:10, y:NULL), e.g. (x=10, y=0)
        // Termination key: end at the first GT(x:10, y:20), e.g. (x=10, y=21)
        //
        // Descending index example: (x=10 AND y<=20)
        // Seek key: start from the first LE(x:10, y:20), e.g. (x=10, y=20) so reversed -> GE(x:10, y:20)
        // Termination key: end at the first LT(x:10), e.g. (x=9, y=usize::MAX), so reversed -> GE(x:10, NULL); i.e. GE the smallest possible (x=10, y) combination (NULL is always LT)
        (IterationDirection::Forwards, ast::Operator::LessEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (key_len - 1, key_len, SeekOp::GT, SeekOp::GT)
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::LE { eq_only: false }.reverse(),
                        SeekOp::LE { eq_only: false }.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, EQ:
        // Example: (x=10 AND y=20)
        // Seek key: start from the last LE(x:10, y:20)
        // Termination key: end at the first LT(x:10, y:20)
        // Ascending vs descending doesn't matter because all the comparisons are equalities.
        (IterationDirection::Backwards, ast::Operator::Equals) => SeekDef {
            key,
            iter_dir,
            seek: Some(SeekKey {
                len: key_len,
                op: SeekOp::LE { eq_only: true },
                null_pad: false,
            }),
            termination: Some(TerminationKey {
                len: key_len,
                op: SeekOp::LT,
                null_pad: false,
            }),
        },
        // Backwards, LT:
        // Ascending index example: (x=10 AND y<20)
        // Seek key: start from the last LT(x:10, y:20), e.g. (x=10, y=19)
        // Termination key: end at the first LE(x:10, NULL), e.g. (x=9, y=usize::MAX)
        //
        // Descending index example: (x=10 AND y<20)
        // Seek key: start from the last GT(x:10, y:NULL), e.g. (x=10, y=0) so reversed -> LT(x:10, NULL)
        // Termination key: end at the first GE(x:10, y:20), e.g. (x=10, y=20) so reversed -> LE(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::Less) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::LT,
                        SeekOp::LE { eq_only: false },
                    )
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::GT.reverse(),
                        SeekOp::GE { eq_only: false }.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, LE:
        // Ascending index example: (x=10 AND y<=20)
        // Seek key: start from the last LE(x:10, y:20), e.g. (x=10, y=20)
        // Termination key: end at the first LT(x:10, NULL), e.g. (x=9, y=usize::MAX)
        //
        // Descending index example: (x=10 AND y<=20)
        // Seek key: start from the last GT(x:10, NULL), e.g. (x=10, y=0) so reversed -> LT(x:10, NULL)
        // Termination key: end at the first GT(x:10, y:20), e.g. (x=10, y=21) so reversed -> LT(x:10, y:20)
        (IterationDirection::Backwards, ast::Operator::LessEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::LE { eq_only: false },
                        SeekOp::LE { eq_only: false },
                    )
                } else {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::GT.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: sort_order_of_last_key == SortOrder::Desc,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: sort_order_of_last_key == SortOrder::Asc,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, GT:
        // Ascending index example: (x=10 AND y>20)
        // Seek key: start from the last LE(x:10), e.g. (x=10, y=usize::MAX)
        // Termination key: end at the first LE(x:10, y:20), e.g. (x=10, y=20)
        //
        // Descending index example: (x=10 AND y>20)
        // Seek key: start from the last GT(x:10, y:20), e.g. (x=10, y=21) so reversed -> LT(x:10, y:20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0) so reversed -> LT(x:10)
        (IterationDirection::Backwards, ast::Operator::Greater) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::LE { eq_only: false },
                        SeekOp::LE { eq_only: false },
                    )
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::GT.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        // Backwards, GE:
        // Ascending index example: (x=10 AND y>=20)
        // Seek key: start from the last LE(x:10), e.g. (x=10, y=usize::MAX)
        // Termination key: end at the first LT(x:10, y:20), e.g. (x=10, y=19)
        //
        // Descending index example: (x=10 AND y>=20)
        // Seek key: start from the last GE(x:10, y:20), e.g. (x=10, y=20) so reversed -> LE(x:10, y:20)
        // Termination key: end at the first GT(x:10), e.g. (x=11, y=0) so reversed -> LT(x:10)
        (IterationDirection::Backwards, ast::Operator::GreaterEquals) => {
            let (seek_key_len, termination_key_len, seek_op, termination_op) =
                if sort_order_of_last_key == SortOrder::Asc {
                    (
                        key_len - 1,
                        key_len,
                        SeekOp::LE { eq_only: false },
                        SeekOp::LT,
                    )
                } else {
                    (
                        key_len,
                        key_len - 1,
                        SeekOp::GE { eq_only: false }.reverse(),
                        SeekOp::GT.reverse(),
                    )
                };
            SeekDef {
                key,
                iter_dir,
                seek: if seek_key_len > 0 {
                    Some(SeekKey {
                        len: seek_key_len,
                        op: seek_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
                termination: if termination_key_len > 0 {
                    Some(TerminationKey {
                        len: termination_key_len,
                        op: termination_op,
                        null_pad: false,
                    })
                } else {
                    None
                },
            }
        }
        (_, op) => {
            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
        }
    })
}

pub fn rewrite_expr(top_level_expr: &mut ast::Expr, param_idx: &mut usize) -> Result<()> {
    walk_expr_mut(top_level_expr, &mut |expr: &mut ast::Expr| -> Result<()> {
        match expr {
            ast::Expr::Id(id) => {
                // Convert "true" and "false" to 1 and 0
                if id.0.eq_ignore_ascii_case("true") {
                    *expr = ast::Expr::Literal(ast::Literal::Numeric(1.to_string()));
                    return Ok(());
                }
                if id.0.eq_ignore_ascii_case("false") {
                    *expr = ast::Expr::Literal(ast::Literal::Numeric(0.to_string()));
                }
            }
            ast::Expr::Variable(var) => {
                if var.is_empty() {
                    // rewrite anonymous variables only, ensure that the `param_idx` starts at 1 and
                    // all the expressions are rewritten in the order they come in the statement
                    *expr = ast::Expr::Variable(format!("{PARAM_PREFIX}{param_idx}"));
                    *param_idx += 1;
                }
            }
            ast::Expr::Between {
                lhs,
                not,
                start,
                end,
            } => {
                // Convert `y NOT BETWEEN x AND z` to `x > y OR y > z`
                let (lower_op, upper_op) = if *not {
                    (ast::Operator::Greater, ast::Operator::Greater)
                } else {
                    // Convert `y BETWEEN x AND z` to `x <= y AND y <= z`
                    (ast::Operator::LessEquals, ast::Operator::LessEquals)
                };

                let start = start.take_ownership();
                let lhs = lhs.take_ownership();
                let end = end.take_ownership();

                let lower_bound =
                    ast::Expr::Binary(Box::new(start), lower_op, Box::new(lhs.clone()));
                let upper_bound = ast::Expr::Binary(Box::new(lhs), upper_op, Box::new(end));

                if *not {
                    *expr = ast::Expr::Binary(
                        Box::new(lower_bound),
                        ast::Operator::Or,
                        Box::new(upper_bound),
                    );
                } else {
                    *expr = ast::Expr::Binary(
                        Box::new(lower_bound),
                        ast::Operator::And,
                        Box::new(upper_bound),
                    );
                }
            }
            _ => {}
        }

        Ok(())
    })
}

trait TakeOwnership {
    fn take_ownership(&mut self) -> Self;
}

impl TakeOwnership for ast::Expr {
    fn take_ownership(&mut self) -> Self {
        std::mem::replace(self, ast::Expr::Literal(ast::Literal::Null))
    }
}
