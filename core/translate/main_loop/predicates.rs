use super::*;

/// Emits a boolean condition that jumps to `fail_target` on false or NULL.
pub(super) fn emit_condition_with_fail_target(
    program: &mut ProgramBuilder,
    resolver: &Resolver<'_>,
    table_references: &TableReferences,
    expr: &Expr,
    fail_target: BranchOffset,
) -> Result<()> {
    let jump_target_when_true = program.allocate_label();
    let condition_metadata = ConditionMetadata {
        jump_if_condition_is_true: false,
        jump_target_when_true,
        jump_target_when_false: fail_target,
        jump_target_when_null: fail_target,
    };
    translate_condition_expr(
        program,
        table_references,
        expr,
        condition_metadata,
        resolver,
    )?;
    program.preassign_label_to_next_insn(jump_target_when_true);
    Ok(())
}

/// Returns true when the expression tree references this subquery result id.
fn expr_references_subquery_id(expr: &Expr, subquery_id: TableInternalId) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |e: &Expr| -> Result<WalkControl> {
        if let Expr::SubqueryResult {
            subquery_id: sid, ..
        } = e
        {
            if *sid == subquery_id {
                found = true;
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    });
    found
}

/// Check if an expression references any of the given subqueries.
fn condition_references_non_from_subquery_result(
    expr: &Expr,
    subqueries: &[NonFromClauseSubquery],
) -> bool {
    subqueries
        .iter()
        .any(|s| expr_references_subquery_id(expr, s.internal_id))
}

/// Returns true when any predicate in this phase reads the subquery result.
fn non_from_subquery_result_referenced_in_predicates(
    predicates: &[WhereTerm],
    from_outer_join: bool,
    subquery_id: TableInternalId,
) -> bool {
    predicates
        .iter()
        .filter(|cond| cond.from_outer_join.is_some() == from_outer_join)
        .any(|cond| expr_references_subquery_id(&cond.expr, subquery_id))
}

#[allow(clippy::too_many_arguments)]
/// Emits correlated subqueries scheduled for the current loop depth.
fn emit_correlated_subqueries_for_loop_phase(
    program: &mut ProgramBuilder,
    resolver: &Resolver<'_>,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    join_index: usize,
    predicates: &[WhereTerm],
    subqueries: &mut [NonFromClauseSubquery],
    on_only: bool,
) -> Result<()> {
    for subquery in subqueries.iter_mut().filter(|s| !s.has_been_evaluated()) {
        if !subquery.correlated {
            continue;
        }
        if on_only
            && !non_from_subquery_result_referenced_in_predicates(
                predicates,
                true,
                subquery.internal_id,
            )
        {
            continue;
        }
        let eval_at = subquery.get_eval_at(join_order, Some(table_references))?;
        if eval_at != EvalAt::Loop(join_index) {
            continue;
        }

        let plan = subquery.consume_plan(eval_at);
        emit_non_from_clause_subquery(
            program,
            resolver,
            *plan,
            &subquery.query_type,
            subquery.correlated,
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
/// Controls whether we emit pre-subquery or post-subquery predicates.
enum PredicateSubqueryReferenceFilter {
    /// Only emit conditions that do NOT reference subqueries (for early evaluation)
    WithoutSubqueryRefs,
    /// Only emit conditions that DO reference subqueries (for late evaluation)
    WithSubqueryRefs,
}

/// Emits conditions and correlated subqueries in the correct order:
/// 1. Conditions that do NOT reference subquery results (early evaluation)
/// 2. Correlated subqueries (to produce their results)
/// 3. Conditions that DO reference subquery results (late evaluation)
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_conditions_with_subqueries(
    program: &mut ProgramBuilder,
    resolver: &Resolver<'_>,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    join_index: usize,
    condition_fail_target: BranchOffset,
    from_outer_join: bool,
    subqueries: &mut [NonFromClauseSubquery],
) -> Result<()> {
    emit_loop_phase_conditions(
        program,
        resolver,
        table_references,
        join_order,
        predicates,
        join_index,
        condition_fail_target,
        from_outer_join,
        subqueries,
        PredicateSubqueryReferenceFilter::WithoutSubqueryRefs,
    )?;
    emit_correlated_subqueries_for_loop_phase(
        program,
        resolver,
        table_references,
        join_order,
        join_index,
        predicates,
        subqueries,
        from_outer_join,
    )?;
    emit_loop_phase_conditions(
        program,
        resolver,
        table_references,
        join_order,
        predicates,
        join_index,
        condition_fail_target,
        from_outer_join,
        subqueries,
        PredicateSubqueryReferenceFilter::WithSubqueryRefs,
    )
}

#[allow(clippy::too_many_arguments)]
/// Emits WHERE/ON predicates that must be evaluated at the current join loop.
fn emit_loop_phase_conditions(
    program: &mut ProgramBuilder,
    resolver: &Resolver<'_>,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    join_index: usize,
    next: BranchOffset,
    from_outer_join: bool,
    subqueries: &[NonFromClauseSubquery],
    subquery_ref_filter: PredicateSubqueryReferenceFilter,
) -> Result<()> {
    for cond in predicates
        .iter()
        .filter(|cond| cond.from_outer_join.is_some() == from_outer_join)
        .filter(|cond| {
            cond.should_eval_at_loop(join_index, join_order, subqueries, Some(table_references))
        })
        .filter(|cond| match subquery_ref_filter {
            PredicateSubqueryReferenceFilter::WithoutSubqueryRefs => {
                !condition_references_non_from_subquery_result(&cond.expr, subqueries)
            }
            PredicateSubqueryReferenceFilter::WithSubqueryRefs => {
                condition_references_non_from_subquery_result(&cond.expr, subqueries)
            }
        })
    {
        emit_condition_with_fail_target(program, resolver, table_references, &cond.expr, next)?;
    }

    Ok(())
}
