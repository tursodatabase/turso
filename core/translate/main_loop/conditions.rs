use super::*;
use crate::translate::subquery::emit_non_from_clause_subquery;

fn condition_references_subquery(expr: &Expr, subqueries: &[NonFromClauseSubquery]) -> bool {
    subqueries
        .iter()
        .any(|s| expr_references_subquery_id(expr, s.internal_id))
}

/// Emit correlated subqueries at the loop that checks their conditions.
///
/// A RIGHT JOIN can move a WHERE condition past the subquery's first possible
/// loop. The subquery must move with the condition so its input cursors are valid.
#[allow(clippy::too_many_arguments)]
fn emit_correlated_subqueries(
    program: &mut ProgramBuilder,
    resolver: &Resolver<'_>,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    join_index: usize,
    predicates: &[WhereTerm],
    subqueries: &mut [NonFromClauseSubquery],
    outer_join_terms: bool,
) -> Result<()> {
    let mut subqueries_to_emit = Vec::new();
    for (subquery_index, subquery) in subqueries.iter().enumerate() {
        if subquery.has_been_evaluated()
            || !subquery.correlated
            || !matches!(subquery.eval_phase, SubqueryEvalPhase::BeforeLoop)
        {
            continue;
        }

        let conditions = predicates.iter().filter(|condition| {
            !condition.consumed
                && condition
                    .origin
                    .join_origin()
                    .is_some_and(JoinOrigin::is_outer)
                    == outer_join_terms
                && expr_references_subquery_id(&condition.expr, subquery.internal_id)
        });
        let mut condition_count = 0;
        let mut condition_runs_here = false;
        for condition in conditions {
            condition_count += 1;
            condition_runs_here |= condition.should_eval_at_loop(
                join_index,
                join_order,
                subqueries,
                Some(table_references),
            );
        }
        if condition_count > 0 {
            // A WHERE subquery must run with its condition. A RIGHT JOIN can
            // move that condition to a later loop so unmatched rows can use it.
            if condition_runs_here {
                subqueries_to_emit.push(subquery_index);
            }
            continue;
        }

        // The non-outer pass also emits correlated subqueries used outside
        // predicates, such as a subquery in the result list.
        if !outer_join_terms
            && subquery.get_eval_at(join_order, Some(table_references))? == EvalAt::Loop(join_index)
        {
            subqueries_to_emit.push(subquery_index);
        }
    }

    for subquery_index in subqueries_to_emit {
        let subquery = &mut subqueries[subquery_index];
        let subquery_plan = subquery.consume_plan(EvalAt::Loop(join_index));
        emit_non_from_clause_subquery(
            program,
            resolver,
            *subquery_plan,
            &subquery.query_type,
            true,
            false,
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubqueryRefFilter {
    WithoutSubqueryRefs,
    WithSubqueryRefs,
}

#[allow(clippy::too_many_arguments)]
fn emit_conditions(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx<'_>,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    join_index: usize,
    next: BranchOffset,
    outer_join_terms: bool,
    subqueries: &[NonFromClauseSubquery],
    subquery_ref_filter: SubqueryRefFilter,
) -> Result<()> {
    for cond in predicates
        .iter()
        .filter(|cond| {
            cond.origin.join_origin().is_some_and(JoinOrigin::is_outer) == outer_join_terms
        })
        .filter(|cond| {
            cond.should_eval_at_loop(join_index, join_order, subqueries, Some(table_references))
        })
        .filter(|cond| match subquery_ref_filter {
            SubqueryRefFilter::WithoutSubqueryRefs => {
                !condition_references_subquery(&cond.expr, subqueries)
            }
            SubqueryRefFilter::WithSubqueryRefs => {
                condition_references_subquery(&cond.expr, subqueries)
            }
        })
    {
        let jump_target_when_true = program.allocate_label();
        let condition_metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true,
            jump_target_when_false: next,
            jump_target_when_null: next,
        };
        translate_condition_expr(
            program,
            table_references,
            &cond.expr,
            condition_metadata,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target_when_true);
    }

    Ok(())
}

/// Per-loop predicate emission.
///
/// Conditions that reference subquery results cannot be emitted until their
/// correlated subqueries have run, so emission proceeds in three ordered steps.
pub(super) struct LoopConditionEmitter<'a, 'ctx> {
    program: &'a mut ProgramBuilder,
    t_ctx: &'a TranslateCtx<'ctx>,
    table_references: &'a TableReferences,
    join_order: &'a [JoinOrderMember],
    predicates: &'a [WhereTerm],
    join_index: usize,
    condition_fail_target: BranchOffset,
    outer_join_terms: bool,
    subqueries: &'a mut [NonFromClauseSubquery],
}

impl<'a, 'ctx> LoopConditionEmitter<'a, 'ctx> {
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        program: &'a mut ProgramBuilder,
        t_ctx: &'a TranslateCtx<'ctx>,
        table_references: &'a TableReferences,
        join_order: &'a [JoinOrderMember],
        predicates: &'a [WhereTerm],
        join_index: usize,
        condition_fail_target: BranchOffset,
        outer_join_terms: bool,
        subqueries: &'a mut [NonFromClauseSubquery],
    ) -> Self {
        Self {
            program,
            t_ctx,
            table_references,
            join_order,
            predicates,
            join_index,
            condition_fail_target,
            outer_join_terms,
            subqueries,
        }
    }

    /// Emit predicates that do not depend on subquery result registers.
    fn emit_early_conditions(&mut self) -> Result<()> {
        emit_conditions(
            self.program,
            self.t_ctx,
            self.table_references,
            self.join_order,
            self.predicates,
            self.join_index,
            self.condition_fail_target,
            self.outer_join_terms,
            self.subqueries,
            SubqueryRefFilter::WithoutSubqueryRefs,
        )
    }

    /// Materialize correlated subqueries that become valid at this loop depth.
    fn emit_correlated_subqueries(&mut self) -> Result<()> {
        emit_correlated_subqueries(
            self.program,
            &self.t_ctx.resolver,
            self.table_references,
            self.join_order,
            self.join_index,
            self.predicates,
            self.subqueries,
            self.outer_join_terms,
        )
    }

    /// Emit predicates that read registers populated by correlated subqueries.
    fn emit_late_conditions(&mut self) -> Result<()> {
        emit_conditions(
            self.program,
            self.t_ctx,
            self.table_references,
            self.join_order,
            self.predicates,
            self.join_index,
            self.condition_fail_target,
            self.outer_join_terms,
            self.subqueries,
            SubqueryRefFilter::WithSubqueryRefs,
        )
    }

    pub(super) fn emit(mut self) -> Result<()> {
        self.emit_early_conditions()?;
        self.emit_correlated_subqueries()?;
        self.emit_late_conditions()
    }
}
