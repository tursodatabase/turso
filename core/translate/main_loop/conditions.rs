use super::*;
use std::marker::PhantomData;

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

fn condition_references_subquery(expr: &Expr, subqueries: &[NonFromClauseSubquery]) -> bool {
    subqueries
        .iter()
        .any(|s| expr_references_subquery_id(expr, s.internal_id))
}

fn subquery_referenced_in_predicates(
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
fn emit_correlated_subqueries(
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
        if on_only && !subquery_referenced_in_predicates(predicates, true, subquery.internal_id) {
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
    from_outer_join: bool,
    subqueries: &[NonFromClauseSubquery],
    subquery_ref_filter: SubqueryRefFilter,
) -> Result<()> {
    for cond in predicates
        .iter()
        .filter(|cond| cond.from_outer_join.is_some() == from_outer_join)
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

struct PendingEarlyConditions;
struct PendingCorrelatedSubqueries;
struct PendingLateConditions;

struct LoopConditionEmitter<'program, 'ctx, 'subqueries, State> {
    program: &'program mut ProgramBuilder,
    t_ctx: &'ctx TranslateCtx<'ctx>,
    table_references: &'ctx TableReferences,
    join_order: &'ctx [JoinOrderMember],
    predicates: &'ctx [WhereTerm],
    join_index: usize,
    condition_fail_target: BranchOffset,
    from_outer_join: bool,
    subqueries: &'subqueries mut [NonFromClauseSubquery],
    _state: PhantomData<State>,
}

impl<'program, 'ctx, 'subqueries, State> LoopConditionEmitter<'program, 'ctx, 'subqueries, State> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        program: &'program mut ProgramBuilder,
        t_ctx: &'ctx TranslateCtx<'ctx>,
        table_references: &'ctx TableReferences,
        join_order: &'ctx [JoinOrderMember],
        predicates: &'ctx [WhereTerm],
        join_index: usize,
        condition_fail_target: BranchOffset,
        from_outer_join: bool,
        subqueries: &'subqueries mut [NonFromClauseSubquery],
    ) -> Self {
        Self {
            program,
            t_ctx,
            table_references,
            join_order,
            predicates,
            join_index,
            condition_fail_target,
            from_outer_join,
            subqueries,
            _state: PhantomData,
        }
    }
}

impl<'program, 'ctx, 'subqueries>
    LoopConditionEmitter<'program, 'ctx, 'subqueries, PendingEarlyConditions>
{
    fn emit_early_conditions(
        self,
    ) -> Result<LoopConditionEmitter<'program, 'ctx, 'subqueries, PendingCorrelatedSubqueries>>
    {
        emit_conditions(
            self.program,
            self.t_ctx,
            self.table_references,
            self.join_order,
            self.predicates,
            self.join_index,
            self.condition_fail_target,
            self.from_outer_join,
            self.subqueries,
            SubqueryRefFilter::WithoutSubqueryRefs,
        )?;
        Ok(LoopConditionEmitter::new(
            self.program,
            self.t_ctx,
            self.table_references,
            self.join_order,
            self.predicates,
            self.join_index,
            self.condition_fail_target,
            self.from_outer_join,
            self.subqueries,
        ))
    }
}

impl<'program, 'ctx, 'subqueries>
    LoopConditionEmitter<'program, 'ctx, 'subqueries, PendingCorrelatedSubqueries>
{
    fn emit_correlated_subqueries(
        self,
    ) -> Result<LoopConditionEmitter<'program, 'ctx, 'subqueries, PendingLateConditions>> {
        emit_correlated_subqueries(
            self.program,
            &self.t_ctx.resolver,
            self.table_references,
            self.join_order,
            self.join_index,
            self.predicates,
            self.subqueries,
            self.from_outer_join,
        )?;
        Ok(LoopConditionEmitter::new(
            self.program,
            self.t_ctx,
            self.table_references,
            self.join_order,
            self.predicates,
            self.join_index,
            self.condition_fail_target,
            self.from_outer_join,
            self.subqueries,
        ))
    }
}

impl<'program, 'ctx, 'subqueries>
    LoopConditionEmitter<'program, 'ctx, 'subqueries, PendingLateConditions>
{
    fn emit_late_conditions(self) -> Result<()> {
        emit_conditions(
            self.program,
            self.t_ctx,
            self.table_references,
            self.join_order,
            self.predicates,
            self.join_index,
            self.condition_fail_target,
            self.from_outer_join,
            self.subqueries,
            SubqueryRefFilter::WithSubqueryRefs,
        )
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_conditions_with_subqueries(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx<'_>,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    join_index: usize,
    condition_fail_target: BranchOffset,
    from_outer_join: bool,
    subqueries: &mut [NonFromClauseSubquery],
) -> Result<()> {
    LoopConditionEmitter::<PendingEarlyConditions>::new(
        program,
        t_ctx,
        table_references,
        join_order,
        predicates,
        join_index,
        condition_fail_target,
        from_outer_join,
        subqueries,
    )
    .emit_early_conditions()?
    .emit_correlated_subqueries()?
    .emit_late_conditions()
}
