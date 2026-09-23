use super::*;
use crate::translate::subquery::emit_non_from_clause_subqueries_for_eval_at;

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
    emit_non_from_clause_subqueries_for_eval_at(
        program,
        resolver,
        subqueries,
        join_order,
        Some(table_references),
        EvalAt::Loop(join_index),
        |subquery| {
            subquery.correlated
                && (!on_only
                    || subquery_referenced_in_predicates(predicates, true, subquery.internal_id))
        },
    )
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
    let prefiltered_terms = hash_build_prefilter_where_terms(
        t_ctx,
        table_references,
        join_order,
        predicates,
        subqueries,
        join_index,
    )?;
    for cond in predicates
        .iter()
        .enumerate()
        .filter(|(cond_idx, _)| !prefiltered_terms.contains(cond_idx))
        .map(|(_, cond)| cond)
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
        emit_where_term(program, table_references, cond, next, &t_ctx.resolver)?;
    }

    Ok(())
}

pub(super) fn emit_where_term(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    term: &WhereTerm,
    next: BranchOffset,
    resolver: &Resolver,
) -> Result<()> {
    let matched = program.allocate_label();
    let Some((table, query)) =
        crate::translate::optimizer::plan_index_method_predicate(term, table_references, resolver)
    else {
        translate_condition_expr(
            program,
            table_references,
            &term.expr,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: matched,
                jump_target_when_false: next,
                jump_target_when_null: next,
            },
            resolver,
        )?;
        program.preassign_label_to_next_insn(matched);
        return Ok(());
    };

    emit_explain!(
        program,
        false,
        crate::translate::eqp::EqpDetail::IndexMethod {
            method: query
                .index
                .index_method
                .as_ref()
                .unwrap()
                .definition()
                .method_name
                .to_string(),
            estimate: None,
        }
    );
    let cursor_id = program.alloc_cursor_index(None, &query.index)?;
    let opened = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: opened,
    });
    program.emit_insn(Insn::OpenRead {
        cursor_id,
        root_page: query.index.root_page,
        db: table.database_id,
    });
    program.preassign_label_to_next_insn(opened);

    let rowid_reg = program.alloc_register();
    translate_expr(
        program,
        Some(table_references),
        &Expr::RowId {
            database: None,
            table: table.internal_id,
        },
        rowid_reg,
        resolver,
    )?;
    program.emit_insn(Insn::IsNull {
        reg: rowid_reg,
        target_pc: next,
    });
    let start_reg = program.alloc_registers(query.arguments.len() + 1);
    program.emit_int(query.pattern_idx as i64, start_reg);
    for (i, argument) in query.arguments.iter().enumerate() {
        translate_expr(
            program,
            Some(table_references),
            argument,
            start_reg + i + 1,
            resolver,
        )?;
    }
    program.emit_insn(Insn::IndexMethodQuery {
        db: table.database_id,
        cursor_id,
        start_reg,
        count_reg: query.arguments.len() + 1,
        pc_if_empty: next,
    });
    let result_rowid = program.alloc_register();
    let search_next = program.allocate_label();
    program.preassign_label_to_next_insn(search_next);
    program.emit_insn(Insn::IdxRowId {
        cursor_id,
        dest: result_rowid,
    });
    program.emit_insn(Insn::Eq {
        lhs: rowid_reg,
        rhs: result_rowid,
        target_pc: matched,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Next {
        cursor_id,
        pc_if_next: search_next,
        fullscan: false,
        is_index: false,
    });
    program.emit_insn(Insn::Goto { target_pc: next });
    program.preassign_label_to_next_insn(matched);
    Ok(())
}

/// Where-clause indices already applied while building the hash table this
/// loop probes. Empty for loops that do not probe a hash table.
pub(super) fn hash_build_prefilter_where_terms(
    t_ctx: &TranslateCtx<'_>,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    subqueries: &[NonFromClauseSubquery],
    join_index: usize,
) -> Result<Vec<usize>> {
    let table = &table_references.joined_tables()[join_order[join_index].original_idx];
    let Operation::HashJoin(hash_join_op) = &table.op else {
        return Ok(Vec::new());
    };
    let uses_materialized_keys_and_payload = t_ctx
        .materialized_build_inputs
        .get(&hash_join_op.build_table_idx)
        .is_some_and(|input| !input.requires_build_table());
    super::hash::build_prefilter_where_terms(
        predicates,
        table_references,
        subqueries,
        hash_join_op,
        uses_materialized_keys_and_payload,
    )
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
    from_outer_join: bool,
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
        from_outer_join: bool,
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
            from_outer_join,
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
            self.from_outer_join,
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
            self.from_outer_join,
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
            self.from_outer_join,
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
