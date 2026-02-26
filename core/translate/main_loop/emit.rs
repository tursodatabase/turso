use super::*;

/// Chooses where each row produced by the main loop should go.
enum LoopEmitTarget {
    GroupBy,
    OrderBySorter,
    AggStep,
    Window,
    QueryResult,
}

/// Picks the loop output target from the query shape.
fn select_loop_emit_target(plan: &SelectPlan) -> LoopEmitTarget {
    let has_group_by_exprs = plan
        .group_by
        .as_ref()
        .is_some_and(|gb| !gb.exprs.is_empty());
    if has_group_by_exprs {
        return LoopEmitTarget::GroupBy;
    }

    if !plan.aggregates.is_empty() {
        return LoopEmitTarget::AggStep;
    }

    if plan.window.is_some() {
        return LoopEmitTarget::Window;
    }

    if !plan.order_by.is_empty() {
        return LoopEmitTarget::OrderBySorter;
    }

    LoopEmitTarget::QueryResult
}

/// Emits the bytecode for the inner loop of a query.
/// At this point the cursors for all tables have been opened and rewound.
pub fn emit_loop<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    // Resolve the innermost anti-join's label_body to the body start.
    // Non-innermost anti-joins have their label_body resolved in open_loop
    // (pointing to the next anti-join's open_loop code, not the body).
    // We use preassign_label_to_next_insn rather than resolve_label because
    // the first body instruction may be a constant (e.g. Integer for count(1))
    // that gets relocated to the init section by emit_constant_insns().
    // preassign_label_to_next_insn anchors to the last emitted non-constant
    // instruction, so after reordering the label correctly resolves to the
    // first non-constant body instruction.
    if let Some(last_join) = plan.join_order.last() {
        let last_idx = last_join.original_idx;
        let is_anti = plan.table_references.joined_tables()[last_idx]
            .join_info
            .as_ref()
            .is_some_and(|ji| ji.is_anti());
        if is_anti {
            if let Some(sa_meta) = t_ctx.meta_semi_anti_joins[last_idx].as_ref() {
                program.preassign_label_to_next_insn(sa_meta.label_body);
            }
        }
    }

    let emit_target = select_loop_emit_target(plan);
    emit_loop_source(program, t_ctx, plan, emit_target)
}

/// Emits the label used when a DISTINCT hash table finds a duplicate.
fn emit_distinct_conflict_label(program: &mut ProgramBuilder, distinctness: &Distinctness) {
    if let Distinctness::Distinct { ctx } = distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
    }
}

/// Emits one loop row into GROUP BY state (sorter-based or direct).
fn emit_group_by_loop_source<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    // This function either:
    // - creates a sorter for GROUP BY operations by allocating registers and translating expressions for three types of columns:
    // 1) GROUP BY columns (used as sorting keys)
    // 2) non-aggregate, non-GROUP BY columns
    // 3) aggregate function arguments
    // - or if the rows produced by the loop are already sorted in the order required by the GROUP BY keys,
    // the group by comparisons are done directly inside the main loop.
    let aggregates = &plan.aggregates;

    let GroupByMetadata {
        row_source,
        registers,
        ..
    } = t_ctx.meta_group_by.as_ref().unwrap();

    let start_reg = registers.reg_group_by_source_cols_start;
    let mut cur_reg = start_reg;

    // Collect all non-aggregate expressions in the following order:
    // 1. GROUP BY expressions. These serve as sort keys.
    // 2. Remaining non-aggregate expressions that are not in GROUP BY.
    //
    // Example:
    //   SELECT col1, col2, SUM(col3) FROM table GROUP BY col1
    //   - col1 is added first (from GROUP BY)
    //   - col2 is added second (non-aggregate, in SELECT, not in GROUP BY)
    for (expr, _) in t_ctx.non_aggregate_expressions.iter() {
        let key_reg = cur_reg;
        cur_reg += 1;
        translate_expr(
            program,
            Some(&plan.table_references),
            expr,
            key_reg,
            &t_ctx.resolver,
        )?;
    }

    // Step 2: Process arguments for all aggregate functions
    // For a query like: SELECT group_col, SUM(val1), AVG(val2) FROM table GROUP BY group_col
    // we'll process val1 and val2 here, storing them in the sorter so they're available
    // when computing the aggregates after sorting by group_col
    for agg in aggregates.iter() {
        for expr in agg.args.iter() {
            let agg_reg = cur_reg;
            cur_reg += 1;
            translate_expr(
                program,
                Some(&plan.table_references),
                expr,
                agg_reg,
                &t_ctx.resolver,
            )?;
        }
    }

    match row_source {
        GroupByRowSource::Sorter {
            sort_cursor,
            sorter_column_count,
            reg_sorter_key,
            ..
        } => {
            sorter_insert(
                program,
                start_reg,
                *sorter_column_count,
                *sort_cursor,
                *reg_sorter_key,
            );
        }
        GroupByRowSource::MainLoop { .. } => group_by_agg_phase(program, t_ctx, plan)?,
    }

    Ok(())
}

/// Emits one loop row into the ORDER BY sorter.
fn emit_order_by_sorter_loop_source(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'_>,
    plan: &SelectPlan,
) -> Result<()> {
    order_by_sorter_insert(program, t_ctx, plan)?;
    emit_distinct_conflict_label(program, &plan.distinctness);
    Ok(())
}

/// Emits per-row aggregation work for aggregate-only plans.
fn emit_agg_step_loop_source<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    let start_reg = t_ctx
        .reg_agg_start
        .expect("aggregate registers must be initialized");

    // In planner.rs, we have collected all aggregates from the SELECT clause, including ones where the aggregate is embedded inside
    // a more complex expression. Some examples: length(sum(x)), sum(x) + avg(y), sum(x) + 1, etc.
    // The result of those more complex expressions depends on the final result of the aggregate, so we don't translate the complete expressions here.
    // Instead, we accumulate the intermediate results of all aggreagates, and evaluate any expressions that do not contain aggregates.
    for (i, agg) in plan.aggregates.iter().enumerate() {
        let reg = start_reg + i;
        translate_aggregation_step(
            program,
            &plan.table_references,
            AggArgumentSource::new_from_expression(&agg.func, &agg.args, &agg.distinctness),
            reg,
            &t_ctx.resolver,
        )?;
        if let Distinctness::Distinct { ctx } = &agg.distinctness {
            let ctx = ctx
                .as_ref()
                .expect("distinct aggregate context not populated");
            program.preassign_label_to_next_insn(ctx.label_on_conflict);
        }
    }

    let label_emit_nonagg_only_once = if let Some(flag) = t_ctx.reg_nonagg_emit_once_flag {
        let if_label = program.allocate_label();
        program.emit_insn(Insn::If {
            reg: flag,
            target_pc: if_label,
            jump_if_null: false,
        });
        Some(if_label)
    } else {
        None
    };

    let col_start = t_ctx.reg_result_cols_start.unwrap();

    // Process only non-aggregate columns
    let non_agg_columns = plan
        .result_columns
        .iter()
        .enumerate()
        .filter(|(_, rc)| !rc.contains_aggregates);

    for (i, rc) in non_agg_columns {
        let reg = col_start + i;

        // Must use no_constant_opt to prevent constant hoisting: in compound
        // selects (UNION ALL), all branches share the same result registers,
        // so hoisted constants from the last branch overwrite earlier branches.
        translate_expr_no_constant_opt(
            program,
            Some(&plan.table_references),
            &rc.expr,
            reg,
            &t_ctx.resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }

    // For result columns that contain aggregates but also reference
    // non-aggregate columns (e.g. CASE WHEN SUM(1) THEN a ELSE b END),
    // pre-read those column references while the cursor is still valid.
    // They are cached in expr_to_reg_cache so that when the full
    // expression is evaluated after AggFinal, translate_expr finds
    // the cached values instead of reading from the exhausted cursor.
    for rc in plan
        .result_columns
        .iter()
        .filter(|rc| rc.contains_aggregates)
    {
        walk_expr(&rc.expr, &mut |expr: &'a Expr| -> Result<WalkControl> {
            match expr {
                Expr::Column { .. } | Expr::RowId { .. } => {
                    let reg = program.alloc_register();
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        expr,
                        reg,
                        &t_ctx.resolver,
                    )?;
                    t_ctx
                        .resolver
                        .expr_to_reg_cache
                        .push((Cow::Borrowed(expr), reg, false));
                    Ok(WalkControl::SkipChildren)
                }
                _ => {
                    if plan.aggregates.iter().any(|a| a.original_expr == *expr) {
                        return Ok(WalkControl::SkipChildren);
                    }
                    Ok(WalkControl::Continue)
                }
            }
        })?;
    }

    if let Some(label) = label_emit_nonagg_only_once {
        program.resolve_label(label, program.offset());
        let flag = t_ctx.reg_nonagg_emit_once_flag.unwrap();
        program.emit_int(1, flag);
    }

    Ok(())
}

/// Emits one row straight to the query result destination.
fn emit_query_result_loop_source<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    turso_assert!(
        plan.aggregates.is_empty(),
        "QueryResult target should not have aggregates"
    );
    let offset_jump_to = t_ctx
        .labels_main_loop
        .first()
        .map(|l| l.next)
        .or(t_ctx.label_main_loop_end);
    emit_select_result(
        program,
        &t_ctx.resolver,
        plan,
        t_ctx.label_main_loop_end,
        offset_jump_to,
        t_ctx.reg_nonagg_emit_once_flag,
        t_ctx.reg_offset,
        t_ctx.reg_result_cols_start.unwrap(),
        t_ctx.limit_ctx,
    )?;
    emit_distinct_conflict_label(program, &plan.distinctness);
    Ok(())
}

/// This is a helper function for inner_loop_emit,
/// which does a different thing depending on the emit target.
/// See the InnerLoopEmitTarget enum for more details.
fn emit_loop_source<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
    emit_target: LoopEmitTarget,
) -> Result<()> {
    match emit_target {
        LoopEmitTarget::GroupBy => emit_group_by_loop_source(program, t_ctx, plan),
        LoopEmitTarget::OrderBySorter => emit_order_by_sorter_loop_source(program, t_ctx, plan),
        LoopEmitTarget::AggStep => emit_agg_step_loop_source(program, t_ctx, plan),
        LoopEmitTarget::QueryResult => emit_query_result_loop_source(program, t_ctx, plan),
        LoopEmitTarget::Window => emit_window_loop_source(program, t_ctx, plan),
    }
}
