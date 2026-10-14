use turso_parser::ast;

use crate::{
    function::{AccumulatorFunc, AggFunc, Func},
    schema::Table,
    sync::Arc,
    translate::collate::CollationSeq,
    vdbe::{
        builder::ProgramBuilder,
        insn::{AggStepData, CmpInsFlags, HashDistinctData, Insn},
    },
    LimboError, Result,
};

/// Emit the runtime range check shared by legacy and HIR percentile lowering.
/// NULL is accepted here and propagated by aggregate finalization.
pub(crate) fn emit_percentile_fraction_range_check(program: &mut ProgramBuilder, fraction: usize) {
    // NULL skips the range check and propagates to a NULL result in finalize.
    // Use one scratch register for both bounds: success falls through to `done`
    // via `Le` on the upper bound; failure on either bound jumps to `bad: Halt`.
    let done = program.allocate_label();
    let bad = program.allocate_label();
    let bound = program.alloc_register();
    program.emit_insn(Insn::IsNull {
        reg: fraction,
        target_pc: done,
    });
    program.emit_insn(Insn::Real {
        value: 0.0,
        dest: bound,
    });
    program.emit_insn(Insn::Lt {
        lhs: fraction,
        rhs: bound,
        target_pc: bad,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Real {
        value: 1.0,
        dest: bound,
    });
    program.emit_insn(Insn::Le {
        lhs: fraction,
        rhs: bound,
        target_pc: done,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.preassign_label_to_next_insn(bad);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: "percentile value is not between 0 and 1".to_string(),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(done);
}

use super::{
    emitter::{OperationMode, Resolver, TranslateCtx},
    expr::{
        resolve_expr, translate_condition_expr, translate_expr, translate_expr_no_constant_opt,
        ConditionMetadata, NoConstantOptReason,
    },
    plan::{
        Aggregate, Distinctness, NonFromClauseSubquery, SelectPlan, SubqueryEvalPhase,
        TableReferences,
    },
    result_row::emit_select_result,
    semantic::hir::{self, HirDocument},
    subquery::emit_non_from_clause_subqueries_for_phase,
};

/// Emits the bytecode for processing an aggregate without a GROUP BY clause.
/// This is called when the main query execution loop has finished processing,
/// and we can now materialize the aggregate results.
pub fn emit_ungrouped_aggregation<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
    output_subqueries: &mut [NonFromClauseSubquery],
) -> Result<()> {
    let agg_start_reg = t_ctx.reg_agg_start.unwrap();

    for (i, agg) in plan.aggregates.iter().enumerate() {
        let agg_result_reg = agg_start_reg + i;
        program.emit_insn(Insn::AggFinal {
            register: agg_result_reg,
            func: AccumulatorFunc::Agg(agg.func.clone()),
        });
    }
    // we now have the agg results in (agg_start_reg..agg_start_reg + aggregates.len() - 1)
    // we need to call translate_expr on each result column, but replace the expr with a register copy in case any part of the
    // result column expression matches a) a group by column or b) an aggregation result.
    for (i, agg) in plan.aggregates.iter().enumerate() {
        t_ctx.resolver.cache_expr_reg(
            std::borrow::Cow::Borrowed(&agg.original_expr),
            agg_start_reg + i,
            false,
            None,
        );
    }
    t_ctx.resolver.enable_expr_to_reg_cache();

    // Subqueries that read an aggregate this query computes need the
    // aggregate's finalized register, so they must be emitted now — after
    // AggFinal and the cache population above, and before the result row that
    // reads them.
    emit_non_from_clause_subqueries_for_phase(
        program,
        &t_ctx.resolver,
        output_subqueries,
        &plan.join_order,
        Some(&plan.table_references),
        SubqueryEvalPhase::UngroupedAggregateOutput,
        |_| true,
    )?;

    // Allocate a label for the end (used by both HAVING and OFFSET to skip row emission)
    let end_label = program.allocate_label();

    // Handle HAVING clause without GROUP BY for ungrouped aggregation
    if let Some(group_by) = &plan.group_by {
        if group_by.exprs.is_empty() {
            if let Some(having) = &group_by.having {
                for expr in having.iter() {
                    let if_true_target = program.allocate_label();
                    translate_condition_expr(
                        program,
                        &plan.table_references,
                        expr,
                        ConditionMetadata {
                            jump_if_condition_is_true: false,
                            jump_target_when_false: end_label,
                            jump_target_when_true: if_true_target,
                            // treat null result as false
                            jump_target_when_null: end_label,
                        },
                        &t_ctx.resolver,
                    )?;
                    program.preassign_label_to_next_insn(if_true_target);
                }
            }
        }
    }

    // Handle OFFSET for ungrouped aggregates
    // Since we only have one result row, either skip it (offset > 0) or emit it
    if let Some(offset_reg) = t_ctx.reg_offset {
        // If offset > 0, jump to end (skip the single row)
        program.emit_insn(Insn::IfPos {
            reg: offset_reg,
            target_pc: end_label,
            decrement_by: 0,
        });
    }

    // If the loop never ran (once-flag is still 0), we need to evaluate non-aggregate columns now.
    // This ensures literals return their values and column references return NULL (since no
    // rows matched). The once-flag mechanism normally evaluates non-agg columns on first
    // iteration, but if there were no iterations, we must do it here.
    //
    // We must emit NullRow for all table cursors first, because after a WHERE-filter
    // jump-out the cursor may still be positioned on a valid (but non-matching) row.
    // Without NullRow, Column instructions would read stale data from that row instead
    // of returning NULL.
    if let Some(once_flag) = t_ctx.reg_nonagg_emit_once_flag {
        let skip_nonagg_eval = program.allocate_label();
        // If once-flag is non-zero (loop ran at least once), skip evaluation
        program.emit_insn(Insn::If {
            reg: once_flag,
            target_pc: skip_nonagg_eval,
            jump_if_null: false,
        });
        // Set all table cursors to NullRow so that Column instructions return NULL
        // instead of leaking stale values from the last scanned (but non-matching) row.
        // Also null out coroutine output registers for CTEs/subqueries.
        for table_ref in plan.table_references.joined_tables() {
            let (table_cursor_id, index_cursor_id) =
                table_ref.resolve_cursors(program, OperationMode::SELECT)?;
            for cursor_id in [table_cursor_id, index_cursor_id].into_iter().flatten() {
                program.emit_insn(Insn::NullRow { cursor_id });
            }
            if let Table::FromClauseSubquery(subquery) = &table_ref.table {
                if let Some(start_reg) = subquery.result_columns_start_reg {
                    let num_cols = subquery.columns.len();
                    if num_cols > 0 {
                        program.emit_insn(Insn::Null {
                            dest: start_reg,
                            dest_end: if num_cols > 1 {
                                Some(start_reg + num_cols - 1)
                            } else {
                                None
                            },
                        });
                    }
                }
            }
        }
        // Evaluate non-aggregate columns now (with cursor in invalid state, columns return NULL)
        // Must use no_constant_opt to prevent constant hoisting which would place the label
        // after the hoisted constants, causing infinite loops in compound selects.
        let col_start = t_ctx.reg_result_cols_start.unwrap();
        for (i, rc) in plan.result_columns.iter().enumerate() {
            if !rc.contains_aggregates {
                translate_expr_no_constant_opt(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    col_start + i,
                    &t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
            }
        }
        program.preassign_label_to_next_insn(skip_nonagg_eval);
    }

    // Emit the result row (if we didn't skip it due to HAVING or OFFSET)
    emit_select_result(
        program,
        &t_ctx.resolver,
        plan,
        None,
        None,
        t_ctx.reg_nonagg_emit_once_flag,
        None, // we've already handled offset
        t_ctx.reg_result_cols_start.unwrap(),
        t_ctx.limit_ctx,
    )?;

    // Resolve the SELECT DISTINCT label if present
    // When a duplicate is found by the Found instruction, jump here to skip emitting the row
    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
    }

    program.preassign_label_to_next_insn(end_label);

    Ok(())
}

/// Resolves the collation a comparison-based aggregate uses for its argument
/// (explicit COLLATE clause, then the column's table-defined collation, then
/// BINARY). The result is stored on the AggStep instruction itself.
pub(crate) fn agg_arg_collation(
    referenced_tables: &TableReferences,
    expr: &ast::Expr,
    resolver: &Resolver,
) -> CollationSeq {
    // Check if this is a column expression with explicit COLLATE clause
    if let ast::Expr::Collate(_, collation_name) = expr {
        if let Ok(collation) = resolver.resolve_collation(collation_name.as_str()) {
            return collation;
        }
        return CollationSeq::Binary;
    }

    // If no explicit collation, check if this is a column with table-defined collation
    if let ast::Expr::Column { table, column, .. } = expr {
        if let Some((_, table_ref)) = referenced_tables.find_table_by_internal_id(*table) {
            if let Some(table_column) = table_ref.get_column_at(*column) {
                if let Some(c) = table_column.collation_opt() {
                    return c;
                }
            }
        }
    }

    CollationSeq::Binary
}

/// Emits the bytecode for handling duplicates in a distinct aggregate.
/// This is used in both GROUP BY and non-GROUP BY aggregations to jump over
/// the AggStep that would otherwise accumulate the same value multiple times.
pub fn handle_distinct(
    program: &mut ProgramBuilder,
    distinctness: &Distinctness,
    agg_arg_reg: usize,
) {
    let Distinctness::Distinct { ctx } = distinctness else {
        return;
    };
    let distinct_ctx = ctx
        .as_ref()
        .expect("distinct aggregate context not populated");
    let num_regs = 1;
    program.emit_insn(Insn::HashDistinct {
        data: Box::new(HashDistinctData {
            hash_table_id: distinct_ctx.hash_table_id,
            key_start_reg: agg_arg_reg,
            num_keys: num_regs,
            collations: distinct_ctx.collations.clone(),
            target_pc: distinct_ctx.label_on_conflict,
        }),
    });
}

/// Source of aggregate function arguments during bytecode emission.
///
/// * `Register`: arguments were pre-computed into contiguous registers
///   (used for GROUP BY without a sorter, where the main loop is already sorted).
/// * `Expression`: arguments are evaluated on-the-fly from the original AST
///   (used for ungrouped aggregates, window functions, and for the GROUP BY sorter
///   path where leaf columns are cached in `expr_to_reg_cache` before evaluation).
pub enum AggArgumentSource<'a> {
    Register {
        src_reg_start: usize,
        aggregate: &'a Aggregate,
    },
    Expression {
        func: &'a AggFunc,
        args: &'a Vec<ast::Expr>,
        distinctness: &'a Distinctness,
    },
}

pub(crate) struct HirAggregateDistinct {
    pub(crate) hash_table: usize,
    pub(crate) collation: CollationSeq,
    pub(crate) on_conflict: crate::vdbe::BranchOffset,
}

pub(crate) struct PreparedAggregateArguments<'a> {
    pub(crate) function: &'a AggFunc,
    pub(crate) registers_start: usize,
    pub(crate) count: usize,
    pub(crate) collations: &'a [CollationSeq],
    pub(crate) comparators: &'a [Option<crate::vdbe::insn::SortComparatorType>],
    pub(crate) custom_types_enabled: bool,
}

enum AggregationArguments<'a, 'resolver> {
    Legacy {
        source: AggArgumentSource<'a>,
        referenced_tables: &'a TableReferences,
        resolver: &'a Resolver<'resolver>,
    },
    Hir {
        document: &'a HirDocument,
        call: &'a hir::FunctionCall,
        func: AggFunc,
        distinct: Option<&'a HirAggregateDistinct>,
    },
    Prepared(PreparedAggregateArguments<'a>),
}

impl AggregationArguments<'_, '_> {
    fn func(&self) -> &AggFunc {
        match self {
            Self::Legacy { source, .. } => source.agg_func(),
            Self::Hir { func, .. } => func,
            Self::Prepared(arguments) => arguments.function,
        }
    }

    fn num_args(&self) -> usize {
        match self {
            Self::Legacy { source, .. } => source.num_args(),
            Self::Hir { call, .. } => match &call.arguments {
                hir::FunctionArguments::Star => 0,
                hir::FunctionArguments::Expressions { values, .. } => values.len(),
                hir::FunctionArguments::OrderedSet { direct, .. } => direct.len() + 1,
            },
            Self::Prepared(arguments) => arguments.count,
        }
    }

    fn hir_argument(call: &hir::FunctionCall, index: usize) -> Option<&hir::Expr> {
        match &call.arguments {
            hir::FunctionArguments::Star => None,
            hir::FunctionArguments::Expressions { values, .. } => values.get(index),
            hir::FunctionArguments::OrderedSet { direct, order_by } => match index {
                0 => Some(&order_by.expr),
                _ => direct.get(index - 1),
            },
        }
    }

    fn translate(&self, program: &mut ProgramBuilder, index: usize) -> Result<usize> {
        match self {
            Self::Legacy {
                source,
                referenced_tables,
                resolver,
            } => source.translate(program, referenced_tables, resolver, index),
            Self::Hir { document, call, .. } => {
                let expression = Self::hir_argument(call, index).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "HIR aggregate argument {index} is outside its function arguments"
                    ))
                })?;
                let register = program.alloc_register();
                super::semantic_lowering::expr::translate_expr(
                    program, document, expression, register,
                )?;
                Ok(register)
            }
            Self::Prepared(arguments) => {
                if index >= arguments.count {
                    return Err(LimboError::InternalError(format!(
                        "prepared aggregate argument {index} is outside {} arguments",
                        arguments.count
                    )));
                }
                Ok(arguments.registers_start + index)
            }
        }
    }

    fn handle_distinct(&self, program: &mut ProgramBuilder, argument: usize) {
        match self {
            Self::Legacy { source, .. } => {
                handle_distinct(program, source.distinctness(), argument)
            }
            Self::Hir {
                distinct: Some(distinct),
                ..
            } => program.emit_insn(Insn::HashDistinct {
                data: Box::new(HashDistinctData {
                    hash_table_id: distinct.hash_table,
                    key_start_reg: argument,
                    num_keys: 1,
                    collations: vec![distinct.collation],
                    target_pc: distinct.on_conflict,
                }),
            }),
            Self::Hir { distinct: None, .. } => {}
            Self::Prepared(_) => {}
        }
    }

    fn collation(&self, index: usize) -> CollationSeq {
        match self {
            Self::Legacy {
                source,
                referenced_tables,
                resolver,
            } => agg_arg_collation(referenced_tables, source.arg_at(index), resolver),
            Self::Hir { call, .. } => match &call.arguments {
                hir::FunctionArguments::Expressions { facts, .. } => facts
                    .get(index)
                    .and_then(|facts| facts.collation.as_ref())
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary),
                hir::FunctionArguments::OrderedSet { order_by, .. } if index == 0 => order_by
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary),
                _ => CollationSeq::Binary,
            },
            Self::Prepared(arguments) => arguments
                .collations
                .get(index)
                .copied()
                .unwrap_or(CollationSeq::Binary),
        }
    }

    fn comparator(&self, index: usize) -> Option<crate::vdbe::insn::SortComparatorType> {
        match self {
            Self::Legacy {
                source,
                referenced_tables,
                resolver,
            } => super::order_by::custom_type_comparator(
                source.arg_at(index),
                referenced_tables,
                resolver.schema(),
            ),
            Self::Hir { call, .. } => match &call.arguments {
                hir::FunctionArguments::Expressions { facts, .. } => {
                    facts.get(index).and_then(|facts| {
                        super::order_by::custom_type_comparator_from_type_fact(&facts.type_fact)
                    })
                }
                hir::FunctionArguments::OrderedSet { order_by, .. } if index == 0 => {
                    super::order_by::custom_type_comparator_from_type_fact(&order_by.type_fact)
                }
                _ => None,
            },
            Self::Prepared(arguments) => arguments.comparators.get(index).copied().flatten(),
        }
    }

    fn translate_integer_one(&self, program: &mut ProgramBuilder) -> Result<usize> {
        match self {
            Self::Legacy {
                referenced_tables,
                resolver,
                ..
            } => {
                let expression = ast::Expr::Literal(ast::Literal::Numeric("1".to_string()));
                translate_const_arg(program, referenced_tables, resolver, &expression)
            }
            Self::Hir { .. } => {
                let register = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: register,
                });
                Ok(register)
            }
            Self::Prepared(_) => {
                let register = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: register,
                });
                Ok(register)
            }
        }
    }

    fn translate_comma(&self, program: &mut ProgramBuilder) -> Result<usize> {
        match self {
            Self::Legacy {
                referenced_tables,
                resolver,
                ..
            } => {
                let expression = ast::Expr::Literal(ast::Literal::String("\",\"".to_string()));
                translate_const_arg(program, referenced_tables, resolver, &expression)
            }
            Self::Hir { .. } => {
                let register = program.alloc_register();
                program.emit_insn(Insn::String8 {
                    value: ",".to_string(),
                    dest: register,
                });
                Ok(register)
            }
            Self::Prepared(_) => {
                let register = program.alloc_register();
                program.emit_insn(Insn::String8 {
                    value: ",".to_string(),
                    dest: register,
                });
                Ok(register)
            }
        }
    }

    fn require_custom_types(&self) -> Result<()> {
        match self {
            Self::Legacy { resolver, .. } => resolver.require_custom_types("Array features"),
            Self::Hir { .. } => Ok(()),
            Self::Prepared(arguments) => {
                if !arguments.custom_types_enabled {
                    crate::bail_parse_error!(
                        "Array features require --experimental-custom-types flag"
                    );
                }
                Ok(())
            }
        }
    }
}

impl<'a> AggArgumentSource<'a> {
    pub fn new_from_registers(src_reg_start: usize, aggregate: &'a Aggregate) -> Self {
        Self::Register {
            src_reg_start,
            aggregate,
        }
    }

    pub fn new_from_expression(
        func: &'a AggFunc,
        args: &'a Vec<ast::Expr>,
        distinctness: &'a Distinctness,
    ) -> Self {
        Self::Expression {
            func,
            args,
            distinctness,
        }
    }

    pub fn distinctness(&self) -> &Distinctness {
        match self {
            AggArgumentSource::Register { aggregate, .. } => &aggregate.distinctness,
            AggArgumentSource::Expression { distinctness, .. } => distinctness,
        }
    }

    pub fn agg_func(&self) -> &AggFunc {
        match self {
            AggArgumentSource::Register { aggregate, .. } => &aggregate.func,
            AggArgumentSource::Expression { func, .. } => func,
        }
    }

    pub fn arg_at(&self, idx: usize) -> &ast::Expr {
        match self {
            AggArgumentSource::Register { aggregate, .. } => &aggregate.args[idx],
            AggArgumentSource::Expression { args, .. } => &args[idx],
        }
    }

    pub fn num_args(&self) -> usize {
        match self {
            AggArgumentSource::Register { aggregate, .. } => aggregate.args.len(),
            AggArgumentSource::Expression { args, .. } => args.len(),
        }
    }

    /// Emit bytecode to read an aggregate function argument into a register.
    pub fn translate(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: &TableReferences,
        resolver: &Resolver,
        arg_idx: usize,
    ) -> Result<usize> {
        match self {
            AggArgumentSource::Register {
                src_reg_start: start_reg,
                ..
            } => Ok(*start_reg + arg_idx),
            AggArgumentSource::Expression { args, .. } => {
                resolve_expr(program, Some(referenced_tables), &args[arg_idx], resolver)
            }
        }
    }
}

/// Emits the bytecode for processing an aggregate step.
///
/// This is distinct from the final step, which is called after a single group has been entirely accumulated,
/// and the actual result value of the aggregation is materialized.
///
/// Ungrouped aggregation is a special case of grouped aggregation that involves a single group.
///
/// Examples:
/// * In `SELECT SUM(price) FROM t`, `price` is evaluated for each row and added to the accumulator.
/// * In `SELECT product_category, SUM(price) FROM t GROUP BY product_category`, `price` is evaluated for
///   each row in the group and added to that group’s accumulator.
pub fn translate_aggregation_step(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    agg_arg_source: AggArgumentSource,
    target_register: usize,
    resolver: &Resolver,
    // For `percentile_cont` / `percentile_disc`: register pre-evaluated by
    // `InitLoop::emit`. `None` for any other aggregate.
    fraction_reg: Option<usize>,
) -> Result<usize> {
    emit_aggregation_step(
        program,
        AggregationArguments::Legacy {
            source: agg_arg_source,
            referenced_tables,
            resolver,
        },
        target_register,
        fraction_reg,
    )
}

pub(crate) fn translate_hir_aggregation_step(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    call: &hir::FunctionCall,
    target_register: usize,
    distinct: Option<&HirAggregateDistinct>,
    fraction_reg: Option<usize>,
) -> Result<usize> {
    if !call.arguments.order_terms().is_empty()
        && !matches!(call.arguments, hir::FunctionArguments::OrderedSet { .. })
    {
        return Err(LimboError::InternalError(
            "HIR aggregate argument ORDER BY is not lowered yet".to_string(),
        ));
    }
    let func = hir_aggregate_function(call)?;
    emit_aggregation_step(
        program,
        AggregationArguments::Hir {
            document,
            call,
            func,
            distinct,
        },
        target_register,
        fraction_reg,
    )
}

pub(crate) fn translate_prepared_aggregation_step(
    program: &mut ProgramBuilder,
    arguments: PreparedAggregateArguments<'_>,
    target_register: usize,
) -> Result<usize> {
    emit_aggregation_step(
        program,
        AggregationArguments::Prepared(arguments),
        target_register,
        None,
    )
}

pub(crate) fn hir_aggregate_function(call: &hir::FunctionCall) -> Result<AggFunc> {
    Ok(match call.function.value() {
        Func::Agg(func) => func.clone(),
        Func::External(external) if external.func.is_aggregate() => {
            AggFunc::External(external.func.clone().into())
        }
        function => {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate identity belongs to non-aggregate function {function}"
            )))
        }
    })
}

fn emit_aggregation_step(
    program: &mut ProgramBuilder,
    agg_arg_source: AggregationArguments<'_, '_>,
    target_register: usize,
    fraction_reg: Option<usize>,
) -> Result<usize> {
    let num_args = agg_arg_source.num_args();
    let func = agg_arg_source.func();
    let dest = match func {
        AggFunc::Avg => {
            if num_args != 1 {
                crate::bail_parse_error!("avg bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Avg),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::Count0 => {
            let expr_reg = agg_arg_source.translate_integer_one(program)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Count0),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::Count => {
            if num_args != 1 {
                crate::bail_parse_error!("count bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Count),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::GroupConcat => {
            if num_args != 1 && num_args != 2 {
                crate::bail_parse_error!("group_concat bad number of arguments");
            }

            let delimiter_reg = if num_args == 2 {
                agg_arg_source.translate(program, 1)?
            } else {
                agg_arg_source.translate_comma(program)?
            };

            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);

            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: delimiter_reg,
                    func: AccumulatorFunc::Agg(AggFunc::GroupConcat),
                    comparator: None,
                    collation: None,
                }),
            });

            target_register
        }
        AggFunc::Max => {
            if num_args != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            let arg_collation = agg_arg_source.collation(0);
            let comparator = agg_arg_source.comparator(0);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Max),
                    comparator,
                    collation: Some(arg_collation),
                }),
            });
            target_register
        }
        AggFunc::Min => {
            if num_args != 1 {
                crate::bail_parse_error!("min bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            let arg_collation = agg_arg_source.collation(0);
            let comparator = agg_arg_source.comparator(0);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Min),
                    comparator,
                    collation: Some(arg_collation),
                }),
            });
            target_register
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            if num_args != 2 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            let value_reg = agg_arg_source.translate(program, 1)?;

            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: value_reg,
                    func: AccumulatorFunc::Agg(AggFunc::JsonGroupObject),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            if num_args != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::JsonGroupArray),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::StringAgg => {
            if num_args != 2 {
                crate::bail_parse_error!("string_agg bad number of arguments");
            }

            let expr_reg = agg_arg_source.translate(program, 0)?;
            let delimiter_reg = agg_arg_source.translate(program, 1)?;

            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: delimiter_reg,
                    func: AccumulatorFunc::Agg(AggFunc::StringAgg),
                    comparator: None,
                    collation: None,
                }),
            });

            target_register
        }
        AggFunc::Sum => {
            if num_args != 1 {
                crate::bail_parse_error!("sum bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Sum),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::Total => {
            if num_args != 1 {
                crate::bail_parse_error!("total bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Total),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::ArrayAgg => {
            agg_arg_source.require_custom_types()?;
            if num_args != 1 {
                crate::bail_parse_error!("array_agg bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::ArrayAgg),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::Mode => {
            // Planner rewrites `mode() WITHIN GROUP (ORDER BY x)` to a single arg `[x]`.
            if num_args != 1 {
                crate::bail_parse_error!("mode bad number of arguments");
            }
            let value_reg = agg_arg_source.translate(program, 0)?;
            // Activate the value's collation so finalize can sort text correctly.
            let arg_collation = agg_arg_source.collation(0);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: value_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Mode),
                    comparator: None,
                    collation: Some(arg_collation),
                }),
            });
            target_register
        }
        AggFunc::PercentileCont | AggFunc::PercentileDisc => {
            // Planner rewrites `percentile_*(fraction) WITHIN GROUP (ORDER BY x)` to
            // args `[x, fraction]`: the value goes in `col`, the fraction in `delimiter`.
            // The fraction is evaluated and range-checked once before the row loop
            // in `InitLoop::emit` — including the input-column / subquery rejection.
            if num_args != 2 {
                crate::bail_parse_error!("percentile bad number of arguments");
            }
            let value_reg = agg_arg_source.translate(program, 0)?;
            let fraction_reg =
                fraction_reg.expect("percentile fraction register must be set by InitLoop::emit");
            let arg_collation = agg_arg_source.collation(0);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: value_reg,
                    delimiter: fraction_reg,
                    func: AccumulatorFunc::Agg(func.clone()),
                    comparator: None,
                    collation: Some(arg_collation),
                }),
            });
            target_register
        }
        AggFunc::External(ref func) => {
            let registered_argc = func.agg_args().map_err(|_| {
                LimboError::ExtensionError(
                    "External aggregate function called with wrong number of arguments".to_string(),
                )
            })?;
            if registered_argc >= 0 && registered_argc as usize != num_args {
                crate::bail_parse_error!(
                    "External aggregate function called with wrong number of arguments"
                );
            }
            let argc = num_args;
            let expr_reg = if argc == 0 {
                0
            } else {
                agg_arg_source.translate(program, 0)?
            };
            for i in 0..argc {
                if i != 0 {
                    let _ = agg_arg_source.translate(program, i)?;
                }
                // invariant: distinct aggregates are only supported for single-argument functions
                if argc == 1 {
                    agg_arg_source.handle_distinct(program, expr_reg + i);
                }
            }
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::External(if registered_argc < 0 {
                        Arc::new(func.with_aggregate_arg_count(num_args))
                    } else {
                        func.clone()
                    })),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
    };
    // Aggregate arguments can carry column or explicit COLLATE metadata for the
    // aggregate's internal comparator, but that state must not leak to the
    // surrounding expression that consumes the aggregate result.
    program.reset_collation();
    Ok(dest)
}

fn translate_const_arg(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    resolver: &Resolver,
    expr: &ast::Expr,
) -> Result<usize> {
    let target_register = program.alloc_register();
    translate_expr(
        program,
        Some(referenced_tables),
        expr,
        target_register,
        resolver,
    )
}
