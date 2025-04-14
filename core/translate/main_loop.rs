use crate::{
    schema::Table,
    translate::result_row::emit_select_result,
    types::SeekOp,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
        BranchOffset,
    },
    Result,
};

use super::{
    aggregation::translate_aggregation_step,
    emitter::{OperationMode, TranslateCtx},
    expr::{translate_condition_expr, translate_expr, ConditionMetadata},
    group_by::is_column_in_group_by,
    optimizer::Optimizable,
    order_by::{order_by_sorter_insert, sorter_insert},
    plan::{
        IterationDirection, Operation, Search, SeekDef, SelectPlan, SelectQueryType,
        TableReference, WhereTerm,
    },
};

// Metadata for handling LEFT JOIN operations
#[derive(Debug)]
pub struct LeftJoinMetadata {
    // integer register that holds a flag that is set to true if the current row has a match for the left join
    pub reg_match_flag: usize,
    // label for the instruction that sets the match flag to true
    pub label_match_flag_set_true: BranchOffset,
    // label for the instruction that checks if the match flag is true
    pub label_match_flag_check_value: BranchOffset,
}

/// Jump labels for each loop in the query's main execution loop
#[derive(Debug, Clone, Copy)]
pub struct LoopLabels {
    /// jump to the start of the loop body
    pub loop_start: BranchOffset,
    /// jump to the Next instruction (or equivalent)
    pub next: BranchOffset,
    /// jump to the end of the loop, exiting it
    pub loop_end: BranchOffset,
}

impl LoopLabels {
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            loop_start: program.allocate_label(),
            next: program.allocate_label(),
            loop_end: program.allocate_label(),
        }
    }
}

/// Initialize resources needed for the source operators (tables, joins, etc)
pub fn init_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
    mode: OperationMode,
) -> Result<()> {
    assert!(
        t_ctx.meta_left_joins.len() == tables.len(),
        "meta_left_joins length does not match tables length"
    );
    for (table_index, table) in tables.iter().enumerate() {
        // Initialize bookkeeping for OUTER JOIN
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_metadata = LeftJoinMetadata {
                    reg_match_flag: program.alloc_register(),
                    label_match_flag_set_true: program.allocate_label(),
                    label_match_flag_check_value: program.allocate_label(),
                };
                t_ctx.meta_left_joins[table_index] = Some(lj_metadata);
            }
        }
        match &table.op {
            Operation::Scan { index, .. } => {
                let cursor_id = program.alloc_cursor_id(
                    Some(table.identifier.clone()),
                    match &table.table {
                        Table::BTree(_) => CursorType::BTreeTable(table.btree().unwrap().clone()),
                        Table::Virtual(_) => {
                            CursorType::VirtualTable(table.virtual_table().unwrap().clone())
                        }
                        other => panic!("Invalid table reference type in Scan: {:?}", other),
                    },
                );
                let index_cursor_id = index.as_ref().map(|i| {
                    program.alloc_cursor_id(Some(i.name.clone()), CursorType::BTreeIndex(i.clone()))
                });
                match (mode, &table.table) {
                    (OperationMode::SELECT, Table::BTree(btree)) => {
                        let root_page = btree.root_page;
                        program.emit_insn(Insn::OpenRead {
                            cursor_id,
                            root_page,
                        });
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn(Insn::OpenRead {
                                cursor_id: index_cursor_id,
                                root_page: index.as_ref().unwrap().root_page,
                            });
                        }
                    }
                    (OperationMode::DELETE, Table::BTree(btree)) => {
                        let root_page = btree.root_page;
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id,
                            root_page: root_page.into(),
                        });
                    }
                    (OperationMode::UPDATE, Table::BTree(btree)) => {
                        let root_page = btree.root_page;
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id,
                            root_page: root_page.into(),
                        });
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn(Insn::OpenWrite {
                                cursor_id: index_cursor_id,
                                root_page: index.as_ref().unwrap().root_page.into(),
                            });
                        }
                    }
                    (_, Table::Virtual(_)) => {
                        program.emit_insn(Insn::VOpen { cursor_id });
                    }
                    _ => {
                        unimplemented!()
                    }
                }
            }
            Operation::Search(search) => {
                let table_cursor_id = program.alloc_cursor_id(
                    Some(table.identifier.clone()),
                    CursorType::BTreeTable(table.btree().unwrap().clone()),
                );

                match mode {
                    OperationMode::SELECT => {
                        program.emit_insn(Insn::OpenRead {
                            cursor_id: table_cursor_id,
                            root_page: table.table.get_root_page(),
                        });
                    }
                    OperationMode::DELETE | OperationMode::UPDATE => {
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id: table_cursor_id,
                            root_page: table.table.get_root_page().into(),
                        });
                    }
                    _ => {
                        unimplemented!()
                    }
                }

                if let Search::Seek {
                    index: Some(index), ..
                } = search
                {
                    let index_cursor_id = program.alloc_cursor_id(
                        Some(index.name.clone()),
                        CursorType::BTreeIndex(index.clone()),
                    );

                    match mode {
                        OperationMode::SELECT => {
                            program.emit_insn(Insn::OpenRead {
                                cursor_id: index_cursor_id,
                                root_page: index.root_page,
                            });
                        }
                        OperationMode::UPDATE | OperationMode::DELETE => {
                            program.emit_insn(Insn::OpenWrite {
                                cursor_id: index_cursor_id,
                                root_page: index.root_page.into(),
                            });
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Set up the main query execution loop
/// For example in the case of a nested table scan, this means emitting the Rewind instruction
/// for all tables involved, outermost first.
pub fn open_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
    predicates: &[WhereTerm],
) -> Result<()> {
    for (table_index, table) in tables.iter().enumerate() {
        let LoopLabels {
            loop_start,
            loop_end,
            next,
        } = *t_ctx
            .labels_main_loop
            .get(table_index)
            .expect("table has no loop labels");

        // Each OUTER JOIN has a "match flag" that is initially set to false,
        // and is set to true when a match is found for the OUTER JOIN.
        // This is used to determine whether to emit actual columns or NULLs for the columns of the right table.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }

        match &table.op {
            Operation::Subquery { plan, .. } => {
                let (yield_reg, coroutine_implementation_start) = match &plan.query_type {
                    SelectQueryType::Subquery {
                        yield_reg,
                        coroutine_implementation_start,
                    } => (*yield_reg, *coroutine_implementation_start),
                    _ => unreachable!("Subquery operator with non-subquery query type"),
                };
                // In case the subquery is an inner loop, it needs to be reinitialized on each iteration of the outer loop.
                program.emit_insn(Insn::InitCoroutine {
                    yield_reg,
                    jump_on_definition: BranchOffset::Offset(0),
                    start_offset: coroutine_implementation_start,
                });
                program.resolve_label(loop_start, program.offset());
                // A subquery within the main loop of a parent query has no cursor, so instead of advancing the cursor,
                // it emits a Yield which jumps back to the main loop of the subquery itself to retrieve the next row.
                // When the subquery coroutine completes, this instruction jumps to the label at the top of the termination_label_stack,
                // which in this case is the end of the Yield-Goto loop in the parent query.
                program.emit_insn(Insn::Yield {
                    yield_reg,
                    end_offset: loop_end,
                });

                // These are predicates evaluated outside of the subquery,
                // so they are translated here.
                // E.g. SELECT foo FROM (SELECT bar as foo FROM t1) sub WHERE sub.foo > 10
                for cond in predicates
                    .iter()
                    .filter(|cond| cond.should_eval_at_loop(table_index))
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
            Operation::Scan { iter_dir, index } => {
                let cursor_id = program.resolve_cursor_id(&table.identifier);
                let index_cursor_id = index.as_ref().map(|i| program.resolve_cursor_id(&i.name));
                let iteration_cursor_id = index_cursor_id.unwrap_or(cursor_id);
                if !matches!(&table.table, Table::Virtual(_)) {
                    if *iter_dir == IterationDirection::Backwards {
                        program.emit_insn(Insn::Last {
                            cursor_id: iteration_cursor_id,
                            pc_if_empty: loop_end,
                        });
                    } else {
                        program.emit_insn(Insn::Rewind {
                            cursor_id: iteration_cursor_id,
                            pc_if_empty: loop_end,
                        });
                    }
                }
                if let Table::Virtual(ref table) = table.table {
                    let start_reg =
                        program.alloc_registers(table.args.as_ref().map(|a| a.len()).unwrap_or(0));
                    let mut cur_reg = start_reg;
                    let args = match table.args.as_ref() {
                        Some(args) => args,
                        None => &vec![],
                    };
                    for arg in args {
                        let reg = cur_reg;
                        cur_reg += 1;
                        let _ = translate_expr(program, Some(tables), arg, reg, &t_ctx.resolver)?;
                    }
                    program.emit_insn(Insn::VFilter {
                        cursor_id,
                        pc_if_empty: loop_end,
                        arg_count: table.args.as_ref().map_or(0, |args| args.len()),
                        args_reg: start_reg,
                    });
                }
                program.resolve_label(loop_start, program.offset());

                if let Some(index_cursor_id) = index_cursor_id {
                    program.emit_insn(Insn::DeferredSeek {
                        index_cursor_id,
                        table_cursor_id: cursor_id,
                    });
                }

                for cond in predicates
                    .iter()
                    .filter(|cond| cond.should_eval_at_loop(table_index))
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
            Operation::Search(search) => {
                let table_cursor_id = program.resolve_cursor_id(&table.identifier);
                // Open the loop for the index search.
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
                if let Search::RowidEq { cmp_expr } = search {
                    let src_reg = program.alloc_register();
                    translate_expr(
                        program,
                        Some(tables),
                        &cmp_expr.expr,
                        src_reg,
                        &t_ctx.resolver,
                    )?;
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: table_cursor_id,
                        src_reg,
                        target_pc: next,
                    });
                } else {
                    // Otherwise, it's an index/rowid scan, i.e. first a seek is performed and then a scan until the comparison expression is not satisfied anymore.
                    let index_cursor_id = if let Search::Seek {
                        index: Some(index), ..
                    } = search
                    {
                        Some(program.resolve_cursor_id(&index.name))
                    } else {
                        None
                    };
                    let is_index = index_cursor_id.is_some();
                    let seek_cursor_id = index_cursor_id.unwrap_or(table_cursor_id);
                    let Search::Seek { seek_def, .. } = search else {
                        unreachable!("Rowid equality point lookup should have been handled above");
                    };

                    let start_reg = program.alloc_registers(seek_def.key.len());
                    emit_seek(
                        program,
                        tables,
                        seek_def,
                        t_ctx,
                        seek_cursor_id,
                        start_reg,
                        loop_end,
                        is_index,
                    )?;
                    emit_seek_termination(
                        program,
                        tables,
                        seek_def,
                        t_ctx,
                        seek_cursor_id,
                        start_reg,
                        loop_start,
                        loop_end,
                        is_index,
                    )?;

                    if let Some(index_cursor_id) = index_cursor_id {
                        // Don't do a btree table seek until it's actually necessary to read from the table.
                        program.emit_insn(Insn::DeferredSeek {
                            index_cursor_id,
                            table_cursor_id,
                        });
                    }
                }

                for cond in predicates
                    .iter()
                    .filter(|cond| cond.should_eval_at_loop(table_index))
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
        }

        // Set the match flag to true if this is a LEFT JOIN.
        // At this point of execution we are going to emit columns for the left table,
        // and either emit columns or NULLs for the right table, depending on whether the null_flag is set
        // for the right table's cursor.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                program.resolve_label(lj_meta.label_match_flag_set_true, program.offset());
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }
    }

    Ok(())
}

/// SQLite (and so Limbo) processes joins as a nested loop.
/// The loop may emit rows to various destinations depending on the query:
/// - a GROUP BY sorter (grouping is done by sorting based on the GROUP BY keys and aggregating while the GROUP BY keys match)
/// - an ORDER BY sorter (when there is no GROUP BY, but there is an ORDER BY)
/// - an AggStep (the columns are collected for aggregation, which is finished later)
/// - a QueryResult (there is none of the above, so the loop either emits a ResultRow, or if it's a subquery, yields to the parent query)
enum LoopEmitTarget {
    GroupBySorter,
    OrderBySorter,
    AggStep,
    QueryResult,
}

/// Emits the bytecode for the inner loop of a query.
/// At this point the cursors for all tables have been opened and rewound.
pub fn emit_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &mut SelectPlan,
) -> Result<()> {
    // if we have a group by, we emit a record into the group by sorter.
    if plan.group_by.is_some() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::GroupBySorter);
    }
    // if we DONT have a group by, but we have aggregates, we emit without ResultRow.
    // we also do not need to sort because we are emitting a single row.
    if !plan.aggregates.is_empty() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::AggStep);
    }
    // if we DONT have a group by, but we have an order by, we emit a record into the order by sorter.
    if plan.order_by.is_some() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::OrderBySorter);
    }
    // if we have neither, we emit a ResultRow. In that case, if we have a Limit, we handle that with DecrJumpZero.
    emit_loop_source(program, t_ctx, plan, LoopEmitTarget::QueryResult)
}

/// This is a helper function for inner_loop_emit,
/// which does a different thing depending on the emit target.
/// See the InnerLoopEmitTarget enum for more details.
fn emit_loop_source(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    emit_target: LoopEmitTarget,
) -> Result<()> {
    match emit_target {
        LoopEmitTarget::GroupBySorter => {
            // This function creates a sorter for GROUP BY operations by allocating registers and
            // translating expressions for three types of columns:
            // 1) GROUP BY columns (used as sorting keys)
            // 2) non-aggregate, non-GROUP BY columns
            // 3) aggregate function arguments
            let group_by = plan.group_by.as_ref().unwrap();
            let aggregates = &plan.aggregates;

            // Identify columns in the result set that are neither in GROUP BY nor contain aggregates
            let non_group_by_non_agg_expr = plan
                .result_columns
                .iter()
                .filter(|rc| {
                    !rc.contains_aggregates && !is_column_in_group_by(&rc.expr, &group_by.exprs)
                })
                .map(|rc| &rc.expr);
            let non_agg_count = non_group_by_non_agg_expr.clone().count();
            // Store the count of non-GROUP BY, non-aggregate columns in the metadata
            // This will be used later during aggregation processing
            t_ctx.meta_group_by.as_mut().map(|meta| {
                meta.non_group_by_non_agg_column_count = Some(non_agg_count);
                meta
            });

            // Calculate the total number of arguments used across all aggregate functions
            let aggregate_arguments_count = plan
                .aggregates
                .iter()
                .map(|agg| agg.args.len())
                .sum::<usize>();

            // Calculate total number of registers needed for all columns in the sorter
            let column_count = group_by.exprs.len() + aggregate_arguments_count + non_agg_count;

            // Allocate a contiguous block of registers for all columns
            let start_reg = program.alloc_registers(column_count);
            let mut cur_reg = start_reg;

            // Step 1: Process GROUP BY columns first
            // These will be the first columns in the sorter and serve as sort keys
            for expr in group_by.exprs.iter() {
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

            // Step 2: Process columns that aren't part of GROUP BY and don't contain aggregates
            // Example: SELECT col1, col2, SUM(col3) FROM table GROUP BY col1
            // Here col2 would be processed in this loop if it's in the result set
            for expr in non_group_by_non_agg_expr {
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

            // Step 3: Process arguments for all aggregate functions
            // For each aggregate, translate all its argument expressions
            for agg in aggregates.iter() {
                // For a query like: SELECT group_col, SUM(val1), AVG(val2) FROM table GROUP BY group_col
                // we'll process val1 and val2 here, storing them in the sorter so they're available
                // when computing the aggregates after sorting by group_col
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

            let group_by_metadata = t_ctx.meta_group_by.as_ref().unwrap();

            sorter_insert(
                program,
                start_reg,
                column_count,
                group_by_metadata.sort_cursor,
                group_by_metadata.reg_sorter_key,
            );

            Ok(())
        }
        LoopEmitTarget::OrderBySorter => order_by_sorter_insert(program, t_ctx, plan),
        LoopEmitTarget::AggStep => {
            let num_aggs = plan.aggregates.len();
            let start_reg = program.alloc_registers(num_aggs);
            t_ctx.reg_agg_start = Some(start_reg);

            // In planner.rs, we have collected all aggregates from the SELECT clause, including ones where the aggregate is embedded inside
            // a more complex expression. Some examples: length(sum(x)), sum(x) + avg(y), sum(x) + 1, etc.
            // The result of those more complex expressions depends on the final result of the aggregate, so we don't translate the complete expressions here.
            // Instead, we accumulate the intermediate results of all aggreagates, and evaluate any expressions that do not contain aggregates.
            for (i, agg) in plan.aggregates.iter().enumerate() {
                let reg = start_reg + i;
                translate_aggregation_step(
                    program,
                    &plan.table_references,
                    agg,
                    reg,
                    &t_ctx.resolver,
                )?;
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

                translate_expr(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    reg,
                    &t_ctx.resolver,
                )?;
            }
            if let Some(label) = label_emit_nonagg_only_once {
                program.resolve_label(label, program.offset());
                let flag = t_ctx.reg_nonagg_emit_once_flag.unwrap();
                program.emit_int(1, flag);
            }

            Ok(())
        }
        LoopEmitTarget::QueryResult => {
            assert!(
                plan.aggregates.is_empty(),
                "We should not get here with aggregates"
            );
            let offset_jump_to = t_ctx
                .labels_main_loop
                .first()
                .map(|l| l.next)
                .or(t_ctx.label_main_loop_end);
            emit_select_result(
                program,
                t_ctx,
                plan,
                t_ctx.label_main_loop_end,
                offset_jump_to,
            )?;

            Ok(())
        }
    }
}

/// Closes the loop for a given source operator.
/// For example in the case of a nested table scan, this means emitting the Next instruction
/// for all tables involved, innermost first.
pub fn close_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
) -> Result<()> {
    // We close the loops for all tables in reverse order, i.e. innermost first.
    // OPEN t1
    //   OPEN t2
    //     OPEN t3
    //       <do stuff>
    //     CLOSE t3
    //   CLOSE t2
    // CLOSE t1
    for (idx, table) in tables.iter().rev().enumerate() {
        let table_index = tables.len() - idx - 1;
        let loop_labels = *t_ctx
            .labels_main_loop
            .get(table_index)
            .expect("source has no loop labels");

        match &table.op {
            Operation::Subquery { .. } => {
                program.resolve_label(loop_labels.next, program.offset());
                // A subquery has no cursor to call Next on, so it just emits a Goto
                // to the Yield instruction, which in turn jumps back to the main loop of the subquery,
                // so that the next row from the subquery can be read.
                program.emit_insn(Insn::Goto {
                    target_pc: loop_labels.loop_start,
                });
            }
            Operation::Scan {
                index, iter_dir, ..
            } => {
                program.resolve_label(loop_labels.next, program.offset());

                let cursor_id = program.resolve_cursor_id(&table.identifier);
                let index_cursor_id = index.as_ref().map(|i| program.resolve_cursor_id(&i.name));
                let iteration_cursor_id = index_cursor_id.unwrap_or(cursor_id);
                match &table.table {
                    Table::BTree(_) => {
                        if *iter_dir == IterationDirection::Backwards {
                            program.emit_insn(Insn::Prev {
                                cursor_id: iteration_cursor_id,
                                pc_if_prev: loop_labels.loop_start,
                            });
                        } else {
                            program.emit_insn(Insn::Next {
                                cursor_id: iteration_cursor_id,
                                pc_if_next: loop_labels.loop_start,
                            });
                        }
                    }
                    Table::Virtual(_) => {
                        program.emit_insn(Insn::VNext {
                            cursor_id,
                            pc_if_next: loop_labels.loop_start,
                        });
                    }
                    other => unreachable!("Unsupported table reference type: {:?}", other),
                }
            }
            Operation::Search(search) => {
                program.resolve_label(loop_labels.next, program.offset());
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a Next instruction.
                if !matches!(search, Search::RowidEq { .. }) {
                    let (cursor_id, iter_dir) = match search {
                        Search::Seek {
                            index: Some(index),
                            seek_def,
                            ..
                        } => (program.resolve_cursor_id(&index.name), seek_def.iter_dir),
                        Search::Seek {
                            index: None,
                            seek_def,
                            ..
                        } => (
                            program.resolve_cursor_id(&table.identifier),
                            seek_def.iter_dir,
                        ),
                        Search::RowidEq { .. } => unreachable!(),
                    };

                    if iter_dir == IterationDirection::Backwards {
                        program.emit_insn(Insn::Prev {
                            cursor_id,
                            pc_if_prev: loop_labels.loop_start,
                        });
                    } else {
                        program.emit_insn(Insn::Next {
                            cursor_id,
                            pc_if_next: loop_labels.loop_start,
                        });
                    }
                }
            }
        }

        program.resolve_label(loop_labels.loop_end, program.offset());

        // Handle OUTER JOIN logic. The reason this comes after the "loop end" mark is that we may need to still jump back
        // and emit a row with NULLs for the right table, and then jump back to the next row of the left table.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                // The left join match flag is set to 1 when there is any match on the right table
                // (e.g. SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a).
                // If the left join match flag has been set to 1, we jump to the next row on the outer table,
                // i.e. continue to the next row of t1 in our example.
                program.resolve_label(lj_meta.label_match_flag_check_value, program.offset());
                let jump_offset = program.offset().add(3u32);
                program.emit_insn(Insn::IfPos {
                    reg: lj_meta.reg_match_flag,
                    target_pc: jump_offset,
                    decrement_by: 0,
                });
                // If the left join match flag is still 0, it means there was no match on the right table,
                // but since it's a LEFT JOIN, we still need to emit a row with NULLs for the right table.
                // In that case, we now enter the routine that does exactly that.
                // First we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL
                let right_cursor_id = match &table.op {
                    Operation::Scan { .. } => program.resolve_cursor_id(&table.identifier),
                    Operation::Search { .. } => program.resolve_cursor_id(&table.identifier),
                    _ => unreachable!(),
                };
                program.emit_insn(Insn::NullRow {
                    cursor_id: right_cursor_id,
                });
                // Then we jump to setting the left join match flag to 1 again,
                // but this time the right table cursor will set everything to null.
                // This leads to emitting a row with cols from the left + nulls from the right,
                // and we will end up back in the IfPos instruction above, which will then
                // check the match flag again, and since it is now 1, we will jump to the
                // next row in the left table.
                program.emit_insn(Insn::Goto {
                    target_pc: lj_meta.label_match_flag_set_true,
                });

                assert_eq!(program.offset(), jump_offset);
            }
        }
    }
    Ok(())
}

/// Emits instructions for an index seek. See e.g. [crate::translate::plan::SeekDef]
/// for more details about the seek definition.
///
/// Index seeks always position the cursor to the first row that matches the seek key,
/// and then continue to emit rows until the termination condition is reached,
/// see [emit_seek_termination] below.
///
/// If either 1. the seek finds no rows or 2. the termination condition is reached,
/// the loop for that given table/index is fully exited.
#[allow(clippy::too_many_arguments)]
fn emit_seek(
    program: &mut ProgramBuilder,
    tables: &[TableReference],
    seek_def: &SeekDef,
    t_ctx: &mut TranslateCtx,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_end: BranchOffset,
    is_index: bool,
) -> Result<()> {
    let Some(seek) = seek_def.seek.as_ref() else {
        assert!(seek_def.iter_dir == IterationDirection::Backwards, "A SeekDef without a seek operation should only be used in backwards iteration direction");
        program.emit_insn(Insn::Last {
            cursor_id: seek_cursor_id,
            pc_if_empty: loop_end,
        });
        return Ok(());
    };
    // We allocated registers for the full index key, but our seek key might not use the full index key.
    // Later on for the termination condition we will overwrite the NULL registers.
    // See [crate::translate::optimizer::build_seek_def] for more details about in which cases we do and don't use the full index key.
    for i in 0..seek_def.key.len() {
        let reg = start_reg + i;
        if i >= seek.len {
            if seek_def.null_pad_unset_cols() {
                program.emit_insn(Insn::Null {
                    dest: reg,
                    dest_end: None,
                });
            }
        } else {
            let expr = &seek_def.key[i];
            translate_expr(program, Some(tables), &expr, reg, &t_ctx.resolver)?;
            // If the seek key column is not verifiably non-NULL, we need check whether it is NULL,
            // and if so, jump to the loop end.
            // This is to avoid returning rows for e.g. SELECT * FROM t WHERE t.x > NULL,
            // which would erroneously return all rows from t, as NULL is lower than any non-NULL value in index key comparisons.
            if !expr.is_nonnull() {
                program.emit_insn(Insn::IsNull {
                    reg,
                    target_pc: loop_end,
                });
            }
        }
    }
    let num_regs = if seek_def.null_pad_unset_cols() {
        seek_def.key.len()
    } else {
        seek.len
    };
    match seek.op {
        SeekOp::GE => program.emit_insn(Insn::SeekGE {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        SeekOp::GT => program.emit_insn(Insn::SeekGT {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        SeekOp::LE => program.emit_insn(Insn::SeekLE {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        SeekOp::LT => program.emit_insn(Insn::SeekLT {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        SeekOp::EQ => panic!("An index seek is never EQ"),
    };

    Ok(())
}

/// Emits instructions for an index seek termination. See e.g. [crate::translate::plan::SeekDef]
/// for more details about the seek definition.
///
/// Index seeks always position the cursor to the first row that matches the seek key
/// (see [emit_seek] above), and then continue to emit rows until the termination condition
/// (if any) is reached.
///
/// If the termination condition is not present, the cursor is fully scanned to the end.
#[allow(clippy::too_many_arguments)]
fn emit_seek_termination(
    program: &mut ProgramBuilder,
    tables: &[TableReference],
    seek_def: &SeekDef,
    t_ctx: &mut TranslateCtx,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    is_index: bool,
) -> Result<()> {
    let Some(termination) = seek_def.termination.as_ref() else {
        program.resolve_label(loop_start, program.offset());
        return Ok(());
    };
    let num_regs = termination.len;
    // If the seek termination was preceded by a seek (which happens in most cases),
    // we can re-use the registers that were allocated for the full index key.
    let start_idx = seek_def.seek.as_ref().map_or(0, |seek| seek.len);
    for i in start_idx..termination.len {
        let reg = start_reg + i;
        translate_expr(
            program,
            Some(tables),
            &seek_def.key[i],
            reg,
            &t_ctx.resolver,
        )?;
    }
    program.resolve_label(loop_start, program.offset());
    let mut rowid_reg = None;
    if !is_index {
        rowid_reg = Some(program.alloc_register());
        program.emit_insn(Insn::RowId {
            cursor_id: seek_cursor_id,
            dest: rowid_reg.unwrap(),
        });
    }

    match (is_index, termination.op) {
        (true, SeekOp::GE) => program.emit_insn(Insn::IdxGE {
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        (true, SeekOp::GT) => program.emit_insn(Insn::IdxGT {
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        (true, SeekOp::LE) => program.emit_insn(Insn::IdxLE {
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        (true, SeekOp::LT) => program.emit_insn(Insn::IdxLT {
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        (false, SeekOp::GE) => program.emit_insn(Insn::Ge {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default(),
        }),
        (false, SeekOp::GT) => program.emit_insn(Insn::Gt {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default(),
        }),
        (false, SeekOp::LE) => program.emit_insn(Insn::Le {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default(),
        }),
        (false, SeekOp::LT) => program.emit_insn(Insn::Lt {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default(),
        }),
        (_, SeekOp::EQ) => {
            panic!("An index termination condition is never EQ")
        }
    };

    Ok(())
}
