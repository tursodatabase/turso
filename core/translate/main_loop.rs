use turso_sqlite3_parser::ast::{self, SortOrder};

use std::sync::Arc;

use crate::{
    schema::{Affinity, Index, IndexColumn, Table},
    translate::{
        plan::{DistinctCtx, Distinctness},
        result_row::emit_select_result,
    },
    types::SeekOp,
    vdbe::{
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{CmpInsFlags, IdxInsertFlags, Insn},
        BranchOffset, CursorID,
    },
    Result,
};

use super::{
    aggregation::translate_aggregation_step,
    emitter::{OperationMode, TranslateCtx},
    expr::{
        translate_condition_expr, translate_expr, translate_expr_no_constant_opt,
        ConditionMetadata, NoConstantOptReason,
    },
    group_by::{group_by_agg_phase, GroupByMetadata, GroupByRowSource},
    optimizer::Optimizable,
    order_by::{order_by_sorter_insert, sorter_insert},
    plan::{
        convert_where_to_vtab_constraint, Aggregate, GroupBy, IterationDirection, JoinOrderMember,
        Operation, QueryDestination, Search, SeekDef, SelectPlan, TableReferences, WhereTerm,
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

pub fn init_distinct(program: &mut ProgramBuilder, plan: &SelectPlan) -> DistinctCtx {
    let index_name = format!("distinct_{}", program.offset().as_offset_int()); // we don't really care about the name that much, just enough that we don't get name collisions
    let index = Arc::new(Index {
        name: index_name.clone(),
        table_name: String::new(),
        ephemeral: true,
        root_page: 0,
        columns: plan
            .result_columns
            .iter()
            .enumerate()
            .map(|(i, col)| IndexColumn {
                name: col.expr.to_string(),
                order: SortOrder::Asc,
                pos_in_table: i,
                collation: None, // FIXME: this should be determined based on the result column expression!
                default: None, // FIXME: this should be determined based on the result column expression!
            })
            .collect(),
        unique: false,
        has_rowid: false,
    });
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    let ctx = DistinctCtx {
        cursor_id,
        ephemeral_index_name: index_name,
        label_on_conflict: program.allocate_label(),
    };

    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });

    ctx
}

/// Initialize resources needed for the source operators (tables, joins, etc)
pub fn init_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &TableReferences,
    aggregates: &mut [Aggregate],
    group_by: Option<&GroupBy>,
    mode: OperationMode,
    where_clause: &[WhereTerm],
) -> Result<()> {
    assert!(
        t_ctx.meta_left_joins.len() == tables.joined_tables().len(),
        "meta_left_joins length does not match tables length"
    );

    let cdc_table = program.capture_data_changes_mode().table();
    if cdc_table.is_some()
        && matches!(
            mode,
            OperationMode::INSERT | OperationMode::UPDATE | OperationMode::DELETE
        )
    {
        assert!(tables.joined_tables().len() == 1);
        let cdc_table_name = cdc_table.unwrap();
        if tables.joined_tables()[0].table.get_name() != cdc_table_name {
            let Some(cdc_table) = t_ctx.resolver.schema.get_table(cdc_table_name) else {
                crate::bail_parse_error!("no such table: {}", cdc_table_name);
            };
            let Some(cdc_btree) = cdc_table.btree().clone() else {
                crate::bail_parse_error!("no such table: {}", cdc_table_name);
            };
            let cdc_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(cdc_btree.clone()));
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cdc_cursor_id,
                root_page: cdc_btree.root_page.into(),
                name: cdc_btree.name.clone(),
            });
            t_ctx.cdc_cursor_id = Some(cdc_cursor_id);
        }
    }

    // Initialize ephemeral indexes for distinct aggregates
    for (i, agg) in aggregates
        .iter_mut()
        .enumerate()
        .filter(|(_, agg)| agg.is_distinct())
    {
        assert!(
            agg.args.len() == 1,
            "DISTINCT aggregate functions must have exactly one argument"
        );
        let index_name = format!("distinct_agg_{}_{}", i, agg.args[0]);
        let index = Arc::new(Index {
            name: index_name.clone(),
            table_name: String::new(),
            ephemeral: true,
            root_page: 0,
            columns: vec![IndexColumn {
                name: agg.args[0].to_string(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                collation: None, // FIXME: this should be inferred from the expression
                default: None,   // FIXME: this should be inferred from the expression
            }],
            has_rowid: false,
            unique: false,
        });
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        if group_by.is_none() {
            // In GROUP BY, the ephemeral index is reinitialized for every group
            // in the clear accumulator subroutine, so we only do it here if there is no GROUP BY.
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id,
                is_table: false,
            });
        }
        agg.distinctness = Distinctness::Distinct {
            ctx: Some(DistinctCtx {
                cursor_id,
                ephemeral_index_name: index_name,
                label_on_conflict: program.allocate_label(),
            }),
        };
    }
    for (table_index, table) in tables.joined_tables().iter().enumerate() {
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
        let (table_cursor_id, index_cursor_id) = table.open_cursors(program, mode)?;
        match &table.op {
            Operation::Scan { index, .. } => match (mode, &table.table) {
                (OperationMode::SELECT, Table::BTree(btree)) => {
                    let root_page = btree.root_page;
                    if let Some(cursor_id) = table_cursor_id {
                        program.emit_insn(Insn::OpenRead {
                            cursor_id,
                            root_page,
                        });
                    }
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
                        cursor_id: table_cursor_id
                            .expect("table cursor is always opened in OperationMode::DELETE"),
                        root_page: root_page.into(),
                        name: btree.name.clone(),
                    });
                    if let Some(index_cursor_id) = index_cursor_id {
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id: index_cursor_id,
                            root_page: index.as_ref().unwrap().root_page.into(),
                            name: index.as_ref().unwrap().name.clone(),
                        });
                    }
                    // For delete, we need to open all the other indexes too for writing
                    if let Some(indexes) = t_ctx.resolver.schema.indexes.get(&btree.name) {
                        for index in indexes {
                            if table
                                .op
                                .index()
                                .is_some_and(|table_index| table_index.name == index.name)
                            {
                                continue;
                            }
                            let cursor_id = program.alloc_cursor_id_keyed(
                                CursorKey::index(table.internal_id, index.clone()),
                                CursorType::BTreeIndex(index.clone()),
                            );
                            program.emit_insn(Insn::OpenWrite {
                                cursor_id,
                                root_page: index.root_page.into(),
                                name: index.name.clone(),
                            });
                        }
                    }
                }
                (OperationMode::UPDATE, Table::BTree(btree)) => {
                    let root_page = btree.root_page;
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id: table_cursor_id
                            .expect("table cursor is always opened in OperationMode::UPDATE"),
                        root_page: root_page.into(),
                        name: btree.name.clone(),
                    });
                    if let Some(index_cursor_id) = index_cursor_id {
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id: index_cursor_id,
                            root_page: index.as_ref().unwrap().root_page.into(),
                            name: index.as_ref().unwrap().name.clone(),
                        });
                    }
                }
                (_, Table::Virtual(_)) => {
                    if let Some(cursor_id) = table_cursor_id {
                        program.emit_insn(Insn::VOpen { cursor_id });
                    }
                }
                _ => {}
            },
            Operation::Search(search) => {
                match mode {
                    OperationMode::SELECT => {
                        if let Some(table_cursor_id) = table_cursor_id {
                            program.emit_insn(Insn::OpenRead {
                                cursor_id: table_cursor_id,
                                root_page: table.table.get_root_page(),
                            });
                        }
                    }
                    OperationMode::DELETE | OperationMode::UPDATE => {
                        let table_cursor_id = table_cursor_id.expect(
                                        "table cursor is always opened in OperationMode::DELETE or OperationMode::UPDATE",
                                    );

                        program.emit_insn(Insn::OpenWrite {
                            cursor_id: table_cursor_id,
                            root_page: table.table.get_root_page().into(),
                            name: table.table.get_name().to_string(),
                        });

                        // For DELETE, we need to open all the indexes for writing
                        // UPDATE opens these in emit_program_for_update() separately
                        if mode == OperationMode::DELETE {
                            if let Some(indexes) =
                                t_ctx.resolver.schema.indexes.get(table.table.get_name())
                            {
                                for index in indexes {
                                    if table
                                        .op
                                        .index()
                                        .is_some_and(|table_index| table_index.name == index.name)
                                    {
                                        continue;
                                    }
                                    let cursor_id = program.alloc_cursor_id_keyed(
                                        CursorKey::index(table.internal_id, index.clone()),
                                        CursorType::BTreeIndex(index.clone()),
                                    );
                                    program.emit_insn(Insn::OpenWrite {
                                        cursor_id,
                                        root_page: index.root_page.into(),
                                        name: index.name.clone(),
                                    });
                                }
                            }
                        }
                    }
                    _ => {
                        unimplemented!()
                    }
                }

                if let Search::Seek {
                    index: Some(index), ..
                } = search
                {
                    // Ephemeral index cursor are opened ad-hoc when needed.
                    if !index.ephemeral {
                        match mode {
                            OperationMode::SELECT => {
                                program.emit_insn(Insn::OpenRead {
                                    cursor_id: index_cursor_id
                                        .expect("index cursor is always opened in Seek with index"),
                                    root_page: index.root_page,
                                });
                            }
                            OperationMode::UPDATE | OperationMode::DELETE => {
                                program.emit_insn(Insn::OpenWrite {
                                    cursor_id: index_cursor_id
                                        .expect("index cursor is always opened in Seek with index"),
                                    root_page: index.root_page.into(),
                                    name: index.name.clone(),
                                });
                            }
                            _ => {
                                unimplemented!()
                            }
                        }
                    }
                }
            }
        }
    }

    for cond in where_clause
        .iter()
        .filter(|c| c.should_eval_before_loop(&[JoinOrderMember::default()]))
    {
        let jump_target = program.allocate_label();
        let meta = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: jump_target,
            jump_target_when_false: t_ctx.label_main_loop_end.unwrap(),
        };
        translate_condition_expr(program, tables, &cond.expr, meta, &t_ctx.resolver)?;
        program.preassign_label_to_next_insn(jump_target);
    }

    Ok(())
}

/// Set up the main query execution loop
/// For example in the case of a nested table scan, this means emitting the Rewind instruction
/// for all tables involved, outermost first.
pub fn open_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    temp_cursor_id: Option<CursorID>,
) -> Result<()> {
    for (join_index, join) in join_order.iter().enumerate() {
        let joined_table_index = join.original_idx;
        let table = &table_references.joined_tables()[joined_table_index];
        let LoopLabels {
            loop_start,
            loop_end,
            next,
        } = *t_ctx
            .labels_main_loop
            .get(joined_table_index)
            .expect("table has no loop labels");

        // Each OUTER JOIN has a "match flag" that is initially set to false,
        // and is set to true when a match is found for the OUTER JOIN.
        // This is used to determine whether to emit actual columns or NULLs for the columns of the right table.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[joined_table_index].as_ref().unwrap();
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }

        let (table_cursor_id, index_cursor_id) = table.resolve_cursors(program)?;

        match &table.op {
            Operation::Scan { iter_dir, .. } => {
                match &table.table {
                    Table::BTree(_) => {
                        let iteration_cursor_id = temp_cursor_id.unwrap_or_else(|| {
                            index_cursor_id.unwrap_or_else(|| {
                                table_cursor_id.expect(
                                    "Either ephemeral or index or table cursor must be opened",
                                )
                            })
                        });
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
                        program.preassign_label_to_next_insn(loop_start);
                    }
                    Table::Virtual(vtab) => {
                        let (start_reg, count, maybe_idx_str, maybe_idx_int) = {
                            // Virtual‑table modules can receive constraints via xBestIndex.
                            // They return information with which to pass to VFilter operation.
                            // We forward every predicate that touches vtab columns.
                            //
                            // vtab.col = literal             (always usable)
                            // vtab.col = outer_table.col     (usable, because outer_table is already positioned)
                            // vtab.col = later_table.col     (forwarded with usable = false)
                            //
                            // xBestIndex decides which ones it wants by setting argvIndex and whether the
                            // core layer may omit them (omit = true).
                            // We then materialise the RHS/LHS into registers before issuing VFilter.
                            let converted_constraints = predicates
                                .iter()
                                .enumerate()
                                .filter(|(_, p)| p.should_eval_at_loop(join_index, join_order))
                                .filter_map(|(i, p)| {
                                    // Build ConstraintInfo from the predicates
                                    convert_where_to_vtab_constraint(
                                        p,
                                        joined_table_index,
                                        i,
                                        join_order,
                                    )
                                    .unwrap_or(None)
                                })
                                .collect::<Vec<_>>();
                            // TODO: get proper order_by information to pass to the vtab.
                            // maybe encode more info on t_ctx? we need: [col_idx, is_descending]
                            let index_info = vtab.best_index(&converted_constraints, &[]);

                            // Determine the number of VFilter arguments (constraints with an argv_index).
                            let args_needed = index_info
                                .constraint_usages
                                .iter()
                                .filter(|u| u.argv_index.is_some())
                                .count();
                            let start_reg = program.alloc_registers(args_needed);

                            // For each constraint used by best_index, translate the opposite side.
                            for (i, usage) in index_info.constraint_usages.iter().enumerate() {
                                if let Some(argv_index) = usage.argv_index {
                                    if let Some(cinfo) = converted_constraints.get(i) {
                                        let (pred_idx, is_rhs) = cinfo.unpack_plan_info();
                                        if let ast::Expr::Binary(lhs, _, rhs) =
                                            &predicates[pred_idx].expr
                                        {
                                            // translate the opposite side of the referenced vtab column
                                            let expr = if is_rhs { lhs } else { rhs };
                                            // argv_index is 1-based; adjust to get the proper register offset.
                                            if argv_index == 0 {
                                                // invalid since argv_index is 1-based
                                                continue;
                                            }
                                            let target_reg = start_reg + (argv_index - 1) as usize;
                                            translate_expr(
                                                program,
                                                Some(table_references),
                                                expr,
                                                target_reg,
                                                &t_ctx.resolver,
                                            )?;
                                            if cinfo.usable && usage.omit {
                                                predicates[pred_idx].consumed.set(true);
                                            }
                                        }
                                    }
                                }
                            }

                            // If best_index provided an idx_str, translate it.
                            let maybe_idx_str = if let Some(idx_str) = index_info.idx_str {
                                let reg = program.alloc_register();
                                program.emit_insn(Insn::String8 {
                                    dest: reg,
                                    value: idx_str,
                                });
                                Some(reg)
                            } else {
                                None
                            };
                            (
                                start_reg,
                                args_needed,
                                maybe_idx_str,
                                Some(index_info.idx_num),
                            )
                        };

                        // Emit VFilter with the computed arguments.
                        program.emit_insn(Insn::VFilter {
                            cursor_id: table_cursor_id
                                .expect("Virtual tables do not support covering indexes"),
                            arg_count: count,
                            args_reg: start_reg,
                            idx_str: maybe_idx_str,
                            idx_num: maybe_idx_int.unwrap_or(0) as usize,
                            pc_if_empty: loop_end,
                        });
                        program.preassign_label_to_next_insn(loop_start);
                    }
                    Table::FromClauseSubquery(from_clause_subquery) => {
                        let (yield_reg, coroutine_implementation_start) =
                            match &from_clause_subquery.plan.query_destination {
                                QueryDestination::CoroutineYield {
                                    yield_reg,
                                    coroutine_implementation_start,
                                } => (*yield_reg, *coroutine_implementation_start),
                                _ => unreachable!("Subquery table with non-subquery query type"),
                            };
                        // In case the subquery is an inner loop, it needs to be reinitialized on each iteration of the outer loop.
                        program.emit_insn(Insn::InitCoroutine {
                            yield_reg,
                            jump_on_definition: BranchOffset::Offset(0),
                            start_offset: coroutine_implementation_start,
                        });
                        program.preassign_label_to_next_insn(loop_start);
                        // A subquery within the main loop of a parent query has no cursor, so instead of advancing the cursor,
                        // it emits a Yield which jumps back to the main loop of the subquery itself to retrieve the next row.
                        // When the subquery coroutine completes, this instruction jumps to the label at the top of the termination_label_stack,
                        // which in this case is the end of the Yield-Goto loop in the parent query.
                        program.emit_insn(Insn::Yield {
                            yield_reg,
                            end_offset: loop_end,
                        });
                    }
                }

                if let Some(table_cursor_id) = table_cursor_id {
                    if let Some(index_cursor_id) = index_cursor_id {
                        program.emit_insn(Insn::DeferredSeek {
                            index_cursor_id,
                            table_cursor_id,
                        });
                    }
                }

                for cond in predicates
                    .iter()
                    .filter(|cond| cond.should_eval_at_loop(join_index, join_order))
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
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
            }
            Operation::Search(search) => {
                assert!(
                    !matches!(table.table, Table::FromClauseSubquery(_)),
                    "Subqueries do not support index seeks"
                );
                // Open the loop for the index search.
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
                if let Search::RowidEq { cmp_expr } = search {
                    let src_reg = program.alloc_register();
                    translate_expr(
                        program,
                        Some(table_references),
                        cmp_expr,
                        src_reg,
                        &t_ctx.resolver,
                    )?;
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: table_cursor_id
                            .expect("Search::RowidEq requires a table cursor"),
                        src_reg,
                        target_pc: next,
                    });
                } else {
                    // Otherwise, it's an index/rowid scan, i.e. first a seek is performed and then a scan until the comparison expression is not satisfied anymore.
                    if let Search::Seek {
                        index: Some(index), ..
                    } = search
                    {
                        if index.ephemeral {
                            let table_has_rowid = if let Table::BTree(btree) = &table.table {
                                btree.has_rowid
                            } else {
                                false
                            };
                            Some(emit_autoindex(
                                program,
                                index,
                                table_cursor_id
                                    .expect("an ephemeral index must have a source table cursor"),
                                index_cursor_id
                                    .expect("an ephemeral index must have an index cursor"),
                                table_has_rowid,
                            )?)
                        } else {
                            index_cursor_id
                        }
                    } else {
                        index_cursor_id
                    };

                    let is_index = index_cursor_id.is_some();
                    let seek_cursor_id = temp_cursor_id.unwrap_or_else(|| {
                        index_cursor_id.unwrap_or_else(|| {
                            table_cursor_id
                                .expect("Either ephemeral or index or table cursor must be opened")
                        })
                    });
                    let Search::Seek { seek_def, .. } = search else {
                        unreachable!("Rowid equality point lookup should have been handled above");
                    };

                    let start_reg = program.alloc_registers(seek_def.key.len());
                    emit_seek(
                        program,
                        table_references,
                        seek_def,
                        t_ctx,
                        seek_cursor_id,
                        start_reg,
                        loop_end,
                        is_index,
                    )?;
                    emit_seek_termination(
                        program,
                        table_references,
                        seek_def,
                        t_ctx,
                        seek_cursor_id,
                        start_reg,
                        loop_start,
                        loop_end,
                        is_index,
                    )?;

                    if let Some(index_cursor_id) = index_cursor_id {
                        if let Some(table_cursor_id) = table_cursor_id {
                            // Don't do a btree table seek until it's actually necessary to read from the table.
                            program.emit_insn(Insn::DeferredSeek {
                                index_cursor_id,
                                table_cursor_id,
                            });
                        }
                    }
                }

                for cond in predicates
                    .iter()
                    .filter(|cond| cond.should_eval_at_loop(join_index, join_order))
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
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
            }
        }

        // Set the match flag to true if this is a LEFT JOIN.
        // At this point of execution we are going to emit columns for the left table,
        // and either emit columns or NULLs for the right table, depending on whether the null_flag is set
        // for the right table's cursor.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[joined_table_index].as_ref().unwrap();
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
/// - a GROUP BY phase with no sorting (when the rows are already in the order required by the GROUP BY keys)
/// - an ORDER BY sorter (when there is no GROUP BY, but there is an ORDER BY)
/// - an AggStep (the columns are collected for aggregation, which is finished later)
/// - a QueryResult (there is none of the above, so the loop either emits a ResultRow, or if it's a subquery, yields to the parent query)
enum LoopEmitTarget {
    GroupBy,
    OrderBySorter,
    AggStep,
    QueryResult,
}

/// Emits the bytecode for the inner loop of a query.
/// At this point the cursors for all tables have been opened and rewound.
pub fn emit_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    // if we have a group by, we emit a record into the group by sorter,
    // or if the rows are already sorted, we do the group by aggregation phase directly.
    if plan.group_by.is_some() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::GroupBy);
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
        LoopEmitTarget::GroupBy => {
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
        LoopEmitTarget::OrderBySorter => {
            order_by_sorter_insert(
                program,
                &t_ctx.resolver,
                t_ctx
                    .meta_sort
                    .as_ref()
                    .expect("sort metadata must exist for ORDER BY"),
                &mut t_ctx.result_column_indexes_in_orderby_sorter,
                plan,
            )?;

            if let Distinctness::Distinct { ctx } = &plan.distinctness {
                let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
                program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
            }

            Ok(())
        }
        LoopEmitTarget::AggStep => {
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
                    agg,
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
                &t_ctx.resolver,
                plan,
                t_ctx.label_main_loop_end,
                offset_jump_to,
                t_ctx.reg_nonagg_emit_once_flag,
                t_ctx.reg_offset,
                t_ctx.reg_result_cols_start.unwrap(),
                t_ctx.limit_ctx,
            )?;

            if let Distinctness::Distinct { ctx } = &plan.distinctness {
                let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
                program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
            }

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
    tables: &TableReferences,
    join_order: &[JoinOrderMember],
    temp_cursor_id: Option<CursorID>,
) -> Result<()> {
    // We close the loops for all tables in reverse order, i.e. innermost first.
    // OPEN t1
    //   OPEN t2
    //     OPEN t3
    //       <do stuff>
    //     CLOSE t3
    //   CLOSE t2
    // CLOSE t1
    for join in join_order.iter().rev() {
        let table_index = join.original_idx;
        let table = &tables.joined_tables()[table_index];
        let loop_labels = *t_ctx
            .labels_main_loop
            .get(table_index)
            .expect("source has no loop labels");

        let (table_cursor_id, index_cursor_id) = table.resolve_cursors(program)?;

        match &table.op {
            Operation::Scan { iter_dir, .. } => {
                program.resolve_label(loop_labels.next, program.offset());
                match &table.table {
                    Table::BTree(_) => {
                        let iteration_cursor_id = temp_cursor_id.unwrap_or_else(|| {
                            index_cursor_id.unwrap_or_else(|| {
                                table_cursor_id.expect(
                                    "Either ephemeral or index or table cursor must be opened",
                                )
                            })
                        });
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
                            cursor_id: table_cursor_id
                                .expect("Virtual tables do not support covering indexes"),
                            pc_if_next: loop_labels.loop_start,
                        });
                    }
                    Table::FromClauseSubquery(_) => {
                        // A subquery has no cursor to call Next on, so it just emits a Goto
                        // to the Yield instruction, which in turn jumps back to the main loop of the subquery,
                        // so that the next row from the subquery can be read.
                        program.emit_insn(Insn::Goto {
                            target_pc: loop_labels.loop_start,
                        });
                    }
                }
                program.preassign_label_to_next_insn(loop_labels.loop_end);
            }
            Operation::Search(search) => {
                assert!(
                    !matches!(table.table, Table::FromClauseSubquery(_)),
                    "Subqueries do not support index seeks"
                );
                program.resolve_label(loop_labels.next, program.offset());
                let iteration_cursor_id = temp_cursor_id.unwrap_or_else(|| {
                    index_cursor_id.unwrap_or_else(|| {
                        table_cursor_id
                            .expect("Either ephemeral or index or table cursor must be opened")
                    })
                });
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a Next instruction.
                if !matches!(search, Search::RowidEq { .. }) {
                    let iter_dir = match search {
                        Search::Seek { seek_def, .. } => seek_def.iter_dir,
                        Search::RowidEq { .. } => unreachable!(),
                    };

                    if iter_dir == IterationDirection::Backwards {
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
                program.preassign_label_to_next_insn(loop_labels.loop_end);
            }
        }

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
                let label_when_right_table_notnull = program.allocate_label();
                program.emit_insn(Insn::IfPos {
                    reg: lj_meta.reg_match_flag,
                    target_pc: label_when_right_table_notnull,
                    decrement_by: 0,
                });
                // If the left join match flag is still 0, it means there was no match on the right table,
                // but since it's a LEFT JOIN, we still need to emit a row with NULLs for the right table.
                // In that case, we now enter the routine that does exactly that.
                // First we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL.
                // This needs to be set for both the table and the index cursor, if present,
                // since even if the iteration cursor is the index cursor, it might fetch values from the table cursor.
                [table_cursor_id, index_cursor_id]
                    .iter()
                    .filter_map(|maybe_cursor_id| maybe_cursor_id.as_ref())
                    .for_each(|cursor_id| {
                        program.emit_insn(Insn::NullRow {
                            cursor_id: *cursor_id,
                        });
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
                program.preassign_label_to_next_insn(label_when_right_table_notnull);
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
    tables: &TableReferences,
    seek_def: &SeekDef,
    t_ctx: &mut TranslateCtx,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_end: BranchOffset,
    is_index: bool,
) -> Result<()> {
    let Some(seek) = seek_def.seek.as_ref() else {
        // If there is no seek key, we start from the first or last row of the index,
        // depending on the iteration direction.
        match seek_def.iter_dir {
            IterationDirection::Forwards => {
                program.emit_insn(Insn::Rewind {
                    cursor_id: seek_cursor_id,
                    pc_if_empty: loop_end,
                });
            }
            IterationDirection::Backwards => {
                program.emit_insn(Insn::Last {
                    cursor_id: seek_cursor_id,
                    pc_if_empty: loop_end,
                });
            }
        }
        return Ok(());
    };
    // We allocated registers for the full index key, but our seek key might not use the full index key.
    // See [crate::translate::optimizer::build_seek_def] for more details about in which cases we do and don't use the full index key.
    for i in 0..seek_def.key.len() {
        let reg = start_reg + i;
        if i >= seek.len {
            if seek.null_pad {
                program.emit_insn(Insn::Null {
                    dest: reg,
                    dest_end: None,
                });
            }
        } else {
            let expr = &seek_def.key[i].0;
            translate_expr_no_constant_opt(
                program,
                Some(tables),
                expr,
                reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            // If the seek key column is not verifiably non-NULL, we need check whether it is NULL,
            // and if so, jump to the loop end.
            // This is to avoid returning rows for e.g. SELECT * FROM t WHERE t.x > NULL,
            // which would erroneously return all rows from t, as NULL is lower than any non-NULL value in index key comparisons.
            if !expr.is_nonnull(tables) {
                program.emit_insn(Insn::IsNull {
                    reg,
                    target_pc: loop_end,
                });
            }
        }
    }
    let num_regs = if seek.null_pad {
        seek_def.key.len()
    } else {
        seek.len
    };
    match seek.op {
        SeekOp::GE { eq_only } => program.emit_insn(Insn::SeekGE {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
            eq_only,
        }),
        SeekOp::GT => program.emit_insn(Insn::SeekGT {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        SeekOp::LE { eq_only } => program.emit_insn(Insn::SeekLE {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
            eq_only,
        }),
        SeekOp::LT => program.emit_insn(Insn::SeekLT {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
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
    tables: &TableReferences,
    seek_def: &SeekDef,
    t_ctx: &mut TranslateCtx,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    is_index: bool,
) -> Result<()> {
    let Some(termination) = seek_def.termination.as_ref() else {
        program.preassign_label_to_next_insn(loop_start);
        return Ok(());
    };

    // How many non-NULL values were used for seeking.
    let seek_len = seek_def.seek.as_ref().map_or(0, |seek| seek.len);

    // How many values will be used for the termination condition.
    let num_regs = if termination.null_pad {
        seek_def.key.len()
    } else {
        termination.len
    };
    for i in 0..seek_def.key.len() {
        let reg = start_reg + i;
        let is_last = i == seek_def.key.len() - 1;

        // For all index key values apart from the last one, we are guaranteed to use the same values
        // as were used for the seek, so we don't need to emit them again.
        if i < seek_len && !is_last {
            continue;
        }
        // For the last index key value, we need to emit a NULL if the termination condition is NULL-padded.
        // See [SeekKey::null_pad] and [crate::translate::optimizer::build_seek_def] for why this is the case.
        if i >= termination.len && !termination.null_pad {
            continue;
        }
        if is_last && termination.null_pad {
            program.emit_insn(Insn::Null {
                dest: reg,
                dest_end: None,
            });
        // if the seek key is shorter than the termination key, we need to translate the remaining suffix of the termination key.
        // if not, we just reuse what was emitted for the seek.
        } else if seek_len < termination.len {
            translate_expr_no_constant_opt(
                program,
                Some(tables),
                &seek_def.key[i].0,
                reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }
    }
    program.preassign_label_to_next_insn(loop_start);
    let mut rowid_reg = None;
    let mut affinity = None;
    if !is_index {
        rowid_reg = Some(program.alloc_register());
        program.emit_insn(Insn::RowId {
            cursor_id: seek_cursor_id,
            dest: rowid_reg.unwrap(),
        });

        affinity = if let Some(table_ref) = tables
            .joined_tables()
            .iter()
            .find(|t| t.columns().iter().any(|c| c.is_rowid_alias))
        {
            if let Some(rowid_col_idx) = table_ref.columns().iter().position(|c| c.is_rowid_alias) {
                Some(table_ref.columns()[rowid_col_idx].affinity())
            } else {
                Some(Affinity::Numeric)
            }
        } else {
            Some(Affinity::Numeric)
        };
    }
    match (is_index, termination.op) {
        (true, SeekOp::GE { .. }) => program.emit_insn(Insn::IdxGE {
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
        (true, SeekOp::LE { .. }) => program.emit_insn(Insn::IdxLE {
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
        (false, SeekOp::GE { .. }) => program.emit_insn(Insn::Ge {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(affinity.unwrap()),
            collation: program.curr_collation(),
        }),
        (false, SeekOp::GT) => program.emit_insn(Insn::Gt {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(affinity.unwrap()),
            collation: program.curr_collation(),
        }),
        (false, SeekOp::LE { .. }) => program.emit_insn(Insn::Le {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(affinity.unwrap()),
            collation: program.curr_collation(),
        }),
        (false, SeekOp::LT) => program.emit_insn(Insn::Lt {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(affinity.unwrap()),
            collation: program.curr_collation(),
        }),
    };

    Ok(())
}

/// Open an ephemeral index cursor and build an automatic index on a table.
/// This is used as a last-resort to avoid a nested full table scan
/// Returns the cursor id of the ephemeral index cursor.
fn emit_autoindex(
    program: &mut ProgramBuilder,
    index: &Arc<Index>,
    table_cursor_id: CursorID,
    index_cursor_id: CursorID,
    table_has_rowid: bool,
) -> Result<CursorID> {
    assert!(index.ephemeral, "Index {} is not ephemeral", index.name);
    let label_ephemeral_build_end = program.allocate_label();
    // Since this typically happens in an inner loop, we only build it once.
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: label_ephemeral_build_end,
    });
    program.emit_insn(Insn::OpenAutoindex {
        cursor_id: index_cursor_id,
    });
    // Rewind source table
    let label_ephemeral_build_loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: table_cursor_id,
        pc_if_empty: label_ephemeral_build_loop_start,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_loop_start);
    // Emit all columns from source table that are needed in the ephemeral index.
    // Also reserve a register for the rowid if the source table has rowids.
    let num_regs_to_reserve = index.columns.len() + table_has_rowid as usize;
    let ephemeral_cols_start_reg = program.alloc_registers(num_regs_to_reserve);
    for (i, col) in index.columns.iter().enumerate() {
        let reg = ephemeral_cols_start_reg + i;
        program.emit_column(table_cursor_id, col.pos_in_table, reg);
    }
    if table_has_rowid {
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: ephemeral_cols_start_reg + index.columns.len(),
        });
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: ephemeral_cols_start_reg,
        count: num_regs_to_reserve,
        dest_reg: record_reg,
        index_name: Some(index.name.clone()),
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: index_cursor_id,
        record_reg,
        unpacked_start: Some(ephemeral_cols_start_reg),
        unpacked_count: Some(num_regs_to_reserve as u16),
        flags: IdxInsertFlags::new().use_seek(false),
    });
    program.emit_insn(Insn::Next {
        cursor_id: table_cursor_id,
        pc_if_next: label_ephemeral_build_loop_start,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_end);
    Ok(index_cursor_id)
}
