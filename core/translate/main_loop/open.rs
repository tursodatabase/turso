use super::*;
use crate::translate::main_loop::{conditions::LoopConditionEmitter, hash::HashProbeSetupEmitter};
use crate::translate::{
    main_loop::close::AutoIndexBuild,
    plan::{self, SubqueryEvalPhase},
    subquery::{materialized_from_clause_subquery_storage, MaterializedFromClauseSubqueryStorage},
};

fn emit_materialized_subquery_result_columns(
    program: &mut ProgramBuilder,
    from_clause_subquery: &crate::schema::FromClauseSubquery,
    cursor_id: CursorID,
    index: Option<&Index>,
) {
    let Some(start_reg) = from_clause_subquery.result_columns_start_reg else {
        return;
    };

    let index_to_table = index.map(|index| {
        let mut source_cols = vec![None; from_clause_subquery.columns.len()];
        for (source_col, idx_col) in index.columns.iter().enumerate() {
            source_cols[idx_col.pos_in_table] = Some(source_col);
        }
        source_cols
    });

    for col_idx in 0..from_clause_subquery.columns.len() {
        let source_col = index_to_table
            .as_ref()
            .map(|source_cols| {
                source_cols[col_idx]
                    .expect("direct materialized subquery index must cover every result column")
            })
            .unwrap_or(col_idx);
        program.emit_insn(Insn::Column {
            cursor_id,
            column: source_col,
            dest: start_reg + col_idx,
            default: None,
        });
    }
}

/// Opens the main loop for each table in the join order, emitting instructions to initialize
/// cursors and perform index seeks as necessary.
pub struct OpenLoop;

impl OpenLoop {
    #[allow(clippy::too_many_arguments)]
    pub fn emit(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx,
        table_references: &TableReferences,
        join_order: &[JoinOrderMember],
        predicates: &[WhereTerm],
        temp_cursor_id: Option<CursorID>,
        mode: OperationMode,
        subqueries: &mut [NonFromClauseSubquery],
    ) -> Result<()> {
        let live_table_ids: HashSet<_> = join_order.iter().map(|member| member.table_id).collect();
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

            // For chained anti-joins (e.g. NOT EXISTS t2 AND NOT EXISTS t3),
            // when anti-join N exhausts without a match, execution should continue
            // to anti-join N+1's open_loop (not jump to the body). Resolve the
            // previous anti-join's label_body to the current program offset.
            if join_index > 0 {
                let prev_table_idx = join_order[join_index - 1].original_idx;
                let prev_is_anti = table_references.joined_tables()[prev_table_idx]
                    .join_info
                    .as_ref()
                    .is_some_and(|ji| ji.is_anti());
                if prev_is_anti {
                    if let Some(prev_sa_meta) = t_ctx.meta_semi_anti_joins[prev_table_idx].as_ref()
                    {
                        program.resolve_label(prev_sa_meta.label_body, program.offset());
                    }
                }
            }

            // Each OUTER JOIN has a "match flag" that is initially set to false,
            // and is set to true when a match is found for the OUTER JOIN.
            // This is used to determine whether to emit actual columns or NULLs for the columns of the right table.
            if let Some(join_info) = table.join_info.as_ref() {
                if join_info.is_outer() {
                    let lj_meta = t_ctx.meta_left_joins[joined_table_index].as_ref().unwrap();
                    program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: lj_meta.reg_match_flag,
                    });
                }
            }

            let (table_cursor_id, index_cursor_id) =
                table.resolve_cursors(program, mode.clone())?;

            match &table.op {
                Operation::Scan(scan) => {
                    match (scan, &table.table) {
                        (Scan::BTreeTable { iter_dir, .. }, Table::BTree(_)) => {
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
                        (
                            Scan::VirtualTable {
                                idx_num,
                                idx_str,
                                constraints,
                            },
                            Table::Virtual(_),
                        ) => {
                            let (start_reg, count, maybe_idx_str, maybe_idx_int) = {
                                let args_needed = constraints.len();
                                let start_reg = program.alloc_registers(args_needed);

                                for (argv_index, expr) in constraints.iter().enumerate() {
                                    let target_reg = start_reg + argv_index;
                                    translate_expr(
                                        program,
                                        Some(table_references),
                                        expr,
                                        target_reg,
                                        &t_ctx.resolver,
                                    )?;
                                }

                                // If best_index provided an idx_str, translate it.
                                let maybe_idx_str = if let Some(idx_str) = idx_str {
                                    let reg = program.alloc_register();
                                    program.emit_insn(Insn::String8 {
                                        dest: reg,
                                        value: idx_str.to_owned(),
                                    });
                                    Some(reg)
                                } else {
                                    None
                                };
                                (start_reg, args_needed, maybe_idx_str, Some(*idx_num))
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
                        (
                            Scan::Subquery { iter_dir },
                            Table::FromClauseSubquery(from_clause_subquery),
                        ) => {
                            match from_clause_subquery.plan.select_query_destination() {
                                Some(QueryDestination::CoroutineYield {
                                    yield_reg,
                                    coroutine_implementation_start,
                                }) => {
                                    turso_assert_eq!(
                                        *iter_dir,
                                        IterationDirection::Forwards,
                                        "coroutine-backed subqueries cannot scan backwards"
                                    );
                                    // Coroutine-based subquery execution
                                    // In case the subquery is an inner loop, it needs to be reinitialized on each iteration of the outer loop.
                                    program.emit_insn(Insn::InitCoroutine {
                                        yield_reg: *yield_reg,
                                        jump_on_definition: BranchOffset::Offset(0),
                                        start_offset: *coroutine_implementation_start,
                                    });
                                    program.preassign_label_to_next_insn(loop_start);
                                    // A subquery within the main loop of a parent query has no cursor, so instead of advancing the cursor,
                                    // it emits a Yield which jumps back to the main loop of the subquery itself to retrieve the next row.
                                    // When the subquery coroutine completes, this instruction jumps to the label at the top of the termination_label_stack,
                                    // which in this case is the end of the Yield-Goto loop in the parent query.
                                    program.emit_insn(Insn::Yield {
                                        yield_reg: *yield_reg,
                                        end_offset: loop_end,
                                        subtype_clear_start_reg: 0,
                                        subtype_clear_count: 0,
                                    });
                                }
                                Some(QueryDestination::EphemeralTable { cursor_id, .. }) => {
                                    // Materialized CTE - scan the ephemeral table with Rewind/Next
                                    if *iter_dir == IterationDirection::Backwards {
                                        program.emit_insn(Insn::Last {
                                            cursor_id: *cursor_id,
                                            pc_if_empty: loop_end,
                                        });
                                    } else {
                                        program.emit_insn(Insn::Rewind {
                                            cursor_id: *cursor_id,
                                            pc_if_empty: loop_end,
                                        });
                                    }
                                    program.preassign_label_to_next_insn(loop_start);
                                    emit_materialized_subquery_result_columns(
                                        program,
                                        from_clause_subquery,
                                        *cursor_id,
                                        None,
                                    );
                                }
                                _ => {
                                    unreachable!("Subquery table with unexpected query destination")
                                }
                            }
                        }
                        _ => unreachable!(
                            "{:?} scan cannot be used with {:?} table",
                            scan, table.table
                        ),
                    }
                    if let Some(table_cursor_id) = table_cursor_id {
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn(Insn::DeferredSeek {
                                index_cursor_id,
                                table_cursor_id,
                            });
                        }
                    }
                }
                Operation::Search(search) => {
                    let materialized_subquery_storage = match (&table.table, search) {
                        (
                            Table::FromClauseSubquery(from_clause_subquery),
                            Search::Seek {
                                index: Some(index), ..
                            },
                        ) if index.ephemeral => {
                            materialized_from_clause_subquery_storage(from_clause_subquery)
                        }
                        _ => None,
                    };

                    // Open the loop for the index search.
                    // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
                    match search {
                        Search::RowidEq { cmp_expr } => {
                            assert!(
                                !matches!(table.table, Table::FromClauseSubquery(_)),
                                "Subqueries do not support rowid seeks"
                            );
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
                        }
                        Search::Seek {
                            index, seek_def, ..
                        } => {
                            // Otherwise, it's an index/rowid scan, i.e. first a seek is performed and then a scan until the comparison expression is not satisfied anymore.
                            let mut bloom_filter = false;
                            if let Some(index) = index {
                                if index.ephemeral
                                    && !matches!(
                                        materialized_subquery_storage,
                                        Some(MaterializedFromClauseSubqueryStorage::DirectIndex)
                                    )
                                {
                                    // Build auxiliary ephemeral indexes lazily from the row source,
                                    // whether it is a base table or a table-backed materialized subquery.
                                    let table_has_rowid = if let Table::BTree(btree) = &table.table
                                    {
                                        btree.has_rowid
                                    } else {
                                        matches!(&table.table, Table::FromClauseSubquery(_))
                                    };
                                    let num_seek_keys = seek_def.size(&seek_def.start);
                                    let table_columns = if let Table::BTree(btree) = &table.table {
                                        Some(btree.columns.as_slice())
                                    } else {
                                        None
                                    };
                                    let AutoIndexResult {
                                        use_bloom_filter, ..
                                    } = emit_autoindex(
                                        program,
                                        AutoIndexBuild {
                                            index,
                                            table_cursor_id: table_cursor_id.expect(
                                                "an ephemeral index must have a source table cursor",
                                            ),
                                            index_cursor_id: index_cursor_id.expect(
                                                "an ephemeral index must have an index cursor",
                                            ),
                                            table_has_rowid,
                                            num_seek_keys,
                                            seek_def,
                                            affinity_str: plan::synthesized_seek_affinity_str(
                                                index, seek_def,
                                            )
                                            .as_ref(),
                                            table_columns,
                                            table_ref_id: table.internal_id,
                                            table_references,
                                            resolver: &t_ctx.resolver,
                                        },
                                    )?;
                                    bloom_filter = use_bloom_filter;
                                }
                            }

                            let seek_cursor_id = if materialized_subquery_storage.is_some() {
                                index_cursor_id
                                    .expect("materialized subquery must have index cursor")
                            } else {
                                temp_cursor_id.unwrap_or_else(|| {
                                    index_cursor_id.unwrap_or_else(|| {
                                        table_cursor_id.expect(
                                        "Either ephemeral or index or table cursor must be opened",
                                    )
                                    })
                                })
                            };

                            let max_registers = seek_def
                                .size(&seek_def.start)
                                .max(seek_def.size(&seek_def.end));
                            let start_reg = program.alloc_registers(max_registers);
                            SeekEmitter::new(
                                program,
                                table_references,
                                seek_def,
                                t_ctx,
                                seek_cursor_id,
                                start_reg,
                                loop_end,
                                index.as_ref(),
                            )
                            .emit(loop_start, bloom_filter)?;

                            if let Some(materialized_subquery_storage) =
                                materialized_subquery_storage
                            {
                                let index_cursor_id = index_cursor_id
                                    .expect("materialized subquery seek requires index cursor");
                                let Table::FromClauseSubquery(from_clause_subquery) = &table.table
                                else {
                                    unreachable!("materialized subquery seek requires subquery")
                                };
                                match materialized_subquery_storage {
                                    MaterializedFromClauseSubqueryStorage::TableBacked => {
                                        let table_cursor_id = table_cursor_id
                                            .expect("materialized subquery must have table cursor");
                                        program.emit_insn(Insn::DeferredSeek {
                                            index_cursor_id,
                                            table_cursor_id,
                                        });
                                        emit_materialized_subquery_result_columns(
                                            program,
                                            from_clause_subquery,
                                            table_cursor_id,
                                            None,
                                        );
                                    }
                                    MaterializedFromClauseSubqueryStorage::DirectIndex => {
                                        let index = index.as_ref().expect(
                                            "direct-index materialized subquery requires index",
                                        );
                                        emit_materialized_subquery_result_columns(
                                            program,
                                            from_clause_subquery,
                                            index_cursor_id,
                                            Some(index.as_ref()),
                                        );
                                    }
                                }
                            } else {
                                // Only emit DeferredSeek for non-subquery tables
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
                        }
                        Search::InSeek { index, source } => {
                            let is_rowid = index.is_none();
                            let ephemeral_cursor_id = open_in_seek_source_cursor(
                                program,
                                table_references,
                                &t_ctx.resolver,
                                index.as_ref(),
                                source,
                            )?;

                            program.emit_insn(Insn::NullRow {
                                cursor_id: ephemeral_cursor_id,
                            });
                            program.emit_insn(Insn::Rewind {
                                cursor_id: ephemeral_cursor_id,
                                pc_if_empty: loop_end,
                            });

                            let outer_loop_start = program.allocate_label();
                            program.preassign_label_to_next_insn(outer_loop_start);
                            let seek_reg = program.alloc_register();
                            // The emitted loop is:
                            //   for each RHS key in the ephemeral cursor
                            //     seek table/index to that key
                            //     scan all matching rows for that key
                            program.emit_insn(Insn::Column {
                                cursor_id: ephemeral_cursor_id,
                                column: 0,
                                dest: seek_reg,
                                default: None,
                            });

                            let next_val_label = program.allocate_label();
                            program.emit_insn(Insn::IsNull {
                                reg: seek_reg,
                                target_pc: next_val_label,
                            });

                            if is_rowid {
                                program.emit_insn(Insn::SeekRowid {
                                    cursor_id: table_cursor_id
                                        .expect("InSeek rowid requires table cursor"),
                                    src_reg: seek_reg,
                                    target_pc: next_val_label,
                                });
                            } else {
                                let idx_cursor = index_cursor_id
                                    .expect("InSeek with index requires index cursor");
                                program.emit_insn(Insn::SeekGE {
                                    cursor_id: idx_cursor,
                                    start_reg: seek_reg,
                                    num_regs: 1,
                                    target_pc: next_val_label,
                                    is_index: true,
                                    eq_only: false,
                                });
                                program.preassign_label_to_next_insn(loop_start);
                                program.emit_insn(Insn::IdxGT {
                                    cursor_id: idx_cursor,
                                    start_reg: seek_reg,
                                    num_regs: 1,
                                    target_pc: next_val_label,
                                });
                                if let Some(table_cursor_id) = table_cursor_id {
                                    program.emit_insn(Insn::DeferredSeek {
                                        index_cursor_id: idx_cursor,
                                        table_cursor_id,
                                    });
                                }
                            }

                            // `close_loop` uses this metadata to stitch together the outer
                            // ephemeral-value loop and the inner scan over matches for the
                            // current value.
                            t_ctx.meta_in_seeks[joined_table_index] = Some(InSeekMetadata {
                                ephemeral_cursor_id,
                                outer_loop_start,
                                next_val_label,
                            });
                        }
                    }
                }
                Operation::IndexMethodQuery(query) => {
                    let start_reg = program.alloc_registers(query.arguments.len() + 1);
                    program.emit_int(query.pattern_idx as i64, start_reg);
                    for i in 0..query.arguments.len() {
                        translate_expr(
                            program,
                            Some(table_references),
                            &query.arguments[i],
                            start_reg + 1 + i,
                            &t_ctx.resolver,
                        )?;
                    }
                    program.emit_insn(Insn::IndexMethodQuery {
                        db: crate::MAIN_DB_ID,
                        cursor_id: index_cursor_id.expect("IndexMethod requires a index cursor"),
                        start_reg,
                        count_reg: query.arguments.len() + 1,
                        pc_if_empty: loop_end,
                    });
                    program.preassign_label_to_next_insn(loop_start);
                    if let Some(table_cursor_id) = table_cursor_id {
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn(Insn::DeferredSeek {
                                index_cursor_id,
                                table_cursor_id,
                            });
                        }
                    }
                }
                Operation::HashJoin(hash_join_op) => {
                    HashProbeSetupEmitter::new(
                        program,
                        t_ctx,
                        table_references,
                        subqueries,
                        predicates,
                        hash_join_op,
                        &mode,
                        table_cursor_id.expect("Probe table must have a cursor"),
                        loop_start,
                        loop_end,
                        next,
                        &live_table_ids,
                    )
                    .emit()?;
                }
                Operation::MultiIndexScan(multi_idx_op) => {
                    emit_multi_index_scan_loop(
                        program,
                        t_ctx,
                        table,
                        table_references,
                        multi_idx_op,
                        loop_start,
                        loop_end,
                    )?;
                }
            }

            let condition_fail_target = if let Operation::HashJoin(ref hj) = table.op {
                t_ctx
                    .hash_table_contexts
                    .get(&hj.build_table_idx)
                    .map(|ctx| ctx.labels.next)
                    .expect("should have hash context for build table")
            } else {
                next
            };
            let is_outer_hj_probe = matches!(table.op, Operation::HashJoin(ref hj) if matches!(
                hj.join_type,
                HashJoinType::LeftOuter | HashJoinType::FullOuter
            ));

            // Emit OUTER JOIN conditions (must run before setting match flags).
            LoopConditionEmitter::new(
                program,
                t_ctx,
                table_references,
                join_order,
                predicates,
                join_index,
                condition_fail_target,
                true,
                subqueries,
            )
            .emit()?;

            // Set the LEFT JOIN match flag. Skip outer hash join probes - they use
            // HashMarkMatched / check_outer instead.
            if let Some(join_info) = table.join_info.as_ref() {
                if join_info.is_outer() && !is_outer_hj_probe {
                    let lj_meta = t_ctx.meta_left_joins[joined_table_index].as_ref().unwrap();
                    program.resolve_label(lj_meta.label_match_flag_set_true, program.offset());
                    program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: lj_meta.reg_match_flag,
                    });
                }
            }

            // Outer hash joins: mark the build entry as matched.
            if let Operation::HashJoin(ref hj) = table.op {
                if matches!(
                    hj.join_type,
                    HashJoinType::LeftOuter | HashJoinType::FullOuter
                ) {
                    let build_table = &table_references.joined_tables()[hj.build_table_idx];
                    let hash_table_id: usize = build_table.internal_id.into();
                    program.emit_insn(Insn::HashMarkMatched { hash_table_id });

                    // FULL OUTER: also set the probe-side match flag.
                    if matches!(hj.join_type, HashJoinType::FullOuter) {
                        let probe_idx = hj.probe_table_idx;
                        if let Some(lj_meta) = t_ctx.meta_left_joins[probe_idx].as_ref() {
                            program
                                .resolve_label(lj_meta.label_match_flag_set_true, program.offset());
                            program.emit_insn(Insn::Integer {
                                value: 1,
                                dest: lj_meta.reg_match_flag,
                            });
                        }
                    }
                }
            }

            // Emit non-OUTER JOIN conditions.
            let from_outer_join = false;
            LoopConditionEmitter::new(
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
            .emit()?;

            // ANTI-JOIN: all conditions passed means a match was found.
            // Skip the outer row by jumping to the outer loop's Next.
            // label_body is resolved later in emit_loop, right before the body is emitted.
            if let Some(join_info) = table.join_info.as_ref() {
                if join_info.is_anti() {
                    let sa_meta = t_ctx.meta_semi_anti_joins[joined_table_index]
                        .as_ref()
                        .expect("anti-join must have SemiAntiJoinMetadata");
                    program.add_comment(program.offset(), "anti-join: match found, skip outer row");
                    program.emit_insn(Insn::Goto {
                        target_pc: sa_meta.label_next_outer,
                    });
                }
            }

            // Outer hash joins wrap inner loops in a Gosub subroutine so that
            // unmatched-row emission paths can re-enter them (cursors get Rewind'd).
            if let Operation::HashJoin(ref hj) = table.op {
                if matches!(
                    hj.join_type,
                    HashJoinType::LeftOuter | HashJoinType::FullOuter
                ) {
                    let return_reg = program.alloc_register();
                    let gosub_label = program.allocate_label();
                    let skip_label = program.allocate_label();

                    program.emit_insn(Insn::Gosub {
                        target_pc: gosub_label,
                        return_reg,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: skip_label,
                    });
                    // Subroutine body starts here (inner loops follow)
                    program.preassign_label_to_next_insn(gosub_label);

                    if let Some(hash_ctx) = t_ctx.hash_table_contexts.get_mut(&hj.build_table_idx) {
                        hash_ctx.inner_loop_gosub_reg = Some(return_reg);
                        hash_ctx.labels.inner_loop_gosub = Some(gosub_label);
                        hash_ctx.labels.inner_loop_skip = Some(skip_label);
                    }
                }
            }
        }

        if subqueries.iter().any(|s| {
            !s.has_been_evaluated() && matches!(s.eval_phase, SubqueryEvalPhase::BeforeLoop)
        }) {
            crate::bail_parse_error!(
                "all before-loop subqueries should have already been emitted, but found {} unevaluated subqueries",
                subqueries
                    .iter()
                    .filter(|s| {
                        !s.has_been_evaluated()
                            && matches!(s.eval_phase, SubqueryEvalPhase::BeforeLoop)
                    })
                    .count()
            );
        }

        Ok(())
    }
}
