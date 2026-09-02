use super::*;
use crate::translate::main_loop::{conditions::LoopConditionEmitter, hash::HashProbeSetupEmitter};
use crate::translate::{
    main_loop::close::AutoIndexBuild,
    plan::{self, SubqueryEvalPhase},
    subquery::{materialized_from_clause_subquery_storage, MaterializedFromClauseSubqueryStorage},
};

/// Reload a materialized subquery row into its result registers.
///
/// The unmatched-row pass scans the materialized cursor after its normal loop.
/// Expressions still read the subquery through these result registers.
pub(super) fn emit_materialized_subquery_result_columns(
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

/// Read the current right-side rowid into the shared match-key register.
///
/// SQLite uses the rowid as the identity of a matched right row. A recursive
/// pseudo-row has a NULL rowid and still follows the same exact-set lookup.
pub(super) fn emit_right_join_key(
    program: &mut ProgramBuilder,
    right_join: &RightJoinMetadata,
    table_cursor_id: CursorID,
    index_cursor_id: Option<CursorID>,
) {
    if let Some(index_cursor_id) = index_cursor_id {
        // The index drives this loop, so its rowid identifies the current row.
        // The table cursor does not move until `DeferredSeek` runs.
        program.emit_insn(Insn::IdxRowId {
            cursor_id: index_cursor_id,
            dest: right_join.rowid_reg,
        });
    } else {
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: right_join.rowid_reg,
        });
    }
}

/// Record one matched right-side row in the exact set and its bloom filter.
///
/// The exact `Found` check avoids duplicate index inserts when several left rows
/// match the same right row. The bloom filter only speeds up the later scan.
fn emit_right_join_match(
    program: &mut ProgramBuilder,
    right_join: &RightJoinMetadata,
    table_cursor_id: CursorID,
) {
    emit_right_join_key(program, right_join, table_cursor_id, None);
    let already_recorded = program.allocate_label();
    program.emit_insn(Insn::Found {
        cursor_id: right_join.matched_rows_cursor_id,
        target_pc: already_recorded,
        record_reg: right_join.rowid_reg,
        num_regs: 1,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(right_join.rowid_reg),
        count: 1,
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: right_join.matched_rows_cursor_id,
        record_reg,
        unpacked_start: Some(right_join.rowid_reg),
        unpacked_count: Some(1),
        flags: IdxInsertFlags::new(),
    });
    program.emit_insn(Insn::FilterAdd {
        cursor_id: right_join.matched_rows_cursor_id,
        key_reg: right_join.rowid_reg,
        num_keys: 1,
    });
    program.preassign_label_to_next_insn(already_recorded);
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
                        program.preassign_label_to_next_insn(prev_sa_meta.label_body);
                    }
                }
            }

            // Each OUTER JOIN has a "match flag" that is initially set to false,
            // and is set to true when a match is found for the OUTER JOIN.
            // This is used to determine whether to emit actual columns or NULLs for the columns of the right table.
            if let Some(join_info) = table.join_info.as_ref() {
                if join_info.keeps_left_rows() {
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
                            emit_virtual_table_scan_start(
                                program,
                                table_references,
                                &t_ctx.resolver,
                                table_cursor_id
                                    .expect("Virtual tables do not support covering indexes"),
                                *idx_num,
                                idx_str.as_deref(),
                                constraints,
                                loop_start,
                                loop_end,
                            )?;
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
                        (Scan::RecursiveCteInput, Table::RecursiveCteInput(_)) => {
                            program.preassign_label_to_next_insn(loop_start);
                        }
                        _ => unreachable!(
                            "{:?} scan cannot be used with {:?} table",
                            scan, table.table
                        ),
                    }
                    if let Some(table_cursor_id) = table_cursor_id {
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_deferred_seek(index_cursor_id, table_cursor_id);
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
                                        Some(btree.columns())
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
                                        program
                                            .emit_deferred_seek(index_cursor_id, table_cursor_id);
                                        emit_materialized_subquery_result_columns(
                                            program,
                                            from_clause_subquery,
                                            table_cursor_id,
                                            None,
                                        );
                                    }
                                    // Expressions read direct index columns when they need them.
                                    // Copying all columns here would only write unused registers.
                                    MaterializedFromClauseSubqueryStorage::DirectIndex => {}
                                }
                            } else {
                                // Only emit DeferredSeek for non-subquery tables
                                if let Some(index_cursor_id) = index_cursor_id {
                                    if let Some(table_cursor_id) = table_cursor_id {
                                        // Don't do a btree table seek until it's actually necessary to read from the table.
                                        program
                                            .emit_deferred_seek(index_cursor_id, table_cursor_id);
                                    }
                                }
                            }
                        }
                        Search::InSeek { index, source } => {
                            let meta = emit_in_seek_start(
                                program,
                                table_references,
                                &t_ctx.resolver,
                                index.as_ref(),
                                source,
                                table_cursor_id,
                                index_cursor_id,
                                loop_start,
                                loop_end,
                            )?;
                            t_ctx.meta_in_seeks[joined_table_index] = Some(meta);
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
                            program.emit_deferred_seek(index_cursor_id, table_cursor_id);
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

            let condition_fail_target =
                if let Some(right_join) = t_ctx.meta_right_joins[joined_table_index].as_ref() {
                    right_join.return_label
                } else if let Operation::HashJoin(ref hj) = table.op {
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

            // Record the right row after its ON terms pass. Later WHERE terms
            // must not change whether this row matched the join.
            if let Some(right_join) = t_ctx.meta_right_joins[joined_table_index].as_ref() {
                let table_cursor_id = table_cursor_id
                    .expect("a right-preserving join must keep its table cursor open");
                emit_right_join_match(program, right_join, table_cursor_id);
            }

            // Set the LEFT JOIN match flag. Skip outer hash join probes - they use
            // HashMarkMatched / check_outer instead.
            if let Some(join_info) = table.join_info.as_ref() {
                if join_info.keeps_left_rows() && !is_outer_hj_probe {
                    let lj_meta = t_ctx.meta_left_joins[joined_table_index].as_ref().unwrap();
                    program.preassign_label_to_next_insn(lj_meta.label_match_flag_set_true);
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
                            program.preassign_label_to_next_insn(lj_meta.label_match_flag_set_true);
                            program.emit_insn(Insn::Integer {
                                value: 1,
                                dest: lj_meta.reg_match_flag,
                            });
                        }
                    }
                }
            }

            // Normal loop execution enters the row body inline. The unmatched
            // right-row scan enters the same body with Gosub.
            if let Some(right_join) = t_ctx.meta_right_joins[joined_table_index].as_ref() {
                program.emit_insn(Insn::BeginSubrtn {
                    dest: right_join.return_reg,
                    dest_end: None,
                });
                program.preassign_label_to_next_insn(right_join.body_label);
            }

            // Emit non-OUTER JOIN conditions.
            let outer_join_terms = false;
            LoopConditionEmitter::new(
                program,
                t_ctx,
                table_references,
                join_order,
                predicates,
                join_index,
                condition_fail_target,
                outer_join_terms,
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

/// Emit the `VFilter` that starts a virtual-table loop.
///
/// The main loop and the unmatched-right read must use the argument order
/// that `best_index` selected.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_virtual_table_scan_start(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    resolver: &Resolver<'_>,
    table_cursor_id: CursorID,
    idx_num: i32,
    idx_str: Option<&str>,
    constraints: &[Expr],
    loop_start: BranchOffset,
    loop_end: BranchOffset,
) -> Result<()> {
    let start_reg = program.alloc_registers(constraints.len());
    for (argument_index, expr) in constraints.iter().enumerate() {
        translate_expr(
            program,
            Some(table_references),
            expr,
            start_reg + argument_index,
            resolver,
        )?;
    }

    let idx_str = idx_str.map(|value| {
        let register = program.alloc_register();
        program.emit_insn(Insn::String8 {
            dest: register,
            value: value.to_owned(),
        });
        register
    });
    program.emit_insn(Insn::VFilter {
        cursor_id: table_cursor_id,
        arg_count: constraints.len(),
        args_reg: start_reg,
        idx_str,
        idx_num: idx_num as usize,
        pc_if_empty: loop_end,
    });
    program.preassign_label_to_next_insn(loop_start);
    Ok(())
}
