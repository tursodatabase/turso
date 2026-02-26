use super::*;

/// Emit WHERE conditions and inner-loop entry for an unmatched outer hash join row.
///
/// Filters applicable WHERE terms (non-ON, non-consumed), optionally restricted to
/// `build_table_idx` / `probe_table_idx` when a Gosub wraps inner tables. Then either
/// enters the inner-loop subroutine via Gosub or calls `emit_loop` directly.
fn emit_hash_join_unmatched_row_conditions_and_loop<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
    build_table_idx: usize,
    probe_table_idx: usize,
    skip_label: BranchOffset,
    gosub: Option<(usize, BranchOffset)>,
) -> Result<()> {
    let has_gosub = gosub.is_some();
    let allowed_tables = {
        let mut m = TableMask::new();
        m.add_table(build_table_idx);
        m.add_table(probe_table_idx);
        m
    };
    for cond in plan
        .where_clause
        .iter()
        .filter(|c| !c.consumed && c.from_outer_join.is_none())
        .filter(|c| {
            !has_gosub || expr_tables_subset_of(&c.expr, &plan.table_references, &allowed_tables)
        })
    {
        emit_condition_with_fail_target(
            program,
            &t_ctx.resolver,
            &plan.table_references,
            &cond.expr,
            skip_label,
        )?;
    }

    if let Some((reg, label)) = gosub {
        program.emit_insn(Insn::Gosub {
            target_pc: label,
            return_reg: reg,
        });
    } else {
        emit_loop(program, t_ctx, plan)?;
    }
    Ok(())
}

/// Shared close-loop helper: resolves `next`, emits advance, then marks `loop_end`.
fn emit_close_loop_with_advance(
    program: &mut ProgramBuilder,
    loop_labels: LoopLabels,
    emit_advance: impl FnOnce(&mut ProgramBuilder),
) -> BranchOffset {
    let pc = program.offset();
    program.resolve_label(loop_labels.next, pc);
    emit_advance(program);
    program.preassign_label_to_next_insn(loop_labels.loop_end);
    pc
}

/// Emits the Next/Prev/VNext/Goto instruction for a Scan operation and resolves loop_end.
/// Returns the PC offset of the "next" instruction for semi/anti-join label resolution.
fn close_scan_operation_loop(
    program: &mut ProgramBuilder,
    table: &JoinedTable,
    scan: &Scan,
    loop_labels: LoopLabels,
    mode: &OperationMode,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
) -> BranchOffset {
    emit_close_loop_with_advance(program, loop_labels, |program| match scan {
        Scan::BTreeTable { iter_dir, .. } => {
            let iteration_cursor_id =
                if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
                    ephemeral_table_cursor_id,
                    ..
                }) = mode
                {
                    *ephemeral_table_cursor_id
                } else {
                    index_cursor_id.unwrap_or_else(|| {
                        table_cursor_id
                            .expect("Either ephemeral or index or table cursor must be opened")
                    })
                };
            emit_cursor_advance(
                program,
                iteration_cursor_id,
                loop_labels.loop_start,
                *iter_dir,
            );
        }
        Scan::VirtualTable { .. } => {
            program.emit_insn(Insn::VNext {
                cursor_id: table_cursor_id.expect("Virtual tables do not support covering indexes"),
                pc_if_next: loop_labels.loop_start,
            });
        }
        Scan::Subquery => {
            let Table::FromClauseSubquery(subquery) = &table.table else {
                unreachable!("Scan::Subquery must have a FromClauseSubquery table");
            };
            if let Some(QueryDestination::EphemeralTable { cursor_id, .. }) =
                subquery.plan.select_query_destination()
            {
                // Materialized CTE - use Next to iterate
                program.emit_insn(Insn::Next {
                    cursor_id: *cursor_id,
                    pc_if_next: loop_labels.loop_start,
                });
            } else {
                // Coroutine-based subquery - use Goto to Yield
                program.emit_insn(Insn::Goto {
                    target_pc: loop_labels.loop_start,
                });
            }
        }
    })
}

/// Emits Next/Prev for a Search operation (no-op for RowidEq) and resolves loop_end.
/// Returns the PC offset of the "next" instruction for semi/anti-join label resolution.
fn close_search_operation_loop(
    program: &mut ProgramBuilder,
    table: &JoinedTable,
    search: &Search,
    loop_labels: LoopLabels,
    mode: &OperationMode,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
) -> BranchOffset {
    // Materialized subqueries with ephemeral indexes are allowed
    let is_materialized_subquery = matches!(&table.table, Table::FromClauseSubquery(_))
        && matches!(search, Search::Seek { index: Some(idx), .. } if idx.ephemeral);
    turso_assert_some!(
        {
            is_from_clause: !matches!(table.table, Table::FromClauseSubquery(_)),
            is_materialized_subquery: is_materialized_subquery
        },
        "Subqueries do not support index seeks unless materialized"
    );
    let iteration_cursor_id =
        if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
            ephemeral_table_cursor_id,
            ..
        }) = mode
        {
            *ephemeral_table_cursor_id
        } else if is_materialized_subquery {
            // For materialized subqueries, use the index cursor
            index_cursor_id.expect("materialized subquery must have index cursor")
        } else {
            index_cursor_id.unwrap_or_else(|| {
                table_cursor_id.expect("Either ephemeral or index or table cursor must be opened")
            })
        };
    emit_close_loop_with_advance(program, loop_labels, |program| {
        // Rowid equality point lookups use SeekRowid which does not loop, so no Next needed.
        if let Search::Seek { seek_def, .. } = search {
            emit_cursor_advance(
                program,
                iteration_cursor_id,
                loop_labels.loop_start,
                seek_def.iter_dir,
            );
        }
    })
}

/// Emits Next for an IndexMethodQuery operation and resolves loop_end.
/// Returns the PC offset of the "next" instruction for semi/anti-join label resolution.
fn close_index_method_query_loop(
    program: &mut ProgramBuilder,
    loop_labels: LoopLabels,
    index_cursor_id: Option<CursorID>,
) -> BranchOffset {
    emit_close_loop_with_advance(program, loop_labels, |program| {
        program.emit_insn(Insn::Next {
            cursor_id: index_cursor_id.unwrap(),
            pc_if_next: loop_labels.loop_start,
        });
    })
}

/// Emits hash match iteration, FULL OUTER check_outer, unmatched row scanning,
/// and probe cursor advancement for a HashJoin close.
/// Returns the PC used for semi/anti-join label resolution.
fn close_hash_join_operation_loop<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    table_index: usize,
    hash_join_op: &HashJoinOp,
    loop_labels: LoopLabels,
    table_cursor_id: Option<CursorID>,
    select_plan: Option<&'a SelectPlan>,
) -> Result<BranchOffset> {
    let (
        hash_table_reg,
        match_reg,
        match_found_label,
        hash_next_label,
        payload_dest_reg,
        num_payload,
        check_outer_label,
        build_cursor_id,
        join_type,
        inner_loop_gosub_reg,
        inner_loop_gosub_label,
        inner_loop_skip_label,
    ) = {
        let hash_ctx = t_ctx
            .hash_table_contexts
            .get(&hash_join_op.build_table_idx)
            .expect("hash join context missing for build table");
        (
            hash_ctx.hash_table_reg,
            hash_ctx.match_reg,
            hash_ctx.labels.match_found_label,
            hash_ctx.labels.hash_next_label,
            hash_ctx.payload_start_reg,
            hash_ctx.payload_columns.len(),
            hash_ctx.labels.check_outer_label,
            hash_ctx.build_cursor_id,
            hash_ctx.join_type,
            hash_ctx.labels.inner_loop_gosub_reg,
            hash_ctx.labels.inner_loop_gosub_label,
            hash_ctx.labels.inner_loop_skip_label,
        )
    };

    let label_next_probe_row = program.allocate_label();
    let semi_anti_next_pc;

    // Probe table: emit logic for iterating through hash matches
    // End the inner-loop subroutine.
    if let Some(gosub_reg) = inner_loop_gosub_reg {
        // For semi/anti-joins inside a Gosub subroutine (LEFT/FULL
        // OUTER hash join), the "next outer" jump must land on Return
        // so the Gosub register routes us back to the correct caller
        // (HashNext in the matched phase, HashNextUnmatched in the
        // unmatched phase). If we jumped to HashNext directly, we'd
        // escape the subroutine and loop forever.
        semi_anti_next_pc = program.offset();
        program.emit_insn(Insn::Return {
            return_reg: gosub_reg,
            can_fallthrough: false,
        });
        if let Some(skip_label) = inner_loop_skip_label {
            program.preassign_label_to_next_insn(skip_label);
        }
    } else {
        semi_anti_next_pc = program.offset();
    }

    // FULL OUTER exhaustion -> check_outer; otherwise -> next_probe_row.
    let hash_next_target = if join_type == HashJoinType::FullOuter {
        check_outer_label.unwrap_or(label_next_probe_row)
    } else {
        label_next_probe_row
    };

    program.resolve_label(hash_next_label, program.offset());

    // Try next hash match; exhaustion jumps to hash_next_target.
    program.emit_insn(Insn::HashNext {
        hash_table_id: hash_table_reg,
        dest_reg: match_reg,
        target_pc: hash_next_target,
        payload_dest_reg,
        num_payload,
    });

    // Back to match processing (skip HashProbe to preserve iteration state).
    program.emit_insn(Insn::Goto {
        target_pc: match_found_label,
    });

    // FULL OUTER check_outer: emit unmatched probe rows with NULLs
    // for the build side (reached on probe miss or hash exhaustion).
    if matches!(join_type, HashJoinType::FullOuter) {
        // Probe table (RHS) has LeftJoinMetadata via join_info.is_outer()=true.
        let probe_table_idx = hash_join_op.probe_table_idx;
        let lj_meta = t_ctx.meta_left_joins[probe_table_idx]
            .as_ref()
            .expect("FULL OUTER probe table must have left join metadata");
        let reg_match_flag = lj_meta.reg_match_flag;

        // Both HashProbe miss and HashNext exhaustion land here.
        if let Some(col) = check_outer_label {
            program.resolve_label(col, program.offset());
        }
        // Resolve here instead of the generic outer-join block (skipped for hash probes).
        program.resolve_label(lj_meta.label_match_flag_check_value, program.offset());

        // If a previous match already set the flag, skip NullRow emission.
        program.emit_insn(Insn::IfPos {
            reg: reg_match_flag,
            target_pc: label_next_probe_row,
            decrement_by: 0,
        });

        // NullRow the build cursor so Column reads return NULL.
        if let Some(cursor_id) = build_cursor_id {
            program.emit_insn(Insn::NullRow { cursor_id });
        }

        // Null out cached payload registers too.
        if let Some(payload_reg) = payload_dest_reg {
            if num_payload > 0 {
                program.emit_insn(Insn::Null {
                    dest: payload_reg,
                    dest_end: Some(payload_reg + num_payload - 1),
                });
            }
        }

        if let Some(plan) = select_plan {
            emit_hash_join_unmatched_row_conditions_and_loop(
                program,
                t_ctx,
                plan,
                hash_join_op.build_table_idx,
                table_index,
                label_next_probe_row,
                inner_loop_gosub_reg.zip(inner_loop_gosub_label),
            )?;
        }
    }
    program.preassign_label_to_next_insn(label_next_probe_row);

    // Advance probe cursor.
    program.resolve_label(loop_labels.next, program.offset());
    let probe_cursor_id = table_cursor_id.expect("Probe table must have a cursor");
    program.emit_insn(Insn::Next {
        cursor_id: probe_cursor_id,
        pc_if_next: loop_labels.loop_start,
    });
    program.preassign_label_to_next_insn(loop_labels.loop_end);

    // Outer joins: emit unmatched build rows with NULLs for the probe side.
    if matches!(
        hash_join_op.join_type,
        HashJoinType::LeftOuter | HashJoinType::FullOuter
    ) {
        if let Some(plan) = select_plan {
            let (
                hash_table_reg,
                match_reg,
                payload_dest_reg,
                num_payload,
                build_cursor_id,
                inner_loop_gosub_reg,
                inner_loop_gosub_label,
            ) = {
                let hash_ctx = t_ctx
                    .hash_table_contexts
                    .get(&hash_join_op.build_table_idx)
                    .expect("hash join context missing for build table");
                (
                    hash_ctx.hash_table_reg,
                    hash_ctx.match_reg,
                    hash_ctx.payload_start_reg,
                    hash_ctx.payload_columns.len(),
                    hash_ctx.build_cursor_id,
                    hash_ctx.labels.inner_loop_gosub_reg,
                    hash_ctx.labels.inner_loop_gosub_label,
                )
            };

            let done_unmatched = program.allocate_label();

            program.emit_insn(Insn::NullRow {
                cursor_id: probe_cursor_id,
            });

            program.emit_insn(Insn::HashScanUnmatched {
                hash_table_id: hash_table_reg,
                dest_reg: match_reg,
                target_pc: done_unmatched,
                payload_dest_reg,
                num_payload,
            });

            let unmatched_loop = program.allocate_label();
            let label_next_unmatched = program.allocate_label();
            program.preassign_label_to_next_insn(unmatched_loop);

            if let Some(cursor_id) = build_cursor_id {
                program.emit_insn(Insn::SeekRowid {
                    cursor_id,
                    src_reg: match_reg,
                    target_pc: done_unmatched,
                });
            }

            emit_hash_join_unmatched_row_conditions_and_loop(
                program,
                t_ctx,
                plan,
                hash_join_op.build_table_idx,
                table_index,
                label_next_unmatched,
                inner_loop_gosub_reg.zip(inner_loop_gosub_label),
            )?;

            program.resolve_label(label_next_unmatched, program.offset());
            program.emit_insn(Insn::HashNextUnmatched {
                hash_table_id: hash_table_reg,
                dest_reg: match_reg,
                target_pc: done_unmatched,
                payload_dest_reg,
                num_payload,
            });
            program.emit_insn(Insn::Goto {
                target_pc: unmatched_loop,
            });

            program.preassign_label_to_next_insn(done_unmatched);
        }
    }

    Ok(semi_anti_next_pc)
}

/// Emits Goto for a MultiIndexScan close and resolves loop_end.
/// Returns the PC offset of the "next" instruction for semi/anti-join label resolution.
fn close_multi_index_scan_operation_loop(
    program: &mut ProgramBuilder,
    loop_labels: LoopLabels,
) -> BranchOffset {
    emit_close_loop_with_advance(program, loop_labels, |program| {
        program.emit_insn(Insn::Goto {
            target_pc: loop_labels.loop_start,
        });
    })
}

/// Resolves semi/anti-join "next outer" labels that target the given table_index.
fn resolve_semi_anti_join_next_labels_for_outer_table(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table_index: usize,
    next_pc: BranchOffset,
) {
    for meta in t_ctx.meta_semi_anti_joins.iter().flatten() {
        if meta.outer_table_idx == table_index {
            program.resolve_label(meta.label_next_outer, next_pc);
        }
    }
}

/// Emits semi/anti-join exhaustion handling after the inner loop ends.
fn emit_semi_anti_join_exhaustion_handling(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table: &JoinedTable,
    table_index: usize,
) {
    let sa_meta = t_ctx.meta_semi_anti_joins[table_index]
        .as_ref()
        .expect("semi/anti-join must have SemiAntiJoinMetadata");
    let join_info = table.join_info.as_ref().unwrap();
    if join_info.is_semi() {
        program.add_comment(program.offset(), "semi-join: no match, skip outer row");
        program.emit_insn(Insn::Goto {
            target_pc: sa_meta.label_next_outer,
        });
    } else {
        // Anti-join: inner exhausted without match -> run body
        program.add_comment(program.offset(), "anti-join: no match, emit outer row");
        program.emit_insn(Insn::Goto {
            target_pc: sa_meta.label_body,
        });
    }
}

/// Emits outer join NULL row handling: checks the match flag, emits NullRow for
/// the right table cursors, and jumps back to re-evaluate post-join predicates.
fn emit_outer_join_null_handling(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table: &JoinedTable,
    table_index: usize,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
) {
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
    for cursor_id in [table_cursor_id, index_cursor_id].into_iter().flatten() {
        program.emit_insn(Insn::NullRow { cursor_id });
    }
    if let Table::FromClauseSubquery(from_clause_subquery) = &table.table {
        if let Some(start_reg) = from_clause_subquery.result_columns_start_reg {
            let column_count = from_clause_subquery.columns.len();
            if column_count > 0 {
                // Subqueries materialize their row into registers rather than being read back
                // through a cursor. NullRow only affects cursor reads, so we also have to
                // explicitly null out the cached registers or stale values would be re-emitted.
                program.emit_insn(Insn::Null {
                    dest: start_reg,
                    dest_end: Some(start_reg + column_count - 1),
                });
            }
        }
    }
    // Re-enter the loop body at match-flag set so
    // post-join predicates are re-evaluated with right-table NULLs.
    program.emit_insn(Insn::Goto {
        target_pc: lj_meta.label_match_flag_set_true,
    });
    program.preassign_label_to_next_insn(label_when_right_table_notnull);
}

#[allow(clippy::too_many_arguments)]
/// Closes one operation kind and returns the PC used for semi/anti-join routing.
fn close_loop_operation<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    table: &JoinedTable,
    table_index: usize,
    loop_labels: LoopLabels,
    mode: &OperationMode,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    select_plan: Option<&'a SelectPlan>,
) -> Result<BranchOffset> {
    match &table.op {
        Operation::Scan(scan) => Ok(close_scan_operation_loop(
            program,
            table,
            scan,
            loop_labels,
            mode,
            table_cursor_id,
            index_cursor_id,
        )),
        Operation::Search(search) => Ok(close_search_operation_loop(
            program,
            table,
            search,
            loop_labels,
            mode,
            table_cursor_id,
            index_cursor_id,
        )),
        Operation::IndexMethodQuery(_) => Ok(close_index_method_query_loop(
            program,
            loop_labels,
            index_cursor_id,
        )),
        Operation::HashJoin(hash_join_op) => close_hash_join_operation_loop(
            program,
            t_ctx,
            table_index,
            hash_join_op,
            loop_labels,
            table_cursor_id,
            select_plan,
        ),
        Operation::MultiIndexScan(_) => {
            Ok(close_multi_index_scan_operation_loop(program, loop_labels))
        }
    }
}

/// Returns true when this table is the probe side of LEFT/FULL hash join logic.
fn is_outer_hash_join_probe(table: &JoinedTable) -> bool {
    matches!(
        table.op,
        Operation::HashJoin(ref hj)
            if matches!(hj.join_type, HashJoinType::LeftOuter | HashJoinType::FullOuter)
    )
}

/// Emits `HashClose` for hash tables that are no longer needed.
fn close_hash_tables_if_needed(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    join_order: &[JoinOrderMember],
) {
    if program.is_nested() {
        return;
    }

    for join in join_order.iter() {
        let table_index = join.original_idx;
        let table = &tables.joined_tables()[table_index];
        if let Operation::HashJoin(hash_join_op) = &table.op {
            let build_table = &tables.joined_tables()[hash_join_op.build_table_idx];
            let hash_table_reg: usize = build_table.internal_id.into();
            if !program.should_keep_hash_table_open(hash_table_reg) {
                program.emit_insn(Insn::HashClose {
                    hash_table_id: hash_table_reg,
                });
                program.clear_hash_build_signature(hash_table_reg);
            }
        }
    }
}

/// Closes the loop for a given source operator.
/// For example in the case of a nested table scan, this means emitting the Next instruction
/// for all tables involved, innermost first.
pub fn close_loop<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    tables: &TableReferences,
    join_order: &[JoinOrderMember],
    mode: OperationMode,
    select_plan: Option<&'a SelectPlan>,
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

        // SEMI/ANTI-JOIN: emit Goto -> outer_next right after the body.
        // For semi-join: after body runs (one match found), skip inner's Next.
        // For anti-join: after body runs (inner exhausted), move to next outer row.
        let is_semi_or_anti = table
            .join_info
            .as_ref()
            .is_some_and(|ji| ji.is_semi_or_anti());
        if is_semi_or_anti {
            let sa_meta = t_ctx.meta_semi_anti_joins[table_index]
                .as_ref()
                .expect("semi/anti-join must have SemiAntiJoinMetadata");
            let comment = if table.join_info.as_ref().unwrap().is_semi() {
                "semi-join: early out after first match"
            } else {
                "anti-join: exit body, next outer row"
            };
            program.add_comment(program.offset(), comment);
            program.emit_insn(Insn::Goto {
                target_pc: sa_meta.label_next_outer,
            });
        }

        let (table_cursor_id, index_cursor_id) = table.resolve_cursors(program, mode.clone())?;

        let semi_anti_next_pc = close_loop_operation(
            program,
            t_ctx,
            table,
            table_index,
            loop_labels,
            &mode,
            table_cursor_id,
            index_cursor_id,
            select_plan,
        )?;

        resolve_semi_anti_join_next_labels_for_outer_table(
            program,
            t_ctx,
            table_index,
            semi_anti_next_pc,
        );

        if is_semi_or_anti {
            emit_semi_anti_join_exhaustion_handling(program, t_ctx, table, table_index);
        }

        // OUTER JOIN: may still need to emit NULLs for the right table.
        // Outer hash join probes are handled above via check_outer / unmatched scan.
        if table
            .join_info
            .as_ref()
            .is_some_and(|join_info| join_info.is_outer())
            && !is_outer_hash_join_probe(table)
        {
            emit_outer_join_null_handling(
                program,
                t_ctx,
                table,
                table_index,
                table_cursor_id,
                index_cursor_id,
            );
        }
    }

    // After ALL loops are closed, emit HashClose for any hash tables that were built.
    // This must happen at the very end because hash join probe loops may be nested
    // inside outer loops that re-enter them. Hash tables used by materialization
    // subplans can be kept open and are skipped here.
    //
    // When inside a nested subquery (correlated or non-correlated), skip HashClose
    // because the hash build is protected by Once and must persist across subquery
    // re-invocations. The hash table will be cleaned up by ProgramState::reset().
    close_hash_tables_if_needed(program, tables, join_order);

    Ok(())
}
