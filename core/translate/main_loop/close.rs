use super::*;
use crate::translate::main_loop::hash::{
    emit_hash_join_unmatched_build_rows, HashProbeCloseEmitter,
};

/// Represents final step of Loop emission
pub struct CloseLoop;

impl CloseLoop {
    pub fn emit<'a>(
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

            let (table_cursor_id, index_cursor_id) =
                table.resolve_cursors(program, mode.clone())?;
            // Track the "next iteration" offset for semi/anti-join label resolution.
            // For most operations this equals the loop_labels.next resolution offset;
            // HashJoin overrides it to point at the Gosub Return or HashNext instead.
            let mut semi_anti_next_pc = None;
            // Helper: resolve loop_labels.next and record its offset for semi/anti-join.
            let mut resolve_next = |program: &mut ProgramBuilder| {
                let pc = program.offset();
                program.resolve_label(loop_labels.next, pc);
                semi_anti_next_pc = Some(pc);
            };
            match &table.op {
                Operation::Scan(scan) => {
                    resolve_next(program);
                    match scan {
                        Scan::BTreeTable { iter_dir, .. } => {
                            let iteration_cursor_id = if let OperationMode::UPDATE(
                                UpdateRowSource::PrebuiltEphemeralTable {
                                    ephemeral_table_cursor_id,
                                    ..
                                },
                            ) = &mode
                            {
                                *ephemeral_table_cursor_id
                            } else {
                                index_cursor_id.unwrap_or_else(|| {
                                    table_cursor_id.expect(
                                        "Either ephemeral or index or table cursor must be opened",
                                    )
                                })
                            };
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
                        Scan::VirtualTable { .. } => {
                            program.emit_insn(Insn::VNext {
                                cursor_id: table_cursor_id
                                    .expect("Virtual tables do not support covering indexes"),
                                pc_if_next: loop_labels.loop_start,
                            });
                        }
                        Scan::Subquery { iter_dir } => {
                            // Check if this is a materialized CTE (EphemeralTable) or coroutine
                            if let Table::FromClauseSubquery(subquery) = &table.table {
                                if let Some(QueryDestination::EphemeralTable {
                                    cursor_id, ..
                                }) = subquery.plan.select_query_destination()
                                {
                                    if *iter_dir == IterationDirection::Backwards {
                                        program.emit_insn(Insn::Prev {
                                            cursor_id: *cursor_id,
                                            pc_if_prev: loop_labels.loop_start,
                                        });
                                    } else {
                                        program.emit_insn(Insn::Next {
                                            cursor_id: *cursor_id,
                                            pc_if_next: loop_labels.loop_start,
                                        });
                                    }
                                } else {
                                    turso_assert_eq!(
                                        *iter_dir,
                                        IterationDirection::Forwards,
                                        "coroutine-backed subqueries cannot scan backwards"
                                    );
                                    // Coroutine-based subquery - use Goto to Yield
                                    program.emit_insn(Insn::Goto {
                                        target_pc: loop_labels.loop_start,
                                    });
                                }
                            } else {
                                // A subquery has no cursor to call Next on, so it just emits a Goto
                                // to the Yield instruction, which in turn jumps back to the main loop of the subquery,
                                // so that the next row from the subquery can be read.
                                program.emit_insn(Insn::Goto {
                                    target_pc: loop_labels.loop_start,
                                });
                            }
                        }
                    }
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::Search(search) => {
                    // Materialized subqueries with ephemeral indexes are allowed
                    let is_materialized_subquery = matches!(
                        &table.table,
                        Table::FromClauseSubquery(_)
                    ) && matches!(search, Search::Seek { index: Some(idx), .. } if idx.ephemeral);
                    turso_assert_some!(
                        {
                            is_from_clause: !matches!(table.table, Table::FromClauseSubquery(_)),
                            is_materialized_subquery: is_materialized_subquery
                        },
                        "Subqueries do not support index seeks unless materialized"
                    );
                    resolve_next(program);
                    let iteration_cursor_id =
                        if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
                            ephemeral_table_cursor_id,
                            ..
                        }) = &mode
                        {
                            *ephemeral_table_cursor_id
                        } else if is_materialized_subquery {
                            // Table-backed materialized subquery seeks iterate the
                            // auxiliary ephemeral index cursor.
                            index_cursor_id.expect("materialized subquery must have index cursor")
                        } else {
                            index_cursor_id.unwrap_or_else(|| {
                                table_cursor_id.expect(
                                    "Either ephemeral or index or table cursor must be opened",
                                )
                            })
                        };
                    // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a Next instruction.
                    match search {
                        Search::RowidEq { .. } => {}
                        Search::Seek { seek_def, .. } => {
                            if seek_def.iter_dir == IterationDirection::Backwards {
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
                        Search::InSeek { index, .. } => {
                            let meta = t_ctx.meta_in_seeks[table_index]
                                .as_ref()
                                .expect("InSeek must have metadata");
                            let ephemeral_cursor_id = meta.ephemeral_cursor_id;
                            let outer_loop_start = meta.outer_loop_start;
                            let next_val_label = meta.next_val_label;

                            let can_have_multiple_matches = index.is_some();
                            if can_have_multiple_matches {
                                // Rowid InSeek uses SeekRowid, so one RHS key can produce at
                                // most one row. Index-backed InSeek can hit duplicates, so
                                // keep scanning the current key's match range before advancing
                                // the ephemeral cursor to the next IN value.
                                program.emit_insn(Insn::Next {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_next: loop_labels.loop_start,
                                });
                            }

                            // Once the current key is exhausted (or a seek found nothing),
                            // advance the outer ephemeral cursor and restart the equality seek.
                            program.resolve_label(next_val_label, program.offset());
                            program.emit_insn(Insn::Next {
                                cursor_id: ephemeral_cursor_id,
                                pc_if_next: outer_loop_start,
                            });
                        }
                    }
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::IndexMethodQuery(_) => {
                    resolve_next(program);
                    program.emit_insn(Insn::Next {
                        cursor_id: index_cursor_id.unwrap(),
                        pc_if_next: loop_labels.loop_start,
                    });
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::HashJoin(ref hash_join_op) => {
                    if let Some(hash_ctx) = t_ctx
                        .hash_table_contexts
                        .get(&hash_join_op.build_table_idx)
                        .cloned()
                    {
                        // Emit the close-loop teardown for a hash-join probe table.
                        semi_anti_next_pc = HashProbeCloseEmitter::new(
                            program,
                            t_ctx,
                            hash_join_op,
                            hash_ctx,
                            select_plan,
                            table_index,
                        )
                        .emit()?
                        .semi_anti_next_pc;
                    }

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
                        if let Some(hash_ctx) = t_ctx
                            .hash_table_contexts
                            .get(&hash_join_op.build_table_idx)
                            .cloned()
                        {
                            emit_hash_join_unmatched_build_rows(
                                program,
                                t_ctx,
                                hash_join_op,
                                &hash_ctx,
                                select_plan,
                                table_index,
                                probe_cursor_id,
                            )?;
                        }
                    }

                    // Grace hash join processing: process spilled partition pairs.
                    // At runtime, this is a no-op if the build side didn't spill.
                    if let Some(hash_ctx) = t_ctx
                        .hash_table_contexts
                        .get(&hash_join_op.build_table_idx)
                        .cloned()
                    {
                        emit_grace_hash_join_loop(
                            program,
                            t_ctx,
                            hash_join_op,
                            &hash_ctx,
                            select_plan,
                            probe_cursor_id,
                        )?;
                    }
                }
                Operation::MultiIndexScan(_) => {
                    // MultiIndexScan uses RowSetRead for iteration - the next is handled
                    // at the end of the RowSet read loop in emit_multi_index_scan_loop
                    resolve_next(program);
                    program.emit_insn(Insn::Goto {
                        target_pc: loop_labels.loop_start,
                    });
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
            }

            // Resolve any semi/anti-join "outer next" labels targeting this table.
            if let Some(pc) = semi_anti_next_pc {
                for meta in t_ctx.meta_semi_anti_joins.iter().flatten() {
                    if meta.outer_table_idx == table_index {
                        program.resolve_label(meta.label_next_outer, pc);
                    }
                }
            }

            // SEMI/ANTI-JOIN: after loop_end (inner loop exhausted).
            // Semi-join: no match found -> skip outer row (Goto -> next_outer).
            // Anti-join: no match found -> run body (Goto -> label_body, jumps backward).
            if is_semi_or_anti {
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

            // OUTER JOIN: may still need to emit NULLs for the right table.
            // Outer hash join probes are handled above via check_outer / unmatched scan.
            let is_outer_hash_join_probe = matches!(
                table.op,
                Operation::HashJoin(ref hj) if matches!(
                    hj.join_type,
                    HashJoinType::LeftOuter | HashJoinType::FullOuter
                )
            );
            if let Some(join_info) = table.join_info.as_ref() {
                if join_info.is_outer() && !is_outer_hash_join_probe {
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
        if !program.is_nested() {
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

        Ok(())
    }
}

pub(super) struct AutoIndexResult {
    pub(super) use_bloom_filter: bool,
}

/// Emit the grace hash join processing loop after the probe cursor is exhausted.
/// At runtime, HashGraceInit/Next are no-ops if the build side didn't spill.
fn emit_grace_hash_join_loop<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    hash_join_op: &HashJoinOp,
    hash_ctx: &HashCtx,
    select_plan: Option<&'a SelectPlan>,
    probe_cursor_id: CursorID,
) -> Result<()> {
    // Only emit grace loop for INNER and LEFT OUTER (v1 scope)
    if !matches!(
        hash_join_op.join_type,
        HashJoinType::Inner | HashJoinType::LeftOuter
    ) {
        return Ok(());
    }

    // Need probe_rowid_reg for grace processing
    let Some(probe_rowid_reg) = hash_ctx.probe_rowid_reg else {
        return Ok(());
    };

    // Don't emit grace loop for aggregate queries -- the aggregation happens
    // in the main loop body and can't be replayed in the grace loop.
    // These queries fall back to LRU-based partition loading.
    if let Some(plan) = select_plan {
        if !plan.aggregates.is_empty() {
            return Ok(());
        }
    }

    let hash_table_reg = hash_ctx.hash_table_reg;
    let match_reg = hash_ctx.match_reg;
    let payload_dest_reg = hash_ctx.payload_start_reg;
    let num_payload = hash_ctx.payload_columns.len();

    let grace_done = program.allocate_label();
    let grace_next_label = program.allocate_label();
    let grace_loop = program.allocate_label();

    // HashGraceInit: finalize probe spill + load first partition + return first match
    program.emit_insn(Insn::HashGraceInit {
        hash_table_id: to_u16(hash_table_reg),
        dest_reg: to_u16(match_reg),
        probe_rowid_dest: to_u16(probe_rowid_reg),
        payload_dest_reg: payload_dest_reg.map(to_u16),
        num_payload: to_u16(num_payload),
        target_pc: grace_done,
    });

    program.preassign_label_to_next_insn(grace_loop);

    // Re-position probe cursor for this probe row
    program.emit_insn(Insn::SeekRowid {
        cursor_id: probe_cursor_id,
        src_reg: probe_rowid_reg,
        target_pc: grace_next_label,
    });

    // Re-position build cursor if needed (when payload doesn't cover all columns)
    if let Some(build_cursor_id) = hash_ctx.build_cursor_id {
        program.emit_insn(Insn::SeekRowid {
            cursor_id: build_cursor_id,
            src_reg: match_reg,
            target_pc: grace_next_label,
        });
    }

    // Emit result row using the inner-loop Gosub subroutine if available.
    if let Some(plan) = select_plan {
        if let Some(gosub_reg) = hash_ctx.inner_loop_gosub_reg {
            if let Some(gosub_label) = hash_ctx.inner_loop_gosub_label {
                program.emit_insn(Insn::Gosub {
                    return_reg: gosub_reg,
                    target_pc: gosub_label,
                });
            }
        } else if let Some(reg_result_cols_start) = t_ctx.reg_result_cols_start {
            // For simple joins without a Gosub, emit result columns and ResultRow
            // directly. The cursors are positioned by SeekRowid and payload
            // registers are filled by HashGraceInit/Next.
            // Skip if the query uses aggregates (they can't be re-evaluated here).
            let has_aggregates = plan.result_columns.iter().any(|rc| rc.contains_aggregates);
            if !has_aggregates {
                let num_result_cols = plan.result_columns.len();
                for (i, rc) in plan.result_columns.iter().enumerate() {
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        &rc.expr,
                        reg_result_cols_start + i,
                        &t_ctx.resolver,
                    )?;
                }
                program.emit_insn(Insn::ResultRow {
                    start_reg: reg_result_cols_start,
                    count: num_result_cols,
                });
                // Respect LIMIT: decrement limit counter and jump to grace_done if exhausted
                if let Some(limit_ctx) = t_ctx.limit_ctx {
                    program.emit_insn(Insn::DecrJumpZero {
                        reg: limit_ctx.reg_limit,
                        target_pc: grace_done,
                    });
                }
            }
        }
    }

    // HashGraceNext: advance to next grace match
    program.resolve_label(grace_next_label, program.offset());
    program.emit_insn(Insn::HashGraceNext {
        hash_table_id: to_u16(hash_table_reg),
        dest_reg: to_u16(match_reg),
        probe_rowid_dest: to_u16(probe_rowid_reg),
        payload_dest_reg: payload_dest_reg.map(to_u16),
        num_payload: to_u16(num_payload),
        target_pc: grace_done,
    });
    program.emit_insn(Insn::Goto {
        target_pc: grace_loop,
    });

    program.preassign_label_to_next_insn(grace_done);
    Ok(())
}

pub(super) struct AutoIndexBuild<'a> {
    pub(super) index: &'a Arc<Index>,
    pub(super) table_cursor_id: CursorID,
    pub(super) index_cursor_id: CursorID,
    pub(super) table_has_rowid: bool,
    pub(super) num_seek_keys: usize,
    pub(super) seek_def: &'a SeekDef,
    pub(super) affinity_str: Option<&'a Arc<String>>,
}

/// Open an ephemeral index cursor and build an automatic index on a table.
/// This is used as a last-resort to avoid a nested full table scan
/// Returns the cursor id of the ephemeral index cursor.
pub(super) fn emit_autoindex(
    program: &mut ProgramBuilder,
    build: AutoIndexBuild<'_>,
) -> Result<AutoIndexResult> {
    let AutoIndexBuild {
        index,
        table_cursor_id,
        index_cursor_id,
        table_has_rowid,
        num_seek_keys,
        seek_def,
        affinity_str,
    } = build;
    turso_assert!(index.ephemeral, "index must be ephemeral", { "index_name": &index.name });
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
        program.emit_column_or_rowid(table_cursor_id, col.pos_in_table, reg);
    }
    if table_has_rowid {
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: ephemeral_cols_start_reg + index.columns.len(),
        });
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(ephemeral_cols_start_reg),
        count: to_u16(num_regs_to_reserve),
        dest_reg: to_u16(record_reg),
        index_name: Some(index.name.clone()),
        affinity_str: affinity_str.map(|s| (**s).clone()),
    });
    // Skip bloom filter for non-binary collations since it uses binary hashing.
    let use_bloom_filter = index.columns.iter().take(num_seek_keys).all(|col| {
        col.collation
            .is_none_or(|coll| matches!(coll, CollationSeq::Binary | CollationSeq::Unset))
    }) && seek_def.start.op.eq_only();
    if use_bloom_filter {
        program.emit_insn(Insn::FilterAdd {
            cursor_id: index_cursor_id,
            key_reg: ephemeral_cols_start_reg,
            num_keys: num_seek_keys,
        });
    }
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
    Ok(AutoIndexResult { use_bloom_filter })
}
