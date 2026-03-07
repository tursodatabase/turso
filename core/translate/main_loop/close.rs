use super::*;

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
                    Scan::Subquery => {
                        // Check if this is a materialized CTE (EphemeralTable) or coroutine
                        if let Table::FromClauseSubquery(subquery) = &table.table {
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
                let is_materialized_subquery = matches!(&table.table, Table::FromClauseSubquery(_))
                    && matches!(search, Search::Seek { index: Some(idx), .. } if idx.ephemeral);
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
                        // For materialized subqueries, use the index cursor
                        index_cursor_id.expect("materialized subquery must have index cursor")
                    } else {
                        index_cursor_id.unwrap_or_else(|| {
                            table_cursor_id
                                .expect("Either ephemeral or index or table cursor must be opened")
                        })
                    };
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
            Operation::IndexMethodQuery(_) => {
                resolve_next(program);
                program.emit_insn(Insn::Next {
                    cursor_id: index_cursor_id.unwrap(),
                    pc_if_next: loop_labels.loop_start,
                });
                program.preassign_label_to_next_insn(loop_labels.loop_end);
            }
            Operation::HashJoin(ref hash_join_op) => {
                // Probe table: emit logic for iterating through hash matches
                if let Some(hash_ctx) = t_ctx.hash_table_contexts.get(&hash_join_op.build_table_idx)
                {
                    let hash_table_reg = hash_ctx.hash_table_reg;
                    let match_reg = hash_ctx.match_reg;
                    let match_found_label = hash_ctx.match_found_label;
                    let hash_next_label = hash_ctx.hash_next_label;
                    let payload_dest_reg = hash_ctx.payload_start_reg;
                    let num_payload = hash_ctx.payload_columns.len();
                    let check_outer_label = hash_ctx.check_outer_label;
                    let build_cursor_id = hash_ctx.build_cursor_id;
                    let join_type = hash_ctx.join_type;
                    let inner_loop_gosub_reg = hash_ctx.inner_loop_gosub_reg;
                    let inner_loop_gosub_label = hash_ctx.inner_loop_gosub_label;
                    let inner_loop_skip_label = hash_ctx.inner_loop_skip_label;
                    let label_next_probe_row = program.allocate_label();

                    // End the inner-loop subroutine.
                    if let Some(gosub_reg) = inner_loop_gosub_reg {
                        // For semi/anti-joins inside a Gosub subroutine (LEFT/FULL
                        // OUTER hash join), the "next outer" jump must land on Return
                        // so the Gosub register routes us back to the correct caller
                        // (HashNext in the matched phase, HashNextUnmatched in the
                        // unmatched phase). If we jumped to HashNext directly, we'd
                        // escape the subroutine and loop forever.
                        semi_anti_next_pc = Some(program.offset());
                        program.emit_insn(Insn::Return {
                            return_reg: gosub_reg,
                            can_fallthrough: false,
                        });
                        if let Some(skip_label) = inner_loop_skip_label {
                            program.preassign_label_to_next_insn(skip_label);
                        }
                    }

                    // FULL OUTER exhaustion -> check_outer; otherwise -> next_probe_row.
                    let hash_next_target = if join_type == HashJoinType::FullOuter {
                        check_outer_label.unwrap_or(label_next_probe_row)
                    } else {
                        label_next_probe_row
                    };

                    if semi_anti_next_pc.is_none() {
                        semi_anti_next_pc = Some(program.offset());
                    }
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
                        program
                            .resolve_label(lj_meta.label_match_flag_check_value, program.offset());

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
                            emit_unmatched_row_conditions_and_loop(
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
                    if let (Some(hash_ctx), Some(plan)) = (
                        t_ctx.hash_table_contexts.get(&hash_join_op.build_table_idx),
                        select_plan,
                    ) {
                        let hash_table_reg = hash_ctx.hash_table_reg;
                        let match_reg = hash_ctx.match_reg;
                        let payload_dest_reg = hash_ctx.payload_start_reg;
                        let num_payload = hash_ctx.payload_columns.len();
                        let build_cursor_id = hash_ctx.build_cursor_id;

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

                        emit_unmatched_row_conditions_and_loop(
                            program,
                            t_ctx,
                            plan,
                            hash_join_op.build_table_idx,
                            table_index,
                            label_next_unmatched,
                            hash_ctx
                                .inner_loop_gosub_reg
                                .zip(hash_ctx.inner_loop_gosub_label),
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

/// Build the affinity string for an index seek, skipping positions where the
/// probe expression already has the right type (mirroring SQLite's optimization
/// via `sqlite3ExprNeedsNoAffinityChange`).
///
/// For each seek key position the index column affinity is checked against the
/// probe expression. If the expression already matches (e.g. a string literal
/// seeking a TEXT column), NONE is used so OP_Affinity becomes a no-op for that
/// slot. When every slot is NONE the caller can skip the instruction entirely.
fn index_seek_affinities(
    idx: &Index,
    tables: &TableReferences,
    seek_def: &SeekDef,
    seek_key: &SeekKey,
) -> String {
    let table = tables
        .joined_tables()
        .iter()
        .find(|jt| jt.table.get_name() == idx.table_name)
        .expect("index source table not found in table references");

    idx.columns
        .iter()
        .zip(seek_def.iter(seek_key))
        .map(|(ic, key_component)| {
            // Expression index columns (e.g. CREATE INDEX ON t(a+b)) have no
            // corresponding table column; their affinity is BLOB/NONE.
            let col_aff = if ic.expr.is_some() {
                Affinity::Blob
            } else {
                table
                    .table
                    .get_column_at(ic.pos_in_table)
                    .expect("index column position out of bounds")
                    .affinity()
            };
            match key_component {
                SeekKeyComponent::Expr(expr) if col_aff.expr_needs_no_affinity_change(expr) => {
                    affinity::SQLITE_AFF_NONE
                }
                _ => col_aff.aff_mask(),
            }
        })
        .collect()
}

/// Encodes seek key registers for custom type index columns.
/// Index entries store encoded values, so seek keys must be encoded to match.
/// `idx_col_offset` is the starting index column position (0 for start keys,
/// `prefix.len()` for end key's last component).
fn encode_seek_keys_for_custom_types(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    seek_index: &Arc<Index>,
    start_reg: usize,
    num_keys: usize,
    idx_col_offset: usize,
    resolver: &Resolver<'_>,
) -> crate::Result<()> {
    // First try by identifier (alias), then fall back to the underlying table
    // name.  Ephemeral auto-indexes store the base table name (e.g. "t1") in
    // `table_name`, but the table reference may use an alias (e.g. "a" in
    // `FROM t1 a`).  Without the fallback, self-joins and aliased tables would
    // fail to find the table, skipping the encode step entirely and causing a
    // seek-key / index-key mismatch.
    let table = tables
        .find_table_by_identifier(&seek_index.table_name)
        .or_else(|| tables.find_table_by_table_name(&seek_index.table_name));
    let table = match table {
        Some(t) => t,
        None => return Ok(()),
    };
    let columns = table.columns();
    for i in 0..num_keys {
        let idx_col_pos = idx_col_offset + i;
        if idx_col_pos >= seek_index.columns.len() {
            break;
        }
        let idx_col = &seek_index.columns[idx_col_pos];
        let table_col = match columns.get(idx_col.pos_in_table) {
            Some(c) => c,
            None => continue,
        };
        let type_def = match resolver
            .schema()
            .get_type_def(&table_col.ty_str, table.is_strict())
        {
            Some(td) => td,
            None => continue,
        };
        let encode_expr = match &type_def.encode {
            Some(e) => e,
            None => continue,
        };
        let reg = start_reg + i;
        let skip_label = program.allocate_label();
        program.emit_insn(crate::vdbe::insn::Insn::IsNull {
            reg,
            target_pc: skip_label,
        });
        crate::translate::expr::emit_type_expr(
            program,
            encode_expr,
            reg,
            reg,
            table_col,
            type_def,
            resolver,
        )?;
        program.resolve_label(skip_label, program.offset());
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
pub(super) fn emit_seek(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    seek_def: &SeekDef,
    t_ctx: &mut TranslateCtx,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_end: BranchOffset,
    seek_index: Option<&Arc<Index>>,
    use_bloom_filter: bool,
) -> Result<()> {
    let is_index = seek_index.is_some();
    if seek_def.prefix.is_empty() && matches!(seek_def.start.last_component, SeekKeyComponent::None)
    {
        // If there is no seek key, we start from the first or last row of the index,
        // depending on the iteration direction.
        //
        // Also, if we will encounter NULLs in the index at the beginning of iteration (Forward + Asc OR Backward + Desc)
        // then, we must explicitly skip them as seek always has some bound condition over indexed column (e.g. c < ?, c >= ?, ...)
        //
        // note that table seek has some rules to convert seek key values to the integer affinity + it has some logic to check for explicit NULL searches
        // so, we emit simple Rewind/Last in case of search over table BTree
        // (this is safe as table BTree keys are always non-null single integer)
        match seek_def.iter_dir {
            IterationDirection::Forwards => {
                if seek_index.is_some_and(|index| index.columns[0].order == SortOrder::Asc) {
                    program.emit_null(start_reg, None);
                    program.emit_insn(Insn::SeekGT {
                        is_index,
                        cursor_id: seek_cursor_id,
                        start_reg,
                        num_regs: 1,
                        target_pc: loop_end,
                    });
                } else {
                    program.emit_insn(Insn::Rewind {
                        cursor_id: seek_cursor_id,
                        pc_if_empty: loop_end,
                    });
                }
            }
            IterationDirection::Backwards => {
                if seek_index.is_some_and(|index| index.columns[0].order == SortOrder::Desc) {
                    program.emit_null(start_reg, None);
                    program.emit_insn(Insn::SeekLT {
                        is_index,
                        cursor_id: seek_cursor_id,
                        start_reg,
                        num_regs: 1,
                        target_pc: loop_end,
                    });
                } else {
                    program.emit_insn(Insn::Last {
                        cursor_id: seek_cursor_id,
                        pc_if_empty: loop_end,
                    });
                }
            }
        }
        return Ok(());
    };
    // We allocated registers for the full index key, but our seek key might not use the full index key.
    // See [crate::translate::optimizer::build_seek_def] for boundary rewrites (including NULL sentinels).
    for (i, key) in seek_def.iter(&seek_def.start).enumerate() {
        let reg = start_reg + i;
        match key {
            SeekKeyComponent::Expr(expr) => {
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
            SeekKeyComponent::Null => program.emit_null(reg, None),
            SeekKeyComponent::None => unreachable!("None component is not possible in iterator"),
        }
    }
    let num_regs = seek_def.size(&seek_def.start);

    // Encode seek keys for custom type columns so they match the encoded
    // values stored in the index.
    if let Some(idx) = seek_index {
        encode_seek_keys_for_custom_types(
            program,
            tables,
            idx,
            start_reg,
            num_regs,
            0,
            &t_ctx.resolver,
        )?;
    }

    if let Some(idx) = seek_index {
        let affinities = index_seek_affinities(idx, tables, seek_def, &seek_def.start);
        if affinities.chars().any(|c| c != affinity::SQLITE_AFF_NONE) {
            program.emit_insn(Insn::Affinity {
                start_reg,
                count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                affinities,
            });
        }
    }
    if let Some(idx) = seek_index {
        if use_bloom_filter {
            turso_assert!(
                idx.ephemeral,
                "bloom filter can only be used with ephemeral indexes"
            );
            program.emit_insn(Insn::Filter {
                cursor_id: seek_cursor_id,
                key_reg: start_reg,
                num_keys: num_regs,
                target_pc: loop_end,
            });
        }
    }
    match seek_def.start.op {
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
pub(super) fn emit_seek_termination(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    seek_def: &SeekDef,
    t_ctx: &mut TranslateCtx,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    seek_index: Option<&Arc<Index>>,
) -> Result<()> {
    let is_index = seek_index.is_some();
    if seek_def.prefix.is_empty() && matches!(seek_def.end.last_component, SeekKeyComponent::None) {
        program.preassign_label_to_next_insn(loop_start);
        // If we will encounter NULLs in the index at the end of iteration (Forward + Desc OR Backward + Asc)
        // then, we must explicitly stop before them as seek always has some bound condition over indexed column (e.g. c < ?, c >= ?, ...)
        match seek_def.iter_dir {
            IterationDirection::Forwards => {
                if seek_index.is_some_and(|index| index.columns[0].order == SortOrder::Desc) {
                    program.emit_null(start_reg, None);
                    program.emit_insn(Insn::IdxGE {
                        cursor_id: seek_cursor_id,
                        start_reg,
                        num_regs: 1,
                        target_pc: loop_end,
                    });
                }
            }
            IterationDirection::Backwards => {
                if seek_index.is_some_and(|index| index.columns[0].order == SortOrder::Asc) {
                    program.emit_null(start_reg, None);
                    program.emit_insn(Insn::IdxLE {
                        cursor_id: seek_cursor_id,
                        start_reg,
                        num_regs: 1,
                        target_pc: loop_end,
                    });
                }
            }
        }
        return Ok(());
    };

    // For all index key values apart from the last one, we are guaranteed to use the same values
    // as these values were emited from common prefix, so we don't need to emit them again.

    let num_regs = seek_def.size(&seek_def.end);
    let last_reg = start_reg + seek_def.prefix.len();
    match &seek_def.end.last_component {
        SeekKeyComponent::Expr(expr) => {
            translate_expr_no_constant_opt(
                program,
                Some(tables),
                expr,
                last_reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            // Encode the end key for custom type columns (prefix keys were
            // already encoded in emit_seek, only the last component is new).
            if let Some(idx) = seek_index {
                encode_seek_keys_for_custom_types(
                    program,
                    tables,
                    idx,
                    last_reg,
                    1,
                    seek_def.prefix.len(),
                    &t_ctx.resolver,
                )?;
            }
            // Apply affinity to the end key (same as we do for start key).
            // Without this, e.g. BETWEEN '15' AND '35' on a numeric column would
            // compare '35' as a string against numeric values, causing incorrect results.
            if let Some(idx) = seek_index {
                let affinities = index_seek_affinities(idx, tables, seek_def, &seek_def.end);
                if affinities.chars().any(|c| c != affinity::SQLITE_AFF_NONE) {
                    program.emit_insn(Insn::Affinity {
                        start_reg,
                        count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                        affinities,
                    });
                }
            }
            // If the seek termination key expression is not verifiably non-NULL, we need to check whether it is NULL,
            // and if so, jump to the loop end.
            // This is to avoid returning rows for e.g. SELECT * FROM t WHERE t.x > NULL,
            // which would erroneously return all rows from t, as NULL is lower than any non-NULL value in index key comparisons.
            if !expr.is_nonnull(tables) {
                program.emit_insn(Insn::IsNull {
                    reg: last_reg,
                    target_pc: loop_end,
                });
            }
        }
        SeekKeyComponent::Null => program.emit_null(last_reg, None),
        SeekKeyComponent::None => {}
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
            .find(|t| t.columns().iter().any(|c| c.is_rowid_alias()))
        {
            if let Some(rowid_col_idx) = table_ref.columns().iter().position(|c| c.is_rowid_alias())
            {
                Some(table_ref.columns()[rowid_col_idx].affinity())
            } else {
                Some(Affinity::Numeric)
            }
        } else {
            Some(Affinity::Numeric)
        };
    }
    match (is_index, seek_def.end.op) {
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

pub(super) struct AutoIndexResult {
    pub(super) use_bloom_filter: bool,
}

/// Open an ephemeral index cursor and build an automatic index on a table.
/// This is used as a last-resort to avoid a nested full table scan
/// Returns the cursor id of the ephemeral index cursor.
pub(super) fn emit_autoindex(
    program: &mut ProgramBuilder,
    index: &Arc<Index>,
    table_cursor_id: CursorID,
    index_cursor_id: CursorID,
    table_has_rowid: bool,
    num_seek_keys: usize,
    seek_def: &SeekDef,
) -> Result<AutoIndexResult> {
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
        affinity_str: None,
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
