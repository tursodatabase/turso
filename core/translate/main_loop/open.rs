use super::*;

/// Check whether an expression references any outer query table.
/// Returns true if any `Column` or `RowId` node in the expression tree refers
/// to a table that lives in an outer query scope.
fn expr_references_outer_query(expr: &Expr, table_references: &TableReferences) -> bool {
    let mut has_outer_ref = false;
    let _ = walk_expr(expr, &mut |e: &Expr| -> Result<WalkControl> {
        match e {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if table_references
                    .find_outer_query_ref_by_internal_id(*table)
                    .is_some()
                {
                    has_outer_ref = true;
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    has_outer_ref
}

/// Emit the hash table build phase for a hash join operation.
///
/// This scans the build input (either the base table or a materialized input)
/// and populates the hash table with matching rows and payload. The returned
/// metadata drives probe-side emission, including bloom filter use and whether
/// build-table seeks are permitted.
/// Uses a separate hash build cursor so the build scan does not interfere with
/// the probe cursor for the same table.
#[allow(clippy::too_many_arguments)]
fn emit_hash_join_build_phase(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    non_from_clause_subqueries: &[NonFromClauseSubquery],
    predicates: &[WhereTerm],
    hash_join_op: &HashJoinOp,
    hash_build_cursor_id: CursorID,
    hash_table_id: usize,
) -> Result<HashBuildPayloadInfo> {
    let build_table = &table_references.joined_tables()[hash_join_op.build_table_idx];
    let btree = build_table
        .btree()
        .expect("Hash join build table must be a BTree table");
    // If build input was materialized, we may use either rowids (SeekRowid) or
    // precomputed keys/payload depending on the materialization mode.
    let materialized_input = t_ctx
        .materialized_build_inputs
        .get(&hash_join_op.build_table_idx);
    let materialized_cursor_id = materialized_input.map(|input| input.cursor_id);

    let num_keys = hash_join_op.join_keys.len();
    // Determine comparison affinity for each join key
    let mut key_affinities = String::new();
    for join_key in hash_join_op.join_keys.iter() {
        let build_expr = join_key.get_build_expr(predicates);
        let probe_expr = join_key.get_probe_expr(predicates);
        let affinity = comparison_affinity(build_expr, probe_expr, Some(table_references), None);
        key_affinities.push(affinity.aff_mask());
    }

    // Extract collations for each join key by examining both sides of the comparison.
    // This follows SQLite's collation precedence rules using the original LHS/RHS
    // of the comparison, not the build/probe assignment (which can be swapped by the
    // optimizer based on ORDER BY or cost estimates).
    let collations: Vec<CollationSeq> = hash_join_op
        .join_keys
        .iter()
        .map(|join_key| {
            let (original_lhs, original_rhs) = match join_key.build_side {
                BinaryExprSide::Lhs => (
                    join_key.get_build_expr(predicates),
                    join_key.get_probe_expr(predicates),
                ),
                BinaryExprSide::Rhs => (
                    join_key.get_probe_expr(predicates),
                    join_key.get_build_expr(predicates),
                ),
            };
            resolve_comparison_collseq(original_lhs, original_rhs, table_references)
                .unwrap_or(CollationSeq::Binary)
        })
        .collect();

    // Bloom filter uses binary hashing; skip if any join key uses non-binary collation.
    let use_bloom_filter = hash_join_op.use_bloom_filter
        && collations
            .iter()
            .all(|c| matches!(c, CollationSeq::Binary | CollationSeq::Unset));

    let (payload_columns, payload_signature_columns, use_materialized_keys, allow_seek) =
        match materialized_input.map(|input| &input.mode) {
            Some(MaterializedBuildInputMode::KeyPayload {
                num_keys: payload_num_keys,
                payload_columns,
            }) => {
                turso_assert!(
                    *payload_num_keys == num_keys,
                    "materialized hash build input key count mismatch"
                );
                let payload_signature_columns = (0..payload_columns.len())
                    .map(|i| *payload_num_keys + i)
                    .collect();
                (
                    payload_columns.clone(),
                    payload_signature_columns,
                    true,
                    false,
                )
            }
            _ => {
                // Collect payload columns from the build table for normal hash builds.
                let payload_signature_columns: Vec<usize> =
                    build_table.col_used_mask.iter().collect();
                let payload_columns: Vec<MaterializedColumnRef> = payload_signature_columns
                    .iter()
                    .map(|col_idx| {
                        let column = build_table
                            .columns()
                            .get(*col_idx)
                            .expect("build table column missing");
                        MaterializedColumnRef::Column {
                            table_id: build_table.internal_id,
                            column_idx: *col_idx,
                            is_rowid_alias: column.is_rowid_alias(),
                        }
                    })
                    .collect();
                (payload_columns, payload_signature_columns, false, true)
            }
        };
    let bloom_filter_cursor_id = if use_materialized_keys {
        materialized_cursor_id.expect("materialized input cursor is required")
    } else {
        hash_build_cursor_id
    };

    let join_key_indices = hash_join_op
        .join_keys
        .iter()
        .map(|key| key.where_clause_idx)
        .collect::<Vec<_>>();
    let signature = HashBuildSignature {
        join_key_indices,
        payload_refs: payload_columns.clone(),
        key_affinities: key_affinities.clone(),
        use_bloom_filter,
        materialized_input_cursor: materialized_cursor_id,
        materialized_mode: materialized_input.as_ref().map(|input| match input.mode {
            MaterializedBuildInputMode::RowidOnly => MaterializedBuildInputModeTag::RowidOnly,
            MaterializedBuildInputMode::KeyPayload { .. } => MaterializedBuildInputModeTag::Payload,
        }),
    };
    if program.hash_build_signature_matches(hash_table_id, &signature) {
        return Ok(HashBuildPayloadInfo {
            payload_columns,
            key_affinities,
            use_bloom_filter,
            bloom_filter_cursor_id,
            allow_seek,
        });
    }
    if program.has_hash_build_signature(hash_table_id) {
        program.emit_insn(Insn::HashClose { hash_table_id });
        program.clear_hash_build_signature(hash_table_id);
    }

    let build_key_start_reg = program.alloc_registers(num_keys);
    let mut build_rowid_reg = None;
    let mut build_iter_cursor_id = hash_build_cursor_id;
    if let Some(input) = materialized_input {
        match &input.mode {
            MaterializedBuildInputMode::RowidOnly => {
                build_rowid_reg = Some(program.alloc_register());
                build_iter_cursor_id = input.cursor_id;
            }
            MaterializedBuildInputMode::KeyPayload { .. } => {
                build_iter_cursor_id = input.cursor_id;
            }
        }
    }

    let build_phase_cursor_id = if use_materialized_keys {
        build_iter_cursor_id
    } else {
        hash_build_cursor_id
    };

    // Create new loop for hash table build phase
    let build_loop_start = program.allocate_label();
    let build_loop_end = program.allocate_label();
    let skip_to_next = program.allocate_label();
    let label_hash_build_end = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: label_hash_build_end,
    });

    // This is a separate cursor from the regular table cursor so we can iterate
    // the build side without disturbing the probe cursor.
    if !use_materialized_keys {
        program.emit_insn(Insn::OpenRead {
            cursor_id: hash_build_cursor_id,
            root_page: btree.root_page,
            db: build_table.database_id,
        });
    }

    program.emit_insn(Insn::Rewind {
        cursor_id: build_iter_cursor_id,
        pc_if_empty: build_loop_end,
    });

    // Set cursor override so translate_expr uses the hash build cursor for this table
    if !use_materialized_keys {
        program.set_cursor_override(build_table.internal_id, hash_build_cursor_id);
    }

    program.preassign_label_to_next_insn(build_loop_start);

    if let (Some(rowid_reg), Some(input_cursor_id)) = (build_rowid_reg, materialized_cursor_id) {
        program.emit_column_or_rowid(input_cursor_id, 0, rowid_reg);
        program.emit_insn(Insn::SeekRowid {
            cursor_id: hash_build_cursor_id,
            src_reg: rowid_reg,
            target_pc: skip_to_next,
        });
    }

    if !use_materialized_keys {
        let build_only_mask =
            TableMask::from_table_number_iter([hash_join_op.build_table_idx].into_iter());
        for cond in predicates.iter() {
            let mask =
                table_mask_from_expr(&cond.expr, table_references, non_from_clause_subqueries)?;
            if !mask.contains_table(hash_join_op.build_table_idx)
                || !build_only_mask.contains_all(&mask)
            {
                continue;
            }
            // Skip correlated predicates that reference outer query tables.
            // The hash build is guarded by Once and only executes on the first
            // outer-loop iteration. Predicates that depend on outer cursor values
            // would produce a stale filter for subsequent iterations.
            if expr_references_outer_query(&cond.expr, table_references) {
                continue;
            }
            let jump_target_when_true = program.allocate_label();
            let condition_metadata = ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true,
                jump_target_when_false: skip_to_next,
                jump_target_when_null: skip_to_next,
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

    if use_materialized_keys {
        for idx in 0..num_keys {
            program.emit_column_or_rowid(build_phase_cursor_id, idx, build_key_start_reg + idx);
        }
    } else {
        for (idx, join_key) in hash_join_op.join_keys.iter().enumerate() {
            let build_expr = join_key.get_build_expr(predicates);
            let target_reg = build_key_start_reg + idx;
            translate_expr(
                program,
                Some(table_references),
                build_expr,
                target_reg,
                &t_ctx.resolver,
            )?;
        }
    }

    // Apply affinity conversion to build keys before hashing
    if let Some(count) = std::num::NonZeroUsize::new(num_keys) {
        program.emit_insn(Insn::Affinity {
            start_reg: build_key_start_reg,
            count,
            affinities: key_affinities.clone(),
        });
    }

    let num_payload = payload_columns.len();

    let payload_start_reg = if num_payload > 0 {
        let payload_reg = program.alloc_registers(num_payload);
        for (i, &col_idx) in payload_signature_columns.iter().enumerate() {
            program.emit_column_or_rowid(build_phase_cursor_id, col_idx, payload_reg + i);
        }
        Some(payload_reg)
    } else {
        None
    };

    let mut payload_info = HashBuildPayloadInfo {
        payload_columns,
        key_affinities: key_affinities.clone(),
        use_bloom_filter: false,
        bloom_filter_cursor_id,
        allow_seek,
    };

    if !use_materialized_keys {
        program.clear_cursor_override(build_table.internal_id);
    }

    // Insert current row into hash table with payload columns.
    program.emit_insn(Insn::HashBuild {
        data: Box::new(HashBuildData {
            cursor_id: build_phase_cursor_id,
            key_start_reg: build_key_start_reg,
            num_keys,
            hash_table_id,
            mem_budget: hash_join_op.mem_budget,
            collations,
            payload_start_reg,
            num_payload,
            track_matched: matches!(
                hash_join_op.join_type,
                HashJoinType::LeftOuter | HashJoinType::FullOuter
            ),
        }),
    });
    if use_bloom_filter {
        program.emit_insn(Insn::FilterAdd {
            cursor_id: bloom_filter_cursor_id,
            key_reg: build_key_start_reg,
            num_keys,
        });
        payload_info.use_bloom_filter = true;
    }

    program.preassign_label_to_next_insn(skip_to_next);
    program.emit_insn(Insn::Next {
        cursor_id: build_iter_cursor_id,
        pc_if_next: build_loop_start,
    });

    program.preassign_label_to_next_insn(build_loop_end);
    program.emit_insn(Insn::HashBuildFinalize { hash_table_id });
    program.record_hash_build_signature(hash_table_id, signature);

    program.preassign_label_to_next_insn(label_hash_build_end);
    Ok(payload_info)
}

/// Emit bytecode for a multi-index scan (OR-by-union or AND-by-intersection).
///
/// For *Union (OR)*: scans each index branch separately, collecting rowids into a RowSet
/// for deduplication, then reads from the RowSet to fetch actual rows.
///
/// For *Intersection (AND)*: uses two RowSets. First branch populates rowset1,
/// subsequent branches test against rowset1 and add found rows to rowset2.
/// Only rowids appearing in ALL branches end up in the final result.
#[allow(clippy::too_many_arguments)]
fn emit_multi_index_scan_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table: &JoinedTable,
    table_references: &TableReferences,
    multi_idx_op: &MultiIndexScanOp,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    _next: BranchOffset,
) -> Result<()> {
    let table_cursor_id = program.resolve_cursor_id(&CursorKey::table(table.internal_id));

    // Allocate register for rowid during collection and final fetch
    let rowid_reg = program.alloc_register();

    // For intersection, we need two rowsets (swap between them for multi-way intersection)
    // For union, we only need one rowset
    let is_intersection = multi_idx_op.set_op == SetOperation::Intersection;

    // Allocate registers for RowSets
    let rowset1_reg = program.alloc_register();
    let rowset2_reg = if is_intersection {
        program.alloc_register()
    } else {
        rowset1_reg // Union only uses one rowset
    };

    // Initialize RowSet(s) to NULL (empty)
    program.emit_insn(Insn::Null {
        dest: rowset1_reg,
        dest_end: None,
    });
    if is_intersection {
        program.emit_insn(Insn::Null {
            dest: rowset2_reg,
            dest_end: None,
        });
    }

    // For intersection, we track which rowset to read from and write to
    // First branch writes to rowset1, subsequent branches read from current and write to next
    let mut current_read_rowset = rowset1_reg;
    let mut current_write_rowset = if is_intersection {
        rowset2_reg
    } else {
        rowset1_reg
    };

    // Emit each index branch - collect rowids into RowSet
    for (branch_idx, branch) in multi_idx_op.branches.iter().enumerate() {
        let branch_loop_start = program.allocate_label();
        let branch_loop_end = program.allocate_label();
        let branch_next = program.allocate_label();

        // For intersection after first branch, we need a label for "found in previous rowset"
        let found_in_prev_label = if is_intersection && branch_idx > 0 {
            Some(program.allocate_label())
        } else {
            None
        };

        let seek_def = &branch.seek_def;
        let is_index = branch.index.is_some();
        let branch_cursor_id = if let Some(index) = &branch.index {
            program.resolve_cursor_id(&CursorKey::index(table.internal_id, index.clone()))
        } else {
            // Rowid-based access
            table_cursor_id
        };

        // Reuse regular seek emission so multi-index branches honor start/end bounds
        // and NULL-key behavior exactly the same as non-multi-index scans.
        let max_key_regs = seek_def
            .size(&seek_def.start)
            .max(seek_def.size(&seek_def.end))
            .max(1);
        let key_start_reg = program.alloc_registers(max_key_regs);
        emit_seek(
            program,
            table_references,
            seek_def,
            t_ctx,
            branch_cursor_id,
            key_start_reg,
            branch_loop_end,
            branch.index.as_ref(),
            false, // multi-index branches never use ephemeral autoindex bloom filters
        )?;
        emit_seek_termination(
            program,
            table_references,
            seek_def,
            t_ctx,
            branch_cursor_id,
            key_start_reg,
            branch_loop_start,
            branch_loop_end,
            branch.index.as_ref(),
        )?;

        // Get rowid from the branch cursor.
        if is_index {
            program.emit_insn(Insn::IdxRowId {
                cursor_id: branch_cursor_id,
                dest: rowid_reg,
            });
        } else {
            program.emit_insn(Insn::RowId {
                cursor_id: branch_cursor_id,
                dest: rowid_reg,
            });
        }

        // Note: For Union, we use RowSetAdd (not RowSetTest) for all branches.
        // Deduplication happens in RowSetRead via sort + dedup.
        // For Intersection, batch semantics are handled in the intersection-specific code.
        if is_intersection {
            if branch_idx == 0 {
                // First branch: just add to rowset1
                program.emit_insn(Insn::RowSetAdd {
                    rowset_reg: rowset1_reg,
                    value_reg: rowid_reg,
                });
            } else {
                // Subsequent branches: test against previous rowset
                program.emit_insn(Insn::RowSetTest {
                    rowset_reg: current_read_rowset,
                    pc_if_found: found_in_prev_label.unwrap(),
                    value_reg: rowid_reg,
                    batch: -1, // Test only, no insert
                });
                // Not found - skip to next
                program.emit_insn(Insn::Goto {
                    target_pc: branch_next,
                });
                // Found - add to result rowset
                program.preassign_label_to_next_insn(found_in_prev_label.unwrap());
                program.emit_insn(Insn::RowSetAdd {
                    rowset_reg: current_write_rowset,
                    value_reg: rowid_reg,
                });
            }
        } else {
            // Union: Just add all rowids, RowSetRead will deduplicate
            program.emit_insn(Insn::RowSetAdd {
                rowset_reg: rowset1_reg,
                value_reg: rowid_reg,
            });
        }

        program.preassign_label_to_next_insn(branch_next);
        emit_cursor_advance(
            program,
            branch_cursor_id,
            branch_loop_start,
            seek_def.iter_dir,
        );
        program.preassign_label_to_next_insn(branch_loop_end);

        // For intersection with more than 2 branches, swap the rowsets
        // so the next iteration reads from what we just wrote and writes to the other
        if is_intersection && branch_idx > 0 && branch_idx < multi_idx_op.branches.len() - 1 {
            std::mem::swap(&mut current_read_rowset, &mut current_write_rowset);
            // Re-initialize the write rowset for the next iteration
            program.emit_insn(Insn::Null {
                dest: current_write_rowset,
                dest_end: None,
            });
        }
    }

    // Determine which rowset to read from for final results
    let final_rowset = if is_intersection && multi_idx_op.branches.len() > 1 {
        // For intersection:
        // - Branch 0: writes to rowset1 (rowset1 is write-only, never tested)
        // - Branch 1: tests against rowset1, writes matches to rowset2
        // - Branch 2: tests against rowset2, writes matches to rowset1 (after swap)
        // - etc.
        //
        // After N branches, the results are in the rowset that branch N-1 wrote to.
        // Branch 0 writes to rowset1, branch 1 writes to rowset2, branch 2 writes to rowset1...
        // Number of swaps = N - 2 (swaps happen after branches 1, 2, ... N-2)
        //
        // For 2 branches: 0 swaps, final is rowset2 (branch 1 wrote there)
        // For 3 branches: 1 swap, final is rowset1 (branch 2 wrote there after swap)
        // For 4 branches: 2 swaps, final is rowset2
        //
        // Pattern: N branches -> (N-2) swaps
        // If (N-2) is even -> rowset2, if odd -> rowset1
        let num_swaps = multi_idx_op.branches.len().saturating_sub(2);
        if num_swaps % 2 == 0 {
            rowset2_reg
        } else {
            rowset1_reg
        }
    } else {
        rowset1_reg
    };

    // Now read from RowSet and fetch actual rows
    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::RowSetRead {
        rowset_reg: final_rowset,
        pc_if_empty: loop_end,
        dest_reg: rowid_reg,
    });

    // Seek to the row in the table
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table_cursor_id,
        src_reg: rowid_reg,
        target_pc: skip_label,
    });

    // Cache the rowid expression for later use
    let rowid_expr = Expr::RowId {
        database: None,
        table: table.internal_id,
    };
    t_ctx
        .resolver
        .expr_to_reg_cache
        .push((Cow::Owned(rowid_expr), rowid_reg, false));

    program.preassign_label_to_next_insn(skip_label);

    Ok(())
}

/// Emit Rewind/Last for a BTreeTable scan.
fn emit_btree_scan_entry(
    program: &mut ProgramBuilder,
    iter_dir: &IterationDirection,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    temp_cursor_id: Option<CursorID>,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
) {
    let iteration_cursor_id = temp_cursor_id.unwrap_or_else(|| {
        index_cursor_id.unwrap_or_else(|| {
            table_cursor_id.expect("Either ephemeral or index or table cursor must be opened")
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

/// Emit VFilter for a VirtualTable scan.
#[allow(clippy::too_many_arguments)]
fn emit_virtual_table_scan_entry(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table_references: &TableReferences,
    table_cursor_id: Option<CursorID>,
    idx_num: &i32,
    idx_str: &Option<String>,
    constraints: &[Expr],
    loop_start: BranchOffset,
    loop_end: BranchOffset,
) -> Result<()> {
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

    program.emit_insn(Insn::VFilter {
        cursor_id: table_cursor_id.expect("Virtual tables do not support covering indexes"),
        arg_count: args_needed,
        args_reg: start_reg,
        idx_str: maybe_idx_str,
        idx_num: *idx_num as usize,
        pc_if_empty: loop_end,
    });
    program.preassign_label_to_next_insn(loop_start);
    Ok(())
}

/// Emit InitCoroutine/Yield or Rewind for a FROM-clause subquery.
fn emit_subquery_scan_entry(
    program: &mut ProgramBuilder,
    from_clause_subquery: &crate::schema::FromClauseSubquery,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
) {
    match from_clause_subquery.plan.select_query_destination() {
        Some(QueryDestination::CoroutineYield {
            yield_reg,
            coroutine_implementation_start,
        }) => {
            program.emit_insn(Insn::InitCoroutine {
                yield_reg: *yield_reg,
                jump_on_definition: BranchOffset::Offset(0),
                start_offset: *coroutine_implementation_start,
            });
            program.preassign_label_to_next_insn(loop_start);
            program.emit_insn(Insn::Yield {
                yield_reg: *yield_reg,
                end_offset: loop_end,
            });
        }
        Some(QueryDestination::EphemeralTable { cursor_id, .. }) => {
            program.emit_insn(Insn::Rewind {
                cursor_id: *cursor_id,
                pc_if_empty: loop_end,
            });
            program.preassign_label_to_next_insn(loop_start);
            if let Some(start_reg) = from_clause_subquery.result_columns_start_reg {
                for col_idx in 0..from_clause_subquery.columns.len() {
                    program.emit_insn(Insn::Column {
                        cursor_id: *cursor_id,
                        column: col_idx,
                        dest: start_reg + col_idx,
                        default: None,
                    });
                }
            }
        }
        _ => unreachable!("Subquery table with unexpected query destination"),
    }
}

/// Emit seek/scan setup for a Search (RowidEq or index Seek) operation.
#[allow(clippy::too_many_arguments)]
fn emit_search_loop_entry(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    table: &JoinedTable,
    search: &Search,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    temp_cursor_id: Option<CursorID>,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    next: BranchOffset,
) -> Result<()> {
    let is_materialized_subquery = matches!(&table.table, Table::FromClauseSubquery(_))
        && matches!(search, Search::Seek { index: Some(idx), .. } if idx.ephemeral);

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
                cursor_id: table_cursor_id.expect("Search::RowidEq requires a table cursor"),
                src_reg,
                target_pc: next,
            });
        }
        Search::Seek {
            index, seek_def, ..
        } => {
            let mut bloom_filter = false;
            if let Some(index) = index {
                if index.ephemeral && !is_materialized_subquery {
                    let table_has_rowid =
                        matches!(&table.table, Table::BTree(btree) if btree.has_rowid);
                    let num_seek_keys = seek_def.size(&seek_def.start);
                    let use_bloom_filter = emit_autoindex(
                        program,
                        index,
                        table_cursor_id
                            .expect("an ephemeral index must have a source table cursor"),
                        index_cursor_id.expect("an ephemeral index must have an index cursor"),
                        table_has_rowid,
                        num_seek_keys,
                        seek_def,
                    )?;
                    bloom_filter = use_bloom_filter;
                }
            }

            let seek_cursor_id = if is_materialized_subquery {
                index_cursor_id.expect("materialized subquery must have index cursor")
            } else {
                temp_cursor_id.unwrap_or_else(|| {
                    index_cursor_id.unwrap_or_else(|| {
                        table_cursor_id
                            .expect("Either ephemeral or index or table cursor must be opened")
                    })
                })
            };

            let max_registers = seek_def
                .size(&seek_def.start)
                .max(seek_def.size(&seek_def.end));
            let start_reg = program.alloc_registers(max_registers);
            emit_seek(
                program,
                table_references,
                seek_def,
                t_ctx,
                seek_cursor_id,
                start_reg,
                loop_end,
                index.as_ref(),
                bloom_filter,
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
                index.as_ref(),
            )?;

            if is_materialized_subquery {
                if let Table::FromClauseSubquery(from_clause_subquery) = &table.table {
                    if let Some(start_reg) = from_clause_subquery.result_columns_start_reg {
                        let index_cursor =
                            index_cursor_id.expect("materialized subquery must have index cursor");
                        for col_idx in 0..from_clause_subquery.columns.len() {
                            program.emit_insn(Insn::Column {
                                cursor_id: index_cursor,
                                column: col_idx,
                                dest: start_reg + col_idx,
                                default: None,
                            });
                        }
                    }
                }
            } else if let Some(index_cursor_id) = index_cursor_id {
                if let Some(table_cursor_id) = table_cursor_id {
                    program.emit_insn(Insn::DeferredSeek {
                        index_cursor_id,
                        table_cursor_id,
                    });
                }
            }
        }
    }
    Ok(())
}

/// Emit loop opening for an IndexMethodQuery.
#[allow(clippy::too_many_arguments)]
fn emit_index_method_query_loop_entry(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table_references: &TableReferences,
    query: &crate::translate::plan::IndexMethodQuery,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
) -> Result<()> {
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
    Ok(())
}

/// Emit hash build + probe setup for a HashJoin operation.
#[allow(clippy::too_many_arguments)]
fn emit_hash_join_probe_loop_entry(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    hash_join_op: &HashJoinOp,
    table_cursor_id: Option<CursorID>,
    predicates: &[WhereTerm],
    subqueries: &mut [NonFromClauseSubquery],
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    next: BranchOffset,
    mode: &OperationMode,
    live_table_ids: &HashSet<TableInternalId>,
) -> Result<()> {
    let build_table = &table_references.joined_tables()[hash_join_op.build_table_idx];
    let (build_cursor_id, _) = build_table.resolve_cursors(program, mode.clone())?;
    let build_cursor_id = if let Some(cursor_id) = build_cursor_id {
        cursor_id
    } else {
        let btree = build_table
            .btree()
            .expect("Hash join build table must be a BTree table");
        let cursor_id = program.alloc_cursor_id_keyed_if_not_exists(
            CursorKey::table(build_table.internal_id),
            CursorType::BTreeTable(btree.clone()),
        );
        program.emit_insn(Insn::OpenRead {
            cursor_id,
            root_page: btree.root_page,
            db: build_table.database_id,
        });
        cursor_id
    };

    let hash_table_id: usize = build_table.internal_id.into();
    let btree = build_table
        .btree()
        .expect("Hash join build table must be a BTree table");
    let hash_build_cursor_id = program.alloc_cursor_id_keyed_if_not_exists(
        CursorKey::hash_build(build_table.internal_id),
        CursorType::BTreeTable(btree),
    );
    let payload_info = emit_hash_join_build_phase(
        program,
        t_ctx,
        table_references,
        subqueries,
        predicates,
        hash_join_op,
        hash_build_cursor_id,
        hash_table_id,
    )?;

    let num_keys = hash_join_op.join_keys.len();

    // Hash Table Probe Phase
    let probe_cursor_id = table_cursor_id.expect("Probe table must have a cursor");
    program.emit_insn(Insn::Rewind {
        cursor_id: probe_cursor_id,
        pc_if_empty: loop_end,
    });

    program.preassign_label_to_next_insn(loop_start);

    let probe_key_start_reg = program.alloc_registers(num_keys);
    for (idx, join_key) in hash_join_op.join_keys.iter().enumerate() {
        let probe_expr = join_key.get_probe_expr(predicates);
        translate_expr(
            program,
            Some(table_references),
            probe_expr,
            probe_key_start_reg + idx,
            &t_ctx.resolver,
        )?;
    }

    if let Some(count) = std::num::NonZeroUsize::new(num_keys) {
        program.emit_insn(Insn::Affinity {
            start_reg: probe_key_start_reg,
            count,
            affinities: payload_info.key_affinities.clone(),
        });
    }

    if payload_info.use_bloom_filter && hash_join_op.join_type != HashJoinType::FullOuter {
        program.emit_insn(Insn::Filter {
            cursor_id: payload_info.bloom_filter_cursor_id,
            target_pc: next,
            key_reg: probe_key_start_reg,
            num_keys,
        });
    }

    let num_payload = payload_info.payload_columns.len();
    let payload_dest_reg = if num_payload > 0 {
        Some(program.alloc_registers(num_payload))
    } else {
        None
    };

    if matches!(hash_join_op.join_type, HashJoinType::FullOuter) {
        let probe_table_idx = hash_join_op.probe_table_idx;
        if let Some(lj_meta) = t_ctx.meta_left_joins[probe_table_idx].as_ref() {
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: lj_meta.reg_match_flag,
            });
        }
    }

    let hash_probe_miss_label = if hash_join_op.join_type == HashJoinType::FullOuter {
        program.allocate_label()
    } else {
        next
    };

    let match_reg = program.alloc_register();
    program.emit_insn(Insn::HashProbe {
        hash_table_id: to_u16(hash_table_id),
        key_start_reg: to_u16(probe_key_start_reg),
        num_keys: to_u16(num_keys),
        dest_reg: to_u16(match_reg),
        target_pc: hash_probe_miss_label,
        payload_dest_reg: payload_dest_reg.map(to_u16),
        num_payload: to_u16(num_payload),
    });

    let match_found_label = program.allocate_label();
    program.preassign_label_to_next_insn(match_found_label);
    let hash_next_label = program.allocate_label();

    let payload_columns = payload_info.payload_columns.clone();
    t_ctx.hash_table_contexts.insert(
        hash_join_op.build_table_idx,
        HashCtx {
            hash_table_reg: hash_table_id,
            match_reg,
            payload_start_reg: payload_dest_reg,
            payload_columns: payload_info.payload_columns,
            build_cursor_id: if payload_info.allow_seek {
                Some(build_cursor_id)
            } else {
                None
            },
            join_type: hash_join_op.join_type,
            labels: HashJoinLabels {
                match_found_label,
                hash_next_label,
                check_outer_label: if hash_join_op.join_type == HashJoinType::FullOuter {
                    Some(hash_probe_miss_label)
                } else {
                    None
                },
                inner_loop_gosub_reg: None,
                inner_loop_gosub_label: None,
                inner_loop_skip_label: None,
            },
        },
    );

    // Add payload columns to resolver's cache
    t_ctx.resolver.enable_expr_to_reg_cache();
    let rowid_expr = Expr::RowId {
        database: None,
        table: build_table.internal_id,
    };
    let payload_has_build_rowid = payload_columns.iter().any(|payload| {
        matches!(
            payload,
            MaterializedColumnRef::RowId { table_id } if *table_id == build_table.internal_id
        )
    });
    let build_table_is_live = live_table_ids.contains(&build_table.internal_id);
    if payload_info.allow_seek && !payload_has_build_rowid && !build_table_is_live {
        t_ctx
            .resolver
            .expr_to_reg_cache
            .push((Cow::Owned(rowid_expr), match_reg, false));
    }
    if let Some(payload_reg) = payload_dest_reg {
        for (i, payload) in payload_columns.iter().enumerate() {
            let (payload_table_id, expr, is_column) = match payload {
                MaterializedColumnRef::Column {
                    table_id,
                    column_idx,
                    is_rowid_alias,
                } => (
                    *table_id,
                    Expr::Column {
                        database: None,
                        table: *table_id,
                        column: *column_idx,
                        is_rowid_alias: *is_rowid_alias,
                    },
                    true,
                ),
                MaterializedColumnRef::RowId { table_id } => (
                    *table_id,
                    Expr::RowId {
                        database: None,
                        table: *table_id,
                    },
                    false,
                ),
            };
            if live_table_ids.contains(&payload_table_id) {
                continue;
            }
            t_ctx
                .resolver
                .expr_to_reg_cache
                .push((Cow::Owned(expr), payload_reg + i, is_column));
        }
    } else if payload_info.allow_seek && !build_table_is_live {
        program.emit_insn(Insn::SeekRowid {
            cursor_id: build_cursor_id,
            src_reg: match_reg,
            target_pc: hash_next_label,
        });
    }
    Ok(())
}

/// Emit post-open join logic: conditions, match flags, anti-join exits, Gosub wrapping.
#[allow(clippy::too_many_arguments)]
fn emit_post_loop_entry_join_logic(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    table: &JoinedTable,
    joined_table_index: usize,
    join_index: usize,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    next: BranchOffset,
    subqueries: &mut [NonFromClauseSubquery],
) -> Result<()> {
    let condition_fail_target = if let Operation::HashJoin(ref hj) = table.op {
        t_ctx
            .hash_table_contexts
            .get(&hj.build_table_idx)
            .map(|ctx| ctx.labels.hash_next_label)
            .expect("should have hash context for build table")
    } else {
        next
    };
    let is_outer_hj_probe = matches!(table.op, Operation::HashJoin(ref hj) if matches!(
        hj.join_type,
        HashJoinType::LeftOuter | HashJoinType::FullOuter
    ));

    // Emit OUTER JOIN conditions (must run before setting match flags).
    emit_conditions_with_subqueries(
        program,
        &t_ctx.resolver,
        table_references,
        join_order,
        predicates,
        join_index,
        condition_fail_target,
        true,
        subqueries,
    )?;

    // Set the LEFT JOIN match flag. Skip outer hash join probes.
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

            if matches!(hj.join_type, HashJoinType::FullOuter) {
                let probe_idx = hj.probe_table_idx;
                if let Some(lj_meta) = t_ctx.meta_left_joins[probe_idx].as_ref() {
                    program.resolve_label(lj_meta.label_match_flag_set_true, program.offset());
                    program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: lj_meta.reg_match_flag,
                    });
                }
            }
        }
    }

    // Emit non-OUTER JOIN conditions.
    emit_conditions_with_subqueries(
        program,
        &t_ctx.resolver,
        table_references,
        join_order,
        predicates,
        join_index,
        condition_fail_target,
        false,
        subqueries,
    )?;

    // ANTI-JOIN: match found -> skip outer row.
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

    // Outer hash joins wrap inner loops in a Gosub subroutine.
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
            program.preassign_label_to_next_insn(gosub_label);

            if let Some(hash_ctx) = t_ctx.hash_table_contexts.get_mut(&hj.build_table_idx) {
                hash_ctx.labels.inner_loop_gosub_reg = Some(return_reg);
                hash_ctx.labels.inner_loop_gosub_label = Some(gosub_label);
                hash_ctx.labels.inner_loop_skip_label = Some(skip_label);
            }
        }
    }
    Ok(())
}

/// Resolves the previous anti-join body label when anti-joins are chained.
fn resolve_previous_chained_anti_join_body_label(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    join_index: usize,
) {
    if join_index == 0 {
        return;
    }
    let prev_table_idx = join_order[join_index - 1].original_idx;
    let prev_is_anti = table_references.joined_tables()[prev_table_idx]
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_anti());
    if prev_is_anti {
        if let Some(prev_sa_meta) = t_ctx.meta_semi_anti_joins[prev_table_idx].as_ref() {
            program.resolve_label(prev_sa_meta.label_body, program.offset());
        }
    }
}

/// Initializes the LEFT/FULL join match flag for one table to false.
fn reset_outer_join_match_flag(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table: &JoinedTable,
    joined_table_index: usize,
) {
    if let Some(join_info) = table.join_info.as_ref() {
        if join_info.is_outer() {
            let lj_meta = t_ctx.meta_left_joins[joined_table_index].as_ref().unwrap();
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: lj_meta.reg_match_flag,
            });
        }
    }
}

/// Emits `DeferredSeek` when both index and table cursors are active.
fn emit_scan_deferred_seek(
    program: &mut ProgramBuilder,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
) {
    if let Some(table_cursor_id) = table_cursor_id {
        if let Some(index_cursor_id) = index_cursor_id {
            program.emit_insn(Insn::DeferredSeek {
                index_cursor_id,
                table_cursor_id,
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
/// Opens one loop level for a specific operation kind.
fn emit_operation_loop_entry(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    table: &JoinedTable,
    predicates: &[WhereTerm],
    subqueries: &mut [NonFromClauseSubquery],
    temp_cursor_id: Option<CursorID>,
    mode: &OperationMode,
    live_table_ids: &HashSet<TableInternalId>,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    next: BranchOffset,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
) -> Result<()> {
    match &table.op {
        Operation::Scan(scan) => {
            match (scan, &table.table) {
                (Scan::BTreeTable { iter_dir, .. }, Table::BTree(_)) => {
                    emit_btree_scan_entry(
                        program,
                        iter_dir,
                        table_cursor_id,
                        index_cursor_id,
                        temp_cursor_id,
                        loop_start,
                        loop_end,
                    );
                }
                (
                    Scan::VirtualTable {
                        idx_num,
                        idx_str,
                        constraints,
                    },
                    Table::Virtual(_),
                ) => {
                    emit_virtual_table_scan_entry(
                        program,
                        t_ctx,
                        table_references,
                        table_cursor_id,
                        idx_num,
                        idx_str,
                        constraints,
                        loop_start,
                        loop_end,
                    )?;
                }
                (Scan::Subquery, Table::FromClauseSubquery(from_clause_subquery)) => {
                    emit_subquery_scan_entry(program, from_clause_subquery, loop_start, loop_end);
                }
                _ => unreachable!(
                    "{:?} scan cannot be used with {:?} table",
                    scan, table.table
                ),
            }
            emit_scan_deferred_seek(program, table_cursor_id, index_cursor_id);
        }
        Operation::Search(search) => {
            emit_search_loop_entry(
                program,
                t_ctx,
                table_references,
                table,
                search,
                table_cursor_id,
                index_cursor_id,
                temp_cursor_id,
                loop_start,
                loop_end,
                next,
            )?;
        }
        Operation::IndexMethodQuery(query) => {
            emit_index_method_query_loop_entry(
                program,
                t_ctx,
                table_references,
                query,
                table_cursor_id,
                index_cursor_id,
                loop_start,
                loop_end,
            )?;
        }
        Operation::HashJoin(hash_join_op) => {
            emit_hash_join_probe_loop_entry(
                program,
                t_ctx,
                table_references,
                hash_join_op,
                table_cursor_id,
                predicates,
                subqueries,
                loop_start,
                loop_end,
                next,
                mode,
                live_table_ids,
            )?;
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
                next,
            )?;
        }
    }
    Ok(())
}

/// Set up the main query execution loop
/// For example in the case of a nested table scan, this means emitting the Rewind instruction
/// for all tables involved, outermost first.
#[allow(clippy::too_many_arguments)]
pub fn open_loop(
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
        resolve_previous_chained_anti_join_body_label(
            program,
            t_ctx,
            table_references,
            join_order,
            join_index,
        );

        // Each OUTER JOIN has a "match flag" that is initially set to false,
        // and is set to true when a match is found for the OUTER JOIN.
        // This is used to determine whether to emit actual columns or NULLs for the columns of the right table.
        reset_outer_join_match_flag(program, t_ctx, table, joined_table_index);

        let (table_cursor_id, index_cursor_id) = table.resolve_cursors(program, mode.clone())?;

        emit_operation_loop_entry(
            program,
            t_ctx,
            table_references,
            table,
            predicates,
            subqueries,
            temp_cursor_id,
            &mode,
            &live_table_ids,
            loop_start,
            loop_end,
            next,
            table_cursor_id,
            index_cursor_id,
        )?;

        emit_post_loop_entry_join_logic(
            program,
            t_ctx,
            table_references,
            table,
            joined_table_index,
            join_index,
            join_order,
            predicates,
            next,
            subqueries,
        )?;
    }

    if subqueries.iter().any(|s| !s.has_been_evaluated()) {
        crate::bail_parse_error!(
            "all subqueries should have already been emitted, but found {} unevaluated subqueries",
            subqueries
                .iter()
                .filter(|s| !s.has_been_evaluated())
                .count()
        );
    }

    Ok(())
}
