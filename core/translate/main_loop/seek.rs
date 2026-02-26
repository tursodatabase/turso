use super::*;

/// Builds the affinity mask used before comparing seek keys to index keys.
fn build_index_seek_affinity_mask(
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
    let Some(table) = table else {
        return Ok(());
    };
    let columns = table.columns();
    for i in 0..num_keys {
        let idx_col_pos = idx_col_offset + i;
        if idx_col_pos >= seek_index.columns.len() {
            break;
        }
        let idx_col = &seek_index.columns[idx_col_pos];
        let Some(table_col) = columns.get(idx_col.pos_in_table) else {
            continue;
        };
        let Some(type_def) = resolver
            .schema()
            .get_type_def(&table_col.ty_str, table.is_strict())
        else {
            continue;
        };
        let Some(encode_expr) = &type_def.encode else {
            continue;
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
        let affinities = build_index_seek_affinity_mask(idx, tables, seek_def, &seek_def.start);
        if affinities.chars().any(|c| c != affinity::SQLITE_AFF_NONE) {
            program.emit_insn(Insn::Affinity {
                start_reg,
                count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                affinities,
            });
        }
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
                let affinities =
                    build_index_seek_affinity_mask(idx, tables, seek_def, &seek_def.end);
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
    if is_index {
        match seek_def.end.op {
            SeekOp::GE { .. } => program.emit_insn(Insn::IdxGE {
                cursor_id: seek_cursor_id,
                start_reg,
                num_regs,
                target_pc: loop_end,
            }),
            SeekOp::GT => program.emit_insn(Insn::IdxGT {
                cursor_id: seek_cursor_id,
                start_reg,
                num_regs,
                target_pc: loop_end,
            }),
            SeekOp::LE { .. } => program.emit_insn(Insn::IdxLE {
                cursor_id: seek_cursor_id,
                start_reg,
                num_regs,
                target_pc: loop_end,
            }),
            SeekOp::LT => program.emit_insn(Insn::IdxLT {
                cursor_id: seek_cursor_id,
                start_reg,
                num_regs,
                target_pc: loop_end,
            }),
        }
    } else {
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: seek_cursor_id,
            dest: rowid_reg,
        });
        let affinity = tables
            .joined_tables()
            .iter()
            .find_map(|t| {
                t.columns()
                    .iter()
                    .find(|c| c.is_rowid_alias())
                    .map(|c| c.affinity())
            })
            .unwrap_or(Affinity::Numeric);
        let collation = program.curr_collation();
        match seek_def.end.op {
            SeekOp::GE { .. } => program.emit_insn(Insn::Ge {
                lhs: rowid_reg,
                rhs: start_reg,
                target_pc: loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity),
                collation,
            }),
            SeekOp::GT => program.emit_insn(Insn::Gt {
                lhs: rowid_reg,
                rhs: start_reg,
                target_pc: loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity),
                collation,
            }),
            SeekOp::LE { .. } => program.emit_insn(Insn::Le {
                lhs: rowid_reg,
                rhs: start_reg,
                target_pc: loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity),
                collation,
            }),
            SeekOp::LT => program.emit_insn(Insn::Lt {
                lhs: rowid_reg,
                rhs: start_reg,
                target_pc: loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity),
                collation,
            }),
        }
    }

    Ok(())
}

/// Open an ephemeral index cursor and build an automatic index on a table.
/// This is used as a last-resort to avoid a nested full table scan.
/// Returns whether a bloom filter was created.
pub(super) fn emit_autoindex(
    program: &mut ProgramBuilder,
    index: &Arc<Index>,
    table_cursor_id: CursorID,
    index_cursor_id: CursorID,
    table_has_rowid: bool,
    num_seek_keys: usize,
    seek_def: &SeekDef,
) -> Result<bool> {
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
    Ok(use_bloom_filter)
}
