use super::*;

pub(crate) struct AutoIndexResult {
    pub(crate) use_bloom_filter: bool,
}

pub(crate) struct AutoIndexBuild<'a, E> {
    pub(crate) index: &'a Arc<Index>,
    pub(crate) table_cursor_id: CursorID,
    pub(crate) index_cursor_id: CursorID,
    pub(crate) table_has_rowid: bool,
    pub(crate) num_seek_keys: usize,
    pub(crate) has_null_matching_key: bool,
    pub(crate) seek_def: &'a SeekDef<E>,
    pub(crate) affinity_str: Option<&'a Arc<String>>,
}

/// Open an ephemeral index cursor and build an automatic index on a table.
/// This is used as a last-resort to avoid a nested full table scan
/// Returns whether the matching seek may use the built-in bloom filter.
pub(crate) fn emit_autoindex<E>(
    program: &mut ProgramBuilder,
    build: AutoIndexBuild<'_, E>,
    mut emit_column: impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
) -> Result<AutoIndexResult> {
    let AutoIndexBuild {
        index,
        table_cursor_id,
        index_cursor_id,
        table_has_rowid,
        num_seek_keys,
        has_null_matching_key,
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
    // Rewind source table. An empty table must skip the loop body: reading
    // its unpositioned cursor would add an all-NULL entry that `IS NULL`
    // seeks then match.
    let label_ephemeral_build_loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: table_cursor_id,
        pc_if_empty: label_ephemeral_build_end,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_loop_start);
    // Emit all columns from source table that are needed in the ephemeral index.
    // Also reserve a register for the rowid if the source table has rowids.
    let num_regs_to_reserve = index.columns.len() + table_has_rowid as usize;
    let ephemeral_cols_start_reg = program.alloc_registers(num_regs_to_reserve);
    for (i, col) in index.columns.iter().enumerate() {
        let reg = ephemeral_cols_start_reg + i;
        emit_column(program, col.pos_in_table, reg)?;
    }
    if table_has_rowid {
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: ephemeral_cols_start_reg + index.columns.len(),
        });
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(ephemeral_cols_start_reg),
        count: to_u32(num_regs_to_reserve),
        dest_reg: to_u32(record_reg),
        index_name: Some(index.name.clone()),
        affinity_str: affinity_str.map(|s| (**s).clone()),
    });
    // Skip bloom filter for non-binary collations since it uses binary hashing.
    // Also skip it when any seek key component comes from a NULL-matching `IS`:
    // the probe treats a NULL key as "definitely absent", which would skip rows
    // whose key IS NULL, so such a seek never probes — and then building the
    // filter would be wasted work on every row.
    let use_bloom_filter = index.columns.iter().take(num_seek_keys).all(|col| {
        col.collation
            .is_none_or(|coll| matches!(coll, CollationSeq::Binary | CollationSeq::Unset))
    }) && seek_def.start.op.eq_only()
        && !has_null_matching_key;
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
        unpacked_count: Some(num_regs_to_reserve as u32),
        flags: IdxInsertFlags::new().use_seek(false),
    });
    program.emit_insn(Insn::Next {
        cursor_id: table_cursor_id,
        pc_if_next: label_ephemeral_build_loop_start,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_end);
    Ok(AutoIndexResult { use_bloom_filter })
}
