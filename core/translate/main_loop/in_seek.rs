use super::*;
use crate::translate::expr::expr_type::TypeEnv;
use crate::translate::expr_context::TableMetadata;
use crate::translate::plan::InSeekPrefix;

/// Emit the start of the two-level loop for an `IN` search.
///
/// The outer loop reads one value from the `IN` list. The inner loop reads all
/// table or index rows that match that value.
/// The main loop and the unmatched-right read use this bytecode shape.
///
/// When `prefix` is not empty, the `IN` column follows equalities on the
/// leading columns of `index`. Each seek key is the prefix values followed by
/// one `IN` value. The prefix is evaluated once, before the outer loop, and a
/// NULL prefix value ends the loop at once because `=` never matches NULL.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_in_seek_start(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    resolver: &Resolver<'_>,
    index: Option<&Arc<Index>>,
    source: &InSeekSource,
    prefix: &[InSeekPrefix],
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
) -> Result<InSeekMetadata> {
    let source_cursor_id = open_in_seek_source_cursor(
        program,
        table_references,
        resolver,
        index,
        source,
        prefix.len(),
    )?;
    program.emit_insn(Insn::NullRow {
        cursor_id: source_cursor_id,
    });
    program.emit_insn(Insn::Rewind {
        cursor_id: source_cursor_id,
        pc_if_empty: loop_end,
    });

    // The prefix registers are followed by the register that holds each `IN` value.
    let key_start = program.alloc_registers(prefix.len() + 1);
    emit_in_seek_prefix(
        program,
        table_references,
        resolver,
        index,
        prefix,
        key_start,
        loop_end,
    )?;
    let num_regs = prefix.len() + 1;

    let outer_loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(outer_loop_start);
    let next_value = program.allocate_label();
    emit_in_seek_key(
        program,
        table_references,
        resolver,
        index,
        source,
        source_cursor_id,
        next_value,
        key_start + prefix.len(),
        prefix.len(),
    )?;
    if index.is_none() {
        program.emit_insn(Insn::SeekRowid {
            cursor_id: table_cursor_id.expect("a rowid IN search must have a table cursor"),
            src_reg: key_start,
            target_pc: next_value,
        });
    } else {
        let index_cursor_id =
            index_cursor_id.expect("an indexed IN search must have an index cursor");
        program.emit_insn(Insn::SeekGE {
            cursor_id: index_cursor_id,
            start_reg: key_start,
            num_regs,
            target_pc: next_value,
            is_index: true,
            eq_only: false,
            null_matching_mask: Default::default(),
        });
        program.preassign_label_to_next_insn(loop_start);
        program.emit_insn(Insn::IdxGT {
            cursor_id: index_cursor_id,
            start_reg: key_start,
            num_regs,
            target_pc: next_value,
        });
        if let Some(table_cursor_id) = table_cursor_id {
            program.emit_deferred_seek(index_cursor_id, table_cursor_id);
        }
    }

    Ok(InSeekMetadata {
        ephemeral_cursor_id: source_cursor_id,
        outer_loop_start,
        next_val_label: next_value,
    })
}

/// Evaluate the equality prefix of an `IN` seek into `key_start..` as seek key
/// components. A NULL value jumps to `loop_end`. An index holds encoded values
/// and compares with affinity-converted keys, so the prefix is prepared as for
/// any other index seek.
fn emit_in_seek_prefix(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    resolver: &Resolver<'_>,
    index: Option<&Arc<Index>>,
    prefix: &[InSeekPrefix],
    key_start: usize,
    loop_end: BranchOffset,
) -> Result<()> {
    if prefix.is_empty() {
        return Ok(());
    }
    for (i, term) in prefix.iter().enumerate() {
        let reg = key_start + i;
        translate_expr_no_constant_opt(
            program,
            Some(table_references),
            &term.expr,
            reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
        if !term.expr.is_nonnull(
            &TableMetadata::new(Some(table_references)),
            &TypeEnv::new(Some(table_references), resolver),
        ) {
            program.emit_insn(Insn::IsNull {
                reg,
                target_pc: loop_end,
            });
        }
    }
    let Some(index) = index else {
        return Ok(());
    };
    encode_seek_keys_for_custom_types(
        program,
        table_references,
        index,
        key_start,
        prefix.iter().map(|term| Some(&term.expr)),
        0,
        None,
        resolver,
    )?;
    let affinities: String = prefix
        .iter()
        .map(|term| {
            if term.affinity.expr_needs_no_affinity_change(&term.expr) {
                affinity::SQLITE_AFF_BLOB
            } else {
                term.affinity.aff_mask()
            }
        })
        .collect();
    if affinities.chars().any(|c| c != affinity::SQLITE_AFF_BLOB) {
        program.emit_insn(Insn::Affinity {
            start_reg: key_start,
            count: std::num::NonZeroUsize::new(prefix.len()).expect("the prefix is not empty"),
            affinities,
        });
    }
    Ok(())
}

/// Read the current `IN` value into `seek_reg`, which follows `prefix_len`
/// prefix registers. A NULL value matches nothing, so it jumps to
/// `next_value`. An index holds encoded values, so the key is encoded as for
/// any other index seek. Values of a literal list are encoded when they are
/// inserted into the ephemeral index, so they are not encoded again here.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_in_seek_key(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    resolver: &Resolver<'_>,
    index: Option<&Arc<Index>>,
    source: &InSeekSource,
    source_cursor_id: CursorID,
    next_value: BranchOffset,
    seek_reg: usize,
    prefix_len: usize,
) -> Result<()> {
    program.emit_insn(Insn::Column {
        cursor_id: source_cursor_id,
        column: 0,
        dest: seek_reg,
        default: None,
    });
    program.emit_insn(Insn::IsNull {
        reg: seek_reg,
        target_pc: next_value,
    });
    if let (Some(index), InSeekSource::Subquery { .. }) = (index, source) {
        encode_seek_keys_for_custom_types(
            program,
            table_references,
            index,
            seek_reg,
            [None],
            prefix_len,
            None,
            resolver,
        )?;
    }
    Ok(())
}

/// Open or reuse the ephemeral cursor that supplies RHS values for an IN-seek.
///
/// Literal lists are materialized once into a unique ephemeral index so both
/// ordinary `Search::InSeek` and multi-index OR branches can drive repeated
/// equality seeks from the same bytecode pattern. Each literal gets the
/// comparison affinity and, when the seek uses an index, is then encoded
/// before it is inserted, so two spellings of one value (`'12:00'` and
/// `'12:00:00'` for a time) become one entry and the matching rows are read
/// once. IN-subqueries already have an ephemeral cursor from subquery
/// translation, so they are reused directly. `prefix_len` is the number of
/// index columns before the one the `IN` constrains.
pub(super) fn open_in_seek_source_cursor(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    resolver: &Resolver<'_>,
    index: Option<&Arc<Index>>,
    source: &InSeekSource,
    prefix_len: usize,
) -> Result<CursorID> {
    match source {
        InSeekSource::LiteralList { values, affinity } => {
            let label_once_end = if values.iter().all(|value| value.is_constant(resolver)) {
                let label_once_end = program.allocate_label();
                program.emit_insn(Insn::Once {
                    target_pc_when_reentered: label_once_end,
                });
                Some(label_once_end)
            } else {
                None
            };
            let collation = index
                .as_ref()
                .and_then(|idx| idx.columns.get(prefix_len))
                .map_or(CollationSeq::BINARY, |c| c.collation);
            let ephemeral_index = Arc::new(Index {
                name: String::new(),
                table_name: String::new(),
                root_page: 0,
                columns: crate::alloc::try_vec![IndexColumn {
                    name: String::new(),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: 0,
                    collation,
                    explicit_collation: None,
                    default: None,
                    expr: None,
                }]?,
                unique: true,
                ephemeral: true,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
                constraint_name: None,
            });
            let eph_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(ephemeral_index));
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id: eph_cursor,
                is_table: false,
            });
            let val_reg = program.alloc_register();
            let record_reg = program.alloc_register();
            let affinity_str = affinity.aff_mask().to_string();
            for value in values.iter() {
                translate_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    value,
                    val_reg,
                    resolver,
                    NoConstantOptReason::InListEphemeral,
                )?;
                if let Some(index) = index {
                    encode_seek_keys_for_custom_types(
                        program,
                        table_references,
                        index,
                        val_reg,
                        [Some(value)],
                        prefix_len,
                        Some(&affinity_str),
                        resolver,
                    )?;
                }
                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(val_reg),
                    count: 1,
                    dest_reg: to_u32(record_reg),
                    index_name: None,
                    affinity_str: index.is_none().then(|| affinity_str.clone()),
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: eph_cursor,
                    record_reg,
                    unpacked_start: Some(val_reg),
                    unpacked_count: Some(1),
                    flags: IdxInsertFlags::new().no_op_duplicate(),
                });
            }
            if let Some(label_once_end) = label_once_end {
                program.preassign_label_to_next_insn(label_once_end);
            }
            Ok(eph_cursor)
        }
        InSeekSource::Subquery { cursor_id } => Ok(*cursor_id),
    }
}

/// Emit the end of the two-level loop for an `IN` search.
pub(super) fn emit_in_seek_end(
    program: &mut ProgramBuilder,
    index_cursor_id: Option<CursorID>,
    loop_start: BranchOffset,
    meta: &InSeekMetadata,
) {
    if let Some(index_cursor_id) = index_cursor_id {
        // An index key can identify more than one row. The inner loop reads all
        // matching rows before the outer loop advances to the next `IN` value.
        program.emit_insn(Insn::Next {
            cursor_id: index_cursor_id,
            pc_if_next: loop_start,
            fullscan: false,
            is_index: false,
        });
    }
    program.preassign_label_to_next_insn(meta.next_val_label);
    program.emit_insn(Insn::Next {
        cursor_id: meta.ephemeral_cursor_id,
        pc_if_next: meta.outer_loop_start,
        fullscan: false,
        is_index: false,
    });
}
