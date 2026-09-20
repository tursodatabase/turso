use super::*;

/// Open or reuse the ephemeral cursor that supplies RHS values for an IN-seek.
///
/// Literal lists are materialized once into a unique ephemeral index so both
/// ordinary `Search::InSeek` and multi-index OR branches can drive repeated
/// equality seeks from the same bytecode pattern. IN-subqueries already have an
/// ephemeral cursor from subquery translation, so they are reused directly.
pub(super) fn open_in_seek_source_cursor(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    resolver: &Resolver<'_>,
    index: Option<&Arc<Index>>,
    source: &InSeekSource,
) -> Result<CursorID> {
    match source {
        InSeekSource::LiteralList { values, affinity } => {
            // A list value that reads a row or a subquery result is a different value on
            // every pass through the enclosing loops, so its ephemeral has to be refilled
            // each time. Only a list that cannot change may be filled once.
            let label_once_end = if in_list_is_loop_invariant(values) {
                let label = program.allocate_label();
                program.emit_insn(Insn::Once {
                    target_pc_when_reentered: label,
                });
                Some(label)
            } else {
                None
            };
            let collation = index
                .as_ref()
                .and_then(|idx| idx.columns.first())
                .and_then(|c| c.collation);
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
                    default: None,
                    expr: None,
                }]?,
                unique: true,
                ephemeral: true,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
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
                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(val_reg),
                    count: 1,
                    dest_reg: to_u32(record_reg),
                    index_name: None,
                    affinity_str: Some(affinity_str.clone()),
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: eph_cursor,
                    record_reg,
                    unpacked_start: Some(val_reg),
                    unpacked_count: Some(1),
                    flags: IdxInsertFlags::new().no_op_duplicate(),
                });
            }
            if let Some(label) = label_once_end {
                program.preassign_label_to_next_insn(label);
            }
            Ok(eph_cursor)
        }
        InSeekSource::Subquery { cursor_id } => Ok(*cursor_id),
    }
}

/// Whether every value in an IN list keeps the same value for the whole query.
fn in_list_is_loop_invariant(values: &[Expr]) -> bool {
    values.iter().all(|value| {
        let mut invariant = true;
        let _ = walk_expr(value, &mut |expr: &Expr| -> Result<WalkControl> {
            if matches!(
                expr,
                Expr::Column { .. } | Expr::RowId { .. } | Expr::SubqueryResult { .. }
            ) {
                invariant = false;
                return Ok(WalkControl::SkipChildren);
            }
            Ok(WalkControl::Continue)
        });
        invariant
    })
}
