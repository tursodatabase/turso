use super::stream::{open_ephemeral_delta, EphemeralDelta};
use crate::alloc::TursoIteratorExt;
use crate::schema::PseudoCursorType;
use crate::translate::collate::CollationSeq;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, CmpInsFlags, InsertFlags, Insn, SorterOpenData};
use crate::Result;
use turso_parser::ast::SortOrder;

/// A delta in which every distinct row appears once, with the sum of its
/// weights, and no row has weight zero.
///
/// A delta is a z-set: the order of its rows carries no meaning, and a row
/// that is added and removed within one batch is not a change. Every
/// operator that merges a delta into state consumes this type, so no merge
/// can see a row that the batch also cancels, or depend on the order the
/// rows were produced in. Consumers must not depend on the order of its
/// rows either: persistent state is keyed by exact values (see
/// `vdbe_maintenance::members`), so an update from the integer 1 to the real
/// 1.0 changes two independent rows.
#[derive(Debug, Clone)]
pub(super) struct ConsolidatedDelta(EphemeralDelta);

impl std::ops::Deref for ConsolidatedDelta {
    type Target = EphemeralDelta;

    fn deref(&self) -> &EphemeralDelta {
        &self.0
    }
}

/// Sum the weights of equal rows of `input` and drop the rows whose weights
/// cancel.
///
/// Two rows are equal when they have the same identity and the same bytes in
/// every other column. Comparing bytes, not SQL values, keeps the integer 1
/// and the real 1.0 apart, and keeps collation-equal text like 'a' and 'A'
/// apart: they are different rows of the z-set. The rows are sorted by the
/// identity followed by one record holding the rest of the row, which
/// encodes each value with its exact type; equal rows end up next to each
/// other and are summed in one pass.
pub(super) fn consolidate_delta(
    program: &mut ProgramBuilder,
    name: &str,
    input: &EphemeralDelta,
) -> Result<ConsolidatedDelta> {
    let output = open_ephemeral_delta(
        program,
        &format!("{name}_consolidated"),
        input.schema.as_ref().clone(),
        input.identity,
        input
            .binding_rowid_columns
            .iter()
            .map(Option::is_some)
            .collect::<Vec<_>>()
            .into(),
    )?;
    turso_assert!(
        output.record_width() == input.record_width()
            && output.value_start == input.value_start
            && output.binding_rowid_columns == input.binding_rowid_columns
            && input.weight_column == input.record_width() - 1
            && input.identity_width() > 0,
        "a consolidated delta must keep the layout of its input, which starts with an identity"
    );

    // Sorter entry: [identity.., bytes of the rest of the row, rest of the
    // row.., weight]. The first `key_width` columns are the sort key.
    let identity_width = input.identity_width();
    let rest_width = input.weight_column - identity_width;
    let key_width = identity_width + 1;
    let entry_width = key_width + rest_width + 1;
    let sorter_cursor = program.alloc_cursor_id(CursorType::Sorter);
    program.emit_insn(Insn::SorterOpen {
        data: Box::new(SorterOpenData {
            cursor_id: sorter_cursor,
            columns: key_width,
            order_collations_nulls: (0..key_width)
                .map(|_| (SortOrder::Asc, CollationSeq::BINARY, None))
                .try_collect()?,
        }),
    });
    let entry_start = program.alloc_registers(entry_width);
    let entry_record_reg = program.alloc_register();
    let fill_done_label = program.allocate_label();
    let fill_loop_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: fill_done_label,
    });
    program.preassign_label_to_next_insn(fill_loop_label);
    for column in 0..identity_width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: entry_start + column,
            default: None,
        });
    }
    for column in 0..=rest_width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: identity_width + column,
            dest: entry_start + key_width + column,
            default: None,
        });
    }
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(entry_start + key_width),
        count: to_u32(rest_width),
        dest_reg: to_u32(entry_start + identity_width),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(entry_start),
        count: to_u32(entry_width),
        dest_reg: to_u32(entry_record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id: sorter_cursor,
        record_reg: entry_record_reg,
    });
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: fill_loop_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(fill_done_label);

    // Walk the sorted entries. A group of equal rows is open while
    // `group_open_reg` is 1: `group_entry_reg` holds its first entry and
    // `group_start` its output row, whose weight is the running sum.
    let sorted_entry_reg = program.alloc_register();
    let pseudo_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: entry_width,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: sorted_entry_reg,
        num_fields: entry_width,
    });
    let group_entry_reg = program.alloc_register();
    let group_open_reg = program.alloc_register();
    let group_start = program.alloc_registers(output.record_width());
    let group_weight_reg = group_start + output.weight_column;
    let weight_reg = program.alloc_register();
    let zero_reg = program.alloc_register();
    let output_rowid_reg = program.alloc_register();
    let output_record_reg = program.alloc_register();
    let flush_return_reg = program.alloc_register();
    program.emit_int(0, group_open_reg);
    program.emit_int(0, zero_reg);

    let sorted_loop_label = program.allocate_label();
    let sorted_next_label = program.allocate_label();
    let sorted_done_label = program.allocate_label();
    let new_group_label = program.allocate_label();
    let start_group_label = program.allocate_label();
    let flush_label = program.allocate_label();
    let flush_return_label = program.allocate_label();
    let end_label = program.allocate_label();

    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter_cursor,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(sorted_loop_label);
    // SorterCompare reads the sorter's current entry, and SorterData moves
    // that entry out of the sorter, so compare first.
    program.emit_insn(Insn::IfNot {
        reg: group_open_reg,
        target_pc: start_group_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::SorterCompare {
        cursor_id: sorter_cursor,
        pc_when_nonequal: new_group_label,
        sorted_record_reg: group_entry_reg,
        num_regs: key_width,
    });
    program.emit_insn(Insn::SorterData {
        cursor_id: sorter_cursor,
        dest_reg: sorted_entry_reg,
        pseudo_cursor,
    });
    program.emit_insn(Insn::Column {
        cursor_id: pseudo_cursor,
        column: entry_width - 1,
        dest: weight_reg,
        default: None,
    });
    program.emit_insn(Insn::Add {
        lhs: group_weight_reg,
        rhs: weight_reg,
        dest: group_weight_reg,
    });
    program.emit_insn(Insn::Goto {
        target_pc: sorted_next_label,
    });

    program.preassign_label_to_next_insn(new_group_label);
    program.emit_insn(Insn::Gosub {
        target_pc: flush_label,
        return_reg: flush_return_reg,
    });
    program.preassign_label_to_next_insn(start_group_label);
    program.emit_insn(Insn::SorterData {
        cursor_id: sorter_cursor,
        dest_reg: sorted_entry_reg,
        pseudo_cursor,
    });
    program.emit_insn(Insn::Column {
        cursor_id: pseudo_cursor,
        column: entry_width - 1,
        dest: group_weight_reg,
        default: None,
    });
    program.emit_insn(Insn::Copy {
        src_reg: sorted_entry_reg,
        dst_reg: group_entry_reg,
        extra_amount: 0,
    });
    for column in 0..identity_width {
        program.emit_insn(Insn::Column {
            cursor_id: pseudo_cursor,
            column,
            dest: group_start + column,
            default: None,
        });
    }
    for column in 0..rest_width {
        program.emit_insn(Insn::Column {
            cursor_id: pseudo_cursor,
            column: key_width + column,
            dest: group_start + identity_width + column,
            default: None,
        });
    }
    program.emit_int(1, group_open_reg);

    program.preassign_label_to_next_insn(sorted_next_label);
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter_cursor,
        pc_if_next: sorted_loop_label,
    });
    program.preassign_label_to_next_insn(sorted_done_label);
    program.emit_insn(Insn::Gosub {
        target_pc: flush_label,
        return_reg: flush_return_reg,
    });
    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });

    // Write the open group's row unless its weights cancelled.
    program.preassign_label_to_next_insn(flush_label);
    program.emit_insn(Insn::Eq {
        lhs: group_weight_reg,
        rhs: zero_reg,
        target_pc: flush_return_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg: output_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(group_start),
        count: to_u32(output.record_width()),
        dest_reg: to_u32(output_record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: output_rowid_reg,
        record_reg: output_record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.preassign_label_to_next_insn(flush_return_label);
    program.emit_insn(Insn::Return {
        return_reg: flush_return_reg,
        can_fallthrough: false,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(ConsolidatedDelta(output))
}
