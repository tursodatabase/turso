use super::consolidate::ConsolidatedDelta;
use super::stream::{synthesized_view_table, DeltaIdentity};
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::Result;

/// Persistent materialized-view table at the root of a maintenance circuit.
pub(super) struct ViewSink {
    pub(super) root_page: i64,
    pub(super) num_columns: usize,
}

/// Consume a fully evaluated one-identity delta stream at the maintenance
/// program boundary. Operator code owns expression evaluation; this adapter
/// only binds the root's standard `(identity, values, weight)` contract to a
/// persistent view.
///
/// The view keeps one row per identity: its values and its weight. That is
/// only a z-set when the identity determines the values, so the merge checks
/// it: a delta row whose identity is already in the view must carry exactly
/// the stored values. An update of a row under one identity is a retraction
/// of the old values plus an insertion of the new ones, and the single slot
/// can hold the new values only once the old ones are gone. So the sink
/// applies the delta in two passes over the same rows, its retractions
/// first and its insertions second; the order of rows within the delta does
/// not matter.
pub(super) fn emit_terminal_delta(
    program: &mut ProgramBuilder,
    view_name: &str,
    input: &ConsolidatedDelta,
    sink: &ViewSink,
) -> Result<()> {
    turso_assert!(
        matches!(
            input.identity,
            DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid
        ) && input.weight_column == input.value_start + input.width,
        "terminal delta streams require one stable operator identity"
    );
    let num_output_columns = sink.num_columns;
    turso_assert!(
        input.width == num_output_columns && num_output_columns > 0,
        "terminal delta width does not match its sink"
    );

    let table = Arc::new(synthesized_view_table(
        view_name,
        sink.root_page,
        num_output_columns,
    )?);
    let view_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: view_cursor_id,
        root_page: RegisterOrLiteral::Literal(sink.root_page),
        db: 0,
    });

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let not_found_label = program.allocate_label();
    let merge_label = program.allocate_label();
    let write_label = program.allocate_label();
    let other_values_label = program.allocate_label();
    let negative_weight_label = program.allocate_label();
    // [identity, values.., weight]: values and weight form the view record.
    let row_start = program.alloc_registers(1 + num_output_columns + 1);
    let identity_reg = row_start;
    let values_start = row_start + 1;
    let weight_reg = values_start + num_output_columns;
    // [values.., stored weight]: the record the view must hold for this row.
    let expected_start = program.alloc_registers(num_output_columns + 1);
    let current_weight_reg = expected_start + num_output_columns;
    let expected_record_reg = program.alloc_register();
    let stored_record_reg = program.alloc_register();
    let new_weight_reg = program.alloc_register();
    let zero_reg = program.alloc_register();
    let record_reg = program.alloc_register();
    program.emit_int(0, zero_reg);

    // 1 while applying insertions, 0 while applying retractions.
    let insertion_pass_reg = program.alloc_register();
    let pass_label = program.allocate_label();
    let pass_done_label = program.allocate_label();
    program.emit_int(0, insertion_pass_reg);
    program.preassign_label_to_next_insn(pass_label);
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: pass_done_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: weight_reg,
        default: None,
    });
    // Skip the rows of the other pass.
    let this_pass_label = program.allocate_label();
    let insertion_row_label = program.allocate_label();
    program.emit_insn(Insn::Gt {
        lhs: weight_reg,
        rhs: zero_reg,
        target_pc: insertion_row_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::If {
        reg: insertion_pass_reg,
        target_pc: next_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: this_pass_label,
    });
    program.preassign_label_to_next_insn(insertion_row_label);
    program.emit_insn(Insn::IfNot {
        reg: insertion_pass_reg,
        target_pc: next_label,
        jump_if_null: false,
    });
    program.preassign_label_to_next_insn(this_pass_label);
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: 0,
        dest: identity_reg,
        default: None,
    });
    for column in 0..num_output_columns {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: values_start + column,
            default: None,
        });
    }

    program.emit_insn(Insn::SeekRowid {
        cursor_id: view_cursor_id,
        src_reg: identity_reg,
        target_pc: not_found_label,
    });
    program.emit_insn(Insn::Column {
        cursor_id: view_cursor_id,
        column: num_output_columns,
        dest: current_weight_reg,
        default: None,
    });
    // Records are encoded the same way for the same values, so equal bytes
    // mean equal values, with types (1 and 1.0 differ).
    program.emit_insn(Insn::Copy {
        src_reg: values_start,
        dst_reg: expected_start,
        extra_amount: num_output_columns - 1,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(expected_start),
        count: to_u32(num_output_columns + 1),
        dest_reg: to_u32(expected_record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::RowData {
        cursor_id: view_cursor_id,
        dest: stored_record_reg,
    });
    program.emit_insn(Insn::Ne {
        lhs: stored_record_reg,
        rhs: expected_record_reg,
        target_pc: other_values_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::Goto {
        target_pc: merge_label,
    });
    program.preassign_label_to_next_insn(not_found_label);
    program.emit_int(0, current_weight_reg);

    program.preassign_label_to_next_insn(merge_label);
    program.emit_insn(Insn::Add {
        lhs: weight_reg,
        rhs: current_weight_reg,
        dest: new_weight_reg,
    });
    program.emit_insn(Insn::Gt {
        lhs: new_weight_reg,
        rhs: zero_reg,
        target_pc: write_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::Lt {
        lhs: new_weight_reg,
        rhs: zero_reg,
        target_pc: negative_weight_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    // The weight dropped to zero. A consolidated delta has no zero weights,
    // so the row was found in the view.
    program.emit_insn(Insn::Delete {
        cursor_id: view_cursor_id,
        table_name: view_name.to_string(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Goto {
        target_pc: next_label,
    });

    program.preassign_label_to_next_insn(write_label);
    program.emit_insn(Insn::Copy {
        src_reg: new_weight_reg,
        dst_reg: weight_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(values_start),
        count: to_u32(num_output_columns + 1),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: view_cursor_id,
        key_reg: identity_reg,
        record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: view_name.to_string(),
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(pass_done_label);
    program.emit_insn(Insn::If {
        reg: insertion_pass_reg,
        target_pc: end_label,
        jump_if_null: false,
    });
    program.emit_int(1, insertion_pass_reg);
    program.emit_insn(Insn::Goto {
        target_pc: pass_label,
    });
    program.preassign_label_to_next_insn(other_values_label);
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: crate::error::SQLITE_ERROR,
        description: format!(
            "materialized view {view_name} received a row whose rowid already holds other values"
        ),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(negative_weight_label);
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: crate::error::SQLITE_ERROR,
        description: format!(
            "materialized view {view_name} received a retraction for a row it does not hold"
        ),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}
