use super::stream::{synthesized_view_table, DeltaIdentity, EphemeralDelta};
use crate::sync::Arc;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral};
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
pub(super) fn emit_terminal_delta(
    program: &mut ProgramBuilder,
    view_name: &str,
    input: &EphemeralDelta,
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
        input.width == num_output_columns,
        "terminal delta width does not match its sink"
    );

    let table = Arc::new(synthesized_view_table(
        view_name,
        sink.root_page,
        num_output_columns,
    ));
    let view_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: view_cursor_id,
        root_page: RegisterOrLiteral::Literal(sink.root_page),
        db: 0,
    });

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let row_start = program.alloc_registers(1 + num_output_columns + 1);
    let identity_reg = row_start;
    let values_start = row_start + 1;
    let weight_reg = values_start + num_output_columns;

    // Join phase decomposition can retract a cross-generation identity before
    // inserting it. Apply positive contributions first at the persistent
    // boundary so an unknown negative is never discarded before its matching
    // positive arrives.
    let pass_reg = input.requires_positive_first.then(|| {
        let pass_reg = program.alloc_register();
        program.emit_int(0, pass_reg);
        pass_reg
    });
    let pass_start_label = program.allocate_label();
    program.preassign_label_to_next_insn(pass_start_label);
    let pass_done_label = if pass_reg.is_some() {
        program.allocate_label()
    } else {
        end_label
    };
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: pass_done_label,
    });
    program.preassign_label_to_next_insn(loop_label);
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
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: weight_reg,
        default: None,
    });
    if let Some(pass_reg) = pass_reg {
        let negative_pass_label = program.allocate_label();
        let pass_ok_label = program.allocate_label();
        let zero_reg = program.alloc_register();
        program.emit_int(0, zero_reg);
        program.emit_insn(Insn::If {
            reg: pass_reg,
            target_pc: negative_pass_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Lt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: pass_ok_label,
        });
        program.preassign_label_to_next_insn(negative_pass_label);
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.preassign_label_to_next_insn(pass_ok_label);
    }

    {
        let not_found_label = program.allocate_label();
        let merge_label = program.allocate_label();
        let write_label = program.allocate_label();
        let delete_label = program.allocate_label();
        let current_weight_reg = program.alloc_register();
        let new_weight_reg = program.alloc_register();
        let found_reg = program.alloc_register();
        let zero_reg = program.alloc_register();
        program.emit_int(0, zero_reg);

        let view_key_reg = identity_reg;
        program.emit_insn(Insn::SeekRowid {
            cursor_id: view_cursor_id,
            src_reg: view_key_reg,
            target_pc: not_found_label,
        });
        program.emit_insn(Insn::Column {
            cursor_id: view_cursor_id,
            column: num_output_columns,
            dest: current_weight_reg,
            default: None,
        });
        program.emit_int(1, found_reg);
        program.emit_insn(Insn::Goto {
            target_pc: merge_label,
        });
        program.preassign_label_to_next_insn(not_found_label);
        program.emit_int(0, current_weight_reg);
        program.emit_int(0, found_reg);
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
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: found_reg,
            target_pc: delete_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });
        program.preassign_label_to_next_insn(delete_label);
        program.emit_insn(Insn::Delete {
            cursor_id: view_cursor_id,
            table_name: view_name.to_string(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });

        program.preassign_label_to_next_insn(write_label);
        // On the negative pass, retain the newer image installed by a
        // positive contribution instead of overwriting it with the
        // retracted old image.
        let values_ready_label = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: values_ready_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        for column in 0..num_output_columns {
            program.emit_insn(Insn::Column {
                cursor_id: view_cursor_id,
                column,
                dest: values_start + column,
                default: None,
            });
        }
        program.preassign_label_to_next_insn(values_ready_label);
        program.emit_insn(Insn::Copy {
            src_reg: new_weight_reg,
            dst_reg: weight_reg,
            extra_amount: 0,
        });
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: values_start as u16,
            count: (num_output_columns + 1) as u16,
            dest_reg: record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: view_cursor_id,
            key_reg: view_key_reg,
            record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: view_name.to_string(),
        });
    }

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    if let Some(pass_reg) = pass_reg {
        program.preassign_label_to_next_insn(pass_done_label);
        program.emit_insn(Insn::If {
            reg: pass_reg,
            target_pc: end_label,
            jump_if_null: false,
        });
        program.emit_int(1, pass_reg);
        program.emit_insn(Insn::Goto {
            target_pc: pass_start_label,
        });
    }
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}
