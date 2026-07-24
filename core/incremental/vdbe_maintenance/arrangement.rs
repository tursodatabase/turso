use super::output::{DeltaSource, EmittedNodeOutput, NodeOutput};
use super::plan::OperatorStateDef;
use super::source::materialize_delta_source;
use super::stream::{
    btree_arrangement, open_ephemeral_delta, ArrangementHandle, ArrangementIdentityColumn,
    DeltaIdentity, EphemeralDelta,
};
use super::MaintenanceInput;
use crate::incremental::dag;
use crate::schema::Schema;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::{LimboError, Result};

/// Integrate one node's declared delta into its persistent output arrangement.
///
/// Interior arrangements preserve the producer identity so later binary joins
/// can compose binding-rowid tuples without losing provenance. A terminal
/// arrangement publishes its own rowid to satisfy the materialized-view sink's
/// one-rowid contract.
#[allow(clippy::too_many_arguments)]
fn emit_output_arrangement(
    program: &mut ProgramBuilder,
    view_name: &str,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
    arrangement_table_name: &str,
    schema: &Schema,
) -> Result<ArrangementHandle> {
    let identity_width = input.identity_width();
    if identity_width == 0 {
        return Err(LimboError::InternalError(
            "an arranged delta must carry a stable source identity".to_string(),
        ));
    }
    let extra_binding_rowid_width = input.value_start - identity_width;
    turso_assert!(
        (output.identity == input.identity || output.identity == DeltaIdentity::OperatorRowid)
            && output.width == input.width
            && output.binding_rowid_columns.len() == input.binding_rowid_columns.len()
            && output
                .binding_rowid_columns
                .iter()
                .map(Option::is_some)
                .eq(input.binding_rowid_columns.iter().map(Option::is_some))
            && input.weight_column == input.value_start + input.width
            && output.weight_column == output.value_start + output.width,
        "output arrangement must publish its planned identity, values, and weight"
    );

    let table = schema
        .get_btree_table(arrangement_table_name)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "output arrangement {arrangement_table_name} of materialized view {view_name} not found"
            ))
        })?;
    let index = schema
        .get_indices(arrangement_table_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "output arrangement {arrangement_table_name} of materialized view {view_name} has no index"
            ))
        })?;
    let expected_columns = identity_width + input.width + extra_binding_rowid_width + 1;
    if table.columns().len() != expected_columns {
        return Err(LimboError::InternalError(format!(
            "output arrangement {arrangement_table_name} has {} columns, expected {expected_columns}",
            table.columns().len()
        )));
    }

    let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: table_cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: 0,
    });
    let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: index_cursor,
        root_page: RegisterOrLiteral::Literal(index.root_page),
        db: 0,
    });

    // The relational arrangement key is `(source identity, full row value)`.
    // A stable source identity can change values during an update, and the
    // old/new images must integrate independently.
    let key_width = identity_width + input.width;
    let lookup_start = program.alloc_registers(key_width + 1);
    let input_values_start = lookup_start + identity_width;
    let arrangement_rowid_reg = lookup_start + key_width;
    let contribution_reg = program.alloc_register();
    let current_mult_reg = program.alloc_register();
    let new_mult_reg = program.alloc_register();
    let zero_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let is_new_reg = program.alloc_register();
    let previous_rowid_reg = program.alloc_register();
    // [source identity..., persisted values..., binding rowids..., multiplicity].
    let state_record_start = program.alloc_registers(expected_columns);
    let state_binding_rowids_start = state_record_start + key_width;
    let state_mult_reg = state_binding_rowids_start + extra_binding_rowid_width;
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    // [published identity..., binding rowids..., delta values..., delta weight].
    let output_record_start = program.alloc_registers(output.record_width());
    let output_values_start = output_record_start + output.value_start;
    let output_weight_reg = output_record_start + output.weight_column;
    let output_record_reg = program.alloc_register();
    let output_rowid_reg = program.alloc_register();

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let found_label = program.allocate_label();
    let update_label = program.allocate_label();
    let write_state_label = program.allocate_label();
    let emit_label = program.allocate_label();
    let corrupt_label = program.allocate_label();

    program.emit_int(0, zero_reg);
    program.emit_int(1, one_reg);
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    for column in 0..identity_width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: lookup_start + column,
            default: None,
        });
    }
    for column in 0..input.width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: input_values_start + column,
            default: None,
        });
    }
    for column in identity_width..input.value_start {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: state_binding_rowids_start + column - identity_width,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: contribution_reg,
        default: None,
    });
    program.emit_insn(Insn::Eq {
        lhs: contribution_reg,
        rhs: zero_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    program.emit_insn(Insn::Found {
        cursor_id: index_cursor,
        target_pc: found_label,
        record_reg: lookup_start,
        num_regs: key_width,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: table_cursor,
        rowid_reg: arrangement_rowid_reg,
        prev_largest_reg: previous_rowid_reg,
    });
    program.emit_insn(Insn::Copy {
        src_reg: contribution_reg,
        dst_reg: new_mult_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: one_reg,
        dst_reg: is_new_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: lookup_start,
        dst_reg: state_record_start,
        extra_amount: key_width - 1,
    });
    program.emit_insn(Insn::Goto {
        target_pc: write_state_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_insn(Insn::Copy {
        src_reg: zero_reg,
        dst_reg: is_new_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::IdxRowId {
        cursor_id: index_cursor,
        dest: arrangement_rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table_cursor,
        src_reg: arrangement_rowid_reg,
        target_pc: corrupt_label,
    });
    program.emit_insn(Insn::Column {
        cursor_id: table_cursor,
        column: key_width + extra_binding_rowid_width,
        dest: current_mult_reg,
        default: None,
    });
    program.emit_insn(Insn::Add {
        lhs: contribution_reg,
        rhs: current_mult_reg,
        dest: new_mult_reg,
    });
    program.emit_insn(Insn::Ne {
        lhs: new_mult_reg,
        rhs: zero_reg,
        target_pc: update_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::IdxDelete {
        start_reg: lookup_start,
        num_regs: key_width + 1,
        cursor_id: index_cursor,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: table_cursor,
        table_name: arrangement_table_name.to_string(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Goto {
        target_pc: emit_label,
    });

    program.preassign_label_to_next_insn(update_label);
    program.emit_insn(Insn::Copy {
        src_reg: lookup_start,
        dst_reg: state_record_start,
        extra_amount: key_width - 1,
    });

    program.preassign_label_to_next_insn(write_state_label);
    program.emit_insn(Insn::Copy {
        src_reg: new_mult_reg,
        dst_reg: state_mult_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: state_record_start as u16,
        count: expected_columns as u16,
        dest_reg: state_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: table_cursor,
        key_reg: arrangement_rowid_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: arrangement_table_name.to_string(),
    });
    // New rows need a primary-key index entry; replacing an existing table
    // row leaves its identity and index entry unchanged.
    program.emit_insn(Insn::Eq {
        lhs: is_new_reg,
        rhs: zero_reg,
        target_pc: emit_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: lookup_start as u16,
        count: (key_width + 1) as u16,
        dest_reg: index_record_reg as u16,
        index_name: Some(index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: index_cursor,
        record_reg: index_record_reg,
        unpacked_start: Some(lookup_start),
        unpacked_count: Some((key_width + 1) as u16),
        flags: IdxInsertFlags::new(),
    });

    program.preassign_label_to_next_insn(emit_label);
    if output.identity == input.identity {
        program.emit_insn(Insn::Copy {
            src_reg: lookup_start,
            dst_reg: output_record_start,
            extra_amount: identity_width - 1,
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: arrangement_rowid_reg,
            dst_reg: output_record_start,
            extra_amount: 0,
        });
    }
    if input.width > 0 {
        program.emit_insn(Insn::Copy {
            src_reg: input_values_start,
            dst_reg: output_values_start,
            extra_amount: input.width - 1,
        });
    }
    for (input_column, output_column) in input
        .binding_rowid_columns
        .iter()
        .zip(output.binding_rowid_columns.iter())
    {
        match (input_column, output_column) {
            (None, None) => {}
            (Some(input_column), Some(output_column))
                if *output_column < output.identity_width() =>
            {
                turso_assert!(
                    output.identity == input.identity && input_column == output_column,
                    "published binding-rowid identity must preserve its source slot"
                );
            }
            (Some(input_column), Some(output_column)) => {
                let source_reg = if *input_column < identity_width {
                    lookup_start + *input_column
                } else {
                    state_binding_rowids_start + *input_column - identity_width
                };
                program.emit_insn(Insn::Copy {
                    src_reg: source_reg,
                    dst_reg: output_record_start + *output_column,
                    extra_amount: 0,
                });
            }
            _ => turso_assert!(
                false,
                "output arrangement must preserve binding-rowid availability"
            ),
        }
    }
    program.emit_insn(Insn::Copy {
        src_reg: contribution_reg,
        dst_reg: output_weight_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: output_record_start as u16,
        count: output.record_width() as u16,
        dest_reg: output_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg: output_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: output_rowid_reg,
        record_reg: output_record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} output arrangement is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);

    let value_columns = (identity_width..identity_width + input.width).collect();
    let binding_rowid_columns = input
        .binding_rowid_columns
        .iter()
        .map(|column| {
            column.map(|column| {
                ArrangementIdentityColumn::Column(if column < identity_width {
                    column
                } else {
                    key_width + column - identity_width
                })
            })
        })
        .collect();
    let (arrangement_identity, identity_columns) = if output.identity == input.identity {
        (
            input.identity,
            (0..identity_width)
                .map(ArrangementIdentityColumn::Column)
                .collect(),
        )
    } else {
        (
            DeltaIdentity::OperatorRowid,
            vec![ArrangementIdentityColumn::RowId],
        )
    };
    Ok(btree_arrangement(
        table,
        arrangement_identity,
        identity_columns,
        binding_rowid_columns,
        value_columns,
        Some(key_width + extra_binding_rowid_width),
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn publish_node_output(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    output: EmittedNodeOutput,
    maintenance_input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
) -> Result<NodeOutput> {
    let (delta, native_arrangement) = output.into_parts();
    let Some(arrangement_def) = &operator_state.arrangement_table else {
        return NodeOutput::new(node_id, delta, native_arrangement, &operator_state.output);
    };
    if native_arrangement.is_some() {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} has both a native and explicit output arrangement"
        )));
    }
    let input = materialize_delta_source(
        program,
        view_name,
        node_id,
        &delta,
        &operator_state.output,
        maintenance_input,
    )?;
    let expected_identity = operator_state.output.emitted_identity;
    if input.identity != expected_identity {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} emits {:?}, expected {:?}",
            input.identity, expected_identity
        )));
    }
    let arranged_output = open_ephemeral_delta(
        program,
        &format!("{view_name}_arranged_delta_{node_id}"),
        operator_state.output.schema.as_ref().clone(),
        operator_state.output.published_identity,
        operator_state.output.binding_rowids.clone(),
        input.requires_positive_first,
    );
    let arrangement = emit_output_arrangement(
        program,
        view_name,
        &input,
        &arranged_output,
        &arrangement_def.table_name,
        schema,
    )?;
    NodeOutput::new(
        node_id,
        DeltaSource::Ephemeral(arranged_output),
        Some(arrangement),
        &operator_state.output,
    )
}
