use super::output::{DeltaSource, EmittedNodeOutput, NodeOutput, NodeOutputContract};
use super::stream::{base_arrangement, open_ephemeral_delta, DeltaIdentity, EphemeralDelta};
use super::MaintenanceInput;
use crate::incremental::dag;
use crate::schema::{BTreeTable, Schema};
use crate::sync::Arc;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{InsertFlags, Insn};
use crate::{LimboError, Result};

pub(super) fn scan_node_output(
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    contract: &NodeOutputContract,
    schema: &Schema,
) -> Result<EmittedNodeOutput> {
    let dag::OpNode::Scan { table, .. } = &dag.nodes[node_id] else {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} is not a scan"
        )));
    };
    let stored_weight_column = schema
        .is_materialized_view(&table.name)
        .then(|| table.columns().len());
    EmittedNodeOutput::new(
        node_id,
        DeltaSource::BaseTable {
            table: table.clone(),
            stored_weight_column,
        },
        Some(base_arrangement(table.clone(), stored_weight_column)),
        contract,
    )
}

pub(super) fn materialize_node_output(
    program: &mut ProgramBuilder,
    view_name: &str,
    source_node: dag::NodeId,
    output: &NodeOutput,
    input: MaintenanceInput,
) -> Result<EphemeralDelta> {
    materialize_delta_source(
        program,
        view_name,
        source_node,
        output.delta(),
        output.contract(),
        input,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn materialize_delta_source(
    program: &mut ProgramBuilder,
    view_name: &str,
    source_node: dag::NodeId,
    source: &DeltaSource,
    source_contract: &NodeOutputContract,
    input: MaintenanceInput,
) -> Result<EphemeralDelta> {
    let DeltaSource::BaseTable {
        table,
        stored_weight_column,
    } = source
    else {
        let DeltaSource::Ephemeral(channel) = source else {
            unreachable!()
        };
        return Ok(channel.clone());
    };
    let channel = open_ephemeral_delta(
        program,
        &format!("{view_name}_scan_delta_{source_node}"),
        source_contract.schema.as_ref().clone(),
        source_contract.emitted_identity,
        source_contract.binding_rowids.clone(),
        false,
    );
    emit_base_scan_delta(
        program,
        view_name,
        table,
        *stored_weight_column,
        input,
        &channel,
    )?;
    Ok(channel)
}

/// Emit a Scan node into its declared ephemeral delta channel.
fn emit_base_scan_delta(
    program: &mut ProgramBuilder,
    view_name: &str,
    base_table: &Arc<BTreeTable>,
    stored_weight_column: Option<usize>,
    input: MaintenanceInput,
    output: &EphemeralDelta,
) -> Result<()> {
    turso_assert!(
        output.identity == DeltaIdentity::BindingRowids(1)
            && output.binding_rowid_columns.as_ref() == [Some(0)]
            && output.value_start == 1
            && output.width == base_table.columns().len()
            && output.weight_column == output.width + 1,
        "scan delta channel must contain rowid, base columns, and weight"
    );
    let input_cursor_id = match input {
        MaintenanceInput::TransactionDelta => {
            let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                view_name: view_name.to_string(),
                table: base_table.clone(),
            });
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: 0,
                db: 0,
            });
            cursor_id
        }
        MaintenanceInput::BaseTable => {
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(base_table.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: base_table.root_page,
                db: 0,
            });
            cursor_id
        }
    };

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: input_cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    let record_start = program.alloc_registers(output.record_width());
    let weight_reg = record_start + output.weight_column;
    program.emit_insn(Insn::RowId {
        cursor_id: input_cursor_id,
        dest: record_start,
    });
    for column in 0..output.width {
        program.emit_column_or_rowid(
            input_cursor_id,
            column,
            record_start + output.value_start + column,
        );
    }
    match input {
        MaintenanceInput::TransactionDelta => {
            program.emit_insn(Insn::Column {
                cursor_id: input_cursor_id,
                column: base_table.columns().len(),
                dest: weight_reg,
                default: None,
            });
        }
        MaintenanceInput::BaseTable => {
            if let Some(column) = stored_weight_column {
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column,
                    dest: weight_reg,
                    default: None,
                });
            } else {
                program.emit_int(1, weight_reg);
            }
        }
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    let output_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg: output_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: output_rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Next {
        cursor_id: input_cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}
