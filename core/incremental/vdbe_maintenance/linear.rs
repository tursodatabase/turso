use super::binding::{remap_bound_expr, seed_ephemeral_stream_cache, stream_table_references};
use super::output::{EmittedNodeOutput, NodeOutputContract};
use super::stream::{emit_ephemeral_delta_row, open_ephemeral_delta, EphemeralDelta};
use crate::incremental::dag;
use crate::schema::Schema;
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    translate_condition_expr, translate_expr_no_constant_opt, ConditionMetadata,
    NoConstantOptReason,
};
use crate::turso_assert;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::{to_u32, InsertFlags, Insn};
use crate::{Connection, LimboError, Result};
use turso_parser::ast;

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_filter(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    output_contract: &NodeOutputContract,
    predicate: &ast::Expr,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<EmittedNodeOutput> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_filter_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;

    let syms = connection.syms.read();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
        &None,
    );
    let (table_references, binding_remap) = stream_table_references(program, &input.schema);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let pass_label = program.allocate_label();
    turso_assert!(
        output.identity == input.identity
            && output.binding_rowid_columns == input.binding_rowid_columns
            && output.value_start == input.value_start
            && output.weight_column == input.weight_column,
        "filter output must preserve its complete input stream contract"
    );
    let record_start = program.alloc_registers(output.record_width());
    let record_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    seed_ephemeral_stream_cache(
        program,
        input,
        &binding_remap,
        &table_references,
        &mut resolver,
    )?;
    let bound_predicate = remap_bound_expr(predicate, &binding_remap)?;
    translate_condition_expr(
        program,
        &table_references,
        &bound_predicate,
        ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: pass_label,
            jump_target_when_false: next_label,
            jump_target_when_null: next_label,
        },
        &resolver,
    )?;
    program.preassign_label_to_next_insn(pass_label);

    for column in 0..input.weight_column {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: record_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: record_start + output.weight_column,
        default: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(record_start),
        count: to_u32(output.record_width()),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    EmittedNodeOutput::from_ephemeral(node_id, output, output_contract)
}

fn append_alias_values(
    program: &mut ProgramBuilder,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
) -> Result<()> {
    if input.width != output.width
        || output.identity_width() != input.identity_width()
        || output.value_start != output.identity_width()
        || output.binding_rowid_columns.iter().any(Option::is_some)
    {
        return Err(LimboError::InternalError(
            "alias stream copy has incompatible input/output layouts".to_string(),
        ));
    }

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let record_start = program.alloc_registers(output.record_width());
    let record_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    for column in 0..output.identity_width() {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: record_start + column,
            default: None,
        });
    }
    for column in 0..input.width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: record_start + output.value_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: record_start + output.weight_column,
        default: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(record_start),
        count: to_u32(output.record_width()),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}

pub(super) fn emit_alias(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    output_contract: &NodeOutputContract,
) -> Result<EmittedNodeOutput> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_alias_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    append_alias_values(program, input, &output)?;
    EmittedNodeOutput::from_ephemeral(node_id, output, output_contract)
}

/// Extend each input row with NULL columns: `before ++ input ++ after`. The
/// bindings of the NULL columns get a NULL rowid.
pub(super) fn emit_pad(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    before: &dag::StreamSchema,
    output_contract: &NodeOutputContract,
) -> Result<EmittedNodeOutput> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_pad_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    let input_bindings = before.bindings.len()..before.bindings.len() + input.schema.bindings.len();
    turso_assert!(
        output.identity_width() == input.identity_width()
            && before.len() + input.width <= output.width
            && input_bindings.end <= output.binding_rowid_columns.len(),
        "padded stream must contain its input stream"
    );

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let identity_start = program.alloc_registers(input.identity_width());
    let values_start = program.alloc_registers(output.width);
    let null_reg = program.alloc_register();
    let weight_reg = program.alloc_register();
    let binding_rowid_regs = output
        .binding_rowid_columns
        .iter()
        .enumerate()
        .map(|(binding, column)| {
            column.map(|_| {
                if input_bindings.contains(&binding) {
                    program.alloc_register()
                } else {
                    null_reg
                }
            })
        })
        .collect::<Vec<_>>();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    for column in 0..input.identity_width() {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: identity_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Null {
        dest: values_start,
        dest_end: Some(values_start + output.width - 1),
    });
    program.emit_insn(Insn::Null {
        dest: null_reg,
        dest_end: None,
    });
    for column in 0..input.width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: values_start + before.len() + column,
            default: None,
        });
    }
    for binding in input_bindings.clone() {
        match (
            binding_rowid_regs[binding],
            input.binding_rowid_columns[binding - input_bindings.start],
        ) {
            (Some(dest), Some(column)) => program.emit_insn(Insn::Column {
                cursor_id: input.cursor_id,
                column,
                dest,
                default: None,
            }),
            (None, None) => {}
            _ => {
                return Err(LimboError::InternalError(
                    "padded stream changes its input's rowid provenance".to_string(),
                ))
            }
        }
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: weight_reg,
        default: None,
    });
    emit_ephemeral_delta_row(
        program,
        &output,
        identity_start,
        &binding_rowid_regs,
        values_start,
        weight_reg,
    );
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_label);
    EmittedNodeOutput::from_ephemeral(node_id, output, output_contract)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_project(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    projections: &[ast::Expr],
    output_contract: &NodeOutputContract,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<EmittedNodeOutput> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_project_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    emit_project_rows(program, input, projections, &output, schema, connection)?;
    EmittedNodeOutput::from_ephemeral(node_id, output, output_contract)
}

#[allow(clippy::too_many_arguments)]
fn emit_project_rows(
    program: &mut ProgramBuilder,
    input: &EphemeralDelta,
    projections: &[ast::Expr],
    output: &EphemeralDelta,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let num_output_columns = output.width;
    turso_assert!(
        projections.len() == num_output_columns,
        "projection output does not match its sink schema"
    );

    let syms = connection.syms.read();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
        &None,
    );
    let (table_references, binding_remap) = stream_table_references(program, &input.schema);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    seed_ephemeral_stream_cache(
        program,
        input,
        &binding_remap,
        &table_references,
        &mut resolver,
    )?;

    turso_assert!(
        output.identity == input.identity
            && output.binding_rowid_columns == input.binding_rowid_columns
            && output.value_start == input.value_start
            && output.weight_column == output.value_start + num_output_columns,
        "linear projection sink must preserve its input metadata contract"
    );
    let metadata_start = program.alloc_registers(output.value_start);
    for column in 0..input.value_start {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: metadata_start + column,
            default: None,
        });
    }
    let out_start = program.alloc_registers(num_output_columns + 1);
    let new_weight_reg = out_start + num_output_columns;
    for (index, expr) in projections.iter().enumerate() {
        let bound = remap_bound_expr(expr, &binding_remap)?;
        translate_expr_no_constant_opt(
            program,
            Some(&table_references),
            &bound,
            out_start + index,
            &resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }
    let weight_reg = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: weight_reg,
        default: None,
    });

    debug_assert_eq!(metadata_start + output.value_start, out_start);
    program.emit_insn(Insn::Copy {
        src_reg: weight_reg,
        dst_reg: new_weight_reg,
        extra_amount: 0,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(metadata_start),
        count: to_u32(output.record_width()),
        dest_reg: to_u32(record_reg),
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
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    Ok(())
}
