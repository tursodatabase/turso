use super::binding::{cache_stream_expr_reg, make_joined_table, remap_bound_expr, BindingRemap};
use super::output::{EmittedNodeOutput, NodeOutput, NodeOutputContract};
use super::plan::OperatorStateDef;
use super::source::materialize_node_output;
use super::stream::{
    base_arrangement, emit_ephemeral_delta_row, open_ephemeral_delta, scratch_table,
    ArrangementHandle, ArrangementIdentityColumn, DeltaIdentity, EphemeralDelta,
};
use super::MaintenanceInput;
use crate::alloc::TursoIteratorExt;
use crate::incremental::dag;
use crate::schema::{is_materialized_view_join_key_index, BTreeTable, Index, IndexColumn, Schema};
use crate::sync::Arc;
use crate::translate::collate::{get_expr_collation_ctx_with_symbols, CollationSeq};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    comparison_affinity, expr_is_array, translate_condition_expr, walk_expr, walk_expr_mut,
    ConditionMetadata, WalkControl,
};
use crate::translate::optimizer::constraints::bind_partial_index_columns;
use crate::translate::plan::{JoinedTable, TableReferences};
use crate::translate::planner::{break_predicate_at_and_boundaries, rewrite_between_exprs};
use crate::turso_assert;
use crate::util::exprs_are_equivalent;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral,
};
use crate::vdbe::BranchOffset;
use crate::{Connection, LimboError, Result};
use std::num::NonZeroUsize;
use turso_parser::ast::{self, SortOrder};

/// One physical input edge of a join.
///
/// Keeping the delta, logical schema, and maintained arrangement together
/// prevents emitters from accidentally combining parallel slices belonging
/// to different inputs.
struct JoinInputContract {
    /// The input's changes in this transaction, copied into an ephemeral
    /// table so that a join can find a changed row again by its rowid.
    /// `None` while a new view is populated, which reads only arrangements.
    delta: Option<EphemeralDelta>,
    schema: Arc<dag::StreamSchema>,
    /// Whether each binding of the input carries its SQL rowid.
    binding_rowids: Arc<[bool]>,
    arrangement: ArrangementHandle,
}

/// What finding a join's keys, and the inputs each term reads, needs from
/// one input: its values, and which of its bindings carry their SQL rowid.
/// Planning a view knows these before any arrangement exists.
pub(super) struct KeyInput<'a> {
    pub(super) schema: &'a dag::StreamSchema,
    pub(super) binding_rowids: &'a [bool],
}

impl JoinInputContract {
    fn delta(&self) -> Result<&EphemeralDelta> {
        self.delta.as_ref().ok_or_else(|| {
            LimboError::InternalError(
                "a join read the changes of its input while populating a view".to_string(),
            )
        })
    }
}

/// The complete physical contract of one join node.
pub(super) struct JoinContract {
    inputs: Vec<JoinInputContract>,
    on: Vec<ast::Expr>,
}

impl JoinContract {
    pub(super) fn from_outputs(
        program: &mut ProgramBuilder,
        view_name: &str,
        input_nodes: &[dag::NodeId],
        outputs: &[&NodeOutput],
        on: &[ast::Expr],
        input: MaintenanceInput,
    ) -> Result<Self> {
        turso_assert!(
            input_nodes.len() == outputs.len(),
            "a join needs one output per input node"
        );
        let inputs = input_nodes
            .iter()
            .zip(outputs)
            .map(|(input_node, output)| {
                let arrangement = output.arrangement().cloned().ok_or_else(|| {
                    LimboError::InternalError(
                        "join input has no materialized arrangement".to_string(),
                    )
                })?;
                let delta = match input {
                    MaintenanceInput::TransactionDelta => Some(materialize_node_output(
                        program,
                        view_name,
                        *input_node,
                        output,
                        input,
                    )?),
                    MaintenanceInput::BaseTable => None,
                };
                Ok(JoinInputContract {
                    delta,
                    schema: output.contract().schema.clone(),
                    binding_rowids: output.contract().binding_rowids.clone(),
                    arrangement,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            inputs,
            on: on.to_vec(),
        })
    }

    fn key_inputs(&self) -> Vec<KeyInput<'_>> {
        self.inputs
            .iter()
            .map(|input| KeyInput {
                schema: &input.schema,
                binding_rowids: &input.binding_rowids,
            })
            .collect()
    }
}

/// Emit a join's natural delta row into a typed ephemeral stream. The join
/// knows only its declared inputs and predicates; downstream group keys,
/// aggregate arguments, filters, and projections are evaluated by their own
/// operators from this stream.
///
/// The returned emitted output is validated against the node's planned
/// identity and binding namespace before the dispatcher can publish it.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_inner_join(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    output_contract: &NodeOutputContract,
    contract: &JoinContract,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<EmittedNodeOutput> {
    let width = output_contract.schema.len();
    let natural_width = contract
        .inputs
        .iter()
        .map(|input| input.schema.len())
        .sum::<usize>();
    turso_assert!(
        width == natural_width,
        "join physical inputs must match the planned output schema"
    );

    let channel = open_ephemeral_delta(
        program,
        &format!("{view_name}_join_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    match channel.identity {
        DeltaIdentity::BindingRowids(_) => {}
        DeltaIdentity::OperatorKey(width) if width == contract.inputs.len() => {}
        DeltaIdentity::OperatorRowid | DeltaIdentity::OperatorKey(_) => {
            return Err(LimboError::InternalError(
                "inner join has an invalid planned identity".to_string(),
            ));
        }
    }

    // [transport identity..., binding rowids..., natural join row..., weight].
    let eph_rec_start = program.alloc_registers(channel.record_width());
    let eph_rowid_reg = program.alloc_register();
    let eph_record_reg = program.alloc_register();

    let syms = connection.syms.read();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        connection.namespaces(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
        &None,
    );

    emit_join_phases(
        program,
        &mut resolver,
        view_name,
        schema,
        contract,
        input,
        &mut |program, row| {
            emit_inner_join_output_row(program, contract, &channel, eph_rec_start, row);
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(eph_rec_start),
                count: to_u32(channel.record_width()),
                dest_reg: to_u32(eph_record_reg),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::NewRowid {
                cursor: channel.cursor_id,
                rowid_reg: eph_rowid_reg,
                prev_largest_reg: 0,
            });
            program.emit_insn(Insn::Insert {
                cursor: channel.cursor_id,
                key_reg: eph_rowid_reg,
                record_reg: eph_record_reg,
                flag: InsertFlags::new().is_ephemeral_table_insert(),
                table_name: String::new(),
            });
            Ok(())
        },
    )?;

    drop(syms);
    EmittedNodeOutput::from_ephemeral(node_id, channel, output_contract)
}

/// Copy one joined row into the output record registers: the inputs'
/// identities (concatenated, or one packed record per input for an opaque
/// key), the binding rowids the identity does not already carry, the
/// inputs' values in input order, and the weight.
fn emit_inner_join_output_row(
    program: &mut ProgramBuilder,
    contract: &JoinContract,
    channel: &EphemeralDelta,
    record_start: usize,
    row: &JoinRow,
) {
    let mut identity_column = 0;
    let mut output_binding = 0;
    let mut value_column = channel.value_start;
    for (position, input) in contract.inputs.iter().enumerate() {
        let image = row.layout.image(position);
        match channel.identity {
            DeltaIdentity::BindingRowids(_) => {
                program.emit_insn(Insn::Copy {
                    src_reg: row.start + image.identity_start,
                    dst_reg: record_start + identity_column,
                    extra_amount: image.identity_width - 1,
                });
                identity_column += image.identity_width;
            }
            _ => program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(row.start + image.identity_start),
                count: to_u32(image.identity_width),
                dest_reg: to_u32(record_start + position),
                index_name: None,
                affinity_str: None,
            }),
        }
        for slot in &image.binding_rowids {
            if let Some(output_column) = channel.binding_rowid_columns[output_binding] {
                if output_column >= channel.identity_width() {
                    let slot = slot.expect("planned join rowid provenance is unavailable");
                    program.emit_insn(Insn::Copy {
                        src_reg: row.start + slot,
                        dst_reg: record_start + output_column,
                        extra_amount: 0,
                    });
                }
            }
            output_binding += 1;
        }
        if !input.schema.columns.is_empty() {
            program.emit_insn(Insn::Copy {
                src_reg: row.start + image.value_start,
                dst_reg: record_start + value_column,
                extra_amount: input.schema.len() - 1,
            });
        }
        value_column += input.schema.len();
    }
    turso_assert!(
        output_binding == channel.schema.bindings.len()
            && value_column == channel.weight_column
            && (!matches!(channel.identity, DeltaIdentity::BindingRowids(_))
                || identity_column == channel.identity_width()),
        "join row does not match its declared output stream"
    );
    program.emit_insn(Insn::Copy {
        src_reg: row.weight_reg,
        dst_reg: record_start + channel.weight_column,
        extra_amount: 0,
    });
}

/// Emit a semi or anti join: the rows of the first input that some (semi) or
/// no (anti) row of the second input matches under the ON predicates.
///
/// For each row of the first input, a hidden table stored under the row's
/// rowid keeps:
///
/// - `present`: the row's multiplicity, the integral of the first input's
///   delta;
/// - `matches`: its number of ON matches, the integral of the join's change
///   `dA ⋈ B_old + A_new ⋈ dB` (see [`emit_join_phases`]);
/// - `published`: the weight the row has in the output, which is `present`
///   when the match test passes (`matches > 0` for semi, `matches == 0` for
///   anti) and zero otherwise, and the image published with it.
///
/// One run first sums the changes to both counts per row in a scratch table
/// (the consolidated delta), then visits each changed row once: it adds the
/// sums to the stored counts and emits the change of the row's output
/// weight. Summing first makes the result independent of the order of the
/// delta rows, and visiting each row once means every decision reads the
/// row's final counts. A row inserted and deleted by the same statement
/// therefore never reaches the output.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_semi_or_anti_join(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    kind: dag::JoinKind,
    contract: &JoinContract,
    output_contract: &NodeOutputContract,
    input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<EmittedNodeOutput> {
    let keeps_matched_rows = match kind {
        dag::JoinKind::Semi => true,
        dag::JoinKind::Anti => false,
        dag::JoinKind::Inner => {
            return Err(LimboError::InternalError(
                "an inner join has no per-row match state".to_string(),
            ))
        }
    };
    let rows = &contract.inputs[0];
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_semi_join_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    if output.width != rows.schema.len()
        || output.identity_width() != 1
        || !matches!(
            rows.arrangement.identity_columns(),
            [ArrangementIdentityColumn::RowId]
        )
    {
        return Err(LimboError::InternalError(
            "semi or anti join rows must be identified by the rowid of their arrangement"
                .to_string(),
        ));
    }

    let state_name = operator_state.match_counts_table_name()?.to_string();
    let state_table = schema.get_btree_table(&state_name).ok_or_else(|| {
        LimboError::InternalError(format!(
            "semi or anti join state {state_name} of materialized view {view_name} not found"
        ))
    })?;

    let syms = connection.syms.read();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        connection.namespaces(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
        &None,
    );

    let state_cursor = program.alloc_cursor_id(CursorType::BTreeTable(state_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_cursor,
        root_page: RegisterOrLiteral::Literal(state_table.root_page),
        db: 0,
    });
    let rows_table = rows.arrangement.table();
    let rows_cursor = program.alloc_cursor_id(CursorType::BTreeTable(rows_table.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: rows_cursor,
        root_page: rows_table.root_page,
        db: 0,
    });
    // Per changed row, keyed by its rowid: (present change, matches change,
    // whether the row's own delta was non-empty).
    let changes_table = Arc::new(scratch_table(&format!("{state_name}_changes"), 3)?);
    let changes_cursor = program.alloc_cursor_id(CursorType::BTreeTable(changes_table));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: changes_cursor,
        is_table: true,
    });

    let zero_reg = program.alloc_register();
    program.emit_int(0, zero_reg);
    let row_reg = program.alloc_register();
    let present_change_reg = program.alloc_register();
    let matches_change_reg = program.alloc_register();
    let return_reg = program.alloc_register();
    let sums_start = program.alloc_registers(3);
    let sums_record_reg = program.alloc_register();
    let add_change_label = program.allocate_label();
    let corrupt_label = program.allocate_label();
    let start_label = program.allocate_label();
    let end_label = program.allocate_label();

    program.emit_insn(Insn::Goto {
        target_pc: start_label,
    });
    emit_add_change_subroutine(
        program,
        add_change_label,
        changes_cursor,
        row_reg,
        present_change_reg,
        matches_change_reg,
        sums_start,
        sums_record_reg,
        zero_reg,
        return_reg,
    );
    program.preassign_label_to_next_insn(start_label);

    // The first input's own delta changes `present`. Population reads the
    // whole arrangement instead.
    let (presence_side, presence_cursor) = match input {
        MaintenanceInput::TransactionDelta => (JoinSide::Delta, rows.delta()?.cursor_id),
        MaintenanceInput::BaseTable => {
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(rows_table.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: rows_table.root_page,
                db: 0,
            });
            (JoinSide::Btree, cursor_id)
        }
    };
    let presence_done = program.allocate_label();
    let presence_loop = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: presence_cursor,
        pc_if_empty: presence_done,
    });
    program.preassign_label_to_next_insn(presence_loop);
    match (presence_side, rows.arrangement.count_column()) {
        (JoinSide::Delta, _) => program.emit_insn(Insn::Column {
            cursor_id: presence_cursor,
            column: rows.delta()?.weight_column,
            dest: present_change_reg,
            default: None,
        }),
        (JoinSide::Btree, Some(count_column)) => program.emit_insn(Insn::Column {
            cursor_id: presence_cursor,
            column: count_column,
            dest: present_change_reg,
            default: None,
        }),
        (JoinSide::Btree, None) => program.emit_int(1, present_change_reg),
    }
    emit_join_input_identity(program, presence_side, rows, presence_cursor, row_reg)?;
    program.emit_int(0, matches_change_reg);
    program.emit_insn(Insn::Gosub {
        target_pc: add_change_label,
        return_reg,
    });
    program.emit_insn(Insn::Next {
        cursor_id: presence_cursor,
        pc_if_next: presence_loop,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(presence_done);

    // Each changed ON match changes `matches` of its first-input row.
    emit_join_phases(
        program,
        &mut resolver,
        view_name,
        schema,
        contract,
        input,
        &mut |program, row| {
            let image = row.layout.image(0);
            program.emit_insn(Insn::Copy {
                src_reg: row.start + image.identity_start,
                dst_reg: row_reg,
                extra_amount: 0,
            });
            program.emit_int(0, present_change_reg);
            program.emit_insn(Insn::Copy {
                src_reg: row.weight_reg,
                dst_reg: matches_change_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Gosub {
                target_pc: add_change_label,
                return_reg,
            });
            Ok(())
        },
    )?;

    // Visit each changed row once.
    // State record: [present, matches, published, output image...], where the
    // image is the row's values followed by its available binding rowids.
    let image_width = output.width
        + output
            .binding_rowid_columns
            .iter()
            .filter(|column| column.is_some())
            .count();
    let state_width = 3 + image_width;
    let state_start = program.alloc_registers(state_width);
    let present_reg = state_start;
    let matches_reg = state_start + 1;
    let published_reg = state_start + 2;
    let image_start = state_start + 3;
    let image_binding_rowid_regs = {
        let mut next = image_start + output.width;
        output
            .binding_rowid_columns
            .iter()
            .map(|column| {
                column.map(|_| {
                    next += 1;
                    next - 1
                })
            })
            .collect::<Vec<_>>()
    };
    let own_delta_reg = sums_start + 2;
    let new_weight_reg = program.alloc_register();
    let retract_weight_reg = program.alloc_register();
    let state_record_reg = program.alloc_register();

    let visit_loop = program.allocate_label();
    let visit_next = program.allocate_label();
    let visit_done = program.allocate_label();
    let new_row = program.allocate_label();
    let counted = program.allocate_label();
    let has_rows = program.allocate_label();
    let decided = program.allocate_label();
    let publish = program.allocate_label();
    let add_new_image = program.allocate_label();
    let store = program.allocate_label();
    let delete_state = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: changes_cursor,
        pc_if_empty: visit_done,
    });
    program.preassign_label_to_next_insn(visit_loop);
    program.emit_insn(Insn::RowId {
        cursor_id: changes_cursor,
        dest: row_reg,
    });
    for column in 0..3 {
        program.emit_insn(Insn::Column {
            cursor_id: changes_cursor,
            column,
            dest: sums_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::NotExists {
        cursor: state_cursor,
        rowid_reg: row_reg,
        target_pc: new_row,
    });
    for column in 0..state_width {
        program.emit_insn(Insn::Column {
            cursor_id: state_cursor,
            column,
            dest: state_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Goto { target_pc: counted });
    program.preassign_label_to_next_insn(new_row);
    for reg in [present_reg, matches_reg, published_reg] {
        program.emit_int(0, reg);
    }
    if image_width > 0 {
        program.emit_insn(Insn::Null {
            dest: image_start,
            dest_end: Some(image_start + image_width - 1),
        });
    }
    program.preassign_label_to_next_insn(counted);
    program.emit_insn(Insn::Add {
        lhs: present_reg,
        rhs: sums_start,
        dest: present_reg,
    });
    program.emit_insn(Insn::Add {
        lhs: matches_reg,
        rhs: sums_start + 1,
        dest: matches_reg,
    });
    // Counts are integrals of z-sets that can only hold existing rows and
    // pairs: neither is negative, and a row that is gone has no matches.
    for reg in [present_reg, matches_reg] {
        program.emit_insn(Insn::Lt {
            lhs: reg,
            rhs: zero_reg,
            target_pc: corrupt_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
    }
    program.emit_insn(Insn::If {
        reg: present_reg,
        target_pc: has_rows,
        jump_if_null: false,
    });
    program.emit_insn(Insn::If {
        reg: matches_reg,
        target_pc: corrupt_label,
        jump_if_null: false,
    });
    program.preassign_label_to_next_insn(has_rows);
    program.emit_int(0, new_weight_reg);
    if keeps_matched_rows {
        program.emit_insn(Insn::IfNot {
            reg: matches_reg,
            target_pc: decided,
            jump_if_null: false,
        });
    } else {
        program.emit_insn(Insn::If {
            reg: matches_reg,
            target_pc: decided,
            jump_if_null: false,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: present_reg,
        dst_reg: new_weight_reg,
        extra_amount: 0,
    });
    program.preassign_label_to_next_insn(decided);
    // Republish when the weight changes, or when the row's own delta may have
    // changed its values while it stays in the output.
    program.emit_insn(Insn::Ne {
        lhs: new_weight_reg,
        rhs: published_reg,
        target_pc: publish,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::IfNot {
        reg: own_delta_reg,
        target_pc: store,
        jump_if_null: false,
    });
    program.emit_insn(Insn::IfNot {
        reg: new_weight_reg,
        target_pc: store,
        jump_if_null: false,
    });
    program.preassign_label_to_next_insn(publish);
    program.emit_insn(Insn::IfNot {
        reg: published_reg,
        target_pc: add_new_image,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Subtract {
        lhs: zero_reg,
        rhs: published_reg,
        dest: retract_weight_reg,
    });
    emit_ephemeral_delta_row(
        program,
        &output,
        row_reg,
        &image_binding_rowid_regs,
        image_start,
        retract_weight_reg,
    );
    program.preassign_label_to_next_insn(add_new_image);
    program.emit_insn(Insn::IfNot {
        reg: new_weight_reg,
        target_pc: store,
        jump_if_null: false,
    });
    // The arrangement is already in its post-statement state.
    program.emit_insn(Insn::SeekRowid {
        cursor_id: rows_cursor,
        src_reg: row_reg,
        target_pc: corrupt_label,
    });
    for (column, physical_column) in rows.arrangement.value_columns().iter().enumerate() {
        program.emit_column_or_rowid(rows_cursor, *physical_column, image_start + column);
    }
    for (binding, reg) in image_binding_rowid_regs.iter().enumerate() {
        if let Some(reg) = reg {
            turso_assert!(
                emit_join_input_binding_rowid(
                    program,
                    JoinSide::Btree,
                    rows,
                    rows_cursor,
                    binding,
                    *reg,
                )?,
                "semi or anti join output rowid provenance is unavailable from its input"
            );
        }
    }
    emit_ephemeral_delta_row(
        program,
        &output,
        row_reg,
        &image_binding_rowid_regs,
        image_start,
        new_weight_reg,
    );
    program.preassign_label_to_next_insn(store);
    program.emit_insn(Insn::Copy {
        src_reg: new_weight_reg,
        dst_reg: published_reg,
        extra_amount: 0,
    });
    // `present` is zero only when `matches` is zero too (checked above).
    program.emit_insn(Insn::IfNot {
        reg: present_reg,
        target_pc: delete_state,
        jump_if_null: false,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(state_start),
        count: to_u32(state_width),
        dest_reg: to_u32(state_record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: state_cursor,
        key_reg: row_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: state_name.clone(),
    });
    program.emit_insn(Insn::Goto {
        target_pc: visit_next,
    });
    program.preassign_label_to_next_insn(delete_state);
    program.emit_insn(Insn::NotExists {
        cursor: state_cursor,
        rowid_reg: row_reg,
        target_pc: visit_next,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: state_cursor,
        table_name: state_name,
        is_part_of_update: true,
    });
    program.preassign_label_to_next_insn(visit_next);
    program.emit_insn(Insn::Next {
        cursor_id: changes_cursor,
        pc_if_next: visit_loop,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(visit_done);

    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} semi or anti join state is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    EmittedNodeOutput::from_ephemeral(node_id, output, output_contract)
}

/// Emit the subroutine that adds one change to a row's sums in the scratch
/// table: `(row_reg, present_change_reg, matches_change_reg)` in, sums
/// `(present change, matches change, own delta seen)` stored under `row_reg`.
#[allow(clippy::too_many_arguments)]
fn emit_add_change_subroutine(
    program: &mut ProgramBuilder,
    label: crate::vdbe::BranchOffset,
    changes_cursor: usize,
    row_reg: usize,
    present_change_reg: usize,
    matches_change_reg: usize,
    sums_start: usize,
    sums_record_reg: usize,
    zero_reg: usize,
    return_reg: usize,
) {
    let first_change = program.allocate_label();
    let add = program.allocate_label();
    let write = program.allocate_label();
    program.preassign_label_to_next_insn(label);
    program.emit_insn(Insn::NotExists {
        cursor: changes_cursor,
        rowid_reg: row_reg,
        target_pc: first_change,
    });
    for column in 0..3 {
        program.emit_insn(Insn::Column {
            cursor_id: changes_cursor,
            column,
            dest: sums_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Goto { target_pc: add });
    program.preassign_label_to_next_insn(first_change);
    for column in 0..3 {
        program.emit_int(0, sums_start + column);
    }
    program.preassign_label_to_next_insn(add);
    program.emit_insn(Insn::Add {
        lhs: sums_start,
        rhs: present_change_reg,
        dest: sums_start,
    });
    program.emit_insn(Insn::Add {
        lhs: sums_start + 1,
        rhs: matches_change_reg,
        dest: sums_start + 1,
    });
    program.emit_insn(Insn::Eq {
        lhs: present_change_reg,
        rhs: zero_reg,
        target_pc: write,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_int(1, sums_start + 2);
    program.preassign_label_to_next_insn(write);
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(sums_start),
        count: 3,
        dest_reg: to_u32(sums_record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: changes_cursor,
        key_reg: row_reg,
        record_reg: sums_record_reg,
        flag: InsertFlags::new()
            .require_seek()
            .is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Return {
        return_reg,
        can_fallthrough: false,
    });
}

/// Which rows of an input one phase of a delta join reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputState {
    /// The rows after this transaction's changes: the input's arrangement.
    New,
    /// Only this transaction's changes.
    Delta,
    /// The rows before the changes: the arrangement, plus the changes with
    /// their weights negated.
    Old,
}

/// Where a join phase reads one input's rows from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    /// The transaction's captured delta for the input.
    Delta,
    /// The input's arrangement, which at maintenance time holds the
    /// post-change state.
    Btree,
}

/// One joined row handed to a join's output step: every input's image at
/// `start` (laid out by `layout`), and the row's signed weight.
struct JoinRow<'a> {
    layout: &'a RowLayout,
    start: usize,
    weight_reg: usize,
}

type JoinRowSink<'a> = dyn FnMut(&mut ProgramBuilder, &JoinRow) -> Result<()> + 'a;

/// Emit every row of a join's change, or of its whole result while a view is
/// populated, into `sink`.
///
/// A change to the inputs is the sum of one phase per input (the delta
/// join): `A'B'C' − ABC = dA·B·C + A'·dB·C + A'·B'·dC`, where `X'` is the
/// input after the change and `X` before it. Phase `i` reads the inputs
/// before `i` in their new state, input `i`'s changes, and the inputs after
/// `i` in their old state, which is the new state plus the changes with their
/// weights negated. No phase reads a stored intermediate join result.
///
/// A phase starts from its driving input and adds one input per level. Level
/// `k` reads the partial rows of the inputs bound so far from an ephemeral
/// table and writes the rows extended by one input to the next table, so
/// each level's code is emitted once. The last level hands its rows to
/// `sink`. Population is one phase that reads every input in its new state.
fn emit_join_phases(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    view_name: &str,
    schema: &Schema,
    contract: &JoinContract,
    input: MaintenanceInput,
    sink: &mut JoinRowSink<'_>,
) -> Result<()> {
    let input_count = contract.inputs.len();
    let key_inputs = contract.key_inputs();
    let (binding_remap, table_references) =
        input_table_references(&key_inputs, |_| program.table_reference_counter.next());
    let keys = find_join_keys(
        &key_inputs,
        &contract.on,
        &binding_remap,
        &table_references,
        resolver,
    )?;
    let term_inputs = contract
        .on
        .iter()
        .map(|term| term_input_positions(&key_inputs, term))
        .collect::<Result<Vec<_>>>()?;
    let input_filters = (0..input_count)
        .map(|input| input_filter(&key_inputs, &contract.on, &term_inputs, input))
        .collect::<Vec<_>>();
    let terms = JoinTerms {
        exprs: &contract.on,
        binding_remap: &binding_remap,
        table_references: &table_references,
    };
    resolver.enable_expr_to_reg_cache();

    let phases: Vec<Vec<InputState>> = match input {
        MaintenanceInput::BaseTable => vec![vec![InputState::New; input_count]],
        MaintenanceInput::TransactionDelta => (0..input_count)
            .map(|driver| {
                (0..input_count)
                    .map(|position| match position.cmp(&driver) {
                        std::cmp::Ordering::Less => InputState::New,
                        std::cmp::Ordering::Equal => InputState::Delta,
                        std::cmp::Ordering::Greater => InputState::Old,
                    })
                    .collect()
            })
            .collect(),
    };
    let corrupt_label = program.allocate_label();
    let done_label = program.allocate_label();
    let minus_one_reg = program.alloc_register();
    program.emit_int(-1, minus_one_reg);
    for states in phases {
        let plan = match states.iter().position(|state| *state == InputState::Delta) {
            Some(driver) => plan_phase(
                schema,
                contract,
                &keys,
                &term_inputs,
                &input_filters,
                driver,
            ),
            None => population_plan(schema, contract, &keys, &term_inputs, &input_filters),
        };
        emit_join_phase(
            program,
            resolver,
            view_name,
            contract,
            &keys,
            &terms,
            &plan,
            &states,
            corrupt_label,
            minus_one_reg,
            sink,
        )?;
    }
    program.emit_insn(Insn::Goto {
        target_pc: done_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!(
            "materialized view {view_name}: a join key lookup found a row that does not exist"
        ),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(done_label);
    Ok(())
}

/// The ON and WHERE terms of a join, and the bindings they are translated with.
struct JoinTerms<'a> {
    exprs: &'a [ast::Expr],
    binding_remap: &'a BindingRemap,
    table_references: &'a TableReferences,
}

/// One phase of a delta join: the input it starts from, and the inputs it
/// adds level by level.
struct PhasePlan {
    driver: usize,
    /// Terms that read only the driver (or nothing): checked before its rows
    /// enter the first partial table.
    driver_terms: Vec<usize>,
    levels: Vec<LevelPlan>,
}

/// One level of a phase: the input it adds and how it finds that input's
/// rows for the partial rows bound so far.
struct LevelPlan {
    input: usize,
    /// The join keys between `input` and an input bound earlier.
    keys: Vec<usize>,
    access: LevelAccess,
    /// Terms whose inputs are all bound once `input` is: checked here.
    terms: Vec<usize>,
}

/// How a level finds the rows of its input that match a partial row.
enum LevelAccess {
    /// Seek the input's rowid with `keys[key]`, once per partial row.
    Rowid { key: usize },
    /// Seek an index of the input with `keys` (indexes into the join's keys,
    /// in the index's column order), once per partial row.
    Index { index: Arc<Index>, keys: Vec<usize> },
    /// No usable rowid or index: index the partial rows by their key values,
    /// read the input once, and look each of its rows up in that index.
    ScanAndProbe,
    /// No join key between the input and the bound inputs: every pair.
    Cross,
}

/// Choose the order in which a phase adds inputs: from the bound inputs,
/// follow a join key to an input that can be looked up by rowid, then one
/// with a usable index, then any keyed input; an input with no key to the
/// bound ones comes last. Every term is checked at the first level where all
/// the inputs it reads are bound.
fn plan_phase(
    schema: &Schema,
    contract: &JoinContract,
    keys: &[JoinKey],
    term_inputs: &[Vec<usize>],
    input_filters: &[Option<InputFilter>],
    driver: usize,
) -> PhasePlan {
    let input_count = contract.inputs.len();
    let mut bound = vec![driver];
    let mut placed = vec![false; term_inputs.len()];
    let mut take_ready_terms = |bound: &[usize]| {
        (0..term_inputs.len())
            .filter(|term| {
                let ready =
                    !placed[*term] && term_inputs[*term].iter().all(|input| bound.contains(input));
                if ready {
                    placed[*term] = true;
                }
                ready
            })
            .collect::<Vec<_>>()
    };
    let driver_terms = take_ready_terms(&bound);
    let mut levels = Vec::with_capacity(input_count - 1);
    while bound.len() < input_count {
        let mut best: Option<(usize, LevelPlan)> = None;
        for input in (0..input_count).filter(|input| !bound.contains(input)) {
            let level_keys = keys
                .iter()
                .enumerate()
                .filter(|(_, key)| {
                    key.other_input(input)
                        .is_some_and(|(other, _)| bound.contains(&other))
                })
                .map(|(position, _)| position)
                .collect::<Vec<_>>();
            if level_keys.is_empty() {
                continue;
            }
            let access = persistent_key_access(
                schema,
                &contract.inputs[input].arrangement,
                input,
                keys,
                &level_keys,
                input_filters[input].as_ref(),
                |_| true,
            )
            .unwrap_or(LevelAccess::ScanAndProbe);
            let rank = match access {
                LevelAccess::Rowid { .. } => 0,
                LevelAccess::Index { .. } => 1,
                LevelAccess::ScanAndProbe | LevelAccess::Cross => 2,
            };
            if best.as_ref().is_none_or(|(best_rank, _)| rank < *best_rank) {
                best = Some((
                    rank,
                    LevelPlan {
                        input,
                        keys: level_keys,
                        access,
                        terms: Vec::new(),
                    },
                ));
            }
        }
        let mut level = match best {
            Some((_, level)) => level,
            None => LevelPlan {
                input: (0..input_count)
                    .find(|input| !bound.contains(input))
                    .expect("an unbound input remains"),
                keys: Vec::new(),
                access: LevelAccess::Cross,
                terms: Vec::new(),
            },
        };
        bound.push(level.input);
        level.terms = take_ready_terms(&bound);
        levels.push(level);
    }
    turso_assert!(
        placed.iter().all(|placed| *placed),
        "every join term must be checked at some level"
    );
    PhasePlan {
        driver,
        driver_terms,
        levels,
    }
}

/// The phase population runs: the one, among phases starting from each
/// input, that looks the most inputs up by rowid or index instead of reading
/// them whole.
fn population_plan(
    schema: &Schema,
    contract: &JoinContract,
    keys: &[JoinKey],
    term_inputs: &[Vec<usize>],
    input_filters: &[Option<InputFilter>],
) -> PhasePlan {
    (0..contract.inputs.len())
        .map(|driver| plan_phase(schema, contract, keys, term_inputs, input_filters, driver))
        .min_by_key(|plan| {
            let reads_whole_inputs = plan
                .levels
                .iter()
                .filter(|level| {
                    matches!(level.access, LevelAccess::ScanAndProbe | LevelAccess::Cross)
                })
                .count();
            (reads_whole_inputs, plan.driver)
        })
        .expect("a join has inputs")
}

/// Emit one phase: copy the driver's rows that pass its terms into the first
/// partial table, then add the other inputs level by level.
#[allow(clippy::too_many_arguments)]
fn emit_join_phase(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    view_name: &str,
    contract: &JoinContract,
    keys: &[JoinKey],
    terms: &JoinTerms,
    plan: &PhasePlan,
    states: &[InputState],
    corrupt_label: BranchOffset,
    minus_one_reg: usize,
    sink: &mut JoinRowSink<'_>,
) -> Result<()> {
    let phase_done = program.allocate_label();
    let driver = &contract.inputs[plan.driver];
    let (driver_side, driver_cursor) = match states[plan.driver] {
        InputState::Delta => {
            // A phase without changes to its driving input adds nothing.
            let cursor_id = driver.delta()?.cursor_id;
            program.emit_insn(Insn::Rewind {
                cursor_id,
                pc_if_empty: phase_done,
            });
            (JoinSide::Delta, cursor_id)
        }
        InputState::New => (JoinSide::Btree, open_arrangement(program, driver)),
        InputState::Old => {
            return Err(LimboError::InternalError(
                "a delta join phase cannot start from an input's old state".to_string(),
            ))
        }
    };

    let mut layout = RowLayout::default().with_input(contract, plan.driver);
    let mut partial = open_partial_table(program, view_name, &layout)?;
    {
        let row = program.alloc_registers(layout.width + 1);
        let weight_reg = row + layout.width;
        let cache_mark = resolver.expr_to_reg_cache.len();
        emit_bind_row(resolver, contract, terms, &layout, row)?;
        let loop_label = program.allocate_label();
        let next_label = program.allocate_label();
        let filled = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: driver_cursor,
            pc_if_empty: filled,
        });
        program.preassign_label_to_next_insn(loop_label);
        emit_read_image(
            program,
            driver,
            driver_side,
            driver_cursor,
            layout.image(plan.driver),
            row,
        )?;
        match emit_read_weight(program, driver, driver_side, driver_cursor)? {
            Some(weight) => program.emit_insn(Insn::Copy {
                src_reg: weight,
                dst_reg: weight_reg,
                extra_amount: 0,
            }),
            None => program.emit_int(1, weight_reg),
        }
        emit_terms(program, resolver, terms, &plan.driver_terms, next_label)?;
        emit_partial_insert(program, partial, row, layout.width);
        program.preassign_label_to_next_insn(next_label);
        program.emit_insn(Insn::Next {
            cursor_id: driver_cursor,
            pc_if_next: loop_label,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(filled);
        resolver.expr_to_reg_cache.truncate(cache_mark);
    }

    for (position, level) in plan.levels.iter().enumerate() {
        // No partial rows: nothing joins any more.
        program.emit_insn(Insn::Rewind {
            cursor_id: partial,
            pc_if_empty: phase_done,
        });
        let next_layout = layout.with_input(contract, level.input);
        let output = if position + 1 == plan.levels.len() {
            LevelOutput::Sink
        } else {
            LevelOutput::Partial(open_partial_table(program, view_name, &next_layout)?)
        };
        emit_join_level(
            program,
            resolver,
            contract,
            keys,
            terms,
            level,
            states[level.input],
            partial,
            &layout,
            &next_layout,
            &output,
            corrupt_label,
            minus_one_reg,
            sink,
        )?;
        if let LevelOutput::Partial(next) = output {
            partial = next;
        }
        layout = next_layout;
    }
    program.preassign_label_to_next_insn(phase_done);
    Ok(())
}

/// Where a level writes the rows it extends: the next partial table, or the
/// join's output step after the last level.
enum LevelOutput {
    Partial(usize),
    Sink,
}

/// Emit one level: extend every partial row with the matching rows of
/// `level.input`. An input read in its old state is read twice at this
/// level: its arrangement, then its changes with the weights negated.
#[allow(clippy::too_many_arguments)]
fn emit_join_level(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    contract: &JoinContract,
    keys: &[JoinKey],
    terms: &JoinTerms,
    level: &LevelPlan,
    state: InputState,
    partial: usize,
    layout: &RowLayout,
    next_layout: &RowLayout,
    output: &LevelOutput,
    corrupt_label: BranchOffset,
    minus_one_reg: usize,
    sink: &mut JoinRowSink<'_>,
) -> Result<()> {
    let input = &contract.inputs[level.input];
    let row = program.alloc_registers(next_layout.width + 1);
    let weight_reg = row + next_layout.width;
    let cache_mark = resolver.expr_to_reg_cache.len();
    emit_bind_row(resolver, contract, terms, next_layout, row)?;
    let mut level_ctx = LevelEmitter {
        contract,
        keys,
        terms,
        level,
        partial,
        layout,
        next_layout,
        output,
        row,
        weight_reg,
        partial_weight_reg: program.alloc_register(),
        corrupt_label,
        minus_one_reg,
        partial_index: None,
    };

    let arrangement_cursor = open_arrangement(program, input);
    match &level.access {
        LevelAccess::Rowid { key } => level_ctx.emit_lookups(
            program,
            resolver,
            KeyLookup::Rowid { key: *key },
            arrangement_cursor,
            sink,
        )?,
        LevelAccess::Index { index, keys } => level_ctx.emit_lookups(
            program,
            resolver,
            KeyLookup::Index { index, keys },
            arrangement_cursor,
            sink,
        )?,
        LevelAccess::ScanAndProbe => {
            level_ctx.build_partial_index(program)?;
            level_ctx.emit_scan_and_probe(
                program,
                resolver,
                JoinSide::Btree,
                arrangement_cursor,
                false,
                sink,
            )?;
        }
        LevelAccess::Cross => level_ctx.emit_cross(
            program,
            resolver,
            JoinSide::Btree,
            arrangement_cursor,
            false,
            sink,
        )?,
    }
    if state == InputState::Old {
        let delta_cursor = input.delta()?.cursor_id;
        let delta_done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: delta_cursor,
            pc_if_empty: delta_done,
        });
        match level.access {
            LevelAccess::Cross => level_ctx.emit_cross(
                program,
                resolver,
                JoinSide::Delta,
                delta_cursor,
                true,
                sink,
            )?,
            _ => {
                if level_ctx.partial_index.is_none() {
                    level_ctx.build_partial_index(program)?;
                }
                level_ctx.emit_scan_and_probe(
                    program,
                    resolver,
                    JoinSide::Delta,
                    delta_cursor,
                    true,
                    sink,
                )?;
            }
        }
        program.preassign_label_to_next_insn(delta_done);
    }
    resolver.expr_to_reg_cache.truncate(cache_mark);
    Ok(())
}

/// How a level that looks its rows up by key finds them: a [`LevelAccess`]
/// with a rowid or index.
enum KeyLookup<'a> {
    Rowid {
        key: usize,
    },
    Index {
        index: &'a Arc<Index>,
        keys: &'a [usize],
    },
}

/// The state shared by the loops of one level.
struct LevelEmitter<'a> {
    contract: &'a JoinContract,
    keys: &'a [JoinKey],
    terms: &'a JoinTerms<'a>,
    level: &'a LevelPlan,
    /// The partial table this level reads.
    partial: usize,
    layout: &'a RowLayout,
    next_layout: &'a RowLayout,
    output: &'a LevelOutput,
    /// The extended row: `layout`'s images, the level input's image, weight.
    row: usize,
    weight_reg: usize,
    partial_weight_reg: usize,
    corrupt_label: BranchOffset,
    minus_one_reg: usize,
    /// An index over the partial rows' key values, once built.
    partial_index: Option<usize>,
}

impl LevelEmitter<'_> {
    /// For each partial row, find the level input's matching rows through
    /// its rowid or index.
    fn emit_lookups(
        &mut self,
        program: &mut ProgramBuilder,
        resolver: &mut Resolver,
        lookup: KeyLookup,
        arrangement_cursor: usize,
        sink: &mut JoinRowSink<'_>,
    ) -> Result<()> {
        let input = &self.contract.inputs[self.level.input];
        let loop_label = program.allocate_label();
        let next_partial = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.partial,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_label);
        self.emit_read_partial(program);
        let candidate_loop = match lookup {
            KeyLookup::Rowid { key } => {
                // Position on the first rowid >= the key, the way `=`
                // compares them, so a REAL key such as -9.223372036854776e18
                // finds rowid -9223372036854775808; SeekRowid only converts
                // REALs that integer affinity converts. A key that is not an
                // integer lands on the next rowid, and the key term checked
                // at this level rejects that row.
                let probe_reg = self.bound_key_reg(key);
                program.emit_insn(Insn::SeekGE {
                    is_index: false,
                    cursor_id: arrangement_cursor,
                    start_reg: probe_reg,
                    num_regs: 1,
                    target_pc: next_partial,
                    eq_only: true,
                    null_matching_mask: Default::default(),
                });
                None
            }
            KeyLookup::Index { index, keys } => {
                let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
                program.emit_insn(Insn::OpenRead {
                    cursor_id: index_cursor,
                    root_page: index.root_page,
                    db: 0,
                });
                let probe_start = program.alloc_registers(keys.len());
                for (position, key) in keys.iter().enumerate() {
                    program.emit_insn(Insn::Copy {
                        src_reg: self.bound_key_reg(*key),
                        dst_reg: probe_start + position,
                        extra_amount: 0,
                    });
                }
                emit_key_affinity(
                    program,
                    keys.iter().map(|key| self.keys[*key].affinity),
                    probe_start,
                );
                program.emit_insn(Insn::SeekGE {
                    is_index: true,
                    cursor_id: index_cursor,
                    start_reg: probe_start,
                    num_regs: keys.len(),
                    target_pc: next_partial,
                    eq_only: true,
                    null_matching_mask: Default::default(),
                });
                let candidate_loop = program.allocate_label();
                program.preassign_label_to_next_insn(candidate_loop);
                program.emit_insn(Insn::IdxGT {
                    cursor_id: index_cursor,
                    start_reg: probe_start,
                    num_regs: keys.len(),
                    target_pc: next_partial,
                });
                let rowid_reg = program.alloc_register();
                program.emit_insn(Insn::IdxRowId {
                    cursor_id: index_cursor,
                    dest: rowid_reg,
                });
                program.emit_insn(Insn::SeekRowid {
                    cursor_id: arrangement_cursor,
                    src_reg: rowid_reg,
                    target_pc: self.corrupt_label,
                });
                Some((candidate_loop, index_cursor))
            }
        };
        let next_candidate = program.allocate_label();
        emit_read_image(
            program,
            input,
            JoinSide::Btree,
            arrangement_cursor,
            self.next_layout.image(self.level.input),
            self.row,
        )?;
        let source_weight = emit_read_weight(program, input, JoinSide::Btree, arrangement_cursor)?;
        self.emit_extended_row(
            program,
            resolver,
            source_weight,
            false,
            next_candidate,
            sink,
        )?;
        program.preassign_label_to_next_insn(next_candidate);
        if let Some((candidate_loop, index_cursor)) = candidate_loop {
            program.emit_insn(Insn::Next {
                cursor_id: index_cursor,
                pc_if_next: candidate_loop,
                fullscan: false,
                is_index: true,
            });
        }
        program.preassign_label_to_next_insn(next_partial);
        program.emit_insn(Insn::Next {
            cursor_id: self.partial,
            pc_if_next: loop_label,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(done);
        Ok(())
    }

    /// Index the partial rows by their values of the level's join keys, with
    /// the affinity and collation `=` uses, so that a probe with the level
    /// input's values finds every partial row `=` matches.
    fn build_partial_index(&mut self, program: &mut ProgramBuilder) -> Result<()> {
        let key_count = self.level.keys.len();
        let columns = self
            .level
            .keys
            .iter()
            .enumerate()
            .map(|(position, key)| IndexColumn {
                name: format!("k{position}"),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: position,
                collation: Some(self.keys[*key].collation),
                default: None,
                expr: None,
            })
            .try_collect::<crate::alloc::Vec<_>>()?;
        let index = Arc::new(Index {
            name: format!("join_keys_{}", self.partial),
            table_name: format!("join_partial_{}", self.partial),
            root_page: 0,
            columns,
            unique: false,
            ephemeral: true,
            has_rowid: true,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        });
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        program.emit_insn(Insn::OpenAutoindex {
            cursor_id: index_cursor,
        });
        let record_start = program.alloc_registers(key_count + 1);
        let record_reg = program.alloc_register();
        let build_loop = program.allocate_label();
        let built = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.partial,
            pc_if_empty: built,
        });
        program.preassign_label_to_next_insn(build_loop);
        for (position, key) in self.level.keys.iter().enumerate() {
            let (other, column) = self.keys[*key]
                .other_input(self.level.input)
                .expect("a level key reaches a bound input");
            program.emit_insn(Insn::Column {
                cursor_id: self.partial,
                column: self.layout.key_offset(other, column),
                dest: record_start + position,
                default: None,
            });
        }
        emit_key_affinity(
            program,
            self.level.keys.iter().map(|key| self.keys[*key].affinity),
            record_start,
        );
        program.emit_insn(Insn::RowId {
            cursor_id: self.partial,
            dest: record_start + key_count,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(record_start),
            count: to_u32(key_count + 1),
            dest_reg: to_u32(record_reg),
            index_name: Some(index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: index_cursor,
            record_reg,
            unpacked_start: Some(record_start),
            unpacked_count: Some(to_u32(key_count + 1)),
            flags: IdxInsertFlags::new().use_seek(false),
        });
        program.emit_insn(Insn::Next {
            cursor_id: self.partial,
            pc_if_next: build_loop,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(built);
        self.partial_index = Some(index_cursor);
        Ok(())
    }

    /// Read every row of the level input from `cursor` once, and look up the
    /// partial rows it matches in the partial-row index.
    fn emit_scan_and_probe(
        &mut self,
        program: &mut ProgramBuilder,
        resolver: &mut Resolver,
        side: JoinSide,
        cursor: usize,
        negate: bool,
        sink: &mut JoinRowSink<'_>,
    ) -> Result<()> {
        let input = &self.contract.inputs[self.level.input];
        let index_cursor = self
            .partial_index
            .expect("the partial-row index is built before it is probed");
        let key_count = self.level.keys.len();
        let probe_start = program.alloc_registers(key_count);
        let loop_label = program.allocate_label();
        let next_input_row = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_label);
        for (position, key) in self.level.keys.iter().enumerate() {
            let column = self.keys[*key]
                .column_of(self.level.input)
                .expect("a level key reads the level input");
            emit_read_key_column(program, input, side, cursor, column, probe_start + position)?;
        }
        emit_key_affinity(
            program,
            self.level.keys.iter().map(|key| self.keys[*key].affinity),
            probe_start,
        );
        program.emit_insn(Insn::SeekGE {
            is_index: true,
            cursor_id: index_cursor,
            start_reg: probe_start,
            num_regs: key_count,
            target_pc: next_input_row,
            eq_only: true,
            null_matching_mask: Default::default(),
        });
        // Most rows of a scanned table match no partial row: read the whole
        // row only for those that do.
        emit_read_image(
            program,
            input,
            side,
            cursor,
            self.next_layout.image(self.level.input),
            self.row,
        )?;
        let source_weight = emit_read_weight(program, input, side, cursor)?;
        let candidate_loop = program.allocate_label();
        let next_candidate = program.allocate_label();
        program.preassign_label_to_next_insn(candidate_loop);
        program.emit_insn(Insn::IdxGT {
            cursor_id: index_cursor,
            start_reg: probe_start,
            num_regs: key_count,
            target_pc: next_input_row,
        });
        let partial_rowid_reg = program.alloc_register();
        program.emit_insn(Insn::IdxRowId {
            cursor_id: index_cursor,
            dest: partial_rowid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: self.partial,
            src_reg: partial_rowid_reg,
            target_pc: self.corrupt_label,
        });
        self.emit_read_partial(program);
        self.emit_extended_row(
            program,
            resolver,
            source_weight,
            negate,
            next_candidate,
            sink,
        )?;
        program.preassign_label_to_next_insn(next_candidate);
        program.emit_insn(Insn::Next {
            cursor_id: index_cursor,
            pc_if_next: candidate_loop,
            fullscan: false,
            is_index: true,
        });
        program.preassign_label_to_next_insn(next_input_row);
        program.emit_insn(Insn::Next {
            cursor_id: cursor,
            pc_if_next: loop_label,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(done);
        Ok(())
    }

    /// Pair every partial row with every row of the level input.
    fn emit_cross(
        &mut self,
        program: &mut ProgramBuilder,
        resolver: &mut Resolver,
        side: JoinSide,
        cursor: usize,
        negate: bool,
        sink: &mut JoinRowSink<'_>,
    ) -> Result<()> {
        let input = &self.contract.inputs[self.level.input];
        let partial_loop = program.allocate_label();
        let next_partial = program.allocate_label();
        let input_loop = program.allocate_label();
        let next_input_row = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.partial,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(partial_loop);
        self.emit_read_partial(program);
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor,
            pc_if_empty: next_partial,
        });
        program.preassign_label_to_next_insn(input_loop);
        emit_read_image(
            program,
            input,
            side,
            cursor,
            self.next_layout.image(self.level.input),
            self.row,
        )?;
        let source_weight = emit_read_weight(program, input, side, cursor)?;
        self.emit_extended_row(
            program,
            resolver,
            source_weight,
            negate,
            next_input_row,
            sink,
        )?;
        program.preassign_label_to_next_insn(next_input_row);
        program.emit_insn(Insn::Next {
            cursor_id: cursor,
            pc_if_next: input_loop,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(next_partial);
        program.emit_insn(Insn::Next {
            cursor_id: self.partial,
            pc_if_next: partial_loop,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(done);
        Ok(())
    }

    /// Copy the current partial row's images into the start of the extended
    /// row, and its weight aside.
    fn emit_read_partial(&self, program: &mut ProgramBuilder) {
        for column in 0..self.layout.width {
            program.emit_insn(Insn::Column {
                cursor_id: self.partial,
                column,
                dest: self.row + column,
                default: None,
            });
        }
        program.emit_insn(Insn::Column {
            cursor_id: self.partial,
            column: self.layout.width,
            dest: self.partial_weight_reg,
            default: None,
        });
    }

    /// With the extended row in registers: compute its weight, check the
    /// level's terms, and write it to the next partial table or the output.
    fn emit_extended_row(
        &self,
        program: &mut ProgramBuilder,
        resolver: &mut Resolver,
        source_weight: Option<usize>,
        negate: bool,
        skip_label: BranchOffset,
        sink: &mut JoinRowSink<'_>,
    ) -> Result<()> {
        emit_terms(program, resolver, self.terms, &self.level.terms, skip_label)?;
        program.emit_insn(Insn::Copy {
            src_reg: self.partial_weight_reg,
            dst_reg: self.weight_reg,
            extra_amount: 0,
        });
        for factor in source_weight
            .into_iter()
            .chain(negate.then_some(self.minus_one_reg))
        {
            program.emit_insn(Insn::Multiply {
                lhs: self.weight_reg,
                rhs: factor,
                dest: self.weight_reg,
            });
        }
        match self.output {
            LevelOutput::Partial(next) => {
                emit_partial_insert(program, *next, self.row, self.next_layout.width);
            }
            LevelOutput::Sink => sink(
                program,
                &JoinRow {
                    layout: self.next_layout,
                    start: self.row,
                    weight_reg: self.weight_reg,
                },
            )?,
        }
        Ok(())
    }

    /// The register holding the bound side's value of `key` in the current
    /// partial row.
    fn bound_key_reg(&self, key: usize) -> usize {
        let (other, column) = self.keys[key]
            .other_input(self.level.input)
            .expect("a level key reaches a bound input");
        self.row + self.layout.key_offset(other, column)
    }
}

/// Where one input's values sit in a row of bound inputs, relative to the
/// row's start: its identity, the SQL rowids of its bindings that it
/// carries, and its logical values.
#[derive(Debug, Clone)]
struct InputImage {
    input: usize,
    identity_start: usize,
    identity_width: usize,
    binding_rowids: Vec<Option<usize>>,
    value_start: usize,
}

/// The layout of a row of bound inputs: their images in the order they were
/// bound. A partial table stores this row followed by its weight.
#[derive(Debug, Clone, Default)]
struct RowLayout {
    images: Vec<InputImage>,
    width: usize,
}

impl RowLayout {
    fn with_input(&self, contract: &JoinContract, input: usize) -> RowLayout {
        let contract_input = &contract.inputs[input];
        let identity_start = self.width;
        let identity_width = contract_input.arrangement.identity().width();
        let mut next = identity_start + identity_width;
        let binding_rowids = contract_input
            .arrangement
            .binding_rowid_columns()
            .iter()
            .map(|column| {
                column.map(|_| {
                    next += 1;
                    next - 1
                })
            })
            .collect();
        let value_start = next;
        let mut images = self.images.clone();
        images.push(InputImage {
            input,
            identity_start,
            identity_width,
            binding_rowids,
            value_start,
        });
        RowLayout {
            images,
            width: value_start + contract_input.schema.len(),
        }
    }

    fn image(&self, input: usize) -> &InputImage {
        self.images
            .iter()
            .find(|image| image.input == input)
            .expect("the input is bound in this row")
    }

    /// The offset of one key column of a bound input.
    fn key_offset(&self, input: usize, column: JoinKeyColumn) -> usize {
        let image = self.image(input);
        match column {
            JoinKeyColumn::Value(column) => image.value_start + column,
            JoinKeyColumn::BindingRowid(binding) => image.binding_rowids[binding]
                .expect("a rowid join key reads a rowid its input carries"),
        }
    }
}

fn open_partial_table(
    program: &mut ProgramBuilder,
    view_name: &str,
    layout: &RowLayout,
) -> Result<usize> {
    let table = scratch_table(
        &format!("{view_name}_join_partial_{}", layout.images.len()),
        layout.width + 1,
    )?;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(Arc::new(table)));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });
    Ok(cursor_id)
}

/// Append the row at `row` (its `width` images followed by the weight) to a
/// partial table.
fn emit_partial_insert(program: &mut ProgramBuilder, partial: usize, row: usize, width: usize) {
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(row),
        count: to_u32(width + 1),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: partial,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: partial,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
}

fn open_arrangement(program: &mut ProgramBuilder, input: &JoinInputContract) -> usize {
    let storage = input.arrangement.table();
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(storage.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id,
        root_page: storage.root_page,
        db: 0,
    });
    cursor_id
}

/// Read the current row of one input into its image in a row of bound
/// inputs.
fn emit_read_image(
    program: &mut ProgramBuilder,
    input: &JoinInputContract,
    side: JoinSide,
    cursor_id: usize,
    image: &InputImage,
    row: usize,
) -> Result<()> {
    let identity_width =
        emit_join_input_identity(program, side, input, cursor_id, row + image.identity_start)?;
    turso_assert!(
        identity_width == image.identity_width,
        "a join input's delta and arrangement must have the same identity"
    );
    for (binding, slot) in image.binding_rowids.iter().enumerate() {
        if let Some(slot) = slot {
            turso_assert!(
                emit_join_input_binding_rowid(
                    program,
                    side,
                    input,
                    cursor_id,
                    binding,
                    row + slot
                )?,
                "a join input's delta must carry the rowids its arrangement carries"
            );
        }
    }
    for column in 0..input.schema.len() {
        emit_join_input_value(
            program,
            side,
            input,
            cursor_id,
            column,
            row + image.value_start + column,
        )?;
    }
    Ok(())
}

/// Read the weight of the current row of one input: a change's signed
/// weight, or an arrangement row's multiplicity. `None` means one.
fn emit_read_weight(
    program: &mut ProgramBuilder,
    input: &JoinInputContract,
    side: JoinSide,
    cursor_id: usize,
) -> Result<Option<usize>> {
    let column = match side {
        JoinSide::Delta => Some(input.delta()?.weight_column),
        JoinSide::Btree => input.arrangement.count_column(),
    };
    Ok(column.map(|column| {
        let weight_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id,
            column,
            dest: weight_reg,
            default: None,
        });
        weight_reg
    }))
}

fn emit_read_key_column(
    program: &mut ProgramBuilder,
    input: &JoinInputContract,
    side: JoinSide,
    cursor_id: usize,
    column: JoinKeyColumn,
    dest: usize,
) -> Result<()> {
    match column {
        JoinKeyColumn::Value(column) => {
            emit_join_input_value(program, side, input, cursor_id, column, dest)
        }
        JoinKeyColumn::BindingRowid(binding) => {
            turso_assert!(
                emit_join_input_binding_rowid(program, side, input, cursor_id, binding, dest)?,
                "a join key reads a rowid its input does not carry"
            );
            Ok(())
        }
    }
}

/// Make the values and rowids of every input bound in `layout` available to
/// expressions, read from the registers at `row`.
fn emit_bind_row(
    resolver: &mut Resolver,
    contract: &JoinContract,
    terms: &JoinTerms,
    layout: &RowLayout,
    row: usize,
) -> Result<()> {
    for image in &layout.images {
        let input = &contract.inputs[image.input];
        for (column, expr) in input.schema.columns.iter().enumerate() {
            let Some(expr) = expr else {
                continue;
            };
            let bound = remap_bound_expr(expr, terms.binding_remap)?;
            cache_stream_expr_reg(
                resolver,
                bound,
                row + image.value_start + column,
                terms.table_references,
            )?;
        }
        for (binding, slot) in input.schema.bindings.iter().zip(&image.binding_rowids) {
            let Some(slot) = slot else {
                continue;
            };
            let phase_id = *terms
                .binding_remap
                .get(&binding.logical_id)
                .ok_or_else(|| {
                    LimboError::InternalError(
                        "a join input table has no table reference in the join program".to_string(),
                    )
                })?;
            resolver.cache_expr_reg(
                std::borrow::Cow::Owned(ast::Expr::RowId {
                    database: None,
                    table: phase_id,
                }),
                row + slot,
                false,
                None,
            );
        }
    }
    Ok(())
}

/// Check `selected` terms: a FALSE or NULL result jumps to `skip_label`.
fn emit_terms(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    terms: &JoinTerms,
    selected: &[usize],
    skip_label: BranchOffset,
) -> Result<()> {
    for term in selected {
        let bound = remap_bound_expr(&terms.exprs[*term], terms.binding_remap)?;
        let term_true = program.allocate_label();
        translate_condition_expr(
            program,
            terms.table_references,
            &bound,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: term_true,
                jump_target_when_false: skip_label,
                jump_target_when_null: skip_label,
            },
            resolver,
        )?;
        program.preassign_label_to_next_insn(term_true);
    }
    Ok(())
}

/// A table reference for every binding of the join's inputs, so that its
/// terms can be translated or compared, each under the id `table_id` gives
/// it.
fn input_table_references(
    inputs: &[KeyInput],
    mut table_id: impl FnMut(ast::TableInternalId) -> ast::TableInternalId,
) -> (BindingRemap, TableReferences) {
    let mut binding_remap = BindingRemap::default();
    let tables = inputs
        .iter()
        .flat_map(|input| input.schema.bindings.iter())
        .map(|binding| {
            let id = table_id(binding.logical_id);
            let previous = binding_remap.insert(binding.logical_id, id);
            turso_assert!(
                previous.is_none(),
                "a join must expose each logical binding exactly once"
            );
            make_joined_table(&binding.table, &binding.identifier, id)
        })
        .collect();
    (binding_remap, TableReferences::new(tables, vec![]))
}

/// An index one inner join needs on a base table, so that a level reading
/// that table can look its rows up by key instead of reading it whole.
pub(super) struct JoinKeyIndex {
    pub(super) table: Arc<BTreeTable>,
    /// Table columns with the collation `=` compares them with, in key order.
    pub(super) columns: Vec<(usize, CollationSeq)>,
    /// The terms of the join that read only this table, written over its
    /// column names, as a partial index WHERE.
    pub(super) where_clause: Option<ast::Expr>,
}

/// For each base-table input and each other input it shares `=` terms with,
/// an index on this input's compared columns, unless a rowid or a user index
/// already finds those rows. Indexes that views created are not counted, so
/// the result lists every such index the join uses. A key whose column
/// affinity would make the index sort values differently from how `=`
/// compares them is left out (`Affinity::index_affinity_ok`); a level whose
/// keys are all left out keeps reading its table whole.
///
/// `tables[i]` is the base table of input `i` when the view may index it:
/// a plain scan of a table, not a derived input or a materialized view.
pub(super) fn needed_join_key_indexes(
    inputs: &[KeyInput],
    tables: &[Option<Arc<BTreeTable>>],
    on: &[ast::Expr],
    schema: &Schema,
    resolver: &Resolver,
) -> Result<Vec<JoinKeyIndex>> {
    let (binding_remap, table_references) = input_table_references(inputs, |id| id);
    let keys = find_join_keys(inputs, on, &binding_remap, &table_references, resolver)?;
    let term_inputs = on
        .iter()
        .map(|term| term_input_positions(inputs, term))
        .collect::<Result<Vec<_>>>()?;
    let mut needed: Vec<JoinKeyIndex> = Vec::new();
    for (input, table) in tables.iter().enumerate() {
        let Some(table) = table else {
            continue;
        };
        let filter = input_filter(inputs, on, &term_inputs, input);
        let arrangement = base_arrangement(table.clone(), None);
        for other in (0..inputs.len()).filter(|other| *other != input) {
            let shared_keys = (0..keys.len())
                .filter(|key| {
                    keys[*key]
                        .other_input(input)
                        .is_some_and(|(side, _)| side == other)
                })
                .collect::<Vec<_>>();
            if shared_keys.is_empty()
                || persistent_key_access(
                    schema,
                    &arrangement,
                    input,
                    &keys,
                    &shared_keys,
                    filter.as_ref(),
                    |index| !is_materialized_view_join_key_index(&index.name),
                )
                .is_some()
            {
                continue;
            }
            let mut columns = Vec::new();
            for key in &shared_keys {
                let Some(JoinKeyColumn::Value(column)) = keys[*key].column_of(input) else {
                    continue;
                };
                let usable = table.columns()[column]
                    .affinity()
                    .index_affinity_ok(keys[*key].affinity);
                if usable && !columns.iter().any(|(indexed, _)| *indexed == column) {
                    columns.push((column, keys[*key].collation));
                }
            }
            if !columns.is_empty() {
                needed.push(JoinKeyIndex {
                    table: table.clone(),
                    columns,
                    where_clause: filter.as_ref().and_then(InputFilter::index_where),
                });
            }
        }
    }
    Ok(needed)
}

/// The terms of a join that read only one input, when that input is one
/// table. A partial index of the table serves the input when every row the
/// terms accept is in the index.
struct InputFilter<'a> {
    /// The input's table, under the id the terms read it by.
    table: JoinedTable,
    terms: Vec<&'a ast::Expr>,
}

fn input_filter<'a>(
    inputs: &[KeyInput],
    on: &'a [ast::Expr],
    term_inputs: &[Vec<usize>],
    input: usize,
) -> Option<InputFilter<'a>> {
    let [binding] = inputs[input].schema.bindings.as_slice() else {
        return None;
    };
    Some(InputFilter {
        table: make_joined_table(&binding.table, &binding.identifier, binding.logical_id),
        terms: on
            .iter()
            .zip(term_inputs)
            .filter(|(_, reads)| reads.as_slice() == [input])
            .map(|(term, _)| term)
            .collect(),
    })
}

impl InputFilter<'_> {
    /// Whether every row the terms accept passes `index_where`: each of its
    /// AND-ed parts is one of the terms, compared the way the optimizer
    /// matches a partial index to a query.
    fn implies(&self, index_where: &ast::Expr) -> bool {
        let mut bound = index_where.clone();
        bind_partial_index_columns(&mut bound, &self.table);
        if rewrite_between_exprs(&mut bound).is_err() {
            return false;
        }
        let mut parts: Vec<ast::Expr> = Vec::new();
        break_predicate_at_and_boundaries(&bound, &mut parts);
        parts.iter().all(|part| {
            self.terms
                .iter()
                .any(|term| exprs_are_equivalent(part, term))
        })
    }

    /// The terms written over the table's column names, as the WHERE of a
    /// partial index, in the form the parser reads back from
    /// `sqlite_schema`. A term that reads the rowid is left out, because a
    /// partial index whose WHERE reads the rowid makes every INSERT into
    /// the table fail (#9663); the join still checks it. `None` when no term
    /// is left or the text does not read back as one expression.
    fn index_where(&self) -> Option<ast::Expr> {
        let columns = self.table.columns();
        let mut combined: Option<ast::Expr> = None;
        for term in &self.terms {
            let mut written = (*term).clone();
            let mut writable = true;
            walk_expr_mut(&mut written, &mut |node| {
                match node {
                    ast::Expr::Column { column, .. } => {
                        match columns.get(*column).and_then(|column| column.name.clone()) {
                            Some(name) => *node = ast::Expr::Id(ast::Name::exact(name)),
                            None => writable = false,
                        }
                    }
                    ast::Expr::RowId { .. } => writable = false,
                    _ => {}
                }
                Ok(WalkControl::Continue)
            })
            .ok()?;
            if !writable {
                continue;
            }
            let written = ast::Expr::Parenthesized(vec![Box::new(written)]);
            combined = Some(match combined {
                None => written,
                Some(earlier) => {
                    ast::Expr::Binary(Box::new(earlier), ast::Operator::And, Box::new(written))
                }
            });
        }
        let sql = format!("SELECT {}", combined?);
        let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
        let Some(Ok(ast::Cmd::Stmt(ast::Stmt::Select(select)))) = parser.next() else {
            return None;
        };
        if !select.body.compounds.is_empty() {
            return None;
        }
        let ast::OneSelect::Select { mut columns, .. } = select.body.select else {
            return None;
        };
        match columns.pop() {
            Some(ast::ResultColumn::Expr(expr, _)) if columns.is_empty() => Some(*expr),
            _ => None,
        }
    }
}

/// The positions of the join inputs a term reads.
fn term_input_positions(inputs: &[KeyInput], term: &ast::Expr) -> Result<Vec<usize>> {
    let mut positions = Vec::new();
    walk_expr(term, &mut |node| {
        if let ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } = node {
            let position = inputs
                .iter()
                .position(|input| {
                    input
                        .schema
                        .bindings
                        .iter()
                        .any(|binding| binding.logical_id == *table)
                })
                .ok_or_else(|| {
                    LimboError::InternalError(
                        "a join term reads a binding outside its inputs".to_string(),
                    )
                })?;
            if !positions.contains(&position) {
                positions.push(position);
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(positions)
}

fn emit_arrangement_column_value(
    program: &mut ProgramBuilder,
    column: ArrangementIdentityColumn,
    cursor_id: usize,
    dest: usize,
) {
    match column {
        ArrangementIdentityColumn::RowId => {
            program.emit_insn(Insn::RowId { cursor_id, dest });
        }
        ArrangementIdentityColumn::Column(column) => {
            program.emit_insn(Insn::Column {
                cursor_id,
                column,
                dest,
                default: None,
            });
        }
    }
}

fn emit_arrangement_identity_value(
    program: &mut ProgramBuilder,
    arrangement: &ArrangementHandle,
    cursor_id: usize,
    identity_column: usize,
    dest: usize,
) {
    emit_arrangement_column_value(
        program,
        arrangement.identity_columns()[identity_column],
        cursor_id,
        dest,
    );
}

/// Read the SQL rowid of one binding of a join input. Returns false when the
/// input does not carry that rowid.
fn emit_join_input_binding_rowid(
    program: &mut ProgramBuilder,
    side: JoinSide,
    input: &JoinInputContract,
    cursor_id: usize,
    binding: usize,
    dest: usize,
) -> Result<bool> {
    let column = match side {
        JoinSide::Delta => input
            .delta()?
            .binding_rowid_columns
            .get(binding)
            .copied()
            .flatten()
            .map(ArrangementIdentityColumn::Column),
        JoinSide::Btree => input
            .arrangement
            .binding_rowid_columns()
            .get(binding)
            .copied()
            .flatten(),
    };
    let Some(column) = column else {
        return Ok(false);
    };
    emit_arrangement_column_value(program, column, cursor_id, dest);
    Ok(true)
}

fn emit_join_input_identity(
    program: &mut ProgramBuilder,
    side: JoinSide,
    input: &JoinInputContract,
    cursor_id: usize,
    dest: usize,
) -> Result<usize> {
    match side {
        JoinSide::Delta => {
            let width = input.delta()?.identity.width();
            for column in 0..width {
                program.emit_insn(Insn::Column {
                    cursor_id,
                    column,
                    dest: dest + column,
                    default: None,
                });
            }
            Ok(width)
        }
        JoinSide::Btree => {
            let width = input.arrangement.identity().width();
            for column in 0..width {
                emit_arrangement_identity_value(
                    program,
                    &input.arrangement,
                    cursor_id,
                    column,
                    dest + column,
                );
            }
            Ok(width)
        }
    }
}

/// Read one logical column of a join input.
fn emit_join_input_value(
    program: &mut ProgramBuilder,
    side: JoinSide,
    input: &JoinInputContract,
    cursor_id: usize,
    logical_column: usize,
    dest: usize,
) -> Result<()> {
    match side {
        JoinSide::Delta => {
            program.emit_insn(Insn::Column {
                cursor_id,
                column: input.delta()?.value_start + logical_column,
                dest,
                default: None,
            });
        }
        JoinSide::Btree => program.emit_column_or_rowid(
            cursor_id,
            input.arrangement.value_columns()[logical_column],
            dest,
        ),
    }
    Ok(())
}

/// One column of a join input that a join key compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinKeyColumn {
    /// A logical column of the input.
    Value(usize),
    /// The SQL rowid of one of the input's bindings.
    BindingRowid(usize),
}

/// An `a = b` term of a join where `a` is a column of one input and `b` a
/// column of another.
struct JoinKey {
    /// The two compared inputs, each with its compared column.
    sides: [(usize, JoinKeyColumn); 2],
    /// The affinity `=` applies to its operands before comparing them.
    affinity: Affinity,
    /// The collation `=` compares text with.
    collation: CollationSeq,
}

impl JoinKey {
    /// The compared column of `input`, if the key reads it.
    fn column_of(&self, input: usize) -> Option<JoinKeyColumn> {
        self.sides
            .iter()
            .find(|(side, _)| *side == input)
            .map(|(_, column)| *column)
    }

    /// The other side of the key, if one side is `input`.
    fn other_input(&self, input: usize) -> Option<(usize, JoinKeyColumn)> {
        match self.sides {
            [(first, _), other] if first == input => Some(other),
            [other, (second, _)] if second == input => Some(other),
            _ => None,
        }
    }
}

fn find_join_keys(
    inputs: &[KeyInput],
    on: &[ast::Expr],
    binding_remap: &BindingRemap,
    table_references: &TableReferences,
    resolver: &Resolver,
) -> Result<Vec<JoinKey>> {
    let mut keys = Vec::new();
    for condition in on {
        let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = condition else {
            continue;
        };
        let (Some(lhs_side), Some(rhs_side)) =
            (join_key_column(inputs, lhs), join_key_column(inputs, rhs))
        else {
            continue;
        };
        if lhs_side.0 == rhs_side.0 {
            continue;
        }
        let lhs = remap_bound_expr(lhs, binding_remap)?;
        let rhs = remap_bound_expr(rhs, binding_remap)?;
        if expr_is_array(&lhs, Some(table_references))
            || expr_is_array(&rhs, Some(table_references))
        {
            continue;
        }
        // The same rules the term itself is compiled with.
        keys.push(JoinKey {
            sides: [lhs_side, rhs_side],
            affinity: comparison_affinity(&lhs, &rhs, Some(table_references), Some(resolver)),
            collation: join_key_collation(&lhs, &rhs, table_references, resolver)?,
        });
    }
    Ok(keys)
}

/// The collation `lhs = rhs` compares text with in the compiled term. The
/// operands are read from the expression cache, where a column carries its
/// declared collation (BINARY by default) and a rowid carries none; the left
/// operand's collation wins.
fn join_key_collation(
    lhs: &ast::Expr,
    rhs: &ast::Expr,
    table_references: &TableReferences,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    let operand_collation = |expr: &ast::Expr| match expr {
        ast::Expr::RowId { .. } => Ok(None),
        _ => {
            get_expr_collation_ctx_with_symbols(expr, table_references, Some(resolver.symbol_table))
        }
    };
    Ok(match (operand_collation(lhs)?, operand_collation(rhs)?) {
        (Some((collation, _)), _) | (None, Some((collation, _))) => collation,
        (None, None) => CollationSeq::Binary,
    })
}

/// Which input an operand of `=` is a plain column of, if any.
fn join_key_column(inputs: &[KeyInput], expr: &ast::Expr) -> Option<(usize, JoinKeyColumn)> {
    inputs
        .iter()
        .enumerate()
        .find_map(|(position, input)| match expr {
            ast::Expr::Column { .. } => input
                .schema
                .columns
                .iter()
                .position(|column| column.as_ref() == Some(expr))
                .map(|column| (position, JoinKeyColumn::Value(column))),
            ast::Expr::RowId { table, .. } => input
                .schema
                .bindings
                .iter()
                .position(|binding| binding.logical_id == *table)
                .filter(|binding| input.binding_rowids[*binding])
                .map(|binding| (position, JoinKeyColumn::BindingRowid(binding))),
            _ => None,
        })
}

/// A rowid or index of `input`'s arrangement that finds every row the
/// `level_keys` (indexes into `keys`) match. An index column is usable only
/// when it sorts values the way `=` compares them: the same collation, and
/// an affinity under which the comparison does not convert the stored values
/// (SQLite's `sqlite3IndexAffinityOk`). Only indexes `may_use` accepts
/// are considered.
fn persistent_key_access(
    schema: &Schema,
    arrangement: &ArrangementHandle,
    position: usize,
    keys: &[JoinKey],
    level_keys: &[usize],
    input_filter: Option<&InputFilter>,
    may_use: impl Fn(&Index) -> bool,
) -> Option<LevelAccess> {
    let table = arrangement.table();
    if !table.has_rowid {
        return None;
    }
    let column_of = |key: usize| {
        keys[key]
            .column_of(position)
            .expect("a level key reads the level input")
    };
    let physical_column = |key: usize| match column_of(key) {
        JoinKeyColumn::Value(column) => Some(arrangement.value_columns()[column]),
        JoinKeyColumn::BindingRowid(_) => None,
    };
    for &key in level_keys {
        let is_rowid = match column_of(key) {
            JoinKeyColumn::BindingRowid(binding) => matches!(
                arrangement.binding_rowid_columns()[binding],
                Some(ArrangementIdentityColumn::RowId)
            ),
            JoinKeyColumn::Value(_) => {
                physical_column(key).is_some_and(|column| table.columns()[column].is_rowid_alias())
            }
        };
        if is_rowid {
            return Some(LevelAccess::Rowid { key });
        }
    }

    let mut best: Option<(Arc<Index>, Vec<usize>)> = None;
    for index in schema
        .get_indices(&table.name)
        .filter(|index| may_use(index))
    {
        // A partial index holds only the rows its WHERE accepts. It serves
        // the level when the terms the level checks on this input anyway
        // imply that WHERE, so the rows it leaves out could never match.
        let where_serves_level = match &index.where_clause {
            None => true,
            Some(index_where) => input_filter.is_some_and(|filter| filter.implies(index_where)),
        };
        if !where_serves_level || index.index_method.is_some() || !index.has_rowid {
            continue;
        }
        let mut used = Vec::new();
        for index_column in &index.columns {
            if index_column.expr.is_some() || index_column.order != SortOrder::Asc {
                break;
            }
            let table_column = &table.columns()[index_column.pos_in_table];
            let index_collation = index_column
                .collation
                .unwrap_or_else(|| table_column.collation());
            let Some(key) = level_keys.iter().copied().find(|key| {
                !used.contains(key)
                    && physical_column(*key) == Some(index_column.pos_in_table)
                    && table_column
                        .affinity()
                        .index_affinity_ok(keys[*key].affinity)
                    && same_collation(index_collation, keys[*key].collation)
            }) else {
                break;
            };
            used.push(key);
        }
        if !used.is_empty()
            && best
                .as_ref()
                .is_none_or(|(_, keys)| used.len() > keys.len())
        {
            best = Some((index.clone(), used));
        }
    }
    best.map(|(index, keys)| LevelAccess::Index { index, keys })
}

fn same_collation(a: CollationSeq, b: CollationSeq) -> bool {
    let normalize = |collation| match collation {
        CollationSeq::Unset => CollationSeq::Binary,
        other => other,
    };
    normalize(a) == normalize(b)
}

/// Apply the comparison affinity of each key to consecutive registers.
fn emit_key_affinity(
    program: &mut ProgramBuilder,
    affinities: impl ExactSizeIterator<Item = Affinity>,
    start_reg: usize,
) {
    let Some(count) = NonZeroUsize::new(affinities.len()) else {
        return;
    };
    program.emit_insn(Insn::Affinity {
        start_reg,
        count,
        affinities: affinities.map(|affinity| affinity.aff_mask()).collect(),
    });
}

#[cfg(test)]
mod tests {
    use super::super::{compile_maintenance_program, MaintenanceInput};
    use crate::sync::Arc;
    use crate::vdbe::builder::CursorType;
    use crate::vdbe::insn::Insn;
    use crate::vdbe::Program;
    use crate::{ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, SqliteDialect};

    /// A join on an INTEGER PRIMARY KEY and an indexed column never reads
    /// either table in full: each changed row, and each row of the other
    /// table while populating, finds its matches by rowid or through the
    /// index.
    #[test]
    fn key_join_never_reads_a_whole_table() {
        let conn = open_views_db("join-key-lookup.db");
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE child(pid INT, v INT)").unwrap();
        conn.execute("CREATE INDEX child_pid ON child(pid)")
            .unwrap();
        conn.execute(
            "CREATE MATERIALIZED VIEW v AS \
             SELECT c.v, p.name FROM child c JOIN parent p ON c.pid = p.id",
        )
        .unwrap();

        let maintenance = compile_view_program(&conn, "v", MaintenanceInput::TransactionDelta);
        assert_eq!(full_reads_of(&maintenance, "parent"), 0);
        assert_eq!(full_reads_of(&maintenance, "child"), 0);
        assert!(rowid_lookups_of(&maintenance, "parent") > 0);
        assert!(rowid_lookups_of(&maintenance, "child") > 0);

        let population = compile_view_program(&conn, "v", MaintenanceInput::BaseTable);
        assert_eq!(full_reads_of(&population, "parent"), 0);
        assert!(rowid_lookups_of(&population, "parent") > 0);
    }

    /// A chain of six tables whose join keys are rowids or indexed: no phase
    /// of maintenance reads a whole table, and population reads only the
    /// table it starts from.
    #[test]
    fn keyed_six_way_join_never_reads_a_whole_table() {
        let conn = open_views_db("six-way-join-key-lookup.db");
        for table in 1..=6 {
            conn.execute(format!(
                "CREATE TABLE t{table}(id INTEGER PRIMARY KEY, p INT, v INT)"
            ))
            .unwrap();
            conn.execute(format!("CREATE INDEX t{table}_p ON t{table}(p)"))
                .unwrap();
        }
        conn.execute(
            "CREATE MATERIALIZED VIEW v AS SELECT t1.v, t6.v FROM t1 \
             JOIN t2 ON t2.p = t1.id JOIN t3 ON t3.p = t2.id JOIN t4 ON t4.p = t3.id \
             JOIN t5 ON t5.p = t4.id JOIN t6 ON t6.p = t5.id",
        )
        .unwrap();

        let maintenance = compile_view_program(&conn, "v", MaintenanceInput::TransactionDelta);
        assert_eq!(stored_table_full_reads(&maintenance), Vec::<String>::new());
        for table in 1..=6 {
            let name = format!("t{table}");
            assert!(rowid_lookups_of(&maintenance, &name) > 0, "{name}");
        }

        let population = compile_view_program(&conn, "v", MaintenanceInput::BaseTable);
        let read_whole = (1..=6)
            .filter(|table| full_reads_of(&population, &format!("t{table}")) > 0)
            .count();
        assert_eq!(read_whole, 1);
    }

    /// A chain of six tables with no index on any join key: the view
    /// creates its own indexes, a partial one where the first table's WHERE
    /// terms apply, reuses an index the user already has, and maintenance
    /// reads no stored table or index in full.
    #[test]
    fn unindexed_six_way_join_creates_its_own_key_indexes() {
        let conn = open_views_db("six-way-join-owned-indexes.db");
        for table in 1..=6 {
            conn.execute(format!(
                "CREATE TABLE t{table}(id INTEGER PRIMARY KEY, p INT, v INT)"
            ))
            .unwrap();
        }
        conn.execute("CREATE INDEX t3_p ON t3(p)").unwrap();
        conn.execute(
            "CREATE MATERIALIZED VIEW v AS SELECT t1.v, t6.v FROM t1 \
             JOIN t2 ON t1.p = t2.id JOIN t3 ON t2.p = t3.id JOIN t4 ON t3.p = t4.id \
             JOIN t5 ON t4.p = t5.id JOIN t6 ON t5.p = t6.id WHERE t1.v > 0",
        )
        .unwrap();

        let schema = conn.schema.read().clone();
        let mut owned: Vec<(String, Option<String>)> = ["t1", "t2", "t3", "t4", "t5", "t6"]
            .into_iter()
            .flat_map(|table| schema.get_indices(table).cloned().collect::<Vec<_>>())
            .filter(|index| crate::schema::is_materialized_view_join_key_index(&index.name))
            .map(|index| {
                (
                    index.table_name.clone(),
                    index.where_clause.as_ref().map(|expr| expr.to_string()),
                )
            })
            .collect();
        owned.sort();
        assert_eq!(
            owned,
            vec![
                ("t1".to_string(), Some("(v > 0)".to_string())),
                ("t2".to_string(), None),
                ("t4".to_string(), None),
                ("t5".to_string(), None),
            ]
        );

        let maintenance = compile_view_program(&conn, "v", MaintenanceInput::TransactionDelta);
        assert_eq!(stored_table_full_reads(&maintenance), Vec::<String>::new());
    }

    /// Two views that need the same index share it, and dropping one keeps
    /// the index the other still reads its matches through.
    #[test]
    fn views_share_a_join_key_index() {
        let conn = open_views_db("shared-join-key-index.db");
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE child(pid INT, v INT)").unwrap();
        for view in ["v1", "v2"] {
            conn.execute(format!(
                "CREATE MATERIALIZED VIEW {view} AS \
                 SELECT c.v, p.name FROM child c JOIN parent p ON c.pid = p.id"
            ))
            .unwrap();
        }
        let join_key_indexes = |conn: &Arc<Connection>| {
            conn.schema
                .read()
                .get_indices("child")
                .filter(|index| crate::schema::is_materialized_view_join_key_index(&index.name))
                .count()
        };
        assert_eq!(join_key_indexes(&conn), 1);

        conn.execute("DROP VIEW v1").unwrap();
        assert_eq!(join_key_indexes(&conn), 1);
        let maintenance = compile_view_program(&conn, "v2", MaintenanceInput::TransactionDelta);
        assert_eq!(stored_table_full_reads(&maintenance), Vec::<String>::new());

        conn.execute("DROP VIEW v2").unwrap();
        assert_eq!(join_key_indexes(&conn), 0);
    }

    /// The partial index a view creates for a filtered table serves that
    /// table's lookups for every kind of term an index WHERE may hold.
    #[test]
    fn view_reads_through_its_partial_index_for_every_filter_shape() {
        let conn = open_views_db("partial-index-filter-shapes.db");
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, p INT, v INT, s TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT)")
            .unwrap();
        conn.execute(
            "CREATE MATERIALIZED VIEW v AS SELECT t1.id, t2.w FROM t1 JOIN t2 ON t1.p = t2.id \
             WHERE t1.s = 'a' COLLATE NOCASE AND CAST(t1.v AS TEXT) <> '9' \
             AND CASE WHEN t1.v > 2 THEN 1 ELSE 0 END = 1 AND t1.v IN (3, 5) \
             AND t1.v BETWEEN 1 AND 10 AND t1.s LIKE '%!_%' ESCAPE '!' AND abs(t1.v) > 1",
        )
        .unwrap();
        let partial_indexes = conn
            .schema
            .read()
            .get_indices("t1")
            .filter(|index| index.where_clause.is_some())
            .count();
        assert_eq!(partial_indexes, 1);
        let maintenance = compile_view_program(&conn, "v", MaintenanceInput::TransactionDelta);
        assert_eq!(stored_table_full_reads(&maintenance), Vec::<String>::new());
    }

    fn open_views_db(name: &str) -> Arc<Connection> {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            name,
            OpenFlags::default(),
            DatabaseOpts::new().with_views(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        db.connect(ConnectOpts::new()).unwrap()
    }

    fn compile_view_program(
        conn: &Arc<Connection>,
        view_name: &str,
        input: MaintenanceInput,
    ) -> Program {
        let schema = conn.schema.read().clone();
        let view = schema.get_materialized_view(view_name).unwrap();
        let (select, root_page, num_columns) = {
            let view = view.lock();
            (
                view.select_stmt.clone(),
                view.get_root_page(),
                view.column_schema.flat_columns().unwrap().len(),
            )
        };
        compile_maintenance_program(
            view_name,
            &select,
            root_page,
            num_columns,
            input,
            &schema,
            conn,
        )
        .unwrap()
    }

    fn full_reads_of(program: &Program, table_name: &str) -> usize {
        program
            .insns
            .iter()
            .filter(|(insn, _)| {
                matches!(insn, Insn::Rewind { cursor_id, .. } if reads_table(program, *cursor_id, table_name))
            })
            .count()
    }

    /// The stored tables and indexes (base tables, their indexes and hidden
    /// view state, not ephemeral tables) that a program reads from start to
    /// end.
    fn stored_table_full_reads(program: &Program) -> Vec<String> {
        program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::Rewind { cursor_id, .. } => match &program.cursor_ref[*cursor_id].1 {
                    CursorType::BTreeTable(table) if table.root_page != 0 => {
                        Some(table.name.clone())
                    }
                    CursorType::BTreeIndex(index) if !index.ephemeral => Some(index.name.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    fn rowid_lookups_of(program: &Program, table_name: &str) -> usize {
        program
            .insns
            .iter()
            .filter(|(insn, _)| match insn {
                Insn::SeekRowid { cursor_id, .. }
                | Insn::SeekGE {
                    cursor_id,
                    is_index: false,
                    ..
                } => reads_table(program, *cursor_id, table_name),
                _ => false,
            })
            .count()
    }

    fn reads_table(program: &Program, cursor_id: usize, table_name: &str) -> bool {
        matches!(
            &program.cursor_ref[cursor_id].1,
            CursorType::BTreeTable(table) if table.name == table_name
        )
    }
}
