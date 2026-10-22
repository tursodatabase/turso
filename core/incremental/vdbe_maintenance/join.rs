use super::binding::{cache_stream_expr_reg, make_joined_table, remap_bound_expr, BindingRemap};
use super::output::{DeltaSource, EmittedNodeOutput, NodeOutput, NodeOutputContract};
use super::plan::OperatorStateDef;
use super::stream::{
    emit_ephemeral_delta_row, open_ephemeral_delta, scratch_table, ArrangementHandle,
    ArrangementIdentityColumn, DeltaIdentity,
};
use super::MaintenanceInput;
use crate::incremental::dag;
use crate::schema::Schema;
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{translate_condition_expr, ConditionMetadata};
use crate::translate::plan::TableReferences;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::{Connection, LimboError, Result};
use turso_parser::ast;

const JOIN_ARITY: usize = 2;

/// One physical input edge of a binary join.
///
/// Keeping the delta, logical schema, and maintained arrangement together
/// prevents emitters from accidentally combining parallel slices belonging
/// to different inputs.
struct JoinInputContract {
    delta: DeltaSource,
    schema: Arc<dag::StreamSchema>,
    arrangement: ArrangementHandle,
}

/// The complete physical contract of one binary join node.
pub(super) struct JoinContract {
    inputs: [JoinInputContract; JOIN_ARITY],
    on: Vec<ast::Expr>,
}

impl JoinContract {
    pub(super) fn from_outputs(
        outputs: [&NodeOutput; JOIN_ARITY],
        on: &[ast::Expr],
    ) -> Result<Self> {
        let input = |position: usize| -> Result<JoinInputContract> {
            let output = outputs[position];
            let schema = output.contract().schema.clone();
            let arrangement = output.arrangement().cloned().ok_or_else(|| {
                LimboError::InternalError("join input has no materialized arrangement".to_string())
            })?;
            Ok(JoinInputContract {
                delta: output.delta().clone(),
                schema,
                arrangement,
            })
        };
        Ok(Self {
            inputs: [input(0)?, input(1)?],
            on: on.to_vec(),
        })
    }
}

/// Which relation a join phase reads for one side of the join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    /// The transaction's captured delta for the table.
    Delta,
    /// The table's btree, which at maintenance time holds the post-change
    /// state.
    Btree,
}

/// The bilinear delta decomposition for one binary join:
/// `d(L ⋈ R) = dL ⋈ R_new + L_new ⋈ dR − dL ⋈ dR`.
///
/// Population is one all-arrangement phase with weight +1. Unlike the deleted
/// n-ary subset expansion, this has constant code size per join node.
fn binary_join_phases(input: MaintenanceInput) -> &'static [([JoinSide; JOIN_ARITY], bool)] {
    const POPULATION: &[([JoinSide; JOIN_ARITY], bool)] =
        &[([JoinSide::Btree, JoinSide::Btree], false)];
    const MAINTENANCE: &[([JoinSide; JOIN_ARITY], bool)] = &[
        ([JoinSide::Delta, JoinSide::Btree], false),
        ([JoinSide::Btree, JoinSide::Delta], false),
        ([JoinSide::Delta, JoinSide::Delta], true),
    ];
    match input {
        MaintenanceInput::BaseTable => POPULATION,
        MaintenanceInput::TransactionDelta => MAINTENANCE,
    }
}

/// Per-match emission hook for [`emit_join_phase`]: receives the program,
/// and the per-input physical cursor ids.
type JoinPhaseSink<'a> = dyn FnMut(&mut ProgramBuilder, &[usize; JOIN_ARITY]) -> Result<()> + 'a;

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

fn emit_join_input_binding_rowid(
    program: &mut ProgramBuilder,
    side: JoinSide,
    delta: &DeltaSource,
    arrangement: &ArrangementHandle,
    cursor_id: usize,
    binding: usize,
    dest: usize,
) -> bool {
    match (side, delta) {
        (JoinSide::Delta, DeltaSource::BaseTable { table, .. }) => {
            turso_assert!(
                binding == 0,
                "a base-table delta exposes exactly one logical binding"
            );
            if !table.has_rowid {
                return false;
            }
            program.emit_insn(Insn::RowId { cursor_id, dest });
            true
        }
        (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
            let Some(column) = channel
                .binding_rowid_columns
                .get(binding)
                .copied()
                .flatten()
            else {
                return false;
            };
            program.emit_insn(Insn::Column {
                cursor_id,
                column,
                dest,
                default: None,
            });
            true
        }
        (JoinSide::Btree, _) => {
            let Some(column) = arrangement
                .binding_rowid_columns()
                .get(binding)
                .copied()
                .flatten()
            else {
                return false;
            };
            emit_arrangement_column_value(program, column, cursor_id, dest);
            true
        }
    }
}

fn emit_join_input_identity(
    program: &mut ProgramBuilder,
    side: JoinSide,
    delta: &DeltaSource,
    arrangement: &ArrangementHandle,
    cursor_id: usize,
    dest: usize,
) -> usize {
    let identity = match side {
        JoinSide::Delta => delta.identity(),
        JoinSide::Btree => arrangement.identity(),
    };
    match (side, delta) {
        (JoinSide::Delta, DeltaSource::BaseTable { .. }) => {
            turso_assert!(
                identity == DeltaIdentity::BindingRowids(1),
                "base-table delta identity must be its rowid"
            );
            program.emit_insn(Insn::RowId { cursor_id, dest });
        }
        (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
            for column in 0..identity.width() {
                program.emit_insn(Insn::Column {
                    cursor_id: channel.cursor_id,
                    column,
                    dest: dest + column,
                    default: None,
                });
            }
        }
        (JoinSide::Btree, _) => {
            for column in 0..identity.width() {
                emit_arrangement_identity_value(
                    program,
                    arrangement,
                    cursor_id,
                    column,
                    dest + column,
                );
            }
        }
    }
    identity.width()
}

/// Emit one join delta phase: nested loops over every joined table — reading
/// the transaction delta for tables in the phase's subset and the post-state
/// btree for the rest — with the delta tables outermost so the probe scans
/// run on the btree side. At the innermost level the ON predicates and WHERE
/// are evaluated, the signed weight product lands in `w_reg`, and `sink`
/// emits the per-match work with all cursors positioned (its slice maps
/// table positions to cursor ids).
#[allow(clippy::too_many_arguments)]
fn emit_join_phase(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    view_name: &str,
    contract: &JoinContract,
    sides: &[JoinSide; JOIN_ARITY],
    negate: bool,
    w_reg: usize,
    sink: &mut JoinPhaseSink<'_>,
) -> Result<()> {
    // A physical input may carry several SQL bindings after a previous binary
    // join. Bound expressions are retargeted to fresh phase ids and every
    // column is seeded from the one physical cursor declared by that edge.
    let mut binding_remap = BindingRemap::default();
    let mut phase_bindings = Vec::new();
    for input in &contract.inputs {
        for binding in &input.schema.bindings {
            let phase_id = program.table_reference_counter.next();
            let previous = binding_remap.insert(binding.logical_id, phase_id);
            turso_assert!(
                previous.is_none(),
                "a join phase must expose each logical binding exactly once"
            );
            phase_bindings.push((binding, phase_id));
        }
    }
    let table_references = TableReferences::new(
        phase_bindings
            .iter()
            .map(|(binding, phase_id)| {
                make_joined_table(&binding.table, &binding.identifier, *phase_id)
            })
            .collect(),
        vec![],
    );

    let mut open_input = |position: usize| -> Result<usize> {
        let input = &contract.inputs[position];
        let storage = input.arrangement.table();
        match sides[position] {
            JoinSide::Delta => match &input.delta {
                DeltaSource::BaseTable {
                    table: delta_table, ..
                } => {
                    turso_assert!(
                        input.schema.len() == delta_table.columns().len()
                            && (Arc::ptr_eq(storage, delta_table)
                                || storage.root_page == delta_table.root_page),
                        "join delta and arrangement must describe the same relation"
                    );
                    let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                        view_name: view_name.to_string(),
                        table: delta_table.clone(),
                    });
                    program.emit_insn(Insn::OpenRead {
                        cursor_id,
                        root_page: 0,
                        db: 0,
                    });
                    Ok(cursor_id)
                }
                DeltaSource::Ephemeral(channel) => Ok(channel.cursor_id),
            },
            JoinSide::Btree => {
                let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(storage.clone()));
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: storage.root_page,
                    db: 0,
                });
                Ok(cursor_id)
            }
        }
    };
    let cursor_ids = [open_input(0)?, open_input(1)?];

    // Nest delta tables outermost.
    let mut order = [0, 1];
    order.sort_by_key(|&i| sides[i] == JoinSide::Btree);

    let phase_done_label = program.allocate_label();
    let loop_labels = std::array::from_fn::<_, JOIN_ARITY, _>(|_| program.allocate_label());
    let next_labels = std::array::from_fn::<_, JOIN_ARITY, _>(|_| program.allocate_label());
    let delta_w_regs = std::array::from_fn::<_, JOIN_ARITY, _>(|position| {
        (sides[position] == JoinSide::Delta).then(|| program.alloc_register())
    });
    let arrangement_count_regs = std::array::from_fn::<_, JOIN_ARITY, _>(|position| {
        (sides[position] == JoinSide::Btree
            && contract.inputs[position]
                .arrangement
                .count_column()
                .is_some())
        .then(|| program.alloc_register())
    });

    for (depth, &pos) in order.iter().enumerate() {
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor_ids[pos],
            pc_if_empty: if depth == 0 {
                phase_done_label
            } else {
                next_labels[depth - 1]
            },
        });
        program.preassign_label_to_next_insn(loop_labels[depth]);
        if let Some(w) = delta_w_regs[pos] {
            let weight_column = match &contract.inputs[pos].delta {
                DeltaSource::BaseTable { .. } => contract.inputs[pos].schema.len(),
                DeltaSource::Ephemeral(channel) => channel.weight_column,
            };
            program.emit_insn(Insn::Column {
                cursor_id: cursor_ids[pos],
                column: weight_column,
                dest: w,
                default: None,
            });
        }
        if let (Some(count_reg), Some(count_column)) = (
            arrangement_count_regs[pos],
            contract.inputs[pos].arrangement.count_column(),
        ) {
            program.emit_insn(Insn::Column {
                cursor_id: cursor_ids[pos],
                column: count_column,
                dest: count_reg,
                default: None,
            });
        }
    }

    // Bind every logical column to the physical arrangement/delta slot used
    // in this phase. This keeps expression translation independent of record
    // layout (aggregate arrangements can project away accumulator payloads,
    // while ephemeral deltas carry identity before their values).
    resolver.enable_expr_to_reg_cache();
    for pos in 0..JOIN_ARITY {
        let input = &contract.inputs[pos];
        for (logical_column, column) in input.schema.columns.iter().enumerate() {
            let Some(expr) = column else {
                continue;
            };
            let physical_column = match (&sides[pos], &input.delta) {
                (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                    channel.value_start + logical_column
                }
                (JoinSide::Delta, DeltaSource::BaseTable { .. }) => logical_column,
                (JoinSide::Btree, _) => input.arrangement.value_columns()[logical_column],
            };
            let value_reg = program.alloc_register();
            match (&sides[pos], &input.delta) {
                (JoinSide::Delta, DeltaSource::Ephemeral(_)) => {
                    program.emit_insn(Insn::Column {
                        cursor_id: cursor_ids[pos],
                        column: physical_column,
                        dest: value_reg,
                        default: None,
                    });
                }
                _ => program.emit_column_or_rowid(cursor_ids[pos], physical_column, value_reg),
            }
            let bound = remap_bound_expr(expr, &binding_remap)?;
            cache_stream_expr_reg(&mut *resolver, bound, value_reg, &table_references, false)?;
        }
        for (binding_index, binding) in input.schema.bindings.iter().enumerate() {
            let rowid_reg = program.alloc_register();
            if !emit_join_input_binding_rowid(
                program,
                sides[pos],
                &input.delta,
                &input.arrangement,
                cursor_ids[pos],
                binding_index,
                rowid_reg,
            ) {
                continue;
            }
            let phase_id = *binding_remap.get(&binding.logical_id).ok_or_else(|| {
                LimboError::InternalError(
                    "join binding rowid has no phase-local binding".to_string(),
                )
            })?;
            resolver.cache_expr_reg(
                std::borrow::Cow::Owned(ast::Expr::RowId {
                    database: None,
                    table: phase_id,
                }),
                rowid_reg,
                false,
                None,
            );
        }
    }

    // ON predicates and WHERE: a FALSE or NULL result means this combination
    // is not a join output.
    let innermost_next = next_labels[JOIN_ARITY - 1];
    for condition in &contract.on {
        let bound = remap_bound_expr(condition, &binding_remap)?;
        let pred_true = program.allocate_label();
        translate_condition_expr(
            program,
            &table_references,
            &bound,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: pred_true,
                jump_target_when_false: innermost_next,
                jump_target_when_null: innermost_next,
            },
            resolver,
        )?;
        program.preassign_label_to_next_insn(pred_true);
    }

    // Signed weight: the product of the delta weights times the phase sign.
    program.emit_int(if negate { -1 } else { 1 }, w_reg);
    for w in delta_w_regs.iter().flatten() {
        program.emit_insn(Insn::Multiply {
            lhs: w_reg,
            rhs: *w,
            dest: w_reg,
        });
    }
    for count in arrangement_count_regs.iter().flatten() {
        program.emit_insn(Insn::Multiply {
            lhs: w_reg,
            rhs: *count,
            dest: w_reg,
        });
    }

    sink(program, &cursor_ids)?;

    for depth in (0..JOIN_ARITY).rev() {
        program.preassign_label_to_next_insn(next_labels[depth]);
        program.emit_insn(Insn::Next {
            cursor_id: cursor_ids[order[depth]],
            pc_if_next: loop_labels[depth],
            fullscan: false,
            is_index: false,
        });
    }
    program.preassign_label_to_next_insn(phase_done_label);
    Ok(())
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
    let eph_cursor_id = channel.cursor_id;

    // [transport identity..., binding rowids..., natural join row..., weight].
    let eph_rec_start = program.alloc_registers(channel.record_width());
    let eph_value_start = eph_rec_start + channel.value_start;
    let eph_weight_reg = eph_rec_start + channel.weight_column;
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

    let emit_eph_insert = |program: &mut ProgramBuilder| {
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(eph_rec_start),
            count: to_u32(channel.record_width()),
            dest_reg: to_u32(eph_record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: eph_cursor_id,
            rowid_reg: eph_rowid_reg,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Insert {
            cursor: eph_cursor_id,
            key_reg: eph_rowid_reg,
            record_reg: eph_record_reg,
            flag: InsertFlags::new().is_ephemeral_table_insert(),
            table_name: String::new(),
        });
    };

    for &(sides, negate) in binary_join_phases(input) {
        emit_join_phase(
            program,
            &mut resolver,
            view_name,
            contract,
            &sides,
            negate,
            eph_weight_reg,
            &mut |program, cursors| {
                match channel.identity {
                    DeltaIdentity::BindingRowids(expected_width) => {
                        let mut destination = eph_rec_start;
                        for (position, input) in contract.inputs.iter().enumerate() {
                            let source_identity = match sides[position] {
                                JoinSide::Delta => input.delta.identity(),
                                JoinSide::Btree => input.arrangement.identity(),
                            };
                            turso_assert!(
                                matches!(source_identity, DeltaIdentity::BindingRowids(_)),
                                "binding-rowid join output requires binding-rowid inputs"
                            );
                            destination += emit_join_input_identity(
                                program,
                                sides[position],
                                &input.delta,
                                &input.arrangement,
                                cursors[position],
                                destination,
                            );
                        }
                        turso_assert!(
                            destination == eph_rec_start + expected_width,
                            "join binding-rowid output width must equal its input identities"
                        );
                    }
                    DeltaIdentity::OperatorKey(2) => {
                        for (position, input) in contract.inputs.iter().enumerate() {
                            let source_identity = match sides[position] {
                                JoinSide::Delta => input.delta.identity(),
                                JoinSide::Btree => input.arrangement.identity(),
                            };
                            let source_start = program.alloc_registers(source_identity.width());
                            let width = emit_join_input_identity(
                                program,
                                sides[position],
                                &input.delta,
                                &input.arrangement,
                                cursors[position],
                                source_start,
                            );
                            program.emit_insn(Insn::MakeRecord {
                                start_reg: to_u32(source_start),
                                count: to_u32(width),
                                dest_reg: to_u32(eph_rec_start + position),
                                index_name: None,
                                affinity_str: None,
                            });
                        }
                    }
                    DeltaIdentity::OperatorRowid | DeltaIdentity::OperatorKey(_) => {
                        return Err(LimboError::InternalError(
                            "binary inner join has an invalid planned identity".to_string(),
                        ));
                    }
                }
                let mut output_binding = 0;
                for (position, input) in contract.inputs.iter().enumerate() {
                    for input_binding in 0..input.schema.bindings.len() {
                        if let Some(output_column) = channel.binding_rowid_columns[output_binding] {
                            if output_column < channel.identity_width() {
                                turso_assert!(
                                    channel.identity
                                        == DeltaIdentity::BindingRowids(
                                            channel.schema.bindings.len()
                                        )
                                        && output_column == output_binding,
                                    "binding-rowid transport slots must follow binding order"
                                );
                            } else {
                                turso_assert!(
                                    emit_join_input_binding_rowid(
                                        program,
                                        sides[position],
                                        &input.delta,
                                        &input.arrangement,
                                        cursors[position],
                                        input_binding,
                                        eph_rec_start + output_column,
                                    ),
                                    "planned join rowid provenance is unavailable from its input"
                                );
                            }
                        }
                        output_binding += 1;
                    }
                }
                turso_assert!(
                    output_binding == channel.schema.bindings.len(),
                    "join input bindings must match its output provenance contract"
                );
                let mut destination = eph_value_start;
                for (position, (input, cursor_id)) in
                    contract.inputs.iter().zip(cursors).enumerate()
                {
                    for column in 0..input.schema.len() {
                        match (&sides[position], &input.delta) {
                            (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                                program.emit_insn(Insn::Column {
                                    cursor_id: *cursor_id,
                                    column: channel.value_start + column,
                                    dest: destination,
                                    default: None,
                                });
                            }
                            (JoinSide::Btree, _) => program.emit_column_or_rowid(
                                *cursor_id,
                                input.arrangement.value_columns()[column],
                                destination,
                            ),
                            (JoinSide::Delta, DeltaSource::BaseTable { .. }) => {
                                program.emit_column_or_rowid(*cursor_id, column, destination);
                            }
                        }
                        destination += 1;
                    }
                }
                turso_assert!(
                    destination == eph_value_start + width,
                    "join stream row does not match its declared schema"
                );
                emit_eph_insert(program);
                Ok(())
            },
        )?;
    }

    drop(syms);
    EmittedNodeOutput::from_ephemeral(node_id, channel, output_contract)
}

/// Emit a semi or anti join: the rows of the first input that some (semi) or
/// no (anti) row of the second input matches under the ON predicates.
///
/// For each row of the first input, a hidden table stored under the row's
/// rowid keeps:
///
/// - `present`: the row's multiplicity, the integral of the first input's
///   delta;
/// - `matches`: its number of ON matches, the integral of the bilinear join
///   delta `dA ⋈ B + A ⋈ dB − dA ⋈ dB`;
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
        MaintenanceInput::TransactionDelta => match &rows.delta {
            DeltaSource::BaseTable { table, .. } => {
                let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                    view_name: view_name.to_string(),
                    table: table.clone(),
                });
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: 0,
                    db: 0,
                });
                (JoinSide::Delta, cursor_id)
            }
            DeltaSource::Ephemeral(channel) => (JoinSide::Delta, channel.cursor_id),
        },
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
    match (presence_side, &rows.delta, rows.arrangement.count_column()) {
        (JoinSide::Delta, DeltaSource::BaseTable { .. }, _) => program.emit_insn(Insn::Column {
            cursor_id: presence_cursor,
            column: rows.schema.len(),
            dest: present_change_reg,
            default: None,
        }),
        (JoinSide::Delta, DeltaSource::Ephemeral(channel), _) => program.emit_insn(Insn::Column {
            cursor_id: presence_cursor,
            column: channel.weight_column,
            dest: present_change_reg,
            default: None,
        }),
        (JoinSide::Btree, _, Some(count_column)) => program.emit_insn(Insn::Column {
            cursor_id: presence_cursor,
            column: count_column,
            dest: present_change_reg,
            default: None,
        }),
        (JoinSide::Btree, _, None) => program.emit_int(1, present_change_reg),
    }
    emit_join_input_identity(
        program,
        presence_side,
        &rows.delta,
        &rows.arrangement,
        presence_cursor,
        row_reg,
    );
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
    let match_weight_reg = program.alloc_register();
    for &(sides, negate) in binary_join_phases(input) {
        emit_join_phase(
            program,
            &mut resolver,
            view_name,
            contract,
            &sides,
            negate,
            match_weight_reg,
            &mut |program, cursors| {
                emit_join_input_identity(
                    program,
                    sides[0],
                    &rows.delta,
                    &rows.arrangement,
                    cursors[0],
                    row_reg,
                );
                program.emit_int(0, present_change_reg);
                program.emit_insn(Insn::Copy {
                    src_reg: match_weight_reg,
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
    }

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
                    &rows.delta,
                    &rows.arrangement,
                    rows_cursor,
                    binding,
                    *reg,
                ),
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
