use super::binding::{remap_bound_expr, seed_ephemeral_stream_cache, stream_table_references};
use super::consolidate::{consolidate_delta, ConsolidatedDelta};
use super::members::{
    emit_add_to_member, emit_exact_value_record, emit_first_member, emit_member_columns,
    emit_member_key, member_columns_width, MemberTable,
};
use super::output::{DeltaSource, EmittedNodeOutput, NodeOutputContract};
use super::plan::{tracks_distinct_values, uses_multiset, OperatorStateDef};
use super::stream::{
    btree_arrangement, emit_operator_rowid_delta, open_ephemeral_delta, synthesized_view_table,
    ArrangementIdentityColumn, DeltaIdentity, EphemeralDelta,
};
use super::MaintenanceInput;
use crate::function::AggFunc;
use crate::incremental::dag;
use crate::namespace::NamespaceId;
use crate::schema::Schema;
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    translate_condition_expr, translate_expr_no_constant_opt, ConditionMetadata,
    NoConstantOptReason,
};
use crate::translate::plan::Aggregate;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, IvmSumData, IvmSumFinalizeData,
    IvmSumKind, RegisterOrLiteral,
};
use crate::{Connection, LimboError, Result};
use turso_parser::ast;

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_group_aggregate(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    channel: &EphemeralDelta,
    group_exprs: &[ast::Expr],
    group_keys: &[dag::KeyColumn],
    aggregates: &[Aggregate],
    multiset_keys: &[Option<dag::KeyColumn>],
    scalar: bool,
    input: MaintenanceInput,
    output_contract: &NodeOutputContract,
    operator_state: &OperatorStateDef,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<EmittedNodeOutput> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_aggregate_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    let channel = consolidate_delta(
        program,
        &format!("{view_name}_aggregate_input_{node_id}"),
        channel,
    )?;
    emit_group_aggregate_rows(
        program,
        view_name,
        &channel,
        group_exprs,
        group_keys,
        aggregates,
        multiset_keys,
        scalar,
        &output,
        input,
        operator_state,
        schema,
        connection,
    )?;

    let state_table_name = operator_state.state_table_name()?;
    let state_table = schema
        .get_btree_table(NamespaceId::ROOT, state_table_name)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "aggregate arrangement table {state_table_name} not found"
            ))
        })?;
    let value_columns = (0..group_exprs.len() + aggregates.len()).collect();
    EmittedNodeOutput::new(
        node_id,
        DeltaSource::Ephemeral(output),
        Some(btree_arrangement(
            state_table,
            DeltaIdentity::OperatorRowid,
            vec![ArrangementIdentityColumn::RowId],
            vec![None; output_contract.schema.bindings.len()],
            value_columns,
            None,
        )),
        output_contract,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_group_aggregate_rows(
    program: &mut ProgramBuilder,
    view_name: &str,
    channel: &ConsolidatedDelta,
    group_exprs: &[ast::Expr],
    group_keys: &[dag::KeyColumn],
    aggregates: &[Aggregate],
    multiset_keys: &[Option<dag::KeyColumn>],
    scalar: bool,
    output: &EphemeralDelta,
    input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let num_view_columns = output.width;
    turso_assert!(
        num_view_columns == group_exprs.len() + aggregates.len()
            && group_keys.len() == group_exprs.len()
            && multiset_keys.len() == aggregates.len(),
        "aggregate output stream must use the node's natural schema"
    );
    let k = group_exprs.len();
    // A group whose class can hold several exact values keeps them all and
    // shows the smallest (see `members`).
    let ambiguous_group = group_keys.iter().any(|key| key.ambiguous);
    // COUNT and SUM/TOTAL/AVG persist fixed-width state in the group state
    // table; MIN/MAX keep a value multiset in the multiset table instead
    // (payload_widths[i] is None for them). DISTINCT aggregates have both:
    // the payload plus multiset rows deciding which values reach it.
    let payload_widths: Vec<Option<usize>> = aggregates
        .iter()
        .map(|agg| ivm_payload_width(&agg.func))
        .collect();
    let total_payload: usize = payload_widths.iter().flatten().sum();
    // Offset of each payload aggregate's slots within the payload block.
    let payload_offsets: Vec<Option<usize>> = payload_widths
        .iter()
        .scan(0usize, |off, w| {
            Some(w.map(|w| {
                let this = *off;
                *off += w;
                this
            }))
        })
        .collect();
    // The state table and its primary-key index are real schema objects
    // created by CREATE MATERIALIZED VIEW and assigned to this DAG node.
    let state_table_name = operator_state.state_table_name()?.to_string();
    let state_table = schema
        .get_btree_table(NamespaceId::ROOT, &state_table_name)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "state table {state_table_name} of materialized view {view_name} not found"
            ))
        })?;
    let state_index = schema
        .get_indices(NamespaceId::ROOT, &state_table_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "state table {state_table_name} of materialized view {view_name} has no index"
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
        None,
    );

    turso_assert!(
        channel.weight_column == channel.value_start + channel.width
            && channel.schema.len() == channel.width,
        "aggregate delta channel layout must match its declared input"
    );
    let input_cursor_id = channel.cursor_id;
    let (table_references, binding_remap) = stream_table_references(program, &channel.schema);

    let state_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(state_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_cursor_id,
        root_page: RegisterOrLiteral::Literal(state_table.root_page),
        db: 0,
    });
    let state_index_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeIndex(state_index.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_index_cursor_id,
        root_page: RegisterOrLiteral::Literal(state_index.root_page),
        db: 0,
    });
    let mut multisets = Vec::with_capacity(aggregates.len());
    for (aggregate_index, aggregate) in aggregates.iter().enumerate() {
        multisets.push(if uses_multiset(aggregate) {
            Some(MemberTable::open(
                program,
                schema,
                view_name,
                operator_state.aggregate_multiset_table_name(aggregate_index)?,
            )?)
        } else {
            None
        });
    }
    let group_members = if ambiguous_group {
        Some(MemberTable::open(
            program,
            schema,
            view_name,
            operator_state.aggregate_group_members_table_name()?,
        )?)
    } else {
        None
    };

    // Aggregate payloads are updated once per input delta, but finalized
    // values are statement-level results. Track each affected state rowid so
    // the second phase can publish exactly one old/new pair per group after
    // every contribution has been applied.
    let touched_groups_table = Arc::new(synthesized_view_table(
        &format!("{state_table_name}_touched"),
        0,
        0,
    )?);
    let touched_groups_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(touched_groups_table));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: touched_groups_cursor_id,
        is_table: true,
    });

    // Register layout.
    let weight_reg = program.alloc_register();
    let zero_reg = program.alloc_register();
    // Weights a DISTINCT aggregate applies to a value entering (+1) or
    // leaving (-1) its set of distinct values.
    let plus_one_reg = program.alloc_register();
    let minus_one_reg = program.alloc_register();
    let found_reg = program.alloc_register();
    let cnt_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    let arg_reg = program.alloc_register();
    // State record image: [g.., finalized aggregate values, payloads..].
    // Keeping the natural output first makes the state table directly
    // scannable as the aggregate node's arrangement.
    let state_rec_start = program.alloc_registers(k + total_payload + aggregates.len());
    let group_start = state_rec_start;
    // The current input row's group values. `group_start` holds the values
    // the group shows, which may be another member of its class.
    let incoming_group_start = program.alloc_registers(k);
    let persisted_value_start = state_rec_start + k;
    let payload_start = persisted_value_start + aggregates.len();
    // Index key image: [g.., state_rowid], contiguous for IdxInsert/IdxDelete.
    // The state rowid doubles as the group's view-btree rowid (see
    // aggregate_state_table_sql).
    let index_rec_start = program.alloc_registers(k + 1);
    let state_rowid_reg = index_rec_start + k;
    // Finalized value of each aggregate for the current group.
    let agg_value_start = program.alloc_registers(aggregates.len());
    // Natural aggregate delta: [groups.., aggregate values.., weight].
    let out_start = program.alloc_registers(num_view_columns + 1);
    // Natural aggregate row used when publishing an old arrangement row:
    // [groups.., aggregate values.., weight].
    let old_out_start = program.alloc_registers(k + aggregates.len() + 1);
    let old_out_weight_reg = old_out_start + k + aggregates.len();
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    // A member's count before and after a change.
    let old_count_reg = program.alloc_register();
    let new_count_reg = program.alloc_register();
    // Group member key: [class.., member columns..].
    let group_member_key_start = program.alloc_registers(k + member_columns_width(k));
    // The member a group shows, and its exact record, while finalizing.
    let shown_group_start = program.alloc_registers(k);
    let shown_group_exact_reg = program.alloc_register();
    let stored_group_exact_reg = program.alloc_register();
    // Multiset key: [g.., val] for a value with one exact member per class,
    // [g.., val, member columns..] for an ambiguous one. Also the seek
    // prefix.
    let value_member_width = 1 + member_columns_width(1);
    let value_key_start = program.alloc_registers(k + value_member_width);
    // For a DISTINCT aggregate over an ambiguous value: the class's shown
    // member [val, member columns..] before and after the change, and whether
    // the class had a member. The member's exact record is its last column.
    let shown_before_start = program.alloc_registers(value_member_width);
    let shown_after_start = program.alloc_registers(value_member_width);
    let shown_exact_offset = value_member_width - 1;
    let has_before_reg = program.alloc_register();
    let has_after_reg = program.alloc_register();
    let touched_group_record_reg = program.alloc_register();

    program.emit_int(0, zero_reg);
    program.emit_int(1, plus_one_reg);
    program.emit_int(-1, minus_one_reg);
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(zero_reg),
        count: 1,
        dest_reg: to_u32(touched_group_record_reg),
        index_name: None,
        affinity_str: None,
    });

    let finalize_label = program.allocate_label();
    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let corrupt_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: input_cursor_id,
        pc_if_empty: finalize_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    // Weight of the current input row.
    program.emit_insn(Insn::Column {
        cursor_id: input_cursor_id,
        column: channel.weight_column,
        dest: weight_reg,
        default: None,
    });
    seed_ephemeral_stream_cache(
        program,
        channel,
        &binding_remap,
        &table_references,
        &mut resolver,
    )?;

    for (i, group_expr) in group_exprs.iter().enumerate() {
        let bound = remap_bound_expr(group_expr, &binding_remap)?;
        translate_expr_no_constant_opt(
            program,
            Some(&table_references),
            &bound,
            incoming_group_start + i,
            &resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }
    program.emit_insn(Insn::Copy {
        src_reg: incoming_group_start,
        dst_reg: group_start,
        extra_amount: k.saturating_sub(1),
    });

    // Group lookup via the state table's primary-key index.
    let found_label = program.allocate_label();
    let apply_label = program.allocate_label();
    program.emit_insn(Insn::Found {
        cursor_id: state_index_cursor_id,
        target_pc: found_label,
        record_reg: group_start,
        num_regs: k,
    });
    // Not found: the input is consolidated, so it only retracts rows that
    // exist, and a group with rows exists.
    program.emit_insn(Insn::Lt {
        lhs: weight_reg,
        rhs: zero_reg,
        target_pc: corrupt_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_int(0, found_reg);
    program.emit_insn(Insn::Null {
        dest: persisted_value_start,
        dest_end: Some(persisted_value_start + aggregates.len() - 1),
    });
    // A fresh group's payloads start at "no rows seen yet".
    for (i, agg) in aggregates.iter().enumerate() {
        if let Some(offset) = payload_offsets[i] {
            emit_payload_zero_init(program, agg, payload_start + offset);
        }
    }
    program.emit_insn(Insn::NewRowid {
        cursor: state_cursor_id,
        rowid_reg: state_rowid_reg,
        prev_largest_reg: prev_rowid_scratch,
    });
    program.emit_insn(Insn::Copy {
        src_reg: group_start,
        dst_reg: index_rec_start,
        extra_amount: k.saturating_sub(1),
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(index_rec_start),
        count: to_u32(k + 1),
        dest_reg: to_u32(index_record_reg),
        index_name: Some(state_index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: state_index_cursor_id,
        record_reg: index_record_reg,
        unpacked_start: Some(index_rec_start),
        unpacked_count: Some(to_u32(k + 1)),
        flags: IdxInsertFlags::new(),
    });
    program.emit_insn(Insn::Goto {
        target_pc: apply_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_int(1, found_reg);
    program.emit_insn(Insn::IdxRowId {
        cursor_id: state_index_cursor_id,
        dest: state_rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: state_cursor_id,
        src_reg: state_rowid_reg,
        target_pc: corrupt_label,
    });
    if ambiguous_group {
        // The group shows one member of its class, not necessarily this one.
        for i in 0..k {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: i,
                dest: group_start + i,
                default: None,
            });
        }
    }
    emit_payload_load(
        program,
        state_cursor_id,
        k + aggregates.len(),
        payload_start,
        total_payload,
    );
    for i in 0..aggregates.len() {
        program.emit_insn(Insn::Column {
            cursor_id: state_cursor_id,
            column: k + i,
            dest: persisted_value_start + i,
            default: None,
        });
    }

    program.preassign_label_to_next_insn(apply_label);
    let first_touch_label = program.allocate_label();
    let touch_done_label = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: touched_groups_cursor_id,
        rowid_reg: state_rowid_reg,
        target_pc: first_touch_label,
    });
    program.emit_insn(Insn::Goto {
        target_pc: touch_done_label,
    });
    program.preassign_label_to_next_insn(first_touch_label);
    program.emit_insn(Insn::Insert {
        cursor: touched_groups_cursor_id,
        key_reg: state_rowid_reg,
        record_reg: touched_group_record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    let retract_old_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: retract_old_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: touch_done_label,
    });
    program.preassign_label_to_next_insn(retract_old_label);
    program.emit_insn(Insn::Copy {
        src_reg: group_start,
        dst_reg: old_out_start,
        extra_amount: k.saturating_sub(1),
    });
    program.emit_insn(Insn::Copy {
        src_reg: persisted_value_start,
        dst_reg: old_out_start + k,
        extra_amount: aggregates.len().saturating_sub(1),
    });
    program.emit_int(-1, old_out_weight_reg);
    emit_operator_rowid_delta(
        program,
        output,
        state_rowid_reg,
        old_out_start,
        old_out_weight_reg,
    );
    program.preassign_label_to_next_insn(touch_done_label);
    if let Some(members) = &group_members {
        emit_member_key(program, incoming_group_start, k, group_member_key_start);
        emit_add_to_member(
            program,
            members,
            group_member_key_start,
            weight_reg,
            old_count_reg,
            new_count_reg,
            corrupt_label,
        );
    }

    for (i, agg) in aggregates.iter().enumerate() {
        let is_minmax = matches!(agg.func, AggFunc::Min | AggFunc::Max);
        let is_distinct = tracks_distinct_values(agg);
        let aggregate_done_label = program.allocate_label();
        if let Some(filter_expr) = &agg.filter_expr {
            let apply_aggregate_label = program.allocate_label();
            let bound_filter = remap_bound_expr(filter_expr, &binding_remap)?;
            translate_condition_expr(
                program,
                &table_references,
                &bound_filter,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true: apply_aggregate_label,
                    jump_target_when_false: aggregate_done_label,
                    jump_target_when_null: aggregate_done_label,
                },
                &resolver,
            )?;
            program.preassign_label_to_next_insn(apply_aggregate_label);
        }
        let col_reg = match agg.args.first() {
            Some(arg_expr) => {
                let bound = remap_bound_expr(arg_expr, &binding_remap)?;
                translate_expr_no_constant_opt(
                    program,
                    Some(&table_references),
                    &bound,
                    arg_reg,
                    &resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                Some(arg_reg)
            }
            None => None,
        };

        if !(is_minmax || is_distinct) {
            // Every remaining aggregate is linear: a row of weight w changes
            // its state by w times the row's contribution, in one step.
            let offset = payload_offsets[i].expect("Count/Sum/Total/Avg have a payload");
            emit_weighted_apply(program, agg, col_reg, weight_reg, payload_start + offset);
            program.preassign_label_to_next_insn(aggregate_done_label);
            continue;
        }

        // Add the weight to the value's count in the aggregate's multiset.
        // NULLs are ignored, matching aggregate NULL semantics.
        let col_reg = col_reg.expect("MIN/MAX and DISTINCT aggregates have an argument");
        let multiset = multisets[i]
            .as_ref()
            .expect("DAG validation guarantees one multiset per multiset aggregate");
        let value_key = multiset_keys[i].expect("a multiset aggregate has a value key");
        program.emit_insn(Insn::IsNull {
            reg: col_reg,
            target_pc: aggregate_done_label,
        });
        program.emit_insn(Insn::Copy {
            src_reg: incoming_group_start,
            dst_reg: value_key_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::Copy {
            src_reg: col_reg,
            dst_reg: value_key_start + k,
            extra_amount: 0,
        });
        if value_key.ambiguous {
            emit_member_columns(program, col_reg, 1, value_key_start + k + 1);
        }
        let ambiguous_distinct = is_distinct && value_key.ambiguous;
        if ambiguous_distinct {
            emit_shown_value(
                program,
                multiset,
                value_key_start,
                k,
                shown_before_start,
                has_before_reg,
            );
        }
        emit_add_to_member(
            program,
            multiset,
            value_key_start,
            weight_reg,
            old_count_reg,
            new_count_reg,
            corrupt_label,
        );
        if !is_distinct {
            program.preassign_label_to_next_insn(aggregate_done_label);
            continue;
        }

        // A DISTINCT aggregate sees one value per class: the value the class
        // shows. Retract the old one and add the new one when it changes.
        let offset = payload_offsets[i].expect("DISTINCT aggregates have a payload");
        if ambiguous_distinct {
            emit_shown_value(
                program,
                multiset,
                value_key_start,
                k,
                shown_after_start,
                has_after_reg,
            );
            let no_before_label = program.allocate_label();
            let retract_label = program.allocate_label();
            program.emit_insn(Insn::IfNot {
                reg: has_before_reg,
                target_pc: no_before_label,
                jump_if_null: false,
            });
            program.emit_insn(Insn::IfNot {
                reg: has_after_reg,
                target_pc: retract_label,
                jump_if_null: false,
            });
            program.emit_insn(Insn::Eq {
                lhs: shown_before_start + shown_exact_offset,
                rhs: shown_after_start + shown_exact_offset,
                target_pc: aggregate_done_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            program.preassign_label_to_next_insn(retract_label);
            emit_weighted_apply(
                program,
                agg,
                Some(shown_before_start),
                minus_one_reg,
                payload_start + offset,
            );
            program.preassign_label_to_next_insn(no_before_label);
            program.emit_insn(Insn::IfNot {
                reg: has_after_reg,
                target_pc: aggregate_done_label,
                jump_if_null: false,
            });
            emit_weighted_apply(
                program,
                agg,
                Some(shown_after_start),
                plus_one_reg,
                payload_start + offset,
            );
        } else {
            // The class has one exact value: it enters when its count leaves
            // zero and leaves when the count returns to zero.
            let not_entering_label = program.allocate_label();
            program.emit_insn(Insn::Ne {
                lhs: old_count_reg,
                rhs: zero_reg,
                target_pc: not_entering_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            emit_weighted_apply(
                program,
                agg,
                Some(col_reg),
                plus_one_reg,
                payload_start + offset,
            );
            program.preassign_label_to_next_insn(not_entering_label);
            program.emit_insn(Insn::Ne {
                lhs: new_count_reg,
                rhs: zero_reg,
                target_pc: aggregate_done_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            emit_weighted_apply(
                program,
                agg,
                Some(col_reg),
                minus_one_reg,
                payload_start + offset,
            );
        }
        program.preassign_label_to_next_insn(aggregate_done_label);
    }

    // Persist only the payloads while the input batch is still being applied.
    // The finalized value columns deliberately remain the values from before
    // the statement, so another contribution to the same group can neither
    // observe nor publish a transient aggregate result.
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(state_rec_start),
        count: to_u32(k + total_payload + aggregates.len()),
        dest_reg: to_u32(state_record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: state_cursor_id,
        key_reg: state_rowid_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: state_table_name.clone(),
    });

    // Finalize a fully applied group and publish its natural row. HAVING is an
    // ordinary downstream Filter node in the maintenance DAG.
    let emit_row_tail = |program: &mut ProgramBuilder| -> Result<()> {
        if let Some(members) = &group_members {
            // Show the group's smallest member. When that changes, the
            // state row and its index entry move to the new member's values.
            let unchanged_label = program.allocate_label();
            emit_first_member(
                program,
                members,
                group_start,
                k,
                0,
                k,
                shown_group_start,
                corrupt_label,
            );
            emit_exact_value_record(program, shown_group_start, k, shown_group_exact_reg);
            emit_exact_value_record(program, group_start, k, stored_group_exact_reg);
            program.emit_insn(Insn::Eq {
                lhs: shown_group_exact_reg,
                rhs: stored_group_exact_reg,
                target_pc: unchanged_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: index_rec_start,
                extra_amount: k - 1,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: index_rec_start,
                num_regs: k + 1,
                cursor_id: state_index_cursor_id,
                raise_error_if_no_matching_entry: true,
            });
            program.emit_insn(Insn::Copy {
                src_reg: shown_group_start,
                dst_reg: group_start,
                extra_amount: k - 1,
            });
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: index_rec_start,
                extra_amount: k - 1,
            });
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(index_rec_start),
                count: to_u32(k + 1),
                dest_reg: to_u32(index_record_reg),
                index_name: Some(state_index.name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: state_index_cursor_id,
                record_reg: index_record_reg,
                unpacked_start: Some(index_rec_start),
                unpacked_count: Some(to_u32(k + 1)),
                flags: IdxInsertFlags::new(),
            });
            program.preassign_label_to_next_insn(unchanged_label);
        }
        for (i, agg) in aggregates.iter().enumerate() {
            let value_reg = agg_value_start + i;
            if matches!(agg.func, AggFunc::Min | AggFunc::Max) {
                // The value collation orders the classes; within the extreme
                // class the result is the member that class shows.
                let multiset = multisets[i]
                    .as_ref()
                    .expect("MIN/MAX aggregates own a multiset table");
                let value_key = multiset_keys[i].expect("MIN/MAX aggregates have a value key");
                let empty_label = program.allocate_label();
                let have_label = program.allocate_label();
                program.emit_insn(Insn::Copy {
                    src_reg: group_start,
                    dst_reg: value_key_start,
                    extra_amount: k.saturating_sub(1),
                });
                if matches!(agg.func, AggFunc::Min) {
                    // The group's first entry is its smallest class's
                    // smallest member.
                    emit_first_member(
                        program,
                        multiset,
                        value_key_start,
                        k,
                        k,
                        1,
                        value_reg,
                        empty_label,
                    );
                } else {
                    program.emit_insn(Insn::SeekLE {
                        is_index: true,
                        cursor_id: multiset.index_cursor_id(),
                        start_reg: value_key_start,
                        num_regs: k,
                        target_pc: empty_label,
                        eq_only: false,
                        null_matching_mask: Default::default(),
                    });
                    // Positioned before the group: no values.
                    program.emit_insn(Insn::IdxLT {
                        cursor_id: multiset.index_cursor_id(),
                        start_reg: value_key_start,
                        num_regs: k,
                        target_pc: empty_label,
                    });
                    program.emit_insn(Insn::Column {
                        cursor_id: multiset.index_cursor_id(),
                        column: k,
                        dest: value_key_start + k,
                        default: None,
                    });
                    if value_key.ambiguous {
                        // The group's last entry is the largest class's
                        // largest member; the class shows its smallest.
                        emit_first_member(
                            program,
                            multiset,
                            value_key_start,
                            k + 1,
                            k,
                            1,
                            value_reg,
                            corrupt_label,
                        );
                    } else {
                        program.emit_insn(Insn::Copy {
                            src_reg: value_key_start + k,
                            dst_reg: value_reg,
                            extra_amount: 0,
                        });
                    }
                }
                program.emit_insn(Insn::Goto {
                    target_pc: have_label,
                });
                program.preassign_label_to_next_insn(empty_label);
                program.emit_insn(Insn::Null {
                    dest: value_reg,
                    dest_end: None,
                });
                program.preassign_label_to_next_insn(have_label);
            } else if let Some(kind) = ivm_sum_kind(&agg.func) {
                let offset = payload_offsets[i].expect("Sum/Total/Avg have a payload");
                program.emit_insn(Insn::IvmSumFinalize {
                    data: Box::new(IvmSumFinalizeData {
                        kind,
                        int_wrapped_reg: payload_start + offset,
                        int_wrap_count_reg: payload_start + offset + 1,
                        float_sum_reg: payload_start + offset + 2,
                        float_err_reg: payload_start + offset + 3,
                        non_int_count_reg: payload_start + offset + 4,
                        non_null_count_reg: payload_start + offset + 5,
                        dest_reg: value_reg,
                    }),
                });
            } else {
                turso_assert!(
                    matches!(agg.func, AggFunc::Count | AggFunc::Count0),
                    "only COUNT keeps its result directly in its payload"
                );
                let offset = payload_offsets[i].expect("COUNT has a payload");
                program.emit_insn(Insn::Copy {
                    src_reg: payload_start + offset,
                    dst_reg: value_reg,
                    extra_amount: 0,
                });
            }
        }

        program.emit_insn(Insn::Copy {
            src_reg: agg_value_start,
            dst_reg: persisted_value_start,
            extra_amount: aggregates.len().saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(state_rec_start),
            count: to_u32(k + total_payload + aggregates.len()),
            dest_reg: to_u32(state_record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: state_cursor_id,
            key_reg: state_rowid_reg,
            record_reg: state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: state_table_name.clone(),
        });

        if k > 0 {
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: out_start,
                extra_amount: k - 1,
            });
        }
        if !aggregates.is_empty() {
            program.emit_insn(Insn::Copy {
                src_reg: agg_value_start,
                dst_reg: out_start + k,
                extra_amount: aggregates.len() - 1,
            });
        }
        program.emit_int(1, out_start + num_view_columns);
        emit_operator_rowid_delta(
            program,
            output,
            state_rowid_reg,
            out_start,
            out_start + num_view_columns,
        );
        Ok(())
    };

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input_cursor_id,
        pc_if_next: loop_label,
        fullscan: false,
        is_index: false,
    });

    // The first phase left every touched state row present, including groups
    // whose final liveness count is zero. Reload their completed payloads and
    // publish or delete each group exactly once.
    program.preassign_label_to_next_insn(finalize_label);
    let finalize_loop_label = program.allocate_label();
    let finalize_next_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: touched_groups_cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(finalize_loop_label);
    program.emit_insn(Insn::RowId {
        cursor_id: touched_groups_cursor_id,
        dest: state_rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: state_cursor_id,
        src_reg: state_rowid_reg,
        target_pc: corrupt_label,
    });
    for i in 0..k {
        program.emit_insn(Insn::Column {
            cursor_id: state_cursor_id,
            column: i,
            dest: group_start + i,
            default: None,
        });
    }
    emit_payload_load(
        program,
        state_cursor_id,
        k + aggregates.len(),
        payload_start,
        total_payload,
    );

    if !scalar {
        turso_assert!(
            matches!(aggregates[0].func, AggFunc::Count0) && payload_offsets[0] == Some(0),
            "the hidden liveness COUNT(*) is the first payload"
        );
        program.emit_insn(Insn::Copy {
            src_reg: payload_start,
            dst_reg: cnt_reg,
            extra_amount: 0,
        });
        let live_group_label = program.allocate_label();
        program.emit_insn(Insn::Lt {
            lhs: cnt_reg,
            rhs: zero_reg,
            target_pc: corrupt_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Ne {
            lhs: cnt_reg,
            rhs: zero_reg,
            target_pc: live_group_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Copy {
            src_reg: group_start,
            dst_reg: index_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::IdxDelete {
            start_reg: index_rec_start,
            num_regs: k + 1,
            cursor_id: state_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: state_cursor_id,
            table_name: state_table_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: finalize_next_label,
        });
        program.preassign_label_to_next_insn(live_group_label);
    }

    emit_row_tail(program)?;

    program.preassign_label_to_next_insn(finalize_next_label);
    program.emit_insn(Insn::Next {
        cursor_id: touched_groups_cursor_id,
        pc_if_next: finalize_loop_label,
        fullscan: false,
        is_index: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} state is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);

    // Batch semantics emit one row for a scalar aggregate even over empty
    // input, so population must leave the single group present when the scan
    // contributed nothing (empty table, or WHERE filtered every row).
    // Maintenance never needs this: the row is created here and, with the
    // group-death path disabled for scalar shapes, never deleted.
    if scalar && input == MaintenanceInput::BaseTable {
        turso_assert!(
            group_members.is_none(),
            "a scalar aggregate's one group has one exact key"
        );
        let ensure_done = program.allocate_label();
        // The synthetic group key is the constant 0 by construction.
        program.emit_int(0, group_start);
        program.emit_insn(Insn::Found {
            cursor_id: state_index_cursor_id,
            target_pc: ensure_done,
            record_reg: group_start,
            num_regs: k,
        });
        for (i, agg) in aggregates.iter().enumerate() {
            if let Some(offset) = payload_offsets[i] {
                emit_payload_zero_init(program, agg, payload_start + offset);
            }
        }
        program.emit_insn(Insn::Null {
            dest: persisted_value_start,
            dest_end: Some(persisted_value_start + aggregates.len() - 1),
        });
        program.emit_insn(Insn::NewRowid {
            cursor: state_cursor_id,
            rowid_reg: state_rowid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
        program.emit_insn(Insn::Copy {
            src_reg: group_start,
            dst_reg: index_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(index_rec_start),
            count: to_u32(k + 1),
            dest_reg: to_u32(index_record_reg),
            index_name: Some(state_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: state_index_cursor_id,
            record_reg: index_record_reg,
            unpacked_start: Some(index_rec_start),
            unpacked_count: Some(to_u32(k + 1)),
            flags: IdxInsertFlags::new(),
        });
        emit_row_tail(program)?;
        program.preassign_label_to_next_insn(ensure_done);
    }

    drop(syms);
    Ok(())
}

/// Read the member that the class of the value key at
/// `value_key_start..value_key_start + k + 1` (group, then value) shows into
/// `dest` as [val, member columns..], and set `has_member_reg` to whether the
/// class has a member.
fn emit_shown_value(
    program: &mut ProgramBuilder,
    multiset: &MemberTable,
    value_key_start: usize,
    k: usize,
    dest: usize,
    has_member_reg: usize,
) {
    let done_label = program.allocate_label();
    program.emit_int(0, has_member_reg);
    emit_first_member(
        program,
        multiset,
        value_key_start,
        k + 1,
        k,
        1 + member_columns_width(1),
        dest,
        done_label,
    );
    program.emit_int(1, has_member_reg);
    program.preassign_label_to_next_insn(done_label);
}

/// Which SUM/TOTAL/AVG linear state (see `IvmSumData`) `func` maintains, or
/// `None` for every other aggregate, which keeps the shared
/// AggStep/AggInverse/AggContext* machinery unchanged.
fn ivm_sum_kind(func: &AggFunc) -> Option<IvmSumKind> {
    match func {
        AggFunc::Sum => Some(IvmSumKind::Sum),
        AggFunc::Total => Some(IvmSumKind::Total),
        AggFunc::Avg => Some(IvmSumKind::Avg),
        _ => None,
    }
}

/// Registers SUM/TOTAL/AVG's IVM state occupies: int_wrapped, int_wrap_count,
/// float_sum, float_err, non_int_count, non_null_count (see `IvmSumData`).
const IVM_SUM_PAYLOAD_WIDTH: usize = 6;

/// Payload width of `func`'s state in a materialized view's state table.
/// SUM/TOTAL/AVG get IVM's own linear state instead of the width the shared
/// window-function/regular-aggregate accumulator (`agg_payload_width`) uses,
/// which must stay unchanged for those other callers.
pub(super) fn ivm_payload_width(func: &AggFunc) -> Option<usize> {
    if ivm_sum_kind(func).is_some() {
        Some(IVM_SUM_PAYLOAD_WIDTH)
    } else {
        crate::vdbe::execute::agg_payload_width(func)
    }
}

/// Set `agg`'s payload registers, starting at `base`, to "no rows seen
/// yet": a zero COUNT, or SUM/TOTAL/AVG's all-zero state (see `IvmSumData`
/// for the field order).
fn emit_payload_zero_init(program: &mut ProgramBuilder, agg: &Aggregate, base: usize) {
    if ivm_sum_kind(&agg.func).is_none() {
        turso_assert!(
            matches!(agg.func, AggFunc::Count | AggFunc::Count0),
            "only COUNT and SUM/TOTAL/AVG keep a payload"
        );
        program.emit_int(0, base);
        return;
    }
    program.emit_int(0, base); // int_wrapped
    program.emit_int(0, base + 1); // int_wrap_count
    program.emit_insn(Insn::Real {
        value: 0.0,
        dest: base + 2, // float_sum
    });
    program.emit_insn(Insn::Real {
        value: 0.0,
        dest: base + 3, // float_err
    });
    program.emit_int(0, base + 4); // non_int_count
    program.emit_int(0, base + 5); // non_null_count
}

/// Read every payload column of the current state row into consecutive
/// registers starting at `payload_start`.
fn emit_payload_load(
    program: &mut ProgramBuilder,
    state_cursor_id: usize,
    first_payload_column: usize,
    payload_start: usize,
    total_payload: usize,
) {
    for j in 0..total_payload {
        program.emit_insn(Insn::Column {
            cursor_id: state_cursor_id,
            column: first_payload_column + j,
            dest: payload_start + j,
            default: None,
        });
    }
}

/// Add a row's contribution to `agg`'s payload (rooted at `payload_start`)
/// `weight_reg` times; a negative weight retracts it. COUNT adds the weight
/// to its count, SUM/TOTAL/AVG add weight times the value (`IvmSumStep`).
/// `arg_reg` is `None` only for COUNT(*).
fn emit_weighted_apply(
    program: &mut ProgramBuilder,
    agg: &Aggregate,
    arg_reg: Option<usize>,
    weight_reg: usize,
    payload_start: usize,
) {
    if ivm_sum_kind(&agg.func).is_some() {
        program.emit_insn(Insn::IvmSumStep {
            data: Box::new(IvmSumData {
                arg_reg: arg_reg.expect("SUM/TOTAL/AVG have an argument"),
                weight_reg,
                int_wrapped_reg: payload_start,
                int_wrap_count_reg: payload_start + 1,
                float_sum_reg: payload_start + 2,
                float_err_reg: payload_start + 3,
                non_int_count_reg: payload_start + 4,
                non_null_count_reg: payload_start + 5,
            }),
        });
        return;
    }
    turso_assert!(
        matches!(agg.func, AggFunc::Count | AggFunc::Count0),
        "only COUNT and SUM/TOTAL/AVG keep a payload"
    );
    // COUNT(x) ignores NULL; COUNT(*) counts every row.
    let skip_label = program.allocate_label();
    if let Some(arg_reg) = arg_reg {
        program.emit_insn(Insn::IsNull {
            reg: arg_reg,
            target_pc: skip_label,
        });
    }
    program.emit_insn(Insn::Add {
        lhs: payload_start,
        rhs: weight_reg,
        dest: payload_start,
    });
    program.preassign_label_to_next_insn(skip_label);
}
