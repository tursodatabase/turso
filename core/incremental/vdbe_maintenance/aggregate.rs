use super::binding::{remap_bound_expr, seed_ephemeral_stream_cache, stream_table_references};
use super::output::{DeltaSource, EmittedNodeOutput, NodeOutputContract};
use super::plan::{tracks_distinct_values, uses_multiset, OperatorStateDef};
use super::stream::{
    btree_arrangement, emit_operator_rowid_delta, open_ephemeral_delta, ArrangementIdentityColumn,
    DeltaIdentity, EphemeralDelta,
};
use super::MaintenanceInput;
use crate::function::AggFunc;
use crate::incremental::dag;
use crate::schema::Schema;
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    translate_condition_expr, translate_expr_no_constant_opt, ConditionMetadata,
    NoConstantOptReason,
};
use crate::translate::plan::Aggregate;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::{Connection, LimboError, Result};
use turso_parser::ast;

struct AggregateMultisetCursors {
    table_name: String,
    table_cursor_id: usize,
    index_cursor_id: usize,
    index_name: String,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_group_aggregate(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    channel: &EphemeralDelta,
    group_exprs: &[ast::Expr],
    group_collations: &[CollationSeq],
    aggregates: &[Aggregate],
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
        false,
    );
    emit_group_aggregate_rows(
        program,
        view_name,
        channel,
        group_exprs,
        group_collations,
        aggregates,
        scalar,
        &output,
        input,
        operator_state,
        schema,
        connection,
    )?;

    let state_table_name = operator_state.state_table_name()?;
    let state_table = schema.get_btree_table(state_table_name).ok_or_else(|| {
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
    channel: &EphemeralDelta,
    group_exprs: &[ast::Expr],
    group_collations: &[CollationSeq],
    aggregates: &[Aggregate],
    scalar: bool,
    output: &EphemeralDelta,
    input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    use crate::function::AccumulatorFunc;
    let num_view_columns = output.width;
    // With a collated key, values that compare equal can differ in bytes.
    // The stored key is the group's first-seen representative: reloading it
    // on every lookup keeps the state row, its index entry, and the view row
    // byte-identical, and matches the value batch GROUP BY displays. With
    // binary keys a matched lookup implies byte equality, so the reload is
    // skipped.
    let reload_stored_group = group_collations
        .iter()
        .any(|c| !matches!(c, CollationSeq::Binary | CollationSeq::Unset));

    turso_assert!(
        num_view_columns == group_exprs.len() + aggregates.len(),
        "aggregate output stream must use the node's natural schema"
    );
    let k = group_exprs.len();
    // Payload aggregates persist fixed-width state in the group state table;
    // MIN/MAX keep a value multiset in the multiset table instead
    // (payload_widths[i] is None for them, and they have no accumulator).
    // DISTINCT aggregates have both: the accumulator payload plus multiset
    // rows deciding which values reach the accumulator.
    let payload_widths: Vec<Option<usize>> = aggregates
        .iter()
        .map(|agg| crate::vdbe::execute::agg_payload_width(&agg.func))
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
    let state_table = schema.get_btree_table(&state_table_name).ok_or_else(|| {
        LimboError::InternalError(format!(
            "state table {state_table_name} of materialized view {view_name} not found"
        ))
    })?;
    let state_index = schema
        .get_indices(&state_table_name)
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
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
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
    let mut mm_cursors = Vec::with_capacity(aggregates.len());
    for (aggregate_index, aggregate) in aggregates.iter().enumerate() {
        if !uses_multiset(aggregate) {
            mm_cursors.push(None);
            continue;
        }
        let table_name = operator_state
            .aggregate_multiset_table_name(aggregate_index)?
            .to_string();
        let mm_table = schema.get_btree_table(&table_name).ok_or_else(|| {
            LimboError::InternalError(format!(
                "aggregate multiset table {table_name} of materialized view {view_name} not found"
            ))
        })?;
        let mm_index = schema
            .get_indices(&table_name)
            .next()
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "aggregate multiset table {table_name} of materialized view {view_name} has no index"
                ))
            })?;
        let mm_table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(mm_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: mm_table_cursor_id,
            root_page: RegisterOrLiteral::Literal(mm_table.root_page),
            db: 0,
        });
        let mm_index_cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(mm_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: mm_index_cursor_id,
            root_page: RegisterOrLiteral::Literal(mm_index.root_page),
            db: 0,
        });
        mm_cursors.push(Some(AggregateMultisetCursors {
            table_name,
            table_cursor_id: mm_table_cursor_id,
            index_cursor_id: mm_index_cursor_id,
            index_name: mm_index.name.clone(),
        }));
    }

    // Register layout.
    let weight_reg = program.alloc_register();
    let null_arg_reg = program.alloc_register(); // stays NULL: COUNT(*) argument
    let zero_reg = program.alloc_register();
    let found_reg = program.alloc_register();
    let cnt_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    let arg_reg = program.alloc_register();
    // Accumulators, one per aggregate (index 0 = hidden liveness COUNT(*)).
    let acc_start = program.alloc_registers(aggregates.len());
    // State record image: [g.., finalized aggregate values, payloads..].
    // Keeping the natural output first makes the state table directly
    // scannable as the aggregate node's arrangement.
    let state_rec_start = program.alloc_registers(k + total_payload + aggregates.len());
    let group_start = state_rec_start;
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
    // Per-aggregate multiset record image: [g.., val, mult-or-rowid].
    let mm_rec_start = program.alloc_registers(k + 2);
    let mm_val_reg = mm_rec_start + k;
    let mm_last_reg = mm_rec_start + k + 1;
    let mm_rowid_reg = program.alloc_register();
    let mm_mult_reg = program.alloc_register();
    let mm_record_reg = program.alloc_register();
    let mm_found_reg = program.alloc_register();

    program.emit_insn(Insn::Null {
        dest: null_arg_reg,
        dest_end: None,
    });
    program.emit_int(0, zero_reg);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let corrupt_label = program.allocate_label();

    // Joined-delta rows apply in two passes: every positive contribution,
    // then every negative one. The single-table capture order guarantees a
    // group's running weight never dips below zero, but the join phase
    // decomposition does not — a pair that exists only across generations
    // contributes its retraction before its insertion, and the
    // unknown-group-retraction no-op below would drop it, leaving the later
    // positive over-counted. Positives-first restores the invariant: every
    // group's (and multiset value's) running weight stays non-negative, and
    // since aggregate contributions carry their own values, reordering them
    // is sound.
    let pass_reg = channel.requires_positive_first.then(|| {
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
        cursor_id: input_cursor_id,
        pc_if_empty: pass_done_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    // Accumulators must start fresh for every input row.
    program.emit_insn(Insn::Null {
        dest: acc_start,
        dest_end: Some(acc_start + aggregates.len() - 1),
    });

    // Weight of the current input row.
    program.emit_insn(Insn::Column {
        cursor_id: input_cursor_id,
        column: channel.weight_column,
        dest: weight_reg,
        default: None,
    });
    // Pass filter: pass 0 skips negative contributions, pass 1 skips
    // positive ones.
    if let Some(pass_reg) = pass_reg {
        let negative_pass_label = program.allocate_label();
        let pass_ok_label = program.allocate_label();
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

    seed_ephemeral_stream_cache(program, channel, &binding_remap, &mut resolver)?;

    for (i, group_expr) in group_exprs.iter().enumerate() {
        let bound = remap_bound_expr(group_expr, &binding_remap)?;
        translate_expr_no_constant_opt(
            program,
            Some(&table_references),
            &bound,
            group_start + i,
            &resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }

    // Group lookup via the state table's primary-key index.
    let found_label = program.allocate_label();
    let apply_label = program.allocate_label();
    program.emit_insn(Insn::Found {
        cursor_id: state_index_cursor_id,
        target_pc: found_label,
        record_reg: group_start,
        num_regs: k,
    });
    // Not found: a retraction for a group the state does not know is a no-op
    // (matching the filter/project merge behavior for unknown rowids).
    program.emit_insn(Insn::Lt {
        lhs: weight_reg,
        rhs: zero_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_int(0, found_reg);
    program.emit_insn(Insn::Null {
        dest: persisted_value_start,
        dest_end: Some(persisted_value_start + aggregates.len() - 1),
    });
    // A fresh group's DISTINCT or filtered accumulators may never be stepped
    // in the apply loop, but the write path stores every payload accumulator.
    // Materialize their empty contexts now. A NULL argument contributes
    // nothing except to COUNT(*), whose initialization step is inverted.
    for (i, agg) in aggregates.iter().enumerate() {
        if !tracks_distinct_values(agg) && agg.filter_expr.is_none() {
            continue;
        }
        program.emit_insn(Insn::AggStep {
            acc_reg: acc_start + i,
            col: null_arg_reg,
            delimiter: 0,
            func: AccumulatorFunc::Agg(agg.func.clone()),
            comparator: None,
        });
        if matches!(agg.func, AggFunc::Count0) {
            program.emit_insn(Insn::AggInverse {
                acc_reg: acc_start + i,
                col: null_arg_reg,
                delimiter: 0,
                func: AccumulatorFunc::Agg(agg.func.clone()),
                comparator: None,
            });
        }
    }
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
    if reload_stored_group {
        for i in 0..k {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: i,
                dest: group_start + i,
                default: None,
            });
        }
    }
    for (i, agg) in aggregates.iter().enumerate() {
        let Some(offset) = payload_offsets[i] else {
            continue; // MIN/MAX: state lives in the multiset table
        };
        for j in 0..payload_widths[i].expect("offset implies width") {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: k + aggregates.len() + offset + j,
                dest: payload_start + offset + j,
                default: None,
            });
        }
        program.emit_insn(Insn::AggContextLoad {
            acc_reg: acc_start + i,
            payload_start_reg: payload_start + offset,
            func: AccumulatorFunc::Agg(agg.func.clone()),
        });
    }
    for i in 0..aggregates.len() {
        program.emit_insn(Insn::Column {
            cursor_id: state_cursor_id,
            column: k + i,
            dest: persisted_value_start + i,
            default: None,
        });
    }
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

    program.preassign_label_to_next_insn(apply_label);
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
                arg_reg
            }
            None => null_arg_reg,
        };

        if is_minmax || is_distinct {
            // Weight-merge the value into the aggregate's multiset:
            // (group.., value) -> multiplicity. NULLs are ignored, matching
            // aggregate NULL semantics. For a DISTINCT aggregate the
            // accumulator steps only when a value's multiplicity transitions
            // 0 -> positive and inverts only on positive -> 0, so it sees
            // each distinct value exactly once. Transition detection relies
            // on delta rows carrying unit weights (each captured DML op is
            // one row of weight +1 or -1, and population scans weigh +1).
            let mm = mm_cursors[i]
                .as_ref()
                .expect("DAG validation guarantees one multiset per multiset aggregate");
            let mm_found_label = program.allocate_label();
            let mm_upsert_label = program.allocate_label();

            program.emit_insn(Insn::IsNull {
                reg: col_reg,
                target_pc: aggregate_done_label,
            });
            // Key image: [g.., val].
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: mm_rec_start,
                extra_amount: k.saturating_sub(1),
            });
            program.emit_insn(Insn::Copy {
                src_reg: col_reg,
                dst_reg: mm_val_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Found {
                cursor_id: mm.index_cursor_id,
                target_pc: mm_found_label,
                record_reg: mm_rec_start,
                num_regs: k + 1,
            });
            // Not found: retraction of an unknown value is a no-op.
            program.emit_insn(Insn::Lt {
                lhs: weight_reg,
                rhs: zero_reg,
                target_pc: aggregate_done_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            program.emit_int(0, mm_found_reg);
            program.emit_insn(Insn::NewRowid {
                cursor: mm.table_cursor_id,
                rowid_reg: mm_rowid_reg,
                prev_largest_reg: prev_rowid_scratch,
            });
            program.emit_insn(Insn::Copy {
                src_reg: weight_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            if is_distinct {
                // First occurrence of this value in the group.
                program.emit_insn(Insn::AggStep {
                    acc_reg: acc_start + i,
                    col: mm_val_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::Goto {
                target_pc: mm_upsert_label,
            });

            program.preassign_label_to_next_insn(mm_found_label);
            program.emit_int(1, mm_found_reg);
            program.emit_insn(Insn::IdxRowId {
                cursor_id: mm.index_cursor_id,
                dest: mm_rowid_reg,
            });
            program.emit_insn(Insn::SeekRowid {
                cursor_id: mm.table_cursor_id,
                src_reg: mm_rowid_reg,
                target_pc: corrupt_label,
            });
            program.emit_insn(Insn::Column {
                cursor_id: mm.table_cursor_id,
                column: k + 1,
                dest: mm_mult_reg,
                default: None,
            });
            program.emit_insn(Insn::Add {
                lhs: weight_reg,
                rhs: mm_mult_reg,
                dest: mm_last_reg,
            });
            program.emit_insn(Insn::Ne {
                lhs: mm_last_reg,
                rhs: zero_reg,
                target_pc: mm_upsert_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            // Multiplicity reached zero: remove the value.
            if is_distinct {
                // Last occurrence of this value left the group.
                program.emit_insn(Insn::AggInverse {
                    acc_reg: acc_start + i,
                    col: mm_val_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: mm_rowid_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: mm_rec_start,
                num_regs: k + 2,
                cursor_id: mm.index_cursor_id,
                raise_error_if_no_matching_entry: true,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: mm.table_cursor_id,
                table_name: mm.table_name.clone(),
                is_part_of_update: true,
            });
            program.emit_insn(Insn::Goto {
                target_pc: aggregate_done_label,
            });

            // Write (g.., val, mult) at the row's rowid; a fresh value also
            // gets an index entry (g.., val, rowid).
            program.preassign_label_to_next_insn(mm_upsert_label);
            program.emit_insn(Insn::MakeRecord {
                start_reg: mm_rec_start as u16,
                count: (k + 2) as u16,
                dest_reg: mm_record_reg as u16,
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::Insert {
                cursor: mm.table_cursor_id,
                key_reg: mm_rowid_reg,
                record_reg: mm_record_reg,
                flag: InsertFlags(
                    InsertFlags::REQUIRE_SEEK
                        | InsertFlags::SKIP_LAST_ROWID
                        | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
                ),
                table_name: mm.table_name.clone(),
            });
            // Only a freshly-inserted value needs an index entry; an
            // existing value's entry is already present.
            let mm_skip_idx_label = program.allocate_label();
            program.emit_insn(Insn::If {
                reg: mm_found_reg,
                target_pc: mm_skip_idx_label,
                jump_if_null: false,
            });
            program.emit_insn(Insn::Copy {
                src_reg: mm_rowid_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MakeRecord {
                start_reg: mm_rec_start as u16,
                count: (k + 2) as u16,
                dest_reg: mm_record_reg as u16,
                index_name: Some(mm.index_name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: mm.index_cursor_id,
                record_reg: mm_record_reg,
                unpacked_start: Some(mm_rec_start),
                unpacked_count: Some((k + 2) as u16),
                flags: IdxInsertFlags::new(),
            });
            program.preassign_label_to_next_insn(mm_skip_idx_label);
            program.preassign_label_to_next_insn(aggregate_done_label);
            continue;
        }

        let step_label = program.allocate_label();
        let after_label = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: step_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::AggInverse {
            acc_reg: acc_start + i,
            col: col_reg,
            delimiter: 0,
            func: AccumulatorFunc::Agg(agg.func.clone()),
            comparator: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: after_label,
        });
        program.preassign_label_to_next_insn(step_label);
        program.emit_insn(Insn::AggStep {
            acc_reg: acc_start + i,
            col: col_reg,
            delimiter: 0,
            func: AccumulatorFunc::Agg(agg.func.clone()),
            comparator: None,
        });
        program.preassign_label_to_next_insn(after_label);
        program.preassign_label_to_next_insn(aggregate_done_label);
    }

    // Group liveness: the hidden COUNT(*) is zero when every row of the
    // group has been retracted. A scalar aggregate's single group is never
    // deleted — retracting the last row rewrites its row with empty-input
    // aggregate values instead.
    if !scalar {
        program.emit_insn(Insn::AggValue {
            acc_reg: acc_start,
            dest_reg: cnt_reg,
            func: AccumulatorFunc::Agg(AggFunc::Count0),
        });
        let write_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: cnt_reg,
            rhs: zero_reg,
            target_pc: write_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        // Group emptied: remove its state row and index entry. The old
        // arrangement row was already retracted above. A
        // fresh group cannot reach zero (its first change was an insert), so
        // found_reg is always set here; stay total anyway.
        let do_delete_label = program.allocate_label();
        program.emit_insn(Insn::If {
            reg: found_reg,
            target_pc: do_delete_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });
        program.preassign_label_to_next_insn(do_delete_label);
        if k > 1 {
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: index_rec_start,
                extra_amount: k - 1,
            });
        } else {
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: index_rec_start,
                extra_amount: 0,
            });
        }
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
            target_pc: next_label,
        });
        program.preassign_label_to_next_insn(write_label);
    }

    // Group live: persist aggregate state and publish its natural delta.
    let have_rowids_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: have_rowids_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: state_cursor_id,
        rowid_reg: state_rowid_reg,
        prev_largest_reg: prev_rowid_scratch,
    });
    program.preassign_label_to_next_insn(have_rowids_label);
    for (i, agg) in aggregates.iter().enumerate() {
        let Some(offset) = payload_offsets[i] else {
            continue; // MIN/MAX: state lives in the multiset table
        };
        program.emit_insn(Insn::AggContextStore {
            acc_reg: acc_start + i,
            payload_start_reg: payload_start + offset,
            func: AccumulatorFunc::Agg(agg.func.clone()),
        });
    }
    let skip_index_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: skip_index_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Copy {
        src_reg: group_start,
        dst_reg: index_rec_start,
        extra_amount: k.saturating_sub(1),
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: index_rec_start as u16,
        count: (k + 1) as u16,
        dest_reg: index_record_reg as u16,
        index_name: Some(state_index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: state_index_cursor_id,
        record_reg: index_record_reg,
        unpacked_start: Some(index_rec_start),
        unpacked_count: Some((k + 1) as u16),
        flags: IdxInsertFlags::new(),
    });
    program.preassign_label_to_next_insn(skip_index_label);

    // Finalize every aggregate and publish the node's natural row. HAVING is
    // an ordinary downstream Filter node in the maintenance DAG.
    let emit_row_tail = |program: &mut ProgramBuilder| -> Result<()> {
        for (i, agg) in aggregates.iter().enumerate() {
            let value_reg = agg_value_start + i;
            if matches!(agg.func, AggFunc::Min | AggFunc::Max) {
                // The group's extreme is the first (MIN) or last (MAX)
                // entry in this aggregate's multiset with the group prefix.
                let mm = mm_cursors[i]
                    .as_ref()
                    .expect("MIN/MAX aggregates own a multiset table");
                let empty_label = program.allocate_label();
                let have_label = program.allocate_label();
                program.emit_insn(Insn::Copy {
                    src_reg: group_start,
                    dst_reg: mm_rec_start,
                    extra_amount: k.saturating_sub(1),
                });
                match agg.func {
                    AggFunc::Min => {
                        program.emit_insn(Insn::SeekGE {
                            is_index: true,
                            cursor_id: mm.index_cursor_id,
                            start_reg: mm_rec_start,
                            num_regs: k,
                            target_pc: empty_label,
                            eq_only: false,
                        });
                        // Positioned past the group: no values.
                        program.emit_insn(Insn::IdxGT {
                            cursor_id: mm.index_cursor_id,
                            start_reg: mm_rec_start,
                            num_regs: k,
                            target_pc: empty_label,
                        });
                    }
                    AggFunc::Max => {
                        program.emit_insn(Insn::SeekLE {
                            is_index: true,
                            cursor_id: mm.index_cursor_id,
                            start_reg: mm_rec_start,
                            num_regs: k,
                            target_pc: empty_label,
                            eq_only: false,
                        });
                        // Positioned before the group: no values.
                        program.emit_insn(Insn::IdxLT {
                            cursor_id: mm.index_cursor_id,
                            start_reg: mm_rec_start,
                            num_regs: k,
                            target_pc: empty_label,
                        });
                    }
                    _ => unreachable!(),
                }
                program.emit_insn(Insn::Column {
                    cursor_id: mm.index_cursor_id,
                    column: k,
                    dest: value_reg,
                    default: None,
                });
                program.emit_insn(Insn::Goto {
                    target_pc: have_label,
                });
                program.preassign_label_to_next_insn(empty_label);
                program.emit_insn(Insn::Null {
                    dest: value_reg,
                    dest_end: None,
                });
                program.preassign_label_to_next_insn(have_label);
            } else {
                program.emit_insn(Insn::AggValue {
                    acc_reg: acc_start + i,
                    dest_reg: value_reg,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                });
            }
        }

        program.emit_insn(Insn::Copy {
            src_reg: agg_value_start,
            dst_reg: persisted_value_start,
            extra_amount: aggregates.len().saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: state_rec_start as u16,
            count: (k + total_payload + aggregates.len()) as u16,
            dest_reg: state_record_reg as u16,
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

    emit_row_tail(program)?;

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input_cursor_id,
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
    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
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
        let ensure_done = program.allocate_label();
        // The synthetic group key is the constant 0 by construction.
        program.emit_int(0, group_start);
        program.emit_insn(Insn::Found {
            cursor_id: state_index_cursor_id,
            target_pc: ensure_done,
            record_reg: group_start,
            num_regs: k,
        });
        // Fresh, empty accumulators: stepping a NULL argument materializes
        // each payload context without contributing to it — except COUNT(*),
        // which counts rows regardless of argument, so its init step is
        // cancelled with the matching inverse.
        program.emit_insn(Insn::Null {
            dest: acc_start,
            dest_end: Some(acc_start + aggregates.len() - 1),
        });
        for (i, agg) in aggregates.iter().enumerate() {
            if payload_offsets[i].is_none() {
                continue; // MIN/MAX: an empty multiset already means NULL
            }
            program.emit_insn(Insn::AggStep {
                acc_reg: acc_start + i,
                col: null_arg_reg,
                delimiter: 0,
                func: AccumulatorFunc::Agg(agg.func.clone()),
                comparator: None,
            });
            if matches!(agg.func, AggFunc::Count0) {
                program.emit_insn(Insn::AggInverse {
                    acc_reg: acc_start + i,
                    col: null_arg_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::AggContextStore {
                acc_reg: acc_start + i,
                payload_start_reg: payload_start + payload_offsets[i].expect("checked above"),
                func: AccumulatorFunc::Agg(agg.func.clone()),
            });
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
            start_reg: index_rec_start as u16,
            count: (k + 1) as u16,
            dest_reg: index_record_reg as u16,
            index_name: Some(state_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: state_index_cursor_id,
            record_reg: index_record_reg,
            unpacked_start: Some(index_rec_start),
            unpacked_count: Some((k + 1) as u16),
            flags: IdxInsertFlags::new(),
        });
        emit_row_tail(program)?;
        program.preassign_label_to_next_insn(ensure_done);
    }

    drop(syms);
    Ok(())
}
