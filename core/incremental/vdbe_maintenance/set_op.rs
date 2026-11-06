use super::consolidate::{consolidate_delta, ConsolidatedDelta};
use super::members::{
    emit_add_to_member, emit_exact_value_record, emit_first_member, emit_member_key,
    member_columns_width, MemberTable,
};
use super::output::{EmittedNodeOutput, NodeOutputContract};
use super::plan::OperatorStateDef;
use super::stream::{
    emit_operator_rowid_delta, open_ephemeral_delta, DeltaIdentity, EphemeralDelta,
};
use crate::incremental::dag;
use crate::namespace::NamespaceId;
use crate::schema::Schema;
use crate::translate::collate::CollationSeq;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral,
};
use crate::{LimboError, Result};
use turso_parser::ast;

/// Emit one compound-query node from its already-materialized branch deltas.
///
/// Pure UNION ALL is a stateless identity-namespacing map. Any deduplicating
/// prefix owns the content and append integrals declared for this node.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_set_op(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    inputs: &[EphemeralDelta],
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    key_columns: &[dag::KeyColumn],
    output_contract: &NodeOutputContract,
    operator_state: &OperatorStateDef,
    schema: &Schema,
) -> Result<EmittedNodeOutput> {
    let output = if prefix_len == 0 {
        if operators
            .iter()
            .any(|operator| *operator != ast::CompoundOperator::UnionAll)
        {
            return Err(LimboError::InternalError(
                "stateless set-op has a deduplicating operator".to_string(),
            ));
        }
        emit_union_all_delta(program, view_name, node_id, inputs, output_contract)?
    } else {
        let output = open_ephemeral_delta(
            program,
            &format!("{view_name}_set_op_delta_{node_id}"),
            output_contract.schema.as_ref().clone(),
            output_contract.emitted_identity,
            output_contract.binding_rowids.clone(),
        )?;
        let inputs = inputs
            .iter()
            .enumerate()
            .map(|(branch, input)| {
                consolidate_delta(
                    program,
                    &format!("{view_name}_set_op_input_{node_id}_{branch}"),
                    input,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        emit_deduplicating_set_op(
            program,
            view_name,
            &inputs,
            operators,
            prefix_len,
            key_columns,
            &output,
            operator_state,
            schema,
        )?;
        output
    };
    EmittedNodeOutput::from_ephemeral(node_id, output, output_contract)
}

fn emit_union_all_delta(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    inputs: &[EphemeralDelta],
    output_contract: &NodeOutputContract,
) -> Result<EphemeralDelta> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_union_all_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    for (branch, input) in inputs.iter().enumerate() {
        append_union_all_branch(program, branch, input, &output)?;
    }
    Ok(output)
}

/// Append one UNION ALL branch while namespacing its complete source identity
/// into the fixed `(branch, packed identity)` key declared by the set-op.
fn append_union_all_branch(
    program: &mut ProgramBuilder,
    branch: usize,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
) -> Result<()> {
    if input.width != output.width
        || input.identity_width() == 0
        || output.identity != DeltaIdentity::OperatorKey(2)
    {
        return Err(LimboError::InternalError(
            "UNION ALL branch has an incompatible delta identity".to_string(),
        ));
    }

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let record_start = program.alloc_registers(output.record_width());
    let source_identity_start = program.alloc_registers(input.identity_width());
    let record_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    program.emit_int(branch as i64, record_start);
    for column in 0..input.identity_width() {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: source_identity_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(source_identity_start),
        count: to_u32(input.identity_width()),
        dest_reg: to_u32(record_start + 1),
        index_name: None,
        affinity_str: None,
    });
    // The planner publishes a binding's rowid only when every branch has it.
    for (binding, output_column) in output.binding_rowid_columns.iter().enumerate() {
        let input_column = input.binding_rowid_columns.get(binding).copied().flatten();
        match (output_column, input_column) {
            (Some(output_column), Some(input_column)) => program.emit_insn(Insn::Column {
                cursor_id: input.cursor_id,
                column: input_column,
                dest: record_start + *output_column,
                default: None,
            }),
            (None, _) => {}
            (Some(_), None) => {
                return Err(LimboError::InternalError(
                    "UNION ALL publishes a rowid that one of its branches lacks".to_string(),
                ))
            }
        }
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

/// Emit `dst = 1` if the count in `src` is positive, else `0`. Counts are
/// always integers, so three-valued logic never applies.
fn emit_presence(program: &mut ProgramBuilder, src: usize, zero_reg: usize, dst: usize) {
    let done = program.allocate_label();
    program.emit_int(1, dst);
    program.emit_insn(Insn::Gt {
        lhs: src,
        rhs: zero_reg,
        target_pc: done,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_int(0, dst);
    program.preassign_label_to_next_insn(done);
}

/// Emit the left-associative visibility fold over the per-branch counts at
/// `cnt_start..cnt_start + prefix_len` into `dest`: a content row is in the
/// compound result iff the fold of per-branch presence is true (UNION and
/// UNION ALL fold as OR, INTERSECT as AND, EXCEPT as AND NOT).
fn emit_visibility_fold(
    program: &mut ProgramBuilder,
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    cnt_start: usize,
    zero_reg: usize,
    scratch_reg: usize,
    dest: usize,
) {
    emit_presence(program, cnt_start, zero_reg, dest);
    for i in 1..prefix_len {
        emit_presence(program, cnt_start + i, zero_reg, scratch_reg);
        match operators[i - 1] {
            ast::CompoundOperator::Union | ast::CompoundOperator::UnionAll => {
                program.emit_insn(Insn::Or {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
            ast::CompoundOperator::Intersect => {
                program.emit_insn(Insn::And {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
            ast::CompoundOperator::Except => {
                program.emit_insn(Insn::Not {
                    reg: scratch_reg,
                    dest: scratch_reg,
                });
                program.emit_insn(Insn::And {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
        }
    }
}

/// Emit the view rowid for a state rowid in `srid_reg` into `dst`. When the
/// view keeps both content rows and append rows, their state rowids come
/// from two independent tables, so they map to disjoint view rowids:
/// 2*rowid for content rows, 2*rowid + 1 for append rows. With a single
/// state table the state rowid is the view rowid, as everywhere else.
fn emit_split_view_rowid(
    program: &mut ProgramBuilder,
    both_tables: bool,
    srid_reg: usize,
    two_reg: usize,
    one_reg: usize,
    dst: usize,
    append: bool,
) {
    if both_tables {
        program.emit_insn(Insn::Multiply {
            lhs: srid_reg,
            rhs: two_reg,
            dest: dst,
        });
        if append {
            program.emit_insn(Insn::Add {
                lhs: dst,
                rhs: one_reg,
                dest: dst,
            });
        }
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: srid_reg,
            dst_reg: dst,
            extra_amount: 0,
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_deduplicating_set_op(
    program: &mut ProgramBuilder,
    view_name: &str,
    branch_streams: &[ConsolidatedDelta],
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    key_columns: &[dag::KeyColumn],
    output: &EphemeralDelta,
    operator_state: &OperatorStateDef,
    schema: &Schema,
) -> Result<()> {
    let n_branches = branch_streams.len();
    let num_view_columns = output.width;
    turso_assert!(
        prefix_len > 0,
        "pure UNION ALL uses stateless branch composition"
    );
    let has_append = prefix_len < n_branches;
    let materializes_append = has_append;
    // Width of the content key: every output column.
    let k = num_view_columns;
    turso_assert!(
        key_columns.len() == k,
        "a deduplicating set operation has one key column per output column"
    );

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
    // Trailing UNION ALL rows preserve source identity in the auxiliary table.
    let (append_table_name, append_table, append_index) = if materializes_append {
        let name = operator_state.set_op_identity_table_name()?.to_string();
        let table = schema
            .get_btree_table(NamespaceId::ROOT, &name)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "append table {name} of materialized view {view_name} not found"
                ))
            })?;
        let index = schema
            .get_indices(NamespaceId::ROOT, &name)
            .next()
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "append table {name} of materialized view {view_name} has no index"
                ))
            })?;
        (name, table, index)
    } else {
        (
            state_table_name.clone(),
            state_table.clone(),
            state_index.clone(),
        )
    };

    let state_cursor = program.alloc_cursor_id(CursorType::BTreeTable(state_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_cursor,
        root_page: RegisterOrLiteral::Literal(state_table.root_page),
        db: 0,
    });
    let state_index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(state_index.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_index_cursor,
        root_page: RegisterOrLiteral::Literal(state_index.root_page),
        db: 0,
    });
    let content_cursors = (state_cursor, state_index_cursor);
    let append_cursors = if materializes_append {
        let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(append_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: table_cursor,
            root_page: RegisterOrLiteral::Literal(append_table.root_page),
            db: 0,
        });
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(append_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor,
            root_page: RegisterOrLiteral::Literal(append_index.root_page),
            db: 0,
        });
        Some((table_cursor, index_cursor))
    } else {
        None
    };
    // A class that can hold several exact values keeps them all and shows
    // the smallest (see `members`).
    let members = if key_columns.iter().any(|key| key.ambiguous) {
        Some(MemberTable::open(
            program,
            schema,
            view_name,
            operator_state.set_op_members_table_name()?,
        )?)
    } else {
        None
    };
    // Register layout, shared by all branches.
    let zero_reg = program.alloc_register();
    let two_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    // [outputs.., weight], contiguous for MakeRecord.
    let out_start = program.alloc_registers(num_view_columns + 1);
    let out_weight_reg = out_start + num_view_columns;
    let old_out_start = program.alloc_registers(num_view_columns);
    let w_reg = program.alloc_register();
    let view_rowid_reg = program.alloc_register();
    // Content-merge registers: [key.., state_rowid] contiguous for
    // Found/IdxInsert/IdxDelete, and [key.., counts..] as the state row
    // image (the counts live in place at the record tail).
    let c_return_reg = program.alloc_register();
    let key_start = program.alloc_registers(k + 1);
    let c_srid_reg = key_start + k;
    let state_rec_start = program.alloc_registers(k + prefix_len.max(1));
    let cnt_start = state_rec_start + k;
    let v_old_reg = program.alloc_register();
    let v_new_reg = program.alloc_register();
    let p_scratch_reg = program.alloc_register();
    let c_state_record_reg = program.alloc_register();
    let c_index_record_reg = program.alloc_register();
    // Class-member key [class.., member columns..] and the member's counts.
    let member_key_start = program.alloc_registers(k + member_columns_width(k));
    let old_count_reg = program.alloc_register();
    let new_count_reg = program.alloc_register();
    // Exact records of the values a class showed before and shows after.
    let old_exact_reg = program.alloc_register();
    let new_exact_reg = program.alloc_register();
    // [values.., state_rowid] of an index entry to delete or insert.
    let index_key_start = program.alloc_registers(k + 1);
    // Append-merge registers: [branch, rid, state_rowid] and the
    // (branch, rid, mult) state row image.
    let a_return_reg = program.alloc_register();
    let akey_start = program.alloc_registers(3);
    let a_srid_reg = akey_start + 2;
    let a_rec_start = program.alloc_registers(3);
    let cur_w_reg = program.alloc_register();
    let new_w_reg = program.alloc_register();
    let a_state_record_reg = program.alloc_register();
    let a_index_record_reg = program.alloc_register();

    program.emit_int(0, zero_reg);
    program.emit_int(2, two_reg);
    program.emit_int(1, one_reg);

    let main_start_label = program.allocate_label();
    let corrupt_label = program.allocate_label();
    let end_label = program.allocate_label();

    program.emit_insn(Insn::Goto {
        target_pc: main_start_label,
    });

    // Content-merge subroutines, one per prefix branch (the branch decides
    // which count column the weight lands in): apply (key, outputs, w) to
    // the content state row, then flip the view row on visibility-fold
    // transitions. Each branch input is consolidated, so it only retracts
    // rows that are present: a count below zero is corrupt state.
    let mut content_merge_labels = Vec::with_capacity(prefix_len);
    for b in 0..prefix_len {
        let (state_cursor_id, state_index_cursor_id) = content_cursors;
        let cm_label = program.allocate_label();
        program.preassign_label_to_next_insn(cm_label);
        content_merge_labels.push(cm_label);

        let found_label = program.allocate_label();
        let counted_label = program.allocate_label();
        let vnew_label = program.allocate_label();
        let not_all_zero_label = program.allocate_label();
        let insert_label = program.allocate_label();
        let ret_label = program.allocate_label();

        // Only branches whose rows can reach the result supply its members:
        // the first branch and every UNION or UNION ALL branch. INTERSECT and
        // EXCEPT branches only decide whether a class is shown.
        let supplies_rows = b == 0
            || matches!(
                operators[b - 1],
                ast::CompoundOperator::Union | ast::CompoundOperator::UnionAll
            );
        if let (Some(members), true) = (&members, supplies_rows) {
            emit_member_key(program, out_start, k, member_key_start);
            emit_add_to_member(
                program,
                members,
                member_key_start,
                w_reg,
                old_count_reg,
                new_count_reg,
                corrupt_label,
            );
        }

        program.emit_insn(Insn::Found {
            cursor_id: state_index_cursor_id,
            target_pc: found_label,
            record_reg: key_start,
            num_regs: k,
        });
        // Fresh content row: this branch's count is the weight, the rest 0.
        program.emit_insn(Insn::Lt {
            lhs: w_reg,
            rhs: zero_reg,
            target_pc: corrupt_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        program.emit_insn(Insn::NewRowid {
            cursor: state_cursor_id,
            rowid_reg: c_srid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
        for i in 0..prefix_len {
            program.emit_int(0, cnt_start + i);
        }
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: cnt_start + b,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(key_start),
            count: to_u32(k + 1),
            dest_reg: to_u32(c_index_record_reg),
            index_name: Some(state_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: state_index_cursor_id,
            record_reg: c_index_record_reg,
            unpacked_start: Some(key_start),
            unpacked_count: Some(to_u32(k + 1)),
            flags: IdxInsertFlags::new(),
        });
        // The new state row and its index entry hold this row's values.
        program.emit_insn(Insn::Copy {
            src_reg: key_start,
            dst_reg: old_out_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_int(0, v_old_reg);
        program.emit_insn(Insn::Goto {
            target_pc: counted_label,
        });

        program.preassign_label_to_next_insn(found_label);
        program.emit_insn(Insn::IdxRowId {
            cursor_id: state_index_cursor_id,
            dest: c_srid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: state_cursor_id,
            src_reg: c_srid_reg,
            target_pc: corrupt_label,
        });
        // The values the class shows now; with one exact value per class
        // they equal this row's.
        if members.is_some() {
            for i in 0..k {
                program.emit_insn(Insn::Column {
                    cursor_id: state_cursor_id,
                    column: i,
                    dest: old_out_start + i,
                    default: None,
                });
            }
        } else {
            program.emit_insn(Insn::Copy {
                src_reg: key_start,
                dst_reg: old_out_start,
                extra_amount: k.saturating_sub(1),
            });
        }
        for i in 0..prefix_len {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: k + i,
                dest: cnt_start + i,
                default: None,
            });
        }
        emit_visibility_fold(
            program,
            operators,
            prefix_len,
            cnt_start,
            zero_reg,
            p_scratch_reg,
            v_old_reg,
        );
        program.emit_insn(Insn::Add {
            lhs: w_reg,
            rhs: cnt_start + b,
            dest: cnt_start + b,
        });

        program.preassign_label_to_next_insn(counted_label);
        program.emit_insn(Insn::Lt {
            lhs: cnt_start + b,
            rhs: zero_reg,
            target_pc: corrupt_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        // Every count zero: the content row is gone from all branches (and
        // the fold below reads all-absent as invisible).
        for i in 0..prefix_len {
            program.emit_insn(Insn::If {
                reg: cnt_start + i,
                target_pc: not_all_zero_label,
                jump_if_null: false,
            });
        }
        program.emit_insn(Insn::Copy {
            src_reg: old_out_start,
            dst_reg: index_key_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::Copy {
            src_reg: c_srid_reg,
            dst_reg: index_key_start + k,
            extra_amount: 0,
        });
        program.emit_insn(Insn::IdxDelete {
            start_reg: index_key_start,
            num_regs: k + 1,
            cursor_id: state_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: state_cursor_id,
            table_name: state_table_name.clone(),
            is_part_of_update: true,
        });
        program.emit_int(0, v_new_reg);
        program.emit_insn(Insn::Goto {
            target_pc: vnew_label,
        });

        program.preassign_label_to_next_insn(not_all_zero_label);
        emit_visibility_fold(
            program,
            operators,
            prefix_len,
            cnt_start,
            zero_reg,
            p_scratch_reg,
            v_new_reg,
        );
        if let Some(members) = &members {
            // A shown class shows its smallest member; a class that is not
            // shown may have none and keeps the values it had.
            let keep_values_label = program.allocate_label();
            let values_chosen_label = program.allocate_label();
            program.emit_insn(Insn::IfNot {
                reg: v_new_reg,
                target_pc: keep_values_label,
                jump_if_null: false,
            });
            emit_first_member(
                program,
                members,
                key_start,
                k,
                0,
                k,
                out_start,
                corrupt_label,
            );
            program.emit_insn(Insn::Goto {
                target_pc: values_chosen_label,
            });
            program.preassign_label_to_next_insn(keep_values_label);
            program.emit_insn(Insn::Copy {
                src_reg: old_out_start,
                dst_reg: out_start,
                extra_amount: k.saturating_sub(1),
            });
            program.preassign_label_to_next_insn(values_chosen_label);
            // Move the index entry when the shown values change.
            let same_values_label = program.allocate_label();
            emit_exact_value_record(program, old_out_start, k, old_exact_reg);
            emit_exact_value_record(program, out_start, k, new_exact_reg);
            program.emit_insn(Insn::Eq {
                lhs: old_exact_reg,
                rhs: new_exact_reg,
                target_pc: same_values_label,
                flags: CmpInsFlags::default(),
                collation: CollationSeq::for_internal_values(),
            });
            program.emit_insn(Insn::Copy {
                src_reg: old_out_start,
                dst_reg: index_key_start,
                extra_amount: k - 1,
            });
            program.emit_insn(Insn::Copy {
                src_reg: c_srid_reg,
                dst_reg: index_key_start + k,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: index_key_start,
                num_regs: k + 1,
                cursor_id: state_index_cursor_id,
                raise_error_if_no_matching_entry: true,
            });
            program.emit_insn(Insn::Copy {
                src_reg: out_start,
                dst_reg: index_key_start,
                extra_amount: k - 1,
            });
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(index_key_start),
                count: to_u32(k + 1),
                dest_reg: to_u32(c_index_record_reg),
                index_name: Some(state_index.name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: state_index_cursor_id,
                record_reg: c_index_record_reg,
                unpacked_start: Some(index_key_start),
                unpacked_count: Some(to_u32(k + 1)),
                flags: IdxInsertFlags::new(),
            });
            program.preassign_label_to_next_insn(same_values_label);
        }
        program.emit_insn(Insn::Copy {
            src_reg: out_start,
            dst_reg: state_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(state_rec_start),
            count: to_u32(k + prefix_len),
            dest_reg: to_u32(c_state_record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: state_cursor_id,
            key_reg: c_srid_reg,
            record_reg: c_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: state_table_name.clone(),
        });

        // Publish the change of the view row: retract the old shown values
        // if they were shown, add the new ones if they are. With one exact
        // value per class only a visibility change changes the output.
        program.preassign_label_to_next_insn(vnew_label);
        let publish_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: v_old_reg,
            rhs: v_new_reg,
            target_pc: publish_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        program.emit_insn(Insn::IfNot {
            reg: v_new_reg,
            target_pc: ret_label,
            jump_if_null: false,
        });
        if members.is_some() {
            // Shown before and after: publish only if the values changed.
            program.emit_insn(Insn::Eq {
                lhs: old_exact_reg,
                rhs: new_exact_reg,
                target_pc: ret_label,
                flags: CmpInsFlags::default(),
                collation: CollationSeq::for_internal_values(),
            });
        } else {
            program.emit_insn(Insn::Goto {
                target_pc: ret_label,
            });
        }
        program.preassign_label_to_next_insn(publish_label);
        emit_split_view_rowid(
            program,
            has_append,
            c_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            false,
        );
        program.emit_insn(Insn::IfNot {
            reg: v_old_reg,
            target_pc: insert_label,
            jump_if_null: false,
        });
        program.emit_int(-1, out_weight_reg);
        emit_operator_rowid_delta(
            program,
            output,
            view_rowid_reg,
            old_out_start,
            out_weight_reg,
        );
        program.preassign_label_to_next_insn(insert_label);
        program.emit_insn(Insn::IfNot {
            reg: v_new_reg,
            target_pc: ret_label,
            jump_if_null: false,
        });
        program.emit_int(1, out_weight_reg);
        emit_operator_rowid_delta(program, output, view_rowid_reg, out_start, out_weight_reg);
        program.preassign_label_to_next_insn(ret_label);
        program.emit_insn(Insn::Return {
            return_reg: c_return_reg,
            can_fallthrough: false,
        });
    }

    // Append-merge subroutine: add w to the multiplicity of (branch, rid)
    // and pass w on under the row's view rowid. The input is consolidated,
    // so it only retracts rows that are present and a multiplicity never
    // goes negative; the view row's weight is the multiplicity, and its
    // change is w.
    let append_merge_label = append_cursors.map(|(append_cursor_id, append_index_cursor_id)| {
        let am_label = program.allocate_label();
        program.preassign_label_to_next_insn(am_label);

        let m_found_label = program.allocate_label();
        let m_update_label = program.allocate_label();
        let m_emit_label = program.allocate_label();
        program.emit_insn(Insn::Found {
            cursor_id: append_index_cursor_id,
            target_pc: m_found_label,
            record_reg: akey_start,
            num_regs: 2,
        });
        program.emit_insn(Insn::Lt {
            lhs: w_reg,
            rhs: zero_reg,
            target_pc: corrupt_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        program.emit_insn(Insn::NewRowid {
            cursor: append_cursor_id,
            rowid_reg: a_srid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
        program.emit_insn(Insn::Copy {
            src_reg: akey_start,
            dst_reg: a_rec_start,
            extra_amount: 1,
        });
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: a_rec_start + 2,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(a_rec_start),
            count: 3,
            dest_reg: to_u32(a_state_record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: append_cursor_id,
            key_reg: a_srid_reg,
            record_reg: a_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: append_table_name.clone(),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(akey_start),
            count: 3,
            dest_reg: to_u32(a_index_record_reg),
            index_name: Some(append_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: append_index_cursor_id,
            record_reg: a_index_record_reg,
            unpacked_start: Some(akey_start),
            unpacked_count: Some(3),
            flags: IdxInsertFlags::new(),
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_emit_label,
        });

        program.preassign_label_to_next_insn(m_found_label);
        program.emit_insn(Insn::IdxRowId {
            cursor_id: append_index_cursor_id,
            dest: a_srid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: append_cursor_id,
            src_reg: a_srid_reg,
            target_pc: corrupt_label,
        });
        program.emit_insn(Insn::Column {
            cursor_id: append_cursor_id,
            column: 2,
            dest: cur_w_reg,
            default: None,
        });
        program.emit_insn(Insn::Add {
            lhs: w_reg,
            rhs: cur_w_reg,
            dest: new_w_reg,
        });
        program.emit_insn(Insn::Lt {
            lhs: new_w_reg,
            rhs: zero_reg,
            target_pc: corrupt_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        program.emit_insn(Insn::Ne {
            lhs: new_w_reg,
            rhs: zero_reg,
            target_pc: m_update_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        // Multiplicity reached zero: the row is gone.
        program.emit_insn(Insn::IdxDelete {
            start_reg: akey_start,
            num_regs: 3,
            cursor_id: append_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: append_cursor_id,
            table_name: append_table_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_emit_label,
        });

        program.preassign_label_to_next_insn(m_update_label);
        program.emit_insn(Insn::Copy {
            src_reg: akey_start,
            dst_reg: a_rec_start,
            extra_amount: 1,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new_w_reg,
            dst_reg: a_rec_start + 2,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(a_rec_start),
            count: 3,
            dest_reg: to_u32(a_state_record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: append_cursor_id,
            key_reg: a_srid_reg,
            record_reg: a_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: append_table_name.clone(),
        });

        program.preassign_label_to_next_insn(m_emit_label);
        emit_split_view_rowid(
            program,
            has_append,
            a_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            true,
        );
        emit_operator_rowid_delta(program, output, view_rowid_reg, out_start, w_reg);
        program.emit_insn(Insn::Return {
            return_reg: a_return_reg,
            can_fallthrough: false,
        });
        am_label
    });

    program.preassign_label_to_next_insn(main_start_label);

    for (branch_idx, stream) in branch_streams.iter().enumerate() {
        let branch_done_label = program.allocate_label();
        let loop_label = program.allocate_label();
        let next_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: stream.cursor_id,
            pc_if_empty: branch_done_label,
        });
        program.preassign_label_to_next_insn(loop_label);

        for column in 0..num_view_columns {
            program.emit_insn(Insn::Column {
                cursor_id: stream.cursor_id,
                column: stream.value_start + column,
                dest: out_start + column,
                default: None,
            });
        }
        program.emit_insn(Insn::Column {
            cursor_id: stream.cursor_id,
            column: stream.weight_column,
            dest: w_reg,
            default: None,
        });
        // Row identity: output content for dedup-prefix branches,
        // (branch, source identity record) for trailing UNION ALL branches.
        if branch_idx < prefix_len {
            program.emit_insn(Insn::Copy {
                src_reg: out_start,
                dst_reg: key_start,
                extra_amount: k.saturating_sub(1),
            });
            program.emit_insn(Insn::Gosub {
                target_pc: content_merge_labels[branch_idx],
                return_reg: c_return_reg,
            });
        } else {
            turso_assert!(
                stream.identity_width() > 0,
                "UNION ALL suffix requires a stable source identity"
            );
            program.emit_int(branch_idx as i64, akey_start);
            let identity_start = program.alloc_registers(stream.identity_width());
            for column in 0..stream.identity_width() {
                program.emit_insn(Insn::Column {
                    cursor_id: stream.cursor_id,
                    column,
                    dest: identity_start + column,
                    default: None,
                });
            }
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(identity_start),
                count: to_u32(stream.identity_width()),
                dest_reg: to_u32(akey_start + 1),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::Gosub {
                target_pc: append_merge_label
                    .expect("UNION ALL suffix implies the append-merge subroutine"),
                return_reg: a_return_reg,
            });
        }

        program.preassign_label_to_next_insn(next_label);
        program.emit_insn(Insn::Next {
            cursor_id: stream.cursor_id,
            pc_if_next: loop_label,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(branch_done_label);
    }

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
    Ok(())
}
