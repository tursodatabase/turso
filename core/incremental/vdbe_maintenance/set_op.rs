use super::plan::{NodeOutputContract, OperatorStateDef};
use super::stream::{
    emit_operator_rowid_delta, open_ephemeral_delta, DeltaIdentity, EphemeralDelta,
};
use super::DeltaSource;
use crate::incremental::dag;
use crate::schema::Schema;
use crate::translate::collate::CollationSeq;
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::{LimboError, Result};
use turso_parser::ast;

/// Emit one compound-query node from its already-materialized branch deltas.
///
/// Pure UNION ALL is a stateless identity-namespacing map. Any deduplicating
/// prefix owns the content and append integrals declared for this node.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_set_op_to_ephemeral(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    inputs: &[EphemeralDelta],
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    key_collations: &[CollationSeq],
    output_contract: &NodeOutputContract,
    operator_state: &OperatorStateDef,
    schema: &Schema,
) -> Result<DeltaSource> {
    if prefix_len == 0 {
        if operators
            .iter()
            .any(|operator| *operator != ast::CompoundOperator::UnionAll)
        {
            return Err(LimboError::InternalError(
                "stateless set-op has a deduplicating operator".to_string(),
            ));
        }
        return emit_union_all_to_ephemeral(program, view_name, node_id, inputs, output_contract);
    }

    let requires_positive_first = inputs.iter().any(|channel| channel.requires_positive_first);
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_set_op_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
        requires_positive_first,
    );
    emit_deduplicating_set_op(
        program,
        view_name,
        inputs,
        operators,
        prefix_len,
        key_collations,
        &output,
        operator_state,
        schema,
    )?;
    Ok(DeltaSource::Ephemeral(output))
}

fn emit_union_all_to_ephemeral(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    inputs: &[EphemeralDelta],
    output_contract: &NodeOutputContract,
) -> Result<DeltaSource> {
    let requires_positive_first = inputs.iter().any(|input| input.requires_positive_first);
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_union_all_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
        requires_positive_first,
    );
    for (branch, input) in inputs.iter().enumerate() {
        append_union_all_branch(program, branch, input, &output)?;
    }
    Ok(DeltaSource::Ephemeral(output))
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
        || output.value_start != 2
        || output.binding_rowid_columns.iter().any(Option::is_some)
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
        start_reg: source_identity_start as u16,
        count: input.identity_width() as u16,
        dest_reg: (record_start + 1) as u16,
        index_name: None,
        affinity_str: None,
    });
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
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
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
        collation: None,
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
    branch_streams: &[EphemeralDelta],
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    key_collations: &[CollationSeq],
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
    // With a collated dedup key, values that compare equal can differ in
    // bytes; the stored key is the row's first-seen representative (see the
    // matching reload in the group-aggregate program). The output columns
    // ARE the key, so the reload refreshes both.
    let reload_stored_key = key_collations
        .iter()
        .any(|c| !matches!(c, CollationSeq::Binary | CollationSeq::Unset));

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
    // Trailing UNION ALL rows preserve source identity in the auxiliary table.
    let (append_table_name, append_table, append_index) = if materializes_append {
        let name = operator_state.auxiliary_table_name()?.to_string();
        let table = schema.get_btree_table(&name).ok_or_else(|| {
            LimboError::InternalError(format!(
                "append table {name} of materialized view {view_name} not found"
            ))
        })?;
        let index = schema.get_indices(&name).next().cloned().ok_or_else(|| {
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
    // Register layout, shared by all branches.
    let zero_reg = program.alloc_register();
    let two_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    // [outputs.., weight], contiguous for MakeRecord.
    let out_start = program.alloc_registers(num_view_columns + 1);
    let out_weight_reg = out_start + num_view_columns;
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
    // transitions. Negative counts are recorded for uniformity with joins;
    // the per-branch capture-order streams cannot dip below zero on their
    // own, and a negative count reads as "absent" in the fold.
    let mut content_merge_labels = Vec::with_capacity(prefix_len);
    for b in 0..prefix_len {
        let (state_cursor_id, state_index_cursor_id) = content_cursors;
        let cm_label = program.allocate_label();
        program.preassign_label_to_next_insn(cm_label);
        content_merge_labels.push(cm_label);

        let found_label = program.allocate_label();
        let vnew_label = program.allocate_label();
        let not_all_zero_label = program.allocate_label();
        let insert_label = program.allocate_label();
        let ret_label = program.allocate_label();

        program.emit_insn(Insn::Found {
            cursor_id: state_index_cursor_id,
            target_pc: found_label,
            record_reg: key_start,
            num_regs: k,
        });
        // Fresh content row: this branch's count is the weight, the rest 0.
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
        program.emit_insn(Insn::Copy {
            src_reg: key_start,
            dst_reg: state_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: state_rec_start as u16,
            count: (k + prefix_len) as u16,
            dest_reg: c_state_record_reg as u16,
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
        program.emit_insn(Insn::MakeRecord {
            start_reg: key_start as u16,
            count: (k + 1) as u16,
            dest_reg: c_index_record_reg as u16,
            index_name: Some(state_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: state_index_cursor_id,
            record_reg: c_index_record_reg,
            unpacked_start: Some(key_start),
            unpacked_count: Some((k + 1) as u16),
            flags: IdxInsertFlags::new(),
        });
        program.emit_int(0, v_old_reg);
        program.emit_insn(Insn::Goto {
            target_pc: vnew_label,
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
        if reload_stored_key {
            for i in 0..k {
                program.emit_insn(Insn::Column {
                    cursor_id: state_cursor_id,
                    column: i,
                    dest: key_start + i,
                    default: None,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: key_start,
                dst_reg: out_start,
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
        // Every count zero: the content row is gone from all branches (and
        // the fold below reads all-absent as invisible).
        for i in 0..prefix_len {
            program.emit_insn(Insn::If {
                reg: cnt_start + i,
                target_pc: not_all_zero_label,
                jump_if_null: false,
            });
        }
        program.emit_insn(Insn::IdxDelete {
            start_reg: key_start,
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
            target_pc: vnew_label,
        });

        program.preassign_label_to_next_insn(not_all_zero_label);
        program.emit_insn(Insn::Copy {
            src_reg: key_start,
            dst_reg: state_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: state_rec_start as u16,
            count: (k + prefix_len) as u16,
            dest_reg: c_state_record_reg as u16,
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

        program.preassign_label_to_next_insn(vnew_label);
        emit_visibility_fold(
            program,
            operators,
            prefix_len,
            cnt_start,
            zero_reg,
            p_scratch_reg,
            v_new_reg,
        );
        // Publish only visibility transitions. A row that stays visible keeps
        // its stored first-seen content, so there is nothing to emit.
        program.emit_insn(Insn::Eq {
            lhs: v_old_reg,
            rhs: v_new_reg,
            target_pc: ret_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: v_new_reg,
            target_pc: insert_label,
            jump_if_null: false,
        });
        // Visible before, not now: publish one retraction.
        program.emit_int(-1, out_weight_reg);
        emit_split_view_rowid(
            program,
            has_append,
            c_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            false,
        );
        emit_operator_rowid_delta(program, output, view_rowid_reg, out_start, out_weight_reg);
        program.emit_insn(Insn::Goto {
            target_pc: ret_label,
        });

        program.preassign_label_to_next_insn(insert_label);
        program.emit_int(1, out_weight_reg);
        emit_split_view_rowid(
            program,
            has_append,
            c_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            false,
        );
        emit_operator_rowid_delta(program, output, view_rowid_reg, out_start, out_weight_reg);
        program.preassign_label_to_next_insn(ret_label);
        program.emit_insn(Insn::Return {
            return_reg: c_return_reg,
            can_fallthrough: false,
        });
    }

    // Append-merge subroutine: apply ((branch, rid), outputs, w) with a
    // single signed multiplicity, the view row existing while it is
    // positive. Signed multiplicities are kept for uniformity with joins;
    // the per-branch capture-order streams cannot dip below zero on their
    // own.
    let append_merge_label = append_cursors.map(|(append_cursor_id, append_index_cursor_id)| {
        let am_label = program.allocate_label();
        program.preassign_label_to_next_insn(am_label);

        let m_found_label = program.allocate_label();
        let m_update_label = program.allocate_label();
        let m_visible_label = program.allocate_label();
        let m_visible_delta_label = program.allocate_label();
        let m_hidden_label = program.allocate_label();
        let m_retract_label = program.allocate_label();
        let m_write_label = program.allocate_label();
        let m_ret_label = program.allocate_label();
        program.emit_insn(Insn::Found {
            cursor_id: append_index_cursor_id,
            target_pc: m_found_label,
            record_reg: akey_start,
            num_regs: 2,
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
            start_reg: a_rec_start as u16,
            count: 3,
            dest_reg: a_state_record_reg as u16,
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
            start_reg: akey_start as u16,
            count: 3,
            dest_reg: a_index_record_reg as u16,
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
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: out_weight_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Gt {
            lhs: w_reg,
            rhs: zero_reg,
            target_pc: m_write_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_ret_label,
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
        program.emit_insn(Insn::Ne {
            lhs: new_w_reg,
            rhs: zero_reg,
            target_pc: m_update_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        // Multiplicity reached zero: the row is gone. Its view row exists
        // only if the multiplicity was positive before this contribution.
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
            target_pc: m_hidden_label,
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
            start_reg: a_rec_start as u16,
            count: 3,
            dest_reg: a_state_record_reg as u16,
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
        program.emit_insn(Insn::Gt {
            lhs: new_w_reg,
            rhs: zero_reg,
            target_pc: m_visible_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_hidden_label,
        });

        program.preassign_label_to_next_insn(m_visible_label);
        // If the row was already visible, only its multiplicity delta is new
        // downstream state. If it crosses from non-positive to positive,
        // publish the complete newly-visible multiplicity; previously
        // suppressed negative deltas must not escape.
        program.emit_insn(Insn::Gt {
            lhs: cur_w_reg,
            rhs: zero_reg,
            target_pc: m_visible_delta_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new_w_reg,
            dst_reg: out_weight_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_write_label,
        });
        program.preassign_label_to_next_insn(m_visible_delta_label);
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: out_weight_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_write_label,
        });

        // A non-positive new multiplicity is invisible. Retract exactly the
        // previously visible multiplicity, if any; retain negative state so a
        // later positive contribution can converge without leaking a
        // retraction for a row the downstream consumer has never seen.
        program.preassign_label_to_next_insn(m_hidden_label);
        program.emit_insn(Insn::Gt {
            lhs: cur_w_reg,
            rhs: zero_reg,
            target_pc: m_retract_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_ret_label,
        });
        program.preassign_label_to_next_insn(m_retract_label);
        emit_split_view_rowid(
            program,
            has_append,
            a_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            true,
        );
        program.emit_insn(Insn::Subtract {
            lhs: zero_reg,
            rhs: cur_w_reg,
            dest: out_weight_reg,
        });
        emit_operator_rowid_delta(program, output, view_rowid_reg, out_start, out_weight_reg);
        program.emit_insn(Insn::Goto {
            target_pc: m_ret_label,
        });

        program.preassign_label_to_next_insn(m_write_label);
        // Fresh positive rows arrive here with `w_reg`; updated rows have
        // already selected either their delta or their newly-visible weight.
        program.emit_insn(Insn::IfNot {
            reg: out_weight_reg,
            target_pc: m_ret_label,
            jump_if_null: true,
        });
        emit_split_view_rowid(
            program,
            has_append,
            a_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            true,
        );
        emit_operator_rowid_delta(program, output, view_rowid_reg, out_start, out_weight_reg);
        program.preassign_label_to_next_insn(m_ret_label);
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
                start_reg: identity_start as u16,
                count: stream.identity_width() as u16,
                dest_reg: (akey_start + 1) as u16,
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
        });
        program.preassign_label_to_next_insn(branch_done_label);
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
    Ok(())
}
