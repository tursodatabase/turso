//! The exact values inside one class of a collated key.
//!
//! Stateful operators group values into classes by SQL equality under a
//! collation: under NOCASE, 'a' and 'A' are one GROUP BY group, one DISTINCT
//! row, one MIN/MAX candidate, one compound-query row. SQL then leaves open
//! which member of the class a result shows; a fresh query shows whichever
//! row its plan meets first, which depends on the plan (an index on the
//! column changes it).
//!
//! A maintained view cannot follow a plan, and it must still show a value
//! that is in the class right now. So an operator whose key is ambiguous (see
//! [`dag::KeyColumn::ambiguous`]) keeps every exact member of each class with
//! its count, and shows the smallest member under BINARY, with an integer
//! before an equal real. The rule depends only on the current members, never
//! on the order rows arrived in or on any plan.
//!
//! Members are keyed by their exact value, so the integer 2 and the real 2.0,
//! or 'a' and 'A', are two members with their own counts, as in a z-set.
//!
//! [`dag::KeyColumn::ambiguous`]: crate::incremental::dag::KeyColumn::ambiguous

use crate::function::{Func, FuncCtx, ScalarFunc};
use crate::namespace::NamespaceId;
use crate::schema::Schema;
use crate::translate::collate::CollationSeq;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral,
};
use crate::vdbe::BranchOffset;
use crate::{LimboError, Result};

/// An open member table: every column but the last (`mult`) is its primary
/// key, which the table's index orders.
pub(super) struct MemberTable {
    table_name: String,
    table_cursor_id: usize,
    index_cursor_id: usize,
    index_name: String,
    /// Number of primary-key columns.
    pub(super) key_width: usize,
}

impl MemberTable {
    /// Open `table_name` and its primary-key index for writing.
    pub(super) fn open(
        program: &mut ProgramBuilder,
        schema: &Schema,
        view_name: &str,
        table_name: &str,
    ) -> Result<Self> {
        let table = schema
            .get_btree_table(NamespaceId::ROOT, table_name)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "member table {table_name} of materialized view {view_name} not found"
                ))
            })?;
        let index = schema
            .get_indices(NamespaceId::ROOT, table_name)
            .next()
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "member table {table_name} of materialized view {view_name} has no index"
                ))
            })?;
        let key_width = table.columns().len() - 1;
        if index.columns.len() != key_width {
            return Err(LimboError::InternalError(format!(
                "member table {table_name} must be keyed by every column but its count"
            )));
        }
        let table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: table_cursor_id,
            root_page: RegisterOrLiteral::Literal(table.root_page),
            db: 0,
        });
        let index_cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor_id,
            root_page: RegisterOrLiteral::Literal(index.root_page),
            db: 0,
        });
        Ok(Self {
            table_name: table_name.to_string(),
            table_cursor_id,
            index_cursor_id,
            index_name: index.name.clone(),
            key_width,
        })
    }

    pub(super) fn index_cursor_id(&self) -> usize {
        self.index_cursor_id
    }
}

/// Write the record of the `width` values at `start` into `dest`. Records
/// encode each value with its exact type, so they tell the integer 2 from
/// the real 2.0 and 'a' from 'A', and equal records mean identical values.
pub(super) fn emit_exact_value_record(
    program: &mut ProgramBuilder,
    start: usize,
    width: usize,
    dest: usize,
) {
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(start),
        count: to_u32(width),
        dest_reg: to_u32(dest),
        index_name: None,
        affinity_str: None,
    });
}

/// Number of member columns that follow the class columns of `width` values:
/// `raw.., type.., exact` (see `plan::member_column_names`).
pub(super) const fn member_columns_width(width: usize) -> usize {
    2 * width + 1
}

/// Fill the member columns `(raw.., type.., exact)` of the `width` values at
/// `values_start` into `dest..dest + member_columns_width(width)`. `raw`
/// holds the values, compared under BINARY. `type` holds their `typeof()`,
/// so that of an integer and an equal real ('integer' < 'real') the integer
/// comes first. `exact` tells apart the rest: the reals 0.0 and -0.0.
pub(super) fn emit_member_columns(
    program: &mut ProgramBuilder,
    values_start: usize,
    width: usize,
    dest: usize,
) {
    program.emit_insn(Insn::Copy {
        src_reg: values_start,
        dst_reg: dest,
        extra_amount: width - 1,
    });
    for column in 0..width {
        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: values_start + column,
            dest: dest + width + column,
            func: FuncCtx {
                func: Func::Scalar(ScalarFunc::Typeof),
                arg_count: 1,
                collation: CollationSeq::BINARY,
            },
        });
    }
    emit_exact_value_record(program, values_start, width, dest + 2 * width);
}

/// Fill the class-member key `(class.., raw.., type.., exact)` of the `width`
/// values at `values_start` into `key_start..`: the class columns hold the
/// values themselves, compared under the key's collations.
pub(super) fn emit_member_key(
    program: &mut ProgramBuilder,
    values_start: usize,
    width: usize,
    key_start: usize,
) {
    program.emit_insn(Insn::Copy {
        src_reg: values_start,
        dst_reg: key_start,
        extra_amount: width - 1,
    });
    emit_member_columns(program, values_start, width, key_start + width);
}

/// Add the weight in `weight_reg` to the count of the member whose primary
/// key is in `key_start..key_start + table.key_width`, creating or deleting
/// its row. `old_count_reg` and `new_count_reg` receive the count before and
/// after (0 for no row). A count below zero means the input retracted a
/// member the state does not hold: jump to `corrupt_label`.
pub(super) fn emit_add_to_member(
    program: &mut ProgramBuilder,
    table: &MemberTable,
    key_start: usize,
    weight_reg: usize,
    old_count_reg: usize,
    new_count_reg: usize,
    corrupt_label: BranchOffset,
) {
    let key_width = table.key_width;
    // [key.., count or rowid], contiguous for MakeRecord and the index.
    let row_start = program.alloc_registers(key_width + 1);
    let last_reg = row_start + key_width;
    let rowid_reg = program.alloc_register();
    let zero_reg = program.alloc_register();
    let is_new_reg = program.alloc_register();
    let record_reg = program.alloc_register();
    let prev_rowid_reg = program.alloc_register();
    let found_label = program.allocate_label();
    let write_label = program.allocate_label();
    let done_label = program.allocate_label();

    program.emit_int(0, zero_reg);
    program.emit_insn(Insn::Copy {
        src_reg: key_start,
        dst_reg: row_start,
        extra_amount: key_width - 1,
    });
    program.emit_insn(Insn::Found {
        cursor_id: table.index_cursor_id,
        target_pc: found_label,
        record_reg: row_start,
        num_regs: key_width,
    });
    program.emit_int(0, old_count_reg);
    program.emit_int(1, is_new_reg);
    program.emit_insn(Insn::NewRowid {
        cursor: table.table_cursor_id,
        rowid_reg,
        prev_largest_reg: prev_rowid_reg,
    });
    program.emit_insn(Insn::Goto {
        target_pc: write_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_int(0, is_new_reg);
    program.emit_insn(Insn::IdxRowId {
        cursor_id: table.index_cursor_id,
        dest: rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table.table_cursor_id,
        src_reg: rowid_reg,
        target_pc: corrupt_label,
    });
    program.emit_insn(Insn::Column {
        cursor_id: table.table_cursor_id,
        column: key_width,
        dest: old_count_reg,
        default: None,
    });

    program.preassign_label_to_next_insn(write_label);
    program.emit_insn(Insn::Add {
        lhs: old_count_reg,
        rhs: weight_reg,
        dest: new_count_reg,
    });
    program.emit_insn(Insn::Lt {
        lhs: new_count_reg,
        rhs: zero_reg,
        target_pc: corrupt_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    let keep_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: new_count_reg,
        rhs: zero_reg,
        target_pc: keep_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    // The count reached zero. A new row never gets here: a consolidated
    // delta has no zero weights.
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: last_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::IdxDelete {
        start_reg: row_start,
        num_regs: key_width + 1,
        cursor_id: table.index_cursor_id,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: table.table_cursor_id,
        table_name: table.table_name.clone(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Goto {
        target_pc: done_label,
    });

    program.preassign_label_to_next_insn(keep_label);
    program.emit_insn(Insn::Copy {
        src_reg: new_count_reg,
        dst_reg: last_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(row_start),
        count: to_u32(key_width + 1),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: table.table_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: table.table_name.clone(),
    });
    // An existing row keeps its index entry.
    program.emit_insn(Insn::IfNot {
        reg: is_new_reg,
        target_pc: done_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: last_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(row_start),
        count: to_u32(key_width + 1),
        dest_reg: to_u32(record_reg),
        index_name: Some(table.index_name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: table.index_cursor_id,
        record_reg,
        unpacked_start: Some(row_start),
        unpacked_count: Some(to_u32(key_width + 1)),
        flags: IdxInsertFlags::new(),
    });
    program.preassign_label_to_next_insn(done_label);
}

/// Position on the first member whose key starts with the `prefix_width`
/// values at `prefix_start`, and read its key columns
/// `first_column..first_column + count` into registers from `dest`. With a
/// whole class as the prefix, that is the member the class shows: the
/// smallest under BINARY. Jump to `none_label` when no member has the prefix.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_first_member(
    program: &mut ProgramBuilder,
    table: &MemberTable,
    prefix_start: usize,
    prefix_width: usize,
    first_column: usize,
    count: usize,
    dest: usize,
    none_label: BranchOffset,
) {
    program.emit_insn(Insn::SeekGE {
        is_index: true,
        cursor_id: table.index_cursor_id,
        start_reg: prefix_start,
        num_regs: prefix_width,
        target_pc: none_label,
        eq_only: false,
        null_matching_mask: Default::default(),
    });
    program.emit_insn(Insn::IdxGT {
        cursor_id: table.index_cursor_id,
        start_reg: prefix_start,
        num_regs: prefix_width,
        target_pc: none_label,
    });
    for column in 0..count {
        program.emit_insn(Insn::Column {
            cursor_id: table.index_cursor_id,
            column: first_column + column,
            dest: dest + column,
            default: None,
        });
    }
}
