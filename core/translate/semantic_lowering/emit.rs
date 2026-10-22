//! Small bytecode helpers for HIR DML lowering: foreign-key probes, cursor
//! opening, constraint error text and statement journal flags.
//!
//! They are shared by the INSERT, UPDATE and DELETE lowering and by the
//! foreign-key and trigger modules.

use std::num::NonZeroUsize;

use turso_parser::ast::ResolveType;

use crate::{
    error::SQLITE_CONSTRAINT_FOREIGNKEY,
    schema::{BTreeTable, ForeignKey, Index},
    sync::Arc,
    translate::collate::CollationSeq,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
        BranchOffset,
    },
    Result,
};

/// Open a read cursor on an index and return its cursor id.
pub(super) fn open_read_index(
    program: &mut ProgramBuilder,
    index: &Arc<Index>,
    db: usize,
) -> usize {
    let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: cursor,
        root_page: index.root_page,
        db,
    });
    cursor
}

/// Open a read cursor on a table and return its cursor id.
pub(super) fn open_read_table(
    program: &mut ProgramBuilder,
    table: &Arc<BTreeTable>,
    db: usize,
) -> usize {
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: cursor,
        root_page: table.root_page,
        db,
    });
    cursor
}

/// Copy `len` registers starting at `start` to new registers and apply the
/// affinities of `index`'s columns in `table`. Returns the first new register.
pub(super) fn copy_with_affinity(
    program: &mut ProgramBuilder,
    start: usize,
    len: usize,
    index: &Index,
    table: &BTreeTable,
) -> usize {
    let destination = program.alloc_registers(len);
    for i in 0..len {
        program.emit_insn(Insn::Copy {
            src_reg: start + i,
            dst_reg: destination + i,
            extra_amount: 0,
        });
    }
    if let Some(count) = NonZeroUsize::new(len) {
        let affinities = index
            .columns
            .iter()
            .map(|column| table.columns()[column.pos_in_table].affinity().aff_mask())
            .collect();
        program.emit_insn(Insn::Affinity {
            start_reg: destination,
            count,
            affinities,
        });
    }
    destination
}

/// Look up the key in `record_reg` in an index with `Found`, run `on_found`
/// or `on_not_found`, then close the cursor.
pub(super) fn index_probe<F, G>(
    program: &mut ProgramBuilder,
    cursor: usize,
    record_reg: usize,
    num_regs: usize,
    mut on_found: F,
    mut on_not_found: G,
) -> Result<()>
where
    F: FnMut(&mut ProgramBuilder) -> Result<()>,
    G: FnMut(&mut ProgramBuilder) -> Result<()>,
{
    let found = program.allocate_label();
    let done = program.allocate_label();
    program.emit_insn(Insn::Found {
        cursor_id: cursor,
        target_pc: found,
        record_reg,
        num_regs,
    });
    on_not_found(program)?;
    program.emit_insn(Insn::Goto { target_pc: done });
    program.preassign_label_to_next_insn(found);
    on_found(program)?;
    program.preassign_label_to_next_insn(done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

/// Run `on_match` for every index entry whose leading columns equal the key
/// at `key_start`, then close the cursor. With `skip_rowid`, the entry of
/// that row is skipped.
pub(super) fn index_scan_match_any<F>(
    program: &mut ProgramBuilder,
    cursor: usize,
    key_start: usize,
    num_regs: usize,
    skip_rowid: Option<usize>,
    mut on_match: F,
) -> Result<()>
where
    F: FnMut(&mut ProgramBuilder) -> Result<()>,
{
    let done = program.allocate_label();
    program.emit_insn(Insn::SeekGE {
        is_index: true,
        cursor_id: cursor,
        start_reg: key_start,
        num_regs,
        target_pc: done,
        eq_only: true,
        null_matching_mask: Default::default(),
    });
    let loop_top = program.allocate_label();
    program.preassign_label_to_next_insn(loop_top);
    program.emit_insn(Insn::IdxGT {
        cursor_id: cursor,
        start_reg: key_start,
        num_regs,
        target_pc: done,
    });
    let next_row = program.allocate_label();
    if let Some(skip_rowid) = skip_rowid {
        let rowid = program.alloc_register();
        program.emit_insn(Insn::IdxRowId {
            cursor_id: cursor,
            dest: rowid,
        });
        program.emit_insn(Insn::Eq {
            lhs: rowid,
            rhs: skip_rowid,
            target_pc: next_row,
            flags: CmpInsFlags::default(),
            collation: None,
        });
    }
    on_match(program)?;
    program.preassign_label_to_next_insn(next_row);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        is_index: program.cursor_is_index(cursor),
        pc_if_next: loop_top,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

/// Jump to `target_pc` when any of `count` registers from `start` is NULL.
pub(super) fn emit_skip_if_any_null(
    program: &mut ProgramBuilder,
    start: usize,
    count: usize,
    target_pc: BranchOffset,
) {
    for i in 0..count {
        program.emit_insn(Insn::IsNull {
            reg: start + i,
            target_pc,
        });
    }
}

/// Scan `table` and run `on_match` for every row whose columns at
/// `positions` all equal the non-NULL key at `key_start`. Rows with a NULL
/// key column are skipped, and so is the row `skip_rowid` when given.
pub(super) fn table_scan_match_any<F>(
    program: &mut ProgramBuilder,
    table: &Arc<BTreeTable>,
    positions: &[usize],
    key_start: usize,
    skip_rowid: Option<usize>,
    database: usize,
    mut on_match: F,
) -> Result<()>
where
    F: FnMut(&mut ProgramBuilder) -> Result<()>,
{
    let cursor = open_read_table(program, table, database);
    let done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: done,
    });
    let loop_top = program.allocate_label();
    program.preassign_label_to_next_insn(loop_top);
    let next_row = program.allocate_label();
    for (i, &position) in positions.iter().enumerate() {
        let value = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: cursor,
            column: position,
            dest: value,
            default: None,
        });
        program.emit_insn(Insn::IsNull {
            reg: value,
            target_pc: next_row,
        });
        let equal = program.allocate_label();
        program.emit_insn(Insn::Eq {
            lhs: value,
            rhs: key_start + i,
            target_pc: equal,
            flags: CmpInsFlags::default(),
            collation: Some(CollationSeq::Binary),
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_row,
        });
        program.preassign_label_to_next_insn(equal);
    }
    if let Some(skip_rowid) = skip_rowid {
        let rowid = program.alloc_register();
        let skip = program.allocate_label();
        program.emit_insn(Insn::RowId {
            cursor_id: cursor,
            dest: rowid,
        });
        program.emit_insn(Insn::Eq {
            lhs: rowid,
            rhs: skip_rowid,
            target_pc: skip,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        on_match(program)?;
        program.preassign_label_to_next_insn(skip);
    } else {
        on_match(program)?;
    }
    program.preassign_label_to_next_insn(next_row);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        is_index: program.cursor_is_index(cursor),
        pc_if_next: loop_top,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

/// Count one foreign-key violation: on the connection for a deferred
/// constraint, on the statement otherwise.
pub(super) fn emit_fk_violation(
    program: &mut ProgramBuilder,
    foreign_key: &ForeignKey,
) -> Result<()> {
    program.emit_insn(Insn::FkCounter {
        increment_value: 1,
        deferred: foreign_key.deferred,
    });
    Ok(())
}

/// Stop the statement with a foreign-key error at once: for RESTRICT, and
/// for a missing parent of a single-row INSERT.
pub(super) fn emit_fk_restrict_halt(program: &mut ProgramBuilder) -> Result<()> {
    program.emit_insn(Insn::Halt {
        err_code: SQLITE_CONSTRAINT_FOREIGNKEY,
        description: "FOREIGN KEY constraint failed".to_string(),
        on_error: None,
        description_reg: None,
    });
    Ok(())
}

/// Take one from the foreign-key violation counter, unless it is already
/// zero, in which case jump to `label`.
pub(super) fn emit_guarded_fk_decrement(
    program: &mut ProgramBuilder,
    label: BranchOffset,
    deferred: bool,
) {
    program.emit_insn(Insn::FkIfZero {
        deferred,
        target_pc: label,
    });
    program.emit_insn(Insn::FkCounter {
        increment_value: -1,
        deferred,
    });
}

/// The UNIQUE error text for `index`, as SQLite writes it: `t.c` for one
/// column, `t.(a, b)` for several.
pub(crate) fn format_unique_violation_desc(table_name: &str, index: &Index) -> String {
    match index.columns.as_slice() {
        [column] => format!("{table_name}.{}", column.name),
        columns => {
            let names = columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("{table_name}.({names})")
        }
    }
}

/// The error text and halt mode for a constraint whose own ON CONFLICT is
/// FAIL or ROLLBACK. The text is formatted here so the VM raises it with
/// that mode. With a statement-level OR clause, the program's own mode
/// applies and the text is left as is.
pub(super) fn halt_desc_and_on_error(
    description: &str,
    effective: ResolveType,
    has_statement_conflict: bool,
) -> (String, Option<ResolveType>) {
    if has_statement_conflict {
        return (description.to_string(), None);
    }
    match effective {
        ResolveType::Fail | ResolveType::Rollback => (
            format!("UNIQUE constraint failed: {description}"),
            Some(effective),
        ),
        _ => (description.to_string(), None),
    }
}

/// Whether any constraint resolves to REPLACE: the statement's OR clause
/// when it has one, otherwise the INTEGER PRIMARY KEY's or an index's own
/// ON CONFLICT.
pub(super) fn any_effective_replace(
    has_statement_conflict: bool,
    statement_conflict: ResolveType,
    rowid_alias_conflict: Option<ResolveType>,
    mut indexes: impl Iterator<Item = Option<ResolveType>>,
) -> bool {
    if has_statement_conflict {
        matches!(statement_conflict, ResolveType::Replace)
    } else {
        rowid_alias_conflict == Some(ResolveType::Replace)
            || indexes.any(|conflict| conflict == Some(ResolveType::Replace))
    }
}

/// Whether a constraint can stop the statement with ABORT, which needs a
/// statement journal. A constraint aborts when it resolves to ABORT (the
/// default), or to REPLACE for NOT NULL and CHECK, which fall back to ABORT.
/// IGNORE, FAIL and ROLLBACK never abort the statement alone. Each index is
/// `(on_conflict, is_unique)`.
pub(super) fn constraint_may_abort(
    has_statement_conflict: bool,
    statement_conflict: ResolveType,
    rowid_alias_conflict: Option<ResolveType>,
    mut indexes: impl Iterator<Item = (Option<ResolveType>, bool)>,
    has_notnull: bool,
    has_check: bool,
    has_unique: bool,
) -> bool {
    if has_statement_conflict {
        return match statement_conflict {
            ResolveType::Abort => has_notnull || has_check || has_unique,
            // A UNIQUE conflict is replaced, not aborted.
            ResolveType::Replace => has_notnull || has_check,
            _ => false,
        };
    }
    let rowid_aborts = match rowid_alias_conflict.unwrap_or(ResolveType::Abort) {
        ResolveType::Abort => has_unique,
        _ => false,
    };
    let index_aborts =
        indexes.any(
            |(conflict, unique)| match conflict.unwrap_or(ResolveType::Abort) {
                ResolveType::Abort => unique,
                ResolveType::Replace => has_notnull || has_check,
                _ => false,
            },
        );
    rowid_aborts || index_aborts || has_notnull || has_check
}
