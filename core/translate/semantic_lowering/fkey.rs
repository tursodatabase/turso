//! Foreign-key checks for DML lowered from HIR.
//!
//! The checks follow legacy's bytecode (`translate/fkeys.rs`), but read key
//! values through HIR sources instead of the resolver. Only NO ACTION and
//! RESTRICT are lowered here: a statement whose parent-side action is
//! CASCADE, SET NULL or SET DEFAULT still goes to legacy.
//!
//! Violations of an immediate constraint add to the statement's counter,
//! checked when the statement ends; deferred ones add to the connection's
//! counter, checked at COMMIT. RESTRICT stops the statement at once.

use std::num::NonZeroUsize;

use turso_parser::ast::RefAct;

use super::emit::{
    copy_with_affinity, emit_fk_restrict_halt, emit_fk_violation, emit_guarded_fk_decrement,
    emit_skip_if_any_null, index_probe, index_scan_match_any, open_read_index, open_read_table,
    table_scan_match_any,
};
use crate::{
    schema::BTreeTable,
    sync::Arc,
    translate::{
        collate::CollationSeq,
        semantic::hir::{self, HirDocument, SourceId},
    },
    vdbe::{
        builder::ProgramBuilder,
        insn::{CmpInsFlags, Insn},
        BranchOffset,
    },
    LimboError, Result,
};

/// Whether a parent-side action needs legacy: HIR lowers only the checks.
pub(super) fn delete_needs_legacy(foreign_keys: &hir::DmlForeignKeys) -> bool {
    foreign_keys
        .incoming
        .iter()
        .any(|foreign_key| !is_check_only(foreign_key.declaration.on_delete))
}

/// Whether an UPDATE that sets the columns at `assigned` changes a parent
/// key whose ON UPDATE action is not a plain check.
pub(super) fn update_needs_legacy(foreign_keys: &hir::DmlForeignKeys, assigned: &[usize]) -> bool {
    foreign_keys.incoming.iter().any(|foreign_key| {
        !is_check_only(foreign_key.declaration.on_update)
            && parent_key_may_change(foreign_key, assigned)
    })
}

/// Whether an UPDATE that sets the columns at `assigned` may change the
/// parent key. A rowid parent key is the INTEGER PRIMARY KEY column, which
/// `assigned` lists when the UPDATE sets the rowid.
fn parent_key_may_change(foreign_key: &hir::ResolvedForeignKey, assigned: &[usize]) -> bool {
    foreign_key
        .parent_positions
        .iter()
        .any(|position| assigned.contains(position))
}

fn child_key_changes(foreign_key: &hir::ResolvedForeignKey, assigned: &[usize]) -> bool {
    foreign_key
        .child_positions
        .iter()
        .any(|position| assigned.contains(position))
}

fn is_check_only(action: RefAct) -> bool {
    matches!(action, RefAct::NoAction | RefAct::Restrict)
}

/// Checks before the row of `target` that the table cursor is on is deleted.
/// `target` must be bound to that cursor and `rowid` must hold its rowid.
///
/// Where the target is the parent, a child row that still points at the row
/// is a violation. Where the target is the child of a deferred constraint, a
/// row that pointed at a missing parent takes back the violation it added.
///
/// `replacement` is a row that REPLACE writes in place of this one, bound to
/// its registers. Like legacy, an immediate NO ACTION constraint whose key
/// it keeps is not checked.
pub(super) fn emit_delete_checks(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_keys: &hir::DmlForeignKeys,
    target: SourceId,
    rowid: usize,
    database: usize,
    replacement: Option<SourceId>,
) -> Result<()> {
    for foreign_key in &foreign_keys.incoming {
        let declaration = &foreign_key.declaration;
        let keeps_key = replacement
            .filter(|_| declaration.on_delete == RefAct::NoAction && !declaration.deferred);
        let kept = match keeps_key {
            Some(replacement) => {
                let width = foreign_key.parent_positions.len();
                let old_key = emit_parent_key(program, document, foreign_key, target)?;
                let new_key = emit_parent_key(program, document, foreign_key, replacement)?;
                let kept = program.allocate_label();
                emit_skip_if_same_key(program, old_key, new_key, width, kept);
                Some(kept)
            }
            None => None,
        };
        emit_children_still_point_here(program, document, foreign_key, target, rowid, database)?;
        if let Some(kept) = kept {
            program.preassign_label_to_next_insn(kept);
        }
    }
    for foreign_key in &foreign_keys.outgoing {
        if foreign_key.declaration.deferred {
            emit_deleted_child_releases_violation(
                program,
                document,
                foreign_key,
                target,
                database,
            )?;
        }
    }
    Ok(())
}

/// Count, or stop at, child rows that point at the parent key of `target`'s
/// current row.
fn emit_children_still_point_here(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_key: &hir::ResolvedForeignKey,
    target: SourceId,
    rowid: usize,
    database: usize,
) -> Result<()> {
    let width = foreign_key.parent_columns.len();
    let key = emit_parent_key(program, document, foreign_key, target)?;
    let skip = program.allocate_label();
    emit_skip_if_any_null(program, key, width, skip);

    let child_table = btree(&foreign_key.child_table)?;
    let self_reference = is_self_reference(foreign_key);
    let restrict = foreign_key.declaration.on_delete == RefAct::Restrict;
    let declaration = foreign_key.declaration.clone();
    let violation = move |program: &mut ProgramBuilder| -> Result<()> {
        if restrict {
            emit_fk_restrict_halt(program)
        } else {
            emit_fk_violation(program, &declaration)
        }
    };
    // Like legacy, a self-reference scans the table so that the row being
    // deleted does not count as its own child.
    match foreign_key.child_index.as_ref().filter(|_| !self_reference) {
        Some(index) => {
            let cursor = open_read_index(program, &index.handle(), database);
            let probe = copy_with_affinity(program, key, width, index.value(), &child_table);
            index_probe(program, cursor, probe, width, violation, |_| Ok(()))?;
        }
        None => table_scan_match_any(
            program,
            &child_table,
            &foreign_key.child_positions,
            key,
            self_reference.then_some(rowid),
            database,
            violation,
        )?,
    }
    program.preassign_label_to_next_insn(skip);
    Ok(())
}

/// A deleted child row whose parent was missing added one to the deferred
/// counter when it was written; take that back.
fn emit_deleted_child_releases_violation(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_key: &hir::ResolvedForeignKey,
    target: SourceId,
    database: usize,
) -> Result<()> {
    let width = foreign_key.child_positions.len();
    let key = emit_child_key(program, document, foreign_key, target)?;
    let skip = program.allocate_label();
    emit_skip_if_any_null(program, key, width, skip);
    let parent_table = btree(&foreign_key.parent_table)?;
    if foreign_key.parent_uses_rowid {
        let cursor = open_read_table(program, &parent_table, database);
        let parent_rowid = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: key,
            dst_reg: parent_rowid,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MustBeInt {
            reg: parent_rowid,
            target_pc: None,
        });
        let missing = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor,
            rowid_reg: parent_rowid,
            target_pc: missing,
        });
        program.emit_insn(Insn::Close { cursor_id: cursor });
        program.emit_insn(Insn::Goto { target_pc: skip });
        program.preassign_label_to_next_insn(missing);
        program.emit_insn(Insn::Close { cursor_id: cursor });
        emit_guarded_fk_decrement(program, skip, true);
    } else {
        let index = parent_index(foreign_key)?;
        let cursor = open_read_index(program, &index.handle(), database);
        let probe = copy_with_affinity(program, key, width, index.value(), &parent_table);
        let found = program.allocate_label();
        program.emit_insn(Insn::Found {
            cursor_id: cursor,
            target_pc: found,
            record_reg: probe,
            num_regs: width,
        });
        program.emit_insn(Insn::Close { cursor_id: cursor });
        emit_guarded_fk_decrement(program, found, true);
        program.preassign_label_to_next_insn(found);
        program.emit_insn(Insn::Close { cursor_id: cursor });
    }
    program.preassign_label_to_next_insn(skip);
    Ok(())
}

/// Checks of a new row of `target` before it is written. `target` must be
/// bound to the row's registers and `rowid` must hold its rowid.
///
/// Where the target is the child, its parent must exist: an immediate
/// constraint stops the statement, a deferred one adds a violation.
pub(super) fn emit_insert_child_checks(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_keys: &hir::DmlForeignKeys,
    target: SourceId,
    rowid: usize,
    database: usize,
) -> Result<()> {
    for foreign_key in &foreign_keys.outgoing {
        let width = foreign_key.child_positions.len();
        let key = emit_child_key(program, document, foreign_key, target)?;
        let ok = program.allocate_label();
        emit_skip_if_any_null(program, key, width, ok);
        let parent_table = btree(&foreign_key.parent_table)?;
        let self_reference = is_self_reference(foreign_key);
        let deferred = foreign_key.declaration.deferred;
        let declaration = foreign_key.declaration.clone();
        let violation = move |program: &mut ProgramBuilder| -> Result<()> {
            if deferred {
                emit_fk_violation(program, &declaration)
            } else {
                emit_fk_restrict_halt(program)
            }
        };
        if foreign_key.parent_uses_rowid {
            let cursor = open_read_table(program, &parent_table, database);
            let parent_rowid = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: key,
                dst_reg: parent_rowid,
                extra_amount: 0,
            });
            let missing = program.allocate_label();
            program.emit_insn(Insn::MustBeInt {
                reg: parent_rowid,
                target_pc: Some(missing),
            });
            // A row that points at itself has its parent once it is written.
            if self_reference {
                program.emit_insn(Insn::Eq {
                    lhs: parent_rowid,
                    rhs: rowid,
                    target_pc: ok,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
            }
            program.emit_insn(Insn::NotExists {
                cursor,
                rowid_reg: parent_rowid,
                target_pc: missing,
            });
            program.emit_insn(Insn::Close { cursor_id: cursor });
            program.emit_insn(Insn::Goto { target_pc: ok });
            program.preassign_label_to_next_insn(missing);
            program.emit_insn(Insn::Close { cursor_id: cursor });
            violation(program)?;
        } else {
            if self_reference {
                emit_own_parent_check(program, document, foreign_key, target, key, ok)?;
            }
            let index = parent_index(foreign_key)?;
            let cursor = open_read_index(program, &index.handle(), database);
            let probe = copy_with_affinity(program, key, width, index.value(), &parent_table);
            index_probe(program, cursor, probe, width, |_| Ok(()), violation)?;
        }
        program.preassign_label_to_next_insn(ok);
    }
    Ok(())
}

/// After a new row of `target` is written: where the target is the parent
/// of a deferred or self-referencing constraint, each child row that points
/// at the new key had a missing parent until now, so take back its
/// violation. `target` must be bound to the row's registers.
///
/// Under REPLACE (`replaced`), a deleted conflicting row may have counted
/// children of any constraint, so every constraint is repaired, as legacy
/// does.
pub(super) fn emit_insert_parent_repairs(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_keys: &hir::DmlForeignKeys,
    target: SourceId,
    database: usize,
    replaced: bool,
) -> Result<()> {
    for foreign_key in &foreign_keys.incoming {
        let deferred = foreign_key.declaration.deferred;
        if !replaced && !deferred && !is_self_reference(foreign_key) {
            continue;
        }
        let width = foreign_key.parent_positions.len();
        let skip = program.allocate_label();
        program.emit_insn(Insn::FkIfZero {
            deferred,
            target_pc: skip,
        });
        let key = emit_parent_key(program, document, foreign_key, target)?;
        let parent_table = btree(&foreign_key.parent_table)?;
        // The key is compared as the parent stores it.
        let affinities = if foreign_key.parent_uses_rowid {
            "i".to_string()
        } else {
            foreign_key
                .parent_positions
                .iter()
                .map(|&position| {
                    parent_table.columns()[position]
                        .affinity_with_strict(parent_table.is_strict)
                        .aff_mask()
                })
                .collect()
        };
        if let Some(count) = NonZeroUsize::new(width) {
            program.emit_insn(Insn::Affinity {
                start_reg: key,
                count,
                affinities,
            });
        }
        emit_skip_if_any_null(program, key, width, skip);
        let child_table = btree(&foreign_key.child_table)?;
        let repair = |program: &mut ProgramBuilder| -> Result<()> {
            let next = program.allocate_label();
            emit_guarded_fk_decrement(program, next, deferred);
            program.preassign_label_to_next_insn(next);
            Ok(())
        };
        match &foreign_key.child_index {
            Some(index) => {
                let cursor = open_read_index(program, &index.handle(), database);
                let probe = copy_with_affinity(program, key, width, index.value(), &child_table);
                index_scan_match_any(program, cursor, probe, width, None, repair)?;
            }
            None => table_scan_match_any(
                program,
                &child_table,
                &foreign_key.child_positions,
                key,
                None,
                database,
                repair,
            )?,
        }
        program.preassign_label_to_next_insn(skip);
    }
    Ok(())
}

/// Checks of an UPDATE before any index entry or the row is rewritten.
/// `old` must be bound to the table cursor on the row, `new` to the new
/// row's registers, and `rowid` must hold the rowid; `assigned` lists the
/// columns the UPDATE sets.
///
/// Like legacy, where the target is the parent and the parent key changes,
/// children of the old key become violations (RESTRICT stops at once) and
/// children of the new key take back deferred violations. Where the target
/// is the child and the child key changes, the old key takes back its
/// deferred violation if its parent was missing, and a new key without a
/// parent adds one.
///
/// `rowid` holds the row's rowid in the table and `new_rowid` the one it is
/// written with.
///
/// When the UPDATE may REPLACE (`replaces`), another row may still hold the
/// new parent key until the write, so the repairs of the new key are
/// returned to be emitted after it with [`emit_update_repairs`].
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_update_checks<'a>(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_keys: &'a hir::DmlForeignKeys,
    old: SourceId,
    new: SourceId,
    rowid: usize,
    new_rowid: usize,
    assigned: &[usize],
    database: usize,
    replaces: bool,
) -> Result<Vec<PendingRepair<'a>>> {
    let mut pending = Vec::new();
    for foreign_key in &foreign_keys.incoming {
        if !parent_key_may_change(foreign_key, assigned) {
            continue;
        }
        assert!(
            is_check_only(foreign_key.declaration.on_update),
            "an ON UPDATE action that changes rows must go to legacy"
        );
        let width = foreign_key.parent_positions.len();
        let old_key = emit_parent_key(program, document, foreign_key, old)?;
        let new_key = emit_parent_key(program, document, foreign_key, new)?;
        let deferred = foreign_key.declaration.deferred;
        // Set when the key changes, for the repairs after the write.
        let changed = (replaces && deferred).then(|| {
            let changed = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: changed,
            });
            changed
        });
        let unchanged = program.allocate_label();
        emit_skip_if_same_key(program, old_key, new_key, width, unchanged);
        if let Some(changed) = changed {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: changed,
            });
            pending.push(PendingRepair {
                foreign_key,
                changed,
                new_key,
            });
        }
        // A row that points at itself and changes its child key too no
        // longer points at its old parent key.
        let exclude = (is_self_reference(foreign_key) && child_key_changes(foreign_key, assigned))
            .then_some(rowid);
        let restrict = foreign_key.declaration.on_update == RefAct::Restrict;
        let declaration = foreign_key.declaration.clone();
        emit_child_probe(
            program,
            foreign_key,
            old_key,
            exclude,
            database,
            move |program: &mut ProgramBuilder| {
                if restrict {
                    emit_fk_restrict_halt(program)
                } else {
                    emit_fk_violation(program, &declaration)
                }
            },
        )?;
        if deferred && !replaces {
            emit_new_parent_key_repairs(program, foreign_key, new_key, database)?;
        }
        program.preassign_label_to_next_insn(unchanged);
    }

    for foreign_key in &foreign_keys.outgoing {
        if !child_key_changes(foreign_key, assigned) {
            continue;
        }
        let width = foreign_key.child_positions.len();
        let parent_table = btree(&foreign_key.parent_table)?;
        if foreign_key.declaration.deferred {
            let old_key = emit_child_key(program, document, foreign_key, old)?;
            let skip = program.allocate_label();
            emit_skip_if_any_null(program, old_key, width, skip);
            if foreign_key.parent_uses_rowid {
                let cursor = open_read_table(program, &parent_table, database);
                let parent_rowid = program.alloc_register();
                program.emit_insn(Insn::Copy {
                    src_reg: old_key,
                    dst_reg: parent_rowid,
                    extra_amount: 0,
                });
                let missing = program.allocate_label();
                program.emit_insn(Insn::MustBeInt {
                    reg: parent_rowid,
                    target_pc: Some(missing),
                });
                program.emit_insn(Insn::NotExists {
                    cursor,
                    rowid_reg: parent_rowid,
                    target_pc: missing,
                });
                program.emit_insn(Insn::Close { cursor_id: cursor });
                program.emit_insn(Insn::Goto { target_pc: skip });
                program.preassign_label_to_next_insn(missing);
                program.emit_insn(Insn::Close { cursor_id: cursor });
                emit_guarded_fk_decrement(program, skip, true);
            } else {
                let index = parent_index(foreign_key)?;
                let cursor = open_read_index(program, &index.handle(), database);
                let probe =
                    copy_with_affinity(program, old_key, width, index.value(), &parent_table);
                index_probe(
                    program,
                    cursor,
                    probe,
                    width,
                    |_| Ok(()),
                    |program| {
                        let next = program.allocate_label();
                        emit_guarded_fk_decrement(program, next, true);
                        program.preassign_label_to_next_insn(next);
                        Ok(())
                    },
                )?;
            }
            program.preassign_label_to_next_insn(skip);
        }

        // Unlike INSERT, a new key without a parent adds a violation even for
        // an immediate constraint, checked when the statement ends.
        let new_key = emit_child_key(program, document, foreign_key, new)?;
        let ok = program.allocate_label();
        emit_skip_if_any_null(program, new_key, width, ok);
        let self_reference = is_self_reference(foreign_key);
        let declaration = foreign_key.declaration.clone();
        let violation =
            move |program: &mut ProgramBuilder| emit_fk_violation(program, &declaration);
        if foreign_key.parent_uses_rowid {
            let cursor = open_read_table(program, &parent_table, database);
            let parent_rowid = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: new_key,
                dst_reg: parent_rowid,
                extra_amount: 0,
            });
            let missing = program.allocate_label();
            program.emit_insn(Insn::MustBeInt {
                reg: parent_rowid,
                target_pc: Some(missing),
            });
            // The row may be its own parent, under its new rowid.
            if self_reference {
                program.emit_insn(Insn::Eq {
                    lhs: parent_rowid,
                    rhs: new_rowid,
                    target_pc: ok,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
            }
            program.emit_insn(Insn::NotExists {
                cursor,
                rowid_reg: parent_rowid,
                target_pc: missing,
            });
            program.emit_insn(Insn::Close { cursor_id: cursor });
            program.emit_insn(Insn::Goto { target_pc: ok });
            program.preassign_label_to_next_insn(missing);
            program.emit_insn(Insn::Close { cursor_id: cursor });
            violation(program)?;
        } else {
            if self_reference {
                emit_own_parent_check(program, document, foreign_key, new, new_key, ok)?;
            }
            let index = parent_index(foreign_key)?;
            let cursor = open_read_index(program, &index.handle(), database);
            let probe = copy_with_affinity(program, new_key, width, index.value(), &parent_table);
            index_probe(program, cursor, probe, width, |_| Ok(()), violation)?;
        }
        program.preassign_label_to_next_insn(ok);
    }
    Ok(pending)
}

/// A repair of children of a new parent key that waits until the UPDATE has
/// written its row.
pub(super) struct PendingRepair<'a> {
    foreign_key: &'a hir::ResolvedForeignKey,
    /// Whether the key changed.
    changed: usize,
    new_key: usize,
}

/// After the row is written, children of a new deferred parent key take
/// back their violations, as legacy does under REPLACE.
pub(super) fn emit_update_repairs(
    program: &mut ProgramBuilder,
    pending: &[PendingRepair<'_>],
    database: usize,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let done = program.allocate_label();
    program.emit_insn(Insn::FkIfZero {
        deferred: true,
        target_pc: done,
    });
    for repair in pending {
        let skip = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: repair.changed,
            target_pc: skip,
            jump_if_null: true,
        });
        emit_new_parent_key_repairs(program, repair.foreign_key, repair.new_key, database)?;
        program.preassign_label_to_next_insn(skip);
    }
    program.preassign_label_to_next_insn(done);
    Ok(())
}

/// Children of a new deferred parent key had no parent until now: take back
/// one violation for each.
fn emit_new_parent_key_repairs(
    program: &mut ProgramBuilder,
    foreign_key: &hir::ResolvedForeignKey,
    new_key: usize,
    database: usize,
) -> Result<()> {
    emit_child_probe(
        program,
        foreign_key,
        new_key,
        None,
        database,
        |program: &mut ProgramBuilder| {
            let next = program.allocate_label();
            emit_guarded_fk_decrement(program, next, true);
            program.preassign_label_to_next_insn(next);
            Ok(())
        },
    )
}

/// Jump to `same` when the two keys are equal, NULLs included, as legacy's
/// parent-key change test does.
fn emit_skip_if_same_key(
    program: &mut ProgramBuilder,
    old_key: usize,
    new_key: usize,
    width: usize,
    same: BranchOffset,
) {
    let changed = program.allocate_label();
    for offset in 0..width {
        let next = if offset + 1 == width {
            same
        } else {
            program.allocate_label()
        };
        program.emit_insn(Insn::Eq {
            lhs: old_key + offset,
            rhs: new_key + offset,
            target_pc: next,
            flags: CmpInsFlags::default().null_eq(),
            collation: None,
        });
        program.emit_insn(Insn::Goto { target_pc: changed });
        if offset + 1 != width {
            program.preassign_label_to_next_insn(next);
        }
    }
    program.preassign_label_to_next_insn(changed);
}

/// Run `on_match` for child rows that point at the parent key in `key`,
/// skipping the child row `exclude` and keys with a NULL. Like legacy, an
/// index that lists exactly the child key is probed, and otherwise the child
/// table is scanned.
fn emit_child_probe(
    program: &mut ProgramBuilder,
    foreign_key: &hir::ResolvedForeignKey,
    key: usize,
    exclude: Option<usize>,
    database: usize,
    on_match: impl FnMut(&mut ProgramBuilder) -> Result<()>,
) -> Result<()> {
    let width = foreign_key.parent_positions.len();
    let skip = program.allocate_label();
    emit_skip_if_any_null(program, key, width, skip);
    let child_table = btree(&foreign_key.child_table)?;
    match &foreign_key.child_index {
        Some(index) => {
            let cursor = open_read_index(program, &index.handle(), database);
            let probe = copy_with_affinity(program, key, width, index.value(), &child_table);
            if exclude.is_some() {
                index_scan_match_any(program, cursor, probe, width, exclude, on_match)?;
            } else {
                index_probe(program, cursor, probe, width, on_match, |_| Ok(()))?;
            }
        }
        None => table_scan_match_any(
            program,
            &child_table,
            &foreign_key.child_positions,
            key,
            exclude,
            database,
            on_match,
        )?,
    }
    program.preassign_label_to_next_insn(skip);
    Ok(())
}

/// Jump to `ok` when the child key in `child_key` equals the parent key of
/// the same row `row`, compared as stored and with BINARY: once written, the
/// row is its own parent. Like legacy and SQLite, the parent's affinity is
/// not applied here.
fn emit_own_parent_check(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_key: &hir::ResolvedForeignKey,
    row: SourceId,
    child_key: usize,
    ok: BranchOffset,
) -> Result<()> {
    let parent_key = emit_parent_key(program, document, foreign_key, row)?;
    let mismatch = program.allocate_label();
    for offset in 0..foreign_key.child_positions.len() {
        program.emit_insn(Insn::Ne {
            lhs: child_key + offset,
            rhs: parent_key + offset,
            target_pc: mismatch,
            flags: CmpInsFlags::default().jump_if_null(),
            collation: Some(CollationSeq::Binary),
        });
    }
    program.emit_insn(Insn::Goto { target_pc: ok });
    program.preassign_label_to_next_insn(mismatch);
    Ok(())
}

/// Read the parent key of `row`, in the constraint's column order, into new
/// registers and return the first.
fn emit_parent_key(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_key: &hir::ResolvedForeignKey,
    row: SourceId,
) -> Result<usize> {
    let start = program.alloc_registers(foreign_key.parent_positions.len());
    for (offset, &column) in foreign_key.parent_positions.iter().enumerate() {
        let expression = if foreign_key.parent_uses_rowid {
            hir::Expr::RowId(row)
        } else {
            hir::Expr::Column(hir::ColumnRef {
                source: row,
                column,
            })
        };
        super::expr::translate_expr_no_constant_opt(
            program,
            document,
            &expression,
            start + offset,
        )?;
    }
    Ok(start)
}

/// Read the child key of `row`, in the constraint's column order, into new
/// registers and return the first.
fn emit_child_key(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    foreign_key: &hir::ResolvedForeignKey,
    row: SourceId,
) -> Result<usize> {
    let start = program.alloc_registers(foreign_key.child_positions.len());
    for (offset, &column) in foreign_key.child_positions.iter().enumerate() {
        super::expr::translate_expr_no_constant_opt(
            program,
            document,
            &hir::Expr::Column(hir::ColumnRef {
                source: row,
                column,
            }),
            start + offset,
        )?;
    }
    Ok(start)
}

fn is_self_reference(foreign_key: &hir::ResolvedForeignKey) -> bool {
    foreign_key.child_table == foreign_key.parent_table
}

fn btree(table: &hir::ResolvedTable) -> Result<Arc<BTreeTable>> {
    table.value().btree().ok_or_else(|| {
        LimboError::InternalError(format!(
            "foreign key table {} is not a B-tree table",
            table.value().get_name()
        ))
    })
}

fn parent_index(foreign_key: &hir::ResolvedForeignKey) -> Result<&hir::ResolvedIndex> {
    foreign_key.parent_unique_index.as_ref().ok_or_else(|| {
        LimboError::InternalError(format!(
            "foreign key to {} has no parent index",
            foreign_key.parent_table.value().get_name()
        ))
    })
}
