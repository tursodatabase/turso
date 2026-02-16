use crate::sync::RwLock;
use rustc_hash::FxHashSet as HashSet;
use turso_parser::ast::{self, Expr, Literal, Name, QualifiedName, RefAct};

use super::{translate_inner, ProgramBuilder, ProgramBuilderOpts};
use crate::{
    error::SQLITE_CONSTRAINT_FOREIGNKEY,
    schema::{BTreeTable, ForeignKey, Index, ResolvedFkRef, ROWID_SENTINEL},
    translate::{collate::CollationSeq, emitter::Resolver, planner::ROWID_STRS},
    vdbe::{
        builder::{CursorType, QueryMode},
        insn::{CmpInsFlags, Insn},
        BranchOffset,
    },
    Connection, LimboError, Result, Statement, Value,
};
use std::{num::NonZero, num::NonZeroUsize, sync::Arc};

#[inline]
pub fn emit_guarded_fk_decrement(
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

/// Open a read cursor on an index and return its cursor id.
#[inline]
pub fn open_read_index(program: &mut ProgramBuilder, idx: &Arc<Index>) -> usize {
    let icur = program.alloc_cursor_id(CursorType::BTreeIndex(idx.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: icur,
        root_page: idx.root_page,
        db: 0,
    });
    icur
}

/// Open a read cursor on a table and return its cursor id.
#[inline]
pub fn open_read_table(program: &mut ProgramBuilder, tbl: &Arc<BTreeTable>) -> usize {
    let tcur = program.alloc_cursor_id(CursorType::BTreeTable(tbl.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: tcur,
        root_page: tbl.root_page,
        db: 0,
    });
    tcur
}

/// Copy `len` registers starting at `src_start` to a fresh block and apply index affinities.
/// Returns the destination start register.
#[inline]
fn copy_with_affinity(
    program: &mut ProgramBuilder,
    src_start: usize,
    len: usize,
    idx: &Index,
    aff_from_tbl: &BTreeTable,
) -> usize {
    let dst = program.alloc_registers(len);
    for i in 0..len {
        program.emit_insn(Insn::Copy {
            src_reg: src_start + i,
            dst_reg: dst + i,
            extra_amount: 0,
        });
    }
    if let Some(count) = NonZeroUsize::new(len) {
        program.emit_insn(Insn::Affinity {
            start_reg: dst,
            count,
            affinities: build_index_affinity_string(idx, aff_from_tbl),
        });
    }
    dst
}

/// Issue an index probe using `Found`/`NotFound` and route to `on_found`/`on_not_found`.
pub fn index_probe<F, G>(
    program: &mut ProgramBuilder,
    icur: usize,
    record_reg: usize,
    num_regs: usize,
    mut on_found: F,
    mut on_not_found: G,
) -> Result<()>
where
    F: FnMut(&mut ProgramBuilder) -> Result<()>,
    G: FnMut(&mut ProgramBuilder) -> Result<()>,
{
    let lbl_found = program.allocate_label();
    let lbl_join = program.allocate_label();

    program.emit_insn(Insn::Found {
        cursor_id: icur,
        target_pc: lbl_found,
        record_reg,
        num_regs,
    });

    // NOT FOUND path
    on_not_found(program)?;
    program.emit_insn(Insn::Goto {
        target_pc: lbl_join,
    });

    // FOUND path
    program.preassign_label_to_next_insn(lbl_found);
    on_found(program)?;

    // Join & close once
    program.preassign_label_to_next_insn(lbl_join);
    program.emit_insn(Insn::Close { cursor_id: icur });
    Ok(())
}

/// Iterate a table and call `on_match` when all child columns equal the key at `parent_key_start`.
/// Skips rows where any FK column is NULL. If `self_exclude_rowid` is Some, the row with that rowid is skipped.
fn table_scan_match_any<F>(
    program: &mut ProgramBuilder,
    child_tbl: &Arc<BTreeTable>,
    child_cols: &[String],
    parent_key_start: usize,
    self_exclude_rowid: Option<usize>,
    mut on_match: F,
) -> Result<()>
where
    F: FnMut(&mut ProgramBuilder) -> Result<()>,
{
    let ccur = open_read_table(program, child_tbl);
    let done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: ccur,
        pc_if_empty: done,
    });

    let loop_top = program.allocate_label();
    program.preassign_label_to_next_insn(loop_top);
    let next_row = program.allocate_label();

    // Compare each FK column to parent key component.
    for (i, cname) in child_cols.iter().enumerate() {
        let (pos, _) = child_tbl
            .get_column(cname)
            .ok_or_else(|| LimboError::InternalError(format!("child col {cname} missing")))?;
        let tmp = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: ccur,
            column: pos,
            dest: tmp,
            default: None,
        });
        program.emit_insn(Insn::IsNull {
            reg: tmp,
            target_pc: next_row,
        });

        let cont = program.allocate_label();
        program.emit_insn(Insn::Eq {
            lhs: tmp,
            rhs: parent_key_start + i,
            target_pc: cont,
            flags: CmpInsFlags::default().jump_if_null(),
            collation: Some(CollationSeq::Binary),
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_row,
        });
        program.preassign_label_to_next_insn(cont);
    }

    //self-reference exclusion on rowid
    if let Some(parent_rowid) = self_exclude_rowid {
        let child_rowid = program.alloc_register();
        let skip = program.allocate_label();
        program.emit_insn(Insn::RowId {
            cursor_id: ccur,
            dest: child_rowid,
        });
        program.emit_insn(Insn::Eq {
            lhs: child_rowid,
            rhs: parent_rowid,
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
        cursor_id: ccur,
        pc_if_next: loop_top,
    });

    program.preassign_label_to_next_insn(done);
    program.emit_insn(Insn::Close { cursor_id: ccur });
    Ok(())
}

/// Build the index affinity mask string (one char per indexed column).
#[inline]
pub fn build_index_affinity_string(idx: &Index, table: &BTreeTable) -> String {
    idx.columns
        .iter()
        .map(|ic| table.columns[ic.pos_in_table].affinity().aff_mask())
        .collect()
}

/// Increment a foreign key violation counter; for deferred FKs, this is a global counter
/// on the connection; for immediate FKs, this is a per-statement counter in the program state.
/// Used for NO ACTION behavior where violation is checked at statement/transaction end.
pub fn emit_fk_violation(program: &mut ProgramBuilder, fk: &ForeignKey) -> Result<()> {
    program.emit_insn(Insn::FkCounter {
        increment_value: 1,
        deferred: fk.deferred,
    });
    Ok(())
}

/// Emit an immediate HALT for FK violations.
pub fn emit_fk_restrict_halt(program: &mut ProgramBuilder) -> Result<()> {
    program.emit_insn(Insn::Halt {
        err_code: SQLITE_CONSTRAINT_FOREIGNKEY,
        description: "FOREIGN KEY constraint failed".to_string(),
    });
    Ok(())
}

/// Stabilize the NEW row image for FK checks (UPDATE):
/// fill in unmodified PK columns from the current row so the NEW PK vector is complete.
pub fn stabilize_new_row_for_fk(
    program: &mut ProgramBuilder,
    table_btree: &BTreeTable,
    set_clauses: &[(usize, Box<Expr>)],
    cursor_id: usize,
    start: usize,
    rowid_new_reg: usize,
) -> Result<()> {
    if table_btree.primary_key_columns.is_empty() {
        return Ok(());
    }
    let set_cols: HashSet<usize> = set_clauses
        .iter()
        .filter_map(|(i, _)| if *i == ROWID_SENTINEL { None } else { Some(*i) })
        .collect();

    for (pk_name, _) in &table_btree.primary_key_columns {
        let (pos, col) = table_btree
            .get_column(pk_name)
            .ok_or_else(|| LimboError::InternalError(format!("pk col {pk_name} missing")))?;
        if !set_cols.contains(&pos) {
            if col.is_rowid_alias() {
                program.emit_insn(Insn::Copy {
                    src_reg: rowid_new_reg,
                    dst_reg: start + pos,
                    extra_amount: 0,
                });
            } else {
                program.emit_insn(Insn::Column {
                    cursor_id,
                    column: pos,
                    dest: start + pos,
                    default: None,
                });
            }
        }
    }
    Ok(())
}

/// Parent-side checks when the parent key might change (UPDATE on parent):
/// Detect if any child references the OLD key (potential violation), and if any references the NEW key
/// (which cancels one potential violation). For composite keys this builds OLD/NEW vectors first.
#[allow(clippy::too_many_arguments)]
pub fn emit_parent_key_change_checks(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_btree: &BTreeTable,
    indexes_to_update: impl Iterator<Item = impl AsRef<Index>>,
    cursor_id: usize,
    old_rowid_reg: usize,
    start: usize,
    rowid_new_reg: usize,
    rowid_set_clause_reg: Option<usize>,
    set_clauses: &[(usize, Box<Expr>)],
) -> Result<()> {
    let updated_positions: HashSet<usize> = set_clauses.iter().map(|(i, _)| *i).collect();
    let incoming = resolver
        .schema
        .resolved_fks_referencing(&table_btree.name)?;
    let affects_pk = incoming
        .iter()
        .any(|r| r.parent_key_may_change(&updated_positions, table_btree));
    if !affects_pk {
        return Ok(());
    }

    let primary_key_is_rowid_alias = table_btree.get_rowid_alias_column().is_some();

    if primary_key_is_rowid_alias || table_btree.primary_key_columns.is_empty() {
        emit_rowid_pk_change_check(
            program,
            &incoming,
            resolver,
            old_rowid_reg,
            rowid_set_clause_reg.unwrap_or(old_rowid_reg),
        )?;
    }

    for index in indexes_to_update {
        emit_parent_index_key_change_checks(
            program,
            cursor_id,
            start,
            old_rowid_reg,
            rowid_new_reg,
            &incoming,
            resolver,
            table_btree,
            index.as_ref(),
        )?;
    }
    Ok(())
}

/// Rowid-table parent PK change: compare rowid OLD vs NEW; if changed, run two-pass counters.
pub fn emit_rowid_pk_change_check(
    program: &mut ProgramBuilder,
    incoming: &[ResolvedFkRef],
    resolver: &Resolver,
    old_rowid_reg: usize,
    new_rowid_reg: usize,
) -> Result<()> {
    let skip = program.allocate_label();
    program.emit_insn(Insn::Eq {
        lhs: new_rowid_reg,
        rhs: old_rowid_reg,
        target_pc: skip,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    let old_pk = program.alloc_register();
    let new_pk = program.alloc_register();
    program.emit_insn(Insn::Copy {
        src_reg: old_rowid_reg,
        dst_reg: old_pk,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: new_rowid_reg,
        dst_reg: new_pk,
        extra_amount: 0,
    });

    emit_fk_parent_pk_change_counters(program, incoming, resolver, old_pk, new_pk, 1)?;
    program.preassign_label_to_next_insn(skip);
    Ok(())
}

/// Foreign keys are only legal if the referenced parent key is:
/// 1. The rowid alias (no separate index)
/// 2. Part of a primary key / unique index (there is no practical difference between the two)
///
/// If the foreign key references a composite key, all of the columns in the key must be referenced.
/// E.g.
/// CREATE TABLE parent (a, b, c, PRIMARY KEY (a, b, c));
/// CREATE TABLE child (a, b, c, FOREIGN KEY (a, b, c) REFERENCES parent (a, b, c));
///
/// Whereas this is not allowed:
/// CREATE TABLE parent (a, b, c, PRIMARY KEY (a, b, c));
/// CREATE TABLE child (a, b, c, FOREIGN KEY (a, b) REFERENCES parent (a, b, c));
///
/// This function checks if the parent key has changed by comparing the OLD and NEW values.
/// If the parent key has changed, it emits the counters for the foreign keys.
/// If the parent key has not changed, it does nothing.
#[allow(clippy::too_many_arguments)]
pub fn emit_parent_index_key_change_checks(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    new_values_start: usize,
    old_rowid_reg: usize,
    new_rowid_reg: usize,
    incoming: &[ResolvedFkRef],
    resolver: &Resolver,
    table_btree: &BTreeTable,
    index: &Index,
) -> Result<()> {
    // Only process FKs that:
    // 1. Reference this specific index (OLD/NEW key vectors are built from this index's columns)
    // 2. Have NO ACTION or RESTRICT on_update action (CASCADE/SET NULL/SET DEFAULT are handled
    //    later by fire_fk_update_actions AFTER the update completes)
    let matching_fks: Vec<_> = incoming
        .iter()
        .filter(|fk_ref| {
            let matches_index = fk_ref
                .parent_unique_index
                .as_ref()
                .is_some_and(|idx| idx.name == index.name);
            let is_noaction_or_restrict =
                matches!(fk_ref.fk.on_update, RefAct::NoAction | RefAct::Restrict);
            matches_index && is_noaction_or_restrict
        })
        .cloned()
        .collect();

    if matching_fks.is_empty() {
        return Ok(());
    }

    let idx_len = index.columns.len();

    let old_key = program.alloc_registers(idx_len);
    for (i, index_col) in index.columns.iter().enumerate() {
        let pos_in_table = index_col.pos_in_table;
        let column = &table_btree.columns[pos_in_table];
        if column.is_rowid_alias() {
            program.emit_insn(Insn::Copy {
                src_reg: old_rowid_reg,
                dst_reg: old_key + i,
                extra_amount: 0,
            });
        } else {
            program.emit_insn(Insn::Column {
                cursor_id,
                column: pos_in_table,
                dest: old_key + i,
                default: None,
            });
        }
    }
    let new_key = program.alloc_registers(idx_len);
    for (i, index_col) in index.columns.iter().enumerate() {
        let pos_in_table = index_col.pos_in_table;
        let column = &table_btree.columns[pos_in_table];
        let src = if column.is_rowid_alias() {
            new_rowid_reg
        } else {
            new_values_start + pos_in_table
        };
        program.emit_insn(Insn::Copy {
            src_reg: src,
            dst_reg: new_key + i,
            extra_amount: 0,
        });
    }

    let skip = program.allocate_label();
    let changed = program.allocate_label();
    for i in 0..idx_len {
        let next = if i + 1 == idx_len {
            None
        } else {
            Some(program.allocate_label())
        };
        program.emit_insn(Insn::Eq {
            lhs: old_key + i,
            rhs: new_key + i,
            target_pc: next.unwrap_or(skip),
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto { target_pc: changed });
        if let Some(n) = next {
            program.preassign_label_to_next_insn(n);
        }
    }

    program.preassign_label_to_next_insn(changed);
    emit_fk_parent_pk_change_counters(program, &matching_fks, resolver, old_key, new_key, idx_len)?;
    program.preassign_label_to_next_insn(skip);
    Ok(())
}

/// Two-pass parent-side maintenance for UPDATE of a parent key:
/// 1. Probe child for OLD key, increment deferred counter if any references exist.
/// 2. Probe child for NEW key, guarded decrement cancels exactly one increment if present
pub fn emit_fk_parent_pk_change_counters(
    program: &mut ProgramBuilder,
    incoming: &[ResolvedFkRef],
    resolver: &Resolver,
    old_pk_start: usize,
    new_pk_start: usize,
    n_cols: usize,
) -> Result<()> {
    for fk_ref in incoming {
        emit_fk_parent_key_probe(
            program,
            resolver,
            fk_ref,
            old_pk_start,
            n_cols,
            ParentProbePass::Old,
        )?;
        emit_fk_parent_key_probe(
            program,
            resolver,
            fk_ref,
            new_pk_start,
            n_cols,
            ParentProbePass::New,
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ParentProbePass {
    Old,
    New,
}

/// Probe the child side for a given parent key
/// For RESTRICT on OLD pass: emits immediate HALT
/// For NO ACTION on OLD pass: increments FK violation counter
fn emit_fk_parent_key_probe(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    fk_ref: &ResolvedFkRef,
    parent_key_start: usize,
    n_cols: usize,
    pass: ParentProbePass,
) -> Result<()> {
    let child_tbl = &fk_ref.child_table;
    let child_cols = &fk_ref.fk.child_columns;
    let is_deferred = fk_ref.fk.deferred;
    let is_restrict = matches!(fk_ref.fk.on_update, RefAct::Restrict);

    let on_match = |p: &mut ProgramBuilder| -> Result<()> {
        match (is_deferred, pass) {
            // OLD key referenced by a child
            (_, ParentProbePass::Old) => {
                if is_restrict {
                    // RESTRICT: immediate halt
                    emit_fk_restrict_halt(p)?;
                } else {
                    // NO ACTION: increment counter (checked at statement/transaction end)
                    emit_fk_violation(p, &fk_ref.fk)?;
                }
            }

            // NEW key referenced by a child (cancel one deferred violation)
            // Note: for RESTRICT, we already halted on OLD pass if child exists,
            // so this branch only applies to NO ACTION deferred FKs
            (true, ParentProbePass::New) => {
                // Guard to avoid underflow if OLD pass didn't increment.
                let skip = p.allocate_label();
                emit_guarded_fk_decrement(p, skip, fk_ref.fk.deferred);
                p.preassign_label_to_next_insn(skip);
            }
            // Immediate FK on NEW pass: nothing to cancel; do nothing.
            (false, ParentProbePass::New) => {}
        }
        Ok(())
    };

    // Prefer exact child index on (child_cols...)
    let idx = resolver.schema.get_indices(&child_tbl.name).find(|ix| {
        ix.columns.len() == child_cols.len()
            && ix
                .columns
                .iter()
                .zip(child_cols.iter())
                .all(|(ic, cc)| ic.name.eq_ignore_ascii_case(cc))
    });

    if let Some(ix) = idx {
        let icur = open_read_index(program, ix);
        let probe = copy_with_affinity(program, parent_key_start, n_cols, ix, child_tbl);

        // FOUND => on_match; NOT FOUND => no-op
        index_probe(program, icur, probe, n_cols, on_match, |_p| Ok(()))?;
    } else {
        // Table scan fallback
        table_scan_match_any(
            program,
            child_tbl,
            child_cols,
            parent_key_start,
            None,
            on_match,
        )?;
    }

    Ok(())
}

/// Build a parent key vector (in FK parent-column order) into `dest_start`.
/// Handles rowid aliasing and explicit ROWID names; uses current row for non-rowid columns.
fn build_parent_key(
    program: &mut ProgramBuilder,
    parent_bt: &BTreeTable,
    parent_cols: &[String],
    parent_cursor_id: usize,
    parent_rowid_reg: usize,
    dest_start: usize,
) -> Result<()> {
    for (i, pcol) in parent_cols.iter().enumerate() {
        let src = if ROWID_STRS.iter().any(|s| pcol.eq_ignore_ascii_case(s)) {
            parent_rowid_reg
        } else {
            let (pos, col) = parent_bt
                .get_column(pcol)
                .ok_or_else(|| LimboError::InternalError(format!("col {pcol} missing")))?;
            if col.is_rowid_alias() {
                parent_rowid_reg
            } else {
                program.emit_insn(Insn::Column {
                    cursor_id: parent_cursor_id,
                    column: pos,
                    dest: dest_start + i,
                    default: None,
                });
                continue;
            }
        };
        program.emit_insn(Insn::Copy {
            src_reg: src,
            dst_reg: dest_start + i,
            extra_amount: 0,
        });
    }
    Ok(())
}

/// Child-side FK maintenance for UPDATE/UPSERT:
/// If any FK columns of this child row changed:
///  Pass 1 (OLD tuple): if OLD is non-NULL and parent is missing: decrement deferred counter (guarded).
///  Pass 2 (NEW tuple): if NEW is non-NULL and parent is missing: immediate error or deferred(+1).
#[allow(clippy::too_many_arguments)]
pub fn emit_fk_child_update_counters(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    child_tbl: &BTreeTable,
    child_table_name: &str,
    child_cursor_id: usize,
    new_start_reg: usize,
    new_rowid_reg: usize,
    updated_cols: &HashSet<usize>,
) -> Result<()> {
    // Helper: materialize OLD tuple for this FK; returns (start_reg, ncols) or None if any component is NULL.
    let load_old_tuple =
        |program: &mut ProgramBuilder, fk_cols: &[String]| -> Option<(usize, usize)> {
            let n = fk_cols.len();
            let start = program.alloc_registers(n);
            let null_jmp = program.allocate_label();

            for (k, cname) in fk_cols.iter().enumerate() {
                let (pos, _col) = match child_tbl.get_column(cname) {
                    Some(v) => v,
                    None => {
                        return None;
                    }
                };
                program.emit_column_or_rowid(child_cursor_id, pos, start + k);
                program.emit_insn(Insn::IsNull {
                    reg: start + k,
                    target_pc: null_jmp,
                });
            }

            // No NULLs, proceed
            let cont = program.allocate_label();
            program.emit_insn(Insn::Goto { target_pc: cont });
            // NULL encountered: invalidate tuple by jumping here
            program.preassign_label_to_next_insn(null_jmp);

            program.preassign_label_to_next_insn(cont);
            Some((start, n))
        };

    for fk_ref in resolver.schema.resolved_fks_for_child(child_table_name)? {
        // If the child-side FK columns did not change, there is nothing to do.
        if !fk_ref.child_key_changed(updated_cols, child_tbl) {
            continue;
        }

        let ncols = fk_ref.child_cols.len();

        // Pass 1: OLD tuple handling only for deferred FKs
        if fk_ref.fk.deferred {
            if let Some((old_start, _)) = load_old_tuple(program, &fk_ref.child_cols) {
                if fk_ref.parent_uses_rowid {
                    // Parent key is rowid: probe parent table by rowid
                    let parent_tbl = resolver
                        .schema
                        .get_btree_table(&fk_ref.fk.parent_table)
                        .expect("parent btree");
                    let pcur = open_read_table(program, &parent_tbl);

                    // first FK col is the rowid value
                    let rid = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: old_start,
                        dst_reg: rid,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::MustBeInt { reg: rid });

                    // If NOT exists => decrement
                    let miss = program.allocate_label();
                    program.emit_insn(Insn::NotExists {
                        cursor: pcur,
                        rowid_reg: rid,
                        target_pc: miss,
                    });
                    // found: close & continue
                    let join = program.allocate_label();
                    program.emit_insn(Insn::Close { cursor_id: pcur });
                    program.emit_insn(Insn::Goto { target_pc: join });

                    // missing: guarded decrement
                    program.preassign_label_to_next_insn(miss);
                    program.emit_insn(Insn::Close { cursor_id: pcur });
                    let skip = program.allocate_label();
                    emit_guarded_fk_decrement(program, skip, fk_ref.fk.deferred);
                    program.preassign_label_to_next_insn(skip);

                    program.preassign_label_to_next_insn(join);
                } else {
                    // Parent key is a unique index: use index probe and guarded decrement on NOT FOUND
                    let parent_tbl = resolver
                        .schema
                        .get_btree_table(&fk_ref.fk.parent_table)
                        .expect("parent btree");
                    let idx = fk_ref
                        .parent_unique_index
                        .as_ref()
                        .expect("parent unique index required");
                    let icur = open_read_index(program, idx);

                    // Copy OLD tuple and apply parent index affinities
                    let probe = copy_with_affinity(program, old_start, ncols, idx, &parent_tbl);
                    // Found: nothing; Not found: guarded decrement
                    index_probe(
                        program,
                        icur,
                        probe,
                        ncols,
                        |_p| Ok(()),
                        |p| {
                            let skip = p.allocate_label();
                            emit_guarded_fk_decrement(p, skip, fk_ref.fk.deferred);
                            p.preassign_label_to_next_insn(skip);
                            Ok(())
                        },
                    )?;
                }
            }
        }

        // Pass 2: NEW tuple handling
        let fk_ok = program.allocate_label();
        for cname in &fk_ref.fk.child_columns {
            let (i, col) = child_tbl.get_column(cname).unwrap();
            let src = if col.is_rowid_alias() {
                new_rowid_reg
            } else {
                new_start_reg + i
            };
            program.emit_insn(Insn::IsNull {
                reg: src,
                target_pc: fk_ok,
            });
        }

        if fk_ref.parent_uses_rowid {
            let parent_tbl = resolver
                .schema
                .get_btree_table(&fk_ref.fk.parent_table)
                .expect("parent btree");
            let pcur = open_read_table(program, &parent_tbl);

            // Take the first child column value from NEW image
            let (i_child, col_child) = child_tbl.get_column(&fk_ref.child_cols[0]).unwrap();
            let val_reg = if col_child.is_rowid_alias() {
                new_rowid_reg
            } else {
                new_start_reg + i_child
            };

            let tmp = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: val_reg,
                dst_reg: tmp,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MustBeInt { reg: tmp });

            let violation = program.allocate_label();
            program.emit_insn(Insn::NotExists {
                cursor: pcur,
                rowid_reg: tmp,
                target_pc: violation,
            });
            // found: close and continue
            program.emit_insn(Insn::Close { cursor_id: pcur });
            program.emit_insn(Insn::Goto { target_pc: fk_ok });

            // missing: violation (immediate HALT or deferred +1)
            program.preassign_label_to_next_insn(violation);
            program.emit_insn(Insn::Close { cursor_id: pcur });
            emit_fk_violation(program, &fk_ref.fk)?;
        } else {
            let parent_tbl = resolver
                .schema
                .get_btree_table(&fk_ref.fk.parent_table)
                .expect("parent btree");
            let idx = fk_ref
                .parent_unique_index
                .as_ref()
                .expect("parent unique index required");
            let icur = open_read_index(program, idx);

            // Build NEW probe (in FK child column order, aligns with parent index columns)
            let probe = {
                let start = program.alloc_registers(ncols);
                for (k, cname) in fk_ref.child_cols.iter().enumerate() {
                    let (i, col) = child_tbl.get_column(cname).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: if col.is_rowid_alias() {
                            new_rowid_reg
                        } else {
                            new_start_reg + i
                        },
                        dst_reg: start + k,
                        extra_amount: 0,
                    });
                }
                // Apply affinities of the parent index/table
                if let Some(cnt) = NonZeroUsize::new(ncols) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: start,
                        count: cnt,
                        affinities: build_index_affinity_string(idx, &parent_tbl),
                    });
                }
                start
            };

            // FOUND: ok; NOT FOUND: violation path
            index_probe(
                program,
                icur,
                probe,
                ncols,
                |_p| Ok(()),
                |p| {
                    emit_fk_violation(p, &fk_ref.fk)?;
                    Ok(())
                },
            )?;
            program.emit_insn(Insn::Goto { target_pc: fk_ok });
        }

        // Skip label for NEW tuple NULL short-circuit
        program.preassign_label_to_next_insn(fk_ok);
    }

    Ok(())
}

/// Single FK existence check for NO ACTION/RESTRICT on DELETE.
/// Raises a violation if any child row references the parent key.
/// For RESTRICT: emits immediate HALT
/// For NO ACTION: increments FK violation counter (checked at statement/transaction end)
fn emit_fk_delete_parent_existence_check_single(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    fk_ref: &ResolvedFkRef,
    parent_bt: &Arc<BTreeTable>,
    parent_table_name: &str,
    parent_cursor_id: usize,
    parent_rowid_reg: usize,
) -> Result<()> {
    let is_self_ref = fk_ref
        .child_table
        .name
        .eq_ignore_ascii_case(parent_table_name);

    let is_restrict = matches!(fk_ref.fk.on_delete, RefAct::Restrict);

    // Build parent key in FK's parent-column order
    let parent_cols: Vec<String> = if fk_ref.fk.parent_columns.is_empty() {
        parent_bt
            .primary_key_columns
            .iter()
            .map(|(n, _)| n.clone())
            .collect()
    } else {
        fk_ref.fk.parent_columns.clone()
    };
    let ncols = parent_cols.len();

    let parent_key_start = program.alloc_registers(ncols);
    build_parent_key(
        program,
        parent_bt,
        &parent_cols,
        parent_cursor_id,
        parent_rowid_reg,
        parent_key_start,
    )?;

    let child_cols = &fk_ref.fk.child_columns;
    let child_idx = if !is_self_ref {
        resolver
            .schema
            .get_indices(&fk_ref.child_table.name)
            .find(|idx| {
                idx.columns.len() == child_cols.len()
                    && idx
                        .columns
                        .iter()
                        .zip(child_cols.iter())
                        .all(|(ic, cc)| ic.name.eq_ignore_ascii_case(cc))
            })
    } else {
        None
    };

    // Closure to emit the appropriate violation based on action type
    let emit_violation = |p: &mut ProgramBuilder| -> Result<()> {
        if is_restrict {
            emit_fk_restrict_halt(p)?;
        } else {
            emit_fk_violation(p, &fk_ref.fk)?;
        }
        Ok(())
    };

    if let Some(idx) = child_idx {
        let icur = open_read_index(program, idx);
        let probe = copy_with_affinity(program, parent_key_start, ncols, idx, &fk_ref.child_table);
        index_probe(
            program,
            icur,
            probe,
            ncols,
            |p| {
                emit_violation(p)?;
                Ok(())
            },
            |_p| Ok(()),
        )?;
    } else {
        table_scan_match_any(
            program,
            &fk_ref.child_table,
            child_cols,
            parent_key_start,
            if is_self_ref {
                Some(parent_rowid_reg)
            } else {
                None
            },
            |p| {
                emit_violation(p)?;
                Ok(())
            },
        )?;
    }
    Ok(())
}

/// Handle all parent-side FK actions on UPDATE based on ON UPDATE action type.
#[allow(clippy::too_many_arguments)]
pub fn emit_fk_update_parent_actions(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_btree: &BTreeTable,
    indexes_to_update: impl Iterator<Item = impl AsRef<Index>>,
    cursor_id: usize,
    old_rowid_reg: usize,
    start: usize,
    rowid_new_reg: usize,
    rowid_set_clause_reg: Option<usize>,
    set_clauses: &[(usize, Box<Expr>)],
) -> Result<()> {
    let updated_positions: HashSet<usize> = set_clauses.iter().map(|(i, _)| *i).collect();
    let incoming = resolver
        .schema
        .resolved_fks_referencing(&table_btree.name)?;
    let affects_pk = incoming
        .iter()
        .any(|r| r.parent_key_may_change(&updated_positions, table_btree));
    if !affects_pk {
        return Ok(());
    }

    // Collect indexes to update into a Vec so we can iterate multiple times
    let indexes: Vec<_> = indexes_to_update.collect();

    // Check if any FK has CASCADE or SET NULL/SET DEFAULT action
    let has_cascade_or_set = incoming
        .iter()
        .any(|fk_ref| !matches!(fk_ref.fk.on_update, RefAct::NoAction | RefAct::Restrict));

    if has_cascade_or_set {
        // We have CASCADE or SET NULL/DEFAULT - need to handle them
        let primary_key_is_rowid_alias = table_btree.get_rowid_alias_column().is_some();

        for fk_ref in &incoming {
            if !fk_ref.parent_key_may_change(&updated_positions, table_btree) {
                continue;
            }

            match fk_ref.fk.on_update {
                RefAct::NoAction | RefAct::Restrict => {
                    // Use existing counter-based logic for just this FK
                    // We need to emit check for this specific FK
                    if (primary_key_is_rowid_alias || table_btree.primary_key_columns.is_empty())
                        && fk_ref.parent_uses_rowid
                    {
                        emit_rowid_pk_change_check(
                            program,
                            &[fk_ref.clone()],
                            resolver,
                            old_rowid_reg,
                            rowid_set_clause_reg.unwrap_or(old_rowid_reg),
                        )?;
                    }

                    for index in &indexes {
                        emit_parent_index_key_change_checks(
                            program,
                            cursor_id,
                            start,
                            old_rowid_reg,
                            rowid_new_reg,
                            &[fk_ref.clone()],
                            resolver,
                            table_btree,
                            index.as_ref(),
                        )?;
                    }
                }
                RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault => {
                    // CASCADE, SET NULL, and SET DEFAULT actions are handled by
                    // fire_fk_update_actions AFTER the parent update (not here).
                    // This function only handles NO ACTION/RESTRICT checks.
                }
            }
        }
    } else {
        // All FKs use NoAction/Restrict - use original counter-based approach
        let primary_key_is_rowid_alias = table_btree.get_rowid_alias_column().is_some();

        if primary_key_is_rowid_alias || table_btree.primary_key_columns.is_empty() {
            emit_rowid_pk_change_check(
                program,
                &incoming,
                resolver,
                old_rowid_reg,
                rowid_set_clause_reg.unwrap_or(old_rowid_reg),
            )?;
        }

        for index in indexes {
            emit_parent_index_key_change_checks(
                program,
                cursor_id,
                start,
                old_rowid_reg,
                rowid_new_reg,
                &incoming,
                resolver,
                table_btree,
                index.as_ref(),
            )?;
        }
    }

    Ok(())
}

/// Context for FK action execution: holds register info for OLD/NEW parent key values
#[derive(Debug)]
pub struct FkActionContext {
    /// Registers containing OLD parent key values (for DELETE and UPDATE)
    pub old_key_registers: Vec<usize>,
    /// Registers containing NEW parent key values (for UPDATE only)
    pub new_key_registers: Option<Vec<usize>>,
}

impl FkActionContext {
    pub fn new_for_delete(old_key_registers: Vec<usize>) -> Self {
        Self {
            old_key_registers,
            new_key_registers: None,
        }
    }

    pub fn new_for_update(old_key_registers: Vec<usize>, new_key_registers: Vec<usize>) -> Self {
        Self {
            old_key_registers,
            new_key_registers: Some(new_key_registers),
        }
    }
}

/// Context for compiling FK action subprograms - maps parameter indices to column values
#[derive(Debug)]
struct FkSubprogramContext {
    /// Map from column index to parameter index (1-indexed) for OLD key values
    old_param_start: usize,
    /// Map from column index to parameter index (1-indexed) for NEW key values (UPDATE only)
    new_param_start: Option<usize>,
}

impl FkSubprogramContext {
    fn new(num_cols: usize, has_new: bool) -> Self {
        Self {
            old_param_start: 1,
            new_param_start: if has_new { Some(num_cols + 1) } else { None },
        }
    }

    fn old_param_index(&self, col_idx: usize) -> NonZero<usize> {
        NonZero::new(self.old_param_start + col_idx).expect("param index should be non-zero")
    }

    fn new_param_index(&self, col_idx: usize) -> Option<NonZero<usize>> {
        self.new_param_start
            .map(|start| NonZero::new(start + col_idx).expect("param index should be non-zero"))
    }
}

/// Get the parent column names for an FK reference.
/// Uses primary key columns if parent_columns is empty.
#[inline]
fn get_fk_parent_cols(fk_ref: &ResolvedFkRef, parent_bt: &BTreeTable) -> Vec<String> {
    if fk_ref.fk.parent_columns.is_empty() {
        parent_bt
            .primary_key_columns
            .iter()
            .map(|(n, _)| n.clone())
            .collect()
    } else {
        fk_ref.fk.parent_columns.clone()
    }
}

/// Copy key values from value registers into destination registers.
/// Handles rowid aliasing for columns that are rowid aliases.
fn copy_key_from_values(
    program: &mut ProgramBuilder,
    parent_bt: &BTreeTable,
    parent_cols: &[String],
    values_start: usize,
    rowid_reg: usize,
    dest_start: usize,
) -> Result<()> {
    for (i, pcol) in parent_cols.iter().enumerate() {
        let src = if ROWID_STRS.iter().any(|s| pcol.eq_ignore_ascii_case(s)) {
            rowid_reg
        } else {
            let (pos, col) = parent_bt
                .get_column(pcol)
                .ok_or_else(|| LimboError::InternalError(format!("col {pcol} missing")))?;
            if col.is_rowid_alias() {
                rowid_reg
            } else {
                values_start + pos
            }
        };
        program.emit_insn(Insn::Copy {
            src_reg: src,
            dst_reg: dest_start + i,
            extra_amount: 0,
        });
    }
    Ok(())
}

/// Emit instructions to detect if key values have changed between old and new registers.
/// Jumps to `skip_label` if all values are equal, falls through to `changed_label` if any differ.
fn emit_key_change_check(
    program: &mut ProgramBuilder,
    old_key_start: usize,
    new_key_start: usize,
    ncols: usize,
    skip_label: BranchOffset,
    changed_label: BranchOffset,
) {
    for i in 0..ncols {
        let next = if i + 1 == ncols {
            None
        } else {
            Some(program.allocate_label())
        };
        program.emit_insn(Insn::Eq {
            lhs: old_key_start + i,
            rhs: new_key_start + i,
            target_pc: next.unwrap_or(skip_label),
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: changed_label,
        });
        if let Some(n) = next {
            program.preassign_label_to_next_insn(n);
        }
    }
}

/// Common options for FK action subprogram builders.
const FK_SUBPROGRAM_OPTS: ProgramBuilderOpts = ProgramBuilderOpts {
    num_cursors: 2,
    approx_num_insns: 32,
    approx_num_labels: 4,
};

/// Compile and emit an FK action as a sub-program.
/// This is the common implementation for CASCADE DELETE, SET NULL, SET DEFAULT, and CASCADE UPDATE.
fn emit_fk_action_subprogram(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    connection: &Arc<Connection>,
    stmt: ast::Stmt,
    ctx: &FkActionContext,
    description: &'static str,
) -> Result<()> {
    let mut subprogram_builder = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        program.capture_data_changes_info().clone(),
        FK_SUBPROGRAM_OPTS,
    );
    subprogram_builder.prologue();
    subprogram_builder =
        translate_inner(stmt, resolver, subprogram_builder, connection, description)?;
    subprogram_builder.epilogue(resolver.schema);
    let built_subprogram = subprogram_builder.build(connection.clone(), true, description)?;

    // Build params: OLD key register indices, then optionally NEW key register indices
    let mut params: Vec<Value> = ctx
        .old_key_registers
        .iter()
        .copied()
        .map(|reg_idx| Value::from_i64(reg_idx as i64))
        .collect();

    if let Some(new_regs) = &ctx.new_key_registers {
        params.extend(
            new_regs
                .iter()
                .copied()
                .map(|reg_idx| Value::from_i64(reg_idx as i64)),
        );
    }

    let turso_stmt = Statement::new(
        built_subprogram,
        connection.pager.load().clone(),
        QueryMode::Normal,
    );
    program.emit_insn(Insn::Program {
        params,
        program: Arc::new(RwLock::new(turso_stmt)),
    });

    Ok(())
}

/// Generate a DELETE statement AST for CASCADE DELETE:
/// DELETE FROM child_table WHERE fk_col1 = ?1 AND fk_col2 = ?2 ...
fn generate_cascade_delete_stmt(
    child_table: &str,
    child_cols: &[String],
    ctx: &FkSubprogramContext,
) -> ast::Stmt {
    ast::Stmt::Delete {
        with: None,
        tbl_name: QualifiedName {
            db_name: None,
            name: Name::from_string(child_table),
            alias: None,
        },
        indexed: None,
        where_clause: Some(Box::new(build_fk_match_where_clause(child_cols, ctx))),
        returning: vec![],
        order_by: vec![],
        limit: None,
    }
}

/// Generate an UPDATE statement AST for SET NULL:
/// UPDATE child_table SET fk_col1 = NULL, fk_col2 = NULL ... WHERE fk_col1 = ?1 AND fk_col2 = ?2 ...
fn generate_set_null_stmt(
    child_table: &str,
    child_cols: &[String],
    ctx: &FkSubprogramContext,
) -> ast::Stmt {
    // Build SET clause: fk_col1 = NULL, fk_col2 = NULL ...
    let sets: Vec<ast::Set> = child_cols
        .iter()
        .map(|col| ast::Set {
            col_names: vec![Name::from_string(col)],
            expr: Box::new(Expr::Literal(Literal::Null)),
        })
        .collect();
    ast::Stmt::Update(ast::Update {
        with: None,
        or_conflict: None,
        tbl_name: QualifiedName {
            db_name: None,
            name: Name::from_string(child_table),
            alias: None,
        },
        indexed: None,
        sets,
        from: None,
        where_clause: Some(Box::new(build_fk_match_where_clause(child_cols, ctx))),
        returning: vec![],
        order_by: vec![],
        limit: None,
    })
}

/// Generate an UPDATE statement AST for SET DEFAULT:
/// UPDATE child_table SET fk_col1 = default1, fk_col2 = default2 ... WHERE fk_col1 = ?old1 AND fk_col2 = ?old2 ...
fn generate_set_default_stmt(
    child_table: &BTreeTable,
    child_cols: &[String],
    ctx: &FkSubprogramContext,
) -> ast::Stmt {
    // Build SET clause: if no default is defined for a column, we use NULL
    let sets: Vec<ast::Set> = child_cols
        .iter()
        .map(|col| {
            let default_expr = child_table
                .get_column(col)
                .and_then(|(_, c)| c.default.as_ref())
                .map(|d| (**d).clone())
                .unwrap_or(Expr::Literal(Literal::Null));
            ast::Set {
                col_names: vec![Name::from_string(col)],
                expr: Box::new(default_expr),
            }
        })
        .collect();

    ast::Stmt::Update(ast::Update {
        with: None,
        or_conflict: None,
        tbl_name: QualifiedName {
            db_name: None,
            name: Name::from_string(&child_table.name),
            alias: None,
        },
        indexed: None,
        sets,
        from: None,
        where_clause: Some(Box::new(build_fk_match_where_clause(child_cols, ctx))),
        returning: vec![],
        order_by: vec![],
        limit: None,
    })
}

/// Generate an UPDATE statement AST for CASCADE UPDATE:
/// UPDATE child_table SET fk_col1 = ?new1, fk_col2 = ?new2 ... WHERE fk_col1 = ?old1 AND fk_col2 = ?old2 ...
fn generate_cascade_update_stmt(
    child_table: &str,
    child_cols: &[String],
    ctx: &FkSubprogramContext,
) -> ast::Stmt {
    // Build SET clause
    let sets: Vec<ast::Set> = child_cols
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let param_idx = ctx
                .new_param_index(i)
                .expect("new params required for cascade update");
            ast::Set {
                col_names: vec![Name::from_string(col)],
                expr: Box::new(Expr::Variable(format!("{}", param_idx.get()))),
            }
        })
        .collect();

    let where_clause = build_fk_match_where_clause(child_cols, ctx);
    ast::Stmt::Update(ast::Update {
        with: None,
        or_conflict: None,
        tbl_name: QualifiedName {
            db_name: None,
            name: Name::from_string(child_table),
            alias: None,
        },
        indexed: None,
        sets,
        from: None,
        where_clause: Some(Box::new(where_clause)),
        returning: vec![],
        order_by: vec![],
        limit: None,
    })
}

/// Build a WHERE clause that matches FK columns to parameter values:
/// fk_col1 = ?1 AND fk_col2 = ?2 ...
fn build_fk_match_where_clause(child_cols: &[String], ctx: &FkSubprogramContext) -> Expr {
    let mut conditions: Vec<Expr> = Vec::with_capacity(child_cols.len());

    for (i, col) in child_cols.iter().enumerate() {
        let param_idx = ctx.old_param_index(i);
        let cond = Expr::Binary(
            Box::new(Expr::Id(Name::from_string(col))),
            ast::Operator::Equals,
            Box::new(Expr::Variable(format!("{}", param_idx.get()))),
        );
        conditions.push(cond);
    }

    // Combine the clauses with AND
    if conditions.len() == 1 {
        conditions.remove(0)
    } else {
        conditions
            .into_iter()
            .reduce(|acc, cond| Expr::Binary(Box::new(acc), ast::Operator::And, Box::new(cond)))
            .expect("at least one condition")
    }
}

/// Compile and emit an FK CASCADE DELETE action as a sub-program.
/// This creates a sub-program that deletes all child rows matching the parent key.
fn fire_fk_cascade_delete(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    fk_ref: &ResolvedFkRef,
    connection: &Arc<Connection>,
    ctx: &FkActionContext,
) -> Result<()> {
    let child_cols = &fk_ref.fk.child_columns;
    let subprog_ctx = FkSubprogramContext::new(child_cols.len(), false);
    let stmt = generate_cascade_delete_stmt(&fk_ref.child_table.name, child_cols, &subprog_ctx);
    emit_fk_action_subprogram(
        program,
        resolver,
        connection,
        stmt,
        ctx,
        "fk cascade delete",
    )
}

/// Compile and emit an FK SET NULL action as a sub-program.
/// This creates a sub-program that sets FK columns to NULL for all matching child rows.
fn fire_fk_set_null(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    fk_ref: &ResolvedFkRef,
    connection: &Arc<Connection>,
    ctx: &FkActionContext,
) -> Result<()> {
    let child_cols = &fk_ref.fk.child_columns;
    let subprog_ctx = FkSubprogramContext::new(child_cols.len(), false);
    let stmt = generate_set_null_stmt(&fk_ref.child_table.name, child_cols, &subprog_ctx);
    emit_fk_action_subprogram(program, resolver, connection, stmt, ctx, "fk set null")
}

/// Compile and emit an FK SET DEFAULT action as a sub-program.
/// This creates a sub-program that sets FK columns to their default values for all matching child rows.
fn fire_fk_set_default(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    fk_ref: &ResolvedFkRef,
    connection: &Arc<Connection>,
    ctx: &FkActionContext,
) -> Result<()> {
    let child_cols = &fk_ref.fk.child_columns;
    let subprog_ctx = FkSubprogramContext::new(child_cols.len(), false);
    let stmt = generate_set_default_stmt(&fk_ref.child_table, child_cols, &subprog_ctx);
    emit_fk_action_subprogram(program, resolver, connection, stmt, ctx, "fk set default")
}

/// Compile and emit an FK CASCADE UPDATE action as a sub-program.
/// This creates a sub-program that updates FK columns to new values for all matching child rows.
fn fire_fk_cascade_update(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    fk_ref: &ResolvedFkRef,
    connection: &Arc<Connection>,
    ctx: &FkActionContext,
) -> Result<()> {
    let child_cols = &fk_ref.fk.child_columns;
    // CASCADE UPDATE needs new params for the SET clause
    let subprog_ctx = FkSubprogramContext::new(child_cols.len(), true);
    let stmt = generate_cascade_update_stmt(&fk_ref.child_table.name, child_cols, &subprog_ctx);
    emit_fk_action_subprogram(
        program,
        resolver,
        connection,
        stmt,
        ctx,
        "fk cascade update",
    )
}

/// Fire FK actions for DELETE on parent table using Program opcode.
/// This is called after the DELETE is performed but before AFTER triggers.
pub fn fire_fk_delete_actions(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    parent_table_name: &str,
    parent_cursor_id: usize,
    parent_rowid_reg: usize,
    connection: &Arc<Connection>,
) -> Result<()> {
    let parent_bt = resolver
        .schema
        .get_btree_table(parent_table_name)
        .ok_or_else(|| LimboError::InternalError("parent not btree".into()))?;

    for fk_ref in resolver
        .schema
        .resolved_fks_referencing(parent_table_name)?
    {
        let parent_cols = get_fk_parent_cols(&fk_ref, &parent_bt);
        let ncols = parent_cols.len();
        let key_regs_start = program.alloc_registers(ncols);

        build_parent_key(
            program,
            &parent_bt,
            &parent_cols,
            parent_cursor_id,
            parent_rowid_reg,
            key_regs_start,
        )?;

        let old_key_registers: Vec<usize> = (key_regs_start..key_regs_start + ncols).collect();
        let ctx = FkActionContext::new_for_delete(old_key_registers);

        match fk_ref.fk.on_delete {
            RefAct::NoAction | RefAct::Restrict => {
                emit_fk_delete_parent_existence_check_single(
                    program,
                    resolver,
                    &fk_ref,
                    &parent_bt,
                    parent_table_name,
                    parent_cursor_id,
                    parent_rowid_reg,
                )?;
            }
            RefAct::Cascade => {
                fire_fk_cascade_delete(program, resolver, &fk_ref, connection, &ctx)?;
            }
            RefAct::SetNull => {
                fire_fk_set_null(program, resolver, &fk_ref, connection, &ctx)?;
            }
            RefAct::SetDefault => {
                fire_fk_set_default(program, resolver, &fk_ref, connection, &ctx)?;
            }
        }
    }

    Ok(())
}

/// Fire FK actions for UPDATE on parent table using Program opcode.
/// This is called after the UPDATE is performed but before AFTER triggers.
/// `old_values_start` is the register where OLD column values are stored (loaded before Delete+Insert).
#[allow(clippy::too_many_arguments)]
pub fn fire_fk_update_actions(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    parent_table_name: &str,
    old_rowid_reg: usize,
    old_values_start: usize,
    new_values_start: usize,
    new_rowid_reg: usize,
    connection: &Arc<Connection>,
) -> Result<()> {
    let parent_bt = resolver
        .schema
        .get_btree_table(parent_table_name)
        .ok_or_else(|| LimboError::InternalError("parent not btree".into()))?;

    for fk_ref in resolver
        .schema
        .resolved_fks_referencing(parent_table_name)?
    {
        let parent_cols = get_fk_parent_cols(&fk_ref, &parent_bt);
        let ncols = parent_cols.len();

        // Copy OLD and NEW parent key values using the helper
        let old_key_start = program.alloc_registers(ncols);
        copy_key_from_values(
            program,
            &parent_bt,
            &parent_cols,
            old_values_start,
            old_rowid_reg,
            old_key_start,
        )?;

        let new_key_start = program.alloc_registers(ncols);
        copy_key_from_values(
            program,
            &parent_bt,
            &parent_cols,
            new_values_start,
            new_rowid_reg,
            new_key_start,
        )?;

        let old_key_registers: Vec<usize> = (old_key_start..old_key_start + ncols).collect();
        let new_key_registers: Vec<usize> = (new_key_start..new_key_start + ncols).collect();

        // Check if parent key changed - skip action if all values are equal
        let skip_action = program.allocate_label();
        let key_changed = program.allocate_label();
        emit_key_change_check(
            program,
            old_key_start,
            new_key_start,
            ncols,
            skip_action,
            key_changed,
        );

        program.preassign_label_to_next_insn(key_changed);

        let ctx = FkActionContext::new_for_update(old_key_registers, new_key_registers);

        match fk_ref.fk.on_update {
            RefAct::NoAction | RefAct::Restrict => {
                // NO ACTION/RESTRICT checks are handled by emit_fk_update_parent_actions
                // which is called BEFORE the update using the counter-based approach.
            }
            RefAct::Cascade => {
                fire_fk_cascade_update(program, resolver, &fk_ref, connection, &ctx)?;
            }
            RefAct::SetNull => {
                fire_fk_set_null(program, resolver, &fk_ref, connection, &ctx)?;
            }
            RefAct::SetDefault => {
                fire_fk_set_default(program, resolver, &fk_ref, connection, &ctx)?;
            }
        }

        program.preassign_label_to_next_insn(skip_action);
    }

    Ok(())
}

/// Emit pre-DROP FK checks and actions for a table.
///
/// SQLite's algorithm:
/// 1. Collect all parent rowids into a RowSet for iteration
/// 2. For each parent rowid:
///    - Seek to parent row and extract parent key column values
///    - Based on the ON DELETE action:
///      - RESTRICT/NO ACTION: Scan child table for matching FK values and count violations
///      - CASCADE/SET NULL/SET DEFAULT: Take appropriate action for child rows that reference the parent
///
/// Only fails if a child row actually references an existing parent row being deleted
/// and the action is RESTRICT/NO ACTION. Correctly handles orphaned FK values.
pub fn emit_fk_drop_table_check(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    parent_table_name: &str,
    connection: &Arc<Connection>,
) -> Result<()> {
    let parent_tbl = resolver
        .schema
        .get_btree_table(parent_table_name)
        .ok_or_else(|| {
            LimboError::InternalError(format!("parent table {parent_table_name} not found"))
        })?;

    // Get all FK references to this parent table
    let fk_refs = resolver
        .schema
        .resolved_fks_referencing(parent_table_name)?;

    if fk_refs.is_empty() {
        return Ok(());
    }

    // Separate FK refs by action type:
    // - action_fk_refs: CASCADE, SET NULL, SET DEFAULT - need to fire action subprograms
    // - check_fk_refs: RESTRICT, NO ACTION - need violation counting
    let action_fk_refs: Vec<_> = fk_refs
        .iter()
        .filter(|fk| {
            matches!(
                fk.fk.on_delete,
                RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault
            )
        })
        .collect();
    let check_fk_refs: Vec<_> = fk_refs
        .iter()
        .filter(|fk| matches!(fk.fk.on_delete, RefAct::Restrict | RefAct::NoAction))
        .collect();

    // Collect all parent rowids into a RowSet
    // r[rowset_reg] = NULL (initializes RowSet)
    let rowset_reg = program.alloc_register();
    program.emit_null(rowset_reg, None);

    let parent_cur = open_read_table(program, &parent_tbl);
    let collect_done = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: parent_cur,
        pc_if_empty: collect_done,
    });

    let collect_loop = program.allocate_label();
    program.preassign_label_to_next_insn(collect_loop);

    // Get parent rowid and add to RowSet
    let parent_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: parent_cur,
        dest: parent_rowid_reg,
    });
    program.emit_insn(Insn::RowSetAdd {
        rowset_reg,
        value_reg: parent_rowid_reg,
    });

    program.emit_insn(Insn::Next {
        cursor_id: parent_cur,
        pc_if_next: collect_loop,
    });

    program.preassign_label_to_next_insn(collect_done);
    program.emit_insn(Insn::Close {
        cursor_id: parent_cur,
    });

    // For each parent rowid, check/execute FK actions
    let parent_write_cur = program.alloc_cursor_id(CursorType::BTreeTable(parent_tbl.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: parent_write_cur,
        root_page: parent_tbl.root_page.into(),
        db: 0,
    });

    let rowset_done = program.allocate_label();
    let rowset_loop = program.allocate_label();
    program.preassign_label_to_next_insn(rowset_loop);
    // Read next rowid from RowSet
    let current_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowSetRead {
        rowset_reg,
        pc_if_empty: rowset_done,
        dest_reg: current_rowid_reg,
    });

    // Verify row still exists, jumps if not found
    let skip_row = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: parent_write_cur,
        rowid_reg: current_rowid_reg,
        target_pc: skip_row,
    });

    // Fire FK actions for CASCADE, SET NULL, SET DEFAULT
    for fk_ref in &action_fk_refs {
        let parent_cols = get_fk_parent_cols(fk_ref, &parent_tbl);
        let ncols = parent_cols.len();
        let key_regs_start = program.alloc_registers(ncols);

        build_parent_key(
            program,
            &parent_tbl,
            &parent_cols,
            parent_write_cur,
            current_rowid_reg,
            key_regs_start,
        )?;

        let old_key_registers: Vec<usize> = (key_regs_start..key_regs_start + ncols).collect();
        let ctx = FkActionContext::new_for_delete(old_key_registers);

        match fk_ref.fk.on_delete {
            RefAct::Cascade => {
                fire_fk_cascade_delete(program, resolver, fk_ref, connection, &ctx)?;
            }
            RefAct::SetNull => {
                fire_fk_set_null(program, resolver, fk_ref, connection, &ctx)?;
            }
            RefAct::SetDefault => {
                fire_fk_set_default(program, resolver, fk_ref, connection, &ctx)?;
            }
            RefAct::NoAction | RefAct::Restrict => {
                // These are handled below in the check_fk_refs loop
            }
        }
    }

    // For RESTRICT/NO ACTION FKs, scan child table for matching rows and count violations
    for fk_ref in &check_fk_refs {
        let child_tbl = &fk_ref.child_table;
        let child_cols = &fk_ref.fk.child_columns;

        // Determine which parent columns are referenced
        let parent_cols = get_fk_parent_cols(fk_ref, &parent_tbl);
        let ncols = parent_cols.len();

        // Build the parent key vector from the current parent row
        let parent_key_start = program.alloc_registers(ncols);
        build_parent_key(
            program,
            &parent_tbl,
            &parent_cols,
            parent_write_cur,
            current_rowid_reg,
            parent_key_start,
        )?;

        // Scan child table for matching rows
        let child_cur = open_read_table(program, child_tbl);
        let child_done = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: child_cur,
            pc_if_empty: child_done,
        });

        let child_loop = program.allocate_label();
        program.preassign_label_to_next_insn(child_loop);
        let child_next = program.allocate_label();

        // Compare each FK column to corresponding parent key column
        // All columns must match for a violation
        for (i, cname) in child_cols.iter().enumerate() {
            let (pos, _) = child_tbl
                .get_column(cname)
                .ok_or_else(|| LimboError::InternalError(format!("child col {cname} missing")))?;

            let child_val_reg = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: child_cur,
                column: pos,
                dest: child_val_reg,
                default: None,
            });
            // If child FK column is NULL, skip (no reference)
            program.emit_insn(Insn::IsNull {
                reg: child_val_reg,
                target_pc: child_next,
            });

            // Compare child FK column to corresponding parent key column
            program.emit_insn(Insn::Ne {
                lhs: child_val_reg,
                rhs: parent_key_start + i,
                target_pc: child_next,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: Some(CollationSeq::Binary),
            });
        }

        // If we reach here, all FK columns match: increment violation counter
        program.emit_insn(Insn::FkCounter {
            increment_value: 1,
            deferred: false,
        });

        program.preassign_label_to_next_insn(child_next);
        program.emit_insn(Insn::Next {
            cursor_id: child_cur,
            pc_if_next: child_loop,
        });

        program.preassign_label_to_next_insn(child_done);
        program.emit_insn(Insn::Close {
            cursor_id: child_cur,
        });
    }

    // Note: SQLite deletes the parent row here, but we skip that since
    // the actual deletion happens later in the DROP TABLE logic
    program.preassign_label_to_next_insn(skip_row);
    program.emit_insn(Insn::Goto {
        target_pc: rowset_loop,
    });

    // After processing all rows, check if there were any violations
    program.preassign_label_to_next_insn(rowset_done);
    program.emit_insn(Insn::Close {
        cursor_id: parent_write_cur,
    });

    // Only check for violations if there are RESTRICT/NO ACTION FKs
    if !check_fk_refs.is_empty() {
        // FkIfZero: if counter == 0, skip the halt
        let no_violations = program.allocate_label();
        program.emit_insn(Insn::FkIfZero {
            deferred: false,
            target_pc: no_violations,
        });

        // There were violations, halt with FK error
        emit_fk_restrict_halt(program)?;
        program.preassign_label_to_next_insn(no_violations);
    }

    Ok(())
}
