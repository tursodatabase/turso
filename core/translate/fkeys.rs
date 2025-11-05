use turso_parser::ast::Expr;

use super::ProgramBuilder;
use crate::{
    schema::{BTreeTable, ForeignKey, Index, ResolvedFkRef, ROWID_SENTINEL},
    translate::{emitter::Resolver, planner::ROWID_STRS},
    vdbe::{
        builder::CursorType,
        insn::{CmpInsFlags, Insn},
        BranchOffset,
    },
    Result,
};
use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

#[inline]
pub fn emit_guarded_fk_decrement(program: &mut ProgramBuilder, label: BranchOffset) {
    program.emit_insn(Insn::FkIfZero {
        deferred: true,
        target_pc: label,
    });
    program.emit_insn(Insn::FkCounter {
        increment_value: -1,
        deferred: true,
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
        let (pos, _) = child_tbl.get_column(cname).ok_or_else(|| {
            crate::LimboError::InternalError(format!("child col {cname} missing"))
        })?;
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
            collation: Some(super::collate::CollationSeq::Binary),
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
pub fn emit_fk_violation(program: &mut ProgramBuilder, fk: &ForeignKey) -> Result<()> {
    program.emit_insn(Insn::FkCounter {
        increment_value: 1,
        deferred: fk.deferred,
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
            .ok_or_else(|| crate::LimboError::InternalError(format!("pk col {pk_name} missing")))?;
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
    emit_fk_parent_pk_change_counters(program, incoming, resolver, old_key, new_key, idx_len)?;
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

    let on_match = |p: &mut ProgramBuilder| -> Result<()> {
        match (is_deferred, pass) {
            // OLD key referenced by a child
            (_, ParentProbePass::Old) => {
                emit_fk_violation(p, &fk_ref.fk)?;
            }

            // NEW key referenced by a child (cancel one deferred violation)
            (true, ParentProbePass::New) => {
                // Guard to avoid underflow if OLD pass didn't increment.
                let skip = p.allocate_label();
                emit_guarded_fk_decrement(p, skip);
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
                .ok_or_else(|| crate::LimboError::InternalError(format!("col {pcol} missing")))?;
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
) -> crate::Result<()> {
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
                    emit_guarded_fk_decrement(program, skip);
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
                            emit_guarded_fk_decrement(p, skip);
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

/// Prevent deleting a parent row that is still referenced by any child.
/// For each incoming FK referencing `parent_table_name`:
///   1. Build the parent key vector from the current parent row (FK parent-column order,
///      or the table's PK columns when the FK omits parent columns).
///   2. Look for referencing child rows:
///        - Prefer an exact child index on (child_columns...). If found, probe the index.
///        - Otherwise scan the child table. For self-referential FKs, exclude the current rowid.
///   3. If a referencing child exists:
///        - Immediate FK: HALT with SQLITE_CONSTRAINT_FOREIGNKEY
///        - Deferred FK: FkCounter +1
pub fn emit_fk_delete_parent_existence_checks(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    parent_table_name: &str,
    parent_cursor_id: usize,
    parent_rowid_reg: usize,
) -> Result<()> {
    let parent_bt = resolver
        .schema
        .get_btree_table(parent_table_name)
        .ok_or_else(|| crate::LimboError::InternalError("parent not btree".into()))?;

    for fk_ref in resolver
        .schema
        .resolved_fks_referencing(parent_table_name)?
    {
        let is_self_ref = fk_ref
            .child_table
            .name
            .eq_ignore_ascii_case(parent_table_name);

        // Build parent key in FK's parent-column order (or table PK columns if unspecified).
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
            &parent_bt,
            &parent_cols,
            parent_cursor_id,
            parent_rowid_reg,
            parent_key_start,
        )?;

        // Try an exact child index on (child_columns...) if available and not self-ref
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

        if let Some(idx) = child_idx {
            // Index probe: FOUND => violation; NOT FOUND => ok.
            let icur = open_read_index(program, idx);
            let probe =
                copy_with_affinity(program, parent_key_start, ncols, idx, &fk_ref.child_table);

            index_probe(
                program,
                icur,
                probe,
                ncols,
                |p| {
                    emit_fk_violation(p, &fk_ref.fk)?;
                    Ok(())
                },
                |_p| Ok(()),
            )?;
        } else {
            // Table scan fallback; for self-ref, exclude the same parent row by rowid.
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
                    emit_fk_violation(p, &fk_ref.fk)?;
                    Ok(())
                },
            )?;
        }
    }
    Ok(())
}
