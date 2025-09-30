use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::{collections::HashMap, sync::Arc};

use turso_parser::ast::{self, Upsert};

use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::insert::format_unique_violation_desc;
use crate::translate::planner::ROWID_STRS;
use crate::vdbe::insn::CmpInsFlags;
use crate::{
    bail_parse_error,
    error::SQLITE_CONSTRAINT_NOTNULL,
    schema::{Index, IndexColumn, Schema, Table},
    translate::{
        emitter::{
            emit_cdc_full_record, emit_cdc_insns, emit_cdc_patch_record, OperationMode, Resolver,
        },
        expr::{
            emit_returning_results, translate_expr, translate_expr_no_constant_opt, walk_expr_mut,
            NoConstantOptReason, ReturningValueRegisters,
        },
        insert::Insertion,
        plan::ResultSetColumn,
    },
    util::normalize_ident,
    vdbe::{
        builder::ProgramBuilder,
        insn::{IdxInsertFlags, InsertFlags, Insn},
        BranchOffset,
    },
};

// The following comment is copied directly from SQLite source and should be used as a guiding light
// whenever we encounter compatibility bugs related to conflict clause handling:

/* UNIQUE and PRIMARY KEY constraints should be handled in the following
** order:
**
**   (1)  OE_Update
**   (2)  OE_Abort, OE_Fail, OE_Rollback, OE_Ignore
**   (3)  OE_Replace
**
** OE_Fail and OE_Ignore must happen before any changes are made.
** OE_Update guarantees that only a single row will change, so it
** must happen before OE_Replace.  Technically, OE_Abort and OE_Rollback
** could happen in any order, but they are grouped up front for
** convenience.
**
** 2018-08-14: Ticket https://www.sqlite.org/src/info/908f001483982c43
** The order of constraints used to have OE_Update as (2) and OE_Abort
** and so forth as (1). But apparently PostgreSQL checks the OE_Update
** constraint before any others, so it had to be moved.
**
** Constraint checking code is generated in this order:
**   (A)  The rowid constraint
**   (B)  Unique index constraints that do not have OE_Replace as their
**        default conflict resolution strategy
**   (C)  Unique index that do use OE_Replace by default.
**
** The ordering of (2) and (3) is accomplished by making sure the linked
** list of indexes attached to a table puts all OE_Replace indexes last
** in the list.  See sqlite3CreateIndex() for where that happens.
*/

/// A ConflictTarget is extracted from each ON CONFLICT target,
// e.g. INSERT INTO x(a) ON CONFLICT  *(a COLLATE nocase)*
#[derive(Debug, Clone)]
pub struct ConflictTarget {
    /// The normalized column name in question
    col_name: String,
    /// Possible collation name, normalized to lowercase
    collate: Option<String>,
}

// Extract `(column, optional_collate)` from an ON CONFLICT target Expr.
// Accepts: Id, Qualified, DoublyQualified, Parenthesized, Collate
fn extract_target_key(e: &ast::Expr) -> Option<ConflictTarget> {
    match e {
        ast::Expr::Collate(inner, c) => {
            let mut tk = extract_target_key(inner.as_ref())?;
            let cstr = c.as_str();
            tk.collate = Some(cstr.to_ascii_lowercase());
            Some(tk)
        }
        ast::Expr::Parenthesized(v) if v.len() == 1 => extract_target_key(&v[0]),

        ast::Expr::Id(name) => Some(ConflictTarget {
            col_name: normalize_ident(name.as_str()),
            collate: None,
        }),
        // t.a or db.t.a: accept ident or quoted in the column position
        ast::Expr::Qualified(_, col) | ast::Expr::DoublyQualified(_, _, col) => {
            let cname = col.as_str();
            Some(ConflictTarget {
                col_name: normalize_ident(cname),
                collate: None,
            })
        }
        _ => None,
    }
}

// Return the index key’s effective collation.
// If `idx_col.collation` is None, fall back to the column default or "BINARY".
fn effective_collation_for_index_col(idx_col: &IndexColumn, table: &Table) -> String {
    if let Some(c) = idx_col.collation.as_ref() {
        return c.to_string().to_ascii_lowercase();
    }
    // Otherwise use the table default, or default to BINARY
    table
        .get_column_by_name(&idx_col.name)
        .map(|s| {
            s.1.collation
                .map(|c| c.to_string().to_ascii_lowercase())
                .unwrap_or_else(|| "binary".to_string())
        })
        .unwrap_or_else(|| "binary".to_string())
}

/// Match ON CONFLICT target to the PRIMARY KEY/rowid alias.
pub fn upsert_matches_rowid_alias(upsert: &Upsert, table: &Table) -> bool {
    let Some(t) = upsert.index.as_ref() else {
        // omitted target matches everything, CatchAll handled elsewhere
        return false;
    };
    if t.targets.len() != 1 {
        return false;
    }
    // Only treat as PK if the PK is the rowid alias (INTEGER PRIMARY KEY)
    let pk = table.columns().iter().find(|c| c.is_rowid_alias);
    if let Some(pkcol) = pk {
        extract_target_key(&t.targets[0].expr).is_some_and(|tk| {
            tk.col_name
                .eq_ignore_ascii_case(pkcol.name.as_ref().unwrap_or(&String::new()))
        })
    } else {
        false
    }
}

/// Returns array of chaned column indicies and whether rowid was changed.
fn collect_changed_cols(
    table: &Table,
    set_pairs: &[(usize, Box<ast::Expr>)],
) -> (HashSet<usize>, bool) {
    let mut cols_changed = HashSet::with_capacity(table.columns().len());
    let mut rowid_changed = false;
    for (col_idx, _) in set_pairs {
        if let Some(c) = table.columns().get(*col_idx) {
            if c.is_rowid_alias {
                rowid_changed = true;
            } else {
                cols_changed.insert(*col_idx);
            }
        }
    }
    (cols_changed, rowid_changed)
}

#[inline]
fn upsert_index_is_affected(
    table: &Table,
    idx: &Index,
    changed_cols: &HashSet<usize>,
    rowid_changed: bool,
) -> bool {
    if rowid_changed {
        return true;
    }
    let km = index_keys(idx);
    let pm = partial_index_cols(idx, table);
    for c in km.iter().chain(pm.iter()) {
        if changed_cols.contains(c) {
            return true;
        }
    }
    false
}

/// Columns used by index key
#[inline]
fn index_keys(idx: &Index) -> Vec<usize> {
    idx.columns.iter().map(|ic| ic.pos_in_table).collect()
}

/// Columns referenced by the partial WHERE (empty if none).
fn partial_index_cols(idx: &Index, table: &Table) -> HashSet<usize> {
    use ast::Expr;
    let Some(expr) = &idx.where_clause else {
        return HashSet::new();
    };
    let mut out = HashSet::new();
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> crate::Result<WalkControl> {
        match e {
            Expr::Id(n) => {
                if let Some((i, _)) = table.get_column_by_name(&normalize_ident(n.as_str())) {
                    out.insert(i);
                }
            }
            Expr::Qualified(ns, c) | Expr::DoublyQualified(_, ns, c) => {
                // Only count columns that belong to this table
                let nsn = normalize_ident(ns.as_str());
                let tname = normalize_ident(table.get_name());
                if nsn.eq_ignore_ascii_case(&tname) {
                    if let Some((i, _)) = table.get_column_by_name(&normalize_ident(c.as_str())) {
                        out.insert(i);
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    out
}

/// Match ON CONFLICT target to a UNIQUE index, *ignoring order* but requiring
/// exact coverage (same column multiset). If the target specifies a COLLATED
/// column, the collation must match the index column's effective collation.
/// If the target omits collation, any index collation is accepted.
/// Partial (WHERE) indexes never match.
pub fn upsert_matches_index(upsert: &Upsert, index: &Index, table: &Table) -> bool {
    let Some(target) = upsert.index.as_ref() else {
        return true;
    };
    // must be a non-partial UNIQUE index with identical arity
    if !index.unique || index.where_clause.is_some() || target.targets.len() != index.columns.len()
    {
        return false;
    }

    // Build a multiset of index columns: (normalized name, effective collation)
    // effective collation = index collation if set, else table column default, else "binary"
    let mut idx_cols: Vec<(String, String)> = index
        .columns
        .iter()
        .map(|ic| {
            (
                normalize_ident(&ic.name),
                effective_collation_for_index_col(ic, table),
            )
        })
        .collect();
    // For each target key, locate a matching index column (name equal ignoring case,
    // and collation equal iff the target specifies one). Consume each match once.
    for te in &target.targets {
        let Some(tk) = extract_target_key(&te.expr) else {
            return false;
        };
        let tname = tk.col_name;
        let mut found = None;

        for (i, (iname, icoll)) in idx_cols.iter().enumerate() {
            if tname.eq_ignore_ascii_case(iname)
                && match tk.collate.as_ref() {
                    Some(c) => c.eq_ignore_ascii_case(icoll),
                    None => true, // unspecified collation -> accept any
                }
            {
                found = Some(i);
                break;
            }
        }
        if let Some(i) = found {
            // consume this index column once (multiset match)
            idx_cols.swap_remove(i);
        } else {
            return false;
        }
    }
    // All target columns matched exactly once
    idx_cols.is_empty()
}

#[derive(Clone)]
pub enum ResolvedUpsertTarget {
    // ON CONFLICT DO
    CatchAll,
    // ON CONFLICT(pk) DO
    PrimaryKey,
    // matched this non-partial UNIQUE index
    Index(Arc<Index>),
}

pub fn resolve_upsert_target(
    schema: &Schema,
    table: &Table,
    upsert: &Upsert,
) -> crate::Result<ResolvedUpsertTarget> {
    // Omitted target, catch-all
    if upsert.index.is_none() {
        return Ok(ResolvedUpsertTarget::CatchAll);
    }

    // Targeted: must match PK, only if PK is a rowid alias
    if upsert_matches_rowid_alias(upsert, table) {
        return Ok(ResolvedUpsertTarget::PrimaryKey);
    }

    // Otherwise match a UNIQUE index, also covering non-rowid PRIMARY KEYs
    for idx in schema.get_indices(table.get_name()) {
        if idx.unique && upsert_matches_index(upsert, idx, table) {
            return Ok(ResolvedUpsertTarget::Index(Arc::clone(idx)));
        }
    }
    crate::bail_parse_error!(
        "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"
    );
}

#[allow(clippy::too_many_arguments)]
/// Emit the bytecode to implement the `DO UPDATE` arm of an UPSERT.
///
/// This routine is entered after the caller has determined that an INSERT
/// would violate a UNIQUE/PRIMARY KEY constraint and that the user requested
/// `ON CONFLICT ... DO UPDATE`.
///
/// High-level flow:
/// 1. Seek to the conflicting row by rowid and load the current row snapshot
///    into a contiguous set of registers.
/// 2. Optionally duplicate CURRENT into BEFORE* (for index rebuild and CDC).
/// 3. Copy CURRENT into NEW, then evaluate SET expressions into NEW,
///    with all references to the target table columns rewritten to read from
///    the CURRENT registers (per SQLite semantics).
/// 4. Enforce NOT NULL constraints and (if STRICT) type checks on NEW.
/// 5. Rebuild indexes (delete keys using BEFORE, insert keys using NEW).
/// 6. Rewrite the table row payload at the same rowid with NEW.
/// 7. Emit CDC rows and RETURNING output if requested.
/// 8. Jump to `row_done_label`.
///
/// Semantics reference: https://sqlite.org/lang_upsert.html
/// Column references in the DO UPDATE expressions refer to the original
/// (unchanged) row. To refer to would-be inserted values, use `excluded.x`.
pub fn emit_upsert(
    program: &mut ProgramBuilder,
    table: &Table,
    insertion: &Insertion,
    tbl_cursor_id: usize,
    conflict_rowid_reg: usize,
    set_pairs: &mut [(usize, Box<ast::Expr>)],
    where_clause: &mut Option<Box<ast::Expr>>,
    resolver: &Resolver,
    idx_cursors: &[(&String, i64, usize)],
    returning: &mut [ResultSetColumn],
    cdc_cursor_id: Option<usize>,
    row_done_label: BranchOffset,
) -> crate::Result<()> {
    // Seek & snapshot CURRENT
    program.emit_insn(Insn::SeekRowid {
        cursor_id: tbl_cursor_id,
        src_reg: conflict_rowid_reg,
        target_pc: row_done_label,
    });
    let num_cols = table.columns().len();
    let current_start = program.alloc_registers(num_cols);
    for (i, col) in table.columns().iter().enumerate() {
        if col.is_rowid_alias {
            program.emit_insn(Insn::RowId {
                cursor_id: tbl_cursor_id,
                dest: current_start + i,
            });
        } else {
            program.emit_insn(Insn::Column {
                cursor_id: tbl_cursor_id,
                column: i,
                dest: current_start + i,
                default: None,
            });
        }
    }

    // BEFORE for index maintenance / CDC
    let before_start = if cdc_cursor_id.is_some() || !idx_cursors.is_empty() {
        let s = program.alloc_registers(num_cols);
        program.emit_insn(Insn::Copy {
            src_reg: current_start,
            dst_reg: s,
            extra_amount: num_cols - 1,
        });
        Some(s)
    } else {
        None
    };

    // NEW = CURRENT, then apply SET
    let new_start = program.alloc_registers(num_cols);
    program.emit_insn(Insn::Copy {
        src_reg: current_start,
        dst_reg: new_start,
        extra_amount: num_cols - 1,
    });

    // WHERE on target row
    if let Some(pred) = where_clause.as_mut() {
        rewrite_expr_to_registers(
            pred,
            table,
            current_start,
            conflict_rowid_reg,
            Some(table.get_name()),
            Some(insertion),
            true,
        )?;
        let pr = program.alloc_register();
        translate_expr(program, None, pred, pr, resolver)?;
        program.emit_insn(Insn::IfNot {
            reg: pr,
            target_pc: row_done_label,
            jump_if_null: true,
        });
    }

    // Apply SET; capture rowid change if any
    let mut new_rowid_reg: Option<usize> = None;
    for (col_idx, expr) in set_pairs.iter_mut() {
        rewrite_expr_to_registers(
            expr,
            table,
            current_start,
            conflict_rowid_reg,
            Some(table.get_name()),
            Some(insertion),
            true,
        )?;
        translate_expr_no_constant_opt(
            program,
            None,
            expr,
            new_start + *col_idx,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
        let col = &table.columns()[*col_idx];
        if col.notnull && !col.is_rowid_alias {
            program.emit_insn(Insn::HaltIfNull {
                target_reg: new_start + *col_idx,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: String::from(table.get_name()) + col.name.as_ref().unwrap(),
            });
        }
        if col.is_rowid_alias {
            // Must be integer; remember the NEW rowid value
            let r = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: new_start + *col_idx,
                dst_reg: r,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MustBeInt { reg: r });
            new_rowid_reg = Some(r);
        }
    }

    if let Some(bt) = table.btree() {
        if bt.is_strict {
            program.emit_insn(Insn::TypeCheck {
                start_reg: new_start,
                count: num_cols,
                check_generated: true,
                table_reference: Arc::clone(&bt),
            });
        }
    }

    // Index rebuild (DELETE old, INSERT new), honoring partial-index WHEREs
    if let Some(before) = before_start {
        let (changed_cols, rowid_changed) = collect_changed_cols(table, set_pairs);

        for (idx_name, _root, idx_cid) in idx_cursors {
            let idx_meta = resolver
                .schema
                .get_index(table.get_name(), idx_name)
                .expect("index exists");

            if !upsert_index_is_affected(table, idx_meta, &changed_cols, rowid_changed) {
                continue; // skip untouched index completely
            }
            let k = idx_meta.columns.len();

            let before_pred_reg = eval_partial_pred_for_row_image(
                program,
                table,
                idx_meta,
                before,
                conflict_rowid_reg,
                resolver,
            );
            let new_rowid = new_rowid_reg.unwrap_or(conflict_rowid_reg);
            let new_pred_reg = eval_partial_pred_for_row_image(
                program, table, idx_meta, new_start, new_rowid, resolver,
            );

            // Skip delete if BEFORE predicate false/NULL
            let maybe_skip_del = before_pred_reg.map(|r| {
                let lbl = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: r,
                    target_pc: lbl,
                    jump_if_null: true,
                });
                lbl
            });

            // DELETE old key
            let del = program.alloc_registers(k + 1);
            for (i, ic) in idx_meta.columns.iter().enumerate() {
                let (ci, _) = table.get_column_by_name(&ic.name).unwrap();
                program.emit_insn(Insn::Copy {
                    src_reg: before + ci,
                    dst_reg: del + i,
                    extra_amount: 0,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: conflict_rowid_reg,
                dst_reg: del + k,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: del,
                num_regs: k + 1,
                cursor_id: *idx_cid,
                raise_error_if_no_matching_entry: false,
            });
            if let Some(label) = maybe_skip_del {
                program.resolve_label(label, program.offset());
            }

            // Skip insert if NEW predicate false/NULL
            let maybe_skip_ins = new_pred_reg.map(|r| {
                let lbl = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: r,
                    target_pc: lbl,
                    jump_if_null: true,
                });
                lbl
            });

            // INSERT new key (use NEW rowid if present)
            let ins = program.alloc_registers(k + 1);
            for (i, ic) in idx_meta.columns.iter().enumerate() {
                let (ci, _) = table.get_column_by_name(&ic.name).unwrap();
                program.emit_insn(Insn::Copy {
                    src_reg: new_start + ci,
                    dst_reg: ins + i,
                    extra_amount: 0,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: new_rowid,
                dst_reg: ins + k,
                extra_amount: 0,
            });

            let rec = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: ins,
                count: k + 1,
                dest_reg: rec,
                index_name: Some((*idx_name).clone()),
                affinity_str: None,
            });

            if idx_meta.unique {
                // Affinity on the key columns for the NoConflict probe
                let ok = program.allocate_label();
                let aff: String = idx_meta
                    .columns
                    .iter()
                    .map(|c| {
                        table
                            .get_column_by_name(&c.name)
                            .map(|(_, col)| col.affinity().aff_mask())
                            .unwrap_or('B')
                    })
                    .collect();

                program.emit_insn(Insn::Affinity {
                    start_reg: ins,
                    count: NonZeroUsize::new(k).unwrap(),
                    affinities: aff,
                });
                program.emit_insn(Insn::NoConflict {
                    cursor_id: *idx_cid,
                    target_pc: ok,
                    record_reg: ins,
                    num_regs: k,
                });
                let hit = program.alloc_register();
                program.emit_insn(Insn::IdxRowId {
                    cursor_id: *idx_cid,
                    dest: hit,
                });
                program.emit_insn(Insn::Eq {
                    lhs: new_rowid,
                    rhs: hit,
                    target_pc: ok,
                    flags: CmpInsFlags::default(),
                    collation: program.curr_collation(),
                });
                let description = format_unique_violation_desc(table.get_name(), idx_meta);
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                    description,
                });
                program.preassign_label_to_next_insn(ok);
            }

            program.emit_insn(Insn::IdxInsert {
                cursor_id: *idx_cid,
                record_reg: rec,
                unpacked_start: Some(ins),
                unpacked_count: Some((k + 1) as u16),
                flags: IdxInsertFlags::new().nchange(true),
            });

            if let Some(lbl) = maybe_skip_ins {
                program.resolve_label(lbl, program.offset());
            }
        }
    }

    // Build NEW table payload
    let rec = program.alloc_register();
    let affinity_str = table
        .columns()
        .iter()
        .map(|c| c.affinity().aff_mask())
        .collect::<String>();
    program.emit_insn(Insn::MakeRecord {
        start_reg: new_start,
        count: num_cols,
        dest_reg: rec,
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    // If rowid changed, first ensure no other row owns it, then delete+insert
    if let Some(rnew) = new_rowid_reg {
        let ok = program.allocate_label();

        // If equal to old rowid, skip uniqueness probe
        program.emit_insn(Insn::Eq {
            lhs: rnew,
            rhs: conflict_rowid_reg,
            target_pc: ok,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });

        // If another row already has rnew -> constraint
        program.emit_insn(Insn::NotExists {
            cursor: tbl_cursor_id,
            rowid_reg: rnew,
            target_pc: ok,
        });
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description: format!(
                "{}.{}",
                table.get_name(),
                table
                    .columns()
                    .iter()
                    .find(|c| c.is_rowid_alias)
                    .and_then(|c| c.name.as_ref())
                    .unwrap_or(&"rowid".to_string())
            ),
        });
        program.preassign_label_to_next_insn(ok);

        // Now replace the row
        program.emit_insn(Insn::Delete {
            cursor_id: tbl_cursor_id,
            table_name: table.get_name().to_string(),
        });
        program.emit_insn(Insn::Insert {
            cursor: tbl_cursor_id,
            key_reg: rnew,
            record_reg: rec,
            flag: InsertFlags::new().require_seek().update_rowid_change(),
            table_name: table.get_name().to_string(),
        });
    } else {
        program.emit_insn(Insn::Insert {
            cursor: tbl_cursor_id,
            key_reg: conflict_rowid_reg,
            record_reg: rec,
            flag: InsertFlags::new(),
            table_name: table.get_name().to_string(),
        });
    }

    // emit CDC instructions
    if let Some(cdc_id) = cdc_cursor_id {
        let new_rowid = new_rowid_reg.unwrap_or(conflict_rowid_reg);
        if new_rowid_reg.is_some() {
            // DELETE (before)
            let before_rec = if program.capture_data_changes_mode().has_before() {
                Some(emit_cdc_full_record(
                    program,
                    table.columns(),
                    tbl_cursor_id,
                    conflict_rowid_reg,
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                resolver,
                OperationMode::DELETE,
                cdc_id,
                conflict_rowid_reg,
                before_rec,
                None,
                None,
                table.get_name(),
            )?;

            // INSERT (after)
            let after_rec = if program.capture_data_changes_mode().has_after() {
                Some(emit_cdc_patch_record(
                    program, table, new_start, rec, new_rowid,
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                resolver,
                OperationMode::INSERT,
                cdc_id,
                new_rowid,
                None,
                after_rec,
                None,
                table.get_name(),
            )?;
        } else {
            let after_rec = if program.capture_data_changes_mode().has_after() {
                Some(emit_cdc_patch_record(
                    program,
                    table,
                    new_start,
                    rec,
                    conflict_rowid_reg,
                ))
            } else {
                None
            };
            let before_rec = if program.capture_data_changes_mode().has_before() {
                Some(emit_cdc_full_record(
                    program,
                    table.columns(),
                    tbl_cursor_id,
                    conflict_rowid_reg,
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                resolver,
                OperationMode::UPDATE,
                cdc_id,
                conflict_rowid_reg,
                before_rec,
                after_rec,
                None,
                table.get_name(),
            )?;
        }
    }

    // RETURNING from NEW image + final rowid
    if !returning.is_empty() {
        let regs = ReturningValueRegisters {
            rowid_register: new_rowid_reg.unwrap_or(conflict_rowid_reg),
            columns_start_register: new_start,
            num_columns: num_cols,
        };
        emit_returning_results(program, returning, &regs)?;
    }

    program.emit_insn(Insn::Goto {
        target_pc: row_done_label,
    });
    Ok(())
}

/// Normalize the `SET` clause into `(column_index, Expr)` pairs using table layout.
///
/// Supports multi-target row-value SETs: `SET (a, b) = (expr1, expr2)`.
/// Enforces same number of column names and RHS values.
/// If the same column is assigned multiple times, the last assignment wins.
pub fn collect_set_clauses_for_upsert(
    table: &Table,
    set_items: &mut [ast::Set],
) -> crate::Result<Vec<(usize, Box<ast::Expr>)>> {
    let lookup: HashMap<String, usize> = table
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, c)| c.name.as_ref().map(|n| (n.to_lowercase(), i)))
        .collect();

    let mut out: Vec<(usize, Box<ast::Expr>)> = vec![];

    for set in set_items {
        let values: Vec<Box<ast::Expr>> = match set.expr.as_ref() {
            ast::Expr::Parenthesized(v) => v.clone(),
            e => vec![e.clone().into()],
        };
        if set.col_names.len() != values.len() {
            bail_parse_error!(
                "{} columns assigned {} values",
                set.col_names.len(),
                values.len()
            );
        }
        for (cn, e) in set.col_names.iter().zip(values.into_iter()) {
            let Some(idx) = lookup.get(&normalize_ident(cn.as_str())) else {
                bail_parse_error!("no such column: {}", cn);
            };
            if let Some(existing) = out.iter_mut().find(|(i, _)| *i == *idx) {
                existing.1 = e;
            } else {
                out.push((*idx, e));
            }
        }
    }
    Ok(out)
}

fn eval_partial_pred_for_row_image(
    prg: &mut ProgramBuilder,
    table: &Table,
    idx: &Index,
    row_start: usize, // base of CURRENT or NEW image
    rowid_reg: usize, // rowid for that image
    resolver: &Resolver,
) -> Option<usize> {
    let Some(where_expr) = &idx.where_clause else {
        return None;
    };
    let mut e = where_expr.as_ref().clone();
    rewrite_expr_to_registers(
        &mut e, table, row_start, rowid_reg, None,  // table_name
        None,  // insertion
        false, // dont allow EXCLUDED
    )
    .ok()?;
    let r = prg.alloc_register();
    translate_expr_no_constant_opt(
        prg,
        None,
        &e,
        r,
        resolver,
        NoConstantOptReason::RegisterReuse,
    )
    .ok()?;
    Some(r)
}

/// Generic rewriter that maps column references to registers for a given row image.
///
/// - Id/Qualified refs to the *target table* (when `table_name` is provided) resolve
///   to the CURRENT/NEW row image starting at `base_start`, with `rowid` (or the
///   rowid-alias) mapped to `rowid_reg`.
/// - If `allow_excluded` and `insertion` are provided, `EXCLUDED.x` resolves to the
///   insertion registers (and `EXCLUDED.rowid` resolves to `insertion.key_register()`).
/// - If `table_name` is `None`, qualified refs never match
/// - Leaves names from other tables/namespaces untouched.
fn rewrite_expr_to_registers(
    e: &mut ast::Expr,
    table: &Table,
    base_start: usize,
    rowid_reg: usize,
    table_name: Option<&str>,
    insertion: Option<&Insertion>,
    allow_excluded: bool,
) -> crate::Result<WalkControl> {
    use ast::Expr;
    let table_name_norm = table_name.map(normalize_ident);

    // Map a column name to a register within the row image at `base_start`.
    let col_reg_from_row_image = |name: &str| -> Option<usize> {
        if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(name)) {
            return Some(rowid_reg);
        }
        let (idx, c) = table.get_column_by_name(name)?;
        if c.is_rowid_alias {
            Some(rowid_reg)
        } else {
            Some(base_start + idx)
        }
    };

    walk_expr_mut(
        e,
        &mut |expr: &mut ast::Expr| -> crate::Result<WalkControl> {
            match expr {
                Expr::Qualified(ns, c) | Expr::DoublyQualified(_, ns, c) => {
                    let ns = normalize_ident(ns.as_str());
                    let c = normalize_ident(c.as_str());
                    // Handle EXCLUDED.* if enabled
                    if allow_excluded && ns.eq_ignore_ascii_case("excluded") {
                        if let Some(ins) = insertion {
                            if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&c)) {
                                *expr = Expr::Register(ins.key_register());
                            } else if let Some(cm) = ins.get_col_mapping_by_name(&c) {
                                *expr = Expr::Register(cm.register);
                            } else {
                                bail_parse_error!("no such column in EXCLUDED: {}", c);
                            }
                        }
                        // If insertion is None, leave EXCLUDED.* untouched.
                        return Ok(WalkControl::Continue);
                    }

                    // Match the target table namespace if provided
                    if let Some(ref tn) = table_name_norm {
                        if ns.eq_ignore_ascii_case(tn) {
                            if let Some(r) = col_reg_from_row_image(&c) {
                                *expr = Expr::Register(r);
                            }
                        }
                    }
                }
                // Unqualified id -> row image (CURRENT/NEW depending on caller)
                Expr::Id(name) => {
                    if let Some(r) = col_reg_from_row_image(&normalize_ident(name.as_str())) {
                        *expr = Expr::Register(r);
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    )
}
