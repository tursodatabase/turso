use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use turso_parser::ast::{self, TriggerEvent, TriggerTime, Upsert};

use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::schema::{BTreeTable, IndexColumn, ROWID_SENTINEL};
use crate::translate::emitter::{emit_check_constraints, UpdateRowSource};
use crate::translate::expr::{rewrite_between_expr, walk_expr, WalkControl};
use crate::translate::fkeys::{
    emit_fk_child_update_counters, emit_parent_key_change_checks, fire_fk_update_actions,
};
use crate::translate::insert::{format_unique_violation_desc, InsertEmitCtx};
use crate::translate::planner::ROWID_STRS;
use crate::translate::trigger_exec::{
    fire_trigger, get_relevant_triggers_type_and_time, TriggerContext,
};
use crate::vdbe::insn::{to_u16, CmpInsFlags};
use crate::{
    bail_parse_error,
    error::SQLITE_CONSTRAINT_NOTNULL,
    schema::{Index, Schema, Table},
    translate::{
        emitter::{
            emit_cdc_full_record, emit_cdc_insns, emit_cdc_patch_record, OperationMode, Resolver,
        },
        expr::{
            emit_returning_results, translate_expr, translate_expr_no_constant_opt, walk_expr_mut,
            NoConstantOptReason,
        },
        insert::Insertion,
        plan::{ResultSetColumn, TableReferences},
    },
    util::{exprs_are_equivalent, normalize_ident},
    vdbe::{
        affinity::Affinity,
        builder::ProgramBuilder,
        insn::{IdxInsertFlags, InsertFlags, Insn},
    },
};
use crate::{CaptureDataChangesExt, Connection};

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

/// For an ON CONFLICT target that is an expression (not a simple column),
/// extract the inner expression and an optional COLLATE annotation.
/// E.g. `lower(val) COLLATE nocase` -> (lower(val), Some("nocase"))
fn extract_target_expr(e: &ast::Expr) -> (&ast::Expr, Option<String>) {
    match e {
        ast::Expr::Collate(inner, c) => {
            let (expr, _) = extract_target_expr(inner.as_ref());
            (expr, Some(c.as_str().to_ascii_lowercase()))
        }
        ast::Expr::Parenthesized(v) if v.len() == 1 => extract_target_expr(&v[0]),
        _ => (e, None),
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
        .map(|s| s.1.collation().to_string())
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
    let pk = table.columns().iter().find(|c| c.is_rowid_alias());
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
    let mut cols_changed =
        HashSet::with_capacity_and_hasher(table.columns().len(), Default::default());
    let mut rowid_changed = false;
    for (col_idx, _) in set_pairs {
        if let Some(c) = table.columns().get(*col_idx) {
            if c.is_rowid_alias() {
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
    let km: HashSet<usize> = idx
        .columns
        .iter()
        .filter_map(|ic| ic.expr.is_none().then_some(ic.pos_in_table))
        .collect();
    let pm = referenced_index_cols(idx, table);
    for c in km.iter().chain(pm.iter()) {
        if changed_cols.contains(c) {
            return true;
        }
    }
    false
}

/// Collect HashSet of columns referenced by the partial WHERE (empty if none), or
/// by the expression of any IndexColumn on the index.
fn referenced_index_cols(idx: &Index, table: &Table) -> HashSet<usize> {
    let mut out = HashSet::default();
    if let Some(expr) = &idx.where_clause {
        index_expression_cols(table, &mut out, expr);
    }
    for ic in &idx.columns {
        if let Some(expr) = &ic.expr {
            index_expression_cols(table, &mut out, expr);
        }
    }
    out
}

/// Columns referenced by any expression index columns on the index.
fn index_expression_cols(table: &Table, out: &mut HashSet<usize>, expr: &ast::Expr) {
    use ast::Expr;
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> crate::Result<WalkControl> {
        match e {
            Expr::Id(n) => {
                if let Some((i, _)) = table.get_column_by_name(&normalize_ident(n.as_str())) {
                    out.insert(i);
                } else if ROWID_STRS
                    .iter()
                    .any(|r| r.eq_ignore_ascii_case(n.as_str()))
                {
                    if let Some(rowid_pos) = table
                        .btree()
                        .and_then(|t| t.get_rowid_alias_column().map(|(p, _)| p))
                    {
                        out.insert(rowid_pos);
                    }
                }
            }
            Expr::Qualified(ns, c) | Expr::DoublyQualified(_, ns, c) => {
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

    // Track which index columns have been matched (consumed).
    let mut matched = vec![false; index.columns.len()];

    for te in &target.targets {
        let mut found = None;

        if let Some(tk) = extract_target_key(&te.expr) {
            // Simple column reference target: match by name and collation.
            let tname = &tk.col_name;
            for (i, ic) in index.columns.iter().enumerate() {
                if matched[i] || ic.expr.is_some() {
                    continue;
                }
                let iname = normalize_ident(&ic.name);
                let icoll = effective_collation_for_index_col(ic, table);
                if tname.eq_ignore_ascii_case(&iname)
                    && match tk.collate.as_ref() {
                        Some(c) => c.eq_ignore_ascii_case(&icoll),
                        None => true, // unspecified collation -> accept any
                    }
                {
                    found = Some(i);
                    break;
                }
            }
        } else {
            // Expression target (e.g. lower(val)): match against expression index
            // columns using semantic equivalence.
            let (target_expr, target_collate) = extract_target_expr(&te.expr);
            for (i, ic) in index.columns.iter().enumerate() {
                if matched[i] {
                    continue;
                }
                if let Some(idx_expr) = &ic.expr {
                    if exprs_are_equivalent(target_expr, idx_expr) {
                        // If target specifies a collation, it must match the index column's.
                        if let Some(ref tc) = target_collate {
                            let icoll = effective_collation_for_index_col(ic, table);
                            if !tc.eq_ignore_ascii_case(&icoll) {
                                continue;
                            }
                        }
                        found = Some(i);
                        break;
                    }
                }
            }
        }

        if let Some(i) = found {
            matched[i] = true;
        } else {
            return false;
        }
    }
    // All target columns matched exactly once, and all index columns consumed
    matched.iter().all(|&m| m)
}

#[derive(Clone, Debug)]
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
#[allow(clippy::too_many_arguments)]
pub fn emit_upsert(
    program: &mut ProgramBuilder,
    table: &Table,
    ctx: &InsertEmitCtx,
    insertion: &Insertion,
    set_pairs: &mut [(usize, Box<ast::Expr>)],
    where_clause: &mut Option<Box<ast::Expr>>,
    resolver: &mut Resolver,
    returning: &mut [ResultSetColumn],
    connection: &Arc<Connection>,
    table_references: &mut TableReferences,
) -> crate::Result<()> {
    // Seek & snapshot CURRENT
    program.emit_insn(Insn::SeekRowid {
        cursor_id: ctx.cursor_id,
        src_reg: ctx.conflict_rowid_reg,
        target_pc: ctx.loop_labels.row_done,
    });
    let num_cols = ctx.table.columns.len();
    let current_start = program.alloc_registers(num_cols);
    for (i, col) in ctx.table.columns.iter().enumerate() {
        if col.is_rowid_alias() {
            program.emit_insn(Insn::RowId {
                cursor_id: ctx.cursor_id,
                dest: current_start + i,
            });
        } else {
            program.emit_insn(Insn::Column {
                cursor_id: ctx.cursor_id,
                column: i,
                dest: current_start + i,
                default: None,
            });
        }
    }

    // BEFORE for index maintenance / CDC
    let before_start = if ctx.cdc_table.is_some() || !ctx.idx_cursors.is_empty() {
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

    // For STRICT tables with custom types, values loaded from disk (current_start)
    // are in encoded form. We need decoded copies so that:
    // - WHERE clause expressions see user-facing values (Bug 13)
    // - SET expressions referencing t1.column see user-facing values
    // - excluded.column references also see decoded values (Bug 7)
    // current_start itself stays encoded for trigger OLD registers and before_start.
    // After SET evaluation, we encode ALL columns in new_start before writing to disk.
    let (decoded_current_start, excluded_decoded_start) = if let Some(bt) = table.btree() {
        if bt.is_strict {
            // Create decoded copy of current_start for WHERE/SET expressions
            let decoded_current = program.alloc_registers(num_cols);
            program.emit_insn(Insn::Copy {
                src_reg: current_start,
                dst_reg: decoded_current,
                extra_amount: num_cols - 1,
            });
            crate::translate::expr::emit_custom_type_decode_columns(
                program,
                resolver,
                &bt.columns,
                decoded_current,
                None,
            )?;
            // Decode new_start in-place (was copied from encoded current_start;
            // after SET applies decoded values, we encode ALL columns)
            crate::translate::expr::emit_custom_type_decode_columns(
                program,
                resolver,
                &bt.columns,
                new_start,
                None,
            )?;
            // Create decoded copies of excluded (insertion) registers so that
            // excluded.column references see user-facing values
            let decoded_excluded = program.alloc_registers(num_cols);
            program.emit_insn(Insn::Copy {
                src_reg: insertion.first_col_register(),
                dst_reg: decoded_excluded,
                extra_amount: num_cols - 1,
            });
            crate::translate::expr::emit_custom_type_decode_columns(
                program,
                resolver,
                &bt.columns,
                decoded_excluded,
                None,
            )?;
            (Some(decoded_current), Some(decoded_excluded))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // For WHERE and SET, use decoded_current_start if available (STRICT with custom types),
    // otherwise fall back to current_start (already decoded or non-custom-type).
    let expr_current_start = decoded_current_start.unwrap_or(current_start);

    // WHERE on target row
    if let Some(pred) = where_clause.as_mut() {
        rewrite_expr_to_registers(
            pred,
            table,
            expr_current_start,
            ctx.conflict_rowid_reg,
            Some(table.get_name()),
            Some(insertion),
            true,
            excluded_decoded_start,
        )?;
        let pr = program.alloc_register();
        translate_expr(program, None, pred, pr, resolver)?;
        program.emit_insn(Insn::IfNot {
            reg: pr,
            target_pc: ctx.loop_labels.row_done,
            jump_if_null: true,
        });
    }

    // Apply SET; capture rowid change if any
    let mut new_rowid_reg: Option<usize> = None;
    for (col_idx, expr) in set_pairs.iter_mut() {
        rewrite_expr_to_registers(
            expr,
            table,
            expr_current_start,
            ctx.conflict_rowid_reg,
            Some(table.get_name()),
            Some(insertion),
            true,
            excluded_decoded_start,
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
        if col.notnull() && !col.is_rowid_alias() {
            program.emit_insn(Insn::HaltIfNull {
                target_reg: new_start + *col_idx,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: String::from(table.get_name()) + "." + col.name.as_ref().unwrap(),
            });
        }
        if col.is_rowid_alias() {
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
            // Encode ALL columns. Both non-SET columns (decoded from disk above)
            // and SET columns (user-facing values from expressions) need encoding
            // before being written to disk.
            crate::translate::expr::emit_custom_type_encode_columns(
                program,
                resolver,
                &bt.columns,
                new_start,
                None,
            )?;

            program.emit_insn(Insn::TypeCheck {
                start_reg: new_start,
                count: num_cols,
                check_generated: true,
                table_reference: BTreeTable::type_check_table_ref(&bt, resolver.schema),
            });
        } else {
            // For non-STRICT tables, apply column affinity to the values.
            // This must happen early so that both index records and the table record
            // use the converted values.
            let affinity = bt.columns.iter().map(|c| c.affinity());

            // Only emit Affinity if there's meaningful affinity to apply
            if affinity.clone().any(|a| a != Affinity::Blob) {
                if let Ok(count) = std::num::NonZeroUsize::try_from(num_cols) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: new_start,
                        count,
                        affinities: affinity.map(|a| a.aff_mask()).collect(),
                    });
                }
            }
        }

        // Evaluate CHECK constraints on the new values
        emit_check_constraints(
            program,
            &bt.check_constraints,
            resolver,
            &bt.name,
            new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg),
            bt.columns
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| col.name.as_deref().map(|n| (n, new_start + idx))),
            connection,
            ast::ResolveType::Abort,
            ctx.loop_labels.row_done,
            Some(table_references),
        )?;
    }

    let (changed_cols, rowid_changed) = collect_changed_cols(table, set_pairs);

    // Fire BEFORE UPDATE triggers
    let upsert_database_id = ctx.database_id;
    let preserved_old_registers: Option<Vec<usize>> = if let Some(btree_table) = table.btree() {
        let updated_column_indices: HashSet<usize> =
            set_pairs.iter().map(|(col_idx, _)| *col_idx).collect();
        let relevant_before_update_triggers: Vec<_> =
            resolver.with_schema(upsert_database_id, |s| {
                get_relevant_triggers_type_and_time(
                    s,
                    TriggerEvent::Update,
                    TriggerTime::Before,
                    Some(updated_column_indices.clone()),
                    &btree_table,
                )
                .collect()
            });
        // OLD row values are in current_start registers
        let old_registers: Vec<usize> = (0..num_cols)
            .map(|i| current_start + i)
            .chain(std::iter::once(ctx.conflict_rowid_reg))
            .collect();
        if !relevant_before_update_triggers.is_empty() {
            // NEW row values are in new_start registers
            let new_rowid_for_trigger = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
            let new_registers: Vec<usize> = (0..num_cols)
                .map(|i| new_start + i)
                .chain(std::iter::once(new_rowid_for_trigger))
                .collect();

            // In UPSERT DO UPDATE context, trigger's INSERT/UPDATE OR IGNORE/REPLACE
            // clauses should not suppress errors. Override conflict resolution to Abort.
            let trigger_ctx = TriggerContext::new_with_override_conflict(
                btree_table.clone(),
                Some(new_registers),
                Some(old_registers.clone()),
                ast::ResolveType::Abort,
            );

            for trigger in relevant_before_update_triggers {
                fire_trigger(
                    program,
                    resolver,
                    trigger,
                    &trigger_ctx,
                    connection,
                    upsert_database_id,
                    ctx.loop_labels.row_done,
                )?;
            }

            // BEFORE UPDATE triggers may have altered the btree, need to re-seek
            program.emit_insn(Insn::NotExists {
                cursor: ctx.cursor_id,
                rowid_reg: ctx.conflict_rowid_reg,
                target_pc: ctx.loop_labels.row_done,
            });

            let has_relevant_after_triggers = resolver.with_schema(upsert_database_id, |s| {
                get_relevant_triggers_type_and_time(
                    s,
                    TriggerEvent::Update,
                    TriggerTime::After,
                    Some(updated_column_indices),
                    &btree_table,
                )
                .count()
                    > 0
            });
            if has_relevant_after_triggers {
                // Preserve OLD registers for AFTER triggers
                let preserved: Vec<usize> = old_registers
                    .iter()
                    .map(|old_reg| {
                        let preserved_reg = program.alloc_register();
                        program.emit_insn(Insn::Copy {
                            src_reg: *old_reg,
                            dst_reg: preserved_reg,
                            extra_amount: 0,
                        });
                        preserved_reg
                    })
                    .collect();
                Some(preserved)
            } else {
                None
            }
        } else {
            // Check if we need to preserve for AFTER triggers
            let has_relevant_after_triggers = resolver.with_schema(upsert_database_id, |s| {
                get_relevant_triggers_type_and_time(
                    s,
                    TriggerEvent::Update,
                    TriggerTime::After,
                    Some(updated_column_indices),
                    &btree_table,
                )
                .count()
                    > 0
            });
            if has_relevant_after_triggers {
                Some(old_registers)
            } else {
                None
            }
        }
    } else {
        None
    };
    let rowid_alias_idx = table.columns().iter().position(|c| c.is_rowid_alias());
    let has_direct_rowid_update = set_pairs
        .iter()
        .any(|(idx, _)| *idx == rowid_alias_idx.unwrap_or(ROWID_SENTINEL));
    let has_user_provided_rowid = if let Some(i) = rowid_alias_idx {
        set_pairs.iter().any(|(idx, _)| *idx == i) || has_direct_rowid_update
    } else {
        has_direct_rowid_update
    };

    let rowid_set_clause_reg = if has_user_provided_rowid {
        Some(new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg))
    } else {
        None
    };
    if let Some(bt) = table.btree() {
        if connection.foreign_keys_enabled() {
            let rowid_new_reg = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);

            // Child-side checks
            if resolver.with_schema(upsert_database_id, |s| s.has_child_fks(bt.name.as_str())) {
                emit_fk_child_update_counters(
                    program,
                    &bt,
                    table.get_name(),
                    ctx.cursor_id,
                    new_start,
                    rowid_new_reg,
                    &changed_cols,
                    upsert_database_id,
                    resolver,
                )?;
            }
            let upsert_indices: Vec<_> = resolver.with_schema(upsert_database_id, |s| {
                s.get_indices(table.get_name()).cloned().collect()
            });
            emit_parent_key_change_checks(
                program,
                &bt,
                upsert_indices.iter().filter(|idx| {
                    upsert_index_is_affected(table, idx, &changed_cols, rowid_changed)
                }),
                ctx.cursor_id,
                ctx.conflict_rowid_reg,
                new_start,
                new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg),
                rowid_set_clause_reg,
                set_pairs,
                upsert_database_id,
                resolver,
            )?;
        }
    }

    // Index rebuild (DELETE old, INSERT new), honoring partial-index WHEREs
    if let Some(before) = before_start {
        for (idx_name, _root, idx_cid) in &ctx.idx_cursors {
            let idx_meta = resolver
                .with_schema(ctx.database_id, |s| {
                    s.get_index(table.get_name(), idx_name).cloned()
                })
                .expect("index exists");

            if !upsert_index_is_affected(table, &idx_meta, &changed_cols, rowid_changed) {
                continue; // skip untouched index completely
            }
            let k = idx_meta.columns.len();

            let before_pred_reg = eval_partial_pred_for_row_image(
                program,
                table,
                &idx_meta,
                before,
                ctx.conflict_rowid_reg,
                resolver,
            );
            let new_rowid = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
            let new_pred_reg = eval_partial_pred_for_row_image(
                program, table, &idx_meta, new_start, new_rowid, resolver,
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
                if let Some(expr) = &ic.expr {
                    let mut e = expr.as_ref().clone();
                    rewrite_expr_to_registers(
                        &mut e,
                        table,
                        before,
                        ctx.conflict_rowid_reg,
                        Some(table.get_name()),
                        None,
                        false,
                        None,
                    )?;
                    translate_expr_no_constant_opt(
                        program,
                        None,
                        &e,
                        del + i,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                } else {
                    let (ci, _) = table.get_column_by_name(&ic.name).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: before + ci,
                        dst_reg: del + i,
                        extra_amount: 0,
                    });
                }
            }
            program.emit_insn(Insn::Copy {
                src_reg: ctx.conflict_rowid_reg,
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
                if let Some(expr) = &ic.expr {
                    let mut e = expr.as_ref().clone();
                    rewrite_expr_to_registers(
                        &mut e,
                        table,
                        new_start,
                        new_rowid,
                        Some(table.get_name()),
                        None,
                        false,
                        None,
                    )?;
                    translate_expr_no_constant_opt(
                        program,
                        None,
                        &e,
                        ins + i,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                } else {
                    let (ci, _) = table.get_column_by_name(&ic.name).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: new_start + ci,
                        dst_reg: ins + i,
                        extra_amount: 0,
                    });
                }
            }
            program.emit_insn(Insn::Copy {
                src_reg: new_rowid,
                dst_reg: ins + k,
                extra_amount: 0,
            });

            let rec = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u16(ins),
                count: to_u16(k + 1),
                dest_reg: to_u16(rec),
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
                        c.expr.as_ref().map_or_else(
                            || {
                                table
                                    .get_column_by_name(&c.name)
                                    .map(|(_, col)| {
                                        let is_strict =
                                            table.btree().is_some_and(|btree| btree.is_strict);
                                        col.affinity_with_strict(is_strict).aff_mask()
                                    })
                                    .unwrap_or('B')
                            },
                            |_| crate::vdbe::affinity::Affinity::Blob.aff_mask(),
                        )
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
                let description = format_unique_violation_desc(table.get_name(), &idx_meta);
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                    description,
                    on_error: None,
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
    let is_strict = table.btree().is_some_and(|btree| btree.is_strict);
    let affinity_str = table
        .columns()
        .iter()
        .map(|c| c.affinity_with_strict(is_strict).aff_mask())
        .collect::<String>();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(new_start),
        count: to_u16(num_cols),
        dest_reg: to_u16(rec),
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    // If rowid changed, first ensure no other row owns it, then delete+insert
    if let Some(rnew) = new_rowid_reg {
        let ok = program.allocate_label();

        // If equal to old rowid, skip uniqueness probe
        program.emit_insn(Insn::Eq {
            lhs: rnew,
            rhs: ctx.conflict_rowid_reg,
            target_pc: ok,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });

        // If another row already has rnew -> constraint
        program.emit_insn(Insn::NotExists {
            cursor: ctx.cursor_id,
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
                    .find(|c| c.is_rowid_alias())
                    .and_then(|c| c.name.as_deref())
                    .unwrap_or("rowid")
            ),
            on_error: None,
        });
        program.preassign_label_to_next_insn(ok);

        // important: the cursor was repositioned in the previous conflict check via NotExists,
        // so if we didn't conflict+halt above, we need to re-seek to the row under update.
        program.emit_insn(Insn::SeekRowid {
            cursor_id: ctx.cursor_id,
            src_reg: ctx.conflict_rowid_reg,
            target_pc: ctx.loop_labels.row_done,
        });

        // Now replace the row
        program.emit_insn(Insn::Delete {
            cursor_id: ctx.cursor_id,
            table_name: table.get_name().to_string(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Insert {
            cursor: ctx.cursor_id,
            key_reg: rnew,
            record_reg: rec,
            flag: InsertFlags::new()
                .require_seek()
                .update_rowid_change()
                .skip_last_rowid(),
            table_name: table.get_name().to_string(),
        });
    } else {
        program.emit_insn(Insn::Insert {
            cursor: ctx.cursor_id,
            key_reg: ctx.conflict_rowid_reg,
            record_reg: rec,
            flag: InsertFlags::new().skip_last_rowid(),
            table_name: table.get_name().to_string(),
        });
    }

    // Fire FK actions (CASCADE, SET NULL, SET DEFAULT) for parent-side updates.
    // This must be done after the update is complete but before AFTER triggers.
    if let Some(bt) = table.btree() {
        if connection.foreign_keys_enabled()
            && resolver.with_schema(upsert_database_id, |s| {
                s.any_resolved_fks_referencing(bt.name.as_str())
            })
        {
            fire_fk_update_actions(
                program,
                resolver,
                bt.name.as_str(),
                ctx.conflict_rowid_reg, // old_rowid_reg
                current_start,          // old_values_start
                new_start,              // new_values_start
                new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg), // new_rowid_reg
                connection,
                upsert_database_id,
            )?;
        }
    }

    // emit CDC instructions
    if let Some((cdc_id, _)) = ctx.cdc_table {
        let new_rowid = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
        if new_rowid_reg.is_some() {
            // DELETE (before)
            let before_rec = if program.capture_data_changes_info().has_before() {
                Some(emit_cdc_full_record(
                    program,
                    table.columns(),
                    ctx.cursor_id,
                    ctx.conflict_rowid_reg,
                    table.btree().is_some_and(|btree| btree.is_strict),
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                resolver,
                OperationMode::DELETE,
                cdc_id,
                ctx.conflict_rowid_reg,
                before_rec,
                None,
                None,
                table.get_name(),
            )?;

            // INSERT (after)
            let after_rec = if program.capture_data_changes_info().has_after() {
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
            let after_rec = if program.capture_data_changes_info().has_after() {
                Some(emit_cdc_patch_record(
                    program,
                    table,
                    new_start,
                    rec,
                    ctx.conflict_rowid_reg,
                ))
            } else {
                None
            };
            let before_rec = if program.capture_data_changes_info().has_before() {
                Some(emit_cdc_full_record(
                    program,
                    table.columns(),
                    ctx.cursor_id,
                    ctx.conflict_rowid_reg,
                    table.btree().is_some_and(|btree| btree.is_strict),
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                resolver,
                OperationMode::UPDATE(UpdateRowSource::Normal),
                cdc_id,
                ctx.conflict_rowid_reg,
                before_rec,
                after_rec,
                None,
                table.get_name(),
            )?;
        }
    }

    // Fire AFTER UPDATE triggers
    if let (Some(btree_table), Some(old_regs)) = (table.btree(), preserved_old_registers) {
        let updated_column_indices: HashSet<usize> =
            set_pairs.iter().map(|(col_idx, _)| *col_idx).collect();
        let relevant_triggers: Vec<_> = resolver.with_schema(upsert_database_id, |s| {
            get_relevant_triggers_type_and_time(
                s,
                TriggerEvent::Update,
                TriggerTime::After,
                Some(updated_column_indices),
                &btree_table,
            )
            .collect()
        });
        if !relevant_triggers.is_empty() {
            let new_rowid_for_trigger = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
            let new_registers_after: Vec<usize> = (0..num_cols)
                .map(|i| new_start + i)
                .chain(std::iter::once(new_rowid_for_trigger))
                .collect();

            // In UPSERT DO UPDATE context, trigger's INSERT/UPDATE OR IGNORE/REPLACE
            // clauses should not suppress errors. Override conflict resolution to Abort.
            // NEW values are encoded at this point; fire_trigger will decode them.
            let trigger_ctx_after = TriggerContext::new_after_with_override_conflict(
                btree_table,
                Some(new_registers_after),
                Some(old_regs),
                ast::ResolveType::Abort,
            );

            // RAISE(IGNORE) in an AFTER trigger should only abort the trigger body,
            // not skip post-row work (RETURNING).
            let after_trigger_done = program.allocate_label();
            for trigger in relevant_triggers {
                fire_trigger(
                    program,
                    resolver,
                    trigger,
                    &trigger_ctx_after,
                    connection,
                    upsert_database_id,
                    after_trigger_done,
                )?;
            }
            program.preassign_label_to_next_insn(after_trigger_done);
        }
    }

    // RETURNING from NEW image + final rowid
    if !returning.is_empty() {
        emit_returning_results(
            program,
            table_references,
            returning,
            new_start,
            new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg),
            resolver,
            ctx.returning_buffer.as_ref(),
        )?;
    }

    program.emit_insn(Insn::Goto {
        target_pc: ctx.loop_labels.row_done,
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
    rewrite_between_expr(&mut e);
    rewrite_expr_to_registers(
        &mut e, table, row_start, rowid_reg, None,  // table_name
        None,  // insertion
        false, // dont allow EXCLUDED
        None,  // no decoded excluded
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
///   When `excluded_decoded_start` is provided, excluded column references resolve to
///   decoded registers at `excluded_decoded_start + col_idx` instead of the raw
///   (encoded) insertion registers. This prevents double-encoding in UPSERT.
/// - If `table_name` is `None`, qualified refs never match
/// - Leaves names from other tables/namespaces untouched.
#[allow(clippy::too_many_arguments)]
fn rewrite_expr_to_registers(
    e: &mut ast::Expr,
    table: &Table,
    base_start: usize,
    rowid_reg: usize,
    table_name: Option<&str>,
    insertion: Option<&Insertion>,
    allow_excluded: bool,
    excluded_decoded_start: Option<usize>,
) -> crate::Result<WalkControl> {
    use ast::Expr;
    let table_name_norm = table_name.map(normalize_ident);

    // Map a column name to a register within the row image at `base_start`.
    let col_reg_from_row_image = |name: &str| -> Option<usize> {
        if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(name)) {
            return Some(rowid_reg);
        }
        let (idx, c) = table.get_column_by_name(name)?;
        if c.is_rowid_alias() {
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
                                // Use decoded excluded registers when available
                                // to prevent double-encoding of custom type values
                                if let Some(decoded_start) = excluded_decoded_start {
                                    let (col_idx, _) =
                                        table.get_column_by_name(&c).expect("column exists");
                                    *expr = Expr::Register(decoded_start + col_idx);
                                } else {
                                    *expr = Expr::Register(cm.register);
                                }
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
                            return Ok(WalkControl::Continue);
                        }
                    }

                    // In UPSERT DO UPDATE context (allow_excluded=true), a qualified
                    // reference that doesn't match the target table or EXCLUDED is
                    // invalid. Return a graceful error instead of leaving it
                    // unresolved (which would panic later in translate_expr).
                    if allow_excluded {
                        bail_parse_error!("no such column: {}.{}", ns, c);
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
