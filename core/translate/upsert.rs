use std::{collections::HashMap, sync::Arc};

use turso_parser::ast::{self, Upsert};

use crate::{
    bail_parse_error,
    error::SQLITE_CONSTRAINT_NOTNULL,
    schema::{Index, IndexColumn, Schema, Table},
    translate::{
        emitter::{
            emit_cdc_full_record, emit_cdc_insns, emit_cdc_patch_record, OperationMode, Resolver,
        },
        expr::{
            emit_returning_results, translate_expr, translate_expr_no_constant_opt,
            NoConstantOptReason, ReturningValueRegisters,
        },
        insert::{Insertion, ROWID_COLUMN},
        plan::ResultSetColumn,
    },
    util::normalize_ident,
    vdbe::{
        builder::ProgramBuilder,
        insn::{IdxInsertFlags, InsertFlags, Insn},
        BranchOffset,
    },
};

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
        // expr COLLATE c: carry c and keep descending into expr
        ast::Expr::Collate(inner, c) => {
            let mut tk = extract_target_key(inner.as_ref())?;
            let cstr = match c {
                ast::Name::Ident(s) => s.as_str(),
                _ => return None,
            };
            tk.collate = Some(cstr.to_ascii_lowercase());
            Some(tk)
        }
        ast::Expr::Parenthesized(v) if v.len() == 1 => extract_target_key(&v[0]),
        // Bare identifier
        ast::Expr::Id(ast::Name::Ident(name)) => Some(ConflictTarget {
            col_name: normalize_ident(name),
            collate: None,
        }),
        // t.a or db.t.a
        ast::Expr::Qualified(_, col) | ast::Expr::DoublyQualified(_, _, col) => {
            let cname = match col {
                ast::Name::Ident(s) => s.as_str(),
                _ => return None,
            };
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

/// Match ON CONFLICT target to the PRIMARY KEY, if any.
/// If no target is specified, it is an automatic match for PRIMARY KEY
pub fn upsert_matches_pk(upsert: &Upsert, table: &Table) -> bool {
    let Some(t) = upsert.index.as_ref() else {
        // Omitted target is automatic
        return true;
    };
    if !t.targets.len().eq(&1) {
        return false;
    }
    let pk = table
        .columns()
        .iter()
        .find(|c| c.is_rowid_alias || c.primary_key)
        .unwrap_or(ROWID_COLUMN);
    extract_target_key(&t.targets[0].expr).is_some_and(|tk| {
        tk.col_name
            .eq_ignore_ascii_case(pk.name.as_ref().unwrap_or(&String::new()))
    })
}

#[derive(Hash, Debug, Eq, PartialEq, Clone)]
/// A hashable descriptor of a single index key term used when
/// matching an `ON CONFLICT` target against a UNIQUE index.
/// captures only the attributes (name and effective collation) that
/// determine whether two key terms are equivalent for conflict detection.
pub struct KeySig {
    /// column name, normalized to lowercase
    name: String,
    /// defaults to "binary" if not specified on the target or col
    coll: String,
}

/// Match ON CONFLICT target to a UNIQUE index, ignoring order, requiring exact
/// coverage, and honoring collations. `table` is used to derive effective collation.
pub fn upsert_matches_index(upsert: &Upsert, index: &Index, table: &Table) -> bool {
    let Some(target) = upsert.index.as_ref() else {
        // catch-all
        return true;
    };
    // if not unique or column count differs, no match
    if !index.unique || target.targets.len() != index.columns.len() {
        return false;
    }

    let mut need: HashMap<KeySig, usize> = HashMap::new();
    for ic in &index.columns {
        let sig = KeySig {
            name: normalize_ident(&ic.name).to_string(),
            coll: effective_collation_for_index_col(ic, table),
        };
        *need.entry(sig).or_insert(0) += 1;
    }

    // Consume from the multiset using target entries, order-insensitive
    for te in &target.targets {
        let tk = match extract_target_key(&te.expr) {
            Some(x) => x,
            None => return false, // not a simple column ref
        };

        // Candidate signatures for this target:
        // If target specifies COLLATE, require exact match on (name, coll).
        // Otherwise, accept any collation currently present for that name.
        let mut matched = false;
        if let Some(ref coll) = tk.collate {
            let sig = KeySig {
                name: tk.col_name.to_string(),
                coll: coll.clone(),
            };
            if let Some(cnt) = need.get_mut(&sig) {
                *cnt -= 1;
                if *cnt == 0 {
                    need.remove(&sig);
                }
                matched = true;
            }
        } else {
            // Try any available collation for this column name
            if let Some((sig, cnt)) = need
                .iter_mut()
                .find(|(k, _)| k.name.eq_ignore_ascii_case(&tk.col_name))
            {
                *cnt -= 1;
                if *cnt == 0 {
                    let key = sig.clone();
                    need.remove(&key);
                }
                matched = true;
            }
        }
        if !matched {
            return false;
        }
    }
    // All targets matched exactly.
    need.is_empty()
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
    schema: &Schema,
    table: &Table,
    insertion: &Insertion,
    tbl_cursor_id: usize,
    conflict_rowid_reg: usize,
    set_pairs: &mut [(usize, Box<ast::Expr>)],
    where_clause: &mut Option<Box<ast::Expr>>,
    resolver: &Resolver,
    idx_cursors: &[(&String, usize, usize)],
    returning: &mut [ResultSetColumn],
    cdc_cursor_id: Option<usize>,
    row_done_label: BranchOffset,
) -> crate::Result<()> {
    // Seek and snapshot current row
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

    // Keep BEFORE snapshot if needed
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

    // NEW snapshot starts as a copy of CURRENT, then SET expressions overwrite
    // the assigned columns. matching SQLite semantics of UPDATE reading the old row.
    let new_start = program.alloc_registers(num_cols);
    program.emit_insn(Insn::Copy {
        src_reg: current_start,
        dst_reg: new_start,
        extra_amount: num_cols - 1,
    });

    // WHERE predicate on the target row. If false or NULL, skip the UPDATE.
    if let Some(pred) = where_clause.as_mut() {
        rewrite_upsert_expr_in_place(
            pred,
            table,
            table.get_name(),
            current_start,
            conflict_rowid_reg,
            insertion,
        )?;
        let pr = program.alloc_register();
        translate_expr(program, None, pred, pr, resolver)?;
        program.emit_insn(Insn::IfNot {
            reg: pr,
            target_pc: row_done_label,
            jump_if_null: true,
        });
    }

    // Evaluate each SET expression into the NEW row img
    for (col_idx, expr) in set_pairs.iter_mut() {
        rewrite_upsert_expr_in_place(
            expr,
            table,
            table.get_name(),
            current_start,
            conflict_rowid_reg,
            insertion,
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
                description: format!("{}.{}", table.get_name(), col.name.as_ref().unwrap()),
            });
        }
    }

    // If STRICT, perform type checks on the NEW image
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

    // Rebuild indexes: remove keys corresponding to BEFORE and insert keys for NEW.
    if let Some(before) = before_start {
        for (idx_name, _root, idx_cid) in idx_cursors {
            let idx_meta = schema
                .get_index(table.get_name(), idx_name)
                .expect("index exists");
            let k = idx_meta.columns.len();

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
                src_reg: conflict_rowid_reg,
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
            program.emit_insn(Insn::IdxInsert {
                cursor_id: *idx_cid,
                record_reg: rec,
                unpacked_start: Some(ins),
                unpacked_count: Some((k + 1) as u16),
                flags: IdxInsertFlags::new().nchange(true),
            });
        }
    }

    // Write table row (same rowid, new payload)
    let rec = program.alloc_register();

    let affinity_str = table
        .columns()
        .iter()
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::MakeRecord {
        start_reg: new_start,
        count: num_cols,
        dest_reg: rec,
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    program.emit_insn(Insn::Insert {
        cursor: tbl_cursor_id,
        key_reg: conflict_rowid_reg,
        record_reg: rec,
        flag: InsertFlags::new(),
        table_name: table.get_name().to_string(),
    });

    if let Some(cdc_id) = cdc_cursor_id {
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
        // Build BEFORE if needed
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

    if !returning.is_empty() {
        let regs = ReturningValueRegisters {
            rowid_register: conflict_rowid_reg,
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
/// Rewrites `EXCLUDED.*` references to direct `Register` reads from the insertion registers
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

/// Rewrite an UPSERT expression so that:
/// EXCLUDED.x -> Register(insertion.x)
/// t.x / x    -> Register(CURRENT.x) when t == target table or unqualified
/// rowid      -> Register(conflict_rowid_reg)
///
/// Only rewrites names in the current expression scope, does not enter subqueries.
fn rewrite_upsert_expr_in_place(
    e: &mut ast::Expr,
    table: &Table,
    table_name: &str,
    current_start: usize,
    conflict_rowid_reg: usize,
    insertion: &Insertion,
) -> crate::Result<()> {
    use ast::Expr::*;

    // helper: return the CURRENT-row register for a column (including rowid alias)
    let col_reg = |name: &str| -> Option<usize> {
        if name.eq_ignore_ascii_case("rowid") {
            return Some(conflict_rowid_reg);
        }
        let (idx, _c) = table.get_column_by_name(&normalize_ident(name))?;
        Some(current_start + idx)
    };

    match e {
        // EXCLUDED.x -> insertion register
        Qualified(ns, ast::Name::Ident(c)) if ns.as_str().eq_ignore_ascii_case("excluded") => {
            let Some(reg) = insertion.get_col_mapping_by_name(c.as_str()) else {
                bail_parse_error!("no such column in EXCLUDED: {}", c);
            };
            *e = Register(reg.register);
        }

        // t.x -> CURRENT, only if t matches the target table name (never "excluded")
        Qualified(ns, ast::Name::Ident(c)) if ns.as_str().eq_ignore_ascii_case(table_name) => {
            if let Some(reg) = col_reg(c.as_str()) {
                *e = Register(reg);
            }
        }
        // Unqualified column id -> CURRENT
        Id(ast::Name::Ident(name)) => {
            if let Some(reg) = col_reg(name.as_str()) {
                *e = Register(reg);
            }
        }
        RowId { .. } => {
            *e = Register(conflict_rowid_reg);
        }
        Collate(inner, _) => rewrite_upsert_expr_in_place(
            inner,
            table,
            table_name,
            current_start,
            conflict_rowid_reg,
            insertion,
        )?,
        Parenthesized(v) => {
            for ex in v {
                rewrite_upsert_expr_in_place(
                    ex,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
        }
        Between {
            lhs, start, end, ..
        } => {
            rewrite_upsert_expr_in_place(
                lhs,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
            rewrite_upsert_expr_in_place(
                start,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
            rewrite_upsert_expr_in_place(
                end,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
        }
        Binary(l, _, r) => {
            rewrite_upsert_expr_in_place(
                l,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
            rewrite_upsert_expr_in_place(
                r,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
        }
        Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(b) = base {
                rewrite_upsert_expr_in_place(
                    b,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
            for (w, t) in when_then_pairs.iter_mut() {
                rewrite_upsert_expr_in_place(
                    w,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
                rewrite_upsert_expr_in_place(
                    t,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
            if let Some(e2) = else_expr {
                rewrite_upsert_expr_in_place(
                    e2,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
        }
        Cast { expr: inner, .. } => {
            rewrite_upsert_expr_in_place(
                inner,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
        }
        FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            for a in args {
                rewrite_upsert_expr_in_place(
                    a,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
            for sc in order_by {
                rewrite_upsert_expr_in_place(
                    &mut sc.expr,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
            if let Some(ref mut f) = &mut filter_over.filter_clause {
                rewrite_upsert_expr_in_place(
                    f,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
        }
        InList { lhs, rhs, .. } => {
            rewrite_upsert_expr_in_place(
                lhs,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
            for ex in rhs {
                rewrite_upsert_expr_in_place(
                    ex,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
        }
        InSelect { lhs, .. } => {
            // rewrite only `lhs`, not the subselect
            rewrite_upsert_expr_in_place(
                lhs,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
        }
        InTable { lhs, .. } => {
            rewrite_upsert_expr_in_place(
                lhs,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
        }
        IsNull(inner) => {
            rewrite_upsert_expr_in_place(
                inner,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
        }
        Like {
            lhs, rhs, escape, ..
        } => {
            rewrite_upsert_expr_in_place(
                lhs,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
            rewrite_upsert_expr_in_place(
                rhs,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
            if let Some(e3) = escape {
                rewrite_upsert_expr_in_place(
                    e3,
                    table,
                    table_name,
                    current_start,
                    conflict_rowid_reg,
                    insertion,
                )?;
            }
        }
        NotNull(inner) => {
            rewrite_upsert_expr_in_place(
                inner,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
        }
        Unary(_, inner) => {
            rewrite_upsert_expr_in_place(
                inner,
                table,
                table_name,
                current_start,
                conflict_rowid_reg,
                insertion,
            )?;
        }

        _ => {}
    }
    Ok(())
}
