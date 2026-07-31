use std::num::NonZeroUsize;
use std::sync::Arc;

use turso_parser::ast::{self, TriggerEvent, TriggerTime};

use super::emitter::gencol::compute_planned_virtual_columns;
use crate::alloc::TursoIteratorExt;
use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::schema::{BTreeTable, ColumnLayout, Table, EXPR_INDEX_SENTINEL, ROWID_SENTINEL};
use crate::translate::emitter::{
    emit_check_constraints, emit_index_column_value_new_image, emit_make_record, UpdateRowSource,
};
use crate::translate::fkeys::{
    emit_fk_child_update_counters, emit_fk_update_parent_actions, fire_fk_update_actions,
    ParentKeyNewProbeMode,
};
use crate::translate::insert::{format_unique_violation_desc, InsertEmitCtx};
use crate::translate::plan::{
    ColumnMask, EvalAt, JoinedTable, NonFromClauseSubquery, PlanRuntimeBindings, RuntimeRowBinding,
    RuntimeSubqueryBinding, RuntimeValueBinding, SourceReadPrograms,
};
use crate::translate::plan_expr::{plan_expr_dependencies, PlanColumnUse, PlanExpr, PlanSourceId};
use crate::translate::semantic::hir::ResolvedIndex;
use crate::translate::subquery::emit_non_from_clause_subquery;
use crate::translate::trigger_exec::{
    fire_trigger, get_triggers_including_temp, has_triggers_including_temp, TriggerContext,
};
use crate::vdbe::insn::{to_u32, CmpInsFlags};
use crate::{
    error::SQLITE_CONSTRAINT_NOTNULL,
    translate::{
        emitter::{
            emit_cdc_full_record, emit_cdc_insns, emit_cdc_patch_record, OperationMode, Resolver,
        },
        expr::{
            emit_plan_source_decode_columns, emit_plan_source_decode_columns_for_reencode,
            emit_plan_source_encode_columns, emit_returning_results, emit_table_column,
            translate_plan_condition_expr, translate_plan_expr_no_constant_opt, ConditionMetadata,
            NoConstantOptReason,
        },
        insert::Insertion,
        plan::{ResultSetColumn, TableReferences},
    },
    vdbe::{
        affinity::Affinity,
        builder::{DmlColumnContext, ProgramBuilder},
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

/// One planned UPSERT action and the branch that enters it.
pub type PlannedUpsertAction = (
    ResolvedUpsertTarget,
    crate::vdbe::BranchOffset,
    PlannedUpsertDo,
);

#[derive(Clone, Debug)]
pub enum PlannedUpsertDo {
    Nothing,
    Update {
        sets: Vec<(usize, PlanExpr)>,
        predicate: Option<PlanExpr>,
    },
}

/// Returns the changed stored columns and whether the rowid changed.
fn collect_changed_cols(table: &Table, set_pairs: &[(usize, PlanExpr)]) -> (ColumnMask, bool) {
    let mut cols_changed = ColumnMask::default();
    let mut rowid_changed = false;
    for (col_idx, _) in set_pairs {
        if *col_idx == ROWID_SENTINEL {
            rowid_changed = true;
        } else if let Some(c) = table.columns().get(*col_idx) {
            if c.is_rowid_alias() {
                rowid_changed = true;
            } else {
                cols_changed.set(*col_idx).expect("TODO: alloc error");
            }
        }
    }
    (cols_changed, rowid_changed)
}

#[inline]
fn upsert_index_is_affected(
    target: &JoinedTable,
    index: &ResolvedIndex,
    directly_changed_cols: &ColumnMask,
    rowid_changed: bool,
) -> crate::Result<bool> {
    if rowid_changed {
        return Ok(true);
    }

    for c in referenced_index_cols(target, index)? {
        if directly_changed_cols.get(c) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Collect the target columns used by a planned index key or predicate.
fn referenced_index_cols(target: &JoinedTable, index: &ResolvedIndex) -> crate::Result<ColumnMask> {
    let mut referenced_cols = ColumnMask::default();
    let planned = target
        .plan_index_expressions(index.value())
        .expect("semantic HIR provided expressions for every catalog index");
    for expr in planned
        .predicate
        .iter()
        .chain(planned.columns.iter().flatten())
    {
        let dependencies = plan_expr_dependencies(expr)?;
        for (source, column) in dependencies.source_uses {
            if source != target.internal_id {
                continue;
            }
            if let PlanColumnUse::Column(column) = column {
                referenced_cols.set(column)?;
            }
        }
    }
    for (position, column) in index.value().columns.iter().enumerate() {
        if planned.columns.get(position).is_none_or(Option::is_none)
            && column.pos_in_table != EXPR_INDEX_SENTINEL
        {
            referenced_cols.set(column.pos_in_table)?;
        }
    }
    match target.table.btree() {
        Some(table) => table.dependencies_of_columns(referenced_cols),
        None => Ok(referenced_cols),
    }
}

#[derive(Clone, Debug)]
pub enum ResolvedUpsertTarget {
    CatchAll,
    PrimaryKey,
    Index(ResolvedIndex),
}

pub fn resolved_upsert_target(
    upsert: &crate::translate::semantic::hir::Upsert,
) -> ResolvedUpsertTarget {
    match &upsert.target {
        None => ResolvedUpsertTarget::CatchAll,
        Some(target) => match &target.matched_index {
            Some(index) => ResolvedUpsertTarget::Index(index.clone()),
            None => ResolvedUpsertTarget::PrimaryKey,
        },
    }
}

fn runtime_row_binding(
    table: &Table,
    columns_start_reg: usize,
    rowid_reg: usize,
    layout: &ColumnLayout,
    needs_decode: bool,
    read_programs: Option<Arc<SourceReadPrograms>>,
) -> RuntimeRowBinding {
    RuntimeRowBinding {
        columns: table
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| RuntimeValueBinding::Register {
                register: if column.is_rowid_alias() {
                    rowid_reg
                } else {
                    layout.to_register(columns_start_reg, index)
                },
                needs_decode,
            })
            .collect(),
        rowid: Some(RuntimeValueBinding::Register {
            register: rowid_reg,
            needs_decode: false,
        }),
        read_programs,
    }
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
/// 3. Copy CURRENT into NEW, then evaluate SET expressions into NEW, mapping
///    planned target-table columns to the CURRENT registers (per SQLite semantics).
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
    set_pairs: &[(usize, PlanExpr)],
    predicate: Option<&PlanExpr>,
    subqueries: &mut [NonFromClauseSubquery],
    excluded_source: PlanSourceId,
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
    let num_cols = ctx.table.columns().len();
    let layout = ctx.table.column_layout()?;
    let has_virtual_columns = layout.num_non_virtual_cols() != num_cols;

    let table_ref_id = table_references
        .joined_tables()
        .first()
        .expect("upsert must have a target table")
        .internal_id;
    let current_start = program.alloc_registers(num_cols);
    for i in 0..num_cols {
        let col = &table.columns()[i];
        let reg = layout.to_register(current_start, i);
        emit_table_column(
            program,
            ctx.cursor_id,
            table_ref_id,
            table_references,
            col,
            i,
            reg,
            resolver,
        )?;
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
            emit_plan_source_decode_columns(
                program,
                Some(table_references),
                ctx.target,
                decoded_current,
                None,
                &layout,
                resolver,
            )?;
            // Decode new_start in-place (was copied from encoded current_start;
            // after SET applies decoded values, we encode ALL columns)
            emit_plan_source_decode_columns_for_reencode(
                program,
                Some(table_references),
                ctx.target,
                new_start,
                None,
                &layout,
                resolver,
            )?;
            // Create decoded copies of excluded (insertion) registers so that
            // excluded.column references see user-facing values
            let decoded_excluded = program.alloc_registers(num_cols);
            program.emit_insn(Insn::Copy {
                src_reg: insertion.first_col_register(),
                dst_reg: decoded_excluded,
                extra_amount: num_cols - 1,
            });
            emit_plan_source_decode_columns(
                program,
                Some(table_references),
                ctx.target,
                decoded_excluded,
                None,
                &layout,
                resolver,
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

    let excluded_start = excluded_decoded_start.unwrap_or(insertion.first_col_register());
    let mut runtime_bindings = {
        let bindings = resolver.plan_runtime_bindings();
        (*bindings).clone()
    };
    for subquery in subqueries.iter() {
        runtime_bindings.bind_subquery(
            subquery.internal_id,
            RuntimeSubqueryBinding {
                query_type: subquery.query_type.clone(),
                output_facts: subquery.output_facts.clone(),
            },
        );
    }
    runtime_bindings.bind_row(
        table_ref_id,
        runtime_row_binding(
            table,
            expr_current_start,
            ctx.conflict_rowid_reg,
            &layout,
            false,
            None,
        ),
    );
    runtime_bindings.bind_row(
        excluded_source,
        runtime_row_binding(
            table,
            excluded_start,
            insertion.key_register(),
            &layout,
            false,
            None,
        ),
    );

    let new_rowid_reg = resolver.with_plan_runtime_bindings(
        runtime_bindings,
        |resolver| -> crate::Result<Option<usize>> {
            for subquery in subqueries
                .iter_mut()
                .filter(|subquery| !subquery.has_been_evaluated())
            {
                let rerun_for_target_scan = subquery.reads_table(ctx.database_id, table.get_name());
                let plan = subquery.consume_plan(EvalAt::Loop(0));
                emit_non_from_clause_subquery(
                    program,
                    resolver,
                    *plan,
                    &subquery.query_type,
                    subquery.correlated || rerun_for_target_scan,
                    true,
                )?;
            }
            if let Some(pred) = predicate {
                let predicate_true = program.allocate_label();
                translate_plan_condition_expr(
                    program,
                    Some(table_references),
                    pred,
                    ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true: predicate_true,
                        jump_target_when_false: ctx.loop_labels.row_done,
                        jump_target_when_null: ctx.loop_labels.row_done,
                    },
                    resolver,
                )?;
                program.preassign_label_to_next_insn(predicate_true);
            }

            let mut new_rowid_reg = None;
            for (column_index, expr) in set_pairs {
                let destination = if *column_index == ROWID_SENTINEL {
                    new_rowid_reg
                        .get_or_insert_with(|| program.alloc_register())
                        .to_owned()
                } else {
                    layout.to_register(new_start, *column_index)
                };
                translate_plan_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    expr,
                    destination,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                let column = table.columns().get(*column_index);
                if column.is_some_and(|column| column.notnull() && !column.is_rowid_alias()) {
                    let column = column.unwrap();
                    program.emit_insn(Insn::HaltIfNull {
                        target_reg: destination,
                        err_code: SQLITE_CONSTRAINT_NOTNULL,
                        description: String::from(table.get_name())
                            + "."
                            + column.name.as_ref().unwrap(),
                    });
                }
                if *column_index == ROWID_SENTINEL
                    || column.is_some_and(|column| column.is_rowid_alias())
                {
                    let register = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: destination,
                        dst_reg: register,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::MustBeInt {
                        reg: register,
                        target_pc: None,
                    });
                    new_rowid_reg = Some(register);
                }
            }
            Ok(new_rowid_reg)
        },
    )?;

    // Recompute virtual columns for the new row after SET clauses have modified base columns.
    // This must happen before CHECK constraints, triggers, and index updates.
    let rowid_reg = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
    let dml_ctx =
        DmlColumnContext::layout(ctx.table.columns(), new_start, rowid_reg, layout.clone());
    if has_virtual_columns {
        compute_planned_virtual_columns(program, ctx.target, &dml_ctx, resolver)?;
    }

    if let Some(bt) = table.btree() {
        if bt.is_strict {
            // Pre-encode TypeCheck: all columns are decoded (user-facing) at this point.
            program.emit_insn(Insn::TypeCheck {
                start_reg: new_start,
                count: layout.num_non_virtual_cols(),
                check_generated: true,
                table_reference: BTreeTable::input_type_check_table_ref(
                    &bt,
                    resolver.schema(),
                    None,
                )?,
            });

            // Encode ALL columns. Both non-SET columns (decoded from disk above)
            // and SET columns (user-facing values from expressions) need encoding
            // before being written to disk.
            emit_plan_source_encode_columns(
                program,
                Some(table_references),
                ctx.target,
                new_start,
                None,
                &layout,
                resolver,
            )?;

            // Post-encode TypeCheck: validate encoded values match storage type.
            program.emit_insn(Insn::TypeCheck {
                start_reg: new_start,
                count: layout.num_non_virtual_cols(),
                check_generated: true,
                table_reference: BTreeTable::type_check_table_ref(&bt, resolver.schema()),
            });
        } else {
            // For non-STRICT tables, apply column affinity to the values.
            // This must happen early so that both index records and the table record
            // use the converted values.
            let affinity = bt
                .columns()
                .iter()
                .filter(|c| !c.is_virtual_generated())
                .map(|c| c.affinity());

            if affinity.clone().any(|a| a != Affinity::Blob) {
                if let Ok(count) = NonZeroUsize::try_from(layout.num_non_virtual_cols()) {
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
            ctx.target,
            &ctx.target.check_constraints,
            &dml_ctx,
            rowid_reg,
            resolver,
            connection,
            ast::ResolveType::Abort,
            ctx.loop_labels.row_done,
        )?;
    }

    let (directly_changed_cols, rowid_changed) = collect_changed_cols(table, set_pairs);

    // Fire BEFORE UPDATE triggers
    let upsert_database_id = ctx.database_id;
    let preserved_old_registers: Option<Vec<usize>> = if let Some(btree_table) = table.btree() {
        let updated_column_indices: ColumnMask = set_pairs
            .iter()
            .filter_map(|(col_idx, _)| (*col_idx != ROWID_SENTINEL).then_some(*col_idx))
            .try_collect()?;
        let relevant_before_update_triggers = get_triggers_including_temp(
            resolver,
            upsert_database_id,
            TriggerEvent::Update,
            TriggerTime::Before,
            Some(updated_column_indices.clone()),
            &btree_table,
        );
        // OLD row values are in current_start registers
        let old_registers: Vec<usize> = (0..num_cols)
            .map(|i| layout.to_register(current_start, i))
            .chain(std::iter::once(ctx.conflict_rowid_reg))
            .collect();
        if !relevant_before_update_triggers.is_empty() {
            // NEW row values are in new_start registers. At this point they are
            // encoded (post-encode for STRICT custom types). Mark new_encoded=true
            // so fire_trigger's decode_trigger_registers will decode them.
            let new_rowid_for_trigger = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
            let new_registers: Vec<usize> = (0..num_cols)
                .map(|i| layout.to_register(new_start, i))
                .chain(std::iter::once(new_rowid_for_trigger))
                .collect();

            // In UPSERT DO UPDATE context, trigger's INSERT/UPDATE OR IGNORE/REPLACE
            // clauses should not suppress errors. Override conflict resolution to Abort.
            // Use new_after variant because NEW values are encoded at this point.
            let trigger_ctx = TriggerContext::new_after_with_override_conflict(
                btree_table.clone(),
                Arc::clone(&ctx.target.read_programs),
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

            let has_relevant_after_triggers = has_triggers_including_temp(
                resolver,
                upsert_database_id,
                TriggerEvent::Update,
                Some(&updated_column_indices),
                &btree_table,
            );
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
            let has_relevant_after_triggers = has_triggers_including_temp(
                resolver,
                upsert_database_id,
                TriggerEvent::Update,
                Some(&updated_column_indices),
                &btree_table,
            );
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
    let has_direct_rowid_update = set_pairs.iter().any(|(idx, _)| *idx == ROWID_SENTINEL);
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
    let updated_positions: ColumnMask = set_pairs
        .iter()
        .filter_map(|(col_idx, _)| (*col_idx != ROWID_SENTINEL).then_some(*col_idx))
        .try_collect()?;
    if let Some(bt) = table.btree() {
        if connection.foreign_keys_enabled() {
            let rowid_new_reg = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);

            // Child-side checks
            if resolver.with_schema(upsert_database_id, |s| s.has_child_fks(bt.name.as_str())) {
                emit_fk_child_update_counters(
                    program,
                    ctx.target,
                    ctx.cursor_id,
                    new_start,
                    rowid_new_reg,
                    &directly_changed_cols,
                    resolver,
                    &layout,
                )?;
            }
            let target_table = table_references
                .joined_tables()
                .first()
                .expect("UPSERT target table must exist");
            let affected_upsert_indices: Vec<_> = ctx
                .indexes
                .iter()
                .filter_map(|idx| {
                    upsert_index_is_affected(
                        target_table,
                        idx,
                        &directly_changed_cols,
                        rowid_changed,
                    )
                    .map(|affected| affected.then(|| idx.handle()))
                    .transpose()
                })
                .collect::<crate::Result<_>>()?;
            let _ = emit_fk_update_parent_actions(
                program,
                ctx.target,
                affected_upsert_indices.iter(),
                ctx.cursor_id,
                ctx.conflict_rowid_reg,
                new_start,
                new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg),
                rowid_set_clause_reg,
                &updated_positions,
                ParentKeyNewProbeMode::BeforeWrite,
                resolver,
            )?;
        }
    }

    // Index maintenance (DELETE old key, INSERT new key), honoring
    // partial-index WHEREs. Mirroring SQLite, every UNIQUE constraint (the
    // rowid first, then each unique index) is verified against the NEW row
    // image before any index entry is touched: a constraint failure must
    // abort the statement without leaving the indexes out of sync with the
    // table (issue #6858).
    let new_rowid = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);

    // If SET changed the rowid, ensure no other row already owns the new one.
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
            description_reg: None,
        });
        program.preassign_label_to_next_insn(ok);
    }

    struct PendingIndexRebuild {
        idx_cid: usize,
        idx_meta: ResolvedIndex,
        before_pred_reg: Option<usize>,
        new_pred_reg: Option<usize>,
        ins_start: usize,
        record_reg: usize,
    }

    if let Some(before) = before_start {
        let mut pending_rebuilds: Vec<PendingIndexRebuild> = Vec::new();
        let target_table = table_references
            .joined_tables()
            .first()
            .expect("UPSERT target table must exist");

        // Pass 1: compute the NEW key for every affected index and probe the
        // unique ones for conflicts, without modifying any index yet.
        for (idx_name, _root, idx_cid) in &ctx.idx_cursors {
            let idx_meta = ctx
                .indexes
                .iter()
                .find(|index| index.value().name == *idx_name)
                .expect("INSERT cursor must retain its resolved index")
                .clone();

            if !upsert_index_is_affected(
                target_table,
                &idx_meta,
                &directly_changed_cols,
                rowid_changed,
            )? {
                continue; // skip untouched index completely
            }
            let k = idx_meta.value().columns.len();

            let before_pred_reg = eval_partial_pred_for_row_image(
                program,
                target_table,
                &idx_meta,
                before,
                ctx.conflict_rowid_reg,
                resolver,
                &layout,
            )?;
            let new_pred_reg = eval_partial_pred_for_row_image(
                program,
                target_table,
                &idx_meta,
                new_start,
                new_rowid,
                resolver,
                &layout,
            )?;

            // Skip key computation and probe if NEW predicate false/NULL:
            // a key that fails the partial-index predicate is never inserted,
            // so it cannot conflict.
            let maybe_skip_probe = new_pred_reg.map(|r| {
                let lbl = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: r,
                    target_pc: lbl,
                    jump_if_null: true,
                });
                lbl
            });

            // NEW key (use NEW rowid if present)
            let ins = program.alloc_registers(k + 1);
            for i in 0..k {
                emit_upsert_index_value(
                    program,
                    resolver,
                    target_table,
                    &idx_meta,
                    i,
                    new_start,
                    new_rowid,
                    ins + i,
                    &layout,
                )?;
            }
            program.emit_insn(Insn::Copy {
                src_reg: new_rowid,
                dst_reg: ins + k,
                extra_amount: 0,
            });

            let rec = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(ins),
                count: to_u32(k + 1),
                dest_reg: to_u32(rec),
                index_name: Some(idx_name.clone()),
                affinity_str: None,
            });

            if idx_meta.value().unique {
                // Affinity on the key columns for the NoConflict probe
                let ok = program.allocate_label();
                let aff: String = idx_meta
                    .value()
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
                // A hit on the row being updated is not a conflict: its old
                // key is deleted before the new one is inserted below.
                program.emit_insn(Insn::Eq {
                    lhs: ctx.conflict_rowid_reg,
                    rhs: hit,
                    target_pc: ok,
                    flags: CmpInsFlags::default(),
                    collation: program.curr_collation(),
                });
                let description = format_unique_violation_desc(table.get_name(), idx_meta.value());
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                    description,
                    on_error: None,
                    description_reg: None,
                });
                program.preassign_label_to_next_insn(ok);
            }

            if let Some(lbl) = maybe_skip_probe {
                program.preassign_label_to_next_insn(lbl);
            }

            pending_rebuilds.push(PendingIndexRebuild {
                idx_cid: *idx_cid,
                idx_meta,
                before_pred_reg,
                new_pred_reg,
                ins_start: ins,
                record_reg: rec,
            });
        }

        // Pass 2: every UNIQUE constraint holds, so the index mutations can
        // no longer be interrupted by a constraint failure.
        for pending in pending_rebuilds {
            let k = pending.idx_meta.value().columns.len();

            // Skip delete if BEFORE predicate false/NULL
            let maybe_skip_del = pending.before_pred_reg.map(|r| {
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
            for i in 0..k {
                emit_upsert_index_value(
                    program,
                    resolver,
                    target_table,
                    &pending.idx_meta,
                    i,
                    before,
                    ctx.conflict_rowid_reg,
                    del + i,
                    &layout,
                )?;
            }
            program.emit_insn(Insn::Copy {
                src_reg: ctx.conflict_rowid_reg,
                dst_reg: del + k,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: del,
                num_regs: k + 1,
                cursor_id: pending.idx_cid,
                raise_error_if_no_matching_entry: false,
            });
            if let Some(label) = maybe_skip_del {
                program.preassign_label_to_next_insn(label);
            }

            // Skip insert if NEW predicate false/NULL
            let maybe_skip_ins = pending.new_pred_reg.map(|r| {
                let lbl = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: r,
                    target_pc: lbl,
                    jump_if_null: true,
                });
                lbl
            });

            program.emit_insn(Insn::IdxInsert {
                cursor_id: pending.idx_cid,
                record_reg: pending.record_reg,
                unpacked_start: Some(pending.ins_start),
                unpacked_count: Some((k + 1) as u32),
                flags: IdxInsertFlags::new().nchange(true),
            });

            if let Some(lbl) = maybe_skip_ins {
                program.preassign_label_to_next_insn(lbl);
            }
        }
    }

    // Build NEW table payload
    let record_reg = program.alloc_register();
    emit_make_record(
        program,
        table.columns().iter(),
        new_start,
        record_reg,
        table.btree().is_some_and(|bt| bt.is_strict),
    );

    // If rowid changed, delete+insert (uniqueness of the new rowid was
    // already verified before index maintenance above)
    if let Some(rnew) = new_rowid_reg {
        // important: the cursor was repositioned in the earlier rowid uniqueness
        // probe via NotExists, so we need to re-seek to the row under update.
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
            record_reg,
            flag: InsertFlags::new()
                .require_seek()
                .update_rowid_change()
                .skip_last_rowid(),
            table_name: table.get_name().to_string(),
        });

        // MVCC AUTOINCREMENT: an ON CONFLICT DO UPDATE that moves the rowid
        // forward must advance the implicit sequence past the new rowid, just
        // like an explicit-rowid INSERT or a plain UPDATE does. The MVCC
        // allocator trusts the backing-table watermark ONLY (it never consults
        // MAX(rowid)), so without this a later AUTOINCREMENT INSERT would emit
        // a value at or below the manually-set rowid and eventually collide.
        // WAL mode is unaffected: its NewRowid path already takes the max of
        // sqlite_sequence.seq and MAX(rowid), so it skips past the new rowid
        // automatically. Mirrors the UPDATE path in emitter/update.rs.
        if table.btree().is_some_and(|bt| bt.has_autoincrement)
            && connection.mv_store_for_db(upsert_database_id).is_some()
        {
            let seq_name = crate::schema::autoincrement_sequence_name(table.get_name());
            let seq = resolver
                .with_schema(upsert_database_id, |s| s.get_sequence(&seq_name).cloned())
                .ok_or_else(|| {
                    crate::LimboError::InternalError(format!(
                        "missing implicit sequence for AUTOINCREMENT table \"{}\"",
                        table.get_name()
                    ))
                })?;
            crate::translate::sequence::emit_disk_advance_past(
                program,
                resolver,
                upsert_database_id,
                &seq_name,
                &seq,
                rnew,
            )?;
        }
    } else {
        program.emit_insn(Insn::Insert {
            cursor: ctx.cursor_id,
            key_reg: ctx.conflict_rowid_reg,
            record_reg,
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
                ctx.target,
                ctx.conflict_rowid_reg, // old_rowid_reg
                current_start,          // old_values_start
                new_start,              // new_values_start
                new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg), // new_rowid_reg
                connection,
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
                    program, table, new_start, record_reg, new_rowid, &layout,
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
                    record_reg,
                    ctx.conflict_rowid_reg,
                    &layout,
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
        let updated_column_indices: ColumnMask = set_pairs
            .iter()
            .filter_map(|(col_idx, _)| (*col_idx != ROWID_SENTINEL).then_some(*col_idx))
            .try_collect()?;
        let relevant_triggers = get_triggers_including_temp(
            resolver,
            upsert_database_id,
            TriggerEvent::Update,
            TriggerTime::After,
            Some(updated_column_indices),
            &btree_table,
        );
        if !relevant_triggers.is_empty() {
            let new_rowid_for_trigger = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
            let new_registers_after: Vec<usize> = (0..num_cols)
                .map(|i| layout.to_register(new_start, i))
                .chain(std::iter::once(new_rowid_for_trigger))
                .collect();

            // In UPSERT DO UPDATE context, trigger's INSERT/UPDATE OR IGNORE/REPLACE
            // clauses should not suppress errors. Override conflict resolution to Abort.
            // NEW values are encoded at this point; fire_trigger will decode them.
            let trigger_ctx_after = TriggerContext::new_after_with_override_conflict(
                btree_table,
                Arc::clone(&ctx.target.read_programs),
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

    // Compute virtual columns for RETURNING (if any virtual columns exist)
    if !returning.is_empty() && has_virtual_columns {
        let rowid_reg = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
        let dml_ctx =
            DmlColumnContext::layout(ctx.table.columns(), new_start, rowid_reg, layout.clone());
        compute_planned_virtual_columns(program, ctx.target, &dml_ctx, resolver)?;
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
            &layout,
        )?;
    }

    program.emit_insn(Insn::Goto {
        target_pc: ctx.loop_labels.row_done,
    });
    Ok(())
}

fn eval_partial_pred_for_row_image(
    program: &mut ProgramBuilder,
    target: &JoinedTable,
    index: &ResolvedIndex,
    row_start: usize, // base of CURRENT or NEW image
    rowid_reg: usize, // rowid for that image
    resolver: &Resolver,
    layout: &ColumnLayout,
) -> crate::Result<Option<usize>> {
    let Some(predicate) = target.partial_index_predicate(index.value()) else {
        return Ok(None);
    };
    let mut bindings = PlanRuntimeBindings::default();
    bindings.bind_row(
        target.internal_id,
        runtime_row_binding(
            &target.table,
            row_start,
            rowid_reg,
            layout,
            true,
            Some(Arc::clone(&target.read_programs)),
        ),
    );
    let register = program.alloc_register();
    resolver.with_plan_runtime_bindings(bindings, |resolver| {
        translate_plan_expr_no_constant_opt(
            program,
            None,
            predicate,
            register,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )
    })?;
    Ok(Some(register))
}

#[allow(clippy::too_many_arguments)]
fn emit_upsert_index_value(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    target: &JoinedTable,
    index: &ResolvedIndex,
    index_column: usize,
    row_start: usize,
    rowid_reg: usize,
    dest_reg: usize,
    layout: &ColumnLayout,
) -> crate::Result<()> {
    let mut bindings = PlanRuntimeBindings::default();
    bindings.bind_row(
        target.internal_id,
        runtime_row_binding(
            &target.table,
            row_start,
            rowid_reg,
            layout,
            true,
            Some(Arc::clone(&target.read_programs)),
        ),
    );
    resolver.with_plan_runtime_bindings(bindings, |resolver| {
        emit_index_column_value_new_image(program, resolver, target, index, index_column, dest_reg)
    })
}
