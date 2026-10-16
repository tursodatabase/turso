//! DELETE lowering from resolved HIR.
//!
//! This covers a plain DELETE from a B-tree table: one loop over the planned
//! access path, index entries removed with `IdxDelete`, then the row removed
//! with `Delete`. RETURNING rows are buffered and returned after the last
//! row is deleted. A WHERE clause with a subquery first collects the rowids,
//! as legacy does. Statements that need foreign-key actions, a RETURNING
//! subquery, CDC, or a virtual table still go through legacy; see
//! [`needs_legacy`].

use turso_parser::ast::TriggerTime;

use crate::{
    translate::{
        optimizer::HirBtreeOperation,
        semantic::hir::{self, HirDocument},
        semantic_to_plan::{HirAccessPlan, HirPlan, HirSourceAccess},
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder, SourceBinding},
        insn::{Insn, RegisterOrLiteral},
        BranchOffset, CursorID,
    },
    CaptureDataChangesExt, LimboError, Result,
};

use super::dml::{
    emit_access_predicates, emit_index_key, has_subquery, operation_index, touches_at_most_one_row,
    DmlTarget, ReturningBuffer, Subprograms,
};
use super::fkey_action::CompiledAction;
use super::query::{
    close_hir_btree_loop, hir_loop_explain, prepare_hir_btree_loop, start_hir_btree_loop,
    PreparedHirBtreeLoop,
};
use super::trigger::{CompiledTrigger, TriggerRow};
use crate::translate::emitter::OperationMode;

/// Whether this DELETE uses something the HIR lowering does not handle yet.
/// It is checked before any bytecode is emitted, so legacy can still take
/// the whole statement.
pub(super) fn needs_legacy(
    document: &HirDocument,
    delete: &hir::Delete,
    access: &HirAccessPlan,
    program: &ProgramBuilder,
) -> Result<bool> {
    if !delete.order_by.is_empty() || delete.limit.is_some() {
        return Ok(true);
    }
    // Change data capture of a virtual table is not lowered yet.
    if let Some(table) = super::dml::virtual_target(document, delete.target)? {
        return Ok(program.capture_data_changes_info().table().is_some()
            || super::dml::virtual_needs_legacy(&table));
    }
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE plan has {} loops for one target",
            access.loops.len()
        )));
    };
    match &planned_loop.access {
        HirSourceAccess::BTree { operation, .. } => {
            Ok(operation_index(operation).is_some_and(|index| index.ephemeral))
        }
        _ => Ok(true),
    }
}

/// Whether a RETURNING value of the DELETE has a subquery.
fn returning_has_subquery(delete: &hir::Delete) -> bool {
    delete.returning.as_ref().is_some_and(|returning| {
        returning
            .outputs
            .iter()
            .any(|output| has_subquery(&output.expr))
    })
}

/// Emit a DELETE that [`needs_legacy`] accepted. `delete` is the plan's
/// root or one command of a trigger in it.
pub(super) fn emit_delete(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    delete: &hir::Delete,
    access: &HirAccessPlan,
    subprograms: &Subprograms<'_>,
) -> Result<()> {
    let triggers = &subprograms.triggers[..];
    let actions = &subprograms.actions[..];
    let document = &plan.document;
    if let Some(table) = super::dml::virtual_target(document, delete.target)? {
        return emit_virtual_delete(program, plan, delete, access, &table);
    }
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE plan has {} loops for one target",
            access.loops.len()
        )));
    };
    if planned_loop.source != delete.target {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE loop reads source {} instead of its target {}",
            planned_loop.source, delete.target
        )));
    }
    let HirSourceAccess::BTree { operation, .. } = &planned_loop.access else {
        return Err(LimboError::InternalError(
            "HIR DELETE target does not use a B-tree loop".into(),
        ));
    };
    let hir::DeleteTargetKind::BTree {
        foreign_keys,
        capture_changes,
        ..
    } = &delete.target_kind
    else {
        return Err(LimboError::InternalError(
            "HIR DELETE target is not a B-tree table".into(),
        ));
    };
    let target = DmlTarget::new(program, document, delete.target, "DELETE")?
        .with_capture(program, capture_changes.as_ref())?;
    // Like legacy, a DELETE that fires triggers or foreign-key actions
    // first collects its rowids, since they can change the table.
    let uses_rowset = uses_rowset(delete) || !triggers.is_empty() || !actions.is_empty();
    // Same statement-journal facts as legacy: without triggers or foreign
    // keys a DELETE cannot abort, and a point lookup writes at most one row.
    // After collecting rowids, legacy treats the delete as a scan.
    let has_foreign_keys = !foreign_keys.incoming.is_empty() || !foreign_keys.outgoing.is_empty();
    if !uses_rowset && !has_foreign_keys && touches_at_most_one_row(operation) {
        program.set_multi_write(false);
    }
    program.set_may_abort(!triggers.is_empty() || has_foreign_keys);

    program.emit_source_explain(
        target.source.id,
        hir_loop_explain(document, None, planned_loop)?,
    );
    let returning = ReturningBuffer::open(program, target.table, delete.returning.as_ref());
    let done = program.allocate_label();
    if uses_rowset {
        emit_rowset_delete(
            program,
            plan,
            &target,
            access,
            foreign_keys,
            returning,
            triggers,
            actions,
            done,
        )?;
    } else {
        emit_scan_delete(
            program,
            &target,
            access,
            operation,
            foreign_keys,
            returning,
            done,
        )?;
    }
    program.preassign_label_to_next_insn(done);
    target.emit_capture_commit(program)?;
    if let Some(returning) = returning {
        returning.emit_rows(program);
    }
    Ok(())
}

/// Whether the DELETE first collects the rowids of the rows to delete, then
/// deletes them. Like legacy, a WHERE clause with a subquery does this, so
/// the subquery never reads a table that is changing under it.
fn uses_rowset(delete: &hir::Delete) -> bool {
    delete.predicate.as_ref().is_some_and(has_subquery)
}

/// Delete each row as the loop reaches it.
fn emit_scan_delete(
    program: &mut ProgramBuilder,
    target: &DmlTarget<'_>,
    access: &HirAccessPlan,
    operation: &HirBtreeOperation,
    foreign_keys: &hir::DmlForeignKeys,
    returning: Option<ReturningBuffer<'_>>,
    done: BranchOffset,
) -> Result<()> {
    let document = target.document;
    // Like legacy, open the table, then the index the loop reads, then every
    // other index, all for writing.
    let iteration_index = operation_index(operation);
    let table_cursor = target.open_table_for_write(program);
    let iteration = iteration_index
        .map(|index| -> Result<_> {
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(index.root_page),
                db: target.database,
            });
            Ok((cursor, target.index_expressions(index)?))
        })
        .transpose()?;
    let other_indexes = target.open_indexes_for_write(program, iteration_index)?;

    let scan_cursor = iteration.map_or(table_cursor, |(cursor, _)| cursor);
    let lookup_cursor = iteration.map(|_| table_cursor);
    program.bind_source(
        target.source.id,
        SourceBinding::BTree {
            scan_cursor,
            table_cursor: lookup_cursor,
        },
    );
    let prepared = PreparedHirBtreeLoop {
        source: target.source,
        operation,
        index: iteration_index,
        cursor: scan_cursor,
        table_cursor: lookup_cursor,
        autoindex_source_cursor: None,
        table_has_rowid: target.table.has_rowid,
        index_keys: Vec::new(),
    };
    emit_access_predicates(program, document, access, None, done)?;
    let hir_loop = start_hir_btree_loop(program, document, &prepared, None)?;
    emit_access_predicates(program, document, access, Some(0), hir_loop.next)?;

    // RowId on the table cursor also finishes a deferred seek from the
    // index, so the table cursor sits on the row before it is deleted.
    let rowid = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: table_cursor,
        dest: rowid,
    });
    emit_delete_row(
        program,
        target,
        DeletedRow {
            table_cursor,
            rowid,
            foreign_keys,
            actions: &[],
        },
        iteration,
        &other_indexes,
        returning,
    )?;
    close_hir_btree_loop(program, &hir_loop, &[]);
    Ok(())
}

/// Collect the rowids of the rows to delete with read cursors, then delete
/// each collected row.
#[allow(clippy::too_many_arguments)]
fn emit_rowset_delete(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    target: &DmlTarget<'_>,
    access: &HirAccessPlan,
    foreign_keys: &hir::DmlForeignKeys,
    returning: Option<ReturningBuffer<'_>>,
    triggers: &[CompiledTrigger],
    actions: &[CompiledAction<'_>],
    done: BranchOffset,
) -> Result<()> {
    let document = target.document;
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE plan has {} loops for one target",
            access.loops.len()
        )));
    };
    let rowset = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: rowset,
        dest_end: None,
    });
    let collected = program.allocate_label();
    let prepared = prepare_hir_btree_loop(program, plan, planned_loop)?;
    emit_access_predicates(program, document, access, None, collected)?;
    let hir_loop = start_hir_btree_loop(program, document, &prepared, None)?;
    emit_access_predicates(program, document, access, Some(0), hir_loop.next)?;
    let rowid = program.alloc_register();
    super::expr::translate_expr(
        program,
        document,
        &hir::Expr::RowId(target.source.id),
        rowid,
    )?;
    program.emit_insn(Insn::RowSetAdd {
        rowset_reg: rowset,
        value_reg: rowid,
    });
    close_hir_btree_loop(program, &hir_loop, &[]);
    program.preassign_label_to_next_insn(collected);
    for cursor in std::iter::once(prepared.cursor).chain(prepared.table_cursor) {
        program.emit_insn(Insn::Close { cursor_id: cursor });
    }

    let table_cursor = target.open_table_for_write(program);
    let indexes = target.open_indexes_for_write(program, None)?;
    let next = program.allocate_label();
    let rowid = program.alloc_register();
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::RowSetRead {
        rowset_reg: rowset,
        pc_if_empty: done,
        dest_reg: rowid,
    });
    program.emit_insn(Insn::NotExists {
        cursor: table_cursor,
        rowid_reg: rowid,
        target_pc: next,
    });
    let columns = target.table.columns().len();
    let old = (!triggers.is_empty())
        .then(|| -> Result<TriggerRow> {
            // The OLD row the triggers read, as a query would read it.
            program.bind_source(
                target.source.id,
                SourceBinding::BTree {
                    scan_cursor: table_cursor,
                    table_cursor: None,
                },
            );
            let start = program.alloc_registers(columns);
            // The trigger program decodes custom-typed values itself, so it is
            // given them as stored.
            super::dml::with_stored_values(program, |program| {
                for column in 0..columns {
                    super::expr::translate_expr_no_constant_opt(
                        program,
                        document,
                        &hir::Expr::Column(hir::ColumnRef {
                            source: target.source.id,
                            column,
                        }),
                        start + column,
                    )?;
                }
                Ok(())
            })?;
            Ok(TriggerRow {
                columns: start,
                rowid,
            })
        })
        .transpose()?;
    let fire = |program: &mut ProgramBuilder, time: TriggerTime| -> Result<()> {
        for trigger in triggers.iter().filter(|trigger| trigger.time == time) {
            // RAISE(IGNORE) skips the rest of this row.
            trigger.emit_fire(program, old, None, columns, next)?;
        }
        Ok(())
    };
    fire(program, TriggerTime::Before)?;
    if triggers
        .iter()
        .any(|trigger| trigger.time == TriggerTime::Before)
    {
        // A BEFORE trigger may have deleted or moved the row.
        program.emit_insn(Insn::NotExists {
            cursor: table_cursor,
            rowid_reg: rowid,
            target_pc: next,
        });
    }
    emit_delete_row(
        program,
        target,
        DeletedRow {
            table_cursor,
            rowid,
            foreign_keys,
            actions,
        },
        None,
        &indexes,
        returning,
    )?;
    fire(program, TriggerTime::After)?;
    program.emit_insn(Insn::Goto { target_pc: next });
    Ok(())
}

/// The row a DELETE is about to remove.
struct DeletedRow<'a> {
    /// The table cursor, positioned on the row.
    table_cursor: CursorID,
    /// The register holding the row's rowid.
    rowid: usize,
    foreign_keys: &'a hir::DmlForeignKeys,
    /// The ON DELETE actions to run once the row is gone.
    actions: &'a [CompiledAction<'a>],
}

/// DELETE from a virtual table, as legacy does it: each row the loop finds
/// is handed to the table with `VUpdate`, with its rowid and a NULL.
fn emit_virtual_delete(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    delete: &hir::Delete,
    access: &HirAccessPlan,
    table: &crate::sync::Arc<crate::vtab::VirtualTable>,
) -> Result<()> {
    if delete.returning.is_some() {
        crate::bail_parse_error!("DELETE RETURNING is not available on virtual tables");
    }
    if table.readonly() {
        return Err(LimboError::ReadOnly);
    }
    program.begin_write_operation()?;
    let cursor = super::dml::open_virtual_for_write(program, table);
    let document = &plan.document;
    super::query::emit_dml_join_loops(
        program,
        plan,
        None,
        super::query::HirLoopsPlan::of_access(access),
        &|program, _next| {
            let start = program.alloc_registers(2);
            super::expr::translate_expr(
                program,
                document,
                &hir::Expr::RowId(delete.target),
                start,
            )?;
            program.emit_null(start + 1, None);
            program.emit_insn(Insn::VUpdate {
                cursor_id: cursor,
                arg_count: 2,
                start_reg: start,
                conflict_action: 0,
            });
            Ok(())
        },
    )
}

/// Delete the row the table cursor is on: its foreign-key checks, its
/// RETURNING values, its index entries, then the row. `iteration` is the
/// index the loop reads, whose entry is removed last so the loop can still
/// advance.
fn emit_delete_row(
    program: &mut ProgramBuilder,
    target: &DmlTarget<'_>,
    row: DeletedRow<'_>,
    iteration: Option<(CursorID, &hir::IndexExpressions)>,
    other_indexes: &[(&hir::IndexExpressions, CursorID)],
    returning: Option<ReturningBuffer<'_>>,
) -> Result<()> {
    let DmlTarget {
        document,
        source,
        table,
        database,
        ..
    } = *target;
    let table_cursor = row.table_cursor;
    // Foreign-key keys, index keys and RETURNING values are read from the
    // table row, as legacy does.
    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: table_cursor,
            table_cursor: None,
        },
    );
    super::fkey::emit_delete_checks(
        program,
        document,
        row.foreign_keys,
        source.id,
        row.rowid,
        database,
        None,
    )?;
    // Like legacy, the parent key the actions need is read before the row
    // is deleted, and the actions run once it is gone.
    let action_keys = row
        .actions
        .iter()
        .map(|action| {
            super::fkey::emit_parent_key(program, document, action.foreign_key, source.id)
        })
        .collect::<Result<Vec<_>>>()?;
    // The key of the index the loop reads is taken before the row is
    // deleted and removed after it.
    let iteration_key = iteration
        .map(|(cursor, expressions)| -> Result<_> {
            let (start, count) =
                emit_index_key(program, document, source, table, table_cursor, expressions)?;
            Ok((cursor, start, count, expressions))
        })
        .transpose()?;
    // RETURNING reads the row before it is deleted, which gives the same
    // values as legacy, which reads a copy of the row after. A subquery in
    // RETURNING must not see the row any more, so then RETURNING reads a
    // copy of the row once it and its index entries are gone.
    let copy = returning
        .as_ref()
        .filter(|returning| {
            returning
                .outputs
                .iter()
                .any(|output| has_subquery(&output.expr))
        })
        .map(|_| {
            // A virtual column is not stored; reading it through the copy
            // computes it from the other columns.
            let start = program.alloc_registers(table.columns().len());
            for (position, column) in table.columns().iter().enumerate() {
                if column.is_virtual_generated() {
                    program.emit_null(start + position, None);
                } else {
                    program.emit_column_or_rowid(table_cursor, position, start + position);
                }
            }
            start
        });
    let returning_row = returning
        .filter(|_| copy.is_none())
        .map(|returning| returning.emit_values(program, document, target.resolved_table()?))
        .transpose()?;
    for (expressions, cursor) in other_indexes {
        let skip = match &expressions.predicate {
            Some(predicate) => {
                let skip = program.allocate_label();
                let register = program.alloc_register();
                super::expr::translate_expr_no_constant_opt(
                    program, document, predicate, register,
                )?;
                program.emit_insn(Insn::IfNot {
                    reg: register,
                    jump_if_null: true,
                    target_pc: skip,
                });
                Some(skip)
            }
            None => None,
        };
        let (start, count) =
            emit_index_key(program, document, source, table, table_cursor, expressions)?;
        program.emit_insn(Insn::IdxDelete {
            start_reg: start,
            num_regs: count,
            cursor_id: *cursor,
            raise_error_if_no_matching_entry: expressions.index.value().where_clause.is_none(),
        });
        if let Some(skip) = skip {
            program.preassign_label_to_next_insn(skip);
        }
    }
    // Like legacy, the captured change reads the row before it is deleted.
    if let Some(capture) = &target.capture {
        let before = capture.before_record(program, table, table_cursor, row.rowid);
        capture.emit_row(
            program,
            OperationMode::DELETE,
            row.rowid,
            before,
            None,
            None,
        )?;
    }
    program.emit_insn(Insn::Delete {
        cursor_id: table_cursor,
        table_name: table.name.clone(),
        is_part_of_update: false,
    });
    if let (Some(returning), Some(start)) = (returning, returning_row) {
        returning.emit_buffer(program, start);
    }
    if let Some((cursor, start, count, expressions)) = iteration_key {
        program.emit_insn(Insn::IdxDelete {
            start_reg: start,
            num_regs: count,
            cursor_id: cursor,
            raise_error_if_no_matching_entry: expressions.index.value().where_clause.is_none(),
        });
    }
    if let (Some(returning), Some(start)) = (returning, copy) {
        program.bind_source(
            source.id,
            SourceBinding::Registers {
                start,
                rowid: Some(row.rowid),
            },
        );
        let values = returning.emit_values(program, document, target.resolved_table()?)?;
        returning.emit_buffer(program, values);
    }
    for (action, key) in row.actions.iter().zip(action_keys) {
        action.emit_fire(program, key, None)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::semantic_to_plan::HirRootPlan;
    use crate::{
        schema::{BTreeTable, Schema},
        sync::Arc,
        translate::semantic_lowering::query::tests::{analyze_plan, indexed_items_schema, program},
    };

    fn root(plan: &HirPlan) -> (&hir::Delete, &HirAccessPlan) {
        match (&plan.document.root, &plan.root) {
            (hir::HirRoot::Delete(delete), HirRootPlan::Delete(access)) => (delete, access),
            _ => panic!("DELETE plan has a DELETE root"),
        }
    }

    /// Lower a DELETE that HIR accepts, as `translate_delete` does.
    fn emit(plan: HirPlan) -> ProgramBuilder {
        let plan = Arc::new(plan);
        let (delete, access) = root(&plan);
        let mut program = program();
        assert!(!needs_legacy(&plan.document, delete, access, &program).unwrap());
        program.set_hir_plan(plan.clone());
        emit_delete(&mut program, &plan, delete, access, &Subprograms::default()).unwrap();
        program
    }

    #[test]
    fn plain_delete_removes_the_row_and_every_index_entry() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "DELETE FROM items WHERE extra = 'x'",
        );
        let program = emit(plan);
        let count = |matches: fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches(insn))
                .count()
        };
        // The table and its one index.
        assert_eq!(count(|insn| matches!(insn, Insn::OpenWrite { .. })), 2);
        assert_eq!(count(|insn| matches!(insn, Insn::OpenRead { .. })), 0);
        assert_eq!(count(|insn| matches!(insn, Insn::IdxDelete { .. })), 1);
        assert_eq!(count(|insn| matches!(insn, Insn::Delete { .. })), 1);
    }

    #[test]
    fn delete_returning_buffers_rows_until_every_row_is_deleted() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "DELETE FROM items WHERE value = 1 RETURNING extra, value + 1",
        );
        let program = emit(plan);
        let position = |matches: fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .unwrap()
        };
        let delete = position(|insn| matches!(insn, Insn::Delete { .. }));
        let buffered = position(|insn| matches!(insn, Insn::Insert { .. }));
        let returned = position(|insn| matches!(insn, Insn::ResultRow { count: 2, .. }));
        assert!(delete < buffered && buffered < returned);
    }

    #[test]
    fn delete_with_a_where_subquery_collects_rowids_before_deleting() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "DELETE FROM items WHERE value IN (SELECT value FROM items WHERE extra = 'x')",
        );
        let program = emit(plan);
        let position = |matches: fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .unwrap()
        };
        let collected = position(|insn| matches!(insn, Insn::RowSetAdd { .. }));
        let read = position(|insn| matches!(insn, Insn::RowSetRead { .. }));
        let delete = position(|insn| matches!(insn, Insn::Delete { .. }));
        assert!(collected < read && read < delete);
    }

    #[test]
    fn delete_with_a_returning_subquery_returns_after_the_row_is_gone() {
        let program = emit(analyze_plan(
            indexed_items_schema(),
            "DELETE FROM items WHERE value = 1 RETURNING (SELECT count(*) FROM items)",
        ));
        let position = |matches: fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .unwrap()
        };
        assert!(
            position(|insn| matches!(insn, Insn::Delete { .. }))
                < position(|insn| matches!(insn, Insn::BeginSubrtn { .. }))
        );
    }

    #[test]
    fn delete_returning_subquery_decodes_the_copy_of_a_custom_typed_row() {
        let mut schema = indexed_items_schema();
        schema
            .add_type_from_sql(
                "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
            )
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE priced(id INTEGER PRIMARY KEY, amount cents) STRICT",
                    160,
                )
                .unwrap(),
            ))
            .unwrap();
        let program = emit(analyze_plan(
            schema,
            "DELETE FROM priced RETURNING amount, (SELECT count(*) FROM priced)",
        ));
        // The copy of the row is read after the delete and decoded once.
        let position = |matches: fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .unwrap()
        };
        assert!(
            position(|insn| matches!(insn, Insn::Delete { .. }))
                < position(|insn| matches!(insn, Insn::Divide { .. }))
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Divide { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn delete_with_foreign_keys_goes_through_hir() {
        let schema = || {
            let mut schema = Schema::new();
            for (sql, root_page) in [
                ("CREATE TABLE parent(id INTEGER PRIMARY KEY)", 2),
                ("CREATE TABLE child(x INTEGER REFERENCES parent(id))", 3),
                ("CREATE TABLE other(id INTEGER PRIMARY KEY)", 4),
                (
                    "CREATE TABLE cascade(x INTEGER REFERENCES other(id) ON DELETE CASCADE)",
                    5,
                ),
            ] {
                schema
                    .add_btree_table(Arc::new(BTreeTable::from_sql(sql, root_page).unwrap()))
                    .unwrap();
            }
            schema
        };
        for (sql, legacy) in [
            ("DELETE FROM parent", false),
            ("DELETE FROM child", false),
            ("DELETE FROM other", false),
            ("DELETE FROM cascade", false),
        ] {
            let plan = analyze_plan(schema(), sql);
            let (delete, access) = root(&plan);
            assert_eq!(
                needs_legacy(&plan.document, delete, access, &program()).unwrap(),
                legacy,
                "{sql}"
            );
        }
    }
}
