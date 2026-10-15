//! UPDATE lowering from resolved HIR.
//!
//! This covers a plain UPDATE of a B-tree table that changes neither the
//! rowid nor the index the loop reads: one loop over the planned access path,
//! the new row built in registers, NOT NULL and UNIQUE checks, then the
//! changed index entries and the row rewritten. RETURNING reads the new row.
//! Everything else still goes through legacy; see [`needs_legacy`].

use std::num::NonZeroUsize;

use turso_parser::ast::{ResolveType, TriggerTime};

use crate::{
    error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_UNIQUE},
    schema::{Column, EXPR_INDEX_SENTINEL},
    translate::{
        insert::halt_desc_and_on_error,
        optimizer::HirBtreeOperation,
        semantic::hir::{self, HirRoot},
        semantic_to_plan::{HirAccessPlan, HirPlan, HirRootPlan, HirSourceAccess},
        stmt_journal::constraint_may_abort,
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder, SourceBinding},
        insn::{to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral},
        BranchOffset, CursorID,
    },
    CaptureDataChangesExt, LimboError, Result,
};

use super::dml::{
    emit_access_predicates, emit_index_key, has_subquery, operation_index, touches_at_most_one_row,
    DmlTarget, ReturningBuffer,
};
use super::query::{
    close_hir_btree_loop, hir_loop_explain, prepare_hir_btree_loop, start_hir_btree_loop,
    PreparedHirBtreeLoop,
};
use super::trigger::{CompiledTrigger, TriggerRow};

/// Whether this UPDATE uses something the HIR lowering does not handle yet.
/// It is checked before any bytecode is emitted, so legacy can still take
/// the whole statement.
pub(super) fn needs_legacy(
    plan: &HirPlan,
    update: &hir::Update,
    access: &HirAccessPlan,
    program: &ProgramBuilder,
) -> Result<bool> {
    let document = &plan.document;
    if update.from.is_some()
        || !update.order_by.is_empty()
        || update.limit.is_some()
        || update.cdc_updates_override.is_some()
        || matches!(update.conflict, Some(ResolveType::Replace))
    {
        return Ok(true);
    }
    // Subqueries: legacy collects the rows to change first when the WHERE
    // clause has one, and reads SET and RETURNING subqueries at set points.
    if update.predicate.as_ref().is_some_and(has_subquery)
        || update
            .assignments
            .iter()
            .any(|assignment| has_subquery(&assignment.value))
        || update.returning.as_ref().is_some_and(|returning| {
            returning
                .outputs
                .iter()
                .any(|output| has_subquery(&output.expr))
        })
    {
        return Ok(true);
    }
    let foreign_keys = match &update.target_kind {
        hir::UpdateTargetKind::BTree { foreign_keys, .. } => foreign_keys,
        hir::UpdateTargetKind::Virtual => return Ok(true),
    };
    if program.capture_data_changes_info().table().is_some() {
        return Ok(true);
    }
    let Some(source) = document.source(update.target) else {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE target {} is missing",
            update.target
        )));
    };
    let hir::SourceKind::Table(table) = &source.kind else {
        return Ok(true);
    };
    let Some(table) = table.value().btree() else {
        return Ok(true);
    };
    // STRICT type checks, custom types, generated columns, and CHECK
    // constraints are not lowered yet.
    if table.is_strict
        || !table.check_constraints.is_empty()
        || table.columns().iter().any(Column::is_generated)
        || source.column_type_programs.iter().any(Option::is_some)
    {
        return Ok(true);
    }
    // A REPLACE conflict deletes other rows while the loop runs.
    if source
        .index_expressions
        .iter()
        .any(|index| matches!(index.index.value().on_conflict, Some(ResolveType::Replace)))
    {
        return Ok(true);
    }
    let assigned = assigned_columns(update)?;
    // ON UPDATE CASCADE, SET NULL and SET DEFAULT change other rows.
    if super::fkey::update_needs_legacy(foreign_keys, &assigned_positions(&assigned)) {
        return Ok(true);
    }
    // NOT NULL ON CONFLICT REPLACE stores the column's default instead.
    if assigned.iter().any(|column| {
        matches!(column, hir::TargetColumn::Column(column)
        if table.columns().get(*column).is_some_and(|column| {
            matches!(column.notnull_conflict_clause, Some(ResolveType::Replace))
        }))
    }) {
        return Ok(true);
    }
    // Changing the rowid moves the row; legacy collects the rows first.
    if assigned.iter().any(|column| match column {
        hir::TargetColumn::RowId => true,
        hir::TargetColumn::Column(column) => table
            .columns()
            .get(*column)
            .is_some_and(Column::is_rowid_alias),
    }) {
        return Ok(true);
    }
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE plan has {} loops for one target",
            access.loops.len()
        )));
    };
    let HirSourceAccess::BTree { operation, .. } = &planned_loop.access else {
        return Ok(true);
    };
    let Some(iteration_index) = operation_index(operation) else {
        return Ok(false);
    };
    if iteration_index.ephemeral {
        return Ok(true);
    }
    // Changing the index the loop reads could visit a row again; legacy
    // collects the rows first.
    let Some(iteration) = source
        .index_expressions
        .iter()
        .find(|index| index.index.value().name == iteration_index.name)
    else {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE target does not list index {}",
            iteration_index.name
        )));
    };
    Ok(index_reads_assigned_column(
        update.target,
        iteration,
        &assigned,
    ))
}

/// Emit an UPDATE that [`needs_legacy`] accepted. `update` is the plan's
/// root or one command of a trigger in it.
pub(super) fn emit_update(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    update: &hir::Update,
    access: &HirAccessPlan,
    triggers: &[CompiledTrigger],
    mvcc: bool,
) -> Result<()> {
    let document = &plan.document;
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE plan has {} loops for one target",
            access.loops.len()
        )));
    };
    if planned_loop.source != update.target {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE loop reads source {} instead of its target {}",
            planned_loop.source, update.target
        )));
    }
    let HirSourceAccess::BTree { operation, .. } = &planned_loop.access else {
        return Err(LimboError::InternalError(
            "HIR UPDATE target does not use a B-tree loop".into(),
        ));
    };
    let hir::UpdateTargetKind::BTree { foreign_keys, .. } = &update.target_kind else {
        return Err(LimboError::InternalError(
            "HIR UPDATE target is not a B-tree table".into(),
        ));
    };
    let target = DmlTarget::new(program, document, update.target, "UPDATE")?;
    let table = target.table;
    let new_source = document.source(update.new_source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR UPDATE new row {} is missing",
            update.new_source
        ))
    })?;
    let or_conflict = update.conflict.unwrap_or(ResolveType::Abort);
    let has_statement_conflict = update.conflict.is_some();
    program.set_resolve_type(or_conflict);
    program
        .flags
        .set_has_statement_conflict(has_statement_conflict);

    let assigned = assigned_columns(update)?;
    let mut values = vec![None; table.columns().len()];
    for assignment in &update.assignments {
        let [hir::TargetColumn::Column(column)] = assignment.columns.as_slice() else {
            return Err(LimboError::InternalError(
                "HIR UPDATE assignment does not set exactly one column".into(),
            ));
        };
        values[*column] = Some(&assignment.value);
    }
    // Indexes whose key or WHERE term reads a changed column, in schema
    // order, with the NEW-row expressions for the same index.
    let mut changed_indexes = Vec::new();
    for old in &target.source.index_expressions {
        if !index_reads_assigned_column(update.target, old, &assigned) {
            continue;
        }
        let new = new_source
            .index_expressions
            .iter()
            .find(|new| new.index.value().name == old.index.value().name)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR UPDATE new row does not list index {}",
                    old.index.value().name
                ))
            })?;
        changed_indexes.push((old, new));
    }

    // Same statement-journal facts as legacy.
    let has_foreign_keys = !foreign_keys.incoming.is_empty() || !foreign_keys.outgoing.is_empty();
    if triggers.is_empty() && !has_foreign_keys && touches_at_most_one_row(operation) {
        program.set_multi_write(false);
    }
    let has_notnull = values
        .iter()
        .zip(table.columns())
        .any(|(value, column)| value.is_some() && column.notnull() && !column.is_rowid_alias());
    let has_unique = !table.unique_sets.is_empty()
        || changed_indexes
            .iter()
            .any(|(old, _)| old.index.value().unique);
    program.set_may_abort(
        !triggers.is_empty()
            || has_foreign_keys
            || constraint_may_abort(
                has_statement_conflict,
                or_conflict,
                table.rowid_alias_conflict_clause,
                changed_indexes.iter().map(|(old, _)| {
                    let index = old.index.value();
                    (index.on_conflict, index.unique)
                }),
                has_notnull,
                false,
                has_unique,
            ),
    );

    program.emit_source_explain(
        target.source.id,
        hir_loop_explain(document, None, planned_loop)?,
    );
    let returning = ReturningBuffer::open(program, table, update.returning.as_ref());
    let row = UpdateRowSettings {
        or_conflict,
        has_statement_conflict,
    };

    let assigned = assigned_positions(&assigned);
    let row_layout = UpdateRowLayout {
        values: &values,
        assigned: &assigned,
        changed_indexes: &changed_indexes,
        new_source: new_source.id,
        foreign_keys,
        returning,
        triggers,
        mvcc,
    };
    let done = program.allocate_label();
    if triggers.is_empty() {
        emit_scan_update(program, &target, access, operation, &row_layout, row, done)?;
    } else {
        emit_rowset_update(program, plan, &target, access, &row_layout, row, done)?;
    }
    program.preassign_label_to_next_insn(done);
    if let Some(returning) = returning {
        returning.emit_rows(program);
    }
    Ok(())
}

/// What every row of an UPDATE computes, shared by both loops.
struct UpdateRowLayout<'a> {
    /// Each column's assigned value, if any.
    values: &'a [Option<&'a hir::Expr>],
    /// The positions of the columns the UPDATE sets.
    assigned: &'a [usize],
    /// The indexes the UPDATE changes, with their OLD- and NEW-row expressions.
    changed_indexes: &'a [(&'a hir::IndexExpressions, &'a hir::IndexExpressions)],
    new_source: hir::SourceId,
    foreign_keys: &'a hir::DmlForeignKeys,
    returning: Option<ReturningBuffer<'a>>,
    triggers: &'a [CompiledTrigger],
    mvcc: bool,
}

/// Registers and conflict settings of an UPDATE's row, before the cursor
/// that reads it is known.
#[derive(Clone, Copy)]
struct UpdateRowSettings {
    or_conflict: ResolveType,
    has_statement_conflict: bool,
}

/// Update each row as the loop reaches it.
fn emit_scan_update(
    program: &mut ProgramBuilder,
    target: &DmlTarget<'_>,
    access: &HirAccessPlan,
    operation: &HirBtreeOperation,
    layout: &UpdateRowLayout<'_>,
    settings: UpdateRowSettings,
    done: BranchOffset,
) -> Result<()> {
    let document = target.document;
    // Like legacy, open the table, the index the loop reads, then each index
    // the UPDATE changes, all for writing.
    let iteration_index = operation_index(operation);
    let table_cursor = target.open_table_for_write(program);
    let iteration_cursor = iteration_index.map(|index| {
        let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: RegisterOrLiteral::Literal(index.root_page),
            db: target.database,
        });
        cursor
    });
    let index_cursors = open_changed_indexes(program, target, layout);

    let scan_cursor = iteration_cursor.unwrap_or(table_cursor);
    let lookup_cursor = iteration_cursor.map(|_| table_cursor);
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
    };
    emit_access_predicates(program, document, access, None, done)?;
    let hir_loop = start_hir_btree_loop(program, document, &prepared, None)?;
    emit_access_predicates(program, document, access, Some(0), hir_loop.next)?;

    let row = UpdateRow {
        target,
        table_cursor,
        rowid: program.alloc_registers(target.table.columns().len() + 1),
        or_conflict: settings.or_conflict,
        has_statement_conflict: settings.has_statement_conflict,
        skip_row: hir_loop.next,
    };
    // RowId on the table cursor also finishes a deferred seek from the
    // index, so the table cursor sits on the row before it is rewritten.
    program.emit_insn(Insn::RowId {
        cursor_id: table_cursor,
        dest: row.rowid,
    });
    program.emit_insn(Insn::IsNull {
        reg: row.rowid,
        target_pc: done,
    });
    row.emit(program, layout, &index_cursors)?;
    close_hir_btree_loop(program, &hir_loop, &[]);
    Ok(())
}

/// Collect the rowids of the rows to update with read cursors, then update
/// each collected row. Like legacy, an UPDATE with triggers does this, since
/// a trigger can change the table.
fn emit_rowset_update(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    target: &DmlTarget<'_>,
    access: &HirAccessPlan,
    layout: &UpdateRowLayout<'_>,
    settings: UpdateRowSettings,
    done: BranchOffset,
) -> Result<()> {
    let document = target.document;
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE plan has {} loops for one target",
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
    let index_cursors = open_changed_indexes(program, target, layout);
    let next = program.allocate_label();
    let row = UpdateRow {
        target,
        table_cursor,
        rowid: program.alloc_registers(target.table.columns().len() + 1),
        or_conflict: settings.or_conflict,
        has_statement_conflict: settings.has_statement_conflict,
        skip_row: next,
    };
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::RowSetRead {
        rowset_reg: rowset,
        pc_if_empty: done,
        dest_reg: row.rowid,
    });
    program.emit_insn(Insn::NotExists {
        cursor: table_cursor,
        rowid_reg: row.rowid,
        target_pc: next,
    });
    row.emit(program, layout, &index_cursors)?;
    program.emit_insn(Insn::Goto { target_pc: next });
    Ok(())
}

fn open_changed_indexes(
    program: &mut ProgramBuilder,
    target: &DmlTarget<'_>,
    layout: &UpdateRowLayout<'_>,
) -> Vec<CursorID> {
    layout
        .changed_indexes
        .iter()
        .map(|(old, _)| {
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(old.index.handle()));
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(old.index.value().root_page),
                db: target.database,
            });
            cursor
        })
        .collect()
}

pub(super) fn update_root(plan: &HirPlan) -> Result<(&hir::Update, &HirAccessPlan)> {
    let HirRoot::Update(update) = &plan.document.root else {
        return Err(LimboError::InternalError(
            "HIR UPDATE analysis did not produce an UPDATE root".into(),
        ));
    };
    let HirRootPlan::Update(access) = &plan.root else {
        return Err(LimboError::InternalError(
            "HIR UPDATE plan has no UPDATE access plan".into(),
        ));
    };
    Ok((update, access))
}

/// The table positions of `assigned`. The rowid has none; an UPDATE that
/// sets it goes to legacy.
fn assigned_positions(assigned: &[hir::TargetColumn]) -> Vec<usize> {
    assigned
        .iter()
        .filter_map(|column| match column {
            hir::TargetColumn::Column(column) => Some(*column),
            hir::TargetColumn::RowId => None,
        })
        .collect()
}

fn assigned_columns(update: &hir::Update) -> Result<Vec<hir::TargetColumn>> {
    Ok(update
        .assignments
        .iter()
        .flat_map(|assignment| assignment.columns.iter().copied())
        .collect())
}

/// Whether an index's key or WHERE term reads a column the UPDATE assigns.
fn index_reads_assigned_column(
    target: hir::SourceId,
    index: &hir::IndexExpressions,
    assigned: &[hir::TargetColumn],
) -> bool {
    let is_assigned = |column: usize| assigned.contains(&hir::TargetColumn::Column(column));
    let reads_assigned = |expression: &hir::Expr| {
        let mut reads = false;
        expression.for_each(&mut |expression| {
            if let hir::Expr::Column(column) = expression {
                reads |= column.source == target && is_assigned(column.column);
            }
        });
        reads
    };
    let key_reads =
        index
            .index
            .value()
            .columns
            .iter()
            .zip(&index.columns)
            .any(|(column, expression)| match expression {
                Some(expression) => reads_assigned(expression),
                None => is_assigned(column.pos_in_table),
            });
    key_reads || index.predicate.as_ref().is_some_and(reads_assigned)
}

/// The registers of the row an UPDATE rewrites: the rowid, then one
/// register per column for the new row.
struct UpdateRow<'a, 'b> {
    target: &'a DmlTarget<'b>,
    table_cursor: CursorID,
    rowid: usize,
    or_conflict: ResolveType,
    has_statement_conflict: bool,
    /// Where `OR IGNORE` goes to leave the row unchanged.
    skip_row: BranchOffset,
}

/// A new index key built before any index changes.
struct NewIndexKey {
    start: usize,
    count: usize,
    record: usize,
    /// Partial index: whether the old row is in the index.
    old_in_index: Option<usize>,
    /// Partial index: whether the new row is in the index.
    new_in_index: Option<usize>,
}

impl UpdateRow<'_, '_> {
    fn start(&self) -> usize {
        self.rowid + 1
    }

    /// Rewrite the row the table cursor is on.
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        layout: &UpdateRowLayout<'_>,
        index_cursors: &[CursorID],
    ) -> Result<()> {
        let document = self.target.document;
        let table = self.target.table;
        let source = self.target.source;
        let columns = table.columns().len();
        let fires = |time: TriggerTime| {
            layout
                .triggers
                .iter()
                .filter(move |trigger| trigger.time == time)
        };
        let has_before = fires(TriggerTime::Before).next().is_some();
        // The old row is read from the table, as legacy does.
        program.bind_source(
            source.id,
            SourceBinding::BTree {
                scan_cursor: self.table_cursor,
                table_cursor: None,
            },
        );
        // Like legacy, a BEFORE trigger runs before NOT NULL is checked.
        self.emit_new_values(program, layout.values, !has_before)?;
        let new_row = TriggerRow {
            columns: self.start(),
            rowid: self.rowid,
        };
        // The OLD row the triggers read, as a query would read it.
        let old_row = (!layout.triggers.is_empty())
            .then(|| -> Result<TriggerRow> {
                let start = program.alloc_registers(columns);
                for column in 0..columns {
                    super::expr::translate_expr_no_constant_opt(
                        program,
                        document,
                        &hir::Expr::Column(hir::ColumnRef {
                            source: source.id,
                            column,
                        }),
                        start + column,
                    )?;
                }
                Ok(TriggerRow {
                    columns: start,
                    rowid: self.rowid,
                })
            })
            .transpose()?;
        if has_before {
            for trigger in fires(TriggerTime::Before) {
                // RAISE(IGNORE) skips the rest of this row.
                trigger.emit_fire(program, old_row, Some(new_row), columns, self.skip_row)?;
            }
            // Like legacy and SQLite: the trigger may have deleted the row,
            // and the columns the UPDATE does not set are read again, since
            // the trigger may have changed them.
            program.emit_insn(Insn::NotExists {
                cursor: self.table_cursor,
                rowid_reg: self.rowid,
                target_pc: self.skip_row,
            });
            for (position, (column, value)) in table.columns().iter().zip(layout.values).enumerate()
            {
                if value.is_none() && !column.is_rowid_alias() {
                    program.emit_column_or_rowid(
                        self.table_cursor,
                        position,
                        self.start() + position,
                    );
                }
            }
            for (position, (column, value)) in table.columns().iter().zip(layout.values).enumerate()
            {
                if value.is_some() && column.notnull() {
                    self.emit_notnull_check(program, column, self.start() + position);
                }
            }
        }
        program.bind_source(
            layout.new_source,
            SourceBinding::Registers {
                start: self.start(),
                rowid: Some(self.rowid),
            },
        );

        // Like legacy, check every changed index before changing any of them:
        // build the new keys and check UNIQUE, then delete the old entries,
        // then insert the new ones.
        let mut new_keys = Vec::with_capacity(layout.changed_indexes.len());
        for ((old, new), cursor) in layout.changed_indexes.iter().zip(index_cursors) {
            new_keys.push(self.emit_new_index_key(program, old, new, *cursor)?);
        }
        // Like legacy, foreign keys are checked after the other constraints
        // and before anything is rewritten.
        super::fkey::emit_update_checks(
            program,
            document,
            layout.foreign_keys,
            source.id,
            layout.new_source,
            self.rowid,
            layout.assigned,
            self.target.database,
        )?;
        for ((old, _), (cursor, key)) in layout
            .changed_indexes
            .iter()
            .zip(index_cursors.iter().zip(&new_keys))
        {
            let skip = key.old_in_index.map(|old_in_index| {
                let skip = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: old_in_index,
                    target_pc: skip,
                    jump_if_null: true,
                });
                skip
            });
            let (start, count) =
                emit_index_key(program, document, source, table, self.table_cursor, old)?;
            program.emit_insn(Insn::IdxDelete {
                start_reg: start,
                num_regs: count,
                cursor_id: *cursor,
                raise_error_if_no_matching_entry: true,
            });
            if let Some(skip) = skip {
                program.preassign_label_to_next_insn(skip);
            }
        }
        for (cursor, key) in index_cursors.iter().zip(&new_keys) {
            let skip = key.new_in_index.map(|new_in_index| {
                let skip = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: new_in_index,
                    target_pc: skip,
                    jump_if_null: true,
                });
                skip
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: *cursor,
                record_reg: key.record,
                unpacked_start: Some(key.start),
                unpacked_count: Some(to_u32(key.count)),
                flags: IdxInsertFlags::new().nchange(true),
            });
            if let Some(skip) = skip {
                program.preassign_label_to_next_insn(skip);
            }
        }

        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(self.start()),
            count: to_u32(columns),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: Some(
                table
                    .columns()
                    .iter()
                    .map(|column| column.affinity_with_strict(table.is_strict).aff_mask())
                    .collect(),
            ),
        });
        // MVCC keeps every version of a row, so the old one is deleted and
        // the new one inserted.
        if layout.mvcc {
            program.emit_insn(Insn::Delete {
                cursor_id: self.table_cursor,
                table_name: table.name.clone(),
                is_part_of_update: true,
            });
        }
        program.emit_insn(Insn::Insert {
            cursor: self.table_cursor,
            key_reg: self.rowid,
            record_reg: record,
            flag: InsertFlags::new().skip_last_rowid(),
            table_name: table.name.clone(),
        });
        if let Some(returning) = layout.returning {
            let start = returning.emit_values(program, document)?;
            returning.emit_buffer(program, start);
        }
        // Like legacy, AFTER triggers fire after RETURNING has read the row,
        // and RAISE(IGNORE) in one only ends that trigger's body.
        let after_triggers = program.allocate_label();
        for trigger in fires(TriggerTime::After) {
            trigger.emit_fire(program, old_row, Some(new_row), columns, after_triggers)?;
        }
        program.preassign_label_to_next_insn(after_triggers);
        Ok(())
    }

    /// Fill the new-row registers: assigned columns get their value, the
    /// others are read from the old row, and an INTEGER PRIMARY KEY column
    /// is NULL, as it is in the stored record.
    fn emit_new_values(
        &self,
        program: &mut ProgramBuilder,
        values: &[Option<&hir::Expr>],
        check_notnull: bool,
    ) -> Result<()> {
        let table = self.target.table;
        for (position, (column, value)) in table.columns().iter().zip(values).enumerate() {
            let register = self.start() + position;
            match value {
                Some(value) => {
                    super::expr::translate_expr(program, self.target.document, value, register)?;
                    if check_notnull && column.notnull() {
                        self.emit_notnull_check(program, column, register);
                    }
                }
                None if column.is_rowid_alias() => program.emit_null(register, None),
                None => program.emit_column_or_rowid(self.table_cursor, position, register),
            }
        }
        // Like legacy, apply column affinity to the new row before any index
        // or constraint reads it.
        let affinities = table
            .columns()
            .iter()
            .map(|column| column.affinity())
            .collect::<Vec<_>>();
        if affinities
            .iter()
            .any(|affinity| *affinity != Affinity::Blob)
        {
            if let Some(count) = NonZeroUsize::new(affinities.len()) {
                program.emit_insn(Insn::Affinity {
                    start_reg: self.start(),
                    count,
                    affinities: affinities
                        .iter()
                        .map(|affinity| affinity.aff_mask())
                        .collect(),
                });
            }
        }
        Ok(())
    }

    fn emit_notnull_check(&self, program: &mut ProgramBuilder, column: &Column, register: usize) {
        let conflict = if self.has_statement_conflict {
            self.or_conflict
        } else {
            column.notnull_conflict_clause.unwrap_or(ResolveType::Abort)
        };
        match conflict {
            ResolveType::Ignore => program.emit_insn(Insn::IsNull {
                reg: register,
                target_pc: self.skip_row,
            }),
            _ => program.emit_insn(Insn::HaltIfNull {
                target_reg: register,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: format!(
                    "{}.{}",
                    self.target.table.name,
                    column.name.as_deref().unwrap_or_default()
                ),
            }),
        }
    }

    /// Build the new key of a changed index and, for a UNIQUE index, check
    /// that no other row has it.
    fn emit_new_index_key(
        &self,
        program: &mut ProgramBuilder,
        old: &hir::IndexExpressions,
        new: &hir::IndexExpressions,
        cursor: CursorID,
    ) -> Result<NewIndexKey> {
        let document = self.target.document;
        let index = new.index.value();
        let table = self.target.table;
        let (old_in_index, new_in_index) = match (&old.predicate, &new.predicate) {
            (Some(old_predicate), Some(new_predicate)) => {
                let old_in_index = program.alloc_register();
                super::expr::translate_expr_no_constant_opt(
                    program,
                    document,
                    old_predicate,
                    old_in_index,
                )?;
                let new_in_index = program.alloc_register();
                super::expr::translate_expr_no_constant_opt(
                    program,
                    document,
                    new_predicate,
                    new_in_index,
                )?;
                (Some(old_in_index), Some(new_in_index))
            }
            (None, None) => (None, None),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR index {} has a WHERE term for only one row",
                    index.name
                )));
            }
        };

        let columns = index.columns.len();
        let start = program.alloc_registers(columns + 1);
        for (offset, (column, expression)) in index.columns.iter().zip(&new.columns).enumerate() {
            let register = start + offset;
            match expression {
                Some(expression) => super::expr::translate_expr_no_constant_opt(
                    program, document, expression, register,
                )?,
                None => {
                    let source = if table
                        .columns()
                        .get(column.pos_in_table)
                        .is_some_and(Column::is_rowid_alias)
                    {
                        self.rowid
                    } else {
                        self.start() + column.pos_in_table
                    };
                    program.emit_insn(Insn::Copy {
                        src_reg: source,
                        dst_reg: register,
                        extra_amount: 0,
                    });
                    register
                }
            };
        }
        program.emit_insn(Insn::Copy {
            src_reg: self.rowid,
            dst_reg: start + columns,
            extra_amount: 0,
        });
        if let Some(count) = NonZeroUsize::new(columns) {
            program.emit_insn(Insn::Affinity {
                start_reg: start,
                count,
                affinities: index
                    .columns
                    .iter()
                    .map(|column| {
                        if column.pos_in_table == EXPR_INDEX_SENTINEL {
                            Affinity::Blob.aff_mask()
                        } else {
                            table.columns()[column.pos_in_table]
                                .affinity_with_strict(table.is_strict)
                                .aff_mask()
                        }
                    })
                    .collect(),
            });
        }
        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(start),
            count: to_u32(columns + 1),
            dest_reg: to_u32(record),
            index_name: Some(index.name.clone()),
            affinity_str: None,
        });

        if index.unique {
            self.emit_unique_check(program, index, cursor, start, columns, new_in_index);
        }
        Ok(NewIndexKey {
            start,
            count: columns + 1,
            record,
            old_in_index,
            new_in_index,
        })
    }

    fn emit_unique_check(
        &self,
        program: &mut ProgramBuilder,
        index: &crate::schema::Index,
        cursor: CursorID,
        start: usize,
        columns: usize,
        new_in_index: Option<usize>,
    ) {
        let conflict = if self.has_statement_conflict {
            self.or_conflict
        } else {
            index.on_conflict.unwrap_or(ResolveType::Abort)
        };
        let unique = program.allocate_label();
        // A row outside a partial index cannot conflict in it.
        if let Some(new_in_index) = new_in_index {
            program.emit_insn(Insn::IfNot {
                reg: new_in_index,
                target_pc: unique,
                jump_if_null: true,
            });
        }
        program.emit_insn(Insn::NoConflict {
            cursor_id: cursor,
            target_pc: unique,
            record_reg: start,
            num_regs: columns,
        });
        // The entry found may be this row's own.
        let found = program.alloc_register();
        program.emit_insn(Insn::IdxRowId {
            cursor_id: cursor,
            dest: found,
        });
        program.emit_insn(Insn::Eq {
            lhs: self.rowid,
            rhs: found,
            target_pc: unique,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        match conflict {
            ResolveType::Ignore => program.emit_insn(Insn::Goto {
                target_pc: self.skip_row,
            }),
            _ => {
                let table_name = &self.target.table.name;
                let columns = index
                    .columns
                    .iter()
                    .map(|column| format!("{table_name}.{}", column.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                let (description, on_error) =
                    halt_desc_and_on_error(&columns, conflict, self.has_statement_conflict);
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_UNIQUE,
                    description,
                    on_error,
                    description_reg: None,
                });
            }
        }
        program.preassign_label_to_next_insn(unique);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{BTreeTable, Index, IndexColumn, Schema},
        sync::Arc,
        translate::semantic_lowering::query::tests::{analyze_plan, program},
    };
    use turso_parser::ast::SortOrder;

    /// `items(id INTEGER PRIMARY KEY, value, extra)` with a UNIQUE index on
    /// `value`.
    fn schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER, extra TEXT)",
                    2,
                )
                .unwrap(),
            ))
            .unwrap();
        schema
            .add_index(Arc::new(Index {
                name: "items_value".to_string(),
                table_name: "items".to_string(),
                root_page: 3,
                columns: vec![IndexColumn {
                    name: "value".to_string(),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: 1,
                    collation: None,
                    default: None,
                    expr: None,
                }],
                unique: true,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }))
            .unwrap();
        schema
    }

    fn emit(sql: &str) -> ProgramBuilder {
        let plan = Arc::new(analyze_plan(schema(), sql));
        let mut program = program();
        let (update, access) = update_root(&plan).unwrap();
        assert!(
            !needs_legacy(&plan, update, access, &program).unwrap(),
            "{sql}"
        );
        program.set_hir_plan(plan.clone());
        emit_update(&mut program, &plan, update, access, &[], false).unwrap();
        program
    }

    fn count(program: &ProgramBuilder, matches: fn(&Insn) -> bool) -> usize {
        program
            .insns
            .iter()
            .filter(|(insn, _)| matches(insn))
            .count()
    }

    #[test]
    fn update_of_an_indexed_column_checks_and_rewrites_its_index_entry() {
        let program = emit("UPDATE items SET value = value + 1 WHERE extra = 'x'");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::NoConflict { .. })),
            1
        );
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::IdxDelete { .. })),
            1
        );
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::IdxInsert { .. })),
            1
        );
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::Insert { .. })),
            1
        );
    }

    #[test]
    fn update_of_other_columns_leaves_the_index_alone() {
        let program = emit("UPDATE items SET extra = 'y' WHERE id = 1");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::IdxDelete { .. })),
            0
        );
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::IdxInsert { .. })),
            0
        );
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::Insert { .. })),
            1
        );
    }

    #[test]
    fn update_that_needs_legacy_is_left_to_it() {
        for sql in [
            "UPDATE items SET id = id + 1",
            "UPDATE items SET rowid = 5 WHERE id = 1",
            "UPDATE OR REPLACE items SET value = 1",
            "UPDATE items SET extra = (SELECT max(extra) FROM items)",
            "UPDATE items SET extra = 'y' WHERE value IN (SELECT value FROM items)",
            "UPDATE items SET extra = 'y' RETURNING (SELECT 1)",
            // The loop reads the index whose key changes.
            "UPDATE items INDEXED BY items_value SET value = value + 1 WHERE value > 1",
        ] {
            let plan = analyze_plan(schema(), sql);
            let (update, access) = update_root(&plan).unwrap();
            assert!(
                needs_legacy(&plan, update, access, &program()).unwrap(),
                "{sql}"
            );
        }
    }
}
