//! INSERT lowering from resolved HIR.
//!
//! This covers a plain INSERT of VALUES rows or DEFAULT VALUES into a rowid
//! B-tree table: the row is built in registers, its rowid taken from the
//! statement or generated, NOT NULL, PRIMARY KEY and UNIQUE checked, then
//! every index entry and the row are written. Several VALUES rows run as a
//! coroutine, as in legacy. RETURNING reads the new row. Everything else
//! still goes through legacy; see [`needs_legacy`].

use std::num::NonZeroUsize;

use turso_parser::ast::{self, ResolveType, TriggerTime};

use super::emit::{
    any_effective_replace, constraint_may_abort, format_unique_violation_desc,
    halt_desc_and_on_error,
};
use crate::{
    error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE},
    schema::{Column, EXPR_INDEX_SENTINEL},
    translate::{
        plan::QueryDestination,
        semantic::hir::{self, HirRoot},
        semantic_to_plan::HirPlan,
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder, SourceBinding},
        insn::{to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn},
        BranchOffset, CursorID,
    },
    CaptureDataChangesExt, LimboError, Result,
};

use super::dml::{DmlTarget, ReturningBuffer};
use super::trigger::{CompiledTrigger, TriggerRow};

/// Whether this INSERT uses something the HIR lowering does not handle yet.
/// It is checked before any bytecode is emitted, so legacy can still take
/// the whole statement.
pub(super) fn needs_legacy(
    plan: &HirPlan,
    insert: &hir::Insert,
    program: &ProgramBuilder,
) -> Result<bool> {
    let document = &plan.document;
    // Like legacy, OR IGNORE drops the UPSERT clauses.
    if !insert.upserts.is_empty() && insert.conflict == Some(ResolveType::Ignore) {
        return Ok(true);
    }
    let (defaults, foreign_keys) = match &insert.target_kind {
        hir::InsertTargetKind::BTree {
            autoincrement,
            defaults,
            triggers,
            foreign_keys,
            ..
        } => {
            if autoincrement.is_some() || !triggers.upsert_update.is_empty() {
                return Ok(true);
            }
            (defaults, foreign_keys)
        }
        hir::InsertTargetKind::Virtual => return Ok(true),
    };
    if program.capture_data_changes_info().table().is_some() {
        return Ok(true);
    }
    let Some(source) = document.source(insert.target) else {
        return Err(LimboError::InternalError(format!(
            "HIR INSERT target {} is missing",
            insert.target
        )));
    };
    let hir::SourceKind::Table(table) = &source.kind else {
        return Ok(true);
    };
    let Some(table) = table.value().btree() else {
        return Ok(true);
    };
    // WITHOUT ROWID tables, STRICT type checks, custom types and generated
    // columns are not lowered yet.
    if !table.has_rowid
        || table.is_strict
        || table.columns().iter().any(Column::is_generated)
        || table.columns().iter().any(Column::hidden)
        || source.column_type_programs.iter().any(Option::is_some)
    {
        return Ok(true);
    }
    for upsert in &insert.upserts {
        let hir::UpsertAction::Update { assignments, .. } = &upsert.action else {
            continue;
        };
        // A DO UPDATE that moves the row, or changes a parent key with an ON
        // UPDATE action, still needs legacy.
        let mut assigned = Vec::new();
        for assignment in assignments {
            for column in &assignment.columns {
                match column {
                    hir::TargetColumn::Column(column)
                        if !table.columns()[*column].is_rowid_alias() =>
                    {
                        assigned.push(*column)
                    }
                    _ => return Ok(true),
                }
            }
        }
        if super::fkey::update_needs_legacy(foreign_keys, &assigned) {
            return Ok(true);
        }
    }
    // REPLACE deletes the conflicting row; an ON DELETE action on it still
    // needs legacy.
    let may_replace = match insert.conflict {
        Some(conflict) => conflict == ResolveType::Replace,
        None => {
            table.rowid_alias_conflict_clause == Some(ResolveType::Replace)
                || source
                    .index_expressions
                    .iter()
                    .any(|index| index.index.value().on_conflict == Some(ResolveType::Replace))
        }
    };
    if may_replace && super::fkey::delete_needs_legacy(foreign_keys) {
        return Ok(true);
    }
    // A DEFAULT on an INTEGER PRIMARY KEY makes the rowid come from it.
    Ok(defaults.iter().any(|default| {
        table
            .columns()
            .get(default.column)
            .is_some_and(Column::is_rowid_alias)
            && !matches!(default.value, hir::Expr::Literal(ast::Literal::Null))
    }))
}

/// Emit an INSERT that [`needs_legacy`] accepted. `insert` is the plan's
/// root or one command of a trigger in it. `enforce_checks` is false under
/// `PRAGMA ignore_check_constraints`.
pub(super) fn emit_insert(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    insert: &hir::Insert,
    triggers: &[CompiledTrigger],
    enforce_checks: bool,
) -> Result<()> {
    let document = &plan.document;
    let hir::InsertTargetKind::BTree {
        defaults,
        replace_defaults,
        foreign_keys,
        ..
    } = &insert.target_kind
    else {
        return Err(LimboError::InternalError(
            "HIR INSERT target is not a B-tree table".into(),
        ));
    };
    let (rows, query): (&[Vec<hir::Expr>], _) = match &insert.source {
        hir::InsertSource::DefaultValues => (&[Vec::new()], None),
        hir::InsertSource::Values(rows) => (rows, None),
        hir::InsertSource::Query(query) => (&[], Some(*query)),
    };
    let target = DmlTarget::new(program, document, insert.target, "INSERT")?;
    let table = target.table;
    let or_conflict = insert.conflict.unwrap_or(ResolveType::Abort);
    let has_statement_conflict = insert.conflict.is_some();
    program.set_resolve_type(or_conflict);
    program
        .flags
        .set_has_statement_conflict(has_statement_conflict);

    // Where each table column's value comes from: a VALUES position, a
    // DEFAULT, or nothing (NULL). An INTEGER PRIMARY KEY column's value is
    // the rowid.
    let mut column_values = vec![ColumnValue::Null; table.columns().len()];
    let mut rowid_value = None;
    for (position, target_column) in insert.columns.iter().enumerate() {
        if !target_column.uses_value {
            continue;
        }
        match target_column.column {
            hir::TargetColumn::RowId => rowid_value = Some(position),
            hir::TargetColumn::Column(column) if table.columns()[column].is_rowid_alias() => {
                rowid_value = Some(position)
            }
            hir::TargetColumn::Column(column) => column_values[column] = ColumnValue::Row(position),
        }
    }
    for default in defaults {
        if !table.columns()[default.column].is_rowid_alias() {
            column_values[default.column] = ColumnValue::Default(&default.value);
        }
    }

    // Same statement-journal facts as legacy.
    let several_rows = rows.len() > 1 || query.is_some();
    let indexes = &target.source.index_expressions;
    let any_replace = any_effective_replace(
        has_statement_conflict,
        or_conflict,
        table.rowid_alias_conflict_clause,
        indexes.iter().map(|index| index.index.value().on_conflict),
    );
    // An UPSERT may rewrite an existing row, and its DO UPDATE always
    // checks with ABORT.
    let has_upsert = !insert.upserts.is_empty();
    let has_do_update = insert
        .upserts
        .iter()
        .any(|upsert| matches!(upsert.action, hir::UpsertAction::Update { .. }));
    if !several_rows && triggers.is_empty() && !any_replace && !has_upsert {
        program.set_multi_write(false);
    }
    let has_foreign_keys = !foreign_keys.incoming.is_empty() || !foreign_keys.outgoing.is_empty();
    program.set_may_abort(
        !triggers.is_empty()
            || has_foreign_keys
            || has_do_update
            || constraint_may_abort(
                has_statement_conflict,
                or_conflict,
                table.rowid_alias_conflict_clause,
                indexes.iter().map(|index| {
                    let index = index.index.value();
                    (index.on_conflict, index.unique)
                }),
                table
                    .columns()
                    .iter()
                    .any(|column| column.notnull() && !column.is_rowid_alias()),
                !table.check_constraints.is_empty(),
                rowid_value.is_some() || !indexes.is_empty(),
            ),
    );

    let returning = ReturningBuffer::open(program, table, insert.returning.as_ref());
    let table_cursor = target.open_table_for_write(program);
    let index_cursors = target.open_indexes_for_write(program, None)?;
    let row = InsertRow {
        target: &target,
        table_cursor,
        rowid: program.alloc_register(),
        columns: program.alloc_registers(table.columns().len()),
        or_conflict,
        has_statement_conflict,
        triggers,
        foreign_keys,
        replace_defaults,
        upserts: &insert.upserts,
        excluded: insert.excluded_source,
        upsert_new: insert.upsert_new_source,
        conflict_rowid: program.alloc_register(),
        enforce_checks,
    };
    let done = program.allocate_label();
    match rows {
        _ if query.is_some() => {
            let query = query.expect("query was checked");
            emit_select_rows(
                program,
                plan,
                insert,
                query,
                &row,
                RowSink {
                    column_values: &column_values,
                    rowid_value,
                    index_cursors: &index_cursors,
                    returning,
                },
                done,
            )?;
        }
        [values] => {
            let row_done = program.allocate_label();
            row.emit(
                program,
                &column_values,
                rowid_value,
                RowValues::Expressions(values),
                &index_cursors,
                returning,
                row_done,
            )?;
            program.preassign_label_to_next_insn(row_done);
        }
        _ => {
            // Like legacy, the VALUES rows run as a coroutine that yields
            // one row at a time into the same registers.
            let width = insert.columns.len();
            let yield_reg = program.alloc_register();
            let values = program.alloc_registers(width.max(1));
            let start = program.allocate_label();
            let defined = program.allocate_label();
            program.emit_insn(Insn::InitCoroutine {
                yield_reg,
                jump_on_definition: defined,
                start_offset: start,
            });
            program.preassign_label_to_next_insn(start);
            for value_row in rows {
                for (position, value) in value_row.iter().enumerate() {
                    super::expr::translate_expr_no_constant_opt(
                        program,
                        document,
                        value,
                        values + position,
                    )?;
                }
                program.emit_insn(Insn::Yield {
                    yield_reg,
                    end_offset: BranchOffset::Offset(0),
                    subtype_clear_start_reg: values,
                    subtype_clear_count: width,
                });
            }
            program.emit_insn(Insn::EndCoroutine { yield_reg });
            program.preassign_label_to_next_insn(defined);
            // Like legacy and SQLite, the rows are computed before any is
            // inserted when they read the target table or the INSERT fires
            // triggers.
            let target_table = target.resolved_table()?;
            let reads_target = rows
                .iter()
                .flatten()
                .any(|value| subquery_reads_table(document, value, target_table));
            emit_coroutine_rows(
                program,
                insert,
                &row,
                RowSink {
                    column_values: &column_values,
                    rowid_value,
                    index_cursors: &index_cursors,
                    returning,
                },
                (yield_reg, values),
                reads_target || !triggers.is_empty(),
                done,
            )?;
        }
    }
    program.preassign_label_to_next_insn(done);
    if let Some(returning) = returning {
        returning.emit_rows(program);
    }
    Ok(())
}

/// Whether `expression` has a subquery that reads `table`.
fn subquery_reads_table(
    document: &hir::HirDocument,
    expression: &hir::Expr,
    table: &hir::ResolvedTable,
) -> bool {
    let mut reads = false;
    expression.for_each(&mut |expression| {
        if let hir::Expr::Subquery(subquery) = expression {
            reads |= document.query_reads_table(super::dml::subquery_query(subquery), table);
        }
    });
    reads
}

pub(super) fn insert_root(plan: &HirPlan) -> Result<&hir::Insert> {
    let HirRoot::Insert(insert) = &plan.document.root else {
        return Err(LimboError::InternalError(
            "HIR INSERT analysis did not produce an INSERT root".into(),
        ));
    };
    Ok(insert)
}

#[derive(Clone, Copy)]
enum ColumnValue<'a> {
    Null,
    /// The value at this position of each VALUES row.
    Row(usize),
    Default(&'a hir::Expr),
}

/// What every inserted row needs besides its values.
struct RowSink<'a, 'b> {
    column_values: &'a [ColumnValue<'b>],
    rowid_value: Option<usize>,
    index_cursors: &'a [(&'b hir::IndexExpressions, CursorID)],
    returning: Option<ReturningBuffer<'b>>,
}

/// INSERT ... SELECT: the query runs as a coroutine that yields one row at
/// a time. Like legacy and SQLite, when the query reads the target table, or
/// the INSERT fires triggers, every row is first copied into an ephemeral
/// table, so inserting never changes what the query reads.
fn emit_select_rows(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    insert: &hir::Insert,
    query: hir::QueryId,
    row: &InsertRow<'_>,
    sink: RowSink<'_, '_>,
    done: BranchOffset,
) -> Result<()> {
    let document = &plan.document;
    let width = insert.columns.len();
    let yield_reg = program.alloc_register();
    let values = program.alloc_registers(width.max(1));
    let start = program.allocate_label();
    let defined = program.allocate_label();
    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: defined,
        start_offset: start,
    });
    program.preassign_label_to_next_insn(start);
    program.bind_coroutine_row(yield_reg, values);
    super::query::emit_planned_query_body(
        program,
        plan,
        query,
        &QueryDestination::CoroutineYield {
            yield_reg,
            coroutine_implementation_start: start,
        },
    )?;
    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(defined);
    let reads_target = document.query_reads_table(query, row.target.resolved_table()?);
    emit_coroutine_rows(
        program,
        insert,
        row,
        sink,
        (yield_reg, values),
        reads_target || !row.triggers.is_empty(),
        done,
    )
}

/// Insert each row a coroutine yields into `values`. With `use_temp`, every
/// row is first copied into an ephemeral table, so that inserting cannot
/// change the rows still to come.
fn emit_coroutine_rows(
    program: &mut ProgramBuilder,
    insert: &hir::Insert,
    row: &InsertRow<'_>,
    sink: RowSink<'_, '_>,
    (yield_reg, values): (usize, usize),
    use_temp: bool,
    done: BranchOffset,
) -> Result<()> {
    let width = insert.columns.len();
    let table = row.target.table;
    if !use_temp {
        let next = program.allocate_label();
        program.preassign_label_to_next_insn(next);
        program.emit_insn(Insn::Yield {
            yield_reg,
            end_offset: done,
            subtype_clear_start_reg: 0,
            subtype_clear_count: 0,
        });
        row.emit(
            program,
            sink.column_values,
            sink.rowid_value,
            RowValues::Registers(values),
            sink.index_cursors,
            sink.returning,
            next,
        )?;
        program.emit_insn(Insn::Goto { target_pc: next });
        return Ok(());
    }

    let temp = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: temp,
        is_table: true,
    });
    let fill = program.allocate_label();
    let filled = program.allocate_label();
    program.preassign_label_to_next_insn(fill);
    program.emit_insn(Insn::Yield {
        yield_reg,
        end_offset: filled,
        subtype_clear_start_reg: 0,
        subtype_clear_count: 0,
    });
    let record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(values),
        count: to_u32(width),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: Some(
            insert
                .columns
                .iter()
                .map(|target| match target.column {
                    hir::TargetColumn::RowId => Affinity::Integer.aff_mask(),
                    hir::TargetColumn::Column(column) => table.columns()[column]
                        .affinity_with_strict(table.is_strict)
                        .aff_mask(),
                })
                .collect(),
        ),
    });
    let temp_rowid = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: temp,
        rowid_reg: temp_rowid,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: temp,
        key_reg: temp_rowid,
        record_reg: record,
        flag: InsertFlags::new()
            .require_seek()
            .is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Goto { target_pc: fill });
    program.preassign_label_to_next_insn(filled);

    let empty = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: temp,
        pc_if_empty: empty,
    });
    let top = program.allocate_label();
    let next = program.allocate_label();
    program.preassign_label_to_next_insn(top);
    for position in 0..width {
        program.emit_insn(Insn::Column {
            cursor_id: temp,
            column: position,
            dest: values + position,
            default: None,
        });
    }
    row.emit(
        program,
        sink.column_values,
        sink.rowid_value,
        RowValues::Registers(values),
        sink.index_cursors,
        sink.returning,
        next,
    )?;
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: temp,
        pc_if_next: top,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(empty);
    program.emit_insn(Insn::Close { cursor_id: temp });
    program.emit_insn(Insn::Goto { target_pc: done });
    Ok(())
}

/// Where the values of the row being inserted are.
#[derive(Clone, Copy)]
enum RowValues<'a> {
    Expressions(&'a [hir::Expr]),
    /// A coroutine yields each row into registers starting here.
    Registers(usize),
}

/// The registers of the row an INSERT writes: the rowid, then one register
/// per column.
struct InsertRow<'a> {
    target: &'a DmlTarget<'a>,
    table_cursor: CursorID,
    rowid: usize,
    columns: usize,
    or_conflict: ResolveType,
    has_statement_conflict: bool,
    triggers: &'a [CompiledTrigger],
    foreign_keys: &'a hir::DmlForeignKeys,
    replace_defaults: &'a [hir::ResolvedDefault],
    upserts: &'a [hir::Upsert],
    excluded: Option<hir::SourceId>,
    upsert_new: Option<hir::SourceId>,
    /// The rowid of the row an UPSERT conflicts with.
    conflict_rowid: usize,
    /// False under `PRAGMA ignore_check_constraints`.
    enforce_checks: bool,
}

impl InsertRow<'_> {
    #[allow(clippy::too_many_arguments)]
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        column_values: &[ColumnValue<'_>],
        rowid_value: Option<usize>,
        values: RowValues<'_>,
        index_cursors: &[(&hir::IndexExpressions, CursorID)],
        returning: Option<ReturningBuffer<'_>>,
        row_done: BranchOffset,
    ) -> Result<()> {
        let document = self.target.document;
        let table = self.target.table;
        let emit_value = |program: &mut ProgramBuilder, position: usize, register: usize| {
            match values {
                RowValues::Expressions(values) => {
                    super::expr::translate_expr_no_constant_opt(
                        program,
                        document,
                        &values[position],
                        register,
                    )?;
                }
                RowValues::Registers(start) => program.emit_insn(Insn::Copy {
                    src_reg: start + position,
                    dst_reg: register,
                    extra_amount: 0,
                }),
            }
            Ok::<_, LimboError>(())
        };
        for (column, value) in column_values.iter().enumerate() {
            let register = self.columns + column;
            match value {
                ColumnValue::Null => program.emit_null(register, None),
                ColumnValue::Row(position) => emit_value(program, *position, register)?,
                ColumnValue::Default(value) => {
                    super::expr::translate_expr_no_constant_opt(
                        program, document, value, register,
                    )?;
                }
            }
        }
        if let Some(position) = rowid_value {
            emit_value(program, position, self.rowid)?;
        }
        // Like legacy, apply column affinity before any check reads the row.
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
                    start_reg: self.columns,
                    count,
                    affinities: affinities
                        .iter()
                        .map(|affinity| affinity.aff_mask())
                        .collect(),
                });
            }
        }

        let new_row = TriggerRow {
            columns: self.columns,
            rowid: self.rowid,
        };
        if self
            .triggers
            .iter()
            .any(|trigger| trigger.time == TriggerTime::Before)
        {
            // Like legacy and SQLite, NEW.rowid reads -1 in a BEFORE trigger
            // while the rowid is not known yet.
            let saved = rowid_value.map(|_| {
                let saved = program.alloc_register();
                program.emit_insn(Insn::Copy {
                    src_reg: self.rowid,
                    dst_reg: saved,
                    extra_amount: 0,
                });
                saved
            });
            let known = program.allocate_label();
            if rowid_value.is_some() {
                program.emit_insn(Insn::NotNull {
                    reg: self.rowid,
                    target_pc: known,
                });
            }
            program.emit_insn(Insn::Integer {
                value: -1,
                dest: self.rowid,
            });
            program.preassign_label_to_next_insn(known);
            for trigger in self
                .triggers
                .iter()
                .filter(|trigger| trigger.time == TriggerTime::Before)
            {
                trigger.emit_fire(
                    program,
                    None,
                    Some(new_row),
                    table.columns().len(),
                    row_done,
                )?;
            }
            if let Some(saved) = saved {
                program.emit_insn(Insn::Copy {
                    src_reg: saved,
                    dst_reg: self.rowid,
                    extra_amount: 0,
                });
            }
        }

        // A NULL rowid from the statement is generated like a missing one.
        let generate = program.allocate_label();
        let ready = program.allocate_label();
        if rowid_value.is_some() {
            let integer = program.allocate_label();
            program.emit_insn(Insn::NotNull {
                reg: self.rowid,
                target_pc: integer,
            });
            program.emit_insn(Insn::Goto {
                target_pc: generate,
            });
            program.preassign_label_to_next_insn(integer);
            program.emit_insn(Insn::MustBeInt {
                reg: self.rowid,
                target_pc: None,
            });
            program.emit_insn(Insn::Goto { target_pc: ready });
        }
        program.preassign_label_to_next_insn(generate);
        program.emit_insn(Insn::NewRowid {
            cursor: self.table_cursor,
            rowid_reg: self.rowid,
            prev_largest_reg: 0,
        });
        program.preassign_label_to_next_insn(ready);

        // CHECK constraints, index expressions, partial-index WHERE terms
        // and RETURNING read the new row.
        self.bind_new_row(program);
        // Like legacy, CHECK constraints run once the rowid is known, before
        // NOT NULL.
        if self.enforce_checks {
            super::dml::emit_check_constraints(
                program,
                document,
                self.check_constraints(self.target.source.id)?,
                self.or_conflict,
                row_done,
            )?;
        }
        for (position, column) in table.columns().iter().enumerate() {
            if column.notnull() && !column.is_rowid_alias() {
                self.emit_notnull_check(program, position, column, row_done)?;
            }
        }
        // Like legacy, the constraints an UPSERT names are checked first, in
        // the UPSERT's order, then the rowid and the indexes.
        let arms = self
            .upserts
            .iter()
            .map(|_| program.allocate_label())
            .collect::<Vec<_>>();
        let mut constraints = rowid_value
            .map(|_| Constraint::RowId)
            .into_iter()
            .chain((0..index_cursors.len()).map(Constraint::Index))
            .map(|constraint| (constraint, self.upsert_for(constraint, index_cursors)))
            .collect::<Vec<_>>();
        constraints.sort_by_key(|(_, upsert)| upsert.unwrap_or(usize::MAX));
        // An INTEGER PRIMARY KEY whose own conflict clause is REPLACE is
        // checked last, so that a FAIL or IGNORE on an index acts before any
        // row is deleted.
        if !self.has_statement_conflict
            && table.rowid_alias_conflict_clause == Some(ResolveType::Replace)
            && constraints.len() > 1
            && !constraints
                .iter()
                .any(|(constraint, upsert)| *constraint == Constraint::RowId && upsert.is_some())
        {
            if let Some(position) = constraints
                .iter()
                .position(|(constraint, _)| *constraint == Constraint::RowId)
            {
                let rowid = constraints.remove(position);
                constraints.push(rowid);
            }
        }
        let catch_all = self
            .upserts
            .last()
            .filter(|upsert| upsert.target.is_none())
            .map(|_| self.upserts.len() - 1);
        // Check every index before writing any of them. Under REPLACE, an
        // index is written as soon as it is checked, as in legacy.
        let mut keys = (0..index_cursors.len()).map(|_| None).collect::<Vec<_>>();
        for (constraint, upsert) in constraints {
            let route = upsert.or(catch_all).map(|upsert| (upsert, arms[upsert]));
            match constraint {
                Constraint::RowId => {
                    self.emit_rowid_check(program, index_cursors, route, row_done)?
                }
                Constraint::Index(position) => {
                    let (index, cursor) = index_cursors[position];
                    keys[position] = Some(self.emit_index_key(
                        program,
                        index,
                        cursor,
                        index_cursors,
                        route,
                        row_done,
                    )?);
                }
            }
        }
        let keys = keys
            .into_iter()
            .map(|key| {
                key.ok_or_else(|| {
                    LimboError::InternalError("HIR INSERT skipped an index key".into())
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(self.columns),
            count: to_u32(table.columns().len()),
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
        // Like legacy, the parent must exist before anything is written.
        super::fkey::emit_insert_child_checks(
            program,
            document,
            self.foreign_keys,
            self.target.source.id,
            self.rowid,
            self.target.database,
        )?;
        for ((index, cursor), key) in index_cursors.iter().zip(&keys) {
            if key.written {
                continue;
            }
            let skip = key.in_index.map(|in_index| {
                let skip = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: in_index,
                    target_pc: skip,
                    jump_if_null: true,
                });
                skip
            });
            let index_record = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(key.start),
                count: to_u32(key.count),
                dest_reg: to_u32(index_record),
                index_name: Some(index.index.value().name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: *cursor,
                record_reg: index_record,
                unpacked_start: Some(key.start),
                unpacked_count: Some(to_u32(key.count)),
                flags: IdxInsertFlags::new().nchange(true),
            });
            if let Some(skip) = skip {
                program.preassign_label_to_next_insn(skip);
            }
        }
        // A REPLACE may have deleted a row and moved the cursor.
        let flag = if self.may_replace() {
            InsertFlags::new().require_seek()
        } else {
            InsertFlags::new()
        };
        program.emit_insn(Insn::Insert {
            cursor: self.table_cursor,
            key_reg: self.rowid,
            record_reg: record,
            flag,
            table_name: table.name.clone(),
        });
        // RAISE(IGNORE) in an AFTER trigger only ends that trigger's body.
        let after_triggers = program.allocate_label();
        for trigger in self
            .triggers
            .iter()
            .filter(|trigger| trigger.time == TriggerTime::After)
        {
            trigger.emit_fire(
                program,
                None,
                Some(new_row),
                table.columns().len(),
                after_triggers,
            )?;
        }
        program.preassign_label_to_next_insn(after_triggers);
        super::fkey::emit_insert_parent_repairs(
            program,
            document,
            self.foreign_keys,
            self.target.source.id,
            self.target.database,
            self.may_replace(),
        )?;
        if let Some(returning) = returning {
            let start = returning.emit_values(program, document, self.target.resolved_table()?)?;
            returning.emit_buffer(program, start);
        }
        if !self.upserts.is_empty() {
            program.emit_insn(Insn::Goto {
                target_pc: row_done,
            });
            for (upsert, arm) in self.upserts.iter().zip(arms) {
                program.preassign_label_to_next_insn(arm);
                match &upsert.action {
                    hir::UpsertAction::Nothing => program.emit_insn(Insn::Goto {
                        target_pc: row_done,
                    }),
                    hir::UpsertAction::Update {
                        assignments,
                        predicate,
                    } => self.emit_do_update(
                        program,
                        assignments,
                        predicate.as_ref(),
                        index_cursors,
                        returning,
                        row_done,
                    )?,
                }
            }
        }
        Ok(())
    }

    /// The DO UPDATE arm of an UPSERT, entered with the conflicting row's
    /// rowid in `conflict_rowid`: like legacy, rewrite that row from its
    /// current values and `excluded`, checking every UNIQUE constraint with
    /// ABORT, then go to the next row.
    fn emit_do_update(
        &self,
        program: &mut ProgramBuilder,
        assignments: &[hir::Assignment],
        predicate: Option<&hir::Expr>,
        index_cursors: &[(&hir::IndexExpressions, CursorID)],
        returning: Option<ReturningBuffer<'_>>,
        row_done: BranchOffset,
    ) -> Result<()> {
        let DmlTarget {
            document,
            source,
            table,
            database,
        } = *self.target;
        let (Some(excluded), Some(new_source)) = (self.excluded, self.upsert_new) else {
            return Err(LimboError::InternalError(
                "HIR UPSERT DO UPDATE has no EXCLUDED or NEW row".into(),
            ));
        };
        let new_source = document.source(new_source).ok_or_else(|| {
            LimboError::InternalError(format!("HIR UPSERT NEW row {new_source} is missing"))
        })?;
        let conflict = self.conflict_rowid;
        program.emit_insn(Insn::SeekRowid {
            cursor_id: self.table_cursor,
            src_reg: conflict,
            target_pc: row_done,
        });
        // The target reads the conflicting row; `excluded` reads the row
        // that was not inserted.
        program.bind_source(
            source.id,
            SourceBinding::BTree {
                scan_cursor: self.table_cursor,
                table_cursor: None,
            },
        );
        program.bind_source(
            excluded,
            SourceBinding::Registers {
                start: self.columns,
                rowid: Some(self.rowid),
            },
        );
        if let Some(predicate) = predicate {
            let register = program.alloc_register();
            super::expr::translate_expr_no_constant_opt(program, document, predicate, register)?;
            program.emit_insn(Insn::IfNot {
                reg: register,
                target_pc: row_done,
                jump_if_null: true,
            });
        }

        let columns = table.columns().len();
        let mut values = vec![None; columns];
        for assignment in assignments {
            let [hir::TargetColumn::Column(column)] = assignment.columns.as_slice() else {
                return Err(LimboError::InternalError(
                    "HIR UPSERT assignment does not set exactly one column".into(),
                ));
            };
            values[*column] = Some(&assignment.value);
        }
        let new_row = program.alloc_registers(columns);
        for (position, (column, value)) in table.columns().iter().zip(&values).enumerate() {
            let register = new_row + position;
            match value {
                Some(value) => {
                    super::expr::translate_expr_no_constant_opt(
                        program, document, value, register,
                    )?;
                    if column.notnull() && !column.is_rowid_alias() {
                        program.emit_insn(Insn::HaltIfNull {
                            target_reg: register,
                            err_code: SQLITE_CONSTRAINT_NOTNULL,
                            description: format!(
                                "{}.{}",
                                table.name,
                                column.name.as_deref().unwrap_or_default()
                            ),
                        });
                    }
                }
                // Like the stored record, an INTEGER PRIMARY KEY column is
                // NULL; reads of it use the rowid.
                None if column.is_rowid_alias() => program.emit_null(register, None),
                None => {
                    super::expr::translate_expr_no_constant_opt(
                        program,
                        document,
                        &hir::Expr::Column(hir::ColumnRef {
                            source: source.id,
                            column: position,
                        }),
                        register,
                    )?;
                }
            }
        }
        let affinities = table
            .columns()
            .iter()
            .map(|column| column.affinity())
            .collect::<Vec<_>>();
        if affinities
            .iter()
            .any(|affinity| *affinity != Affinity::Blob)
        {
            if let Some(count) = NonZeroUsize::new(columns) {
                program.emit_insn(Insn::Affinity {
                    start_reg: new_row,
                    count,
                    affinities: affinities
                        .iter()
                        .map(|affinity| affinity.aff_mask())
                        .collect(),
                });
            }
        }
        program.bind_source(
            new_source.id,
            SourceBinding::Registers {
                start: new_row,
                rowid: Some(conflict),
            },
        );
        // Like legacy, every CHECK constraint is checked with ABORT.
        if self.enforce_checks {
            super::dml::emit_check_constraints(
                program,
                document,
                self.check_constraints(new_source.id)?,
                ResolveType::Abort,
                row_done,
            )?;
        }

        let assigned = assignments
            .iter()
            .flat_map(|assignment| assignment.columns.iter().copied())
            .collect::<Vec<_>>();
        let assigned_positions = assigned
            .iter()
            .filter_map(|column| match column {
                hir::TargetColumn::Column(column) => Some(*column),
                hir::TargetColumn::RowId => None,
            })
            .collect::<Vec<_>>();
        let pending_repairs = super::fkey::emit_update_checks(
            program,
            document,
            self.foreign_keys,
            source.id,
            new_source.id,
            conflict,
            &assigned_positions,
            database,
            false,
        )?;

        // Like legacy, every UNIQUE constraint is checked against the new
        // row before any index changes.
        struct Rebuild<'x> {
            cursor: CursorID,
            old: &'x hir::IndexExpressions,
            old_in_index: Option<usize>,
            new_in_index: Option<usize>,
            start: usize,
            count: usize,
            record: usize,
        }
        let mut rebuilds = Vec::new();
        for (old, cursor) in index_cursors {
            if !super::update::index_reads_assigned_column(source.id, old, &assigned) {
                continue;
            }
            let index = old.index.value();
            let new = new_source
                .index_expressions
                .iter()
                .find(|new| new.index == old.index)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "HIR UPSERT NEW row does not list index {}",
                        index.name
                    ))
                })?;
            let old_in_index = old
                .predicate
                .as_ref()
                .map(|predicate| -> Result<usize> {
                    let register = program.alloc_register();
                    super::expr::translate_expr_no_constant_opt(
                        program, document, predicate, register,
                    )?;
                    Ok(register)
                })
                .transpose()?;
            let new_in_index = new
                .predicate
                .as_ref()
                .map(|predicate| -> Result<usize> {
                    let register = program.alloc_register();
                    super::expr::translate_expr_no_constant_opt(
                        program, document, predicate, register,
                    )?;
                    Ok(register)
                })
                .transpose()?;
            // A new key outside a partial index is never written, so it
            // cannot conflict.
            let checked = program.allocate_label();
            if let Some(new_in_index) = new_in_index {
                program.emit_insn(Insn::IfNot {
                    reg: new_in_index,
                    target_pc: checked,
                    jump_if_null: true,
                });
            }
            let width = index.columns.len();
            let start = program.alloc_registers(width + 1);
            for (offset, (column, expression)) in index.columns.iter().zip(&new.columns).enumerate()
            {
                match expression {
                    Some(expression) => super::expr::translate_expr_no_constant_opt(
                        program,
                        document,
                        expression,
                        start + offset,
                    )?,
                    None => {
                        let from = if table.columns()[column.pos_in_table].is_rowid_alias() {
                            conflict
                        } else {
                            new_row + column.pos_in_table
                        };
                        program.emit_insn(Insn::Copy {
                            src_reg: from,
                            dst_reg: start + offset,
                            extra_amount: 0,
                        });
                        start + offset
                    }
                };
            }
            program.emit_insn(Insn::Copy {
                src_reg: conflict,
                dst_reg: start + width,
                extra_amount: 0,
            });
            let record = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(start),
                count: to_u32(width + 1),
                dest_reg: to_u32(record),
                index_name: Some(index.name.clone()),
                affinity_str: None,
            });
            if index.unique {
                if let Some(count) = NonZeroUsize::new(width) {
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
                program.emit_insn(Insn::NoConflict {
                    cursor_id: *cursor,
                    target_pc: checked,
                    record_reg: start,
                    num_regs: width,
                });
                // The entry found may be this row's own.
                let found = program.alloc_register();
                program.emit_insn(Insn::IdxRowId {
                    cursor_id: *cursor,
                    dest: found,
                });
                program.emit_insn(Insn::Eq {
                    lhs: conflict,
                    rhs: found,
                    target_pc: checked,
                    flags: CmpInsFlags::default(),
                    collation: program.curr_collation(),
                });
                // Legacy reports this as a PRIMARY KEY failure.
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                    description: format_unique_violation_desc(&table.name, index),
                    on_error: None,
                    description_reg: None,
                });
            }
            program.preassign_label_to_next_insn(checked);
            rebuilds.push(Rebuild {
                cursor: *cursor,
                old,
                old_in_index,
                new_in_index,
                start,
                count: width + 1,
                record,
            });
        }
        for rebuild in &rebuilds {
            let skip_delete = rebuild.old_in_index.map(|old_in_index| {
                let skip = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: old_in_index,
                    target_pc: skip,
                    jump_if_null: true,
                });
                skip
            });
            let (start, count) = super::dml::emit_index_key(
                program,
                document,
                source,
                table,
                self.table_cursor,
                rebuild.old,
            )?;
            program.emit_insn(Insn::IdxDelete {
                start_reg: start,
                num_regs: count,
                cursor_id: rebuild.cursor,
                raise_error_if_no_matching_entry: false,
            });
            if let Some(skip) = skip_delete {
                program.preassign_label_to_next_insn(skip);
            }
            let skip_insert = rebuild.new_in_index.map(|new_in_index| {
                let skip = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: new_in_index,
                    target_pc: skip,
                    jump_if_null: true,
                });
                skip
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: rebuild.cursor,
                record_reg: rebuild.record,
                unpacked_start: Some(rebuild.start),
                unpacked_count: Some(to_u32(rebuild.count)),
                flags: IdxInsertFlags::new().nchange(true),
            });
            if let Some(skip) = skip_insert {
                program.preassign_label_to_next_insn(skip);
            }
        }

        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(new_row),
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
        program.emit_insn(Insn::Insert {
            cursor: self.table_cursor,
            key_reg: conflict,
            record_reg: record,
            flag: InsertFlags::new().skip_last_rowid(),
            table_name: table.name.clone(),
        });
        super::fkey::emit_update_repairs(program, &pending_repairs, database)?;
        // RETURNING reads the row as written.
        program.bind_source(
            source.id,
            SourceBinding::Registers {
                start: new_row,
                rowid: Some(conflict),
            },
        );
        if let Some(returning) = returning {
            let start = returning.emit_values(program, document, self.target.resolved_table()?)?;
            returning.emit_buffer(program, start);
        }
        program.emit_insn(Insn::Goto {
            target_pc: row_done,
        });
        Ok(())
    }

    /// The first UPSERT whose conflict target names `constraint`.
    fn upsert_for(
        &self,
        constraint: Constraint,
        index_cursors: &[(&hir::IndexExpressions, CursorID)],
    ) -> Option<usize> {
        self.upserts.iter().position(|upsert| {
            let Some(target) = &upsert.target else {
                return false;
            };
            match (constraint, &target.matched_index) {
                (Constraint::RowId, None) => true,
                (Constraint::Index(position), Some(index)) => {
                    index_cursors[position].0.index == *index
                }
                _ => false,
            }
        })
    }

    fn conflict(&self, declared: Option<ResolveType>) -> ResolveType {
        if self.has_statement_conflict {
            self.or_conflict
        } else {
            declared.unwrap_or(ResolveType::Abort)
        }
    }

    /// Whether some constraint of the row resolves conflicts with REPLACE.
    /// Like legacy, an UPSERT turns REPLACE off.
    fn may_replace(&self) -> bool {
        let table = self.target.table;
        if !self.upserts.is_empty() {
            return false;
        }
        if self.has_statement_conflict {
            return self.or_conflict == ResolveType::Replace;
        }
        table.rowid_alias_conflict_clause == Some(ResolveType::Replace)
            || self
                .target
                .source
                .index_expressions
                .iter()
                .any(|index| index.index.value().on_conflict == Some(ResolveType::Replace))
    }

    /// The CHECK constraints analysis froze for `source`.
    fn check_constraints(&self, source: hir::SourceId) -> Result<&[hir::CheckConstraint]> {
        self.target
            .document
            .source(source)
            .and_then(|source| source.check_constraints.as_deref())
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR INSERT source {source} has no CHECK constraints"
                ))
            })
    }

    fn bind_new_row(&self, program: &mut ProgramBuilder) {
        program.bind_source(
            self.target.source.id,
            SourceBinding::Registers {
                start: self.columns,
                rowid: Some(self.rowid),
            },
        );
    }

    fn emit_notnull_check(
        &self,
        program: &mut ProgramBuilder,
        position: usize,
        column: &Column,
        row_done: BranchOffset,
    ) -> Result<()> {
        let register = self.columns + position;
        let conflict = self.conflict(column.notnull_conflict_clause);
        // REPLACE stores the column's DEFAULT instead of a NULL. Without a
        // DEFAULT, or if it is NULL too, the statement aborts.
        if conflict == ResolveType::Replace {
            if let Some(default) = self
                .replace_defaults
                .iter()
                .find(|default| default.column == position)
            {
                let not_null = program.allocate_label();
                program.emit_insn(Insn::NotNull {
                    reg: register,
                    target_pc: not_null,
                });
                super::expr::translate_expr_no_constant_opt(
                    program,
                    self.target.document,
                    &default.value,
                    register,
                )?;
                let affinity = column.affinity();
                if !self.target.table.is_strict && affinity != Affinity::Blob {
                    program.emit_column_affinity(register, affinity);
                }
                program.preassign_label_to_next_insn(not_null);
            }
        }
        match conflict {
            ResolveType::Ignore => program.emit_insn(Insn::IsNull {
                reg: register,
                target_pc: row_done,
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
        Ok(())
    }

    /// REPLACE: delete the row whose rowid is in `conflict`, then bind the
    /// target to the new row again.
    fn emit_replace_delete(
        &self,
        program: &mut ProgramBuilder,
        index_cursors: &[(&hir::IndexExpressions, CursorID)],
        conflict: usize,
    ) -> Result<()> {
        super::dml::emit_replace_delete(
            program,
            self.target,
            self.table_cursor,
            index_cursors,
            self.foreign_keys,
            conflict,
            None,
        )?;
        self.bind_new_row(program);
        Ok(())
    }

    /// Write an index entry for the new row as soon as it is checked, as
    /// legacy does under REPLACE.
    fn emit_index_insert(
        &self,
        program: &mut ProgramBuilder,
        cursor: CursorID,
        key: &IndexKey,
        name: &str,
    ) {
        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(key.start),
            count: to_u32(key.count),
            dest_reg: to_u32(record),
            index_name: Some(name.to_string()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: cursor,
            record_reg: record,
            unpacked_start: Some(key.start),
            unpacked_count: Some(to_u32(key.count)),
            flags: IdxInsertFlags::new().nchange(true),
        });
    }

    /// An explicit rowid must not belong to another row.
    fn emit_rowid_check(
        &self,
        program: &mut ProgramBuilder,
        index_cursors: &[(&hir::IndexExpressions, CursorID)],
        route: Option<(usize, BranchOffset)>,
        row_done: BranchOffset,
    ) -> Result<()> {
        let table = self.target.table;
        let unique = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: self.table_cursor,
            rowid_reg: self.rowid,
            target_pc: unique,
        });
        let conflict = self.conflict(table.rowid_alias_conflict_clause);
        match conflict {
            // An UPSERT for this conflict takes over the row.
            _ if route.is_some() => {
                let (_, arm) = route.expect("route was checked");
                program.emit_insn(Insn::Copy {
                    src_reg: self.rowid,
                    dst_reg: self.conflict_rowid,
                    extra_amount: 0,
                });
                program.emit_insn(Insn::Goto { target_pc: arm });
            }
            ResolveType::Ignore => program.emit_insn(Insn::Goto {
                target_pc: row_done,
            }),
            ResolveType::Replace if self.upserts.is_empty() => {
                let conflict = program.alloc_register();
                program.emit_insn(Insn::Copy {
                    src_reg: self.rowid,
                    dst_reg: conflict,
                    extra_amount: 0,
                });
                self.emit_replace_delete(program, index_cursors, conflict)?;
            }
            conflict => {
                let column = table
                    .get_rowid_alias_column()
                    .and_then(|(_, column)| column.name.clone())
                    .unwrap_or_else(|| "rowid".to_string());
                let (description, on_error) = halt_desc_and_on_error(
                    &format!("{}.{column}", table.name),
                    conflict,
                    self.has_statement_conflict,
                );
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                    description,
                    on_error,
                    description_reg: None,
                });
            }
        }
        program.preassign_label_to_next_insn(unique);
        Ok(())
    }

    /// Build an index's key for the new row and, for a UNIQUE index, check
    /// that no row has it yet.
    fn emit_index_key(
        &self,
        program: &mut ProgramBuilder,
        expressions: &hir::IndexExpressions,
        cursor: CursorID,
        index_cursors: &[(&hir::IndexExpressions, CursorID)],
        route: Option<(usize, BranchOffset)>,
        row_done: BranchOffset,
    ) -> Result<IndexKey> {
        let document = self.target.document;
        let table = self.target.table;
        let index = expressions.index.value();
        let in_index = expressions
            .predicate
            .as_ref()
            .map(|predicate| {
                let register = program.alloc_register();
                super::expr::translate_expr_no_constant_opt(
                    program, document, predicate, register,
                )?;
                Ok::<_, LimboError>(register)
            })
            .transpose()?;
        let columns = index.columns.len();
        let start = program.alloc_registers(columns + 1);
        for (offset, (column, expression)) in
            index.columns.iter().zip(&expressions.columns).enumerate()
        {
            let register = start + offset;
            match expression {
                Some(expression) => {
                    super::expr::translate_expr_no_constant_opt(
                        program, document, expression, register,
                    )?;
                }
                None => {
                    let source = if table.columns()[column.pos_in_table].is_rowid_alias() {
                        self.rowid
                    } else {
                        self.columns + column.pos_in_table
                    };
                    program.emit_insn(Insn::Copy {
                        src_reg: source,
                        dst_reg: register,
                        extra_amount: 0,
                    });
                }
            }
        }
        program.emit_insn(Insn::Copy {
            src_reg: self.rowid,
            dst_reg: start + columns,
            extra_amount: 0,
        });
        let conflict = self.conflict(index.on_conflict);
        // A row outside a partial index is neither checked nor written now.
        let outside = program.allocate_label();
        if index.unique {
            let unique = program.allocate_label();
            // A row outside a partial index cannot conflict in it.
            if let Some(in_index) = in_index {
                program.emit_insn(Insn::IfNot {
                    reg: in_index,
                    target_pc: unique,
                    jump_if_null: true,
                });
            }
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
            program.emit_insn(Insn::NoConflict {
                cursor_id: cursor,
                target_pc: unique,
                record_reg: start,
                num_regs: columns,
            });
            match conflict {
                // An UPSERT for this conflict takes over the row.
                _ if route.is_some() => {
                    let (upsert, arm) = route.expect("route was checked");
                    match self.upserts[upsert].action {
                        hir::UpsertAction::Nothing => program.emit_insn(Insn::Goto {
                            target_pc: row_done,
                        }),
                        hir::UpsertAction::Update { .. } => {
                            program.emit_insn(Insn::IdxRowId {
                                cursor_id: cursor,
                                dest: self.conflict_rowid,
                            });
                            program.emit_insn(Insn::Goto { target_pc: arm });
                        }
                    }
                }
                // Like legacy, with an UPSERT any other conflict stops the
                // statement.
                ResolveType::Ignore if self.upserts.is_empty() => program.emit_insn(Insn::Goto {
                    target_pc: row_done,
                }),
                ResolveType::Replace if self.upserts.is_empty() => {
                    let conflicting = program.alloc_register();
                    program.emit_insn(Insn::IdxRowId {
                        cursor_id: cursor,
                        dest: conflicting,
                    });
                    self.emit_replace_delete(program, index_cursors, conflicting)?;
                }
                conflict => {
                    let (description, on_error) = halt_desc_and_on_error(
                        &format_unique_violation_desc(&table.name, index),
                        conflict,
                        self.has_statement_conflict,
                    );
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
        let key = IndexKey {
            start,
            count: columns + 1,
            in_index,
            written: conflict == ResolveType::Replace && self.upserts.is_empty(),
        };
        if key.written {
            if let Some(in_index) = in_index {
                program.emit_insn(Insn::IfNot {
                    reg: in_index,
                    target_pc: outside,
                    jump_if_null: true,
                });
            }
            self.emit_index_insert(program, cursor, &key, &index.name);
        }
        program.preassign_label_to_next_insn(outside);
        Ok(key)
    }
}

/// A constraint the new row is checked against: its rowid, or the index at
/// a position of the target's index list.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Constraint {
    RowId,
    Index(usize),
}

/// A new index key built before any index is written.
struct IndexKey {
    start: usize,
    count: usize,
    /// Partial index: whether the new row is in the index.
    in_index: Option<usize>,
    /// REPLACE already wrote the entry when the index was checked.
    written: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sync::Arc,
        translate::semantic_lowering::query::tests::{analyze_plan, indexed_items_schema, program},
    };

    /// `indexed_items_schema` with `checked(id INTEGER PRIMARY KEY, a
    /// CHECK (a > 0), b CHECK (b > 0))`.
    fn schema() -> crate::schema::Schema {
        let mut schema = indexed_items_schema();
        schema
            .add_btree_table(Arc::new(
                crate::schema::BTreeTable::from_sql(
                    "CREATE TABLE checked(id INTEGER PRIMARY KEY, a CHECK (a > 0), b CHECK (b > 0))",
                    100,
                )
                .unwrap(),
            ))
            .unwrap();
        schema
    }

    fn emit(sql: &str) -> ProgramBuilder {
        let plan = Arc::new(analyze_plan(schema(), sql));
        let mut program = program();
        let insert = insert_root(&plan).unwrap();
        assert!(!needs_legacy(&plan, insert, &program).unwrap(), "{sql}");
        program.set_hir_plan(plan.clone());
        emit_insert(&mut program, &plan, insert, &[], true).unwrap();
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
    fn single_row_insert_writes_the_row_and_its_index_entry() {
        let program = emit("INSERT INTO items VALUES (1, 'x')");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::NewRowid { .. })),
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
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::InitCoroutine { .. })),
            0
        );
    }

    #[test]
    fn insert_checks_every_check_constraint() {
        let program = emit("INSERT INTO checked(a) VALUES (1)");
        assert_eq!(
            count(&program, |insn| matches!(
                insn,
                Insn::Halt { err_code, .. } if *err_code == crate::error::SQLITE_CONSTRAINT_CHECK
            )),
            2
        );
    }

    #[test]
    fn several_rows_run_as_a_coroutine_with_one_copy_of_the_row_code() {
        let program = emit("INSERT INTO items VALUES (1, 'x'), (2, 'y'), (3, 'z')");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::InitCoroutine { .. })),
            1
        );
        // One yield per row, and one where the rows are read.
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::Yield { .. })),
            4
        );
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::Insert { .. })),
            1
        );
    }

    #[test]
    fn insert_select_reading_its_own_table_goes_through_a_temp_table() {
        let program = emit("INSERT INTO items SELECT value + 1, extra FROM items");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::OpenEphemeral { .. })),
            1
        );
        let program = emit("INSERT INTO items SELECT 1, 'x'");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::OpenEphemeral { .. })),
            0
        );
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::InitCoroutine { .. })),
            1
        );
    }

    #[test]
    fn values_reading_their_own_table_go_through_a_temp_table() {
        let program =
            emit("INSERT INTO items VALUES ((SELECT max(value) FROM items), 'x'), (2, 'y')");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::OpenEphemeral { .. })),
            1
        );
        let program = emit("INSERT INTO items VALUES ((SELECT 1), 'x'), (2, 'y')");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::OpenEphemeral { .. })),
            0
        );
    }

    #[test]
    fn upsert_goes_through_hir() {
        for sql in [
            "INSERT INTO items VALUES (1, 'x') ON CONFLICT DO NOTHING",
            "INSERT INTO items(rowid, value) VALUES (1, 2) \
             ON CONFLICT DO UPDATE SET extra = excluded.extra || extra",
        ] {
            let program = emit(sql);
            assert_eq!(
                count(&program, |insn| matches!(insn, Insn::Insert { .. })),
                if sql.contains("DO UPDATE") { 2 } else { 1 },
                "{sql}"
            );
        }
    }

    #[test]
    fn insert_that_needs_legacy_is_left_to_it() {
        for sql in ["INSERT OR IGNORE INTO items VALUES (1, 'x') ON CONFLICT DO NOTHING"] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let insert = insert_root(&plan).unwrap();
            assert!(needs_legacy(&plan, insert, &program()).unwrap(), "{sql}");
        }
    }
}
