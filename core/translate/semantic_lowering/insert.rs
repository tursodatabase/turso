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

use crate::{
    error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE},
    schema::{Column, EXPR_INDEX_SENTINEL},
    translate::{
        insert::{format_unique_violation_desc, halt_desc_and_on_error},
        semantic::hir::{self, HirRoot},
        semantic_to_plan::HirPlan,
        stmt_journal::constraint_may_abort,
    },
    vdbe::{
        affinity::Affinity,
        builder::{ProgramBuilder, SourceBinding},
        insn::{to_u32, IdxInsertFlags, InsertFlags, Insn},
        BranchOffset, CursorID,
    },
    CaptureDataChangesExt, LimboError, Result,
};

use super::dml::{has_subquery, DmlTarget, ReturningBuffer};
use super::trigger::{CompiledTrigger, TriggerRow};

/// Whether this INSERT uses something the HIR lowering does not handle yet.
/// It is checked before any bytecode is emitted, so legacy can still take
/// the whole statement.
pub(super) fn needs_legacy(
    plan: &HirPlan,
    insert: &hir::Insert,
    foreign_keys_enabled: bool,
    program: &ProgramBuilder,
) -> Result<bool> {
    let document = &plan.document;
    // INSERT ... SELECT and subqueries.
    let values_have_subquery = match &insert.source {
        hir::InsertSource::Query(_) => true,
        hir::InsertSource::DefaultValues => false,
        hir::InsertSource::Values(rows) => rows.iter().flatten().any(has_subquery),
    };
    if values_have_subquery
        || insert.returning.as_ref().is_some_and(|returning| {
            returning
                .outputs
                .iter()
                .any(|output| has_subquery(&output.expr))
        })
    {
        return Ok(true);
    }
    if !insert.upserts.is_empty()
        || insert.excluded_source.is_some()
        || matches!(insert.conflict, Some(ResolveType::Replace))
    {
        return Ok(true);
    }
    let defaults = match &insert.target_kind {
        hir::InsertTargetKind::BTree {
            autoincrement,
            defaults,
            triggers,
            foreign_keys,
        } => {
            if autoincrement.is_some() || !triggers.upsert_update.is_empty() {
                return Ok(true);
            }
            if foreign_keys_enabled
                && (!foreign_keys.outgoing.is_empty() || !foreign_keys.incoming.is_empty())
            {
                return Ok(true);
            }
            defaults
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
    // WITHOUT ROWID tables, STRICT type checks, custom types, generated
    // columns, and CHECK constraints are not lowered yet.
    if !table.has_rowid
        || table.is_strict
        || !table.check_constraints.is_empty()
        || table.columns().iter().any(Column::is_generated)
        || table.columns().iter().any(Column::hidden)
        || source.column_type_programs.iter().any(Option::is_some)
    {
        return Ok(true);
    }
    // REPLACE deletes the conflicting row, or stores a column's default.
    if matches!(
        table.rowid_alias_conflict_clause,
        Some(ResolveType::Replace)
    ) || table
        .columns()
        .iter()
        .any(|column| matches!(column.notnull_conflict_clause, Some(ResolveType::Replace)))
        || source
            .index_expressions
            .iter()
            .any(|index| matches!(index.index.value().on_conflict, Some(ResolveType::Replace)))
    {
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
/// root or one command of a trigger in it.
pub(super) fn emit_insert(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    insert: &hir::Insert,
    triggers: &[CompiledTrigger],
) -> Result<()> {
    let document = &plan.document;
    let hir::InsertTargetKind::BTree { defaults, .. } = &insert.target_kind else {
        return Err(LimboError::InternalError(
            "HIR INSERT target is not a B-tree table".into(),
        ));
    };
    let rows: &[Vec<hir::Expr>] = match &insert.source {
        hir::InsertSource::DefaultValues => &[Vec::new()],
        hir::InsertSource::Values(rows) => rows,
        hir::InsertSource::Query(_) => {
            return Err(LimboError::InternalError(
                "HIR INSERT ... SELECT is not lowered".into(),
            ));
        }
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
    let several_rows = rows.len() > 1;
    if !several_rows && triggers.is_empty() {
        program.set_multi_write(false);
    }
    let indexes = &target.source.index_expressions;
    program.set_may_abort(
        !triggers.is_empty()
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
                false,
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
    };
    let done = program.allocate_label();
    match rows {
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
                &column_values,
                rowid_value,
                RowValues::Registers(values),
                &index_cursors,
                returning,
                next,
            )?;
            program.emit_insn(Insn::Goto { target_pc: next });
        }
    }
    program.preassign_label_to_next_insn(done);
    if let Some(returning) = returning {
        returning.emit_rows(program);
    }
    Ok(())
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

        for (position, column) in table.columns().iter().enumerate() {
            if column.notnull() && !column.is_rowid_alias() {
                self.emit_notnull_check(program, column, self.columns + position, row_done);
            }
        }
        // Index expressions, partial-index WHERE terms and RETURNING read
        // the new row.
        program.bind_source(
            self.target.source.id,
            SourceBinding::Registers {
                start: self.columns,
                rowid: Some(self.rowid),
            },
        );
        if rowid_value.is_some() {
            self.emit_rowid_check(program, row_done);
        }
        // Check every index before writing any of them.
        let mut keys = Vec::with_capacity(index_cursors.len());
        for (index, cursor) in index_cursors {
            keys.push(self.emit_index_key(program, index, *cursor, row_done)?);
        }

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
        for ((index, cursor), key) in index_cursors.iter().zip(&keys) {
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
        program.emit_insn(Insn::Insert {
            cursor: self.table_cursor,
            key_reg: self.rowid,
            record_reg: record,
            flag: InsertFlags::new(),
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
        if let Some(returning) = returning {
            let start = returning.emit_values(program, document)?;
            returning.emit_buffer(program, start);
        }
        Ok(())
    }

    fn conflict(&self, declared: Option<ResolveType>) -> ResolveType {
        if self.has_statement_conflict {
            self.or_conflict
        } else {
            declared.unwrap_or(ResolveType::Abort)
        }
    }

    fn emit_notnull_check(
        &self,
        program: &mut ProgramBuilder,
        column: &Column,
        register: usize,
        row_done: BranchOffset,
    ) {
        match self.conflict(column.notnull_conflict_clause) {
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
    }

    /// An explicit rowid must not belong to another row.
    fn emit_rowid_check(&self, program: &mut ProgramBuilder, row_done: BranchOffset) {
        let table = self.target.table;
        let unique = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: self.table_cursor,
            rowid_reg: self.rowid,
            target_pc: unique,
        });
        match self.conflict(table.rowid_alias_conflict_clause) {
            ResolveType::Ignore => program.emit_insn(Insn::Goto {
                target_pc: row_done,
            }),
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
    }

    /// Build an index's key for the new row and, for a UNIQUE index, check
    /// that no row has it yet.
    fn emit_index_key(
        &self,
        program: &mut ProgramBuilder,
        expressions: &hir::IndexExpressions,
        cursor: CursorID,
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
            match self.conflict(index.on_conflict) {
                ResolveType::Ignore => program.emit_insn(Insn::Goto {
                    target_pc: row_done,
                }),
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
        Ok(IndexKey {
            start,
            count: columns + 1,
            in_index,
        })
    }
}

/// A new index key built before any index is written.
struct IndexKey {
    start: usize,
    count: usize,
    /// Partial index: whether the new row is in the index.
    in_index: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sync::Arc,
        translate::semantic_lowering::query::tests::{analyze_plan, indexed_items_schema, program},
    };

    fn emit(sql: &str) -> ProgramBuilder {
        let plan = Arc::new(analyze_plan(indexed_items_schema(), sql));
        let mut program = program();
        let insert = insert_root(&plan).unwrap();
        assert!(
            !needs_legacy(&plan, insert, true, &program).unwrap(),
            "{sql}"
        );
        program.set_hir_plan(plan.clone());
        emit_insert(&mut program, &plan, insert, &[]).unwrap();
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
    fn insert_that_needs_legacy_is_left_to_it() {
        for sql in [
            "INSERT INTO items SELECT value, extra FROM items",
            "INSERT OR REPLACE INTO items VALUES (1, 'x')",
            "INSERT INTO items VALUES (1, 'x') ON CONFLICT DO NOTHING",
            "INSERT INTO items VALUES ((SELECT 1), 'x')",
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let insert = insert_root(&plan).unwrap();
            assert!(
                needs_legacy(&plan, insert, false, &program()).unwrap(),
                "{sql}"
            );
        }
    }
}
