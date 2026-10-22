//! UPDATE lowering from resolved HIR.
//!
//! An UPDATE of a B-tree table runs one loop over the planned access path,
//! builds the new row in registers, checks NOT NULL and UNIQUE, then rewrites
//! the changed index entries and the row. RETURNING reads the new row. An
//! UPDATE that changes the rowid or the index its loop reads, or one with a
//! FROM clause, collects its rows first. An UPDATE of a virtual table hands
//! each row to the table with `VUpdate`.

use std::num::NonZeroUsize;

use turso_parser::ast::{ResolveType, TriggerTime};

use super::emit::{any_effective_replace, constraint_may_abort};
use crate::{
    error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE},
    schema::{Column, EXPR_INDEX_SENTINEL},
    translate::{
        optimizer::HirBtreeOperation,
        semantic::hir::{self, HirRoot},
        semantic_to_plan::{HirAccessPlan, HirPlan, HirRootPlan, HirSourceAccess},
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder, SourceBinding},
        insn::{to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral},
        BranchOffset, CursorID,
    },
    LimboError, Result,
};

use super::autoincrement::Sequence;
use super::dml::{
    collect_target_rowids, emit_access_predicates, emit_index_key, has_subquery, operation_index,
    touches_at_most_one_row, DmlTarget, ReturningBuffer, Subprograms,
};
use super::fkey_action::CompiledAction;
use super::query::{
    close_hir_btree_loop, hir_loop_explain, start_hir_btree_loop, PreparedHirBtreeLoop,
};
use super::trigger::{CompiledTrigger, TriggerRow};
use crate::translate::emitter::OperationMode;

/// Check an UPDATE before any bytecode is emitted: what the lowering expects
/// of its target and plan, and an error for each target it cannot write.
pub(super) fn check(plan: &HirPlan, update: &hir::Update, access: &HirAccessPlan) -> Result<()> {
    let document = &plan.document;
    // The parser rejects ORDER BY and LIMIT on an UPDATE.
    if !update.order_by.is_empty() || update.limit.is_some() {
        return Err(LimboError::InternalError(
            "HIR UPDATE with ORDER BY or LIMIT is not lowered".into(),
        ));
    }
    // Writes to a virtual table record no change data.
    if super::dml::virtual_target(document, update.target)?.is_some() {
        return Ok(());
    }
    let Some(source) = document.source(update.target) else {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE target {} is missing",
            update.target
        )));
    };
    // Analysis resolves the target to a table, which is a B-tree once
    // virtual tables are out of the way.
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(
            "HIR UPDATE target is not a table".into(),
        ));
    };
    let Some(table) = table.value().btree() else {
        return Err(LimboError::InternalError(
            "HIR UPDATE target has no B-tree".into(),
        ));
    };
    super::dml::check_strict_types(&table, source)?;
    if update.from.is_some() {
        // UPDATE ... FROM fills a scratch table during the join, so no loop
        // reads a table it changes. RETURNING reads only the updated row.
        let reads_from = update.returning.as_ref().is_some_and(|returning| {
            returning.outputs.iter().any(|output| {
                let mut other = false;
                output.expr.for_each(&mut |expression| match expression {
                    hir::Expr::Column(hir::ColumnRef { source, .. }) | hir::Expr::RowId(source) => {
                        other |= *source != update.target && *source != update.new_source;
                    }
                    _ => {}
                });
                other
            })
        });
        // Analysis rejects a RETURNING value that reads a FROM table.
        if reads_from {
            return Err(LimboError::InternalError(
                "HIR UPDATE ... FROM RETURNING reads a FROM table".into(),
            ));
        }
        return Ok(());
    }
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE plan has {} loops for one target",
            access.loops.len()
        )));
    };
    match &planned_loop.access {
        // The planner builds an automatic index only for a table joined
        // after another one, and without FROM the target is the only loop.
        HirSourceAccess::BTree { operation, .. } => {
            if operation_index(operation).is_some_and(|index| index.ephemeral) {
                return Err(LimboError::InternalError(
                    "HIR UPDATE target reads an automatic index".into(),
                ));
            }
            Ok(())
        }
        HirSourceAccess::OrUnion(_) | HirSourceAccess::IndexMethod(_) => Ok(()),
        _ => Err(LimboError::InternalError(
            "HIR UPDATE target is not read by a table loop".into(),
        )),
    }
}

/// Emit an UPDATE that [`check`] accepted. `update` is the plan's
/// root or one command of a trigger in it.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_update(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    update: &hir::Update,
    access: &HirAccessPlan,
    subprograms: &Subprograms<'_>,
    mvcc: bool,
    enforce_checks: bool,
    schema_change: Option<&str>,
) -> Result<()> {
    let triggers = &subprograms.triggers[..];
    let actions = &subprograms.actions[..];
    let replace_actions = &subprograms.replace_actions[..];
    let document = &plan.document;
    if let Some(table) = super::dml::virtual_target(document, update.target)? {
        return emit_virtual_update(program, plan, update, access, &table);
    }
    // UPDATE ... FROM joins the target with the FROM clause; otherwise the
    // target is the only loop.
    let single_loop = match &update.from {
        Some(_) => None,
        None => {
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
            // One seek per branch of an OR term reads rows in rowid order
            // through a row set, and an index method may stream its rows
            // lazily; the UPDATE then collects its rowids first.
            let operation = match &planned_loop.access {
                HirSourceAccess::BTree { operation, .. } => Some(operation),
                HirSourceAccess::OrUnion(_) | HirSourceAccess::IndexMethod(_) => None,
                _ => {
                    return Err(LimboError::InternalError(
                        "HIR UPDATE target does not use a B-tree loop".into(),
                    ))
                }
            };
            Some((planned_loop, operation))
        }
    };
    let hir::UpdateTargetKind::BTree {
        foreign_keys,
        defaults,
        autoincrement,
        capture_changes,
        ..
    } = &update.target_kind
    else {
        return Err(LimboError::InternalError(
            "HIR UPDATE target is not a B-tree table".into(),
        ));
    };
    let target = DmlTarget::new(program, document, update.target, "UPDATE")?
        .with_capture(program, capture_changes.as_ref())?;
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

    let assigned = with_generated_dependents(table, assigned_columns(update)?)?;
    let mut values = vec![None; table.columns().len()];
    // The new rowid, when the UPDATE sets it. The INTEGER PRIMARY KEY
    // column keeps NULL, as in the stored record.
    let mut rowid_value = None;
    for assignment in &update.assignments {
        let width = assignment.columns.len();
        for (part, target_column) in assignment.columns.iter().enumerate() {
            let value = match width {
                1 => NewValue::Expr(&assignment.value),
                _ => NewValue::RowPart {
                    row: &assignment.value,
                    part,
                    width,
                },
            };
            match target_column {
                column if is_rowid_target(table, column) => rowid_value = Some(value),
                hir::TargetColumn::Column(column) => values[*column] = Some(value),
                hir::TargetColumn::RowId => unreachable!("the rowid target was matched above"),
            }
        }
    }
    // Indexes whose key or WHERE term reads a changed column, in schema
    // order, with the NEW-row expressions for the same index.
    let mut changed_indexes = Vec::new();
    // Every index entry holds the rowid, so a new rowid changes them all.
    for old in &target.source.index_expressions {
        if rowid_value.is_none() && !index_reads_assigned_column(update.target, old, &assigned) {
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

    // Whether the statement needs a statement journal. A table with any
    // foreign key counts, even one this UPDATE leaves alone.
    let has_foreign_keys = foreign_keys.table_has_foreign_keys;
    let replaces = any_effective_replace(
        has_statement_conflict,
        or_conflict,
        table.rowid_alias_conflict_clause,
        changed_indexes
            .iter()
            .map(|(old, _)| old.index.value().on_conflict),
    );
    if triggers.is_empty()
        && !has_foreign_keys
        && !replaces
        && single_loop.is_some_and(|(_, operation)| operation.is_some_and(touches_at_most_one_row))
    {
        program.set_multi_write(false);
    }
    // Every column the UPDATE affects counts: a NOT NULL generated column
    // computed from a SET column can fail the statement too. A new rowid
    // must be an integer that no other row has, so setting the rowid or
    // the INTEGER PRIMARY KEY can fail like a UNIQUE constraint.
    let affected = table.columns_affected_by_update(assigned_positions(table, &assigned))?;
    let has_notnull = table
        .columns()
        .iter()
        .enumerate()
        .any(|(position, column)| {
            affected.get(position) && column.notnull() && !column.is_rowid_alias()
        });
    let has_unique = rowid_value.is_some()
        || !table.unique_sets.is_empty()
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
                !table.check_constraints.is_empty(),
                has_unique,
            ),
    );

    match single_loop {
        Some((planned_loop, _)) => program.emit_source_explain(
            target.source.id,
            hir_loop_explain(document, None, &[], planned_loop)?,
        ),
        None => {
            for planned_loop in &access.loops {
                program.emit_source_explain(
                    planned_loop.source,
                    hir_loop_explain(document, update.from.as_ref(), &[], planned_loop)?,
                );
            }
        }
    }
    let returning = ReturningBuffer::open(program, table, update.returning.as_ref());
    let row = UpdateRowSettings {
        or_conflict,
        has_statement_conflict,
    };

    let assigned = assigned_positions(table, &assigned);
    // Like SQLite, only the CHECK constraints that read a changed column
    // run. They read the new row.
    let checks = if enforce_checks {
        let constraints = new_source.check_constraints.as_deref().ok_or_else(|| {
            LimboError::InternalError("HIR UPDATE new row has no CHECK constraints".into())
        })?;
        constraints
            .iter()
            .filter(|constraint| {
                let mut reads_changed = false;
                constraint.expression.for_each(&mut |expression| {
                    reads_changed |= match expression {
                        hir::Expr::Column(hir::ColumnRef { source, column }) => {
                            *source == new_source.id && assigned.contains(column)
                        }
                        hir::Expr::RowId(source) => {
                            *source == new_source.id && rowid_value.is_some()
                        }
                        _ => false,
                    };
                });
                reads_changed
            })
            .collect()
    } else {
        Vec::new()
    };
    // UPDATE ... FROM computes the assigned values during the join, into a
    // scratch table with one column per assigned column, then one for the
    // new rowid when the UPDATE sets it.
    let scratch_columns = values
        .iter()
        .scan(0, |next, value| {
            Some(value.map(|_| {
                *next += 1;
                *next - 1
            }))
        })
        .collect::<Vec<_>>();
    let scratch = update
        .from
        .as_ref()
        .map(|_| -> Result<CursorID> {
            let width =
                scratch_columns.iter().flatten().count() + usize::from(rowid_value.is_some());
            let table = crate::sync::Arc::new(crate::schema::BTreeTable::new(
                0,
                "update_from_scratch".to_string(),
                Vec::new(),
                (0..width)
                    // BLOB affinity keeps the values as computed; the
                    // column's affinity is applied when the row is built.
                    .map(|column| {
                        crate::schema::Column::new(
                            Some(format!("value_{column}")),
                            "BLOB".to_string(),
                            None,
                            None,
                            crate::schema::Type::Blob,
                            None,
                            crate::schema::ColDef::default(),
                        )
                    })
                    .collect(),
                crate::schema::BTreeCharacteristics::HAS_ROWID,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
            ));
            Ok(program.alloc_cursor_id(CursorType::BTreeTable(table)))
        })
        .transpose()?;
    let sequence = match (autoincrement, rowid_value) {
        (Some(autoincrement), Some(_)) => {
            Some(Sequence::new(autoincrement, target.database, table, mvcc)?)
        }
        _ => None,
    };
    let row_layout = UpdateRowLayout {
        values: &values,
        scratch: scratch.map(|cursor| (cursor, scratch_columns.as_slice())),
        defaults,
        replaces,
        assigned: &assigned,
        changed_indexes: &changed_indexes,
        checks: &checks,
        rowid_value,
        new_source: new_source.id,
        foreign_keys,
        returning,
        triggers,
        mvcc,
        actions,
        replace_actions,
        sequence: sequence.as_ref(),
        // ALTER TABLE rewrites a schema row with a generated UPDATE; change
        // data capture records the user's DDL as the new `sql` value
        // instead.
        schema_change: schema_change.and_then(|ddl| {
            table
                .columns()
                .iter()
                .position(|column| column.name.as_deref() == Some("sql"))
                .filter(|position| assigned.contains(position))
                .map(|position| (position, ddl))
        }),
    };
    let done = program.allocate_label();
    // An UPDATE with triggers, OR REPLACE or a subquery in its WHERE
    // clause first collects its rowids, since a trigger or a REPLACE
    // can change the table, and the subquery must not see changed rows.
    let statement_replace = has_statement_conflict && or_conflict == ResolveType::Replace;
    let where_subquery = update.predicate.as_ref().is_some_and(has_subquery);
    // An UPDATE that moves rows, changes the key of the index its loop
    // reads, or may REPLACE rows while reading an index collects its rows
    // first, so the loop never reaches a changed row again.
    let moves_rows = rowid_value.is_some()
        || single_loop
            .and_then(|(_, operation)| operation_index(operation?))
            .is_some_and(|index| {
                replaces
                    || changed_indexes
                        .iter()
                        .any(|(old, _)| old.index.value().name == index.name)
            });
    match single_loop {
        None => emit_from_update(
            program,
            plan,
            update,
            &target,
            access,
            &row_layout,
            row,
            done,
        )?,
        Some((_, Some(operation)))
            if triggers.is_empty()
                && actions.is_empty()
                && !statement_replace
                && !where_subquery
                && !moves_rows =>
        {
            emit_scan_update(program, &target, access, operation, &row_layout, row, done)?
        }
        Some(_) => emit_rowset_update(program, plan, &target, access, &row_layout, row, done)?,
    }
    program.preassign_label_to_next_insn(done);
    target.emit_capture_commit(program)?;
    if let Some(returning) = returning {
        returning.emit_rows(program);
    }
    Ok(())
}

/// UPDATE of a virtual table: each row the loop finds is handed to the
/// table with `VUpdate`, as its old rowid, its new rowid and
/// every column, the columns it does not set read from the row.
fn emit_virtual_update(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    update: &hir::Update,
    access: &HirAccessPlan,
    table: &crate::sync::Arc<crate::vtab::VirtualTable>,
) -> Result<()> {
    if update.returning.is_some() {
        crate::bail_parse_error!("UPDATE RETURNING is not available on virtual tables");
    }
    if table.readonly() && !program.dbpage_writable() {
        return Err(LimboError::ReadOnly);
    }
    let document = &plan.document;
    let columns = table.columns.len();
    let mut values = vec![None; columns];
    let mut rowid_value = None;
    for assignment in &update.assignments {
        let width = assignment.columns.len();
        for (part, target_column) in assignment.columns.iter().enumerate() {
            let value = match width {
                1 => NewValue::Expr(&assignment.value),
                _ => NewValue::RowPart {
                    row: &assignment.value,
                    part,
                    width,
                },
            };
            match target_column {
                hir::TargetColumn::RowId => rowid_value = Some(value),
                hir::TargetColumn::Column(column) => values[*column] = Some(value),
            }
        }
    }
    let conflict_action = update
        .conflict
        .map_or(0, |conflict| conflict.bit_value() as u16);
    program.begin_write_operation()?;
    let cursor = super::dml::open_virtual_for_write(program, table);
    // UPDATE ... FROM: like SQLite, the join of the FROM clause and the
    // target writes each row's new values into a scratch table keyed by the
    // row's rowid (a row joined more than once keeps the last values), and
    // the table is updated from the scratch table afterwards, so that no
    // loop reads a table it changes.
    let scratch = update.from.as_ref().map(|_| {
        let table = crate::sync::Arc::new(crate::schema::BTreeTable::new(
            0,
            "update_from_scratch".to_string(),
            Vec::new(),
            (0..columns + 2)
                .map(|column| {
                    crate::schema::Column::new(
                        Some(format!("value_{column}")),
                        "BLOB".to_string(),
                        None,
                        None,
                        crate::schema::Type::Blob,
                        None,
                        crate::schema::ColDef::default(),
                    )
                })
                .collect(),
            crate::schema::BTreeCharacteristics::HAS_ROWID,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        ));
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: cursor,
            is_table: true,
        });
        cursor
    });
    super::query::emit_dml_join_loops(
        program,
        plan,
        update.from.as_ref(),
        super::query::HirLoopsPlan::of_access(access),
        &|program, next| {
            // The columns the UPDATE does not set are read from the loop's
            // cursor, as a B-tree UPDATE reads them from its table cursor.
            let Some(SourceBinding::Virtual {
                cursor: loop_cursor,
            }) = program.source_binding(update.target).copied()
            else {
                return Err(LimboError::InternalError(
                    "HIR virtual-table UPDATE target is not read by a virtual-table loop".into(),
                ));
            };
            let start = program.alloc_registers(columns + 2);
            super::expr::translate_expr(
                program,
                document,
                &hir::Expr::RowId(update.target),
                start,
            )?;
            // A row without a rowid is skipped.
            program.emit_insn(Insn::IsNull {
                reg: start,
                target_pc: next,
            });
            let mut rows = RowValues::default();
            match rowid_value {
                Some(value) => value.emit(
                    program,
                    document,
                    start + 1,
                    &mut rows,
                    super::expr::translate_expr_no_constant_opt,
                )?,
                None => program.emit_insn(Insn::Copy {
                    src_reg: start,
                    dst_reg: start + 1,
                    extra_amount: 0,
                }),
            }
            for (column, value) in values.iter().enumerate() {
                let register = start + 2 + column;
                match value {
                    Some(value) => value.emit(
                        program,
                        document,
                        register,
                        &mut rows,
                        super::expr::translate_expr_no_constant_opt,
                    )?,
                    None => program.emit_insn(Insn::VColumn {
                        cursor_id: loop_cursor,
                        column,
                        dest: register,
                    }),
                }
            }
            match scratch {
                Some(scratch) => {
                    let record = program.alloc_register();
                    program.emit_insn(Insn::MakeRecord {
                        start_reg: to_u32(start),
                        count: to_u32(columns + 2),
                        dest_reg: to_u32(record),
                        index_name: None,
                        affinity_str: None,
                    });
                    program.emit_insn(Insn::Insert {
                        cursor: scratch,
                        key_reg: start,
                        record_reg: record,
                        flag: InsertFlags::new()
                            .require_seek()
                            .is_ephemeral_table_insert(),
                        table_name: String::new(),
                    });
                }
                None => program.emit_insn(Insn::VUpdate {
                    cursor_id: cursor,
                    arg_count: columns + 2,
                    start_reg: start,
                    conflict_action,
                }),
            }
            Ok(())
        },
    )?;
    let Some(scratch) = scratch else {
        return Ok(());
    };
    let empty = program.allocate_label();
    let next = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: scratch,
        pc_if_empty: empty,
    });
    let start = program.alloc_registers(columns + 2);
    program.preassign_label_to_next_insn(next);
    for column in 0..columns + 2 {
        program.emit_insn(Insn::Column {
            cursor_id: scratch,
            column,
            dest: start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::VUpdate {
        cursor_id: cursor,
        arg_count: columns + 2,
        start_reg: start,
        conflict_action,
    });
    program.emit_insn(Insn::Next {
        cursor_id: scratch,
        is_index: program.cursor_is_index(scratch),
        pc_if_next: next,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(empty);
    program.emit_insn(Insn::Close { cursor_id: scratch });
    Ok(())
}

/// The new value of one column an UPDATE sets.
#[derive(Clone, Copy)]
enum NewValue<'a> {
    Expr(&'a hir::Expr),
    /// `SET (a, b) = (SELECT ...)`: value `part` of a row of `width` values.
    RowPart {
        row: &'a hir::Expr,
        part: usize,
        width: usize,
    },
}

/// The rows of `SET (a, b) = (SELECT ...)` already computed for the current
/// row, each in consecutive registers, so that each runs once.
#[derive(Default)]
struct RowValues<'a> {
    computed: Vec<(&'a hir::Expr, usize)>,
}

/// Lowers one HIR expression into a register.
type Translate = fn(&mut ProgramBuilder, &hir::HirDocument, &hir::Expr, usize) -> Result<usize>;

impl<'a> NewValue<'a> {
    fn emit(
        self,
        program: &mut ProgramBuilder,
        document: &hir::HirDocument,
        register: usize,
        rows: &mut RowValues<'a>,
        translate: Translate,
    ) -> Result<()> {
        match self {
            NewValue::Expr(value) => {
                translate(program, document, value, register)?;
            }
            NewValue::RowPart { row, part, width } => {
                let start = match rows
                    .computed
                    .iter()
                    .find(|(computed, _)| std::ptr::eq(*computed, row))
                {
                    Some((_, start)) => *start,
                    None => {
                        let start = program.alloc_registers(width);
                        super::expr::translate_expr_no_constant_opt(program, document, row, start)?;
                        rows.computed.push((row, start));
                        start
                    }
                };
                program.emit_insn(Insn::Copy {
                    src_reg: start + part,
                    dst_reg: register,
                    extra_amount: 0,
                });
            }
        }
        Ok(())
    }
}

/// What every row of an UPDATE computes, shared by both loops.
struct UpdateRowLayout<'a> {
    /// Each column's assigned value, if any.
    values: &'a [Option<NewValue<'a>>],
    /// UPDATE ... FROM: the scratch table the join filled, and for each
    /// assigned column the scratch column holding its value.
    scratch: Option<(CursorID, &'a [Option<usize>])>,
    /// What NOT NULL ON CONFLICT REPLACE stores instead of a NULL.
    defaults: &'a [hir::ResolvedDefault],
    /// Whether a conflict of the new row may resolve with REPLACE.
    replaces: bool,
    /// The positions of the columns the UPDATE sets.
    assigned: &'a [usize],
    /// The indexes the UPDATE changes, with their OLD- and NEW-row expressions.
    changed_indexes: &'a [(&'a hir::IndexExpressions, &'a hir::IndexExpressions)],
    /// The CHECK constraints the new row must pass.
    checks: &'a [&'a hir::CheckConstraint],
    /// The new rowid, when the UPDATE sets it.
    rowid_value: Option<NewValue<'a>>,
    new_source: hir::SourceId,
    foreign_keys: &'a hir::DmlForeignKeys,
    returning: Option<ReturningBuffer<'a>>,
    triggers: &'a [CompiledTrigger],
    mvcc: bool,
    /// The ON UPDATE actions to run once the row is written.
    actions: &'a [CompiledAction<'a>],
    /// For ALTER TABLE's schema-row UPDATE: the `sql` column, and the DDL
    /// change data capture records as its new value.
    schema_change: Option<(usize, &'a str)>,
    /// The ON DELETE actions of a row REPLACE deletes.
    replace_actions: &'a [CompiledAction<'a>],
    /// AUTOINCREMENT, when the UPDATE sets the rowid: the target's sequence.
    sequence: Option<&'a Sequence>,
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
    // Open the table, the index the loop reads, then each index the UPDATE
    // changes, all for writing.
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
    let index_cursors = open_changed_indexes(program, target, layout)?;

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
        index_keys: Vec::new(),
    };
    emit_access_predicates(program, document, access, None, done)?;
    let hir_loop = start_hir_btree_loop(program, document, &prepared, None)?;
    emit_access_predicates(program, document, access, Some(0), hir_loop.next)?;

    let row = UpdateRow {
        target,
        table_cursor,
        rowid: program.alloc_registers(target.table.columns().len() + 1),
        new_rowid: layout.rowid_value.map(|_| program.alloc_register()),
        or_conflict: settings.or_conflict,
        has_statement_conflict: settings.has_statement_conflict,
        defaults: layout.defaults,
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
/// each collected row. An UPDATE with triggers does this, since a trigger
/// can change the table.
fn emit_rowset_update(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    target: &DmlTarget<'_>,
    access: &HirAccessPlan,
    layout: &UpdateRowLayout<'_>,
    settings: UpdateRowSettings,
    done: BranchOffset,
) -> Result<()> {
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR UPDATE plan has {} loops for one target",
            access.loops.len()
        )));
    };
    let rowset = collect_target_rowids(program, plan, target, access, planned_loop)?;

    let table_cursor = target.open_table_for_write(program);
    let index_cursors = open_changed_indexes(program, target, layout)?;
    let next = program.allocate_label();
    let row = UpdateRow {
        target,
        table_cursor,
        rowid: program.alloc_registers(target.table.columns().len() + 1),
        new_rowid: layout.rowid_value.map(|_| program.alloc_register()),
        or_conflict: settings.or_conflict,
        has_statement_conflict: settings.has_statement_conflict,
        defaults: layout.defaults,
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

/// UPDATE ... FROM: run the join of the FROM clause and the target first,
/// writing the assigned values of each target row into a scratch table keyed
/// by its rowid (a target row joined more than once keeps the last values),
/// then update each row of the scratch table.
#[allow(clippy::too_many_arguments)]
fn emit_from_update(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    update: &hir::Update,
    target: &DmlTarget<'_>,
    access: &HirAccessPlan,
    layout: &UpdateRowLayout<'_>,
    settings: UpdateRowSettings,
    done: BranchOffset,
) -> Result<()> {
    let document = target.document;
    let (scratch, columns) = layout.scratch.ok_or_else(|| {
        LimboError::InternalError("HIR UPDATE ... FROM has no scratch table".into())
    })?;
    let width = columns.iter().flatten().count() + usize::from(layout.rowid_value.is_some());
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: scratch,
        is_table: true,
    });
    let assigned = layout
        .values
        .iter()
        .filter_map(|value| *value)
        .chain(layout.rowid_value)
        .collect::<Vec<_>>();
    let write = |program: &mut ProgramBuilder, _next: BranchOffset| -> Result<()> {
        let start = program.alloc_registers(width.max(1));
        let mut rows = RowValues::default();
        for (offset, value) in assigned.iter().enumerate() {
            value.emit(
                program,
                document,
                start + offset,
                &mut rows,
                super::expr::translate_expr_no_constant_opt,
            )?;
        }
        let rowid = program.alloc_register();
        super::expr::translate_expr(
            program,
            document,
            &hir::Expr::RowId(target.source.id),
            rowid,
        )?;
        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(start),
            count: to_u32(width),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: scratch,
            key_reg: rowid,
            record_reg: record,
            flag: InsertFlags::new()
                .require_seek()
                .is_ephemeral_table_insert(),
            table_name: String::new(),
        });
        Ok(())
    };
    super::query::emit_dml_join_loops(
        program,
        plan,
        update.from.as_ref(),
        super::query::HirLoopsPlan::of_access(access),
        &write,
    )?;

    let table_cursor = target.open_table_for_write(program);
    let index_cursors = open_changed_indexes(program, target, layout)?;
    let next = program.allocate_label();
    let row = UpdateRow {
        target,
        table_cursor,
        rowid: program.alloc_registers(target.table.columns().len() + 1),
        new_rowid: layout.rowid_value.map(|_| program.alloc_register()),
        or_conflict: settings.or_conflict,
        has_statement_conflict: settings.has_statement_conflict,
        defaults: layout.defaults,
        skip_row: next,
    };
    let empty = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: scratch,
        pc_if_empty: empty,
    });
    let top = program.allocate_label();
    program.preassign_label_to_next_insn(top);
    program.emit_insn(Insn::RowId {
        cursor_id: scratch,
        dest: row.rowid,
    });
    program.emit_insn(Insn::NotExists {
        cursor: table_cursor,
        rowid_reg: row.rowid,
        target_pc: next,
    });
    row.emit(program, layout, &index_cursors)?;
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: scratch,
        is_index: program.cursor_is_index(scratch),
        pc_if_next: top,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(empty);
    program.emit_insn(Insn::Close { cursor_id: scratch });
    program.emit_insn(Insn::Goto { target_pc: done });
    Ok(())
}

/// The index cursors an UPDATE writes through.
struct IndexCursors<'a> {
    /// One for each changed index, in the same order.
    changed: Vec<CursorID>,
    /// Every index, when a REPLACE may delete another row; empty otherwise.
    all: Vec<(&'a hir::IndexExpressions, CursorID)>,
}

/// Open the changed indexes for writing, or every index when a REPLACE may
/// delete another row.
fn open_changed_indexes<'a>(
    program: &mut ProgramBuilder,
    target: &'a DmlTarget<'_>,
    layout: &UpdateRowLayout<'_>,
) -> Result<IndexCursors<'a>> {
    if layout.replaces {
        let all = target.open_indexes_for_write(program, None)?;
        let changed = layout
            .changed_indexes
            .iter()
            .map(|(old, _)| {
                all.iter()
                    .find(|(expressions, _)| expressions.index == old.index)
                    .map(|(_, cursor)| *cursor)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR UPDATE did not open index {}",
                            old.index.value().name
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(IndexCursors { changed, all });
    }
    let changed = layout
        .changed_indexes
        .iter()
        .map(|(old, _)| {
            // An index method without a B-tree of its own (such as FTS)
            // gets its own kind of cursor.
            let cursor = program.alloc_cursor_index(None, &old.index.handle())?;
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(old.index.value().root_page),
                db: target.database,
            });
            Ok(cursor)
        })
        .collect::<Result<_>>()?;
    Ok(IndexCursors {
        changed,
        all: Vec::new(),
    })
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

/// Whether a conflict of the new row may resolve with REPLACE: the
/// statement's OR REPLACE, or ON CONFLICT REPLACE on an index.
fn may_replace(
    update: &hir::Update,
    source: &hir::Source,
    table: &crate::schema::BTreeTable,
    sets_rowid: bool,
) -> bool {
    match update.conflict {
        Some(conflict) => conflict == ResolveType::Replace,
        None => {
            (sets_rowid && table.rowid_alias_conflict_clause == Some(ResolveType::Replace))
                || source
                    .index_expressions
                    .iter()
                    .any(|index| index.index.value().on_conflict == Some(ResolveType::Replace))
        }
    }
}

/// Whether an assignment target is the rowid: `rowid` itself or the
/// INTEGER PRIMARY KEY column.
fn is_rowid_target(table: &crate::schema::BTreeTable, column: &hir::TargetColumn) -> bool {
    match column {
        hir::TargetColumn::RowId => true,
        hir::TargetColumn::Column(column) => table
            .columns()
            .get(*column)
            .is_some_and(Column::is_rowid_alias),
    }
}

/// The table positions of `assigned`. Setting `rowid` sets the INTEGER
/// PRIMARY KEY column, when the table has one.
fn assigned_positions(
    table: &crate::schema::BTreeTable,
    assigned: &[hir::TargetColumn],
) -> Vec<usize> {
    let mut positions = assigned
        .iter()
        .filter_map(|column| match column {
            hir::TargetColumn::Column(column) => Some(*column),
            hir::TargetColumn::RowId => table.columns().iter().position(Column::is_rowid_alias),
        })
        .collect::<Vec<_>>();
    positions.sort_unstable();
    positions.dedup();
    positions
}

/// `assigned` and the virtual generated columns that read one of them, which
/// change with them.
pub(super) fn with_generated_dependents(
    table: &crate::schema::BTreeTable,
    mut assigned: Vec<hir::TargetColumn>,
) -> Result<Vec<hir::TargetColumn>> {
    if !table.has_virtual_columns() {
        return Ok(assigned);
    }
    let affected = table.columns_affected_by_update(assigned_positions(table, &assigned))?;
    for (position, column) in table.columns().iter().enumerate() {
        let dependent = hir::TargetColumn::Column(position);
        if column.is_virtual_generated() && affected.get(position) && !assigned.contains(&dependent)
        {
            assigned.push(dependent);
        }
    }
    Ok(assigned)
}

/// Compile the ON DELETE actions of the rows REPLACE may delete.
pub(super) fn replace_actions<'a>(
    session: &super::HirSession<'_>,
    parent: &ProgramBuilder,
    plan: &HirPlan,
    update: &'a hir::Update,
) -> Result<Vec<CompiledAction<'a>>> {
    let hir::UpdateTargetKind::BTree { foreign_keys, .. } = &update.target_kind else {
        return Ok(Vec::new());
    };
    let source = plan.document.source(update.target).ok_or_else(|| {
        LimboError::InternalError(format!("HIR UPDATE target {} is missing", update.target))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return Ok(Vec::new());
    };
    let table = table.value().require_btree()?;
    let sets_rowid = assigned_columns(update)?
        .iter()
        .any(|column| is_rowid_target(&table, column));
    if !may_replace(update, source, &table, sets_rowid) {
        return Ok(Vec::new());
    }
    super::fkey_action::compile_actions(
        session,
        parent,
        foreign_keys,
        super::fkey_action::ParentChange::Delete,
        &[],
    )
}

/// The table positions an UPDATE changes: the columns it sets, the INTEGER
/// PRIMARY KEY when it sets the rowid, and the virtual columns that read
/// them.
pub(super) fn changed_positions(plan: &HirPlan, update: &hir::Update) -> Result<Vec<usize>> {
    let source = plan.document.source(update.target).ok_or_else(|| {
        LimboError::InternalError(format!("HIR UPDATE target {} is missing", update.target))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(
            "HIR UPDATE target is not a table".into(),
        ));
    };
    let table = table.value().require_btree()?;
    let assigned = with_generated_dependents(&table, assigned_columns(update)?)?;
    Ok(assigned_positions(&table, &assigned))
}

fn assigned_columns(update: &hir::Update) -> Result<Vec<hir::TargetColumn>> {
    Ok(update
        .assignments
        .iter()
        .flat_map(|assignment| assignment.columns.iter().copied())
        .collect())
}

/// Whether an index's key or WHERE term reads a column the UPDATE assigns.
pub(super) fn index_reads_assigned_column(
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
    /// The new rowid, when the UPDATE sets it.
    new_rowid: Option<usize>,
    or_conflict: ResolveType,
    has_statement_conflict: bool,
    /// What NOT NULL ON CONFLICT REPLACE stores instead of a NULL.
    defaults: &'a [hir::ResolvedDefault],
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

    /// The rowid the row is written with.
    fn written_rowid(&self) -> usize {
        self.new_rowid.unwrap_or(self.rowid)
    }

    /// Bind the NEW row to its registers and compute its virtual generated
    /// columns there. With `check_notnull`, a changed NOT NULL virtual
    /// column is checked too.
    fn emit_virtual_columns(
        &self,
        program: &mut ProgramBuilder,
        layout: &UpdateRowLayout<'_>,
        check_notnull: bool,
    ) -> Result<()> {
        let table = self.target.table;
        if table.has_virtual_columns() && super::dml::has_custom_types(self.target.source) {
            // The new row holds the values the UPDATE sets as SQL values and
            // the others as stored, so the virtual columns are computed from
            // a copy of it as SQL values.
            let row = self.emit_sql_values(program, layout)?;
            for (position, column) in table.columns().iter().enumerate() {
                if column.is_virtual_generated() {
                    program.emit_insn(Insn::Copy {
                        src_reg: row + position,
                        dst_reg: self.start() + position,
                        extra_amount: 0,
                    });
                }
            }
        }
        program.bind_source(
            layout.new_source,
            SourceBinding::Registers {
                start: self.start(),
                rowid: Some(self.written_rowid()),
            },
        );
        if !super::dml::has_custom_types(self.target.source) {
            super::dml::emit_virtual_columns(
                program,
                self.target.document,
                layout.new_source,
                table,
                self.start(),
            )?;
        }
        if check_notnull {
            for (position, column) in table.columns().iter().enumerate() {
                if column.is_virtual_generated()
                    && column.notnull()
                    && layout.assigned.contains(&position)
                {
                    self.emit_notnull_check(program, position, column)?;
                }
            }
        }
        Ok(())
    }

    /// A copy of the new row as SQL values, with its virtual columns
    /// computed: the values the UPDATE sets as they are, and the other
    /// columns, which hold stored values, decoded.
    fn emit_sql_values(
        &self,
        program: &mut ProgramBuilder,
        layout: &UpdateRowLayout<'_>,
    ) -> Result<usize> {
        let DmlTarget {
            document,
            source,
            table,
            ..
        } = *self.target;
        let columns = table.columns().len();
        let row = program.alloc_registers(columns);
        program.emit_insn(Insn::Copy {
            src_reg: self.start(),
            dst_reg: row,
            extra_amount: columns - 1,
        });
        for (position, value) in layout.values.iter().enumerate() {
            if value.is_some() {
                continue;
            }
            if let Some(programs) = source.column_type_programs[position]
                .as_ref()
                .filter(|programs| !programs.decode.is_empty())
            {
                let null = program.allocate_label();
                program.emit_insn(Insn::IsNull {
                    reg: row + position,
                    target_pc: null,
                });
                super::expr::translate_schema_calls_in_place(
                    program,
                    document,
                    &programs.decode,
                    row + position,
                )?;
                program.preassign_label_to_next_insn(null);
            }
        }
        program.bind_source(
            layout.new_source,
            SourceBinding::Registers {
                start: row,
                rowid: Some(self.written_rowid()),
            },
        );
        super::dml::with_stored_values(program, |program| {
            super::dml::emit_virtual_columns(program, document, layout.new_source, table, row)
        })?;
        Ok(row)
    }

    /// Rewrite the row the table cursor is on.
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        layout: &UpdateRowLayout<'_>,
        cursors: &IndexCursors<'_>,
    ) -> Result<()> {
        let index_cursors = &cursors.changed;
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
        // The old row is read from the table.
        program.bind_source(
            source.id,
            SourceBinding::BTree {
                scan_cursor: self.table_cursor,
                table_cursor: None,
            },
        );
        // Change data capture in `full` mode records which columns changed:
        // a flag per column, then the new value of each changed one.
        let updates = self
            .target
            .capture
            .as_ref()
            .filter(|_| super::cdc::Capture::records_updates(program))
            .map(|_| program.alloc_registers(2 * columns));
        // A BEFORE trigger runs before NOT NULL is checked.
        self.emit_new_values(program, layout, !has_before, updates)?;
        // STRICT: the values the UPDATE sets are checked, and converted
        // where SQLite allows, before the generated columns read them and
        // before any conflict is resolved. Custom-typed values are checked
        // when they are encoded.
        if table.is_strict && !super::dml::has_custom_types(source) {
            super::dml::emit_strict_type_check(program, table, self.start());
        }
        self.emit_virtual_columns(program, layout, !has_before)?;
        if let (Some(updates), Some((position, ddl))) = (updates, layout.schema_change) {
            program.emit_insn(Insn::String8 {
                value: ddl.to_string(),
                dest: updates + columns + position,
            });
        }
        if let Some(updates) = updates {
            // A changed virtual column records its value.
            for &position in layout.assigned {
                if table.columns()[position].is_virtual_generated() {
                    program.emit_insn(Insn::Copy {
                        src_reg: self.start() + position,
                        dst_reg: updates + columns + position,
                        extra_amount: 0,
                    });
                }
            }
        }
        let new_row = TriggerRow {
            columns: self.start(),
            rowid: self.written_rowid(),
        };
        // The OLD row the triggers read, as a query would read it.
        let old_row = (!layout.triggers.is_empty())
            .then(|| -> Result<TriggerRow> {
                let start = program.alloc_registers(columns);
                // The trigger program decodes custom-typed values itself, so it is
                // given them as stored.
                super::dml::with_stored_values(program, |program| {
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
                    Ok(())
                })?;
                Ok(TriggerRow {
                    columns: start,
                    rowid: self.rowid,
                })
            })
            .transpose()?;
        if has_before {
            // A BEFORE trigger reads NEW as SQL values.
            let new_row = if super::dml::has_custom_types(source) {
                TriggerRow {
                    columns: self.emit_sql_values(program, layout)?,
                    rowid: self.written_rowid(),
                }
            } else {
                new_row
            };
            for trigger in fires(TriggerTime::Before) {
                // RAISE(IGNORE) skips the rest of this row.
                trigger.emit_fire(program, old_row, Some(new_row), columns, self.skip_row)?;
            }
            // Like SQLite: the trigger may have deleted the row,
            // and the columns the UPDATE does not set are read again, since
            // the trigger may have changed them.
            program.emit_insn(Insn::NotExists {
                cursor: self.table_cursor,
                rowid_reg: self.rowid,
                target_pc: self.skip_row,
            });
            for (position, (column, value)) in table.columns().iter().zip(layout.values).enumerate()
            {
                if value.is_none() && !column.is_rowid_alias() && !column.is_virtual_generated() {
                    super::dml::emit_stored_column(
                        program,
                        self.table_cursor,
                        column,
                        position,
                        self.start() + position,
                    );
                }
            }
            for (position, (column, value)) in table.columns().iter().zip(layout.values).enumerate()
            {
                if value.is_some() && column.notnull() {
                    self.emit_notnull_check(program, position, column)?;
                }
            }
        }
        if has_before {
            // The trigger may have changed the columns they read.
            self.emit_virtual_columns(program, layout, true)?;
        }
        // A new rowid is checked first, unless REPLACE resolves its
        // conflict, which happens after the indexes are checked.
        if self.new_rowid.is_some() && self.rowid_conflict() != ResolveType::Replace {
            self.emit_rowid_check(program)?;
        }
        // A STRICT type check and then CHECK constraints run after NOT NULL
        // and before any index is checked.
        if super::dml::has_custom_types(source) {
            // Only the values the UPDATE sets are checked as input and
            // encoded; the others are read as stored.
            let set = layout
                .values
                .iter()
                .enumerate()
                .filter_map(|(position, value)| value.map(|_| position))
                .collect::<Vec<_>>();
            super::dml::emit_custom_type_writes(
                program,
                document,
                source,
                table,
                self.start(),
                Some(&set),
            )?;
        }
        super::dml::emit_strict_generated_check(program, table, self.start());
        super::dml::emit_check_constraints(
            program,
            document,
            layout.checks.iter().copied(),
            self.or_conflict,
            self.skip_row,
        )?;

        // Check every changed index before changing any of them:
        // build the new keys and check UNIQUE, then delete the old entries,
        // then insert the new ones.
        let mut new_keys = Vec::with_capacity(layout.changed_indexes.len());
        for ((old, new), cursor) in layout.changed_indexes.iter().zip(index_cursors) {
            new_keys.push(self.emit_new_index_key(program, layout, cursors, old, new, *cursor)?);
        }
        if let Some(new_rowid) = self.new_rowid {
            if self.rowid_conflict() == ResolveType::Replace {
                self.emit_rowid_replace(program, layout, cursors, new_rowid)?;
            }
        }
        // Foreign keys are checked after the other constraints and before
        // anything is rewritten.
        let pending_repairs = super::fkey::emit_update_checks(
            program,
            document,
            layout.foreign_keys,
            source.id,
            layout.new_source,
            self.rowid,
            self.written_rowid(),
            layout.assigned,
            self.target.database,
            layout.replaces,
        )?;
        // The parent key before the change, for the actions.
        let action_keys = layout
            .actions
            .iter()
            .map(|action| {
                super::fkey::emit_parent_key(program, document, action.foreign_key, source.id)
            })
            .collect::<Result<Vec<_>>>()?;
        // The row before the change, for change data capture.
        let capture_before = self.target.capture.as_ref().and_then(|capture| {
            capture.before_record(program, table, self.table_cursor, self.rowid)
        });
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

        let record = super::dml::emit_row_record(program, table, self.start());
        // MVCC keeps every version of a row, so the old one is deleted and
        // the new one inserted. A row with a new rowid moves the same way.
        if layout.mvcc || self.new_rowid.is_some() {
            program.emit_insn(Insn::Delete {
                cursor_id: self.table_cursor,
                table_name: table.name.clone(),
                is_part_of_update: true,
            });
        }
        let flag = match self.new_rowid {
            // The cursor is not on the new rowid's place in the table.
            Some(_) => InsertFlags::new()
                .require_seek()
                .update_rowid_change()
                .skip_last_rowid(),
            None => InsertFlags::new().skip_last_rowid(),
        };
        program.emit_insn(Insn::Insert {
            cursor: self.table_cursor,
            key_reg: self.written_rowid(),
            record_reg: record,
            flag,
            table_name: table.name.clone(),
        });
        super::fkey::emit_update_repairs(program, &pending_repairs, self.target.database)?;
        if let (Some(sequence), Some(_)) = (layout.sequence, self.new_rowid) {
            sequence.emit_moved_row(program, self.written_rowid())?;
        }
        // Like SQLite, the actions run once the row is written,
        // for a parent key that changed, before RETURNING reads the row.
        for (action, old_key) in layout.actions.iter().zip(action_keys) {
            let new_key = super::fkey::emit_parent_key(
                program,
                document,
                action.foreign_key,
                layout.new_source,
            )?;
            let unchanged = program.allocate_label();
            super::fkey::emit_skip_if_same_key(
                program,
                old_key,
                new_key,
                action.foreign_key.parent_positions.len(),
                unchanged,
            );
            action.emit_fire(program, old_key, Some(new_key))?;
            program.preassign_label_to_next_insn(unchanged);
        }
        if !layout.actions.is_empty() {
            // An action may have written this table and moved the cursor.
            let back = program.allocate_label();
            program.emit_insn(Insn::SeekRowid {
                cursor_id: self.table_cursor,
                src_reg: self.written_rowid(),
                target_pc: back,
            });
            program.preassign_label_to_next_insn(back);
        }
        // A moved row is captured as deleted and inserted.
        if let Some(capture) = &self.target.capture {
            let after = capture.after_record(program, table, self.start(), self.written_rowid());
            if self.new_rowid.is_some() {
                capture.emit_row(
                    program,
                    OperationMode::DELETE,
                    self.rowid,
                    capture_before,
                    None,
                    None,
                )?;
                capture.emit_row(
                    program,
                    OperationMode::INSERT,
                    self.written_rowid(),
                    None,
                    after,
                    None,
                )?;
            } else {
                let updates = updates.map(|updates| {
                    let record = program.alloc_register();
                    program.emit_insn(Insn::MakeRecord {
                        start_reg: to_u32(updates),
                        count: to_u32(2 * columns),
                        dest_reg: to_u32(record),
                        index_name: None,
                        affinity_str: None,
                    });
                    record
                });
                capture.emit_row(
                    program,
                    OperationMode::UPDATE(crate::translate::emitter::UpdateRowSource::Normal),
                    self.rowid,
                    capture_before,
                    after,
                    updates,
                )?;
            }
        }
        if let Some(returning) = layout.returning {
            let start = returning.emit_values(program, document, self.target.resolved_table()?)?;
            returning.emit_buffer(program, start);
        }
        // AFTER triggers fire after RETURNING has read the row,
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
        layout: &UpdateRowLayout<'_>,
        check_notnull: bool,
        updates: Option<usize>,
    ) -> Result<()> {
        let table = self.target.table;
        let mut rows = RowValues::default();
        if let (Some(value), Some(new_rowid)) = (layout.rowid_value, self.new_rowid) {
            match layout.scratch {
                // UPDATE ... FROM computed it during the join, after the
                // assigned columns.
                Some((cursor, columns)) => program.emit_insn(Insn::Column {
                    cursor_id: cursor,
                    column: columns.iter().flatten().count(),
                    dest: new_rowid,
                    default: None,
                }),
                None => value.emit(
                    program,
                    self.target.document,
                    new_rowid,
                    &mut rows,
                    super::expr::translate_expr,
                )?,
            }
            program.emit_insn(Insn::MustBeInt {
                reg: new_rowid,
                target_pc: None,
            });
        }
        for (position, (column, value)) in table.columns().iter().zip(layout.values).enumerate() {
            let register = self.start() + position;
            match value {
                Some(value) => {
                    match layout.scratch {
                        // UPDATE ... FROM computed the value during the join.
                        Some((cursor, columns)) => {
                            let scratch_column = columns[position].ok_or_else(|| {
                                LimboError::InternalError(format!(
                                    "HIR UPDATE ... FROM has no scratch column for column {position}"
                                ))
                            })?;
                            program.emit_insn(Insn::Column {
                                cursor_id: cursor,
                                column: scratch_column,
                                dest: register,
                                default: None,
                            });
                        }
                        None => {
                            value.emit(
                                program,
                                self.target.document,
                                register,
                                &mut rows,
                                super::expr::translate_expr,
                            )?;
                        }
                    }
                    if check_notnull && column.notnull() {
                        self.emit_notnull_check(program, position, column)?;
                    }
                }
                // A virtual column is computed once the row is built.
                None if column.is_rowid_alias() || column.is_virtual_generated() => {
                    program.emit_null(register, None)
                }
                None => super::dml::emit_stored_column(
                    program,
                    self.table_cursor,
                    column,
                    position,
                    register,
                ),
            }
        }
        // The changed columns record their new values before affinity is
        // applied; a changed INTEGER PRIMARY KEY records NULL.
        if let Some(updates) = updates {
            let columns = table.columns().len();
            for (position, value) in layout.values.iter().enumerate() {
                let changed = layout.assigned.contains(&position);
                program.emit_bool(changed, updates + position);
                match value {
                    Some(_) if changed => program.emit_insn(Insn::Copy {
                        src_reg: self.start() + position,
                        dst_reg: updates + columns + position,
                        extra_amount: 0,
                    }),
                    _ => program.emit_null(updates + columns + position, None),
                }
            }
        }
        // Apply column affinity to the new row before any index or
        // constraint reads it. A STRICT table is type checked later.
        let affinities = table
            .columns()
            .iter()
            .map(|column| column.affinity())
            .collect::<Vec<_>>();
        if !table.is_strict
            && affinities
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

    fn emit_notnull_check(
        &self,
        program: &mut ProgramBuilder,
        position: usize,
        column: &Column,
    ) -> Result<()> {
        let register = self.start() + position;
        let conflict = if self.has_statement_conflict {
            self.or_conflict
        } else {
            column.notnull_conflict_clause.unwrap_or(ResolveType::Abort)
        };
        // REPLACE stores the column's DEFAULT instead of a NULL. Without a
        // DEFAULT, or if it is NULL too, the statement aborts.
        if conflict == ResolveType::Replace {
            if let Some(default) = self
                .defaults
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
                program.preassign_label_to_next_insn(not_null);
            }
        }
        match conflict {
            ResolveType::Ignore => program.emit_insn(Insn::IsNull {
                reg: register,
                target_pc: self.skip_row,
            }),
            _ => program.emit_insn(Insn::HaltIfNull {
                target_reg: register,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                on_error: program.constraint_halt_on_error(conflict),
                description: format!(
                    "{}.{}",
                    self.target.table.name,
                    column.name.as_deref().unwrap_or_default()
                ),
            }),
        }
        Ok(())
    }

    /// Build the new key of a changed index and, for a UNIQUE index, check
    /// that no other row has it.
    fn emit_new_index_key(
        &self,
        program: &mut ProgramBuilder,
        layout: &UpdateRowLayout<'_>,
        cursors: &IndexCursors<'_>,
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
                Some(expression) if !super::dml::is_virtual_key_column(table, column) => {
                    super::expr::translate_expr_no_constant_opt(
                        program, document, expression, register,
                    )?
                }
                _ => {
                    let source = if table
                        .columns()
                        .get(column.pos_in_table)
                        .is_some_and(Column::is_rowid_alias)
                    {
                        self.written_rowid()
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
            src_reg: self.written_rowid(),
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
                            table.columns()[column.pos_in_table].affinity().aff_mask()
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
            self.emit_unique_check(
                program,
                layout,
                cursors,
                index,
                cursor,
                start,
                columns,
                new_in_index,
            )?;
        }
        Ok(NewIndexKey {
            start,
            count: columns + 1,
            record,
            old_in_index,
            new_in_index,
        })
    }

    /// How a conflict on the new rowid resolves.
    fn rowid_conflict(&self) -> ResolveType {
        if self.has_statement_conflict {
            self.or_conflict
        } else {
            self.target
                .table
                .rowid_alias_conflict_clause
                .unwrap_or(ResolveType::Abort)
        }
    }

    /// A new rowid must not belong to another row. This runs before the
    /// indexes are checked; then the cursor goes back to the row.
    fn emit_rowid_check(&self, program: &mut ProgramBuilder) -> Result<()> {
        let new_rowid = self.new_rowid.expect("the UPDATE sets the rowid");
        let free = program.allocate_label();
        program.emit_insn(Insn::Eq {
            lhs: new_rowid,
            rhs: self.rowid,
            target_pc: free,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        program.emit_insn(Insn::NotExists {
            cursor: self.table_cursor,
            rowid_reg: new_rowid,
            target_pc: free,
        });
        match self.rowid_conflict() {
            ResolveType::Ignore => program.emit_insn(Insn::Goto {
                target_pc: self.skip_row,
            }),
            conflict => {
                let table = self.target.table;
                let column = table
                    .columns()
                    .iter()
                    .find(|column| column.is_rowid_alias())
                    .and_then(|column| column.name.as_deref())
                    .unwrap_or("rowid");
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                    description: format!("{}.{column}", table.name),
                    on_error: program.constraint_halt_on_error(conflict),
                    description_reg: None,
                });
            }
        }
        program.preassign_label_to_next_insn(free);
        // NotExists moved the cursor.
        program.emit_insn(Insn::NotExists {
            cursor: self.table_cursor,
            rowid_reg: self.rowid,
            target_pc: self.skip_row,
        });
        Ok(())
    }

    /// REPLACE on a new rowid: after the indexes are checked, delete the
    /// row that has it, then go back to this row.
    fn emit_rowid_replace(
        &self,
        program: &mut ProgramBuilder,
        layout: &UpdateRowLayout<'_>,
        cursors: &IndexCursors<'_>,
        new_rowid: usize,
    ) -> Result<()> {
        let free = program.allocate_label();
        program.emit_insn(Insn::Eq {
            lhs: new_rowid,
            rhs: self.rowid,
            target_pc: free,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        program.emit_insn(Insn::NotExists {
            cursor: self.table_cursor,
            rowid_reg: new_rowid,
            target_pc: free,
        });
        super::dml::emit_replace_delete(
            program,
            self.target,
            self.table_cursor,
            &cursors.all,
            layout.foreign_keys,
            new_rowid,
            Some(layout.new_source),
            layout.replace_actions,
        )?;
        program.preassign_label_to_next_insn(free);
        program.emit_insn(Insn::NotExists {
            cursor: self.table_cursor,
            rowid_reg: self.rowid,
            target_pc: self.skip_row,
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_unique_check(
        &self,
        program: &mut ProgramBuilder,
        layout: &UpdateRowLayout<'_>,
        cursors: &IndexCursors<'_>,
        index: &crate::schema::Index,
        cursor: CursorID,
        start: usize,
        columns: usize,
        new_in_index: Option<usize>,
    ) -> Result<()> {
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
            ResolveType::Replace => {
                // Delete the other row, then go back to this one.
                super::dml::emit_replace_delete(
                    program,
                    self.target,
                    self.table_cursor,
                    &cursors.all,
                    layout.foreign_keys,
                    found,
                    Some(layout.new_source),
                    layout.replace_actions,
                )?;
                let back = program.allocate_label();
                program.emit_insn(Insn::SeekRowid {
                    cursor_id: self.table_cursor,
                    src_reg: self.rowid,
                    target_pc: back,
                });
                program.preassign_label_to_next_insn(back);
            }
            _ => {
                let table_name = &self.target.table.name;
                let columns = index
                    .columns
                    .iter()
                    .map(|column| format!("{table_name}.{}", column.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_UNIQUE,
                    description: columns,
                    on_error: program.constraint_halt_on_error(conflict),
                    description_reg: None,
                });
            }
        }
        program.preassign_label_to_next_insn(unique);
        Ok(())
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
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE checked(id INTEGER PRIMARY KEY, a CHECK (a > 0), b CHECK (b > 0))",
                    4,
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
        emit_with(schema(), sql)
    }

    fn emit_with(schema: Schema, sql: &str) -> ProgramBuilder {
        let plan = Arc::new(analyze_plan(schema, sql));
        let mut program = program();
        let (update, access) = update_root(&plan).unwrap();
        check(&plan, update, access).unwrap_or_else(|error| panic!("{sql}: {error}"));
        program.set_hir_plan(plan.clone());
        emit_update(
            &mut program,
            &plan,
            update,
            access,
            &Subprograms::default(),
            false,
            true,
            None,
        )
        .unwrap();
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
    fn update_checks_only_the_check_constraints_that_read_a_changed_column() {
        fn is_check_halt(insn: &Insn) -> bool {
            matches!(
                insn,
                Insn::Halt { err_code, .. } if *err_code == crate::error::SQLITE_CONSTRAINT_CHECK
            )
        }
        let program = emit("UPDATE checked SET a = 1");
        assert_eq!(count(&program, is_check_halt), 1);
        let program = emit("UPDATE checked SET a = 1, b = 2");
        assert_eq!(count(&program, is_check_halt), 2);
    }

    #[test]
    fn update_with_a_where_subquery_collects_its_rowids_first() {
        let program = emit("UPDATE items SET extra = 'y' WHERE value IN (SELECT value FROM items)");
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::RowSetAdd { .. })),
            1
        );
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::RowSetRead { .. })),
            1
        );
    }

    #[test]
    fn update_with_set_subqueries_goes_through_hir() {
        for sql in [
            "UPDATE items SET extra = (SELECT max(extra) FROM items)",
            "UPDATE items SET extra = (SELECT max(i.extra) FROM items AS i WHERE i.id < items.id)",
            "UPDATE items SET (value, extra) = (SELECT 1, 'x') WHERE id = 1",
        ] {
            // `emit` checks that HIR takes the statement.
            let program = emit(sql);
            assert!(
                count(&program, |insn| matches!(insn, Insn::Insert { .. })) >= 1,
                "{sql}"
            );
        }
    }

    #[test]
    fn returning_subquery_that_reads_the_target_runs_for_each_row() {
        let once = |sql| count(&emit(sql), |insn| matches!(insn, Insn::Once { .. }));
        assert_eq!(
            once("UPDATE items SET extra = 'y' RETURNING (SELECT count(*) FROM items)"),
            0
        );
        assert_eq!(once("UPDATE items SET extra = 'y' RETURNING (SELECT 1)"), 1);
    }

    #[test]
    fn update_of_the_rowid_moves_the_row() {
        for sql in [
            "UPDATE items SET id = id + 1",
            "UPDATE items SET rowid = 5 WHERE id = 1",
            // Like SQLite, the last of two rowid assignments wins.
            "UPDATE items SET id = 4, rowid = 5 WHERE id = 1",
        ] {
            let program = emit(sql);
            // The rows are collected first, and the row is deleted and
            // inserted again under its new rowid.
            assert_eq!(
                count(&program, |insn| matches!(insn, Insn::RowSetAdd { .. })),
                1,
                "{sql}"
            );
            assert_eq!(
                count(&program, |insn| matches!(
                    insn,
                    Insn::Delete {
                        is_part_of_update: true,
                        ..
                    }
                )),
                1,
                "{sql}"
            );
            // The index entry holds the rowid, so it changes too.
            assert_eq!(
                count(&program, |insn| matches!(insn, Insn::IdxInsert { .. })),
                1,
                "{sql}"
            );
        }
    }

    #[test]
    fn update_from_that_sets_the_rowid_reads_it_from_the_scratch_table() {
        let program = emit("UPDATE items SET id = o.id + 10 FROM items AS o WHERE o.id = items.id");
        let scratch = program
            .insns
            .iter()
            .find_map(|(insn, _)| match insn {
                Insn::OpenEphemeral { cursor_id, .. } => Some(*cursor_id),
                _ => None,
            })
            .unwrap();
        // The join stores the new rowid; the row is then moved with it.
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .unwrap()
        };
        let read = position(
            &|insn| matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == scratch),
        );
        assert!(read < position(&|insn| matches!(insn, Insn::MustBeInt { .. })));
        assert_eq!(
            count(&program, |insn| matches!(
                insn,
                Insn::Delete {
                    is_part_of_update: true,
                    ..
                }
            )),
            1
        );
    }

    #[test]
    fn update_or_replace_that_fires_foreign_key_actions_goes_through_hir() {
        let schema = || {
            let mut schema = schema();
            for (sql, root_page) in [
                ("CREATE TABLE parent(id INTEGER PRIMARY KEY, k UNIQUE)", 10),
                (
                    "CREATE TABLE child(x REFERENCES parent(k) ON DELETE SET NULL)",
                    11,
                ),
            ] {
                schema
                    .add_btree_table(Arc::new(BTreeTable::from_sql(sql, root_page).unwrap()))
                    .unwrap();
            }
            schema
        };
        for sql in [
            "UPDATE OR REPLACE parent SET k = 'a'",
            "UPDATE OR REPLACE parent SET id = 1",
        ] {
            let plan = analyze_plan(schema(), sql);
            let (update, access) = update_root(&plan).unwrap();
            check(&plan, update, access).unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
    }

    #[test]
    fn update_through_a_changing_index_collects_its_rows_first() {
        for sql in [
            "UPDATE OR REPLACE items SET value = 1",
            // The loop reads the index whose key changes.
            "UPDATE items INDEXED BY items_value SET value = value + 1 WHERE value > 1",
        ] {
            let program = emit(sql);
            assert_eq!(
                count(&program, |insn| matches!(insn, Insn::RowSetAdd { .. })),
                1,
                "{sql}"
            );
        }
    }

    #[test]
    fn update_of_an_array_element_rewrites_the_array() {
        let mut schema = schema();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE tagged(id INTEGER PRIMARY KEY, tags TEXT[], other TEXT[]) STRICT",
                    150,
                )
                .unwrap(),
            ))
            .unwrap();
        let program = emit_with(schema, "UPDATE tagged SET tags[1] = 'x'");
        assert_eq!(
            count(&program, |insn| matches!(
                insn,
                Insn::ArraySetElement { .. }
            )),
            1
        );
        // Only the array the UPDATE sets is encoded again.
        assert_eq!(
            count(&program, |insn| matches!(insn, Insn::ArrayEncode { .. })),
            1
        );
    }

    #[test]
    fn update_encodes_only_the_custom_typed_columns_it_sets() {
        let mut schema = schema();
        schema
            .add_type_from_sql(
                "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
            )
            .unwrap();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE priced(id INTEGER PRIMARY KEY, amount cents, other cents) STRICT",
                    140,
                )
                .unwrap(),
            ))
            .unwrap();
        let program = emit_with(schema, "UPDATE priced SET amount = 20");
        // The input check, the encoding, then the check of the stored type,
        // for `amount` only: `other` is written back as stored.
        let position = |matches: fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .enumerate()
                .filter(|(_, (insn, _))| matches(insn))
                .map(|(position, _)| position)
                .collect::<Vec<_>>()
        };
        let checks = position(|insn| matches!(insn, Insn::TypeCheck { .. }));
        let encodes = position(|insn| matches!(insn, Insn::Multiply { .. }));
        assert_eq!(checks.len(), 2);
        assert_eq!(encodes.len(), 1);
        assert!(checks[0] < encodes[0] && encodes[0] < checks[1]);
    }
}
