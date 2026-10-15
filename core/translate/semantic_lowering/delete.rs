//! DELETE lowering from resolved HIR.
//!
//! This covers a plain DELETE from a B-tree table: one loop over the planned
//! access path, index entries removed with `IdxDelete`, then the row removed
//! with `Delete`. RETURNING rows are buffered and returned after the last
//! row is deleted. Statements that need triggers, foreign keys, subqueries,
//! CDC, or a virtual table still go through legacy; see [`needs_legacy`].

use turso_parser::ast;

use crate::{
    schema::{BTreeTable, Table, EXPR_INDEX_SENTINEL},
    sync::Arc,
    translate::{
        expr::{emit_returning_scan_back, ConditionMetadata, ReturningBufferCtx},
        optimizer::HirBtreeOperation,
        semantic::hir::{self, HirDocument, HirRoot},
        semantic_to_plan::{HirPlan, HirRootPlan, HirSourceAccess},
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder, SourceBinding},
        insn::{to_u32, InsertFlags, Insn, RegisterOrLiteral},
        CursorID,
    },
    CaptureDataChangesExt, LimboError, Result,
};

use super::query::{
    close_hir_btree_loop, emit_array_results, hir_loop_explain, predicate_loop,
    start_hir_btree_loop, PreparedHirBtreeLoop,
};

/// Whether this DELETE uses something the HIR lowering does not handle yet.
/// It is checked before any bytecode is emitted, so legacy can still take
/// the whole statement.
pub(super) fn needs_legacy(
    plan: &HirPlan,
    foreign_keys_enabled: bool,
    program: &ProgramBuilder,
) -> Result<bool> {
    let document = &plan.document;
    let HirRoot::Delete(delete) = &document.root else {
        return Err(LimboError::InternalError(
            "HIR DELETE analysis did not produce a DELETE root".into(),
        ));
    };
    let HirRootPlan::Delete(access) = &plan.root else {
        return Err(LimboError::InternalError(
            "HIR DELETE plan has no DELETE access plan".into(),
        ));
    };
    if !delete.order_by.is_empty() || delete.limit.is_some() {
        return Ok(true);
    }
    // Subqueries and CTEs: legacy collects the rowids first when the WHERE
    // clause has a subquery, and a RETURNING subquery that reads the target
    // must see the table after the row is deleted.
    if !document.queries.is_empty() {
        return Ok(true);
    }
    match &delete.target_kind {
        hir::DeleteTargetKind::BTree {
            triggers,
            foreign_keys,
        } => {
            if !triggers.is_empty() {
                return Ok(true);
            }
            if foreign_keys_enabled
                && (!foreign_keys.outgoing.is_empty() || !foreign_keys.incoming.is_empty())
            {
                return Ok(true);
            }
        }
        hir::DeleteTargetKind::Virtual => return Ok(true),
    }
    if program.capture_data_changes_info().table().is_some() {
        return Ok(true);
    }
    let [planned_loop] = access.loops.as_slice() else {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE plan has {} loops for one target",
            access.loops.len()
        )));
    };
    match &planned_loop.access {
        HirSourceAccess::BTree { operation, .. } => {
            Ok(hir_operation_index(operation).is_some_and(|index| index.ephemeral))
        }
        _ => Ok(true),
    }
}

/// Emit a DELETE that [`needs_legacy`] accepted.
pub(super) fn emit_delete(program: &mut ProgramBuilder, plan: &HirPlan) -> Result<()> {
    let document = &plan.document;
    let HirRoot::Delete(delete) = &document.root else {
        return Err(LimboError::InternalError(
            "HIR DELETE analysis did not produce a DELETE root".into(),
        ));
    };
    let HirRootPlan::Delete(access) = &plan.root else {
        return Err(LimboError::InternalError(
            "HIR DELETE plan has no DELETE access plan".into(),
        ));
    };
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
    let source = document.source(delete.target).ok_or_else(|| {
        LimboError::InternalError(format!("HIR DELETE target {} is missing", delete.target))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE target {} is not a table",
            source.id
        )));
    };
    let Table::BTree(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE target {} is not a B-tree table",
            source.id
        )));
    };
    if !matches!(source.index_coverage, hir::IndexCoverage::Complete { .. }) {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE target {} does not list all of its indexes",
            source.id
        )));
    }
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!("HIR DELETE target {} has no database", source.id))
    })?;
    let schema_version = document
        .database(database)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR DELETE target {} references missing database {database:?}",
                source.id
            ))
        })?
        .schema_version;
    program.begin_write_on_database(database.index(), schema_version)?;
    // Same statement-journal facts as legacy: without triggers or foreign
    // keys a DELETE cannot abort, and a point lookup writes at most one row.
    if hir_delete_touches_at_most_one_row(operation) {
        program.set_multi_write(false);
    }
    program.set_may_abort(false);

    program.emit_source_explain(source.id, hir_loop_explain(document, None, planned_loop)?);

    // Like legacy, RETURNING rows wait in an ephemeral table until every row
    // is deleted.
    let returning = delete.returning.as_ref().map(|returning| {
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: cursor,
            is_table: true,
        });
        (cursor, returning.outputs.as_slice())
    });

    // Like legacy, open the table, then the index the loop reads, then every
    // other index, all for writing.
    let iteration_index = hir_operation_index(operation);
    let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: table_cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: database.index(),
    });
    let iteration_cursor = iteration_index.map(|index| {
        let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: RegisterOrLiteral::Literal(index.root_page),
            db: database.index(),
        });
        cursor
    });
    let mut iteration_entry = None;
    let mut other_indexes = Vec::with_capacity(source.index_expressions.len());
    for expressions in &source.index_expressions {
        let index = expressions.index.value();
        if expressions.columns.len() != index.columns.len() {
            return Err(LimboError::InternalError(format!(
                "HIR index {} has {} key expressions for {} columns",
                index.name,
                expressions.columns.len(),
                index.columns.len()
            )));
        }
        if iteration_index.is_some_and(|iteration| iteration.name == index.name) {
            iteration_entry = Some(expressions);
            continue;
        }
        let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(expressions.index.handle()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: RegisterOrLiteral::Literal(index.root_page),
            db: database.index(),
        });
        other_indexes.push((expressions, cursor));
    }
    if iteration_index.is_some() && iteration_entry.is_none() {
        return Err(LimboError::InternalError(format!(
            "HIR DELETE target {} does not list the index its loop reads",
            source.id
        )));
    }

    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: iteration_cursor.unwrap_or(table_cursor),
            table_cursor: iteration_cursor.map(|_| table_cursor),
        },
    );
    let prepared = PreparedHirBtreeLoop {
        source,
        operation,
        index: iteration_index,
        cursor: iteration_cursor.unwrap_or(table_cursor),
        table_cursor: iteration_cursor.map(|_| table_cursor),
        autoindex_source_cursor: None,
        table_has_rowid: table.has_rowid,
    };

    let done = program.allocate_label();
    emit_delete_predicates(program, document, access, None, done)?;
    let hir_loop = start_hir_btree_loop(program, document, &prepared, None)?;
    emit_delete_predicates(program, document, access, Some(0), hir_loop.next)?;

    // RowId on the table cursor also finishes a deferred seek from the
    // index, so the table cursor sits on the row before it is deleted.
    let rowid = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: table_cursor,
        dest: rowid,
    });
    // Index keys are read from the table row, as legacy does.
    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: table_cursor,
            table_cursor: None,
        },
    );
    // The key of the index the loop reads is taken before the row is
    // deleted and removed after it.
    let iteration_key = match (iteration_entry, iteration_cursor) {
        (Some(expressions), Some(cursor)) => {
            let (start, count) =
                emit_index_key(program, document, source, table, table_cursor, expressions)?;
            Some((cursor, start, count, expressions))
        }
        _ => None,
    };
    // RETURNING reads the row before it is deleted. Without subqueries this
    // gives the same values as legacy, which reads a copy of the row after.
    let returning_row = returning
        .map(|(_, outputs)| -> Result<(usize, usize)> {
            let start = program.alloc_registers(outputs.len());
            for (position, output) in outputs.iter().enumerate() {
                super::expr::translate_expr_no_constant_opt(
                    program,
                    document,
                    &output.expr,
                    start + position,
                )?;
            }
            emit_array_results(program, document, outputs, start);
            Ok((start, outputs.len()))
        })
        .transpose()?;
    for (expressions, cursor) in &other_indexes {
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
    program.emit_insn(Insn::Delete {
        cursor_id: table_cursor,
        table_name: table.name.clone(),
        is_part_of_update: false,
    });
    if let (Some((cursor, _)), Some((start, count))) = (returning, returning_row) {
        let record = program.alloc_register();
        let key = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(start),
            count: to_u32(count),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::NewRowid {
            cursor,
            rowid_reg: key,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Insert {
            cursor,
            key_reg: key,
            record_reg: record,
            flag: InsertFlags::new().is_ephemeral_table_insert(),
            table_name: String::new(),
        });
    }
    if let Some((cursor, start, count, expressions)) = iteration_key {
        program.emit_insn(Insn::IdxDelete {
            start_reg: start,
            num_regs: count,
            cursor_id: cursor,
            raise_error_if_no_matching_entry: expressions.index.value().where_clause.is_none(),
        });
    }
    close_hir_btree_loop(program, &hir_loop, &[]);
    program.preassign_label_to_next_insn(done);
    if let Some((cursor, outputs)) = returning {
        // Legacy checks foreign keys before returning any row.
        program.emit_insn(Insn::FkCheck { deferred: false });
        emit_returning_scan_back(
            program,
            &ReturningBufferCtx {
                cursor_id: cursor,
                num_columns: outputs.len(),
            },
        );
    }
    Ok(())
}

/// Emit the WHERE terms that become checkable at `loop_index`: `None` is
/// before the loop, for terms that read no column of the target.
fn emit_delete_predicates(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    access: &crate::translate::semantic_to_plan::HirAccessPlan,
    loop_index: Option<usize>,
    fail: crate::vdbe::BranchOffset,
) -> Result<()> {
    for predicate in access
        .predicates
        .iter()
        .filter(|predicate| !predicate.consumed)
    {
        if predicate.from_outer_join.is_some() {
            return Err(LimboError::InternalError(
                "HIR DELETE predicate belongs to an outer join".into(),
            ));
        }
        if predicate_loop(program, document, &predicate.expr, &access.loops)? != loop_index {
            continue;
        }
        let passed = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            &predicate.expr,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: passed,
                jump_target_when_false: fail,
                jump_target_when_null: fail,
            },
        )?;
        program.preassign_label_to_next_insn(passed);
    }
    Ok(())
}

/// Read one index's key for the current row, followed by the rowid. Values
/// come from the table row, like legacy's old-image reads: plain columns are
/// read as stored, and generated columns and expressions are computed.
fn emit_index_key(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    source: &hir::Source,
    table: &BTreeTable,
    table_cursor: CursorID,
    expressions: &hir::IndexExpressions,
) -> Result<(usize, usize)> {
    let index = expressions.index.value();
    let count = index.columns.len() + 1;
    let start = program.alloc_registers(count);
    for (offset, (column, expression)) in index.columns.iter().zip(&expressions.columns).enumerate()
    {
        let register = start + offset;
        let table_column = (column.pos_in_table != EXPR_INDEX_SENTINEL)
            .then(|| table.columns().get(column.pos_in_table))
            .flatten();
        match (expression, table_column) {
            (Some(expression), _) => {
                super::expr::translate_expr_no_constant_opt(
                    program, document, expression, register,
                )?;
                // An indexed virtual generated column keeps its declared
                // affinity.
                if let Some(table_column) =
                    table_column.filter(|table_column| table_column.is_virtual_generated())
                {
                    program.emit_column_affinity(register, table_column.affinity());
                }
            }
            (None, Some(table_column)) if table_column.is_virtual_generated() => {
                super::expr::translate_expr_no_constant_opt(
                    program,
                    document,
                    &hir::Expr::Column(hir::ColumnRef {
                        source: source.id,
                        column: column.pos_in_table,
                    }),
                    register,
                )?;
            }
            (None, Some(_)) => {
                program.emit_column_or_rowid(table_cursor, column.pos_in_table, register)
            }
            (None, None) => {
                return Err(LimboError::InternalError(format!(
                    "HIR index {} has an expression column without an expression",
                    index.name
                )));
            }
        }
    }
    program.emit_insn(Insn::RowId {
        cursor_id: table_cursor,
        dest: start + count - 1,
    });
    Ok((start, count))
}

fn hir_operation_index(operation: &HirBtreeOperation) -> Option<&Arc<crate::schema::Index>> {
    match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_ref(),
        HirBtreeOperation::RowidEq { .. } => None,
    }
}

/// Legacy's single-row test for a DELETE access path: a rowid lookup, or a
/// seek where every column of a unique index (or the rowid) is fixed by `=`.
fn hir_delete_touches_at_most_one_row(operation: &HirBtreeOperation) -> bool {
    use crate::translate::plan::SeekKeyComponent;
    match operation {
        HirBtreeOperation::RowidEq { .. } => true,
        HirBtreeOperation::Seek {
            index: None,
            seek_def,
        } => {
            seek_def.prefix.len() == 1
                && seek_def.prefix[0].eq.is_some()
                && matches!(seek_def.start.last_component, SeekKeyComponent::None)
        }
        HirBtreeOperation::Seek {
            index: Some(index),
            seek_def,
        } => {
            index.unique
                && seek_def
                    .prefix
                    .iter()
                    .filter(|component| component.eq.is_some())
                    .count()
                    == index.columns.len()
                // `x IS NULL` can match many NULL keys of a UNIQUE index.
                && seek_def.prefix.iter().all(|component| {
                    component.eq.as_ref().is_none_or(|(operator, expression, _)| {
                        *operator != ast::Operator::Is || is_non_null_literal(expression)
                    })
                })
        }
        HirBtreeOperation::Scan { .. } | HirBtreeOperation::InSeek { .. } => false,
    }
}

fn is_non_null_literal(expression: &hir::Expr) -> bool {
    matches!(
        expression,
        hir::Expr::Literal(
            ast::Literal::Numeric(_)
                | ast::Literal::String(_)
                | ast::Literal::Blob(_)
                | ast::Literal::True
                | ast::Literal::False
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{BTreeTable, Schema},
        translate::semantic_lowering::query::tests::{analyze_plan, indexed_items_schema, program},
    };

    #[test]
    fn plain_delete_removes_the_row_and_every_index_entry() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "DELETE FROM items WHERE extra = 'x'",
        );
        let mut program = program();
        assert!(!needs_legacy(&plan, true, &program).unwrap());
        emit_delete(&mut program, &plan).unwrap();
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
        let mut program = program();
        assert!(!needs_legacy(&plan, false, &program).unwrap());
        emit_delete(&mut program, &plan).unwrap();
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
    fn delete_with_a_subquery_stays_on_legacy() {
        for sql in [
            "DELETE FROM items WHERE value = 1 RETURNING (SELECT count(*) FROM items)",
            "DELETE FROM items WHERE value IN (SELECT value FROM items)",
            "DELETE FROM items WHERE value = (SELECT 1)",
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            assert!(needs_legacy(&plan, false, &program()).unwrap(), "{sql}");
        }
    }

    #[test]
    fn delete_with_foreign_keys_stays_on_legacy_only_when_they_are_enforced() {
        let schema = || {
            let mut schema = Schema::new();
            for (sql, root_page) in [
                ("CREATE TABLE parent(id INTEGER PRIMARY KEY)", 2),
                ("CREATE TABLE child(x INTEGER REFERENCES parent(id))", 3),
            ] {
                schema
                    .add_btree_table(Arc::new(BTreeTable::from_sql(sql, root_page).unwrap()))
                    .unwrap();
            }
            schema
        };
        for sql in ["DELETE FROM parent", "DELETE FROM child"] {
            let plan = analyze_plan(schema(), sql);
            assert!(needs_legacy(&plan, true, &program()).unwrap(), "{sql}");
            assert!(!needs_legacy(&plan, false, &program()).unwrap(), "{sql}");
        }
    }
}
