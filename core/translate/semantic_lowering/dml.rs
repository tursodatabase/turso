//! Lowering shared by HIR DELETE and UPDATE.

use turso_parser::ast;

use crate::{
    schema::{BTreeTable, Table, EXPR_INDEX_SENTINEL},
    sync::Arc,
    translate::{
        expr::{emit_returning_scan_back, ConditionMetadata, ReturningBufferCtx},
        optimizer::HirBtreeOperation,
        semantic::hir::{self, HirDocument, SourceId},
        semantic_to_plan::HirAccessPlan,
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder, SourceBinding},
        insn::{to_u32, InsertFlags, Insn, RegisterOrLiteral},
        BranchOffset, CursorID,
    },
    LimboError, Result,
};

use super::query::{emit_array_results, predicate_loop};

/// The B-tree table a DELETE or UPDATE writes, with what HIR resolved for it.
pub(super) struct DmlTarget<'a> {
    pub(super) document: &'a HirDocument,
    pub(super) source: &'a hir::Source,
    pub(super) table: &'a Arc<BTreeTable>,
    pub(super) database: usize,
}

impl<'a> DmlTarget<'a> {
    /// Resolve `target`, which must be a B-tree table source that lists all
    /// of its indexes, and register the write on its database.
    pub(super) fn new(
        program: &mut ProgramBuilder,
        document: &'a HirDocument,
        target: SourceId,
        statement: &str,
    ) -> Result<Self> {
        let source = document.source(target).ok_or_else(|| {
            LimboError::InternalError(format!("HIR {statement} target {target} is missing"))
        })?;
        let hir::SourceKind::Table(table) = &source.kind else {
            return Err(LimboError::InternalError(format!(
                "HIR {statement} target {target} is not a table"
            )));
        };
        let Table::BTree(table) = table.value() else {
            return Err(LimboError::InternalError(format!(
                "HIR {statement} target {target} is not a B-tree table"
            )));
        };
        if !matches!(source.index_coverage, hir::IndexCoverage::Complete { .. }) {
            return Err(LimboError::InternalError(format!(
                "HIR {statement} target {target} does not list all of its indexes"
            )));
        }
        let database = source.database.ok_or_else(|| {
            LimboError::InternalError(format!("HIR {statement} target {target} has no database"))
        })?;
        let schema_version = document
            .database(database)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR {statement} target {target} references missing database {database:?}"
                ))
            })?
            .schema_version;
        program.begin_write_on_database(database.index(), schema_version)?;
        Ok(Self {
            document,
            source,
            table,
            database: database.index(),
        })
    }

    pub(super) fn open_table_for_write(&self, program: &mut ProgramBuilder) -> CursorID {
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(self.table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: RegisterOrLiteral::Literal(self.table.root_page),
            db: self.database,
        });
        cursor
    }

    /// Open every index for writing, except `skip`.
    pub(super) fn open_indexes_for_write(
        &self,
        program: &mut ProgramBuilder,
        skip: Option<&Arc<crate::schema::Index>>,
    ) -> Result<Vec<(&hir::IndexExpressions, CursorID)>> {
        let mut indexes = Vec::with_capacity(self.source.index_expressions.len());
        for expressions in &self.source.index_expressions {
            let index = expressions.index.value();
            if expressions.columns.len() != index.columns.len() {
                return Err(LimboError::InternalError(format!(
                    "HIR index {} has {} key expressions for {} columns",
                    index.name,
                    expressions.columns.len(),
                    index.columns.len()
                )));
            }
            if skip.is_some_and(|skip| skip.name == index.name) {
                continue;
            }
            let cursor =
                program.alloc_cursor_id(CursorType::BTreeIndex(expressions.index.handle()));
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(index.root_page),
                db: self.database,
            });
            indexes.push((expressions, cursor));
        }
        Ok(indexes)
    }

    pub(super) fn index_expressions(
        &self,
        index: &crate::schema::Index,
    ) -> Result<&hir::IndexExpressions> {
        self.source
            .index_expressions
            .iter()
            .find(|expressions| expressions.index.value().name == index.name)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR DELETE target {} does not list index {}",
                    self.source.id, index.name
                ))
            })
    }
}

/// Emit the WHERE terms that become checkable at `loop_index`: `None` is
/// before the loop, for terms that read no column of the target.
pub(super) fn emit_access_predicates(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    access: &HirAccessPlan,
    loop_index: Option<usize>,
    fail: BranchOffset,
) -> Result<()> {
    for predicate in access
        .predicates
        .iter()
        .filter(|predicate| !predicate.consumed)
    {
        if predicate.from_outer_join.is_some() {
            return Err(LimboError::InternalError(
                "HIR DML predicate belongs to an outer join".into(),
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
pub(super) fn emit_index_key(
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

/// REPLACE: delete the row whose rowid is in `conflict`, as legacy does:
/// the foreign-key checks of deleting it, its entry in each of `indexes`,
/// then the row. No DELETE trigger fires. The target is left bound to
/// `table_cursor`, which no longer sits on any row.
///
/// `replacement` is the row an UPDATE writes in its place: an immediate NO
/// ACTION parent key it keeps cannot leave a child without its parent.
pub(super) fn emit_replace_delete(
    program: &mut ProgramBuilder,
    target: &DmlTarget<'_>,
    table_cursor: CursorID,
    indexes: &[(&hir::IndexExpressions, CursorID)],
    foreign_keys: &hir::DmlForeignKeys,
    conflict: usize,
    replacement: Option<SourceId>,
) -> Result<()> {
    let DmlTarget {
        document,
        source,
        table,
        database,
    } = *target;
    let missing = program.allocate_label();
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table_cursor,
        src_reg: conflict,
        target_pc: missing,
    });
    // Keys and foreign keys of the deleted row are read from the table.
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
        foreign_keys,
        source.id,
        conflict,
        database,
        replacement,
    )?;
    for (expressions, cursor) in indexes {
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
        is_part_of_update: true,
    });
    program.preassign_label_to_next_insn(missing);
    Ok(())
}

pub(super) fn operation_index(operation: &HirBtreeOperation) -> Option<&Arc<crate::schema::Index>> {
    match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_ref(),
        HirBtreeOperation::RowidEq { .. } => None,
    }
}

/// Legacy's single-row test for a DML access path: a rowid lookup, or a
/// seek where every column of a unique index (or the rowid) is fixed by `=`.
pub(super) fn touches_at_most_one_row(operation: &HirBtreeOperation) -> bool {
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

/// RETURNING rows of a DML statement. Like legacy, they wait in an
/// ephemeral table until every row is written, then are returned.
#[derive(Clone, Copy)]
pub(super) struct ReturningBuffer<'a> {
    cursor: CursorID,
    pub(super) outputs: &'a [hir::Output],
}

impl<'a> ReturningBuffer<'a> {
    pub(super) fn open(
        program: &mut ProgramBuilder,
        table: &Arc<BTreeTable>,
        returning: Option<&'a hir::Returning>,
    ) -> Option<Self> {
        returning.map(|returning| {
            let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id: cursor,
                is_table: true,
            });
            Self {
                cursor,
                outputs: &returning.outputs,
            }
        })
    }

    /// Evaluate the RETURNING values of the current row into new registers.
    pub(super) fn emit_values(
        &self,
        program: &mut ProgramBuilder,
        document: &HirDocument,
    ) -> Result<usize> {
        let start = program.alloc_registers(self.outputs.len());
        for (position, output) in self.outputs.iter().enumerate() {
            super::expr::translate_expr_no_constant_opt(
                program,
                document,
                &output.expr,
                start + position,
            )?;
        }
        emit_array_results(program, document, self.outputs, start);
        Ok(start)
    }

    /// Add values from [`Self::emit_values`] to the buffer.
    pub(super) fn emit_buffer(&self, program: &mut ProgramBuilder, start: usize) {
        let record = program.alloc_register();
        let key = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(start),
            count: to_u32(self.outputs.len()),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: self.cursor,
            rowid_reg: key,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Insert {
            cursor: self.cursor,
            key_reg: key,
            record_reg: record,
            flag: InsertFlags::new().is_ephemeral_table_insert(),
            table_name: String::new(),
        });
    }

    /// After the last row is written: return the buffered rows.
    pub(super) fn emit_rows(&self, program: &mut ProgramBuilder) {
        // Legacy checks foreign keys before returning any row.
        program.emit_insn(Insn::FkCheck { deferred: false });
        emit_returning_scan_back(
            program,
            &ReturningBufferCtx {
                cursor_id: self.cursor,
                num_columns: self.outputs.len(),
            },
        );
    }
}

/// Whether an expression contains a subquery.
pub(super) fn has_subquery(expression: &hir::Expr) -> bool {
    let mut found = false;
    expression.for_each(&mut |expression| {
        found |= matches!(expression, hir::Expr::Subquery(_));
    });
    found
}
