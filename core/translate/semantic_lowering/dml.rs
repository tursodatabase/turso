//! Lowering shared by HIR DELETE and UPDATE.

use turso_parser::ast;

use crate::{
    error::SQLITE_CONSTRAINT_CHECK,
    schema::{BTreeTable, Table, EXPR_INDEX_SENTINEL},
    sync::Arc,
    translate::{
        expr::ConditionMetadata,
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

use super::fkey_action::CompiledAction;
use super::query::{emit_array_results, predicate_loop};
use super::trigger::CompiledTrigger;
use super::{HirPlan, HirSession};

/// The subprograms a DML statement fires. They are compiled before anything
/// is emitted, so one that needs legacy leaves the whole statement to it.
#[derive(Default)]
pub(super) struct Subprograms<'a> {
    /// The statement's row triggers.
    pub(super) triggers: Vec<CompiledTrigger>,
    /// The foreign-key actions of the statement's own change: ON DELETE for
    /// a DELETE, ON UPDATE for an UPDATE.
    pub(super) actions: Vec<CompiledAction<'a>>,
    /// The ON DELETE actions of the rows REPLACE deletes.
    pub(super) replace_actions: Vec<CompiledAction<'a>>,
    /// INSERT: the UPDATE triggers an UPSERT's DO UPDATE fires.
    pub(super) upsert_triggers: Vec<CompiledTrigger>,
    /// INSERT: the ON UPDATE actions of each UPSERT clause's DO UPDATE, in
    /// clause order.
    pub(super) upsert_actions: Vec<Vec<CompiledAction<'a>>>,
}

impl<'a> Subprograms<'a> {
    /// What a DELETE fires. `None` means one needs legacy.
    pub(super) fn for_delete(
        session: &HirSession<'_>,
        parent: &ProgramBuilder,
        plan: &HirPlan,
        delete: &'a hir::Delete,
    ) -> Result<Option<Self>> {
        let Some(triggers) = super::trigger::delete_triggers(session, parent, plan, delete)? else {
            return Ok(None);
        };
        let Some(actions) = super::fkey_action::delete_actions(session, parent, delete)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            triggers,
            actions,
            ..Self::default()
        }))
    }

    /// What an UPDATE fires. `None` means one needs legacy.
    pub(super) fn for_update(
        session: &HirSession<'_>,
        parent: &ProgramBuilder,
        plan: &HirPlan,
        update: &'a hir::Update,
    ) -> Result<Option<Self>> {
        let Some(triggers) = super::trigger::update_triggers(session, parent, plan, update)? else {
            return Ok(None);
        };
        let Some(actions) = super::fkey_action::update_actions(session, parent, plan, update)?
        else {
            return Ok(None);
        };
        let Some(replace_actions) = super::update::replace_actions(session, parent, plan, update)?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            triggers,
            actions,
            replace_actions,
            ..Self::default()
        }))
    }

    /// What an INSERT fires. `None` means one needs legacy.
    pub(super) fn for_insert(
        session: &HirSession<'_>,
        parent: &ProgramBuilder,
        plan: &HirPlan,
        insert: &'a hir::Insert,
    ) -> Result<Option<Self>> {
        let Some(triggers) = super::trigger::insert_triggers(session, parent, plan, insert)? else {
            return Ok(None);
        };
        let Some(upsert_triggers) =
            super::trigger::upsert_update_triggers(session, parent, plan, insert)?
        else {
            return Ok(None);
        };
        let Some(replace_actions) = super::insert::replace_actions(session, parent, plan, insert)?
        else {
            return Ok(None);
        };
        let Some(upsert_actions) =
            super::fkey_action::upsert_actions(session, parent, plan, insert)?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            triggers,
            actions: Vec::new(),
            replace_actions,
            upsert_triggers,
            upsert_actions,
        }))
    }
}

/// The B-tree table a DELETE or UPDATE writes, with what HIR resolved for it.
pub(super) struct DmlTarget<'a> {
    pub(super) document: &'a HirDocument,
    pub(super) source: &'a hir::Source,
    pub(super) table: &'a Arc<BTreeTable>,
    pub(super) database: usize,
    /// The capture table, when the statement captures its changes.
    pub(super) capture: Option<super::cdc::Capture<'a>>,
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
            capture: None,
        })
    }

    /// Open the capture table when the statement captures its changes.
    pub(super) fn with_capture(
        mut self,
        program: &mut ProgramBuilder,
        capture: Option<&'a hir::CaptureChanges>,
    ) -> Result<Self> {
        self.capture = super::cdc::Capture::open(program, capture, self.table)?;
        Ok(self)
    }

    /// At the end of the statement: the COMMIT row of change data capture.
    pub(super) fn emit_capture_commit(&self, program: &mut ProgramBuilder) -> Result<()> {
        match &self.capture {
            Some(capture) => capture.emit_commit(program),
            None => Ok(()),
        }
    }

    /// The catalog table the target writes.
    pub(super) fn resolved_table(&self) -> Result<&'a hir::ResolvedTable> {
        match &self.source.kind {
            hir::SourceKind::Table(table) => Ok(table),
            _ => Err(LimboError::InternalError(format!(
                "HIR DML target {} is not a table",
                self.source.id
            ))),
        }
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
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_replace_delete(
    program: &mut ProgramBuilder,
    target: &DmlTarget<'_>,
    table_cursor: CursorID,
    indexes: &[(&hir::IndexExpressions, CursorID)],
    foreign_keys: &hir::DmlForeignKeys,
    conflict: usize,
    replacement: Option<SourceId>,
    actions: &[CompiledAction<'_>],
) -> Result<()> {
    let DmlTarget {
        document,
        source,
        table,
        database,
        ..
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
    // Like legacy, the ON DELETE actions of the deleted row run once it is
    // gone.
    let action_keys = actions
        .iter()
        .map(|action| {
            super::fkey::emit_parent_key(program, document, action.foreign_key, source.id)
        })
        .collect::<Result<Vec<_>>>()?;
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
    if let Some(capture) = &target.capture {
        let before = capture.before_record(program, table, table_cursor, conflict);
        capture.emit_row(
            program,
            crate::translate::emitter::OperationMode::DELETE,
            conflict,
            before,
            None,
            None,
        )?;
    }
    program.emit_insn(Insn::Delete {
        cursor_id: table_cursor,
        table_name: table.name.clone(),
        is_part_of_update: true,
    });
    for (action, key) in actions.iter().zip(action_keys) {
        action.emit_fire(program, key, None)?;
    }
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

/// The virtual table a DML statement writes, or `None` for a B-tree target.
pub(super) fn virtual_target(
    document: &HirDocument,
    target: SourceId,
) -> Result<Option<Arc<crate::vtab::VirtualTable>>> {
    let source = document
        .source(target)
        .ok_or_else(|| LimboError::InternalError(format!("HIR DML target {target} is missing")))?;
    Ok(match &source.kind {
        hir::SourceKind::Table(table) => table.value().virtual_table(),
        _ => None,
    })
}

/// Whether a write to a virtual table stays in legacy: `sqlite_dbpage`,
/// which legacy writes only in testing builds.
pub(super) fn virtual_needs_legacy(table: &crate::vtab::VirtualTable) -> bool {
    table.name == "sqlite_dbpage"
}

/// Open a virtual table for writing, as legacy does: the rows a statement
/// writes are handed to the table with `VUpdate` through this cursor, inside
/// the table's own transaction.
pub(super) fn open_virtual_for_write(
    program: &mut ProgramBuilder,
    table: &Arc<crate::vtab::VirtualTable>,
) -> CursorID {
    let cursor = program.alloc_cursor_id(CursorType::VirtualTable(table.clone()));
    program.emit_insn(Insn::VOpen { cursor_id: cursor });
    program.emit_insn(Insn::VBegin { cursor_id: cursor });
    cursor
}

/// RETURNING rows of a DML statement. Like legacy, they wait in an
/// ephemeral table until every row is written, then are returned.
#[derive(Clone, Copy)]
pub(super) struct ReturningBuffer<'a> {
    cursor: CursorID,
    pub(super) outputs: &'a [hir::Output],
}

impl<'a> ReturningBuffer<'a> {
    /// Like [`Self::open`], for a target that is not a B-tree table.
    pub(super) fn open_scratch(
        program: &mut ProgramBuilder,
        returning: Option<&'a hir::Returning>,
    ) -> Option<Self> {
        let returning = returning?;
        let table = Arc::new(BTreeTable::new(
            0,
            "returning_scratch".to_string(),
            Vec::new(),
            (0..returning.outputs.len())
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
        Self::open(program, &table, Some(returning))
    }

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
        target: &hir::ResolvedTable,
    ) -> Result<usize> {
        // Like SQLite, a subquery in RETURNING whose FROM clause names the
        // target table runs again for each row, so it sees the rows written
        // so far; any other runs once.
        for output in self.outputs {
            output.expr.for_each(&mut |expression| {
                if let hir::Expr::Subquery(subquery) = expression {
                    let query = subquery_query(subquery);
                    if document.query_reads_table_directly(query, target) {
                        program.rerun_subquery(query);
                    }
                }
            });
        }
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
        let done = program.allocate_label();
        let next = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.cursor,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(next);
        let count = self.outputs.len();
        let start = program.alloc_registers(count);
        for i in 0..count {
            program.emit_insn(Insn::Column {
                cursor_id: self.cursor,
                column: i,
                dest: start + i,
                default: None,
            });
        }
        program.emit_insn(Insn::ResultRow {
            start_reg: start,
            count,
        });
        program.emit_insn(Insn::Next {
            cursor_id: self.cursor,
            pc_if_next: next,
            fullscan: false,
        });
        program.preassign_label_to_next_insn(done);
    }
}

/// Check each CHECK constraint against the new row, which the constraint's
/// source must be bound to. Like SQLite, a NULL result passes. A failure
/// skips the row under IGNORE and stops the statement otherwise; REPLACE
/// does not apply to CHECK.
pub(super) fn emit_check_constraints<'c>(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    constraints: impl IntoIterator<Item = &'c hir::CheckConstraint>,
    or_conflict: ast::ResolveType,
    skip_row: BranchOffset,
) -> Result<()> {
    for constraint in constraints {
        let result = program.alloc_register();
        super::expr::translate_expr_no_constant_opt(
            program,
            document,
            &constraint.expression,
            result,
        )?;
        let passed = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: result,
            target_pc: passed,
        });
        program.emit_insn(Insn::If {
            reg: result,
            target_pc: passed,
            jump_if_null: false,
        });
        if or_conflict == ast::ResolveType::Ignore {
            program.emit_insn(Insn::Goto {
                target_pc: skip_row,
            });
        } else {
            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_CHECK,
                description: constraint.description.clone(),
                on_error: None,
                description_reg: None,
            });
        }
        program.preassign_label_to_next_insn(passed);
    }
    Ok(())
}

/// The query an expression subquery runs.
pub(super) fn subquery_query(subquery: &hir::SubqueryExpr) -> hir::QueryId {
    match subquery {
        hir::SubqueryExpr::Scalar { query, .. }
        | hir::SubqueryExpr::Row { query }
        | hir::SubqueryExpr::Exists(query)
        | hir::SubqueryExpr::In { query, .. } => *query,
    }
}

/// Whether the target is a STRICT table HIR does not lower yet: one with a
/// custom type or array column, whose type check needs the resolved types.
pub(super) fn strict_needs_legacy(table: &BTreeTable) -> bool {
    table.is_strict
        && table.columns().iter().any(|column| {
            column.is_array()
                || !["ANY", "INTEGER", "INT", "REAL", "BLOB", "TEXT"]
                    .iter()
                    .any(|name| column.ty_str.eq_ignore_ascii_case(name))
        })
}

/// STRICT: check each value of the row in `start..` against its column's
/// type, converting it where SQLite allows and failing otherwise, as legacy
/// does. A STRICT table gets this instead of column affinity.
pub(super) fn emit_strict_type_check(
    program: &mut ProgramBuilder,
    table: &Arc<BTreeTable>,
    start: usize,
) {
    // Like SQLite, a virtual column is not checked: it is checked as ANY.
    let table_reference = if table.has_virtual_columns() {
        let mut checked = (**table).clone();
        for column in checked.columns_mut().iter_mut() {
            if column.is_virtual_generated() {
                column.ty_str = "ANY".to_string();
            }
        }
        Arc::new(checked)
    } else {
        table.clone()
    };
    program.emit_insn(Insn::TypeCheck {
        start_reg: start,
        count: table.columns().len(),
        check_generated: true,
        table_reference,
    });
}

/// Compute each virtual generated column of the row in registers
/// `start + column` into its register, in dependency order, as legacy does.
/// `source` must be bound to those registers. Reads of the row through
/// `source` compute virtual columns anyway; this fills the registers that
/// are copied directly, such as index keys, NOT NULL checks and triggers.
pub(super) fn emit_virtual_columns(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    source: SourceId,
    table: &BTreeTable,
    start: usize,
) -> Result<()> {
    if !table.has_virtual_columns() {
        return Ok(());
    }
    for (column, _) in table
        .columns_topo_sort()?
        .iter()
        .filter(|(_, column)| column.is_virtual_generated())
    {
        super::expr::translate_expr_no_constant_opt(
            program,
            document,
            &hir::Expr::Column(hir::ColumnRef { source, column }),
            start + column,
        )?;
    }
    Ok(())
}

/// The record of the row in registers `start + column`. A virtual
/// generated column is not stored, so with one the stored columns are first
/// copied together, in their stored order.
pub(super) fn emit_row_record(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    start: usize,
) -> usize {
    let stored = if table.has_virtual_columns() {
        let mut stored = table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| !column.is_virtual_generated())
            .map(|(position, column)| {
                (table.logical_to_physical_column(position), position, column)
            })
            .collect::<Vec<_>>();
        stored.sort_by_key(|(physical, _, _)| *physical);
        let block = program.alloc_registers(stored.len());
        for (offset, (_, position, _)) in stored.iter().enumerate() {
            program.emit_insn(Insn::Copy {
                src_reg: start + position,
                dst_reg: block + offset,
                extra_amount: 0,
            });
        }
        let affinities = stored
            .iter()
            .map(|(_, _, column)| column.affinity_with_strict(table.is_strict).aff_mask())
            .collect::<String>();
        (block, stored.len(), affinities)
    } else {
        let affinities = table
            .columns()
            .iter()
            .map(|column| column.affinity_with_strict(table.is_strict).aff_mask())
            .collect::<String>();
        (start, table.columns().len(), affinities)
    };
    let (block, count, affinities) = stored;
    let record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(block),
        count: to_u32(count),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: Some(affinities),
    });
    record
}

/// Whether an index key column is a virtual generated column of `table`.
/// Its value is in the row's registers, computed with the column's
/// affinity, so a new key copies it instead of computing it again.
pub(super) fn is_virtual_key_column(
    table: &BTreeTable,
    column: &crate::schema::IndexColumn,
) -> bool {
    column.pos_in_table != EXPR_INDEX_SENTINEL
        && table
            .columns()
            .get(column.pos_in_table)
            .is_some_and(|column| column.is_virtual_generated())
}

/// Whether an expression contains a subquery.
pub(super) fn has_subquery(expression: &hir::Expr) -> bool {
    let mut found = false;
    expression.for_each(&mut |expression| {
        found |= matches!(expression, hir::Expr::Subquery(_));
    });
    found
}
