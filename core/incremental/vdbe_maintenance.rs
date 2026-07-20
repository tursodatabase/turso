//! VDBE bytecode maintenance for materialized views.
//!
//! This is the execution half of the IVM engine rewrite: the DBSP circuit
//! remains the logical description of a view, but all row-level evaluation is
//! compiled to a VDBE program so that view maintenance shares the exact
//! expression, comparison, affinity, and NULL semantics of regular query
//! execution instead of re-implementing them.
//!
//! A maintenance program has the shape:
//!
//! ```text
//! OpenRead   c_in    (transaction delta, or the base table for population)
//! OpenWrite  c_view  (the view's btree; records are row values + weight)
//! Rewind c_in -> end
//! loop:
//!   w      := delta weight (Column c_in, n) or the literal +1 for population
//!   if NOT WHERE(row)  -> next     (three-valued logic via translate_condition_expr)
//!   rid    := RowId c_in
//!   out[i] := projection expressions (translate_expr)
//!   merge (rid, out, w) into the view btree:
//!     found:      new_w = cur_w + w
//!     not found:  new_w = w
//!     new_w > 0  -> Insert record(out.., new_w) (upsert at rid)
//!     new_w <= 0 -> Delete, when the row existed
//! next: Next c_in -> loop
//! end:  Halt
//! ```
//!
//! Programs are built with [`ProgramBuilder::new_for_subprogram`], so they
//! emit no transaction instructions and their `Halt` does not commit: they
//! always run inside an already-open write transaction — at commit time for
//! maintenance (`apply_view_deltas`), or inside the CREATE MATERIALIZED VIEW
//! statement for initial population.
//!
//! Delta rows must be applied in capture order: an UPDATE is captured as
//! delete(old image) followed by insert(new image) under the same rowid, and
//! the rowid-keyed weight merge is only correct when the deletion lands
//! first. The [`DeltaCursor`] therefore iterates the captured `Delta`
//! verbatim, without consolidation.

use std::sync::Arc;

use turso_parser::ast;

use crate::function::Func;
use crate::incremental::dbsp::Delta;
use crate::schema::{BTreeTable, Schema};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    bind_and_rewrite_expr, translate_condition_expr, translate_expr_no_constant_opt,
    BindingBehavior, ConditionMetadata, NoConstantOptReason, WalkControl,
};
use crate::translate::plan::{
    ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences,
};
use crate::turso_assert;
use crate::util::walk_expr_with_subqueries;
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::Program;
use crate::{Connection, LimboError, QueryMode, Result, Value};

/// What the maintenance program reads as its input relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceInput {
    /// The transaction's captured delta for the view's base table:
    /// rows are (rowid, base column values..., weight).
    TransactionDelta,
    /// A full scan of the base table with an implicit weight of +1 per row.
    /// Used for initial population at CREATE MATERIALIZED VIEW time.
    BaseTable,
}

/// Read-only cursor over a captured [`Delta`] for one base table, in capture
/// order. Exposes the base columns at their table positions and the weight as
/// one extra trailing column; the delta row's key is exposed as the rowid.
#[derive(Debug)]
pub struct DeltaCursor {
    /// (rowid, row image, weight) in capture order.
    rows: Vec<(i64, Vec<Value>, isize)>,
    /// Number of base-table columns; the weight is column `num_columns`.
    num_columns: usize,
    /// Position of the current row; `rows.len()` means exhausted.
    pos: usize,
    /// False until `rewind` has been called.
    positioned: bool,
}

impl DeltaCursor {
    pub fn new(delta: Delta, num_columns: usize) -> Self {
        let rows = delta
            .changes
            .into_iter()
            .map(|(row, weight)| (row.rowid, row.values, weight))
            .collect();
        Self {
            rows,
            num_columns,
            pos: 0,
            positioned: false,
        }
    }

    pub fn empty(num_columns: usize) -> Self {
        Self::new(Delta::new(), num_columns)
    }

    /// Position at the first row. Returns false when the delta is empty.
    pub fn rewind(&mut self) -> bool {
        self.pos = 0;
        self.positioned = true;
        !self.rows.is_empty()
    }

    /// Advance to the next row. Returns false when exhausted.
    pub fn next(&mut self) -> bool {
        turso_assert!(self.positioned, "DeltaCursor::next before rewind");
        if self.pos < self.rows.len() {
            self.pos += 1;
        }
        self.pos < self.rows.len()
    }

    fn current(&self) -> &(i64, Vec<Value>, isize) {
        turso_assert!(self.positioned, "DeltaCursor read before rewind");
        turso_assert!(self.pos < self.rows.len(), "DeltaCursor read past end");
        &self.rows[self.pos]
    }

    pub fn rowid(&self) -> i64 {
        self.current().0
    }

    pub fn column(&self, idx: usize) -> Value {
        let (_, values, weight) = self.current();
        if idx == self.num_columns {
            return Value::from_i64(*weight as i64);
        }
        let num_columns = self.num_columns;
        turso_assert!(
            idx < num_columns,
            "DeltaCursor column {idx} out of range ({num_columns} columns + weight)"
        );
        // Row images are captured as full records, but be tolerant of short
        // rows the same way btree Column is: missing trailing columns are NULL.
        values.get(idx).cloned().unwrap_or(Value::Null)
    }
}

/// Check whether a view definition is maintainable by the VDBE codegen.
///
/// This is the CREATE MATERIALIZED VIEW gate: shapes outside the supported
/// set are rejected outright rather than silently mis-maintained. The set
/// grows as codegen for more operators lands (aggregates, joins, set ops).
pub fn validate_vdbe_maintainable(select: &ast::Select) -> Result<()> {
    let unsupported = |what: &str| -> Result<()> {
        Err(LimboError::ParseError(format!(
            "materialized views with {what} are not yet supported",
        )))
    };

    if select.with.is_some() {
        return unsupported("WITH clauses");
    }
    if !select.body.compounds.is_empty() {
        return unsupported("compound SELECTs (UNION/INTERSECT/EXCEPT)");
    }
    if !select.order_by.is_empty() {
        return unsupported("ORDER BY");
    }
    if select.limit.is_some() {
        return unsupported("LIMIT");
    }

    let ast::OneSelect::Select {
        distinctness,
        columns,
        from,
        where_clause,
        group_by,
        window_clause,
        ..
    } = &select.body.select
    else {
        return unsupported("VALUES");
    };

    if distinctness.is_some() {
        return unsupported("DISTINCT");
    }
    if group_by.is_some() {
        return unsupported("GROUP BY");
    }
    if !window_clause.is_empty() {
        return unsupported("window functions");
    }

    let Some(from) = from else {
        return unsupported("no FROM clause");
    };
    if !from.joins.is_empty() {
        return unsupported("joins");
    }
    if !matches!(from.select.as_ref(), ast::SelectTable::Table(_, _, _)) {
        return unsupported("subqueries or table functions in FROM");
    }

    let check_expr = |expr: &ast::Expr| -> Result<()> {
        walk_expr_with_subqueries(expr, &mut |e: &ast::Expr| {
            match e {
                ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                    return Err(LimboError::ParseError(
                        "materialized views with subqueries are not yet supported".to_string(),
                    ));
                }
                ast::Expr::FunctionCall { name, args, .. } => {
                    if matches!(
                        Func::resolve_function(name.as_str(), args.len()),
                        Ok(Some(Func::Agg(_)))
                    ) {
                        return Err(LimboError::ParseError(
                            "materialized views with aggregate functions are not yet supported"
                                .to_string(),
                        ));
                    }
                }
                ast::Expr::FunctionCallStar { name, .. } => {
                    if matches!(
                        Func::resolve_function(name.as_str(), 0),
                        Ok(Some(Func::Agg(_)))
                    ) {
                        return Err(LimboError::ParseError(
                            "materialized views with aggregate functions are not yet supported"
                                .to_string(),
                        ));
                    }
                }
                ast::Expr::Variable(_) => {
                    return Err(LimboError::ParseError(
                        "materialized views with bound parameters are not supported".to_string(),
                    ));
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })
    };

    for column in columns {
        if let ast::ResultColumn::Expr(expr, _) = column {
            check_expr(expr)?;
        }
    }
    if let Some(where_expr) = where_clause {
        check_expr(where_expr)?;
    }

    Ok(())
}

/// Compile the maintenance (or population) program for a filter/project view.
///
/// `view_root_page` is the root of the view's data btree; `view_columns` are
/// the view's output columns (the on-disk record is these plus a trailing
/// weight column).
pub fn compile_maintenance_program(
    view_name: &str,
    select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    validate_vdbe_maintainable(select)?;

    let ast::OneSelect::Select {
        columns,
        from,
        where_clause,
        ..
    } = &select.body.select
    else {
        unreachable!("validate_vdbe_maintainable admits only OneSelect::Select");
    };
    let from = from
        .as_ref()
        .expect("validate_vdbe_maintainable requires a FROM clause");
    let ast::SelectTable::Table(base_name, base_alias, _) = from.select.as_ref() else {
        unreachable!("validate_vdbe_maintainable admits only plain tables in FROM");
    };

    let base_table = schema
        .get_btree_table(base_name.name.as_str())
        .ok_or_else(|| {
            LimboError::ParseError(format!(
                "materialized view base table {} not found",
                base_name.name.as_str()
            ))
        })?;
    let num_base_columns = base_table.columns().len();

    let mut program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts {
            num_cursors: 2,
            approx_num_insns: 32 + 8 * num_view_columns,
            approx_num_labels: 8,
        },
    );
    program.prologue();

    let syms = connection.syms.read();
    let resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
    );

    // The input relation binds like a scan of the base table, so WHERE and
    // projection expressions resolve exactly as they would in the defining
    // query. The alias (if any) from the view definition is preserved.
    let table_ref_id = program.table_reference_counter.next();
    let identifier = match base_alias {
        Some(ast::As::As(name) | ast::As::Elided(name) | ast::As::ImplicitColumnName(name)) => {
            name.as_str().to_string()
        }
        None => base_table.name.clone(),
    };
    let mut table_references = TableReferences::new(
        vec![JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            table: crate::schema::Table::BTree(base_table.clone()),
            identifier,
            internal_id: table_ref_id,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: 0,
            indexed: None,
        }],
        vec![],
    );

    let input_cursor_id = match input {
        MaintenanceInput::TransactionDelta => {
            let cursor_id = program.alloc_cursor_id_keyed(
                CursorKey::table(table_ref_id),
                CursorType::ViewDelta {
                    view_name: view_name.to_string(),
                    table: base_table.clone(),
                },
            );
            // root_page is unused for delta cursors; the open dispatches on
            // the cursor type and reads the connection's captured deltas.
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: 0,
                db: 0,
            });
            cursor_id
        }
        MaintenanceInput::BaseTable => {
            let cursor_id = program.alloc_cursor_id_keyed(
                CursorKey::table(table_ref_id),
                CursorType::BTreeTable(base_table.clone()),
            );
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: base_table.root_page,
                db: 0,
            });
            cursor_id
        }
    };

    // Write cursor over the view btree. The synthesized table exists only to
    // size the cursor: view records are the output columns plus the weight.
    let view_btree = Arc::new(synthesized_view_table(
        view_name,
        view_root_page,
        num_view_columns,
    ));
    let view_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(view_btree));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: view_cursor_id,
        root_page: RegisterOrLiteral::Literal(view_root_page),
        db: 0,
    });

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: input_cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    // Weight of the current input row.
    let weight_reg = program.alloc_register();
    match input {
        MaintenanceInput::TransactionDelta => {
            program.emit_insn(Insn::Column {
                cursor_id: input_cursor_id,
                column: num_base_columns,
                dest: weight_reg,
                default: None,
            });
        }
        MaintenanceInput::BaseTable => {
            program.emit_int(1, weight_reg);
        }
    }

    // WHERE: rows whose predicate is FALSE or NULL do not belong to the view.
    if let Some(where_expr) = where_clause {
        let mut bound = where_expr.as_ref().clone();
        bind_and_rewrite_expr(
            &mut bound,
            Some(&mut table_references),
            None,
            &resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        let pred_true = program.allocate_label();
        translate_condition_expr(
            &mut program,
            &table_references,
            &bound,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: pred_true,
                jump_target_when_false: next_label,
                jump_target_when_null: next_label,
            },
            &resolver,
        )?;
        program.preassign_label_to_next_insn(pred_true);
    }

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: input_cursor_id,
        dest: rowid_reg,
    });

    // Projection into a contiguous register block, with one extra slot at the
    // end for the merged weight so MakeRecord can consume the whole range.
    let out_start_reg = program.alloc_registers(num_view_columns + 1);
    let new_weight_reg = out_start_reg + num_view_columns;
    let mut out_reg = out_start_reg;
    for column in columns {
        match column {
            ast::ResultColumn::Expr(expr, _) => {
                let mut bound = expr.as_ref().clone();
                bind_and_rewrite_expr(
                    &mut bound,
                    Some(&mut table_references),
                    None,
                    &resolver,
                    BindingBehavior::ResultColumnsNotAllowed,
                )?;
                translate_expr_no_constant_opt(
                    &mut program,
                    Some(&table_references),
                    &bound,
                    out_reg,
                    &resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                out_reg += 1;
            }
            ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {
                for (i, _col) in base_table.columns().iter().enumerate() {
                    program.emit_column_or_rowid(input_cursor_id, i, out_reg);
                    out_reg += 1;
                }
            }
        }
    }
    let num_projected = out_reg - out_start_reg;
    turso_assert!(
        num_projected == num_view_columns,
        "projection produced {num_projected} columns, view declares {num_view_columns}"
    );

    // Merge (rowid, out.., weight) into the view btree.
    let notfound_label = program.allocate_label();
    let merge_label = program.allocate_label();
    let write_label = program.allocate_label();
    let delete_label = program.allocate_label();

    let cur_weight_reg = program.alloc_register();
    let found_reg = program.alloc_register();
    let zero_reg = program.alloc_register();
    program.emit_int(0, zero_reg);

    program.emit_insn(Insn::SeekRowid {
        cursor_id: view_cursor_id,
        src_reg: rowid_reg,
        target_pc: notfound_label,
    });
    program.emit_insn(Insn::Column {
        cursor_id: view_cursor_id,
        column: num_view_columns,
        dest: cur_weight_reg,
        default: None,
    });
    program.emit_int(1, found_reg);
    program.emit_insn(Insn::Goto {
        target_pc: merge_label,
    });
    program.preassign_label_to_next_insn(notfound_label);
    program.emit_int(0, cur_weight_reg);
    program.emit_int(0, found_reg);
    program.preassign_label_to_next_insn(merge_label);

    program.emit_insn(Insn::Add {
        lhs: weight_reg,
        rhs: cur_weight_reg,
        dest: new_weight_reg,
    });
    program.emit_insn(Insn::Gt {
        lhs: new_weight_reg,
        rhs: zero_reg,
        target_pc: write_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    // new_weight <= 0: remove the row if it existed; a retraction of a row
    // the view never held (weight <= 0 and not found) is a no-op, matching
    // the previous engine's write_row behavior.
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: delete_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: next_label,
    });
    program.preassign_label_to_next_insn(delete_label);
    program.emit_insn(Insn::Delete {
        cursor_id: view_cursor_id,
        table_name: view_name.to_string(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Goto {
        target_pc: next_label,
    });

    program.preassign_label_to_next_insn(write_label);
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: out_start_reg as u16,
        count: (num_view_columns + 1) as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: view_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: view_name.to_string(),
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input_cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);

    program.epilogue(schema);
    drop(syms);
    program.build(connection.clone(), false, "materialized view maintenance")
}

/// A minimal BTreeTable describing the view's on-disk layout (output columns
/// plus the trailing weight column), used only to size the write cursor.
fn synthesized_view_table(view_name: &str, root_page: i64, num_view_columns: usize) -> BTreeTable {
    use crate::schema::BTreeCharacteristics;
    // Declared type is empty (BLOB affinity) so nothing about this synthetic
    // schema can coerce the values the maintenance program writes.
    let mut columns: Vec<crate::schema::Column> = (0..num_view_columns)
        .map(|i| {
            crate::schema::Column::new_default_text(Some(format!("c{i}")), String::new(), None)
        })
        .collect();
    columns.push(crate::schema::Column::new_default_text(
        Some("__ivm_weight".to_string()),
        String::new(),
        None,
    ));
    BTreeTable::new(
        root_page,
        view_name.to_string(),
        Vec::new(),
        columns,
        BTreeCharacteristics::HAS_ROWID,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    )
}
