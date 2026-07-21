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

use crate::function::{AggFunc, Func};
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
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
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

/// Where the maintenance program's output goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceOutput {
    /// Weight-merge into the view's btree (commit-time maintenance and
    /// initial population).
    ViewBtree,
    /// Emit each transformed row as a result row `(rowid, columns.., weight)`
    /// without touching any btree. Used to overlay uncommitted transaction
    /// changes for same-transaction reads of the view.
    EmitRows,
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

/// One aggregate call in a GROUP BY view's SELECT list.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub func: AggFunc,
    /// The aggregate's argument expression; None for COUNT(*).
    pub arg: Option<ast::Expr>,
}

/// How each result column of a GROUP BY view is produced.
#[derive(Debug, Clone, Copy)]
pub enum OutputColumn {
    /// The i-th GROUP BY expression.
    Group(usize),
    /// The i-th entry of [`ViewShape::GroupAggregate::aggregates`].
    Aggregate(usize),
}

/// The maintainable shape of a view definition, decided at CREATE time.
#[derive(Debug, Clone)]
pub enum ViewShape {
    /// Single-table SELECT with optional WHERE: rows map 1:1 to base rows.
    FilterProject,
    /// Single-table GROUP BY with invertible aggregates.
    GroupAggregate {
        group_exprs: Vec<ast::Expr>,
        /// `aggregates[0]` is always a hidden COUNT(*) tracking group
        /// liveness (a group's view row exists iff its row count > 0).
        aggregates: Vec<AggregateSpec>,
        outputs: Vec<OutputColumn>,
    },
}

fn unsupported<T>(what: &str) -> Result<T> {
    Err(LimboError::ParseError(format!(
        "materialized views with {what} are not yet supported",
    )))
}

/// Reject subqueries, bound parameters, and (unless the expression IS a
/// supported aggregate handled by the caller) aggregate function calls.
fn check_scalar_expr(expr: &ast::Expr) -> Result<()> {
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
                    return unsupported("expressions over aggregate results");
                }
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                if matches!(
                    Func::resolve_function(name.as_str(), 0),
                    Ok(Some(Func::Agg(_)))
                ) {
                    return unsupported("expressions over aggregate results");
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
}

/// Recognize a result column that is exactly one supported aggregate call.
fn as_supported_aggregate(expr: &ast::Expr) -> Result<Option<AggregateSpec>> {
    match expr {
        ast::Expr::FunctionCallStar { name, filter_over } => {
            let Ok(Some(Func::Agg(func))) = Func::resolve_function(name.as_str(), 0) else {
                return Ok(None);
            };
            if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() {
                return unsupported("FILTER or OVER clauses on aggregates");
            }
            match func {
                AggFunc::Count0 => Ok(Some(AggregateSpec { func, arg: None })),
                _ => unsupported(&format!("{}(*)", name.as_str())),
            }
        }
        ast::Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            within_group,
            filter_over,
        } => {
            let Ok(Some(Func::Agg(func))) = Func::resolve_function(name.as_str(), args.len())
            else {
                return Ok(None);
            };
            if distinctness.is_some() {
                return unsupported("DISTINCT aggregates");
            }
            if !order_by.is_empty() || !within_group.is_empty() {
                return unsupported("ordered aggregates");
            }
            if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() {
                return unsupported("FILTER or OVER clauses on aggregates");
            }
            match func {
                AggFunc::Count | AggFunc::Sum | AggFunc::Total | AggFunc::Avg => {
                    let [arg] = args.as_slice() else {
                        return unsupported("multi-argument aggregates");
                    };
                    check_scalar_expr(arg)?;
                    Ok(Some(AggregateSpec {
                        func,
                        arg: Some(arg.as_ref().clone()),
                    }))
                }
                // MIN/MAX need stored value multisets to handle retraction of
                // the current extreme; GROUP_CONCAT and friends have no
                // inverse at all.
                other => unsupported(&format!("the {} aggregate", other.as_str())),
            }
        }
        _ => Ok(None),
    }
}

/// Classify a view definition into its maintainable shape.
///
/// This is the CREATE MATERIALIZED VIEW gate: shapes outside the supported
/// set are rejected outright rather than silently mis-maintained. The set
/// grows as codegen for more operators lands (MIN/MAX, joins, set ops).
pub fn classify_view(select: &ast::Select) -> Result<ViewShape> {
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

    if let Some(where_expr) = where_clause {
        check_scalar_expr(where_expr)?;
    }

    let Some(group_by) = group_by else {
        // No GROUP BY: any aggregate call makes this a single-group scalar
        // aggregate, which needs an always-present output row even over an
        // empty table — not supported yet.
        for column in columns {
            if let ast::ResultColumn::Expr(expr, _) = column {
                if as_supported_aggregate(expr)?.is_some() {
                    return unsupported("aggregates without GROUP BY");
                }
                check_scalar_expr(expr)?;
            }
        }
        return Ok(ViewShape::FilterProject);
    };

    if group_by.having.is_some() {
        return unsupported("HAVING");
    }

    let group_exprs: Vec<ast::Expr> = group_by.exprs.iter().map(|e| e.as_ref().clone()).collect();
    for g in &group_exprs {
        check_scalar_expr(g)?;
    }

    // aggregates[0] is the hidden liveness COUNT(*).
    let mut aggregates = vec![AggregateSpec {
        func: AggFunc::Count0,
        arg: None,
    }];
    let mut outputs = Vec::with_capacity(columns.len());
    for column in columns {
        let ast::ResultColumn::Expr(expr, _) = column else {
            return unsupported("star projections combined with GROUP BY");
        };
        if let Some(spec) = as_supported_aggregate(expr)? {
            outputs.push(OutputColumn::Aggregate(aggregates.len()));
            aggregates.push(spec);
            continue;
        }
        check_scalar_expr(expr)?;
        let Some(idx) = group_exprs
            .iter()
            .position(|g| crate::util::exprs_are_equivalent(g, expr))
        else {
            // SQLite's bare-column semantics (an arbitrary row of the group)
            // cannot be maintained incrementally.
            return unsupported("non-aggregate result columns not in GROUP BY");
        };
        outputs.push(OutputColumn::Group(idx));
    }

    Ok(ViewShape::GroupAggregate {
        group_exprs,
        aggregates,
        outputs,
    })
}

/// Check whether a view definition is maintainable by the VDBE codegen.
pub fn validate_vdbe_maintainable(select: &ast::Select) -> Result<()> {
    classify_view(select).map(|_| ())
}

/// The CREATE TABLE statement for a GROUP BY view's internal state table:
/// one row per live group, holding the group-key values, the rowid of the
/// group's row in the view btree, and each aggregate's persisted state
/// payload. The PRIMARY KEY over the group columns gives the automatic
/// index used to find a group's state.
///
/// Declared column types are empty so nothing coerces the stored values.
pub fn aggregate_state_table_sql(state_table_name: &str, shape: &ViewShape) -> Result<String> {
    let ViewShape::GroupAggregate {
        group_exprs,
        aggregates,
        ..
    } = shape
    else {
        return Err(LimboError::InternalError(
            "only GROUP BY views have state tables".to_string(),
        ));
    };
    let mut columns = Vec::new();
    for i in 0..group_exprs.len() {
        columns.push(format!("g{i}"));
    }
    columns.push("view_rowid".to_string());
    for (i, spec) in aggregates.iter().enumerate() {
        let width = crate::vdbe::execute::agg_payload_width(&spec.func).ok_or_else(|| {
            LimboError::InternalError(format!(
                "aggregate {:?} has no fixed-width state",
                spec.func
            ))
        })?;
        for j in 0..width {
            columns.push(format!("a{i}_{j}"));
        }
    }
    let key: Vec<String> = (0..group_exprs.len()).map(|i| format!("g{i}")).collect();
    Ok(format!(
        "CREATE TABLE {state_table_name} ({}, PRIMARY KEY ({}))",
        columns.join(", "),
        key.join(", ")
    ))
}

/// Compile the maintenance (or population) program for a filter/project view.
///
/// `view_root_page` is the root of the view's data btree; `view_columns` are
/// the view's output columns (the on-disk record is these plus a trailing
/// weight column).
/// Compile the maintenance (or population) program for a materialized view,
/// dispatching on its classified shape.
#[allow(clippy::too_many_arguments)]
pub fn compile_maintenance_program(
    view_name: &str,
    select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    output: MaintenanceOutput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    match classify_view(select)? {
        ViewShape::FilterProject => compile_filter_project_program(
            view_name,
            select,
            view_root_page,
            num_view_columns,
            input,
            output,
            schema,
            connection,
        ),
        shape @ ViewShape::GroupAggregate { .. } => {
            if matches!(output, MaintenanceOutput::EmitRows) {
                return Err(LimboError::InternalError(
                    "aggregate views serve uncommitted reads by recomputing the defining query"
                        .to_string(),
                ));
            }
            compile_group_aggregate_program(
                view_name,
                select,
                &shape,
                view_root_page,
                num_view_columns,
                input,
                schema,
                connection,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn compile_filter_project_program(
    view_name: &str,
    select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    output: MaintenanceOutput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
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

    // Write cursor over the view btree (btree output only). The synthesized
    // table exists only to size the cursor: view records are the output
    // columns plus the weight.
    let view_cursor_id = match output {
        MaintenanceOutput::ViewBtree => {
            let view_btree = Arc::new(synthesized_view_table(
                view_name,
                view_root_page,
                num_view_columns,
            ));
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(view_btree));
            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(view_root_page),
                db: 0,
            });
            Some(cursor_id)
        }
        MaintenanceOutput::EmitRows => None,
    };

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

    match output {
        MaintenanceOutput::EmitRows => {
            // rowid_reg is allocated immediately before the out block, so
            // [rowid, out.., weight] is one contiguous range. The input
            // weight is copied into the trailing slot and the whole row is
            // emitted for the caller to collect.
            debug_assert_eq!(rowid_reg + 1, out_start_reg);
            program.emit_insn(Insn::Copy {
                src_reg: weight_reg,
                dst_reg: new_weight_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::ResultRow {
                start_reg: rowid_reg,
                count: num_view_columns + 2,
            });
        }
        MaintenanceOutput::ViewBtree => {
            // Merge (rowid, out.., weight) into the view btree.
            let view_cursor_id = view_cursor_id.expect("btree output mode opened the view cursor");
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
            // new_weight <= 0: remove the row if it existed; a retraction of
            // a row the view never held (weight <= 0 and not found) is a
            // no-op, matching the previous engine's write_row behavior.
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
        }
    }

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

/// Compile the maintenance (or population) program for a GROUP BY view with
/// invertible aggregates.
///
/// Program shape, per input row (delta row with weight w, or base-table row
/// with w = +1 during population):
///
/// ```text
///   if NOT WHERE(row) -> next
///   g[..] := GROUP BY expressions of the row
///   look up the group in the state table's primary-key index
///     found:     load view_rowid + each aggregate's persisted payload
///     not found: retraction of an unknown group -> next; else fresh state
///   for each aggregate: w > 0 ? AggStep(arg) : AggInverse(arg)
///   if hidden COUNT(*) == 0:  delete state row + index entry + view row
///   else:                     (new group: allocate state and view rowids)
///                             persist payloads, upsert state row,
///                             write the group's view row (outputs + weight 1)
/// ```
#[allow(clippy::too_many_arguments)]
fn compile_group_aggregate_program(
    view_name: &str,
    select: &ast::Select,
    shape: &ViewShape,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    use crate::function::AccumulatorFunc;

    let ViewShape::GroupAggregate {
        group_exprs,
        aggregates,
        outputs,
    } = shape
    else {
        unreachable!("compile_group_aggregate_program requires a GroupAggregate shape");
    };
    let ast::OneSelect::Select {
        from, where_clause, ..
    } = &select.body.select
    else {
        unreachable!("classify_view admits only OneSelect::Select");
    };
    let from = from.as_ref().expect("classify_view requires a FROM clause");
    let ast::SelectTable::Table(base_name, base_alias, _) = from.select.as_ref() else {
        unreachable!("classify_view admits only plain tables in FROM");
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

    turso_assert!(
        num_view_columns == outputs.len(),
        "view column count does not match classified outputs"
    );
    let k = group_exprs.len();
    let payload_widths: Vec<usize> = aggregates
        .iter()
        .map(|spec| {
            crate::vdbe::execute::agg_payload_width(&spec.func)
                .expect("classify_view admits only fixed-width aggregates")
        })
        .collect();
    let total_payload: usize = payload_widths.iter().sum();

    // The state table and its primary-key index are real schema objects
    // created by CREATE MATERIALIZED VIEW.
    let state_table_name = format!(
        "{}{}_{view_name}",
        crate::schema::DBSP_TABLE_PREFIX,
        crate::incremental::view::DBSP_CIRCUIT_VERSION
    );
    let state_table = schema.get_btree_table(&state_table_name).ok_or_else(|| {
        LimboError::InternalError(format!(
            "state table {state_table_name} of materialized view {view_name} not found"
        ))
    })?;
    let state_index = schema
        .get_indices(&state_table_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "state table {state_table_name} of materialized view {view_name} has no index"
            ))
        })?;

    let mut program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts {
            num_cursors: 4,
            approx_num_insns: 64 + 16 * (num_view_columns + aggregates.len()),
            approx_num_labels: 16,
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

    // Input cursor: transaction delta or full base-table scan.
    let input_cursor_id = match input {
        MaintenanceInput::TransactionDelta => {
            let cursor_id = program.alloc_cursor_id_keyed(
                CursorKey::table(table_ref_id),
                CursorType::ViewDelta {
                    view_name: view_name.to_string(),
                    table: base_table,
                },
            );
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

    let state_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(state_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_cursor_id,
        root_page: RegisterOrLiteral::Literal(state_table.root_page),
        db: 0,
    });
    let state_index_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeIndex(state_index.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_index_cursor_id,
        root_page: RegisterOrLiteral::Literal(state_index.root_page),
        db: 0,
    });
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

    // Register layout.
    let weight_reg = program.alloc_register();
    let null_arg_reg = program.alloc_register(); // stays NULL: COUNT(*) argument
    let zero_reg = program.alloc_register();
    let found_reg = program.alloc_register();
    let cnt_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    let arg_reg = program.alloc_register();
    // Accumulators, one per aggregate (index 0 = hidden liveness COUNT(*)).
    let acc_start = program.alloc_registers(aggregates.len());
    // State record image: [g.., view_rowid, payloads..], contiguous for MakeRecord.
    let state_rec_start = program.alloc_registers(k + 1 + total_payload);
    let group_start = state_rec_start;
    let view_rowid_reg = state_rec_start + k;
    let payload_start = state_rec_start + k + 1;
    let payload_offsets: Vec<usize> = payload_widths
        .iter()
        .scan(0usize, |off, w| {
            let this = *off;
            *off += w;
            Some(this)
        })
        .collect();
    // Index key image: [g.., state_rowid], contiguous for IdxInsert/IdxDelete.
    let index_rec_start = program.alloc_registers(k + 1);
    let state_rowid_reg = index_rec_start + k;
    // View row image: [outputs.., weight].
    let out_start = program.alloc_registers(num_view_columns + 1);
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    let view_record_reg = program.alloc_register();

    program.emit_insn(Insn::Null {
        dest: null_arg_reg,
        dest_end: None,
    });
    program.emit_int(0, zero_reg);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let corrupt_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: input_cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    // Accumulators must start fresh for every input row.
    program.emit_insn(Insn::Null {
        dest: acc_start,
        dest_end: Some(acc_start + aggregates.len() - 1),
    });

    // Weight of the current input row.
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

    // WHERE: rows whose predicate is FALSE or NULL do not reach the group.
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

    // Group key expressions.
    for (i, group_expr) in group_exprs.iter().enumerate() {
        let mut bound = group_expr.clone();
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
            group_start + i,
            &resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }

    // Group lookup via the state table's primary-key index.
    let found_label = program.allocate_label();
    let apply_label = program.allocate_label();
    program.emit_insn(Insn::Found {
        cursor_id: state_index_cursor_id,
        target_pc: found_label,
        record_reg: group_start,
        num_regs: k,
    });
    // Not found: a retraction for a group the state does not know is a no-op
    // (matching the filter/project merge behavior for unknown rowids).
    program.emit_insn(Insn::Lt {
        lhs: weight_reg,
        rhs: zero_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Null {
        dest: view_rowid_reg,
        dest_end: None,
    });
    program.emit_int(0, found_reg);
    program.emit_insn(Insn::Goto {
        target_pc: apply_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_int(1, found_reg);
    program.emit_insn(Insn::IdxRowId {
        cursor_id: state_index_cursor_id,
        dest: state_rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: state_cursor_id,
        src_reg: state_rowid_reg,
        target_pc: corrupt_label,
    });
    program.emit_insn(Insn::Column {
        cursor_id: state_cursor_id,
        column: k,
        dest: view_rowid_reg,
        default: None,
    });
    for (i, spec) in aggregates.iter().enumerate() {
        for j in 0..payload_widths[i] {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: k + 1 + payload_offsets[i] + j,
                dest: payload_start + payload_offsets[i] + j,
                default: None,
            });
        }
        program.emit_insn(Insn::AggContextLoad {
            acc_reg: acc_start + i,
            payload_start_reg: payload_start + payload_offsets[i],
            func: AccumulatorFunc::Agg(spec.func.clone()),
        });
    }

    program.preassign_label_to_next_insn(apply_label);
    for (i, spec) in aggregates.iter().enumerate() {
        let col_reg = match &spec.arg {
            Some(arg_expr) => {
                let mut bound = arg_expr.clone();
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
                    arg_reg,
                    &resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                arg_reg
            }
            None => null_arg_reg,
        };
        let step_label = program.allocate_label();
        let after_label = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: step_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::AggInverse {
            acc_reg: acc_start + i,
            col: col_reg,
            delimiter: 0,
            func: AccumulatorFunc::Agg(spec.func.clone()),
            comparator: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: after_label,
        });
        program.preassign_label_to_next_insn(step_label);
        program.emit_insn(Insn::AggStep {
            acc_reg: acc_start + i,
            col: col_reg,
            delimiter: 0,
            func: AccumulatorFunc::Agg(spec.func.clone()),
            comparator: None,
        });
        program.preassign_label_to_next_insn(after_label);
    }

    // Group liveness: the hidden COUNT(*) is zero when every row of the
    // group has been retracted.
    program.emit_insn(Insn::AggValue {
        acc_reg: acc_start,
        dest_reg: cnt_reg,
        func: AccumulatorFunc::Agg(AggFunc::Count0),
    });
    let write_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: cnt_reg,
        rhs: zero_reg,
        target_pc: write_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    // Group emptied: remove its state row, index entry, and view row. A
    // fresh group cannot reach zero (its first change was an insert), so
    // found_reg is always set here; stay total anyway.
    let do_delete_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: do_delete_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: next_label,
    });
    program.preassign_label_to_next_insn(do_delete_label);
    if k > 1 {
        program.emit_insn(Insn::Copy {
            src_reg: group_start,
            dst_reg: index_rec_start,
            extra_amount: k - 1,
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: group_start,
            dst_reg: index_rec_start,
            extra_amount: 0,
        });
    }
    program.emit_insn(Insn::IdxDelete {
        start_reg: index_rec_start,
        num_regs: k + 1,
        cursor_id: state_index_cursor_id,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: state_cursor_id,
        table_name: state_table_name.clone(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::IsNull {
        reg: view_rowid_reg,
        target_pc: next_label,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: view_cursor_id,
        src_reg: view_rowid_reg,
        target_pc: corrupt_label,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: view_cursor_id,
        table_name: view_name.to_string(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Goto {
        target_pc: next_label,
    });

    // Group live: persist aggregate state and (re)write the view row.
    program.preassign_label_to_next_insn(write_label);
    let have_rowids_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: have_rowids_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: state_cursor_id,
        rowid_reg: state_rowid_reg,
        prev_largest_reg: prev_rowid_scratch,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: view_cursor_id,
        rowid_reg: view_rowid_reg,
        prev_largest_reg: prev_rowid_scratch,
    });
    program.preassign_label_to_next_insn(have_rowids_label);
    for (i, spec) in aggregates.iter().enumerate() {
        program.emit_insn(Insn::AggContextStore {
            acc_reg: acc_start + i,
            payload_start_reg: payload_start + payload_offsets[i],
            func: AccumulatorFunc::Agg(spec.func.clone()),
        });
    }
    program.emit_insn(Insn::MakeRecord {
        start_reg: state_rec_start as u16,
        count: (k + 1 + total_payload) as u16,
        dest_reg: state_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: state_cursor_id,
        key_reg: state_rowid_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: state_table_name,
    });
    let skip_index_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: skip_index_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Copy {
        src_reg: group_start,
        dst_reg: index_rec_start,
        extra_amount: k.saturating_sub(1),
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: index_rec_start as u16,
        count: (k + 1) as u16,
        dest_reg: index_record_reg as u16,
        index_name: Some(state_index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: state_index_cursor_id,
        record_reg: index_record_reg,
        unpacked_start: Some(index_rec_start),
        unpacked_count: Some((k + 1) as u16),
        flags: IdxInsertFlags::new(),
    });
    program.preassign_label_to_next_insn(skip_index_label);

    for (out_idx, output) in outputs.iter().enumerate() {
        match output {
            OutputColumn::Group(group_idx) => {
                program.emit_insn(Insn::Copy {
                    src_reg: group_start + group_idx,
                    dst_reg: out_start + out_idx,
                    extra_amount: 0,
                });
            }
            OutputColumn::Aggregate(agg_idx) => {
                program.emit_insn(Insn::AggValue {
                    acc_reg: acc_start + agg_idx,
                    dest_reg: out_start + out_idx,
                    func: AccumulatorFunc::Agg(aggregates[*agg_idx].func.clone()),
                });
            }
        }
    }
    program.emit_int(1, out_start + num_view_columns);
    program.emit_insn(Insn::MakeRecord {
        start_reg: out_start as u16,
        count: (num_view_columns + 1) as u16,
        dest_reg: view_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: view_cursor_id,
        key_reg: view_rowid_reg,
        record_reg: view_record_reg,
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
    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} state is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);

    program.epilogue(schema);
    drop(syms);
    program.build(
        connection.clone(),
        false,
        "materialized view aggregate maintenance",
    )
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
