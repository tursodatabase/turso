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
    Aggregate, ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences,
};
use crate::translate::planner::resolve_window_and_aggregate_functions;
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

/// Whether an aggregate's accumulator must see each distinct argument value
/// exactly once, tracked through the value multiset table. MIN/MAX see each
/// value once by definition, so DISTINCT changes nothing for them — their
/// multiset participation is about extremes, not distinctness.
fn tracks_distinct_values(agg: &Aggregate) -> bool {
    agg.distinctness.is_distinct() && !matches!(agg.func, AggFunc::Min | AggFunc::Max)
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
#[allow(clippy::large_enum_variant)]
pub enum ViewShape {
    /// Single-table SELECT with optional WHERE: rows map 1:1 to base rows.
    FilterProject,
    /// Single-table GROUP BY with invertible aggregates.
    GroupAggregate {
        group_exprs: Vec<ast::Expr>,
        /// `aggregates[0]` is always a hidden COUNT(*) tracking group
        /// liveness (a group's view row exists iff its row count > 0).
        /// Aggregates appearing only in HAVING are also collected here.
        aggregates: Vec<Aggregate>,
        outputs: Vec<OutputColumn>,
        /// HAVING predicate over group expressions and aggregate results;
        /// gates only the view row, not the group's persisted state.
        having: Option<ast::Expr>,
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

/// Reject collected aggregates the maintenance codegen cannot invert.
///
/// Aggregate recognition and deduplication use the planner's own collector
/// ([`resolve_window_and_aggregate_functions`]); this is only the IVM gate on
/// top of it.
fn validate_supported_aggregate(agg: &Aggregate) -> Result<()> {
    if agg.filter_expr.is_some() {
        return unsupported("FILTER clauses on aggregates");
    }
    match agg.func {
        AggFunc::Count0
        | AggFunc::Count
        | AggFunc::Sum
        | AggFunc::Total
        | AggFunc::Avg
        | AggFunc::Min
        | AggFunc::Max => {}
        // GROUP_CONCAT and friends have no inverse and no multiset-based
        // retraction strategy.
        ref other => return unsupported(&format!("the {} aggregate", other.as_str())),
    }
    if agg.args.len() > 1 {
        return unsupported("multi-argument aggregates");
    }
    for arg in &agg.args {
        check_scalar_expr(arg)?;
    }
    Ok(())
}

/// Validate that a HAVING expression evaluates only group expressions and
/// aggregate results.
///
/// The aggregates themselves were already collected into `aggregates` by the
/// planner's collector; this walk rejects what remains outside them: bare
/// column references (SQLite's arbitrary-row semantics cannot be maintained
/// incrementally), subqueries, and bound parameters.
fn check_having_scalar_context(
    expr: &ast::Expr,
    group_exprs: &[ast::Expr],
    aggregates: &[Aggregate],
) -> Result<()> {
    walk_expr_with_subqueries(expr, &mut |e: &ast::Expr| {
        if group_exprs
            .iter()
            .any(|g| crate::util::exprs_are_equivalent(g, e))
            || aggregates
                .iter()
                .any(|a| crate::util::exprs_are_equivalent(&a.original_expr, e))
        {
            return Ok(WalkControl::SkipChildren);
        }
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                Err(LimboError::ParseError(
                    "materialized views with subqueries are not yet supported".to_string(),
                ))
            }
            ast::Expr::Variable(_) => Err(LimboError::ParseError(
                "materialized views with bound parameters are not supported".to_string(),
            )),
            ast::Expr::Id(_) | ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                unsupported("HAVING referencing columns outside GROUP BY")
            }
            _ => Ok(WalkControl::Continue),
        }
    })
}

/// Classify a view definition into its maintainable shape.
///
/// This is the CREATE MATERIALIZED VIEW gate: shapes outside the supported
/// set are rejected outright rather than silently mis-maintained. The set
/// grows as codegen for more operators lands (MIN/MAX, joins, set ops).
///
/// Aggregate recognition uses the planner's collector so it cannot drift
/// from what a regular GROUP BY query would compute; the `resolver` is what
/// the collector needs to resolve (and here, reject) extension functions.
pub fn classify_view(select: &ast::Select, resolver: &Resolver) -> Result<ViewShape> {
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
                let mut aggs = Vec::new();
                if resolve_window_and_aggregate_functions(expr, resolver, &mut aggs, None, &mut [])?
                {
                    return unsupported("aggregates without GROUP BY");
                }
                check_scalar_expr(expr)?;
            }
        }
        return Ok(ViewShape::FilterProject);
    };

    let group_exprs: Vec<ast::Expr> = group_by.exprs.iter().map(|e| e.as_ref().clone()).collect();
    for g in &group_exprs {
        check_scalar_expr(g)?;
    }

    // aggregates[0] is the hidden liveness COUNT(*); its expression form lets
    // an explicit COUNT(*) in the SELECT list or HAVING dedup onto the same
    // accumulator through the collector.
    let count_star = ast::Expr::FunctionCallStar {
        name: ast::Name::exact("count".to_string()),
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    let mut aggregates = Vec::new();
    resolve_window_and_aggregate_functions(&count_star, resolver, &mut aggregates, None, &mut [])?;
    turso_assert!(
        aggregates.len() == 1 && matches!(aggregates[0].func, AggFunc::Count0),
        "count(*) must collect as the single hidden liveness aggregate"
    );

    let mut outputs = Vec::with_capacity(columns.len());
    for column in columns {
        let ast::ResultColumn::Expr(expr, _) = column else {
            return unsupported("star projections combined with GROUP BY");
        };
        if let Some(idx) = group_exprs
            .iter()
            .position(|g| crate::util::exprs_are_equivalent(g, expr))
        {
            outputs.push(OutputColumn::Group(idx));
            continue;
        }
        resolve_window_and_aggregate_functions(expr, resolver, &mut aggregates, None, &mut [])?;
        let Some(idx) = aggregates
            .iter()
            .position(|a| crate::util::exprs_are_equivalent(&a.original_expr, expr))
        else {
            // The column is not one aggregate call: either an expression over
            // aggregate results, or SQLite's bare-column semantics (an
            // arbitrary row of the group) — neither is maintainable yet.
            check_scalar_expr(expr)?;
            return unsupported("non-aggregate result columns not in GROUP BY");
        };
        outputs.push(OutputColumn::Aggregate(idx));
    }

    let having = match &group_by.having {
        Some(having) => {
            resolve_window_and_aggregate_functions(
                having,
                resolver,
                &mut aggregates,
                None,
                &mut [],
            )?;
            check_having_scalar_context(having, &group_exprs, &aggregates)?;
            Some(having.as_ref().clone())
        }
        None => None,
    };

    for agg in &aggregates {
        validate_supported_aggregate(agg)?;
    }

    Ok(ViewShape::GroupAggregate {
        group_exprs,
        aggregates,
        outputs,
        having,
    })
}

/// [`classify_view`] with a transient resolver built from a connection, for
/// callers outside the translate layer (maintenance compilation, cursors).
pub fn classify_view_for_connection(
    select: &ast::Select,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<ViewShape> {
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
    classify_view(select, &resolver)
}

/// The CREATE TABLE statement for a GROUP BY view's internal state table:
/// one row per live group, holding the group-key values and each payload
/// aggregate's persisted state. The PRIMARY KEY over the group columns gives
/// the automatic index used to find a group's state. MIN/MAX aggregates
/// contribute no columns here — their state is the value multiset table.
///
/// The state row's rowid doubles as the group's rowid in the view btree:
/// state rows are written unconditionally for live groups, so their rowids
/// are unique, stable, and — unlike a separately reserved rowid — cannot
/// collide when HAVING suppresses a group's view row.
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
    for (i, agg) in aggregates.iter().enumerate() {
        if let Some(width) = crate::vdbe::execute::agg_payload_width(&agg.func) {
            for j in 0..width {
                columns.push(format!("a{i}_{j}"));
            }
        }
    }
    let key: Vec<String> = (0..group_exprs.len()).map(|i| format!("g{i}")).collect();
    Ok(format!(
        "CREATE TABLE {state_table_name} ({}, PRIMARY KEY ({}))",
        columns.join(", "),
        key.join(", ")
    ))
}

/// Whether an aggregate keeps per-value state in the multiset table: MIN/MAX
/// (to find the next extreme after a retraction) and DISTINCT aggregates (to
/// step the accumulator only on first occurrence of a value).
fn uses_multiset(agg: &Aggregate) -> bool {
    matches!(agg.func, AggFunc::Min | AggFunc::Max) || tracks_distinct_values(agg)
}

/// Whether the view needs the value-multiset table.
pub fn needs_multiset_table(shape: &ViewShape) -> bool {
    matches!(shape, ViewShape::GroupAggregate { aggregates, .. }
        if aggregates.iter().any(uses_multiset))
}

/// The CREATE TABLE statement for a GROUP BY view's value multiset: one row
/// per (aggregate, group, distinct value) with its multiplicity. For MIN/MAX
/// the PRIMARY KEY ordering makes a group's extreme value an index seek, so
/// retracting the current extreme never needs a rescan in Rust; for DISTINCT
/// aggregates the multiplicity decides when a value enters or leaves the
/// accumulator.
pub fn multiset_table_sql(multiset_table_name: &str, shape: &ViewShape) -> Result<String> {
    let ViewShape::GroupAggregate { group_exprs, .. } = shape else {
        return Err(LimboError::InternalError(
            "only GROUP BY views have multiset tables".to_string(),
        ));
    };
    let mut key = vec!["agg_id".to_string()];
    for i in 0..group_exprs.len() {
        key.push(format!("g{i}"));
    }
    key.push("val".to_string());
    Ok(format!(
        "CREATE TABLE {multiset_table_name} ({}, mult, PRIMARY KEY ({}))",
        key.join(", "),
        key.join(", ")
    ))
}

/// Validation of multiset-tracked arguments that needs the schema: the
/// multiset index compares values with default (binary) collation, so
/// MIN/MAX ordering and DISTINCT equality over arguments carrying a
/// collation would be maintained under the wrong comparison. Must run at
/// translate time — rejecting later, inside the CREATE program, corrupts
/// the database (see validate_and_extract_columns).
pub fn validate_multiset_args(
    select: &ast::Select,
    shape: &ViewShape,
    schema: &Schema,
) -> Result<()> {
    let ViewShape::GroupAggregate { aggregates, .. } = shape else {
        return Ok(());
    };
    let ast::OneSelect::Select { from, .. } = &select.body.select else {
        return Ok(());
    };
    let Some(from) = from else { return Ok(()) };
    let ast::SelectTable::Table(base_name, _, _) = from.select.as_ref() else {
        return Ok(());
    };
    let Some(base_table) = schema.get_btree_table(base_name.name.as_str()) else {
        return Ok(());
    };

    for agg in aggregates {
        if !uses_multiset(agg) {
            continue;
        }
        let Some(arg) = agg.args.first() else {
            continue;
        };
        // Explicit COLLATE anywhere in the argument.
        walk_expr_with_subqueries(arg, &mut |e: &ast::Expr| {
            if matches!(e, ast::Expr::Collate(_, _)) {
                return unsupported("MIN/MAX or DISTINCT aggregates over collated expressions");
            }
            Ok(WalkControl::Continue)
        })?;
        // A bare column reference with a declared collation.
        if let ast::Expr::Id(name) = arg {
            let collated = base_table
                .columns()
                .iter()
                .any(|c| c.name.as_deref() == Some(name.as_str()) && c.collation_opt().is_some());
            if collated {
                return unsupported("MIN/MAX or DISTINCT aggregates over collated columns");
            }
        }
    }
    Ok(())
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
    match classify_view_for_connection(select, schema, connection)? {
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
///     found:     load each payload aggregate's persisted state
///     not found: retraction of an unknown group -> next; else fresh state
///   for each aggregate: w > 0 ? AggStep(arg) : AggInverse(arg)
///     (multiset aggregates weight-merge (agg, g.., value) rows instead;
///      DISTINCT accumulators step/invert only on 0 <-> positive transitions)
///   if hidden COUNT(*) == 0:  delete state row + index entry + view row
///   else:                     (new group: allocate the state rowid, which
///                             doubles as the group's view-btree rowid)
///                             persist payloads, upsert state row,
///                             finalize aggregate values,
///                             HAVING true -> write the view row,
///                             HAVING false/NULL -> delete it if present
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
        having,
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
    // Payload aggregates persist fixed-width state in the group state table;
    // MIN/MAX keep a value multiset in the multiset table instead
    // (payload_widths[i] is None for them, and they have no accumulator).
    // DISTINCT aggregates have both: the accumulator payload plus multiset
    // rows deciding which values reach the accumulator.
    let payload_widths: Vec<Option<usize>> = aggregates
        .iter()
        .map(|agg| crate::vdbe::execute::agg_payload_width(&agg.func))
        .collect();
    let total_payload: usize = payload_widths.iter().flatten().sum();
    // Offset of each payload aggregate's slots within the payload block.
    let payload_offsets: Vec<Option<usize>> = payload_widths
        .iter()
        .scan(0usize, |off, w| {
            Some(w.map(|w| {
                let this = *off;
                *off += w;
                this
            }))
        })
        .collect();
    let has_multiset = needs_multiset_table(shape);

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

    // The value multiset table, when the view has MIN/MAX or DISTINCT
    // aggregates.
    let multiset_table_name = format!(
        "{}{}_{view_name}",
        crate::schema::DBSP_MULTISET_TABLE_PREFIX,
        crate::incremental::view::DBSP_CIRCUIT_VERSION
    );
    let mm_schema = if has_multiset {
        let table = schema
            .get_btree_table(&multiset_table_name)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                "multiset table {multiset_table_name} of materialized view {view_name} not found"
            ))
            })?;
        let index = schema
            .get_indices(&multiset_table_name)
            .next()
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "multiset table {multiset_table_name} of materialized view {view_name} has no index"
                ))
            })?;
        Some((table, index))
    } else {
        None
    };

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
    let mut resolver = Resolver::new(
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

    let mut mm_cursors = None;
    let mut mm_index_name = String::new();
    if let Some((mm_table, mm_index)) = &mm_schema {
        let mm_table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(mm_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: mm_table_cursor_id,
            root_page: RegisterOrLiteral::Literal(mm_table.root_page),
            db: 0,
        });
        let mm_index_cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(mm_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: mm_index_cursor_id,
            root_page: RegisterOrLiteral::Literal(mm_index.root_page),
            db: 0,
        });
        mm_cursors = Some((mm_table_cursor_id, mm_index_cursor_id));
        mm_index_name.clone_from(&mm_index.name);
    }

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
    // State record image: [g.., payloads..], contiguous for MakeRecord.
    let state_rec_start = program.alloc_registers(k + total_payload);
    let group_start = state_rec_start;
    let payload_start = state_rec_start + k;
    // Index key image: [g.., state_rowid], contiguous for IdxInsert/IdxDelete.
    // The state rowid doubles as the group's view-btree rowid (see
    // aggregate_state_table_sql).
    let index_rec_start = program.alloc_registers(k + 1);
    let state_rowid_reg = index_rec_start + k;
    // Finalized value of each aggregate for the current group; group outputs
    // and HAVING both read from here.
    let agg_value_start = program.alloc_registers(aggregates.len());
    // View row image: [outputs.., weight].
    let out_start = program.alloc_registers(num_view_columns + 1);
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    let view_record_reg = program.alloc_register();
    // Multiset record image: [agg_id, g.., val, mult-or-rowid], contiguous.
    let mm_rec_start = program.alloc_registers(1 + k + 2);
    let mm_val_reg = mm_rec_start + 1 + k;
    let mm_last_reg = mm_rec_start + 1 + k + 1;
    let mm_rowid_reg = program.alloc_register();
    let mm_mult_reg = program.alloc_register();
    let mm_record_reg = program.alloc_register();
    let mm_found_reg = program.alloc_register();

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
    program.emit_int(0, found_reg);
    // A fresh group's DISTINCT accumulators may never be stepped in the apply
    // loop (a NULL argument contributes nothing), but the write path stores
    // every payload accumulator, so materialize empty contexts now. Stepping
    // a NULL argument initializes the context without contributing to it.
    for (i, agg) in aggregates.iter().enumerate() {
        if tracks_distinct_values(agg) {
            program.emit_insn(Insn::AggStep {
                acc_reg: acc_start + i,
                col: null_arg_reg,
                delimiter: 0,
                func: AccumulatorFunc::Agg(agg.func.clone()),
                comparator: None,
            });
        }
    }
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
    for (i, agg) in aggregates.iter().enumerate() {
        let Some(offset) = payload_offsets[i] else {
            continue; // MIN/MAX: state lives in the multiset table
        };
        for j in 0..payload_widths[i].expect("offset implies width") {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: k + offset + j,
                dest: payload_start + offset + j,
                default: None,
            });
        }
        program.emit_insn(Insn::AggContextLoad {
            acc_reg: acc_start + i,
            payload_start_reg: payload_start + offset,
            func: AccumulatorFunc::Agg(agg.func.clone()),
        });
    }

    program.preassign_label_to_next_insn(apply_label);
    for (i, agg) in aggregates.iter().enumerate() {
        let is_minmax = matches!(agg.func, AggFunc::Min | AggFunc::Max);
        let is_distinct = tracks_distinct_values(agg);
        let col_reg = match agg.args.first() {
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

        if is_minmax || is_distinct {
            // Weight-merge the value into the aggregate's multiset:
            // (agg_id, group.., value) -> multiplicity. NULLs are ignored,
            // matching aggregate NULL semantics. For a DISTINCT aggregate the
            // accumulator steps only when a value's multiplicity transitions
            // 0 -> positive and inverts only on positive -> 0, so it sees
            // each distinct value exactly once. Transition detection relies
            // on delta rows carrying unit weights (each captured DML op is
            // one row of weight +1 or -1, and population scans weigh +1).
            let (mm_table_cursor_id, mm_index_cursor_id) = mm_cursors
                .expect("classify_view guarantees the multiset table for multiset aggregates");
            let done_label = program.allocate_label();
            let mm_found_label = program.allocate_label();
            let mm_upsert_label = program.allocate_label();

            program.emit_insn(Insn::IsNull {
                reg: col_reg,
                target_pc: done_label,
            });
            // Key image: [agg_id, g.., val].
            program.emit_int(i as i64, mm_rec_start);
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: mm_rec_start + 1,
                extra_amount: k.saturating_sub(1),
            });
            program.emit_insn(Insn::Copy {
                src_reg: col_reg,
                dst_reg: mm_val_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Found {
                cursor_id: mm_index_cursor_id,
                target_pc: mm_found_label,
                record_reg: mm_rec_start,
                num_regs: 1 + k + 1,
            });
            // Not found: retraction of an unknown value is a no-op.
            program.emit_insn(Insn::Lt {
                lhs: weight_reg,
                rhs: zero_reg,
                target_pc: done_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            program.emit_int(0, mm_found_reg);
            program.emit_insn(Insn::NewRowid {
                cursor: mm_table_cursor_id,
                rowid_reg: mm_rowid_reg,
                prev_largest_reg: prev_rowid_scratch,
            });
            program.emit_insn(Insn::Copy {
                src_reg: weight_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            if is_distinct {
                // First occurrence of this value in the group.
                program.emit_insn(Insn::AggStep {
                    acc_reg: acc_start + i,
                    col: mm_val_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::Goto {
                target_pc: mm_upsert_label,
            });

            program.preassign_label_to_next_insn(mm_found_label);
            program.emit_int(1, mm_found_reg);
            program.emit_insn(Insn::IdxRowId {
                cursor_id: mm_index_cursor_id,
                dest: mm_rowid_reg,
            });
            program.emit_insn(Insn::SeekRowid {
                cursor_id: mm_table_cursor_id,
                src_reg: mm_rowid_reg,
                target_pc: corrupt_label,
            });
            program.emit_insn(Insn::Column {
                cursor_id: mm_table_cursor_id,
                column: 1 + k + 1,
                dest: mm_mult_reg,
                default: None,
            });
            program.emit_insn(Insn::Add {
                lhs: weight_reg,
                rhs: mm_mult_reg,
                dest: mm_last_reg,
            });
            program.emit_insn(Insn::Ne {
                lhs: mm_last_reg,
                rhs: zero_reg,
                target_pc: mm_upsert_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            // Multiplicity reached zero: remove the value.
            if is_distinct {
                // Last occurrence of this value left the group.
                program.emit_insn(Insn::AggInverse {
                    acc_reg: acc_start + i,
                    col: mm_val_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: mm_rowid_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: mm_rec_start,
                num_regs: 1 + k + 2,
                cursor_id: mm_index_cursor_id,
                raise_error_if_no_matching_entry: true,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: mm_table_cursor_id,
                table_name: multiset_table_name.clone(),
                is_part_of_update: true,
            });
            program.emit_insn(Insn::Goto {
                target_pc: done_label,
            });

            // Write (agg_id, g.., val, mult) at the row's rowid; a fresh
            // value also gets an index entry (agg_id, g.., val, rowid).
            program.preassign_label_to_next_insn(mm_upsert_label);
            program.emit_insn(Insn::MakeRecord {
                start_reg: mm_rec_start as u16,
                count: (1 + k + 2) as u16,
                dest_reg: mm_record_reg as u16,
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::Insert {
                cursor: mm_table_cursor_id,
                key_reg: mm_rowid_reg,
                record_reg: mm_record_reg,
                flag: InsertFlags(
                    InsertFlags::REQUIRE_SEEK
                        | InsertFlags::SKIP_LAST_ROWID
                        | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
                ),
                table_name: multiset_table_name.clone(),
            });
            // Only a freshly-inserted value needs an index entry; an
            // existing value's entry is already present.
            let mm_skip_idx_label = program.allocate_label();
            program.emit_insn(Insn::If {
                reg: mm_found_reg,
                target_pc: mm_skip_idx_label,
                jump_if_null: false,
            });
            program.emit_insn(Insn::Copy {
                src_reg: mm_rowid_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MakeRecord {
                start_reg: mm_rec_start as u16,
                count: (1 + k + 2) as u16,
                dest_reg: mm_record_reg as u16,
                index_name: Some(mm_index_name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: mm_index_cursor_id,
                record_reg: mm_record_reg,
                unpacked_start: Some(mm_rec_start),
                unpacked_count: Some((1 + k + 2) as u16),
                flags: IdxInsertFlags::new(),
            });
            program.preassign_label_to_next_insn(mm_skip_idx_label);
            program.preassign_label_to_next_insn(done_label);
            continue;
        }

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
            func: AccumulatorFunc::Agg(agg.func.clone()),
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
            func: AccumulatorFunc::Agg(agg.func.clone()),
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
    // Without HAVING every live group has its view row, so a missing row is
    // corruption; with HAVING the dying group may have been suppressed.
    program.emit_insn(Insn::SeekRowid {
        cursor_id: view_cursor_id,
        src_reg: state_rowid_reg,
        target_pc: if having.is_some() {
            next_label
        } else {
            corrupt_label
        },
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
    program.preassign_label_to_next_insn(have_rowids_label);
    for (i, agg) in aggregates.iter().enumerate() {
        let Some(offset) = payload_offsets[i] else {
            continue; // MIN/MAX: state lives in the multiset table
        };
        program.emit_insn(Insn::AggContextStore {
            acc_reg: acc_start + i,
            payload_start_reg: payload_start + offset,
            func: AccumulatorFunc::Agg(agg.func.clone()),
        });
    }
    program.emit_insn(Insn::MakeRecord {
        start_reg: state_rec_start as u16,
        count: (k + total_payload) as u16,
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

    // Finalize every aggregate's value for the group; output columns and
    // HAVING both read from this block.
    for (i, agg) in aggregates.iter().enumerate() {
        let value_reg = agg_value_start + i;
        if matches!(agg.func, AggFunc::Min | AggFunc::Max) {
            // The group's extreme is the first (MIN) or last (MAX) multiset
            // entry with the (agg_id, group..) prefix.
            let (_, mm_index_cursor_id) =
                mm_cursors.expect("MIN/MAX aggregates imply the multiset table");
            let empty_label = program.allocate_label();
            let have_label = program.allocate_label();
            program.emit_int(i as i64, mm_rec_start);
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: mm_rec_start + 1,
                extra_amount: k.saturating_sub(1),
            });
            match agg.func {
                AggFunc::Min => {
                    program.emit_insn(Insn::SeekGE {
                        is_index: true,
                        cursor_id: mm_index_cursor_id,
                        start_reg: mm_rec_start,
                        num_regs: 1 + k,
                        target_pc: empty_label,
                        eq_only: false,
                    });
                    // Positioned past the group: no values.
                    program.emit_insn(Insn::IdxGT {
                        cursor_id: mm_index_cursor_id,
                        start_reg: mm_rec_start,
                        num_regs: 1 + k,
                        target_pc: empty_label,
                    });
                }
                AggFunc::Max => {
                    program.emit_insn(Insn::SeekLE {
                        is_index: true,
                        cursor_id: mm_index_cursor_id,
                        start_reg: mm_rec_start,
                        num_regs: 1 + k,
                        target_pc: empty_label,
                        eq_only: false,
                    });
                    // Positioned before the group: no values.
                    program.emit_insn(Insn::IdxLT {
                        cursor_id: mm_index_cursor_id,
                        start_reg: mm_rec_start,
                        num_regs: 1 + k,
                        target_pc: empty_label,
                    });
                }
                _ => unreachable!(),
            }
            program.emit_insn(Insn::Column {
                cursor_id: mm_index_cursor_id,
                column: 1 + k,
                dest: value_reg,
                default: None,
            });
            program.emit_insn(Insn::Goto {
                target_pc: have_label,
            });
            program.preassign_label_to_next_insn(empty_label);
            program.emit_insn(Insn::Null {
                dest: value_reg,
                dest_end: None,
            });
            program.preassign_label_to_next_insn(have_label);
        } else {
            program.emit_insn(Insn::AggValue {
                acc_reg: acc_start + i,
                dest_reg: value_reg,
                func: AccumulatorFunc::Agg(agg.func.clone()),
            });
        }
    }

    // HAVING gates only the view row: the group's state stays maintained
    // above so the predicate flips the row in and out as aggregates change.
    // Group expressions and aggregate calls inside the predicate resolve to
    // their computed registers through the expression cache, exactly like
    // HAVING translation in a regular GROUP BY query.
    let suppress_label = having.as_ref().map(|_| program.allocate_label());
    if let Some(having_expr) = having {
        let suppress_label = suppress_label.expect("allocated above");
        for (i, group_expr) in group_exprs.iter().enumerate() {
            let mut bound = group_expr.clone();
            bind_and_rewrite_expr(
                &mut bound,
                Some(&mut table_references),
                None,
                &resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
            resolver.cache_expr_reg(std::borrow::Cow::Owned(bound), group_start + i, false, None);
        }
        for (i, agg) in aggregates.iter().enumerate() {
            let mut bound = agg.original_expr.clone();
            bind_and_rewrite_expr(
                &mut bound,
                Some(&mut table_references),
                None,
                &resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
            resolver.cache_expr_reg(
                std::borrow::Cow::Owned(bound),
                agg_value_start + i,
                false,
                None,
            );
        }
        resolver.enable_expr_to_reg_cache();
        let mut bound = having_expr.clone();
        bind_and_rewrite_expr(
            &mut bound,
            Some(&mut table_references),
            None,
            &resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        let pass_label = program.allocate_label();
        translate_condition_expr(
            &mut program,
            &table_references,
            &bound,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: pass_label,
                jump_target_when_false: suppress_label,
                jump_target_when_null: suppress_label,
            },
            &resolver,
        )?;
        program.preassign_label_to_next_insn(pass_label);
    }

    for (out_idx, output) in outputs.iter().enumerate() {
        let src_reg = match output {
            OutputColumn::Group(group_idx) => group_start + group_idx,
            OutputColumn::Aggregate(agg_idx) => agg_value_start + agg_idx,
        };
        program.emit_insn(Insn::Copy {
            src_reg,
            dst_reg: out_start + out_idx,
            extra_amount: 0,
        });
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
        key_reg: state_rowid_reg,
        record_reg: view_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: view_name.to_string(),
    });

    if let Some(suppress_label) = suppress_label {
        // Group failed HAVING: remove its previously-visible row, if any.
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });
        program.preassign_label_to_next_insn(suppress_label);
        program.emit_insn(Insn::SeekRowid {
            cursor_id: view_cursor_id,
            src_reg: state_rowid_reg,
            target_pc: next_label,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: view_cursor_id,
            table_name: view_name.to_string(),
            is_part_of_update: true,
        });
    }

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
