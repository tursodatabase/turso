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
use crate::incremental::dag;
use crate::incremental::dbsp::Delta;
use crate::schema::{BTreeTable, Schema};
use crate::translate::collate::{get_collseq_from_expr_with_symbols, CollationSeq};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    bind_and_rewrite_expr, translate_condition_expr, translate_expr_no_constant_opt,
    BindingBehavior, ConditionMetadata, NoConstantOptReason, WalkControl,
};
use crate::translate::plan::{
    Aggregate, ColumnUsedMask, IterationDirection, JoinInfo, JoinedTable, Operation, Scan,
    TableReferences,
};
use crate::translate::planner::resolve_window_and_aggregate_functions;
use crate::turso_assert;
use crate::util::walk_expr_with_subqueries;
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::Program;
use crate::{Connection, LimboError, QueryMode, Result, Value};
use turso_parser::ast::TableInternalId;

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
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum OutputColumn {
    /// The i-th GROUP BY expression.
    Group(usize),
    /// The i-th entry of [`ViewShape::GroupAggregate::aggregates`].
    Aggregate(usize),
    /// An expression over aggregate results and group expressions
    /// (e.g. `SUM(x) + 1`), evaluated after the group's aggregates are
    /// finalized; its aggregate calls and group-expression subtrees resolve
    /// to their computed registers through the expression cache, exactly
    /// like HAVING.
    Expr(ast::Expr),
}

/// Maximum number of tables in a join view: maintenance runs one delta phase
/// per non-empty subset of the joined tables, so programs grow as 2^N - 1.
pub const MAX_JOIN_TABLES: usize = 4;

/// The maintainable shape of a view definition, decided at CREATE time.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ViewShape {
    /// Single-table SELECT with optional WHERE: rows map 1:1 to base rows.
    FilterProject,
    /// Join of up to [`MAX_JOIN_TABLES`] tables with arbitrary ON
    /// predicates. Join rows are identified by their source-rowid tuple
    /// through a hidden state table whose rowid doubles as the view rowid
    /// and whose signed multiplicity absorbs transient dips from the delta
    /// phase decomposition.
    Join {
        /// Number of joined tables (at least two).
        n_tables: usize,
        /// A two-table LEFT JOIN. The auxiliary table tracks each left
        /// row's presence and its count of ON-matches: a NULL-padded view
        /// row exists while the row is present with zero matches (and the
        /// WHERE clause passes over the padded image). Inner rows keep the
        /// pair-map machinery; the two tables' rowids map to disjoint view
        /// rowids.
        left_outer: bool,
    },
    /// Compound SELECT (UNION, UNION ALL, INTERSECT, EXCEPT) over
    /// filter/project branches. Every operator except UNION ALL
    /// deduplicates, so a content row's membership in the result depends
    /// only on which branches currently produce it: visibility is a
    /// left-associative fold of per-branch presence (UNION and UNION ALL
    /// fold as OR, INTERSECT as AND, EXCEPT as AND NOT). Only a trailing
    /// run of UNION ALL operators appends rows with their multiplicity
    /// preserved, identified by (branch, source rowid).
    Compound {
        /// One operator per compound entry, in chain order.
        operators: Vec<ast::CompoundOperator>,
        /// Number of result columns (with stars expanded), which is also
        /// the width of the content key in the state table.
        arity: usize,
        /// Number of leading branches deduplicated by content: everything
        /// up to and including the right side of the last non-UNION-ALL
        /// operator. The state table keeps one signed count per prefix
        /// branch. Zero when the chain is UNION ALL throughout (every
        /// branch appends).
        prefix_len: usize,
        /// Collation each content-key column dedups under (left-most prefix
        /// branch with a resolved collation wins, matching compound dedup
        /// indexes). Carried onto the state table's key columns. Empty when
        /// `prefix_len` is zero.
        key_collations: Vec<CollationSeq>,
    },
    /// Single-table GROUP BY with invertible aggregates.
    GroupAggregate {
        group_exprs: Vec<ast::Expr>,
        /// Collation each group expression compares under (parallel to
        /// `group_exprs`), resolved the way regular GROUP BY translation
        /// resolves it. Carried onto the state table's key columns so the
        /// hidden index groups exactly like the batch query.
        group_collations: Vec<CollationSeq>,
        /// `aggregates[0]` is always a hidden COUNT(*) tracking group
        /// liveness (a group's view row exists iff its row count > 0).
        /// Aggregates appearing only in HAVING are also collected here.
        aggregates: Vec<Aggregate>,
        /// Collation of every multiset-tracked aggregate argument (MIN/MAX
        /// and DISTINCT aggregates), carried onto the multiset table's `val`
        /// column. One shared table stores all such values, so aggregates
        /// with differing argument collations are rejected at classify.
        multiset_collation: CollationSeq,
        /// HAVING predicate over group expressions and aggregate results;
        /// gates only the view row, not the group's persisted state.
        having: Option<ast::Expr>,
        /// Aggregates without GROUP BY: the single group, keyed by a
        /// synthetic constant group expression, exists even over an empty
        /// input — its row is created at population and never deleted, with
        /// empty-input aggregate values (COUNT 0, SUM NULL, ...) instead of
        /// group death.
        scalar: bool,
    },
    /// A pure UNION ALL whose branches are not all plain filter/project — at
    /// least one is a join or aggregate. UNION ALL branches are independent
    /// bags, so each is maintained by its own sub-program writing a disjoint
    /// rowid slice (`local * n_branches + branch_idx`) of the shared view
    /// btree, with its own hidden state tables suffixed `__b<idx>`. There is
    /// no cross-branch merge. (Pure UNION ALL over only filter/project
    /// branches, and any chain with a deduplicating operator, use
    /// [`ViewShape::Compound`] instead.)
    CompoundAll {
        /// Per-branch maintainable sub-shape, in chain order. Each is one of
        /// FilterProject, Join, or GroupAggregate.
        branch_shapes: Vec<ViewShape>,
    },
}

/// Build the standalone single-`SELECT` view for one branch of a compound
/// `SELECT`, so it can be classified and compiled with the ordinary
/// per-shape machinery.
fn branch_select(select: &ast::Select, branch_idx: usize) -> ast::Select {
    let one = if branch_idx == 0 {
        select.body.select.clone()
    } else {
        select.body.compounds[branch_idx - 1].select.clone()
    };
    ast::Select {
        with: None,
        body: ast::SelectBody {
            select: one,
            compounds: Vec::new(),
        },
        order_by: Vec::new(),
        limit: None,
    }
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
fn check_group_row_expr(
    expr: &ast::Expr,
    group_exprs: &[ast::Expr],
    aggregates: &[Aggregate],
    bare_column_error: &str,
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
                unsupported(bare_column_error)
            }
            _ => Ok(WalkControl::Continue),
        }
    })
}

/// Resolve one GROUP BY term the way the planner does before classification
/// compares it against result columns: an integer literal is a 1-based
/// result-column reference, and an identifier that does not name a base-table
/// column resolves to a result-column alias (canonical columns take
/// precedence, mirroring `BindingBehavior::TryCanonicalColumnsFirst`).
fn resolve_group_by_term(
    term: &ast::Expr,
    columns: &[ast::ResultColumn],
    base_tables: &[Arc<BTreeTable>],
) -> Result<ast::Expr> {
    let column_expr = |idx: usize| -> Result<ast::Expr> {
        match &columns[idx] {
            ast::ResultColumn::Expr(expr, _) => Ok(expr.as_ref().clone()),
            ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {
                unsupported("star projections combined with GROUP BY")
            }
        }
    };
    match term {
        ast::Expr::Literal(ast::Literal::Numeric(num)) => {
            if let Ok(column_number) = num.parse::<usize>() {
                if column_number == 0 || column_number > columns.len() {
                    return Err(LimboError::ParseError(format!(
                        "1st GROUP BY term out of range - should be between 1 and {}",
                        columns.len()
                    )));
                }
                return column_expr(column_number - 1);
            }
            Ok(term.clone())
        }
        ast::Expr::Id(name) => {
            let target = crate::util::normalize_ident(name.as_str());
            let is_base_column = base_tables.iter().any(|table| {
                table.columns().iter().any(|c| {
                    c.name
                        .as_deref()
                        .is_some_and(|n| crate::util::normalize_ident(n) == target)
                })
            });
            if !is_base_column {
                for (idx, column) in columns.iter().enumerate() {
                    if let ast::ResultColumn::Expr(
                        _,
                        Some(ast::As::As(alias) | ast::As::Elided(alias)),
                    ) = column
                    {
                        if crate::util::normalize_ident(alias.as_str()) == target {
                            return column_expr(idx);
                        }
                    }
                }
            }
            Ok(term.clone())
        }
        _ => Ok(term.clone()),
    }
}

/// Validate one branch of a UNION/UNION ALL view: a single-table
/// filter/project SELECT, mirroring the FilterProject shape. Returns the
/// branch's result-column count (with stars expanded) for the compound
/// arity check.
fn validate_union_branch(
    branch: &ast::OneSelect,
    schema: &Schema,
    resolver: &Resolver,
) -> Result<(usize, Vec<Option<CollationSeq>>)> {
    let ast::OneSelect::Select {
        distinctness,
        columns,
        from,
        where_clause,
        group_by,
        window_clause,
        ..
    } = branch
    else {
        return unsupported("VALUES");
    };
    if matches!(distinctness, Some(ast::Distinctness::Distinct)) {
        return unsupported("DISTINCT branches in compound SELECTs");
    }
    if !window_clause.is_empty() {
        return unsupported("window functions");
    }
    if group_by.is_some() {
        return unsupported("GROUP BY branches in compound SELECTs");
    }
    let Some(from) = from else {
        return unsupported("no FROM clause");
    };
    if !from.joins.is_empty() {
        return unsupported("join branches in compound SELECTs");
    }
    let ast::SelectTable::Table(name, _, _) = from.select.as_ref() else {
        return unsupported("subqueries or table functions in FROM");
    };
    let base_table = schema.get_btree_table(name.name.as_str()).ok_or_else(|| {
        LimboError::ParseError(format!(
            "materialized view base table {} not found",
            name.name.as_str()
        ))
    })?;
    if let Some(where_expr) = where_clause {
        check_scalar_expr(where_expr)?;
    }
    let mut arity = 0;
    let mut collations = Vec::with_capacity(columns.len());
    for column in columns {
        match column {
            ast::ResultColumn::Expr(expr, _) => {
                let mut aggs = Vec::new();
                if resolve_window_and_aggregate_functions(expr, resolver, &mut aggs, None, &mut [])?
                {
                    return unsupported("aggregate branches in compound SELECTs");
                }
                check_scalar_expr(expr)?;
                collations.extend(resolve_term_collations(
                    std::slice::from_ref(expr.as_ref()),
                    from,
                    schema,
                    resolver,
                )?);
                arity += 1;
            }
            ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {
                arity += base_table.columns().len();
                for column in base_table.columns() {
                    let collation = column.collation();
                    if collation.is_custom() {
                        return unsupported("custom collations on grouping or dedup keys");
                    }
                    collations.push(Some(collation));
                }
            }
        }
    }
    Ok((arity, collations))
}

/// Expand a join view's result columns into the qualified expressions that
/// `SELECT DISTINCT` groups by: one per output column, with stars expanded
/// across every joined table (a USING/NATURAL-merged column appearing once,
/// from the left-most table, matching the projection). Aggregate result
/// columns are rejected — `DISTINCT` over an aggregate query dedups the
/// aggregated rows, which the group machinery cannot express.
fn expand_join_output_exprs(
    from: &ast::FromClause,
    schema: &Schema,
    columns: &[ast::ResultColumn],
    resolver: &Resolver,
) -> Result<Vec<ast::Expr>> {
    let using_per_table = crate::util::join_using_columns(from, schema)?;
    // (identifier, table) for every joined table, in FROM order.
    let mut joined: Vec<(String, Arc<BTreeTable>)> = Vec::with_capacity(from.joins.len() + 1);
    for select_table in std::iter::once(from.select.as_ref())
        .chain(from.joins.iter().map(|join| join.table.as_ref()))
    {
        let ast::SelectTable::Table(name, alias, _) = select_table else {
            return unsupported("subqueries or table functions in FROM");
        };
        let table = schema.get_btree_table(name.name.as_str()).ok_or_else(|| {
            LimboError::ParseError(format!(
                "materialized view base table {} not found",
                name.name.as_str()
            ))
        })?;
        let identifier = match alias {
            Some(ast::As::As(a) | ast::As::Elided(a) | ast::As::ImplicitColumnName(a)) => {
                a.as_str().to_string()
            }
            None => table.name.clone(),
        };
        joined.push((identifier, table));
    }

    let qualified = |identifier: &str, column: &crate::schema::Column| -> Result<ast::Expr> {
        let name = column.name.clone().ok_or_else(|| {
            LimboError::InternalError("btree table column without a name".to_string())
        })?;
        Ok(ast::Expr::Qualified(
            ast::Name::exact(identifier.to_string()),
            ast::Name::exact(name),
        ))
    };

    let mut group_exprs = Vec::new();
    for column in columns {
        match column {
            ast::ResultColumn::Expr(expr, _) => {
                let mut aggs = Vec::new();
                if resolve_window_and_aggregate_functions(expr, resolver, &mut aggs, None, &mut [])?
                {
                    return unsupported("DISTINCT over aggregates");
                }
                check_scalar_expr(expr)?;
                group_exprs.push(expr.as_ref().clone());
            }
            ast::ResultColumn::Star => {
                for (table_idx, (identifier, table)) in joined.iter().enumerate() {
                    let merged = &using_per_table[table_idx];
                    for column in table.columns() {
                        if merged.iter().any(|u| {
                            column
                                .name
                                .as_deref()
                                .is_some_and(|n| n.eq_ignore_ascii_case(u))
                        }) {
                            continue;
                        }
                        group_exprs.push(qualified(identifier, column)?);
                    }
                }
            }
            ast::ResultColumn::TableStar(name) => {
                let target = crate::util::normalize_ident(name.as_str());
                let Some((identifier, table)) = joined
                    .iter()
                    .find(|(identifier, _)| crate::util::normalize_ident(identifier) == target)
                else {
                    return Err(LimboError::ParseError(format!("no such table: {target}")));
                };
                for column in table.columns() {
                    group_exprs.push(qualified(identifier, column)?);
                }
            }
        }
    }
    Ok(group_exprs)
}

/// Decompose a view's defining `SELECT` into the incremental operator DAG
/// (see [`crate::incremental::dag`]). Generic relational-algebra
/// decomposition — a `SELECT` core lowers to `Scan`(s) → `Join`? →
/// `Filter`(WHERE) → `Aggregate`? → `Filter`(HAVING) → `Project` →
/// `Distinct`?, with no per-shape dispatch and no combination-specific
/// logic. Reuses the per-operator extraction helpers ([`collect_join_tables`],
/// [`classify_group_aggregate`], [`expand_join_output_exprs`]).
///
/// During migration this runs on selects `classify_view` has already
/// accepted, so it assumes validity and produces structure only; the CREATE
/// gate's per-operator validation folds in here at cutover.
pub fn build_dag(
    select: &ast::Select,
    schema: &Schema,
    resolver: &Resolver,
) -> Result<dag::MaintenanceDag> {
    let mut builder = dag::DagBuilder::new();
    let root = build_select(&mut builder, select, schema, resolver)?;
    Ok(builder.finish(root))
}

fn build_select(
    builder: &mut dag::DagBuilder,
    select: &ast::Select,
    schema: &Schema,
    resolver: &Resolver,
) -> Result<dag::NodeId> {
    if select.body.compounds.is_empty() {
        return build_select_core(builder, &select.body.select, schema, resolver);
    }

    // Compound → one SetOp over each branch's sub-DAG. Both the deduplicating
    // compounds and pure UNION ALL collapse into this single node; the branch
    // sub-DAGs may be any shape, since a branch is just another SELECT.
    let operators: Vec<ast::CompoundOperator> =
        select.body.compounds.iter().map(|c| c.operator).collect();
    let n_branches = operators.len() + 1;
    let mut inputs = Vec::with_capacity(n_branches);
    for branch_idx in 0..n_branches {
        inputs.push(build_select(
            builder,
            &branch_select(select, branch_idx),
            schema,
            resolver,
        )?);
    }

    // Leading branches deduplicated by content: up to and including the right
    // side of the last non-UNION-ALL operator (0 for pure UNION ALL).
    let prefix_len = operators
        .iter()
        .rposition(|op| *op != ast::CompoundOperator::UnionAll)
        .map_or(0, |last_dedup| last_dedup + 2);
    let arity = crate::util::extract_view_columns(&branch_select(select, 0), schema)?
        .columns
        .len();

    // Content-key collation per column: the left-most deduplicated branch
    // that resolves a collation wins (matching compound dedup indexes).
    let mut folded: Vec<Option<CollationSeq>> = Vec::new();
    for branch_idx in 0..prefix_len {
        let branch = if branch_idx == 0 {
            &select.body.select
        } else {
            &select.body.compounds[branch_idx - 1].select
        };
        let (_, branch_collations) = validate_union_branch(branch, schema, resolver)?;
        if folded.is_empty() {
            folded = branch_collations;
        } else {
            for (slot, collation) in folded.iter_mut().zip(branch_collations) {
                if slot.is_none() {
                    *slot = collation;
                }
            }
        }
    }
    let key_collations = folded
        .into_iter()
        .map(|collation| collation.unwrap_or(CollationSeq::Binary))
        .collect();

    Ok(builder.push(dag::OpNode::SetOp {
        inputs,
        operators,
        arity,
        prefix_len,
        key_collations,
    }))
}

fn build_select_core(
    builder: &mut dag::DagBuilder,
    one_select: &ast::OneSelect,
    schema: &Schema,
    resolver: &Resolver,
) -> Result<dag::NodeId> {
    let ast::OneSelect::Select {
        distinctness,
        columns,
        from,
        where_clause,
        group_by,
        ..
    } = one_select
    else {
        return unsupported("VALUES");
    };
    let from = from.as_ref().expect("view definition has a FROM clause");

    // Source: a scan per FROM table, joined when there is more than one.
    let mut node = build_source(builder, from, schema, resolver)?;

    // WHERE: selection over the source. For a LEFT join this is the Filter
    // above the join (ON gates matching in the Join node, WHERE gates output
    // over the padded rows) — pure composition, not a special case.
    if let Some(where_clause) = where_clause {
        node = builder.push(dag::OpNode::Filter {
            input: node,
            predicate: where_clause.as_ref().clone(),
        });
    }

    // Aggregation, if the query groups or references aggregates anywhere.
    let mut has_aggregates = group_by.is_some();
    for column in columns {
        if let ast::ResultColumn::Expr(expr, _) = column {
            let mut aggs = Vec::new();
            if resolve_window_and_aggregate_functions(expr, resolver, &mut aggs, None, &mut [])? {
                has_aggregates = true;
            }
        }
    }

    // DISTINCT is GROUP BY every output column, kept alive by a hidden
    // COUNT(*): synthesize that grouping and route it through the aggregate
    // operator, exactly as the classifier does. DISTINCT over an aggregate
    // query or an outer join is not incrementally maintainable.
    let distinct_columns;
    let distinct_group_by;
    let (columns, group_by): (&[ast::ResultColumn], &Option<ast::GroupBy>) = if matches!(
        distinctness,
        Some(ast::Distinctness::Distinct)
    ) {
        if has_aggregates {
            return unsupported("DISTINCT over aggregates");
        }
        let left_outer = from.joins.iter().any(|join| {
                matches!(&join.operator, ast::JoinOperator::TypedJoin(Some(jt)) if jt.contains(ast::JoinType::LEFT))
            });
        if left_outer {
            return unsupported("DISTINCT over outer joins");
        }
        let group_exprs = expand_join_output_exprs(from, schema, columns, resolver)?;
        distinct_columns = group_exprs
            .iter()
            .cloned()
            .map(|expr| ast::ResultColumn::Expr(Box::new(expr), None))
            .collect::<Vec<_>>();
        distinct_group_by = Some(ast::GroupBy {
            exprs: group_exprs.into_iter().map(Box::new).collect(),
            having: None,
        });
        has_aggregates = true;
        (&distinct_columns, &distinct_group_by)
    } else {
        (columns.as_slice(), group_by)
    };

    if has_aggregates {
        let base_tables: Vec<Arc<BTreeTable>> = std::iter::once(from.select.as_ref())
            .chain(from.joins.iter().map(|join| join.table.as_ref()))
            .filter_map(|table| match table {
                ast::SelectTable::Table(name, _, _) => schema.get_btree_table(name.name.as_str()),
                _ => None,
            })
            .collect();
        // Reuse the aggregate-operator extraction (group keys, collected
        // aggregates, collations); HAVING and the output expressions become
        // the Filter/Project above, not fields of the operator.
        let shape =
            classify_group_aggregate(columns, group_by, from, &base_tables, schema, resolver)?;
        let ViewShape::GroupAggregate {
            group_exprs,
            group_collations,
            aggregates,
            multiset_collation,
            having,
            scalar,
            ..
        } = shape
        else {
            unreachable!("classify_group_aggregate returns a GroupAggregate shape");
        };
        node = builder.push(dag::OpNode::Aggregate {
            input: node,
            group_exprs,
            group_collations,
            aggregates,
            multiset_collation,
            scalar,
        });
        if let Some(having) = having {
            node = builder.push(dag::OpNode::Filter {
                input: node,
                predicate: having,
            });
        }
        // Output expressions over the group row (group keys, aggregate
        // results, expressions over them). No stars (rejected with GROUP BY).
        let projections = columns
            .iter()
            .map(|column| match column {
                ast::ResultColumn::Expr(expr, alias) => {
                    Ok((expr.as_ref().clone(), projection_alias(alias)))
                }
                _ => unsupported("star projection combined with GROUP BY"),
            })
            .collect::<Result<Vec<_>>>()?;
        node = builder.push(dag::OpNode::Project {
            input: node,
            projections,
        });
    } else {
        // Plain projection, stars expanded across the FROM tables.
        let projections = expand_join_output_exprs(from, schema, columns, resolver)?
            .into_iter()
            .map(|expr| (expr, None))
            .collect();
        node = builder.push(dag::OpNode::Project {
            input: node,
            projections,
        });
    }

    Ok(node)
}

/// Build the FROM source: one [`dag::OpNode::Scan`] per table, combined by a
/// [`dag::OpNode::Join`] when there is more than one. Returns the source node.
fn build_source(
    builder: &mut dag::DagBuilder,
    from: &ast::FromClause,
    schema: &Schema,
    _resolver: &Resolver,
) -> Result<dag::NodeId> {
    let (tables, on) = collect_join_tables(from, schema)?;
    let scans: Vec<dag::NodeId> = tables
        .iter()
        .map(|(table, identifier, _)| {
            builder.push(dag::OpNode::Scan {
                table: table.clone(),
                identifier: identifier.clone(),
            })
        })
        .collect();
    if scans.len() == 1 {
        return Ok(scans[0]);
    }
    let is_left = from.joins.iter().any(|join| {
        matches!(&join.operator, ast::JoinOperator::TypedJoin(Some(jt)) if jt.contains(ast::JoinType::LEFT))
    });
    let is_right = from.joins.iter().any(|join| {
        matches!(&join.operator, ast::JoinOperator::TypedJoin(Some(jt))
            if jt.contains(ast::JoinType::RIGHT) && !jt.contains(ast::JoinType::LEFT))
    });
    let left_outer = is_left || is_right;
    let mut scans = scans;
    let mut using: Vec<Vec<String>> = tables.iter().map(|(_, _, u)| u.clone()).collect();
    if is_right {
        // A RIGHT JOIN B is maintained as B LEFT JOIN A: swap the inputs so
        // the padded (unmatched) side is the outer left. classify restricts
        // RIGHT to two ON-joined tables, so this is a plain reversal and the
        // root projection still binds output columns by identity.
        scans.swap(0, 1);
        using.swap(0, 1);
    }
    Ok(builder.push(dag::OpNode::Join {
        inputs: scans,
        using,
        on,
        left_outer,
    }))
}

/// The output-column name for a projection, from an explicit alias only.
fn projection_alias(alias: &Option<ast::As>) -> Option<String> {
    match alias {
        Some(ast::As::As(name) | ast::As::Elided(name)) => Some(name.as_str().to_string()),
        _ => None,
    }
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
pub fn classify_view(
    select: &ast::Select,
    schema: &Schema,
    resolver: &Resolver,
) -> Result<ViewShape> {
    if select.with.is_some() {
        return unsupported("WITH clauses");
    }
    if !select.order_by.is_empty() {
        return unsupported("ORDER BY");
    }
    if select.limit.is_some() {
        return unsupported("LIMIT");
    }

    if !select.body.compounds.is_empty() {
        let operators: Vec<ast::CompoundOperator> =
            select.body.compounds.iter().map(|c| c.operator).collect();

        // A pure UNION ALL whose branches are not all plain filter/project
        // is maintained as independent per-branch sub-views (no dedup, no
        // cross-branch merge). Classify each branch with the ordinary
        // per-shape machinery; if any is a join or aggregate, this is a
        // CompoundAll. Filter/project-only chains (and any chain with a
        // deduplicating operator) fall through to the Compound path below.
        let n_branches = operators.len() + 1;
        if operators
            .iter()
            .all(|op| *op == ast::CompoundOperator::UnionAll)
        {
            let mut branch_shapes = Vec::with_capacity(n_branches);
            let mut arity: Option<usize> = None;
            for branch_idx in 0..n_branches {
                let branch = branch_select(select, branch_idx);
                let branch_arity = crate::util::extract_view_columns(&branch, schema)?
                    .columns
                    .len();
                if *arity.get_or_insert(branch_arity) != branch_arity {
                    return Err(LimboError::ParseError(format!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        operators[branch_idx - 1],
                    )));
                }
                let shape = classify_view(&branch, schema, resolver)?;
                match &shape {
                    ViewShape::FilterProject | ViewShape::GroupAggregate { .. } => {}
                    ViewShape::Join { left_outer, .. } => {
                        if *left_outer {
                            // A LEFT-join branch would need both the rowid
                            // split (inner vs padded) and the branch
                            // namespace, which the view-rowid transform does
                            // not compose.
                            return unsupported("LEFT JOIN branches in a UNION ALL");
                        }
                    }
                    ViewShape::Compound { .. } | ViewShape::CompoundAll { .. } => {
                        return unsupported("compound SELECTs nested in a UNION ALL branch");
                    }
                }
                branch_shapes.push(shape);
            }
            if branch_shapes
                .iter()
                .any(|s| !matches!(s, ViewShape::FilterProject))
            {
                return Ok(ViewShape::CompoundAll { branch_shapes });
            }
        }

        // Branches up to and including the right side of the last
        // deduplicating operator are content-keyed: any later UNION ALL
        // appends its rows with multiplicity preserved, while a UNION ALL
        // *before* a deduplicating operator only feeds presence (the dedup
        // collapses its multiplicities anyway).
        let prefix_len = operators
            .iter()
            .rposition(|op| *op != ast::CompoundOperator::UnionAll)
            .map_or(0, |last_dedup| last_dedup + 2);

        let mut arity: Option<usize> = None;
        let mut folded: Vec<Option<CollationSeq>> = Vec::new();
        for (branch_idx, branch) in std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().map(|c| &c.select))
            .enumerate()
        {
            let (branch_arity, branch_collations) =
                validate_union_branch(branch, schema, resolver)?;
            if *arity.get_or_insert(branch_arity) != branch_arity {
                // The operator joining the mismatching branch to the chain.
                let operator = operators[branch_idx - 1];
                return Err(LimboError::ParseError(format!(
                    "SELECTs to the left and right of {operator} do not have the same number of result columns",
                )));
            }
            // Left precedence among the deduplicated branches: the left-most
            // branch that resolves a collation for a column decides it, like
            // compound dedup indexes. Append-suffix branches do not
            // participate in dedup comparisons.
            if branch_idx < prefix_len {
                if folded.is_empty() {
                    folded = branch_collations;
                } else {
                    for (slot, branch_collation) in folded.iter_mut().zip(branch_collations) {
                        if slot.is_none() {
                            *slot = branch_collation;
                        }
                    }
                }
            }
        }
        let key_collations = folded
            .into_iter()
            .map(|collation| collation.unwrap_or(CollationSeq::Binary))
            .collect();
        return Ok(ViewShape::Compound {
            operators,
            arity: arity.expect("at least two branches"),
            prefix_len,
            key_collations,
        });
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

    // SELECT ALL is the default semantics spelled out; only DISTINCT changes
    // the shape (handled below once the FROM structure is known).
    let select_distinct = matches!(distinctness, Some(ast::Distinctness::Distinct));
    if !window_clause.is_empty() {
        return unsupported("window functions");
    }

    let Some(from) = from else {
        return unsupported("no FROM clause");
    };
    if !matches!(from.select.as_ref(), ast::SelectTable::Table(_, _, _)) {
        return unsupported("subqueries or table functions in FROM");
    }

    if let Some(where_expr) = where_clause {
        check_scalar_expr(where_expr)?;
    }

    if !from.joins.is_empty() {
        // Maintenance runs one delta phase per non-empty subset of the
        // joined tables (2^N - 1), so the table count is capped.
        if from.joins.len() > MAX_JOIN_TABLES - 1 {
            return Err(LimboError::ParseError(format!(
                "materialized views with joins over more than {MAX_JOIN_TABLES} tables are not yet supported",
            )));
        }
        let mut left_outer = false;
        for join in &from.joins {
            if let ast::JoinOperator::TypedJoin(join_type) = &join.operator {
                let join_type = join_type.unwrap_or(ast::JoinType::INNER);
                let is_left = join_type.contains(ast::JoinType::LEFT);
                let is_right = join_type.contains(ast::JoinType::RIGHT);
                let is_full = (is_left && is_right)
                    || (join_type.contains(ast::JoinType::OUTER) && !is_left && !is_right);
                if is_full {
                    return unsupported("FULL outer joins");
                }
                if is_left || is_right {
                    // The NULL-padded row bookkeeping tracks one outer side;
                    // outer joins are supported for exactly two tables. A
                    // RIGHT join is maintained as a LEFT join with the tables
                    // swapped (build_dag does the swap); the USING/NATURAL
                    // column merge is order-sensitive and does not survive the
                    // swap, so RIGHT is restricted to an ON constraint.
                    if from.joins.len() > 1 {
                        return unsupported(if is_left {
                            "LEFT JOIN over more than two tables"
                        } else {
                            "RIGHT JOIN over more than two tables"
                        });
                    }
                    if is_right
                        && (join_type.contains(ast::JoinType::NATURAL)
                            || matches!(join.constraint, Some(ast::JoinConstraint::Using(_))))
                    {
                        return unsupported("RIGHT JOIN with USING or NATURAL");
                    }
                    left_outer = true;
                }
            }
            if !matches!(join.table.as_ref(), ast::SelectTable::Table(_, _, _)) {
                return unsupported("subqueries or table functions in FROM");
            }
            if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                check_scalar_expr(expr)?;
            }
        }
        // USING and NATURAL desugar to equality predicates against the
        // left-most earlier table with each merged column. Validate here —
        // at translate time — so a bad constraint can never fail inside the
        // CREATE program. The desugared predicates qualify columns by table
        // identifier, so duplicate identifiers cannot be resolved.
        let using_per_table = crate::util::join_using_columns(from, schema)?;
        if using_per_table.iter().any(|using| !using.is_empty()) {
            let mut identifiers: Vec<String> = Vec::with_capacity(from.joins.len() + 1);
            for table in std::iter::once(from.select.as_ref())
                .chain(from.joins.iter().map(|join| join.table.as_ref()))
            {
                if let ast::SelectTable::Table(name, alias, _) = table {
                    identifiers.push(crate::util::normalize_ident(match alias {
                        Some(
                            ast::As::As(alias)
                            | ast::As::Elided(alias)
                            | ast::As::ImplicitColumnName(alias),
                        ) => alias.as_str(),
                        None => name.name.as_str(),
                    }));
                }
            }
            let unique: std::collections::HashSet<&String> = identifiers.iter().collect();
            if unique.len() != identifiers.len() {
                return unsupported("NATURAL or USING joins between tables with the same name");
            }
        }
        if select_distinct {
            // DISTINCT over a join is GROUP BY every output column over the
            // join: one group per distinct output row, kept alive by the
            // hidden COUNT(*). Expand the result columns to qualified
            // references (stars across every joined table, merged USING
            // columns once) and route through the shared aggregate path,
            // whose compile step re-reads the join from the FROM clause.
            if left_outer {
                return unsupported("DISTINCT over outer joins");
            }
            let group_exprs = expand_join_output_exprs(from, schema, columns, resolver)?;
            let synthetic_columns: Vec<ast::ResultColumn> = group_exprs
                .iter()
                .map(|expr| ast::ResultColumn::Expr(Box::new(expr.clone()), None))
                .collect();
            let group_by = ast::GroupBy {
                exprs: group_exprs.iter().cloned().map(Box::new).collect(),
                having: None,
            };
            let base_tables: Vec<Arc<BTreeTable>> = std::iter::once(from.select.as_ref())
                .chain(from.joins.iter().map(|join| join.table.as_ref()))
                .filter_map(|table| match table {
                    ast::SelectTable::Table(name, _, _) => {
                        schema.get_btree_table(name.name.as_str())
                    }
                    _ => None,
                })
                .collect();
            return classify_group_aggregate(
                &synthetic_columns,
                &Some(group_by),
                from,
                &base_tables,
                schema,
                resolver,
            );
        }
        let mut contains_aggregates = group_by.is_some();
        for column in columns {
            if let ast::ResultColumn::Expr(expr, _) = column {
                let mut aggs = Vec::new();
                if resolve_window_and_aggregate_functions(expr, resolver, &mut aggs, None, &mut [])?
                {
                    contains_aggregates = true;
                }
            }
        }
        if contains_aggregates {
            if left_outer {
                // The joined-delta ephemeral feed assumes inner semantics;
                // NULL-padded rows never reach the aggregate loop.
                return unsupported("aggregates over outer joins");
            }
            // Aggregates over a join: the join's deltas feed the group
            // machinery, so this classifies as a GroupAggregate; the compile
            // step re-reads the join structure from the FROM clause.
            let mut base_tables = Vec::new();
            for table in std::iter::once(from.select.as_ref())
                .chain(from.joins.iter().map(|join| join.table.as_ref()))
            {
                if let ast::SelectTable::Table(name, _, _) = table {
                    base_tables.extend(schema.get_btree_table(name.name.as_str()));
                }
            }
            return classify_group_aggregate(
                columns,
                group_by,
                from,
                &base_tables,
                schema,
                resolver,
            );
        }
        for column in columns {
            if let ast::ResultColumn::Expr(expr, _) = column {
                check_scalar_expr(expr)?;
            }
        }
        return Ok(ViewShape::Join {
            n_tables: from.joins.len() + 1,
            left_outer,
        });
    }

    // SELECT DISTINCT over plain projections is GROUP BY every result column:
    // one group per distinct output row, kept alive by the hidden COUNT(*)
    // while any source row still produces it. DISTINCT over an aggregate
    // query dedups the aggregated rows themselves, which the group machinery
    // cannot express.
    if select_distinct {
        if group_by.is_some() {
            return unsupported("DISTINCT over aggregates");
        }
        let mut group_exprs = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                ast::ResultColumn::Expr(expr, _) => {
                    let mut aggs = Vec::new();
                    if resolve_window_and_aggregate_functions(
                        expr,
                        resolver,
                        &mut aggs,
                        None,
                        &mut [],
                    )? {
                        return unsupported("DISTINCT over aggregates");
                    }
                    check_scalar_expr(expr)?;
                    group_exprs.push(expr.as_ref().clone());
                }
                ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => {
                    let ast::SelectTable::Table(name, _, _) = from.select.as_ref() else {
                        unreachable!("single-table FROM checked above");
                    };
                    let base_table =
                        schema.get_btree_table(name.name.as_str()).ok_or_else(|| {
                            LimboError::ParseError(format!(
                                "materialized view base table {} not found",
                                name.name.as_str()
                            ))
                        })?;
                    for column in base_table.columns() {
                        let name = column.name.clone().ok_or_else(|| {
                            LimboError::InternalError(
                                "btree table column without a name".to_string(),
                            )
                        })?;
                        group_exprs.push(ast::Expr::Id(ast::Name::exact(name)));
                    }
                }
            }
        }
        let count_star = ast::Expr::FunctionCallStar {
            name: ast::Name::exact("count".to_string()),
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let mut aggregates = Vec::new();
        resolve_window_and_aggregate_functions(
            &count_star,
            resolver,
            &mut aggregates,
            None,
            &mut [],
        )?;
        let group_collations = resolve_key_collations(&group_exprs, from, schema, resolver)?;
        return Ok(ViewShape::GroupAggregate {
            group_exprs,
            group_collations,
            aggregates,
            multiset_collation: CollationSeq::Binary,
            having: None,
            scalar: false,
        });
    }

    // HAVING without GROUP BY parses as a GroupBy node with no expressions;
    // it makes the query an aggregate query just like an aggregate call does.
    let scalar = group_by.as_ref().is_none_or(|gb| gb.exprs.is_empty());
    if scalar && group_by.as_ref().is_none_or(|gb| gb.having.is_none()) {
        let mut contains_aggregates = false;
        for column in columns {
            if let ast::ResultColumn::Expr(expr, _) = column {
                let mut aggs = Vec::new();
                if resolve_window_and_aggregate_functions(expr, resolver, &mut aggs, None, &mut [])?
                {
                    contains_aggregates = true;
                }
            }
        }
        if !contains_aggregates {
            for column in columns {
                if let ast::ResultColumn::Expr(expr, _) = column {
                    check_scalar_expr(expr)?;
                }
            }
            return Ok(ViewShape::FilterProject);
        }
    }

    let base_tables: Vec<Arc<BTreeTable>> = match from.select.as_ref() {
        ast::SelectTable::Table(name, _, _) => schema
            .get_btree_table(name.name.as_str())
            .into_iter()
            .collect(),
        _ => Vec::new(),
    };
    classify_group_aggregate(columns, group_by, from, &base_tables, schema, resolver)
}

/// Classify an aggregate query (GROUP BY, scalar aggregates, HAVING) into a
/// [`ViewShape::GroupAggregate`], shared by the single-table and join paths.
/// `base_tables` are the FROM tables, used to resolve GROUP BY terms the way
/// the planner does (ordinals and aliases with canonical-column precedence).
fn classify_group_aggregate(
    columns: &[ast::ResultColumn],
    group_by: &Option<ast::GroupBy>,
    from: &ast::FromClause,
    base_tables: &[Arc<BTreeTable>],
    schema: &Schema,
    resolver: &Resolver,
) -> Result<ViewShape> {
    let (raw_group_exprs, having_clause): (Vec<ast::Expr>, Option<&ast::Expr>) = match group_by {
        Some(group_by) => {
            let exprs = group_by
                .exprs
                .iter()
                .map(|e| resolve_group_by_term(e, columns, base_tables))
                .collect::<Result<Vec<_>>>()?;
            (exprs, group_by.having.as_deref())
        }
        None => (Vec::new(), None),
    };

    // Scalar aggregates form one group keyed by a synthetic constant, so the
    // single row flows through the same state-table machinery as GROUP BY.
    let scalar = raw_group_exprs.is_empty();
    let group_exprs: Vec<ast::Expr> = if scalar {
        vec![ast::Expr::Literal(ast::Literal::Numeric("0".to_string()))]
    } else {
        raw_group_exprs
    };
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

    // Collect aggregates referenced by the result columns and validate that
    // every column is a group key, a plain aggregate, or an expression over
    // them (SUM(x) + 1, and the like — the rule HAVING also follows). Bare
    // columns outside GROUP BY carry SQLite's arbitrary-row semantics, which
    // cannot be maintained incrementally. The output-column mapping itself is
    // derived from the DAG's projection at codegen time, not stored here.
    for column in columns {
        let ast::ResultColumn::Expr(expr, _) = column else {
            return unsupported("star projections combined with GROUP BY");
        };
        if group_exprs
            .iter()
            .any(|g| crate::util::exprs_are_equivalent(g, expr))
        {
            continue;
        }
        resolve_window_and_aggregate_functions(expr, resolver, &mut aggregates, None, &mut [])?;
        if aggregates
            .iter()
            .any(|a| crate::util::exprs_are_equivalent(&a.original_expr, expr))
        {
            continue;
        }
        check_group_row_expr(
            expr,
            &group_exprs,
            &aggregates,
            "non-aggregate result columns not in GROUP BY",
        )?;
    }

    let having = match having_clause {
        Some(having) => {
            resolve_window_and_aggregate_functions(
                having,
                resolver,
                &mut aggregates,
                None,
                &mut [],
            )?;
            check_group_row_expr(
                having,
                &group_exprs,
                &aggregates,
                "HAVING referencing columns outside GROUP BY",
            )?;
            Some(having.clone())
        }
        None => None,
    };

    for agg in &aggregates {
        validate_supported_aggregate(agg)?;
    }

    let group_collations = resolve_key_collations(&group_exprs, from, schema, resolver)?;
    let mut multiset_collation = CollationSeq::Binary;
    let mut multiset_seen = false;
    for agg in aggregates.iter().filter(|a| uses_multiset(a)) {
        let collation = match agg.args.first() {
            Some(arg) => {
                resolve_key_collations(std::slice::from_ref(arg), from, schema, resolver)?[0]
            }
            None => CollationSeq::Binary,
        };
        if !multiset_seen {
            multiset_seen = true;
            multiset_collation = collation;
        } else if multiset_collation != collation {
            // All multiset-tracked values share one table whose `val` column
            // carries a single collation.
            return unsupported(
                "different collations across MIN/MAX or DISTINCT aggregate arguments",
            );
        }
    }

    Ok(ViewShape::GroupAggregate {
        group_exprs,
        group_collations,
        aggregates,
        multiset_collation,
        having,
        scalar,
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
    classify_view(select, schema, &resolver)
}

/// Whether the view keeps a hidden internal state table.
pub fn needs_state_table(shape: &ViewShape) -> bool {
    // CompoundAll has no single view-level state table; each branch's tables
    // are enumerated by [`hidden_tables`].
    matches!(
        shape,
        ViewShape::GroupAggregate { .. } | ViewShape::Join { .. } | ViewShape::Compound { .. }
    )
}

/// A hidden state/multiset table a view needs, fully named with its CREATE
/// SQL. The single source of truth for the DDL the CREATE program emits.
pub struct HiddenTableDef {
    pub table_name: String,
    pub create_sql: String,
}

/// Enumerate every hidden table a view needs, in creation order. A
/// non-compound view has at most a state table and a multiset table (empty
/// branch suffix). A CompoundAll view has none of its own — it enumerates
/// each branch's tables under that branch's `__b<idx>` suffix, matching the
/// names the branch sub-programs look up.
pub fn hidden_tables(view_name: &str, shape: &ViewShape) -> Result<Vec<HiddenTableDef>> {
    fn tables_for(
        view_name: &str,
        suffix: &str,
        shape: &ViewShape,
        out: &mut Vec<HiddenTableDef>,
    ) -> Result<()> {
        use crate::incremental::view::DBSP_CIRCUIT_VERSION;
        if needs_state_table(shape) {
            let table_name = format!(
                "{}{DBSP_CIRCUIT_VERSION}_{view_name}{suffix}",
                crate::schema::DBSP_TABLE_PREFIX,
            );
            let create_sql = state_table_sql(&table_name, shape)?;
            out.push(HiddenTableDef {
                table_name,
                create_sql,
            });
        }
        if needs_multiset_table(shape) {
            let table_name = format!(
                "{}{DBSP_CIRCUIT_VERSION}_{view_name}{suffix}",
                crate::schema::DBSP_MULTISET_TABLE_PREFIX,
            );
            let create_sql = multiset_table_sql(&table_name, shape)?;
            out.push(HiddenTableDef {
                table_name,
                create_sql,
            });
        }
        Ok(())
    }

    let mut out = Vec::new();
    match shape {
        ViewShape::CompoundAll { branch_shapes } => {
            for (idx, sub) in branch_shapes.iter().enumerate() {
                tables_for(view_name, &format!("__b{idx}"), sub, &mut out)?;
            }
        }
        _ => tables_for(view_name, "", shape, &mut out)?,
    }
    Ok(out)
}

/// The CREATE TABLE statement for a view's internal state table.
///
/// For a GROUP BY view: one row per live group, holding the group-key values
/// and each payload aggregate's persisted state, with the PRIMARY KEY over
/// the group columns giving the automatic group-lookup index. MIN/MAX
/// aggregates contribute no columns here — their state is the value multiset
/// table.
///
/// For a join view: one row per live join-output row, keyed by the pair of
/// base-table rowids that produced it.
///
/// For a compound view with a dedup prefix: one row per distinct content
/// key, holding one signed count per prefix branch (the visibility fold
/// reads them); a pure UNION ALL chain instead keys rows by (branch, source
/// rowid) with a single multiplicity. A trailing UNION ALL suffix behind a
/// dedup prefix keeps its (branch, rowid, mult) rows in the auxiliary table
/// (see [`multiset_table_sql`]).
///
/// In each case the state row's rowid doubles as the row's rowid in the
/// view btree: state rows are written unconditionally for live groups/pairs,
/// so their rowids are unique, stable, and — unlike a separately reserved
/// rowid — cannot collide when HAVING suppresses a group's view row.
///
/// Declared column types are empty so nothing coerces the stored values.
pub fn state_table_sql(state_table_name: &str, shape: &ViewShape) -> Result<String> {
    match shape {
        ViewShape::GroupAggregate {
            group_exprs,
            group_collations,
            aggregates,
            ..
        } => {
            let mut columns = Vec::new();
            for (i, collation) in group_collations.iter().enumerate() {
                columns.push(format!("g{i}{}", collate_clause(*collation)));
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
        ViewShape::Join { n_tables, .. } => {
            let key: Vec<String> = (0..*n_tables).map(|i| format!("r{i}")).collect();
            Ok(format!(
                "CREATE TABLE {state_table_name} ({}, mult, PRIMARY KEY ({}))",
                key.join(", "),
                key.join(", ")
            ))
        }
        ViewShape::Compound {
            arity,
            prefix_len,
            key_collations,
            ..
        } => {
            if *prefix_len == 0 {
                // Pure UNION ALL: (branch, rid) identity, one multiplicity.
                return Ok(format!(
                    "CREATE TABLE {state_table_name} (branch, rid, mult, PRIMARY KEY (branch, rid))"
                ));
            }
            let mut columns: Vec<String> = (0..*arity)
                .map(|i| format!("c{i}{}", collate_clause(key_collations[i])))
                .collect();
            for i in 0..*prefix_len {
                columns.push(format!("cnt{i}"));
            }
            let key: Vec<String> = (0..*arity).map(|i| format!("c{i}")).collect();
            Ok(format!(
                "CREATE TABLE {state_table_name} ({}, PRIMARY KEY ({}))",
                columns.join(", "),
                key.join(", ")
            ))
        }
        ViewShape::FilterProject | ViewShape::CompoundAll { .. } => Err(LimboError::InternalError(
            "this view shape has no single state table".to_string(),
        )),
    }
}

/// Whether an aggregate keeps per-value state in the multiset table: MIN/MAX
/// (to find the next extreme after a retraction) and DISTINCT aggregates (to
/// step the accumulator only on first occurrence of a value).
fn uses_multiset(agg: &Aggregate) -> bool {
    matches!(agg.func, AggFunc::Min | AggFunc::Max) || tracks_distinct_values(agg)
}

/// Whether the view needs the auxiliary multiset table: value multiplicities
/// for MIN/MAX and DISTINCT aggregates, or (branch, rowid) multiplicities
/// for a compound view's trailing UNION ALL branches behind a dedup prefix
/// (the content-keyed state table can't hold rows that preserve
/// multiplicity).
pub fn needs_multiset_table(shape: &ViewShape) -> bool {
    match shape {
        ViewShape::GroupAggregate { aggregates, .. } => aggregates.iter().any(uses_multiset),
        ViewShape::Compound {
            operators,
            prefix_len,
            ..
        } => *prefix_len > 0 && *prefix_len < operators.len() + 1,
        ViewShape::Join { left_outer, .. } => *left_outer,
        _ => false,
    }
}

/// The CREATE TABLE statement for a GROUP BY view's value multiset: one row
/// per (aggregate, group, distinct value) with its multiplicity. For MIN/MAX
/// the PRIMARY KEY ordering makes a group's extreme value an index seek, so
/// retracting the current extreme never needs a rescan in Rust; for DISTINCT
/// aggregates the multiplicity decides when a value enters or leaves the
/// accumulator.
pub fn multiset_table_sql(multiset_table_name: &str, shape: &ViewShape) -> Result<String> {
    match shape {
        ViewShape::GroupAggregate {
            group_collations,
            multiset_collation,
            ..
        } => {
            let mut columns = vec!["agg_id".to_string()];
            let mut key = vec!["agg_id".to_string()];
            for (i, collation) in group_collations.iter().enumerate() {
                columns.push(format!("g{i}{}", collate_clause(*collation)));
                key.push(format!("g{i}"));
            }
            columns.push(format!("val{}", collate_clause(*multiset_collation)));
            key.push("val".to_string());
            Ok(format!(
                "CREATE TABLE {multiset_table_name} ({}, mult, PRIMARY KEY ({}))",
                columns.join(", "),
                key.join(", ")
            ))
        }
        // A compound view's trailing UNION ALL branches: a multiset of
        // (branch, source rowid), each row's rowid doubling as its view
        // rowid (disambiguated from content-row rowids by the caller).
        ViewShape::Compound { .. } => Ok(format!(
            "CREATE TABLE {multiset_table_name} (branch, rid, mult, PRIMARY KEY (branch, rid))"
        )),
        // A LEFT JOIN view's per-left-row bookkeeping: signed presence,
        // count of ON-matches, and whether its NULL-padded view row is
        // currently written. The padded row exists while the left row is
        // present with zero matches and WHERE passes over the padded image;
        // the stored bit records the current state because WHERE was
        // evaluated against an image that later contributions may no longer
        // be able to reconstruct.
        ViewShape::Join { .. } => Ok(format!(
            "CREATE TABLE {multiset_table_name} (l_rid, present, matches, padded, PRIMARY KEY (l_rid))"
        )),
        _ => Err(LimboError::InternalError(
            "only GROUP BY, compound, and LEFT JOIN views have multiset tables".to_string(),
        )),
    }
}

/// The `COLLATE` clause for a hidden-table key column, empty for the
/// default binary comparison. The name round-trips through
/// [`CollationSeq::new`] when the generated DDL is parsed back; custom
/// collations (which do not) are rejected at classify time.
fn collate_clause(collation: CollationSeq) -> String {
    match collation {
        CollationSeq::Binary | CollationSeq::Unset => String::new(),
        other => format!(" COLLATE \"{}\"", other.name()),
    }
}

/// Compile the maintenance (or population) program for a filter/project view.
///
/// `view_root_page` is the root of the view's data btree; `view_columns` are
/// the view's output columns (the on-disk record is these plus a trailing
/// weight column).

/// How a branch of a compound view maps its local view rowids into the
/// shared view btree, and how its hidden state tables are named apart from
/// sibling branches. The standalone identity ([`BranchNamespace::none`],
/// scale 1 / offset 0 / empty suffix) leaves the local rowid register
/// untouched, so a standalone view generates byte-identical bytecode.
#[derive(Clone)]
struct BranchNamespace {
    /// Local rowid `r` maps to view rowid `r * scale + offset`; branches of
    /// an N-branch compound use `scale = N`, `offset = branch index`, so
    /// their rowid ranges are disjoint.
    scale: i64,
    offset: i64,
    /// Appended to the view name when forming this branch's hidden-table
    /// names, keeping each branch's state tables distinct.
    suffix: String,
}

impl BranchNamespace {
    fn none() -> Self {
        Self {
            scale: 1,
            offset: 0,
            suffix: String::new(),
        }
    }

    fn is_identity(&self) -> bool {
        self.scale == 1 && self.offset == 0
    }

    /// Emit the view rowid for `local_reg`. Returns `local_reg` unchanged for
    /// the identity namespace (no instructions); otherwise emits
    /// `local * scale + offset` into a fresh register and returns it.
    fn emit_view_rowid(&self, program: &mut ProgramBuilder, local_reg: usize) -> usize {
        if self.is_identity() {
            return local_reg;
        }
        let dst = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: local_reg,
            dst_reg: dst,
            extra_amount: 0,
        });
        self.apply_in_place(program, dst);
        dst
    }

    /// Map `reg` from a local view rowid to this branch's view rowid in
    /// place. A no-op for the identity namespace.
    fn apply_in_place(&self, program: &mut ProgramBuilder, reg: usize) {
        if self.is_identity() {
            return;
        }
        let scratch = program.alloc_register();
        program.emit_int(self.scale, scratch);
        program.emit_insn(Insn::Multiply {
            lhs: reg,
            rhs: scratch,
            dest: reg,
        });
        program.emit_int(self.offset, scratch);
        program.emit_insn(Insn::Add {
            lhs: reg,
            rhs: scratch,
            dest: reg,
        });
    }
}

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
    let dag = build_dag_for_connection(select, schema, connection)?;
    if dag_is_all_linear(&dag) {
        return compile_dag_view_program(
            view_name,
            select,
            &dag,
            view_root_page,
            num_view_columns,
            input,
            output,
            schema,
            connection,
        );
    }
    if dag_is_aggregate(&dag) {
        if matches!(output, MaintenanceOutput::EmitRows) {
            return Err(LimboError::InternalError(
                "aggregate views serve uncommitted reads by recomputing the defining query"
                    .to_string(),
            ));
        }
        return compile_dag_view_program(
            view_name,
            select,
            &dag,
            view_root_page,
            num_view_columns,
            input,
            output,
            schema,
            connection,
        );
    }
    if dag_is_join(&dag) {
        if matches!(output, MaintenanceOutput::EmitRows) {
            return Err(LimboError::InternalError(
                "join views serve uncommitted reads by recomputing the defining query".to_string(),
            ));
        }
        return compile_dag_view_program(
            view_name,
            select,
            &dag,
            view_root_page,
            num_view_columns,
            input,
            output,
            schema,
            connection,
        );
    }
    if dag_is_compound(&dag) {
        if matches!(output, MaintenanceOutput::EmitRows) {
            return Err(LimboError::InternalError(
                "compound views serve uncommitted reads by recomputing the defining query"
                    .to_string(),
            ));
        }
        return compile_dag_compound_program(
            view_name,
            &dag,
            view_root_page,
            num_view_columns,
            input,
            schema,
            connection,
        );
    }
    if dag_is_compound_all(&dag) {
        if matches!(output, MaintenanceOutput::EmitRows) {
            return Err(LimboError::InternalError(
                "compound views serve uncommitted reads by recomputing the defining query"
                    .to_string(),
            ));
        }
        return compile_dag_compound_all_program(
            view_name,
            select,
            &dag,
            view_root_page,
            num_view_columns,
            input,
            schema,
            connection,
        );
    }

    Err(LimboError::InternalError(format!(
        "materialized view {view_name} has no maintainable operator DAG shape"
    )))
}

/// [`build_dag`] with a transient resolver from a connection, mirroring
/// [`classify_view_for_connection`].
fn build_dag_for_connection(
    select: &ast::Select,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<dag::MaintenanceDag> {
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
    build_dag(select, schema, &resolver)
}

/// Whether every node is a `Scan`, `Filter`, or `Project`.
fn dag_is_all_linear(dag: &dag::MaintenanceDag) -> bool {
    dag.nodes.iter().all(|node| {
        matches!(
            node,
            dag::OpNode::Scan { .. } | dag::OpNode::Filter { .. } | dag::OpNode::Project { .. }
        )
    })
}

/// Convert a `Project` node's `(expr, alias)` projections into the
/// `ResultColumn`s the shared emitters translate.
fn projections_to_result_columns(
    projections: &[(ast::Expr, Option<String>)],
) -> Vec<ast::ResultColumn> {
    projections
        .iter()
        .map(|(expr, alias)| {
            ast::ResultColumn::Expr(
                Box::new(expr.clone()),
                alias
                    .clone()
                    .map(|name| ast::As::As(ast::Name::exact(name))),
            )
        })
        .collect()
}

/// Emit one view — or one compound-all branch — from the sub-tree rooted at
/// `root` into `program`, dispatching on the operator directly under the
/// `Project` root: a `Scan` (filter/project), an `Aggregate`, or a `Join`.
/// The `Filter` between the `Project` and that operator, if present, is the
/// WHERE for filter/project and joins and the HAVING for aggregates. `select`
/// supplies the FROM/WHERE the aggregate emitter still reads (the view's
/// select, or a compound-all branch's).
#[allow(clippy::too_many_arguments)]
fn emit_dag_view(
    program: &mut ProgramBuilder,
    view_name: &str,
    dag: &dag::MaintenanceDag,
    root: dag::NodeId,
    select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    output: MaintenanceOutput,
    ns: &BranchNamespace,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let dag::OpNode::Project {
        input: proj_input,
        projections,
    } = &dag.nodes[root]
    else {
        return Err(LimboError::InternalError(
            "view DAG root is not a projection".to_string(),
        ));
    };
    let (op_id, filter): (dag::NodeId, Option<&ast::Expr>) = match &dag.nodes[*proj_input] {
        dag::OpNode::Filter { input, predicate } => (*input, Some(predicate)),
        _ => (*proj_input, None),
    };
    match &dag.nodes[op_id] {
        dag::OpNode::Scan { table, identifier } => {
            let columns = projections_to_result_columns(projections);
            emit_filter_project(
                program,
                view_name,
                table,
                identifier,
                &columns,
                filter,
                view_root_page,
                num_view_columns,
                input,
                output,
                ns,
                schema,
                connection,
            )
        }
        dag::OpNode::Aggregate {
            group_exprs,
            group_collations,
            aggregates,
            scalar,
            ..
        } => {
            let outputs = dag_aggregate_outputs(projections, group_exprs, aggregates);
            emit_group_aggregate(
                program,
                view_name,
                select,
                group_exprs,
                group_collations,
                aggregates,
                &outputs,
                filter,
                *scalar,
                view_root_page,
                num_view_columns,
                input,
                ns,
                schema,
                connection,
            )
        }
        dag::OpNode::Join {
            inputs,
            using,
            on,
            left_outer,
        } => {
            let mut tables: Vec<JoinTable> = Vec::with_capacity(inputs.len());
            for (pos, &scan_id) in inputs.iter().enumerate() {
                let dag::OpNode::Scan { table, identifier } = &dag.nodes[scan_id] else {
                    return Err(LimboError::InternalError(
                        "join input is not a base table scan".to_string(),
                    ));
                };
                tables.push((table.clone(), identifier.clone(), using[pos].clone()));
            }
            let columns = projections_to_result_columns(projections);
            emit_join(
                program,
                view_name,
                &columns,
                &tables,
                on,
                filter,
                *left_outer,
                view_root_page,
                num_view_columns,
                input,
                ns,
                schema,
                connection,
            )
        }
        _ => Err(LimboError::InternalError(
            "unsupported operator under view projection".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn compile_dag_view_program(
    view_name: &str,
    select: &ast::Select,
    dag: &dag::MaintenanceDag,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    output: MaintenanceOutput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let mut program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts {
            num_cursors: 13,
            approx_num_insns: 128 + 64 * num_view_columns,
            approx_num_labels: 48,
        },
    );
    program.prologue();
    emit_dag_view(
        &mut program,
        view_name,
        dag,
        dag.root,
        select,
        view_root_page,
        num_view_columns,
        input,
        output,
        &BranchNamespace::none(),
        schema,
        connection,
    )?;
    program.epilogue(schema);
    program.build(
        connection.clone(),
        false,
        "materialized view dag maintenance",
    )
}

/// Whether the DAG is an aggregate view: `Project` root over an `Aggregate`,
/// optionally with a HAVING `Filter` between them.
fn dag_is_aggregate(dag: &dag::MaintenanceDag) -> bool {
    let dag::OpNode::Project {
        input: proj_input, ..
    } = dag.root_node()
    else {
        return false;
    };
    let below = match &dag.nodes[*proj_input] {
        dag::OpNode::Filter { input, .. } => *input,
        _ => *proj_input,
    };
    matches!(dag.nodes[below], dag::OpNode::Aggregate { .. })
}

/// Whether the DAG is a join view: `Project` root over a `Join`, optionally
/// with a WHERE `Filter` between them.
fn dag_is_join(dag: &dag::MaintenanceDag) -> bool {
    let dag::OpNode::Project {
        input: proj_input, ..
    } = dag.root_node()
    else {
        return false;
    };
    let below = match &dag.nodes[*proj_input] {
        dag::OpNode::Filter { input, .. } => *input,
        _ => *proj_input,
    };
    matches!(dag.nodes[below], dag::OpNode::Join { .. })
}

/// Whether the DAG is a set-op view over filter/project branches: a `SetOp`
/// root whose every other node is linear. A `SetOp` with a join or aggregate
/// branch (`CompoundAll` over non-filter/project branches) is not handled by
/// [`emit_compound_program`] and stays on the per-shape path.
fn dag_is_compound(dag: &dag::MaintenanceDag) -> bool {
    if !matches!(dag.root_node(), dag::OpNode::SetOp { .. }) {
        return false;
    }
    dag.nodes.iter().enumerate().all(|(id, node)| {
        id == dag.root
            || matches!(
                node,
                dag::OpNode::Scan { .. } | dag::OpNode::Filter { .. } | dag::OpNode::Project { .. }
            )
    })
}

/// The filter/project [`BranchContent`] a compound `SetOp` branch produces,
/// read from its `Project` -> `Filter`? -> `Scan` sub-tree.
fn dag_branch_content(
    dag: &dag::MaintenanceDag,
    branch_root: dag::NodeId,
) -> Result<BranchContent> {
    let bad = || LimboError::InternalError("compound branch is not filter/project".to_string());
    let dag::OpNode::Project {
        input: proj_input,
        projections,
    } = &dag.nodes[branch_root]
    else {
        return Err(bad());
    };
    let (scan_id, where_clause): (dag::NodeId, Option<ast::Expr>) = match &dag.nodes[*proj_input] {
        dag::OpNode::Filter { input, predicate } => (*input, Some(predicate.clone())),
        dag::OpNode::Scan { .. } => (*proj_input, None),
        _ => return Err(bad()),
    };
    let dag::OpNode::Scan { table, identifier } = &dag.nodes[scan_id] else {
        return Err(bad());
    };
    let columns = projections
        .iter()
        .map(|(expr, alias)| {
            ast::ResultColumn::Expr(
                Box::new(expr.clone()),
                alias
                    .clone()
                    .map(|name| ast::As::As(ast::Name::exact(name))),
            )
        })
        .collect();
    Ok(BranchContent {
        base_table: table.clone(),
        identifier: identifier.clone(),
        columns,
        where_clause,
    })
}

/// Reconstruct the [`OutputColumn`] mapping for an aggregate view from the
/// `Project` node's expressions, resolving each against the `Aggregate` node's
/// group keys and aggregate calls (which already collect every aggregate
/// appearing in the outputs or HAVING).
fn dag_aggregate_outputs(
    projections: &[(ast::Expr, Option<String>)],
    group_exprs: &[ast::Expr],
    aggregates: &[Aggregate],
) -> Vec<OutputColumn> {
    projections
        .iter()
        .map(|(expr, _)| {
            if let Some(idx) = group_exprs
                .iter()
                .position(|g| crate::util::exprs_are_equivalent(g, expr))
            {
                OutputColumn::Group(idx)
            } else if let Some(idx) = aggregates
                .iter()
                .position(|a| crate::util::exprs_are_equivalent(&a.original_expr, expr))
            {
                OutputColumn::Aggregate(idx)
            } else {
                OutputColumn::Expr(expr.clone())
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn compile_dag_compound_program(
    view_name: &str,
    dag: &dag::MaintenanceDag,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let dag::OpNode::SetOp {
        inputs,
        operators,
        prefix_len,
        key_collations,
        ..
    } = dag.root_node()
    else {
        unreachable!("caller checked compound shape");
    };
    let branches = inputs
        .iter()
        .map(|&branch_root| dag_branch_content(dag, branch_root))
        .collect::<Result<Vec<_>>>()?;
    emit_compound_program(
        view_name,
        &branches,
        operators,
        *prefix_len,
        key_collations,
        view_root_page,
        num_view_columns,
        input,
        schema,
        connection,
    )
}

/// Whether the DAG is a pure UNION ALL over branches that are not all
/// filter/project (at least one join or aggregate branch): a `SetOp` root
/// that [`dag_is_compound`] does not cover. Each branch writes a disjoint
/// rowid slice of the view; there is no cross-branch merge.
fn dag_is_compound_all(dag: &dag::MaintenanceDag) -> bool {
    matches!(dag.root_node(), dag::OpNode::SetOp { .. }) && !dag_is_compound(dag)
}

#[allow(clippy::too_many_arguments)]
fn compile_dag_compound_all_program(
    view_name: &str,
    select: &ast::Select,
    dag: &dag::MaintenanceDag,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let dag::OpNode::SetOp { inputs, .. } = dag.root_node() else {
        unreachable!("caller checked compound-all shape");
    };
    let n_branches = inputs.len();
    let mut program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts {
            num_cursors: 6 * n_branches,
            approx_num_insns: 128 * n_branches + 32 * num_view_columns * n_branches,
            approx_num_labels: 48 * n_branches,
        },
    );
    program.prologue();
    for (branch_idx, &branch_root) in inputs.iter().enumerate() {
        let ns = BranchNamespace {
            scale: n_branches as i64,
            offset: branch_idx as i64,
            suffix: format!("__b{branch_idx}"),
        };
        let branch = branch_select(select, branch_idx);
        emit_dag_view(
            &mut program,
            view_name,
            dag,
            branch_root,
            &branch,
            view_root_page,
            num_view_columns,
            input,
            MaintenanceOutput::ViewBtree,
            &ns,
            schema,
            connection,
        )?;
    }
    program.epilogue(schema);
    program.build(
        connection.clone(),
        false,
        "materialized view compound-all maintenance",
    )
}

// A single-table filter/project maintenance pipeline. Parameterized on its
// base table and its WHERE/projection so it serves both the standalone
// filter/project view (via `emit_filter_project_from_select`) and the DAG
// codegen's linear pipeline (a `Scan` → `Filter`? → `Project` chain).
#[allow(clippy::too_many_arguments)]
fn emit_filter_project(
    program: &mut ProgramBuilder,
    view_name: &str,
    base_table: &Arc<BTreeTable>,
    identifier: &str,
    columns: &[ast::ResultColumn],
    where_clause: Option<&ast::Expr>,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    output: MaintenanceOutput,
    ns: &BranchNamespace,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let num_base_columns = base_table.columns().len();

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
    let mut table_references = TableReferences::new(
        vec![JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            table: crate::schema::Table::BTree(base_table.clone()),
            identifier: identifier.to_string(),
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
        let mut bound = where_expr.clone();
        bind_and_rewrite_expr(
            &mut bound,
            Some(&mut table_references),
            None,
            &resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        let pred_true = program.allocate_label();
        translate_condition_expr(
            program,
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
                    program,
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

            // The view key namespaces the source rowid into this branch's
            // slice of a shared compound btree (identity for a standalone
            // view).
            let view_key_reg = ns.emit_view_rowid(program, rowid_reg);
            program.emit_insn(Insn::SeekRowid {
                cursor_id: view_cursor_id,
                src_reg: view_key_reg,
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
                key_reg: view_key_reg,
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

    drop(syms);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_group_aggregate(
    program: &mut ProgramBuilder,
    view_name: &str,
    select: &ast::Select,
    group_exprs: &[ast::Expr],
    group_collations: &[CollationSeq],
    aggregates: &[Aggregate],
    outputs: &[OutputColumn],
    having: Option<&ast::Expr>,
    scalar: bool,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    ns: &BranchNamespace,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    use crate::function::AccumulatorFunc;
    // With a collated key, values that compare equal can differ in bytes.
    // The stored key is the group's first-seen representative: reloading it
    // on every lookup keeps the state row, its index entry, and the view row
    // byte-identical, and matches the value batch GROUP BY displays. With
    // binary keys a matched lookup implies byte equality, so the reload is
    // skipped.
    let reload_stored_group = group_collations
        .iter()
        .any(|c| !matches!(c, CollationSeq::Binary | CollationSeq::Unset));
    let ast::OneSelect::Select {
        from, where_clause, ..
    } = &select.body.select
    else {
        unreachable!("classify_view admits only OneSelect::Select");
    };
    let from = from.as_ref().expect("classify_view requires a FROM clause");
    // Aggregates over a join read their input from a joined-delta ephemeral
    // table instead of directly from a base table's delta or btree.
    let joined = !from.joins.is_empty();

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
    let has_multiset = aggregates.iter().any(uses_multiset);

    // The state table and its primary-key index are real schema objects
    // created by CREATE MATERIALIZED VIEW; the namespace suffix keeps each
    // compound branch's tables distinct.
    let state_table_name = format!(
        "{}{}_{view_name}{}",
        crate::schema::DBSP_TABLE_PREFIX,
        crate::incremental::view::DBSP_CIRCUIT_VERSION,
        ns.suffix,
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
        "{}{}_{view_name}{}",
        crate::schema::DBSP_MULTISET_TABLE_PREFIX,
        crate::incremental::view::DBSP_CIRCUIT_VERSION,
        ns.suffix,
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

    // Input relation: for a single table, its transaction delta or a full
    // scan; for a join, the joined-delta ephemeral table built below. The
    // table references are what group/argument/HAVING expressions bind
    // against — for joins the values are pre-evaluated into the ephemeral
    // rows, so only HAVING still binds against them (and resolves entirely
    // through the expression cache).
    let (input_cursor_id, mut table_references, join_ctx, num_base_columns) = if joined {
        let (eph_cursor_id, join_ctx, table_references) = emit_join_deltas_to_ephemeral(
            program,
            &resolver,
            view_name,
            from,
            where_clause,
            group_exprs,
            aggregates,
            input,
            schema,
        )?;
        (eph_cursor_id, table_references, Some(join_ctx), 0)
    } else {
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
        let table_ref_id = program.table_reference_counter.next();
        let identifier = match base_alias {
            Some(ast::As::As(name) | ast::As::Elided(name) | ast::As::ImplicitColumnName(name)) => {
                name.as_str().to_string()
            }
            None => base_table.name.clone(),
        };
        let table_references = TableReferences::new(
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
        let cursor_id = match input {
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
        (cursor_id, table_references, None, num_base_columns)
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

    // Joined-delta rows apply in two passes: every positive contribution,
    // then every negative one. The single-table capture order guarantees a
    // group's running weight never dips below zero, but the join phase
    // decomposition does not — a pair that exists only across generations
    // contributes its retraction before its insertion, and the
    // unknown-group-retraction no-op below would drop it, leaving the later
    // positive over-counted. Positives-first restores the invariant: every
    // group's (and multiset value's) running weight stays non-negative, and
    // since aggregate contributions carry their own values, reordering them
    // is sound.
    let pass_reg = join_ctx.as_ref().map(|_| {
        let pass_reg = program.alloc_register();
        program.emit_int(0, pass_reg);
        pass_reg
    });
    let pass_start_label = program.allocate_label();
    program.preassign_label_to_next_insn(pass_start_label);
    let pass_done_label = if pass_reg.is_some() {
        program.allocate_label()
    } else {
        end_label
    };

    program.emit_insn(Insn::Rewind {
        cursor_id: input_cursor_id,
        pc_if_empty: pass_done_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    // Accumulators must start fresh for every input row.
    program.emit_insn(Insn::Null {
        dest: acc_start,
        dest_end: Some(acc_start + aggregates.len() - 1),
    });

    // Weight of the current input row.
    if let Some(join_ctx) = &join_ctx {
        program.emit_insn(Insn::Column {
            cursor_id: input_cursor_id,
            column: join_ctx.k + join_ctx.n_args,
            dest: weight_reg,
            default: None,
        });
        // Pass filter: pass 0 skips negative contributions, pass 1 skips
        // positive ones.
        let pass_reg = pass_reg.expect("pass register allocated for joins");
        let negative_pass_label = program.allocate_label();
        let pass_ok_label = program.allocate_label();
        program.emit_insn(Insn::If {
            reg: pass_reg,
            target_pc: negative_pass_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Lt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: pass_ok_label,
        });
        program.preassign_label_to_next_insn(negative_pass_label);
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.preassign_label_to_next_insn(pass_ok_label);
    } else {
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
    }

    // WHERE: rows whose predicate is FALSE or NULL do not reach the group.
    // (For joins, ON and WHERE were already applied while building the
    // joined-delta rows.)
    if join_ctx.is_none() {
        if let Some(where_expr) = where_clause {
            let mut bound = where_expr.clone();
            bind_and_rewrite_expr(
                &mut bound,
                Some(&mut table_references),
                None,
                &resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
            let pred_true = program.allocate_label();
            translate_condition_expr(
                program,
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
    }

    // Group key expressions (pre-evaluated into the leading joined-delta
    // columns for joins).
    if join_ctx.is_some() {
        for i in 0..k {
            program.emit_insn(Insn::Column {
                cursor_id: input_cursor_id,
                column: i,
                dest: group_start + i,
                default: None,
            });
        }
    } else {
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
                program,
                Some(&table_references),
                &bound,
                group_start + i,
                &resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }
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
    if reload_stored_group {
        for i in 0..k {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: i,
                dest: group_start + i,
                default: None,
            });
        }
    }
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
        let col_reg = if let Some(join_ctx) = &join_ctx {
            // Argument values were pre-evaluated into the joined-delta row.
            match join_ctx.arg_positions[i] {
                Some(pos) => {
                    program.emit_insn(Insn::Column {
                        cursor_id: input_cursor_id,
                        column: k + pos,
                        dest: arg_reg,
                        default: None,
                    });
                    arg_reg
                }
                None => null_arg_reg,
            }
        } else {
            match agg.args.first() {
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
                        program,
                        Some(&table_references),
                        &bound,
                        arg_reg,
                        &resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                    arg_reg
                }
                None => null_arg_reg,
            }
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
    // group has been retracted. A scalar aggregate's single group is never
    // deleted — retracting the last row rewrites its row with empty-input
    // aggregate values instead.
    if !scalar {
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
        // Without HAVING every live group has its view row, so a missing row
        // is corruption; with HAVING the dying group may have been
        // suppressed.
        let view_rowid_reg = ns.emit_view_rowid(program, state_rowid_reg);
        program.emit_insn(Insn::SeekRowid {
            cursor_id: view_cursor_id,
            src_reg: view_rowid_reg,
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
        program.preassign_label_to_next_insn(write_label);
    }

    // Group live: persist aggregate state and (re)write the view row.
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
        table_name: state_table_name.clone(),
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

    // HAVING and expression output columns both evaluate over the finalized
    // group row: group expressions and aggregate calls inside them resolve to
    // their computed registers through the expression cache, exactly like
    // HAVING translation in a regular GROUP BY query. HAVING gates only the
    // view row — the group's state stays maintained above so the predicate
    // flips the row in and out as aggregates change. Bind and seed once —
    // the row tail below is emitted twice for scalar population.
    let has_expr_outputs = outputs.iter().any(|o| matches!(o, OutputColumn::Expr(_)));
    let mut bound_having: Option<ast::Expr> = None;
    let mut bound_output_exprs: Vec<Option<ast::Expr>> = vec![None; outputs.len()];
    if having.is_some() || has_expr_outputs {
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
        if let Some(having_expr) = having {
            let mut bound = having_expr.clone();
            bind_and_rewrite_expr(
                &mut bound,
                Some(&mut table_references),
                None,
                &resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
            bound_having = Some(bound);
        }
        for (i, output) in outputs.iter().enumerate() {
            let OutputColumn::Expr(expr) = output else {
                continue;
            };
            let mut bound = expr.clone();
            bind_and_rewrite_expr(
                &mut bound,
                Some(&mut table_references),
                None,
                &resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
            bound_output_exprs[i] = Some(bound);
        }
    }

    // The row tail: finalize every aggregate's value, evaluate HAVING, and
    // write (or remove) the group's view row. Falls through — and jumps on
    // suppression — to `done_label`, which the caller places right after.
    let emit_row_tail =
        |program: &mut ProgramBuilder, done_label: crate::vdbe::BranchOffset| -> Result<()> {
            for (i, agg) in aggregates.iter().enumerate() {
                let value_reg = agg_value_start + i;
                if matches!(agg.func, AggFunc::Min | AggFunc::Max) {
                    // The group's extreme is the first (MIN) or last (MAX)
                    // multiset entry with the (agg_id, group..) prefix.
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

            let suppress_label = bound_having.as_ref().map(|_| program.allocate_label());
            if let Some(bound) = &bound_having {
                let suppress_label = suppress_label.expect("allocated above");
                let pass_label = program.allocate_label();
                translate_condition_expr(
                    program,
                    &table_references,
                    bound,
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
                    OutputColumn::Expr(_) => {
                        let bound = bound_output_exprs[out_idx]
                            .as_ref()
                            .expect("Expr outputs are bound before the row tail");
                        translate_expr_no_constant_opt(
                            program,
                            Some(&table_references),
                            bound,
                            out_start + out_idx,
                            &resolver,
                            NoConstantOptReason::RegisterReuse,
                        )?;
                        continue;
                    }
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
            let view_rowid_reg = ns.emit_view_rowid(program, state_rowid_reg);
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

            if let Some(suppress_label) = suppress_label {
                // Group failed HAVING: remove its previously-visible row, if
                // any.
                program.emit_insn(Insn::Goto {
                    target_pc: done_label,
                });
                program.preassign_label_to_next_insn(suppress_label);
                let view_rowid_reg = ns.emit_view_rowid(program, state_rowid_reg);
                program.emit_insn(Insn::SeekRowid {
                    cursor_id: view_cursor_id,
                    src_reg: view_rowid_reg,
                    target_pc: done_label,
                });
                program.emit_insn(Insn::Delete {
                    cursor_id: view_cursor_id,
                    table_name: view_name.to_string(),
                    is_part_of_update: true,
                });
            }
            Ok(())
        };

    emit_row_tail(program, next_label)?;

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input_cursor_id,
        pc_if_next: loop_label,
    });
    if let Some(pass_reg) = pass_reg {
        program.preassign_label_to_next_insn(pass_done_label);
        program.emit_insn(Insn::If {
            reg: pass_reg,
            target_pc: end_label,
            jump_if_null: false,
        });
        program.emit_int(1, pass_reg);
        program.emit_insn(Insn::Goto {
            target_pc: pass_start_label,
        });
    }
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

    // Batch semantics emit one row for a scalar aggregate even over empty
    // input, so population must leave the single group present when the scan
    // contributed nothing (empty table, or WHERE filtered every row).
    // Maintenance never needs this: the row is created here and, with the
    // group-death path disabled for scalar shapes, never deleted.
    if scalar && input == MaintenanceInput::BaseTable {
        let ensure_done = program.allocate_label();
        // The synthetic group key is the constant 0 by construction.
        program.emit_int(0, group_start);
        program.emit_insn(Insn::Found {
            cursor_id: state_index_cursor_id,
            target_pc: ensure_done,
            record_reg: group_start,
            num_regs: k,
        });
        // Fresh, empty accumulators: stepping a NULL argument materializes
        // each payload context without contributing to it — except COUNT(*),
        // which counts rows regardless of argument, so its init step is
        // cancelled with the matching inverse.
        program.emit_insn(Insn::Null {
            dest: acc_start,
            dest_end: Some(acc_start + aggregates.len() - 1),
        });
        for (i, agg) in aggregates.iter().enumerate() {
            if payload_offsets[i].is_none() {
                continue; // MIN/MAX: an empty multiset already means NULL
            }
            program.emit_insn(Insn::AggStep {
                acc_reg: acc_start + i,
                col: null_arg_reg,
                delimiter: 0,
                func: AccumulatorFunc::Agg(agg.func.clone()),
                comparator: None,
            });
            if matches!(agg.func, AggFunc::Count0) {
                program.emit_insn(Insn::AggInverse {
                    acc_reg: acc_start + i,
                    col: null_arg_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::AggContextStore {
                acc_reg: acc_start + i,
                payload_start_reg: payload_start + payload_offsets[i].expect("checked above"),
                func: AccumulatorFunc::Agg(agg.func.clone()),
            });
        }
        program.emit_insn(Insn::NewRowid {
            cursor: state_cursor_id,
            rowid_reg: state_rowid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
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
        emit_row_tail(program, ensure_done)?;
        program.preassign_label_to_next_insn(ensure_done);
    }

    drop(syms);
    Ok(())
}

/// Which relation a join phase reads for one side of the join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    /// The transaction's captured delta for the table.
    Delta,
    /// The table's btree, which at maintenance time holds the post-change
    /// state.
    Btree,
}

/// The delta phases of an N-way inner join. With the base btrees holding
/// post-change state at commit time, the view delta is the
/// inclusion–exclusion sum over every non-empty subset S of the joined
/// tables: join the deltas of the tables in S against the post-state btrees
/// of the rest, signed (-1)^(|S|+1). The two-table case is the familiar
/// `d(L ⋈ R) = dL ⋈ R_new + L_new ⋈ dR − dL ⋈ dR`. Population is a single
/// all-btree phase with weight +1.
fn join_subset_phases(n_tables: usize, input: MaintenanceInput) -> Vec<(Vec<JoinSide>, bool)> {
    match input {
        MaintenanceInput::BaseTable => vec![(vec![JoinSide::Btree; n_tables], false)],
        MaintenanceInput::TransactionDelta => (1u32..(1 << n_tables))
            .map(|mask| {
                let sides = (0..n_tables)
                    .map(|i| {
                        if mask & (1 << i) != 0 {
                            JoinSide::Delta
                        } else {
                            JoinSide::Btree
                        }
                    })
                    .collect();
                (sides, mask.count_ones() % 2 == 0)
            })
            .collect(),
    }
}

/// A [`JoinedTable`] scan entry for synthesized maintenance-program table
/// references. `using` carries the table's merged USING/NATURAL column
/// names so unqualified references to them bind without ambiguity, exactly
/// as in the defining query.
fn make_joined_table(
    table: &Arc<BTreeTable>,
    identifier: &str,
    using: &[String],
    id: TableInternalId,
) -> JoinedTable {
    JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        table: crate::schema::Table::BTree(table.clone()),
        identifier: identifier.to_string(),
        internal_id: id,
        join_info: (!using.is_empty()).then(|| JoinInfo {
            join_type: crate::translate::plan::JoinType::Inner,
            using: using.iter().map(|n| ast::Name::exact(n.clone())).collect(),
            no_reorder: false,
        }),
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: 0,
        indexed: None,
    }
}

/// A joined table with the identifier its columns bind under and the column
/// names its USING (or NATURAL) join constraint merges.
type JoinTable = (Arc<BTreeTable>, String, Vec<String>);

/// Per-match emission hook for [`emit_join_phase`]: receives the program,
/// the per-table-position cursor ids, and the phase's table references.
type JoinPhaseSink<'a> =
    dyn FnMut(&mut ProgramBuilder, &[usize], &mut TableReferences) -> Result<()> + 'a;

/// Resolve the collation each key term compares under in the defining query,
/// by binding it against the FROM tables and asking the same resolver that
/// regular GROUP BY / ORDER BY translation asks. The result is carried onto
/// the hidden state tables' key columns, so their primary-key indexes group
/// and dedup exactly like the batch query. Custom collations are rejected:
/// they are connection-local and cannot be embedded in the hidden tables'
/// CREATE TABLE.
fn resolve_key_collations(
    terms: &[ast::Expr],
    from: &ast::FromClause,
    schema: &Schema,
    resolver: &Resolver,
) -> Result<Vec<CollationSeq>> {
    Ok(resolve_term_collations(terms, from, schema, resolver)?
        .into_iter()
        .map(|collation| collation.unwrap_or(CollationSeq::Binary))
        .collect())
}

/// Like [`resolve_key_collations`], but keeps "no collation resolved"
/// distinct from BINARY: UNION dedup folds branch collations with left
/// precedence, where an unresolved left column (e.g. a literal) falls
/// through to the right branch's collation.
fn resolve_term_collations(
    terms: &[ast::Expr],
    from: &ast::FromClause,
    schema: &Schema,
    resolver: &Resolver,
) -> Result<Vec<Option<CollationSeq>>> {
    let (tables, _) = collect_join_tables(from, schema)?;
    let joined = tables
        .iter()
        .enumerate()
        .map(|(i, (table, identifier, using))| {
            make_joined_table(table, identifier, using, TableInternalId::from(i))
        })
        .collect();
    let mut table_references = TableReferences::new(joined, vec![]);
    terms
        .iter()
        .map(|term| {
            let mut bound = term.clone();
            bind_and_rewrite_expr(
                &mut bound,
                Some(&mut table_references),
                None,
                resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
            let collation = get_collseq_from_expr_with_symbols(
                &bound,
                &table_references,
                Some(resolver.symbol_table),
            )?;
            if collation.is_some_and(|c| c.is_custom()) {
                return unsupported("custom collations on grouping or dedup keys");
            }
            Ok(collation.map(|c| match c {
                CollationSeq::Unset => CollationSeq::Binary,
                other => other,
            }))
        })
        .collect()
}

/// Extract the joined tables (with their binding identifiers) and the ON
/// predicates of a join view's FROM clause.
fn collect_join_tables(
    from: &ast::FromClause,
    schema: &Schema,
) -> Result<(Vec<JoinTable>, Vec<ast::Expr>)> {
    let identifier_for = |alias: &Option<ast::As>, table: &Arc<BTreeTable>| match alias {
        Some(ast::As::As(name) | ast::As::Elided(name) | ast::As::ImplicitColumnName(name)) => {
            name.as_str().to_string()
        }
        None => table.name.clone(),
    };
    let lookup = |name: &ast::QualifiedName| {
        schema.get_btree_table(name.name.as_str()).ok_or_else(|| {
            LimboError::ParseError(format!(
                "materialized view base table {} not found",
                name.name.as_str()
            ))
        })
    };
    // Per joined table, the columns its USING (or NATURAL) constraint
    // merges. Each merged column desugars into an equality between the
    // left-most earlier table that has it and the joined table, exactly the
    // predicates the planner generates for the defining query.
    let using_per_table = crate::util::join_using_columns(from, schema)?;

    let mut tables: Vec<JoinTable> = Vec::with_capacity(from.joins.len() + 1);
    let ast::SelectTable::Table(name, alias, _) = from.select.as_ref() else {
        unreachable!("classify_view admits only plain tables in FROM");
    };
    let table = lookup(name)?;
    let identifier = identifier_for(alias, &table);
    tables.push((table, identifier, Vec::new()));

    let mut conditions = Vec::new();
    for (join_idx, join) in from.joins.iter().enumerate() {
        let ast::SelectTable::Table(name, alias, _) = join.table.as_ref() else {
            unreachable!("classify_view admits only plain tables in joins");
        };
        let table = lookup(name)?;
        let identifier = identifier_for(alias, &table);
        let using = using_per_table[join_idx + 1].clone();
        for column_name in &using {
            let left_identifier = tables
                .iter()
                .find(|(left_table, _, _)| {
                    left_table.columns().iter().any(|col| {
                        col.name
                            .as_deref()
                            .is_some_and(|n| n.eq_ignore_ascii_case(column_name))
                    })
                })
                .map(|(_, left_identifier, _)| left_identifier.clone())
                .expect("join_using_columns verified the column exists on the left");
            conditions.push(ast::Expr::Binary(
                Box::new(ast::Expr::Qualified(
                    ast::Name::exact(left_identifier),
                    ast::Name::exact(column_name.clone()),
                )),
                ast::Operator::Equals,
                Box::new(ast::Expr::Qualified(
                    ast::Name::exact(identifier.clone()),
                    ast::Name::exact(column_name.clone()),
                )),
            ));
        }
        tables.push((table, identifier, using));
        match &join.constraint {
            Some(ast::JoinConstraint::On(expr)) => conditions.push(expr.as_ref().clone()),
            None | Some(ast::JoinConstraint::Using(_)) => {}
        }
    }
    Ok((tables, conditions))
}

/// Emit one join delta phase: nested loops over every joined table — reading
/// the transaction delta for tables in the phase's subset and the post-state
/// btree for the rest — with the delta tables outermost so the probe scans
/// run on the btree side. At the innermost level the ON predicates and WHERE
/// are evaluated, the signed weight product lands in `w_reg`, and `sink`
/// emits the per-match work with all cursors positioned (its slice maps
/// table positions to cursor ids).
#[allow(clippy::too_many_arguments)]
fn emit_join_phase(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    view_name: &str,
    tables: &[JoinTable],
    conditions: &[&ast::Expr],
    sides: &[JoinSide],
    negate: bool,
    w_reg: usize,
    sink: &mut JoinPhaseSink<'_>,
) -> Result<()> {
    let n = tables.len();

    // Fresh table references per phase: the same logical table reads from a
    // different relation (delta vs btree) in each phase, and cursors are
    // keyed by the internal id expressions bind against.
    let ids: Vec<_> = (0..n)
        .map(|_| program.table_reference_counter.next())
        .collect();
    let mut table_references = TableReferences::new(
        tables
            .iter()
            .zip(&ids)
            .map(|((table, ident, using), id)| make_joined_table(table, ident, using, *id))
            .collect(),
        vec![],
    );

    let cursor_ids: Vec<usize> = tables
        .iter()
        .zip(&ids)
        .zip(sides)
        .map(|(((table, _, _), id), side)| {
            let cursor_id = match side {
                JoinSide::Delta => program.alloc_cursor_id_keyed(
                    CursorKey::table(*id),
                    CursorType::ViewDelta {
                        view_name: view_name.to_string(),
                        table: table.clone(),
                    },
                ),
                JoinSide::Btree => program.alloc_cursor_id_keyed(
                    CursorKey::table(*id),
                    CursorType::BTreeTable(table.clone()),
                ),
            };
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: match side {
                    JoinSide::Delta => 0,
                    JoinSide::Btree => table.root_page,
                },
                db: 0,
            });
            cursor_id
        })
        .collect();

    // Nest delta tables outermost.
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| sides[i] == JoinSide::Btree);

    let phase_done_label = program.allocate_label();
    let loop_labels: Vec<_> = (0..n).map(|_| program.allocate_label()).collect();
    let next_labels: Vec<_> = (0..n).map(|_| program.allocate_label()).collect();
    let delta_w_regs: Vec<Option<usize>> = sides
        .iter()
        .map(|side| (*side == JoinSide::Delta).then(|| program.alloc_register()))
        .collect();

    for (depth, &pos) in order.iter().enumerate() {
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor_ids[pos],
            pc_if_empty: if depth == 0 {
                phase_done_label
            } else {
                next_labels[depth - 1]
            },
        });
        program.preassign_label_to_next_insn(loop_labels[depth]);
        if let Some(w) = delta_w_regs[pos] {
            program.emit_insn(Insn::Column {
                cursor_id: cursor_ids[pos],
                column: tables[pos].0.columns().len(),
                dest: w,
                default: None,
            });
        }
    }

    // ON predicates and WHERE: a FALSE or NULL result means this combination
    // is not a join output.
    let innermost_next = next_labels[n - 1];
    for condition in conditions {
        let mut bound = (*condition).clone();
        bind_and_rewrite_expr(
            &mut bound,
            Some(&mut table_references),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        let pred_true = program.allocate_label();
        translate_condition_expr(
            program,
            &table_references,
            &bound,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: pred_true,
                jump_target_when_false: innermost_next,
                jump_target_when_null: innermost_next,
            },
            resolver,
        )?;
        program.preassign_label_to_next_insn(pred_true);
    }

    // Signed weight: the product of the delta weights times the phase sign.
    program.emit_int(if negate { -1 } else { 1 }, w_reg);
    for w in delta_w_regs.iter().flatten() {
        program.emit_insn(Insn::Multiply {
            lhs: w_reg,
            rhs: *w,
            dest: w_reg,
        });
    }

    sink(program, &cursor_ids, &mut table_references)?;

    for depth in (0..n).rev() {
        program.preassign_label_to_next_insn(next_labels[depth]);
        program.emit_insn(Insn::Next {
            cursor_id: cursor_ids[order[depth]],
            pc_if_next: loop_labels[depth],
        });
    }
    program.preassign_label_to_next_insn(phase_done_label);
    Ok(())
}

/// Layout of the joined-delta ephemeral rows feeding an aggregate-over-join
/// view: `[group keys.., aggregate arguments.., weight]`.
struct JoinAggCtx {
    /// Number of group-key columns.
    k: usize,
    /// Number of pre-evaluated aggregate-argument columns.
    n_args: usize,
    /// For each aggregate, the position of its argument within the argument
    /// block; None for argument-less aggregates (COUNT(*)).
    arg_positions: Vec<Option<usize>>,
}

/// Emit the join half of an aggregate-over-join program: the join delta
/// phases evaluate each matched pair's group keys, aggregate arguments, and
/// contribution weight — with both sides' cursors positioned, so expressions
/// over either table bind normally — and append them as rows of an ephemeral
/// table. The caller's aggregate loop then consumes those rows exactly like
/// unit-weight delta rows, reading values by column position.
///
/// Returns the ephemeral cursor, the row layout, and a table-references
/// instance over both join tables for binding HAVING (whose column-bearing
/// subtrees all resolve through the expression cache, never the cursors).
#[allow(clippy::too_many_arguments)]
fn emit_join_deltas_to_ephemeral(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    view_name: &str,
    from: &ast::FromClause,
    where_clause: &Option<Box<ast::Expr>>,
    group_exprs: &[ast::Expr],
    aggregates: &[Aggregate],
    input: MaintenanceInput,
    schema: &Schema,
) -> Result<(usize, JoinAggCtx, TableReferences)> {
    let (tables, on_conditions) = collect_join_tables(from, schema)?;
    let conditions: Vec<&ast::Expr> = on_conditions
        .iter()
        .chain(where_clause.as_deref())
        .collect();

    let k = group_exprs.len();
    let arg_positions: Vec<Option<usize>> = aggregates
        .iter()
        .scan(0usize, |pos, agg| {
            Some(agg.args.first().map(|_| {
                let this = *pos;
                *pos += 1;
                this
            }))
        })
        .collect();
    let n_args = arg_positions.iter().flatten().count();

    // The ephemeral rows are [g.., args.., weight]; the synthesized table's
    // trailing weight column provides the extra slot.
    let eph_table = Arc::new(synthesized_view_table(
        &format!("{view_name}_joined_delta"),
        0,
        k + n_args,
    ));
    let eph_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(eph_table));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: eph_cursor_id,
        is_table: true,
    });

    // [g.., args.., weight], contiguous for MakeRecord.
    let eph_rec_start = program.alloc_registers(k + n_args + 1);
    let eph_weight_reg = eph_rec_start + k + n_args;
    let eph_rowid_reg = program.alloc_register();
    let eph_record_reg = program.alloc_register();

    for (sides, negate) in join_subset_phases(tables.len(), input) {
        emit_join_phase(
            program,
            resolver,
            view_name,
            &tables,
            &conditions,
            &sides,
            negate,
            eph_weight_reg,
            &mut |program, _cursors, table_references| {
                for (i, group_expr) in group_exprs.iter().enumerate() {
                    let mut bound = group_expr.clone();
                    bind_and_rewrite_expr(
                        &mut bound,
                        Some(table_references),
                        None,
                        resolver,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                    translate_expr_no_constant_opt(
                        program,
                        Some(table_references),
                        &bound,
                        eph_rec_start + i,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                }
                for (agg, pos) in aggregates.iter().zip(&arg_positions) {
                    let (Some(arg), Some(pos)) = (agg.args.first(), pos) else {
                        continue;
                    };
                    let mut bound = arg.clone();
                    bind_and_rewrite_expr(
                        &mut bound,
                        Some(table_references),
                        None,
                        resolver,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                    translate_expr_no_constant_opt(
                        program,
                        Some(table_references),
                        &bound,
                        eph_rec_start + k + pos,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                }
                program.emit_insn(Insn::MakeRecord {
                    start_reg: eph_rec_start as u16,
                    count: (k + n_args + 1) as u16,
                    dest_reg: eph_record_reg as u16,
                    index_name: None,
                    affinity_str: None,
                });
                program.emit_insn(Insn::NewRowid {
                    cursor: eph_cursor_id,
                    rowid_reg: eph_rowid_reg,
                    prev_largest_reg: 0,
                });
                program.emit_insn(Insn::Insert {
                    cursor: eph_cursor_id,
                    key_reg: eph_rowid_reg,
                    record_reg: eph_record_reg,
                    flag: InsertFlags::new().is_ephemeral_table_insert(),
                    table_name: String::new(),
                });
                Ok(())
            },
        )?;
    }

    // Table references over the join tables for the caller's HAVING
    // translation.
    let having_references = TableReferences::new(
        tables
            .iter()
            .map(|(table, ident, using)| {
                let id = program.table_reference_counter.next();
                make_joined_table(table, ident, using, id)
            })
            .collect(),
        vec![],
    );

    Ok((
        eph_cursor_id,
        JoinAggCtx {
            k,
            n_args,
            arg_positions,
        },
        having_references,
    ))
}

#[allow(clippy::too_many_arguments)]
fn emit_join(
    program: &mut ProgramBuilder,
    view_name: &str,
    columns: &[ast::ResultColumn],
    tables: &[JoinTable],
    on_conditions: &[ast::Expr],
    where_clause: Option<&ast::Expr>,
    left_outer: bool,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    ns: &BranchNamespace,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let n = tables.len();
    turso_assert!(
        !left_outer || n == 2,
        "classify_view admits LEFT JOIN for exactly two tables"
    );
    // For an inner join ON and WHERE gate matches identically, so they merge
    // into one innermost predicate list. For LEFT JOIN they differ: ON
    // decides whether a pair is a match (which suppresses the NULL-padded
    // row), while WHERE only decides whether the matched row reaches the
    // view — a pair passing ON but failing WHERE still counts as a match
    // and produces nothing.
    let conditions: Vec<&ast::Expr> = if left_outer {
        on_conditions.iter().collect()
    } else {
        on_conditions.iter().chain(where_clause).collect()
    };

    let state_table_name = format!(
        "{}{}_{view_name}{}",
        crate::schema::DBSP_TABLE_PREFIX,
        crate::incremental::view::DBSP_CIRCUIT_VERSION,
        ns.suffix,
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
    // LEFT JOIN: per-left-row (present, matches, padded) bookkeeping in the
    // auxiliary table.
    let aux = if left_outer {
        let name = format!(
            "{}{}_{view_name}{}",
            crate::schema::DBSP_MULTISET_TABLE_PREFIX,
            crate::incremental::view::DBSP_CIRCUIT_VERSION,
            ns.suffix,
        );
        let table = schema.get_btree_table(&name).ok_or_else(|| {
            LimboError::InternalError(format!(
                "aux table {name} of materialized view {view_name} not found"
            ))
        })?;
        let index = schema.get_indices(&name).next().cloned().ok_or_else(|| {
            LimboError::InternalError(format!(
                "aux table {name} of materialized view {view_name} has no index"
            ))
        })?;
        Some((name, table, index))
    } else {
        None
    };

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

    // LEFT JOIN cursors: the aux table with its index, and dedicated left
    // and right btree read cursors (with their own table references) for
    // writing the NULL-padded row — the right cursor is put on its null row
    // so every column read yields NULL.
    let outer = if let Some((aux_name, aux_table, aux_index)) = &aux {
        let aux_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(aux_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: aux_cursor_id,
            root_page: RegisterOrLiteral::Literal(aux_table.root_page),
            db: 0,
        });
        let aux_index_cursor_id =
            program.alloc_cursor_id(CursorType::BTreeIndex(aux_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: aux_index_cursor_id,
            root_page: RegisterOrLiteral::Literal(aux_index.root_page),
            db: 0,
        });

        let pad_ids: Vec<_> = (0..n)
            .map(|_| program.table_reference_counter.next())
            .collect();
        let mut pad_references = TableReferences::new(
            tables
                .iter()
                .zip(&pad_ids)
                .map(|((table, ident, using), id)| make_joined_table(table, ident, using, *id))
                .collect(),
            vec![],
        );
        let pad_cursors: Vec<usize> = tables
            .iter()
            .zip(&pad_ids)
            .map(|((table, _, _), id)| {
                let cursor_id = program.alloc_cursor_id_keyed(
                    CursorKey::table(*id),
                    CursorType::BTreeTable(table.clone()),
                );
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: table.root_page,
                    db: 0,
                });
                cursor_id
            })
            .collect();
        // Bind the WHERE clause against the padded-row references once; the
        // aux-merge subroutine evaluates it over (left row, NULL right).
        let pad_where = match where_clause {
            Some(where_expr) => {
                let mut bound = where_expr.clone();
                bind_and_rewrite_expr(
                    &mut bound,
                    Some(&mut pad_references),
                    None,
                    &resolver,
                    BindingBehavior::ResultColumnsNotAllowed,
                )?;
                Some(bound)
            }
            None => None,
        };
        Some((
            aux_name.clone(),
            aux_index.name.clone(),
            aux_cursor_id,
            aux_index_cursor_id,
            pad_references,
            pad_cursors,
            pad_where,
        ))
    } else {
        None
    };

    // Register layout, shared by all phases: the merge subroutine reads the
    // rowid tuple, the projected outputs, and the contribution weight.
    let zero_reg = program.alloc_register();
    let gosub_return_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    // [rowids.., state_rowid], contiguous for Found/IdxInsert/IdxDelete.
    let pair_start = program.alloc_registers(n + 1);
    let srid_reg = pair_start + n;
    // [outputs.., weight], contiguous for MakeRecord.
    let out_start = program.alloc_registers(num_view_columns + 1);
    let out_weight_reg = out_start + num_view_columns;
    // [rowids.., multiplicity], the state row image.
    let state_rec_start = program.alloc_registers(n + 1);
    let state_mult_reg = state_rec_start + n;
    let w_reg = program.alloc_register();
    let cur_w_reg = program.alloc_register();
    let new_w_reg = program.alloc_register();
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    let view_record_reg = program.alloc_register();
    // LEFT JOIN registers: view rowids split across the two state tables
    // (2*rowid for inner rows, 2*rowid + 1 for padded rows), and the
    // aux-merge subroutine's working set.
    let view_rowid_reg = program.alloc_register();
    let two_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let a_return_reg = program.alloc_register();
    // [l_rid, aux_rowid], contiguous for Found/IdxInsert/IdxDelete.
    let akey_start = program.alloc_registers(2);
    let a_srid_reg = akey_start + 1;
    // [l_rid, present, matches, padded], the aux row image.
    let a_rec_start = program.alloc_registers(4);
    let a_present_reg = a_rec_start + 1;
    let a_matches_reg = a_rec_start + 2;
    let a_padded_reg = a_rec_start + 3;
    let dp_reg = program.alloc_register();
    let dm_reg = program.alloc_register();
    let a_fresh_reg = program.alloc_register();
    let padded_new_reg = program.alloc_register();
    let a_state_record_reg = program.alloc_register();
    let a_index_record_reg = program.alloc_register();

    program.emit_int(0, zero_reg);
    program.emit_int(2, two_reg);
    program.emit_int(1, one_reg);

    let merge_label = program.allocate_label();
    let main_start_label = program.allocate_label();
    let corrupt_label = program.allocate_label();
    let end_label = program.allocate_label();

    program.emit_insn(Insn::Goto {
        target_pc: main_start_label,
    });

    // Weight-merge subroutine: apply (pair, outputs, w) to the state and
    // view btrees. The state row carries the pair's signed multiplicity: the
    // delta phase decomposition can retract a pair before the matching
    // insertion arrives (a pair that exists only across the transaction's
    // old/new generations), so multiplicities dip below zero transiently and
    // must be recorded rather than dropped — otherwise the later positive
    // contribution materializes a phantom row. The view row exists only
    // while the multiplicity is positive; after a full application every
    // pair converges to 0 (gone) or 1.
    program.preassign_label_to_next_insn(merge_label);
    let m_found_label = program.allocate_label();
    let m_update_label = program.allocate_label();
    let m_visible_label = program.allocate_label();
    let m_write_label = program.allocate_label();
    let m_ret_label = program.allocate_label();
    program.emit_insn(Insn::Found {
        cursor_id: state_index_cursor_id,
        target_pc: m_found_label,
        record_reg: pair_start,
        num_regs: n,
    });
    // Unknown tuple: record its multiplicity, whatever the sign.
    program.emit_insn(Insn::NewRowid {
        cursor: state_cursor_id,
        rowid_reg: srid_reg,
        prev_largest_reg: prev_rowid_scratch,
    });
    program.emit_insn(Insn::Copy {
        src_reg: pair_start,
        dst_reg: state_rec_start,
        extra_amount: n - 1,
    });
    program.emit_insn(Insn::Copy {
        src_reg: w_reg,
        dst_reg: state_mult_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: state_rec_start as u16,
        count: (n + 1) as u16,
        dest_reg: state_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: state_cursor_id,
        key_reg: srid_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: state_table_name.clone(),
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: pair_start as u16,
        count: (n + 1) as u16,
        dest_reg: index_record_reg as u16,
        index_name: Some(state_index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: state_index_cursor_id,
        record_reg: index_record_reg,
        unpacked_start: Some(pair_start),
        unpacked_count: Some((n + 1) as u16),
        flags: IdxInsertFlags::new(),
    });
    program.emit_insn(Insn::Lt {
        lhs: w_reg,
        rhs: zero_reg,
        target_pc: m_ret_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Copy {
        src_reg: w_reg,
        dst_reg: out_weight_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Goto {
        target_pc: m_write_label,
    });

    program.preassign_label_to_next_insn(m_found_label);
    program.emit_insn(Insn::IdxRowId {
        cursor_id: state_index_cursor_id,
        dest: srid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: state_cursor_id,
        src_reg: srid_reg,
        target_pc: corrupt_label,
    });
    program.emit_insn(Insn::Column {
        cursor_id: state_cursor_id,
        column: n,
        dest: cur_w_reg,
        default: None,
    });
    program.emit_insn(Insn::Add {
        lhs: w_reg,
        rhs: cur_w_reg,
        dest: new_w_reg,
    });
    program.emit_insn(Insn::Ne {
        lhs: new_w_reg,
        rhs: zero_reg,
        target_pc: m_update_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    // Multiplicity reached zero: the tuple is gone. Its view row exists only
    // if the tuple was visible (positive) before this contribution.
    program.emit_insn(Insn::IdxDelete {
        start_reg: pair_start,
        num_regs: n + 1,
        cursor_id: state_index_cursor_id,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: state_cursor_id,
        table_name: state_table_name.clone(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Lt {
        lhs: cur_w_reg,
        rhs: zero_reg,
        target_pc: m_ret_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    emit_split_view_rowid(
        program,
        left_outer,
        srid_reg,
        two_reg,
        one_reg,
        view_rowid_reg,
        false,
        ns,
    );
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
        target_pc: m_ret_label,
    });

    // Multiplicity changed but is nonzero: rewrite the state row, and write
    // or remove the view row per the new sign. Unit contributions cannot
    // jump from positive to negative (they pass through zero above), so a
    // nonpositive new multiplicity means the pair was not visible before.
    program.preassign_label_to_next_insn(m_update_label);
    program.emit_insn(Insn::Copy {
        src_reg: pair_start,
        dst_reg: state_rec_start,
        extra_amount: n - 1,
    });
    program.emit_insn(Insn::Copy {
        src_reg: new_w_reg,
        dst_reg: state_mult_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: state_rec_start as u16,
        count: (n + 1) as u16,
        dest_reg: state_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: state_cursor_id,
        key_reg: srid_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: state_table_name,
    });
    program.emit_insn(Insn::Gt {
        lhs: new_w_reg,
        rhs: zero_reg,
        target_pc: m_visible_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Goto {
        target_pc: m_ret_label,
    });

    program.preassign_label_to_next_insn(m_visible_label);
    program.emit_insn(Insn::Copy {
        src_reg: new_w_reg,
        dst_reg: out_weight_reg,
        extra_amount: 0,
    });
    program.preassign_label_to_next_insn(m_write_label);
    program.emit_insn(Insn::MakeRecord {
        start_reg: out_start as u16,
        count: (num_view_columns + 1) as u16,
        dest_reg: view_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    emit_split_view_rowid(
        program,
        left_outer,
        srid_reg,
        two_reg,
        one_reg,
        view_rowid_reg,
        false,
        ns,
    );
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
    program.preassign_label_to_next_insn(m_ret_label);
    program.emit_insn(Insn::Return {
        return_reg: gosub_return_reg,
        can_fallthrough: false,
    });

    // LEFT JOIN aux-merge subroutine: apply (l_rid, dPresent, dMatches) to
    // the left row's bookkeeping and flip its NULL-padded view row on
    // transitions of "present with zero matches and WHERE passes over the
    // padded image". The padded bit is stored: it was decided against an
    // image that later contributions may not be able to reconstruct (WHERE
    // is only ever evaluated when the row is present, so the post-state
    // btree has the image). Negative counts are recorded for uniformity
    // with the pair map and read as absent.
    let aux_merge_label = if let Some((
        aux_name,
        aux_index_name,
        aux_cursor_id,
        aux_index_cursor_id,
        mut pad_references,
        pad_cursors,
        pad_where,
    )) = outer
    {
        {
            let am_label = program.allocate_label();
            program.preassign_label_to_next_insn(am_label);

            let af_found = program.allocate_label();
            let af_have_row = program.allocate_label();
            let af_transition = program.allocate_label();
            let af_padwrite = program.allocate_label();
            let af_store = program.allocate_label();
            let af_write = program.allocate_label();
            let af_ret = program.allocate_label();

            program.emit_insn(Insn::Found {
                cursor_id: aux_index_cursor_id,
                target_pc: af_found,
                record_reg: akey_start,
                num_regs: 1,
            });
            // Fresh left row (or a retraction arriving first: recorded with
            // its signs).
            program.emit_int(1, a_fresh_reg);
            program.emit_insn(Insn::NewRowid {
                cursor: aux_cursor_id,
                rowid_reg: a_srid_reg,
                prev_largest_reg: prev_rowid_scratch,
            });
            program.emit_insn(Insn::Copy {
                src_reg: akey_start,
                dst_reg: a_rec_start,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Copy {
                src_reg: dp_reg,
                dst_reg: a_present_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Copy {
                src_reg: dm_reg,
                dst_reg: a_matches_reg,
                extra_amount: 0,
            });
            program.emit_int(0, a_padded_reg);
            program.emit_insn(Insn::Goto {
                target_pc: af_have_row,
            });

            program.preassign_label_to_next_insn(af_found);
            program.emit_int(0, a_fresh_reg);
            program.emit_insn(Insn::IdxRowId {
                cursor_id: aux_index_cursor_id,
                dest: a_srid_reg,
            });
            program.emit_insn(Insn::SeekRowid {
                cursor_id: aux_cursor_id,
                src_reg: a_srid_reg,
                target_pc: corrupt_label,
            });
            for (column, dest) in [(1, a_present_reg), (2, a_matches_reg), (3, a_padded_reg)] {
                program.emit_insn(Insn::Column {
                    cursor_id: aux_cursor_id,
                    column,
                    dest,
                    default: None,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: akey_start,
                dst_reg: a_rec_start,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Add {
                lhs: dp_reg,
                rhs: a_present_reg,
                dest: a_present_reg,
            });
            program.emit_insn(Insn::Add {
                lhs: dm_reg,
                rhs: a_matches_reg,
                dest: a_matches_reg,
            });

            program.preassign_label_to_next_insn(af_have_row);
            program.emit_int(0, padded_new_reg);
            program.emit_insn(Insn::Le {
                lhs: a_present_reg,
                rhs: zero_reg,
                target_pc: af_transition,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            program.emit_insn(Insn::If {
                reg: a_matches_reg,
                target_pc: af_transition,
                jump_if_null: false,
            });
            // Candidate padded row: position the left cursor on the row
            // (present, so the post-state btree has it), put the right
            // cursor on its null row, and let WHERE decide over that image.
            program.emit_insn(Insn::SeekRowid {
                cursor_id: pad_cursors[0],
                src_reg: akey_start,
                target_pc: corrupt_label,
            });
            program.emit_insn(Insn::NullRow {
                cursor_id: pad_cursors[1],
            });
            if let Some(pad_where) = &pad_where {
                let pad_pass = program.allocate_label();
                translate_condition_expr(
                    program,
                    &pad_references,
                    pad_where,
                    ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true: pad_pass,
                        jump_target_when_false: af_transition,
                        jump_target_when_null: af_transition,
                    },
                    &resolver,
                )?;
                program.preassign_label_to_next_insn(pad_pass);
            }
            program.emit_int(1, padded_new_reg);
            program.preassign_label_to_next_insn(af_transition);
            program.emit_insn(Insn::Eq {
                lhs: a_padded_reg,
                rhs: padded_new_reg,
                target_pc: af_store,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            program.emit_insn(Insn::If {
                reg: padded_new_reg,
                target_pc: af_padwrite,
                jump_if_null: false,
            });
            // Padded before, not now: remove the padded view row.
            emit_split_view_rowid(
                program,
                true,
                a_srid_reg,
                two_reg,
                one_reg,
                view_rowid_reg,
                true,
                ns,
            );
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
                target_pc: af_store,
            });

            // Not padded before, padded now: write the padded view row. The
            // only path that sets `padded_new` positioned the pad cursors,
            // so the projection reads the left row with NULLs on the right.
            program.preassign_label_to_next_insn(af_padwrite);
            emit_join_projection(
                program,
                &resolver,
                columns,
                tables,
                &pad_cursors,
                &mut pad_references,
                out_start,
                num_view_columns,
            )?;
            program.emit_int(1, out_weight_reg);
            program.emit_insn(Insn::MakeRecord {
                start_reg: out_start as u16,
                count: (num_view_columns + 1) as u16,
                dest_reg: view_record_reg as u16,
                index_name: None,
                affinity_str: None,
            });
            emit_split_view_rowid(
                program,
                true,
                a_srid_reg,
                two_reg,
                one_reg,
                view_rowid_reg,
                true,
                ns,
            );
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
            program.preassign_label_to_next_insn(af_store);
            // Persist the padded bit the transitions just enacted.
            program.emit_insn(Insn::Copy {
                src_reg: padded_new_reg,
                dst_reg: a_padded_reg,
                extra_amount: 0,
            });
            // Delete the aux row once everything it tracks is zero;
            // otherwise (re)write it.
            program.emit_insn(Insn::If {
                reg: a_present_reg,
                target_pc: af_write,
                jump_if_null: false,
            });
            program.emit_insn(Insn::If {
                reg: a_matches_reg,
                target_pc: af_write,
                jump_if_null: false,
            });
            program.emit_insn(Insn::If {
                reg: a_fresh_reg,
                target_pc: af_ret,
                jump_if_null: false,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: akey_start,
                num_regs: 2,
                cursor_id: aux_index_cursor_id,
                raise_error_if_no_matching_entry: true,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: aux_cursor_id,
                table_name: aux_name.clone(),
                is_part_of_update: true,
            });
            program.emit_insn(Insn::Goto { target_pc: af_ret });

            program.preassign_label_to_next_insn(af_write);
            program.emit_insn(Insn::MakeRecord {
                start_reg: a_rec_start as u16,
                count: 4,
                dest_reg: a_state_record_reg as u16,
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::Insert {
                cursor: aux_cursor_id,
                key_reg: a_srid_reg,
                record_reg: a_state_record_reg,
                flag: InsertFlags(
                    InsertFlags::REQUIRE_SEEK
                        | InsertFlags::SKIP_LAST_ROWID
                        | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
                ),
                table_name: aux_name,
            });
            let af_skip_index = program.allocate_label();
            program.emit_insn(Insn::IfNot {
                reg: a_fresh_reg,
                target_pc: af_skip_index,
                jump_if_null: true,
            });
            program.emit_insn(Insn::MakeRecord {
                start_reg: akey_start as u16,
                count: 2,
                dest_reg: a_index_record_reg as u16,
                index_name: Some(aux_index_name),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: aux_index_cursor_id,
                record_reg: a_index_record_reg,
                unpacked_start: Some(akey_start),
                unpacked_count: Some(2),
                flags: IdxInsertFlags::new(),
            });
            program.preassign_label_to_next_insn(af_skip_index);
            program.preassign_label_to_next_insn(af_ret);
            program.emit_insn(Insn::Return {
                return_reg: a_return_reg,
                can_fallthrough: false,
            });
            Some(am_label)
        }
    } else {
        None
    };

    program.preassign_label_to_next_insn(main_start_label);

    // LEFT JOIN: the left-input pass updates each touched left row's
    // presence (a new unmatched left row reaches no join phase). During
    // population the input is the full left btree, so every left row gets
    // its bookkeeping row before the join phase counts its matches.
    if let Some(aux_merge_label) = aux_merge_label {
        let (left_table, _, _) = &tables[0];
        let num_left_columns = left_table.columns().len();
        let input_cursor_id = match input {
            MaintenanceInput::TransactionDelta => {
                let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                    view_name: view_name.to_string(),
                    table: left_table.clone(),
                });
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: 0,
                    db: 0,
                });
                cursor_id
            }
            MaintenanceInput::BaseTable => {
                let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(left_table.clone()));
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: left_table.root_page,
                    db: 0,
                });
                cursor_id
            }
        };
        let pass_done = program.allocate_label();
        let pass_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: input_cursor_id,
            pc_if_empty: pass_done,
        });
        program.preassign_label_to_next_insn(pass_loop);
        match input {
            MaintenanceInput::TransactionDelta => {
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column: num_left_columns,
                    dest: dp_reg,
                    default: None,
                });
            }
            MaintenanceInput::BaseTable => {
                program.emit_int(1, dp_reg);
            }
        }
        program.emit_insn(Insn::RowId {
            cursor_id: input_cursor_id,
            dest: akey_start,
        });
        program.emit_int(0, dm_reg);
        program.emit_insn(Insn::Gosub {
            target_pc: aux_merge_label,
            return_reg: a_return_reg,
        });
        program.emit_insn(Insn::Next {
            cursor_id: input_cursor_id,
            pc_if_next: pass_loop,
        });
        program.preassign_label_to_next_insn(pass_done);
    }

    for (sides, negate) in join_subset_phases(n, input) {
        emit_join_phase(
            program,
            &resolver,
            view_name,
            tables,
            &conditions,
            &sides,
            negate,
            w_reg,
            &mut |program, cursors, table_references| {
                // Row identity: the source-rowid tuple, in table order.
                for (pos, cursor_id) in cursors.iter().enumerate() {
                    program.emit_insn(Insn::RowId {
                        cursor_id: *cursor_id,
                        dest: pair_start + pos,
                    });
                }

                let skip_label = program.allocate_label();
                if left_outer {
                    // Every ON match adjusts the left row's match count,
                    // whether or not WHERE lets the inner row through.
                    program.emit_insn(Insn::Copy {
                        src_reg: pair_start,
                        dst_reg: akey_start,
                        extra_amount: 0,
                    });
                    program.emit_int(0, dp_reg);
                    program.emit_insn(Insn::Copy {
                        src_reg: w_reg,
                        dst_reg: dm_reg,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::Gosub {
                        target_pc: aux_merge_label
                            .expect("LEFT JOIN emits the aux-merge subroutine"),
                        return_reg: a_return_reg,
                    });
                    // WHERE gates only the inner view row (the match was
                    // already counted above).
                    if let Some(where_expr) = where_clause {
                        let mut bound = where_expr.clone();
                        bind_and_rewrite_expr(
                            &mut bound,
                            Some(table_references),
                            None,
                            &resolver,
                            BindingBehavior::ResultColumnsNotAllowed,
                        )?;
                        let pass_label = program.allocate_label();
                        translate_condition_expr(
                            program,
                            table_references,
                            &bound,
                            ConditionMetadata {
                                jump_if_condition_is_true: false,
                                jump_target_when_true: pass_label,
                                jump_target_when_false: skip_label,
                                jump_target_when_null: skip_label,
                            },
                            &resolver,
                        )?;
                        program.preassign_label_to_next_insn(pass_label);
                    }
                }

                emit_join_projection(
                    program,
                    &resolver,
                    columns,
                    tables,
                    cursors,
                    table_references,
                    out_start,
                    num_view_columns,
                )?;

                program.emit_insn(Insn::Gosub {
                    target_pc: merge_label,
                    return_reg: gosub_return_reg,
                });
                program.preassign_label_to_next_insn(skip_label);
                Ok(())
            },
        )?;
    }

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

    drop(syms);
    Ok(())
}

/// Emit the projection of a join view's result columns into
/// `out_start..out_start + num_view_columns`, with every joined cursor
/// positioned (`cursors` maps table positions to cursor ids). Shared by the
/// inner-row sink and the LEFT JOIN padded-row writer, whose right cursor
/// sits on its null row so every read yields NULL.
#[allow(clippy::too_many_arguments)]
fn emit_join_projection(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[ast::ResultColumn],
    tables: &[JoinTable],
    cursors: &[usize],
    table_references: &mut TableReferences,
    out_start: usize,
    num_view_columns: usize,
) -> Result<()> {
    let mut out_reg = out_start;
    for column in columns {
        match column {
            ast::ResultColumn::Expr(expr, _) => {
                let mut bound = expr.as_ref().clone();
                bind_and_rewrite_expr(
                    &mut bound,
                    Some(table_references),
                    None,
                    resolver,
                    BindingBehavior::ResultColumnsNotAllowed,
                )?;
                translate_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    &bound,
                    out_reg,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                out_reg += 1;
            }
            ast::ResultColumn::Star => {
                // A USING (or NATURAL) join merges its columns: the right
                // table's copy is not part of the star expansion.
                for ((table, _, using), cursor_id) in tables.iter().zip(cursors) {
                    for (i, col) in table.columns().iter().enumerate() {
                        if using.iter().any(|u| {
                            col.name
                                .as_deref()
                                .is_some_and(|n| n.eq_ignore_ascii_case(u))
                        }) {
                            continue;
                        }
                        program.emit_column_or_rowid(*cursor_id, i, out_reg);
                        out_reg += 1;
                    }
                }
            }
            ast::ResultColumn::TableStar(name) => {
                let target = crate::util::normalize_ident(name.as_str());
                let Some(pos) = tables
                    .iter()
                    .position(|(_, ident, _)| crate::util::normalize_ident(ident) == target)
                else {
                    return Err(LimboError::ParseError(format!("no such table: {target}")));
                };
                for (i, _col) in tables[pos].0.columns().iter().enumerate() {
                    program.emit_column_or_rowid(cursors[pos], i, out_reg);
                    out_reg += 1;
                }
            }
        }
    }
    let num_projected = out_reg - out_start;
    turso_assert!(
        num_projected == num_view_columns,
        "projection produced {num_projected} columns, view declares {num_view_columns}"
    );
    Ok(())
}

/// Emit `dst = 1` if the count in `src` is positive, else `0`. Counts are
/// always integers, so three-valued logic never applies.
fn emit_presence(program: &mut ProgramBuilder, src: usize, zero_reg: usize, dst: usize) {
    let done = program.allocate_label();
    program.emit_int(1, dst);
    program.emit_insn(Insn::Gt {
        lhs: src,
        rhs: zero_reg,
        target_pc: done,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_int(0, dst);
    program.preassign_label_to_next_insn(done);
}

/// Emit the left-associative visibility fold over the per-branch counts at
/// `cnt_start..cnt_start + prefix_len` into `dest`: a content row is in the
/// compound result iff the fold of per-branch presence is true (UNION and
/// UNION ALL fold as OR, INTERSECT as AND, EXCEPT as AND NOT).
fn emit_visibility_fold(
    program: &mut ProgramBuilder,
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    cnt_start: usize,
    zero_reg: usize,
    scratch_reg: usize,
    dest: usize,
) {
    emit_presence(program, cnt_start, zero_reg, dest);
    for i in 1..prefix_len {
        emit_presence(program, cnt_start + i, zero_reg, scratch_reg);
        match operators[i - 1] {
            ast::CompoundOperator::Union | ast::CompoundOperator::UnionAll => {
                program.emit_insn(Insn::Or {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
            ast::CompoundOperator::Intersect => {
                program.emit_insn(Insn::And {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
            ast::CompoundOperator::Except => {
                program.emit_insn(Insn::Not {
                    reg: scratch_reg,
                    dest: scratch_reg,
                });
                program.emit_insn(Insn::And {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
        }
    }
}

/// Emit the view rowid for a state rowid in `srid_reg` into `dst`. When the
/// view keeps both content rows and append rows, their state rowids come
/// from two independent tables, so they map to disjoint view rowids:
/// 2*rowid for content rows, 2*rowid + 1 for append rows. With a single
/// state table the state rowid is the view rowid, as everywhere else.
#[allow(clippy::too_many_arguments)]
fn emit_split_view_rowid(
    program: &mut ProgramBuilder,
    both_tables: bool,
    srid_reg: usize,
    two_reg: usize,
    one_reg: usize,
    dst: usize,
    append: bool,
    ns: &BranchNamespace,
) {
    if both_tables {
        program.emit_insn(Insn::Multiply {
            lhs: srid_reg,
            rhs: two_reg,
            dest: dst,
        });
        if append {
            program.emit_insn(Insn::Add {
                lhs: dst,
                rhs: one_reg,
                dest: dst,
            });
        }
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: srid_reg,
            dst_reg: dst,
            extra_amount: 0,
        });
    }
    // Namespace into this branch's slice of the shared compound btree. The
    // both-tables split and a branch namespace never co-occur (LEFT joins
    // and dedup compounds are never compound branches).
    ns.apply_in_place(program, dst);
}

/// Compile the maintenance (or population) program for a compound SELECT
/// view (UNION, UNION ALL, INTERSECT, EXCEPT) over filter/project branches.
///
/// Each branch is an independent single-table delta stream (or scan during
/// population). Branches in the dedup prefix funnel through a per-branch
/// weight-merge subroutine keyed by the row's output content: the state row
/// keeps one signed count per prefix branch, and the view row exists while
/// the visibility fold over those counts is true. Branches in the trailing
/// UNION ALL suffix keep (branch, source rowid) identity — duplicates are
/// distinct view rows — through one append-merge subroutine, in the state
/// table when the whole chain is UNION ALL and in the auxiliary multiset
/// table when a dedup prefix owns the state table.
#[allow(clippy::too_many_arguments)]
/// One filter/project branch of a compound view: the base table it scans,
/// the identifier its columns bind under, its projected result columns, and
/// its WHERE. Extracted from either the branch `OneSelect` or its DAG
/// sub-tree so the compound maintenance body is agnostic to its source.
struct BranchContent {
    base_table: Arc<BTreeTable>,
    identifier: String,
    columns: Vec<ast::ResultColumn>,
    where_clause: Option<ast::Expr>,
}

#[allow(clippy::too_many_arguments)]
fn emit_compound_program(
    view_name: &str,
    branches: &[BranchContent],
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    key_collations: &[CollationSeq],
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let n_branches = branches.len();
    let has_prefix = prefix_len > 0;
    let has_append = prefix_len < n_branches;
    let both_tables = has_prefix && has_append;
    // Width of the content key: every output column.
    let k = num_view_columns;
    // With a collated dedup key, values that compare equal can differ in
    // bytes; the stored key is the row's first-seen representative (see the
    // matching reload in the group-aggregate program). The output columns
    // ARE the key, so the reload refreshes both.
    let reload_stored_key = key_collations
        .iter()
        .any(|c| !matches!(c, CollationSeq::Binary | CollationSeq::Unset));

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
    // The append rows' home: the state table for a pure UNION ALL chain,
    // the auxiliary multiset table when a dedup prefix owns the state table.
    let (append_table_name, append_table, append_index) = if both_tables {
        let name = format!(
            "{}{}_{view_name}",
            crate::schema::DBSP_MULTISET_TABLE_PREFIX,
            crate::incremental::view::DBSP_CIRCUIT_VERSION
        );
        let table = schema.get_btree_table(&name).ok_or_else(|| {
            LimboError::InternalError(format!(
                "append table {name} of materialized view {view_name} not found"
            ))
        })?;
        let index = schema.get_indices(&name).next().cloned().ok_or_else(|| {
            LimboError::InternalError(format!(
                "append table {name} of materialized view {view_name} has no index"
            ))
        })?;
        (name, table, index)
    } else {
        (
            state_table_name.clone(),
            state_table.clone(),
            state_index.clone(),
        )
    };

    let mut program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts {
            num_cursors: 6 + branches.len(),
            approx_num_insns: 128 + 48 * num_view_columns * branches.len(),
            approx_num_labels: 24 + 8 * branches.len(),
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

    let content_cursors = if has_prefix {
        let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(state_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: table_cursor,
            root_page: RegisterOrLiteral::Literal(state_table.root_page),
            db: 0,
        });
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(state_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor,
            root_page: RegisterOrLiteral::Literal(state_index.root_page),
            db: 0,
        });
        Some((table_cursor, index_cursor))
    } else {
        None
    };
    let append_cursors = if has_append {
        let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(append_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: table_cursor,
            root_page: RegisterOrLiteral::Literal(append_table.root_page),
            db: 0,
        });
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(append_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor,
            root_page: RegisterOrLiteral::Literal(append_index.root_page),
            db: 0,
        });
        Some((table_cursor, index_cursor))
    } else {
        None
    };
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

    // Register layout, shared by all branches.
    let zero_reg = program.alloc_register();
    let two_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    // [outputs.., weight], contiguous for MakeRecord.
    let out_start = program.alloc_registers(num_view_columns + 1);
    let out_weight_reg = out_start + num_view_columns;
    let w_reg = program.alloc_register();
    let view_record_reg = program.alloc_register();
    let view_rowid_reg = program.alloc_register();
    // Content-merge registers: [key.., state_rowid] contiguous for
    // Found/IdxInsert/IdxDelete, and [key.., counts..] as the state row
    // image (the counts live in place at the record tail).
    let c_return_reg = program.alloc_register();
    let key_start = program.alloc_registers(k + 1);
    let c_srid_reg = key_start + k;
    let state_rec_start = program.alloc_registers(k + prefix_len.max(1));
    let cnt_start = state_rec_start + k;
    let v_old_reg = program.alloc_register();
    let v_new_reg = program.alloc_register();
    let p_scratch_reg = program.alloc_register();
    let c_state_record_reg = program.alloc_register();
    let c_index_record_reg = program.alloc_register();
    // Append-merge registers: [branch, rid, state_rowid] and the
    // (branch, rid, mult) state row image.
    let a_return_reg = program.alloc_register();
    let akey_start = program.alloc_registers(3);
    let a_srid_reg = akey_start + 2;
    let a_rec_start = program.alloc_registers(3);
    let cur_w_reg = program.alloc_register();
    let new_w_reg = program.alloc_register();
    let a_state_record_reg = program.alloc_register();
    let a_index_record_reg = program.alloc_register();

    program.emit_int(0, zero_reg);
    program.emit_int(2, two_reg);
    program.emit_int(1, one_reg);

    let main_start_label = program.allocate_label();
    let corrupt_label = program.allocate_label();
    let end_label = program.allocate_label();

    program.emit_insn(Insn::Goto {
        target_pc: main_start_label,
    });

    // Content-merge subroutines, one per prefix branch (the branch decides
    // which count column the weight lands in): apply (key, outputs, w) to
    // the content state row, then flip the view row on visibility-fold
    // transitions. Negative counts are recorded for uniformity with joins;
    // the per-branch capture-order streams cannot dip below zero on their
    // own, and a negative count reads as "absent" in the fold.
    let mut content_merge_labels = Vec::with_capacity(prefix_len);
    for b in 0..prefix_len {
        let (state_cursor_id, state_index_cursor_id) =
            content_cursors.expect("prefix branches imply the content state table");
        let cm_label = program.allocate_label();
        program.preassign_label_to_next_insn(cm_label);
        content_merge_labels.push(cm_label);

        let found_label = program.allocate_label();
        let vnew_label = program.allocate_label();
        let not_all_zero_label = program.allocate_label();
        let insert_label = program.allocate_label();
        let ret_label = program.allocate_label();

        program.emit_insn(Insn::Found {
            cursor_id: state_index_cursor_id,
            target_pc: found_label,
            record_reg: key_start,
            num_regs: k,
        });
        // Fresh content row: this branch's count is the weight, the rest 0.
        program.emit_insn(Insn::NewRowid {
            cursor: state_cursor_id,
            rowid_reg: c_srid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
        for i in 0..prefix_len {
            program.emit_int(0, cnt_start + i);
        }
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: cnt_start + b,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: key_start,
            dst_reg: state_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: state_rec_start as u16,
            count: (k + prefix_len) as u16,
            dest_reg: c_state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: state_cursor_id,
            key_reg: c_srid_reg,
            record_reg: c_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: state_table_name.clone(),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: key_start as u16,
            count: (k + 1) as u16,
            dest_reg: c_index_record_reg as u16,
            index_name: Some(state_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: state_index_cursor_id,
            record_reg: c_index_record_reg,
            unpacked_start: Some(key_start),
            unpacked_count: Some((k + 1) as u16),
            flags: IdxInsertFlags::new(),
        });
        program.emit_int(0, v_old_reg);
        program.emit_insn(Insn::Goto {
            target_pc: vnew_label,
        });

        program.preassign_label_to_next_insn(found_label);
        program.emit_insn(Insn::IdxRowId {
            cursor_id: state_index_cursor_id,
            dest: c_srid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: state_cursor_id,
            src_reg: c_srid_reg,
            target_pc: corrupt_label,
        });
        if reload_stored_key {
            for i in 0..k {
                program.emit_insn(Insn::Column {
                    cursor_id: state_cursor_id,
                    column: i,
                    dest: key_start + i,
                    default: None,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: key_start,
                dst_reg: out_start,
                extra_amount: k.saturating_sub(1),
            });
        }
        for i in 0..prefix_len {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: k + i,
                dest: cnt_start + i,
                default: None,
            });
        }
        emit_visibility_fold(
            &mut program,
            operators,
            prefix_len,
            cnt_start,
            zero_reg,
            p_scratch_reg,
            v_old_reg,
        );
        program.emit_insn(Insn::Add {
            lhs: w_reg,
            rhs: cnt_start + b,
            dest: cnt_start + b,
        });
        // Every count zero: the content row is gone from all branches (and
        // the fold below reads all-absent as invisible).
        for i in 0..prefix_len {
            program.emit_insn(Insn::If {
                reg: cnt_start + i,
                target_pc: not_all_zero_label,
                jump_if_null: false,
            });
        }
        program.emit_insn(Insn::IdxDelete {
            start_reg: key_start,
            num_regs: k + 1,
            cursor_id: state_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: state_cursor_id,
            table_name: state_table_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: vnew_label,
        });

        program.preassign_label_to_next_insn(not_all_zero_label);
        program.emit_insn(Insn::Copy {
            src_reg: key_start,
            dst_reg: state_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: state_rec_start as u16,
            count: (k + prefix_len) as u16,
            dest_reg: c_state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: state_cursor_id,
            key_reg: c_srid_reg,
            record_reg: c_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: state_table_name.clone(),
        });

        program.preassign_label_to_next_insn(vnew_label);
        emit_visibility_fold(
            &mut program,
            operators,
            prefix_len,
            cnt_start,
            zero_reg,
            p_scratch_reg,
            v_new_reg,
        );
        // The view row changes only on visibility transitions: a row that
        // stays visible keeps its stored first-seen content, so there is
        // nothing to rewrite.
        program.emit_insn(Insn::Eq {
            lhs: v_old_reg,
            rhs: v_new_reg,
            target_pc: ret_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: v_new_reg,
            target_pc: insert_label,
            jump_if_null: false,
        });
        // Visible before, not now: remove the view row.
        emit_split_view_rowid(
            &mut program,
            both_tables,
            c_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            false,
            &BranchNamespace::none(),
        );
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
            target_pc: ret_label,
        });

        program.preassign_label_to_next_insn(insert_label);
        program.emit_int(1, out_weight_reg);
        program.emit_insn(Insn::MakeRecord {
            start_reg: out_start as u16,
            count: (num_view_columns + 1) as u16,
            dest_reg: view_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        emit_split_view_rowid(
            &mut program,
            both_tables,
            c_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            false,
            &BranchNamespace::none(),
        );
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
        program.preassign_label_to_next_insn(ret_label);
        program.emit_insn(Insn::Return {
            return_reg: c_return_reg,
            can_fallthrough: false,
        });
    }

    // Append-merge subroutine: apply ((branch, rid), outputs, w) with a
    // single signed multiplicity, the view row existing while it is
    // positive. Signed multiplicities are kept for uniformity with joins;
    // the per-branch capture-order streams cannot dip below zero on their
    // own.
    let append_merge_label = append_cursors.map(|(append_cursor_id, append_index_cursor_id)| {
        let am_label = program.allocate_label();
        program.preassign_label_to_next_insn(am_label);

        let m_found_label = program.allocate_label();
        let m_update_label = program.allocate_label();
        let m_visible_label = program.allocate_label();
        let m_write_label = program.allocate_label();
        let m_ret_label = program.allocate_label();
        program.emit_insn(Insn::Found {
            cursor_id: append_index_cursor_id,
            target_pc: m_found_label,
            record_reg: akey_start,
            num_regs: 2,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: append_cursor_id,
            rowid_reg: a_srid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
        program.emit_insn(Insn::Copy {
            src_reg: akey_start,
            dst_reg: a_rec_start,
            extra_amount: 1,
        });
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: a_rec_start + 2,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: a_rec_start as u16,
            count: 3,
            dest_reg: a_state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: append_cursor_id,
            key_reg: a_srid_reg,
            record_reg: a_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: append_table_name.clone(),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: akey_start as u16,
            count: 3,
            dest_reg: a_index_record_reg as u16,
            index_name: Some(append_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: append_index_cursor_id,
            record_reg: a_index_record_reg,
            unpacked_start: Some(akey_start),
            unpacked_count: Some(3),
            flags: IdxInsertFlags::new(),
        });
        program.emit_insn(Insn::Lt {
            lhs: w_reg,
            rhs: zero_reg,
            target_pc: m_ret_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: out_weight_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_write_label,
        });

        program.preassign_label_to_next_insn(m_found_label);
        program.emit_insn(Insn::IdxRowId {
            cursor_id: append_index_cursor_id,
            dest: a_srid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: append_cursor_id,
            src_reg: a_srid_reg,
            target_pc: corrupt_label,
        });
        program.emit_insn(Insn::Column {
            cursor_id: append_cursor_id,
            column: 2,
            dest: cur_w_reg,
            default: None,
        });
        program.emit_insn(Insn::Add {
            lhs: w_reg,
            rhs: cur_w_reg,
            dest: new_w_reg,
        });
        program.emit_insn(Insn::Ne {
            lhs: new_w_reg,
            rhs: zero_reg,
            target_pc: m_update_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        // Multiplicity reached zero: the row is gone. Its view row exists
        // only if the multiplicity was positive before this contribution.
        program.emit_insn(Insn::IdxDelete {
            start_reg: akey_start,
            num_regs: 3,
            cursor_id: append_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: append_cursor_id,
            table_name: append_table_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Lt {
            lhs: cur_w_reg,
            rhs: zero_reg,
            target_pc: m_ret_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        emit_split_view_rowid(
            &mut program,
            both_tables,
            a_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            true,
            &BranchNamespace::none(),
        );
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
            target_pc: m_ret_label,
        });

        program.preassign_label_to_next_insn(m_update_label);
        program.emit_insn(Insn::Copy {
            src_reg: akey_start,
            dst_reg: a_rec_start,
            extra_amount: 1,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new_w_reg,
            dst_reg: a_rec_start + 2,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: a_rec_start as u16,
            count: 3,
            dest_reg: a_state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: append_cursor_id,
            key_reg: a_srid_reg,
            record_reg: a_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: append_table_name.clone(),
        });
        program.emit_insn(Insn::Gt {
            lhs: new_w_reg,
            rhs: zero_reg,
            target_pc: m_visible_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_ret_label,
        });

        program.preassign_label_to_next_insn(m_visible_label);
        program.emit_insn(Insn::Copy {
            src_reg: new_w_reg,
            dst_reg: out_weight_reg,
            extra_amount: 0,
        });
        program.preassign_label_to_next_insn(m_write_label);
        program.emit_insn(Insn::MakeRecord {
            start_reg: out_start as u16,
            count: (num_view_columns + 1) as u16,
            dest_reg: view_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        emit_split_view_rowid(
            &mut program,
            both_tables,
            a_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            true,
            &BranchNamespace::none(),
        );
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
        program.preassign_label_to_next_insn(m_ret_label);
        program.emit_insn(Insn::Return {
            return_reg: a_return_reg,
            can_fallthrough: false,
        });
        am_label
    });

    program.preassign_label_to_next_insn(main_start_label);

    for (branch_idx, branch) in branches.iter().enumerate() {
        let base_table = &branch.base_table;
        let columns = &branch.columns;
        let where_clause = branch.where_clause.as_ref();
        let num_base_columns = base_table.columns().len();

        let table_ref_id = program.table_reference_counter.next();
        let mut table_references = TableReferences::new(
            vec![JoinedTable {
                op: Operation::Scan(Scan::BTreeTable {
                    iter_dir: IterationDirection::Forwards,
                    index: None,
                }),
                table: crate::schema::Table::BTree(base_table.clone()),
                identifier: branch.identifier.clone(),
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

        let branch_done_label = program.allocate_label();
        let loop_label = program.allocate_label();
        let next_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: input_cursor_id,
            pc_if_empty: branch_done_label,
        });
        program.preassign_label_to_next_insn(loop_label);

        match input {
            MaintenanceInput::TransactionDelta => {
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column: num_base_columns,
                    dest: w_reg,
                    default: None,
                });
            }
            MaintenanceInput::BaseTable => {
                program.emit_int(1, w_reg);
            }
        }

        if let Some(where_expr) = where_clause {
            let mut bound = where_expr.clone();
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

        let mut out_reg = out_start;
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
        let num_projected = out_reg - out_start;
        turso_assert!(
            num_projected == num_view_columns,
            "projection produced {num_projected} columns, view declares {num_view_columns}"
        );

        // Row identity: output content for dedup-prefix branches,
        // (branch, rowid) for the trailing UNION ALL branches.
        if branch_idx < prefix_len {
            program.emit_insn(Insn::Copy {
                src_reg: out_start,
                dst_reg: key_start,
                extra_amount: k.saturating_sub(1),
            });
            program.emit_insn(Insn::Gosub {
                target_pc: content_merge_labels[branch_idx],
                return_reg: c_return_reg,
            });
        } else {
            program.emit_int(branch_idx as i64, akey_start);
            program.emit_insn(Insn::RowId {
                cursor_id: input_cursor_id,
                dest: akey_start + 1,
            });
            program.emit_insn(Insn::Gosub {
                target_pc: append_merge_label
                    .expect("append branches imply the append-merge subroutine"),
                return_reg: a_return_reg,
            });
        }

        program.preassign_label_to_next_insn(next_label);
        program.emit_insn(Insn::Next {
            cursor_id: input_cursor_id,
            pc_if_next: loop_label,
        });
        program.preassign_label_to_next_insn(branch_done_label);
    }

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
        "materialized view compound maintenance",
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
