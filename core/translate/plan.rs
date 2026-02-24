use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use std::{cmp::Ordering, marker::PhantomData, sync::Arc};
use turso_parser::ast::{
    self, FrameBound, FrameClause, FrameExclude, FrameMode, ResolveType, SortOrder, SubqueryType,
};

use crate::{
    function::AggFunc,
    schema::{BTreeTable, ColDef, Column, FromClauseSubquery, Index, Schema, Table},
    translate::{
        collate::{get_collseq_from_expr, CollationSeq},
        emitter::UpdateRowSource,
        expr::{as_binary_components, get_expr_affinity},
        expression_index::{normalize_expr_for_index_matching, single_table_column_usage},
        optimizer::constraints::{BinaryExprSide, SeekRangeConstraint},
        planner::determine_where_to_eval_term,
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{HashDistinctData, Insn},
        BranchOffset, CursorID,
    },
    Result, VirtualTable,
};
use crate::{schema::Type, types::SeekOp};

use turso_parser::ast::TableInternalId;

use super::emitter::OperationMode;

/// Infer the Type and type name from an expression's affinity.
///
/// Used for subquery result columns. SQLite derives column affinity from:
/// - Column references: the declared column type
/// - CAST expressions: the cast target type
/// - Subqueries: recursively from the subquery's result expression
/// - Literals: BLOB affinity (no affinity)
///
/// The affinity determines comparison behavior in IN expressions, etc.
fn infer_type_from_expr(
    expr: &ast::Expr,
    tables: Option<&TableReferences>,
) -> (Type, &'static str) {
    let affinity = get_expr_affinity(expr, tables, None);
    match affinity {
        Affinity::Integer => (Type::Integer, "INTEGER"),
        Affinity::Real => (Type::Real, "REAL"),
        Affinity::Text => (Type::Text, "TEXT"),
        Affinity::Numeric => (Type::Numeric, "NUMERIC"),
        Affinity::Blob => (Type::Blob, "BLOB"),
    }
}

#[derive(Debug, Clone)]
pub struct ResultSetColumn {
    pub expr: ast::Expr,
    pub alias: Option<String>,
    // TODO: encode which aggregates (e.g. index bitmask of plan.aggregates) are present in this column
    pub contains_aggregates: bool,
}

impl ResultSetColumn {
    pub fn name<'a>(&'a self, tables: &'a TableReferences) -> Option<&'a str> {
        if let Some(alias) = &self.alias {
            return Some(alias);
        }
        match &self.expr {
            ast::Expr::Column { table, column, .. } => {
                if let Some(joined_table_ref) = tables.find_joined_table_by_internal_id(*table) {
                    if let Operation::IndexMethodQuery(module) = &joined_table_ref.op {
                        if module.covered_columns.contains_key(column) {
                            return None;
                        }
                    }
                    joined_table_ref
                        .table
                        .get_column_at(*column)
                        .unwrap()
                        .name
                        .as_deref()
                } else {
                    // Column references an outer query table (correlated subquery).
                    let (_, table_ref) = tables.find_table_by_internal_id(*table)?;
                    table_ref.get_column_at(*column)?.name.as_deref()
                }
            }
            ast::Expr::RowId { table, .. } => {
                // If there is a rowid alias column, use its name
                let (_, table_ref) = tables.find_table_by_internal_id(*table)?;
                if let Table::BTree(table) = &table_ref {
                    if let Some(rowid_alias_column) = table.get_rowid_alias_column() {
                        if let Some(name) = &rowid_alias_column.1.name {
                            return Some(name);
                        }
                    }
                }

                // If there is no rowid alias, use "rowid".
                Some("rowid")
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupBy {
    pub exprs: Vec<ast::Expr>,
    /// sort order, if a sorter is required (= the columns aren't already in the correct order)
    pub sort_order: Option<Vec<SortOrder>>,
    /// having clause split into a vec at 'AND' boundaries.
    pub having: Option<Vec<ast::Expr>>,
}

/// In a query plan, WHERE clause conditions and JOIN conditions are all folded into a vector of WhereTerm.
/// This is done so that we can evaluate the conditions at the correct loop depth.
/// We also need to keep track of whether the condition came from an OUTER JOIN. Take this example:
/// SELECT * FROM users u LEFT JOIN products p ON u.id = 5.
/// Even though the condition only refers to 'u', we CANNOT evaluate it at the users loop, because we need to emit NULL
/// values for the columns of 'p', for EVERY row in 'u', instead of completely skipping any rows in 'u' where the condition is false.
#[derive(Debug, Clone)]
pub struct WhereTerm {
    /// The original condition expression.
    pub expr: ast::Expr,
    /// For normal JOIN conditions (ON or WHERE clauses), we break them up into individual [WhereTerm] conditions
    /// and let the optimizer determine when each should be evaluated based on the tables they reference.
    /// See e.g. [EvalAt].
    /// For example, in "SELECT * FROM x JOIN y WHERE x.a = 2", we want to evaluate x.a = 2 right after opening x
    /// since it only depends on x.
    ///
    /// However, OUTER JOIN conditions require special handling. Consider:
    ///   SELECT * FROM t LEFT JOIN s ON t.a = 2
    ///
    /// Even though t.a = 2 only references t, we cannot evaluate it during t's loop and skip rows where t.a != 2.
    /// Instead, we must:
    /// 1. Process ALL rows from t
    /// 2. For each t row where t.a != 2, emit NULL values for s's columns
    /// 3. For each t row where t.a = 2, emit the actual s values
    ///
    /// This means the condition must be evaluated during s's loop, regardless of which tables it references.
    /// We track this requirement using [WhereTerm::from_outer_join], which contains the [TableInternalId] of the
    /// right-side table of the OUTER JOIN (in this case, s). When evaluating conditions, if [WhereTerm::from_outer_join]
    /// is set, we force evaluation to happen during that table's loop.
    pub from_outer_join: Option<TableInternalId>,
    /// Whether the condition has been consumed by the optimizer in some way, and it should not be evaluated
    /// in the normal place where WHERE terms are evaluated.
    /// A term may have been consumed e.g. if:
    /// - it has been converted into a constraint in a seek key
    /// - it has been removed due to being trivially true or false
    pub consumed: bool,
}

impl WhereTerm {
    pub fn should_eval_before_loop(
        &self,
        join_order: &[JoinOrderMember],
        subqueries: &[NonFromClauseSubquery],
        table_references: Option<&TableReferences>,
    ) -> bool {
        if self.consumed {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order, subqueries, table_references) else {
            return false;
        };
        eval_at == EvalAt::BeforeLoop
    }

    pub fn should_eval_at_loop(
        &self,
        loop_idx: usize,
        join_order: &[JoinOrderMember],
        subqueries: &[NonFromClauseSubquery],
        table_references: Option<&TableReferences>,
    ) -> bool {
        if self.consumed {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order, subqueries, table_references) else {
            return false;
        };
        eval_at == EvalAt::Loop(loop_idx)
    }

    fn eval_at(
        &self,
        join_order: &[JoinOrderMember],
        subqueries: &[NonFromClauseSubquery],
        table_references: Option<&TableReferences>,
    ) -> Result<EvalAt> {
        determine_where_to_eval_term(self, join_order, subqueries, table_references)
    }
}

impl From<Expr> for WhereTerm {
    fn from(value: Expr) -> Self {
        Self {
            expr: value,
            from_outer_join: None,
            consumed: false,
        }
    }
}

use crate::ast::Expr;
use crate::util::exprs_are_equivalent;

/// The loop index where to evaluate the condition.
/// For example, in `SELECT * FROM u JOIN p WHERE u.id = 5`, the condition can already be evaluated at the first loop (idx 0),
/// because that is the rightmost table that it references.
///
/// Conditions like 1=2 can be evaluated before the main loop is opened, because they are constant.
/// In theory we should be able to statically analyze them all and reduce them to a single boolean value,
/// but that is not implemented yet.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum EvalAt {
    Loop(usize),
    BeforeLoop,
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for EvalAt {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (EvalAt::Loop(a), EvalAt::Loop(b)) => a.partial_cmp(b),
            (EvalAt::BeforeLoop, EvalAt::BeforeLoop) => Some(Ordering::Equal),
            (EvalAt::BeforeLoop, _) => Some(Ordering::Less),
            (_, EvalAt::BeforeLoop) => Some(Ordering::Greater),
        }
    }
}

impl Ord for EvalAt {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other)
            .expect("total ordering not implemented for EvalAt")
    }
}

/// Describes one ORDER BY term for a compound SELECT (UNION/INTERSECT/EXCEPT/UNION ALL).
/// Each term maps to a result column index with a sort direction and optional collation.
#[derive(Debug, Clone)]
pub struct CompoundSelectOrderBy {
    /// 0-based index into the result columns of the first SELECT.
    pub column_idx: usize,
    /// Sort direction (ASC or DESC).
    pub order: SortOrder,
    /// Collation sequence for sorting; derived from explicit COLLATE or column definition.
    pub collation: Option<CollationSeq>,
}

/// A query plan is either a SELECT or a DELETE (for now)
#[derive(Debug, Clone)]
pub enum Plan {
    Select(SelectPlan),
    CompoundSelect {
        left: Vec<(SelectPlan, ast::CompoundOperator)>,
        right_most: SelectPlan,
        limit: Option<Box<Expr>>,
        offset: Option<Box<Expr>>,
        order_by: Option<Vec<CompoundSelectOrderBy>>,
    },
    Delete(DeletePlan),
    Update(UpdatePlan),
}

impl Plan {
    /// Returns true if this SELECT plan contains a reference to the given table.
    /// For compound selects, checks all component selects.
    /// Returns false for Delete/Update plans.
    pub fn select_contains_table(&self, table: &Table) -> bool {
        match self {
            Plan::Select(select_plan) => select_plan.table_references.contains_table(table),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                right_most.table_references.contains_table(table)
                    || left
                        .iter()
                        .any(|(plan, _)| plan.table_references.contains_table(table))
            }
            Plan::Delete(_) | Plan::Update(_) => false,
        }
    }

    /// Returns the query destination for Select/CompoundSelect plans.
    /// Returns None for Delete/Update plans.
    pub fn select_query_destination(&self) -> Option<&QueryDestination> {
        match self {
            Plan::Select(select_plan) => Some(&select_plan.query_destination),
            Plan::CompoundSelect { right_most, .. } => Some(&right_most.query_destination),
            Plan::Delete(_) | Plan::Update(_) => None,
        }
    }

    /// Returns a mutable reference to the query destination for Select/CompoundSelect plans.
    /// Returns None for Delete/Update plans.
    pub fn select_query_destination_mut(&mut self) -> Option<&mut QueryDestination> {
        match self {
            Plan::Select(select_plan) => Some(&mut select_plan.query_destination),
            Plan::CompoundSelect { right_most, .. } => Some(&mut right_most.query_destination),
            Plan::Delete(_) | Plan::Update(_) => None,
        }
    }

    /// Returns the result columns for Select/CompoundSelect plans.
    /// Returns None for Delete/Update plans.
    pub fn select_result_columns(&self) -> Option<&[ResultSetColumn]> {
        match self {
            Plan::Select(select_plan) => Some(&select_plan.result_columns),
            Plan::CompoundSelect { right_most, .. } => Some(&right_most.result_columns),
            Plan::Delete(_) | Plan::Update(_) => None,
        }
    }

    /// Returns the table references for Select/CompoundSelect plans.
    /// Returns None for Delete/Update plans.
    pub fn select_table_references(&self) -> Option<&TableReferences> {
        match self {
            Plan::Select(select_plan) => Some(&select_plan.table_references),
            Plan::CompoundSelect { right_most, .. } => Some(&right_most.table_references),
            Plan::Delete(_) | Plan::Update(_) => None,
        }
    }
}

/// The destination of the results of a query.
/// Typically, the results of a query are returned to the caller.
/// However, there are some cases where the results are not returned to the caller,
/// but rather are yielded to a parent query via coroutine, or stored in a temp table,
/// later used by the parent query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EphemeralRowidMode {
    /// The last result column is used as the rowid key.
    FromResultColumns,
    /// Generate a fresh rowid for each inserted row.
    Auto,
}

#[derive(Debug, Clone)]
pub enum QueryDestination {
    /// The results of the query are returned to the caller.
    ResultRows,
    /// The results of the query are yielded to a parent query via coroutine.
    CoroutineYield {
        /// The register that holds the program offset that handles jumping to/from the coroutine.
        yield_reg: usize,
        /// The index of the first instruction in the bytecode that implements the coroutine.
        coroutine_implementation_start: BranchOffset,
    },
    /// The results of the query are stored in an ephemeral index,
    /// later used by the parent query.
    EphemeralIndex {
        /// The cursor ID of the ephemeral index that will be used to store the results.
        cursor_id: CursorID,
        /// The index that will be used to store the results.
        index: Arc<Index>,
        /// Optional MakeRecord affinity string to apply before inserting keys.
        /// For `IN (SELECT ...)` this must match the left-hand side expression affinity.
        affinity_str: Option<Arc<String>>,
        /// Whether this is a delete operation that will remove the index entries
        is_delete: bool,
    },
    /// The results of the query are stored in an ephemeral table,
    /// later used by the parent query.
    EphemeralTable {
        /// The cursor ID of the ephemeral table that will be used to store the results.
        cursor_id: CursorID,
        /// The table that will be used to store the results.
        table: Arc<BTreeTable>,
        /// How to determine the rowid key for inserts.
        rowid_mode: EphemeralRowidMode,
    },
    /// The result of an EXISTS subquery are stored in a single register.
    ExistsSubqueryResult {
        /// The register that holds the result of the EXISTS subquery.
        result_reg: usize,
    },
    /// The results of a subquery that is neither 'EXISTS' nor 'IN' are stored in a range of registers.
    RowValueSubqueryResult {
        /// The start register of the range that holds the result of the subquery.
        result_reg_start: usize,
        /// The number of registers that hold the result of the subquery.
        num_regs: usize,
    },
    /// The results of the query are stored in a RowSet (for DELETE operations with triggers).
    /// Rowids are added to the RowSet using RowSetAdd, then read back using RowSetRead.
    RowSet {
        /// The register that holds the RowSet object.
        rowset_reg: usize,
    },
    /// Decision made at some point after query plan construction.
    Unset,
}

impl QueryDestination {
    pub fn placeholder_for_subquery() -> Self {
        QueryDestination::CoroutineYield {
            yield_reg: usize::MAX, // will be set later in bytecode emission
            coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JoinOrderMember {
    /// The internal ID of the[TableReference]
    pub table_id: TableInternalId,
    /// The index of the table in the original join order.
    /// This is used to index into e.g. [TableReferences::joined_tables()]
    pub original_idx: usize,
    /// Whether this member is the right side of an OUTER JOIN
    pub is_outer: bool,
}

#[derive(Debug, Clone, PartialEq)]

/// Whether a column is DISTINCT or not.
pub enum Distinctness {
    /// The column is not a DISTINCT column.
    NonDistinct,
    /// The column is a DISTINCT column,
    /// and includes a translation context for handling duplicates.
    Distinct { ctx: Option<DistinctCtx> },
}

impl Distinctness {
    pub fn from_ast(distinctness: Option<&ast::Distinctness>) -> Self {
        match distinctness {
            Some(ast::Distinctness::Distinct) => Self::Distinct { ctx: None },
            Some(ast::Distinctness::All) => Self::NonDistinct,
            None => Self::NonDistinct,
        }
    }
    pub fn is_distinct(&self) -> bool {
        matches!(self, Distinctness::Distinct { .. })
    }
}

/// Translation context for handling DISTINCT columns.
#[derive(Debug, Clone, PartialEq)]
pub struct DistinctCtx {
    /// Hash table id used to deduplicate results.
    pub hash_table_id: usize,
    /// Collations for each distinct key column.
    pub collations: Vec<CollationSeq>,
    /// The label for the on conflict branch.
    /// When a duplicate is found, the program will jump to the offset this label points to.
    pub label_on_conflict: BranchOffset,
}

impl DistinctCtx {
    pub fn emit_deduplication_insns(
        &self,
        program: &mut ProgramBuilder,
        num_regs: usize,
        start_reg: usize,
    ) {
        program.emit_insn(Insn::HashDistinct {
            data: Box::new(HashDistinctData {
                hash_table_id: self.hash_table_id,
                key_start_reg: start_reg,
                num_keys: num_regs,
                collations: self.collations.clone(),
                target_pc: self.label_on_conflict,
            }),
        });
    }
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub table_references: TableReferences,
    /// The order in which the tables are joined. Tables have usize Ids (their index in joined_tables)
    pub join_order: Vec<JoinOrderMember>,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries. all join conditions also get shoved in here,
    /// and we keep track of which join they came from (mainly for OUTER JOIN processing)
    pub where_clause: Vec<WhereTerm>,
    /// group by clause
    pub group_by: Option<GroupBy>,
    /// order by clause
    pub order_by: Vec<(Box<ast::Expr>, SortOrder)>,
    /// all the aggregates collected from the result columns, order by, and (TODO) having clauses
    pub aggregates: Vec<Aggregate>,
    /// limit clause
    pub limit: Option<Box<Expr>>,
    /// offset clause
    pub offset: Option<Box<Expr>>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
    /// the destination of the resulting rows from this plan.
    pub query_destination: QueryDestination,
    /// whether the query is DISTINCT
    pub distinctness: Distinctness,
    /// values: https://sqlite.org/syntax/select-core.html
    pub values: Vec<Vec<Expr>>,
    /// The window definition and all window functions associated with it. There is at most one
    /// window per SELECT. If the original query contains more, they are pushed down into subqueries.
    pub window: Option<Window>,
    /// Subqueries that appear in any part of the query apart from the FROM clause
    pub non_from_clause_subqueries: Vec<NonFromClauseSubquery>,
    /// Estimated output rows from the optimizer's join order computation.
    /// Used to propagate cardinality estimates for CTE/subquery tables.
    pub estimated_output_rows: Option<f64>,
}

impl SelectPlan {
    pub fn joined_tables(&self) -> &[JoinedTable] {
        self.table_references.joined_tables()
    }

    pub fn agg_args_count(&self) -> usize {
        self.aggregates.iter().map(|agg| agg.args.len()).sum()
    }

    /// Whether this query or any of its subqueries reference columns from the outer query.
    pub fn is_correlated(&self) -> bool {
        self.table_references
            .outer_query_refs()
            .iter()
            .any(|t| t.is_used())
            || self.non_from_clause_subqueries.iter().any(|s| s.correlated)
            || self
                .table_references
                .joined_tables()
                .iter()
                .any(|t| match &t.table {
                    Table::FromClauseSubquery(subquery) => plan_is_correlated(&subquery.plan),
                    _ => false,
                })
    }

    /// Reference: https://github.com/sqlite/sqlite/blob/5db695197b74580c777b37ab1b787531f15f7f9f/src/select.c#L8613
    ///
    /// Checks to see if the query is of the format `SELECT count(*) FROM <tbl>`
    pub fn is_simple_count(&self) -> bool {
        if !self.where_clause.is_empty()
            || self.aggregates.len() != 1
            || matches!(
                self.query_destination,
                QueryDestination::CoroutineYield { .. }
            )
            || self.table_references.joined_tables().len() != 1
            || !self.table_references.outer_query_refs().is_empty()
            || self.result_columns.len() != 1
            || self.group_by.is_some()
            || self.contains_constant_false_condition
        // TODO: (pedrocarlo) maybe can optimize to use the count optmization with more columns
        {
            return false;
        }
        let table_ref = self.table_references.joined_tables().first().unwrap();
        if !matches!(table_ref.table, crate::schema::Table::BTree(..)) {
            return false;
        }
        let agg = self.aggregates.first().unwrap();
        if !matches!(agg.func, AggFunc::Count0) {
            return false;
        }

        let count = ast::Expr::FunctionCall {
            name: ast::Name::exact("count".to_string()),
            distinctness: None,
            args: vec![],
            order_by: vec![],
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let count_star = ast::Expr::FunctionCallStar {
            name: ast::Name::exact("count".to_string()),
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let result_col_expr = &self.result_columns.first().unwrap().expr;
        if *result_col_expr != count && *result_col_expr != count_star {
            return false;
        }
        true
    }
}

/// Why an UPDATE/DELETE must gather target rowids first, then apply writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlSafetyReason {
    /// Triggers exist, so we lock in target rows before writing.
    Trigger,
    /// WHERE has a subquery, so we lock in target rows before writing.
    SubqueryInWhere,
    /// The plan reads rowids from multiple index branches (multi-index scan).
    MultiIndexScan,
    /// REPLACE may delete conflicting rows while we are scanning.
    ReplaceMode,
    /// The statement updates key columns used by the scan itself.
    KeyMutation,
}

/// Safety decisions made while planning UPDATE/DELETE.
#[derive(Debug, Clone, Default)]
pub struct DmlSafety {
    /// Why the safer "collect first, write later" mode was enabled.
    pub reasons: SmallVec<[DmlSafetyReason; 2]>,
}

impl DmlSafety {
    pub fn requires_stable_write_set(&self) -> bool {
        !self.reasons.is_empty()
    }

    pub fn require(&mut self, reason: DmlSafetyReason) {
        if !self.reasons.contains(&reason) {
            self.reasons.push(reason);
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table_references: TableReferences,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries.
    pub where_clause: Vec<WhereTerm>,
    /// order by clause
    pub order_by: Vec<(Box<ast::Expr>, SortOrder)>,
    /// limit clause
    pub limit: Option<Box<Expr>>,
    /// offset clause
    pub offset: Option<Box<Expr>>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
    /// Indexes that must be updated by the delete operation.
    pub indexes: Vec<Arc<Index>>,
    /// When DELETE cannot safely write while scanning, we first collect rowids into a RowSet.
    pub rowset_plan: Option<SelectPlan>,
    /// Register ID for the RowSet (if rowset_plan is Some)
    pub rowset_reg: Option<usize>,
    /// Subqueries that appear in the WHERE clause (for non-rowset path)
    pub non_from_clause_subqueries: Vec<NonFromClauseSubquery>,
    /// Whether this DELETE plan uses the safer pre-materialization path, and why.
    pub safety: DmlSafety,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table_references: TableReferences,
    /// Conflict resolution strategy (e.g., OR IGNORE, OR REPLACE)
    pub or_conflict: Option<ResolveType>,
    // (colum index, new value) pairs
    pub set_clauses: Vec<(usize, Box<ast::Expr>)>,
    pub where_clause: Vec<WhereTerm>,
    pub order_by: Vec<(Box<ast::Expr>, SortOrder)>,
    pub limit: Option<Box<Expr>>,
    pub offset: Option<Box<Expr>>,
    // TODO: optional RETURNING clause
    pub returning: Option<Vec<ResultSetColumn>>,
    // whether the WHERE clause is always false
    pub contains_constant_false_condition: bool,
    pub indexes_to_update: Vec<Arc<Index>>,
    // If the UPDATE modifies any column that is present in the key of the btree used to iterate over the table (either the table itself or an index),
    // gather all the target rowids into an ephemeral table, and then use that table as the single JoinedTable for the actual UPDATE loop.
    // This ensures the keys of the btree used to iterate cannot be changed during the UPDATE loop itself, ensuring all the intended rows actually get
    // updated and none are skipped.
    pub ephemeral_plan: Option<SelectPlan>,
    // For ALTER TABLE turso-db emits appropriate DDL statement in the "updates" cell of CDC table
    // This field is present only for update plan created for ALTER TABLE when CDC mode has "updates" values
    pub cdc_update_alter_statement: Option<String>,
    /// Subqueries that appear in the WHERE clause (for non-ephemeral path)
    pub non_from_clause_subqueries: Vec<NonFromClauseSubquery>,
    /// Whether this UPDATE plan uses the safer pre-materialization path, and why.
    pub safety: DmlSafety,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
}

pub fn select_star(
    tables: &[JoinedTable],
    out_columns: &mut Vec<ResultSetColumn>,
    right_join_swapped: bool,
) {
    // RIGHT JOIN swapped tables; iterate in reverse to restore original column order.
    let table_iter: Vec<&JoinedTable> = if right_join_swapped {
        tables.iter().rev().collect()
    } else {
        tables.iter().collect()
    };
    for table in table_iter {
        out_columns.extend(
            table
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, col)| !col.hidden())
                .filter(|(_, col)| {
                    // If we are joining with USING, we need to deduplicate the columns from the right table
                    // that are also present in the USING clause.
                    if let Some(join_info) = &table.join_info {
                        !join_info.using.iter().any(|using_col| {
                            col.name
                                .as_ref()
                                .is_some_and(|name| name.eq_ignore_ascii_case(using_col.as_str()))
                        })
                    } else {
                        true
                    }
                })
                .map(|(i, col)| ResultSetColumn {
                    alias: None,
                    expr: ast::Expr::Column {
                        database: None,
                        table: table.internal_id,
                        column: i,
                        is_rowid_alias: col.is_rowid_alias(),
                    },
                    contains_aggregates: false,
                }),
        );
    }
}

/// Join information for a table reference.
#[derive(Debug, Clone)]
pub struct JoinInfo {
    /// Whether this is an OUTER JOIN.
    pub outer: bool,
    /// FULL OUTER JOIN (implies `outer = true`).
    pub full_outer: bool,
    /// The USING clause for the join, if any. NATURAL JOIN is transformed into USING (col1, col2, ...).
    pub using: Vec<ast::Name>,
}

/// A joined table in the query plan.
/// For example,
/// ```sql
/// SELECT * FROM users u JOIN products p JOIN (SELECT * FROM users) sub;
/// ```
/// has three table references where
/// - all have [Operation::Scan]
/// - identifiers are `t`, `p`, `sub`
/// - `t` and `p` are [Table::BTree] while `sub` is [Table::FromClauseSubquery]
/// - join_info is None for the first table reference, and Some(JoinInfo { outer: false, using: None }) for the second and third table references
#[derive(Debug, Clone)]
pub struct JoinedTable {
    /// The operation that this table reference performs.
    pub op: Operation,
    /// Table object, which contains metadata about the table, e.g. columns.
    pub table: Table,
    /// The name of the table as referred to in the query, either the literal name or an alias e.g. "users" or "u"
    pub identifier: String,
    /// Internal ID of the table reference, used in e.g. [Expr::Column] to refer to this table.
    pub internal_id: TableInternalId,
    /// The join info for this table reference, if it is the right side of a join (which all except the first table reference have)
    pub join_info: Option<JoinInfo>,
    /// Bitmask of columns that are referenced in the query.
    /// Used to decide whether a covering index can be used.
    pub col_used_mask: ColumnUsedMask,
    /// Count of how many times each column is referenced.
    ///
    /// Expression indexes can satisfy a column requirement if the column is
    /// only used to build the expression itself. Tracking counts lets us
    /// subtract a column from the covering set only when every usage is
    /// accounted for by an expression index.
    pub column_use_counts: Vec<usize>,
    /// Expressions referencing this table that may be satisfied by an expression index.
    ///
    /// Each entry stores the normalized expression text and the columns it
    /// needs. During covering checks we ask: does an index contain this
    /// expression? If yes, all columns that *only* feed this expression can be
    /// removed from the required-column set.
    pub expression_index_usages: Vec<ExpressionIndexUsage>,
    /// The index of the database. "main" is always zero.
    pub database_id: usize,
}

#[derive(Debug, Clone)]
pub struct OuterQueryReference {
    /// The name of the table as referred to in the query, either the literal name or an alias e.g. "users" or "u"
    pub identifier: String,
    /// Internal ID of the table reference, used in e.g. [Expr::Column] to refer to this table.
    pub internal_id: TableInternalId,
    /// Table object, which contains metadata about the table, e.g. columns.
    pub table: Table,
    /// Bitmask of columns that are referenced in the query.
    /// Used to track dependencies, so that it can be resolved
    /// when a WHERE clause subquery should be evaluated;
    /// i.e., if the subquery depends on tables T and U,
    /// then both T and U need to be in scope for the subquery to be evaluated.
    pub col_used_mask: ColumnUsedMask,
    /// Original CTE SELECT AST for re-planning. When a CTE is referenced
    /// multiple times, each reference needs a fresh plan with unique
    /// internal_ids to avoid cursor key collisions.
    pub cte_select: Option<ast::Select>,
    /// Explicit column names from WITH t(a, b) AS (...) syntax.
    pub cte_explicit_columns: Vec<String>,
    /// CTE ID if this is a CTE reference. Used to track CTE reference counts
    /// for materialization decisions.
    pub cte_id: Option<usize>,
    /// When true, this entry is only for CTE definition lookup in subquery
    /// FROM clauses, not for column resolution. This is set when the CTE
    /// has been consumed by a FROM clause (with or without an alias), so
    /// column resolution goes through the joined_table instead.
    pub cte_definition_only: bool,
    /// Whether the rowid of this table is referenced. Tracked separately from
    /// col_used_mask because rowid is not a real column and setting a fake
    /// column index in col_used_mask could mislead covering index decisions.
    pub rowid_referenced: bool,
}

impl OuterQueryReference {
    /// Returns the columns of the table that this outer query reference refers to.
    pub fn columns(&self) -> &[Column] {
        self.table.columns()
    }

    /// Marks a column as used; used means that the column is referenced in the query.
    pub fn mark_column_used(&mut self, column_index: usize) {
        self.col_used_mask.set(column_index);
    }

    /// Whether the OuterQueryReference is used by the current query scope.
    /// This is used primarily to determine at what loop depth a subquery should be evaluated.
    pub fn is_used(&self) -> bool {
        !self.col_used_mask.is_empty() || self.rowid_referenced
    }
}

#[derive(Debug, Clone)]
/// A collection of table references in a given SQL statement.
///
/// `TableReferences::joined_tables` is the list of tables that are joined together.
/// Example: SELECT * FROM t JOIN u JOIN v -- the joined tables are t, u and v.
///
/// `TableReferences::outer_query_refs` are references to tables outside the current scope.
/// Example: SELECT * FROM t WHERE EXISTS (SELECT * FROM u WHERE u.foo = t.foo)
/// -- here, 'u' is an outer query reference for the subquery (SELECT * FROM u WHERE u.foo = t.foo),
/// since that query does not declare 't' in its FROM clause.
///
///
/// Typically a query will only have joined tables, but the following may have outer query references:
/// - CTEs that refer to other preceding CTEs
/// - Correlated subqueries, i.e. subqueries that depend on the outer scope
pub struct TableReferences {
    /// Tables that are joined together in this query scope.
    joined_tables: Vec<JoinedTable>,
    /// Tables from outer scopes that are referenced in this query scope.
    outer_query_refs: Vec<OuterQueryReference>,
    /// Set when a RIGHT JOIN is rewritten as LEFT JOIN by swapping the two tables,
    /// so `select_star` emits columns in the original user-visible order.
    right_join_swapped: bool,
}

impl TableReferences {
    /// The maximum number of tables that can be joined together in a query.
    /// This limit is arbitrary, although we currently use a u128 to represent the [crate::translate::planner::TableMask],
    /// which can represent up to 128 tables.
    /// Even at 63 tables we currently cannot handle the optimization performantly, hence the arbitrary cap.
    pub const MAX_JOINED_TABLES: usize = 63;
    pub fn new(
        joined_tables: Vec<JoinedTable>,
        outer_query_refs: Vec<OuterQueryReference>,
    ) -> Self {
        Self {
            joined_tables,
            outer_query_refs,
            right_join_swapped: false,
        }
    }
    pub fn new_empty() -> Self {
        Self {
            joined_tables: Vec::new(),
            outer_query_refs: Vec::new(),
            right_join_swapped: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.joined_tables.is_empty() && self.outer_query_refs.is_empty()
    }

    /// Mark that tables were swapped for a RIGHT-to-LEFT JOIN rewrite.
    pub fn set_right_join_swapped(&mut self) {
        self.right_join_swapped = true;
    }

    /// Whether tables were swapped for a RIGHT JOIN rewrite.
    pub fn right_join_swapped(&self) -> bool {
        self.right_join_swapped
    }

    /// Add a new [JoinedTable] to the query plan.
    pub fn add_joined_table(&mut self, joined_table: JoinedTable) {
        self.joined_tables.push(joined_table);
    }

    /// Add a new [OuterQueryReference] to the query plan.
    pub fn add_outer_query_reference(&mut self, outer_query_reference: OuterQueryReference) {
        self.outer_query_refs.push(outer_query_reference);
    }

    /// Returns an immutable reference to the [JoinedTable]s in the query plan.
    pub fn joined_tables(&self) -> &[JoinedTable] {
        &self.joined_tables
    }

    /// Returns a mutable reference to the [JoinedTable]s in the query plan.
    pub fn joined_tables_mut(&mut self) -> &mut Vec<JoinedTable> {
        &mut self.joined_tables
    }

    /// Resets the expression index usages for all joined tables.
    pub fn reset_expression_index_usages(&mut self) {
        for table in self.joined_tables.iter_mut() {
            table.clear_expression_index_usages();
        }
    }

    /// Called before optimization so we can reuse the same registration
    /// for result columns, ORDER BY, and GROUP BY expressions. If a
    /// SELECT lists `LOWER(name)` and an index exists on `LOWER(name)`, we
    /// can plan a covering scan because the expression value lives inside
    /// the index key.
    pub fn register_expression_index_usage(&mut self, expr: &ast::Expr) {
        let Some((table_id, columns_mask)) = single_table_column_usage(expr) else {
            return;
        };
        let Some(table_ref) = self
            .joined_tables()
            .iter()
            .find(|t| t.internal_id == table_id)
        else {
            return;
        };
        let normalized = normalize_expr_for_index_matching(expr, table_ref, self);
        if let Some(table_ref_mut) = self
            .joined_tables_mut()
            .iter_mut()
            .find(|t| t.internal_id == table_id)
        {
            table_ref_mut.register_expression_index_usage(normalized, columns_mask);
        }
    }

    /// Returns an immutable reference to the [OuterQueryReference]s in the query plan.
    pub fn outer_query_refs(&self) -> &[OuterQueryReference] {
        &self.outer_query_refs
    }

    /// Returns an immutable reference to the [OuterQueryReference] with the given internal ID.
    pub fn find_outer_query_ref_by_internal_id(
        &self,
        internal_id: TableInternalId,
    ) -> Option<&OuterQueryReference> {
        self.outer_query_refs
            .iter()
            .find(|t| t.internal_id == internal_id)
    }

    /// Returns a mutable reference to the [OuterQueryReference] with the given internal ID.
    pub fn find_outer_query_ref_by_internal_id_mut(
        &mut self,
        internal_id: TableInternalId,
    ) -> Option<&mut OuterQueryReference> {
        self.outer_query_refs
            .iter_mut()
            .find(|t| t.internal_id == internal_id)
    }

    /// Returns an immutable reference to the [Table] with the given internal ID,
    /// plus a boolean indicating whether the table is a joined table from the current query scope (false),
    /// or an outer query reference (true).
    pub fn find_table_by_internal_id(
        &self,
        internal_id: TableInternalId,
    ) -> Option<(bool, &Table)> {
        self.joined_tables
            .iter()
            .find(|t| t.internal_id == internal_id)
            .map(|t| (false, &t.table))
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|t| t.internal_id == internal_id)
                    .map(|t| (true, &t.table))
            })
    }

    /// Returns an immutable reference to the [Table] with the given identifier,
    /// where identifier is either the literal name of the table or an alias.
    pub fn find_table_by_identifier(&self, identifier: &str) -> Option<&Table> {
        self.joined_tables
            .iter()
            .find(|t| t.identifier == identifier)
            .map(|t| &t.table)
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|t| t.identifier == identifier)
                    .map(|t| &t.table)
            })
    }

    /// Returns an immutable reference to the [OuterQueryReference] with the given identifier,
    /// where identifier is either the literal name of the table or an alias.
    pub fn find_outer_query_ref_by_identifier(
        &self,
        identifier: &str,
    ) -> Option<&OuterQueryReference> {
        self.outer_query_refs
            .iter()
            .find(|t| t.identifier == identifier)
    }

    /// Marks the pre-planned [OuterQueryReference] with the given identifier as
    /// "CTE definition only". This prevents it from being used for column
    /// resolution while still allowing CTE definition lookup in subquery FROM
    /// clauses. Called when a CTE is consumed by a FROM clause, since column
    /// resolution is then handled by the joined_table entry instead.
    pub fn mark_outer_query_ref_cte_definition_only(&mut self, identifier: &str) {
        if let Some(outer_ref) = self
            .outer_query_refs
            .iter_mut()
            .find(|t| t.identifier == identifier)
        {
            outer_ref.cte_definition_only = true;
        }
    }

    /// Returns the internal ID and immutable reference to the [Table] with the given identifier,
    pub fn find_table_and_internal_id_by_identifier(
        &self,
        identifier: &str,
    ) -> Option<(TableInternalId, &Table)> {
        self.joined_tables
            .iter()
            .find(|t| t.identifier == identifier)
            .map(|t| (t.internal_id, &t.table))
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|t| t.identifier == identifier && !t.cte_definition_only)
                    .map(|t| (t.internal_id, &t.table))
            })
    }

    /// Returns an immutable reference to the [JoinedTable] with the given internal ID.
    pub fn find_joined_table_by_internal_id(
        &self,
        internal_id: TableInternalId,
    ) -> Option<&JoinedTable> {
        self.joined_tables
            .iter()
            .find(|t| t.internal_id == internal_id)
    }

    /// Returns a mutable reference to the [JoinedTable] with the given internal ID.
    pub fn find_joined_table_by_internal_id_mut(
        &mut self,
        internal_id: TableInternalId,
    ) -> Option<&mut JoinedTable> {
        self.joined_tables
            .iter_mut()
            .find(|t| t.internal_id == internal_id)
    }

    /// Marks a column as used; used means that the column is referenced in the query.
    pub fn mark_column_used(&mut self, internal_id: TableInternalId, column_index: usize) {
        if let Some(joined_table) = self.find_joined_table_by_internal_id_mut(internal_id) {
            joined_table.mark_column_used(column_index);
        } else if let Some(outer_query_ref) =
            self.find_outer_query_ref_by_internal_id_mut(internal_id)
        {
            outer_query_ref.mark_column_used(column_index);
        } else {
            panic!("table with internal id {internal_id} not found in table references");
        }
    }

    /// Marks the rowid of a table as referenced. This is tracked separately
    /// from column usage because rowid is not a real column.
    pub fn mark_rowid_referenced(&mut self, internal_id: TableInternalId) {
        if let Some(outer_query_ref) = self.find_outer_query_ref_by_internal_id_mut(internal_id) {
            outer_query_ref.rowid_referenced = true;
        }
        // For joined tables, rowid references don't need special tracking
        // since correlated subquery detection only looks at outer_query_refs.
    }

    pub fn contains_table(&self, table: &Table) -> bool {
        self.joined_tables
            .iter()
            .map(|t| &t.table)
            .chain(self.outer_query_refs.iter().map(|t| &t.table))
            .any(|t| match t {
                Table::FromClauseSubquery(subquery_table) => {
                    subquery_table.plan.select_contains_table(table)
                }
                _ => t == table,
            })
    }

    pub fn extend(&mut self, other: TableReferences) {
        self.joined_tables.extend(other.joined_tables);
        self.outer_query_refs.extend(other.outer_query_refs);
    }
}

/// Tracks which columns are used in a query. Optimized for the common case
/// of ≤64 columns (single u64), with heap-allocated overflow
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColumnUsedMask {
    inline: u64,
    overflow: Option<Vec<u64>>,
}

impl ColumnUsedMask {
    const INLINE_BITS: usize = 64;

    pub fn set(&mut self, index: usize) {
        if index < Self::INLINE_BITS {
            self.inline |= 1 << index;
        } else {
            let overflow_idx = (index - Self::INLINE_BITS) / 64;
            let bit = (index - Self::INLINE_BITS) % 64;
            let overflow = self.overflow.get_or_insert_with(Vec::new);
            if overflow_idx >= overflow.len() {
                overflow.resize(overflow_idx + 1, 0);
            }
            overflow[overflow_idx] |= 1 << bit;
        }
    }

    pub fn get(&self, index: usize) -> bool {
        if index < Self::INLINE_BITS {
            (self.inline >> index) & 1 != 0
        } else {
            let Some(overflow) = &self.overflow else {
                return false;
            };
            let overflow_idx = (index - Self::INLINE_BITS) / 64;
            let bit = (index - Self::INLINE_BITS) % 64;
            overflow
                .get(overflow_idx)
                .is_some_and(|word| (word >> bit) & 1 != 0)
        }
    }

    pub fn clear(&mut self, index: usize) {
        if index < Self::INLINE_BITS {
            self.inline &= !(1 << index);
        } else if let Some(overflow) = &mut self.overflow {
            let overflow_idx = (index - Self::INLINE_BITS) / 64;
            let bit = (index - Self::INLINE_BITS) % 64;
            if let Some(word) = overflow.get_mut(overflow_idx) {
                *word &= !(1 << bit);
            }
        }
    }

    pub fn contains_all_set_bits_of(&self, other: &Self) -> bool {
        if (self.inline & other.inline) != other.inline {
            return false;
        }
        match (&self.overflow, &other.overflow) {
            (None, None) => true,
            (None, Some(other_ov)) => other_ov.iter().all(|&w| w == 0),
            (Some(_), None) => true,
            (Some(self_ov), Some(other_ov)) => other_ov.iter().enumerate().all(|(i, &other_w)| {
                let self_w = self_ov.get(i).copied().unwrap_or(0);
                (self_w & other_w) == other_w
            }),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inline == 0
            && self
                .overflow
                .as_ref()
                .is_none_or(|ov| ov.iter().all(|&w| w == 0))
    }

    pub fn is_only(&self, index: usize) -> bool {
        if index < Self::INLINE_BITS {
            self.inline == (1 << index)
                && self
                    .overflow
                    .as_ref()
                    .is_none_or(|ov| ov.iter().all(|&w| w == 0))
        } else {
            if self.inline != 0 {
                return false;
            }
            let Some(overflow) = &self.overflow else {
                return false;
            };
            let overflow_idx = (index - Self::INLINE_BITS) / 64;
            let bit = (index - Self::INLINE_BITS) % 64;
            // The overflow vector must be long enough to contain the target index
            if overflow_idx >= overflow.len() {
                return false;
            }
            overflow.iter().enumerate().all(|(i, &w)| {
                if i == overflow_idx {
                    w == (1 << bit)
                } else {
                    w == 0
                }
            })
        }
    }

    pub fn subtract(&mut self, other: &Self) {
        self.inline &= !other.inline;
        if let (Some(self_ov), Some(other_ov)) = (&mut self.overflow, &other.overflow) {
            for (i, other_w) in other_ov.iter().enumerate() {
                if let Some(self_w) = self_ov.get_mut(i) {
                    *self_w &= !other_w;
                }
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        let inline_iter = (0..Self::INLINE_BITS).filter(|&i| (self.inline >> i) & 1 != 0);
        let overflow_iter = self
            .overflow
            .iter()
            .flat_map(|ov| ov.iter().enumerate())
            .flat_map(|(word_idx, &word)| {
                (0..64).filter_map(move |bit| {
                    if (word >> bit) & 1 != 0 {
                        Some(Self::INLINE_BITS + word_idx * 64 + bit)
                    } else {
                        None
                    }
                })
            });
        inline_iter.chain(overflow_iter)
    }
}

impl std::ops::BitOrAssign<&Self> for ColumnUsedMask {
    fn bitor_assign(&mut self, rhs: &Self) {
        self.inline |= rhs.inline;
        if let Some(rhs_ov) = &rhs.overflow {
            let self_ov = self.overflow.get_or_insert_with(Vec::new);
            if self_ov.len() < rhs_ov.len() {
                self_ov.resize(rhs_ov.len(), 0);
            }
            for (i, &rhs_w) in rhs_ov.iter().enumerate() {
                self_ov[i] |= rhs_w;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExpressionIndexUsage {
    /// Normalized (non-bound) ast of the expression as stored on an index column.
    /// Example: `lower(name)` for INDEX ON t(lower(name)).
    pub normalized_expr: Box<ast::Expr>,
    /// Columns required to compute the expression. Helps decide whether using
    /// the expression value from the index fully covers those column reads.
    pub columns_mask: ColumnUsedMask,
}

/// Represents one key pair in a hash join equality condition.
/// For `expr1 = expr2`, this tracks which WHERE term contains the equality
/// and which side of the equality belongs to the build table.
#[derive(Debug, Clone, Copy)]
pub struct HashJoinKey {
    /// Index into the where_clause vector
    pub where_clause_idx: usize,
    /// Which side of the binary equality expression belongs to the build table.
    /// The other side belongs to the probe table.
    pub build_side: BinaryExprSide,
}

impl HashJoinKey {
    /// Get the build table's expression from the WHERE clause.
    pub fn get_build_expr<'a>(&self, where_clause: &'a [WhereTerm]) -> &'a ast::Expr {
        let where_term = &where_clause[self.where_clause_idx];
        let Ok(Some((lhs, _, rhs))) = as_binary_components(&where_term.expr) else {
            panic!("HashJoinKey: expected a valid binary expression");
        };
        if self.build_side == BinaryExprSide::Lhs {
            lhs
        } else {
            rhs
        }
    }

    /// Get the probe table's expression from the WHERE clause.
    pub fn get_probe_expr<'a>(&self, where_clause: &'a [WhereTerm]) -> &'a ast::Expr {
        let where_term = &where_clause[self.where_clause_idx];
        let Ok(Some((lhs, _, rhs))) = as_binary_components(&where_term.expr) else {
            panic!("HashJoinKey: expected a valid binary expression");
        };
        if self.build_side == BinaryExprSide::Lhs {
            rhs // probe is the opposite side
        } else {
            lhs
        }
    }
}

/// Hash join semantics. Build = LHS (populates hash table), Probe = RHS (scanned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashJoinType {
    /// Only matching rows emitted.
    Inner,
    /// All build rows appear; unmatched build rows get NULLs for the probe side.
    LeftOuter,
    /// Like LeftOuter, plus unmatched probe rows get NULLs for the build side.
    FullOuter,
}

/// Hash join operation metadata
#[derive(Debug, Clone)]
pub struct HashJoinOp {
    /// Index of the build table in the join order
    pub build_table_idx: usize,
    /// Index of the probe table in the join order
    pub probe_table_idx: usize,
    /// Join key references, each entry points to an equality condition in the [WhereTerm]
    /// and indicates which side of the equality belongs to the build table.
    pub join_keys: Vec<HashJoinKey>,
    /// Memory budget for hash table
    pub mem_budget: usize,
    /// Whether the build input should be materialized as a rowid list before hash build.
    pub materialize_build_input: bool,
    /// Whether to use a bloom filter on the probe side.
    pub use_bloom_filter: bool,
    /// Join semantics (inner, left outer, or full outer).
    pub join_type: HashJoinType,
}

/// Distinguishes union (OR) from intersection (AND) operations for multi-index scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperation {
    /// Union: rowid appears in result if it's in ANY branch (OR)
    Union,
    /// Intersection: rowid appears in result only if it's in ALL branches (AND)
    Intersection,
}

/// Multi-index scan operation metadata for OR-by-union or AND-by-intersection optimization.
///
/// When a WHERE clause contains an OR of terms that can each use a different index,
/// we can scan each index separately and combine the results using a RowSet for deduplication.
/// For example: `WHERE a = 1 OR b = 2` with indexes on `a` and `b`.
///
/// Similarly, when a WHERE clause contains AND terms on different indexed columns,
/// we can scan each index and intersect the results to reduce the number of table fetches.
/// For example: `WHERE a = 1 AND b = 2` with separate indexes on `a` and `b`.
#[derive(Debug, Clone)]
pub struct MultiIndexScanOp {
    /// Each branch represents one term with its own index access
    pub branches: Vec<MultiIndexBranch>,
    /// Index of the primary WHERE term.
    /// For Union: the index of the OR expression.
    /// For Intersection: the index of the first AND term consumed.
    pub where_term_idx: usize,
    /// The set operation to perform when combining branches
    pub set_op: SetOperation,
    /// For Intersection: indices of additional WHERE terms consumed (besides where_term_idx)
    pub additional_consumed_terms: Vec<usize>,
}

/// A single branch of a multi-index scan, representing one disjunct of an OR expression.
#[derive(Debug, Clone)]
pub struct MultiIndexBranch {
    /// The index to use for this branch, or None for rowid access
    pub index: Option<Arc<Index>>,
    /// The seek definition for this branch
    pub seek_def: SeekDef,
    /// Estimated number of rows from this branch
    pub estimated_rows: f64,
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Operation {
    // Scan operation
    // This operation is used to scan a table.
    Scan(Scan),
    // Search operation
    // This operation is used to search for a row in a table using an index
    // (i.e. a primary key or a secondary index)
    Search(Search),
    // Access through custom index method query
    IndexMethodQuery(IndexMethodQuery),
    // Hash join operation
    // This operation is used on the probe side of a hash join.
    // The build table is accessed normally (via Scan), and the probe table
    // uses this operation to indicate it should probe the hash table.
    HashJoin(HashJoinOp),
    // Multi-index scan operation for OR-by-union optimization.
    // This operation scans multiple indexes (one per OR branch) and combines
    // results using RowSet deduplication.
    MultiIndexScan(MultiIndexScanOp),
}

impl Operation {
    pub fn default_scan_for(table: &Table) -> Self {
        match table {
            Table::BTree(_) => Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            Table::Virtual(_) => Operation::Scan(Scan::VirtualTable {
                idx_num: -1,
                idx_str: None,
                constraints: Vec::new(),
            }),
            Table::FromClauseSubquery(_) => Operation::Scan(Scan::Subquery),
        }
    }

    pub fn index(&self) -> Option<&Arc<Index>> {
        match self {
            Operation::Scan(Scan::BTreeTable { index, .. }) => index.as_ref(),
            Operation::Search(Search::Seek { index, .. }) => index.as_ref(),
            Operation::IndexMethodQuery(IndexMethodQuery { index, .. }) => Some(index),
            Operation::Scan(_) => None,
            Operation::Search(Search::RowidEq { .. }) => None,
            Operation::HashJoin(_) => None,
            // Multi-index scan uses multiple indexes; return None as there's no single index
            Operation::MultiIndexScan(_) => None,
        }
    }
}

impl JoinedTable {
    /// Returns the btree table for this table reference, if it is a BTreeTable.
    pub fn btree(&self) -> Option<Arc<BTreeTable>> {
        match &self.table {
            Table::BTree(_) => self.table.btree(),
            _ => None,
        }
    }
    pub fn virtual_table(&self) -> Option<Arc<VirtualTable>> {
        match &self.table {
            Table::Virtual(_) => self.table.virtual_table(),
            _ => None,
        }
    }

    /// Creates a new TableReference for a subquery from a SelectPlan.
    pub fn new_subquery(
        identifier: String,
        plan: SelectPlan,
        join_info: Option<JoinInfo>,
        internal_id: TableInternalId,
    ) -> Result<Self> {
        let mut columns = plan
            .result_columns
            .iter()
            .map(|rc| {
                let (col_type, type_name) =
                    infer_type_from_expr(&rc.expr, Some(&plan.table_references));
                Column::new(
                    rc.name(&plan.table_references).map(String::from),
                    type_name.to_string(),
                    None,
                    None,
                    col_type,
                    None,
                    ColDef::default(),
                )
            })
            .collect::<Vec<_>>();

        for (i, column) in columns.iter_mut().enumerate() {
            column.set_collation(get_collseq_from_expr(
                &plan.result_columns[i].expr,
                &plan.table_references,
            )?);
        }

        let table = Table::FromClauseSubquery(Arc::new(FromClauseSubquery {
            name: identifier.clone(),
            plan: Box::new(Plan::Select(plan)),
            columns,
            result_columns_start_reg: None,
            cte_id: None,
            materialize_hint: false,
        }));
        Ok(Self {
            op: Operation::default_scan_for(&table),
            table,
            identifier,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: 0,
        })
    }

    /// Creates a new TableReference for a subquery from a Plan (either SelectPlan or CompoundSelect).
    /// If `explicit_columns` is provided, those names override the derived column names from the SELECT.
    /// If `cte_id` is provided, this subquery is a CTE reference that can share materialized data.
    /// If `materialize_hint` is true, the CTE was declared with AS MATERIALIZED and should always
    /// be materialized regardless of reference count.
    pub fn new_subquery_from_plan(
        identifier: String,
        plan: Plan,
        join_info: Option<JoinInfo>,
        internal_id: TableInternalId,
        explicit_columns: Option<&[String]>,
        cte_id: Option<usize>,
        materialize_hint: bool,
    ) -> Result<Self> {
        // Get result columns and table references from the plan
        let (result_columns, table_references) = match &plan {
            Plan::Select(select_plan) => {
                (&select_plan.result_columns, &select_plan.table_references)
            }
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                // For compound selects, SQLite uses the leftmost select's column names.
                // The leftmost select is left[0] if the vec is not empty, otherwise right_most.
                if !left.is_empty() {
                    (&left[0].0.result_columns, &left[0].0.table_references)
                } else {
                    (&right_most.result_columns, &right_most.table_references)
                }
            }
            Plan::Delete(_) | Plan::Update(_) => {
                unreachable!("DELETE/UPDATE plans cannot be subqueries")
            }
        };

        // Validate explicit column count if provided
        if let Some(cols) = explicit_columns {
            if cols.len() != result_columns.len() {
                crate::bail_parse_error!(
                    "table {} has {} columns but {} column names were provided",
                    identifier,
                    result_columns.len(),
                    cols.len()
                );
            }
        }

        let mut columns = result_columns
            .iter()
            .enumerate()
            .map(|(i, rc)| {
                // Use explicit column name if provided, otherwise derive from result column
                let col_name = explicit_columns
                    .and_then(|cols| cols.get(i).cloned())
                    .or_else(|| rc.name(table_references).map(String::from));
                let (col_type, type_name) = infer_type_from_expr(&rc.expr, Some(table_references));
                Column::new(
                    col_name,
                    type_name.to_string(),
                    None,
                    None,
                    col_type,
                    None,
                    ColDef::default(),
                )
            })
            .collect::<Vec<_>>();

        for (i, column) in columns.iter_mut().enumerate() {
            column.set_collation(get_collseq_from_expr(
                &result_columns[i].expr,
                table_references,
            )?);
        }

        // materialize_hint is set true for explicit WITH ... AS MATERIALIZED hint.
        // Multi-reference CTEs are also detected at emission time via reference counting,
        // and they may be materialized regardless of explicit keyword usage.
        let table = Table::FromClauseSubquery(Arc::new(FromClauseSubquery {
            name: identifier.clone(),
            plan: Box::new(plan),
            columns,
            result_columns_start_reg: None,
            cte_id,
            materialize_hint,
        }));
        Ok(Self {
            op: Operation::default_scan_for(&table),
            table,
            identifier,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: 0,
        })
    }

    pub fn columns(&self) -> &[Column] {
        self.table.columns()
    }

    /// Mark a column as used in the query.
    /// This is used to determine whether a covering index can be used.
    pub fn mark_column_used(&mut self, index: usize) {
        if index >= self.column_use_counts.len() {
            self.column_use_counts.resize(index + 1, 0);
        }
        self.column_use_counts[index] += 1;
        self.col_used_mask.set(index);
    }

    /// Clear any previously registered expression index usages.
    pub fn clear_expression_index_usages(&mut self) {
        self.expression_index_usages.clear();
    }

    /// Example: SELECT a+b FROM t WHERE a+b=5 with INDEX ON t(a+b)
    /// We want to remember that (a+b) is available on an index key and that
    /// columns a and b are only needed to produce that expression. Later we
    /// can avoid opening the table cursor if all column references are
    /// covered by expression keys.
    pub fn register_expression_index_usage(
        &mut self,
        normalized_expr: ast::Expr,
        columns_mask: ColumnUsedMask,
    ) {
        if columns_mask.is_empty() {
            return;
        }
        if self
            .expression_index_usages
            .iter()
            .any(|usage| exprs_are_equivalent(&usage.normalized_expr, &normalized_expr))
        {
            return;
        }
        self.expression_index_usages.push(ExpressionIndexUsage {
            normalized_expr: Box::new(normalized_expr),
            columns_mask,
        });
    }

    /// Provided an index that may contain expression keys, remove any
    /// columns from `required_columns` that are fully covered by expression index values.
    fn apply_expression_index_coverage(
        &self,
        index: &Index,
        required_columns: &mut ColumnUsedMask,
    ) {
        let mut coverage_counts = vec![0usize; self.column_use_counts.len()];
        let mut any_covered = false;
        for usage in &self.expression_index_usages {
            // If the index stores the expression (e.g. idx on lower(name)), all
            // columns needed *solely* for that expression can be treated as
            // covered by the index key. Example:
            //   CREATE INDEX idx ON t(lower(name));
            //   SELECT lower(name) FROM t;
            // Column `name` is not otherwise needed, so we can rely on the
            // expression value from the index and drop the table cursor.
            if index
                .expression_to_index_pos(&usage.normalized_expr)
                .is_some()
            {
                any_covered = true;
                for col_idx in usage.columns_mask.iter() {
                    if col_idx >= coverage_counts.len() {
                        coverage_counts.resize(col_idx + 1, 0);
                    }
                    coverage_counts[col_idx] += 1;
                }
            }
        }
        if !any_covered {
            return;
        }
        for (col_idx, &covered) in coverage_counts.iter().enumerate() {
            if covered == 0 {
                continue;
            }
            // Only drop the requirement if *all* references to this column are
            // satisfied by expression-index values. If the column is also
            // selected or filtered directly, the table data is still needed.
            if self.column_use_counts.get(col_idx).copied().unwrap_or(0) == covered {
                required_columns.clear(col_idx);
            }
        }
    }

    /// Open the necessary cursors for this table reference.
    /// Generally a table cursor is always opened unless a SELECT query can use a covering index.
    /// An index cursor is opened if an index is used in any way for reading data from the table.
    pub fn open_cursors(
        &self,
        program: &mut ProgramBuilder,
        mode: OperationMode,
        schema: &Schema,
    ) -> Result<(Option<CursorID>, Option<CursorID>)> {
        let index = self.op.index();
        match &self.table {
            Table::BTree(btree) => {
                let use_covering_index = self.utilizes_covering_index();
                let index_is_ephemeral = index.is_some_and(|index| index.ephemeral);
                let table_not_required = matches!(mode, OperationMode::SELECT)
                    && use_covering_index
                    && !index_is_ephemeral;
                let table_cursor_id = if table_not_required {
                    None
                } else if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
                    target_table,
                    ..
                }) = &mode
                {
                    // The cursor for the ephemeral table was already allocated earlier. Let's allocate one for the target table,
                    // in case it wasn't already allocated when populating the ephemeral table.
                    Some(program.alloc_cursor_id_keyed_if_not_exists(
                        CursorKey::table(target_table.internal_id),
                        match &target_table.table {
                            Table::BTree(btree) => CursorType::BTreeTable(btree.clone()),
                            Table::Virtual(virtual_table) => {
                                CursorType::VirtualTable(virtual_table.clone())
                            }
                            _ => unreachable!("target table must be a btree or virtual table"),
                        },
                    ))
                } else {
                    // Check if this is a materialized view
                    let cursor_type =
                        if let Some(view_mutex) = schema.get_materialized_view(&btree.name) {
                            CursorType::MaterializedView(btree.clone(), view_mutex)
                        } else {
                            CursorType::BTreeTable(btree.clone())
                        };
                    Some(program.alloc_cursor_id_keyed_if_not_exists(
                        CursorKey::table(self.internal_id),
                        cursor_type,
                    ))
                };

                let index_cursor_id = index
                    .map(|index| {
                        program.alloc_cursor_index(
                            Some(CursorKey::index(self.internal_id, index.clone())),
                            index,
                        )
                    })
                    .transpose()?;
                Ok((table_cursor_id, index_cursor_id))
            }
            Table::Virtual(virtual_table) => {
                let table_cursor_id = Some(program.alloc_cursor_id_keyed(
                    CursorKey::table(self.internal_id),
                    CursorType::VirtualTable(virtual_table.clone()),
                ));
                let index_cursor_id = None;
                Ok((table_cursor_id, index_cursor_id))
            }
            Table::FromClauseSubquery(..) => Ok((None, None)),
        }
    }

    /// Resolve the already opened cursors for this table reference.
    pub fn resolve_cursors(
        &self,
        program: &mut ProgramBuilder,
        mode: OperationMode,
    ) -> Result<(Option<CursorID>, Option<CursorID>)> {
        let index = self.op.index();
        let table_cursor_id =
            if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
                target_table,
                ..
            }) = &mode
            {
                program.resolve_cursor_id_safe(&CursorKey::table(target_table.internal_id))
            } else {
                program.resolve_cursor_id_safe(&CursorKey::table(self.internal_id))
            };
        let index_cursor_id = index.map(|index| {
            program.resolve_cursor_id(&CursorKey::index(self.internal_id, index.clone()))
        });
        Ok((table_cursor_id, index_cursor_id))
    }

    /// Returns true if a given index is a covering index for this [TableReference].
    pub fn index_is_covering(&self, index: &Index) -> bool {
        let Table::BTree(btree) = &self.table else {
            return false;
        };
        if self.col_used_mask.is_empty() {
            return false;
        }
        if index.index_method.is_some() {
            return false;
        }

        if self.expression_index_usages.is_empty() {
            Self::index_covers_columns(index, btree, &self.col_used_mask)
        } else {
            let mut required_columns = self.col_used_mask.clone();
            self.apply_expression_index_coverage(index, &mut required_columns);
            if required_columns.is_empty() {
                return true;
            }
            Self::index_covers_columns(index, btree, &required_columns)
        }
    }

    fn index_covers_columns(
        index: &Index,
        btree: &BTreeTable,
        required_columns: &ColumnUsedMask,
    ) -> bool {
        // If a table has a rowid, the index is guaranteed to contain it as well.
        let rowid_alias_pos = if btree.has_rowid {
            btree.get_rowid_alias_column().map(|(pos, _)| pos)
        } else {
            None
        };

        if let Some(pos) = rowid_alias_pos {
            if required_columns.is_only(pos) {
                // If the index would be ONLY used for the rowid, don't bother.
                // Example: SELECT id FROM t where id is a rowid alias - just scan the table.
                return false;
            }
        }

        // Check that every required column is covered by the index
        for required_col in required_columns.iter() {
            if rowid_alias_pos == Some(required_col) {
                // rowid is always implicitly covered by the index
                continue;
            }
            let covered_by_index = index
                .columns
                .iter()
                .any(|c| c.expr.is_none() && c.pos_in_table == required_col);
            if !covered_by_index {
                return false;
            }
        }
        true
    }

    /// Returns true if the index selected for use with this [TableReference] is a covering index,
    /// meaning that it contains all the columns that are referenced in the query.
    pub fn utilizes_covering_index(&self) -> bool {
        let Some(index) = self.op.index() else {
            return false;
        };
        self.index_is_covering(index.as_ref())
    }

    pub fn column_is_used(&self, index: usize) -> bool {
        self.col_used_mask.get(index)
    }
}

/// A definition of a rowid/index search.
///
/// [SeekKey] is the condition that is used to seek to a specific row in a table/index.
/// [SeekKey] also used to represent range scan termination condition.
#[derive(Debug, Clone)]
pub struct SeekDef {
    /// Common prefix of the key which is shared between start/end fields
    /// For example, given:
    /// - CREATE INDEX i ON t (x, y desc)
    /// - SELECT * FROM t WHERE x = 1 AND y >= 30
    ///
    /// Then, prefix=[(eq=1, ASC)], start=Some((ge, Expr(30))), end=Some((gt, Sentinel))
    pub prefix: Vec<SeekRangeConstraint>,
    /// The condition to use when seeking. See [SeekKey] for more details.
    pub start: SeekKey,
    /// The condition to use when terminating the scan that follows the seek. See [SeekKey] for more details.
    pub end: SeekKey,
    /// The direction of the scan that follows the seek.
    pub iter_dir: IterationDirection,
}

pub struct SeekDefKeyIterator<'a, T> {
    seek_def: &'a SeekDef,
    seek_key: &'a SeekKey,
    pos: usize,
    _t: PhantomData<T>,
}

impl<'a> Iterator for SeekDefKeyIterator<'a, SeekKeyComponent<&'a ast::Expr>> {
    type Item = SeekKeyComponent<&'a ast::Expr>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.pos < self.seek_def.prefix.len() {
            Some(SeekKeyComponent::Expr(
                &self.seek_def.prefix[self.pos].eq.as_ref().unwrap().1,
            ))
        } else if self.pos == self.seek_def.prefix.len() {
            match &self.seek_key.last_component {
                SeekKeyComponent::Expr(expr) => Some(SeekKeyComponent::Expr(expr)),
                SeekKeyComponent::Null => Some(SeekKeyComponent::Null),
                SeekKeyComponent::None => None,
            }
        } else {
            None
        };
        self.pos += 1;
        result
    }
}

impl<'a> Iterator for SeekDefKeyIterator<'a, Affinity> {
    type Item = Affinity;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.pos < self.seek_def.prefix.len() {
            Some(self.seek_def.prefix[self.pos].eq.as_ref().unwrap().2)
        } else if self.pos == self.seek_def.prefix.len() {
            match &self.seek_key.last_component {
                SeekKeyComponent::Expr(..) => Some(self.seek_key.affinity),
                // NULL sentinel does not require conversion; use NONE affinity so width matches.
                SeekKeyComponent::Null => Some(Affinity::Blob),
                SeekKeyComponent::None => None,
            }
        } else {
            None
        };
        self.pos += 1;
        result
    }
}

impl SeekDef {
    /// returns amount of values in the given seek key
    /// - so, for SELECT * FROM t WHERE x = 10 AND y = 20 AND y >= 30 there will be 3 values (10, 20, 30)
    pub fn size(&self, key: &SeekKey) -> usize {
        self.prefix.len()
            + match key.last_component {
                SeekKeyComponent::Expr(_) => 1,
                SeekKeyComponent::Null => 1,
                SeekKeyComponent::None => 0,
            }
    }
    /// iterate over value expressions in the given seek key
    pub fn iter<'a>(
        &'a self,
        key: &'a SeekKey,
    ) -> SeekDefKeyIterator<'a, SeekKeyComponent<&'a ast::Expr>> {
        SeekDefKeyIterator {
            seek_def: self,
            seek_key: key,
            pos: 0,
            _t: PhantomData,
        }
    }

    /// iterate over affinity in the given seek key
    pub fn iter_affinity<'a>(&'a self, key: &'a SeekKey) -> SeekDefKeyIterator<'a, Affinity> {
        SeekDefKeyIterator {
            seek_def: self,
            seek_key: key,
            pos: 0,
            _t: PhantomData,
        }
    }
}

/// [SeekKeyComponent] represents the optional trailing component of a seek key.
/// Besides user-provided expressions, planner logic may inject a synthetic NULL sentinel
/// to encode SQLite-compatible boundary behavior on composite indexes.
/// This enum accepts generic argument E so we can use both
/// SeekKeyComponent<ast::Expr> and SeekKeyComponent<&ast::Expr>.
#[derive(Debug, Clone)]
pub enum SeekKeyComponent<E> {
    Expr(E),
    Null,
    None,
}

/// A condition to use when seeking.
#[derive(Debug, Clone)]
pub struct SeekKey {
    /// Complete key must be constructed from common [SeekDef::prefix] and optional last_component
    pub last_component: SeekKeyComponent<ast::Expr>,

    /// The comparison operator to use when seeking.
    pub op: SeekOp,

    /// Affinity of the comparison
    pub affinity: Affinity,
}

/// Represents the type of table scan performed during query execution.
#[derive(Clone, Debug)]
pub enum Scan {
    /// A scan of a B-tree–backed table, optionally using an index, and with an iteration direction.
    BTreeTable {
        /// The iter_dir is used to indicate the direction of the iterator.
        iter_dir: IterationDirection,
        /// The index that we are using to scan the table, if any.
        index: Option<Arc<Index>>,
    },
    /// A scan of a virtual table, delegated to the table’s `filter` and related methods.
    VirtualTable {
        /// Index identifier returned by the table's `best_index` method.
        idx_num: i32,
        /// Optional index name returned by the table’s `best_index` method.
        idx_str: Option<String>,
        /// Constraining expressions to be passed to the table’s `filter` method.
        /// The order of expressions matches the argument order expected by the virtual table.
        constraints: Vec<Expr>,
    },
    /// A scan of a subquery in the `FROM` clause (using coroutines).
    Subquery,
}

/// An enum that represents a search operation that can be used to search for a row in a table using an index
/// (i.e. a primary key or a secondary index)
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum Search {
    /// A rowid equality point lookup. This is a special case that uses the SeekRowid bytecode instruction and does not loop.
    RowidEq { cmp_expr: ast::Expr },
    /// A search on a table btree (via `rowid`) or a secondary index search. Uses bytecode instructions like SeekGE, SeekGT etc.
    Seek {
        index: Option<Arc<Index>>,
        seek_def: SeekDef,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub struct IndexMethodQuery {
    /// index method to use
    pub index: Arc<Index>,
    /// idx of the pattern from [crate::index_method::IndexMethodAttachment::definition] which planner chose to use for the access
    pub pattern_idx: usize,
    /// captured arguments for the pattern chosen by the planner
    pub arguments: Vec<Expr>,
    /// mapping from index of [ast::Expr::Column] to the column index of IndexMethod response
    pub covered_columns: HashMap<usize, usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub func: AggFunc,
    pub args: Vec<ast::Expr>,
    pub original_expr: ast::Expr,
    pub distinctness: Distinctness,
}

impl Aggregate {
    pub fn new(func: AggFunc, args: &[Box<Expr>], expr: &Expr, distinctness: Distinctness) -> Self {
        Aggregate {
            func,
            args: args.iter().map(|arg| *arg.clone()).collect(),
            original_expr: expr.clone(),
            distinctness,
        }
    }

    pub fn is_distinct(&self) -> bool {
        self.distinctness.is_distinct()
    }
}

/// Represents the window definition and all window functions associated with a single SELECT.
#[derive(Debug, Clone)]
pub struct Window {
    /// The window name, either provided in the original statement or synthetically generated by
    /// the planner. This is optional because it can be assigned at different stages of query
    /// processing, but it should eventually always be set.
    pub name: Option<String>,
    /// Expressions from the PARTITION BY clause.
    pub partition_by: Vec<Expr>,
    /// The number of unique expressions in the PARTITION BY clause. This determines how many of
    /// the leftmost columns in the subquery output make up the partition key.
    pub deduplicated_partition_by_len: Option<usize>,
    /// Expressions from the ORDER BY clause.
    pub order_by: Vec<(Expr, SortOrder)>,
    /// All window functions associated with this window.
    pub functions: Vec<WindowFunction>,
}

impl Window {
    const DEFAULT_SORT_ORDER: SortOrder = SortOrder::Asc;

    pub fn new(name: Option<String>, ast: &ast::Window) -> Result<Self> {
        if !Self::is_default_frame_spec(&ast.frame_clause) {
            crate::bail_parse_error!("Custom frame specifications are not supported yet");
        }

        Ok(Window {
            name,
            partition_by: ast.partition_by.iter().map(|arg| *arg.clone()).collect(),
            deduplicated_partition_by_len: None,
            order_by: ast
                .order_by
                .iter()
                .map(|col| {
                    (
                        *col.expr.clone(),
                        col.order.unwrap_or(Self::DEFAULT_SORT_ORDER),
                    )
                })
                .collect(),
            functions: vec![],
        })
    }

    pub fn is_equivalent(&self, ast: &ast::Window) -> bool {
        if !Self::is_default_frame_spec(&ast.frame_clause) {
            return false;
        }

        if self.partition_by.len() != ast.partition_by.len() {
            return false;
        }
        if !self
            .partition_by
            .iter()
            .zip(&ast.partition_by)
            .all(|(a, b)| exprs_are_equivalent(a, b))
        {
            return false;
        }

        if self.order_by.len() != ast.order_by.len() {
            return false;
        }
        self.order_by
            .iter()
            .zip(&ast.order_by)
            .all(|((expr_a, order_a), col_b)| {
                exprs_are_equivalent(expr_a, &col_b.expr)
                    && *order_a == col_b.order.unwrap_or(Self::DEFAULT_SORT_ORDER)
            })
    }

    fn is_default_frame_spec(frame: &Option<FrameClause>) -> bool {
        if let Some(frame_clause) = frame {
            let FrameClause {
                mode,
                start,
                end,
                exclude,
            } = frame_clause;
            if *mode != FrameMode::Range {
                return false;
            }
            if *start != FrameBound::UnboundedPreceding {
                return false;
            }
            if *end != Some(FrameBound::CurrentRow) {
                return false;
            }
            if let Some(exclude) = exclude {
                if *exclude != FrameExclude::NoOthers {
                    return false;
                }
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct WindowFunction {
    /// The resolved function. Currently, only regular aggregate functions are supported.
    /// In the future, more specialized window functions such as `RANK()`, `ROW_NUMBER()`, etc. will be added.
    pub func: AggFunc,
    /// The expression from which the function was resolved.
    pub original_expr: Expr,
}

#[derive(Debug, Clone)]
pub enum SubqueryState {
    /// The subquery has not been evaluated yet.
    /// The 'plan' field is only optional because it is .take()'d when the the subquery
    /// is translated into bytecode.
    Unevaluated { plan: Option<Box<SelectPlan>> },
    /// The subquery has been evaluated.
    /// The [evaluated_at] field contains the loop index where the subquery was evaluated.
    /// The query plan struct no longer exists because translating the plan currently
    /// requires an ownership transfer. We retain the outer table references so
    /// later masking/evaluation logic can still reason about dependencies.
    Evaluated {
        /// Join-loop position where the subquery was emitted into bytecode.
        evaluated_at: EvalAt,
        /// Outer table ids referenced by the subquery when it was planned.
        /// We keep these so later analysis can still understand dependencies
        /// even after the plan is consumed.
        outer_ref_ids: Vec<TableInternalId>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryPosition {
    ResultColumn,
    Where,
    GroupBy,
    Having,
    OrderBy,
    LimitOffset,
}

impl SubqueryPosition {
    /// Returns true if a subquery in this position of the SELECT can be correlated, i.e. if it can reference columns from the outer query.
    /// FIXME: HAVING and ORDER BY should allow correlated subqueries, but our translation system currently does not support this well.
    /// Subqueries in these positions should be evaluated after the main loop, AND they should also have access to aggregations computed
    /// in the main query.
    pub fn allow_correlated(&self) -> bool {
        matches!(
            self,
            SubqueryPosition::ResultColumn | SubqueryPosition::Where | SubqueryPosition::GroupBy
        )
    }

    pub fn name(&self) -> &'static str {
        match self {
            SubqueryPosition::ResultColumn => "SELECT list",
            SubqueryPosition::Where => "WHERE",
            SubqueryPosition::GroupBy => "GROUP BY",
            SubqueryPosition::Having => "HAVING",
            SubqueryPosition::OrderBy => "ORDER BY",
            SubqueryPosition::LimitOffset => "LIMIT/OFFSET",
        }
    }
}

#[derive(Debug, Clone)]
/// A subquery that is not part of the `FROM` clause.
/// This is used for subqueries in the WHERE clause, HAVING clause, ORDER BY clause, LIMIT clause, OFFSET clause, etc.
/// Currently only subqueries in the WHERE clause are supported.
pub struct NonFromClauseSubquery {
    pub internal_id: TableInternalId,
    pub query_type: SubqueryType,
    pub state: SubqueryState,
    pub correlated: bool,
}

impl NonFromClauseSubquery {
    /// Returns true if the subquery has been evaluated (translated into bytecode).
    pub fn has_been_evaluated(&self) -> bool {
        matches!(self.state, SubqueryState::Evaluated { .. })
    }

    /// Returns the loop index where the subquery should be evaluated in this join order.
    ///
    /// If the subquery references tables from the parent query, it is evaluated at
    /// the right-most loop that makes those tables available. For hash joins, this
    /// may map a build-table reference to the probe loop where its rows are produced.
    pub fn get_eval_at(
        &self,
        join_order: &[JoinOrderMember],
        table_references: Option<&TableReferences>,
    ) -> Result<EvalAt> {
        let plan = match &self.state {
            SubqueryState::Unevaluated { plan } => plan.as_ref().unwrap(),
            SubqueryState::Evaluated { evaluated_at, .. } => {
                return Ok(*evaluated_at);
            }
        };
        eval_at_for_select_plan(plan, join_order, table_references)
    }

    /// Consumes the plan and returns it, and sets the subquery to the evaluated state.
    ///
    /// This captures any outer references before the plan is moved so later
    /// phases can still reason about dependencies.
    pub fn consume_plan(&mut self, evaluated_at: EvalAt) -> Box<SelectPlan> {
        match &mut self.state {
            SubqueryState::Unevaluated { plan } => {
                let outer_ref_ids = plan
                    .as_ref()
                    .map(|plan| {
                        plan.table_references
                            .outer_query_refs()
                            .iter()
                            .filter(|t| t.is_used())
                            .map(|t| t.internal_id)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let plan = plan.take().unwrap();
                self.state = SubqueryState::Evaluated {
                    evaluated_at,
                    outer_ref_ids,
                };
                plan
            }
            SubqueryState::Evaluated { .. } => {
                panic!("subquery has already been evaluated");
            }
        }
    }
}

/// Determine the earliest evaluation point for a nested plan by walking all SELECT components.
fn eval_at_for_plan(
    plan: &Plan,
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    match plan {
        Plan::Select(select_plan) => {
            eval_at_for_select_plan(select_plan, join_order, table_references)
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut eval_at = EvalAt::BeforeLoop;
            for (select_plan, _) in left.iter() {
                eval_at = eval_at.max(eval_at_for_select_plan(
                    select_plan,
                    join_order,
                    table_references,
                )?);
            }
            eval_at = eval_at.max(eval_at_for_select_plan(
                right_most,
                join_order,
                table_references,
            )?);
            Ok(eval_at)
        }
        Plan::Delete(_) | Plan::Update(_) => Ok(EvalAt::BeforeLoop),
    }
}

/// Returns true if a plan (including compound SELECTs) references outer-scope tables.
pub fn plan_is_correlated(plan: &Plan) -> bool {
    match plan {
        Plan::Select(select_plan) => select_plan.is_correlated(),
        Plan::CompoundSelect {
            left, right_most, ..
        } => left.iter().any(|(plan, _)| plan.is_correlated()) || right_most.is_correlated(),
        Plan::Delete(_) | Plan::Update(_) => false,
    }
}

/// Determine when a SELECT plan can be evaluated, including nested non-FROM and FROM-clause subqueries.
fn eval_at_for_select_plan(
    plan: &SelectPlan,
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    let mut eval_at = EvalAt::BeforeLoop;
    let used_outer_refs = plan
        .table_references
        .outer_query_refs()
        .iter()
        .filter(|t| t.is_used());

    for outer_ref in used_outer_refs {
        if let Some(loop_idx) =
            resolve_outer_ref_loop(outer_ref.internal_id, join_order, table_references)
        {
            eval_at = eval_at.max(EvalAt::Loop(loop_idx));
        }
    }
    for subquery in plan.non_from_clause_subqueries.iter() {
        let eval_at_inner = subquery.get_eval_at(join_order, table_references)?;
        eval_at = eval_at.max(eval_at_inner);
    }
    for joined_table in plan.table_references.joined_tables().iter() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &joined_table.table {
            eval_at = eval_at.max(eval_at_for_plan(
                from_clause_subquery.plan.as_ref(),
                join_order,
                table_references,
            )?);
        }
    }
    Ok(eval_at)
}

/// Resolves the loop index for an outer-table reference.
///
/// If the table is not present in the join order, we look for a hash join
/// where that table is the build side and map it to the probe loop.
fn resolve_outer_ref_loop(
    table_id: TableInternalId,
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
) -> Option<usize> {
    if let Some(loop_idx) = join_order.iter().position(|t| t.table_id == table_id) {
        return Some(loop_idx);
    }
    let tables = table_references?;
    for (probe_idx, member) in join_order.iter().enumerate() {
        let probe_table = &tables.joined_tables()[member.original_idx];
        if let Operation::HashJoin(ref hj) = probe_table.op {
            let build_table = &tables.joined_tables()[hj.build_table_idx];
            if build_table.internal_id == table_id {
                return Some(probe_idx);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    #[test]
    fn test_column_used_mask_empty() {
        let mask = ColumnUsedMask::default();
        assert!(mask.is_empty());

        let mut mask2 = ColumnUsedMask::default();
        mask2.set(0);
        assert!(!mask2.is_empty());
    }

    #[test]
    fn test_column_used_mask_set_and_get() {
        let mut mask = ColumnUsedMask::default();

        let max_columns = 10000;
        let mut set_indices = Vec::new();
        let mut rng = ChaCha8Rng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        for i in 0..max_columns {
            if rng.next_u32() % 3 == 0 {
                set_indices.push(i);
                mask.set(i);
            }
        }

        // Verify set bits are present
        for &i in &set_indices {
            assert!(mask.get(i), "Expected bit {i} to be set");
        }

        // Verify unset bits are not present
        for i in 0..max_columns {
            if !set_indices.contains(&i) {
                assert!(!mask.get(i), "Expected bit {i} to not be set");
            }
        }
    }

    #[test]
    fn test_column_used_mask_subset_relationship() {
        let mut full_mask = ColumnUsedMask::default();
        let mut subset_mask = ColumnUsedMask::default();

        let max_columns = 5000;
        let mut rng = ChaCha8Rng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        // Create a pattern where subset has fewer bits
        for i in 0..max_columns {
            if rng.next_u32() % 5 == 0 {
                full_mask.set(i);
                if i % 2 == 0 {
                    subset_mask.set(i);
                }
            }
        }

        // full_mask contains all bits of subset_mask
        assert!(full_mask.contains_all_set_bits_of(&subset_mask));

        // subset_mask does not contain all bits of full_mask
        assert!(!subset_mask.contains_all_set_bits_of(&full_mask));

        // A mask contains itself
        assert!(full_mask.contains_all_set_bits_of(&full_mask));
        assert!(subset_mask.contains_all_set_bits_of(&subset_mask));
    }

    #[test]
    fn test_column_used_mask_empty_subset() {
        let mut mask = ColumnUsedMask::default();
        for i in (0..1000).step_by(7) {
            mask.set(i);
        }

        let empty_mask = ColumnUsedMask::default();

        // Empty mask is subset of everything
        assert!(mask.contains_all_set_bits_of(&empty_mask));
        assert!(empty_mask.contains_all_set_bits_of(&empty_mask));
    }

    #[test]
    fn test_column_used_mask_sparse_indices() {
        let mut sparse_mask = ColumnUsedMask::default();

        // Test with very sparse, large indices
        let sparse_indices = vec![0, 137, 1042, 5389, 10000, 50000, 100000, 500000, 1000000];

        for &idx in &sparse_indices {
            sparse_mask.set(idx);
        }

        for &idx in &sparse_indices {
            assert!(sparse_mask.get(idx), "Expected bit {idx} to be set");
        }

        // Check some indices that shouldn't be set
        let unset_indices = vec![1, 100, 1000, 5000, 25000, 75000, 250000, 750000];
        for &idx in &unset_indices {
            assert!(!sparse_mask.get(idx), "Expected bit {idx} to not be set");
        }

        assert!(!sparse_mask.is_empty());
    }

    #[test]
    fn test_column_used_mask_clear() {
        let mut mask = ColumnUsedMask::default();

        // Test inline clear
        mask.set(5);
        mask.set(10);
        assert!(mask.get(5));
        mask.clear(5);
        assert!(!mask.get(5));
        assert!(mask.get(10));

        // Test overflow clear
        mask.set(100);
        mask.set(200);
        assert!(mask.get(100));
        mask.clear(100);
        assert!(!mask.get(100));
        assert!(mask.get(200));

        // Clear non-existent bit should be no-op
        mask.clear(999);
        assert!(!mask.get(999));
    }

    #[test]
    fn test_column_used_mask_is_only() {
        // Test inline is_only
        let mut mask = ColumnUsedMask::default();
        mask.set(5);
        assert!(mask.is_only(5));
        assert!(!mask.is_only(0));
        assert!(!mask.is_only(100));

        mask.set(10);
        assert!(!mask.is_only(5));
        assert!(!mask.is_only(10));

        // Test overflow is_only
        let mut mask2 = ColumnUsedMask::default();
        mask2.set(100);
        assert!(mask2.is_only(100));
        assert!(!mask2.is_only(0));
        assert!(!mask2.is_only(50));

        mask2.set(200);
        assert!(!mask2.is_only(100));

        // Test empty mask
        let empty = ColumnUsedMask::default();
        assert!(!empty.is_only(0));
        assert!(!empty.is_only(100));
    }

    #[test]
    fn test_column_used_mask_subtract() {
        let mut mask1 = ColumnUsedMask::default();
        let mut mask2 = ColumnUsedMask::default();

        // Set up mask1 with inline and overflow bits
        for i in [1, 5, 10, 63, 64, 100, 200] {
            mask1.set(i);
        }

        // Set up mask2 with some overlapping bits
        for i in [5, 10, 100] {
            mask2.set(i);
        }

        mask1.subtract(&mask2);

        // Should remain
        assert!(mask1.get(1));
        assert!(mask1.get(63));
        assert!(mask1.get(64));
        assert!(mask1.get(200));

        // Should be cleared
        assert!(!mask1.get(5));
        assert!(!mask1.get(10));
        assert!(!mask1.get(100));
    }

    #[test]
    fn test_column_used_mask_iter() {
        let mut mask = ColumnUsedMask::default();
        let indices = vec![0, 5, 63, 64, 65, 127, 128, 200, 1000];

        for &i in &indices {
            mask.set(i);
        }

        let collected: Vec<usize> = mask.iter().collect();
        assert_eq!(collected, indices);

        // Empty mask iter
        let empty = ColumnUsedMask::default();
        assert_eq!(empty.iter().count(), 0);
    }

    #[test]
    fn test_column_used_mask_bitor_assign() {
        let mut mask1 = ColumnUsedMask::default();
        let mut mask2 = ColumnUsedMask::default();

        // Inline bits
        mask1.set(1);
        mask1.set(5);
        mask2.set(5);
        mask2.set(10);

        // Overflow bits
        mask1.set(100);
        mask2.set(200);

        mask1 |= &mask2;

        assert!(mask1.get(1));
        assert!(mask1.get(5));
        assert!(mask1.get(10));
        assert!(mask1.get(100));
        assert!(mask1.get(200));

        // mask2 should be unchanged
        assert!(!mask2.get(1));
        assert!(mask2.get(5));
        assert!(mask2.get(10));
        assert!(!mask2.get(100));
        assert!(mask2.get(200));
    }

    #[test]
    fn test_column_used_mask_boundary_conditions() {
        let mut mask = ColumnUsedMask::default();

        // Test at inline/overflow boundary
        mask.set(63); // last inline bit
        mask.set(64); // first overflow bit

        assert!(mask.get(63));
        assert!(mask.get(64));
        assert!(!mask.get(62));
        assert!(!mask.get(65));

        // Test is_only at boundary
        let mut mask2 = ColumnUsedMask::default();
        mask2.set(63);
        assert!(mask2.is_only(63));

        let mut mask3 = ColumnUsedMask::default();
        mask3.set(64);
        assert!(mask3.is_only(64));
    }

    fn rng_from_env_or_time() -> (ChaCha8Rng, u64) {
        let seed = std::env::var("TEST_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        (ChaCha8Rng::seed_from_u64(seed), seed)
    }

    /// Reference implementation using BTreeSet for correctness comparison
    struct ReferenceMask(std::collections::BTreeSet<usize>);

    impl ReferenceMask {
        fn new() -> Self {
            Self(std::collections::BTreeSet::new())
        }
        fn set(&mut self, index: usize) {
            self.0.insert(index);
        }
        fn get(&self, index: usize) -> bool {
            self.0.contains(&index)
        }
        fn clear(&mut self, index: usize) {
            self.0.remove(&index);
        }
        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
        fn is_only(&self, index: usize) -> bool {
            self.0.len() == 1 && self.0.contains(&index)
        }
        fn contains_all_set_bits_of(&self, other: &Self) -> bool {
            other.0.is_subset(&self.0)
        }
        fn subtract(&mut self, other: &Self) {
            for &idx in &other.0 {
                self.0.remove(&idx);
            }
        }
        fn bitor_assign(&mut self, other: &Self) {
            for &idx in &other.0 {
                self.0.insert(idx);
            }
        }
    }

    #[test]
    fn test_column_used_mask_fuzz() {
        let (mut rng, seed) = rng_from_env_or_time();
        eprintln!("test_column_used_mask_random_ops seed: {seed}");

        let mut mask = ColumnUsedMask::default();
        let mut reference = ReferenceMask::new();

        let num_ops = 100000;
        let max_index = 4096;

        for _ in 0..num_ops {
            let op = rng.next_u32() % 10;
            let idx = (rng.next_u32() % max_index) as usize;

            match op {
                0..=2 => {
                    // Set (more frequent)
                    mask.set(idx);
                    reference.set(idx);
                }
                3 => {
                    // Get
                    assert_eq!(
                        mask.get(idx),
                        reference.get(idx),
                        "get({idx}) mismatch, seed={seed}"
                    );
                }
                4 => {
                    // Clear
                    mask.clear(idx);
                    reference.clear(idx);
                }
                5 => {
                    // IsEmpty
                    assert_eq!(
                        mask.is_empty(),
                        reference.is_empty(),
                        "is_empty mismatch, seed={seed}"
                    );
                }
                6 => {
                    // IsOnly
                    assert_eq!(
                        mask.is_only(idx),
                        reference.is_only(idx),
                        "is_only({idx}) mismatch, seed={seed}"
                    );
                }
                7 => {
                    // ContainsAllSetBitsOf with random other mask
                    let mut other_mask = ColumnUsedMask::default();
                    let mut other_ref = ReferenceMask::new();
                    for _ in 0..(rng.next_u32() % 20) {
                        let other_idx = (rng.next_u32() % max_index) as usize;
                        other_mask.set(other_idx);
                        other_ref.set(other_idx);
                    }
                    assert_eq!(
                        mask.contains_all_set_bits_of(&other_mask),
                        reference.contains_all_set_bits_of(&other_ref),
                        "contains_all_set_bits_of mismatch, seed={seed}"
                    );
                }
                8 => {
                    // BitOrAssign with random other mask
                    let mut other_mask = ColumnUsedMask::default();
                    let mut other_ref = ReferenceMask::new();
                    for _ in 0..(rng.next_u32() % 20) {
                        let other_idx = (rng.next_u32() % max_index) as usize;
                        other_mask.set(other_idx);
                        other_ref.set(other_idx);
                    }
                    mask |= &other_mask;
                    reference.bitor_assign(&other_ref);
                }
                9 => {
                    // Subtract with random other mask
                    let mut other_mask = ColumnUsedMask::default();
                    let mut other_ref = ReferenceMask::new();
                    for _ in 0..(rng.next_u32() % 20) {
                        let other_idx = (rng.next_u32() % max_index) as usize;
                        other_mask.set(other_idx);
                        other_ref.set(other_idx);
                    }
                    mask.subtract(&other_mask);
                    reference.subtract(&other_ref);
                }
                _ => unreachable!(),
            }
        }

        // Final verification: iter should produce same results
        let mask_set: std::collections::BTreeSet<usize> = mask.iter().collect();
        assert_eq!(mask_set, reference.0, "final iter mismatch, seed={seed}");
    }
}
