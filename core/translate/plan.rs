use std::{cmp::Ordering, collections::HashMap, sync::Arc};
use turso_parser::ast::{
    self, FrameBound, FrameClause, FrameExclude, FrameMode, SortOrder, SubqueryType,
};

use crate::{
    function::AggFunc,
    schema::{BTreeTable, Column, FromClauseSubquery, Index, Schema, Table},
    translate::{
        collate::get_collseq_from_expr, emitter::UpdateRowSource,
        optimizer::constraints::SeekRangeConstraint,
    },
    vdbe::{
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{IdxInsertFlags, Insn},
        BranchOffset, CursorID,
    },
    Result, VirtualTable,
};
use crate::{schema::Type, types::SeekOp};

use turso_parser::ast::TableInternalId;

use super::{emitter::OperationMode, planner::determine_where_to_eval_term};

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
                let joined_table_ref = tables.find_joined_table_by_internal_id(*table).unwrap();
                if let Operation::IndexMethodQuery(module) = &joined_table_ref.op {
                    if module.covered_columns.contains_key(column) {
                        return None;
                    }
                }
                let table_ref = &joined_table_ref.table;
                table_ref.get_column_at(*column).unwrap().name.as_deref()
            }
            ast::Expr::RowId { table, .. } => {
                // If there is a rowid alias column, use its name
                let (_, table_ref) = tables.find_table_by_internal_id(*table).unwrap();
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
    ) -> bool {
        if self.consumed {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order, subqueries) else {
            return false;
        };
        eval_at == EvalAt::BeforeLoop
    }

    pub fn should_eval_at_loop(
        &self,
        loop_idx: usize,
        join_order: &[JoinOrderMember],
        subqueries: &[NonFromClauseSubquery],
    ) -> bool {
        if self.consumed {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order, subqueries) else {
            return false;
        };
        eval_at == EvalAt::Loop(loop_idx)
    }

    fn eval_at(
        &self,
        join_order: &[JoinOrderMember],
        subqueries: &[NonFromClauseSubquery],
    ) -> Result<EvalAt> {
        determine_where_to_eval_term(self, join_order, subqueries)
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

/// A query plan is either a SELECT or a DELETE (for now)
#[derive(Debug, Clone)]
pub enum Plan {
    Select(SelectPlan),
    CompoundSelect {
        left: Vec<(SelectPlan, ast::CompoundOperator)>,
        right_most: SelectPlan,
        limit: Option<Box<Expr>>,
        offset: Option<Box<Expr>>,
        order_by: Option<Vec<(ast::Expr, SortOrder)>>,
    },
    Delete(DeletePlan),
    Update(UpdatePlan),
}

/// The destination of the results of a query.
/// Typically, the results of a query are returned to the caller.
/// However, there are some cases where the results are not returned to the caller,
/// but rather are yielded to a parent query via coroutine, or stored in a temp table,
/// later used by the parent query.
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
    /// The cursor ID for the ephemeral index opened for the purpose of deduplicating results.
    pub cursor_id: usize,
    /// The index name for the ephemeral index, needed to lookup the cursor ID.
    pub ephemeral_index_name: String,
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
        program.emit_insn(Insn::Found {
            cursor_id: self.cursor_id,
            target_pc: self.label_on_conflict,
            record_reg: start_reg,
            num_regs,
        });
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg,
            count: num_regs,
            dest_reg: record_reg,
            index_name: Some(self.ephemeral_index_name.to_string()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: self.cursor_id,
            record_reg,
            unpacked_start: None,
            unpacked_count: None,
            flags: IdxInsertFlags::new(),
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
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table_references: TableReferences,
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
}

pub fn select_star(tables: &[JoinedTable], out_columns: &mut Vec<ResultSetColumn>) {
    for table in tables.iter() {
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
        !self.col_used_mask.is_empty()
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
        }
    }
    pub fn new_empty() -> Self {
        Self {
            joined_tables: Vec::new(),
            outer_query_refs: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.joined_tables.is_empty() && self.outer_query_refs.is_empty()
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
                    .find(|t| t.identifier == identifier)
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

    pub fn contains_table(&self, table: &Table) -> bool {
        self.joined_tables
            .iter()
            .map(|t| &t.table)
            .chain(self.outer_query_refs.iter().map(|t| &t.table))
            .any(|t| match t {
                Table::FromClauseSubquery(subquery_table) => {
                    subquery_table.plan.table_references.contains_table(table)
                }
                _ => t == table,
            })
    }

    pub fn extend(&mut self, other: TableReferences) {
        self.joined_tables.extend(other.joined_tables);
        self.outer_query_refs.extend(other.outer_query_refs);
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
#[repr(transparent)]
pub struct ColumnUsedMask(roaring::RoaringBitmap);

impl ColumnUsedMask {
    pub fn set(&mut self, index: usize) {
        self.0.insert(index as u32);
    }

    pub fn get(&self, index: usize) -> bool {
        self.0.contains(index as u32)
    }

    pub fn contains_all_set_bits_of(&self, other: &Self) -> bool {
        other.0.is_subset(&self.0)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::ops::BitOrAssign<&Self> for ColumnUsedMask {
    fn bitor_assign(&mut self, rhs: &Self) {
        self.0 |= &rhs.0;
    }
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

    /// Creates a new TableReference for a subquery.
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
                Column::new(
                    rc.name(&plan.table_references).map(String::from),
                    "BLOB".to_string(),
                    None,
                    Type::Blob, // FIXME: infer proper type
                    None,
                    false,
                    false,
                    false,
                    false,
                    false,
                )
            })
            .collect::<Vec<_>>();

        for (i, column) in columns.iter_mut().enumerate() {
            column.set_collation(get_collseq_from_expr(
                &plan.result_columns[i].expr,
                &plan.table_references,
            )?);
        }

        let table = Table::FromClauseSubquery(FromClauseSubquery {
            name: identifier.clone(),
            plan: Box::new(plan),
            columns,
            result_columns_start_reg: None,
        });
        Ok(Self {
            op: Operation::default_scan_for(&table),
            table,
            identifier,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
            database_id: 0,
        })
    }

    pub fn columns(&self) -> &[Column] {
        self.table.columns()
    }

    /// Mark a column as used in the query.
    /// This is used to determine whether a covering index can be used.
    pub fn mark_column_used(&mut self, index: usize) {
        self.col_used_mask.set(index);
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
                    Some(
                        program
                            .alloc_cursor_id_keyed(CursorKey::table(self.internal_id), cursor_type),
                    )
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
        let mut index_cols_mask = ColumnUsedMask::default();
        for col in index.columns.iter() {
            index_cols_mask.set(col.pos_in_table);
        }

        // If a table has a rowid (i.e. is not a WITHOUT ROWID table), the index is guaranteed to contain the rowid as well.
        if btree.has_rowid {
            if let Some(pos_of_rowid_alias_col) = btree.get_rowid_alias_column().map(|(pos, _)| pos)
            {
                let mut empty_mask = ColumnUsedMask::default();
                empty_mask.set(pos_of_rowid_alias_col);
                if self.col_used_mask == empty_mask {
                    // However if the index would be ONLY used for the rowid, then let's not bother using it to cover the query.
                    // Example: if the query is SELECT id FROM t, and id is a rowid alias, then let's rather just scan the table
                    // instead of an index.
                    return false;
                }
                index_cols_mask.set(pos_of_rowid_alias_col);
            }
        }

        index_cols_mask.contains_all_set_bits_of(&self.col_used_mask)
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

pub struct SeekDefKeyIterator<'a> {
    seek_def: &'a SeekDef,
    seek_key: &'a SeekKey,
    pos: usize,
}

impl<'a> Iterator for SeekDefKeyIterator<'a> {
    type Item = SeekKeyComponent<&'a ast::Expr>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.pos < self.seek_def.prefix.len() {
            Some(SeekKeyComponent::Expr(
                &self.seek_def.prefix[self.pos].eq.as_ref().unwrap().1,
            ))
        } else if self.pos == self.seek_def.prefix.len() {
            match &self.seek_key.last_component {
                SeekKeyComponent::Expr(expr) => Some(SeekKeyComponent::Expr(expr)),
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
                SeekKeyComponent::None => 0,
            }
    }
    /// iterate over value expressions in the given seek key
    pub fn iter<'a>(&'a self, key: &'a SeekKey) -> SeekDefKeyIterator<'a> {
        SeekDefKeyIterator {
            seek_def: self,
            seek_key: key,
            pos: 0,
        }
    }
}

/// [SeekKeyComponent] enum represents optional last_component of the [SeekKey]
///
/// This component represented by separate enum instead of Option<E> because before there were third Sentinel value
/// For now - we don't need this and it's enough to just either use some user-provided expression or omit last component of the key completely
/// But as separate enum is almost never a harm - I decided to keep it here.
///
/// This enum accepts generic argument E in order to use both SeekKeyComponent<ast::Expr> and SeekKeyComponent<&ast::Expr>
#[derive(Debug, Clone)]
pub enum SeekKeyComponent<E> {
    Expr(E),
    None,
}

/// A condition to use when seeking.
#[derive(Debug, Clone)]
pub struct SeekKey {
    /// Complete key must be constructed from common [SeekDef::prefix] and optional last_component
    pub last_component: SeekKeyComponent<ast::Expr>,

    /// The comparison operator to use when seeking.
    pub op: SeekOp,
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
    /// A scan of a subquery in the `FROM` clause.
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
    /// requires an ownership transfer.
    Evaluated { evaluated_at: EvalAt },
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

    /// Returns the loop index where the subquery should be evaluated in this particular join order.
    /// If the subquery references tables from the parent query, it will be evaluated at the right-most
    /// nested loop whose table it references.
    pub fn get_eval_at(&self, join_order: &[JoinOrderMember]) -> Result<EvalAt> {
        let mut eval_at = EvalAt::BeforeLoop;
        let SubqueryState::Unevaluated { plan } = &self.state else {
            crate::bail_parse_error!("subquery has already been evaluated");
        };
        let used_outer_refs = plan
            .as_ref()
            .unwrap()
            .table_references
            .outer_query_refs()
            .iter()
            .filter(|t| t.is_used());

        for outer_ref in used_outer_refs {
            let Some(loop_idx) = join_order
                .iter()
                .position(|t| t.table_id == outer_ref.internal_id)
            else {
                continue;
            };
            eval_at = eval_at.max(EvalAt::Loop(loop_idx));
        }
        for subquery in plan.as_ref().unwrap().non_from_clause_subqueries.iter() {
            let eval_at_inner = subquery.get_eval_at(join_order)?;
            eval_at = eval_at.max(eval_at_inner);
        }
        Ok(eval_at)
    }

    /// Consumes the plan and returns it, and sets the subquery to the evaluated state.
    /// This is used when the subquery is translated into bytecode.
    pub fn consume_plan(&mut self, evaluated_at: EvalAt) -> Box<SelectPlan> {
        match &mut self.state {
            SubqueryState::Unevaluated { plan } => {
                let plan = plan.take().unwrap();
                self.state = SubqueryState::Evaluated { evaluated_at };
                plan
            }
            SubqueryState::Evaluated { .. } => {
                panic!("subquery has already been evaluated");
            }
        }
    }
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
}
