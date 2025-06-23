use std::{cell::Cell, cmp::Ordering, rc::Rc, sync::Arc};
use turso_ext::{ConstraintInfo, ConstraintOp};
use turso_sqlite3_parser::ast::{self, SortOrder};

use crate::{
    function::AggFunc,
    schema::{BTreeTable, Column, FromClauseSubquery, Index, Table},
    vdbe::{
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{IdxInsertFlags, Insn},
        BranchOffset, CursorID,
    },
    Result, VirtualTable,
};
use crate::{schema::Type, types::SeekOp, util::can_pushdown_predicate};

use turso_sqlite3_parser::ast::TableInternalId;

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
                let table_ref = tables.find_table_by_internal_id(*table).unwrap();
                table_ref.get_column_at(*column).unwrap().name.as_deref()
            }
            ast::Expr::RowId { table, .. } => {
                // If there is a rowid alias column, use its name
                let table_ref = tables.find_table_by_internal_id(*table).unwrap();
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
    /// Is this condition originally from an OUTER JOIN, and if so, what is the internal ID of the [TableReference] that it came from?
    /// The ID is always the right-hand-side table of the OUTER JOIN.
    /// If `from_outer_join` is Some, we need to evaluate this term at the loop of the the corresponding table,
    /// regardless of which tables it references.
    /// We also cannot e.g. short circuit the entire query in the optimizer if the condition is statically false.
    pub from_outer_join: Option<TableInternalId>,
    /// Whether the condition has been consumed by the optimizer in some way, and it should not be evaluated
    /// in the normal place where WHERE terms are evaluated.
    /// A term may have been consumed e.g. if:
    /// - it has been converted into a constraint in a seek key
    /// - it has been removed due to being trivially true or false
    ///
    /// FIXME: this can be made into a simple `bool` once we move the virtual table constraint resolution
    /// code out of `init_loop()`, because that's the only place that requires a mutable reference to the where clause
    /// that causes problems to other code that needs immutable references to the where clause.
    pub consumed: Cell<bool>,
}

impl WhereTerm {
    pub fn should_eval_before_loop(&self, join_order: &[JoinOrderMember]) -> bool {
        if self.consumed.get() {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order) else {
            return false;
        };
        eval_at == EvalAt::BeforeLoop
    }

    pub fn should_eval_at_loop(&self, loop_idx: usize, join_order: &[JoinOrderMember]) -> bool {
        if self.consumed.get() {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order) else {
            return false;
        };
        eval_at == EvalAt::Loop(loop_idx)
    }

    fn eval_at(&self, join_order: &[JoinOrderMember]) -> Result<EvalAt> {
        determine_where_to_eval_term(self, join_order)
    }
}

use crate::ast::{Expr, Operator};

// This function takes an operator and returns the operator you would obtain if the operands were swapped.
// e.g. "literal < column"
// which is not the canonical order for constraint pushdown.
// This function will return > so that the expression can be treated as if it were written "column > literal"
fn reverse_operator(op: &Operator) -> Option<Operator> {
    match op {
        Operator::Equals => Some(Operator::Equals),
        Operator::Less => Some(Operator::Greater),
        Operator::LessEquals => Some(Operator::GreaterEquals),
        Operator::Greater => Some(Operator::Less),
        Operator::GreaterEquals => Some(Operator::LessEquals),
        Operator::NotEquals => Some(Operator::NotEquals),
        Operator::Is => Some(Operator::Is),
        Operator::IsNot => Some(Operator::IsNot),
        _ => None,
    }
}

fn to_ext_constraint_op(op: &Operator) -> Option<ConstraintOp> {
    match op {
        Operator::Equals => Some(ConstraintOp::Eq),
        Operator::Less => Some(ConstraintOp::Lt),
        Operator::LessEquals => Some(ConstraintOp::Le),
        Operator::Greater => Some(ConstraintOp::Gt),
        Operator::GreaterEquals => Some(ConstraintOp::Ge),
        Operator::NotEquals => Some(ConstraintOp::Ne),
        _ => None,
    }
}

/// This function takes a WhereTerm for a select involving a VTab at index 'table_index'.
/// It determines whether or not it involves the given table and whether or not it can
/// be converted into a ConstraintInfo which can be passed to the vtab module's xBestIndex
/// method, which will possibly calculate some information to improve the query plan, that we can send
/// back to it as arguments for the VFilter operation.
/// is going to be filtered against: e.g:
/// 'SELECT key, value FROM vtab WHERE key = 'some_key';
/// we need to send the Value('some_key') as an argument to VFilter, and possibly omit it from
/// the filtration in the vdbe layer.
pub fn convert_where_to_vtab_constraint(
    term: &WhereTerm,
    table_idx: usize,
    pred_idx: usize,
    join_order: &[JoinOrderMember],
) -> Result<Option<ConstraintInfo>> {
    if term.from_outer_join.is_some() {
        return Ok(None);
    }
    let Expr::Binary(lhs, op, rhs) = &term.expr else {
        return Ok(None);
    };
    let expr_is_ready =
        |e: &Expr| -> Result<bool> { can_pushdown_predicate(e, table_idx, join_order) };
    let (vcol_idx, op_for_vtab, usable, is_rhs) = match (&**lhs, &**rhs) {
        (
            Expr::Column {
                table: tbl_l,
                column: col_l,
                ..
            },
            Expr::Column {
                table: tbl_r,
                column: col_r,
                ..
            },
        ) => {
            // one side must be the virtual table
            let tbl_l_idx = join_order
                .iter()
                .position(|j| j.table_id == *tbl_l)
                .unwrap();
            let tbl_r_idx = join_order
                .iter()
                .position(|j| j.table_id == *tbl_r)
                .unwrap();
            let vtab_on_l = tbl_l_idx == table_idx;
            let vtab_on_r = tbl_r_idx == table_idx;
            if vtab_on_l == vtab_on_r {
                return Ok(None); // either both or none -> not convertible
            }

            if vtab_on_l {
                // vtab on left side: operator unchanged
                let usable = tbl_r_idx < table_idx; // usable if the other table is already positioned
                (col_l, op, usable, false)
            } else {
                // vtab on right side of the expr: reverse operator
                let usable = tbl_l_idx < table_idx;
                (col_r, &reverse_operator(op).unwrap_or(*op), usable, true)
            }
        }
        (Expr::Column { table, column, .. }, other)
            if join_order
                .iter()
                .position(|j| j.table_id == *table)
                .unwrap()
                == table_idx =>
        {
            (
                column,
                op,
                expr_is_ready(other)?, // literal / earlier‑table / deterministic func ?
                false,
            )
        }
        (other, Expr::Column { table, column, .. })
            if join_order
                .iter()
                .position(|j| j.table_id == *table)
                .unwrap()
                == table_idx =>
        {
            (
                column,
                &reverse_operator(op).unwrap_or(*op),
                expr_is_ready(other)?,
                true,
            )
        }

        _ => return Ok(None), // does not involve the virtual table at all
    };

    let Some(op) = to_ext_constraint_op(op_for_vtab) else {
        return Ok(None);
    };

    Ok(Some(ConstraintInfo {
        column_index: *vcol_idx as u32,
        op,
        usable,
        plan_info: ConstraintInfo::pack_plan_info(pred_idx as u32, is_rhs),
    }))
}
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
        limit: Option<isize>,
        offset: Option<isize>,
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
        table: Rc<BTreeTable>,
    },
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
    pub order_by: Option<Vec<(ast::Expr, SortOrder)>>,
    /// all the aggregates collected from the result columns, order by, and (TODO) having clauses
    pub aggregates: Vec<Aggregate>,
    /// limit clause
    pub limit: Option<isize>,
    /// offset clause
    pub offset: Option<isize>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
    /// the destination of the resulting rows from this plan.
    pub query_destination: QueryDestination,
    /// whether the query is DISTINCT
    pub distinctness: Distinctness,
    /// values: https://sqlite.org/syntax/select-core.html
    pub values: Vec<Vec<Expr>>,
}

impl SelectPlan {
    pub fn joined_tables(&self) -> &[JoinedTable] {
        self.table_references.joined_tables()
    }

    pub fn agg_args_count(&self) -> usize {
        self.aggregates.iter().map(|agg| agg.args.len()).sum()
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
            || self.table_references.outer_query_refs().is_empty()
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

        let count = turso_sqlite3_parser::ast::Expr::FunctionCall {
            name: turso_sqlite3_parser::ast::Id("count".to_string()),
            distinctness: None,
            args: None,
            order_by: None,
            filter_over: None,
        };
        let count_star = turso_sqlite3_parser::ast::Expr::FunctionCallStar {
            name: turso_sqlite3_parser::ast::Id("count".to_string()),
            filter_over: None,
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
    pub order_by: Option<Vec<(ast::Expr, SortOrder)>>,
    /// limit clause
    pub limit: Option<isize>,
    /// offset clause
    pub offset: Option<isize>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
    /// Indexes that must be updated by the delete operation.
    pub indexes: Vec<Arc<Index>>,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table_references: TableReferences,
    // (colum index, new value) pairs
    pub set_clauses: Vec<(usize, ast::Expr)>,
    pub where_clause: Vec<WhereTerm>,
    pub order_by: Option<Vec<(ast::Expr, SortOrder)>>,
    pub limit: Option<isize>,
    pub offset: Option<isize>,
    // TODO: optional RETURNING clause
    pub returning: Option<Vec<ResultSetColumn>>,
    // whether the WHERE clause is always false
    pub contains_constant_false_condition: bool,
    pub indexes_to_update: Vec<Arc<Index>>,
    // If the table's rowid alias is used, gather all the target rowids into an ephemeral table, and then use that table as the single JoinedTable for the actual UPDATE loop.
    pub ephemeral_plan: Option<SelectPlan>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
}

pub fn select_star(tables: &[JoinedTable], out_columns: &mut Vec<ResultSetColumn>) {
    for table in tables.iter() {
        let maybe_using_cols = table
            .join_info
            .as_ref()
            .and_then(|join_info| join_info.using.as_ref());
        out_columns.extend(
            table
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, col)| {
                    // If we are joining with USING, we need to deduplicate the columns from the right table
                    // that are also present in the USING clause.
                    if let Some(using_cols) = maybe_using_cols {
                        !using_cols.iter().any(|using_col| {
                            col.name
                                .as_ref()
                                .is_some_and(|name| name.eq_ignore_ascii_case(&using_col.0))
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
                        is_rowid_alias: col.is_rowid_alias,
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
    pub using: Option<ast::DistinctNames>,
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
    pub fn new(
        joined_tables: Vec<JoinedTable>,
        outer_query_refs: Vec<OuterQueryReference>,
    ) -> Self {
        Self {
            joined_tables,
            outer_query_refs,
        }
    }

    /// Add a new [JoinedTable] to the query plan.
    pub fn add_joined_table(&mut self, joined_table: JoinedTable) {
        self.joined_tables.push(joined_table);
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

    /// Returns an immutable reference to the [Table] with the given internal ID.
    pub fn find_table_by_internal_id(&self, internal_id: TableInternalId) -> Option<&Table> {
        self.joined_tables
            .iter()
            .find(|t| t.internal_id == internal_id)
            .map(|t| &t.table)
            .or_else(|| {
                self.outer_query_refs
                    .iter()
                    .find(|t| t.internal_id == internal_id)
                    .map(|t| &t.table)
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
        self.joined_tables.iter().any(|t| t.table == *table)
            || self.outer_query_refs.iter().any(|t| t.table == *table)
    }

    pub fn extend(&mut self, other: TableReferences) {
        self.joined_tables.extend(other.joined_tables);
        self.outer_query_refs.extend(other.outer_query_refs);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct ColumnUsedMask(u128);

impl ColumnUsedMask {
    pub fn set(&mut self, index: usize) {
        assert!(
            index < 128,
            "ColumnUsedMask only supports up to 128 columns"
        );
        self.0 |= 1 << index;
    }

    pub fn get(&self, index: usize) -> bool {
        assert!(
            index < 128,
            "ColumnUsedMask only supports up to 128 columns"
        );
        self.0 & (1 << index) != 0
    }

    pub fn contains_all_set_bits_of(&self, other: &Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

#[derive(Clone, Debug)]
pub enum Operation {
    // Scan operation
    // This operation is used to scan a table.
    // The iter_dir is used to indicate the direction of the iterator.
    Scan {
        iter_dir: IterationDirection,
        /// The index that we are using to scan the table, if any.
        index: Option<Arc<Index>>,
    },
    // Search operation
    // This operation is used to search for a row in a table using an index
    // (i.e. a primary key or a secondary index)
    Search(Search),
}

impl Operation {
    pub fn index(&self) -> Option<&Arc<Index>> {
        match self {
            Operation::Scan { index, .. } => index.as_ref(),
            Operation::Search(Search::RowidEq { .. }) => None,
            Operation::Search(Search::Seek { index, .. }) => index.as_ref(),
        }
    }
}

impl JoinedTable {
    /// Returns the btree table for this table reference, if it is a BTreeTable.
    pub fn btree(&self) -> Option<Rc<BTreeTable>> {
        match &self.table {
            Table::BTree(_) => self.table.btree(),
            _ => None,
        }
    }
    pub fn virtual_table(&self) -> Option<Rc<VirtualTable>> {
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
    ) -> Self {
        let columns = plan
            .result_columns
            .iter()
            .map(|rc| Column {
                name: rc.name(&plan.table_references).map(String::from),
                ty: Type::Blob, // FIXME: infer proper type
                ty_str: "BLOB".to_string(),
                is_rowid_alias: false,
                primary_key: false,
                notnull: false,
                default: None,
                unique: false,
                collation: None, // FIXME: infer collation from subquery
            })
            .collect();

        let table = Table::FromClauseSubquery(FromClauseSubquery {
            name: identifier.clone(),
            plan: Box::new(plan),
            columns,
            result_columns_start_reg: None,
        });
        Self {
            op: Operation::Scan {
                iter_dir: IterationDirection::Forwards,
                index: None,
            },
            table,
            identifier,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
        }
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
    ) -> Result<(Option<CursorID>, Option<CursorID>)> {
        let index = self.op.index();
        match &self.table {
            Table::BTree(btree) => {
                let use_covering_index = self.utilizes_covering_index();
                let index_is_ephemeral = index.is_some_and(|index| index.ephemeral);
                let table_not_required =
                    OperationMode::SELECT == mode && use_covering_index && !index_is_ephemeral;
                let table_cursor_id = if table_not_required {
                    None
                } else {
                    Some(program.alloc_cursor_id_keyed(
                        CursorKey::table(self.internal_id),
                        CursorType::BTreeTable(btree.clone()),
                    ))
                };
                let index_cursor_id = index.map(|index| {
                    program.alloc_cursor_id_keyed(
                        CursorKey::index(self.internal_id, index.clone()),
                        CursorType::BTreeIndex(index.clone()),
                    )
                });
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
    ) -> Result<(Option<CursorID>, Option<CursorID>)> {
        let index = self.op.index();
        let table_cursor_id = program.resolve_cursor_id_safe(&CursorKey::table(self.internal_id));
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
/// [TerminationKey] is the condition that is used to terminate the search after a seek.
#[derive(Debug, Clone)]
pub struct SeekDef {
    /// The key to use when seeking and when terminating the scan that follows the seek.
    /// For example, given:
    /// - CREATE INDEX i ON t (x, y desc)
    /// - SELECT * FROM t WHERE x = 1 AND y >= 30
    ///
    /// The key is [(1, ASC), (30, DESC)]
    pub key: Vec<(ast::Expr, SortOrder)>,
    /// The condition to use when seeking. See [SeekKey] for more details.
    pub seek: Option<SeekKey>,
    /// The condition to use when terminating the scan that follows the seek. See [TerminationKey] for more details.
    pub termination: Option<TerminationKey>,
    /// The direction of the scan that follows the seek.
    pub iter_dir: IterationDirection,
}

/// A condition to use when seeking.
#[derive(Debug, Clone)]
pub struct SeekKey {
    /// How many columns from [SeekDef::key] are used in seeking.
    pub len: usize,
    /// Whether to NULL pad the last column of the seek key to match the length of [SeekDef::key].
    /// The reason it is done is that sometimes our full index key is not used in seeking,
    /// but we want to find the lowest value that matches the non-null prefix of the key.
    /// For example, given:
    /// - CREATE INDEX i ON t (x, y)
    /// - SELECT * FROM t WHERE x = 1 AND y < 30
    ///
    /// We want to seek to the first row where x = 1, and then iterate forwards.
    /// In this case, the seek key is GT(1, NULL) since NULL is always LT in index key comparisons.
    /// We can't use just GT(1) because in index key comparisons, only the given number of columns are compared,
    /// so this means any index keys with (x=1) will compare equal, e.g. (x=1, y=usize::MAX) will compare equal to the seek key (x:1)
    pub null_pad: bool,
    /// The comparison operator to use when seeking.
    pub op: SeekOp,
}

#[derive(Debug, Clone)]
/// A condition to use when terminating the scan that follows a seek.
pub struct TerminationKey {
    /// How many columns from [SeekDef::key] are used in terminating the scan that follows the seek.
    pub len: usize,
    /// Whether to NULL pad the last column of the termination key to match the length of [SeekDef::key].
    /// See [SeekKey::null_pad].
    pub null_pad: bool,
    /// The comparison operator to use when terminating the scan that follows the seek.
    pub op: SeekOp,
}

/// An enum that represents a search operation that can be used to search for a row in a table using an index
/// (i.e. a primary key or a secondary index)
#[allow(clippy::enum_variant_names)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub func: AggFunc,
    pub args: Vec<ast::Expr>,
    pub original_expr: ast::Expr,
    pub distinctness: Distinctness,
}

impl Aggregate {
    pub fn is_distinct(&self) -> bool {
        self.distinctness.is_distinct()
    }
}
