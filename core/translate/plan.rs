use core::fmt;
use limbo_ext::{ConstraintInfo, ConstraintOp};
use limbo_sqlite3_parser::ast::{self, SortOrder};
use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
    rc::Rc,
    sync::Arc,
};

use crate::{
    function::AggFunc,
    schema::{BTreeTable, Column, Index, Table},
    util::exprs_are_equivalent,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        BranchOffset, CursorID,
    },
    Result, VirtualTable,
};
use crate::{
    schema::{PseudoTable, Type},
    types::SeekOp,
    util::can_pushdown_predicate,
};

use super::{emitter::OperationMode, planner::determine_where_to_eval_term, schema::ParseSchema};

#[derive(Debug, Clone)]
pub struct ResultSetColumn {
    pub expr: ast::Expr,
    pub alias: Option<String>,
    // TODO: encode which aggregates (e.g. index bitmask of plan.aggregates) are present in this column
    pub contains_aggregates: bool,
}

impl ResultSetColumn {
    pub fn name<'a>(&'a self, tables: &'a [TableReference]) -> Option<&'a str> {
        if let Some(alias) = &self.alias {
            return Some(alias);
        }
        match &self.expr {
            ast::Expr::Column { table, column, .. } => {
                tables[*table].columns()[*column].name.as_deref()
            }
            ast::Expr::RowId { table, .. } => {
                // If there is a rowid alias column, use its name
                if let Table::BTree(table) = &tables[*table].table {
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
    /// Is this condition originally from an OUTER JOIN, and which table number in the plan's [TableReference] vector?
    /// If so, we need to evaluate it at the loop of the right table in that JOIN,
    /// regardless of which tables it references.
    /// We also cannot e.g. short circuit the entire query in the optimizer if the condition is statically false.
    pub from_outer_join: Option<usize>,
    /// Whether the condition has been consumed by the optimizer in some way, and it should not be evaluated
    /// in the normal place where WHERE terms are evaluated.
    /// A term may have been consumed e.g. if:
    /// - it has been converted into a constraint in a seek key
    /// - it has been removed due to being trivially true or false
    pub consumed: bool,
}

impl WhereTerm {
    pub fn should_eval_before_loop(&self, join_order: &[JoinOrderMember]) -> bool {
        if self.consumed {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order) else {
            return false;
        };
        eval_at == EvalAt::BeforeLoop
    }

    pub fn should_eval_at_loop(&self, loop_idx: usize, join_order: &[JoinOrderMember]) -> bool {
        if self.consumed {
            return false;
        }
        let Ok(eval_at) = self.eval_at(join_order) else {
            return false;
        };
        eval_at == EvalAt::Loop(loop_idx)
    }

    fn eval_at(&self, join_order: &[JoinOrderMember]) -> Result<EvalAt> {
        determine_where_to_eval_term(&self, join_order)
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
    table_index: usize,
    pred_idx: usize,
) -> Option<ConstraintInfo> {
    if term.from_outer_join.is_some() {
        return None;
    }
    let Expr::Binary(lhs, op, rhs) = &term.expr else {
        return None;
    };
    let expr_is_ready = |e: &Expr| -> bool { can_pushdown_predicate(e, table_index) };
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
            let vtab_on_l = *tbl_l == table_index;
            let vtab_on_r = *tbl_r == table_index;
            if vtab_on_l == vtab_on_r {
                return None; // either both or none -> not convertible
            }

            if vtab_on_l {
                // vtab on left side: operator unchanged
                let usable = *tbl_r < table_index; // usable if the other table is already positioned
                (col_l, op, usable, false)
            } else {
                // vtab on right side of the expr: reverse operator
                let usable = *tbl_l < table_index;
                (col_r, &reverse_operator(op).unwrap_or(*op), usable, true)
            }
        }
        (Expr::Column { table, column, .. }, other) if *table == table_index => {
            (
                column,
                op,
                expr_is_ready(other), // literal / earlier‑table / deterministic func ?
                false,
            )
        }
        (other, Expr::Column { table, column, .. }) if *table == table_index => (
            column,
            &reverse_operator(op).unwrap_or(*op),
            expr_is_ready(other),
            true,
        ),

        _ => return None, // does not involve the virtual table at all
    };

    Some(ConstraintInfo {
        column_index: *vcol_idx as u32,
        op: to_ext_constraint_op(op_for_vtab)?,
        usable,
        plan_info: ConstraintInfo::pack_plan_info(pred_idx as u32, is_rhs),
    })
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
    Delete(DeletePlan),
    Update(UpdatePlan),
}

/// The type of the query, either top level or subquery
#[derive(Debug, Clone)]
pub enum SelectQueryType {
    TopLevel,
    Subquery {
        /// The register that holds the program offset that handles jumping to/from the subquery.
        yield_reg: usize,
        /// The index of the first instruction in the bytecode that implements the subquery.
        coroutine_implementation_start: BranchOffset,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinOrderMember {
    /// The index of the table in the plan's vector of [TableReference]
    pub table_no: usize,
    /// Whether this member is the right side of an OUTER JOIN
    pub is_outer: bool,
}

impl Default for JoinOrderMember {
    fn default() -> Self {
        Self {
            table_no: 0,
            is_outer: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    /// List of table references in loop order, outermost first.
    pub table_references: Vec<TableReference>,
    /// The order in which the tables are joined. Tables have usize Ids (their index in table_references)
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
    /// query type (top level or subquery)
    pub query_type: SelectQueryType,
}

impl SelectPlan {
    pub fn agg_args_count(&self) -> usize {
        self.aggregates.iter().map(|agg| agg.args.len()).sum()
    }

    pub fn group_by_col_count(&self) -> usize {
        self.group_by
            .as_ref()
            .map_or(0, |group_by| group_by.exprs.len())
    }

    pub fn non_group_by_non_agg_columns(&self) -> impl Iterator<Item = &ast::Expr> {
        self.result_columns
            .iter()
            .filter(|c| {
                !c.contains_aggregates
                    && !self.group_by.as_ref().map_or(false, |group_by| {
                        group_by
                            .exprs
                            .iter()
                            .any(|expr| exprs_are_equivalent(&c.expr, expr))
                    })
            })
            .map(|c| &c.expr)
    }

    pub fn non_group_by_non_agg_column_count(&self) -> usize {
        self.non_group_by_non_agg_columns().count()
    }

    pub fn group_by_sorter_column_count(&self) -> usize {
        self.agg_args_count() + self.group_by_col_count() + self.non_group_by_non_agg_column_count()
    }

    /// Reference: https://github.com/sqlite/sqlite/blob/5db695197b74580c777b37ab1b787531f15f7f9f/src/select.c#L8613
    ///
    /// Checks to see if the query is of the format `SELECT count(*) FROM <tbl>`
    pub fn is_simple_count(&self) -> bool {
        if !self.where_clause.is_empty()
            || self.aggregates.len() != 1
            || matches!(self.query_type, SelectQueryType::Subquery { .. })
            || self.table_references.len() != 1
            || self.result_columns.len() != 1
            || self.group_by.is_some()
            || self.contains_constant_false_condition
        // TODO: (pedrocarlo) maybe can optimize to use the count optmization with more columns
        {
            return false;
        }
        let table_ref = self.table_references.first().unwrap();
        if !matches!(table_ref.table, crate::schema::Table::BTree(..)) {
            return false;
        }
        let agg = self.aggregates.first().unwrap();
        if !matches!(agg.func, AggFunc::Count0) {
            return false;
        }

        let count = limbo_sqlite3_parser::ast::Expr::FunctionCall {
            name: limbo_sqlite3_parser::ast::Id("count".to_string()),
            distinctness: None,
            args: None,
            order_by: None,
            filter_over: None,
        };
        let count_star = limbo_sqlite3_parser::ast::Expr::FunctionCallStar {
            name: limbo_sqlite3_parser::ast::Id("count".to_string()),
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
    /// List of table references. Delete is always a single table.
    pub table_references: Vec<TableReference>,
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
    // table being updated is always first
    pub table_references: Vec<TableReference>,
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
    pub parse_schema: ParseSchema,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
}

pub fn select_star(tables: &[TableReference], out_columns: &mut Vec<ResultSetColumn>) {
    for (current_table_index, table) in tables.iter().enumerate() {
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
                                .map_or(false, |name| name.eq_ignore_ascii_case(&using_col.0))
                        })
                    } else {
                        true
                    }
                })
                .map(|(i, col)| ResultSetColumn {
                    alias: None,
                    expr: ast::Expr::Column {
                        database: None,
                        table: current_table_index,
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

/// A table reference in the query plan.
/// For example, SELECT * FROM users u JOIN products p JOIN (SELECT * FROM users) sub
/// has three table references:
/// 1. operation=Scan, table=users, table_identifier=u, reference_type=BTreeTable, join_info=None
/// 2. operation=Scan, table=products, table_identifier=p, reference_type=BTreeTable, join_info=Some(JoinInfo { outer: false, using: None }),
/// 3. operation=Subquery, table=users, table_identifier=sub, reference_type=Subquery, join_info=None
#[derive(Debug, Clone)]
pub struct TableReference {
    /// The operation that this table reference performs.
    pub op: Operation,
    /// Table object, which contains metadata about the table, e.g. columns.
    pub table: Table,
    /// The name of the table as referred to in the query, either the literal name or an alias e.g. "users" or "u"
    pub identifier: String,
    /// The join info for this table reference, if it is the right side of a join (which all except the first table reference have)
    pub join_info: Option<JoinInfo>,
    /// Bitmask of columns that are referenced in the query.
    /// Used to decide whether a covering index can be used.
    pub col_used_mask: ColumnUsedMask,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct ColumnUsedMask(u128);

impl ColumnUsedMask {
    pub fn new() -> Self {
        Self(0)
    }

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
    /// Subquery operation
    /// This operation is used to represent a subquery in the query plan.
    /// The subquery itself (recursively) contains an arbitrary SelectPlan.
    Subquery {
        plan: Box<SelectPlan>,
        result_columns_start_reg: usize,
    },
}

impl Operation {
    pub fn index(&self) -> Option<&Arc<Index>> {
        match self {
            Operation::Scan { index, .. } => index.as_ref(),
            Operation::Search(Search::RowidEq { .. }) => None,
            Operation::Search(Search::Seek { index, .. }) => index.as_ref(),
            Operation::Subquery { .. } => None,
        }
    }
}

impl TableReference {
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
    pub fn new_subquery(identifier: String, plan: SelectPlan, join_info: Option<JoinInfo>) -> Self {
        let table = Table::Pseudo(Rc::new(PseudoTable::new_with_columns(
            plan.result_columns
                .iter()
                .map(|rc| Column {
                    name: rc.name(&plan.table_references).map(String::from),
                    ty: Type::Text, // FIXME: infer proper type
                    ty_str: "TEXT".to_string(),
                    is_rowid_alias: false,
                    primary_key: false,
                    notnull: false,
                    default: None,
                    unique: false,
                })
                .collect(),
        )));
        Self {
            op: Operation::Subquery {
                plan: Box::new(plan),
                result_columns_start_reg: 0, // Will be set in the bytecode emission phase
            },
            table,
            identifier: identifier.clone(),
            join_info,
            col_used_mask: ColumnUsedMask::new(),
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
                let index_is_ephemeral = index.map_or(false, |index| index.ephemeral);
                let table_not_required =
                    OperationMode::SELECT == mode && use_covering_index && !index_is_ephemeral;
                let table_cursor_id = if table_not_required {
                    None
                } else {
                    Some(program.alloc_cursor_id(
                        Some(self.identifier.clone()),
                        CursorType::BTreeTable(btree.clone()),
                    ))
                };
                let index_cursor_id = if let Some(index) = index {
                    Some(program.alloc_cursor_id(
                        Some(index.name.clone()),
                        CursorType::BTreeIndex(index.clone()),
                    ))
                } else {
                    None
                };
                Ok((table_cursor_id, index_cursor_id))
            }
            Table::Virtual(virtual_table) => {
                let table_cursor_id = Some(program.alloc_cursor_id(
                    Some(self.identifier.clone()),
                    CursorType::VirtualTable(virtual_table.clone()),
                ));
                let index_cursor_id = None;
                Ok((table_cursor_id, index_cursor_id))
            }
            Table::Pseudo(_) => Ok((None, None)),
        }
    }

    /// Resolve the already opened cursors for this table reference.
    pub fn resolve_cursors(
        &self,
        program: &mut ProgramBuilder,
    ) -> Result<(Option<CursorID>, Option<CursorID>)> {
        let index = self.op.index();
        let table_cursor_id = program.resolve_cursor_id_safe(&self.identifier);
        let index_cursor_id = index.map(|index| program.resolve_cursor_id(&index.name));
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
        let mut index_cols_mask = ColumnUsedMask::new();
        for col in index.columns.iter() {
            index_cols_mask.set(col.pos_in_table);
        }

        // If a table has a rowid (i.e. is not a WITHOUT ROWID table), the index is guaranteed to contain the rowid as well.
        if btree.has_rowid {
            if let Some(pos_of_rowid_alias_col) = btree.get_rowid_alias_column().map(|(pos, _)| pos)
            {
                let mut empty_mask = ColumnUsedMask::new();
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

#[derive(Clone, Debug, PartialEq)]
pub struct Aggregate {
    pub func: AggFunc,
    pub args: Vec<ast::Expr>,
    pub original_expr: ast::Expr,
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let args_str = self
            .args
            .iter()
            .map(|arg| arg.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{:?}({})", self.func, args_str)
    }
}

/// For EXPLAIN QUERY PLAN
impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select(select_plan) => select_plan.fmt(f),
            Self::Delete(delete_plan) => delete_plan.fmt(f),
            Self::Update(update_plan) => update_plan.fmt(f),
        }
    }
}

impl Display for SelectPlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        // Print each table reference with appropriate indentation based on join depth
        for (i, reference) in self.table_references.iter().enumerate() {
            let is_last = i == self.table_references.len() - 1;
            let indent = if i == 0 {
                if is_last { "`--" } else { "|--" }.to_string()
            } else {
                format!(
                    "   {}{}",
                    "|  ".repeat(i - 1),
                    if is_last { "`--" } else { "|--" }
                )
            };

            match &reference.op {
                Operation::Scan { .. } => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    writeln!(f, "{}SCAN {}", indent, table_name)?;
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                            indent, reference.identifier
                        )?;
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                },
                Operation::Subquery { plan, .. } => {
                    writeln!(f, "{}SUBQUERY {}", indent, reference.identifier)?;
                    // Indent and format the subquery plan
                    for line in format!("{}", plan).lines() {
                        writeln!(f, "{}   {}", indent, line)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Display for DeletePlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        // Delete plan should only have one table reference
        if let Some(reference) = self.table_references.first() {
            let indent = "`--";

            match &reference.op {
                Operation::Scan { .. } => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    writeln!(f, "{}DELETE FROM {}", indent, table_name)?;
                }
                Operation::Search { .. } => {
                    panic!("DELETE plans should not contain search operations");
                }
                Operation::Subquery { .. } => {
                    panic!("DELETE plans should not contain subqueries");
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for UpdatePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "QUERY PLAN")?;

        for (i, reference) in self.table_references.iter().enumerate() {
            let is_last = i == self.table_references.len() - 1;
            let indent = if i == 0 {
                if is_last { "`--" } else { "|--" }.to_string()
            } else {
                format!(
                    "   {}{}",
                    "|  ".repeat(i - 1),
                    if is_last { "`--" } else { "|--" }
                )
            };

            match &reference.op {
                Operation::Scan { .. } => {
                    let table_name = if reference.table.get_name() == reference.identifier {
                        reference.identifier.clone()
                    } else {
                        format!("{} AS {}", reference.table.get_name(), reference.identifier)
                    };

                    if i == 0 {
                        writeln!(f, "{}UPDATE {}", indent, table_name)?;
                    } else {
                        writeln!(f, "{}SCAN {}", indent, table_name)?;
                    }
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                            indent, reference.identifier
                        )?;
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        writeln!(
                            f,
                            "{}SEARCH {} USING INDEX {}",
                            indent, reference.identifier, index.name
                        )?;
                    }
                },
                Operation::Subquery { plan, .. } => {
                    writeln!(f, "{}SUBQUERY {}", indent, reference.identifier)?;
                    for line in format!("{}", plan).lines() {
                        writeln!(f, "{}   {}", indent, line)?;
                    }
                }
            }
        }
        if let Some(order_by) = &self.order_by {
            writeln!(f, "ORDER BY:")?;
            for (expr, dir) in order_by {
                writeln!(
                    f,
                    "  - {} {}",
                    expr,
                    if *dir == SortOrder::Asc {
                        "ASC"
                    } else {
                        "DESC"
                    }
                )?;
            }
        }
        if let Some(limit) = self.limit {
            writeln!(f, "LIMIT: {}", limit)?;
        }
        if let Some(ret) = &self.returning {
            writeln!(f, "RETURNING:")?;
            for col in ret {
                writeln!(f, "  - {}", col.expr)?;
            }
        }

        Ok(())
    }
}
