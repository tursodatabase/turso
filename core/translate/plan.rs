use core::fmt;
use limbo_sqlite3_parser::ast;
use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
    rc::Rc,
    sync::Arc,
};

use crate::{
    function::AggFunc,
    schema::{BTreeTable, Column, Index, Table},
    vdbe::BranchOffset,
    VirtualTable,
};
use crate::{
    schema::{PseudoTable, Type},
    types::SeekOp,
};

#[derive(Debug, Clone)]
pub struct ResultSetColumn {
    pub expr: ast::Expr,
    pub alias: Option<String>,
    // TODO: encode which aggregates (e.g. index bitmask of plan.aggregates) are present in this column
    pub contains_aggregates: bool,
}

impl ResultSetColumn {
    pub fn name<'a>(&'a self, tables: &'a [TableReference]) -> Option<&'a String> {
        if let Some(alias) = &self.alias {
            return Some(alias);
        }
        match &self.expr {
            ast::Expr::Column { table, column, .. } => {
                tables[*table].columns()[*column].name.as_ref()
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupBy {
    pub exprs: Vec<ast::Expr>,
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
    /// Is this condition originally from an OUTER JOIN?
    /// If so, we need to evaluate it at the loop of the right table in that JOIN,
    /// regardless of which tables it references.
    /// We also cannot e.g. short circuit the entire query in the optimizer if the condition is statically false.
    pub from_outer_join: bool,
    pub eval_at: EvalAt,
}

impl WhereTerm {
    pub fn is_constant(&self) -> bool {
        self.eval_at == EvalAt::BeforeLoop
    }

    pub fn should_eval_at_loop(&self, loop_idx: usize) -> bool {
        self.eval_at == EvalAt::Loop(loop_idx)
    }
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

#[derive(Debug, Clone)]
pub struct SelectPlan {
    /// List of table references in loop order, outermost first.
    pub table_references: Vec<TableReference>,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries. all join conditions also get shoved in here,
    /// and we keep track of which join they came from (mainly for OUTER JOIN processing)
    pub where_clause: Vec<WhereTerm>,
    /// group by clause
    pub group_by: Option<GroupBy>,
    /// order by clause
    pub order_by: Option<Vec<(ast::Expr, Direction)>>,
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
    pub order_by: Option<Vec<(ast::Expr, Direction)>>,
    /// limit clause
    pub limit: Option<isize>,
    /// offset clause
    pub offset: Option<isize>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    // table being updated is always first
    pub table_references: Vec<TableReference>,
    // (colum index, new value) pairs
    pub set_clauses: Vec<(usize, ast::Expr)>,
    pub where_clause: Vec<WhereTerm>,
    pub order_by: Option<Vec<(ast::Expr, Direction)>>,
    pub limit: Option<isize>,
    pub offset: Option<isize>,
    // TODO: optional RETURNING clause
    pub returning: Option<Vec<ResultSetColumn>>,
    // whether the WHERE clause is always false
    pub contains_constant_false_condition: bool,
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
                    name: rc.name(&plan.table_references).map(String::clone),
                    ty: Type::Text, // FIXME: infer proper type
                    ty_str: "TEXT".to_string(),
                    is_rowid_alias: false,
                    primary_key: false,
                    notnull: false,
                    default: None,
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
        }
    }

    pub fn columns(&self) -> &[Column] {
        self.table.columns()
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
    /// - CREATE INDEX i ON t (x, y)
    /// - SELECT * FROM t WHERE x = 1 AND y >= 30
    /// The key is [1, 30]
    pub key: Vec<ast::Expr>,
    /// The condition to use when seeking. See [SeekKey] for more details.
    pub seek: Option<SeekKey>,
    /// The condition to use when terminating the scan that follows the seek. See [TerminationKey] for more details.
    pub termination: Option<TerminationKey>,
    /// The direction of the scan that follows the seek.
    pub iter_dir: IterationDirection,
}

impl SeekDef {
    /// Whether we should null pad unset columns when seeking.
    /// This is only done for forward seeks.
    /// The reason it is done is that sometimes our full index key is not used in seeking.
    /// See [SeekKey] for more details.
    ///
    /// For example, given:
    /// - CREATE INDEX i ON t (x, y)
    /// - SELECT * FROM t WHERE x = 1 AND y < 30
    /// We want to seek to the first row where x = 1, and then iterate forwards.
    /// In this case, the seek key is GT(1, NULL) since '30' cannot be used to seek (since we want y < 30),
    /// and any value of y will be greater than NULL.
    ///
    /// In backwards iteration direction, we do not null pad because we want to seek to the last row that matches the seek key.
    /// For example, given:
    /// - CREATE INDEX i ON t (x, y)
    /// - SELECT * FROM t WHERE x = 1 AND y > 30 ORDER BY y
    /// We want to seek to the last row where x = 1, and then iterate backwards.
    /// In this case, the seek key is just LE(1) so any row with x = 1 will be a match.
    pub fn null_pad_unset_cols(&self) -> bool {
        self.iter_dir == IterationDirection::Forwards
    }
}

/// A condition to use when seeking.
#[derive(Debug, Clone)]
pub struct SeekKey {
    /// How many columns from [SeekDef::key] are used in seeking.
    pub len: usize,
    /// The comparison operator to use when seeking.
    pub op: SeekOp,
}

#[derive(Debug, Clone)]
/// A condition to use when terminating the scan that follows a seek.
pub struct TerminationKey {
    /// How many columns from [SeekDef::key] are used in terminating the scan that follows the seek.
    pub len: usize,
    /// The comparison operator to use when terminating the scan that follows the seek.
    pub op: SeekOp,
}

/// An enum that represents a search operation that can be used to search for a row in a table using an index
/// (i.e. a primary key or a secondary index)
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug)]
pub enum Search {
    /// A rowid equality point lookup. This is a special case that uses the SeekRowid bytecode instruction and does not loop.
    RowidEq { cmp_expr: WhereTerm },
    /// A search on a table btree (via `rowid`) or a secondary index search. Uses bytecode instructions like SeekGE, SeekGT etc.
    Seek {
        index: Option<Arc<Index>>,
        seek_def: SeekDef,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Direction::Ascending => write!(f, "ASC"),
            Direction::Descending => write!(f, "DESC"),
        }
    }
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
                writeln!(f, "  - {} {}", expr, dir)?;
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
