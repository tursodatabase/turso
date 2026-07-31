//! Logical plan representation for SQL queries
//!
//! This module provides a platform-independent intermediate representation
//! for SQL queries. The logical plan is a DAG (Directed Acyclic Graph) that
//! supports CTEs and can be used for query optimization before being compiled
//! to an execution plan (e.g., DBSP circuits).
//!
//! The main entry point is `LogicalPlanBuilder` which constructs logical plans
//! from resolved semantic HIR.
use crate::function::{AggFunc, Deterministic, Func};
use crate::schema::Type;
use crate::sync::Arc;
use crate::translate::semantic::hir::{
    self, CteId, HirDocument, OutputId, QueryBlockId, QueryId, SourceId,
};
use crate::types::Value;
use crate::vdbe::affinity::Affinity;
use crate::{LimboError, Result};
use rustc_hash::FxHashMap as HashMap;
use std::fmt::{self, Display, Formatter};
use turso_parser::ast;

/// Stable identity of a value in a logical row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogicalColumnId {
    Source(hir::ColumnRef),
    Output(OutputId),
    Synthetic(usize),
}

/// Information about a column in a logical schema
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub id: LogicalColumnId,
    pub name: String,
    pub ty: Type,
    pub affinity: crate::vdbe::affinity::Affinity,
    pub has_affinity: bool,
    pub collation: Option<hir::ResolvedCollation>,
    pub database: Option<String>,
    pub table: Option<String>,
    pub table_alias: Option<String>,
}

/// Schema information for logical plan nodes
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalSchema {
    pub columns: Vec<ColumnInfo>,
}

/// A reference to a schema that can be shared between nodes
pub type SchemaRef = Arc<LogicalSchema>;

impl LogicalSchema {
    pub fn new(columns: Vec<ColumnInfo>) -> Self {
        Self { columns }
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn find_column_id(&self, id: LogicalColumnId) -> Option<(usize, &ColumnInfo)> {
        self.columns
            .iter()
            .position(|column| column.id == id)
            .map(|index| (index, &self.columns[index]))
    }
}

/// Logical representation of a SQL query plan
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Projection - SELECT expressions
    Projection(Projection),
    /// Filter - WHERE/HAVING clause
    Filter(Filter),
    /// Aggregate - GROUP BY with aggregate functions
    Aggregate(Aggregate),
    /// Join - combining two relations
    Join(Join),
    /// Sort - ORDER BY clause
    Sort(Sort),
    /// Limit - LIMIT/OFFSET clause
    Limit(Limit),
    /// Table scan - reading from a base table
    TableScan(TableScan),
    /// Union - UNION/UNION ALL/INTERSECT/EXCEPT
    Union(Union),
    /// Distinct - remove duplicates
    Distinct(Distinct),
    /// Empty relation - no rows
    EmptyRelation(EmptyRelation),
    /// Values - literal rows (VALUES clause)
    Values(Values),
    /// CTE support - WITH clause
    WithCTE(WithCTE),
    /// Reference to a CTE
    CTERef(CTERef),
}

impl LogicalPlan {
    /// Get the schema of this plan node
    pub fn schema(&self) -> &SchemaRef {
        match self {
            LogicalPlan::Projection(p) => &p.schema,
            LogicalPlan::Filter(f) => f.input.schema(),
            LogicalPlan::Aggregate(a) => &a.schema,
            LogicalPlan::Join(j) => &j.schema,
            LogicalPlan::Sort(s) => s.input.schema(),
            LogicalPlan::Limit(l) => l.input.schema(),
            LogicalPlan::TableScan(t) => &t.schema,
            LogicalPlan::Union(u) => &u.schema,
            LogicalPlan::Distinct(d) => d.input.schema(),
            LogicalPlan::EmptyRelation(e) => &e.schema,
            LogicalPlan::Values(v) => &v.schema,
            LogicalPlan::WithCTE(w) => w.body.schema(),
            LogicalPlan::CTERef(c) => &c.schema,
        }
    }
}

/// Projection operator - SELECT expressions
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    pub input: Arc<LogicalPlan>,
    pub exprs: Vec<LogicalExpr>,
    pub schema: SchemaRef,
}

/// Filter operator - WHERE/HAVING predicates
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub input: Arc<LogicalPlan>,
    pub predicate: LogicalExpr,
}

/// Aggregate operator - GROUP BY with aggregations
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub input: Arc<LogicalPlan>,
    pub group_expr: Vec<LogicalExpr>,
    pub aggr_expr: Vec<LogicalExpr>,
    pub schema: SchemaRef,
}

/// Types of joins
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Join operator - combines two relations
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub join_type: JoinType,
    pub on: Vec<(LogicalExpr, LogicalExpr)>, // Equijoin conditions (left_expr, right_expr)
    pub filter: Option<LogicalExpr>,         // Additional filter conditions
    pub schema: SchemaRef,
}

/// Sort operator - ORDER BY
#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub input: Arc<LogicalPlan>,
    pub exprs: Vec<SortExpr>,
}

/// Sort expression with direction
#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    pub expr: LogicalExpr,
    pub asc: bool,
    pub nulls_first: bool,
}

/// Limit operator - LIMIT/OFFSET
#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    pub input: Arc<LogicalPlan>,
    pub skip: Option<usize>,
    pub fetch: Option<usize>,
}

/// Table scan operator
#[derive(Debug, Clone, PartialEq)]
pub struct TableScan {
    pub source: SourceId,
    pub table: hir::ResolvedTable,
    pub table_name: String,
    pub alias: Option<String>,
    pub schema: SchemaRef,
    pub projection: Option<Vec<usize>>, // Column indices to project
}

/// Union operator
#[derive(Debug, Clone, PartialEq)]
pub struct Union {
    pub inputs: Vec<Arc<LogicalPlan>>,
    pub all: bool, // true for UNION ALL, false for UNION
    pub schema: SchemaRef,
}

/// Distinct operator
#[derive(Debug, Clone, PartialEq)]
pub struct Distinct {
    pub input: Arc<LogicalPlan>,
}

/// Empty relation - produces no rows
#[derive(Debug, Clone, PartialEq)]
pub struct EmptyRelation {
    pub produce_one_row: bool,
    pub schema: SchemaRef,
}

/// Values operator - literal rows
#[derive(Debug, Clone, PartialEq)]
pub struct Values {
    pub rows: Vec<Vec<LogicalExpr>>,
    pub schema: SchemaRef,
}

/// WITH clause - CTEs
#[derive(Debug, Clone, PartialEq)]
pub struct WithCTE {
    pub ctes: HashMap<CteId, Arc<LogicalPlan>>,
    pub body: Arc<LogicalPlan>,
}

/// Reference to a CTE
#[derive(Debug, Clone, PartialEq)]
pub struct CTERef {
    pub id: CteId,
    pub source: SourceId,
    pub name: String,
    pub schema: SchemaRef,
}

/// Logical expression representation
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpr {
    /// Column reference
    Column(Column),
    /// Literal value
    Literal(Value),
    /// Binary expression
    BinaryExpr {
        left: Box<LogicalExpr>,
        op: BinaryOperator,
        right: Box<LogicalExpr>,
    },
    /// Unary expression
    UnaryExpr {
        op: UnaryOperator,
        expr: Box<LogicalExpr>,
    },
    /// Aggregate function
    AggregateFunction {
        fun: AggregateFunction,
        args: Vec<LogicalExpr>,
        arg_types: Vec<Type>,
        distinct: bool,
        result_type: Type,
    },
    /// Scalar function call
    ScalarFunction {
        function: hir::ResolvedFunction,
        args: Vec<LogicalExpr>,
        result_type: Type,
    },
    /// CASE expression
    Case {
        expr: Option<Box<LogicalExpr>>,
        when_then: Vec<(LogicalExpr, LogicalExpr)>,
        else_expr: Option<Box<LogicalExpr>>,
    },
    /// IN list
    InList {
        expr: Box<LogicalExpr>,
        list: Vec<LogicalExpr>,
        negated: bool,
    },
    /// IN subquery
    InSubquery {
        expr: Box<LogicalExpr>,
        subquery: Arc<LogicalPlan>,
        negated: bool,
    },
    /// EXISTS subquery
    Exists {
        subquery: Arc<LogicalPlan>,
        negated: bool,
    },
    /// Scalar subquery
    ScalarSubquery {
        plan: Arc<LogicalPlan>,
        output: usize,
    },
    /// Alias for an expression
    Alias {
        expr: Box<LogicalExpr>,
        alias: String,
    },
    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<LogicalExpr>,
        negated: bool,
    },
    /// BETWEEN
    Between {
        expr: Box<LogicalExpr>,
        low: Box<LogicalExpr>,
        high: Box<LogicalExpr>,
        negated: bool,
    },
    /// LIKE pattern matching
    Like {
        expr: Box<LogicalExpr>,
        pattern: Box<LogicalExpr>,
        escape: Option<Box<LogicalExpr>>,
        negated: bool,
        operator: ast::LikeOperator,
        function: hir::ResolvedFunction,
        argument_count: usize,
    },
    /// CAST expression
    Cast {
        expr: Box<LogicalExpr>,
        name: String,
        parameters: Vec<LogicalExpr>,
        ty: Type,
    },
    /// Explicit collation chosen during semantic analysis.
    Collate {
        expr: Box<LogicalExpr>,
        collation: hir::ResolvedCollation,
    },
}

/// Column reference
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub id: LogicalColumnId,
    pub name: String,
    pub table: Option<String>,
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self.table {
            Some(t) => write!(f, "{}.{}", t, self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

/// Type alias for binary operators
pub type BinaryOperator = ast::Operator;

/// Type alias for unary operators
pub type UnaryOperator = ast::UnaryOperator;

/// Type alias for aggregate functions
pub type AggregateFunction = AggFunc;

/// Lowers one semantically resolved query into the logical representation used
/// by the incremental compiler. The document is borrowed only while lowering;
/// every object kept by the returned plan is owned.
pub struct LogicalPlanBuilder<'a> {
    document: &'a HirDocument,
    query_plans: HashMap<QueryId, Arc<LogicalPlan>>,
    cte_plans: HashMap<CteId, Arc<LogicalPlan>>,
    building_queries: Vec<QueryId>,
    building_ctes: Vec<CteId>,
    next_synthetic_column: usize,
}

impl<'a> LogicalPlanBuilder<'a> {
    pub fn new(document: &'a HirDocument) -> Self {
        Self {
            document,
            query_plans: HashMap::default(),
            cte_plans: HashMap::default(),
            building_queries: Vec::new(),
            building_ctes: Vec::new(),
            next_synthetic_column: 0,
        }
    }

    pub fn build_query(&mut self, query: QueryId) -> Result<LogicalPlan> {
        Ok(self.build_query_shared(query)?.as_ref().clone())
    }

    fn build_query_shared(&mut self, query_id: QueryId) -> Result<Arc<LogicalPlan>> {
        if let Some(plan) = self.query_plans.get(&query_id) {
            return Ok(plan.clone());
        }
        if self.building_queries.contains(&query_id) {
            return Err(LimboError::ParseError(format!(
                "recursive query {query_id} is not supported by incremental views"
            )));
        }
        let query = self.document.query(query_id).cloned().ok_or_else(|| {
            LimboError::InternalError(format!("semantic HIR has no query {query_id}"))
        })?;

        self.building_queries.push(query_id);
        let result = self.build_query_uncached(&query);
        self.building_queries.pop();
        let plan = Arc::new(result?);
        self.query_plans.insert(query_id, plan.clone());
        Ok(plan)
    }

    fn build_query_uncached(&mut self, query: &hir::Query) -> Result<LogicalPlan> {
        for cte in &query.reachable_ctes {
            self.build_cte(*cte)?;
        }

        let mut plan = self.build_query_block(query.first)?;
        for arm in &query.compounds {
            let right = self.build_query_block(arm.block)?;
            let all = match arm.operator {
                ast::CompoundOperator::Union => false,
                ast::CompoundOperator::UnionAll => true,
                ast::CompoundOperator::Except | ast::CompoundOperator::Intersect => {
                    return Err(LimboError::ParseError(format!(
                        "{:?} is not supported by incremental views",
                        arm.operator
                    )));
                }
            };
            if plan.schema().column_count() != right.schema().column_count() {
                return Err(LimboError::InternalError(format!(
                    "semantic HIR compound query {} has mismatched row widths",
                    query.id
                )));
            }
            let schema = self.schema_from_output_ids(&query.output)?;
            plan = LogicalPlan::Union(Union {
                inputs: vec![Arc::new(plan), Arc::new(right)],
                all,
                schema,
            });
        }

        if !query.order_by.is_empty() {
            let mut exprs = Vec::with_capacity(query.order_by.len());
            for term in &query.order_by {
                let expr = match &term.expr {
                    hir::Expr::Output(output) => self.output_column(*output)?,
                    expr => self.build_expr(expr)?,
                };
                let asc = matches!(term.order, ast::SortOrder::Asc);
                let nulls_first = match term.nulls {
                    Some(ast::NullsOrder::First) => true,
                    Some(ast::NullsOrder::Last) => false,
                    None => asc,
                };
                exprs.push(SortExpr {
                    expr,
                    asc,
                    nulls_first,
                });
            }
            plan = LogicalPlan::Sort(Sort {
                input: Arc::new(plan),
                exprs,
            });
        }

        if let Some(limit) = &query.limit {
            let fetch = Self::literal_limit(&limit.limit, "LIMIT")?;
            let skip = limit
                .offset
                .as_ref()
                .map(|offset| Self::literal_limit(offset, "OFFSET"))
                .transpose()?;
            plan = LogicalPlan::Limit(Limit {
                input: Arc::new(plan),
                skip,
                fetch: Some(fetch),
            });
        }

        Ok(plan)
    }

    fn build_query_block(&mut self, block_id: QueryBlockId) -> Result<LogicalPlan> {
        let block = self
            .document
            .query_block(block_id)
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!("semantic HIR has no query block {block_id:?}"))
            })?;
        match &block.body {
            hir::QueryBlockBody::Values { rows } => self.build_values(&block, rows),
            hir::QueryBlockBody::Select {
                distinctness,
                filter,
                grouping,
                windows,
            } => {
                if !windows.is_empty() {
                    return Err(LimboError::ParseError(
                        "window definitions are not supported by incremental views".to_string(),
                    ));
                }
                self.build_select_block(&block, *distinctness, filter.as_ref(), grouping.as_ref())
            }
        }
    }

    fn build_select_block(
        &mut self,
        block: &hir::QueryBlock,
        distinctness: Option<ast::Distinctness>,
        filter: Option<&hir::Expr>,
        grouping: Option<&hir::Grouping>,
    ) -> Result<LogicalPlan> {
        let mut plan = match &block.from {
            Some(from) => self.build_from(from)?,
            None => LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(LogicalSchema::empty()),
            }),
        };

        if let Some(filter) = filter {
            plan = LogicalPlan::Filter(Filter {
                input: Arc::new(plan),
                predicate: self.build_expr(filter)?,
            });
        }

        let mut outputs = Vec::with_capacity(block.outputs.len());
        for output in &block.outputs {
            outputs.push(self.build_expr(&output.expr)?);
        }
        let having = grouping
            .and_then(|grouping| grouping.having.as_ref())
            .map(|expr| self.build_expr(expr))
            .transpose()?;
        let group_exprs = grouping
            .map(|grouping| {
                grouping
                    .keys
                    .iter()
                    .map(|expr| {
                        Ok((
                            self.build_expr(expr)?,
                            self.finalize_type_fact(
                                &self.expression_type_fact(expr),
                                "GROUP BY expression",
                            )?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();

        let needs_aggregate = !group_exprs.is_empty()
            || outputs.iter().any(Self::contains_aggregate)
            || having.as_ref().is_some_and(Self::contains_aggregate);
        let output_schema = self.schema_from_outputs(&block.outputs)?;
        plan = if needs_aggregate {
            self.build_aggregate_plan(plan, group_exprs, outputs, having, output_schema)?
        } else {
            if having.is_some() {
                return Err(LimboError::InternalError(
                    "semantic HIR contains HAVING without grouping or aggregation".to_string(),
                ));
            }
            LogicalPlan::Projection(Projection {
                input: Arc::new(plan),
                exprs: outputs,
                schema: output_schema,
            })
        };

        if matches!(distinctness, Some(ast::Distinctness::Distinct)) {
            plan = LogicalPlan::Distinct(Distinct {
                input: Arc::new(plan),
            });
        }
        Ok(plan)
    }

    fn build_values(
        &mut self,
        block: &hir::QueryBlock,
        rows: &[Vec<hir::Expr>],
    ) -> Result<LogicalPlan> {
        let expected = block.outputs.len();
        let mut logical_rows = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != expected {
                return Err(LimboError::InternalError(format!(
                    "semantic HIR VALUES row has {} values but expected {expected}",
                    row.len()
                )));
            }
            logical_rows.push(
                row.iter()
                    .map(|expr| self.build_expr(expr))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(LogicalPlan::Values(Values {
            rows: logical_rows,
            schema: self.schema_from_outputs(&block.outputs)?,
        }))
    }

    fn build_from(&mut self, from: &hir::From) -> Result<LogicalPlan> {
        let mut plan = self.build_source(from.first)?;
        for join in &from.joins {
            let right = self.build_source(join.right)?;
            let left_schema = plan.schema().clone();
            let right_schema = right.schema().clone();
            let (on, filter) = match &join.constraint {
                hir::JoinConstraint::None => (Vec::new(), None),
                hir::JoinConstraint::On(expr) => {
                    let expr = self.build_expr(expr)?;
                    Self::split_join_condition(expr, &left_schema, &right_schema)
                }
                hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                    let mut pairs = Vec::with_capacity(columns.len());
                    for column in columns {
                        pairs.push((
                            self.build_expr(&column.left)?,
                            self.column_for_ref(column.right)?,
                        ));
                    }
                    (pairs, None)
                }
            };
            let join_type = match join.kind {
                hir::JoinKind::Comma => JoinType::Cross,
                hir::JoinKind::Inner => JoinType::Inner,
                hir::JoinKind::Cross => JoinType::Cross,
                hir::JoinKind::Left => JoinType::Left,
                hir::JoinKind::Right => JoinType::Right,
                hir::JoinKind::Full => JoinType::Full,
            };
            let mut columns = left_schema.columns.clone();
            columns.extend(right_schema.columns.clone());
            plan = LogicalPlan::Join(Join {
                left: Arc::new(plan),
                right: Arc::new(right),
                join_type,
                on,
                filter,
                schema: Arc::new(LogicalSchema::new(columns)),
            });
        }
        Ok(plan)
    }

    fn build_source(&mut self, source_id: SourceId) -> Result<LogicalPlan> {
        let source = self.document.source(source_id).cloned().ok_or_else(|| {
            LimboError::InternalError(format!("semantic HIR has no source {source_id}"))
        })?;
        match &source.kind {
            hir::SourceKind::Table(table) => Ok(LogicalPlan::TableScan(TableScan {
                source: source_id,
                table: table.clone(),
                table_name: table.value().get_name().to_string(),
                alias: source.alias.clone(),
                schema: self.schema_from_source(&source)?,
                projection: None,
            })),
            hir::SourceKind::Derived(query) => {
                let plan = self.build_query_shared(*query)?;
                self.rebind_source(plan, &source)
            }
            hir::SourceKind::Cte(cte) => {
                let plan = self.build_cte(*cte)?;
                self.rebind_source(plan, &source)
            }
            hir::SourceKind::RecursiveInput(cte) => Err(LimboError::ParseError(format!(
                "recursive CTE {cte} is not supported by incremental views"
            ))),
            hir::SourceKind::TableFunction { .. } => Err(LimboError::ParseError(
                "table-valued functions are not supported by incremental views".to_string(),
            )),
            hir::SourceKind::SchemaExpression | hir::SourceKind::Pseudo { .. } => {
                Err(LimboError::InternalError(format!(
                    "non-query source {source_id} reached incremental query lowering"
                )))
            }
        }
    }

    fn build_cte(&mut self, cte_id: CteId) -> Result<Arc<LogicalPlan>> {
        if let Some(plan) = self.cte_plans.get(&cte_id) {
            return Ok(plan.clone());
        }
        if self.building_ctes.contains(&cte_id) {
            return Err(LimboError::ParseError(format!(
                "recursive CTE {cte_id} is not supported by incremental views"
            )));
        }
        let cte = self.document.cte(cte_id).cloned().ok_or_else(|| {
            LimboError::InternalError(format!("semantic HIR has no CTE {cte_id}"))
        })?;
        self.building_ctes.push(cte_id);
        let result = match cte.body {
            hir::CteBody::Query(query) => self.build_query_shared(query),
            hir::CteBody::Recursive(_) => Err(LimboError::ParseError(format!(
                "recursive CTE '{}' is not supported by incremental views",
                cte.name
            ))),
        };
        self.building_ctes.pop();
        let plan = result?;
        self.cte_plans.insert(cte_id, plan.clone());
        Ok(plan)
    }

    fn rebind_source(
        &mut self,
        input: Arc<LogicalPlan>,
        source: &hir::Source,
    ) -> Result<LogicalPlan> {
        if input.schema().column_count() != source.columns.len() {
            return Err(LimboError::InternalError(format!(
                "source {} exposes {} columns but its query produces {}",
                source.id,
                source.columns.len(),
                input.schema().column_count()
            )));
        }
        let exprs = input
            .schema()
            .columns
            .iter()
            .map(|column| {
                LogicalExpr::Column(Column {
                    id: column.id,
                    name: column.name.clone(),
                    table: column.table_alias.clone().or_else(|| column.table.clone()),
                })
            })
            .collect();
        Ok(LogicalPlan::Projection(Projection {
            input,
            exprs,
            schema: self.schema_from_source(source)?,
        }))
    }

    fn split_join_condition(
        expr: LogicalExpr,
        left: &LogicalSchema,
        right: &LogicalSchema,
    ) -> (Vec<(LogicalExpr, LogicalExpr)>, Option<LogicalExpr>) {
        let mut on = Vec::new();
        let mut filters = Vec::new();
        Self::collect_join_conditions(expr, left, right, &mut on, &mut filters);
        let filter = filters
            .into_iter()
            .reduce(|left, right| LogicalExpr::BinaryExpr {
                left: Box::new(left),
                op: ast::Operator::And,
                right: Box::new(right),
            });
        (on, filter)
    }

    fn collect_join_conditions(
        expr: LogicalExpr,
        left: &LogicalSchema,
        right: &LogicalSchema,
        on: &mut Vec<(LogicalExpr, LogicalExpr)>,
        filters: &mut Vec<LogicalExpr>,
    ) {
        match expr {
            LogicalExpr::BinaryExpr {
                left: lhs,
                op: ast::Operator::And,
                right: rhs,
            } => {
                Self::collect_join_conditions(*lhs, left, right, on, filters);
                Self::collect_join_conditions(*rhs, left, right, on, filters);
            }
            LogicalExpr::BinaryExpr {
                left: lhs,
                op: ast::Operator::Equals,
                right: rhs,
            } => {
                let lhs_side = Self::column_side(&lhs, left, right);
                let rhs_side = Self::column_side(&rhs, left, right);
                match (lhs_side, rhs_side) {
                    (Some(false), Some(true)) => on.push((*lhs, *rhs)),
                    (Some(true), Some(false)) => on.push((*rhs, *lhs)),
                    _ => filters.push(LogicalExpr::BinaryExpr {
                        left: lhs,
                        op: ast::Operator::Equals,
                        right: rhs,
                    }),
                }
            }
            expr => filters.push(expr),
        }
    }

    /// Returns false for the left input and true for the right input.
    fn column_side(
        expr: &LogicalExpr,
        left: &LogicalSchema,
        right: &LogicalSchema,
    ) -> Option<bool> {
        let LogicalExpr::Column(column) = expr else {
            return None;
        };
        match (
            left.find_column_id(column.id).is_some(),
            right.find_column_id(column.id).is_some(),
        ) {
            (true, false) => Some(false),
            (false, true) => Some(true),
            _ => None,
        }
    }

    fn build_aggregate_plan(
        &mut self,
        input: LogicalPlan,
        group_exprs: Vec<(LogicalExpr, Type)>,
        output_exprs: Vec<LogicalExpr>,
        having: Option<LogicalExpr>,
        output_schema: SchemaRef,
    ) -> Result<LogicalPlan> {
        let mut deduplicated_groups = Vec::with_capacity(group_exprs.len());
        for group in group_exprs {
            if !Self::can_share_expression(&group.0)
                || !deduplicated_groups
                    .iter()
                    .any(|(expr, _): &(LogicalExpr, Type)| expr == &group.0)
            {
                deduplicated_groups.push(group);
            }
        }
        let group_exprs = deduplicated_groups;
        let original_groups = group_exprs
            .iter()
            .map(|(expr, _)| expr.clone())
            .collect::<Vec<_>>();
        let input_schema = input.schema().clone();
        let mut pre_exprs = input_schema
            .columns
            .iter()
            .map(|column| {
                LogicalExpr::Column(Column {
                    id: column.id,
                    name: column.name.clone(),
                    table: column.table_alias.clone().or_else(|| column.table.clone()),
                })
            })
            .collect::<Vec<_>>();
        let mut pre_columns = input_schema.columns.clone();
        let original_pre_len = pre_exprs.len();

        let mut normalized_groups = Vec::with_capacity(group_exprs.len());
        for (expr, ty) in group_exprs {
            normalized_groups.push(self.precompute_expression(
                expr,
                ty,
                "group",
                &mut pre_exprs,
                &mut pre_columns,
            ));
        }

        // Assign every aggregate expression its output slot while extracting it
        // from scalar expressions. Aggregate functions belong to the Aggregate
        // node; projections and HAVING consume only the resulting columns.
        // Doing both actions in one traversal avoids relying on expression
        // equality to find the same aggregate again during a later rewrite.
        let mut aggregates = Vec::new();
        let output_exprs = output_exprs
            .into_iter()
            .map(|expr| self.extract_aggregates(expr, &mut aggregates))
            .collect::<Vec<_>>();
        let having = having.map(|expr| self.extract_aggregates(expr, &mut aggregates));

        let mut normalized_aggregates = Vec::with_capacity(aggregates.len());
        let mut aggregate_outputs = Vec::with_capacity(aggregates.len());
        for (aggregate, column) in aggregates {
            let LogicalExpr::AggregateFunction {
                fun,
                args,
                arg_types,
                distinct,
                result_type,
            } = aggregate
            else {
                unreachable!("aggregate extraction returned a non-aggregate expression")
            };
            if args.len() != arg_types.len() {
                return Err(LimboError::InternalError(
                    "semantic aggregate arguments and type facts have different lengths"
                        .to_string(),
                ));
            }
            let normalized_args = args
                .into_iter()
                .zip(arg_types.iter().copied())
                .map(|(expr, ty)| {
                    self.precompute_expression(
                        expr,
                        ty,
                        "aggregate_arg",
                        &mut pre_exprs,
                        &mut pre_columns,
                    )
                })
                .collect();
            // HIR records SQLite's semantic SUM type as NUMERIC because the
            // result can be integer or real. The current incremental
            // aggregate stores and emits its running sum as f64, so this
            // internal row slot has a concrete REAL storage type. The final
            // projection still exposes the semantic HIR output type.
            let physical_result_type = if matches!(&fun, AggFunc::Sum) {
                Type::Real
            } else {
                result_type
            };
            aggregate_outputs.push((column, physical_result_type));
            normalized_aggregates.push(LogicalExpr::AggregateFunction {
                fun,
                args: normalized_args,
                arg_types,
                distinct,
                result_type,
            });
        }

        let aggregate_input = if pre_exprs.len() == original_pre_len {
            Arc::new(input)
        } else {
            Arc::new(LogicalPlan::Projection(Projection {
                input: Arc::new(input),
                exprs: pre_exprs,
                schema: Arc::new(LogicalSchema::new(pre_columns.clone())),
            }))
        };
        let pre_schema = aggregate_input.schema().clone();

        let mut aggregate_columns = Vec::new();
        let mut group_replacements = Vec::new();
        for group in &normalized_groups {
            let LogicalExpr::Column(column) = group else {
                unreachable!("precomputed grouping expression is not a column")
            };
            let (_, info) = pre_schema.find_column_id(column.id).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "precomputed grouping column {:?} is absent from its schema",
                    column.id
                ))
            })?;
            aggregate_columns.push(info.clone());
        }
        group_replacements.extend(
            original_groups
                .into_iter()
                .zip(normalized_groups.iter().cloned()),
        );

        for (column, physical_result_type) in aggregate_outputs {
            aggregate_columns.push(ColumnInfo {
                id: column.id,
                name: column.name.clone(),
                ty: physical_result_type,
                affinity: crate::vdbe::affinity::Affinity::Blob,
                has_affinity: false,
                collation: None,
                database: None,
                table: None,
                table_alias: None,
            });
        }

        let aggregate_schema = Arc::new(LogicalSchema::new(aggregate_columns));
        let mut plan = LogicalPlan::Aggregate(Aggregate {
            input: aggregate_input,
            group_expr: normalized_groups.clone(),
            aggr_expr: normalized_aggregates,
            schema: aggregate_schema.clone(),
        });

        if let Some(having) = having {
            let predicate = Self::rewrite_groups_after_aggregate(having, &group_replacements);
            Self::require_columns_in_schema(&predicate, &aggregate_schema)?;
            plan = LogicalPlan::Filter(Filter {
                input: Arc::new(plan),
                predicate,
            });
        }

        let rewritten_outputs = output_exprs
            .into_iter()
            .map(|expr| Self::rewrite_groups_after_aggregate(expr, &group_replacements))
            .collect::<Vec<_>>();
        for expr in &rewritten_outputs {
            Self::require_columns_in_schema(expr, &aggregate_schema)?;
        }
        Ok(LogicalPlan::Projection(Projection {
            input: Arc::new(plan),
            exprs: rewritten_outputs,
            schema: output_schema,
        }))
    }

    fn precompute_expression(
        &mut self,
        expr: LogicalExpr,
        ty: Type,
        prefix: &str,
        expressions: &mut Vec<LogicalExpr>,
        columns: &mut Vec<ColumnInfo>,
    ) -> LogicalExpr {
        if matches!(&expr, LogicalExpr::Column(_)) {
            return expr;
        }
        if Self::can_share_expression(&expr) {
            if let Some(index) = expressions.iter().position(|existing| existing == &expr) {
                let column = &columns[index];
                return LogicalExpr::Column(Column {
                    id: column.id,
                    name: column.name.clone(),
                    table: column.table_alias.clone().or_else(|| column.table.clone()),
                });
            }
        }
        let column = self.synthetic_column(prefix);
        expressions.push(expr);
        columns.push(ColumnInfo {
            id: column.id,
            name: column.name.clone(),
            ty,
            affinity: crate::vdbe::affinity::Affinity::Blob,
            has_affinity: false,
            collation: None,
            database: None,
            table: None,
            table_alias: None,
        });
        LogicalExpr::Column(column)
    }

    fn synthetic_column(&mut self, prefix: &str) -> Column {
        let index = self.next_synthetic_column;
        self.next_synthetic_column += 1;
        Column {
            id: LogicalColumnId::Synthetic(index),
            name: format!("__{prefix}_{index}"),
            table: None,
        }
    }

    fn extract_aggregates(
        &mut self,
        expr: LogicalExpr,
        aggregates: &mut Vec<(LogicalExpr, Column)>,
    ) -> LogicalExpr {
        Self::rewrite_expression(expr, &mut |expr| {
            if !matches!(expr, LogicalExpr::AggregateFunction { .. }) {
                return None;
            }
            // Sharing a slot is only an optimization. If equality does not
            // recognize two equivalent aggregates, each gets a valid slot.
            if Self::can_share_expression(expr) {
                if let Some((_, column)) =
                    aggregates.iter().find(|(aggregate, _)| aggregate == expr)
                {
                    return Some(LogicalExpr::Column(column.clone()));
                }
            }
            let column = self.synthetic_column("agg");
            aggregates.push((expr.clone(), column.clone()));
            Some(LogicalExpr::Column(column))
        })
    }

    fn contains_aggregate(expr: &LogicalExpr) -> bool {
        if matches!(expr, LogicalExpr::AggregateFunction { .. }) {
            return true;
        }
        let mut found = false;
        Self::for_each_child(expr, |child| found |= Self::contains_aggregate(child));
        found
    }

    fn for_each_child(expr: &LogicalExpr, mut visit: impl FnMut(&LogicalExpr)) {
        match expr {
            LogicalExpr::BinaryExpr { left, right, .. } => {
                visit(left);
                visit(right);
            }
            LogicalExpr::UnaryExpr { expr, .. }
            | LogicalExpr::Alias { expr, .. }
            | LogicalExpr::IsNull { expr, .. }
            | LogicalExpr::Collate { expr, .. } => visit(expr),
            LogicalExpr::Cast {
                expr, parameters, ..
            } => {
                visit(expr);
                for parameter in parameters {
                    visit(parameter);
                }
            }
            LogicalExpr::AggregateFunction { args, .. }
            | LogicalExpr::ScalarFunction { args, .. } => {
                for arg in args {
                    visit(arg);
                }
            }
            LogicalExpr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                if let Some(expr) = expr {
                    visit(expr);
                }
                for (when, then) in when_then {
                    visit(when);
                    visit(then);
                }
                if let Some(expr) = else_expr {
                    visit(expr);
                }
            }
            LogicalExpr::InList { expr, list, .. } => {
                visit(expr);
                for value in list {
                    visit(value);
                }
            }
            LogicalExpr::InSubquery { expr, .. } => visit(expr),
            LogicalExpr::Between {
                expr, low, high, ..
            } => {
                visit(expr);
                visit(low);
                visit(high);
            }
            LogicalExpr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                visit(expr);
                visit(pattern);
                if let Some(escape) = escape {
                    visit(escape);
                }
            }
            LogicalExpr::Column(_)
            | LogicalExpr::Literal(_)
            | LogicalExpr::Exists { .. }
            | LogicalExpr::ScalarSubquery { .. } => {}
        }
    }

    /// Whether two structurally equal occurrences may share one evaluation.
    /// Subqueries and nondeterministic scalar calls stay occurrence-local even
    /// when their syntax is identical.
    fn can_share_expression(expr: &LogicalExpr) -> bool {
        match expr {
            LogicalExpr::ScalarFunction { function, args, .. } => {
                function.value().is_deterministic() && args.iter().all(Self::can_share_expression)
            }
            LogicalExpr::AggregateFunction { fun, args, .. } => {
                !matches!(fun, AggFunc::External(_)) && args.iter().all(Self::can_share_expression)
            }
            LogicalExpr::Like {
                function,
                expr,
                pattern,
                escape,
                ..
            } => {
                function.value().is_deterministic()
                    && Self::can_share_expression(expr)
                    && Self::can_share_expression(pattern)
                    && escape.as_deref().is_none_or(Self::can_share_expression)
            }
            LogicalExpr::Exists { .. }
            | LogicalExpr::ScalarSubquery { .. }
            | LogicalExpr::InSubquery { .. } => false,
            LogicalExpr::Column(_) | LogicalExpr::Literal(_) => true,
            _ => {
                let mut shareable = true;
                Self::for_each_child(expr, |child| {
                    shareable &= Self::can_share_expression(child);
                });
                shareable
            }
        }
    }

    fn rewrite_groups_after_aggregate(
        expr: LogicalExpr,
        groups: &[(LogicalExpr, LogicalExpr)],
    ) -> LogicalExpr {
        Self::rewrite_expression(expr, &mut |expr| {
            groups
                .iter()
                .find(|(value, _)| Self::can_share_expression(value) && value == expr)
                .map(|(_, replacement)| replacement.clone())
        })
    }

    fn rewrite_expression(
        expr: LogicalExpr,
        rewrite: &mut impl FnMut(&LogicalExpr) -> Option<LogicalExpr>,
    ) -> LogicalExpr {
        if let Some(replacement) = rewrite(&expr) {
            return replacement;
        }
        match expr {
            LogicalExpr::BinaryExpr { left, op, right } => LogicalExpr::BinaryExpr {
                left: Box::new(Self::rewrite_expression(*left, rewrite)),
                op,
                right: Box::new(Self::rewrite_expression(*right, rewrite)),
            },
            LogicalExpr::UnaryExpr { op, expr } => LogicalExpr::UnaryExpr {
                op,
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
            },
            LogicalExpr::AggregateFunction {
                fun,
                args,
                arg_types,
                distinct,
                result_type,
            } => LogicalExpr::AggregateFunction {
                fun,
                args: args
                    .into_iter()
                    .map(|arg| Self::rewrite_expression(arg, rewrite))
                    .collect(),
                arg_types,
                distinct,
                result_type,
            },
            LogicalExpr::ScalarFunction {
                function,
                args,
                result_type,
            } => LogicalExpr::ScalarFunction {
                function,
                args: args
                    .into_iter()
                    .map(|arg| Self::rewrite_expression(arg, rewrite))
                    .collect(),
                result_type,
            },
            LogicalExpr::Case {
                expr,
                when_then,
                else_expr,
            } => LogicalExpr::Case {
                expr: expr.map(|expr| Box::new(Self::rewrite_expression(*expr, rewrite))),
                when_then: when_then
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            Self::rewrite_expression(when, rewrite),
                            Self::rewrite_expression(then, rewrite),
                        )
                    })
                    .collect(),
                else_expr: else_expr.map(|expr| Box::new(Self::rewrite_expression(*expr, rewrite))),
            },
            LogicalExpr::InList {
                expr,
                list,
                negated,
            } => LogicalExpr::InList {
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
                list: list
                    .into_iter()
                    .map(|expr| Self::rewrite_expression(expr, rewrite))
                    .collect(),
                negated,
            },
            LogicalExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => LogicalExpr::InSubquery {
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
                subquery,
                negated,
            },
            LogicalExpr::Alias { expr, alias } => LogicalExpr::Alias {
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
                alias,
            },
            LogicalExpr::IsNull { expr, negated } => LogicalExpr::IsNull {
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
                negated,
            },
            LogicalExpr::Between {
                expr,
                low,
                high,
                negated,
            } => LogicalExpr::Between {
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
                low: Box::new(Self::rewrite_expression(*low, rewrite)),
                high: Box::new(Self::rewrite_expression(*high, rewrite)),
                negated,
            },
            LogicalExpr::Like {
                expr,
                pattern,
                escape,
                negated,
                operator,
                function,
                argument_count,
            } => LogicalExpr::Like {
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
                pattern: Box::new(Self::rewrite_expression(*pattern, rewrite)),
                escape: escape.map(|expr| Box::new(Self::rewrite_expression(*expr, rewrite))),
                negated,
                operator,
                function,
                argument_count,
            },
            LogicalExpr::Cast {
                expr,
                name,
                parameters,
                ty,
            } => LogicalExpr::Cast {
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
                name,
                parameters: parameters
                    .into_iter()
                    .map(|parameter| Self::rewrite_expression(parameter, rewrite))
                    .collect(),
                ty,
            },
            LogicalExpr::Collate { expr, collation } => LogicalExpr::Collate {
                expr: Box::new(Self::rewrite_expression(*expr, rewrite)),
                collation,
            },
            LogicalExpr::Column(column) => LogicalExpr::Column(column),
            LogicalExpr::Literal(value) => LogicalExpr::Literal(value),
            LogicalExpr::Exists { subquery, negated } => LogicalExpr::Exists { subquery, negated },
            LogicalExpr::ScalarSubquery { plan, output } => {
                LogicalExpr::ScalarSubquery { plan, output }
            }
        }
    }

    fn require_columns_in_schema(expr: &LogicalExpr, schema: &LogicalSchema) -> Result<()> {
        if let LogicalExpr::Column(column) = expr {
            if schema.find_column_id(column.id).is_none() {
                return Err(LimboError::ParseError(format!(
                    "column '{}' is neither grouped nor aggregated",
                    column.name
                )));
            }
            return Ok(());
        }
        let mut result = Ok(());
        Self::for_each_child(expr, |child| {
            if result.is_ok() {
                result = Self::require_columns_in_schema(child, schema);
            }
        });
        result
    }

    fn build_expr(&mut self, expr: &hir::Expr) -> Result<LogicalExpr> {
        match expr {
            hir::Expr::Literal(literal) => Ok(LogicalExpr::Literal(Self::literal_value(literal)?)),
            hir::Expr::Column(column) => self.column_for_ref(*column),
            hir::Expr::MergedColumn(column) => {
                let left = self.build_expr(&column.left)?;
                let right = self.column_for_ref(column.right)?;
                match column.value {
                    hir::MergedColumnValue::Left => Ok(left),
                    hir::MergedColumnValue::Right => Ok(right),
                    hir::MergedColumnValue::Coalesce => Ok(LogicalExpr::Case {
                        expr: None,
                        when_then: vec![(
                            LogicalExpr::IsNull {
                                expr: Box::new(left.clone()),
                                negated: true,
                            },
                            left,
                        )],
                        else_expr: Some(Box::new(right)),
                    }),
                }
            }
            hir::Expr::Output(output) => {
                let output = self.document.output(*output).cloned().ok_or_else(|| {
                    LimboError::InternalError(format!("semantic HIR has no output {output:?}"))
                })?;
                self.build_expr(&output.expr)
            }
            hir::Expr::Unary { operator, expr } => Ok(LogicalExpr::UnaryExpr {
                op: *operator,
                expr: Box::new(self.build_expr(expr)?),
            }),
            hir::Expr::Binary {
                lhs,
                operator,
                rhs,
                custom,
            } => {
                if custom.is_some() {
                    return Err(LimboError::ParseError(
                        "custom-type operators are not supported by incremental views".to_string(),
                    ));
                }
                Ok(LogicalExpr::BinaryExpr {
                    left: Box::new(self.build_expr(lhs)?),
                    op: *operator,
                    right: Box::new(self.build_expr(rhs)?),
                })
            }
            hir::Expr::Between {
                expr,
                negated,
                start,
                end,
            } => Ok(LogicalExpr::Between {
                expr: Box::new(self.build_expr(expr)?),
                low: Box::new(self.build_expr(start)?),
                high: Box::new(self.build_expr(end)?),
                negated: *negated,
            }),
            hir::Expr::Case {
                base,
                when_then,
                else_expr,
            } => Ok(LogicalExpr::Case {
                expr: base
                    .as_ref()
                    .map(|expr| self.build_expr(expr).map(Box::new))
                    .transpose()?,
                when_then: when_then
                    .iter()
                    .map(|(when, then)| Ok((self.build_expr(when)?, self.build_expr(then)?)))
                    .collect::<Result<Vec<_>>>()?,
                else_expr: else_expr
                    .as_ref()
                    .map(|expr| self.build_expr(expr).map(Box::new))
                    .transpose()?,
            }),
            hir::Expr::Cast { expr, target } => {
                if target.array_dimensions > 0 {
                    return Err(LimboError::ParseError(format!(
                        "cast target '{}' is not supported by incremental views",
                        target.name
                    )));
                }
                let ty = self.finalize_type_fact(&target.type_fact, "CAST result")?;
                Ok(LogicalExpr::Cast {
                    expr: Box::new(self.build_expr(expr)?),
                    name: target.name.clone(),
                    parameters: target
                        .parameters
                        .iter()
                        .map(|parameter| self.build_expr(parameter))
                        .collect::<Result<Vec<_>>>()?,
                    ty,
                })
            }
            hir::Expr::Collate { expr, collation } => Ok(LogicalExpr::Collate {
                expr: Box::new(self.build_expr(expr)?),
                collation: collation.clone(),
            }),
            hir::Expr::Function(call) => self.build_function(call),
            hir::Expr::IsNull(expr) => Ok(LogicalExpr::IsNull {
                expr: Box::new(self.build_expr(expr)?),
                negated: false,
            }),
            hir::Expr::NotNull(expr) => Ok(LogicalExpr::IsNull {
                expr: Box::new(self.build_expr(expr)?),
                negated: true,
            }),
            hir::Expr::InList {
                lhs,
                negated,
                values,
            } => Ok(LogicalExpr::InList {
                expr: Box::new(self.build_expr(lhs)?),
                list: values
                    .iter()
                    .map(|expr| self.build_expr(expr))
                    .collect::<Result<Vec<_>>>()?,
                negated: *negated,
            }),
            hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }) => {
                Ok(LogicalExpr::ScalarSubquery {
                    plan: self.build_query_shared(*query)?,
                    output: *output,
                })
            }
            hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)) => Ok(LogicalExpr::Exists {
                subquery: self.build_query_shared(*query)?,
                negated: false,
            }),
            hir::Expr::Subquery(hir::SubqueryExpr::In {
                lhs,
                query,
                negated,
            }) => Ok(LogicalExpr::InSubquery {
                expr: Box::new(self.build_expr(lhs)?),
                subquery: self.build_query_shared(*query)?,
                negated: *negated,
            }),
            hir::Expr::Like {
                lhs,
                negated,
                operator,
                function,
                argument_count,
                rhs,
                escape,
            } => Ok(LogicalExpr::Like {
                expr: Box::new(self.build_expr(lhs)?),
                pattern: Box::new(self.build_expr(rhs)?),
                escape: escape
                    .as_ref()
                    .map(|expr| self.build_expr(expr).map(Box::new))
                    .transpose()?,
                negated: *negated,
                operator: *operator,
                function: function.clone(),
                argument_count: *argument_count,
            }),
            hir::Expr::Parameter(_) => Err(LimboError::ParseError(
                "parameters are not supported in incremental view definitions".to_string(),
            )),
            hir::Expr::RowId(_) => Err(LimboError::ParseError(
                "rowid expressions are not supported by incremental views".to_string(),
            )),
            hir::Expr::Row(_)
            | hir::Expr::Array(_)
            | hir::Expr::Subscript { .. }
            | hir::Expr::FieldAccess(_)
            | hir::Expr::Raise { .. } => Err(LimboError::ParseError(format!(
                "expression is not supported by incremental views: {expr:?}"
            ))),
        }
    }

    fn build_function(&mut self, call: &hir::FunctionCall) -> Result<LogicalExpr> {
        if !call.argument_order.is_empty()
            || !call.within_group.is_empty()
            || call.filter.is_some()
            || call.window.is_some()
            || call.custom_type_operation.is_some()
            || call.sequence_operation.is_some()
        {
            return Err(LimboError::ParseError(format!(
                "function '{}' uses syntax not supported by incremental views",
                call.function.value()
            )));
        }
        let args = call
            .arguments
            .iter()
            .map(|expr| self.build_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let result_type = self.finalize_type_fact(&call.result_type, "function result")?;
        match call.function.value() {
            Func::Agg(fun) => {
                let arg_types = call
                    .arguments
                    .iter()
                    .map(|expr| {
                        self.finalize_type_fact(
                            &self.expression_type_fact(expr),
                            "aggregate argument",
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalExpr::AggregateFunction {
                    fun: fun.clone(),
                    args,
                    arg_types,
                    distinct: matches!(call.distinctness, Some(ast::Distinctness::Distinct)),
                    result_type,
                })
            }
            Func::Window(_) => Err(LimboError::ParseError(
                "window functions are not supported by incremental views".to_string(),
            )),
            _ => Ok(LogicalExpr::ScalarFunction {
                function: call.function.clone(),
                args,
                result_type,
            }),
        }
    }

    fn column_for_ref(&self, reference: hir::ColumnRef) -> Result<LogicalExpr> {
        let source = self.document.source(reference.source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "semantic HIR column refers to missing source {}",
                reference.source
            ))
        })?;
        let column = source.columns.get(reference.column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "semantic HIR column {} is outside source {}",
                reference.column, reference.source
            ))
        })?;
        Ok(LogicalExpr::Column(Column {
            id: LogicalColumnId::Source(reference),
            name: column.name.clone(),
            table: source.alias.clone().or_else(|| Some(source.name.clone())),
        }))
    }

    fn output_column(&self, output_id: OutputId) -> Result<LogicalExpr> {
        let output = self.document.output(output_id).ok_or_else(|| {
            LimboError::InternalError(format!("semantic HIR has no output {output_id:?}"))
        })?;
        Ok(LogicalExpr::Column(Column {
            id: LogicalColumnId::Output(output_id),
            name: output.name.clone(),
            table: None,
        }))
    }

    fn schema_from_source(&self, source: &hir::Source) -> Result<SchemaRef> {
        source
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Ok(ColumnInfo {
                    id: LogicalColumnId::Source(hir::ColumnRef {
                        source: source.id,
                        column: index,
                    }),
                    name: column.name.clone(),
                    ty: self.finalize_type_fact(
                        &column.type_fact,
                        &format!("column '{}.{}'", source.name, column.name),
                    )?,
                    affinity: column.affinity,
                    has_affinity: column.has_affinity,
                    collation: column.collation.clone(),
                    database: None,
                    table: Some(source.name.clone()),
                    table_alias: source.alias.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(LogicalSchema::new)
            .map(Arc::new)
    }

    fn schema_from_outputs(&self, outputs: &[hir::Output]) -> Result<SchemaRef> {
        outputs
            .iter()
            .map(|output| {
                Ok(ColumnInfo {
                    id: LogicalColumnId::Output(output.id),
                    name: output.name.clone(),
                    ty: self.finalize_type_fact(
                        &output.type_fact,
                        &format!("output '{}'", output.name),
                    )?,
                    affinity: output.affinity,
                    has_affinity: output.has_affinity,
                    collation: output.collation.clone(),
                    database: None,
                    table: None,
                    table_alias: None,
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(LogicalSchema::new)
            .map(Arc::new)
    }

    fn schema_from_output_ids(&self, outputs: &[OutputId]) -> Result<SchemaRef> {
        outputs
            .iter()
            .map(|output| {
                self.document.output(*output).cloned().ok_or_else(|| {
                    LimboError::InternalError(format!("semantic HIR has no output {output:?}"))
                })
            })
            .collect::<Result<Vec<_>>>()
            .and_then(|outputs| self.schema_from_outputs(&outputs))
    }

    fn finalize_type_fact(&self, fact: &hir::TypeFact, context: &str) -> Result<Type> {
        if fact.is_array() {
            return Err(LimboError::ParseError(format!(
                "{context} has an array type, which incremental views do not support"
            )));
        }
        if let Some(declared) = &fact.declared {
            if declared.custom().is_some() {
                return Err(LimboError::ParseError(format!(
                    "{context} has custom type '{}', which incremental views do not support",
                    declared.name
                )));
            }
        }
        fact.storage.ok_or_else(|| {
            LimboError::ParseError(format!(
                "{context} has a dynamic type; incremental views require a concrete storage type"
            ))
        })
    }

    fn expression_type_fact(&self, expr: &hir::Expr) -> hir::TypeFact {
        match expr {
            hir::Expr::Literal(literal) => match literal {
                ast::Literal::Numeric(value)
                    if value
                        .as_bytes()
                        .iter()
                        .any(|byte| matches!(byte, b'.' | b'e' | b'E')) =>
                {
                    hir::TypeFact::known(Type::Real)
                }
                ast::Literal::Numeric(_) | ast::Literal::True | ast::Literal::False => {
                    hir::TypeFact::known(Type::Integer)
                }
                ast::Literal::String(_)
                | ast::Literal::Keyword(_)
                | ast::Literal::CurrentDate
                | ast::Literal::CurrentTime
                | ast::Literal::CurrentTimestamp => hir::TypeFact::known(Type::Text),
                ast::Literal::Blob(_) => hir::TypeFact::known(Type::Blob),
                ast::Literal::Null => hir::TypeFact::known(Type::Null),
            },
            hir::Expr::Parameter(parameter) => parameter.type_fact.clone(),
            hir::Expr::Column(reference) => self
                .document
                .source(reference.source)
                .and_then(|source| source.columns.get(reference.column))
                .map(|column| column.type_fact.clone())
                .unwrap_or_default(),
            hir::Expr::MergedColumn(column) => column.type_fact.clone(),
            hir::Expr::RowId(_) => hir::TypeFact::known(Type::Integer),
            hir::Expr::Output(output) => self
                .document
                .output(*output)
                .map(|output| output.type_fact.clone())
                .unwrap_or_default(),
            hir::Expr::Cast { target, .. } => target.type_fact.clone(),
            hir::Expr::Function(call) => call.result_type.clone(),
            hir::Expr::FieldAccess(access) => access.result_type.clone(),
            hir::Expr::Collate { expr, .. } => self.expression_type_fact(expr),
            hir::Expr::Unary {
                operator: ast::UnaryOperator::Not | ast::UnaryOperator::BitwiseNot,
                ..
            } => hir::TypeFact::known(Type::Integer),
            hir::Expr::Unary { expr, .. } => self.expression_type_fact(expr),
            hir::Expr::IsNull(_)
            | hir::Expr::NotNull(_)
            | hir::Expr::Like { .. }
            | hir::Expr::Between { .. }
            | hir::Expr::InList { .. }
            | hir::Expr::Subquery(hir::SubqueryExpr::Exists(_))
            | hir::Expr::Subquery(hir::SubqueryExpr::In { .. }) => {
                hir::TypeFact::known(Type::Integer)
            }
            hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }) => self
                .document
                .query(*query)
                .and_then(|query| query.output.get(*output))
                .and_then(|output| self.document.output(*output))
                .map(|output| output.type_fact.clone())
                .unwrap_or_default(),
            hir::Expr::Array(elements) => hir::TypeFact::array_literal_result(
                elements
                    .iter()
                    .map(|element| self.expression_type_fact(element)),
            ),
            hir::Expr::Subscript { base, .. } => {
                let mut fact = self.expression_type_fact(base);
                if !fact.is_array() {
                    return hir::TypeFact::dynamic();
                }
                fact.array_dimensions = fact.array_dimensions.saturating_sub(1);
                if let Some(declared) = fact.declared.as_mut() {
                    declared.array_dimensions = declared.array_dimensions.saturating_sub(1);
                    declared.storage = if declared.array_dimensions == 0 {
                        match Affinity::affinity(&declared.name) {
                            Affinity::Integer => Type::Integer,
                            Affinity::Text => Type::Text,
                            Affinity::Blob => Type::Blob,
                            Affinity::Real => Type::Real,
                            Affinity::Numeric => Type::Numeric,
                        }
                    } else {
                        Type::Blob
                    };
                    fact.storage = Some(declared.storage);
                } else if fact.is_array() {
                    fact.storage = Some(Type::Blob);
                } else {
                    fact.storage = None;
                }
                fact
            }
            hir::Expr::Binary {
                lhs,
                operator:
                    ast::Operator::Add
                    | ast::Operator::Subtract
                    | ast::Operator::Multiply
                    | ast::Operator::Divide,
                rhs,
                ..
            } => hir::TypeFact::arithmetic_result(
                &self.expression_type_fact(lhs),
                &self.expression_type_fact(rhs),
            ),
            hir::Expr::Binary {
                operator:
                    ast::Operator::Modulus
                    | ast::Operator::BitwiseAnd
                    | ast::Operator::BitwiseOr
                    | ast::Operator::LeftShift
                    | ast::Operator::RightShift,
                ..
            } => hir::TypeFact::known(Type::Integer),
            hir::Expr::Binary {
                lhs,
                operator: ast::Operator::Concat,
                rhs,
                ..
            } => hir::TypeFact::concat_result(
                &self.expression_type_fact(lhs),
                &self.expression_type_fact(rhs),
            ),
            hir::Expr::Binary {
                operator: ast::Operator::ArrowRight,
                ..
            } => hir::TypeFact::known(Type::Text),
            hir::Expr::Binary { operator, .. }
                if matches!(
                    operator,
                    ast::Operator::And
                        | ast::Operator::Or
                        | ast::Operator::Equals
                        | ast::Operator::NotEquals
                        | ast::Operator::Less
                        | ast::Operator::LessEquals
                        | ast::Operator::Greater
                        | ast::Operator::GreaterEquals
                        | ast::Operator::Is
                        | ast::Operator::IsNot
                ) =>
            {
                hir::TypeFact::known(Type::Integer)
            }
            hir::Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                let mut results = Vec::with_capacity(when_then.len() + else_expr.iter().count());
                results.extend(
                    when_then
                        .iter()
                        .map(|(_, result)| self.expression_type_fact(result)),
                );
                if let Some(else_expr) = else_expr {
                    results.push(self.expression_type_fact(else_expr));
                }
                hir::TypeFact::selected_value_result(&results)
            }
            _ => hir::TypeFact::dynamic(),
        }
    }

    fn literal_value(literal: &ast::Literal) -> Result<Value> {
        match literal {
            ast::Literal::Null => Ok(Value::Null),
            ast::Literal::True => Ok(Value::from_i64(1)),
            ast::Literal::False => Ok(Value::from_i64(0)),
            ast::Literal::Numeric(value) => crate::util::parse_numeric_literal(value),
            ast::Literal::String(value) => Ok(Value::Text(
                crate::translate::expr::sanitize_string(value).into(),
            )),
            ast::Literal::Blob(value) => {
                let bytes = ast::blob_literal_hex(value)
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| {
                        let pair = std::str::from_utf8(pair).map_err(|error| {
                            LimboError::ParseError(format!("invalid blob literal: {error}"))
                        })?;
                        u8::from_str_radix(pair, 16).map_err(|error| {
                            LimboError::ParseError(format!("invalid blob literal: {error}"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Value::from_slice(&bytes)?)
            }
            ast::Literal::Keyword(value) => Ok(Value::Text(value.clone().into())),
            ast::Literal::CurrentDate
            | ast::Literal::CurrentTime
            | ast::Literal::CurrentTimestamp => Err(LimboError::ParseError(
                "current-time literals are not supported by incremental views".to_string(),
            )),
        }
    }

    fn literal_limit(expr: &hir::Expr, context: &str) -> Result<usize> {
        let value = match expr {
            hir::Expr::Literal(ast::Literal::Numeric(value)) => value.parse::<i64>().ok(),
            hir::Expr::Unary {
                operator: ast::UnaryOperator::Positive,
                expr,
            } => match expr.as_ref() {
                hir::Expr::Literal(ast::Literal::Numeric(value)) => value.parse::<i64>().ok(),
                _ => None,
            },
            _ => None,
        }
        .ok_or_else(|| {
            LimboError::ParseError(format!(
                "incremental views require {context} to be a non-negative integer literal"
            ))
        })?;
        usize::try_from(value).map_err(|_| {
            LimboError::ParseError(format!(
                "incremental views require {context} to be a non-negative integer literal"
            ))
        })
    }
}
