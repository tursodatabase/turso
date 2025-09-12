//! Logical plan representation for SQL queries
//!
//! This module provides a platform-independent intermediate representation
//! for SQL queries. The logical plan is a DAG (Directed Acyclic Graph) that
//! supports CTEs and can be used for query optimization before being compiled
//! to an execution plan (e.g., DBSP circuits).
//!
//! The main entry point is `LogicalPlanBuilder` which constructs logical plans
//! from SQL AST nodes.
use crate::function::AggFunc;
use crate::schema::{Schema, Type};
use crate::types::Value;
use crate::{LimboError, Result};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use turso_macros::match_ignore_ascii_case;
use turso_parser::ast;

/// Result type for preprocessing aggregate expressions
type PreprocessAggregateResult = (
    bool,                // needs_pre_projection
    Vec<LogicalExpr>,    // pre_projection_exprs
    Vec<(String, Type)>, // pre_projection_schema
    Vec<LogicalExpr>,    // modified_aggr_exprs
);

/// Schema information for logical plan nodes
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalSchema {
    /// Column names and types
    pub columns: Vec<(String, Type)>,
}
/// A reference to a schema that can be shared between nodes
pub type SchemaRef = Arc<LogicalSchema>;

impl LogicalSchema {
    pub fn new(columns: Vec<(String, Type)>) -> Self {
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

    pub fn find_column(&self, name: &str) -> Option<(usize, &Type)> {
        self.columns
            .iter()
            .position(|(n, _)| n == name)
            .map(|idx| (idx, &self.columns[idx].1))
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
    // TODO: Join - combining two relations (not yet implemented)
    // Join(Join),
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
            // LogicalPlan::Join(j) => &j.schema,
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

// TODO: Join operator (not yet implemented)
// #[derive(Debug, Clone, PartialEq)]
// pub struct Join {
//     pub left: Arc<LogicalPlan>,
//     pub right: Arc<LogicalPlan>,
//     pub join_type: JoinType,
//     pub on: Vec<(LogicalExpr, LogicalExpr)>, // Equijoin conditions
//     pub filter: Option<LogicalExpr>,         // Additional filter conditions
//     pub schema: SchemaRef,
// }

// TODO: Types of joins (not yet implemented)
// #[derive(Debug, Clone, Copy, PartialEq)]
// pub enum JoinType {
//     Inner,
//     Left,
//     Right,
//     Full,
//     Cross,
// }

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
    pub ctes: HashMap<String, Arc<LogicalPlan>>,
    pub body: Arc<LogicalPlan>,
}

/// Reference to a CTE
#[derive(Debug, Clone, PartialEq)]
pub struct CTERef {
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
        distinct: bool,
    },
    /// Scalar function call
    ScalarFunction { fun: String, args: Vec<LogicalExpr> },
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
    ScalarSubquery(Arc<LogicalPlan>),
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
        escape: Option<char>,
        negated: bool,
    },
}

/// Column reference
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub table: Option<String>,
}

impl Column {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: None,
        }
    }

    pub fn with_table(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: Some(table.into()),
        }
    }
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

/// Compiler from AST to LogicalPlan
pub struct LogicalPlanBuilder<'a> {
    schema: &'a Schema,
    ctes: HashMap<String, Arc<LogicalPlan>>,
}

impl<'a> LogicalPlanBuilder<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            ctes: HashMap::new(),
        }
    }

    /// Main entry point: compile a statement to a logical plan
    pub fn build_statement(&mut self, stmt: &ast::Stmt) -> Result<LogicalPlan> {
        match stmt {
            ast::Stmt::Select(select) => self.build_select(select),
            _ => Err(LimboError::ParseError(
                "Only SELECT statements are currently supported in logical plans".to_string(),
            )),
        }
    }

    // Convert Name to String
    fn name_to_string(name: &ast::Name) -> String {
        match name {
            ast::Name::Ident(s) | ast::Name::Quoted(s) => s.clone(),
        }
    }

    // Build a SELECT statement
    // Build a logical plan from a SELECT statement
    fn build_select(&mut self, select: &ast::Select) -> Result<LogicalPlan> {
        // Handle WITH clause if present
        if let Some(with) = &select.with {
            return self.build_with_cte(with, select);
        }

        // Build the main query body
        let order_by = &select.order_by;
        let limit = &select.limit;
        self.build_select_body(&select.body, order_by, limit)
    }

    // Build WITH CTE
    fn build_with_cte(&mut self, with: &ast::With, select: &ast::Select) -> Result<LogicalPlan> {
        let mut cte_plans = HashMap::new();

        // Build each CTE
        for cte in &with.ctes {
            let cte_plan = self.build_select(&cte.select)?;
            let cte_name = Self::name_to_string(&cte.tbl_name);
            cte_plans.insert(cte_name.clone(), Arc::new(cte_plan));
            self.ctes
                .insert(cte_name.clone(), cte_plans[&cte_name].clone());
        }

        // Build the main body with CTEs available
        let order_by = &select.order_by;
        let limit = &select.limit;
        let body = self.build_select_body(&select.body, order_by, limit)?;

        // Clear CTEs from builder context
        for cte in &with.ctes {
            self.ctes.remove(&Self::name_to_string(&cte.tbl_name));
        }

        Ok(LogicalPlan::WithCTE(WithCTE {
            ctes: cte_plans,
            body: Arc::new(body),
        }))
    }

    // Build SELECT body
    fn build_select_body(
        &mut self,
        body: &ast::SelectBody,
        order_by: &[ast::SortedColumn],
        limit: &Option<ast::Limit>,
    ) -> Result<LogicalPlan> {
        let mut plan = self.build_one_select(&body.select)?;

        // Handle compound operators (UNION, INTERSECT, EXCEPT)
        if !body.compounds.is_empty() {
            for compound in &body.compounds {
                let right = self.build_one_select(&compound.select)?;
                plan = Self::build_compound(plan, right, &compound.operator)?;
            }
        }

        // Apply ORDER BY
        if !order_by.is_empty() {
            plan = self.build_sort(plan, order_by)?;
        }

        // Apply LIMIT
        if let Some(limit) = limit {
            plan = Self::build_limit(plan, limit)?;
        }

        Ok(plan)
    }

    // Build a single SELECT (without compounds)
    fn build_one_select(&mut self, select: &ast::OneSelect) -> Result<LogicalPlan> {
        match select {
            ast::OneSelect::Select {
                distinctness,
                columns,
                from,
                where_clause,
                group_by,
                window_clause: _,
            } => {
                // Start with FROM clause
                let mut plan = if let Some(from) = from {
                    self.build_from(from)?
                } else {
                    // No FROM clause - single row
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(LogicalSchema::empty()),
                    })
                };

                // Apply WHERE
                if let Some(where_expr) = where_clause {
                    let predicate = self.build_expr(where_expr, plan.schema())?;
                    plan = LogicalPlan::Filter(Filter {
                        input: Arc::new(plan),
                        predicate,
                    });
                }

                // Apply GROUP BY and aggregations
                if let Some(group_by) = group_by {
                    plan = self.build_aggregate(plan, group_by, columns)?;
                } else if Self::has_aggregates(columns) {
                    // Aggregation without GROUP BY
                    plan = self.build_aggregate_no_group(plan, columns)?;
                } else {
                    // Regular projection
                    plan = self.build_projection(plan, columns)?;
                }

                // Apply HAVING (part of GROUP BY)
                if let Some(ref group_by) = group_by {
                    if let Some(ref having_expr) = group_by.having {
                        let predicate = self.build_expr(having_expr, plan.schema())?;
                        plan = LogicalPlan::Filter(Filter {
                            input: Arc::new(plan),
                            predicate,
                        });
                    }
                }

                // Apply DISTINCT
                if distinctness.is_some() {
                    plan = LogicalPlan::Distinct(Distinct {
                        input: Arc::new(plan),
                    });
                }

                Ok(plan)
            }
            ast::OneSelect::Values(values) => self.build_values(values),
        }
    }

    // Build FROM clause
    fn build_from(&mut self, from: &ast::FromClause) -> Result<LogicalPlan> {
        let mut plan = { self.build_select_table(&from.select)? };

        // Handle JOINs
        if !from.joins.is_empty() {
            for join in &from.joins {
                let right = self.build_select_table(&join.table)?;
                plan = self.build_join(plan, right, &join.operator, &join.constraint)?;
            }
        }

        Ok(plan)
    }

    // Build a table reference
    fn build_select_table(&mut self, table: &ast::SelectTable) -> Result<LogicalPlan> {
        match table {
            ast::SelectTable::Table(name, alias, _indexed) => {
                let table_name = Self::name_to_string(&name.name);
                // Check if it's a CTE reference
                if let Some(cte_plan) = self.ctes.get(&table_name) {
                    return Ok(LogicalPlan::CTERef(CTERef {
                        name: table_name.clone(),
                        schema: cte_plan.schema().clone(),
                    }));
                }

                // Regular table scan
                let table_schema = self.get_table_schema(&table_name)?;
                let table_alias = alias.as_ref().map(|a| match a {
                    ast::As::As(name) => Self::name_to_string(name),
                    ast::As::Elided(name) => Self::name_to_string(name),
                });
                Ok(LogicalPlan::TableScan(TableScan {
                    table_name,
                    alias: table_alias,
                    schema: table_schema,
                    projection: None,
                }))
            }
            ast::SelectTable::Select(subquery, _alias) => self.build_select(subquery),
            ast::SelectTable::TableCall(_, _, _) => Err(LimboError::ParseError(
                "Table-valued functions are not supported in logical plans".to_string(),
            )),
            ast::SelectTable::Sub(_, _) => Err(LimboError::ParseError(
                "Subquery in FROM clause not yet supported".to_string(),
            )),
        }
    }

    // Build JOIN
    fn build_join(
        &mut self,
        _left: LogicalPlan,
        _right: LogicalPlan,
        _op: &ast::JoinOperator,
        _constraint: &Option<ast::JoinConstraint>,
    ) -> Result<LogicalPlan> {
        Err(LimboError::ParseError(
            "JOINs are not yet supported in logical plans".to_string(),
        ))
    }

    // Build projection
    fn build_projection(
        &mut self,
        input: LogicalPlan,
        columns: &[ast::ResultColumn],
    ) -> Result<LogicalPlan> {
        let input_schema = input.schema();
        let mut proj_exprs = Vec::new();
        let mut schema_columns = Vec::new();

        for col in columns {
            match col {
                ast::ResultColumn::Expr(expr, alias) => {
                    let logical_expr = self.build_expr(expr, input_schema)?;
                    let col_name = match alias {
                        Some(as_alias) => match as_alias {
                            ast::As::As(name) | ast::As::Elided(name) => Self::name_to_string(name),
                        },
                        None => Self::expr_to_column_name(expr),
                    };
                    let col_type = Self::infer_expr_type(&logical_expr, input_schema)?;

                    schema_columns.push((col_name.clone(), col_type));

                    if let Some(as_alias) = alias {
                        let alias_name = match as_alias {
                            ast::As::As(name) | ast::As::Elided(name) => Self::name_to_string(name),
                        };
                        proj_exprs.push(LogicalExpr::Alias {
                            expr: Box::new(logical_expr),
                            alias: alias_name,
                        });
                    } else {
                        proj_exprs.push(logical_expr);
                    }
                }
                ast::ResultColumn::Star => {
                    // Expand * to all columns
                    for (name, typ) in &input_schema.columns {
                        proj_exprs.push(LogicalExpr::Column(Column::new(name.clone())));
                        schema_columns.push((name.clone(), *typ));
                    }
                }
                ast::ResultColumn::TableStar(table) => {
                    // Expand table.* to all columns from that table
                    let table_name = Self::name_to_string(table);
                    for (name, typ) in &input_schema.columns {
                        // Simple check - would need proper table tracking in real implementation
                        proj_exprs.push(LogicalExpr::Column(Column::with_table(
                            name.clone(),
                            table_name.clone(),
                        )));
                        schema_columns.push((name.clone(), *typ));
                    }
                }
            }
        }

        Ok(LogicalPlan::Projection(Projection {
            input: Arc::new(input),
            exprs: proj_exprs,
            schema: Arc::new(LogicalSchema::new(schema_columns)),
        }))
    }

    // Helper function to preprocess aggregate expressions that contain complex arguments
    // Returns: (needs_pre_projection, pre_projection_exprs, pre_projection_schema, modified_aggr_exprs)
    //
    // This will be used in expressions like select sum(hex(a + 2)) from tbl => hex(a + 2) is a
    // pre-projection.
    //
    // Another alternative is to always generate a projection together with an aggregation, and
    // just have "a" be the identity projection if we don't have a complex case. But that's quite
    // wasteful.
    fn preprocess_aggregate_expressions(
        aggr_exprs: &[LogicalExpr],
        group_exprs: &[LogicalExpr],
        input_schema: &SchemaRef,
    ) -> Result<PreprocessAggregateResult> {
        let mut needs_pre_projection = false;
        let mut pre_projection_exprs = Vec::new();
        let mut pre_projection_schema = Vec::new();
        let mut modified_aggr_exprs = Vec::new();
        let mut projected_col_counter = 0;

        // First, add all group by expressions to the pre-projection
        for expr in group_exprs {
            if let LogicalExpr::Column(col) = expr {
                pre_projection_exprs.push(expr.clone());
                let col_type = Self::infer_expr_type(expr, input_schema)?;
                pre_projection_schema.push((col.name.clone(), col_type));
            } else {
                // Complex group by expression - project it
                needs_pre_projection = true;
                let proj_col_name = format!("__group_proj_{projected_col_counter}");
                projected_col_counter += 1;
                pre_projection_exprs.push(expr.clone());
                let col_type = Self::infer_expr_type(expr, input_schema)?;
                pre_projection_schema.push((proj_col_name.clone(), col_type));
            }
        }

        // Check each aggregate expression
        for agg_expr in aggr_exprs {
            if let LogicalExpr::AggregateFunction {
                fun,
                args,
                distinct,
            } = agg_expr
            {
                let mut modified_args = Vec::new();
                for arg in args {
                    // Check if the argument is a simple column reference or a complex expression
                    match arg {
                        LogicalExpr::Column(_) => {
                            // Simple column - just use it
                            modified_args.push(arg.clone());
                            // Make sure the column is in the pre-projection
                            if !pre_projection_exprs.iter().any(|e| e == arg) {
                                pre_projection_exprs.push(arg.clone());
                                let col_type = Self::infer_expr_type(arg, input_schema)?;
                                if let LogicalExpr::Column(col) = arg {
                                    pre_projection_schema.push((col.name.clone(), col_type));
                                }
                            }
                        }
                        _ => {
                            // Complex expression - we need to project it first
                            needs_pre_projection = true;
                            let proj_col_name = format!("__agg_arg_proj_{projected_col_counter}");
                            projected_col_counter += 1;

                            // Add the expression to the pre-projection
                            pre_projection_exprs.push(arg.clone());
                            let col_type = Self::infer_expr_type(arg, input_schema)?;
                            pre_projection_schema.push((proj_col_name.clone(), col_type));

                            // In the aggregate, reference the projected column
                            modified_args.push(LogicalExpr::Column(Column::new(proj_col_name)));
                        }
                    }
                }

                // Create the modified aggregate expression
                modified_aggr_exprs.push(LogicalExpr::AggregateFunction {
                    fun: fun.clone(),
                    args: modified_args,
                    distinct: *distinct,
                });
            } else {
                modified_aggr_exprs.push(agg_expr.clone());
            }
        }

        Ok((
            needs_pre_projection,
            pre_projection_exprs,
            pre_projection_schema,
            modified_aggr_exprs,
        ))
    }

    // Build aggregate with GROUP BY
    fn build_aggregate(
        &mut self,
        input: LogicalPlan,
        group_by: &ast::GroupBy,
        columns: &[ast::ResultColumn],
    ) -> Result<LogicalPlan> {
        let input_schema = input.schema();

        // Build grouping expressions
        let mut group_exprs = Vec::new();
        for expr in &group_by.exprs {
            group_exprs.push(self.build_expr(expr, input_schema)?);
        }

        // Use the unified aggregate builder
        self.build_aggregate_internal(input, group_exprs, columns)
    }

    // Build aggregate without GROUP BY
    fn build_aggregate_no_group(
        &mut self,
        input: LogicalPlan,
        columns: &[ast::ResultColumn],
    ) -> Result<LogicalPlan> {
        // Use the unified aggregate builder with empty group expressions
        self.build_aggregate_internal(input, vec![], columns)
    }

    // Unified internal aggregate builder that handles both GROUP BY and non-GROUP BY cases
    fn build_aggregate_internal(
        &mut self,
        input: LogicalPlan,
        group_exprs: Vec<LogicalExpr>,
        columns: &[ast::ResultColumn],
    ) -> Result<LogicalPlan> {
        let input_schema = input.schema();
        let has_group_by = !group_exprs.is_empty();

        // Build aggregate expressions and projection expressions
        let mut aggr_exprs = Vec::new();
        let mut projection_exprs = Vec::new();
        let mut aggregate_schema_columns = Vec::new();

        // First, add GROUP BY columns to the aggregate output schema
        // These are always part of the aggregate operator's output
        for group_expr in &group_exprs {
            let col_name = match group_expr {
                LogicalExpr::Column(col) => col.name.clone(),
                _ => {
                    // For complex GROUP BY expressions, generate a name
                    format!("__group_{}", aggregate_schema_columns.len())
                }
            };
            let col_type = Self::infer_expr_type(group_expr, input_schema)?;
            aggregate_schema_columns.push((col_name, col_type));
        }

        // Track aggregates we've already seen to avoid duplicates
        let mut aggregate_map: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        for col in columns {
            match col {
                ast::ResultColumn::Expr(expr, alias) => {
                    let logical_expr = self.build_expr(expr, input_schema)?;

                    // Determine the column name for this expression
                    let col_name = match alias {
                        Some(as_alias) => match as_alias {
                            ast::As::As(name) | ast::As::Elided(name) => Self::name_to_string(name),
                        },
                        None => Self::expr_to_column_name(expr),
                    };

                    // Check if the TOP-LEVEL expression is an aggregate
                    // We only care about immediate aggregates, not nested ones
                    if Self::is_aggregate_expr(&logical_expr) {
                        // Pure aggregate function - check if we've seen it before
                        let agg_key = format!("{logical_expr:?}");

                        let agg_col_name = if let Some(existing_name) = aggregate_map.get(&agg_key)
                        {
                            // Reuse existing aggregate
                            existing_name.clone()
                        } else {
                            // New aggregate - add it
                            let col_type = Self::infer_expr_type(&logical_expr, input_schema)?;
                            aggregate_schema_columns.push((col_name.clone(), col_type));
                            aggr_exprs.push(logical_expr);
                            aggregate_map.insert(agg_key, col_name.clone());
                            col_name.clone()
                        };

                        // In the projection, reference this aggregate by name
                        projection_exprs.push(LogicalExpr::Column(Column {
                            name: agg_col_name,
                            table: None,
                        }));
                    } else if Self::contains_aggregate(&logical_expr) {
                        // This is an expression that contains an aggregate somewhere
                        // (e.g., sum(a + 2) * 2)
                        // We need to extract aggregates and replace them with column references
                        let (processed_expr, extracted_aggs) =
                            Self::extract_and_replace_aggregates_with_dedup(
                                logical_expr,
                                &mut aggregate_map,
                            )?;

                        // Add only new aggregates
                        for (agg_expr, agg_name) in extracted_aggs {
                            let agg_type = Self::infer_expr_type(&agg_expr, input_schema)?;
                            aggregate_schema_columns.push((agg_name, agg_type));
                            aggr_exprs.push(agg_expr);
                        }

                        // Add the processed expression (with column refs) to projection
                        projection_exprs.push(processed_expr);
                    } else {
                        // Non-aggregate expression - validation depends on GROUP BY presence
                        if has_group_by {
                            // With GROUP BY: only allow constants and grouped columns
                            // TODO: SQLite actually allows any column here and returns the value from
                            // the first row encountered in each group. We should support this in the
                            // future for full SQLite compatibility, but for now we're stricter to
                            // simplify the DBSP compilation.
                            if !Self::is_constant_expr(&logical_expr)
                                && !Self::is_valid_in_group_by(&logical_expr, &group_exprs)
                            {
                                return Err(LimboError::ParseError(format!(
                                    "Column '{col_name}' must appear in the GROUP BY clause or be used in an aggregate function"
                                )));
                            }
                        } else {
                            // Without GROUP BY: only allow constant expressions
                            // TODO: SQLite allows any column here and returns a value from an
                            // arbitrary row. We should support this for full compatibility,
                            // but for now we're stricter to simplify DBSP compilation.
                            if !Self::is_constant_expr(&logical_expr) {
                                return Err(LimboError::ParseError(format!(
                                    "Column '{col_name}' must be used in an aggregate function when using aggregates without GROUP BY"
                                )));
                            }
                        }
                        projection_exprs.push(logical_expr);
                    }
                }
                _ => {
                    let error_msg = if has_group_by {
                        "* not supported with GROUP BY".to_string()
                    } else {
                        "* not supported with aggregate functions".to_string()
                    };
                    return Err(LimboError::ParseError(error_msg));
                }
            }
        }

        // Check if any aggregate functions have complex expressions as arguments
        // If so, we need to insert a projection before the aggregate
        let (
            needs_pre_projection,
            pre_projection_exprs,
            pre_projection_schema,
            modified_aggr_exprs,
        ) = Self::preprocess_aggregate_expressions(&aggr_exprs, &group_exprs, input_schema)?;

        // Build the final schema for the projection
        let mut projection_schema_columns = Vec::new();
        for (i, expr) in projection_exprs.iter().enumerate() {
            let col_name = if i < columns.len() {
                match &columns[i] {
                    ast::ResultColumn::Expr(e, alias) => match alias {
                        Some(as_alias) => match as_alias {
                            ast::As::As(name) | ast::As::Elided(name) => Self::name_to_string(name),
                        },
                        None => Self::expr_to_column_name(e),
                    },
                    _ => format!("col_{i}"),
                }
            } else {
                format!("col_{i}")
            };

            // For type inference, we need the aggregate schema for column references
            let aggregate_schema = LogicalSchema::new(aggregate_schema_columns.clone());
            let col_type = Self::infer_expr_type(expr, &Arc::new(aggregate_schema))?;
            projection_schema_columns.push((col_name, col_type));
        }

        // Create the input plan (with pre-projection if needed)
        let aggregate_input = if needs_pre_projection {
            Arc::new(LogicalPlan::Projection(Projection {
                input: Arc::new(input),
                exprs: pre_projection_exprs,
                schema: Arc::new(LogicalSchema::new(pre_projection_schema)),
            }))
        } else {
            Arc::new(input)
        };

        // Use modified aggregate expressions if we inserted a pre-projection
        let final_aggr_exprs = if needs_pre_projection {
            modified_aggr_exprs
        } else {
            aggr_exprs
        };

        // Check if we need the outer projection
        // We need a projection if:
        // 1. Any expression is more complex than a simple column reference (e.g., abs(sum(id)))
        // 2. We're selecting a different set of columns than what the aggregate outputs
        // 3. Columns are renamed or reordered
        let needs_outer_projection = {
            // Check if any expression is more complex than a simple column reference
            let has_complex_exprs = projection_exprs
                .iter()
                .any(|expr| !matches!(expr, LogicalExpr::Column(_)));

            if has_complex_exprs {
                true
            } else {
                // All are simple columns - check if we're selecting exactly what the aggregate outputs
                // The projection might be selecting a subset (e.g., only aggregates without group columns)
                // or reordering columns, or using different names

                // For now, keep it simple: if schemas don't match exactly, we need projection
                // This handles all cases: subset selection, reordering, renaming
                projection_schema_columns != aggregate_schema_columns
            }
        };

        // Create the aggregate node
        let aggregate_plan = LogicalPlan::Aggregate(Aggregate {
            input: aggregate_input,
            group_expr: group_exprs,
            aggr_expr: final_aggr_exprs,
            schema: Arc::new(LogicalSchema::new(aggregate_schema_columns)),
        });

        if needs_outer_projection {
            Ok(LogicalPlan::Projection(Projection {
                input: Arc::new(aggregate_plan),
                exprs: projection_exprs,
                schema: Arc::new(LogicalSchema::new(projection_schema_columns)),
            }))
        } else {
            // No projection needed - the aggregate output is exactly what we want
            Ok(aggregate_plan)
        }
    }

    /// Build VALUES clause
    #[allow(clippy::vec_box)]
    fn build_values(&mut self, values: &[Vec<Box<ast::Expr>>]) -> Result<LogicalPlan> {
        if values.is_empty() {
            return Err(LimboError::ParseError("Empty VALUES clause".to_string()));
        }

        let mut rows = Vec::new();
        let first_row_len = values[0].len();

        // Infer schema from first row
        let mut schema_columns = Vec::new();
        for (i, _) in values[0].iter().enumerate() {
            schema_columns.push((format!("column{}", i + 1), Type::Text));
        }

        for row in values {
            if row.len() != first_row_len {
                return Err(LimboError::ParseError(
                    "All rows in VALUES must have the same number of columns".to_string(),
                ));
            }

            let mut logical_row = Vec::new();
            for expr in row {
                // VALUES doesn't have input schema
                let empty_schema = Arc::new(LogicalSchema::empty());
                logical_row.push(self.build_expr(expr, &empty_schema)?);
            }
            rows.push(logical_row);
        }

        Ok(LogicalPlan::Values(Values {
            rows,
            schema: Arc::new(LogicalSchema::new(schema_columns)),
        }))
    }

    // Build SORT
    fn build_sort(
        &mut self,
        input: LogicalPlan,
        exprs: &[ast::SortedColumn],
    ) -> Result<LogicalPlan> {
        let input_schema = input.schema();
        let mut sort_exprs = Vec::new();

        for sorted_col in exprs {
            let expr = self.build_expr(&sorted_col.expr, input_schema)?;
            sort_exprs.push(SortExpr {
                expr,
                asc: sorted_col.order != Some(ast::SortOrder::Desc),
                nulls_first: sorted_col.nulls == Some(ast::NullsOrder::First),
            });
        }

        Ok(LogicalPlan::Sort(Sort {
            input: Arc::new(input),
            exprs: sort_exprs,
        }))
    }

    // Build LIMIT
    fn build_limit(input: LogicalPlan, limit: &ast::Limit) -> Result<LogicalPlan> {
        let fetch = match limit.expr.as_ref() {
            ast::Expr::Literal(ast::Literal::Numeric(s)) => s.parse::<usize>().ok(),
            _ => {
                return Err(LimboError::ParseError(
                    "LIMIT must be a literal integer".to_string(),
                ))
            }
        };

        let skip = if let Some(offset) = &limit.offset {
            match offset.as_ref() {
                ast::Expr::Literal(ast::Literal::Numeric(s)) => s.parse::<usize>().ok(),
                _ => {
                    return Err(LimboError::ParseError(
                        "OFFSET must be a literal integer".to_string(),
                    ))
                }
            }
        } else {
            None
        };

        Ok(LogicalPlan::Limit(Limit {
            input: Arc::new(input),
            skip,
            fetch,
        }))
    }

    // Build compound operator (UNION, INTERSECT, EXCEPT)
    fn build_compound(
        left: LogicalPlan,
        right: LogicalPlan,
        op: &ast::CompoundOperator,
    ) -> Result<LogicalPlan> {
        // Check schema compatibility
        if left.schema().column_count() != right.schema().column_count() {
            return Err(LimboError::ParseError(
                "UNION/INTERSECT/EXCEPT requires same number of columns".to_string(),
            ));
        }

        let all = matches!(op, ast::CompoundOperator::UnionAll);

        match op {
            ast::CompoundOperator::Union | ast::CompoundOperator::UnionAll => {
                let schema = left.schema().clone();
                Ok(LogicalPlan::Union(Union {
                    inputs: vec![Arc::new(left), Arc::new(right)],
                    all,
                    schema,
                }))
            }
            _ => Err(LimboError::ParseError(
                "INTERSECT and EXCEPT not yet supported in logical plans".to_string(),
            )),
        }
    }

    // Build expression from AST
    fn build_expr(&mut self, expr: &ast::Expr, _schema: &SchemaRef) -> Result<LogicalExpr> {
        match expr {
            ast::Expr::Id(name) => Ok(LogicalExpr::Column(Column::new(Self::name_to_string(name)))),

            ast::Expr::DoublyQualified(db, table, col) => {
                Ok(LogicalExpr::Column(Column::with_table(
                    Self::name_to_string(col),
                    format!(
                        "{}.{}",
                        Self::name_to_string(db),
                        Self::name_to_string(table)
                    ),
                )))
            }

            ast::Expr::Qualified(table, col) => Ok(LogicalExpr::Column(Column::with_table(
                Self::name_to_string(col),
                Self::name_to_string(table),
            ))),

            ast::Expr::Literal(lit) => Ok(LogicalExpr::Literal(Self::build_literal(lit)?)),

            ast::Expr::Binary(lhs, op, rhs) => {
                // Special case: IS NULL and IS NOT NULL
                if matches!(op, ast::Operator::Is | ast::Operator::IsNot) {
                    if let ast::Expr::Literal(ast::Literal::Null) = rhs.as_ref() {
                        let expr = Box::new(self.build_expr(lhs, _schema)?);
                        return Ok(LogicalExpr::IsNull {
                            expr,
                            negated: matches!(op, ast::Operator::IsNot),
                        });
                    }
                }

                let left = Box::new(self.build_expr(lhs, _schema)?);
                let right = Box::new(self.build_expr(rhs, _schema)?);
                Ok(LogicalExpr::BinaryExpr {
                    left,
                    op: *op,
                    right,
                })
            }

            ast::Expr::Unary(op, expr) => {
                let inner = Box::new(self.build_expr(expr, _schema)?);
                Ok(LogicalExpr::UnaryExpr {
                    op: *op,
                    expr: inner,
                })
            }

            ast::Expr::FunctionCall {
                name,
                distinctness,
                args,
                filter_over,
                ..
            } => {
                // Check for window functions (OVER clause)
                if filter_over.over_clause.is_some() {
                    return Err(LimboError::ParseError(
                        "Unsupported expression type: window functions are not yet supported"
                            .to_string(),
                    ));
                }

                let func_name = Self::name_to_string(name);
                let arg_count = args.len();
                // Check if it's an aggregate function (considering argument count for min/max)
                if let Some(agg_fun) = Self::parse_aggregate_function(&func_name, arg_count) {
                    let distinct = distinctness.is_some();
                    let arg_exprs = args
                        .iter()
                        .map(|e| self.build_expr(e, _schema))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(LogicalExpr::AggregateFunction {
                        fun: agg_fun,
                        args: arg_exprs,
                        distinct,
                    })
                } else {
                    // Regular scalar function
                    let arg_exprs = args
                        .iter()
                        .map(|e| self.build_expr(e, _schema))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(LogicalExpr::ScalarFunction {
                        fun: func_name,
                        args: arg_exprs,
                    })
                }
            }

            ast::Expr::FunctionCallStar { name, .. } => {
                // Handle COUNT(*) and similar
                let func_name = Self::name_to_string(name);
                // FunctionCallStar always has 0 args (it's the * form)
                if let Some(agg_fun) = Self::parse_aggregate_function(&func_name, 0) {
                    Ok(LogicalExpr::AggregateFunction {
                        fun: agg_fun,
                        args: vec![],
                        distinct: false,
                    })
                } else {
                    Err(LimboError::ParseError(format!(
                        "Function {func_name}(*) is not supported"
                    )))
                }
            }

            ast::Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                let case_expr = if let Some(e) = base {
                    Some(Box::new(self.build_expr(e, _schema)?))
                } else {
                    None
                };

                let when_then_exprs = when_then_pairs
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            self.build_expr(when, _schema)?,
                            self.build_expr(then, _schema)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let else_result = if let Some(e) = else_expr {
                    Some(Box::new(self.build_expr(e, _schema)?))
                } else {
                    None
                };

                Ok(LogicalExpr::Case {
                    expr: case_expr,
                    when_then: when_then_exprs,
                    else_expr: else_result,
                })
            }

            ast::Expr::InList { lhs, not, rhs } => {
                let expr = Box::new(self.build_expr(lhs, _schema)?);
                let list = rhs
                    .iter()
                    .map(|e| self.build_expr(e, _schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalExpr::InList {
                    expr,
                    list,
                    negated: *not,
                })
            }

            ast::Expr::InSelect { lhs, not, rhs } => {
                let expr = Box::new(self.build_expr(lhs, _schema)?);
                let subquery = Arc::new(self.build_select(rhs)?);
                Ok(LogicalExpr::InSubquery {
                    expr,
                    subquery,
                    negated: *not,
                })
            }

            ast::Expr::Exists(select) => {
                let subquery = Arc::new(self.build_select(select)?);
                Ok(LogicalExpr::Exists {
                    subquery,
                    negated: false,
                })
            }

            ast::Expr::Subquery(select) => {
                let subquery = Arc::new(self.build_select(select)?);
                Ok(LogicalExpr::ScalarSubquery(subquery))
            }

            ast::Expr::IsNull(lhs) => {
                let expr = Box::new(self.build_expr(lhs, _schema)?);
                Ok(LogicalExpr::IsNull {
                    expr,
                    negated: false,
                })
            }

            ast::Expr::NotNull(lhs) => {
                let expr = Box::new(self.build_expr(lhs, _schema)?);
                Ok(LogicalExpr::IsNull {
                    expr,
                    negated: true,
                })
            }

            ast::Expr::Between {
                lhs,
                not,
                start,
                end,
            } => {
                let expr = Box::new(self.build_expr(lhs, _schema)?);
                let low = Box::new(self.build_expr(start, _schema)?);
                let high = Box::new(self.build_expr(end, _schema)?);
                Ok(LogicalExpr::Between {
                    expr,
                    low,
                    high,
                    negated: *not,
                })
            }

            ast::Expr::Like {
                lhs,
                not,
                op: _,
                rhs,
                escape,
            } => {
                let expr = Box::new(self.build_expr(lhs, _schema)?);
                let pattern = Box::new(self.build_expr(rhs, _schema)?);
                let escape_char = escape.as_ref().and_then(|e| {
                    if let ast::Expr::Literal(ast::Literal::String(s)) = e.as_ref() {
                        s.chars().next()
                    } else {
                        None
                    }
                });
                Ok(LogicalExpr::Like {
                    expr,
                    pattern,
                    escape: escape_char,
                    negated: *not,
                })
            }

            ast::Expr::Parenthesized(exprs) => {
                // the assumption is that there is at least one parenthesis here.
                // If this is not true, then I don't understand this code and can't be trusted.
                assert!(!exprs.is_empty());
                // Multiple expressions in parentheses is unusual but handle it
                // by building the first one (SQLite behavior)
                self.build_expr(&exprs[0], _schema)
            }

            _ => Err(LimboError::ParseError(format!(
                "Unsupported expression type in logical plan: {expr:?}"
            ))),
        }
    }

    /// Build literal value
    fn build_literal(lit: &ast::Literal) -> Result<Value> {
        match lit {
            ast::Literal::Null => Ok(Value::Null),
            ast::Literal::Keyword(k) => {
                let k_bytes = k.as_bytes();
                match_ignore_ascii_case!(match k_bytes {
                    b"true" => Ok(Value::Integer(1)),  // SQLite uses int for bool
                    b"false" => Ok(Value::Integer(0)), // SQLite uses int for bool
                    _ => Ok(Value::Text(k.clone().into())),
                })
            }
            ast::Literal::Numeric(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    Ok(Value::Integer(i))
                } else if let Ok(f) = s.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Ok(Value::Text(s.clone().into()))
                }
            }
            ast::Literal::String(s) => {
                // Strip surrounding quotes from the SQL literal
                // The parser includes quotes in the string value
                let unquoted = if s.starts_with('\'') && s.ends_with('\'') && s.len() > 1 {
                    &s[1..s.len() - 1]
                } else {
                    s.as_str()
                };
                Ok(Value::Text(unquoted.to_string().into()))
            }
            ast::Literal::Blob(b) => Ok(Value::Blob(b.clone().into())),
            ast::Literal::CurrentDate
            | ast::Literal::CurrentTime
            | ast::Literal::CurrentTimestamp => Err(LimboError::ParseError(
                "Temporal literals not yet supported".to_string(),
            )),
        }
    }

    /// Parse aggregate function name (considering argument count for min/max)
    fn parse_aggregate_function(name: &str, arg_count: usize) -> Option<AggregateFunction> {
        let name_bytes = name.as_bytes();
        match_ignore_ascii_case!(match name_bytes {
            b"COUNT" => Some(AggFunc::Count),
            b"SUM" => Some(AggFunc::Sum),
            b"AVG" => Some(AggFunc::Avg),
            // MIN and MAX are only aggregates with 1 argument
            // With 2+ arguments, they're scalar functions
            b"MIN" if arg_count == 1 => Some(AggFunc::Min),
            b"MAX" if arg_count == 1 => Some(AggFunc::Max),
            b"GROUP_CONCAT" => Some(AggFunc::GroupConcat),
            b"STRING_AGG" => Some(AggFunc::StringAgg),
            b"TOTAL" => Some(AggFunc::Total),
            _ => None,
        })
    }

    // Check if expression contains aggregates
    fn has_aggregates(columns: &[ast::ResultColumn]) -> bool {
        for col in columns {
            if let ast::ResultColumn::Expr(expr, _) = col {
                if Self::expr_has_aggregate(expr) {
                    return true;
                }
            }
        }
        false
    }

    // Check if AST expression contains aggregates
    fn expr_has_aggregate(expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::FunctionCall { name, args, .. } => {
                // Check if the function itself is an aggregate (considering arg count for min/max)
                let arg_count = args.len();
                if Self::parse_aggregate_function(&Self::name_to_string(name), arg_count).is_some()
                {
                    return true;
                }
                // Also check if any arguments contain aggregates (for nested functions like HEX(SUM(...)))
                args.iter().any(|arg| Self::expr_has_aggregate(arg))
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                // FunctionCallStar always has 0 args
                Self::parse_aggregate_function(&Self::name_to_string(name), 0).is_some()
            }
            ast::Expr::Binary(lhs, _, rhs) => {
                Self::expr_has_aggregate(lhs) || Self::expr_has_aggregate(rhs)
            }
            ast::Expr::Unary(_, e) => Self::expr_has_aggregate(e),
            ast::Expr::Case {
                when_then_pairs,
                else_expr,
                ..
            } => {
                when_then_pairs
                    .iter()
                    .any(|(w, t)| Self::expr_has_aggregate(w) || Self::expr_has_aggregate(t))
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| Self::expr_has_aggregate(e))
            }
            ast::Expr::Parenthesized(exprs) => {
                // Check if any parenthesized expression contains an aggregate
                exprs.iter().any(|e| Self::expr_has_aggregate(e))
            }
            _ => false,
        }
    }

    // Check if logical expression is an aggregate
    fn is_aggregate_expr(expr: &LogicalExpr) -> bool {
        match expr {
            LogicalExpr::AggregateFunction { .. } => true,
            LogicalExpr::Alias { expr, .. } => Self::is_aggregate_expr(expr),
            _ => false,
        }
    }

    // Check if logical expression contains an aggregate anywhere
    fn contains_aggregate(expr: &LogicalExpr) -> bool {
        match expr {
            LogicalExpr::AggregateFunction { .. } => true,
            LogicalExpr::Alias { expr, .. } => Self::contains_aggregate(expr),
            LogicalExpr::BinaryExpr { left, right, .. } => {
                Self::contains_aggregate(left) || Self::contains_aggregate(right)
            }
            LogicalExpr::UnaryExpr { expr, .. } => Self::contains_aggregate(expr),
            LogicalExpr::ScalarFunction { args, .. } => args.iter().any(Self::contains_aggregate),
            LogicalExpr::Case {
                when_then,
                else_expr,
                ..
            } => {
                when_then
                    .iter()
                    .any(|(w, t)| Self::contains_aggregate(w) || Self::contains_aggregate(t))
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| Self::contains_aggregate(e))
            }
            _ => false,
        }
    }

    // Check if an expression is a constant (contains only literals)
    fn is_constant_expr(expr: &LogicalExpr) -> bool {
        match expr {
            LogicalExpr::Literal(_) => true,
            LogicalExpr::BinaryExpr { left, right, .. } => {
                Self::is_constant_expr(left) && Self::is_constant_expr(right)
            }
            LogicalExpr::UnaryExpr { expr, .. } => Self::is_constant_expr(expr),
            LogicalExpr::ScalarFunction { args, .. } => args.iter().all(Self::is_constant_expr),
            LogicalExpr::Alias { expr, .. } => Self::is_constant_expr(expr),
            _ => false,
        }
    }

    // Check if an expression is valid in GROUP BY context
    // An expression is valid if it's:
    // 1. A constant literal
    // 2. An aggregate function
    // 3. A grouping column (or expression involving only grouping columns)
    fn is_valid_in_group_by(expr: &LogicalExpr, group_exprs: &[LogicalExpr]) -> bool {
        match expr {
            LogicalExpr::Literal(_) => true, // Constants are always valid
            LogicalExpr::AggregateFunction { .. } => true, // Aggregates are valid
            LogicalExpr::Column(col) => {
                // Check if this column is in the GROUP BY
                group_exprs.iter().any(|g| match g {
                    LogicalExpr::Column(gcol) => gcol.name == col.name,
                    _ => false,
                })
            }
            LogicalExpr::BinaryExpr { left, right, .. } => {
                // Both sides must be valid
                Self::is_valid_in_group_by(left, group_exprs)
                    && Self::is_valid_in_group_by(right, group_exprs)
            }
            LogicalExpr::UnaryExpr { expr, .. } => Self::is_valid_in_group_by(expr, group_exprs),
            LogicalExpr::ScalarFunction { args, .. } => {
                // All arguments must be valid
                args.iter()
                    .all(|arg| Self::is_valid_in_group_by(arg, group_exprs))
            }
            LogicalExpr::Alias { expr, .. } => Self::is_valid_in_group_by(expr, group_exprs),
            _ => false, // Other expressions are not valid
        }
    }

    // Extract aggregates from an expression and replace them with column references, with deduplication
    // Returns the modified expression and a list of NEW (aggregate_expr, column_name) pairs
    fn extract_and_replace_aggregates_with_dedup(
        expr: LogicalExpr,
        aggregate_map: &mut std::collections::HashMap<String, String>,
    ) -> Result<(LogicalExpr, Vec<(LogicalExpr, String)>)> {
        let mut new_aggregates = Vec::new();
        let mut counter = aggregate_map.len();
        let new_expr = Self::replace_aggregates_with_columns_dedup(
            expr,
            &mut new_aggregates,
            aggregate_map,
            &mut counter,
        )?;
        Ok((new_expr, new_aggregates))
    }

    // Recursively replace aggregate functions with column references, with deduplication
    fn replace_aggregates_with_columns_dedup(
        expr: LogicalExpr,
        new_aggregates: &mut Vec<(LogicalExpr, String)>,
        aggregate_map: &mut std::collections::HashMap<String, String>,
        counter: &mut usize,
    ) -> Result<LogicalExpr> {
        match expr {
            LogicalExpr::AggregateFunction { .. } => {
                // Found an aggregate - check if we've seen it before
                let agg_key = format!("{expr:?}");

                let col_name = if let Some(existing_name) = aggregate_map.get(&agg_key) {
                    // Reuse existing aggregate
                    existing_name.clone()
                } else {
                    // New aggregate
                    let col_name = format!("__agg_{}", *counter);
                    *counter += 1;
                    aggregate_map.insert(agg_key, col_name.clone());
                    new_aggregates.push((expr, col_name.clone()));
                    col_name
                };

                Ok(LogicalExpr::Column(Column {
                    name: col_name,
                    table: None,
                }))
            }
            LogicalExpr::BinaryExpr { left, op, right } => {
                let new_left = Self::replace_aggregates_with_columns_dedup(
                    *left,
                    new_aggregates,
                    aggregate_map,
                    counter,
                )?;
                let new_right = Self::replace_aggregates_with_columns_dedup(
                    *right,
                    new_aggregates,
                    aggregate_map,
                    counter,
                )?;
                Ok(LogicalExpr::BinaryExpr {
                    left: Box::new(new_left),
                    op,
                    right: Box::new(new_right),
                })
            }
            LogicalExpr::UnaryExpr { op, expr } => {
                let new_expr = Self::replace_aggregates_with_columns_dedup(
                    *expr,
                    new_aggregates,
                    aggregate_map,
                    counter,
                )?;
                Ok(LogicalExpr::UnaryExpr {
                    op,
                    expr: Box::new(new_expr),
                })
            }
            LogicalExpr::ScalarFunction { fun, args } => {
                let mut new_args = Vec::new();
                for arg in args {
                    new_args.push(Self::replace_aggregates_with_columns_dedup(
                        arg,
                        new_aggregates,
                        aggregate_map,
                        counter,
                    )?);
                }
                Ok(LogicalExpr::ScalarFunction {
                    fun,
                    args: new_args,
                })
            }
            LogicalExpr::Case {
                expr: case_expr,
                when_then,
                else_expr,
            } => {
                let new_case_expr = if let Some(e) = case_expr {
                    Some(Box::new(Self::replace_aggregates_with_columns_dedup(
                        *e,
                        new_aggregates,
                        aggregate_map,
                        counter,
                    )?))
                } else {
                    None
                };

                let mut new_when_then = Vec::new();
                for (when, then) in when_then {
                    let new_when = Self::replace_aggregates_with_columns_dedup(
                        when,
                        new_aggregates,
                        aggregate_map,
                        counter,
                    )?;
                    let new_then = Self::replace_aggregates_with_columns_dedup(
                        then,
                        new_aggregates,
                        aggregate_map,
                        counter,
                    )?;
                    new_when_then.push((new_when, new_then));
                }

                let new_else = if let Some(e) = else_expr {
                    Some(Box::new(Self::replace_aggregates_with_columns_dedup(
                        *e,
                        new_aggregates,
                        aggregate_map,
                        counter,
                    )?))
                } else {
                    None
                };

                Ok(LogicalExpr::Case {
                    expr: new_case_expr,
                    when_then: new_when_then,
                    else_expr: new_else,
                })
            }
            LogicalExpr::Alias { expr, alias } => {
                let new_expr = Self::replace_aggregates_with_columns_dedup(
                    *expr,
                    new_aggregates,
                    aggregate_map,
                    counter,
                )?;
                Ok(LogicalExpr::Alias {
                    expr: Box::new(new_expr),
                    alias,
                })
            }
            // Other expressions - keep as is
            _ => Ok(expr),
        }
    }

    // Get column name from expression
    fn expr_to_column_name(expr: &ast::Expr) -> String {
        match expr {
            ast::Expr::Id(name) => Self::name_to_string(name),
            ast::Expr::Qualified(_, col) => Self::name_to_string(col),
            ast::Expr::FunctionCall { name, .. } => Self::name_to_string(name),
            ast::Expr::FunctionCallStar { name, .. } => {
                format!("{}(*)", Self::name_to_string(name))
            }
            _ => "expr".to_string(),
        }
    }

    // Get table schema
    fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        // Look up table in schema
        let table = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| LimboError::ParseError(format!("Table '{table_name}' not found")))?;

        let mut columns = Vec::new();
        for col in table.columns() {
            if let Some(ref name) = col.name {
                columns.push((name.clone(), col.ty));
            }
        }

        Ok(Arc::new(LogicalSchema::new(columns)))
    }

    // Infer expression type
    fn infer_expr_type(expr: &LogicalExpr, schema: &SchemaRef) -> Result<Type> {
        match expr {
            LogicalExpr::Column(col) => {
                if let Some((_, typ)) = schema.find_column(&col.name) {
                    Ok(*typ)
                } else {
                    Ok(Type::Text)
                }
            }
            LogicalExpr::Literal(Value::Integer(_)) => Ok(Type::Integer),
            LogicalExpr::Literal(Value::Float(_)) => Ok(Type::Real),
            LogicalExpr::Literal(Value::Text(_)) => Ok(Type::Text),
            LogicalExpr::Literal(Value::Null) => Ok(Type::Null),
            LogicalExpr::Literal(Value::Blob(_)) => Ok(Type::Blob),
            LogicalExpr::BinaryExpr { op, left, right } => {
                match op {
                    ast::Operator::Add | ast::Operator::Subtract | ast::Operator::Multiply => {
                        // Infer types of operands to match SQLite/Numeric behavior
                        let left_type = Self::infer_expr_type(left, schema)?;
                        let right_type = Self::infer_expr_type(right, schema)?;

                        // Integer op Integer = Integer (matching core/numeric/mod.rs behavior)
                        // Any operation with Real = Real
                        match (left_type, right_type) {
                            (Type::Integer, Type::Integer) => Ok(Type::Integer),
                            (Type::Integer, Type::Real)
                            | (Type::Real, Type::Integer)
                            | (Type::Real, Type::Real) => Ok(Type::Real),
                            (Type::Null, _) | (_, Type::Null) => Ok(Type::Null),
                            // For Text/Blob, SQLite coerces to numeric, defaulting to Real
                            _ => Ok(Type::Real),
                        }
                    }
                    ast::Operator::Divide => {
                        // Division always produces Real in SQLite
                        Ok(Type::Real)
                    }
                    ast::Operator::Modulus => {
                        // Modulus follows same rules as other arithmetic ops
                        let left_type = Self::infer_expr_type(left, schema)?;
                        let right_type = Self::infer_expr_type(right, schema)?;
                        match (left_type, right_type) {
                            (Type::Integer, Type::Integer) => Ok(Type::Integer),
                            _ => Ok(Type::Real),
                        }
                    }
                    ast::Operator::Equals
                    | ast::Operator::NotEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::And
                    | ast::Operator::Or
                    | ast::Operator::Is
                    | ast::Operator::IsNot => Ok(Type::Integer),
                    ast::Operator::Concat => Ok(Type::Text),
                    _ => Ok(Type::Text), // Default for other operators
                }
            }
            LogicalExpr::UnaryExpr { op, expr } => match op {
                ast::UnaryOperator::Not => Ok(Type::Integer),
                ast::UnaryOperator::Negative | ast::UnaryOperator::Positive => {
                    Self::infer_expr_type(expr, schema)
                }
                ast::UnaryOperator::BitwiseNot => Ok(Type::Integer),
            },
            LogicalExpr::AggregateFunction { fun, .. } => match fun {
                AggFunc::Count | AggFunc::Count0 => Ok(Type::Integer),
                AggFunc::Sum | AggFunc::Avg | AggFunc::Total => Ok(Type::Real),
                AggFunc::Min | AggFunc::Max => Ok(Type::Text),
                AggFunc::GroupConcat | AggFunc::StringAgg => Ok(Type::Text),
                #[cfg(feature = "json")]
                AggFunc::JsonbGroupArray
                | AggFunc::JsonGroupArray
                | AggFunc::JsonbGroupObject
                | AggFunc::JsonGroupObject => Ok(Type::Text),
                AggFunc::External(_) => Ok(Type::Text), // Default for external
            },
            LogicalExpr::Alias { expr, .. } => Self::infer_expr_type(expr, schema),
            LogicalExpr::IsNull { .. } => Ok(Type::Integer),
            LogicalExpr::InList { .. } | LogicalExpr::InSubquery { .. } => Ok(Type::Integer),
            LogicalExpr::Exists { .. } => Ok(Type::Integer),
            LogicalExpr::Between { .. } => Ok(Type::Integer),
            LogicalExpr::Like { .. } => Ok(Type::Integer),
            _ => Ok(Type::Text),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{BTreeTable, Column as SchemaColumn, Schema, Type};
    use turso_parser::parser::Parser;

    fn create_test_schema() -> Schema {
        let mut schema = Schema::new(false);

        // Create users table
        let users_table = BTreeTable {
            name: "users".to_string(),
            root_page: 2,
            primary_key_columns: vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
            columns: vec![
                SchemaColumn {
                    name: Some("id".to_string()),
                    ty: Type::Integer,
                    ty_str: "INTEGER".to_string(),
                    primary_key: true,
                    is_rowid_alias: true,
                    notnull: true,
                    default: None,
                    unique: false,
                    collation: None,
                    hidden: false,
                },
                SchemaColumn {
                    name: Some("name".to_string()),
                    ty: Type::Text,
                    ty_str: "TEXT".to_string(),
                    primary_key: false,
                    is_rowid_alias: false,
                    notnull: false,
                    default: None,
                    unique: false,
                    collation: None,
                    hidden: false,
                },
                SchemaColumn {
                    name: Some("age".to_string()),
                    ty: Type::Integer,
                    ty_str: "INTEGER".to_string(),
                    primary_key: false,
                    is_rowid_alias: false,
                    notnull: false,
                    default: None,
                    unique: false,
                    collation: None,
                    hidden: false,
                },
                SchemaColumn {
                    name: Some("email".to_string()),
                    ty: Type::Text,
                    ty_str: "TEXT".to_string(),
                    primary_key: false,
                    is_rowid_alias: false,
                    notnull: false,
                    default: None,
                    unique: false,
                    collation: None,
                    hidden: false,
                },
            ],
            has_rowid: true,
            is_strict: false,
            unique_sets: vec![],
        };
        schema.add_btree_table(Arc::new(users_table));

        // Create orders table
        let orders_table = BTreeTable {
            name: "orders".to_string(),
            root_page: 3,
            primary_key_columns: vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
            columns: vec![
                SchemaColumn {
                    name: Some("id".to_string()),
                    ty: Type::Integer,
                    ty_str: "INTEGER".to_string(),
                    primary_key: true,
                    is_rowid_alias: true,
                    notnull: true,
                    default: None,
                    unique: false,
                    collation: None,
                    hidden: false,
                },
                SchemaColumn {
                    name: Some("user_id".to_string()),
                    ty: Type::Integer,
                    ty_str: "INTEGER".to_string(),
                    primary_key: false,
                    is_rowid_alias: false,
                    notnull: false,
                    default: None,
                    unique: false,
                    collation: None,
                    hidden: false,
                },
                SchemaColumn {
                    name: Some("product".to_string()),
                    ty: Type::Text,
                    ty_str: "TEXT".to_string(),
                    primary_key: false,
                    is_rowid_alias: false,
                    notnull: false,
                    default: None,
                    unique: false,
                    collation: None,
                    hidden: false,
                },
                SchemaColumn {
                    name: Some("amount".to_string()),
                    ty: Type::Real,
                    ty_str: "REAL".to_string(),
                    primary_key: false,
                    is_rowid_alias: false,
                    notnull: false,
                    default: None,
                    unique: false,
                    collation: None,
                    hidden: false,
                },
            ],
            has_rowid: true,
            is_strict: false,
            unique_sets: vec![],
        };
        schema.add_btree_table(Arc::new(orders_table));

        schema
    }

    fn parse_and_build(sql: &str, schema: &Schema) -> Result<LogicalPlan> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser
            .next()
            .ok_or_else(|| LimboError::ParseError("Empty statement".to_string()))?
            .map_err(|e| LimboError::ParseError(e.to_string()))?;
        match cmd {
            ast::Cmd::Stmt(stmt) => {
                let mut builder = LogicalPlanBuilder::new(schema);
                builder.build_statement(&stmt)
            }
            _ => Err(LimboError::ParseError(
                "Only SQL statements are supported".to_string(),
            )),
        }
    }

    #[test]
    fn test_simple_select() {
        let schema = create_test_schema();
        let sql = "SELECT id, name FROM users";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 2);
                assert!(matches!(proj.exprs[0], LogicalExpr::Column(_)));
                assert!(matches!(proj.exprs[1], LogicalExpr::Column(_)));

                match &*proj.input {
                    LogicalPlan::TableScan(scan) => {
                        assert_eq!(scan.table_name, "users");
                    }
                    _ => panic!("Expected TableScan"),
                }
            }
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_select_with_filter() {
        let schema = create_test_schema();
        let sql = "SELECT name FROM users WHERE age > 18";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 1);

                match &*proj.input {
                    LogicalPlan::Filter(filter) => {
                        assert!(matches!(
                            filter.predicate,
                            LogicalExpr::BinaryExpr {
                                op: ast::Operator::Greater,
                                ..
                            }
                        ));

                        match &*filter.input {
                            LogicalPlan::TableScan(scan) => {
                                assert_eq!(scan.table_name, "users");
                            }
                            _ => panic!("Expected TableScan"),
                        }
                    }
                    _ => panic!("Expected Filter"),
                }
            }
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_aggregate_with_group_by() {
        let schema = create_test_schema();
        let sql = "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Aggregate(agg) => {
                assert_eq!(agg.group_expr.len(), 1);
                assert_eq!(agg.aggr_expr.len(), 1);
                assert_eq!(agg.schema.column_count(), 2);

                assert!(matches!(
                    agg.aggr_expr[0],
                    LogicalExpr::AggregateFunction {
                        fun: AggFunc::Sum,
                        ..
                    }
                ));

                match &*agg.input {
                    LogicalPlan::TableScan(scan) => {
                        assert_eq!(scan.table_name, "orders");
                    }
                    _ => panic!("Expected TableScan"),
                }
            }
            _ => panic!("Expected Aggregate (no projection)"),
        }
    }

    #[test]
    fn test_aggregate_without_group_by() {
        let schema = create_test_schema();
        let sql = "SELECT COUNT(*), MAX(age) FROM users";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Aggregate(agg) => {
                assert_eq!(agg.group_expr.len(), 0);
                assert_eq!(agg.aggr_expr.len(), 2);
                assert_eq!(agg.schema.column_count(), 2);

                assert!(matches!(
                    agg.aggr_expr[0],
                    LogicalExpr::AggregateFunction {
                        fun: AggFunc::Count,
                        ..
                    }
                ));

                assert!(matches!(
                    agg.aggr_expr[1],
                    LogicalExpr::AggregateFunction {
                        fun: AggFunc::Max,
                        ..
                    }
                ));
            }
            _ => panic!("Expected Aggregate (no projection)"),
        }
    }

    #[test]
    fn test_order_by() {
        let schema = create_test_schema();
        let sql = "SELECT name FROM users ORDER BY age DESC, name ASC";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Sort(sort) => {
                assert_eq!(sort.exprs.len(), 2);
                assert!(!sort.exprs[0].asc); // DESC
                assert!(sort.exprs[1].asc); // ASC

                match &*sort.input {
                    LogicalPlan::Projection(_) => {}
                    _ => panic!("Expected Projection"),
                }
            }
            _ => panic!("Expected Sort"),
        }
    }

    #[test]
    fn test_limit_offset() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Limit(limit) => {
                assert_eq!(limit.fetch, Some(10));
                assert_eq!(limit.skip, Some(5));
            }
            _ => panic!("Expected Limit"),
        }
    }

    #[test]
    fn test_order_by_with_limit() {
        let schema = create_test_schema();
        let sql = "SELECT name FROM users ORDER BY age DESC LIMIT 5";
        let plan = parse_and_build(sql, &schema).unwrap();

        // Should produce: Limit -> Sort -> Projection -> TableScan
        match plan {
            LogicalPlan::Limit(limit) => {
                assert_eq!(limit.fetch, Some(5));
                assert_eq!(limit.skip, None);

                match &*limit.input {
                    LogicalPlan::Sort(sort) => {
                        assert_eq!(sort.exprs.len(), 1);
                        assert!(!sort.exprs[0].asc); // DESC

                        match &*sort.input {
                            LogicalPlan::Projection(_) => {}
                            _ => panic!("Expected Projection under Sort"),
                        }
                    }
                    _ => panic!("Expected Sort under Limit"),
                }
            }
            _ => panic!("Expected Limit at top level"),
        }
    }

    #[test]
    fn test_distinct() {
        let schema = create_test_schema();
        let sql = "SELECT DISTINCT name FROM users";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Distinct(distinct) => match &*distinct.input {
                LogicalPlan::Projection(_) => {}
                _ => panic!("Expected Projection"),
            },
            _ => panic!("Expected Distinct"),
        }
    }

    #[test]
    fn test_union() {
        let schema = create_test_schema();
        let sql = "SELECT id FROM users UNION SELECT user_id FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Union(union) => {
                assert!(!union.all);
                assert_eq!(union.inputs.len(), 2);
            }
            _ => panic!("Expected Union"),
        }
    }

    #[test]
    fn test_union_all() {
        let schema = create_test_schema();
        let sql = "SELECT id FROM users UNION ALL SELECT user_id FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Union(union) => {
                assert!(union.all);
                assert_eq!(union.inputs.len(), 2);
            }
            _ => panic!("Expected Union"),
        }
    }

    #[test]
    fn test_union_with_order_by() {
        let schema = create_test_schema();
        let sql = "SELECT id, name FROM users UNION SELECT user_id, name FROM orders ORDER BY id";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Sort(sort) => {
                assert_eq!(sort.exprs.len(), 1);
                assert!(sort.exprs[0].asc); // Default ASC

                match &*sort.input {
                    LogicalPlan::Union(union) => {
                        assert!(!union.all); // UNION (not UNION ALL)
                        assert_eq!(union.inputs.len(), 2);
                    }
                    _ => panic!("Expected Union under Sort"),
                }
            }
            _ => panic!("Expected Sort at top level"),
        }
    }

    #[test]
    fn test_with_cte() {
        let schema = create_test_schema();
        let sql = "WITH active_users AS (SELECT * FROM users WHERE age > 18) SELECT name FROM active_users";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::WithCTE(with) => {
                assert_eq!(with.ctes.len(), 1);
                assert!(with.ctes.contains_key("active_users"));

                let cte = &with.ctes["active_users"];
                match &**cte {
                    LogicalPlan::Projection(proj) => match &*proj.input {
                        LogicalPlan::Filter(_) => {}
                        _ => panic!("Expected Filter in CTE"),
                    },
                    _ => panic!("Expected Projection in CTE"),
                }

                match &*with.body {
                    LogicalPlan::Projection(proj) => match &*proj.input {
                        LogicalPlan::CTERef(cte_ref) => {
                            assert_eq!(cte_ref.name, "active_users");
                        }
                        _ => panic!("Expected CTERef"),
                    },
                    _ => panic!("Expected Projection in body"),
                }
            }
            _ => panic!("Expected WithCTE"),
        }
    }

    #[test]
    fn test_case_expression() {
        let schema = create_test_schema();
        let sql = "SELECT CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END FROM users";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 1);
                assert!(matches!(proj.exprs[0], LogicalExpr::Case { .. }));
            }
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_in_list() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => match &*proj.input {
                LogicalPlan::Filter(filter) => match &filter.predicate {
                    LogicalExpr::InList { list, negated, .. } => {
                        assert!(!negated);
                        assert_eq!(list.len(), 3);
                    }
                    _ => panic!("Expected InList"),
                },
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_in_subquery() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => match &*proj.input {
                LogicalPlan::Filter(filter) => {
                    assert!(matches!(filter.predicate, LogicalExpr::InSubquery { .. }));
                }
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_exists_subquery() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => match &*proj.input {
                LogicalPlan::Filter(filter) => {
                    assert!(matches!(filter.predicate, LogicalExpr::Exists { .. }));
                }
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_between() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => match &*proj.input {
                LogicalPlan::Filter(filter) => match &filter.predicate {
                    LogicalExpr::Between { negated, .. } => {
                        assert!(!negated);
                    }
                    _ => panic!("Expected Between"),
                },
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_like() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM users WHERE name LIKE 'John%'";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => match &*proj.input {
                LogicalPlan::Filter(filter) => match &filter.predicate {
                    LogicalExpr::Like {
                        negated, escape, ..
                    } => {
                        assert!(!negated);
                        assert!(escape.is_none());
                    }
                    _ => panic!("Expected Like"),
                },
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_is_null() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM users WHERE email IS NULL";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => match &*proj.input {
                LogicalPlan::Filter(filter) => match &filter.predicate {
                    LogicalExpr::IsNull { negated, .. } => {
                        assert!(!negated);
                    }
                    _ => panic!("Expected IsNull, got: {:?}", filter.predicate),
                },
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_is_not_null() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM users WHERE email IS NOT NULL";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => match &*proj.input {
                LogicalPlan::Filter(filter) => match &filter.predicate {
                    LogicalExpr::IsNull { negated, .. } => {
                        assert!(negated);
                    }
                    _ => panic!("Expected IsNull"),
                },
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_values_clause() {
        let schema = create_test_schema();
        let sql = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'))";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => match &*proj.input {
                LogicalPlan::Values(values) => {
                    assert_eq!(values.rows.len(), 2);
                    assert_eq!(values.rows[0].len(), 2);
                }
                _ => panic!("Expected Values"),
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_complex_expression_with_aggregation() {
        // Test: SELECT sum(id + 2) * 2 FROM orders GROUP BY user_id
        let schema = create_test_schema();

        // Test the complex case: sum((id + 2)) * 2 with parentheses
        let sql = "SELECT sum((id + 2)) * 2 FROM orders GROUP BY user_id";
        let plan = parse_and_build(sql, &schema).unwrap();

        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 1);
                match &proj.exprs[0] {
                    LogicalExpr::BinaryExpr { left, op, right } => {
                        assert_eq!(*op, BinaryOperator::Multiply);
                        assert!(matches!(**left, LogicalExpr::Column(_)));
                        assert!(matches!(**right, LogicalExpr::Literal(_)));
                    }
                    _ => panic!("Expected BinaryExpr in projection"),
                }

                match &*proj.input {
                    LogicalPlan::Aggregate(agg) => {
                        assert_eq!(agg.group_expr.len(), 1);

                        assert_eq!(agg.aggr_expr.len(), 1);
                        match &agg.aggr_expr[0] {
                            LogicalExpr::AggregateFunction { fun, args, .. } => {
                                assert_eq!(*fun, AggregateFunction::Sum);
                                assert_eq!(args.len(), 1);
                                match &args[0] {
                                    LogicalExpr::Column(col) => {
                                        assert!(col.name.starts_with("__agg_arg_proj_"));
                                    }
                                    _ => panic!("Expected Column reference to projected expression in aggregate args, got {:?}", args[0]),
                                }
                            }
                            _ => panic!("Expected AggregateFunction"),
                        }

                        match &*agg.input {
                            LogicalPlan::Projection(inner_proj) => {
                                assert!(inner_proj.exprs.len() >= 2);
                                let has_binary_add = inner_proj.exprs.iter().any(|e| {
                                    matches!(
                                        e,
                                        LogicalExpr::BinaryExpr {
                                            op: BinaryOperator::Add,
                                            ..
                                        }
                                    )
                                });
                                assert!(
                                    has_binary_add,
                                    "Should have id + 2 expression in inner projection"
                                );
                            }
                            _ => panic!("Expected Projection as input to Aggregate"),
                        }
                    }
                    _ => panic!("Expected Aggregate under Projection"),
                }
            }
            _ => panic!("Expected Projection at top level"),
        }
    }

    #[test]
    fn test_function_on_aggregate_result() {
        let schema = create_test_schema();

        let sql = "SELECT abs(sum(id)) FROM orders GROUP BY user_id";
        let plan = parse_and_build(sql, &schema).unwrap();
        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 1);
                match &proj.exprs[0] {
                    LogicalExpr::ScalarFunction { fun, args } => {
                        assert_eq!(fun, "abs");
                        assert_eq!(args.len(), 1);
                        assert!(matches!(args[0], LogicalExpr::Column(_)));
                    }
                    _ => panic!("Expected ScalarFunction in projection"),
                }
            }
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_multiple_aggregates_with_arithmetic() {
        let schema = create_test_schema();

        let sql = "SELECT sum(id) * 2 + count(*) FROM orders GROUP BY user_id";
        let plan = parse_and_build(sql, &schema).unwrap();
        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 1);
                match &proj.exprs[0] {
                    LogicalExpr::BinaryExpr { op, .. } => {
                        assert_eq!(*op, BinaryOperator::Add);
                    }
                    _ => panic!("Expected BinaryExpr"),
                }

                match &*proj.input {
                    LogicalPlan::Aggregate(agg) => {
                        assert_eq!(agg.aggr_expr.len(), 2);
                    }
                    _ => panic!("Expected Aggregate"),
                }
            }
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_projection_aggregation_projection() {
        let schema = create_test_schema();

        // This tests: projection -> aggregation -> projection
        // The inner projection computes (id + 2), then we aggregate sum(), then apply abs()
        let sql = "SELECT abs(sum(id + 2)) FROM orders GROUP BY user_id";
        let plan = parse_and_build(sql, &schema).unwrap();

        // Should produce: Projection(abs) -> Aggregate(sum) -> Projection(id + 2) -> TableScan
        match plan {
            LogicalPlan::Projection(outer_proj) => {
                assert_eq!(outer_proj.exprs.len(), 1);

                // Outer projection should apply abs() function
                match &outer_proj.exprs[0] {
                    LogicalExpr::ScalarFunction { fun, args } => {
                        assert_eq!(fun, "abs");
                        assert_eq!(args.len(), 1);
                        assert!(matches!(args[0], LogicalExpr::Column(_)));
                    }
                    _ => panic!("Expected abs() function in outer projection"),
                }

                // Next should be the Aggregate
                match &*outer_proj.input {
                    LogicalPlan::Aggregate(agg) => {
                        assert_eq!(agg.group_expr.len(), 1);
                        assert_eq!(agg.aggr_expr.len(), 1);

                        // The aggregate should be summing a column reference
                        match &agg.aggr_expr[0] {
                            LogicalExpr::AggregateFunction { fun, args, .. } => {
                                assert_eq!(*fun, AggregateFunction::Sum);
                                assert_eq!(args.len(), 1);

                                // Should reference the projected column
                                match &args[0] {
                                    LogicalExpr::Column(col) => {
                                        assert!(col.name.starts_with("__agg_arg_proj_"));
                                    }
                                    _ => panic!("Expected column reference in aggregate"),
                                }
                            }
                            _ => panic!("Expected AggregateFunction"),
                        }

                        // Input to aggregate should be a projection computing id + 2
                        match &*agg.input {
                            LogicalPlan::Projection(inner_proj) => {
                                // Should have at least the group column and the computed expression
                                assert!(inner_proj.exprs.len() >= 2);

                                // Check for the id + 2 expression
                                let has_add_expr = inner_proj.exprs.iter().any(|e| {
                                    matches!(
                                        e,
                                        LogicalExpr::BinaryExpr {
                                            op: BinaryOperator::Add,
                                            ..
                                        }
                                    )
                                });
                                assert!(
                                    has_add_expr,
                                    "Should have id + 2 expression in inner projection"
                                );
                            }
                            _ => panic!("Expected inner Projection under Aggregate"),
                        }
                    }
                    _ => panic!("Expected Aggregate under outer Projection"),
                }
            }
            _ => panic!("Expected Projection at top level"),
        }
    }

    #[test]
    fn test_group_by_validation_allow_grouped_column() {
        let schema = create_test_schema();

        // Test that grouped columns are allowed
        let sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id";
        let result = parse_and_build(sql, &schema);

        assert!(result.is_ok(), "Should allow grouped column in SELECT");
    }

    #[test]
    fn test_group_by_validation_allow_constants() {
        let schema = create_test_schema();

        // Test that simple constants are allowed even when not grouped
        let sql = "SELECT user_id, 42, COUNT(*) FROM orders GROUP BY user_id";
        let result = parse_and_build(sql, &schema);

        assert!(
            result.is_ok(),
            "Should allow simple constants in SELECT with GROUP BY"
        );

        let sql_complex = "SELECT user_id, (100 + 50) * 2, COUNT(*) FROM orders GROUP BY user_id";
        let result_complex = parse_and_build(sql_complex, &schema);

        assert!(
            result_complex.is_ok(),
            "Should allow complex constant expressions in SELECT with GROUP BY"
        );
    }

    #[test]
    fn test_parenthesized_aggregate_expressions() {
        let schema = create_test_schema();

        let sql = "SELECT 25, (MAX(id) / 3), 39 FROM orders";
        let result = parse_and_build(sql, &schema);

        assert!(
            result.is_ok(),
            "Should handle parenthesized aggregate expressions"
        );

        let plan = result.unwrap();

        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 3);

                assert!(matches!(
                    proj.exprs[0],
                    LogicalExpr::Literal(Value::Integer(25))
                ));

                match &proj.exprs[1] {
                    LogicalExpr::BinaryExpr { left, op, right } => {
                        assert_eq!(*op, BinaryOperator::Divide);
                        assert!(matches!(&**left, LogicalExpr::Column(_)));
                        assert!(matches!(&**right, LogicalExpr::Literal(Value::Integer(3))));
                    }
                    _ => panic!("Expected BinaryExpr for (MAX(id) / 3)"),
                }

                assert!(matches!(
                    proj.exprs[2],
                    LogicalExpr::Literal(Value::Integer(39))
                ));

                match &*proj.input {
                    LogicalPlan::Aggregate(agg) => {
                        assert_eq!(agg.aggr_expr.len(), 1);
                        assert!(matches!(
                            agg.aggr_expr[0],
                            LogicalExpr::AggregateFunction {
                                fun: AggFunc::Max,
                                ..
                            }
                        ));
                    }
                    _ => panic!("Expected Aggregate node under Projection"),
                }
            }
            _ => panic!("Expected Projection at top level"),
        }
    }

    #[test]
    fn test_duplicate_aggregate_reuse() {
        let schema = create_test_schema();

        let sql = "SELECT (COUNT(*) - 225), 30, COUNT(*) FROM orders";
        let result = parse_and_build(sql, &schema);

        assert!(result.is_ok(), "Should handle duplicate aggregates");

        let plan = result.unwrap();

        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 3);

                match &proj.exprs[0] {
                    LogicalExpr::BinaryExpr { left, op, right } => {
                        assert_eq!(*op, BinaryOperator::Subtract);
                        match &**left {
                            LogicalExpr::Column(col) => {
                                assert!(col.name.starts_with("__agg_") || col.name == "COUNT(*)");
                            }
                            _ => panic!("Expected Column reference for COUNT(*)"),
                        }
                        assert!(matches!(
                            &**right,
                            LogicalExpr::Literal(Value::Integer(225))
                        ));
                    }
                    _ => panic!("Expected BinaryExpr for (COUNT(*) - 225)"),
                }

                assert!(matches!(
                    proj.exprs[1],
                    LogicalExpr::Literal(Value::Integer(30))
                ));

                match &proj.exprs[2] {
                    LogicalExpr::Column(col) => {
                        assert!(col.name.starts_with("__agg_") || col.name == "COUNT(*)");
                    }
                    _ => panic!("Expected Column reference for COUNT(*)"),
                }

                match &*proj.input {
                    LogicalPlan::Aggregate(agg) => {
                        assert_eq!(
                            agg.aggr_expr.len(),
                            1,
                            "Should have only one COUNT(*) aggregate"
                        );
                        assert!(matches!(
                            agg.aggr_expr[0],
                            LogicalExpr::AggregateFunction {
                                fun: AggFunc::Count,
                                ..
                            }
                        ));
                    }
                    _ => panic!("Expected Aggregate node under Projection"),
                }
            }
            _ => panic!("Expected Projection at top level"),
        }
    }

    #[test]
    fn test_aggregate_without_group_by_allow_constants() {
        let schema = create_test_schema();

        // Test that constants are allowed with aggregates even without GROUP BY
        let sql = "SELECT 42, COUNT(*), MAX(amount) FROM orders";
        let result = parse_and_build(sql, &schema);

        assert!(
            result.is_ok(),
            "Should allow simple constants with aggregates without GROUP BY"
        );

        // Test complex constant expressions
        let sql_complex = "SELECT (9 / 6) % 5, COUNT(*), MAX(amount) FROM orders";
        let result_complex = parse_and_build(sql_complex, &schema);

        assert!(
            result_complex.is_ok(),
            "Should allow complex constant expressions with aggregates without GROUP BY"
        );
    }

    #[test]
    fn test_aggregate_without_group_by_creates_aggregate_node() {
        let schema = create_test_schema();

        // Test that aggregate without GROUP BY creates proper Aggregate node
        let sql = "SELECT MAX(amount) FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();

        // Should be: Aggregate -> TableScan (no projection needed for simple aggregate)
        match plan {
            LogicalPlan::Aggregate(agg) => {
                assert_eq!(agg.group_expr.len(), 0, "Should have no group expressions");
                assert_eq!(
                    agg.aggr_expr.len(),
                    1,
                    "Should have one aggregate expression"
                );
                assert_eq!(
                    agg.schema.column_count(),
                    1,
                    "Schema should have one column"
                );
            }
            _ => panic!("Expected Aggregate at top level (no projection)"),
        }
    }

    #[test]
    fn test_scalar_vs_aggregate_function_classification() {
        let schema = create_test_schema();

        // Test MIN/MAX with 1 argument - should be aggregate
        let sql = "SELECT MIN(amount) FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();
        match plan {
            LogicalPlan::Aggregate(agg) => {
                assert_eq!(agg.aggr_expr.len(), 1, "MIN(x) should be an aggregate");
                match &agg.aggr_expr[0] {
                    LogicalExpr::AggregateFunction { fun, args, .. } => {
                        assert!(matches!(fun, AggFunc::Min));
                        assert_eq!(args.len(), 1);
                    }
                    _ => panic!("Expected AggregateFunction"),
                }
            }
            _ => panic!("Expected Aggregate node for MIN(x)"),
        }

        // Test MIN/MAX with 2 arguments - should be scalar in projection
        let sql = "SELECT MIN(amount, user_id) FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();
        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 1, "Should have one projection expression");
                match &proj.exprs[0] {
                    LogicalExpr::ScalarFunction { fun, args } => {
                        assert_eq!(
                            fun.to_lowercase(),
                            "min",
                            "MIN(x,y) should be a scalar function"
                        );
                        assert_eq!(args.len(), 2);
                    }
                    _ => panic!("Expected ScalarFunction for MIN(x,y)"),
                }
            }
            _ => panic!("Expected Projection node for scalar MIN(x,y)"),
        }

        // Test MAX with 3 arguments - should be scalar
        let sql = "SELECT MAX(amount, user_id, id) FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();
        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 1);
                match &proj.exprs[0] {
                    LogicalExpr::ScalarFunction { fun, args } => {
                        assert_eq!(
                            fun.to_lowercase(),
                            "max",
                            "MAX(x,y,z) should be a scalar function"
                        );
                        assert_eq!(args.len(), 3);
                    }
                    _ => panic!("Expected ScalarFunction for MAX(x,y,z)"),
                }
            }
            _ => panic!("Expected Projection node for scalar MAX(x,y,z)"),
        }

        // Test that MIN with 0 args is treated as scalar (will fail later in execution)
        let sql = "SELECT MIN() FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();
        match plan {
            LogicalPlan::Projection(proj) => match &proj.exprs[0] {
                LogicalExpr::ScalarFunction { fun, args } => {
                    assert_eq!(fun.to_lowercase(), "min");
                    assert_eq!(args.len(), 0, "MIN() should be scalar with 0 args");
                }
                _ => panic!("Expected ScalarFunction for MIN()"),
            },
            _ => panic!("Expected Projection for MIN()"),
        }

        // Test other functions that are always aggregate (COUNT, SUM, AVG)
        let sql = "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();
        match plan {
            LogicalPlan::Aggregate(agg) => {
                assert_eq!(agg.aggr_expr.len(), 3, "Should have 3 aggregate functions");
                for expr in &agg.aggr_expr {
                    assert!(matches!(expr, LogicalExpr::AggregateFunction { .. }));
                }
            }
            _ => panic!("Expected Aggregate node"),
        }

        // Test scalar functions that are never aggregates (ABS, ROUND, etc.)
        let sql = "SELECT ABS(amount), ROUND(amount), LENGTH(product) FROM orders";
        let plan = parse_and_build(sql, &schema).unwrap();
        match plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 3, "Should have 3 scalar functions");
                for expr in &proj.exprs {
                    match expr {
                        LogicalExpr::ScalarFunction { .. } => {}
                        _ => panic!("Expected all ScalarFunctions"),
                    }
                }
            }
            _ => panic!("Expected Projection node for scalar functions"),
        }
    }

    #[test]
    fn test_mixed_aggregate_and_group_columns() {
        let schema = create_test_schema();

        // When selecting both aggregate and grouping columns
        let sql = "SELECT user_id, sum(id) FROM orders GROUP BY user_id";
        let plan = parse_and_build(sql, &schema).unwrap();

        // No projection needed - aggregate outputs exactly what we select
        match plan {
            LogicalPlan::Aggregate(agg) => {
                assert_eq!(agg.group_expr.len(), 1);
                assert_eq!(agg.aggr_expr.len(), 1);
                assert_eq!(agg.schema.column_count(), 2);
            }
            _ => panic!("Expected Aggregate (no projection)"),
        }
    }

    #[test]
    fn test_scalar_function_wrapping_aggregate_no_group_by() {
        // Test: SELECT HEX(SUM(age + 2)) FROM users
        // Expected structure:
        // Projection { exprs: [ScalarFunction(HEX, [Column])] }
        //   -> Aggregate { aggr_expr: [Sum(BinaryExpr(age + 2))], group_expr: [] }
        //     -> Projection { exprs: [BinaryExpr(age + 2)] }
        //       -> TableScan("users")

        let schema = create_test_schema();
        let sql = "SELECT HEX(SUM(age + 2)) FROM users";
        let mut parser = Parser::new(sql.as_bytes());
        let stmt = parser.next().unwrap().unwrap();

        let plan = match stmt {
            ast::Cmd::Stmt(stmt) => {
                let mut builder = LogicalPlanBuilder::new(&schema);
                builder.build_statement(&stmt).unwrap()
            }
            _ => panic!("Expected SQL statement"),
        };

        match &plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.exprs.len(), 1, "Should have one expression");

                match &proj.exprs[0] {
                    LogicalExpr::ScalarFunction { fun, args } => {
                        assert_eq!(fun, "HEX", "Outer function should be HEX");
                        assert_eq!(args.len(), 1, "HEX should have one argument");

                        match &args[0] {
                            LogicalExpr::Column(_) => {}
                            LogicalExpr::AggregateFunction { .. } => {
                                panic!("Aggregate function should not be embedded in projection! It should be in a separate Aggregate operator");
                            }
                            _ => panic!(
                                "Expected column reference as argument to HEX, got: {:?}",
                                args[0]
                            ),
                        }
                    }
                    _ => panic!("Expected ScalarFunction (HEX), got: {:?}", proj.exprs[0]),
                }

                match &*proj.input {
                    LogicalPlan::Aggregate(agg) => {
                        assert_eq!(agg.group_expr.len(), 0, "Should have no GROUP BY");
                        assert_eq!(
                            agg.aggr_expr.len(),
                            1,
                            "Should have one aggregate expression"
                        );

                        match &agg.aggr_expr[0] {
                            LogicalExpr::AggregateFunction {
                                fun,
                                args,
                                distinct,
                            } => {
                                assert_eq!(*fun, crate::function::AggFunc::Sum, "Should be SUM");
                                assert!(!distinct, "Should not be DISTINCT");
                                assert_eq!(args.len(), 1, "SUM should have one argument");

                                match &args[0] {
                                    LogicalExpr::Column(col) => {
                                        // When aggregate arguments are complex, they get pre-projected
                                        assert!(col.name.starts_with("__agg_arg_proj_"), 
                                               "Should reference pre-projected column, got: {}", col.name);
                                    }
                                    LogicalExpr::BinaryExpr { left, op, right } => {
                                        // Simple case without pre-projection (shouldn't happen with current implementation)
                                        assert_eq!(*op, ast::Operator::Add, "Should be addition");

                                        match (&**left, &**right) {
                                            (LogicalExpr::Column(col), LogicalExpr::Literal(val)) => {
                                                assert_eq!(col.name, "age", "Should reference age column");
                                                assert_eq!(*val, Value::Integer(2), "Should add 2");
                                            }
                                            _ => panic!("Expected age + 2"),
                                        }
                                    }
                                    _ => panic!("Expected Column reference or BinaryExpr for aggregate argument, got: {:?}", args[0]),
                                }
                            }
                            _ => panic!("Expected AggregateFunction"),
                        }

                        match &*agg.input {
                            LogicalPlan::TableScan(scan) => {
                                assert_eq!(scan.table_name, "users");
                            }
                            LogicalPlan::Projection(proj) => match &*proj.input {
                                LogicalPlan::TableScan(scan) => {
                                    assert_eq!(scan.table_name, "users");
                                }
                                _ => panic!("Expected TableScan under projection"),
                            },
                            _ => panic!("Expected TableScan or Projection under Aggregate"),
                        }
                    }
                    _ => panic!(
                        "Expected Aggregate operator under Projection, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection as top-level operator, got: {plan:?}"),
        }
    }
}
