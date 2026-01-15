//! SELECT statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;
use std::ops::RangeInclusive;

use crate::condition::{OrderByItem, optional_where_clause, order_by_for_table};
use crate::expression::{Expression, ExpressionContext, ExpressionProfile};
use crate::function::builtin_functions;
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};

// =============================================================================
// SELECT STATEMENT PROFILE
// =============================================================================

/// Profile for controlling SELECT statement generation.
#[derive(Debug, Clone)]
pub struct SelectProfile {
    /// Maximum depth for expressions in SELECT list.
    pub expression_max_depth: u32,
    /// Whether to allow aggregate functions.
    pub allow_aggregates: bool,
    /// Weight for SELECT * (empty column list).
    pub select_star_weight: u32,
    /// Weight for expression list in SELECT.
    pub expression_list_weight: u32,
    /// Weight for column subsequence in SELECT.
    pub column_list_weight: u32,
    /// Range for number of expressions in SELECT list.
    pub expression_count_range: RangeInclusive<usize>,
    /// Range for LIMIT clause values.
    pub limit_range: RangeInclusive<u32>,
    /// Range for OFFSET clause values.
    pub offset_range: RangeInclusive<u32>,
    /// Expression profile for SELECT expressions.
    pub expression_profile: ExpressionProfile,
}

impl Default for SelectProfile {
    fn default() -> Self {
        Self {
            expression_max_depth: 2,
            allow_aggregates: true,
            select_star_weight: 3,
            expression_list_weight: 7,
            column_list_weight: 5,
            expression_count_range: 1..=5,
            limit_range: 1..=1000,
            offset_range: 0..=100,
            expression_profile: ExpressionProfile::default(),
        }
    }
}

impl SelectProfile {
    /// Create a profile for simple SELECT queries.
    pub fn simple() -> Self {
        Self {
            expression_max_depth: 1,
            allow_aggregates: false,
            select_star_weight: 5,
            expression_list_weight: 3,
            column_list_weight: 7,
            expression_count_range: 1..=3,
            limit_range: 1..=100,
            offset_range: 0..=10,
            expression_profile: ExpressionProfile::simple(),
        }
    }

    /// Create a profile for complex SELECT queries.
    pub fn complex() -> Self {
        Self {
            expression_max_depth: 4,
            allow_aggregates: true,
            select_star_weight: 1,
            expression_list_weight: 10,
            column_list_weight: 3,
            expression_count_range: 1..=10,
            limit_range: 1..=10000,
            offset_range: 0..=1000,
            expression_profile: ExpressionProfile::function_heavy(),
        }
    }

    /// Builder method to set expression max depth.
    pub fn with_expression_max_depth(mut self, depth: u32) -> Self {
        self.expression_max_depth = depth;
        self
    }

    /// Builder method to set whether aggregates are allowed.
    pub fn with_aggregates(mut self, allow: bool) -> Self {
        self.allow_aggregates = allow;
        self
    }

    /// Builder method to set SELECT * weight.
    pub fn with_select_star_weight(mut self, weight: u32) -> Self {
        self.select_star_weight = weight;
        self
    }

    /// Builder method to set expression list weight.
    pub fn with_expression_list_weight(mut self, weight: u32) -> Self {
        self.expression_list_weight = weight;
        self
    }

    /// Builder method to set column list weight.
    pub fn with_column_list_weight(mut self, weight: u32) -> Self {
        self.column_list_weight = weight;
        self
    }

    /// Builder method to set expression count range.
    pub fn with_expression_count_range(mut self, range: RangeInclusive<usize>) -> Self {
        self.expression_count_range = range;
        self
    }

    /// Builder method to set LIMIT range.
    pub fn with_limit_range(mut self, range: RangeInclusive<u32>) -> Self {
        self.limit_range = range;
        self
    }

    /// Builder method to set OFFSET range.
    pub fn with_offset_range(mut self, range: RangeInclusive<u32>) -> Self {
        self.offset_range = range;
        self
    }

    /// Builder method to set expression profile.
    pub fn with_expression_profile(mut self, profile: ExpressionProfile) -> Self {
        self.expression_profile = profile;
        self
    }
}

/// A SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub table: String,
    /// The columns/expressions in the SELECT list. Empty means SELECT *.
    pub columns: Vec<Expression>,
    pub where_clause: Option<Expression>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl SelectStatement {
    /// Returns true if this SELECT has a LIMIT clause without an ORDER BY.
    ///
    /// Queries with LIMIT but no ORDER BY may return different rows between
    /// database implementations since the order is undefined.
    pub fn has_unordered_limit(&self) -> bool {
        self.limit.is_some() && self.order_by.is_empty()
    }
}

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ")?;

        if self.columns.is_empty() {
            write!(f, "*")?;
        } else {
            let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
            write!(f, "{}", cols.join(", "))?;
        }

        write!(f, " FROM \"{}\"", self.table)?;

        if let Some(cond) = &self.where_clause {
            write!(f, " WHERE {cond}")?;
        }

        if !self.order_by.is_empty() {
            let orders: Vec<String> = self.order_by.iter().map(|o| o.to_string()).collect();
            write!(f, " ORDER BY {}", orders.join(", "))?;
        }

        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }

        if let Some(offset) = self.offset {
            write!(f, " OFFSET {offset}")?;
        }

        Ok(())
    }
}

/// Generate a SELECT statement for a table with profile.
pub fn select_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<SelectStatement> {
    let table_name = table.name.clone();
    let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let functions = builtin_functions();

    // Extract profile values from the SelectProfile
    let select_profile = profile.select_profile();
    let expression_max_depth = select_profile.expression_max_depth;
    let allow_aggregates = select_profile.allow_aggregates;
    let select_star_weight = select_profile.select_star_weight;
    let expression_list_weight = select_profile.expression_list_weight;
    let column_list_weight = select_profile.column_list_weight;
    let expression_count_range = select_profile.expression_count_range.clone();
    let limit_range = select_profile.limit_range.clone();
    let offset_range = select_profile.offset_range.clone();

    // Build expression context for generating expressions
    let ctx = ExpressionContext::new(functions)
        .with_columns(table.columns.clone())
        .with_max_depth(expression_max_depth)
        .with_aggregates(allow_aggregates);

    // Generate either SELECT * or a list of expressions
    let columns_strategy = proptest::strategy::Union::new_weighted(vec![
        (
            select_star_weight,
            Just(vec![]).boxed(), // SELECT *
        ),
        (
            expression_list_weight,
            proptest::collection::vec(crate::expression::expression(&ctx), expression_count_range)
                .boxed(),
        ),
        (
            column_list_weight,
            proptest::sample::subsequence(col_names.clone(), 1..=col_names.len())
                .prop_map(|cols| cols.into_iter().map(Expression::Column).collect::<Vec<_>>())
                .boxed(),
        ),
    ]);

    (
        columns_strategy,
        optional_where_clause(table, schema, profile),
        order_by_for_table(table, profile),
        proptest::option::of(limit_range),
        proptest::option::of(offset_range),
    )
        .prop_map(
            move |(columns, where_clause, order_by, limit, offset)| SelectStatement {
                table: table_name.clone(),
                columns,
                where_clause,
                order_by,
                limit,
                offset: if limit.is_some() { offset } else { None },
            },
        )
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::OrderDirection;
    use crate::expression::BinaryOperator;
    use crate::value::SqlValue;

    #[test]
    fn test_select_display() {
        let stmt = SelectStatement {
            table: "users".to_string(),
            columns: vec![
                Expression::Column("id".to_string()),
                Expression::Column("name".to_string()),
            ],
            where_clause: Some(Expression::binary(
                Expression::Column("age".to_string()),
                BinaryOperator::Ge,
                Expression::Value(SqlValue::Integer(21)),
            )),
            order_by: vec![OrderByItem::column("name", OrderDirection::Asc)],
            limit: Some(10),
            offset: Some(5),
        };

        let sql = stmt.to_string();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"age\" >= 21 ORDER BY \"name\" ASC LIMIT 10 OFFSET 5"
        );
    }

    #[test]
    fn test_select_with_function() {
        let stmt = SelectStatement {
            table: "users".to_string(),
            columns: vec![
                Expression::Column("id".to_string()),
                Expression::function_call("UPPER", vec![Expression::Column("name".to_string())]),
            ],
            where_clause: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let sql = stmt.to_string();
        assert_eq!(sql, "SELECT \"id\", UPPER(\"name\") FROM \"users\"");
    }

    proptest::proptest! {
        #[test]
        fn generated_select_is_valid(
            stmt in {
                let table = crate::schema::Table::new(
                    "test",
                    vec![
                        crate::schema::ColumnDef::new("id", crate::schema::DataType::Integer).primary_key(),
                        crate::schema::ColumnDef::new("name", crate::schema::DataType::Text),
                        crate::schema::ColumnDef::new("age", crate::schema::DataType::Integer),
                    ],
                );
                let schema = crate::schema::SchemaBuilder::new().add_table(table.clone()).build();
                let table_ref: crate::schema::TableRef = table.into();
                select_for_table(&table_ref, &schema, &StatementProfile::default())
            }
        ) {
            let sql = stmt.to_string();
            proptest::prop_assert!(sql.starts_with("SELECT"));
            proptest::prop_assert!(sql.contains("FROM \"test\""));
        }
    }

    #[test]
    fn test_select_generates_functions() {
        use proptest::strategy::Strategy;
        use proptest::test_runner::TestRunner;

        let table = crate::schema::Table::new(
            "test",
            vec![
                crate::schema::ColumnDef::new("id", crate::schema::DataType::Integer).primary_key(),
                crate::schema::ColumnDef::new("name", crate::schema::DataType::Text),
                crate::schema::ColumnDef::new("age", crate::schema::DataType::Integer),
            ],
        );
        let schema = crate::schema::SchemaBuilder::new()
            .add_table(table.clone())
            .build();
        let table_ref: crate::schema::TableRef = table.into();
        let strategy = select_for_table(&table_ref, &schema, &StatementProfile::default());

        let mut runner = TestRunner::default();
        let mut found_function = false;

        for _ in 0..50 {
            let stmt = strategy.new_tree(&mut runner).unwrap().current();
            let sql = stmt.to_string();
            // Check if any column expression contains a function (has parentheses that aren't just quotes)
            if sql.contains("(") && !sql.starts_with("SELECT *") {
                found_function = true;
                println!("Found function in: {sql}");
                break;
            }
        }

        assert!(
            found_function,
            "Expected to generate at least one SELECT with function calls in 50 attempts"
        );
    }
}
