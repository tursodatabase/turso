//! SELECT statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::condition::{Condition, OrderByItem, optional_where_clause, order_by_for_table};
use crate::expression::{Expression, ExpressionContext};
use crate::function::builtin_functions;
use crate::schema::Table;

/// A SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub table: String,
    /// The columns/expressions in the SELECT list. Empty means SELECT *.
    pub columns: Vec<Expression>,
    pub where_clause: Option<Condition>,
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

/// Generate a SELECT statement for a table with expression support.
///
/// This generates function calls and other expressions in the SELECT list.
pub fn select_for_table(table: &Table) -> BoxedStrategy<SelectStatement> {
    let table_name = table.name.clone();
    let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let functions = builtin_functions();

    // Build expression context for generating expressions
    let ctx = ExpressionContext::new(functions)
        .with_columns(table.columns.clone())
        .with_max_depth(2)
        .with_aggregates(true);

    // Generate either SELECT * or a list of expressions
    let columns_strategy = prop_oneof![
        3 => Just(vec![]), // SELECT * (weighted less)
        7 => proptest::collection::vec(
            crate::expression::expression(&ctx),
            1..=5
        ),
        5 => proptest::sample::subsequence(col_names.clone(), 1..=col_names.len())
            .prop_map(|cols| cols.into_iter().map(Expression::Column).collect::<Vec<_>>()),
    ];

    (
        columns_strategy,
        optional_where_clause(table),
        order_by_for_table(table),
        proptest::option::of(1u32..1000),
        proptest::option::of(0u32..100),
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
    use crate::condition::{ComparisonOp, OrderDirection};
    use crate::value::SqlValue;

    #[test]
    fn test_select_display() {
        let stmt = SelectStatement {
            table: "users".to_string(),
            columns: vec![
                Expression::Column("id".to_string()),
                Expression::Column("name".to_string()),
            ],
            where_clause: Some(Condition::SimpleComparison {
                column: "age".to_string(),
                op: ComparisonOp::Ge,
                value: SqlValue::Integer(21),
            }),
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
                select_for_table(&table)
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
        let strategy = select_for_table(&table);

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
