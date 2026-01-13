//! WHERE clause conditions and generation strategies.

use proptest::prelude::*;
use std::fmt;

use crate::schema::{Column, Table};
use crate::value::{SqlValue, value_for_type};

/// Comparison operators for WHERE clauses.
#[derive(Debug, Clone, Copy)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonOp::Eq => write!(f, "="),
            ComparisonOp::Ne => write!(f, "!="),
            ComparisonOp::Lt => write!(f, "<"),
            ComparisonOp::Le => write!(f, "<="),
            ComparisonOp::Gt => write!(f, ">"),
            ComparisonOp::Ge => write!(f, ">="),
        }
    }
}

/// Logical operators for combining conditions.
#[derive(Debug, Clone, Copy)]
pub enum LogicalOp {
    And,
    Or,
}

impl fmt::Display for LogicalOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalOp::And => write!(f, "AND"),
            LogicalOp::Or => write!(f, "OR"),
        }
    }
}

/// A WHERE clause condition.
#[derive(Debug, Clone)]
pub enum Condition {
    Comparison {
        column: String,
        op: ComparisonOp,
        value: SqlValue,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Condition::Comparison { column, op, value } => {
                write!(f, "\"{column}\" {op} {value}")
            }
            Condition::IsNull { column } => write!(f, "\"{column}\" IS NULL"),
            Condition::IsNotNull { column } => write!(f, "\"{column}\" IS NOT NULL"),
            Condition::And(left, right) => write!(f, "({left} AND {right})"),
            Condition::Or(left, right) => write!(f, "({left} OR {right})"),
        }
    }
}

/// ORDER BY direction.
#[derive(Debug, Clone, Copy)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// An ORDER BY clause item.
#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub column: String,
    pub direction: OrderDirection,
}

impl fmt::Display for OrderByItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\" {}", self.column, self.direction)
    }
}

/// Generate a comparison operator.
pub fn comparison_op() -> impl Strategy<Value = ComparisonOp> {
    prop_oneof![
        Just(ComparisonOp::Eq),
        Just(ComparisonOp::Ne),
        Just(ComparisonOp::Lt),
        Just(ComparisonOp::Le),
        Just(ComparisonOp::Gt),
        Just(ComparisonOp::Ge),
    ]
}

/// Generate a logical operator.
pub fn logical_op() -> impl Strategy<Value = LogicalOp> {
    prop_oneof![Just(LogicalOp::And), Just(LogicalOp::Or),]
}

/// Generate an order direction.
pub fn order_direction() -> impl Strategy<Value = OrderDirection> {
    prop_oneof![Just(OrderDirection::Asc), Just(OrderDirection::Desc),]
}

/// Generate a simple condition for a column.
pub fn simple_condition(column: &Column) -> BoxedStrategy<Condition> {
    let col_name = column.name.clone();
    let col_name2 = column.name.clone();
    let col_name3 = column.name.clone();

    let comparison =
        (comparison_op(), value_for_type(&column.data_type, false)).prop_map(move |(op, value)| {
            Condition::Comparison {
                column: col_name.clone(),
                op,
                value,
            }
        });

    if column.nullable {
        prop_oneof![
            comparison,
            Just(Condition::IsNull {
                column: col_name2.clone()
            }),
            Just(Condition::IsNotNull {
                column: col_name3.clone()
            }),
        ]
        .boxed()
    } else {
        comparison.boxed()
    }
}

/// Generate a condition tree for a table (recursive, bounded depth).
pub fn condition_for_table(table: &Table, max_depth: u32) -> BoxedStrategy<Condition> {
    let filterable = table.filterable_columns();
    if filterable.is_empty() {
        // No filterable columns: generate a tautology
        return Just(Condition::Comparison {
            column: "1".to_string(),
            op: ComparisonOp::Eq,
            value: SqlValue::Integer(1),
        })
        .boxed();
    }

    // Create leaf strategies from all filterable columns
    let leaves: Vec<BoxedStrategy<Condition>> =
        filterable.iter().map(|c| simple_condition(c)).collect();

    let leaf = proptest::strategy::Union::new(leaves).boxed();

    if max_depth == 0 {
        return leaf;
    }

    // Recursive case
    let table_clone = table.clone();
    leaf.prop_flat_map(move |left| {
        let table_inner = table_clone.clone();
        condition_for_table(&table_inner, max_depth - 1)
            .prop_map(move |right| Condition::And(Box::new(left.clone()), Box::new(right)))
    })
    .boxed()
}

/// Generate an optional WHERE clause for a table.
pub fn optional_where_clause(table: &Table) -> BoxedStrategy<Option<Condition>> {
    let table_clone = table.clone();
    prop_oneof![
        Just(None),
        condition_for_table(&table_clone, 2).prop_map(Some),
    ]
    .boxed()
}

/// Generate ORDER BY items for a table.
pub fn order_by_for_table(table: &Table) -> BoxedStrategy<Vec<OrderByItem>> {
    let filterable = table.filterable_columns();
    if filterable.is_empty() {
        return Just(vec![]).boxed();
    }

    let col_names: Vec<String> = filterable.iter().map(|c| c.name.clone()).collect();

    proptest::collection::vec(
        (proptest::sample::select(col_names), order_direction()),
        0..=3,
    )
    .prop_map(|items| {
        // Deduplicate columns in ORDER BY
        let mut seen = std::collections::HashSet::new();
        items
            .into_iter()
            .filter_map(|(column, direction)| {
                if seen.insert(column.clone()) {
                    Some(OrderByItem { column, direction })
                } else {
                    None
                }
            })
            .collect()
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_condition_display() {
        let cond = Condition::Comparison {
            column: "age".to_string(),
            op: ComparisonOp::Gt,
            value: SqlValue::Integer(18),
        };
        assert_eq!(cond.to_string(), "\"age\" > 18");

        let null_cond = Condition::IsNull {
            column: "email".to_string(),
        };
        assert_eq!(null_cond.to_string(), "\"email\" IS NULL");

        let and_cond = Condition::And(Box::new(cond.clone()), Box::new(null_cond));
        assert_eq!(and_cond.to_string(), "(\"age\" > 18 AND \"email\" IS NULL)");
    }
}
