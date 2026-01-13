//! SELECT statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::condition::{Condition, OrderByItem, optional_where_clause, order_by_for_table};
use crate::schema::Table;

/// A SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub table: String,
    pub columns: Vec<String>,
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
            let cols: Vec<String> = self.columns.iter().map(|c| format!("\"{c}\"")).collect();
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

/// Generate a SELECT statement for a table.
pub fn select_for_table(table: &Table) -> BoxedStrategy<SelectStatement> {
    let table_name = table.name.clone();
    let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();

    let columns_strategy = prop_oneof![
        Just(vec![]), // SELECT *
        proptest::sample::subsequence(col_names.clone(), 1..=col_names.len())
            .prop_map(|v| v.into_iter().collect::<Vec<_>>()),
    ];

    let table_for_where = table.clone();
    let table_for_order = table.clone();

    (
        columns_strategy,
        optional_where_clause(&table_for_where),
        order_by_for_table(&table_for_order),
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
            columns: vec!["id".to_string(), "name".to_string()],
            where_clause: Some(Condition::Comparison {
                column: "age".to_string(),
                op: ComparisonOp::Ge,
                value: SqlValue::Integer(21),
            }),
            order_by: vec![OrderByItem {
                column: "name".to_string(),
                direction: OrderDirection::Asc,
            }],
            limit: Some(10),
            offset: Some(5),
        };

        let sql = stmt.to_string();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"age\" >= 21 ORDER BY \"name\" ASC LIMIT 10 OFFSET 5"
        );
    }
}
