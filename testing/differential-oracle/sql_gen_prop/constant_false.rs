//! WHERE clauses for DELETE and UPDATE that are false for every row.
//!
//! An engine can see that such a WHERE never matches and skip the whole row
//! loop at plan time. Code that runs after the loop must then not depend on
//! anything the loop opens.

use proptest::prelude::*;

use crate::expression::{Expression, UnaryOperator};
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};
use crate::select::{condition_for_table, optional_where_clause};
use crate::value::SqlValue;

/// Weight of the usual optional WHERE against
/// `StatementProfile::constant_false_where_weight`.
const USUAL_WHERE_WEIGHT: u32 = 10;

/// The WHERE clause of a DELETE or UPDATE. With a constant-false weight of 0
/// this is exactly `optional_where_clause`.
pub fn delete_or_update_where_clause(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<Option<Expression>> {
    let usual = optional_where_clause(table, schema, profile);
    if profile.constant_false_where_weight == 0 {
        return usual;
    }
    prop_oneof![
        USUAL_WHERE_WEIGHT => usual,
        profile.constant_false_where_weight => constant_false_condition(table, schema, profile).prop_map(Some),
    ]
    .boxed()
}

/// `0`, `NOT 1`, `NULL`, or `0 AND (<condition>)`.
fn constant_false_condition(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<Expression> {
    let zero = Expression::Value(SqlValue::Integer(0));
    let not_one = Expression::UnaryOp {
        op: UnaryOperator::Not,
        operand: Box::new(Expression::Value(SqlValue::Integer(1))),
    };
    let zero_and = condition_for_table(table, schema, profile).prop_map(|condition| {
        Expression::and(
            Expression::Value(SqlValue::Integer(0)),
            Expression::Parenthesized(Box::new(condition)),
        )
    });
    prop_oneof![
        Just(zero),
        Just(not_one),
        Just(Expression::Value(SqlValue::Null)),
        zero_and,
    ]
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    use super::*;
    use crate::Table;
    use crate::delete::{DeleteStatement, delete_for_table};
    use crate::schema::{ColumnDef, DataType, SchemaBuilder};

    fn table_t() -> (TableRef, Schema) {
        let table = Table::new(
            "t",
            vec![
                ColumnDef::new("id", DataType::Integer).primary_key(),
                ColumnDef::new("v", DataType::Text),
            ],
        );
        let schema = SchemaBuilder::new().add_table(table.clone()).build();
        (table.into(), schema)
    }

    fn generated_deletes(constant_false_where_weight: u32) -> Vec<String> {
        let (table, schema) = table_t();
        let profile = StatementProfile {
            constant_false_where_weight,
            ..StatementProfile::default()
        };
        let strategy = delete_for_table(&table, &schema, &profile);
        let mut runner = TestRunner::deterministic();
        (0..500)
            .map(|_| {
                strategy
                    .new_tree(&mut runner)
                    .unwrap()
                    .current()
                    .to_string()
            })
            .collect()
    }

    fn constant_false_forms(deletes: &[String]) -> BTreeSet<&'static str> {
        deletes
            .iter()
            .filter_map(|sql| match sql.split_once(" WHERE ")?.1 {
                "0" => Some("0"),
                "NOT 1" => Some("NOT 1"),
                "NULL" => Some("NULL"),
                condition if condition.starts_with("0 AND (") => Some("0 AND"),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_positive_weight_generates_every_constant_false_form() {
        assert_eq!(
            constant_false_forms(&generated_deletes(5)),
            BTreeSet::from(["0", "0 AND", "NOT 1", "NULL"])
        );
    }

    #[test]
    fn weight_zero_generates_the_same_statements_as_the_usual_where_clause() {
        let (table, schema) = table_t();
        let profile = StatementProfile::default();
        let mut runner = TestRunner::deterministic();
        let usual: Vec<String> = (0..500)
            .map(|_| {
                let where_clause = optional_where_clause(&table, &schema, &profile)
                    .new_tree(&mut runner)
                    .unwrap()
                    .current();
                DeleteStatement {
                    table: "t".to_string(),
                    where_clause,
                }
                .to_string()
            })
            .collect();

        let deletes = generated_deletes(0);

        assert_eq!(deletes, usual);
        assert!(constant_false_forms(&deletes).is_empty(), "{deletes:?}");
    }
}
