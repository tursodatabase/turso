//! Contains code regarding generation for [ast::Expr::Unary] Predicate
//! TODO: for now just generating [ast::Literal], but want to also generate Columns and any
//! arbitrary [ast::Expr]

use turso_parser::ast::{self, Expr};

use crate::{
    generation::{
        backtrack, pick, predicate::SimplePredicate, ArbitraryFromMaybe, GenerationContext,
    },
    model::{
        query::predicate::Predicate,
        table::{SimValue, TableContext},
    },
};

pub struct TrueValue(pub SimValue);

impl ArbitraryFromMaybe<&SimValue> for TrueValue {
    fn arbitrary_from_maybe<R: rand::Rng + ?Sized, C: GenerationContext>(
        _rng: &mut R,
        _context: &C,
        value: &SimValue,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        // If the Value is a true value return it else you cannot return a true Value
        value.as_bool().then_some(Self(value.clone()))
    }
}

impl ArbitraryFromMaybe<&Vec<&SimValue>> for TrueValue {
    fn arbitrary_from_maybe<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        values: &Vec<&SimValue>,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return Some(Self(SimValue::TRUE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, context, *value)
    }
}

pub struct FalseValue(pub SimValue);

impl ArbitraryFromMaybe<&SimValue> for FalseValue {
    fn arbitrary_from_maybe<R: rand::Rng + ?Sized, C: GenerationContext>(
        _rng: &mut R,
        _context: &C,
        value: &SimValue,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        // If the Value is a false value return it else you cannot return a false Value
        (!value.as_bool()).then_some(Self(value.clone()))
    }
}

impl ArbitraryFromMaybe<&Vec<&SimValue>> for FalseValue {
    fn arbitrary_from_maybe<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        values: &Vec<&SimValue>,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return Some(Self(SimValue::FALSE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, context, *value)
    }
}

#[allow(dead_code)]
pub struct BitNotValue(pub SimValue);

impl ArbitraryFromMaybe<(&SimValue, bool)> for BitNotValue {
    fn arbitrary_from_maybe<R: rand::Rng + ?Sized, C: GenerationContext>(
        _rng: &mut R,
        _context: &C,
        (value, predicate): (&SimValue, bool),
    ) -> Option<Self>
    where
        Self: Sized,
    {
        let bit_not_val = value.unary_exec(ast::UnaryOperator::BitwiseNot);
        // If you bit not the Value and it meets the predicate return Some, else None
        (bit_not_val.as_bool() == predicate).then_some(BitNotValue(value.clone()))
    }
}

impl ArbitraryFromMaybe<(&Vec<&SimValue>, bool)> for BitNotValue {
    fn arbitrary_from_maybe<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        (values, predicate): (&Vec<&SimValue>, bool),
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return None;
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, context, (*value, predicate))
    }
}

// TODO: have some more complex generation with columns names here as well
impl SimplePredicate {
    /// Generates a true [ast::Expr::Unary] [SimplePredicate] from a [TableContext] for some values in the table
    pub fn true_unary<R: rand::Rng + ?Sized, C: GenerationContext, T: TableContext>(
        rng: &mut R,
        context: &C,
        _table: &T,
        row: &[SimValue],
    ) -> Self {
        // Pick a random column
        let column_index = rng.random_range(0..row.len());
        let column_value = &row[column_index];
        let num_retries = row.len();
        // Avoid creation of NULLs
        if row.is_empty() {
            return SimplePredicate(Predicate(Expr::Literal(SimValue::TRUE.into())));
        }
        let expr = backtrack(
            vec![
                (
                    num_retries,
                    Box::new(|rng| {
                        TrueValue::arbitrary_from_maybe(rng, context, column_value).map(|value| {
                            assert!(value.0.as_bool());
                            // Positive is a no-op in Sqlite
                            Expr::unary(ast::UnaryOperator::Positive, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                // (
                //     num_retries,
                //     Box::new(|rng| {
                //         TrueValue::arbitrary_from_maybe(rng, column_value).map(|value| {
                //             assert!(value.0.as_bool());
                //             // True Value with negative is still True
                //             Expr::unary(ast::UnaryOperator::Negative, Expr::Literal(value.0.into()))
                //         })
                //     }),
                // ),
                // (
                //     num_retries,
                //     Box::new(|rng| {
                //         BitNotValue::arbitrary_from_maybe(rng, (column_value, true)).map(|value| {
                //             Expr::unary(
                //                 ast::UnaryOperator::BitwiseNot,
                //                 Expr::Literal(value.0.into()),
                //             )
                //         })
                //     }),
                // ),
                (
                    num_retries,
                    Box::new(|rng| {
                        FalseValue::arbitrary_from_maybe(rng, context, column_value).map(|value| {
                            assert!(!value.0.as_bool());
                            Expr::unary(ast::UnaryOperator::Not, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
            ],
            rng,
        );
        // If cannot generate a value
        SimplePredicate(Predicate(
            expr.unwrap_or(Expr::Literal(SimValue::TRUE.into())),
        ))
    }

    /// Generates a false [ast::Expr::Unary] [SimplePredicate] from a [TableContext] for a row in the table
    pub fn false_unary<R: rand::Rng + ?Sized, C: GenerationContext, T: TableContext>(
        rng: &mut R,
        context: &C,
        _table: &T,
        row: &[SimValue],
    ) -> Self {
        // Avoid creation of NULLs
        if row.is_empty() {
            return SimplePredicate(Predicate(Expr::Literal(SimValue::FALSE.into())));
        }
        // Pick a random column
        let column_index = rng.random_range(0..row.len());
        let column_value = &row[column_index];
        let num_retries = row.len();
        let expr = backtrack(
            vec![
                // (
                //     num_retries,
                //     Box::new(|rng| {
                //         FalseValue::arbitrary_from_maybe(rng, column_value).map(|value| {
                //             assert!(!value.0.as_bool());
                //             // Positive is a no-op in Sqlite
                //             Expr::unary(ast::UnaryOperator::Positive, Expr::Literal(value.0.into()))
                //         })
                //     }),
                // ),
                // (
                //     num_retries,
                //     Box::new(|rng| {
                //         FalseValue::arbitrary_from_maybe(rng, column_value).map(|value| {
                //             assert!(!value.0.as_bool());
                //             // True Value with negative is still True
                //             Expr::unary(ast::UnaryOperator::Negative, Expr::Literal(value.0.into()))
                //         })
                //     }),
                // ),
                // (
                //     num_retries,
                //     Box::new(|rng| {
                //         BitNotValue::arbitrary_from_maybe(rng, (column_value, false)).map(|value| {
                //             Expr::unary(
                //                 ast::UnaryOperator::BitwiseNot,
                //                 Expr::Literal(value.0.into()),
                //             )
                //         })
                //     }),
                // ),
                (
                    num_retries,
                    Box::new(|rng| {
                        TrueValue::arbitrary_from_maybe(rng, context, column_value).map(|value| {
                            assert!(value.0.as_bool());
                            Expr::unary(ast::UnaryOperator::Not, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
            ],
            rng,
        );
        // If cannot generate a value
        SimplePredicate(Predicate(
            expr.unwrap_or(Expr::Literal(SimValue::FALSE.into())),
        ))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/generation/predicate/unary/tests.rs"]
mod tests;
