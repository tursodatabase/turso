//! Contains code regarding generation for [ast::Expr::Unary] Predicate
//! TODO: for now just generating [ast::Literal], but want to also generate Columns and any
//! arbitrary [ast::Expr]

use limbo_sqlite3_parser::ast::{self, Expr};

use crate::{
    generation::{backtrack, pick, predicate::SimplePredicate, ArbitraryFromMaybe},
    model::{
        query::predicate::Predicate,
        table::{SimValue, Table},
    },
};

pub struct TrueValue(pub SimValue);

impl ArbitraryFromMaybe<&SimValue> for TrueValue {
    fn arbitrary_from_maybe<R: rand::Rng>(_rng: &mut R, value: &SimValue) -> Option<Self>
    where
        Self: Sized,
    {
        // If the Value is a true value return it else you cannot return a true Value
        value.as_bool().then_some(Self(value.clone()))
    }
}

impl ArbitraryFromMaybe<&Vec<&SimValue>> for TrueValue {
    fn arbitrary_from_maybe<R: rand::Rng>(rng: &mut R, values: &Vec<&SimValue>) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return Some(Self(SimValue::TRUE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, *value)
    }
}

pub struct FalseValue(pub SimValue);

impl ArbitraryFromMaybe<&SimValue> for FalseValue {
    fn arbitrary_from_maybe<R: rand::Rng>(_rng: &mut R, value: &SimValue) -> Option<Self>
    where
        Self: Sized,
    {
        // If the Value is a false value return it else you cannot return a false Value
        (!value.as_bool()).then_some(Self(value.clone()))
    }
}

impl ArbitraryFromMaybe<&Vec<&SimValue>> for FalseValue {
    fn arbitrary_from_maybe<R: rand::Rng>(rng: &mut R, values: &Vec<&SimValue>) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return Some(Self(SimValue::FALSE));
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, *value)
    }
}

pub struct BitNotValue(pub SimValue);

impl ArbitraryFromMaybe<(&SimValue, bool)> for BitNotValue {
    fn arbitrary_from_maybe<R: rand::Rng>(
        _rng: &mut R,
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
    fn arbitrary_from_maybe<R: rand::Rng>(
        rng: &mut R,
        (values, predicate): (&Vec<&SimValue>, bool),
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if values.is_empty() {
            return None;
        }

        let value = pick(values, rng);
        Self::arbitrary_from_maybe(rng, (*value, predicate))
    }
}

// TODO: have some more complex generation with columns names here as well
impl SimplePredicate {
    /// Generates a true [ast::Expr::Unary] [SimplePredicate] from a [Table] for some values in the table
    pub fn true_unary<R: rand::Rng>(rng: &mut R, table: &Table, row: &[SimValue]) -> Self {
        // Pick a random column
        let column_index = rng.gen_range(0..table.columns.len());
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
                        TrueValue::arbitrary_from_maybe(rng, column_value).map(|value| {
                            assert!(value.0.as_bool());
                            // Positive is a no-op in Sqlite
                            Expr::unary(ast::UnaryOperator::Positive, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        TrueValue::arbitrary_from_maybe(rng, column_value).map(|value| {
                            assert!(value.0.as_bool());
                            // True Value with negative is still True
                            Expr::unary(ast::UnaryOperator::Negative, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        BitNotValue::arbitrary_from_maybe(rng, (column_value, true)).map(|value| {
                            Expr::unary(
                                ast::UnaryOperator::BitwiseNot,
                                Expr::Literal(value.0.into()),
                            )
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        FalseValue::arbitrary_from_maybe(rng, column_value).map(|value| {
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

    /// Generates a false [ast::Expr::Unary] [SimplePredicate] from a [Table] for a row in the table
    pub fn false_unary<R: rand::Rng>(rng: &mut R, table: &Table, row: &[SimValue]) -> Self {
        // Pick a random column
        let column_index = rng.gen_range(0..table.columns.len());
        let column_value = &row[column_index];
        let num_retries = row.len();
        // Avoid creation of NULLs
        if row.is_empty() {
            return SimplePredicate(Predicate(Expr::Literal(SimValue::FALSE.into())));
        }
        let expr = backtrack(
            vec![
                (
                    num_retries,
                    Box::new(|rng| {
                        FalseValue::arbitrary_from_maybe(rng, column_value).map(|value| {
                            assert!(!value.0.as_bool());
                            // Positive is a no-op in Sqlite
                            Expr::unary(ast::UnaryOperator::Positive, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        FalseValue::arbitrary_from_maybe(rng, column_value).map(|value| {
                            assert!(!value.0.as_bool());
                            // True Value with negative is still True
                            Expr::unary(ast::UnaryOperator::Negative, Expr::Literal(value.0.into()))
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        BitNotValue::arbitrary_from_maybe(rng, (column_value, false)).map(|value| {
                            Expr::unary(
                                ast::UnaryOperator::BitwiseNot,
                                Expr::Literal(value.0.into()),
                            )
                        })
                    }),
                ),
                (
                    num_retries,
                    Box::new(|rng| {
                        TrueValue::arbitrary_from_maybe(rng, column_value).map(|value| {
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
mod tests {
    use rand::{Rng as _, SeedableRng as _};
    use rand_chacha::ChaCha8Rng;

    use crate::{
        generation::{pick, predicate::SimplePredicate, Arbitrary, ArbitraryFrom as _},
        model::table::{SimValue, Table},
    };

    fn get_seed() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn fuzz_true_unary_simple_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        for _ in 0..10000 {
            let mut table = Table::arbitrary(&mut rng);
            let num_rows = rng.gen_range(1..10);
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(&mut rng, &c.column_type))
                        .collect()
                })
                .collect();
            table.rows.extend(values.clone());
            let row = pick(&table.rows, &mut rng);
            let predicate = SimplePredicate::true_unary(&mut rng, &table, row);
            let result = values
                .iter()
                .map(|row| predicate.0.test(row, &table))
                .reduce(|accum, curr| accum || curr)
                .unwrap_or(false);
            assert!(result, "Predicate: {:#?}\nSeed: {}", predicate, seed)
        }
    }

    #[test]
    fn fuzz_false_unary_simple_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        for _ in 0..10000 {
            let mut table = Table::arbitrary(&mut rng);
            let num_rows = rng.gen_range(1..10);
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(&mut rng, &c.column_type))
                        .collect()
                })
                .collect();
            table.rows.extend(values.clone());
            let row = pick(&table.rows, &mut rng);
            let predicate = SimplePredicate::false_unary(&mut rng, &table, row);
            let result = values
                .iter()
                .map(|row| predicate.0.test(row, &table))
                .any(|res| !res);
            assert!(result, "Predicate: {:#?}\nSeed: {}", predicate, seed)
        }
    }
}
