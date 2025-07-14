//! Contains code for generation for [ast::Expr::Binary] Predicate

use turso_sqlite3_parser::ast::{self, Expr};

use crate::{
    generation::{
        backtrack, one_of, pick,
        predicate::{CompoundPredicate, SimplePredicate},
        table::{GTValue, LTValue, LikeValue},
        ArbitraryFrom, ArbitraryFromMaybe as _,
    },
    model::{
        query::predicate::Predicate,
        table::{SimValue, Table},
    },
};

impl Predicate {
    /// Generate an [ast::Expr::Binary] [Predicate] from a column and [SimValue]
    pub fn from_column_binary<R: rand::Rng>(
        rng: &mut R,
        column_name: &str,
        value: &SimValue,
    ) -> Predicate {
        let expr = one_of(
            vec![
                Box::new(|_| {
                    Expr::Binary(
                        Box::new(Expr::Id(ast::Id(column_name.to_string()))),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(value.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, value).0;
                    Expr::Binary(
                        Box::new(Expr::Id(ast::Id(column_name.to_string()))),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, value).0;
                    Expr::Binary(
                        Box::new(Expr::Id(ast::Id(column_name.to_string()))),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(lt_value.into())),
                    )
                }),
            ],
            rng,
        );
        Predicate(expr)
    }

    /// Produces a true [ast::Expr::Binary] [Predicate] that is true for the provided row in the given table
    pub fn true_binary<R: rand::Rng>(rng: &mut R, t: &Table, row: &[SimValue]) -> Predicate {
        // Pick a column
        let column_index = rng.gen_range(0..t.columns.len());
        let column = &t.columns[column_index];
        let value = &row[column_index];
        let expr = backtrack(
            vec![
                (
                    1,
                    Box::new(|_| {
                        Some(Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name(t.name.clone()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Equals,
                            Box::new(Expr::Literal(value.into())),
                        ))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let v = SimValue::arbitrary_from(rng, &column.column_type);
                        if &v == value {
                            None
                        } else {
                            Some(Expr::Binary(
                                Box::new(ast::Expr::Qualified(
                                    ast::Name(t.name.clone()),
                                    ast::Name(column.name.clone()),
                                )),
                                ast::Operator::NotEquals,
                                Box::new(Expr::Literal(v.into())),
                            ))
                        }
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let lt_value = LTValue::arbitrary_from(rng, value).0;
                        Some(Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name(t.name.clone()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Greater,
                            Box::new(Expr::Literal(lt_value.into())),
                        ))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let gt_value = GTValue::arbitrary_from(rng, value).0;
                        Some(Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name(t.name.clone()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Less,
                            Box::new(Expr::Literal(gt_value.into())),
                        ))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        // TODO: generation for Like and Glob expressions should be extracted to different module
                        LikeValue::arbitrary_from_maybe(rng, value).map(|like| {
                            Expr::Like {
                                lhs: Box::new(ast::Expr::Qualified(
                                    ast::Name(t.name.clone()),
                                    ast::Name(column.name.clone()),
                                )),
                                not: false, // TODO: also generate this value eventually
                                op: ast::LikeOperator::Like,
                                rhs: Box::new(Expr::Literal(like.0.into())),
                                escape: None, // TODO: implement
                            }
                        })
                    }),
                ),
            ],
            rng,
        );
        // Backtrack will always return Some here
        Predicate(expr.unwrap())
    }

    /// Produces an [ast::Expr::Binary] [Predicate] that is false for the provided row in the given table
    pub fn false_binary<R: rand::Rng>(rng: &mut R, t: &Table, row: &[SimValue]) -> Predicate {
        // Pick a column
        let column_index = rng.gen_range(0..t.columns.len());
        let column = &t.columns[column_index];
        let value = &row[column_index];
        let expr = one_of(
            vec![
                Box::new(|_| {
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(t.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::NotEquals,
                        Box::new(Expr::Literal(value.into())),
                    )
                }),
                Box::new(|rng| {
                    let v = loop {
                        let v = SimValue::arbitrary_from(rng, &column.column_type);
                        if &v != value {
                            break v;
                        }
                    };
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(t.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(v.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, value).0;
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(t.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, value).0;
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(t.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(lt_value.into())),
                    )
                }),
            ],
            rng,
        );
        Predicate(expr)
    }
}

impl SimplePredicate {
    /// Generates a true [ast::Expr::Binary] [SimplePredicate] from a [Table] for a row in the table
    pub fn true_binary<R: rand::Rng>(rng: &mut R, table: &Table, row: &[SimValue]) -> Self {
        // Pick a random column
        let column_index = rng.gen_range(0..table.columns.len());
        let column = &table.columns[column_index];
        let column_value = &row[column_index];
        // Avoid creation of NULLs
        if row.is_empty() {
            return SimplePredicate(Predicate(Expr::Literal(SimValue::TRUE.into())));
        }
        let expr = one_of(
            vec![
                Box::new(|_rng| {
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(column_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, column_value).0;
                    Expr::Binary(
                        Box::new(Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(lt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, column_value).0;
                    Expr::Binary(
                        Box::new(Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
            ],
            rng,
        );
        SimplePredicate(Predicate(expr))
    }

    /// Generates a false [ast::Expr::Binary] [SimplePredicate] from a [Table] for a row in the table
    pub fn false_binary<R: rand::Rng>(rng: &mut R, table: &Table, row: &[SimValue]) -> Self {
        // Pick a random column
        let column_index = rng.gen_range(0..table.columns.len());
        let column = &table.columns[column_index];
        let column_value = &row[column_index];
        // Avoid creation of NULLs
        if row.is_empty() {
            return SimplePredicate(Predicate(Expr::Literal(SimValue::FALSE.into())));
        }
        let expr = one_of(
            vec![
                Box::new(|_rng| {
                    Expr::Binary(
                        Box::new(Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::NotEquals,
                        Box::new(Expr::Literal(column_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, column_value).0;
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, column_value).0;
                    Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name(table.name.clone()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(lt_value.into())),
                    )
                }),
            ],
            rng,
        );
        SimplePredicate(Predicate(expr))
    }
}

impl CompoundPredicate {
    /// Decide if you want to create an AND or an OR
    ///
    /// Creates a Compound Predicate that is TRUE or FALSE for at least a single row
    pub fn from_table_binary<R: rand::Rng>(
        rng: &mut R,
        table: &Table,
        predicate_value: bool,
    ) -> Self {
        // Cannot pick a row if the table is empty
        if table.rows.is_empty() {
            return Self(if predicate_value {
                Predicate::true_()
            } else {
                Predicate::false_()
            });
        }
        let row = pick(&table.rows, rng);
        let predicate = if rng.gen_bool(0.7) {
            // An AND for true requires each of its children to be true
            // An AND for false requires at least one of its children to be false
            if predicate_value {
                (0..rng.gen_range(0..=3))
                    .map(|_| SimplePredicate::arbitrary_from(rng, (table, row, true)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::And,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::true_())
            } else {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(0..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();

                let len = booleans.len();

                // Make sure at least one of them is false
                if !booleans.is_empty() && booleans.iter().all(|b| *b) {
                    booleans[rng.gen_range(0..len)] = false;
                }

                booleans
                    .iter()
                    .map(|b| SimplePredicate::arbitrary_from(rng, (table, row, *b)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::And,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::false_())
            }
        } else {
            // An OR for true requires at least one of its children to be true
            // An OR for false requires each of its children to be false
            if predicate_value {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(0..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();
                let len = booleans.len();
                // Make sure at least one of them is true
                if !booleans.is_empty() && booleans.iter().all(|b| !*b) {
                    booleans[rng.gen_range(0..len)] = true;
                }

                booleans
                    .iter()
                    .map(|b| SimplePredicate::arbitrary_from(rng, (table, row, *b)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::Or,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::true_())
            } else {
                (0..rng.gen_range(0..=3))
                    .map(|_| SimplePredicate::arbitrary_from(rng, (table, row, false)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::Or,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::false_())
            }
        };
        Self(predicate)
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng as _, SeedableRng as _};
    use rand_chacha::ChaCha8Rng;

    use crate::{
        generation::{pick, predicate::SimplePredicate, Arbitrary, ArbitraryFrom as _},
        model::{
            query::predicate::{expr_to_value, Predicate},
            table::{SimValue, Table},
        },
    };

    fn get_seed() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn fuzz_true_binary_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        for _ in 0..10000 {
            let table = Table::arbitrary(&mut rng);
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
            let row = pick(&values, &mut rng);
            let predicate = Predicate::true_binary(&mut rng, &table, row);
            let value = expr_to_value(&predicate.0, row, &table);
            assert!(
                value.as_ref().is_some_and(|value| value.as_bool()),
                "Predicate: {predicate:#?}\nValue: {value:#?}\nSeed: {seed}"
            )
        }
    }

    #[test]
    fn fuzz_false_binary_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        for _ in 0..10000 {
            let table = Table::arbitrary(&mut rng);
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
            let row = pick(&values, &mut rng);
            let predicate = Predicate::false_binary(&mut rng, &table, row);
            let value = expr_to_value(&predicate.0, row, &table);
            assert!(
                !value.as_ref().is_some_and(|value| value.as_bool()),
                "Predicate: {predicate:#?}\nValue: {value:#?}\nSeed: {seed}"
            )
        }
    }

    #[test]
    fn fuzz_true_binary_simple_predicate() {
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
            let predicate = SimplePredicate::true_binary(&mut rng, &table, row);
            let result = values
                .iter()
                .map(|row| predicate.0.test(row, &table))
                .reduce(|accum, curr| accum || curr)
                .unwrap_or(false);
            assert!(result, "Predicate: {predicate:#?}\nSeed: {seed}")
        }
    }

    #[test]
    fn fuzz_false_binary_simple_predicate() {
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
            let predicate = SimplePredicate::false_binary(&mut rng, &table, row);
            let result = values
                .iter()
                .map(|row| predicate.0.test(row, &table))
                .any(|res| !res);
            assert!(result, "Predicate: {predicate:#?}\nSeed: {seed}")
        }
    }
}
