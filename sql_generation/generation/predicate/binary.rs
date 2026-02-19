//! Contains code for generation for [ast::Expr::Binary] Predicate

use turso_parser::ast::{self, Expr, Name};

use crate::{
    generation::{
        backtrack, one_of, pick,
        predicate::{CompoundPredicate, SimplePredicate},
        value::{GTValue, LTValue, LikeValue},
        ArbitraryFrom, ArbitraryFromMaybe as _, GenerationContext,
    },
    model::{
        query::predicate::Predicate,
        table::{SimValue, Table, TableContext},
    },
};

/// Create a qualified column reference expression, handling attached DB prefixes.
/// For table names like "aux0.t1", produces `DoublyQualified(aux0, t1, col)`.
/// For plain table names like "t1", produces `Qualified(t1, col)`.
fn qualified_column_expr(table_name: &str, column_name: &str) -> Expr {
    if let Some((db, tbl)) = table_name.split_once('.') {
        Expr::DoublyQualified(
            Name::from_string(db),
            Name::from_string(tbl),
            Name::from_string(column_name),
        )
    } else {
        Expr::Qualified(
            Name::from_string(table_name),
            Name::from_string(column_name),
        )
    }
}

impl Predicate {
    /// Produces a true [ast::Expr::Binary] [Predicate] that is true for the provided row in the given table
    pub fn true_binary<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        t: &Table,
        row: &[SimValue],
    ) -> Predicate {
        // Pick a column
        let column_index = rng.random_range(0..t.columns.len());
        let mut column = t.columns[column_index].clone();
        let value = &row[column_index];

        let mut table_name = t.name.clone();
        if t.name.is_empty() {
            // If the table name is empty, we cannot create a qualified expression
            // so we use the column name directly
            let mut splitted = column.name.split('.');
            table_name = splitted
                .next()
                .expect("Column name should have a table prefix for a joined table")
                .to_string();
            column.name = splitted
                .next()
                .expect("Column name should have a column suffix for a joined table")
                .to_string();
        }

        let expr = backtrack(
            vec![
                (
                    1,
                    Box::new(|_| {
                        Some(Expr::Binary(
                            Box::new(qualified_column_expr(&table_name, &column.name)),
                            ast::Operator::Equals,
                            Box::new(Expr::Literal(value.into())),
                        ))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let v = SimValue::arbitrary_from(rng, context, &column.column_type);
                        if &v == value {
                            None
                        } else {
                            Some(Expr::Binary(
                                Box::new(qualified_column_expr(&table_name, &column.name)),
                                ast::Operator::NotEquals,
                                Box::new(Expr::Literal(v.into())),
                            ))
                        }
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let lt_value =
                            LTValue::arbitrary_from(rng, context, (value, column.column_type)).0;
                        Some(Expr::Binary(
                            Box::new(qualified_column_expr(&table_name, &column.name)),
                            ast::Operator::Greater,
                            Box::new(Expr::Literal(lt_value.into())),
                        ))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        let gt_value =
                            GTValue::arbitrary_from(rng, context, (value, column.column_type)).0;
                        Some(Expr::Binary(
                            Box::new(qualified_column_expr(&table_name, &column.name)),
                            ast::Operator::Less,
                            Box::new(Expr::Literal(gt_value.into())),
                        ))
                    }),
                ),
                (
                    1,
                    Box::new(|rng| {
                        // TODO: generation for Like and Glob expressions should be extracted to different module
                        LikeValue::arbitrary_from_maybe(rng, context, value).map(|like| {
                            Expr::Like {
                                lhs: Box::new(qualified_column_expr(&table_name, &column.name)),
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
    pub fn false_binary<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        t: &Table,
        row: &[SimValue],
    ) -> Predicate {
        // Pick a column
        let column_index = rng.random_range(0..t.columns.len());
        let mut column = t.columns[column_index].clone();
        let mut table_name = t.name.clone();
        let value = &row[column_index];

        if t.name.is_empty() {
            // If the table name is empty, we cannot create a qualified expression
            // so we use the column name directly
            let mut splitted = column.name.split('.');
            table_name = splitted
                .next()
                .expect("Column name should have a table prefix for a joined table")
                .to_string();
            column.name = splitted
                .next()
                .expect("Column name should have a column suffix for a joined table")
                .to_string();
        }

        let expr = one_of(
            vec![
                Box::new(|_| {
                    Expr::Binary(
                        Box::new(qualified_column_expr(&table_name, &column.name)),
                        ast::Operator::NotEquals,
                        Box::new(Expr::Literal(value.into())),
                    )
                }),
                Box::new(|rng| {
                    let v = loop {
                        let v = SimValue::arbitrary_from(rng, context, &column.column_type);
                        if &v != value {
                            break v;
                        }
                    };
                    Expr::Binary(
                        Box::new(qualified_column_expr(&table_name, &column.name)),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(v.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value =
                        GTValue::arbitrary_from(rng, context, (value, column.column_type)).0;
                    Expr::Binary(
                        Box::new(qualified_column_expr(&table_name, &column.name)),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value =
                        LTValue::arbitrary_from(rng, context, (value, column.column_type)).0;
                    Expr::Binary(
                        Box::new(qualified_column_expr(&table_name, &column.name)),
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
    /// Generates a true [ast::Expr::Binary] [SimplePredicate] from a [TableContext] for a row in the table
    pub fn true_binary<R: rand::Rng + ?Sized, C: GenerationContext, T: TableContext>(
        rng: &mut R,
        context: &C,
        table: &T,
        row: &[SimValue],
    ) -> Self {
        // Avoid creation of NULLs
        if row.is_empty() {
            return SimplePredicate(Predicate(Expr::Literal(SimValue::TRUE.into())));
        }
        // Pick a random column
        let columns = table.columns().collect::<Vec<_>>();
        let column_index = rng.random_range(0..row.len());
        let column = columns[column_index];
        let column_value = &row[column_index];
        let table_name = column.table_name;

        let expr = one_of(
            vec![
                Box::new(|_rng| {
                    Expr::Binary(
                        Box::new(qualified_column_expr(table_name, &column.column.name)),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(column_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(
                        rng,
                        context,
                        (column_value, column.column.column_type),
                    )
                    .0;
                    Expr::Binary(
                        Box::new(qualified_column_expr(table_name, &column.column.name)),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(lt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(
                        rng,
                        context,
                        (column_value, column.column.column_type),
                    )
                    .0;
                    Expr::Binary(
                        Box::new(qualified_column_expr(table_name, &column.column.name)),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
            ],
            rng,
        );
        SimplePredicate(Predicate(expr))
    }

    /// Generates a false [ast::Expr::Binary] [SimplePredicate] from a [TableContext] for a row in the table
    pub fn false_binary<R: rand::Rng + ?Sized, C: GenerationContext, T: TableContext>(
        rng: &mut R,
        context: &C,
        table: &T,
        row: &[SimValue],
    ) -> Self {
        // Avoid creation of NULLs
        if row.is_empty() {
            return SimplePredicate(Predicate(Expr::Literal(SimValue::FALSE.into())));
        }
        let columns = table.columns().collect::<Vec<_>>();
        // Pick a random column
        let column_index = rng.random_range(0..row.len());
        let column = columns[column_index];
        let column_value = &row[column_index];
        let table_name = column.table_name;

        let expr = one_of(
            vec![
                Box::new(|_rng| {
                    Expr::Binary(
                        Box::new(qualified_column_expr(table_name, &column.column.name)),
                        ast::Operator::NotEquals,
                        Box::new(Expr::Literal(column_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(
                        rng,
                        context,
                        (column_value, column.column.column_type),
                    )
                    .0;
                    Expr::Binary(
                        Box::new(qualified_column_expr(table_name, &column.column.name)),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    )
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(
                        rng,
                        context,
                        (column_value, column.column.column_type),
                    )
                    .0;
                    Expr::Binary(
                        Box::new(qualified_column_expr(table_name, &column.column.name)),
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
    pub fn from_table_binary<R: rand::Rng + ?Sized, C: GenerationContext, T: TableContext>(
        rng: &mut R,
        context: &C,
        table: &T,
        predicate_value: bool,
    ) -> Self {
        // Cannot pick a row if the table is empty
        let rows = table.rows();
        if rows.is_empty() {
            return Self(if predicate_value {
                Predicate::true_()
            } else {
                Predicate::false_()
            });
        }
        let row = pick(rows, rng);

        let predicate = if rng.random_bool(0.7) {
            // An AND for true requires each of its children to be true
            // An AND for false requires at least one of its children to be false
            if predicate_value {
                (0..rng.random_range(1..=3))
                    .map(|_| SimplePredicate::arbitrary_from(rng, context, (table, row, true)).0)
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
                let mut booleans = (0..rng.random_range(1..=3))
                    .map(|_| rng.random_bool(0.5))
                    .collect::<Vec<_>>();

                let len = booleans.len();

                // Make sure at least one of them is false
                if booleans.iter().all(|b| *b) {
                    booleans[rng.random_range(0..len)] = false;
                }

                booleans
                    .iter()
                    .map(|b| SimplePredicate::arbitrary_from(rng, context, (table, row, *b)).0)
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
                let mut booleans = (0..rng.random_range(1..=3))
                    .map(|_| rng.random_bool(0.5))
                    .collect::<Vec<_>>();
                let len = booleans.len();
                // Make sure at least one of them is true
                if booleans.iter().all(|b| !*b) {
                    booleans[rng.random_range(0..len)] = true;
                }

                booleans
                    .iter()
                    .map(|b| SimplePredicate::arbitrary_from(rng, context, (table, row, *b)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::Or,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::true_())
            } else {
                (0..rng.random_range(1..=3))
                    .map(|_| SimplePredicate::arbitrary_from(rng, context, (table, row, false)).0)
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
        generation::{
            pick, predicate::SimplePredicate, tests::TestContext, Arbitrary, ArbitraryFrom as _,
        },
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
        let context = &TestContext::default();

        for _ in 0..10000 {
            let table = Table::arbitrary(&mut rng, context);
            let num_rows = rng.random_range(1..10);
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(&mut rng, context, &c.column_type))
                        .collect()
                })
                .collect();
            let row = pick(&values, &mut rng);
            let predicate = Predicate::true_binary(&mut rng, context, &table, row);
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
        let context = &TestContext::default();

        for _ in 0..10000 {
            let table = Table::arbitrary(&mut rng, context);
            let num_rows = rng.random_range(1..10);
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(&mut rng, context, &c.column_type))
                        .collect()
                })
                .collect();
            let row = pick(&values, &mut rng);
            let predicate = Predicate::false_binary(&mut rng, context, &table, row);
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
        let context = &TestContext::default();

        for _ in 0..10000 {
            let mut table = Table::arbitrary(&mut rng, context);
            let num_rows = rng.random_range(1..10);
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(&mut rng, context, &c.column_type))
                        .collect()
                })
                .collect();
            table.rows.extend(values.clone());
            let row = pick(&table.rows, &mut rng);
            let predicate = SimplePredicate::true_binary(&mut rng, context, &table, row);
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
        let context = &TestContext::default();

        for _ in 0..10000 {
            let mut table = Table::arbitrary(&mut rng, context);
            let num_rows = rng.random_range(1..10);
            let values: Vec<Vec<SimValue>> = (0..num_rows)
                .map(|_| {
                    table
                        .columns
                        .iter()
                        .map(|c| SimValue::arbitrary_from(&mut rng, context, &c.column_type))
                        .collect()
                })
                .collect();
            table.rows.extend(values.clone());
            let row = pick(&table.rows, &mut rng);
            let predicate = SimplePredicate::false_binary(&mut rng, context, &table, row);
            let result = values
                .iter()
                .map(|row| predicate.0.test(row, &table))
                .any(|res| !res);
            assert!(result, "Predicate: {predicate:#?}\nSeed: {seed}")
        }
    }
}
