use limbo_sqlite3_parser::ast::{self, Expr};
use rand::{seq::SliceRandom as _, Rng};

use crate::model::{
    query::predicate::Predicate,
    table::{Table, Value},
};

use super::{
    backtrack, one_of,
    table::{GTValue, LTValue, LikeValue},
    ArbitraryFrom, ArbitraryFromMaybe as _,
};

struct CompoundPredicate(Predicate);
struct SimplePredicate(Predicate);

impl ArbitraryFrom<(&Table, bool)> for SimplePredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        // Pick a random column
        let column_index = rng.gen_range(0..table.columns.len());
        let column = &table.columns[column_index];
        let column_values = table
            .rows
            .iter()
            .map(|r| &r[column_index])
            .collect::<Vec<_>>();
        // Pick an operator
        let operator = match predicate_value {
            true => one_of(
                vec![
                    Box::new(|rng| {
                        Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name("".to_string()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Equals,
                            Box::new(Expr::arbitrary_from(rng, &column_values)),
                        )
                    }),
                    Box::new(|rng| {
                        let gt_value = GTValue::arbitrary_from(rng, &column_values).0;
                        Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name("".to_string()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Greater,
                            Box::new(Expr::Literal(gt_value.into())),
                        )
                    }),
                    Box::new(|rng| {
                        let lt_value = LTValue::arbitrary_from(rng, &column_values).0;
                        Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name("".to_string()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Less,
                            Box::new(Expr::Literal(lt_value.into())),
                        )
                    }),
                ],
                rng,
            ),
            false => one_of(
                vec![
                    Box::new(|rng| {
                        Expr::Binary(
                            Box::new(Expr::Qualified(
                                ast::Name("".to_string()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::NotEquals,
                            Box::new(Expr::arbitrary_from(rng, &column_values)),
                        )
                    }),
                    Box::new(|rng| {
                        let lt_value = LTValue::arbitrary_from(rng, &column_values).0;
                        Expr::Binary(
                            Box::new(Expr::Qualified(
                                ast::Name("".to_string()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Greater,
                            Box::new(Expr::Literal(lt_value.into())),
                        )
                    }),
                    Box::new(|rng| {
                        let gt_value = GTValue::arbitrary_from(rng, &column_values).0;
                        Expr::Binary(
                            Box::new(Expr::Qualified(
                                ast::Name("".to_string()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::Less,
                            Box::new(Expr::Literal(gt_value.into())),
                        )
                    }),
                ],
                rng,
            ),
        };

        Self(Predicate(operator))
    }
}

impl ArbitraryFrom<(&Table, bool)> for CompoundPredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        // Decide if you want to create an AND or an OR
        Self(if rng.gen_bool(0.7) {
            // An AND for true requires each of its children to be true
            // An AND for false requires at least one of its children to be false
            if predicate_value {
                (0..rng.gen_range(0..=3))
                    .map(|_| SimplePredicate::arbitrary_from(rng, (table, true)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::And,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::true_()) // Empty And is True
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
                    .map(|b| SimplePredicate::arbitrary_from(rng, (table, *b)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::And,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::true_()) // Empty And is True
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
                    .map(|b| SimplePredicate::arbitrary_from(rng, (table, *b)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::Or,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::false_()) // Empty Or is False
            } else {
                (0..rng.gen_range(0..=3))
                    .map(|_| SimplePredicate::arbitrary_from(rng, (table, false)).0)
                    .reduce(|accum, curr| {
                        Predicate(Expr::Binary(
                            Box::new(accum.0),
                            ast::Operator::Or,
                            Box::new(curr.0),
                        ))
                    })
                    .unwrap_or(Predicate::false_()) // Empty Or is False
            }
        })
    }
}

impl ArbitraryFrom<&Table> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        let predicate_value = rng.gen_bool(0.5);
        CompoundPredicate::arbitrary_from(rng, (table, predicate_value)).0
    }
}

impl ArbitraryFrom<(&str, &Value)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (column_name, value): (&str, &Value)) -> Self {
        one_of(
            vec![
                Box::new(|_| {
                    Predicate(Expr::Binary(
                        Box::new(Expr::Qualified(
                            ast::Name("".to_string()),
                            ast::Name(column_name.to_string()),
                        )),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(value.into())),
                    ))
                }),
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, value).0;
                    Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name("".to_string()),
                            ast::Name(column_name.to_string()),
                        )),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(gt_value.into())),
                    ))
                }),
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, value).0;
                    Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name("".to_string()),
                            ast::Name(column_name.to_string()),
                        )),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(lt_value.into())),
                    ))
                }),
            ],
            rng,
        )
    }
}

/// Produces a predicate that is true for the provided row in the given table
fn produce_true_predicate<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Predicate {
    // Pick a column
    let column_index = rng.gen_range(0..t.columns.len());
    let column = &t.columns[column_index];
    let value = &row[column_index];
    backtrack(
        vec![
            (
                1,
                Box::new(|_| {
                    Some(Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name("".to_string()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Equals,
                        Box::new(Expr::Literal(value.into())),
                    )))
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    let v = Value::arbitrary_from(rng, &column.column_type);
                    if &v == value {
                        None
                    } else {
                        Some(Predicate(Expr::Binary(
                            Box::new(ast::Expr::Qualified(
                                ast::Name("".to_string()),
                                ast::Name(column.name.clone()),
                            )),
                            ast::Operator::NotEquals,
                            Box::new(Expr::Literal(v.into())),
                        )))
                    }
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    let lt_value = LTValue::arbitrary_from(rng, value).0;
                    Some(Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name("".to_string()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(lt_value.into())),
                    )))
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    let gt_value = GTValue::arbitrary_from(rng, value).0;
                    Some(Predicate(Expr::Binary(
                        Box::new(ast::Expr::Qualified(
                            ast::Name("".to_string()),
                            ast::Name(column.name.clone()),
                        )),
                        ast::Operator::Less,
                        Box::new(Expr::Literal(gt_value.into())),
                    )))
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    LikeValue::arbitrary_from_maybe(rng, value).map(|like| {
                        Predicate(Expr::Like {
                            lhs: Box::new(ast::Expr::Qualified(
                                ast::Name("".to_string()),
                                ast::Name(column.name.clone()),
                            )),
                            not: false, // TODO: also generate this value eventually
                            op: ast::LikeOperator::Like,
                            rhs: Box::new(Expr::Literal(like.0.into())),
                            escape: None, // TODO: implement
                        })
                    })
                }),
            ),
        ],
        rng,
    )
}

/// Produces a predicate that is false for the provided row in the given table
fn produce_false_predicate<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Predicate {
    // Pick a column
    let column_index = rng.gen_range(0..t.columns.len());
    let column = &t.columns[column_index];
    let value = &row[column_index];
    one_of(
        vec![
            Box::new(|_| {
                Predicate(Expr::Binary(
                    Box::new(ast::Expr::Qualified(
                        ast::Name("".to_string()),
                        ast::Name(column.name.clone()),
                    )),
                    ast::Operator::NotEquals,
                    Box::new(Expr::Literal(value.into())),
                ))
            }),
            Box::new(|rng| {
                let v = loop {
                    let v = Value::arbitrary_from(rng, &column.column_type);
                    if &v != value {
                        break v;
                    }
                };
                Predicate(Expr::Binary(
                    Box::new(ast::Expr::Qualified(
                        ast::Name("".to_string()),
                        ast::Name(column.name.clone()),
                    )),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(v.into())),
                ))
            }),
            Box::new(|rng| {
                let gt_value = GTValue::arbitrary_from(rng, value).0;
                Predicate(Expr::Binary(
                    Box::new(ast::Expr::Qualified(
                        ast::Name("".to_string()),
                        ast::Name(column.name.clone()),
                    )),
                    ast::Operator::Greater,
                    Box::new(Expr::Literal(gt_value.into())),
                ))
            }),
            Box::new(|rng| {
                let lt_value = LTValue::arbitrary_from(rng, value).0;
                Predicate(Expr::Binary(
                    Box::new(ast::Expr::Qualified(
                        ast::Name("".to_string()),
                        ast::Name(column.name.clone()),
                    )),
                    ast::Operator::Less,
                    Box::new(Expr::Literal(lt_value.into())),
                ))
            }),
        ],
        rng,
    )
}

impl ArbitraryFrom<(&Table, &Vec<Value>)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Self {
        // We want to produce a predicate that is true for the row
        // We can do this by creating several predicates that
        // are true, some that are false, combiend them in ways that correspond to the creation of a true predicate

        // Produce some true and false predicates
        let mut true_predicates = (1..=rng.gen_range(1..=4))
            .map(|_| produce_true_predicate(rng, (t, row)))
            .collect::<Vec<_>>();

        let false_predicates = (0..=rng.gen_range(0..=3))
            .map(|_| produce_false_predicate(rng, (t, row)))
            .collect::<Vec<_>>();

        // Start building a top level predicate from a true predicate
        let mut result = true_predicates.pop().unwrap();

        let mut predicates = true_predicates
            .iter()
            .map(|p| (true, p.clone()))
            .chain(false_predicates.iter().map(|p| (false, p.clone())))
            .collect::<Vec<_>>();

        predicates.shuffle(rng);

        while !predicates.is_empty() {
            // Create a new predicate from at least 1 and at most 3 predicates
            let context =
                predicates[0..rng.gen_range(0..=usize::min(3, predicates.len()))].to_vec();
            // Shift `predicates` to remove the predicates in the context
            predicates = predicates[context.len()..].to_vec();

            // `result` is true, so we have the following three options to make a true predicate:
            // T or F
            // T or T
            // T and T

            result = one_of(
                vec![
                    // T or (X1 or X2 or ... or Xn)
                    Box::new(|_| {
                        Predicate(Expr::Binary(
                            Box::new(result.0.clone()),
                            ast::Operator::Or,
                            Box::new(
                                context
                                    .iter()
                                    .map(|(_, p)| p.clone())
                                    .reduce(|accum, curr| {
                                        Predicate(Expr::Binary(
                                            Box::new(accum.0),
                                            ast::Operator::Or,
                                            Box::new(curr.0),
                                        ))
                                    })
                                    .unwrap_or(Predicate::false_())
                                    .0,
                            ),
                        ))
                    }),
                    // T or (T1 and T2 and ... and Tn)
                    Box::new(|_| {
                        Predicate(Expr::Binary(
                            Box::new(result.0.clone()),
                            ast::Operator::Or,
                            Box::new(
                                context
                                    .iter()
                                    .map(|(_, p)| p.clone())
                                    .reduce(|accum, curr| {
                                        Predicate(Expr::Binary(
                                            Box::new(accum.0),
                                            ast::Operator::And,
                                            Box::new(curr.0),
                                        ))
                                    })
                                    .unwrap_or(Predicate::true_())
                                    .0,
                            ),
                        ))
                    }),
                    // T and T
                    Box::new(|_| {
                        // Check if all the predicates in the context are true
                        if context.iter().all(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            Predicate(Expr::Binary(
                                Box::new(result.0.clone()),
                                ast::Operator::And,
                                Box::new(
                                    context
                                        .iter()
                                        .map(|(_, p)| p.clone())
                                        .reduce(|accum, curr| {
                                            Predicate(Expr::Binary(
                                                Box::new(accum.0),
                                                ast::Operator::And,
                                                Box::new(curr.0),
                                            ))
                                        })
                                        .unwrap_or(Predicate::true_())
                                        .0,
                                ),
                            ))
                        }
                        // Check if there is at least one true predicate
                        else if context.iter().any(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            Predicate(Expr::Binary(
                                Box::new(result.0.clone()),
                                ast::Operator::And,
                                Box::new(
                                    context
                                        .iter()
                                        .map(|(_, p)| p.clone())
                                        .reduce(|accum, curr| {
                                            Predicate(Expr::Binary(
                                                Box::new(accum.0),
                                                ast::Operator::Or,
                                                Box::new(curr.0),
                                            ))
                                        })
                                        .unwrap_or(Predicate::false_())
                                        .0,
                                ),
                            ))
                            // Predicate::And(vec![
                            //     result.clone(),
                            //     Predicate::Or(context.iter().map(|(_, p)| p.clone()).collect()),
                            // ])
                        } else {
                            // T and (X1 or X2 or ... or Xn or TRUE)
                            Predicate(Expr::Binary(
                                Box::new(result.0.clone()),
                                ast::Operator::And,
                                Box::new(
                                    context
                                        .iter()
                                        .map(|(_, p)| p.clone())
                                        .chain(std::iter::once(Predicate::true_()))
                                        .reduce(|accum, curr| {
                                            Predicate(Expr::Binary(
                                                Box::new(accum.0),
                                                ast::Operator::Or,
                                                Box::new(curr.0),
                                            ))
                                        })
                                        .unwrap() // Chain guarantees at least one value
                                        .0,
                                ),
                            ))
                        }
                    }),
                ],
                rng,
            );
        }

        result
    }
}
