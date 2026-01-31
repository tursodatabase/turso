use rand::{seq::SliceRandom as _, Rng};

use crate::{
    generation::GenerationContext,
    model::{
        query::predicate::Predicate,
        table::{SimValue, Table, TableContext},
    },
};

use super::{one_of, ArbitraryFrom};

pub mod binary;
pub mod unary;

#[derive(Debug)]
struct CompoundPredicate(Predicate);

#[derive(Debug)]
struct SimplePredicate(Predicate);

impl<A: AsRef<[SimValue]>, T: TableContext> ArbitraryFrom<(&T, A, bool)> for SimplePredicate {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        (table, row, predicate_value): (&T, A, bool),
    ) -> Self {
        let row = row.as_ref();
        // Pick an operator
        let choice = rng.random_range(0..2);
        // Pick an operator
        match predicate_value {
            true => match choice {
                0 => SimplePredicate::true_binary(rng, context, table, row),
                1 => SimplePredicate::true_unary(rng, context, table, row),
                _ => unreachable!(),
            },
            false => match choice {
                0 => SimplePredicate::false_binary(rng, context, table, row),
                1 => SimplePredicate::false_unary(rng, context, table, row),
                _ => unreachable!(),
            },
        }
    }
}

impl<T: TableContext> ArbitraryFrom<(&T, bool)> for CompoundPredicate {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        (table, predicate_value): (&T, bool),
    ) -> Self {
        CompoundPredicate::from_table_binary(rng, context, table, predicate_value)
    }
}

impl<T: TableContext> ArbitraryFrom<&T> for Predicate {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        table: &T,
    ) -> Self {
        let predicate_value = rng.random_bool(0.5);
        Predicate::arbitrary_from(rng, context, (table, predicate_value)).parens()
    }
}

impl<T: TableContext> ArbitraryFrom<(&T, bool)> for Predicate {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        (table, predicate_value): (&T, bool),
    ) -> Self {
        CompoundPredicate::arbitrary_from(rng, context, (table, predicate_value)).0
    }
}

impl ArbitraryFrom<(&Table, &Vec<SimValue>)> for Predicate {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        (t, row): (&Table, &Vec<SimValue>),
    ) -> Self {
        // We want to produce a predicate that is true for the row
        // We can do this by creating several predicates that
        // are true, some that are false, combiend them in ways that correspond to the creation of a true predicate

        // Produce some true and false predicates
        let mut true_predicates = (1..=rng.random_range(1..=4))
            .map(|_| Predicate::true_binary(rng, context, t, row))
            .collect::<Vec<_>>();

        let false_predicates = (0..=rng.random_range(0..=3))
            .map(|_| Predicate::false_binary(rng, context, t, row))
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
                predicates[0..rng.random_range(0..=usize::min(3, predicates.len()))].to_vec();
            // Shift `predicates` to remove the predicates in the context
            predicates = predicates[context.len()..].to_vec();

            // `result` is true, so we have the following three options to make a true predicate:
            // T or F
            // T or T
            // T and T

            // Prepare a vector of the predicates in context
            let ctx_preds: Vec<Predicate> = context.iter().map(|(_, p)| p.clone()).collect();

            result = one_of(
                vec![
                    // T or (X1 or X2 or ... or Xn)
                    Box::new(|_| {
                        let or_expr = Predicate::or(ctx_preds.clone());
                        Predicate::or(vec![result.clone(), or_expr])
                    }),
                    // T or (T1 and T2 and ... and Tn)
                    Box::new(|_| {
                        let and_expr = Predicate::and(ctx_preds.clone());
                        Predicate::or(vec![result.clone(), and_expr])
                    }),
                    // T and T
                    Box::new(|_| {
                        // Check if all the predicates in the context are true
                        if context.iter().all(|(b, _)| *b) {
                            // T and (X1 and X2 and ... and Xn)
                            let and_expr = Predicate::and(ctx_preds.clone());
                            Predicate::and(vec![result.clone(), and_expr])
                        }
                        // Check if there is at least one true predicate
                        else if context.iter().any(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            let or_expr = Predicate::or(ctx_preds.clone());
                            Predicate::and(vec![result.clone(), or_expr])
                        } else {
                            // T and (X1 or X2 or ... or Xn or TRUE)
                            let mut preds = ctx_preds.clone();
                            preds.push(Predicate::true_());
                            let or_expr = Predicate::or(preds);
                            Predicate::and(vec![result.clone(), or_expr])
                        }
                    }),
                ],
                rng,
            );
        }
        result
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
    fn fuzz_arbitrary_table_true_simple_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let context = &TestContext::no_gencol();

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
            let predicate =
                SimplePredicate::arbitrary_from(&mut rng, context, (&table, row, true)).0;
            let value = expr_to_value(&predicate.0, row, &table);
            assert!(
                value.as_ref().is_some_and(|value| value.as_bool()),
                "Predicate: {predicate:#?}\nValue: {value:#?}\nSeed: {seed}"
            )
        }
    }

    #[test]
    fn fuzz_arbitrary_table_false_simple_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let context = &TestContext::no_gencol();

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
            let predicate =
                SimplePredicate::arbitrary_from(&mut rng, context, (&table, row, false)).0;
            let value = expr_to_value(&predicate.0, row, &table);
            assert!(
                !value.as_ref().is_some_and(|value| value.as_bool()),
                "Predicate: {predicate:#?}\nValue: {value:#?}\nSeed: {seed}"
            )
        }
    }

    #[test]
    fn fuzz_arbitrary_row_table_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let context = &TestContext::no_gencol();

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
            let predicate = Predicate::arbitrary_from(&mut rng, context, (&table, row));
            let value = expr_to_value(&predicate.0, row, &table);
            assert!(
                value.as_ref().is_some_and(|value| value.as_bool()),
                "Predicate: {predicate:#?}\nValue: {value:#?}\nSeed: {seed}"
            )
        }
    }

    #[test]
    fn fuzz_arbitrary_true_table_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let context = &TestContext::no_gencol();

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
            let predicate = Predicate::arbitrary_from(&mut rng, context, (&table, true));
            let result = values
                .iter()
                .map(|row| predicate.test(row, &table))
                .reduce(|accum, curr| accum || curr)
                .unwrap_or(false);
            assert!(result, "Predicate: {predicate:#?}\nSeed: {seed}")
        }
    }

    #[test]
    fn fuzz_arbitrary_false_table_predicate() {
        let seed = get_seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let context = &TestContext::no_gencol();

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
            let predicate = Predicate::arbitrary_from(&mut rng, context, (&table, false));
            let result = values
                .iter()
                .map(|row| predicate.test(row, &table))
                .any(|res| !res);
            assert!(result, "Predicate: {predicate:#?}\nSeed: {seed}")
        }
    }
}
