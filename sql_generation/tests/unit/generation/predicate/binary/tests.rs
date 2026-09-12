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
