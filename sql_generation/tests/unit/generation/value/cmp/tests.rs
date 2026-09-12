use anarchist_readable_name_generator_lib::readable_name;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::generation::tests::TestContext;

use super::*;

/// `random_range` panics on empty ranges. Check that boundaries and arbitrary values don't panic.
#[test]
fn lt_gt_value_boundaries_do_not_panic() {
    let mut rng = ChaCha8Rng::seed_from_u64(0);
    let ctx = TestContext::default();

    let cases: [(SimValue, ColumnType); 8] = [
        (SimValue(Value::from_i64(i64::MIN)), ColumnType::Integer),
        (SimValue(Value::from_i64(i64::MIN + 1)), ColumnType::Integer),
        (SimValue(Value::from_i64(i64::MAX)), ColumnType::Integer),
        (SimValue(Value::from_i64(i64::MAX - 1)), ColumnType::Integer),
        (SimValue(Value::from_f64(1e10)), ColumnType::Float),
        (SimValue(Value::from_f64(2e10)), ColumnType::Float),
        (
            SimValue(Value::Blob(turso_core::alloc::vec![])),
            ColumnType::Blob,
        ),
        (
            SimValue(Value::Blob(turso_core::alloc::vec![0, 255])),
            ColumnType::Blob,
        ),
    ];
    for (val, col_type) in &cases {
        for _ in 0..200 {
            let _ = LTValue::arbitrary_from(&mut rng, &ctx, (val, *col_type));
            let _ = GTValue::arbitrary_from(&mut rng, &ctx, (val, *col_type));
        }
    }
}

#[test]
fn test_mutate_string_fuzz() {
    let mut rng = rand::rng();
    for _ in 0..1000 {
        let mut t = readable_name();
        while !t.is_ascii() {
            t = readable_name();
        }
        let t2 = mutate_string(&t, &mut rng, MutationType::Decrement);
        assert!(t2.is_ascii(), "{}", t);
        assert!(t2 < t);
    }
    for _ in 0..1000 {
        let mut t = readable_name();
        while !t.is_ascii() {
            t = readable_name();
        }
        let t2 = mutate_string(&t, &mut rng, MutationType::Increment);
        assert!(t2.is_ascii(), "{}", t);
        assert!(t2 > t);
    }
}
