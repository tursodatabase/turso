use super::*;
use crate::types::Text;

#[test]
fn test_is_null_predicate() {
    let predicate = FilterPredicate::IsNull { column_idx: 1 };
    let filter = FilterOperator::new(predicate);

    // Test with NULL value
    let values_with_null = vec![
        Value::from_i64(1),
        Value::Null,
        Value::Text(Text::from("test")),
    ];
    assert!(filter.evaluate_predicate(&values_with_null));

    // Test with non-NULL value
    let values_without_null = vec![
        Value::from_i64(1),
        Value::from_i64(42),
        Value::Text(Text::from("test")),
    ];
    assert!(!filter.evaluate_predicate(&values_without_null));

    // Test with different non-NULL types
    let values_with_text = vec![
        Value::from_i64(1),
        Value::Text(Text::from("not null")),
        Value::Text(Text::from("test")),
    ];
    assert!(!filter.evaluate_predicate(&values_with_text));

    let values_with_blob = vec![
        Value::from_i64(1),
        Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG),
        Value::Text(Text::from("test")),
    ];
    assert!(!filter.evaluate_predicate(&values_with_blob));
}

#[test]
fn test_is_not_null_predicate() {
    let predicate = FilterPredicate::IsNotNull { column_idx: 1 };
    let filter = FilterOperator::new(predicate);

    // Test with NULL value
    let values_with_null = vec![
        Value::from_i64(1),
        Value::Null,
        Value::Text(Text::from("test")),
    ];
    assert!(!filter.evaluate_predicate(&values_with_null));

    // Test with non-NULL value (Integer)
    let values_with_integer = vec![
        Value::from_i64(1),
        Value::from_i64(42),
        Value::Text(Text::from("test")),
    ];
    assert!(filter.evaluate_predicate(&values_with_integer));

    // Test with non-NULL value (Text)
    let values_with_text = vec![
        Value::from_i64(1),
        Value::Text(Text::from("not null")),
        Value::Text(Text::from("test")),
    ];
    assert!(filter.evaluate_predicate(&values_with_text));

    // Test with non-NULL value (Blob)
    let values_with_blob = vec![
        Value::from_i64(1),
        Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG),
        Value::Text(Text::from("test")),
    ];
    assert!(filter.evaluate_predicate(&values_with_blob));
}

#[test]
fn test_is_null_with_and() {
    // Test: column_0 = 1 AND column_1 IS NULL
    let predicate = FilterPredicate::And(
        Box::new(FilterPredicate::Equals {
            column_idx: 0,
            value: Value::from_i64(1),
        }),
        Box::new(FilterPredicate::IsNull { column_idx: 1 }),
    );
    let filter = FilterOperator::new(predicate);

    // Should match: column_0 = 1 AND column_1 IS NULL
    let values_match = vec![
        Value::from_i64(1),
        Value::Null,
        Value::Text(Text::from("test")),
    ];
    assert!(filter.evaluate_predicate(&values_match));

    // Should not match: column_0 = 2 AND column_1 IS NULL
    let values_wrong_first = vec![
        Value::from_i64(2),
        Value::Null,
        Value::Text(Text::from("test")),
    ];
    assert!(!filter.evaluate_predicate(&values_wrong_first));

    // Should not match: column_0 = 1 AND column_1 IS NOT NULL
    let values_not_null = vec![
        Value::from_i64(1),
        Value::from_i64(42),
        Value::Text(Text::from("test")),
    ];
    assert!(!filter.evaluate_predicate(&values_not_null));
}

#[test]
fn test_is_not_null_with_or() {
    // Test: column_0 = 1 OR column_1 IS NOT NULL
    let predicate = FilterPredicate::Or(
        Box::new(FilterPredicate::Equals {
            column_idx: 0,
            value: Value::from_i64(1),
        }),
        Box::new(FilterPredicate::IsNotNull { column_idx: 1 }),
    );
    let filter = FilterOperator::new(predicate);

    // Should match: column_0 = 1 (regardless of column_1)
    let values_first_matches = vec![
        Value::from_i64(1),
        Value::Null,
        Value::Text(Text::from("test")),
    ];
    assert!(filter.evaluate_predicate(&values_first_matches));

    // Should match: column_1 IS NOT NULL (regardless of column_0)
    let values_second_matches = vec![
        Value::from_i64(2),
        Value::from_i64(42),
        Value::Text(Text::from("test")),
    ];
    assert!(filter.evaluate_predicate(&values_second_matches));

    // Should not match: column_0 != 1 AND column_1 IS NULL
    let values_no_match = vec![
        Value::from_i64(2),
        Value::Null,
        Value::Text(Text::from("test")),
    ];
    assert!(!filter.evaluate_predicate(&values_no_match));
}

#[test]
fn test_complex_null_predicates() {
    // Test: (column_0 IS NULL OR column_1 IS NOT NULL) AND column_2 = 'test'
    let predicate = FilterPredicate::And(
        Box::new(FilterPredicate::Or(
            Box::new(FilterPredicate::IsNull { column_idx: 0 }),
            Box::new(FilterPredicate::IsNotNull { column_idx: 1 }),
        )),
        Box::new(FilterPredicate::Equals {
            column_idx: 2,
            value: Value::Text(Text::from("test")),
        }),
    );
    let filter = FilterOperator::new(predicate);

    // Should match: column_0 IS NULL, column_2 = 'test'
    let values1 = vec![Value::Null, Value::Null, Value::Text(Text::from("test"))];
    assert!(filter.evaluate_predicate(&values1));

    // Should match: column_1 IS NOT NULL, column_2 = 'test'
    let values2 = vec![
        Value::from_i64(1),
        Value::from_i64(42),
        Value::Text(Text::from("test")),
    ];
    assert!(filter.evaluate_predicate(&values2));

    // Should not match: column_2 != 'test'
    let values3 = vec![
        Value::Null,
        Value::from_i64(42),
        Value::Text(Text::from("other")),
    ];
    assert!(!filter.evaluate_predicate(&values3));

    // Should not match: column_0 IS NOT NULL AND column_1 IS NULL AND column_2 = 'test'
    let values4 = vec![
        Value::from_i64(1),
        Value::Null,
        Value::Text(Text::from("test")),
    ];
    assert!(!filter.evaluate_predicate(&values4));
}

#[test]
fn test_cross_type_numeric_comparisons() {
    // GreaterThan: Integer > Float
    let predicate = FilterPredicate::GreaterThan {
        column_idx: 0,
        value: Value::from_f64(1.5),
    };
    let filter = FilterOperator::new(predicate);
    assert!(filter.evaluate_predicate(&[Value::from_i64(2)])); // 2 > 1.5
    assert!(!filter.evaluate_predicate(&[Value::from_i64(1)])); // 1 > 1.5

    // GreaterThan: Float > Integer
    let predicate = FilterPredicate::GreaterThan {
        column_idx: 0,
        value: Value::from_i64(2),
    };
    let filter = FilterOperator::new(predicate);
    assert!(filter.evaluate_predicate(&[Value::from_f64(2.5)])); // 2.5 > 2
    assert!(!filter.evaluate_predicate(&[Value::from_f64(1.5)])); // 1.5 > 2

    // GreaterThanOrEqual: Integer >= Float
    let predicate = FilterPredicate::GreaterThanOrEqual {
        column_idx: 0,
        value: Value::from_f64(2.0),
    };
    let filter = FilterOperator::new(predicate);
    assert!(filter.evaluate_predicate(&[Value::from_i64(2)])); // 2 >= 2.0
    assert!(filter.evaluate_predicate(&[Value::from_i64(3)])); // 3 >= 2.0
    assert!(!filter.evaluate_predicate(&[Value::from_i64(1)])); // 1 >= 2.0

    // GreaterThanOrEqual: Float >= Integer
    let predicate = FilterPredicate::GreaterThanOrEqual {
        column_idx: 0,
        value: Value::from_i64(2),
    };
    let filter = FilterOperator::new(predicate);
    assert!(filter.evaluate_predicate(&[Value::from_f64(2.0)])); // 2.0 >= 2
    assert!(!filter.evaluate_predicate(&[Value::from_f64(1.9)])); // 1.9 >= 2

    // LessThan: Integer < Float
    let predicate = FilterPredicate::LessThan {
        column_idx: 0,
        value: Value::from_f64(1.5),
    };
    let filter = FilterOperator::new(predicate);
    assert!(filter.evaluate_predicate(&[Value::from_i64(1)])); // 1 < 1.5
    assert!(!filter.evaluate_predicate(&[Value::from_i64(2)])); // 2 < 1.5

    // LessThan: Float < Integer
    let predicate = FilterPredicate::LessThan {
        column_idx: 0,
        value: Value::from_i64(2),
    };
    let filter = FilterOperator::new(predicate);
    assert!(filter.evaluate_predicate(&[Value::from_f64(1.5)])); // 1.5 < 2
    assert!(!filter.evaluate_predicate(&[Value::from_f64(2.5)])); // 2.5 < 2

    // LessThanOrEqual: Integer <= Float
    let predicate = FilterPredicate::LessThanOrEqual {
        column_idx: 0,
        value: Value::from_f64(2.0),
    };
    let filter = FilterOperator::new(predicate);
    assert!(filter.evaluate_predicate(&[Value::from_i64(2)])); // 2 <= 2.0
    assert!(filter.evaluate_predicate(&[Value::from_i64(1)])); // 1 <= 2.0
    assert!(!filter.evaluate_predicate(&[Value::from_i64(3)])); // 3 <= 2.0

    // LessThanOrEqual: Float <= Integer
    let predicate = FilterPredicate::LessThanOrEqual {
        column_idx: 0,
        value: Value::from_i64(2),
    };
    let filter = FilterOperator::new(predicate);
    assert!(filter.evaluate_predicate(&[Value::from_f64(2.0)])); // 2.0 <= 2
    assert!(!filter.evaluate_predicate(&[Value::from_f64(2.1)])); // 2.1 <= 2
}
