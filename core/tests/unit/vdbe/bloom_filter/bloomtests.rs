use super::*;
use crate::types::Text;

#[test]
fn test_bloom_filter_i64() {
    let mut bf = BloomFilter::new();
    bf.insert_i64(1);
    bf.insert_i64(2);
    bf.insert_i64(3);

    // These should definitely be found (no false negatives)
    assert!(bf.contains_i64(1));
    assert!(bf.contains_i64(2));
    assert!(bf.contains_i64(3));
}

#[test]
fn test_bloom_filter_values() {
    let mut bf = BloomFilter::new();

    let int_val = Value::from_i64(42);
    let text_val = Value::Text(Text::new("hello".to_string()));
    let null_val = Value::Null;

    bf.insert_value(&int_val);
    bf.insert_value(&text_val);
    bf.insert_value(&null_val);

    assert!(bf.contains_value(&int_val));
    assert!(bf.contains_value(&text_val));
    // NULLs are not hashed into the filter, so membership should be false.
    assert!(!bf.contains_value(&null_val));
}

#[test]
fn test_bloom_filter_false_positive_rate() {
    // Test that false positive rate is roughly as expected
    let mut bf = BloomFilter::with_capacity(1000, 0.01);

    // Insert 1000 values
    for i in 0..1000 {
        bf.insert_i64(i);
    }

    // Check false positive rate on non-inserted values
    let mut false_positives = 0;
    let test_count = 10000;
    for i in 1000..(1000 + test_count) {
        if bf.contains_i64(i) {
            false_positives += 1;
        }
    }

    // False positive rate should be around 1% (allow some variance)
    let rate = false_positives as f64 / test_count as f64;
    assert!(rate < 0.05, "False positive rate {rate} is too high");
}

#[test]
fn test_bloom_filter_numeric_equivalence() {
    let mut bf = BloomFilter::new();

    // Zero variants should all be found regardless of sign or int/float representation
    let zero_float = Value::from_f64(0.0);
    let zero_neg_float = Value::from_f64(-0.0);
    let zero_int = Value::from_i64(0);
    bf.insert_value(&zero_float);
    assert!(bf.contains_value(&zero_float));
    assert!(bf.contains_value(&zero_neg_float));
    assert!(bf.contains_value(&zero_int));

    // Integer/float representations of the same numeric value should match
    let ten_int = Value::from_i64(10);
    let ten_float = Value::from_f64(10.0);
    bf.insert_value(&ten_int);
    assert!(bf.contains_value(&ten_int));
    assert!(bf.contains_value(&ten_float));

    let neg_ten_float = Value::from_f64(-10.0);
    let neg_ten_int = Value::from_i64(-10);
    bf.insert_value(&neg_ten_float);
    assert!(bf.contains_value(&neg_ten_float));
    assert!(bf.contains_value(&neg_ten_int));
}
