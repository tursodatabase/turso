use bloom::{BloomFilter as BloomFilterInner, ASMS};
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::types::{Value, ValueRef};

/// Default number of expected items for bloom filter sizing.
/// This is used when the expected count is not known ahead of time.
const DEFAULT_EXPECTED_ITEMS: u32 = 1024;

/// Default false positive rate (1%).
const DEFAULT_FALSE_POSITIVE_RATE: f32 = 0.01;

/// A bloom filter for fast probabilistic set membership testing.
///
/// Each bloom filter is associated with a cursor or operation that builds it
/// during ephemeral index/hash table construction.
pub struct BloomFilter {
    inner: BloomFilterInner,
    /// Number of items inserted into the filter
    count: usize,
}

impl fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BloomFilter")
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

impl BloomFilter {
    /// Creates a new bloom filter with default parameters.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_EXPECTED_ITEMS, DEFAULT_FALSE_POSITIVE_RATE)
    }

    /// Creates a new bloom filter with the specified expected item count and false positive rate.
    pub fn with_capacity(expected_items: u32, false_positive_rate: f32) -> Self {
        Self {
            inner: BloomFilterInner::with_rate(false_positive_rate, expected_items),
            count: 0,
        }
    }

    #[inline]
    /// Inserts an i64 value into the bloom filter.
    pub fn insert_i64(&mut self, value: i64) {
        self.inner.insert(&value);
        self.count += 1;
    }

    #[inline]
    /// Checks if an i64 value might be in the bloom filter.
    pub fn contains_i64(&self, value: i64) -> bool {
        self.inner.contains(&value)
    }

    #[inline]
    /// Inserts a byte slice into the bloom filter.
    pub fn insert_bytes(&mut self, value: &[u8]) {
        self.inner.insert(&value);
        self.count += 1;
    }

    #[inline]
    /// Checks if a byte slice might be in the bloom filter.
    pub fn contains_bytes(&self, value: &[u8]) -> bool {
        self.inner.contains(&value)
    }

    /// Inserts a Value into the bloom filter.
    /// Safety NOTE: does not accept NULL values.
    pub fn insert_value(&mut self, value: &Value) {
        if !matches!(value, Value::Null) {
            let mut hasher = rapidhash::fast::RapidHasher::default();
            hash_value(&mut hasher, &value.as_ref());
            let hash = hasher.finish();
            self.inner.insert(&hash);
        }
        self.count += 1;
    }

    /// Checks if a Value might be in the bloom filter.
    pub fn contains_value(&self, value: &Value) -> bool {
        if matches!(value, Value::Null) {
            return false;
        }
        let mut hasher = rapidhash::fast::RapidHasher::default();
        hash_value(&mut hasher, &value.as_ref());
        let hash = hasher.finish();
        self.inner.contains(&hash)
    }

    /// Inserts multiple owned Values as a composite key into the bloom filter.
    /// This is because bloom filters only support a single value insertion, so to handle multi
    /// join-key situations we hash the composite key into a single u64 and then insert that
    pub fn insert_values(&mut self, values: &[&Value]) {
        let mut hasher = rapidhash::fast::RapidHasher::default();
        for value in values {
            hash_value(&mut hasher, &value.as_ref());
        }
        let hash = hasher.finish();
        self.inner.insert(&hash);
        self.count += 1;
    }

    /// Checks if multiple owned Values as a composite key might be in the bloom filter.
    pub fn contains_values(&self, values: &[&Value]) -> bool {
        let mut hasher = rapidhash::fast::RapidHasher::default();
        for value in values {
            if matches!(value, Value::Null) {
                // if any value is NULL, we can never have a match
                return false;
            }
            hash_value(&mut hasher, &value.as_ref());
        }
        let hash = hasher.finish();
        self.inner.contains(&hash)
    }

    pub fn count(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn clear(&mut self) {
        self.inner.clear();
        self.count = 0;
    }
}

impl Default for BloomFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// Hashes an owned Value into the provided hasher.
fn hash_value<H: Hasher>(hasher: &mut H, value: &ValueRef) {
    match value {
        // do nothing for NULLs as we will always return false for set membership
        ValueRef::Null => {}
        ValueRef::Integer(i) => {
            // Hash integers in the same bucket as numerically equivalent REALs so
            // bloom-filter membership can never return a false-negative for e.g. 10 vs 10.0.
            let f = *i as f64;
            if (f as i64) == *i && f.is_finite() {
                hash_numeric(hasher, f);
            } else {
                // Fallback to the integer domain when the float representation would lose precision.
                1u8.hash(hasher);
                i.hash(hasher);
            }
        }
        ValueRef::Float(f) => {
            hash_numeric(hasher, *f);
        }
        ValueRef::Text(s) => {
            3u8.hash(hasher);
            s.as_str().hash(hasher);
        }
        ValueRef::Blob(b) => {
            4u8.hash(hasher);
            b.hash(hasher);
        }
    }
}

/// Hashes numeric values (both INTEGER and REAL) into the same domain to mirror SQLite's
/// numeric comparison semantics (e.g. 10 == 10.0, -0.0 == 0.0).
fn hash_numeric<H: Hasher>(hasher: &mut H, f: f64) {
    const NUMERIC_TAG: u8 = 2;
    let bits = normalized_f64_bits(f);
    NUMERIC_TAG.hash(hasher);
    bits.hash(hasher);
}

/// Normalize signed zero so 0.0 and -0.0 hash the same.
#[inline]
fn normalized_f64_bits(f: f64) -> u64 {
    if f == 0.0 {
        0.0f64.to_bits()
    } else {
        f.to_bits()
    }
}

#[cfg(test)]
mod bloomtests {
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

        let int_val = Value::Integer(42);
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
        let zero_float = Value::Float(0.0);
        let zero_neg_float = Value::Float(-0.0);
        let zero_int = Value::Integer(0);
        bf.insert_value(&zero_float);
        assert!(bf.contains_value(&zero_float));
        assert!(bf.contains_value(&zero_neg_float));
        assert!(bf.contains_value(&zero_int));

        // Integer/float representations of the same numeric value should match
        let ten_int = Value::Integer(10);
        let ten_float = Value::Float(10.0);
        bf.insert_value(&ten_int);
        assert!(bf.contains_value(&ten_int));
        assert!(bf.contains_value(&ten_float));

        let neg_ten_float = Value::Float(-10.0);
        let neg_ten_int = Value::Integer(-10);
        bf.insert_value(&neg_ten_float);
        assert!(bf.contains_value(&neg_ten_float));
        assert!(bf.contains_value(&neg_ten_int));
    }
}
