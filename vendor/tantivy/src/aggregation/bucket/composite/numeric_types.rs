/// This module helps comparing numerical values of different types (i64, u64
/// and f64).
pub(super) mod num_cmp {
    use std::cmp::Ordering;

    use crate::TantivyError;

    pub fn cmp_i64_f64(left_i: i64, right_f: f64) -> crate::Result<Ordering> {
        if right_f.is_nan() {
            return Err(TantivyError::InvalidArgument(
                "NaN comparison is not supported".to_string(),
            ));
        }

        // If right_f is < i64::MIN then left_i > right_f (i64::MIN=-2^63 can be
        // exactly represented as f64)
        if right_f < i64::MIN as f64 {
            return Ok(Ordering::Greater);
        }
        // If right_f is >= i64::MAX then left_i < right_f (i64::MAX=2^63-1 cannot
        // be exactly represented as f64)
        if right_f >= i64::MAX as f64 {
            return Ok(Ordering::Less);
        }

        // Now right_f is in (i64::MIN, i64::MAX), so `right_f as i64` is
        // well-defined (truncation toward 0)
        let right_as_i = right_f as i64;

        let result = match left_i.cmp(&right_as_i) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                // they have the same integer part, compare the fraction
                let rem = right_f - (right_as_i as f64);
                if rem == 0.0 {
                    Ordering::Equal
                } else if right_f > 0.0 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
        };
        Ok(result)
    }

    pub fn cmp_u64_f64(left_u: u64, right_f: f64) -> crate::Result<Ordering> {
        if right_f.is_nan() {
            return Err(TantivyError::InvalidArgument(
                "NaN comparison is not supported".to_string(),
            ));
        }

        // Negative floats are always less than any u64 >= 0
        if right_f < 0.0 {
            return Ok(Ordering::Greater);
        }

        // If right_f is >= u64::MAX then left_u < right_f (u64::MAX=2^64-1 cannot be exactly)
        let max_as_f = u64::MAX as f64;
        if right_f > max_as_f {
            return Ok(Ordering::Less);
        }

        // Now right_f is in (0, u64::MAX), so `right_f as u64` is well-defined
        // (truncation toward 0)
        let right_as_u = right_f as u64;

        let result = match left_u.cmp(&right_as_u) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                // they have the same integer part, compare the fraction
                let rem = right_f - (right_as_u as f64);
                if rem == 0.0 {
                    Ordering::Equal
                } else {
                    Ordering::Less
                }
            }
        };
        Ok(result)
    }

    pub fn cmp_i64_u64(left_i: i64, right_u: u64) -> Ordering {
        if left_i < 0 {
            Ordering::Less
        } else {
            let left_as_u = left_i as u64;
            left_as_u.cmp(&right_u)
        }
    }
}

/// This module helps projecting numerical values to other numerical types.
/// When the target value space cannot exactly represent the source value, the
/// next representable value is returned (or AfterLast if the source value is
/// larger than the largest representable value).
///
/// All functions in this module assume that f64 values are not NaN.
pub(super) mod num_proj {
    #[derive(Debug, PartialEq)]
    pub enum ProjectedNumber<T> {
        Exact(T),
        Next(T),
        AfterLast,
    }

    pub fn i64_to_u64(value: i64) -> ProjectedNumber<u64> {
        if value < 0 {
            ProjectedNumber::Next(0)
        } else {
            ProjectedNumber::Exact(value as u64)
        }
    }

    pub fn u64_to_i64(value: u64) -> ProjectedNumber<i64> {
        if value > i64::MAX as u64 {
            ProjectedNumber::AfterLast
        } else {
            ProjectedNumber::Exact(value as i64)
        }
    }

    pub fn f64_to_u64(value: f64) -> ProjectedNumber<u64> {
        if value < 0.0 {
            ProjectedNumber::Next(0)
        } else if value > u64::MAX as f64 {
            ProjectedNumber::AfterLast
        } else if value.fract() == 0.0 {
            ProjectedNumber::Exact(value as u64)
        } else {
            // casting f64 to u64 truncates toward zero
            ProjectedNumber::Next(value as u64 + 1)
        }
    }

    pub fn f64_to_i64(value: f64) -> ProjectedNumber<i64> {
        if value < (i64::MIN as f64) {
            ProjectedNumber::Next(i64::MIN)
        } else if value >= (i64::MAX as f64) {
            ProjectedNumber::AfterLast
        } else if value.fract() == 0.0 {
            ProjectedNumber::Exact(value as i64)
        } else if value > 0.0 {
            // casting f64 to i64 truncates toward zero
            ProjectedNumber::Next(value as i64 + 1)
        } else {
            ProjectedNumber::Next(value as i64)
        }
    }

    pub fn i64_to_f64(value: i64) -> ProjectedNumber<f64> {
        let value_f = value as f64;
        let k_roundtrip = value_f as i64;
        if k_roundtrip == value {
            // between -2^53 and 2^53 all i64 are exactly represented as f64
            ProjectedNumber::Exact(value_f)
        } else {
            // for very large/small i64 values, it is approximated to the closest f64
            if k_roundtrip > value {
                ProjectedNumber::Next(value_f)
            } else {
                ProjectedNumber::Next(value_f.next_up())
            }
        }
    }

    pub fn u64_to_f64(value: u64) -> ProjectedNumber<f64> {
        let value_f = value as f64;
        let k_roundtrip = value_f as u64;
        if k_roundtrip == value {
            // between 0 and 2^53 all u64 are exactly represented as f64
            ProjectedNumber::Exact(value_f)
        } else if k_roundtrip > value {
            ProjectedNumber::Next(value_f)
        } else {
            ProjectedNumber::Next(value_f.next_up())
        }
    }
}

#[cfg(test)]
mod num_cmp_tests {
    use std::cmp::Ordering;

    use super::num_cmp::*;

    #[test]
    fn test_cmp_u64_f64() {
        // Basic comparisons
        assert_eq!(cmp_u64_f64(5, 5.0).unwrap(), Ordering::Equal);
        assert_eq!(cmp_u64_f64(5, 6.0).unwrap(), Ordering::Less);
        assert_eq!(cmp_u64_f64(6, 5.0).unwrap(), Ordering::Greater);
        assert_eq!(cmp_u64_f64(0, 0.0).unwrap(), Ordering::Equal);
        assert_eq!(cmp_u64_f64(0, 0.1).unwrap(), Ordering::Less);

        // Negative float values should always be less than any u64
        assert_eq!(cmp_u64_f64(0, -0.1).unwrap(), Ordering::Greater);
        assert_eq!(cmp_u64_f64(5, -5.0).unwrap(), Ordering::Greater);
        assert_eq!(cmp_u64_f64(u64::MAX, -1e20).unwrap(), Ordering::Greater);

        // Tests with extreme values
        assert_eq!(cmp_u64_f64(u64::MAX, 1e20).unwrap(), Ordering::Less);

        // Precision edge cases: large u64 that loses precision when converted to f64
        // => 2^54, exactly represented as f64
        let large_f64 = 18_014_398_509_481_984.0;
        let large_u64 = 18_014_398_509_481_984;
        // prove that large_u64 is exactly represented as f64
        assert_eq!(large_u64 as f64, large_f64);
        assert_eq!(cmp_u64_f64(large_u64, large_f64).unwrap(), Ordering::Equal);
        // => (2^54 + 1) cannot be exactly represented in f64
        let large_u64_plus_1 = 18_014_398_509_481_985;
        // prove that it is represented as f64 by large_f64
        assert_eq!(large_u64_plus_1 as f64, large_f64);
        assert_eq!(
            cmp_u64_f64(large_u64_plus_1, large_f64).unwrap(),
            Ordering::Greater
        );
        // => (2^54 - 1) cannot be exactly represented in f64
        let large_u64_minus_1 = 18_014_398_509_481_983;
        // prove that it is also represented as f64 by large_f64
        assert_eq!(large_u64_minus_1 as f64, large_f64);
        assert_eq!(
            cmp_u64_f64(large_u64_minus_1, large_f64).unwrap(),
            Ordering::Less
        );

        // NaN comparison results in an error
        assert!(cmp_u64_f64(0, f64::NAN).is_err());
    }

    #[test]
    fn test_cmp_i64_f64() {
        // Basic comparisons
        assert_eq!(cmp_i64_f64(5, 5.0).unwrap(), Ordering::Equal);
        assert_eq!(cmp_i64_f64(5, 6.0).unwrap(), Ordering::Less);
        assert_eq!(cmp_i64_f64(6, 5.0).unwrap(), Ordering::Greater);
        assert_eq!(cmp_i64_f64(-5, -5.0).unwrap(), Ordering::Equal);
        assert_eq!(cmp_i64_f64(-5, -4.0).unwrap(), Ordering::Less);
        assert_eq!(cmp_i64_f64(-4, -5.0).unwrap(), Ordering::Greater);
        assert_eq!(cmp_i64_f64(-5, 5.0).unwrap(), Ordering::Less);
        assert_eq!(cmp_i64_f64(5, -5.0).unwrap(), Ordering::Greater);
        assert_eq!(cmp_i64_f64(0, -0.1).unwrap(), Ordering::Greater);
        assert_eq!(cmp_i64_f64(0, 0.1).unwrap(), Ordering::Less);
        assert_eq!(cmp_i64_f64(-1, -0.5).unwrap(), Ordering::Less);
        assert_eq!(cmp_i64_f64(-1, 0.0).unwrap(), Ordering::Less);
        assert_eq!(cmp_i64_f64(0, 0.0).unwrap(), Ordering::Equal);

        // Tests with extreme values
        assert_eq!(cmp_i64_f64(i64::MAX, 1e20).unwrap(), Ordering::Less);
        assert_eq!(cmp_i64_f64(i64::MIN, -1e20).unwrap(), Ordering::Greater);

        // Precision edge cases: large i64 that loses precision when converted to f64
        // => 2^54, exactly represented as f64
        let large_f64 = 18_014_398_509_481_984.0;
        let large_i64 = 18_014_398_509_481_984;
        // prove that large_i64 is exactly represented as f64
        assert_eq!(large_i64 as f64, large_f64);
        assert_eq!(cmp_i64_f64(large_i64, large_f64).unwrap(), Ordering::Equal);
        // => (1_i64 << 54) + 1 cannot be exactly represented in f64
        let large_i64_plus_1 = 18_014_398_509_481_985;
        // prove that it is represented as f64 by large_f64
        assert_eq!(large_i64_plus_1 as f64, large_f64);
        assert_eq!(
            cmp_i64_f64(large_i64_plus_1, large_f64).unwrap(),
            Ordering::Greater
        );
        // => (1_i64 << 54) - 1 cannot be exactly represented in f64
        let large_i64_minus_1 = 18_014_398_509_481_983;
        // prove that it is also represented as f64 by large_f64
        assert_eq!(large_i64_minus_1 as f64, large_f64);
        assert_eq!(
            cmp_i64_f64(large_i64_minus_1, large_f64).unwrap(),
            Ordering::Less
        );

        // Same precision edge case but with negative values
        // => -2^54, exactly represented as f64
        let large_neg_f64 = -18_014_398_509_481_984.0;
        let large_neg_i64 = -18_014_398_509_481_984;
        // prove that large_neg_i64 is exactly represented as f64
        assert_eq!(large_neg_i64 as f64, large_neg_f64);
        assert_eq!(
            cmp_i64_f64(large_neg_i64, large_neg_f64).unwrap(),
            Ordering::Equal
        );
        // => (-2^54 + 1) cannot be exactly represented in f64
        let large_neg_i64_plus_1 = -18_014_398_509_481_985;
        // prove that it is represented as f64 by large_neg_f64
        assert_eq!(large_neg_i64_plus_1 as f64, large_neg_f64);
        assert_eq!(
            cmp_i64_f64(large_neg_i64_plus_1, large_neg_f64).unwrap(),
            Ordering::Less
        );
        // => (-2^54 - 1) cannot be exactly represented in f64
        let large_neg_i64_minus_1 = -18_014_398_509_481_983;
        // prove that it is also represented as f64 by large_neg_f64
        assert_eq!(large_neg_i64_minus_1 as f64, large_neg_f64);
        assert_eq!(
            cmp_i64_f64(large_neg_i64_minus_1, large_neg_f64).unwrap(),
            Ordering::Greater
        );

        // NaN comparison results in an error
        assert!(cmp_i64_f64(0, f64::NAN).is_err());
    }

    #[test]
    fn test_cmp_i64_u64() {
        // Test with negative i64 values (should always be less than any u64)
        assert_eq!(cmp_i64_u64(-1, 0), Ordering::Less);
        assert_eq!(cmp_i64_u64(i64::MIN, 0), Ordering::Less);
        assert_eq!(cmp_i64_u64(i64::MIN, u64::MAX), Ordering::Less);

        // Test with positive i64 values
        assert_eq!(cmp_i64_u64(0, 0), Ordering::Equal);
        assert_eq!(cmp_i64_u64(1, 0), Ordering::Greater);
        assert_eq!(cmp_i64_u64(1, 1), Ordering::Equal);
        assert_eq!(cmp_i64_u64(0, 1), Ordering::Less);
        assert_eq!(cmp_i64_u64(5, 10), Ordering::Less);
        assert_eq!(cmp_i64_u64(10, 5), Ordering::Greater);

        // Test with values near i64::MAX and u64 conversion
        assert_eq!(cmp_i64_u64(i64::MAX, i64::MAX as u64), Ordering::Equal);
        assert_eq!(cmp_i64_u64(i64::MAX, (i64::MAX as u64) + 1), Ordering::Less);
        assert_eq!(cmp_i64_u64(i64::MAX, u64::MAX), Ordering::Less);
    }
}

#[cfg(test)]
mod num_proj_tests {
    use super::num_proj::{self, ProjectedNumber};

    #[test]
    fn test_i64_to_u64() {
        assert_eq!(num_proj::i64_to_u64(-1), ProjectedNumber::Next(0));
        assert_eq!(num_proj::i64_to_u64(i64::MIN), ProjectedNumber::Next(0));
        assert_eq!(num_proj::i64_to_u64(0), ProjectedNumber::Exact(0));
        assert_eq!(num_proj::i64_to_u64(42), ProjectedNumber::Exact(42));
        assert_eq!(
            num_proj::i64_to_u64(i64::MAX),
            ProjectedNumber::Exact(i64::MAX as u64)
        );
    }

    #[test]
    fn test_u64_to_i64() {
        assert_eq!(num_proj::u64_to_i64(0), ProjectedNumber::Exact(0));
        assert_eq!(num_proj::u64_to_i64(42), ProjectedNumber::Exact(42));
        assert_eq!(
            num_proj::u64_to_i64(i64::MAX as u64),
            ProjectedNumber::Exact(i64::MAX)
        );
        assert_eq!(
            num_proj::u64_to_i64((i64::MAX as u64) + 1),
            ProjectedNumber::AfterLast
        );
        assert_eq!(num_proj::u64_to_i64(u64::MAX), ProjectedNumber::AfterLast);
    }

    #[test]
    fn test_f64_to_u64() {
        assert_eq!(num_proj::f64_to_u64(-1e25), ProjectedNumber::Next(0));
        assert_eq!(num_proj::f64_to_u64(-0.1), ProjectedNumber::Next(0));
        assert_eq!(num_proj::f64_to_u64(1e20), ProjectedNumber::AfterLast);
        assert_eq!(
            num_proj::f64_to_u64(f64::INFINITY),
            ProjectedNumber::AfterLast
        );
        assert_eq!(num_proj::f64_to_u64(0.0), ProjectedNumber::Exact(0));
        assert_eq!(num_proj::f64_to_u64(42.0), ProjectedNumber::Exact(42));
        assert_eq!(num_proj::f64_to_u64(0.5), ProjectedNumber::Next(1));
        assert_eq!(num_proj::f64_to_u64(42.1), ProjectedNumber::Next(43));
    }

    #[test]
    fn test_f64_to_i64() {
        assert_eq!(num_proj::f64_to_i64(-1e20), ProjectedNumber::Next(i64::MIN));
        assert_eq!(
            num_proj::f64_to_i64(f64::NEG_INFINITY),
            ProjectedNumber::Next(i64::MIN)
        );
        assert_eq!(num_proj::f64_to_i64(1e20), ProjectedNumber::AfterLast);
        assert_eq!(
            num_proj::f64_to_i64(f64::INFINITY),
            ProjectedNumber::AfterLast
        );
        assert_eq!(num_proj::f64_to_i64(0.0), ProjectedNumber::Exact(0));
        assert_eq!(num_proj::f64_to_i64(42.0), ProjectedNumber::Exact(42));
        assert_eq!(num_proj::f64_to_i64(-42.0), ProjectedNumber::Exact(-42));
        assert_eq!(num_proj::f64_to_i64(0.5), ProjectedNumber::Next(1));
        assert_eq!(num_proj::f64_to_i64(42.1), ProjectedNumber::Next(43));
        assert_eq!(num_proj::f64_to_i64(-0.5), ProjectedNumber::Next(0));
        assert_eq!(num_proj::f64_to_i64(-42.1), ProjectedNumber::Next(-42));
    }

    #[test]
    fn test_i64_to_f64() {
        assert_eq!(num_proj::i64_to_f64(0), ProjectedNumber::Exact(0.0));
        assert_eq!(num_proj::i64_to_f64(42), ProjectedNumber::Exact(42.0));
        assert_eq!(num_proj::i64_to_f64(-42), ProjectedNumber::Exact(-42.0));

        let max_exact = 9_007_199_254_740_992; // 2^53
        assert_eq!(
            num_proj::i64_to_f64(max_exact),
            ProjectedNumber::Exact(max_exact as f64)
        );

        // Test values that cannot be exactly represented as f64 (integers above 2^53)
        let large_i64 = 9_007_199_254_740_993; // 2^53 + 1
        let closest_f64 = 9_007_199_254_740_992.0;
        assert_eq!(large_i64 as f64, closest_f64);
        if let ProjectedNumber::Next(val) = num_proj::i64_to_f64(large_i64) {
            // Verify that the returned float is different from the direct cast
            assert!(val > closest_f64);
            assert!(val - closest_f64 < 2. * f64::EPSILON * closest_f64);
        } else {
            panic!("Expected ProjectedNumber::Next for large_i64");
        }

        // Test with very large negative value
        let large_neg_i64 = -9_007_199_254_740_993; // -(2^53 + 1)
        let closest_neg_f64 = -9_007_199_254_740_992.0;
        assert_eq!(large_neg_i64 as f64, closest_neg_f64);
        if let ProjectedNumber::Next(val) = num_proj::i64_to_f64(large_neg_i64) {
            // Verify that the returned float is the closest representable f64
            assert_eq!(val, closest_neg_f64);
        } else {
            panic!("Expected ProjectedNumber::Next for large_neg_i64");
        }
    }

    #[test]
    fn test_u64_to_f64() {
        assert_eq!(num_proj::u64_to_f64(0), ProjectedNumber::Exact(0.0));
        assert_eq!(num_proj::u64_to_f64(42), ProjectedNumber::Exact(42.0));

        // Test the largest u64 value that can be exactly represented as f64 (2^53)
        let max_exact = 9_007_199_254_740_992; // 2^53
        assert_eq!(
            num_proj::u64_to_f64(max_exact),
            ProjectedNumber::Exact(max_exact as f64)
        );

        // Test values that cannot be exactly represented as f64 (integers above 2^53)
        let large_u64 = 9_007_199_254_740_993; // 2^53 + 1
        let closest_f64 = 9_007_199_254_740_992.0;
        assert_eq!(large_u64 as f64, closest_f64);
        if let ProjectedNumber::Next(val) = num_proj::u64_to_f64(large_u64) {
            // Verify that the returned float is different from the direct cast
            assert!(val > closest_f64);
            assert!(val - closest_f64 < 2. * f64::EPSILON * closest_f64);
        } else {
            panic!("Expected ProjectedNumber::Next for large_u64");
        }
    }
}
