use std::cmp::Ordering;

/// A REAL value. SQL dialect rules are applied when results are published.
///
/// Values are totally ordered, as PostgreSQL orders floats: NaN equals NaN and
/// is greater than every number, and -0 equals 0.
#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Double(f64);

impl Double {
    pub const fn new(value: f64) -> Self {
        Double(value)
    }

    pub const fn non_nan(value: f64) -> Option<Self> {
        if value.is_nan() {
            return None;
        }

        Some(Double(value))
    }

    pub const fn is_nan(self) -> bool {
        self.0.is_nan()
    }

    /// Bits for hashing and sort prefixes, consistent with this type's equality.
    /// All NaNs share one positive encoding, as do both signs of zero.
    pub const fn canonical_bits(self) -> u64 {
        if self.0 == 0.0 {
            0.0f64.to_bits()
        } else if self.0.is_nan() {
            // Rust does not specify the sign or payload of f64::NAN.
            0x7ff8_0000_0000_0000
        } else {
            self.0.to_bits()
        }
    }
}

impl PartialEq for Double {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Double {}

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp_pg(other.0)
    }
}

trait TotalCmpPg {
    fn total_cmp_pg(self, other: f64) -> Ordering;
}

impl TotalCmpPg for f64 {
    fn total_cmp_pg(self, other: f64) -> Ordering {
        match (self.is_nan(), other.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => self.partial_cmp(&other).unwrap_or(Ordering::Equal),
        }
    }
}

impl PartialEq<Double> for f64 {
    fn eq(&self, other: &Double) -> bool {
        *self == other.0
    }
}

impl PartialEq<f64> for Double {
    fn eq(&self, other: &f64) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<f64> for Double {
    fn partial_cmp(&self, other: &f64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialOrd<Double> for f64 {
    fn partial_cmp(&self, other: &Double) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl From<i64> for Double {
    fn from(value: i64) -> Self {
        Double(value as f64)
    }
}

impl From<Double> for f64 {
    fn from(value: Double) -> Self {
        value.0
    }
}

impl std::ops::Deref for Double {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::Add for Double {
    type Output = Option<Double>;

    fn add(self, rhs: Self) -> Self::Output {
        Some(Self::new(self.0 + rhs.0))
    }
}

impl std::ops::Sub for Double {
    type Output = Option<Double>;

    fn sub(self, rhs: Self) -> Self::Output {
        Some(Self::new(self.0 - rhs.0))
    }
}

impl std::ops::Mul for Double {
    type Output = Option<Double>;

    fn mul(self, rhs: Self) -> Self::Output {
        Some(Self::new(self.0 * rhs.0))
    }
}

impl std::ops::Div for Double {
    type Output = Option<Double>;

    fn div(self, rhs: Self) -> Self::Output {
        Some(Self::new(self.0 / rhs.0))
    }
}

impl std::ops::Rem for Double {
    type Output = Option<Double>;

    fn rem(self, rhs: Self) -> Self::Output {
        Some(Self::new(self.0 % rhs.0))
    }
}

impl std::fmt::Display for Double {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Neg for Double {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nan_equals_itself_and_is_greater_than_every_number() {
        let nan = Double::new(f64::NAN);
        let numbers = [f64::NEG_INFINITY, -1.5, -0.0, 0.0, 1.5, f64::INFINITY].map(Double::new);
        assert_eq!(nan, Double::new(-f64::NAN));
        for number in numbers {
            assert!(nan > number);
            assert!(number < nan);
            assert_ne!(nan, number);
        }
    }

    #[test]
    fn negative_zero_equals_zero() {
        assert_eq!(Double::new(-0.0), Double::new(0.0));
        assert_eq!(Double::new(-0.0).cmp(&Double::new(0.0)), Ordering::Equal);
    }

    #[test]
    fn non_nan_refuses_nan_and_new_keeps_it() {
        assert!(Double::non_nan(f64::NAN).is_none());
        assert!(Double::non_nan(f64::INFINITY).is_some());
        assert!(Double::new(f64::NAN).is_nan());
    }

    #[test]
    fn arithmetic_preserves_nan() {
        let infinity = Double::new(f64::INFINITY);
        assert!((infinity - infinity).unwrap().is_nan());
        assert!((Double::new(f64::NAN) + Double::new(1.0)).unwrap().is_nan());
    }

    #[test]
    fn a_numeric_constructor_preserves_nan() {
        assert!(matches!(
            crate::Value::from_f64(f64::NAN),
            crate::Value::Numeric(crate::Numeric::Float(f)) if f.is_nan()
        ));
    }
}
