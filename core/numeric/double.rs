#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Double(f64);

impl Double {
    pub const fn non_nan(value: f64) -> Option<Self> {
        if value.is_nan() {
            return None;
        }

        Some(Double(value))
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
        Self::non_nan(self.0 + rhs.0)
    }
}

impl std::ops::Sub for Double {
    type Output = Option<Double>;

    fn sub(self, rhs: Self) -> Self::Output {
        Self::non_nan(self.0 - rhs.0)
    }
}

impl std::ops::Mul for Double {
    type Output = Option<Double>;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::non_nan(self.0 * rhs.0)
    }
}

impl std::ops::Div for Double {
    type Output = Option<Double>;

    fn div(self, rhs: Self) -> Self::Output {
        Self::non_nan(self.0 / rhs.0)
    }
}

impl std::ops::Rem for Double {
    type Output = Option<Double>;

    fn rem(self, rhs: Self) -> Self::Output {
        Self::non_nan(self.0 % rhs.0)
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
