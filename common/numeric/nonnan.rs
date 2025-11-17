#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NonNan(f64);

impl NonNan {
    pub fn new(value: f64) -> Option<Self> {
        if value.is_nan() {
            return None;
        }

        Some(NonNan(value))
    }
}

impl PartialEq<NonNan> for f64 {
    fn eq(&self, other: &NonNan) -> bool {
        *self == other.0
    }
}

impl PartialEq<f64> for NonNan {
    fn eq(&self, other: &f64) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<f64> for NonNan {
    fn partial_cmp(&self, other: &f64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialOrd<NonNan> for f64 {
    fn partial_cmp(&self, other: &NonNan) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl From<i64> for NonNan {
    fn from(value: i64) -> Self {
        NonNan(value as f64)
    }
}

impl From<NonNan> for f64 {
    fn from(value: NonNan) -> Self {
        value.0
    }
}

impl std::ops::Deref for NonNan {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::Add for NonNan {
    type Output = Option<NonNan>;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.0 + rhs.0)
    }
}

impl std::ops::Sub for NonNan {
    type Output = Option<NonNan>;

    fn sub(self, rhs: Self) -> Self::Output {
        Self::new(self.0 - rhs.0)
    }
}

impl std::ops::Mul for NonNan {
    type Output = Option<NonNan>;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::new(self.0 * rhs.0)
    }
}

impl std::ops::Div for NonNan {
    type Output = Option<NonNan>;

    fn div(self, rhs: Self) -> Self::Output {
        Self::new(self.0 / rhs.0)
    }
}

impl std::ops::Rem for NonNan {
    type Output = Option<NonNan>;

    fn rem(self, rhs: Self) -> Self::Output {
        Self::new(self.0 % rhs.0)
    }
}

impl std::ops::Neg for NonNan {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}
