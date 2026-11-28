use crate::{turso_debug_assert, types::AsValueRef, Value, ValueRef};

pub mod decimal;
pub mod nonnan;

use nonnan::NonNan;

// TODO: Remove when https://github.com/rust-lang/libs-team/issues/230 is available
trait SaturatingShl {
    fn saturating_shl(self, rhs: u32) -> Self;
}

impl SaturatingShl for i64 {
    fn saturating_shl(self, rhs: u32) -> Self {
        if rhs >= Self::BITS {
            0
        } else {
            self << rhs
        }
    }
}

// TODO: Remove when https://github.com/rust-lang/libs-team/issues/230 is available
trait SaturatingShr {
    fn saturating_shr(self, rhs: u32) -> Self;
}

impl SaturatingShr for i64 {
    fn saturating_shr(self, rhs: u32) -> Self {
        if rhs >= Self::BITS {
            if self >= 0 {
                0
            } else {
                -1
            }
        } else {
            self >> rhs
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Numeric {
    Integer(i64),
    Float(NonNan),
}

impl Numeric {
    pub fn from_value<T: AsValueRef>(value: T) -> Option<Self> {
        let value = value.as_value_ref();

        match value {
            ValueRef::Null => None,
            ValueRef::Numeric(v) => Some(v),
            ValueRef::Text(text) => Some(Numeric::from(text.as_str())),
            ValueRef::Blob(blob) => {
                let text = String::from_utf8_lossy(blob);
                Some(Numeric::from(&text))
            }
        }
    }

    #[inline]
    pub fn from_value_strict(value: &Value) -> Option<Self> {
        match value {
            Value::Null | Value::Blob(_) => None,
            Value::Numeric(n) => Some(*n),
            Value::Text(text) => {
                let s = text.as_str();

                match str_to_f64(s) {
                    None
                    | Some(StrToF64::FractionalPrefix(_))
                    | Some(StrToF64::DecimalPrefix(_)) => None,
                    Some(StrToF64::Fractional(value)) => Some(Self::Float(value)),
                    Some(StrToF64::Decimal(real)) => Some(match str_to_i64_checked(s) {
                        Ok(integer) => Self::Integer(integer),
                        Err(_) => Self::Float(real),
                    }),
                }
            }
        }
    }

    #[inline]
    pub fn to_f64(&self) -> f64 {
        match self {
            Numeric::Integer(v) => *v as _,
            Numeric::Float(v) => (*v).into(),
        }
    }

    #[inline]
    pub fn to_bool(&self) -> bool {
        match self {
            Numeric::Integer(0) => false,
            Numeric::Float(non_nan) if *non_nan == 0.0 => false,
            _ => true,
        }
    }

    #[inline]
    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        match (self, rhs) {
            (Numeric::Integer(lhs), Numeric::Integer(rhs)) => match lhs.checked_add(rhs) {
                None => Numeric::Float(lhs.into()).checked_add(Numeric::Float(rhs.into())),
                Some(i) => Some(Numeric::Integer(i)),
            },
            (Numeric::Float(lhs), Numeric::Float(rhs)) => (lhs + rhs).map(Numeric::Float),
            (f @ Numeric::Float(_), Numeric::Integer(i))
            | (Numeric::Integer(i), f @ Numeric::Float(_)) => {
                f.checked_add(Numeric::Float(i.into()))
            }
        }
    }

    #[inline]
    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        match (self, rhs) {
            (Numeric::Float(lhs), Numeric::Float(rhs)) => (lhs - rhs).map(Numeric::Float),
            (Numeric::Integer(lhs), Numeric::Integer(rhs)) => match lhs.checked_sub(rhs) {
                None => Numeric::Float(lhs.into()).checked_sub(Numeric::Float(rhs.into())),
                Some(i) => Some(Numeric::Integer(i)),
            },
            (f @ Numeric::Float(_), Numeric::Integer(i)) => f.checked_sub(Numeric::Float(i.into())),
            (Numeric::Integer(i), f @ Numeric::Float(_)) => Numeric::Float(i.into()).checked_sub(f),
        }
    }

    #[inline]
    pub fn checked_mul(self, rhs: Self) -> Option<Self> {
        match (self, rhs) {
            (Numeric::Float(lhs), Numeric::Float(rhs)) => (lhs * rhs).map(Numeric::Float),
            (Numeric::Integer(lhs), Numeric::Integer(rhs)) => match lhs.checked_mul(rhs) {
                None => Numeric::Float(lhs.into()).checked_mul(Numeric::Float(rhs.into())),
                Some(i) => Some(Numeric::Integer(i)),
            },
            (f @ Numeric::Float(_), Numeric::Integer(i))
            | (Numeric::Integer(i), f @ Numeric::Float(_)) => {
                f.checked_mul(Numeric::Float(i.into()))
            }
        }
    }

    #[inline]
    pub fn checked_div(self, rhs: Self) -> Option<Self> {
        match (self, rhs) {
            (Numeric::Float(lhs), Numeric::Float(rhs)) => match lhs / rhs {
                Some(v) if rhs != 0.0 => Some(Numeric::Float(v)),
                _ => None,
            },
            (Numeric::Integer(lhs), Numeric::Integer(rhs)) => match lhs.checked_div(rhs) {
                None => Numeric::Float(lhs.into()).checked_div(Numeric::Float(rhs.into())),
                Some(v) => Some(Numeric::Integer(v)),
            },
            (f @ Numeric::Float(_), Numeric::Integer(i)) => f.checked_div(Numeric::Float(i.into())),
            (Numeric::Integer(i), f @ Numeric::Float(_)) => Numeric::Float(i.into()).checked_div(f),
        }
    }
}

impl From<Numeric> for NullableInteger {
    #[inline]
    fn from(value: Numeric) -> Self {
        match value {
            Numeric::Integer(v) => NullableInteger::Integer(v),
            Numeric::Float(v) => NullableInteger::Integer(f64::from(v) as i64),
        }
    }
}

impl From<Numeric> for Value {
    #[inline]
    fn from(value: Numeric) -> Self {
        Value::Numeric(value)
    }
}

impl From<Option<Numeric>> for Value {
    fn from(value: Option<Numeric>) -> Self {
        value.map_or_else(|| Value::Null, Value::from)
    }
}

impl<T: AsRef<str>> From<T> for Numeric {
    fn from(value: T) -> Self {
        let text = value.as_ref();

        match str_to_f64(text) {
            None => Self::Integer(0),
            Some(StrToF64::Fractional(value) | StrToF64::FractionalPrefix(value)) => {
                Self::Float(value)
            }
            Some(StrToF64::Decimal(real) | StrToF64::DecimalPrefix(real)) => {
                match str_to_i64_checked(text) {
                    Ok(integer) => Self::Integer(integer),
                    Err(_) => Self::Float(real),
                }
            }
        }
    }
}

impl From<Value> for Option<Numeric> {
    fn from(value: Value) -> Self {
        Self::from(&value)
    }
}
impl From<&Value> for Option<Numeric> {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => None,
            Value::Numeric(n) => Some(*n),
            Value::Text(text) => Some(Numeric::from(text.as_str())),
            Value::Blob(blob) => {
                let text = String::from_utf8_lossy(blob.as_slice());
                Some(Numeric::from(&text))
            }
        }
    }
}

impl std::ops::Neg for Numeric {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            Numeric::Integer(v) => match v.checked_neg() {
                None => -Numeric::Float(v.into()),
                Some(i) => Numeric::Integer(i),
            },
            Numeric::Float(v) => Numeric::Float(-v),
        }
    }
}

impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Numeric {}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Numeric {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Numeric::Integer(a), Numeric::Integer(b)) => a.cmp(b),
            (Numeric::Float(a), Numeric::Float(b)) => a.cmp(b),
            (Numeric::Integer(int), Numeric::Float(float)) => {
                sqlite_int_float_cmp(*int, f64::from(*float))
            }
            (Numeric::Float(float), Numeric::Integer(int)) => {
                sqlite_int_float_cmp(*int, f64::from(*float)).reverse()
            }
        }
    }
}

/// Compare an integer and a float following SQLite semantics.
///
/// NaN is greater than every number. A binary whose frontend has no NaN never
/// holds one, so its comparisons do not reach this case.
///
/// See sqlite3IntFloatCompare in src/vdbeaux.c.
fn sqlite_int_float_cmp(int_val: i64, float_val: f64) -> std::cmp::Ordering {
    if float_val.is_nan() {
        return std::cmp::Ordering::Less;
    }

    if float_val < -9_223_372_036_854_775_808.0 {
        return std::cmp::Ordering::Greater;
    }
    if float_val >= 9_223_372_036_854_775_808.0 {
        return std::cmp::Ordering::Less;
    }

    let float_as_int = float_val as i64;
    match int_val.cmp(&float_as_int) {
        std::cmp::Ordering::Equal => {
            let int_as_float = int_val as f64;
            int_as_float
                .partial_cmp(&float_val)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        other => other,
    }
}

#[derive(Debug)]
pub enum NullableInteger {
    Null,
    Integer(i64),
}

impl From<NullableInteger> for Value {
    fn from(value: NullableInteger) -> Self {
        match value {
            NullableInteger::Null => Value::Null,
            NullableInteger::Integer(v) => Value::from_i64(v),
        }
    }
}

impl<T: AsRef<str>> From<T> for NullableInteger {
    fn from(value: T) -> Self {
        Self::Integer(str_to_i64(value.as_ref()).unwrap_or(0))
    }
}

impl From<Value> for NullableInteger {
    fn from(value: Value) -> Self {
        Self::from(&value)
    }
}

impl From<&Value> for NullableInteger {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Numeric(Numeric::Integer(v)) => Self::Integer(*v),
            Value::Numeric(Numeric::Float(v)) => Self::Integer(f64::from(*v) as i64),
            Value::Text(text) => Self::from(text.as_str()),
            Value::Blob(blob) => {
                let text = String::from_utf8_lossy(blob.as_slice());
                Self::from(text)
            }
        }
    }
}

impl std::ops::Not for NullableInteger {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            NullableInteger::Null => NullableInteger::Null,
            NullableInteger::Integer(lhs) => NullableInteger::Integer(!lhs),
        }
    }
}

impl std::ops::BitAnd for NullableInteger {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NullableInteger::Null, _) | (_, NullableInteger::Null) => NullableInteger::Null,
            (NullableInteger::Integer(lhs), NullableInteger::Integer(rhs)) => {
                NullableInteger::Integer(lhs & rhs)
            }
        }
    }
}

impl std::ops::BitOr for NullableInteger {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NullableInteger::Null, _) | (_, NullableInteger::Null) => NullableInteger::Null,
            (NullableInteger::Integer(lhs), NullableInteger::Integer(rhs)) => {
                NullableInteger::Integer(lhs | rhs)
            }
        }
    }
}

impl std::ops::Shl for NullableInteger {
    type Output = Self;

    fn shl(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NullableInteger::Null, _) | (_, NullableInteger::Null) => NullableInteger::Null,
            (NullableInteger::Integer(lhs), NullableInteger::Integer(rhs)) => {
                NullableInteger::Integer(if rhs.is_positive() {
                    lhs.saturating_shl(rhs.try_into().unwrap_or(u32::MAX))
                } else {
                    lhs.saturating_shr(rhs.saturating_abs().try_into().unwrap_or(u32::MAX))
                })
            }
        }
    }
}

impl std::ops::Shr for NullableInteger {
    type Output = Self;

    fn shr(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NullableInteger::Null, _) | (_, NullableInteger::Null) => NullableInteger::Null,
            (NullableInteger::Integer(lhs), NullableInteger::Integer(rhs)) => {
                NullableInteger::Integer(if rhs.is_positive() {
                    lhs.saturating_shr(rhs.try_into().unwrap_or(u32::MAX))
                } else {
                    lhs.saturating_shl(rhs.saturating_abs().try_into().unwrap_or(u32::MAX))
                })
            }
        }
    }
}

impl std::ops::Rem for NullableInteger {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NullableInteger::Null, _) | (_, NullableInteger::Null) => NullableInteger::Null,
            (_, NullableInteger::Integer(0)) => NullableInteger::Null,
            (lhs, NullableInteger::Integer(-1)) => lhs % NullableInteger::Integer(1),
            (NullableInteger::Integer(lhs), NullableInteger::Integer(rhs)) => {
                NullableInteger::Integer(lhs % rhs)
            }
        }
    }
}

// Maximum u64 that can survive a f64 round trip
const MAX_EXACT: u64 = u64::MAX << 11;

const VERTICAL_TAB: char = '\u{b}';

/// Encapsulates Dekker's arithmetic for higher precision. This is spiritually the same as using a
/// f128 for arithmetic, but cross platform and compatible with sqlite.
#[derive(Debug, Clone, Copy)]
pub struct DoubleDouble(pub f64, pub f64);

impl DoubleDouble {
    pub const E100: Self = DoubleDouble(1.0e+100, -1.590_289_110_975_991_8e83);
    pub const E10: Self = DoubleDouble(1.0e+10, 0.0);
    pub const E1: Self = DoubleDouble(1.0e+01, 0.0);

    pub const NEG_E100: Self = DoubleDouble(1.0e-100, -1.999_189_980_260_288_3e-117);
    pub const NEG_E10: Self = DoubleDouble(1.0e-10, -3.643_219_731_549_774e-27);
    pub const NEG_E1: Self = DoubleDouble(1.0e-01, -5.551_115_123_125_783e-18);
}

impl From<u64> for DoubleDouble {
    fn from(value: u64) -> Self {
        let r = value as f64;

        // If the value is smaller than MAX_EXACT, the error isn't significant
        let rr = if r <= MAX_EXACT as f64 {
            let round_tripped = value as f64 as u64;
            let sign = if value >= round_tripped { 1.0 } else { -1.0 };

            // Error term is the signed distance of the round tripped value and itself
            sign * value.abs_diff(round_tripped) as f64
        } else {
            0.0
        };

        DoubleDouble(r, rr)
    }
}

impl From<DoubleDouble> for u64 {
    fn from(value: DoubleDouble) -> Self {
        if value.1 < 0.0 {
            value.0 as u64 - value.1.abs() as u64
        } else {
            value.0 as u64 + value.1 as u64
        }
    }
}

impl From<DoubleDouble> for f64 {
    fn from(DoubleDouble(a, aa): DoubleDouble) -> Self {
        a + aa
    }
}

impl std::ops::Mul for DoubleDouble {
    type Output = Self;

    /// Double-Double multiplication.  (self.0, self.1) *= (rhs.0, rhs.1)
    ///
    /// Reference:
    ///   T. J. Dekker, "A Floating-Point Technique for Extending the Available Precision".
    ///   1971-07-26.
    ///
    fn mul(self, rhs: Self) -> Self::Output {
        // TODO: Better variable naming

        let mask = u64::MAX << 26;

        let hx = f64::from_bits(self.0.to_bits() & mask);
        let tx = self.0 - hx;

        let hy = f64::from_bits(rhs.0.to_bits() & mask);
        let ty = rhs.0 - hy;

        let p = hx * hy;
        let q = hx * ty + tx * hy;

        let c = p + q;
        let cc = p - c + q + tx * ty;
        let cc = self.0 * rhs.1 + self.1 * rhs.0 + cc;

        let r = c + cc;
        let rr = (c - r) + cc;

        DoubleDouble(r, rr)
    }
}

impl std::ops::MulAssign for DoubleDouble {
    fn mul_assign(&mut self, rhs: Self) {
        *self = *self * rhs;
    }
}

pub fn str_to_i64(input: impl AsRef<str>) -> Option<i64> {
    Some(match str_to_i64_checked(input) {
        Ok(v) => v,
        Err(IntOverflow::Positive) => i64::MAX,
        Err(IntOverflow::Negative) => i64::MIN,
    })
}

#[derive(Debug, Clone, Copy)]
pub enum IntOverflow {
    Positive,
    Negative,
}

/// Like [`str_to_i64`] but distinguishes overflow from a successful parse.
/// Returns `Err(_)` when the leading numeric prefix does not fit in i64.
pub fn str_to_i64_checked(input: impl AsRef<str>) -> Result<i64, IntOverflow> {
    let input = input
        .as_ref()
        .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == VERTICAL_TAB);

    let mut iter = input.chars().enumerate().peekable();

    iter.next_if(|(_, ch)| matches!(ch, '+' | '-'));
    let Some((end, _)) = iter.take_while(|(_, ch)| ch.is_ascii_digit()).last() else {
        return Ok(0);
    };

    input[0..=end]
        .parse::<i64>()
        .map_err(|err| match err.kind() {
            std::num::IntErrorKind::PosOverflow => IntOverflow::Positive,
            std::num::IntErrorKind::NegOverflow => IntOverflow::Negative,
            // Empty/InvalidDigit are ruled out: the early `return Ok(0)` above
            // fires when there are no digits, and the slice spans only an
            // optional sign followed by ASCII digits. Zero is produced only by
            // `NonZeroI*::from_str`, not by `i64::from_str`. `IntErrorKind` is
            // `#[non_exhaustive]`, hence the catch-all.
            _ => unreachable!("unexpected IntErrorKind from i64::from_str: {err:?}"),
        })
}

#[derive(Debug, Clone, Copy)]
pub enum StrToF64 {
    Fractional(NonNan),
    Decimal(NonNan),
    FractionalPrefix(NonNan),
    DecimalPrefix(NonNan),
}

impl From<StrToF64> for f64 {
    fn from(value: StrToF64) -> Self {
        match value {
            StrToF64::Fractional(non_nan) => non_nan.into(),
            StrToF64::Decimal(non_nan) => non_nan.into(),
            StrToF64::FractionalPrefix(non_nan) => non_nan.into(),
            StrToF64::DecimalPrefix(non_nan) => non_nan.into(),
        }
    }
}

pub fn str_to_f64(input: impl AsRef<str>) -> Option<StrToF64> {
    // Every character that the parser looks for is ASCII, so it reads bytes.
    // A byte of a character that is not ASCII matches nothing, as the
    // character does, so the parser stops at the same place.
    let input = input
        .as_ref()
        .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == VERTICAL_TAB)
        .as_bytes();
    let digit_at = |pos: usize| match input.get(pos) {
        Some(&byte @ b'0'..=b'9') => Some(byte - b'0'),
        _ => None,
    };
    let mut pos = 0;

    let sign = match input.first() {
        Some(b'-') => {
            pos += 1;
            -1.0
        }
        Some(b'+') => {
            pos += 1;
            1.0
        }
        _ => 1.0,
    };

    let mut had_digits = false;
    let mut is_fractional = false;

    let mut significant: u64 = 0;

    // Copy as many significant digits as we can
    while let Some(digit) = digit_at(pos) {
        had_digits = true;

        match significant
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(digit)))
        {
            Some(new) => significant = new,
            None => break,
        }

        pos += 1;
    }

    let mut exponent = 0;

    // Increment the exponent for every non significant digit we skipped
    while digit_at(pos).is_some() {
        exponent += 1;
        pos += 1;
    }

    if input.get(pos) == Some(&b'.') {
        pos += 1;

        if had_digits {
            is_fractional = true;
        }

        if digit_at(pos).is_some() {
            is_fractional = true;
        }

        while let Some(digit) = digit_at(pos) {
            if significant < (u64::MAX - 9) / 10 {
                significant = significant * 10 + u64::from(digit);
                exponent -= 1;
            }

            pos += 1;
        }
    };

    let mut valid_exponent = true;

    if (had_digits || is_fractional) && matches!(input.get(pos), Some(b'e' | b'E')) {
        pos += 1;
        let sign = match input.get(pos) {
            Some(b'-') => {
                pos += 1;
                -1
            }
            Some(b'+') => {
                pos += 1;
                1
            }
            _ => 1,
        };

        if digit_at(pos).is_some() {
            is_fractional = true;
            let mut e = 0;

            while let Some(digit) = digit_at(pos) {
                e = (e * 10 + i32::from(digit)).min(1000);
                pos += 1;
            }

            exponent += sign * e;
        } else {
            valid_exponent = false;
        }
    };

    if !(had_digits || is_fractional) {
        return None;
    }

    while exponent.is_positive() && significant < MAX_EXACT / 10 {
        significant *= 10;
        exponent -= 1;
    }

    while exponent.is_negative() && significant % 10 == 0 {
        significant /= 10;
        exponent += 1;
    }

    let mut result = DoubleDouble::from(significant);

    if exponent > 0 {
        while exponent >= 100 {
            exponent -= 100;
            result *= DoubleDouble::E100;
        }
        while exponent >= 10 {
            exponent -= 10;
            result *= DoubleDouble::E10;
        }
        while exponent >= 1 {
            exponent -= 1;
            result *= DoubleDouble::E1;
        }
    } else {
        while exponent <= -100 {
            exponent += 100;
            result *= DoubleDouble::NEG_E100;
        }
        while exponent <= -10 {
            exponent += 10;
            result *= DoubleDouble::NEG_E10;
        }
        while exponent <= -1 {
            exponent += 1;
            result *= DoubleDouble::NEG_E1;
        }
    }

    let product = f64::from(result) * sign;
    let result = NonNan::new(if product.is_nan() {
        sign * f64::INFINITY
    } else {
        product
    })
    .unwrap();

    if !valid_exponent || pos < input.len() {
        if is_fractional {
            return Some(StrToF64::FractionalPrefix(result));
        } else {
            return Some(StrToF64::DecimalPrefix(result));
        }
    }

    Some(if is_fractional {
        StrToF64::Fractional(result)
    } else {
        StrToF64::Decimal(result)
    })
}

pub fn round_half_away_from_zero_tie(f: f64, precision: usize) -> Option<f64> {
    use num_traits::Float;

    let (m, k, sign): (u64, i16, i8) = f.integer_decode();
    let k = k as i32;

    let precision_i32 = precision as i32;
    let q = -(k + precision_i32);

    const MAX_TIE_PRODUCT_BITS: i32 = 127;
    if !(1..=MAX_TIE_PRODUCT_BITS).contains(&q) {
        return None;
    }

    let precision_u32 = precision as u32;
    let five_pow = 5u128.pow(precision_u32);
    let scaled = (m as u128) * five_pow;

    let low_q_bits = scaled & ((1u128 << q) - 1);
    let is_tie = low_q_bits == (1u128 << (q - 1));
    if !is_tie {
        return None;
    }

    let rounded_int = (scaled >> q) + 1;

    let ten_pow = 10u128.pow(precision_u32);
    let int_part = rounded_int / ten_pow;
    let frac_part = rounded_int % ten_pow;
    let sign_str = if sign.is_negative() { "-" } else { "" };
    let s = format!("{sign_str}{int_part}.{frac_part:0precision$}");

    let result: f64 = str_to_f64(s)
        .expect("constructed digit string should always parse")
        .into();
    Some(result)
}

enum FloatParts {
    Special(String),
    Normal {
        negative: bool,
        digits: DecimalDigits,
        exp: i32,
    },
}

fn decompose_float(v: f64, precision: usize) -> FloatParts {
    if v.is_nan() {
        return FloatParts::Special("NaN".to_string());
    }

    if v.is_infinite() {
        return FloatParts::Special(if v.is_sign_negative() { "-Inf" } else { "Inf" }.to_string());
    }

    if v == 0.0 {
        return FloatParts::Special("0.0".to_string());
    }

    let negative = v < 0.0;
    let mut d = DoubleDouble(v.abs(), 0.0);
    let mut exp = 0;

    if d.0 > 9.223_372_036_854_775e18 {
        while d.0 > 9.223_372_036_854_774e118 {
            exp += 100;
            d *= DoubleDouble::NEG_E100;
        }
        while d.0 > 9.223_372_036_854_774e28 {
            exp += 10;
            d *= DoubleDouble::NEG_E10;
        }
        while d.0 > 9.223_372_036_854_775e18 {
            exp += 1;
            d *= DoubleDouble::NEG_E1;
        }
    } else {
        while d.0 < 9.223_372_036_854_775e-83 {
            exp -= 100;
            d *= DoubleDouble::E100;
        }
        while d.0 < 9.223_372_036_854_775e7 {
            exp -= 10;
            d *= DoubleDouble::E10;
        }
        while d.0 < 9.223_372_036_854_775e17 {
            exp -= 1;
            d *= DoubleDouble::E1;
        }
    }

    let mut digits = DecimalDigits::new(u64::from(d));
    let mut decimal_pos = digits.len() as i32 + exp;

    'out: {
        if digits.len() > precision {
            let round_up = digits[precision] >= b'5';
            digits.truncate(precision);

            if round_up {
                for i in (0..precision).rev() {
                    if digits[i] < b'9' {
                        digits[i] += 1;
                        break 'out;
                    }
                    digits[i] = b'0';
                }

                digits.push_carry(b'1');
                decimal_pos += 1;
            }
        }
    }

    while digits.len() > 1 && digits[digits.len() - 1] == b'0' {
        digits.pop();
    }

    FloatParts::Normal {
        negative,
        digits,
        exp: decimal_pos - 1,
    }
}

/// The decimal digits of a float as ASCII bytes, in `buf[start..end]`. A
/// `u64` has at most 20 digits, which go after the first byte of `buf`. The
/// first byte stays free for the carry digit of a rounding.
struct DecimalDigits {
    buf: [u8; 21],
    start: usize,
    end: usize,
}

impl DecimalDigits {
    /// The decimal digits of `n`, with no leading zeros.
    fn new(n: u64) -> Self {
        let mut buf = [0u8; 21];
        let after_carry: &mut [u8; 20] = (&mut buf[1..])
            .try_into()
            .expect("the buffer has 20 bytes after the carry byte");
        let len = write_decimal_digits(after_carry, n).len();
        Self {
            buf,
            start: 21 - len,
            end: 21,
        }
    }

    fn truncate(&mut self, len: usize) {
        self.end = self.end.min(self.start + len);
    }

    /// Put `digit` before the other digits.
    fn push_carry(&mut self, digit: u8) {
        turso_debug_assert!(self.start > 0, "a rounding adds only one carry digit");
        self.start -= 1;
        self.buf[self.start] = digit;
    }

    fn pop(&mut self) {
        turso_debug_assert!(self.end > self.start);
        self.end -= 1;
    }
}

impl std::ops::Deref for DecimalDigits {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }
}

impl std::ops::DerefMut for DecimalDigits {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.start..self.end]
    }
}

fn format_float_scientific(v: f64, precision: usize) -> String {
    match decompose_float(v, precision) {
        FloatParts::Special(s) => s,
        FloatParts::Normal {
            negative,
            digits,
            exp,
        } => format_scientific(negative, &digits, exp),
    }
}

pub fn format_float(v: f64) -> String {
    match decompose_float(v, 15) {
        FloatParts::Special(s) => s,
        FloatParts::Normal {
            negative,
            digits,
            exp,
        } => {
            if (-4..=14).contains(&exp) {
                format_decimal(negative, &digits, exp)
            } else {
                format_scientific(negative, &digits, exp)
            }
        }
    }
}

/// Format `0.d1d2d3... * 10^(exp + 1)`, where `digits` are d1, d2, d3 and so
/// on, as `[-]i.f`. Both `i` and `f` have at least one digit.
fn format_decimal(negative: bool, digits: &[u8], exp: i32) -> String {
    let decimal_pos = exp + 1;
    let mut out = String::with_capacity(digits.len() + 24);
    if negative {
        out.push('-');
    }
    if decimal_pos > 0 {
        let int_len = (decimal_pos as usize).min(digits.len());
        push_ascii_digits(&mut out, &digits[..int_len]);
        for _ in int_len..decimal_pos as usize {
            out.push('0');
        }
    } else {
        out.push('0');
    }
    out.push('.');
    for _ in 0..decimal_pos.min(0).unsigned_abs() {
        out.push('0');
    }
    match digits.get(decimal_pos.max(0) as usize..) {
        Some(frac) if !frac.is_empty() => push_ascii_digits(&mut out, frac),
        _ => out.push('0'),
    }
    out
}

/// Format the same value as [format_decimal] as `[-]d.ddde[+-]xx`. The
/// exponent has at least two digits, and three when it is more than 99.
fn format_scientific(negative: bool, digits: &[u8], exp: i32) -> String {
    let mut out = String::with_capacity(digits.len() + 10);
    if negative {
        out.push('-');
    }
    out.push(digits.first().copied().unwrap_or(b'0') as char);
    out.push('.');
    match digits.get(1..) {
        Some(rest) if !rest.is_empty() => push_ascii_digits(&mut out, rest),
        _ => out.push('0'),
    }
    out.push('e');
    out.push(if exp >= 0 { '+' } else { '-' });
    let mut buf = [0u8; 20];
    let exp_digits = write_decimal_digits(&mut buf, u64::from(exp.unsigned_abs()));
    let width = if exp.unsigned_abs() > 99 { 3 } else { 2 };
    for _ in exp_digits.len()..width {
        out.push('0');
    }
    push_ascii_digits(&mut out, exp_digits);
    out
}

fn push_ascii_digits(out: &mut String, digits: &[u8]) {
    // SAFETY: `digits` holds only the ASCII bytes b'0'..=b'9'.
    out.push_str(unsafe { str::from_utf8_unchecked(digits) });
}

/// The decimal text of an `i64`, the same text as `i64::to_string()`, in a
/// stack buffer.
pub(crate) struct IntegerText {
    buf: [u8; 20],
    start: usize,
}

impl IntegerText {
    // Not inlined, so that its callers, such as the operands of `||`, stay
    // small enough for the compiler to inline them.
    #[inline(never)]
    pub(crate) fn new(value: i64) -> Self {
        let mut buf = [0u8; 20];
        let digits = write_decimal_digits(&mut buf, value.unsigned_abs()).len();
        let mut start = buf.len() - digits;
        if value < 0 {
            start -= 1;
            buf[start] = b'-';
        }
        Self { buf, start }
    }

    pub(crate) fn as_str(&self) -> &str {
        // SAFETY: the text is ASCII digits, after a minus sign when negative.
        unsafe { str::from_utf8_unchecked(&self.buf[self.start..]) }
    }
}

/// Write the decimal digits of `n` at the end of `buf`, and return them.
// Not inlined: in a caller that owns `buf`, the compiler unrolls the loop for
// all 20 digits and keeps each byte of `buf` in a register. That makes the
// caller several kilobytes larger, and each byte of the result is moved one at
// a time.
#[inline(never)]
pub(crate) fn write_decimal_digits(buf: &mut [u8; 20], mut n: u64) -> &[u8] {
    let mut start = buf.len();
    // Two digits for each division, from a table of the pairs.
    while n >= 100 {
        let pair = usize::from((n % 100) as u8) * 2;
        n /= 100;
        start -= 2;
        buf[start..start + 2].copy_from_slice(&DIGIT_PAIRS.as_bytes()[pair..pair + 2]);
    }
    if n >= 10 {
        let pair = usize::from(n as u8) * 2;
        start -= 2;
        buf[start..start + 2].copy_from_slice(&DIGIT_PAIRS.as_bytes()[pair..pair + 2]);
    } else {
        start -= 1;
        buf[start] = b'0' + n as u8;
    }
    &buf[start..]
}

/// The numbers from 0 to 99, each as two ASCII digits.
pub(crate) const DIGIT_PAIRS: &str = "\
    0001020304050607080910111213141516171819\
    2021222324252627282930313233343536373839\
    4041424344454647484950515253545556575859\
    6061626364656667686970717273747576777879\
    8081828384858687888990919293949596979899";

pub fn format_float_for_quote(v: f64) -> String {
    if v.is_nan() {
        return "NULL".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_negative() {
            "-9.0e+999".to_string()
        } else {
            "9.0e+999".to_string()
        };
    }
    let default = format_float(v);
    if str_to_f64(&default).map(f64::from) == Some(v) {
        return default;
    }
    format_float_scientific(v, 19)
}

#[test]
fn test_decode_float() {
    assert_eq!(format_float(9.93e-322), "9.93071948140905e-322");
    assert_eq!(format_float(9.93), "9.93");
    assert_eq!(format_float(0.093), "0.093");
    assert_eq!(format_float(-0.093), "-0.093");
    assert_eq!(format_float(0.0), "0.0");
    assert_eq!(format_float(4.94e-322), "4.94065645841247e-322");
    assert_eq!(format_float(-20228007.0), "-20228007.0");
    // The rounding carries into a new leading digit.
    assert_eq!(format_float(9.999999999999998), "10.0");
}

#[test]
fn integer_text_matches_to_string() {
    for value in (-100_000i64..100_000).chain([
        i64::MIN,
        i64::MIN + 1,
        i64::MAX,
        i64::MAX - 1,
        i64::from(i32::MIN),
        i64::from(u32::MAX),
        10_i64.pow(18),
        -(10_i64.pow(18)),
    ]) {
        assert_eq!(IntegerText::new(value).as_str(), value.to_string());
    }
}

#[test]
fn test_format_float_for_quote() {
    assert_eq!(format_float_for_quote(f64::INFINITY), "9.0e+999");
    assert_eq!(format_float_for_quote(f64::NEG_INFINITY), "-9.0e+999");
    assert_eq!(format_float_for_quote(f64::NAN), "NULL");
    assert_eq!(format_float_for_quote(12.34), "12.34");
    assert_eq!(format_float_for_quote(0.0), "0.0");
}

#[test]
fn test_round_half_away_from_zero_tie() {
    assert_eq!(round_half_away_from_zero_tie(2.25, 1), Some(2.3));
    assert_eq!(round_half_away_from_zero_tie(0.125, 2), Some(0.13));
    assert_eq!(round_half_away_from_zero_tie(1.125, 2), Some(1.13));
    assert_eq!(round_half_away_from_zero_tie(-2.25, 1), Some(-2.3));
    assert_eq!(round_half_away_from_zero_tie(2.35, 1), None);
    assert_eq!(round_half_away_from_zero_tie(1.005, 2), None);
    assert_eq!(round_half_away_from_zero_tie(5e-320, 10), None);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_to_f64_reads_the_number_at_the_start_of_the_text() {
        let parts = |text: &str| {
            str_to_f64(text).map(|result| match result {
                StrToF64::Decimal(v) => ("Decimal", f64::from(v)),
                StrToF64::Fractional(v) => ("Fractional", f64::from(v)),
                StrToF64::DecimalPrefix(v) => ("DecimalPrefix", f64::from(v)),
                StrToF64::FractionalPrefix(v) => ("FractionalPrefix", f64::from(v)),
            })
        };
        let cases = [
            ("12", Some(("Decimal", 12.0))),
            (" +12.5e+3 ", Some(("Fractional", 12500.0))),
            ("-0.25", Some(("Fractional", -0.25))),
            ("12.5e", Some(("FractionalPrefix", 12.5))),
            ("12.5e+", Some(("FractionalPrefix", 12.5))),
            ("1\u{e9}", Some(("DecimalPrefix", 1.0))),
            ("\u{e9}1", None),
            (".e1", None),
            ("1.e1", Some(("Fractional", 10.0))),
            ("0x10", Some(("DecimalPrefix", 0.0))),
            (
                "18446744073709551616",
                Some(("Decimal", 1.8446744073709552e19)),
            ),
            ("1e400", Some(("Fractional", f64::INFINITY))),
            ("\u{b}7\u{b}", Some(("Decimal", 7.0))),
            ("", None),
            ("-", None),
            (".", None),
        ];
        for (text, expected) in cases {
            assert_eq!(parts(text), expected, "{text:?}");
        }
    }
}
