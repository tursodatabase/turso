use crate::{types::AsValueRef, Value, ValueRef};

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
                    Some(StrToF64::Decimal(real)) => {
                        let integer = str_to_i64(s).unwrap_or(0);

                        Some(if real == integer as f64 {
                            Self::Integer(integer)
                        } else {
                            Self::Float(real)
                        })
                    }
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
                let integer = str_to_i64(text).unwrap_or(0);

                if real == integer as f64 {
                    Self::Integer(integer)
                } else {
                    Self::Float(real)
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
            (Numeric::Float(a), Numeric::Float(b)) => {
                let fa: f64 = (*a).into();
                let fb: f64 = (*b).into();
                // NonNan guarantees no NaN, so partial_cmp always returns Some.
                // SQLite's float-vs-float uses raw IEEE 754 < and > operators,
                // which both return false for NaN, resulting in "equal".
                fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
            }
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
/// SQLite treats NaN as NULL, so int > NaN. In practice, NonNan prevents NaN
/// from appearing in Numeric::Float, but we check defensively.
///
/// See sqlite3IntFloatCompare in src/vdbeaux.c.
fn sqlite_int_float_cmp(int_val: i64, float_val: f64) -> std::cmp::Ordering {
    if float_val.is_nan() {
        // NaN is treated as NULL; all integers are greater than NULL
        return std::cmp::Ordering::Greater;
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
    let input = input
        .as_ref()
        .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == VERTICAL_TAB);

    let mut iter = input.chars().enumerate().peekable();

    iter.next_if(|(_, ch)| matches!(ch, '+' | '-'));
    let Some((end, _)) = iter.take_while(|(_, ch)| ch.is_ascii_digit()).last() else {
        return Some(0);
    };

    input[0..=end].parse::<i64>().map_or_else(
        |err| match err.kind() {
            std::num::IntErrorKind::PosOverflow => Some(i64::MAX),
            std::num::IntErrorKind::NegOverflow => Some(i64::MIN),
            std::num::IntErrorKind::Empty => unreachable!(),
            _ => Some(0),
        },
        Some,
    )
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
    let mut input = input
        .as_ref()
        .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == VERTICAL_TAB)
        .chars()
        .peekable();

    let sign = match input.next_if(|ch| matches!(ch, '-' | '+')) {
        Some('-') => -1.0,
        _ => 1.0,
    };

    let mut had_digits = false;
    let mut is_fractional = false;

    let mut significant: u64 = 0;

    // Copy as many significant digits as we can
    while let Some(digit) = input.peek().and_then(|ch| ch.to_digit(10)) {
        had_digits = true;

        match significant
            .checked_mul(10)
            .and_then(|v| v.checked_add(digit as u64))
        {
            Some(new) => significant = new,
            None => break,
        }

        input.next();
    }

    let mut exponent = 0;

    // Increment the exponent for every non significant digit we skipped
    while input.next_if(char::is_ascii_digit).is_some() {
        exponent += 1
    }

    if input.next_if(|ch| matches!(ch, '.')).is_some() {
        if had_digits {
            is_fractional = true;
        }

        if input.peek().is_some_and(char::is_ascii_digit) {
            is_fractional = true;
        }

        while let Some(digit) = input.peek().and_then(|ch| ch.to_digit(10)) {
            if significant < (u64::MAX - 9) / 10 {
                significant = significant * 10 + digit as u64;
                exponent -= 1;
            }

            input.next();
        }
    };

    let mut valid_exponent = true;

    if (had_digits || is_fractional) && input.next_if(|ch| matches!(ch, 'e' | 'E')).is_some() {
        let sign = match input.next_if(|ch| matches!(ch, '-' | '+')) {
            Some('-') => -1,
            _ => 1,
        };

        if input.peek().is_some_and(char::is_ascii_digit) {
            is_fractional = true;
            let mut e = 0;

            while let Some(ch) = input.next_if(char::is_ascii_digit) {
                e = (e * 10 + ch.to_digit(10).unwrap() as i32).min(1000);
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

    let result = NonNan::new(f64::from(result) * sign)
        .unwrap_or_else(|| NonNan::new(sign * f64::INFINITY).unwrap());

    if !valid_exponent || input.count() > 0 {
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

pub fn format_float(v: f64) -> String {
    if v.is_nan() {
        return "".to_string();
    }

    if v.is_infinite() {
        return if v.is_sign_negative() { "-Inf" } else { "Inf" }.to_string();
    }

    if v == 0.0 {
        return "0.0".to_string();
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

    let v = u64::from(d);

    let mut digits = v.to_string().into_bytes();

    let precision = 15;

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

                digits.insert(0, b'1');
                decimal_pos += 1;
            }
        }
    }

    while digits.len() > 1 && digits[digits.len() - 1] == b'0' {
        digits.pop();
    }

    let exp = decimal_pos - 1;

    if (-4..=14).contains(&exp) {
        format!(
            "{}{}.{}{}",
            if negative { "-" } else { Default::default() },
            if decimal_pos > 0 {
                let zeroes = (decimal_pos - digits.len() as i32).max(0) as usize;
                let digits = digits
                    .get(0..(decimal_pos.min(digits.len() as i32) as usize))
                    .unwrap();
                (unsafe { str::from_utf8_unchecked(digits) }).to_owned() + &"0".repeat(zeroes)
            } else {
                "0".to_string()
            },
            "0".repeat(decimal_pos.min(0).unsigned_abs() as usize),
            digits
                .get((decimal_pos.max(0) as usize)..)
                .filter(|v| !v.is_empty())
                .map(|v| unsafe { str::from_utf8_unchecked(v) })
                .unwrap_or("0")
        )
    } else {
        format!(
            "{}{}.{}e{}{:0width$}",
            if negative { "-" } else { "" },
            digits.first().cloned().unwrap_or(b'0') as char,
            digits
                .get(1..)
                .filter(|v| !v.is_empty())
                .map(|v| unsafe { str::from_utf8_unchecked(v) })
                .unwrap_or("0"),
            if exp.is_positive() { "+" } else { "-" },
            exp.abs(),
            width = if exp > 100 { 3 } else { 2 }
        )
    }
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
}
