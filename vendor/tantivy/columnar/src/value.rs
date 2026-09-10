use std::str::FromStr;

use common::DateTime;

use crate::InvalidData;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum NumericalValue {
    I64(i64),
    U64(u64),
    F64(f64),
}

impl FromStr for NumericalValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        if let Ok(val_i64) = s.parse::<i64>() {
            return Ok(val_i64.into());
        }
        if let Ok(val_u64) = s.parse::<u64>() {
            return Ok(val_u64.into());
        }
        if let Ok(val_f64) = s.parse::<f64>() {
            return Ok(NumericalValue::from(val_f64).normalize());
        }
        Err(())
    }
}

impl NumericalValue {
    pub fn numerical_type(&self) -> NumericalType {
        match self {
            NumericalValue::I64(_) => NumericalType::I64,
            NumericalValue::U64(_) => NumericalType::U64,
            NumericalValue::F64(_) => NumericalType::F64,
        }
    }

    /// Tries to normalize the numerical value in the following priorities:
    /// i64, i64, f64
    pub fn normalize(self) -> Self {
        match self {
            NumericalValue::U64(val) => {
                if val <= i64::MAX as u64 {
                    NumericalValue::I64(val as i64)
                } else {
                    NumericalValue::U64(val)
                }
            }
            NumericalValue::I64(val) => NumericalValue::I64(val),
            NumericalValue::F64(val) => {
                let fract = val.fract();
                if fract == 0.0 && val >= i64::MIN as f64 && val <= i64::MAX as f64 {
                    NumericalValue::I64(val as i64)
                } else if fract == 0.0 && val >= u64::MIN as f64 && val <= u64::MAX as f64 {
                    NumericalValue::U64(val as u64)
                } else {
                    NumericalValue::F64(val)
                }
            }
        }
    }
}

impl From<u64> for NumericalValue {
    fn from(val: u64) -> NumericalValue {
        NumericalValue::U64(val)
    }
}

impl From<i64> for NumericalValue {
    fn from(val: i64) -> Self {
        NumericalValue::I64(val)
    }
}

impl From<f64> for NumericalValue {
    fn from(val: f64) -> Self {
        NumericalValue::F64(val)
    }
}

#[derive(Clone, Copy, Debug, Default, Hash, Eq, PartialEq)]
#[repr(u8)]
pub enum NumericalType {
    #[default]
    I64 = 0,
    U64 = 1,
    F64 = 2,
}

impl NumericalType {
    pub fn to_code(self) -> u8 {
        self as u8
    }

    pub fn try_from_code(code: u8) -> Result<NumericalType, InvalidData> {
        match code {
            0 => Ok(NumericalType::I64),
            1 => Ok(NumericalType::U64),
            2 => Ok(NumericalType::F64),
            _ => Err(InvalidData),
        }
    }
}

/// We voluntarily avoid using `Into` here to keep this
/// implementation quirk as private as possible.
///
/// # Panics
/// This coercion trait actually panics if it is used
/// to convert a loose types to a stricter type.
///
/// The level is strictness is somewhat arbitrary.
/// - i64
/// - u64
/// - f64.
pub(crate) trait Coerce {
    fn coerce(numerical_value: NumericalValue) -> Self;
}

impl Coerce for i64 {
    fn coerce(value: NumericalValue) -> Self {
        match value {
            NumericalValue::I64(val) => val,
            NumericalValue::U64(val) => val as i64,
            NumericalValue::F64(_) => unreachable!(),
        }
    }
}

impl Coerce for u64 {
    fn coerce(value: NumericalValue) -> Self {
        match value {
            NumericalValue::I64(val) => val as u64,
            NumericalValue::U64(val) => val,
            NumericalValue::F64(_) => unreachable!(),
        }
    }
}

impl Coerce for f64 {
    fn coerce(value: NumericalValue) -> Self {
        match value {
            NumericalValue::I64(val) => val as f64,
            NumericalValue::U64(val) => val as f64,
            NumericalValue::F64(val) => val,
        }
    }
}

impl Coerce for DateTime {
    fn coerce(value: NumericalValue) -> Self {
        let timestamp_micros = i64::coerce(value);
        DateTime::from_timestamp_nanos(timestamp_micros)
    }
}

#[cfg(test)]
mod tests {
    use super::NumericalType;
    use crate::NumericalValue;

    #[test]
    fn test_numerical_type_code() {
        let mut num_numerical_type = 0;
        for code in u8::MIN..=u8::MAX {
            if let Ok(numerical_type) = NumericalType::try_from_code(code) {
                assert_eq!(numerical_type.to_code(), code);
                num_numerical_type += 1;
            }
        }
        assert_eq!(num_numerical_type, 3);
    }

    #[test]
    fn test_parse_numerical() {
        assert_eq!(
            "123".parse::<NumericalValue>().unwrap(),
            NumericalValue::I64(123)
        );
        assert_eq!(
            "18446744073709551615".parse::<NumericalValue>().unwrap(),
            NumericalValue::U64(18446744073709551615u64)
        );
        assert_eq!(
            "1.0".parse::<NumericalValue>().unwrap(),
            NumericalValue::I64(1i64)
        );
        assert_eq!(
            "1.1".parse::<NumericalValue>().unwrap(),
            NumericalValue::F64(1.1f64)
        );
        assert_eq!(
            "-1.0".parse::<NumericalValue>().unwrap(),
            NumericalValue::I64(-1i64)
        );
    }

    #[test]
    fn test_normalize_numerical() {
        assert_eq!(
            NumericalValue::from(1u64).normalize(),
            NumericalValue::I64(1i64),
        );
        let limit_val = i64::MAX as u64 + 1u64;
        assert_eq!(
            NumericalValue::from(limit_val).normalize(),
            NumericalValue::U64(limit_val),
        );
        assert_eq!(
            NumericalValue::from(-1i64).normalize(),
            NumericalValue::I64(-1i64),
        );
        assert_eq!(
            NumericalValue::from(-2.0f64).normalize(),
            NumericalValue::I64(-2i64),
        );
        assert_eq!(
            NumericalValue::from(-2.1f64).normalize(),
            NumericalValue::F64(-2.1f64),
        );
        let large_float = 2.0f64.powf(70.0f64);
        assert_eq!(
            NumericalValue::from(large_float).normalize(),
            NumericalValue::F64(large_float),
        );
    }
}
