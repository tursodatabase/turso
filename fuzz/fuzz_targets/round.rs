#![no_main]

use std::cell::RefCell;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rusqlite::{params, types::Value as SqlValue};
use turso_core::Value as CoreValue;

#[derive(Debug, Clone)]
enum FuzzValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl FuzzValue {
    fn to_core_value(&self) -> CoreValue {
        match self {
            Self::Null => CoreValue::Null,
            Self::Integer(v) => CoreValue::from_i64(*v),
            Self::Real(v) => CoreValue::from_f64(*v),
            Self::Text(v) => CoreValue::from_text(v.clone()),
            Self::Blob(v) => CoreValue::from_blob(v.clone()),
        }
    }

    fn to_sql_value(&self) -> SqlValue {
        match self {
            Self::Null => SqlValue::Null,
            Self::Integer(v) => SqlValue::Integer(*v),
            Self::Real(v) => {
                if v.is_nan() {
                    SqlValue::Null
                } else {
                    SqlValue::Real(*v)
                }
            }
            Self::Text(v) => SqlValue::Text(v.clone()),
            Self::Blob(v) => SqlValue::Blob(v.clone()),
        }
    }

    fn arbitrary_value(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        Ok(match u.int_in_range(0..=4u8)? {
            0 => Self::Null,
            1 => Self::Integer(i64::arbitrary(u)?),
            2 => Self::Real(f64::arbitrary(u)?),
            3 => Self::Text(Self::arbitrary_text(u)?),
            4 => Self::Blob(Vec::<u8>::arbitrary(u)?),
            _ => unreachable!(),
        })
    }

    fn arbitrary_text(u: &mut Unstructured<'_>) -> arbitrary::Result<String> {
        Ok(match u.int_in_range(0..=6u8)? {
            0 => i64::arbitrary(u)?.to_string(),
            1 => {
                let f = f64::arbitrary(u)?;
                if f.is_nan() {
                    "NaN".to_string()
                } else {
                    f.to_string()
                }
            }
            2 => format!(" {} ", u.int_in_range(-40i16..=40i16)?),
            3 => format!("{}.5", u.int_in_range(-10_000i32..=10_000i32)?),
            4 => String::new(),
            5 => "not-a-number".to_string(),
            6 => String::arbitrary(u)?,
            _ => unreachable!(),
        })
    }
}

#[derive(Debug, Clone)]
struct RoundCase {
    value: FuzzValue,
    precision: Option<FuzzValue>,
}

impl RoundCase {
    fn exact_tie(u: &mut Unstructured<'_>, text_value: bool) -> arbitrary::Result<Self> {
        let precision = u.int_in_range(1..=30u8)? as usize;
        let odd = (u.int_in_range(0..=4095u16)? as u64) * 2 + 1;
        let sign = if bool::arbitrary(u)? { -1.0 } else { 1.0 };
        // For precision p, odd / 2^(p+1) is an exact midpoint once scaled by 10^p.
        let tie = sign * (odd as f64) / ((1u64 << (precision as u32 + 1)) as f64);
        let value = if text_value {
            FuzzValue::Text(tie.to_string())
        } else {
            FuzzValue::Real(tie)
        };
        let precision = if bool::arbitrary(u)? {
            Some(FuzzValue::Integer(precision as i64))
        } else {
            Some(FuzzValue::Text(precision.to_string()))
        };
        Ok(Self { value, precision })
    }

    fn random_numeric(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let value = match u.int_in_range(0..=2u8)? {
            0 => FuzzValue::Real(f64::arbitrary(u)?),
            1 => FuzzValue::Integer(i64::arbitrary(u)?),
            2 => FuzzValue::Text(FuzzValue::arbitrary_text(u)?),
            _ => unreachable!(),
        };
        let precision = match u.int_in_range(0..=5u8)? {
            0 => None,
            1 => Some(FuzzValue::Null),
            2 => Some(FuzzValue::Integer(u.int_in_range(-100i16..=100i16)? as i64)),
            3 => Some(FuzzValue::Real(f64::arbitrary(u)?)),
            4 => Some(FuzzValue::Text(FuzzValue::arbitrary_text(u)?)),
            5 => Some(FuzzValue::Blob(Vec::<u8>::arbitrary(u)?)),
            _ => unreachable!(),
        };
        Ok(Self { value, precision })
    }

    fn coercion_heavy(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let precision = match u.int_in_range(0..=4u8)? {
            0 => None,
            1 => Some(FuzzValue::Text(FuzzValue::arbitrary_text(u)?)),
            2 => Some(FuzzValue::Blob(Vec::<u8>::arbitrary(u)?)),
            3 => Some(FuzzValue::Null),
            4 => Some(FuzzValue::Real(f64::arbitrary(u)?)),
            _ => unreachable!(),
        };
        Ok(Self {
            value: FuzzValue::arbitrary_value(u)?,
            precision,
        })
    }
}

impl<'a> Arbitrary<'a> for RoundCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        match u.int_in_range(0..=9u8)? {
            0..=4 => Self::exact_tie(u, false),
            5..=6 => Self::exact_tie(u, true),
            7..=8 => Self::random_numeric(u),
            9 => Self::coercion_heavy(u),
            _ => unreachable!(),
        }
    }
}

thread_local! {
    static SQLITE: RefCell<rusqlite::Connection> = RefCell::new(
        rusqlite::Connection::open_in_memory().expect("sqlite fuzz oracle should open")
    );
}

fn sqlite_round(case: &RoundCase) -> rusqlite::Result<CoreValue> {
    SQLITE.with(|conn| {
        let conn = conn.borrow();
        let value = match case.precision.as_ref() {
            Some(precision) => conn.query_row(
                "SELECT round(?1, ?2)",
                params![case.value.to_sql_value(), precision.to_sql_value()],
                |row| row.get::<_, SqlValue>(0),
            )?,
            None => conn.query_row(
                "SELECT round(?1)",
                params![case.value.to_sql_value()],
                |row| row.get::<_, SqlValue>(0),
            )?,
        };
        Ok(match value {
            SqlValue::Null => CoreValue::Null,
            SqlValue::Integer(v) => CoreValue::from_f64(v as f64),
            SqlValue::Real(v) => CoreValue::from_f64(v),
            SqlValue::Text(v) => CoreValue::from_text(v),
            SqlValue::Blob(v) => CoreValue::from_blob(v),
        })
    })
}

fuzz_target!(|case: RoundCase| {
    let value = case.value.to_core_value();
    let precision = case.precision.as_ref().map(FuzzValue::to_core_value);
    let expected = sqlite_round(&case).expect("sqlite round should succeed");
    let found = value.exec_round(precision.as_ref());
    assert_eq!(expected, found, "round case: {case:?}");
});
