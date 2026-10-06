use crate::functions::pg_types::{
    check_numeric_scale, format_temporal, numeric_type_parameter, parse_canonical_temporal,
    scale_decimal, PgTemporal, Remainder, ScaledDecimal, TemporalText, NUMERIC_STORED_LIMIT,
};
use crate::numeric::decimal::value_to_bigdecimal;
use crate::types::Value;
use crate::{turso_assert_eq, LimboError, Numeric, Result};

const UUID_TEXT_LEN: usize = 36;
const UUID_HYPHEN_POSITIONS: [usize; 4] = [8, 13, 18, 23];
// An integer sorts below every blob, and 17 bytes of 0xff sort above every 16-byte blob.
const KEY_BELOW_EVERY_UUID: i64 = 0;
const KEY_ABOVE_EVERY_UUID: [u8; 17] = [0xff; 17];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoSeekKey {
    Null = 0,
    BelowEveryValue = -1,
    AboveEveryValue = 1,
}

pub(crate) fn exec_uuid_seek_key(value: &Value, no_key: &Value) -> Result<Value> {
    if let Value::Text(text) = value {
        if let Some(bytes) = canonical_uuid_bytes(text.as_str().as_bytes()) {
            return Ok(Value::from_slice(&bytes)?);
        }
    }
    let Value::Numeric(Numeric::Integer(no_key)) = no_key else {
        unreachable!("the seek passes an integer for the missing key, got {no_key:?}");
    };
    Ok(match *no_key {
        n if n == NoSeekKey::BelowEveryValue as i64 => Value::from_i64(KEY_BELOW_EVERY_UUID),
        n if n == NoSeekKey::AboveEveryValue as i64 => Value::from_slice(&KEY_ABOVE_EVERY_UUID)?,
        n => {
            turso_assert_eq!(n, NoSeekKey::Null as i64);
            Value::Null
        }
    })
}

pub(crate) fn exec_pg_temporal_seek_key(
    kind: PgTemporal,
    operand: &Value,
    no_key: &Value,
) -> Result<Value> {
    let bound = seek_bound(no_key);
    let (first, last) = kind.stored_range();
    let key = match operand {
        Value::Null => None,
        Value::Text(text) => match parse_canonical_temporal(kind, text.as_str()) {
            Some(stored) => Some(stored),
            None => match bound {
                NoSeekKey::Null => None,
                NoSeekKey::BelowEveryValue => {
                    Some(last_stored_below(kind, text.as_str().as_bytes()))
                }
                NoSeekKey::AboveEveryValue => {
                    Some(first_stored_above(kind, text.as_str().as_bytes()))
                }
            },
        },
        Value::Numeric(_) => match bound {
            NoSeekKey::Null => None,
            NoSeekKey::BelowEveryValue => Some(i64::MIN),
            NoSeekKey::AboveEveryValue => Some(first),
        },
        Value::Blob(_) => match bound {
            NoSeekKey::Null => None,
            NoSeekKey::BelowEveryValue => Some(last),
            NoSeekKey::AboveEveryValue => Some(i64::MAX),
        },
    };
    Ok(key.map_or(Value::Null, Value::from_i64))
}

pub(crate) fn exec_pg_numeric_seek_key(
    operand: &Value,
    no_key: &Value,
    scale: &Value,
) -> Result<Value> {
    let scale = numeric_type_parameter("scale", scale)?;
    check_numeric_scale(scale)?;
    if matches!(operand, Value::Null) {
        return Ok(Value::Null);
    }
    let bound = seek_bound(no_key);
    let decimal = value_to_bigdecimal(operand)?;
    let above_every_value = match bound {
        NoSeekKey::Null => None,
        NoSeekKey::BelowEveryValue | NoSeekKey::AboveEveryValue => Some(i64::MAX),
    };
    let below_every_value = match bound {
        NoSeekKey::Null => None,
        NoSeekKey::BelowEveryValue | NoSeekKey::AboveEveryValue => Some(i64::MIN),
    };
    let key = match scale_decimal(&decimal, scale) {
        ScaledDecimal::Above => above_every_value,
        ScaledDecimal::Below => below_every_value,
        ScaledDecimal::InRange { truncated, .. } if truncated >= NUMERIC_STORED_LIMIT => {
            above_every_value
        }
        ScaledDecimal::InRange { truncated, .. } if truncated <= -NUMERIC_STORED_LIMIT => {
            below_every_value
        }
        ScaledDecimal::InRange {
            truncated,
            remainder: Remainder::None,
        } => Some(truncated as i64),
        ScaledDecimal::InRange { truncated, .. } => {
            let floor = truncated as i64 - i64::from(decimal.sign() == num_bigint::Sign::Minus);
            match bound {
                NoSeekKey::Null => None,
                NoSeekKey::BelowEveryValue => Some(floor),
                NoSeekKey::AboveEveryValue => Some(floor + 1),
            }
        }
    };
    Ok(key.map_or(Value::Null, Value::from_i64))
}

/// The key of an equality seek on the `numeric` type: the stored blob of the
/// operand when a value of the column can equal it, else NULL. Text that is
/// not a number raises the error of `numeric_eq`, so the error does not
/// depend on the plan.
pub(crate) fn exec_numeric_seek_key(
    operand: &Value,
    precision: &Value,
    scale: &Value,
) -> Result<Value> {
    let Value::Numeric(Numeric::Integer(precision)) = precision else {
        return Err(LimboError::Constraint(
            "numeric_encode: precision must be an integer".to_string(),
        ));
    };
    let Value::Numeric(Numeric::Integer(scale)) = scale else {
        return Err(LimboError::Constraint(
            "numeric_encode: scale must be an integer".to_string(),
        ));
    };
    if matches!(operand, Value::Null) {
        return Ok(Value::Null);
    }
    let decimal = value_to_bigdecimal(operand)?;
    if decimal.with_scale(*scale) != decimal {
        return Ok(Value::Null);
    }
    Ok(
        match crate::numeric::decimal::validate_precision_scale(&decimal, *precision, *scale) {
            Ok(stored) => Value::from_blob(crate::numeric::decimal::bigdecimal_to_blob(&stored)),
            Err(_) => Value::Null,
        },
    )
}

fn seek_bound(no_key: &Value) -> NoSeekKey {
    let Value::Numeric(Numeric::Integer(no_key)) = no_key else {
        unreachable!("the seek passes an integer for the missing key, got {no_key:?}");
    };
    match *no_key {
        n if n == NoSeekKey::BelowEveryValue as i64 => NoSeekKey::BelowEveryValue,
        n if n == NoSeekKey::AboveEveryValue as i64 => NoSeekKey::AboveEveryValue,
        n => {
            turso_assert_eq!(n, NoSeekKey::Null as i64);
            NoSeekKey::Null
        }
    }
}

fn last_stored_below(kind: PgTemporal, operand: &[u8]) -> i64 {
    let (first, last) = kind.stored_range();
    if decoded(kind, first).as_bytes() >= operand {
        return i64::MIN;
    }
    if decoded(kind, last).as_bytes() < operand {
        return last;
    }
    let (mut below, mut not_below) = (first, last);
    while not_below - below > 1 {
        let middle = below + (not_below - below) / 2;
        if decoded(kind, middle).as_bytes() < operand {
            below = middle;
        } else {
            not_below = middle;
        }
    }
    below
}

fn first_stored_above(kind: PgTemporal, operand: &[u8]) -> i64 {
    let (first, last) = kind.stored_range();
    if decoded(kind, last).as_bytes() <= operand {
        return i64::MAX;
    }
    if decoded(kind, first).as_bytes() > operand {
        return first;
    }
    let (mut not_above, mut above) = (first, last);
    while above - not_above > 1 {
        let middle = not_above + (above - not_above) / 2;
        if decoded(kind, middle).as_bytes() > operand {
            above = middle;
        } else {
            not_above = middle;
        }
    }
    above
}

fn decoded(kind: PgTemporal, stored: i64) -> TemporalText {
    format_temporal(kind, stored).expect("the search stays in the stored range")
}

fn canonical_uuid_bytes(text: &[u8]) -> Option<[u8; 16]> {
    if text.len() != UUID_TEXT_LEN {
        return None;
    }
    let mut bytes = [0u8; 16];
    let mut nibble = 0;
    for (position, &c) in text.iter().enumerate() {
        if UUID_HYPHEN_POSITIONS.contains(&position) {
            if c != b'-' {
                return None;
            }
            continue;
        }
        let digit = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            _ => return None,
        };
        let shift = if nibble % 2 == 0 { 4 } else { 0 };
        bytes[nibble / 2] |= digit << shift;
        nibble += 1;
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::cmp::Ordering;

    fn no_key(no_key: NoSeekKey) -> Value {
        Value::from_i64(no_key as i64)
    }

    fn uuid_text(bytes: &[u8; 16]) -> String {
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        )
    }

    fn compare_stored_with_key(stored: &[u8; 16], key: &Value) -> Ordering {
        match key {
            Value::Blob(key) => stored.as_slice().cmp(key.as_slice()),
            Value::Numeric(_) => Ordering::Greater,
            other => panic!("unexpected seek key {other:?}"),
        }
    }

    fn compare_decoded_with_operand(stored: &[u8; 16], operand: &Value) -> Ordering {
        match operand {
            Value::Text(text) => uuid_text(stored).as_str().cmp(text.as_str()),
            Value::Numeric(_) => Ordering::Greater,
            Value::Blob(_) => Ordering::Less,
            Value::Null => panic!("a NULL operand never reaches the seek key"),
        }
    }

    fn random_operand(rng: &mut ChaCha8Rng, near: &[u8; 16]) -> Value {
        let mut bytes = *near;
        if rng.random_bool(0.5) {
            let position = rng.random_range(0..16);
            bytes[position] = rng.random();
        }
        let text = uuid_text(&bytes);
        match rng.random_range(0..7) {
            0 | 1 => Value::build_text(text),
            2 => Value::build_text(text.to_uppercase()),
            3 => Value::build_text(text[..rng.random_range(0..UUID_TEXT_LEN)].to_string()),
            4 => Value::build_text(format!("{{{text}}}")),
            5 => Value::from_slice(&bytes).unwrap(),
            _ => Value::from_i64(rng.random()),
        }
    }

    #[test]
    fn seek_key_compares_like_the_decoded_value() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        for _ in 0..20_000 {
            let stored: [u8; 16] = rng.random();
            let operand = random_operand(&mut rng, &stored);
            let equal_key = exec_uuid_seek_key(&operand, &no_key(NoSeekKey::Null)).unwrap();
            let decoded_cmp = compare_decoded_with_operand(&stored, &operand);
            if matches!(equal_key, Value::Null) {
                assert_ne!(decoded_cmp, Ordering::Equal, "{operand:?}");
                let below =
                    exec_uuid_seek_key(&operand, &no_key(NoSeekKey::BelowEveryValue)).unwrap();
                let above =
                    exec_uuid_seek_key(&operand, &no_key(NoSeekKey::AboveEveryValue)).unwrap();
                assert_eq!(compare_stored_with_key(&stored, &below), Ordering::Greater);
                assert_eq!(compare_stored_with_key(&stored, &above), Ordering::Less);
                continue;
            }
            assert_eq!(
                compare_stored_with_key(&stored, &equal_key),
                decoded_cmp,
                "{operand:?}"
            );
        }
    }

    #[test]
    fn seek_key_accepts_only_canonical_text() {
        let canonical = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        let key =
            exec_uuid_seek_key(&Value::build_text(canonical), &no_key(NoSeekKey::Null)).unwrap();
        assert!(matches!(key, Value::Blob(ref b) if b.len() == 16));
        for operand in [
            Value::build_text(canonical.to_uppercase()),
            Value::build_text(canonical.replace('-', "")),
            Value::build_text(format!("{canonical} ")),
            Value::build_text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1g"),
            Value::build_text("a0eebc99x9c0b-4ef8-bb6d-6bb9bd380a11"),
            Value::from_slice(&[0u8; 16]).unwrap(),
            Value::from_i64(5),
            Value::Null,
        ] {
            let key = exec_uuid_seek_key(&operand, &no_key(NoSeekKey::Null)).unwrap();
            assert!(matches!(key, Value::Null), "{operand:?}");
        }
    }

    fn integer_key(key: &Value) -> Option<i64> {
        match key {
            Value::Null => None,
            Value::Numeric(Numeric::Integer(key)) => Some(*key),
            other => panic!("unexpected seek key {other:?}"),
        }
    }

    /// The rows that a seek with the key finds must include every row that
    /// the scan finds: an equality key finds exactly the equal rows, a lower
    /// bound keeps every row above the operand and an upper bound every row
    /// below it.
    fn assert_key_keeps_scan_rows(
        stored: i64,
        decoded_cmp: Ordering,
        equal: Option<i64>,
        lower: i64,
        upper: i64,
        operand: &str,
    ) {
        match equal {
            Some(key) => assert_eq!(stored.cmp(&key), decoded_cmp, "{operand}"),
            None => assert_ne!(decoded_cmp, Ordering::Equal, "{operand}"),
        }
        if decoded_cmp != Ordering::Less {
            assert!(
                stored >= lower,
                "{operand}: {stored} below lower key {lower}"
            );
        }
        if decoded_cmp == Ordering::Greater {
            assert!(
                stored > lower,
                "{operand}: {stored} not above lower key {lower}"
            );
        }
        if decoded_cmp != Ordering::Greater {
            assert!(
                stored <= upper,
                "{operand}: {stored} above upper key {upper}"
            );
        }
        if decoded_cmp == Ordering::Less {
            assert!(
                stored < upper,
                "{operand}: {stored} not below upper key {upper}"
            );
        }
    }

    fn temporal_operand(rng: &mut ChaCha8Rng, kind: PgTemporal, near: i64) -> Value {
        let text = format_temporal(kind, near).unwrap().as_str().to_string();
        match rng.random_range(0..8) {
            0 | 1 => Value::build_text(text),
            2 => Value::build_text(text[..rng.random_range(0..text.len())].to_string()),
            3 => Value::build_text(format!("{text}0")),
            4 => Value::build_text(text.replace(' ', "T")),
            5 => Value::build_text(format!(
                "{}{}",
                &text[..text.len() - 1],
                rng.random_range(0..10)
            )),
            6 => Value::from_i64(rng.random_range(-1000..100_000)),
            _ => Value::from_slice(text.as_bytes()).unwrap(),
        }
    }

    fn compare_decoded_text_with_operand(decoded: &str, operand: &Value) -> Ordering {
        match operand {
            Value::Text(text) => decoded.as_bytes().cmp(text.as_str().as_bytes()),
            Value::Numeric(_) => Ordering::Greater,
            Value::Blob(_) => Ordering::Less,
            Value::Null => panic!("a NULL operand never reaches the seek key"),
        }
    }

    #[test]
    fn temporal_seek_key_keeps_the_rows_of_the_text_comparison() {
        let mut rng = ChaCha8Rng::seed_from_u64(17);
        for kind in [PgTemporal::Timestamp, PgTemporal::Date, PgTemporal::Time] {
            let (first, last) = kind.stored_range();
            for _ in 0..2_000 {
                let near = rng.random_range(first..=last);
                let operand = temporal_operand(&mut rng, kind, near);
                let key = |bound| {
                    integer_key(&exec_pg_temporal_seek_key(kind, &operand, &no_key(bound)).unwrap())
                };
                let equal = key(NoSeekKey::Null);
                let lower = key(NoSeekKey::BelowEveryValue).unwrap();
                let upper = key(NoSeekKey::AboveEveryValue).unwrap();
                let mut samples: Vec<i64> = (0..50)
                    .map(|_| (near + rng.random_range(-100_000..100_000)).clamp(first, last))
                    .collect();
                samples.extend([first, last, near]);
                for stored in samples {
                    let decoded = format_temporal(kind, stored).unwrap();
                    let decoded_cmp = compare_decoded_text_with_operand(decoded.as_str(), &operand);
                    assert_key_keeps_scan_rows(
                        stored,
                        decoded_cmp,
                        equal,
                        lower,
                        upper,
                        &format!("{kind:?} {operand:?}"),
                    );
                }
            }
        }
    }

    #[test]
    fn temporal_seek_key_bounds_a_date_only_operand() {
        let lower = exec_pg_temporal_seek_key(
            PgTemporal::Timestamp,
            &Value::build_text("2024-01-01"),
            &no_key(NoSeekKey::BelowEveryValue),
        )
        .unwrap();
        let midnight = crate::functions::pg_types::exec_pg_temporal_encode(
            PgTemporal::Timestamp,
            &Value::build_text("2024-01-01 00:00:00"),
        )
        .unwrap();
        assert_eq!(integer_key(&lower), integer_key(&midnight).map(|m| m - 1));
    }

    fn numeric_compare(stored: i64, scale: i64, operand: &Value) -> Ordering {
        let decoded = crate::functions::pg_types::format_scaled_integer(stored, scale);
        let decoded = value_to_bigdecimal(&Value::build_text(decoded)).unwrap();
        decoded.cmp(&value_to_bigdecimal(operand).unwrap())
    }

    fn numeric_operand(rng: &mut ChaCha8Rng, near: i64, scale: i64) -> Value {
        let text = crate::functions::pg_types::format_scaled_integer(near, scale);
        match rng.random_range(0..7) {
            0 => Value::build_text(text),
            1 => Value::build_text(format!("{text}{}", rng.random_range(0..10))),
            2 => Value::from_i64(near / 10i64.pow(scale as u32)),
            3 => Value::from_f64(text.parse::<f64>().unwrap()),
            4 => Value::build_text(format!("{text}e{}", rng.random_range(-3..3))),
            5 => Value::build_text(format!(
                "{}9999999999999999999",
                if near < 0 { "-" } else { "" }
            )),
            _ => Value::build_text(format!("{text}5")),
        }
    }

    #[test]
    fn numeric_seek_key_keeps_the_rows_of_the_decimal_comparison() {
        let mut rng = ChaCha8Rng::seed_from_u64(23);
        let limit = NUMERIC_STORED_LIMIT as i64 - 1;
        for _ in 0..5_000 {
            let scale = rng.random_range(0..=18);
            let near = match rng.random_range(0..3) {
                0 => rng.random_range(-1000..1000),
                1 => rng.random_range(-limit..=limit),
                _ => [limit, -limit, 0][rng.random_range(0..3)],
            };
            let operand = numeric_operand(&mut rng, near, scale);
            let scale_value = Value::from_i64(scale);
            let key = |bound| {
                integer_key(
                    &exec_pg_numeric_seek_key(&operand, &no_key(bound), &scale_value).unwrap(),
                )
            };
            let equal = key(NoSeekKey::Null);
            let lower = key(NoSeekKey::BelowEveryValue).unwrap();
            let upper = key(NoSeekKey::AboveEveryValue).unwrap();
            let mut samples: Vec<i64> = (0..30)
                .map(|_| {
                    near.saturating_add(rng.random_range(-1000..1000))
                        .clamp(-limit, limit)
                })
                .collect();
            samples.extend([-limit, limit, near]);
            for stored in samples {
                let decoded_cmp = numeric_compare(stored, scale, &operand);
                assert_key_keeps_scan_rows(
                    stored,
                    decoded_cmp,
                    equal,
                    lower,
                    upper,
                    &format!("{operand:?} scale {scale}"),
                );
            }
        }
    }

    #[test]
    fn v1_numeric_seek_key_is_the_blob_of_the_equal_value() {
        use crate::numeric::decimal::{bigdecimal_to_blob, validate_precision_scale};
        let mut rng = ChaCha8Rng::seed_from_u64(29);
        for _ in 0..5_000 {
            let precision = rng.random_range(1..=18);
            let scale = rng.random_range(0..=precision);
            let limit = 10i64.pow(precision as u32) - 1;
            let near = rng.random_range(-limit..=limit);
            let operand = numeric_operand(&mut rng, near, scale);
            let key = exec_numeric_seek_key(
                &operand,
                &Value::from_i64(precision),
                &Value::from_i64(scale),
            )
            .unwrap();
            for stored in [near, near.saturating_add(1).min(limit), -near] {
                let decimal = bigdecimal::BigDecimal::new(stored.into(), scale);
                let blob = Value::from_blob(bigdecimal_to_blob(
                    &validate_precision_scale(&decimal, precision, scale).unwrap(),
                ));
                assert_eq!(
                    key == blob,
                    decimal == value_to_bigdecimal(&operand).unwrap(),
                    "{operand:?} and {decimal} at numeric({precision}, {scale})"
                );
            }
        }
        let err = exec_numeric_seek_key(
            &Value::build_text("12.5abc"),
            &Value::from_i64(10),
            &Value::from_i64(2),
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid numeric value"), "{err}");
    }

    #[test]
    fn numeric_seek_key_raises_like_the_scan() {
        let message = exec_pg_numeric_seek_key(
            &Value::build_text("12.5abc"),
            &no_key(NoSeekKey::Null),
            &Value::from_i64(2),
        )
        .unwrap_err()
        .to_string();
        assert!(message.contains("invalid numeric value"), "{message}");
    }

    #[test]
    fn numeric_seek_keys_refuse_type_parameters_that_are_not_integers() {
        let operand = Value::from_i64(1);
        let message =
            exec_pg_numeric_seek_key(&operand, &no_key(NoSeekKey::Null), &Value::from_f64(2.5))
                .unwrap_err()
                .to_string();
        assert!(message.contains("scale must be an integer"), "{message}");
        assert!(
            exec_pg_numeric_seek_key(&operand, &no_key(NoSeekKey::Null), &Value::from_i64(30))
                .is_err()
        );
        let message = exec_numeric_seek_key(&operand, &Value::from_i64(10), &Value::from_f64(2.5))
            .unwrap_err()
            .to_string();
        assert!(message.contains("scale must be an integer"), "{message}");
        let message = exec_numeric_seek_key(&operand, &Value::from_f64(10.5), &Value::from_i64(2))
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("precision must be an integer"),
            "{message}"
        );
    }
}
