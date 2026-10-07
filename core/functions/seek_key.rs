use crate::functions::pg_types::{
    check_numeric_scale, format_temporal, numeric_type_parameter, parse_canonical_temporal,
    scale_decimal, PgTemporal, Remainder, ScaledDecimal, TemporalText, NUMERIC_STORED_LIMIT,
};
use crate::numeric::decimal::value_to_bigdecimal;
use crate::types::{SeekOp, Value};
use crate::{turso_assert_eq, LimboError, Numeric, Result};
use turso_parser::ast::{self, SortOrder};

const UUID_TEXT_LEN: usize = 36;
const UUID_HYPHEN_POSITIONS: [usize; 4] = [8, 13, 18, 23];
// An integer sorts below every blob, and 17 bytes of 0xff sort above every 16-byte blob.
const KEY_BELOW_EVERY_UUID: i64 = 0;
const KEY_ABOVE_EVERY_UUID: [u8; 17] = [0xff; 17];
const KEY_EQUAL_TO_NO_NUMERIC: i64 = i64::MAX;
const _: () = assert!(KEY_EQUAL_TO_NO_NUMERIC as i128 >= NUMERIC_STORED_LIMIT);

/// What a seek key function gives when no stored value shows the operand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoSeekKey {
    /// NULL: no row is equal to the operand.
    Null = 0,
    /// A key below the operand. The `pg_` types give the last stored value
    /// below the operand, and `uuid` gives a key below every stored value.
    Below = -1,
    /// A key above the operand. The `pg_` types give the first stored value
    /// above the operand, and `uuid` gives a key above every stored value.
    Above = 1,
}

/// The key that a scan compares the stored integers of a `pg_numeric`
/// column with for `column <op> operand`, in the stored order: the key of
/// a bound for a range operator, and NULL for `=` and `!=`.
pub(crate) fn scan_comparison_key(op: &ast::Operator) -> NoSeekKey {
    match op {
        ast::Operator::Equals | ast::Operator::NotEquals => NoSeekKey::Null,
        ast::Operator::Greater => exact_bound_key(SeekOp::GT, SortOrder::Asc),
        ast::Operator::GreaterEquals => {
            exact_bound_key(SeekOp::GE { eq_only: false }, SortOrder::Asc)
        }
        ast::Operator::Less => exact_bound_key(SeekOp::LT, SortOrder::Asc),
        ast::Operator::LessEquals => exact_bound_key(SeekOp::LE { eq_only: false }, SortOrder::Asc),
        other => unreachable!("{other:?} is not a comparison of a pg_numeric scan"),
    }
}

/// The key of a range bound of a `pg_` type that gives exactly the rows of
/// the comparison. `op` is the seek or the stop of the bound in the order of
/// the index. `GT` and `LE` put the key with the entries before the bound,
/// and `GE` and `LT` put it with the entries after the bound. In stored
/// order, a key with the lower values must be the last stored value below
/// the operand, and a key with the higher values the first stored value
/// above it. A DESC index stores the higher values first.
pub(crate) fn exact_bound_key(op: SeekOp, order: SortOrder) -> NoSeekKey {
    let key_is_before_bound = matches!(op, SeekOp::GT | SeekOp::LE { .. });
    match (key_is_before_bound, order) {
        (true, SortOrder::Asc) | (false, SortOrder::Desc) => NoSeekKey::Below,
        (false, SortOrder::Asc) | (true, SortOrder::Desc) => NoSeekKey::Above,
    }
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
        n if n == NoSeekKey::Below as i64 => Value::from_i64(KEY_BELOW_EVERY_UUID),
        n if n == NoSeekKey::Above as i64 => Value::from_slice(&KEY_ABOVE_EVERY_UUID)?,
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
                NoSeekKey::Below => Some(last_stored_below(kind, text.as_str().as_bytes())),
                NoSeekKey::Above => Some(first_stored_above(kind, text.as_str().as_bytes())),
            },
        },
        Value::Numeric(_) => match bound {
            NoSeekKey::Null => None,
            NoSeekKey::Below => Some(i64::MIN),
            NoSeekKey::Above => Some(first),
        },
        Value::Blob(_) => match bound {
            NoSeekKey::Null => None,
            NoSeekKey::Below => Some(last),
            NoSeekKey::Above => Some(i64::MAX),
        },
    };
    Ok(key.map_or(Value::Null, Value::from_i64))
}

/// The key that a scan compares the stored integer of a `pg_numeric`
/// column with, in place of `numeric_lt` or `numeric_eq` with the operand.
/// `no_key` comes from [scan_comparison_key]. For `=` and `!=`, an operand
/// that no stored value equals gets a key outside the stored values. NULL
/// when the operand is NULL or not a number: the scan then compares the
/// decimals, which gives NULL or raises the error.
pub(crate) fn exec_pg_numeric_compare_key(
    operand: &Value,
    no_key: &Value,
    scale: &Value,
) -> Result<Value> {
    if matches!(operand, Value::Null) || value_to_bigdecimal(operand).is_err() {
        return Ok(Value::Null);
    }
    let key = exec_pg_numeric_seek_key(operand, no_key, scale)?;
    Ok(match (key, seek_bound(no_key)) {
        (Value::Null, NoSeekKey::Null) => Value::from_i64(KEY_EQUAL_TO_NO_NUMERIC),
        (key, _) => key,
    })
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
        NoSeekKey::Below | NoSeekKey::Above => Some(i64::MAX),
    };
    let below_every_value = match bound {
        NoSeekKey::Null => None,
        NoSeekKey::Below | NoSeekKey::Above => Some(i64::MIN),
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
                NoSeekKey::Below => Some(floor),
                NoSeekKey::Above => Some(floor + 1),
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
            "numeric_seek_key: precision must be an integer".to_string(),
        ));
    };
    let Value::Numeric(Numeric::Integer(scale)) = scale else {
        return Err(LimboError::Constraint(
            "numeric_seek_key: scale must be an integer".to_string(),
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
        n if n == NoSeekKey::Below as i64 => NoSeekKey::Below,
        n if n == NoSeekKey::Above as i64 => NoSeekKey::Above,
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
    use crate::translate::collate::CollationSeq;
    use crate::types::compare_immutable_single;
    use crate::vdbe::affinity::Affinity;
    use crate::vdbe::execute::apply_affinity_char;
    use crate::vdbe::Register;
    use either::Either;
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
                let below = exec_uuid_seek_key(&operand, &no_key(NoSeekKey::Below)).unwrap();
                let above = exec_uuid_seek_key(&operand, &no_key(NoSeekKey::Above)).unwrap();
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
                let lower = key(NoSeekKey::Below).unwrap();
                let upper = key(NoSeekKey::Above).unwrap();
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
            &no_key(NoSeekKey::Below),
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
            let lower = key(NoSeekKey::Below).unwrap();
            let upper = key(NoSeekKey::Above).unwrap();
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

    #[test]
    fn temporal_seek_reads_exactly_the_rows_of_the_scan() {
        let mut rng = ChaCha8Rng::seed_from_u64(31);
        for (kind, key_kind) in [
            (PgTemporal::Timestamp, PgTemporal::Timestamp),
            (PgTemporal::Timestamptz, PgTemporal::Timestamp),
            (PgTemporal::Date, PgTemporal::Date),
            (PgTemporal::Time, PgTemporal::Time),
        ] {
            let (first, last) = kind.stored_range();
            for _ in 0..3_000 {
                let near = match rng.random_range(0..4) {
                    0 => first + rng.random_range(0..3),
                    1 => last - rng.random_range(0..3),
                    _ => rng.random_range(first..=last),
                };
                let operand = temporal_test_operand(&mut rng, kind, near);
                let affinity = [
                    Affinity::Numeric,
                    Affinity::Integer,
                    Affinity::Real,
                    Affinity::Blob,
                ][rng.random_range(0..4)];
                let seek_operand = after_affinity(&operand, affinity);
                let mut stored_values: Vec<i64> = (0..40)
                    .map(|_| {
                        let distance = 10i64.pow(rng.random_range(0..12));
                        (near + rng.random_range(-distance..=distance)).clamp(first, last)
                    })
                    .collect();
                stored_values.extend([first, first + 1, last - 1, last, near]);
                for comparison in COMPARISONS {
                    assert_seek_reads_the_rows_of_the_scan(
                        comparison,
                        |side| {
                            integer_key(
                                &exec_pg_temporal_seek_key(key_kind, &seek_operand, &no_key(side))
                                    .unwrap(),
                            )
                        },
                        &stored_values,
                        |stored| {
                            comparison.holds(temporal_scan_order(kind, stored, &operand, affinity))
                        },
                        &format!("{kind:?} {operand:?} with affinity {affinity:?}"),
                    );
                }
            }
        }
    }

    fn temporal_test_operand(rng: &mut ChaCha8Rng, kind: PgTemporal, near: i64) -> Value {
        let text = format_temporal(kind, near).unwrap().as_str().to_string();
        let (first, last) = kind.stored_range();
        let edge = [first, last][rng.random_range(0..2)];
        let edge_text = format_temporal(kind, edge).unwrap().as_str().to_string();
        let random_text: String = (0..rng.random_range(0..12))
            .map(|_| char::from(rng.random_range(0x20u8..0x7f)))
            .collect();
        match rng.random_range(0..16) {
            0 | 1 => Value::build_text(text),
            2 => Value::build_text(edge_text),
            3 => Value::build_text(text[..rng.random_range(0..text.len())].to_string()),
            4 => Value::build_text(format!(
                "{text}{}",
                ["0", " ", "Z", "+00", ".0"][rng.random_range(0..5)]
            )),
            5 => Value::build_text(text.replace(' ', "T")),
            6 => Value::build_text(format!(
                "{}{}",
                &text[..text.len() - 1],
                rng.random_range(0..10)
            )),
            7 => Value::build_text(format!(" {text}")),
            8 => Value::build_text(
                [
                    "5", " 12 ", "1e3", "-7", "0x10", "1.5", "2024", "", "1e999", "+3", ".5",
                    "5abc",
                ][rng.random_range(0..12)],
            ),
            9 => Value::build_text(random_text),
            10 => Value::from_i64(rng.random_range(-100_000..100_000)),
            11 => Value::from_i64([i64::MIN, i64::MAX, near, first, last][rng.random_range(0..5)]),
            12 => Value::from_f64(rng.random_range(-1e6..1e6)),
            13 => Value::from_slice(text.as_bytes()).unwrap(),
            14 => Value::from_slice(&rng.random::<[u8; 4]>()).unwrap(),
            _ => Value::from_slice(&[]).unwrap(),
        }
    }

    fn after_affinity(value: &Value, affinity: Affinity) -> Value {
        let mut register = Register::Value(value.clone());
        apply_affinity_char(&mut register, affinity);
        register.get_value().clone()
    }

    /// The comparison of a scan: the column shows its DECODEd text, and both
    /// operands are converted with the affinity of the comparison.
    fn temporal_scan_order(
        kind: PgTemporal,
        stored: i64,
        operand: &Value,
        affinity: Affinity,
    ) -> Ordering {
        let decoded =
            crate::functions::pg_types::exec_pg_temporal_decode(kind, &Value::from_i64(stored))
                .unwrap();
        let decoded_converted = affinity.convert_for_compare(&decoded);
        let operand_converted = affinity.convert_for_compare(operand);
        compare_immutable_single(
            decoded_converted
                .as_ref()
                .map_or(Either::Left(&decoded), Either::Right),
            operand_converted
                .as_ref()
                .map_or(Either::Left(operand), Either::Right),
            CollationSeq::Binary,
        )
    }

    #[test]
    fn numeric_seek_reads_exactly_the_rows_of_the_scan() {
        let mut rng = ChaCha8Rng::seed_from_u64(37);
        for _ in 0..4_000 {
            let precision = rng.random_range(1..=18);
            let scale = rng.random_range(0..=precision);
            let limit = 10i64.pow(precision as u32) - 1;
            let near = match rng.random_range(0..4) {
                0 => rng.random_range(-1000..1000).clamp(-limit, limit),
                1 => [limit, -limit, 0][rng.random_range(0..3)],
                _ => rng.random_range(-limit..=limit),
            };
            let operand = numeric_test_operand(&mut rng, near, scale);
            let scale_value = Value::from_i64(scale);
            let mut stored_values: Vec<i64> = (0..30)
                .map(|_| {
                    let distance = 10i64.pow(rng.random_range(0..precision as u32));
                    near.saturating_add(rng.random_range(-distance..=distance))
                        .clamp(-limit, limit)
                })
                .collect();
            stored_values.extend([-limit, limit, 0, near]);
            let decoded: Vec<Value> = stored_values
                .iter()
                .map(|&stored| {
                    crate::functions::pg_types::exec_pg_numeric_decode(
                        &Value::from_i64(stored),
                        &scale_value,
                    )
                    .unwrap()
                })
                .collect();
            let key_error =
                exec_pg_numeric_seek_key(&operand, &no_key(NoSeekKey::Null), &scale_value).is_err();
            for comparison in COMPARISONS {
                let scan: Vec<Result<bool>> = decoded
                    .iter()
                    .map(|decoded| numeric_scan(comparison, decoded, &operand))
                    .collect();
                if key_error {
                    assert!(
                        scan.iter().all(Result::is_err),
                        "{operand:?}: the key raises an error but the scan does not"
                    );
                    continue;
                }
                let scan: Vec<bool> = scan
                    .into_iter()
                    .map(|result| {
                        result.unwrap_or_else(|e| {
                            panic!("{operand:?}: the scan raises {e} but the key does not")
                        })
                    })
                    .collect();
                assert_seek_reads_the_rows_of_the_scan(
                    comparison,
                    |side| {
                        integer_key(
                            &exec_pg_numeric_seek_key(&operand, &no_key(side), &scale_value)
                                .unwrap(),
                        )
                    },
                    &stored_values,
                    |stored| scan[stored_values.iter().position(|&s| s == stored).unwrap()],
                    &format!("{operand:?} at numeric({precision}, {scale})"),
                );
            }
        }
    }

    fn numeric_test_operand(rng: &mut ChaCha8Rng, near: i64, scale: i64) -> Value {
        let text = crate::functions::pg_types::format_scaled_integer(near, scale);
        match rng.random_range(0..14) {
            0..=6 => numeric_operand(rng, near, scale),
            7 => Value::build_text(format!(" {text} ")),
            8 => Value::build_text(
                ["-0", "-0.00", "0e5", "1_000", "+.5", "-.5"][rng.random_range(0..6)],
            ),
            9 => Value::build_text(["abc", "", "1e", "--1", "1_", "NaN"][rng.random_range(0..6)]),
            10 => Value::from_f64([f64::INFINITY, -0.0, 1e300, -1e-300][rng.random_range(0..4)]),
            11 => Value::from_i64([i64::MIN, i64::MAX, 0, -1][rng.random_range(0..4)]),
            12 => {
                let decimal = value_to_bigdecimal(&Value::build_text(text)).unwrap();
                Value::from_blob(crate::numeric::decimal::bigdecimal_to_blob(&decimal))
            }
            _ => Value::from_slice(&rng.random::<[u8; 6]>()).unwrap(),
        }
    }

    /// The comparison of a scan: `numeric_lt` and `numeric_eq` read the
    /// DECODEd column and the operand as decimals. The other operators are
    /// derived from them as `find_operator_function` derives them.
    fn numeric_scan(comparison: Comparison, decoded: &Value, operand: &Value) -> Result<bool> {
        use crate::numeric::decimal::{exec_numeric_eq, exec_numeric_lt};
        let is_true = |value: Value| Ok(value == Value::from_i64(1));
        match comparison {
            Comparison::Eq => is_true(exec_numeric_eq(decoded, operand)?),
            Comparison::Ne => is_true(exec_numeric_eq(decoded, operand)?).map(|eq| !eq),
            Comparison::Lt => is_true(exec_numeric_lt(decoded, operand)?),
            Comparison::Gt => is_true(exec_numeric_lt(operand, decoded)?),
            Comparison::Ge => is_true(exec_numeric_lt(decoded, operand)?).map(|lt| !lt),
            Comparison::Le => is_true(exec_numeric_lt(operand, decoded)?).map(|lt| !lt),
        }
    }

    #[test]
    fn numeric_scan_key_compares_like_the_decimals() {
        let mut rng = ChaCha8Rng::seed_from_u64(41);
        for _ in 0..4_000 {
            let precision = rng.random_range(1..=18);
            let scale = rng.random_range(0..=precision);
            let limit = 10i64.pow(precision as u32) - 1;
            let near = match rng.random_range(0..4) {
                0 => rng.random_range(-1000..1000).clamp(-limit, limit),
                1 => [limit, -limit, 0][rng.random_range(0..3)],
                _ => rng.random_range(-limit..=limit),
            };
            let operand = numeric_test_operand(&mut rng, near, scale);
            let scale_value = Value::from_i64(scale);
            let mut stored_values: Vec<i64> = (0..30)
                .map(|_| {
                    let distance = 10i64.pow(rng.random_range(0..precision as u32));
                    near.saturating_add(rng.random_range(-distance..=distance))
                        .clamp(-limit, limit)
                })
                .collect();
            stored_values.extend([-limit, limit, 0, near]);
            for comparison in SCAN_COMPARISONS {
                let mode = no_key(scan_comparison_key(&comparison.operator()));
                let key = exec_pg_numeric_compare_key(&operand, &mode, &scale_value).unwrap();
                let Value::Numeric(Numeric::Integer(key)) = key else {
                    assert_eq!(key, Value::Null, "{operand:?}");
                    assert!(
                        value_to_bigdecimal(&operand).is_err(),
                        "{operand:?} is a number but has no key"
                    );
                    continue;
                };
                for &stored in &stored_values {
                    let decoded = crate::functions::pg_types::exec_pg_numeric_decode(
                        &Value::from_i64(stored),
                        &scale_value,
                    )
                    .unwrap();
                    let decimal = numeric_scan(comparison, &decoded, &operand).unwrap();
                    assert_eq!(
                        comparison.holds(stored.cmp(&key)),
                        decimal,
                        "{decoded:?} {comparison:?} {operand:?} at scale {scale}: key {key}"
                    );
                }
            }
        }
        for comparison in SCAN_COMPARISONS {
            let mode = no_key(scan_comparison_key(&comparison.operator()));
            let key =
                exec_pg_numeric_compare_key(&Value::Null, &mode, &Value::from_i64(2)).unwrap();
            assert_eq!(key, Value::Null);
        }
    }

    /// For every loop that can read an index for `column <comparison>
    /// operand`, the rows that the seek reads must be exactly the rows for
    /// which `scan` is true: no row in the bounds fails the comparison, and
    /// no row outside the bounds passes it.
    fn assert_seek_reads_the_rows_of_the_scan(
        comparison: Comparison,
        key: impl Fn(NoSeekKey) -> Option<i64>,
        stored_values: &[i64],
        scan: impl Fn(i64) -> bool,
        context: &str,
    ) {
        let loops: Vec<SeekLoop> = match comparison {
            Comparison::Eq => vec![SeekLoop::Equality(key(NoSeekKey::Null))],
            _ => [false, true]
                .into_iter()
                .flat_map(|backwards| [(backwards, SortOrder::Asc), (backwards, SortOrder::Desc)])
                .map(|(backwards, order)| {
                    let bound = loop_bound(comparison, backwards, order);
                    let side = match bound {
                        LoopBound::Seek(op) | LoopBound::Stop(op) => exact_bound_key(op, order),
                    };
                    SeekLoop::Range {
                        bound,
                        order,
                        key: key(side).expect("a range bound always has a key"),
                    }
                })
                .collect(),
        };
        for seek_loop in loops {
            for &stored in stored_values {
                let in_bounds = seek_loop.reads(stored);
                let passes = scan(stored);
                assert!(
                    !in_bounds || passes,
                    "{context}: stored {stored} is read by {seek_loop:?} for {comparison:?} but fails the scan"
                );
                assert!(
                    in_bounds || !passes,
                    "{context}: stored {stored} passes the scan for {comparison:?} but {seek_loop:?} does not read it"
                );
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum Comparison {
        Eq,
        Ne,
        Lt,
        Le,
        Gt,
        Ge,
    }

    const COMPARISONS: [Comparison; 5] = [
        Comparison::Eq,
        Comparison::Lt,
        Comparison::Le,
        Comparison::Gt,
        Comparison::Ge,
    ];

    const SCAN_COMPARISONS: [Comparison; 6] = [
        Comparison::Eq,
        Comparison::Ne,
        Comparison::Lt,
        Comparison::Le,
        Comparison::Gt,
        Comparison::Ge,
    ];

    impl Comparison {
        fn operator(self) -> ast::Operator {
            match self {
                Self::Eq => ast::Operator::Equals,
                Self::Ne => ast::Operator::NotEquals,
                Self::Lt => ast::Operator::Less,
                Self::Le => ast::Operator::LessEquals,
                Self::Gt => ast::Operator::Greater,
                Self::Ge => ast::Operator::GreaterEquals,
            }
        }

        fn holds(self, order: Ordering) -> bool {
            match self {
                Self::Eq => order.is_eq(),
                Self::Ne => order.is_ne(),
                Self::Lt => order.is_lt(),
                Self::Le => order.is_le(),
                Self::Gt => order.is_gt(),
                Self::Ge => order.is_ge(),
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum LoopBound {
        Seek(SeekOp),
        Stop(SeekOp),
    }

    /// The seek or the stop that `build_seek_def` gives the bound of a range
    /// comparison `column <op> operand`, for the direction of the loop and the
    /// order of the index.
    fn loop_bound(comparison: Comparison, backwards: bool, order: SortOrder) -> LoopBound {
        let ge = SeekOp::GE { eq_only: false };
        let le = SeekOp::LE { eq_only: false };
        let is_lower_bound = match comparison {
            Comparison::Gt | Comparison::Ge => true,
            Comparison::Lt | Comparison::Le => false,
            Comparison::Eq | Comparison::Ne => unreachable!("{comparison:?} has no range bound"),
        };
        let is_strict = matches!(comparison, Comparison::Gt | Comparison::Lt);
        let walks_up_the_stored_values = (order == SortOrder::Asc) != backwards;
        let is_seek = is_lower_bound == walks_up_the_stored_values;
        match (backwards, is_seek, is_strict) {
            (false, true, true) => LoopBound::Seek(SeekOp::GT),
            (false, true, false) => LoopBound::Seek(ge),
            (false, false, true) => LoopBound::Stop(ge),
            (false, false, false) => LoopBound::Stop(SeekOp::GT),
            (true, true, true) => LoopBound::Seek(SeekOp::LT),
            (true, true, false) => LoopBound::Seek(le),
            (true, false, true) => LoopBound::Stop(le),
            (true, false, false) => LoopBound::Stop(SeekOp::LT),
        }
    }

    /// Whether the loop of the seek reads the row with the stored value. The
    /// loop reads the entries that pass the operation of the seek, and ends
    /// at the first entry that passes the operation of the stop.
    fn loop_reads(bound: LoopBound, order: SortOrder, stored: i64, key: i64) -> bool {
        let in_index_order = match order {
            SortOrder::Asc => stored.cmp(&key),
            SortOrder::Desc => key.cmp(&stored),
        };
        let passes = |op: SeekOp| match op {
            SeekOp::GT => in_index_order.is_gt(),
            SeekOp::GE { .. } => in_index_order.is_ge(),
            SeekOp::LT => in_index_order.is_lt(),
            SeekOp::LE { .. } => in_index_order.is_le(),
        };
        match bound {
            LoopBound::Seek(op) => passes(op),
            LoopBound::Stop(op) => !passes(op),
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum SeekLoop {
        Equality(Option<i64>),
        Range {
            bound: LoopBound,
            order: SortOrder,
            key: i64,
        },
    }

    impl SeekLoop {
        fn reads(self, stored: i64) -> bool {
            match self {
                Self::Equality(key) => key == Some(stored),
                Self::Range { bound, order, key } => loop_reads(bound, order, stored, key),
            }
        }
    }
}
