use crate::types::Value;
use crate::{turso_assert_eq, Numeric, Result};

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
}
