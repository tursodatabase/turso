//! Key conditioning for the sorter: turns the sort key of a record into a
//! byte string whose byte-wise order is the SQL order of the key. The
//! adaptive sort in [crate::vdbe::adaptive_sort] compares and partitions
//! keys one byte at a time, so it needs keys in this form.
//!
//! Each column becomes a class byte followed by the column value:
//! - NULL is the class byte alone, ranked below or above the other classes
//!   as the NULLS FIRST/LAST rule of the column requires.
//! - Numbers use three classes: floats below the i64 range, values in the
//!   i64 range, and floats above it. In-range values are the floor as a
//!   sign-flipped big-endian i64, then a 0 byte for integral values or a 1
//!   byte plus the order-preserving f64 bits for fractional values, so that
//!   integers and floats interleave exactly like `Numeric::cmp`.
//! - Text and blobs are the bytes with 0x00 escaped as 0x00 0xFF and a
//!   0x00 0x00 terminator, so a shorter key sorts before its extensions and
//!   the next column starts at a known position. NOCASE folds ASCII letters
//!   and stops at the first NUL byte, where `CollationSeq::nocase_cmp`
//!   compares lengths instead. RTRIM removes trailing spaces.
//!
//! DESC columns are stored bit-inverted, which reverses their byte order.

use turso_parser::ast::{NullsOrder, SortOrder};

use crate::alloc::*;
use crate::numeric::Numeric;
use crate::translate::collate::CollationSeq;
use crate::types::{KeyInfo, ValueRef};
use crate::vdbe::sorter::SortComparator;
use crate::Result;

const CLASS_NULL_LOW: u8 = 0;
const CLASS_FLOAT_BELOW_I64: u8 = 1;
const CLASS_NUMERIC: u8 = 2;
const CLASS_FLOAT_ABOVE_I64: u8 = 3;
const CLASS_TEXT: u8 = 4;
const CLASS_BLOB: u8 = 5;
const CLASS_NULL_HIGH: u8 = 6;

const ESCAPED_ZERO: [u8; 2] = [0x00, 0xFF];
const TERMINATOR: [u8; 2] = [0x00, 0x00];
/// Replaces the terminator when a NOCASE string has a NUL byte; the string
/// length follows it, because that is what the collation compares next.
const NOCASE_NUL_MARKER: [u8; 2] = [0x00, 0x01];

const I64_RANGE: f64 = 9_223_372_036_854_775_808.0;

/// Conditioned keys reproduce the SQL order only for columns that compare by
/// bytes. Custom comparators and locale or connection-defined collations
/// need the value comparison.
pub(crate) fn keys_are_byte_orderable(
    key_info: &[KeyInfo],
    comparators: &[Option<SortComparator>],
) -> bool {
    comparators.iter().all(Option::is_none)
        && key_info.iter().all(|key| {
            matches!(
                key.collation,
                CollationSeq::Unset
                    | CollationSeq::Binary
                    | CollationSeq::NoCase
                    | CollationSeq::Rtrim
            )
        })
}

/// Appends the conditioned key for `values` to `out`.
pub(crate) fn encode_sort_key(
    values: &[ValueRef<'_>],
    key_info: &[KeyInfo],
    out: &mut Vec<u8>,
) -> Result<()> {
    let bound: usize = values.iter().map(encoded_size_bound).sum();
    // Reserved for the largest possible encoding, so the pushes below cannot grow the vector.
    out.try_reserve(bound)?;
    for (value, key) in values.iter().zip(key_info) {
        let start = out.len();
        encode_column(value, key, out);
        if key.sort_order == SortOrder::Desc {
            for byte in &mut out[start..] {
                *byte = !*byte;
            }
        }
    }
    Ok(())
}

fn encoded_size_bound(value: &ValueRef<'_>) -> usize {
    match value {
        ValueRef::Null => 1,
        ValueRef::Numeric(_) => 1 + 8 + 1 + 8,
        ValueRef::Text(text) => 1 + 2 * text.value.len() + NOCASE_NUL_MARKER.len() + 8,
        ValueRef::Blob(blob) => 1 + 2 * blob.len() + TERMINATOR.len(),
    }
}

fn encode_column(value: &ValueRef<'_>, key: &KeyInfo, out: &mut Vec<u8>) {
    match value {
        ValueRef::Null => {
            let nulls_high = matches!(
                (key.nulls_order, key.sort_order),
                (Some(NullsOrder::Last), SortOrder::Asc)
                    | (Some(NullsOrder::First), SortOrder::Desc)
            );
            out.push(if nulls_high {
                CLASS_NULL_HIGH
            } else {
                CLASS_NULL_LOW
            });
        }
        ValueRef::Numeric(numeric) => encode_numeric(*numeric, out),
        ValueRef::Text(text) => {
            out.push(CLASS_TEXT);
            match key.collation {
                CollationSeq::Unset | CollationSeq::Binary => {
                    encode_bytes(text.value.as_bytes(), out)
                }
                CollationSeq::NoCase => encode_nocase_text(text.value, out),
                CollationSeq::Rtrim => {
                    encode_bytes(text.value.trim_end_matches(' ').as_bytes(), out)
                }
                CollationSeq::Locale(_) | CollationSeq::Custom(_) => {
                    unreachable!(
                        "keys_are_byte_orderable rejects collations that do not compare by bytes"
                    )
                }
            }
        }
        ValueRef::Blob(blob) => {
            out.push(CLASS_BLOB);
            encode_bytes(blob, out);
        }
    }
}

fn encode_numeric(numeric: Numeric, out: &mut Vec<u8>) {
    match numeric {
        Numeric::Integer(int) => {
            out.push(CLASS_NUMERIC);
            out.extend_from_slice(&sign_flipped(int));
            out.push(0);
        }
        Numeric::Float(float) => {
            let float = f64::from(float);
            if float < -I64_RANGE {
                out.push(CLASS_FLOAT_BELOW_I64);
                out.extend_from_slice(&order_preserving_bits(float));
            } else if float >= I64_RANGE {
                out.push(CLASS_FLOAT_ABOVE_I64);
                out.extend_from_slice(&order_preserving_bits(float));
            } else {
                let floor = float.floor();
                out.push(CLASS_NUMERIC);
                out.extend_from_slice(&sign_flipped(floor as i64));
                if float == floor {
                    out.push(0);
                } else {
                    out.push(1);
                    out.extend_from_slice(&order_preserving_bits(float));
                }
            }
        }
    }
}

fn sign_flipped(int: i64) -> [u8; 8] {
    ((int as u64) ^ (1 << 63)).to_be_bytes()
}

fn order_preserving_bits(float: f64) -> [u8; 8] {
    let bits = float.to_bits();
    let ordered = if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    };
    ordered.to_be_bytes()
}

fn encode_bytes(mut bytes: &[u8], out: &mut Vec<u8>) {
    while let Some(zero_at) = bytes.iter().position(|byte| *byte == 0) {
        out.extend_from_slice(&bytes[..zero_at]);
        out.extend_from_slice(&ESCAPED_ZERO);
        bytes = &bytes[zero_at + 1..];
    }
    out.extend_from_slice(bytes);
    out.extend_from_slice(&TERMINATOR);
}

fn encode_nocase_text(text: &str, out: &mut Vec<u8>) {
    let bytes = text.as_bytes();
    let folded_end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    out.extend(bytes[..folded_end].iter().map(u8::to_ascii_lowercase));
    if folded_end < bytes.len() {
        out.extend_from_slice(&NOCASE_NUL_MARKER);
        out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    } else {
        out.extend_from_slice(&TERMINATOR);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{compare_immutable, AsValueRef, Value};
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };
    use std::cmp::Ordering;

    fn seed() -> u64 {
        std::env::var("SEED").map_or(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            |v| v.parse().expect("SEED must be a u64"),
        )
    }

    fn random_value(rng: &mut ChaCha8Rng) -> Value {
        match rng.next_u64() % 12 {
            0 => Value::Null,
            1 => Value::from_i64(rng.next_u64() as i64),
            2 => Value::from_i64((rng.next_u64() % 100) as i64 - 50),
            3 => {
                let base = 1i64 << (48 + rng.next_u64() % 15);
                Value::from_i64(base.wrapping_add((rng.next_u64() % 5) as i64 - 2))
            }
            4 => Value::from_i64([i64::MIN, i64::MAX, 0, -1, 1][(rng.next_u64() % 5) as usize]),
            5 => {
                let numerator = rng.next_u64() as f64;
                let denominator = (rng.next_u64() as f64).abs().max(1.0);
                Value::from_f64(numerator / denominator)
            }
            6 => {
                let int = (rng.next_u64() % 100) as f64 - 50.0;
                Value::from_f64(int + [0.0, 0.5, -0.5, 0.25][(rng.next_u64() % 4) as usize])
            }
            7 => Value::from_f64(
                [
                    0.0,
                    -0.0,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    I64_RANGE,
                    -I64_RANGE,
                    I64_RANGE * 2.0,
                    -I64_RANGE * 2.0,
                    9.007199254740992e15,
                    9.007199254740994e15,
                    -9.223372036854775e18,
                ][(rng.next_u64() % 11) as usize],
            ),
            8..=10 => {
                let alphabet = [b'a', b'b', b'A', b'B', b' ', b'\0', b'z', 0xC3, 0xA9];
                let len = (rng.next_u64() % 10) as usize;
                let bytes: std::vec::Vec<u8> = (0..len)
                    .map(|_| alphabet[(rng.next_u64() % alphabet.len() as u64) as usize])
                    .collect();
                Value::build_text(String::from_utf8_lossy(&bytes).into_owned())
            }
            _ => {
                let alphabet = [0x00, 0x01, 0xFF, 0x7F];
                let len = (rng.next_u64() % 10) as usize;
                let mut blob = try_vec![0u8; len].unwrap();
                for byte in blob.iter_mut() {
                    *byte = if rng.next_u64() % 2 == 0 {
                        alphabet[(rng.next_u64() % 4) as usize]
                    } else {
                        rng.next_u64() as u8
                    };
                }
                Value::Blob(blob)
            }
        }
    }

    fn random_key(rng: &mut ChaCha8Rng) -> KeyInfo {
        KeyInfo {
            sort_order: if rng.next_u64() % 2 == 0 {
                SortOrder::Asc
            } else {
                SortOrder::Desc
            },
            collation: match rng.next_u64() % 4 {
                0 => CollationSeq::Unset,
                1 => CollationSeq::Binary,
                2 => CollationSeq::NoCase,
                _ => CollationSeq::Rtrim,
            },
            nulls_order: match rng.next_u64() % 3 {
                0 => None,
                1 => Some(NullsOrder::First),
                _ => Some(NullsOrder::Last),
            },
        }
    }

    /// The byte order of two conditioned keys must equal the column-wise
    /// value comparison, including ties: a tie sends the records into the
    /// same partition, which is only right when the values are equal.
    #[test]
    fn fuzz_conditioned_key_order_matches_value_comparison() {
        let seed = seed();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut left_key = try_vec![0u8; 0].unwrap();
        let mut right_key = try_vec![0u8; 0].unwrap();
        for _ in 0..300_000 {
            let ncols = 1 + (rng.next_u64() % 3) as usize;
            let keys = [
                random_key(&mut rng),
                random_key(&mut rng),
                random_key(&mut rng),
            ];
            let left = [
                random_value(&mut rng),
                random_value(&mut rng),
                random_value(&mut rng),
            ];
            let right = if rng.next_u64() % 4 == 0 {
                left.clone()
            } else {
                [
                    random_value(&mut rng),
                    random_value(&mut rng),
                    random_value(&mut rng),
                ]
            };
            let left_refs = [
                left[0].as_value_ref(),
                left[1].as_value_ref(),
                left[2].as_value_ref(),
            ];
            let right_refs = [
                right[0].as_value_ref(),
                right[1].as_value_ref(),
                right[2].as_value_ref(),
            ];
            left_key.clear();
            right_key.clear();
            encode_sort_key(&left_refs[..ncols], &keys[..ncols], &mut left_key).unwrap();
            encode_sort_key(&right_refs[..ncols], &keys[..ncols], &mut right_key).unwrap();
            let expected = compare_immutable(
                left_refs[..ncols].iter(),
                right_refs[..ncols].iter(),
                &keys[..ncols],
            );
            assert_eq!(
                left_key.as_slice().cmp(right_key.as_slice()),
                expected,
                "seed {seed}: {left:?} vs {right:?} with keys {keys:?} encoded as {left_key:?} vs {right_key:?}"
            );
        }
    }

    #[test]
    fn integers_and_floats_interleave_exactly() {
        let key = KeyInfo {
            sort_order: SortOrder::Asc,
            collation: CollationSeq::Binary,
            nulls_order: None,
        };
        let ordered = [
            Value::from_f64(f64::NEG_INFINITY),
            Value::from_f64(-1e300),
            Value::from_i64(i64::MIN),
            Value::from_f64(-1.5),
            Value::from_i64(-1),
            Value::from_f64(-0.5),
            Value::from_i64(0),
            Value::from_f64(0.25),
            Value::from_i64(1),
            Value::from_i64(1 << 53),
            Value::from_i64((1 << 53) + 1),
            Value::from_f64(9.007199254740994e15),
            Value::from_i64(i64::MAX),
            Value::from_f64(I64_RANGE),
            Value::from_f64(f64::INFINITY),
        ];
        let mut previous = try_vec![0u8; 0].unwrap();
        let mut current = try_vec![0u8; 0].unwrap();
        for pair in ordered.windows(2) {
            previous.clear();
            current.clear();
            encode_sort_key(&[pair[0].as_value_ref()], &[key], &mut previous).unwrap();
            encode_sort_key(&[pair[1].as_value_ref()], &[key], &mut current).unwrap();
            assert_eq!(
                previous.as_slice().cmp(current.as_slice()),
                Ordering::Less,
                "{:?} must sort before {:?}",
                pair[0],
                pair[1]
            );
        }
        let mut zero = try_vec![0u8; 0].unwrap();
        let mut negative_zero = try_vec![0u8; 0].unwrap();
        encode_sort_key(&[Value::from_i64(0).as_value_ref()], &[key], &mut zero).unwrap();
        encode_sort_key(
            &[Value::from_f64(-0.0).as_value_ref()],
            &[key],
            &mut negative_zero,
        )
        .unwrap();
        assert_eq!(zero, negative_zero);
    }
}
