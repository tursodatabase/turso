//! Binary sort-key encoding for the sorter fast path.
//!
//! This module builds a byte sequence such that lexicographic byte comparison
//! matches SQLite-compatible value ordering for binary collations and ASC/DESC
//! directions. The encoding is used only when all key columns use the binary
//! collation; it is not a general-purpose collation implementation.
//!
//! High-level format (per value):
//! - A 1-byte type tag that establishes cross-type ordering.
//! - Type-specific payload that preserves ordering within the type.
//! - For DESC columns, all bytes of that value are bitwise inverted.
//!
//! Text and blob payloads are encoded with a zero-escaping scheme so that
//! concatenation remains prefix-safe and order-preserving:
//! - `0x00` -> `0x00 0xFF`
//! - end-of-value marker -> `0x00 0x00`
//!
//! Numeric payloads are normalized into a sign/magnitude representation (sign + absolute value)
//! so that lexicographic comparison matches numeric order (including distinct handling
//! for NaN and signed zero).
//!
//! NOTE: ValueRef float-vs-float ordering unwraps `partial_cmp` and will panic
//! on NaN. To stay aligned with that behavior, the sorter fast path must not be
//! used when NaN can appear in key columns (tests normalize NaN away).
use bumpalo::collections::Vec as BumpVec;
use bumpalo::Bump;
use turso_parser::ast::SortOrder;

use crate::types::{KeyInfo, ValueRef};

/// Type ordering tags are chosen so byte-wise comparison matches ValueRef's type ordering.
/// Order is: NULL < numeric < text < blob (same as ValueRef::partial_cmp).
/// Type ordering tag: NULL sorts before all other types.
const TAG_NULL: u8 = 0x00;
/// Type ordering tag: numeric values (integers and floats).
const TAG_NUMERIC: u8 = 0x10;
/// Type ordering tag: text values (binary collation).
const TAG_TEXT: u8 = 0x20;
/// Type ordering tag: blobs (bytewise ordering).
const TAG_BLOB: u8 = 0x30;

/// Numeric subtag: NaN (orders before other numeric values).
/// Note: ValueRef panics on float-vs-float NaN comparisons, so NaN should not
/// reach this fast path in practice.
const NUM_SUBTAG_NAN: u8 = 0x00;
/// Numeric subtag: negative finite values.
const NUM_SUBTAG_NEG: u8 = 0x01;
/// Numeric subtag: zero (covers both +0.0 and -0.0).
const NUM_SUBTAG_ZERO: u8 = 0x02;
/// Numeric subtag: positive finite values.
const NUM_SUBTAG_POS: u8 = 0x03;

/// Bias for the normalized exponent stored as a u16.
/// A bias just shifts negative exponents into unsigned space so byte ordering works,
/// i.e. negative exponents sort earlier.
const EXP_BIAS: i32 = 32768;
/// Total bits in a u64.
const U64_BITS: i16 = 64;
/// IEEE754 f64 exponent bits.
const F64_EXP_BITS: u64 = 0x7FF;
/// IEEE754 f64 fraction bits mask (52 bits).
const F64_FRAC_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
/// IEEE754 f64 mantissa bits count.
const F64_MANTISSA_BITS: i16 = 52;
/// IEEE754 f64 exponent bias (the fixed offset used in the stored exponent field).
const F64_EXP_BIAS: i32 = 1023;
/// Minimum unbiased exponent used by very small values (1 - bias).
const F64_EXP_MIN: i32 = 1 - F64_EXP_BIAS;

/// Normalized numeric kinds used by the sort-key encoding.
#[derive(Clone, Copy)]
enum NumericKind {
    /// NaN payloads.
    NaN,
    /// Zero payloads.
    Zero,
    /// Negative finite numbers.
    Negative,
    /// Positive finite numbers.
    Positive,
}

/// Normalized numeric representation for ordering.
///
/// The `(exponent, mantissa)` pair is chosen so that lexicographic ordering
/// matches numeric ordering after applying sign-specific bit inversion.
/// Here "mantissa" means the significant bits of the absolute value, left-aligned to bit 63.
/// The exponent records how much we shifted to do that (base-2, not decimal).
/// The tag/subtag carry type and sign; the pair carries magnitude and scale.
#[derive(Clone, Copy)]
struct NumericRepr {
    kind: NumericKind,
    exponent: i16,
    mantissa: u64,
}

/// Encode a sort key into a heap vector (tests only).
///
/// In production, prefer `encode_sort_key_in` to write directly into the arena.
#[cfg(test)]
pub(super) fn encode_sort_key(values: &[ValueRef<'_>], key_info: &[KeyInfo]) -> Vec<u8> {
    debug_assert_eq!(values.len(), key_info.len());
    let mut buf = Vec::new();
    encode_sort_key_into(&mut buf, values, key_info);
    buf
}

/// Encode a sort key directly into a bump arena.
///
/// The returned slice is arena-backed and valid until the arena is reset.
pub(super) fn encode_sort_key_in<'a>(
    arena: &'a Bump,
    values: &[ValueRef<'_>],
    key_info: &[KeyInfo],
) -> &'a [u8] {
    debug_assert_eq!(values.len(), key_info.len());
    let mut buf = BumpVec::new_in(arena);
    encode_sort_key_into(&mut buf, values, key_info);
    buf.into_bump_slice()
}

/// Shared encoding path for heap and bump-backed buffers.
fn encode_sort_key_into<B: ByteBuf>(buf: &mut B, values: &[ValueRef<'_>], key_info: &[KeyInfo]) {
    for (value, key_info) in values.iter().zip(key_info.iter()) {
        // Track the start so DESC inversion is scoped to this value only.
        // (We must not invert bytes from the next column.)
        let start = buf.len();
        encode_value(value, buf);
        if key_info.sort_order == SortOrder::Desc {
            // DESC is handled by bitwise inversion (cheap and reversible).
            invert_bytes(buf.slice_from_mut(start));
        }
    }
}

/// Encode a single value with a type tag and type-specific payload.
fn encode_value<B: ByteBuf>(value: &ValueRef<'_>, buf: &mut B) {
    match value {
        ValueRef::Null => buf.push(TAG_NULL),
        ValueRef::Integer(value) => encode_numeric(numeric_from_int(*value), buf),
        ValueRef::Float(value) => encode_numeric(numeric_from_float(*value), buf),
        ValueRef::Text(text) => {
            // Binary collation: use raw bytes with escaping.
            // The escaping makes concatenated values prefix-safe.
            buf.push(TAG_TEXT);
            encode_bytes_escaped(text.value.as_bytes(), buf);
        }
        ValueRef::Blob(blob) => {
            // Blob sort is pure byte-wise compare.
            // We still escape 0x00 so the key terminator stays unambiguous.
            buf.push(TAG_BLOB);
            encode_bytes_escaped(blob, buf);
        }
    }
}

/// Encode a normalized numeric representation.
/// Layout: TAG_NUMERIC, subtag, exponent (big-endian), mantissa (big-endian).
fn encode_numeric<B: ByteBuf>(repr: NumericRepr, buf: &mut B) {
    buf.push(TAG_NUMERIC);
    match repr.kind {
        NumericKind::NaN => buf.push(NUM_SUBTAG_NAN),
        NumericKind::Zero => buf.push(NUM_SUBTAG_ZERO),
        NumericKind::Negative | NumericKind::Positive => {
            // The subtag splits negatives from positives so they don't interleave.
            buf.push(match repr.kind {
                NumericKind::Negative => NUM_SUBTAG_NEG,
                _ => NUM_SUBTAG_POS,
            });
            // Bias exponent into unsigned space so lexicographic compare matches numeric order.
            // Positive exponents become values above EXP_BIAS, negative exponents below;
            // -> negative exponents sort earlier.
            let mut exponent = (repr.exponent as i32 + EXP_BIAS) as u16;
            let mut mantissa = repr.mantissa;
            if matches!(repr.kind, NumericKind::Negative) {
                // Flip ordering within negatives so "more negative" sorts earlier.
                exponent = !exponent;
                mantissa = !mantissa;
            }
            // Big-endian so lexicographic byte order matches numeric order.
            buf.extend_from_slice(&exponent.to_be_bytes());
            buf.extend_from_slice(&mantissa.to_be_bytes());
        }
    }
}

/// Convert an integer to the normalized numeric representation.
fn numeric_from_int(value: i64) -> NumericRepr {
    if value == 0 {
        return NumericRepr {
            kind: NumericKind::Zero,
            exponent: 0,
            mantissa: 0,
        };
    }

    let negative = value < 0;
    // Use wrapping_neg to handle i64::MIN, which cannot be negated normally.
    let abs = if negative {
        value.wrapping_neg() as u64
    } else {
        value as u64
    };
    // We only use magnitude (absolute value) here; sign is stored separately in the tag/subtag.
    // Aligning the highest set bit to the top bit (index 63) makes byte-wise compare work.
    let top_bit_index = U64_BITS - 1;

    // Normalize magnitude so the highest set bit lands at bit 63 (sortable as bytes).
    let highest_bit = top_bit_index - abs.leading_zeros() as i16;
    let shift = top_bit_index - highest_bit;
    let mantissa = abs << shift;
    let exponent = -shift; // counts how much we left-shifted

    NumericRepr {
        kind: if negative {
            NumericKind::Negative
        } else {
            NumericKind::Positive
        },
        exponent,
        mantissa,
    }
}

/// Convert an IEEE754 float to the normalized numeric representation.
///
/// NaN and signed zero are handled explicitly to match SQLite comparison rules.
fn numeric_from_float(value: f64) -> NumericRepr {
    if value.is_nan() {
        return NumericRepr {
            kind: NumericKind::NaN,
            exponent: 0,
            mantissa: 0,
        };
    }

    if value == 0.0 {
        return NumericRepr {
            kind: NumericKind::Zero,
            exponent: 0,
            mantissa: 0,
        };
    }

    // Split the IEEE754 bits into sign/exponent/fraction for normalization.
    // We reassemble into (exponent, mantissa) so lexicographic order matches numeric order.
    let bits = value.to_bits();
    let negative = (bits >> 63) != 0;
    let exp_bits = ((bits >> F64_MANTISSA_BITS) & F64_EXP_BITS) as i32;
    let frac = bits & F64_FRAC_MASK;

    let top_bit_index = U64_BITS - 1;
    let (mantissa, exponent) = if exp_bits == 0 {
        // Very small exponent: no implicit leading 1, normalize fraction like integers.
        let highest_bit = top_bit_index - frac.leading_zeros() as i16;
        let shift = top_bit_index - highest_bit;
        let mantissa = frac << shift;
        // Actual exponent is (1 - bias - mantissa_bits), then adjust for shift.
        let exponent = (F64_EXP_MIN - F64_MANTISSA_BITS as i32) - shift as i32;
        (mantissa, exponent)
    } else {
        // Normal: IEEE754 omits the leading 1 from storage; put it back, then left-align to bit 63.
        let align_shift = top_bit_index - F64_MANTISSA_BITS;
        let mantissa = ((1_u64 << F64_MANTISSA_BITS) | frac) << align_shift;
        // Exponent is unbiased, then adjusted for left-aligning the mantissa.
        let exponent = exp_bits - F64_EXP_BIAS - top_bit_index as i32;
        (mantissa, exponent)
    };

    NumericRepr {
        kind: if negative {
            NumericKind::Negative
        } else {
            NumericKind::Positive
        },
        exponent: exponent as i16,
        mantissa,
    }
}

/// Escape 0x00 bytes and append a keyterminator so concatenated keys remain sortable.
fn encode_bytes_escaped<B: ByteBuf>(bytes: &[u8], buf: &mut B) {
    for &b in bytes {
        if b == 0 {
            // 0x00 is reserved for the key terminator; escape to preserve prefix ordering.
            buf.push(0);
            buf.push(0xFF);
        } else {
            buf.push(b);
        }
    }
    // Key terminator ensures shorter values sort before their longer extensions.
    buf.push(0);
    buf.push(0);
}

/// Bitwise invert bytes for DESC ordering (bytewise compare becomes reversed).
fn invert_bytes(bytes: &mut [u8]) {
    for b in bytes {
        *b = !*b;
    }
}

/// Minimal buffer abstraction to share encoding logic between heap and bump vecs.
trait ByteBuf {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, bytes: &[u8]);
    fn len(&self) -> usize;
    fn slice_from_mut(&mut self, start: usize) -> &mut [u8];
}

impl ByteBuf for Vec<u8> {
    fn push(&mut self, byte: u8) {
        self.push(byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn slice_from_mut(&mut self, start: usize) -> &mut [u8] {
        &mut self[start..]
    }
}

impl<'a> ByteBuf for BumpVec<'a, u8> {
    fn push(&mut self, byte: u8) {
        self.push(byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn slice_from_mut(&mut self, start: usize) -> &mut [u8] {
        &mut self.as_mut_slice()[start..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::collate::CollationSeq;
    use crate::types::{compare_immutable_single, AsValueRef, Text, Value};
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use turso_parser::ast::SortOrder;

    fn key_for(value: &Value, sort_order: SortOrder) -> Vec<u8> {
        let key_info = [KeyInfo {
            sort_order,
            collation: CollationSeq::Binary,
        }];
        let value_ref = value.as_value_ref();
        encode_sort_key(std::slice::from_ref(&value_ref), &key_info)
    }

    fn assert_key_matches_value_order(values: &[Value]) {
        for left in values {
            for right in values {
                let value_cmp = left.as_value_ref().cmp(&right.as_value_ref());
                let key_cmp = key_for(left, SortOrder::Asc).cmp(&key_for(right, SortOrder::Asc));
                assert_eq!(
                    value_cmp, key_cmp,
                    "mismatch: left={left:?} right={right:?}"
                );
            }
        }
    }

    #[test]
    fn sort_key_matches_value_order_edge_cases() {
        let two_63 = (1u64 << 63) as f64;
        let values = vec![
            Value::Null,
            Value::Integer(i64::MIN),
            Value::Integer(-1),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(i64::MAX),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(-two_63 * 2.0),
            Value::Float(-two_63),
            Value::Float(-two_63 + 0.5),
            Value::Float(-1.0),
            Value::Float(-0.0),
            Value::Float(0.0),
            Value::Float(0.5),
            Value::Float(1.0),
            Value::Float(1.5),
            Value::Float(two_63 - 1.0),
            Value::Float(two_63),
            Value::Float(two_63 * 2.0),
            Value::Float(f64::INFINITY),
            Value::Text(Text::new("")),
            Value::Text(Text::new("a")),
            Value::Text(Text::new("a\u{0}b")),
            Value::Blob(Vec::new()),
            Value::Blob(vec![0]),
            Value::Blob(vec![0, 1]),
        ];

        assert_key_matches_value_order(&values);
    }

    #[test]
    fn sort_key_desc_matches_value_order() {
        let values = vec![
            Value::Null,
            Value::Integer(-5),
            Value::Integer(0),
            Value::Integer(7),
            Value::Float(-1.25),
            Value::Float(0.0),
            Value::Float(3.5),
            Value::Text(Text::new("a")),
            Value::Text(Text::new("b")),
            Value::Blob(vec![1, 2, 3]),
        ];

        for left in &values {
            for right in &values {
                let value_cmp = left.as_value_ref().cmp(&right.as_value_ref()).reverse();
                let key_cmp = key_for(left, SortOrder::Desc).cmp(&key_for(right, SortOrder::Desc));
                assert_eq!(
                    value_cmp, key_cmp,
                    "desc mismatch: left={left:?} right={right:?}"
                );
            }
        }
    }

    #[test]
    fn sort_key_multi_column_matches_value_order() {
        let key_info = [
            KeyInfo {
                sort_order: SortOrder::Asc,
                collation: CollationSeq::Binary,
            },
            KeyInfo {
                sort_order: SortOrder::Desc,
                collation: CollationSeq::Binary,
            },
        ];

        let rows = vec![
            vec![Value::Integer(1), Value::Text(Text::new("b"))],
            vec![Value::Integer(1), Value::Text(Text::new("a"))],
            vec![Value::Integer(0), Value::Text(Text::new("z"))],
            vec![Value::Integer(2), Value::Text(Text::new("c"))],
        ];

        let mut keys = Vec::new();
        for row in &rows {
            let value_refs: Vec<_> = row.iter().map(|v| v.as_value_ref()).collect();
            keys.push(encode_sort_key(&value_refs, &key_info));
        }

        for (i, left) in rows.iter().enumerate() {
            for (j, right) in rows.iter().enumerate() {
                let value_cmp = left
                    .iter()
                    .zip(right.iter())
                    .zip(key_info.iter())
                    .find_map(|((l, r), info)| {
                        let cmp = compare_immutable_single(
                            l.as_value_ref(),
                            r.as_value_ref(),
                            info.collation,
                        );
                        if cmp.is_eq() {
                            None
                        } else {
                            Some(match info.sort_order {
                                SortOrder::Asc => cmp,
                                SortOrder::Desc => cmp.reverse(),
                            })
                        }
                    })
                    .unwrap_or(std::cmp::Ordering::Equal);
                let key_cmp = keys[i].cmp(&keys[j]);
                assert_eq!(
                    value_cmp, key_cmp,
                    "multi-column mismatch: left={left:?} right={right:?}"
                );
            }
        }
    }

    #[derive(Clone, Debug)]
    struct QCValue(Value);

    impl Arbitrary for QCValue {
        fn arbitrary(g: &mut Gen) -> Self {
            let tag = u8::arbitrary(g) % 5;
            let value = match tag {
                0 => Value::Null,
                1 => Value::Integer(i64::arbitrary(g)),
                2 => Value::Float(random_f64(g)),
                3 => Value::Text(Text::new(random_ascii_string(g))),
                _ => Value::Blob(random_bytes(g, 32)),
            };
            Self(value)
        }
    }

    #[derive(Clone, Debug)]
    struct QCRowPair {
        left: Vec<Value>,
        right: Vec<Value>,
        sort_orders: Vec<SortOrder>,
    }

    impl Arbitrary for QCRowPair {
        fn arbitrary(g: &mut Gen) -> Self {
            let len = (usize::arbitrary(g) % 4) + 1;
            let mut left = Vec::with_capacity(len);
            let mut right = Vec::with_capacity(len);
            let mut sort_orders = Vec::with_capacity(len);

            for _ in 0..len {
                left.push(QCValue::arbitrary(g).0);
                right.push(QCValue::arbitrary(g).0);
                sort_orders.push(if bool::arbitrary(g) {
                    SortOrder::Asc
                } else {
                    SortOrder::Desc
                });
            }

            Self {
                left,
                right,
                sort_orders,
            }
        }
    }

    #[quickcheck]
    fn sort_key_matches_value_order_property(left: QCValue, right: QCValue) -> bool {
        let left = left.0;
        let right = right.0;
        let value_cmp = left.as_value_ref().cmp(&right.as_value_ref());
        let key_cmp = key_for(&left, SortOrder::Asc).cmp(&key_for(&right, SortOrder::Asc));
        value_cmp == key_cmp
    }

    #[quickcheck]
    fn sort_key_matches_value_order_rows(pair: QCRowPair) -> bool {
        let key_info: Vec<KeyInfo> = pair
            .sort_orders
            .iter()
            .map(|&sort_order| KeyInfo {
                sort_order,
                collation: CollationSeq::Binary,
            })
            .collect();

        let left_refs: Vec<_> = pair.left.iter().map(|v| v.as_value_ref()).collect();
        let right_refs: Vec<_> = pair.right.iter().map(|v| v.as_value_ref()).collect();

        let left_key = encode_sort_key(&left_refs, &key_info);
        let right_key = encode_sort_key(&right_refs, &key_info);
        let key_cmp = left_key.cmp(&right_key);

        let value_cmp = compare_rows(&pair.left, &pair.right, &key_info);
        key_cmp == value_cmp
    }

    fn compare_rows(left: &[Value], right: &[Value], key_info: &[KeyInfo]) -> std::cmp::Ordering {
        for ((l, r), info) in left.iter().zip(right.iter()).zip(key_info.iter()) {
            let cmp = compare_immutable_single(l.as_value_ref(), r.as_value_ref(), info.collation);
            if !cmp.is_eq() {
                return match info.sort_order {
                    SortOrder::Asc => cmp,
                    SortOrder::Desc => cmp.reverse(),
                };
            }
        }
        std::cmp::Ordering::Equal
    }

    fn random_f64(g: &mut Gen) -> f64 {
        let bits = u64::arbitrary(g);
        let value = f64::from_bits(bits);
        if value.is_nan() {
            0.0
        } else {
            value
        }
    }

    fn random_ascii_string(g: &mut Gen) -> String {
        let len = usize::arbitrary(g) % 32;
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            let byte = u8::arbitrary(g) % 128;
            bytes.push(byte);
        }
        String::from_utf8(bytes).expect("ASCII should be valid UTF-8")
    }

    fn random_bytes(g: &mut Gen, max_len: usize) -> Vec<u8> {
        let len = usize::arbitrary(g) % (max_len + 1);
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            bytes.push(u8::arbitrary(g));
        }
        bytes
    }
}
