#![allow(clippy::len_without_is_empty)]

use std::ops::Deref;

pub use byteorder::LittleEndian as Endianness;

mod bitset;
pub mod bounds;
mod byte_count;
mod datetime;
pub mod file_slice;
mod group_by;
pub mod json_path_writer;
mod serialize;
mod vint;
mod writer;
pub use bitset::*;
pub use byte_count::ByteCount;
pub use datetime::{DateTime, DateTimePrecision};
pub use group_by::GroupByIteratorExtended;
pub use json_path_writer::JsonPathWriter;
pub use ownedbytes::{OwnedBytes, StableDeref};
pub use serialize::{BinarySerializable, DeserializeFrom, FixedSize};
pub use vint::{
    VInt, VIntU128, read_u32_vint, read_u32_vint_no_advance, serialize_vint_u32, write_u32_vint,
};
pub use writer::{AntiCallToken, CountingWriter, TerminatingWrite};

/// Has length trait
pub trait HasLen {
    /// Return length
    fn len(&self) -> usize;

    /// Returns true iff empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Deref<Target = [u8]>> HasLen for T {
    fn len(&self) -> usize {
        self.deref().len()
    }
}

const HIGHEST_BIT: u64 = 1 << 63;

/// Maps a `i64` to `u64`
///
/// For simplicity, tantivy internally handles `i64` as `u64`.
/// The mapping is defined by this function.
///
/// Maps `i64` to `u64` so that
/// `-2^63 .. 2^63-1` is mapped
///     to
/// `0 .. 2^64-1`
/// in that order.
///
/// This is more suited than simply casting (`val as u64`)
/// because of bitpacking.
///
/// Imagine a list of `i64` ranging from -10 to 10.
/// When casting negative values, the negative values are projected
/// to values over 2^63, and all values end up requiring 64 bits.
///
/// # See also
/// The reverse mapping is [`u64_to_i64()`].
#[inline]
pub fn i64_to_u64(val: i64) -> u64 {
    (val as u64) ^ HIGHEST_BIT
}

/// Reverse the mapping given by [`i64_to_u64()`].
#[inline]
pub fn u64_to_i64(val: u64) -> i64 {
    (val ^ HIGHEST_BIT) as i64
}

/// Maps a `f64` to `u64`
///
/// For simplicity, tantivy internally handles `f64` as `u64`.
/// The mapping is defined by this function.
///
/// Maps `f64` to `u64` in a monotonic manner, so that bytes lexical order is preserved.
///
/// This is more suited than simply casting (`val as u64`)
/// which would truncate the result
///
/// # Reference
///
/// Daniel Lemire's [blog post](https://lemire.me/blog/2020/12/14/converting-floating-point-numbers-to-integers-while-preserving-order/)
/// explains the mapping in a clear manner.
///
/// # See also
/// The reverse mapping is [`u64_to_f64()`].
#[inline]
pub fn f64_to_u64(val: f64) -> u64 {
    let bits = val.to_bits();
    if val.is_sign_positive() {
        bits ^ HIGHEST_BIT
    } else {
        !bits
    }
}

/// Reverse the mapping given by [`f64_to_u64()`].
#[inline]
pub fn u64_to_f64(val: u64) -> f64 {
    f64::from_bits(if val & HIGHEST_BIT != 0 {
        val ^ HIGHEST_BIT
    } else {
        !val
    })
}

/// Replaces a given byte in the `bytes` slice of bytes.
///
/// This function assumes that the needle is rarely contained in the bytes string
/// and offers a fast path if the needle is not present.
#[inline]
pub fn replace_in_place(needle: u8, replacement: u8, bytes: &mut [u8]) {
    if !bytes.contains(&needle) {
        return;
    }
    for b in bytes {
        if *b == needle {
            *b = replacement;
        }
    }
}

#[cfg(test)]
pub(crate) mod test {

    use proptest::prelude::*;

    use super::{f64_to_u64, i64_to_u64, u64_to_f64, u64_to_i64};

    fn test_i64_converter_helper(val: i64) {
        assert_eq!(u64_to_i64(i64_to_u64(val)), val);
    }

    fn test_f64_converter_helper(val: f64) {
        assert_eq!(u64_to_f64(f64_to_u64(val)), val);
    }

    proptest! {
        #[test]
        fn test_f64_converter_monotonicity_proptest((left, right) in (proptest::num::f64::NORMAL, proptest::num::f64::NORMAL)) {
            let left_u64 = f64_to_u64(left);
            let right_u64 = f64_to_u64(right);
            assert_eq!(left_u64 < right_u64,  left < right);
        }
    }

    #[test]
    fn test_i64_converter() {
        assert_eq!(i64_to_u64(i64::MIN), u64::MIN);
        assert_eq!(i64_to_u64(i64::MAX), u64::MAX);
        test_i64_converter_helper(0i64);
        test_i64_converter_helper(i64::MIN);
        test_i64_converter_helper(i64::MAX);
        for i in -1000i64..1000i64 {
            test_i64_converter_helper(i);
        }
    }

    #[test]
    fn test_f64_converter() {
        test_f64_converter_helper(f64::INFINITY);
        test_f64_converter_helper(f64::NEG_INFINITY);
        test_f64_converter_helper(0.0);
        test_f64_converter_helper(-0.0);
        test_f64_converter_helper(1.0);
        test_f64_converter_helper(-1.0);
    }

    #[test]
    fn test_f64_order() {
        assert!(
            !(f64_to_u64(f64::NEG_INFINITY)..f64_to_u64(f64::INFINITY))
                .contains(&f64_to_u64(f64::NAN))
        ); // nan is not a number
        assert!(f64_to_u64(1.5) > f64_to_u64(1.0)); // same exponent, different mantissa
        assert!(f64_to_u64(2.0) > f64_to_u64(1.0)); // same mantissa, different exponent
        assert!(f64_to_u64(2.0) > f64_to_u64(1.5)); // different exponent and mantissa
        assert!(f64_to_u64(1.0) > f64_to_u64(-1.0)); // pos > neg
        assert!(f64_to_u64(-1.5) < f64_to_u64(-1.0));
        assert!(f64_to_u64(-2.0) < f64_to_u64(1.0));
        assert!(f64_to_u64(-2.0) < f64_to_u64(-1.5));
    }

    #[test]
    fn test_replace_in_place() {
        let test_aux = |before_replacement: &[u8], expected: &[u8]| {
            let mut bytes: Vec<u8> = before_replacement.to_vec();
            super::replace_in_place(b'b', b'c', &mut bytes);
            assert_eq!(&bytes[..], expected);
        };
        test_aux(b"", b"");
        test_aux(b"b", b"c");
        test_aux(b"baaa", b"caaa");
        test_aux(b"aaab", b"aaac");
        test_aux(b"aaabaa", b"aaacaa");
        test_aux(b"aaaaaa", b"aaaaaa");
        test_aux(b"bbbb", b"cccc");
    }
}
