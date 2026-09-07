mod bitpacker;
mod blocked_bitpacker;
mod filter_vec;

use std::cmp::Ordering;

pub use crate::bitpacker::{BitPacker, BitUnpacker};
pub use crate::blocked_bitpacker::BlockedBitpacker;

/// Computes the number of bits that will be used for bitpacking.
///
/// In general the target is the minimum number of bits
/// required to express the amplitude given in argument.
///
/// e.g. If the amplitude is 10, we can store all ints on simply 4bits.
///
/// The logic is slightly more convoluted here as for optimization
/// reasons, we want to ensure that a value spawns over at most 8 bytes
/// of aligned bytes.
///
/// Spanning over 9 bytes is possible for instance, if we do
/// bitpacking with an amplitude of 63 bits.
/// In this case, the second int will start on bit
/// 63 (which belongs to byte 7) and ends at byte 15;
/// Hence 9 bytes (from byte 7 to byte 15 included).
///
/// To avoid this, we force the number of bits to 64bits
/// when the result is greater than `64-8 = 56 bits`.
///
/// Note that this only affects rare use cases spawning over
/// a very large range of values. Even in this case, it results
/// in an extra cost of at most 12% compared to the optimal
/// number of bits.
pub fn compute_num_bits(n: u64) -> u8 {
    let amplitude = (64u32 - n.leading_zeros()) as u8;
    if amplitude <= 64 - 8 { amplitude } else { 64 }
}

/// Computes the (min, max) of an iterator of `PartialOrd` values.
///
/// For values implementing `Ord` (in a way consistent to their `PartialOrd` impl),
/// this function behaves as expected.
///
/// For values with partial ordering, the behavior is non-trivial and may
/// depends on the order of the values.
/// For floats however, it simply returns the same results as if NaN were
/// skipped.
pub fn minmax<I, T>(mut vals: I) -> Option<(T, T)>
where
    I: Iterator<Item = T>,
    T: Copy + PartialOrd,
{
    let first_el = vals.find(|val| {
        // We use this to make sure we skip all NaN values when
        // working with a float type.
        val.partial_cmp(val) == Some(Ordering::Equal)
    })?;
    let mut min_so_far: T = first_el;
    let mut max_so_far: T = first_el;
    for val in vals {
        if val.partial_cmp(&min_so_far) == Some(Ordering::Less) {
            min_so_far = val;
        }
        if val.partial_cmp(&max_so_far) == Some(Ordering::Greater) {
            max_so_far = val;
        }
    }
    Some((min_so_far, max_so_far))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_num_bits() {
        assert_eq!(compute_num_bits(1), 1u8);
        assert_eq!(compute_num_bits(0), 0u8);
        assert_eq!(compute_num_bits(2), 2u8);
        assert_eq!(compute_num_bits(3), 2u8);
        assert_eq!(compute_num_bits(4), 3u8);
        assert_eq!(compute_num_bits(255), 8u8);
        assert_eq!(compute_num_bits(256), 9u8);
        assert_eq!(compute_num_bits(5_000_000_000), 33u8);
    }

    #[test]
    fn test_minmax_empty() {
        let vals: Vec<u32> = vec![];
        assert_eq!(minmax(vals.into_iter()), None);
    }

    #[test]
    fn test_minmax_one() {
        assert_eq!(minmax(vec![1].into_iter()), Some((1, 1)));
    }

    #[test]
    fn test_minmax_two() {
        assert_eq!(minmax(vec![1, 2].into_iter()), Some((1, 2)));
        assert_eq!(minmax(vec![2, 1].into_iter()), Some((1, 2)));
    }

    #[test]
    fn test_minmax_nan() {
        assert_eq!(
            minmax(vec![f64::NAN, 1f64, 2f64].into_iter()),
            Some((1f64, 2f64))
        );
        assert_eq!(
            minmax(vec![2f64, f64::NAN, 1f64].into_iter()),
            Some((1f64, 2f64))
        );
        assert_eq!(
            minmax(vec![2f64, 1f64, f64::NAN].into_iter()),
            Some((1f64, 2f64))
        );
    }

    #[test]
    fn test_minmax_inf() {
        assert_eq!(
            minmax(vec![f64::INFINITY, 1f64, 2f64].into_iter()),
            Some((1f64, f64::INFINITY))
        );
        assert_eq!(
            minmax(vec![-f64::INFINITY, 1f64, 2f64].into_iter()),
            Some((-f64::INFINITY, 2f64))
        );
        assert_eq!(
            minmax(vec![2f64, f64::INFINITY, 1f64].into_iter()),
            Some((1f64, f64::INFINITY))
        );
        assert_eq!(
            minmax(vec![2f64, 1f64, -f64::INFINITY].into_iter()),
            Some((-f64::INFINITY, 2f64))
        );
    }
}
