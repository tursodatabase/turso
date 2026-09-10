use std::fmt::Debug;
use std::marker::PhantomData;

use common::DateTime;

use super::MonotonicallyMappableToU128;
use crate::RowId;

/// Monotonic maps a value to u64 value space.
/// Monotonic mapping enables `PartialOrd` on u64 space without conversion to original space.
pub trait MonotonicallyMappableToU64: 'static + PartialOrd + Debug + Copy + Send + Sync {
    /// Converts a value to u64.
    ///
    /// Internally all fast field values are encoded as u64.
    fn to_u64(self) -> u64;

    /// Converts a value from u64
    ///
    /// Internally all fast field values are encoded as u64.
    /// **Note: To be used for converting encoded Term, Posting values.**
    fn from_u64(val: u64) -> Self;
}

/// Values need to be strictly monotonic mapped to a `Internal` value (u64 or u128) that can be
/// used in fast field codecs.
///
/// The monotonic mapping is required so that `PartialOrd` can be used on `Internal` without
/// converting to `External`.
///
/// All strictly monotonic functions are invertible because they are guaranteed to have a one-to-one
/// mapping from their range to their domain. The `inverse` method is required when opening a codec,
/// so a value can be converted back to its original domain (e.g. ip address or f64) from its
/// internal representation.
pub trait StrictlyMonotonicFn<External, Internal> {
    /// Strictly monotonically maps the value from External to Internal.
    fn mapping(&self, inp: External) -> Internal;
    /// Inverse of `mapping`. Maps the value from Internal to External.
    fn inverse(&self, out: Internal) -> External;
}

/// Inverts a strictly monotonic mapping from `StrictlyMonotonicFn<A, B>` to
/// `StrictlyMonotonicFn<B, A>`.
///
/// # Warning
///
/// This type comes with a footgun. A type being strictly monotonic does not impose that the inverse
/// mapping is strictly monotonic over the entire space External. e.g. a -> a * 2. Use at your own
/// risks.
pub(crate) struct StrictlyMonotonicMappingInverter<T> {
    orig_mapping: T,
}
impl<T> From<T> for StrictlyMonotonicMappingInverter<T> {
    fn from(orig_mapping: T) -> Self {
        Self { orig_mapping }
    }
}

impl<From, To, T> StrictlyMonotonicFn<To, From> for StrictlyMonotonicMappingInverter<T>
where T: StrictlyMonotonicFn<From, To>
{
    #[inline(always)]
    fn mapping(&self, val: To) -> From {
        self.orig_mapping.inverse(val)
    }

    #[inline(always)]
    fn inverse(&self, val: From) -> To {
        self.orig_mapping.mapping(val)
    }
}

/// Applies the strictly monotonic mapping from `T` without any additional changes.
pub(crate) struct StrictlyMonotonicMappingToInternal<T> {
    _phantom: PhantomData<T>,
}

impl<T> StrictlyMonotonicMappingToInternal<T> {
    pub(crate) fn new() -> StrictlyMonotonicMappingToInternal<T> {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<External: MonotonicallyMappableToU128, T: MonotonicallyMappableToU128>
    StrictlyMonotonicFn<External, u128> for StrictlyMonotonicMappingToInternal<T>
where T: MonotonicallyMappableToU128
{
    #[inline(always)]
    fn mapping(&self, inp: External) -> u128 {
        External::to_u128(inp)
    }

    #[inline(always)]
    fn inverse(&self, out: u128) -> External {
        External::from_u128(out)
    }
}

impl<External: MonotonicallyMappableToU64, T: MonotonicallyMappableToU64>
    StrictlyMonotonicFn<External, u64> for StrictlyMonotonicMappingToInternal<T>
where T: MonotonicallyMappableToU64
{
    #[inline(always)]
    fn mapping(&self, inp: External) -> u64 {
        External::to_u64(inp)
    }

    #[inline(always)]
    fn inverse(&self, out: u64) -> External {
        External::from_u64(out)
    }
}

impl MonotonicallyMappableToU64 for u64 {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        self
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        val
    }
}

impl MonotonicallyMappableToU64 for i64 {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        common::i64_to_u64(self)
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        common::u64_to_i64(val)
    }
}

impl MonotonicallyMappableToU64 for DateTime {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        common::i64_to_u64(self.into_timestamp_nanos())
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        DateTime::from_timestamp_nanos(common::u64_to_i64(val))
    }
}

impl MonotonicallyMappableToU64 for bool {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        u64::from(self)
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        val > 0
    }
}

impl MonotonicallyMappableToU64 for RowId {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        u64::from(self)
    }

    #[inline(always)]
    fn from_u64(val: u64) -> RowId {
        val as RowId
    }
}

// TODO remove me.
// Tantivy should refuse NaN values and work with NotNaN internally.
impl MonotonicallyMappableToU64 for f64 {
    #[inline(always)]
    fn to_u64(self) -> u64 {
        common::f64_to_u64(self)
    }

    #[inline(always)]
    fn from_u64(val: u64) -> Self {
        common::u64_to_f64(val)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn strictly_monotonic_test() {
        // identity mapping
        test_round_trip(&StrictlyMonotonicMappingToInternal::<u64>::new(), 100u64);
        // round trip to i64
        test_round_trip(&StrictlyMonotonicMappingToInternal::<i64>::new(), 100u64);
        // TODO
        // identity mapping
        // test_round_trip(&StrictlyMonotonicMappingToInternal::<u128>::new(), 100u128);
    }

    fn test_round_trip<T: StrictlyMonotonicFn<K, L>, K: std::fmt::Debug + Eq + Copy, L>(
        mapping: &T,
        test_val: K,
    ) {
        assert_eq!(mapping.inverse(mapping.mapping(test_val)), test_val);
    }
}
