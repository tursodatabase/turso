//! Filepath: `src/tree/range/iterator/range_bound.rs`
//!
//! Range bound type for scan operations.

use std::ops::Bound;

use crate::key::IKEY_SIZE;

/// Range bound for scan operations.
///
/// Specifies the start or end of a key range for scanning.
///
/// # Variants
///
/// - [`Unbounded`](RangeBound::Unbounded): No bound (all keys)
/// - [`Included`](RangeBound::Included): Include the specified key
/// - [`Excluded`](RangeBound::Excluded): Exclude the specified key
///
/// # Example
///
/// ```rust,ignore
/// // Scan from "aaa" (inclusive) to "zzz" (exclusive)
/// let start = RangeBound::Included(b"aaa");
/// let end = RangeBound::Excluded(b"zzz");
///
/// for entry in tree.range(start, end, &guard) {
///     // entry.key will be >= "aaa" and < "zzz"
/// }
///```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeBound<'a> {
    /// No bound - start from minimum or continue to maximum.
    Unbounded,

    /// Inclusive bound - include the specified key.
    Included(&'a [u8]),

    /// Exclusive bound - exclude the specified key.
    Excluded(&'a [u8]),
}

impl<'a> RangeBound<'a> {
    /// Check if a key is within this bound (for end bound checking).
    ///
    /// For end bounds:
    /// - `Unbounded`: all keys are within
    /// - `Included(k)`: keys <= k are within
    /// - `Excluded(k)`: keys < k are within
    ///
    /// # Arguments
    ///
    /// - `key`: The key to check
    ///
    /// # Returns
    ///
    /// `true` if the key is within the bound.
    #[must_use]
    #[inline(always)]
    pub fn contains(&self, key: &[u8]) -> bool {
        match self {
            RangeBound::Unbounded => true,

            RangeBound::Included(bound) => key <= *bound,

            RangeBound::Excluded(bound) => key < *bound,
        }
    }

    /// Convert to `(start_key, emit_equal)` parameters for `find_initial`.
    ///
    /// For start bounds:
    /// - `Unbounded`: empty key, `emit_equal = true`
    /// - `Included(k)`: key k, `emit_equal = true`
    /// - `Excluded(k)`: key k, `emit_equal = false`
    ///
    /// # Returns
    ///
    /// Tuple of (key bytes, `emit_equal` flag).
    #[must_use]
    #[inline(always)]
    pub const fn to_start_params(&self) -> (&'a [u8], bool) {
        match self {
            RangeBound::Unbounded => (&[], true),

            RangeBound::Included(k) => (*k, true),

            RangeBound::Excluded(k) => (*k, false),
        }
    }

    /// Check if this is an unbounded bound.
    #[must_use]
    #[inline(always)]
    pub const fn is_unbounded(&self) -> bool {
        matches!(self, RangeBound::Unbounded)
    }

    /// Get the bound key if this is a bounded bound.
    ///
    /// # Note
    ///
    /// This method is provided for API completeness and may be useful for
    /// external callers who need to inspect bounds programmatically.
    #[must_use]
    #[inline(always)]
    pub const fn key(&self) -> Option<&'a [u8]> {
        match self {
            RangeBound::Unbounded => None,

            RangeBound::Included(k) | RangeBound::Excluded(k) => Some(*k),
        }
    }

    /// Check if a key is within this bound for reverse iteration (start bound).
    ///
    /// For start bounds used in reverse iteration:
    /// - `Unbounded`: all keys are within
    /// - `Included(k)`: keys >= k are within
    /// - `Excluded(k)`: keys > k are within
    ///
    /// This is the reverse of `contains()` which is for end bounds.
    #[must_use]
    #[inline(always)]
    pub fn contains_reverse(&self, key: &[u8]) -> bool {
        match self {
            RangeBound::Unbounded => true,

            RangeBound::Included(bound) => key >= *bound,

            RangeBound::Excluded(bound) => key > *bound,
        }
    }

    /// Extract the ikey from a bound for fast comparison.
    ///
    /// Returns `None` for `Unbounded`, otherwise extracts the first 8 bytes
    /// as a big-endian u64 (zero-padded if key is shorter).
    ///
    /// This is used by value-only scans to perform approximate end bound checks
    /// without building the full key.
    #[must_use]
    #[inline]
    pub fn extract_ikey(&self) -> Option<u64> {
        self.extract_ikey_at(0)
    }

    /// Extract the ikey at a specific key offset for fast layer-aligned checks.
    ///
    /// `offset` is the number of key bytes already consumed by trie descent.
    /// Returns `None` for `Unbounded`.
    #[must_use]
    #[inline]
    pub fn extract_ikey_at(&self, offset: usize) -> Option<u64> {
        match self {
            RangeBound::Unbounded => None,

            RangeBound::Included(bound) | RangeBound::Excluded(bound) => {
                Some(Self::key_to_ikey_at(bound, offset))
            }
        }
    }

    /// Convert key bytes to ikey (first 8 bytes as big-endian u64).
    #[inline]
    #[expect(clippy::indexing_slicing, reason = "length is checked before indexing")]
    fn key_to_ikey_at(key: &[u8], offset: usize) -> u64 {
        if offset >= key.len() {
            return 0;
        }

        let remaining = key.len() - offset;
        if remaining >= IKEY_SIZE {
            // SAFETY: length checked above
            #[expect(clippy::expect_used, reason = "length is checked")]
            u64::from_be_bytes(
                key[offset..offset + IKEY_SIZE]
                    .try_into()
                    .expect("slice is 8 bytes"),
            )
        } else {
            // Zero-pad short keys
            let mut bytes = [0u8; IKEY_SIZE];
            bytes[..remaining].copy_from_slice(&key[offset..]);
            u64::from_be_bytes(bytes)
        }
    }
}

// Conversion from std::ops::Bound
impl<'a> From<Bound<&'a [u8]>> for RangeBound<'a> {
    fn from(bound: Bound<&'a [u8]>) -> Self {
        match bound {
            Bound::Unbounded => RangeBound::Unbounded,

            Bound::Included(k) => RangeBound::Included(k),

            Bound::Excluded(k) => RangeBound::Excluded(k),
        }
    }
}
