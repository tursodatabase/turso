//! Batch insert preparation and analysis utilities.
//!
//! The `insert_batch()` method is available on all tree types via `MassTreeGeneric`.

#![allow(clippy::indexing_slicing, reason = "Bounded by key length checks")]

use std::collections::BTreeSet;

// ============================================================================
// Entry Preparation
// ============================================================================

/// Zip separate key and value iterators into batch entries.
///
/// Returns `Vec<(Vec<u8>, V)>` ready for `insert_batch`.
#[inline]
#[must_use]
pub fn zip_into_entries<K, V, IK, IV>(keys: IK, values: IV) -> Vec<(Vec<u8>, V)>
where
    K: Into<Vec<u8>>,
    IK: IntoIterator<Item = K>,
    IV: IntoIterator<Item = V>,
{
    keys.into_iter()
        .zip(values)
        .map(|(k, v)| (k.into(), v))
        .collect()
}

/// Convert a `(key, value)` iterator into batch entries.
///
/// Returns `Vec<(Vec<u8>, V)>` ready for `insert_batch`.
#[inline]
#[must_use]
pub fn from_iter<K, V, I>(iter: I) -> Vec<(Vec<u8>, V)>
where
    K: Into<Vec<u8>>,
    I: IntoIterator<Item = (K, V)>,
{
    iter.into_iter().map(|(k, v)| (k.into(), v)).collect()
}

/// Generate sequential keys: `{prefix}{number:0width}` for `[start, end)`.
///
/// Useful for benchmarking batch insert performance.
#[must_use]
pub fn sequential_keys(prefix: &[u8], start: usize, end: usize, width: usize) -> Vec<Vec<u8>> {
    (start..end)
        .map(|i| {
            let mut key = prefix.to_vec();
            key.extend(format!("{i:0width$}").as_bytes());
            key
        })
        .collect()
}

/// Generate sequential 8-byte keys (u64 big-endian) for `[start, end)`.
///
/// Optimal locality for batch insert (single ikey per entry).
#[must_use]
#[inline(always)]
pub fn sequential_u64_keys(start: u64, end: u64) -> Vec<Vec<u8>> {
    (start..end).map(|i| i.to_be_bytes().to_vec()).collect()
}

// ============================================================================
// Batch Analysis
// ============================================================================

/// Pre-insertion statistics for batch performance estimation.
#[must_use]
#[derive(Debug, Clone, Default)]
pub struct BatchStats {
    /// Total entry count.
    pub count: usize,

    /// Unique ikey prefixes (first 8 bytes). Lower = better locality.
    pub unique_ikeys: usize,

    /// Average key length in bytes.
    pub avg_key_len: f64,

    /// Keys with suffixes (> 8 bytes).
    pub keys_with_suffix: usize,

    /// Single-layer keys (≤ 8 bytes).
    pub single_layer_keys: usize,
}

impl BatchStats {
    /// Compute statistics for a batch of entries.
    pub fn analyze<V>(entries: &[(Vec<u8>, V)]) -> Self {
        let count = entries.len();

        if count == 0 {
            return Self::default();
        }

        // BTreeSet: deterministic order, lower allocation overhead than HashSet
        let mut unique_ikeys = BTreeSet::new();
        let mut total_key_len: usize = 0;
        let mut keys_with_suffix = 0;
        let mut single_layer_keys = 0;

        for (key, _) in entries {
            // Extract ikey (first 8 bytes, zero-padded)
            let mut buf = [0u8; 8];
            let len = key.len().min(8);
            buf[..len].copy_from_slice(&key[..len]);
            let ikey = u64::from_be_bytes(buf);

            unique_ikeys.insert(ikey);
            total_key_len += key.len();

            if key.len() > 8 {
                keys_with_suffix += 1;
            } else {
                single_layer_keys += 1;
            }
        }

        #[expect(
            clippy::cast_precision_loss,
            reason = "Statistical averages don't need full usize precision"
        )]
        let avg_key_len = total_key_len as f64 / count as f64;

        Self {
            count,
            unique_ikeys: unique_ikeys.len(),
            avg_key_len,
            keys_with_suffix,
            single_layer_keys,
        }
    }

    /// Locality factor (0.0-1.0). Higher = better clustering.
    ///
    /// 1.0 = all same ikey prefix, 0.0 = all unique prefixes.
    #[inline]
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "Locality ratio doesn't need full usize precision"
    )]
    pub fn locality_factor(&self) -> f64 {
        if self.count == 0 || self.unique_ikeys == 0 {
            return 0.0;
        }
        (1.0 - (self.unique_ikeys as f64 / self.count as f64)).clamp(0.0, 1.0)
    }

    /// Estimated leaf count (may increase if splits occur).
    #[must_use]
    #[inline(always)]
    pub const fn estimated_leaves(&self) -> usize {
        self.unique_ikeys
    }

    /// True if all keys are single-layer (≤ 8 bytes).
    #[must_use]
    #[inline(always)]
    pub const fn all_single_layer(&self) -> bool {
        self.keys_with_suffix == 0
    }
}
