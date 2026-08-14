//! Filepath: src/key.rs
//!
//! Key representation for `MassTree`
//!
//! Keys are divided into 8-byte "ikeys" for efficient comparison.
//! The [`Key`] struct tracks the current position during tree traversal
//! and supports shifting to descend into trie layers.

use std::cmp::{self as StdCmp, Ordering};

/// Size of an ikey in bytes.
pub const IKEY_SIZE: usize = 8;

/// Maximum supported key length in bytes (32 layers * 8 bytes).
pub const MAX_KEY_LENGTH: usize = 256;

/// A key for [`crate::MassTree`] operations.
#[derive(Clone, Copy, Debug)]
pub struct Key<'a> {
    /// The full key data (never modified).
    data: &'a [u8],

    /// Current 8-byte slice as big-endian u64.
    ikey: u64,

    /// Number of 8-byte chunks consumed by `shift()`.
    shift_count: usize,

    /// Offset where the suffix begins (relative to original key start).
    suffix_start: usize,
}

impl<'a> Key<'a> {
    /// Create a new key from a byte slice.
    ///
    /// # Panics
    ///
    /// Panics if `data.len() > MAX_KEY_LENGTH` (256 bytes). Keys longer than
    /// this would require more than 32 trie layers.
    #[must_use]
    #[inline]
    pub fn new(data: &'a [u8]) -> Self {
        assert!(
            data.len() <= MAX_KEY_LENGTH,
            "key length {} exceeds maximum {}",
            data.len(),
            MAX_KEY_LENGTH
        );

        let ikey: u64 = Self::read_ikey(data, 0);
        let suffix_start: usize = StdCmp::min(IKEY_SIZE, data.len());

        Self {
            data,
            ikey,
            shift_count: 0,
            suffix_start,
        }
    }

    /// Create a key from a raw ikey value (for testing/internal use).
    #[must_use]
    #[inline]
    pub const fn from_ikey(ikey: u64) -> Self {
        let len: usize = if ikey == 0 {
            0
        } else {
            IKEY_SIZE - ((ikey.trailing_zeros() / 8) as usize)
        };

        Self {
            data: &[],
            ikey,
            shift_count: 0,
            suffix_start: len,
        }
    }

    /// Create a [`Key`] from an existing suffix.
    #[must_use]
    #[inline(always)]
    pub fn from_suffix(suffix: &'a [u8]) -> Self {
        Self::new(suffix)
    }

    /// Return the current 8-byte slice as a big-endian u64.
    #[must_use]
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.data.len()
    }

    /// Return the current 8-byte slice as a big-endian u64.
    #[must_use]
    #[inline(always)]
    pub const fn ikey(&self) -> u64 {
        self.ikey
    }

    /// Return the number of shifts performed.
    #[must_use]
    #[inline(always)]
    pub const fn shift_count(&self) -> usize {
        self.shift_count
    }

    /// Return the suffix start offset (relative to original key).
    #[must_use]
    #[inline(always)]
    pub const fn suffix_start(&self) -> usize {
        self.suffix_start
    }

    /// Return the length of the key at the current layer.
    #[must_use]
    #[inline(always)]
    pub const fn current_len(&self) -> usize {
        self.data.len().saturating_sub(self.shift_count * IKEY_SIZE)
    }

    /// Check if the key is empty.
    #[must_use]
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.ikey == 0 && self.data.is_empty()
    }

    /// Check if the key has been shifted.
    #[must_use]
    #[inline(always)]
    pub const fn is_shifted(&self) -> bool {
        self.shift_count > 0
    }

    /// Check if the key has been shifted.
    #[must_use]
    #[inline(always)]
    pub const fn has_suffix(&self) -> bool {
        self.current_len() > IKEY_SIZE
    }

    /// Return the suffix (bytes after the current ikey).
    #[must_use]
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "Guarded by length check, suffix_start <= data.len()"
    )]
    pub fn suffix(&self) -> &'a [u8] {
        //  INVARIANT: suffix_start <= data.len()
        if self.suffix_start < self.data.len() {
            &self.data[self.suffix_start..]
        } else {
            &[]
        }
    }

    /// Return the length of the suffix.
    #[must_use]
    #[inline(always)]
    pub const fn suffix_len(&self) -> usize {
        self.data.len().saturating_sub(self.suffix_start)
    }

    /// Shift the key forward by 8 bytes to the next layer.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `!has_suffix()`.
    #[inline(always)]
    pub fn shift(&mut self) {
        debug_assert!(self.has_suffix(), "shift() called without suffix");

        self.shift_count += 1;

        let offset: usize = self.shift_count * IKEY_SIZE;
        let suffix_start: usize = offset + IKEY_SIZE;

        self.ikey = Self::read_ikey(self.data, offset);
        self.suffix_start = StdCmp::min(suffix_start, self.data.len());
    }

    /// Shift by a specific number of bytes (must be a multiple of 8).
    ///
    /// # Panics
    ///
    /// Panics if `bytes` is not a multiple of `IKEY_SIZE`
    #[inline(always)]
    pub fn shift_by(&mut self, bytes: usize) {
        debug_assert!(
            bytes.is_multiple_of(IKEY_SIZE),
            "shift_by must be multiple of 8"
        );

        let shifts: usize = bytes / IKEY_SIZE;

        for _ in 0..shifts {
            self.shift();
        }
    }

    /// Shift the key backward by 8 bytes (undo one shift).
    ///
    /// # Panics
    /// Panics in debug mode if `is_shifted()`.
    #[inline(always)]
    pub fn unshift(&mut self) {
        debug_assert!(self.is_shifted(), "unshift() called at position 0");

        self.shift_count -= 1;

        let offset: usize = self.shift_count * IKEY_SIZE;
        let suffix_start: usize = offset + IKEY_SIZE;

        self.ikey = Self::read_ikey(self.data, offset);
        self.suffix_start = StdCmp::min(suffix_start, self.data.len());
    }

    /// Reset to the original position (undo all shifts).
    #[inline(always)]
    pub fn unshift_all(&mut self) {
        if self.shift_count > 0 {
            self.shift_count = 0;
            self.ikey = Self::read_ikey(self.data, 0);
            self.suffix_start = StdCmp::min(IKEY_SIZE, self.data.len());
        }
    }

    /// Compare this key's length against a stored keylenx value.
    #[must_use]
    #[inline(always)]
    pub fn compare(&self, other_ikey: u64, keylenx: usize) -> Ordering {
        match self.ikey.cmp(&other_ikey) {
            Ordering::Equal => {}

            ord => return ord,
        }

        let self_len: usize = self.current_len();

        if self_len > IKEY_SIZE {
            if keylenx <= IKEY_SIZE {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        } else {
            self_len.cmp(&keylenx)
        }
    }

    /// Get the full key data.
    #[must_use]
    #[inline(always)]
    pub const fn full_data(&self) -> &'a [u8] {
        self.data
    }

    /// Compare two ikeys.
    #[must_use]
    #[inline(always)]
    pub const fn compare_ikey(a: u64, b: u64) -> Ordering {
        // Use if-else for const fn compatibility
        if a < b {
            Ordering::Less
        } else if a > b {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }

    /// Read an 8-byte ikey from data at the given offset.
    #[must_use]
    #[inline(always)]
    pub fn read_ikey(data: &[u8], offset: usize) -> u64 {
        if let Some(remaining) = data.get(offset..) {
            if let Some(bytes) = remaining.first_chunk::<IKEY_SIZE>() {
                return u64::from_be_bytes(*bytes);
            }

            if !remaining.is_empty() {
                return Self::read_ikey_slow(remaining);
            }
        }

        0
    }

    /// Read partial ikeys (1-7 bytes).
    #[inline]
    #[must_use]
    pub fn read_ikey_slow(remaining: &[u8]) -> u64 {
        let mut bytes: [u8; 8] = [0u8; 8];

        //  INVARIANT: `read_ikey` only calls this when `remaining.len() < 8`.
        #[expect(
            clippy::indexing_slicing,
            reason = "remaining.len() < 8 by caller, bytes is [u8; 8]"
        )]
        bytes[..remaining.len()].copy_from_slice(remaining);

        u64::from_be_bytes(bytes)
    }
}
