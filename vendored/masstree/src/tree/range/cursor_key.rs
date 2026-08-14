//! Mutable key buffer for range scan operations.
//!
//! [`CursorKey`] tracks the current scan position and stores the "last emitted"
//! key for duplicate filtering. Owns its data in a fixed-size buffer and
//! supports in-place modifications (unlike borrowed [`Key<'a>`]).
//!
//! # Layer Navigation
//!
//! - [`CursorKey::shift`]: Descend using the user's start key bytes
//! - [`CursorKey::shift_clear`]: Descend into a scan-discovered layer (ikey=0)
//! - [`CursorKey::unshift`]: Return to parent layer (len=9 sentinel for dup filtering)

#![expect(clippy::indexing_slicing, reason = "Safety notes are provided inline")]

use std::fmt::{self as StdFmt, Formatter};
use std::{cmp::Ordering, fmt::Debug};

#[cfg(debug_assertions)]
use std::fmt::Display;

use crate::key::{IKEY_SIZE, MAX_KEY_LENGTH};

/// Sentinel length set by [`CursorKey::unshift`] after ascending from a sublayer.
///
/// Value `9` (`IKEY_SIZE + 1`) ensures `compare()` returns Equal/Greater for
/// the layer pointer slot in the parent, skipping the already-scanned layer.
pub const UNSHIFT_SENTINEL_LEN: usize = IKEY_SIZE + 1;

/// Mutable key buffer for scan operations.
#[derive(Clone)]
pub struct CursorKey {
    buf: [u8; MAX_KEY_LENGTH],

    /// Offset to current layer's ikey (always a multiple of `IKEY_SIZE`).
    offset: usize,

    /// Current layer key length. `<= 8`: inline, `> 8`: has suffix, `9`: unshift sentinel.
    len: usize,

    /// Current layer's ikey (cached `u64::from_be_bytes(buf[offset..offset+8])`).
    ikey: u64,
}

#[expect(clippy::missing_fields_in_debug, reason = "don't print buffer")]
impl Debug for CursorKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("CursorKey")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .field("ikey", &format_args!("{:#018x}", self.ikey))
            .field("full_key", &self.full_key())
            .finish()
    }
}

/// Creates a minimum-key cursor (ikey=0, len=0). Equivalent to [`CursorKey::empty()`].
impl Default for CursorKey {
    fn default() -> Self {
        Self::empty()
    }
}

impl CursorKey {
    // ========================================================================
    //  Constructors
    // ========================================================================

    /// Create from an initial key slice. Panics if `data.len() > MAX_KEY_LENGTH`.
    #[must_use]
    #[inline]
    pub fn from_slice(data: &[u8]) -> Self {
        assert!(
            data.len() <= MAX_KEY_LENGTH,
            "key length {} exceeds maximum {}",
            data.len(),
            MAX_KEY_LENGTH
        );

        let mut buf: [u8; MAX_KEY_LENGTH] = [0u8; MAX_KEY_LENGTH];
        buf[..data.len()].copy_from_slice(data);

        let ikey: u64 = Self::read_ikey_from_buf(&buf, 0, data.len());

        Self {
            buf,
            offset: 0,
            len: data.len(),
            ikey,
        }
    }

    /// Create an empty cursor (ikey=0, len=0) for unbounded forward scans.
    #[must_use]
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            buf: [0u8; MAX_KEY_LENGTH],
            offset: 0,
            len: 0,
            ikey: 0,
        }
    }

    /// Create a cursor for reverse scan from an end bound.
    ///
    /// - `Unbounded`: ikey=MAX, len=9 sentinel (start from maximum)
    /// - `Included`/`Excluded`: positioned at the bound key
    #[must_use]
    #[inline]
    pub fn for_reverse_scan(end: &super::iterator::RangeBound<'_>) -> Self {
        use super::iterator::RangeBound;

        match end {
            RangeBound::Unbounded => Self {
                buf: [0xFF; MAX_KEY_LENGTH],
                offset: 0,
                len: UNSHIFT_SENTINEL_LEN,
                ikey: u64::MAX,
            },
            RangeBound::Included(k) | RangeBound::Excluded(k) => Self::from_slice(k),
        }
    }

    // ========================================================================
    //  Accessors
    // ========================================================================

    /// Current layer's ikey (big-endian u64, directly comparable with stored ikeys).
    #[must_use]
    #[inline(always)]
    pub const fn current_ikey(&self) -> u64 {
        self.ikey
    }

    /// Full key bytes from start to current position + len.
    #[must_use]
    #[inline(always)]
    pub fn full_key(&self) -> &[u8] {
        let end: usize = self.offset + self.len;
        &self.buf[..end]
    }

    /// Full key bytes without bounds checking.
    ///
    /// # Safety
    ///
    /// Caller must ensure `offset + len <= MAX_KEY_LENGTH` (guaranteed by
    /// `CursorKey`'s internal invariants).
    #[must_use]
    #[inline(always)]
    pub unsafe fn full_key_unchecked(&self) -> &[u8] {
        let end: usize = self.offset + self.len;
        debug_assert!(
            end <= MAX_KEY_LENGTH,
            "CursorKey invariant violated: offset({}) + len({}) = {} > MAX_KEY_LENGTH({})",
            self.offset,
            self.len,
            end,
            MAX_KEY_LENGTH
        );
        unsafe { self.buf.get_unchecked(..end) }
    }

    /// Suffix bytes after current ikey. Empty if `len <= 8`.
    #[must_use]
    #[inline]
    pub fn suffix(&self) -> &[u8] {
        if self.len > IKEY_SIZE {
            let suffix_start: usize = self.offset + IKEY_SIZE;
            let suffix_end: usize = self.offset + self.len;
            &self.buf[suffix_start..suffix_end]
        } else {
            &[]
        }
    }

    /// True if current position has suffix bytes (len > 8).
    #[must_use]
    #[inline(always)]
    pub const fn has_suffix(&self) -> bool {
        self.len > IKEY_SIZE
    }

    /// Current key length at this layer.
    #[must_use]
    #[inline(always)]
    pub const fn current_len(&self) -> usize {
        self.len
    }

    /// Layer offset (bytes consumed by shifts).
    #[must_use]
    #[inline(always)]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// True if at layer 0 (no shifts performed).
    #[must_use]
    #[inline(always)]
    pub const fn is_at_root_layer(&self) -> bool {
        self.offset == 0
    }

    /// Number of layers deep (offset / 8).
    #[must_use]
    #[inline(always)]
    #[cfg(any(test, debug_assertions))]
    pub const fn layer_depth(&self) -> usize {
        self.offset / IKEY_SIZE
    }

    // ========================================================================
    //  Layer Navigation
    // ========================================================================

    /// Shift to next layer following the start key bytes.
    ///
    /// Computes the next layer's ikey from existing buffer contents.
    /// Debug-panics if `!has_suffix()`.
    #[inline]
    pub fn shift(&mut self) {
        debug_assert!(self.has_suffix(), "shift() called without suffix");

        self.offset += IKEY_SIZE;
        self.len = self.len.saturating_sub(IKEY_SIZE);
        self.ikey = Self::read_ikey_from_buf(&self.buf, self.offset, self.len);
    }

    /// Shift to sublayer for reverse scan (set to maximum: ikey=MAX, len=9).
    ///
    /// The `len = 9` sentinel is critical — it makes the cursor behave like
    /// it has a suffix, affecting comparisons with layer pointers.
    #[inline]
    pub fn shift_clear_reverse(&mut self) {
        debug_assert!(
            self.offset + IKEY_SIZE <= MAX_KEY_LENGTH,
            "shift_clear_reverse: would exceed MAX_KEY_LENGTH"
        );

        self.offset += IKEY_SIZE;
        self.ikey = u64::MAX;
        self.buf[self.offset..self.offset + IKEY_SIZE].copy_from_slice(&u64::MAX.to_be_bytes());
        self.len = UNSHIFT_SENTINEL_LEN;
    }

    /// Shift to next layer and clear (ikey=0, len=0) for scanning from sublayer minimum.
    ///
    /// Called when following a layer pointer to enumerate all keys in the sublayer.
    /// Matches C++ `key::shift_clear()`.
    #[inline]
    pub fn shift_clear(&mut self) {
        self.offset += IKEY_SIZE;
        self.len = 0;
        self.ikey = 0;

        #[cfg(debug_assertions)]
        {
            let clear_start: usize = self.offset;
            let clear_end: usize = (self.offset + IKEY_SIZE).min(MAX_KEY_LENGTH);
            self.buf[clear_start..clear_end].fill(0);
        }
    }

    /// Return to parent layer after exhausting a sublayer.
    ///
    /// Sets `len = 9` sentinel so `compare()` returns Equal/Greater for the
    /// layer pointer slot, skipping the already-processed layer.
    /// Debug-panics if at root layer.
    #[inline(always)]
    pub fn unshift(&mut self) {
        debug_assert!(self.offset >= IKEY_SIZE, "unshift() called at root layer");

        self.offset -= IKEY_SIZE;
        self.ikey = Self::read_ikey_from_buf(&self.buf, self.offset, IKEY_SIZE);
        self.len = UNSHIFT_SENTINEL_LEN;
    }

    /// True if cursor is at root layer with no content (offset=0, len=0).
    ///
    /// Occurs after `unshift()` ascends past the original key's layer,
    /// indicating all sublayers are exhausted.
    #[inline(always)]
    pub const fn is_at_empty_root(&self) -> bool {
        self.offset == 0 && self.len == 0
    }

    // ========================================================================
    //  Key Assignment (for duplicate filtering)
    // ========================================================================

    /// Store an ikey from a leaf slot into the buffer at current offset.
    #[inline(always)]
    pub fn assign_store_ikey(&mut self, ikey: u64) {
        self.ikey = ikey;

        let bytes: [u8; 8] = ikey.to_be_bytes();
        let start: usize = self.offset;
        let end: usize = start + IKEY_SIZE;
        self.buf[start..end].copy_from_slice(&bytes);
    }

    /// Store suffix bytes and return total key length (`IKEY_SIZE + suffix.len()`).
    ///
    /// Callers must ensure `offset + IKEY_SIZE + suffix.len() <= MAX_KEY_LENGTH`
    /// (guaranteed when suffix comes from a valid leaf slot).
    #[inline]
    pub fn assign_store_suffix(&mut self, suffix: &[u8]) -> usize {
        let suffix_start: usize = self.offset + IKEY_SIZE;
        let suffix_end: usize = suffix_start + suffix.len();

        debug_assert!(
            suffix_end <= MAX_KEY_LENGTH,
            "suffix would overflow buffer: offset={}, suffix.len={}",
            self.offset,
            suffix.len()
        );

        self.buf[suffix_start..suffix_end].copy_from_slice(suffix);

        IKEY_SIZE + suffix.len()
    }

    /// Set the key length at current layer.
    #[inline(always)]
    pub fn assign_store_length(&mut self, len: usize) {
        debug_assert!(
            len <= MAX_KEY_LENGTH - self.offset,
            "len {} would overflow at offset {}",
            len,
            self.offset
        );
        self.len = len;
    }

    /// No-op. Kept for C++ `key::mark_key_complete()` API parity.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "C++ API compatibility")]
    pub const fn mark_key_complete(&self) {}

    // ========================================================================
    //  Comparison
    // ========================================================================

    /// Compare cursor position against a `(ikey, keylenx)` pair.
    ///
    /// Matches C++ `key::compare()` semantics:
    /// 1. Compare ikeys first
    /// 2. If equal: cursor with suffix vs no-suffix slot = Greater;
    ///    both have suffix = Equal (need suffix compare); else compare lengths
    ///
    /// Layer pointers (`keylenx >= 128`) are treated as "very long keys".
    /// After `unshift()` (len=9), cursor compares Equal/Greater to skip them.
    #[must_use]
    #[inline(always)]
    pub fn compare(&self, other_ikey: u64, keylenx: usize) -> Ordering {
        match self.ikey.cmp(&other_ikey) {
            Ordering::Equal => {}
            ord => return ord,
        }

        if self.len > IKEY_SIZE {
            if keylenx <= IKEY_SIZE {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        } else {
            self.len.cmp(&keylenx)
        }
    }

    /// Compare suffix bytes against stored suffix (used after `compare()` returns Equal).
    #[must_use]
    #[inline(always)]
    pub fn compare_suffix(&self, stored_suffix: &[u8]) -> Ordering {
        self.suffix().cmp(stored_suffix)
    }

    // ========================================================================
    //  Debug Helpers
    // ========================================================================

    /// Snapshot of cursor state for tracing ordering violations.
    #[must_use]
    #[cfg(debug_assertions)]
    pub fn debug_state(&self) -> CursorDebugState {
        CursorDebugState {
            offset: self.offset,
            len: self.len,
            ikey: self.ikey,
            full_key: self.full_key().to_vec(),
            layer_depth: self.layer_depth(),
        }
    }

    // ========================================================================
    //  Internal Helpers
    // ========================================================================

    /// Read ikey from buffer at offset. Pads with zeros if fewer than 8 bytes remain.
    #[inline]
    fn read_ikey_from_buf(buf: &[u8; MAX_KEY_LENGTH], offset: usize, len: usize) -> u64 {
        if len == 0 {
            return 0;
        }

        let available: usize = len.min(IKEY_SIZE);
        let start: usize = offset;
        let end: usize = offset + available;

        // Fast path: full 8 bytes available
        if available == IKEY_SIZE {
            #[expect(clippy::expect_used, reason = "infallible: guarded by available == 8")]
            let arr: [u8; 8] = buf[start..end]
                .try_into()
                .expect("slice is exactly 8 bytes");
            return u64::from_be_bytes(arr);
        }

        // Slow path: partial read with zero-padding
        let mut bytes: [u8; 8] = [0u8; 8];
        bytes[..available].copy_from_slice(&buf[start..end]);
        u64::from_be_bytes(bytes)
    }
}

// ============================================================================
//  Debug State Struct
// ============================================================================

/// Debug snapshot of cursor state for tracing ordering violations.
#[cfg(debug_assertions)]
#[derive(Clone, Debug)]
pub struct CursorDebugState {
    pub offset: usize,
    pub len: usize,
    pub ikey: u64,
    pub full_key: Vec<u8>,
    pub layer_depth: usize,
}

#[cfg(debug_assertions)]
impl Display for CursorDebugState {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        write!(
            f,
            "CursorState {{ offset: {}, len: {}, ikey: {:016x}, layer: {}, key: {:?} }}",
            self.offset,
            self.len,
            self.ikey,
            self.layer_depth,
            String::from_utf8_lossy(&self.full_key)
        )
    }
}

// ============================================================================
//  Tests
// ============================================================================
