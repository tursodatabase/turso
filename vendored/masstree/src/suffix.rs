//! Filepath: src/suffix.rs
//!
//! Suffix storage for keys longer than 8 bytes.
//!
//! When a key is longer than 8 bytes, the first 8 bytes are stored as `ikey0`
//! and the remaining bytes are stored in a [`SuffixBag`].

use std::cell::UnsafeCell;
use std::fmt::{self, Debug, Formatter};

use crate::TreePermutation;

mod clone;
mod compact;
mod inline;
mod sidecar;

pub use inline::InlineSuffixBag;

pub use sidecar::{SideCarUtils, SuffixSidecar};

/// Number of slots (matches `WIDTH_15` leaf node).
const WIDTH: usize = 15;

/// Initial capacity for suffix storage.
const INITIAL_CAPACITY: usize = 128;

// ============================================================================
//  SlotMeta
// ============================================================================

/// Metadata for a single slot's suffix.
#[derive(Clone, Copy, Debug, Default)]
struct SlotMeta {
    /// Offset into the data buffer (`u32::MAX` if no suffix).
    offset: u32,

    /// Length of the suffix.
    len: u16,

    /// Padding for alignment (unused).
    _pad: u16,
}

impl SlotMeta {
    /// Sentinel value indicating no suffix stored.
    const EMPTY: Self = Self {
        offset: u32::MAX,
        len: 0,
        _pad: 0,
    };

    /// Check if this slot has a suffix.
    #[inline(always)]
    const fn has_suffix(self) -> bool {
        self.offset != u32::MAX
    }
}

// ============================================================================
//  PermutationProvider Trait
// ============================================================================

/// Trait for types that can provide permutation information.
///
/// This allows [`SuffixBag`] to work with different permutation implementations,
/// primarily [`crate::permuter::Permuter`].
pub trait PermutationProvider {
    /// Return the number of active slots.
    fn size(&self) -> usize;

    /// Return the physical slot index at logical position `i`.
    fn get(&self, i: usize) -> usize;
}

// ============================================================================
//  SuffixBag
// ============================================================================

/// Contiguous storage for key suffixes.
#[derive(Debug)]
pub struct SuffixBag {
    /// Per-slot metadata: (offset, length) pairs.
    slots: [SlotMeta; WIDTH],

    /// Contiguous suffix data buffer.
    data: Vec<u8>,

    /// Cached count of slots with suffixes (avoids O(WIDTH) recount).
    suffix_count: u8,
}

impl SuffixBag {
    // ========================================================================
    //  Constructor
    // ========================================================================

    /// Create a new suffix bag with initial capacity.
    #[must_use]
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            slots: [SlotMeta::EMPTY; WIDTH],
            data: Vec::with_capacity(INITIAL_CAPACITY),
            suffix_count: 0,
        }
    }

    /// Create a new suffix bag with specified capacity.
    #[must_use]
    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: [SlotMeta::EMPTY; WIDTH],
            data: Vec::with_capacity(capacity),
            suffix_count: 0,
        }
    }

    /// Construct [`SuffixBag`] by reusing an existing [`Vec<u8>`] buffer.
    #[must_use]
    #[inline(always)]
    pub fn from_vec(data: Vec<u8>) -> Self {
        debug_assert!(data.is_empty(), "from_vec expects empty Vec");

        Self {
            slots: [SlotMeta::EMPTY; WIDTH],
            data,
            suffix_count: 0,
        }
    }

    // ========================================================================
    //  Capacity & Size
    // ========================================================================

    /// Return the current capacity of the data buffer.
    #[must_use]
    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Return the number of bytes currently used.
    #[must_use]
    #[inline(always)]
    pub const fn used(&self) -> usize {
        self.data.len()
    }

    /// Return the number of slots that have suffixes.
    #[must_use]
    #[inline(always)]
    pub const fn count(&self) -> usize {
        self.suffix_count as usize
    }

    /// Reserve additional capacity for suffix data.
    #[inline(always)]
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    // ========================================================================
    //  Smart Assignment with Compaction
    // ========================================================================

    /// Append suffix data and update slot metadata + count.
    #[expect(clippy::indexing_slicing, reason = "Checked by caller")]
    #[expect(clippy::cast_possible_truncation)]
    fn append_and_record(&mut self, slot: usize, suffix: &[u8]) {
        if !self.slots[slot].has_suffix() {
            self.suffix_count += 1;
        }

        let offset: usize = self.data.len();
        self.data.extend_from_slice(suffix);

        self.slots[slot] = SlotMeta {
            offset: offset as u32,
            len: suffix.len() as u16,
            _pad: 0,
        };
    }

    /// Assign a suffix with smart space management.
    ///
    /// # Panics
    ///
    /// Panics if `suffix.len() > u16::MAX` (65535 bytes).
    #[expect(clippy::indexing_slicing, reason = "Checked access")]
    pub fn try_assign(&mut self, slot: usize, suffix: &[u8]) -> bool {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");

        let suffix_len: usize = suffix.len();

        debug_assert!(
            u16::try_from(suffix_len).is_ok(),
            "suffix too long: {suffix_len} > {}",
            u16::MAX
        );

        let meta: SlotMeta = self.slots[slot];

        // Fast path: reuse existing slot space if new suffix fits.
        if meta.has_suffix() && (suffix_len <= (meta.len as usize)) {
            let start: usize = meta.offset as usize;
            self.data[start..(start + suffix_len)].copy_from_slice(suffix);

            #[expect(clippy::cast_possible_truncation)]
            {
                self.slots[slot] = SlotMeta {
                    offset: meta.offset,
                    len: suffix_len as u16,
                    _pad: 0,
                };
            }

            return true;
        }

        // Try append to end of buffer.
        if (self.data.len() + suffix_len) <= self.data.capacity() {
            self.append_and_record(slot, suffix);
            return true;
        }

        // Compact and retry.
        self.compact_in_place();

        if (self.data.len() + suffix_len) <= self.data.capacity() {
            self.append_and_record(slot, suffix);
            return true;
        }

        // Still not enough space, grow (aborts on OOM).
        self.data.reserve(suffix_len);
        self.append_and_record(slot, suffix);

        false // Buffer grew
    }

    // ========================================================================
    //  Slot Access
    // ========================================================================

    /// Check if a slot has a suffix.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 15`.
    #[must_use]
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "Slot bounds checked via caller contract"
    )]
    pub fn has_suffix(&self, slot: usize) -> bool {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");
        self.slots[slot].has_suffix()
    }

    /// Get the suffix for a slot, or `None` if no suffix.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 15`.
    #[must_use]
    #[inline(always)]
    pub fn get(&self, slot: usize) -> Option<&[u8]> {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");

        let meta: SlotMeta = self.slots[slot];

        if !meta.has_suffix() {
            return None;
        }

        let start: usize = meta.offset as usize;
        let end: usize = start + meta.len as usize;

        // Torn SlotMeta reads under OCC can produce out-of-range offsets.
        // Return None so the caller's OCC retry handles it safely.
        if end > self.data.len() {
            return None;
        }

        Some(&self.data[start..end])
    }

    /// Get the suffix for a slot, or empty slice if no suffix.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 15`.
    #[must_use]
    #[inline(always)]
    pub fn get_or_empty(&self, slot: usize) -> &[u8] {
        self.get(slot).unwrap_or(&[])
    }

    // ========================================================================
    //  Suffix Assignment
    // ========================================================================

    /// Append-only suffix assignment for concurrent use.
    ///
    /// Unlike [`try_assign_in_place`](Self::try_assign_in_place), this method
    /// never overwrites existing suffix bytes. New data is always appended to
    /// the end of the buffer, so concurrent OCC readers reading old offsets
    /// see stable data. Old suffix space becomes dead until drain-and-rebuild.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 15` or if suffix length exceeds `u16::MAX`.
    #[inline]
    pub fn try_assign_append_only(&mut self, slot: usize, suffix: &[u8]) -> bool {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");
        debug_assert!(
            u16::try_from(suffix.len()).is_ok(),
            "suffix too long: {} > {}",
            suffix.len(),
            u16::MAX
        );

        self.try_append_suffix(slot, suffix)
    }

    /// Try to assign a suffix to a slot in-place, without growing the buffer.
    ///
    /// This method may overwrite existing suffix bytes in-place when the new
    /// suffix fits. It is NOT safe for concurrent use (readers may observe
    /// partially-written data). Use [`try_assign_append_only`](Self::try_assign_append_only)
    /// for the concurrent path.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 15` or if suffix length exceeds `u16::MAX`.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "Slot bounds checked via debug_assert"
    )]
    pub fn try_assign_in_place(&mut self, slot: usize, suffix: &[u8]) -> bool {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");
        debug_assert!(
            u16::try_from(suffix.len()).is_ok(),
            "suffix too long: {} > {}",
            suffix.len(),
            u16::MAX
        );

        let meta: SlotMeta = self.slots[slot];

        if meta.has_suffix() && (suffix.len() <= (meta.len as usize)) {
            let start: usize = meta.offset as usize;
            self.data[start..(start + suffix.len())].copy_from_slice(suffix);

            #[expect(clippy::cast_possible_truncation, reason = "len checked above")]
            {
                self.slots[slot] = SlotMeta {
                    offset: meta.offset,
                    len: suffix.len() as u16,
                    _pad: 0,
                };
            }

            return true;
        }

        self.try_append_suffix(slot, suffix)
    }

    /// Append suffix data to the end of the buffer if capacity allows.
    ///
    /// Shared by `try_assign_append_only` and the fallback path of
    /// `try_assign_in_place`.
    #[inline]
    #[expect(clippy::indexing_slicing, reason = "Slot bounds checked by caller")]
    fn try_append_suffix(&mut self, slot: usize, suffix: &[u8]) -> bool {
        let new_offset: usize = self.data.len();

        if (new_offset + suffix.len()) <= self.data.capacity() {
            if !self.slots[slot].has_suffix() {
                self.suffix_count += 1;
            }

            self.data.extend_from_slice(suffix);

            // NOTE: SlotMeta write is non-atomic. Concurrent OCC readers may
            // observe a torn (offset, len) pair, but they validate the leaf
            // version after reading, so torn data is always discarded.
            #[expect(clippy::cast_possible_truncation, reason = "offset and len checked")]
            {
                self.slots[slot] = SlotMeta {
                    offset: new_offset as u32,
                    len: suffix.len() as u16,
                    _pad: 0,
                };
            }

            return true;
        }

        false
    }

    /// Assign a suffix to a slot.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 15` or if suffix length exceeds `u16::MAX`.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "Slot bounds checked via debug_assert"
    )]
    pub fn assign(&mut self, slot: usize, suffix: &[u8]) {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");
        debug_assert!(
            u16::try_from(suffix.len()).is_ok(),
            "suffix too long: {} > {}",
            suffix.len(),
            u16::MAX
        );

        let meta: SlotMeta = self.slots[slot];

        if !meta.has_suffix() {
            self.suffix_count += 1;
        }

        let offset: usize = self.data.len();
        self.data.extend_from_slice(suffix);

        #[expect(
            clippy::cast_possible_truncation,
            reason = "offset bounded by Vec capacity, len checked above"
        )]
        {
            self.slots[slot] = SlotMeta {
                offset: offset as u32,
                len: suffix.len() as u16,
                _pad: 0,
            };
        }
    }

    /// Clear the suffix for a slot.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 15`.
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "Slot bounds checked via debug_assert"
    )]
    pub fn clear(&mut self, slot: usize) {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");

        if self.slots[slot].has_suffix() {
            self.suffix_count -= 1;
        }

        self.slots[slot] = SlotMeta::EMPTY;
    }

    // ========================================================================
    //  Comparison Helpers
    // ========================================================================

    /// Check if a slot's suffix equals the given suffix.
    #[must_use]
    #[inline(always)]
    pub fn suffix_equals(&self, slot: usize, suffix: &[u8]) -> bool {
        self.get(slot).is_some_and(|stored: &[u8]| stored == suffix)
    }

    /// Compare a slot's suffix with the given suffix.
    #[must_use]
    #[inline(always)]
    pub fn suffix_compare(&self, slot: usize, suffix: &[u8]) -> Option<std::cmp::Ordering> {
        self.get(slot).map(|stored: &[u8]| stored.cmp(suffix))
    }
}

impl Default for SuffixBag {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
//  SuffixBagCell (internal concurrent wrapper)
// ============================================================================

/// Interior-mutable wrapper for [`SuffixBag`] used inside leaf nodes.
///
/// Provides `UnsafeCell`-based access so that writers (under the leaf lock)
/// can mutate through `&self` while readers form `&SuffixBag` for read-only
/// access under the OCC protocol.
///
/// This type is NOT publicly exported. It exists solely to eliminate the
/// `&mut SuffixBag` / `&SuffixBag` aliasing UB in the tree's concurrent paths.
pub struct SuffixBagCell {
    inner: UnsafeCell<SuffixBag>,
}

// SAFETY: SuffixBagCell is Send because SuffixBag contains Vec<u8> and plain
// data, all of which are Send. We intentionally do NOT implement Sync. The
// tree accesses SuffixBagCell only through raw pointers from AtomicPtr, forming
// short-lived references locally. Writers hold the leaf lock, readers validate
// via OCC version checks.
unsafe impl Send for SuffixBagCell {}

impl SuffixBagCell {
    /// Create a new cell wrapping a default `SuffixBag`.
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self {
            inner: UnsafeCell::new(SuffixBag::new()),
        }
    }

    /// Create a cell from an existing `SuffixBag`.
    #[inline(always)]
    pub(crate) const fn from_bag(bag: SuffixBag) -> Self {
        Self {
            inner: UnsafeCell::new(bag),
        }
    }

    /// Get a shared reference for read-only access.
    ///
    /// # Safety
    ///
    /// Caller must ensure no concurent `as_mut()` call is active on the same
    /// cell, OR that the access is protected by OCC (readers validate version
    /// after reading).
    #[inline(always)]
    pub(crate) unsafe fn as_ref(&self) -> &SuffixBag {
        // SAFETY: Caller guarantees no concurrent mutable access, or access
        // is protected by OCC protocol.
        unsafe { &*self.inner.get() }
    }

    /// Get a mutable reference for write access.
    ///
    /// # Safety
    ///
    /// Caller must hold the leaf lock (exclusive write access).
    #[inline(always)]
    #[expect(clippy::mut_from_ref, reason = "Interior mutability via UnsafeCell")]
    pub(crate) unsafe fn as_mut(&self) -> &mut SuffixBag {
        // SAFETY: Caller holds the leaf lock, guaranteeing exclusive write
        // access. UnsafeCell allows forming &mut through &self.
        unsafe { &mut *self.inner.get() }
    }
}

impl Default for SuffixBagCell {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for SuffixBagCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // SAFETY: Debug is called from single-threaded contexts (tests, logging).
        let bag: &SuffixBag = unsafe { &*self.inner.get() };

        f.debug_struct("SuffixBagCell").field("inner", bag).finish()
    }
}

// ============================================================================
//  Tests
// ============================================================================
