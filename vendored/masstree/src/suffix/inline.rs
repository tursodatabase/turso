use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::fmt::{self as StdFmt, Debug, Formatter};
use std::ptr as StdPtr;
use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32, Ordering as AtomicOrdering};

use super::{SuffixBag, TreePermutation};

/// Number of slots (matches `WIDTH_15` leaf node).
const WIDTH: usize = 15;

/// Inline suffix data capacity.
///
/// Default: 512 bytes (fewer heap allocations, better insert throughput).
/// With `small-suffix-capacity` feature: 256 bytes (smaller sidecar heap allocation).
#[cfg(not(feature = "small-suffix-capacity"))]
const CAPACITY: usize = 512;

#[cfg(feature = "small-suffix-capacity")]
const CAPACITY: usize = 256;

const U16_MAX: usize = u16::MAX as usize;

/// Atomic ordering for reading slot metadata (pairs with Release stores).
const READ_ORD: AtomicOrdering = AtomicOrdering::Acquire;

/// Atomic ordering for writing slot metadata (pairs with Acquire loads).
const WRITE_ORD: AtomicOrdering = AtomicOrdering::Release;

/// Relaxed ordering for non-synchronizing accesses (under lock).
const RELAXED: AtomicOrdering = AtomicOrdering::Relaxed;

// ============================================================================
//  InlineSlotMeta
// ============================================================================

/// Metadata for a single slot's suffix in inline storage.
#[derive(Clone, Copy, Debug)]
struct InlineSlotMeta {
    /// Offset into the data buffer (`u16::MAX` if no suffix).
    offset: u16,

    /// Length of the suffix.
    len: u16,
}

impl InlineSlotMeta {
    /// Sentinel value indicating no suffix stored.
    const EMPTY: Self = Self {
        offset: u16::MAX,
        len: 0,
    };

    /// Packed representation of EMPTY for atomic initialization.
    const EMPTY_PACKED: u32 = Self::EMPTY.pack();

    /// Check if this slot has a suffix.
    #[inline(always)]
    const fn has_suffix(self) -> bool {
        self.offset != u16::MAX
    }

    /// Pack into u32 for atomic storage.
    #[inline(always)]
    const fn pack(self) -> u32 {
        ((self.offset as u32) << 16) | (self.len as u32)
    }

    /// Unpack from u32 loaded atomically.
    #[inline(always)]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Intentional: extracting u16 fields from packed u32"
    )]
    const fn unpack(packed: u32) -> Self {
        Self {
            offset: (packed >> 16) as u16,
            len: packed as u16,
        }
    }
}

impl Default for InlineSlotMeta {
    #[inline(always)]
    fn default() -> Self {
        Self::EMPTY
    }
}

// ============================================================================
//  InlineSuffixBag
// ============================================================================

/// Fixed-capacity suffix storage embedded directly in a leaf node.
#[repr(C)]
pub struct InlineSuffixBag {
    /// Per-slot metadata: packed (offset, length) pairs.
    slots: [AtomicU32; WIDTH],

    /// Current write position in data buffer.
    size: AtomicU16,

    /// Cached count of slots with suffixes.
    suffix_count: AtomicU8,

    /// Padding for alignment.
    _pad: u8,

    /// Fixed-size data buffer.
    data: UnsafeCell<[u8; CAPACITY]>,
}

impl Debug for InlineSuffixBag {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("InlineSuffixBag")
            .field("size", &self.size.load(RELAXED))
            .field("suffix_count", &self.suffix_count.load(RELAXED))
            .field("capacity", &CAPACITY)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)]
impl InlineSuffixBag {
    // ========================================================================
    //  Constructor
    // ========================================================================

    /// Packed empty sentinel for const array initialization.
    const EMPTY_PACKED: u32 = InlineSlotMeta::EMPTY_PACKED;

    /// Create an empty inline suffix bag.
    #[must_use]
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            slots: [const { AtomicU32::new(Self::EMPTY_PACKED) }; WIDTH],
            size: AtomicU16::new(0),
            suffix_count: AtomicU8::new(0),
            _pad: 0,
            data: UnsafeCell::new([0u8; CAPACITY]),
        }
    }

    // ========================================================================
    //  Slot Access (Atomic)
    // ========================================================================

    /// Load slot metadata atomically.
    #[inline(always)]
    #[expect(clippy::indexing_slicing, reason = "Bounds checked via debug_assert")]
    fn load_meta(&self, slot: usize) -> InlineSlotMeta {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");

        // SAFETY: slot bounds checked above
        let packed: u32 = self.slots[slot].load(READ_ORD);
        InlineSlotMeta::unpack(packed)
    }

    /// Store slot metadata atomically.
    #[inline(always)]
    #[expect(clippy::indexing_slicing, reason = "Bounds checked via debug_assert")]
    fn store_meta(&self, slot: usize, meta: InlineSlotMeta) {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");

        // SAFETY: slot bounds checked above
        self.slots[slot].store(meta.pack(), WRITE_ORD);
    }

    // ========================================================================
    //  Capacity & Size
    // ========================================================================

    /// Return the fixed data capacity of this inline bag.
    #[must_use]
    #[inline(always)]
    #[expect(clippy::unused_self)]
    pub const fn capacity(&self) -> usize {
        CAPACITY
    }

    /// Return the number of bytes currently used.
    #[must_use]
    #[inline(always)]
    pub fn used(&self) -> usize {
        self.size.load(RELAXED) as usize
    }

    /// Return the remaining capacity.
    #[must_use]
    #[inline(always)]
    pub fn remaining(&self) -> usize {
        CAPACITY - self.used()
    }

    /// Return the number of slots that have suffixes.
    #[must_use]
    #[inline(always)]
    pub fn count(&self) -> usize {
        self.suffix_count.load(RELAXED) as usize
    }

    // ========================================================================
    //  Fallible Operations
    // ========================================================================

    /// Core drain logic: gather active inline suffixes, copy them + new suffix
    /// into a [`SuffixBag`], and return it.
    ///
    /// `active_slots` yields `(slot_index, skip_new_slot)` tuples representing
    /// which inline slots to copy. `buffer` optionally provides a pre-allocated
    /// `Vec<u8>` to reuse.
    #[expect(clippy::indexing_slicing)]
    fn drain_core(
        &self,
        active_slots: impl Iterator<Item = usize>,
        new_slot: usize,
        new_suffix: &[u8],
        buffer: Option<Vec<u8>>,
    ) -> SuffixBag {
        let mut required_capacity: usize = new_suffix.len();

        let mut slots_to_copy: [(usize, usize, usize); WIDTH] = [(0, 0, 0); WIDTH];
        let mut copy_count: usize = 0;

        // SAFETY: We hold the lock, so data buffer is stable
        let data: &[u8; CAPACITY] = unsafe { &*self.data.get() };

        for slot in active_slots {
            if (slot != new_slot) && (slot < WIDTH) {
                let meta: InlineSlotMeta = self.load_meta(slot);

                if meta.has_suffix() {
                    let start: usize = meta.offset as usize;
                    let len: usize = meta.len as usize;
                    required_capacity += len;

                    if copy_count < WIDTH {
                        slots_to_copy[copy_count] = (slot, start, len);
                        copy_count += 1;
                    }
                }
            }
        }

        // Reserve headroom for future inserts into this leaf, avoiding
        // repeated drain-and-rebuild cycles as the leaf fills up.
        // Heuristic: assume remaining slots will need similar-sized suffixes.
        let avg_suffix: usize = if copy_count > 0 {
            required_capacity / (copy_count + 1)
        } else {
            new_suffix.len()
        };
        let headroom: usize = (WIDTH - (copy_count + 1)) * avg_suffix;
        let target_capacity: usize = required_capacity + headroom;

        let mut external: SuffixBag = match buffer {
            Some(vec) => {
                let mut bag: SuffixBag = SuffixBag::from_vec(vec);
                if bag.capacity() < target_capacity {
                    bag.reserve(target_capacity.saturating_sub(bag.used()));
                }
                bag
            }
            None => SuffixBag::with_capacity(target_capacity),
        };

        for &(slot, start, len) in &slots_to_copy[..copy_count] {
            let suffix: &[u8] = &data[start..(start + len)];
            external.assign(slot, suffix);
        }

        external.assign(new_slot, new_suffix);

        external
    }

    /// Drain inline suffixes to external bag (normal operation).
    ///
    /// Uses the permutation to find active slots. This is the common case
    /// for suffix overflow during normal inserts.
    pub fn drain_to_external(
        &self,
        perm: &impl TreePermutation,
        new_slot: usize,
        new_suffix: &[u8],
    ) -> SuffixBag {
        self.drain_core(
            (0..perm.size()).map(|i: usize| perm.get(i)),
            new_slot,
            new_suffix,
            None,
        )
    }

    /// Drain inline suffixes to external bag, reusing a pre-allocated buffer.
    ///
    /// # Panics
    ///
    /// On OOM, while trying to allocate for [`SuffixBag`].
    pub fn drain_to_external_with_vec(
        &self,
        perm: &impl TreePermutation,
        new_slot: usize,
        new_suffix: &[u8],
        buffer: Vec<u8>,
    ) -> SuffixBag {
        self.drain_core(
            (0..perm.size()).map(|i: usize| perm.get(i)),
            new_slot,
            new_suffix,
            Some(buffer),
        )
    }

    /// Drain inline suffixes to external bag during node initialization.
    #[cold]
    pub fn drain_to_external_init(&self, new_slot: usize, new_suffix: &[u8]) -> SuffixBag {
        self.drain_core(0..new_slot, new_slot, new_suffix, None)
    }

    // ========================================================================
    //  Slot Access (Read)
    // ========================================================================

    /// Check if a slot has a suffix.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `slot >= 15`.
    #[must_use]
    #[inline(always)]
    pub fn has_suffix(&self, slot: usize) -> bool {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");
        self.load_meta(slot).has_suffix()
    }

    /// Get the suffix for a slot, or `None` if no suffix.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `slot >= 15`.
    #[must_use]
    #[inline(always)]
    pub fn get(&self, slot: usize) -> Option<&[u8]> {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");

        let meta: InlineSlotMeta = self.load_meta(slot);

        if !meta.has_suffix() {
            return None;
        }

        let start: usize = meta.offset as usize;
        let len: usize = meta.len as usize;

        // INVARIANT: Valid metadata points to valid data range.
        debug_assert!(
            start + len <= CAPACITY,
            "inline suffix metadata points past capacity: {} > {CAPACITY}",
            start + len
        );

        // SAFETY:
        // - Metadata bounds are validated by invariant (debug_assert above)
        // - Readers retry if version changed (OCC protocol)
        // - Writers hold the lock, ensuring no concurrent writes
        let data_ptr: *const u8 = self.data.get().cast::<u8>();
        let suffix: &[u8] = unsafe { std::slice::from_raw_parts(data_ptr.add(start), len) };

        Some(suffix)
    }

    /// Get the suffix for a slot, or empty slice if no suffix.
    #[must_use]
    #[inline(always)]
    pub fn get_or_empty(&self, slot: usize) -> &[u8] {
        self.get(slot).unwrap_or(&[])
    }

    // ========================================================================
    //  Suffix Assignment (Write - requires lock)
    // ========================================================================

    /// Try to assign a suffix to a slot in-place.
    ///
    /// # Panics
    ///
    /// Panics if `slot >= 15` or if suffix length exceeds `u16::MAX`.
    #[inline]
    pub fn try_assign(&self, slot: usize, suffix: &[u8]) -> bool {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");

        let suffix_len: usize = suffix.len();

        if suffix_len > U16_MAX {
            return false;
        }

        let meta: InlineSlotMeta = self.load_meta(slot);

        // SAFETY: We hold the lock, exclusive write access to data buffer.
        // Readers use OCC validation to detect our writes.
        let data_ptr: *mut u8 = self.data.get().cast::<u8>();

        if meta.has_suffix() && (suffix_len <= (meta.len as usize)) {
            let start: usize = meta.offset as usize;
            // SAFETY: start + suffix_len <= meta.len <= CAPACITY (invariant)
            unsafe {
                StdPtr::copy_nonoverlapping(suffix.as_ptr(), data_ptr.add(start), suffix_len);
            }

            #[expect(clippy::cast_possible_truncation, reason = "len checked above")]
            self.store_meta(
                slot,
                InlineSlotMeta {
                    offset: meta.offset,
                    len: suffix_len as u16,
                },
            );

            // Count unchanged, slot already had suffix
            return true;
        }

        let current_size: usize = self.size.load(RELAXED) as usize;

        if (current_size + suffix_len) <= CAPACITY {
            // SAFETY: current_size + suffix_len <= CAPACITY (checked above)
            unsafe {
                StdPtr::copy_nonoverlapping(
                    suffix.as_ptr(),
                    data_ptr.add(current_size),
                    suffix_len,
                );
            }

            // Update count if this is a new suffix
            if !meta.has_suffix() {
                self.suffix_count.fetch_add(1, RELAXED);
            }

            #[expect(
                clippy::cast_possible_truncation,
                reason = "offset and len checked to fit"
            )]
            {
                self.store_meta(
                    slot,
                    InlineSlotMeta {
                        offset: current_size as u16,
                        len: suffix_len as u16,
                    },
                );

                self.size.store((current_size + suffix_len) as u16, RELAXED);
            }

            return true;
        }

        // Out of capacity, caller should drain to external
        false
    }

    /// Clear the suffix for a slot.
    ///
    /// # Safety
    ///
    /// Caller must hold the leaf lock.
    #[inline(always)]
    pub fn clear(&self, slot: usize) {
        debug_assert!(slot < WIDTH, "slot {slot} >= WIDTH {WIDTH}");

        if self.load_meta(slot).has_suffix() {
            self.suffix_count.fetch_sub(1, RELAXED);
        }

        self.store_meta(slot, InlineSlotMeta::EMPTY);
    }

    /// Clear all slots and reset size to zero.
    ///
    /// Used after draining to an external bag.
    ///
    /// # Safety
    ///
    /// Caller must hold the leaf lock.
    #[inline(always)]
    pub fn clear_all(&self) {
        for slot in 0..WIDTH {
            self.store_meta(slot, InlineSlotMeta::EMPTY);
        }
        self.size.store(0, RELAXED);
        self.suffix_count.store(0, RELAXED);
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
    pub fn suffix_compare(&self, slot: usize, suffix: &[u8]) -> Option<Ordering> {
        self.get(slot).map(|stored: &[u8]| stored.cmp(suffix))
    }
}

impl Default for InlineSuffixBag {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for InlineSuffixBag {
    #[inline(always)]
    fn clone(&self) -> Self {
        // SAFETY: Clone is typically called under exclusive access or during init
        let data: [u8; CAPACITY] = unsafe { *self.data.get() };
        Self {
            slots: std::array::from_fn(|i: usize| AtomicU32::new(self.slots[i].load(RELAXED))),
            size: AtomicU16::new(self.size.load(RELAXED)),
            suffix_count: AtomicU8::new(self.suffix_count.load(RELAXED)),
            _pad: 0,
            data: UnsafeCell::new(data),
        }
    }
}
