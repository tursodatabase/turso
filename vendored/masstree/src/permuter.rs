//! Filepath: src/permuter.rs
//!
//! Permutation array for leaf node slot ordering.
//!
//! The [`Permuter`] encodes a permutation of slot indices in a single `u64`,
//! enabling O(1) logical reordering without moving key/value data.

/// Maximum allowed WIDTH for u64 permuter encoding.
pub const MAX_WIDTH: usize = 15;

/// Mask for extracting size (lower 4 bits).
const SIZE_MASK: u64 = 0xF;

use crate::{leaf_trait::TreePermutation, suffix::PermutationProvider};

/// Namespace for const helper functions used by [`Permuter`].
struct PermuterUtils;

impl PermuterUtils {
    /// Compute initial permuter value for a given WIDTH.
    const fn compute_initial_value<const WIDTH: usize>() -> u64 {
        let mut value: u64 = 0;
        let mut i: usize = 0;

        while i < WIDTH {
            let slot: u64 = (WIDTH - 1 - i) as u64;

            value |= slot << ((i * 4) + 4);
            i += 1;
        }

        value
    }

    const fn compute_sorted_value<const WIDTH: usize>() -> u64 {
        let mut value: u64 = 0;
        let mut i: usize = 0;

        while i < WIDTH {
            value |= (i as u64) << ((i * 4) + 4);
            i += 1;
        }

        value
    }
}

/// A permutation of slot indices for a leaf node.
///
/// Encodes which physical slot holds the key at each logical position.
/// The logical positions `0..size()` are in sorted key order.
///
/// # Type Parameters
///
/// * `WIDTH` - Number of slots (default: 15, max: 15)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Permuter<const WIDTH: usize = 15> {
    value: u64,
}

/// Compile-time assertion: WIDTH must be in range 1..=15
impl<const WIDTH: usize> Permuter<WIDTH> {
    /// Compile-time check that WIDTH is valid.
    const WIDTH_CHECK: () = {
        assert!(WIDTH > 0, "WIDTH must be at least 1");

        assert!(
            WIDTH <= MAX_WIDTH,
            "WIDTH must be at most 15 (u64 encoding limit)"
        );
    };

    /// Initial value with slots in reverse order, size = 0.
    pub const INITIAL: u64 = PermuterUtils::compute_initial_value::<WIDTH>();

    /// Sorted value: position i -> slot i, size = 0.
    ///
    /// For WIDTH=15: `0xEDCB_A987_6543_2100`
    const SORTED: u64 = PermuterUtils::compute_sorted_value::<WIDTH>();

    /// Mask covering all valid bits (size + WIDTH positions).
    const WIDTH_MASK: u64 = {
        let width_shift: usize = (WIDTH + 1) * 4;
        if width_shift >= 64 {
            u64::MAX
        } else {
            (1u64 << width_shift) - 1
        }
    };
}

impl<const WIDTH: usize> Default for Permuter<WIDTH> {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

/// Type alias for standard 15-slot permuter.
pub type Permuter15 = Permuter<15>;

impl<const WIDTH: usize> Permuter<WIDTH> {
    /// Create an empty permuter with size 0.
    #[must_use]
    #[inline(always)]
    pub const fn empty() -> Self {
        // Trigger compile-time WIDTH check
        let _: () = Self::WIDTH_CHECK;

        Self {
            value: Self::INITIAL,
        }
    }

    /// Create a sorted permuter with `n` elements.
    ///
    /// # Panics
    ///
    /// Panics if `n > WIDTH`.
    #[must_use]
    pub fn make_sorted(n: usize) -> Self {
        debug_assert!(n <= WIDTH, "make_sorted: n ({n}) > WIDTH ({WIDTH})");

        if n == WIDTH {
            return Self {
                value: Self::SORTED | (WIDTH as u64),
            };
        }

        let sorted: u64 = Self::SORTED;

        let mut value: u64 = n as u64;

        let sorted_mask: u64 = ((1u64 << (n * 4)) - 1) << 4;
        value |= sorted & sorted_mask;

        let mut pos: usize = n;

        while pos < WIDTH {
            let slot: u64 = (WIDTH - 1 - (pos - n)) as u64;
            value |= slot << ((pos * 4) + 4);
            pos += 1;
        }

        Self { value }
    }

    /// Return the number of slots in use.
    #[must_use]
    #[inline(always)]
    pub const fn size(&self) -> usize {
        (self.value & SIZE_MASK) as usize
    }

    /// Return the slot index at logical position `i`.
    ///
    /// # Panics
    /// Panics in debug mode if `i >= WIDTH`.
    #[must_use]
    #[inline(always)]
    pub const fn get(&self, i: usize) -> usize {
        debug_assert!(i < WIDTH, "get: index out of bounds");

        // Shift: skip 4-bit size field, then 4 bits per position
        ((self.value >> ((i * 4) + 4)) & 0xF) as usize
    }

    /// Return the slot at the back (position WIDTH - 1)
    #[must_use]
    #[inline(always)]
    pub const fn back(&self) -> usize {
        self.get(WIDTH - 1)
    }

    /// Get the slot at `back()` with an offset into the free region.
    ///
    /// # Panics
    /// Debug-panics if `size() + offset >= WIDTH` (no more free slots at that offset).
    #[must_use]
    #[inline(always)]
    pub const fn back_at_offset(&self, offset: usize) -> usize {
        debug_assert!(
            self.size() + offset < WIDTH,
            "back_at_offset: offset exceeds free slots"
        );
        self.get(WIDTH - 1 - offset)
    }

    /// Return the raw u64 value.
    #[must_use]
    #[inline(always)]
    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Create a Permuter from a raw u64 value.
    #[must_use]
    #[inline(always)]
    pub const fn from_value(value: u64) -> Self {
        Self { value }
    }

    /// Set the size without changing slot positions.
    ///
    /// # Panics
    /// Debug-panics if `n > WIDTH`.
    #[inline(always)]
    pub fn set_size(&mut self, n: usize) {
        debug_assert!(n <= WIDTH, "set_size: n ({n}) > WIDTH ({WIDTH})");

        self.value = (self.value & !SIZE_MASK) | (n as u64);
    }

    /// Set the slot at a given position.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if position or slot is out of range.
    #[inline(always)]
    pub fn set(&mut self, i: usize, slot: usize) {
        debug_assert!(i < WIDTH, "set: position {i} >= WIDTH {WIDTH}");
        debug_assert!(slot < WIDTH, "set: slot {slot} >= WIDTH {WIDTH}");

        let shift: usize = (i + 1) * 4;
        let mask: u64 = 0xFu64 << shift;
        self.value = (self.value & !mask) | ((slot as u64) << shift);
    }

    /// Swap two slots in the free region (positions >= size).
    ///
    /// # Panics
    ///
    /// Panics in debug mode if positions are not in the free region.
    #[inline(always)]
    pub fn swap_free_slots(&mut self, pos_i: usize, pos_j: usize) {
        let size: usize = self.size();

        debug_assert!(
            pos_i >= size,
            "swap_free_slots: pos_i ({pos_i}) must be >= size ({size})"
        );
        debug_assert!(
            pos_j >= size,
            "swap_free_slots: pos_j ({pos_j}) must be >= size ({size})"
        );
        debug_assert!(pos_i < WIDTH, "swap_free_slots: pos_i out of range");
        debug_assert!(pos_j < WIDTH, "swap_free_slots: pos_j out of range");

        if pos_i == pos_j {
            return; // Nothing to swap
        }

        let i_shift: usize = (pos_i + 1) * 4;
        let j_shift: usize = (pos_j + 1) * 4;
        let diff: u64 = ((self.value >> i_shift) ^ (self.value >> j_shift)) & 0xF;

        self.value ^= (diff << i_shift) | (diff << j_shift);
    }

    /// Allocate a slot from the back and insert it at position `i`.
    ///
    /// # Panics
    /// Panics in debug mode if `i > size()` or `size() >= WIDTH`.
    #[must_use]
    #[inline(always)]
    pub fn insert_from_back(&mut self, i: usize) -> usize {
        debug_assert!(i <= self.size(), "insert_from_back: i > size");
        debug_assert!(self.size() < WIDTH, "insert_from_back: permuter full");

        let slot: usize = self.back();
        let i_shift: usize = (i * 4) + 4;
        let low_mask: u64 = (1u64 << i_shift) - 1;

        self.value = ((self.value + 1) & low_mask)
            | ((slot as u64) << i_shift)
            | ((self.value << 4) & !(low_mask | (0xF << i_shift)));

        #[cfg(debug_assertions)]
        self.debug_assert_valid();

        slot
    }

    /// Compute the result of `insert_from_back` without mutating self.
    ///
    /// # Panics
    /// Panics in debug mode if `i > size()` or `size() >= WIDTH`.
    #[must_use]
    #[inline(always)]
    pub fn insert_from_back_immutable(&self, i: usize) -> (Self, usize) {
        debug_assert!(i <= self.size(), "insert_from_back_immutable: i > size");
        debug_assert!(
            self.size() < WIDTH,
            "insert_from_back_immutable: permuter full"
        );

        let slot: usize = self.back();
        let i_shift: usize = (i * 4) + 4;
        let low_mask: u64 = (1u64 << i_shift) - 1;

        let new_value: u64 = ((self.value + 1) & low_mask)
            | ((slot as u64) << i_shift)
            | ((self.value << 4) & !(low_mask | (0xF << i_shift)));

        let new_perm = Self { value: new_value };

        #[cfg(debug_assertions)]
        new_perm.debug_assert_valid();

        (new_perm, slot)
    }

    /// Remove the element at position `i` and move it to the back.
    ///
    /// # Panics
    /// Panics in debug mode if `i >= size()`.
    #[inline(always)]
    pub fn remove_to_back(&mut self, i: usize) {
        debug_assert!(i < self.size(), "remove_to_back: i >= size");

        let i_shift: usize = (i + 1) * 4;
        let mask: u64 = !((1u64 << i_shift) - 1);
        let x: u64 = self.value & Self::WIDTH_MASK;
        let shift_to_back: usize = (WIDTH - i - 1) * 4;

        self.value = ((x - 1) & !mask) | ((x >> 4) & mask) | ((x & mask) << shift_to_back);

        #[cfg(debug_assertions)]
        self.debug_assert_valid();
    }

    /// Remove the element at position `i`.
    ///
    /// # Panics
    /// Panics in debug mode if `i >= size()`.
    #[inline(always)]
    pub fn remove(&mut self, i: usize) {
        let size: usize = self.size();
        debug_assert!(i < size, "remove: i >= size");

        // Fast path: removing the last element
        if size == i + 1 {
            self.value -= 1;

            #[cfg(debug_assertions)]
            self.debug_assert_valid();

            return;
        }

        let rot_amount: usize = (size - i - 1) * 4;

        let rot_mask: u64 = (((1u64 << rot_amount) << 4) - 1) << ((i + 1) * 4);

        self.value = ((self.value - 1) & !rot_mask)
            | (((self.value & rot_mask) >> 4) & rot_mask)
            | (((self.value & rot_mask) << rot_amount) & rot_mask);

        #[cfg(debug_assertions)]
        self.debug_assert_valid();
    }

    /// Remove a physical slot from the permutation.
    ///
    /// # Panics
    /// Debug-panics if `slot` is not found in the in-use portion of the permutation.
    #[inline(always)]
    pub fn remove_slot(&mut self, slot: usize) {
        let size = self.size();
        for i in 0..size {
            if self.get(i) == slot {
                self.remove(i);
                return;
            }
        }
        debug_assert!(false, "remove_slot: slot {slot} not in permutation");
    }

    /// Exchange (swap) the elements at positions `i` and `j`.
    ///
    /// # Panics
    /// Panics in debug mode if `i >= WIDTH` or `j >= WIDTH`.
    #[inline(always)]
    pub fn exchange(&mut self, i: usize, j: usize) {
        debug_assert!(i < WIDTH, "exchange: i >= WIDTH");
        debug_assert!(j < WIDTH, "exchange: j >= WIDTH");

        if i == j {
            return;
        }

        // Shift amounts for positions i and j (add 4 to skip size bits)
        let i_shift: usize = (i + 1) * 4;
        let j_shift: usize = (j + 1) * 4;

        let diff: u64 = ((self.value >> i_shift) ^ (self.value >> j_shift)) & 0xF;
        self.value ^= (diff << i_shift) | (diff << j_shift);

        #[cfg(debug_assertions)]
        self.debug_assert_valid();
    }

    /// Rotate elements from position `i` to end, moving positions `i..j` to the back.
    ///
    /// # Panics
    /// Panics in debug mode if `i > j` or `j > WIDTH`.
    #[inline(always)]
    pub fn rotate(&mut self, i: usize, j: usize) {
        debug_assert!(i <= j, "rotate: i > j");
        debug_assert!(j <= WIDTH, "rotate: j > WIDTH");

        if i == j || i == WIDTH {
            return;
        }

        let i_shift: usize = (i + 1) * 4;
        let mask: u64 = (1u64 << i_shift) - 1;

        let x: u64 = self.value & Self::WIDTH_MASK;

        let rotate_amount: usize = (j - i) * 4;
        let rotate_back: usize = (WIDTH - j) * 4;

        self.value = (x & mask) | ((x >> rotate_amount) & !mask) | ((x & !mask) << rotate_back);

        #[cfg(debug_assertions)]
        self.debug_assert_valid();
    }

    /// Verify permuter invariants (debug builds only).
    ///
    /// Checks:
    /// - Size is in range [0, WIDTH]
    /// - All slot indices 0-14 appear exactly once
    ///
    /// # Panics
    /// If any of the invariants are not satisfied.
    #[cfg(debug_assertions)]
    pub fn debug_assert_valid(&self) {
        let size: usize = self.size();
        assert!(size <= WIDTH, "invalid size: {size} > {WIDTH}");

        // Check all slots 0..(WIDTH-1) appear exactly once
        let mut seen: u16 = 0;

        for i in 0..WIDTH {
            let slot: usize = self.get(i);
            assert!(slot < WIDTH, "invalid slot index: {slot}");

            let bit: u16 = 1u16 << slot;
            assert!(seen & bit == 0, "duplicate slot index: {slot}");

            seen |= bit;
        }

        assert_eq!(seen, (1u16 << WIDTH) - 1, "missing slot indices");
    }

    /// Verify permuter invariants (no-op in release builds).
    #[inline]
    #[cfg(not(debug_assertions))]
    pub const fn debug_assert_valid(&self) {}
}

// ============================================================================
//  PermutationProvider Implementation
// ============================================================================

impl<const WIDTH: usize> PermutationProvider for Permuter<WIDTH> {
    #[inline(always)]
    fn size(&self) -> usize {
        self.size()
    }

    #[inline(always)]
    fn get(&self, i: usize) -> usize {
        self.get(i)
    }
}

// ============================================================================
//  AtomicPermuter (u64-based atomic wrapper)
// ============================================================================

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic wrapper for [`Permuter<WIDTH>`] where WIDTH <= 15.
#[derive(Debug)]
#[repr(transparent)]
pub struct AtomicPermuter<const WIDTH: usize = 15> {
    inner: AtomicU64,
}

impl<const WIDTH: usize> AtomicPermuter<WIDTH> {
    /// Create with empty permutation.
    #[must_use]
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            inner: AtomicU64::new(Permuter::<WIDTH>::INITIAL),
        }
    }

    /// Create from existing permuter.
    #[must_use]
    #[inline(always)]
    pub const fn from_permuter(perm: Permuter<WIDTH>) -> Self {
        Self {
            inner: AtomicU64::new(perm.value),
        }
    }

    /// Load with ordering.
    #[inline(always)]
    pub fn load(&self, order: Ordering) -> Permuter<WIDTH> {
        Permuter::from_value(self.inner.load(order))
    }

    /// Store with ordering.
    #[inline(always)]
    pub fn store(&self, val: Permuter<WIDTH>, order: Ordering) {
        self.inner.store(val.value, order);
    }

    /// Compare-and-exchange.
    ///
    /// # Errors
    ///
    /// Returns `Err` with the current value if the comparison failed.
    #[inline(always)]
    pub fn compare_exchange(
        &self,
        expected: Permuter<WIDTH>,
        new: Permuter<WIDTH>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Permuter<WIDTH>, Permuter<WIDTH>> {
        self.inner
            .compare_exchange(expected.value, new.value, success, failure)
            .map(Permuter::from_value)
            .map_err(Permuter::from_value)
    }

    /// Compare-and-exchange weak.
    ///
    /// # Errors
    ///
    /// Returns `Err` with the current value if the comparison failed, or spuriously.
    #[inline(always)]
    pub fn compare_exchange_weak(
        &self,
        expected: Permuter<WIDTH>,
        new: Permuter<WIDTH>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Permuter<WIDTH>, Permuter<WIDTH>> {
        self.inner
            .compare_exchange_weak(expected.value, new.value, success, failure)
            .map(Permuter::from_value)
            .map_err(Permuter::from_value)
    }

    /// Load raw u64 value.
    #[inline(always)]
    pub fn load_raw(&self, order: Ordering) -> u64 {
        self.inner.load(order)
    }

    /// Store raw u64 value.
    #[inline(always)]
    pub fn store_raw(&self, val: u64, order: Ordering) {
        self.inner.store(val, order);
    }

    /// CAS on raw value.
    ///
    /// # Errors
    ///
    /// Returns `Err` with the current value if the comparison failed.
    #[inline(always)]
    pub fn compare_exchange_raw(
        &self,
        expected: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.inner.compare_exchange(expected, new, success, failure)
    }
}

impl<const WIDTH: usize> Default for AtomicPermuter<WIDTH> {
    fn default() -> Self {
        Self::new()
    }
}

/// Type alias for the standard 15-slot atomic permuter.
pub type AtomicPermuter15 = AtomicPermuter<15>;

// ============================================================================
//  TreePermutation Implementation
// ============================================================================

impl<const WIDTH: usize> TreePermutation for Permuter<WIDTH> {
    type Raw = u64;
    const WIDTH: usize = WIDTH;

    #[inline(always)]
    fn empty() -> Self {
        Self::empty()
    }

    #[inline(always)]
    fn make_sorted(n: usize) -> Self {
        Self::make_sorted(n)
    }

    #[inline(always)]
    fn from_value(raw: u64) -> Self {
        Self::from_value(raw)
    }

    #[inline(always)]
    fn value(&self) -> u64 {
        self.value
    }

    #[inline(always)]
    fn size(&self) -> usize {
        self.size()
    }

    #[inline(always)]
    fn get(&self, i: usize) -> usize {
        self.get(i)
    }

    #[inline(always)]
    fn back(&self) -> usize {
        self.back()
    }

    #[inline(always)]
    fn back_at_offset(&self, offset: usize) -> usize {
        self.back_at_offset(offset)
    }

    #[inline(always)]
    fn insert_from_back(&mut self, i: usize) -> usize {
        Self::insert_from_back(self, i)
    }

    #[inline(always)]
    fn insert_from_back_immutable(&self, i: usize) -> (Self, usize) {
        Self::insert_from_back_immutable(self, i)
    }

    #[inline(always)]
    fn swap_free_slots(&mut self, pos_i: usize, pos_j: usize) {
        Self::swap_free_slots(self, pos_i, pos_j);
    }

    #[inline(always)]
    fn set_size(&mut self, n: usize) {
        Self::set_size(self, n);
    }

    #[inline(always)]
    fn remove(&mut self, i: usize) {
        Self::remove(self, i);
    }
}

