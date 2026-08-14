//! True-inline value array implementation.

use std::marker::PhantomData;
use std::ptr as StdPtr;
use std::sync::atomic::{AtomicPtr, AtomicU64};

use crate::inline::bits::InlineBits;
use crate::inline::sentinel::InlineSentinel;
use crate::leaf15::WIDTH_15;
use crate::ordering::{READ_ORD, RELAXED, WRITE_ORD};

use super::RetireHandle;
use super::ValueArray;

// ============================================================================
//  InlineValueArray<V>
// ============================================================================

/// Value array with dual storage: state tags + inline bits.
#[repr(C)]
pub struct InlineValueArray<V: InlineBits> {
    /// State discriminator tags.
    tags: [AtomicPtr<u8>; WIDTH_15],

    /// Inline value bits storage.
    values: [AtomicU64; WIDTH_15],

    _marker: PhantomData<V>,
}

// SAFETY: InlineValueArray is Send+Sync when V: Send+Sync (which InlineBits requires).
// The AtomicPtr and AtomicU64 provide thread-safe access. Layer pointers stored
// in tags are protected by the tree's concurrency protocol (OCC + locks).
unsafe impl<V: InlineBits> Send for InlineValueArray<V> {}
unsafe impl<V: InlineBits> Sync for InlineValueArray<V> {}

impl<V: InlineBits> ValueArray<V> for InlineValueArray<V> {
    #[inline(always)]
    fn new() -> Self {
        // SAFETY: InlineValueArray is #[repr(C)]. All fields are zero-safe:
        // - tags: [AtomicPtr<u8>; 15] — null pointers are all-zero-bits
        // - values: [AtomicU64; 15] — zero is all-zero-bits
        // - _marker: PhantomData<V> — ZST, no bytes
        // AtomicPtr/AtomicU64 are #[repr(C)] wrapping UnsafeCell; 0/null is 0.
        unsafe { std::mem::zeroed() }
    }

    // ========================================================================
    //  Slot Classification
    // ========================================================================

    #[inline(always)]
    fn is_empty(&self, slot: usize) -> bool {
        debug_assert!(slot < WIDTH_15, "is_empty: slot {slot} out of bounds");
        self.tags[slot].load(READ_ORD).is_null()
    }

    #[inline(always)]
    fn is_empty_relaxed(&self, slot: usize) -> bool {
        debug_assert!(
            slot < WIDTH_15,
            "is_empty_relaxed: slot {slot} out of bounds"
        );
        self.tags[slot].load(RELAXED).is_null()
    }

    #[inline(always)]
    fn is_layer(&self, slot: usize) -> bool {
        debug_assert!(slot < WIDTH_15, "is_layer: slot {slot} out of bounds");

        let tag: *mut u8 = self.tags[slot].load(READ_ORD);

        !tag.is_null() && !InlineSentinel::is_inline_sentinel(tag)
    }

    // ========================================================================
    //  Terminal Value Operations
    // ========================================================================

    #[inline(always)]
    fn load(&self, slot: usize) -> Option<V> {
        debug_assert!(slot < WIDTH_15, "load: slot {slot} out of bounds");

        let tag: *mut u8 = self.tags[slot].load(READ_ORD);

        if InlineSentinel::is_inline_sentinel(tag) {
            let bits: u64 = self.values[slot].load(RELAXED);

            Some(V::from_bits(bits))
        } else {
            None
        }
    }

    #[inline(always)]
    fn store(&self, slot: usize, output: &V) {
        debug_assert!(slot < WIDTH_15, "store: slot {slot} out of bounds");

        self.values[slot].store(output.to_bits(), RELAXED);
        self.tags[slot].store(InlineSentinel::inline_sentinel_ptr(), WRITE_ORD);
    }

    #[inline(always)]
    fn store_relaxed(&self, slot: usize, output: &V) {
        debug_assert!(slot < WIDTH_15, "store_relaxed: slot {slot} out of bounds");

        self.values[slot].store(output.to_bits(), RELAXED);
        self.tags[slot].store(InlineSentinel::inline_sentinel_ptr(), RELAXED);
    }

    #[inline(always)]
    fn update_in_place(&self, slot: usize, output: &V) -> RetireHandle {
        debug_assert!(
            slot < WIDTH_15,
            "update_in_place: slot {slot} out of bounds"
        );
        debug_assert!(
            InlineSentinel::is_inline_sentinel(self.tags[slot].load(RELAXED)),
            "update_in_place called on non-value slot {slot}"
        );

        self.values[slot].store(output.to_bits(), WRITE_ORD);

        RetireHandle::Noop
    }

    #[inline(always)]
    fn update_in_place_relaxed(&self, slot: usize, output: &V) -> RetireHandle {
        debug_assert!(
            slot < WIDTH_15,
            "update_in_place_relaxed: slot {slot} out of bounds"
        );
        debug_assert!(
            InlineSentinel::is_inline_sentinel(self.tags[slot].load(RELAXED)),
            "update_in_place_relaxed called on non-value slot {slot}"
        );

        self.values[slot].store(output.to_bits(), RELAXED);

        RetireHandle::Noop
    }

    #[inline(always)]
    fn take(&self, slot: usize) -> Option<V> {
        debug_assert!(slot < WIDTH_15, "take: slot {slot} out of bounds");

        let tag: *mut u8 = self.tags[slot].load(RELAXED);

        if InlineSentinel::is_inline_sentinel(tag) {
            let bits: u64 = self.values[slot].load(RELAXED);
            let value: V = V::from_bits(bits);

            self.tags[slot].store(StdPtr::null_mut(), RELAXED);

            Some(value)
        } else {
            None
        }
    }

    // ========================================================================
    //  Layer Pointer Operations
    // ========================================================================

    #[inline(always)]
    fn load_raw(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < WIDTH_15, "load_raw: slot {slot} out of bounds");

        self.tags[slot].load(READ_ORD)
    }

    #[inline(always)]
    fn load_raw_relaxed(&self, slot: usize) -> *mut u8 {
        debug_assert!(
            slot < WIDTH_15,
            "load_raw_relaxed: slot {slot} out of bounds"
        );

        self.tags[slot].load(RELAXED)
    }

    #[inline(always)]
    fn load_layer(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < WIDTH_15, "load_layer: slot {slot} out of bounds");

        self.tags[slot].load(READ_ORD)
    }

    #[inline(always)]
    fn store_layer(&self, slot: usize, ptr: *mut u8) {
        debug_assert!(slot < WIDTH_15, "store_layer: slot {slot} out of bounds");
        debug_assert!(
            !ptr.is_null(),
            "store_layer: null layer pointer at slot {slot}"
        );
        debug_assert!(
            !InlineSentinel::is_inline_sentinel(ptr),
            "store_layer: sentinel pointer used as layer pointer at slot {slot}"
        );

        self.tags[slot].store(ptr, WRITE_ORD);
    }

    // ========================================================================
    //  Relaxed Load (under lock)
    // ========================================================================

    #[inline(always)]
    fn load_relaxed(&self, slot: usize) -> Option<V> {
        debug_assert!(slot < WIDTH_15, "load_relaxed: slot {slot} out of bounds");

        let tag: *mut u8 = self.tags[slot].load(RELAXED);

        if InlineSentinel::is_inline_sentinel(tag) {
            let bits: u64 = self.values[slot].load(RELAXED);

            Some(V::from_bits(bits))
        } else {
            None
        }
    }

    // ========================================================================
    //  Slot Management
    // ========================================================================

    #[inline(always)]
    fn clear(&self, slot: usize) {
        debug_assert!(slot < WIDTH_15, "clear: slot {slot} out of bounds");

        self.tags[slot].store(StdPtr::null_mut(), WRITE_ORD);
    }

    #[inline(always)]
    fn clear_relaxed(&self, slot: usize) {
        debug_assert!(slot < WIDTH_15, "clear_relaxed: slot {slot} out of bounds");

        self.tags[slot].store(StdPtr::null_mut(), RELAXED);
    }

    #[inline(always)]
    fn move_slot(&self, dst: &Self, src_slot: usize, dst_slot: usize) {
        debug_assert!(
            src_slot < WIDTH_15,
            "move_slot: src_slot {src_slot} out of bounds"
        );
        debug_assert!(
            dst_slot < WIDTH_15,
            "move_slot: dst_slot {dst_slot} out of bounds"
        );

        let tag: *mut u8 = self.tags[src_slot].load(RELAXED);

        if InlineSentinel::is_inline_sentinel(tag) {
            let bits: u64 = self.values[src_slot].load(RELAXED);

            dst.values[dst_slot].store(bits, WRITE_ORD);
            dst.tags[dst_slot].store(InlineSentinel::inline_sentinel_ptr(), WRITE_ORD);
        } else {
            dst.tags[dst_slot].store(tag, WRITE_ORD);
        }
    }

    #[inline(always)]
    fn move_slot_relaxed(&self, dst: &Self, src_slot: usize, dst_slot: usize) {
        debug_assert!(
            src_slot < WIDTH_15,
            "move_slot_relaxed: src_slot {src_slot} out of bounds"
        );
        debug_assert!(
            dst_slot < WIDTH_15,
            "move_slot_relaxed: dst_slot {dst_slot} out of bounds"
        );

        let tag: *mut u8 = self.tags[src_slot].load(RELAXED);

        if InlineSentinel::is_inline_sentinel(tag) {
            let bits: u64 = self.values[src_slot].load(RELAXED);

            dst.values[dst_slot].store(bits, RELAXED);
            dst.tags[dst_slot].store(InlineSentinel::inline_sentinel_ptr(), RELAXED);
        } else {
            dst.tags[dst_slot].store(tag, RELAXED);
        }
    }

    // ========================================================================
    //  Lifecycle
    // ========================================================================

    #[inline(always)]
    unsafe fn cleanup(&self, _slot: usize) {}
}
