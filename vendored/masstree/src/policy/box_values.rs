//! Box-based value array: stores `Box<V>` as raw pointers in `[AtomicPtr<u8>; 15]`.
//!
//! For `V` where `size_of::<V>() <= 8`, the array supports write-through updates:
//! the old value is read and the new value is written in place through the Box
//! pointer, avoiding allocation and retirement on updates.

use std::marker::PhantomData;
use std::mem::{self as StdMem, size_of};
use std::ptr as StdPtr;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering};

use crate::leaf15::WIDTH_15;
use crate::ordering::{READ_ORD, RELAXED, WRITE_ORD};

use super::RetireHandle;
use super::ValueArray;
use super::ValuePtr;

// ============================================================================
//  Atomic Value Helpers (for write-through: V <= 8 bytes)
// ============================================================================

/// Dispatch an atomic load by `size_of::<V>()`. Compile-time dead-code
/// elimination removes the unused arms. Must be called inside `unsafe`.
macro_rules! atomic_load_dispatch {
    ($ptr:expr, $ordering:expr) => {
        match size_of::<V>() {
            1 => StdMem::transmute_copy(&(*$ptr.cast::<AtomicU8>()).load($ordering)),

            2 => StdMem::transmute_copy(&(*$ptr.cast::<AtomicU16>()).load($ordering)),

            4 => StdMem::transmute_copy(&(*$ptr.cast::<AtomicU32>()).load($ordering)),

            8 => StdMem::transmute_copy(&(*$ptr.cast::<AtomicU64>()).load($ordering)),

            _ => unreachable!(),
        }
    };
}

/// Dispatch an atomic store by `size_of::<V>()`. Compile-time dead-code
/// elimination removes the unused arms. Must be called inside `unsafe`.
macro_rules! atomic_store_dispatch {
    ($ptr:expr, $value:expr, $ordering:expr) => {
        match size_of::<V>() {
            1 => (*$ptr.cast::<AtomicU8>()).store(StdMem::transmute_copy($value), $ordering),

            2 => (*$ptr.cast::<AtomicU16>()).store(StdMem::transmute_copy($value), $ordering),

            4 => (*$ptr.cast::<AtomicU32>()).store(StdMem::transmute_copy($value), $ordering),

            8 => (*$ptr.cast::<AtomicU64>()).store(StdMem::transmute_copy($value), $ordering),

            _ => unreachable!(),
        }
    };
}

/// Atomically read V from a Box allocation.
///
/// Dispatches on `size_of::<V>()` at compile time. Dead branches are
/// eliminated by the compiler.
///
/// # Safety
///
/// - `ptr` must point to a valid, naturally-aligned allocation of at
///   least `size_of::<V>()` bytes.
/// - `size_of::<V>()` must be 1, 2, 4, or 8 (enforced by `CAN_WRITE_THROUGH`).
#[inline(always)]
pub unsafe fn atomic_read_value<V>(ptr: *const u8, ordering: Ordering) -> V {
    // SAFETY: Caller guarantees alignment and size constraints.
    unsafe { atomic_load_dispatch!(ptr, ordering) }
}

/// Atomically write V to a Box allocation.
///
/// # Safety
///
/// Same preconditions as `atomic_read_value`.
#[inline(always)]
pub(super) unsafe fn atomic_write_value<V>(ptr: *mut u8, value: &V, ordering: Ordering) {
    // SAFETY: Caller guarantees alignment and size constraints.
    unsafe { atomic_store_dispatch!(ptr, value, ordering) }
}

// ============================================================================
//  BoxValueArray<V>
// ============================================================================

/// Value array storing `Box<V>` pointers in `[AtomicPtr<u8>; 15]`.
#[repr(C)]
pub struct BoxValueArray<V> {
    ptrs: [AtomicPtr<u8>; WIDTH_15],

    _marker: PhantomData<V>,
}

impl<V> BoxValueArray<V> {
    /// Load the raw pointer at `slot` without typed interpretation.
    #[inline(always)]
    pub(crate) fn load_raw(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < WIDTH_15, "load_raw: slot {slot} out of bounds");

        self.ptrs[slot].load(READ_ORD)
    }

    /// Load the raw pointer at `slot` with Relaxed ordering (for prefetching).
    #[inline(always)]
    pub(crate) fn load_raw_relaxed(&self, slot: usize) -> *mut u8 {
        debug_assert!(
            slot < WIDTH_15,
            "load_raw_relaxed: slot {slot} out of bounds"
        );

        self.ptrs[slot].load(RELAXED)
    }

    // ========================================================================
    //  Write-Through Operations (V <= 8 bytes)
    // ========================================================================

    /// Atomic write-through update: read old value, write new value, no allocation.
    ///
    /// Returns the old value by copy. No Box allocation or EBR retirement needed.
    ///
    /// # Safety
    ///
    /// - Slot must contain a non-null terminal value (not empty, not layer).
    /// - `size_of::<V>()` must be `<= 8`.
    /// - Caller must hold the leaf lock.
    #[inline(always)]
    pub(crate) unsafe fn write_through_update(&self, slot: usize, new_value: &V) -> V {
        debug_assert!(slot < WIDTH_15, "write_through_update: slot {slot} OOB");
        debug_assert!(size_of::<V>() <= 8, "write-through requires V <= 8 bytes");

        let box_ptr: *mut u8 = self.ptrs[slot].load(RELAXED);
        debug_assert!(
            !box_ptr.is_null(),
            "write_through_update on empty slot {slot}"
        );

        // SAFETY: box_ptr is a valid Box<V> allocation. CAN_WRITE_THROUGH
        // guarantees size 1/2/4/8 with natural alignment. Atomic load/store
        // eliminates the data race with concurrent OCC readers under Rust's
        // memory model.
        // Relaxed read: under lock, old value is for return only.
        // Relaxed write: lock Release-on-drop publishes the new value.
        let old_value: V = unsafe { atomic_read_value::<V>(box_ptr, RELAXED) };
        unsafe { atomic_write_value::<V>(box_ptr, new_value, RELAXED) };

        old_value
    }
}

// SAFETY: AtomicPtr provides thread-safe access; raw pointers are valid
// Box<V> or layer pointers protected by OCC + locks.
unsafe impl<V: Send + Sync> Send for BoxValueArray<V> {}
unsafe impl<V: Send + Sync> Sync for BoxValueArray<V> {}

impl<V: Send + Sync + 'static> ValueArray<ValuePtr<V>> for BoxValueArray<V> {
    #[inline(always)]
    fn new() -> Self {
        // SAFETY: All-zero is valid — AtomicPtr null is zero-bits, PhantomData is ZST.
        unsafe { StdMem::zeroed() }
    }

    // ========================================================================
    //  Slot Classification
    // ========================================================================

    #[inline(always)]
    fn is_empty(&self, slot: usize) -> bool {
        debug_assert!(slot < WIDTH_15, "is_empty: slot {slot} out of bounds");

        self.ptrs[slot].load(READ_ORD).is_null()
    }

    #[inline(always)]
    fn is_empty_relaxed(&self, slot: usize) -> bool {
        debug_assert!(
            slot < WIDTH_15,
            "is_empty_relaxed: slot {slot} out of bounds"
        );

        self.ptrs[slot].load(RELAXED).is_null()
    }

    #[inline(always)]
    fn is_layer(&self, slot: usize) -> bool {
        debug_assert!(slot < WIDTH_15, "is_layer: slot {slot} out of bounds");

        // Cannot distinguish values from layers — returns true for any non-null.
        // Authoritative check is `keylenx >= LAYER_KEYLENX` on the leaf.
        !self.ptrs[slot].load(READ_ORD).is_null()
    }

    // ========================================================================
    //  Terminal Value Operations
    // ========================================================================

    #[inline(always)]
    fn load(&self, slot: usize) -> Option<ValuePtr<V>> {
        debug_assert!(slot < WIDTH_15, "load: slot {slot} out of bounds");

        let ptr: *mut u8 = self.ptrs[slot].load(READ_ORD);
        if ptr.is_null() {
            return None;
        }

        // SAFETY: Caller verified keylenx < LAYER_KEYLENX. ptr was stored via
        // Box::into_raw. Valid while caller's EBR guard is held.
        //
        // NOTE: For write-through types (V <= 8 bytes): the Box allocation is never
        // swapped or retired on updates. Instead, its contents are modified
        // in place via write_through_update. On x86-64 and ARM64, aligned
        // reads of size_of::<V>() <= 8 bytes are naturally atomic at the
        // hardware level, so concurrent write-through does not produce torn
        // reads. The OCC version check ensures the reader uses a consistent
        // snapshot.
        unsafe { Some(ValuePtr::from_raw(ptr.cast::<V>())) }
    }

    #[inline(always)]
    fn store(&self, slot: usize, output: &ValuePtr<V>) {
        debug_assert!(slot < WIDTH_15, "store: slot {slot} out of bounds");

        let ptr: *mut u8 = output.as_ptr().cast::<u8>();
        self.ptrs[slot].store(ptr, WRITE_ORD);
    }

    #[inline(always)]
    fn store_relaxed(&self, slot: usize, output: &ValuePtr<V>) {
        debug_assert!(slot < WIDTH_15, "store_relaxed: slot {slot} out of bounds");

        let ptr: *mut u8 = output.as_ptr().cast::<u8>();
        self.ptrs[slot].store(ptr, RELAXED);
    }

    #[inline(always)]
    fn update_in_place(&self, slot: usize, output: &ValuePtr<V>) -> RetireHandle {
        debug_assert!(
            slot < WIDTH_15,
            "update_in_place: slot {slot} out of bounds"
        );

        let old_ptr: *mut u8 = self.ptrs[slot].load(RELAXED);
        debug_assert!(
            !old_ptr.is_null(),
            "update_in_place called on empty slot {slot}"
        );

        let new_ptr: *mut u8 = output.as_ptr().cast::<u8>();
        self.ptrs[slot].store(new_ptr, WRITE_ORD);

        RetireHandle::Ptr(old_ptr)
    }

    #[inline(always)]
    fn update_in_place_relaxed(&self, slot: usize, output: &ValuePtr<V>) -> RetireHandle {
        debug_assert!(
            slot < WIDTH_15,
            "update_in_place_relaxed: slot {slot} out of bounds"
        );

        let old_ptr: *mut u8 = self.ptrs[slot].load(RELAXED);
        debug_assert!(
            !old_ptr.is_null(),
            "update_in_place_relaxed called on empty slot {slot}"
        );

        let new_ptr: *mut u8 = output.as_ptr().cast::<u8>();
        self.ptrs[slot].store(new_ptr, RELAXED);

        RetireHandle::Ptr(old_ptr)
    }

    #[inline(always)]
    fn take(&self, slot: usize) -> Option<ValuePtr<V>> {
        debug_assert!(slot < WIDTH_15, "take: slot {slot} out of bounds");

        let old_ptr: *mut u8 = self.ptrs[slot].swap(StdPtr::null_mut(), RELAXED);

        if old_ptr.is_null() {
            return None;
        }

        // SAFETY: ptr was stored via Box::into_raw. Swap to null prevents double-free.
        unsafe { Some(ValuePtr::from_raw(old_ptr.cast::<V>())) }
    }

    // ========================================================================
    //  Layer Pointer Operations
    // ========================================================================

    #[inline(always)]
    fn load_raw(&self, slot: usize) -> *mut u8 {
        self.load_raw(slot)
    }

    #[inline(always)]
    fn load_raw_relaxed(&self, slot: usize) -> *mut u8 {
        self.load_raw_relaxed(slot)
    }

    #[inline(always)]
    fn load_layer(&self, slot: usize) -> *mut u8 {
        debug_assert!(slot < WIDTH_15, "load_layer: slot {slot} out of bounds");

        self.ptrs[slot].load(READ_ORD)
    }

    #[inline(always)]
    fn store_layer(&self, slot: usize, ptr: *mut u8) {
        debug_assert!(slot < WIDTH_15, "store_layer: slot {slot} out of bounds");

        self.ptrs[slot].store(ptr, WRITE_ORD);
    }

    // ========================================================================
    //  Relaxed Load (under lock)
    // ========================================================================

    #[inline(always)]
    fn load_relaxed(&self, slot: usize) -> Option<ValuePtr<V>> {
        debug_assert!(slot < WIDTH_15, "load_relaxed: slot {slot} out of bounds");

        let ptr: *mut u8 = self.ptrs[slot].load(RELAXED);

        if ptr.is_null() {
            return None;
        }

        // SAFETY: Caller verified keylenx < LAYER_KEYLENX. ptr was stored via
        // Box::into_raw. Valid while caller's EBR guard is held.
        unsafe { Some(ValuePtr::from_raw(ptr.cast::<V>())) }
    }

    // ========================================================================
    //  Slot Management
    // ========================================================================

    #[inline(always)]
    fn clear(&self, slot: usize) {
        debug_assert!(slot < WIDTH_15, "clear: slot {slot} out of bounds");

        self.ptrs[slot].store(StdPtr::null_mut(), WRITE_ORD);
    }

    #[inline(always)]
    fn clear_relaxed(&self, slot: usize) {
        debug_assert!(slot < WIDTH_15, "clear_relaxed: slot {slot} out of bounds");

        self.ptrs[slot].store(StdPtr::null_mut(), RELAXED);
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

        let ptr: *mut u8 = self.ptrs[src_slot].load(RELAXED);
        dst.ptrs[dst_slot].store(ptr, WRITE_ORD);
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

        let ptr: *mut u8 = self.ptrs[src_slot].load(RELAXED);
        dst.ptrs[dst_slot].store(ptr, RELAXED);
    }

    // ========================================================================
    //  Lifecycle
    // ========================================================================

    #[inline(always)]
    unsafe fn cleanup(&self, slot: usize) {
        debug_assert!(slot < WIDTH_15, "cleanup: slot {slot} out of bounds");

        let ptr: *mut u8 = self.ptrs[slot].load(RELAXED);
        debug_assert!(!ptr.is_null(), "cleanup called on empty slot {slot}");

        // SAFETY: Caller guarantees exclusive access, slot is a terminal value,
        // and the pointer was stored via Box::into_raw.
        unsafe {
            drop(Box::from_raw(ptr.cast::<V>()));
        }
    }
}
