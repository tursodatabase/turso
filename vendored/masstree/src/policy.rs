//! Leaf node storage policy system.
//!
//! This module defines the trait hierarchy that parameterizes `LeafNode15` over
//! its value and suffix storage strategy.

mod box_values;
#[cfg(not(feature = "sidecar-suffix"))]
mod embedded_suffix;
mod inline_values;
mod sidecar_suffix;
mod suffix_ops;
#[cfg(test)]
mod tests;

pub use box_values::BoxValueArray;

#[expect(
    clippy::redundant_pub_crate,
    reason = "pub(crate) is intentional: not part of public API"
)]
pub(crate) use box_values::atomic_read_value;

#[cfg(not(feature = "sidecar-suffix"))]
pub use embedded_suffix::EmbeddedSuffix;

pub use inline_values::InlineValueArray;
pub use sidecar_suffix::SidecarSuffix;

use std::cmp::Ordering;
use std::fmt::{self as StdFmt, Debug, Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::{MaybeUninit, align_of, needs_drop, size_of};
use std::ops::Deref;
use std::ptr::NonNull;

use seize::{Guard, LocalGuard};

use crate::inline::bits::InlineBits;
use crate::ordering::READ_ORD;
use crate::prefetch::prefetch_read;
use crate::suffix::SuffixBagCell;

// ============================================================================
//  ValuePtr<V>
// ============================================================================

/// A lightweight, `Copy` pointer to a heap-allocated value.
#[repr(transparent)]
pub struct ValuePtr<V>(NonNull<V>);

impl<V> ValuePtr<V> {
    /// Create a `ValuePtr` from a raw pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null and point to a valid, aligned `V` that will
    /// remain live for as long as any copy of this `ValuePtr` is dereferenced.
    #[inline(always)]
    pub(crate) const unsafe fn from_raw(ptr: *mut V) -> Self {
        // SAFETY: Caller guarantees ptr is non-null.
        Self(unsafe { NonNull::new_unchecked(ptr) })
    }

    /// Get the raw pointer.
    #[inline(always)]
    pub(crate) const fn as_ptr(self) -> *mut V {
        self.0.as_ptr()
    }
}

// Copy + Clone: trivial pointer copy, zero cost.
impl<V> Copy for ValuePtr<V> {}

impl<V> Clone for ValuePtr<V> {
    #[inline(always)]
    fn clone(&self) -> Self {
        *self
    }
}

// Deref to &V for convenient field access and method calls.
impl<V> Deref for ValuePtr<V> {
    type Target = V;

    #[inline(always)]
    fn deref(&self) -> &V {
        // SAFETY: The pointer is valid as long as the EBR guard is held.
        unsafe { self.0.as_ref() }
    }
}

// SAFETY: ValuePtr<V> is Send when V: Send + Sync.
unsafe impl<V: Send + Sync> Send for ValuePtr<V> {}

// SAFETY: ValuePtr<V> is Sync when V: Send + Sync.
// Sharing &ValuePtr across threads is safe because Deref yields &V,
// and V: Sync means &V can be shared.
unsafe impl<V: Send + Sync> Sync for ValuePtr<V> {}

impl<V: Debug> Debug for ValuePtr<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        Debug::fmt(&**self, f)
    }
}

impl<V: PartialEq> PartialEq for ValuePtr<V> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<V: Eq> Eq for ValuePtr<V> {}

impl<V: Display> Display for ValuePtr<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        Display::fmt(&**self, f)
    }
}

impl<V: Hash> Hash for ValuePtr<V> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl<V: PartialOrd> PartialOrd for ValuePtr<V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (**self).partial_cmp(&**other)
    }
}

impl<V: Ord> Ord for ValuePtr<V> {
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).cmp(&**other)
    }
}

impl<V> AsRef<V> for ValuePtr<V> {
    #[inline(always)]
    fn as_ref(&self) -> &V {
        self
    }
}

// ============================================================================
//  ValueRef<V> - Cow-like value reference for soundness with write-through
// ============================================================================

/// Cow-like value handle: either a borrowed reference or an owned copy.
///
/// Returned by [`get_ref`](crate::MassTreeGeneric::get_ref). For policies
/// without write-through updates, this contains `Borrowed(&V)` (zero-copy).
/// For write-through policies (`V` <= 8 bytes), this contains `Owned(V)` from
/// an atomic read, avoiding an aliasing violation with concurrent writers.
pub enum ValueRef<'a, V> {
    /// Zero-copy borrow into the tree's heap storage.
    Borrowed(&'a V),
    /// Owned copy produced by an atomic read (write-through path).
    Owned(V),
}

impl<V> Deref for ValueRef<'_, V> {
    type Target = V;

    #[inline(always)]
    fn deref(&self) -> &V {
        match self {
            ValueRef::Borrowed(r) => r,

            ValueRef::Owned(v) => v,
        }
    }
}

impl<V: Debug> Debug for ValueRef<'_, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        Debug::fmt(&**self, f)
    }
}

impl<V: Display> Display for ValueRef<'_, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        Display::fmt(&**self, f)
    }
}

impl<V: PartialEq> PartialEq for ValueRef<'_, V> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<V: Eq> Eq for ValueRef<'_, V> {}

impl<V: PartialEq> PartialEq<V> for ValueRef<'_, V> {
    #[inline(always)]
    fn eq(&self, other: &V) -> bool {
        **self == *other
    }
}

impl<V: Clone> ValueRef<'_, V> {
    /// Convert to an owned value, cloning if necessary.
    #[inline(always)]
    pub fn into_owned(self) -> V {
        match self {
            ValueRef::Borrowed(r) => r.clone(),
            ValueRef::Owned(v) => v,
        }
    }
}

impl<V: Copy> ValueRef<'_, V> {
    /// Copy the value out.
    #[inline(always)]
    pub fn copied(&self) -> V {
        **self
    }
}

// ============================================================================
//  Sealed Module
// ============================================================================

mod sealed {
    use crate::inline::bits::InlineBits;

    use super::{BoxPolicy, BoxValueArray, InlinePolicy, InlineValueArray, SidecarSuffix};

    #[cfg(not(feature = "sidecar-suffix"))]
    use super::EmbeddedSuffix;

    /// Prevents external crates from implementing policy traits.
    pub trait Sealed {}

    impl<V: Send + Sync + 'static> Sealed for BoxPolicy<V> {}
    impl<V: InlineBits> Sealed for InlinePolicy<V> {}

    /// Prevents external crates from implementing `ValueArray`.
    pub trait ValueArraySealed {}

    impl<V: Send + Sync + 'static> ValueArraySealed for BoxValueArray<V> {}
    impl<V: InlineBits> ValueArraySealed for InlineValueArray<V> {}

    /// Prevents external crates from implementing `SuffixStore`.
    pub trait SuffixStoreSealed {}

    impl SuffixStoreSealed for SidecarSuffix {}

    #[cfg(not(feature = "sidecar-suffix"))]
    impl SuffixStoreSealed for EmbeddedSuffix {}

    /// Prevents external crates from implementing `RefPolicy`.
    pub trait RefPolicySealed {}

    impl<V: Send + Sync + 'static> RefPolicySealed for BoxPolicy<V> {}
}

// ============================================================================
//  RetireHandle
// ============================================================================

/// Opaque handle for deferred value retirement.
#[derive(Debug, PartialEq, Eq)]
pub enum RetireHandle {
    /// A raw pointer to an old heap allocation that needs EBR retirement.
    Ptr(*mut u8),

    /// No retirement needed (inline values are Copy).
    Noop,
}

// SAFETY: RetireHandle::Ptr contains a pointer that was obtained from Box::into_raw
// and will be consumed exactly once by retire_handle(). The pointer is not
// dereferenced outside of the retirement path which runs under EBR protection.
unsafe impl Send for RetireHandle {}

// ============================================================================
//  SlotKind / SlotState
// ============================================================================

/// Classification of a leaf slot's contents with value extraction.
pub enum SlotKind<O> {
    /// Slot is empty (not yet assigned or cleared).
    Empty,

    /// Slot contains a terminal value.
    Value(O),

    /// Slot contains a layer pointer to a sublayer root.
    Layer(*mut u8),
}

/// Lightweight slot classification without value extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Slot is empty.
    Empty,

    /// Slot contains a terminal value (not extracted).
    Value,

    /// Slot contains a layer pointer.
    Layer(*mut u8),
}

// ============================================================================
//  ValueArray<O>
// ============================================================================

/// Abstraction over how terminal values and layer pointers are stored in leaf slots.
///
/// WARN: SEALED
#[allow(dead_code, reason = "Sealed trait")]
pub trait ValueArray<O>: sealed::ValueArraySealed + Send + Sync + Sized + 'static {
    /// Create a new array with all slots empty.
    fn new() -> Self;

    // ========================================================================
    //  Slot Classification
    // ========================================================================

    /// Check if a slot is empty (no value, no layer).
    fn is_empty(&self, slot: usize) -> bool;

    /// Relaxed empty check for use in OCC search loops.
    fn is_empty_relaxed(&self, slot: usize) -> bool;

    /// Check if a slot contains a layer pointer.
    fn is_layer(&self, slot: usize) -> bool;

    // ========================================================================
    //  Terminal Value Operations
    // ========================================================================

    /// Load the terminal value at a slot.
    fn load(&self, slot: usize) -> Option<O>;

    /// Store a terminal value at a slot.
    fn store(&self, slot: usize, output: &O);

    /// Store a terminal value with Relaxed ordering.
    fn store_relaxed(&self, slot: usize, output: &O);

    /// Update an existing terminal value in place, returning a retire handle
    /// for the old value.
    fn update_in_place(&self, slot: usize, output: &O) -> RetireHandle;

    /// Update an existing terminal value with Relaxed store ordering.
    /// For use under lock where the lock's Release on drop provides synchronization.
    fn update_in_place_relaxed(&self, slot: usize, output: &O) -> RetireHandle;

    /// Take the terminal value, leaving the slot empty.
    fn take(&self, slot: usize) -> Option<O>;

    // ========================================================================
    //  Layer Pointer Operations
    // ========================================================================

    /// Load the raw pointer at a slot without any typed interpretation.
    fn load_raw(&self, slot: usize) -> *mut u8;

    /// Load the raw pointer with Relaxed ordering.
    ///
    /// Use when a subsequent atomic read provides synchronization
    /// (e.g., write-through scan where the value read is Acquire).
    fn load_raw_relaxed(&self, slot: usize) -> *mut u8;

    /// Load the layer pointer at a slot.
    fn load_layer(&self, slot: usize) -> *mut u8;

    /// Store a layer pointer at a slot.
    fn store_layer(&self, slot: usize, ptr: *mut u8);

    // ========================================================================
    //  Slot Management
    // ========================================================================

    /// Load the terminal value with Relaxed ordering.
    /// For use under lock where the lock's Acquire provides visibility.
    fn load_relaxed(&self, slot: usize) -> Option<O>;

    /// Clear a slot (set to empty state).
    fn clear(&self, slot: usize);

    /// Clear a slot with Relaxed ordering.
    /// For use under lock where the lock's Release on drop provides synchronization.
    fn clear_relaxed(&self, slot: usize);

    /// Move a slot's contents from `self[src_slot]` to `dst[dst_slot]`.
    fn move_slot(&self, dst: &Self, src_slot: usize, dst_slot: usize);

    /// Move a slot's contents with Relaxed store ordering on the destination.
    /// For use under lock where the lock's Release on drop provides synchronization.
    fn move_slot_relaxed(&self, dst: &Self, src_slot: usize, dst_slot: usize);

    // ========================================================================
    //  Lifecycle
    // ========================================================================

    /// Clean up a slot's value during leaf Drop.
    ///
    /// # Safety
    ///
    /// - Caller has exclusive access (`&mut` via Drop).
    /// - Slot must contain a terminal value (not empty, not layer).
    unsafe fn cleanup(&self, slot: usize);
}

// ============================================================================
//  SuffixStore — Key Suffix Storage Trait
// ============================================================================

/// Abstraction over how key suffixes are stored for a leaf node.
///
/// WARN: SEALED
#[allow(dead_code, reason = "Sealed trait")]
pub trait SuffixStore: sealed::SuffixStoreSealed + Send + Sync + Sized + 'static {
    /// Create a new empty suffix store.
    fn new() -> Self;

    // ========================================================================
    //  Read Operations
    // ========================================================================

    /// Get the suffix bytes for a slot.
    fn get(&self, slot: usize) -> Option<&[u8]>;

    /// Check if a slot's suffix equals the given bytes.
    fn suffix_equals(&self, slot: usize, suffix: &[u8]) -> bool;

    /// Compare a slot's suffix with the given bytes.
    fn suffix_compare(&self, slot: usize, suffix: &[u8]) -> Option<Ordering>;

    /// Check if external (overflow) storage has been allocated.
    fn has_external(&self) -> bool;

    /// Prefetch suffix storage into cache for upcoming access.
    ///
    /// For sidecar-based storage, this prefetches the sidecar structure.
    /// For embedded storage, this is a no-op (data is already in the leaf).
    fn prefetch(&self);

    // ========================================================================
    //  Write Operations (lock required)
    // ========================================================================

    /// Assign a suffix to a slot.
    ///
    /// # Safety
    ///
    /// - Caller must hold the leaf lock.
    /// - `guard` must come from this tree's collector.
    unsafe fn assign(
        &self,
        slot: usize,
        suffix: &[u8],
        perm: &impl crate::TreePermutation,
        guard: &LocalGuard<'_>,
    ) -> *mut u8;

    /// Assign a suffix during node initialization (sequential slots 0..slot).
    ///
    /// # Safety
    ///
    /// Same as `assign`.
    unsafe fn assign_init(&self, slot: usize, suffix: &[u8], guard: &LocalGuard<'_>);

    /// Assign a suffix using a pre-allocated buffer (reduces critical section time).
    ///
    /// # Safety
    ///
    /// Same as `assign`.
    unsafe fn assign_prealloc(
        &self,
        slot: usize,
        suffix: &[u8],
        perm: &impl crate::TreePermutation,
        guard: &LocalGuard<'_>,
        prealloc: Vec<u8>,
    ) -> *mut u8;

    /// Pre-allocate external overflow storage.
    ///
    /// # Safety
    ///
    /// Caller must hold the leaf lock.
    unsafe fn ensure_external(&self) -> *mut SuffixBagCell;

    /// Clear a slot's suffix.
    ///
    /// # Safety
    ///
    /// Caller must hold the leaf lock.
    unsafe fn clear(&self, slot: usize, guard: &LocalGuard<'_>);

    /// Retire a suffix bag pointer returned by `assign`.
    ///
    /// # Safety
    ///
    /// `ptr` must have come from `assign` on this suffix store type.
    unsafe fn retire_bag_ptr(ptr: *mut u8, guard: &LocalGuard<'_>);

    // ========================================================================
    //  Lifecycle
    // ========================================================================

    /// Drop all owned heap allocations (external bag, sidecar).
    ///
    /// # Safety
    ///
    /// Caller has exclusive access (no concurrent readers).
    unsafe fn drop_storage(&mut self);

    /// Initialize suffix storage at a zeroed memory location.
    ///
    /// # Safety
    ///
    /// `ptr` must point to valid, zeroed, properly-aligned memory.
    unsafe fn init_at_zero(ptr: *mut Self);
}

// ============================================================================
//  LeafPolicy
// ============================================================================

/// Bundles value storage, suffix storage, and type conversion for a leaf node.
pub trait LeafPolicy: sealed::Sealed + Send + Sync + Sized + 'static {
    /// The user-facing value type provided to `insert()`.
    type Value: Send + Sync;

    /// The internal output type returned from `get()` and carried across retries.
    type Output: Clone + Send + Sync;

    /// The value storage array type embedded in the leaf.
    type Values: ValueArray<Self::Output>;

    /// The suffix storage type embedded in the leaf.
    type Suffix: SuffixStore;

    /// Whether old value pointers require EBR deferred retirement.
    const NEEDS_RETIREMENT: bool;

    /// Whether this policy supports zero-copy reference returns.
    const SUPPORTS_REF: bool;

    /// Whether this policy supports write-through updates for small values.
    ///
    /// When true, update operations for existing keys can modify the value
    /// in place (via atomic write) without allocating a new Box or scheduling
    /// EBR retirement.
    ///
    /// Requirements: `size_of::<Value>() <= 8` and the storage array supports
    /// atomic read/write through the allocation pointer.
    const CAN_WRITE_THROUGH: bool = false;

    // ========================================================================
    //  Slot Queries
    // ========================================================================

    /// Check if a slot contains no value (empty or layer).
    fn is_value_empty(values: &Self::Values, slot: usize) -> bool;

    /// Relaxed empty check for OCC search loops where the permutation
    /// Acquire load provides synchronization and `has_changed()` validates.
    fn is_value_empty_relaxed(values: &Self::Values, slot: usize) -> bool;

    /// Prefetch the value at a slot for upcoming access.
    fn prefetch_value(values: &Self::Values, slot: usize);

    /// Load a raw pointer to the value at a slot.
    ///
    /// # Safety
    ///
    /// Caller must hold a valid guard and validate OCC version after use.
    unsafe fn load_value_ptr(values: &Self::Values, slot: usize) -> *const Self::Value;

    /// Take the value at a slot when converting it to a layer pointer.
    fn take_value_for_layer(values: &Self::Values, slot: usize) -> RetireHandle;

    // ========================================================================
    //  Value Conversion
    // ========================================================================

    /// Convert a user value into an output handle.
    fn into_output(value: Self::Value) -> Self::Output;

    /// Clone an output for return to the caller.
    #[inline(always)]
    fn clone_output(output: &Self::Output) -> Self::Output {
        output.clone()
    }

    /// Clone an owned `Value` from an `Output` reference.
    fn clone_value_from_output(output: &Self::Output) -> Self::Value
    where
        Self::Value: Clone;

    // ========================================================================
    //  Output Reconstruction
    // ========================================================================

    /// Reconstruct an `Output` from a raw retired pointer returned by
    /// [`ValueArray::update_in_place`](ValueArray::update_in_place).
    ///
    /// This exists to let hot update paths avoid an extra `load_value()` when
    /// the old value can be represented directly by the retired pointer
    /// (e.g., `BoxPolicy`).
    ///
    /// # Safety
    ///
    /// - `ptr` must be a non-null pointer previously stored in a value slot.
    /// - The caller must ensure appropriate synchronization (typically under lock).
    unsafe fn output_from_retire_ptr(ptr: *mut u8) -> Self::Output;

    // ========================================================================
    //  Retirement
    // ========================================================================

    /// Retire an old value through EBR using a handle captured before mutation.
    ///
    /// # Safety
    ///
    /// The handle must have been produced by a prior `update_in_place()` call
    /// on the same leaf, and must not have been retired already.
    unsafe fn retire_handle(handle: RetireHandle, guard: &LocalGuard<'_>);

    /// Retire a taken value through EBR.
    ///
    /// # Safety
    ///
    /// The output must no longer be accessible to concurrent readers after
    /// the current epoch ends.
    unsafe fn retire_output(output: Self::Output, guard: &LocalGuard<'_>);

    // ========================================================================
    //  Write-Through Update
    // ========================================================================

    /// Atomically read the old value and write the new value in place.
    ///
    /// Only callable when `CAN_WRITE_THROUGH` is true. Bypasses Box allocation
    /// and EBR retirement entirely. Returns the old value by copy.
    ///
    /// # Safety
    ///
    /// - Caller must hold the leaf lock.
    /// - Slot must contain a terminal value (not empty, not layer).
    /// - `CAN_WRITE_THROUGH` must be true.
    unsafe fn write_through_update(
        _values: &Self::Values,
        _slot: usize,
        _new_value: &Self::Value,
    ) -> Self::Value {
        unreachable!("write_through_update called on policy with CAN_WRITE_THROUGH=false")
    }

    // ========================================================================
    //  Reference Access
    // ========================================================================

    /// Load a reference to the value at a slot, without cloning.
    ///
    /// # Safety
    ///
    /// - Caller must hold a valid guard preventing deallocation.
    /// - Caller must validate the OCC version after reading.
    /// - The returned reference is valid only as long as the guard is held.
    unsafe fn load_value_ref<'g>(
        values: &Self::Values,
        slot: usize,
    ) -> Option<ValueRef<'g, Self::Value>>;
}

// ============================================================================
//  RefPolicy
// ============================================================================

/// Marker trait for policies that support zero-copy reference returns.
pub trait RefPolicy: sealed::RefPolicySealed + LeafPolicy {
    /// Borrow the underlying value from an output handle.
    ///
    /// For non-write-through types only. For write-through types, use
    /// [`output_as_ref_sound`](Self::output_as_ref_sound) instead.
    fn output_as_ref(output: &Self::Output) -> &Self::Value;

    /// Soundly read a value reference from an output handle.
    ///
    /// For `CAN_WRITE_THROUGH` types, atomically copies the value into
    /// `scratch` and returns a reference to the copy. For non-write-through
    /// types, returns a direct reference via [`output_as_ref`](Self::output_as_ref).
    ///
    /// # Safety
    ///
    /// The output handle must point to valid data protected by an EBR guard.
    unsafe fn output_as_ref_sound<'a>(
        output: &'a Self::Output,
        scratch: &'a mut MaybeUninit<Self::Value>,
    ) -> &'a Self::Value;
}

// ============================================================================
//  BoxPolicy<V>
// ============================================================================

/// Box-based value storage with lazy sidecar suffix.
pub struct BoxPolicy<V>(PhantomData<V>);

impl<V: Send + Sync + 'static> LeafPolicy for BoxPolicy<V> {
    type Value = V;
    type Output = ValuePtr<V>;
    type Values = BoxValueArray<V>;
    type Suffix = SidecarSuffix;

    const NEEDS_RETIREMENT: bool = true;
    const SUPPORTS_REF: bool = true;

    /// Write-through is supported only for no-drop, naturally-aligned
    /// value types up to 8 bytes, where atomic writes through the Box pointer
    /// avoid heap allocation and EBR retirement on updates.
    ///
    /// `needs_drop == false` prevents duplicating ownership-bearing values via
    /// atomic load. The `align_of >= size_of` check excludes packed structs
    /// and composite types whose size exceeds their alignment, which would not
    /// have naturally-aligned Box allocations.
    const CAN_WRITE_THROUGH: bool = !needs_drop::<V>()
        && size_of::<V>() <= 8
        && size_of::<V>().is_power_of_two()
        && align_of::<V>() >= size_of::<V>();

    #[inline(always)]
    fn is_value_empty(values: &BoxValueArray<V>, slot: usize) -> bool {
        values.is_empty(slot)
    }

    #[inline(always)]
    fn is_value_empty_relaxed(values: &BoxValueArray<V>, slot: usize) -> bool {
        values.is_empty_relaxed(slot)
    }

    #[inline(always)]
    fn prefetch_value(values: &BoxValueArray<V>, slot: usize) {
        // Prefetch the Box<V> target into L1 cache for upcoming dereference.
        //
        // Relaxed is sufficient here: prefetch is a performance hint only and
        // does not participate in synchronization.
        let ptr: *mut u8 = values.load_raw_relaxed(slot);

        if !ptr.is_null() {
            prefetch_read(ptr.cast::<V>());
        }
    }

    #[inline(always)]
    unsafe fn load_value_ptr(values: &BoxValueArray<V>, slot: usize) -> *const V {
        // SAFETY: Caller holds a valid guard and will validate OCC version after.
        let ptr: *mut u8 = values.load_raw(slot);

        if ptr.is_null() {
            std::ptr::null()
        } else {
            ptr.cast::<V>()
        }
    }

    #[inline(always)]
    fn take_value_for_layer(values: &BoxValueArray<V>, slot: usize) -> RetireHandle {
        values.clear_relaxed(slot);
        RetireHandle::Noop
    }

    #[inline(always)]
    fn into_output(value: V) -> ValuePtr<V> {
        let ptr: *mut V = Box::into_raw(Box::new(value));
        // SAFETY: Box::into_raw returns a non-null, properly aligned pointer.
        unsafe { ValuePtr::from_raw(ptr) }
    }

    #[inline(always)]
    fn clone_value_from_output(output: &ValuePtr<V>) -> V
    where
        V: Clone,
    {
        if Self::CAN_WRITE_THROUGH {
            // SAFETY: ValuePtr wraps a valid Box<V> pointer. CAN_WRITE_THROUGH
            // guarantees size 1/2/4/8 with natural alignment. Atomic read avoids
            // creating &V to data that may be concurrently modified by
            // write_through_update.
            unsafe { atomic_read_value::<V>(output.as_ptr().cast(), READ_ORD) }
        } else {
            (**output).clone()
        }
    }

    #[inline(always)]
    unsafe fn output_from_retire_ptr(ptr: *mut u8) -> ValuePtr<V> {
        debug_assert!(!ptr.is_null());
        // SAFETY: ptr was stored via Box::into_raw in into_output().
        unsafe { ValuePtr::from_raw(ptr.cast::<V>()) }
    }

    #[inline(always)]
    unsafe fn retire_handle(handle: RetireHandle, guard: &LocalGuard<'_>) {
        if let RetireHandle::Ptr(ptr) = handle {
            debug_assert!(!ptr.is_null());
            // SAFETY: ptr was obtained from Box::into_raw in into_output().
            // The guard ensures no readers can still observe the old value
            // after the current epoch completes.
            unsafe {
                guard.defer_retire(ptr.cast::<V>(), |ptr: *mut V, _| {
                    drop(Box::from_raw(ptr));
                });
            }
        }
    }

    #[inline(always)]
    unsafe fn retire_output(output: ValuePtr<V>, guard: &LocalGuard<'_>) {
        let ptr: *mut V = output.as_ptr();

        // SAFETY: ptr is a valid Box<V> pointer. The guard ensures no readers
        // can observe this value after the current epoch completes.
        unsafe {
            guard.defer_retire(ptr, |ptr: *mut V, _| {
                drop(Box::from_raw(ptr));
            });
        }
    }

    #[inline(always)]
    unsafe fn write_through_update(values: &BoxValueArray<V>, slot: usize, new_value: &V) -> V {
        debug_assert!(
            Self::CAN_WRITE_THROUGH,
            "write_through_update called but CAN_WRITE_THROUGH is false"
        );

        // SAFETY: Caller guarantees lock is held, slot has a value, and
        // CAN_WRITE_THROUGH (size_of::<V>() <= 8) is true.
        unsafe { values.write_through_update(slot, new_value) }
    }

    #[inline(always)]
    unsafe fn load_value_ref<'g>(
        values: &BoxValueArray<V>,
        slot: usize,
    ) -> Option<ValueRef<'g, V>> {
        // SAFETY: Caller guarantees:
        // 1. A valid guard prevents retirement of this value.
        // 2. OCC version will be validated after this load.
        // 3. The slot is known to contain a value (not empty, not layer).
        let ptr: *mut u8 = values.load_raw(slot);

        if ptr.is_null() {
            None
        } else if Self::CAN_WRITE_THROUGH {
            // SAFETY: CAN_WRITE_THROUGH guarantees size 1/2/4/8 with natural
            // alignment. Atomic read avoids creating &V to data that may be
            // concurrently modified by write_through_update.
            Some(ValueRef::Owned(unsafe {
                atomic_read_value::<V>(ptr, READ_ORD)
            }))
        } else {
            Some(ValueRef::Borrowed(unsafe { &*ptr.cast::<V>() }))
        }
    }
}

impl<V: Send + Sync + 'static> RefPolicy for BoxPolicy<V> {
    #[inline(always)]
    fn output_as_ref(output: &ValuePtr<V>) -> &V {
        output
    }

    #[inline(always)]
    unsafe fn output_as_ref_sound<'a>(
        output: &'a ValuePtr<V>,
        scratch: &'a mut MaybeUninit<V>,
    ) -> &'a V {
        if Self::CAN_WRITE_THROUGH {
            // SAFETY: CAN_WRITE_THROUGH guarantees size 1/2/4/8 with natural
            // alignment. Atomic read avoids aliasing violation with concurrent
            // write_through_update. The copy is stored in scratch so the
            // returned &V is to owned stack data.
            *scratch = MaybeUninit::new(unsafe {
                atomic_read_value::<V>(output.as_ptr().cast(), READ_ORD)
            });

            unsafe { scratch.assume_init_ref() }
        } else {
            // SAFETY: Non-write-through types allocate a new Box on update,
            // so the existing allocation is immutable while the guard is held.
            Self::output_as_ref(output)
        }
    }
}

// ============================================================================
//  InlinePolicy<V>
// ============================================================================

/// Inline value storage with embedded suffix (default) or sidecar suffix.
///
/// # Type Parameters
///
/// - `V`: The user value type. Must implement [`InlineBits`].
pub struct InlinePolicy<V: InlineBits>(PhantomData<V>);

impl<V: InlineBits> LeafPolicy for InlinePolicy<V> {
    type Value = V;
    type Output = V;
    type Values = InlineValueArray<V>;

    #[cfg(not(feature = "sidecar-suffix"))]
    type Suffix = EmbeddedSuffix;

    #[cfg(feature = "sidecar-suffix")]
    type Suffix = SidecarSuffix;

    const NEEDS_RETIREMENT: bool = false;
    const SUPPORTS_REF: bool = false;

    #[inline(always)]
    fn is_value_empty(values: &InlineValueArray<V>, slot: usize) -> bool {
        values.is_empty(slot)
    }

    #[inline(always)]
    fn is_value_empty_relaxed(values: &InlineValueArray<V>, slot: usize) -> bool {
        values.is_empty_relaxed(slot)
    }

    #[inline(always)]
    fn prefetch_value(_values: &InlineValueArray<V>, _slot: usize) {}

    #[inline(always)]
    unsafe fn load_value_ptr(_values: &InlineValueArray<V>, _slot: usize) -> *const V {
        unreachable!("inline values do not support pointer-based access")
    }

    #[inline(always)]
    fn take_value_for_layer(values: &InlineValueArray<V>, slot: usize) -> RetireHandle {
        values.clear_relaxed(slot);
        RetireHandle::Noop
    }

    #[inline(always)]
    fn into_output(value: V) -> V {
        value
    }

    #[inline(always)]
    fn clone_value_from_output(output: &V) -> V
    where
        V: Clone,
    {
        *output // V: Copy implies Clone; bitwise copy.
    }

    #[inline(always)]
    unsafe fn output_from_retire_ptr(_ptr: *mut u8) -> V {
        unreachable!("inline values have no retired pointer")
    }

    #[inline(always)]
    unsafe fn retire_handle(_handle: RetireHandle, _guard: &LocalGuard<'_>) {}

    #[inline(always)]
    unsafe fn retire_output(_output: V, _guard: &LocalGuard<'_>) {}

    #[inline(always)]
    unsafe fn load_value_ref<'g>(
        _values: &InlineValueArray<V>,
        _slot: usize,
    ) -> Option<ValueRef<'g, V>> {
        unreachable!("inline values do not support reference returns")
    }
}
