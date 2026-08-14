//! Filepath: `src/tree/range/scan_state.rs`
//!
//! Scan state machine types for range scan operations.

use std::fmt::{self as StdFmt, Debug, Formatter};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self as StdPtr, NonNull};

use arrayvec::ArrayVec;

use crate::leaf_trait::TreeLeafNode;
use crate::leaf15::LeafNode15;
use crate::ordering::READ_ORD;
use crate::policy::LeafPolicy;
use crate::policy::atomic_read_value;

// ============================================================================
//  ScanState Enum
// ============================================================================

/// State machine states for range scan operations.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanState {
    /// Ready to emit the current entry to the visitor.
    Emit = 0,

    /// Searching for the next entry within the current leaf/layer.
    FindNext = 1,

    /// Descending into a sublayer (encountered a layer pointer).
    Down = 2,

    /// Ascending to parent layer (current layer exhausted).
    Up = 3,

    /// Repositioning after a conflict or layer transition.
    Retry = 4,
}

impl ScanState {
    /// Check if this state will yield an entry.
    #[inline(always)]
    #[allow(dead_code, reason = "Scan state API")]
    pub const fn is_emit(self) -> bool {
        matches!(self, Self::Emit)
    }

    /// Check if this state requires layer stack manipulation.
    #[inline(always)]
    #[allow(dead_code, reason = "Scan state API")]
    pub const fn is_layer_transition(self) -> bool {
        matches!(self, Self::Down | Self::Up)
    }
}

/// Result of [`super::forward_ctx::ForwardScanCtx::step_transitions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    /// Transition handled (Down/Up/Retry). Caller should `continue`.
    Continue,

    /// State is Emit/FindNext. Caller proceeds to hot path.
    Ready,

    /// Tree exhausted. Caller should return.
    Exhausted,
}

// ============================================================================
//  ScanStackElement
// ============================================================================

/// Current layer position during a range scan.
pub struct ScanStackElement<P>
where
    P: LeafPolicy,
{
    /// Current layer root (may be leaf or internode).
    root: *const u8,

    /// Current leaf in this layer.
    leaf: Option<NonNull<LeafNode15<P>>>,

    /// Version snapshot for optimistic validation.
    version: u32,

    /// Cached permutation snapshot.
    perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,

    /// Current logical position in permutation.
    ki: usize,

    /// Cached ikey for monotonicity check optimization.
    last_ikey: u64,

    /// Marker for the slot type.
    _marker: PhantomData<P>,
}

// Manual Clone impl to avoid requiring `L: Clone` and `S: Clone` bounds.
// All fields are Copy types (pointers, primitives, PhantomData).
impl<P> Clone for ScanStackElement<P>
where
    P: LeafPolicy,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root,
            leaf: self.leaf,
            version: self.version,
            perm: self.perm,
            ki: self.ki,
            last_ikey: self.last_ikey,
            _marker: PhantomData,
        }
    }
}

#[expect(
    clippy::missing_fields_in_debug,
    reason = "Intentionally omit PhantomData and show perm.size() instead of full permutation"
)]
impl<P> Debug for ScanStackElement<P>
where
    P: LeafPolicy,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("ScanStackElement")
            .field("root", &self.root)
            .field("leaf", &self.leaf)
            .field("version", &self.version)
            .field("ki", &self.ki)
            .field("perm_size", &self.perm.size())
            .finish()
    }
}

impl<P> ScanStackElement<P>
where
    P: LeafPolicy,
{
    // ========================================================================
    //  Construction
    // ========================================================================

    /// Create a new stack element for a layer.
    #[must_use]
    #[expect(clippy::missing_const_for_fn, reason = "Perm::empty() is not const")]
    pub fn new(root: *const u8) -> Self {
        Self {
            root,
            leaf: None,
            version: 0,
            perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm::empty(),
            ki: 0,
            last_ikey: 0,
            _marker: PhantomData,
        }
    }

    /// Create a stack element with all fields initialized.
    #[must_use]
    #[allow(dead_code, reason = "Scan stack element API")]
    pub const fn with_leaf(
        root: *const u8,
        leaf: *mut LeafNode15<P>,
        version: u32,
        perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ki: usize,
    ) -> Self {
        Self {
            root,
            leaf: NonNull::new(leaf),
            version,
            perm,
            ki,
            last_ikey: 0,
            _marker: PhantomData,
        }
    }

    // ========================================================================
    //  Accessors
    // ========================================================================

    /// Get the layer root pointer.
    #[inline(always)]
    pub const fn root(&self) -> *const u8 {
        self.root
    }

    /// Get the current leaf pointer as a raw pointer.
    #[inline(always)]
    pub const fn leaf_ptr(&self) -> *mut LeafNode15<P> {
        match self.leaf {
            Some(nn) => nn.as_ptr(),

            None => StdPtr::null_mut(),
        }
    }

    /// Get a reference to the current leaf.
    #[inline(always)]
    pub unsafe fn leaf_ref(&self) -> &LeafNode15<P> {
        debug_assert!(self.leaf.is_some(), "leaf_ref called on null leaf");

        // SAFETY: Caller ensures leaf is valid and Some
        unsafe { self.leaf.unwrap_unchecked().as_ref() }
    }

    /// Try to get a reference to the current leaf.
    #[inline(always)]
    #[allow(dead_code, reason = "Scan stack element API")]
    pub unsafe fn try_leaf_ref(&self) -> Option<&LeafNode15<P>> {
        // SAFETY: Caller ensures guard is held
        self.leaf.map(|nn| unsafe { nn.as_ref() })
    }

    /// Get the cached version.
    #[inline(always)]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Get the cached permutation.
    #[inline(always)]
    pub const fn perm(&self) -> <LeafNode15<P> as TreeLeafNode<P>>::Perm {
        self.perm
    }

    /// Get the current logical position.
    #[inline(always)]
    pub const fn ki(&self) -> usize {
        self.ki
    }

    /// Get the physical slot at current position, or `None` if exhausted.
    #[inline]
    #[expect(
        clippy::missing_const_for_fn,
        reason = "perm.size()/get() are not const"
    )]
    pub fn kp(&self) -> Option<usize> {
        if self.ki < self.perm.size() {
            Some(self.perm.get(self.ki))
        } else {
            None
        }
    }

    /// Check if the leaf is exhausted (no more slots at current position).
    #[inline(always)]
    #[allow(dead_code, reason = "Scan stack element API")]
    #[expect(clippy::missing_const_for_fn, reason = "perm.size() is not const")]
    pub fn is_exhausted(&self) -> bool {
        self.ki >= self.perm.size()
    }

    /// Check if the leaf pointer is null.
    #[inline(always)]
    pub const fn is_null(&self) -> bool {
        self.leaf.is_none()
    }

    // ========================================================================
    //  Mutation
    // ========================================================================

    /// Set the layer root pointer.
    #[inline(always)]
    pub const fn set_root(&mut self, root: *const u8) {
        self.root = root;
    }

    /// Set the leaf pointer from a raw pointer.
    #[inline(always)]
    pub const fn set_leaf(&mut self, leaf: *mut LeafNode15<P>) {
        self.leaf = NonNull::new(leaf);
    }

    /// Set the cached version.
    #[inline(always)]
    #[allow(dead_code, reason = "Scan stack element API")]
    pub const fn set_version(&mut self, version: u32) {
        self.version = version;
    }

    /// Set the cached permutation.
    #[inline(always)]
    #[allow(dead_code, reason = "Scan stack element API")]
    pub const fn set_perm(&mut self, perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm) {
        self.perm = perm;
    }

    /// Set the logical position.
    #[inline(always)]
    #[allow(dead_code, reason = "Scan stack element API")]
    pub const fn set_ki(&mut self, ki: usize) {
        self.ki = ki;
    }

    /// Get the cached last ikey for monotonicity check.
    #[inline(always)]
    pub const fn last_ikey(&self) -> u64 {
        self.last_ikey
    }

    /// Set the cached last ikey after a successful slot read.
    #[inline(always)]
    pub const fn set_last_ikey(&mut self, ikey: u64) {
        self.last_ikey = ikey;
    }

    /// Advance to the next logical position.
    #[inline(always)]
    pub const fn next(&mut self) {
        self.ki += 1;
    }

    /// Update all leaf state (version, perm, ki).
    #[inline]
    pub const fn update_state(
        &mut self,
        version: u32,
        perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ki: usize,
    ) {
        self.version = version;
        self.perm = perm;
        self.ki = ki;
    }

    /// Refresh state from the current leaf.
    #[allow(dead_code, reason = "Scan stack element API")]
    pub unsafe fn refresh_from_leaf(&mut self, ki: usize) {
        debug_assert!(self.leaf.is_some(), "refresh_from_leaf called on null leaf");
        // SAFETY: Caller ensures leaf is valid
        let leaf: &LeafNode15<P> = unsafe { self.leaf.unwrap_unchecked().as_ref() };

        self.version = leaf.version().stable();
        self.perm = leaf.permutation();
        self.ki = ki;
    }
}

// ============================================================================
//  LayerContext and LayerStack
// ============================================================================

/// Parent layer context stored during sublayer descent.
#[derive(Clone, Copy)]
pub struct LayerContext<P: LeafPolicy> {
    /// Parent layer root.
    pub root: *const u8,

    /// Parent leaf (where the layer pointer was found).
    pub leaf: NonNull<LeafNode15<P>>,
}

impl<P: LeafPolicy> Debug for LayerContext<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("LayerContext")
            .field("root", &self.root)
            .field("leaf", &self.leaf)
            .finish()
    }
}

impl<P: LeafPolicy> LayerContext<P> {
    /// Create a new layer context.
    ///
    /// # Panics
    ///
    /// Panics if `leaf` is null.
    #[track_caller]
    #[inline(always)]
    #[expect(clippy::expect_used, reason = "Infallible")]
    pub const fn new(root: *const u8, leaf: *mut LeafNode15<P>) -> Self {
        Self {
            root,
            leaf: NonNull::new(leaf).expect("LayerContext requires non-null leaf"),
        }
    }

    /// Get the leaf as a raw mutable pointer.
    #[inline(always)]
    pub const fn leaf_ptr(&self) -> *mut LeafNode15<P> {
        self.leaf.as_ptr()
    }
}

/// Stack of parent layer contexts for sublayer navigation.
pub struct LayerStack<P: LeafPolicy> {
    /// Inline storage for the common case (up to 6 layers).
    inline: ArrayVec<LayerContext<P>, 6>,

    /// Heap spillover for deep keys (> 6 layers). `None` until first spill.
    overflow: Option<Vec<LayerContext<P>>>,
}

impl<P: LeafPolicy> LayerStack<P> {
    /// Create a new empty layer stack.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            inline: ArrayVec::new_const(),
            overflow: None,
        }
    }

    /// Push a layer context onto the stack.
    #[inline]
    pub fn push(&mut self, ctx: LayerContext<P>) {
        if let Some(ref mut overflow) = self.overflow {
            overflow.push(ctx);
        } else if let Err(err) = self.inline.try_push(ctx) {
            // Spill: move all inline elements to heap + the rejected element.
            let mut heap = Vec::with_capacity(8);

            heap.extend(self.inline.drain(..));
            heap.push(err.element());
            self.overflow = Some(heap);
        }
    }

    /// Pop the most recent layer context.
    #[inline]
    pub fn pop(&mut self) -> Option<LayerContext<P>> {
        if let Some(ref mut overflow) = self.overflow {
            let result = overflow.pop();
            // Reclaim heap when empty (shrink back to inline-only mode).
            if overflow.is_empty() {
                self.overflow = None;
            }
            result
        } else {
            self.inline.pop()
        }
    }

    /// Returns `true` if the stack has no elements.
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        match self.overflow {
            Some(ref overflow) => overflow.is_empty(),
            None => self.inline.is_empty(),
        }
    }

    /// Remove all elements from the stack.
    #[inline]
    #[allow(dead_code, reason = "API completeness")]
    pub fn clear(&mut self) {
        self.overflow = None;
        self.inline.clear();
    }

    /// Returns the total number of elements.
    #[cfg(test)]
    pub const fn len(&self) -> usize {
        match self.overflow.as_ref() {
            Some(v) => v.len(),
            None => self.inline.len(),
        }
    }
}

// ============================================================================
//  ScanSnapshot - Captured slot data for emission
// ============================================================================

/// Snapshot of slot data captured for emission.
#[derive(Debug, Clone)]
pub struct ScanSnapshot<P: LeafPolicy> {
    /// The value output (`ValuePtr<V>` clone or V copy).
    pub value: P::Output,

    /// The key length at current layer.
    #[allow(dead_code, reason = "Used in tests and part of snapshot API")]
    pub key_len: usize,
}

// ============================================================================
//  ScanSnapshotPtr - Zero-copy snapshot for scan_ref
// ============================================================================

/// Snapshot of slot data for zero-copy emission.
///
/// Unlike [`ScanSnapshot`] which clones the value,
/// this stores a typed pointer and defers dereferencing to the caller.
#[derive(Debug, Clone, Copy)]
pub struct ScanSnapshotPtr<V> {
    /// Typed pointer to the value data.
    pub value_ptr: *const V,

    /// The key length at current layer (same as [`ScanSnapshot`]).
    #[allow(dead_code, reason = "Used in tests")]
    pub key_len: usize,
}

#[allow(dead_code, reason = "Zero-copy scan snapshot API")]
impl<V> ScanSnapshotPtr<V> {
    /// Create a new zero-copy snapshot.
    #[inline(always)]
    pub const fn new(value_ptr: *const V, key_len: usize) -> Self {
        Self { value_ptr, key_len }
    }

    /// Create from a raw `*const u8` pointer by casting.
    #[inline(always)]
    pub const fn from_raw(ptr: *const u8, key_len: usize) -> Self {
        Self {
            value_ptr: ptr.cast::<V>(),
            key_len,
        }
    }

    /// Get a reference to the value.
    ///
    /// Only sound for non-write-through types. For `CAN_WRITE_THROUGH` types,
    /// use [`value_copy`](Self::value_copy) instead.
    ///
    /// # Safety
    ///
    /// Caller ensures pointer is valid, guard is held, and no concurrent
    /// write-through update can modify the pointed-to data.
    #[inline(always)]
    pub const unsafe fn value_ref(&self) -> &V {
        // SAFETY: Caller ensures pointer is valid and guard is held.
        unsafe { &*self.value_ptr }
    }

    /// Atomically read the value by copy.
    ///
    /// Sound for `CAN_WRITE_THROUGH` types where the pointed-to data may be
    /// concurrently modified by `write_through_update`.
    ///
    /// # Safety
    ///
    /// - Pointer must be valid and the guard must be held.
    /// - `CAN_WRITE_THROUGH` must be true (size 1/2/4/8, natural alignment).
    #[inline(always)]
    pub unsafe fn value_copy(&self) -> V {
        // SAFETY: CAN_WRITE_THROUGH guarantees size 1/2/4/8 with natural
        // alignment. Atomic read avoids aliasing violation.
        unsafe { atomic_read_value::<V>(self.value_ptr.cast(), READ_ORD) }
    }

    /// Resolve a value reference, using atomic copy for write-through types.
    ///
    /// For write-through policies, atomically copies the value into `scratch`
    /// and returns a reference to it. For non-write-through policies, returns
    /// a direct reference to the pointed-to value.
    ///
    /// # Safety
    ///
    /// - Pointer must be valid and protected by a guard.
    /// - For write-through: pointer must be valid for atomic read.
    /// - For non-write-through: no concurrent write-through update may modify
    ///   the pointed-to data.
    #[inline(always)]
    pub unsafe fn resolve_value_ref<'a, P: LeafPolicy<Value = V>>(
        &self,
        scratch: &'a mut MaybeUninit<V>,
    ) -> &'a V {
        if P::CAN_WRITE_THROUGH {
            // SAFETY: CAN_WRITE_THROUGH guarantees atomic read is sound.
            *scratch = MaybeUninit::new(unsafe { self.value_copy() });

            // SAFETY: Just initialized above.
            unsafe { scratch.assume_init_ref() }
        } else {
            // SAFETY: Caller ensures pointer is valid and no concurrent modification.
            unsafe { &*self.value_ptr }
        }
    }
}

impl<P: LeafPolicy> ScanSnapshot<P> {
    /// Create a new scan snapshot.
    #[inline(always)]
    pub const fn new(value: P::Output, key_len: usize) -> Self {
        Self { value, key_len }
    }
}

// ============================================================================
//  ScanStateBack Enum (for DoubleEndedIterator)
// ============================================================================

/// State machine states for reverse range scan operations.
///
/// Similar to [`ScanState`] but for backward iteration via `next_back()`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ScanStateBack {
    /// Ready to emit the current entry via `next_back()`.
    Emit = 0,

    /// Searching for the previous entry within the current leaf.
    #[default]
    FindPrev = 1,

    /// Descending into a sublayer (encountered a layer pointer).
    ///
    /// For reverse scan, we descend to the MAXIMUJM of the sublayer.
    Down = 2,

    /// Ascending to parent layer (current layer exhausted a layer pointer).
    Up = 3,

    /// Repositioning after version conflict or layer transition.
    Retry = 4,
}

impl ScanStateBack {
    /// Check if this state will yield an entry.
    #[inline(always)]
    pub const fn is_emit(self) -> bool {
        matches!(self, Self::Emit)
    }
}

// ============================================================================
//  BackStackElement (for DoubleEndedIterator)
// ============================================================================

pub struct BackStackElement<P>
where
    P: LeafPolicy,
{
    /// Current layer root (may be leaf or internode).
    root: *const u8,

    /// Current leaf in this layer.
    leaf: Option<NonNull<LeafNode15<P>>>,

    /// Version snapshot for optimisitic validation.
    version: u32,

    /// Cached permutation snapshot.
    perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,

    /// Current logical position in permutation.
    ki: isize,

    /// Cached ikey for monotonicity check optimization (reverse direction).
    last_ikey: u64,

    /// Marker for the slot type.
    _marker: PhantomData<P>,
}

impl<P> BackStackElement<P>
where
    P: LeafPolicy,
{
    /// Create a new back stack element for a layer.
    ///
    /// Matches [`ScanStackElement::new`] signature for consistency.
    #[inline(always)]
    #[expect(clippy::missing_const_for_fn, reason = "Perm::empty() is not const")]
    pub fn new(root: *const u8) -> Self {
        Self {
            root,
            leaf: None,
            version: 0,
            perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm::empty(),
            ki: 0,
            last_ikey: u64::MAX,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub const fn get_root(&self) -> *const u8 {
        self.root
    }

    /// Set the layer root.
    #[inline(always)]
    pub const fn set_root(&mut self, root: *const u8) {
        self.root = root;
    }

    /// Get the current leaf pointer.
    #[inline(always)]
    pub fn get_leaf_ptr(&self) -> *mut LeafNode15<P> {
        self.leaf.map_or(StdPtr::null_mut(), NonNull::as_ptr)
    }

    #[inline(always)]
    pub const fn set_leaf(&mut self, leaf: *mut LeafNode15<P>) {
        self.leaf = NonNull::new(leaf);
    }

    /// Update state after version validation.
    #[inline(always)]
    pub const fn update_state(
        &mut self,
        version: u32,
        perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ki: isize,
    ) {
        self.version = version;
        self.perm = perm;
        self.ki = ki;
    }

    /// Get current version.
    #[inline(always)]
    pub const fn get_version(&self) -> u32 {
        self.version
    }

    /// Get current position (signed).
    #[inline(always)]
    pub const fn get_perm_ref(&self) -> &<LeafNode15<P> as TreeLeafNode<P>>::Perm {
        &self.perm
    }

    /// Get current position (signed).
    #[inline(always)]
    pub const fn get_ki(&self) -> isize {
        self.ki
    }

    /// Set current position.
    #[inline(always)]
    pub const fn set_ki(&mut self, ki: isize) {
        self.ki = ki;
    }

    /// Get the cached last ikey for monotonicity check.
    #[inline(always)]
    pub const fn last_ikey(&self) -> u64 {
        self.last_ikey
    }

    /// Set the cached last ikey after a successful slot read.
    #[inline(always)]
    pub const fn set_last_ikey(&mut self, ikey: u64) {
        self.last_ikey = ikey;
    }
}

impl<P> Default for BackStackElement<P>
where
    P: LeafPolicy,
{
    fn default() -> Self {
        Self::new(StdPtr::null())
    }
}

impl<P> Clone for BackStackElement<P>
where
    P: LeafPolicy,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root,
            leaf: self.leaf,
            version: self.version,
            perm: self.perm,
            ki: self.ki,
            last_ikey: self.last_ikey,
            _marker: PhantomData,
        }
    }
}

impl<P> Debug for BackStackElement<P>
where
    P: LeafPolicy,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("BackStackElement")
            .field("root", &self.root)
            .field("leaf", &self.leaf)
            .field("version", &self.version)
            .field("ki", &self.ki)
            .field("last_ikey", &self.last_ikey)
            .field("perm_size", &self.perm.size())
            .finish()
    }
}

// ============================================================================
//  Tests
// ============================================================================
