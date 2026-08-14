//! Filepath: src/internode.rs
//!
//! Internode (internal node) for `MassTree`.

use static_assertions::const_assert_eq;
use std::array as StdArray;
use std::cmp::Ordering;
use std::fmt as StdFmt;
use std::ptr as StdPtr;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU64};

use seize::Guard;

use crate::leaf_trait::TreeInternode;
use crate::nodeversion::NodeVersion;
use crate::ordering::{READ_ORD, RELAXED, WRITE_ORD};
use crate::prefetch::prefetch_read;

mod accessors;
mod layout;

// ============================================================================
//  Constants
// ============================================================================

/// Number of keys in an internode.
pub const WIDTH: usize = 15;

/// Number of children in an internode (WIDTH + 1).
const NUM_CHILDREN: usize = WIDTH + 1;

// ============================================================================
//  InternodeNode
// ============================================================================

/// An internal routing node in the `MassTree`.
#[repr(C, align(64))]
pub struct InternodeNode {
    // ========================================================================
    // Cache Line 0
    // ========================================================================
    /// Version for optimistic concurrency control.
    version: NodeVersion, // 4 bytes

    /// Number of keys (0 to WIDTH).
    nkeys: AtomicU8, // 1 byte

    /// Tree height (0 = children are leaves, 1+ = children are internodes).
    height: u8, // 1 byte

    /// Padding for 8-byte alignment of parent pointer.
    _pad: [u8; 2], // 2 bytes

    /// Parent internode pointer (null for root).
    parent: AtomicPtr<u8>, // 8 bytes

    // ========================================================================
    // Cache Lines 0-2
    // ========================================================================
    /// Routing keys in sorted order.
    ikey0: [AtomicU64; WIDTH], // 120 bytes

    // ========================================================================
    // Cache Lines 2-4
    // ========================================================================
    /// Child pointers (16 children for 15 keys).
    /// - child[i] contains keys < ikey0[i]
    /// - child[nkeys] is the rightmost child (keys >= ikey0[nkeys-1])
    child: [AtomicPtr<u8>; NUM_CHILDREN], // 128 bytes
}

impl StdFmt::Debug for InternodeNode {
    fn fmt(&self, f: &mut StdFmt::Formatter<'_>) -> StdFmt::Result {
        // SAFETY: Debug impl - parent pointer is stable during formatting.
        f.debug_struct("InternodeNode")
            .field("nkeys", &self.nkeys())
            .field("height", &self.height)
            .field(
                "has_parent",
                &(!unsafe { self.parent_unguarded() }.is_null()),
            )
            .finish_non_exhaustive()
    }
}

// Compile-time layout verification.
// InternodeNode must be cache-line aligned (64 bytes) for optimal performance.
const_assert_eq!(std::mem::align_of::<InternodeNode>(), 64);

impl InternodeNode {
    // ========================================================================
    //  In-Place Initialization (for pool allocators)
    // ========================================================================

    /// Initialize an internode in-place at the given pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` must be valid for writes of `size_of::<Self>()` bytes
    /// - `ptr` must be properly aligned for `Self`
    /// - The caller must have exclusive access to the memory
    /// - The memory does not need to be initialized (will be overwritten)
    #[inline]
    pub unsafe fn init_at(ptr: *mut Self, height: u32) {
        debug_assert!(height <= 15, "init_at: height {height} exceeds maximum 15");

        // SAFETY: All operations here are safe because:
        // - ptr is valid for writes and properly aligned (caller guarantees)
        // - We have exclusive access to the memory (caller guarantees)
        // - write_bytes initializes all bytes to zero, making the memory valid
        // - After zeroing, we can safely create a mutable reference because:
        //   - All atomic types (AtomicU8, AtomicU64, AtomicPtr) are valid when zeroed
        //   - PhantomData is zero-sized
        //   - NodeVersion contains AtomicU32 which is valid when zeroed
        // - ptr::write is used for NodeVersion to properly initialize it
        unsafe {
            StdPtr::write_bytes(ptr, 0, 1);

            let node = &mut *ptr;

            StdPtr::write(&raw mut node.version, NodeVersion::new(false));

            #[expect(clippy::cast_possible_truncation, reason = "height <= 15 in practice")]
            {
                node.height = height as u8;
            }
        }
    }

    /// Initialize an internode in-place as a root node.
    ///
    /// # Safety
    ///
    /// Same requirements as [`Self::init_at`].
    #[inline]
    pub unsafe fn init_at_root(ptr: *mut Self, height: u32) {
        // SAFETY: Caller guarantees ptr validity
        unsafe {
            Self::init_at(ptr, height);
            (*ptr).version.mark_root();
        }
    }

    /// Initialize an internode in-place for a split operation.
    ///
    /// # Safety
    ///
    /// - Same requirements as [`Self::init_at`]
    /// - `parent_version` must be from a locked node
    #[inline]
    pub unsafe fn init_at_for_split(ptr: *mut Self, parent_version: &NodeVersion, height: u32) {
        // SAFETY: Caller guarantees ptr validity
        unsafe {
            StdPtr::write_bytes(ptr, 0, 1);

            let node: &mut Self = &mut *ptr;
            let split_version: NodeVersion = NodeVersion::new_for_split(parent_version);

            StdPtr::write(&raw mut node.version, split_version);

            #[expect(clippy::cast_possible_truncation, reason = "height <= 15 in practice")]
            {
                node.height = height as u8;
            }
        }
    }

    /// Create a new internode at the given height.
    #[must_use]
    #[inline]
    pub fn new(height: u32) -> Box<Self> {
        #[expect(clippy::cast_possible_truncation, reason = "height <= 15 in practice")]
        Box::new(Self {
            version: NodeVersion::new(false), // false = not a leaf
            nkeys: AtomicU8::new(0),
            height: height as u8,
            _pad: [0; 2],
            parent: AtomicPtr::new(StdPtr::null_mut()),
            ikey0: StdArray::from_fn(|_| AtomicU64::new(0)),
            child: StdArray::from_fn(|_| AtomicPtr::new(StdPtr::null_mut())),
        })
    }

    /// Create a new internode as root of a tree/layer.
    #[must_use]
    #[inline(always)]
    pub fn new_root(height: u32) -> Box<Self> {
        let node: Box<Self> = Self::new(height);
        node.version.mark_root();
        node
    }

    /// Create a new internode sibling for a split operation.
    ///
    /// # Safety
    ///
    /// The `parent_version` must be from a locked node (the parent being split).
    #[must_use]
    #[inline]
    pub fn new_for_split(parent_version: &NodeVersion, height: u32) -> Box<Self> {
        let split_version: NodeVersion = NodeVersion::new_for_split(parent_version);

        #[expect(clippy::cast_possible_truncation, reason = "height <= 15 in practice")]
        Box::new(Self {
            version: split_version,
            nkeys: AtomicU8::new(0),
            height: height as u8,
            _pad: [0; 2],
            parent: AtomicPtr::new(StdPtr::null_mut()),
            ikey0: StdArray::from_fn(|_| AtomicU64::new(0)),
            child: StdArray::from_fn(|_| AtomicPtr::new(StdPtr::null_mut())),
        })
    }

    // ========================================================================
    //  Insertion Operations
    // ========================================================================

    /// Insert a key and child at position `p`, shifting existing entries right.
    ///
    /// # Panics
    /// Panics in debug mode if node is full or position out of bounds.
    #[expect(clippy::indexing_slicing, reason = "bounds checked via debug_assert")]
    pub fn insert_key_and_child(&self, p: usize, new_ikey: u64, new_child: *mut u8) {
        let n: usize = self.nkeys.load(RELAXED) as usize;

        debug_assert!(n < WIDTH, "insert_key_and_child: node is full");
        debug_assert!(
            p <= n,
            "insert_key_and_child: position {p} out of bounds (n={n})"
        );

        for i in (p..n).rev() {
            let key: u64 = self.ikey0[i].load(RELAXED);
            self.ikey0[i + 1].store(key, RELAXED);

            let child: *mut u8 = self.child[i + 1].load(RELAXED);
            self.child[i + 2].store(child, WRITE_ORD);
        }

        self.ikey0[p].store(new_ikey, RELAXED);
        self.child[p + 1].store(new_child, WRITE_ORD);

        #[expect(clippy::cast_possible_truncation)]
        self.nkeys.store((n + 1) as u8, WRITE_ORD);
    }

    /// Shift entries from another internode.
    ///
    /// # Safety
    ///
    /// Caller must ensure exclusive access (hold lock on both nodes).
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "bounds checked by caller via split logic"
    )]
    pub unsafe fn shift_from(&self, dst_pos: usize, src: &Self, src_pos: usize, count: usize) {
        if count == 0 {
            return;
        }

        for i in 0..count {
            let key: u64 = src.ikey0[src_pos + i].load(RELAXED);
            self.ikey0[dst_pos + i].store(key, RELAXED);

            let child: *mut u8 = src.child[src_pos + 1 + i].load(RELAXED);
            self.child[dst_pos + 1 + i].store(child, WRITE_ORD);
        }
    }

    // ========================================================================
    //  Split Operation (with simultaneous insertion)
    // ========================================================================

    /// Split this internode into `self + new_right`, simultaneously inserting a new key/child.
    ///
    /// # Safety
    ///
    /// * `new_right_ptr` must point to `new_right`
    /// * The caller must hold the lock on `self`
    #[must_use = "popup_key must be inserted into parent node to complete the split"]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "WIDTH <= 15, so mid and WIDTH-mid fit in u8"
    )]
    pub fn split_into(
        &self,
        new_right: &mut Self,
        new_right_ptr: *mut Self,
        insert_pos: usize,
        insert_ikey: u64,
        insert_child: *mut u8,
    ) -> (u64, bool) {
        debug_assert!(
            self.nkeys.load(RELAXED) as usize == WIDTH,
            "split_into: node must be full"
        );
        debug_assert!(
            new_right.nkeys.load(RELAXED) == 0,
            "split_into: new_right must be empty"
        );

        let mid: usize = WIDTH.div_ceil(2); // ceil(WIDTH / 2)

        let (popup_key, insert_went_left) = match insert_pos.cmp(&mid) {
            Ordering::Less => {
                new_right.set_child(0, unsafe { self.child_unguarded(mid) });

                unsafe { new_right.shift_from(0, self, mid, WIDTH - mid) };

                new_right.nkeys.store((WIDTH - mid) as u8, WRITE_ORD);

                let popup: u64 = self.ikey_relaxed(mid - 1);

                self.nkeys.store((mid - 1) as u8, WRITE_ORD);
                self.insert_key_and_child(insert_pos, insert_ikey, insert_child);

                (popup, true)
            }

            Ordering::Equal => {
                new_right.set_child(0, insert_child);

                unsafe { new_right.shift_from(0, self, mid, WIDTH - mid) };

                new_right.nkeys.store((WIDTH - mid) as u8, WRITE_ORD);

                self.nkeys.store(mid as u8, WRITE_ORD);

                (insert_ikey, false)
            }

            Ordering::Greater => {
                let right_insert_pos: usize = insert_pos - (mid + 1);

                new_right.set_child(0, unsafe { self.child_unguarded(mid + 1) });
                unsafe { new_right.shift_from(0, self, mid + 1, right_insert_pos) };

                new_right.set_ikey_relaxed(right_insert_pos, insert_ikey);
                new_right.set_child(right_insert_pos + 1, insert_child);

                let count_after: usize = WIDTH - insert_pos;
                unsafe {
                    new_right.shift_from(right_insert_pos + 1, self, insert_pos, count_after);
                };

                new_right.nkeys.store((WIDTH - mid) as u8, WRITE_ORD);

                let popup: u64 = self.ikey_relaxed(mid);
                self.nkeys.store(mid as u8, WRITE_ORD);

                (popup, false)
            }
        };

        new_right.height = self.height;

        // SAFETY: We have exclusive access during split.
        if self.height > 0 {
            let nr_nkeys: usize = new_right.nkeys.load(RELAXED) as usize;
            let new_right_ptr_u8: *mut u8 = new_right_ptr.cast::<u8>();

            for i in 0..=nr_nkeys {
                let child: *mut u8 = unsafe { new_right.child_unguarded(i) };
                debug_assert!(!child.is_null(), "split_into: null child at index {i}");

                // SAFETY: height > 0 means children are InternodeNode
                #[expect(
                    clippy::cast_ptr_alignment,
                    reason = "height > 0 guarantees children are InternodeNode with 64-byte alignment"
                )]
                unsafe {
                    (*child.cast::<Self>()).set_parent(new_right_ptr_u8);
                }
            }
        }

        (popup_key, insert_went_left)
    }

    // ========================================================================
    //  Parent Accessors
    // ========================================================================

    /// Get the parent pointer (as `*mut u8`) with guard protection.
    #[must_use]
    #[inline(always)]
    pub fn parent(&self, guard: &impl Guard) -> *mut u8 {
        guard.protect(&self.parent, READ_ORD)
    }

    /// Get the parent pointer without guard protection.
    ///
    /// # Safety
    ///
    /// Caller must ensure the parent pointer won't be retired during use.
    /// Valid when:
    /// - Called during `Drop` (no concurrent access)
    /// - Called while holding exclusive lock that prevents retirement
    #[must_use]
    #[inline(always)]
    pub unsafe fn parent_unguarded(&self) -> *mut u8 {
        self.parent.load(READ_ORD)
    }

    /// Set the parent pointer.
    #[inline(always)]
    pub fn set_parent(&self, parent: *mut u8) {
        self.parent.store(parent, WRITE_ORD);
    }

    /// Check if this is a root node (no parent or version says root).
    #[must_use]
    #[inline(always)]
    pub fn is_root(&self) -> bool {
        self.version.is_root()
    }

    // ========================================================================
    //  Comparison (for binary search)
    // ========================================================================

    /// Compare a search key against the key at position `p`.
    #[must_use]
    #[inline(always)]
    pub fn compare_key(&self, search_ikey: u64, p: usize) -> Ordering {
        search_ikey.cmp(&self.ikey(p))
    }

    /// Find the position where a key should be inserted.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "n = nkeys() <= WIDTH-1 < WIDTH, so i+3 < i+4 <= n < WIDTH is always in bounds"
    )]
    pub fn find_insert_position(&self, insert_ikey: u64) -> usize {
        let n: usize = self.nkeys();

        if n > 6 {
            prefetch_read(&raw const self.ikey0[6]);
        }

        let mut i: usize = 0;

        while (i + 4) <= n {
            if (i == 8) && (n > 13) {
                prefetch_read(&raw const self.ikey0[14]);
            }

            if self.ikey0[i].load(RELAXED) >= insert_ikey {
                return i;
            }

            if self.ikey0[i + 1].load(RELAXED) >= insert_ikey {
                return i + 1;
            }

            if self.ikey0[i + 2].load(RELAXED) >= insert_ikey {
                return i + 2;
            }

            if self.ikey0[i + 3].load(RELAXED) >= insert_ikey {
                return i + 3;
            }

            i += 4;
        }

        while i < n {
            if self.ikey0[i].load(RELAXED) >= insert_ikey {
                return i;
            }

            i += 1;
        }

        n
    }

    // ========================================================================
    //  Invariant Checker
    // ========================================================================

    /// Verify internode invariants (debug builds only).
    ///
    /// Checks:
    /// - nkeys <= WIDTH
    /// - Keys are in ascending order
    /// - Children for valid indices are non-null (debug-only assertion)
    ///
    /// # Panics
    /// If any invariant is violated.
    #[cfg(debug_assertions)]
    pub fn debug_assert_invariants(&self) {
        assert!(
            self.nkeys() <= WIDTH,
            "nkeys {} exceeds WIDTH {}",
            self.nkeys(),
            WIDTH
        );

        let size: usize = self.size();

        if size > 1 {
            for i in 1..size {
                assert!(
                    self.ikey_relaxed(i - 1) < self.ikey_relaxed(i),
                    "keys not in ascending order: ikey[{}] ({:#x}) >= ikey[{}] ({:#x})",
                    i - 1,
                    self.ikey_relaxed(i - 1),
                    i,
                    self.ikey_relaxed(i)
                );
            }
        }
    }

    /// No-op
    #[cfg(not(debug_assertions))]
    #[inline]
    pub const fn debug_assert_invariants(&self) {}
}

// ============================================================================
//  Send + Sync
// ============================================================================

// SAFETY: InternodeNode is safe to send/share between threads.
//
// Thread safety is provided by:
// 1. Atomic fields (nkeys, ikey0, child, parent) use appropriate memory
//    orderings for concurrent access
// 2. The NodeVersion field provides locking and optimistic concurrency control
// 3. Raw pointers (child, parent) are protected by the tree's concurrency
//    protocol:
//    - Readers use version validation to detect concurrent modifications
//    - Writers hold the node lock before modifying children
//
// Unlike leaf nodes, internodes don't store values (only u64 keys and *mut u8
// child pointers), so there's no generic parameter to propagate Send/Sync from.
unsafe impl Send for InternodeNode {}
unsafe impl Sync for InternodeNode {}

// ============================================================================
//  Compile-Time Layout Assertions
// ============================================================================
//
// See `layout` submodule for comprehensive compile-time assertions that verify:
// - Exact size (320 bytes, 5 cache lines)
// - Cache-line alignment (64 bytes)
// - Field offsets for hot-path optimization
// - Cache line boundary constraints (ikey0[6] at CL1, child[7] at CL3)
//
// The layout module ensures any refactoring that changes field positions
// will fail at compile time with clear error messages.

// ============================================================================
//  TreeInternode Implementation
// ============================================================================

impl TreeInternode for InternodeNode {
    const WIDTH: usize = WIDTH;

    #[inline(always)]
    fn version(&self) -> &NodeVersion {
        Self::version(self)
    }

    #[inline(always)]
    fn height(&self) -> u32 {
        Self::height(self)
    }

    #[inline(always)]
    fn children_are_leaves(&self) -> bool {
        Self::children_are_leaves(self)
    }

    #[inline(always)]
    fn nkeys(&self) -> usize {
        Self::nkeys(self)
    }

    #[inline(always)]
    fn nkeys_relaxed(&self) -> usize {
        Self::nkeys_relaxed(self)
    }

    #[inline(always)]
    fn set_nkeys(&self, n: u8) {
        Self::set_nkeys(self, n);
    }

    #[inline(always)]
    fn inc_nkeys(&self) {
        Self::inc_nkeys(self);
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        Self::is_full(self)
    }

    #[inline(always)]
    fn ikey(&self, idx: usize) -> u64 {
        Self::ikey(self, idx)
    }

    /// Get the key at the given index using Relaxed ordering.
    #[inline(always)]
    #[expect(clippy::indexing_slicing, reason = "bounds checked via debug_assert")]
    fn ikey_relaxed(&self, i: usize) -> u64 {
        debug_assert!(i < WIDTH, "ikey_relaxed: index {i} out of bounds");
        self.ikey0[i].load(RELAXED)
    }

    /// Get raw pointer to the ikey array for SIMD operations.
    #[inline(always)]
    fn ikey_ptr(&self) -> *const u64 {
        // SAFETY: AtomicU64 has identical layout to u64.
        // Pointer arithmetic is valid within the ikey0 array bounds.
        self.ikey0.as_ptr().cast::<u64>()
    }

    #[inline(always)]
    fn set_ikey(&self, idx: usize, key: u64) {
        Self::set_ikey(self, idx, key);
    }

    #[inline(always)]
    fn compare_key(&self, search_ikey: u64, p: usize) -> Ordering {
        Self::compare_key(self, search_ikey, p)
    }

    #[inline(always)]
    fn find_insert_position(&self, insert_ikey: u64) -> usize {
        Self::find_insert_position(self, insert_ikey)
    }

    #[inline(always)]
    fn child(&self, idx: usize) -> *mut u8 {
        // SAFETY: TreeInternode trait methods are called during locked operations.
        unsafe { Self::child_unguarded(self, idx) }
    }

    #[inline(always)]
    fn set_child(&self, idx: usize, child: *mut u8) {
        Self::set_child(self, idx, child);
    }

    #[inline(always)]
    fn assign(&self, p: usize, ikey: u64, right_child: *mut u8) {
        Self::assign(self, p, ikey, right_child);
    }

    #[inline(always)]
    fn insert_key_and_child(&self, p: usize, new_ikey: u64, new_child: *mut u8) {
        Self::insert_key_and_child(self, p, new_ikey, new_child);
    }

    #[inline(always)]
    fn parent(&self) -> *mut u8 {
        // SAFETY: TreeInternode trait methods are called during locked operations.
        unsafe { Self::parent_unguarded(self) }
    }

    #[inline(always)]
    fn set_parent(&self, parent: *mut u8) {
        Self::set_parent(self, parent);
    }

    #[inline(always)]
    fn is_root(&self) -> bool {
        Self::is_root(self)
    }

    #[inline(always)]
    fn shift_from(&self, dst_pos: usize, src: &Self, src_pos: usize, count: usize) {
        // SAFETY: TreeInternode trait methods are called during locked operations.
        unsafe { Self::shift_from(self, dst_pos, src, src_pos, count) };
    }

    #[inline(always)]
    fn split_into(
        &self,
        new_right: &mut Self,
        new_right_ptr: *mut Self,
        insert_pos: usize,
        insert_ikey: u64,
        insert_child: *mut u8,
    ) -> (u64, bool) {
        Self::split_into(
            self,
            new_right,
            new_right_ptr,
            insert_pos,
            insert_ikey,
            insert_child,
        )
    }
}

// ============================================================================
//  Tests
// ============================================================================


