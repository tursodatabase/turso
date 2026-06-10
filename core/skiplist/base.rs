//! A lock-free skip list. See [`SkipList`].

use core::borrow::Borrow;
use core::cmp;
use core::fmt;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Bound, Deref, Index, RangeBounds};
use core::ptr;
use core::sync::atomic::{fence, AtomicUsize, Ordering};
use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};

use crossbeam_epoch::{self as epoch, Atomic, Collector, Guard, Shared};
use crossbeam_utils::CachePadded;

/// Number of bits needed to store height.
const HEIGHT_BITS: usize = 5;

/// Maximum height of a skip list tower.
const MAX_HEIGHT: usize = 1 << HEIGHT_BITS;

/// The bits of `refs_and_height` that keep the height.
const HEIGHT_MASK: usize = (1 << HEIGHT_BITS) - 1;

/// The tower of atomic pointers.
///
/// The actual size of the tower will vary depending on the height that a node
/// was allocated with.
#[repr(C)]
struct Tower<K, V> {
    pointers: [Atomic<Node<K, V>>; 0],
}

impl<K, V> Index<usize> for Tower<K, V> {
    type Output = Atomic<Node<K, V>>;
    fn index(&self, index: usize) -> &Atomic<Node<K, V>> {
        // This implementation is actually unsafe since we don't check if the
        // index is in-bounds. But this is fine since this is only used internally.
        unsafe { &*(&self.pointers as *const Atomic<Node<K, V>>).add(index) }
    }
}

/// Tower at the head of a skip list.
///
/// This is located in the `SkipList` struct itself and holds a full height
/// tower.
#[repr(C)]
struct Head<K, V> {
    pointers: [Atomic<Node<K, V>>; MAX_HEIGHT],
}

impl<K, V> Head<K, V> {
    /// Initializes a `Head`.
    #[inline]
    fn new() -> Self {
        // Initializing arrays in rust is a pain...
        Self {
            pointers: Default::default(),
        }
    }
}

impl<K, V> Deref for Head<K, V> {
    type Target = Tower<K, V>;
    fn deref(&self) -> &Tower<K, V> {
        unsafe { &*(self as *const _ as *const Tower<K, V>) }
    }
}

/// A skip list node.
///
/// This struct is marked with `repr(C)` so that the specific order of fields is enforced.
/// It is important that the tower is the last field since it is dynamically sized. The key,
/// reference count, and height are kept close to the tower to improve cache locality during
/// skip list traversal.
#[repr(C)]
struct Node<K, V> {
    /// The value.
    value: V,

    /// The key.
    key: K,

    /// Keeps the reference count and the height of its tower.
    ///
    /// The reference count is equal to the number of `Entry`s pointing to this node, plus the
    /// number of levels in which this node is installed.
    refs_and_height: AtomicUsize,

    /// The tower of atomic pointers.
    tower: Tower<K, V>,
}

impl<K, V> Node<K, V> {
    /// Allocates a node.
    ///
    /// The returned node will start with reference count of `ref_count` and the tower will be initialized
    /// with null pointers. However, the key and the value will be left uninitialized, and that is
    /// why this function is unsafe.
    unsafe fn alloc(height: usize, ref_count: usize) -> *mut Self {
        let layout = Self::get_layout(height);
        unsafe {
            let ptr = alloc(layout).cast::<Self>();
            if ptr.is_null() {
                handle_alloc_error(layout);
            }

            ptr::addr_of_mut!((*ptr).refs_and_height)
                .write(AtomicUsize::new((height - 1) | ref_count << HEIGHT_BITS));
            ptr::addr_of_mut!((*ptr).tower.pointers)
                .cast::<Atomic<Self>>()
                .write_bytes(0, height);
            ptr
        }
    }

    /// Deallocates a node.
    ///
    /// This function will not run any destructors.
    unsafe fn dealloc(ptr: *mut Self) {
        unsafe {
            let height = (*ptr).height();
            let layout = Self::get_layout(height);
            dealloc(ptr.cast::<u8>(), layout);
        }
    }

    /// Returns the layout of a node with the given `height`.
    fn get_layout(height: usize) -> Layout {
        assert!((1..=MAX_HEIGHT).contains(&height));

        Layout::new::<Self>()
            .extend(Layout::array::<Atomic<Self>>(height).unwrap())
            .unwrap()
            .0
            .pad_to_align()
    }

    /// Returns the height of this node's tower.
    #[inline]
    fn height(&self) -> usize {
        (self.refs_and_height.load(Ordering::Relaxed) & HEIGHT_MASK) + 1
    }

    /// Marks all pointers in the tower and returns `true` if the level 0 was not marked.
    fn mark_tower(&self) -> bool {
        let height = self.height();

        for level in (0..height).rev() {
            let tag = unsafe {
                // We're loading the pointer only for the tag, so it's okay to use
                // `epoch::unprotected()` in this situation.
                // TODO(Amanieu): can we use release ordering here?
                self.tower[level]
                    .fetch_or(1, Ordering::SeqCst, epoch::unprotected())
                    .tag()
            };

            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && tag == 1 {
                return false;
            }
        }

        // We marked the level 0 pointer, therefore we removed the node.
        true
    }

    /// Returns `true` if the node is removed.
    #[inline]
    fn is_removed(&self) -> bool {
        let tag = unsafe {
            // We're loading the pointer only for the tag, so it's okay to use
            // `epoch::unprotected()` in this situation.
            self.tower[0]
                .load(Ordering::Relaxed, epoch::unprotected())
                .tag()
        };
        tag == 1
    }

    /// Attempts to increment the reference count of a node and returns `true` on success.
    ///
    /// The reference count can be incremented only if it is non-zero.
    ///
    /// # Panics
    ///
    /// Panics if the reference count overflows.
    #[inline]
    unsafe fn try_increment(&self) -> bool {
        let mut refs_and_height = self.refs_and_height.load(Ordering::Relaxed);

        loop {
            // If the reference count is zero, then the node has already been
            // queued for deletion. Incrementing it again could lead to a
            // double-free.
            if refs_and_height & !HEIGHT_MASK == 0 {
                return false;
            }

            // If all bits in the reference count are ones, we're about to overflow it.
            let new_refs_and_height = refs_and_height
                .checked_add(1 << HEIGHT_BITS)
                .expect("SkipList reference count overflow");

            // Try incrementing the count.
            match self.refs_and_height.compare_exchange_weak(
                refs_and_height,
                new_refs_and_height,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => refs_and_height = current,
            }
        }
    }

    /// Decrements the reference count of a node, destroying it if the count becomes zero.
    #[inline]
    unsafe fn decrement(&self, guard: &Guard) {
        if self
            .refs_and_height
            .fetch_sub(1 << HEIGHT_BITS, Ordering::Release)
            >> HEIGHT_BITS
            == 1
        {
            fence(Ordering::Acquire);
            unsafe { guard.defer_unchecked(move || Self::finalize(self)) }
        }
    }

    /// Decrements the reference count of a node, pinning the thread and destroying the node
    /// if the count become zero.
    #[inline]
    unsafe fn decrement_with_pin<F>(&self, parent: &SkipList<K, V>, pin: F)
    where
        F: FnOnce() -> Guard,
    {
        if self
            .refs_and_height
            .fetch_sub(1 << HEIGHT_BITS, Ordering::Release)
            >> HEIGHT_BITS
            == 1
        {
            fence(Ordering::Acquire);
            let guard = &pin();
            parent.check_guard(guard);
            unsafe { guard.defer_unchecked(move || Self::finalize(self)) }
        }
    }

    /// Drops the key and value of a node, then deallocates it.
    #[cold]
    unsafe fn finalize(ptr: *const Self) {
        let ptr = ptr as *mut Self;

        unsafe {
            // Call destructors: drop the key and the value.
            ptr::drop_in_place(&mut (*ptr).key);
            ptr::drop_in_place(&mut (*ptr).value);

            // Finally, deallocate the memory occupied by the node.
            Self::dealloc(ptr);
        }
    }
}

impl<K, V> fmt::Debug for Node<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Node")
            .field(&self.key)
            .field(&self.value)
            .finish()
    }
}

/// A search result.
///
/// The result indicates whether the key was found, as well as what were the adjacent nodes to the
/// key on each level of the skip list.
struct Position<'a, K, V> {
    /// Reference to a node with the given key, if found.
    ///
    /// If this is `Some` then it will point to the same node as `right[0]`.
    found: Option<&'a Node<K, V>>,

    /// Adjacent nodes with smaller keys (predecessors).
    left: [&'a Tower<K, V>; MAX_HEIGHT],

    /// Adjacent nodes with equal or greater keys (successors).
    right: [Shared<'a, Node<K, V>>; MAX_HEIGHT],
}

/// Frequently modified data associated with a skip list.
struct HotData {
    /// The seed for random height generation.
    seed: AtomicUsize,

    /// The number of entries in the skip list.
    len: AtomicUsize,

    /// Highest tower currently in use. This value is used as a hint for where
    /// to start lookups and never decreases.
    max_height: AtomicUsize,
}

/// A lock-free skip list.
// TODO(stjepang): Embed a custom `epoch::Collector` inside `SkipList<K, V>`. Instead of adding
// garbage to the default global collector, we should add it to a local collector tied to the
// particular skip list instance.
//
// Since global collector might destroy garbage arbitrarily late in the future, some skip list
// methods have `K: 'static` and `V: 'static` bounds. But a local collector embedded in the skip
// list would destroy all remaining garbage when the skip list is dropped, so in that case we'd be
// able to remove those bounds on types `K` and `V`.
//
// As a further future optimization, if `!mem::needs_drop::<K>() && !mem::needs_drop::<V>()`
// (neither key nor the value have destructors), there's no point in creating a new local
// collector, so we should simply use the global one.
pub struct SkipList<K, V> {
    /// The head of the skip list (just a dummy node, not a real entry).
    head: Head<K, V>,

    /// The `Collector` associated with this skip list.
    collector: Collector,

    /// Hot data associated with the skip list, stored in a dedicated cache line.
    hot_data: CachePadded<HotData>,
}

unsafe impl<K: Send + Sync, V: Send + Sync> Send for SkipList<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for SkipList<K, V> {}

impl<K, V> SkipList<K, V> {
    /// Returns a new, empty skip list.
    pub fn new(collector: Collector) -> Self {
        Self {
            head: Head::new(),
            collector,
            hot_data: CachePadded::new(HotData {
                seed: AtomicUsize::new(1),
                len: AtomicUsize::new(0),
                max_height: AtomicUsize::new(1),
            }),
        }
    }

    /// Returns `true` if the skip list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of entries in the skip list.
    ///
    /// If the skip list is being concurrently modified, consider the returned number just an
    /// approximation without any guarantees.
    pub fn len(&self) -> usize {
        let len = self.hot_data.len.load(Ordering::Relaxed);

        // Due to the relaxed memory ordering, the length counter may sometimes
        // underflow and produce a very large value. We treat such values as 0.
        if len > isize::MAX as usize {
            0
        } else {
            len
        }
    }

    /// Ensures that all `Guard`s used with the skip list come from the same
    /// `Collector`.
    fn check_guard(&self, guard: &Guard) {
        if let Some(c) = guard.collector() {
            assert!(c == &self.collector);
        }
    }
}

impl<K, V> SkipList<K, V>
where
    K: Ord,
{
    /// Returns the entry with the smallest key.
    pub fn front<'a: 'g, 'g>(&'a self, guard: &'g Guard) -> Option<Entry<'a, 'g, K, V>> {
        self.check_guard(guard);
        let n = self.next_node(&self.head, Bound::Unbounded, guard)?;
        Some(Entry {
            parent: self,
            node: n,
            guard,
        })
    }

    /// Returns the entry with the largest key.
    pub fn back<'a: 'g, 'g>(&'a self, guard: &'g Guard) -> Option<Entry<'a, 'g, K, V>> {
        self.check_guard(guard);
        let n = self.search_bound(Bound::Unbounded, true, guard)?;
        Some(Entry {
            parent: self,
            node: n,
            guard,
        })
    }

    /// Returns `true` if the map contains a value for the specified key.
    pub fn contains_key<Q>(&self, key: &Q, guard: &Guard) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.get(key, guard).is_some()
    }

    /// Returns an entry with the specified `key`.
    pub fn get<'a: 'g, 'g, Q>(&'a self, key: &Q, guard: &'g Guard) -> Option<Entry<'a, 'g, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.check_guard(guard);
        let n = self.search_bound(Bound::Included(key), false, guard)?;
        if n.key.borrow() != key {
            return None;
        }
        Some(Entry {
            parent: self,
            node: n,
            guard,
        })
    }

    /// Returns an `Entry` pointing to the lowest element whose key is above
    /// the given bound. If no such element is found then `None` is
    /// returned.
    pub fn lower_bound<'a: 'g, 'g, Q>(
        &'a self,
        bound: Bound<&Q>,
        guard: &'g Guard,
    ) -> Option<Entry<'a, 'g, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.check_guard(guard);
        let n = self.search_bound(bound, false, guard)?;
        Some(Entry {
            parent: self,
            node: n,
            guard,
        })
    }

    /// Returns an `Entry` pointing to the highest element whose key is below
    /// the given bound. If no such element is found then `None` is
    /// returned.
    pub fn upper_bound<'a: 'g, 'g, Q>(
        &'a self,
        bound: Bound<&Q>,
        guard: &'g Guard,
    ) -> Option<Entry<'a, 'g, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.check_guard(guard);
        let n = self.search_bound(bound, true, guard)?;
        Some(Entry {
            parent: self,
            node: n,
            guard,
        })
    }

    /// Finds an entry with the specified key, or inserts a new `key`-`value` pair if none exist.
    pub fn get_or_insert(&self, key: K, value: V, guard: &Guard) -> RefEntry<'_, K, V> {
        self.insert_internal(key, || value, |_| false, guard)
    }

    /// Finds an entry with the specified key, or inserts a new `key`-`value` pair if none exist,
    /// where value is calculated with a function.
    ///
    ///
    /// <b>Note:</b> Another thread may write key value first, leading to the result of this closure
    /// discarded. If closure is modifying some other state (such as shared counters or shared
    /// objects), it may lead to <u>undesired behaviour</u> such as counters being changed without
    /// result of closure inserted
    pub fn get_or_insert_with<F>(&self, key: K, value: F, guard: &Guard) -> RefEntry<'_, K, V>
    where
        F: FnOnce() -> V,
    {
        self.insert_internal(key, value, |_| false, guard)
    }

    /// Returns an iterator over all entries in the skip list.
    pub fn iter<'a: 'g, 'g>(&'a self, guard: &'g Guard) -> Iter<'a, 'g, K, V> {
        self.check_guard(guard);
        Iter {
            parent: self,
            head: None,
            tail: None,
            guard,
        }
    }

    /// Returns an iterator over all entries in the skip list.
    pub fn ref_iter(&self) -> RefIter<'_, K, V> {
        RefIter {
            parent: self,
            head: None,
            tail: None,
        }
    }

    /// Returns an iterator over a subset of entries in the skip list.
    pub fn range<'a: 'g, 'g, Q, R>(
        &'a self,
        range: R,
        guard: &'g Guard,
    ) -> Range<'a, 'g, Q, R, K, V>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        self.check_guard(guard);
        Range {
            parent: self,
            head: None,
            tail: None,
            range,
            guard,
            _marker: PhantomData,
        }
    }

    /// Returns an iterator over a subset of entries in the skip list.
    #[allow(clippy::needless_lifetimes)]
    pub fn ref_range<'a, Q, R>(&'a self, range: R) -> RefRange<'a, Q, R, K, V>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        RefRange {
            parent: self,
            range,
            head: None,
            tail: None,
            _marker: PhantomData,
        }
    }

    /// Generates a random height and returns it.
    fn random_height(&self) -> usize {
        // Pseudorandom number generation from "Xorshift RNGs" by George Marsaglia.
        //
        // This particular set of operations generates 32-bit integers. See:
        // https://en.wikipedia.org/wiki/Xorshift#Example_implementation
        let mut num = self.hot_data.seed.load(Ordering::Relaxed);
        num ^= num << 13;
        num ^= num >> 17;
        num ^= num << 5;
        self.hot_data.seed.store(num, Ordering::Relaxed);

        let mut height = cmp::min(MAX_HEIGHT, num.trailing_zeros() as usize + 1);
        unsafe {
            // Keep decreasing the height while it's much larger than all towers currently in the
            // skip list.
            //
            // Note that we're loading the pointer only to check whether it is null, so it's okay
            // to use `epoch::unprotected()` in this situation.
            while height >= 4
                && self.head[height - 2]
                    .load(Ordering::Relaxed, epoch::unprotected())
                    .is_null()
            {
                height -= 1;
            }
        }

        // Track the max height to speed up lookups
        let mut max_height = self.hot_data.max_height.load(Ordering::Relaxed);
        while height > max_height {
            match self.hot_data.max_height.compare_exchange_weak(
                max_height,
                height,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(h) => max_height = h,
            }
        }
        height
    }

    /// If we encounter a deleted node while searching, help with the deletion
    /// by attempting to unlink the node from the list.
    ///
    /// If the unlinking is successful then this function returns the next node
    /// with which the search should continue on the current level.
    #[cold]
    unsafe fn help_unlink<'a>(
        &'a self,
        pred: &'a Atomic<Node<K, V>>,
        curr: &'a Node<K, V>,
        succ: Shared<'a, Node<K, V>>,
        guard: &'a Guard,
    ) -> Option<Shared<'a, Node<K, V>>> {
        // If `succ` is marked, that means `curr` is removed. Let's try
        // unlinking it from the skip list at this level.
        match pred.compare_exchange(
            Shared::from(curr as *const _),
            succ.with_tag(0),
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        ) {
            Ok(_) => {
                unsafe { curr.decrement(guard) }
                Some(succ.with_tag(0))
            }
            Err(_) => None,
        }
    }

    /// Returns the successor of a node.
    ///
    /// This will keep searching until a non-deleted node is found. If a deleted
    /// node is reached then a search is performed using the given key.
    fn next_node<'a>(
        &'a self,
        pred: &'a Tower<K, V>,
        lower_bound: Bound<&K>,
        guard: &'a Guard,
    ) -> Option<&'a Node<K, V>> {
        unsafe {
            // Load the level 0 successor of the current node.
            let mut curr = pred[0].load_consume(guard);

            // If `curr` is marked, that means `pred` is removed and we have to use
            // a key search.
            if curr.tag() == 1 {
                return self.search_bound(lower_bound, false, guard);
            }

            while let Some(c) = curr.as_ref() {
                let succ = c.tower[0].load_consume(guard);

                if succ.tag() == 1 {
                    if let Some(c) = self.help_unlink(&pred[0], c, succ, guard) {
                        // On success, continue searching through the current level.
                        curr = c;
                        continue;
                    } else {
                        // On failure, we cannot do anything reasonable to continue
                        // searching from the current position. Restart the search.
                        return self.search_bound(lower_bound, false, guard);
                    }
                }

                return Some(c);
            }

            None
        }
    }

    /// Searches for first/last node that is greater/less/equal to a key in the skip list.
    ///
    /// If `upper_bound == true`: the last node less than (or equal to) the key.
    ///
    /// If `upper_bound == false`: the first node greater than (or equal to) the key.
    ///
    /// This is unsafe because the returned nodes are bound to the lifetime of
    /// the `SkipList`, not the `Guard`.
    fn search_bound<'a, Q>(
        &'a self,
        bound: Bound<&Q>,
        upper_bound: bool,
        guard: &'a Guard,
    ) -> Option<&'a Node<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        unsafe {
            'search: loop {
                // The current level we're at.
                let mut level = self.hot_data.max_height.load(Ordering::Relaxed);

                // Fast loop to skip empty tower levels.
                while level >= 1
                    && self.head[level - 1]
                        .load(Ordering::Relaxed, guard)
                        .is_null()
                {
                    level -= 1;
                }

                // The current best node
                let mut result = None;

                // The predecessor node
                let mut pred = &*self.head;

                while level >= 1 {
                    level -= 1;

                    // Two adjacent nodes at the current level.
                    let mut curr = pred[level].load_consume(guard);

                    // If `curr` is marked, that means `pred` is removed and we have to restart the
                    // search.
                    if curr.tag() == 1 {
                        continue 'search;
                    }

                    // Iterate through the current level until we reach a node with a key greater
                    // than or equal to `key`.
                    while let Some(c) = curr.as_ref() {
                        let succ = c.tower[level].load_consume(guard);

                        if succ.tag() == 1 {
                            if let Some(c) = self.help_unlink(&pred[level], c, succ, guard) {
                                // On success, continue searching through the current level.
                                curr = c;
                                continue;
                            } else {
                                // On failure, we cannot do anything reasonable to continue
                                // searching from the current position. Restart the search.
                                continue 'search;
                            }
                        }

                        // If `curr` contains a key that is greater than (or equal) to `key`, we're
                        // done with this level.
                        //
                        // The condition determines whether we should stop the search. For the upper
                        // bound, we return the last node before the condition became true. For the
                        // lower bound, we return the first node after the condition became true.
                        if upper_bound {
                            if !below_upper_bound(&bound, c.key.borrow()) {
                                break;
                            }
                            result = Some(c);
                        } else if above_lower_bound(&bound, c.key.borrow()) {
                            result = Some(c);
                            break;
                        }

                        // Move one step forward.
                        pred = &c.tower;
                        curr = succ;
                    }
                }

                return result;
            }
        }
    }

    /// Searches for a key in the skip list and returns a list of all adjacent nodes.
    fn search_position<'a, Q>(&'a self, key: &Q, guard: &'a Guard) -> Position<'a, K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        unsafe {
            'search: loop {
                // The result of this search.
                let mut result = Position {
                    found: None,
                    left: [&*self.head; MAX_HEIGHT],
                    right: [Shared::null(); MAX_HEIGHT],
                };

                // The current level we're at.
                let mut level = self.hot_data.max_height.load(Ordering::Relaxed);

                // Fast loop to skip empty tower levels.
                while level >= 1
                    && self.head[level - 1]
                        .load(Ordering::Relaxed, guard)
                        .is_null()
                {
                    level -= 1;
                }

                // The predecessor node
                let mut pred = &*self.head;

                while level >= 1 {
                    level -= 1;

                    // Two adjacent nodes at the current level.
                    let mut curr = pred[level].load_consume(guard);

                    // If `curr` is marked, that means `pred` is removed and we have to restart the
                    // search.
                    if curr.tag() == 1 {
                        continue 'search;
                    }

                    // Iterate through the current level until we reach a node with a key greater
                    // than or equal to `key`.
                    while let Some(c) = curr.as_ref() {
                        let succ = c.tower[level].load_consume(guard);

                        if succ.tag() == 1 {
                            if let Some(c) = self.help_unlink(&pred[level], c, succ, guard) {
                                // On success, continue searching through the current level.
                                curr = c;
                                continue;
                            } else {
                                // On failure, we cannot do anything reasonable to continue
                                // searching from the current position. Restart the search.
                                continue 'search;
                            }
                        }

                        // If `curr` contains a key that is greater than or equal to `key`, we're
                        // done with this level.
                        match c.key.borrow().cmp(key) {
                            cmp::Ordering::Greater => break,
                            cmp::Ordering::Equal => {
                                result.found = Some(c);
                                break;
                            }
                            cmp::Ordering::Less => {}
                        }

                        // Move one step forward.
                        pred = &c.tower;
                        curr = succ;
                    }

                    // Store the position at the current level into the result.
                    result.left[level] = pred;
                    result.right[level] = curr;
                }

                return result;
            }
        }
    }

    /// Inserts an entry with the specified `key` and `value`.
    ///
    /// If `replace` is `true`, then any existing entry with this key will first be removed.
    fn insert_internal<F, CompareF>(
        &self,
        key: K,
        value: F,
        replace: CompareF,
        guard: &Guard,
    ) -> RefEntry<'_, K, V>
    where
        F: FnOnce() -> V,
        CompareF: Fn(&V) -> bool,
    {
        self.check_guard(guard);

        unsafe {
            // Rebind the guard to the lifetime of self. This is a bit of a
            // hack but it allows us to return references that are not bound to
            // the lifetime of the guard.
            let guard = &*(guard as *const _);

            let mut search;
            loop {
                // First try searching for the key.
                // Note that the `Ord` implementation for `K` may panic during the search.
                search = self.search_position(&key, guard);

                let r = match search.found {
                    Some(r) => r,
                    None => break,
                };
                let replace = replace(&r.value);
                if replace {
                    // If a node with the key was found and we should replace it, mark its tower
                    // and then repeat the search.
                    if r.mark_tower() {
                        self.hot_data.len.fetch_sub(1, Ordering::Relaxed);
                    }
                } else {
                    // If a node with the key was found and we're not going to replace it, let's
                    // try returning it as an entry.
                    if let Some(e) = RefEntry::try_acquire(self, r) {
                        return e;
                    }

                    // If we couldn't increment the reference count, that means someone has just
                    // now removed the node.
                    break;
                }
            }

            // create value before creating node, so extra allocation doesn't happen if value() function panics
            let value = value();
            // Create a new node.
            let height = self.random_height();
            let (node, n) = {
                // The reference count is initially two to account for:
                // 1. The entry that will be returned.
                // 2. The link at the level 0 of the tower.
                let n = Node::<K, V>::alloc(height, 2);

                // Write the key and the value into the node.
                ptr::addr_of_mut!((*n).key).write(key);
                ptr::addr_of_mut!((*n).value).write(value);

                (Shared::<Node<K, V>>::from(n as *const _), &*n)
            };

            // Optimistically increment `len`.
            self.hot_data.len.fetch_add(1, Ordering::Relaxed);

            loop {
                // Set the lowest successor of `n` to `search.right[0]`.
                n.tower[0].store(search.right[0], Ordering::Relaxed);

                // Try installing the new node into the skip list (at level 0).
                // TODO(Amanieu): can we use release ordering here?
                if search.left[0][0]
                    .compare_exchange(
                        search.right[0],
                        node,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    )
                    .is_ok()
                {
                    break;
                }

                // We failed. Let's search for the key and try again.
                {
                    // Create a guard that destroys the new node in case search panics.
                    struct ScopeGuard<K, V>(*const Node<K, V>);
                    impl<K, V> Drop for ScopeGuard<K, V> {
                        fn drop(&mut self) {
                            unsafe { Node::finalize(self.0) }
                        }
                    }
                    let sg = ScopeGuard(node.as_raw());
                    search = self.search_position(&n.key, guard);
                    mem::forget(sg);
                }

                if let Some(r) = search.found {
                    let replace = replace(&r.value);
                    if replace {
                        // If a node with the key was found and we should replace it, mark its
                        // tower and then repeat the search.
                        if r.mark_tower() {
                            self.hot_data.len.fetch_sub(1, Ordering::Relaxed);
                        }
                    } else {
                        // If a node with the key was found and we're not going to replace it,
                        // let's try returning it as an entry.
                        if let Some(e) = RefEntry::try_acquire(self, r) {
                            // Destroy the new node.
                            Node::finalize(node.as_raw());
                            self.hot_data.len.fetch_sub(1, Ordering::Relaxed);

                            return e;
                        }

                        // If we couldn't increment the reference count, that means someone has
                        // just now removed the node.
                    }
                }
            }

            // The new node was successfully installed. Let's create an entry associated with it.
            let entry = RefEntry {
                parent: self,
                node: n,
            };

            // Build the rest of the tower above level 0.
            'build: for level in 1..height {
                loop {
                    // Obtain the predecessor and successor at the current level.
                    let pred = search.left[level];
                    let succ = search.right[level];

                    // Load the current value of the pointer in the tower at this level.
                    // TODO(Amanieu): can we use relaxed ordering here?
                    let next = n.tower[level].load(Ordering::SeqCst, guard);

                    // If the current pointer is marked, that means another thread is already
                    // removing the node we've just inserted. In that case, let's just stop
                    // building the tower.
                    if next.tag() == 1 {
                        break 'build;
                    }

                    // When searching for `key` and traversing the skip list from the highest level
                    // to the lowest, it is possible to observe a node with an equal key at higher
                    // levels and then find it missing at the lower levels if it gets removed
                    // during traversal. Even worse, it is possible to observe completely different
                    // nodes with the exact same key at different levels.
                    //
                    // Linking the new node to a dead successor with an equal key could create
                    // subtle corner cases that would require special care. It's much easier to
                    // simply prohibit linking two nodes with equal keys.
                    //
                    // If the successor has the same key as the new node, that means it is marked
                    // as removed and should be unlinked from the skip list. In that case, let's
                    // repeat the search to make sure it gets unlinked and try again.
                    //
                    // If this comparison or the following search panics, we simply stop building
                    // the tower without breaking any invariants. Note that building higher levels
                    // is completely optional. Only the lowest level really matters, and all the
                    // higher levels are there just to make searching faster.
                    if succ.as_ref().map(|s| &s.key) == Some(&n.key) {
                        search = self.search_position(&n.key, guard);
                        continue;
                    }

                    // Change the pointer at the current level from `next` to `succ`. If this CAS
                    // operation fails, that means another thread has marked the pointer and we
                    // should stop building the tower.
                    // TODO(Amanieu): can we use release ordering here?
                    if n.tower[level]
                        .compare_exchange(next, succ, Ordering::SeqCst, Ordering::SeqCst, guard)
                        .is_err()
                    {
                        break 'build;
                    }

                    // Increment the reference count. The current value will always be at least 1
                    // because we are holding `entry`.
                    n.refs_and_height
                        .fetch_add(1 << HEIGHT_BITS, Ordering::Relaxed);

                    // Try installing the new node at the current level.
                    // TODO(Amanieu): can we use release ordering here?
                    if pred[level]
                        .compare_exchange(succ, node, Ordering::SeqCst, Ordering::SeqCst, guard)
                        .is_ok()
                    {
                        // Success! Continue on the next level.
                        break;
                    }

                    // Installation failed. Decrement the reference count.
                    n.refs_and_height
                        .fetch_sub(1 << HEIGHT_BITS, Ordering::Relaxed);

                    // We don't have the most up-to-date search results. Repeat the search.
                    //
                    // If this search panics, we simply stop building the tower without breaking
                    // any invariants. Note that building higher levels is completely optional.
                    // Only the lowest level really matters, and all the higher levels are there
                    // just to make searching faster.
                    search = self.search_position(&n.key, guard);
                }
            }

            // If any pointer in the tower is marked, that means our node is in the process of
            // removal or already removed. It is possible that another thread (either partially or
            // completely) removed the new node while we were building the tower, and just after
            // that we installed the new node at one of the higher levels. In order to undo that
            // installation, we must repeat the search, which will unlink the new node at that
            // level.
            // TODO(Amanieu): can we use relaxed ordering here?
            if n.tower[height - 1].load(Ordering::SeqCst, guard).tag() == 1 {
                self.search_bound(Bound::Included(&n.key), false, guard);
            }

            // Finally, return the new entry.
            entry
        }
    }
}

impl<K, V> SkipList<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    /// Inserts a `key`-`value` pair into the skip list and returns the new entry.
    ///
    /// If there is an existing entry with this key, it will be removed before inserting the new
    /// one.
    pub fn insert(&self, key: K, value: V, guard: &Guard) -> RefEntry<'_, K, V> {
        self.insert_internal(key, || value, |_| true, guard)
    }

    /// Inserts a `key`-`value` pair into the skip list and returns the new entry.
    ///
    /// If there is an existing entry with this key and compare(entry.value) returns true,
    /// it will be removed before inserting the new one.
    /// The closure will not be called if the key is not present.
    pub fn compare_insert<F>(
        &self,
        key: K,
        value: V,
        compare_fn: F,
        guard: &Guard,
    ) -> RefEntry<'_, K, V>
    where
        F: Fn(&V) -> bool,
    {
        self.insert_internal(key, || value, compare_fn, guard)
    }

    /// Removes an entry with the specified `key` from the map and returns it.
    pub fn remove<Q>(&self, key: &Q, guard: &Guard) -> Option<RefEntry<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.check_guard(guard);

        unsafe {
            // Rebind the guard to the lifetime of self. This is a bit of a
            // hack but it allows us to return references that are not bound to
            // the lifetime of the guard.
            let guard = &*(guard as *const _);

            loop {
                // Try searching for the key.
                let search = self.search_position(key, guard);

                let n = search.found?;

                // First try incrementing the reference count because we have to return the node as
                // an entry. If this fails, repeat the search.
                let entry = match RefEntry::try_acquire(self, n) {
                    Some(e) => e,
                    None => continue,
                };

                // Try removing the node by marking its tower.
                if n.mark_tower() {
                    // Success! Decrement `len`.
                    self.hot_data.len.fetch_sub(1, Ordering::Relaxed);

                    // Unlink the node at each level of the skip list. We could do this by simply
                    // repeating the search, but it's usually faster to unlink it manually using
                    // the `left` and `right` lists.
                    for level in (0..n.height()).rev() {
                        // TODO(Amanieu): can we use relaxed ordering here?
                        let succ = n.tower[level].load(Ordering::SeqCst, guard).with_tag(0);

                        // Try linking the predecessor and successor at this level.
                        // TODO(Amanieu): can we use release ordering here?
                        if search.left[level][level]
                            .compare_exchange(
                                Shared::from(n as *const _),
                                succ,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                guard,
                            )
                            .is_ok()
                        {
                            // Success! Decrement the reference count.
                            n.decrement(guard);
                        } else {
                            // Failed! Just repeat the search to completely unlink the node.
                            self.search_bound(Bound::Included(key), false, guard);
                            break;
                        }
                    }
                }
                return Some(entry);
            }
        }
    }

    /// Removes an entry from the front of the skip list.
    pub fn pop_front(&self, guard: &Guard) -> Option<RefEntry<'_, K, V>> {
        self.check_guard(guard);
        loop {
            let e = self.front(guard)?;
            if let Some(e) = e.pin() {
                if e.remove(guard) {
                    return Some(e);
                } else {
                    e.release(guard);
                }
            }
        }
    }

    /// Removes an entry from the back of the skip list.
    pub fn pop_back(&self, guard: &Guard) -> Option<RefEntry<'_, K, V>> {
        self.check_guard(guard);
        loop {
            let e = self.back(guard)?;
            if let Some(e) = e.pin() {
                if e.remove(guard) {
                    return Some(e);
                } else {
                    e.release(guard);
                }
            }
        }
    }

    /// Iterates over the map and removes every entry.
    pub fn clear(&self, guard: &mut Guard) {
        self.check_guard(guard);

        /// Number of steps after which we repin the current thread and unlink removed nodes.
        const BATCH_SIZE: usize = 100;

        loop {
            {
                // Search for the first entry in order to unlink all the preceding entries
                // we have removed.
                //
                // By unlinking nodes in batches we make sure that the final search doesn't
                // unlink all nodes at once, which could keep the current thread pinned for a
                // long time.
                let mut entry = self.lower_bound(Bound::Unbounded, guard);

                for _ in 0..BATCH_SIZE {
                    // Stop if we have reached the end of the list.
                    let e = match entry {
                        None => return,
                        Some(e) => e,
                    };

                    // Before removing the current entry, first obtain the following one.
                    let next = e.next();

                    // Try removing the current entry.
                    if e.node.mark_tower() {
                        // Success! Decrement `len`.
                        self.hot_data.len.fetch_sub(1, Ordering::Relaxed);
                    }

                    entry = next;
                }
            }

            // Repin the current thread because we don't want to keep it pinned in the same
            // epoch for a too long time.
            guard.repin();
        }
    }
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut node = self.head[0]
                .load(Ordering::Relaxed, epoch::unprotected())
                .as_ref();

            // Iterate through the whole skip list and destroy every node.
            while let Some(n) = node {
                // Unprotected loads are okay because this function is the only one currently using
                // the skip list.
                let next = n.tower[0]
                    .load(Ordering::Relaxed, epoch::unprotected())
                    .as_ref();

                // Deallocate every node.
                Node::finalize(n);

                node = next;
            }
        }
    }
}

impl<K, V> fmt::Debug for SkipList<K, V>
where
    K: Ord + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("SkipList { .. }")
    }
}

impl<K, V> IntoIterator for SkipList<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        unsafe {
            // Load the front node.
            //
            // Unprotected loads are okay because this function is the only one currently using
            // the skip list.
            let front = self.head[0]
                .load(Ordering::Relaxed, epoch::unprotected())
                .as_raw();

            // Clear the skip list by setting all pointers in head to null.
            for level in 0..MAX_HEIGHT {
                self.head[level].store(Shared::null(), Ordering::Relaxed);
            }

            IntoIter {
                node: front as *mut Node<K, V>,
            }
        }
    }
}

/// An entry in a skip list, protected by a `Guard`.
///
/// The lifetimes of the key and value are the same as that of the `Guard`
/// used when creating the `Entry` (`'g`). This lifetime is also constrained to
/// not outlive the `SkipList`.
pub struct Entry<'a: 'g, 'g, K, V> {
    parent: &'a SkipList<K, V>,
    node: &'g Node<K, V>,
    guard: &'g Guard,
}

impl<'a: 'g, 'g, K: 'a, V: 'a> Entry<'a, 'g, K, V> {
    /// Returns `true` if the entry is removed from the skip list.
    pub fn is_removed(&self) -> bool {
        self.node.is_removed()
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &'g K {
        &self.node.key
    }

    /// Returns a reference to the value.
    pub fn value(&self) -> &'g V {
        &self.node.value
    }

    /// Returns a reference to the parent `SkipList`
    pub fn skiplist(&self) -> &'a SkipList<K, V> {
        self.parent
    }

    /// Attempts to pin the entry with a reference count, ensuring that it
    /// remains accessible even after the `Guard` is dropped.
    ///
    /// This method may return `None` if the reference count is already 0 and
    /// the node has been queued for deletion.
    pub fn pin(&self) -> Option<RefEntry<'a, K, V>> {
        unsafe { RefEntry::try_acquire(self.parent, self.node) }
    }
}

impl<K, V> Entry<'_, '_, K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    /// Removes the entry from the skip list.
    ///
    /// Returns `true` if this call removed the entry and `false` if it was already removed.
    pub fn remove(&self) -> bool {
        // Try marking the tower.
        if self.node.mark_tower() {
            // Success - the entry is removed. Now decrement `len`.
            self.parent.hot_data.len.fetch_sub(1, Ordering::Relaxed);

            // Search for the key to unlink the node from the skip list.
            self.parent
                .search_bound(Bound::Included(&self.node.key), false, self.guard);

            true
        } else {
            false
        }
    }
}

impl<K, V> Clone for Entry<'_, '_, K, V> {
    fn clone(&self) -> Self {
        Self {
            parent: self.parent,
            node: self.node,
            guard: self.guard,
        }
    }
}

impl<K, V> fmt::Debug for Entry<'_, '_, K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Entry")
            .field(self.key())
            .field(self.value())
            .finish()
    }
}

impl<'a: 'g, 'g, K, V> Entry<'a, 'g, K, V>
where
    K: Ord,
{
    /// Moves to the next entry in the skip list.
    pub fn move_next(&mut self) -> bool {
        match self.next() {
            None => false,
            Some(n) => {
                *self = n;
                true
            }
        }
    }

    /// Returns the next entry in the skip list.
    pub fn next(&self) -> Option<Entry<'a, 'g, K, V>> {
        let n = self.parent.next_node(
            &self.node.tower,
            Bound::Excluded(&self.node.key),
            self.guard,
        )?;
        Some(Entry {
            parent: self.parent,
            node: n,
            guard: self.guard,
        })
    }

    /// Moves to the previous entry in the skip list.
    pub fn move_prev(&mut self) -> bool {
        match self.prev() {
            None => false,
            Some(n) => {
                *self = n;
                true
            }
        }
    }

    /// Returns the previous entry in the skip list.
    pub fn prev(&self) -> Option<Entry<'a, 'g, K, V>> {
        let n = self
            .parent
            .search_bound(Bound::Excluded(&self.node.key), true, self.guard)?;
        Some(Entry {
            parent: self.parent,
            node: n,
            guard: self.guard,
        })
    }
}

/// A reference-counted entry in a skip list.
///
/// You *must* call `release` to free this type, otherwise the node will be
/// leaked. This is because releasing the entry requires a `Guard`.
pub struct RefEntry<'a, K, V> {
    parent: &'a SkipList<K, V>,
    node: &'a Node<K, V>,
}

impl<'a, K: 'a, V: 'a> RefEntry<'a, K, V> {
    /// Returns `true` if the entry is removed from the skip list.
    pub fn is_removed(&self) -> bool {
        self.node.is_removed()
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        &self.node.key
    }

    /// Returns a reference to the value.
    pub fn value(&self) -> &V {
        &self.node.value
    }

    /// Returns a reference to the parent `SkipList`
    pub fn skiplist(&self) -> &'a SkipList<K, V> {
        self.parent
    }

    /// Releases the reference on the entry.
    pub fn release(self, guard: &Guard) {
        self.parent.check_guard(guard);
        unsafe { self.node.decrement(guard) }
    }

    /// Releases the reference of the entry, pinning the thread only when
    /// the reference count of the node becomes 0.
    pub fn release_with_pin<F>(self, pin: F)
    where
        F: FnOnce() -> Guard,
    {
        unsafe { self.node.decrement_with_pin(self.parent, pin) }
    }

    /// Tries to create a new `RefEntry` by incrementing the reference count of
    /// a node.
    unsafe fn try_acquire(
        parent: &'a SkipList<K, V>,
        node: &Node<K, V>,
    ) -> Option<RefEntry<'a, K, V>> {
        if unsafe { node.try_increment() } {
            Some(RefEntry {
                parent,

                // We re-bind the lifetime of the node here to that of the skip
                // list since we now hold a reference to it.
                node: unsafe { &*(node as *const _) },
            })
        } else {
            None
        }
    }
}

impl<K, V> RefEntry<'_, K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    /// Removes the entry from the skip list.
    ///
    /// Returns `true` if this call removed the entry and `false` if it was already removed.
    pub fn remove(&self, guard: &Guard) -> bool {
        self.parent.check_guard(guard);

        // Try marking the tower.
        if self.node.mark_tower() {
            // Success - the entry is removed. Now decrement `len`.
            self.parent.hot_data.len.fetch_sub(1, Ordering::Relaxed);

            // Search for the key to unlink the node from the skip list.
            self.parent
                .search_bound(Bound::Included(&self.node.key), false, guard);

            true
        } else {
            false
        }
    }
}

impl<K, V> Clone for RefEntry<'_, K, V> {
    fn clone(&self) -> Self {
        unsafe {
            // Incrementing will always succeed since we're already holding a reference to the node.
            Node::try_increment(self.node);
        }
        Self {
            parent: self.parent,
            node: self.node,
        }
    }
}

impl<K, V> fmt::Debug for RefEntry<'_, K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RefEntry")
            .field(self.key())
            .field(self.value())
            .finish()
    }
}

impl<'a, K, V> RefEntry<'a, K, V>
where
    K: Ord,
{
    /// Moves to the next entry in the skip list.
    pub fn move_next(&mut self, guard: &Guard) -> bool {
        match self.next(guard) {
            None => false,
            Some(e) => {
                mem::replace(self, e).release(guard);
                true
            }
        }
    }

    /// Returns the next entry in the skip list.
    pub fn next(&self, guard: &Guard) -> Option<RefEntry<'a, K, V>> {
        self.parent.check_guard(guard);
        unsafe {
            let mut n = self.node;
            loop {
                n = self
                    .parent
                    .next_node(&n.tower, Bound::Excluded(&n.key), guard)?;
                if let Some(e) = RefEntry::try_acquire(self.parent, n) {
                    return Some(e);
                }
            }
        }
    }

    /// Moves to the previous entry in the skip list.
    pub fn move_prev(&mut self, guard: &Guard) -> bool {
        match self.prev(guard) {
            None => false,
            Some(e) => {
                mem::replace(self, e).release(guard);
                true
            }
        }
    }

    /// Returns the previous entry in the skip list.
    pub fn prev(&self, guard: &Guard) -> Option<RefEntry<'a, K, V>> {
        self.parent.check_guard(guard);
        unsafe {
            let mut n = self.node;
            loop {
                n = self
                    .parent
                    .search_bound(Bound::Excluded(&n.key), true, guard)?;
                if let Some(e) = RefEntry::try_acquire(self.parent, n) {
                    return Some(e);
                }
            }
        }
    }
}

/// An iterator over the entries of a `SkipList`.
pub struct Iter<'a: 'g, 'g, K, V> {
    parent: &'a SkipList<K, V>,
    head: Option<&'g Node<K, V>>,
    tail: Option<&'g Node<K, V>>,
    guard: &'g Guard,
}

impl<'a: 'g, 'g, K: 'a, V: 'a> Iterator for Iter<'a, 'g, K, V>
where
    K: Ord,
{
    type Item = Entry<'a, 'g, K, V>;

    fn next(&mut self) -> Option<Entry<'a, 'g, K, V>> {
        self.head = match self.head {
            Some(n) => self
                .parent
                .next_node(&n.tower, Bound::Excluded(&n.key), self.guard),
            None => self
                .parent
                .next_node(&self.parent.head, Bound::Unbounded, self.guard),
        };
        if let (Some(h), Some(t)) = (self.head, self.tail) {
            if h.key >= t.key {
                self.head = None;
                self.tail = None;
            }
        }
        self.head.map(|n| Entry {
            parent: self.parent,
            node: n,
            guard: self.guard,
        })
    }
}

impl<'a: 'g, 'g, K: 'a, V: 'a> DoubleEndedIterator for Iter<'a, 'g, K, V>
where
    K: Ord,
{
    fn next_back(&mut self) -> Option<Entry<'a, 'g, K, V>> {
        self.tail = match self.tail {
            Some(n) => self
                .parent
                .search_bound(Bound::Excluded(&n.key), true, self.guard),
            None => self.parent.search_bound(Bound::Unbounded, true, self.guard),
        };
        if let (Some(h), Some(t)) = (self.head, self.tail) {
            if h.key >= t.key {
                self.head = None;
                self.tail = None;
            }
        }
        self.tail.map(|n| Entry {
            parent: self.parent,
            node: n,
            guard: self.guard,
        })
    }
}

impl<K, V> fmt::Debug for Iter<'_, '_, K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Iter")
            .field("head", &self.head.map(|n| (&n.key, &n.value)))
            .field("tail", &self.tail.map(|n| (&n.key, &n.value)))
            .finish()
    }
}

/// An iterator over reference-counted entries of a `SkipList`.
pub struct RefIter<'a, K, V> {
    parent: &'a SkipList<K, V>,
    head: Option<RefEntry<'a, K, V>>,
    tail: Option<RefEntry<'a, K, V>>,
}

impl<K, V> fmt::Debug for RefIter<'_, K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("RefIter");
        match &self.head {
            None => d.field("head", &None::<(&K, &V)>),
            Some(e) => d.field("head", &(e.key(), e.value())),
        };
        match &self.tail {
            None => d.field("tail", &None::<(&K, &V)>),
            Some(e) => d.field("tail", &(e.key(), e.value())),
        };
        d.finish()
    }
}

impl<'a, K: 'a, V: 'a> RefIter<'a, K, V>
where
    K: Ord,
{
    /// Advances the iterator and returns the next value.
    pub fn next(&mut self, guard: &Guard) -> Option<RefEntry<'a, K, V>> {
        self.parent.check_guard(guard);
        let next_head = match &self.head {
            Some(e) => e.next(guard),
            None => try_pin_loop(|| self.parent.front(guard)),
        };
        match (&next_head, &self.tail) {
            // The next key is larger than the latest tail key we observed with this iterator.
            (Some(ref next), Some(t)) if next.key() >= t.key() => {
                unsafe {
                    next.node.decrement(guard);
                }
                None
            }
            (Some(_), _) => {
                if let Some(e) = mem::replace(&mut self.head, next_head.clone()) {
                    unsafe {
                        e.node.decrement(guard);
                    }
                }
                next_head
            }
            (None, _) => None,
        }
    }

    /// Removes and returns an element from the end of the iterator.
    pub fn next_back(&mut self, guard: &Guard) -> Option<RefEntry<'a, K, V>> {
        self.parent.check_guard(guard);
        let next_tail = match &self.tail {
            Some(e) => e.prev(guard),
            None => try_pin_loop(|| self.parent.back(guard)),
        };
        match (&self.head, &next_tail) {
            // The prev key is smaller than the latest head key we observed with this iterator.
            (Some(h), Some(next)) if h.key() >= next.key() => {
                unsafe {
                    next.node.decrement(guard);
                }
                None
            }
            (_, Some(_)) => {
                if let Some(e) = mem::replace(&mut self.tail, next_tail.clone()) {
                    unsafe {
                        e.node.decrement(guard);
                    }
                }
                next_tail
            }
            (_, None) => None,
        }
    }
}

impl<'a, K: 'a, V: 'a> RefIter<'a, K, V> {
    /// Decrements the reference count of `RefEntry` owned by the iterator.
    pub fn drop_impl(&mut self, guard: &Guard) {
        self.parent.check_guard(guard);
        if let Some(e) = self.head.take() {
            unsafe { e.node.decrement(guard) };
        }
        if let Some(e) = self.tail.take() {
            unsafe { e.node.decrement(guard) };
        }
    }
}

/// An iterator over a subset of entries of a `SkipList`.
pub struct Range<'a: 'g, 'g, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    parent: &'a SkipList<K, V>,
    head: Option<&'g Node<K, V>>,
    tail: Option<&'g Node<K, V>>,
    range: R,
    guard: &'g Guard,
    _marker: PhantomData<fn() -> Q>, // covariant over `Q`
}

impl<'a: 'g, 'g, Q, R, K: 'a, V: 'a> Iterator for Range<'a, 'g, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Entry<'a, 'g, K, V>;

    fn next(&mut self) -> Option<Entry<'a, 'g, K, V>> {
        self.head = match self.head {
            Some(n) => self
                .parent
                .next_node(&n.tower, Bound::Excluded(&n.key), self.guard),
            None => self
                .parent
                .search_bound(self.range.start_bound(), false, self.guard),
        };
        if let Some(h) = self.head {
            let bound = match self.tail {
                Some(t) => Bound::Excluded(t.key.borrow()),
                None => self.range.end_bound(),
            };
            if !below_upper_bound(&bound, h.key.borrow()) {
                self.head = None;
                self.tail = None;
            }
        }
        self.head.map(|n| Entry {
            parent: self.parent,
            node: n,
            guard: self.guard,
        })
    }
}

impl<'a: 'g, 'g, Q, R, K: 'a, V: 'a> DoubleEndedIterator for Range<'a, 'g, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn next_back(&mut self) -> Option<Entry<'a, 'g, K, V>> {
        self.tail = match self.tail {
            Some(n) => self
                .parent
                .search_bound(Bound::Excluded(n.key.borrow()), true, self.guard),
            None => self
                .parent
                .search_bound(self.range.end_bound(), true, self.guard),
        };
        if let Some(t) = self.tail {
            let bound = match self.head {
                Some(h) => Bound::Excluded(h.key.borrow()),
                None => self.range.start_bound(),
            };
            if !above_lower_bound(&bound, t.key.borrow()) {
                self.head = None;
                self.tail = None;
            }
        }
        self.tail.map(|n| Entry {
            parent: self.parent,
            node: n,
            guard: self.guard,
        })
    }
}

impl<Q, R, K, V> fmt::Debug for Range<'_, '_, Q, R, K, V>
where
    K: Ord + Borrow<Q> + fmt::Debug,
    V: fmt::Debug,
    R: RangeBounds<Q> + fmt::Debug,
    Q: Ord + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Range")
            .field("range", &self.range)
            .field("head", &self.head)
            .field("tail", &self.tail)
            .finish()
    }
}

/// An iterator over reference-counted subset of entries of a `SkipList`.
pub struct RefRange<'a, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    parent: &'a SkipList<K, V>,
    pub(crate) head: Option<RefEntry<'a, K, V>>,
    pub(crate) tail: Option<RefEntry<'a, K, V>>,
    pub(crate) range: R,
    _marker: PhantomData<fn() -> Q>, // covariant over `Q`
}

unsafe impl<Q, R, K, V> Send for RefRange<'_, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
}

unsafe impl<Q, R, K, V> Sync for RefRange<'_, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
}

impl<Q, R, K, V> fmt::Debug for RefRange<'_, Q, R, K, V>
where
    K: Ord + Borrow<Q> + fmt::Debug,
    V: fmt::Debug,
    R: RangeBounds<Q> + fmt::Debug,
    Q: Ord + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefRange")
            .field("range", &self.range)
            .field("head", &self.head)
            .field("tail", &self.tail)
            .finish()
    }
}

impl<'a, Q, R, K: 'a, V: 'a> RefRange<'a, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    /// Advances the iterator and returns the next value.
    pub fn next(&mut self, guard: &Guard) -> Option<RefEntry<'a, K, V>> {
        self.parent.check_guard(guard);
        let next_head = match self.head {
            Some(ref e) => e.next(guard),
            None => try_pin_loop(|| self.parent.lower_bound(self.range.start_bound(), guard)),
        };

        if let Some(ref h) = next_head {
            let bound = match self.tail {
                Some(ref t) => Bound::Excluded(t.key().borrow()),
                None => self.range.end_bound(),
            };
            if below_upper_bound(&bound, h.key().borrow()) {
                self.head = next_head.clone();
                next_head
            } else {
                unsafe {
                    h.node.decrement(guard);
                }
                None
            }
        } else {
            None
        }
    }

    /// Removes and returns an element from the end of the iterator.
    pub fn next_back(&mut self, guard: &Guard) -> Option<RefEntry<'a, K, V>> {
        self.parent.check_guard(guard);
        let next_tail = match self.tail {
            Some(ref e) => e.prev(guard),
            None => try_pin_loop(|| self.parent.upper_bound(self.range.end_bound(), guard)),
        };

        if let Some(ref t) = next_tail {
            let bound = match self.head {
                Some(ref h) => Bound::Excluded(h.key().borrow()),
                None => self.range.start_bound(),
            };
            if above_lower_bound(&bound, t.key().borrow()) {
                self.tail = next_tail.clone();
                next_tail
            } else {
                unsafe {
                    t.node.decrement(guard);
                }
                None
            }
        } else {
            None
        }
    }

    /// Decrements a reference count owned by this iterator.
    pub fn drop_impl(&mut self, guard: &Guard) {
        self.parent.check_guard(guard);
        if let Some(e) = self.head.take() {
            unsafe { e.node.decrement(guard) };
        }
        if let Some(e) = self.tail.take() {
            unsafe { e.node.decrement(guard) };
        }
    }
}

/// An owning iterator over the entries of a `SkipList`.
pub struct IntoIter<K, V> {
    /// The current node.
    ///
    /// All preceding nods have already been destroyed.
    node: *mut Node<K, V>,
}

impl<K, V> Drop for IntoIter<K, V> {
    fn drop(&mut self) {
        // Iterate through the whole chain and destroy every node.
        while !self.node.is_null() {
            unsafe {
                // Unprotected loads are okay because this function is the only one currently using
                // the skip list.
                let next = (&(*self.node).tower)[0].load(Ordering::Relaxed, epoch::unprotected());

                // We can safely do this without deferring because references to
                // keys & values that we give out never outlive the SkipList.
                Node::finalize(self.node);

                self.node = next.as_raw() as *mut Node<K, V>;
            }
        }
    }
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        loop {
            // Have we reached the end of the skip list?
            if self.node.is_null() {
                return None;
            }

            unsafe {
                // Take the key and value out of the node.
                let key = ptr::read(&(*self.node).key);
                let value = ptr::read(&(*self.node).value);

                // Get the next node in the skip list.
                //
                // Unprotected loads are okay because this function is the only one currently using
                // the skip list.
                let next = (&(*self.node).tower)[0].load(Ordering::Relaxed, epoch::unprotected());

                // Deallocate the current node and move to the next one.
                Node::dealloc(self.node);
                self.node = next.as_raw() as *mut Node<K, V>;

                // The current node may be marked. If it is, it's been removed from the skip list
                // and we should just skip it.
                if next.tag() == 0 {
                    return Some((key, value));
                }
            }
        }
    }
}

impl<K, V> fmt::Debug for IntoIter<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("IntoIter { .. }")
    }
}

/// Helper function to retry an operation until pinning succeeds or `None` is
/// returned.
pub(crate) fn try_pin_loop<'a: 'g, 'g, F, K, V>(mut f: F) -> Option<RefEntry<'a, K, V>>
where
    F: FnMut() -> Option<Entry<'a, 'g, K, V>>,
{
    loop {
        if let Some(e) = f()?.pin() {
            return Some(e);
        }
    }
}

/// Helper function to check if a value is above a lower bound
fn above_lower_bound<T: Ord + ?Sized>(bound: &Bound<&T>, other: &T) -> bool {
    match *bound {
        Bound::Unbounded => true,
        Bound::Included(key) => other >= key,
        Bound::Excluded(key) => other > key,
    }
}

/// Helper function to check if a value is below an upper bound
fn below_upper_bound<T: Ord + ?Sized>(bound: &Bound<&T>, other: &T) -> bool {
    match *bound {
        Bound::Unbounded => true,
        Bound::Included(key) => other <= key,
        Bound::Excluded(key) => other < key,
    }
}
