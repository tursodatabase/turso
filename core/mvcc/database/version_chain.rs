use crate::mvcc::database::{Row, RowVersion, TxTimestampOrID};
use crate::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::fmt;
use std::marker::PhantomData;
use std::ptr;

/// Bit flag to distinguish TxID from Timestamp in the packed u64 encoding.
///
/// Encoding:
///   0                  = None
///   1..2^63-1          = Timestamp(value)
///   2^63 | lower bits  = TxID(lower 63 bits)
const TX_ID_BIT: u64 = 0x8000_0000_0000_0000;

/// Encode `Option<TxTimestampOrID>` into a u64 for atomic storage.
#[inline]
pub fn encode_ts(ts: &Option<TxTimestampOrID>) -> u64 {
    match ts {
        None => 0,
        Some(TxTimestampOrID::Timestamp(t)) => {
            debug_assert!(*t != 0, "Timestamp 0 is reserved for None encoding");
            debug_assert!(
                *t & TX_ID_BIT == 0,
                "Timestamp value exceeds 63-bit capacity"
            );
            *t
        }
        Some(TxTimestampOrID::TxID(id)) => {
            debug_assert!(*id & TX_ID_BIT == 0, "TxID value exceeds 63-bit capacity");
            *id | TX_ID_BIT
        }
    }
}

/// Decode a u64 into `Option<TxTimestampOrID>`.
#[inline]
pub fn decode_ts(v: u64) -> Option<TxTimestampOrID> {
    if v == 0 {
        None
    } else if v & TX_ID_BIT != 0 {
        Some(TxTimestampOrID::TxID(v & !TX_ID_BIT))
    } else {
        Some(TxTimestampOrID::Timestamp(v))
    }
}

/// A node in the lock-free row version chain.
///
/// Fields `id`, `row`, and `btree_resident` are immutable after construction.
/// Fields `begin` and `end` are atomic and can be updated concurrently.
/// Field `next` is set once during prepend and never modified after linking.
pub struct RowVersionNode {
    pub id: u64,
    begin: AtomicU64,
    end: AtomicU64,
    pub row: Row,
    pub btree_resident: bool,
    next: AtomicPtr<RowVersionNode>,
}

// Safety: All mutable state is behind atomics. The `row` field is immutable after
// construction. Nodes are only freed under exclusive access (checkpoint lock) or
// when the chain is dropped (single owner).
unsafe impl Send for RowVersionNode {}
unsafe impl Sync for RowVersionNode {}

impl RowVersionNode {
    pub fn new(
        id: u64,
        begin: Option<TxTimestampOrID>,
        end: Option<TxTimestampOrID>,
        row: Row,
        btree_resident: bool,
    ) -> Self {
        Self {
            id,
            begin: AtomicU64::new(encode_ts(&begin)),
            end: AtomicU64::new(encode_ts(&end)),
            row,
            btree_resident,
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }

    pub fn from_row_version(rv: RowVersion) -> Self {
        Self::new(rv.id, rv.begin, rv.end, rv.row, rv.btree_resident)
    }

    /// Snapshot the node into a `RowVersion` (non-atomic copy).
    pub fn to_row_version(&self) -> RowVersion {
        RowVersion {
            id: self.id,
            begin: self.begin(),
            end: self.end(),
            row: self.row.clone(),
            btree_resident: self.btree_resident,
        }
    }

    #[inline]
    pub fn begin(&self) -> Option<TxTimestampOrID> {
        decode_ts(self.begin.load(Ordering::Acquire))
    }

    #[inline]
    pub fn end(&self) -> Option<TxTimestampOrID> {
        decode_ts(self.end.load(Ordering::Acquire))
    }

    #[inline]
    pub fn set_begin(&self, ts: Option<TxTimestampOrID>) {
        self.begin.store(encode_ts(&ts), Ordering::Release);
    }

    #[inline]
    pub fn set_end(&self, ts: Option<TxTimestampOrID>) {
        self.end.store(encode_ts(&ts), Ordering::Release);
    }

    #[inline]
    pub fn begin_raw(&self) -> u64 {
        self.begin.load(Ordering::Acquire)
    }

    #[inline]
    pub fn end_raw(&self) -> u64 {
        self.end.load(Ordering::Acquire)
    }

    /// Check if this node is garbage (begin=None, end=None).
    #[inline]
    pub fn is_garbage(&self) -> bool {
        self.begin_raw() == 0 && self.end_raw() == 0
    }

    /// Mark this node as garbage by clearing begin and end.
    #[inline]
    pub fn mark_garbage(&self) {
        self.begin.store(0, Ordering::Release);
        self.end.store(0, Ordering::Release);
    }
}

impl fmt::Debug for RowVersionNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowVersionNode")
            .field("id", &self.id)
            .field("begin", &self.begin())
            .field("end", &self.end())
            .field("row", &self.row)
            .field("btree_resident", &self.btree_resident)
            .finish()
    }
}

/// A lock-free singly-linked list of row versions, ordered newest-first.
///
/// - **Reads are wait-free**: traverse via immutable `next` pointers + atomic begin/end loads.
/// - **Inserts are lock-free**: prepend new nodes using CAS on the head pointer.
/// - **begin/end mutations are wait-free**: atomic stores (used by commit, delete, rollback).
/// - **GC marking is wait-free**: set begin=0, end=0 atomically.
/// - **GC compaction requires exclusive access**: rebuild the chain under checkpoint lock.
pub struct RowVersionChain {
    head: AtomicPtr<RowVersionNode>,
}

// Safety: head is atomic, nodes are Send+Sync.
unsafe impl Send for RowVersionChain {}
unsafe impl Sync for RowVersionChain {}

impl RowVersionChain {
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Prepend a new node to the head of the chain. Lock-free via CAS.
    /// The new node becomes the newest version in the chain.
    pub fn prepend(&self, node: Box<RowVersionNode>) {
        let new_ptr = Box::into_raw(node);
        loop {
            let head = self.head.load(Ordering::Acquire);
            // Safety: new_ptr is valid and not yet visible to other threads.
            unsafe {
                (*new_ptr).next.store(head, Ordering::Relaxed);
            }
            if self
                .head
                .compare_exchange_weak(head, new_ptr, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Insert a node in sorted position (by begin timestamp, newest first).
    /// Only safe to call when no concurrent readers/writers exist (e.g., during recovery).
    ///
    /// `get_begin_timestamp` resolves TxID to a comparable timestamp.
    pub fn insert_sorted<F>(&self, node: Box<RowVersionNode>, get_begin_ts: F)
    where
        F: Fn(&Option<TxTimestampOrID>) -> u64,
    {
        let new_begin = get_begin_ts(&node.begin());
        let new_ptr = Box::into_raw(node);

        let head = self.head.load(Ordering::Acquire);
        if head.is_null() {
            self.head.store(new_ptr, Ordering::Release);
            return;
        }

        // Check if we should insert at head (newest)
        let head_begin = get_begin_ts(&(unsafe { &*head }).begin());
        if new_begin > head_begin {
            unsafe {
                (*new_ptr).next.store(head, Ordering::Relaxed);
            }
            self.head.store(new_ptr, Ordering::Release);
            return;
        }

        // Handle duplicate begin timestamps: if head has same begin and same row id, replace it.
        if head_begin == new_begin {
            let head_ref = unsafe { &*head };
            let new_ref = unsafe { &*new_ptr };
            if head_ref.row.id == new_ref.row.id
                && head_ref.begin().is_some()
                && head_ref.begin() == new_ref.begin()
            {
                // Replace: new node takes head's next, becomes new head, old head is freed
                let old_next = head_ref.next.load(Ordering::Relaxed);
                unsafe {
                    (*new_ptr).next.store(old_next, Ordering::Relaxed);
                }
                self.head.store(new_ptr, Ordering::Release);
                // Free old head
                unsafe {
                    drop(Box::from_raw(head));
                }
                return;
            }
        }

        // Walk to find insertion point
        let mut prev = head;
        loop {
            let prev_ref = unsafe { &*prev };
            let next = prev_ref.next.load(Ordering::Relaxed);

            if next.is_null() {
                // Insert at end
                unsafe {
                    (*new_ptr).next.store(ptr::null_mut(), Ordering::Relaxed);
                }
                prev_ref.next.store(new_ptr, Ordering::Relaxed);
                return;
            }

            let next_ref = unsafe { &*next };
            let next_begin = get_begin_ts(&next_ref.begin());
            if new_begin >= next_begin {
                // Check for duplicate replacement
                if new_begin == next_begin
                    && next_ref.row.id == (unsafe { &*new_ptr }).row.id
                    && next_ref.begin().is_some()
                    && next_ref.begin() == (unsafe { &*new_ptr }).begin()
                {
                    // Replace next node
                    let old_next_next = next_ref.next.load(Ordering::Relaxed);
                    unsafe {
                        (*new_ptr).next.store(old_next_next, Ordering::Relaxed);
                    }
                    prev_ref.next.store(new_ptr, Ordering::Relaxed);
                    unsafe {
                        drop(Box::from_raw(next));
                    }
                    return;
                }
                // Insert between prev and next
                unsafe {
                    (*new_ptr).next.store(next, Ordering::Relaxed);
                }
                prev_ref.next.store(new_ptr, Ordering::Relaxed);
                return;
            }

            prev = next;
        }
    }

    /// Iterate over all nodes in the chain (newest first). Wait-free.
    pub fn iter(&self) -> RowVersionChainIter<'_> {
        RowVersionChainIter {
            current: self.head.load(Ordering::Acquire),
            _marker: PhantomData,
        }
    }

    /// Check if the chain has no nodes.
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire).is_null()
    }

    /// Count all nodes in the chain (including garbage).
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Count non-garbage nodes.
    pub fn live_count(&self) -> usize {
        self.iter().filter(|n| !n.is_garbage()).count()
    }

    /// Apply GC rules to mark versions as garbage. Safe to call concurrently.
    ///
    /// Rule 1: Aborted garbage (begin=None, end=None) — already invisible, nothing to do.
    /// Rule 2: Superseded (end=Timestamp(e), e <= lwm) — mark as garbage unless it's a
    ///         tombstone whose deletion hasn't been checkpointed.
    /// Rule 3: Current checkpointed sole-survivor — mark as garbage.
    ///
    /// Returns the number of versions newly marked as garbage.
    pub fn gc_mark(&self, lwm: u64, ckpt_max: u64) -> usize {
        let mut dropped = 0;

        // Check if any non-garbage version is a committed current version
        let has_current = self.iter().any(|node| {
            !node.is_garbage()
                && node.end().is_none()
                && matches!(node.begin(), Some(TxTimestampOrID::Timestamp(_)))
        });

        // Rule 2: superseded versions below LWM
        for node in self.iter() {
            if node.is_garbage() {
                continue;
            }
            if let Some(TxTimestampOrID::Timestamp(e)) = node.end() {
                if e <= lwm {
                    let should_retain = !has_current && e > ckpt_max;
                    if !should_retain {
                        node.mark_garbage();
                        dropped += 1;
                    }
                }
            }
        }

        // Rule 3: checkpointed sole-survivor
        let mut sole_candidate: Option<&RowVersionNode> = None;
        let mut live_count = 0;
        for node in self.iter() {
            if !node.is_garbage() {
                live_count += 1;
                sole_candidate = Some(node);
                if live_count > 1 {
                    break;
                }
            }
        }
        if live_count == 1 {
            if let Some(node) = sole_candidate {
                if let (Some(TxTimestampOrID::Timestamp(b)), None) = (node.begin(), node.end()) {
                    if b <= ckpt_max && b < lwm {
                        node.mark_garbage();
                        dropped += 1;
                    }
                }
            }
        }

        dropped
    }

    /// Physically remove garbage nodes and rebuild the chain.
    ///
    /// # Safety
    /// Must only be called when no concurrent readers/writers exist
    /// (e.g., under the blocking checkpoint lock).
    pub fn gc_compact(&self) -> usize {
        let mut keep: Vec<*mut RowVersionNode> = Vec::new();
        let mut garbage: Vec<*mut RowVersionNode> = Vec::new();

        let mut ptr = self.head.load(Ordering::Relaxed);
        while !ptr.is_null() {
            let node = unsafe { &*ptr };
            let next = node.next.load(Ordering::Relaxed);
            if node.is_garbage() {
                garbage.push(ptr);
            } else {
                keep.push(ptr);
            }
            ptr = next;
        }

        let removed = garbage.len();

        // Rebuild chain from kept nodes (preserve newest-first order)
        if keep.is_empty() {
            self.head.store(ptr::null_mut(), Ordering::Relaxed);
        } else {
            self.head.store(keep[0], Ordering::Relaxed);
            for i in 0..keep.len() - 1 {
                unsafe {
                    (*keep[i]).next.store(keep[i + 1], Ordering::Relaxed);
                }
            }
            unsafe {
                (*keep[keep.len() - 1])
                    .next
                    .store(ptr::null_mut(), Ordering::Relaxed);
            }
        }

        // Free garbage nodes
        for ptr in garbage {
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }

        removed
    }
}

impl Default for RowVersionChain {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for RowVersionChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl Drop for RowVersionChain {
    fn drop(&mut self) {
        let mut ptr = *self.head.get_mut();
        while !ptr.is_null() {
            let mut node = unsafe { Box::from_raw(ptr) };
            ptr = *node.next.get_mut();
        }
    }
}

/// Iterator over nodes in a `RowVersionChain` (newest first). Wait-free.
pub struct RowVersionChainIter<'a> {
    current: *const RowVersionNode,
    _marker: PhantomData<&'a RowVersionNode>,
}

impl<'a> Iterator for RowVersionChainIter<'a> {
    type Item = &'a RowVersionNode;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            return None;
        }
        let node = unsafe { &*self.current };
        self.current = node.next.load(Ordering::Acquire);
        Some(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::database::{RowID, RowKey};

    fn make_row(table_id: i64, row_id: i64) -> Row {
        Row::new_table_row(
            RowID::new(table_id.into(), RowKey::Int(row_id)),
            Vec::new(),
            0,
        )
    }

    fn make_node(
        id: u64,
        begin: Option<TxTimestampOrID>,
        end: Option<TxTimestampOrID>,
    ) -> Box<RowVersionNode> {
        Box::new(RowVersionNode::new(id, begin, end, make_row(-1, 1), false))
    }

    #[test]
    fn test_encode_decode_none() {
        assert_eq!(encode_ts(&None), 0);
        assert_eq!(decode_ts(0), None);
    }

    #[test]
    fn test_encode_decode_timestamp() {
        let ts = Some(TxTimestampOrID::Timestamp(42));
        let encoded = encode_ts(&ts);
        assert_eq!(encoded, 42);
        assert_eq!(decode_ts(encoded), ts);
    }

    #[test]
    fn test_encode_decode_txid() {
        let ts = Some(TxTimestampOrID::TxID(42));
        let encoded = encode_ts(&ts);
        assert_eq!(encoded, 42 | TX_ID_BIT);
        assert_eq!(decode_ts(encoded), ts);
    }

    #[test]
    fn test_chain_empty() {
        let chain = RowVersionChain::new();
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);
        assert!(chain.iter().next().is_none());
    }

    #[test]
    fn test_chain_prepend_single() {
        let chain = RowVersionChain::new();
        chain.prepend(make_node(1, Some(TxTimestampOrID::Timestamp(10)), None));
        assert!(!chain.is_empty());
        assert_eq!(chain.len(), 1);

        let node = chain.iter().next().unwrap();
        assert_eq!(node.id, 1);
        assert_eq!(node.begin(), Some(TxTimestampOrID::Timestamp(10)));
        assert_eq!(node.end(), None);
    }

    #[test]
    fn test_chain_prepend_order() {
        let chain = RowVersionChain::new();
        chain.prepend(make_node(1, Some(TxTimestampOrID::Timestamp(10)), None));
        chain.prepend(make_node(2, Some(TxTimestampOrID::Timestamp(20)), None));
        chain.prepend(make_node(3, Some(TxTimestampOrID::Timestamp(30)), None));

        let ids: Vec<u64> = chain.iter().map(|n| n.id).collect();
        assert_eq!(ids, vec![3, 2, 1]);
    }

    #[test]
    fn test_atomic_begin_end_mutation() {
        let chain = RowVersionChain::new();
        chain.prepend(make_node(1, Some(TxTimestampOrID::TxID(5)), None));

        let node = chain.iter().next().unwrap();
        assert_eq!(node.begin(), Some(TxTimestampOrID::TxID(5)));
        assert_eq!(node.end(), None);

        // Simulate commit: update begin from TxID to Timestamp
        node.set_begin(Some(TxTimestampOrID::Timestamp(100)));
        assert_eq!(node.begin(), Some(TxTimestampOrID::Timestamp(100)));

        // Simulate soft delete: set end
        node.set_end(Some(TxTimestampOrID::TxID(7)));
        assert_eq!(node.end(), Some(TxTimestampOrID::TxID(7)));
    }

    #[test]
    fn test_gc_mark_aborted_garbage() {
        let chain = RowVersionChain::new();
        // Aborted garbage: begin=None, end=None — already invisible, gc_mark doesn't need to mark
        chain.prepend(make_node(1, None, None));
        chain.prepend(make_node(2, Some(TxTimestampOrID::Timestamp(10)), None));

        // gc_mark doesn't re-mark already-garbage nodes, and can't mark the live one
        let dropped = chain.gc_mark(5, 5);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn test_gc_mark_superseded() {
        let chain = RowVersionChain::new();
        // Old version superseded
        chain.prepend(make_node(
            1,
            Some(TxTimestampOrID::Timestamp(5)),
            Some(TxTimestampOrID::Timestamp(10)),
        ));
        // Current version
        chain.prepend(make_node(2, Some(TxTimestampOrID::Timestamp(10)), None));

        // lwm=15 means all versions with end <= 15 are invisible to all readers
        let dropped = chain.gc_mark(15, 15);
        assert_eq!(dropped, 1);
        assert!(chain.iter().find(|n| n.id == 1).unwrap().is_garbage());
        assert!(!chain.iter().find(|n| n.id == 2).unwrap().is_garbage());
    }

    #[test]
    fn test_gc_mark_sole_survivor() {
        let chain = RowVersionChain::new();
        chain.prepend(make_node(1, Some(TxTimestampOrID::Timestamp(5)), None));

        // b <= ckpt_max && b < lwm: version has been checkpointed and no reader needs it
        let dropped = chain.gc_mark(10, 10);
        assert_eq!(dropped, 1);
        assert!(chain.iter().next().unwrap().is_garbage());
    }

    #[test]
    fn test_gc_compact() {
        let chain = RowVersionChain::new();
        chain.prepend(make_node(1, None, None)); // garbage
        chain.prepend(make_node(2, Some(TxTimestampOrID::Timestamp(10)), None)); // live
        chain.prepend(make_node(3, None, None)); // garbage

        assert_eq!(chain.len(), 3);
        let removed = chain.gc_compact();
        assert_eq!(removed, 2);
        assert_eq!(chain.len(), 1);
        assert_eq!(chain.iter().next().unwrap().id, 2);
    }

    #[test]
    fn test_to_row_version() {
        let chain = RowVersionChain::new();
        chain.prepend(make_node(
            1,
            Some(TxTimestampOrID::Timestamp(10)),
            Some(TxTimestampOrID::Timestamp(20)),
        ));

        let node = chain.iter().next().unwrap();
        let rv = node.to_row_version();
        assert_eq!(rv.id, 1);
        assert_eq!(rv.begin, Some(TxTimestampOrID::Timestamp(10)));
        assert_eq!(rv.end, Some(TxTimestampOrID::Timestamp(20)));
    }

    #[test]
    fn test_chain_drop_frees_all_nodes() {
        // This test verifies that Drop doesn't leak. Miri would catch leaks.
        let chain = RowVersionChain::new();
        for i in 0..100 {
            chain.prepend(make_node(i, Some(TxTimestampOrID::Timestamp(i)), None));
        }
        drop(chain);
    }
}
