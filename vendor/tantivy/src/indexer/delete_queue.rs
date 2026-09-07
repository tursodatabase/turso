use std::ops::DerefMut;
use std::sync::{Arc, RwLock, Weak};

use super::operation::DeleteOperation;
use crate::Opstamp;

/// The DeleteQueue is similar in conceptually to a multiple
/// consumer single producer broadcast channel.
///
/// All consumer will receive all messages.
///
/// Consumer of the delete queue are holding a `DeleteCursor`,
/// which points to a specific place of the `DeleteQueue`.
///
/// New consumer can be created in two ways
/// - calling `delete_queue.cursor()` returns a cursor, that will include all future delete
///   operation (and some or none of the past operations... The client is in charge of checking the
///   opstamps.).
/// - cloning an existing cursor returns a new cursor, that is at the exact same position, and can
///   now advance independently from the original cursor.
#[derive(Default)]
struct InnerDeleteQueue {
    writer: Vec<DeleteOperation>,
    last_block: Weak<Block>,
}

/// The delete queue is a linked list storing delete operations.
///
/// Several consumers can hold a reference to it. Delete operations
/// get dropped/gc'ed when no more consumers are holding a reference
/// to them.
#[derive(Clone, Default)]
pub struct DeleteQueue {
    inner: Arc<RwLock<InnerDeleteQueue>>,
}

impl DeleteQueue {
    fn get_last_block(&self) -> Arc<Block> {
        {
            // try get the last block with simply acquiring the read lock.
            let rlock = self.inner.read().unwrap();
            if let Some(block) = rlock.last_block.upgrade() {
                return block;
            }
        }
        // It failed. Let's double check after acquiring the write, as someone could have called
        // `get_last_block` right after we released the rlock.
        let mut wlock = self.inner.write().unwrap();
        if let Some(block) = wlock.last_block.upgrade() {
            return block;
        }
        let block = Arc::new(Block {
            operations: Arc::new([]),
            next: NextBlock::from(self.clone()),
        });
        wlock.last_block = Arc::downgrade(&block);
        block
    }

    /// Creates a new cursor that makes it possible to
    /// consume future delete operations.
    ///
    /// Past delete operations are not accessible.
    pub fn cursor(&self) -> DeleteCursor {
        let last_block = self.get_last_block();
        let operations_len = last_block.operations.len();
        DeleteCursor {
            block: last_block,
            pos: operations_len,
        }
    }

    /// Appends a new delete operations.
    pub fn push(&self, delete_operation: DeleteOperation) {
        self.inner
            .write()
            .expect("Failed to acquire write lock on delete queue writer")
            .writer
            .push(delete_operation);
    }

    // DeleteQueue is a linked list of blocks of
    // delete operations.
    //
    // Writing happens by simply appending to a vec.
    // `.flush()` takes this pending delete operations vec
    // creates a new read-only block from it,
    // and appends it to the linked list.
    //
    // `.flush()` happens when, for instance,
    // a consumer reaches the last read-only operations.
    // It then ask the delete queue if there happen to
    // be some unflushed operations.
    //
    fn flush(&self) -> Option<Arc<Block>> {
        let mut self_wlock = self
            .inner
            .write()
            .expect("Failed to acquire write lock on delete queue writer");

        if self_wlock.writer.is_empty() {
            return None;
        }

        let delete_operations = std::mem::take(&mut self_wlock.writer);

        let new_block = Arc::new(Block {
            operations: Arc::from(delete_operations.into_boxed_slice()),
            next: NextBlock::from(self.clone()),
        });

        self_wlock.last_block = Arc::downgrade(&new_block);
        Some(new_block)
    }
}

enum InnerNextBlock {
    Writer(DeleteQueue),
    Closed(Arc<Block>),
}

struct NextBlock(RwLock<InnerNextBlock>);

impl From<DeleteQueue> for NextBlock {
    fn from(delete_queue: DeleteQueue) -> NextBlock {
        NextBlock(RwLock::new(InnerNextBlock::Writer(delete_queue)))
    }
}

impl NextBlock {
    fn next_block(&self) -> Option<Arc<Block>> {
        {
            let next_read_lock = self
                .0
                .read()
                .expect("Failed to acquire write lock in delete queue");
            if let InnerNextBlock::Closed(ref block) = *next_read_lock {
                return Some(Arc::clone(block));
            }
        }
        let next_block;
        {
            let mut next_write_lock = self
                .0
                .write()
                .expect("Failed to acquire write lock in delete queue");
            match *next_write_lock {
                InnerNextBlock::Closed(ref block) => {
                    return Some(Arc::clone(block));
                }
                InnerNextBlock::Writer(ref writer) => match writer.flush() {
                    Some(flushed_next_block) => {
                        next_block = flushed_next_block;
                    }
                    None => {
                        return None;
                    }
                },
            }
            *next_write_lock.deref_mut() = InnerNextBlock::Closed(Arc::clone(&next_block));
            Some(next_block)
        }
    }
}

struct Block {
    operations: Arc<[DeleteOperation]>,
    next: NextBlock,
}

/// As we process delete operations, keeps track of our position.
#[derive(Clone)]
pub struct DeleteCursor {
    block: Arc<Block>,
    pos: usize,
}

impl DeleteCursor {
    /// Skips operations and position it so that
    /// - either all of the delete operation currently in the queue are consume and the next get
    ///   will return `None`.
    /// - the next get will return the first operation with an `opstamp >= target_opstamp`.
    pub fn skip_to(&mut self, target_opstamp: Opstamp) {
        // TODO Can be optimize as we work with block.
        while self.is_behind_opstamp(target_opstamp) {
            self.advance();
        }
    }

    fn is_behind_opstamp(&mut self, target_opstamp: Opstamp) -> bool {
        self.get()
            .map(|operation| operation.opstamp < target_opstamp)
            .unwrap_or(false)
    }

    /// If the current block has been entirely
    /// consumed, try to load the next one.
    ///
    /// Return `true`, if after this attempt,
    /// the cursor is on a block that has not
    /// been entirely consumed.
    /// Return `false`, if we have reached the end of the queue.
    fn load_block_if_required(&mut self) -> bool {
        if self.pos >= self.block.operations.len() {
            // we have consumed our operations entirely.
            // let's ask our writer if he has more for us.
            // self.go_next_block();
            match self.block.next.next_block() {
                Some(block) => {
                    self.block = block;
                    self.pos = 0;
                    true
                }
                None => false,
            }
        } else {
            true
        }
    }

    /// Advance to the next delete operation.
    /// Returns true if and only if there is such an operation.
    pub fn advance(&mut self) -> bool {
        if self.load_block_if_required() {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Get the current delete operation.
    /// Calling `.get` does not advance the cursor.
    pub fn get(&mut self) -> Option<&DeleteOperation> {
        if self.load_block_if_required() {
            Some(&self.block.operations[self.pos])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{DeleteOperation, DeleteQueue};
    use crate::index::SegmentReader;
    use crate::query::{Explanation, Scorer, Weight};
    use crate::{DocId, Score};

    struct DummyWeight;
    impl Weight for DummyWeight {
        fn scorer(&self, _reader: &SegmentReader, _boost: Score) -> crate::Result<Box<dyn Scorer>> {
            Err(crate::TantivyError::InternalError("dummy impl".to_owned()))
        }

        fn explain(&self, _reader: &SegmentReader, _doc: DocId) -> crate::Result<Explanation> {
            Err(crate::TantivyError::InternalError("dummy impl".to_owned()))
        }
    }

    #[test]
    fn test_deletequeue() {
        let delete_queue = DeleteQueue::default();

        let make_op = |i: usize| DeleteOperation {
            opstamp: i as u64,
            target: Box::new(DummyWeight),
        };

        delete_queue.push(make_op(1));
        delete_queue.push(make_op(2));

        let snapshot = delete_queue.cursor();
        {
            let mut operations_it = snapshot.clone();
            assert_eq!(operations_it.get().unwrap().opstamp, 1);
            operations_it.advance();
            assert_eq!(operations_it.get().unwrap().opstamp, 2);
            operations_it.advance();
            assert!(operations_it.get().is_none());
            operations_it.advance();

            let mut snapshot2 = delete_queue.cursor();
            assert!(snapshot2.get().is_none());
            delete_queue.push(make_op(3));
            assert_eq!(snapshot2.get().unwrap().opstamp, 3);
            assert_eq!(operations_it.get().unwrap().opstamp, 3);
            assert_eq!(operations_it.get().unwrap().opstamp, 3);
            operations_it.advance();
            assert!(operations_it.get().is_none());
            operations_it.advance();
        }
        {
            let mut operations_it = snapshot;
            assert_eq!(operations_it.get().unwrap().opstamp, 1);
            operations_it.advance();
            assert_eq!(operations_it.get().unwrap().opstamp, 2);
            operations_it.advance();
            assert_eq!(operations_it.get().unwrap().opstamp, 3);
            operations_it.advance();
            assert!(operations_it.get().is_none());
        }
    }
}
