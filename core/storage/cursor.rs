use std::any::Any;
use std::cell::Ref;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::storage::btree::{BTreeCursor, BTreeKey};
use crate::types::{
    IOResult, ImmutableRecord, IndexInfo, RecordCursor, SeekKey, SeekOp, SeekResult,
};
use crate::{MvCursor, Pager, Result, Value};

pub trait CursorTrait: Any {
    /// Move cursor to last entry.
    fn last(&mut self) -> Result<IOResult<()>>;
    /// Move cursor to next entry.
    fn next(&mut self) -> Result<IOResult<bool>>;
    /// Move cursor to previous entry.
    fn prev(&mut self) -> Result<IOResult<bool>>;
    /// Get the rowid of the entry the cursor is poiting to if any
    fn rowid(&self) -> Result<IOResult<Option<i64>>>;
    /// Get the record of the entry the cursor is poiting to if any
    fn record(&self) -> Result<IOResult<Option<Ref<'_, ImmutableRecord>>>>;
    /// Move the cursor based on the key and the type of operation (op).
    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>>;
    /// Insert a record in the position the cursor is at.
    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>>;
    /// Delete a record in the position the cursor is at.
    fn delete(&mut self) -> Result<IOResult<()>>;
    fn set_null_flag(&mut self, flag: bool);
    fn get_null_flag(&self) -> bool;
    /// Check if a key exists.
    fn exists(&mut self, key: &Value) -> Result<IOResult<bool>>;
    fn clear_btree(&mut self) -> Result<IOResult<Option<usize>>>;
    fn btree_destroy(&mut self) -> Result<IOResult<Option<usize>>>;
    /// Count the number of entries in the b-tree
    ///
    /// Only supposed to be used in the context of a simple Count Select Statement
    fn count(&mut self) -> Result<IOResult<usize>>;
    fn is_empty(&self) -> bool;
    fn root_page(&self) -> i64;
    /// Move cursor at the start.
    fn rewind(&mut self) -> Result<IOResult<()>>;
    /// Check if cursor is poiting at a valid entry with a record.
    fn has_record(&self) -> bool;
    fn set_has_record(&self, has_record: bool);
    fn get_index_info(&self) -> &IndexInfo;

    fn seek_end(&mut self) -> Result<IOResult<()>>;
    fn seek_to_last(&mut self) -> Result<IOResult<()>>;

    // --- start: BTreeCursor specific functions ----
    fn invalidate_record(&mut self);
    fn has_rowid(&self) -> bool;
    fn record_cursor_mut(&self) -> std::cell::RefMut<'_, RecordCursor>;
    fn get_pager(&self) -> Arc<Pager>;
    fn get_skip_advance(&self) -> bool;

    // FIXME: remove once we implement trait for mvcc
    fn get_mvcc_cursor(&self) -> Arc<RwLock<MvCursor>>;
    // --- end: BTreeCursor specific functions ----
}

/// Add more variants if we need to hold different types of Cursors
pub enum CursorDispatch {
    BTree(BTreeCursor),
}

impl CursorTrait for CursorDispatch {
    fn last(&mut self) -> Result<IOResult<()>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.last(),
        }
    }

    fn next(&mut self) -> Result<IOResult<bool>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.next(),
        }
    }

    fn prev(&mut self) -> Result<IOResult<bool>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.prev(),
        }
    }

    fn rowid(&self) -> Result<IOResult<Option<i64>>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.rowid(),
        }
    }

    fn record(&self) -> Result<IOResult<Option<Ref<'_, ImmutableRecord>>>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.record(),
        }
    }

    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.seek(key, op),
        }
    }

    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.insert(key),
        }
    }

    fn delete(&mut self) -> Result<IOResult<()>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.delete(),
        }
    }

    fn set_null_flag(&mut self, flag: bool) {
        match self {
            CursorDispatch::BTree(cursor) => cursor.set_null_flag(flag),
        }
    }

    fn get_null_flag(&self) -> bool {
        match self {
            CursorDispatch::BTree(cursor) => cursor.get_null_flag(),
        }
    }

    fn exists(&mut self, key: &Value) -> Result<IOResult<bool>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.exists(key),
        }
    }

    fn clear_btree(&mut self) -> Result<IOResult<Option<usize>>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.clear_btree(),
        }
    }

    fn btree_destroy(&mut self) -> Result<IOResult<Option<usize>>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.btree_destroy(),
        }
    }

    fn count(&mut self) -> Result<IOResult<usize>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.count(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            CursorDispatch::BTree(cursor) => cursor.is_empty(),
        }
    }

    fn root_page(&self) -> i64 {
        match self {
            CursorDispatch::BTree(cursor) => cursor.root_page(),
        }
    }

    fn rewind(&mut self) -> Result<IOResult<()>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.rewind(),
        }
    }

    fn has_record(&self) -> bool {
        match self {
            CursorDispatch::BTree(cursor) => cursor.has_record(),
        }
    }

    fn set_has_record(&self, has_record: bool) {
        match self {
            CursorDispatch::BTree(cursor) => cursor.set_has_record(has_record),
        }
    }

    fn get_index_info(&self) -> &IndexInfo {
        match self {
            CursorDispatch::BTree(cursor) => cursor.get_index_info(),
        }
    }

    fn seek_end(&mut self) -> Result<IOResult<()>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.seek_end(),
        }
    }

    fn seek_to_last(&mut self) -> Result<IOResult<()>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.seek_to_last(),
        }
    }

    fn invalidate_record(&mut self) {
        match self {
            CursorDispatch::BTree(cursor) => cursor.invalidate_record(),
        }
    }

    fn has_rowid(&self) -> bool {
        match self {
            CursorDispatch::BTree(cursor) => cursor.has_rowid(),
        }
    }

    fn record_cursor_mut(&self) -> std::cell::RefMut<'_, RecordCursor> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.record_cursor_mut(),
        }
    }

    fn get_pager(&self) -> Arc<Pager> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.get_pager(),
        }
    }

    fn get_skip_advance(&self) -> bool {
        match self {
            CursorDispatch::BTree(cursor) => cursor.get_skip_advance(),
        }
    }

    fn get_mvcc_cursor(&self) -> Arc<RwLock<MvCursor>> {
        match self {
            CursorDispatch::BTree(cursor) => cursor.get_mvcc_cursor(),
        }
    }
}
