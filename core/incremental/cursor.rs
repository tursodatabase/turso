use crate::storage::btree::{BTreeKey, CursorStep, CursorTrait, SavePositionResult};
use crate::sync::Arc;
use crate::types::{IOResultOr, ImmutableRecord, IndexInfo};
use crate::{
    return_if_io,
    types::{IOResult, SeekKey, SeekOp, SeekResult, Value},
    LimboError, Pager, Register,
};

/// Read cursor for a transactionally maintained materialized-view btree.
///
/// The view btree is an ordinary table btree whose records hold the view's
/// columns followed by the row's weight, so every read goes straight to the
/// btree cursor and the VDBE treats this cursor like any table cursor. The
/// one extra duty is running pending view maintenance before the cursor is
/// positioned: a read from inside a still-running write statement (for
/// example a trigger) must see that statement's earlier writes.
pub struct MaterializedViewCursor {
    btree_cursor: Box<dyn CursorTrait>,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,

    // The transaction-wide change-log revision this cursor has applied.
    // This must be global rather than the direct view subscription revision so
    // reads through a chain of materialized views see base changes before
    // commit scheduling has produced the downstream delta.
    last_change_log_revision: u64,

    // In-progress maintenance batch, parked across I/O yields when a view is
    // read before its enclosing write statement reaches Halt.
    view_delta_state: crate::vdbe::ViewDeltaApplyState,
}

impl MaterializedViewCursor {
    pub fn new(
        btree_cursor: Box<dyn CursorTrait>,
        pager: Arc<Pager>,
        connection: Arc<crate::Connection>,
    ) -> Self {
        Self {
            btree_cursor,
            pager,
            connection,
            last_change_log_revision: 0,
            view_delta_state: crate::vdbe::ViewDeltaApplyState::NotStarted,
        }
    }

    /// Apply pending transaction changes before reading the view.
    ///
    /// This invokes the same dependency-ordered maintenance programs used at
    /// statement completion and commit. It exists for reads reached from
    /// inside a still-running write statement; ordinary later statements see
    /// an already-maintained btree and take the fast path.
    fn ensure_tx_changes_computed(&mut self) -> IOResultOr<()> {
        let current_revision = self.connection.transaction_changes.change_revision();
        if current_revision == self.last_change_log_revision {
            return Ok(IOResult::Done(()));
        }

        match crate::vdbe::step_view_delta_batch(
            &self.connection,
            &mut self.view_delta_state,
            false,
            &self.pager,
        )? {
            IOResult::IO(io) => Ok(IOResult::IO(io)),
            IOResult::Done(()) => {
                self.view_delta_state = crate::vdbe::ViewDeltaApplyState::NotStarted;
                self.last_change_log_revision =
                    self.connection.transaction_changes.change_revision();
                Ok(IOResult::Done(()))
            }
        }
    }
}

#[cfg(feature = "test_helper")]
impl crate::Connection {
    /// A read cursor over materialized view `view_name` in the main database,
    /// the same kind of cursor a query opens on the view.
    pub fn materialized_view_cursor_for_testing(
        self: &Arc<Self>,
        view_name: &str,
    ) -> crate::Result<Box<dyn CursorTrait>> {
        let view = self
            .schema
            .read()
            .get_materialized_view(crate::NamespaceId::ROOT, view_name)
            .ok_or_else(|| {
                LimboError::InternalError(format!("no materialized view {view_name}"))
            })?;
        let view = view.lock();
        let root_page = view.get_root_page();
        if root_page == 0 {
            return Err(LimboError::InternalError(format!(
                "materialized view {view_name} has no btree"
            )));
        }
        let num_columns = view.column_schema.columns.len();
        drop(view);

        let pager = self.get_pager();
        let btree_cursor = Box::new(crate::storage::btree::BTreeCursor::new(
            pager.clone(),
            root_page,
            num_columns,
        ));
        let cursor = MaterializedViewCursor::new(btree_cursor, pager, self.clone());
        cursor.register_with_pager();
        Ok(Box::new(cursor))
    }
}

/// Positioning methods first bring the view up to date; everything else is
/// the btree cursor's own behavior. Writes go through the maintenance
/// programs' own cursors, never through this one.
impl CursorTrait for MaterializedViewCursor {
    fn last(&mut self) -> IOResultOr<()> {
        return_if_io!(self.ensure_tx_changes_computed());
        self.btree_cursor.last()
    }

    fn next(&mut self) -> IOResultOr<()> {
        self.btree_cursor.next()
    }

    fn prev(&mut self) -> IOResultOr<()> {
        self.btree_cursor.prev()
    }

    fn next_row(&mut self) -> CursorStep {
        self.btree_cursor.next_row()
    }

    fn prev_row(&mut self) -> CursorStep {
        self.btree_cursor.prev_row()
    }

    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
        self.btree_cursor.rowid()
    }

    fn note_external_row_write(&mut self, rowid: Option<i64>) {
        self.btree_cursor.note_external_row_write(rowid)
    }

    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
        self.btree_cursor.record()
    }

    fn record_payload(&mut self) -> IOResultOr<Option<&[u8]>> {
        self.btree_cursor.record_payload()
    }

    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        if matches!(key, SeekKey::IndexKey(_)) {
            return Err(index_seek_error());
        }
        return_if_io!(self.ensure_tx_changes_computed());
        self.btree_cursor.seek(key, op)
    }

    fn seek_unpacked(&mut self, _registers: &[Register], _op: SeekOp) -> IOResultOr<SeekResult> {
        Err(index_seek_error())
    }

    fn insert(&mut self, _key: &BTreeKey) -> IOResultOr<()> {
        Err(read_only_error())
    }

    fn delete(&mut self) -> IOResultOr<()> {
        Err(read_only_error())
    }

    fn set_null_flag(&mut self, flag: bool) {
        self.btree_cursor.set_null_flag(flag)
    }

    fn get_null_flag(&self) -> bool {
        self.btree_cursor.get_null_flag()
    }

    fn exists(&mut self, key: &Value) -> IOResultOr<bool> {
        return_if_io!(self.ensure_tx_changes_computed());
        self.btree_cursor.exists(key)
    }

    fn clear_btree(&mut self) -> IOResultOr<u64> {
        Err(read_only_error())
    }

    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        Err(read_only_error())
    }

    fn count(&mut self) -> IOResultOr<usize> {
        return_if_io!(self.ensure_tx_changes_computed());
        self.btree_cursor.count()
    }

    fn is_empty(&self) -> bool {
        self.btree_cursor.is_empty()
    }

    fn root_page(&self) -> i64 {
        self.btree_cursor.root_page()
    }

    fn rewind(&mut self) -> IOResultOr<()> {
        return_if_io!(self.ensure_tx_changes_computed());
        self.btree_cursor.rewind()
    }

    fn has_record(&self) -> bool {
        self.btree_cursor.has_record()
    }

    fn set_has_record(&mut self, has_record: bool) {
        self.btree_cursor.set_has_record(has_record)
    }

    fn index_info(&self) -> Option<&Arc<IndexInfo>> {
        self.btree_cursor.index_info()
    }

    fn seek_end(&mut self) -> IOResultOr<()> {
        Err(read_only_error())
    }

    fn seek_to_last(&mut self) -> IOResultOr<()> {
        return_if_io!(self.ensure_tx_changes_computed());
        self.btree_cursor.seek_to_last()
    }

    fn is_mvcc(&self) -> bool {
        self.btree_cursor.is_mvcc()
    }

    fn invalidate_record(&mut self) {
        self.btree_cursor.invalidate_record()
    }

    fn has_rowid(&self) -> bool {
        self.btree_cursor.has_rowid()
    }

    fn get_pager(&self) -> Arc<Pager> {
        self.btree_cursor.get_pager()
    }

    fn get_skip_advance(&self) -> bool {
        self.btree_cursor.get_skip_advance()
    }

    fn invalidate_btree_cache(&mut self) {
        self.btree_cursor.invalidate_btree_cache()
    }

    fn register_with_pager(&self) {
        self.btree_cursor.register_with_pager()
    }

    fn recycle(self: Box<Self>) {
        self.btree_cursor.recycle()
    }

    fn set_has_peers_for_external_writes(&self, has_peers: bool) {
        self.btree_cursor
            .set_has_peers_for_external_writes(has_peers)
    }

    fn try_save_position_for_external_balance(&mut self) -> IOResultOr<SavePositionResult> {
        self.btree_cursor.try_save_position_for_external_balance()
    }
}

fn index_seek_error() -> Box<LimboError> {
    Box::new(LimboError::ParseError(
        "Cannot search a materialized view with an index key".to_string(),
    ))
}

fn read_only_error() -> Box<LimboError> {
    Box::new(LimboError::InternalError(
        "a materialized view cursor cannot write; maintenance writes the view".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    //! Cursor behavior when the btree cursor returns IO (pending).
    use super::*;
    use crate::io::Completion;
    use crate::storage::btree::{BTreeKey, CursorStep, CursorTrait};
    use crate::types::{IOCompletions, ImmutableRecord, IndexInfo};
    use crate::Register;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock btree cursor that tracks calls and can simulate IO pending states.
    /// Used to verify that seek operations aren't redundantly repeated after IO resumption.
    struct MockBTreeCursor {
        /// Number of times seek() was called
        seek_count: AtomicUsize,
        /// Number of times next() was called
        next_count: AtomicUsize,
        /// Number of times prev() was called
        prev_count: AtomicUsize,
        /// Current rowid to return
        current_rowid: Option<i64>,
        /// Record to return (needs to live for the cursor lifetime)
        record: ImmutableRecord,
        /// Index info
        index_info: Arc<IndexInfo>,
        advance_completion: Completion,
        advance_error: Option<Box<crate::LimboError>>,
        null_flag: bool,
    }

    #[test]
    fn cursor_step_owns_io_after_cursor_is_dropped() {
        for forward in [true, false] {
            let mut cursor = MockBTreeCursor::new();
            cursor.advance_completion = Completion::new_sync(|_| {});
            let completion = cursor.advance_completion.clone();
            let mut cursor: Box<dyn CursorTrait> = Box::new(cursor);
            let step = if forward {
                cursor.next_row()
            } else {
                cursor.prev_row()
            };
            let CursorStep::IO(io) = step else {
                panic!("expected IO, got {step:?}");
            };
            let resumed = if forward {
                cursor.next_row()
            } else {
                cursor.prev_row()
            };
            assert!(matches!(resumed, CursorStep::Row));
            drop(cursor);
            assert!(!io.finished());
            completion.complete(0);
            assert!(io.finished());
        }
    }

    #[test]
    fn cursor_step_preserves_advance_errors() {
        for forward in [true, false] {
            let mut cursor = MockBTreeCursor::new();
            let error = Box::new(crate::LimboError::InternalError("advance failed".into()));
            let original = std::ptr::from_ref(error.as_ref());
            cursor.advance_error = Some(error);
            let cursor: &mut dyn CursorTrait = &mut cursor;
            let step = if forward {
                cursor.next_row()
            } else {
                cursor.prev_row()
            };
            let CursorStep::Error(error) = step else {
                panic!("expected error, got {step:?}");
            };
            assert_eq!(std::ptr::from_ref(error.as_ref()), original);
        }
    }

    #[test]
    fn cursor_step_handles_null_and_empty_rows() {
        for forward in [true, false] {
            let mut cursor = MockBTreeCursor::new();
            cursor.set_null_flag(true);
            let advance = |cursor: &mut dyn CursorTrait| {
                if forward {
                    cursor.next_row()
                } else {
                    cursor.prev_row()
                }
            };
            assert!(matches!(advance(&mut cursor), CursorStep::Empty));
            assert!(!cursor.get_null_flag());
            assert_eq!(cursor.next_count.load(Ordering::SeqCst), 0);
            assert_eq!(cursor.get_prev_count(), 0);
            assert!(matches!(advance(&mut cursor), CursorStep::IO(_)));
            assert!(matches!(advance(&mut cursor), CursorStep::Row));
            cursor.current_rowid = None;
            assert!(matches!(advance(&mut cursor), CursorStep::Empty));
        }
    }

    impl MockBTreeCursor {
        fn new() -> Self {
            // Create a minimal record with rowid=1, value=10, weight=1
            let record = Self::create_test_record(1, 10, 1);
            Self {
                seek_count: AtomicUsize::new(0),
                next_count: AtomicUsize::new(0),
                prev_count: AtomicUsize::new(0),
                current_rowid: Some(1),
                record,
                index_info: Arc::new(
                    IndexInfo::new([], true, 1, false, crate::types::NanPolicy::Null).unwrap(),
                ),
                advance_completion: Completion::new_yield(),
                advance_error: None,
                null_flag: false,
            }
        }

        fn create_test_record(rowid: i64, value: i64, weight: i64) -> ImmutableRecord {
            // Build a binary record with format: [header_size, type1, type2, type3, rowid, value, weight]
            // For integers, type code is 1 for 1-byte int, 2 for 2-byte, etc.
            // Using type 6 (8-byte integer) for all values
            // Header: 4 bytes (header size byte + 3 type bytes)
            let mut payload = crate::alloc::vec![
                4u8, // header size
                6u8, // type for rowid (8-byte int)
                6u8, // type for value (8-byte int)
                6u8, // type for weight (8-byte int)
            ];

            // Data: 3 x 8-byte integers
            payload.extend_from_slice(&rowid.to_be_bytes());
            payload.extend_from_slice(&value.to_be_bytes());
            payload.extend_from_slice(&weight.to_be_bytes());

            ImmutableRecord::from_bin_record(payload)
        }

        fn get_prev_count(&self) -> usize {
            self.prev_count.load(Ordering::SeqCst)
        }
    }

    impl CursorTrait for MockBTreeCursor {
        fn seek(&mut self, _key: SeekKey<'_>, _op: SeekOp) -> IOResultOr<SeekResult> {
            let count = self.seek_count.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                // First seek returns TryAdvance
                Ok(IOResult::Done(SeekResult::TryAdvance))
            } else {
                // Subsequent seeks return Found to avoid infinite loop
                // (The bug is that this second seek happens at all)
                Ok(IOResult::Done(SeekResult::Found))
            }
        }

        fn seek_unpacked(
            &mut self,
            _registers: &[Register],
            _op: SeekOp,
        ) -> IOResultOr<SeekResult> {
            // Not used in these tests
            Ok(IOResult::Done(SeekResult::NotFound))
        }

        fn next(&mut self) -> IOResultOr<()> {
            if let Some(err) = self.advance_error.take() {
                return Err(err);
            }
            let count = self.next_count.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                // First call returns IO (pending)
                Ok(IOResult::IO(IOCompletions(self.advance_completion.clone())))
            } else {
                // Subsequent calls return Done
                Ok(IOResult::Done(()))
            }
        }

        fn prev(&mut self) -> IOResultOr<()> {
            if let Some(err) = self.advance_error.take() {
                return Err(err);
            }
            let count = self.prev_count.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                // First call returns IO (pending)
                Ok(IOResult::IO(IOCompletions(self.advance_completion.clone())))
            } else {
                // Subsequent calls return Done
                Ok(IOResult::Done(()))
            }
        }

        fn rowid(&mut self) -> IOResultOr<Option<i64>> {
            Ok(IOResult::Done(self.current_rowid))
        }

        fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
            Ok(IOResult::Done(Some(&self.record)))
        }

        fn last(&mut self) -> IOResultOr<()> {
            Ok(IOResult::Done(()))
        }

        fn insert(&mut self, _key: &BTreeKey) -> IOResultOr<()> {
            Ok(IOResult::Done(()))
        }

        fn delete(&mut self) -> IOResultOr<()> {
            Ok(IOResult::Done(()))
        }

        fn set_null_flag(&mut self, flag: bool) {
            self.null_flag = flag;
        }

        fn get_null_flag(&self) -> bool {
            self.null_flag
        }

        fn exists(&mut self, _key: &Value) -> IOResultOr<bool> {
            Ok(IOResult::Done(false))
        }

        fn clear_btree(&mut self) -> IOResultOr<u64> {
            Ok(IOResult::Done(0))
        }

        fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
            Ok(IOResult::Done(None))
        }

        fn count(&mut self) -> IOResultOr<usize> {
            Ok(IOResult::Done(0))
        }

        fn is_empty(&self) -> bool {
            self.current_rowid.is_none()
        }

        fn root_page(&self) -> i64 {
            1
        }

        fn rewind(&mut self) -> IOResultOr<()> {
            Ok(IOResult::Done(()))
        }

        fn has_record(&self) -> bool {
            true
        }

        fn set_has_record(&mut self, _has_record: bool) {}

        fn index_info(&self) -> Option<&Arc<IndexInfo>> {
            Some(&self.index_info)
        }

        fn seek_end(&mut self) -> IOResultOr<()> {
            Ok(IOResult::Done(()))
        }

        fn seek_to_last(&mut self) -> IOResultOr<()> {
            Ok(IOResult::Done(()))
        }

        fn invalidate_record(&mut self) {}

        fn has_rowid(&self) -> bool {
            true
        }

        fn get_pager(&self) -> Arc<Pager> {
            panic!("MockBTreeCursor::get_pager should not be called")
        }

        fn get_skip_advance(&self) -> bool {
            false
        }
    }
}
