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
