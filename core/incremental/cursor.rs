use crate::numeric::Numeric;
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::types::IOResultOr;
use crate::{
    incremental::{
        compiler::{DeltaSet, ExecuteState},
        dbsp::{Delta, HashableRow, RowKeyZSet},
        view::{IncrementalView, ViewTransactionState},
    },
    return_if_io,
    storage::btree::CursorTrait,
    types::{IOResult, SeekKey, SeekOp, SeekResult, Value},
    LimboError, Pager, Result,
};

/// State machine for seek operations
#[derive(Debug)]
enum SeekState {
    /// Initial state before seeking
    Init,

    /// Actively seeking with btree and uncommitted iterators
    Seek {
        /// The row we are trying to find
        target: i64,
    },

    /// Btree seek returned TryAdvance, now advancing with next()/prev()
    Advancing {
        /// The row we are trying to find
        target: i64,
        /// The seek operation (determines direction of advance)
        op: SeekOp,
    },

    /// Seek completed successfully
    Done,
}

/// Cursor for reading materialized views that combines:
/// 1. Persistent btree data (committed state)
/// 2. Transaction-specific DBSP deltas (uncommitted changes)
///
/// Works like a regular table cursor - reads from disk on-demand
/// and overlays transaction changes as needed.
pub struct MaterializedViewCursor {
    // Core components
    btree_cursor: Box<dyn CursorTrait>,
    view: Arc<Mutex<IncrementalView>>,
    pager: Arc<Pager>,

    // Current changes that are uncommitted
    uncommitted: RowKeyZSet,

    // Reference to shared transaction state for this specific view - shared with Connection
    tx_state: Arc<ViewTransactionState>,

    // The transaction state always grows. It never gets reduced. That is in the very nature of
    // DBSP, because deletions are just appends with weight < 0. So we will use the length of the
    // state to check if we have to recompute the transaction state
    last_tx_state_len: usize,

    // Current row cache - only cache the current row we're looking at
    current_row: Option<(i64, Vec<Value>)>,

    // Execution state for circuit processing
    execute_state: ExecuteState,

    // State machine for seek operations
    seek_state: SeekState,
}

impl MaterializedViewCursor {
    pub fn new(
        btree_cursor: Box<dyn CursorTrait>,
        view: Arc<Mutex<IncrementalView>>,
        pager: Arc<Pager>,
        tx_state: Arc<ViewTransactionState>,
    ) -> Result<Self> {
        Ok(Self {
            btree_cursor,
            view,
            pager,
            uncommitted: RowKeyZSet::new(),
            tx_state,
            last_tx_state_len: 0,
            current_row: None,
            execute_state: ExecuteState::Uninitialized,
            seek_state: SeekState::Init,
        })
    }

    /// Compute transaction changes lazily on first access
    fn ensure_tx_changes_computed(&mut self) -> IOResultOr<()> {
        // Check if we've already processed the current state
        let current_len = self.tx_state.len();
        if current_len == self.last_tx_state_len {
            return Ok(IOResult::Done(()));
        }

        // Get the view and the current transaction state
        let mut view_guard = self.view.lock();
        let table_deltas = self.tx_state.get_table_deltas();

        // Process the deltas through the circuit to get materialized changes
        let mut uncommitted = DeltaSet::new();
        for (table_name, delta) in table_deltas {
            uncommitted.insert(table_name, delta);
        }

        let processed_delta = return_if_io!(view_guard.execute_with_uncommitted(
            uncommitted,
            self.pager.clone(),
            &mut self.execute_state
        ));

        self.uncommitted = RowKeyZSet::from_delta(&processed_delta);
        self.last_tx_state_len = current_len;
        Ok(IOResult::Done(()))
    }

    // Read the current btree entry as a vector (empty if no current position)
    fn read_btree_delta_entry(&mut self) -> IOResultOr<Vec<(HashableRow, isize)>> {
        let btree_rowid = return_if_io!(self.btree_cursor.rowid());
        let rowid = match btree_rowid {
            None => return Ok(IOResult::Done(Vec::new())),
            Some(rowid) => rowid,
        };

        let btree_record = return_if_io!(self.btree_cursor.record()).ok_or_else(|| {
            crate::LimboError::InternalError(
                "Invalid data in materialized view: found a rowid, but not the row!".to_string(),
            )
        })?;
        let mut btree_values = btree_record.get_values_owned()?;

        // The last column should be the weight
        let weight_value = btree_values.pop().ok_or_else(|| {
            crate::LimboError::InternalError(
                "Invalid data in materialized view: no weight column found".to_string(),
            )
        })?;

        // Convert the Value to isize weight
        let weight = match weight_value {
            Value::Numeric(Numeric::Integer(w)) => w as isize,
            _ => {
                return Err(crate::LimboError::InternalError(format!(
                    "Invalid data in materialized view: expected integer weight, found {weight_value:?}"
                )).into())
            }
        };

        if weight <= 0 {
            return Err(crate::LimboError::InternalError(format!(
                "Invalid data in materialized view: expected a positive weight, found {weight}"
            ))
            .into());
        }

        // TODO: std boundary conversion; adjust once incremental uses the
        // allocator with fallible allocations everywhere.
        Ok(IOResult::Done(vec![(
            HashableRow::new(rowid, btree_values.into_iter().collect()),
            weight,
        )]))
    }

    /// Process btree changes: merge with uncommitted, build zset, and determine result.
    /// Returns the next state action: either Done with a result, or updates seek_state for another iteration.
    fn process_btree_changes(
        &mut self,
        target: i64,
        target_rowid: i64,
        op: SeekOp,
        changes: Vec<(HashableRow, isize)>,
    ) -> IOResultOr<()> {
        let mut btree_entries = Delta { changes };
        let changes = self.uncommitted.seek(target, op);

        let uncommitted_entries = Delta { changes };
        btree_entries.merge(&uncommitted_entries);

        // if empty pre-zset, means nothing was found. Empty post-zset can mean that
        // we just canceled weights.
        if btree_entries.is_empty() {
            self.seek_state = SeekState::Done;
            return Ok(IOResult::Done(()));
        }

        let min_seen = btree_entries
            .changes
            .first()
            .expect("cannot be empty, we just tested for it")
            .0
            .rowid;
        let max_seen = btree_entries
            .changes
            .last()
            .expect("cannot be empty, we just tested for it")
            .0
            .rowid;

        let zset = RowKeyZSet::from_delta(&btree_entries);
        let ret = zset.seek(target_rowid, op);

        if !ret.is_empty() {
            let (row, _) = &ret[0];
            self.current_row = Some((row.rowid, row.values.clone()));
            self.seek_state = SeekState::Done;
            return Ok(IOResult::Done(()));
        }

        let new_target = match op {
            SeekOp::GT => Some(max_seen),
            SeekOp::GE { eq_only: false } => Some(max_seen + 1),
            SeekOp::LT => Some(min_seen),
            SeekOp::LE { eq_only: false } => Some(min_seen - 1),
            SeekOp::LE { eq_only: true } | SeekOp::GE { eq_only: true } => None,
        };

        if let Some(target) = new_target {
            self.seek_state = SeekState::Seek { target };
        } else {
            self.seek_state = SeekState::Done;
        }
        Ok(IOResult::Done(()))
    }

    /// Internal seek implementation that doesn't check preconditions
    fn do_seek(&mut self, target_rowid: i64, op: SeekOp) -> IOResultOr<SeekResult> {
        loop {
            // Process state machine - need to handle mutable borrow carefully
            match &mut self.seek_state {
                SeekState::Init => {
                    self.current_row = None;
                    self.seek_state = SeekState::Seek {
                        target: target_rowid,
                    };
                }
                SeekState::Seek { target } => {
                    let target = *target;
                    let btree_result =
                        return_if_io!(self.btree_cursor.seek(SeekKey::TableRowId(target), op));

                    let changes = match btree_result {
                        SeekResult::Found => return_if_io!(self.read_btree_delta_entry()),
                        SeekResult::TryAdvance => {
                            // Transition to Advancing state before calling next/prev.
                            // This ensures that if next/prev returns IO, we resume in
                            // Advancing state and don't redundantly call seek again.
                            self.seek_state = SeekState::Advancing { target, op };
                            continue;
                        }
                        SeekResult::NotFound => Vec::new(),
                    };

                    return_if_io!(self.process_btree_changes(target, target_rowid, op, changes));

                    // Check if we're done or need to continue seeking
                    if matches!(self.seek_state, SeekState::Done) {
                        let result = if self.current_row.is_some() {
                            SeekResult::Found
                        } else {
                            SeekResult::NotFound
                        };
                        return Ok(IOResult::Done(result));
                    }
                    // Otherwise state is Seek with new target, loop continues
                }
                SeekState::Advancing { target, op } => {
                    let target = *target;
                    let op = *op;

                    // Cursor is positioned at the leaf but current entry doesn't match.
                    // Advance in the appropriate direction to find the next matching entry.
                    match op {
                        SeekOp::GT | SeekOp::GE { .. } => {
                            return_if_io!(self.btree_cursor.next())
                        }
                        SeekOp::LT | SeekOp::LE { .. } => {
                            return_if_io!(self.btree_cursor.prev())
                        }
                    };
                    // read_btree_delta_entry handles the case where cursor is at end
                    let changes = return_if_io!(self.read_btree_delta_entry());

                    return_if_io!(self.process_btree_changes(target, target_rowid, op, changes));

                    // Check if we're done or need to continue seeking
                    if matches!(self.seek_state, SeekState::Done) {
                        let result = if self.current_row.is_some() {
                            SeekResult::Found
                        } else {
                            SeekResult::NotFound
                        };
                        return Ok(IOResult::Done(result));
                    }
                    // Otherwise state is Seek with new target, loop continues
                }
                SeekState::Done => {
                    // We always return before setting the state to done. Meaning if we got here,
                    // this is a new seek.
                    self.seek_state = SeekState::Init;
                }
            }
        }
    }

    pub fn seek(&mut self, key: SeekKey, op: SeekOp) -> IOResultOr<SeekResult> {
        // Ensure transaction changes are computed
        return_if_io!(self.ensure_tx_changes_computed());

        let target_rowid = match &key {
            SeekKey::TableRowId(rowid) => *rowid,
            SeekKey::IndexKey(_) => {
                return Err(LimboError::ParseError(
                    "Cannot search a materialized view with an index key".to_string(),
                )
                .into());
            }
        };

        self.do_seek(target_rowid, op)
    }

    pub fn next(&mut self) -> IOResultOr<bool> {
        // If there's a pending seek operation (due to IO), complete it first.
        // SeekState::Seek or SeekState::Advancing means IO was interrupted mid-seek and we need to resume.
        // SeekState::Init means cursor was never positioned - don't resume, fall through to check current_row.
        if matches!(
            self.seek_state,
            SeekState::Seek { .. } | SeekState::Advancing { .. }
        ) {
            // target is ignored when resuming
            let result = return_if_io!(self.do_seek(0, SeekOp::GT));
            return Ok(IOResult::Done(result == SeekResult::Found));
        }

        // If cursor is not positioned (no current_row), return false
        // This matches BTreeCursor behavior when valid_state == Invalid
        let Some((current_rowid, _)) = &self.current_row else {
            return Ok(IOResult::Done(false));
        };

        // Use GT to find the next row after current position
        let result = return_if_io!(self.do_seek(*current_rowid, SeekOp::GT));
        Ok(IOResult::Done(result == SeekResult::Found))
    }

    pub fn column(&mut self, col: usize) -> IOResultOr<Value> {
        if let Some((_, ref values)) = self.current_row {
            Ok(IOResult::Done(
                values.get(col).cloned().unwrap_or(Value::Null),
            ))
        } else {
            Ok(IOResult::Done(Value::Null))
        }
    }

    pub fn rowid(&self) -> IOResultOr<Option<i64>> {
        Ok(IOResult::Done(self.current_row.as_ref().map(|(id, _)| *id)))
    }

    pub fn rewind(&mut self) -> IOResultOr<()> {
        return_if_io!(self.ensure_tx_changes_computed());
        // Seek GT from i64::MIN to find the first row using internal do_seek
        let _result = return_if_io!(self.do_seek(i64::MIN, SeekOp::GT));
        Ok(IOResult::Done(()))
    }

    pub fn is_valid(&self) -> Result<bool> {
        Ok(self.current_row.is_some())
    }
}

#[cfg(test)]
#[path = "../tests/unit/incremental/cursor/tests.rs"]
mod tests;
