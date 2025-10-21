use parking_lot::RwLock;

use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{MVTableId, MvStore, Row, RowID};
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::types::{IOResult, ImmutableRecord, RecordCursor, SeekKey, SeekOp, SeekResult};
use crate::{return_if_io, Result};
use crate::{Pager, Value};
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
enum CursorPosition {
    /// We haven't loaded any row yet.
    BeforeFirst,
    /// We have loaded a row.
    Loaded(RowID),
    /// We have reached the end of the table.
    End,
}

pub struct MvccLazyCursor<Clock: LogicalClock> {
    pub db: Arc<MvStore<Clock>>,
    current_pos: RefCell<CursorPosition>,
    pub table_id: MVTableId,
    tx_id: u64,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: RefCell<Option<ImmutableRecord>>,
    _btree_cursor: Box<dyn CursorTrait>,
    null_flag: bool,
    record_cursor: RefCell<RecordCursor>,
    next_rowid_lock: Arc<RwLock<()>>,
}

impl<Clock: LogicalClock + 'static> MvccLazyCursor<Clock> {
    pub fn new(
        db: Arc<MvStore<Clock>>,
        tx_id: u64,
        root_page_or_table_id: i64,
        pager: Arc<Pager>,
        btree_cursor: Box<dyn CursorTrait>,
    ) -> Result<MvccLazyCursor<Clock>> {
        assert!(
            (&*btree_cursor as &dyn Any).is::<BTreeCursor>(),
            "BTreeCursor expected for mvcc cursor"
        );
        let table_id = db.get_table_id_from_root_page(root_page_or_table_id);
        db.maybe_initialize_table(table_id, pager)?;
        Ok(Self {
            db,
            tx_id,
            current_pos: RefCell::new(CursorPosition::BeforeFirst),
            table_id,
            reusable_immutable_record: RefCell::new(None),
            _btree_cursor: btree_cursor,
            null_flag: false,
            record_cursor: RefCell::new(RecordCursor::new()),
            next_rowid_lock: Arc::new(RwLock::new(())),
        })
    }

    pub fn current_row(&self) -> Result<Option<Row>> {
        match *self.current_pos.borrow() {
            CursorPosition::Loaded(id) => self.db.read(self.tx_id, id),
            CursorPosition::BeforeFirst => {
                // If we are before first, we need to try and find the first row.
                let maybe_rowid =
                    self.db
                        .get_next_row_id_for_table(self.table_id, i64::MIN, self.tx_id);
                if let Some(id) = maybe_rowid {
                    self.current_pos.replace(CursorPosition::Loaded(id));
                    self.db.read(self.tx_id, id)
                } else {
                    Ok(None)
                }
            }
            CursorPosition::End => Ok(None),
        }
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn get_next_rowid(&mut self) -> i64 {
        // lock so we don't get same two rowids
        let lock = self.next_rowid_lock.clone();
        let _lock = lock.write();
        let _ = self.last();
        match *self.current_pos.borrow() {
            CursorPosition::Loaded(id) => id.row_id + 1,
            CursorPosition::BeforeFirst => 1,
            CursorPosition::End => i64::MAX,
        }
    }

    fn get_immutable_record_or_create(&self) -> std::cell::RefMut<'_, Option<ImmutableRecord>> {
        let mut reusable_immutable_record = self.reusable_immutable_record.borrow_mut();
        if reusable_immutable_record.is_none() {
            let record = ImmutableRecord::new(1024);
            reusable_immutable_record.replace(record);
        }
        reusable_immutable_record
    }

    fn get_current_pos(&self) -> CursorPosition {
        *self.current_pos.borrow()
    }
}

impl<Clock: LogicalClock + 'static> CursorTrait for MvccLazyCursor<Clock> {
    fn last(&mut self) -> Result<IOResult<()>> {
        let last_rowid = self.db.get_last_rowid(self.table_id);
        if let Some(last_rowid) = last_rowid {
            self.current_pos.replace(CursorPosition::Loaded(RowID {
                table_id: self.table_id,
                row_id: last_rowid,
            }));
        } else {
            self.current_pos.replace(CursorPosition::BeforeFirst);
        }
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    /// Move the cursor to the next row. Returns true if the cursor moved to the next row, false if the cursor is at the end of the table.
    fn next(&mut self) -> Result<IOResult<bool>> {
        let before_first = matches!(self.get_current_pos(), CursorPosition::BeforeFirst);
        let min_id = match *self.current_pos.borrow() {
            CursorPosition::Loaded(id) => id.row_id + 1,
            // TODO: do we need to forward twice?
            CursorPosition::BeforeFirst => i64::MIN, // we need to find first row, so we look from the first id,
            CursorPosition::End => {
                // let's keep same state, we reached the end so no point in moving forward.
                return Ok(IOResult::Done(false));
            }
        };

        let new_position =
            match self
                .db
                .get_next_row_id_for_table(self.table_id, min_id, self.tx_id)
            {
                Some(id) => CursorPosition::Loaded(id),
                None => {
                    if before_first {
                        // if it wasn't loaded and we didn't find anything, it means the table is empty.
                        CursorPosition::BeforeFirst
                    } else {
                        // if we had something loaded, and we didn't find next key then it means we are at the end.
                        CursorPosition::End
                    }
                }
            };
        self.current_pos.replace(new_position);
        self.invalidate_record();

        Ok(IOResult::Done(matches!(
            self.get_current_pos(),
            CursorPosition::Loaded(_)
        )))
    }

    fn prev(&mut self) -> Result<IOResult<bool>> {
        todo!()
    }

    fn rowid(&self) -> Result<IOResult<Option<i64>>> {
        let rowid = match self.get_current_pos() {
            CursorPosition::Loaded(id) => Some(id.row_id),
            CursorPosition::BeforeFirst => {
                // If we are before first, we need to try and find the first row.
                let maybe_rowid =
                    self.db
                        .get_next_row_id_for_table(self.table_id, i64::MIN, self.tx_id);
                if let Some(id) = maybe_rowid {
                    self.current_pos.replace(CursorPosition::Loaded(id));
                    Some(id.row_id)
                } else {
                    self.current_pos.replace(CursorPosition::BeforeFirst);
                    None
                }
            }
            CursorPosition::End => None,
        };
        Ok(IOResult::Done(rowid))
    }

    fn record(
        &self,
    ) -> Result<IOResult<Option<std::cell::Ref<'_, crate::types::ImmutableRecord>>>> {
        let Some(row) = self.current_row()? else {
            return Ok(IOResult::Done(None));
        };

        {
            let mut record = self.get_immutable_record_or_create();
            let record = record.as_mut().unwrap();
            record.invalidate();
            record.start_serialization(&row.data);
        }

        let record_ref =
            Ref::filter_map(self.reusable_immutable_record.borrow(), |opt| opt.as_ref()).unwrap();
        Ok(IOResult::Done(Some(record_ref)))
    }

    fn seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        let row_id = match seek_key {
            SeekKey::TableRowId(row_id) => row_id,
            SeekKey::IndexKey(_) => {
                todo!();
            }
        };
        // gt -> lower_bound bound excluded, we want first row after row_id
        // ge -> lower_bound bound included, we want first row equal to row_id or first row after row_id
        // lt -> upper_bound bound excluded, we want last row before row_id
        // le -> upper_bound bound included, we want last row equal to row_id or first row before row_id
        let rowid = RowID {
            table_id: self.table_id,
            row_id,
        };
        let (bound, lower_bound) = match op {
            SeekOp::GT => (Bound::Excluded(&rowid), true),
            SeekOp::GE { eq_only: _ } => (Bound::Included(&rowid), true),
            SeekOp::LT => (Bound::Excluded(&rowid), false),
            SeekOp::LE { eq_only: _ } => (Bound::Included(&rowid), false),
        };
        self.invalidate_record();
        let rowid = self.db.seek_rowid(bound, lower_bound, self.tx_id);
        if let Some(rowid) = rowid {
            self.current_pos.replace(CursorPosition::Loaded(rowid));
            if op.eq_only() {
                if rowid.row_id == row_id {
                    Ok(IOResult::Done(SeekResult::Found))
                } else {
                    Ok(IOResult::Done(SeekResult::NotFound))
                }
            } else {
                Ok(IOResult::Done(SeekResult::Found))
            }
        } else {
            let forwards = matches!(op, SeekOp::GE { eq_only: _ } | SeekOp::GT);
            if forwards {
                let _ = self.last()?;
            } else {
                let _ = self.rewind()?;
            }
            Ok(IOResult::Done(SeekResult::NotFound))
        }
    }

    /// Insert a row into the table.
    /// Sets the cursor to the inserted row.
    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>> {
        let Some(rowid) = key.maybe_rowid() else {
            todo!()
        };
        let row_id = RowID::new(self.table_id, rowid);
        let record_buf = key.get_record().unwrap().get_payload().to_vec();
        let num_columns = match key {
            BTreeKey::IndexKey(record) => record.column_count(),
            BTreeKey::TableRowId((_, record)) => record.as_ref().unwrap().column_count(),
        };
        let row = crate::mvcc::database::Row::new(row_id, record_buf, num_columns);

        self.current_pos.replace(CursorPosition::Loaded(row.id));
        if self.db.read(self.tx_id, row.id)?.is_some() {
            self.db.update(self.tx_id, row).inspect_err(|_| {
                self.current_pos.replace(CursorPosition::BeforeFirst);
            })?;
        } else {
            self.db.insert(self.tx_id, row).inspect_err(|_| {
                self.current_pos.replace(CursorPosition::BeforeFirst);
            })?;
        }
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    fn delete(&mut self) -> Result<IOResult<()>> {
        let IOResult::Done(Some(rowid)) = self.rowid()? else {
            todo!();
        };
        let rowid = RowID::new(self.table_id, rowid);
        self.db.delete(self.tx_id, rowid)?;
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    fn exists(&mut self, key: &Value) -> Result<IOResult<bool>> {
        self.invalidate_record();
        let int_key = match key {
            Value::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let rowid = self.db.seek_rowid(
            Bound::Included(&RowID {
                table_id: self.table_id,
                row_id: *int_key,
            }),
            true,
            self.tx_id,
        );
        tracing::trace!("found {rowid:?}");
        let exists = if let Some(rowid) = rowid {
            rowid.row_id == *int_key
        } else {
            false
        };
        if exists {
            self.current_pos.replace(CursorPosition::Loaded(RowID {
                table_id: self.table_id,
                row_id: *int_key,
            }));
        }
        Ok(IOResult::Done(exists))
    }

    fn clear_btree(&mut self) -> Result<IOResult<Option<usize>>> {
        todo!()
    }

    fn btree_destroy(&mut self) -> Result<IOResult<Option<usize>>> {
        todo!()
    }

    fn count(&mut self) -> Result<IOResult<usize>> {
        todo!()
    }

    /// Returns true if the is not pointing to any row.
    fn is_empty(&self) -> bool {
        // If we reached the end of the table, it means we traversed the whole table therefore there must be something in the table.
        // If we have loaded a row, it means there is something in the table.
        match self.get_current_pos() {
            CursorPosition::Loaded(_) => false,
            CursorPosition::BeforeFirst => true,
            CursorPosition::End => true,
        }
    }

    fn root_page(&self) -> i64 {
        self.table_id.into()
    }

    fn rewind(&mut self) -> Result<IOResult<()>> {
        self.invalidate_record();
        if !matches!(self.get_current_pos(), CursorPosition::BeforeFirst) {
            self.current_pos.replace(CursorPosition::BeforeFirst);
        }
        // Next will set cursor position to a valid position if it exists, otherwise it will set it to one that doesn't exist.
        let _ = return_if_io!(self.next());
        Ok(IOResult::Done(()))
    }

    fn has_record(&self) -> bool {
        todo!()
    }

    fn set_has_record(&self, _has_record: bool) {
        todo!()
    }

    fn get_index_info(&self) -> &crate::types::IndexInfo {
        todo!()
    }

    fn seek_end(&mut self) -> Result<IOResult<()>> {
        todo!()
    }

    fn seek_to_last(&mut self) -> Result<IOResult<()>> {
        self.invalidate_record();
        let max_rowid = RowID {
            table_id: self.table_id,
            row_id: i64::MAX,
        };
        let bound = Bound::Included(&max_rowid);
        let lower_bound = false;

        let rowid = self.db.seek_rowid(bound, lower_bound, self.tx_id);
        if let Some(rowid) = rowid {
            self.current_pos.replace(CursorPosition::Loaded(rowid));
        } else {
            self.current_pos.replace(CursorPosition::End);
        }
        Ok(IOResult::Done(()))
    }

    fn invalidate_record(&mut self) {
        self.get_immutable_record_or_create()
            .as_mut()
            .unwrap()
            .invalidate();
        self.record_cursor.borrow_mut().invalidate();
    }

    fn has_rowid(&self) -> bool {
        todo!()
    }

    fn record_cursor_mut(&self) -> std::cell::RefMut<'_, crate::types::RecordCursor> {
        self.record_cursor.borrow_mut()
    }

    fn get_pager(&self) -> Arc<Pager> {
        todo!()
    }

    fn get_skip_advance(&self) -> bool {
        todo!()
    }
}

impl<Clock: LogicalClock> Debug for MvccLazyCursor<Clock> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvccLazyCursor")
            .field("current_pos", &self.current_pos)
            .field("table_id", &self.table_id)
            .field("tx_id", &self.tx_id)
            .field("reusable_immutable_record", &self.reusable_immutable_record)
            .field("btree_cursor", &())
            .finish()
    }
}
