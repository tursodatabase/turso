use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{MvStore, Row, RowID};
use crate::types::{IOResult, SeekKey, SeekOp, SeekResult};
use crate::Result;
use crate::{Pager, Value};
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
#[derive(Debug)]
pub struct MvccLazyCursor<Clock: LogicalClock> {
    pub db: Arc<MvStore<Clock>>,
    current_pos: CursorPosition,
    table_id: i64,
    tx_id: u64,
}

impl<Clock: LogicalClock> MvccLazyCursor<Clock> {
    pub fn new(
        db: Arc<MvStore<Clock>>,
        tx_id: u64,
        table_id: i64,
        pager: Arc<Pager>,
    ) -> Result<MvccLazyCursor<Clock>> {
        db.maybe_initialize_table(table_id, pager)?;
        let cursor = Self {
            db,
            tx_id,
            current_pos: CursorPosition::BeforeFirst,
            table_id,
        };
        Ok(cursor)
    }

    /// Insert a row into the table.
    /// Sets the cursor to the inserted row.
    pub fn insert(&mut self, row: Row) -> Result<()> {
        self.current_pos = CursorPosition::Loaded(row.id);
        if self.db.read(self.tx_id, row.id)?.is_some() {
            self.db.update(self.tx_id, row).inspect_err(|_| {
                self.current_pos = CursorPosition::BeforeFirst;
            })?;
        } else {
            self.db.insert(self.tx_id, row).inspect_err(|_| {
                self.current_pos = CursorPosition::BeforeFirst;
            })?;
        }
        Ok(())
    }

    pub fn delete(&mut self, rowid: RowID) -> Result<()> {
        self.db.delete(self.tx_id, rowid)?;
        Ok(())
    }

    pub fn current_row_id(&mut self) -> Option<RowID> {
        match self.current_pos {
            CursorPosition::Loaded(id) => Some(id),
            CursorPosition::BeforeFirst => {
                // If we are before first, we need to try and find the first row.
                let maybe_rowid =
                    self.db
                        .get_next_row_id_for_table(self.table_id, i64::MIN, self.tx_id);
                if let Some(id) = maybe_rowid {
                    self.current_pos = CursorPosition::Loaded(id);
                    Some(id)
                } else {
                    self.current_pos = CursorPosition::BeforeFirst;
                    None
                }
            }
            CursorPosition::End => None,
        }
    }

    pub fn current_row(&mut self) -> Result<Option<Row>> {
        match self.current_pos {
            CursorPosition::Loaded(id) => self.db.read(self.tx_id, id),
            CursorPosition::BeforeFirst => {
                // If we are before first, we need to try and find the first row.
                let maybe_rowid =
                    self.db
                        .get_next_row_id_for_table(self.table_id, i64::MIN, self.tx_id);
                if let Some(id) = maybe_rowid {
                    self.current_pos = CursorPosition::Loaded(id);
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

    /// Move the cursor to the next row. Returns true if the cursor moved to the next row, false if the cursor is at the end of the table.
    pub fn forward(&mut self) -> bool {
        let before_first = matches!(self.current_pos, CursorPosition::BeforeFirst);
        let min_id = match self.current_pos {
            CursorPosition::Loaded(id) => id.row_id + 1,
            // TODO: do we need to forward twice?
            CursorPosition::BeforeFirst => i64::MIN, // we need to find first row, so we look from the first id,
            CursorPosition::End => {
                // let's keep same state, we reached the end so no point in moving forward.
                return false;
            }
        };
        self.current_pos =
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
        matches!(self.current_pos, CursorPosition::Loaded(_))
    }

    /// Returns true if the is not pointing to any row.
    pub fn is_empty(&self) -> bool {
        // If we reached the end of the table, it means we traversed the whole table therefore there must be something in the table.
        // If we have loaded a row, it means there is something in the table.
        match self.current_pos {
            CursorPosition::Loaded(_) => false,
            CursorPosition::BeforeFirst => true,
            CursorPosition::End => true,
        }
    }

    pub fn rewind(&mut self) {
        self.current_pos = CursorPosition::BeforeFirst;
    }

    pub fn last(&mut self) {
        let last_rowid = self.db.get_last_rowid(self.table_id);
        if let Some(last_rowid) = last_rowid {
            self.current_pos = CursorPosition::Loaded(RowID {
                table_id: self.table_id,
                row_id: last_rowid,
            });
        } else {
            self.current_pos = CursorPosition::BeforeFirst;
        }
    }

    pub fn get_next_rowid(&mut self) -> i64 {
        self.last();
        match self.current_pos {
            CursorPosition::Loaded(id) => id.row_id + 1,
            CursorPosition::BeforeFirst => 1,
            CursorPosition::End => i64::MAX,
        }
    }

    pub fn seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
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
        let rowid = self.db.seek_rowid(bound, lower_bound, self.tx_id);
        if let Some(rowid) = rowid {
            self.current_pos = CursorPosition::Loaded(rowid);
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
                self.last();
            } else {
                self.rewind();
            }
            Ok(IOResult::Done(SeekResult::NotFound))
        }
    }

    pub fn exists(&mut self, key: &Value) -> Result<IOResult<bool>> {
        let int_key = match key {
            Value::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let exists = self
            .db
            .seek_rowid(
                Bound::Included(&RowID {
                    table_id: self.table_id,
                    row_id: *int_key,
                }),
                true,
                self.tx_id,
            )
            .is_some();
        if exists {
            self.current_pos = CursorPosition::Loaded(RowID {
                table_id: self.table_id,
                row_id: *int_key,
            });
        }
        Ok(IOResult::Done(exists))
    }
}
