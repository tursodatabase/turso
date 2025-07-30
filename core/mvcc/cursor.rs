use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{MvStore, Result, Row, RowID};
use std::fmt::Debug;
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
    table_id: u64,
    tx_id: u64,
}

impl<Clock: LogicalClock> MvccLazyCursor<Clock> {
    pub fn new(
        db: Arc<MvStore<Clock>>,
        tx_id: u64,
        table_id: u64,
    ) -> Result<MvccLazyCursor<Clock>> {
        Ok(Self {
            db,
            tx_id,
            current_pos: CursorPosition::BeforeFirst,
            table_id,
        })
    }

    /// Insert a row into the table.
    /// Sets the cursor to the inserted row.
    pub fn insert(&mut self, row: Row) -> Result<()> {
        self.current_pos = CursorPosition::Loaded(row.id);
        self.db.insert(self.tx_id, row).inspect_err(|_| {
            self.current_pos = CursorPosition::BeforeFirst;
        })?;
        Ok(())
    }

    pub fn current_row_id(&self) -> Option<RowID> {
        match self.current_pos {
            CursorPosition::Loaded(id) => Some(id),
            CursorPosition::BeforeFirst => None,
            CursorPosition::End => None,
        }
    }

    pub fn current_row(&self) -> Result<Option<Row>> {
        match self.current_pos {
            CursorPosition::Loaded(id) => self.db.read(self.tx_id, id),
            CursorPosition::BeforeFirst => Ok(None),
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
            CursorPosition::BeforeFirst => i64::MIN, // we need to find first row, so we look from the first id
            CursorPosition::End => {
                // let's keep same state, we reached the end so no point in moving forward.
                return false;
            }
        };
        self.current_pos = match self.db.get_next_row_id_for_table(self.table_id, min_id) {
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
}
