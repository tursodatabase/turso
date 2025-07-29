use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{MvStore, Result, Row, RowID};
use crate::turso_assert;
use std::rc::Rc;

pub struct ScanCursor<Clock: LogicalClock> {
    pub db: Rc<MvStore<Clock>>,
    pub row_ids: Vec<RowID>,
    pub index: usize,
    tx_id: u64,
    pager: Rc<crate::storage::pager::Pager>,
}

impl<Clock: LogicalClock> ScanCursor<Clock> {
    pub fn new(
        db: Rc<MvStore<Clock>>,
        tx_id: u64,
        table_id: u64,
        pager: Rc<crate::storage::pager::Pager>,
    ) -> Result<ScanCursor<Clock>> {
        let row_ids = db.scan_row_ids_for_table(table_id, pager.clone())?;
        Ok(Self {
            db,
            tx_id,
            row_ids,
            index: 0,
            pager,
        })
    }

    pub fn rewind(&mut self) {
        self.index = 0;
    }

    pub fn insert(&self, row: Row) -> Result<()> {
        self.db.insert(self.tx_id, row)
    }

    pub fn current_row_id(&self) -> Option<RowID> {
        turso_assert!(self.index > 0, "index must be greater than zero");
        let idx = self.index - 1;
        if idx >= self.row_ids.len() {
            return None;
        }
        Some(self.row_ids[idx])
    }

    pub fn current_row(&self) -> Result<Option<Row>> {
        turso_assert!(self.index > 0, "index must be greater than zero");
        let idx = self.index - 1;
        if idx >= self.row_ids.len() {
            return Ok(None);
        }
        let id = self.row_ids[idx];
        self.db.read(self.tx_id, id, self.pager.clone())
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn forward(&mut self) -> bool {
        self.index += 1;
        let idx = self.index - 1;
        idx < self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index >= self.row_ids.len()
    }
}

pub struct LazyScanCursor<Clock: LogicalClock> {
    pub db: Rc<MvStore<Clock>>,
    pub current_pos: Option<RowID>,
    pub prev_pos: Option<RowID>,
    table_id: u64,
    tx_id: u64,
    pager: Rc<crate::storage::pager::Pager>,
}

impl<Clock: LogicalClock> LazyScanCursor<Clock> {
    pub fn new(
        db: Rc<MvStore<Clock>>,
        tx_id: u64,
        table_id: u64,
        pager: Rc<crate::storage::pager::Pager>,
    ) -> Result<LazyScanCursor<Clock>> {
        let current_pos = db.get_next_row_id_for_table(table_id, 0);
        Ok(Self {
            db,
            tx_id,
            current_pos,
            prev_pos: None,
            table_id,
            pager,
        })
    }

    pub fn insert(&self, row: Row) -> Result<()> {
        self.db.insert(self.tx_id, row)
    }

    pub fn current_row_id(&self) -> Option<RowID> {
        self.current_pos
    }

    pub fn current_row(&self) -> Result<Option<Row>> {
        if let Some(id) = self.current_pos {
            self.db.read(self.tx_id, id, self.pager.clone())
        } else {
            Ok(None)
        }
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn forward(&mut self) -> bool {
        self.prev_pos = self.current_pos;
        if let Some(row_id) = self.prev_pos {
            let next_id = row_id.row_id + 1;
            self.current_pos = self.db.get_next_row_id_for_table(self.table_id, next_id);
            println!("{:?}", self.current_pos);
            self.current_pos.is_some()
        } else {
            false
        }
    }

    pub fn is_empty(&self) -> bool {
        self.current_pos.is_none()
    }
}

pub struct BucketScanCursor<Clock: LogicalClock> {
    pub db: Rc<MvStore<Clock>>,
    pub bucket: Vec<RowID>,
    bucket_size: u64,
    table_id: u64,
    tx_id: u64,
    index: usize,
    pager: Rc<crate::storage::pager::Pager>,
}

impl<Clock: LogicalClock> BucketScanCursor<Clock> {
    pub fn new(
        db: Rc<MvStore<Clock>>,
        tx_id: u64,
        table_id: u64,
        size: u64,
        pager: Rc<crate::storage::pager::Pager>,
    ) -> Result<BucketScanCursor<Clock>> {
        let mut bucket = Vec::with_capacity(size as usize);
        db.get_row_id_range(table_id, 0, &mut bucket, size)?;
        Ok(Self {
            db,
            tx_id,
            bucket,
            bucket_size: size,
            table_id,
            index: 0,
            pager,
        })
    }

    pub fn insert(&self, row: Row) -> Result<()> {
        self.db.insert(self.tx_id, row)
    }

    pub fn current_row_id(&self) -> Option<RowID> {
        if self.index >= self.bucket.len() {
            return None;
        }
        Some(self.bucket[self.index])
    }

    pub fn current_row(&self) -> Result<Option<Row>> {
        if self.index >= self.bucket.len() {
            return Ok(None);
        }
        let id = self.bucket[self.index];
        self.db.read(self.tx_id, id, self.pager.clone())
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn forward(&mut self) -> bool {
        self.index += 1;
        if self.index < self.bucket.len() {
            true
        } else {
            self.next_bucket().unwrap_or_default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.index >= self.bucket.len()
    }

    fn next_bucket(&mut self) -> Result<bool> {
        let last_id = if !self.bucket.is_empty() {
            Some(self.bucket[self.bucket.len() - 1].row_id + 1)
        } else {
            None
        };

        self.bucket.clear();

        if let Some(next_id) = last_id {
            self.db
                .get_row_id_range(self.table_id, next_id, &mut self.bucket, self.bucket_size)?;

            self.index = 0;
            Ok(!self.bucket.is_empty())
        } else {
            Ok(false)
        }
    }
}
