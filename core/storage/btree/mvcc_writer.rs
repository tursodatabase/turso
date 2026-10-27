use super::mvcc_reader::{
    compare_index_keys, HeldRowidLock, IndexKeys, MvccKeys, MvccReader, Row, RowFrom,
    TableChainRow, TableKeys,
};
use super::{BTreeCursor, BTreeKey, CursorTrait};
use crate::mvcc::database::{Row as VersionRow, RowID, RowKey, SortableIndexKey};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult, IOResultOr, ImmutableRecord, ImmutableRecordRef};
use crate::{return_if_io, turso_assert, Completion, LimboError, MvStore, Result};

pub(crate) enum NextRowidResult {
    /// The allocator has no largest rowid yet. The caller reads it from the
    /// last row of the table and gives it to `mvcc_initialize_max_rowid`.
    Uninitialized,
    Next {
        new_rowid: i64,
        prev_rowid: Option<i64>,
    },
    /// The rowids reached `i64::MAX`, so the caller tries random rowids.
    FindRandom,
}

impl BTreeCursor {
    pub(super) fn mvcc_insert(&mut self, key: &BTreeKey) -> IOResultOr<()> {
        match key {
            BTreeKey::TableRowId((rowid, record)) => {
                let record = record.ok_or_else(|| {
                    LimboError::InternalError("an MVCC table insert needs a record".to_string())
                })?;
                self.mvcc_insert_table_row(*rowid, record)?;
            }
            BTreeKey::IndexKey(record) => self.mvcc_insert_index_key(record)?,
        }
        Ok(IOResult::Done(()))
    }

    pub(super) fn mvcc_delete(&mut self) -> IOResultOr<()> {
        let from_cell = match self.mvcc_row_from() {
            Some(from_cell) => from_cell,
            None => {
                return Err(
                    LimboError::InternalError("Cannot delete: no current row".to_string()).into(),
                )
            }
        };
        // The version store keeps the bytes of a B-tree row that a delete
        // hides. Read them before the delete, which has no IO.
        let btree_record = if from_cell {
            let record = return_if_io!(self.record());
            turso_assert!(
                record.is_some(),
                "a cursor on a B-tree cell must have the record of the cell"
            );
            self.reusable_immutable_record.take()
        } else {
            None
        };
        let result = if self.mvcc_table_reader.is_some() {
            self.mvcc_delete_table_row(btree_record.as_ref())
        } else {
            self.mvcc_delete_index_key(btree_record.as_ref())
        };
        if btree_record.is_some() {
            self.reusable_immutable_record = btree_record;
        }
        if let Err(err) = result {
            self.has_record = false;
            self.forget_mvcc_position();
            return Err(err.into());
        }
        Ok(IOResult::Done(()))
    }

    /// `SeekEnd` positions a cursor before an insert. An MVCC insert writes
    /// the version store, which needs no position.
    pub(super) fn mvcc_seek_end(&mut self) {
        self.has_record = false;
        self.forget_mvcc_position();
    }

    pub(crate) fn mvcc_start_new_rowid(&mut self) -> IOResultOr<NextRowidResult> {
        let reader = TableKeys::reader_mut(self);
        turso_assert!(
            reader.keys.held_rowid_lock.is_none(),
            "the cursor already holds the rowid allocator lock"
        );
        let allocator = reader.store.get_rowid_allocator(&reader.table_id);
        if !allocator.lock() {
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        }
        let result = if allocator.is_uninitialized() {
            NextRowidResult::Uninitialized
        } else if let Some((new_rowid, prev_rowid)) = allocator.get_next_rowid() {
            NextRowidResult::Next {
                new_rowid,
                prev_rowid,
            }
        } else {
            NextRowidResult::FindRandom
        };
        reader.keys.held_rowid_lock = Some(HeldRowidLock(allocator));
        Ok(IOResult::Done(result))
    }

    pub(crate) fn mvcc_initialize_max_rowid(&mut self, max_rowid: Option<i64>) {
        let lock = self.held_rowid_lock();
        lock.0.initialize(max_rowid);
    }

    pub(crate) fn mvcc_allocate_next_rowid(&mut self) -> Option<(i64, Option<i64>)> {
        let lock = self.held_rowid_lock();
        lock.0.get_next_rowid()
    }

    /// Releases the rowid allocator lock, if the cursor holds it.
    pub(crate) fn mvcc_end_new_rowid(&mut self) {
        if let Some(reader) = self.mvcc_table_reader.as_deref_mut() {
            reader.keys.held_rowid_lock = None;
        }
    }

    pub(crate) fn has_mvcc_table_reader(&self) -> bool {
        self.mvcc_table_reader.is_some()
    }

    fn held_rowid_lock(&self) -> &HeldRowidLock {
        TableKeys::reader(self)
            .keys
            .held_rowid_lock
            .as_ref()
            .expect("the cursor must hold the rowid allocator lock")
    }

    /// Whether the current row of the cursor is a cell, or `None` when the
    /// cursor has no row.
    fn mvcc_row_from(&self) -> Option<bool> {
        if let Some(reader) = self.mvcc_table_reader.as_deref() {
            return reader.row_is_cell();
        }
        IndexKeys::reader(self).row_is_cell()
    }

    fn mvcc_insert_table_row(&mut self, rowid: i64, record: &ImmutableRecord) -> Result<()> {
        let reader = TableKeys::reader_mut(self);
        turso_assert!(reader.is_idle(), "an MVCC cursor writes between its moves");
        let row_id = RowID::new(reader.table_id, RowKey::Int(rowid));
        let row = crate::with_mv_store_allocation_site!(
            RowPayload,
            VersionRow::new_table_row_in(
                row_id,
                record.get_payload(),
                record.column_count(),
                reader.store.allocator(),
            )
        )?;
        let payload = row
            .data
            .clone()
            .expect("a table row version must have a payload");
        let at_row = reader.row_is_cell().is_some() && reader.row_rowid() == rowid;
        let on_btree_row = at_row && reader.row_is_cell() == Some(true);
        let result =
            reader.write_at_row(|store, tx_id| write_row(store, tx_id, row, None, on_btree_row));
        if let Err(err) = result {
            self.has_record = false;
            self.forget_mvcc_position();
            return Err(err);
        }
        let reader = TableKeys::reader_mut(self);
        reader.set_written_row(TableChainRow { rowid, payload }, at_row);
        self.settle_mvcc_row(false, true);
        Ok(())
    }

    fn mvcc_insert_index_key(&mut self, record: &ImmutableRecordRef<'_>) -> Result<()> {
        let reader = IndexKeys::reader_mut(self);
        turso_assert!(reader.is_idle(), "an MVCC cursor writes between its moves");
        let key = Arc::new(SortableIndexKey::new_from_payload_in(
            record.get_payload(),
            reader.keys.index_info.clone(),
            reader.store.allocator(),
        )?);
        let row_id = RowID::new(reader.table_id, RowKey::Record(key.clone()));
        let row = VersionRow::new_index_row(row_id, record.column_count());
        let at_row = match reader.row_key_payload() {
            Some(row_key) => {
                let info = reader.keys.index_info.as_ref();
                compare_index_keys(
                    row_key,
                    record.get_payload(),
                    &info.key_info[..info.num_cols],
                )?
                .is_eq()
            }
            None => false,
        };
        let on_btree_row = at_row && reader.row_is_cell() == Some(true);
        let index_id = reader.table_id;
        let result = reader.write_at_row(|store, tx_id| {
            write_row(store, tx_id, row, Some(index_id), on_btree_row)
        });
        if let Err(err) = result {
            self.has_record = false;
            self.forget_mvcc_position();
            return Err(err);
        }
        let reader = IndexKeys::reader_mut(self);
        reader.set_written_row(key, at_row);
        self.settle_mvcc_row(false, true);
        Ok(())
    }

    fn mvcc_delete_table_row(&mut self, btree_record: Option<&ImmutableRecord>) -> Result<()> {
        let reader = TableKeys::reader_mut(self);
        turso_assert!(reader.is_idle(), "an MVCC cursor writes between its moves");
        let row_id = RowID::new(reader.table_id, RowKey::Int(reader.row_rowid()));
        reader.write_at_row(|store, tx_id| {
            store.delete_from_table_or_index(tx_id, row_id, None, btree_record)
        })?;
        Ok(())
    }

    fn mvcc_delete_index_key(&mut self, btree_record: Option<&ImmutableRecord>) -> Result<()> {
        let reader = IndexKeys::reader_mut(self);
        turso_assert!(reader.is_idle(), "an MVCC cursor writes between its moves");
        let key = IndexKeys::row_seek_key(reader)?;
        let index_id = reader.table_id;
        let row_id = RowID::new(index_id, RowKey::Record(key));
        reader.write_at_row(|store, tx_id| {
            store.delete_from_table_or_index(tx_id, row_id, Some(index_id), btree_record)
        })?;
        Ok(())
    }
}

/// Writes `row` as its transaction sees it: a new version over a visible
/// one, a version over a B-tree row that has no version yet, or a new row.
fn write_row(
    store: &MvStore,
    tx_id: u64,
    row: VersionRow,
    index_id: Option<crate::mvcc::database::MVTableId>,
    on_btree_row: bool,
) -> Result<()> {
    if store
        .read_from_table_or_index(tx_id, &row.id, index_id)?
        .is_some()
    {
        let updated = store.update_to_table_or_index(tx_id, row, index_id)?;
        turso_assert!(
            updated,
            "read found a visible version but update could not supersede it"
        );
    } else if on_btree_row {
        store.insert_btree_resident_to_table_or_index(tx_id, row, index_id)?;
    } else {
        store.insert_to_table_or_index(tx_id, row, index_id)?;
    }
    Ok(())
}

impl<K: MvccKeys> MvccReader<K> {
    /// Runs a write of the cursor at its own row. The write changes only the
    /// version chain of that row, which the cursor has passed, so the cursor
    /// does not position its chain iterator again because of it.
    fn write_at_row<T>(&mut self, write: impl FnOnce(&MvStore, u64) -> Result<T>) -> Result<T> {
        let writes_before = self.own_table_writes.get();
        let result = write(&self.store, self.snapshot.tx_id);
        if self.own_table_writes_seen == writes_before {
            self.own_table_writes_seen = self.own_table_writes.get();
        }
        result
    }

    /// The row is the one that the cursor wrote. When the write is at the
    /// key of the current row, the cell and the chain positions stay valid.
    fn set_written_row(&mut self, written: K::ChainRow, at_row: bool) {
        self.row = match self.row {
            Row::At {
                cell_has_key,
                chain_has_key,
                ..
            } if at_row => Row::At {
                from: RowFrom::Chain(written),
                cell_has_key,
                chain_has_key,
            },
            _ => Row::Written(written),
        };
    }
}
