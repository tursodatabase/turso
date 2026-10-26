//! Column patches that let the logical log store an UPDATE of a table row as one
//! `OP_UPDATE_TABLE` op, instead of a delete op and an upsert op with the full
//! record.
//!
//! In MVCC mode an UPDATE is a `Delete` and then an `Insert` of the same rowid on
//! the same cursor. The delete gives an [`UpdateBase`]: the record that it ended.
//! The insert makes the patch from that record to the new record, and the
//! transaction keeps the patch in [`UpdateDeltas`] until commit.
//!
//! Each patch starts at a committed record: a version of another transaction, or
//! the B-tree row. When a transaction updates its own new version again, the two
//! patches are merged, so the patch still starts at the committed record. A patch
//! is tied to the id of the committed version (or B-tree tombstone) that the
//! transaction ended and to the id of the new version. The commit uses a patch
//! only when both ids are the ids of the versions that it logs. Thus a patch whose
//! versions a savepoint rollback removed is never used.

use super::{MVTableId, RowID, RowKey, RowVersion, TxTimestampOrID};
use crate::mvcc::column_delta;
use crate::types::ImmutableRecord;
use crate::Result;
use rustc_hash::FxHashMap as HashMap;

/// The record that a table-row delete ended. The cursor keeps it until the
/// insert of the same rowid.
pub(crate) struct UpdateBase {
    rowid: i64,
    /// The id of the version that the delete ended, or of the B-tree tombstone
    /// that the delete added.
    version_id: u64,
    record: BaseRecord,
    /// True when the ended version is a version that this transaction made.
    made_by_tx: bool,
}

pub(crate) enum BaseRecord {
    Version(crate::alloc::ArcSlice<u8>),
    BTree(ImmutableRecord),
}

impl BaseRecord {
    fn bytes(&self) -> &[u8] {
        match self {
            BaseRecord::Version(bytes) => bytes,
            BaseRecord::BTree(record) => record.get_payload(),
        }
    }
}

impl UpdateBase {
    pub(crate) fn new(rowid: i64, version_id: u64, record: BaseRecord, made_by_tx: bool) -> Self {
        Self {
            rowid,
            version_id,
            record,
            made_by_tx,
        }
    }

    pub(crate) fn rowid(&self) -> i64 {
        self.rowid
    }
}

/// The update patches of one transaction.
#[derive(Debug, Default)]
pub(crate) struct UpdateDeltas {
    by_row: HashMap<(MVTableId, i64), UpdateDelta>,
    /// All patches, one after another. A new patch of a row goes into the bytes
    /// of the old patch of the row when it fits. Else it goes to the end, and the
    /// old bytes stay unused until the transaction ends.
    patches: Vec<u8>,
    new_patch: Vec<u8>,
    merged_patch: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct UpdateDelta {
    base_version_id: u64,
    new_version_id: u64,
    base_crc32c: u32,
    patch_start: usize,
    patch_len: usize,
}

impl UpdateDeltas {
    /// Keeps the patch from the committed record of the row to `new_record`,
    /// which is the record of version `new_version_id`.
    pub(crate) fn record(
        &mut self,
        table_id: MVTableId,
        base: UpdateBase,
        new_version_id: u64,
        new_record: &[u8],
    ) -> Result<()> {
        let key = (table_id, base.rowid);
        let earlier = if base.made_by_tx {
            match self.by_row.get(&key) {
                Some(earlier) if earlier.new_version_id == base.version_id => Some(*earlier),
                _ => {
                    self.by_row.remove(&key);
                    return Ok(());
                }
            }
        } else {
            None
        };
        if !column_delta::encode(base.record.bytes(), new_record, &mut self.new_patch)? {
            self.by_row.remove(&key);
            return Ok(());
        }
        let (base_version_id, base_crc32c) = match earlier {
            Some(earlier) => {
                column_delta::merge(
                    &self.patches[earlier.patch_start..][..earlier.patch_len],
                    &self.new_patch,
                    &mut self.merged_patch,
                )?;
                std::mem::swap(&mut self.new_patch, &mut self.merged_patch);
                (earlier.base_version_id, earlier.base_crc32c)
            }
            None => (base.version_id, crc32c::crc32c(base.record.bytes())),
        };
        // A patch of more than half the record saves too little to be worth the
        // extra work of recovery.
        if self.new_patch.len() * 2 > new_record.len() {
            self.by_row.remove(&key);
            return Ok(());
        }
        // The old patch of the row can be the patch that `earlier` merged, or a
        // patch whose new version a savepoint rollback removed.
        let patch_start = match self.by_row.get(&key) {
            Some(old) if self.new_patch.len() <= old.patch_len => old.patch_start,
            _ => self.patches.len(),
        };
        let patch_end = patch_start + self.new_patch.len();
        if patch_end > self.patches.len() {
            self.patches.resize(patch_end, 0);
        }
        self.patches[patch_start..patch_end].copy_from_slice(&self.new_patch);
        self.by_row.insert(
            key,
            UpdateDelta {
                base_version_id,
                new_version_id,
                base_crc32c,
                patch_start,
                patch_len: self.new_patch.len(),
            },
        );
        Ok(())
    }

    /// Returns the CRC32C of the base record and the patch that turns the
    /// record of `old` into the record of `new`, when this transaction made one.
    ///
    /// `old` and `new` are the committed images that the commit logs for the
    /// write-set entry `id`: `old` ends at `end_ts`, and `new` begins at `end_ts`.
    pub(crate) fn find(
        &self,
        id: &RowID,
        old: &RowVersion,
        new: &RowVersion,
        end_ts: u64,
    ) -> Option<(u32, &[u8])> {
        let RowKey::Int(rowid) = id.row_id else {
            return None;
        };
        let delta = self.by_row.get(&(id.table_id, rowid))?;
        let committed_at_end_ts = |ts: Option<TxTimestampOrID>| matches!(ts, Some(TxTimestampOrID::Timestamp(ts)) if ts == end_ts);
        let matches = delta.base_version_id == old.id
            && delta.new_version_id == new.id
            && committed_at_end_ts(old.end())
            && !committed_at_end_ts(old.begin())
            && committed_at_end_ts(new.begin())
            && new.end().is_none();
        matches.then(|| {
            (
                delta.base_crc32c,
                &self.patches[delta.patch_start..][..delta.patch_len],
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value_blob_from_slice;
    use crate::Value;

    fn record(n: i64) -> ImmutableRecord {
        let values = [
            Value::from_i64(n),
            Value::from_blob(value_blob_from_slice(&[7; 300]).unwrap()),
        ];
        ImmutableRecord::from_values(&values, values.len()).unwrap()
    }

    /// `SAVEPOINT s; UPDATE ...; ROLLBACK TO s;` in a loop: each UPDATE starts
    /// again at the committed record, and its patch replaces the patch of the
    /// rolled-back UPDATE.
    #[test]
    fn patch_after_savepoint_rollback_uses_the_bytes_of_the_old_patch() {
        let table_id = MVTableId::new(-2);
        let mut deltas = UpdateDeltas::default();
        for n in 10..110 {
            let base = UpdateBase::new(1, 5, BaseRecord::BTree(record(0)), false);
            deltas
                .record(table_id, base, 100 + n as u64, record(n).get_payload())
                .unwrap();
        }
        let delta = deltas.by_row[&(table_id, 1)];
        assert_eq!(delta.new_version_id, 209);
        assert_eq!(deltas.patches.len(), delta.patch_len);
    }
}
