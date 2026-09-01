//! Resumable row-level operations on the FTS backing B-tree.
//!
//! The backing store is one index B-tree whose complete key is the record
//! `(path TEXT, chunk_no INTEGER, bytes BLOB)`. Everything the segment
//! registry does reduces to two primitives, each implemented as a small
//! state machine that can be re-entered after an `IOResult::IO` yield
//! without repeating a mutation:
//!
//! * [`RowInserter`] — insert a batch of rows (segment descriptors, chunks,
//!   tombstones, the control row). Publishing a segment is nothing more
//!   than inserting its rows; nothing shared is rewritten.
//! * [`RowDeleter`] — delete every row matching a list of path targets.
//!   Only merge/OPTIMIZE deletes rows.

use crate::{
    numeric::Numeric,
    return_if_io,
    storage::btree::{BTreeKey, CursorTrait},
    types::{
        IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult, TextRef, TextSubtype, ValueRef,
    },
    LimboError, Result,
};

/// One backing row waiting to be inserted.
#[derive(Debug, Clone)]
pub(super) struct PendingRow {
    pub path: String,
    pub chunk_no: i64,
    pub bytes: Vec<u8>,
}

impl PendingRow {
    /// Serialize the row straight from its fields: the record buffer is the
    /// only copy the path and chunk bytes take.
    fn record(&self) -> Result<ImmutableRecord> {
        ImmutableRecord::from_values(
            [
                ValueRef::Text(TextRef::new(&self.path, TextSubtype::Text)),
                ValueRef::Numeric(Numeric::Integer(self.chunk_no)),
                ValueRef::Blob(&self.bytes),
            ],
            3,
        )
    }
}

/// Split one file into `(path, chunk_no, bytes)` rows of at most
/// `chunk_size` bytes. An empty file still gets one empty chunk so the row
/// exists.
pub(super) fn chunk_rows(path: &str, data: &[u8], chunk_size: usize) -> Vec<PendingRow> {
    let num_chunks = data.len().div_ceil(chunk_size).max(1);
    (0..num_chunks)
        .map(|chunk_no| {
            let start = chunk_no * chunk_size;
            let end = (start + chunk_size).min(data.len());
            PendingRow {
                path: path.to_string(),
                chunk_no: chunk_no as i64,
                bytes: data[start..end].to_vec(),
            }
        })
        .collect()
}

/// Extract `(path, chunk_no, bytes)` from the cursor's current record.
pub(super) fn row_fields(record: &ImmutableRecord) -> Result<(String, i64, Vec<u8>)> {
    let path = record
        .get_value_opt(0)
        .and_then(|value| match value {
            crate::types::ValueRef::Text(text) => Some(text.value.to_string()),
            _ => None,
        })
        .ok_or_else(|| LimboError::Corrupt("FTS row path is not text".into()))?;
    let chunk_no = record
        .get_value_opt(1)
        .and_then(|value| match value {
            crate::types::ValueRef::Numeric(crate::numeric::Numeric::Integer(value)) => Some(value),
            _ => None,
        })
        .ok_or_else(|| LimboError::Corrupt("FTS row chunk number is not an integer".into()))?;
    let bytes = record
        .get_value_opt(2)
        .and_then(|value| match value {
            crate::types::ValueRef::Blob(blob) => Some(blob.to_vec()),
            _ => None,
        })
        .ok_or_else(|| LimboError::Corrupt("FTS row payload is not a blob".into()))?;
    Ok((path, chunk_no, bytes))
}

/// Extract only the path from the cursor's current record.
fn row_path(record: &ImmutableRecord) -> Result<String> {
    record
        .get_value_opt(0)
        .and_then(|value| match value {
            crate::types::ValueRef::Text(text) => Some(text.value.to_string()),
            _ => None,
        })
        .ok_or_else(|| LimboError::Corrupt("FTS row path is not text".into()))
}

/// Seek key positioned at (or before) the first possible row for `path`.
pub(super) fn seek_key_for_path(path: &str) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        [
            ValueRef::Text(TextRef::new(path, TextSubtype::Text)),
            ValueRef::Numeric(Numeric::Integer(0)),
            ValueRef::Blob(&[]),
        ],
        3,
    )
}

/// The row in flight, with its serialized key. The record lives in the
/// phase because the cursor's seek and insert state machines are resumed
/// with the same key after an I/O yield: the key must survive the yield,
/// not be rebuilt on every re-entry.
#[derive(Debug)]
enum InsertPhase {
    Seeking {
        record: ImmutableRecord,
    },
    /// The seek stopped one leaf early (`SeekResult::TryAdvance`): a stale
    /// interior divider can navigate the cursor to a leaf that no longer
    /// holds the seek boundary. `insert` only checks the cursor's current
    /// cell for an exact-key overwrite, so inserting from this position
    /// would add a second physical copy of a key that already exists at the
    /// start of the next leaf. Advance once before inserting, exactly like
    /// the VDBE's `seek_internal` does.
    Advancing {
        record: ImmutableRecord,
    },
    Inserting {
        record: ImmutableRecord,
    },
}

/// Insert-only flush: every row is written once with a fresh key, so the
/// machine is seek-then-insert. A row whose exact key is already present
/// (a resumed insert) is overwritten in place — but only if the cursor is
/// actually positioned on the equal cell when `insert` runs. A seek that
/// stops one leaf early (`TryAdvance`) must advance first, or the insert
/// adds a second physical copy of the key (see [`InsertPhase::Advancing`]).
#[derive(Debug)]
pub(super) struct RowInserter {
    rows: Vec<PendingRow>,
    idx: usize,
    /// `None` between rows; the record is built once per row.
    phase: Option<InsertPhase>,
}

impl RowInserter {
    pub fn new(rows: Vec<PendingRow>) -> Self {
        Self {
            rows,
            idx: 0,
            phase: None,
        }
    }

    pub fn step(&mut self, cursor: &mut dyn CursorTrait) -> Result<IOResult<()>> {
        loop {
            match &mut self.phase {
                None => {
                    let Some(row) = self.rows.get(self.idx) else {
                        return Ok(IOResult::Done(()));
                    };
                    self.phase = Some(InsertPhase::Seeking {
                        record: row.record()?,
                    });
                }
                Some(InsertPhase::Seeking { record }) => {
                    // `eq_only: true`: `NotFound` positions the cursor at
                    // the exact slot where the key belongs (the fresh-key
                    // case), `Found` lands on the equal cell (the resumed
                    // re-insert case, overwritten in place). `TryAdvance`
                    // means the equal key may sit at the start of the next
                    // leaf behind a stale interior divider: advancing before
                    // the insert reaches it, so the overwrite happens
                    // instead of a second physical copy of the key.
                    let seek_result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: true },
                    ));
                    // Insert in a separate phase so an I/O yield does not
                    // repeat the seek.
                    let Some(InsertPhase::Seeking { record }) = self.phase.take() else {
                        unreachable!("phase matched above");
                    };
                    self.phase = Some(match seek_result {
                        SeekResult::TryAdvance => InsertPhase::Advancing { record },
                        _ => InsertPhase::Inserting { record },
                    });
                }
                Some(InsertPhase::Advancing { .. }) => {
                    return_if_io!(cursor.next());
                    let Some(InsertPhase::Advancing { record }) = self.phase.take() else {
                        unreachable!("phase matched above");
                    };
                    self.phase = Some(InsertPhase::Inserting { record });
                }
                Some(InsertPhase::Inserting { record }) => {
                    return_if_io!(cursor.insert(&BTreeKey::IndexKey(record.as_record_ref())));
                    self.idx += 1;
                    self.phase = None;
                }
            }
        }
    }
}

/// What rows a [`RowDeleter`] target matches.
#[derive(Debug, Clone)]
pub(super) enum PathTarget {
    /// Every row whose path equals this string (all chunk numbers).
    Exact(String),
    /// Every row whose path starts with this string.
    Prefix(String),
}

impl PathTarget {
    fn seek_path(&self) -> &str {
        match self {
            Self::Exact(path) | Self::Prefix(path) => path,
        }
    }

    fn matches(&self, path: &str) -> bool {
        match self {
            Self::Exact(target) => path == target,
            Self::Prefix(prefix) => path.starts_with(prefix.as_str()),
        }
    }
}

#[derive(Debug)]
enum DeletePhase {
    Seeking,
    Advancing,
    Checking,
    Deleting,
}

/// Delete every row matching each target, one target at a time.
///
/// After every deletion the machine re-seeks from the target's logical
/// prefix: B-tree deletion may retreat, preserve, or advance the physical
/// cursor depending on balancing, so `next()` could skip a sibling row.
/// The seek key is built once per target and kept until the target is
/// done: the cursor's seek state machine is resumed with the same key
/// after an I/O yield, and every re-seek after a deletion uses it again.
#[derive(Debug)]
pub(super) struct RowDeleter {
    targets: Vec<PathTarget>,
    idx: usize,
    seek_key: Option<ImmutableRecord>,
    phase: DeletePhase,
}

impl RowDeleter {
    pub fn new(targets: Vec<PathTarget>) -> Self {
        Self {
            targets,
            idx: 0,
            seek_key: None,
            phase: DeletePhase::Seeking,
        }
    }

    /// Move on to the next target; its seek key is built on first use.
    fn next_target(&mut self) {
        self.idx += 1;
        self.seek_key = None;
        self.phase = DeletePhase::Seeking;
    }

    pub fn step(&mut self, cursor: &mut dyn CursorTrait) -> Result<IOResult<()>> {
        loop {
            let Some(target) = self.targets.get(self.idx) else {
                return Ok(IOResult::Done(()));
            };
            match self.phase {
                DeletePhase::Seeking => {
                    let seek_key = match &self.seek_key {
                        Some(seek_key) => seek_key,
                        None => self.seek_key.insert(seek_key_for_path(target.seek_path())?),
                    };
                    let seek_result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(seek_key.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    ));
                    match seek_result {
                        SeekResult::NotFound => self.next_target(),
                        SeekResult::TryAdvance => self.phase = DeletePhase::Advancing,
                        SeekResult::Found => self.phase = DeletePhase::Checking,
                    }
                }
                DeletePhase::Advancing => {
                    return_if_io!(cursor.next());
                    if cursor.has_record() {
                        self.phase = DeletePhase::Checking;
                    } else {
                        self.next_target();
                    }
                }
                DeletePhase::Checking => {
                    if !cursor.has_record() {
                        self.next_target();
                        continue;
                    }
                    let record = return_if_io!(cursor.record()).ok_or_else(|| {
                        LimboError::Corrupt("FTS cursor has no record payload".into())
                    })?;
                    let path = row_path(record)?;
                    if target.matches(&path) {
                        self.phase = DeletePhase::Deleting;
                    } else {
                        self.next_target();
                    }
                }
                DeletePhase::Deleting => {
                    return_if_io!(cursor.delete());
                    self.phase = DeletePhase::Seeking;
                }
            }
        }
    }
}
