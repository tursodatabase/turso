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
    return_if_io,
    storage::btree::{BTreeKey, CursorTrait},
    types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult, Text},
    LimboError, Result, Value,
};

/// One backing row waiting to be inserted.
#[derive(Debug, Clone)]
pub(super) struct PendingRow {
    pub path: String,
    pub chunk_no: i64,
    pub bytes: Vec<u8>,
}

impl PendingRow {
    fn record(&self) -> Result<ImmutableRecord> {
        ImmutableRecord::from_values(
            &[
                Value::Text(Text::new(self.path.clone())),
                Value::from_i64(self.chunk_no),
                Value::from_slice(&self.bytes)?,
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
        &[
            Value::Text(Text::new(path.to_string())),
            Value::from_i64(0),
            Value::Blob(crate::alloc::vec![]),
        ],
        3,
    )
}

#[derive(Debug)]
enum InsertPhase {
    Seeking,
    Inserting { record: ImmutableRecord },
}

/// Insert-only flush: every row is written once with a fresh key, so the
/// machine is pure seek-then-insert. Re-entry after an I/O yield re-seeks.
/// A row whose exact key is already present (a resumed insert) needs no
/// probe here: the B-tree overwrites on an exact index-key match and the
/// MVCC cursor updates the existing version, so re-inserting identical
/// bytes is idempotent by construction.
#[derive(Debug)]
pub(super) struct RowInserter {
    rows: Vec<PendingRow>,
    idx: usize,
    phase: InsertPhase,
}

impl RowInserter {
    pub fn new(rows: Vec<PendingRow>) -> Self {
        Self {
            rows,
            idx: 0,
            phase: InsertPhase::Seeking,
        }
    }

    pub fn step(&mut self, cursor: &mut dyn CursorTrait) -> Result<IOResult<()>> {
        loop {
            if self.idx >= self.rows.len() {
                return Ok(IOResult::Done(()));
            }
            match &mut self.phase {
                InsertPhase::Seeking => {
                    let row = &self.rows[self.idx];
                    let record = row.record()?;
                    // Positioning only: `insert` writes at the cursor's
                    // current position (see the struct docs for why no
                    // existence probe is needed).
                    return_if_io!(cursor.seek(
                        SeekKey::IndexKey(record.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    ));
                    // Insert in a separate phase so an I/O yield does not
                    // repeat the seek.
                    self.phase = InsertPhase::Inserting { record };
                }
                InsertPhase::Inserting { record } => {
                    return_if_io!(cursor.insert(&BTreeKey::IndexKey(record.as_record_ref())));
                    self.idx += 1;
                    self.phase = InsertPhase::Seeking;
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
#[derive(Debug)]
pub(super) struct RowDeleter {
    targets: Vec<PathTarget>,
    idx: usize,
    phase: DeletePhase,
}

impl RowDeleter {
    pub fn new(targets: Vec<PathTarget>) -> Self {
        Self {
            targets,
            idx: 0,
            phase: DeletePhase::Seeking,
        }
    }

    pub fn step(&mut self, cursor: &mut dyn CursorTrait) -> Result<IOResult<()>> {
        loop {
            let Some(target) = self.targets.get(self.idx) else {
                return Ok(IOResult::Done(()));
            };
            match self.phase {
                DeletePhase::Seeking => {
                    let seek_key = seek_key_for_path(target.seek_path())?;
                    let seek_result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(seek_key.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    ));
                    self.phase = match seek_result {
                        SeekResult::NotFound => {
                            self.idx += 1;
                            DeletePhase::Seeking
                        }
                        SeekResult::TryAdvance => DeletePhase::Advancing,
                        SeekResult::Found => DeletePhase::Checking,
                    };
                }
                DeletePhase::Advancing => {
                    return_if_io!(cursor.next());
                    self.phase = if cursor.has_record() {
                        DeletePhase::Checking
                    } else {
                        self.idx += 1;
                        DeletePhase::Seeking
                    };
                }
                DeletePhase::Checking => {
                    if !cursor.has_record() {
                        self.idx += 1;
                        self.phase = DeletePhase::Seeking;
                        continue;
                    }
                    let record = return_if_io!(cursor.record()).ok_or_else(|| {
                        LimboError::Corrupt("FTS cursor has no record payload".into())
                    })?;
                    let path = row_path(record)?;
                    if target.matches(&path) {
                        self.phase = DeletePhase::Deleting;
                    } else {
                        self.idx += 1;
                        self.phase = DeletePhase::Seeking;
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
