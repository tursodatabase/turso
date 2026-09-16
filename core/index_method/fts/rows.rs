//! Resumable row-level operations on the FTS backing B-tree.
//!
//! The backing store is one index B-tree whose complete key is the record
//! `(path TEXT, chunk_no INTEGER, bytes BLOB)`. Everything the segment
//! registry does reduces to two primitives, each implemented as an async
//! function that resumes after an `IOResult::IO` yield without repeating
//! a mutation:
//!
//! * [`RowInserter`] — insert a batch of rows (segment descriptors, chunks,
//!   tombstones, the control row). Publishing a segment is nothing more
//!   than inserting its rows; nothing shared is rewritten.
//! * [`RowDeleter`] — delete every row matching a list of path targets.
//!   Only merge/OPTIMIZE deletes rows.
//! * [`SegmentClaimer`] — delete the registry row of each segment a merge
//!   wants, and record which segments this transaction got.

use super::format::segment_registry_path;
use crate::coro::{with_handle, BoxedResumable, Co, Runner, StepContext, YieldSlot};
use crate::types::{IOCompletions, IOResultOr};
use crate::{
    numeric::Numeric,
    storage::btree::{BTreeKey, CursorTrait},
    types::{
        IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult, TextRef, TextSubtype, ValueRef,
    },
    LimboError, Result,
};
use std::marker::PhantomData;
use tantivy::index::SegmentId;

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

/// Names [`RowCtx`] as the context type of the async row operations that
/// `O` owns.
struct RowStep<O>(PhantomData<O>);

impl<O: 'static> StepContext for RowStep<O> {
    type Error = Box<LimboError>;
    type Ctx<'a> = RowCtx<'a, O>;
}

/// The context of one step of an async row operation: the owner of the
/// operation and the cursor of that step. The async function gets it back
/// on every step, so it never keeps the cursor across a yield.
struct RowCtx<'a, O> {
    owner: &'a mut O,
    cursor: &'a mut dyn CursorTrait,
    io: Option<IOCompletions>,
    err: Option<Box<LimboError>>,
}

impl<O> YieldSlot<Box<LimboError>> for RowCtx<'_, O> {
    fn park_io(&mut self, io: IOCompletions) {
        self.io = Some(io);
    }

    fn take_io(&mut self) -> Option<IOCompletions> {
        self.io.take()
    }

    fn park_err(&mut self, err: Box<LimboError>) {
        self.err = Some(err);
    }

    fn take_err(&mut self) -> Option<Box<LimboError>> {
        self.err.take()
    }
}

/// The runner of the row operation of `O`, boxed on the first step and
/// reused for every later step.
struct RowOp<O>(Option<BoxedResumable<RowStep<O>, (), ()>>);

impl<O> Default for RowOp<O> {
    fn default() -> Self {
        Self(None)
    }
}

impl<O: 'static> std::fmt::Debug for RowOp<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match &self.0 {
            Some(op) if op.is_active() => "RowOp(active)",
            _ => "RowOp(idle)",
        })
    }
}

/// Insert-only flush: every row is written once with a fresh key, so the
/// operation is seek-then-insert. A row whose exact key is already present
/// (a resumed insert) is overwritten in place, but only if the cursor is
/// actually positioned on the equal cell when `insert` runs. A seek that
/// stops one leaf early (`TryAdvance`) must advance first, or the insert
/// adds a second physical copy of the key.
#[derive(Debug)]
pub(super) struct RowInserter {
    rows: Vec<PendingRow>,
    idx: usize,
    op: RowOp<Self>,
}

impl RowInserter {
    pub fn new(rows: Vec<PendingRow>) -> Self {
        Self {
            rows,
            idx: 0,
            op: RowOp::default(),
        }
    }

    pub fn step(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<()> {
        let mut op = self
            .op
            .0
            .take()
            .unwrap_or_else(|| Runner::boxed(|co, args| with_handle(co, args, insert_rows)));
        let mut ctx = RowCtx {
            owner: self,
            cursor,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, ());
        self.op.0 = Some(op);
        result
    }
}

/// Inserts the rows in order. The record of a row is built once and lives
/// in the future: the seek and the insert of the cursor are resumed with
/// the same key after an I/O yield.
///
/// The seek uses `eq_only: true`: `NotFound` positions the cursor at the
/// exact slot where the key belongs (the fresh-key case), `Found` lands on
/// the equal cell (the resumed re-insert case, overwritten in place).
/// `TryAdvance` means the equal key can sit at the start of the next leaf
/// behind a stale interior divider: advancing before the insert reaches
/// it, so the overwrite happens instead of a second physical copy of the
/// key, exactly like the VDBE's `seek_internal` does.
async fn insert_rows(co: &mut Co<RowStep<RowInserter>>, _: ()) -> Result<(), Box<LimboError>> {
    loop {
        let Some(record) = co.with(|ctx| {
            ctx.owner
                .rows
                .get(ctx.owner.idx)
                .map(PendingRow::record)
                .transpose()
        })?
        else {
            return Ok(());
        };
        let seek_result = co
            .io(|ctx| {
                ctx.cursor.seek(
                    SeekKey::IndexKey(record.as_record_ref()),
                    SeekOp::GE { eq_only: true },
                )
            })
            .await;
        if matches!(seek_result, SeekResult::TryAdvance) {
            co.io(|ctx| ctx.cursor.next()).await;
        }
        co.io(|ctx| {
            ctx.cursor
                .insert(&BTreeKey::IndexKey(record.as_record_ref()))
        })
        .await;
        co.with(|ctx| ctx.owner.idx += 1);
    }
}

/// What rows a [`RowDeleter`] target matches.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Delete every row matching each target, one target at a time.
///
/// After every deletion the operation re-seeks from the target's logical
/// prefix: B-tree deletion may retreat, preserve, or advance the physical
/// cursor depending on balancing, so `next()` could skip a sibling row.
/// The seek key is built once per target and kept until the target is
/// done: the cursor's seek is resumed with the same key after an I/O
/// yield, and every re-seek after a deletion uses it again.
#[derive(Debug)]
pub(super) struct RowDeleter {
    targets: Vec<PathTarget>,
    idx: usize,
    op: RowOp<Self>,
}

impl RowDeleter {
    pub fn new(targets: Vec<PathTarget>) -> Self {
        Self {
            targets,
            idx: 0,
            op: RowOp::default(),
        }
    }

    #[cfg(test)]
    pub fn targets(&self) -> &[PathTarget] {
        &self.targets
    }

    pub fn step(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<()> {
        let mut op = self
            .op
            .0
            .take()
            .unwrap_or_else(|| Runner::boxed(|co, args| with_handle(co, args, delete_rows)));
        let mut ctx = RowCtx {
            owner: self,
            cursor,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, ());
        self.op.0 = Some(op);
        result
    }
}

async fn delete_rows(co: &mut Co<RowStep<RowDeleter>>, _: ()) -> Result<(), Box<LimboError>> {
    loop {
        let Some(seek_key) = co.with(|ctx| {
            ctx.owner
                .targets
                .get(ctx.owner.idx)
                .map(|target| seek_key_for_path(target.seek_path()))
                .transpose()
        })?
        else {
            return Ok(());
        };
        loop {
            if !seek_to_path(co, &seek_key).await {
                break;
            }
            let path = co.io(|ctx| read_row_path(ctx.cursor)).await;
            if !co.with(|ctx| ctx.owner.targets[ctx.owner.idx].matches(&path)) {
                break;
            }
            co.io(|ctx| ctx.cursor.delete()).await;
        }
        co.with(|ctx| ctx.owner.idx += 1);
    }
}

/// Delete the registry row (the row that says a segment exists) of every
/// segment a merge wants to rewrite, one segment at a time.
///
/// The delete is the lock on the segment. Under MVCC, a row that another
/// transaction deleted, committed or not, refuses the delete with a
/// write-write conflict. That segment is taken by another merge and stays
/// out of this one, and so does a segment whose row is already gone. The
/// segments whose rows this transaction deleted are claimed: no other merge
/// can delete them until this transaction ends. In WAL mode the pager write
/// lock serializes writers, so every candidate is claimed.
#[derive(Debug)]
pub(super) struct SegmentClaimer {
    candidates: Vec<(SegmentId, String)>,
    idx: usize,
    claimed: Vec<SegmentId>,
    taken: Vec<SegmentId>,
    op: RowOp<Self>,
}

impl SegmentClaimer {
    pub fn new(candidates: impl IntoIterator<Item = SegmentId>) -> Self {
        Self {
            candidates: candidates
                .into_iter()
                .map(|id| (id, segment_registry_path(&id)))
                .collect(),
            idx: 0,
            claimed: Vec::new(),
            taken: Vec::new(),
            op: RowOp::default(),
        }
    }

    /// The segments this transaction got, then the segments another merge
    /// holds. Call it after `step` returned `Done`.
    pub fn into_outcome(self) -> (Vec<SegmentId>, Vec<SegmentId>) {
        (self.claimed, self.taken)
    }

    pub fn step(&mut self, cursor: &mut dyn CursorTrait) -> IOResultOr<()> {
        let mut op = self
            .op
            .0
            .take()
            .unwrap_or_else(|| Runner::boxed(|co, args| with_handle(co, args, claim_segments)));
        let mut ctx = RowCtx {
            owner: self,
            cursor,
            io: None,
            err: None,
        };
        let result = op.resume(&mut ctx, ());
        self.op.0 = Some(op);
        result
    }

    fn finish_current(&mut self, claimed: bool) {
        let (id, _) = self.candidates[self.idx];
        if claimed {
            self.claimed.push(id);
        } else {
            self.taken.push(id);
        }
        self.idx += 1;
    }
}

async fn claim_segments(
    co: &mut Co<RowStep<SegmentClaimer>>,
    _: (),
) -> Result<(), Box<LimboError>> {
    loop {
        let Some(seek_key) = co.with(|ctx| {
            ctx.owner
                .candidates
                .get(ctx.owner.idx)
                .map(|(_, path)| seek_key_for_path(path))
                .transpose()
        })?
        else {
            return Ok(());
        };
        let mut claimed = false;
        if seek_to_path(co, &seek_key).await {
            let path = co.io(|ctx| read_row_path(ctx.cursor)).await;
            if co.with(|ctx| ctx.owner.candidates[ctx.owner.idx].1 == path) {
                claimed = co.io(|ctx| delete_unless_taken(ctx.cursor)).await;
            }
        }
        co.with(|ctx| ctx.owner.finish_current(claimed));
    }
}

/// Seeks to the first row at or after `seek_key`. Returns true when the
/// cursor is on a row afterwards.
async fn seek_to_path<O: 'static>(co: &mut Co<RowStep<O>>, seek_key: &ImmutableRecord) -> bool {
    let seek_result = co
        .io(|ctx| {
            ctx.cursor.seek(
                SeekKey::IndexKey(seek_key.as_record_ref()),
                SeekOp::GE { eq_only: false },
            )
        })
        .await;
    match seek_result {
        SeekResult::NotFound => false,
        SeekResult::TryAdvance => {
            co.io(|ctx| ctx.cursor.next()).await;
            co.with(|ctx| ctx.cursor.has_record())
        }
        SeekResult::Found => co.with(|ctx| ctx.cursor.has_record()),
    }
}

/// The path of the row the cursor is on.
fn read_row_path(cursor: &mut dyn CursorTrait) -> IOResultOr<String> {
    let record = match cursor.record()? {
        IOResult::Done(record) => record,
        IOResult::IO(io) => return Ok(IOResult::IO(io)),
    };
    let record =
        record.ok_or_else(|| LimboError::Corrupt("FTS cursor has no record payload".into()))?;
    Ok(IOResult::Done(row_path(record)?))
}

/// Deletes the row the cursor is on. Returns false when another
/// transaction holds the row.
fn delete_unless_taken(cursor: &mut dyn CursorTrait) -> IOResultOr<bool> {
    match cursor.delete() {
        Ok(result) => Ok(result.map(|()| true)),
        Err(err) if matches!(*err, LimboError::WriteWriteConflict) => Ok(IOResult::Done(false)),
        Err(err) => Err(err),
    }
}
