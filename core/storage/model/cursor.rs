//! `ModelCursor`: a `CursorTrait` implementation that keeps the tree in memory and runs each
//! opcode through the spec layer (`spec.rs`). It does no I/O.
//!
//! The VDBE calls the cursor at a finer grain than the contract: one opcode can make several
//! calls (`IdxInsert` seeks, reads and inserts). `note_opcode` tells the cursor which opcode
//! starts. The calls inside that opcode are answered from the tree without a spec step, and
//! the opcode is one spec step when it ends.

use super::spec::{self, Op, Outcome, Pos, Rule, Shape, Tree, Write};
use super::stats;
use crate::storage::btree::{BTreeKey, CursorOpcode, CursorStep, CursorTrait};
use crate::storage::pager::Pager;
use crate::sync::{Arc, RwLock};
use crate::types::{
    compare_record, IOResult, IOResultOr, ImmutableRecord, IndexInfo, RecordBuf, SeekKey, SeekOp,
    SeekResult, ValueRef,
};
use crate::vdbe::Register;
use crate::{Numeric, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::ops::Bound;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};

/// The key of an entry: the rowid of a table row, or the whole record of an index entry.
/// A seek key of an index is a record too, and can be shorter than an entry.
#[derive(Clone)]
pub enum Key {
    Rowid(i64),
    Record(ImmutableRecord),
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Rowid(r) => write!(f, "{r}"),
            Key::Record(rec) => {
                let values = rec.get_values().map_err(|_| fmt::Error)?;
                let values: Vec<String> = values.iter().map(|v| format!("{v}")).collect();
                write!(f, "({})", values.join(", "))
            }
        }
    }
}

/// The entries of a tree. An index keeps its records sorted with the compare of the B-tree.
enum Rows {
    Table(BTreeMap<i64, ImmutableRecord>),
    Index(Arc<IndexInfo>, Vec<ImmutableRecord>),
}

/// A tree that a cursor and its `OpenDup` copies share.
struct SharedTree {
    rows: Rows,
    /// Goes up by one at each write. A cursor that saw an older value lost its position.
    version: u64,
    /// The last write was a clear: the other cursors are at the end, not lost.
    cleared_last: bool,
}

/// What the cursor knows about the opcode that runs now.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Pending {
    None,
    /// The next seek is the seek of `Found`, `NotFound` or `NoConflict`.
    Found,
    /// `NewRowid`: it reads the last rowid with `seek_to_last`, `rowid` and `next`.
    NewRowid,
    Insert { use_seek: bool },
    IdxInsert { use_seek: bool },
    IdxDelete,
}

impl Pending {
    /// The cursor calls of these opcodes are not opcodes of their own.
    fn is_internal(self) -> bool {
        !matches!(self, Pending::None | Pending::Found)
    }
}

/// The fake root pages of model trees. They are far above the page numbers of real trees.
static NEXT_ROOT: AtomicI64 = AtomicI64::new(1 << 40);

/// How many ops a panic message lists.
const RECENT: usize = 12;

pub struct ModelCursor {
    tree: Arc<RwLock<SharedTree>>,
    info: Option<Arc<IndexInfo>>,
    root: i64,
    /// The tree version after the last op of this cursor.
    seen: u64,
    /// The cursor lost its position because another cursor wrote (U9).
    peer_wrote: bool,
    shape: Shape<Key>,
    pending: Pending,
    /// Where the calls inside a pending opcode put the cursor.
    scratch: Option<Key>,
    /// The record that `record` returned.
    record: Option<ImmutableRecord>,
    /// The main pager of the connection. Only `ClearBtree` asks for it.
    pager: Arc<Pager>,
    recent: VecDeque<&'static str>,
}

impl ModelCursor {
    pub fn new_table(pager: Arc<Pager>) -> Self {
        stats::tree_opened(false);
        Self::open(pager, None, Rows::Table(BTreeMap::new()))
    }

    pub fn new_index(pager: Arc<Pager>, info: Arc<IndexInfo>) -> Self {
        stats::tree_opened(true);
        Self::open(pager, Some(info.clone()), Rows::Index(info, Vec::new()))
    }

    /// A second cursor on the same tree (`OpenDup`).
    pub fn dup(&self) -> Self {
        stats::dup_opened();
        let version = self.tree.read().version;
        Self {
            tree: self.tree.clone(),
            info: self.info.clone(),
            root: self.root,
            seen: version,
            peer_wrote: false,
            shape: Shape::fresh(),
            pending: Pending::None,
            scratch: None,
            record: None,
            pager: self.pager.clone(),
            recent: VecDeque::new(),
        }
    }

    fn open(pager: Arc<Pager>, info: Option<Arc<IndexInfo>>, rows: Rows) -> Self {
        let root = NEXT_ROOT.fetch_add(1, AtomicOrdering::Relaxed);
        let tree = SharedTree {
            rows,
            version: 0,
            cleared_last: false,
        };
        Self {
            tree: Arc::new(RwLock::new(tree)),
            info,
            root,
            seen: 0,
            peer_wrote: false,
            shape: Shape::fresh(),
            pending: Pending::None,
            scratch: None,
            record: None,
            pager,
            recent: VecDeque::new(),
        }
    }

    /// One spec step. An illegal op panics with `unsupported: U<n>: ...`.
    fn run(&mut self, op: Op<Key>, value: Option<&ImmutableRecord>) -> bool {
        self.catch_up();
        stats::step(op.name());
        if self.recent.len() == RECENT {
            self.recent.pop_front();
        }
        self.recent.push_back(op.name());
        let result = spec::step(&self.tree.read().rows, &self.shape, &op);
        let Outcome { shape, hit, write } = match result {
            Ok(outcome) => outcome,
            Err(e) => panic!(
                "unsupported: {}: {}{} (ephemeral {}, root {}, op {op:?}, shape {:?}, recent ops: {})",
                if self.peer_wrote { Rule::U9 } else { e.rule },
                if self.peer_wrote {
                    "another cursor wrote to the tree, then: "
                } else {
                    ""
                },
                e.why,
                if self.info.is_some() { "index" } else { "table" },
                self.root,
                self.shape,
                Vec::from(self.recent.clone()).join(" "),
            ),
        };
        if let Some(w) = write {
            let mut tree = self.tree.write();
            tree.apply(w, value);
            self.seen = tree.version;
        }
        self.shape = shape;
        self.record = None;
        self.peer_wrote = false;
        hit
    }

    /// After a write by another cursor the position is lost (U9), or at the end after a clear.
    fn catch_up(&mut self) {
        let tree = self.tree.read();
        if self.seen != tree.version {
            self.shape = if tree.cleared_last {
                Shape::ended()
            } else {
                Shape::lost()
            };
            self.peer_wrote = !tree.cleared_last;
            self.seen = tree.version;
            self.record = None;
        }
    }

    /// The opcode noted before ended without the call that finishes it: an `IdxInsert` that
    /// found a duplicate, an `IdxDelete` that found no key, an `Insert` that saw the same bytes.
    fn finish_pending(&mut self) {
        let pending = std::mem::replace(&mut self.pending, Pending::None);
        let scratch = self.scratch.take();
        match pending {
            Pending::None | Pending::Found | Pending::NewRowid => {}
            Pending::IdxInsert { .. } => {
                self.run(Op::InsertNoop, None);
            }
            Pending::IdxDelete => {
                self.run(Op::IdxDelete(None), None);
            }
            Pending::Insert { use_seek } => {
                // The no-op check of an UPDATE found the same bytes and skipped the write.
                self.catch_up();
                match scratch.or_else(|| self.entry()) {
                    Some(key) => self.run(Op::Insert { key, use_seek }, None),
                    None => self.run(Op::InsertNoop, None),
                };
            }
        }
    }

    /// The key under the cursor, if a read gives an entry.
    fn entry(&self) -> Option<Key> {
        match &self.shape.pos {
            Pos::At(k) if self.shape.on_entry() => Some(k.clone()),
            _ => None,
        }
    }

    /// A read. Inside an opcode it reads where the opcode's own seek went.
    fn read_entry(&mut self) -> Option<Key> {
        self.catch_up();
        if self.pending.is_internal() {
            return self.scratch.clone().or_else(|| self.entry());
        }
        self.finish_pending();
        self.run(Op::Read, None);
        self.entry()
    }

    fn seek_key(&mut self, key: Key, op: SeekOp) -> SeekResult {
        self.catch_up();
        let hit = match self.pending {
            Pending::Found => {
                self.pending = Pending::None;
                self.run(Op::Found(key), None)
            }
            p if p.is_internal() => {
                self.scratch = self.tree.read().rows.seek(&key, op);
                self.scratch.is_some()
            }
            _ => self.run(Op::Seek(key, op), None),
        };
        if hit {
            SeekResult::Found
        } else {
            SeekResult::NotFound
        }
    }

    /// `Next` and `Prev` as one opcode.
    fn advance(&mut self, op: Op<Key>) -> CursorStep {
        self.finish_pending();
        self.run(op, None);
        if self.shape.on_entry() {
            CursorStep::Row
        } else {
            CursorStep::Empty
        }
    }

    fn is_current(&self) -> bool {
        self.seen == self.tree.read().version
    }
}

impl SharedTree {
    fn apply(&mut self, w: Write<Key>, value: Option<&ImmutableRecord>) {
        self.version += 1;
        self.cleared_last = matches!(w, Write::Clear);
        match (&mut self.rows, w) {
            (Rows::Table(map), Write::Put(Key::Rowid(r))) => {
                // No value: an `Insert` that kept the same bytes.
                if let Some(v) = value {
                    map.insert(r, v.clone());
                }
            }
            (Rows::Table(map), Write::Remove(Key::Rowid(r))) => {
                map.remove(&r);
            }
            (Rows::Index(info, rows), Write::Put(Key::Record(rec))) => {
                let i = rows.partition_point(|s| cmp(info, s, &rec, Ordering::Equal).is_lt());
                if i < rows.len() && cmp(info, &rows[i], &rec, Ordering::Equal).is_eq() {
                    rows[i] = rec;
                } else {
                    rows.insert(i, rec);
                }
            }
            (Rows::Index(info, rows), Write::Remove(Key::Record(rec))) => {
                rows.retain(|s| !cmp(info, s, &rec, Ordering::Equal).is_eq());
            }
            (Rows::Table(map), Write::Clear) => map.clear(),
            (Rows::Index(_, rows), Write::Clear) => rows.clear(),
            (_, w) => unreachable!("model: write {w:?} does not fit the tree"),
        }
    }
}

/// The compare of the B-tree: a stored record against a key, with the tie-breaker for a key
/// that is a prefix.
fn cmp(info: &IndexInfo, stored: &ImmutableRecord, key: &ImmutableRecord, tb: Ordering) -> Ordering {
    let values = key.get_values().expect("model: a key record is readable");
    compare_record(stored.get_payload(), values.iter(), info, tb)
        .expect("model: a stored record is readable")
}

fn record_of(key: &Key) -> &ImmutableRecord {
    match key {
        Key::Record(r) => r,
        Key::Rowid(_) => unreachable!("model: a rowid key on an index"),
    }
}

fn rowid_of(key: &Key) -> i64 {
    match key {
        Key::Rowid(r) => *r,
        Key::Record(_) => unreachable!("model: a record key on a table"),
    }
}

impl Tree for Rows {
    type Key = Key;

    fn first(&self) -> Option<Key> {
        match self {
            Rows::Table(map) => map.keys().next().map(|r| Key::Rowid(*r)),
            Rows::Index(_, rows) => rows.first().cloned().map(Key::Record),
        }
    }

    fn last(&self) -> Option<Key> {
        match self {
            Rows::Table(map) => map.keys().next_back().map(|r| Key::Rowid(*r)),
            Rows::Index(_, rows) => rows.last().cloned().map(Key::Record),
        }
    }

    fn after(&self, k: &Key) -> Option<Key> {
        match self {
            Rows::Table(map) => map
                .range((Bound::Excluded(rowid_of(k)), Bound::Unbounded))
                .next()
                .map(|(x, _)| Key::Rowid(*x)),
            Rows::Index(info, rows) => {
                let k = record_of(k);
                let i = rows.partition_point(|s| !cmp(info, s, k, Ordering::Equal).is_gt());
                rows.get(i).cloned().map(Key::Record)
            }
        }
    }

    fn before(&self, k: &Key) -> Option<Key> {
        match self {
            Rows::Table(map) => map.range(..rowid_of(k)).next_back().map(|(x, _)| Key::Rowid(*x)),
            Rows::Index(info, rows) => {
                let k = record_of(k);
                let i = rows.partition_point(|s| cmp(info, s, k, Ordering::Equal).is_lt());
                i.checked_sub(1).map(|i| Key::Record(rows[i].clone()))
            }
        }
    }

    fn seek(&self, key: &Key, op: SeekOp) -> Option<Key> {
        match self {
            Rows::Table(map) => {
                let r = rowid_of(key);
                let hit = match op {
                    SeekOp::GE { eq_only: true } | SeekOp::LE { eq_only: true } => {
                        map.contains_key(&r).then_some(r)
                    }
                    SeekOp::GE { .. } => map.range(r..).next().map(|(x, _)| *x),
                    SeekOp::GT => map
                        .range((Bound::Excluded(r), Bound::Unbounded))
                        .next()
                        .map(|(x, _)| *x),
                    SeekOp::LE { .. } => map.range(..=r).next_back().map(|(x, _)| *x),
                    SeekOp::LT => map.range(..r).next_back().map(|(x, _)| *x),
                };
                hit.map(Key::Rowid)
            }
            Rows::Index(info, rows) => {
                let key = record_of(key);
                let tb = crate::types::get_tie_breaker_from_seek_op(op);
                let c = |s: &ImmutableRecord| cmp(info, s, key, tb);
                let i = match op {
                    SeekOp::GE { .. } | SeekOp::GT => {
                        Some(rows.partition_point(|s| c(s).is_lt()))
                    }
                    SeekOp::LE { .. } | SeekOp::LT => {
                        rows.partition_point(|s| !c(s).is_gt()).checked_sub(1)
                    }
                };
                let e = i.and_then(|i| rows.get(i))?;
                if op.eq_only() && !c(e).is_eq() {
                    return None;
                }
                Some(Key::Record(e.clone()))
            }
        }
    }

    fn contains(&self, k: &Key) -> bool {
        match self {
            Rows::Table(map) => map.contains_key(&rowid_of(k)),
            Rows::Index(info, rows) => {
                let k = record_of(k);
                rows.iter().any(|s| cmp(info, s, k, Ordering::Equal).is_eq())
            }
        }
    }

    fn single_match(&self, key: &Key) -> bool {
        match self {
            Rows::Table(_) => true,
            Rows::Index(info, _) => {
                if !info.is_unique {
                    return false;
                }
                let n = info.num_cols - usize::from(info.has_rowid);
                let key = record_of(key);
                let values = key.get_values().expect("model: a key record is readable");
                values.len() >= n && values[..n].iter().all(|v| !matches!(v, ValueRef::Null))
            }
        }
    }

    fn token_allows(&self, token: &Key, hit: bool, k: &Key) -> bool {
        match self {
            Rows::Table(_) => rowid_of(token) == rowid_of(k),
            Rows::Index(info, _) => {
                let (token, k) = (record_of(token), record_of(k));
                // A hit names one entry only with the whole key. A miss names the place of every
                // key with that prefix.
                (!hit || token.column_count() == k.column_count())
                    && cmp(info, k, token, Ordering::Equal).is_eq()
            }
        }
    }

    fn above_all(&self, k: &Key) -> bool {
        match self {
            Rows::Table(map) => map.keys().next_back().is_none_or(|m| *m < rowid_of(k)),
            Rows::Index(info, rows) => rows
                .last()
                .is_none_or(|s| cmp(info, s, record_of(k), Ordering::Equal).is_lt()),
        }
    }
}

fn copy_record(payload: &[u8]) -> ImmutableRecord {
    ImmutableRecord::copy_payload(payload, RecordBuf::alloc()).expect("model: out of memory")
}

impl CursorTrait for ModelCursor {
    fn last(&mut self) -> IOResultOr<()> {
        self.finish_pending();
        self.run(Op::Last, None);
        Ok(IOResult::Done(()))
    }

    fn next(&mut self) -> IOResultOr<()> {
        if self.pending == Pending::NewRowid {
            // The last call of `NewRowid`.
            self.pending = Pending::None;
            self.scratch = None;
            return Ok(IOResult::Done(()));
        }
        let _ = self.advance(Op::Next);
        Ok(IOResult::Done(()))
    }

    fn prev(&mut self) -> IOResultOr<()> {
        let _ = self.advance(Op::Prev);
        Ok(IOResult::Done(()))
    }

    fn next_row(&mut self) -> CursorStep {
        self.advance(Op::Next)
    }

    fn prev_row(&mut self) -> CursorStep {
        self.advance(Op::Prev)
    }

    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
        let Some(key) = self.read_entry() else {
            return Ok(IOResult::Done(None));
        };
        let rowid = match key {
            Key::Rowid(r) => Some(r),
            Key::Record(_) if !self.has_rowid() => None,
            Key::Record(rec) => match rec.last_value() {
                Some(Ok(ValueRef::Numeric(Numeric::Integer(r)))) => Some(r),
                other => panic!("model: an index entry ends with {other:?}, not a rowid"),
            },
        };
        Ok(IOResult::Done(rowid))
    }

    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
        let record = self.read_entry().and_then(|key| {
            let tree = self.tree.read();
            match (&tree.rows, key) {
                (Rows::Table(map), Key::Rowid(r)) => map.get(&r).cloned(),
                (Rows::Index(info, rows), Key::Record(k)) => rows
                    .iter()
                    .find(|s| cmp(info, s, &k, Ordering::Equal).is_eq())
                    .cloned(),
                _ => unreachable!("model: key and tree differ"),
            }
        });
        self.record = record;
        Ok(IOResult::Done(self.record.as_ref()))
    }

    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        let key = match key {
            SeekKey::TableRowId(r) => Key::Rowid(r),
            SeekKey::IndexKey(r) => Key::Record(copy_record(r.get_payload())),
        };
        Ok(IOResult::Done(self.seek_key(key, op)))
    }

    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        let record = ImmutableRecord::from_registers(registers, registers.len())?;
        Ok(IOResult::Done(self.seek_key(Key::Record(record), op)))
    }

    fn insert(&mut self, key: &BTreeKey) -> IOResultOr<()> {
        let (key, value) = match key {
            BTreeKey::TableRowId((r, value)) => (Key::Rowid(*r), value.cloned()),
            BTreeKey::IndexKey(r) => {
                let rec = copy_record(r.get_payload());
                (Key::Record(rec.clone()), Some(rec))
            }
        };
        self.catch_up();
        let use_seek = match self.pending {
            Pending::Insert { use_seek } | Pending::IdxInsert { use_seek } => use_seek,
            _ => true,
        };
        self.pending = Pending::None;
        self.scratch = None;
        self.run(Op::Insert { key, use_seek }, value.as_ref());
        Ok(IOResult::Done(()))
    }

    fn delete(&mut self) -> IOResultOr<()> {
        self.catch_up();
        if self.pending == Pending::IdxDelete {
            self.pending = Pending::None;
            let key = self.scratch.take();
            self.run(Op::IdxDelete(key), None);
        } else {
            self.finish_pending();
            self.run(Op::Delete, None);
        }
        Ok(IOResult::Done(()))
    }

    fn set_null_flag(&mut self, flag: bool) {
        if flag {
            self.finish_pending();
            self.run(Op::NullRow, None);
        } else {
            self.catch_up();
            self.shape.null_row = false;
        }
    }

    fn get_null_flag(&self) -> bool {
        self.is_current() && self.shape.null_row
    }

    fn exists(&mut self, key: &Value) -> IOResultOr<bool> {
        let Value::Numeric(Numeric::Integer(r)) = key else {
            panic!("model: a table key is not an integer: {key:?}");
        };
        let hit = self.seek_key(Key::Rowid(*r), SeekOp::GE { eq_only: true });
        Ok(IOResult::Done(hit == SeekResult::Found))
    }

    fn clear_btree(&mut self) -> IOResultOr<u64> {
        self.finish_pending();
        let n = match &self.tree.read().rows {
            Rows::Table(map) => map.len(),
            Rows::Index(_, rows) => rows.len(),
        };
        self.run(Op::Clear, None);
        Ok(IOResult::Done(n as u64))
    }

    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        panic!("unsupported: Destroy of a model tree (root {})", self.root)
    }

    fn count(&mut self) -> IOResultOr<usize> {
        self.finish_pending();
        let n = match &self.tree.read().rows {
            Rows::Table(map) => map.len(),
            Rows::Index(_, rows) => rows.len(),
        };
        self.run(Op::Count, None);
        Ok(IOResult::Done(n))
    }

    fn is_empty(&self) -> bool {
        !self.has_record()
    }

    fn root_page(&self) -> i64 {
        self.root
    }

    fn rewind(&mut self) -> IOResultOr<()> {
        self.finish_pending();
        self.run(Op::Rewind, None);
        Ok(IOResult::Done(()))
    }

    fn has_record(&self) -> bool {
        if self.pending.is_internal() {
            return self.scratch.is_some();
        }
        self.is_current() && self.shape.on_entry()
    }

    fn set_has_record(&mut self, _has_record: bool) {
        panic!("unsupported: set_has_record on a model tree (root {})", self.root)
    }

    fn index_info(&self) -> Option<&Arc<IndexInfo>> {
        self.info.as_ref()
    }

    fn seek_end(&mut self) -> IOResultOr<()> {
        self.finish_pending();
        self.run(Op::SeekEnd, None);
        Ok(IOResult::Done(()))
    }

    fn seek_to_last(&mut self) -> IOResultOr<()> {
        if self.pending == Pending::NewRowid {
            self.catch_up();
            self.scratch = self.tree.read().rows.last();
            return Ok(IOResult::Done(()));
        }
        self.last()
    }

    fn invalidate_record(&mut self) {}

    fn has_rowid(&self) -> bool {
        self.info.as_ref().is_none_or(|info| info.has_rowid)
    }

    fn get_pager(&self) -> Arc<Pager> {
        self.pager.clone()
    }

    fn get_skip_advance(&self) -> bool {
        false
    }

    fn note_opcode(&mut self, op: CursorOpcode) {
        self.finish_pending();
        match op {
            CursorOpcode::Found | CursorOpcode::NoConflict { has_null: false } => {
                self.pending = Pending::Found;
            }
            CursorOpcode::NoConflict { has_null: true } => {
                self.run(Op::NoConflictNull, None);
            }
            CursorOpcode::NewRowid => {
                self.run(Op::NewRowid, None);
                self.pending = Pending::NewRowid;
            }
            CursorOpcode::Insert { require_seek } => {
                self.pending = Pending::Insert {
                    use_seek: !require_seek,
                };
            }
            CursorOpcode::IdxInsert { use_seek } => {
                self.pending = Pending::IdxInsert { use_seek };
            }
            CursorOpcode::IdxDelete => self.pending = Pending::IdxDelete,
        }
    }
}
