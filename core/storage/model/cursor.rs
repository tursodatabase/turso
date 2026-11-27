//! `ModelCursor`: a `CursorTrait` implementation that keeps the tree in memory and runs each
//! opcode through the spec layer (`spec.rs`). It does no I/O.
//!
//! The VDBE calls the cursor at a finer grain than the contract, and some opcodes make the
//! same calls. The cursor infers the opcode from the call pattern: `seek_to_last` starts a
//! `NewRowid` (its `rowid` and `next` are part of it), an eq-only `SeekGE` on an index is a
//! `Found`, and an `Insert` or `IdxInsert` that does not seek uses the seek token of the last
//! move. Facts that no call shows are listed in SPEC section 2e.

use super::spec::{self, Dir, Outcome, Pos, Token, Tree, Write};
use super::stats;
use super::store::ConnTrees;
use crate::storage::btree::{BTreeKey, CursorStep, CursorTrait};
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
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};

/// The key of an entry: the rowid of a table row, or the whole record of an index entry.
/// A seek key of an index is a record too, and can be shorter than an entry.
#[derive(Clone, PartialEq)]
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
#[derive(Clone)]
pub(super) enum Rows {
    Table(BTreeMap<i64, ImmutableRecord>),
    Index(Arc<IndexInfo>, Vec<ImmutableRecord>),
}

/// A tree and its write counter.
#[derive(Clone)]
pub(super) struct SharedTree {
    pub(super) rows: Rows,
    /// Changes at each write. A cursor that saw another value must catch up.
    version: u64,
    /// The version of the last clear, or 0.
    last_clear: u64,
}

impl SharedTree {
    pub(super) fn new(info: Option<Arc<IndexInfo>>) -> Self {
        let rows = match info {
            Some(info) => Rows::Index(info, Vec::new()),
            None => Rows::Table(BTreeMap::new()),
        };
        Self {
            rows,
            version: next_version(),
            last_clear: 0,
        }
    }

    /// A tree that does not exist yet: it reads as empty, and its version never changes.
    pub(super) fn missing(info: Option<Arc<IndexInfo>>) -> Self {
        Self {
            version: 0,
            ..Self::new(info)
        }
    }

    pub(super) fn len(&self) -> usize {
        match &self.rows {
            Rows::Table(map) => map.len(),
            Rows::Index(_, rows) => rows.len(),
        }
    }

    /// True when the tree holds index records exactly when a cursor with `info` expects them.
    pub(super) fn fits(&self, info: &Option<Arc<IndexInfo>>) -> bool {
        matches!(
            (&self.rows, info),
            (Rows::Table(_), None) | (Rows::Index(..), Some(_))
        )
    }

    pub(super) fn clear(&mut self) {
        self.apply(Write::Clear, None);
    }

    fn apply(&mut self, w: Write<Key>, value: Option<&ImmutableRecord>) {
        self.version = next_version();
        if matches!(w, Write::Clear) {
            self.last_clear = self.version;
        }
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

/// Versions come from one counter, so a tree that a savepoint rollback puts back never has
/// the version that a cursor saw last.
fn next_version() -> u64 {
    static VERSION: AtomicU64 = AtomicU64::new(1);
    VERSION.fetch_add(1, AtomicOrdering::Relaxed)
}

/// Where a cursor finds its tree.
#[derive(Clone)]
enum TreeRef {
    /// An ephemeral tree: the cursor and its `OpenDup` copies share it.
    Own(Arc<RwLock<SharedTree>>),
    /// A tree of the database, looked up by root in the version the connection sees now.
    Db { trees: Arc<ConnTrees>, root: i64 },
}

impl TreeRef {
    fn read<R>(&self, info: &Option<Arc<IndexInfo>>, f: impl FnOnce(&SharedTree) -> R) -> R {
        match self {
            TreeRef::Own(tree) => f(&tree.read()),
            TreeRef::Db { trees, root } => trees.read_tree(*root, info, f),
        }
    }

    fn write<R>(&self, info: &Option<Arc<IndexInfo>>, f: impl FnOnce(&mut SharedTree) -> R) -> R {
        match self {
            TreeRef::Own(tree) => f(&mut tree.write()),
            TreeRef::Db { trees, root } => trees.write_tree(*root, info, f),
        }
    }
}

type Shape = spec::Shape<Key, Key>;
type Op = spec::Op<Key, Key>;

/// The fake root pages of ephemeral trees. They are far above the page numbers of real trees.
static NEXT_ROOT: AtomicI64 = AtomicI64::new(1 << 40);

/// How many ops a panic message lists.
const RECENT: usize = 12;

pub struct ModelCursor {
    tree: TreeRef,
    info: Option<Arc<IndexInfo>>,
    root: i64,
    /// The tree version after the last op of this cursor.
    seen: u64,
    shape: Shape,
    /// A `NewRowid` runs: `seek_to_last` started it, `next` or a missed seek ends it.
    in_new_rowid: bool,
    /// Where the calls inside `NewRowid` put the cursor.
    scratch: Option<Key>,
    /// The record that `record` returned.
    record: Option<ImmutableRecord>,
    /// The main pager of the connection. Only `ClearBtree` asks for it.
    pager: Arc<Pager>,
    recent: VecDeque<&'static str>,
    /// The key of the last eq-only seek and the shape before it. `Insert` with `REQUIRE_SEEK`
    /// and `IdxInsert` without `USE_SEEK` make this seek themselves and then insert the same
    /// key: that is `Insert { use_seek: false }` on the shape before the seek. A seek opcode
    /// followed by an insert without a seek makes the same calls: that is a seek and
    /// `Insert { use_seek: true }`. The two differ only in the direction afterwards.
    /// The third field is `alt_dirs` before the seek.
    own_seek: Option<(Key, Shape, Vec<Dir>)>,
    /// The directions of the other readings of the last insert, while they are still open.
    /// An op that is illegal with the direction of the cursor is tried with these.
    alt_dirs: Vec<Dir>,
}

impl ModelCursor {
    /// A cursor on a new ephemeral tree.
    pub fn ephemeral(pager: Arc<Pager>, info: Option<Arc<IndexInfo>>) -> Self {
        stats::tree_opened(info.is_some());
        let tree = TreeRef::Own(Arc::new(RwLock::new(SharedTree::new(info.clone()))));
        let root = NEXT_ROOT.fetch_add(1, AtomicOrdering::Relaxed);
        Self::open(pager, tree, root, info)
    }

    /// A cursor on the tree `root` of a database.
    pub(super) fn on_tree(
        pager: Arc<Pager>,
        trees: Arc<ConnTrees>,
        root: i64,
        info: Option<Arc<IndexInfo>>,
    ) -> Self {
        stats::cursor_opened();
        Self::open(pager, TreeRef::Db { trees, root }, root, info)
    }

    fn open(pager: Arc<Pager>, tree: TreeRef, root: i64, info: Option<Arc<IndexInfo>>) -> Self {
        let seen = tree.read(&info, |t| t.version);
        Self {
            tree,
            info,
            root,
            seen,
            shape: Shape::fresh(),
            in_new_rowid: false,
            scratch: None,
            record: None,
            pager,
            recent: VecDeque::new(),
            own_seek: None,
            alt_dirs: Vec::new(),
        }
    }

    /// One spec step. An illegal op panics with `unsupported: U<n>: ...`.
    fn run(&mut self, op: Op, value: Option<&ImmutableRecord>) -> bool {
        self.catch_up();
        let is_read = matches!(op, Op::Read);
        // A read between the seek and the insert is part of the opcode (a unique check).
        if !is_read {
            self.own_seek = None;
        }
        let alt_dirs = if is_read {
            self.alt_dirs.clone()
        } else {
            std::mem::take(&mut self.alt_dirs)
        };
        stats::step(op.name());
        if self.recent.len() == RECENT {
            self.recent.pop_front();
        }
        self.recent.push_back(op.name());
        let shape = &self.shape;
        let mut used_alt = false;
        let result = self.tree.read(&self.info, |t| {
            let first = spec::step(&t.rows, shape, &op);
            if first.is_ok() {
                return first;
            }
            for dir in &alt_dirs {
                let alt = Shape {
                    dir: *dir,
                    ..shape.clone()
                };
                if let Ok(outcome) = spec::step(&t.rows, &alt, &op) {
                    used_alt = true;
                    return Ok(outcome);
                }
            }
            first
        });
        let Outcome { shape, hit, write } = match result {
            Ok(outcome) => outcome,
            Err(e) => panic!(
                "unsupported: {}: {} ({}, root {}, op {op:?}, shape {:?}, recent ops: {})",
                e.rule,
                e.why,
                match (&self.tree, &self.info) {
                    (TreeRef::Own(_), Some(_)) => "ephemeral index",
                    (TreeRef::Own(_), None) => "ephemeral table",
                    (TreeRef::Db { .. }, Some(_)) => "index",
                    (TreeRef::Db { .. }, None) => "table",
                },
                self.root,
                self.shape,
                Vec::from(self.recent.clone()).join(" "),
            ),
        };
        if let Some(w) = write {
            self.seen = self.tree.write(&self.info, |t| {
                t.apply(w, value);
                t.version
            });
        }
        // The other reading stays open while the ops keep the direction (a read, a delete).
        if !is_read && !used_alt && shape.dir == self.shape.dir {
            self.alt_dirs = alt_dirs;
        }
        self.shape = shape;
        self.record = None;
        hit
    }

    /// After writes by other cursors the cursor keeps its position by key, or is at the end
    /// after a clear (`spec::after_peer_write`).
    fn catch_up(&mut self) {
        let seen = self.seen;
        let shape = &self.shape;
        let caught = self.tree.read(&self.info, |t| {
            (t.version != seen).then(|| {
                let cleared = t.last_clear > seen;
                (spec::after_peer_write(&t.rows, shape, cleared), t.version)
            })
        });
        if let Some((shape, version)) = caught {
            self.own_seek = None;
            self.shape = shape;
            self.seen = version;
            self.record = None;
        }
    }

    /// Ends a `NewRowid` whose last call (`next`) did not come.
    fn finish_new_rowid(&mut self) {
        self.in_new_rowid = false;
        self.scratch = None;
    }

    /// The key under the cursor, if a read gives an entry.
    fn entry(&self) -> Option<Key> {
        match &self.shape.pos {
            Pos::At(k) if self.shape.on_entry() => Some(k.clone()),
            _ => None,
        }
    }

    /// A read. Inside `NewRowid` it reads the last entry.
    fn read_entry(&mut self) -> Option<Key> {
        self.catch_up();
        if self.in_new_rowid {
            return self.scratch.clone();
        }
        self.run(Op::Read, None);
        self.entry()
    }

    /// The stored record of `key`.
    fn load(&self, key: &Key) -> Option<ImmutableRecord> {
        self.tree.read(&self.info, |t| match (&t.rows, key) {
            (Rows::Table(map), Key::Rowid(r)) => map.get(r).cloned(),
            (Rows::Index(info, rows), Key::Record(k)) => rows
                .iter()
                .find(|s| cmp(info, s, k, Ordering::Equal).is_eq())
                .cloned(),
            _ => unreachable!("model: key and tree differ"),
        })
    }

    fn seek_key(&mut self, key: Key, op: SeekOp) -> SeekResult {
        self.catch_up();
        let hit = if self.in_new_rowid {
            // `NewRowid` looks for a free random rowid.
            let found = self.tree.read(&self.info, |t| t.rows.seek(&key, op));
            if found.is_none() {
                // The rowid is free, and the insert that follows uses this seek.
                self.finish_new_rowid();
                self.shape.token = Token::Key { key, hit: false };
            }
            found.is_some()
        } else if op == (SeekOp::GE { eq_only: true }) {
            let before = self.shape.clone();
            let before_alts = self.alt_dirs.clone();
            // On an index an eq-only `SeekGE` is also the seek of `Found`, `NotFound`,
            // `NoConflict`, `IdxInsert` and `IdxDelete`: they all get the rules of `Found`.
            let hit = match &key {
                Key::Record(_) => self.run(Op::Found(key.clone()), None),
                Key::Rowid(_) => self.run(Op::Seek(key.clone(), op), None),
            };
            self.own_seek = Some((key, before, before_alts));
            hit
        } else {
            self.run(Op::Seek(key, op), None)
        };
        if hit {
            SeekResult::Found
        } else {
            SeekResult::NotFound
        }
    }

    /// `Next` and `Prev` as one opcode.
    fn advance(&mut self, op: Op) -> CursorStep {
        self.finish_new_rowid();
        self.run(op, None);
        if self.shape.on_entry() {
            CursorStep::Row
        } else {
            CursorStep::Empty
        }
    }

    fn is_current(&self) -> bool {
        self.seen == self.tree.read(&self.info, |t| t.version)
    }

    fn rowid_of_key(&self, key: Key) -> Option<i64> {
        match key {
            Key::Rowid(r) => Some(r),
            Key::Record(_) if !self.has_rowid() => None,
            Key::Record(rec) => match rec.last_value() {
                Some(Ok(ValueRef::Numeric(Numeric::Integer(r)))) => Some(r),
                other => panic!("model: an index entry ends with {other:?}, not a rowid"),
            },
        }
    }
}

/// The compare of the B-tree: a stored record against a key, with the tie-breaker for a key
/// that is a prefix.
fn cmp(
    info: &IndexInfo,
    stored: &ImmutableRecord,
    key: &ImmutableRecord,
    tb: Ordering,
) -> Ordering {
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
    type Probe = Key;

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
            Rows::Table(map) => map
                .range(..rowid_of(k))
                .next_back()
                .map(|(x, _)| Key::Rowid(*x)),
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
                    SeekOp::GE { .. } | SeekOp::GT => Some(rows.partition_point(|s| c(s).is_lt())),
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
                rows.iter()
                    .any(|s| cmp(info, s, k, Ordering::Equal).is_eq())
            }
        }
    }

    fn token_allows(&self, token: &Key, hit: bool, k: &Key) -> bool {
        match self {
            Rows::Table(_) => rowid_of(token) == rowid_of(k),
            Rows::Index(info, _) => {
                let (token, k) = (record_of(token), record_of(k));
                // A hit names one entry only with all key columns. A miss names the place of
                // every key with that prefix. (The key columns can be fewer than the columns
                // of a record: the backing trees of an index method compare a prefix only.)
                (!hit || token.column_count() >= info.key_info.len())
                    && cmp(info, k, token, Ordering::Equal).is_eq()
            }
        }
    }

    fn same_entry(&self, a: &Key, b: &Key) -> bool {
        match self {
            Rows::Table(_) => rowid_of(a) == rowid_of(b),
            Rows::Index(info, _) => cmp(info, record_of(a), record_of(b), Ordering::Equal).is_eq(),
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
        self.finish_new_rowid();
        self.run(Op::Last, None);
        Ok(IOResult::Done(()))
    }

    fn next(&mut self) -> IOResultOr<()> {
        if self.in_new_rowid {
            // The last call of `NewRowid`.
            self.finish_new_rowid();
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
        Ok(IOResult::Done(self.rowid_of_key(key)))
    }

    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
        let record = self.read_entry().and_then(|key| self.load(&key));
        self.record = record;
        Ok(IOResult::Done(self.record.as_ref()))
    }

    fn holds_row(&mut self, rowid: i64, payload: &[u8]) -> IOResultOr<bool> {
        // Part of `Insert`: it asks before it writes, also when the cursor is in the gap of
        // its own `Delete`. It is not a read of the contract.
        self.catch_up();
        let Some(key) = self.entry() else {
            return Ok(IOResult::Done(false));
        };
        if key != Key::Rowid(rowid) {
            return Ok(IOResult::Done(false));
        }
        let same = self
            .load(&key)
            .is_some_and(|stored| stored.get_payload() == payload);
        Ok(IOResult::Done(same))
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
        self.finish_new_rowid();
        self.catch_up();
        // The insert checks the seek token of the last move. When that move was an eq-only
        // seek of the same key, the seek can also be the own seek of the insert opcode
        // (`own_seek`): then the direction before the seek is the other reading.
        let mut other = Vec::new();
        if let Some((sought, before, before_alts)) = self.own_seek.take() {
            let op = Op::Insert {
                key: key.clone(),
                use_seek: false,
            };
            // The insert keeps the direction, and its legality does not depend on it.
            if sought == key
                && self
                    .tree
                    .read(&self.info, |t| spec::step(&t.rows, &before, &op).is_ok())
            {
                other.push(before.dir);
                other.extend(before_alts);
            }
        }
        self.run(
            Op::Insert {
                key,
                use_seek: true,
            },
            value.as_ref(),
        );
        other.retain(|dir| *dir != self.shape.dir);
        other.dedup();
        self.alt_dirs = other;
        Ok(IOResult::Done(()))
    }

    fn delete(&mut self) -> IOResultOr<()> {
        self.finish_new_rowid();
        self.run(Op::Delete, None);
        Ok(IOResult::Done(()))
    }

    fn set_null_flag(&mut self, flag: bool) {
        if flag {
            self.finish_new_rowid();
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
        self.finish_new_rowid();
        let n = self.tree.read(&self.info, |t| t.len());
        self.run(Op::Clear, None);
        Ok(IOResult::Done(n as u64))
    }

    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        panic!(
            "unsupported in model storage: Destroy through a cursor (root {})",
            self.root
        )
    }

    fn count(&mut self) -> IOResultOr<usize> {
        self.finish_new_rowid();
        let n = self.tree.read(&self.info, |t| t.len());
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
        self.finish_new_rowid();
        self.run(Op::Rewind, None);
        Ok(IOResult::Done(()))
    }

    fn has_record(&self) -> bool {
        if self.in_new_rowid {
            return self.scratch.is_some();
        }
        self.is_current() && self.shape.on_entry()
    }

    fn set_has_record(&mut self, _has_record: bool) {
        panic!(
            "unsupported in model storage: set_has_record (root {})",
            self.root
        )
    }

    fn index_info(&self) -> Option<&Arc<IndexInfo>> {
        self.info.as_ref()
    }

    fn seek_end(&mut self) -> IOResultOr<()> {
        self.finish_new_rowid();
        self.run(Op::SeekEnd, None);
        Ok(IOResult::Done(()))
    }

    fn seek_to_last(&mut self) -> IOResultOr<()> {
        // Only `NewRowid` calls this. It then reads the rowid and calls `next`.
        self.finish_new_rowid();
        self.run(Op::NewRowid, None);
        self.in_new_rowid = true;
        self.scratch = self.tree.read(&self.info, |t| t.rows.last());
        Ok(IOResult::Done(()))
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

    fn duplicate(&self) -> Option<Box<dyn CursorTrait>> {
        stats::dup_opened();
        Some(Box::new(Self::open(
            self.pager.clone(),
            self.tree.clone(),
            self.root,
            self.info.clone(),
        )))
    }
}
