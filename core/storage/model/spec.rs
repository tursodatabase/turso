//! The cursor contract as a pure function (`tests/integration/cursor_contract/SPEC.md`).
//!
//! `step(tree, shape, op)` gives the new shape of one cursor and the write to apply to the
//! tree, or the rule of SPEC section 2d that makes the op illegal. `after_peer_write` gives
//! the shape of every other cursor of the tree after that write. Neither changes its inputs.
//! They know the tree only through the `Tree` trait, so the same code runs with any key type
//! and compare function: the model storage of the VDBE and the test models use it.
//!
//! The ops are opcode-level: one op is one VDBE opcode, also when the opcode makes several
//! cursor calls (an `IdxInsert` seeks, may read, then inserts).

use crate::types::SeekOp;
use std::fmt;

/// The rules of SPEC section 2d. An op that breaks one of them is Undefined. (U9, a use of the
/// position after a peer write, is retired: a peer keeps its position by key.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// `Next` or `Prev` in the wrong direction.
    U1,
    /// `Next` or `Prev` from a position that has no direction.
    U2,
    /// A write, `Count`, `NewRowid` or `SeekEnd` after `NullRow`.
    U3,
    /// `Delete` with no entry under the cursor.
    U4,
    /// An insert without the seek token of its key.
    U5,
    /// An index compare on an entry that is gone.
    U6,
    /// A delete of an index key that is not in the index, when the opcode asks for an error.
    U7,
    /// A read at a position that is not defined.
    U8,
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Why an op is illegal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Illegal {
    pub rule: Rule,
    pub why: &'static str,
}

/// The direction of the last move. `Next` needs `Fwd`, `Prev` needs `Bwd`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    None,
    Fwd,
    Bwd,
}

/// Where a cursor is. `K` is the key of an entry, `P` the key of a seek (for an index, a
/// prefix of the columns).
#[derive(Debug, Clone, PartialEq)]
pub enum Pos<K, P> {
    /// Not on an entry: a fresh cursor, an ended scan, a range seek that found nothing.
    /// A read gives NULL.
    Invalid,
    /// On the entry with key `K`.
    At(K),
    /// An exact seek for `P` missed. A read is Undefined. After a miss of `Found` or
    /// `NotFound`, `Next` goes to the first entry after `P`; after other exact seeks the
    /// cursor has no direction.
    Seeked(P),
    /// The entry `K` was deleted, by this cursor or by another one. `Next` and `Prev` go on
    /// from the gap; a read is Undefined.
    Gone(K),
    /// No position: after `Count`, `SeekEnd`, `NewRowid`, `IdxDelete`, `NoConflict` with a
    /// NULL.
    Lost,
}

/// The seek token. It makes an insert without its own seek legal (SPEC section 3).
#[derive(Debug, Clone, PartialEq)]
pub enum Token<P> {
    None,
    /// The last move was an exact seek for `key`. `hit` tells whether an entry matched.
    Key {
        key: P,
        hit: bool,
    },
    /// The last move was `SeekEnd` or `NewRowid`: an insert of a key above every key is legal.
    Append,
}

/// The state of one cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct Shape<K, P> {
    pub pos: Pos<K, P>,
    pub dir: Dir,
    /// The NullRow flag.
    pub null_row: bool,
    pub token: Token<P>,
}

impl<K, P> Shape<K, P> {
    /// A cursor that was just opened.
    pub fn fresh() -> Self {
        Self::new(Pos::Invalid, Dir::None, Token::None)
    }

    /// A cursor without a position.
    pub fn lost() -> Self {
        Self::new(Pos::Lost, Dir::None, Token::None)
    }

    /// A cursor of a tree that was cleared: an ended forward scan.
    pub fn ended() -> Self {
        Self::new(Pos::Invalid, Dir::Fwd, Token::None)
    }

    pub fn new(pos: Pos<K, P>, dir: Dir, token: Token<P>) -> Self {
        Self {
            pos,
            dir,
            null_row: false,
            token,
        }
    }

    /// True when a read gives an entry.
    pub fn on_entry(&self) -> bool {
        !self.null_row && matches!(self.pos, Pos::At(_))
    }
}

/// One opcode on one cursor.
#[derive(Debug, Clone, PartialEq)]
pub enum Op<K, P> {
    Rewind,
    Last,
    Next,
    Prev,
    /// `SeekGE/GT/LE/LT`, and the exact (eq-only) seeks of a table: `SeekRowid`, `NotExists`.
    Seek(P, SeekOp),
    /// `Found`, `NotFound`, and `NoConflict` with a key that has no NULL: an eq-only `SeekGE`
    /// on an index. A miss keeps the forward direction, so `Next` is legal after it.
    Found(P),
    /// `NoConflict` with a key that has a NULL: it jumps and does not seek.
    NoConflictNull,
    NewRowid,
    /// `Insert` and `IdxInsert`. `use_seek`: the opcode does not seek and needs the seek
    /// token (`Insert` without `REQUIRE_SEEK`, `IdxInsert` with `USE_SEEK`).
    Insert {
        key: K,
        use_seek: bool,
    },
    /// `IdxInsert` with `NO_OP_DUPLICATE` that found the key and did not write.
    InsertNoop,
    Delete,
    /// `IdxDelete`. `key` is the stored entry, `None` when the key is not in the index.
    /// `must_exist`: P5 is set, so a missing key is an error.
    IdxDelete {
        key: Option<K>,
        must_exist: bool,
    },
    NullRow,
    Count,
    SeekEnd,
    Clear,
    /// `Column`, `RowId`, `IdxRowId`: a read of the entry under the cursor.
    Read,
    /// `IdxGE/GT/LE/LT`: a compare of the entry under the cursor with a key.
    Compare,
}

impl<K, P> Op<K, P> {
    /// The name of the op, for messages and counters.
    pub fn name(&self) -> &'static str {
        match self {
            Op::Rewind => "Rewind",
            Op::Last => "Last",
            Op::Next => "Next",
            Op::Prev => "Prev",
            Op::Seek(_, op) => match op {
                SeekOp::GE { eq_only: true } => "SeekEq",
                SeekOp::GE { eq_only: false } => "SeekGE",
                SeekOp::GT => "SeekGT",
                SeekOp::LE { eq_only: true } => "SeekLEEq",
                SeekOp::LE { eq_only: false } => "SeekLE",
                SeekOp::LT => "SeekLT",
            },
            Op::Found(_) => "Found",
            Op::NoConflictNull => "NoConflictNull",
            Op::NewRowid => "NewRowid",
            Op::Insert { use_seek: true, .. } => "InsertUseSeek",
            Op::Insert { .. } => "Insert",
            Op::InsertNoop => "InsertNoop",
            Op::Delete => "Delete",
            Op::IdxDelete { key: Some(_), .. } => "IdxDelete",
            Op::IdxDelete { key: None, .. } => "IdxDeleteMiss",
            Op::NullRow => "NullRow",
            Op::Count => "Count",
            Op::SeekEnd => "SeekEnd",
            Op::Clear => "Clear",
            Op::Read => "Read",
            Op::Compare => "Compare",
        }
    }
}

/// A change of the tree.
#[derive(Debug, Clone, PartialEq)]
pub enum Write<K> {
    /// Add the key, or replace the entry that compares equal to it (new bytes win).
    Put(K),
    /// Remove the entry that compares equal to the key.
    Remove(K),
    Clear,
}

/// The result of a legal op.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome<K, P> {
    pub shape: Shape<K, P>,
    /// A seek or a move found an entry.
    pub hit: bool,
    pub write: Option<Write<K>>,
}

/// What the contract needs to know about a tree.
pub trait Tree {
    type Key: Clone + fmt::Debug + PartialEq;
    type Probe: Clone + fmt::Debug + PartialEq;
    fn first(&self) -> Option<Self::Key>;
    fn last(&self) -> Option<Self::Key>;
    /// The first entry after `k`. `k` does not have to be in the tree.
    fn after(&self, k: &Self::Key) -> Option<Self::Key>;
    /// The last entry before `k`.
    fn before(&self, k: &Self::Key) -> Option<Self::Key>;
    /// The entry a seek lands on. An eq-only seek lands only on an entry whose prefix is equal.
    fn seek(&self, p: &Self::Probe, op: SeekOp) -> Option<Self::Key>;
    fn contains(&self, k: &Self::Key) -> bool;
    /// May an insert of `k` use the token of an exact seek for `token`? A miss allows every
    /// key with that prefix; a hit only the same whole key.
    fn token_allows(&self, token: &Self::Probe, hit: bool, k: &Self::Key) -> bool;
    /// True when `k` is above every entry.
    fn above_all(&self, k: &Self::Key) -> bool;
}

/// The shape of a cursor and the op it runs, with the types of tree `T`.
pub type TShape<T> = Shape<<T as Tree>::Key, <T as Tree>::Probe>;
pub type TOp<T> = Op<<T as Tree>::Key, <T as Tree>::Probe>;
pub type TOutcome<T> = Outcome<<T as Tree>::Key, <T as Tree>::Probe>;

/// One op. `Err` names the rule that makes it illegal in this state.
pub fn step<T: Tree>(t: &T, c: &TShape<T>, op: &TOp<T>) -> Result<TOutcome<T>, Illegal> {
    if c.null_row && !allowed_after_null_row(op) {
        return illegal(
            Rule::U3,
            "a write, Count, NewRowid or SeekEnd after NullRow",
        );
    }
    match op {
        Op::Rewind => Ok(land(t.first(), Dir::Fwd)),
        Op::Last => Ok(land(t.last(), Dir::Bwd)),
        Op::Seek(p, op) => Ok(seek(t, p, *op, Dir::None)),
        Op::Found(p) => Ok(seek(t, p, SeekOp::GE { eq_only: true }, Dir::Fwd)),
        Op::NoConflictNull => Ok(keep(Shape::lost())),
        // SQLite's `OP_Next` on a cursor after `OP_NullRow` finds no row. The position
        // afterwards is none.
        Op::Next | Op::Prev if c.null_row => Ok(keep(Shape::lost())),
        Op::Next => advance(t, c, Dir::Fwd),
        Op::Prev => advance(t, c, Dir::Bwd),
        Op::NewRowid => Ok(keep(Shape::new(Pos::Lost, Dir::None, Token::Append))),
        Op::Insert { key, use_seek } => {
            if *use_seek && !token_allows(t, &c.token, key) {
                return illegal(Rule::U5, "an insert without the seek token of its key");
            }
            let shape = Shape::new(Pos::At(key.clone()), c.dir, Token::None);
            Ok(write(shape, Write::Put(key.clone())))
        }
        Op::InsertNoop => Ok(keep(Shape::lost())),
        Op::Delete => match &c.pos {
            Pos::At(k) if t.contains(k) => {
                let shape = Shape::new(Pos::Gone(k.clone()), c.dir, Token::None);
                Ok(write(shape, Write::Remove(k.clone())))
            }
            _ => illegal(Rule::U4, "Delete with no entry under the cursor"),
        },
        Op::IdxDelete { key: Some(k), .. } => Ok(write(Shape::lost(), Write::Remove(k.clone()))),
        Op::IdxDelete {
            key: None,
            must_exist: true,
        } => illegal(
            Rule::U7,
            "IdxDelete with P5 of a key that is not in the index",
        ),
        Op::IdxDelete { key: None, .. } => Ok(keep(Shape::lost())),
        Op::NullRow => Ok(keep(Shape {
            null_row: true,
            ..c.clone()
        })),
        Op::Count => Ok(keep(Shape::lost())),
        Op::SeekEnd => Ok(keep(Shape::new(Pos::Lost, Dir::None, Token::Append))),
        Op::Clear => Ok(write(Shape::ended(), Write::Clear)),
        Op::Read | Op::Compare => match c.pos {
            _ if c.null_row => Ok(keep(c.clone())),
            Pos::Invalid | Pos::At(_) => Ok(keep(c.clone())),
            Pos::Gone(_) if matches!(op, Op::Compare) => {
                illegal(Rule::U6, "an index compare after the entry was deleted")
            }
            Pos::Gone(_) => illegal(Rule::U8, "a read after the entry was deleted"),
            Pos::Seeked(_) => illegal(Rule::U8, "a read after an exact seek that missed"),
            Pos::Lost => illegal(Rule::U8, "a read at a cursor without a position"),
        },
    }
}

/// The shape of a cursor after another cursor of the tree wrote. A peer keeps its position by
/// key, as SQLite's saved cursors do: if its entry is gone, it is in the gap. A clear ends
/// every scan. Every write ends every seek token.
pub fn after_peer_write<T: Tree>(t: &T, c: &TShape<T>, cleared: bool) -> TShape<T> {
    if cleared {
        return Shape::ended();
    }
    let pos = match &c.pos {
        Pos::At(k) | Pos::Gone(k) if t.contains(k) => Pos::At(k.clone()),
        Pos::At(k) | Pos::Gone(k) => Pos::Gone(k.clone()),
        other => other.clone(),
    };
    Shape {
        pos,
        token: Token::None,
        ..c.clone()
    }
}

/// SPEC section 2: after `NullRow` only reads, compares, moves, seeks and `NullRow` are legal.
fn allowed_after_null_row<K, P>(op: &Op<K, P>) -> bool {
    matches!(
        op,
        Op::Rewind
            | Op::Last
            | Op::Next
            | Op::Prev
            | Op::Seek(..)
            | Op::Found(_)
            | Op::NoConflictNull
            | Op::Read
            | Op::Compare
            | Op::NullRow
    )
}

fn illegal<K, P>(rule: Rule, why: &'static str) -> Result<Outcome<K, P>, Illegal> {
    Err(Illegal { rule, why })
}

fn keep<K, P>(shape: Shape<K, P>) -> Outcome<K, P> {
    let hit = shape.on_entry();
    Outcome {
        shape,
        hit,
        write: None,
    }
}

fn write<K, P>(shape: Shape<K, P>, w: Write<K>) -> Outcome<K, P> {
    Outcome {
        shape,
        hit: false,
        write: Some(w),
    }
}

/// `Rewind`, `Last` and the range seeks: on the entry, else ended in the direction.
fn land<K, P>(entry: Option<K>, dir: Dir) -> Outcome<K, P> {
    match entry {
        Some(k) => keep(Shape::new(Pos::At(k), dir, Token::None)),
        None => keep(Shape::new(Pos::Invalid, dir, Token::None)),
    }
}

/// A range seek, or an exact (eq-only) seek. An exact seek leaves the seek token. A hit is on
/// the first (for `LE`, the last) matching entry; a miss is at the place of the key, with
/// the direction `miss_dir`.
fn seek<T: Tree>(t: &T, p: &T::Probe, op: SeekOp, miss_dir: Dir) -> TOutcome<T> {
    let dir = match op {
        SeekOp::GE { .. } | SeekOp::GT => Dir::Fwd,
        SeekOp::LE { .. } | SeekOp::LT => Dir::Bwd,
    };
    let entry = t.seek(p, op);
    if !op.eq_only() {
        return land(entry, dir);
    }
    let hit = entry.is_some();
    let token = Token::Key {
        key: p.clone(),
        hit,
    };
    match entry {
        Some(e) => keep(Shape::new(Pos::At(e), dir, token)),
        None => keep(Shape::new(Pos::Seeked(p.clone()), miss_dir, token)),
    }
}

/// `Next` and `Prev`. A move that finds no entry ends the scan.
fn advance<T: Tree>(t: &T, c: &TShape<T>, d: Dir) -> Result<TOutcome<T>, Illegal> {
    if c.dir == Dir::None {
        return illegal(Rule::U2, "Next or Prev from a position with no direction");
    }
    if c.dir != d {
        return illegal(Rule::U1, "Next or Prev in the wrong direction");
    }
    let next = match (&c.pos, d) {
        (Pos::Invalid, _) => return Ok(keep(c.clone())),
        (Pos::At(k) | Pos::Gone(k), Dir::Fwd) => t.after(k),
        (Pos::At(k) | Pos::Gone(k), _) => t.before(k),
        (Pos::Seeked(p), Dir::Fwd) => t.seek(p, SeekOp::GE { eq_only: false }),
        (Pos::Seeked(p), _) => t.seek(p, SeekOp::LE { eq_only: false }),
        (Pos::Lost, _) => {
            return illegal(Rule::U2, "Next or Prev from a position with no direction")
        }
    };
    Ok(land(next, d))
}

fn token_allows<T: Tree>(t: &T, tok: &Token<T::Probe>, k: &T::Key) -> bool {
    match tok {
        Token::None => false,
        Token::Key { key, hit } => t.token_allows(key, *hit, k),
        Token::Append => t.above_all(k),
    }
}
