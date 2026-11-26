//! The cursor contract as a pure function (`tests/integration/cursor_contract/SPEC.md`).
//!
//! `step(tree, shape, op)` gives the new shape of one cursor and the write to apply to the
//! tree, or the rule of SPEC section 2d that makes the op illegal. It never changes its
//! inputs. It knows the tree only through the `Tree` trait, so the same code runs with any
//! key type and compare function.
//!
//! The ops are opcode-level: one op is one VDBE opcode, also when the opcode makes several
//! cursor calls (an `IdxInsert` seeks, may read, then inserts).

use crate::types::SeekOp;
use std::fmt;

/// The rules of SPEC section 2d. An op that breaks one of them is Undefined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// `Next` or `Prev` in the wrong direction.
    U1,
    /// `Next` or `Prev` from a position that has no direction.
    U2,
    /// A move or a write after `NullRow`.
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
    /// A use of the old position after another cursor wrote to the tree.
    U9,
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

/// Where a cursor is.
#[derive(Debug, Clone, PartialEq)]
pub enum Pos<K> {
    /// Not on an entry: a fresh cursor, an ended scan, a range seek that found nothing.
    /// A read gives NULL.
    Invalid,
    /// On the entry with key `K`.
    At(K),
    /// A `Found`-style hit on an index where more than one entry can match. The cursor is on
    /// some match, but a read or a move is Undefined, so the match cannot be seen.
    Matched,
    /// An exact seek missed. Only an insert of the sought key may follow.
    Seeked,
    /// This cursor deleted `K`. `Next` and `Prev` go on from the gap; a read is Undefined.
    Gone(K),
    /// No position: after `Count`, `SeekEnd`, `NewRowid`, `IdxDelete`, or a peer write.
    Lost,
}

/// The seek token. It makes an insert without its own seek legal (SPEC section 3).
#[derive(Debug, Clone, PartialEq)]
pub enum Token<K> {
    None,
    /// The last move was an exact seek for `key`. `hit` tells whether an entry matched.
    Key { key: K, hit: bool },
    /// The last move was `SeekEnd` or `NewRowid`: an insert of a key above every key is legal.
    Append,
}

/// The state of one cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct Shape<K> {
    pub pos: Pos<K>,
    pub dir: Dir,
    /// The NullRow flag.
    pub null_row: bool,
    pub token: Token<K>,
}

impl<K> Shape<K> {
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

    fn new(pos: Pos<K>, dir: Dir, token: Token<K>) -> Self {
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
pub enum Op<K> {
    Rewind,
    Last,
    Next,
    Prev,
    /// `SeekGE/GT/LE/LT`, and the exact seeks: `SeekRowid`, `NotExists`, an eq-only seek.
    Seek(K, SeekOp),
    /// `Found`, `NotFound`, and `NoConflict` with a key that has no NULL.
    Found(K),
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
    /// `IdxDelete`. `None`: the key is not in the index, so the opcode does nothing.
    IdxDelete(Option<K>),
    NullRow,
    Count,
    SeekEnd,
    Clear,
    /// `Column`, `RowId`, `IdxRowId`, `IdxGE/GT/LE/LT`: a read of the entry under the cursor.
    Read,
}

impl<K> Op<K> {
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
            Op::Insert { use_seek: false, .. } => "Insert",
            Op::InsertNoop => "InsertNoop",
            Op::Delete => "Delete",
            Op::IdxDelete(Some(_)) => "IdxDelete",
            Op::IdxDelete(None) => "IdxDeleteMiss",
            Op::NullRow => "NullRow",
            Op::Count => "Count",
            Op::SeekEnd => "SeekEnd",
            Op::Clear => "Clear",
            Op::Read => "Read",
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
pub struct Outcome<K> {
    pub shape: Shape<K>,
    /// A seek or a move found an entry.
    pub hit: bool,
    pub write: Option<Write<K>>,
}

/// What the contract needs to know about a tree.
pub trait Tree {
    type Key: Clone + fmt::Debug;
    fn first(&self) -> Option<Self::Key>;
    fn last(&self) -> Option<Self::Key>;
    /// The first entry after `k`. `k` does not have to be in the tree.
    fn after(&self, k: &Self::Key) -> Option<Self::Key>;
    /// The last entry before `k`.
    fn before(&self, k: &Self::Key) -> Option<Self::Key>;
    /// The entry a seek lands on. An eq-only seek lands only on an equal entry.
    fn seek(&self, key: &Self::Key, op: SeekOp) -> Option<Self::Key>;
    fn contains(&self, k: &Self::Key) -> bool;
    /// True when at most one entry can match `key` in a `Found`-style seek.
    fn single_match(&self, key: &Self::Key) -> bool;
    /// May an insert of `k` use the token of an exact seek for `token`?
    fn token_allows(&self, token: &Self::Key, hit: bool, k: &Self::Key) -> bool;
    /// True when `k` is above every entry.
    fn above_all(&self, k: &Self::Key) -> bool;
}

/// One op. `Err` names the rule that makes it illegal in this state.
pub fn step<T: Tree>(
    t: &T,
    c: &Shape<T::Key>,
    op: &Op<T::Key>,
) -> Result<Outcome<T::Key>, Illegal> {
    if c.null_row && !allowed_after_null_row(op) {
        return illegal(Rule::U3, "a move or a write after NullRow");
    }
    match op {
        Op::Rewind => Ok(land(t.first(), Dir::Fwd)),
        Op::Last => Ok(land(t.last(), Dir::Bwd)),
        Op::Seek(k, op) => Ok(seek(t, k, *op)),
        Op::Found(k) => Ok(found(t, k)),
        Op::NoConflictNull => Ok(keep(Shape::lost())),
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
        Op::IdxDelete(Some(k)) => Ok(write(Shape::lost(), Write::Remove(k.clone()))),
        Op::IdxDelete(None) => Ok(keep(Shape::lost())),
        Op::NullRow => match c.pos {
            Pos::Lost => illegal(Rule::U9, "NullRow on a cursor without a position"),
            _ => Ok(keep(Shape {
                null_row: true,
                ..c.clone()
            })),
        },
        Op::Count => Ok(keep(Shape::lost())),
        Op::SeekEnd => Ok(keep(Shape::new(Pos::Lost, Dir::None, Token::Append))),
        Op::Clear => Ok(write(Shape::ended(), Write::Clear)),
        Op::Read => match c.pos {
            _ if c.null_row => Ok(keep(c.clone())),
            Pos::Invalid | Pos::At(_) => Ok(keep(c.clone())),
            Pos::Matched => illegal(Rule::U8, "a read after a Found-style hit"),
            Pos::Seeked => illegal(Rule::U8, "a read after an exact seek that missed"),
            Pos::Gone(_) => illegal(Rule::U8, "a read after the cursor deleted its entry"),
            Pos::Lost => illegal(Rule::U8, "a read at a cursor without a position"),
        },
    }
}

/// SPEC section 2: after `NullRow` only reads, `Rewind`, `Last`, seeks and `NullRow` are legal.
fn allowed_after_null_row<K>(op: &Op<K>) -> bool {
    matches!(
        op,
        Op::Rewind | Op::Last | Op::Seek(..) | Op::Read | Op::NullRow
    )
}

fn illegal<K>(rule: Rule, why: &'static str) -> Result<Outcome<K>, Illegal> {
    Err(Illegal { rule, why })
}

fn keep<K>(shape: Shape<K>) -> Outcome<K> {
    let hit = shape.on_entry();
    Outcome {
        shape,
        hit,
        write: None,
    }
}

fn write<K>(shape: Shape<K>, w: Write<K>) -> Outcome<K> {
    Outcome {
        shape,
        hit: false,
        write: Some(w),
    }
}

/// `Rewind`, `Last` and the range seeks: on the entry, else ended in the direction.
fn land<K>(entry: Option<K>, dir: Dir) -> Outcome<K> {
    match entry {
        Some(k) => keep(Shape::new(Pos::At(k), dir, Token::None)),
        None => keep(Shape::new(Pos::Invalid, dir, Token::None)),
    }
}

/// A range seek, or an exact (eq-only) seek. An exact seek leaves the seek token.
fn seek<T: Tree>(t: &T, k: &T::Key, op: SeekOp) -> Outcome<T::Key> {
    let dir = match op {
        SeekOp::GE { .. } | SeekOp::GT => Dir::Fwd,
        SeekOp::LE { .. } | SeekOp::LT => Dir::Bwd,
    };
    let entry = t.seek(k, op);
    if !op.eq_only() {
        return land(entry, dir);
    }
    let hit = entry.is_some();
    let token = Token::Key {
        key: k.clone(),
        hit,
    };
    match entry {
        Some(e) => keep(Shape::new(Pos::At(e), dir, token)),
        None => keep(Shape::new(Pos::Seeked, Dir::None, token)),
    }
}

/// `Found`, `NotFound`, `NoConflict`. A hit gives no token (SQLite seeks again before an
/// insert). The entry of a hit can be read only when no other entry can match.
fn found<T: Tree>(t: &T, k: &T::Key) -> Outcome<T::Key> {
    match t.seek(k, SeekOp::GE { eq_only: true }) {
        Some(e) if t.single_match(k) => Outcome {
            shape: Shape::new(Pos::At(e), Dir::None, Token::None),
            hit: true,
            write: None,
        },
        Some(_) => Outcome {
            shape: Shape::new(Pos::Matched, Dir::None, Token::None),
            hit: true,
            write: None,
        },
        None => keep(Shape::new(
            Pos::Seeked,
            Dir::None,
            Token::Key {
                key: k.clone(),
                hit: false,
            },
        )),
    }
}

/// `Next` and `Prev`. A move that finds no entry ends the scan.
fn advance<T: Tree>(t: &T, c: &Shape<T::Key>, d: Dir) -> Result<Outcome<T::Key>, Illegal> {
    if c.dir == Dir::None {
        return illegal(Rule::U2, "Next or Prev from a position with no direction");
    }
    if c.dir != d {
        return illegal(Rule::U1, "Next or Prev in the wrong direction");
    }
    match &c.pos {
        Pos::Invalid => Ok(keep(c.clone())),
        Pos::At(k) | Pos::Gone(k) => {
            let next = match d {
                Dir::Fwd => t.after(k),
                _ => t.before(k),
            };
            Ok(land(next, d))
        }
        Pos::Matched | Pos::Seeked | Pos::Lost => {
            illegal(Rule::U2, "Next or Prev from a position with no direction")
        }
    }
}

fn token_allows<T: Tree>(t: &T, tok: &Token<T::Key>, k: &T::Key) -> bool {
    match tok {
        Token::None => false,
        Token::Key { key, hit } => t.token_allows(key, *hit, k),
        Token::Append => t.above_all(k),
    }
}
