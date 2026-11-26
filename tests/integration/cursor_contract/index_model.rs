//! Reference model of index cursors (SPEC 2b) with the sort order and the collation of the index
//! columns (SPEC 2c, `IdxDef`).
//!
//! The model has these parts: `ipos`, `itok`, `icursor`, `istep`, `irun`, `iseek_ge`,
//! `cmp_prefix`, and so on. A caller comes before the functions it calls. The unit tests at
//! the end are examples with hand-checked expected values.
//!
//! The model is the oracle of the index random walk (`index_walk.rs`). `istep` returns `None`
//! for an illegal op, and a value that the contract leaves open is `Spec::Unspec`, which the
//! comparison with an engine skips.

use super::index_program::{run_index, IObs, IOp, IndexRun, Outcome, Row};
use super::model::{Dir, Spec};
use super::program::ReadMode;
use super::{DbKind, Layout};
use std::cmp::Ordering;

// ---------------------------------------------------------------------------------------
// Types (cols, ikey, istore, ipos, itok, icursor, istate, iop, iaction, iobs)
// ---------------------------------------------------------------------------------------

/// One value of an index column. The order is NULL, then integers, then text (BINARY, by
/// byte). NULL equals NULL.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Val {
    Null,
    Int(i64),
    Text(String),
}

/// Prints a value as Rust source (`Val::Int(1)`), so that the repro tests that the walker
/// prints compile.
impl std::fmt::Debug for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::Null => write!(f, "Val::Null"),
            Val::Int(x) => write!(f, "Val::Int({x})"),
            Val::Text(t) => write!(f, "Val::Text({t:?}.to_string())"),
        }
    }
}

impl Val {
    pub fn int(x: i64) -> Val {
        Val::Int(x)
    }

    pub fn text(x: &str) -> Val {
        Val::Text(x.to_string())
    }
}

impl From<i64> for Val {
    fn from(x: i64) -> Val {
        Val::Int(x)
    }
}

impl From<&str> for Val {
    fn from(x: &str) -> Val {
        Val::text(x)
    }
}

/// The sort order of an index column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

/// The collation of an index column (SPEC 2c).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coll {
    Binary,
    NoCase,
    RTrim,
}

/// Sort order and collation of one index column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColDef {
    pub order: Order,
    pub coll: Coll,
}

impl ColDef {
    pub const fn new(order: Order, coll: Coll) -> ColDef {
        ColDef { order, coll }
    }
}

/// The definition of the index `i_ab ON ti(a, b)`: one `ColDef` for `a` and one for `b`. The
/// trailing rowid is always ASC and BINARY. The default is `(ASC, BINARY)` for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdxDef {
    pub cols: [ColDef; 2],
}

impl IdxDef {
    pub const fn new(a: ColDef, b: ColDef) -> IdxDef {
        IdxDef { cols: [a, b] }
    }

    /// The definition as Rust source, for the repro tests that the walker prints.
    pub fn src(self) -> String {
        let col = |c: ColDef| format!("ColDef::new(Order::{:?}, Coll::{:?})", c.order, c.coll);
        format!("IdxDef::new({}, {})", col(self.cols[0]), col(self.cols[1]))
    }

    /// Is this the plain `(ASC, BINARY)` index?
    pub fn is_default(self) -> bool {
        self == IdxDef::default()
    }
}

impl Default for ColDef {
    fn default() -> ColDef {
        ColDef::new(Order::Asc, Coll::Binary)
    }
}

impl Default for IdxDef {
    fn default() -> IdxDef {
        IdxDef::new(ColDef::default(), ColDef::default())
    }
}

/// The columns of an index key.
pub type Cols = Vec<Val>;

/// `(columns, rowid)`.
pub type IKey = (Cols, i64);

/// The keys of the index, sorted by `cmp_key`.
pub type IStore = Vec<IKey>;

/// Like `Pos` in `model.rs`, with two more states. `Matched(p)`: a `Found`-style opcode hit,
/// the cursor is on some entry with prefix `p`, reads are `Unspec`, `Next` and `Prev` are
/// illegal. `Seeked(p)`: such an opcode missed.
#[derive(Debug, Clone, PartialEq)]
pub enum IPos {
    Invalid,
    At(IKey),
    Matched(Cols),
    Seeked(Cols),
    Gone(IKey),
    Lost,
}

/// The seek token holds the prefix of the last `Found`-style opcode, or "append".
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)] // variant names are the token names of the contract
pub enum ITok {
    ITNone,
    ITKey(Cols),
    ITApp,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ICursor {
    pub icpos: IPos,
    pub icdir: Dir,
    /// The NullRow flag.
    pub icnull: bool,
    pub ictok: ITok,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IState {
    pub irows: IStore,
    pub icurs: Vec<ICursor>,
    pub idef: IdxDef,
}

/// The operations of the model. The harness `IOp` is mapped onto this type by
/// `IReplay::action_of`.
#[derive(Debug, Clone, PartialEq)]
pub enum IMOp {
    IRewind,
    ILast,
    INext,
    IPrev,
    ISeekGE(Cols),
    ISeekGT(Cols),
    ISeekLE(Cols),
    ISeekLT(Cols),
    IFound(Cols),
    INotFound(Cols),
    INoConflict(Cols),
    IdxGE(Cols),
    IdxGT(Cols),
    IdxLE(Cols),
    IdxLT(Cols),
    /// `(key, use_seek)`.
    IdxInsert(IKey, bool),
    IdxDelete(IKey),
    IDelete,
    ISeekEnd,
    IClear,
    INullRow,
    ICount,
    /// Reads the cursor and changes nothing.
    IObserve,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IAction {
    pub iacur: usize,
    pub iaop: IMOp,
}

/// What the model says after every action: the jump flag, `IdxRowId` and all columns.
/// `None` is NULL.
#[derive(Debug, Clone, PartialEq)]
pub struct IMObs {
    pub ijumped: bool,
    pub irid: Spec<Option<i64>>,
    pub icolv: Spec<Option<Cols>>,
}

impl ICursor {
    fn new(icpos: IPos, icdir: Dir, icnull: bool, ictok: ITok) -> ICursor {
        ICursor {
            icpos,
            icdir,
            icnull,
            ictok,
        }
    }
}

pub fn fresh_icursor() -> ICursor {
    ICursor::new(IPos::Invalid, Dir::DNone, false, ITok::ITNone)
}

// ---------------------------------------------------------------------------------------
// Run and step
// ---------------------------------------------------------------------------------------

pub fn iinit(s: IStore, n: usize) -> IState {
    iinit_with(s, n, IdxDef::default())
}

pub fn iinit_with(s: IStore, n: usize, idef: IdxDef) -> IState {
    IState {
        irows: s,
        icurs: vec![fresh_icursor(); n],
        idef,
    }
}

/// Run a whole program; `None` if any action is illegal.
pub fn irun(st: &IState, p: &[IAction]) -> Option<(IState, Vec<IMObs>)> {
    let mut st = st.clone();
    let mut os = Vec::new();
    for a in p {
        let (st2, o) = istep(&st, a)?;
        st = st2;
        os.push(o);
    }
    Some((st, os))
}

/// One action. `None` means the action is illegal in this state.
pub fn istep(st: &IState, a: &IAction) -> Option<(IState, IMObs)> {
    let c = st.icurs.get(a.iacur)?.clone();
    if c.icnull && !ok_after_null(&a.iaop) {
        return None;
    }
    let s = &st.irows;
    let d = st.idef;
    let put = |c2: ICursor, j: bool| iput(st, a.iacur, c2, j);
    let seek = |o: Option<IKey>, d: Dir| {
        let (c2, j) = imove_to(o, d);
        put(c2, j)
    };
    // Next and Prev: a move that finds no entry ends the scan and does not jump.
    let advance = |o: Option<IKey>| match o {
        Some(k) => put(
            ICursor::new(IPos::At(k), c.icdir, false, ITok::ITNone),
            true,
        ),
        None => put(
            ICursor::new(IPos::Invalid, c.icdir, false, ITok::ITNone),
            false,
        ),
    };
    // `Found`-style opcodes: `hit_jump` is the jump flag on a hit.
    let found = |p: &Cols, hit_jump: bool| {
        if has_prefix(d, p, s) {
            put(
                ICursor::new(
                    IPos::Matched(p.clone()),
                    Dir::DNone,
                    false,
                    ITok::ITKey(p.clone()),
                ),
                hit_jump,
            )
        } else {
            put(
                ICursor::new(
                    IPos::Seeked(p.clone()),
                    Dir::DNone,
                    false,
                    ITok::ITKey(p.clone()),
                ),
                !hit_jump,
            )
        }
    };
    // `Idx*`: compare the entry under the cursor with the prefix; no state change.
    let idx = |p: &Cols, f: fn(Ordering) -> bool| match &c.icpos {
        IPos::At(k) => put(c.clone(), f(cmp_prefix(d, k, p))),
        IPos::Invalid => put(c.clone(), true),
        _ => None,
    };
    match &a.iaop {
        IMOp::IRewind => seek(imin(s), Dir::Fwd),
        IMOp::ILast => seek(imax(s), Dir::Bwd),
        IMOp::ISeekGE(p) => seek(iseek_ge(d, p, s), Dir::Fwd),
        IMOp::ISeekGT(p) => seek(iseek_gt(d, p, s), Dir::Fwd),
        IMOp::ISeekLE(p) => seek(iseek_le(d, p, s), Dir::Bwd),
        IMOp::ISeekLT(p) => seek(iseek_lt(d, p, s), Dir::Bwd),
        IMOp::IFound(p) => found(p, true),
        IMOp::INotFound(p) => found(p, false),
        IMOp::INoConflict(p) => {
            if has_null(p) {
                put(
                    ICursor::new(IPos::Lost, Dir::DNone, false, ITok::ITNone),
                    true,
                )
            } else {
                found(p, false)
            }
        }
        IMOp::IdxGE(p) => idx(p, is_ge),
        IMOp::IdxGT(p) => idx(p, is_gt),
        IMOp::IdxLE(p) => idx(p, is_le),
        IMOp::IdxLT(p) => idx(p, is_lt),
        IMOp::INext => {
            if c.icdir != Dir::Fwd {
                return None;
            }
            match &c.icpos {
                IPos::Invalid => put(c.clone(), false),
                IPos::At(k) | IPos::Gone(k) => advance(isucc(d, k, s)),
                _ => None,
            }
        }
        IMOp::IPrev => {
            if c.icdir != Dir::Bwd {
                return None;
            }
            match &c.icpos {
                IPos::Invalid => put(c.clone(), false),
                IPos::At(k) | IPos::Gone(k) => advance(ipred(d, k, s)),
                _ => None,
            }
        }
        IMOp::IdxInsert(key, use_seek) => {
            if !imem(d, key, s) && (!*use_seek || tok_ok(d, &c, key, s)) {
                let s2 = iins(d, key, s);
                let c2 = ICursor::new(IPos::At(key.clone()), c.icdir, false, ITok::ITNone);
                Some(iwrite(st, s2, a.iacur, c2))
            } else {
                None
            }
        }
        IMOp::IdxDelete(key) => {
            // Only the stored bytes name the entry: a key that is equal under the collations
            // but not identical is Undefined (SPEC 2c).
            if s.contains(key) {
                let s2 = idel(d, key, s);
                let c2 = ICursor::new(IPos::Lost, Dir::DNone, false, ITok::ITNone);
                Some(iwrite(st, s2, a.iacur, c2))
            } else {
                None
            }
        }
        IMOp::IDelete => match &c.icpos {
            IPos::At(k) if imem(d, k, s) => {
                let s2 = idel(d, k, s);
                let c2 = ICursor::new(IPos::Gone(k.clone()), c.icdir, false, ITok::ITNone);
                Some(iwrite(st, s2, a.iacur, c2))
            }
            _ => None,
        },
        IMOp::ISeekEnd => put(
            ICursor::new(IPos::Lost, Dir::DNone, false, ITok::ITApp),
            false,
        ),
        IMOp::IClear => {
            let ended = ICursor::new(IPos::Invalid, Dir::Fwd, false, ITok::ITNone);
            Some((
                IState {
                    irows: Vec::new(),
                    icurs: vec![ended; st.icurs.len()],
                    idef: d,
                },
                IMObs {
                    ijumped: false,
                    irid: Spec::Is(None),
                    icolv: Spec::Is(None),
                },
            ))
        }
        IMOp::INullRow => put(
            ICursor {
                icnull: true,
                ..c.clone()
            },
            false,
        ),
        IMOp::ICount => {
            let c2 = ICursor::new(IPos::Lost, Dir::DNone, false, ITok::ITNone);
            Some((
                IState {
                    irows: s.clone(),
                    icurs: iset_nth(&st.icurs, a.iacur, c2),
                    idef: d,
                },
                IMObs {
                    ijumped: false,
                    irid: Spec::Is(Some(s.len() as i64)),
                    icolv: Spec::Unspec,
                },
            ))
        }
        IMOp::IObserve => put(c.clone(), false),
    }
}

/// Opcodes that are still legal after `NullRow`.
fn ok_after_null(o: &IMOp) -> bool {
    matches!(
        o,
        IMOp::IRewind
            | IMOp::ILast
            | IMOp::ISeekGE(_)
            | IMOp::ISeekGT(_)
            | IMOp::ISeekLE(_)
            | IMOp::ISeekLT(_)
            | IMOp::IObserve
            | IMOp::INullRow
    )
}

/// May an `IdxInsert` with `use_seek` go ahead? The cursor holds the token for exactly the
/// columns of the key, or an append token and the key is above every key, or the columns have
/// a NULL (Turso seeks then).
fn tok_ok(d: IdxDef, c: &ICursor, key: &IKey, s: &IStore) -> bool {
    (match &c.ictok {
        ITok::ITKey(p) => cmp_cols(d, p, &key.0) == Ordering::Equal,
        ITok::ITApp => above_all(d, key, s),
        ITok::ITNone => false,
    }) || has_null(&key.0)
}

/// The store stays the same, one cursor changes.
fn iput(st: &IState, i: usize, c2: ICursor, j: bool) -> Option<(IState, IMObs)> {
    let o = iobs_of(st.idef, &st.irows, &c2, j);
    Some((
        IState {
            irows: st.irows.clone(),
            icurs: iset_nth(&st.icurs, i, c2),
            idef: st.idef,
        },
        o,
    ))
}

/// A write: new store, and every pending seek token ends.
fn iwrite(st: &IState, s2: IStore, i: usize, c2: ICursor) -> (IState, IMObs) {
    let o = iobs_of(st.idef, &s2, &c2, false);
    (
        IState {
            irows: s2,
            icurs: iset_nth(&iclear_toks(&st.icurs), i, c2),
            idef: st.idef,
        },
        o,
    )
}

fn imove_to(o: Option<IKey>, d: Dir) -> (ICursor, bool) {
    match o {
        Some(k) => (ICursor::new(IPos::At(k), d, false, ITok::ITNone), false),
        None => (ICursor::new(IPos::Invalid, d, false, ITok::ITNone), true),
    }
}

/// A write by any cursor ends every pending seek token.
fn iclear_toks(cs: &[ICursor]) -> Vec<ICursor> {
    cs.iter()
        .map(|c| ICursor {
            ictok: ITok::ITNone,
            ..c.clone()
        })
        .collect()
}

fn iset_nth(cs: &[ICursor], i: usize, c: ICursor) -> Vec<ICursor> {
    let mut out = cs.to_vec();
    out[i] = c;
    out
}

// ---------------------------------------------------------------------------------------
// Observations
// ---------------------------------------------------------------------------------------

/// After every action the test reads the jump flag, `IdxRowId` and `Column` (all columns).
fn iobs_of(d: IdxDef, s: &IStore, c: &ICursor, j: bool) -> IMObs {
    match iread(d, s, c) {
        Spec::Unspec => IMObs {
            ijumped: j,
            irid: Spec::Unspec,
            icolv: Spec::Unspec,
        },
        Spec::Is(None) => IMObs {
            ijumped: j,
            irid: Spec::Is(None),
            icolv: Spec::Is(None),
        },
        Spec::Is(Some((cs, r))) => IMObs {
            ijumped: j,
            irid: Spec::Is(Some(r)),
            icolv: Spec::Is(Some(cs)),
        },
    }
}

/// A cursor `At k` reads the stored entry that equals `k` under the collations. After a peer
/// deleted `('a', 1)` and inserted `('A', 1)` the cursor reads `('A', 1)` (SPEC 2c).
fn iread(d: IdxDef, s: &IStore, c: &ICursor) -> Spec<Option<IKey>> {
    if c.icnull {
        return Spec::Is(None);
    }
    match &c.icpos {
        IPos::Invalid => Spec::Is(None),
        IPos::At(k) => Spec::Is(s.iter().find(|k2| is_eq(cmp_key(d, k2, k))).cloned()),
        IPos::Matched(_) | IPos::Seeked(_) | IPos::Gone(_) | IPos::Lost => Spec::Unspec,
    }
}

// ---------------------------------------------------------------------------------------
// Keys and order (cmp_val, cmp_cols, cmp_key, cmp_pre, cmp_prefix, is_*)
// ---------------------------------------------------------------------------------------

/// Types decide first: NULL, then integers, then text. Two texts compare with the collation of
/// the column. A DESC column negates the result (SPEC 2c).
fn cmp_val(d: ColDef, a: &Val, b: &Val) -> Ordering {
    let asc = match (a, b) {
        (Val::Null, Val::Null) => Ordering::Equal,
        (Val::Null, _) => Ordering::Less,
        (_, Val::Null) => Ordering::Greater,
        (Val::Int(x), Val::Int(y)) => x.cmp(y),
        (Val::Int(_), Val::Text(_)) => Ordering::Less,
        (Val::Text(_), Val::Int(_)) => Ordering::Greater,
        (Val::Text(x), Val::Text(y)) => cmp_text(d.coll, x, y),
    };
    match d.order {
        Order::Asc => asc,
        Order::Desc => asc.reverse(),
    }
}

/// `NOCASE` folds only the ASCII letters. `RTRIM` drops trailing spaces (0x20) and then
/// compares bytes.
fn cmp_text(c: Coll, x: &str, y: &str) -> Ordering {
    let (x, y) = (x.as_bytes(), y.as_bytes());
    match c {
        Coll::Binary => x.cmp(y),
        Coll::NoCase => x.to_ascii_lowercase().cmp(&y.to_ascii_lowercase()),
        Coll::RTrim => rtrim(x).cmp(rtrim(y)),
    }
}

/// Drops trailing spaces. A tab or a newline stays.
fn rtrim(mut x: &[u8]) -> &[u8] {
    while let [rest @ .., b' '] = x {
        x = rest;
    }
    x
}

/// Lexicographic order on whole column lists; a shorter list that is a prefix is smaller.
pub fn cmp_cols(d: IdxDef, a: &[Val], b: &[Val]) -> Ordering {
    for (i, (x, y)) in a.iter().zip(b).enumerate() {
        match cmp_val(d.cols[i], x, y) {
            Ordering::Equal => {}
            c => return c,
        }
    }
    a.len().cmp(&b.len())
}

/// The rowid is always ASC.
pub fn cmp_key(d: IdxDef, a: &IKey, b: &IKey) -> Ordering {
    match cmp_cols(d, &a.0, &b.0) {
        Ordering::Equal => a.1.cmp(&b.1),
        c => c,
    }
}

/// Compare the first `|p|` columns of `a` with `p`. A key with fewer columns is smaller.
fn cmp_pre(d: IdxDef, a: &[Val], p: &[Val]) -> Ordering {
    for (i, y) in p.iter().enumerate() {
        let Some(x) = a.get(i) else {
            return Ordering::Less;
        };
        match cmp_val(d.cols[i], x, y) {
            Ordering::Equal => {}
            c => return c,
        }
    }
    Ordering::Equal
}

pub fn cmp_prefix(d: IdxDef, k: &IKey, p: &[Val]) -> Ordering {
    cmp_pre(d, &k.0, p)
}

fn is_ge(c: Ordering) -> bool {
    c != Ordering::Less
}

fn is_gt(c: Ordering) -> bool {
    c == Ordering::Greater
}

fn is_le(c: Ordering) -> bool {
    c != Ordering::Greater
}

fn is_lt(c: Ordering) -> bool {
    c == Ordering::Less
}

fn is_eq(c: Ordering) -> bool {
    c == Ordering::Equal
}

// ---------------------------------------------------------------------------------------
// Store (iins, idel, imem, iseek_*, imin, imax, isucc, ipred, has_prefix, ...)
// ---------------------------------------------------------------------------------------

/// A key that is equal to a stored key (under the collations) leaves the store as it is.
pub fn iins(d: IdxDef, k: &IKey, s: &IStore) -> IStore {
    let mut out = s.clone();
    if let Err(i) = out.binary_search_by(|k2| cmp_key(d, k2, k)) {
        out.insert(i, k.clone());
    }
    out
}

pub fn idel(d: IdxDef, k: &IKey, s: &IStore) -> IStore {
    s.iter()
        .filter(|k2| !is_eq(cmp_key(d, k, k2)))
        .cloned()
        .collect()
}

pub fn imem(d: IdxDef, k: &IKey, s: &IStore) -> bool {
    s.iter().any(|k2| is_eq(cmp_key(d, k, k2)))
}

/// First key whose prefix compares `>= p`, `> p`; last key whose prefix compares `<= p`,
/// `< p`.
pub fn iseek_ge(d: IdxDef, p: &[Val], s: &IStore) -> Option<IKey> {
    s.iter().find(|k| is_ge(cmp_prefix(d, k, p))).cloned()
}

pub fn iseek_gt(d: IdxDef, p: &[Val], s: &IStore) -> Option<IKey> {
    s.iter().find(|k| is_gt(cmp_prefix(d, k, p))).cloned()
}

pub fn iseek_le(d: IdxDef, p: &[Val], s: &IStore) -> Option<IKey> {
    s.iter().rev().find(|k| is_le(cmp_prefix(d, k, p))).cloned()
}

pub fn iseek_lt(d: IdxDef, p: &[Val], s: &IStore) -> Option<IKey> {
    s.iter().rev().find(|k| is_lt(cmp_prefix(d, k, p))).cloned()
}

pub fn imin(s: &IStore) -> Option<IKey> {
    s.first().cloned()
}

pub fn imax(s: &IStore) -> Option<IKey> {
    s.last().cloned()
}

/// Next and previous key in the full key order.
pub fn isucc(d: IdxDef, k: &IKey, s: &IStore) -> Option<IKey> {
    s.iter().find(|k2| is_gt(cmp_key(d, k2, k))).cloned()
}

pub fn ipred(d: IdxDef, k: &IKey, s: &IStore) -> Option<IKey> {
    s.iter().rev().find(|k2| is_lt(cmp_key(d, k2, k))).cloned()
}

pub fn has_prefix(d: IdxDef, p: &[Val], s: &IStore) -> bool {
    s.iter().any(|k| is_eq(cmp_prefix(d, k, p)))
}

pub fn has_null(p: &[Val]) -> bool {
    p.iter().any(|v| v == &Val::Null)
}

/// The key is above every key of the store: an append is legal.
pub fn above_all(d: IdxDef, k: &IKey, s: &IStore) -> bool {
    s.iter().all(|k2| is_lt(cmp_key(d, k2, k)))
}

// ---------------------------------------------------------------------------------------
// Harness bridge: map `IOp` onto `IMOp`, replay an engine run, compare observations
// ---------------------------------------------------------------------------------------

/// The model state plus the mapping of the harness `IOp`. `IOp` has ops that the model does
/// not know (`DeferredSeek`, `TblColumn`, `FinishSeek`); `apply` calls them illegal.
#[derive(Debug, Clone)]
pub struct IReplay {
    pub st: IState,
}

impl IReplay {
    /// `ephemeral`: the program of an ephemeral run writes the setup entries through cursor 0,
    /// so that cursor starts in a position that the contract does not define (`Lost`).
    pub fn new(rows: &[Row], nidx: usize, ephemeral: bool, idef: IdxDef) -> IReplay {
        let mut keys: IStore = rows
            .iter()
            .map(|(id, a, b)| (vec![a.clone(), b.clone()], *id))
            .collect();
        keys.sort_by(|x, y| cmp_key(idef, x, y));
        let mut st = iinit_with(keys, nidx, idef);
        if ephemeral && nidx > 0 {
            st.icurs[0].icpos = IPos::Lost;
        }
        IReplay { st }
    }

    /// Applies one harness op. `None` means the op is illegal.
    pub fn apply(&mut self, op: &IOp) -> Option<IMObs> {
        let a = Self::action_of(op)?;
        let (st2, o) = istep(&self.st, &a)?;
        self.st = st2;
        Some(o)
    }

    pub fn action_of(op: &IOp) -> Option<IAction> {
        let act = |iacur, iaop| Some(IAction { iacur, iaop });
        match op {
            IOp::Rewind(c) => act(*c, IMOp::IRewind),
            IOp::Last(c) => act(*c, IMOp::ILast),
            IOp::Next(c) => act(*c, IMOp::INext),
            IOp::Prev(c) => act(*c, IMOp::IPrev),
            IOp::SeekGE(c, p) => act(*c, IMOp::ISeekGE(p.clone())),
            IOp::SeekGT(c, p) => act(*c, IMOp::ISeekGT(p.clone())),
            IOp::SeekLE(c, p) => act(*c, IMOp::ISeekLE(p.clone())),
            IOp::SeekLT(c, p) => act(*c, IMOp::ISeekLT(p.clone())),
            IOp::Found(c, p) => act(*c, IMOp::IFound(p.clone())),
            IOp::NotFound(c, p) => act(*c, IMOp::INotFound(p.clone())),
            IOp::NoConflict(c, p) => act(*c, IMOp::INoConflict(p.clone())),
            IOp::IdxGE(c, p) => act(*c, IMOp::IdxGE(p.clone())),
            IOp::IdxGT(c, p) => act(*c, IMOp::IdxGT(p.clone())),
            IOp::IdxLE(c, p) => act(*c, IMOp::IdxLE(p.clone())),
            IOp::IdxLT(c, p) => act(*c, IMOp::IdxLT(p.clone())),
            IOp::IdxInsert {
                cur,
                a,
                b,
                rowid,
                use_seek,
            } => act(
                *cur,
                IMOp::IdxInsert((vec![a.clone(), b.clone()], *rowid), *use_seek),
            ),
            IOp::IdxDelete { cur, a, b, rowid } => {
                act(*cur, IMOp::IdxDelete((vec![a.clone(), b.clone()], *rowid)))
            }
            IOp::Delete(c) => act(*c, IMOp::IDelete),
            IOp::Column(c, _) | IOp::IdxRowId(c) | IOp::Observe(c) => act(*c, IMOp::IObserve),
            IOp::SeekEnd(c) => act(*c, IMOp::ISeekEnd),
            IOp::NullRow(c) => act(*c, IMOp::INullRow),
            IOp::Count(c) => act(*c, IMOp::ICount),
            IOp::Clear(c) => act(*c, IMOp::IClear),
            IOp::DeferredSeek { .. } | IOp::TblColumn(..) | IOp::FinishSeek(_) => None,
        }
    }
}

/// The four slots of an engine `IObs`, as the model says them: jump flag, `rowid`, `c0`, `c1`.
pub type Slots = (bool, Spec<Option<i64>>, Spec<Val>, Spec<Val>);

/// What the harness `IObs` of `op` must contain, given the model observation. A slot that the
/// op does not read is NULL.
pub fn project(op: &IOp, m: &IMObs) -> Slots {
    let null = Spec::Is(None);
    let vnull = || Spec::Is(Val::Null);
    let col = |i: usize| match &m.icolv {
        Spec::Unspec => Spec::Unspec,
        Spec::Is(None) => Spec::Is(Val::Null),
        Spec::Is(Some(cs)) => Spec::Is(cs.get(i).cloned().unwrap_or(Val::Null)),
    };
    match op {
        IOp::IdxRowId(_) | IOp::Count(_) => (m.ijumped, m.irid, vnull(), vnull()),
        IOp::Column(_, 0) => (m.ijumped, null, col(0), vnull()),
        IOp::Column(..) => (m.ijumped, null, vnull(), col(1)),
        IOp::Observe(_) => (m.ijumped, m.irid, col(0), col(1)),
        _ => (m.ijumped, null, vnull(), vnull()),
    }
}

fn agrees<A: PartialEq>(spec: &Spec<A>, got: &A) -> bool {
    match spec {
        Spec::Unspec => true,
        Spec::Is(a) => a == got,
    }
}

/// Engine `IObs` against the model observation, skipping fields the model leaves open.
pub fn iobs_agree(op: &IOp, engine: &IObs, m: &IMObs) -> bool {
    differing_fields(op, engine, m).is_empty()
}

/// Names the fields that differ, for the failure signature.
fn differing_fields(op: &IOp, engine: &IObs, m: &IMObs) -> String {
    let (j, rid, c0, c1) = project(op, m);
    let mut out = Vec::new();
    if engine.jumped != j {
        out.push("jumped");
    }
    if !agrees(&rid, &engine.rowid) {
        out.push("rowid");
    }
    if !agrees(&c0, &engine.c0) {
        out.push("c0");
    }
    if !agrees(&c1, &engine.c1) {
        out.push("c1");
    }
    out.join("+")
}

#[derive(Debug, Clone, PartialEq)]
pub enum IFailure {
    /// The engine returned an error or panicked.
    Engine(String),
    /// The engine returned a different number of rows than there are ops.
    Length { engine: usize, ops: usize },
    /// The observation differs from the model.
    Mismatch {
        step: usize,
        engine: IObs,
        model: IMObs,
    },
    /// Two engines differ where the model says nothing (see `compare_engines`).
    Cross { step: usize, a: IObs, b: IObs },
}

impl IFailure {
    /// A short name. The shrinker keeps it the same, so that it does not wander to another bug.
    pub fn signature(&self, ops: &[IOp]) -> String {
        let name = |step: usize| {
            let d = format!("{:?}", ops[step]);
            d.split(['(', ' ']).next().unwrap_or("").to_string()
        };
        match self {
            // The message tells two panics apart. Digits are dropped because they are
            // positions and sizes.
            IFailure::Engine(e) => format!(
                "engine-error:{}",
                e.chars()
                    .filter(|c| !c.is_ascii_digit())
                    .take(60)
                    .collect::<String>()
            ),
            IFailure::Length { .. } => "length".to_string(),
            IFailure::Mismatch {
                step,
                engine,
                model,
            } => format!(
                "mismatch:{}:{}",
                name(*step),
                differing_fields(&ops[*step], engine, model)
            ),
            IFailure::Cross { step, .. } => format!("cross:{}", name(*step)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IVerdict {
    /// The engine agrees with the model. Carries the model trace, for `compare_engines`.
    Pass(Vec<IMObs>),
    /// The model says the op at `step` is illegal and the engine agreed until then. This is
    /// not a failure: the test case is not a legal program.
    Skip {
        step: usize,
    },
    Fail(IFailure),
}

/// Replays `ops` on the model and compares every observation of the engine. When the engine
/// stopped with an error, the observations before it are compared first.
pub fn icheck(
    rows: &[Row],
    nidx: usize,
    ephemeral: bool,
    idef: IdxDef,
    ops: &[IOp],
    got: &Outcome,
) -> IVerdict {
    let mut rep = IReplay::new(rows, nidx, ephemeral, idef);
    let mut trace = Vec::new();
    for (step, (op, e)) in ops.iter().zip(&got.obs).enumerate() {
        let Some(m) = rep.apply(op) else {
            return IVerdict::Skip { step };
        };
        if !iobs_agree(op, e, &m) {
            return IVerdict::Fail(IFailure::Mismatch {
                step,
                engine: e.clone(),
                model: m,
            });
        }
        trace.push(m);
    }
    if let Some(error) = &got.error {
        // An op that the model calls illegal may fail in the engine without a bug.
        return match ops.get(got.obs.len()) {
            Some(op) if rep.apply(op).is_none() => IVerdict::Skip {
                step: got.obs.len(),
            },
            _ => IVerdict::Fail(IFailure::Engine(error.clone())),
        };
    }
    if got.obs.len() != ops.len() {
        return IVerdict::Fail(IFailure::Length {
            engine: got.obs.len(),
            ops: ops.len(),
        });
    }
    IVerdict::Pass(trace)
}

/// Compares two engine runs directly. A slot is skipped where either model trace says
/// `Unspec` (the contract leaves it open, so the engines may differ).
pub fn compare_engines(
    ops: &[IOp],
    a: (&[IObs], &[IMObs]),
    b: (&[IObs], &[IMObs]),
) -> Option<IFailure> {
    fn open<A: PartialEq>(x: &Spec<A>, y: &Spec<A>) -> bool {
        *x == Spec::Unspec || *y == Spec::Unspec
    }
    for (i, op) in ops.iter().enumerate() {
        let (aj, ar, a0, a1) = project(op, &a.1[i]);
        let (bj, br, b0, b1) = project(op, &b.1[i]);
        let differ = aj != bj
            || (!open(&ar, &br) && a.0[i].rowid != b.0[i].rowid)
            || (!open(&a0, &b0) && a.0[i].c0 != b.0[i].c0)
            || (!open(&a1, &b1) && a.0[i].c1 != b.0[i].c1);
        if differ {
            return Some(IFailure::Cross {
                step: i,
                a: a.0[i].clone(),
                b: b.0[i].clone(),
            });
        }
    }
    None
}

/// Is the whole program legal for the model?
pub fn is_legal(rows: &[Row], nidx: usize, ephemeral: bool, idef: IdxDef, ops: &[IOp]) -> bool {
    let mut rep = IReplay::new(rows, nidx, ephemeral, idef);
    ops.iter().all(|op| rep.apply(op).is_some())
}

/// Runs one config and panics unless the engine agrees with the model on every step. The
/// repro tests that the walker prints use it.
#[track_caller]
pub fn assert_agrees(
    kind: DbKind,
    layout: Layout,
    mode: ReadMode,
    rows: &[Row],
    nidx: usize,
    ops: &[IOp],
) {
    assert_agrees_def(kind, layout, mode, IdxDef::default(), rows, nidx, ops)
}

/// `assert_agrees` for an index with the definition `idef`.
#[track_caller]
pub fn assert_agrees_def(
    kind: DbKind,
    layout: Layout,
    mode: ReadMode,
    idef: IdxDef,
    rows: &[Row],
    nidx: usize,
    ops: &[IOp],
) {
    let cfg = IndexRun {
        kind,
        layout,
        rows,
        idef,
        nidx,
        ntbl: 0,
        mode,
    };
    let got = run_index(&cfg, ops);
    match icheck(rows, nidx, kind == DbKind::Ephemeral, idef, ops, &got) {
        IVerdict::Pass(_) => {}
        IVerdict::Skip { step } => panic!(
            "{kind:?}/{layout:?}: the model says op {step} ({:?}) is illegal",
            ops[step]
        ),
        IVerdict::Fail(f) => {
            let at = match &f {
                IFailure::Mismatch { step, .. } => format!(" at op {step} ({:?})", ops[*step]),
                _ => String::new(),
            };
            panic!("{kind:?}/{layout:?}/{mode:?}: engine differs from the model{at}: {f:?}\nengine: {got:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use IMOp::*;

    const NUL: Val = Val::Null;

    fn some(v: i64) -> Val {
        Val::int(v)
    }

    fn k(c1: Val, c2: Val, r: i64) -> IKey {
        (vec![c1, c2], r)
    }

    fn a(cur: usize, op: IMOp) -> IAction {
        IAction {
            iacur: cur,
            iaop: op,
        }
    }

    /// Observation of a read on the entry `k`, and of a read on nothing.
    fn kobs(j: bool, k: &IKey) -> IMObs {
        IMObs {
            ijumped: j,
            irid: Spec::Is(Some(k.1)),
            icolv: Spec::Is(Some(k.0.clone())),
        }
    }

    fn iend(j: bool) -> IMObs {
        IMObs {
            ijumped: j,
            irid: Spec::Is(None),
            icolv: Spec::Is(None),
        }
    }

    fn iunspec(j: bool) -> IMObs {
        IMObs {
            ijumped: j,
            irid: Spec::Unspec,
            icolv: Spec::Unspec,
        }
    }

    /// Seven entries (`k1` to `k7`); the key order is: NULL first, rowid breaks
    /// ties between equal columns.
    fn k1() -> IKey {
        k(NUL, some(1), 7)
    }
    fn k2() -> IKey {
        k(some(1), some(1), 4)
    }
    fn k3() -> IKey {
        k(some(1), some(2), 2)
    }
    fn k4() -> IKey {
        k(some(1), some(2), 9)
    }
    fn k5() -> IKey {
        k(some(2), NUL, 3)
    }
    fn k6() -> IKey {
        k(some(2), NUL, 8)
    }
    fn k7() -> IKey {
        k(some(3), some(1), 5)
    }
    fn ix() -> IStore {
        vec![k1(), k2(), k3(), k4(), k5(), k6(), k7()]
    }

    fn obs_of_run(n: usize, p: &[IAction]) -> Option<Vec<IMObs>> {
        irun(&iinit(ix(), n), p).map(|r| r.1)
    }

    fn some_p(vs: &[i64]) -> Cols {
        vs.iter().map(|v| Val::int(*v)).collect()
    }

    #[test]
    fn example_iscan() {
        let mut p = vec![a(0, IRewind)];
        p.extend((0..7).map(|_| a(0, INext)));
        let ks = [k1(), k2(), k3(), k4(), k5(), k6(), k7()];
        let mut want = vec![kobs(false, &ks[0])];
        want.extend(ks[1..].iter().map(|x| kobs(true, x)));
        want.push(iend(false));
        assert_eq!(obs_of_run(1, &p), Some(want));
    }

    #[test]
    fn example_iscan_back() {
        let mut p = vec![a(0, ILast)];
        p.extend((0..7).map(|_| a(0, IPrev)));
        let ks = [k7(), k6(), k5(), k4(), k3(), k2(), k1()];
        let mut want = vec![kobs(false, &ks[0])];
        want.extend(ks[1..].iter().map(|x| kobs(true, x)));
        want.push(iend(false));
        assert_eq!(obs_of_run(1, &p), Some(want));
    }

    #[test]
    fn example_iscan_empty() {
        let got = irun(&iinit(vec![], 1), &[a(0, IRewind), a(0, ILast)]).map(|r| r.1);
        assert_eq!(got, Some(vec![iend(true), iend(true)]));
    }

    #[test]
    fn example_iseek_short_prefix() {
        let p = [
            a(0, ISeekGE(vec![some(1)])),
            a(1, ISeekGT(vec![some(1)])),
            a(2, ISeekLE(vec![some(1)])),
            a(3, ISeekLT(vec![some(1)])),
        ];
        assert_eq!(
            obs_of_run(4, &p),
            Some(vec![
                kobs(false, &k2()),
                kobs(false, &k5()),
                kobs(false, &k4()),
                kobs(false, &k1())
            ])
        );
    }

    #[test]
    fn example_iseek_full_prefix() {
        let p = [
            a(0, ISeekGE(some_p(&[1, 2]))),
            a(1, ISeekGT(some_p(&[1, 2]))),
            a(2, ISeekLE(some_p(&[1, 2]))),
            a(3, ISeekLT(some_p(&[1, 2]))),
        ];
        assert_eq!(
            obs_of_run(4, &p),
            Some(vec![
                kobs(false, &k3()),
                kobs(false, &k5()),
                kobs(false, &k4()),
                kobs(false, &k2())
            ])
        );
    }

    #[test]
    fn example_iseek_empty_prefix() {
        let p = [
            a(0, ISeekGE(vec![])),
            a(1, ISeekGT(vec![])),
            a(2, ISeekLE(vec![])),
            a(3, ISeekLT(vec![])),
        ];
        assert_eq!(
            obs_of_run(4, &p),
            Some(vec![
                kobs(false, &k1()),
                iend(true),
                kobs(false, &k7()),
                iend(true)
            ])
        );
    }

    #[test]
    fn example_iseek_null() {
        let p = [a(0, ISeekGE(vec![NUL])), a(1, ISeekLE(vec![NUL]))];
        assert_eq!(
            obs_of_run(2, &p),
            Some(vec![kobs(false, &k1()), kobs(false, &k1())])
        );
    }

    #[test]
    fn example_iseek_miss() {
        let p = [a(0, ISeekGE(some_p(&[4]))), a(1, ISeekLT(vec![NUL]))];
        assert_eq!(obs_of_run(2, &p), Some(vec![iend(true), iend(true)]));
    }

    #[test]
    fn example_ifound_null() {
        let p = [
            a(0, IFound(vec![NUL, some(1)])),
            a(1, INotFound(vec![NUL, some(1)])),
            a(2, INoConflict(vec![NUL, some(1)])),
            a(3, INoConflict(some_p(&[1, 2]))),
            a(4, INoConflict(some_p(&[9, 9]))),
        ];
        assert_eq!(
            obs_of_run(5, &p),
            Some(vec![
                iunspec(true),
                iunspec(false),
                iunspec(true),
                iunspec(false),
                iunspec(true)
            ])
        );
    }

    #[test]
    fn example_ifound_miss() {
        let p = [a(0, IFound(some_p(&[9]))), a(1, INotFound(some_p(&[9])))];
        assert_eq!(obs_of_run(2, &p), Some(vec![iunspec(false), iunspec(true)]));
    }

    #[test]
    fn example_ifound_then_next() {
        let p = [a(0, IFound(some_p(&[1]))), a(0, INext)];
        assert_eq!(obs_of_run(1, &p), None);
    }

    #[test]
    fn example_iidx_compare() {
        let p = [
            a(0, ISeekGE(some_p(&[2]))),
            a(0, IdxGE(some_p(&[2]))),
            a(0, IdxGT(some_p(&[2]))),
            a(0, IdxLE(some_p(&[2]))),
            a(0, IdxLT(some_p(&[2]))),
            a(0, IdxGT(some_p(&[1]))),
            a(0, IdxLT(some_p(&[3]))),
        ];
        let k = k5();
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![
                kobs(false, &k),
                kobs(true, &k),
                kobs(false, &k),
                kobs(true, &k),
                kobs(false, &k),
                kobs(true, &k),
                kobs(true, &k)
            ])
        );
    }

    #[test]
    fn example_iidx_invalid() {
        let p = [a(0, IdxGE(some_p(&[1]))), a(0, IdxLT(some_p(&[1])))];
        assert_eq!(obs_of_run(1, &p), Some(vec![iend(true), iend(true)]));
    }

    /// The loop of a range scan: after a `SeekGE p`, repeat "`IdxGT p`: jump out, else read
    /// the entry and `Next`". Returns what the reads saw. Out of fuel is `None`.
    fn prefix_loop(fuel: usize, st: &IState, i: usize, p: &Cols) -> Option<Vec<IMObs>> {
        if fuel == 0 {
            return None;
        }
        let (st1, o1) = istep(st, &a(i, IdxGT(p.clone())))?;
        if o1.ijumped {
            return Some(vec![]);
        }
        let (st2, o2) = istep(&st1, &a(i, IObserve))?;
        let (st3, _) = istep(&st2, &a(i, INext))?;
        let mut rest = prefix_loop(fuel - 1, &st3, i, p)?;
        rest.insert(0, o2);
        Some(rest)
    }

    fn loop_after_seek_ge(p: Cols) -> Option<Vec<IMObs>> {
        let (st1, _) = istep(&iinit(ix(), 1), &a(0, ISeekGE(p.clone())))?;
        prefix_loop(10, &st1, 0, &p)
    }

    #[test]
    fn example_iloop_prefix() {
        assert_eq!(
            loop_after_seek_ge(some_p(&[1])),
            Some(vec![
                kobs(false, &k2()),
                kobs(false, &k3()),
                kobs(false, &k4())
            ])
        );
        assert_eq!(
            loop_after_seek_ge(some_p(&[2])),
            Some(vec![kobs(false, &k5()), kobs(false, &k6())])
        );
        // The loop also ends at the end of the tree.
        assert_eq!(
            loop_after_seek_ge(some_p(&[3])),
            Some(vec![kobs(false, &k7())])
        );
        // No entry with the prefix: the loop reads nothing.
        assert_eq!(loop_after_seek_ge(some_p(&[0])), Some(vec![]));
    }

    #[test]
    fn example_iinsert_noseek() {
        let key = k(some(1), some(2), 5);
        let got =
            irun(&iinit(ix(), 1), &[a(0, IdxInsert(key.clone(), false))]).map(|r| (r.0.irows, r.1));
        assert_eq!(
            got,
            Some((
                vec![k1(), k2(), k3(), key.clone(), k4(), k5(), k6(), k7()],
                vec![kobs(false, &key)]
            ))
        );
    }

    #[test]
    fn example_iinsert_useseek() {
        // No token: illegal.
        let p = [a(0, IdxInsert(k(some(1), some(3), 5), true))];
        assert_eq!(obs_of_run(1, &p), None);
        // After a miss, and after a hit: the token is for the columns of the key.
        let key = k(some(1), some(3), 5);
        let p = [
            a(0, INotFound(some_p(&[1, 3]))),
            a(0, IdxInsert(key.clone(), true)),
        ];
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![iunspec(true), kobs(false, &key)])
        );
        let key = k(some(1), some(2), 5);
        let p = [
            a(0, IFound(some_p(&[1, 2]))),
            a(0, IdxInsert(key.clone(), true)),
        ];
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![iunspec(true), kobs(false, &key)])
        );
    }

    #[test]
    fn example_iinsert_useseek_wrong_token() {
        let p = [
            a(0, INotFound(some_p(&[1, 3]))),
            a(0, IdxInsert(k(some(1), some(4), 5), true)),
        ];
        assert_eq!(obs_of_run(1, &p), None);
    }

    #[test]
    fn example_iinsert_useseek_peer_write() {
        let p = [
            a(0, INotFound(some_p(&[1, 3]))),
            a(1, IdxInsert(k(some(9), some(9), 1), false)),
            a(0, IdxInsert(k(some(1), some(3), 5), true)),
        ];
        assert_eq!(obs_of_run(2, &p), None);
    }

    #[test]
    fn example_iinsert_useseek_once() {
        let p = [
            a(0, INotFound(some_p(&[1, 3]))),
            a(0, IdxInsert(k(some(1), some(3), 5), true)),
            a(0, IdxInsert(k(some(1), some(3), 6), true)),
        ];
        assert_eq!(obs_of_run(1, &p), None);
    }

    #[test]
    fn example_iinsert_useseek_null() {
        let key = k(NUL, NUL, 5);
        let p = [a(0, IdxInsert(key.clone(), true))];
        assert_eq!(obs_of_run(1, &p), Some(vec![kobs(false, &key)]));
    }

    #[test]
    fn example_iinsert_duplicate() {
        let p = [a(0, IdxInsert(k3(), false))];
        assert_eq!(obs_of_run(1, &p), None);
    }

    #[test]
    fn example_iseekend() {
        // An append is legal; the same columns with a higher rowid too.
        let key = k(some(4), NUL, 1);
        let p = [a(0, ISeekEnd), a(0, IdxInsert(key.clone(), true))];
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![iunspec(false), kobs(false, &key)])
        );
        let key = k(some(3), some(1), 6);
        let p = [a(0, ISeekEnd), a(0, IdxInsert(key.clone(), true))];
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![iunspec(false), kobs(false, &key)])
        );
        // Not above every key, and equal to the maximum.
        let p = [
            a(0, ISeekEnd),
            a(0, IdxInsert(k(some(1), some(5), 1), true)),
        ];
        assert_eq!(obs_of_run(1, &p), None);
        let p = [
            a(0, ISeekEnd),
            a(0, IdxInsert(k(some(3), some(1), 5), true)),
        ];
        assert_eq!(obs_of_run(1, &p), None);
    }

    #[test]
    fn example_iidxdelete() {
        // Another cursor goes on with `Next` and skips the deleted entry.
        let p = [
            a(0, ISeekGE(some_p(&[1]))),
            a(1, IdxDelete(k3())),
            a(0, INext),
        ];
        assert_eq!(
            obs_of_run(2, &p),
            Some(vec![kobs(false, &k2()), iunspec(false), kobs(true, &k4())])
        );
        // The deleting cursor is `Lost`.
        let p = [a(0, IdxDelete(k3())), a(0, INext)];
        assert_eq!(obs_of_run(1, &p), None);
        // The key must be in the index.
        let p = [a(0, IdxDelete(k(some(7), NUL, 1)))];
        assert_eq!(obs_of_run(1, &p), None);
    }

    #[test]
    fn example_ipeer_delete() {
        let p = [
            a(0, ISeekGE(some_p(&[1, 2]))),
            a(1, IdxDelete(k3())),
            a(0, IObserve),
            a(0, INext),
        ];
        assert_eq!(
            obs_of_run(2, &p),
            Some(vec![
                kobs(false, &k3()),
                iunspec(false),
                iend(false),
                kobs(true, &k4())
            ])
        );
    }

    #[test]
    fn example_idelete() {
        let p = [a(0, ISeekGE(some_p(&[1, 2]))), a(0, IDelete), a(0, INext)];
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![kobs(false, &k3()), iunspec(false), kobs(true, &k4())])
        );
        let p = [a(0, ISeekLE(some_p(&[1, 2]))), a(0, IDelete), a(0, IPrev)];
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![kobs(false, &k4()), iunspec(false), kobs(true, &k3())])
        );
        let p = [
            a(0, ISeekGE(some_p(&[1, 2]))),
            a(0, IDelete),
            a(0, IObserve),
        ];
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![kobs(false, &k3()), iunspec(false), iunspec(false)])
        );
        // `Delete` needs a position on an entry.
        assert_eq!(obs_of_run(1, &[a(0, IDelete)]), None);
    }

    #[test]
    fn example_inull_row() {
        let p = [
            a(0, ISeekGE(some_p(&[1]))),
            a(0, INullRow),
            a(0, IObserve),
            a(0, ISeekGE(some_p(&[1]))),
            a(0, IObserve),
        ];
        assert_eq!(
            obs_of_run(1, &p),
            Some(vec![
                kobs(false, &k2()),
                iend(false),
                iend(false),
                kobs(false, &k2()),
                kobs(false, &k2())
            ])
        );
        // `Next`, `Count` and `Found` are illegal after `NullRow`.
        assert_eq!(
            obs_of_run(1, &[a(0, IRewind), a(0, INullRow), a(0, INext)]),
            None
        );
        assert_eq!(
            obs_of_run(1, &[a(0, IRewind), a(0, INullRow), a(0, ICount)]),
            None
        );
        assert_eq!(
            obs_of_run(1, &[a(0, INullRow), a(0, IFound(some_p(&[1])))]),
            None
        );
    }

    #[test]
    fn example_iclear_all() {
        let p = [
            a(0, IRewind),
            a(1, ILast),
            a(0, IClear),
            a(0, IObserve),
            a(1, IObserve),
            a(1, INext),
            a(0, ICount),
        ];
        assert_eq!(
            obs_of_run(2, &p),
            Some(vec![
                kobs(false, &k1()),
                kobs(false, &k7()),
                iend(false),
                iend(false),
                iend(false),
                iend(false),
                IMObs {
                    ijumped: false,
                    irid: Spec::Is(Some(0)),
                    icolv: Spec::Unspec
                }
            ])
        );
        // `Clear` and `SeekEnd` are illegal after `NullRow`.
        assert_eq!(obs_of_run(1, &[a(0, INullRow), a(0, IClear)]), None);
        assert_eq!(obs_of_run(1, &[a(0, INullRow), a(0, ISeekEnd)]), None);
    }

    #[test]
    fn example_icount() {
        assert_eq!(
            obs_of_run(1, &[a(0, ICount)]),
            Some(vec![IMObs {
                ijumped: false,
                irid: Spec::Is(Some(7)),
                icolv: Spec::Unspec
            }])
        );
    }

    // Column order and collation (SPEC 2c). The expected orders are what SQLite gives for an
    // index scan (checked with the `sqlite3` CLI).

    const ASC_BIN: ColDef = ColDef::new(Order::Asc, Coll::Binary);

    fn col(order: Order, coll: Coll) -> ColDef {
        ColDef::new(order, coll)
    }

    fn txt(x: &str) -> Val {
        Val::text(x)
    }

    /// The values in index order under the column definition `d`. Ties keep their input order.
    fn sorted(d: ColDef, vals: &[Val]) -> Vec<Val> {
        let mut out = vals.to_vec();
        out.sort_by(|x, y| cmp_val(d, x, y));
        out
    }

    #[test]
    fn order_default_is_asc_binary() {
        assert!(IdxDef::default().is_default());
        assert_eq!(IdxDef::default().cols, [ASC_BIN, ASC_BIN]);
        let vals = [txt("b"), some(2), NUL, txt("a"), some(1)];
        assert_eq!(
            sorted(ASC_BIN, &vals),
            vec![NUL, some(1), some(2), txt("a"), txt("b")]
        );
    }

    #[test]
    fn order_desc_text_first_null_last() {
        let vals = [txt("a"), some(1), NUL, some(2), txt("Z")];
        assert_eq!(
            sorted(col(Order::Desc, Coll::Binary), &vals),
            vec![txt("a"), txt("Z"), some(2), some(1), NUL]
        );
    }

    #[test]
    fn order_text_never_equals_integer() {
        for coll in [Coll::Binary, Coll::NoCase, Coll::RTrim] {
            let d = col(Order::Asc, coll);
            assert_eq!(cmp_val(d, &txt("1"), &some(1)), Ordering::Greater);
            assert_eq!(cmp_val(d, &some(1), &txt("1")), Ordering::Less);
        }
    }

    #[test]
    fn order_binary_by_byte() {
        let vals = [txt("a"), txt("`"), txt("_"), txt("["), txt("Z")];
        assert_eq!(
            sorted(ASC_BIN, &vals),
            vec![txt("Z"), txt("["), txt("_"), txt("`"), txt("a")]
        );
        // A shorter text that is a prefix goes first. A non-ASCII letter goes after ASCII.
        assert_eq!(cmp_val(ASC_BIN, &txt("a"), &txt("a ")), Ordering::Less);
        assert_eq!(cmp_val(ASC_BIN, &txt("z"), &txt("é")), Ordering::Less);
    }

    #[test]
    fn order_nocase_folds_only_ascii_letters() {
        let d = col(Order::Asc, Coll::NoCase);
        let vals = [txt("a"), txt("Z"), txt("`"), txt("_"), txt("["), txt("A")];
        // `a` and `A` are equal, so they keep the input order.
        assert_eq!(
            sorted(d, &vals),
            vec![txt("["), txt("_"), txt("`"), txt("a"), txt("A"), txt("Z")]
        );
        assert_eq!(cmp_val(d, &txt("a"), &txt("A")), Ordering::Equal);
        assert_eq!(cmp_val(d, &txt("é"), &txt("É")), Ordering::Greater);
        assert_eq!(cmp_val(d, &txt("a"), &txt("a ")), Ordering::Less);
    }

    #[test]
    fn order_rtrim_drops_trailing_spaces_only() {
        let d = col(Order::Asc, Coll::RTrim);
        assert_eq!(cmp_val(d, &txt(""), &txt(" ")), Ordering::Equal);
        assert_eq!(cmp_val(d, &txt(" "), &txt("  ")), Ordering::Equal);
        assert_eq!(cmp_val(d, &txt("a"), &txt("a ")), Ordering::Equal);
        assert_eq!(cmp_val(d, &txt("a"), &txt("a\t")), Ordering::Less);
        // Only the end is trimmed, and the case matters.
        assert_eq!(cmp_val(d, &txt(" a"), &txt("a")), Ordering::Less);
        assert_eq!(cmp_val(d, &txt("a"), &txt("A")), Ordering::Greater);
    }

    #[test]
    fn order_rowid_is_always_asc() {
        let d = IdxDef::new(
            col(Order::Desc, Coll::NoCase),
            col(Order::Desc, Coll::Binary),
        );
        let (x, y) = (k(txt("a"), some(1), 2), k(txt("A"), some(1), 5));
        assert_eq!(cmp_key(d, &x, &y), Ordering::Less);
        assert_eq!(cmp_key(d, &y, &x), Ordering::Greater);
        // The columns decide before the rowid, in the order of the column.
        let (x, y) = (k(some(1), some(1), 9), k(some(2), some(1), 1));
        assert_eq!(cmp_key(d, &x, &y), Ordering::Greater);
    }

    /// The index `(a DESC, b NOCASE)` with the rows of the SPEC 2c example.
    fn desc_nocase() -> (IdxDef, IStore) {
        let d = IdxDef::new(
            col(Order::Desc, Coll::Binary),
            col(Order::Asc, Coll::NoCase),
        );
        let rows = vec![
            k(some(3), NUL, 8),
            k(some(3), txt("z"), 7),
            k(some(2), txt("a"), 5),
            k(some(1), txt("B"), 3),
            k(some(1), txt("b"), 4),
            k(some(1), txt("a"), 2),
            k(NUL, txt("x"), 6),
        ];
        let mut keys = rows.clone();
        keys.sort_by(|x, y| cmp_key(d, x, y));
        (d, keys)
    }

    #[test]
    fn order_desc_nocase_index_order() {
        let (_, keys) = desc_nocase();
        let rowids: Vec<i64> = keys.iter().map(|k| k.1).collect();
        // `a` goes 3, 2, 1, NULL. In `a = 1`, `'a'` is before the tie of `'B'` and `'b'`.
        assert_eq!(rowids, vec![8, 7, 5, 2, 3, 4, 6]);
    }

    #[test]
    fn order_desc_nocase_seeks() {
        let (d, keys) = desc_nocase();
        let st = iinit_with(keys, 6, d);
        let seek = |cur: usize, op: IMOp| a(cur, op);
        let p = [
            seek(0, ISeekGE(vec![some(3)])),
            seek(1, ISeekGT(vec![some(3)])),
            seek(2, ISeekGE(vec![some(1), txt("B")])),
            seek(3, ISeekGT(vec![some(1), txt("B")])),
            seek(4, ISeekLE(vec![some(1), txt("B")])),
            seek(5, ISeekLT(vec![some(1), txt("B")])),
        ];
        let (_, obs) = irun(&st, &p).unwrap();
        let rowids: Vec<Option<i64>> = obs
            .iter()
            .map(|o| match o.irid {
                Spec::Is(r) => r,
                Spec::Unspec => panic!("unspecified"),
            })
            .collect();
        // SeekGE [3] is the first entry with a = 3, SeekGT [3] is a = 2. For [1, 'B'] the
        // tie of (1, 'B', 3) and (1, 'b', 4): GE the first, GT the entry after both, LE the
        // last, LT the entry before both.
        assert_eq!(
            rowids,
            vec![Some(8), Some(5), Some(3), Some(6), Some(4), Some(2)]
        );
    }

    #[test]
    fn order_desc_scan_and_prefix_compare() {
        // `Next` goes along the index order: for `a DESC` it goes to smaller values.
        let (d, keys) = desc_nocase();
        let st = iinit_with(keys, 1, d);
        let (st, _) = istep(&st, &a(0, ISeekGE(vec![some(2)]))).unwrap();
        let (_, o) = istep(&st, &a(0, INext)).unwrap();
        assert_eq!(o.irid, Spec::Is(Some(2)));
        // `IdxGT [2]` on the entry with a = 1 jumps: it is after the prefix in index order.
        let (st, _) = istep(&st, &a(0, INext)).unwrap();
        let (_, o) = istep(&st, &a(0, IdxGT(vec![some(2)]))).unwrap();
        assert!(o.ijumped);
        // `Found` ignores the order and the case.
        let (_, o) = istep(&st, &a(0, IFound(vec![some(1), txt("B")]))).unwrap();
        assert!(o.ijumped);
        let (_, o) = istep(&st, &a(0, IFound(vec![some(1), txt("c")]))).unwrap();
        assert!(!o.ijumped);
    }

    #[test]
    fn same_key_rules() {
        let d = IdxDef::new(col(Order::Asc, Coll::NoCase), ASC_BIN);
        let keys = vec![k(txt("a"), some(1), 1)];
        let st = iinit_with(keys, 1, d);
        let del = |key: IKey| a(0, IdxDelete(key));
        let ins = |key: IKey| a(0, IdxInsert(key, false));
        // An equal but not identical key: neither insert nor delete is legal.
        assert!(istep(&st, &ins(k(txt("A"), some(1), 1))).is_none());
        assert!(istep(&st, &del(k(txt("A"), some(1), 1))).is_none());
        // Another rowid is another key.
        assert!(istep(&st, &ins(k(txt("A"), some(1), 2))).is_some());
        // Delete, then insert the case variant: legal. A read gives the new bytes.
        let p = [
            a(0, IRewind),
            del(k(txt("a"), some(1), 1)),
            ins(k(txt("A"), some(1), 1)),
            a(0, IRewind),
        ];
        let (_, obs) = irun(&st, &p).unwrap();
        assert_eq!(obs[3], kobs(false, &k(txt("A"), some(1), 1)));
    }

    #[test]
    fn peer_reads_the_replacement_of_an_equal_key() {
        // Cursor 0 is on ('a', 1, 1). A peer replaces the entry by ('A', 1, 1). The cursor
        // stays on the entry, and reads the new bytes.
        let d = IdxDef::new(col(Order::Asc, Coll::NoCase), ASC_BIN);
        let st = iinit_with(vec![k(txt("a"), some(1), 1)], 2, d);
        let p = [
            a(0, IRewind),
            a(1, IdxDelete(k(txt("a"), some(1), 1))),
            a(1, IdxInsert(k(txt("A"), some(1), 1), false)),
            a(0, IObserve),
        ];
        let (_, obs) = irun(&st, &p).unwrap();
        assert_eq!(obs[3], kobs(false, &k(txt("A"), some(1), 1)));
    }

    #[test]
    fn rtrim_token_and_append_use_the_collation() {
        let d = IdxDef::new(col(Order::Asc, Coll::RTrim), ASC_BIN);
        let st = iinit_with(vec![k(txt("a"), some(1), 1)], 1, d);
        // The token of `NotFound ['b ', 1]` fits the key `['b', 1]`.
        let p = [
            a(0, INotFound(vec![txt("b "), some(1)])),
            a(0, IdxInsert(k(txt("b"), some(1), 5), true)),
        ];
        assert!(irun(&st, &p).is_some());
        // `SeekEnd`: `('a ', 1, 2)` is above `('a', 1, 1)`, but `('a ', 1, 0)` is below.
        let up = [
            a(0, ISeekEnd),
            a(0, IdxInsert(k(txt("a "), some(1), 2), true)),
        ];
        assert!(irun(&st, &up).is_some());
        let down = [
            a(0, ISeekEnd),
            a(0, IdxInsert(k(txt("a "), some(1), 0), true)),
        ];
        assert!(irun(&st, &down).is_none());
    }

    // The harness bridge.

    #[test]
    fn bridge_projects_reads_by_op() {
        let rows: Vec<Row> = vec![(1, some(1), some(10)), (2, NUL, some(5))];
        let mut rep = IReplay::new(&rows, 1, false, IdxDef::default());
        let ops = [
            IOp::Rewind(0),
            IOp::Column(0, 1),
            IOp::IdxRowId(0),
            IOp::Observe(0),
            IOp::Count(0),
        ];
        let trace: Vec<IMObs> = ops.iter().map(|op| rep.apply(op).unwrap()).collect();
        let mk = |jumped, rowid, c0, c1| IObs {
            jumped,
            rowid,
            c0,
            c1,
        };
        // The first entry in the order is (NULL, 5, rowid 2).
        assert!(iobs_agree(
            &ops[1],
            &mk(false, None, NUL, some(5)),
            &trace[1]
        ));
        assert!(!iobs_agree(
            &ops[1],
            &mk(false, Some(2), NUL, some(5)),
            &trace[1]
        ));
        assert!(iobs_agree(
            &ops[2],
            &mk(false, Some(2), NUL, NUL),
            &trace[2]
        ));
        assert!(iobs_agree(
            &ops[3],
            &mk(false, Some(2), NUL, some(5)),
            &trace[3]
        ));
        assert!(iobs_agree(
            &ops[4],
            &mk(false, Some(2), NUL, NUL),
            &trace[4]
        ));
    }

    #[test]
    fn bridge_ephemeral_cursor_starts_lost() {
        let rows: Vec<Row> = vec![(1, some(1), some(10))];
        // On a persistent database a fresh cursor is `Invalid`, so `IdxGE` jumps. In an
        // ephemeral run the position is open, so the model calls it illegal.
        assert!(is_legal(
            &rows,
            1,
            false,
            IdxDef::default(),
            &[IOp::IdxGE(0, vec![some(1)])]
        ));
        assert!(!is_legal(
            &rows,
            1,
            true,
            IdxDef::default(),
            &[IOp::IdxGE(0, vec![some(1)])]
        ));
        assert!(is_legal(
            &rows,
            1,
            true,
            IdxDef::default(),
            &[IOp::Rewind(0), IOp::Next(0)]
        ));
    }

    #[test]
    fn bridge_unknown_ops_are_illegal() {
        let rows: Vec<Row> = vec![];
        assert!(!is_legal(
            &rows,
            1,
            false,
            IdxDef::default(),
            &[IOp::FinishSeek(0)]
        ));
    }
}
