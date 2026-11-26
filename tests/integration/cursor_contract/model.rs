//! Reference model of table cursors (SPEC section 2).
//!
//! The model has these parts: `pos`, `dir`, `cursor`, `step`, `run`, `key_ge`, and so on.
//! A caller comes before the functions it calls.
//!
//! The model is the oracle of the random walk tester. It decides which ops are legal
//! (`step` returns `None` for an illegal op) and what an op may observe. A value that the
//! contract leaves open is `Spec::Unspec`, and the comparison skips it.

use super::program::{Obs, Op};
use super::{run as run_engine, DbKind, Layout};

// ---------------------------------------------------------------------------------------
// Types (tbl, pos, dir, cursor, state, op, action, spec, obs)
// ---------------------------------------------------------------------------------------

/// A list of `(rowid, value)` pairs sorted by rowid.
pub type Tbl = Vec<(i64, i64)>;

/// `Invalid`: not on a row. `At(k)`: on key `k`; if `k` is gone this is a gap between its
/// neighbours. `Seeked(k)`: an exact seek for `k` missed (or `NewRowid` chose `k`); only a
/// no-seek `Insert` of `k` may follow. `Gone(k)`: this cursor deleted `k`; like `At(k)` but
/// reading is undefined. `Lost`: position undefined (after `Count` and `SeekEnd`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Pos {
    Invalid,
    At(i64),
    Seeked(i64),
    Gone(i64),
    Lost,
}

/// Which way the last move went. SQLite only allows `Next` after a forward move and
/// `Prev` after a backward move.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Dir {
    DNone,
    Fwd,
    Bwd,
}

/// The seek token. `TKey(k)`: the last move was an exact seek (or `NewRowid`) for `k`.
/// `TApp`: the last move was `SeekEnd`; a no-seek `Insert` is then legal iff its key is above
/// every key in the table. `TNone`: no token.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(clippy::enum_variant_names)] // variant names are the token names of the contract
pub enum Tok {
    TNone,
    TKey(i64),
    TApp,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cursor {
    pub cpos: Pos,
    pub cdir: Dir,
    /// The NullRow flag.
    pub cnull: bool,
    /// The seek token, for a no-seek `Insert`.
    pub ctok: Tok,
}

/// A cursor without a position.
const LOST: Cursor = Cursor {
    cpos: Pos::Lost,
    cdir: Dir::DNone,
    cnull: false,
    ctok: Tok::TNone,
};

pub const FRESH_CURSOR: Cursor = Cursor {
    cpos: Pos::Invalid,
    cdir: Dir::DNone,
    cnull: false,
    ctok: Tok::TNone,
};

#[derive(Debug, Clone, PartialEq)]
pub struct State {
    pub rows: Tbl,
    pub curs: Vec<Cursor>,
}

/// The operations of the model. The harness `Op` is mapped onto this type by
/// `Replay::action_of`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MOp {
    Rewind,
    Last,
    Next,
    Prev,
    SeekGE(i64),
    SeekGT(i64),
    SeekLE(i64),
    SeekLT(i64),
    SeekRowid(i64),
    NotExists(i64),
    /// The argument is the rowid that the engine returned.
    NewRowid(i64),
    /// `(key, value, require_seek)`.
    Insert(i64, i64, bool),
    Delete,
    NullRow,
    Count,
    /// Position `Lost`, token "append".
    SeekEnd,
    /// Empties the table. Every cursor becomes `Invalid`.
    Clear,
    /// Reads the cursor and changes nothing.
    Observe,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Action {
    pub acur: usize,
    pub aop: MOp,
}

/// A value that the model may leave open.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Spec<A> {
    Unspec,
    Is(A),
}

/// What the model says after every action: the jump flag, `RowId` and `Column`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MObs {
    pub jumped: bool,
    pub orid: Spec<Option<i64>>,
    pub ocol: Spec<Option<i64>>,
}

// ---------------------------------------------------------------------------------------
// Run and step
// ---------------------------------------------------------------------------------------

pub fn init(rs: Tbl, n: usize) -> State {
    State {
        rows: rs,
        curs: vec![FRESH_CURSOR; n],
    }
}

/// Run a whole program; `None` if any action is illegal.
pub fn run(st: &State, p: &[Action]) -> Option<(State, Vec<MObs>)> {
    let mut st = st.clone();
    let mut os = Vec::new();
    for a in p {
        let (st2, o) = step(&st, a)?;
        st = st2;
        os.push(o);
    }
    Some((st, os))
}

/// One action. `None` means the action is illegal in this state.
pub fn step(st: &State, a: &Action) -> Option<(State, MObs)> {
    let c = *st.curs.get(a.acur)?;
    let s = &st.rows;
    let put = |c2: Cursor, j: bool| put_cursor(st, a.acur, c2, j);
    let seek = |o: Option<i64>, d: Dir| {
        let (c2, j) = move_to(o, d);
        put(c2, j)
    };
    // Next and Prev: a move that finds no row ends the scan and does not jump.
    let advance = |o: Option<i64>| match o {
        Some(k) => put(
            Cursor {
                cpos: Pos::At(k),
                cdir: c.cdir,
                cnull: false,
                ctok: Tok::TNone,
            },
            true,
        ),
        None => put(
            Cursor {
                cpos: Pos::Invalid,
                cdir: c.cdir,
                cnull: false,
                ctok: Tok::TNone,
            },
            false,
        ),
    };
    let exact = |k: i64| match lookup(k, s) {
        Some(_) => put(
            Cursor {
                cpos: Pos::At(k),
                cdir: Dir::Fwd,
                cnull: false,
                ctok: Tok::TKey(k),
            },
            false,
        ),
        None => put(
            Cursor {
                cpos: Pos::Seeked(k),
                cdir: Dir::DNone,
                cnull: false,
                ctok: Tok::TKey(k),
            },
            true,
        ),
    };
    match a.aop {
        MOp::Rewind => seek(key_min(s), Dir::Fwd),
        MOp::Last => seek(key_max(s), Dir::Bwd),
        MOp::SeekGE(k) => seek(key_ge(k, s), Dir::Fwd),
        MOp::SeekGT(k) => seek(key_gt(k, s), Dir::Fwd),
        MOp::SeekLE(k) => seek(key_le(k, s), Dir::Bwd),
        MOp::SeekLT(k) => seek(key_lt(k, s), Dir::Bwd),
        MOp::SeekRowid(k) | MOp::NotExists(k) => exact(k),
        // `Next` and `Prev` after `NullRow` find no row (SQLite: `OP_Next` on an invalid
        // cursor). The position afterwards is none.
        MOp::Next | MOp::Prev if c.cnull => put(LOST, false),
        MOp::Next => {
            if !c.cnull && c.cdir == Dir::Fwd {
                match c.cpos {
                    Pos::Invalid => put(c, false),
                    Pos::At(k) | Pos::Gone(k) => advance(key_gt(k, s)),
                    Pos::Seeked(_) | Pos::Lost => None,
                }
            } else {
                None
            }
        }
        MOp::Prev => {
            if !c.cnull && c.cdir == Dir::Bwd {
                match c.cpos {
                    Pos::Invalid => put(c, false),
                    Pos::At(k) | Pos::Gone(k) => advance(key_lt(k, s)),
                    Pos::Seeked(_) | Pos::Lost => None,
                }
            } else {
                None
            }
        }
        MOp::NewRowid(r) => {
            if !c.cnull && fresh(r, s) {
                put(
                    Cursor {
                        cpos: Pos::Seeked(r),
                        cdir: Dir::DNone,
                        cnull: false,
                        ctok: Tok::TKey(r),
                    },
                    false,
                )
            } else {
                None
            }
        }
        MOp::Insert(k, v, req) => {
            if !c.cnull && (req || tok_allows_insert(c.ctok, k, s)) {
                let s2 = ins(k, v, s);
                let c2 = Cursor {
                    cpos: Pos::At(k),
                    cdir: c.cdir,
                    cnull: false,
                    ctok: Tok::TNone,
                };
                Some(write_cursor(st, s2, a.acur, c2))
            } else {
                None
            }
        }
        MOp::Delete => match c.cpos {
            Pos::At(k) => {
                if !c.cnull && lookup(k, s).is_some() {
                    let s2 = del(k, s);
                    let c2 = Cursor {
                        cpos: Pos::Gone(k),
                        cdir: c.cdir,
                        cnull: false,
                        ctok: Tok::TNone,
                    };
                    Some(write_cursor(st, s2, a.acur, c2))
                } else {
                    None
                }
            }
            _ => None,
        },
        // A cursor without a position (`Lost`) cannot be set to the null row: Undefined (U9).
        MOp::NullRow => match c.cpos {
            Pos::Lost => None,
            _ => put(Cursor { cnull: true, ..c }, false),
        },
        MOp::Count => {
            if c.cnull {
                return None;
            }
            let c2 = Cursor {
                cpos: Pos::Lost,
                cdir: Dir::DNone,
                cnull: false,
                ctok: Tok::TNone,
            };
            let st2 = State {
                rows: s.clone(),
                curs: set_nth_cursor(&st.curs, a.acur, c2),
            };
            Some((
                st2,
                MObs {
                    jumped: false,
                    orid: Spec::Is(Some(s.len() as i64)),
                    ocol: Spec::Unspec,
                },
            ))
        }
        MOp::SeekEnd => {
            if c.cnull {
                return None;
            }
            put(
                Cursor {
                    cpos: Pos::Lost,
                    cdir: Dir::DNone,
                    cnull: false,
                    ctok: Tok::TApp,
                },
                false,
            )
        }
        MOp::Clear => {
            if c.cnull {
                return None;
            }
            let ended = Cursor {
                cpos: Pos::Invalid,
                cdir: Dir::Fwd,
                cnull: false,
                ctok: Tok::TNone,
            };
            Some((
                State {
                    rows: Vec::new(),
                    curs: vec![ended; st.curs.len()],
                },
                MObs {
                    jumped: false,
                    orid: Spec::Is(None),
                    ocol: Spec::Is(None),
                },
            ))
        }
        // A read needs a defined position. At `Seeked`, `Gone` and `Lost` it is Undefined (U8).
        MOp::Observe => match read_row(s, &c) {
            Spec::Unspec => None,
            Spec::Is(_) => put(c, false),
        },
    }
}

/// May a no-seek `Insert` of `k` go ahead? The cursor holds the token for `k`, or an append
/// token and `k` is above every key.
fn tok_allows_insert(t: Tok, k: i64, s: &Tbl) -> bool {
    match t {
        Tok::TKey(t) => t == k,
        Tok::TApp => fresh(k, s),
        Tok::TNone => false,
    }
}

/// The table stays the same, one cursor changes.
fn put_cursor(st: &State, i: usize, c2: Cursor, j: bool) -> Option<(State, MObs)> {
    Some((
        State {
            rows: st.rows.clone(),
            curs: set_nth_cursor(&st.curs, i, c2),
        },
        obs_of(&st.rows, &c2, j),
    ))
}

/// A write: new table, and every pending seek token ends.
fn write_cursor(st: &State, s2: Tbl, i: usize, c2: Cursor) -> (State, MObs) {
    let o = obs_of(&s2, &c2, false);
    (
        State {
            rows: s2,
            curs: set_nth_cursor(&lose_peers(&st.curs), i, c2),
        },
        o,
    )
}

fn move_to(o: Option<i64>, d: Dir) -> (Cursor, bool) {
    match o {
        Some(k) => (
            Cursor {
                cpos: Pos::At(k),
                cdir: d,
                cnull: false,
                ctok: Tok::TNone,
            },
            false,
        ),
        None => (
            Cursor {
                cpos: Pos::Invalid,
                cdir: d,
                cnull: false,
                ctok: Tok::TNone,
            },
            true,
        ),
    }
}

/// A write by a cursor leaves every cursor of the tree without a position, and ends every
/// pending seek token (U9). The writer gets its own position afterwards. A cursor without a
/// position can only be placed again by a seek, `Rewind` or `Last`. SQLite saves the other
/// cursors by key; Turso does not promise that, so a read or a move from the old position
/// is Undefined.
fn lose_peers(cs: &[Cursor]) -> Vec<Cursor> {
    cs.iter()
        .map(|_| Cursor {
            cpos: Pos::Lost,
            cdir: Dir::DNone,
            cnull: false,
            ctok: Tok::TNone,
        })
        .collect()
}

fn set_nth_cursor(cs: &[Cursor], i: usize, c: Cursor) -> Vec<Cursor> {
    let mut out = cs.to_vec();
    out[i] = c;
    out
}

// ---------------------------------------------------------------------------------------
// Observations
// ---------------------------------------------------------------------------------------

/// After every action the test reads the jump flag, `RowId` and `Column`.
fn obs_of(s: &Tbl, c: &Cursor, j: bool) -> MObs {
    match read_row(s, c) {
        Spec::Unspec => MObs {
            jumped: j,
            orid: Spec::Unspec,
            ocol: Spec::Unspec,
        },
        Spec::Is(None) => MObs {
            jumped: j,
            orid: Spec::Is(None),
            ocol: Spec::Is(None),
        },
        Spec::Is(Some((k, v))) => MObs {
            jumped: j,
            orid: Spec::Is(Some(k)),
            ocol: Spec::Is(Some(v)),
        },
    }
}

fn read_row(s: &Tbl, c: &Cursor) -> Spec<Option<(i64, i64)>> {
    if c.cnull {
        return Spec::Is(None);
    }
    match c.cpos {
        Pos::Invalid => Spec::Is(None),
        Pos::At(k) => Spec::Is(lookup(k, s).map(|v| (k, v))),
        Pos::Seeked(_) | Pos::Gone(_) | Pos::Lost => Spec::Unspec,
    }
}

// ---------------------------------------------------------------------------------------
// Table (ins, del, lookup, key_*, fresh)
// ---------------------------------------------------------------------------------------

/// A rowid that `NewRowid` may return: unused and above every key.
pub fn fresh(r: i64, s: &Tbl) -> bool {
    s.iter().all(|p| p.0 < r)
}

/// A rowid that the WAL engine returns. The generator uses it for `NewRowid`.
pub fn guess_rowid(s: &Tbl) -> i64 {
    key_max(s).map_or(1, |k| k + 1)
}

pub fn ins(k: i64, v: i64, s: &Tbl) -> Tbl {
    let mut out = s.clone();
    match out.binary_search_by_key(&k, |p| p.0) {
        Ok(i) => out[i] = (k, v),
        Err(i) => out.insert(i, (k, v)),
    }
    out
}

pub fn del(k: i64, s: &Tbl) -> Tbl {
    s.iter().copied().filter(|p| p.0 != k).collect()
}

pub fn lookup(k: i64, s: &Tbl) -> Option<i64> {
    s.iter().find(|p| p.0 == k).map(|p| p.1)
}

/// First key `>= k`, first key `> k`, last key `<= k`, last key `< k`.
pub fn key_ge(k: i64, s: &Tbl) -> Option<i64> {
    s.iter().find(|p| k <= p.0).map(|p| p.0)
}

pub fn key_gt(k: i64, s: &Tbl) -> Option<i64> {
    s.iter().find(|p| k < p.0).map(|p| p.0)
}

pub fn key_le(k: i64, s: &Tbl) -> Option<i64> {
    s.iter().rev().find(|p| p.0 <= k).map(|p| p.0)
}

pub fn key_lt(k: i64, s: &Tbl) -> Option<i64> {
    s.iter().rev().find(|p| p.0 < k).map(|p| p.0)
}

pub fn key_min(s: &Tbl) -> Option<i64> {
    s.first().map(|p| p.0)
}

pub fn key_max(s: &Tbl) -> Option<i64> {
    s.last().map(|p| p.0)
}

// ---------------------------------------------------------------------------------------
// Harness bridge: map `Op` onto `MOp`, replay an engine run, compare observations
// ---------------------------------------------------------------------------------------

/// The model state plus what the harness needs to translate `Op`: `InsertNew` uses the rowid
/// of the last `NewRowid` on the same cursor.
#[derive(Debug, Clone)]
pub struct Replay {
    pub st: State,
    pub last_new: Vec<Option<i64>>,
}

impl Replay {
    pub fn new(setup: &[(i64, i64)], ncursors: usize) -> Self {
        let mut rows = setup.to_vec();
        rows.sort();
        Replay {
            st: init(rows, ncursors),
            last_new: vec![None; ncursors],
        }
    }

    /// Applies one harness op. `observed` is the rowid the engine returned for `NewRowid`
    /// (the model only checks the predicate `fresh`). Without it the WAL rule is used.
    /// `None` means the op is illegal.
    pub fn apply(&mut self, op: &Op, observed: Option<i64>) -> Option<MObs> {
        let a = self.action_of(op, observed)?;
        let (st2, o) = step(&self.st, &a)?;
        if let MOp::NewRowid(r) = a.aop {
            self.last_new[a.acur] = Some(r);
        }
        self.st = st2;
        Some(o)
    }

    pub fn action_of(&self, op: &Op, observed: Option<i64>) -> Option<Action> {
        let act = |acur, aop| Some(Action { acur, aop });
        match *op {
            Op::Rewind(c) => act(c, MOp::Rewind),
            Op::Last(c) => act(c, MOp::Last),
            Op::Next(c) => act(c, MOp::Next),
            Op::Prev(c) => act(c, MOp::Prev),
            Op::SeekRowid(c, k) => act(c, MOp::SeekRowid(k)),
            Op::SeekGE(c, k) => act(c, MOp::SeekGE(k)),
            Op::SeekGT(c, k) => act(c, MOp::SeekGT(k)),
            Op::SeekLE(c, k) => act(c, MOp::SeekLE(k)),
            Op::SeekLT(c, k) => act(c, MOp::SeekLT(k)),
            Op::NotExists(c, k) => act(c, MOp::NotExists(k)),
            Op::NewRowid(c) => {
                let r = observed.unwrap_or_else(|| guess_rowid(&self.st.rows));
                act(c, MOp::NewRowid(r))
            }
            Op::Insert {
                cur,
                rowid,
                value,
                require_seek,
            } => act(cur, MOp::Insert(rowid, value, require_seek)),
            Op::InsertNew {
                cur,
                value,
                require_seek,
            } => {
                let k = self.last_new.get(cur).copied().flatten()?;
                act(cur, MOp::Insert(k, value, require_seek))
            }
            Op::Delete(c) => act(c, MOp::Delete),
            // The model keeps no per-column state. Column 0 is the rowid alias, which the
            // harness does not use.
            Op::Column(c, col) => {
                assert_eq!(col, 1, "the model only knows column 1 (v)");
                act(c, MOp::Observe)
            }
            Op::RowId(c) => act(c, MOp::Observe),
            Op::NullRow(c) => act(c, MOp::NullRow),
            Op::Count(c) => act(c, MOp::Count),
        }
    }
}

/// What the harness `Obs` of `op` must contain, given the model observation: the jump
/// flag, the `rowid` slot and the `col` slot. A slot that the op does not read is NULL.
fn project(op: &Op, m: &MObs) -> (bool, Spec<Option<i64>>, Spec<Option<i64>>) {
    match op {
        Op::RowId(_) | Op::Count(_) => (m.jumped, m.orid, Spec::Is(None)),
        Op::Column(..) => (m.jumped, Spec::Is(None), m.ocol),
        // The engine chooses the rowid, the model only checks that it is fresh.
        Op::NewRowid(_) => (m.jumped, Spec::Unspec, Spec::Is(None)),
        _ => (m.jumped, Spec::Is(None), Spec::Is(None)),
    }
}

fn agrees<A: PartialEq>(spec: &Spec<A>, got: &A) -> bool {
    match spec {
        Spec::Unspec => true,
        Spec::Is(a) => a == got,
    }
}

/// Engine `Obs` against the model observation, skipping fields the model leaves open.
pub fn obs_agree(op: &Op, engine: &Obs, m: &MObs) -> bool {
    let (j, rid, col) = project(op, m);
    engine.jumped == j && agrees(&rid, &engine.rowid) && agrees(&col, &engine.col)
}

/// Names the fields that differ, for the failure signature.
fn differing_fields(op: &Op, engine: &Obs, m: &MObs) -> String {
    let (j, rid, col) = project(op, m);
    let mut out = Vec::new();
    if engine.jumped != j {
        out.push("jumped");
    }
    if !agrees(&rid, &engine.rowid) {
        out.push("rowid");
    }
    if !agrees(&col, &engine.col) {
        out.push("col");
    }
    out.join("+")
}

#[derive(Debug, Clone, PartialEq)]
pub enum Failure {
    /// The engine returned an error or panicked.
    Engine(String),
    /// The engine returned a different number of rows than there are ops.
    Length { engine: usize, ops: usize },
    /// `NewRowid` returned NULL or a rowid that is not fresh.
    BadRowid { step: usize, rowid: Option<i64> },
    /// The observation differs from the model.
    Mismatch {
        step: usize,
        engine: Obs,
        model: MObs,
    },
    /// WAL and MVCC differ where the model says nothing (see `compare_engines`).
    Cross { step: usize, wal: Obs, mvcc: Obs },
}

impl Failure {
    /// A short name. The shrinker keeps it the same, so that it does not wander to another bug.
    pub fn signature(&self, ops: &[Op]) -> String {
        let name = |step: usize| {
            let d = format!("{:?}", ops[step]);
            d.split(['(', ' ']).next().unwrap_or("").to_string()
        };
        match self {
            Failure::Engine(_) => "engine-error".to_string(),
            Failure::Length { .. } => "length".to_string(),
            Failure::BadRowid { .. } => "bad-rowid".to_string(),
            Failure::Mismatch {
                step,
                engine,
                model,
            } => format!(
                "mismatch:{}:{}",
                name(*step),
                differing_fields(&ops[*step], engine, model)
            ),
            Failure::Cross { step, .. } => format!("cross:{}", name(*step)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// The engine agrees with the model. Carries the model trace, for `compare_engines`.
    Pass(Vec<MObs>),
    /// The model says the op at `step` is illegal and the engine agreed until then. This is
    /// not a failure: the test case is not a legal program.
    Skip {
        step: usize,
    },
    Fail(Failure),
}

/// Replays `ops` on the model with the rowids that the engine returned and compares every
/// observation. `allow_illegal` is for the generator without the D4 filter: an engine
/// error after a model-illegal op is then a failure too.
pub fn check(
    setup: &[(i64, i64)],
    ncursors: usize,
    ops: &[Op],
    got: &Result<Vec<Obs>, String>,
    allow_illegal: bool,
) -> Verdict {
    let mut rep = Replay::new(setup, ncursors);
    let obs = match got {
        Err(e) => {
            let illegal = ops.iter().position(|op| rep.apply(op, None).is_none());
            return match illegal {
                Some(step) if !allow_illegal => Verdict::Skip { step },
                _ => Verdict::Fail(Failure::Engine(e.clone())),
            };
        }
        Ok(obs) => obs,
    };
    if obs.len() != ops.len() {
        return Verdict::Fail(Failure::Length {
            engine: obs.len(),
            ops: ops.len(),
        });
    }
    let mut trace = Vec::new();
    for (step, (op, e)) in ops.iter().zip(obs).enumerate() {
        let observed = if matches!(op, Op::NewRowid(_)) {
            if e.rowid.is_none_or(|r| !fresh(r, &rep.st.rows)) {
                return Verdict::Fail(Failure::BadRowid {
                    step,
                    rowid: e.rowid,
                });
            }
            e.rowid
        } else {
            None
        };
        let Some(m) = rep.apply(op, observed) else {
            return Verdict::Skip { step };
        };
        if !obs_agree(op, e, &m) {
            return Verdict::Fail(Failure::Mismatch {
                step,
                engine: *e,
                model: m,
            });
        }
        trace.push(m);
    }
    Verdict::Pass(trace)
}

/// Compares the WAL and MVCC results of one layout directly. A slot is skipped where
/// either model trace says `Unspec` (the contract leaves it open, so the engines may
/// differ). Nothing is compared if the engines chose different `NewRowid` values, because
/// the two runs are then different programs.
pub fn compare_engines(
    ops: &[Op],
    wal: (&[Obs], &[MObs]),
    mvcc: (&[Obs], &[MObs]),
) -> Option<Failure> {
    let (w, wt) = wal;
    let (m, mt) = mvcc;
    for (i, op) in ops.iter().enumerate() {
        if matches!(op, Op::NewRowid(_)) && w[i].rowid != m[i].rowid {
            return None;
        }
    }
    for (i, op) in ops.iter().enumerate() {
        let (wj, wr, wc) = project(op, &wt[i]);
        let (mj, mr, mc) = project(op, &mt[i]);
        let open =
            |a: &Spec<Option<i64>>, b: &Spec<Option<i64>>| *a == Spec::Unspec || *b == Spec::Unspec;
        let differ = wj != mj
            || (!open(&wr, &mr) && w[i].rowid != m[i].rowid)
            || (!open(&wc, &mc) && w[i].col != m[i].col);
        if differ {
            return Some(Failure::Cross {
                step: i,
                wal: w[i],
                mvcc: m[i],
            });
        }
    }
    None
}

/// Runs one config and panics unless the engine agrees with the model on every step.
/// The repro tests that the walker prints use it.
#[track_caller]
pub fn assert_agrees(
    kind: DbKind,
    layout: Layout,
    setup: &[(i64, i64)],
    ncursors: usize,
    ops: &[Op],
) {
    if let Some(step) = agree_prefix(kind, layout, setup, ncursors, ops) {
        panic!(
            "{kind:?}/{layout:?}: the model says op {step} ({:?}) is illegal",
            ops[step]
        );
    }
}

/// Like `assert_agrees`, but an op that the model calls illegal ends the comparison
/// without a failure. The hand-written scenarios use it: `delete_then_prev` does a `Prev`
/// after `SeekRowid`, and the contract (D3) leaves that open.
#[track_caller]
pub fn assert_agrees_prefix(
    kind: DbKind,
    layout: Layout,
    setup: &[(i64, i64)],
    ncursors: usize,
    ops: &[Op],
) {
    agree_prefix(kind, layout, setup, ncursors, ops);
}

/// Returns the first step that the model calls illegal, if any. Panics on a mismatch.
#[track_caller]
fn agree_prefix(
    kind: DbKind,
    layout: Layout,
    setup: &[(i64, i64)],
    ncursors: usize,
    ops: &[Op],
) -> Option<usize> {
    let got = run_engine(kind, layout, setup, ncursors, ops);
    match check(setup, ncursors, ops, &got, false) {
        Verdict::Pass(_) => None,
        Verdict::Skip { step } => Some(step),
        Verdict::Fail(f) => {
            let step = match &f {
                Failure::Mismatch { step, .. } | Failure::BadRowid { step, .. } => Some(*step),
                _ => None,
            };
            let at = step.map_or(String::new(), |s| format!(" at op {s} ({:?})", ops[s]));
            panic!("{kind:?}/{layout:?}: engine differs from the model{at}: {f:?}\nengine: {got:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use MOp::*;

    fn is(k: i64, v: i64) -> MObs {
        MObs {
            jumped: false,
            orid: Spec::Is(Some(k)),
            ocol: Spec::Is(Some(v)),
        }
    }

    fn jumped(mut o: MObs) -> MObs {
        o.jumped = true;
        o
    }

    const NONE: MObs = MObs {
        jumped: false,
        orid: Spec::Is(None),
        ocol: Spec::Is(None),
    };
    const OPEN: MObs = MObs {
        jumped: false,
        orid: Spec::Unspec,
        ocol: Spec::Unspec,
    };

    fn t3() -> Tbl {
        vec![(1, 10), (3, 30), (5, 50)]
    }

    fn acts(p: &[(usize, MOp)]) -> Vec<Action> {
        p.iter().map(|&(acur, aop)| Action { acur, aop }).collect()
    }

    fn obs_of_run(n: usize, p: &[(usize, MOp)]) -> Vec<MObs> {
        run(&init(t3(), n), &acts(p)).expect("legal").1
    }

    // Examples of the model with hand-checked expected values.

    #[test]
    fn example_scan() {
        let got = obs_of_run(1, &[(0, Rewind), (0, Next), (0, Next), (0, Next)]);
        assert_eq!(got, [is(1, 10), jumped(is(3, 30)), jumped(is(5, 50)), NONE]);
    }

    #[test]
    fn example_delete_gap_next() {
        let got = obs_of_run(1, &[(0, Rewind), (0, Next), (0, Delete), (0, Next)]);
        assert_eq!(got, [is(1, 10), jumped(is(3, 30)), OPEN, jumped(is(5, 50))]);
    }

    #[test]
    fn example_delete_gap_prev() {
        let got = obs_of_run(1, &[(0, Last), (0, Prev), (0, Delete), (0, Prev)]);
        assert_eq!(got, [is(5, 50), jumped(is(3, 30)), OPEN, jumped(is(1, 10))]);
    }

    /// A write by another cursor leaves our cursor without a position (U9): a read or a move
    /// from the old position is illegal. A seek places it again.
    #[test]
    fn example_peer_write_loses_the_position() {
        let before = [(0, Rewind), (0, Next), (1, SeekRowid(3)), (1, Delete)];
        for op in [Observe, Next, Prev, Delete] {
            let mut p = before.to_vec();
            p.push((0, op));
            assert!(!legal(2, &p), "{op:?}");
        }
        let mut p = before.to_vec();
        p.extend([(0, SeekGE(3)), (0, Observe)]);
        assert!(legal(2, &p));
    }

    fn legal(n: usize, p: &[(usize, MOp)]) -> bool {
        run(&init(t3(), n), &acts(p)).is_some()
    }

    // Examples about `NullRow`, `Count`, `SeekEnd` and `Clear`.

    #[test]
    fn example_null_newrowid_and_count_are_illegal() {
        assert!(!legal(1, &[(0, Rewind), (0, NullRow), (0, NewRowid(9))]));
        assert!(!legal(1, &[(0, Rewind), (0, NullRow), (0, Count)]));
        // `Insert` and `Delete` after `NullRow` are illegal. `Next` finds no row.
        assert!(legal(1, &[(0, Rewind), (0, NullRow), (0, Next)]));
        assert!(!legal(
            1,
            &[(0, Rewind), (0, NullRow), (0, Insert(2, 1, true))]
        ));
        assert!(!legal(1, &[(0, Rewind), (0, NullRow), (0, Delete)]));
        assert!(!legal(1, &[(0, Rewind), (0, NullRow), (0, SeekEnd)]));
        assert!(!legal(1, &[(0, Rewind), (0, NullRow), (0, Clear)]));
        // A seek clears the flag.
        assert!(legal(
            1,
            &[(0, Rewind), (0, NullRow), (0, SeekGE(2)), (0, Count)]
        ));
    }

    #[test]
    fn example_count_ok() {
        let got = obs_of_run(1, &[(0, Count)]);
        assert_eq!(
            got,
            [MObs {
                jumped: false,
                orid: Spec::Is(Some(3)),
                ocol: Spec::Unspec
            }]
        );
    }

    #[test]
    fn example_seekend_insert_above() {
        let got = obs_of_run(1, &[(0, SeekEnd), (0, Insert(6, 60, false))]);
        assert_eq!(got, [OPEN, is(6, 60)]);
    }

    #[test]
    fn example_seekend_insert_not_above() {
        // Equal to the maximum, and below it.
        assert!(!legal(1, &[(0, SeekEnd), (0, Insert(5, 55, false))]));
        assert!(!legal(1, &[(0, SeekEnd), (0, Insert(4, 40, false))]));
    }

    #[test]
    fn example_seekend_insert_seek() {
        let got = obs_of_run(1, &[(0, SeekEnd), (0, Insert(4, 40, true))]);
        assert_eq!(got, [OPEN, is(4, 40)]);
    }

    #[test]
    fn example_insert_noseek_no_token() {
        assert!(!legal(1, &[(0, Insert(6, 60, false))]));
    }

    #[test]
    fn example_seekend_token_cleared_by_peer_write() {
        assert!(!legal(
            2,
            &[
                (0, SeekEnd),
                (1, Insert(4, 40, true)),
                (0, Insert(9, 90, false))
            ]
        ));
    }

    #[test]
    fn example_seekend_token_used_once() {
        assert!(!legal(
            1,
            &[
                (0, SeekEnd),
                (0, Insert(6, 60, false)),
                (0, Insert(7, 70, false))
            ]
        ));
    }

    #[test]
    fn example_clear_all() {
        let got = obs_of_run(
            2,
            &[
                (0, Rewind),
                (1, Last),
                (0, Clear),
                (0, Observe),
                (1, Observe),
                (1, Next),
                (0, Count),
            ],
        );
        assert_eq!(
            got,
            [
                is(1, 10),
                is(5, 50),
                NONE,
                NONE,
                NONE,
                NONE,
                MObs {
                    jumped: false,
                    orid: Spec::Is(Some(0)),
                    ocol: Spec::Unspec
                }
            ]
        );
    }

    #[test]
    fn example_clear_rows() {
        let run = run(&init(t3(), 2), &acts(&[(1, Clear)])).expect("legal");
        assert!(run.0.rows.is_empty());
    }

    #[test]
    fn illegal_actions() {
        let st = init(t3(), 1);
        // Next on a fresh cursor, Delete without a row, Insert without a token.
        for op in [Next, Prev, Delete, Insert(2, 1, false), NewRowid(5)] {
            assert!(step(&st, &Action { acur: 0, aop: op }).is_none(), "{op:?}");
        }
        assert!(step(
            &st,
            &Action {
                acur: 1,
                aop: Observe
            }
        )
        .is_none());
    }
}
