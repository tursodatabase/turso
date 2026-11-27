//! Seeded random walks over the cursor contract.
//!
//! The generator runs the model (`model.rs`) next to the program it writes, so it only
//! emits legal ops. Every sequence runs on WAL and MVCC, with the setup rows in memory and
//! checkpointed, and the engine result is compared with the model and with the other engine.
//! A failing sequence is shrunk and printed as a Rust test body.
//!
//! Environment:
//! - `SEED`: seed of the first case (decimal or `0x` hex). Case `i` uses `SEED + i`, so the
//!   seed in a failure report reproduces that case with `ITERS=1`.
//! - `ITERS`: number of cases.
//! - `NO_FILTER=1`: switch off the known gap filters, to see the known gaps again.

use super::model::{check, compare_engines, lookup, Failure, MObs, Pos, Replay, Tok, Verdict};
use super::program::{Obs, Op};
use super::{run as run_engine, DbKind, Layout};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::HashSet;

/// The keys of the walk are in `0..=KEY_MAX`.
const KEY_MAX: i64 = 26;
const MAX_OPS: usize = 25;
const CONFIGS: [(DbKind, Layout); 4] = [
    (DbKind::Wal, Layout::AllInMemory),
    (DbKind::Wal, Layout::Checkpointed),
    (DbKind::Mvcc, Layout::AllInMemory),
    (DbKind::Mvcc, Layout::Checkpointed),
];

// ---------------------------------------------------------------------------------------
// Known gaps
// ---------------------------------------------------------------------------------------

/// A set of generator filters. Each bit avoids the exact pattern of one known engine gap
/// (see SPEC.md section 5 and the `known_bug_*` tests in `mod.rs`), so that a walk does not
/// find the same bug again and again.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KnownGaps(u32);

impl KnownGaps {
    /// MVCC with checkpointed rows: `Next` or `Prev` right after an `Insert` loses the position.
    pub const D1: KnownGaps = KnownGaps(1 << 2);
    /// MVCC with checkpointed rows: `Column` after an update by `Insert` reads the old record.
    pub const G4: KnownGaps = KnownGaps(1 << 3);
    /// `Delete` on a cursor without a row panics. With this filter on, `Delete` is only
    /// emitted when the model says it is legal.
    pub const D4: KnownGaps = KnownGaps(1 << 4);

    /// MVCC: `Next` after `SeekRowid` or `NotExists` that hit does not find the next row.
    pub const N1: KnownGaps = KnownGaps(1 << 5);

    /// WAL: `NewRowid` on a cursor with the null-row flag returns 1, a rowid that may be in
    /// use, and `Count` keeps the flag. SPEC section 2 makes both ops illegal
    /// after `NullRow`, so the model already keeps them out of every walk. The bit stays so
    /// that the numbering of the gaps does not change.
    pub const N3: KnownGaps = KnownGaps(1 << 7);
    /// MVCC with checkpointed rows: `Delete` of a row that an `Insert` of this run wrote
    /// (added or updated) loses the position (`Next` or `Prev` after it find nothing) or panics.
    pub const N4: KnownGaps = KnownGaps(1 << 8);
    /// G3: WAL and ephemeral: a cursor with the null-row flag and a write by another cursor
    /// since its last move reads the real row instead of NULL.
    pub const G3: KnownGaps = KnownGaps(1 << 9);
    /// PD1: WAL and ephemeral: `Next` or `Prev` from the gap of an own `Delete`, after a write
    /// by another cursor, skips the new row or panics (`self.current_page >= 0`).
    pub const PD1: KnownGaps = KnownGaps(1 << 10);

    pub const NONE: KnownGaps = KnownGaps(0);
    pub const ALL: KnownGaps = KnownGaps(0b111_1011_1100);

    pub fn has(self, other: KnownGaps) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn from_env() -> KnownGaps {
        if std::env::var("NO_FILTER").is_ok_and(|v| v != "0" && !v.is_empty()) {
            KnownGaps::NONE
        } else {
            KnownGaps::ALL
        }
    }
}

// ---------------------------------------------------------------------------------------
// Case
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Case {
    pub setup: Vec<(i64, i64)>,
    pub ncursors: usize,
    pub ops: Vec<Op>,
}

// ---------------------------------------------------------------------------------------
// Walk
// ---------------------------------------------------------------------------------------

pub struct WalkConfig {
    pub seed: u64,
    pub iters: usize,
    pub gaps: KnownGaps,
}

impl WalkConfig {
    pub fn from_env(default_iters: usize) -> WalkConfig {
        let seed = std::env::var("SEED").ok().map_or(0x5EED_C0DE, |s| {
            match s.strip_prefix("0x") {
                Some(hex) => u64::from_str_radix(hex, 16),
                None => s.parse(),
            }
            .expect("SEED must be a number")
        });
        let iters = std::env::var("ITERS").ok().map_or(default_iters, |s| {
            s.parse().expect("ITERS must be a number")
        });
        WalkConfig {
            seed,
            iters,
            gaps: KnownGaps::from_env(),
        }
    }
}

/// Runs the walk and returns one report per distinct failure (after shrinking).
pub fn walk(cfg: &WalkConfig) -> Vec<String> {
    let mut reports = Vec::new();
    let mut seen = HashSet::new();
    let (mut ran, mut skipped) = (0, 0);
    for i in 0..cfg.iters {
        let case_seed = cfg.seed.wrapping_add(i as u64);
        let mut rng = ChaCha8Rng::seed_from_u64(case_seed);
        let filter = Filter {
            gaps: cfg.gaps,
            scope: random_scope(&mut rng),
        };
        let case = generate(&mut rng, &filter);
        let (r, s) = run_case(&case, &filter, case_seed, &mut seen, &mut reports);
        ran += r;
        skipped += s;
    }
    eprintln!(
        "cursor walk: seed {:#x}, {} cases, {ran} engine runs, {skipped} skipped, {} distinct failures",
        cfg.seed,
        cfg.iters,
        reports.len()
    );
    reports
}

fn random_scope(rng: &mut ChaCha8Rng) -> Vec<(DbKind, Layout)> {
    match rng.random_range(0..10) {
        0..=3 => CONFIGS.to_vec(),
        4 => CONFIGS[..2].to_vec(),
        5 => CONFIGS[2..].to_vec(),
        _ => vec![CONFIGS[rng.random_range(0..4)]],
    }
}

/// Runs one case on the given configs. Returns (engine runs, skipped runs).
fn run_case(
    case: &Case,
    filter: &Filter,
    case_seed: u64,
    seen: &mut HashSet<String>,
    reports: &mut Vec<String>,
) -> (usize, usize) {
    let configs = &filter.scope;
    let allow_illegal = !filter.gaps.has(KnownGaps::D4);
    let results: Vec<_> = configs
        .iter()
        .map(|&(k, l)| run_engine(k, l, &case.setup, case.ncursors, &case.ops))
        .collect();
    let mut skipped = 0;
    let mut passes: Vec<(DbKind, Layout, Vec<MObs>)> = Vec::new();
    let mut failing: Vec<(Cfg, String)> = Vec::new();
    for (&(kind, layout), got) in configs.iter().zip(&results) {
        match check(&case.setup, case.ncursors, &case.ops, got, allow_illegal) {
            Verdict::Pass(trace) => passes.push((kind, layout, trace)),
            Verdict::Skip { .. } => skipped += 1,
            Verdict::Fail(f) => failing.push((Cfg::Single(kind, layout), f.signature(&case.ops))),
        }
    }
    for layout in [Layout::AllInMemory, Layout::Checkpointed] {
        let find = |k: DbKind| {
            let i = configs.iter().position(|c| *c == (k, layout))?;
            let (_, _, trace) = passes.iter().find(|p| (p.0, p.1) == (k, layout))?;
            Some((results[i].as_ref().unwrap().as_slice(), trace.as_slice()))
        };
        if let (Some(w), Some(m)) = (find(DbKind::Wal), find(DbKind::Mvcc)) {
            if let Some(f) = compare_engines(&case.ops, w, m) {
                failing.push((Cfg::Cross(layout), f.signature(&case.ops)));
            }
        }
    }
    for (cfg, sig) in failing {
        let small = shrink(case, cfg, &sig, filter);
        let report = report(&small, case, cfg, allow_illegal, case_seed);
        if seen.insert(report.key.clone()) {
            reports.push(report.text);
        }
    }
    (configs.len(), skipped)
}

// ---------------------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------------------

/// The gap filters and the configs a case runs on. The scope matters because a filter that
/// protects only some configs (D1, G4, N1) is on only when the scope has one of them, so
/// the other configs still see those patterns.
#[derive(Debug, Clone)]
pub struct Filter {
    pub gaps: KnownGaps,
    pub scope: Vec<(DbKind, Layout)>,
}

impl Filter {
    /// True if the generator could have written `case`: no op has a filtered pattern.
    fn clean(&self, case: &Case) -> bool {
        let mut g = Gen::new(&case.setup, case.ncursors, self);
        for op in &case.ops {
            if g.blocked(op) {
                return false;
            }
            if g.replay.clone().apply(op, None).is_none() {
                return true;
            }
            g.note(op);
        }
        true
    }
}

/// Draws a case. The model runs along, so every op is legal.
pub fn generate(rng: &mut ChaCha8Rng, filter: &Filter) -> Case {
    let ncursors = rng.random_range(1..=3);
    let density = rng.random_range(0.1..0.8);
    let mut setup = Vec::new();
    for k in 0..24 {
        if rng.random_bool(density) {
            setup.push((k, rng.random_range(1..1000)));
        }
    }
    let mut g = Gen::new(&setup, ncursors, filter);
    let len = rng.random_range(1..=MAX_OPS);
    let mut ops = Vec::new();
    while ops.len() < len {
        let op = g.next_op(rng);
        // A `Delete` the model rejects (only possible without the D4 filter) ends the
        // program, because the model has no state after it.
        let legal = g.replay.clone().apply(&op, None).is_some();
        ops.push(op);
        if !legal {
            break;
        }
        g.note(&op);
    }
    Case {
        setup,
        ncursors,
        ops,
    }
}

/// The generator state: the model, and a little history for the gap filters.
struct Gen {
    replay: Replay,
    gaps: KnownGaps,
    /// The configs the case runs on.
    scope: Vec<(DbKind, Layout)>,
    /// The last write of the cursor was an `Insert` and it did not move since.
    inserted: Vec<bool>,
    /// The row under the cursor was updated by an `Insert` and the cursor did not move since.
    stale: Vec<bool>,
    /// The last move of the cursor was a `SeekRowid` or `NotExists` that hit.
    exact: Vec<bool>,
    /// Another cursor wrote since the cursor moved the last time.
    peer_wrote: Vec<bool>,
    /// Keys that an `Insert` of this run wrote (added or updated).
    written: HashSet<i64>,
}

impl Gen {
    fn new(setup: &[(i64, i64)], ncursors: usize, filter: &Filter) -> Gen {
        Gen {
            replay: Replay::new(setup, ncursors),
            gaps: filter.gaps,
            scope: filter.scope.clone(),
            inserted: vec![false; ncursors],
            stale: vec![false; ncursors],
            exact: vec![false; ncursors],
            peer_wrote: vec![false; ncursors],
            written: HashSet::new(),
        }
    }

    /// Draws ops until one is allowed. Falls back to `Rewind`, which is always legal and
    /// never filtered.
    fn next_op(&mut self, rng: &mut ChaCha8Rng) -> Op {
        let c = rng.random_range(0..self.replay.st.curs.len());
        for _ in 0..40 {
            let op = self.random_op(rng, c);
            if self.blocked(&op) {
                continue;
            }
            let delete_exempt = matches!(op, Op::Delete(_)) && !self.gaps.has(KnownGaps::D4);
            if delete_exempt || self.replay.clone().apply(&op, None).is_some() {
                return op;
            }
        }
        Op::Rewind(c)
    }

    fn random_op(&self, rng: &mut ChaCha8Rng, c: usize) -> Op {
        // Next, Prev, seeks, inserts and deletes make most of the traffic. Reads come next,
        // `NullRow` and `Count` are rare.
        let kind = rng.random_range(0..100);
        match kind {
            0..=13 => Op::Next(c),
            14..=23 => Op::Prev(c),
            24..=28 => Op::SeekGE(c, self.key(rng)),
            29..=33 => Op::SeekGT(c, self.key(rng)),
            34..=38 => Op::SeekLE(c, self.key(rng)),
            39..=43 => Op::SeekLT(c, self.key(rng)),
            44..=48 => Op::SeekRowid(c, self.key(rng)),
            49..=51 => Op::NotExists(c, self.key(rng)),
            52..=54 => Op::Rewind(c),
            55..=57 => Op::Last(c),
            58..=65 => self.insert(rng, c),
            66..=67 => match self.replay.last_new[c] {
                Some(_) => Op::InsertNew {
                    cur: c,
                    value: rng.random_range(1..1000),
                    require_seek: rng.random_bool(0.3),
                },
                None => Op::NewRowid(c),
            },
            68..=69 => Op::NewRowid(c),
            70..=76 => Op::Delete(c),
            77..=82 => Op::RowId(c),
            83..=88 => Op::Column(c, 1),
            89 => Op::NullRow(c),
            90 => Op::Count(c),
            _ => {
                if rng.random_bool(0.5) {
                    Op::Next(c)
                } else {
                    Op::Prev(c)
                }
            }
        }
    }

    /// An `Insert`. Without `require_seek` it must use the key of the seek token.
    fn insert(&self, rng: &mut ChaCha8Rng, c: usize) -> Op {
        let value = rng.random_range(1..1000);
        match self.replay.st.curs[c].token {
            Tok::Key { key: t, .. } if rng.random_bool(0.5) => Op::Insert {
                cur: c,
                rowid: t,
                value,
                require_seek: false,
            },
            _ => Op::Insert {
                cur: c,
                rowid: self.key(rng),
                value,
                require_seek: true,
            },
        }
    }

    /// A key near the table, so that seeks hit and miss.
    fn key(&self, rng: &mut ChaCha8Rng) -> i64 {
        let rows = &self.replay.st.rows;
        let near = |rng: &mut ChaCha8Rng| rows[rng.random_range(0..rows.len())].0;
        match rng.random_range(0..10) {
            0..=4 if !rows.is_empty() => near(rng),
            5..=7 if !rows.is_empty() => (near(rng) + rng.random_range(-1..=1)).max(0),
            _ => rng.random_range(0..=KEY_MAX),
        }
    }

    /// Updates the model and the filter history after `op` was emitted.
    fn note(&mut self, op: &Op) {
        let before = self.replay.st.clone();
        let inserted_key = match *op {
            Op::Insert { rowid, .. } => Some(rowid),
            Op::InsertNew { cur, .. } => self.replay.last_new[cur],
            _ => None,
        };
        self.replay
            .apply(op, None)
            .expect("the generator checked legality");
        let c = cursor_of(op).expect("every op has a cursor");
        if let Some(k) = inserted_key {
            let existed = lookup(k, &before.rows).is_some();
            self.written.insert(k);
            self.stale[c] = existed;
            if existed {
                for (j, cj) in before.curs.iter().enumerate() {
                    if j != c && cj.pos == Pos::At(k) {
                        self.stale[j] = true;
                    }
                }
            }
        }
        // Reads and `NullRow` do not move the cursor. Everything else does.
        if !matches!(op, Op::RowId(_) | Op::Column(..) | Op::NullRow(_)) {
            let hit = matches!(op, Op::SeekRowid(..) | Op::NotExists(..))
                && matches!(self.replay.st.curs[c].pos, Pos::At(_));
            self.inserted[c] = inserted_key.is_some();
            self.exact[c] = hit;
            if inserted_key.is_none() {
                self.stale[c] = false;
            }
            self.peer_wrote[c] = false;
        }
        if matches!(op, Op::Insert { .. } | Op::InsertNew { .. } | Op::Delete(_)) {
            for (j, w) in self.peer_wrote.iter_mut().enumerate() {
                *w |= j != c;
            }
        }
    }

    // -----------------------------------------------------------------------------------
    // Known gap filters. Each returns true when `op` has the pattern of the gap.
    // -----------------------------------------------------------------------------------

    fn blocked(&self, op: &Op) -> bool {
        let mvcc = |c: &(DbKind, Layout)| c.0 == DbKind::Mvcc;
        let mvcc_ckpt = |c: &(DbKind, Layout)| *c == (DbKind::Mvcc, Layout::Checkpointed);
        let wal = |c: &(DbKind, Layout)| c.0 != DbKind::Mvcc;
        (self.gaps.has(KnownGaps::D1) && self.scope.iter().any(mvcc_ckpt) && self.d1(op))
            || (self.gaps.has(KnownGaps::G4) && self.scope.iter().any(mvcc_ckpt) && self.g4(op))
            || (self.gaps.has(KnownGaps::N1) && self.scope.iter().any(mvcc) && self.n1(op))
            || (self.gaps.has(KnownGaps::N3) && self.n3(op))
            || (self.gaps.has(KnownGaps::N4) && self.scope.iter().any(mvcc_ckpt) && self.n4(op))
            || (self.gaps.has(KnownGaps::G3) && self.scope.iter().any(wal) && self.g3(op))
            || (self.gaps.has(KnownGaps::PD1) && self.scope.iter().any(wal) && self.pd1(op))
    }

    /// PD1: `Next` or `Prev` from a gap after a peer write.
    fn pd1(&self, op: &Op) -> bool {
        matches!(*op, Op::Next(c) | Op::Prev(c)
            if self.peer_wrote[c] && matches!(self.replay.st.curs[c].pos, Pos::Gone(_)))
    }

    /// G3: a read of a cursor with the null-row flag after a peer write.
    fn g3(&self, op: &Op) -> bool {
        matches!(*op, Op::RowId(c) | Op::Column(c, _)
            if self.replay.st.curs[c].null_row && self.peer_wrote[c])
    }

    /// N3: `NewRowid` or `Count` on a cursor with the null-row flag.
    fn n3(&self, op: &Op) -> bool {
        matches!(*op, Op::NewRowid(c) | Op::Count(c) if self.replay.st.curs[c].null_row)
    }

    /// N4: `Delete` of a row that an `Insert` of this run wrote.
    fn n4(&self, op: &Op) -> bool {
        let Op::Delete(c) = *op else { return false };
        matches!(self.replay.st.curs[c].pos, Pos::At(k) if self.written.contains(&k))
    }

    /// N1: `Next` after a `SeekRowid` or `NotExists` that hit.
    fn n1(&self, op: &Op) -> bool {
        matches!(*op, Op::Next(c) if self.exact[c])
    }

    /// D1: `Next` or `Prev` right after an `Insert` of the same cursor.
    fn d1(&self, op: &Op) -> bool {
        match *op {
            Op::Next(c) | Op::Prev(c) => self.inserted[c],
            _ => false,
        }
    }

    /// G4: `Column` on a row that an `Insert` updated, without a move in between.
    fn g4(&self, op: &Op) -> bool {
        matches!(*op, Op::Column(c, _) if self.stale[c])
    }
}

fn cursor_of(op: &Op) -> Option<usize> {
    Some(match *op {
        Op::Rewind(c)
        | Op::Last(c)
        | Op::Next(c)
        | Op::Prev(c)
        | Op::SeekRowid(c, _)
        | Op::SeekGE(c, _)
        | Op::SeekGT(c, _)
        | Op::SeekLE(c, _)
        | Op::SeekLT(c, _)
        | Op::NotExists(c, _)
        | Op::NewRowid(c)
        | Op::Delete(c)
        | Op::Column(c, _)
        | Op::RowId(c)
        | Op::NullRow(c)
        | Op::Count(c) => c,
        Op::Insert { cur, .. } | Op::InsertNew { cur, .. } => cur,
    })
}

fn map_cursor(op: &Op, f: impl Fn(usize) -> usize) -> Op {
    match *op {
        Op::Rewind(c) => Op::Rewind(f(c)),
        Op::Last(c) => Op::Last(f(c)),
        Op::Next(c) => Op::Next(f(c)),
        Op::Prev(c) => Op::Prev(f(c)),
        Op::SeekRowid(c, k) => Op::SeekRowid(f(c), k),
        Op::SeekGE(c, k) => Op::SeekGE(f(c), k),
        Op::SeekGT(c, k) => Op::SeekGT(f(c), k),
        Op::SeekLE(c, k) => Op::SeekLE(f(c), k),
        Op::SeekLT(c, k) => Op::SeekLT(f(c), k),
        Op::NotExists(c, k) => Op::NotExists(f(c), k),
        Op::NewRowid(c) => Op::NewRowid(f(c)),
        Op::Insert {
            cur,
            rowid,
            value,
            require_seek,
        } => Op::Insert {
            cur: f(cur),
            rowid,
            value,
            require_seek,
        },
        Op::InsertNew {
            cur,
            value,
            require_seek,
        } => Op::InsertNew {
            cur: f(cur),
            value,
            require_seek,
        },
        Op::Delete(c) => Op::Delete(f(c)),
        Op::Column(c, col) => Op::Column(f(c), col),
        Op::RowId(c) => Op::RowId(f(c)),
        Op::NullRow(c) => Op::NullRow(f(c)),
        Op::Count(c) => Op::Count(f(c)),
    }
}

// ---------------------------------------------------------------------------------------
// Shrinking and reporting
// ---------------------------------------------------------------------------------------

/// What a failure is about: one engine config, or the direct WAL-vs-MVCC comparison.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Cfg {
    Single(DbKind, Layout),
    Cross(Layout),
}

/// Runs the case on `cfg` and returns the signature of the failure, if any.
fn evaluate(cfg: Cfg, case: &Case, allow_illegal: bool) -> Option<String> {
    let go = |k, l| run_engine(k, l, &case.setup, case.ncursors, &case.ops);
    let verdict = |got: &Result<Vec<Obs>, String>| {
        check(&case.setup, case.ncursors, &case.ops, got, allow_illegal)
    };
    match cfg {
        Cfg::Single(k, l) => match verdict(&go(k, l)) {
            Verdict::Fail(f) => Some(f.signature(&case.ops)),
            _ => None,
        },
        Cfg::Cross(l) => {
            let (w, m) = (go(DbKind::Wal, l), go(DbKind::Mvcc, l));
            let (Verdict::Pass(wt), Verdict::Pass(mt)) = (verdict(&w), verdict(&m)) else {
                return None;
            };
            compare_engines(
                &case.ops,
                (w.as_ref().unwrap(), &wt),
                (m.as_ref().unwrap(), &mt),
            )
            .map(|f: Failure| f.signature(&case.ops))
        }
    }
}

/// Removes ops, setup rows and unused cursors while the failure keeps its signature.
fn shrink(case: &Case, cfg: Cfg, sig: &str, filter: &Filter) -> Case {
    let allow_illegal = !filter.gaps.has(KnownGaps::D4);
    let still_fails = |c: &Case| {
        // A candidate must stay a program that the generator could have written. Without
        // this the shrinker walks from the bug it started with to a known gap.
        !c.ops.is_empty()
            && filter.clean(c)
            && evaluate(cfg, c, allow_illegal).as_deref() == Some(sig)
    };
    let mut cur = case.clone();
    loop {
        let before = (cur.ops.len(), cur.setup.len(), cur.ncursors);
        // Remove chunks of ops, from half of the program down to single ops.
        let mut chunk = (cur.ops.len() / 2).max(1);
        loop {
            let mut i = 0;
            while i < cur.ops.len() {
                let mut cand = cur.clone();
                cand.ops.drain(i..(i + chunk).min(cur.ops.len()));
                if still_fails(&cand) {
                    cur = cand;
                } else {
                    i += chunk;
                }
            }
            if chunk == 1 {
                break;
            }
            chunk = (chunk / 2).max(1);
        }
        let mut i = 0;
        while i < cur.setup.len() {
            let mut cand = cur.clone();
            cand.setup.remove(i);
            if still_fails(&cand) {
                cur = cand;
            } else {
                i += 1;
            }
        }
        // Drop a cursor that no op uses, and renumber the cursors above it.
        for j in (0..cur.ncursors).rev() {
            if cur.ncursors > 1 && cur.ops.iter().all(|op| cursor_of(op) != Some(j)) {
                let mut cand = cur.clone();
                cand.ncursors -= 1;
                cand.ops = cur
                    .ops
                    .iter()
                    .map(|op| map_cursor(op, |c| if c > j { c - 1 } else { c }))
                    .collect();
                if still_fails(&cand) {
                    cur = cand;
                }
            }
        }
        if before == (cur.ops.len(), cur.setup.len(), cur.ncursors) {
            return cur;
        }
    }
}

struct Report {
    /// Same key, same bug. Used to print each minimal failure once.
    key: String,
    text: String,
}

fn report(case: &Case, original: &Case, cfg: Cfg, allow_illegal: bool, case_seed: u64) -> Report {
    let configs: Vec<(DbKind, Layout)> = match cfg {
        Cfg::Single(k, l) => vec![(k, l)],
        Cfg::Cross(l) => vec![(DbKind::Wal, l), (DbKind::Mvcc, l)],
    };
    let mut text = String::new();
    let sig = evaluate(cfg, case, allow_illegal).unwrap_or_else(|| "flaky".to_string());
    text += &format!("=== cursor contract failure: {sig}\n");
    text += &format!("seed {case_seed:#x} (rerun: SEED={case_seed:#x} ITERS=1)\n");
    text += &format!(
        "config {cfg:?}, setup rows {:?}, {} cursors\n",
        case.setup, case.ncursors
    );
    for &(k, l) in &configs {
        let got = run_engine(k, l, &case.setup, case.ncursors, &case.ops);
        text += &format!("{k:?}/{l:?}, step: op | engine | model\n");
        let mut rep = Replay::new(&case.setup, case.ncursors);
        let engine = got.as_ref().ok();
        if let Err(e) = &got {
            text += &format!("  engine error: {e}\n");
        }
        for (i, op) in case.ops.iter().enumerate() {
            let observed = engine
                .and_then(|o| o[i].rowid)
                .filter(|_| matches!(op, Op::NewRowid(_)));
            let model = rep.apply(op, observed);
            let e = engine.map_or("-".to_string(), |o| format!("{:?}", o[i]));
            let m = model.map_or("illegal".to_string(), |m| format!("{m:?}"));
            text += &format!("  {i:2}: {op:?} | {e} | {m}\n");
        }
    }
    text += &format!(
        "unshrunk: {} cursors, rows {:?}, ops {:?}\n",
        original.ncursors, original.setup, original.ops
    );
    text += "repro:\n";
    text += "    #[test]\n    fn repro() {\n        use Op::*;\n";
    text += &format!("        let rows = {:?};\n", case.setup);
    text += &format!("        let ops = {:?};\n", case.ops);
    for &(k, l) in &configs {
        text += &format!(
            "        assert_agrees(DbKind::{k:?}, Layout::{l:?}, &rows, {}, &ops);\n",
            case.ncursors
        );
    }
    text += "    }\n";
    Report {
        // Digits are masked, so that the same program with other keys is the same failure.
        key: format!("{cfg:?} {sig} {:?}", case.ops)
            .chars()
            .map(|c| if c.is_ascii_digit() { '#' } else { c })
            .collect(),
        text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cases per run of the quick test. `ITERS` overrides it.
    const QUICK_ITERS: usize = 150;

    fn run_walk(default_iters: usize) {
        let cfg = WalkConfig::from_env(default_iters);
        let reports = walk(&cfg);
        assert!(
            reports.is_empty(),
            "{} distinct failures:\n{}",
            reports.len(),
            reports.join("\n")
        );
    }

    #[test]
    fn random_walk_all_configs() {
        run_walk(QUICK_ITERS);
    }

    /// The long run: `SEED` and `ITERS` as above, 200 cases by default.
    #[test]
    #[ignore = "long run, use ITERS and SEED"]
    fn random_walk_long() {
        run_walk(200);
    }

    #[test]
    fn generator_only_emits_legal_programs() {
        for seed in 0..300 {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let case = generate(
                &mut rng,
                &Filter {
                    gaps: KnownGaps::ALL,
                    scope: CONFIGS.to_vec(),
                },
            );
            let mut rep = Replay::new(&case.setup, case.ncursors);
            for op in &case.ops {
                assert!(rep.apply(op, None).is_some(), "seed {seed}: {op:?}");
            }
        }
    }
}
