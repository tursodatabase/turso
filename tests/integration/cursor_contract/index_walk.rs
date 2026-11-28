//! Seeded random walks over the index cursor contract (SPEC 2b), the index twin of `walk.rs`.
//!
//! The generator runs the model (`index_model.rs`) next to the program it writes, so it only
//! emits legal ops. Every sequence runs on WAL and MVCC, with the setup entries in memory and
//! checkpointed (and on an ephemeral index, when the sequence has one cursor). The engine
//! result is compared with the model and with the other engines. A failing sequence is shrunk
//! and printed as a Rust test body.
//!
//! The index is `i_ab ON ti(a, b)`. `a` has few values (0..5) and NULLs, so there are many
//! duplicates in `a`, and the prefix of a seek has one or two columns. A case draws the sort
//! order and the collation of the two columns (`IdxDef`, SPEC 2c), and a value pool: integers
//! only, or integers and a small pool of texts that differ by case and by trailing spaces.
//!
//! Environment:
//! - `SEED`: seed of the first case (decimal or `0x` hex). Case `i` uses `SEED + i`, so the
//!   seed in a failure report reproduces that case with `ITERS=1`.
//! - `ITERS`: number of cases.
//! - `NO_FILTER=1`: switch off the known gap filters, to see the known gaps again.
//! - `IDEF`: the index definition family of all cases: `default` (ASC, BINARY), `desc`,
//!   `nocase`, `rtrim`, `mixed` (any non-default definition). Unset: most cases use the
//!   default definition and the others a random one.

use super::index_model::{
    above_all, cmp_cols, cmp_key, compare_engines, icheck, imax, is_legal, project, ColDef, Coll,
    IFailure, IKey, IMObs, IPos, IReplay, ITok, IVerdict, IdxDef, Order, Val,
};
use super::index_program::{run_index, IOp, IndexRun, Outcome, Prefix, Row};
use super::model::Spec;
use super::program::ReadMode;
use super::{DbKind, Layout};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::cmp::Ordering;
use std::collections::HashSet;

/// Values of `a` are in `0..A_RANGE`, values of `b` in `0..B_RANGE`.
const A_RANGE: i64 = 5;
const B_RANGE: i64 = 8;
const MAX_OPS: usize = 30;
/// Rowids of new entries.
const ROWID_MAX: i64 = 40;

const EPHEMERAL: (DbKind, Layout) = (DbKind::Ephemeral, Layout::AllInMemory);
/// The `:spec-model:` engine runs in every scope: it is fast, and it must agree with the test
/// model on every legal program.
const SPEC_MODEL: (DbKind, Layout) = (DbKind::SpecModel, Layout::AllInMemory);
const CONFIGS: [(DbKind, Layout); 4] = [
    (DbKind::Wal, Layout::AllInMemory),
    (DbKind::Wal, Layout::Checkpointed),
    (DbKind::Mvcc, Layout::AllInMemory),
    (DbKind::Mvcc, Layout::Checkpointed),
];

// ---------------------------------------------------------------------------------------
// Known gaps
// ---------------------------------------------------------------------------------------

/// A set of generator filters. Each bit avoids the exact pattern of one known engine gap (see
/// the `known_bug_*` tests in `index_tests.rs` and at the end of this file), so that a walk does
/// not find the same bug again and again. The names are the ids of the gaps.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IdxGaps(u32);

impl IdxGaps {
    /// MVCC with checkpointed entries: `Next` or `Prev` right after an `IdxInsert` of the same
    /// cursor loses the position (D1, the index twin of the table filter `D1`).
    pub const IDX_D1_MVCC_CKPT: IdxGaps = IdxGaps(1);
    /// WAL and ephemeral: after an `IdxInsert` of a key that is above every key, the cursor is
    /// not on the new entry. Every op that uses the position is blocked until the cursor moves.
    pub const IDX_END_INSERT: IdxGaps = IdxGaps(1 << 1);
    /// `RowData` mode, WAL and ephemeral: a read right after an `IdxInsert` returns the entry
    /// after the new one.
    pub const ROWDATA_IDX_INSERT: IdxGaps = IdxGaps(1 << 2);
    /// `RowData` mode: a read of a cursor with the null-row flag returns the real entry.
    pub const ROWDATA_NULL_ROW: IdxGaps = IdxGaps(1 << 3);

    /// IDX_N4_MVCC_CKPT_DELETE_OF_WRITTEN: MVCC with checkpointed entries: `Delete` of an entry
    /// that an `IdxInsert` of this run wrote loses the position (`Next` and `Prev` after it
    /// give the wrong answer). The index twin of the table gap N4.
    pub const IDX_N4_MVCC_CKPT_DELETE_OF_WRITTEN: IdxGaps = IdxGaps(1 << 12);

    /// CL1: MVCC: `IdxInsert` of a key that equals, under the collations, a key that the index
    /// held earlier in the run, but that is not identical to it (`('A', 1)` after
    /// `('a', 1)`). The overlay keeps the bytes of the first key, so a read gives the old text.
    pub const CL1_MVCC_EQUAL_KEY_REINSERT: IdxGaps = IdxGaps(1 << 14);

    /// FM1: every engine: `Next` after a `Found`, `NotFound` or `NoConflict` that missed does
    /// not go to the first entry after the key. WAL and ephemeral skip an entry or end the
    /// scan, MVCC ends the scan.
    pub const FM1_NEXT_AFTER_FOUND_MISS: IdxGaps = IdxGaps(1 << 15);
    /// PW2: WAL and ephemeral: after an `IdxInsert` of the cursor and a write by another
    /// cursor, the cursor is not on the new entry: a read gives NULL or another entry, `Prev`
    /// returns the new entry again.
    pub const PW2_READ_AFTER_INSERT_AND_PEER_WRITE: IdxGaps = IdxGaps(1 << 16);
    /// G3: WAL and ephemeral: a cursor with the null-row flag and a write by another cursor
    /// since its last move reads the real entry instead of NULL.
    pub const G3_NULL_ROW_AND_PEER_WRITE: IdxGaps = IdxGaps(1 << 17);
    /// OV1: MVCC: `Next` after a `Found`, `NotFound` or `NoConflict` that hit skips entries or
    /// ends the scan (the equality seek gap OV1, reached through `Found`).
    pub const OV1_NEXT_AFTER_FOUND_HIT: IdxGaps = IdxGaps(1 << 18);
    /// PD2: WAL: `Delete` of the entry under the cursor after another cursor wrote to the
    /// index panics (`idx < cell_count`).
    pub const PD2_DELETE_AFTER_PEER_WRITE: IdxGaps = IdxGaps(1 << 19);
    /// MI1: the `:spec-model:` engine: `NoConflict` with a NULL in the key makes no call on the
    /// cursor, so the engine does not see that it clears the null-row flag, and refuses a
    /// write or `Count` after it (U3). The translator does not emit `NullRow` before
    /// `NoConflict` on a cursor.
    pub const MI1_SPEC_MODEL_NO_CONFLICT_NULL_AFTER_NULL_ROW: IdxGaps = IdxGaps(1 << 20);

    pub const NONE: IdxGaps = IdxGaps(0);
    pub const ALL: IdxGaps = IdxGaps(0b1_1111_1101_0000_0000_1111);

    pub fn has(self, other: IdxGaps) -> bool {
        self.0 & other.0 == other.0
    }

    /// `NO_FILTER=1` switches all filters off. `NO_FILTER=CL1,ROWDATA_NULL_ROW` switches off the named
    /// filters only (the names of `NAMES`), to see one gap again.
    pub fn from_env() -> IdxGaps {
        match std::env::var("NO_FILTER") {
            Ok(v) if v == "1" => IdxGaps::NONE,
            Ok(v) if !v.is_empty() && v != "0" => {
                let mut gaps = IdxGaps::ALL;
                for name in v.split(',') {
                    let (_, bit) = IdxGaps::NAMES
                        .iter()
                        .find(|(n, _)| n == &name)
                        .unwrap_or_else(|| panic!("unknown filter {name}"));
                    gaps = IdxGaps(gaps.0 & !bit.0);
                }
                gaps
            }
            _ => IdxGaps::ALL,
        }
    }

    pub const NAMES: [(&'static str, IdxGaps); 12] = [
        ("IDX_D1_MVCC_CKPT", IdxGaps::IDX_D1_MVCC_CKPT),
        ("IDX_END_INSERT", IdxGaps::IDX_END_INSERT),
        ("ROWDATA_IDX_INSERT", IdxGaps::ROWDATA_IDX_INSERT),
        ("ROWDATA_NULL_ROW", IdxGaps::ROWDATA_NULL_ROW),
        (
            "IDX_N4_MVCC_CKPT_DELETE_OF_WRITTEN",
            IdxGaps::IDX_N4_MVCC_CKPT_DELETE_OF_WRITTEN,
        ),
        ("CL1", IdxGaps::CL1_MVCC_EQUAL_KEY_REINSERT),
        ("FM1", IdxGaps::FM1_NEXT_AFTER_FOUND_MISS),
        ("PW2", IdxGaps::PW2_READ_AFTER_INSERT_AND_PEER_WRITE),
        ("G3", IdxGaps::G3_NULL_ROW_AND_PEER_WRITE),
        ("OV1", IdxGaps::OV1_NEXT_AFTER_FOUND_HIT),
        ("PD2", IdxGaps::PD2_DELETE_AFTER_PEER_WRITE),
        (
            "MI1",
            IdxGaps::MI1_SPEC_MODEL_NO_CONFLICT_NULL_AFTER_NULL_ROW,
        ),
    ];
}

// ---------------------------------------------------------------------------------------
// Case
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Case {
    pub idef: IdxDef,
    pub rows: Vec<Row>,
    pub nidx: usize,
    pub mode: ReadMode,
    pub ops: Vec<IOp>,
}

/// The gap filters and the configs a case runs on. The scope matters because a filter that
/// protects only some configs is on only when the scope has one of them, so the other configs
/// still see those patterns.
#[derive(Debug, Clone)]
pub struct Filter {
    pub gaps: IdxGaps,
    pub scope: Vec<(DbKind, Layout)>,
}

impl Filter {
    fn ephemeral(&self) -> bool {
        self.scope.contains(&EPHEMERAL)
    }

    /// True if the generator could have written `case`: every op is legal and none has a
    /// filtered pattern.
    fn clean(&self, case: &Case) -> bool {
        let mut g = Gen::new(&case.rows, case.nidx, case.mode, case.idef, self);
        for op in &case.ops {
            if g.blocked(op) || g.replay.clone().apply(op).is_none() {
                return false;
            }
            g.note(op);
        }
        true
    }
}

// ---------------------------------------------------------------------------------------
// Walk
// ---------------------------------------------------------------------------------------

pub struct WalkConfig {
    pub seed: u64,
    pub iters: usize,
    pub gaps: IdxGaps,
    pub family: Family,
}

/// The index definitions that a walk draws (`IDEF`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Family {
    /// Mostly the default definition, else a random one.
    Any,
    Default,
    Desc,
    NoCase,
    RTrim,
    /// A random definition that is not the default one.
    Mixed,
}

impl Family {
    fn from_env() -> Family {
        match std::env::var("IDEF").as_deref() {
            Err(_) | Ok("") => Family::Any,
            Ok("default") => Family::Default,
            Ok("desc") => Family::Desc,
            Ok("nocase") => Family::NoCase,
            Ok("rtrim") => Family::RTrim,
            Ok("mixed") => Family::Mixed,
            Ok(other) => panic!("unknown IDEF {other}"),
        }
    }

    fn draw(self, rng: &mut ChaCha8Rng) -> IdxDef {
        let both = |order, coll| IdxDef::new(ColDef::new(order, coll), ColDef::new(order, coll));
        match self {
            Family::Default => IdxDef::default(),
            Family::Desc => both(Order::Desc, Coll::Binary),
            Family::NoCase => both(Order::Asc, Coll::NoCase),
            Family::RTrim => both(Order::Asc, Coll::RTrim),
            Family::Any if rng.random_bool(0.6) => IdxDef::default(),
            Family::Any | Family::Mixed => loop {
                let mut col = || {
                    let order = if rng.random_bool(0.5) {
                        Order::Desc
                    } else {
                        Order::Asc
                    };
                    let coll = match rng.random_range(0..10) {
                        0..=3 => Coll::Binary,
                        4..=6 => Coll::NoCase,
                        _ => Coll::RTrim,
                    };
                    ColDef::new(order, coll)
                };
                let def = IdxDef::new(col(), col());
                if !def.is_default() {
                    return def;
                }
            },
        }
    }
}

impl WalkConfig {
    pub fn from_env(default_iters: usize) -> WalkConfig {
        let seed = std::env::var("SEED").ok().map_or(0x1DE5_C0DE, |s| {
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
            gaps: IdxGaps::from_env(),
            family: Family::from_env(),
        }
    }
}

/// Runs the walk and returns one report per distinct failure (after shrinking).
pub fn walk(cfg: &WalkConfig) -> Vec<String> {
    let mut reports = Vec::new();
    let mut seen = HashSet::new();
    let (mut ran, mut ops_total) = (0, 0);
    for i in 0..cfg.iters {
        let case_seed = cfg.seed.wrapping_add(i as u64);
        let mut rng = ChaCha8Rng::seed_from_u64(case_seed);
        let (case, filter) = generate(&mut rng, cfg.gaps, cfg.family);
        ran += filter.scope.len();
        ops_total += case.ops.len();
        run_case(&case, &filter, case_seed, &mut seen, &mut reports);
    }
    eprintln!(
        "index walk: seed {:#x}, {} cases, {ops_total} ops, {ran} engine runs, {} distinct failures",
        cfg.seed,
        cfg.iters,
        reports.len()
    );
    reports
}

fn random_scope(rng: &mut ChaCha8Rng, nidx: usize) -> Vec<(DbKind, Layout)> {
    let mut scope = match rng.random_range(0..10) {
        0..=3 => CONFIGS.to_vec(),
        4 => CONFIGS[..2].to_vec(),
        5 => CONFIGS[2..].to_vec(),
        _ => vec![CONFIGS[rng.random_range(0..4)]],
    };
    // `OpenDup` does not support index cursors, so an ephemeral index has one cursor.
    if nidx == 1 {
        match rng.random_range(0..10) {
            0..=3 => scope.push(EPHEMERAL),
            4 => scope = vec![EPHEMERAL],
            _ => {}
        }
    }
    scope.push(SPEC_MODEL);
    scope
}

fn run_one(kind: DbKind, layout: Layout, case: &Case) -> Outcome {
    run_index(
        &IndexRun {
            kind,
            layout,
            rows: &case.rows,
            idef: case.idef,
            nidx: case.nidx,
            ntbl: 0,
            mode: case.mode,
        },
        &case.ops,
    )
}

/// Runs one case on the configs of its scope and reports every failure after shrinking.
fn run_case(
    case: &Case,
    filter: &Filter,
    case_seed: u64,
    seen: &mut HashSet<String>,
    reports: &mut Vec<String>,
) {
    let configs = &filter.scope;
    let outs: Vec<Outcome> = configs.iter().map(|&(k, l)| run_one(k, l, case)).collect();
    let mut passes: Vec<(usize, Vec<IMObs>)> = Vec::new();
    let mut failing: Vec<(Cfg, String)> = Vec::new();
    for (i, (&(kind, layout), got)) in configs.iter().zip(&outs).enumerate() {
        let eph = kind == DbKind::Ephemeral;
        match icheck(&case.rows, case.nidx, eph, case.idef, &case.ops, got) {
            IVerdict::Pass(trace) => passes.push((i, trace)),
            IVerdict::Skip { .. } => {}
            IVerdict::Fail(f) => failing.push((Cfg::Single(kind, layout), f.signature(&case.ops))),
        }
    }
    // Every pair of engines that agree with the model is compared directly.
    for (x, (i, ti)) in passes.iter().enumerate() {
        for (j, tj) in &passes[x + 1..] {
            if let Some(f) = compare_engines(&case.ops, (&outs[*i].obs, ti), (&outs[*j].obs, tj)) {
                failing.push((Cfg::Cross(configs[*i], configs[*j]), f.signature(&case.ops)));
            }
        }
    }
    for (cfg, sig) in failing {
        let small = shrink(case, cfg, &sig, filter);
        let report = report(&small, case, cfg, case_seed);
        if seen.insert(report.key.clone()) {
            reports.push(report.text);
        }
    }
}

// ---------------------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------------------

/// Draws a case. The model runs along, so every op is legal.
pub fn generate(rng: &mut ChaCha8Rng, gaps: IdxGaps, family: Family) -> (Case, Filter) {
    let idef = family.draw(rng);
    // The default definition keeps the integer pool most of the time, the others have text.
    let pool = if idef.is_default() && rng.random_bool(0.7) {
        Pool::Ints
    } else {
        Pool::Mixed
    };
    let nidx = rng.random_range(1..=3);
    let filter = Filter {
        gaps,
        scope: random_scope(rng, nidx),
    };
    let mode = match rng.random_range(0..10) {
        0..=5 => ReadMode::Column,
        6 => ReadMode::ColumnRange,
        7 => ReadMode::ColumnHasField,
        _ => ReadMode::RowData,
    };
    let count = if rng.random_bool(0.05) {
        0
    } else {
        rng.random_range(2..=14)
    };
    let rows: Vec<Row> = (1..=count)
        .map(|id| (id, rand_a(rng, pool), rand_b(rng, pool)))
        .collect();
    let mut g = Gen::new(&rows, nidx, mode, idef, &filter);
    g.pool = pool;
    let len = rng.random_range(1..=MAX_OPS);
    let mut ops: Vec<IOp> = Vec::new();
    while ops.len() < len {
        let mut batch = g.next_batch(rng);
        batch.truncate(len - ops.len());
        for op in batch {
            g.note(&op);
            ops.push(op);
        }
    }
    let case = Case {
        idef,
        rows,
        nidx,
        mode,
        ops,
    };
    (case, filter)
}

/// The generator state: the model, and a little history for the gap filters.
#[derive(Clone)]
struct Gen {
    replay: IReplay,
    /// The values of new columns.
    pool: Pool,
    gaps: IdxGaps,
    /// The configs the case runs on.
    scope: Vec<(DbKind, Layout)>,
    mode: ReadMode,
    /// The last write of the cursor was an `IdxInsert` and it did not move since.
    after_insert: Vec<bool>,
    /// The last write of the cursor was an `IdxInsert` of a key above every key, and the
    /// cursor did not move since.
    appended: Vec<bool>,
    /// The last move of the cursor was a `Found`, `NotFound` or `NoConflict` that missed.
    found_miss: Vec<bool>,
    /// The last move of the cursor was a `Found`, `NotFound` or `NoConflict` that hit.
    found_hit: Vec<bool>,
    /// Another cursor wrote to the index since the cursor moved the last time.
    peer_wrote: Vec<bool>,
    /// Keys that an `IdxInsert` of this run wrote.
    written: Vec<IKey>,
    /// Every key that the index held at some time of the run: the setup rows and `written`.
    ever: Vec<IKey>,
}

impl Gen {
    fn new(rows: &[Row], nidx: usize, mode: ReadMode, idef: IdxDef, filter: &Filter) -> Gen {
        let replay = IReplay::new(rows, nidx, filter.ephemeral(), idef);
        Gen {
            ever: replay.st.irows.clone(),
            replay,
            pool: Pool::Ints,
            gaps: filter.gaps,
            scope: filter.scope.clone(),
            mode,
            after_insert: vec![false; nidx],
            appended: vec![false; nidx],
            found_miss: vec![false; nidx],
            found_hit: vec![false; nidx],
            peer_wrote: vec![false; nidx],
            written: Vec::new(),
        }
    }

    /// Draws ops until one is allowed. Falls back to `Rewind`, which is always legal and never
    /// filtered.
    fn next_batch(&self, rng: &mut ChaCha8Rng) -> Vec<IOp> {
        let c = rng.random_range(0..self.replay.st.icurs.len());
        for _ in 0..40 {
            let batch = self.random_batch(rng, c);
            let n = self.allowed_prefix(&batch);
            if n > 0 {
                return batch[..n].to_vec();
            }
        }
        vec![IOp::Rewind(c)]
    }

    /// How many ops of `batch`, from the start, are legal and not filtered.
    fn allowed_prefix(&self, batch: &[IOp]) -> usize {
        let mut g = self.clone();
        for (i, op) in batch.iter().enumerate() {
            if g.blocked(op) || g.replay.clone().apply(op).is_none() {
                return i;
            }
            g.note(op);
        }
        batch.len()
    }

    fn random_batch(&self, rng: &mut ChaCha8Rng, c: usize) -> Vec<IOp> {
        // Seeks, `Next` and `Prev` and the compare ops make most of the traffic. Writes come
        // next, `NullRow`, `Count` and `SeekEnd` are rare.
        let kind = rng.random_range(0..100);
        let one = |op| vec![op];
        match kind {
            0..=11 => one(IOp::Next(c)),
            12..=19 => one(IOp::Prev(c)),
            20..=25 => one(IOp::SeekGE(c, self.prefix(rng, None))),
            26..=31 => one(IOp::SeekGT(c, self.prefix(rng, None))),
            32..=36 => one(IOp::SeekLE(c, self.prefix(rng, None))),
            37..=40 => one(IOp::SeekLT(c, self.prefix(rng, None))),
            41..=43 => one(IOp::Rewind(c)),
            44..=45 => one(IOp::Last(c)),
            46..=48 => one(IOp::IdxGE(c, self.near_entry(rng, c))),
            49..=53 => one(IOp::IdxGT(c, self.near_entry(rng, c))),
            54..=55 => one(IOp::IdxLE(c, self.near_entry(rng, c))),
            56..=58 => one(IOp::IdxLT(c, self.near_entry(rng, c))),
            59..=61 => one(IOp::Found(c, self.prefix(rng, Some(2)))),
            62..=64 => one(IOp::NotFound(c, self.prefix(rng, Some(2)))),
            65..=68 => one(IOp::NoConflict(c, self.prefix(rng, Some(2)))),
            69..=75 => one(self.idx_insert(rng, c)),
            76..=79 => one(self.idx_delete(rng, c)),
            80..=83 => one(IOp::Delete(c)),
            84..=88 => one(IOp::Observe(c)),
            89..=90 => one(IOp::Column(c, rng.random_range(0..2))),
            91 => one(IOp::IdxRowId(c)),
            92 => one(IOp::NullRow(c)),
            93 => one(IOp::Count(c)),
            94 => one(IOp::SeekEnd(c)),
            95..=96 => self.replace_with_variant(rng, c),
            _ => self.range_loop(rng, c),
        }
    }

    /// The loop of a range scan: seek, then repeat "compare with the end of the range, read,
    /// move". The batch is cut at the first op that a filter blocks.
    fn range_loop(&self, rng: &mut ChaCha8Rng, c: usize) -> Vec<IOp> {
        let p = self.prefix(rng, None);
        let rounds = rng.random_range(1..=4);
        let mut ops = Vec::new();
        if rng.random_bool(0.7) {
            ops.push(IOp::SeekGE(c, p.clone()));
            for _ in 0..rounds {
                ops.push(IOp::IdxGT(c, p.clone()));
                ops.push(IOp::Observe(c));
                ops.push(IOp::Next(c));
            }
        } else {
            ops.push(IOp::SeekLE(c, p.clone()));
            for _ in 0..rounds {
                ops.push(IOp::IdxLT(c, p.clone()));
                ops.push(IOp::Observe(c));
                ops.push(IOp::Prev(c));
            }
        }
        ops
    }

    /// Deletes a stored key and inserts a key with the same rowid and columns that differ by
    /// case or by trailing spaces, then seeks to it and reads. Under NOCASE or RTRIM the new
    /// key is equal to the old one, so the entry must read with the new bytes (SPEC 2c).
    fn replace_with_variant(&self, rng: &mut ChaCha8Rng, c: usize) -> Vec<IOp> {
        let s = &self.replay.st.irows;
        if s.is_empty() {
            return Vec::new();
        }
        let (cols, rowid) = s[rng.random_range(0..s.len())].clone();
        let (a, b) = (variant(rng, &cols[0]), variant(rng, &cols[1]));
        vec![
            IOp::IdxDelete {
                cur: c,
                a: cols[0].clone(),
                b: cols[1].clone(),
                rowid,
            },
            IOp::IdxInsert {
                cur: c,
                a: a.clone(),
                b,
                rowid,
                use_seek: false,
            },
            IOp::SeekGE(c, vec![a]),
            IOp::Observe(c),
        ]
    }

    /// An `IdxInsert`. With `use_seek` the key must go with the seek token of the cursor.
    fn idx_insert(&self, rng: &mut ChaCha8Rng, c: usize) -> IOp {
        let cur = &self.replay.st.icurs[c];
        let make = |a, b, rowid, use_seek| IOp::IdxInsert {
            cur: c,
            a,
            b,
            rowid,
            use_seek,
        };
        if rng.random_bool(0.5) {
            match &cur.token {
                ITok::Key { key: p, hit: false } if p.len() == 2 => {
                    let rowid = self.new_rowid(rng, p);
                    return make(p[0].clone(), p[1].clone(), rowid, true);
                }
                ITok::Append => {
                    let (a, b, rowid) = self.above_all_key(rng);
                    return make(a, b, rowid, true);
                }
                _ => {
                    // A NULL in the key: Turso seeks, so no token is needed.
                    if rng.random_bool(0.4) {
                        let (a, b) = if rng.random_bool(0.7) {
                            (Val::Null, rand_b(rng, self.pool))
                        } else {
                            (rand_a(rng, self.pool), Val::Null)
                        };
                        let rowid = self.new_rowid(rng, &[a.clone(), b.clone()]);
                        return make(a, b, rowid, true);
                    }
                }
            }
        }
        let (a, b) = (rand_a(rng, self.pool), rand_b(rng, self.pool));
        let rowid = self.new_rowid(rng, &[a.clone(), b.clone()]);
        make(a, b, rowid, false)
    }

    /// A rowid that makes the key `(cols, rowid)` new.
    fn new_rowid(&self, rng: &mut ChaCha8Rng, cols: &[Val]) -> i64 {
        let s = &self.replay.st.irows;
        for _ in 0..20 {
            let r = rng.random_range(1..=ROWID_MAX);
            let d = self.replay.st.idef;
            if !s
                .iter()
                .any(|k| k.1 == r && cmp_cols(d, &k.0, cols) == Ordering::Equal)
            {
                return r;
            }
        }
        ROWID_MAX + 1
    }

    /// A key that is above every key of the index: an append.
    fn above_all_key(&self, rng: &mut ChaCha8Rng) -> (Val, Val, i64) {
        let (d, s) = (self.replay.st.idef, &self.replay.st.irows);
        match imax(s) {
            // The same columns as the largest key, with a higher rowid.
            Some((cols, r)) if rng.random_bool(0.6) => {
                (cols[0].clone(), cols[1].clone(), r + rng.random_range(1..4))
            }
            // A random key, if it is above every key (the index order decides, so for a DESC
            // column this is a small value).
            Some((cols, r)) => {
                for _ in 0..10 {
                    let key = (
                        vec![rand_a(rng, self.pool), rand_b(rng, self.pool)],
                        rng.random_range(1..=ROWID_MAX),
                    );
                    if above_all(d, &key, s) {
                        return (key.0[0].clone(), key.0[1].clone(), key.1);
                    }
                }
                (cols[0].clone(), cols[1].clone(), r + 1)
            }
            None => (
                rand_a(rng, self.pool),
                rand_b(rng, self.pool),
                rng.random_range(1..=ROWID_MAX),
            ),
        }
    }

    /// An `IdxDelete` of a key that is in the index.
    fn idx_delete(&self, rng: &mut ChaCha8Rng, c: usize) -> IOp {
        let s = &self.replay.st.irows;
        if s.is_empty() {
            return IOp::IdxDelete {
                cur: c,
                a: rand_a(rng, self.pool),
                b: rand_b(rng, self.pool),
                rowid: 1,
            };
        }
        let (cols, rowid) = s[rng.random_range(0..s.len())].clone();
        IOp::IdxDelete {
            cur: c,
            a: cols[0].clone(),
            b: cols[1].clone(),
            rowid,
        }
    }

    /// A prefix of the entry under the cursor, or a random prefix.
    fn near_entry(&self, rng: &mut ChaCha8Rng, c: usize) -> Prefix {
        match &self.replay.st.icurs[c].pos {
            IPos::At(k) | IPos::Gone(k) if rng.random_bool(0.7) => {
                let len = rng.random_range(1..=2);
                perturb(rng, self.pool, k.0[..len].to_vec())
            }
            _ => self.prefix(rng, None),
        }
    }

    /// A prefix of one or two values, near the entries of the index, so that seeks hit and
    /// miss. `len` fixes the length, else it is drawn.
    fn prefix(&self, rng: &mut ChaCha8Rng, len: Option<usize>) -> Prefix {
        let len = len.unwrap_or_else(|| rng.random_range(1..=2));
        let s = &self.replay.st.irows;
        let base = if !s.is_empty() && rng.random_bool(0.75) {
            s[rng.random_range(0..s.len())].0.clone()
        } else {
            vec![rand_a(rng, self.pool), rand_b(rng, self.pool)]
        };
        perturb(rng, self.pool, base[..len].to_vec())
    }

    /// Updates the model and the filter history after `op` was emitted.
    fn note(&mut self, op: &IOp) {
        let before = self.replay.st.clone();
        self.replay
            .apply(op)
            .expect("the generator checked legality");
        let c = cursor_of(op);
        // Reads, compare ops and `NullRow` do not move the cursor. Everything else does.
        let moves = !matches!(
            op,
            IOp::Column(..)
                | IOp::IdxRowId(_)
                | IOp::Observe(_)
                | IOp::NullRow(_)
                | IOp::IdxGE(..)
                | IOp::IdxGT(..)
                | IOp::IdxLE(..)
                | IOp::IdxLT(..)
        );
        // `NoConflict` with a NULL does not touch the engine cursor, so the history of the
        // cursor stays as it is (a position that `Count` or `SeekEnd` broke stays broken).
        let keeps_position = matches!(op, IOp::NoConflict(_, p) if p.contains(&Val::Null));
        if moves && !keeps_position {
            let insert = match op {
                IOp::IdxInsert { a, b, rowid, .. } => Some((vec![a.clone(), b.clone()], *rowid)),
                _ => None,
            };
            if let Some(k) = &insert {
                self.written.push(k.clone());
                self.ever.push(k.clone());
            }
            self.after_insert[c] = insert.is_some();
            self.appended[c] = insert.is_some_and(|k| above_all(before.idef, &k, &before.irows));
            let found = matches!(op, IOp::Found(..) | IOp::NotFound(..) | IOp::NoConflict(..));
            let pos = &self.replay.st.icurs[c].pos;
            self.found_miss[c] = found && matches!(pos, IPos::Seeked(_));
            self.found_hit[c] = found && matches!(pos, IPos::At(_));
            self.peer_wrote[c] = false;
        }
        let writes = matches!(
            op,
            IOp::IdxInsert { .. } | IOp::IdxDelete { .. } | IOp::Delete(_) | IOp::Clear(_)
        );
        if writes {
            for (j, w) in self.peer_wrote.iter_mut().enumerate() {
                *w |= j != c;
            }
        }
    }

    // -----------------------------------------------------------------------------------
    // Known gap filters. Each returns true when `op` has the pattern of the gap.
    // -----------------------------------------------------------------------------------

    fn blocked(&self, op: &IOp) -> bool {
        let wal_or_eph = self
            .scope
            .iter()
            .any(|c| matches!(c.0, DbKind::Wal | DbKind::Ephemeral));
        let mvcc = self.scope.iter().any(|c| c.0 == DbKind::Mvcc);
        let mvcc_ckpt = self.scope.contains(&(DbKind::Mvcc, Layout::Checkpointed));
        let spec_model = self.scope.iter().any(|c| c.0 == DbKind::SpecModel);
        (self
            .gaps
            .has(IdxGaps::MI1_SPEC_MODEL_NO_CONFLICT_NULL_AFTER_NULL_ROW)
            && spec_model
            && matches!(op, IOp::NoConflict(c, p)
                if self.replay.st.icurs[*c].null_row && p.contains(&Val::Null)))
            || (self.gaps.has(IdxGaps::IDX_D1_MVCC_CKPT) && mvcc_ckpt && self.idx_d1(op))
            || (self.gaps.has(IdxGaps::IDX_END_INSERT) && wal_or_eph && self.idx_end_insert(op))
            || (self.gaps.has(IdxGaps::ROWDATA_IDX_INSERT)
                && wal_or_eph
                && self.rowdata_idx_insert(op))
            || (self.gaps.has(IdxGaps::ROWDATA_NULL_ROW) && self.rowdata_null_row(op))
            || (self.gaps.has(IdxGaps::IDX_N4_MVCC_CKPT_DELETE_OF_WRITTEN)
                && mvcc_ckpt
                && self.idx_n4(op))
            || (self.gaps.has(IdxGaps::CL1_MVCC_EQUAL_KEY_REINSERT) && mvcc && self.cl1(op))
            || (self.gaps.has(IdxGaps::PD2_DELETE_AFTER_PEER_WRITE)
                && wal_or_eph
                && matches!(*op, IOp::Delete(c) if self.peer_wrote[c]))
            || (self.gaps.has(IdxGaps::OV1_NEXT_AFTER_FOUND_HIT)
                && mvcc
                && matches!(*op, IOp::Next(c) if self.found_hit[c]))
            || (self.gaps.has(IdxGaps::FM1_NEXT_AFTER_FOUND_MISS)
                && matches!(*op, IOp::Next(c) if self.found_miss[c]))
            || (self.gaps.has(IdxGaps::PW2_READ_AFTER_INSERT_AND_PEER_WRITE)
                && wal_or_eph
                && (self.uses_entry(op, |c| self.after_insert[c] && self.peer_wrote[c])
                    || matches!(*op, IOp::Next(c) | IOp::Prev(c) | IOp::Delete(c)
                        if self.after_insert[c] && self.peer_wrote[c])))
            || (self.gaps.has(IdxGaps::G3_NULL_ROW_AND_PEER_WRITE)
                && wal_or_eph
                && self.uses_entry(op, |c| {
                    self.replay.st.icurs[c].null_row && self.peer_wrote[c]
                }))
    }

    /// A read or a compare of the entry under a cursor for which `bad` is true.
    fn uses_entry(&self, op: &IOp, bad: impl Fn(usize) -> bool) -> bool {
        match op {
            IOp::Observe(c)
            | IOp::Column(c, _)
            | IOp::IdxRowId(c)
            | IOp::IdxGE(c, _)
            | IOp::IdxGT(c, _)
            | IOp::IdxLE(c, _)
            | IOp::IdxLT(c, _) => bad(*c),
            _ => false,
        }
    }

    /// CL1: an `IdxInsert` of a key that is equal but not identical to a key that the index
    /// held earlier.
    fn cl1(&self, op: &IOp) -> bool {
        let IOp::IdxInsert { a, b, rowid, .. } = op else {
            return false;
        };
        let key = (vec![a.clone(), b.clone()], *rowid);
        let d = self.replay.st.idef;
        self.ever
            .iter()
            .any(|k| k != &key && cmp_key(d, k, &key) == Ordering::Equal)
    }

    /// IDX_N4_MVCC_CKPT_DELETE_OF_WRITTEN: `Delete` of an entry that an `IdxInsert` of this run
    /// wrote.
    fn idx_n4(&self, op: &IOp) -> bool {
        let IOp::Delete(c) = *op else { return false };
        matches!(&self.replay.st.icurs[c].pos, IPos::At(k) if self.written.contains(k))
    }

    /// IDX_D1_MVCC_CKPT: `Next` or `Prev` right after an `IdxInsert` of the same cursor.
    fn idx_d1(&self, op: &IOp) -> bool {
        matches!(*op, IOp::Next(c) | IOp::Prev(c) if self.after_insert[c])
    }

    /// IDX_END_INSERT: an op that uses the position of a cursor that appended a key.
    fn idx_end_insert(&self, op: &IOp) -> bool {
        match op {
            IOp::Next(c)
            | IOp::Prev(c)
            | IOp::Delete(c)
            | IOp::Observe(c)
            | IOp::Column(c, _)
            | IOp::IdxRowId(c)
            | IOp::IdxGE(c, _)
            | IOp::IdxGT(c, _)
            | IOp::IdxLE(c, _)
            | IOp::IdxLT(c, _) => self.appended[*c],
            _ => false,
        }
    }

    /// ROWDATA_IDX_INSERT: a `RowData` read right after an `IdxInsert`.
    fn rowdata_idx_insert(&self, op: &IOp) -> bool {
        self.is_row_data()
            && matches!(*op, IOp::Observe(c) | IOp::Column(c, _) | IOp::IdxRowId(c)
                if self.after_insert[c])
    }

    /// ROWDATA_NULL_ROW: a `RowData` read of a cursor with the null-row flag.
    fn rowdata_null_row(&self, op: &IOp) -> bool {
        self.is_row_data()
            && matches!(*op, IOp::Observe(c) | IOp::Column(c, _) | IOp::IdxRowId(c)
                if self.replay.st.icurs[c].null_row)
    }

    fn is_row_data(&self) -> bool {
        matches!(self.mode, ReadMode::RowData | ReadMode::RowDataRaw)
    }
}

/// Where the values of new columns come from.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Pool {
    /// NULL and small integers.
    Ints,
    /// NULL, small integers and the texts of `TEXTS`.
    Mixed,
}

/// Texts that differ by case and by trailing blanks. There is no NUL byte (SPEC 2c).
const TEXTS: [&str; 13] = [
    "", " ", "  ", "a", "A", "a ", "a\t", "b", "B", "[", "_", "é", "É",
];

fn rand_text(rng: &mut ChaCha8Rng) -> Val {
    Val::text(TEXTS[rng.random_range(0..TEXTS.len())])
}

fn rand_a(rng: &mut ChaCha8Rng, pool: Pool) -> Val {
    if rng.random_bool(0.15) {
        Val::Null
    } else if pool == Pool::Mixed && rng.random_bool(0.65) {
        rand_text(rng)
    } else {
        Val::Int(rng.random_range(0..A_RANGE))
    }
}

fn rand_b(rng: &mut ChaCha8Rng, pool: Pool) -> Val {
    if rng.random_bool(0.1) {
        Val::Null
    } else if pool == Pool::Mixed && rng.random_bool(0.65) {
        rand_text(rng)
    } else {
        Val::Int(rng.random_range(0..B_RANGE))
    }
}

/// A text with the other case, or with a trailing space added or removed. Other values stay.
fn variant(rng: &mut ChaCha8Rng, v: &Val) -> Val {
    let Val::Text(t) = v else { return v.clone() };
    match rng.random_range(0..4) {
        0 => Val::Text(t.clone()),
        1 => Val::Text(t.to_ascii_uppercase()),
        2 => Val::Text(t.to_ascii_lowercase()),
        _ => Val::Text(match t.strip_suffix(' ') {
            Some(shorter) => shorter.to_string(),
            None => format!("{t} "),
        }),
    }
}

/// Moves some values of a prefix by one, or to NULL, so that a seek can land between entries.
/// A text becomes a variant of itself or another text.
fn perturb(rng: &mut ChaCha8Rng, pool: Pool, mut p: Prefix) -> Prefix {
    for v in p.iter_mut() {
        match rng.random_range(0..20) {
            0..=2 => {
                if let Val::Int(x) = v {
                    *x = (*x - 1).max(-1);
                }
            }
            3..=5 => {
                if let Val::Int(x) = v {
                    *x += 1;
                }
            }
            6..=7 => *v = Val::Null,
            8..=11 if pool == Pool::Mixed => *v = variant(rng, v),
            12 if pool == Pool::Mixed => *v = rand_text(rng),
            _ => {}
        }
    }
    p
}

fn cursor_of(op: &IOp) -> usize {
    match op {
        IOp::Rewind(c)
        | IOp::Last(c)
        | IOp::Next(c)
        | IOp::Prev(c)
        | IOp::SeekGE(c, _)
        | IOp::SeekGT(c, _)
        | IOp::SeekLE(c, _)
        | IOp::SeekLT(c, _)
        | IOp::Found(c, _)
        | IOp::NotFound(c, _)
        | IOp::NoConflict(c, _)
        | IOp::IdxGE(c, _)
        | IOp::IdxGT(c, _)
        | IOp::IdxLE(c, _)
        | IOp::IdxLT(c, _)
        | IOp::Delete(c)
        | IOp::Column(c, _)
        | IOp::IdxRowId(c)
        | IOp::SeekEnd(c)
        | IOp::NullRow(c)
        | IOp::Count(c)
        | IOp::Clear(c)
        | IOp::Observe(c) => *c,
        IOp::IdxInsert { cur, .. } | IOp::IdxDelete { cur, .. } => *cur,
        IOp::DeferredSeek { .. } | IOp::TblColumn(..) | IOp::FinishSeek(_) => {
            unreachable!("the walk does not use table cursors")
        }
    }
}

fn map_cursor(op: &IOp, f: impl Fn(usize) -> usize) -> IOp {
    match op.clone() {
        IOp::Rewind(c) => IOp::Rewind(f(c)),
        IOp::Last(c) => IOp::Last(f(c)),
        IOp::Next(c) => IOp::Next(f(c)),
        IOp::Prev(c) => IOp::Prev(f(c)),
        IOp::SeekGE(c, p) => IOp::SeekGE(f(c), p),
        IOp::SeekGT(c, p) => IOp::SeekGT(f(c), p),
        IOp::SeekLE(c, p) => IOp::SeekLE(f(c), p),
        IOp::SeekLT(c, p) => IOp::SeekLT(f(c), p),
        IOp::Found(c, p) => IOp::Found(f(c), p),
        IOp::NotFound(c, p) => IOp::NotFound(f(c), p),
        IOp::NoConflict(c, p) => IOp::NoConflict(f(c), p),
        IOp::IdxGE(c, p) => IOp::IdxGE(f(c), p),
        IOp::IdxGT(c, p) => IOp::IdxGT(f(c), p),
        IOp::IdxLE(c, p) => IOp::IdxLE(f(c), p),
        IOp::IdxLT(c, p) => IOp::IdxLT(f(c), p),
        IOp::IdxInsert {
            cur,
            a,
            b,
            rowid,
            use_seek,
        } => IOp::IdxInsert {
            cur: f(cur),
            a,
            b,
            rowid,
            use_seek,
        },
        IOp::IdxDelete { cur, a, b, rowid } => IOp::IdxDelete {
            cur: f(cur),
            a,
            b,
            rowid,
        },
        IOp::Delete(c) => IOp::Delete(f(c)),
        IOp::Column(c, i) => IOp::Column(f(c), i),
        IOp::IdxRowId(c) => IOp::IdxRowId(f(c)),
        IOp::SeekEnd(c) => IOp::SeekEnd(f(c)),
        IOp::NullRow(c) => IOp::NullRow(f(c)),
        IOp::Count(c) => IOp::Count(f(c)),
        IOp::Clear(c) => IOp::Clear(f(c)),
        IOp::Observe(c) => IOp::Observe(f(c)),
        IOp::DeferredSeek { .. } | IOp::TblColumn(..) | IOp::FinishSeek(_) => {
            unreachable!("the walk does not use table cursors")
        }
    }
}

// ---------------------------------------------------------------------------------------
// Shrinking and reporting
// ---------------------------------------------------------------------------------------

/// What a failure is about: one engine config, or the direct comparison of two configs.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Cfg {
    Single(DbKind, Layout),
    Cross((DbKind, Layout), (DbKind, Layout)),
}

impl Cfg {
    fn configs(self) -> Vec<(DbKind, Layout)> {
        match self {
            Cfg::Single(k, l) => vec![(k, l)],
            Cfg::Cross(a, b) => vec![a, b],
        }
    }
}

/// Runs the case on `cfg` and returns the signature of the failure, if any.
fn evaluate(cfg: Cfg, case: &Case) -> Option<String> {
    let verdict = |kind: DbKind, got: &Outcome| {
        icheck(
            &case.rows,
            case.nidx,
            kind == DbKind::Ephemeral,
            case.idef,
            &case.ops,
            got,
        )
    };
    match cfg {
        Cfg::Single(k, l) => match verdict(k, &run_one(k, l, case)) {
            IVerdict::Fail(f) => Some(f.signature(&case.ops)),
            _ => None,
        },
        Cfg::Cross((ka, la), (kb, lb)) => {
            let (a, b) = (run_one(ka, la, case), run_one(kb, lb, case));
            let (IVerdict::Pass(ta), IVerdict::Pass(tb)) = (verdict(ka, &a), verdict(kb, &b))
            else {
                return None;
            };
            compare_engines(&case.ops, (&a.obs, &ta), (&b.obs, &tb))
                .map(|f: IFailure| f.signature(&case.ops))
        }
    }
}

/// Removes ops, setup rows and unused cursors while the failure keeps its signature.
fn shrink(case: &Case, cfg: Cfg, sig: &str, filter: &Filter) -> Case {
    let still_fails = |c: &Case| {
        // A candidate must stay a program that the generator could have written, and that the
        // model accepts on every config of the failure. Without this the shrinker walks from
        // the bug it started with to a known gap, or to an illegal program.
        !c.ops.is_empty()
            && cfg
                .configs()
                .iter()
                .all(|(k, _)| is_legal(&c.rows, c.nidx, *k == DbKind::Ephemeral, c.idef, &c.ops))
            && filter.clean(c)
            && evaluate(cfg, c).as_deref() == Some(sig)
    };
    let mut cur = case.clone();
    loop {
        let before = (cur.ops.len(), cur.rows.len(), cur.nidx, cur.idef);
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
        while i < cur.rows.len() {
            let mut cand = cur.clone();
            cand.rows.remove(i);
            if still_fails(&cand) {
                cur = cand;
            } else {
                i += 1;
            }
        }
        // The definition may be the default one, or default for one column.
        for cand_def in [
            IdxDef::default(),
            IdxDef::new(cur.idef.cols[0], ColDef::default()),
            IdxDef::new(ColDef::default(), cur.idef.cols[1]),
        ] {
            if cand_def != cur.idef {
                let cand = Case {
                    idef: cand_def,
                    ..cur.clone()
                };
                if still_fails(&cand) {
                    cur = cand;
                }
            }
        }
        // A prefix of two values may need only one.
        for i in 0..cur.ops.len() {
            if let Some(cand) = with_shorter_prefix(&cur, i) {
                if still_fails(&cand) {
                    cur = cand;
                }
            }
        }
        // Drop a cursor that no op uses, and renumber the cursors above it.
        for j in (0..cur.nidx).rev() {
            if cur.nidx > 1 && cur.ops.iter().all(|op| cursor_of(op) != j) {
                let mut cand = cur.clone();
                cand.nidx -= 1;
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
        if before == (cur.ops.len(), cur.rows.len(), cur.nidx, cur.idef) {
            return cur;
        }
    }
}

/// The case with the prefix of op `i` cut to one value, if the op has a prefix of two.
fn with_shorter_prefix(case: &Case, i: usize) -> Option<Case> {
    let shorten = |p: &Prefix| (p.len() == 2).then(|| vec![p[0].clone()]);
    let op = match case.ops[i].clone() {
        IOp::SeekGE(c, p) => IOp::SeekGE(c, shorten(&p)?),
        IOp::SeekGT(c, p) => IOp::SeekGT(c, shorten(&p)?),
        IOp::SeekLE(c, p) => IOp::SeekLE(c, shorten(&p)?),
        IOp::SeekLT(c, p) => IOp::SeekLT(c, shorten(&p)?),
        IOp::Found(c, p) => IOp::Found(c, shorten(&p)?),
        IOp::NotFound(c, p) => IOp::NotFound(c, shorten(&p)?),
        IOp::NoConflict(c, p) => IOp::NoConflict(c, shorten(&p)?),
        IOp::IdxGE(c, p) => IOp::IdxGE(c, shorten(&p)?),
        IOp::IdxGT(c, p) => IOp::IdxGT(c, shorten(&p)?),
        IOp::IdxLE(c, p) => IOp::IdxLE(c, shorten(&p)?),
        IOp::IdxLT(c, p) => IOp::IdxLT(c, shorten(&p)?),
        _ => return None,
    };
    let mut cand = case.clone();
    cand.ops[i] = op;
    Some(cand)
}

struct Report {
    /// Same key, same bug. Used to print each minimal failure once.
    key: String,
    text: String,
}

/// An op as Rust source: `Prefix` is a `Vec`, so a prefix needs `vec!`.
fn show_op(op: &IOp) -> String {
    format!("{op:?}").replacen(", [", ", vec![", 1)
}

fn show_slot<A: std::fmt::Debug>(s: &Spec<A>) -> String {
    match s {
        Spec::Unspec => "?".to_string(),
        Spec::Is(v) => format!("{v:?}"),
    }
}

fn report(case: &Case, original: &Case, cfg: Cfg, case_seed: u64) -> Report {
    let configs = cfg.configs();
    let mut text = String::new();
    let sig = evaluate(cfg, case).unwrap_or_else(|| "flaky".to_string());
    text += &format!("=== index cursor contract failure: {sig}\n");
    text += &format!("seed {case_seed:#x} (rerun: SEED={case_seed:#x} ITERS=1)\n");
    text += &format!(
        "config {cfg:?}, read mode {:?}, index {}, setup rows {:?}, {} cursors\n",
        case.mode,
        case.idef.src(),
        case.rows,
        case.nidx
    );
    for &(k, l) in &configs {
        let got = run_one(k, l, case);
        text += &format!("{k:?}/{l:?}, step: op | engine | model (jump rowid c0 c1, ? is open)\n");
        let mut rep = IReplay::new(&case.rows, case.nidx, k == DbKind::Ephemeral, case.idef);
        if let Some(e) = &got.error {
            text += &format!("  engine error after {} ops: {e}\n", got.obs.len());
        }
        for (i, op) in case.ops.iter().enumerate() {
            let model = rep.apply(op);
            let e = got.obs.get(i).map_or("-".to_string(), |o| format!("{o:?}"));
            let m = model.map_or("illegal".to_string(), |m| {
                let (j, r, c0, c1) = project(op, &m);
                format!(
                    "{j} {} {} {}",
                    show_slot(&r),
                    show_slot(&c0),
                    show_slot(&c1)
                )
            });
            text += &format!("  {i:2}: {op:?} | {e} | {m}\n");
        }
    }
    text += &format!(
        "unshrunk: {} cursors, mode {:?}, index {}, rows {:?}, ops {:?}\n",
        original.nidx,
        original.mode,
        original.idef.src(),
        original.rows,
        original.ops
    );
    text += "repro:\n";
    text += "    #[test]\n    fn repro() {\n        use IOp::*;\n";
    text += &format!("        let rows = vec!{:?};\n", case.rows);
    text += "        let ops = vec![\n";
    for op in &case.ops {
        text += &format!("            {},\n", show_op(op));
    }
    text += "        ];\n";
    for &(k, l) in &configs {
        text += &format!(
            "        assert_agrees_def(DbKind::{k:?}, Layout::{l:?}, ReadMode::{:?}, {}, &rows, {}, &ops);\n",
            case.mode,
            case.idef.src(),
            case.nidx
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
    const QUICK_ITERS: usize = 100;

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
    fn index_random_walk_all_configs() {
        run_walk(QUICK_ITERS);
    }

    /// The long run: `SEED` and `ITERS` as above, 2000 cases by default.
    #[test]
    #[ignore = "long run, use ITERS and SEED"]
    fn index_random_walk_long() {
        run_walk(2000);
    }

    // Gaps that the walk found. Each test is the minimal program that the shrinker printed. The
    // tests are `known_bug_*` tests: they pass while the bug is present (the body must panic).
    // Each one asserts what the model says. The ids are in SPEC.md section 5b; an `idx` in
    // the name marks the index twin of a table gap.

    use super::super::index_model::{assert_agrees, Val};
    use super::super::known_bug;

    const NUL: Val = Val::Null;

    fn v(x: i64) -> Val {
        Val::Int(x)
    }

    const MVCC_CKPT: [(DbKind, Layout); 1] = [(DbKind::Mvcc, Layout::Checkpointed)];

    /// Runs the program on each config and asserts that the engine agrees with the model.
    #[track_caller]
    fn agree(configs: &[(DbKind, Layout)], rows: &[Row], nidx: usize, ops: &[IOp]) {
        for &(kind, layout) in configs {
            assert_agrees(kind, layout, ReadMode::Column, rows, nidx, ops);
        }
    }

    /// IDX_D1 and IDX_N4, the index twin of the table gap N4: `Delete` of an entry that an
    /// `IdxInsert` of this run wrote. MVCC with checkpointed entries loses the position, so
    /// `Next` after it finds nothing.
    ///
    /// Fails today because of IDX_N4; see SPEC section 5b.
    #[test]
    fn known_bug_idx_n4_delete_of_inserted_entry_then_next() {
        known_bug("IDX_N4", "Mismatch", || {
            use IOp::*;
            let rows = [(6, v(2), v(3))];
            let ops = [
                SeekGE(0, vec![v(2)]),
                IdxInsert {
                    cur: 0,
                    a: NUL,
                    b: v(1),
                    rowid: 33,
                    use_seek: true,
                },
                Delete(0),
                Next(0),
            ];
            agree(&MVCC_CKPT, &rows, 1, &ops);
        });
    }

    /// The walk must draw non-default definitions, texts, and the replacement of a key by a
    /// case variant, or it tests nothing of SPEC 2c.
    #[test]
    fn index_generator_covers_collation_cases() {
        let (mut defs, mut texts, mut replaces) = (0, 0, 0);
        for seed in 0..300 {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let (case, _) = generate(&mut rng, IdxGaps::ALL, Family::Any);
            defs += usize::from(!case.idef.is_default());
            texts += usize::from(case.rows.iter().any(|r| matches!(r.1, Val::Text(_))));
            replaces += case
                .ops
                .windows(2)
                .filter(|w| {
                    matches!((&w[0], &w[1]),
                    (IOp::IdxDelete { a: a0, rowid: r0, .. }, IOp::IdxInsert { a: a1, rowid: r1, .. })
                    if r0 == r1 && a0 != a1)
                })
                .count();
        }
        assert!(
            defs > 60 && texts > 60 && replaces > 5,
            "{defs} {texts} {replaces}"
        );
    }

    #[test]
    fn index_generator_only_emits_legal_programs() {
        for seed in 0..300 {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let (case, filter) = generate(&mut rng, IdxGaps::ALL, Family::Any);
            let mut rep = IReplay::new(&case.rows, case.nidx, filter.ephemeral(), case.idef);
            for op in &case.ops {
                assert!(rep.apply(op).is_some(), "seed {seed}: {op:?}");
            }
            assert!(filter.clean(&case), "seed {seed}: {:?}", case.ops);
        }
    }
}
