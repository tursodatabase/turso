//! Hand-written scenarios for the index cursor ops (SPEC 2b, 3). Each test runs one program
//! on every engine, layout and read mode, and asserts two things: the observations are the
//! same everywhere, and they are the values that SPEC says (worked out by hand in the test).
//!
//! The setup rows of `ROWS` give this index order, `(a, b, rowid)`:
//! `(NULL,5,6) (1,10,1) (1,20,2) (2,10,3) (2,30,4) (3,NULL,5)`.
//! Column `a` has duplicates, NULL sorts first, and `b` is NULL in the last entry.

use super::index_model::Val;
use super::index_program::{run_index, IObs, IOp::*, IndexRun, Outcome, Row};
use super::program::ReadMode;
use super::{known_bug, DbKind, Layout};

const NULL: Val = Val::Null;

const fn v(x: i64) -> Val {
    Val::Int(x)
}

const ROWS: &[Row] = &[
    (1, v(1), v(10)),
    (2, v(1), v(20)),
    (3, v(2), v(10)),
    (4, v(2), v(30)),
    (5, v(3), NULL),
    (6, NULL, v(5)),
];

/// A prefix, written as `p![1, NULL]`.
macro_rules! p {
    ($($x:expr),*) => { vec![$(Val::from($x)),*] };
}

// Expected observations.

/// No jump, nothing read. Also what a read of a cursor without a row gives.
fn n() -> IObs {
    IObs {
        jumped: false,
        rowid: None,
        c0: NULL,
        c1: NULL,
    }
}

/// The jump was taken.
fn j() -> IObs {
    IObs {
        jumped: true,
        ..n()
    }
}

/// `Observe` on the entry `(a, b, rowid)`.
fn e(rowid: i64, a: impl Into<Val>, b: impl Into<Val>) -> IObs {
    IObs {
        jumped: false,
        rowid: Some(rowid),
        c0: a.into(),
        c1: b.into(),
    }
}

/// A rowid or a count.
fn r(rowid: i64) -> IObs {
    IObs {
        rowid: Some(rowid),
        ..n()
    }
}

/// The configs of every test. Layout has no meaning for the ephemeral engine, so it runs once.
const CONFIGS: [(DbKind, Layout); 5] = [
    (DbKind::Wal, Layout::AllInMemory),
    (DbKind::Wal, Layout::Checkpointed),
    (DbKind::Mvcc, Layout::AllInMemory),
    (DbKind::Mvcc, Layout::Checkpointed),
    (DbKind::Ephemeral, Layout::AllInMemory),
];

/// The read modes that agree everywhere, as long as the program does not read a cursor that
/// an `IdxInsert` just wrote, or a null-row cursor, or a cursor with a pending `DeferredSeek`
/// (see the `known_bug_*` tests at the end of the file).
const SAFE_MODES: [ReadMode; 2] = [ReadMode::Column, ReadMode::ColumnRange];

type Ops = [super::index_program::IOp];

/// What one test runs on.
#[derive(Clone, Copy)]
struct Opts {
    /// The number of index cursors. All of them are open on `i_ab`.
    nidx: usize,
    /// The number of table cursors, for `DeferredSeek`.
    ntbl: usize,
    modes: &'static [ReadMode],
    /// The first cursor of an ephemeral run starts on the last setup entry, because the
    /// program wrote the setup rows with it. A test that reads a fresh cursor sets this to
    /// false.
    ephemeral: bool,
}

const ALL_MODES: &[ReadMode] = &ReadMode::ALL;
const SAFE: &[ReadMode] = &SAFE_MODES;

impl Default for Opts {
    fn default() -> Self {
        Opts {
            nidx: 1,
            ntbl: 0,
            modes: ALL_MODES,
            ephemeral: true,
        }
    }
}

/// Runs the program on every config and read mode of `opts`.
fn run_all(rows: &[Row], opts: Opts, ops: &Ops) -> Vec<(String, Outcome)> {
    let mut out = Vec::new();
    for (kind, layout) in CONFIGS {
        // `OpenDup` does not support index cursors, so an ephemeral index has one cursor.
        if kind == DbKind::Ephemeral && (opts.nidx > 1 || !opts.ephemeral) {
            continue;
        }
        for mode in opts.modes {
            let cfg = IndexRun {
                kind,
                layout,
                rows,
                nidx: opts.nidx,
                ntbl: opts.ntbl,
                mode: *mode,
            };
            out.push((
                format!("{kind:?}/{layout:?}/{mode:?}"),
                run_index(&cfg, ops),
            ));
        }
    }
    out
}

/// Asserts that all configs give `expected` and no error.
#[track_caller]
fn check_all(rows: &[Row], nidx: usize, ops: &Ops, expected: &[IObs]) {
    check(
        rows,
        Opts {
            nidx,
            ..Opts::default()
        },
        ops,
        expected,
    )
}

#[track_caller]
fn check(rows: &[Row], opts: Opts, ops: &Ops, expected: &[IObs]) {
    assert_eq!(ops.len(), expected.len(), "one observation per op");
    let mut failures = Vec::new();
    for (label, outcome) in run_all(rows, opts, ops) {
        if let Some(why) = divergence(&outcome, ops, expected) {
            failures.push(format!("{label}: {why}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} configs differ from the expected observations of {ops:?}\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Describes the first op where the outcome differs from the expected observations.
fn divergence(outcome: &Outcome, ops: &Ops, expected: &[IObs]) -> Option<String> {
    for (i, want) in expected.iter().enumerate() {
        match outcome.obs.get(i) {
            Some(got) if got == want => {}
            Some(got) => {
                return Some(format!(
                    "op #{i} {:?}: expected {want:?}, got {got:?}",
                    ops[i]
                ))
            }
            None => {
                return Some(format!(
                    "op #{i} {:?}: no observation, error {:?}",
                    ops[i], outcome.error
                ))
            }
        }
    }
    outcome.error.as_ref().map(|e| format!("error {e}"))
}

#[test]
fn scan_forward_with_duplicates_in_a() {
    let ops = [
        Rewind(0),
        Observe(0),
        Next(0),
        Observe(0),
        Next(0),
        Observe(0),
        Next(0),
        Observe(0),
        Next(0),
        Observe(0),
        Next(0),
        Observe(0),
        // The scan ended: no jump, and the cursor reads NULL.
        Next(0),
        Observe(0),
    ];
    let expected = [
        n(),
        e(6, NULL, 5),
        j(),
        e(1, 1, 10),
        j(),
        e(2, 1, 20),
        j(),
        e(3, 2, 10),
        j(),
        e(4, 2, 30),
        j(),
        e(5, 3, NULL),
        n(),
        n(),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn scan_backward_with_duplicates_in_a() {
    let ops = [
        Last(0),
        Observe(0),
        Prev(0),
        Observe(0),
        Prev(0),
        Observe(0),
        Prev(0),
        Observe(0),
        Prev(0),
        Observe(0),
        Prev(0),
        Observe(0),
        Prev(0),
        Observe(0),
    ];
    let expected = [
        n(),
        e(5, 3, NULL),
        j(),
        e(4, 2, 30),
        j(),
        e(3, 2, 10),
        j(),
        e(2, 1, 20),
        j(),
        e(1, 1, 10),
        j(),
        e(6, NULL, 5),
        n(),
        n(),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn separate_reads_agree_with_observe() {
    // `Column` and `IdxRowId` read one field each, `Observe` reads all.
    let ops = [
        SeekGE(0, p![2]),
        Column(0, 0),
        Column(0, 1),
        IdxRowId(0),
        Observe(0),
    ];
    let expected = [
        n(),
        IObs { c0: v(2), ..n() },
        IObs { c1: v(10), ..n() },
        r(3),
        e(3, 2, 10),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn empty_index() {
    let ops = [
        Rewind(0),
        Observe(0),
        Last(0),
        SeekGE(0, p![1]),
        Count(0),
        Found(0, p![1]),
        NotFound(0, p![1]),
    ];
    let expected = [j(), n(), j(), j(), r(0), n(), j()];
    check_all(&[], 1, &ops, &expected);
}

#[test]
fn seek_with_one_column_prefix() {
    // The prefix `[1]` ignores `b` and the rowid, so the four seeks land on the edges of the
    // run of entries with a = 1.
    let ops = [
        SeekGE(0, p![1]),
        Observe(0),
        SeekGT(0, p![1]),
        Observe(0),
        SeekLE(0, p![1]),
        Observe(0),
        SeekLT(0, p![1]),
        Observe(0),
        // A prefix between two values of `a`.
        SeekGE(0, p![4]),
        Observe(0),
        SeekLE(0, p![4]),
        Observe(0),
        SeekGT(0, p![3]),
        SeekLT(0, p![0]),
    ];
    let expected = [
        n(),
        e(1, 1, 10),
        n(),
        e(3, 2, 10),
        n(),
        e(2, 1, 20),
        n(),
        e(6, NULL, 5),
        j(),
        n(),
        n(),
        e(5, 3, NULL),
        j(),
        // `SeekLT([0])` finds the NULL entry: NULL sorts first.
        n(),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn seek_with_two_column_prefix() {
    let ops = [
        SeekGE(0, p![2, 20]),
        Observe(0),
        SeekGT(0, p![2, 30]),
        Observe(0),
        SeekLE(0, p![2, 20]),
        Observe(0),
        SeekLT(0, p![2, 10]),
        Observe(0),
        SeekGE(0, p![1, 20]),
        Observe(0),
        SeekGT(0, p![1, 20]),
        Observe(0),
    ];
    let expected = [
        n(),
        e(4, 2, 30),
        n(),
        e(5, 3, NULL),
        n(),
        e(3, 2, 10),
        n(),
        e(2, 1, 20),
        n(),
        e(2, 1, 20),
        n(),
        e(3, 2, 10),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn seek_then_scan_across_duplicates() {
    // A seek on `[1]` followed by a scan until `IdxGT [1]`: the usual range loop.
    let ops = [
        SeekGE(0, p![1]),
        IdxGT(0, p![1]),
        Observe(0),
        Next(0),
        IdxGT(0, p![1]),
        Observe(0),
        Next(0),
        IdxGT(0, p![1]),
        Observe(0),
    ];
    let expected = [
        n(),
        n(),
        e(1, 1, 10),
        j(),
        n(),
        e(2, 1, 20),
        j(),
        // On (2,10,3): a = 2 is greater than 1, so the loop stops.
        j(),
        e(3, 2, 10),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn idx_compare_ops_on_an_entry() {
    // The cursor is on (2,10,3). The ops compare only the prefix.
    let ops = [
        SeekGE(0, p![2, 10]),
        // prefix [2]: equal.
        IdxGE(0, p![2]),
        IdxGT(0, p![2]),
        IdxLE(0, p![2]),
        IdxLT(0, p![2]),
        // prefix [2,20]: the entry is smaller.
        IdxGE(0, p![2, 20]),
        IdxGT(0, p![2, 20]),
        IdxLE(0, p![2, 20]),
        IdxLT(0, p![2, 20]),
        // prefix [1]: the entry is greater.
        IdxGE(0, p![1]),
        IdxGT(0, p![1]),
        IdxLE(0, p![1]),
        IdxLT(0, p![1]),
        // The compare ops do not move the cursor.
        Observe(0),
    ];
    let expected = [
        n(),
        j(),
        n(),
        j(),
        n(),
        n(),
        n(),
        j(),
        j(),
        j(),
        j(),
        n(),
        n(),
        e(3, 2, 10),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn idx_compare_on_ended_scan_jumps() {
    // SPEC 2b: on a cursor without an entry the model jumps (Turso; SQLite asserts).
    // Six entries: five `Next` find a row, the sixth one ends the scan.
    let ops = [
        Rewind(0),
        Next(0),
        Next(0),
        Next(0),
        Next(0),
        Next(0),
        Next(0),
        IdxGE(0, p![2]),
        IdxGT(0, p![2]),
        IdxLE(0, p![2]),
        IdxLT(0, p![2]),
    ];
    let expected = [n(), j(), j(), j(), j(), j(), n(), j(), j(), j(), j()];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn found_and_not_found() {
    // After a hit the position is "matched", so there are no reads here, only jumps.
    let ops = [
        Found(0, p![1]),
        Found(0, p![1, 20]),
        Found(0, p![1, 15]),
        Found(0, p![4]),
        NotFound(0, p![1]),
        NotFound(0, p![4]),
        NotFound(0, p![1, 15]),
    ];
    let expected = [j(), j(), n(), n(), n(), j(), j()];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn found_with_null_matches_null() {
    // SPEC 1: for `Found`, NULL equals NULL.
    let ops = [
        Found(0, p![NULL]),
        Found(0, p![NULL, 5]),
        Found(0, p![3, NULL]),
        Found(0, p![NULL, 6]),
        NotFound(0, p![NULL, 5]),
    ];
    let expected = [j(), j(), j(), n(), n()];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn no_conflict_jumps_when_there_is_no_conflict() {
    // `NoConflict` jumps if no entry has the prefix, and also if the prefix has a NULL
    // (two NULLs never conflict in a UNIQUE index).
    let ops = [
        NoConflict(0, p![1, 15]),
        NoConflict(0, p![1, 10]),
        NoConflict(0, p![1]),
        NoConflict(0, p![4]),
        NoConflict(0, p![NULL, 5]),
        NoConflict(0, p![3, NULL]),
        NoConflict(0, p![NULL]),
    ];
    let expected = [j(), n(), n(), j(), j(), j(), j()];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn seek_with_null_in_prefix() {
    // SPEC 1: NULL equals NULL and sorts first, also in a seek.
    let ops = [
        SeekGE(0, p![NULL]),
        Observe(0),
        SeekGT(0, p![NULL]),
        Observe(0),
        SeekLE(0, p![NULL]),
        Observe(0),
        SeekLT(0, p![NULL]),
        SeekGE(0, p![3, NULL]),
        Observe(0),
        SeekLT(0, p![3, NULL]),
        Observe(0),
    ];
    let expected = [
        n(),
        e(6, NULL, 5),
        n(),
        e(1, 1, 10),
        n(),
        e(6, NULL, 5),
        // No entry is smaller than NULL.
        j(),
        n(),
        e(5, 3, NULL),
        n(),
        e(4, 2, 30),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

/// Scans the whole index with `Rewind`/`Next` and `Observe`, to check what the index holds.
fn scan_all() -> Vec<super::index_program::IOp> {
    let mut ops = vec![Rewind(0), Observe(0)];
    for _ in 0..8 {
        ops.push(Next(0));
        ops.push(Observe(0));
    }
    ops
}

/// What `scan_all` gives for the given entries (in index order) and a trailing end.
fn scan_expected(entries: &[(i64, Val, Val)]) -> Vec<IObs> {
    let mut out = vec![if entries.is_empty() { j() } else { n() }];
    let mut it = entries.iter();
    match it.next() {
        Some((rowid, a, b)) => out.push(e(*rowid, a.clone(), b.clone())),
        None => out.push(n()),
    }
    for _ in 0..8 {
        match it.next() {
            Some((rowid, a, b)) => {
                out.push(j());
                out.push(e(*rowid, a.clone(), b.clone()));
            }
            None => {
                out.push(n());
                out.push(n());
            }
        }
    }
    out
}

#[test]
fn idx_insert_after_no_conflict_miss_with_seek() {
    // `NoConflict` miss leaves the token for the prefix, so `use_seek` is legal (SPEC 3).
    let mut ops = vec![
        NoConflict(0, p![1, 15]),
        IdxInsert {
            cur: 0,
            a: v(1),
            b: v(15),
            rowid: 7,
            use_seek: true,
        },
        // The cursor is on the new entry.
        Observe(0),
        Count(0),
    ];
    let prefix = ops.len();
    ops.extend(scan_all());
    let mut expected = vec![j(), n(), e(7, 1, 15), r(7)];
    expected.extend(scan_expected(&[
        (6, NULL, v(5)),
        (1, v(1), v(10)),
        (7, v(1), v(15)),
        (2, v(1), v(20)),
        (3, v(2), v(10)),
        (4, v(2), v(30)),
        (5, v(3), NULL),
    ]));
    assert_eq!(ops.len(), prefix + 18);
    check(
        ROWS,
        Opts {
            modes: SAFE,
            ..Opts::default()
        },
        &ops,
        &expected,
    );
}

#[test]
fn idx_insert_without_seek_flag() {
    // Without `use_seek` the engine seeks by itself, so no token is needed.
    let mut ops = vec![
        IdxInsert {
            cur: 0,
            a: v(2),
            b: v(20),
            rowid: 8,
            use_seek: false,
        },
        Observe(0),
    ];
    let mut expected = vec![n(), e(8, 2, 20)];
    ops.extend(scan_all());
    expected.extend(scan_expected(&[
        (6, NULL, v(5)),
        (1, v(1), v(10)),
        (2, v(1), v(20)),
        (3, v(2), v(10)),
        (8, v(2), v(20)),
        (4, v(2), v(30)),
        (5, v(3), NULL),
    ]));
    // `Observe` right after an `IdxInsert`: see `rowdata_after_idx_insert_reads_the_new_entry`.
    check(
        ROWS,
        Opts {
            modes: SAFE,
            ..Opts::default()
        },
        &ops,
        &expected,
    );
}

#[test]
fn idx_insert_duplicate_prefix_different_rowid() {
    // The same `(a, b)` with another rowid is a different key: the rowid orders them.
    let mut ops = vec![
        IdxInsert {
            cur: 0,
            a: v(1),
            b: v(10),
            rowid: 0,
            use_seek: false,
        },
        IdxInsert {
            cur: 0,
            a: v(1),
            b: v(10),
            rowid: 9,
            use_seek: false,
        },
    ];
    let mut expected = vec![n(), n()];
    ops.extend(scan_all());
    expected.extend(scan_expected(&[
        (6, NULL, v(5)),
        (0, v(1), v(10)),
        (1, v(1), v(10)),
        (9, v(1), v(10)),
        (2, v(1), v(20)),
        (3, v(2), v(10)),
        (4, v(2), v(30)),
        (5, v(3), NULL),
    ]));
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn idx_insert_with_null_key_and_use_seek() {
    // A key with NULL: `NoConflict` jumps without a token, so `use_seek` would have nothing to
    // trust. SPEC 2b: Turso seeks in this case.
    let mut ops = vec![
        NoConflict(0, p![NULL, 7]),
        IdxInsert {
            cur: 0,
            a: NULL,
            b: v(7),
            rowid: 8,
            use_seek: true,
        },
        Observe(0),
    ];
    let mut expected = vec![j(), n(), e(8, NULL, 7)];
    ops.extend(scan_all());
    expected.extend(scan_expected(&[
        (6, NULL, v(5)),
        (8, NULL, v(7)),
        (1, v(1), v(10)),
        (2, v(1), v(20)),
        (3, v(2), v(10)),
        (4, v(2), v(30)),
        (5, v(3), NULL),
    ]));
    check(
        ROWS,
        Opts {
            modes: SAFE,
            ..Opts::default()
        },
        &ops,
        &expected,
    );
}

#[test]
fn idx_insert_at_both_ends() {
    let mut ops = vec![
        IdxInsert {
            cur: 0,
            a: v(0),
            b: v(0),
            rowid: 20,
            use_seek: false,
        },
        IdxInsert {
            cur: 0,
            a: v(9),
            b: v(9),
            rowid: 21,
            use_seek: false,
        },
    ];
    let mut expected = vec![n(), n()];
    ops.extend(scan_all());
    expected.extend(scan_expected(&[
        (6, NULL, v(5)),
        (20, v(0), v(0)),
        (1, v(1), v(10)),
        (2, v(1), v(20)),
        (3, v(2), v(10)),
        (4, v(2), v(30)),
        (5, v(3), NULL),
        (21, v(9), v(9)),
    ]));
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn idx_delete_removes_the_entry() {
    // The position after the own `IdxDelete` is open (SPEC 2b), so the program checks the
    // result with fresh seeks.
    let mut ops = vec![
        SeekGE(0, p![2, 10]),
        IdxDelete {
            cur: 0,
            a: v(2),
            b: v(10),
            rowid: 3,
        },
        NotFound(0, p![2, 10]),
        Count(0),
    ];
    let mut expected = vec![n(), n(), j(), r(5)];
    ops.extend(scan_all());
    expected.extend(scan_expected(&[
        (6, NULL, v(5)),
        (1, v(1), v(10)),
        (2, v(1), v(20)),
        (4, v(2), v(30)),
        (5, v(3), NULL),
    ]));
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn idx_delete_with_null_columns() {
    let mut ops = vec![
        IdxDelete {
            cur: 0,
            a: NULL,
            b: v(5),
            rowid: 6,
        },
        IdxDelete {
            cur: 0,
            a: v(3),
            b: NULL,
            rowid: 5,
        },
    ];
    let mut expected = vec![n(), n()];
    ops.extend(scan_all());
    expected.extend(scan_expected(&[
        (1, v(1), v(10)),
        (2, v(1), v(20)),
        (3, v(2), v(10)),
        (4, v(2), v(30)),
    ]));
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn peer_idx_delete_then_next() {
    // SPEC 1: when a peer deletes our entry, we sit in the gap, and `Next` goes to the entry
    // after it. `Prev` goes to the entry before it.
    let ops = [
        SeekGE(0, p![2, 10]),
        SeekGE(1, p![2, 10]),
        IdxDelete {
            cur: 1,
            a: v(2),
            b: v(10),
            rowid: 3,
        },
        Next(0),
        Observe(0),
    ];
    let expected = [n(), n(), n(), j(), e(4, 2, 30)];
    check_all(ROWS, 2, &ops, &expected);

    let ops = [
        SeekGE(0, p![2, 10]),
        SeekGE(1, p![2, 10]),
        IdxDelete {
            cur: 1,
            a: v(2),
            b: v(10),
            rowid: 3,
        },
        Prev(0),
        Observe(0),
    ];
    let expected = [n(), n(), n(), j(), e(2, 1, 20)];
    check_all(ROWS, 2, &ops, &expected);
}

#[test]
fn delete_then_next() {
    let ops = [
        SeekGE(0, p![2, 10]),
        Delete(0),
        Next(0),
        Observe(0),
        Count(0),
    ];
    let expected = [n(), n(), j(), e(4, 2, 30), r(5)];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn delete_then_prev() {
    let ops = [
        SeekGE(0, p![2, 10]),
        Delete(0),
        Prev(0),
        Observe(0),
        Count(0),
    ];
    let expected = [n(), n(), j(), e(2, 1, 20), r(5)];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn delete_every_entry_with_scan() {
    let ops = [
        Rewind(0),
        Delete(0),
        Next(0),
        Delete(0),
        Next(0),
        Delete(0),
        Next(0),
        Delete(0),
        Next(0),
        Delete(0),
        Next(0),
        Delete(0),
        Next(0),
        Count(0),
    ];
    let expected = [
        n(),
        n(),
        j(),
        n(),
        j(),
        n(),
        j(),
        n(),
        j(),
        n(),
        j(),
        n(),
        // The last `Delete` removed the last entry: the scan ends.
        n(),
        r(0),
    ];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn peer_insert_is_seen_by_next() {
    // SPEC 1: no snapshot inside one transaction. The peer inserts after our entry.
    let ops = [
        SeekGE(0, p![1, 20]),
        IdxInsert {
            cur: 1,
            a: v(1),
            b: v(25),
            rowid: 9,
            use_seek: false,
        },
        Next(0),
        Observe(0),
        Next(0),
        Observe(0),
    ];
    let expected = [n(), n(), j(), e(9, 1, 25), j(), e(3, 2, 10)];
    check_all(ROWS, 2, &ops, &expected);
}

#[test]
fn peer_insert_before_is_seen_by_prev() {
    let ops = [
        SeekGE(0, p![1, 20]),
        IdxInsert {
            cur: 1,
            a: v(1),
            b: v(15),
            rowid: 8,
            use_seek: false,
        },
        Prev(0),
        Observe(0),
        // Going on backwards: the entry before the new one.
        Prev(0),
        Observe(0),
    ];
    let expected = [n(), n(), j(), e(8, 1, 15), j(), e(1, 1, 10)];
    check_all(ROWS, 2, &ops, &expected);
}

#[test]
fn peer_insert_keeps_our_entry() {
    // `At k` stays `At k` while `k` exists.
    let ops = [
        SeekGE(0, p![2, 10]),
        IdxInsert {
            cur: 1,
            a: v(0),
            b: v(1),
            rowid: 30,
            use_seek: false,
        },
        IdxInsert {
            cur: 1,
            a: v(2),
            b: v(10),
            rowid: 4,
            use_seek: false,
        },
        Observe(0),
    ];
    let expected = [n(), n(), n(), e(3, 2, 10)];
    check_all(ROWS, 2, &ops, &expected);
}

#[test]
fn null_row_reads_null() {
    let ops = [
        Rewind(0),
        NullRow(0),
        Observe(0),
        IdxRowId(0),
        // A new seek clears the flag.
        Rewind(0),
        Observe(0),
    ];
    let expected = [n(), n(), n(), n(), n(), e(6, NULL, 5)];
    // `RowData` on a null-row cursor: see `rowdata_on_null_row_reads_null`.
    check(
        ROWS,
        Opts {
            modes: SAFE,
            ..Opts::default()
        },
        &ops,
        &expected,
    );
}

#[test]
fn fresh_cursor_reads_null() {
    let ops = [Observe(0), IdxRowId(0), Column(0, 0), Column(0, 1)];
    let expected = [n(), n(), n(), n()];
    check(
        ROWS,
        Opts {
            ephemeral: false,
            ..Opts::default()
        },
        &ops,
        &expected,
    );
}

#[test]
fn failed_seek_reads_null() {
    // SPEC G8 for index cursors: after a failed seek the cursor reads NULL.
    let ops = [SeekGE(0, p![9]), Observe(0), SeekLE(0, p![0]), Observe(0)];
    // `SeekLE [0]` finds (NULL,5,6): NULL is below 0, so it is not a miss.
    let expected = [j(), n(), n(), e(6, NULL, 5)];
    check_all(ROWS, 1, &ops, &expected);
    let ops = [
        SeekGT(0, p![3]),
        Observe(0),
        SeekLT(0, p![NULL]),
        Observe(0),
    ];
    let expected = [j(), n(), j(), n()];
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn seek_end_then_idx_insert_at_the_end() {
    // After `SeekEnd` the next `IdxInsert` is legal if the key is above every key.
    let mut ops = vec![
        SeekEnd(0),
        IdxInsert {
            cur: 0,
            a: v(9),
            b: v(9),
            rowid: 40,
            use_seek: false,
        },
        // The cursor position after this insert is `known_bug_d1_idx_insert_at_the_end_...`.
        Last(0),
        Observe(0),
    ];
    let mut expected = vec![n(), n(), n(), e(40, 9, 9)];
    ops.extend(scan_all());
    expected.extend(scan_expected(&[
        (6, NULL, v(5)),
        (1, v(1), v(10)),
        (2, v(1), v(20)),
        (3, v(2), v(10)),
        (4, v(2), v(30)),
        (5, v(3), NULL),
        (40, v(9), v(9)),
    ]));
    check_all(ROWS, 1, &ops, &expected);
}

#[test]
fn count_after_writes() {
    let ops = [
        Count(0),
        IdxInsert {
            cur: 1,
            a: v(5),
            b: v(5),
            rowid: 50,
            use_seek: false,
        },
        Count(0),
        IdxDelete {
            cur: 1,
            a: v(5),
            b: v(5),
            rowid: 50,
        },
        Count(0),
    ];
    let expected = [r(6), n(), r(7), n(), r(6)];
    check_all(ROWS, 2, &ops, &expected);
}

#[test]
fn clear_empties_the_index_in_wal_and_errors_in_mvcc() {
    // SPEC 2b: `Clear` is a WAL feature, MVCC returns an error. The observations before the
    // failing op are kept. In WAL every cursor on the tree is `Invalid` after it.
    let ops = [
        Rewind(0),
        Rewind(1),
        Observe(0),
        Clear(0),
        Observe(0),
        Observe(1),
        Count(0),
        Rewind(0),
    ];
    for (kind, layout) in CONFIGS {
        if kind == DbKind::Ephemeral {
            // `ClearBtree` takes a root page of the database, an ephemeral b-tree has none.
            continue;
        }
        for mode in ReadMode::ALL {
            let cfg = IndexRun {
                kind,
                layout,
                rows: ROWS,
                nidx: 2,
                ntbl: 0,
                mode,
            };
            let got = run_index(&cfg, &ops);
            let label = format!("{kind:?}/{layout:?}/{mode:?}: {got:?}");
            match kind {
                DbKind::Wal => assert_eq!(
                    got,
                    Outcome {
                        obs: vec![n(), n(), e(6, NULL, 5), n(), n(), n(), r(0), j()],
                        error: None
                    },
                    "{label}"
                ),
                DbKind::Mvcc => {
                    assert!(got.error.is_some(), "{label}");
                    assert_eq!(got.obs, vec![n(), n(), e(6, NULL, 5)], "{label}");
                }
                DbKind::Ephemeral => unreachable!(),
            }
        }
    }
}

#[test]
fn deferred_seek_reads_the_table_row() {
    // Entry (2,10,3) points to the table row 3 = (id 3, a 2, b 10).
    let ops = [
        SeekGE(0, p![2, 10]),
        DeferredSeek { idx: 0, tbl: 0 },
        TblColumn(0, 1),
        TblColumn(0, 2),
        TblColumn(0, 0),
        FinishSeek(0),
        // The row with a NULL: entry (NULL,5,6) points to the row 6 = (id 6, a NULL, b 5).
        Rewind(0),
        DeferredSeek { idx: 0, tbl: 0 },
        TblColumn(0, 1),
        TblColumn(0, 2),
        TblColumn(0, 0),
    ];
    let c0 = |x| IObs { c0: v(x), ..n() };
    let c1 = |x| IObs { c1: v(x), ..n() };
    let expected = [
        n(),
        n(),
        c0(2),
        c1(10),
        r(3),
        n(),
        n(),
        n(),
        // `a` is NULL.
        n(),
        c1(5),
        r(6),
    ];
    check(
        ROWS,
        Opts {
            ntbl: 1,
            modes: SAFE,
            ..Opts::default()
        },
        &ops,
        &expected,
    );
}

#[test]
fn deferred_seek_then_move_the_index() {
    // The table cursor follows the entry that the index cursor had at `DeferredSeek`.
    let ops = [
        SeekGE(0, p![1, 20]),
        DeferredSeek { idx: 0, tbl: 0 },
        Next(0),
        TblColumn(0, 2),
        FinishSeek(0),
        TblColumn(0, 0),
    ];
    let c1 = |x| IObs { c1: v(x), ..n() };
    // The index is on (2,10,3), the table cursor on row 2 = (1, 20).
    let expected = [n(), n(), j(), c1(20), n(), r(2)];
    check(
        ROWS,
        Opts {
            ntbl: 1,
            modes: SAFE,
            ..Opts::default()
        },
        &ops,
        &expected,
    );
}

// Everything below is a known gap between an engine and the contract (or between engines).
// Each `known_bug_*` test asserts what SPEC says. The test passes while the bug is present
// (the body must panic). When a bug is fixed, remove the `known_bug` wrapper.

const ROW_DATA: &[ReadMode] = &[ReadMode::RowData];
const ROW_DATA_RAW: &[ReadMode] = &[ReadMode::RowDataRaw];
const HAS_FIELD_AND_ROW_DATA: &[ReadMode] = &[ReadMode::ColumnHasField, ReadMode::RowData];

fn insert_key(a: i64, b: i64, rowid: i64) -> super::index_program::IOp {
    IdxInsert {
        cur: 0,
        a: v(a),
        b: v(b),
        rowid,
        use_seek: false,
    }
}

/// D1 for index cursors: after `IdxInsert` the cursor is on the new entry, so `Next` goes to
/// the entry after it. MVCC with the entries in the B-tree: `Next` does not jump and the
/// cursor reads nothing, as if the scan had ended. Same gap as the table test
/// `insert_then_next_from_new_row`. WAL, ephemeral and MVCC with entries in memory agree.
///
/// Fails today because of D1; see SPEC section 5b.
#[test]
fn known_bug_d1_idx_insert_then_next_from_new_entry() {
    known_bug("D1", "differ from the expected observations", || {
        let ops = [insert_key(2, 20, 8), Next(0), Observe(0)];
        let expected = [n(), j(), e(4, 2, 30)];
        check(
            ROWS,
            Opts {
                modes: SAFE,
                ..Opts::default()
            },
            &ops,
            &expected,
        );
    });
}

/// Same as above, with `Prev`.
///
/// Fails today because of D1; see SPEC section 5b.
#[test]
fn known_bug_d1_idx_insert_then_prev_from_new_entry() {
    known_bug("D1", "differ from the expected observations", || {
        let ops = [insert_key(2, 20, 8), Prev(0), Observe(0)];
        let expected = [n(), j(), e(3, 2, 10)];
        check(
            ROWS,
            Opts {
                modes: SAFE,
                ..Opts::default()
            },
            &ops,
            &expected,
        );
    });
}

/// D1: `IdxInsert` of a key that is above every key. The cursor must be on the new entry.
/// WAL and ephemeral: the cursor reads nothing (it looks like it is past the end). MVCC is
/// fine. An insert in the middle is fine in all engines.
///
/// Fails today because of D1; see SPEC section 5b.
#[test]
fn known_bug_d1_idx_insert_at_the_end_positions_the_cursor() {
    known_bug("D1", "differ from the expected observations", || {
        let ops = [insert_key(9, 9, 40), Observe(0)];
        let expected = [n(), e(40, 9, 9)];
        check(
            ROWS,
            Opts {
                modes: SAFE,
                ..Opts::default()
            },
            &ops,
            &expected,
        );
    });
}

/// The same after `SeekEnd`, the pattern of `CREATE INDEX`.
///
/// Fails today because of D1; see SPEC section 5b.
#[test]
fn known_bug_d1_idx_insert_after_seek_end_positions_the_cursor() {
    known_bug("D1", "differ from the expected observations", || {
        let ops = [SeekEnd(0), insert_key(9, 9, 40), Observe(0)];
        let expected = [n(), n(), e(40, 9, 9)];
        check(
            ROWS,
            Opts {
                modes: SAFE,
                ..Opts::default()
            },
            &ops,
            &expected,
        );
    });
}

/// SPEC 2b: `RowData` reads the same row as `Column`. After an `IdxInsert` in the middle of
/// the index, `Column` reads the new entry, but `RowData` reads the entry after it (WAL and
/// ephemeral; MVCC is fine). `RowData` is not guarded, with `RowDataRaw` the result is the same.
///
/// Fails today because of R1; see SPEC section 5b.
#[test]
fn known_bug_r1_rowdata_after_idx_insert_reads_the_new_entry() {
    known_bug("R1", "differ from the expected observations", || {
        let ops = [insert_key(1, 15, 7), Observe(0)];
        let expected = [n(), e(7, 1, 15)];
        for modes in [ROW_DATA, ROW_DATA_RAW] {
            check(
                ROWS,
                Opts {
                    modes,
                    ..Opts::default()
                },
                &ops,
                &expected,
            );
        }
    });
}

/// SPEC 1: a null-row cursor reads NULL. `RowData` returns the real row in WAL and in
/// ephemeral, and fails with "cursor has no record" in MVCC.
///
/// Fails today because of R1; see SPEC section 5b.
#[test]
fn known_bug_r1_rowdata_on_null_row_reads_null() {
    known_bug("R1", "differ from the expected observations", || {
        let ops = [Rewind(0), NullRow(0), Observe(0)];
        let expected = [n(), n(), n()];
        check(
            ROWS,
            Opts {
                modes: ROW_DATA_RAW,
                ..Opts::default()
            },
            &ops,
            &expected,
        );
    });
}

/// SPEC 1: a cursor without an entry reads NULL. `RowData` fails with "cursor has no
/// record" in every engine. SQLite asserts here, so this is probably by design: the `RowData`
/// read mode of the harness skips the read with a `ColumnHasField` guard.
///
/// Fails today because of R1; see SPEC section 5b.
#[test]
fn known_bug_r1_rowdata_on_cursor_without_entry_reads_null() {
    known_bug("R1", "cursor has no record", || {
        let ops = [
            Rewind(0),
            Next(0),
            Next(0),
            Next(0),
            Next(0),
            Next(0),
            Next(0),
            Observe(0),
        ];
        let expected = [n(), j(), j(), j(), j(), j(), n(), n()];
        check(
            ROWS,
            Opts {
                modes: ROW_DATA_RAW,
                ..Opts::default()
            },
            &ops,
            &expected,
        );
    });
}

/// G7 neighbour: `ColumnHasField` and `RowData` do not complete a pending `DeferredSeek`.
/// `Column` does. `ColumnHasField` sees a table cursor without a row and skips the read (WAL,
/// MVCC); in the ephemeral engine the table cursor has an old position and `RowData` reads
/// the wrong row.
///
/// Fails today because of R1; see SPEC section 5b.
#[test]
fn known_bug_r1_deferred_seek_with_has_field_and_row_data() {
    known_bug("R1", "differ from the expected observations", || {
        let ops = [
            SeekGE(0, p![2, 10]),
            DeferredSeek { idx: 0, tbl: 0 },
            TblColumn(0, 1),
            TblColumn(0, 2),
        ];
        let c0 = IObs { c0: v(2), ..n() };
        let c1 = IObs { c1: v(10), ..n() };
        let expected = [n(), n(), c0, c1];
        check(
            ROWS,
            Opts {
                ntbl: 1,
                modes: HAS_FIELD_AND_ROW_DATA,
                ..Opts::default()
            },
            &ops,
            &expected,
        );
    });
}

/// `OpenDup` does not support index cursors, so an ephemeral index cannot have a peer cursor
/// (the table variant works). The harness skips ephemeral runs with more than one index cursor.
///
/// Fails today because of O1; see SPEC section 5b.
#[test]
fn known_bug_o1_ephemeral_index_peer_cursor() {
    known_bug("O1", "OpenDup is not supported", || {
        let ops = [Rewind(0), Rewind(1), Observe(1)];
        let cfg = IndexRun {
            kind: DbKind::Ephemeral,
            layout: Layout::AllInMemory,
            rows: ROWS,
            nidx: 2,
            ntbl: 0,
            mode: ReadMode::Column,
        };
        let got = run_index(&cfg, &ops);
        assert_eq!(got.error, None);
        assert_eq!(got.obs, vec![n(), n(), e(6, NULL, 5)]);
    });
}
