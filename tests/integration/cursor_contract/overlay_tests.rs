//! Tests for the bug candidates of the dual cursor, for tables and for indexes. Each test builds a state with
//! rows in the B-tree (checkpointed setup rows) and rows in the MVCC overlay (writes of a
//! peer cursor in the same transaction), then reads the tree with a second cursor. The
//! expected sequence is the flat view of the store, which is what WAL gives. In WAL
//! everything is in the B-tree, so WAL is the oracle.

use super::index_program::{run_index, with_eq_only_seek_ge, IObs, IOp, IndexRun, Row};
use super::program::{Obs, ReadMode};
use super::{known_bug, run, DbKind, Layout};
use IOp::{IdxInsert, Next as INext, Observe, Rewind as IRewind, SeekGE as ISeekGE};

/// The engines under test. The layout is `Checkpointed` for all of them.
const ENGINES: [DbKind; 2] = [DbKind::Wal, DbKind::Mvcc];

/// Collects the failures of every engine, so that one test run shows all of them.
fn assert_no_failures(failures: Vec<String>) {
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

mod table {
    use super::super::program::Op::{self, *};
    use super::*;

    /// B-tree rows (checkpointed): 1, 3, 5, 7.
    const BTREE: &[(i64, i64)] = &[(1, 10), (3, 30), (5, 50), (7, 70)];

    fn insert(rowid: i64, value: i64) -> Op {
        Insert {
            cur: 1,
            rowid,
            value,
            require_seek: true,
        }
    }

    /// The overlay of the table tests: row 3 is updated and row 4 is new. The flat view is
    /// 1, 3, 4, 5, 7.
    fn overlay_writes() -> Vec<Op> {
        vec![insert(3, 33), insert(4, 44)]
    }

    /// The rowids that the `RowId` ops of the program observed, in program order.
    fn rowids(ops: &[Op], got: &[Obs]) -> Vec<Option<i64>> {
        ops.iter()
            .zip(got)
            .filter(|(op, _)| matches!(op, RowId(_)))
            .map(|(_, obs)| obs.rowid)
            .collect()
    }

    /// Runs `ops` on every engine with the B-tree rows and asserts that the `RowId` reads on
    /// cursor 0 give `expected`, and that every `Next` found a row.
    #[track_caller]
    fn check(name: &str, ops: &[Op], expected: &[i64]) {
        let expected: Vec<Option<i64>> = expected.iter().copied().map(Some).collect();
        let mut failures = Vec::new();
        for kind in ENGINES {
            let got = match run(kind, Layout::Checkpointed, BTREE, 2, ops) {
                Ok(got) => got,
                Err(e) => {
                    failures.push(format!("{name} {kind:?}: engine error {e}"));
                    continue;
                }
            };
            let ids = rowids(ops, &got);
            let all_found = ops
                .iter()
                .zip(&got)
                .filter(|(op, _)| matches!(op, Next(0)))
                .all(|(_, obs)| obs.jumped);
            // The first `RowId` is the one after the seek: it is not a `Next` result.
            if ids[1..] != expected[..] || !all_found {
                failures.push(format!(
                    "{name} {kind:?}: expected rowids after the seek {expected:?}, \
                     got {:?}, every Next found a row: {all_found}\nobservations: {got:?}",
                    &ids[1..]
                ));
            }
        }
        assert_no_failures(failures);
    }

    fn steps(n: usize) -> Vec<Op> {
        (0..n).flat_map(|_| [Next(0), RowId(0)]).collect()
    }

    /// T1: the overlay hit (row 3 is in both stores). `Next` must go to 4, 5, 7.
    /// Fails today because of N1; see SPEC section 5b.
    #[test]
    fn known_bug_n1_t1_seek_rowid_overlay_hit_then_next() {
        known_bug("N1", "T1 Mvcc: expected rowids", || {
            let mut ops = overlay_writes();
            ops.extend([SeekRowid(0, 3), RowId(0)]);
            ops.extend(steps(3));
            check("T1", &ops, &[4, 5, 7]);
        });
    }

    /// T2: the B-tree hit (row 1 is only in the B-tree). `Next` must go to 3, 4, 5.
    /// Fails today because of N1; see SPEC section 5b.
    #[test]
    fn known_bug_n1_t2_seek_rowid_btree_hit_then_next() {
        known_bug("N1", "T2 Mvcc: expected rowids", || {
            let mut ops = overlay_writes();
            ops.extend([SeekRowid(0, 1), RowId(0)]);
            ops.extend(steps(3));
            check("T2", &ops, &[3, 4, 5]);
        });
    }

    /// T3: `NotExists` is the other opcode of the "hit then Next" contract of SPEC section 2.
    /// The hit is in the overlay (row 3). `Next` must go to 4, 5, 7.
    /// Fails today because of N1; see SPEC section 5b.
    #[test]
    fn known_bug_n1_t3_not_exists_overlay_hit_then_next() {
        known_bug("N1", "T3 overlay hit Mvcc: expected rowids", || {
            let mut ops = overlay_writes();
            ops.extend([NotExists(0, 3), RowId(0)]);
            ops.extend(steps(3));
            check("T3 overlay hit", &ops, &[4, 5, 7]);
        });
    }

    /// T3, the hit is in the B-tree (row 1). `Next` must go to 3, 4, 5.
    /// Fails today because of N1; see SPEC section 5b.
    #[test]
    fn known_bug_n1_t3_not_exists_btree_hit_then_next() {
        known_bug("N1", "T3 B-tree hit Mvcc: expected rowids", || {
            let mut ops = overlay_writes();
            ops.extend([NotExists(0, 1), RowId(0)]);
            ops.extend(steps(3));
            check("T3 B-tree hit", &ops, &[3, 4, 5]);
        });
    }

    /// T4: a peer cursor inserts row 9 after the seek. `Next` must see it at the end.
    /// Fails today because of N1; see SPEC section 5b.
    #[test]
    fn known_bug_n1_t4_seek_rowid_hit_peer_write_then_next() {
        known_bug("N1", "T4 Mvcc: expected rowids", || {
            let mut ops = overlay_writes();
            ops.extend([SeekRowid(0, 3), RowId(0), insert(9, 99)]);
            ops.extend(steps(4));
            check("T4", &ops, &[4, 5, 7, 9]);
        });
    }
}

mod index {
    use super::super::index_model::Val;
    use super::*;

    const fn v(x: i64) -> Val {
        Val::Int(x)
    }

    /// A shorter name for the expected entry of `Observe`: `(a, b, rowid)`.
    fn e(a: i64, b: i64, rowid: i64) -> (Val, Val, Option<i64>) {
        (v(a), v(b), Some(rowid))
    }

    type Entry = (Val, Val, Option<i64>);

    fn entry(obs: &IObs) -> Entry {
        (obs.c0.clone(), obs.c1.clone(), obs.rowid)
    }

    fn insert_key(a: i64, b: i64, rowid: i64, use_seek: bool) -> IOp {
        IdxInsert {
            cur: 1,
            a: v(a),
            b: v(b),
            rowid,
            use_seek,
        }
    }

    /// `Next` and `Observe`, `n` times.
    fn steps(n: usize) -> Vec<IOp> {
        (0..n).flat_map(|_| [INext(0), Observe(0)]).collect()
    }

    /// Runs `ops` on every engine with the B-tree rows `rows`. Asserts that the `Observe`
    /// ops give the entries of `expected` (an observe on a missing entry gives NULLs) and
    /// that every `Next` found a row, but the last ones if `expected` is shorter.
    #[track_caller]
    fn check(name: &str, rows: &[Row], eq_only: bool, ops: &[IOp], expected: &[Entry]) {
        let mut failures = Vec::new();
        for kind in ENGINES {
            let cfg = IndexRun {
                kind,
                layout: Layout::Checkpointed,
                rows,
                nidx: 2,
                ntbl: 0,
                mode: ReadMode::Column,
            };
            let outcome = if eq_only {
                with_eq_only_seek_ge(|| run_index(&cfg, ops))
            } else {
                run_index(&cfg, ops)
            };
            let seen: Vec<Entry> = ops
                .iter()
                .zip(&outcome.obs)
                .filter(|(op, _)| matches!(op, Observe(0)))
                .map(|(_, obs)| entry(obs))
                .collect();
            if seen != expected || outcome.error.is_some() {
                failures.push(format!(
                    "{name} {kind:?} (eq_only {eq_only}): expected (a, b, rowid) {expected:?}, \
                     got {seen:?}, error {:?}",
                    outcome.error
                ));
            }
        }
        assert_no_failures(failures);
    }

    /// The cluster of the index tests: B-tree entries (1,1,4) and (3,1,5), overlay
    /// entries (1,2,5) and (2,5,1).
    fn cluster_rows() -> Vec<Row> {
        vec![(4, v(1), v(1)), (5, v(3), v(1))]
    }

    fn cluster_writes() -> Vec<IOp> {
        vec![insert_key(1, 2, 5, false), insert_key(2, 5, 1, false)]
    }

    fn seek_and_scan(prefix: Vec<Val>) -> Vec<IOp> {
        let mut ops = cluster_writes();
        ops.extend([ISeekGE(0, prefix), Observe(0)]);
        ops.extend(steps(3));
        ops
    }

    const FLAT: [Entry; 4] = [
        (v(1), v(1), Some(4)),
        (v(1), v(2), Some(5)),
        (v(2), v(5), Some(1)),
        (v(3), v(1), Some(5)),
    ];

    /// I1, plain `SeekGE` on the prefix [1], then `Next` three times. Control for I1 eq-only.
    #[test]
    fn i1_seek_ge_prefix_then_next() {
        check(
            "I1 plain",
            &cluster_rows(),
            false,
            &seek_and_scan(vec![v(1)]),
            &FLAT,
        );
    }

    /// I1, the eq-only `SeekGE` that the translator emits in front of an `IdxGT` loop.
    /// `Next` must reach the overlay key (2,5,1). A range loop stops at the first key above
    /// the prefix, so the first three entries of the flat view are the ones that matter.
    /// Fails today because of OV1; see SPEC section 5b.
    #[test]
    fn known_bug_ov1_i1_eq_only_seek_ge_prefix_then_next() {
        known_bug("OV1", "I1 eq-only Mvcc", || {
            check(
                "I1 eq-only",
                &cluster_rows(),
                true,
                &seek_and_scan(vec![v(1)]),
                &FLAT,
            );
        });
    }

    /// I2: the B-tree has only (3,1,5) and the overlay has only (1,2,5). The eq-only seek on
    /// the prefix [1] finds the overlay entry, then `Next` must go to the B-tree entry.
    /// Fails today because of OV1; see SPEC section 5b.
    #[test]
    fn known_bug_ov1_i2_eq_only_seek_ge_overlay_only_then_next() {
        known_bug("OV1", "I2 eq-only Mvcc", || {
            let rows = vec![(5, v(3), v(1))];
            let ops = [
                insert_key(1, 2, 5, false),
                ISeekGE(0, vec![v(1)]),
                Observe(0),
                INext(0),
                Observe(0),
            ];
            check("I2 eq-only", &rows, true, &ops, &[e(1, 2, 5), e(3, 1, 5)]);
            // The same without eq-only.
            check("I2 plain", &rows, false, &ops, &[e(1, 2, 5), e(3, 1, 5)]);
        });
    }

    /// I3 on the key (a, b): `Found` on the prefix [a, b] hits, then `IdxInsert` of
    /// (a, b, 9) with `use_seek` (SPEC 2b: the token of a hit is the prefix, so this is
    /// legal). A scan from the start must show the new key in key order. The B-tree has
    /// (1,1,4) and (3,1,5), the overlay has (1,2,5).
    #[track_caller]
    fn found_hit_then_insert(name: &str, a: i64, b: i64, expected: &[Entry]) {
        let mut ops = vec![
            insert_key(1, 2, 5, false),
            IOp::Found(0, vec![v(a), v(b)]),
            IdxInsert {
                cur: 0,
                a: v(a),
                b: v(b),
                rowid: 9,
                use_seek: true,
            },
            IRewind(0),
            Observe(0),
        ];
        ops.extend(steps(3));
        check(name, &cluster_rows(), false, &ops, expected);
    }

    /// I3, the hit is the overlay entry (1,2,5). WAL (the oracle) puts (1,2,9) before
    /// (1,2,5): the insert goes to the position of the hit and not to the place of its key,
    /// so the tree is out of key order. MVCC gives the right order. The translator only
    /// uses `use_seek` after a miss. The expected order is the key order.
    /// Fails today because of N11; see SPEC section 5b.
    #[test]
    fn known_bug_n11_found_overlay_hit_then_idx_insert_use_seek() {
        known_bug("N11", "I3 overlay hit Wal", || {
            found_hit_then_insert(
                "I3 overlay hit",
                1,
                2,
                &[e(1, 1, 4), e(1, 2, 5), e(1, 2, 9), e(3, 1, 5)],
            );
        });
    }

    /// I3, the hit is the B-tree entry (1,1,4). The new key (1,1,9) must come after it.
    /// Fails today because of N11; see SPEC section 5b.
    #[test]
    fn known_bug_n11_found_btree_hit_then_idx_insert_use_seek() {
        known_bug("N11", "I3 B-tree hit Wal", || {
            found_hit_then_insert(
                "I3 B-tree hit",
                1,
                1,
                &[e(1, 1, 4), e(1, 1, 9), e(1, 2, 5), e(3, 1, 5)],
            );
        });
    }
}
