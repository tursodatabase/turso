//! Regression tests for the Turso scan fixes (see Cargo.toml header).
//!
//! Both bugs were found while porting Turso's MVCC row maps onto this crate:
//! 16-byte keys sharing an 8-byte prefix build a sublayer, and scans over
//! that shape were wrong in two ways upstream (v0.9.5).

use turso_masstree::{MassTree15, RangeBound};

fn key16(prefix: i64, suffix: u64) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&(((prefix as u64) ^ (1u64 << 63)).to_be_bytes()));
    bytes[8..].copy_from_slice(&suffix.to_be_bytes());
    bytes
}

fn suffix_of(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[8..16].try_into().unwrap())
}

/// Reverse scans must keep serving the keys to the LEFT of a leaf that a
/// remove emptied. Upstream returned "layer exhausted" when the reverse
/// descent landed on an emptied rightmost leaf, hiding every live key of
/// the layer.
#[test]
fn reverse_scan_survives_emptied_rightmost_leaf() {
    let tree: MassTree15<u64> = MassTree15::new();
    let guard = tree.guard();
    // 16 keys: one leaf split (leaves are 15 wide), then empty the right leaf.
    for i in 0..16u64 {
        tree.insert_with_guard(&key16(-3, i), i, &guard);
    }
    tree.remove_with_guard(&key16(-3, 15), &guard).unwrap();

    let mut seen = Vec::new();
    tree.scan_rev_batch(
        RangeBound::Unbounded,
        RangeBound::Unbounded,
        |key, _value| {
            seen.push(suffix_of(key));
            true
        },
        &guard,
    );
    assert_eq!(seen, (0..15).rev().collect::<Vec<_>>());
}

/// Same shape, but with the whole layer emptied and one key re-inserted:
/// the state the Turso MVCC store reaches after GC unlinks every chain of a
/// table and a new write recreates one.
#[test]
fn reverse_scan_sees_reinserted_key_after_layer_emptied() {
    let tree: MassTree15<u64> = MassTree15::new();
    let guard = tree.guard();
    tree.insert_with_guard(&key16(-2, 1), 22, &guard);
    for i in 0..1000u64 {
        tree.insert_with_guard(&key16(-3, i), i, &guard);
    }
    for i in 0..1000u64 {
        tree.remove_with_guard(&key16(-3, i), &guard).unwrap();
    }
    tree.insert_with_guard(&key16(-3, 1), 33, &guard);

    let mut seen = Vec::new();
    tree.scan_rev_batch(
        RangeBound::Unbounded,
        RangeBound::Unbounded,
        |key, value| {
            seen.push((suffix_of(key), *value));
            true
        },
        &guard,
    );
    assert_eq!(seen, vec![(1, 22), (1, 33)]);
}

/// 8-byte keys, same emptied-rightmost-leaf shape, exercised through the
/// single-layer fast path.
#[test]
fn reverse_scan_survives_emptied_rightmost_leaf_single_layer() {
    let tree: MassTree15<u64> = MassTree15::new();
    let guard = tree.guard();
    for i in 0..16u64 {
        tree.insert_with_guard(&i.to_be_bytes(), i, &guard);
    }
    tree.remove_with_guard(&15u64.to_be_bytes(), &guard)
        .unwrap();

    let mut seen = Vec::new();
    tree.scan_rev_batch(
        RangeBound::Unbounded,
        RangeBound::Unbounded,
        |key, _value| {
            seen.push(u64::from_be_bytes(key.try_into().unwrap()));
            true
        },
        &guard,
    );
    assert_eq!(seen, (0..15).rev().collect::<Vec<_>>());
}

/// A forward scan whose `Included` start key sits past the first leaf of a
/// sublayer must still begin AT that key. Upstream skipped it (behaved like
/// `Excluded`), which broke every eq-only point seek in Turso's MVCC store.
#[test]
fn forward_scan_included_start_is_exact_in_sublayers() {
    let tree: MassTree15<u64> = MassTree15::new();
    let guard = tree.guard();
    for i in 0..60u64 {
        tree.insert_with_guard(&key16(-3, i), i, &guard);
    }
    for i in 0..60u64 {
        let key = key16(-3, i);
        let mut first = None;
        tree.scan(
            RangeBound::Included(&key),
            RangeBound::Unbounded,
            |hit, _value| {
                first = Some(suffix_of(hit));
                false
            },
            &guard,
        );
        assert_eq!(first, Some(i), "Included start must begin at key {i}");

        let mut singleton = Vec::new();
        tree.scan(
            RangeBound::Included(&key),
            RangeBound::Included(&key),
            |hit, _value| {
                singleton.push(suffix_of(hit));
                true
            },
            &guard,
        );
        assert_eq!(singleton, vec![i], "singleton range at key {i}");
    }
}

/// Forward `Excluded` starts and reverse seeks were already exact; pin that.
#[test]
fn forward_excluded_and_reverse_seeks_stay_exact() {
    let tree: MassTree15<u64> = MassTree15::new();
    let guard = tree.guard();
    for i in 0..60u64 {
        tree.insert_with_guard(&key16(-3, i), i, &guard);
    }
    for i in 0..59u64 {
        let key = key16(-3, i);
        let mut first = None;
        tree.scan(
            RangeBound::Excluded(&key),
            RangeBound::Unbounded,
            |hit, _value| {
                first = Some(suffix_of(hit));
                false
            },
            &guard,
        );
        assert_eq!(first, Some(i + 1), "Excluded start after key {i}");

        let mut last = None;
        tree.scan_rev_batch(
            RangeBound::Unbounded,
            RangeBound::Included(&key),
            |hit, _value| {
                last = Some(suffix_of(hit));
                false
            },
            &guard,
        );
        assert_eq!(last, Some(i), "reverse Included end at key {i}");
    }
}
