use super::*;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicI64;
use std::sync::Barrier;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct IntKey(i64);

impl KeyPrefix for IntKey {}

impl KeyPrefix for i64 {}

unsafe impl TreeKey for IntKey {
    type Slot = AtomicI64;

    const NEEDS_DEFERRED_DROP: bool = false;

    fn write(slot: &Self::Slot, key: Self) {
        slot.store(key.0, Ordering::Release);
    }

    fn move_from(slot: &Self::Slot, from: &Self::Slot) {
        slot.store(from.load(Ordering::Acquire), Ordering::Release);
    }

    fn read<R>(slot: &Self::Slot, f: impl FnOnce(&Self) -> R) -> Option<R> {
        Some(f(&IntKey(slot.load(Ordering::Acquire))))
    }

    fn take(slot: &Self::Slot) -> Self {
        IntKey(slot.load(Ordering::Acquire))
    }

    fn clear(_slot: &Self::Slot) {}
}

type IntMap = BPlusTreeMap<IntKey, Arc<i64>>;
type ArcMap = BPlusTreeMap<Arc<i64>, Arc<i64>>;

fn collect_int(map: &IntMap) -> Vec<(i64, i64)> {
    map.iter().map(|e| (e.key().0, **e.value())).collect()
}

fn random_bound(rng: &mut impl Rng, max: i64) -> Bound<IntKey> {
    match rng.random_range(0..3) {
        0 => Bound::Unbounded,
        1 => Bound::Included(IntKey(rng.random_range(-5..max + 5))),
        _ => Bound::Excluded(IntKey(rng.random_range(-5..max + 5))),
    }
}

fn check_range(map: &IntMap, model: &BTreeMap<i64, i64>, lo: Bound<IntKey>, hi: Bound<IntKey>) {
    let lo_model = lo.map(|k| k.0);
    let hi_model = hi.map(|k| k.0);
    let valid = match (lo_model, hi_model) {
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
            a < b
                || (a == b
                    && matches!(
                        (lo_model, hi_model),
                        (Bound::Included(_), Bound::Included(_))
                    ))
        }
        _ => true,
    };
    if !valid {
        return;
    }
    let expected: Vec<(i64, i64)> = model
        .range((lo_model, hi_model))
        .map(|(k, v)| (*k, *v))
        .collect();
    let forward: Vec<(i64, i64)> = map
        .range((lo, hi))
        .map(|e| (e.key().0, **e.value()))
        .collect();
    assert_eq!(forward, expected, "forward range {lo:?}..{hi:?}");
    let mut backward: Vec<(i64, i64)> = map
        .range((lo, hi))
        .rev()
        .map(|e| (e.key().0, **e.value()))
        .collect();
    backward.reverse();
    assert_eq!(backward, expected, "backward range {lo:?}..{hi:?}");
    let mut iter = map.range((lo, hi));
    let mut front = Vec::new();
    let mut back = Vec::new();
    let mut take_front = true;
    loop {
        let next = if take_front {
            iter.next()
        } else {
            iter.next_back()
        };
        let Some(entry) = next else { break };
        if take_front {
            front.push((entry.key().0, **entry.value()));
        } else {
            back.push((entry.key().0, **entry.value()));
        }
        take_front = !take_front;
    }
    back.reverse();
    front.extend(back);
    assert_eq!(front, expected, "two-ended range {lo:?}..{hi:?}");
}

#[test]
fn matches_btreemap_under_random_operations() {
    for seed in 0..20u64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let map = IntMap::new();
        let mut model = BTreeMap::new();
        let max = if seed % 2 == 0 { 300 } else { 5000 };
        for step in 0..20_000 {
            let k = rng.random_range(0..max);
            match rng.random_range(0..10) {
                0..=4 => {
                    let v = rng.random_range(0..1_000_000);
                    let entry = map.get_or_insert_with(IntKey(k), || Arc::new(v));
                    let expected = *model.entry(k).or_insert(v);
                    assert_eq!(**entry.value(), expected);
                }
                5..=6 => {
                    let removed = map.remove(&IntKey(k)).map(|e| **e.value());
                    assert_eq!(removed, model.remove(&k));
                }
                7..=8 => {
                    let got = map.get(&IntKey(k)).map(|e| **e.value());
                    assert_eq!(got, model.get(&k).copied());
                }
                _ => {
                    let lo = random_bound(&mut rng, max);
                    let hi = random_bound(&mut rng, max);
                    check_range(&map, &model, lo, hi);
                }
            }
            if step % 5000 == 0 {
                assert_eq!(
                    collect_int(&map),
                    model.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>()
                );
            }
        }
        assert_eq!(map.len(), model.len());
        assert_eq!(
            collect_int(&map),
            model.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>()
        );
        assert_eq!(map.front().map(|e| e.key().0), model.keys().next().copied());
        assert_eq!(
            map.back().map(|e| e.key().0),
            model.keys().next_back().copied()
        );
    }
}

#[test]
fn sequential_inserts_fill_leaves() {
    let map = IntMap::new();
    for i in 0..100_000 {
        map.insert(IntKey(i), Arc::new(i));
    }
    assert_eq!(map.len(), 100_000);
    let keys: Vec<i64> = map.iter().map(|e| e.key().0).collect();
    assert_eq!(keys, (0..100_000).collect::<Vec<_>>());
    let keys: Vec<i64> = map.iter().rev().map(|e| e.key().0).collect();
    assert_eq!(keys, (0..100_000).rev().collect::<Vec<_>>());
}

#[test]
fn seeks_skip_leaves_that_became_empty() {
    let map = IntMap::new();
    for i in 0..10_000 {
        map.insert(IntKey(i), Arc::new(i));
    }
    for i in 100..9_900 {
        assert!(map.remove(&IntKey(i)).is_some());
    }
    let keys: Vec<i64> = map.iter().map(|e| e.key().0).collect();
    let expected: Vec<i64> = (0..100).chain(9_900..10_000).collect();
    assert_eq!(keys, expected);
    let keys: Vec<i64> = map.iter().rev().map(|e| e.key().0).collect();
    assert_eq!(keys, expected.iter().rev().copied().collect::<Vec<_>>());
    assert_eq!(
        map.lower_bound(Bound::Included(&IntKey(500)))
            .map(|e| e.key().0),
        Some(9_900)
    );
    assert_eq!(
        map.upper_bound(Bound::Included(&IntKey(500)))
            .map(|e| e.key().0),
        Some(99)
    );
    assert_eq!(
        map.upper_bound(Bound::Excluded(&IntKey(9_900)))
            .map(|e| e.key().0),
        Some(99)
    );
    assert_eq!(
        map.lower_bound(Bound::Excluded(&IntKey(99)))
            .map(|e| e.key().0),
        Some(9_900)
    );
}

#[test]
fn iterator_sees_inserts_ahead_of_it() {
    let map = IntMap::new();
    for i in (0..1000).step_by(2) {
        map.insert(IntKey(i), Arc::new(i));
    }
    let mut seen = Vec::new();
    for entry in map.iter() {
        let k = entry.key().0;
        seen.push(k);
        if k % 2 == 0 && k < 998 {
            map.insert(IntKey(k + 1), Arc::new(k + 1));
        }
    }
    assert_eq!(seen, (0..999).collect::<Vec<_>>());
}

#[test]
fn entry_remove_only_removes_the_same_value() {
    let map = IntMap::new();
    let first = map.insert(IntKey(1), Arc::new(10));
    assert!(first.remove());
    assert!(first.is_removed());
    let second = map.insert(IntKey(1), Arc::new(20));
    assert!(!first.remove());
    assert!(!second.is_removed());
    assert_eq!(map.get(&IntKey(1)).map(|e| **e.value()), Some(20));
    assert!(second.remove());
    assert!(map.is_empty());
}

#[test]
fn arc_keys_can_be_found_by_borrowed_keys() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(3);
    let map = ArcMap::new();
    let mut model = BTreeMap::new();
    for _ in 0..50_000 {
        let k = rng.random_range(0..3000i64);
        if rng.random_bool(0.7) {
            map.get_or_insert_with(Arc::new(k), || Arc::new(k * 2));
            model.insert(k, k * 2);
        } else {
            assert_eq!(map.remove(&k).is_some(), model.remove(&k).is_some());
        }
    }
    for k in 0..3000i64 {
        assert_eq!(map.get(&k).map(|e| **e.value()), model.get(&k).copied());
    }
    let got: Vec<i64> = map.range(100i64..=2000).map(|e| **e.key()).collect();
    let expected: Vec<i64> = model.range(100..=2000).map(|(k, _)| *k).collect();
    assert_eq!(got, expected);
}

#[test]
fn values_are_dropped() {
    let value = Arc::new(7i64);
    {
        let map = ArcMap::new();
        for i in 0..5000 {
            map.insert(Arc::new(i), value.clone());
        }
        for i in 0..2500 {
            map.remove(&i);
        }
    }
    for _ in 0..64 {
        epoch::pin().flush();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while Arc::strong_count(&value) != 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "values were not dropped"
        );
        epoch::pin().flush();
    }
}

#[test]
fn concurrent_inserts_of_the_same_keys_agree() {
    let map = Arc::new(IntMap::new());
    let threads = 4;
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let map = map.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let mut got = Vec::new();
                for i in 0..20_000 {
                    let entry = map.get_or_insert_with(IntKey(i), || Arc::new(t as i64));
                    got.push(**entry.value());
                }
                got
            })
        })
        .collect();
    let results: Vec<Vec<i64>> = handles
        .into_iter()
        .map(|h| h.join().expect("thread"))
        .collect();
    for i in 0..20_000usize {
        let winner = results[0][i];
        assert!(results.iter().all(|r| r[i] == winner));
        assert_eq!(
            map.get(&IntKey(i as i64)).map(|e| **e.value()),
            Some(winner)
        );
    }
    assert_eq!(map.len(), 20_000);
}

#[test]
fn concurrent_writers_and_scanners_keep_order() {
    let map = Arc::new(IntMap::new());
    for i in 0..10_000 {
        map.insert(IntKey(i * 4), Arc::new(i * 4));
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..2)
        .map(|t| {
            let map = map.clone();
            std::thread::spawn(move || {
                let mut rng = rand::rngs::StdRng::seed_from_u64(t);
                for _ in 0..200_000 {
                    let k = rng.random_range(0..10_000) * 4 + 1 + t as i64;
                    if rng.random_bool(0.5) {
                        map.insert(IntKey(k), Arc::new(k));
                    } else {
                        map.remove(&IntKey(k));
                    }
                }
            })
        })
        .collect();
    let scanners: Vec<_> = (0..2)
        .map(|t| {
            let map = map.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut rng = rand::rngs::StdRng::seed_from_u64(100 + t);
                let mut scans = 0;
                while !stop.load(Ordering::Relaxed) || scans < 10 {
                    let forward = rng.random_bool(0.5);
                    let keys: Vec<i64> = if forward {
                        map.iter().map(|e| e.key().0).collect()
                    } else {
                        map.iter().rev().map(|e| e.key().0).collect()
                    };
                    let stable = keys.iter().filter(|k| *k % 4 == 0).count();
                    assert_eq!(stable, 10_000, "a scan missed a key that was always there");
                    for w in keys.windows(2) {
                        if forward {
                            assert!(w[0] < w[1], "forward scan out of order");
                        } else {
                            assert!(w[0] > w[1], "backward scan out of order");
                        }
                    }
                    for e in keys.iter().filter(|k| *k % 4 != 0) {
                        assert!(*e % 4 == 1 || *e % 4 == 2);
                    }
                    let probe = rng.random_range(0..10_000) * 4;
                    assert_eq!(map.get(&IntKey(probe)).map(|e| **e.value()), Some(probe));
                    scans += 1;
                }
            })
        })
        .collect();
    for w in writers {
        w.join().expect("writer");
    }
    stop.store(true, Ordering::Relaxed);
    for s in scanners {
        s.join().expect("scanner");
    }
    let keys: Vec<i64> = map.iter().map(|e| e.key().0).collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(keys.len(), map.len());
}

#[test]
fn a_split_before_the_leaf_read_does_not_hide_keys() {
    let map = map_with_a_full_first_leaf();
    split_the_first_leaf_before_the_next_child_version_read(&map);
    assert_eq!(map.get(&IntKey(620)).map(|e| **e.value()), Some(620));

    let map = map_with_a_full_first_leaf();
    split_the_first_leaf_before_the_next_child_version_read(&map);
    assert_eq!(map.remove(&IntKey(620)).map(|e| **e.value()), Some(620));

    let map = map_with_a_full_first_leaf();
    split_the_first_leaf_before_the_next_child_version_read(&map);
    let last = map.range(..=IntKey(620)).next_back().map(|e| e.key().0);
    assert_eq!(last, Some(620));
}

fn map_with_a_full_first_leaf() -> Arc<IntMap> {
    let map = Arc::new(IntMap::new());
    for i in 0..200 {
        map.insert(IntKey(i * 10), Arc::new(i * 10));
    }
    map.insert(IntKey(5), Arc::new(5));
    map
}

fn split_the_first_leaf_before_the_next_child_version_read(map: &Arc<IntMap>) {
    let map = map.clone();
    BEFORE_CHILD_VERSION_READ.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            map.insert(IntKey(15), Arc::new(15));
        }));
    });
}

thread_local! {
    static BEFORE_CHILD_VERSION_READ: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn before_child_version_read() {
    if let Some(hook) = BEFORE_CHILD_VERSION_READ.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
}

#[test]
fn concurrent_disjoint_writers_end_with_their_keys() {
    let map = Arc::new(ArcMap::new());
    let threads = 4;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let map = map.clone();
            std::thread::spawn(move || {
                let mut rng = rand::rngs::StdRng::seed_from_u64(t);
                let mut model = BTreeMap::new();
                for _ in 0..100_000 {
                    let k = rng.random_range(0..5_000i64) * threads as i64 + t as i64;
                    if rng.random_bool(0.6) {
                        map.get_or_insert_with(Arc::new(k), || Arc::new(k));
                        model.insert(k, k);
                    } else {
                        assert_eq!(map.remove(&k).is_some(), model.remove(&k).is_some());
                    }
                    if rng.random_range(0..1000) == 0 {
                        let lo = rng.random_range(0..5_000i64) * threads as i64;
                        let mine: Vec<i64> = map
                            .range(lo..lo + 400)
                            .map(|e| **e.key())
                            .filter(|k| k.rem_euclid(threads as i64) == t as i64)
                            .collect();
                        let expected: Vec<i64> =
                            model.range(lo..lo + 400).map(|(k, _)| *k).collect();
                        assert_eq!(mine, expected);
                    }
                }
                model
            })
        })
        .collect();
    let mut all = BTreeMap::new();
    for h in handles {
        all.extend(h.join().expect("thread"));
    }
    let keys: Vec<i64> = map.iter().map(|e| **e.key()).collect();
    assert_eq!(keys, all.keys().copied().collect::<Vec<_>>());
}

#[test]
fn each_step_returns_the_current_successor() {
    for seed in 0..10u64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let map = IntMap::new();
        let mut model = BTreeMap::new();
        for _ in 0..3000 {
            let k = rng.random_range(0..4000i64);
            map.insert(IntKey(k), Arc::new(k));
            model.insert(k, k);
        }
        let forward = seed % 2 == 0;
        let mut iter = map.iter();
        let mut last: Option<i64> = None;
        loop {
            let got = if forward {
                iter.next()
            } else {
                iter.next_back()
            };
            let expected = match (last, forward) {
                (None, true) => model.keys().next().copied(),
                (None, false) => model.keys().next_back().copied(),
                (Some(l), true) => model.range(l + 1..).next().map(|(k, _)| *k),
                (Some(l), false) => model.range(..l).next_back().map(|(k, _)| *k),
            };
            assert_eq!(got.as_ref().map(|e| e.key().0), expected, "seed {seed}");
            let Some(entry) = got else { break };
            last = Some(entry.key().0);
            for _ in 0..rng.random_range(0..4) {
                let k = rng.random_range(0..4000i64);
                if rng.random_bool(0.5) {
                    map.insert(IntKey(k), Arc::new(k));
                    model.insert(k, k);
                } else {
                    map.remove(&IntKey(k));
                    model.remove(&k);
                }
            }
        }
    }
}

#[test]
fn insert_replaces_and_get_or_insert_keeps() {
    let map = IntMap::new();
    map.insert(IntKey(1), Arc::new(10));
    assert_eq!(
        **map.get_or_insert_with(IntKey(1), || Arc::new(20)).value(),
        10
    );
    assert_eq!(**map.insert(IntKey(1), Arc::new(30)).value(), 30);
    assert_eq!(map.get(&IntKey(1)).map(|e| **e.value()), Some(30));
    assert_eq!(map.len(), 1);
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CoarseKey(i64);

impl KeyPrefix for CoarseKey {
    fn prefix(&self) -> Option<u64> {
        Some(((self.0 >> 4) as u64) ^ (1 << 63))
    }
}

#[test]
fn prefixes_with_many_ties_match_btreemap() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(21);
    let map: BPlusTreeMap<Arc<CoarseKey>, Arc<i64>> = BPlusTreeMap::new();
    let mut model = BTreeMap::new();
    for _ in 0..60_000 {
        let k = rng.random_range(-3000..3000i64);
        match rng.random_range(0..4) {
            0 | 1 => {
                map.get_or_insert_with(Arc::new(CoarseKey(k)), || Arc::new(k));
                model.insert(k, k);
            }
            2 => {
                assert_eq!(
                    map.remove(&CoarseKey(k)).is_some(),
                    model.remove(&k).is_some()
                );
            }
            _ => {
                assert_eq!(
                    map.get(&CoarseKey(k)).map(|e| **e.value()),
                    model.get(&k).copied()
                );
                let lo = CoarseKey(k);
                let hi = CoarseKey(k + rng.random_range(0..200));
                let got: Vec<i64> = map
                    .range::<CoarseKey, _>((Bound::Included(&lo), Bound::Included(&hi)))
                    .map(|e| e.key().0)
                    .collect();
                let expected: Vec<i64> = model.range(lo.0..=hi.0).map(|(k, _)| *k).collect();
                assert_eq!(got, expected);
                let got: Vec<i64> = map
                    .range::<CoarseKey, _>((Bound::Included(&lo), Bound::Excluded(&hi)))
                    .rev()
                    .map(|e| e.key().0)
                    .collect();
                let expected: Vec<i64> = model.range(lo.0..hi.0).rev().map(|(k, _)| *k).collect();
                assert_eq!(got, expected);
            }
        }
    }
    let keys: Vec<i64> = map.iter().map(|e| e.key().0).collect();
    assert_eq!(keys, model.keys().copied().collect::<Vec<_>>());
}
