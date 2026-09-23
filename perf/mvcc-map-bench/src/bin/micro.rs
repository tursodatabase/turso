use parking_lot::RwLock;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;
use turso_core::mvcc::database::{MVTableId, RowID, RowKey, SortableIndexKey};
use turso_core::types::{ImmutableRecord, IndexInfo, KeyInfo};
use turso_core::Value;

type Val = Arc<RwLock<Vec<u64>>>;

const USAGE: &str = "usage: mvcc-micro <map> <keys:table|index> <workload> <preload> <threads> <ops_per_thread> [touch] [clone]
maps: skiplist rwlock_btree scc_tree cmap olc_btree
workloads: get scan100 cursor100 insert_rand insert_seq_shared mixed95 remove";

trait Key: Ord + Clone + Send + Sync + 'static {
    const CHEAP: bool;
    fn make(i: u64) -> Self;
}

fn probe<K: Key>(keys: &[K], i: u64) -> std::borrow::Cow<'_, K> {
    if K::CHEAP {
        std::borrow::Cow::Owned(K::make(i))
    } else {
        std::borrow::Cow::Borrowed(&keys[i as usize])
    }
}

impl Key for RowID {
    const CHEAP: bool = true;
    fn make(i: u64) -> Self {
        RowID::new(MVTableId::new(-2), RowKey::Int(i as i64))
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct IndexKey(Arc<SortableIndexKey>);

fn index_info() -> Arc<IndexInfo> {
    static INFO: std::sync::OnceLock<Arc<IndexInfo>> = std::sync::OnceLock::new();
    INFO.get_or_init(|| {
        let key_info = KeyInfo {
            sort_order: turso_parser::ast::SortOrder::Asc,
            collation: Default::default(),
            nulls_order: None,
        };
        Arc::new(IndexInfo::new(vec![key_info.clone(), key_info], true, 2, false).unwrap())
    })
    .clone()
}

impl Key for IndexKey {
    const CHEAP: bool = false;
    fn make(i: u64) -> Self {
        let a = (i.wrapping_mul(2_654_435_761)) % 1_000_003;
        let record = ImmutableRecord::from_values(
            &[Value::from_i64(a as i64), Value::from_i64(i as i64)],
            2,
        )
        .unwrap();
        IndexKey(Arc::new(
            SortableIndexKey::new_from_payload_in(
                record.as_blob(),
                index_info(),
                turso_core::alloc::TursoAllocator,
            )
            .unwrap(),
        ))
    }
}

trait Map<K: Key>: Send + Sync + 'static {
    fn insert(&self, k: K, v: Val);
    fn get(&self, k: &K, clone: bool) -> bool;
    fn scan(&self, from: &K, n: usize, touch: bool) -> usize;
    fn cursor_scan(&self, from: &K, n: usize, touch: bool) -> usize;
    fn remove(&self, k: &K) -> bool;
    fn per_thread(&self) -> Box<dyn Map<K>>;
}

fn found(v: &Val, clone: bool) -> bool {
    if clone {
        black_box(v.clone());
    } else {
        black_box(Arc::as_ptr(v));
    }
    true
}

fn visit(k: &impl Sized, v: &Val, touch: bool) {
    visit_owned(k, v.clone(), touch);
}

fn visit_owned(k: &impl Sized, v: Val, touch: bool) {
    black_box(k);
    if touch {
        black_box(v.read().len());
    }
    black_box(&v);
}

// ---------------------------------------------------------------------------
// Vendored crossbeam skiplist (what MvStore uses today).
struct Skip<K: Key>(Arc<turso_core::skiplist::SkipMap<K, Val>>);

impl<K: Key> Map<K> for Skip<K> {
    fn insert(&self, k: K, v: Val) {
        self.0.get_or_insert_with(k, || v);
    }
    fn get(&self, k: &K, clone: bool) -> bool {
        self.0.get(k).is_some_and(|e| found(e.value(), clone))
    }
    fn scan(&self, from: &K, n: usize, touch: bool) -> usize {
        let mut c = 0;
        for e in self
            .0
            .range((Bound::Included(from), Bound::Unbounded))
            .take(n)
        {
            visit(e.key(), e.value(), touch);
            c += 1;
        }
        c
    }
    fn cursor_scan(&self, from: &K, n: usize, touch: bool) -> usize {
        self.scan(from, n, touch)
    }
    fn remove(&self, k: &K) -> bool {
        self.0.remove(k).is_some()
    }
    fn per_thread(&self) -> Box<dyn Map<K>> {
        Box::new(Skip(self.0.clone()))
    }
}

// ---------------------------------------------------------------------------
// std BTreeMap behind one parking_lot RwLock. A cursor cannot keep the read
// lock between steps, so cursor_scan re-seeks after the last key per step.
struct LockedBTree<K: Key>(Arc<RwLock<BTreeMap<K, Val>>>);

impl<K: Key> Map<K> for LockedBTree<K> {
    fn insert(&self, k: K, v: Val) {
        self.0.write().entry(k).or_insert(v);
    }
    fn get(&self, k: &K, clone: bool) -> bool {
        self.0.read().get(k).is_some_and(|v| found(v, clone))
    }
    fn scan(&self, from: &K, n: usize, touch: bool) -> usize {
        let m = self.0.read();
        let mut c = 0;
        for (k, v) in m.range((Bound::Included(from), Bound::Unbounded)).take(n) {
            visit(k, v, touch);
            c += 1;
        }
        c
    }
    fn cursor_scan(&self, from: &K, n: usize, touch: bool) -> usize {
        let mut last: Option<K> = None;
        let mut c = 0;
        while c < n {
            let m = self.0.read();
            let next = match &last {
                None => m.range((Bound::Included(from), Bound::Unbounded)).next(),
                Some(k) => m.range((Bound::Excluded(k), Bound::Unbounded)).next(),
            };
            let Some((k, v)) = next else { break };
            visit(k, v, touch);
            last = Some(k.clone());
            c += 1;
        }
        c
    }
    fn remove(&self, k: &K) -> bool {
        self.0.write().remove(k).is_some()
    }
    fn per_thread(&self) -> Box<dyn Map<K>> {
        Box::new(LockedBTree(self.0.clone()))
    }
}

// ---------------------------------------------------------------------------
// scc::TreeIndex: B+ tree with lock-free reads. The cursor keeps its guard
// between steps (an epoch pin, like the skiplist iterator).
struct SccTree<K: Key>(Arc<scc::TreeIndex<K, Val>>);

impl<K: Key> Map<K> for SccTree<K> {
    fn insert(&self, k: K, v: Val) {
        let _ = self.0.insert_sync(k, v);
    }
    fn get(&self, k: &K, clone: bool) -> bool {
        self.0.peek_with(k, |_, v| found(v, clone)).unwrap_or(false)
    }
    fn scan(&self, from: &K, n: usize, touch: bool) -> usize {
        let guard = scc::Guard::new();
        let mut c = 0;
        for (k, v) in self
            .0
            .range::<K, _>((Bound::Included(from), Bound::Unbounded), &guard)
            .take(n)
        {
            visit(k, v, touch);
            c += 1;
        }
        c
    }
    fn cursor_scan(&self, from: &K, n: usize, touch: bool) -> usize {
        self.scan(from, n, touch)
    }
    fn remove(&self, k: &K) -> bool {
        self.0.remove_sync(k)
    }
    fn per_thread(&self) -> Box<dyn Map<K>> {
        Box::new(SccTree(self.0.clone()))
    }
}

// ---------------------------------------------------------------------------
// concurrent-map: lock-free B+ tree from sled. Needs one handle per thread.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CmKey<K> {
    Min,
    Key(K),
}

impl<K: Ord> concurrent_map::Minimum for CmKey<K> {
    const MIN: Self = CmKey::Min;
}

struct CMap<K: Key>(concurrent_map::ConcurrentMap<CmKey<K>, Val>);

unsafe impl<K: Key> Sync for CMap<K> {}

impl<K: Key> Map<K> for CMap<K> {
    fn insert(&self, k: K, v: Val) {
        self.0.insert(CmKey::Key(k), v);
    }
    fn get(&self, k: &K, _clone: bool) -> bool {
        self.0.get(&CmKey::Key(k.clone())).is_some()
    }
    fn scan(&self, from: &K, n: usize, touch: bool) -> usize {
        let mut c = 0;
        for (k, v) in self.0.range(CmKey::Key(from.clone())..).take(n) {
            visit(&k, &v, touch);
            c += 1;
        }
        c
    }
    fn cursor_scan(&self, from: &K, n: usize, touch: bool) -> usize {
        self.scan(from, n, touch)
    }
    fn remove(&self, k: &K) -> bool {
        self.0.remove(&CmKey::Key(k.clone())).is_some()
    }
    fn per_thread(&self) -> Box<dyn Map<K>> {
        Box::new(CMap(self.0.clone()))
    }
}

// ---------------------------------------------------------------------------
// B+ tree with optimistic lock coupling (turso_core::bplus_tree).
impl turso_core::bplus_tree::KeyPrefix for IndexKey {
    fn prefix(&self) -> Option<u64> {
        turso_core::bplus_tree::KeyPrefix::prefix(&*self.0)
    }
}

type ArcIndexKey = Arc<SortableIndexKey>;

unsafe impl turso_core::bplus_tree::TreeKey for IndexKey {
    type Slot = <ArcIndexKey as turso_core::bplus_tree::TreeKey>::Slot;

    const NEEDS_DEFERRED_DROP: bool = true;

    fn write(slot: &Self::Slot, key: Self) {
        <ArcIndexKey as turso_core::bplus_tree::TreeKey>::write(slot, key.0)
    }

    fn move_from(slot: &Self::Slot, from: &Self::Slot) {
        <ArcIndexKey as turso_core::bplus_tree::TreeKey>::move_from(slot, from)
    }

    fn read<R>(slot: &Self::Slot, f: impl FnOnce(&Self) -> R) -> Option<R> {
        <ArcIndexKey as turso_core::bplus_tree::TreeKey>::read(slot, |key| {
            // SAFETY: IndexKey is a repr(transparent) wrapper of the Arc.
            f(unsafe { &*std::ptr::from_ref(key).cast::<IndexKey>() })
        })
    }

    fn take(slot: &Self::Slot) -> Self {
        IndexKey(<ArcIndexKey as turso_core::bplus_tree::TreeKey>::take(slot))
    }

    fn clear(slot: &Self::Slot) {
        <ArcIndexKey as turso_core::bplus_tree::TreeKey>::clear(slot)
    }

    fn slot_prefix(slot: &Self::Slot) -> Option<u64> {
        <ArcIndexKey as turso_core::bplus_tree::TreeKey>::slot_prefix(slot)
    }
}

trait TreeKeyBound: Key + turso_core::bplus_tree::TreeKey {}
impl<K: Key + turso_core::bplus_tree::TreeKey> TreeKeyBound for K {}

struct OlcTree<K: TreeKeyBound>(Arc<turso_core::bplus_tree::BPlusTreeMap<K, Val>>);

impl<K: TreeKeyBound> Map<K> for OlcTree<K> {
    fn insert(&self, k: K, v: Val) {
        self.0.get_or_insert_with(k, || v);
    }
    fn get(&self, k: &K, clone: bool) -> bool {
        self.0.get(k).is_some_and(|e| found(e.value(), clone))
    }
    fn scan(&self, from: &K, n: usize, touch: bool) -> usize {
        let mut c = 0;
        for e in self
            .0
            .range((Bound::Included(from), Bound::Unbounded))
            .take(n)
        {
            let k = e.key().clone();
            visit_owned(&k, e.into_value(), touch);
            c += 1;
        }
        c
    }
    fn cursor_scan(&self, from: &K, n: usize, touch: bool) -> usize {
        self.scan(from, n, touch)
    }
    fn remove(&self, k: &K) -> bool {
        self.0.remove(k).is_some()
    }
    fn per_thread(&self) -> Box<dyn Map<K>> {
        Box::new(OlcTree(self.0.clone()))
    }
}

// ---------------------------------------------------------------------------

fn new_map<K: TreeKeyBound>(name: &str) -> Box<dyn Map<K>> {
    match name {
        "skiplist" => Box::new(Skip::<K>(Arc::new(turso_core::skiplist::SkipMap::new()))),
        "rwlock_btree" => Box::new(LockedBTree::<K>(Arc::new(RwLock::new(BTreeMap::new())))),
        "scc_tree" => Box::new(SccTree::<K>(Arc::new(scc::TreeIndex::new()))),
        "cmap" => Box::new(CMap::<K>(concurrent_map::ConcurrentMap::default())),
        "olc_btree" => Box::new(OlcTree::<K>(Arc::new(
            turso_core::bplus_tree::BPlusTreeMap::new(),
        ))),
        other => panic!("unknown map {other}\n{USAGE}"),
    }
}

fn perf_control(cmd: &str) {
    let Ok(path) = std::env::var("PERF_CTL_FIFO") else {
        return;
    };
    let mut ctl = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    std::io::Write::write_all(&mut ctl, format!("{cmd}\n").as_bytes()).unwrap();
    if let Ok(ack_path) = std::env::var("PERF_ACK_FIFO") {
        let mut buf = [0u8; 5];
        let mut ack = std::fs::File::open(ack_path).unwrap();
        let _ = std::io::Read::read(&mut ack, &mut buf);
    }
}

fn new_val(i: u64) -> Val {
    Arc::new(RwLock::new(vec![i]))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 7 {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
    match args[2].as_str() {
        "table" => run::<RowID>(&args),
        "index" => run::<IndexKey>(&args),
        other => panic!("unknown key type {other}"),
    }
}

fn run<K: TreeKeyBound>(args: &[String]) {
    let map_name = args[1].as_str();
    let workload = args[3].as_str();
    let preload: u64 = args[4].parse().unwrap();
    let threads: usize = args[5].parse().unwrap();
    let ops: u64 = args[6].parse().unwrap();
    let touch = args[7..].iter().any(|s| s == "touch");
    let clone = args[7..].iter().any(|s| s == "clone");

    let map = new_map::<K>(map_name);
    let keys: Vec<K> = if K::CHEAP {
        Vec::new()
    } else {
        (0..preload + threads as u64 * ops + 1)
            .map(K::make)
            .collect()
    };
    let keys = Arc::new(keys);
    for i in 0..preload {
        map.insert(probe(&keys, i).into_owned(), new_val(i));
    }

    let next_seq = Arc::new(AtomicU64::new(preload));
    let barrier = Arc::new(Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let map = map.per_thread();
            let keys = keys.clone();
            let barrier = barrier.clone();
            let next_seq = next_seq.clone();
            let workload = workload.to_string();
            std::thread::spawn(move || {
                let mut rng = rand::rngs::StdRng::seed_from_u64(99 + t as u64);
                let fresh: Vec<Val> = if workload.starts_with("insert") || workload == "mixed95" {
                    (0..ops).map(new_val).collect()
                } else {
                    Vec::new()
                };
                barrier.wait();
                let mut sink = 0usize;
                match workload.as_str() {
                    "get" => {
                        for _ in 0..ops {
                            let i = rng.random_range(0..preload);
                            sink += map.get(&probe(&keys, i), clone) as usize;
                        }
                    }
                    "scan100" => {
                        for _ in 0..ops {
                            let i = rng.random_range(0..preload);
                            sink += map.scan(&probe(&keys, i), 100, touch);
                        }
                    }
                    "cursor100" => {
                        for _ in 0..ops {
                            let i = rng.random_range(0..preload);
                            sink += map.cursor_scan(&probe(&keys, i), 100, touch);
                        }
                    }
                    "insert_rand" => {
                        let base = preload + t as u64 * ops;
                        let mut order: Vec<u64> = (0..ops).collect();
                        for i in (1..order.len()).rev() {
                            let j = rng.random_range(0..=i);
                            order.swap(i, j);
                        }
                        for (n, &o) in order.iter().enumerate() {
                            map.insert(probe(&keys, base + o).into_owned(), fresh[n].clone());
                        }
                        sink += ops as usize;
                    }
                    "insert_seq_shared" => {
                        for n in 0..ops as usize {
                            let i = next_seq.fetch_add(1, Ordering::Relaxed);
                            map.insert(probe(&keys, i).into_owned(), fresh[n].clone());
                        }
                        sink += ops as usize;
                    }
                    "mixed95" => {
                        let base = preload + t as u64 * ops;
                        let mut inserted = 0u64;
                        for n in 0..ops as usize {
                            if rng.random_range(0..100) < 5 {
                                let i = base + inserted;
                                inserted += 1;
                                map.insert(probe(&keys, i).into_owned(), fresh[n].clone());
                            } else {
                                let i = rng.random_range(0..preload);
                                sink += map.get(&probe(&keys, i), clone) as usize;
                            }
                        }
                    }
                    "remove" => {
                        let per = preload / threads as u64;
                        let start = t as u64 * per;
                        for i in start..start + per.min(ops) {
                            sink += map.remove(&probe(&keys, i)) as usize;
                        }
                    }
                    other => panic!("unknown workload {other}\n{USAGE}"),
                }
                sink
            })
        })
        .collect();
    perf_control("enable");
    barrier.wait();
    let start = Instant::now();
    let sink: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let secs = start.elapsed().as_secs_f64();
    perf_control("disable");
    let total_ops = match workload {
        "remove" => (preload / threads as u64).min(ops) * threads as u64,
        _ => ops * threads as u64,
    };
    println!(
        "map={map_name} keys={} workload={workload} preload={preload} threads={threads} ops={total_ops} secs={secs:.3} mops={:.3} ns_per_op={:.1} sink={sink}",
        args[2],
        total_ops as f64 / secs / 1e6,
        secs * 1e9 * threads as f64 / total_ops as f64,
    );
    drop(map);
}
