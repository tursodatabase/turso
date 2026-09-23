use super::*;
use crate::DatabaseAllocators;
use std::{
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc as StdArc,
    },
};

struct LowerBoundOnly {
    next: usize,
    end: usize,
}

impl Iterator for LowerBoundOnly {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }
        let value = self.next;
        self.next += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.next, None)
    }
}

struct UnderreportedLowerBound {
    next: usize,
    end: usize,
}

impl Iterator for UnderreportedLowerBound {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }
        let value = self.next;
        self.next += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        ((self.end - self.next).saturating_sub(1), None)
    }
}

#[derive(Clone)]
struct CountingAlloc {
    allocations: StdArc<AtomicUsize>,
    deallocations: StdArc<AtomicUsize>,
}

unsafe impl ApiAllocator for CountingAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        <Global as ApiAllocator>::allocate(&Global, layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        unsafe {
            <Global as ApiAllocator>::deallocate(&Global, ptr, layout);
        }
    }
}

#[test]
fn database_allocators_preserve_distinct_concrete_types() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let allocators: DatabaseAllocators<Global, CountingAlloc> = DatabaseAllocators {
        mv_store: Global,
        fts: CountingAlloc {
            allocations: allocations.clone(),
            deallocations: deallocations.clone(),
        },
    };
    let cloned = allocators.clone();
    let layout = Layout::new::<u64>();
    let mv_block = cloned.mv_store.allocate(layout).unwrap();
    assert_eq!(allocations.load(Ordering::Relaxed), 0);
    let fts_block = cloned.fts.allocate(layout).unwrap();
    assert_eq!(allocations.load(Ordering::Relaxed), 1);
    unsafe {
        allocators.mv_store.deallocate(mv_block.cast(), layout);
        allocators.fts.deallocate(fts_block.cast(), layout);
    }
    assert_eq!(deallocations.load(Ordering::Relaxed), 1);
}

#[test]
fn dyn_allocator_delegates_skiplist_allocations() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let alloc = DynAllocator::new(CountingAlloc {
        allocations: allocations.clone(),
        deallocations,
    });
    let map: crate::skiplist::SkipMap<i32, i32, _, DynAllocator> =
        crate::skiplist::SkipMap::new_in(alloc);

    map.try_insert(1, 2).unwrap();

    assert!(allocations.load(Ordering::Relaxed) > 0);
}

#[test]
fn dyn_arc_slice_uses_allocator_until_last_clone_drops() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let data: ArcSlice<u8> = try_dyn_arc_slice_from_slice_in(
        b"allocator",
        CountingAlloc {
            allocations: allocations.clone(),
            deallocations: deallocations.clone(),
        },
    )
    .unwrap();
    let expected_allocations = usize::from(cfg!(nightly));
    assert_eq!(allocations.load(Ordering::Relaxed), expected_allocations);
    let clone = data.clone();
    assert_eq!(data.as_ptr(), clone.as_ptr());
    drop(data);
    assert_eq!(&*clone, b"allocator");
    assert_eq!(deallocations.load(Ordering::Relaxed), 0);
    drop(clone);
    assert_eq!(deallocations.load(Ordering::Relaxed), expected_allocations);
}

#[cfg(nightly)]
#[test]
fn arc_slice_preserves_concrete_allocator_until_last_clone_drops() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let data: ArcSlice<u8, CountingAlloc> = try_arc_slice_from_slice_in(
        b"allocator",
        CountingAlloc {
            allocations: allocations.clone(),
            deallocations: deallocations.clone(),
        },
    )
    .unwrap();
    assert_eq!(allocations.load(Ordering::Relaxed), 1);
    let clone = data.clone();
    drop(data);
    assert_eq!(&*clone, b"allocator");
    assert_eq!(deallocations.load(Ordering::Relaxed), 0);
    drop(clone);
    assert_eq!(deallocations.load(Ordering::Relaxed), 1);
}

#[test]
fn shared_bytes_preserve_contents_and_share_clones() {
    for contents in [b"".as_slice(), b"shared bytes".as_slice()] {
        let mut buffer: DynVec<u8> = TursoVecInExt::new_in(DynAllocator::default());
        buffer.try_extend(contents.iter().copied()).unwrap();
        let shared = SharedBytes::try_from_vec(buffer).unwrap();
        assert_eq!(&*shared, contents);
        let clone = shared.clone();
        assert_eq!(clone.as_ptr(), shared.as_ptr());
        drop(shared);
        assert_eq!(&*clone, contents);
    }
}

#[cfg(nightly)]
#[test]
fn shared_bytes_preserve_buffer_and_allocator_until_last_clone_drops() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let mut buffer = std::vec::Vec::try_with_capacity_in(
        32,
        CountingAlloc {
            allocations: allocations.clone(),
            deallocations: deallocations.clone(),
        },
    )
    .unwrap();
    buffer.extend_from_slice(b"abc");
    let pointer = buffer.as_ptr();
    let shared: SharedBytes<CountingAlloc> = SharedBytes::try_from_vec(buffer).unwrap();
    assert_eq!(shared.as_ptr(), pointer);
    assert_eq!(shared.capacity(), 32);
    assert_eq!(allocations.load(Ordering::Relaxed), 2);
    let clone = shared.clone();
    drop(shared);
    assert_eq!(&*clone, b"abc");
    assert_eq!(deallocations.load(Ordering::Relaxed), 0);
    drop(clone);
    assert_eq!(deallocations.load(Ordering::Relaxed), 2);
}

#[test]
fn database_open_with_allocator_uses_allocator_for_mvstore_skiplist() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let fts_allocations = StdArc::new(AtomicUsize::new(0));
    let alloc = DynAllocator::new(CountingAlloc {
        allocations: allocations.clone(),
        deallocations,
    });
    let io = StdArc::new(crate::MemoryIO::new());
    let file = crate::IO::open_file(
        io.as_ref(),
        "open-with-allocator.db",
        crate::OpenFlags::Create,
        true,
    )
    .unwrap();
    let db_file = StdArc::new(crate::storage::database::DatabaseFile::new(file));
    let db = crate::Database::open(
        io,
        "open-with-allocator.db",
        crate::OpenOptions::new(StdArc::new(crate::SqliteDialect))
            .storage(db_file)
            .allocators(DatabaseAllocators {
                mv_store: alloc,
                fts: DynAllocator::new(CountingAlloc {
                    allocations: fts_allocations.clone(),
                    deallocations: StdArc::new(AtomicUsize::new(0)),
                }),
            }),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    allocations.store(0, Ordering::Relaxed);
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

    assert!(db.get_mv_store().is_some());
    assert!(allocations.load(Ordering::Relaxed) > 0);
    assert_eq!(fts_allocations.load(Ordering::Relaxed), 0);
}

#[cfg(all(nightly, feature = "fts"))]
#[test]
fn database_fts_build_and_merge_use_only_the_fts_allocator() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let mv_allocations = StdArc::new(AtomicUsize::new(0));
    let db = crate::Database::open(
        StdArc::new(crate::MemoryIO::new()),
        ":memory:",
        crate::OpenOptions::new(StdArc::new(crate::SqliteDialect))
            .db_opts(crate::DatabaseOpts::default().with_index_method(true))
            .allocators(DatabaseAllocators {
                mv_store: DynAllocator::new(CountingAlloc {
                    allocations: mv_allocations.clone(),
                    deallocations: StdArc::new(AtomicUsize::new(0)),
                }),
                fts: DynAllocator::new(CountingAlloc {
                    allocations: allocations.clone(),
                    deallocations: deallocations.clone(),
                }),
            }),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for sql in [
        "INSERT INTO docs VALUES (7, 'hello turso')",
        "INSERT INTO docs VALUES (19, 'hello world')",
        "OPTIMIZE INDEX docs_fts",
    ] {
        let before = allocations.load(Ordering::Relaxed);
        conn.execute(sql).unwrap();
        assert!(allocations.load(Ordering::Relaxed) > before, "{sql}");
        assert_eq!(mv_allocations.load(Ordering::Relaxed), 0, "{sql}");
    }
    let rows = conn
        .prepare("SELECT id FROM docs WHERE fts_match(body, 'hello') ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        std::vec![
            std::vec![crate::Value::from_i64(7)],
            std::vec![crate::Value::from_i64(19)]
        ]
    );
    drop(conn);
    drop(db);
    assert_eq!(
        allocations.load(Ordering::Relaxed),
        deallocations.load(Ordering::Relaxed)
    );
}

#[cfg(nightly)]
#[test]
fn logical_log_shared_buffer_retains_dyn_allocator() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let alloc = DynAllocator::new(CountingAlloc {
        allocations: allocations.clone(),
        deallocations: deallocations.clone(),
    });
    let record = crate::mvcc::database::LogRecord::new(1, alloc).unwrap();
    let data: DynBoxedSlice<u8> = record.buf.into_boxed_slice();
    let shared = crate::io::SharedBufferData::new(crate::sync::Arc::new(data));
    let returned = shared.clone();

    assert!(allocations.load(Ordering::Relaxed) > 0);
    drop(shared);
    assert_eq!(deallocations.load(Ordering::Relaxed), 0);
    drop(returned);
    assert!(deallocations.load(Ordering::Relaxed) > 0);
}

#[test]
fn try_extend_accepts_exact_size_iterators() {
    let mut values: Vec<_> = TursoAllocExt::new();

    values.try_extend([1, 2, 3]).unwrap();

    assert_eq!(values.as_slice(), &[1, 2, 3]);
}

#[test]
fn try_extend_accepts_iterators_without_upper_bounds() {
    let mut values: Vec<_> = TursoAllocExt::new();

    values
        .try_extend(LowerBoundOnly { next: 0, end: 3 })
        .unwrap();

    assert_eq!(values.as_slice(), &[0, 1, 2]);
}

#[test]
fn try_extend_accepts_underreported_lower_bound_iterators() {
    let mut values = <Vec<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(4).unwrap();

    values
        .try_extend(UnderreportedLowerBound { next: 0, end: 4 })
        .unwrap();

    assert_eq!(values.as_slice(), &[0, 1, 2, 3]);
}

#[test]
fn vec_push_within_capacity_uses_reserved_slot() {
    let mut values = <Vec<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(1).unwrap();
    let initial_capacity = values.capacity();

    *values.push_within_capacity(1).unwrap() = 2;

    assert_eq!(values.as_slice(), &[2]);
    assert_eq!(values.capacity(), initial_capacity);
}

#[test]
fn vec_push_within_capacity_returns_value_when_full() {
    let mut values = <Vec<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(0).unwrap();

    assert_eq!(values.push_within_capacity(1), Err(1));
    assert!(values.is_empty());
}

#[test]
fn try_vec_with_allocator_builds_requested_values() {
    let values: DynVec<_> = try_vec![7; 3; DynAllocator::default()].unwrap();

    assert_eq!(values.as_slice(), &[7, 7, 7]);
}

#[test]
fn iterator_try_collect_accepts_iterators_without_upper_bounds() {
    let values: Vec<_> = LowerBoundOnly { next: 0, end: 3 }.try_collect().unwrap();

    assert_eq!(values.as_slice(), &[0, 1, 2]);
}

#[cfg(nightly)]
#[test]
fn vec_try_extend_reserves_before_mutation() {
    let mut values = self::vec![1];

    let result = values.try_extend(std::iter::repeat(2).take(usize::MAX));

    assert!(result.is_err());
    assert_eq!(values.as_slice(), &[1]);
}

#[test]
fn hash_set_try_insert_and_extend_reserve_before_mutation() {
    let mut values: HashSet<usize> = HashSet::default();

    assert!(TursoHashSetExt::try_insert(&mut values, 1).unwrap());
    assert!(!TursoHashSetExt::try_insert(&mut values, 1).unwrap());
    values.try_extend([2, 3]).unwrap();

    assert!(values.contains(&1));
    assert!(values.contains(&2));
    assert!(values.contains(&3));
}

#[test]
fn vec_deque_try_push_and_extend_reserve_before_mutation() {
    let mut values: VecDeque<usize> = TursoAllocExt::new();

    values.try_push_back(2).unwrap();
    values.try_push_front(1).unwrap();
    values.try_extend([3, 4]).unwrap();

    assert_eq!(values.pop_front(), Some(1));
    assert_eq!(values.pop_front(), Some(2));
    assert_eq!(values.pop_front(), Some(3));
    assert_eq!(values.pop_front(), Some(4));
    assert_eq!(values.pop_front(), None);
}

#[test]
fn binary_heap_try_push_and_extend_reserve_before_mutation() {
    let mut values: BinaryHeap<usize> = TursoAllocExt::new();

    values.try_push(2).unwrap();
    values.try_extend([1, 3]).unwrap();

    assert_eq!(values.pop(), Some(3));
    assert_eq!(values.pop(), Some(2));
    assert_eq!(values.pop(), Some(1));
    assert_eq!(values.pop(), None);
}

#[test]
fn iterator_try_collect_builds_turso_vec() {
    let values: Vec<_> = [1, 2, 3].into_iter().try_collect().unwrap();

    assert_eq!(values.as_slice(), &[1, 2, 3]);
}

#[cfg(nightly)]
#[test]
fn iterator_try_collect_in_builds_global_vec() {
    let values: Vec<_, Global> = [1, 2, 3].into_iter().try_collect_in(Global).unwrap();

    assert_eq!(values.as_slice(), &[1, 2, 3]);
}

#[test]
fn iterator_try_collect_builds_boxed_slice() {
    let values: Box<[_]> = [1, 2, 3].into_iter().try_collect().unwrap();

    assert_eq!(&*values, &[1, 2, 3]);
}

#[test]
fn iterator_try_collect_builds_turso_collections() {
    let map: HashMap<_, _> = [("one", 1), ("two", 2)].into_iter().try_collect().unwrap();
    let set: HashSet<_> = [1, 2, 3].into_iter().try_collect().unwrap();
    let heap: BinaryHeap<_> = [1, 3, 2].into_iter().try_collect().unwrap();

    assert_eq!(map.get("one"), Some(&1));
    assert!(set.contains(&3));
    assert_eq!(heap.peek(), Some(&3));
}

#[test]
fn iterator_try_collect_builds_option_collection() {
    let values: Option<Vec<_>> = [Some(1), Some(2), Some(3)]
        .into_iter()
        .try_collect()
        .unwrap();
    let none: Option<Vec<_>> = [Some(1), None, Some(3)].into_iter().try_collect().unwrap();

    assert_eq!(values.unwrap().as_slice(), &[1, 2, 3]);
    assert!(none.is_none());
}

#[test]
fn iterator_try_collect_builds_result_collection() {
    let values: Result<Vec<_>, &str> = [Ok::<_, &str>(1), Ok(2), Ok(3)]
        .into_iter()
        .try_collect()
        .unwrap();
    let error: Result<Vec<_>, &str> = [Ok(1), Err("bad"), Ok(3)]
        .into_iter()
        .try_collect()
        .unwrap();

    assert_eq!(values.unwrap().as_slice(), &[1, 2, 3]);
    assert_eq!(error, Err("bad"));
}

#[cfg(nightly)]
#[test]
fn iterator_try_collect_result_trusted_len_reserves_exact_capacity() {
    let values = (0..10)
        .map(Ok::<_, TryReserveError>)
        .try_collect::<Result<Vec<_>, TryReserveError>>()
        .unwrap()
        .unwrap();

    assert_eq!(values.as_slice(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(values.capacity(), values.len());
}

#[cfg(nightly)]
#[test]
fn iterator_try_collect_result_trusted_len_stops_on_error() {
    let mut consumed = 0;
    let result = [Ok(1), Err("bad"), Ok(3)]
        .into_iter()
        .inspect(|_| consumed += 1)
        .try_collect::<Result<Vec<_>, &str>>()
        .unwrap();

    assert_eq!(result, Err("bad"));
    assert_eq!(consumed, 2);
}

#[test]
fn iterator_try_collect_converts_result_error() {
    #[derive(Debug, PartialEq)]
    struct Converted(&'static str);

    impl From<&'static str> for Converted {
        fn from(value: &'static str) -> Self {
            Self(value)
        }
    }

    let values: Result<Vec<_>, Converted> = [
        Ok::<_, &'static str>(1),
        Ok::<_, &'static str>(2),
        Ok::<_, &'static str>(3),
    ]
    .into_iter()
    .try_collect::<Result<Vec<_>, Converted>>()
    .unwrap();
    let error: Result<Vec<_>, Converted> = [Ok(1), Err("bad"), Ok(3)]
        .into_iter()
        .try_collect::<Result<Vec<_>, Converted>>()
        .unwrap();

    assert_eq!(values.unwrap().as_slice(), &[1, 2, 3]);
    assert_eq!(error, Err(Converted("bad")));
}

#[test]
fn tuple_try_extend_extends_both_collections() {
    let mut values: (Vec<_>, VecDeque<_>) = (TursoAllocExt::new(), VecDeque::new());

    values
        .try_extend([(1, "one"), (2, "two"), (3, "three")])
        .unwrap();

    assert_eq!(values.0.as_slice(), &[1, 2, 3]);
    assert_eq!(
        values.1.into_iter().collect::<std::vec::Vec<_>>(),
        ["one", "two", "three"]
    );
}

#[test]
fn tuple_try_collect_builds_three_collections() {
    let (numbers, words, flags): (Vec<_>, VecDeque<_>, Vec<_>) =
        [(1, "one", true), (2, "two", false), (3, "three", true)]
            .into_iter()
            .try_collect()
            .unwrap();

    assert_eq!(numbers.as_slice(), &[1, 2, 3]);
    assert_eq!(
        words.into_iter().collect::<std::vec::Vec<_>>(),
        ["one", "two", "three"]
    );
    assert_eq!(flags.as_slice(), &[true, false, true]);
}

#[test]
fn iterator_try_unzip_builds_turso_collections() {
    let (numbers, words): (Vec<_>, VecDeque<_>) = [(1, "one"), (2, "two"), (3, "three")]
        .into_iter()
        .try_unzip()
        .unwrap();

    assert_eq!(numbers.as_slice(), &[1, 2, 3]);
    assert_eq!(
        words.into_iter().collect::<std::vec::Vec<_>>(),
        ["one", "two", "three"]
    );
}

#[test]
fn into_boxed_slice_builds_boxed_slice_alias() {
    let values: Vec<u32> = self::vec![1, 2, 3];

    let boxed: BoxedSlice<u32> = values.into_boxed_slice();

    assert_eq!(&*boxed, &[1, 2, 3]);
}

#[test]
fn slice_try_to_vec_builds_turso_vec() {
    let slice: &[u8] = &[1, 2, 3];

    let values: Vec<u8> = slice.try_to_vec().unwrap();

    assert_eq!(values.as_slice(), slice);
    assert!(values.capacity() >= 3);
}

#[test]
fn try_with_capacity_builds_turso_collections() {
    let values: Vec<usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();
    let map: HashMap<usize, usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();
    let set: HashSet<usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();
    let queue: VecDeque<usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();
    let heap: BinaryHeap<usize> = TursoTryWithCapacityExt::try_with_capacity_ext(3).unwrap();

    assert!(values.capacity() >= 3);
    assert!(map.capacity() >= 3);
    assert!(set.capacity() >= 3);
    assert!(queue.capacity() >= 3);
    assert!(heap.capacity() >= 3);
}

/// Element type whose fallible clone always fails: proves `Vec::try_clone`
/// clones elements through `TryClone` instead of the infallible `Clone`.
#[derive(Clone)]
struct FallibleElem;

impl TryClone for FallibleElem {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Err(TryReserveError)
    }
}

#[test]
fn try_clone_from_uses_default_replacement() {
    let source = 7_u32;
    let mut destination = 3_u32;

    destination.try_clone_from(&source).unwrap();

    assert_eq!(destination, source);
}

#[test]
fn vec_try_clone_clones_elements_fallibly() {
    let mut source: Vec<FallibleElem> = self::vec![];
    source.try_push(FallibleElem).unwrap();
    assert!(
        source.try_clone().is_err(),
        "Vec::try_clone must clone elements through TryClone"
    );
}

#[test]
fn vec_try_clone_bulk_copies_copy_elements() {
    let mut source: Vec<u32> = self::vec![];
    source.try_push(7).unwrap();
    assert_eq!(source.try_clone().unwrap().as_slice(), &[7]);
}

/// Fails cloning a marked element: exercises the early-`Err` path of the
/// spare-capacity write loop (partially written clone must drop cleanly,
/// source must stay intact).
#[derive(Clone, Debug, PartialEq)]
struct FailOnMarked {
    payload: std::string::String,
    fail: bool,
}

impl TryClone for FailOnMarked {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        if self.fail {
            Err(TryReserveError)
        } else {
            Ok(self.clone())
        }
    }
}

#[test]
fn vec_try_clone_partial_element_failure_is_clean() {
    let elem = |payload: &str, fail| FailOnMarked {
        payload: payload.into(),
        fail,
    };
    let mut source: Vec<FailOnMarked> = self::vec![];
    source.try_push(elem("a", false)).unwrap();
    source.try_push(elem("b", false)).unwrap();
    source.try_push(elem("c", true)).unwrap();
    source.try_push(elem("d", false)).unwrap();

    assert!(source.try_clone().is_err());
    // Source unchanged; the two successfully written clones were dropped.
    assert_eq!(source.len(), 4);
    assert_eq!(source[0], elem("a", false));
    assert_eq!(source[3], elem("d", false));
}

#[test]
fn vec_try_clone_deep_values_roundtrip() {
    let mut source: Vec<(std::string::String, u32)> = self::vec![];
    for i in 0..100u32 {
        source.try_push((format!("value-{i}"), i)).unwrap();
    }
    let cloned = source.try_clone().unwrap();
    assert_eq!(cloned.as_slice(), source.as_slice());
}
