#![cfg_attr(nightly, feature(allocator_api))]

use divan::{black_box, AllocProfiler, Bencher};
use mimalloc::MiMalloc;
use rustc_hash::FxBuildHasher;
use turso_core::alloc::{
    self, TursoBinaryHeapExt, TursoFromIterator, TursoHashMapExt, TursoHashSetExt,
    TursoIteratorExt, TursoTryWithCapacityExt, TursoVecDequeExt, TursoVecExt,
};

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

#[cfg(not(feature = "codspeed"))]
const SAMPLE_COUNT: u32 = 1000;

#[cfg(not(feature = "codspeed"))]
fn main() {
    divan::Divan::default().sample_count(SAMPLE_COUNT).main();
}

#[cfg(feature = "codspeed")]
fn main() {
    divan::main();
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct NonCopyValue {
    id: usize,
    payload: [usize; 4],
}

impl NonCopyValue {
    fn new(id: usize) -> Self {
        Self {
            id,
            payload: [
                id,
                id.wrapping_mul(3),
                id.rotate_left(7),
                id ^ 0x9e37_79b9_7f4a_7c15,
            ],
        }
    }
}

struct LowerBoundOnly<I> {
    iter: I,
}

impl<I> LowerBoundOnly<I> {
    fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I> Iterator for LowerBoundOnly<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, _) = self.iter.size_hint();
        (lower, None)
    }
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_push_turso(bencher: Bencher, len: usize) {
    bencher
        .with_inputs(|| {
            <alloc::Vec<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len).unwrap()
        })
        .bench_local_values(|mut values| {
            for value in 0..len {
                values.try_push(black_box(value)).unwrap();
            }
            black_box(values)
        });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_push_std(bencher: Bencher, len: usize) {
    bencher
        .with_inputs(|| std::vec::Vec::with_capacity(len))
        .bench_local_values(|mut values| {
            for value in 0..len {
                values.push(black_box(value));
            }
            black_box(values)
        });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(black_box)
            .try_collect::<alloc::Vec<_>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len).map(black_box).collect::<std::vec::Vec<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::Vec<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len).unwrap();
        TursoFromIterator::try_extend(&mut values, (0..len).map(black_box)).unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::vec::Vec::with_capacity(len);
        values.extend((0..len).map(black_box));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_push_back_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::VecDeque<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        for value in 0..len {
            values.try_push_back(black_box(value)).unwrap();
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_push_back_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::VecDeque::with_capacity(len);
        for value in 0..len {
            values.push_back(black_box(value));
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(black_box)
            .try_collect::<alloc::VecDeque<_>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(black_box)
            .collect::<std::collections::VecDeque<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::VecDeque<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        TursoFromIterator::try_extend(&mut values, (0..len).map(black_box)).unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::VecDeque::with_capacity(len);
        values.extend((0..len).map(black_box));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_push_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::BinaryHeap<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        for value in 0..len {
            values.try_push(black_box(value)).unwrap();
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_push_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::BinaryHeap::with_capacity(len);
        for value in 0..len {
            values.push(black_box(value));
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(black_box)
            .try_collect::<alloc::BinaryHeap<_>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(black_box)
            .collect::<std::collections::BinaryHeap<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::BinaryHeap<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        TursoFromIterator::try_extend(&mut values, (0..len).map(black_box)).unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::BinaryHeap::with_capacity(len);
        values.extend((0..len).map(black_box));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_insert_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashSet<usize, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        for value in 0..len {
            TursoHashSetExt::try_insert(&mut values, black_box(value)).unwrap();
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_insert_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashSet::with_capacity_and_hasher(len, FxBuildHasher);
        for value in 0..len {
            values.insert(black_box(value));
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(black_box)
            .try_collect::<alloc::HashSet<_, FxBuildHasher>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(black_box)
            .collect::<std::collections::HashSet<_, FxBuildHasher>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashSet<usize, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        TursoFromIterator::try_extend(&mut values, (0..len).map(black_box)).unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashSet::with_capacity_and_hasher(len, FxBuildHasher);
        values.extend((0..len).map(black_box));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_insert_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashMap<usize, usize, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        for value in 0..len {
            TursoHashMapExt::try_insert(&mut values, black_box(value), black_box(value)).unwrap();
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| (black_box(value), black_box(value)))
            .try_collect::<alloc::HashMap<_, _, FxBuildHasher>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| (black_box(value), black_box(value)))
            .collect::<std::collections::HashMap<_, _, FxBuildHasher>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashMap<usize, usize, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        TursoFromIterator::try_extend(
            &mut values,
            (0..len).map(|value| (black_box(value), black_box(value))),
        )
        .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashMap::with_capacity_and_hasher(len, FxBuildHasher);
        values.extend((0..len).map(|value| (black_box(value), black_box(value))));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_insert_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashMap::with_capacity_and_hasher(len, FxBuildHasher);
        for value in 0..len {
            values.insert(black_box(value), black_box(value));
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_collect_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = LowerBoundOnly::new((0..len).map(black_box)).collect::<std::vec::Vec<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_collect_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = LowerBoundOnly::new((0..len).map(black_box))
            .try_collect::<alloc::Vec<_>>()
            .unwrap();
        black_box(values)
    });
}

#[cfg(nightly)]
#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_collect_unknown_upper_global_turso_traits(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = LowerBoundOnly::new((0..len).map(black_box))
            .try_collect_in::<alloc::Vec<_, alloc::Global>, _>(alloc::Global)
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_extend_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::vec::Vec::with_capacity(len);
        values.extend(LowerBoundOnly::new((0..len).map(black_box)));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_extend_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::Vec<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len).unwrap();
        TursoFromIterator::try_extend(&mut values, LowerBoundOnly::new((0..len).map(black_box)))
            .unwrap();
        black_box(values)
    });
}

#[cfg(nightly)]
#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_extend_unknown_upper_global_turso_traits(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = alloc::Vec::with_capacity_in(len, alloc::Global);
        TursoFromIterator::try_extend(&mut values, LowerBoundOnly::new((0..len).map(black_box)))
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_collect_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values =
            LowerBoundOnly::new((0..len).map(black_box)).collect::<std::collections::VecDeque<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_collect_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = LowerBoundOnly::new((0..len).map(black_box))
            .try_collect::<alloc::VecDeque<_>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_extend_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::VecDeque::with_capacity(len);
        values.extend(LowerBoundOnly::new((0..len).map(black_box)));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_extend_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::VecDeque<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        TursoFromIterator::try_extend(&mut values, LowerBoundOnly::new((0..len).map(black_box)))
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_collect_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = LowerBoundOnly::new((0..len).map(black_box))
            .collect::<std::collections::BinaryHeap<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_collect_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = LowerBoundOnly::new((0..len).map(black_box))
            .try_collect::<alloc::BinaryHeap<_>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_extend_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::BinaryHeap::with_capacity(len);
        values.extend(LowerBoundOnly::new((0..len).map(black_box)));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_extend_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::BinaryHeap<usize> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        TursoFromIterator::try_extend(&mut values, LowerBoundOnly::new((0..len).map(black_box)))
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_collect_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = LowerBoundOnly::new((0..len).map(black_box))
            .collect::<std::collections::HashSet<_, FxBuildHasher>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_collect_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = LowerBoundOnly::new((0..len).map(black_box))
            .try_collect::<alloc::HashSet<_, FxBuildHasher>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_extend_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashSet::with_capacity_and_hasher(len, FxBuildHasher);
        values.extend(LowerBoundOnly::new((0..len).map(black_box)));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_extend_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashSet<usize, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        TursoFromIterator::try_extend(&mut values, LowerBoundOnly::new((0..len).map(black_box)))
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_collect_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values =
            LowerBoundOnly::new((0..len).map(|value| (black_box(value), black_box(value))))
                .collect::<std::collections::HashMap<_, _, FxBuildHasher>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_collect_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values =
            LowerBoundOnly::new((0..len).map(|value| (black_box(value), black_box(value))))
                .try_collect::<alloc::HashMap<_, _, FxBuildHasher>>()
                .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_extend_unknown_upper_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashMap::with_capacity_and_hasher(len, FxBuildHasher);
        values.extend(LowerBoundOnly::new(
            (0..len).map(|value| (black_box(value), black_box(value))),
        ));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_extend_unknown_upper_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashMap<usize, usize, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        TursoFromIterator::try_extend(
            &mut values,
            LowerBoundOnly::new((0..len).map(|value| (black_box(value), black_box(value)))),
        )
        .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_non_copy_push_std(bencher: Bencher, len: usize) {
    bencher
        .with_inputs(|| std::vec::Vec::with_capacity(len))
        .bench_local_values(|mut values| {
            for value in 0..len {
                values.push(black_box(NonCopyValue::new(value)));
            }
            black_box(values)
        });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_non_copy_push_turso(bencher: Bencher, len: usize) {
    bencher
        .with_inputs(|| {
            <alloc::Vec<NonCopyValue> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap()
        })
        .bench_local_values(|mut values| {
            for value in 0..len {
                values
                    .try_push(black_box(NonCopyValue::new(value)))
                    .unwrap();
            }
            black_box(values)
        });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_non_copy_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| black_box(NonCopyValue::new(value)))
            .collect::<std::vec::Vec<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_non_copy_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| black_box(NonCopyValue::new(value)))
            .try_collect::<alloc::Vec<_>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_non_copy_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::vec::Vec::with_capacity(len);
        values.extend((0..len).map(|value| black_box(NonCopyValue::new(value))));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_non_copy_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::Vec<NonCopyValue> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        TursoFromIterator::try_extend(
            &mut values,
            (0..len).map(|value| black_box(NonCopyValue::new(value))),
        )
        .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_non_copy_push_back_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::VecDeque::with_capacity(len);
        for value in 0..len {
            values.push_back(black_box(NonCopyValue::new(value)));
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_non_copy_push_back_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::VecDeque<NonCopyValue> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        for value in 0..len {
            values
                .try_push_back(black_box(NonCopyValue::new(value)))
                .unwrap();
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_non_copy_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| black_box(NonCopyValue::new(value)))
            .collect::<std::collections::VecDeque<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_non_copy_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| black_box(NonCopyValue::new(value)))
            .try_collect::<alloc::VecDeque<_>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_non_copy_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::VecDeque::with_capacity(len);
        values.extend((0..len).map(|value| black_box(NonCopyValue::new(value))));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn vec_deque_non_copy_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::VecDeque<NonCopyValue> as TursoTryWithCapacityExt>::try_with_capacity_ext(len)
                .unwrap();
        TursoFromIterator::try_extend(
            &mut values,
            (0..len).map(|value| black_box(NonCopyValue::new(value))),
        )
        .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_non_copy_push_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::BinaryHeap::with_capacity(len);
        for value in 0..len {
            values.push(black_box(NonCopyValue::new(value)));
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_non_copy_push_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::BinaryHeap<NonCopyValue> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        for value in 0..len {
            values
                .try_push(black_box(NonCopyValue::new(value)))
                .unwrap();
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_non_copy_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| black_box(NonCopyValue::new(value)))
            .collect::<std::collections::BinaryHeap<_>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_non_copy_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| black_box(NonCopyValue::new(value)))
            .try_collect::<alloc::BinaryHeap<_>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_non_copy_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::BinaryHeap::with_capacity(len);
        values.extend((0..len).map(|value| black_box(NonCopyValue::new(value))));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn binary_heap_non_copy_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::BinaryHeap<NonCopyValue> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        TursoFromIterator::try_extend(
            &mut values,
            (0..len).map(|value| black_box(NonCopyValue::new(value))),
        )
        .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_non_copy_insert_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashSet::with_capacity_and_hasher(len, FxBuildHasher);
        for value in 0..len {
            values.insert(black_box(NonCopyValue::new(value)));
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_non_copy_insert_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashSet<NonCopyValue, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        for value in 0..len {
            TursoHashSetExt::try_insert(&mut values, black_box(NonCopyValue::new(value))).unwrap();
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_non_copy_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| black_box(NonCopyValue::new(value)))
            .collect::<std::collections::HashSet<_, FxBuildHasher>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_non_copy_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| black_box(NonCopyValue::new(value)))
            .try_collect::<alloc::HashSet<_, FxBuildHasher>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_non_copy_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashSet::with_capacity_and_hasher(len, FxBuildHasher);
        values.extend((0..len).map(|value| black_box(NonCopyValue::new(value))));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_set_non_copy_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashSet<NonCopyValue, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        TursoFromIterator::try_extend(
            &mut values,
            (0..len).map(|value| black_box(NonCopyValue::new(value))),
        )
        .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_non_copy_insert_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashMap::with_capacity_and_hasher(len, FxBuildHasher);
        for value in 0..len {
            values.insert(
                black_box(NonCopyValue::new(value)),
                black_box(NonCopyValue::new(value.wrapping_mul(3))),
            );
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_non_copy_insert_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashMap<NonCopyValue, NonCopyValue, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        for value in 0..len {
            TursoHashMapExt::try_insert(
                &mut values,
                black_box(NonCopyValue::new(value)),
                black_box(NonCopyValue::new(value.wrapping_mul(3))),
            )
            .unwrap();
        }
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_non_copy_collect_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| {
                (
                    black_box(NonCopyValue::new(value)),
                    black_box(NonCopyValue::new(value.wrapping_mul(3))),
                )
            })
            .collect::<std::collections::HashMap<_, _, FxBuildHasher>>();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_non_copy_collect_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let values = (0..len)
            .map(|value| {
                (
                    black_box(NonCopyValue::new(value)),
                    black_box(NonCopyValue::new(value.wrapping_mul(3))),
                )
            })
            .try_collect::<alloc::HashMap<_, _, FxBuildHasher>>()
            .unwrap();
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_non_copy_extend_std(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values = std::collections::HashMap::with_capacity_and_hasher(len, FxBuildHasher);
        values.extend((0..len).map(|value| {
            (
                black_box(NonCopyValue::new(value)),
                black_box(NonCopyValue::new(value.wrapping_mul(3))),
            )
        }));
        black_box(values)
    });
}

#[turso_macros::divan_bench(args = [64, 1_024, 16_384])]
fn hash_map_non_copy_extend_turso(bencher: Bencher, len: usize) {
    bencher.bench_local(|| {
        let mut values =
            <alloc::HashMap<NonCopyValue, NonCopyValue, FxBuildHasher> as TursoTryWithCapacityExt>::try_with_capacity_ext(
                len,
            )
            .unwrap();
        TursoFromIterator::try_extend(
            &mut values,
            (0..len).map(|value| {
                (
                    black_box(NonCopyValue::new(value)),
                    black_box(NonCopyValue::new(value.wrapping_mul(3))),
                )
            }),
        )
        .unwrap();
        black_box(values)
    });
}
