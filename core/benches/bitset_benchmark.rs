//! Microbenchmarks for [`turso_core::BitSet`], the dense bitset that the join
//! optimizer uses for table masks and the planner uses for column masks.
//!
//! The shapes come from real planner code: table masks hold a handful of bits
//! out of fewer than 64, and column masks stay inline until a table has more
//! than 64 columns.
//!
//! Run with:
//!   cargo bench -p turso_core --features bench --bench bitset_benchmark
//!
//! For instruction counts, build the binary and run the fixed workload under
//! callgrind. The fixed workload does the same number of operations on every
//! run, so the counts are stable:
//!   cargo bench -p turso_core --features bench --bench bitset_benchmark --no-run
//!   valgrind --tool=callgrind --callgrind-out-file=cg.out <binary> --fixed
//!   callgrind_annotate cg.out

use divan::{black_box, AllocProfiler};
use mimalloc::MiMalloc;
use turso_core::alloc::TryClone;
use turso_core::BitSet;

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

#[cfg(not(feature = "codspeed"))]
fn main() {
    if std::env::args().any(|arg| arg == "--fixed") {
        println!("checksum {}", fixed::run());
        return;
    }
    divan::Divan::default().sample_count(100).main();
}

#[cfg(feature = "codspeed")]
fn main() {
    divan::main();
}

/// Bits of a typical table mask: 8 tables, every third one joined.
const TABLE_BITS: [usize; 3] = [0, 3, 7];
/// Bits of a typical column mask on a wide table.
const WIDE_COLUMN_BITS: [usize; 6] = [1, 40, 64, 100, 190, 250];

fn inline_mask() -> BitSet {
    let mut set = BitSet::default();
    for bit in black_box(TABLE_BITS) {
        set.set(bit).unwrap();
    }
    set
}

fn wide_mask() -> BitSet {
    let mut set = BitSet::default();
    for bit in black_box(WIDE_COLUMN_BITS) {
        set.set(bit).unwrap();
    }
    set
}

#[turso_macros::divan_bench]
fn set_inline() -> BitSet {
    inline_mask()
}

#[turso_macros::divan_bench]
fn set_overflow() -> BitSet {
    wide_mask()
}

#[turso_macros::divan_bench]
fn get_inline() -> usize {
    let set = inline_mask();
    let mut hits = 0;
    for bit in 0..64 {
        hits += black_box(&set).get(bit) as usize;
    }
    hits
}

#[turso_macros::divan_bench]
fn get_overflow() -> usize {
    let set = wide_mask();
    let mut hits = 0;
    for bit in 0..256 {
        hits += black_box(&set).get(bit) as usize;
    }
    hits
}

#[turso_macros::divan_bench]
fn iter_inline() -> usize {
    let set = inline_mask();
    black_box(&set).iter().sum()
}

#[turso_macros::divan_bench]
fn iter_overflow() -> usize {
    let set = wide_mask();
    black_box(&set).iter().sum()
}

#[turso_macros::divan_bench]
fn iter_dense() -> usize {
    let mut set = BitSet::default();
    for bit in 0..256 {
        set.set(bit).unwrap();
    }
    black_box(&set).iter().sum()
}

#[turso_macros::divan_bench]
fn contains_all_inline() -> bool {
    let big = inline_mask();
    let small = BitSet::try_from(0b1001u128).unwrap();
    black_box(&big).contains_all_set_bits_of(black_box(&small))
}

#[turso_macros::divan_bench]
fn contains_all_overflow() -> bool {
    let big = wide_mask();
    let small = inline_mask();
    black_box(&big).contains_all_set_bits_of(black_box(&small))
}

#[turso_macros::divan_bench]
fn intersects_inline() -> bool {
    let left = inline_mask();
    let right = BitSet::try_from(0b1000u128).unwrap();
    black_box(&left).intersects(black_box(&right))
}

#[turso_macros::divan_bench]
fn union_inline() -> BitSet {
    let mut left = inline_mask();
    let right = BitSet::try_from(0b1_0000_1000u128).unwrap();
    left.union_with(black_box(&right)).unwrap();
    left
}

#[turso_macros::divan_bench]
fn union_overflow() -> BitSet {
    let mut left = wide_mask();
    let right = wide_mask();
    left.union_with(black_box(&right)).unwrap();
    left
}

#[turso_macros::divan_bench]
fn subtract_inline() -> BitSet {
    let mut left = inline_mask();
    let right = BitSet::try_from(0b1000u128).unwrap();
    left.subtract(black_box(&right));
    left
}

#[turso_macros::divan_bench]
fn count_inline() -> usize {
    let set = inline_mask();
    black_box(&set).count()
}

#[turso_macros::divan_bench]
fn rank_overflow() -> usize {
    let set = wide_mask();
    black_box(&set).rank(200)
}

#[turso_macros::divan_bench]
fn is_only_inline() -> bool {
    let set = BitSet::try_from(0b1000u128).unwrap();
    black_box(&set).is_only(3)
}

#[turso_macros::divan_bench]
fn clone_inline() -> BitSet {
    let set = inline_mask();
    black_box(&set).try_clone().unwrap()
}

#[turso_macros::divan_bench]
fn clone_overflow() -> BitSet {
    let set = wide_mask();
    black_box(&set).try_clone().unwrap()
}

/// Fixed-size workload for callgrind. Every function runs the same number of
/// operations on every run, so instruction counts can be compared directly.
#[cfg(not(feature = "codspeed"))]
mod fixed {
    use super::*;

    const ROUNDS: usize = 10_000;

    pub fn run() -> usize {
        let mut total = 0;
        total += fixed_set_inline();
        total += fixed_set_overflow();
        total += fixed_get_inline();
        total += fixed_get_overflow();
        total += fixed_iter_inline();
        total += fixed_iter_overflow();
        total += fixed_iter_dense();
        total += fixed_contains_all_inline();
        total += fixed_contains_all_overflow();
        total += fixed_intersects_inline();
        total += fixed_union_inline();
        total += fixed_union_overflow();
        total += fixed_subtract_inline();
        total += fixed_count_inline();
        total += fixed_rank_overflow();
        total += fixed_is_only_inline();
        total += fixed_clone_inline();
        total += fixed_clone_overflow();
        total
    }

    #[inline(never)]
    fn fixed_set_inline() -> usize {
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += inline_mask().count();
        }
        total
    }

    #[inline(never)]
    fn fixed_set_overflow() -> usize {
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += wide_mask().count();
        }
        total
    }

    #[inline(never)]
    fn fixed_get_inline() -> usize {
        let set = inline_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            for bit in 0..64 {
                total += black_box(&set).get(bit) as usize;
            }
        }
        total
    }

    #[inline(never)]
    fn fixed_get_overflow() -> usize {
        let set = wide_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            for bit in 0..256 {
                total += black_box(&set).get(bit) as usize;
            }
        }
        total
    }

    #[inline(never)]
    fn fixed_iter_inline() -> usize {
        let set = inline_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&set).iter().sum::<usize>();
        }
        total
    }

    #[inline(never)]
    fn fixed_iter_overflow() -> usize {
        let set = wide_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&set).iter().sum::<usize>();
        }
        total
    }

    #[inline(never)]
    fn fixed_iter_dense() -> usize {
        let mut set = BitSet::default();
        for bit in 0..256 {
            set.set(bit).unwrap();
        }
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&set).iter().sum::<usize>();
        }
        total
    }

    #[inline(never)]
    fn fixed_contains_all_inline() -> usize {
        let big = inline_mask();
        let small = BitSet::try_from(0b1001u128).unwrap();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&big).contains_all_set_bits_of(black_box(&small)) as usize;
        }
        total
    }

    #[inline(never)]
    fn fixed_contains_all_overflow() -> usize {
        let big = wide_mask();
        let small = inline_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&big).contains_all_set_bits_of(black_box(&small)) as usize;
        }
        total
    }

    #[inline(never)]
    fn fixed_intersects_inline() -> usize {
        let left = inline_mask();
        let right = BitSet::try_from(0b1000u128).unwrap();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&left).intersects(black_box(&right)) as usize;
        }
        total
    }

    #[inline(never)]
    fn fixed_union_inline() -> usize {
        let right = BitSet::try_from(0b1_0000_1000u128).unwrap();
        let mut total = 0;
        for _ in 0..ROUNDS {
            let mut left = inline_mask();
            left.union_with(black_box(&right)).unwrap();
            total += left.count();
        }
        total
    }

    #[inline(never)]
    fn fixed_union_overflow() -> usize {
        let right = wide_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            let mut left = wide_mask();
            left.union_with(black_box(&right)).unwrap();
            total += left.count();
        }
        total
    }

    #[inline(never)]
    fn fixed_subtract_inline() -> usize {
        let right = BitSet::try_from(0b1000u128).unwrap();
        let mut total = 0;
        for _ in 0..ROUNDS {
            let mut left = inline_mask();
            left.subtract(black_box(&right));
            total += left.count();
        }
        total
    }

    #[inline(never)]
    fn fixed_count_inline() -> usize {
        let set = inline_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&set).count();
        }
        total
    }

    #[inline(never)]
    fn fixed_rank_overflow() -> usize {
        let set = wide_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&set).rank(200);
        }
        total
    }

    #[inline(never)]
    fn fixed_is_only_inline() -> usize {
        let set = BitSet::try_from(0b1000u128).unwrap();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&set).is_only(3) as usize;
        }
        total
    }

    #[inline(never)]
    fn fixed_clone_inline() -> usize {
        let set = inline_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&set).try_clone().unwrap().count();
        }
        total
    }

    #[inline(never)]
    fn fixed_clone_overflow() -> usize {
        let set = wide_mask();
        let mut total = 0;
        for _ in 0..ROUNDS {
            total += black_box(&set).try_clone().unwrap().count();
        }
        total
    }
}
