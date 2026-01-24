# Achieving Determinism in the Shuttle Stress Test

This document details the changes required to make the `turso_stress` test fully deterministic under
the [Shuttle](https://github.com/awslabs/shuttle) concurrency testing framework.

## Overview

Shuttle is a randomized concurrency testing tool that explores different thread interleavings to find bugs. For
reproducibility, the same seed should produce identical execution traces across runs. However, several sources of
non-determinism in the codebase prevented this.

## Required Changes

Only **two categories** of changes are required for determinism:

1. **Seeded Scheduler** - Using `RandomScheduler::new_from_seed(seed, 1)` instead of `RandomScheduler::new(5)`
2. **HashMap to BTreeMap** - Non-deterministic iteration order in HashMap affects scheduling decisions

### What's NOT Required

Through systematic testing, we determined that the following changes are **NOT needed** for determinism with a properly seeded scheduler:

- **SpinLock shuttle wrapper** - The busy-wait loop is deterministic because Shuttle controls the atomics
- **TursoRwLock shuttle wrapper** - Atomic compare_exchange is deterministic under Shuttle's control
- **ArcSwap custom implementations** - The `arc_swap` crate works correctly with Shuttle

Shuttle's scheduler controls all atomic operations, including those in spin loops and compare-exchange sequences.
With a seeded scheduler, these execute deterministically regardless of their implementation style.

## Change Details

### 1. Shuttle Scheduler Seeding

The shuttle `main()` function must use a seeded scheduler:

```rust
// Before - hardcoded iterations, not tied to --seed
let scheduler = RandomScheduler::new(5);

// After - uses actual seed from command line
let seed = opts.seed.unwrap_or_else(rand::random);
opts.seed = Some(seed);
eprintln!("Using seed: {seed}");

let scheduler = RandomScheduler::new_from_seed(seed, 1);
let runner = shuttle::Runner::new(scheduler, config);
runner.run(move || {
    shuttle::future::block_on(Box::pin(async_main_with_opts(opts.clone()))).unwrap()
});
```

### 2. HashMap to BTreeMap Conversions

`HashMap` uses randomized hashing, making iteration order non-deterministic across runs. When iteration order affects
which code path runs first (e.g., which index to consider first in query planning), this creates scheduling
divergence.

All `HashMap` usages in hot paths were converted to `BTreeMap`.

#### Files Modified

| File                               | Fields/Variables Changed                                                             |
|------------------------------------|--------------------------------------------------------------------------------------|
| `core/schema.rs`                   | `tables`, `indexes`, `triggers`, `views`, `virtual_tables`, `pragma_schemas`         |
| `core/translate/emitter.rs`        | `hash_table_contexts`, `materialized_build_inputs`                                   |
| `core/translate/optimizer/join.rs` | `best_plan_memo`, `left_join_illegal_map`, `best_for_mask_by_last`, `left_join_deps` |
| `core/translate/optimizer/mod.rs`  | `covered_columns`, `parameters`                                                      |
| `core/translate/plan.rs`           | `covered_columns`                                                                    |
| `core/connection.rs`               | `functions`, `vtabs`, `vtab_modules`, `index_methods` in `SymbolTable`               |
| `core/util.rs`                     | Function parameters and return types                                                 |
| `core/vdbe/mod.rs`                 | `RegexCache`, `parameters`, `rowsets`, `bloom_filters`, `hash_tables`                |
| `core/vdbe/likeop.rs`              | Regex cache parameter                                                                |
| `core/vdbe/value.rs`               | `exec_like` parameter                                                                |

#### Example Change

```rust
// Before
use std::collections::HashMap;
pub struct Schema {
    pub tables: HashMap<String, Arc<Table>>,
    // ...
}

// After
use std::collections::BTreeMap;
pub struct Schema {
    pub tables: BTreeMap<String, Arc<Table>>,
    // ...
}
```

### 3. Arc::ptr_eq to Name Equality

`Arc::ptr_eq` compares memory addresses, which vary due to ASLR (Address Space Layout Randomization). These were
replaced with semantic equality checks.

| File                                      | Location   | Change                                             |
|-------------------------------------------|------------|----------------------------------------------------|
| `core/translate/optimizer/constraints.rs` | Line ~720  | `Arc::ptr_eq(&index, &i)` → `index.name == i.name` |
| `core/translate/insert.rs`                | Line ~2705 | Similar pointer comparison fix                     |

## Sync Primitives (Optional Optimizations)

The following sync primitive wrappers exist in the codebase but are **not required** for determinism.
They were added during debugging but testing confirmed they can be removed:

### SpinLock (`core/fast_lock.rs`)

The standard spin-loop implementation works correctly:

```rust
pub fn lock(&self) -> SpinLockGuard<'_, T> {
    while self.locked.swap(true, Ordering::Acquire) {
        spin_loop();  // Shuttle controls this atomic operation
    }
    SpinLockGuard { lock: self }
}
```

### TursoRwLock (`core/storage/wal.rs`)

The atomic-based implementation works correctly:

```rust
pub struct TursoRwLock(AtomicU64);  // Shuttle controls the AtomicU64
```

### ArcSwap (`core/sync.rs`)

The `arc_swap` crate can be used directly:

```rust
pub use arc_swap::{ArcSwap, ArcSwapOption};
```

## Verification

The `shuttle_diff.sh` script verifies determinism by running the stress test multiple times with the same seed and
comparing outputs:

```bash
cd testing/stress
./shuttle_diff.sh [num_seeds] [runs_per_seed] [start_seed]

# Examples
./shuttle_diff.sh 1 3 42      # Quick: seed 42, 3 runs
./shuttle_diff.sh 10 3 1      # Thorough: seeds 1-10, 3 runs each
./shuttle_diff.sh 5 5 100     # Medium: seeds 100-104, 5 runs each
```

## Build Instructions

```bash
# Build with shuttle (required for stress test)
RUSTFLAGS="--cfg shuttle" cargo build -p turso_stress

# Run stress test with specific seed
RUSTFLAGS="--cfg shuttle" cargo run -p turso_stress -- --seed 42

# Verify determinism
cd testing/stress
./shuttle_diff.sh 5 3 100
```

## Key Principles for Future Development

1. **Use `BTreeMap` instead of `HashMap`** when iteration order could affect behavior (query planning, schema
   operations, etc.).

2. **Avoid `Arc::ptr_eq`** for semantic comparisons - use field equality instead.

3. **Use seeded schedulers** in Shuttle tests - `RandomScheduler::new_from_seed(seed, iterations)`.

4. **Test determinism after changes** - run `shuttle_diff.sh` to verify.

5. **Sync primitives are fine** - SpinLock, atomics, and arc_swap work correctly with Shuttle's seeded scheduler.

## Related Commits

- `6a22feafc` - stress: implement JIT query generation
- `0fb2094e5` - schema: use BTreeMap for deterministic iteration
- `6f987f3aa` - optimizer: use name equality instead of Arc::ptr_eq
- `7f2d003c2` - stress: achieve full determinism under shuttle (includes sync wrappers that were later found unnecessary)
