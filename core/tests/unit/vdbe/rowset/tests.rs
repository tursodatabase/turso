use super::*;
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

fn get_seed() -> u64 {
    std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    ) as u64
}

#[test]
fn test_empty_rowset() {
    let rowset = RowSet::new();
    assert!(rowset.is_empty());
}

#[test]
fn test_insert_and_test() {
    let mut rowset = RowSet::new();

    rowset.insert(10).unwrap();
    rowset.insert(20).unwrap();
    rowset.insert(30).unwrap();

    assert!(!rowset.test(10, 0));
    assert!(!rowset.test(20, 0));
    assert!(!rowset.test(30, 0));

    assert!(rowset.test(10, 1));
    assert!(rowset.test(20, 1));
    assert!(rowset.test(30, 1));
    assert!(!rowset.test(40, 1));
}

#[test]
fn test_batch_consolidation() {
    let mut rowset = RowSet::new();

    // Insert values into batch 0 (first set)
    rowset.insert(10).unwrap();
    rowset.insert(20).unwrap();
    // Batch 0 doesn't test for membership (guaranteed not to contain values)
    assert!(!rowset.test(10, 0));

    // Insert more values (still in fresh, not consolidated yet)
    rowset.insert(30).unwrap();
    rowset.insert(40).unwrap();

    // Test with batch 1: triggers consolidation of fresh values (10, 20, 30, 40)
    // All should be found after consolidation
    assert!(rowset.test(10, 1));
    assert!(rowset.test(20, 1));
    assert!(rowset.test(30, 1));
    assert!(rowset.test(40, 1));
    assert!(!rowset.test(50, 1));

    // Insert value 50 (goes to fresh, not consolidated yet)
    rowset.insert(50).unwrap();

    // Test with same batch (1): no new consolidation, so 50 not found yet
    assert!(rowset.test(10, 1));
    assert!(!rowset.test(50, 1));

    // Test with new batch (2): triggers consolidation, now 50 is found
    assert!(rowset.test(50, 2));
}

#[test]
fn test_smallest_extraction() {
    let mut rowset = RowSet::new();

    rowset.insert(30).unwrap();
    rowset.insert(10).unwrap();
    rowset.insert(50).unwrap();
    rowset.insert(20).unwrap();
    rowset.insert(40).unwrap();

    assert_eq!(rowset.smallest(), Some(10));
    assert_eq!(rowset.smallest(), Some(20));
    assert_eq!(rowset.smallest(), Some(30));
    assert_eq!(rowset.smallest(), Some(40));
    assert_eq!(rowset.smallest(), Some(50));
    assert_eq!(rowset.smallest(), None);
    assert!(rowset.is_empty());
}

#[test]
fn test_smallest_with_duplicates() {
    let mut rowset = RowSet::new();

    rowset.insert(10).unwrap();
    rowset.insert(20).unwrap();
    rowset.insert(10).unwrap();
    rowset.insert(30).unwrap();
    rowset.insert(20).unwrap();

    assert_eq!(rowset.smallest(), Some(10));
    assert_eq!(rowset.smallest(), Some(20));
    assert_eq!(rowset.smallest(), Some(30));
    assert_eq!(rowset.smallest(), None);
}

#[test]
fn test_insert_after_smallest_panics() {
    let mut rowset = RowSet::new();
    rowset.insert(10).unwrap();
    rowset.smallest();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rowset.insert(20).unwrap();
    }));
    assert!(result.is_err());
}

#[test]
fn test_test_after_smallest_panics() {
    let mut rowset = RowSet::new();
    rowset.insert(10).unwrap();
    rowset.smallest();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rowset.test(10, 1);
    }));
    assert!(result.is_err());
}

#[test]
fn test_smallest_after_test_panics() {
    let mut rowset = RowSet::new();
    rowset.insert(10).unwrap();
    rowset.test(10, 1);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rowset.smallest();
    }));
    assert!(result.is_err());
}

#[test]
fn test_batch_zero_allows_smallest() {
    let mut rowset = RowSet::new();
    rowset.insert(10).unwrap();
    rowset.insert(20).unwrap();
    rowset.insert(30).unwrap();
    rowset.insert(5).unwrap();
    rowset.insert(15).unwrap();

    assert_eq!(rowset.smallest(), Some(5));
    assert_eq!(rowset.smallest(), Some(10));
    assert_eq!(rowset.smallest(), Some(15));
    assert_eq!(rowset.smallest(), Some(20));
    assert_eq!(rowset.smallest(), Some(30));
    assert_eq!(rowset.smallest(), None);
}

#[test]
fn test_empty_smallest() {
    let mut rowset = RowSet::new();
    assert_eq!(rowset.smallest(), None);
    assert!(rowset.is_empty());
}

#[test]
fn test_batch_zero_semantics() {
    let mut rowset = RowSet::new();

    rowset.insert(10).unwrap();
    rowset.insert(20).unwrap();

    assert!(!rowset.test(10, 0));
    assert!(!rowset.test(20, 0));

    assert!(rowset.test(10, 1));
    assert!(rowset.test(20, 1));
}

#[test]
fn test_batch_final_semantics() {
    let mut rowset = RowSet::new();

    // Insert and consolidate with batch 1
    rowset.insert(10).unwrap();
    assert!(rowset.test(10, 1));

    // Insert more (goes to fresh)
    rowset.insert(20).unwrap();

    // Test with batch -1 (final set): consolidates fresh values
    // Both 10 (already consolidated) and 20 (now consolidated) should be found
    assert!(rowset.test(10, -1));
    assert!(rowset.test(20, -1));

    // Test non-existent value: should not insert (batch == -1 means no insertion)
    // Verify it's still not found on second test
    assert!(!rowset.test(30, -1));
    assert!(!rowset.test(30, -1));
}

#[test]
fn test_negative_values() {
    let mut rowset = RowSet::new();

    rowset.insert(-10).unwrap();
    rowset.insert(-5).unwrap();
    rowset.insert(0).unwrap();
    rowset.insert(5).unwrap();
    rowset.insert(10).unwrap();

    assert!(rowset.test(-10, 1));
    assert!(rowset.test(-5, 1));
    assert!(rowset.test(0, 1));
    assert!(rowset.test(5, 1));
    assert!(rowset.test(10, 1));

    assert!(rowset.test(-10, 2));
    assert!(rowset.test(-5, 2));
    assert!(rowset.test(0, 2));
    assert!(rowset.test(5, 2));
    assert!(rowset.test(10, 2));
}

#[test]
fn test_large_values() {
    let mut rowset = RowSet::new();

    let large1 = i64::MAX;
    let large2 = i64::MAX - 1;
    let large3 = i64::MIN;
    let large4 = i64::MIN + 1;

    rowset.insert(large1).unwrap();
    rowset.insert(large2).unwrap();
    rowset.insert(large3).unwrap();
    rowset.insert(large4).unwrap();

    assert!(rowset.test(large1, 1));
    assert!(rowset.test(large2, 1));
    assert!(rowset.test(large3, 1));
    assert!(rowset.test(large4, 1));

    assert!(rowset.test(large1, 2));
    assert!(rowset.test(large2, 2));
    assert!(rowset.test(large3, 2));
    assert!(rowset.test(large4, 2));
}

#[test]
fn fuzz_basic_operations() {
    // Fuzz test for smallest() extraction: insert random values and verify
    // they are extracted in sorted order without duplicates.
    let seed = get_seed();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let attempts = 10;
    for _ in 0..attempts {
        let mut rowset = RowSet::new();
        let mut inserted = std::collections::BTreeSet::new();

        // Insert random values (may include duplicates)
        let num_inserts = 100 + (rng.next_u64() % 900) as usize;
        for _ in 0..num_inserts {
            let value = rng.next_u64() as i64;
            rowset.insert(value).unwrap();
            inserted.insert(value);
        }

        // Extract all values using smallest()
        let mut extracted = Vec::new();
        while let Some(value) = rowset.smallest() {
            extracted.push(value);
        }

        // Verify all unique values were extracted exactly once
        assert_eq!(extracted.len(), inserted.len());

        // Verify they're in sorted order
        let mut sorted_inserted: Vec<i64> = inserted.iter().copied().try_collect().unwrap();
        sorted_inserted.sort_unstable();
        assert_eq!(extracted, sorted_inserted);
    }
}

#[test]
fn fuzz_batch_operations() {
    // Fuzz test for batch-based consolidation: insert values in distinct batches
    // and verify they can be found after consolidation.
    let seed = get_seed();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let attempts = 10;
    for _ in 0..attempts {
        let mut rowset = RowSet::new();
        let mut batches: Vec<(i32, Vec<i64>)> = crate::alloc::vec![];

        // Create multiple batches: batch 0 (first), intermediate batches (>0), and batch -1 (final)
        let num_batches = 5 + (rng.next_u64() % 10) as usize;
        for batch_idx in 0..num_batches {
            let batch = if batch_idx == 0 {
                0
            } else if batch_idx == num_batches - 1 {
                -1
            } else {
                batch_idx as i32
            };

            // Insert values for this batch
            let mut batch_values = crate::alloc::vec![];
            let num_values = 10 + (rng.next_u64() % 90) as usize;

            for _ in 0..num_values {
                let value = rng.next_u64() as i64;
                rowset.insert(value).unwrap();
                batch_values.push(value);
            }

            batches.push((batch, batch_values));
        }

        // Verify all values can be found when testing with their batch
        for (batch, values) in &batches {
            for &value in values {
                if *batch == 0 {
                    // Batch 0: guaranteed not to contain values
                    assert!(!rowset.test(value, *batch));
                } else {
                    // Other batches: should find values after consolidation
                    assert!(
                        rowset.test(value, *batch),
                        "Value {value} should be found in batch {batch}",
                    );
                }
            }
        }
    }
}

#[test]
fn fuzz_mixed_operations() {
    // Fuzz test mixing insertions and tests: randomly insert values and test them
    // with incrementing batch numbers to verify consolidation works correctly.
    let seed = get_seed();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let attempts = 3;
    for _ in 0..attempts {
        let mut rowset = RowSet::new();
        let mut all_values = std::collections::BTreeSet::new();
        let mut next_batch = 1;

        let num_ops = 20 + (rng.next_u64() % 30) as usize;

        // Randomly interleave insertions and tests
        for _ in 0..num_ops {
            let op = rng.next_u64() % 2;

            match op {
                0 => {
                    // Insert a random value
                    let value = rng.next_u64() as i64;
                    rowset.insert(value).unwrap();
                    all_values.insert(value);
                }
                _ => {
                    // Test a previously inserted value with a new batch number
                    // This triggers consolidation of all fresh values
                    if !all_values.is_empty() {
                        let values_vec: Vec<i64> =
                            all_values.iter().copied().try_collect().unwrap();
                        let idx = (rng.next_u64() % values_vec.len() as u64) as usize;
                        let value = values_vec[idx];

                        let found = rowset.test(value, next_batch);
                        assert!(found, "Value {value} should be found in batch {next_batch}",);
                        next_batch += 1;
                    }
                }
            }
        }

        // Verify all inserted values can be found after final consolidation
        if !all_values.is_empty() {
            let final_batch = next_batch;
            for &value in &all_values {
                assert!(
                    rowset.test(value, final_batch),
                    "Value {value} should be found in batch {final_batch}",
                );
            }
        }
    }
}

#[test]
fn fuzz_long() {
    // Long-running fuzz test: insert values in batches and verify batch consolidation.
    // This tests the core RowSet behavior where values are inserted in distinct phases
    // and consolidated when the batch number changes.
    let seed = get_seed();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    println!("Fuzz seed: {seed}");

    let attempts = 2;
    for attempt in 0..attempts {
        let mut rowset = RowSet::new();
        let mut reference = std::collections::BTreeSet::new();
        let mut batches: Vec<(i32, Vec<i64>)> = crate::alloc::vec![];

        // Generate random number of batches and total inserts
        let num_batches = 10 + (rng.next_u64() % 40) as usize;
        let total_inserts = 1000 + (rng.next_u64() % 9000) as usize;
        let inserts_per_batch = (total_inserts / num_batches).max(1);

        // Insert values in batches
        for batch_idx in 0..num_batches {
            // Determine batch number: 0 for first, -1 for last, sequential for others
            let batch = if batch_idx == 0 {
                0
            } else if batch_idx == num_batches - 1 {
                -1
            } else {
                batch_idx as i32
            };

            // Calculate how many values to insert in this batch
            // (distribute total_inserts across batches with some randomness)
            let mut batch_values = crate::alloc::vec![];
            let already_inserted = batches.iter().map(|(_, v)| v.len()).sum::<usize>();
            let batch_inserts = if batch_idx == num_batches - 1 {
                // Last batch gets remaining values
                total_inserts.saturating_sub(already_inserted)
            } else {
                // Other batches get inserts_per_batch plus some random variation
                let remaining = total_inserts.saturating_sub(already_inserted);
                let max_for_this_batch = remaining.min(inserts_per_batch * 2);
                inserts_per_batch
                    + (rng.next_u64()
                        % (max_for_this_batch.saturating_sub(inserts_per_batch) + 1) as u64)
                        as usize
            };

            // Insert values for this batch
            for _ in 0..batch_inserts {
                let value = rng.next_u64() as i64;
                rowset.insert(value).unwrap();
                reference.insert(value);
                batch_values.push(value);
            }

            // For batches > 0, test some values to verify consolidation works
            if batch > 0 {
                let test_count = (batch_values.len() / 10).max(1);
                for _ in 0..test_count {
                    let idx = (rng.next_u64() % batch_values.len() as u64) as usize;
                    let value = batch_values[idx];

                    let found = rowset.test(value, batch);
                    assert!(
                        found,
                        "Attempt {attempt}, batch {batch}, value {value} should be found",
                    );
                }
            }

            batches.push((batch, batch_values));
        }

        // Final verification: ensure all values can be found after consolidation
        if !reference.is_empty() && !batches.is_empty() {
            let last_batch = batches.last().unwrap().0;
            // Use a new batch number to force final consolidation
            let final_batch = if last_batch == -1 { -1 } else { last_batch + 1 };

            for &value in &reference {
                assert!(
                    rowset.test(value, final_batch),
                    "Attempt {attempt}, value {value} should be found in batch {final_batch}",
                );
            }
        }
    }
}
