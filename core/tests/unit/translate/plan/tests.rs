use crate::alloc::TursoFromIterator;

use super::*;
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

type TestResult = std::result::Result<(), alloc::TryReserveError>;

#[test]
fn test_column_used_mask_empty() -> TestResult {
    let mask = ColumnUsedMask::default();
    assert!(mask.is_empty());

    let mut mask2 = ColumnUsedMask::default();
    mask2.set(0)?;
    assert!(!mask2.is_empty());
    Ok(())
}

#[test]
fn test_column_used_mask_set_and_get() -> TestResult {
    let mut mask = ColumnUsedMask::default();

    let max_columns = 10000;
    let mut set_indices = Vec::new();
    let mut rng = ChaCha8Rng::seed_from_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );

    for i in 0..max_columns {
        if rng.next_u32() % 3 == 0 {
            set_indices.push(i);
            mask.set(i)?;
        }
    }

    // Verify set bits are present
    for &i in &set_indices {
        assert!(mask.get(i), "Expected bit {i} to be set");
    }

    // Verify unset bits are not present
    for i in 0..max_columns {
        if !set_indices.contains(&i) {
            assert!(!mask.get(i), "Expected bit {i} to not be set");
        }
    }
    Ok(())
}

#[test]
fn test_column_used_mask_subset_relationship() -> TestResult {
    let mut full_mask = ColumnUsedMask::default();
    let mut subset_mask = ColumnUsedMask::default();

    let max_columns = 5000;
    let mut rng = ChaCha8Rng::seed_from_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );

    // Create a pattern where subset has fewer bits
    for i in 0..max_columns {
        if rng.next_u32() % 5 == 0 {
            full_mask.set(i)?;
            if i % 2 == 0 {
                subset_mask.set(i)?;
            }
        }
    }

    // full_mask contains all bits of subset_mask
    assert!(full_mask.contains_all_set_bits_of(&subset_mask));

    // subset_mask does not contain all bits of full_mask
    assert!(!subset_mask.contains_all_set_bits_of(&full_mask));

    // A mask contains itself
    assert!(full_mask.contains_all_set_bits_of(&full_mask));
    assert!(subset_mask.contains_all_set_bits_of(&subset_mask));
    Ok(())
}

#[test]
fn test_column_used_mask_empty_subset() -> TestResult {
    let mut mask = ColumnUsedMask::default();
    for i in (0..1000).step_by(7) {
        mask.set(i)?;
    }

    let empty_mask = ColumnUsedMask::default();

    // Empty mask is subset of everything
    assert!(mask.contains_all_set_bits_of(&empty_mask));
    assert!(empty_mask.contains_all_set_bits_of(&empty_mask));
    Ok(())
}

#[test]
fn test_column_used_mask_sparse_indices() -> TestResult {
    let mut sparse_mask = ColumnUsedMask::default();

    // Test with very sparse, large indices
    let sparse_indices = vec![0, 137, 1042, 5389, 10000, 50000, 100000, 500000, 1000000];

    for &idx in &sparse_indices {
        sparse_mask.set(idx)?;
    }

    for &idx in &sparse_indices {
        assert!(sparse_mask.get(idx), "Expected bit {idx} to be set");
    }

    // Check some indices that shouldn't be set
    let unset_indices = vec![1, 100, 1000, 5000, 25000, 75000, 250000, 750000];
    for &idx in &unset_indices {
        assert!(!sparse_mask.get(idx), "Expected bit {idx} to not be set");
    }

    assert!(!sparse_mask.is_empty());
    Ok(())
}

#[test]
fn test_column_used_mask_clear() -> TestResult {
    let mut mask = ColumnUsedMask::default();

    // Test inline clear
    mask.set(5)?;
    mask.set(10)?;
    assert!(mask.get(5));
    mask.clear(5);
    assert!(!mask.get(5));
    assert!(mask.get(10));

    // Test overflow clear
    mask.set(100)?;
    mask.set(200)?;
    assert!(mask.get(100));
    mask.clear(100);
    assert!(!mask.get(100));
    assert!(mask.get(200));

    // Clear non-existent bit should be no-op
    mask.clear(999);
    assert!(!mask.get(999));
    Ok(())
}

#[test]
fn test_column_used_mask_is_only() -> TestResult {
    // Test inline is_only
    let mut mask = ColumnUsedMask::default();
    mask.set(5)?;
    assert!(mask.is_only(5));
    assert!(!mask.is_only(0));
    assert!(!mask.is_only(100));

    mask.set(10)?;
    assert!(!mask.is_only(5));
    assert!(!mask.is_only(10));

    // Test overflow is_only
    let mut mask2 = ColumnUsedMask::default();
    mask2.set(100)?;
    assert!(mask2.is_only(100));
    assert!(!mask2.is_only(0));
    assert!(!mask2.is_only(50));

    mask2.set(200)?;
    assert!(!mask2.is_only(100));

    // Test empty mask
    let empty = ColumnUsedMask::default();
    assert!(!empty.is_only(0));
    assert!(!empty.is_only(100));
    Ok(())
}

#[test]
fn test_column_used_mask_subtract() -> TestResult {
    let mut mask1 = ColumnUsedMask::default();
    let mut mask2 = ColumnUsedMask::default();

    // Set up mask1 with inline and overflow bits
    for i in [1, 5, 10, 63, 64, 100, 200] {
        mask1.set(i)?;
    }

    // Set up mask2 with some overlapping bits
    for i in [5, 10, 100] {
        mask2.set(i)?;
    }

    mask1.subtract(&mask2);

    // Should remain
    assert!(mask1.get(1));
    assert!(mask1.get(63));
    assert!(mask1.get(64));
    assert!(mask1.get(200));

    // Should be cleared
    assert!(!mask1.get(5));
    assert!(!mask1.get(10));
    assert!(!mask1.get(100));
    Ok(())
}

#[test]
fn test_column_used_mask_iter() -> TestResult {
    let mut mask = ColumnUsedMask::default();
    let indices = vec![0, 5, 63, 64, 65, 127, 128, 200, 1000];

    for &i in &indices {
        mask.set(i)?;
    }

    let collected: Vec<usize> = mask.iter().collect();
    assert_eq!(collected, indices);

    // Empty mask iter
    let empty = ColumnUsedMask::default();
    assert_eq!(empty.iter().count(), 0);
    Ok(())
}

#[test]
fn test_column_used_mask_bitor_assign() -> TestResult {
    let mut mask1 = ColumnUsedMask::default();
    let mut mask2 = ColumnUsedMask::default();

    // Inline bits
    mask1.set(1)?;
    mask1.set(5)?;
    mask2.set(5)?;
    mask2.set(10)?;

    // Overflow bits
    mask1.set(100)?;
    mask2.set(200)?;

    mask1.union_with(&mask2)?;

    assert!(mask1.get(1));
    assert!(mask1.get(5));
    assert!(mask1.get(10));
    assert!(mask1.get(100));
    assert!(mask1.get(200));

    // mask2 should be unchanged
    assert!(!mask2.get(1));
    assert!(mask2.get(5));
    assert!(mask2.get(10));
    assert!(!mask2.get(100));
    assert!(mask2.get(200));
    Ok(())
}

#[test]
fn test_column_used_mask_boundary_conditions() -> TestResult {
    let mut mask = ColumnUsedMask::default();

    // Test at inline/overflow boundary
    mask.set(63)?; // last inline bit
    mask.set(64)?; // first overflow bit

    assert!(mask.get(63));
    assert!(mask.get(64));
    assert!(!mask.get(62));
    assert!(!mask.get(65));

    // Test is_only at boundary
    let mut mask2 = ColumnUsedMask::default();
    mask2.set(63)?;
    assert!(mask2.is_only(63));

    let mut mask3 = ColumnUsedMask::default();
    mask3.set(64)?;
    assert!(mask3.is_only(64));
    Ok(())
}

#[test]
fn test_column_mask_rowid_sentinel() -> TestResult {
    // ColumnMask stores `usize::MAX` (ROWID_SENTINEL) in an out-of-band bool
    // so that the underlying dense BitSet never sees it. The small API surface
    // that ColumnMask exposes must all honor the sentinel consistently.

    // set / get round-trip on the sentinel alone
    let mut mask = ColumnMask::default();
    assert!(!mask.get(usize::MAX));
    mask.set(usize::MAX)?;
    assert!(mask.get(usize::MAX));
    assert_eq!(mask.count(), 1);

    // sentinel coexists with dense bits
    let mut mixed = ColumnMask::default();
    mixed.set(0)?;
    mixed.set(63)?;
    mixed.set(64)?; // crosses into overflow
    mixed.set(500)?;
    mixed.set(usize::MAX)?;
    assert!(mixed.get(0));
    assert!(mixed.get(63));
    assert!(mixed.get(64));
    assert!(mixed.get(500));
    assert!(mixed.get(usize::MAX));
    assert_eq!(mixed.count(), 5);

    // iter yields dense positions in ascending order, then usize::MAX at the end
    let collected: Vec<usize> = (&mixed).into_iter().collect();
    assert_eq!(collected, vec![0, 63, 64, 500, usize::MAX]);
    // count() and iter().count() must agree
    assert_eq!(mixed.count(), (&mixed).into_iter().count());

    // fallible collection round-trip through the sentinel
    let built = ColumnMask::try_from_iter([0usize, 63, 64, 500, usize::MAX])?;
    assert_eq!(built, mixed);
    let round = ColumnMask::try_from_iter(&mixed)?;
    assert_eq!(round, mixed);
    let mut extended = ColumnMask::default();
    extended.try_extend([0usize, 63, 64, 500, usize::MAX])?;
    assert_eq!(extended, mixed);

    // owned IntoIterator (used by flat_map in the UPDATE emitter)
    let mixed_owned: Vec<usize> = mixed.clone().into_iter().collect();
    assert_eq!(mixed_owned, vec![0, 63, 64, 500, usize::MAX]);
    Ok(())
}

fn rng_from_env_or_time() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    (ChaCha8Rng::seed_from_u64(seed), seed)
}

/// Reference implementation using BTreeSet for correctness comparison
struct ReferenceMask(std::collections::BTreeSet<usize>);

impl ReferenceMask {
    fn new() -> Self {
        Self(std::collections::BTreeSet::new())
    }
    fn set(&mut self, index: usize) {
        self.0.insert(index);
    }
    fn get(&self, index: usize) -> bool {
        self.0.contains(&index)
    }
    fn clear(&mut self, index: usize) {
        self.0.remove(&index);
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn is_only(&self, index: usize) -> bool {
        self.0.len() == 1 && self.0.contains(&index)
    }
    fn contains_all_set_bits_of(&self, other: &Self) -> bool {
        other.0.is_subset(&self.0)
    }
    fn subtract(&mut self, other: &Self) {
        for &idx in &other.0 {
            self.0.remove(&idx);
        }
    }
    fn bitor_assign(&mut self, other: &Self) {
        for &idx in &other.0 {
            self.0.insert(idx);
        }
    }
}

#[test]
fn test_column_used_mask_fuzz() -> TestResult {
    fn pick_index(rng: &mut ChaCha8Rng, max_index: u32) -> usize {
        (rng.next_u32() % max_index) as usize
    }

    let (mut rng, seed) = rng_from_env_or_time();
    eprintln!("test_column_used_mask_random_ops seed: {seed}");

    let mut mask = ColumnUsedMask::default();
    let mut reference = ReferenceMask::new();

    let num_ops = 100000;
    let max_index = 4096;

    for _ in 0..num_ops {
        let op = rng.next_u32() % 10;
        let idx = pick_index(&mut rng, max_index);

        match op {
            0..=2 => {
                // Set (more frequent)
                mask.set(idx)?;
                reference.set(idx);
            }
            3 => {
                // Get
                assert_eq!(
                    mask.get(idx),
                    reference.get(idx),
                    "get({idx}) mismatch, seed={seed}"
                );
            }
            4 => {
                // Clear
                mask.clear(idx);
                reference.clear(idx);
            }
            5 => {
                // IsEmpty
                assert_eq!(
                    mask.is_empty(),
                    reference.is_empty(),
                    "is_empty mismatch, seed={seed}"
                );
            }
            6 => {
                // IsOnly
                assert_eq!(
                    mask.is_only(idx),
                    reference.is_only(idx),
                    "is_only({idx}) mismatch, seed={seed}"
                );
            }
            7 => {
                // ContainsAllSetBitsOf with random other mask
                let mut other_mask = ColumnUsedMask::default();
                let mut other_ref = ReferenceMask::new();
                for _ in 0..(rng.next_u32() % 20) {
                    let other_idx = pick_index(&mut rng, max_index);
                    other_mask.set(other_idx)?;
                    other_ref.set(other_idx);
                }
                assert_eq!(
                    mask.contains_all_set_bits_of(&other_mask),
                    reference.contains_all_set_bits_of(&other_ref),
                    "contains_all_set_bits_of mismatch, seed={seed}"
                );
            }
            8 => {
                // BitOrAssign with random other mask
                let mut other_mask = ColumnUsedMask::default();
                let mut other_ref = ReferenceMask::new();
                for _ in 0..(rng.next_u32() % 20) {
                    let other_idx = pick_index(&mut rng, max_index);
                    other_mask.set(other_idx)?;
                    other_ref.set(other_idx);
                }
                mask.union_with(&other_mask)?;
                reference.bitor_assign(&other_ref);
            }
            9 => {
                // Subtract with random other mask
                let mut other_mask = ColumnUsedMask::default();
                let mut other_ref = ReferenceMask::new();
                for _ in 0..(rng.next_u32() % 20) {
                    let other_idx = pick_index(&mut rng, max_index);
                    other_mask.set(other_idx)?;
                    other_ref.set(other_idx);
                }
                mask.subtract(&other_mask);
                reference.subtract(&other_ref);
            }
            _ => unreachable!(),
        }
    }

    // Final verification: iter should produce same results
    let mask_set: std::collections::BTreeSet<usize> = mask.iter().collect();
    assert_eq!(mask_set, reference.0, "final iter mismatch, seed={seed}");
    Ok(())
}

#[test]
fn test_bitset_properties_fuzz() -> TestResult {
    fn sample_other(
        rng: &mut ChaCha8Rng,
        max_index: usize,
    ) -> Result<(BitSet, std::collections::BTreeSet<usize>), alloc::TryReserveError> {
        let mut m = BitSet::default();
        let mut r = std::collections::BTreeSet::new();
        for _ in 0..(rng.next_u32() % 20) {
            let i = (rng.next_u32() as usize) % max_index;
            m.set(i)?;
            r.insert(i);
        }
        Ok((m, r))
    }

    let (mut rng, seed) = rng_from_env_or_time();
    eprintln!("test_bitset_properties_fuzz seed: {seed}");

    let mut mask = BitSet::default();
    let mut reference = std::collections::BTreeSet::<usize>::new();
    let max_index: usize = 2048;
    let num_ops = 30_000;

    for step in 0..num_ops {
        let op = rng.next_u32() % 16;
        let idx = (rng.next_u32() as usize) % max_index;

        match op {
            0..=3 => {
                // Set (weighted to grow the set)
                mask.set(idx)?;
                reference.insert(idx);
            }
            4 => {
                // Clear
                mask.clear(idx);
                reference.remove(&idx);
            }
            5 => {
                // count() agrees with reference size
                assert_eq!(
                    mask.count(),
                    reference.len(),
                    "step={step} seed={seed} op=count"
                );
            }
            6 => {
                // rank(k) agrees with |{x in ref : x < k}|
                let expected = reference.range(..idx).count();
                assert_eq!(
                    mask.rank(idx),
                    expected,
                    "step={step} seed={seed} op=rank({idx})"
                );
            }
            7 => {
                // intersects() agrees with BTreeSet intersection
                let (other_mask, other_ref) = sample_other(&mut rng, max_index)?;
                let expected = reference.intersection(&other_ref).next().is_some();
                assert_eq!(
                    mask.intersects(&other_mask),
                    expected,
                    "step={step} seed={seed} op=intersects"
                );
                // Symmetry: intersects is commutative
                assert_eq!(
                    other_mask.intersects(&mask),
                    expected,
                    "step={step} seed={seed} op=intersects-symmetric"
                );
            }
            8 => {
                // Fallible collection: building a fresh BitSet from the reference
                // must compare equal to the mask.
                let built = BitSet::try_from_iter(reference.iter().copied())?;
                assert_eq!(built, mask, "step={step} seed={seed} op=try_from_iter");
            }
            9 => {
                // iter() -> try_from_iter() round trip is the identity
                let round = BitSet::try_from_iter(mask.iter())?;
                assert_eq!(round, mask, "step={step} seed={seed} op=iter-roundtrip");

                // iter() yields bits in strictly increasing order, matching the reference
                let collected: Vec<usize> = mask.iter().collect();
                for w in collected.windows(2) {
                    assert!(
                        w[0] < w[1],
                        "step={step} seed={seed} iter not strictly increasing"
                    );
                }
                let ref_vec: Vec<usize> = reference.iter().copied().collect();
                assert_eq!(
                    collected, ref_vec,
                    "step={step} seed={seed} iter contents vs ref"
                );
            }
            10 => {
                // TryFrom<u128>: sample a random u128, verify per-bit and count
                let val = ((rng.next_u32() as u128) << 96)
                    | ((rng.next_u32() as u128) << 64)
                    | ((rng.next_u32() as u128) << 32)
                    | (rng.next_u32() as u128);
                let bs = BitSet::try_from(val)?;
                assert_eq!(
                    bs.count(),
                    val.count_ones() as usize,
                    "step={step} seed={seed} TryFrom<u128>({val:#x}) count"
                );
                for i in 0..128 {
                    let expected = (val >> i) & 1 != 0;
                    assert_eq!(
                        bs.get(i),
                        expected,
                        "step={step} seed={seed} TryFrom<u128>({val:#x}) get({i})"
                    );
                }
                // Path equivalence: same bits via set() must compare equal
                let mut manual = BitSet::default();
                for i in 0..128 {
                    if (val >> i) & 1 != 0 {
                        manual.set(i)?;
                    }
                }
                assert_eq!(
                    bs, manual,
                    "step={step} seed={seed} TryFrom<u128>({val:#x}) vs manual"
                );
                // TryFrom<u128>(0) must equal default (equality anchor)
                assert_eq!(
                    BitSet::<usize>::try_from(0u128)?,
                    BitSet::<usize>::default(),
                    "step={step} seed={seed} TryFrom<u128>(0) != default"
                );
            }
            11 => {
                // SubAssign (delegates to subtract)
                let (other_mask, other_ref) = sample_other(&mut rng, max_index)?;
                mask -= &other_mask;
                for i in &other_ref {
                    reference.remove(i);
                }
            }
            12 => {
                // union_with
                let (other_mask, other_ref) = sample_other(&mut rng, max_index)?;
                mask.union_with(&other_mask)?;
                for i in other_ref {
                    reference.insert(i);
                }
            }
            13 => {
                // Cross-method: count() == iter().count() == rank(usize::MAX)
                let c = mask.count();
                assert_eq!(
                    c,
                    mask.iter().count(),
                    "step={step} seed={seed} count vs iter().count()"
                );
                assert_eq!(
                    c,
                    mask.rank(usize::MAX),
                    "step={step} seed={seed} count vs rank(MAX)"
                );
            }
            14 => {
                // Cross-method: contains_all(other) && !other.is_empty() => intersects(other)
                let (other_mask, other_ref) = sample_other(&mut rng, max_index)?;
                if mask.contains_all_set_bits_of(&other_mask) && !other_ref.is_empty() {
                    assert!(
                        mask.intersects(&other_mask),
                        "step={step} seed={seed} contains_all should imply intersects"
                    );
                }
            }
            15 => {
                // Cross-method: is_empty() iff count() == 0
                assert_eq!(
                    mask.is_empty(),
                    mask.count() == 0,
                    "step={step} seed={seed} is_empty vs count==0"
                );
                assert_eq!(
                    mask.is_empty(),
                    reference.is_empty(),
                    "step={step} seed={seed} is_empty vs ref"
                );
            }
            _ => unreachable!(),
        }
    }

    // Final verification: complete iter vs reference, and count agreement
    let collected: std::collections::BTreeSet<usize> = mask.iter().collect();
    assert_eq!(collected, reference, "final iter mismatch, seed={seed}");
    assert_eq!(
        mask.count(),
        reference.len(),
        "final count mismatch, seed={seed}"
    );
    Ok(())
}

#[test]
fn test_bitset_with_table_internal_id() -> TestResult {
    let a = TableInternalId::from(3);
    let b = TableInternalId::from(70); // exercises overflow path
    let c = TableInternalId::from(200);

    let mut mask: BitSet<TableInternalId> = BitSet::default();
    mask.set(a)?;
    mask.set(b)?;
    mask.set(c)?;

    assert!(mask.get(a));
    assert!(mask.get(b));
    assert!(mask.get(c));
    assert!(!mask.get(TableInternalId::from(4)));
    assert_eq!(mask.count(), 3);

    mask.clear(b);
    assert!(!mask.get(b));
    assert_eq!(mask.count(), 2);

    // Iterator yields TableInternalId, not usize.
    let collected: Vec<TableInternalId> = (&mask).into_iter().collect();
    assert_eq!(collected, vec![a, c]);

    // Fallible collection preserves TableInternalId.
    let rebuilt = BitSet::<TableInternalId>::try_from_iter([a, c])?;
    assert_eq!(rebuilt, mask);
    let mut extended = BitSet::<TableInternalId>::default();
    extended.try_extend([a, c])?;
    assert_eq!(extended, mask);
    Ok(())
}

#[test]
fn test_column_mask_sub_assign() -> TestResult {
    let mut a = ColumnMask::try_from_iter([1, 3, ROWID_SENTINEL])?;
    let b = ColumnMask::try_from_iter([3, ROWID_SENTINEL])?;
    a -= &b;
    assert!(a.get(1));
    assert!(!a.get(3));
    assert!(!a.get(ROWID_SENTINEL));
    assert_eq!(a.count(), 1);

    // Subtracting without rowid sentinel leaves it intact
    let mut a = ColumnMask::try_from_iter([2, 4, ROWID_SENTINEL])?;
    let b = ColumnMask::try_from_iter([2])?;
    a -= &b;
    assert!(!a.get(2));
    assert!(a.get(4));
    assert!(a.get(ROWID_SENTINEL));
    assert_eq!(a.count(), 2);
    Ok(())
}
