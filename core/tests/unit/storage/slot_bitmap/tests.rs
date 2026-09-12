use super::*;
use rand::{rngs::StdRng, Rng, SeedableRng};

fn atomic_free_vec(ab: &AtomicSlotBitmap) -> Vec<bool> {
    (0..ab.n_slots).map(|i| ab.is_free(i)).collect()
}

fn assert_equivalent(ab: &AtomicSlotBitmap, model: &[bool]) {
    let av = atomic_free_vec(ab);
    assert_eq!(av, model, "bitmap bits disagree with reference model");
}

#[test]
fn alloc_one_exhausts_all() {
    let ab = AtomicSlotBitmap::new(256);
    let mut model = vec![true; 256];

    let mut count = 0;
    while let Some(idx) = ab.alloc_one() {
        assert!(model[idx as usize], "must be free in model");
        model[idx as usize] = false;
        count += 1;
    }
    assert_eq!(count, 256, "should allocate all slots once");
    assert!(ab.alloc_one().is_none(), "no slots left");
    assert_equivalent(&ab, &model);
}

#[test]
fn free_one_allows_reuse() {
    let ab = AtomicSlotBitmap::new(128);
    let mut model = vec![true; 128];

    let a = ab.alloc_one().unwrap();
    let b = ab.alloc_one().unwrap();
    model[a as usize] = false;
    model[b as usize] = false;

    ab.free_one(a);
    model[a as usize] = true;
    assert_equivalent(&ab, &model);

    let c = ab.alloc_one().unwrap();
    model[c as usize] = false;
    assert_equivalent(&ab, &model);
}

#[test]
fn freeing_earlier_slot_updates_hint() {
    let ab = AtomicSlotBitmap::new(64);
    let mut allocated = Vec::new();
    while let Some(s) = ab.alloc_one() {
        allocated.push(s);
    }
    let freed = allocated[0];
    ab.free_one(freed);
    assert_eq!(ab.alloc_one(), Some(freed));
}

#[test]
fn fuzz_alloc_free_compare_with_reference_model() {
    let seeds: &[u64] = &[
        std::time::SystemTime::UNIX_EPOCH
            .elapsed()
            .unwrap_or_default()
            .as_secs(),
        1234567890,
        0x69420,
        94822,
        165029,
    ];
    for &seed in seeds {
        let mut rng = StdRng::seed_from_u64(seed);
        let n_slots = rng.random_range(1..10) * 64;
        let ab = AtomicSlotBitmap::new(n_slots);
        let mut model = vec![true; n_slots as usize];

        for _ in 0..2000usize {
            match rng.random_range(0..100) {
                0..=59 => {
                    let got = ab.alloc_one();
                    if let Some(i) = got {
                        assert!(i < n_slots, "index in range");
                        assert!(model[i as usize], "bit must be free");
                        model[i as usize] = false;
                    } else {
                        assert!(
                            !model.iter().any(|&b| b),
                            "allocator returned None but a free slot exists"
                        );
                    }
                }
                _ => {
                    let idx = rng.random_range(0..model.len());
                    if !model[idx] {
                        ab.free_one(idx as u32);
                        model[idx] = true;
                    }
                }
            }
            assert_equivalent(&ab, &model);
        }
    }
}
