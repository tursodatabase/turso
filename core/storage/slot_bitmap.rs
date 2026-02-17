use crate::turso_assert;

#[derive(Debug)]
/// Fixed-size bitmap for tracking allocated slots in an arena.
///
/// Bit meaning:
/// - 1 = free
/// - 0 = allocated
pub(super) struct SlotBitmap {
    words: Box<[u64]>,
    n_slots: u32,
    /// Hint for where the next allocation scan should start (word index).
    next_word: usize,
}

impl SlotBitmap {
    /// 64 bits per word, so shift by 6 to get slot index.
    const WORD_SHIFT: u32 = 6;
    const WORD_BITS: u32 = 64;
    const WORD_MASK: u32 = 63;

    const ALL_FREE: u64 = u64::MAX;
    const ALL_ALLOCATED: u64 = 0u64;

    /// Creates a new `SlotBitmap` capable of tracking `n_slots` slots.
    pub fn new(n_slots: u32) -> Self {
        turso_assert!(n_slots > 0, "number of slots must be non-zero");
        turso_assert!(
            n_slots % Self::WORD_BITS == 0,
            "number of slots in map must be a multiple of 64"
        );
        let n_words = (n_slots / Self::WORD_BITS) as usize;
        let words = vec![Self::ALL_FREE; n_words].into_boxed_slice();
        Self {
            words,
            n_slots,
            next_word: 0,
        }
    }

    #[inline]
    const fn word_and_bit_to_slot(word_idx: usize, bit: u32) -> u32 {
        (word_idx as u32) << Self::WORD_SHIFT | bit
    }

    #[inline]
    const fn slot_to_word_and_bit(slot_idx: u32) -> (usize, u32) {
        (
            (slot_idx >> Self::WORD_SHIFT) as usize,
            slot_idx & Self::WORD_MASK,
        )
    }

    /// Returns whether a slot is currently free.
    pub(super) fn is_free(&self, slot: u32) -> bool {
        if slot >= self.n_slots {
            return false;
        }
        let (word_idx, bit) = Self::slot_to_word_and_bit(slot);
        (self.words[word_idx] & (1u64 << bit)) != 0
    }

    /// Allocates a single free slot from the bitmap.
    pub fn alloc_one(&mut self) -> Option<u32> {
        let n_words = self.words.len();
        if n_words == 0 {
            return None;
        }

        let mut start = self.next_word.min(n_words - 1);
        for _pass in 0..2 {
            for word_idx in start..n_words {
                let word = self.words[word_idx];
                if word == Self::ALL_ALLOCATED {
                    continue;
                }
                let bit = word.trailing_zeros();
                self.words[word_idx] &= !(1u64 << bit);

                if self.words[word_idx] == Self::ALL_ALLOCATED {
                    self.next_word = (word_idx + 1) % n_words;
                } else {
                    self.next_word = word_idx;
                }

                return Some(Self::word_and_bit_to_slot(word_idx, bit));
            }
            start = 0;
        }
        None
    }

    /// Frees a previously allocated slot.
    pub fn free_one(&mut self, slot: u32) {
        turso_assert!(slot < self.n_slots, "free_one out of bounds");
        let (word_idx, bit) = Self::slot_to_word_and_bit(slot);
        let mask = 1u64 << bit;
        turso_assert!((self.words[word_idx] & mask) == 0, "slot already free");
        self.words[word_idx] |= mask;
        if word_idx < self.next_word {
            self.next_word = word_idx;
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    fn pb_free_vec(pb: &SlotBitmap) -> Vec<bool> {
        let mut v = vec![false; pb.n_slots as usize];
        v.iter_mut()
            .enumerate()
            .take(pb.n_slots as usize)
            .map(|(i, v)| {
                let w = pb.words[i >> 6];
                let bit = i & 63;
                *v = (w & (1u64 << bit)) != 0;
                *v
            })
            .collect()
    }

    /// Returns `true` if the bitmap's notion of free bits equals the reference model exactly.
    fn assert_equivalent(pb: &SlotBitmap, model: &[bool]) {
        let pv = pb_free_vec(pb);
        assert_eq!(pv, model, "bitmap bits disagree with reference model");
    }

    #[test]
    fn alloc_one_exhausts_all() {
        let mut pb = SlotBitmap::new(256);
        let mut model = vec![true; 256];

        let mut count = 0;
        while let Some(idx) = pb.alloc_one() {
            assert!(model[idx as usize], "must be free in model");
            model[idx as usize] = false;
            count += 1;
        }
        assert_eq!(count, 256, "should allocate all slots once");
        assert!(pb.alloc_one().is_none(), "no slots left");
        assert_equivalent(&pb, &model);
    }

    #[test]
    fn free_one_allows_reuse() {
        let mut pb = SlotBitmap::new(128);
        let mut model = vec![true; 128];

        let a = pb.alloc_one().unwrap();
        let b = pb.alloc_one().unwrap();
        model[a as usize] = false;
        model[b as usize] = false;

        pb.free_one(a);
        model[a as usize] = true;
        assert_equivalent(&pb, &model);

        let c = pb.alloc_one().unwrap();
        model[c as usize] = false;
        assert_equivalent(&pb, &model);
    }

    #[test]
    fn freeing_earlier_slot_updates_hint() {
        let mut pb = SlotBitmap::new(64);
        let mut allocated = Vec::new();
        while let Some(s) = pb.alloc_one() {
            allocated.push(s);
        }
        let freed = allocated[0];
        pb.free_one(freed);
        assert_eq!(pb.alloc_one(), Some(freed));
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
            let mut pb = SlotBitmap::new(n_slots);
            let mut model = vec![true; n_slots as usize];

            for _ in 0..2000usize {
                match rng.random_range(0..100) {
                    0..=59 => {
                        let got = pb.alloc_one();
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
                        // Free a random allocated slot (if any).
                        let idx = rng.random_range(0..model.len());
                        if !model[idx] {
                            pb.free_one(idx as u32);
                            model[idx] = true;
                        }
                    }
                }
                assert_equivalent(&pb, &model);
            }
        }
    }
}
