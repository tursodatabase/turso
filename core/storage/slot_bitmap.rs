use crate::turso_assert;

#[derive(Debug)]
/// Immutable-size bitmap for use in tracking allocated slots from an arena.
pub(super) struct SlotBitmap {
    /// 1 = free, 0 = allocated
    words: Box<[u64]>,
    /// total capacity of slots in the arena
    n_slots: u32,
    /// where single allocations search downward from
    scan_one_high: u32,
    /// where runs search upward from
    scan_run_low: u32,
}

///
/// ```text
///                          bitmap
/// -----------|----------------------------------------------|-----
///        low ^ scan_run_low                   scan_one_high ^ high
/// ```
///
/// There are two hints or 'pointers' where we begin scanning for free slots,
/// one on each end of the map. This strategy preserves contiguous free space
/// at the low end of the arena, which is beneficial for allocating many contiguous
/// buffers that can be coalesced into fewer I/O operations.
///
/// Single-slot allocations (`alloc_one`) start searching from `scan_one_high` and work
/// downward, preserving large contiguous runs at the low end. When the hint area is
/// exhausted, the search wraps around to check from the top of the bitmap.
///
/// Run allocations (`alloc_run`) scan upward from `scan_run_low`, making it likely
/// that sequential allocations will be physically contiguous.
///
/// After freeing slots, we update hints to encourage reuse:
/// - If freed slots are below `scan_run_low`, we move it down to include them
/// - If freed slots are above `scan_one_high`, we move it up to include them
///
///```ignore
/// let mut bitmap = SlotBitmap::new(128);
///
/// // Single allocations come from high end
/// assert_eq!(bitmap.alloc_one(), Some(127));
/// assert_eq!(bitmap.alloc_one(), Some(126));
///
/// // Run allocations come from low end
/// assert_eq!(bitmap.alloc_run(10), Some(0));
/// assert_eq!(bitmap.alloc_run(10), Some(10));
///
/// // Free and reallocate
/// bitmap.free_run(5, 3);
/// assert_eq!(bitmap.alloc_run(3), Some(5)); // Reuses freed space
/// ```
impl SlotBitmap {
    /// 64 bits per word, so shift by 6 to get slot index
    const WORD_SHIFT: u32 = 6;
    const WORD_BITS: u32 = 64;
    const WORD_MASK: u32 = 63;
    const ALL_FREE: u64 = u64::MAX;
    const ALL_ALLOCATED: u64 = 0u64;

    const ALLOC: bool = false;
    const FREE: bool = true;

    /// Creates a new `SlotBitmap` capable of tracking `n_slots` slots.
    ///
    /// If `n_slots` is not a multiple of 64, the trailing bits in the last
    /// word are marked as allocated to prevent out-of-bounds allocations.
    pub fn new(n_slots: u32) -> Self {
        turso_assert!(
            n_slots % 64 == 0,
            "number of slots in map must be a multiple of 64"
        );
        let n_words = (n_slots / Self::WORD_BITS) as usize;
        let words = vec![Self::ALL_FREE; n_words].into_boxed_slice();

        Self {
            words,
            n_slots,
            scan_run_low: 0,
            scan_one_high: n_slots - 1,
        }
    }

    #[inline]
    /// Convert word index and bit offset to slot index
    /// Example:
    /// word_idx: 1, bit: 10
    /// shift the word index by WORD_SHIFT and OR with `bit` to get the slot
    /// Slot number: 1 << 6 | 10   =  slot index: 74
    const fn word_and_bit_to_slot(word_idx: usize, bit: u32) -> u32 {
        (word_idx as u32) << Self::WORD_SHIFT | bit
    }

    #[inline]
    /// Convert slot index to word index and bit offset
    /// Example
    /// Slot number: 74
    /// (74 >> 6, 74 & 63) = (1, 10)
    const fn slot_to_word_and_bit(slot_idx: u32) -> (usize, u32) {
        (
            (slot_idx >> Self::WORD_SHIFT) as usize,
            slot_idx & Self::WORD_MASK,
        )
    }

    /// Mask to ignore bits below `bit` inclusive for upward scans.
    #[inline]
    const fn mask_from(bit: u32) -> u64 {
        u64::MAX << bit
    }

    /// Mask to ignore bits above `bit` for downward scans (keep [0..=bit]).
    #[inline]
    const fn mask_through(bit: u32) -> u64 {
        if bit == 63 {
            u64::MAX
        } else {
            (1u64 << (bit + 1)) - 1
        }
    }

    /// Count consecutive forward free bits starting at `bit` in `word`.
    /// Returns number of 1s (0..=64-pos).
    #[inline]
    const fn run_len_from(word: u64, bit: u32) -> u32 {
        // Shift so `bit` becomes LSB, then count trailing ones.
        // trailing ones = trailing_zeros of the inverted value.
        (!(word >> bit)).trailing_zeros()
    }

    /// Allocates a single free slot from the bitmap.
    ///
    /// This method scans from high to low addresses to preserve contiguous
    /// runs of free slots at the low end of the bitmap.
    pub fn alloc_one(&mut self) -> Option<u32> {
        for start in [self.scan_one_high, self.n_slots - 1] {
            let (mut word_idx, bit) = Self::slot_to_word_and_bit(start);
            let mut word = self.words[word_idx] & Self::mask_through(bit);
            if word != Self::ALL_ALLOCATED {
                // Fast path: pick highest set bit in this masked word
                let bit = 63 - word.leading_zeros();
                self.words[word_idx] &= !(1u64 << bit);
                let slot = Self::word_and_bit_to_slot(word_idx, bit);
                self.scan_one_high = slot.saturating_sub(1);
                return Some(slot);
            }
            // Walk lower words
            while word_idx > 0 {
                word_idx -= 1;
                word = self.words[word_idx];
                if word != Self::ALL_ALLOCATED {
                    let bits = 63 - word.leading_zeros();
                    self.words[word_idx] &= !(1u64 << bits);
                    let slot = Self::word_and_bit_to_slot(word_idx, bits);
                    self.scan_one_high = slot.saturating_sub(1);
                    return Some(slot);
                }
            }
            if self.scan_one_high == self.n_slots - 1 {
                // dont try again if we already started there
                return None;
            }
        }
        None
    }

    /// Allocates a contiguous run of `need` slots from the bitmap.
    /// This method scans from low to high addresses, starting from `scan_run_low`,
    /// next allocation continues from where we left off by updating the hint.
    pub fn alloc_run(&mut self, need: u32) -> Option<u32> {
        if need == 0 || need > self.n_slots {
            return None;
        }
        if need == 1 {
            return self.alloc_one();
        }
        // Try from hint first, then from start if different
        for &start_pos in &[self.scan_run_low, 0] {
            if let Some(found) = self.find_free_run_up(start_pos, need) {
                self.mark_run(found, need, Self::ALLOC);
                self.scan_run_low = found + need;
                // Update single-slot hint if this run extends beyond it
                let last_slot = found + need - 1;
                if last_slot > self.scan_one_high {
                    self.scan_one_high = last_slot.min(self.n_slots - 1);
                }
                return Some(found);
            }
            // Don't search from 0 if we already started there
            if start_pos == 0 {
                break;
            }
        }
        None
    }

    /// Find an unallocated sequence of `need` slots, scanning *upward* from `start`
    ///
    /// Overview:
    /// Set limit = n_slots - (need - 1): as the most slots we could iterate through
    /// and iterate `pos` while `pos < limit`.
    ///
    /// Split pos into (word_idx, bit_offset) and compute to keep bits at/after `bit_offset`.
    ///
    /// If zero: no free bit in this word at or after `pos`, so we jump to next word boundary:
    /// pos = (word_idx + 1) << WORD_SHIFT and continue.
    ///
    /// Otherwise, locate the first free bit at or after `run_start` position, and measure the free span in the
    /// word starting at `first` to get `free_in_curr`.
    ///
    /// If there are enough consecutive free bits and the entire run fits in this word, then return our `run_start`.
    /// If `first + free_in_cur < 64`: then the free span ends before the word boundary, and the run cannot
    /// bridge into the next word so we advance to the next.
    ///
    /// If the span reaches the boundary exactly, we try to extend across, consume subsequent words while
    /// `is_full_free(word)` in 64-slot chunks and return if `need` is satisfied.
    ///
    /// If still short, then check the tail in the next (partial) word and if tail >= remaining: return.
    /// otherwise advance `pos = run_start + slots_found + 1`.
    fn find_free_run_up(&self, start: u32, need: u32) -> Option<u32> {
        let limit = self.n_slots.saturating_sub(need - 1);
        let mut pos = start;
        while pos < limit {
            let (word_idx, bit_offset) = Self::slot_to_word_and_bit(pos);

            // Current word from bit_offset onward
            let current_word = self.words[word_idx] & Self::mask_from(bit_offset);
            if current_word == 0 {
                // Jump to next word boundary
                pos = ((word_idx + 1) as u32) << Self::WORD_SHIFT;
                continue;
            }

            // First free bit >= pos
            let first_free_bit = current_word.trailing_zeros();
            let run_start = ((word_idx as u32) << Self::WORD_SHIFT) + first_free_bit;

            // Free span within this word
            let free_in_cur = Self::run_len_from(self.words[word_idx], first_free_bit);
            if free_in_cur >= need {
                return Some(run_start);
            }

            // If we didn't reach the word boundary, the run is broken here.
            if first_free_bit + free_in_cur < Self::WORD_BITS {
                pos = run_start + free_in_cur + 1;
                continue;
            }

            // We exactly reached the boundary, extend across whole free words
            let mut slots_found = free_in_cur;
            let mut wi = word_idx + 1;

            while slots_found + Self::WORD_BITS <= need
                && wi < self.words.len()
                && self.words[wi] == Self::ALL_FREE
            {
                slots_found += Self::WORD_BITS;
                wi += 1;
            }
            if slots_found >= need {
                return Some(run_start);
            }

            // Tail in the next partial word
            if wi < self.words.len() {
                let remaining = need - slots_found;
                let w = self.words[wi];
                let tail = (!w).trailing_zeros();
                if tail >= remaining {
                    return Some(run_start);
                }
                // Run broken in this word: skip past the failure point to avoid checking again
                pos = run_start + slots_found + 1;
                continue;
            }
            // No space
            return None;
        }
        None
    }

    /// Frees a contiguous run of slots, marking them as available for reallocation
    /// and update the scan hints to  allocations.
    pub fn free_run(&mut self, start: u32, count: u32) {
        if count == 0 {
            return;
        }
        turso_assert!(start + count <= self.n_slots, "free_run out of bounds");
        self.mark_run(start, count, Self::FREE);
        // Update hints based on what we're freeing

        // if this was a single slot and higher than current hint, bump the high hint up
        if count == 1 && start > self.scan_one_high {
            self.scan_one_high = start;
            return;
        }

        if start < self.scan_run_low {
            self.scan_run_low = start;
        }
        // Also update scan_one_high hint if the run extends beyond it
        let last_slot = (start + count - 1).min(self.n_slots - 1);
        if last_slot > self.scan_one_high {
            self.scan_one_high = last_slot;
        }
    }

    /// Checks whether a contiguous run of slots is completely free.
    /// # Example
    /// Checking slots 125-134 (10 slots starting at 125)
    /// This spans from word 1 (bit 61) to word 2 (bit 6)
    ///
    /// Word 1: ...11111111_11110000  (bits 60-63 must be checked)
    /// Word 2: 00000000_01111111...  (bits 0-6 must be checked)
    pub(super) fn check_run_free(&self, start: u32, len: u32) -> bool {
        if start + len > self.n_slots {
            return false;
        }
        let (mut word_idx, bit_offset) = Self::slot_to_word_and_bit(start);
        let mut remaining = len as usize;
        let mut pos_in_word = bit_offset as usize;

        // process words until we have checked `len` bits
        while remaining > 0 {
            // Calculate how many bits to check in the current word
            // This is either the remaining bits needed, or the bits left in the current word
            // Example: if pos_in_word = 61 and remaining = 10, we can only check 3 bits in this word
            let bits_to_process = remaining.min((Self::WORD_BITS as usize) - pos_in_word);

            // Create a mask with 1s in the positions we want to check
            let mask = if bits_to_process == Self::WORD_BITS as usize {
                Self::ALL_FREE
            } else {
                // Create mask with `bits_to_process` 1's, shifted to start at `pos_in_word`
                // Example: bits_to_process = 3, pos_in_word = 61
                // (1 << 3) - 1 = 0b111
                // 0b111 << 61 = puts three 1s at positions 61, 62, 63
                ((1u64 << bits_to_process) - 1) << pos_in_word
            };
            // If all bits under the mask are 1 (free), then word & mask == mask
            if (self.words[word_idx] & mask) != mask {
                return false;
            }
            remaining -= bits_to_process;
            word_idx += 1;
            pos_in_word = 0;
        }
        true
    }

    /// Marks a contiguous run of slots as either free or allocated.
    pub fn mark_run(&mut self, start: u32, len: u32, free: bool) {
        turso_assert!(start + len <= self.n_slots, "mark_run out of bounds");

        let (mut word_idx, mut bit_offset) = Self::slot_to_word_and_bit(start);
        let mut remaining = len as usize;
        while remaining > 0 {
            let bits = (Self::WORD_BITS as usize) - bit_offset as usize;
            let take = remaining.min(bits);
            let mask = if take == Self::WORD_BITS as usize {
                Self::ALL_FREE
            } else {
                ((1u64 << take) - 1) << bit_offset
            };
            if free {
                self.words[word_idx] |= mask;
            } else {
                self.words[word_idx] &= !mask;
            }
            remaining -= take;
            word_idx += 1;
            bit_offset = 0;
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

    /// Count free bits in the reference model.
    fn ref_count_free(model: &[bool]) -> usize {
        model.iter().filter(|&&b| b).count()
    }

    /// Check whether [start, start+len) are all free in the reference model.
    fn ref_check_run_free(model: &[bool], start: u32, len: u32) -> bool {
        let s = start as usize;
        let l = len as usize;
        if s + l > model.len() {
            return false;
        }
        model[s..s + l].iter().all(|&b| b)
    }

    /// Mark [start, start+len) free(=true) or allocated(=false) in the reference model.
    fn ref_mark_run(model: &mut [bool], start: u32, len: u32, free: bool) {
        let st = start as usize;
        let len = len as usize;
        for slot in &mut model[st..st + len] {
            *slot = free;
        }
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
    fn alloc_run_basic_and_free() {
        let mut pb = SlotBitmap::new(128);
        let mut model = vec![true; 128];

        let need = 70;
        let start = pb.alloc_run(need).expect("expected a run");
        assert!(ref_check_run_free(&model, start, need));
        ref_mark_run(&mut model, start, need, false);
        assert!(!pb.check_run_free(start, need));
        assert_equivalent(&pb, &model);

        pb.free_run(start, need);
        ref_mark_run(&mut model, start, need, true);
        assert!(pb.check_run_free(start, need));
        assert_equivalent(&pb, &model);
    }

    #[test]
    fn alloc_run_none_when_only_smaller_gaps() {
        let n = 128u32;
        let mut pb = SlotBitmap::new(n);

        // Allocate everything in one go.
        let start = pb.alloc_run(n).expect("whole arena should be free");
        assert_eq!(start, 0);
        let mut model = vec![false; n as usize];
        assert_equivalent(&pb, &model);

        // Fragment: free exactly every other slot (isolated 1-slot holes)
        for i in (0..n).step_by(2) {
            pb.free_run(i, 1);
            model[i as usize] = true;
        }
        assert_equivalent(&pb, &model);

        // No run of 2 should exist
        assert!(
            pb.alloc_run(2).is_none(),
            "no free run of length 2 should exist"
        );
        assert_equivalent(&pb, &model);
    }

    #[test]
    fn singles_from_tail_preserve_front_run() {
        let mut pb = SlotBitmap::new(512);
        let mut model = vec![true; 512];

        for _ in 0..100 {
            let idx = pb.alloc_one().unwrap();
            model[idx as usize] = false;
        }
        assert_equivalent(&pb, &model);

        let r = pb.alloc_run(64).expect("64-run should still be available");
        assert!(ref_check_run_free(&model, r, 64));
        ref_mark_run(&mut model, r, 64, false);
        assert_equivalent(&pb, &model);
    }

    #[test]
    fn fuzz_rand_compare_with_reference_model() {
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

            let iters = 2000usize;
            for _ in 0..iters {
                let op = rng.random_range(0..100);
                match op {
                    0..=49 => {
                        // alloc_one
                        let before_free = ref_count_free(&model);
                        let got = pb.alloc_one();
                        if let Some(i) = got {
                            assert!(i < n_slots, "index in range");
                            assert!(model[i as usize], "bit must be free");
                            model[i as usize] = false;
                            assert_eq!(ref_count_free(&model), before_free - 1);
                        } else {
                            assert_eq!(before_free, 0, "no free bits if None returned");
                        }
                    }
                    50..=79 => {
                        // alloc_run with random length
                        let need = rng
                            .random_range(1..=std::cmp::max(1, (n_slots as usize).min(128)))
                            as u32;
                        let got = pb.alloc_run(need);
                        if let Some(start) = got {
                            assert!(start + need <= n_slots, "within bounds");
                            assert!(ref_check_run_free(&model, start, need), "run must be free");
                            ref_mark_run(&mut model, start, need, false);
                        } else {
                            let mut exists = false;
                            for s in 0..=(n_slots.saturating_sub(need)) {
                                if ref_check_run_free(&model, s, need) {
                                    exists = true;
                                    break;
                                }
                            }
                            assert!(!exists, "allocator returned None but a free run exists");
                        }
                    }
                    _ => {
                        // free_run on a random valid range
                        let len = rng
                            .random_range(1..=std::cmp::max(1, (n_slots as usize).min(128)))
                            as u32;
                        let max_start = n_slots.saturating_sub(len);
                        let start = if max_start == 0 {
                            0
                        } else {
                            rng.random_range(0..=max_start)
                        };
                        pb.free_run(start, len);
                        ref_mark_run(&mut model, start, len, true);
                    }
                }

                // Always keep representations in sync
                assert_equivalent(&pb, &model);
            }
        }
    }

    #[test]
    fn test_run_crossing_word_boundaries_and_edge_cases() {
        let mut pb = SlotBitmap::new(256);
        let cases = [(60, 8), (62, 4), (120, 16), (0, 128), (32, 64)];

        for (start, len) in cases {
            assert!(pb.check_run_free(start, len));
            pb.mark_run(start, len, false);
            assert!(!pb.check_run_free(start, len));
            pb.mark_run(start, len, true);
            assert!(pb.check_run_free(start, len));
        }

        let mut pb = SlotBitmap::new(128);
        assert_eq!(pb.alloc_run(128), Some(0));
        pb.free_run(0, 128);
        assert_eq!(pb.alloc_run(129), None);

        for i in (10..100).step_by(20) {
            pb.mark_run(i, 10, false);
        }
        assert_eq!(pb.alloc_run(10), Some(0));
        assert_eq!(pb.alloc_run(10), Some(20));
        assert_eq!(pb.alloc_run(10), Some(40));
        assert_eq!(pb.alloc_run(11), Some(100));
    }

    #[test]
    fn test_reused_hints() {
        let mut bitmap = SlotBitmap::new(128);
        // Single allocations come from high end
        assert_eq!(bitmap.alloc_one(), Some(127));
        assert_eq!(bitmap.alloc_one(), Some(126));

        // Run allocations come from low end
        assert_eq!(bitmap.alloc_run(10), Some(0));
        assert_eq!(bitmap.alloc_run(10), Some(10));

        // Free and reallocate
        bitmap.free_run(5, 3);
        assert_eq!(bitmap.alloc_run(3), Some(5)); // Reuses freed space
    }
}
