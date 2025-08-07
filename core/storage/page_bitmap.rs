use crate::turso_assert;

#[derive(Debug)]
/// Immutable-size bitmap for use in tracking allocated pages from an arena.
pub(super) struct PageBitmap {
    /// 1 = free, 0 = allocated
    words: Box<[u64]>,
    /// total capacity of pages in the arena
    n_pages: u32,
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
/// There are two hints or 'pointers' where we begin scanning for free pages,
/// one on each end of the map. This strategy preserves contiguous free space
/// at the low end of the arena, which is beneficial for allocating many contiguous
/// buffers that can be coalesced into fewer I/O operations.
///
/// Single-page allocations (`alloc_one`) start searching from `scan_one_high` and work
/// downward, preserving large contiguous runs at the low end. When the hint area is
/// exhausted, the search wraps around to check from the top of the bitmap.
///
/// Run allocations (`alloc_run`) scan upward from `scan_run_low`, making it likely
/// that sequential allocations will be physically contiguous.
///
/// After freeing pages, we update hints to encourage reuse:
/// - If freed pages are below `scan_run_low`, we move it down to include them
/// - If freed pages are above `scan_one_high`, we move it up to include them
///
///```ignore
/// let mut bitmap = PageBitmap::new(128);
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
impl PageBitmap {
    /// 64 bits per word, so shift by 6 to get page index
    const WORD_SHIFT: u32 = 6;
    const WORD_BITS: u32 = 64;
    const WORD_MASK: u32 = 63;
    const ALL_FREE: u64 = u64::MAX;
    const ALL_ALLOCATED: u64 = 0u64;

    const ALLOC: bool = false;
    const FREE: bool = true;

    /// Creates a new `PageBitmap` capable of tracking `n_pages` pages.
    ///
    /// If `n_pages` is not a multiple of 64, the trailing bits in the last
    /// word are marked as allocated to prevent out-of-bounds allocations.
    pub fn new(n_pages: u32) -> Self {
        turso_assert!(n_pages > 0, "PageBitmap must have at least one page");
        let n_words = n_pages.div_ceil(Self::WORD_BITS) as usize;
        let mut words = vec![Self::ALL_FREE; n_words].into_boxed_slice();

        let trailing_bits = n_pages % Self::WORD_BITS;
        if trailing_bits != 0 {
            words[n_words - 1] &= (1u64 << trailing_bits) - 1;
        }
        Self {
            words,
            n_pages,
            scan_run_low: 0,
            scan_one_high: n_pages - 1,
        }
    }

    #[inline]
    /// Convert word index and bit offset to page index
    /// Example:
    /// word_idx: 1, bit: 10
    /// shift the word index by WORD_SHIFT and OR with `bit` to get the page
    /// Page number: 1 << 6 | 10   =  page index: 74
    const fn word_and_bit_to_page(word_idx: usize, bit: u32) -> u32 {
        (word_idx as u32) << Self::WORD_SHIFT | bit
    }

    #[inline]
    /// Convert page index to word index and bit offset
    /// Example
    /// Page number: 74
    /// (74 >> 6, 74 & 63) = (1, 10)
    const fn page_to_word_and_bit(page_idx: u32) -> (usize, u32) {
        (
            (page_idx >> Self::WORD_SHIFT) as usize,
            page_idx & Self::WORD_MASK,
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

    /// Allocates a single free page from the bitmap.
    ///
    /// This method scans from high to low addresses to preserve contiguous
    /// runs of free pages at the low end of the bitmap.
    pub fn alloc_one(&mut self) -> Option<u32> {
        for start in [self.scan_one_high, self.n_pages - 1] {
            let (mut word_idx, bit) = Self::page_to_word_and_bit(start);
            let mut word = self.words[word_idx] & Self::mask_through(bit);
            if word != Self::ALL_ALLOCATED {
                // Fast path: pick highest set bit in this masked word
                let bit = 63 - word.leading_zeros();
                self.words[word_idx] &= !(1u64 << bit);
                let page = Self::word_and_bit_to_page(word_idx, bit);
                self.scan_one_high = page.saturating_sub(1);
                return Some(page);
            }
            // Walk lower words
            while word_idx > 0 {
                word_idx -= 1;
                word = self.words[word_idx];
                if word != Self::ALL_ALLOCATED {
                    let bits = 63 - word.leading_zeros();
                    self.words[word_idx] &= !(1u64 << bits);
                    let page = Self::word_and_bit_to_page(word_idx, bits);
                    self.scan_one_high = page.saturating_sub(1);
                    return Some(page);
                }
            }
            if self.scan_one_high == self.n_pages - 1 {
                // dont try again if we already started there
                return None;
            }
        }
        None
    }

    /// Allocates a contiguous run of `need` pages from the bitmap.
    /// This method scans from low to high addresses, starting from `scan_run_low`,
    /// next allocation continues from where we left off by updating the hint.
    pub fn alloc_run(&mut self, need: u32) -> Option<u32> {
        if need == 0 || need > self.n_pages {
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
                // Update single-page hint if this run extends beyond it
                let last_page = found + need - 1;
                if last_page > self.scan_one_high {
                    self.scan_one_high = last_page.min(self.n_pages - 1);
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

    #[inline]
    fn masked_word(&self, idx: usize) -> u64 {
        let word = self.words[idx];
        if idx + 1 != self.words.len() {
            // fast path, if it's not the last word we can use it as-is
            return word;
        }
        // otherwise keep invalid tail bits treated as allocated
        let trailing = self.n_pages % Self::WORD_BITS;
        if trailing == 0 {
            word
        } else {
            word & ((1u64 << trailing) - 1)
        }
    }

    #[inline]
    fn is_full_free(&self, idx: usize) -> bool {
        if idx + 1 != self.words.len() {
            return self.words[idx] == Self::ALL_FREE;
        }

        // handle the case where the last word has some bits that we dont include
        let trailing = self.n_pages % Self::WORD_BITS;
        if trailing == 0 {
            self.words[idx] == Self::ALL_FREE
        } else {
            self.masked_word(idx) == ((1u64 << trailing) - 1)
        }
    }

    /// Find an unallocated sequence of `need` pages, scanning *upward* from `start`
    ///
    /// Overview:
    /// Set limit = n_pages - (need - 1): as the most pages we could iterate through
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
    /// `is_full_free(word)` in 64-page chunks and return if `need` is satisfied.
    ///
    /// If still short, then check the tail in the next (partial) word and if tail >= remaining: return.
    /// otherwise advance `pos = run_start + pages_found + 1`.
    fn find_free_run_up(&self, start: u32, need: u32) -> Option<u32> {
        let limit = self.n_pages.saturating_sub(need - 1);
        let mut pos = start;
        while pos < limit {
            let (word_idx, bit_offset) = Self::page_to_word_and_bit(pos);

            // Current word from bit_offset onward
            let current_word = self.masked_word(word_idx) & Self::mask_from(bit_offset);
            if current_word == 0 {
                // Jump to next word boundary
                pos = ((word_idx + 1) as u32) << Self::WORD_SHIFT;
                continue;
            }

            // First free bit >= pos
            let first_free_bit = current_word.trailing_zeros();
            let run_start = ((word_idx as u32) << Self::WORD_SHIFT) + first_free_bit;

            // Free span within this word
            let free_in_cur = Self::run_len_from(self.masked_word(word_idx), first_free_bit);
            if free_in_cur >= need {
                return Some(run_start);
            }

            // If we didn't reach the word boundary, the run is broken here.
            if first_free_bit + free_in_cur < Self::WORD_BITS {
                pos = run_start + free_in_cur + 1;
                continue;
            }

            // We exactly reached the boundary, extend across whole free words
            let mut pages_found = free_in_cur;
            let mut wi = word_idx + 1;

            while pages_found + Self::WORD_BITS <= need
                && wi < self.words.len()
                && self.is_full_free(wi)
            {
                pages_found += Self::WORD_BITS;
                wi += 1;
            }
            if pages_found >= need {
                return Some(run_start);
            }

            // Tail in the next partial word
            if wi < self.words.len() {
                let remaining = need - pages_found;
                let w = self.masked_word(wi);
                let tail = (!w).trailing_zeros();
                if tail >= remaining {
                    return Some(run_start);
                }
                // Run broken in this word: skip past the failure point to avoid checking again
                pos = run_start + pages_found + 1;
                continue;
            }
            // No space
            return None;
        }
        None
    }

    /// Frees a contiguous run of pages, marking them as available for reallocation
    /// and update the scan hints to  allocations.
    pub fn free_run(&mut self, start: u32, count: u32) {
        if count == 0 {
            return;
        }
        turso_assert!(start + count <= self.n_pages, "free_run out of bounds");
        self.mark_run(start, count, Self::FREE);
        // Update hints based on what we're freeing

        // if this was a single page and higher than current hint, bump the high hint up
        if count == 1 && start > self.scan_one_high {
            self.scan_one_high = start;
            return;
        }

        if start < self.scan_run_low {
            self.scan_run_low = start;
        }
        // Also update scan_one_high hint if the run extends beyond it
        let last_page = (start + count - 1).min(self.n_pages - 1);
        if last_page > self.scan_one_high {
            self.scan_one_high = last_page;
        }
    }

    /// Checks whether a contiguous run of pages is completely free.
    /// # Example
    /// Checking pages 125-134 (10 pages starting at 125)
    /// This spans from word 1 (bit 61) to word 2 (bit 6)
    ///
    /// Word 1: ...11111111_11110000  (bits 60-63 must be checked)
    /// Word 2: 00000000_01111111...  (bits 0-6 must be checked)
    pub fn check_run_free(&self, start: u32, len: u32) -> bool {
        if start + len > self.n_pages {
            return false;
        }
        let (mut word_idx, bit_offset) = Self::page_to_word_and_bit(start);
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

    /// Marks a contiguous run of pages as either free or allocated.
    pub fn mark_run(&mut self, start: u32, len: u32, free: bool) {
        turso_assert!(start + len <= self.n_pages, "mark_run out of bounds");

        let (mut word_idx, mut bit_offset) = Self::page_to_word_and_bit(start);
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
        // keep last-word tail clamped to the proper amount of pages
        let last = self.words.len() - 1;
        self.words[last] &= if (self.n_pages % Self::WORD_BITS) == 0 {
            Self::ALL_FREE
        } else {
            (1u64 << (self.n_pages % Self::WORD_BITS)) - 1
        };
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use turso_macros::turso_test;

    fn pb_free_vec(pb: &PageBitmap) -> Vec<bool> {
        let mut v = vec![false; pb.n_pages as usize];
        v.iter_mut()
            .enumerate()
            .take(pb.n_pages as usize)
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
        for page in &mut model[st..st + len] {
            *page = free;
        }
    }

    /// Returns `true` if the bitmap's notion of free bits equals the reference model exactly.
    fn assert_equivalent(pb: &PageBitmap, model: &[bool]) {
        let pv = pb_free_vec(pb);
        assert_eq!(pv, model, "bitmap bits disagree with reference model");
    }

    #[turso_test]
    fn new_masks_trailing_bits() {
        for n_pages in [1, 63, 64, 65, 127, 128, 129, 255, 256, 1023] {
            let pb = PageBitmap::new(n_pages);
            let free = pb_free_vec(&pb);
            assert_eq!(free.len(), n_pages as usize);
            assert!(free.iter().all(|&b| b), "all pages should start free");

            if n_pages > 0 {
                let words_len = pb.words.len();
                let valid_bits = (n_pages as usize) & 63;
                if valid_bits != 0 {
                    let last = pb.words[words_len - 1];
                    let mask = (1u64 << valid_bits) - 1;
                    assert_eq!(last & !mask, 0, "bits beyond n_pages must be 0");
                }
            }
        }
    }

    #[turso_test]
    fn alloc_one_exhausts_all() {
        let mut pb = PageBitmap::new(257);
        let mut model = vec![true; 257];

        let mut count = 0;
        while let Some(idx) = pb.alloc_one() {
            assert!(model[idx as usize], "must be free in model");
            model[idx as usize] = false;
            count += 1;
        }
        assert_eq!(count, 257, "should allocate all pages once");
        assert!(pb.alloc_one().is_none(), "no pages left");
        assert_equivalent(&pb, &model);
    }

    #[turso_test]
    fn alloc_run_basic_and_free() {
        let mut pb = PageBitmap::new(200);
        let mut model = vec![true; 200];

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

    #[turso_test]
    fn alloc_run_none_when_only_smaller_gaps() {
        let n = 128u32;
        let mut pb = PageBitmap::new(n);

        // Allocate everything in one go.
        let start = pb.alloc_run(n).expect("whole arena should be free");
        assert_eq!(start, 0);
        let mut model = vec![false; n as usize];
        assert_equivalent(&pb, &model);

        // Fragment: free exactly every other page (isolated 1-page holes)
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

    #[turso_test]
    fn singles_from_tail_preserve_front_run() {
        let mut pb = PageBitmap::new(512);
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

    #[turso_test]
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
            let n_pages = match rng.gen_range(0..6) {
                0 => 777,
                1 => 1,
                2 => 63,
                3 => 64,
                4 => 65,
                _ => rng.gen_range(1..=2048),
            } as u32;

            let mut pb = PageBitmap::new(n_pages);
            let mut model = vec![true; n_pages as usize];

            let iters = 2000usize;
            for _ in 0..iters {
                let op = rng.gen_range(0..100);
                match op {
                    0..=49 => {
                        // alloc_one
                        let before_free = ref_count_free(&model);
                        let got = pb.alloc_one();
                        if let Some(i) = got {
                            assert!(i < n_pages, "index in range");
                            assert!(model[i as usize], "bit must be free");
                            model[i as usize] = false;
                            assert_eq!(ref_count_free(&model), before_free - 1);
                        } else {
                            assert_eq!(before_free, 0, "no free bits if None returned");
                        }
                    }
                    50..=79 => {
                        // alloc_run with random length
                        let need =
                            rng.gen_range(1..=std::cmp::max(1, (n_pages as usize).min(128))) as u32;
                        let got = pb.alloc_run(need);
                        if let Some(start) = got {
                            assert!(start + need <= n_pages, "within bounds");
                            assert!(ref_check_run_free(&model, start, need), "run must be free");
                            ref_mark_run(&mut model, start, need, false);
                        } else {
                            let mut exists = false;
                            for s in 0..=(n_pages.saturating_sub(need)) {
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
                        let len =
                            rng.gen_range(1..=std::cmp::max(1, (n_pages as usize).min(128))) as u32;
                        let max_start = n_pages.saturating_sub(len);
                        let start = if max_start == 0 {
                            0
                        } else {
                            rng.gen_range(0..=max_start)
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

    #[turso_test]
    fn test_run_crossing_word_boundaries_and_edge_cases() {
        let mut pb = PageBitmap::new(256);
        let cases = [(60, 8), (62, 4), (120, 16), (0, 128), (32, 64)];

        for (start, len) in cases {
            assert!(pb.check_run_free(start, len));
            pb.mark_run(start, len, false);
            assert!(!pb.check_run_free(start, len));
            pb.mark_run(start, len, true);
            assert!(pb.check_run_free(start, len));
        }

        let mut pb = PageBitmap::new(100);
        assert_eq!(pb.alloc_run(100), Some(0));
        pb.free_run(0, 100);
        assert_eq!(pb.alloc_run(101), None);

        for i in (10..100).step_by(20) {
            pb.mark_run(i, 10, false);
        }
        assert_eq!(pb.alloc_run(10), Some(0));
        assert_eq!(pb.alloc_run(10), Some(20));
        assert_eq!(pb.alloc_run(10), Some(40));
        assert_eq!(pb.alloc_run(11), None);
    }

    #[turso_test]
    fn test_reused_hints() {
        let mut bitmap = PageBitmap::new(128);
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
