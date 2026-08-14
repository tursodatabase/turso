// ========================================================================
//  Compaction
// ========================================================================

use arrayvec::ArrayVec;

use super::{PermutationProvider, SlotMeta, SuffixBag, WIDTH};

/// Entry for tracking suffix during compaction.
#[derive(Clone, Copy)]
struct CompactEntry {
    slot: usize,
    offset: u32,
    len: u16,
}

impl SuffixBag {
    /// Compact in-place by moving data within the existing buffer.
    ///
    /// Only reachable via `SuffixBag::try_assign(&mut self, ...)`, which
    /// guarantees exclusive access. Not used in concurrent code paths
    /// (those use `try_assign_append_only` + drain-and-rebuild).
    #[expect(clippy::indexing_slicing, reason = "Bounds checked via slot iteration")]
    pub(super) fn compact_in_place(&mut self) {
        if self.suffix_count == 0 {
            self.data.clear();
            return;
        }

        let mut entries: ArrayVec<CompactEntry, WIDTH> = ArrayVec::new();
        let mut already_sorted: bool = true;
        let mut last_offset: u32 = 0;

        for slot in 0..WIDTH {
            let meta: SlotMeta = self.slots[slot];

            if meta.has_suffix() {
                if meta.offset < last_offset {
                    already_sorted = false;
                }
                last_offset = meta.offset;

                entries.push(CompactEntry {
                    slot,
                    offset: meta.offset,
                    len: meta.len,
                });
            }
        }

        if entries.is_empty() {
            self.data.clear();
            self.suffix_count = 0;
            return;
        }

        if !already_sorted {
            entries.sort_unstable_by_key(|e: &CompactEntry| e.offset);
        }

        let mut write_pos: usize = 0;

        for entry in &entries {
            let src_start: usize = entry.offset as usize;
            let src_end: usize = src_start + entry.len as usize;

            debug_assert!(
                write_pos <= src_start,
                "compaction invariant violated: write_pos ({write_pos}) > src_start ({src_start})"
            );

            if write_pos != src_start {
                self.data.copy_within(src_start..src_end, write_pos);
            }

            #[expect(clippy::cast_possible_truncation)]
            {
                self.slots[entry.slot] = SlotMeta {
                    offset: write_pos as u32,
                    len: entry.len,
                    _pad: 0,
                };
            }

            write_pos += entry.len as usize;
        }

        self.data.truncate(write_pos);
    }

    /// Compact keeping only specified active slots, using in-place moves.
    #[expect(clippy::indexing_slicing, reason = "Slot bounds explicitly checked")]
    pub(super) fn compact(&mut self, active_slots: impl Iterator<Item = usize>) -> usize {
        let old_used: usize = self.data.len();
        let mut entries: ArrayVec<CompactEntry, WIDTH> = ArrayVec::new();
        let mut seen: u64 = 0;
        let mut already_sorted: bool = true;
        let mut last_offset: u32 = 0;

        for slot in active_slots {
            debug_assert!(
                slot < WIDTH,
                "active_slots yielded out-of-bounds slot {slot} (WIDTH={WIDTH})"
            );

            if slot >= WIDTH {
                continue;
            }

            let mask: u64 = 1 << slot;

            if seen & mask != 0 {
                continue;
            }

            seen |= mask;

            let meta: SlotMeta = self.slots[slot];

            if meta.has_suffix() {
                if meta.offset < last_offset {
                    already_sorted = false;
                }
                last_offset = meta.offset;

                entries.push(CompactEntry {
                    slot,
                    offset: meta.offset,
                    len: meta.len,
                });
            }
        }

        if entries.is_empty() {
            self.data.clear();
            self.slots = [SlotMeta::EMPTY; WIDTH];
            self.suffix_count = 0;
            return old_used;
        }

        if !already_sorted {
            entries.sort_unstable_by_key(|e: &CompactEntry| e.offset);
        }

        let mut new_slots: [SlotMeta; WIDTH] = [SlotMeta::EMPTY; WIDTH];

        let mut write_pos: usize = 0;

        for entry in &entries {
            let src_start: usize = entry.offset as usize;
            let src_end: usize = src_start + entry.len as usize;

            debug_assert!(
                write_pos <= src_start,
                "compaction invariant violated: write_pos ({write_pos}) > src_start ({src_start})"
            );

            if write_pos != src_start {
                self.data.copy_within(src_start..src_end, write_pos);
            }

            #[expect(clippy::cast_possible_truncation)]
            {
                new_slots[entry.slot] = SlotMeta {
                    offset: write_pos as u32,
                    len: entry.len,
                    _pad: 0,
                };
            }

            write_pos += entry.len as usize;
        }

        self.data.truncate(write_pos);
        self.slots = new_slots;

        #[expect(clippy::cast_possible_truncation)]
        {
            self.suffix_count = entries.len() as u8;
        }

        old_used.saturating_sub(write_pos)
    }

    /// Compact using a permutation to determine active slots.
    ///
    /// # Returns
    ///
    /// The number of bytes reclaimed.
    pub fn compact_with_permuter<P: PermutationProvider>(
        &mut self,
        perm: &P,
        exclude_slot: Option<usize>,
    ) -> usize {
        let active = (0..perm.size())
            .map(|i: usize| perm.get(i))
            .filter(|s: &usize| Some(*s) != exclude_slot);

        self.compact(active)
    }
}
