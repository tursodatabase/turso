use super::{INITIAL_CAPACITY, SlotMeta, SuffixBag, WIDTH};

impl Clone for SuffixBag {
    /// Clone with compaction, only copies active suffix data.
    ///
    /// This avoids copying dead/garbage data from the original buffer.
    fn clone(&self) -> Self {
        if self.suffix_count == 0 {
            return Self::new();
        }

        // Calculate exact size needed for active suffixes
        let needed: usize = self
            .slots
            .iter()
            .filter(|s: &&SlotMeta| s.has_suffix())
            .map(|s: &SlotMeta| s.len as usize)
            .sum();

        let capacity: usize = needed.next_power_of_two().max(INITIAL_CAPACITY);
        let mut new_data: Vec<u8> = Vec::with_capacity(capacity);
        let mut new_slots: [SlotMeta; WIDTH] = [SlotMeta::EMPTY; WIDTH];

        #[expect(clippy::indexing_slicing, reason = "data bounds from meta")]
        for (new_slot, meta) in new_slots.iter_mut().zip(self.slots.iter()) {
            if !meta.has_suffix() {
                continue;
            }

            let start: usize = meta.offset as usize;
            let end: usize = start + meta.len as usize;

            let new_offset: usize = new_data.len();
            new_data.extend_from_slice(&self.data[start..end]);

            #[expect(clippy::cast_possible_truncation)]
            {
                *new_slot = SlotMeta {
                    offset: new_offset as u32,
                    len: meta.len,
                    _pad: 0,
                };
            }
        }

        Self {
            slots: new_slots,
            data: new_data,
            suffix_count: self.suffix_count,
        }
    }
}
