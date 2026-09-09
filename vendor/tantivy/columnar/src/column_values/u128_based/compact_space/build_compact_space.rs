use std::collections::{BTreeSet, BinaryHeap};
use std::iter;
use std::ops::RangeInclusive;

use itertools::Itertools;

use super::blank_range::BlankRange;
use super::{CompactSpace, RangeMapping};

/// Put the blanks for the sorted values into a binary heap
fn get_blanks(values_sorted: &BTreeSet<u128>) -> BinaryHeap<BlankRange> {
    let mut blanks: BinaryHeap<BlankRange> = BinaryHeap::new();
    for (first, second) in values_sorted.iter().copied().tuple_windows() {
        // Correctness Overflow: the values are deduped and sorted (BTreeSet property), that means
        // there's always space between two values.
        let blank_range = first + 1..=second - 1;
        let blank_range: Result<BlankRange, _> = blank_range.try_into();
        if let Ok(blank_range) = blank_range {
            blanks.push(blank_range);
        }
    }

    blanks
}

struct BlankCollector {
    blanks: Vec<BlankRange>,
    staged_blanks_sum: u128,
}
impl BlankCollector {
    fn new() -> Self {
        Self {
            blanks: vec![],
            staged_blanks_sum: 0,
        }
    }
    fn stage_blank(&mut self, blank: BlankRange) {
        self.staged_blanks_sum += blank.blank_size();
        self.blanks.push(blank);
    }
    fn drain(&mut self) -> impl Iterator<Item = BlankRange> + '_ {
        self.staged_blanks_sum = 0;
        self.blanks.drain(..)
    }
    fn staged_blanks_sum(&self) -> u128 {
        self.staged_blanks_sum
    }
    fn num_staged_blanks(&self) -> usize {
        self.blanks.len()
    }
}
fn num_bits(val: u128) -> u8 {
    (128u32 - val.leading_zeros()) as u8
}

/// Will collect blanks and add them to compact space if more bits are saved than cost from
/// metadata.
pub fn get_compact_space(
    values_deduped_sorted: &BTreeSet<u128>,
    total_num_values: u32,
    cost_per_blank: usize,
) -> CompactSpace {
    let mut compact_space_builder = CompactSpaceBuilder::new();
    if values_deduped_sorted.is_empty() {
        return compact_space_builder.finish();
    }

    // We start by space that's limited to min_value..=max_value
    // Replace after stabilization of https://github.com/rust-lang/rust/issues/62924
    let min_value = values_deduped_sorted.iter().next().copied().unwrap_or(0);
    let max_value = values_deduped_sorted.iter().last().copied().unwrap_or(0);

    let mut blanks: BinaryHeap<BlankRange> = get_blanks(values_deduped_sorted);

    // +1 for null, in case min and max covers the whole space, we are off by one.
    let mut amplitude_compact_space = (max_value - min_value).saturating_add(1);
    if min_value != 0 {
        compact_space_builder.add_blanks(iter::once(0..=min_value - 1));
    }
    if max_value != u128::MAX {
        compact_space_builder.add_blanks(iter::once(max_value + 1..=u128::MAX));
    }

    let mut amplitude_bits: u8 = num_bits(amplitude_compact_space);

    let mut blank_collector = BlankCollector::new();

    // We will stage blanks until they reduce the compact space by at least 1 bit and then flush
    // them if the metadata cost is lower than the total number of saved bits.
    // Binary heap to process the gaps by their size
    while let Some(blank_range) = blanks.pop() {
        blank_collector.stage_blank(blank_range);

        let staged_spaces_sum: u128 = blank_collector.staged_blanks_sum();
        let amplitude_new_compact_space = amplitude_compact_space - staged_spaces_sum;
        let amplitude_new_bits = num_bits(amplitude_new_compact_space);

        if amplitude_bits == amplitude_new_bits {
            continue;
        }
        let saved_bits = (amplitude_bits - amplitude_new_bits) as usize * total_num_values as usize;
        // TODO: Maybe calculate exact cost of blanks and run this more expensive computation only,
        // when amplitude_new_bits changes
        let cost = blank_collector.num_staged_blanks() * cost_per_blank;

        // We want to end up with a compact space that fits into 32 bits.
        // In order to deal with pathological cases, we force the algorithm to keep
        // refining the compact space the amplitude bits is lower than 32.
        //
        // The worst case scenario happens for a large number of u128s regularly
        // spread over the full u128 space.
        //
        // This change will force the algorithm to degenerate into dictionary encoding.
        if amplitude_bits <= 32 && cost >= saved_bits {
            // Continue here, since although we walk over the blanks by size,
            // we can potentially save a lot at the last bits, which are smaller blanks
            //
            // E.g. if the first range reduces the compact space by 1000 from 2000 to 1000, which
            // saves 11-10=1 bit and the next range reduces the compact space by 950 to
            // 50, which saves 10-6=4 bit
            continue;
        }

        amplitude_compact_space = amplitude_new_compact_space;
        amplitude_bits = amplitude_new_bits;
        compact_space_builder.add_blanks(blank_collector.drain().map(|blank| blank.blank_range()));
    }

    assert!(amplitude_bits <= 32);

    // special case, when we don't collected any blanks because:
    // * the data is empty (early exit)
    // * the algorithm did decide it's not worth the cost, which can be the case for single values
    //
    // We drain one collected blank unconditionally, so the empty case is reserved for empty
    // data, and therefore empty compact_space means the data is empty and no data is covered
    // (conversely to all data) and we can assign null to it.
    if compact_space_builder.is_empty() {
        compact_space_builder.add_blanks(
            blank_collector
                .drain()
                .map(|blank| blank.blank_range())
                .take(1),
        );
    }

    let compact_space = compact_space_builder.finish();
    if max_value - min_value != u128::MAX {
        debug_assert_eq!(
            compact_space.amplitude_compact_space(),
            amplitude_compact_space
        );
    }
    compact_space
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CompactSpaceBuilder {
    blanks: Vec<RangeInclusive<u128>>,
}

impl CompactSpaceBuilder {
    /// Creates a new compact space builder which will initially cover the whole space.
    fn new() -> Self {
        Self { blanks: Vec::new() }
    }

    /// Assumes that repeated add_blank calls don't overlap and are not adjacent,
    /// e.g. [3..=5, 5..=10] is not allowed
    ///
    /// Both of those assumptions are true when blanks are produced from sorted values.
    fn add_blanks(&mut self, blank: impl Iterator<Item = RangeInclusive<u128>>) {
        self.blanks.extend(blank);
    }

    fn is_empty(&self) -> bool {
        self.blanks.is_empty()
    }

    /// Convert blanks to covered space and assign null value
    fn finish(mut self) -> CompactSpace {
        // sort by start. ranges are not allowed to overlap
        self.blanks.sort_unstable_by_key(|blank| *blank.start());

        let mut covered_space = Vec::with_capacity(self.blanks.len());

        // beginning of the blanks
        if let Some(first_blank_start) = self.blanks.first().map(RangeInclusive::start)
            && *first_blank_start != 0
        {
            covered_space.push(0..=first_blank_start - 1);
        }

        // Between the blanks
        let between_blanks = self.blanks.iter().tuple_windows().map(|(left, right)| {
            assert!(
                left.end() < right.start(),
                "overlapping or adjacent ranges detected"
            );
            *left.end() + 1..=*right.start() - 1
        });
        covered_space.extend(between_blanks);

        // end of the blanks
        if let Some(last_blank_end) = self.blanks.last().map(RangeInclusive::end)
            && *last_blank_end != u128::MAX
        {
            covered_space.push(last_blank_end + 1..=u128::MAX);
        }

        if covered_space.is_empty() {
            covered_space.push(0..=0); // empty data case
        };

        let mut compact_start: u32 = 1; // 0 is reserved for `null`
        let mut ranges_mapping: Vec<RangeMapping> = Vec::with_capacity(covered_space.len());
        for cov in covered_space {
            let range_mapping = super::RangeMapping {
                value_range: cov,
                compact_start,
            };
            let covered_range_len = range_mapping.range_length();
            ranges_mapping.push(range_mapping);
            compact_start += covered_range_len;
        }
        // println!("num ranges {}", ranges_mapping.len());
        CompactSpace { ranges_mapping }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::u128_based::compact_space::COST_PER_BLANK_IN_BITS;

    #[test]
    fn test_binary_heap_pop_order() {
        let mut blanks: BinaryHeap<BlankRange> = BinaryHeap::new();
        blanks.push((0..=10).try_into().unwrap());
        blanks.push((100..=200).try_into().unwrap());
        blanks.push((100..=110).try_into().unwrap());
        assert_eq!(blanks.pop().unwrap().blank_size(), 101);
        assert_eq!(blanks.pop().unwrap().blank_size(), 11);
    }

    #[test]
    fn test_worst_case_scenario() {
        let vals: BTreeSet<u128> = (0..8).map(|i| i * ((1u128 << 34) / 8)).collect();
        let compact_space = get_compact_space(&vals, vals.len() as u32, COST_PER_BLANK_IN_BITS);
        assert!(compact_space.amplitude_compact_space() < u32::MAX as u128);
    }
}
