use std::ops::Range;

use common::{BitSet, OwnedBytes, ReadOnlyBitSet};

use crate::{ColumnarReader, RowAddr, RowId};

pub struct StackMergeOrder {
    // This does not start at 0. The first row is the number of
    // rows in the first columnar.
    cumulated_row_ids: Vec<RowId>,
}

impl StackMergeOrder {
    #[cfg(test)]
    pub fn stack_for_test(num_rows_per_columnar: &[u32]) -> StackMergeOrder {
        let mut cumulated_row_ids: Vec<RowId> = Vec::with_capacity(num_rows_per_columnar.len());
        let mut cumulated_row_id = 0;
        for &num_rows in num_rows_per_columnar {
            cumulated_row_id += num_rows;
            cumulated_row_ids.push(cumulated_row_id);
        }
        StackMergeOrder { cumulated_row_ids }
    }

    pub fn stack(columnars: &[&ColumnarReader]) -> StackMergeOrder {
        let mut cumulated_row_ids: Vec<RowId> = Vec::with_capacity(columnars.len());
        let mut cumulated_row_id = 0;
        for columnar in columnars {
            cumulated_row_id += columnar.num_docs();
            cumulated_row_ids.push(cumulated_row_id);
        }
        StackMergeOrder { cumulated_row_ids }
    }

    pub fn num_rows(&self) -> RowId {
        self.cumulated_row_ids.last().copied().unwrap_or(0)
    }

    pub fn offset(&self, columnar_id: usize) -> RowId {
        if columnar_id == 0 {
            return 0;
        }
        self.cumulated_row_ids[columnar_id - 1]
    }

    pub fn columnar_range(&self, columnar_id: usize) -> Range<RowId> {
        self.offset(columnar_id)..self.offset(columnar_id + 1)
    }
}

pub enum MergeRowOrder {
    /// Columnar tables are simply stacked one above the other.
    /// If the i-th columnar_readers has n_rows_i rows, then
    /// in the resulting columnar,
    /// rows [r0..n_row_0) contains the row of `columnar_readers[0]`, in ordder
    /// rows [n_row_0..n_row_0 + n_row_1 contains the row of `columnar_readers[1]`, in order.
    /// ..
    /// No documents is deleted.
    Stack(StackMergeOrder),
    /// Some more complex mapping, that may interleaves rows from the different readers and
    /// drop rows, or do both.
    Shuffled(ShuffleMergeOrder),
}

impl From<StackMergeOrder> for MergeRowOrder {
    fn from(stack_merge_order: StackMergeOrder) -> MergeRowOrder {
        MergeRowOrder::Stack(stack_merge_order)
    }
}

impl From<ShuffleMergeOrder> for MergeRowOrder {
    fn from(shuffle_merge_order: ShuffleMergeOrder) -> MergeRowOrder {
        MergeRowOrder::Shuffled(shuffle_merge_order)
    }
}

impl MergeRowOrder {
    pub fn num_rows(&self) -> RowId {
        match self {
            MergeRowOrder::Stack(stack_row_order) => stack_row_order.num_rows(),
            MergeRowOrder::Shuffled(complex_mapping) => complex_mapping.num_rows(),
        }
    }
}

pub struct ShuffleMergeOrder {
    pub new_row_id_to_old_row_id: Vec<RowAddr>,
    pub alive_bitsets: Vec<Option<ReadOnlyBitSet>>,
}

impl ShuffleMergeOrder {
    pub fn for_test(
        segment_num_rows: &[RowId],
        new_row_id_to_old_row_id: Vec<RowAddr>,
    ) -> ShuffleMergeOrder {
        let mut alive_bitsets: Vec<BitSet> = segment_num_rows
            .iter()
            .map(|&num_rows| BitSet::with_max_value(num_rows))
            .collect();
        for &RowAddr {
            segment_ord,
            row_id,
        } in &new_row_id_to_old_row_id
        {
            alive_bitsets[segment_ord as usize].insert(row_id);
        }
        let alive_bitsets: Vec<Option<ReadOnlyBitSet>> = alive_bitsets
            .into_iter()
            .map(|alive_bitset| {
                let mut buffer = Vec::new();
                alive_bitset.serialize(&mut buffer).unwrap();
                let data = OwnedBytes::new(buffer);
                Some(ReadOnlyBitSet::open(data))
            })
            .collect();
        ShuffleMergeOrder {
            new_row_id_to_old_row_id,
            alive_bitsets,
        }
    }

    pub fn num_rows(&self) -> RowId {
        self.new_row_id_to_old_row_id.len() as RowId
    }

    pub fn iter_new_to_old_row_addrs(&self) -> impl Iterator<Item = RowAddr> + '_ {
        self.new_row_id_to_old_row_id.iter().copied()
    }
}
