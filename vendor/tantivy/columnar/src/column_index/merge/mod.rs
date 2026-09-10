mod shuffled;
mod stacked;

use common::ReadOnlyBitSet;
use shuffled::merge_column_index_shuffled;
use stacked::merge_column_index_stacked;

use crate::column_index::SerializableColumnIndex;
use crate::{Cardinality, ColumnIndex, MergeRowOrder};

fn detect_cardinality_single_column_index(
    column_index: &ColumnIndex,
    alive_bitset_opt: &Option<ReadOnlyBitSet>,
) -> Cardinality {
    let Some(alive_bitset) = alive_bitset_opt else {
        return column_index.get_cardinality();
    };
    let cardinality_before_deletes = column_index.get_cardinality();
    if cardinality_before_deletes == Cardinality::Full {
        // The columnar cardinality can only become more restrictive in the presence of deletes
        // (where cardinality sorted from the more restrictive to the least restrictive are Full,
        // Optional, Multivalued)
        //
        // If we are already "Full", we are guaranteed to stay "Full" after deletes.
        return Cardinality::Full;
    }
    let mut cardinality_so_far = Cardinality::Full;
    for doc_id in alive_bitset.iter() {
        let num_values = column_index.value_row_ids(doc_id).len();
        let row_cardinality = match num_values {
            0 => Cardinality::Optional,
            1 => Cardinality::Full,
            _ => Cardinality::Multivalued,
        };
        cardinality_so_far = cardinality_so_far.max(row_cardinality);
        if cardinality_so_far >= cardinality_before_deletes {
            // There won't be any improvement in the cardinality.
            // We can early exit.
            return cardinality_before_deletes;
        }
    }
    cardinality_so_far
}

fn detect_cardinality(
    column_indexes: &[ColumnIndex],
    merge_row_order: &MergeRowOrder,
) -> Cardinality {
    match merge_row_order {
        MergeRowOrder::Stack(_) => column_indexes
            .iter()
            .map(ColumnIndex::get_cardinality)
            .max()
            .unwrap_or(Cardinality::Full),
        MergeRowOrder::Shuffled(shuffle_merge_order) => {
            let mut merged_cardinality = Cardinality::Full;
            for (column_index, alive_bitset_opt) in column_indexes
                .iter()
                .zip(shuffle_merge_order.alive_bitsets.iter())
            {
                let cardinality: Cardinality =
                    detect_cardinality_single_column_index(column_index, alive_bitset_opt);
                if cardinality == Cardinality::Multivalued {
                    return cardinality;
                }
                merged_cardinality = merged_cardinality.max(cardinality);
            }
            merged_cardinality
        }
    }
}

pub fn merge_column_index<'a>(
    columns: &'a [ColumnIndex],
    merge_row_order: &'a MergeRowOrder,
) -> SerializableColumnIndex<'a> {
    // For simplification, we do not try to detect whether the cardinality could be
    // downgraded thanks to deletes.
    let cardinality_after_merge = detect_cardinality(columns, merge_row_order);
    match merge_row_order {
        MergeRowOrder::Stack(stack_merge_order) => {
            merge_column_index_stacked(columns, cardinality_after_merge, stack_merge_order)
        }
        MergeRowOrder::Shuffled(complex_merge_order) => {
            merge_column_index_shuffled(columns, cardinality_after_merge, complex_merge_order)
        }
    }
}

// TODO actually, the shuffled code path is a bit too general.
// In practise, we do not really shuffle everything.
// The merge order restricted to a specific column keeps the original row order.
//
// This may offer some optimization that we have not explored yet.

#[cfg(test)]
mod tests {
    use common::OwnedBytes;

    use crate::column_index::merge::detect_cardinality;
    use crate::column_index::multivalued_index::{
        MultiValueIndex, open_multivalued_index, serialize_multivalued_index,
    };
    use crate::column_index::{OptionalIndex, SerializableColumnIndex, merge_column_index};
    use crate::{
        Cardinality, ColumnIndex, MergeRowOrder, RowAddr, RowId, ShuffleMergeOrder, StackMergeOrder,
    };

    #[test]
    fn test_detect_cardinality() {
        assert_eq!(
            detect_cardinality(&[], &StackMergeOrder::stack_for_test(&[]).into()),
            Cardinality::Full
        );
        let optional_index: ColumnIndex = OptionalIndex::for_test(1, &[]).into();
        let multivalued_index: ColumnIndex = MultiValueIndex::for_test(&[0, 1]).into();
        assert_eq!(
            detect_cardinality(
                &[optional_index.clone(), ColumnIndex::Empty { num_docs: 0 }],
                &StackMergeOrder::stack_for_test(&[1, 0]).into()
            ),
            Cardinality::Optional
        );
        assert_eq!(
            detect_cardinality(
                &[optional_index.clone(), ColumnIndex::Full],
                &StackMergeOrder::stack_for_test(&[1, 1]).into()
            ),
            Cardinality::Optional
        );
        assert_eq!(
            detect_cardinality(
                &[
                    multivalued_index.clone(),
                    ColumnIndex::Empty { num_docs: 0 }
                ],
                &StackMergeOrder::stack_for_test(&[1, 0]).into()
            ),
            Cardinality::Multivalued
        );
        assert_eq!(
            detect_cardinality(
                &[multivalued_index.clone(), optional_index.clone()],
                &StackMergeOrder::stack_for_test(&[1, 1]).into()
            ),
            Cardinality::Multivalued
        );
        assert_eq!(
            detect_cardinality(
                &[optional_index, multivalued_index],
                &StackMergeOrder::stack_for_test(&[1, 1]).into()
            ),
            Cardinality::Multivalued
        );
    }

    #[test]
    fn test_merge_index_multivalued_sorted() {
        let column_indexes: Vec<ColumnIndex> = vec![MultiValueIndex::for_test(&[0, 2, 5]).into()];
        let merge_row_order: MergeRowOrder = ShuffleMergeOrder::for_test(
            &[2],
            vec![
                RowAddr {
                    segment_ord: 0u32,
                    row_id: 1u32,
                },
                RowAddr {
                    segment_ord: 0u32,
                    row_id: 0u32,
                },
            ],
        )
        .into();
        let merged_column_index = merge_column_index(&column_indexes[..], &merge_row_order);
        let SerializableColumnIndex::Multivalued(start_index_iterable) = merged_column_index else {
            panic!("Expected a multivalued index")
        };
        let mut output = Vec::new();
        serialize_multivalued_index(&start_index_iterable, &mut output).unwrap();
        let multivalue =
            open_multivalued_index(OwnedBytes::new(output), crate::Version::V2).unwrap();
        let start_indexes: Vec<RowId> = multivalue.get_start_index_column().iter().collect();
        assert_eq!(&start_indexes, &[0, 3, 5]);
    }

    #[test]
    fn test_merge_index_multivalued_sorted_several_segment() {
        let column_indexes: Vec<ColumnIndex> = vec![
            MultiValueIndex::for_test(&[0, 2, 5]).into(),
            ColumnIndex::Empty { num_docs: 0 },
            MultiValueIndex::for_test(&[0, 1, 4]).into(),
        ];
        let merge_row_order: MergeRowOrder = ShuffleMergeOrder::for_test(
            &[2, 0, 2],
            vec![
                RowAddr {
                    segment_ord: 2u32,
                    row_id: 1u32,
                },
                RowAddr {
                    segment_ord: 0u32,
                    row_id: 0u32,
                },
                RowAddr {
                    segment_ord: 2u32,
                    row_id: 0u32,
                },
            ],
        )
        .into();

        let merged_column_index = merge_column_index(&column_indexes[..], &merge_row_order);
        let SerializableColumnIndex::Multivalued(start_index_iterable) = merged_column_index else {
            panic!("Expected a multivalued index")
        };
        let mut output = Vec::new();
        serialize_multivalued_index(&start_index_iterable, &mut output).unwrap();
        let multivalue =
            open_multivalued_index(OwnedBytes::new(output), crate::Version::V2).unwrap();
        let start_indexes: Vec<RowId> = multivalue.get_start_index_column().iter().collect();
        assert_eq!(&start_indexes, &[0, 3, 5, 6]);
    }
}
