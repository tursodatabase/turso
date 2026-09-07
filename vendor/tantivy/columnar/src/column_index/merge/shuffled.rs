use std::iter;

use crate::column_index::{
    SerializableColumnIndex, SerializableMultivalueIndex, SerializableOptionalIndex, Set,
};
use crate::iterable::Iterable;
use crate::{Cardinality, ColumnIndex, RowId, ShuffleMergeOrder};

pub fn merge_column_index_shuffled<'a>(
    column_indexes: &'a [ColumnIndex],
    cardinality_after_merge: Cardinality,
    shuffle_merge_order: &'a ShuffleMergeOrder,
) -> SerializableColumnIndex<'a> {
    match cardinality_after_merge {
        Cardinality::Full => SerializableColumnIndex::Full,
        Cardinality::Optional => {
            let non_null_row_ids =
                merge_column_index_shuffled_optional(column_indexes, shuffle_merge_order);
            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                non_null_row_ids,
                num_rows: shuffle_merge_order.num_rows(),
            })
        }
        Cardinality::Multivalued => {
            let non_null_row_ids =
                merge_column_index_shuffled_optional(column_indexes, shuffle_merge_order);
            SerializableColumnIndex::Multivalued(SerializableMultivalueIndex {
                doc_ids_with_values: SerializableOptionalIndex {
                    non_null_row_ids,
                    num_rows: shuffle_merge_order.num_rows(),
                },
                start_offsets: merge_column_index_shuffled_multivalued(
                    column_indexes,
                    shuffle_merge_order,
                ),
            })
        }
    }
}

/// Merge several column indexes into one, ordering rows according to the merge_order passed as
/// argument. While it is true that the `merge_order` may imply deletes and hence could in theory a
/// multivalued index into an optional one, this is not supported today for simplification.
///
/// In other words the column_indexes passed as argument may NOT be multivalued.
fn merge_column_index_shuffled_optional<'a>(
    column_indexes: &'a [ColumnIndex],
    merge_order: &'a ShuffleMergeOrder,
) -> Box<dyn Iterable<RowId> + 'a> {
    Box::new(ShuffledIndex {
        column_indexes,
        merge_order,
    })
}

struct ShuffledIndex<'a> {
    column_indexes: &'a [ColumnIndex],
    merge_order: &'a ShuffleMergeOrder,
}

impl Iterable<u32> for ShuffledIndex<'_> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        Box::new(
            self.merge_order
                .iter_new_to_old_row_addrs()
                .enumerate()
                .filter_map(|(new_row_id, old_row_addr)| {
                    let column_index = &self.column_indexes[old_row_addr.segment_ord as usize];
                    let row_id = new_row_id as u32;
                    if column_index.has_value(old_row_addr.row_id) {
                        Some(row_id)
                    } else {
                        None
                    }
                }),
        )
    }
}

fn merge_column_index_shuffled_multivalued<'a>(
    column_indexes: &'a [ColumnIndex],
    merge_order: &'a ShuffleMergeOrder,
) -> Box<dyn Iterable<RowId> + 'a> {
    Box::new(ShuffledMultivaluedIndex {
        column_indexes,
        merge_order,
    })
}

struct ShuffledMultivaluedIndex<'a> {
    column_indexes: &'a [ColumnIndex],
    merge_order: &'a ShuffleMergeOrder,
}

fn iter_num_values<'a>(
    column_indexes: &'a [ColumnIndex],
    merge_order: &'a ShuffleMergeOrder,
) -> impl Iterator<Item = u32> + 'a {
    merge_order.iter_new_to_old_row_addrs().map(|row_addr| {
        let column_index = &column_indexes[row_addr.segment_ord as usize];
        match column_index {
            ColumnIndex::Empty { .. } => 0u32,
            ColumnIndex::Full => 1,
            ColumnIndex::Optional(optional_index) => {
                u32::from(optional_index.contains(row_addr.row_id))
            }
            ColumnIndex::Multivalued(multivalued_index) => {
                multivalued_index.range(row_addr.row_id).len() as u32
            }
        }
    })
}

/// Transforms an iterator containing the number of vals per row (with `num_rows` elements)
/// into a `start_offset` iterator starting at 0 and (with `num_rows + 1` element)
///
/// This will filter values with 0 values as these are covered by the optional index in the
/// multivalue index.
fn integrate_num_vals(num_vals: impl Iterator<Item = u32>) -> impl Iterator<Item = RowId> {
    iter::once(0u32).chain(
        num_vals
            .filter(|num_vals| *num_vals != 0)
            .scan(0, |state, num_vals| {
                *state += num_vals;
                Some(*state)
            }),
    )
}

impl Iterable<u32> for ShuffledMultivaluedIndex<'_> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        let num_vals_per_row = iter_num_values(self.column_indexes, self.merge_order);
        Box::new(integrate_num_vals(num_vals_per_row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RowAddr;
    use crate::column_index::OptionalIndex;

    #[test]
    fn test_integrate_num_vals_empty() {
        assert!(integrate_num_vals(iter::empty()).eq(iter::once(0)));
    }

    #[test]
    fn test_integrate_num_vals_one_el() {
        assert!(integrate_num_vals(iter::once(10)).eq([0, 10].into_iter()));
    }

    #[test]
    fn test_integrate_num_vals_several() {
        assert!(integrate_num_vals([3, 0, 10, 20].into_iter()).eq([0, 3, 13, 33].into_iter()));
    }

    #[test]
    fn test_merge_column_index_optional_shuffle() {
        let optional_index: ColumnIndex = OptionalIndex::for_test(2, &[0]).into();
        let column_indexes = [optional_index, ColumnIndex::Full];
        let row_addrs = vec![
            RowAddr {
                segment_ord: 0u32,
                row_id: 1u32,
            },
            RowAddr {
                segment_ord: 1u32,
                row_id: 0u32,
            },
        ];
        let shuffle_merge_order = ShuffleMergeOrder::for_test(&[2, 1], row_addrs);
        let serializable_index = merge_column_index_shuffled(
            &column_indexes[..],
            Cardinality::Optional,
            &shuffle_merge_order,
        );
        let SerializableColumnIndex::Optional(SerializableOptionalIndex {
            non_null_row_ids,
            num_rows,
        }) = serializable_index
        else {
            panic!()
        };
        assert_eq!(num_rows, 2);
        let non_null_rows: Vec<RowId> = non_null_row_ids.boxed_iter().collect();
        assert_eq!(&non_null_rows, &[1]);
    }
}
