use std::fmt::Debug;
use std::sync::Arc;

use crate::iterable::Iterable;
use crate::{ColumnIndex, ColumnValues, MergeRowOrder};

pub(crate) struct MergedColumnValues<'a, T> {
    pub(crate) column_indexes: &'a [ColumnIndex],
    pub(crate) column_values: &'a [Option<Arc<dyn ColumnValues<T>>>],
    pub(crate) merge_row_order: &'a MergeRowOrder,
}

impl<T: Copy + PartialOrd + Debug + 'static> Iterable<T> for MergedColumnValues<'_, T> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        match self.merge_row_order {
            MergeRowOrder::Stack(_) => Box::new(
                self.column_values
                    .iter()
                    .flatten()
                    .flat_map(|column_value| column_value.iter()),
            ),
            MergeRowOrder::Shuffled(shuffle_merge_order) => Box::new(
                shuffle_merge_order
                    .iter_new_to_old_row_addrs()
                    .flat_map(|row_addr| {
                        let column_index = &self.column_indexes[row_addr.segment_ord as usize];
                        let column_values =
                            self.column_values[row_addr.segment_ord as usize].as_ref()?;
                        let value_range = column_index.value_row_ids(row_addr.row_id);
                        Some((value_range, column_values))
                    })
                    .flat_map(|(value_range, column_values)| {
                        value_range
                            .into_iter()
                            .map(|val| column_values.get_val(val))
                    }),
            ),
        }
    }
}
