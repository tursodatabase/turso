use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Range, RangeInclusive};

use crate::ColumnValues;
use crate::column_values::monotonic_mapping::StrictlyMonotonicFn;

struct MonotonicMappingColumn<C, T, Input> {
    from_column: C,
    monotonic_mapping: T,
    _phantom: PhantomData<Input>,
}

/// Creates a view of a column transformed by a strictly monotonic mapping. See
/// [`StrictlyMonotonicFn`].
///
/// E.g. apply a gcd monotonic_mapping([100, 200, 300]) == [1, 2, 3]
/// monotonic_mapping.mapping() is expected to be injective, and we should always have
/// monotonic_mapping.inverse(monotonic_mapping.mapping(el)) == el
///
/// The inverse of the mapping is required for:
/// `fn get_positions_for_value_range(&self, range: RangeInclusive<T>) -> Vec<u64> `
/// The user provides the original value range and we need to monotonic map them in the same way the
/// serialization does before calling the underlying column.
///
/// Note that when opening a codec, the monotonic_mapping should be the inverse of the mapping
/// during serialization. And therefore the monotonic_mapping_inv when opening is the same as
/// monotonic_mapping during serialization.
pub fn monotonic_map_column<C, T, Input, Output>(
    from_column: C,
    monotonic_mapping: T,
) -> impl ColumnValues<Output>
where
    C: ColumnValues<Input> + 'static,
    T: StrictlyMonotonicFn<Input, Output> + Send + Sync + 'static,
    Input: PartialOrd + Debug + Send + Sync + Clone + 'static,
    Output: PartialOrd + Debug + Send + Sync + Clone + 'static,
{
    MonotonicMappingColumn {
        from_column,
        monotonic_mapping,
        _phantom: PhantomData,
    }
}

impl<C, T, Input, Output> ColumnValues<Output> for MonotonicMappingColumn<C, T, Input>
where
    C: ColumnValues<Input> + 'static,
    T: StrictlyMonotonicFn<Input, Output> + Send + Sync + 'static,
    Input: PartialOrd + Send + Debug + Sync + Clone + 'static,
    Output: PartialOrd + Send + Debug + Sync + Clone + 'static,
{
    #[inline(always)]
    fn get_val(&self, idx: u32) -> Output {
        let from_val = self.from_column.get_val(idx);
        self.monotonic_mapping.mapping(from_val)
    }

    fn min_value(&self) -> Output {
        let from_min_value = self.from_column.min_value();
        self.monotonic_mapping.mapping(from_min_value)
    }

    fn max_value(&self) -> Output {
        let from_max_value = self.from_column.max_value();
        self.monotonic_mapping.mapping(from_max_value)
    }

    fn num_vals(&self) -> u32 {
        self.from_column.num_vals()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Output> + '_> {
        Box::new(
            self.from_column
                .iter()
                .map(|el| self.monotonic_mapping.mapping(el)),
        )
    }

    fn get_row_ids_for_value_range(
        &self,
        range: RangeInclusive<Output>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        self.from_column.get_row_ids_for_value_range(
            self.monotonic_mapping.inverse(range.start().clone())
                ..=self.monotonic_mapping.inverse(range.end().clone()),
            doc_id_range,
            positions,
        )
    }

    // We voluntarily do not implement get_range as it yields a regression,
    // and we do not have any specialized implementation anyway.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::VecColumn;
    use crate::column_values::monotonic_mapping::{
        StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
    };

    #[test]
    fn test_monotonic_mapping_iter() {
        let vals: Vec<u64> = (0..100u64).map(|el| el * 10).collect();
        let col = VecColumn::from(vals);
        let mapped = monotonic_map_column(
            col,
            StrictlyMonotonicMappingInverter::from(StrictlyMonotonicMappingToInternal::<i64>::new()),
        );
        let val_i64s: Vec<u64> = mapped.iter().collect();
        for i in 0..100 {
            assert_eq!(val_i64s[i as usize], mapped.get_val(i));
        }
    }
}
