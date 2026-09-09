use std::fmt::Debug;

use tantivy_bitpacker::minmax;

use crate::ColumnValues;

/// VecColumn provides `Column` over a `Vec<T>`.
pub struct VecColumn<T = u64> {
    pub(crate) values: Vec<T>,
    pub(crate) min_value: T,
    pub(crate) max_value: T,
}

impl<T: Copy + PartialOrd + Send + Sync + Debug + 'static> ColumnValues<T> for VecColumn<T> {
    fn get_val(&self, position: u32) -> T {
        self.values[position as usize]
    }

    fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        Box::new(self.values.iter().copied())
    }

    fn min_value(&self) -> T {
        self.min_value
    }

    fn max_value(&self) -> T {
        self.max_value
    }

    fn num_vals(&self) -> u32 {
        self.values.len() as u32
    }

    fn get_range(&self, start: u64, output: &mut [T]) {
        output.copy_from_slice(&self.values[start as usize..][..output.len()])
    }
}

impl<T: Copy + PartialOrd + Default> From<Vec<T>> for VecColumn<T> {
    fn from(values: Vec<T>) -> Self {
        let (min_value, max_value) = minmax(values.iter().copied()).unwrap_or_default();
        Self {
            values,
            min_value,
            max_value,
        }
    }
}
impl From<VecColumn> for Vec<u64> {
    fn from(column: VecColumn) -> Self {
        column.values
    }
}
