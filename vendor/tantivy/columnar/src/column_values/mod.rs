#![warn(missing_docs)]

//! # `fastfield_codecs`
//!
//! - Columnar storage of data for tantivy [`crate::Column`].
//! - Encode data in different codecs.
//! - Monotonically map values to u64/u128

use std::fmt::Debug;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use downcast_rs::DowncastSync;
pub use monotonic_mapping::{MonotonicallyMappableToU64, StrictlyMonotonicFn};
pub use monotonic_mapping_u128::MonotonicallyMappableToU128;

mod merge;
pub(crate) mod monotonic_mapping;
pub(crate) mod monotonic_mapping_u128;
mod stats;
mod u128_based;
mod u64_based;
mod vec_column;

mod monotonic_column;

pub(crate) use merge::MergedColumnValues;
pub use stats::ColumnStats;
pub use u64_based::{
    ALL_U64_CODEC_TYPES, CodecType, load_u64_based_column_values,
    serialize_and_load_u64_based_column_values, serialize_u64_based_column_values,
};
pub use u128_based::{
    CompactHit, CompactSpaceU64Accessor, open_u128_as_compact_u64, open_u128_mapped,
    serialize_column_values_u128,
};
pub use vec_column::VecColumn;

pub use self::monotonic_column::monotonic_map_column;
use crate::RowId;

/// `ColumnValues` provides access to a dense field column.
///
/// `Column` are just a wrapper over `ColumnValues` and a `ColumnIndex`.
///
/// Any methods with a default and specialized implementation need to be called in the
/// wrappers that implement the trait: Arc and MonotonicMappingColumn
pub trait ColumnValues<T: PartialOrd = u64>: Send + Sync + DowncastSync {
    /// Return the value associated with the given idx.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_val(&self, idx: u32) -> T;

    /// Allows to push down multiple fetch calls, to avoid dynamic dispatch overhead.
    ///
    /// idx and output should have the same length
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_vals(&self, indexes: &[u32], output: &mut [T]) {
        assert!(indexes.len() == output.len());
        let out_and_idx_chunks = output.chunks_exact_mut(4).zip(indexes.chunks_exact(4));
        for (out_x4, idx_x4) in out_and_idx_chunks {
            out_x4[0] = self.get_val(idx_x4[0]);
            out_x4[1] = self.get_val(idx_x4[1]);
            out_x4[2] = self.get_val(idx_x4[2]);
            out_x4[3] = self.get_val(idx_x4[3]);
        }

        let out_and_idx_chunks = output
            .chunks_exact_mut(4)
            .into_remainder()
            .iter_mut()
            .zip(indexes.chunks_exact(4).remainder());
        for (out, idx) in out_and_idx_chunks {
            *out = self.get_val(*idx);
        }
    }

    /// Allows to push down multiple fetch calls, to avoid dynamic dispatch overhead.
    /// The slightly weird `Option<T>` in output allows pushdown to full columns.
    ///
    /// idx and output should have the same length
    ///
    /// # Panics
    ///
    /// May panic if `idx` is greater than the column length.
    fn get_vals_opt(&self, indexes: &[u32], output: &mut [Option<T>]) {
        assert!(indexes.len() == output.len());
        let out_and_idx_chunks = output.chunks_exact_mut(4).zip(indexes.chunks_exact(4));
        for (out_x4, idx_x4) in out_and_idx_chunks {
            out_x4[0] = Some(self.get_val(idx_x4[0]));
            out_x4[1] = Some(self.get_val(idx_x4[1]));
            out_x4[2] = Some(self.get_val(idx_x4[2]));
            out_x4[3] = Some(self.get_val(idx_x4[3]));
        }
        let out_and_idx_chunks = output
            .chunks_exact_mut(4)
            .into_remainder()
            .iter_mut()
            .zip(indexes.chunks_exact(4).remainder());
        for (out, idx) in out_and_idx_chunks {
            *out = Some(self.get_val(*idx));
        }
    }

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// # Panics
    ///
    /// Must panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    #[inline(always)]
    fn get_range(&self, start: u64, output: &mut [T]) {
        for (out, idx) in output.iter_mut().zip(start..) {
            *out = self.get_val(idx as u32);
        }
    }

    /// Get the row ids of values which are in the provided value range.
    ///
    /// Note that position == docid for single value fast fields
    fn get_row_ids_for_value_range(
        &self,
        value_range: RangeInclusive<T>,
        row_id_range: Range<RowId>,
        row_id_hits: &mut Vec<RowId>,
    ) {
        let row_id_range = row_id_range.start..row_id_range.end.min(self.num_vals());
        for idx in row_id_range {
            let val = self.get_val(idx);
            if value_range.contains(&val) {
                row_id_hits.push(idx);
            }
        }
    }

    /// Returns a lower bound for this column of values.
    ///
    /// All values are guaranteed to be higher than `.min_value()`
    /// but this value is not necessary the best boundary value.
    ///
    /// We have
    /// ∀i < self.num_vals(), self.get_val(i) >= self.min_value()
    /// But we don't have necessarily
    /// ∃i < self.num_vals(), self.get_val(i) == self.min_value()
    fn min_value(&self) -> T;

    /// Returns an upper bound for this column of values.
    ///
    /// All values are guaranteed to be lower than `.max_value()`
    /// but this value is not necessary the best boundary value.
    ///
    /// We have
    /// ∀i < self.num_vals(), self.get_val(i) <= self.max_value()
    /// But we don't have necessarily
    /// ∃i < self.num_vals(), self.get_val(i) == self.max_value()
    fn max_value(&self) -> T;

    /// The number of values in the column.
    fn num_vals(&self) -> u32;

    /// Returns a iterator over the data
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = T> + 'a> {
        Box::new((0..self.num_vals()).map(|idx| self.get_val(idx)))
    }
}
downcast_rs::impl_downcast!(sync ColumnValues<T> where T: PartialOrd);

/// Empty column of values.
pub struct EmptyColumnValues;

impl<T: PartialOrd + Default> ColumnValues<T> for EmptyColumnValues {
    fn get_val(&self, _idx: u32) -> T {
        panic!("Internal Error: Called get_val of empty column.")
    }

    fn min_value(&self) -> T {
        T::default()
    }

    fn max_value(&self) -> T {
        T::default()
    }

    fn num_vals(&self) -> u32 {
        0
    }
}

impl<T: Copy + PartialOrd + Debug + 'static> ColumnValues<T> for Arc<dyn ColumnValues<T>> {
    #[inline(always)]
    fn get_val(&self, idx: u32) -> T {
        self.as_ref().get_val(idx)
    }

    #[inline(always)]
    fn get_vals_opt(&self, indexes: &[u32], output: &mut [Option<T>]) {
        self.as_ref().get_vals_opt(indexes, output)
    }

    #[inline(always)]
    fn min_value(&self) -> T {
        self.as_ref().min_value()
    }

    #[inline(always)]
    fn max_value(&self) -> T {
        self.as_ref().max_value()
    }

    #[inline(always)]
    fn num_vals(&self) -> u32 {
        self.as_ref().num_vals()
    }

    #[inline(always)]
    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = T> + 'b> {
        self.as_ref().iter()
    }

    #[inline(always)]
    fn get_range(&self, start: u64, output: &mut [T]) {
        self.as_ref().get_range(start, output)
    }

    #[inline(always)]
    fn get_row_ids_for_value_range(
        &self,
        range: RangeInclusive<T>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        self.as_ref()
            .get_row_ids_for_value_range(range, doc_id_range, positions)
    }
}
