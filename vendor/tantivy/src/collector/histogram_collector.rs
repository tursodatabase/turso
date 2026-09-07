use std::sync::Arc;

use columnar::ColumnValues;
use fastdivide::DividerU64;

use crate::collector::{Collector, SegmentCollector};
use crate::fastfield::{FastFieldNotAvailableError, FastValue};
use crate::schema::Type;
use crate::{DocId, Score};

/// Histogram builds an histogram of the values of a fastfield for the
/// collected DocSet.
///
/// At construction, it is given parameters that define a partition of an interval
/// [min_val, max_val) into N buckets with the same width.
/// The ith bucket is then defined by `[min_val + i * bucket_width, min_val + (i+1) * bucket_width)`
///
/// An histogram is then defined as a `Vec<u64>` of length `num_buckets`, that contains a count of
/// documents for each value bucket.
///
/// See also [`HistogramCollector::new()`].
///
/// # Warning
///
/// f64 fields are not supported.
#[derive(Clone)]
pub struct HistogramCollector {
    min_value: u64,
    num_buckets: usize,
    divider: DividerU64,
    field: String,
}

impl HistogramCollector {
    /// Builds a new HistogramCollector.
    ///
    /// The scale/range of the histogram is not dynamic. It is required to
    /// define it by supplying following parameter:
    ///  - `min_value`: the minimum value that can be recorded in the histogram.
    ///  - `bucket_width`: the length of the interval that is associated with each buckets.
    ///  - `num_buckets`: The overall number of buckets.
    ///
    /// Together, this parameters define a partition of `[min_value, min_value + num_buckets *
    /// bucket_width)` into `num_buckets` intervals of width bucket that we call `bucket`.
    ///
    /// # Disclaimer
    /// This function panics if the field given is of type f64.
    pub fn new<TFastValue: FastValue>(
        field: String,
        min_value: TFastValue,
        bucket_width: u64,
        num_buckets: usize,
    ) -> HistogramCollector {
        let fast_type = TFastValue::to_type();
        assert!(fast_type == Type::U64 || fast_type == Type::I64 || fast_type == Type::Date);
        HistogramCollector {
            min_value: min_value.to_u64(),
            num_buckets,
            field,
            divider: DividerU64::divide_by(bucket_width),
        }
    }
}

struct HistogramComputer {
    counts: Vec<u64>,
    min_value: u64,
    divider: DividerU64,
}

impl HistogramComputer {
    #[inline]
    pub(crate) fn add_value(&mut self, value: u64) {
        if value < self.min_value {
            return;
        }
        let delta = value - self.min_value;
        let bucket_id: usize = self.divider.divide(delta) as usize;
        if bucket_id < self.counts.len() {
            self.counts[bucket_id] += 1;
        }
    }

    fn harvest(self) -> Vec<u64> {
        self.counts
    }
}
pub struct SegmentHistogramCollector {
    histogram_computer: HistogramComputer,
    column_u64: Arc<dyn ColumnValues<u64>>,
}

impl SegmentCollector for SegmentHistogramCollector {
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let value = self.column_u64.get_val(doc);
        self.histogram_computer.add_value(value);
    }

    fn harvest(self) -> Self::Fruit {
        self.histogram_computer.harvest()
    }
}

impl Collector for HistogramCollector {
    type Fruit = Vec<u64>;
    type Child = SegmentHistogramCollector;

    fn for_segment(
        &self,
        _segment_local_id: crate::SegmentOrdinal,
        segment: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let column_opt = segment.fast_fields().u64_lenient(&self.field)?;
        let (column, _column_type) = column_opt.ok_or_else(|| FastFieldNotAvailableError {
            field_name: self.field.clone(),
        })?;
        let column_u64 = column.first_or_default_col(0u64);
        Ok(SegmentHistogramCollector {
            histogram_computer: HistogramComputer {
                counts: vec![0; self.num_buckets],
                min_value: self.min_value,
                divider: self.divider,
            },
            column_u64,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, child_histograms: Vec<Vec<u64>>) -> crate::Result<Vec<u64>> {
        Ok(add_vecs(child_histograms, self.num_buckets))
    }
}

pub fn add_arrays_into(acc: &mut [u64], add: &[u64]) {
    assert_eq!(acc.len(), add.len());
    for (dest_bucket, bucket_count) in acc.iter_mut().zip(add) {
        *dest_bucket += bucket_count;
    }
}

fn add_vecs(mut vals_list: Vec<Vec<u64>>, len: usize) -> Vec<u64> {
    let mut acc = vals_list.pop().unwrap_or_else(|| vec![0u64; len]);
    assert_eq!(acc.len(), len);
    for vals in vals_list {
        add_arrays_into(&mut acc, &vals);
    }
    acc
}

#[cfg(test)]
mod tests {
    use fastdivide::DividerU64;
    use query::AllQuery;

    use super::{add_vecs, HistogramCollector, HistogramComputer};
    use crate::schema::{Schema, FAST};
    use crate::time::{Date, Month};
    use crate::{query, DateTime, Index};

    #[test]
    fn test_add_histograms_simple() {
        assert_eq!(
            add_vecs(vec![vec![1, 0, 3], vec![11, 2, 3], vec![0, 0, 1]], 3),
            vec![12, 2, 7]
        )
    }

    #[test]
    fn test_add_histograms_empty() {
        assert_eq!(add_vecs(vec![], 3), vec![0, 0, 0])
    }

    #[test]
    fn test_histogram_builder_simple() {
        // [1..3)
        // [3..5)
        // ..
        // [9..11)
        let mut histogram_computer = HistogramComputer {
            counts: vec![0; 5],
            min_value: 1,
            divider: DividerU64::divide_by(2),
        };
        histogram_computer.add_value(1);
        histogram_computer.add_value(7);
        assert_eq!(histogram_computer.harvest(), vec![1, 0, 0, 1, 0]);
    }

    #[test]
    fn test_histogram_too_low_is_ignored() {
        let mut histogram_computer = HistogramComputer {
            counts: vec![0; 5],
            min_value: 2,
            divider: DividerU64::divide_by(2),
        };
        histogram_computer.add_value(0);
        assert_eq!(histogram_computer.harvest(), vec![0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_histogram_too_high_is_ignored() {
        let mut histogram_computer = HistogramComputer {
            counts: vec![0u64; 5],
            min_value: 0,
            divider: DividerU64::divide_by(2),
        };
        histogram_computer.add_value(10);
        assert_eq!(histogram_computer.harvest(), vec![0, 0, 0, 0, 0]);
    }
    #[test]
    fn test_no_segments() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("val_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let all_query = AllQuery;
        let histogram_collector = HistogramCollector::new("val_field".to_string(), 0u64, 2, 5);
        let histogram = searcher.search(&all_query, &histogram_collector)?;
        assert_eq!(histogram, vec![0; 5]);
        Ok(())
    }

    #[test]
    fn test_histogram_i64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let val_field = schema_builder.add_i64_field("val_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(val_field=>12i64))?;
        writer.add_document(doc!(val_field=>-30i64))?;
        writer.add_document(doc!(val_field=>-12i64))?;
        writer.add_document(doc!(val_field=>-10i64))?;
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let all_query = AllQuery;
        let histogram_collector =
            HistogramCollector::new("val_field".to_string(), -20i64, 10u64, 4);
        let histogram = searcher.search(&all_query, &histogram_collector)?;
        assert_eq!(histogram, vec![1, 1, 0, 1]);
        Ok(())
    }

    #[test]
    fn test_histogram_merge() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let val_field = schema_builder.add_i64_field("val_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(val_field=>12i64))?;
        writer.commit()?;
        writer.add_document(doc!(val_field=>-30i64))?;
        writer.commit()?;
        writer.add_document(doc!(val_field=>-12i64))?;
        writer.commit()?;
        writer.add_document(doc!(val_field=>-10i64))?;
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let all_query = AllQuery;
        let histogram_collector =
            HistogramCollector::new("val_field".to_string(), -20i64, 10u64, 4);
        let histogram = searcher.search(&all_query, &histogram_collector)?;
        assert_eq!(histogram, vec![1, 1, 0, 1]);
        Ok(())
    }

    #[test]
    fn test_histogram_dates() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1982, Month::September, 17)?.with_hms(0, 0, 0)?)))?;
        writer.add_document(
            doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1986, Month::March, 9)?.with_hms(0, 0, 0)?)),
        )?;
        writer.add_document(doc!(date_field=>DateTime::from_primitive(Date::from_calendar_date(1983, Month::September, 27)?.with_hms(0, 0, 0)?)))?;
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let all_query = AllQuery;
        let week_histogram_collector = HistogramCollector::new(
            "date_field".to_string(),
            DateTime::from_primitive(
                Date::from_calendar_date(1980, Month::January, 1)?.with_hms(0, 0, 0)?,
            ),
            3_600_000_000_000 * 24 * 365, // it is just for a unit test... sorry leap years.
            10,
        );
        let week_histogram = searcher.search(&all_query, &week_histogram_collector)?;
        assert_eq!(week_histogram, vec![0, 0, 1, 1, 0, 0, 1, 0, 0, 0]);
        Ok(())
    }
}
