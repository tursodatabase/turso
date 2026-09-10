use std::net::Ipv6Addr;

use columnar::column_values::{CompactHit, CompactSpaceU64Accessor};
use columnar::{Column, ColumnType, MonotonicallyMappableToU64, StrColumn, TermOrdHit};

use crate::aggregation::accessor_helpers::get_numeric_or_date_column_types;
use crate::aggregation::bucket::composite::numeric_types::num_proj;
use crate::aggregation::bucket::composite::numeric_types::num_proj::ProjectedNumber;
use crate::aggregation::bucket::composite::ToTypePaginationOrder;
use crate::aggregation::bucket::{
    parse_into_milliseconds, CalendarInterval, CompositeAggregation, CompositeAggregationSource,
    MissingOrder, Order,
};
use crate::aggregation::intermediate_agg_result::CompositeIntermediateKey;
use crate::{SegmentReader, TantivyError};

/// Contains all information required by the SegmentCompositeCollector to perform the
/// composite aggregation on a segment.
pub struct CompositeAggReqData {
    /// The name of the aggregation.
    pub name: String,
    /// The normalized term aggregation request.
    pub req: CompositeAggregation,
    /// Accessors for each source, each source can have multiple accessors (columns).
    pub composite_accessors: Vec<CompositeSourceAccessors>,
}

impl CompositeAggReqData {
    /// Estimate the memory consumption of this struct in bytes.
    pub fn get_memory_consumption(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.composite_accessors.len() * std::mem::size_of::<CompositeSourceAccessors>()
    }
}

/// Accessors for a single column in a composite source.
pub struct CompositeAccessor {
    /// The fast field column
    pub column: Column<u64>,
    /// The column type
    pub column_type: ColumnType,
    /// Term dictionary if the column type is Str
    ///
    /// Only used by term sources
    pub str_dict_column: Option<StrColumn>,
    /// Parsed date interval for date histogram sources
    pub date_histogram_interval: PrecomputedDateInterval,
}

/// Accessors to all the columns that belong to the field of a composite source.
pub struct CompositeSourceAccessors {
    /// The accessors for this source
    pub accessors: Vec<CompositeAccessor>,
    /// The key after which to start collecting results. Applies to the first
    /// column of the source.
    pub after_key: PrecomputedAfterKey,

    /// The column index the after_key applies to. The after_key only applies to
    /// one column. Columns before should be skipped. Columns after should be
    /// kept without comparison to the after_key.
    pub after_key_accessor_idx: usize,

    /// Whether to skip missing values because of the after_key. Skipping only
    /// applies if the value for previous columns were exactly equal to the
    /// corresponding after keys (is_on_after_key).
    pub skip_missing: bool,

    /// The after key was set to null to indicate that the last collected key
    /// was a missing value.
    pub is_after_key_explicit_missing: bool,
}

impl CompositeSourceAccessors {
    /// Creates a new set of accessors for the composite source.
    ///
    /// Precomputes some values to make collection faster.
    pub fn build_for_source(
        reader: &SegmentReader,
        source: &CompositeAggregationSource,
        // First option is None when no after key was set in the query, the
        // second option is None when the after key was set but its value for
        // this source was set to `null`
        source_after_key_opt: Option<&CompositeIntermediateKey>,
    ) -> crate::Result<Self> {
        let is_after_key_explicit_missing = source_after_key_opt
            .map(|after_key| matches!(after_key, CompositeIntermediateKey::Null))
            .unwrap_or(false);
        let mut skip_missing = false;
        if let Some(CompositeIntermediateKey::Null) = source_after_key_opt {
            if !source.missing_bucket() {
                return Err(TantivyError::InvalidArgument(
                    "the 'after' key for a source cannot be null when 'missing_bucket' is false"
                        .to_string(),
                ));
            }
        } else if source_after_key_opt.is_some() {
            // if missing buckets come first and we have a non null after key, we skip missing
            if MissingOrder::First == source.missing_order() {
                skip_missing = true;
            }
            if MissingOrder::Default == source.missing_order() && Order::Asc == source.order() {
                skip_missing = true;
            }
        };

        match source {
            CompositeAggregationSource::Terms(source) => {
                let allowed_column_types = [
                    ColumnType::I64,
                    ColumnType::U64,
                    ColumnType::F64,
                    ColumnType::Str,
                    ColumnType::DateTime,
                    ColumnType::Bool,
                    ColumnType::IpAddr,
                    // ColumnType::Bytes Unsupported
                ];
                let mut columns_and_types = reader
                    .fast_fields()
                    .u64_lenient_for_type_all(Some(&allowed_column_types), &source.field)?;

                // Sort columns by their pagination order and determine which to skip
                columns_and_types.sort_by_key(|(_, col_type): &(Column, ColumnType)| {
                    col_type.column_pagination_order()
                });
                if source.order == Order::Desc {
                    columns_and_types.reverse();
                }
                let after_key_accessor_idx = find_first_column_to_collect(
                    &columns_and_types,
                    source_after_key_opt,
                    source.missing_order,
                    source.order,
                )?;

                let source_collectors: Vec<CompositeAccessor> = columns_and_types
                    .into_iter()
                    .map(|(column, column_type)| {
                        Ok(CompositeAccessor {
                            column,
                            column_type,
                            str_dict_column: reader.fast_fields().str(&source.field)?,
                            date_histogram_interval: PrecomputedDateInterval::NotApplicable,
                        })
                    })
                    .collect::<crate::Result<_>>()?;

                let after_key = if let Some(first_col) =
                    source_collectors.get(after_key_accessor_idx)
                {
                    match source_after_key_opt {
                        Some(after_key) => PrecomputedAfterKey::precompute(
                            first_col,
                            after_key,
                            &source.field,
                            source.missing_order,
                            source.order,
                        )?,
                        None => {
                            precompute_missing_after_key(false, source.missing_order, source.order)
                        }
                    }
                } else {
                    // if no columns, we don't care about the after_key
                    PrecomputedAfterKey::Next(0)
                };

                Ok(CompositeSourceAccessors {
                    accessors: source_collectors,
                    is_after_key_explicit_missing,
                    skip_missing,
                    after_key,
                    after_key_accessor_idx,
                })
            }
            CompositeAggregationSource::Histogram(source) => {
                let column_and_types: Vec<(Column, ColumnType)> =
                    reader.fast_fields().u64_lenient_for_type_all(
                        Some(get_numeric_or_date_column_types()),
                        &source.field,
                    )?;
                let source_collectors: Vec<CompositeAccessor> = column_and_types
                    .into_iter()
                    .map(|(column, column_type)| {
                        Ok(CompositeAccessor {
                            column,
                            column_type,
                            str_dict_column: None,
                            date_histogram_interval: PrecomputedDateInterval::NotApplicable,
                        })
                    })
                    .collect::<crate::Result<_>>()?;
                let after_key = match source_after_key_opt {
                    Some(CompositeIntermediateKey::F64(key)) => {
                        let normalized_key = *key / source.interval;
                        num_proj::f64_to_i64(normalized_key).into()
                    }
                    Some(CompositeIntermediateKey::Null) => {
                        precompute_missing_after_key(true, source.missing_order, source.order)
                    }
                    None => precompute_missing_after_key(true, source.missing_order, source.order),
                    _ => {
                        return Err(crate::TantivyError::InvalidArgument(
                            "After key type invalid for interval composite source".to_string(),
                        ));
                    }
                };
                Ok(CompositeSourceAccessors {
                    accessors: source_collectors,
                    is_after_key_explicit_missing,
                    skip_missing,
                    after_key,
                    after_key_accessor_idx: 0,
                })
            }
            CompositeAggregationSource::DateHistogram(source) => {
                let column_and_types = reader
                    .fast_fields()
                    .u64_lenient_for_type_all(Some(&[ColumnType::DateTime]), &source.field)?;
                let date_histogram_interval =
                    PrecomputedDateInterval::from_date_histogram_source_intervals(
                        &source.fixed_interval,
                        source.calendar_interval,
                    )?;
                let source_collectors: Vec<CompositeAccessor> = column_and_types
                    .into_iter()
                    .map(|(column, column_type)| {
                        Ok(CompositeAccessor {
                            column,
                            column_type,
                            str_dict_column: None,
                            date_histogram_interval,
                        })
                    })
                    .collect::<crate::Result<_>>()?;
                let after_key = match source_after_key_opt {
                    Some(CompositeIntermediateKey::DateTime(key)) => {
                        PrecomputedAfterKey::Exact(key.to_u64())
                    }
                    Some(CompositeIntermediateKey::Null) => {
                        precompute_missing_after_key(true, source.missing_order, source.order)
                    }
                    None => precompute_missing_after_key(true, source.missing_order, source.order),
                    _ => {
                        return Err(crate::TantivyError::InvalidArgument(
                            "After key type invalid for interval composite source".to_string(),
                        ));
                    }
                };
                Ok(CompositeSourceAccessors {
                    accessors: source_collectors,
                    is_after_key_explicit_missing,
                    skip_missing,
                    after_key,
                    after_key_accessor_idx: 0,
                })
            }
        }
    }
}

/// Finds the index of the first column we should start collecting from to
/// resume the pagination from the after_key.
fn find_first_column_to_collect<T>(
    sorted_columns: &[(T, ColumnType)],
    after_key_opt: Option<&CompositeIntermediateKey>,
    missing_order: MissingOrder,
    order: Order,
) -> crate::Result<usize> {
    let after_key = match after_key_opt {
        None => return Ok(0), // No pagination, start from beginning
        Some(key) => key,
    };
    // Handle null after_key (we were on a missing value last time)
    if matches!(after_key, CompositeIntermediateKey::Null) {
        return match (missing_order, order) {
            // Missing values come first, so all columns remain
            (MissingOrder::First, _) | (MissingOrder::Default, Order::Asc) => Ok(0),
            // Missing values come last, so all columns are done
            (MissingOrder::Last, _) | (MissingOrder::Default, Order::Desc) => {
                Ok(sorted_columns.len())
            }
        };
    }
    // Find the first column whose type order matches or follows the after_key's
    // type in the pagination sequence
    let after_key_column_order = after_key.column_pagination_order();
    for (idx, (_, col_type)) in sorted_columns.iter().enumerate() {
        let col_order = col_type.column_pagination_order();
        let is_first_to_collect = match order {
            Order::Asc => col_order >= after_key_column_order,
            Order::Desc => col_order <= after_key_column_order,
        };
        if is_first_to_collect {
            return Ok(idx);
        }
    }
    // All columns are before the after_key, nothing left to collect
    Ok(sorted_columns.len())
}

fn precompute_missing_after_key(
    is_after_key_explicit_missing: bool,
    missing_order: MissingOrder,
    order: Order,
) -> PrecomputedAfterKey {
    let after_last = PrecomputedAfterKey::AfterLast;
    let before_first = PrecomputedAfterKey::Next(0);
    match (is_after_key_explicit_missing, missing_order, order) {
        (true, MissingOrder::First, Order::Asc) => before_first,
        (true, MissingOrder::First, Order::Desc) => after_last,
        (true, MissingOrder::Last, Order::Asc) => after_last,
        (true, MissingOrder::Last, Order::Desc) => before_first,
        (true, MissingOrder::Default, Order::Asc) => before_first,
        (true, MissingOrder::Default, Order::Desc) => after_last,
        (false, _, Order::Asc) => before_first,
        (false, _, Order::Desc) => after_last,
    }
}

/// A parsed representation of the date interval for date histogram sources
#[derive(Clone, Copy, Debug)]
pub enum PrecomputedDateInterval {
    /// This is not a date histogram source
    NotApplicable,
    /// Source was configured with a fixed interval
    FixedNanoseconds(i64),
    /// Source was configured with a calendar interval
    Calendar(CalendarInterval),
}

impl PrecomputedDateInterval {
    /// Validates the date histogram source interval fields and parses a date interval from them.
    pub fn from_date_histogram_source_intervals(
        fixed_interval: &Option<String>,
        calendar_interval: Option<CalendarInterval>,
    ) -> crate::Result<Self> {
        match (fixed_interval, calendar_interval) {
            (Some(_), Some(_)) | (None, None) => Err(TantivyError::InvalidArgument(
                "date histogram source must one and only one of fixed_interval or \
                 calendar_interval set"
                    .to_string(),
            )),
            (Some(fixed_interval), None) => {
                let fixed_interval_ms = parse_into_milliseconds(fixed_interval)?;
                Ok(PrecomputedDateInterval::FixedNanoseconds(
                    fixed_interval_ms * 1_000_000,
                ))
            }
            (None, Some(calendar_interval)) => {
                Ok(PrecomputedDateInterval::Calendar(calendar_interval))
            }
        }
    }
}

/// The after key projected to the u64 column space
///
/// Some column types (term, IP) might not have an exact representation of the
/// specified after key
#[derive(Debug)]
pub enum PrecomputedAfterKey {
    /// The after key could be exactly represented in the column space.
    Exact(u64),
    /// The after key could not be exactly represented exactly represented, so
    /// this is the next closest one.
    Next(u64),
    /// The after key could not be represented in the column space, it is
    /// greater than all value
    AfterLast,
}

impl From<CompactHit> for PrecomputedAfterKey {
    fn from(hit: CompactHit) -> Self {
        match hit {
            CompactHit::Exact(ord) => PrecomputedAfterKey::Exact(ord as u64),
            CompactHit::Next(ord) => PrecomputedAfterKey::Next(ord as u64),
            CompactHit::AfterLast => PrecomputedAfterKey::AfterLast,
        }
    }
}

impl From<TermOrdHit> for PrecomputedAfterKey {
    fn from(hit: TermOrdHit) -> Self {
        match hit {
            TermOrdHit::Exact(ord) => PrecomputedAfterKey::Exact(ord),
            // TermOrdHit represents AfterLast as Next(u64::MAX), we keep it as is
            TermOrdHit::Next(ord) => PrecomputedAfterKey::Next(ord),
        }
    }
}

impl<T: MonotonicallyMappableToU64> From<ProjectedNumber<T>> for PrecomputedAfterKey {
    fn from(num: ProjectedNumber<T>) -> Self {
        match num {
            ProjectedNumber::Exact(number) => PrecomputedAfterKey::Exact(number.to_u64()),
            ProjectedNumber::Next(number) => PrecomputedAfterKey::Next(number.to_u64()),
            ProjectedNumber::AfterLast => PrecomputedAfterKey::AfterLast,
        }
    }
}

// /!\ These operators only makes sense if both values are in the same column space
impl PrecomputedAfterKey {
    pub fn equals(&self, column_value: u64) -> bool {
        match self {
            PrecomputedAfterKey::Exact(v) => *v == column_value,
            PrecomputedAfterKey::Next(_) => false,
            PrecomputedAfterKey::AfterLast => false,
        }
    }

    pub fn gt(&self, column_value: u64) -> bool {
        match self {
            PrecomputedAfterKey::Exact(v) => *v > column_value,
            PrecomputedAfterKey::Next(v) => *v > column_value,
            PrecomputedAfterKey::AfterLast => true,
        }
    }

    pub fn lt(&self, column_value: u64) -> bool {
        match self {
            PrecomputedAfterKey::Exact(v) => *v < column_value,
            // a value equal to the next is greater than the after key
            PrecomputedAfterKey::Next(v) => *v <= column_value,
            PrecomputedAfterKey::AfterLast => false,
        }
    }

    fn precompute_ip_addr(column: &Column<u64>, key: &Ipv6Addr) -> crate::Result<Self> {
        let compact_space_accessor = column
            .values
            .clone()
            .downcast_arc::<CompactSpaceU64Accessor>()
            .map_err(|_| {
                TantivyError::AggregationError(crate::aggregation::AggregationError::InternalError(
                    "type mismatch: could not downcast to CompactSpaceU64Accessor".to_string(),
                ))
            })?;
        let ip_u128 = key.to_bits();
        let ip_next_compact = compact_space_accessor.u128_to_next_compact(ip_u128);
        Ok(ip_next_compact.into())
    }

    fn precompute_term_ord(
        str_dict_column: &Option<StrColumn>,
        key: &str,
        field: &str,
    ) -> crate::Result<Self> {
        let dict = str_dict_column
            .as_ref()
            .expect("dictionary missing for str accessor")
            .dictionary();
        let next_ord = dict.term_ord_or_next(key).map_err(|_| {
            TantivyError::InvalidArgument(format!(
                "failed to lookup after_key '{}' for field '{}'",
                key, field
            ))
        })?;
        Ok(next_ord.into())
    }

    /// Projects the after key into the column space of the given accessor.
    ///
    /// The computed after key will not take care of skipping entire columns
    /// when the after key type is ordered after the accessor's type, that
    /// should be performed earlier.
    pub fn precompute(
        composite_accessor: &CompositeAccessor,
        source_after_key: &CompositeIntermediateKey,
        field: &str,
        missing_order: MissingOrder,
        order: Order,
    ) -> crate::Result<Self> {
        use CompositeIntermediateKey as CIKey;
        let precomputed_key = match (composite_accessor.column_type, source_after_key) {
            (ColumnType::Bytes, _) => panic!("unsupported"),
            // null after key
            (_, CIKey::Null) => precompute_missing_after_key(false, missing_order, order),
            // numerical
            (ColumnType::I64, CIKey::I64(k)) => PrecomputedAfterKey::Exact(k.to_u64()),
            (ColumnType::I64, CIKey::U64(k)) => num_proj::u64_to_i64(*k).into(),
            (ColumnType::I64, CIKey::F64(k)) => num_proj::f64_to_i64(*k).into(),
            (ColumnType::U64, CIKey::I64(k)) => num_proj::i64_to_u64(*k).into(),
            (ColumnType::U64, CIKey::U64(k)) => PrecomputedAfterKey::Exact(*k),
            (ColumnType::U64, CIKey::F64(k)) => num_proj::f64_to_u64(*k).into(),
            (ColumnType::F64, CIKey::I64(k)) => num_proj::i64_to_f64(*k).into(),
            (ColumnType::F64, CIKey::U64(k)) => num_proj::u64_to_f64(*k).into(),
            (ColumnType::F64, CIKey::F64(k)) => PrecomputedAfterKey::Exact(k.to_u64()),
            // boolean
            (ColumnType::Bool, CIKey::Bool(key)) => PrecomputedAfterKey::Exact(key.to_u64()),
            // string
            (ColumnType::Str, CIKey::Str(key)) => PrecomputedAfterKey::precompute_term_ord(
                &composite_accessor.str_dict_column,
                key,
                field,
            )?,
            // date time
            (ColumnType::DateTime, CIKey::DateTime(key)) => {
                PrecomputedAfterKey::Exact(key.to_u64())
            }
            // ip address
            (ColumnType::IpAddr, CIKey::IpAddr(key)) => {
                PrecomputedAfterKey::precompute_ip_addr(&composite_accessor.column, key)?
            }
            // assume the column's type is ordered after the after_key's type
            _ => PrecomputedAfterKey::keep_all(order),
        };
        Ok(precomputed_key)
    }

    fn keep_all(order: Order) -> Self {
        match order {
            Order::Asc => PrecomputedAfterKey::Next(0),
            Order::Desc => PrecomputedAfterKey::Next(u64::MAX),
        }
    }
}
