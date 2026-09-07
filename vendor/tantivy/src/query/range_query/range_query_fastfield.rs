//! Fastfields support efficient scanning for range queries.
//! We use this variant only if the fastfield exists, otherwise the default in `range_query` is
//! used, which uses the term dictionary + postings.

use std::net::Ipv6Addr;
use std::ops::{Bound, RangeInclusive};

use columnar::{
    BytesColumn, Cardinality, Column, ColumnType, MonotonicallyMappableToU128,
    MonotonicallyMappableToU64, NumericalType, StrColumn,
};
use common::bounds::{BoundsRange, TransformBound};

use super::fast_field_range_doc_set::RangeDocSet;
use crate::query::{
    AllScorer, ConstScorer, EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight,
};
use crate::schema::{Type, ValueBytes};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError, Term};

#[derive(Clone, Debug)]
/// `FastFieldRangeQuery` is the same as [RangeQuery] but only uses the fast field
pub struct FastFieldRangeQuery {
    bounds: BoundsRange<Term>,
}
impl FastFieldRangeQuery {
    /// Create new `FastFieldRangeQuery`
    pub fn new(lower_bound: Bound<Term>, upper_bound: Bound<Term>) -> FastFieldRangeQuery {
        Self {
            bounds: BoundsRange::new(lower_bound, upper_bound),
        }
    }
}

impl Query for FastFieldRangeQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(FastFieldRangeWeight::new(self.bounds.clone())))
    }
}

/// `FastFieldRangeWeight` uses the fast field to execute range queries.
#[derive(Clone, Debug)]
pub struct FastFieldRangeWeight {
    bounds: BoundsRange<Term>,
}

impl FastFieldRangeWeight {
    /// Create a new FastFieldRangeWeight
    pub fn new(bounds: BoundsRange<Term>) -> Self {
        Self { bounds }
    }
}

impl Weight for FastFieldRangeWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        // Check if both bounds are Bound::Unbounded
        if self.bounds.is_unbounded() {
            return Ok(Box::new(AllScorer::new(reader.max_doc())));
        }

        let term = self
            .bounds
            .get_inner()
            .expect("At least one bound must be set");
        let schema = reader.schema();
        let field_type = schema.get_field_entry(term.field()).field_type();
        assert_eq!(
            term.typ(),
            field_type.value_type(),
            "Field is of type {:?}, but got term of type {:?}",
            field_type,
            term.typ()
        );
        let field_name = term.get_full_path(reader.schema());

        let get_value_bytes = |term: &Term| term.value().value_bytes_payload();

        let term_value = term.value();
        if field_type.is_json() {
            let bounds = self
                .bounds
                .map_bound(|term| term.value().as_json_value_bytes().unwrap().to_owned());
            // Unlike with other field types JSON may have multiple columns of different types
            // under the same name
            //
            // In the JSON case the provided type in term may not exactly match the column type,
            // especially with the numeric type interpolation
            let json_value_bytes = term_value
                .as_json_value_bytes()
                .expect("expected json type in term");
            let typ = json_value_bytes.typ();

            match typ {
                Type::Str => {
                    let Some(str_dict_column): Option<StrColumn> =
                        reader.fast_fields().str(&field_name)?
                    else {
                        return Ok(Box::new(EmptyScorer));
                    };
                    let dict = str_dict_column.dictionary();

                    let bounds = self.bounds.map_bound(get_value_bytes);
                    // Get term ids for terms
                    let (lower_bound, upper_bound) =
                        dict.term_bounds_to_ord(bounds.lower_bound, bounds.upper_bound)?;
                    let fast_field_reader = reader.fast_fields();
                    let Some((column, _col_type)) = fast_field_reader
                        .u64_lenient_for_type(Some(&[ColumnType::Str]), &field_name)?
                    else {
                        return Ok(Box::new(EmptyScorer));
                    };
                    search_on_u64_ff(column, boost, BoundsRange::new(lower_bound, upper_bound))
                }
                Type::U64 | Type::I64 | Type::F64 => {
                    search_on_json_numerical_field(reader, &field_name, typ, bounds, boost)
                }
                Type::Date => {
                    let fast_field_reader = reader.fast_fields();
                    let Some((column, _col_type)) = fast_field_reader
                        .u64_lenient_for_type(Some(&[ColumnType::DateTime]), &field_name)?
                    else {
                        return Ok(Box::new(EmptyScorer));
                    };
                    let bounds = bounds.map_bound(|term| term.as_date().unwrap().to_u64());
                    search_on_u64_ff(
                        column,
                        boost,
                        BoundsRange::new(bounds.lower_bound, bounds.upper_bound),
                    )
                }
                Type::Bool | Type::Facet | Type::Bytes | Type::Json | Type::IpAddr => {
                    Err(crate::TantivyError::InvalidArgument(format!(
                        "unsupported value bytes type in json term value_bytes {:?}",
                        term_value.typ()
                    )))
                }
            }
        } else if field_type.is_ip_addr() {
            let parse_ip_from_bytes = |term: &Term| {
                term.value().as_ip_addr().ok_or_else(|| {
                    crate::TantivyError::InvalidArgument("Expected ip address".to_string())
                })
            };
            let bounds: BoundsRange<Ipv6Addr> = self.bounds.map_bound_res(parse_ip_from_bytes)?;

            let Some(ip_addr_column): Option<Column<Ipv6Addr>> =
                reader.fast_fields().column_opt(&field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            let value_range = bound_range_inclusive_ip(
                &bounds.lower_bound,
                &bounds.upper_bound,
                ip_addr_column.min_value(),
                ip_addr_column.max_value(),
            );
            let docset = RangeDocSet::new(value_range, ip_addr_column);
            Ok(Box::new(ConstScorer::new(docset, boost)))
        } else if field_type.is_str() {
            let Some(str_dict_column): Option<StrColumn> = reader.fast_fields().str(&field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            let dict = str_dict_column.dictionary();

            let bounds = self.bounds.map_bound(get_value_bytes);
            // Get term ids for terms
            let (lower_bound, upper_bound) =
                dict.term_bounds_to_ord(bounds.lower_bound, bounds.upper_bound)?;
            let fast_field_reader = reader.fast_fields();
            let Some((column, _col_type)) =
                fast_field_reader.u64_lenient_for_type(None, &field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            search_on_u64_ff(column, boost, BoundsRange::new(lower_bound, upper_bound))
        } else if field_type.is_bytes() {
            let Some(bytes_column): Option<BytesColumn> =
                reader.fast_fields().bytes(&field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            let dict = bytes_column.dictionary();

            let bounds = self.bounds.map_bound(get_value_bytes);
            // Get term ids for terms
            let (lower_bound, upper_bound) =
                dict.term_bounds_to_ord(bounds.lower_bound, bounds.upper_bound)?;
            let fast_field_reader = reader.fast_fields();
            let Some((column, _col_type)) =
                fast_field_reader.u64_lenient_for_type(None, &field_name)?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            search_on_u64_ff(column, boost, BoundsRange::new(lower_bound, upper_bound))
        } else {
            assert!(
                maps_to_u64_fastfield(field_type.value_type()),
                "{field_type:?}"
            );

            let bounds = self.bounds.map_bound_res(|term| {
                let value = term.value();
                let val = if let Some(val) = value.as_u64() {
                    val
                } else if let Some(val) = value.as_i64() {
                    val.to_u64()
                } else if let Some(val) = value.as_f64() {
                    val.to_u64()
                } else if let Some(val) = value.as_date() {
                    val.to_u64()
                } else {
                    return Err(TantivyError::InvalidArgument(format!(
                        "Expected term with u64, i64, f64 or date, but got {term:?}"
                    )));
                };
                Ok(val)
            })?;

            let fast_field_reader = reader.fast_fields();
            let Some((column, _col_type)) = fast_field_reader.u64_lenient_for_type(
                Some(&[
                    ColumnType::U64,
                    ColumnType::I64,
                    ColumnType::F64,
                    ColumnType::DateTime,
                ]),
                &field_name,
            )?
            else {
                return Ok(Box::new(EmptyScorer));
            };
            search_on_u64_ff(
                column,
                boost,
                BoundsRange::new(bounds.lower_bound, bounds.upper_bound),
            )
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let explanation = Explanation::new("Const", scorer.score());

        Ok(explanation)
    }
}

/// On numerical fields the column type may not match the user provided one.
///
/// Convert into fast field value space and search.
fn search_on_json_numerical_field(
    reader: &SegmentReader,
    field_name: &str,
    typ: Type,
    bounds: BoundsRange<ValueBytes<Vec<u8>>>,
    boost: Score,
) -> crate::Result<Box<dyn Scorer>> {
    // Since we don't know which type was interpolated for the internal column we
    // have to check for all numeric types (only one exists)
    let allowed_column_types: Option<&[ColumnType]> =
        Some(&[ColumnType::F64, ColumnType::I64, ColumnType::U64]);
    let fast_field_reader = reader.fast_fields();
    let Some((column, col_type)) =
        fast_field_reader.u64_lenient_for_type(allowed_column_types, field_name)?
    else {
        return Ok(Box::new(EmptyScorer));
    };
    let actual_column_type: NumericalType = col_type
        .numerical_type()
        .unwrap_or_else(|| panic!("internal error: couldn't cast to numerical_type: {col_type:?}"));

    let bounds = match typ.numerical_type().unwrap() {
        NumericalType::I64 => {
            let bounds = bounds.map_bound(|term| term.as_i64().unwrap());
            match actual_column_type {
                NumericalType::I64 => bounds.map_bound(|&term| term.to_u64()),
                NumericalType::U64 => {
                    bounds.transform_inner(
                        |&val| {
                            if val < 0 {
                                return TransformBound::NewBound(Bound::Unbounded);
                            }
                            TransformBound::Existing(val as u64)
                        },
                        |&val| {
                            if val < 0 {
                                // no hits case
                                return TransformBound::NewBound(Bound::Excluded(0));
                            }
                            TransformBound::Existing(val as u64)
                        },
                    )
                }
                NumericalType::F64 => bounds.map_bound(|&term| (term as f64).to_u64()),
            }
        }
        NumericalType::U64 => {
            let bounds = bounds.map_bound(|term| term.as_u64().unwrap());
            match actual_column_type {
                NumericalType::U64 => bounds.map_bound(|&term| term.to_u64()),
                NumericalType::I64 => {
                    bounds.transform_inner(
                        |&val| {
                            if val > i64::MAX as u64 {
                                // Actual no hits case
                                return TransformBound::NewBound(Bound::Excluded(i64::MAX as u64));
                            }
                            TransformBound::Existing((val as i64).to_u64())
                        },
                        |&val| {
                            if val > i64::MAX as u64 {
                                return TransformBound::NewBound(Bound::Unbounded);
                            }
                            TransformBound::Existing((val as i64).to_u64())
                        },
                    )
                }
                NumericalType::F64 => bounds.map_bound(|&term| (term as f64).to_u64()),
            }
        }
        NumericalType::F64 => {
            let bounds = bounds.map_bound(|term| term.as_f64().unwrap());
            match actual_column_type {
                NumericalType::U64 => transform_from_f64_bounds::<u64>(&bounds),
                NumericalType::I64 => transform_from_f64_bounds::<i64>(&bounds),
                NumericalType::F64 => bounds.map_bound(|&term| term.to_u64()),
            }
        }
    };
    search_on_u64_ff(
        column,
        boost,
        BoundsRange::new(bounds.lower_bound, bounds.upper_bound),
    )
}

trait IntType {
    fn min() -> Self;
    fn max() -> Self;
    fn to_f64(self) -> f64;
    fn from_f64(val: f64) -> Self;
}
impl IntType for i64 {
    fn min() -> Self {
        Self::MIN
    }
    fn max() -> Self {
        Self::MAX
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(val: f64) -> Self {
        val as Self
    }
}
impl IntType for u64 {
    fn min() -> Self {
        Self::MIN
    }
    fn max() -> Self {
        Self::MAX
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(val: f64) -> Self {
        val as Self
    }
}

fn transform_from_f64_bounds<T: IntType + MonotonicallyMappableToU64>(
    bounds: &BoundsRange<f64>,
) -> BoundsRange<u64> {
    bounds.transform_inner(
        |&lower_bound| {
            if lower_bound < T::min().to_f64() {
                return TransformBound::NewBound(Bound::Unbounded);
            }
            if lower_bound > T::max().to_f64() {
                // no hits case
                return TransformBound::NewBound(Bound::Excluded(u64::MAX));
            }

            if lower_bound.fract() == 0.0 {
                TransformBound::Existing(T::from_f64(lower_bound).to_u64())
            } else {
                TransformBound::NewBound(Bound::Included(T::from_f64(lower_bound.trunc()).to_u64()))
            }
        },
        |&upper_bound| {
            if upper_bound < T::min().to_f64() {
                return TransformBound::NewBound(Bound::Unbounded);
            }
            if upper_bound > T::max().to_f64() {
                // no hits case
                return TransformBound::NewBound(Bound::Included(u64::MAX));
            }
            if upper_bound.fract() == 0.0 {
                TransformBound::Existing(T::from_f64(upper_bound).to_u64())
            } else {
                TransformBound::NewBound(Bound::Included(T::from_f64(upper_bound.trunc()).to_u64()))
            }
        },
    )
}

fn search_on_u64_ff(
    column: Column<u64>,
    boost: Score,
    bounds: BoundsRange<u64>,
) -> crate::Result<Box<dyn Scorer>> {
    let col_min_value = column.min_value();
    let col_max_value = column.max_value();
    #[expect(clippy::reversed_empty_ranges)]
    let value_range = bound_to_value_range(
        &bounds.lower_bound,
        &bounds.upper_bound,
        column.min_value(),
        column.max_value(),
    )
    .unwrap_or(1..=0); // empty range
    if value_range.is_empty() {
        return Ok(Box::new(EmptyScorer));
    }
    if col_min_value >= *value_range.start() && col_max_value <= *value_range.end() {
        // all values in the column are within the range.
        if column.index.get_cardinality() == Cardinality::Full {
            if boost != 1.0f32 {
                return Ok(Box::new(ConstScorer::new(
                    AllScorer::new(column.num_docs()),
                    boost,
                )));
            } else {
                return Ok(Box::new(AllScorer::new(column.num_docs())));
            }
        } else {
            // TODO Make it a field presence request for that specific column
        }
    }

    let docset = RangeDocSet::new(value_range, column);
    Ok(Box::new(ConstScorer::new(docset, boost)))
}

/// Returns true if the type maps to a u64 fast field
pub(crate) fn maps_to_u64_fastfield(typ: Type) -> bool {
    match typ {
        Type::U64 | Type::I64 | Type::F64 | Type::Bool | Type::Date => true,
        Type::IpAddr => false,
        Type::Str | Type::Facet | Type::Bytes | Type::Json => false,
    }
}

fn bound_range_inclusive_ip(
    lower_bound: &Bound<Ipv6Addr>,
    upper_bound: &Bound<Ipv6Addr>,
    min_value: Ipv6Addr,
    max_value: Ipv6Addr,
) -> RangeInclusive<Ipv6Addr> {
    let start_value = match lower_bound {
        Bound::Included(ip_addr) => *ip_addr,
        Bound::Excluded(ip_addr) => Ipv6Addr::from(ip_addr.to_u128() + 1),
        Bound::Unbounded => min_value,
    };

    let end_value = match upper_bound {
        Bound::Included(ip_addr) => *ip_addr,
        Bound::Excluded(ip_addr) => Ipv6Addr::from(ip_addr.to_u128() - 1),
        Bound::Unbounded => max_value,
    };
    start_value..=end_value
}

// Returns None, if the range cannot be converted to a inclusive range (which equals to a empty
// range).
fn bound_to_value_range<T: MonotonicallyMappableToU64>(
    lower_bound: &Bound<T>,
    upper_bound: &Bound<T>,
    min_value: T,
    max_value: T,
) -> Option<RangeInclusive<T>> {
    let mut start_value = match lower_bound {
        Bound::Included(val) => *val,
        Bound::Excluded(val) => T::from_u64(val.to_u64().checked_add(1)?),
        Bound::Unbounded => min_value,
    };
    if start_value.partial_cmp(&min_value) == Some(std::cmp::Ordering::Less) {
        start_value = min_value;
    }
    let end_value = match upper_bound {
        Bound::Included(val) => *val,
        Bound::Excluded(val) => T::from_u64(val.to_u64().checked_sub(1)?),
        Bound::Unbounded => max_value,
    };
    Some(start_value..=end_value)
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeInclusive};

    use common::bounds::BoundsRange;
    use common::DateTime;
    use proptest::prelude::*;
    use rand::rngs::StdRng;
    use rand::seq::IndexedRandom;
    use rand::SeedableRng;
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    use crate::collector::{Count, TopDocs};
    use crate::fastfield::FastValue;
    use crate::query::range_query::range_query_fastfield::FastFieldRangeWeight;
    use crate::query::{QueryParser, RangeQuery, Weight};
    use crate::schema::{
        DateOptions, Field, NumericOptions, Schema, SchemaBuilder, FAST, INDEXED, STORED, STRING,
        TEXT,
    };
    use crate::{Index, IndexWriter, TantivyDocument, Term, TERMINATED};

    #[test]
    fn test_text_field_ff_range_query() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_for_tests()?;
        let title = schema.get_field("title").unwrap();
        index_writer.add_document(doc!(
          title => "bbb"
        ))?;
        index_writer.add_document(doc!(
          title => "ddd"
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![title]);

        let test_query = |query, num_hits| {
            let query = query_parser.parse_query(query).unwrap();
            let top_docs = searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap();
            assert_eq!(top_docs.len(), num_hits);
        };

        test_query("title:[aaa TO ccc]", 1);
        test_query("title:[aaa TO bbb]", 1);
        test_query("title:[bbb TO bbb]", 1);
        test_query("title:[bbb TO ddd]", 2);
        test_query("title:[bbb TO eee]", 2);
        test_query("title:[bb TO eee]", 2);
        test_query("title:[ccc TO ccc]", 0);
        test_query("title:[ccc TO ddd]", 1);
        test_query("title:[ccc TO eee]", 1);

        test_query("title:[aaa TO *}", 2);
        test_query("title:[bbb TO *]", 2);
        test_query("title:[bb TO *]", 2);
        test_query("title:[ccc TO *]", 1);
        test_query("title:[ddd TO *]", 1);
        test_query("title:[dddd TO *]", 0);

        test_query("title:{aaa TO *}", 2);
        test_query("title:{bbb TO *]", 1);
        test_query("title:{bb TO *]", 2);
        test_query("title:{ccc TO *]", 1);
        test_query("title:{ddd TO *]", 0);
        test_query("title:{dddd TO *]", 0);

        test_query("title:[* TO bb]", 0);
        test_query("title:[* TO bbb]", 1);
        test_query("title:[* TO ccc]", 1);
        test_query("title:[* TO ddd]", 2);
        test_query("title:[* TO ddd}", 1);
        test_query("title:[* TO eee]", 2);

        Ok(())
    }

    #[test]
    fn test_date_range_query() {
        let mut schema_builder = Schema::builder();
        let options = DateOptions::default()
            .set_precision(common::DateTimePrecision::Microseconds)
            .set_fast();
        let date_field = schema_builder.add_date_field("date", options);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema.clone());
        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
            // This is added a string and creates a string column!
            index_writer
                .add_document(doc!(date_field => DateTime::from_utc(
                    OffsetDateTime::parse("2022-12-01T00:00:01Z", &Rfc3339).unwrap(),
                )))
                .unwrap();
            index_writer
                .add_document(doc!(date_field => DateTime::from_utc(
                    OffsetDateTime::parse("2023-12-01T00:00:01Z", &Rfc3339).unwrap(),
                )))
                .unwrap();
            index_writer
                .add_document(doc!(date_field => DateTime::from_utc(
                    OffsetDateTime::parse("2015-02-01T00:00:00.001Z", &Rfc3339).unwrap(),
                )))
                .unwrap();
            index_writer.commit().unwrap();
        }

        // Date field
        let dt1 =
            DateTime::from_utc(OffsetDateTime::parse("2022-12-01T00:00:01Z", &Rfc3339).unwrap());
        let dt2 =
            DateTime::from_utc(OffsetDateTime::parse("2023-12-01T00:00:01Z", &Rfc3339).unwrap());
        let dt3 = DateTime::from_utc(
            OffsetDateTime::parse("2015-02-01T00:00:00.001Z", &Rfc3339).unwrap(),
        );
        let dt4 = DateTime::from_utc(
            OffsetDateTime::parse("2015-02-01T00:00:00.002Z", &Rfc3339).unwrap(),
        );

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![date_field]);
        let test_query = |query, num_hits| {
            let query = query_parser.parse_query(query).unwrap();
            let top_docs = searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap();
            assert_eq!(top_docs.len(), num_hits);
        };

        test_query(
            "date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.001Z]",
            1,
        );
        test_query(
            "date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z}",
            1,
        );
        test_query(
            "date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z]",
            1,
        );
        test_query(
            "date:{2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z]",
            0,
        );

        let count = |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(Term::from_field_date(date_field, dt3)),
                Bound::Excluded(Term::from_field_date(date_field, dt4)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(Term::from_field_date(date_field, dt3)),
                Bound::Included(Term::from_field_date(date_field, dt4)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(Term::from_field_date(date_field, dt1)),
                Bound::Included(Term::from_field_date(date_field, dt2)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(Term::from_field_date(date_field, dt1)),
                Bound::Excluded(Term::from_field_date(date_field, dt2)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Excluded(Term::from_field_date(date_field, dt1)),
                Bound::Excluded(Term::from_field_date(date_field, dt2)),
            )),
            0
        );
    }

    fn get_json_term<T: FastValue>(field: Field, path: &str, value: T) -> Term {
        let mut term = Term::from_field_json_path(field, path, true);
        term.append_type_and_fast_value(value);
        term
    }

    #[test]
    fn mixed_numerical_test() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("id_i64", STORED | FAST);
        schema_builder.add_u64_field("id_u64", STORED | FAST);
        schema_builder.add_f64_field("id_f64", STORED | FAST);
        let schema = schema_builder.build();

        fn get_json_term<T: FastValue>(schema: &Schema, path: &str, value: T) -> Term {
            let field = schema.get_field(path).unwrap();
            Term::from_fast_value(field, &value)
            // term.append_type_and_fast_value(value);
            // term
        }
        let index = Index::create_in_ram(schema.clone());
        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();

            let doc = json!({
                "id_u64": 0,
                "id_i64": 50,
            });
            let doc = TantivyDocument::parse_json(&schema, &serde_json::to_string(&doc).unwrap())
                .unwrap();
            index_writer.add_document(doc).unwrap();
            let doc = json!({
                "id_u64": 10,
                "id_i64": 1000,
            });
            let doc = TantivyDocument::parse_json(&schema, &serde_json::to_string(&doc).unwrap())
                .unwrap();
            index_writer.add_document(doc).unwrap();

            index_writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let count = |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        // u64 on u64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(&schema, "id_u64", 10u64)),
                Bound::Included(get_json_term(&schema, "id_u64", 10u64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(&schema, "id_u64", 9u64)),
                Bound::Excluded(get_json_term(&schema, "id_u64", 10u64)),
            )),
            0
        );

        // i64 on i64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(&schema, "id_i64", 50i64)),
                Bound::Included(get_json_term(&schema, "id_i64", 1000i64)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(&schema, "id_i64", 50i64)),
                Bound::Excluded(get_json_term(&schema, "id_i64", 1000i64)),
            )),
            1
        );
    }

    #[test]
    fn json_range_mixed_val() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT | STORED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
            let doc = json!({
                "mixed_val": 10000,
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "mixed_val": 20000,
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "mixed_val": "1000a",
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "mixed_val": "2000a",
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            index_writer.commit().unwrap();
        }
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let count = |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "mixed_val", 10000u64)),
                Bound::Included(get_json_term(json_field, "mixed_val", 20000u64)),
            )),
            2
        );
        fn get_json_term_str(field: Field, path: &str, value: &str) -> Term {
            let mut term = Term::from_field_json_path(field, path, true);
            term.append_type_and_str(value);
            term
        }
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term_str(json_field, "mixed_val", "1000a")),
                Bound::Included(get_json_term_str(json_field, "mixed_val", "2000b")),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term_str(json_field, "mixed_val", "1000")),
                Bound::Included(get_json_term_str(json_field, "mixed_val", "2000a")),
            )),
            2
        );
    }

    #[test]
    fn json_range_test() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT | STORED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let u64_val = u64::MAX - 1;
        {
            let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
            let doc = json!({
                "id_u64": 0,
                "id_f64": 10.5,
                "id_i64": -100,
                "date": "2022-12-01T00:00:01Z"
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "id_u64": u64_val,
                "id_f64": 1000.5,
                "id_i64": 1000,
                "date": "2023-12-01T00:00:01Z"
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();
            let doc = json!({
                "date": "2015-02-01T00:00:00.001Z"
            });
            index_writer.add_document(doc!(json_field => doc)).unwrap();

            index_writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let count = |range_query: RangeQuery| searcher.search(&range_query, &Count).unwrap();

        // u64 on u64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_u64", u64_val)),
                Bound::Included(get_json_term(json_field, "id_u64", u64_val)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_u64", u64_val)),
                Bound::Excluded(get_json_term(json_field, "id_u64", u64_val)),
            )),
            0
        );
        // f64 on u64 field
        assert_eq!(
            count(RangeQuery::new(
                // We need to subtract since there is some inaccuracy
                Bound::Included(get_json_term(
                    json_field,
                    "id_u64",
                    (u64_val - 10000) as f64
                )),
                Bound::Included(get_json_term(json_field, "id_u64", (u64_val) as f64)),
            )),
            1
        );
        // i64 on u64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_u64", 0_i64)),
                Bound::Included(get_json_term(json_field, "id_u64", 0_i64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_u64", 1_i64)),
                Bound::Included(get_json_term(json_field, "id_u64", 1_i64)),
            )),
            0
        );
        // u64 on f64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_f64", 10_u64)),
                Bound::Included(get_json_term(json_field, "id_f64", 11_u64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_f64", 10_u64)),
                Bound::Included(get_json_term(json_field, "id_f64", 2000_u64)),
            )),
            2
        );
        // i64 on f64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_f64", 10_i64)),
                Bound::Included(get_json_term(json_field, "id_f64", 2000_i64)),
            )),
            2
        );

        // i64 on i64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000i64)),
                Bound::Included(get_json_term(json_field, "id_i64", 1000i64)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", 1000i64)),
                Bound::Excluded(get_json_term(json_field, "id_i64", 1001i64)),
            )),
            1
        );

        // u64 on i64
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", 0_u64)),
                Bound::Included(get_json_term(json_field, "id_i64", 1000u64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", 0_u64)),
                Bound::Included(get_json_term(json_field, "id_i64", 999u64)),
            )),
            0
        );
        // f64 on i64 field
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0)),
                Bound::Included(get_json_term(json_field, "id_i64", 1000.0)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0f64)),
                Bound::Excluded(get_json_term(json_field, "id_i64", 1000.0f64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0f64)),
                Bound::Included(get_json_term(json_field, "id_i64", 1000.0f64)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0f64)),
                Bound::Excluded(get_json_term(json_field, "id_i64", 1000.01f64)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "id_i64", -1000.0f64)),
                Bound::Included(get_json_term(json_field, "id_i64", 999.99f64)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Excluded(get_json_term(json_field, "id_i64", 999.9)),
                Bound::Excluded(get_json_term(json_field, "id_i64", 1000.1)),
            )),
            1
        );

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![json_field]);
        let test_query = |query, num_hits| {
            let query = query_parser.parse_query(query).unwrap();
            let top_docs = searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap();
            assert_eq!(top_docs.len(), num_hits);
        };

        test_query(
            "json.date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.001Z]",
            1,
        );
        test_query(
            "json.date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z}",
            1,
        );
        test_query(
            "json.date:[2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z]",
            1,
        );
        test_query(
            "json.date:{2015-02-01T00:00:00.001Z TO 2015-02-01T00:00:00.002Z]",
            0,
        );

        // Date field
        let dt1 =
            DateTime::from_utc(OffsetDateTime::parse("2022-12-01T00:00:01Z", &Rfc3339).unwrap());
        let dt2 =
            DateTime::from_utc(OffsetDateTime::parse("2023-12-01T00:00:01Z", &Rfc3339).unwrap());

        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "date", dt1)),
                Bound::Included(get_json_term(json_field, "date", dt2)),
            )),
            2
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Included(get_json_term(json_field, "date", dt1)),
                Bound::Excluded(get_json_term(json_field, "date", dt2)),
            )),
            1
        );
        assert_eq!(
            count(RangeQuery::new(
                Bound::Excluded(get_json_term(json_field, "date", dt1)),
                Bound::Excluded(get_json_term(json_field, "date", dt2)),
            )),
            0
        );
        // Date precision test. We don't want to truncate the precision
        let dt3 = DateTime::from_utc(
            OffsetDateTime::parse("2015-02-01T00:00:00.001Z", &Rfc3339).unwrap(),
        );
        let dt4 = DateTime::from_utc(
            OffsetDateTime::parse("2015-02-01T00:00:00.002Z", &Rfc3339).unwrap(),
        );
        let query = RangeQuery::new(
            Bound::Included(get_json_term(json_field, "date", dt3)),
            Bound::Excluded(get_json_term(json_field, "date", dt4)),
        );
        assert_eq!(count(query), 1);
    }

    #[derive(Clone, Debug)]
    pub struct Doc {
        pub id_name: String,
        pub id: u64,
    }

    fn operation_strategy() -> impl Strategy<Value = Doc> {
        prop_oneof![
            (0u64..10_000u64).prop_map(doc_from_id_1),
            (1u64..10_000u64).prop_map(doc_from_id_2),
        ]
    }

    fn doc_from_id_1(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            id_name: format!("id_name{id:010}"),
            id,
        }
    }
    fn doc_from_id_2(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            id_name: format!("id_name{:010}", id - 1),
            id,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn test_range_for_docs_prop(ops in proptest::collection::vec(operation_strategy(), 1..1000)) {
            assert!(test_id_range_for_docs(ops).is_ok());
        }
    }

    #[test]
    fn range_regression1_test() {
        let ops = vec![doc_from_id_1(0)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn range_regression1_test_json() {
        let ops = vec![doc_from_id_1(0)];
        assert!(test_id_range_for_docs_json(ops).is_ok());
    }

    #[test]
    fn test_range_regression2() {
        let ops = vec![
            doc_from_id_1(52),
            doc_from_id_1(63),
            doc_from_id_1(12),
            doc_from_id_2(91),
            doc_from_id_2(33),
        ];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_range_regression3() {
        let ops = vec![doc_from_id_1(9), doc_from_id_1(0), doc_from_id_1(13)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_range_regression_simplified() {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_u64_field("test_field", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer_for_tests().unwrap();
        writer.add_document(doc!(field=>52_000u64)).unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let range_query = FastFieldRangeWeight::new(BoundsRange::new(
            Bound::Included(Term::from_field_u64(field, 50_000)),
            Bound::Included(Term::from_field_u64(field, 50_002)),
        ));
        let scorer = range_query
            .scorer(searcher.segment_reader(0), 1.0f32)
            .unwrap();
        assert_eq!(scorer.doc(), TERMINATED);
    }

    #[test]
    fn range_regression3_test() {
        let ops = vec![doc_from_id_1(1), doc_from_id_1(2), doc_from_id_1(3)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    #[test]
    fn range_regression4_test() {
        let ops = vec![doc_from_id_2(100)];
        assert!(test_id_range_for_docs(ops).is_ok());
    }

    pub fn create_index_from_docs(docs: &[Doc], json_field: bool) -> Index {
        let mut schema_builder = Schema::builder();
        if json_field {
            let json_field = schema_builder.add_json_field("json", TEXT | STORED | FAST);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);

            {
                let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
                for doc in docs.iter() {
                    let doc = json!({
                        "ids_i64": doc.id as i64,
                        "ids_i64": doc.id as i64,
                        "ids_f64": doc.id as f64,
                        "ids_f64": doc.id as f64,
                        "ids": doc.id,
                        "ids": doc.id,
                        "id": doc.id,
                        "id_f64": doc.id as f64,
                        "id_i64": doc.id as i64,
                        "id_name": doc.id_name.to_string(),
                        "id_name_fast": doc.id_name.to_string(),
                    });
                    index_writer.add_document(doc!(json_field => doc)).unwrap();
                }

                index_writer.commit().unwrap();
            }
            index
        } else {
            let id_u64_field = schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
            let ids_u64_field = schema_builder
                .add_u64_field("ids", NumericOptions::default().set_fast().set_indexed());

            let id_f64_field = schema_builder.add_f64_field("id_f64", INDEXED | STORED | FAST);
            let ids_f64_field = schema_builder.add_f64_field(
                "ids_f64",
                NumericOptions::default().set_fast().set_indexed(),
            );

            let id_i64_field = schema_builder.add_i64_field("id_i64", INDEXED | STORED | FAST);
            let ids_i64_field = schema_builder.add_i64_field(
                "ids_i64",
                NumericOptions::default().set_fast().set_indexed(),
            );

            let text_field = schema_builder.add_text_field("id_name", STRING | STORED);
            let text_field2 = schema_builder.add_text_field("id_name_fast", STRING | STORED | FAST);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);

            {
                let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
                for doc in docs.iter() {
                    index_writer
                        .add_document(doc!(
                            ids_i64_field => doc.id as i64,
                            ids_i64_field => doc.id as i64,
                            ids_f64_field => doc.id as f64,
                            ids_f64_field => doc.id as f64,
                            ids_u64_field => doc.id,
                            ids_u64_field => doc.id,
                            id_u64_field => doc.id,
                            id_f64_field => doc.id as f64,
                            id_i64_field => doc.id as i64,
                            text_field => doc.id_name.to_string(),
                            text_field2 => doc.id_name.to_string(),
                        ))
                        .unwrap();
                }

                index_writer.commit().unwrap();
            }
            index
        }
    }

    fn test_id_range_for_docs(docs: Vec<Doc>) -> crate::Result<()> {
        test_id_range_for_docs_with_opt(docs, false)
    }
    fn test_id_range_for_docs_json(docs: Vec<Doc>) -> crate::Result<()> {
        test_id_range_for_docs_with_opt(docs, true)
    }

    fn test_id_range_for_docs_with_opt(docs: Vec<Doc>, json: bool) -> crate::Result<()> {
        let index = create_index_from_docs(&docs, json);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);

        let get_num_hits = |query| searcher.search(&query, &Count).unwrap();
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };

        let field_path = |field: &str| {
            if json {
                format!("json.{field}")
            } else {
                field.to_string()
            }
        };

        let gen_query_inclusive = |field: &str, range: RangeInclusive<u64>| {
            format!(
                "{}:[{} TO {}]",
                field_path(field),
                range.start(),
                range.end()
            )
        };
        let gen_query_exclusive = |field: &str, range: RangeInclusive<u64>| {
            format!(
                "{}:{{{} TO {}}}",
                field_path(field),
                range.start(),
                range.end()
            )
        };

        let test_sample = |sample_docs: Vec<Doc>| {
            let mut ids: Vec<u64> = sample_docs.iter().map(|doc| doc.id).collect();
            ids.sort();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ids[0]..=ids[1]).contains(&doc.id))
                .count();

            let query = gen_query_inclusive("id", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            let query = gen_query_inclusive("ids", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Text query
            {
                let test_text_query = |field_name: &str| {
                    let mut id_names: Vec<&str> =
                        sample_docs.iter().map(|doc| doc.id_name.as_str()).collect();
                    id_names.sort();
                    let expected_num_hits = docs
                        .iter()
                        .filter(|doc| (id_names[0]..=id_names[1]).contains(&doc.id_name.as_str()))
                        .count();
                    let query = format!(
                        "{}:[{} TO {}]",
                        field_path(field_name),
                        id_names[0],
                        id_names[1]
                    );
                    assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
                };

                test_text_query("id_name");
                test_text_query("id_name_fast");
            }

            // Exclusive range
            let expected_num_hits = docs
                .iter()
                .filter(|doc| {
                    (ids[0].saturating_add(1)..=ids[1].saturating_sub(1)).contains(&doc.id)
                })
                .count();

            let query = gen_query_exclusive("id", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            let query = gen_query_exclusive("ids", ids[0]..=ids[1]);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search
            let id_filter = sample_docs[0].id_name.to_string();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ids[0]..=ids[1]).contains(&doc.id) && doc.id_name == id_filter)
                .count();
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("id", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("id_f64", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("id_i64", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search on multivalue id field
            let id_filter = sample_docs[0].id_name.to_string();
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("ids", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("ids_f64", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
            let query = format!(
                "{} AND {}:{}",
                gen_query_inclusive("ids_i64", ids[0]..=ids[1]),
                field_path("id_name"),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
        };

        test_sample(vec![docs[0].clone(), docs[0].clone()]);

        let samples: Vec<_> = docs.choose_multiple(&mut rng, 3).collect();

        if samples.len() > 1 {
            test_sample(vec![samples[0].clone(), samples[1].clone()]);
            test_sample(vec![samples[1].clone(), samples[1].clone()]);
        }
        if samples.len() > 2 {
            test_sample(vec![samples[1].clone(), samples[2].clone()]);
        }

        Ok(())
    }

    #[test]
    fn test_bytes_field_ff_range_query() -> crate::Result<()> {
        use crate::schema::BytesOptions;

        let mut schema_builder = Schema::builder();
        let bytes_field = schema_builder
            .add_bytes_field("data", BytesOptions::default().set_fast().set_indexed());
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;

        // Insert documents with lexicographically sortable byte values
        // Using simple byte sequences that have clear ordering
        let values: Vec<Vec<u8>> = vec![
            vec![0x00, 0x10],
            vec![0x00, 0x20],
            vec![0x00, 0x30],
            vec![0x01, 0x00],
            vec![0x01, 0x10],
            vec![0x02, 0x00],
        ];

        for value in &values {
            let mut doc = TantivyDocument::new();
            doc.add_bytes(bytes_field, value);
            index_writer.add_document(doc)?;
        }
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Test: Range query [0x00, 0x20] to [0x01, 0x00] (inclusive)
        // Should match: [0x00, 0x20], [0x00, 0x30], [0x01, 0x00]
        let lower = Term::from_field_bytes(bytes_field, &[0x00, 0x20]);
        let upper = Term::from_field_bytes(bytes_field, &[0x01, 0x00]);
        let range_query = RangeQuery::new(Bound::Included(lower), Bound::Included(upper));
        let count = searcher.search(&range_query, &Count)?;
        assert_eq!(
            count, 3,
            "Expected 3 documents in range [0x00,0x20] to [0x01,0x00]"
        );

        // Test: Range query > [0x01, 0x00] (exclusive lower bound)
        // Should match: [0x01, 0x10], [0x02, 0x00]
        let lower = Term::from_field_bytes(bytes_field, &[0x01, 0x00]);
        let range_query = RangeQuery::new(Bound::Excluded(lower), Bound::Unbounded);
        let count = searcher.search(&range_query, &Count)?;
        assert_eq!(count, 2, "Expected 2 documents > [0x01,0x00]");

        // Test: Range query < [0x00, 0x30] (exclusive upper bound)
        // Should match: [0x00, 0x10], [0x00, 0x20]
        let upper = Term::from_field_bytes(bytes_field, &[0x00, 0x30]);
        let range_query = RangeQuery::new(Bound::Unbounded, Bound::Excluded(upper));
        let count = searcher.search(&range_query, &Count)?;
        assert_eq!(count, 2, "Expected 2 documents < [0x00,0x30]");

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod ip_range_tests {
    use proptest::prelude::ProptestConfig;
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};

    use super::*;
    use crate::collector::Count;
    use crate::query::QueryParser;
    use crate::schema::{Schema, FAST, INDEXED, STORED, STRING};
    use crate::{Index, IndexWriter};

    #[derive(Clone, Debug)]
    pub struct Doc {
        pub id: String,
        pub ip: Ipv6Addr,
    }

    fn operation_strategy() -> impl Strategy<Value = Doc> {
        prop_oneof![
            (0u64..10_000u64).prop_map(doc_from_id_1),
            (1u64..10_000u64).prop_map(doc_from_id_2),
        ]
    }

    pub fn doc_from_id_1(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            // ip != id
            id: id.to_string(),
            ip: Ipv6Addr::from_u128(id as u128),
        }
    }
    fn doc_from_id_2(id: u64) -> Doc {
        let id = id * 1000;
        Doc {
            // ip != id
            id: (id - 1).to_string(),
            ip: Ipv6Addr::from_u128(id as u128),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn test_ip_range_for_docs_prop(ops in proptest::collection::vec(operation_strategy(), 1..1000)) {
            assert!(test_ip_range_for_docs(&ops).is_ok());
        }
    }

    #[test]
    fn test_ip_range_regression1() {
        let ops = &[doc_from_id_1(0)];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_ip_range_regression2() {
        let ops = &[
            doc_from_id_1(52),
            doc_from_id_1(63),
            doc_from_id_1(12),
            doc_from_id_2(91),
            doc_from_id_2(33),
        ];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_ip_range_regression3() {
        let ops = &[doc_from_id_1(1), doc_from_id_1(2), doc_from_id_1(3)];
        assert!(test_ip_range_for_docs(ops).is_ok());
    }

    #[test]
    fn test_ip_range_regression3_simple() {
        let mut schema_builder = Schema::builder();
        let ips_field = schema_builder.add_ip_addr_field("ips", FAST | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer_for_tests().unwrap();
        let ip_addrs: Vec<Ipv6Addr> = [1000, 2000, 3000]
            .into_iter()
            .map(Ipv6Addr::from_u128)
            .collect();
        for &ip_addr in &ip_addrs {
            writer
                .add_document(doc!(ips_field=>ip_addr, ips_field=>ip_addr))
                .unwrap();
        }
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let range_weight = FastFieldRangeWeight::new(BoundsRange::new(
            Bound::Included(Term::from_field_ip_addr(ips_field, ip_addrs[1])),
            Bound::Included(Term::from_field_ip_addr(ips_field, ip_addrs[2])),
        ));

        let count =
            crate::query::weight::Weight::count(&range_weight, searcher.segment_reader(0)).unwrap();
        assert_eq!(count, 2);
    }

    pub fn create_index_from_ip_docs(docs: &[Doc]) -> Index {
        let mut schema_builder = Schema::builder();
        let ip_field = schema_builder.add_ip_addr_field("ip", STORED | FAST);
        let ips_field = schema_builder.add_ip_addr_field("ips", FAST | INDEXED);
        let text_field = schema_builder.add_text_field("id", STRING | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer = index.writer_with_num_threads(2, 60_000_000).unwrap();
            for doc in docs.iter() {
                index_writer
                    .add_document(doc!(
                        ips_field => doc.ip,
                        ips_field => doc.ip,
                        ip_field => doc.ip,
                        text_field => doc.id.to_string(),
                    ))
                    .unwrap();
            }

            index_writer.commit().unwrap();
        }
        index
    }

    fn test_ip_range_for_docs(docs: &[Doc]) -> crate::Result<()> {
        let index = create_index_from_ip_docs(docs);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let get_num_hits = |query| searcher.search(&query, &Count).unwrap();
        let query_from_text = |text: &str| {
            QueryParser::for_index(&index, vec![])
                .parse_query(text)
                .unwrap()
        };

        let gen_query_inclusive = |field: &str, ip_range: &RangeInclusive<Ipv6Addr>| {
            format!("{field}:[{} TO {}]", ip_range.start(), ip_range.end())
        };

        let test_sample = |sample_docs: &[Doc]| {
            let mut ips: Vec<Ipv6Addr> = sample_docs.iter().map(|doc| doc.ip).collect();
            ips.sort();
            let ip_range = ips[0]..=ips[1];
            let expected_num_hits = docs
                .iter()
                .filter(|doc| (ips[0]..=ips[1]).contains(&doc.ip))
                .count();

            let query = gen_query_inclusive("ip", &ip_range);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            let query = gen_query_inclusive("ips", &ip_range);
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search
            let id_filter = sample_docs[0].id.to_string();
            let expected_num_hits = docs
                .iter()
                .filter(|doc| ip_range.contains(&doc.ip) && doc.id == id_filter)
                .count();
            let query = format!(
                "{} AND id:{}",
                gen_query_inclusive("ip", &ip_range),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);

            // Intersection search on multivalue ip field
            let id_filter = sample_docs[0].id.to_string();
            let query = format!(
                "{} AND id:{}",
                gen_query_inclusive("ips", &ip_range),
                &id_filter
            );
            assert_eq!(get_num_hits(query_from_text(&query)), expected_num_hits);
        };

        test_sample(&[docs[0].clone(), docs[0].clone()]);
        if docs.len() > 1 {
            test_sample(&[docs[0].clone(), docs[1].clone()]);
            test_sample(&[docs[1].clone(), docs[1].clone()]);
        }
        if docs.len() > 2 {
            test_sample(&[docs[1].clone(), docs[2].clone()]);
        }

        Ok(())
    }
}
