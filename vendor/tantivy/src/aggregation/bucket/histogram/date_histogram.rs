use serde::{Deserialize, Serialize};

use super::{HistogramAggregation, HistogramBounds};
use crate::aggregation::*;

/// DateHistogramAggregation is similar to `HistogramAggregation`, but it can only be used with date
/// type.
///
/// Currently only **fixed time** intervals are supported. Calendar-aware time intervals are not
/// supported.
///
/// Like the histogram, values are rounded down into the closest bucket.
///
/// For this calculation all fastfield values are converted to f64.
///
/// # Limitations/Compatibility
/// Only fixed time intervals are supported.
///
/// # JSON Format
/// ```json
/// {
///     "prices": {
///         "date_histogram": {
///             "field": "price",
///             "fixed_interval": "30d"
///         }
///     }
/// }
/// ```
///
/// Response
/// See [`BucketEntry`](crate::aggregation::agg_result::BucketEntry)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DateHistogramAggregationReq {
    #[doc(hidden)]
    /// Only for validation
    pub interval: Option<String>,
    #[doc(hidden)]
    /// Only for validation
    pub calendar_interval: Option<String>,
    /// The field to aggregate on.
    pub field: String,
    /// The format to format dates. Unsupported currently.
    pub format: Option<String>,
    /// The interval to chunk your data range. Each bucket spans a value range of
    /// [0..fixed_interval). Accepted values
    ///
    /// Fixed intervals are configured with the `fixed_interval` parameter.
    /// In contrast to calendar-aware intervals, fixed intervals are a fixed number of SI units and
    /// never deviate, regardless of where they fall on the calendar. One second is always
    /// composed of 1000ms. This allows fixed intervals to be specified in any multiple of the
    /// supported units. However, it means fixed intervals cannot express other units such as
    /// months, since the duration of a month is not a fixed quantity. Attempting to specify a
    /// calendar interval like month or quarter will return an Error.
    ///
    /// The accepted units for fixed intervals are:
    /// * `ms`: milliseconds
    /// * `s`: seconds. Defined as 1000 milliseconds each.
    /// * `m`: minutes. Defined as 60 seconds each (60_000 milliseconds).
    /// * `h`: hours. Defined as 60 minutes each (3_600_000 milliseconds).
    /// * `d`: days. Defined as 24 hours (86_400_000 milliseconds).
    ///
    /// Fractional time values are not supported, but you can address this by shifting to another
    /// time unit (e.g., `1.5h` could instead be specified as `90m`).
    ///
    /// `Option` for validation, the parameter is not optional
    pub fixed_interval: Option<String>,
    /// Intervals implicitly defines an absolute grid of buckets `[interval * k, interval * (k +
    /// 1))`.
    ///
    /// Offset makes it possible to shift this grid into
    /// `[offset + interval * k, offset + interval * (k + 1))`. Offset has to be in the range [0,
    /// interval).
    ///
    /// The `offset` parameter is has the same syntax as the `fixed_interval` parameter, but
    /// also allows for negative values.
    pub offset: Option<String>,
    /// The minimum number of documents in a bucket to be returned. Defaults to 0.
    pub min_doc_count: Option<u64>,
    /// Limits the data range to `[min, max]` closed interval.
    ///
    /// This can be used to filter values if they are not in the data range.
    ///
    /// hard_bounds only limits the buckets, to force a range set both extended_bounds and
    /// hard_bounds to the same range.
    ///
    /// Needs to be provided as timestamp in millisecond precision.
    ///
    /// ## Example
    /// ```json
    /// {
    ///     "sales_over_time": {
    ///        "date_histogram": {
    ///            "field": "dates",
    ///            "interval": "1d",
    ///            "hard_bounds": {
    ///                "min": 0,
    ///                "max": 1420502400000
    ///            }
    ///        }
    ///    }
    /// }
    /// ```
    pub hard_bounds: Option<HistogramBounds>,
    /// Can be set to extend your bounds. The range of the buckets is by default defined by the
    /// data range of the values of the documents. As the name suggests, this can only be used to
    /// extend the value range. If the bounds for min or max are not extending the range, the value
    /// has no effect on the returned buckets.
    ///
    /// Cannot be set in conjunction with min_doc_count > 0, since the empty buckets from extended
    /// bounds would not be returned.
    pub extended_bounds: Option<HistogramBounds>,

    /// Whether to return the buckets as a hash map
    #[serde(default)]
    pub keyed: bool,
}

impl DateHistogramAggregationReq {
    pub(crate) fn to_histogram_req(&self) -> crate::Result<HistogramAggregation> {
        self.validate()?;
        Ok(HistogramAggregation {
            field: self.field.to_string(),
            interval: parse_into_milliseconds(self.fixed_interval.as_ref().unwrap())? as f64,
            offset: self
                .offset
                .as_ref()
                .map(|offset| parse_offset_into_milliseconds(offset))
                .transpose()?
                .map(|el| el as f64),
            min_doc_count: self.min_doc_count,
            hard_bounds: self.hard_bounds,
            extended_bounds: self.extended_bounds,
            keyed: self.keyed,
            is_normalized_to_ns: false,
        })
    }

    fn validate(&self) -> crate::Result<()> {
        if let Some(interval) = self.interval.as_ref() {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "`interval` parameter {interval:?} in date histogram is unsupported, only \
                 `fixed_interval` is supported"
            )));
        }
        if let Some(interval) = self.calendar_interval.as_ref() {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "`calendar_interval` parameter {interval:?} in date histogram is unsupported, \
                 only `fixed_interval` is supported"
            )));
        }
        if self.format.is_some() {
            return Err(crate::TantivyError::InvalidArgument(
                "format parameter on date_histogram is unsupported".to_string(),
            ));
        }

        if self.fixed_interval.is_none() {
            return Err(crate::TantivyError::InvalidArgument(
                "fixed_interval in date histogram is missing".to_string(),
            ));
        }

        parse_into_milliseconds(self.fixed_interval.as_ref().unwrap())?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
/// Errors when parsing the fixed interval for `DateHistogramAggregationReq`.
pub enum DateHistogramParseError {
    /// Unit not recognized in passed String
    #[error("Unit not recognized in passed String {0:?}")]
    UnitNotRecognized(String),
    /// Number not found in passed String
    #[error("Number not found in passed String {0:?}")]
    NumberMissing(String),
    /// Unit not found in passed String
    #[error("Unit not found in passed String {0:?}")]
    UnitMissing(String),
    /// Offset invalid
    #[error("passed offset is invalid {0:?}")]
    InvalidOffset(String),
    /// Value out of bounds
    #[error("passed value is out of bounds: {0:?}")]
    OutOfBounds(String),
}

fn parse_offset_into_milliseconds(input: &str) -> Result<i64, AggregationError> {
    let is_sign = |byte| &[byte] == b"-" || &[byte] == b"+";
    if input.is_empty() {
        return Err(DateHistogramParseError::InvalidOffset(input.to_string()).into());
    }

    let has_sign = is_sign(input.as_bytes()[0]);
    if has_sign {
        let (sign, input) = input.split_at(1);
        let val = parse_into_milliseconds(input)?;
        if sign == "-" {
            Ok(-val)
        } else {
            Ok(val)
        }
    } else {
        parse_into_milliseconds(input)
    }
}

pub(crate) fn parse_into_milliseconds(input: &str) -> Result<i64, AggregationError> {
    let split_boundary = input
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let (number, unit) = input.split_at(split_boundary);
    if number.is_empty() {
        return Err(DateHistogramParseError::NumberMissing(input.to_string()).into());
    }
    if unit.is_empty() {
        return Err(DateHistogramParseError::UnitMissing(input.to_string()).into());
    }
    let number: i64 = number
        .parse()
        // Technically this should never happen, but there was a bug
        // here and being defensive does not hurt.
        .map_err(|_err| DateHistogramParseError::NumberMissing(input.to_string()))?;

    let unit_in_ms = match unit {
        "ms" | "milliseconds" => 1,
        "s" | "seconds" => 1000,
        "m" | "minutes" => 60 * 1000,
        "h" | "hours" => 60 * 60 * 1000,
        "d" | "days" => 24 * 60 * 60 * 1000,
        _ => return Err(DateHistogramParseError::UnitNotRecognized(unit.to_string()).into()),
    };

    let val = number * unit_in_ms;
    // The field type is in nanoseconds precision, so validate the value to fit the range
    val.checked_mul(1_000_000)
        .ok_or_else(|| DateHistogramParseError::OutOfBounds(input.to_string()))?;

    Ok(val)
}

#[cfg(test)]
pub(crate) mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::exec_request;
    use crate::indexer::NoMergePolicy;
    use crate::schema::{Schema, FAST, STRING};
    use crate::{Index, IndexWriter, TantivyDocument};

    #[test]
    fn test_parse_into_millisecs() {
        assert_eq!(parse_into_milliseconds("1m").unwrap(), 60_000);
        assert_eq!(parse_into_milliseconds("2m").unwrap(), 120_000);
        assert_eq!(parse_into_milliseconds("2minutes").unwrap(), 120_000);
        assert_eq!(
            parse_into_milliseconds("2y").unwrap_err(),
            DateHistogramParseError::UnitNotRecognized("y".to_string()).into()
        );
        assert_eq!(
            parse_into_milliseconds("2000").unwrap_err(),
            DateHistogramParseError::UnitMissing("2000".to_string()).into()
        );
        assert_eq!(
            parse_into_milliseconds("ms").unwrap_err(),
            DateHistogramParseError::NumberMissing("ms".to_string()).into()
        );
    }

    #[test]
    fn test_parse_offset_into_milliseconds() {
        assert_eq!(parse_offset_into_milliseconds("1m").unwrap(), 60_000);
        assert_eq!(parse_offset_into_milliseconds("+1m").unwrap(), 60_000);
        assert_eq!(parse_offset_into_milliseconds("-1m").unwrap(), -60_000);
        assert_eq!(parse_offset_into_milliseconds("2m").unwrap(), 120_000);
        assert_eq!(parse_offset_into_milliseconds("+2m").unwrap(), 120_000);
        assert_eq!(parse_offset_into_milliseconds("-2m").unwrap(), -120_000);
        assert_eq!(parse_offset_into_milliseconds("-2ms").unwrap(), -2);
        assert_eq!(
            parse_offset_into_milliseconds("2y").unwrap_err(),
            DateHistogramParseError::UnitNotRecognized("y".to_string()).into()
        );
        assert_eq!(
            parse_offset_into_milliseconds("2000").unwrap_err(),
            DateHistogramParseError::UnitMissing("2000".to_string()).into()
        );
        assert_eq!(
            parse_offset_into_milliseconds("ms").unwrap_err(),
            DateHistogramParseError::NumberMissing("ms".to_string()).into()
        );
    }

    #[test]
    fn test_parse_into_milliseconds_do_not_accept_non_ascii() {
        assert!(parse_into_milliseconds("１m").is_err());
    }

    pub fn get_test_index_from_docs(
        merge_segments: bool,
        segment_and_docs: &[Vec<&str>],
    ) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_date_field("date", FAST);
        schema_builder.add_json_field("mixed", FAST);
        schema_builder.add_text_field("text", FAST | STRING);
        schema_builder.add_text_field("text2", FAST | STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        {
            let mut index_writer = index.writer_with_num_threads(1, 30_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            for values in segment_and_docs {
                for doc_str in values {
                    let doc = TantivyDocument::parse_json(&schema, doc_str)?;
                    index_writer.add_document(doc)?;
                }
                // writing the segment
                index_writer.commit()?;
            }
        }
        if merge_segments {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            if segment_ids.len() > 1 {
                let mut index_writer: IndexWriter = index.writer_for_tests()?;
                index_writer.merge(&segment_ids).wait()?;
                index_writer.wait_merging_threads()?;
            }
        }

        Ok(index)
    }

    #[test]
    fn histogram_test_date_force_merge_segments() {
        histogram_test_date_merge_segments(true)
    }

    #[test]
    fn histogram_test_date() {
        histogram_test_date_merge_segments(false)
    }

    fn histogram_test_date_merge_segments(merge_segments: bool) {
        let docs = vec![
            vec![r#"{ "date": "2015-01-01T12:10:30Z", "text": "aaa" }"#],
            vec![r#"{ "date": "2015-01-01T11:11:30Z", "text": "bbb" }"#],
            vec![r#"{ "date": "2015-01-01T11:11:30Z", "text": "bbb" }"#],
            vec![r#"{ "date": "2015-01-02T00:00:00Z", "text": "bbb" }"#],
            vec![r#"{ "date": "2015-01-06T00:00:00Z", "text": "ccc" }"#],
            vec![r#"{ "date": "2015-01-06T00:00:00Z", "text": "ccc" }"#],
        ];
        let index = get_test_index_from_docs(merge_segments, &docs).unwrap();

        {
            // 30day + offset
            let elasticsearch_compatible_json = json!(
                {
                "sales_over_time": {
                    "date_histogram": {
                    "field": "date",
                    "fixed_interval": "30d",
                    "offset": "-4d"
                    }
                }
                }
            );

            let agg_req: Aggregations = serde_json::from_str(
                &serde_json::to_string(&elasticsearch_compatible_json).unwrap(),
            )
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            let expected_res = json!({
                "sales_over_time" : {
                    "buckets" : [
                        {
                            "key_as_string" : "2015-01-01T00:00:00Z",
                            "key" : 1420070400000.0,
                            "doc_count" : 6
                        }
                    ]
                }
            });
            assert_eq!(res, expected_res);
        }

        {
            // 30day + offset + sub_agg
            let elasticsearch_compatible_json = json!(
                {
                    "sales_over_time": {
                        "date_histogram": {
                        "field": "date",
                        "fixed_interval": "30d",
                        "offset": "-4d"
                        },
                        "aggs": {
                            "texts": {
                                "terms": {"field": "text"}
                            }
                        }
                    }
                }
            );

            let agg_req: Aggregations = serde_json::from_str(
                &serde_json::to_string(&elasticsearch_compatible_json).unwrap(),
            )
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            let expected_res = json!({
                "sales_over_time" : {
                "buckets" : [
                    {
                        "key_as_string" : "2015-01-01T00:00:00Z",
                        "key" : 1420070400000.0,
                        "doc_count" : 6,
                        "texts": {
                            "buckets": [
                                {
                                "doc_count": 3,
                                "key": "bbb"
                                },
                                {
                                "doc_count": 2,
                                "key": "ccc"
                                },
                                {
                                "doc_count": 1,
                                "key": "aaa"
                                }
                            ],
                            "doc_count_error_upper_bound": 0,
                            "sum_other_doc_count": 0
                            }
                        }
                    ]
                }
            });
            assert_eq!(res, expected_res);
        }
        {
            // 1day
            let elasticsearch_compatible_json = json!(
                {
                    "sales_over_time": {
                        "date_histogram": {
                            "field": "date",
                            "fixed_interval": "1d"
                        }
                    }
                }
            );

            let agg_req: Aggregations = serde_json::from_str(
                &serde_json::to_string(&elasticsearch_compatible_json).unwrap(),
            )
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            let expected_res = json!( {
                "sales_over_time": {
                    "buckets": [
                        {
                            "doc_count": 3,
                            "key": 1420070400000.0,
                            "key_as_string": "2015-01-01T00:00:00Z"
                        },
                        {
                            "doc_count": 1,
                            "key": 1420156800000.0,
                            "key_as_string": "2015-01-02T00:00:00Z"
                        },
                        {
                            "doc_count": 0,
                            "key": 1420243200000.0,
                            "key_as_string": "2015-01-03T00:00:00Z"
                        },
                        {
                            "doc_count": 0,
                            "key": 1420329600000.0,
                            "key_as_string": "2015-01-04T00:00:00Z"
                        },
                        {
                            "doc_count": 0,
                            "key": 1420416000000.0,
                            "key_as_string": "2015-01-05T00:00:00Z"
                        },
                        {
                            "doc_count": 2,
                            "key": 1420502400000.0,
                            "key_as_string": "2015-01-06T00:00:00Z"
                        }
                    ]
                }
            });
            assert_eq!(res, expected_res);
        }

        {
            // 1day + extended_bounds
            let elasticsearch_compatible_json = json!(
                {
                    "sales_over_time": {
                        "date_histogram": {
                            "field": "date",
                            "fixed_interval": "1d",
                            "extended_bounds": {
                                "min": 1419984000000.0,
                                "max": 1420588800000.0
                            }
                        }
                    }
                }
            );

            let agg_req: Aggregations = serde_json::from_str(
                &serde_json::to_string(&elasticsearch_compatible_json).unwrap(),
            )
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            let expected_res = json!({
                "sales_over_time" : {
                    "buckets": [
                        {
                            "doc_count": 0,
                            "key": 1419984000000.0,
                            "key_as_string": "2014-12-31T00:00:00Z"
                        },
                        {
                            "doc_count": 3,
                            "key": 1420070400000.0,
                            "key_as_string": "2015-01-01T00:00:00Z"
                        },
                        {
                            "doc_count": 1,
                            "key": 1420156800000.0,
                            "key_as_string": "2015-01-02T00:00:00Z"
                        },
                        {
                            "doc_count": 0,
                            "key": 1420243200000.0,
                            "key_as_string": "2015-01-03T00:00:00Z"
                        },
                        {
                            "doc_count": 0,
                            "key": 1420329600000.0,
                            "key_as_string": "2015-01-04T00:00:00Z"
                        },
                        {
                            "doc_count": 0,
                            "key": 1420416000000.0,
                            "key_as_string": "2015-01-05T00:00:00Z"
                        },
                        {
                            "doc_count": 2,
                            "key": 1420502400000.0,
                            "key_as_string": "2015-01-06T00:00:00Z"
                        },
                        {
                            "doc_count": 0,
                            "key": 1420588800000.0,
                            "key_as_string": "2015-01-07T00:00:00Z"
                        }
                    ]
                }
            });
            assert_eq!(res, expected_res);
        }
        {
            // 1day + hard_bounds + extended_bounds
            let elasticsearch_compatible_json = json!(
                {
                    "sales_over_time": {
                        "date_histogram": {
                            "field": "date",
                            "fixed_interval": "1d",
                            "hard_bounds": {
                                "min": 1420156800000.0,
                                "max": 1420243200000.0
                            }
                        }
                    }
                }
            );

            let agg_req: Aggregations = serde_json::from_str(
                &serde_json::to_string(&elasticsearch_compatible_json).unwrap(),
            )
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            let expected_res = json!({
                "sales_over_time" : {
                    "buckets": [
                        {
                            "doc_count": 1,
                            "key": 1420156800000.0,
                            "key_as_string": "2015-01-02T00:00:00Z"
                        }
                    ]
                }
            });
            assert_eq!(res, expected_res);
        }

        {
            // 1day + hard_bounds as Rfc3339
            let elasticsearch_compatible_json = json!(
                {
                    "sales_over_time": {
                        "date_histogram": {
                            "field": "date",
                            "fixed_interval": "1d",
                            "hard_bounds": {
                                "min": "2015-01-02T00:00:00Z",
                                "max": "2015-01-02T12:00:00Z"
                            }
                        }
                    }
                }
            );

            let agg_req: Aggregations = serde_json::from_str(
                &serde_json::to_string(&elasticsearch_compatible_json).unwrap(),
            )
            .unwrap();
            let res = exec_request(agg_req, &index).unwrap();
            let expected_res = json!({
                "sales_over_time" : {
                    "buckets": [
                        {
                            "doc_count": 1,
                            "key": 1420156800000.0,
                            "key_as_string": "2015-01-02T00:00:00Z"
                        }
                    ]
                }
            });
            assert_eq!(res, expected_res);
        }
    }
    #[test]
    fn histogram_test_invalid_req() {
        let docs = vec![];

        let index = get_test_index_from_docs(false, &docs).unwrap();
        let elasticsearch_compatible_json = json!(
            {
              "sales_over_time": {
                "date_histogram": {
                  "field": "date",
                  "interval": "30d",
                  "offset": "-4d"
                }
              }
            }
        );

        let agg_req: Aggregations =
            serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
                .unwrap();
        let err = exec_request(agg_req, &index).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"An invalid argument was passed: '`interval` parameter "30d" in date histogram is unsupported, only `fixed_interval` is supported'"#
        );
    }
}
