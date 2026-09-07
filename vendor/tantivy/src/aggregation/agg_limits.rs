use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use common::ByteCount;

use super::collector::DEFAULT_MEMORY_LIMIT;
use super::{AggregationError, DEFAULT_BUCKET_LIMIT};

/// An estimate for memory consumption. Non recursive
pub trait MemoryConsumption {
    fn memory_consumption(&self) -> usize;
}

impl<K, V, S> MemoryConsumption for HashMap<K, V, S> {
    fn memory_consumption(&self) -> usize {
        let capacity = self.capacity();
        (std::mem::size_of::<K>() + std::mem::size_of::<V>() + 1) * capacity
    }
}

/// Aggregation memory limit after which the request fails. Defaults to DEFAULT_MEMORY_LIMIT
/// (500MB). The limit is shared by all SegmentCollectors
///
/// The memory limit is also a guard, which tracks how much it allocated and releases it's memory
/// on the shared counter. Cloning will create a new guard.
pub struct AggregationLimitsGuard {
    /// The counter which is shared between the aggregations for one request.
    memory_consumption: Arc<AtomicU64>,
    /// The memory_limit in bytes
    memory_limit: ByteCount,
    /// The maximum number of buckets _returned_
    /// This is not counting intermediate buckets.
    bucket_limit: u32,
    /// Allocated memory with this guard.
    allocated_with_the_guard: u64,
}

impl Clone for AggregationLimitsGuard {
    fn clone(&self) -> Self {
        Self {
            memory_consumption: Arc::clone(&self.memory_consumption),
            memory_limit: self.memory_limit,
            bucket_limit: self.bucket_limit,
            allocated_with_the_guard: 0,
        }
    }
}

impl Drop for AggregationLimitsGuard {
    /// Removes the memory consumed tracked by this _instance_ of AggregationLimits.
    /// This is used to clear the segment specific memory consumption all at once.
    fn drop(&mut self) {
        self.memory_consumption
            .fetch_sub(self.allocated_with_the_guard, Ordering::Relaxed);
    }
}

impl Default for AggregationLimitsGuard {
    fn default() -> Self {
        Self {
            memory_consumption: Default::default(),
            memory_limit: DEFAULT_MEMORY_LIMIT.into(),
            bucket_limit: DEFAULT_BUCKET_LIMIT,
            allocated_with_the_guard: 0,
        }
    }
}

impl AggregationLimitsGuard {
    /// *memory_limit*
    /// memory_limit is defined in bytes.
    /// Aggregation fails when the estimated memory consumption of the aggregation is higher than
    /// memory_limit.
    /// memory_limit will default to `DEFAULT_MEMORY_LIMIT` (500MB)
    ///
    /// *bucket_limit*
    /// Limits the maximum number of buckets returned from an aggregation request.
    /// bucket_limit will default to `DEFAULT_BUCKET_LIMIT` (65000)
    ///
    /// Note: The returned instance contains a Arc shared counter to track memory consumption.
    pub fn new(memory_limit: Option<u64>, bucket_limit: Option<u32>) -> Self {
        Self {
            memory_consumption: Default::default(),
            memory_limit: memory_limit.unwrap_or(DEFAULT_MEMORY_LIMIT).into(),
            bucket_limit: bucket_limit.unwrap_or(DEFAULT_BUCKET_LIMIT),
            allocated_with_the_guard: 0,
        }
    }

    pub(crate) fn add_memory_consumed(&mut self, add_num_bytes: u64) -> crate::Result<()> {
        let prev_value = self
            .memory_consumption
            .fetch_add(add_num_bytes, Ordering::Relaxed);
        self.allocated_with_the_guard += add_num_bytes;
        validate_memory_consumption(prev_value + add_num_bytes, self.memory_limit)?;
        Ok(())
    }

    pub(crate) fn get_bucket_limit(&self) -> u32 {
        self.bucket_limit
    }
}

fn validate_memory_consumption(
    memory_consumption: u64,
    memory_limit: ByteCount,
) -> Result<(), AggregationError> {
    // Load the estimated memory consumed by the aggregations
    let memory_consumed: ByteCount = memory_consumption.into();
    if memory_consumed > memory_limit {
        return Err(AggregationError::MemoryExceeded {
            limit: memory_limit,
            current: memory_consumed,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::aggregation::tests::exec_request_with_query;

    // https://github.com/quickwit-oss/quickwit/issues/3837
    #[test]
    fn test_agg_limits_with_empty_merge() {
        use crate::aggregation::agg_req::Aggregations;
        use crate::aggregation::bucket::tests::get_test_index_from_docs;

        let docs = vec![
            vec![r#"{ "date": "2015-01-02T00:00:00Z", "text": "bbb", "text2": "bbb" }"#],
            vec![r#"{ "text": "aaa", "text2": "bbb" }"#],
        ];
        let index = get_test_index_from_docs(false, &docs).unwrap();

        {
            let elasticsearch_compatible_json = json!(
                {
                    "1": {
                        "terms": {"field": "text2", "min_doc_count": 0},
                        "aggs": {
                            "2":{
                                "date_histogram": {
                                    "field": "date",
                                    "fixed_interval": "1d",
                                    "extended_bounds": {
                                        "min": "2015-01-01T00:00:00Z",
                                        "max": "2015-01-10T00:00:00Z"
                                    }
                                }
                            }
                        }
                    }
                }
            );

            let agg_req: Aggregations = serde_json::from_str(
                &serde_json::to_string(&elasticsearch_compatible_json).unwrap(),
            )
            .unwrap();
            let res = exec_request_with_query(agg_req, &index, Some(("text", "bbb"))).unwrap();
            let expected_res = json!({
             "1": {
                "buckets": [
                  {
                    "2": {
                      "buckets": [
                        { "doc_count": 0, "key": 1420070400000.0, "key_as_string": "2015-01-01T00:00:00Z" },
                        { "doc_count": 1, "key": 1420156800000.0, "key_as_string": "2015-01-02T00:00:00Z" },
                        { "doc_count": 0, "key": 1420243200000.0, "key_as_string": "2015-01-03T00:00:00Z" },
                        { "doc_count": 0, "key": 1420329600000.0, "key_as_string": "2015-01-04T00:00:00Z" },
                        { "doc_count": 0, "key": 1420416000000.0, "key_as_string": "2015-01-05T00:00:00Z" },
                        { "doc_count": 0, "key": 1420502400000.0, "key_as_string": "2015-01-06T00:00:00Z" },
                        { "doc_count": 0, "key": 1420588800000.0, "key_as_string": "2015-01-07T00:00:00Z" },
                        { "doc_count": 0, "key": 1420675200000.0, "key_as_string": "2015-01-08T00:00:00Z" },
                        { "doc_count": 0, "key": 1420761600000.0, "key_as_string": "2015-01-09T00:00:00Z" },
                        { "doc_count": 0, "key": 1420848000000.0, "key_as_string": "2015-01-10T00:00:00Z" }
                      ]
                    },
                    "doc_count": 1,
                    "key": "bbb"
                  }
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
              }
            });
            assert_eq!(res, expected_res);
        }
    }

    // https://github.com/quickwit-oss/quickwit/issues/3837
    #[test]
    fn test_agg_limits_with_empty_data() {
        use crate::aggregation::agg_req::Aggregations;
        use crate::aggregation::bucket::tests::get_test_index_from_docs;

        let docs = vec![vec![r#"{ "text": "aaa", "text2": "bbb" }"#]];
        let index = get_test_index_from_docs(false, &docs).unwrap();

        {
            // Empty result since there is no doc with dates
            let elasticsearch_compatible_json = json!(
                {
                    "1": {
                        "terms": {"field": "text2", "min_doc_count": 0},
                        "aggs": {
                            "2":{
                                "date_histogram": {
                                    "field": "date",
                                    "fixed_interval": "1d",
                                    "extended_bounds": {
                                        "min": "2015-01-01T00:00:00Z",
                                        "max": "2015-01-10T00:00:00Z"
                                    }
                                }
                            }
                        }
                    }
                }
            );

            let agg_req: Aggregations = serde_json::from_str(
                &serde_json::to_string(&elasticsearch_compatible_json).unwrap(),
            )
            .unwrap();
            let res = exec_request_with_query(agg_req, &index, Some(("text", "bbb"))).unwrap();
            let expected_res = json!({
             "1": {
                "buckets": [
                  {
                    "2": {
                      "buckets": [
                        { "doc_count": 0, "key": 1420070400000.0, "key_as_string": "2015-01-01T00:00:00Z" },
                        { "doc_count": 0, "key": 1420156800000.0, "key_as_string": "2015-01-02T00:00:00Z" },
                        { "doc_count": 0, "key": 1420243200000.0, "key_as_string": "2015-01-03T00:00:00Z" },
                        { "doc_count": 0, "key": 1420329600000.0, "key_as_string": "2015-01-04T00:00:00Z" },
                        { "doc_count": 0, "key": 1420416000000.0, "key_as_string": "2015-01-05T00:00:00Z" },
                        { "doc_count": 0, "key": 1420502400000.0, "key_as_string": "2015-01-06T00:00:00Z" },
                        { "doc_count": 0, "key": 1420588800000.0, "key_as_string": "2015-01-07T00:00:00Z" },
                        { "doc_count": 0, "key": 1420675200000.0, "key_as_string": "2015-01-08T00:00:00Z" },
                        { "doc_count": 0, "key": 1420761600000.0, "key_as_string": "2015-01-09T00:00:00Z" },
                        { "doc_count": 0, "key": 1420848000000.0, "key_as_string": "2015-01-10T00:00:00Z" }
                      ]
                    },
                    "doc_count": 0,
                    "key": "bbb"
                  }
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
              }
            });
            assert_eq!(res, expected_res);
        }
    }
}
