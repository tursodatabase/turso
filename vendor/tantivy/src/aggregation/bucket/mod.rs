//! Module for all bucket aggregations.
//!
//! BucketAggregations create buckets of documents.
//! Each bucket is associated with a rule which
//! determines whether or not a document in the falls into it. In other words, the buckets
//! effectively define document sets. Buckets are not necessarily disjunct, therefore a document can
//! fall into multiple buckets. In addition to the buckets themselves, the bucket aggregations also
//! compute and return the number of documents for each bucket. Bucket aggregations, as opposed to
//! metric aggregations, can hold sub-aggregations. These sub-aggregations will be aggregated for
//! the buckets created by their "parent" bucket aggregation. There are different bucket
//! aggregators, each with a different "bucketing" strategy. Some define a single bucket, some
//! define fixed number of multiple buckets, and others dynamically create the buckets during the
//! aggregation process.
//!
//! Results of final buckets are [`BucketResult`](super::agg_result::BucketResult).
//! Results of intermediate buckets are
//! [`IntermediateBucketResult`](super::intermediate_agg_result::IntermediateBucketResult)
//!
//! ## Supported Bucket Aggregations
//! - [Histogram](HistogramAggregation)
//! - [DateHistogram](DateHistogramAggregationReq)
//! - [Range](RangeAggregation)
//! - [Terms](TermsAggregation)

mod composite;
mod filter;
mod histogram;
mod range;
mod term_agg;
mod term_missing_agg;

use std::collections::HashMap;
use std::fmt;

pub use composite::*;
pub use filter::*;
pub use histogram::*;
pub use range::*;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
pub use term_agg::*;
pub use term_missing_agg::*;

/// Order for buckets in a bucket aggregation.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Default)]
pub enum Order {
    /// Asc order
    #[serde(rename = "asc")]
    Asc,
    /// Desc order
    #[serde(rename = "desc")]
    #[default]
    Desc,
}

#[derive(Clone, Debug, PartialEq)]
/// Order property by which to apply the order
#[derive(Default)]
pub enum OrderTarget {
    /// The key of the bucket
    Key,
    /// The doc count of the bucket
    #[default]
    Count,
    /// Order by value of the sub aggregation metric with identified by given `String`.
    ///
    /// Only single value metrics are supported currently
    SubAggregation(String),
}

impl From<&str> for OrderTarget {
    fn from(val: &str) -> Self {
        match val {
            "_key" => OrderTarget::Key,
            "_count" => OrderTarget::Count,
            _ => OrderTarget::SubAggregation(val.to_string()),
        }
    }
}

impl fmt::Display for OrderTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OrderTarget::Key => f.write_str("_key"),
            OrderTarget::Count => f.write_str("_count"),
            OrderTarget::SubAggregation(agg) => agg.fmt(f),
        }
    }
}

/// Set the order. target is either "_count", "_key", or the name of
/// a metric sub_aggregation.
///
/// De/Serializes to elasticsearch compatible JSON.
///
/// Examples in JSON format:
/// { "_count": "asc" }
/// { "_key": "asc" }
/// { "average_price": "asc" }
#[derive(Clone, Default, Debug, PartialEq)]
pub struct CustomOrder {
    /// The target property by which to sort by
    pub target: OrderTarget,
    /// The order asc or desc
    pub order: Order,
}

impl Serialize for CustomOrder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let map: HashMap<String, Order> =
            std::iter::once((self.target.to_string(), self.order)).collect();
        map.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CustomOrder {
    fn deserialize<D>(deserializer: D) -> Result<CustomOrder, D::Error>
    where D: Deserializer<'de> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let return_err = |message, val: serde_json::Value| {
            de::Error::custom(format!(
                "{}, but got {}",
                message,
                serde_json::to_string(&val).unwrap()
            ))
        };

        match value {
            serde_json::Value::Object(map) => {
                if map.len() != 1 {
                    return Err(return_err(
                        "expected exactly one key-value pair in the order map",
                        map.into(),
                    ));
                }

                let (key, value) = map.into_iter().next().unwrap();
                let order = serde_json::from_value(value).map_err(de::Error::custom)?;

                Ok(CustomOrder {
                    target: key.as_str().into(),
                    order,
                })
            }
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    return Err(return_err("unexpected empty array in order", arr.into()));
                }
                if arr.len() != 1 {
                    return Err(return_err(
                        "only one sort order supported currently",
                        arr.into(),
                    ));
                }
                let entry = arr.into_iter().next().unwrap();
                let map = entry
                    .as_object()
                    .ok_or_else(|| return_err("expected object as sort order", entry.clone()))?;
                let (key, value) = map.into_iter().next().ok_or_else(|| {
                    return_err(
                        "expected exactly one key-value pair in the order map",
                        entry.clone(),
                    )
                })?;
                let order = serde_json::from_value(value.clone()).map_err(de::Error::custom)?;

                Ok(CustomOrder {
                    target: key.as_str().into(),
                    order,
                })
            }
            _ => Err(return_err(
                "unexpected type, expected an object or array",
                value,
            )),
        }
    }
}

#[test]
fn custom_order_serde_test() {
    let order = CustomOrder {
        target: OrderTarget::Key,
        order: Order::Desc,
    };

    let order_str = serde_json::to_string(&order).unwrap();
    assert_eq!(order_str, "{\"_key\":\"desc\"}");
    let order_deser = serde_json::from_str(&order_str).unwrap();

    assert_eq!(order, order_deser);
    let order_deser: CustomOrder = serde_json::from_str("[{\"_key\":\"desc\"}]").unwrap();
    assert_eq!(order, order_deser);

    let order_deser: serde_json::Result<CustomOrder> = serde_json::from_str("{}");
    assert!(order_deser.is_err());

    let order_deser: serde_json::Result<CustomOrder> = serde_json::from_str("[]");
    assert!(order_deser
        .unwrap_err()
        .to_string()
        .contains("unexpected empty array in order"));

    let order_deser: serde_json::Result<CustomOrder> =
        serde_json::from_str(r#"[{"_key":"desc"},{"_key":"desc"}]"#);
    assert_eq!(
        order_deser.unwrap_err().to_string(),
        r#"only one sort order supported currently, but got [{"_key":"desc"},{"_key":"desc"}]"#
    );
}
