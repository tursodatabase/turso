use common::ByteCount;

use super::bucket::DateHistogramParseError;

/// Error that may occur when opening a directory
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AggregationError {
    /// InternalError Aggregation Request
    #[error("InternalError: {0:?}")]
    InternalError(String),
    /// Invalid Aggregation Request
    #[error("InvalidRequest: {0:?}")]
    InvalidRequest(String),
    /// Date histogram parse error
    #[error("Date histogram parse error: {0:?}")]
    DateHistogramParseError(#[from] DateHistogramParseError),
    /// Memory limit exceeded
    #[error(
        "Aborting aggregation because memory limit was exceeded. Limit: {limit:?}, Current: \
         {current:?}"
    )]
    MemoryExceeded {
        /// Memory consumption limit
        limit: ByteCount,
        /// Current memory consumption
        current: ByteCount,
    },
    /// Bucket limit exceeded
    #[error(
        "Aborting aggregation because bucket limit was exceeded. Limit: {limit:?}, Current: \
         {current:?}"
    )]
    BucketLimitExceeded {
        /// Bucket limit
        limit: u32,
        /// Current num buckets
        current: u32,
    },
}
