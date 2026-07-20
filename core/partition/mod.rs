//! Time-based table partitioning support.
//!
//! This module provides automatic partitioning of tables by a timestamp column.
//! Data is automatically routed to separate files based on the partition interval
//! (typically daily), while appearing as a single table to queries.
//!
//! # Features
//!
//! - Transparent INSERT routing to the correct partition file
//! - SELECT queries automatically span all attached partitions
//! - Configurable partition paths via the `PartitionPathResolver` trait
//! - Support for partition pruning based on timestamp ranges
//! - External API for partition management (list, attach, detach)
//!
//! # Example
//!
//! ```ignore
//! use turso_core::partition::{PartitionManager, PartitionConfig};
//! use turso_core::partition::path_resolver::DefaultPathResolver;
//! use std::path::PathBuf;
//!
//! // Create a partition manager
//! let mut manager = PartitionManager::new();
//!
//! // Configure a partitioned table
//! let resolver = Box::new(DefaultPathResolver::daily(PathBuf::from("/data")));
//! let config = PartitionConfig::new(
//!     resolver,
//!     "CREATE TABLE events (id INTEGER, ts INTEGER NOT NULL, data TEXT)".to_string(),
//!     "ts".to_string(),
//! );
//!
//! manager.register_table("events", config).unwrap();
//!
//! // Ensure a partition exists for a timestamp
//! let partition = manager.ensure_partition("events", 1737547200_000_000).unwrap();
//!
//! // List all partitions
//! let partitions = manager.list_partitions("events").unwrap();
//! ```
//!
//! # Limitations
//!
//! - Time partitioning is available only in native builds, not WebAssembly
//! - Cross-partition writes in a single transaction are not supported
//! - Global unique indexes across partitions are not supported
//! - TTL/rotation must be handled externally
//! - Recorder tables are append-oriented; row-level UPDATE and DELETE are not supported
//! - Catalog reconciliation and first-file creation currently use synchronous filesystem I/O

pub mod error;
pub mod file;
pub mod manager;
pub mod path_resolver;
#[cfg(feature = "test_helper")]
pub(crate) mod test_hooks;

pub use error::PartitionError;
#[cfg(feature = "fs")]
pub(crate) use file::{remove_partition_sidecars, sync_parent_directory, with_partition_path_lock};
pub use file::{PartitionFile, PartitionInfo};
pub use manager::{PartitionConfig, PartitionManager};
pub use path_resolver::{DefaultPathResolver, PartitionPathResolver, VideoAnalyticsPathResolver};

/// Source of a partition key in a prepared INSERT statement.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
#[derive(Clone, Debug)]
pub(crate) enum PartitionInsertValue {
    Literal(i64),
    Parameter(std::num::NonZeroUsize),
}

/// Routing contract captured while compiling a partitioned INSERT.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
#[derive(Clone, Debug)]
pub(crate) struct PartitionedInsert {
    pub table_name: String,
    pub values: Vec<PartitionInsertValue>,
    pub compiled_range_start: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionWriteTarget {
    pub table_name: String,
    pub db_alias: String,
    pub range_start: i64,
}
