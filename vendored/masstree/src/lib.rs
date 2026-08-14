//! # [`MassTree`]
//!
//! A high-performance concurrent ordered map for Rust. Stores keys as `&[u8]` and
//! supports variable-length keys by building a trie of B+trees.
//!
//! ## Features
//!
//! - Ordered map for byte keys (lexicographic ordering)
//! - Lock-free reads with version validation
//! - Concurrent inserts and deletes with fine-grained leaf locking
//! - Range scans with `scan` and `scan_prefix`
//! - Memory reclamation via epoch-based deferred cleanup
//! - Lazy leaf coalescing for deleted entries
//! - High-performance inline variant [`MassTree15Inline`] (the default `MassTree` alias)
//!
//! ## Quick Start
//!
//! ```rust
//! use masstree::{MassTree, RangeBound};
//!
//! let tree: MassTree<u64> = MassTree::new();
//! let guard = tree.guard();
//!
//! // Insert
//! tree.insert_with_guard(b"hello", 123, &guard);
//!
//! // Point lookup (lock-free, returns copy for inline storage)
//! assert_eq!(tree.get_with_guard(b"hello", &guard), Some(123));
//!
//! // Remove
//! tree.remove_with_guard(b"hello", &guard).unwrap();
//!
//! // Range scan
//! tree.scan(RangeBound::Included(b"a"), RangeBound::Excluded(b"z"), |_key, _value| {
//!     true // continue scanning
//! }, &guard);
//! ```
//!
//! ## Thread Safety
//!
//! `MassTree<V>` is `Send + Sync` when `V: Send + Sync`. Concurrent access
//! requires using the guard-based API. The guard ties operations to an epoch
//! for safe memory reclamation.
//!
//! ```rust
//! use masstree::MassTree;
//!
//! let tree: MassTree<u64> = MassTree::new();
//! let guard = tree.guard();
//!
//! // Concurrent get (lock-free)
//! let value = tree.get_with_guard(b"key", &guard);
//!
//! // Concurrent insert (fine-grained locking)
//! let old = tree.insert_with_guard(b"key", 42, &guard);
//! ```
//!
//! ## Key Constraints
//!
//! - Keys must be 0-256 bytes. Longer keys will panic.
//! - Keys are byte slices (`&[u8]`), not generic types.

#![deny(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::inline_always)]
#![allow(clippy::multiple_crate_versions)]
#![allow(clippy::cast_ptr_alignment)]
#![allow(clippy::option_if_let_else)]

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Initialize tracing with file and console output.
///
/// Writes NDJSON logs to `logs/masstree.jsonl` for structured analysis.
/// Also outputs to console unless `MASSTREE_LOG_CONSOLE=0` is set.
///
/// Safe to call multiple times - only the first call takes effect.
///
/// # Environment Variables
///
/// - `RUST_LOG`: Filter directives (default: `masstree=info`). Example: `masstree=debug,masstree::tree::locked=trace`
/// - `MASSTREE_LOG_DIR`: Log directory (default: `logs/`)
/// - `MASSTREE_LOG_CONSOLE`: Set to `0` to disable console output
///
/// # Example
///
/// ```bash
/// # Run with debug logging to file, no console spam
/// RUST_LOG=masstree=debug MASSTREE_LOG_CONSOLE=0 cargo run --features tracing
///
/// # Analyze logs with jq
/// cat logs/masstree.jsonl | jq 'select(.fields.ikey != null)'
/// ```
#[cfg(feature = "tracing")]
pub fn init_tracing() {
    use std::env as StdEnv;
    use std::fs as StdFs;
    use std::sync::OnceLock;
    use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

    static GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

    GUARD.get_or_init(|| {
        let log_dir: String =
            StdEnv::var("MASSTREE_LOG_DIR").unwrap_or_else(|_| "logs".to_string());
        let console_enabled: bool = false;
        let filter_str: String =
            StdEnv::var("RUST_LOG").unwrap_or_else(|_| "masstree=info".to_string());

        let _ = StdFs::create_dir_all(&log_dir);

        let file_appender = tracing_appender::rolling::never(&log_dir, "masstree.jsonl");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(non_blocking)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
            .json()
            .with_filter(
                EnvFilter::try_new(&filter_str).unwrap_or_else(|_| EnvFilter::new("info")),
            );

        let console_layer = if console_enabled {
            Some(
                tracing_subscriber::fmt::layer()
                    .with_thread_ids(true)
                    .compact()
                    .with_filter(
                        EnvFilter::try_new(&filter_str).unwrap_or_else(|_| EnvFilter::new("trace")),
                    ),
            )
        } else {
            None
        };

        let _ = tracing_subscriber::registry()
            .with(file_layer)
            .with(console_layer)
            .try_init();

        guard
    });
}

/// No-op when the `tracing` feature is disabled.
#[cfg(not(feature = "tracing"))]
pub const fn init_tracing() {}

// All modules are crate-internal. Public types are re-exported below.
pub(crate) mod alloc15;
pub(crate) mod alloc_trait;
pub(crate) mod hints;
pub(crate) mod inline;
pub(crate) mod internode;
pub(crate) mod key;
pub(crate) mod ksearch;
pub(crate) mod leaf15;
pub(crate) mod leaf_trait;
pub(crate) mod link;
pub(crate) mod node_pool;
pub(crate) mod nodeversion;
pub(crate) mod ordering;
pub(crate) mod permuter;
pub(crate) mod policy;
pub(crate) mod prefetch;
mod retirement;
mod shard_counter;
pub(crate) mod suffix;
pub(crate) mod tree;

// ============================================================================
//  Public API Re-exports
//
//  All public types are re-exported here. Internal modules are pub(crate).
// ============================================================================

// Core tree types
pub use tree::{BatchEntry, BatchInsertResult, Entry, OccupiedEntry, VacantEntry, batch};
pub use tree::{InsertError, RemoveError};
pub use tree::{KeysIter, RangeBound, RangeIter, ScanEntry, ValuesIter};
pub use tree::{MassTree, MassTree15, MassTree15Inline, MassTreeGeneric};

// Value storage policies
pub use inline::bits::InlineBits;
pub use policy::RefPolicy as RefLeafPolicy;
pub use policy::{BoxPolicy, LeafPolicy, ValuePtr, ValueRef};

// Key types and constants
pub use key::{IKEY_SIZE, Key, MAX_KEY_LENGTH};

// Node types (for generic tree programming)
pub use alloc_trait::TreeAllocator;
pub use alloc15::SeizeAllocator;
pub use internode::InternodeNode;
pub use leaf_trait::{TreeInternode, TreeLeafNode, TreePermutation};
pub use leaf15::LeafNode15;
pub use nodeversion::NodeVersion;
pub use permuter::{AtomicPermuter, AtomicPermuter15, MAX_WIDTH, Permuter, Permuter15};
// InlineSuffixBag is intentionally not re-exported: it relies on the leaf's
// lock/OCC protocol and is not meant to be used as a standalone concurrent type.
pub use suffix::{PermutationProvider, SuffixBag};

// Memory reclamation
pub use retirement::BatchedRetire;

// Internal re-exports (crate-only)
pub(crate) use link::Linker;
