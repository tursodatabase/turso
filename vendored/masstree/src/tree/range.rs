//! Filepath: src/tree/range/mod.rs
//!
//! Range scan module for [`crate::MassTree`].
//!
//! This module implements ordered range iteration over the tree.
//!
//! # Module Organization
//!
//! - `cursor_key`: Mutable key buffer for scan position tracking
//! - `scan_state`: State machine types ([`ScanState`], [`ScanStackElement`], etc.)
//! - `helper`: Forward scan helper and lower bound search
//! - `find`: Core algorithm (`find_initial`, `find_next`, `find_retry`)
//! - `iterator`: [`RangeIter`] and [`ScanEntry`] types
//! - `api`: Public API methods on [`crate::MassTreeGeneric`]
//!
//! # Public API
//!
//! The main public types are:
//! - [`RangeBound`]: Start/end bound specification
//! - [`ScanEntry`]: Key-value entry returned by iterator
//! - [`RangeIter`]: Iterator over a key range
//!
//! # Example
//!
//! ```ignore
//! use masstree::{MassTree, RangeBound};
//!
//! let tree = MassTree::<String>::new();
//! // ... insert entries ...
//!
//! let guard = tree.guard();
//! for entry in tree.range(
//!     RangeBound::Included(b"start"),
//!     RangeBound::Excluded(b"end"),
//!     &guard
//! ) {
//!     println!("{:?} -> {}", entry.key, entry.value);
//! }
//!

mod api;
mod api_ref;
mod batch_common;
mod cursor_key;
mod find;
mod find_rev;
mod forward_ctx;
mod helper;
mod iterator;
mod reverse_ctx;
mod scan_state;
mod traversal;

// Re-export public types
pub use iterator::{KeysIter, RangeBound, RangeIter, ScanEntry, ValuesIter};
