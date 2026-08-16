//! SQL value types for the Turso database library: `Value`, `ValueRef`,
//! `Text`, blobs, aggregate accumulator state, and numeric conversions.

pub mod numeric;
mod value;

pub use value::*;

// Module aliases so the files moved out of `turso_core` keep their
// `crate::alloc::...`-style paths.
use turso_core_common::alloc;
use turso_core_common::{LimboError, Result};
