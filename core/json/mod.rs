//! Facade over `turso_core_json`, plus the JSON virtual tables that need
//! this crate's vtab machinery.

pub use turso_core_json::*;

pub(crate) mod vtab;
