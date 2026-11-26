//! Chooses the function or the operator of a call from the types of its
//! arguments, with the rules of PostgreSQL 18.

mod catalog;
mod coerce;

/// Why a call cannot be resolved here and is left to the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    Polymorphic,
    Domain,
    Composite,
    Array,
    /// The catalog has no `pg_type` row for a type of the call.
    MissingType,
}
