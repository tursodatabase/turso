//! Catalog-free physical planning and emission state.
//!
//! This layer consumes document-local HIR identities. It must not resolve SQL
//! names or read schemas; all semantic facts arrive in `HirDocument`.

mod delete;
mod expression;
mod plan;
mod query;
mod runtime_bindings;

pub(crate) use delete::*;
pub(crate) use expression::*;
pub(crate) use plan::*;
pub(crate) use query::*;
pub(crate) use runtime_bindings::*;

#[cfg(test)]
mod delete_properties;

#[cfg(test)]
mod expression_properties;

#[cfg(test)]
mod query_properties;

#[cfg(test)]
mod runtime_binding_properties;
