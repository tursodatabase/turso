//! Catalog-free physical planning and emission state.
//!
//! This layer consumes document-local HIR identities. It must not resolve SQL
//! names or read schemas; all semantic facts arrive in `HirDocument`.

mod delete;
mod expression;
mod index;
mod insert;
mod plan;
mod query;
mod returning;
mod row;
mod runtime_bindings;
mod update;

pub(crate) use delete::*;
pub(crate) use expression::*;
pub(crate) use index::*;
pub(crate) use insert::*;
pub(crate) use plan::*;
pub(crate) use query::*;
pub(crate) use returning::*;
pub(crate) use row::*;
pub(crate) use runtime_bindings::*;
pub(crate) use update::*;

#[cfg(test)]
mod delete_properties;

#[cfg(test)]
mod conflict_properties;

#[cfg(test)]
mod expression_properties;

#[cfg(test)]
mod insert_properties;

#[cfg(test)]
mod index_properties;

#[cfg(test)]
mod query_properties;

#[cfg(test)]
mod runtime_binding_properties;

#[cfg(test)]
mod row_properties;

#[cfg(test)]
mod returning_properties;

#[cfg(test)]
mod update_properties;
