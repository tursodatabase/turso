//! Catalog-free physical planning and emission state.
//!
//! This layer consumes document-local HIR identities. It must not resolve SQL
//! names or read schemas; all semantic facts arrive in `HirDocument`.

mod cdc;
mod delete;
mod expression;
mod foreign_keys;
mod index;
mod insert;
mod mutation;
mod plan;
mod query;
mod returning;
mod row;
mod runtime_bindings;
mod schema_expression;
mod trigger;
mod update;

pub(crate) use cdc::*;
pub(crate) use delete::*;
pub(crate) use expression::*;
pub(crate) use foreign_keys::*;
pub(crate) use index::*;
pub(crate) use insert::*;
pub(crate) use mutation::*;
pub(crate) use plan::*;
pub(crate) use query::*;
pub(crate) use returning::*;
pub(crate) use row::*;
pub(crate) use runtime_bindings::*;
pub(crate) use schema_expression::*;
pub(crate) use trigger::*;
pub(crate) use update::*;

#[cfg(test)]
mod delete_properties;

#[cfg(test)]
mod conflict_properties;

#[cfg(test)]
mod cdc_properties;

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
mod schema_expression_properties;

#[cfg(test)]
mod row_properties;

#[cfg(test)]
mod returning_properties;

#[cfg(test)]
mod trigger_properties;

#[cfg(test)]
mod update_properties;
