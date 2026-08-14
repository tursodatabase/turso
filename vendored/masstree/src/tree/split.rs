//! Hand-over-hand split propagation.
//!
//! # Module Organization
//!
//! - [`PropagationContext`]: Unified-lifetime context for RAII guards
//! - [`Propagation`]: Core hand-over-hand propagation loop
//! - [`RootCreation`]: Root and layer-root creation helpers
//! - [`ParentLocking`]: Membership validation helpers

mod parent_locking;
mod propagation;
mod propagation_context;
mod root_creation;

pub use propagation::Propagation;
