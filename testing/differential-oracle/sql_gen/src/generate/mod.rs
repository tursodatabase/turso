//! SQL generation modules.
//!
//! # Design Principles
//!
//! **Always pass configs, never have functions without configs.**
//!
//! All generator functions must receive their configuration through explicit parameters,
//! typically via `&SqlGen<C>` which provides access to `generator.policy()`. This ensures:
//!
//! - All generation behavior is controllable via the Policy
//! - No hardcoded magic numbers or probabilities in generator code
//! - Tests can verify behavior by configuring specific policies
//! - Users have full control over generation characteristics
//!
//! When adding new generator functions:
//! 1. Add any new configurable parameters to the appropriate config struct in `policy.rs`
//! 2. Access configs via `generator.policy().<config_name>`
//! 3. Never use hardcoded values for probabilities, ranges, or weights

pub mod expr;
pub mod literal;
pub mod select;
pub mod stmt;
