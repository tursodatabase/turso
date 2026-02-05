//! Type-state marker traits for compile-time capability restrictions.
//!
//! The capability system uses marker traits to encode what SQL constructs
//! are allowed at compile time. This provides safety guarantees that certain
//! operations cannot be generated when they're not allowed.
//!
//! # Example
//!
//! ```
//! use sql_gen::{SqlGen, SelectOnly, DmlOnly, Full};
//!
//! // With Full capabilities, all statement types are available
//! // let gen: SqlGen<Full> = ...;
//!
//! // With SelectOnly, only SELECT statements can be generated
//! // let gen: SqlGen<SelectOnly> = ...;
//!
//! // With DmlOnly, only DML statements (SELECT, INSERT, UPDATE, DELETE)
//! // let gen: SqlGen<DmlOnly> = ...;
//! ```

/// Marker trait for statement-level capabilities.
///
/// This trait defines which SQL constructs are allowed for a given capability set.
/// Each associated constant controls a specific feature.
pub trait Capabilities: 'static + Clone {
    // DML
    const SELECT: bool;
    const INSERT: bool;
    const UPDATE: bool;
    const DELETE: bool;

    // DDL
    const CREATE_TABLE: bool;
    const DROP_TABLE: bool;
    const ALTER_TABLE: bool;
    const CREATE_INDEX: bool;
    const DROP_INDEX: bool;
    const CREATE_TRIGGER: bool;
    const DROP_TRIGGER: bool;

    // Transactions
    const BEGIN: bool;
    const COMMIT: bool;
    const ROLLBACK: bool;

    // Expression features
    const SUBQUERY: bool;
    const AGGREGATE: bool;
    const WINDOW_FN: bool;
    const CTE: bool;
}

// =============================================================================
// Capability helper traits for method availability
// =============================================================================

/// Trait for capabilities that allow SELECT statements.
pub trait CanSelect: Capabilities {}

/// Trait for capabilities that allow INSERT statements.
pub trait CanInsert: Capabilities {}

/// Trait for capabilities that allow UPDATE statements.
pub trait CanUpdate: Capabilities {}

/// Trait for capabilities that allow DELETE statements.
pub trait CanDelete: Capabilities {}

/// Trait for capabilities that allow subqueries.
pub trait CanSubquery: Capabilities {}

/// Trait for capabilities that allow aggregate functions.
pub trait CanAggregate: Capabilities {}

/// Trait for capabilities that allow window functions.
pub trait CanWindowFn: Capabilities {}

/// Trait for capabilities that allow CTEs.
pub trait CanCte: Capabilities {}

// =============================================================================
// Capability implementations
// =============================================================================

/// All statements allowed (default).
#[derive(Debug, Clone, Copy, Default)]
pub struct Full;

impl Capabilities for Full {
    const SELECT: bool = true;
    const INSERT: bool = true;
    const UPDATE: bool = true;
    const DELETE: bool = true;
    const CREATE_TABLE: bool = true;
    const DROP_TABLE: bool = true;
    const ALTER_TABLE: bool = true;
    const CREATE_INDEX: bool = true;
    const DROP_INDEX: bool = true;
    const CREATE_TRIGGER: bool = true;
    const DROP_TRIGGER: bool = true;
    const BEGIN: bool = true;
    const COMMIT: bool = true;
    const ROLLBACK: bool = true;
    const SUBQUERY: bool = true;
    const AGGREGATE: bool = true;
    const WINDOW_FN: bool = true;
    const CTE: bool = true;
}

impl CanSelect for Full {}
impl CanInsert for Full {}
impl CanUpdate for Full {}
impl CanDelete for Full {}
impl CanSubquery for Full {}
impl CanAggregate for Full {}
impl CanWindowFn for Full {}
impl CanCte for Full {}

/// DML only (SELECT, INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, Copy, Default)]
pub struct DmlOnly;

impl Capabilities for DmlOnly {
    const SELECT: bool = true;
    const INSERT: bool = true;
    const UPDATE: bool = true;
    const DELETE: bool = true;
    const CREATE_TABLE: bool = false;
    const DROP_TABLE: bool = false;
    const ALTER_TABLE: bool = false;
    const CREATE_INDEX: bool = false;
    const DROP_INDEX: bool = false;
    const CREATE_TRIGGER: bool = false;
    const DROP_TRIGGER: bool = false;
    const BEGIN: bool = false;
    const COMMIT: bool = false;
    const ROLLBACK: bool = false;
    const SUBQUERY: bool = true;
    const AGGREGATE: bool = true;
    const WINDOW_FN: bool = true;
    const CTE: bool = true;
}

impl CanSelect for DmlOnly {}
impl CanInsert for DmlOnly {}
impl CanUpdate for DmlOnly {}
impl CanDelete for DmlOnly {}
impl CanSubquery for DmlOnly {}
impl CanAggregate for DmlOnly {}
impl CanWindowFn for DmlOnly {}
impl CanCte for DmlOnly {}

/// Only SELECT statements.
#[derive(Debug, Clone, Copy, Default)]
pub struct SelectOnly;

impl Capabilities for SelectOnly {
    const SELECT: bool = true;
    const INSERT: bool = false;
    const UPDATE: bool = false;
    const DELETE: bool = false;
    const CREATE_TABLE: bool = false;
    const DROP_TABLE: bool = false;
    const ALTER_TABLE: bool = false;
    const CREATE_INDEX: bool = false;
    const DROP_INDEX: bool = false;
    const CREATE_TRIGGER: bool = false;
    const DROP_TRIGGER: bool = false;
    const BEGIN: bool = false;
    const COMMIT: bool = false;
    const ROLLBACK: bool = false;
    const SUBQUERY: bool = true;
    const AGGREGATE: bool = true;
    const WINDOW_FN: bool = true;
    const CTE: bool = true;
}

impl CanSelect for SelectOnly {}
impl CanSubquery for SelectOnly {}
impl CanAggregate for SelectOnly {}
impl CanWindowFn for SelectOnly {}
impl CanCte for SelectOnly {}

/// No subqueries (useful for expression-only contexts).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSubquery;

impl Capabilities for NoSubquery {
    const SELECT: bool = true;
    const INSERT: bool = true;
    const UPDATE: bool = true;
    const DELETE: bool = true;
    const CREATE_TABLE: bool = true;
    const DROP_TABLE: bool = true;
    const ALTER_TABLE: bool = true;
    const CREATE_INDEX: bool = true;
    const DROP_INDEX: bool = true;
    const CREATE_TRIGGER: bool = true;
    const DROP_TRIGGER: bool = true;
    const BEGIN: bool = true;
    const COMMIT: bool = true;
    const ROLLBACK: bool = true;
    const SUBQUERY: bool = false;
    const AGGREGATE: bool = true;
    const WINDOW_FN: bool = false;
    const CTE: bool = false;
}

impl CanSelect for NoSubquery {}
impl CanInsert for NoSubquery {}
impl CanUpdate for NoSubquery {}
impl CanDelete for NoSubquery {}
impl CanAggregate for NoSubquery {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_full_capabilities() {
        assert!(Full::SELECT);
        assert!(Full::INSERT);
        assert!(Full::UPDATE);
        assert!(Full::DELETE);
        assert!(Full::SUBQUERY);
        assert!(Full::AGGREGATE);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_dml_only_capabilities() {
        assert!(DmlOnly::SELECT);
        assert!(DmlOnly::INSERT);
        assert!(!DmlOnly::CREATE_TABLE);
        assert!(!DmlOnly::BEGIN);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_select_only_capabilities() {
        assert!(SelectOnly::SELECT);
        assert!(!SelectOnly::INSERT);
        assert!(!SelectOnly::UPDATE);
        assert!(!SelectOnly::DELETE);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_no_subquery_capabilities() {
        assert!(NoSubquery::SELECT);
        assert!(!NoSubquery::SUBQUERY);
        assert!(!NoSubquery::CTE);
        assert!(!NoSubquery::WINDOW_FN);
    }
}
