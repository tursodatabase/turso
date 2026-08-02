//! Statement journal flag analysis (`is_multi_write` / `may_abort`).
//!
//! Inside an explicit transaction (BEGIN...COMMIT), each statement runs within
//! the larger transaction. If a statement partially completes and then aborts
//! (e.g. a UNIQUE constraint violation on the third row of a multi-row INSERT),
//! the partial writes must be rolled back without discarding the entire
//! transaction. SQLite solves this with a "statement journal" (subjournal): a
//! savepoint taken at the start of each statement, rolled back on abort.
//!
//! Statement journals are expensive, so SQLite skips them when provably
//! unnecessary. The condition is: `usesStmtJournal = isMultiWrite && mayAbort`.
//!
//! - **isMultiWrite**: the statement may modify more than one row (or more than
//!   one table, e.g. FK counter + data table). A single-row write is atomic —
//!   either all writes happen or none do — so no partial state to roll back.
//!
//! - **mayAbort**: the statement may fail mid-execution with an ABORT (e.g.
//!   constraint violation, FK violation, RAISE(ABORT) in a trigger). If a
//!   multi-write statement can never abort, partial rollback is moot.
//!
//! Both flags default to `true` (conservative). Each DML translate path calls
//! into this module to set them to `false` when safe.

use turso_parser::ast::ResolveType;

/// Check whether any DDL-level constraint (IPK or index) uses REPLACE.
pub(crate) fn any_index_or_ipk_has_replace(
    rowid_alias_conflict: Option<ResolveType>,
    mut indexes: impl Iterator<Item = Option<ResolveType>>,
) -> bool {
    rowid_alias_conflict == Some(ResolveType::Replace)
        || indexes.any(|oc| oc == Some(ResolveType::Replace))
}

/// Check whether any constraint's effective resolution is REPLACE.
///
/// When a statement-level override exists, only the statement conflict mode matters.
/// Otherwise, both the PK's DDL mode and each index's DDL mode are checked.
pub(crate) fn any_effective_replace(
    has_statement_conflict: bool,
    statement_conflict: ResolveType,
    rowid_alias_conflict: Option<ResolveType>,
    indexes: impl Iterator<Item = Option<ResolveType>>,
) -> bool {
    if has_statement_conflict {
        matches!(statement_conflict, ResolveType::Replace)
    } else {
        any_index_or_ipk_has_replace(rowid_alias_conflict, indexes)
    }
}

/// Determine whether any constraint's effective resolution can trigger an
/// ABORT. Each constraint has an effective resolution mode — either the
/// statement-level override (when present) or its DDL-level mode (defaulting
/// to ABORT). A constraint can cause an ABORT when:
///
/// - Its effective mode is ABORT and it has any checkable constraint
///   (NOT NULL, CHECK, UNIQUE).
/// - Its effective mode is REPLACE and the table has NOT NULL or CHECK
///   constraints, because REPLACE falls back to ABORT for those.
///
/// IGNORE and FAIL never trigger statement-level ABORT.
/// Each index is represented as `(on_conflict, is_unique)`.
pub(crate) fn constraint_may_abort(
    has_statement_conflict: bool,
    statement_conflict: ResolveType,
    rowid_alias_conflict: Option<ResolveType>,
    mut indexes: impl Iterator<Item = (Option<ResolveType>, bool)>,
    has_notnull: bool,
    has_check: bool,
    has_unique: bool,
) -> bool {
    if has_statement_conflict {
        // Statement-level override applies uniformly to all constraints.
        return match statement_conflict {
            ResolveType::Abort => has_notnull || has_check || has_unique,
            ResolveType::Replace => has_notnull || has_check, // UNIQUE conflict gets replaced, not aborted.
            _ => false, // IGNORE, FAIL, ROLLBACK don't need statement journal
        };
    }
    // No statement-level override — each constraint uses its DDL-level mode.
    let pk_mode = rowid_alias_conflict.unwrap_or(ResolveType::Abort);
    let pk_aborts = match pk_mode {
        ResolveType::Abort => has_unique, // PK is a unique constraint
        ResolveType::Replace => false,    // PK REPLACE doesn't fall back for unique
        _ => false,
    };
    let idx_aborts = indexes.any(|(on_conflict, unique)| {
        let mode = on_conflict.unwrap_or(ResolveType::Abort);
        match mode {
            ResolveType::Abort => unique, // only unique indexes can conflict
            ResolveType::Replace => has_notnull || has_check,
            _ => false,
        }
    });
    // Default ABORT applies to NOT NULL and CHECK (they aren't per-index).
    let default_aborts = has_notnull || has_check;
    pk_aborts || idx_aborts || default_aborts
}
