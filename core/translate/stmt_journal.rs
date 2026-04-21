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

use crate::translate::emitter::Resolver;
use crate::translate::plan::{DeletePlan, DmlSafetyReason, UpdatePlan};
use crate::translate::trigger_exec::has_triggers_including_temp;
use crate::vdbe::builder::ProgramBuilder;
use crate::{sync::Arc, Connection, Result};
use turso_parser::ast::{ResolveType, TriggerEvent};

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

/// Check whether a table has any FK relationships (child or parent side).
fn table_has_fks(
    connection: &crate::Connection,
    resolver: &Resolver,
    database_id: usize,
    table_name: &str,
) -> bool {
    connection.foreign_keys_enabled()
        && (resolver.with_schema(database_id, |s| s.has_child_fks(table_name))
            || resolver.with_schema(database_id, |s| s.any_resolved_fks_referencing(table_name)))
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

/// Set multi_write / may_abort for INSERT statements.
///
/// Constraint analysis (any_replace, constraint_may_abort) is computed internally
/// from the table schema and resolver. The caller provides INSERT-specific flags
/// that come from the emitter's own analysis.
#[allow(clippy::too_many_arguments)]
pub(crate) fn set_insert_stmt_journal_flags(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    table: &crate::schema::BTreeTable,
    has_statement_conflict: bool,
    statement_conflict: ResolveType,
    inserting_multiple_rows: bool,
    has_triggers: bool,
    has_fks: bool,
    has_upsert: bool,
    has_autoincrement: bool,
    notnull_col_exists: bool,
    has_unique: bool,
) {
    let index_modes: Vec<(Option<ResolveType>, bool)> = resolver.with_schema(database_id, |s| {
        s.get_indices(&table.name)
            .map(|idx| (idx.on_conflict, idx.unique))
            .collect()
    });
    let any_replace = any_effective_replace(
        has_statement_conflict,
        statement_conflict,
        table.rowid_alias_conflict_clause,
        index_modes.iter().map(|(oc, _)| *oc),
    );
    let has_check = !table.check_constraints.is_empty();
    let may_abort = has_triggers
        || has_fks
        || constraint_may_abort(
            has_statement_conflict,
            statement_conflict,
            table.rowid_alias_conflict_clause,
            index_modes.into_iter(),
            notnull_col_exists,
            has_check,
            has_unique,
        );

    // UPSERT is multi-write because DO UPDATE modifies an existing row.
    // AUTOINCREMENT is multi-write because sqlite_sequence is updated before constraint checks.
    if !inserting_multiple_rows
        && !has_triggers
        && !any_replace
        && !has_upsert
        && !has_autoincrement
    {
        program.set_multi_write(false);
    }
    program.set_may_abort(may_abort);
}

/// Set multi_write / may_abort for UPDATE statements.
pub(crate) fn set_update_stmt_journal_flags(
    program: &mut ProgramBuilder,
    plan: &UpdatePlan,
    resolver: &Resolver,
    connection: &crate::sync::Arc<crate::Connection>,
) -> Result<()> {
    let target_table = &plan.target_table;
    let Some(btree_table) = target_table.btree() else {
        return Ok(()); // Virtual table — keep conservative defaults.
    };
    let database_id = target_table.database_id;

    let updated_cols = plan
        .set_clauses
        .iter()
        .map(|set_clause| set_clause.column_index)
        .collect();
    let has_triggers = has_triggers_including_temp(
        resolver,
        database_id,
        TriggerEvent::Update,
        Some(&updated_cols),
        &btree_table,
    );
    let has_fks = table_has_fks(connection, resolver, database_id, btree_table.name.as_str());

    let or_conflict = plan.or_conflict.unwrap_or(ResolveType::Abort);
    let has_statement_conflict = plan.or_conflict.is_some();

    let any_replace = any_effective_replace(
        has_statement_conflict,
        or_conflict,
        btree_table.rowid_alias_conflict_clause,
        plan.indexes_to_update.iter().map(|idx| idx.on_conflict),
    );

    // Ephemeral tables (used for key mutation / Halloween protection) always scan all
    // collected rows, so affects_max_1_row() returns false — multi_write stays true.
    let is_single_row =
        plan.limit.is_none() && plan.offset.is_none() && target_table.op.affects_max_1_row();
    if is_single_row && !has_triggers && !any_replace && !has_fks {
        program.set_multi_write(false);
    }

    let has_notnull_cols = plan.set_clauses.iter().any(|set_clause| {
        if set_clause.column_index == crate::schema::ROWID_SENTINEL {
            return false;
        }
        btree_table
            .columns()
            .get(set_clause.column_index)
            .is_some_and(|c| c.notnull() && !c.is_rowid_alias())
    });
    let has_check = !btree_table.check_constraints.is_empty();
    let has_unique =
        !btree_table.unique_sets.is_empty() || plan.indexes_to_update.iter().any(|idx| idx.unique);

    let may_abort = has_triggers
        || has_fks
        || constraint_may_abort(
            has_statement_conflict,
            or_conflict,
            btree_table.rowid_alias_conflict_clause,
            plan.indexes_to_update
                .iter()
                .map(|idx| (idx.on_conflict, idx.unique)),
            has_notnull_cols,
            has_check,
            has_unique,
        );
    program.set_may_abort(may_abort);
    Ok(())
}

/// Set multi_write / may_abort for DELETE statements.
pub(crate) fn set_delete_stmt_journal_flags(
    program: &mut ProgramBuilder,
    plan: &DeletePlan,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    database_id: usize,
) -> Result<()> {
    let Some(target_table) = plan.table_references.joined_tables().first() else {
        crate::bail_parse_error!("DELETE should have one target table");
    };
    let Some(btree_table) = target_table.btree() else {
        return Ok(()); // Virtual table — keep conservative defaults.
    };
    let has_triggers = plan.safety.reasons.contains(&DmlSafetyReason::Trigger);
    let has_fks = table_has_fks(connection, resolver, database_id, btree_table.name.as_str());

    // After rowset rewriting (for triggers/safety), the target table op is reset to a
    // Scan, so affects_max_1_row correctly returns false — no false optimization.
    let is_single_row =
        plan.limit.is_none() && plan.offset.is_none() && target_table.op.affects_max_1_row();
    if is_single_row && !has_triggers && !has_fks {
        program.set_multi_write(false);
    }

    // DELETE has no ON CONFLICT clause, so NOT NULL/CHECK/UNIQUE don't apply —
    // only triggers (RAISE(ABORT)) or FK violations can abort.
    if !has_triggers && !has_fks {
        program.set_may_abort(false);
    }
    Ok(())
}
