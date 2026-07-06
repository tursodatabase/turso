//! Statement journal flag analysis (`is_multi_write`).
//!
//! Inside an explicit transaction (BEGIN...COMMIT), each statement runs within
//! the larger transaction. If a statement partially completes and is then
//! aborted (e.g. a UNIQUE constraint violation on the third row of a multi-row
//! INSERT, or the application dropping the statement at an I/O yield point),
//! the partial writes must be rolled back without discarding the entire
//! transaction. SQLite solves this with a "statement journal" (subjournal): a
//! savepoint taken at the start of each statement, rolled back on abort.
//!
//! Statement journals are expensive, so they are skipped when provably
//! unnecessary. SQLite's condition is `usesStmtJournal = isMultiWrite &&
//! mayAbort`, but the `mayAbort` half relies on SQLite's VDBE never returning
//! control to the application in the middle of built-in row mutation. Turso's
//! VM yields `StepResult::IO` mid-mutation, and the caller may abandon (drop)
//! the statement at any such yield point — so even a statement that cannot
//! SQL-abort can need statement-level rollback (see #7594). Turso therefore
//! only skips the statement journal for single-row writes:
//!
//! - **isMultiWrite**: the statement may modify more than one row (or more than
//!   one table, e.g. FK counter + data table). A single-row write is atomic —
//!   either all writes happen or none do — so no partial state to roll back.
//!
//! The flag defaults to `true` (conservative). Each DML translate path calls
//! into this module to set it to `false` when safe.

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

/// Set multi_write for INSERT statements.
///
/// Constraint analysis (any_replace) is computed internally from the table
/// schema and resolver. The caller provides INSERT-specific flags that come
/// from the emitter's own analysis.
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
    has_upsert: bool,
    has_autoincrement: bool,
) {
    let index_modes: Vec<Option<ResolveType>> = resolver.with_schema(database_id, |s| {
        s.get_indices(&table.name)
            .map(|idx| idx.on_conflict)
            .collect()
    });
    let any_replace = any_effective_replace(
        has_statement_conflict,
        statement_conflict,
        table.rowid_alias_conflict_clause,
        index_modes.into_iter(),
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
}

/// Set multi_write for UPDATE statements.
pub(crate) fn set_update_stmt_journal_flags(
    program: &mut ProgramBuilder,
    plan: &UpdatePlan,
    resolver: &Resolver,
    connection: &crate::sync::Arc<crate::Connection>,
) -> Result<()> {
    use crate::alloc::*;
    let target_table = &plan.target_table;
    let Some(btree_table) = target_table.btree() else {
        return Ok(()); // Virtual table — keep conservative defaults.
    };
    let database_id = target_table.database_id;

    let updated_cols = plan
        .set_clauses
        .iter()
        .map(|set_clause| set_clause.column_index)
        .try_collect()?;
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
    Ok(())
}

/// Set multi_write for DELETE statements.
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
    Ok(())
}
