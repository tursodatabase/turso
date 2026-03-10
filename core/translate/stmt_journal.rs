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
use crate::translate::trigger_exec::has_relevant_triggers_type_only;
use crate::vdbe::builder::ProgramBuilder;
use crate::{sync::Arc, Connection, HashSet, Result};
use turso_parser::ast::TriggerEvent;

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

/// Determine whether a DML statement may abort mid-execution.
///
/// REPLACE falls back to ABORT for NOT NULL (without default) and CHECK
/// constraints, so even REPLACE statements can abort in those cases.
fn compute_may_abort(
    has_triggers: bool,
    has_fks: bool,
    has_abort_resolution: bool,
    is_replace: bool,
    has_notnull: bool,
    has_check: bool,
    has_unique: bool,
) -> bool {
    let has_constraint_that_aborts = has_notnull || has_check || has_unique;
    let replace_can_abort = is_replace && (has_notnull || has_check);
    has_triggers
        || has_fks
        || (has_abort_resolution && has_constraint_that_aborts)
        || replace_can_abort
}

/// Context for INSERT statement journal flag analysis.
pub(crate) struct InsertJournalCtx {
    pub inserting_multiple_rows: bool,
    pub has_triggers: bool,
    pub has_fks: bool,
    pub is_replace: bool,
    pub has_upsert: bool,
    pub has_autoincrement: bool,
    pub has_abort_resolution: bool,
    pub notnull_col_exists: bool,
    pub has_check: bool,
    pub has_unique: bool,
}

/// Set multi_write / may_abort for INSERT statements.
pub(crate) fn set_insert_stmt_journal_flags(program: &mut ProgramBuilder, ctx: &InsertJournalCtx) {
    // UPSERT is multi-write because DO UPDATE modifies an existing row.
    // AUTOINCREMENT is multi-write because sqlite_sequence is updated before constraint checks.
    if !ctx.inserting_multiple_rows
        && !ctx.has_triggers
        && !ctx.is_replace
        && !ctx.has_upsert
        && !ctx.has_autoincrement
    {
        program.set_multi_write(false);
    }
    program.set_may_abort(compute_may_abort(
        ctx.has_triggers,
        ctx.has_fks,
        ctx.has_abort_resolution,
        ctx.is_replace,
        ctx.notnull_col_exists,
        ctx.has_check,
        ctx.has_unique,
    ));
}

/// Set multi_write / may_abort for UPDATE statements.
pub(crate) fn set_update_stmt_journal_flags(
    program: &mut ProgramBuilder,
    plan: &UpdatePlan,
    resolver: &Resolver,
    connection: &crate::sync::Arc<crate::Connection>,
) -> Result<()> {
    // When an ephemeral table is used (key mutation / Halloween protection),
    // the actual target table is in the ephemeral_plan's table_references.
    let table_refs = plan
        .ephemeral_plan
        .as_ref()
        .map(|ep| &ep.table_references)
        .unwrap_or(&plan.table_references);
    let Some(target_table) = table_refs.joined_tables().first() else {
        crate::bail_parse_error!("UPDATE should have one target table");
    };
    let Some(btree_table) = target_table.btree() else {
        return Ok(()); // Virtual table — keep conservative defaults.
    };
    let database_id = target_table.database_id;

    let updated_cols: HashSet<usize> = plan.set_clauses.iter().map(|(i, _)| *i).collect();
    let has_triggers = resolver.with_schema(database_id, |s| {
        has_relevant_triggers_type_only(s, TriggerEvent::Update, Some(&updated_cols), &btree_table)
    });
    let has_fks = table_has_fks(connection, resolver, database_id, btree_table.name.as_str());
    let is_replace = plan
        .or_conflict
        .as_ref()
        .is_some_and(|c| matches!(c, turso_parser::ast::ResolveType::Replace));

    // Partial unique indexes can't be preflighted (their WHERE predicate isn't evaluated
    // until the per-index loop), so the loop may interleave constraint checks with mutations
    // from non-partial indexes. When this can happen, keep multi_write=true so the statement
    // journal protects against orphan index entries on abort.
    let has_partial_unique = plan
        .indexes_to_update
        .iter()
        .any(|idx| idx.unique && idx.where_clause.is_some());

    // Ephemeral tables (used for key mutation / Halloween protection) always scan all
    // collected rows, so affects_max_1_row() returns false — multi_write stays true.
    let is_single_row =
        plan.limit.is_none() && plan.offset.is_none() && target_table.op.affects_max_1_row();
    if is_single_row && !has_triggers && !is_replace && !has_fks && !has_partial_unique {
        program.set_multi_write(false);
    }

    let or_conflict = plan
        .or_conflict
        .unwrap_or(turso_parser::ast::ResolveType::Abort);
    let has_abort_resolution = matches!(or_conflict, turso_parser::ast::ResolveType::Abort);
    let has_notnull_cols = plan.set_clauses.iter().any(|(col_idx, _)| {
        if *col_idx == crate::schema::ROWID_SENTINEL {
            return false;
        }
        btree_table
            .columns
            .get(*col_idx)
            .is_some_and(|c| c.notnull() && !c.is_rowid_alias())
    });
    let has_check = !btree_table.check_constraints.is_empty();
    let has_unique =
        !btree_table.unique_sets.is_empty() || plan.indexes_to_update.iter().any(|idx| idx.unique);

    program.set_may_abort(compute_may_abort(
        has_triggers,
        has_fks,
        has_abort_resolution,
        is_replace,
        has_notnull_cols,
        has_check,
        has_unique,
    ));
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
