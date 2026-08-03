//! Trigger row-image, conflict, and database rules.

use turso_parser::ast;

use super::{
    hir::{self, PseudoSource},
    scope::QueryEnvironment,
};
use crate::{MAIN_DB_ID, TEMP_DB_ID};

const NEW_NOT_VISIBLE: &str = "NEW references are only valid in INSERT and UPDATE triggers";
const OLD_NOT_VISIBLE: &str = "OLD references are only valid in UPDATE and DELETE triggers";

pub(super) fn query_environment(environment: &hir::TriggerEnvironment) -> QueryEnvironment {
    let environment_builder = QueryEnvironment::empty().within_trigger_program();
    let environment_builder = match environment.new_source {
        Some(source) => environment_builder.with_visible_pseudo_source(PseudoSource::New, source),
        None => {
            environment_builder.with_forbidden_pseudo_source(PseudoSource::New, NEW_NOT_VISIBLE)
        }
    };
    match environment.old_source {
        Some(source) => environment_builder.with_visible_pseudo_source(PseudoSource::Old, source),
        None => {
            environment_builder.with_forbidden_pseudo_source(PseudoSource::Old, OLD_NOT_VISIBLE)
        }
    }
}

/// Preserve SQLite's rule that the statement which fired a trigger overrides
/// a conflict clause written inside the trigger program.
pub(super) fn effective_conflict_policy(
    inherited: Option<ast::ResolveType>,
    local: Option<ast::ResolveType>,
) -> Option<ast::ResolveType> {
    inherited.or(local)
}

pub(super) fn apply_pseudo_column_affinity(columns: &mut [hir::SourceColumn]) {
    for column in columns {
        // SQLite treats NEW/OLD fields as register values rather than table
        // columns during comparisons. A true rowid alias is the exception:
        // it keeps INTEGER affinity just like NEW.rowid and OLD.rowid.
        column.has_affinity = column.rowid_alias;
    }
}

pub(super) const fn restricts_database_references(trigger_database: usize) -> bool {
    trigger_database != TEMP_DB_ID
}

pub(super) const fn default_database(trigger_database: usize) -> usize {
    if restricts_database_references(trigger_database) {
        trigger_database
    } else {
        MAIN_DB_ID
    }
}

pub(super) const fn database_reference_allowed(
    trigger_database: usize,
    referenced_database: usize,
) -> bool {
    !restricts_database_references(trigger_database) || trigger_database == referenced_database
}
