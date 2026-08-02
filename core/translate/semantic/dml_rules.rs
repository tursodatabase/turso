//! DML destination and expression-namespace rules.

use turso_parser::ast;

use super::{
    hir::{self, TargetColumn},
    scope::Scope,
};
use crate::{schema::Table, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DmlOperation {
    Insert,
    Update,
    Delete,
}

impl DmlOperation {
    pub(super) fn generated_column_verb(self) -> &'static str {
        match self {
            Self::Insert => "INSERT into",
            Self::Update => "UPDATE",
            Self::Delete => unreachable!("DELETE has no destination columns"),
        }
    }
}

pub(super) fn resolve_insert_columns(
    table: &Table,
    columns: &[ast::Name],
) -> Result<Vec<TargetColumn>> {
    if columns.is_empty() {
        return Ok(table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| !column.hidden() && !column.is_generated())
            .map(|(index, _)| TargetColumn::Column(index))
            .collect());
    }

    columns
        .iter()
        .map(|name| resolve_target_column(table, name, DmlOperation::Insert))
        .collect()
}

pub(super) fn resolve_assignment_columns(
    table: &Table,
    columns: &[ast::Name],
) -> Result<Vec<TargetColumn>> {
    columns
        .iter()
        .map(|name| resolve_target_column(table, name, DmlOperation::Update))
        .collect()
}

pub(super) fn resolve_target_column(
    table: &Table,
    name: &ast::Name,
    operation: DmlOperation,
) -> Result<TargetColumn> {
    let normalized = crate::util::normalize_ident(name.as_str());
    if let Some((index, column)) = table.get_column_by_name(&normalized) {
        column.ensure_not_generated(operation.generated_column_verb(), &normalized)?;
        return Ok(TargetColumn::Column(index));
    }

    if is_rowid_name(&normalized) && table_has_rowid(table) {
        if let Some((index, _)) = table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.is_rowid_alias())
        {
            return Ok(TargetColumn::Column(index));
        }
        return Ok(TargetColumn::RowId);
    }

    match operation {
        DmlOperation::Insert => crate::bail_parse_error!(
            "table {} has no column named {}",
            table.get_name(),
            normalized
        ),
        DmlOperation::Update => crate::bail_parse_error!("no such column: {}", name),
        DmlOperation::Delete => unreachable!("DELETE has no destination columns"),
    }
}

/// Add the unchanged target row visible to UPDATE/DELETE expressions.
pub(super) fn configure_target_read_scope(scope: &mut Scope, target: &hir::Source) {
    scope.add_source(target, true);
}

/// Add the two row images visible to `DO UPDATE` expressions.
///
/// The target participates in unqualified lookup; `excluded` does not. This
/// lets `value` mean the old target value while `excluded.value` means the
/// value proposed by the INSERT.
pub(super) fn configure_upsert_scope(
    scope: &mut Scope,
    target: &hir::Source,
    excluded: &hir::Source,
) {
    scope.add_source(target, true);
    scope.add_source(excluded, false);
    scope.report_missing_qualified_name_as_column();
}

/// Add a DML target under its schema name, ignoring any write-target alias.
pub(super) fn add_schema_named_target(scope: &mut Scope, target: &hir::Source) {
    scope.add_source_with_schema_name(target, true);
}

fn table_has_rowid(table: &Table) -> bool {
    table.btree().is_some_and(|table| table.has_rowid) || table.virtual_table().is_some()
}

fn is_rowid_name(name: &str) -> bool {
    matches!(name, "rowid" | "_rowid_" | "oid")
}
