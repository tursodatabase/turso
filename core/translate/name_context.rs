#![allow(dead_code)]

use super::expr::BindingBehavior;
use super::plan::{JoinInfo, OuterQueryReference, ResultSetColumn, TableReferences};
use crate::schema::Table;
use crate::translate::planner::parse_row_id;
use crate::util::normalize_ident;
use crate::Result;
use turso_parser::ast::TableInternalId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AliasVisibility {
    Hidden,
    Fallback,
    Preferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourcePrecedence {
    SourceFirst,
    ResultAliasFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateVisibility {
    Deny,
    Allow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NameResolutionPolicy {
    pub local_result_aliases: AliasVisibility,
    pub outer_result_aliases: AliasVisibility,
    pub source_precedence: SourcePrecedence,
    pub allow_aggregates: AggregateVisibility,
}

impl NameResolutionPolicy {
    pub fn for_legacy_behavior(binding_behavior: BindingBehavior) -> Self {
        match binding_behavior {
            BindingBehavior::TryResultColumnsFirst => Self {
                local_result_aliases: AliasVisibility::Preferred,
                outer_result_aliases: AliasVisibility::Preferred,
                source_precedence: SourcePrecedence::ResultAliasFirst,
                allow_aggregates: AggregateVisibility::Allow,
            },
            BindingBehavior::TryCanonicalColumnsFirst => Self {
                local_result_aliases: AliasVisibility::Fallback,
                outer_result_aliases: AliasVisibility::Fallback,
                source_precedence: SourcePrecedence::SourceFirst,
                allow_aggregates: AggregateVisibility::Allow,
            },
            BindingBehavior::ResultColumnsNotAllowed | BindingBehavior::AllowUnboundIdentifiers => {
                Self {
                    local_result_aliases: AliasVisibility::Hidden,
                    outer_result_aliases: AliasVisibility::Hidden,
                    source_precedence: SourcePrecedence::SourceFirst,
                    allow_aggregates: AggregateVisibility::Deny,
                }
            }
        }
    }

    pub fn prefers_local_result_aliases(self) -> bool {
        self.local_result_aliases == AliasVisibility::Preferred
    }

    pub fn allows_local_result_alias_fallback(self) -> bool {
        self.local_result_aliases == AliasVisibility::Fallback
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SourceTableRef<'a> {
    pub identifier: &'a str,
    pub internal_id: TableInternalId,
    pub table: &'a Table,
}

#[derive(Debug, Clone, Copy)]
pub struct OuterSourceRef<'a> {
    pub identifier: &'a str,
    pub internal_id: TableInternalId,
    pub table: &'a Table,
    pub cte_definition_only: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct CteNameRef<'a> {
    pub identifier: &'a str,
    pub internal_id: TableInternalId,
}

#[derive(Debug, Clone, Copy)]
pub struct ResultAliasBinding<'a> {
    pub alias: &'a str,
    pub result_column_index: usize,
    pub contains_aggregates: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedColumn {
    pub table_id: TableInternalId,
    pub column_index: usize,
    pub is_rowid_alias: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedSourceName {
    Column(ResolvedColumn),
    RowId { table_id: TableInternalId },
}

#[derive(Debug, Clone, Copy)]
pub struct SourceScope<'a> {
    table_references: &'a TableReferences,
}

impl<'a> SourceScope<'a> {
    pub fn new(table_references: &'a TableReferences) -> Self {
        Self { table_references }
    }

    pub fn local_tables(&self) -> impl Iterator<Item = SourceTableRef<'a>> {
        self.table_references
            .joined_tables()
            .iter()
            .map(|table_ref| SourceTableRef {
                identifier: table_ref.identifier.as_str(),
                internal_id: table_ref.internal_id,
                table: &table_ref.table,
            })
    }

    pub fn outer_refs(&self) -> impl Iterator<Item = OuterSourceRef<'a>> {
        self.table_references
            .outer_query_refs()
            .iter()
            .map(|outer_ref| OuterSourceRef {
                identifier: outer_ref.identifier.as_str(),
                internal_id: outer_ref.internal_id,
                table: &outer_ref.table,
                cte_definition_only: outer_ref.cte_definition_only,
            })
    }

    pub fn from_visible_ctes(&self) -> impl Iterator<Item = CteNameRef<'a>> {
        self.table_references
            .outer_query_refs()
            .iter()
            .filter(|outer_ref| matches!(outer_ref.table, Table::FromClauseSubquery(_)))
            .map(|outer_ref| CteNameRef {
                identifier: outer_ref.identifier.as_str(),
                internal_id: outer_ref.internal_id,
            })
    }

    pub fn table_references(&self) -> &'a TableReferences {
        self.table_references
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResultAliasScope<'a> {
    result_columns: &'a [ResultSetColumn],
}

impl<'a> ResultAliasScope<'a> {
    pub fn new(result_columns: &'a [ResultSetColumn]) -> Self {
        Self { result_columns }
    }

    pub fn aliases(&self) -> impl Iterator<Item = ResultAliasBinding<'a>> {
        self.result_columns
            .iter()
            .enumerate()
            .filter_map(|(result_column_index, result_column)| {
                result_column
                    .alias
                    .as_deref()
                    .map(|alias| ResultAliasBinding {
                        alias,
                        result_column_index,
                        contains_aggregates: result_column.contains_aggregates,
                    })
            })
    }

    pub fn result_columns(&self) -> &'a [ResultSetColumn] {
        self.result_columns
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NameContext<'a> {
    pub parent: Option<&'a NameContext<'a>>,
    pub source_scope: Option<SourceScope<'a>>,
    pub result_scope: Option<ResultAliasScope<'a>>,
    pub policy: NameResolutionPolicy,
}

impl<'a> NameContext<'a> {
    pub fn new(
        parent: Option<&'a NameContext<'a>>,
        source_scope: Option<SourceScope<'a>>,
        result_scope: Option<ResultAliasScope<'a>>,
        policy: NameResolutionPolicy,
    ) -> Self {
        Self {
            parent,
            source_scope,
            result_scope,
            policy,
        }
    }

    pub fn from_legacy(
        table_references: Option<&'a TableReferences>,
        result_columns: Option<&'a [ResultSetColumn]>,
        binding_behavior: BindingBehavior,
    ) -> Self {
        Self {
            parent: None,
            source_scope: table_references.map(SourceScope::new),
            result_scope: result_columns.map(ResultAliasScope::new),
            policy: NameResolutionPolicy::for_legacy_behavior(binding_behavior),
        }
    }

    pub fn legacy_outer_refs(&self) -> &'a [OuterQueryReference] {
        self.source_scope
            .map(|scope| scope.table_references().outer_query_refs())
            .unwrap_or(&[])
    }

    pub fn find_local_result_alias(&self, identifier: &str) -> Option<&'a ResultSetColumn> {
        let normalized_id = normalize_ident(identifier);
        self.result_scope.and_then(|scope| {
            scope.result_columns().iter().find(|result_column| {
                result_column
                    .alias
                    .as_ref()
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(&normalized_id))
            })
        })
    }

    pub fn find_outer_result_alias(&self, identifier: &str) -> Option<&'a ResultSetColumn> {
        if self.policy.outer_result_aliases == AliasVisibility::Hidden {
            return None;
        }
        self.parent.and_then(|parent| {
            parent
                .find_local_result_alias(identifier)
                .or_else(|| parent.find_outer_result_alias(identifier))
        })
    }

    pub fn resolve_unqualified_source_column(
        &self,
        identifier: &str,
    ) -> Result<Option<ResolvedSourceName>> {
        let Some(source_scope) = self.source_scope else {
            return Ok(None);
        };
        let normalized_id = normalize_ident(identifier);
        let table_references = source_scope.table_references();

        let mut match_result = None;
        for joined_table in table_references.joined_tables().iter() {
            let col_idx = joined_table.table.columns().iter().position(|c| {
                c.name
                    .as_ref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
            });
            if let Some(col_idx) = col_idx {
                if let Some(existing) = match_result {
                    if !Self::column_ambiguity_allowed(&joined_table.join_info, &normalized_id) {
                        crate::bail_parse_error!("Column {} is ambiguous", identifier);
                    }
                    match_result = Some(existing);
                } else {
                    let col = joined_table.table.columns().get(col_idx).unwrap();
                    match_result = Some(ResolvedColumn {
                        table_id: joined_table.internal_id,
                        column_index: col_idx,
                        is_rowid_alias: col.is_rowid_alias(),
                    });
                }
            }
        }

        if match_result.is_some() {
            return Ok(match_result.map(ResolvedSourceName::Column));
        }

        if table_references.joined_tables().len() == 1 {
            let joined_table = &table_references.joined_tables()[0];
            if let Table::BTree(btree) = &joined_table.table {
                if let Some(row_id_expr) =
                    parse_row_id(&normalized_id, joined_table.internal_id, || false)?
                {
                    if !btree.has_rowid {
                        crate::bail_parse_error!("no such column: {}", identifier);
                    }
                    return Ok(Some(Self::resolved_source_name_from_rowid_expr(
                        &row_id_expr,
                    )));
                }
            }
        }

        for outer_ref in table_references.outer_query_refs().iter() {
            if matches!(outer_ref.table, Table::FromClauseSubquery(_)) {
                continue;
            }
            let col_idx = outer_ref.table.columns().iter().position(|c| {
                c.name
                    .as_ref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(&normalized_id))
            });
            if let Some(col_idx) = col_idx {
                if match_result.is_some() {
                    crate::bail_parse_error!("Column {} is ambiguous", identifier);
                }
                let col = outer_ref.table.columns().get(col_idx).unwrap();
                match_result = Some(ResolvedColumn {
                    table_id: outer_ref.internal_id,
                    column_index: col_idx,
                    is_rowid_alias: col.is_rowid_alias(),
                });
            }
        }

        Ok(match_result.map(ResolvedSourceName::Column))
    }

    fn column_ambiguity_allowed(join_info: &Option<JoinInfo>, identifier: &str) -> bool {
        join_info.as_ref().is_some_and(|join_info| {
            join_info
                .using
                .iter()
                .any(|using_col| using_col.as_str().eq_ignore_ascii_case(identifier))
        })
    }

    fn resolved_source_name_from_rowid_expr(expr: &turso_parser::ast::Expr) -> ResolvedSourceName {
        match expr {
            turso_parser::ast::Expr::RowId { table, .. } => {
                ResolvedSourceName::RowId { table_id: *table }
            }
            _ => unreachable!("expected rowid expression"),
        }
    }
}
