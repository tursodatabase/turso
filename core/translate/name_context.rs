#![allow(dead_code)]

use super::expr::BindingBehavior;
use super::plan::{OuterQueryReference, ResultSetColumn, TableReferences};
use crate::schema::Table;
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

#[derive(Debug, Clone, Copy)]
pub struct SourceScope<'a> {
    table_references: &'a TableReferences,
}

impl<'a> SourceScope<'a> {
    pub fn new(table_references: &'a TableReferences) -> Self {
        Self { table_references }
    }

    pub fn local_tables(&self) -> impl Iterator<Item = SourceTableRef<'a>> {
        self.table_references.joined_tables().iter().map(|table_ref| SourceTableRef {
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
                result_column.alias.as_deref().map(|alias| ResultAliasBinding {
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
}
