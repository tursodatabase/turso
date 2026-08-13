//! Query-local source-name visibility.

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use super::hir::{self, ColumnRef, OutputId, SourceId, TypeFact};
use crate::{schema::Type, sync::Arc, vdbe::affinity::Affinity, Result};

/// Namespace order for one SQL clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NamePrecedence {
    SourcesOnly,
    SourceThenOutput,
    OutputThenSource,
}

#[derive(Clone, Debug)]
pub(crate) enum ExprCollation {
    Absent,
    Explicit(hir::ResolvedCollation),
    Inherited(hir::ResolvedCollation),
}

impl ExprCollation {
    pub(crate) fn inherited(collation: Option<hir::ResolvedCollation>) -> Self {
        match collation {
            Some(collation) => Self::Inherited(collation),
            None => Self::Absent,
        }
    }

    pub(crate) fn value(&self) -> Option<&hir::ResolvedCollation> {
        match self {
            Self::Absent => None,
            Self::Explicit(collation) | Self::Inherited(collation) => Some(collation),
        }
    }

    pub(crate) fn into_output(self) -> (Option<hir::ResolvedCollation>, bool) {
        match self {
            Self::Absent => (None, false),
            Self::Explicit(collation) => (Some(collation), true),
            Self::Inherited(collation) => (Some(collation), false),
        }
    }

    pub(crate) fn output(collation: Option<hir::ResolvedCollation>, explicit: bool) -> Self {
        match (collation, explicit) {
            (None, false) => Self::Absent,
            (Some(collation), true) => Self::Explicit(collation),
            (Some(collation), false) => Self::Inherited(collation),
            (None, true) => unreachable!("explicit output collation must be resolved"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedScopeExpr {
    pub(crate) expr: hir::Expr,
    pub(crate) type_fact: TypeFact,
    pub(crate) affinity: Affinity,
    pub(crate) has_affinity: bool,
    pub(crate) collation: ExprCollation,
}

#[derive(Clone, Debug)]
pub(crate) struct ExpandedColumn {
    pub(crate) name: String,
    pub(crate) resolved: ResolvedScopeExpr,
}

pub(crate) struct GroupColumn {
    pub(crate) name: String,
    pub(crate) resolved: ResolvedScopeExpr,
    pub(crate) hidden: bool,
}

#[derive(Clone, Debug)]
struct ScopeColumn {
    source: SourceId,
    display_name: String,
    lookup_name: String,
    expr: hir::Expr,
    type_fact: TypeFact,
    affinity: Affinity,
    has_affinity: bool,
    collation: Option<hir::ResolvedCollation>,
    hidden: bool,
}

impl ScopeColumn {
    fn from_source(source: SourceId, index: usize, column: &hir::SourceColumn) -> Self {
        Self {
            source,
            display_name: column.name.clone(),
            lookup_name: crate::util::normalize_ident(&column.name),
            expr: hir::Expr::Column(ColumnRef {
                source,
                column: index,
            }),
            type_fact: column.type_fact.clone(),
            affinity: column.affinity,
            has_affinity: column.has_affinity,
            collation: column.collation.clone(),
            hidden: column.hidden,
        }
    }

    fn resolved(&self) -> ResolvedScopeExpr {
        ResolvedScopeExpr {
            expr: self.expr.clone(),
            type_fact: self.type_fact.clone(),
            affinity: self.affinity,
            has_affinity: self.has_affinity,
            collation: ExprCollation::inherited(self.collation.clone()),
        }
    }
}

pub(crate) fn resolve_source_column(source: &hir::Source, name: &str) -> Result<ResolvedScopeExpr> {
    let normalized = crate::util::normalize_ident(name);
    source
        .columns
        .iter()
        .enumerate()
        .find(|(_, column)| crate::util::normalize_ident(&column.name) == normalized)
        .map(|(index, column)| ScopeColumn::from_source(source.id, index, column).resolved())
        .ok_or_else(|| {
            crate::LimboError::ParseError(format!(
                "cannot join using column {name} - column not present in both tables"
            ))
        })
}

#[derive(Clone, Debug)]
struct ScopeSource {
    id: SourceId,
    qualifier: String,
    table_name: String,
    database: Option<hir::DatabaseId>,
    database_qualified: bool,
    columns: Vec<ScopeColumn>,
    rowid_available: bool,
    unqualified: bool,
}

#[derive(Clone, Debug)]
struct ScopeOutput {
    id: OutputId,
    name: String,
    name_kind: hir::OutputNameKind,
    expr: hir::Expr,
    type_fact: TypeFact,
    affinity: Affinity,
    has_affinity: bool,
    collation: Option<hir::ResolvedCollation>,
    collation_is_explicit: bool,
}

/// Source namespace visible from one query block.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scope {
    sources: Vec<ScopeSource>,
    visible_columns: Vec<ScopeColumn>,
    outputs: Vec<ScopeOutput>,
    outer: Option<Arc<Scope>>,
    outer_resolution_blocked: bool,
}

impl Scope {
    pub(crate) fn new(outer: Option<Scope>) -> Self {
        Self {
            outer: outer.map(Arc::new),
            ..Self::default()
        }
    }

    pub(crate) fn without_outer_resolution(mut self) -> Self {
        self.outer_resolution_blocked = true;
        self
    }

    pub(crate) fn add_source(&mut self, source: &hir::Source, unqualified: bool) {
        let columns: Vec<_> = source
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| ScopeColumn::from_source(source.id, index, column))
            .collect();
        if unqualified {
            self.visible_columns.extend(columns.iter().cloned());
        }
        self.sources.push(ScopeSource {
            id: source.id,
            qualifier: crate::util::normalize_ident(
                source.alias.as_deref().unwrap_or(&source.name),
            ),
            table_name: crate::util::normalize_ident(&source.name),
            database: source.database,
            database_qualified: source.alias.is_none(),
            columns,
            rowid_available: source.rowid_available,
            unqualified,
        });
    }

    /// Add another root-level source namespace at the same resolution level.
    pub(crate) fn append_local(&mut self, other: Self) {
        assert!(self.outer.is_none(), "destination scope must be root-level");
        assert!(other.outer.is_none(), "appended scope must be root-level");
        assert!(self.outputs.is_empty(), "destination scope has outputs");
        assert!(other.outputs.is_empty(), "appended scope has outputs");
        assert!(
            !self.outer_resolution_blocked && !other.outer_resolution_blocked,
            "root-level source scopes must allow ordinary resolution"
        );
        self.sources.extend(other.sources);
        self.visible_columns.extend(other.visible_columns);
    }

    pub(crate) fn without_outer(mut self) -> Self {
        self.outer = None;
        self
    }

    pub(crate) fn set_outer(&mut self, outer: Option<&Self>) {
        assert!(self.outer.is_none(), "scope already has an outer scope");
        self.outer = outer.cloned().map(Arc::new);
    }

    /// Add only another scope's qualified table namespaces. Group columns
    /// provide unqualified visibility; inner sources remain available for
    /// `table.column` lookup when the group has no alias.
    pub(crate) fn append_qualified_sources(&mut self, other: Self) {
        assert!(self.outputs.is_empty(), "destination scope has outputs");
        assert!(other.outputs.is_empty(), "appended scope has outputs");
        self.sources.extend(other.sources);
    }

    pub(crate) fn group_columns(&self) -> Vec<GroupColumn> {
        self.visible_columns
            .iter()
            .map(|column| GroupColumn {
                name: column.display_name.clone(),
                resolved: column.resolved(),
                hidden: column.hidden,
            })
            .collect()
    }

    pub(crate) fn has_sources(&self) -> bool {
        !self.sources.is_empty()
    }

    pub(crate) fn set_outputs(&mut self, outputs: &[hir::Output]) {
        self.outputs = outputs
            .iter()
            .map(|output| ScopeOutput {
                id: output.id,
                name: crate::util::normalize_ident(&output.name),
                name_kind: output.name_kind,
                expr: output.expr.clone(),
                type_fact: output.type_fact.clone(),
                affinity: output.affinity,
                has_affinity: output.has_affinity,
                collation: output.collation.clone(),
                collation_is_explicit: output.collation_is_explicit,
            })
            .collect();
    }

    pub(crate) fn resolve_output_ordinal(
        &self,
        ordinal: usize,
        clause: &str,
    ) -> Result<ResolvedScopeExpr> {
        let Some(output) = ordinal
            .checked_sub(1)
            .and_then(|index| self.outputs.get(index))
        else {
            crate::bail_parse_error!(
                "{} term out of range - should be between 1 and {}",
                clause,
                self.outputs.len()
            );
        };
        Ok(output.resolved())
    }

    pub(crate) fn output_type(&self, id: OutputId) -> Option<&TypeFact> {
        self.output(id)
            .map(|output| &output.type_fact)
            .or_else(|| self.outer.as_deref()?.output_type(id))
    }

    pub(crate) fn output_affinity(&self, id: OutputId) -> Option<Affinity> {
        self.output(id)
            .map(|output| output.affinity)
            .or_else(|| self.outer.as_deref()?.output_affinity(id))
    }

    pub(crate) fn output_has_affinity(&self, id: OutputId) -> Option<bool> {
        self.output(id)
            .map(|output| output.has_affinity)
            .or_else(|| self.outer.as_deref()?.output_has_affinity(id))
    }

    pub(crate) fn output_collation(&self, id: OutputId) -> Option<ExprCollation> {
        self.output(id)
            .map(ScopeOutput::collation)
            .or_else(|| self.outer.as_deref()?.output_collation(id))
    }

    pub(crate) fn output_expr(&self, id: OutputId) -> Option<&hir::Expr> {
        self.output(id)
            .map(|output| &output.expr)
            .or_else(|| self.outer.as_deref()?.output_expr(id))
    }

    pub(crate) fn resolve_unqualified(
        &self,
        name: &str,
        precedence: NamePrecedence,
    ) -> Result<Option<ResolvedScopeExpr>> {
        let normalized = crate::util::normalize_ident(name);
        let current = match precedence {
            NamePrecedence::SourcesOnly => self.resolve_source_column(&normalized)?,
            NamePrecedence::SourceThenOutput => self
                .resolve_source_column(&normalized)?
                .or_else(|| self.resolve_output(&normalized)),
            NamePrecedence::OutputThenSource => {
                if let Some(output) = self.resolve_output(&normalized) {
                    // SQLite still reports an ambiguous source name when an
                    // output alias wins this clause's namespace order.
                    self.resolve_source_column(&normalized)?;
                    Some(output)
                } else {
                    self.resolve_source_column(&normalized)?
                }
            }
        };
        if current.is_some() {
            return Ok(current);
        }
        if self.outer_resolution_blocked {
            return Ok(None);
        }
        self.outer.as_deref().map_or(Ok(None), |outer| {
            outer.resolve_unqualified(name, NamePrecedence::SourceThenOutput)
        })
    }

    pub(crate) fn resolve_qualified(
        &self,
        qualifier: &str,
        column: &str,
    ) -> Result<Option<ResolvedScopeExpr>> {
        let normalized_qualifier = crate::util::normalize_ident(qualifier);
        let normalized_column = crate::util::normalize_ident(column);
        let matching_sources: Vec<_> = self
            .sources
            .iter()
            .filter(|source| source.qualifier == normalized_qualifier)
            .collect();

        if matching_sources.is_empty() {
            if self.outer_resolution_blocked {
                if self
                    .outer
                    .as_deref()
                    .is_some_and(|outer| outer.has_qualifier(&normalized_qualifier))
                {
                    crate::bail_parse_error!("no such column: {qualifier}.{column}");
                }
                return Ok(None);
            }
            return self
                .outer
                .as_deref()
                .map_or(Ok(None), |outer| outer.resolve_qualified(qualifier, column));
        }

        let mut found = None;
        for source in matching_sources {
            let resolved = source
                .columns
                .iter()
                .find(|candidate| candidate.lookup_name == normalized_column)
                .map(ScopeColumn::resolved)
                .or_else(|| {
                    (source.rowid_available && is_rowid_name(&normalized_column))
                        .then(|| resolved_rowid(source.id))
                });
            if resolved.is_some() && found.is_some() {
                crate::bail_parse_error!("ambiguous column name: {}.{}", qualifier, column);
            }
            if resolved.is_some() {
                found = resolved;
            }
        }

        let Some(found) = found else {
            crate::bail_parse_error!("no such column: {}.{}", qualifier, column);
        };
        Ok(Some(found))
    }

    pub(crate) fn resolve_database_qualified(
        &self,
        database: hir::DatabaseId,
        table: &str,
        column: &str,
    ) -> Result<Option<ResolvedScopeExpr>> {
        let normalized_table = crate::util::normalize_ident(table);
        let normalized_column = crate::util::normalize_ident(column);
        let matching_sources: Vec<_> = self
            .sources
            .iter()
            .filter(|source| {
                source.database_qualified
                    && source.database == Some(database)
                    && source.table_name == normalized_table
            })
            .collect();

        if matching_sources.is_empty() {
            if self.outer_resolution_blocked {
                return Ok(None);
            }
            return self.outer.as_deref().map_or(Ok(None), |outer| {
                outer.resolve_database_qualified(database, table, column)
            });
        }

        let mut found = None;
        for source in matching_sources {
            let resolved = source
                .columns
                .iter()
                .find(|candidate| candidate.lookup_name == normalized_column)
                .map(ScopeColumn::resolved)
                .or_else(|| {
                    (source.rowid_available && is_rowid_name(&normalized_column))
                        .then(|| resolved_rowid(source.id))
                });
            if resolved.is_some() && found.is_some() {
                crate::bail_parse_error!("ambiguous column name: {}.{}", table, column);
            }
            if resolved.is_some() {
                found = resolved;
            }
        }

        let Some(found) = found else {
            crate::bail_parse_error!("no such column: {}.{}", table, column);
        };
        Ok(Some(found))
    }

    pub(crate) fn expand_star(&self) -> Result<Vec<ExpandedColumn>> {
        // Repeated qualifiers are legal across databases. Within one database,
        // an unmerged visible column makes an unqualified star ambiguous.
        let mut visible_sources = HashSet::default();
        let mut visible_identities = HashMap::default();
        for column in self.visible_columns.iter().filter(|column| !column.hidden) {
            visible_sources.insert(column.source);
        }
        for source in self
            .sources
            .iter()
            .filter(|source| visible_sources.contains(&source.id))
        {
            let identity = (source.database, source.qualifier.as_str());
            if visible_identities.insert(identity, source.id).is_some() {
                let column = self
                    .visible_columns
                    .iter()
                    .find(|column| !column.hidden && column.source == source.id)
                    .expect("visible source must own a visible column");
                crate::bail_parse_error!(
                    "ambiguous column name: {}.{}",
                    source.qualifier,
                    column.display_name
                );
            }
        }

        Ok(self
            .visible_columns
            .iter()
            .filter(|column| !column.hidden)
            .map(|column| ExpandedColumn {
                name: column.display_name.clone(),
                resolved: column.resolved(),
            })
            .collect())
    }

    pub(crate) fn expand_table_star(&self, qualifier: &str) -> Result<Vec<ExpandedColumn>> {
        let normalized = crate::util::normalize_ident(qualifier);
        let matching: Vec<_> = self
            .sources
            .iter()
            .filter(|source| source.qualifier == normalized)
            .collect();
        if matching.is_empty() {
            crate::bail_parse_error!("no such table: {}", qualifier);
        }
        if matching.len() > 1 {
            crate::bail_parse_error!("ambiguous table name: {}", qualifier);
        }
        Ok(matching[0]
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .map(|column| ExpandedColumn {
                name: column.display_name.clone(),
                resolved: column.resolved(),
            })
            .collect())
    }

    pub(crate) fn resolve_using_left(&self, name: &str) -> Result<ResolvedScopeExpr> {
        let normalized = crate::util::normalize_ident(name);
        self.visible_columns
            .iter()
            .find(|column| column.lookup_name == normalized)
            .map(ScopeColumn::resolved)
            .ok_or_else(|| {
                crate::LimboError::ParseError(format!(
                    "cannot join using column {name} - column not present in both tables"
                ))
            })
    }

    pub(crate) fn resolve_natural_left(&self, name: &str) -> Result<ResolvedScopeExpr> {
        let normalized = crate::util::normalize_ident(name);
        let mut matches = self
            .visible_columns
            .iter()
            .filter(|column| !column.hidden && column.lookup_name == normalized);
        let Some(column) = matches.next() else {
            return Err(crate::LimboError::InternalError(format!(
                "NATURAL join column {name} disappeared from the visible scope"
            )));
        };
        if matches.next().is_some() {
            crate::bail_parse_error!("ambiguous column name: {name}");
        }
        Ok(column.resolved())
    }

    pub(crate) fn natural_common_columns(&self, right: &hir::Source) -> Vec<String> {
        right
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .filter_map(|right_column| {
                let lookup = crate::util::normalize_ident(&right_column.name);
                self.visible_columns
                    .iter()
                    .find(|left_column| !left_column.hidden && left_column.lookup_name == lookup)
                    .map(|left_column| left_column.display_name.clone())
            })
            .collect()
    }

    pub(crate) fn apply_using(&mut self, columns: &[hir::UsingColumn]) -> Result<()> {
        for using in columns {
            let right_position = self.visible_columns.iter().position(|column| {
                matches!(
                    &column.expr,
                    hir::Expr::Column(reference) if *reference == using.right
                )
            });
            let Some(right_position) = right_position else {
                return Err(crate::LimboError::InternalError(format!(
                    "USING column {} did not belong to the right source",
                    using.name
                )));
            };
            let lookup_name = crate::util::normalize_ident(&using.name);
            let left_position = self
                .visible_columns
                .iter()
                .enumerate()
                .find(|(position, column)| {
                    *position != right_position
                        && column.lookup_name == lookup_name
                        && same_join_column(&column.expr, &using.left)
                })
                .map(|(position, _)| position);
            let Some(left_position) = left_position else {
                return Err(crate::LimboError::InternalError(format!(
                    "USING column {} did not belong to the left sources",
                    using.name
                )));
            };

            let merged = hir::Expr::MergedColumn(hir::MergedColumn {
                left: using.left.clone(),
                right: using.right,
                value: using.value,
                type_fact: using.type_fact.clone(),
                affinity: using.affinity,
                has_affinity: using.has_affinity,
                collation: using.collation.clone(),
            });

            let left = &mut self.visible_columns[left_position];
            left.expr = merged;
            left.type_fact = using.type_fact.clone();
            left.affinity = using.affinity;
            left.has_affinity = using.has_affinity;
            left.collation = using.collation.clone();
            self.visible_columns.remove(right_position);
        }
        Ok(())
    }

    fn resolve_source_column(&self, name: &str) -> Result<Option<ResolvedScopeExpr>> {
        let mut found = None;
        for column in self
            .visible_columns
            .iter()
            .filter(|column| column.lookup_name == name)
        {
            if found.is_some() {
                crate::bail_parse_error!("ambiguous column name: {}", name);
            }
            found = Some(column.resolved());
        }
        if found.is_some() {
            return Ok(found);
        }

        if !is_rowid_name(name) {
            return Ok(None);
        }
        for source in self
            .sources
            .iter()
            .filter(|source| source.unqualified && source.rowid_available)
        {
            if found.is_some() {
                crate::bail_parse_error!("ambiguous column name: {}", name);
            }
            found = Some(resolved_rowid(source.id));
        }
        Ok(found)
    }

    fn resolve_output(&self, name: &str) -> Option<ResolvedScopeExpr> {
        self.outputs
            .iter()
            .find(|output| {
                output.name_kind == hir::OutputNameKind::ExplicitAlias && output.name == name
            })
            .or_else(|| self.outputs.iter().find(|output| output.name == name))
            .map(ScopeOutput::resolved)
    }

    fn output(&self, id: OutputId) -> Option<&ScopeOutput> {
        self.outputs.iter().find(|output| output.id == id)
    }

    fn has_qualifier(&self, qualifier: &str) -> bool {
        self.sources
            .iter()
            .any(|source| source.qualifier == qualifier)
            || self
                .outer
                .as_deref()
                .is_some_and(|outer| outer.has_qualifier(qualifier))
    }
}

impl ScopeOutput {
    fn collation(&self) -> ExprCollation {
        ExprCollation::output(self.collation.clone(), self.collation_is_explicit)
    }

    fn resolved(&self) -> ResolvedScopeExpr {
        ResolvedScopeExpr {
            expr: hir::Expr::Output(self.id),
            type_fact: self.type_fact.clone(),
            affinity: self.affinity,
            has_affinity: self.has_affinity,
            collation: self.collation(),
        }
    }
}

fn same_join_column(left: &hir::Expr, right: &hir::Expr) -> bool {
    match (left, right) {
        (hir::Expr::Column(left), hir::Expr::Column(right)) => left == right,
        (hir::Expr::MergedColumn(left), hir::Expr::MergedColumn(right)) => {
            left.right == right.right
                && left.value == right.value
                && same_join_column(&left.left, &right.left)
        }
        _ => false,
    }
}

fn resolved_rowid(source: SourceId) -> ResolvedScopeExpr {
    ResolvedScopeExpr {
        expr: hir::Expr::RowId(source),
        type_fact: TypeFact::known(Type::Integer),
        affinity: Affinity::Integer,
        has_affinity: true,
        collation: ExprCollation::Absent,
    }
}

fn is_rowid_name(name: &str) -> bool {
    matches!(name, "rowid" | "_rowid_" | "oid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::collate::CollationSeq;
    use crate::translate::semantic::hir::{
        CatalogObject, CatalogObjectId, CatalogSnapshot, ColumnReadExpression, ComparisonComponent,
        ComparisonSemantics, DatabaseId, Expr, IndexCoverage, IndexHint, MergedColumnValue, Output,
        OutputNameKind, QueryBlockId, QueryId, Source, SourceColumn, SourceKind, SourceOwner,
        UsingColumn,
    };
    use turso_parser::ast::Literal;

    fn column(name: &str, hidden: bool) -> SourceColumn {
        SourceColumn {
            name: name.to_string(),
            type_fact: TypeFact::known(Type::Text),
            affinity: Affinity::Text,
            has_affinity: true,
            collation: None,
            hidden,
            rowid_alias: false,
        }
    }

    fn source(
        id: usize,
        name: &str,
        alias: Option<&str>,
        columns: Vec<SourceColumn>,
        rowid_available: bool,
    ) -> Source {
        let width = columns.len();
        Source {
            id: SourceId::new(id),
            owner: SourceOwner::Root,
            database: None,
            name: name.to_string(),
            alias: alias.map(str::to_string),
            kind: SourceKind::SchemaExpression,
            columns,
            generated_expressions: vec![ColumnReadExpression::Absent; width],
            default_expressions: vec![ColumnReadExpression::Absent; width],
            column_type_programs: vec![None; width],
            check_constraints: None,
            rowid_available,
            index_hint: IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    fn output(position: usize, name: &str, name_kind: OutputNameKind) -> Output {
        let block = QueryBlockId::new(QueryId::new(0), 0);
        Output {
            id: OutputId::query(block, position),
            name: name.to_string(),
            expr: Expr::Literal(Literal::Null),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            collation_is_explicit: false,
            name_kind,
        }
    }

    fn collation(id: u64) -> hir::ResolvedCollation {
        CatalogObject::new(
            CatalogObjectId::new(id),
            CatalogSnapshot::from_id(1),
            None,
            Arc::new(CollationSeq::NoCase),
        )
    }

    fn in_database(mut source: Source, database: usize) -> Source {
        source.database = Some(DatabaseId::new(database));
        source
    }

    fn using_column(left: Expr, right: ColumnRef, value: MergedColumnValue) -> UsingColumn {
        UsingColumn {
            name: "id".to_string(),
            left: Box::new(left),
            right,
            value,
            type_fact: TypeFact::known(Type::Text),
            affinity: Affinity::Text,
            has_affinity: true,
            collation: None,
            comparison: ComparisonSemantics {
                components: vec![ComparisonComponent {
                    affinity: Affinity::Text,
                    collation: None,
                    array: false,
                }],
            },
        }
    }

    fn expect_column(resolved: ResolvedScopeExpr, source: SourceId, column: usize) {
        assert!(resolved.has_affinity);
        assert_eq!(resolved.affinity, Affinity::Text);
        assert_eq!(resolved.type_fact.storage, Some(Type::Text));
        assert!(matches!(resolved.collation, ExprCollation::Absent));
        assert!(matches!(
            resolved.expr,
            Expr::Column(ColumnRef {
                source: actual_source,
                column: actual_column,
            }) if actual_source == source && actual_column == column
        ));
    }

    fn expect_output(resolved: ResolvedScopeExpr, output: OutputId) {
        assert!(matches!(resolved.expr, Expr::Output(actual) if actual == output));
        assert_eq!(resolved.type_fact.storage, Some(Type::Integer));
        assert_eq!(resolved.affinity, Affinity::Integer);
        assert!(resolved.has_affinity);
        assert!(matches!(resolved.collation, ExprCollation::Absent));
    }

    #[test]
    fn unqualified_names_bind_case_insensitively_including_hidden_columns() {
        let items = source(
            0,
            "items",
            None,
            vec![column("visible", false), column("Secret", true)],
            false,
        );
        let mut scope = Scope::default();
        scope.add_source(&items, true);

        let resolved = scope
            .resolve_unqualified("sEcReT", NamePrecedence::SourcesOnly)
            .expect("source namespace is valid")
            .expect("hidden column remains name-addressable");

        expect_column(resolved, items.id, 1);
    }

    #[test]
    fn duplicate_unqualified_columns_are_ambiguous() {
        let left = source(0, "left", None, vec![column("id", false)], false);
        let right = source(1, "right", None, vec![column("id", false)], false);
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);

        let error = scope
            .resolve_unqualified("id", NamePrecedence::SourcesOnly)
            .expect_err("duplicate visible columns are ambiguous");

        assert!(error.to_string().contains("ambiguous column name: id"));
    }

    #[test]
    fn qualified_names_use_alias_and_report_missing_columns() {
        let items = source(
            0,
            "items",
            Some("chosen"),
            vec![column("value", false)],
            false,
        );
        let mut scope = Scope::default();
        scope.add_source(&items, true);

        let resolved = scope
            .resolve_qualified("CHOSEN", "VALUE")
            .expect("source namespace is valid")
            .expect("alias resolves");
        expect_column(resolved, items.id, 0);

        let error = scope
            .resolve_qualified("chosen", "missing")
            .expect_err("matched qualifier owns no such column");
        assert!(error.to_string().contains("no such column: chosen.missing"));
    }

    #[test]
    fn duplicate_qualified_columns_are_ambiguous() {
        let left = source(
            0,
            "left_items",
            Some("chosen"),
            vec![column("id", false)],
            false,
        );
        let right = source(
            1,
            "right_items",
            Some("chosen"),
            vec![column("id", false)],
            false,
        );
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);

        let error = scope
            .resolve_qualified("chosen", "id")
            .expect_err("duplicate qualified columns are ambiguous");

        assert!(error
            .to_string()
            .contains("ambiguous column name: chosen.id"));
    }

    #[test]
    fn local_scope_wins_before_outer_scope() {
        let outer_items = source(0, "outer_items", None, vec![column("id", false)], false);
        let mut outer = Scope::default();
        outer.add_source(&outer_items, true);

        let inner_items = source(1, "inner_items", None, vec![column("id", false)], false);
        let mut inner = Scope::new(Some(outer));
        inner.add_source(&inner_items, true);

        let resolved = inner
            .resolve_unqualified("id", NamePrecedence::SourcesOnly)
            .expect("source namespace is valid")
            .expect("local column resolves");

        expect_column(resolved, inner_items.id, 0);
    }

    #[test]
    fn missing_local_qualifier_falls_back_to_outer_scope() {
        let outer_items = source(
            0,
            "outer_items",
            Some("parent"),
            vec![column("id", false)],
            false,
        );
        let mut outer = Scope::default();
        outer.add_source(&outer_items, true);
        let inner = Scope::new(Some(outer));

        let resolved = inner
            .resolve_qualified("parent", "id")
            .expect("source namespace is valid")
            .expect("outer qualified column resolves");

        expect_column(resolved, outer_items.id, 0);
    }

    #[test]
    fn matched_local_qualifier_blocks_outer_fallback() {
        let outer_items = source(
            0,
            "outer_items",
            Some("chosen"),
            vec![column("id", false)],
            false,
        );
        let mut outer = Scope::default();
        outer.add_source(&outer_items, true);

        let inner_items = source(
            1,
            "inner_items",
            Some("chosen"),
            vec![column("other", false)],
            false,
        );
        let mut inner = Scope::new(Some(outer));
        inner.add_source(&inner_items, true);

        let error = inner
            .resolve_qualified("chosen", "id")
            .expect_err("local qualifier prevents outer-column fallback");

        assert!(error.to_string().contains("no such column: chosen.id"));
    }

    #[test]
    fn database_qualified_names_bind_columns_and_rowids() {
        let items = in_database(
            source(0, "Items", None, vec![column("Value", false)], true),
            2,
        );
        let mut scope = Scope::default();
        scope.add_source(&items, true);

        let value = scope
            .resolve_database_qualified(DatabaseId::new(2), "items", "value")
            .expect("source namespace is valid")
            .expect("database-qualified column resolves");
        expect_column(value, items.id, 0);

        let rowid = scope
            .resolve_database_qualified(DatabaseId::new(2), "ITEMS", "_RoWiD_")
            .expect("source namespace is valid")
            .expect("database-qualified rowid resolves");
        assert!(matches!(rowid.expr, Expr::RowId(source) if source == items.id));

        let wrong_database = scope
            .resolve_database_qualified(DatabaseId::new(1), "items", "value")
            .expect("source namespace is valid");
        assert!(wrong_database.is_none());
    }

    #[test]
    fn alias_hides_database_qualified_table_name() {
        let items = in_database(
            source(0, "items", Some("chosen"), vec![column("id", false)], false),
            0,
        );
        let mut scope = Scope::default();
        scope.add_source(&items, true);

        let database_qualified = scope
            .resolve_database_qualified(DatabaseId::new(0), "items", "id")
            .expect("source namespace is valid");
        assert!(database_qualified.is_none());

        let aliased = scope
            .resolve_qualified("chosen", "id")
            .expect("source namespace is valid")
            .expect("alias resolves");
        expect_column(aliased, items.id, 0);
    }

    #[test]
    fn duplicate_database_qualified_columns_are_ambiguous() {
        let left = in_database(
            source(0, "items", None, vec![column("id", false)], false),
            0,
        );
        let right = in_database(
            source(1, "items", None, vec![column("id", false)], false),
            0,
        );
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);

        let error = scope
            .resolve_database_qualified(DatabaseId::new(0), "items", "id")
            .expect_err("duplicate database-qualified columns are ambiguous");

        assert!(error
            .to_string()
            .contains("ambiguous column name: items.id"));
    }

    #[test]
    fn matched_local_database_table_blocks_outer_fallback() {
        let outer_items = in_database(
            source(0, "items", None, vec![column("id", false)], false),
            0,
        );
        let mut outer = Scope::default();
        outer.add_source(&outer_items, true);

        let inner_items = in_database(
            source(1, "items", None, vec![column("other", false)], false),
            0,
        );
        let mut inner = Scope::new(Some(outer));
        inner.add_source(&inner_items, true);

        let error = inner
            .resolve_database_qualified(DatabaseId::new(0), "items", "id")
            .expect_err("local database table prevents outer-column fallback");

        assert!(error.to_string().contains("no such column: items.id"));
    }

    #[test]
    fn using_exposes_one_merged_column_and_keeps_qualified_columns() {
        for value in [
            MergedColumnValue::Left,
            MergedColumnValue::Right,
            MergedColumnValue::Coalesce,
        ] {
            let left = source(0, "left", None, vec![column("id", false)], false);
            let right = source(1, "right", None, vec![column("id", false)], false);
            let left_ref = ColumnRef {
                source: left.id,
                column: 0,
            };
            let right_ref = ColumnRef {
                source: right.id,
                column: 0,
            };
            let using = using_column(Expr::Column(left_ref), right_ref, value);
            let mut scope = Scope::default();
            scope.add_source(&left, true);
            scope.add_source(&right, true);
            scope
                .apply_using(std::slice::from_ref(&using))
                .expect("USING identities belong to visible sources");

            let unqualified = scope
                .resolve_unqualified("id", NamePrecedence::SourcesOnly)
                .expect("merged namespace is valid")
                .expect("merged column resolves");
            assert_eq!(unqualified.type_fact.storage, Some(Type::Text));
            assert_eq!(unqualified.affinity, Affinity::Text);
            assert!(unqualified.has_affinity);
            assert!(matches!(unqualified.collation, ExprCollation::Absent));
            let Expr::MergedColumn(merged) = unqualified.expr else {
                panic!("USING produces a merged column");
            };
            assert!(matches!(*merged.left, Expr::Column(actual) if actual == left_ref));
            assert_eq!(merged.right, right_ref);
            assert_eq!(merged.value, value);

            let qualified_left = scope
                .resolve_qualified("left", "id")
                .expect("source namespace is valid")
                .expect("left column remains qualified");
            expect_column(qualified_left, left.id, 0);
            let qualified_right = scope
                .resolve_qualified("right", "id")
                .expect("source namespace is valid")
                .expect("right column remains qualified");
            expect_column(qualified_right, right.id, 0);
        }
    }

    #[test]
    fn using_can_extend_an_earlier_merged_column() {
        let left = source(0, "left", None, vec![column("id", false)], false);
        let middle = source(1, "middle", None, vec![column("id", false)], false);
        let right = source(2, "right", None, vec![column("id", false)], false);
        let left_ref = ColumnRef {
            source: left.id,
            column: 0,
        };
        let middle_ref = ColumnRef {
            source: middle.id,
            column: 0,
        };
        let right_ref = ColumnRef {
            source: right.id,
            column: 0,
        };
        let first = using_column(Expr::Column(left_ref), middle_ref, MergedColumnValue::Left);
        let first_expr = Expr::MergedColumn(hir::MergedColumn {
            left: first.left.clone(),
            right: first.right,
            value: first.value,
            type_fact: first.type_fact.clone(),
            affinity: first.affinity,
            has_affinity: first.has_affinity,
            collation: first.collation.clone(),
        });
        let second = using_column(first_expr, right_ref, MergedColumnValue::Coalesce);
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&middle, true);
        scope
            .apply_using(std::slice::from_ref(&first))
            .expect("first USING identities are valid");
        scope.add_source(&right, true);
        scope
            .apply_using(std::slice::from_ref(&second))
            .expect("earlier merged column remains a valid left identity");

        let resolved = scope
            .resolve_unqualified("id", NamePrecedence::SourcesOnly)
            .expect("merged namespace is valid")
            .expect("three-way merged column resolves");
        let Expr::MergedColumn(merged) = resolved.expr else {
            panic!("second USING produces a merged column");
        };
        assert_eq!(merged.right, right_ref);
        assert_eq!(merged.value, MergedColumnValue::Coalesce);
        assert!(matches!(*merged.left, Expr::MergedColumn(_)));
    }

    #[test]
    fn natural_columns_ignore_hidden_columns() {
        let left = source(
            0,
            "left",
            None,
            vec![
                column("ID", false),
                column("left_hidden", true),
                column("right_hidden", false),
            ],
            false,
        );
        let right = source(
            1,
            "right",
            None,
            vec![
                column("id", false),
                column("left_hidden", false),
                column("right_hidden", true),
            ],
            false,
        );
        let mut scope = Scope::default();
        scope.add_source(&left, true);

        assert_eq!(scope.natural_common_columns(&right), vec!["ID"]);
        let resolved = scope
            .resolve_natural_left("id")
            .expect("visible NATURAL column resolves");
        expect_column(resolved, left.id, 0);
    }

    #[test]
    fn natural_join_rejects_ambiguous_left_column() {
        let first = source(0, "first", None, vec![column("id", false)], false);
        let second = source(1, "second", None, vec![column("id", false)], false);
        let mut scope = Scope::default();
        scope.add_source(&first, true);
        scope.add_source(&second, true);

        let error = scope
            .resolve_natural_left("id")
            .expect_err("NATURAL left column must be unique");

        assert!(error.to_string().contains("ambiguous column name: id"));
    }

    #[test]
    fn using_rejects_source_identities_outside_visible_sides() {
        let left = source(0, "left", None, vec![column("id", false)], false);
        let right = source(1, "right", None, vec![column("id", false)], false);
        let using = using_column(
            Expr::Column(ColumnRef {
                source: left.id,
                column: 0,
            }),
            ColumnRef {
                source: SourceId::new(99),
                column: 0,
            },
            MergedColumnValue::Left,
        );
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);

        let error = scope
            .apply_using(std::slice::from_ref(&using))
            .expect_err("unknown right identity violates scope invariant");

        assert!(error
            .to_string()
            .contains("USING column id did not belong to the right source"));
    }

    #[test]
    fn using_reports_missing_left_column() {
        let scope = Scope::default();

        let error = scope
            .resolve_using_left("missing")
            .expect_err("USING requires a visible left column");

        assert!(error
            .to_string()
            .contains("cannot join using column missing"));
    }

    #[test]
    fn star_preserves_order_filters_hidden_and_uses_merged_columns() {
        let left = source(
            0,
            "left",
            None,
            vec![
                column("left_value", false),
                column("id", false),
                column("left_hidden", true),
            ],
            false,
        );
        let right = source(
            1,
            "right",
            None,
            vec![
                column("id", false),
                column("right_value", false),
                column("right_hidden", true),
            ],
            false,
        );
        let using = using_column(
            Expr::Column(ColumnRef {
                source: left.id,
                column: 1,
            }),
            ColumnRef {
                source: right.id,
                column: 0,
            },
            MergedColumnValue::Left,
        );
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);
        scope
            .apply_using(std::slice::from_ref(&using))
            .expect("USING identities belong to visible sources");

        let expanded = scope.expand_star().expect("star namespace is valid");

        assert_eq!(
            expanded
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["left_value", "id", "right_value"]
        );
        assert!(matches!(&expanded[1].resolved.expr, Expr::MergedColumn(_)));
        assert_eq!(expanded[1].resolved.type_fact.storage, Some(Type::Text));
        assert_eq!(expanded[1].resolved.affinity, Affinity::Text);
        assert!(expanded[1].resolved.has_affinity);
        assert!(matches!(
            expanded[1].resolved.collation,
            ExprCollation::Absent
        ));

        let qualified = scope
            .expand_table_star("right")
            .expect("qualified star namespace is valid");
        assert_eq!(
            qualified
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "right_value"]
        );
        assert!(matches!(
            &qualified[0].resolved.expr,
            Expr::Column(ColumnRef { source, column: 0 }) if *source == right.id
        ));
    }

    #[test]
    fn unqualified_star_allows_same_table_name_from_different_databases() {
        let main_items = in_database(
            source(0, "items", None, vec![column("id", false)], false),
            0,
        );
        let attached_items = in_database(
            source(1, "items", None, vec![column("id", false)], false),
            2,
        );
        let mut scope = Scope::default();
        scope.add_source(&main_items, true);
        scope.add_source(&attached_items, true);

        let expanded = scope
            .expand_star()
            .expect("database identity distinguishes repeated table names");

        assert_eq!(expanded.len(), 2);
    }

    #[test]
    fn unqualified_star_rejects_repeated_identity_with_visible_columns() {
        let left = in_database(
            source(0, "items", None, vec![column("left", false)], false),
            0,
        );
        let right = in_database(
            source(1, "items", None, vec![column("right", false)], false),
            0,
        );
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);

        let error = scope
            .expand_star()
            .expect_err("same database and qualifier make star ambiguous");

        assert!(error
            .to_string()
            .contains("ambiguous column name: items.right"));
    }

    #[test]
    fn fully_merged_self_join_has_one_unqualified_star_column() {
        let left = in_database(
            source(0, "items", None, vec![column("id", false)], false),
            0,
        );
        let right = in_database(
            source(1, "items", None, vec![column("id", false)], false),
            0,
        );
        let using = using_column(
            Expr::Column(ColumnRef {
                source: left.id,
                column: 0,
            }),
            ColumnRef {
                source: right.id,
                column: 0,
            },
            MergedColumnValue::Left,
        );
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);
        scope
            .apply_using(std::slice::from_ref(&using))
            .expect("USING identities belong to self-join sources");

        let expanded = scope
            .expand_star()
            .expect("fully merged self-join has one visible source identity");

        assert_eq!(expanded.len(), 1);
        assert_eq!(expanded[0].name, "id");
        assert!(matches!(&expanded[0].resolved.expr, Expr::MergedColumn(_)));
    }

    #[test]
    fn table_star_reports_missing_and_repeated_qualifiers() {
        let left = source(0, "items", Some("chosen"), vec![column("id", false)], false);
        let right = source(1, "other", Some("chosen"), vec![column("id", false)], false);
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);

        let missing = scope
            .expand_table_star("missing")
            .expect_err("unknown qualifier cannot expand star");
        assert!(missing.to_string().contains("no such table: missing"));

        let ambiguous = scope
            .expand_table_star("chosen")
            .expect_err("repeated qualifier cannot expand table star");
        assert!(ambiguous
            .to_string()
            .contains("ambiguous table name: chosen"));
    }

    #[test]
    fn source_then_output_prefers_source_and_uses_alias_as_fallback() {
        let items = source(0, "items", None, vec![column("name", false)], false);
        let outputs = [
            output(0, "name", OutputNameKind::ExplicitAlias),
            output(1, "alias_only", OutputNameKind::ExplicitAlias),
        ];
        let mut scope = Scope::default();
        scope.add_source(&items, true);
        scope.set_outputs(&outputs);

        let source_result = scope
            .resolve_unqualified("name", NamePrecedence::SourceThenOutput)
            .expect("source namespace is valid")
            .expect("source column resolves");
        expect_column(source_result, items.id, 0);

        let output_result = scope
            .resolve_unqualified("alias_only", NamePrecedence::SourceThenOutput)
            .expect("output namespace is valid")
            .expect("output alias resolves as fallback");
        expect_output(output_result, outputs[1].id);
    }

    #[test]
    fn output_then_source_prefers_alias() {
        let items = source(0, "items", None, vec![column("name", false)], false);
        let outputs = [output(0, "name", OutputNameKind::ExplicitAlias)];
        let mut scope = Scope::default();
        scope.add_source(&items, true);
        scope.set_outputs(&outputs);

        let resolved = scope
            .resolve_unqualified("name", NamePrecedence::OutputThenSource)
            .expect("source namespace is valid")
            .expect("output alias resolves first");

        expect_output(resolved, outputs[0].id);
    }

    #[test]
    fn output_first_lookup_still_rejects_ambiguous_sources() {
        let left = source(0, "left", None, vec![column("name", false)], false);
        let right = source(1, "right", None, vec![column("name", false)], false);
        let outputs = [output(0, "name", OutputNameKind::ExplicitAlias)];
        let mut scope = Scope::default();
        scope.add_source(&left, true);
        scope.add_source(&right, true);
        scope.set_outputs(&outputs);

        let error = scope
            .resolve_unqualified("name", NamePrecedence::OutputThenSource)
            .expect_err("ambiguous sources remain an error");

        assert!(error.to_string().contains("ambiguous column name: name"));
    }

    #[test]
    fn explicit_alias_wins_over_inferred_output_name() {
        let outputs = [
            output(0, "chosen", OutputNameKind::Inferred),
            output(1, "chosen", OutputNameKind::ExplicitAlias),
        ];
        let mut scope = Scope::default();
        scope.set_outputs(&outputs);

        let resolved = scope
            .resolve_unqualified("chosen", NamePrecedence::OutputThenSource)
            .expect("output namespace is valid")
            .expect("matching output resolves");

        expect_output(resolved, outputs[1].id);
    }

    #[test]
    fn source_only_lookup_ignores_output_names() {
        let outputs = [output(0, "chosen", OutputNameKind::ExplicitAlias)];
        let mut scope = Scope::default();
        scope.set_outputs(&outputs);

        let resolved = scope
            .resolve_unqualified("chosen", NamePrecedence::SourcesOnly)
            .expect("source namespace is valid");

        assert!(resolved.is_none());
    }

    #[test]
    fn output_ordinals_are_one_based_and_keep_output_facts() {
        let outputs = [
            output(0, "first", OutputNameKind::Inferred),
            output(1, "second", OutputNameKind::Inferred),
        ];
        let mut scope = Scope::default();
        scope.set_outputs(&outputs);

        let resolved = scope
            .resolve_output_ordinal(2, "ORDER BY")
            .expect("second output ordinal resolves");
        expect_output(resolved, outputs[1].id);

        for ordinal in [0, 3] {
            let error = scope
                .resolve_output_ordinal(ordinal, "ORDER BY")
                .expect_err("out-of-range ordinal is rejected");
            assert_eq!(
                error.to_string(),
                "Parse error: ORDER BY term out of range - should be between 1 and 2"
            );
        }
    }

    #[test]
    fn output_metadata_is_available_from_nested_scopes() {
        let mut output = output(0, "value", OutputNameKind::Inferred);
        output.expr = Expr::Literal(Literal::Numeric("7".into()));
        output.type_fact = TypeFact::known(Type::Real);
        output.affinity = Affinity::Real;
        output.has_affinity = false;
        let output_id = output.id;
        let mut outer = Scope::default();
        outer.set_outputs(&[output]);
        let scope = Scope::new(Some(outer));

        assert_eq!(
            scope.output_type(output_id).unwrap().storage,
            Some(Type::Real)
        );
        assert_eq!(scope.output_affinity(output_id), Some(Affinity::Real));
        assert_eq!(scope.output_has_affinity(output_id), Some(false));
        assert!(matches!(
            scope.output_expr(output_id),
            Some(Expr::Literal(Literal::Numeric(value))) if value == "7"
        ));
        assert!(matches!(
            scope.output_collation(output_id),
            Some(ExprCollation::Absent)
        ));
        assert!(scope
            .output_type(OutputId::query(QueryBlockId::new(QueryId::new(9), 0), 0))
            .is_none());
    }

    #[test]
    fn output_collation_distinguishes_explicit_and_inherited_values() {
        let mut explicit = output(0, "explicit", OutputNameKind::Inferred);
        explicit.collation = Some(collation(10));
        explicit.collation_is_explicit = true;
        let mut inherited = output(1, "inherited", OutputNameKind::Inferred);
        inherited.collation = Some(collation(11));
        let explicit_id = explicit.id;
        let inherited_id = inherited.id;
        let mut scope = Scope::default();
        scope.set_outputs(&[explicit, inherited]);

        assert!(matches!(
            scope.output_collation(explicit_id),
            Some(ExprCollation::Explicit(value)) if value.id() == CatalogObjectId::new(10)
        ));
        assert!(matches!(
            scope.output_collation(inherited_id),
            Some(ExprCollation::Inherited(value)) if value.id() == CatalogObjectId::new(11)
        ));
    }

    #[test]
    fn declared_rowid_name_shadows_implicit_rowid() {
        let items = source(0, "items", None, vec![column("rowid", false)], true);
        let mut scope = Scope::default();
        scope.add_source(&items, true);

        let declared = scope
            .resolve_unqualified("rowid", NamePrecedence::SourcesOnly)
            .expect("source namespace is valid")
            .expect("declared rowid column resolves");
        expect_column(declared, items.id, 0);

        let implicit = scope
            .resolve_unqualified("oid", NamePrecedence::SourcesOnly)
            .expect("source namespace is valid")
            .expect("implicit rowid alias resolves");
        assert!(matches!(implicit.expr, Expr::RowId(source) if source == items.id));
        assert_eq!(implicit.type_fact.storage, Some(Type::Integer));
        assert_eq!(implicit.affinity, Affinity::Integer);
        assert!(implicit.has_affinity);
        assert!(matches!(implicit.collation, ExprCollation::Absent));
    }
}
