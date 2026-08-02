//! Query-local name visibility.

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use super::{
    cte_bindings::CteBindings,
    hir::{self, ColumnRef, OutputId, SourceId, TypeFact},
};
use crate::vdbe::affinity::Affinity;
use crate::{sync::Arc, Result};

/// Which namespace wins when an unqualified name is both a source column and
/// a result-column alias.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NamePrecedence {
    SourcesOnly,
    SourceThenOutput,
    OutputThenSource,
}

/// Visibility of one trigger/DML pseudo-table name.
#[derive(Clone, Debug, Default)]
pub(crate) enum PseudoSourceVisibility {
    #[default]
    Hidden,
    Visible(SourceId),
    Forbidden(&'static str),
}

/// Pseudo-tables inherited by nested queries in a statement.
#[derive(Clone, Debug, Default)]
pub(crate) struct PseudoSources {
    new: PseudoSourceVisibility,
    old: PseudoSourceVisibility,
    excluded: PseudoSourceVisibility,
}

impl PseudoSources {
    pub(crate) fn set_visible(&mut self, kind: hir::PseudoSource, source: SourceId) {
        *self.state_mut(kind) = PseudoSourceVisibility::Visible(source);
    }

    pub(crate) fn set_forbidden(&mut self, kind: hir::PseudoSource, message: &'static str) {
        *self.state_mut(kind) = PseudoSourceVisibility::Forbidden(message);
    }

    pub(crate) fn state(&self, kind: hir::PseudoSource) -> &PseudoSourceVisibility {
        match kind {
            hir::PseudoSource::New => &self.new,
            hir::PseudoSource::Old => &self.old,
            hir::PseudoSource::Excluded => &self.excluded,
        }
    }

    fn state_mut(&mut self, kind: hir::PseudoSource) -> &mut PseudoSourceVisibility {
        match kind {
            hir::PseudoSource::New => &mut self.new,
            hir::PseudoSource::Old => &mut self.old,
            hir::PseudoSource::Excluded => &mut self.excluded,
        }
    }
}

/// Inputs inherited by a query from its containing statement or query.
#[derive(Clone, Debug, Default)]
pub(crate) struct QueryEnvironment {
    pub(crate) outer: Option<Scope>,
    pub(crate) pseudo_sources: PseudoSources,
    pub(crate) ctes: CteBindings,
    /// Destination custom types for this query's outputs. Nested subqueries
    /// start without these facts unless their own caller supplies them.
    pub(crate) expected_output_types: Vec<Option<hir::ResolvedType>>,
    /// Resolved INSERT defaults available to top-level VALUES expressions.
    pub(crate) expected_defaults: Vec<Option<hir::Expr>>,
    /// RAISE() remains legal throughout a trigger program, including nested queries.
    pub(crate) allow_raise: bool,
}

impl QueryEnvironment {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn for_subquery(scope: &Scope) -> Self {
        Self {
            outer: Some(scope.clone()),
            pseudo_sources: scope.pseudo_sources().clone(),
            ctes: scope.ctes().clone(),
            expected_output_types: Vec::new(),
            expected_defaults: Vec::new(),
            allow_raise: scope.allow_raise(),
        }
    }

    pub(crate) fn with_visible_pseudo_source(
        mut self,
        kind: hir::PseudoSource,
        source: SourceId,
    ) -> Self {
        self.pseudo_sources.set_visible(kind, source);
        self
    }

    pub(crate) fn with_forbidden_pseudo_source(
        mut self,
        kind: hir::PseudoSource,
        message: &'static str,
    ) -> Self {
        self.pseudo_sources.set_forbidden(kind, message);
        self
    }

    pub(crate) fn with_expected_output_types(
        mut self,
        expected: Vec<Option<hir::ResolvedType>>,
    ) -> Self {
        self.expected_output_types = expected;
        self
    }

    pub(crate) fn expected_output_types(&self) -> &[Option<hir::ResolvedType>] {
        &self.expected_output_types
    }

    pub(crate) fn with_expected_defaults(mut self, defaults: Vec<Option<hir::Expr>>) -> Self {
        self.expected_defaults = defaults;
        self
    }

    pub(crate) fn expected_defaults(&self) -> &[Option<hir::Expr>] {
        &self.expected_defaults
    }

    pub(crate) fn within_trigger_program(mut self) -> Self {
        self.allow_raise = true;
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedScopeExpr {
    pub(crate) expr: hir::Expr,
    pub(crate) type_fact: TypeFact,
    pub(crate) affinity: Affinity,
    pub(crate) has_affinity: bool,
    pub(crate) collation: Option<hir::ResolvedCollation>,
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
            collation: self.collation.clone(),
        }
    }
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
}

/// One query block's source and output namespaces, plus enclosing blocks.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scope {
    sources: Vec<ScopeSource>,
    visible_columns: Vec<ScopeColumn>,
    outputs: Vec<ScopeOutput>,
    pseudo_sources: PseudoSources,
    forbidden_qualifiers: HashMap<String, &'static str>,
    pruned_outer_qualifiers: HashSet<String>,
    missing_qualified_name_is_column: bool,
    named_windows: HashMap<String, hir::WindowSpec>,
    ctes: CteBindings,
    outer: Option<Arc<Scope>>,
    allow_raise: bool,
}

impl Scope {
    pub(crate) fn new(outer: Option<Scope>) -> Self {
        Self {
            outer: outer.map(Arc::new),
            ..Self::default()
        }
    }

    pub(crate) fn outer(&self) -> Option<&Scope> {
        self.outer.as_deref()
    }

    /// Clone this query block's namespace without any enclosing query blocks.
    pub(crate) fn without_outer(&self) -> Self {
        let mut scope = self.clone();
        let mut outer = self.outer.as_deref();
        while let Some(pruned) = outer {
            scope
                .pruned_outer_qualifiers
                .extend(pruned.sources.iter().map(|source| source.qualifier.clone()));
            scope
                .pruned_outer_qualifiers
                .extend(pruned.pruned_outer_qualifiers.iter().cloned());
            outer = pruned.outer.as_deref();
        }
        scope.outer = None;
        scope
    }

    pub(crate) fn set_ctes(&mut self, ctes: CteBindings) {
        self.ctes = ctes;
    }

    pub(crate) fn ctes(&self) -> &CteBindings {
        &self.ctes
    }

    pub(crate) fn set_pseudo_sources(&mut self, pseudo_sources: PseudoSources) {
        self.pseudo_sources = pseudo_sources;
    }

    pub(crate) fn pseudo_sources(&self) -> &PseudoSources {
        &self.pseudo_sources
    }

    pub(crate) fn set_allow_raise(&mut self, allow: bool) {
        self.allow_raise = allow;
    }

    pub(crate) fn allow_raise(&self) -> bool {
        self.allow_raise
    }

    pub(crate) fn insert_window(&mut self, name: &str, window: hir::WindowSpec) {
        let name = crate::util::normalize_ident(name);
        self.named_windows.entry(name).or_insert(window);
    }

    pub(crate) fn window(&self, name: &str) -> Option<&hir::WindowSpec> {
        self.named_windows.get(&crate::util::normalize_ident(name))
    }

    pub(crate) fn add_source(&mut self, source: &hir::Source, unqualified: bool) {
        self.add_source_with_qualifier(
            source,
            source.alias.as_deref().unwrap_or(source.name.as_str()),
            unqualified,
        );
    }

    /// Add an existing source under a schema-owned qualifier. Index-method
    /// patterns use the table name written by the method, not the alias of the
    /// query occurrence the pattern may optimize.
    pub(crate) fn add_source_with_qualifier(
        &mut self,
        source: &hir::Source,
        qualifier: &str,
        unqualified: bool,
    ) {
        let qualifier = crate::util::normalize_ident(qualifier);
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
            qualifier,
            table_name: crate::util::normalize_ident(&source.name),
            database: source.database,
            database_qualified: source.alias.is_none(),
            columns,
            rowid_available: source.rowid_available,
            unqualified,
        });
    }

    pub(crate) fn forbid_qualifier(&mut self, qualifier: &str, message: &'static str) {
        self.forbidden_qualifiers
            .insert(crate::util::normalize_ident(qualifier), message);
    }

    pub(crate) fn report_missing_qualified_name_as_column(&mut self) {
        self.missing_qualified_name_is_column = true;
    }

    pub(crate) fn missing_qualified_name_is_column(&self) -> bool {
        self.missing_qualified_name_is_column
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
            })
            .collect();
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
                    // SQLite still reports an ambiguous source name even when
                    // a result alias wins ORDER BY/HAVING precedence.
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
        self.outer.as_deref().map_or(Ok(None), |outer| {
            // A correlated subquery can refer to an output alias of the
            // query block that owns it. Source columns still win in every
            // enclosing block, matching SQLite's alias rules for WHERE,
            // GROUP BY, HAVING, and ORDER BY subqueries.
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
        if !matching_sources.is_empty() {
            let mut found = None;
            for source in matching_sources {
                let resolved = source
                    .columns
                    .iter()
                    .find(|candidate| candidate.lookup_name == normalized_column)
                    .map(ScopeColumn::resolved)
                    .or_else(|| {
                        (source.rowid_available && is_rowid_name(&normalized_column)).then(|| {
                            ResolvedScopeExpr {
                                expr: hir::Expr::RowId(source.id),
                                type_fact: TypeFact::known(crate::schema::Type::Integer),
                                affinity: Affinity::Integer,
                                has_affinity: true,
                                collation: None,
                            }
                        })
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
            return Ok(Some(found));
        }

        if let Some(message) = self.forbidden_qualifiers.get(&normalized_qualifier) {
            return Err(crate::LimboError::ParseError((*message).to_string()));
        }
        if self.pruned_outer_qualifiers.contains(&normalized_qualifier) {
            crate::bail_parse_error!("no such column: {}.{}", qualifier, column);
        }
        if let Some(outer) = self.outer.as_deref() {
            if let Some(resolved) = outer.resolve_qualified(qualifier, column)? {
                return Ok(Some(resolved));
            }
        }
        // A WITH name is a table candidate, but it does not enter the query's
        // column namespace until it appears in FROM. Check enclosing range
        // variables first: nested queries inherit the CTE binding as well as
        // the outer source, and the source is the only one that owns columns.
        if self.ctes.find(&normalized_qualifier).is_some() {
            crate::bail_parse_error!("no such column: {}.{}", qualifier, column);
        }
        Ok(None)
    }

    pub(crate) fn resolve_database_qualified(
        &self,
        database: hir::DatabaseId,
        table: &str,
        column: &str,
    ) -> Result<Option<ResolvedScopeExpr>> {
        let table = crate::util::normalize_ident(table);
        let column_lookup = crate::util::normalize_ident(column);
        let matching: Vec<_> = self
            .sources
            .iter()
            .filter(|source| {
                source.database_qualified
                    && source.database == Some(database)
                    && source.table_name == table
            })
            .collect();
        if !matching.is_empty() {
            let mut found = None;
            for source in matching {
                let resolved = source
                    .columns
                    .iter()
                    .find(|candidate| candidate.lookup_name == column_lookup)
                    .map(ScopeColumn::resolved)
                    .or_else(|| {
                        (source.rowid_available && is_rowid_name(&column_lookup)).then(|| {
                            ResolvedScopeExpr {
                                expr: hir::Expr::RowId(source.id),
                                type_fact: TypeFact::known(crate::schema::Type::Integer),
                                affinity: Affinity::Integer,
                                has_affinity: true,
                                collation: None,
                            }
                        })
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
            return Ok(Some(found));
        }
        self.outer.as_deref().map_or(Ok(None), |outer| {
            outer.resolve_database_qualified(database, &table, column)
        })
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
        Ok(ResolvedScopeExpr {
            expr: hir::Expr::Output(output.id),
            type_fact: output.type_fact.clone(),
            affinity: output.affinity,
            has_affinity: output.has_affinity,
            collation: output.collation.clone(),
        })
    }

    pub(crate) fn output_type(&self, id: OutputId) -> Option<&TypeFact> {
        self.outputs
            .iter()
            .find(|output| output.id == id)
            .map(|output| &output.type_fact)
            .or_else(|| self.outer.as_deref()?.output_type(id))
    }

    pub(crate) fn output_affinity(&self, id: OutputId) -> Option<Affinity> {
        self.outputs
            .iter()
            .find(|output| output.id == id)
            .map(|output| output.affinity)
            .or_else(|| self.outer.as_deref()?.output_affinity(id))
    }

    pub(crate) fn output_has_affinity(&self, id: OutputId) -> Option<bool> {
        self.outputs
            .iter()
            .find(|output| output.id == id)
            .map(|output| output.has_affinity)
            .or_else(|| self.outer.as_deref()?.output_has_affinity(id))
    }

    pub(crate) fn output_collation(&self, id: OutputId) -> Option<Option<&hir::ResolvedCollation>> {
        self.outputs
            .iter()
            .find(|output| output.id == id)
            .map(|output| output.collation.as_ref())
            .or_else(|| self.outer.as_deref()?.output_collation(id))
    }

    pub(crate) fn output_expr(&self, id: OutputId) -> Option<&hir::Expr> {
        self.outputs
            .iter()
            .find(|output| output.id == id)
            .map(|output| &output.expr)
            .or_else(|| self.outer.as_deref()?.output_expr(id))
    }

    pub(crate) fn expand_star(
        &self,
    ) -> Result<
        Vec<(
            String,
            hir::Expr,
            TypeFact,
            Affinity,
            bool,
            Option<hir::ResolvedCollation>,
        )>,
    > {
        // SQLite permits the same unaliased table name across different
        // databases, but a repeated database/qualifier pair is ambiguous as
        // soon as both occurrences contribute a column to `*`. USING/NATURAL
        // joins remove one of the duplicate columns from `visible_columns`, so
        // a fully merged self-join remains valid while any remaining column on
        // both sides reports the ambiguity.
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
            .map(|column| {
                (
                    column.display_name.clone(),
                    column.expr.clone(),
                    column.type_fact.clone(),
                    column.affinity,
                    column.has_affinity,
                    column.collation.clone(),
                )
            })
            .collect())
    }

    pub(crate) fn expand_table_star(
        &self,
        qualifier: &str,
    ) -> Result<
        Vec<(
            String,
            hir::Expr,
            TypeFact,
            Affinity,
            bool,
            Option<hir::ResolvedCollation>,
        )>,
    > {
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
            .map(|column| {
                (
                    column.display_name.clone(),
                    column.expr.clone(),
                    column.type_fact.clone(),
                    column.affinity,
                    column.has_affinity,
                    column.collation.clone(),
                )
            })
            .collect())
    }

    pub(crate) fn resolve_using_left(&self, name: &str) -> Result<ResolvedScopeExpr> {
        let normalized = crate::util::normalize_ident(name);
        self.resolve_source_column(&normalized)?.ok_or_else(|| {
            crate::LimboError::ParseError(format!(
                "cannot join using column {name} - column not present in both tables"
            ))
        })
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
            let mut left_positions = self
                .visible_columns
                .iter()
                .enumerate()
                .filter(|(position, column)| {
                    *position != right_position && column.lookup_name == lookup_name
                })
                .map(|(position, _)| position);
            let Some(left_position) = left_positions.next() else {
                return Err(crate::LimboError::InternalError(format!(
                    "USING column {} did not belong to the left sources",
                    using.name
                )));
            };
            if left_positions.next().is_some() {
                crate::bail_parse_error!("ambiguous reference to {} in USING()", using.name);
            }

            let merged = hir::Expr::MergedColumn(hir::MergedColumn {
                left: using.left.clone(),
                right: using.right,
                value: using.value,
                type_fact: using.type_fact.clone(),
                affinity: using.affinity,
                has_affinity: using.has_affinity,
                collation: using.collation.clone(),
            });

            match using.value {
                hir::MergedColumnValue::Left => {
                    self.visible_columns[left_position].expr = merged;
                    self.visible_columns[left_position].type_fact = using.type_fact.clone();
                    self.visible_columns[left_position].affinity = using.affinity;
                    self.visible_columns[left_position].has_affinity = using.has_affinity;
                    self.visible_columns[left_position].collation = using.collation.clone();
                    self.visible_columns.remove(right_position);
                }
                hir::MergedColumnValue::Right => {
                    self.visible_columns[right_position].expr = merged;
                    self.visible_columns[right_position].type_fact = using.type_fact.clone();
                    self.visible_columns[right_position].affinity = using.affinity;
                    self.visible_columns[right_position].has_affinity = using.has_affinity;
                    self.visible_columns[right_position].collation = using.collation.clone();
                    self.visible_columns.remove(left_position);
                }
                hir::MergedColumnValue::Coalesce => {
                    self.visible_columns[left_position].expr = merged;
                    self.visible_columns[left_position].type_fact = using.type_fact.clone();
                    self.visible_columns[left_position].affinity = using.affinity;
                    self.visible_columns[left_position].has_affinity = using.has_affinity;
                    self.visible_columns[left_position].collation = using.collation.clone();
                    self.visible_columns.remove(right_position);
                }
            }
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

        let mut rowid = None;
        if is_rowid_name(name) {
            for source in self
                .sources
                .iter()
                .filter(|source| source.unqualified && source.rowid_available)
            {
                if rowid.is_some() {
                    crate::bail_parse_error!("ambiguous column name: {}", name);
                }
                rowid = Some(ResolvedScopeExpr {
                    expr: hir::Expr::RowId(source.id),
                    type_fact: TypeFact::known(crate::schema::Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                });
            }
        }
        Ok(rowid)
    }

    fn resolve_output(&self, name: &str) -> Option<ResolvedScopeExpr> {
        self.outputs
            .iter()
            .find(|output| {
                output.name_kind == hir::OutputNameKind::ExplicitAlias && output.name == name
            })
            .or_else(|| self.outputs.iter().find(|output| output.name == name))
            .map(|output| ResolvedScopeExpr {
                expr: hir::Expr::Output(output.id),
                type_fact: output.type_fact.clone(),
                affinity: output.affinity,
                has_affinity: output.has_affinity,
                collation: output.collation.clone(),
            })
    }
}

fn is_rowid_name(name: &str) -> bool {
    matches!(name, "rowid" | "_rowid_" | "oid")
}
