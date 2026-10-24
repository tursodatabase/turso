//! Query-local source-name visibility.

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use std::borrow::Cow;

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
    /// A column that declares no collation. It compares as BINARY, and as
    /// a comparison operand it takes precedence over the other side's
    /// column collation, as any column does. The value itself carries no
    /// collation.
    InheritedBinary(hir::ResolvedCollation),
}

impl ExprCollation {
    pub(crate) fn inherited(collation: Option<hir::ResolvedCollation>) -> Self {
        match collation {
            Some(collation) => Self::Inherited(collation),
            None => Self::Absent,
        }
    }

    /// The collation a table column supplies: its declared one, or `binary`.
    pub(crate) fn column(
        collation: Option<hir::ResolvedCollation>,
        binary: &hir::ResolvedCollation,
    ) -> Self {
        match collation {
            Some(collation) => Self::Inherited(collation),
            None => Self::InheritedBinary(binary.clone()),
        }
    }

    /// The collation the value carries, if any.
    pub(crate) fn value(&self) -> Option<&hir::ResolvedCollation> {
        match self {
            Self::Absent | Self::InheritedBinary(_) => None,
            Self::Explicit(collation) | Self::Inherited(collation) => Some(collation),
        }
    }

    /// The collation a comparison decided by this operand uses: BINARY for
    /// a column without a declared collation, so that the planner does not
    /// take the collation of an index on the other side instead.
    pub(crate) fn comparison_value(&self) -> Option<&hir::ResolvedCollation> {
        match self {
            Self::Absent => None,
            Self::Explicit(collation)
            | Self::Inherited(collation)
            | Self::InheritedBinary(collation) => Some(collation),
        }
    }

    pub(crate) fn into_output(self) -> (Option<hir::ResolvedCollation>, bool) {
        match self {
            Self::Absent | Self::InheritedBinary(_) => (None, false),
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
    /// The source's alias, or its table name or `(subquery-N)` without one.
    pub(crate) qualifier: String,
    pub(crate) resolved: ResolvedScopeExpr,
}

#[derive(Debug)]
struct ColumnBinding {
    source: SourceId,
    display_name: Arc<str>,
    lookup_name: ColumnName,
    resolved: ResolvedScopeExpr,
    hidden: bool,
    /// The implicit rowid a parenthesized join group saved for one of its
    /// tables: a bare `rowid`, `oid` or `_rowid_` finds it.
    rowid_names: bool,
    /// The name a USING or NATURAL join compares: the column's own name,
    /// or for a column of a join group the unique name the group gave it,
    /// since SQLite looks the group's subquery up by column name.
    using_name: ColumnName,
}

#[derive(Clone, Debug)]
struct ColumnName(Arc<str>);

impl ColumnName {
    fn matches(&self, name: &str) -> bool {
        self.0.eq_ignore_ascii_case(name)
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl Clone for ColumnBinding {
    fn clone(&self) -> Self {
        #[cfg(test)]
        tests::COLUMN_BINDINGS_MADE.with(|made| made.set(made.get() + 1));
        Self {
            source: self.source,
            display_name: self.display_name.clone(),
            lookup_name: self.lookup_name.clone(),
            resolved: self.resolved.clone(),
            hidden: self.hidden,
            rowid_names: self.rowid_names,
            using_name: self.using_name.clone(),
        }
    }
}

impl ColumnBinding {
    fn from_source(source: &hir::Source, index: usize, column: &hir::SourceColumn) -> Self {
        let expr = match &source.kind {
            hir::SourceKind::FromGroup(group) => group.columns[index].clone(),
            _ => hir::Expr::Column(ColumnRef {
                source: source.id,
                column: index,
            }),
        };
        Self::new(source.id, expr, column)
    }

    fn new(source: SourceId, expr: hir::Expr, column: &hir::SourceColumn) -> Self {
        #[cfg(test)]
        tests::COLUMN_BINDINGS_MADE.with(|made| made.set(made.get() + 1));
        Self {
            source,
            display_name: column.name.clone(),
            lookup_name: ColumnName(column.name.clone()),
            resolved: ResolvedScopeExpr {
                expr,
                type_fact: column.type_fact.clone(),
                affinity: column.affinity,
                has_affinity: column.has_affinity,
                collation: ExprCollation::inherited(column.collation.clone()),
            },
            hidden: column.hidden,
            rowid_names: false,
            using_name: ColumnName(column.name.clone()),
        }
    }

    fn resolved(&self) -> ResolvedScopeExpr {
        self.resolved.clone()
    }
}

pub(crate) fn resolve_source_column(source: &hir::Source, name: &str) -> Result<ResolvedScopeExpr> {
    let normalized = crate::util::normalize_ident(name);
    source
        .columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.name.eq_ignore_ascii_case(&normalized))
        .map(|(index, column)| ColumnBinding::from_source(source, index, column).resolved())
        .ok_or_else(|| {
            crate::LimboError::ParseError(format!(
                "cannot join using column {name} - column not present in both tables"
            ))
        })
}

/// A column as the scope's names find it: a column of a source, read in
/// place, or a binding that a join group or a USING join changed.
#[derive(Clone, Copy)]
enum ColumnView<'a> {
    Source {
        source: &'a ScopeSource,
        index: usize,
    },
    Binding(&'a ColumnBinding),
}

impl<'a> ColumnView<'a> {
    fn source(self) -> SourceId {
        match self {
            Self::Source { source, .. } => source.id,
            Self::Binding(binding) => binding.source,
        }
    }

    fn display_name(self) -> &'a str {
        match self {
            Self::Source { source, index } => &source.columns[index].name,
            Self::Binding(binding) => &binding.display_name,
        }
    }

    fn lookup_name(self) -> &'a str {
        match self {
            Self::Source { source, index } => &source.columns[index].name,
            Self::Binding(binding) => binding.lookup_name.as_str(),
        }
    }

    fn lookup_matches(self, name: &str) -> bool {
        self.lookup_name().eq_ignore_ascii_case(name)
    }

    fn using_name(self) -> &'a str {
        match self {
            Self::Source { source, index } => &source.columns[index].name,
            Self::Binding(binding) => binding.using_name.as_str(),
        }
    }

    fn using_matches(self, name: &str) -> bool {
        self.using_name().eq_ignore_ascii_case(name)
    }

    fn hidden(self) -> bool {
        match self {
            Self::Source { source, index } => source.columns[index].hidden,
            Self::Binding(binding) => binding.hidden,
        }
    }

    fn rowid_names(self) -> bool {
        match self {
            Self::Source { .. } => false,
            Self::Binding(binding) => binding.rowid_names,
        }
    }

    fn expr(self) -> Cow<'a, hir::Expr> {
        match self {
            Self::Source { source, index } => match &source.group_values {
                Some(values) => Cow::Borrowed(&values[index]),
                None => Cow::Owned(hir::Expr::Column(ColumnRef {
                    source: source.id,
                    column: index,
                })),
            },
            Self::Binding(binding) => Cow::Borrowed(&binding.resolved.expr),
        }
    }

    fn resolved(self) -> ResolvedScopeExpr {
        match self {
            Self::Source { source, index } => {
                let column = &source.columns[index];
                ResolvedScopeExpr {
                    expr: self.expr().into_owned(),
                    type_fact: column.type_fact.clone(),
                    affinity: column.affinity,
                    has_affinity: column.has_affinity,
                    collation: ExprCollation::inherited(column.collation.clone()),
                }
            }
            Self::Binding(binding) => binding.resolved(),
        }
    }

    fn to_binding(self) -> ColumnBinding {
        match self {
            Self::Source { source, index } => {
                ColumnBinding::new(source.id, self.expr().into_owned(), &source.columns[index])
            }
            Self::Binding(binding) => binding.clone(),
        }
    }
}

#[derive(Clone, Debug)]
enum VisibleColumn {
    AllOfSource(usize),
    Binding(ColumnBinding),
}

#[derive(Clone, Debug)]
struct ScopeSource {
    id: SourceId,
    qualifier: String,
    table_name: String,
    database: Option<hir::DatabaseId>,
    database_qualified: bool,
    columns: Arc<[hir::SourceColumn]>,
    /// For a parenthesized join group: the value each column reads.
    group_values: Option<Vec<hir::Expr>>,
    rowid_available: bool,
    unqualified: bool,
    /// Lookup names of this source's columns that `table.*` reads as the
    /// merged USING value instead of the source's own column: a later USING
    /// merges the name and a later RIGHT or FULL JOIN can leave the source's
    /// own copy NULL. SQLite expands such a column as an unqualified name.
    star_merged: Vec<String>,
    /// For a parenthesized join group: what each column came from, and how
    /// a name can reach it. Index-aligned with `columns`.
    saved: Option<Vec<SavedGroupColumn>>,
    /// Columns of this source that the USING or NATURAL join it is the
    /// right side of merged, with how the merged value is formed. A
    /// qualified name finds the copy on the left instead, unless the join
    /// is a RIGHT JOIN.
    using_merged: Vec<(String, hir::MergedColumnValue)>,
}

impl ScopeSource {
    fn column(&self, index: usize) -> ColumnView<'_> {
        ColumnView::Source {
            source: self,
            index,
        }
    }

    fn column_views(&self) -> impl Iterator<Item = ColumnView<'_>> + '_ {
        (0..self.columns.len()).map(|index| self.column(index))
    }
}

/// How a name of the outer query can reach a column of a parenthesized
/// join group, as SQLite keeps it for the subquery it makes of the group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GroupColumnVisibility {
    /// Bare names, qualified names and `*` find the column.
    Visible,
    /// Only a qualified name finds the column: a copy a USING merged.
    QualifiedOnly,
    /// Bare and qualified names find the column, but `*` omits it: a later
    /// column whose name repeats a merged USING column's.
    HiddenFromStar,
}

/// Where a column of a parenthesized join group came from. Names are
/// normalized.
#[derive(Clone, Debug)]
pub(crate) enum SavedGroupColumnKind {
    /// The one value of a USING column, placed before the sources it merges.
    Using { name: String },
    /// A column of a source inside the group, found through that source's
    /// own name.
    Table {
        table: String,
        database: Option<hir::DatabaseId>,
        name: String,
    },
    /// The implicit rowid of a source inside the group, found through that
    /// source's name with any rowid name.
    RowId {
        table: String,
        database: Option<hir::DatabaseId>,
    },
}

impl SavedGroupColumnKind {
    fn matches_table(&self, table: &str, database: Option<hir::DatabaseId>) -> bool {
        match self {
            Self::Using { .. } => false,
            Self::Table {
                table: saved_table,
                database: saved_database,
                ..
            }
            | Self::RowId {
                table: saved_table,
                database: saved_database,
            } => {
                saved_table == table
                    && database.is_none_or(|database| *saved_database == Some(database))
            }
        }
    }

    fn matches_column(&self, name: &str) -> bool {
        match self {
            Self::Using { name: saved } | Self::Table { name: saved, .. } => saved == name,
            Self::RowId { .. } => is_rowid_name(name),
        }
    }
}

/// What a qualified name found among one scope's own sources.
enum QualifiedLookup {
    Found(Box<ResolvedScopeExpr>),
    /// A source of that name exists here, but none has the column.
    NoSuchColumn,
    /// No source of that name exists here.
    NoSuchTable,
}

#[derive(Clone, Debug)]
pub(crate) struct SavedGroupColumn {
    pub(crate) kind: SavedGroupColumnKind,
    pub(crate) visibility: GroupColumnVisibility,
}

/// One column a parenthesized join group exposes to the query around it.
pub(crate) struct GroupColumnDef {
    /// The column's name in the group, unique among the group's columns.
    pub(crate) name: String,
    pub(crate) value: ResolvedScopeExpr,
    pub(crate) saved: SavedGroupColumn,
}

#[derive(Clone, Debug)]
struct ScopeOutput {
    id: OutputId,
    name: String,
    name_kind: hir::OutputNameKind,
    type_fact: TypeFact,
    affinity: Affinity,
    has_affinity: bool,
    collation: Option<hir::ResolvedCollation>,
    collation_is_explicit: bool,
}

/// Source namespace visible from one query block.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scope {
    sources: Arc<Vec<ScopeSource>>,
    visible: Arc<Vec<VisibleColumn>>,
    outputs: Vec<ScopeOutput>,
    outer: Option<Arc<Scope>>,
    outer_resolution_blocked: bool,
    /// PostgreSQL rule: `t.c` reads the nearest source named `t`, and is an
    /// error when that source has no column `c`, even when an outer query
    /// has a `t` with one.
    qualified_uses_nearest_table_only: bool,
    /// The names of the attached databases by index, for error messages
    /// that name a column with its database.
    database_names: Vec<String>,
}

impl Scope {
    pub(crate) fn new(outer: Option<Scope>) -> Self {
        Self {
            qualified_uses_nearest_table_only: outer
                .as_ref()
                .is_some_and(|outer| outer.qualified_uses_nearest_table_only),
            outer: outer.map(Arc::new),
            ..Self::default()
        }
    }

    pub(crate) fn with_qualified_uses_nearest_table_only(mut self, enabled: bool) -> Self {
        self.qualified_uses_nearest_table_only = enabled;
        self
    }

    pub(crate) fn set_database_names(&mut self, names: Vec<String>) {
        self.database_names = names;
    }

    fn database_name(&self, database: Option<hir::DatabaseId>) -> Option<&str> {
        self.database_names
            .get(database?.index())
            .map(String::as_str)
    }

    pub(crate) fn without_outer_resolution(mut self) -> Self {
        self.outer_resolution_blocked = true;
        self
    }

    pub(crate) fn add_source(&mut self, source: &hir::Source, unqualified: bool) {
        #[cfg(test)]
        tests::SOURCES_ADDED.with(|added| added.set(added.get() + 1));
        let group_values = match &source.kind {
            hir::SourceKind::FromGroup(group) => Some(group.columns.clone()),
            _ => None,
        };
        if unqualified {
            Arc::make_mut(&mut self.visible).push(VisibleColumn::AllOfSource(self.sources.len()));
        }
        Arc::make_mut(&mut self.sources).push(ScopeSource {
            id: source.id,
            qualifier: crate::util::normalize_ident(
                source.alias.as_deref().unwrap_or(&source.name),
            ),
            table_name: crate::util::normalize_ident(&source.name),
            database: source.database,
            database_qualified: source.alias.is_none(),
            columns: source.columns.clone(),
            group_values,
            rowid_available: source.rowid_available,
            unqualified,
            star_merged: Vec::new(),
            saved: None,
            using_merged: Vec::new(),
        });
    }

    /// Add a parenthesized join group of several sources. A bare name finds
    /// a column by the name it had in the group, not by the unique name the
    /// group gave it; a column a USING merged is found through a qualified
    /// name only.
    pub(crate) fn add_group_source(&mut self, source: &hir::Source, saved: Vec<SavedGroupColumn>) {
        assert_eq!(
            source.columns.len(),
            saved.len(),
            "every column of a join group has a saved origin"
        );
        self.add_source(source, false);
        let added = Arc::make_mut(&mut self.sources)
            .last_mut()
            .expect("source was just added");
        added.unqualified = true;
        for (column, saved) in added.column_views().zip(&saved) {
            if saved.visibility == GroupColumnVisibility::QualifiedOnly {
                continue;
            }
            let mut visible = column.to_binding();
            visible.hidden = saved.visibility == GroupColumnVisibility::HiddenFromStar;
            match &saved.kind {
                SavedGroupColumnKind::Using { name } | SavedGroupColumnKind::Table { name, .. } => {
                    visible.lookup_name = ColumnName(name.as_str().into());
                }
                // `*` never lists a saved rowid.
                SavedGroupColumnKind::RowId { .. } => {
                    visible.rowid_names = true;
                    visible.hidden = true;
                }
            }
            Arc::make_mut(&mut self.visible).push(VisibleColumn::Binding(visible));
        }
        added.saved = Some(saved);
    }

    /// The columns a parenthesized join group of these sources exposes, as
    /// SQLite's `selectExpander` lists them for the subquery it makes of
    /// the group: for each source, the one value of each USING column of
    /// the next join, then the source's own columns, then its implicit
    /// rowid. Names repeated within the group get `:N` suffixes.
    pub(crate) fn saved_group_columns(&self, from: &hir::From) -> Result<Vec<GroupColumnDef>> {
        if self.sources.len() != from.joins.len() + 1 {
            return Err(crate::LimboError::InternalError(
                "join group scope does not match its FROM clause".to_string(),
            ));
        }
        let using_names = |join: Option<&hir::Join>| -> Vec<String> {
            match join.map(|join| &join.constraint) {
                Some(
                    hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns),
                ) => columns
                    .iter()
                    .map(|column| crate::util::normalize_ident(&column.name))
                    .collect(),
                Some(hir::JoinConstraint::On(_) | hir::JoinConstraint::None) | None => Vec::new(),
            }
        };
        let mut defs = Vec::new();
        for (position, source) in self.sources.iter().enumerate() {
            let next_using = using_names(from.joins.get(position));
            let own_using = using_names(position.checked_sub(1).map(|join| &from.joins[join]));
            for name in &next_using {
                // The group's subquery reads the USING column as a bare
                // name: the merged value the joins left visible. Another
                // unmerged copy makes that name ambiguous, whether or not
                // the outer query reads it.
                let Some(value) = self.resolve_source_column(name)? else {
                    return Err(crate::LimboError::InternalError(format!(
                        "USING column {name} of a join group is not visible"
                    )));
                };
                defs.push(GroupColumnDef {
                    name: name.clone(),
                    value,
                    saved: SavedGroupColumn {
                        kind: SavedGroupColumnKind::Using { name: name.clone() },
                        visibility: GroupColumnVisibility::Visible,
                    },
                });
            }
            for (index, column) in source.column_views().enumerate() {
                let mut saved = match &source.saved {
                    // A rowid entry is not copied through a second group.
                    Some(saved)
                        if matches!(saved[index].kind, SavedGroupColumnKind::RowId { .. }) =>
                    {
                        continue
                    }
                    Some(saved) => saved[index].clone(),
                    None if column.hidden() => continue,
                    None => SavedGroupColumn {
                        kind: SavedGroupColumnKind::Table {
                            table: source.qualifier.clone(),
                            database: source.database,
                            name: crate::util::normalize_ident(column.lookup_name()),
                        },
                        visibility: GroupColumnVisibility::Visible,
                    },
                };
                if own_using.iter().any(|name| column.lookup_matches(name))
                    || next_using.iter().any(|name| column.lookup_matches(name))
                {
                    saved.visibility = GroupColumnVisibility::QualifiedOnly;
                }
                defs.push(GroupColumnDef {
                    name: column.display_name().to_string(),
                    value: column.resolved(),
                    saved,
                });
            }
            if source.rowid_available && source.saved.is_none() {
                // The first rowid name that is not a declared column.
                if let Some(name) = ["_ROWID_", "ROWID", "OID"].into_iter().find(|name| {
                    !source
                        .column_views()
                        .any(|column| column.lookup_matches(name))
                }) {
                    defs.push(GroupColumnDef {
                        name: name.to_string(),
                        value: resolved_rowid(source.id),
                        saved: SavedGroupColumn {
                            kind: SavedGroupColumnKind::RowId {
                                table: source.qualifier.clone(),
                                database: source.database,
                            },
                            visibility: GroupColumnVisibility::Visible,
                        },
                    });
                }
            }
        }
        // Repeated names get `:N` suffixes. A later copy of a merged USING
        // value is found by a qualified name only; an unrelated column that
        // repeats a USING name stays findable but leaves `*`.
        for index in 0..defs.len() {
            let original = defs[index].name.clone();
            let base = match original.rfind(':') {
                Some(colon)
                    if colon > 0 && original[colon + 1..].chars().all(|c| c.is_ascii_digit()) =>
                {
                    original[..colon].to_string()
                }
                _ => original.clone(),
            };
            let mut candidate = original;
            let mut suffix = 0;
            while let Some(previous) = defs[..index]
                .iter()
                .position(|def| def.name.eq_ignore_ascii_case(&candidate))
            {
                if matches!(
                    defs[previous].saved.kind,
                    SavedGroupColumnKind::Using { .. }
                ) {
                    let current = &mut defs[index].saved;
                    if matches!(current.kind, SavedGroupColumnKind::Using { .. }) {
                        current.visibility = GroupColumnVisibility::QualifiedOnly;
                    } else if current.visibility != GroupColumnVisibility::QualifiedOnly {
                        current.visibility = GroupColumnVisibility::HiddenFromStar;
                    }
                }
                suffix += 1;
                candidate = format!("{base}:{suffix}");
            }
            defs[index].name = candidate;
        }
        Ok(defs)
    }

    /// Record the columns of `source` that `table.*` reads as merged USING
    /// values. `names` are the USING column names of the joins after the
    /// source; they apply only when a RIGHT or FULL JOIN also follows it.
    pub(crate) fn mark_star_merged_columns(&mut self, source: SourceId, names: &[String]) {
        let Some(scope_source) = Arc::make_mut(&mut self.sources)
            .iter_mut()
            .find(|entry| entry.id == source)
        else {
            return;
        };
        scope_source.star_merged = names
            .iter()
            .map(|name| crate::util::normalize_ident(name))
            .collect();
    }

    /// Add a table the way its schema text sees it: under the table's own
    /// name, whatever alias the statement gave it. Index, generated-column,
    /// default, and CHECK expressions name their table this way.
    pub(crate) fn add_schema_source(&mut self, source: &hir::Source) {
        self.add_source(source, true);
        let added = Arc::make_mut(&mut self.sources)
            .last_mut()
            .expect("source was just added");
        added.qualifier = added.table_name.clone();
        added.database_qualified = true;
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
        let first_appended_source = self.sources.len();
        Arc::make_mut(&mut self.sources).extend(Arc::unwrap_or_clone(other.sources));
        Arc::make_mut(&mut self.visible).extend(
            Arc::unwrap_or_clone(other.visible)
                .into_iter()
                .map(|visible| match visible {
                    VisibleColumn::AllOfSource(source) => {
                        VisibleColumn::AllOfSource(first_appended_source + source)
                    }
                    VisibleColumn::Binding(binding) => VisibleColumn::Binding(binding),
                }),
        );
    }

    pub(crate) fn without_outer(mut self) -> Self {
        self.outer = None;
        self
    }

    pub(crate) fn set_outer(&mut self, outer: Option<&Self>) {
        assert!(self.outer.is_none(), "scope already has an outer scope");
        self.outer = outer.cloned().map(Arc::new);
    }

    pub(crate) fn has_sources(&self) -> bool {
        !self.sources.is_empty()
    }

    pub(crate) fn set_outputs(&mut self, outputs: &[hir::Output]) {
        #[cfg(test)]
        tests::OUTPUT_LISTS_SET.with(|set| set.set(set.get() + 1));
        self.outputs = outputs
            .iter()
            .map(|output| ScopeOutput {
                id: output.id,
                name: crate::util::normalize_ident(&output.name),
                name_kind: output.name_kind,
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
        let lookup =
            self.qualified_lookup(&normalized_qualifier, None, &normalized_column, || {
                format!("{qualifier}.{column}")
            })?;
        match lookup {
            QualifiedLookup::Found(resolved) => Ok(Some(*resolved)),
            QualifiedLookup::NoSuchColumn => {
                // A source of that name without the column does not hide an
                // outer source that has it, except under PostgreSQL's rule.
                if self.outer_resolution_blocked || self.qualified_uses_nearest_table_only {
                    crate::bail_parse_error!("no such column: {qualifier}.{column}");
                }
                match self.outer.as_deref() {
                    Some(outer) => match outer.resolve_qualified(qualifier, column) {
                        Ok(Some(resolved)) => Ok(Some(resolved)),
                        Ok(None) | Err(_) => {
                            crate::bail_parse_error!("no such column: {qualifier}.{column}")
                        }
                    },
                    None => crate::bail_parse_error!("no such column: {qualifier}.{column}"),
                }
            }
            QualifiedLookup::NoSuchTable => {
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
                self.outer
                    .as_deref()
                    .map_or(Ok(None), |outer| outer.resolve_qualified(qualifier, column))
            }
        }
    }

    /// Look `table.column` up among this scope's own sources: the sources
    /// of that name, and the sources of that name inside parenthesized
    /// join groups. A group named `table` is looked up by the names it
    /// gave its columns only when no source inside it is named `table`.
    /// The copy of a USING column on the join's right side gives way to
    /// the copy on the left, except under a RIGHT JOIN, as SQLite's
    /// `lookupName` resolves it.
    fn qualified_lookup(
        &self,
        table: &str,
        database: Option<hir::DatabaseId>,
        column: &str,
        written: impl Fn() -> String,
    ) -> Result<QualifiedLookup> {
        let mut found: Option<ResolvedScopeExpr> = None;
        let mut table_seen = false;
        // Like SQLite's `lookupName`: of the copies of a USING column that
        // the name matches, a LEFT or inner join reads the left one, a
        // RIGHT JOIN the right one, and a FULL JOIN the coalesced value.
        let mut add =
            |candidate: ResolvedScopeExpr, merged: Option<hir::MergedColumnValue>| -> Result<()> {
                match (merged, &found) {
                    (Some(hir::MergedColumnValue::Right), _) | (_, None) => found = Some(candidate),
                    (Some(hir::MergedColumnValue::Coalesce), Some(_)) => {
                        found = Some(self.merged_star_column_named(column, &written())?);
                    }
                    (Some(hir::MergedColumnValue::Left), Some(_)) => {}
                    (None, Some(_)) => {
                        crate::bail_parse_error!("ambiguous column name: {}", written());
                    }
                }
                Ok(())
            };
        let merged_value = |source: &ScopeSource, name: &str| {
            source
                .using_merged
                .iter()
                .find(|(merged, _)| merged.eq_ignore_ascii_case(name))
                .map(|(_, value)| *value)
        };
        for source in self.sources.iter() {
            if let Some(saved) = &source.saved {
                let inner: Vec<usize> = saved
                    .iter()
                    .enumerate()
                    .filter(|(_, saved)| saved.kind.matches_table(table, database))
                    .map(|(index, _)| index)
                    .collect();
                if !inner.is_empty() {
                    table_seen = true;
                    // A declared column wins over the table's implicit rowid.
                    let declared: Vec<usize> = inner
                        .iter()
                        .copied()
                        .filter(|index| {
                            matches!(&saved[*index].kind, SavedGroupColumnKind::Table { name, .. } if name == column)
                        })
                        .collect();
                    let matches: Vec<usize> = if declared.is_empty() {
                        inner
                            .iter()
                            .copied()
                            .filter(|index| saved[*index].kind.matches_column(column))
                            .collect()
                    } else {
                        declared
                    };
                    // A source inside the group without the column leaves
                    // the group's own name to be checked.
                    if matches.is_empty() && database.is_none() && source.qualifier == table {
                        if let Some(candidate) = source
                            .column_views()
                            .find(|candidate| candidate.lookup_matches(column))
                        {
                            add(candidate.resolved(), None)?;
                        }
                        continue;
                    }
                    for index in matches {
                        let name = match &saved[index].kind {
                            SavedGroupColumnKind::Using { name }
                            | SavedGroupColumnKind::Table { name, .. } => name.as_str(),
                            SavedGroupColumnKind::RowId { .. } => column,
                        };
                        add(
                            source.column(index).resolved(),
                            merged_value(source, source.column(index).using_name())
                                .or_else(|| merged_value(source, name)),
                        )?;
                    }
                    continue;
                }
            }
            // SQLite checks the database against the table's and the
            // name against the alias: `main.a.id` finds `main.t AS a`.
            let named = match database {
                Some(database) => source.database == Some(database) && source.qualifier == table,
                None => source.qualifier == table,
            };
            if !named {
                continue;
            }
            table_seen = true;
            let declared = source
                .column_views()
                .find(|candidate| candidate.lookup_matches(column));
            match declared {
                Some(candidate) => add(candidate.resolved(), merged_value(source, column))?,
                None if source.rowid_available && is_rowid_name(column) => {
                    add(resolved_rowid(source.id), None)?
                }
                None => {}
            }
        }
        Ok(match found {
            Some(found) => QualifiedLookup::Found(Box::new(found)),
            None if table_seen => QualifiedLookup::NoSuchColumn,
            None => QualifiedLookup::NoSuchTable,
        })
    }

    /// The columns of parenthesized join groups that came from a source
    /// named `table`, with the group they are in.
    fn saved_group_matches(
        &self,
        table: &str,
        database: Option<hir::DatabaseId>,
    ) -> Vec<(&ScopeSource, usize)> {
        self.sources
            .iter()
            .filter_map(|source| source.saved.as_ref().map(|saved| (source, saved)))
            .flat_map(|(source, saved)| {
                saved
                    .iter()
                    .enumerate()
                    .filter(move |(_, saved)| saved.kind.matches_table(table, database))
                    .map(move |(index, _)| (source, index))
            })
            .collect()
    }

    pub(crate) fn resolve_database_qualified(
        &self,
        database: hir::DatabaseId,
        table: &str,
        column: &str,
    ) -> Result<Option<ResolvedScopeExpr>> {
        let normalized_table = crate::util::normalize_ident(table);
        let normalized_column = crate::util::normalize_ident(column);
        let written = || {
            let database = self.database_name(Some(database)).unwrap_or("main");
            format!("{database}.{table}.{column}")
        };
        let lookup = self.qualified_lookup(
            &normalized_table,
            Some(database),
            &normalized_column,
            written,
        )?;
        match lookup {
            QualifiedLookup::Found(resolved) => Ok(Some(*resolved)),
            QualifiedLookup::NoSuchColumn => {
                if self.outer_resolution_blocked || self.qualified_uses_nearest_table_only {
                    crate::bail_parse_error!("no such column: {}", written());
                }
                match self.outer.as_deref() {
                    Some(outer) => {
                        match outer.resolve_database_qualified(database, table, column) {
                            Ok(Some(resolved)) => Ok(Some(resolved)),
                            Ok(None) | Err(_) => {
                                crate::bail_parse_error!("no such column: {}", written())
                            }
                        }
                    }
                    None => crate::bail_parse_error!("no such column: {}", written()),
                }
            }
            QualifiedLookup::NoSuchTable => {
                if self.outer_resolution_blocked {
                    return Ok(None);
                }
                self.outer.as_deref().map_or(Ok(None), |outer| {
                    outer.resolve_database_qualified(database, table, column)
                })
            }
        }
    }

    pub(crate) fn expand_star(&self) -> Result<Vec<ExpandedColumn>> {
        // Repeated qualifiers are legal across databases. Within one database,
        // an unmerged visible column makes an unqualified star ambiguous.
        let mut visible_sources = HashSet::default();
        let mut visible_identities = HashMap::default();
        for column in self.visible_columns().filter(|column| !column.hidden()) {
            visible_sources.insert(column.source());
        }
        for source in self
            .sources
            .iter()
            .filter(|source| visible_sources.contains(&source.id))
        {
            let identity = (source.database, source.qualifier.as_str());
            if visible_identities.insert(identity, source.id).is_some() {
                let column = self
                    .visible_columns()
                    .find(|column| !column.hidden() && column.source() == source.id)
                    .expect("visible source must own a visible column");
                crate::bail_parse_error!(
                    "ambiguous column name: {}.{}",
                    source.qualifier,
                    column.display_name()
                );
            }
        }

        // The one source of a FROM clause that is a join group expands as
        // bare names, which the group's repeated names make ambiguous.
        let bare_group = matches!(self.sources.as_slice(), [source] if source.saved.is_some());
        // With several sources, a column of a direct source expands as
        // `database.table.column`, which a source of the same name inside a
        // join group can make ambiguous.
        let several = self.sources.len() > 1;
        self.visible_columns()
            .filter(|column| !column.hidden())
            .map(|column| {
                if bare_group {
                    self.resolve_source_column(&crate::util::normalize_ident(column.lookup_name()))?;
                }
                let source = self
                    .sources
                    .iter()
                    .find(|source| source.id == column.source());
                if let Some(source) = source.filter(|source| several && source.saved.is_none()) {
                    let collides = self
                        .saved_group_matches(&source.qualifier, source.database)
                        .into_iter()
                        .any(|(group, index)| {
                            let saved = group.saved.as_ref().expect("group");
                            matches!(&saved[index].kind, SavedGroupColumnKind::Table { name, .. } if column.lookup_matches(name))
                                && !group
                                    .using_merged
                                    .iter()
                                    .any(|(merged, _)| column.lookup_matches(merged))
                        });
                    if collides {
                        let written = match self.database_name(source.database) {
                            Some(database) => {
                                format!("{database}.{}.{}", source.qualifier, column.display_name())
                            }
                            None => format!("{}.{}", source.qualifier, column.display_name()),
                        };
                        crate::bail_parse_error!("ambiguous column name: {written}");
                    }
                }
                let qualifier = self
                    .sources
                    .iter()
                    .find(|source| source.id == column.source())
                    .map(|source| source.qualifier.clone())
                    .ok_or_else(|| {
                        crate::LimboError::InternalError(format!(
                            "visible column {} has no source in scope",
                            column.display_name()
                        ))
                    })?;
                Ok(ExpandedColumn {
                    name: column.display_name().to_string(),
                    qualifier,
                    resolved: column.resolved(),
                })
            })
            .collect()
    }

    pub(crate) fn expand_table_star(&self, qualifier: &str) -> Result<Vec<ExpandedColumn>> {
        let normalized = crate::util::normalize_ident(qualifier);
        // The name of a join group does not qualify `*`; the names of the
        // sources inside it do.
        let matching: Vec<_> = self
            .sources
            .iter()
            .filter(|source| source.qualifier == normalized && source.saved.is_none())
            .collect();
        let saved_matches: Vec<_> = self
            .saved_group_matches(&normalized, None)
            .into_iter()
            .filter(|(source, index)| {
                !matches!(
                    source.saved.as_ref().expect("group")[*index].kind,
                    SavedGroupColumnKind::RowId { .. }
                )
            })
            .collect();
        if matching.is_empty() && saved_matches.is_empty() {
            crate::bail_parse_error!("no such table: {}", qualifier);
        }
        if matching.len() > 1 {
            crate::bail_parse_error!("ambiguous table name: {}", qualifier);
        }
        if !saved_matches.is_empty() {
            // The group's subquery expands these as names qualified by the
            // source, so a direct source of the same name with the same
            // column makes them ambiguous. The one source of a FROM
            // clause expands as bare names instead.
            let bare = self.sources.len() == 1;
            let mut expanded = Vec::new();
            for source in &matching {
                for column in source.column_views().filter(|column| !column.hidden()) {
                    // With several sources, the column expands as
                    // `database.table.column`, which a source of the same
                    // name in another database does not make ambiguous.
                    let resolved = if source
                        .star_merged
                        .iter()
                        .any(|name| column.lookup_matches(name))
                    {
                        self.merged_star_column(column)?
                    } else {
                        match source.database {
                            Some(database) => self.resolve_database_qualified(
                                database,
                                qualifier,
                                column.display_name(),
                            )?,
                            None => self.resolve_qualified(qualifier, column.display_name())?,
                        }
                        .unwrap_or_else(|| column.resolved())
                    };
                    expanded.push(ExpandedColumn {
                        name: column.display_name().to_string(),
                        qualifier: source.qualifier.clone(),
                        resolved,
                    });
                }
            }
            for (source, index) in saved_matches {
                let column = source.column(index);
                let saved_name = self.saved_column_name(column);
                // The group's subquery expands these under the group's
                // own name, so a direct source cannot make them ambiguous.
                // The one source of a FROM clause expands them as bare
                // names, by the unique names the group gave them.
                let resolved = if bare {
                    let Some(resolved) = self.resolve_source_column(
                        &crate::util::normalize_ident(column.using_name()),
                    )?
                    else {
                        crate::bail_parse_error!("no such column: {}", column.display_name());
                    };
                    resolved
                } else if source.star_merged.contains(&saved_name) {
                    self.merged_star_column_named(&saved_name, column.display_name())?
                } else {
                    column.resolved()
                };
                // SQLite names a column of a join group after the group,
                // which it treats as a subquery, unless the group is the
                // one source of the FROM clause.
                expanded.push(ExpandedColumn {
                    name: column.display_name().to_string(),
                    qualifier: if bare {
                        normalized.clone()
                    } else {
                        source.qualifier.clone()
                    },
                    resolved,
                });
            }
            return Ok(expanded);
        }
        let source = matching[0];
        source
            .column_views()
            .filter(|column| !column.hidden())
            .map(|column| {
                let resolved = if source
                    .star_merged
                    .iter()
                    .any(|name| column.lookup_matches(name))
                {
                    self.merged_star_column(column)?
                } else {
                    column.resolved()
                };
                Ok(ExpandedColumn {
                    name: column.display_name().to_string(),
                    qualifier: source.qualifier.clone(),
                    resolved,
                })
            })
            .collect()
    }

    /// The name a column had before its join group made it unique.
    fn saved_column_name(&self, column: ColumnView<'_>) -> String {
        self.sources
            .iter()
            .find(|source| source.id == column.source())
            .and_then(|source| {
                let saved = source.saved.as_ref()?;
                let index = source
                    .column_views()
                    .position(|candidate| candidate.display_name() == column.display_name())?;
                match &saved[index].kind {
                    SavedGroupColumnKind::Using { name }
                    | SavedGroupColumnKind::Table { name, .. } => Some(name.clone()),
                    SavedGroupColumnKind::RowId { .. } => None,
                }
            })
            .unwrap_or_else(|| crate::util::normalize_ident(column.lookup_name()))
    }

    /// The value `table.*` reads for a column a later USING merged: the one
    /// visible column of that name, as an unqualified reference would find.
    fn merged_star_column(&self, column: ColumnView<'_>) -> Result<ResolvedScopeExpr> {
        self.merged_star_column_named(column.lookup_name(), column.display_name())
    }

    fn merged_star_column_named(
        &self,
        lookup_name: &str,
        display_name: &str,
    ) -> Result<ResolvedScopeExpr> {
        let mut matches = self
            .visible_columns()
            .filter(|visible| visible.lookup_matches(lookup_name));
        let Some(first) = matches.next() else {
            crate::bail_parse_error!("no such column: {display_name}");
        };
        if matches.next().is_some() {
            crate::bail_parse_error!("ambiguous column name: {display_name}");
        }
        Ok(first.resolved())
    }

    /// The earlier column a USING or NATURAL join compares with. Copies an
    /// earlier USING already merged are no longer visible, so the visible
    /// copies are the unmerged ones. Without a RIGHT or FULL JOIN in the
    /// FROM list, SQLite compares only the first copy. With one, a second
    /// unmerged copy is an error, as in `sqlite3ProcessJoin` (select.c):
    ///
    /// ```sql
    /// SELECT * FROM t1 JOIN t2 USING(a) JOIN t3 USING(a) RIGHT JOIN t4 ON 1;  -- ok
    /// SELECT * FROM t1 JOIN t2 ON 1 JOIN t3 USING(a) RIGHT JOIN t4 ON 1;
    /// -- ambiguous reference to a in USING()
    /// ```
    ///
    /// A NATURAL join ignores hidden columns; a USING clause does not.
    pub(crate) fn resolve_using_left(
        &self,
        name: &str,
        natural: bool,
        has_right_or_full_join: bool,
    ) -> Result<ResolvedScopeExpr> {
        let normalized = crate::util::normalize_ident(name);
        // One copy per source: a join group is one source with one column
        // of each name.
        let mut matches = self.sources.iter().filter_map(|source| {
            self.visible_columns().find(|column| {
                column.source() == source.id
                    && (!natural || !column.hidden())
                    && column.using_matches(&normalized)
            })
        });
        let Some(column) = matches.next() else {
            if natural {
                return Err(crate::LimboError::InternalError(format!(
                    "NATURAL join column {name} disappeared from the visible scope"
                )));
            }
            crate::bail_parse_error!(
                "cannot join using column {name} - column not present in both tables"
            );
        };
        if has_right_or_full_join && matches.next().is_some() {
            crate::bail_parse_error!("ambiguous reference to {name} in USING()");
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
                self.visible_columns()
                    .find(|left_column| !left_column.hidden() && left_column.using_matches(&lookup))
                    .map(|_| right_column.name.to_string())
            })
            .collect()
    }

    pub(crate) fn apply_using(&mut self, columns: &[hir::UsingColumn]) -> Result<()> {
        for using in columns {
            let right_position = self.visible_columns().position(|column| {
                matches!(
                    column.expr().as_ref(),
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
            if let Some(right_source) = Arc::make_mut(&mut self.sources)
                .iter_mut()
                .find(|source| source.id == using.right.source)
            {
                right_source
                    .using_merged
                    .push((lookup_name.clone(), using.value));
            }
            let left_position = self
                .visible_columns()
                .enumerate()
                .find(|(position, column)| {
                    *position != right_position
                        && column.using_matches(&lookup_name)
                        && same_join_column(&column.expr(), &using.left)
                })
                .map(|(position, _)| position);
            let Some(left_position) = left_position else {
                return Err(crate::LimboError::InternalError(format!(
                    "USING column {} did not belong to the left sources",
                    using.name
                )));
            };

            let merged = hir::Expr::MergedColumn(Box::new(hir::MergedColumn {
                left: using.left.clone(),
                right: Box::new(hir::Expr::Column(using.right)),
                value: using.value,
                type_fact: using.type_fact.clone(),
                affinity: using.affinity,
                has_affinity: using.has_affinity,
                collation: using.collation.clone(),
            }));

            let visible = self.owned_visible_columns();
            let VisibleColumn::Binding(left) = &mut visible[left_position] else {
                unreachable!("owned visible columns are bindings");
            };
            left.resolved = ResolvedScopeExpr {
                expr: merged,
                type_fact: using.type_fact.clone(),
                affinity: using.affinity,
                has_affinity: using.has_affinity,
                collation: ExprCollation::inherited(using.collation.clone()),
            };
            let left_source = left.source;
            let merged_expr = left.resolved.expr.clone();
            visible.remove(right_position);
            // Every column of the right source with the merged name gives
            // way to the merged value: a join group can hold several
            // columns the name finds. Under a RIGHT JOIN the merged value
            // also replaces every other copy on the left, as SQLite's
            // `lookupName` lets the right-most table win.
            let right_join = using.value == hir::MergedColumnValue::Right;
            visible.retain(|column| {
                let VisibleColumn::Binding(column) = column else {
                    unreachable!("owned visible columns are bindings");
                };
                if !column.lookup_name.matches(&lookup_name) {
                    return true;
                }
                if same_join_column(&column.resolved.expr, &merged_expr) {
                    return true;
                }
                if right_join {
                    return false;
                }
                column.source == left_source || column.source != using.right.source
            });
        }
        Ok(())
    }

    fn resolve_source_column(&self, name: &str) -> Result<Option<ResolvedScopeExpr>> {
        let mut found = None;
        for column in self
            .visible_columns()
            .filter(|column| !column.rowid_names() && column.lookup_matches(name))
        {
            if found.is_some() {
                crate::bail_parse_error!("ambiguous column name: {}", name);
            }
            found = Some(column.resolved());
        }
        if found.is_some() {
            return Ok(found);
        }

        // A declared column of any name wins over every implicit rowid,
        // including the rowids parenthesized join groups saved.
        if !is_rowid_name(name) {
            return Ok(None);
        }
        for column in self.visible_columns().filter(|column| column.rowid_names()) {
            if found.is_some() {
                crate::bail_parse_error!("ambiguous column name: {}", name);
            }
            found = Some(column.resolved());
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

    /// Whether a source of this scope or an enclosing one is named
    /// `qualifier` (normalized), whatever this scope may resolve.
    pub(crate) fn has_qualifier(&self, qualifier: &str) -> bool {
        self.sources
            .iter()
            .any(|source| source.qualifier == qualifier)
            || !self.saved_group_matches(qualifier, None).is_empty()
            || self
                .outer
                .as_deref()
                .is_some_and(|outer| outer.has_qualifier(qualifier))
    }

    fn visible_columns(&self) -> impl Iterator<Item = ColumnView<'_>> + '_ {
        self.visible.iter().flat_map(|visible| {
            let (source, binding) = match visible {
                VisibleColumn::AllOfSource(source) => (Some(&self.sources[*source]), None),
                VisibleColumn::Binding(binding) => (None, Some(ColumnView::Binding(binding))),
            };
            source
                .into_iter()
                .flat_map(ScopeSource::column_views)
                .chain(binding)
        })
    }

    fn owned_visible_columns(&mut self) -> &mut Vec<VisibleColumn> {
        if self
            .visible
            .iter()
            .any(|visible| matches!(visible, VisibleColumn::AllOfSource(_)))
        {
            let owned = self
                .visible_columns()
                .map(|column| VisibleColumn::Binding(column.to_binding()))
                .collect();
            self.visible = Arc::new(owned);
        }
        Arc::make_mut(&mut self.visible)
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
            same_join_column(&left.right, &right.right)
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
pub(crate) mod tests {
    thread_local! {
        /// How many sources were added to scopes on this thread.
        pub(crate) static SOURCES_ADDED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        /// How many column bindings were made or copied on this thread.
        pub(crate) static COLUMN_BINDINGS_MADE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        /// How many times a scope was given a block's outputs on this thread.
        pub(crate) static OUTPUT_LISTS_SET: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

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
            name: name.into(),
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
            columns: columns.into(),
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
    fn local_qualifier_without_the_column_falls_back_to_the_outer_source() {
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

        // A local source of that name without the column does not hide
        // the outer source that has it.
        let resolved = inner
            .resolve_qualified("chosen", "id")
            .expect("outer column resolves")
            .expect("outer column is found");
        expect_column(resolved, outer_items.id, 0);
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

        assert!(
            error
                .to_string()
                .contains("ambiguous column name: main.items.id"),
            "{error}"
        );
    }

    #[test]
    fn local_database_table_without_the_column_falls_back_to_the_outer_source() {
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

        let resolved = inner
            .resolve_database_qualified(DatabaseId::new(0), "items", "id")
            .expect("outer column resolves")
            .expect("outer column is found");
        expect_column(resolved, outer_items.id, 0);
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
            assert!(matches!(*merged.right, Expr::Column(actual) if actual == right_ref));
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
        let first_expr = Expr::MergedColumn(Box::new(hir::MergedColumn {
            left: first.left.clone(),
            right: Box::new(Expr::Column(first.right)),
            value: first.value,
            type_fact: first.type_fact.clone(),
            affinity: first.affinity,
            has_affinity: first.has_affinity,
            collation: first.collation.clone(),
        }));
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
        assert!(matches!(*merged.right, Expr::Column(actual) if actual == right_ref));
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

        assert_eq!(scope.natural_common_columns(&right), vec!["id"]);
        let resolved = scope
            .resolve_using_left("id", true, false)
            .expect("visible NATURAL column resolves");
        expect_column(resolved, left.id, 0);
    }

    #[test]
    fn natural_join_compares_the_first_left_copy_without_a_right_or_full_join() {
        let first = source(0, "first", None, vec![column("id", false)], false);
        let second = source(1, "second", None, vec![column("id", false)], false);
        let mut scope = Scope::default();
        scope.add_source(&first, true);
        scope.add_source(&second, true);

        let resolved = scope
            .resolve_using_left("id", true, false)
            .expect("the first copy is compared");
        expect_column(resolved, first.id, 0);
    }

    #[test]
    fn natural_join_rejects_a_second_unmerged_left_copy_with_a_right_or_full_join() {
        let first = source(0, "first", None, vec![column("id", false)], false);
        let second = source(1, "second", None, vec![column("id", false)], false);
        let mut scope = Scope::default();
        scope.add_source(&first, true);
        scope.add_source(&second, true);

        let error = scope
            .resolve_using_left("id", true, true)
            .expect_err("a RIGHT or FULL JOIN makes the second copy an error");

        assert!(error
            .to_string()
            .contains("ambiguous reference to id in USING()"));
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
            .resolve_using_left("missing", false, false)
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
