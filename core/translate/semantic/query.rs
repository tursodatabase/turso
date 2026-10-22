//! Query-source conversion into HIR arenas and scopes.

use turso_parser::ast;

use super::{
    analyze::{output_from_resolved, Analyzer, CatalogObjectKind},
    cte::{CteBindingContext, CteResolution},
    expr::{build_using_column, ExprPolicies, ExprPolicy},
    hir::{self, CatalogObject, DeclaredType, SourceOwner, TypeFact},
    schema_program::TypeTransform,
    scope::{resolve_source_column, ResolvedScopeExpr, SavedGroupColumn, Scope},
};
use crate::{
    schema::Table, sync::Arc, translate::expr::StorageClassMask, vdbe::affinity::Affinity,
    LimboError, Result,
};

/// The affinity of column `position` of a compound query, combined across
/// its arms like SQLite's `sqlite3SubqueryColumnTypes`: the first arm with an
/// affinity sets it, and a later arm that can give a value of the other
/// class (text against numeric) removes it, so the column compares by
/// storage class. The second value is whether the column has an affinity.
pub(crate) fn compound_column_affinity<'a>(
    query: &hir::Query,
    position: usize,
    source_of: &dyn Fn(hir::SourceId) -> Option<&'a hir::Source>,
) -> (Affinity, bool) {
    let outputs = query
        .blocks
        .iter()
        .filter_map(|block| block.outputs.get(position))
        .collect::<Vec<_>>();
    let affinity_of = |output: &hir::Output| {
        if output.has_affinity {
            output.schema_affinity
        } else {
            Affinity::None
        }
    };
    let mut data_types = StorageClassMask::from_null();
    let mut arms = outputs.iter();
    let mut affinity = Affinity::None;
    for output in arms.by_ref() {
        affinity = affinity_of(output);
        if affinity != Affinity::None {
            break;
        }
        data_types |= expr_data_type(&output.expr, source_of);
    }
    if affinity == Affinity::None {
        return (Affinity::Blob, false);
    }
    for output in arms {
        data_types |= expr_data_type(&output.expr, source_of);
    }
    let conflicts = match affinity {
        Affinity::Text => data_types.has_numeric(),
        affinity => affinity.is_numeric() && data_types.has_text(),
    };
    if conflicts {
        (Affinity::Blob, false)
    } else {
        (affinity, true)
    }
}

/// The storage classes an expression can give, like SQLite's
/// `sqlite3ExprDataType`.
pub(crate) fn expr_data_type<'a>(
    expression: &hir::Expr,
    source_of: &dyn Fn(hir::SourceId) -> Option<&'a hir::Source>,
) -> StorageClassMask {
    let from_affinity = |affinity: Affinity| {
        if affinity.is_numeric() {
            StorageClassMask::NUMERIC | StorageClassMask::BLOB
        } else if affinity == Affinity::Text {
            StorageClassMask::TEXT | StorageClassMask::BLOB
        } else {
            StorageClassMask::all()
        }
    };
    match expression {
        hir::Expr::Collate { expr, .. }
        | hir::Expr::Unary {
            operator: ast::UnaryOperator::Positive,
            expr,
        } => expr_data_type(expr, source_of),
        hir::Expr::Literal(literal) => {
            match crate::translate::alter::literal_default_value(literal)
                .map(|value| value.value_type())
            {
                Ok(crate::types::ValueType::Null) => StorageClassMask::from_null(),
                Ok(crate::types::ValueType::Text) => StorageClassMask::from_text(),
                Ok(crate::types::ValueType::Blob) => StorageClassMask::from_blob(),
                _ => StorageClassMask::from_numeric(),
            }
        }
        hir::Expr::Binary {
            operator: ast::Operator::Concat,
            ..
        } => StorageClassMask::TEXT | StorageClassMask::BLOB,
        hir::Expr::Function(_) | hir::Expr::Parameter(_) => StorageClassMask::all(),
        hir::Expr::Column(reference) => source_of(reference.source)
            .and_then(|source| source.columns.get(reference.column))
            .filter(|column| column.has_affinity)
            .map_or(StorageClassMask::all(), |column| {
                from_affinity(column.affinity)
            }),
        hir::Expr::RowId(_) => from_affinity(Affinity::Integer),
        hir::Expr::Cast { target, .. } => from_affinity(target.affinity),
        hir::Expr::Subquery(_) => StorageClassMask::all(),
        hir::Expr::Case {
            when_then,
            else_expr,
            ..
        } => {
            let mut types = StorageClassMask::from_null();
            for (_, then) in when_then {
                types |= expr_data_type(then, source_of);
            }
            if let Some(else_expr) = else_expr {
                types |= expr_data_type(else_expr, source_of);
            }
            types
        }
        _ => StorageClassMask::from_numeric(),
    }
}

struct AnalyzedTableSource<'ast> {
    id: hir::SourceId,
    function_arguments: Option<&'ast [Box<ast::Expr>]>,
    /// For a parenthesized join group of several sources: where each of
    /// its columns came from, for name lookup in the query around it.
    saved: Option<Vec<SavedGroupColumn>>,
    /// A LATERAL subquery, which may read the FROM items on its left.
    lateral: bool,
}

/// How a parenthesized join group of several sources joins the query
/// around it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GroupMode {
    /// Its sources join the outer sources directly; the group only keeps
    /// them together for join ordering and name lookup.
    Flatten,
    /// It runs as a subquery whose rows the outer join reads, as SQLite
    /// runs every such group: the right side of a RIGHT, FULL, USING or
    /// NATURAL join, or of a LEFT JOIN when the group has outer joins of
    /// its own.
    Materialize,
}

/// Whether the right side of `join` runs as a subquery when it is a
/// parenthesized join group.
fn group_mode_of_join(join: &ast::JoinedSelectTable) -> GroupMode {
    let kind = join_kind(join.operator);
    let merges = is_natural_join(join.operator)
        || matches!(join.constraint, Some(ast::JoinConstraint::Using(_)));
    let outer_joins_inside = matches!(
        join.table.as_ref(),
        ast::SelectTable::Sub(inner, _) if !from_syntax_is_ordinary(inner)
    );
    // A RIGHT or FULL JOIN inside the group keeps only the group's own
    // sources NULL on its unmatched rows; joined directly, the pass that
    // finds those rows would NULL the outer sources before the group too.
    let keeps_right_rows_inside = matches!(
        join.table.as_ref(),
        ast::SelectTable::Sub(inner, _) if from_syntax_keeps_right_rows(inner)
    );
    if merges
        || matches!(kind, hir::JoinKind::Right | hir::JoinKind::Full)
        || (kind == hir::JoinKind::Left && outer_joins_inside)
        || keeps_right_rows_inside
    {
        GroupMode::Materialize
    } else {
        GroupMode::Flatten
    }
}

/// Whether a FROM clause has only inner joins, including inside its
/// parenthesized groups.
fn from_syntax_is_ordinary(from: &ast::FromClause) -> bool {
    let ordinary_term = |term: &ast::SelectTable| match term {
        ast::SelectTable::Sub(inner, _) => from_syntax_is_ordinary(inner),
        _ => true,
    };
    ordinary_term(&from.select)
        && from.joins.iter().all(|join| {
            matches!(
                join_kind(join.operator),
                hir::JoinKind::Inner | hir::JoinKind::Comma | hir::JoinKind::Cross
            ) && ordinary_term(&join.table)
        })
}

/// Whether a FROM clause has a RIGHT or FULL JOIN, including inside its
/// parenthesized groups.
fn from_syntax_keeps_right_rows(from: &ast::FromClause) -> bool {
    let inside = |term: &ast::SelectTable| match term {
        ast::SelectTable::Sub(inner, _) => from_syntax_keeps_right_rows(inner),
        _ => false,
    };
    inside(&from.select)
        || from.joins.iter().any(|join| {
            matches!(
                join_kind(join.operator),
                hir::JoinKind::Right | hir::JoinKind::Full
            ) || inside(&join.table)
        })
}

pub(super) struct AnalyzedSourceColumn {
    pub(super) column: hir::SourceColumn,
    pub(super) programs: Option<hir::BoundColumnTypePrograms>,
}

#[derive(Clone, Copy)]
pub(super) enum FromContext<'scope> {
    QueryBlock {
        block: hir::QueryBlockId,
        outer_scope: Option<&'scope Scope>,
        policies: ExprPolicies,
    },
    Dml {
        outer_scope: Option<&'scope Scope>,
        policies: ExprPolicies,
    },
}

impl<'scope> FromContext<'scope> {
    fn source_owner(self) -> SourceOwner {
        match self {
            Self::QueryBlock { block, .. } => SourceOwner::QueryBlock(block),
            Self::Dml { .. } => SourceOwner::Root,
        }
    }

    fn query_parent(self) -> Option<hir::QueryId> {
        match self {
            Self::QueryBlock { block, .. } => Some(block.query),
            Self::Dml { .. } => None,
        }
    }

    fn outer_scope(self) -> Option<&'scope Scope> {
        match self {
            Self::QueryBlock { outer_scope, .. } => outer_scope,
            Self::Dml { outer_scope, .. } => outer_scope,
        }
    }

    fn cte_context(self) -> CteBindingContext<'scope> {
        match self {
            Self::QueryBlock {
                block,
                outer_scope,
                policies,
            } => CteBindingContext::new(block.query, outer_scope, policies),
            Self::Dml { policies, .. } => CteBindingContext::root(policies),
        }
    }

    fn expr_policies(self) -> ExprPolicies {
        match self {
            Self::QueryBlock { policies, .. } => policies,
            Self::Dml { policies, .. } => policies,
        }
    }
}

#[derive(Clone, Copy)]
enum CatalogSourceKind {
    Table,
    TableFunction,
}

impl<'context, 'catalog, 'ast> Analyzer<'context, 'catalog, 'ast> {
    pub(super) fn analyze_named_relation_query(
        &mut self,
        name: &ast::QualifiedName,
        arguments: &'ast [Box<ast::Expr>],
        parent: Option<hir::QueryId>,
        outer_scope: &Scope,
        policies: ExprPolicies,
    ) -> Result<hir::QueryId> {
        let query = self.reserve_query();
        let block_id = hir::QueryBlockId::new(query, 0);
        let analyzed = self.analyze_table_function_source(
            name,
            arguments,
            None,
            SourceOwner::QueryBlock(block_id),
            CteBindingContext::new(query, Some(outer_scope), policies),
        )?;
        if let Some(arguments) = analyzed.function_arguments {
            self.analyze_table_function_arguments(
                analyzed.id,
                arguments,
                outer_scope,
                Some(query),
                policies,
            )?;
        }

        let source = self.source(analyzed.id).ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing semantic relation source {}",
                analyzed.id
            ))
        })?;
        let mut scope = Scope::new(Some(outer_scope.clone()));
        scope.add_source(source, true);
        let expanded = scope.expand_star()?;
        let mut outputs = Vec::with_capacity(expanded.len());
        for column in expanded {
            let resolved = self.resolve_atomic_expr(column.resolved.expr, &scope)?;
            outputs.push(output_from_resolved(
                hir::OutputId::query(block_id, outputs.len()),
                column.name,
                hir::OutputNameKind::StarExpansion,
                resolved,
            ));
        }

        let mut block = hir::QueryBlock::new(
            block_id,
            hir::QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: analyzed.id,
            joins: Vec::new(),
        });
        block.outputs = outputs;
        let output = block.outputs.iter().map(|output| output.id).collect();
        let reachable_ctes = self.direct_ctes(std::slice::from_ref(&block))?;
        let mut query = hir::Query {
            id: query,
            parent,
            captures: Vec::new(),
            reachable_ctes,
            first: block_id,
            blocks: vec![block],
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output,
        };
        query.captures = query.direct_captures(|source| self.source(source));
        let id = query.id;
        self.insert_query(id, query)?;
        Ok(id)
    }

    pub(super) fn analyze_from_clause(
        &mut self,
        syntax: &'ast ast::FromClause,
        context: FromContext<'_>,
    ) -> Result<(hir::From, Scope)> {
        // A parenthesized group without an alias at the start of the FROM
        // clause is the clause itself: `(a JOIN b) JOIN c` is `a JOIN b JOIN
        // c`, as SQLite's parser reads it.
        let mut first_syntax: &'ast ast::SelectTable = &syntax.select;
        let mut syntax_joins: Vec<&'ast ast::JoinedSelectTable> = syntax.joins.iter().collect();
        while let ast::SelectTable::Sub(inner, None) = first_syntax {
            first_syntax = &inner.select;
            syntax_joins.splice(0..0, inner.joins.iter());
        }
        let mut scope = Scope::new(context.outer_scope().cloned());
        scope.set_database_names(
            (0..)
                .map_while(|index| self.context().database_name_by_index(index))
                .collect(),
        );
        let first =
            self.analyze_table_source(first_syntax, context, 0, None, GroupMode::Flatten, &scope)?;
        let source = first.id;
        let mut table_functions = Vec::new();
        if let Some(arguments) = first.function_arguments {
            table_functions.push((source, arguments));
        }
        let mut lateral_sources = Vec::new();
        if first.lateral {
            lateral_sources.push(source);
        }
        let definition = self.source(source).ok_or_else(|| {
            crate::LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        match first.saved {
            Some(saved) => scope.add_group_source(definition, saved),
            None => scope.add_source(definition, true),
        }

        // SQLite checks USING columns against every earlier source once any
        // RIGHT or FULL JOIN appears in the FROM list, even one written later.
        let has_right_or_full_join = syntax_joins.iter().any(|syntax_join| {
            matches!(
                join_kind(syntax_join.operator),
                hir::JoinKind::Right | hir::JoinKind::Full
            )
        });
        let mut joins = Vec::with_capacity(syntax_joins.len());
        for syntax_join in &syntax_joins {
            let kind = join_kind(syntax_join.operator);
            let natural = is_natural_join(syntax_join.operator);
            if natural && syntax_join.constraint.is_some() {
                crate::bail_parse_error!("a NATURAL join may not have an ON or USING clause");
            }
            let analyzed = self.analyze_table_source(
                &syntax_join.table,
                context,
                joins.len() + 1,
                None,
                group_mode_of_join(syntax_join),
                &scope,
            )?;
            let right = analyzed.id;
            if let Some(arguments) = analyzed.function_arguments {
                table_functions.push((right, arguments));
            }
            if analyzed.lateral {
                lateral_sources.push(right);
            }
            let definition = self.source(right).ok_or_else(|| {
                crate::LimboError::InternalError(format!("missing semantic source {right}"))
            })?;

            let using_names = if natural {
                Some(scope.natural_common_columns(definition))
            } else if let Some(ast::JoinConstraint::Using(names)) = &syntax_join.constraint {
                Some(names.iter().map(|name| name.as_str().to_string()).collect())
            } else {
                None
            };
            let using_columns = using_names
                .map(|names| {
                    names
                        .into_iter()
                        .map(|name| {
                            // The right column is checked first: a missing
                            // right column is reported before an ambiguous
                            // left copy.
                            let right = resolve_source_column(definition, &name)?;
                            let left =
                                scope.resolve_using_left(&name, natural, has_right_or_full_join)?;
                            // The columns compare as `left = right` written
                            // in an ON clause would: a table column without
                            // a declared collation decides the comparison
                            // as BINARY, like any column decides it.
                            let right = self.using_operand(right, &scope)?;
                            let left = self.using_operand(left, &scope)?;
                            let value = merged_column_value(kind);
                            let mut column = build_using_column(name, left.clone(), right, value)?;
                            if value == hir::MergedColumnValue::Coalesce {
                                // The merged column of a FULL JOIN keeps the
                                // left column's affinity and collation, so
                                // later joins and aggregates compare it the
                                // way the left table does.
                                column.affinity = left.affinity;
                                column.has_affinity = left.has_affinity;
                                column.collation = left.collation.value().cloned();
                            }
                            Ok(column)
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;

            match analyzed.saved {
                Some(saved) => scope.add_group_source(definition, saved),
                None => scope.add_source(definition, true),
            }
            if let Some(columns) = &using_columns {
                scope.apply_using(columns)?;
            }
            let constraint = match using_columns {
                Some(columns) if natural => hir::JoinConstraint::Natural(columns),
                Some(columns) => hir::JoinConstraint::Using(columns),
                None => hir::JoinConstraint::None,
            };

            joins.push(hir::Join {
                right,
                kind,
                constraint,
            });
        }

        // As in PostgreSQL, a LATERAL subquery on either side of a RIGHT or
        // FULL JOIN cannot read the FROM items on its left: the join's
        // unmatched rows have no row of those items to read.
        if joins
            .iter()
            .any(|join| matches!(join.kind, hir::JoinKind::Right | hir::JoinKind::Full))
        {
            let from_sources: Vec<hir::SourceId> = std::iter::once(source)
                .chain(joins.iter().map(|join| join.right))
                .collect();
            for lateral in &lateral_sources {
                let Some(hir::SourceKind::Derived(query)) =
                    self.source(*lateral).map(|source| &source.kind)
                else {
                    continue;
                };
                if let Some(read) = self.query_reads_any_source(*query, &from_sources) {
                    let read = self.source(read).expect("FROM source exists");
                    let right_of_the_join = joins.iter().any(|join| {
                        join.right == *lateral
                            && matches!(join.kind, hir::JoinKind::Right | hir::JoinKind::Full)
                    });
                    if right_of_the_join {
                        crate::bail_parse_error!(
                            "invalid reference to FROM-clause entry for table \"{}\": the combining JOIN type must be INNER or LEFT for a LATERAL reference",
                            read.alias.as_deref().unwrap_or(&read.name)
                        );
                    }
                    // The pass that finds the join's unmatched rows runs
                    // the subquery with its left sources NULL-extended,
                    // which this subquery's rows cannot stand for.
                    crate::bail_parse_error!(
                        "RIGHT and FULL joins are not supported in a FROM clause with a LATERAL subquery that reads a table on its left"
                    );
                }
            }
        }

        // `table.*` on a source left of a RIGHT or FULL JOIN reads the merged
        // value of every column a later USING merges, as SQLite does.
        let sources = std::iter::once(source).chain(joins.iter().map(|join| join.right));
        for (position, source) in sources.enumerate() {
            let later_joins = &joins[position..];
            if !later_joins
                .iter()
                .any(|join| matches!(join.kind, hir::JoinKind::Right | hir::JoinKind::Full))
            {
                continue;
            }
            let names: Vec<String> = later_joins
                .iter()
                .flat_map(|join| match &join.constraint {
                    hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                        columns.as_slice()
                    }
                    hir::JoinConstraint::On(_) | hir::JoinConstraint::None => &[],
                })
                .map(|column| column.name.clone())
                .collect();
            if !names.is_empty() {
                scope.mark_star_merged_columns(source, &names);
            }
        }

        for (source, arguments) in table_functions {
            self.analyze_table_function_arguments(
                source,
                arguments,
                &scope,
                context.query_parent(),
                context.expr_policies(),
            )?;
        }

        let policy = context.expr_policies().select();
        for (index, syntax_join) in syntax_joins.iter().enumerate() {
            let Some(ast::JoinConstraint::On(expression)) = &syntax_join.constraint else {
                continue;
            };
            let constraint = self.analyze_source_scalar_expr(
                expression,
                &scope,
                policy,
                context.query_parent(),
            )?;
            // An ON clause may read any table of the FROM list, but a
            // LATERAL subquery to its right runs after it and is not visible.
            let later_laterals: Vec<hir::SourceId> = joins[index + 1..]
                .iter()
                .map(|join| join.right)
                .filter(|right| lateral_sources.contains(right))
                .collect();
            if let Some(read) = self.expr_reads_any_source(&constraint, &later_laterals) {
                let read = self.source(read).expect("FROM source exists");
                crate::bail_parse_error!(
                    "no such table: {}",
                    read.alias.as_deref().unwrap_or(&read.name)
                );
            }
            joins[index].constraint = hir::JoinConstraint::On(constraint);
        }

        Ok((
            hir::From {
                first: source,
                joins,
            },
            scope,
        ))
    }

    /// `alias_override` is the alias of a parenthesized group around a
    /// single source: it replaces the source's own alias, also when it is
    /// none.
    /// `left` is the scope of the FROM items before this one, which a
    /// LATERAL subquery reads.
    fn analyze_table_source(
        &mut self,
        syntax: &'ast ast::SelectTable,
        context: FromContext<'_>,
        position: usize,
        alias_override: Option<Option<&'ast ast::As>>,
        group_mode: GroupMode,
        left: &Scope,
    ) -> Result<AnalyzedTableSource<'ast>> {
        let owner = context.source_owner();
        let cte_context = context.cte_context();
        match syntax {
            ast::SelectTable::Table(name, alias, indexed) => {
                let alias = alias_override.unwrap_or(alias.as_ref());
                if name.namespace.is_none() {
                    if let Some(cte) = self.resolve_cte(name.name.as_str(), cte_context)? {
                        if let Some(ast::Indexed::IndexedBy(index)) = indexed {
                            crate::bail_parse_error!("no such index: {}", index.as_str());
                        }
                        let id = self.analyze_cte_source(
                            cte,
                            name.name.as_str(),
                            alias
                                .map(ast::As::name)
                                .or(name.alias.as_ref())
                                .map(ast::Name::as_str),
                            owner,
                        )?;
                        return Ok(AnalyzedTableSource {
                            id,
                            function_arguments: None,
                            saved: None,
                            lateral: false,
                        });
                    }
                }
                let id = if self.table_exists_in_scope(name)? {
                    self.analyze_base_table_source(name, alias, indexed.as_ref(), owner)?
                } else if let Some((database, view)) = self.resolve_view_in_scope(name)? {
                    if let Some(ast::Indexed::IndexedBy(index)) = indexed {
                        crate::bail_parse_error!("no such index: {}", index.as_str());
                    }
                    self.analyze_view_source(database, view, alias, context)?
                } else if self.context().is_incompatible_view(name) {
                    crate::bail_parse_error!(
                        "Materialized view '{}' has an incompatible version. \n\
                         The view was created with a different DBSP version than the current version ({}). \n\
                         Please DROP and recreate the view to use it.",
                        crate::util::normalize_ident(name.name.as_str()),
                        crate::incremental::view::DBSP_CIRCUIT_VERSION
                    );
                } else if self.context().is_broken_view(name) {
                    crate::bail_parse_error!(
                        "view '{}' could not be loaded: its SQL in sqlite_schema does not parse. \n\
                         Use DROP VIEW to remove it, then recreate it.",
                        crate::util::normalize_ident(name.name.as_str())
                    );
                } else {
                    self.analyze_base_table_source(name, alias, indexed.as_ref(), owner)?
                };
                Ok(AnalyzedTableSource {
                    id,
                    function_arguments: None,
                    saved: None,
                    lateral: false,
                })
            }
            ast::SelectTable::TableCall(name, arguments, alias) => self
                .analyze_table_function_source(
                    name,
                    arguments,
                    alias_override.unwrap_or(alias.as_ref()),
                    owner,
                    cte_context,
                ),
            ast::SelectTable::Select {
                select,
                alias,
                lateral,
            } => {
                let alias = alias_override.unwrap_or(alias.as_ref());
                let id = self.analyze_derived_source(
                    select,
                    alias,
                    context,
                    position,
                    lateral.then_some(left),
                )?;
                Ok(AnalyzedTableSource {
                    id,
                    function_arguments: None,
                    saved: None,
                    lateral: *lateral,
                })
            }
            ast::SelectTable::Sub(from, alias) => {
                let alias = alias_override.unwrap_or(alias.as_ref());
                if from.joins.is_empty() {
                    // A group around one source is that source, named by
                    // the group's alias.
                    return self.analyze_table_source(
                        &from.select,
                        context,
                        position,
                        Some(alias),
                        group_mode,
                        left,
                    );
                }
                self.analyze_from_group(from, alias, context, position, group_mode)
            }
        }
    }

    fn analyze_table_function_source(
        &mut self,
        name: &ast::QualifiedName,
        arguments: &'ast [Box<ast::Expr>],
        alias: Option<&ast::As>,
        owner: SourceOwner,
        cte_context: CteBindingContext<'_>,
    ) -> Result<AnalyzedTableSource<'ast>> {
        if name.namespace.is_none() {
            if let Some(cte) = self.resolve_cte(name.name.as_str(), cte_context)? {
                if !arguments.is_empty() {
                    match cte {
                        CteResolution::RecursiveInput { .. } => crate::bail_parse_error!(
                            "too many arguments on {}() - max 0",
                            name.name.as_str()
                        ),
                        CteResolution::Cte(_) => {
                            crate::bail_parse_error!("'{}' is not a function", name.name.as_str())
                        }
                    }
                }
                let id = self.analyze_cte_source(
                    cte,
                    name.name.as_str(),
                    alias.map(ast::As::name).map(ast::Name::as_str),
                    owner,
                )?;
                return Ok(AnalyzedTableSource {
                    id,
                    function_arguments: None,
                    saved: None,
                    lateral: false,
                });
            }
        }

        let (database, table) = match self.resolve_table_in_scope(name) {
            Ok(resolved) => resolved,
            // A module's eponymous table exists in every namespace, as in
            // SQLite, where `temp.json_each(...)` names the same function.
            Err(error) if name.namespace.is_some() => {
                let unqualified = ast::QualifiedName {
                    namespace: None,
                    name: name.name.clone(),
                    alias: name.alias.clone(),
                };
                match self.context().resolve_table(&unqualified) {
                    Ok((database, table)) if table.virtual_table().is_some() => (database, table),
                    _ => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        let kind = if table.virtual_table().is_some() {
            let maximum = table
                .columns()
                .iter()
                .filter(|column| column.hidden())
                .count();
            if arguments.len() > maximum {
                crate::bail_parse_error!(
                    "Too many arguments for {}: expected at most {}, got {}",
                    table.get_name(),
                    maximum,
                    arguments.len()
                );
            }
            CatalogSourceKind::TableFunction
        } else {
            if !arguments.is_empty() {
                crate::bail_parse_error!("'{}' is not a function", name.name.as_str());
            }
            CatalogSourceKind::Table
        };
        let id = self.analyze_catalog_table_source(
            &name.name,
            alias.map(ast::As::name).or(name.alias.as_ref()),
            None,
            owner,
            database,
            table,
            kind,
        )?;
        Ok(AnalyzedTableSource {
            id,
            function_arguments: matches!(kind, CatalogSourceKind::TableFunction)
                .then_some(arguments)
                .filter(|arguments| !arguments.is_empty()),
            saved: None,
            lateral: false,
        })
    }

    /// A parenthesized group of several sources. SQLite makes a subquery
    /// of it whose columns keep the names of the sources inside, so the
    /// outer query can name them. Here the group either joins the outer
    /// sources directly or runs as that subquery, by `mode`; the names it
    /// exposes are the same either way.
    fn analyze_from_group(
        &mut self,
        syntax: &'ast ast::FromClause,
        alias: Option<&ast::As>,
        context: FromContext<'_>,
        position: usize,
        mode: GroupMode,
    ) -> Result<AnalyzedTableSource<'ast>> {
        let alias = alias
            .map(ast::As::name)
            .map(ast::Name::as_str)
            .map(crate::util::normalize_ident);
        let source = self.reserve_source();
        let (kind, defs) = match mode {
            GroupMode::Flatten => {
                let (from, inner_scope) = self.analyze_from_clause(syntax, context)?;
                let defs = inner_scope.saved_group_columns(&from)?;
                let values = defs.iter().map(|def| def.value.expr.clone()).collect();
                (
                    hir::SourceKind::FromGroup(hir::FromGroup {
                        from: Box::new(from),
                        columns: values,
                    }),
                    defs,
                )
            }
            GroupMode::Materialize => {
                let query_id = self.reserve_query();
                let block_id = hir::QueryBlockId::new(query_id, 0);
                let (from, inner_scope) = self.analyze_from_clause(
                    syntax,
                    FromContext::QueryBlock {
                        block: block_id,
                        outer_scope: context.outer_scope(),
                        policies: context.expr_policies(),
                    },
                )?;
                let defs = inner_scope.saved_group_columns(&from)?;
                let outputs: Vec<hir::Output> = defs
                    .iter()
                    .enumerate()
                    .map(|(index, def)| {
                        output_from_resolved(
                            hir::OutputId::query(block_id, index),
                            def.name.clone(),
                            hir::OutputNameKind::ExplicitAlias,
                            def.value.clone(),
                        )
                    })
                    .collect();
                let output = outputs.iter().map(|output| output.id).collect();
                let mut block = hir::QueryBlock::new(
                    block_id,
                    hir::QueryBlockBody::Select {
                        distinctness: None,
                        filter: None,
                        grouping: None,
                    },
                );
                block.from = Some(from);
                block.outputs = outputs;
                let blocks = vec![block];
                let reachable_ctes = self.direct_ctes(&blocks)?;
                let mut query = hir::Query {
                    id: query_id,
                    parent: context.query_parent(),
                    captures: Vec::new(),
                    reachable_ctes,
                    blocks,
                    first: block_id,
                    compounds: Vec::new(),
                    order_by: Vec::new(),
                    limit: None,
                    output,
                };
                query.captures = query.direct_captures(|source| self.source(source));
                self.insert_query(query_id, query)?;
                (hir::SourceKind::Derived(query_id), defs)
            }
        };
        let width = defs.len();
        let columns = defs
            .iter()
            .map(|def| hir::SourceColumn {
                name: def.name.clone(),
                type_fact: def.value.type_fact.clone(),
                affinity: def.value.affinity,
                has_affinity: def.value.has_affinity,
                collation: def.value.collation.value().cloned(),
                hidden: false,
                rowid_alias: false,
            })
            .collect();
        let saved = defs.into_iter().map(|def| def.saved).collect();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner: context.source_owner(),
                database: None,
                name: format!("(join-{position})"),
                alias,
                kind,
                columns,
                generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
                default_expressions: vec![hir::ColumnReadExpression::Absent; width],
                column_type_programs: vec![None; width],
                check_constraints: None,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(AnalyzedTableSource {
            id: source,
            function_arguments: None,
            saved: Some(saved),
            lateral: false,
        })
    }

    fn analyze_table_function_arguments(
        &mut self,
        source: hir::SourceId,
        syntax: &'ast [Box<ast::Expr>],
        scope: &Scope,
        parent: Option<hir::QueryId>,
        policies: ExprPolicies,
    ) -> Result<()> {
        let policy = policies.table_function();
        let mut arguments = Vec::with_capacity(syntax.len());
        for argument in syntax {
            arguments
                .push(self.analyze_source_scalar_expr_with_facts(argument, scope, policy, parent)?);
        }
        let hidden_columns = self
            .source(source)
            .ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "missing semantic table-function source {source}"
                ))
            })?
            .columns
            .iter()
            .enumerate()
            .filter_map(|(column, definition)| definition.hidden.then_some(column))
            .take(arguments.len())
            .collect::<Vec<_>>();
        if hidden_columns.len() != arguments.len() {
            return Err(crate::LimboError::InternalError(format!(
                "table-function source {source} argument count exceeds hidden columns"
            )));
        }
        let mut argument_predicates = Vec::with_capacity(arguments.len());
        for (argument, column) in arguments.into_iter().zip(hidden_columns) {
            let hidden = self.resolve_atomic_expr(hir::Expr::column(source, column), scope)?;
            // SQLite compares the hidden column with the argument, NULL
            // included, so a NULL argument matches no row. Unary plus keeps
            // the hidden column's affinity from converting the argument.
            let argument = ResolvedScopeExpr {
                expr: hir::Expr::Unary {
                    operator: ast::UnaryOperator::Positive,
                    expr: Box::new(argument.expr),
                },
                has_affinity: false,
                ..argument
            };
            let predicate = self
                .build_scalar_binary_expr(hidden, ast::Operator::Equals, argument)?
                .expr;
            argument_predicates.push(predicate);
        }
        let source = self.source_mut(source).ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing semantic table-function source {source}"
            ))
        })?;
        let hir::SourceKind::TableFunction {
            argument_predicates: bound,
            ..
        } = &mut source.kind
        else {
            return Err(crate::LimboError::InternalError(format!(
                "source {} is not a table function",
                source.id
            )));
        };
        if !bound.is_empty() {
            return Err(crate::LimboError::InternalError(format!(
                "table-function source {} arguments were already bound",
                source.id
            )));
        }
        *bound = argument_predicates;
        Ok(())
    }

    /// Like SQLite, a FULL JOIN whose WHERE clause rejects the NULL row of
    /// one side keeps only the other side's unmatched rows: it runs as a
    /// RIGHT JOIN when a term rejects the right side's NULL row, as a LEFT
    /// JOIN when one rejects the left side's, and as an inner join when
    /// terms reject both. A merged USING column keeps its coalesced value,
    /// which equals the kept side's value on every row left.
    pub(super) fn reduce_full_joins_rejected_by_where(
        &self,
        from: &mut hir::From,
        filter: &hir::Expr,
    ) {
        let mut terms = Vec::new();
        where_conjuncts(filter, &mut terms);
        let rejects = |sources: &[hir::SourceId]| {
            sources.iter().any(|source| {
                terms
                    .iter()
                    .any(|term| crate::translate::semantic_to_plan::rejects_null_row(term, *source))
            })
        };
        let mut left = Vec::new();
        self.collect_from_item_sources(from.first, &mut left);
        for join in &mut from.joins {
            let mut right = Vec::new();
            self.collect_from_item_sources(join.right, &mut right);
            if join.kind == hir::JoinKind::Full {
                join.kind = match (rejects(&left), rejects(&right)) {
                    (true, true) => hir::JoinKind::Inner,
                    (true, false) => hir::JoinKind::Left,
                    (false, true) => hir::JoinKind::Right,
                    (false, false) => hir::JoinKind::Full,
                };
                if let hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) =
                    &mut join.constraint
                {
                    for column in columns {
                        column.value = merged_column_value(join.kind);
                    }
                }
            }
            left.append(&mut right);
        }
    }

    /// `source` and, for a parenthesized join, every source inside it.
    fn collect_from_item_sources(&self, source: hir::SourceId, sources: &mut Vec<hir::SourceId>) {
        sources.push(source);
        if let Some(hir::SourceKind::FromGroup(group)) =
            self.source(source).map(|source| &source.kind)
        {
            for inner in std::iter::once(group.from.first)
                .chain(group.from.joins.iter().map(|join| join.right))
            {
                self.collect_from_item_sources(inner, sources);
            }
        }
    }

    /// A USING operand with the collation its table column supplies, as a
    /// column reference written in an expression has.
    fn using_operand(
        &self,
        operand: ResolvedScopeExpr,
        scope: &Scope,
    ) -> Result<ResolvedScopeExpr> {
        match operand.expr {
            hir::Expr::Column(_) => self.resolve_atomic_expr(operand.expr, scope),
            _ => Ok(operand),
        }
    }

    fn analyze_source_scalar_expr(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        parent: Option<hir::QueryId>,
    ) -> Result<hir::Expr> {
        self.analyze_source_scalar_expr_with_facts(syntax, scope, policy, parent)
            .map(|resolved| resolved.expr)
    }

    fn analyze_source_scalar_expr_with_facts(
        &mut self,
        syntax: &'ast ast::Expr,
        scope: &Scope,
        policy: ExprPolicy,
        parent: Option<hir::QueryId>,
    ) -> Result<ResolvedScopeExpr> {
        match parent {
            Some(parent) => self.analyze_query_scalar_expr(syntax, scope, policy, parent),
            None => self.analyze_root_expr(syntax, scope, policy),
        }
    }

    fn analyze_cte_source(
        &mut self,
        resolution: CteResolution,
        name: &str,
        alias: Option<&str>,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let (cte, recursive, cte_columns) = match resolution {
            CteResolution::Cte(cte) => {
                let definition = self.cte(cte).ok_or_else(|| {
                    crate::LimboError::InternalError(format!("missing semantic CTE {cte}"))
                })?;
                (cte, false, definition.columns.clone())
            }
            CteResolution::RecursiveInput { cte, columns } => (cte, true, columns),
        };
        // Like SQLite, a recursive CTE's reference to itself has no
        // affinity; a read from outside keeps the anchor query's.
        let columns = cte_columns
            .iter()
            .map(|column| hir::SourceColumn {
                name: column.name.clone(),
                type_fact: column.type_fact.clone(),
                affinity: if recursive {
                    Affinity::Blob
                } else {
                    column.affinity
                },
                has_affinity: column.has_affinity && !recursive,
                collation: column.collation.clone(),
                hidden: false,
                rowid_alias: false,
            })
            .collect::<Vec<_>>();
        let width = columns.len();
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database: None,
                name: crate::util::normalize_ident(name),
                alias: alias.map(crate::util::normalize_ident),
                kind: if recursive {
                    hir::SourceKind::RecursiveInput(cte)
                } else {
                    hir::SourceKind::Cte(cte)
                },
                columns,
                generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
                default_expressions: vec![hir::ColumnReadExpression::Absent; width],
                column_type_programs: vec![None; width],
                check_constraints: None,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        if recursive {
            self.record_recursive_input(cte, source)?;
        }
        Ok(source)
    }

    /// `lateral` is the scope of the FROM items on the subquery's left when
    /// the subquery is LATERAL: it reads them like an outer query, and runs
    /// again for each of their rows.
    fn analyze_derived_source(
        &mut self,
        select: &'ast ast::Select,
        alias: Option<&ast::As>,
        context: FromContext<'_>,
        position: usize,
        lateral: Option<&Scope>,
    ) -> Result<hir::SourceId> {
        let owner = context.source_owner();
        let parent = match owner {
            SourceOwner::QueryBlock(block) => Some(block.query),
            SourceOwner::Root => None,
        };
        let query = match lateral {
            Some(left) => {
                // An aggregate of only the left items' columns is an
                // aggregate of the query the FROM clause belongs to, which
                // PostgreSQL rejects: that query cannot aggregate in its
                // own FROM clause.
                let host = match owner {
                    SourceOwner::QueryBlock(block) => Some(block),
                    SourceOwner::Root => None,
                };
                self.aggregate_hosts.push(host);
                let query = self.analyze_select_with_scope(
                    select,
                    parent,
                    Some(left),
                    None,
                    context.expr_policies(),
                );
                self.aggregate_hosts.pop();
                let query = query?;
                if let Some(host) = host {
                    if self.query_has_aggregate_of(query, host) {
                        crate::bail_parse_error!(
                            "aggregate functions are not allowed in FROM clause of their own query level"
                        );
                    }
                }
                query
            }
            None => self.analyze_select_with_scope(
                select,
                parent,
                context.outer_scope(),
                None,
                context.expr_policies(),
            )?,
        };
        let columns = self.query_source_columns(query)?;
        let width = columns.len();
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database: None,
                name: format!("(subquery-{position})"),
                alias: alias
                    .map(ast::As::name)
                    .map(ast::Name::as_str)
                    .map(crate::util::normalize_ident),
                kind: hir::SourceKind::Derived(query),
                columns,
                generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
                default_expressions: vec![hir::ColumnReadExpression::Absent; width],
                column_type_programs: vec![None; width],
                check_constraints: None,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }

    /// The first of `candidates` that `query` or a query nested in it reads.
    /// The first of `candidates` that `expression` reads, directly or
    /// through a subquery written in it.
    fn expr_reads_any_source(
        &self,
        expression: &hir::Expr,
        candidates: &[hir::SourceId],
    ) -> Option<hir::SourceId> {
        if candidates.is_empty() {
            return None;
        }
        let mut found = None;
        expression.for_each(&mut |expression| {
            if found.is_some() {
                return;
            }
            found = match expression {
                hir::Expr::Column(reference) if candidates.contains(&reference.source) => {
                    Some(reference.source)
                }
                hir::Expr::RowId(source) if candidates.contains(source) => Some(*source),
                hir::Expr::Subquery(subquery) => {
                    let query = match subquery {
                        hir::SubqueryExpr::Scalar { query, .. }
                        | hir::SubqueryExpr::Row { query }
                        | hir::SubqueryExpr::In { query, .. }
                        | hir::SubqueryExpr::Exists(query) => *query,
                    };
                    self.query_reads_any_source(query, candidates)
                }
                _ => None,
            };
        });
        found
    }

    fn query_reads_any_source(
        &self,
        query: hir::QueryId,
        candidates: &[hir::SourceId],
    ) -> Option<hir::SourceId> {
        let mut pending = vec![query];
        let mut seen = Vec::new();
        while let Some(id) = pending.pop() {
            if seen.contains(&id) {
                continue;
            }
            seen.push(id);
            let Some(definition) = self.query(id) else {
                continue;
            };
            if let Some(read) = candidates
                .iter()
                .find(|candidate| definition.captures.contains(candidate))
            {
                return Some(*read);
            }
            pending.extend(
                self.queries_with_parent(id)
                    .into_iter()
                    .filter(|nested| !seen.contains(nested)),
            );
        }
        None
    }

    /// Whether an expression of `query`'s own blocks holds an aggregate
    /// that `block`, another query's block, owns.
    fn query_has_aggregate_of(&self, query: hir::QueryId, block: hir::QueryBlockId) -> bool {
        let Some(definition) = self.query(query) else {
            return false;
        };
        let mut found = false;
        let mut check = |expression: &hir::Expr| {
            expression.for_each(&mut |expression| {
                if let hir::Expr::Function(call) = expression {
                    if matches!(
                        &call.evaluation,
                        hir::FunctionEvaluation::Aggregate { id, .. } if id.block == block
                    ) {
                        found = true;
                    }
                }
            });
        };
        for query_block in &definition.blocks {
            for output in &query_block.outputs {
                check(&output.expr);
            }
            match &query_block.body {
                hir::QueryBlockBody::Select {
                    filter, grouping, ..
                } => {
                    if let Some(filter) = filter {
                        check(filter);
                    }
                    if let Some(grouping) = grouping {
                        for key in &grouping.keys {
                            check(key);
                        }
                        if let Some(having) = &grouping.having {
                            check(having);
                        }
                    }
                }
                hir::QueryBlockBody::Values { rows } => {
                    for row in rows {
                        for value in row {
                            check(value);
                        }
                    }
                }
            }
        }
        for term in &definition.order_by {
            check(&term.expr);
        }
        found
    }

    /// Expand a view: its stored SELECT becomes a derived source named after
    /// the view, with the view's column names.
    /// The SELECT resolves against the catalog only, so the statement's CTEs
    /// are hidden while it is analyzed, and it cannot read outer columns.
    fn analyze_view_source(
        &mut self,
        database: hir::DatabaseId,
        view: &'ast crate::schema::View,
        alias: Option<&ast::As>,
        context: FromContext<'_>,
    ) -> Result<hir::SourceId> {
        let name = crate::util::normalize_ident(&view.name);
        if self.views_in_progress.contains(&name) {
            crate::bail_parse_error!("view {} is circularly defined", view.name);
        }
        let owner = context.source_owner();
        self.views_in_progress.push(name.clone());
        let statement_ctes = std::mem::take(&mut self.cte_scopes);
        // A view's body names its tables within the view's own database,
        // never through a TEMP table of the same name (SQLite's
        // sqlite3FixSelect). A TEMP view may name any database.
        let enclosing_view_database = self.view_database.replace(database);
        let query = self.analyze_select_with_scope(
            &view.select_stmt,
            context.query_parent(),
            None,
            None,
            context.expr_policies(),
        );
        self.view_database = enclosing_view_database;
        assert!(
            self.cte_scopes.is_empty(),
            "view analysis leaked a CTE scope"
        );
        self.cte_scopes = statement_ctes;
        self.views_in_progress.pop();
        let query = query?;

        let mut columns = self.query_source_columns(query)?;
        if columns.len() != view.columns.len() {
            return Err(crate::LimboError::InternalError(format!(
                "view {} has {} columns but its SELECT returns {}",
                view.name,
                view.columns.len(),
                columns.len()
            )));
        }
        for (column, view_column) in columns.iter_mut().zip(&view.columns) {
            if let Some(view_name) = &view_column.name {
                column.name.clone_from(view_name);
            }
        }
        let width = columns.len();
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database: None,
                name: name.clone(),
                alias: Some(
                    alias
                        .map(ast::As::name)
                        .map(ast::Name::as_str)
                        .map(crate::util::normalize_ident)
                        .unwrap_or(name),
                ),
                kind: hir::SourceKind::Derived(query),
                columns,
                generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
                default_expressions: vec![hir::ColumnReadExpression::Absent; width],
                column_type_programs: vec![None; width],
                check_constraints: None,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }

    pub(super) fn query_source_columns(
        &self,
        query: hir::QueryId,
    ) -> Result<Vec<hir::SourceColumn>> {
        let query = self.query(query).ok_or_else(|| {
            crate::LimboError::InternalError(format!("missing semantic query {query}"))
        })?;
        let first = query.blocks.first().ok_or_else(|| {
            crate::LimboError::InternalError(format!("semantic query {} has no blocks", query.id))
        })?;
        let mut names: Vec<String> = Vec::with_capacity(first.outputs.len());
        Ok(first
            .outputs
            .iter()
            .enumerate()
            .map(|(position, output)| {
                let (affinity, has_affinity) = if query.blocks.len() > 1 {
                    self.compound_column_affinity(query, position)
                } else {
                    (output.schema_affinity, output.has_affinity)
                };
                let name = unique_subquery_column_name(self.subquery_column_name(output), &names);
                names.push(name.clone());
                hir::SourceColumn {
                    name,
                    type_fact: output.type_fact.clone(),
                    affinity,
                    has_affinity,
                    collation: output.collation.clone(),
                    hidden: false,
                    rowid_alias: false,
                }
            })
            .collect())
    }

    /// The affinity of column `position` of a compound subquery, combined
    /// across its arms like SQLite's `sqlite3SubqueryColumnTypes`:
    /// the first arm with an affinity sets it, and a later arm that can give
    /// a value of the other class (text against numeric) removes it, so the
    /// column compares by storage class.
    fn compound_column_affinity(&self, query: &hir::Query, position: usize) -> (Affinity, bool) {
        compound_column_affinity(query, position, &|source| self.source(source))
    }

    /// Like SQLite, a subquery column without an alias is named after the
    /// column it reads: `(SELECT b.k FROM l AS b)` has a column `k`, not
    /// `b.k`. A rowid read keeps the identifier as written (`rowid`, `oid`,
    /// `_rowid_`), not the rowid alias column, since SQLite names the column
    /// before it resolves the read. Other expressions keep their SQL text.
    fn subquery_column_name(&self, output: &hir::Output) -> String {
        if output.name_kind != hir::OutputNameKind::Inferred {
            return output.name.clone();
        }
        let column_name = match &output.expr {
            hir::Expr::Column(reference) => self
                .source(reference.source)
                .and_then(|source| source.columns.get(reference.column))
                .map(|column| column.name.clone()),
            hir::Expr::RowId(_) => output
                .name
                .rsplit('.')
                .next()
                .map(|written| written.trim_matches(['"', '`', '[', ']']).to_string()),
            _ => None,
        };
        column_name.unwrap_or_else(|| output.name.clone())
    }

    pub(super) fn analyze_base_table_source(
        &mut self,
        name: &ast::QualifiedName,
        alias: Option<&ast::As>,
        indexed: Option<&ast::Indexed>,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let (database, table) = self.resolve_table_in_scope(name)?;
        self.analyze_catalog_table_source(
            &name.name,
            alias.map(ast::As::name).or(name.alias.as_ref()),
            indexed,
            owner,
            database,
            table,
            CatalogSourceKind::Table,
        )
    }

    pub(super) fn analyze_base_table_source_in_database(
        &mut self,
        name: &ast::Name,
        database: hir::DatabaseId,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let table_name = crate::util::normalize_ident(name.as_str());
        let table = self
            .context()
            .schema(database)?
            .get_table(&table_name)
            .ok_or_else(|| LimboError::ParseError(format!("no such table: {table_name}")))?;
        self.analyze_catalog_table_source(
            name,
            None,
            None,
            owner,
            database,
            table,
            CatalogSourceKind::Table,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_catalog_table_source(
        &mut self,
        name: &ast::Name,
        alias: Option<&ast::Name>,
        indexed: Option<&ast::Indexed>,
        owner: SourceOwner,
        database: hir::DatabaseId,
        table: Arc<Table>,
        source_kind: CatalogSourceKind,
    ) -> Result<hir::SourceId> {
        let table_name = crate::util::normalize_ident(name.as_str());
        let table_id =
            self.catalog_object_id(Some(database), CatalogObjectKind::Table, table_name.clone());
        let table = CatalogObject::new(table_id, self.context().snapshot(), Some(database), table);
        let analyzed_columns = self.source_columns(&table)?;
        let mut columns = Vec::with_capacity(analyzed_columns.len());
        let mut column_type_programs = Vec::with_capacity(analyzed_columns.len());
        for analyzed in analyzed_columns {
            columns.push(analyzed.column);
            column_type_programs.push(analyzed.programs);
        }
        let generated_expressions = table
            .value()
            .columns()
            .iter()
            .map(|column| {
                if column.generated_expr().is_some() {
                    hir::ColumnReadExpression::NotRequired
                } else {
                    hir::ColumnReadExpression::Absent
                }
            })
            .collect();
        // A DEFAULT fills a column missing from a short B-tree record. A
        // virtual table returns every column itself.
        let reads_defaults = table.value().btree().is_some();
        let default_expressions = table
            .value()
            .columns()
            .iter()
            .map(|column| {
                if reads_defaults && column.default.is_some() {
                    hir::ColumnReadExpression::NotRequired
                } else {
                    hir::ColumnReadExpression::Absent
                }
            })
            .collect();
        let index_hint = match indexed {
            None => hir::IndexHint::None,
            Some(ast::Indexed::NotIndexed) => hir::IndexHint::NotIndexed,
            Some(ast::Indexed::IndexedBy(name)) => {
                if table.value().btree().is_some_and(|btree| !btree.has_rowid) {
                    crate::bail_parse_error!(
                        "INDEXED BY is not supported on WITHOUT ROWID tables: {}",
                        name.as_str()
                    );
                }
                let index_name = crate::util::normalize_ident(name.as_str());
                let index = self
                    .context()
                    .resolve_index(database, &table_name, name.as_str())?;
                let index_id =
                    self.catalog_object_id(Some(database), CatalogObjectKind::Index, index_name);
                hir::IndexHint::Indexed(CatalogObject::new(
                    index_id,
                    self.context().snapshot(),
                    Some(database),
                    index,
                ))
            }
        };
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database: Some(database),
                name: table_name,
                alias: alias.map(|name| crate::util::normalize_ident(name.as_str())),
                kind: match source_kind {
                    CatalogSourceKind::Table => hir::SourceKind::Table(table.clone()),
                    CatalogSourceKind::TableFunction => hir::SourceKind::TableFunction {
                        table: table.clone(),
                        argument_predicates: Vec::new(),
                    },
                },
                columns,
                generated_expressions,
                default_expressions,
                column_type_programs,
                check_constraints: None,
                rowid_available: table_has_rowid(table.value()),
                index_hint,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        // Every index of a WITHOUT ROWID table also stores the table's
        // primary key columns, which Turso's Index does not model, so a
        // query never reads such a table through an index.
        if matches!(source_kind, CatalogSourceKind::Table)
            && matches!(owner, SourceOwner::QueryBlock(_))
            && table.value().btree().is_some_and(|btree| btree.has_rowid)
        {
            let scope = {
                let definition = self.source(source).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing SELECT table source {source} for index metadata"
                    ))
                })?;
                let mut scope = Scope::default();
                scope.add_schema_source(definition);
                scope
            };
            self.analyze_btree_index_metadata(
                source,
                &table,
                &scope,
                ExprPolicy::schema_expression().with_self_source(source),
            )?;
        }
        Ok(source)
    }

    pub(super) fn source_columns(
        &mut self,
        table: &hir::ResolvedTable,
    ) -> Result<Vec<AnalyzedSourceColumn>> {
        self.source_columns_with(table, true)
    }

    /// The columns of `table`; with `type_programs` false, custom-typed
    /// columns get no programs, for a row that is never decoded.
    pub(super) fn source_columns_with(
        &mut self,
        table: &hir::ResolvedTable,
        type_programs: bool,
    ) -> Result<Vec<AnalyzedSourceColumn>> {
        let is_strict = matches!(table.value(), Table::BTree(table) if table.is_strict);
        let database = table.database().ok_or_else(|| {
            LimboError::InternalError("table source has no owning database".to_string())
        })?;
        let mut columns = Vec::with_capacity(table.value().columns().len());
        for (index, column) in table.value().columns().iter().enumerate() {
            let resolved_type = self
                .context()
                .schema(database)?
                .resolve_type(&column.ty_str, is_strict)?;
            let (type_fact, programs) = match resolved_type {
                Some(resolved) if !type_programs => {
                    let (type_fact, _) =
                        self.freeze_type_fact(&column.ty_str, resolved, column.array_dimensions());
                    (type_fact, None)
                }
                Some(resolved) => {
                    let (type_fact, _) =
                        self.freeze_type_fact(&column.ty_str, resolved, column.array_dimensions());
                    let arguments = column
                        .ty_params
                        .iter()
                        .map(|argument| {
                            self.analyze_resolved_expr(
                                argument,
                                &Scope::default(),
                                ExprPolicy::schema_expression(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let chain = &type_fact
                        .declared
                        .as_ref()
                        .expect("resolved custom column has a declaration")
                        .custom_chain;
                    let mut encode = Vec::new();
                    for definition in chain {
                        if let Some(call) =
                            self.bind_type_transform(definition, &arguments, TypeTransform::Encode)?
                        {
                            encode.push(call);
                        }
                    }
                    let mut decode = Vec::new();
                    for definition in chain.iter().rev() {
                        if let Some(call) =
                            self.bind_type_transform(definition, &arguments, TypeTransform::Decode)?
                        {
                            decode.push(call);
                        }
                    }
                    let encode_nulls = column.array_dimensions() == 0
                        && chain.iter().any(|definition| definition.value().not_null);
                    (
                        type_fact,
                        Some(hir::BoundColumnTypePrograms {
                            encode,
                            decode,
                            encode_nulls,
                        }),
                    )
                }
                None if column.ty_str.is_empty() => (TypeFact::known(column.ty()), None),
                None => (
                    TypeFact::declared(DeclaredType {
                        name: column.ty_str.clone(),
                        storage: column.ty(),
                        custom_chain: Vec::new(),
                        array_dimensions: column.array_dimensions(),
                    }),
                    None,
                ),
            };
            let collation = column.collation_opt().map(|collation| {
                let name = collation.to_string();
                let id = self.catalog_object_id(None, CatalogObjectKind::Collation, name);
                CatalogObject::new(id, self.context().snapshot(), None, Arc::new(collation))
            });
            columns.push(AnalyzedSourceColumn {
                column: hir::SourceColumn {
                    name: column
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("column{}", index + 1)),
                    type_fact,
                    affinity: column.affinity(),
                    has_affinity: true,
                    collation,
                    hidden: column.hidden(),
                    rowid_alias: column.is_rowid_alias(),
                },
                programs,
            });
        }
        Ok(columns)
    }
}

/// Make a subquery column name unique among the columns before it, as
/// SQLite's `sqlite3ColumnsFromExprList` does: a repeated name gets a `:N`
/// suffix, counting from 1, and a name that already ends in `:N` counts
/// from its base, so `SELECT a, a, a` has columns `a`, `a:1`, `a:2`.
fn unique_subquery_column_name(name: String, earlier: &[String]) -> String {
    let taken = |candidate: &str| {
        earlier
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(candidate))
    };
    if !taken(&name) {
        return name;
    }
    let base = match name.rfind(':') {
        Some(colon) if colon > 0 && name[colon + 1..].chars().all(|c| c.is_ascii_digit()) => {
            &name[..colon]
        }
        _ => name.as_str(),
    };
    let mut count = 1;
    loop {
        let candidate = format!("{base}:{count}");
        if !taken(&candidate) {
            return candidate;
        }
        count += 1;
    }
}

fn join_kind(operator: ast::JoinOperator) -> hir::JoinKind {
    match operator {
        ast::JoinOperator::Comma => hir::JoinKind::Comma,
        ast::JoinOperator::TypedJoin(None) => hir::JoinKind::Inner,
        ast::JoinOperator::TypedJoin(Some(kind)) if is_full_join(kind) => hir::JoinKind::Full,
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::RIGHT) => {
            hir::JoinKind::Right
        }
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::LEFT) => {
            hir::JoinKind::Left
        }
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::CROSS) => {
            hir::JoinKind::Cross
        }
        ast::JoinOperator::TypedJoin(Some(_)) => hir::JoinKind::Inner,
    }
}

fn is_full_join(kind: ast::JoinType) -> bool {
    let left = kind.contains(ast::JoinType::LEFT);
    let right = kind.contains(ast::JoinType::RIGHT);
    let outer = kind.contains(ast::JoinType::OUTER);
    (left && right) || (outer && !left && !right)
}

/// The terms a WHERE clause ANDs together.
fn where_conjuncts<'e>(expression: &'e hir::Expr, terms: &mut Vec<&'e hir::Expr>) {
    match expression {
        hir::Expr::Binary {
            lhs,
            operator: ast::Operator::And,
            rhs,
            ..
        } => {
            where_conjuncts(lhs, terms);
            where_conjuncts(rhs, terms);
        }
        _ => terms.push(expression),
    }
}

fn merged_column_value(kind: hir::JoinKind) -> hir::MergedColumnValue {
    match kind {
        hir::JoinKind::Right => hir::MergedColumnValue::Right,
        hir::JoinKind::Full => hir::MergedColumnValue::Coalesce,
        hir::JoinKind::Comma
        | hir::JoinKind::Inner
        | hir::JoinKind::Cross
        | hir::JoinKind::Left
        | hir::JoinKind::Semi
        | hir::JoinKind::Anti => hir::MergedColumnValue::Left,
    }
}

fn is_natural_join(operator: ast::JoinOperator) -> bool {
    matches!(
        operator,
        ast::JoinOperator::TypedJoin(Some(kind)) if kind.contains(ast::JoinType::NATURAL)
    )
}

pub(super) fn table_has_rowid(table: &Table) -> bool {
    table.btree().is_some_and(|table| table.has_rowid) || table.virtual_table().is_some()
}
