//! Query-source conversion into HIR arenas and scopes.

use turso_parser::ast;

use super::{
    analyze::{output_from_resolved, Analyzer, CatalogObjectKind},
    cte::{CteBindingContext, CteResolution},
    expr::{build_using_column, ExprPolicies, ExprPolicy},
    hir::{self, CatalogObject, DeclaredType, SourceOwner, TypeFact},
    schema_program::TypeTransform,
    scope::{resolve_source_column, ResolvedScopeExpr, Scope},
};
use crate::{schema::Table, sync::Arc, LimboError, Result};

struct AnalyzedTableSource<'ast> {
    id: hir::SourceId,
    function_arguments: Option<&'ast [Box<ast::Expr>]>,
    qualified_scope: Option<Scope>,
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
    Root,
}

impl<'scope> FromContext<'scope> {
    fn source_owner(self) -> SourceOwner {
        match self {
            Self::QueryBlock { block, .. } => SourceOwner::QueryBlock(block),
            Self::Dml { .. } | Self::Root => SourceOwner::Root,
        }
    }

    fn query_parent(self) -> Option<hir::QueryId> {
        match self {
            Self::QueryBlock { block, .. } => Some(block.query),
            Self::Dml { .. } | Self::Root => None,
        }
    }

    fn outer_scope(self) -> Option<&'scope Scope> {
        match self {
            Self::QueryBlock { outer_scope, .. } => outer_scope,
            Self::Dml { outer_scope, .. } => outer_scope,
            Self::Root => None,
        }
    }

    fn cte_context(self, dqs_dml: super::context::DoubleQuotedDml) -> CteBindingContext<'scope> {
        match self {
            Self::QueryBlock {
                block,
                outer_scope,
                policies,
            } => CteBindingContext::new(block.query, outer_scope, policies),
            Self::Dml { policies, .. } => CteBindingContext::root(policies),
            Self::Root => CteBindingContext::root(ExprPolicies::statement(dqs_dml)),
        }
    }

    fn expr_policies(self, dqs_dml: super::context::DoubleQuotedDml) -> ExprPolicies {
        match self {
            Self::QueryBlock { policies, .. } => policies,
            Self::Dml { policies, .. } => policies,
            Self::Root => ExprPolicies::statement(dqs_dml),
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
        let first = self.analyze_table_source(&syntax.select, context, 0)?;
        let source = first.id;
        let mut table_functions = Vec::new();
        if let Some(arguments) = first.function_arguments {
            table_functions.push((source, arguments));
        }
        let mut scope = Scope::new(context.outer_scope().cloned());
        let definition = self.source(source).ok_or_else(|| {
            crate::LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        scope.add_source(definition, true);
        if let Some(qualified_scope) = first.qualified_scope {
            scope.append_qualified_sources(qualified_scope);
        }

        let mut joins = Vec::with_capacity(syntax.joins.len());
        for syntax_join in &syntax.joins {
            let kind = join_kind(syntax_join.operator);
            if kind == hir::JoinKind::Right && !joins.is_empty() {
                crate::bail_parse_error!(
                    "RIGHT JOIN following another join is not yet supported. \
                     Try rewriting as LEFT JOIN or using a subquery."
                );
            }
            let natural = is_natural_join(syntax_join.operator);
            if natural && syntax_join.constraint.is_some() {
                crate::bail_parse_error!("a NATURAL join may not have an ON or USING clause");
            }
            let analyzed =
                self.analyze_table_source(&syntax_join.table, context, joins.len() + 1)?;
            let right = analyzed.id;
            if let Some(arguments) = analyzed.function_arguments {
                table_functions.push((right, arguments));
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
                            let left = if natural {
                                scope.resolve_natural_left(&name)?
                            } else {
                                scope.resolve_using_left(&name)?
                            };
                            let right = resolve_source_column(definition, &name)?;
                            build_using_column(name, left, right, merged_column_value(kind))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;

            scope.add_source(definition, true);
            if let Some(qualified_scope) = analyzed.qualified_scope {
                scope.append_qualified_sources(qualified_scope);
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

        for (source, arguments) in table_functions {
            self.analyze_table_function_arguments(
                source,
                arguments,
                &scope,
                context.query_parent(),
                context.expr_policies(self.context().dqs_dml()),
            )?;
        }

        let policy = context.expr_policies(self.context().dqs_dml()).select();
        for (syntax_join, join) in syntax.joins.iter().zip(&mut joins) {
            if let Some(ast::JoinConstraint::On(expression)) = &syntax_join.constraint {
                join.constraint = hir::JoinConstraint::On(self.analyze_source_scalar_expr(
                    expression,
                    &scope,
                    policy,
                    context.query_parent(),
                )?);
            }
        }

        Ok((
            hir::From {
                first: source,
                joins,
            },
            scope,
        ))
    }

    fn analyze_table_source(
        &mut self,
        syntax: &'ast ast::SelectTable,
        context: FromContext<'_>,
        position: usize,
    ) -> Result<AnalyzedTableSource<'ast>> {
        let owner = context.source_owner();
        let cte_context = context.cte_context(self.context().dqs_dml());
        match syntax {
            ast::SelectTable::Table(name, alias, indexed) => {
                if name.db_name.is_none() {
                    if let Some(cte) = self.resolve_cte(name.name.as_str(), cte_context)? {
                        if let Some(ast::Indexed::IndexedBy(index)) = indexed {
                            crate::bail_parse_error!("no such index: {}", index.as_str());
                        }
                        let id = self.analyze_cte_source(
                            cte,
                            name.name.as_str(),
                            alias
                                .as_ref()
                                .map(ast::As::name)
                                .or(name.alias.as_ref())
                                .map(ast::Name::as_str),
                            owner,
                        )?;
                        return Ok(AnalyzedTableSource {
                            id,
                            function_arguments: None,
                            qualified_scope: None,
                        });
                    }
                }
                let id =
                    self.analyze_base_table_source(name, alias.as_ref(), indexed.as_ref(), owner)?;
                Ok(AnalyzedTableSource {
                    id,
                    function_arguments: None,
                    qualified_scope: None,
                })
            }
            ast::SelectTable::TableCall(name, arguments, alias) => self
                .analyze_table_function_source(name, arguments, alias.as_ref(), owner, cte_context),
            ast::SelectTable::Select(select, alias) => {
                let id = self.analyze_derived_source(select, alias.as_ref(), context, position)?;
                Ok(AnalyzedTableSource {
                    id,
                    function_arguments: None,
                    qualified_scope: None,
                })
            }
            ast::SelectTable::Sub(from, alias) => {
                self.analyze_from_group(from, alias.as_ref(), context, position)
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
        if name.db_name.is_none() {
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
                    qualified_scope: None,
                });
            }
        }

        let (database, table) = self.context().resolve_table(name)?;
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
            qualified_scope: None,
        })
    }

    fn analyze_from_group(
        &mut self,
        syntax: &'ast ast::FromClause,
        alias: Option<&ast::As>,
        context: FromContext<'_>,
        position: usize,
    ) -> Result<AnalyzedTableSource<'ast>> {
        let (from, inner_scope) = self.analyze_from_clause(syntax, context)?;
        let group_columns = inner_scope.group_columns();
        let width = group_columns.len();
        let columns = group_columns
            .iter()
            .map(|column| hir::SourceColumn {
                name: column.name.clone(),
                type_fact: column.resolved.type_fact.clone(),
                affinity: column.resolved.affinity,
                has_affinity: column.resolved.has_affinity,
                collation: column.resolved.collation.value().cloned(),
                hidden: column.hidden,
                rowid_alias: false,
            })
            .collect();
        let values = group_columns
            .into_iter()
            .map(|column| column.resolved.expr)
            .collect();
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner: context.source_owner(),
                database: None,
                name: format!("(from-group-{position})"),
                alias: alias
                    .map(ast::As::name)
                    .map(ast::Name::as_str)
                    .map(crate::util::normalize_ident),
                kind: hir::SourceKind::FromGroup(hir::FromGroup {
                    from: Box::new(from),
                    columns: values,
                }),
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
            qualified_scope: alias.is_none().then_some(inner_scope),
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
            let predicate = if matches!(&argument.expr, hir::Expr::Literal(ast::Literal::Null)) {
                hir::Expr::IsNull(Box::new(hidden.expr))
            } else {
                self.build_scalar_binary_expr(hidden, ast::Operator::Equals, argument)?
                    .expr
            };
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
        let columns = cte_columns
            .iter()
            .map(|column| hir::SourceColumn {
                name: column.name.clone(),
                type_fact: column.type_fact.clone(),
                affinity: column.affinity,
                has_affinity: column.has_affinity,
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

    fn analyze_derived_source(
        &mut self,
        select: &'ast ast::Select,
        alias: Option<&ast::As>,
        context: FromContext<'_>,
        position: usize,
    ) -> Result<hir::SourceId> {
        let owner = context.source_owner();
        let parent = match owner {
            SourceOwner::QueryBlock(block) => Some(block.query),
            SourceOwner::Root => None,
            SourceOwner::Cte(cte) => {
                return Err(crate::LimboError::InternalError(format!(
                    "derived source cannot be owned directly by CTE {cte}"
                )));
            }
        };
        let query = self.analyze_select_with_scope(
            select,
            parent,
            context.outer_scope(),
            None,
            context.expr_policies(self.context().dqs_dml()),
        )?;
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
        Ok(first
            .outputs
            .iter()
            .map(|output| hir::SourceColumn {
                name: output.name.clone(),
                type_fact: output.type_fact.clone(),
                affinity: output.schema_affinity,
                has_affinity: output.has_affinity,
                collation: output.collation.clone(),
                hidden: false,
                rowid_alias: false,
            })
            .collect())
    }

    pub(super) fn analyze_base_table_source(
        &mut self,
        name: &ast::QualifiedName,
        alias: Option<&ast::As>,
        indexed: Option<&ast::Indexed>,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let (database, table) = self.context().resolve_table(name)?;
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
        let default_expressions = table
            .value()
            .columns()
            .iter()
            .map(|column| {
                if column.default.is_some() {
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
        if matches!(source_kind, CatalogSourceKind::Table)
            && matches!(owner, SourceOwner::QueryBlock(_))
            && table.value().btree().is_some()
        {
            let scope = {
                let definition = self.source(source).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing SELECT table source {source} for index metadata"
                    ))
                })?;
                let mut scope = Scope::default();
                scope.add_source(definition, true);
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
                    affinity: column.affinity_with_strict(is_strict),
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

fn merged_column_value(kind: hir::JoinKind) -> hir::MergedColumnValue {
    match kind {
        hir::JoinKind::Right => hir::MergedColumnValue::Right,
        hir::JoinKind::Full => hir::MergedColumnValue::Coalesce,
        hir::JoinKind::Comma
        | hir::JoinKind::Inner
        | hir::JoinKind::Cross
        | hir::JoinKind::Left => hir::MergedColumnValue::Left,
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
