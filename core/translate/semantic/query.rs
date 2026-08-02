//! Query, FROM-clause, and row-source semantic analysis.

use turso_parser::ast;

use super::{
    cte_bindings::CteState,
    expr::ExprPolicy,
    hir::{self, CatalogObject, DatabaseId, SourceOwner},
    scope::{QueryEnvironment, Scope},
    Analyzer, CatalogObjectKind,
};
use crate::{
    schema::{Index, Table, Type},
    sync::Arc,
    vdbe::affinity::Affinity,
    LimboError, Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IndexMetadataMode {
    Read,
    Dml,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FromAnalysisKind {
    Query,
    Update,
}

enum IndexPatternError {
    /// The stored pattern cannot describe an optimizer candidate. An
    /// unhinted read may omit that index; forced use and DML still surface it.
    Unusable(LimboError),
    Fatal(LimboError),
}

impl IndexPatternError {
    const fn may_omit_index(&self) -> bool {
        matches!(self, Self::Unusable(_))
    }

    fn into_limbo_error(self) -> LimboError {
        match self {
            Self::Unusable(error) | Self::Fatal(error) => error,
        }
    }
}

impl From<LimboError> for IndexPatternError {
    fn from(error: LimboError) -> Self {
        if matches!(&error, LimboError::ParseError(_)) {
            Self::Unusable(error)
        } else {
            Self::Fatal(error)
        }
    }
}

impl Analyzer<'_, '_> {
    /// Analyze one SELECT and every query it owns into this document's arenas.
    pub(crate) fn analyze_query(
        &mut self,
        syntax: &ast::Select,
        environment: QueryEnvironment,
    ) -> Result<hir::QueryId> {
        self.analyze_select_query(syntax, environment)
    }

    pub(crate) fn analyze_query_with_expected_types(
        &mut self,
        syntax: &ast::Select,
        environment: QueryEnvironment,
        expected_types: &[Option<hir::ResolvedType>],
    ) -> Result<hir::QueryId> {
        self.analyze_select_query(
            syntax,
            environment.with_expected_output_types(expected_types.to_vec()),
        )
    }

    pub(crate) fn analyze_query_with_inputs(
        &mut self,
        syntax: &ast::Select,
        environment: QueryEnvironment,
        expected_types: &[Option<hir::ResolvedType>],
        expected_defaults: &[Option<hir::Expr>],
    ) -> Result<hir::QueryId> {
        self.analyze_select_query(
            syntax,
            environment
                .with_expected_output_types(expected_types.to_vec())
                .with_expected_defaults(expected_defaults.to_vec()),
        )
    }

    /// Add a statement or query WITH clause without eagerly analyzing unused CTEs.
    pub(crate) fn prepare_query_environment(
        &self,
        mut environment: QueryEnvironment,
        with: Option<&ast::With>,
    ) -> Result<QueryEnvironment> {
        environment.ctes = environment
            .ctes
            .with_clause(with, environment.outer.clone())?;
        Ok(environment)
    }

    /// Build the inherited part of a scope: outer query, CTE bindings, and
    /// trigger/DML pseudo-tables.
    pub(crate) fn scope_for_environment(&self, environment: &QueryEnvironment) -> Result<Scope> {
        let mut scope = Scope::new(environment.outer.clone());
        scope.set_ctes(environment.ctes.clone());
        scope.add_environment_pseudo_sources(environment, |source| self.source(source))?;
        Ok(scope)
    }

    /// Add a previously registered source to a scope.
    pub(crate) fn add_source_to_scope(
        &self,
        scope: &mut Scope,
        source: hir::SourceId,
        unqualified: bool,
    ) -> Result<()> {
        let source = self.source(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        scope.add_source(source, unqualified);
        Ok(())
    }

    /// Resolve and register a real schema table. CTEs, views, and derived
    /// sources use separate query-only paths.
    pub(crate) fn analyze_base_table_source(
        &mut self,
        name: &ast::QualifiedName,
        alias: Option<&ast::Name>,
        indexed: Option<&ast::Indexed>,
        owner: SourceOwner,
        index_mode: IndexMetadataMode,
    ) -> Result<hir::SourceId> {
        let (database_id, table) = self.context().resolve_table(name)?;
        let table_name = crate::util::normalize_ident(name.name.as_str());
        let table = match table {
            Some(table) => table,
            None => {
                let schema = self.context().schema(database_id).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "database {database_id} disappeared during semantic analysis"
                    ))
                })?;
                if schema.incompatible_views.contains(&table_name) {
                    use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
                    crate::bail_parse_error!(
                        "Materialized view '{}' has an incompatible version. \n\
                         The view was created with a different DBSP version than the current version ({}). \n\
                         Please DROP and recreate the view to use it.",
                        table_name,
                        DBSP_CIRCUIT_VERSION
                    );
                }
                if schema.broken_views.contains(&table_name) {
                    crate::bail_parse_error!(
                        "view '{}' could not be loaded: its SQL in sqlite_schema does not parse. \n\
                         Use DROP VIEW to remove it, then recreate it.",
                        table_name
                    );
                }
                crate::bail_parse_error!("no such table: {table_name}");
            }
        };
        let object_id = self.catalog_object_id(
            Some(database_id),
            CatalogObjectKind::Table,
            table_name.clone(),
        );
        let table = CatalogObject::new(
            object_id,
            self.context().snapshot(),
            Some(DatabaseId::new(database_id)),
            table,
        );
        let columns = self.source_columns_for_table(&table)?;
        let index_hint = self.resolve_index_hint(&table, indexed)?;
        let source_id = self.reserve_source();
        self.insert_source(
            source_id,
            hir::Source {
                id: source_id,
                owner,
                database: Some(DatabaseId::new(database_id)),
                name: table_name,
                alias: alias
                    .or(name.alias.as_ref())
                    .map(|name| crate::util::normalize_ident(name.as_str())),
                kind: hir::SourceKind::Table(table.clone()),
                generated_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                default_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                column_type_programs: vec![None; columns.len()],
                check_constraints: Vec::new(),
                columns,
                rowid_available: table_has_rowid(table.value()),
                index_hint,
                index_expressions: Vec::new(),
                index_method_patterns: Vec::new(),
            },
        )?;
        self.initialize_table_read_expression_slots(source_id, table.value())?;
        self.populate_table_index_metadata(source_id, database_id, &table, index_mode)?;
        Ok(source_id)
    }

    /// Register NEW, OLD, or excluded against an already resolved table.
    pub(crate) fn analyze_pseudo_source(
        &mut self,
        kind: hir::PseudoSource,
        table: hir::ResolvedTable,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let columns = self.source_columns_for_table(&table)?;
        let database = table.database();
        let source_id = self.reserve_source();
        self.insert_source(
            source_id,
            hir::Source {
                id: source_id,
                owner,
                database,
                name: pseudo_source_name(kind).to_string(),
                alias: None,
                kind: hir::SourceKind::Pseudo {
                    kind,
                    table: table.clone(),
                },
                generated_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                default_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                column_type_programs: vec![None; columns.len()],
                check_constraints: Vec::new(),
                columns,
                rowid_available: table_has_rowid(table.value()),
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source_id)
    }

    /// Return the table handle behind a base source for DML and trigger setup.
    pub(crate) fn resolved_source_table(
        &self,
        source: hir::SourceId,
    ) -> Result<hir::ResolvedTable> {
        let source = self.source(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        match &source.kind {
            hir::SourceKind::Table(table)
            | hir::SourceKind::TableFunction { table, .. }
            | hir::SourceKind::Pseudo { table, .. } => Ok(table.clone()),
            _ => Err(LimboError::InternalError(format!(
                "semantic source {source:?} is not a schema table"
            ))),
        }
    }

    /// Analyze an UPDATE FROM clause and return both its HIR and its final name scope.
    pub(crate) fn analyze_update_from(
        &mut self,
        syntax: &ast::FromClause,
        owner: SourceOwner,
        environment: &QueryEnvironment,
    ) -> Result<(hir::From, Scope)> {
        self.analyze_from_clause_with_kind(syntax, owner, environment, FromAnalysisKind::Update)
    }

    /// Resolve a SELECT/RETURNING style projection against an existing scope.
    pub(crate) fn analyze_returning(
        &mut self,
        syntax: &[ast::ResultColumn],
        scope: &Scope,
    ) -> Result<hir::Returning> {
        self.analyze_returning_with_policy(syntax, scope, ExprPolicy::returning())
    }

    pub(crate) fn analyze_returning_with_policy(
        &mut self,
        syntax: &[ast::ResultColumn],
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<hir::Returning> {
        let outputs =
            self.analyze_result_columns(syntax, scope, hir::OutputOwner::Root, policy, &[])?;
        Ok(hir::Returning { outputs })
    }

    fn source_columns_for_table(
        &mut self,
        table: &hir::ResolvedTable,
    ) -> Result<Vec<hir::SourceColumn>> {
        let database_id = table.database().map(DatabaseId::index).ok_or_else(|| {
            LimboError::InternalError(format!(
                "resolved table {} has no database identity",
                table.value().get_name()
            ))
        })?;
        let mut columns = Vec::with_capacity(table.value().columns().len());
        let is_strict = matches!(table.value(), Table::BTree(table) if table.is_strict);
        for (index, column) in table.value().columns().iter().enumerate() {
            let type_fact = self.table_column_type_fact(column, database_id, is_strict)?;
            columns.push(hir::SourceColumn {
                name: column
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("column{}", index + 1)),
                type_fact,
                affinity: column.affinity_with_strict(is_strict),
                has_affinity: true,
                collation: column
                    .collation_opt()
                    .map(|collation| self.resolve_collation(&collation.to_string()))
                    .transpose()?,
                hidden: column.hidden(),
                rowid_alias: column.is_rowid_alias(),
            });
        }
        Ok(columns)
    }

    /// Record which source columns own stored read expressions without
    /// compiling them. Unused unresolved schema text must remain dormant.
    fn initialize_table_read_expression_slots(
        &mut self,
        source: hir::SourceId,
        table: &Table,
    ) -> Result<()> {
        let generated_expressions = table
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

        let definition = self.source_mut(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        definition.generated_expressions = generated_expressions;
        definition.default_expressions = default_expressions;
        Ok(())
    }

    /// Close all stored column expressions needed by this statement. A
    /// generated expression may require another generated column, so newly
    /// discovered dependencies are processed to a fixed point.
    pub(super) fn materialize_required_source_expressions(
        &mut self,
        root: &hir::HirRoot,
    ) -> Result<()> {
        // Writable targets are complete row images. INSERT and UPDATE build a
        // full NEW record, while DELETE may cache the full OLD record for
        // attached triggers decided during emission. Until row-image demand
        // itself is represented in HIR, every target field must be closed.
        let dml_target = match root {
            hir::HirRoot::Insert(insert) => Some(insert.target),
            hir::HirRoot::Update(update) => Some(update.target),
            hir::HirRoot::Delete(delete) => Some(delete.target),
            hir::HirRoot::Query(_) | hir::HirRoot::TriggerPredicate(_) => None,
        };
        if let Some(target) = dml_target {
            let column_count = self
                .source(target)
                .ok_or_else(|| {
                    LimboError::InternalError(format!("missing DML target source {target}"))
                })?
                .columns
                .len();
            for column in 0..column_count {
                self.require_source_column(target, column);
            }
        }

        let mut processed = rustc_hash::FxHashSet::default();
        loop {
            let next = self
                .required_source_columns
                .iter()
                .filter(|required| !processed.contains(*required))
                .min_by_key(|(source, column)| (source.index(), *column))
                .copied();
            let Some((source, column)) = next else {
                break;
            };
            processed.insert((source, column));
            self.materialize_required_source_column(source, column)?;
        }
        Ok(())
    }

    fn materialize_required_source_column(
        &mut self,
        source: hir::SourceId,
        column: usize,
    ) -> Result<()> {
        let (table, type_fact, type_programs_bound, generated_state, default_state) = {
            let definition = self.source(source).ok_or_else(|| {
                LimboError::InternalError(format!("missing semantic source {source}"))
            })?;
            if column >= definition.columns.len() {
                return Err(LimboError::InternalError(format!(
                    "semantic source {source} has no required column {column}"
                )));
            }
            if definition.generated_expressions.len() != definition.columns.len()
                || definition.default_expressions.len() != definition.columns.len()
                || definition.column_type_programs.len() != definition.columns.len()
            {
                return Err(LimboError::InternalError(format!(
                    "read-program metadata for source {source} is not aligned with its columns"
                )));
            }
            let table = match &definition.kind {
                hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => {
                    table.handle()
                }
                hir::SourceKind::SchemaExpression
                | hir::SourceKind::Cte(_)
                | hir::SourceKind::Derived(_)
                | hir::SourceKind::RecursiveInput(_)
                | hir::SourceKind::Pseudo { .. } => return Ok(()),
            };
            (
                table,
                definition.columns[column].type_fact.clone(),
                definition.column_type_programs[column].is_some(),
                definition.generated_expressions[column].clone(),
                definition.default_expressions[column].clone(),
            )
        };

        let schema_column = table.columns().get(column).cloned().ok_or_else(|| {
            LimboError::InternalError(format!(
                "catalog table '{}' has no source column {column}",
                table.get_name()
            ))
        })?;
        // The outer option says whether this pass bound the column. The inner
        // option is the aligned HIR slot: built-in types intentionally keep it empty.
        let type_programs_update = if type_programs_bound {
            None
        } else {
            Some(self.bind_column_type_programs(&schema_column, &type_fact, table.get_name())?)
        };
        let generated = self.instantiate_required_column_read_expression(
            source,
            column,
            "generated",
            schema_column.generated_expr(),
            generated_state,
        )?;
        let default = self.instantiate_required_column_read_expression(
            source,
            column,
            "default",
            schema_column.default.as_deref(),
            default_state,
        )?;

        if generated.is_none() && default.is_none() && type_programs_update.is_none() {
            return Ok(());
        }
        let definition = self.source_mut(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        if let Some(expression) = generated {
            definition.generated_expressions[column] =
                hir::ColumnReadExpression::Planned(expression);
        }
        if let Some(expression) = default {
            definition.default_expressions[column] = hir::ColumnReadExpression::Planned(expression);
        }
        if let Some(programs) = type_programs_update {
            definition.column_type_programs[column] = programs;
        }
        Ok(())
    }

    fn instantiate_required_column_read_expression(
        &mut self,
        source: hir::SourceId,
        column: usize,
        kind: &str,
        stored: Option<&crate::schema_expr::SchemaExpr>,
        state: hir::ColumnReadExpression,
    ) -> Result<Option<hir::Expr>> {
        match (stored, state) {
            (None, hir::ColumnReadExpression::Absent)
            | (Some(_), hir::ColumnReadExpression::Planned(_)) => Ok(None),
            (Some(stored), hir::ColumnReadExpression::NotRequired) => {
                self.instantiate_column_schema_expr(stored, source, column)
                    .map(Some)
            }
            (None, hir::ColumnReadExpression::NotRequired)
            | (None, hir::ColumnReadExpression::Planned(_))
            | (Some(_), hir::ColumnReadExpression::Absent) => {
                Err(LimboError::InternalError(format!(
                    "{kind} expression state for source {source} column {column} does not match the catalog"
                )))
            }
        }
    }

    /// Instantiate CHECK constraints only for a table being written. Reads do
    /// not need CHECK metadata, and ignored constraints must not make leniently
    /// loaded schema text fail semantic analysis.
    pub(super) fn populate_dml_check_constraints(
        &mut self,
        source: hir::SourceId,
        table: &Table,
    ) -> Result<()> {
        if self.context().dml_policy().check_constraints_ignored() {
            return Ok(());
        }
        let Table::BTree(table) = table else {
            return Ok(());
        };
        let column_names = table
            .columns()
            .iter()
            .map(|column| {
                column.name.as_deref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "table '{}' has an unnamed column in CHECK constraint analysis",
                        table.name
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut check_constraints = Vec::with_capacity(table.check_constraints.len());
        for check in &table.check_constraints {
            let description = match &check.name {
                Some(name) => name.clone(),
                None => check.expr.render(&column_names)?,
            };
            let expression = self.instantiate_table_schema_expr(&check.expr, source)?;
            check_constraints.push(hir::CheckConstraint {
                expression,
                description,
            });
        }
        let definition = self.source_mut(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        definition.check_constraints = check_constraints;
        Ok(())
    }

    /// Instantiate the index programs this source may use. Ordinary reads
    /// omit wholly unresolved indexes, while a forced index and DML index
    /// maintenance require their stored expressions to be valid.
    pub(super) fn populate_table_index_metadata(
        &mut self,
        source: hir::SourceId,
        database_id: usize,
        table: &hir::ResolvedTable,
        mode: IndexMetadataMode,
    ) -> Result<()> {
        let index_hint = self
            .source(source)
            .ok_or_else(|| LimboError::InternalError(format!("missing semantic source {source}")))?
            .index_hint
            .clone();
        if mode == IndexMetadataMode::Read && matches!(&index_hint, hir::IndexHint::NotIndexed) {
            let definition = self.source_mut(source).ok_or_else(|| {
                LimboError::InternalError(format!("missing semantic source {source}"))
            })?;
            definition.index_expressions.clear();
            definition.index_method_patterns.clear();
            return Ok(());
        }

        let indexes: Vec<Arc<Index>> = self
            .context()
            .schema(database_id)
            .ok_or(LimboError::SchemaUpdated)?
            .get_indices(table.value().get_name())
            .cloned()
            .collect();
        let mut index_expressions = Vec::with_capacity(indexes.len());
        let mut index_method_patterns = Vec::new();

        for index in indexes {
            let resolved_index = self.resolved_index(database_id, index.clone());
            if mode == IndexMetadataMode::Read {
                if let hir::IndexHint::Indexed(required) = &index_hint {
                    if &resolved_index != required {
                        continue;
                    }
                }
                if matches!(&index_hint, hir::IndexHint::None)
                    && (index.columns.iter().any(|column| {
                        column
                            .expr
                            .as_deref()
                            .is_some_and(|expression| expression.as_unresolved().is_some())
                    }) || index
                        .where_clause
                        .as_deref()
                        .is_some_and(|predicate| predicate.as_unresolved().is_some()))
                {
                    continue;
                }
            }

            // Validate the complete index before instantiating any part. An
            // omitted index must never leave partial semantic dependencies.
            let stored_columns = index
                .columns
                .iter()
                .map(|column| {
                    column
                        .expr
                        .as_deref()
                        .map(|expr| expr.as_valid())
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let stored_predicate = index
                .where_clause
                .as_deref()
                .map(|expr| expr.as_valid())
                .transpose()?;
            let may_omit_invalid_pattern =
                mode == IndexMetadataMode::Read && matches!(&index_hint, hir::IndexHint::None);
            let mut prior_requirements =
                may_omit_invalid_pattern.then(|| std::mem::take(&mut self.required_source_columns));
            if mode == IndexMetadataMode::Dml {
                for index_column in &index.columns {
                    if index_column.pos_in_table != crate::schema::EXPR_INDEX_SENTINEL {
                        self.require_source_column(source, index_column.pos_in_table);
                    }
                }
            }

            let columns = stored_columns
                .into_iter()
                .map(|expression| {
                    expression
                        .map(|expression| self.instantiate_schema_expr(expression, source))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let predicate = stored_predicate
                .map(|predicate| self.instantiate_schema_expr(predicate, source))
                .transpose()?;
            let pattern_result = if let Some(index_method) = &index.index_method {
                if !index.is_backing_btree_index() {
                    index_method
                        .definition()
                        .patterns
                        .iter()
                        .enumerate()
                        .map(|(pattern, syntax)| {
                            self.analyze_index_method_pattern(
                                syntax,
                                source,
                                &resolved_index,
                                pattern,
                            )
                        })
                        .collect::<std::result::Result<Vec<_>, IndexPatternError>>()
                } else {
                    Ok(Vec::new())
                }
            } else {
                Ok(Vec::new())
            };
            let patterns = match pattern_result {
                Ok(patterns) => patterns,
                Err(error) => {
                    let may_omit = error.may_omit_index();
                    if let Some(prior) = prior_requirements.take() {
                        self.required_source_columns = prior;
                        if may_omit {
                            continue;
                        }
                    }
                    return Err(error.into_limbo_error());
                }
            };
            if let Some(mut prior) = prior_requirements.take() {
                prior.extend(std::mem::take(&mut self.required_source_columns));
                self.required_source_columns = prior;
            }
            index_expressions.push(hir::IndexExpressions {
                index: resolved_index,
                columns,
                predicate,
            });
            index_method_patterns.extend(patterns);
        }

        let definition = self.source_mut(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        definition.index_expressions = index_expressions;
        definition.index_method_patterns = index_method_patterns;
        Ok(())
    }

    fn resolved_index(&mut self, database_id: usize, index: Arc<Index>) -> hir::ResolvedIndex {
        let object_id = self.catalog_object_id(
            Some(database_id),
            CatalogObjectKind::Index,
            crate::util::normalize_ident(&index.name),
        );
        CatalogObject::new(
            object_id,
            self.context().snapshot(),
            Some(DatabaseId::new(database_id)),
            index,
        )
    }

    fn analyze_index_method_pattern(
        &mut self,
        syntax: &ast::Select,
        source: hir::SourceId,
        index: &hir::ResolvedIndex,
        pattern: usize,
    ) -> std::result::Result<hir::IndexMethodPattern, IndexPatternError> {
        let invalid = |message: &str| {
            IndexPatternError::Unusable(LimboError::InternalError(format!(
                "index method pattern {pattern} for '{}' {message}",
                index.value().name
            )))
        };
        if syntax.with.is_some() || !syntax.body.compounds.is_empty() {
            return Err(invalid("must be a single SELECT"));
        }
        let ast::OneSelect::Select {
            columns,
            from: Some(ast::FromClause { select, joins }),
            distinctness: None,
            where_clause,
            group_by: None,
            window_clause,
        } = &syntax.body.select
        else {
            return Err(invalid("has an unsupported SELECT body"));
        };
        if !joins.is_empty() || !window_clause.is_empty() {
            return Err(invalid("cannot contain joins or windows"));
        }
        let ast::SelectTable::Table(pattern_table_name, _, _) = select.as_ref() else {
            return Err(invalid("must read one table"));
        };
        let definition = self.source(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        if !pattern_table_name
            .name
            .as_str()
            .eq_ignore_ascii_case(&definition.name)
        {
            return Err(invalid(&format!(
                "reads '{}', expected '{}'",
                pattern_table_name.name.as_str(),
                definition.name
            )));
        }

        let mut scope = Scope::new(None);
        scope.add_source_with_qualifier(definition, pattern_table_name.name.as_str(), true);
        let id = hir::IndexMethodPatternId {
            source,
            index: index.id(),
            pattern,
        };
        let policy = ExprPolicy::select()
            .without_subqueries()
            .without_aggregate();
        let outputs = self.analyze_result_columns(
            columns,
            &scope,
            hir::OutputOwner::IndexMethodPattern(id),
            policy.clone(),
            &[],
        )?;
        scope.set_outputs(&outputs);
        let predicate = where_clause
            .as_deref()
            .map(|expression| {
                self.analyze_expr(
                    expression,
                    &scope,
                    ExprPolicy::output_then_source()
                        .without_subqueries()
                        .without_aggregate(),
                )
            })
            .transpose()?;
        let order_by = self.analyze_order_by_terms(
            &syntax.order_by,
            &scope,
            ExprPolicy::output_then_source()
                .without_subqueries()
                .without_aggregate(),
        )?;
        let limit = syntax
            .limit
            .as_ref()
            .map(|syntax| -> Result<hir::Limit> {
                let empty = Scope::new(None);
                let policy = ExprPolicy::select()
                    .without_subqueries()
                    .without_aggregate();
                Ok(hir::Limit {
                    limit: self.analyze_expr(&syntax.expr, &empty, policy.clone())?,
                    offset: syntax
                        .offset
                        .as_deref()
                        .map(|expression| self.analyze_expr(expression, &empty, policy))
                        .transpose()?,
                })
            })
            .transpose()?;
        Ok(hir::IndexMethodPattern {
            id,
            index: index.clone(),
            outputs,
            predicate,
            order_by,
            limit,
        })
    }

    fn resolve_index_hint(
        &mut self,
        table: &hir::ResolvedTable,
        indexed: Option<&ast::Indexed>,
    ) -> Result<hir::IndexHint> {
        let Some(indexed) = indexed else {
            return Ok(hir::IndexHint::None);
        };
        match indexed {
            ast::Indexed::NotIndexed => Ok(hir::IndexHint::NotIndexed),
            ast::Indexed::IndexedBy(name) => {
                let database_id = table.database().map(DatabaseId::index).ok_or_else(|| {
                    LimboError::InternalError("indexed table has no database identity".to_string())
                })?;
                let index_name = crate::util::normalize_ident(name.as_str());
                let index = self
                    .context()
                    .schema(database_id)
                    .and_then(|schema| {
                        schema
                            .get_index(table.value().get_name(), &index_name)
                            .cloned()
                    })
                    .ok_or_else(|| {
                        LimboError::ParseError(format!("no such index: {index_name}"))
                    })?;
                let object_id =
                    self.catalog_object_id(Some(database_id), CatalogObjectKind::Index, index_name);
                Ok(hir::IndexHint::Indexed(CatalogObject::new(
                    object_id,
                    self.context().snapshot(),
                    Some(DatabaseId::new(database_id)),
                    index,
                )))
            }
        }
    }

    fn analyze_select_query(
        &mut self,
        syntax: &ast::Select,
        environment: QueryEnvironment,
    ) -> Result<hir::QueryId> {
        let query_id = self.reserve_query();
        let environment = self.prepare_query_environment(environment, syntax.with.as_ref())?;

        let first_id = hir::QueryBlockId::new(query_id, 0);
        // Lazy CTE resolution can recurse through many query blocks. Finish
        // the compound query only after that recursive path has returned.
        let (first, result_scope) =
            self.analyze_query_block(&syntax.body.select, first_id, &environment)?;

        self.finish_analyze_select_query(
            syntax,
            query_id,
            first_id,
            first,
            result_scope,
            environment,
        )
    }

    fn finish_analyze_select_query(
        &mut self,
        syntax: &ast::Select,
        query_id: hir::QueryId,
        first_id: hir::QueryBlockId,
        first: hir::QueryBlock,
        mut result_scope: Scope,
        environment: QueryEnvironment,
    ) -> Result<hir::QueryId> {
        let output_count = first.outputs.len();
        let output = first.outputs.iter().map(|output| output.id).collect();
        let mut blocks = vec![first];
        let mut compounds = Vec::with_capacity(syntax.body.compounds.len());

        for (index, compound) in syntax.body.compounds.iter().enumerate() {
            let block_id = hir::QueryBlockId::new(query_id, index + 1);
            let (block, _) = self.analyze_query_block(&compound.select, block_id, &environment)?;
            if block.outputs.len() != output_count {
                crate::bail_parse_error!(
                    "SELECTs to the left and right of {} do not have the same number of result columns",
                    compound.operator
                );
            }
            compounds.push(hir::CompoundArm {
                operator: compound.operator,
                block: block_id,
            });
            blocks.push(block);
        }

        if !compounds.is_empty() {
            let resolved_outputs = (0..output_count)
                .map(|index| {
                    Ok((
                        compound_output_type_fact(query_id, &blocks, index)?,
                        compound_output_affinity(query_id, &blocks, index)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            for (output, (type_fact, (affinity, has_affinity))) in
                blocks[0].outputs.iter_mut().zip(resolved_outputs)
            {
                output.type_fact = type_fact;
                output.affinity = affinity;
                output.has_affinity = has_affinity;
            }
        }

        result_scope.set_outputs(&blocks[0].outputs);
        let compound_outputs = (!compounds.is_empty()).then(|| {
            blocks
                .iter()
                .map(|block| block.outputs.clone())
                .collect::<Vec<_>>()
        });
        let order_by = self.analyze_query_order_by_in_block(
            &syntax.order_by,
            &result_scope,
            compound_outputs.as_deref(),
            Some(&blocks[0]),
        )?;
        let limit = self.analyze_query_limit(syntax.limit.as_ref(), &environment)?;
        let reachable_ctes = self.collect_reachable_ctes(&blocks);
        self.insert_query(
            query_id,
            hir::Query {
                id: query_id,
                reachable_ctes,
                blocks,
                first: first_id,
                compounds,
                order_by,
                limit,
                output,
            },
        )?;
        Ok(query_id)
    }

    fn analyze_query_block(
        &mut self,
        syntax: &ast::OneSelect,
        block_id: hir::QueryBlockId,
        environment: &QueryEnvironment,
    ) -> Result<(hir::QueryBlock, Scope)> {
        match syntax {
            ast::OneSelect::Select { .. } => {
                self.analyze_select_query_block(syntax, block_id, environment)
            }
            ast::OneSelect::Values(_) => {
                self.analyze_values_query_block(syntax, block_id, environment)
            }
        }
    }

    fn analyze_select_query_block(
        &mut self,
        syntax: &ast::OneSelect,
        block_id: hir::QueryBlockId,
        environment: &QueryEnvironment,
    ) -> Result<(hir::QueryBlock, Scope)> {
        let ast::OneSelect::Select { from, .. } = syntax else {
            unreachable!("SELECT query-block analysis received VALUES syntax");
        };
        let (from, scope) = match from {
            Some(from) => {
                let (resolved, scope) =
                    self.analyze_from_clause(from, SourceOwner::QueryBlock(block_id), environment)?;
                (Some(resolved), scope)
            }
            None => (None, self.scope_for_environment(environment)?),
        };

        self.finish_analyze_select_query_block(syntax, block_id, environment, from, scope)
    }

    fn finish_analyze_select_query_block(
        &mut self,
        syntax: &ast::OneSelect,
        block_id: hir::QueryBlockId,
        environment: &QueryEnvironment,
        from: Option<hir::From>,
        mut scope: Scope,
    ) -> Result<(hir::QueryBlock, Scope)> {
        let ast::OneSelect::Select {
            distinctness,
            columns,
            where_clause,
            group_by,
            window_clause,
            ..
        } = syntax
        else {
            unreachable!("SELECT query-block finalization received VALUES syntax");
        };

        let mut windows = Vec::with_capacity(window_clause.len());
        for definition in window_clause {
            if scope.window(definition.name.as_str()).is_some() {
                continue;
            }
            let spec =
                self.analyze_window(&definition.window, &scope, query_policy(environment))?;
            scope.insert_window(definition.name.as_str(), spec.clone());
            windows.push(hir::NamedWindow {
                name: crate::util::normalize_ident(definition.name.as_str()),
                spec,
            });
        }

        let outputs = self.analyze_result_columns(
            columns,
            &scope,
            hir::OutputOwner::QueryBlock(block_id),
            query_policy(environment),
            environment.expected_output_types(),
        )?;
        scope.set_outputs(&outputs);
        let filter = where_clause
            .as_deref()
            .map(|expr| {
                self.analyze_expr(
                    expr,
                    &scope,
                    ExprPolicy::source_then_output()
                        .with_raise(scope.allow_raise())
                        .without_aggregate(),
                )
            })
            .transpose()?;
        let grouping = group_by
            .as_ref()
            .map(|grouping| self.analyze_grouping(grouping, &scope))
            .transpose()?;

        Ok((
            hir::QueryBlock {
                id: block_id,
                from,
                outputs,
                body: hir::QueryBlockBody::Select {
                    distinctness: *distinctness,
                    filter,
                    grouping,
                    windows,
                },
            },
            scope,
        ))
    }

    fn analyze_values_query_block(
        &mut self,
        syntax: &ast::OneSelect,
        block_id: hir::QueryBlockId,
        environment: &QueryEnvironment,
    ) -> Result<(hir::QueryBlock, Scope)> {
        let ast::OneSelect::Values(syntax_rows) = syntax else {
            unreachable!("VALUES query-block analysis received SELECT syntax");
        };
        let mut scope = self.scope_for_environment(environment)?;
        let expected_count = syntax_rows.first().map_or(0, Vec::len);
        let mut rows = Vec::with_capacity(syntax_rows.len());
        for syntax_row in syntax_rows {
            if syntax_row.len() != expected_count {
                crate::bail_parse_error!("all VALUES must have the same number of terms");
            }
            let mut row = Vec::with_capacity(syntax_row.len());
            for (index, expression) in syntax_row.iter().enumerate() {
                if matches!(expression.as_ref(), ast::Expr::Default) {
                    let default = environment
                        .expected_defaults()
                        .get(index)
                        .cloned()
                        .flatten()
                        .ok_or_else(|| {
                            LimboError::ParseError(
                                "DEFAULT is only valid in an INSERT value".to_string(),
                            )
                        })?;
                    row.push(default);
                    continue;
                }
                let expected = environment
                    .expected_output_types()
                    .get(index)
                    .cloned()
                    .flatten();
                let expression = self.analyze_expr(
                    expression,
                    &scope,
                    query_policy(environment).with_expected_type(expected),
                )?;
                if matches!(&expression, hir::Expr::Row(_)) {
                    crate::bail_parse_error!("row value misused");
                }
                row.push(expression);
            }
            rows.push(row);
        }
        let first_row = rows.first();
        let outputs = (0..expected_count)
            .map(|index| {
                let expression = first_row
                    .and_then(|row| row.get(index))
                    .cloned()
                    .unwrap_or(hir::Expr::Literal(ast::Literal::Null));
                let row_type_facts = rows
                    .iter()
                    .map(|row| self.expression_type_fact(&row[index], &scope))
                    .collect::<Vec<_>>();
                let type_fact = hir::TypeFact::selected_value_result(&row_type_facts);
                let affinity = self.expression_affinity(&expression, &scope);
                let has_affinity = self.expression_has_affinity(&expression, &scope);
                let collation = self.expression_collation(&expression, &scope);
                hir::Output {
                    id: hir::OutputId::query(block_id, index),
                    name: format!("column{}", index + 1),
                    expr: expression,
                    type_fact,
                    affinity,
                    has_affinity,
                    collation,
                    name_kind: hir::OutputNameKind::Inferred,
                }
            })
            .collect::<Vec<_>>();
        scope.set_outputs(&outputs);
        Ok((
            hir::QueryBlock {
                id: block_id,
                from: None,
                outputs,
                body: hir::QueryBlockBody::Values { rows },
            },
            scope,
        ))
    }

    fn analyze_grouping(&mut self, syntax: &ast::GroupBy, scope: &Scope) -> Result<hir::Grouping> {
        let key_scope = scope.without_outer();
        let mut keys = Vec::with_capacity(syntax.exprs.len());
        let key_policy = ExprPolicy::source_then_output()
            .with_raise(key_scope.allow_raise())
            .without_aggregate();
        for (index, expression) in syntax.exprs.iter().enumerate() {
            let clause = format!("{} GROUP BY", ordinal_name(index));
            if let Some(resolved) = self.analyze_output_ordinal(expression, &key_scope, &clause)? {
                self.validate_existing_expr(&resolved, &key_scope, &key_policy)?;
                keys.push(resolved);
            } else {
                keys.push(self.analyze_expr(expression, &key_scope, key_policy.clone())?);
            }
        }
        let having = syntax
            .having
            .as_deref()
            .map(|expression| {
                self.analyze_expr(
                    expression,
                    scope,
                    ExprPolicy::output_then_source()
                        .with_raise(scope.allow_raise())
                        .without_window(),
                )
            })
            .transpose()?;
        Ok(hir::Grouping { keys, having })
    }

    pub(super) fn analyze_query_order_by(
        &mut self,
        syntax: &[ast::SortedColumn],
        scope: &Scope,
        compound_outputs: Option<&[Vec<hir::Output>]>,
    ) -> Result<Vec<hir::OrderTerm>> {
        self.analyze_query_order_by_in_block(syntax, scope, compound_outputs, None)
    }

    fn analyze_query_order_by_in_block(
        &mut self,
        syntax: &[ast::SortedColumn],
        scope: &Scope,
        compound_outputs: Option<&[Vec<hir::Output>]>,
        query_block: Option<&hir::QueryBlock>,
    ) -> Result<Vec<hir::OrderTerm>> {
        if let Some(outputs) = compound_outputs {
            return self.analyze_compound_order_by(syntax, outputs);
        }
        let mut order = Vec::with_capacity(syntax.len());
        for (index, term) in syntax.iter().enumerate() {
            let clause = format!("{} ORDER BY", ordinal_name(index));
            let expression = if let Some(expression) =
                self.analyze_output_ordinal(&term.expr, scope, &clause)?
            {
                expression
            } else {
                self.analyze_expr(
                    &term.expr,
                    scope,
                    ExprPolicy::output_then_source().with_raise(scope.allow_raise()),
                )?
            };
            let ends_order = query_block
                .map(|query_block| self.order_term_is_unique_rowid(&expression, query_block))
                .transpose()?
                .unwrap_or(false);
            order.push(hir::OrderTerm {
                expr: expression,
                order: term.order.unwrap_or(ast::SortOrder::Asc),
                nulls: term.nulls,
            });
            if ends_order {
                break;
            }
        }
        Ok(order)
    }

    fn order_term_is_unique_rowid(
        &self,
        expression: &hir::Expr,
        query_block: &hir::QueryBlock,
    ) -> Result<bool> {
        let Some(from) = &query_block.from else {
            return Ok(false);
        };
        if !from.joins.is_empty() {
            return Ok(false);
        }

        let expression = match expression {
            hir::Expr::Output(id) => {
                &query_block
                    .outputs
                    .iter()
                    .find(|output| output.id == *id)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "ORDER BY resolved missing output column {id:?}"
                        ))
                    })?
                    .expr
            }
            expression => expression,
        };
        match expression {
            hir::Expr::RowId(source) => Ok(*source == from.first),
            hir::Expr::Column(column) if column.source == from.first => {
                let source = self.source(column.source).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "ORDER BY resolved missing source {}",
                        column.source
                    ))
                })?;
                let column = source.columns.get(column.column).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "ORDER BY resolved missing column {} from source {}",
                        column.column, column.source
                    ))
                })?;
                Ok(column.rowid_alias)
            }
            _ => Ok(false),
        }
    }

    fn analyze_output_ordinal(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        clause: &str,
    ) -> Result<Option<hir::Expr>> {
        match syntax {
            ast::Expr::Collate(inner, collation) => {
                let Some(inner) = self.analyze_output_ordinal(inner, scope, clause)? else {
                    return Ok(None);
                };
                Ok(Some(hir::Expr::Collate {
                    expr: Box::new(inner),
                    collation: self.resolve_collation(collation.as_str())?,
                }))
            }
            ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                self.analyze_output_ordinal(&expressions[0], scope, clause)
            }
            _ => expression_ordinal(syntax)
                .map(|ordinal| {
                    scope
                        .resolve_output_ordinal(usize::try_from(ordinal).unwrap_or(0), clause)
                        .map(|value| value.expr)
                })
                .transpose(),
        }
    }

    fn analyze_compound_order_by(
        &mut self,
        syntax: &[ast::SortedColumn],
        output_arms: &[Vec<hir::Output>],
    ) -> Result<Vec<hir::OrderTerm>> {
        let first = output_arms.first().ok_or_else(|| {
            LimboError::InternalError("compound query has no first output arm".to_string())
        })?;
        let mut order = Vec::with_capacity(syntax.len());
        for (index, term) in syntax.iter().enumerate() {
            let expression =
                self.resolve_compound_order_by_expr(&term.expr, output_arms, first, index + 1)?;
            order.push(hir::OrderTerm {
                expr: expression,
                order: term.order.unwrap_or(ast::SortOrder::Asc),
                nulls: term.nulls,
            });
        }
        Ok(order)
    }

    fn resolve_compound_order_by_expr(
        &mut self,
        syntax: &ast::Expr,
        output_arms: &[Vec<hir::Output>],
        first: &[hir::Output],
        term_number: usize,
    ) -> Result<hir::Expr> {
        let column = match syntax {
            ast::Expr::Collate(inner, collation) => {
                return Ok(hir::Expr::Collate {
                    expr: Box::new(self.resolve_compound_order_by_expr(
                        inner,
                        output_arms,
                        first,
                        term_number,
                    )?),
                    collation: self.resolve_collation(collation.as_str())?,
                });
            }
            ast::Expr::Literal(ast::Literal::Numeric(number)) => {
                let Ok(column_number) = number.parse::<i32>() else {
                    crate::bail_parse_error!(
                        "{} ORDER BY term does not match any column in the result set",
                        ordinal(term_number)
                    );
                };
                if column_number <= 0 || column_number as usize > first.len() {
                    crate::bail_parse_error!(
                        "{} ORDER BY term out of range - should be between 1 and {}",
                        column_number,
                        first.len()
                    );
                }
                column_number as usize - 1
            }
            ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                return self.resolve_compound_order_by_expr(
                    &expressions[0],
                    output_arms,
                    first,
                    term_number,
                );
            }
            ast::Expr::Id(name) => {
                let mut found = None;
                for outputs in output_arms {
                    found = outputs
                        .iter()
                        .position(|output| {
                            output.name_kind == hir::OutputNameKind::ExplicitAlias
                                && output.name.eq_ignore_ascii_case(name.as_str())
                        })
                        .or_else(|| {
                            outputs.iter().position(|output| {
                                !output.name.is_empty()
                                    && output.name.eq_ignore_ascii_case(name.as_str())
                            })
                        });
                    if found.is_some() {
                        break;
                    }
                }
                let Some(found) = found else {
                    crate::bail_parse_error!(
                        "{} ORDER BY term does not match any column in the result set",
                        ordinal(term_number)
                    );
                };
                found
            }
            _ => {
                crate::bail_parse_error!(
                    "{} ORDER BY term does not match any column in the result set",
                    ordinal(term_number)
                );
            }
        };
        let output = first.get(column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "compound ORDER BY resolved missing output column {column}"
            ))
        })?;
        Ok(hir::Expr::Output(output.id))
    }

    fn analyze_order_by_terms(
        &mut self,
        syntax: &[ast::SortedColumn],
        scope: &Scope,
        policy: ExprPolicy,
    ) -> Result<Vec<hir::OrderTerm>> {
        syntax
            .iter()
            .map(|term| {
                Ok(hir::OrderTerm {
                    expr: self.analyze_expr(&term.expr, scope, policy.clone())?,
                    order: term.order.unwrap_or(ast::SortOrder::Asc),
                    nulls: term.nulls,
                })
            })
            .collect()
    }

    pub(super) fn analyze_query_limit(
        &mut self,
        syntax: Option<&ast::Limit>,
        environment: &QueryEnvironment,
    ) -> Result<Option<hir::Limit>> {
        let Some(syntax) = syntax else {
            return Ok(None);
        };
        // Ordinary query sources are not visible in LIMIT/OFFSET, but trigger
        // pseudo-tables remain visible throughout the trigger program.
        let mut limit_environment = environment.clone();
        limit_environment.outer = None;
        let scope = self.scope_for_environment(&limit_environment)?;
        let policy = query_policy(environment).without_aggregate();
        Ok(Some(hir::Limit {
            limit: self.analyze_expr(&syntax.expr, &scope, policy.clone())?,
            offset: syntax
                .offset
                .as_deref()
                .map(|expr| self.analyze_expr(expr, &scope, policy))
                .transpose()?,
        }))
    }

    fn collect_reachable_ctes(&self, blocks: &[hir::QueryBlock]) -> Vec<hir::CteId> {
        let mut ctes = Vec::new();
        let mut add = |source_id: hir::SourceId| {
            let Some(source) = self.source(source_id) else {
                return;
            };
            let id = match source.kind {
                hir::SourceKind::Cte(id) | hir::SourceKind::RecursiveInput(id) => id,
                _ => return,
            };
            if !ctes.contains(&id) {
                ctes.push(id);
            }
        };
        for block in blocks {
            if let Some(from) = &block.from {
                add(from.first);
                for join in &from.joins {
                    add(join.right);
                }
            }
        }
        ctes
    }

    fn analyze_from_clause(
        &mut self,
        syntax: &ast::FromClause,
        owner: SourceOwner,
        environment: &QueryEnvironment,
    ) -> Result<(hir::From, Scope)> {
        self.analyze_from_clause_with_kind(syntax, owner, environment, FromAnalysisKind::Query)
    }

    fn analyze_from_clause_with_kind(
        &mut self,
        syntax: &ast::FromClause,
        owner: SourceOwner,
        environment: &QueryEnvironment,
        kind: FromAnalysisKind,
    ) -> Result<(hir::From, Scope)> {
        // Resolve the first source before allocating the state used to bind
        // joins. Linear CTE chains only need this recursive source path.
        let first = self.analyze_select_table(&syntax.select, owner, environment, 0)?;

        self.finish_analyze_from_clause(syntax, owner, environment, first, kind)
    }

    fn finish_analyze_from_clause(
        &mut self,
        syntax: &ast::FromClause,
        owner: SourceOwner,
        environment: &QueryEnvironment,
        first: hir::SourceId,
        kind: FromAnalysisKind,
    ) -> Result<(hir::From, Scope)> {
        let mut right_sources = Vec::with_capacity(syntax.joins.len());
        for (index, join) in syntax.joins.iter().enumerate() {
            right_sources.push(self.analyze_select_table(
                &join.table,
                owner,
                environment,
                index + 1,
            )?);
        }

        if kind == FromAnalysisKind::Update {
            self.reject_duplicate_update_from_identifiers(first, &right_sources)?;
        }

        // SQLite resolves ON clauses and table-function arguments against
        // the complete FROM namespace, including later sources.
        let mut complete_scope = self.scope_for_environment(environment)?;
        self.add_source_to_scope(&mut complete_scope, first, true)?;
        for source in &right_sources {
            self.add_source_to_scope(&mut complete_scope, *source, true)?;
        }
        self.analyze_table_function_arguments(&syntax.select, first, &complete_scope)?;
        for (join, source) in syntax.joins.iter().zip(&right_sources) {
            self.analyze_table_function_arguments(&join.table, *source, &complete_scope)?;
        }

        let mut scope = self.scope_for_environment(environment)?;
        self.add_source_to_scope(&mut scope, first, true)?;
        let mut joins = Vec::with_capacity(syntax.joins.len());
        for (syntax_join, right) in syntax.joins.iter().zip(right_sources) {
            let (kind, natural) = resolved_join_kind(syntax_join.operator);
            if natural && syntax_join.constraint.is_some() {
                crate::bail_parse_error!("a NATURAL join may not have an ON or USING clause");
            }

            let right_definition = self.source(right).cloned().ok_or_else(|| {
                LimboError::InternalError(format!("missing right join source {right}"))
            })?;
            let using_names = if natural {
                scope.natural_common_columns(&right_definition)
            } else if let Some(ast::JoinConstraint::Using(columns)) = &syntax_join.constraint {
                columns
                    .iter()
                    .map(|column| column.as_str().to_string())
                    .collect()
            } else {
                Vec::new()
            };

            let mut using_columns = Vec::with_capacity(using_names.len());
            for name in using_names {
                let left = scope.resolve_using_left(&name)?;
                let normalized = crate::util::normalize_ident(&name);
                let (right_index, right_column) = right_definition
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| crate::util::normalize_ident(&column.name) == normalized)
                    .ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "cannot join using column {name} - column not present in both tables"
                        ))
                    })?;
                let value = match kind {
                    hir::JoinKind::Right => hir::MergedColumnValue::Right,
                    hir::JoinKind::Full => hir::MergedColumnValue::Coalesce,
                    _ => hir::MergedColumnValue::Left,
                };
                let type_fact = match value {
                    hir::MergedColumnValue::Left => left.type_fact.clone(),
                    hir::MergedColumnValue::Right => right_column.type_fact.clone(),
                    hir::MergedColumnValue::Coalesce => {
                        merge_type_facts(&left.type_fact, &right_column.type_fact)
                    }
                };
                let affinity = match value {
                    hir::MergedColumnValue::Right => right_column.affinity,
                    hir::MergedColumnValue::Left | hir::MergedColumnValue::Coalesce => {
                        left.affinity
                    }
                };
                let has_affinity = match value {
                    hir::MergedColumnValue::Right => right_column.has_affinity,
                    hir::MergedColumnValue::Left | hir::MergedColumnValue::Coalesce => {
                        left.has_affinity
                    }
                };
                let collation = match value {
                    hir::MergedColumnValue::Right => right_column.collation.clone(),
                    hir::MergedColumnValue::Left | hir::MergedColumnValue::Coalesce => {
                        left.collation.clone()
                    }
                };
                self.require_source_columns_in_expr(&left.expr);
                self.require_source_column(right, right_index);
                using_columns.push(hir::UsingColumn {
                    name,
                    left: Box::new(left.expr),
                    right: hir::ColumnRef {
                        source: right,
                        column: right_index,
                    },
                    value,
                    type_fact,
                    affinity,
                    has_affinity,
                    collation,
                });
            }

            self.add_source_to_scope(&mut scope, right, true)?;
            let constraint = match &syntax_join.constraint {
                Some(ast::JoinConstraint::On(expression)) => hir::JoinConstraint::On(
                    self.analyze_expr(
                        expression,
                        &complete_scope,
                        ExprPolicy::select()
                            .with_raise(complete_scope.allow_raise())
                            .without_aggregate(),
                    )?,
                ),
                Some(ast::JoinConstraint::Using(_)) => {
                    scope.apply_using(&using_columns)?;
                    hir::JoinConstraint::Using(using_columns)
                }
                None if natural => {
                    scope.apply_using(&using_columns)?;
                    hir::JoinConstraint::Natural(using_columns)
                }
                None => hir::JoinConstraint::None,
            };
            joins.push(hir::Join {
                right,
                kind,
                constraint,
            });
        }
        Ok((hir::From { first, joins }, scope))
    }

    fn reject_duplicate_update_from_identifiers(
        &self,
        first: hir::SourceId,
        right_sources: &[hir::SourceId],
    ) -> Result<()> {
        let mut seen = Vec::with_capacity(right_sources.len() + 1);
        for source_id in std::iter::once(first).chain(right_sources.iter().copied()) {
            let source = self.source(source_id).ok_or_else(|| {
                LimboError::InternalError(format!("missing UPDATE FROM source {source_id}"))
            })?;
            let identifier = crate::util::normalize_ident(
                source.alias.as_deref().unwrap_or(source.name.as_str()),
            );
            if seen.iter().any(|seen| seen == &identifier) {
                let database_name = source
                    .database
                    .and_then(|database| self.context().database_name(database.index()))
                    .unwrap_or("main");
                crate::bail_parse_error!(
                    "ambiguous column name: {database_name}.{identifier}._ROWID_"
                );
            }
            seen.push(identifier);
        }
        Ok(())
    }

    fn analyze_select_table(
        &mut self,
        syntax: &ast::SelectTable,
        owner: SourceOwner,
        environment: &QueryEnvironment,
        position: usize,
    ) -> Result<hir::SourceId> {
        match syntax {
            ast::SelectTable::Table(name, alias, indexed) => self.analyze_named_source(
                name,
                alias.as_ref().map(ast::As::name),
                indexed.as_ref(),
                owner,
                environment,
            ),
            _ => self.analyze_non_named_select_table(syntax, owner, environment, position),
        }
    }

    fn analyze_non_named_select_table(
        &mut self,
        syntax: &ast::SelectTable,
        owner: SourceOwner,
        environment: &QueryEnvironment,
        position: usize,
    ) -> Result<hir::SourceId> {
        match syntax {
            ast::SelectTable::Table(..) => {
                unreachable!("non-table source analysis received a named table")
            }
            ast::SelectTable::TableCall(name, arguments, alias) => {
                if name.db_name.is_none() {
                    if let Some(binding) = environment.ctes.find(name.name.as_str()) {
                        if matches!(
                            binding.state(),
                            CteState::Analyzing {
                                recursive_columns: Some(_),
                                ..
                            }
                        ) {
                            crate::bail_parse_error!(
                                "too many arguments on {}() - max 0",
                                name.name.as_str()
                            );
                        }
                        crate::bail_parse_error!("'{}' is not a function", name.name.as_str());
                    }
                }
                let source = self.analyze_base_table_source(
                    name,
                    alias.as_ref().map(ast::As::name),
                    None,
                    owner,
                    IndexMetadataMode::Read,
                )?;
                let table = self.resolved_source_table(source)?;
                if !arguments.is_empty() && table.value().btree().is_some() {
                    crate::bail_parse_error!("'{}' is not a function", name.name.as_str());
                }
                let definition = self.source_mut(source).ok_or_else(|| {
                    LimboError::InternalError(format!("missing table-function source {source}"))
                })?;
                definition.kind = hir::SourceKind::TableFunction {
                    table,
                    arguments: Vec::new(),
                };
                Ok(source)
            }
            ast::SelectTable::Select(select, alias) => {
                let nested_environment = nested_query_environment(environment);
                let query = self.analyze_query(select, nested_environment)?;
                self.register_derived_source(
                    query,
                    alias.as_ref().map(ast::As::name),
                    format!("(subquery-{position})"),
                    None,
                    owner,
                    None,
                )
            }
            ast::SelectTable::Sub(from, alias) => {
                let select = ast::Select {
                    with: None,
                    body: ast::SelectBody {
                        select: ast::OneSelect::Select {
                            distinctness: None,
                            columns: vec![ast::ResultColumn::Star],
                            from: Some(from.clone()),
                            where_clause: None,
                            group_by: None,
                            window_clause: Vec::new(),
                        },
                        compounds: Vec::new(),
                    },
                    order_by: Vec::new(),
                    limit: None,
                };
                let query = self.analyze_query(&select, nested_query_environment(environment))?;
                self.register_derived_source(
                    query,
                    alias.as_ref().map(ast::As::name),
                    format!("(subquery-{position})"),
                    None,
                    owner,
                    None,
                )
            }
        }
    }

    fn analyze_named_source(
        &mut self,
        name: &ast::QualifiedName,
        alias: Option<&ast::Name>,
        indexed: Option<&ast::Indexed>,
        owner: SourceOwner,
        environment: &QueryEnvironment,
    ) -> Result<hir::SourceId> {
        if name.db_name.is_none() {
            if let Some(binding) = environment.ctes.find(name.name.as_str()) {
                if let Some(ast::Indexed::IndexedBy(index)) = indexed {
                    crate::bail_parse_error!("no such index: {}", index.as_str());
                }
                return self.analyze_cte_source(binding, alias, owner, environment);
            }
        }

        self.analyze_catalog_named_source(name, alias, indexed, owner)
    }

    fn analyze_catalog_named_source(
        &mut self,
        name: &ast::QualifiedName,
        alias: Option<&ast::Name>,
        indexed: Option<&ast::Indexed>,
        owner: SourceOwner,
    ) -> Result<hir::SourceId> {
        let database_id = self.context().resolve_existing_table_database_id(name)?;
        let table_name = crate::util::normalize_ident(name.name.as_str());
        let view = self
            .context()
            .schema(database_id)
            .and_then(|schema| schema.get_view(&table_name));
        if let Some(view) = view {
            if let Some(ast::Indexed::IndexedBy(index)) = indexed {
                crate::bail_parse_error!("no such index: {}", index.as_str());
            }
            view.process()?;
            let result = self.analyze_query(&view.select_stmt, QueryEnvironment::empty());
            view.done();
            let query = result?;
            return self.register_derived_source(
                query,
                alias,
                table_name,
                Some(DatabaseId::new(database_id)),
                owner,
                Some(&view.columns),
            );
        }

        self.analyze_base_table_source(name, alias, indexed, owner, IndexMetadataMode::Read)
    }

    fn register_derived_source(
        &mut self,
        query: hir::QueryId,
        alias: Option<&ast::Name>,
        name: String,
        database: Option<DatabaseId>,
        owner: SourceOwner,
        declared_columns: Option<&[crate::schema::Column]>,
    ) -> Result<hir::SourceId> {
        let outputs = self.query_outputs(query)?;
        let columns: Vec<hir::SourceColumn> = outputs
            .iter()
            .enumerate()
            .map(|(index, output)| hir::SourceColumn {
                name: declared_columns
                    .and_then(|columns| columns.get(index))
                    .and_then(|column| column.name.clone())
                    .unwrap_or_else(|| output.name.clone()),
                type_fact: output.type_fact.clone(),
                affinity: output.affinity,
                has_affinity: output.has_affinity,
                collation: output.collation.clone(),
                hidden: false,
                rowid_alias: false,
            })
            .collect();
        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner,
                database,
                name,
                alias: alias.map(|alias| crate::util::normalize_ident(alias.as_str())),
                kind: hir::SourceKind::Derived(query),
                generated_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                default_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                column_type_programs: vec![None; columns.len()],
                check_constraints: Vec::new(),
                columns,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }

    pub(super) fn query_outputs(&self, query: hir::QueryId) -> Result<Vec<hir::Output>> {
        let query = self
            .query(query)
            .ok_or_else(|| LimboError::InternalError(format!("missing derived query {query}")))?;
        let first = query.blocks.get(query.first.index).ok_or_else(|| {
            LimboError::InternalError(format!("missing first block of query {query:?}"))
        })?;
        Ok(first.outputs.clone())
    }

    fn analyze_table_function_arguments(
        &mut self,
        syntax: &ast::SelectTable,
        source: hir::SourceId,
        scope: &Scope,
    ) -> Result<()> {
        let ast::SelectTable::TableCall(_, syntax_arguments, _) = syntax else {
            return Ok(());
        };
        let mut arguments = Vec::with_capacity(syntax_arguments.len());
        for argument in syntax_arguments {
            arguments.push(
                self.analyze_expr(
                    argument,
                    scope,
                    ExprPolicy::select()
                        .with_raise(scope.allow_raise())
                        .without_aggregate(),
                )?,
            );
        }
        let definition = self.source_mut(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing table-function source {source}"))
        })?;
        let hir::SourceKind::TableFunction {
            arguments: resolved,
            ..
        } = &mut definition.kind
        else {
            return Err(LimboError::InternalError(format!(
                "source {source} stopped being a table function"
            )));
        };
        *resolved = arguments;
        Ok(())
    }

    fn analyze_result_columns(
        &mut self,
        syntax: &[ast::ResultColumn],
        scope: &Scope,
        owner: hir::OutputOwner,
        policy: ExprPolicy,
        expected_types: &[Option<hir::ResolvedType>],
    ) -> Result<Vec<hir::Output>> {
        let mut outputs = Vec::new();
        for column in syntax {
            match column {
                ast::ResultColumn::Expr(expr, alias) => {
                    let expected = expected_types.get(outputs.len()).cloned().flatten();
                    let expression = self.analyze_expr(
                        expr,
                        scope,
                        policy.clone().with_expected_type(expected),
                    )?;
                    if matches!(&expression, hir::Expr::Row(_)) {
                        crate::bail_parse_error!("row value misused");
                    }
                    let name = match alias.as_ref() {
                        Some(ast::As::ImplicitColumnName(_))
                            if matches!(
                                expr.as_ref(),
                                ast::Expr::Id(_)
                                    | ast::Expr::Name(_)
                                    | ast::Expr::Qualified(_, _)
                                    | ast::Expr::DoublyQualified(_, _, _)
                            ) =>
                        {
                            inferred_output_name(expr)
                        }
                        Some(alias) => alias.name().as_str().to_string(),
                        None => inferred_output_name(expr),
                    };
                    let type_fact = self.expression_type_fact(&expression, scope);
                    let affinity = self.expression_affinity(&expression, scope);
                    let has_affinity = self.expression_has_affinity(&expression, scope);
                    let collation = self.expression_collation(&expression, scope);
                    outputs.push(hir::Output {
                        id: hir::OutputId {
                            owner,
                            index: outputs.len(),
                        },
                        name,
                        expr: expression,
                        type_fact,
                        affinity,
                        has_affinity,
                        collation,
                        name_kind: if alias.as_ref().is_some_and(ast::As::is_explicit) {
                            hir::OutputNameKind::ExplicitAlias
                        } else {
                            hir::OutputNameKind::Inferred
                        },
                    });
                }
                ast::ResultColumn::Star => {
                    let expanded = scope.expand_star()?;
                    if expanded.is_empty() {
                        crate::bail_parse_error!("no tables specified");
                    }
                    for (name, expr, type_fact, affinity, has_affinity, collation) in expanded {
                        self.require_source_columns_in_expr(&expr);
                        outputs.push(hir::Output {
                            id: hir::OutputId {
                                owner,
                                index: outputs.len(),
                            },
                            name,
                            expr,
                            type_fact,
                            affinity,
                            has_affinity,
                            collation,
                            name_kind: hir::OutputNameKind::StarExpansion,
                        });
                    }
                }
                ast::ResultColumn::TableStar(table) => {
                    for (name, expr, type_fact, affinity, has_affinity, collation) in
                        scope.expand_table_star(table.as_str())?
                    {
                        self.require_source_columns_in_expr(&expr);
                        outputs.push(hir::Output {
                            id: hir::OutputId {
                                owner,
                                index: outputs.len(),
                            },
                            name,
                            expr,
                            type_fact,
                            affinity,
                            has_affinity,
                            collation,
                            name_kind: hir::OutputNameKind::StarExpansion,
                        });
                    }
                }
            }
        }
        Ok(outputs)
    }
}

/// Compute the value fact exposed by a compound output. A compound chooses one
/// arm at runtime, so it follows the same merge rule as CASE and COALESCE.
pub(super) fn compound_output_type_fact(
    query: hir::QueryId,
    blocks: &[hir::QueryBlock],
    index: usize,
) -> Result<hir::TypeFact> {
    let mut facts = Vec::with_capacity(blocks.len());
    for block in blocks {
        let output = block.outputs.get(index).ok_or_else(|| {
            LimboError::InternalError(format!(
                "compound query {query} block {} is missing output column {index}",
                block.id.index
            ))
        })?;
        facts.push(&output.type_fact);
    }
    Ok(hir::TypeFact::selected_value_result(facts))
}

/// Compute the effective affinity of one compound-query output using the
/// storage classes and declared affinity of every arm. A real BLOB-affinity
/// column is deliberately distinct from an expression with no affinity.
fn compound_output_affinity(
    query: hir::QueryId,
    blocks: &[hir::QueryBlock],
    index: usize,
) -> Result<(Affinity, bool)> {
    let mut chosen = None;
    let mut has_numeric_storage = false;
    let mut has_text_storage = false;

    for block in blocks {
        let output = block.outputs.get(index).ok_or_else(|| {
            LimboError::InternalError(format!(
                "compound query {query} block {} is missing output column {index}",
                block.id.index
            ))
        })?;
        if chosen.is_none() && output.has_affinity {
            chosen = Some(output.affinity);
            continue;
        }

        let (numeric, text) = output_storage_classes(output);
        has_numeric_storage |= numeric;
        has_text_storage |= text;
    }

    let Some(affinity) = chosen else {
        return Ok((Affinity::Blob, false));
    };
    if (affinity == Affinity::Text && has_numeric_storage)
        || (affinity.is_numeric() && has_text_storage)
    {
        return Ok((Affinity::Blob, false));
    }
    Ok((affinity, true))
}

/// Return whether an output can produce numeric and text storage classes.
/// These are the only classes that can conflict with compound affinity.
fn output_storage_classes(output: &hir::Output) -> (bool, bool) {
    if output.has_affinity {
        return if output.affinity.is_numeric() {
            (true, false)
        } else if output.affinity == Affinity::Text {
            (false, true)
        } else {
            (true, true)
        };
    }

    match output.type_fact.storage {
        Some(Type::Text) => (false, true),
        Some(Type::Numeric | Type::Integer | Type::Real) => (true, false),
        Some(Type::Null | Type::Blob) => (false, false),
        None => (true, true),
    }
}

fn table_has_rowid(table: &Table) -> bool {
    table.btree().is_some_and(|table| table.has_rowid) || table.virtual_table().is_some()
}

fn pseudo_source_name(kind: hir::PseudoSource) -> &'static str {
    match kind {
        hir::PseudoSource::Excluded => "excluded",
        hir::PseudoSource::New => "new",
        hir::PseudoSource::Old => "old",
    }
}

fn inferred_output_name(expr: &ast::Expr) -> String {
    match expr {
        ast::Expr::Id(name) | ast::Expr::Name(name) => name.as_str().to_string(),
        ast::Expr::Qualified(_, name) | ast::Expr::DoublyQualified(_, _, name) => {
            name.as_str().to_string()
        }
        _ => expr.to_string(),
    }
}

fn nested_query_environment(environment: &QueryEnvironment) -> QueryEnvironment {
    let mut nested = environment.clone();
    nested.expected_output_types.clear();
    nested.expected_defaults.clear();
    nested
}

fn query_policy(environment: &QueryEnvironment) -> ExprPolicy {
    ExprPolicy::select().with_raise(environment.allow_raise)
}

fn resolved_join_kind(operator: ast::JoinOperator) -> (hir::JoinKind, bool) {
    match operator {
        ast::JoinOperator::Comma => (hir::JoinKind::Comma, false),
        ast::JoinOperator::TypedJoin(None) => (hir::JoinKind::Inner, false),
        ast::JoinOperator::TypedJoin(Some(kind)) => {
            let left = kind.contains(ast::JoinType::LEFT);
            let right = kind.contains(ast::JoinType::RIGHT);
            let outer = kind.contains(ast::JoinType::OUTER);
            let natural = kind.contains(ast::JoinType::NATURAL);
            let resolved = if (left && right) || (outer && !left && !right) {
                hir::JoinKind::Full
            } else if right {
                hir::JoinKind::Right
            } else if left || outer {
                hir::JoinKind::Left
            } else if kind.contains(ast::JoinType::CROSS) {
                hir::JoinKind::Cross
            } else {
                hir::JoinKind::Inner
            };
            (resolved, natural)
        }
    }
}

fn merge_type_facts(left: &hir::TypeFact, right: &hir::TypeFact) -> hir::TypeFact {
    hir::TypeFact::selected_value_result([left, right])
}

fn expression_ordinal(expression: &ast::Expr) -> Option<i32> {
    let (sign, value) = match expression {
        ast::Expr::Literal(ast::Literal::Numeric(value)) => (1_i64, value),
        ast::Expr::Unary(ast::UnaryOperator::Positive, expression) => {
            let ast::Expr::Literal(ast::Literal::Numeric(value)) = expression.as_ref() else {
                return None;
            };
            (1, value)
        }
        ast::Expr::Unary(ast::UnaryOperator::Negative, expression) => {
            let ast::Expr::Literal(ast::Literal::Numeric(value)) = expression.as_ref() else {
                return None;
            };
            (-1, value)
        }
        _ => return None,
    };
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let value = value.parse::<i64>().ok()?.checked_mul(sign)?;
    i32::try_from(value).ok()
}

fn ordinal_name(index: usize) -> String {
    let ordinal = index + 1;
    let suffix = if (11..=13).contains(&(ordinal % 100)) {
        "th"
    } else {
        match ordinal % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        }
    };
    format!("{ordinal}{suffix}")
}

fn ordinal(number: usize) -> String {
    let suffix = match (number % 10, number % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{number}{suffix}")
}
