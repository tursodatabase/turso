//! Instantiation of positional stored expressions into one HIR document.

use turso_parser::ast;

use crate::function::{Func, ScalarFunc};
use crate::schema::{Column, Table, Type, TypeDef};
use crate::schema_expr::{
    BuiltinSchemaExprResolver, ResolutionMode, SchemaColumn, SchemaCustomTypeFunction, SchemaExpr,
    SchemaExprContext, SchemaExprNode, SchemaExprResolver, SchemaFieldAccess, SchemaTable,
    SchemaTypeName, SchemaTypeParameter, SchemaTypeSize, SchemaValueType, SelfColumn,
    ValidSchemaExpr,
};
use crate::sync::Arc;
use crate::util::{check_literal_equivalency, normalize_ident, type_from_name};
use crate::vdbe::affinity::Affinity;
use crate::{LimboError, Result};

use super::hir::{self, CatalogObject, DatabaseId, SourceId, SourceKind, SourceOwner, TypeFact};
use super::{
    context::SemanticContext, expr::ExprPolicy, scope::Scope, Analyzer, CatalogObjectKind,
};

/// One stored expression analyzed against one explicit source occurrence.
///
/// This deliberately is not a query document. The closed document retains the
/// resolved source and catalog snapshot so physical consumers only provide the
/// runtime row or cursor before emitting the expression.
pub(crate) struct AnalyzedSchemaExpr {
    pub(crate) document: hir::HirDocument,
}

/// Several stored expressions analyzed against one shared table occurrence.
///
/// Batch analysis is useful for schema operations such as index rebuilds: all
/// keys, predicates, and generated columns must use one source identity.
pub(crate) struct AnalyzedSchemaExprs {
    pub(crate) document: hir::HirDocument,
}

/// Parser syntax still held by the catalog, together with the schema rule that
/// defines its legal names and operations.
pub(crate) struct SchemaSyntaxInput<'syntax> {
    pub(crate) syntax: &'syntax ast::Expr,
    pub(crate) profile: crate::schema_expr::SchemaExprProfile,
    pub(crate) owner_column: Option<usize>,
}

/// One positional input to a schema expression that is not owned by a table.
///
/// Callers may preserve an already-resolved fact when they have one. Otherwise
/// semantic analysis resolves `declared_type` in `database_id`. Keeping the
/// spelling alongside the fact also gives domain and type-definition callers a
/// stable fallback when no fact has been computed yet.
#[derive(Clone, Debug)]
pub(crate) struct SchemaExprInput {
    pub(crate) name: String,
    pub(crate) declared_type: Option<String>,
    pub(crate) array_dimensions: u32,
    pub(crate) type_fact: Option<TypeFact>,
}

/// Analyze one runtime scalar that has no table inputs.
///
/// ATTACH/DETACH use this boundary: syntax and catalog meaning are closed
/// before the bytecode emitter receives the expression.
pub(crate) fn analyze_scalar_syntax(
    context: &SemanticContext<'_>,
    database_id: usize,
    syntax: &ast::Expr,
) -> Result<AnalyzedSchemaExpr> {
    let mut analyzer = Analyzer::new(context);
    let source = analyzer.create_schema_expression_source(database_id, &[])?;
    let expression = analyzer.analyze_expr(
        syntax,
        &Scope::default(),
        ExprPolicy::select()
            .without_subqueries()
            .without_aggregate()
            .without_window(),
    )?;
    let document = analyzer.finish(hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
        source,
        expressions: vec![expression],
    }))?;
    Ok(AnalyzedSchemaExpr { document })
}

/// Analyze an incremental expression against positional runtime inputs.
/// Input column names become HIR column identities; registers are supplied
/// only to physical emission and never written into parser syntax.
pub(crate) fn analyze_positional_scalar_syntax(
    context: &SemanticContext<'_>,
    database_id: usize,
    syntax: &ast::Expr,
    inputs: &[SchemaExprInput],
) -> Result<AnalyzedSchemaExpr> {
    let mut analyzer = Analyzer::new(context);
    let table = SchemaTable::new(
        "incremental expression inputs",
        inputs
            .iter()
            .map(|input| SchemaColumn::new(input.name.clone(), false, input.declared_type.clone()))
            .collect(),
        false,
    );
    let resolver = SemanticSchemaExprResolver {
        context,
        database_id,
        source_types: Vec::new(),
    };
    let expression = SchemaExpr::resolve(
        syntax,
        crate::schema_expr::SchemaExprProfile::GeneratedColumn,
        SchemaExprContext::for_table(&table),
        &resolver,
        ResolutionMode::Strict,
    )?;
    let source = analyzer.create_schema_expression_source(database_id, inputs)?;
    let expression = analyzer.instantiate_schema_expr(expression.as_valid()?, source)?;
    let document = analyzer.finish(hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
        source,
        expressions: vec![expression],
    }))?;
    Ok(AnalyzedSchemaExpr { document })
}

/// Resolve a stored expression against the same immutable catalog snapshot as
/// the HIR source that will execute it.
struct SemanticSchemaExprResolver<'context, 'catalog> {
    context: &'context SemanticContext<'catalog>,
    database_id: usize,
    source_types: Vec<(String, Arc<TypeDef>)>,
}

impl SemanticSchemaExprResolver<'_, '_> {
    fn source_type(&self, name: &str) -> Option<Arc<TypeDef>> {
        let name = normalize_ident(name);
        self.source_types
            .iter()
            .find(|(source_name, _)| source_name == &name)
            .map(|(_, definition)| Arc::clone(definition))
    }
}

impl SchemaExprResolver for SemanticSchemaExprResolver<'_, '_> {
    fn resolve_function(&self, name: &str, argument_count: usize) -> Result<Option<Func>> {
        self.context.resolve_function(name, argument_count)
    }

    fn resolve_collation(&self, name: &str) -> Result<crate::translate::collate::CollationSeq> {
        self.context.resolve_collation(name)
    }

    fn resolve_type(&self, name: &str) -> Result<Option<SchemaValueType>> {
        if self.source_type(name).is_some() {
            return Ok(Some(SchemaValueType::Custom(normalize_ident(name))));
        }
        if let Some(resolved) = SchemaExprResolver::resolve_type(&BuiltinSchemaExprResolver, name)?
        {
            return Ok(Some(resolved));
        }
        Ok(self
            .context
            .schema(self.database_id)
            .ok_or(LimboError::SchemaUpdated)?
            .get_type_def_unchecked(name)
            .map(|_| SchemaValueType::Custom(normalize_ident(name))))
    }

    fn resolve_custom_type(&self, name: &str) -> Result<Option<Arc<TypeDef>>> {
        if let Some(definition) = self.source_type(name) {
            return Ok(Some(definition));
        }
        Ok(self
            .context
            .schema(self.database_id)
            .ok_or(LimboError::SchemaUpdated)?
            .get_type_def_unchecked(name)
            .cloned())
    }
}

/// Analyze a valid stored expression for one explicit catalog table.
///
/// Stored expressions cannot contain query sources or subqueries, so this
/// creates exactly one root-owned source and does not populate query or index
/// metadata. The returned source is the complete identity boundary needed by
/// a VDBE-plan consumer.
pub(crate) fn analyze_schema_expr(
    context: &SemanticContext<'_>,
    database_id: usize,
    table: Arc<Table>,
    expression: &ValidSchemaExpr,
) -> Result<AnalyzedSchemaExpr> {
    let analyzed = analyze_schema_exprs(context, database_id, table, &[expression])?;
    Ok(AnalyzedSchemaExpr {
        document: analyzed.document,
    })
}

/// Analyze a batch of valid stored expressions for one explicit catalog
/// table. An empty batch is allowed and still returns the resolved source.
pub(crate) fn analyze_schema_exprs(
    context: &SemanticContext<'_>,
    database_id: usize,
    table: Arc<Table>,
    expressions: &[&ValidSchemaExpr],
) -> Result<AnalyzedSchemaExprs> {
    let mut analyzer = Analyzer::new(context);
    let source_id = create_table_schema_source(&mut analyzer, database_id, table)?;
    let expressions = expressions
        .iter()
        .map(|expression| analyzer.instantiate_schema_expr(expression, source_id))
        .collect::<Result<Vec<_>>>()?;
    finish_schema_expression_root(analyzer, source_id, expressions)
}

/// Resolve catalog-held parser syntax and close it into an independent HIR
/// root before physical emission begins.
pub(crate) fn analyze_table_schema_syntax(
    context: &SemanticContext<'_>,
    database_id: usize,
    table: Arc<Table>,
    expressions: &[SchemaSyntaxInput<'_>],
) -> Result<AnalyzedSchemaExprs> {
    let mut analyzer = Analyzer::new(context);
    let source_id = create_table_schema_source(&mut analyzer, database_id, table)?;
    let expressions = expressions
        .iter()
        .map(|input| match input.owner_column {
            Some(owner_column) => analyzer.instantiate_column_schema_syntax(
                input.syntax,
                input.profile,
                source_id,
                owner_column,
            ),
            None => {
                analyzer.instantiate_table_schema_syntax(input.syntax, input.profile, source_id)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    finish_schema_expression_root(analyzer, source_id, expressions)
}

fn create_table_schema_source(
    analyzer: &mut Analyzer<'_, '_>,
    database_id: usize,
    table: Arc<Table>,
) -> Result<SourceId> {
    if analyzer.context().schema(database_id).is_none() {
        return Err(LimboError::SchemaUpdated);
    }

    let table_name = normalize_ident(table.get_name());

    let mut columns = Vec::with_capacity(table.columns().len());
    let is_strict = matches!(table.as_ref(), Table::BTree(table) if table.is_strict);
    for (index, column) in table.columns().iter().enumerate() {
        let type_fact = analyzer.table_column_type_fact(column, database_id, is_strict)?;
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
                .map(|collation| analyzer.resolve_collation(&collation.to_string()))
                .transpose()?,
            hidden: column.hidden(),
            rowid_alias: column.is_rowid_alias(),
        });
    }

    let source_id = analyzer.reserve_source();
    analyzer.insert_source(
        source_id,
        hir::Source {
            id: source_id,
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(database_id)),
            name: table_name,
            alias: None,
            kind: SourceKind::SchemaExpression,
            generated_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
            default_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
            column_type_programs: vec![None; columns.len()],
            check_constraints: None,
            columns,
            rowid_available: matches!(table.as_ref(), Table::BTree(table) if table.has_rowid),
            index_hint: hir::IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: hir::IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        },
    )?;
    analyzer.bind_source_catalog_table(source_id, table.clone());
    analyzer.initialize_table_read_expression_slots(source_id, &table)?;

    Ok(source_id)
}

fn finish_schema_expression_root(
    analyzer: Analyzer<'_, '_>,
    source_id: SourceId,
    expressions: Vec<hir::Expr>,
) -> Result<AnalyzedSchemaExprs> {
    let document = analyzer.finish(hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
        source: source_id,
        expressions,
    }))?;
    Ok(AnalyzedSchemaExprs { document })
}

/// Analyze an ENCODE or DECODE expression against the synthetic positional
/// source `[value, user parameters...]`.
pub(crate) fn analyze_type_transform_schema_expr(
    context: &SemanticContext<'_>,
    database_id: usize,
    expression: &ValidSchemaExpr,
    inputs: &[SchemaExprInput],
) -> Result<AnalyzedSchemaExpr> {
    let expression = expression.specialize_type_parameters()?;
    analyze_synthetic_schema_expr(context, database_id, &expression, inputs)
}

/// Analyze a domain CHECK after binding its semantic `value` input to column
/// zero of a synthetic source.
pub(crate) fn analyze_domain_check_schema_expr(
    context: &SemanticContext<'_>,
    database_id: usize,
    expression: &ValidSchemaExpr,
    value: SchemaExprInput,
) -> Result<AnalyzedSchemaExpr> {
    let expression = expression.specialize_domain_value(SelfColumn::new(0, false))?;
    analyze_synthetic_schema_expr(context, database_id, &expression, &[value])
}

/// Analyze a stored expression with no row or named runtime inputs.
///
/// This is used for positionally stored type arguments and other standalone
/// schema values. Any unexpected source reference is rejected during HIR
/// instantiation instead of being converted back into parser syntax.
pub(crate) fn analyze_standalone_schema_expr(
    context: &SemanticContext<'_>,
    database_id: usize,
    expression: &ValidSchemaExpr,
) -> Result<AnalyzedSchemaExpr> {
    analyze_synthetic_schema_expr(context, database_id, expression, &[])
}

fn analyze_synthetic_schema_expr(
    context: &SemanticContext<'_>,
    database_id: usize,
    expression: &ValidSchemaExpr,
    inputs: &[SchemaExprInput],
) -> Result<AnalyzedSchemaExpr> {
    let mut analyzer = Analyzer::new(context);
    let (source_id, expression) =
        analyzer.instantiate_synthetic_schema_expr(database_id, expression, inputs)?;
    let document = analyzer.finish(hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
        source: source_id,
        expressions: vec![expression],
    }))?;
    Ok(AnalyzedSchemaExpr { document })
}

fn schema_expr_input_affinity(type_fact: &TypeFact) -> Affinity {
    if let Some(declared) = &type_fact.declared {
        if declared.array_dimensions == 0 && declared.custom().is_none() {
            return Affinity::affinity(&declared.name);
        }
    }
    match type_fact.storage {
        Some(Type::Integer) => Affinity::Integer,
        Some(Type::Real) => Affinity::Real,
        Some(Type::Text) => Affinity::Text,
        Some(Type::Numeric) => Affinity::Numeric,
        Some(Type::Null | Type::Blob) | None => Affinity::Blob,
    }
}

impl Analyzer<'_, '_> {
    /// Resolve catalog syntax that has no table columns. Type transforms,
    /// domain checks, and type arguments use this path until the catalog owns
    /// `SchemaExpr` directly.
    pub(crate) fn resolve_standalone_schema_syntax(
        &self,
        syntax: &ast::Expr,
        profile: crate::schema_expr::SchemaExprProfile,
        database_id: usize,
        expected_type: Option<&str>,
        type_parameters: &[ast::TypeParam],
        visible_types: &[Arc<TypeDef>],
    ) -> Result<SchemaExpr> {
        let parameters = type_parameters
            .iter()
            .map(|parameter| SchemaTypeParameter::new(parameter.name.clone(), parameter.ty.clone()))
            .collect::<Vec<_>>();
        let source_types = visible_types
            .iter()
            .map(|definition| (normalize_ident(&definition.name), Arc::clone(definition)))
            .collect();
        let resolver = SemanticSchemaExprResolver {
            context: self.context(),
            database_id,
            source_types,
        };
        let context = SchemaExprContext::without_table()
            .with_expected_type(expected_type)
            .with_type_parameters(&parameters);
        SchemaExpr::resolve(syntax, profile, context, &resolver, ResolutionMode::Strict)
    }

    /// Resolve a catalog column without letting connection-wide custom types
    /// change SQLite's declared-type rules for a non-STRICT table.
    pub(crate) fn table_column_type_fact(
        &mut self,
        column: &Column,
        database_id: usize,
        is_strict: bool,
    ) -> Result<TypeFact> {
        if column.ty_str.is_empty() {
            return Ok(TypeFact::known(column.ty()));
        }
        if is_strict {
            return self.resolve_declared_type_fact_in_database(
                &column.ty_str,
                column.array_dimensions(),
                database_id,
            );
        }

        let array_dimensions = column.array_dimensions();
        let storage = if array_dimensions > 0 {
            Type::Blob
        } else {
            column.ty()
        };
        Ok(TypeFact::declared(hir::DeclaredType {
            name: column.ty_str.clone(),
            storage,
            custom_chain: Vec::new(),
            array_dimensions,
        }))
    }

    /// Insert positional schema-program inputs into this Analyzer's source
    /// arena so the finished HIR document owns their identities.
    pub(crate) fn create_schema_expression_source(
        &mut self,
        database_id: usize,
        inputs: &[SchemaExprInput],
    ) -> Result<SourceId> {
        if self.context().schema(database_id).is_none() {
            return Err(LimboError::SchemaUpdated);
        }

        let mut columns = Vec::with_capacity(inputs.len());
        for input in inputs {
            let type_fact = match (&input.type_fact, &input.declared_type) {
                (Some(type_fact), _) => type_fact.clone(),
                (None, Some(declared_type)) => self.resolve_declared_type_fact_in_database(
                    declared_type,
                    input.array_dimensions,
                    database_id,
                )?,
                (None, None) => TypeFact::dynamic(),
            };
            columns.push(hir::SourceColumn {
                name: input.name.clone(),
                affinity: schema_expr_input_affinity(&type_fact),
                has_affinity: true,
                type_fact,
                collation: None,
                hidden: false,
                rowid_alias: false,
            });
        }

        let source = self.reserve_source();
        self.insert_source(
            source,
            hir::Source {
                id: source,
                owner: SourceOwner::Root,
                database: Some(DatabaseId::new(database_id)),
                name: "schema expression inputs".to_string(),
                alias: None,
                kind: SourceKind::SchemaExpression,
                generated_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                default_expressions: vec![hir::ColumnReadExpression::Absent; columns.len()],
                column_type_programs: vec![None; columns.len()],
                check_constraints: None,
                columns,
                rowid_available: false,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            },
        )?;
        Ok(source)
    }

    /// Instantiate an already-specialized expression against a new synthetic
    /// source owned by this HIR document.
    pub(crate) fn instantiate_synthetic_schema_expr(
        &mut self,
        database_id: usize,
        expression: &ValidSchemaExpr,
        inputs: &[SchemaExprInput],
    ) -> Result<(SourceId, hir::Expr)> {
        let source = self.create_schema_expression_source(database_id, inputs)?;
        let expression = self.instantiate_schema_expr(expression, source)?;
        Ok((source, expression))
    }

    /// Resolve a column-owned expression on demand, then instantiate it
    /// against the exact source occurrence that requires the value.
    pub(crate) fn instantiate_column_schema_expr(
        &mut self,
        expression: &SchemaExpr,
        source: SourceId,
        owner_column: usize,
    ) -> Result<hir::Expr> {
        self.instantiate_source_schema_expr(expression, source, Some(owner_column))
    }

    /// Resolve a table-owned expression on demand, then instantiate it against
    /// the exact source occurrence that requires the value.
    pub(crate) fn instantiate_table_schema_expr(
        &mut self,
        expression: &SchemaExpr,
        source: SourceId,
    ) -> Result<hir::Expr> {
        self.instantiate_source_schema_expr(expression, source, None)
    }

    /// Resolve parser syntax still held by the legacy catalog, then bind its
    /// positional meaning to one concrete source occurrence.
    ///
    /// This adapter belongs at the schema-expression boundary: query and DML
    /// analysis never call the old statement binder. Once the catalog stores
    /// `SchemaExpr` directly, callers move to `instantiate_*_schema_expr` and
    /// this compatibility entry point disappears.
    pub(crate) fn instantiate_column_schema_syntax(
        &mut self,
        syntax: &ast::Expr,
        profile: crate::schema_expr::SchemaExprProfile,
        source: SourceId,
        owner_column: usize,
    ) -> Result<hir::Expr> {
        self.instantiate_source_schema_syntax(syntax, profile, source, Some(owner_column))
    }

    pub(crate) fn instantiate_table_schema_syntax(
        &mut self,
        syntax: &ast::Expr,
        profile: crate::schema_expr::SchemaExprProfile,
        source: SourceId,
    ) -> Result<hir::Expr> {
        self.instantiate_source_schema_syntax(syntax, profile, source, None)
    }

    fn instantiate_source_schema_syntax(
        &mut self,
        syntax: &ast::Expr,
        profile: crate::schema_expr::SchemaExprProfile,
        source: SourceId,
        owner_column: Option<usize>,
    ) -> Result<hir::Expr> {
        let expression = SchemaExpr::preserve_unresolved(syntax.clone(), profile);
        self.instantiate_source_schema_expr(&expression, source, owner_column)
    }

    /// Schema loading may preserve parser syntax before user-defined types are
    /// available. Required stored expressions must be retried at the semantic
    /// boundary, where the owning source and catalog snapshot are both fixed,
    /// instead of replaying the earlier incomplete type error.
    fn instantiate_source_schema_expr(
        &mut self,
        expression: &SchemaExpr,
        source: SourceId,
        owner_column: Option<usize>,
    ) -> Result<hir::Expr> {
        let Some(unresolved) = expression.as_unresolved() else {
            return self.instantiate_schema_expr(expression.as_valid()?, source);
        };

        let (database_id, table, expected_type, source_types) = {
            let source = self.source(source).ok_or_else(|| {
                LimboError::InternalError(format!("stored expression source {source} is missing"))
            })?;
            let database_id = source.database.map(DatabaseId::index).ok_or_else(|| {
                LimboError::InternalError(
                    "stored schema expression source has no database identity".to_string(),
                )
            })?;
            let expected_type = match owner_column {
                Some(owner_column) => source
                    .columns
                    .get(owner_column)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "stored expression owner column {owner_column} is outside source {}",
                            source.id
                        ))
                    })?
                    .type_fact
                    .declared
                    .as_ref()
                    .map(|declared| declared.name.clone()),
                None => None,
            };
            let source_types = source
                .columns
                .iter()
                .filter_map(|column| column.type_fact.declared.as_ref())
                .flat_map(|declared| declared.custom_chain.iter())
                .map(|definition| {
                    (
                        normalize_ident(&definition.value().name),
                        definition.handle(),
                    )
                })
                .collect();
            let columns = source
                .columns
                .iter()
                .map(|column| {
                    SchemaColumn::new(
                        column.name.clone(),
                        column.rowid_alias,
                        column
                            .type_fact
                            .declared
                            .as_ref()
                            .map(|declared| declared.name.clone()),
                    )
                })
                .collect();
            (
                database_id,
                SchemaTable::new(source.name.clone(), columns, source.rowid_available),
                expected_type,
                source_types,
            )
        };

        let resolution_context = if expression.profile().allows_table_columns() {
            SchemaExprContext::for_table(&table)
        } else {
            SchemaExprContext::without_table()
        }
        .with_expected_type(expected_type.as_deref());
        let resolver = SemanticSchemaExprResolver {
            context: self.context(),
            database_id,
            source_types,
        };
        let resolved = SchemaExpr::resolve(
            unresolved.syntax(),
            expression.profile(),
            resolution_context,
            &resolver,
            ResolutionMode::Strict,
        )?;
        self.instantiate_schema_expr(resolved.as_valid()?, source)
    }

    /// Instantiate a valid stored expression for one concrete table source.
    /// This is the only point where schema-local column positions become HIR
    /// source identities.
    pub(crate) fn instantiate_schema_expr(
        &mut self,
        expr: &ValidSchemaExpr,
        source: SourceId,
    ) -> Result<hir::Expr> {
        self.schema_expr_database(source)?;
        let expression = self.instantiate_schema_node(expr.root(), source)?;
        self.require_source_columns_in_expr(&expression);
        Ok(expression)
    }

    fn instantiate_schema_node(
        &mut self,
        expr: &SchemaExprNode,
        source: SourceId,
    ) -> Result<hir::Expr> {
        match expr {
            SchemaExprNode::Between {
                lhs,
                not,
                start,
                end,
            } => {
                let expr = self.instantiate_schema_node(lhs, source)?;
                let start = self.instantiate_schema_node(start, source)?;
                let end = self.instantiate_schema_node(end, source)?;
                let scope = Scope::default();
                let start_comparison = self.comparison_semantics(&expr, &start, &scope, false)?;
                let end_comparison = self.comparison_semantics(&expr, &end, &scope, false)?;
                Ok(hir::Expr::Between {
                    expr: Box::new(expr),
                    negated: *not,
                    start: Box::new(start),
                    end: Box::new(end),
                    start_comparison,
                    end_comparison,
                })
            }
            SchemaExprNode::Binary(lhs, operator, rhs) => {
                let lhs = self.instantiate_schema_node(lhs, source)?;
                let rhs = self.instantiate_schema_node(rhs, source)?;
                let custom =
                    self.resolve_custom_binary_operator(&lhs, *operator, &rhs, &Scope::default())?;
                let array_concat = *operator == ast::Operator::Concat
                    && (self
                        .expression_type_fact(&lhs, &Scope::default())
                        .is_array()
                        || self
                            .expression_type_fact(&rhs, &Scope::default())
                            .is_array());
                let comparison = operator
                    .is_comparison()
                    .then(|| self.comparison_semantics(&lhs, &rhs, &Scope::default(), false))
                    .transpose()?;
                Ok(hir::Expr::Binary {
                    lhs: Box::new(lhs),
                    operator: *operator,
                    rhs: Box::new(rhs),
                    array_concat,
                    custom,
                    comparison,
                })
            }
            SchemaExprNode::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                let base = base
                    .as_deref()
                    .map(|expr| self.instantiate_schema_node(expr, source).map(Box::new))
                    .transpose()?;
                let when_then = when_then_pairs
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            self.instantiate_schema_node(when, source)?,
                            self.instantiate_schema_node(then, source)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = else_expr
                    .as_deref()
                    .map(|expr| self.instantiate_schema_node(expr, source).map(Box::new))
                    .transpose()?;
                let base_comparisons = base.as_deref().map_or_else(
                    || Ok(Vec::new()),
                    |base| {
                        when_then
                            .iter()
                            .map(|(when, _)| {
                                self.comparison_semantics(base, when, &Scope::default(), false)
                            })
                            .collect::<Result<Vec<_>>>()
                    },
                )?;
                Ok(hir::Expr::Case {
                    base,
                    when_then,
                    else_expr,
                    base_comparisons,
                })
            }
            SchemaExprNode::Cast {
                expr,
                type_name,
                resolved_type,
            } => {
                let database = self.schema_expr_database(source)?;
                let type_fact = self.schema_type_fact(
                    type_name.name(),
                    type_name.array_dimensions(),
                    resolved_type.as_ref(),
                    database,
                )?;
                let parameters = self.schema_type_parameters(type_name, source)?;
                let programs =
                    self.bind_cast_programs(&type_fact, &parameters, &Scope::default())?;
                let affinity = if programs.apply_builtin_affinity {
                    if type_name.name().is_empty() {
                        Affinity::Numeric
                    } else {
                        Affinity::affinity(type_name.name())
                    }
                } else {
                    super::expr::type_fact_affinity(&type_fact)
                };
                Ok(hir::Expr::Cast {
                    expr: Box::new(self.instantiate_schema_node(expr, source)?),
                    target: hir::TypeName {
                        name: type_name.name().to_string(),
                        parameters,
                        array_dimensions: type_name.array_dimensions(),
                        type_fact,
                        affinity,
                        programs,
                    },
                })
            }
            SchemaExprNode::Collate {
                expr,
                name,
                collation,
            } => {
                let id = self.catalog_object_id(
                    None,
                    CatalogObjectKind::Collation,
                    normalize_ident(name.as_str()),
                );
                Ok(hir::Expr::Collate {
                    expr: Box::new(self.instantiate_schema_node(expr, source)?),
                    collation: CatalogObject::new(
                        id,
                        self.context().snapshot(),
                        None,
                        Arc::new(*collation),
                    ),
                })
            }
            SchemaExprNode::FieldAccess {
                base,
                field,
                resolution,
            } => {
                let base = self.instantiate_schema_node(base, source)?;
                let container_type = self.schema_expr_custom_type(&base).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "stored field access {} has no custom container type",
                        field.as_str()
                    ))
                })?;
                let database = container_type.database().ok_or_else(|| {
                    LimboError::InternalError(
                        "stored field access type has no database identity".to_string(),
                    )
                })?;
                let (kind, result_name, result_array_dimensions) = match resolution {
                    SchemaFieldAccess::StructField { field_index } => {
                        let definition = container_type
                            .value()
                            .struct_def()
                            .and_then(|definition| definition.fields.get(*field_index))
                            .ok_or(LimboError::SchemaUpdated)?;
                        if !definition.name.eq_ignore_ascii_case(field.as_str()) {
                            return Err(LimboError::SchemaUpdated);
                        }
                        (
                            hir::FieldAccessKind::Struct {
                                field_index: *field_index,
                            },
                            definition.type_name.clone(),
                            definition.array_dimensions,
                        )
                    }
                    SchemaFieldAccess::UnionVariant { tag_index } => {
                        let definition = container_type
                            .value()
                            .union_def()
                            .and_then(|definition| {
                                definition
                                    .variants
                                    .iter()
                                    .find(|variant| variant.tag_index == *tag_index)
                            })
                            .ok_or(LimboError::SchemaUpdated)?;
                        if !definition.tag_name.eq_ignore_ascii_case(field.as_str()) {
                            return Err(LimboError::SchemaUpdated);
                        }
                        (
                            hir::FieldAccessKind::Union {
                                tag_index: *tag_index,
                            },
                            definition.type_name.clone(),
                            definition.array_dimensions,
                        )
                    }
                };
                let result_type = self.schema_type_fact(
                    &result_name,
                    result_array_dimensions,
                    None,
                    database.index(),
                )?;
                Ok(hir::Expr::FieldAccess(hir::FieldAccess {
                    base: Box::new(base),
                    field_name: field.as_str().to_string(),
                    kind,
                    container_type,
                    result_type,
                }))
            }
            SchemaExprNode::CustomTypeFunction { call, resolution } => {
                let call = self.instantiate_schema_node(call, source)?;
                self.instantiate_custom_type_operation(call, resolution, source)
            }
            SchemaExprNode::Function {
                name,
                function,
                distinctness,
                args,
                star,
            } => {
                let arguments = args
                    .iter()
                    .map(|arg| self.instantiate_schema_node(arg, source))
                    .collect::<Result<Vec<_>>>()?;
                let sequence_operation =
                    self.resolve_schema_sequence_operation(function, &arguments)?;
                let scope = Scope::default();
                let argument_types = arguments
                    .iter()
                    .map(|argument| self.expression_type_fact(argument, &scope))
                    .collect::<Vec<_>>();
                let result_type = super::expr::builtin_function_result_type(
                    function,
                    &argument_types,
                    &arguments,
                    None,
                );
                let id = self.catalog_object_id(
                    None,
                    CatalogObjectKind::Function {
                        argument_count: args.len(),
                    },
                    normalize_ident(name.as_str()),
                );
                Ok(hir::Expr::Function(hir::FunctionCall {
                    function: CatalogObject::new(
                        id,
                        self.context().snapshot(),
                        None,
                        Arc::new(function.clone()),
                    ),
                    evaluation: hir::FunctionEvaluation::Scalar,
                    star: *star,
                    arguments,
                    distinctness: *distinctness,
                    argument_order: Vec::new(),
                    within_group: Vec::new(),
                    filter: None,
                    window: None,
                    result_type,
                    custom_type_operation: None,
                    sequence_operation,
                }))
            }
            SchemaExprNode::SelfColumn(column) => {
                let source_definition = self.source(source).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "stored expression source {source} is missing"
                    ))
                })?;
                let Some(definition) = source_definition.columns.get(column.position()) else {
                    return Err(LimboError::SchemaUpdated);
                };
                if definition.rowid_alias != column.is_rowid_alias() {
                    return Err(LimboError::SchemaUpdated);
                }
                Ok(hir::Expr::column(source, column.position()))
            }
            SchemaExprNode::SelfRowId => {
                let source_definition = self.source(source).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "stored expression source {source} is missing"
                    ))
                })?;
                if !source_definition.rowid_available {
                    return Err(LimboError::SchemaUpdated);
                }
                Ok(hir::Expr::rowid(source))
            }
            SchemaExprNode::DomainValue => Err(LimboError::InternalError(
                "domain CHECK value must be specialized before HIR instantiation".to_string(),
            )),
            SchemaExprNode::TypeParameter { .. } => Err(LimboError::InternalError(
                "type transform parameter must be specialized before HIR instantiation".to_string(),
            )),
            SchemaExprNode::InList { lhs, not, rhs } => {
                let lhs = self.instantiate_schema_node(lhs, source)?;
                let values = rhs
                    .iter()
                    .map(|expr| self.instantiate_schema_node(expr, source))
                    .collect::<Result<Vec<_>>>()?;
                let comparisons = values
                    .iter()
                    .map(|value| self.comparison_semantics(&lhs, value, &Scope::default(), true))
                    .collect::<Result<Vec<_>>>()?;
                Ok(hir::Expr::InList {
                    lhs: Box::new(lhs),
                    negated: *not,
                    values,
                    comparisons,
                })
            }
            SchemaExprNode::IsNull(expr) => Ok(hir::Expr::IsNull(Box::new(
                self.instantiate_schema_node(expr, source)?,
            ))),
            SchemaExprNode::Like {
                lhs,
                not,
                op,
                rhs,
                escape,
            } => {
                let lhs = self.instantiate_schema_node(lhs, source)?;
                let rhs = self.instantiate_schema_node(rhs, source)?;
                let escape = escape
                    .as_deref()
                    .map(|expr| self.instantiate_schema_node(expr, source).map(Box::new))
                    .transpose()?;
                let (function, argument_count) =
                    self.resolve_like_operator_function(*op, &lhs, escape.is_some())?;
                Ok(hir::Expr::Like {
                    lhs: Box::new(lhs),
                    negated: *not,
                    operator: *op,
                    function,
                    argument_count,
                    rhs: Box::new(rhs),
                    escape,
                })
            }
            SchemaExprNode::Literal(literal) => Ok(hir::Expr::Literal(literal.clone())),
            SchemaExprNode::NotNull(expr) => Ok(hir::Expr::NotNull(Box::new(
                self.instantiate_schema_node(expr, source)?,
            ))),
            SchemaExprNode::Parenthesized(exprs) if exprs.len() == 1 => {
                self.instantiate_schema_node(&exprs[0], source)
            }
            SchemaExprNode::Parenthesized(exprs) => Ok(hir::Expr::Row(
                exprs
                    .iter()
                    .map(|expr| self.instantiate_schema_node(expr, source))
                    .collect::<Result<_>>()?,
            )),
            SchemaExprNode::Unary(operator, expr) => Ok(hir::Expr::Unary {
                operator: *operator,
                expr: Box::new(self.instantiate_schema_node(expr, source)?),
            }),
            SchemaExprNode::Array(elements) => Ok(hir::Expr::Array(
                elements
                    .iter()
                    .map(|expr| self.instantiate_schema_node(expr, source))
                    .collect::<Result<_>>()?,
            )),
            SchemaExprNode::Subscript { base, index } => Ok(hir::Expr::Subscript {
                base: Box::new(self.instantiate_schema_node(base, source)?),
                index: Box::new(self.instantiate_schema_node(index, source)?),
            }),
            SchemaExprNode::Raise { action, message } => Ok(hir::Expr::Raise {
                action: *action,
                message: message
                    .as_deref()
                    .map(|message| self.instantiate_schema_node(message, source).map(Box::new))
                    .transpose()?,
            }),
        }
    }

    fn schema_type_parameters(
        &mut self,
        type_name: &SchemaTypeName,
        source: SourceId,
    ) -> Result<Vec<hir::Expr>> {
        match type_name.size() {
            None => Ok(Vec::new()),
            Some(SchemaTypeSize::MaxSize(size)) => {
                Ok(vec![self.instantiate_schema_node(size, source)?])
            }
            Some(SchemaTypeSize::TypeSize(precision, scale)) => Ok(vec![
                self.instantiate_schema_node(precision, source)?,
                self.instantiate_schema_node(scale, source)?,
            ]),
        }
    }

    fn schema_expr_database(&self, source: SourceId) -> Result<usize> {
        let source = self.source(source).ok_or_else(|| {
            LimboError::InternalError(format!("stored expression source {source} is missing"))
        })?;
        source.database.map(DatabaseId::index).ok_or_else(|| {
            LimboError::InternalError(
                "stored schema expression source has no database identity".to_string(),
            )
        })
    }

    fn resolve_schema_sequence_operation(
        &mut self,
        function: &Func,
        arguments: &[hir::Expr],
    ) -> Result<Option<hir::SequenceOperation>> {
        let kind = match function {
            Func::Scalar(ScalarFunc::NextVal) => hir::SequenceOperationKind::NextValue,
            Func::Scalar(ScalarFunc::SetVal) => hir::SequenceOperationKind::SetValue,
            _ => return Ok(None),
        };
        let user_name = match arguments.first() {
            Some(hir::Expr::Literal(ast::Literal::String(name))) => {
                name.trim_matches('\'').to_string()
            }
            _ => crate::bail_parse_error!("expected a string literal argument"),
        };
        self.resolve_sequence_catalog_operation(kind, user_name)
            .map(Some)
    }

    fn schema_type_fact(
        &mut self,
        name: &str,
        array_dimensions: u32,
        resolved: Option<&SchemaValueType>,
        database: usize,
    ) -> Result<TypeFact> {
        let custom_name = match resolved {
            Some(SchemaValueType::Custom(name)) => Some(name.as_str()),
            _ => None,
        };
        let lookup_name = custom_name.unwrap_or(name);
        let resolved_custom = self
            .context()
            .schema(database)
            .ok_or(LimboError::SchemaUpdated)?
            .resolve_type_unchecked(lookup_name)?;
        if let Some(resolved_custom) = resolved_custom {
            let definition = resolved_custom
                .chain
                .first()
                .cloned()
                .ok_or(LimboError::SchemaUpdated)?;
            let storage = if array_dimensions > 0 {
                Type::Blob
            } else {
                type_from_name(definition.base()).0
            };
            let mut custom_chain = Vec::with_capacity(resolved_custom.chain.len());
            for definition in resolved_custom.chain {
                let id = self.catalog_object_id(
                    Some(database),
                    CatalogObjectKind::Type,
                    normalize_ident(&definition.name),
                );
                custom_chain.push(CatalogObject::new(
                    id,
                    self.context().snapshot(),
                    Some(DatabaseId::new(database)),
                    definition,
                ));
            }
            return Ok(TypeFact::declared(hir::DeclaredType {
                name: name.to_string(),
                storage,
                custom_chain,
                array_dimensions,
            }));
        }

        let storage = match resolved {
            _ if array_dimensions > 0 => Type::Blob,
            Some(SchemaValueType::Integer) => Type::Integer,
            Some(SchemaValueType::Real) => Type::Real,
            Some(SchemaValueType::Text) => Type::Text,
            Some(SchemaValueType::Blob) => Type::Blob,
            Some(SchemaValueType::Any) => type_from_name(name).0,
            Some(SchemaValueType::Custom(_)) => return Err(LimboError::SchemaUpdated),
            None => type_from_name(name).0,
        };
        Ok(TypeFact::declared(hir::DeclaredType {
            name: name.to_string(),
            storage,
            custom_chain: Vec::new(),
            array_dimensions,
        }))
    }

    fn schema_expr_custom_type(&self, expr: &hir::Expr) -> Option<hir::ResolvedType> {
        self.schema_expr_type_fact(expr)?
            .declared
            .and_then(|declared| declared.custom().cloned())
    }

    fn schema_expr_type_fact(&self, expr: &hir::Expr) -> Option<TypeFact> {
        match expr {
            hir::Expr::Column(column) => self
                .source(column.source)?
                .columns
                .get(column.column)
                .map(|column| column.type_fact.clone()),
            hir::Expr::MergedColumn(column) => Some(column.type_fact.clone()),
            hir::Expr::RowId(_) => Some(TypeFact::known(Type::Integer)),
            hir::Expr::Cast { target, .. } => Some(target.type_fact.clone()),
            hir::Expr::Function(call) => Some(call.result_type.clone()),
            hir::Expr::FieldAccess(access) => Some(access.result_type.clone()),
            hir::Expr::Collate { expr, .. } => self.schema_expr_type_fact(expr),
            _ => None,
        }
    }

    fn instantiate_custom_type_operation(
        &mut self,
        call: hir::Expr,
        resolution: &SchemaCustomTypeFunction,
        source: SourceId,
    ) -> Result<hir::Expr> {
        let hir::Expr::Function(mut call) = call else {
            return Err(LimboError::InternalError(
                "stored custom-type operation does not contain a function".to_string(),
            ));
        };
        let database = self.schema_expr_database(source)?;
        let (operation, result_type) = match resolution {
            SchemaCustomTypeFunction::UnionValue {
                tag_index,
                tag_name,
                result_type,
            } => {
                let result = self.schema_type_fact(result_type, 0, None, database)?;
                let union_type = result
                    .declared
                    .as_ref()
                    .and_then(|declared| declared.custom().cloned())
                    .filter(|definition| definition.value().is_union())
                    .ok_or(LimboError::SchemaUpdated)?;
                if !union_type.value().union_def().is_some_and(|definition| {
                    definition.variants.iter().any(|variant| {
                        variant.tag_index == *tag_index
                            && variant.tag_name.eq_ignore_ascii_case(tag_name)
                    })
                }) {
                    return Err(LimboError::SchemaUpdated);
                }
                (
                    hir::CustomTypeOperation::UnionValue {
                        union_type,
                        tag_index: *tag_index,
                        result_type: result.clone(),
                    },
                    result,
                )
            }
            SchemaCustomTypeFunction::UnionTag { tag_names } => {
                let union_type = call
                    .arguments
                    .first()
                    .and_then(|argument| self.schema_expr_custom_type(argument))
                    .filter(|definition| definition.value().is_union())
                    .ok_or(LimboError::SchemaUpdated)?;
                let current_names = &union_type
                    .value()
                    .union_def()
                    .ok_or(LimboError::SchemaUpdated)?
                    .tag_names;
                if current_names.as_ref() != tag_names.as_slice() {
                    return Err(LimboError::SchemaUpdated);
                }
                (
                    hir::CustomTypeOperation::UnionTag {
                        union_type,
                        tag_names: Arc::from(tag_names.clone()),
                    },
                    TypeFact::known(Type::Text),
                )
            }
            SchemaCustomTypeFunction::UnionExtract {
                tag_index,
                tag_name,
                result_type,
                result_array_dimensions,
            } => {
                let union_type = call
                    .arguments
                    .first()
                    .and_then(|argument| self.schema_expr_custom_type(argument))
                    .filter(|definition| definition.value().is_union())
                    .ok_or(LimboError::SchemaUpdated)?;
                let current_result = union_type
                    .value()
                    .union_def()
                    .and_then(|definition| {
                        definition.variants.iter().find(|variant| {
                            variant.tag_index == *tag_index
                                && variant.tag_name.eq_ignore_ascii_case(tag_name)
                        })
                    })
                    .map(|variant| (variant.type_name.clone(), variant.array_dimensions))
                    .ok_or(LimboError::SchemaUpdated)?;
                if !current_result.0.eq_ignore_ascii_case(result_type)
                    || current_result.1 != *result_array_dimensions
                {
                    return Err(LimboError::SchemaUpdated);
                }
                let result =
                    self.schema_type_fact(result_type, *result_array_dimensions, None, database)?;
                (
                    hir::CustomTypeOperation::UnionExtract {
                        union_type,
                        tag_index: *tag_index,
                        result_type: result.clone(),
                    },
                    result,
                )
            }
            SchemaCustomTypeFunction::StructExtract {
                field_index,
                field_name,
                result_type,
                result_array_dimensions,
            } => {
                let struct_type = call
                    .arguments
                    .first()
                    .and_then(|argument| self.schema_expr_custom_type(argument))
                    .filter(|definition| definition.value().is_struct())
                    .ok_or(LimboError::SchemaUpdated)?;
                let current_result = struct_type
                    .value()
                    .struct_def()
                    .and_then(|definition| definition.fields.get(*field_index))
                    .filter(|field| field.name.eq_ignore_ascii_case(field_name))
                    .map(|field| (field.type_name.clone(), field.array_dimensions))
                    .ok_or(LimboError::SchemaUpdated)?;
                if !current_result.0.eq_ignore_ascii_case(result_type)
                    || current_result.1 != *result_array_dimensions
                {
                    return Err(LimboError::SchemaUpdated);
                }
                let result =
                    self.schema_type_fact(result_type, *result_array_dimensions, None, database)?;
                (
                    hir::CustomTypeOperation::StructExtract {
                        struct_type,
                        field_index: *field_index,
                        result_type: result.clone(),
                    },
                    result,
                )
            }
        };
        call.result_type = result_type;
        call.custom_type_operation = Some(operation);
        Ok(hir::Expr::Function(call))
    }
}

/// Semantic expression equivalence used by expression indexes and UPSERT
/// conflict-target matching. Commutative binary operands may be reversed.
pub(crate) fn equivalent(lhs: &hir::Expr, rhs: &hir::Expr) -> bool {
    match (lhs, rhs) {
        (hir::Expr::Literal(lhs), hir::Expr::Literal(rhs)) => check_literal_equivalency(lhs, rhs),
        (hir::Expr::Parameter(lhs), hir::Expr::Parameter(rhs)) => {
            lhs.index == rhs.index && lhs.name == rhs.name
        }
        (hir::Expr::Column(lhs), hir::Expr::Column(rhs)) => lhs == rhs,
        (hir::Expr::MergedColumn(lhs), hir::Expr::MergedColumn(rhs)) => {
            equivalent(lhs.left.as_ref(), rhs.left.as_ref())
                && lhs.right == rhs.right
                && lhs.value == rhs.value
                && optional_catalog_equivalent(lhs.collation.as_ref(), rhs.collation.as_ref())
        }
        (hir::Expr::RowId(lhs), hir::Expr::RowId(rhs)) => lhs == rhs,
        (hir::Expr::Output(lhs), hir::Expr::Output(rhs)) => lhs == rhs,
        (
            hir::Expr::Unary {
                operator: lhs_operator,
                expr: lhs,
            },
            hir::Expr::Unary {
                operator: rhs_operator,
                expr: rhs,
            },
        ) => lhs_operator == rhs_operator && equivalent(lhs, rhs),
        (
            hir::Expr::Binary {
                lhs: lhs_left,
                operator: lhs_operator,
                rhs: lhs_right,
                array_concat: lhs_array_concat,
                custom: lhs_custom,
                comparison: lhs_comparison,
            },
            hir::Expr::Binary {
                lhs: rhs_left,
                operator: rhs_operator,
                rhs: rhs_right,
                array_concat: rhs_array_concat,
                custom: rhs_custom,
                comparison: rhs_comparison,
            },
        ) => {
            lhs_operator == rhs_operator
                && lhs_array_concat == rhs_array_concat
                && lhs_comparison == rhs_comparison
                && ((equivalent(lhs_left, rhs_left)
                    && equivalent(lhs_right, rhs_right)
                    && custom_binary_operator_equivalent(
                        lhs_custom.as_ref(),
                        rhs_custom.as_ref(),
                        false,
                    ))
                    || (lhs_operator.is_commutative()
                        && equivalent(lhs_left, rhs_right)
                        && equivalent(lhs_right, rhs_left)
                        && custom_binary_operator_equivalent(
                            lhs_custom.as_ref(),
                            rhs_custom.as_ref(),
                            true,
                        )))
        }
        (
            hir::Expr::Between {
                expr: lhs,
                negated: lhs_negated,
                start: lhs_start,
                end: lhs_end,
                start_comparison: lhs_start_comparison,
                end_comparison: lhs_end_comparison,
            },
            hir::Expr::Between {
                expr: rhs,
                negated: rhs_negated,
                start: rhs_start,
                end: rhs_end,
                start_comparison: rhs_start_comparison,
                end_comparison: rhs_end_comparison,
            },
        ) => {
            lhs_negated == rhs_negated
                && lhs_start_comparison == rhs_start_comparison
                && lhs_end_comparison == rhs_end_comparison
                && equivalent(lhs, rhs)
                && equivalent(lhs_start, rhs_start)
                && equivalent(lhs_end, rhs_end)
        }
        (
            hir::Expr::Case {
                base: lhs_base,
                when_then: lhs_pairs,
                else_expr: lhs_else,
                base_comparisons: lhs_comparisons,
            },
            hir::Expr::Case {
                base: rhs_base,
                when_then: rhs_pairs,
                else_expr: rhs_else,
                base_comparisons: rhs_comparisons,
            },
        ) => {
            lhs_comparisons == rhs_comparisons
                && optional_expr_equivalent(lhs_base.as_deref(), rhs_base.as_deref())
                && expr_pair_slices_equivalent(lhs_pairs, rhs_pairs)
                && optional_expr_equivalent(lhs_else.as_deref(), rhs_else.as_deref())
        }
        (
            hir::Expr::Cast {
                expr: lhs,
                target: lhs_target,
            },
            hir::Expr::Cast {
                expr: rhs,
                target: rhs_target,
            },
        ) => equivalent(lhs, rhs) && type_name_equivalent(lhs_target, rhs_target),
        (
            hir::Expr::Collate {
                expr: lhs,
                collation: lhs_collation,
            },
            hir::Expr::Collate {
                expr: rhs,
                collation: rhs_collation,
            },
        ) => catalog_equivalent(lhs_collation, rhs_collation) && equivalent(lhs, rhs),
        (hir::Expr::Function(lhs), hir::Expr::Function(rhs)) => function_equivalent(lhs, rhs),
        (hir::Expr::IsNull(lhs), hir::Expr::IsNull(rhs))
        | (hir::Expr::NotNull(lhs), hir::Expr::NotNull(rhs)) => equivalent(lhs, rhs),
        (
            hir::Expr::InList {
                lhs,
                negated: lhs_negated,
                values: lhs_values,
                comparisons: lhs_comparisons,
            },
            hir::Expr::InList {
                lhs: rhs,
                negated: rhs_negated,
                values: rhs_values,
                comparisons: rhs_comparisons,
            },
        ) => {
            lhs_negated == rhs_negated
                && lhs_comparisons == rhs_comparisons
                && equivalent(lhs, rhs)
                && expr_slices_equivalent(lhs_values, rhs_values)
        }
        (hir::Expr::Subquery(lhs), hir::Expr::Subquery(rhs)) => subquery_equivalent(lhs, rhs),
        (
            hir::Expr::Like {
                lhs,
                negated: lhs_negated,
                operator: lhs_operator,
                function: lhs_function,
                argument_count: lhs_argument_count,
                rhs: lhs_pattern,
                escape: lhs_escape,
            },
            hir::Expr::Like {
                lhs: rhs,
                negated: rhs_negated,
                operator: rhs_operator,
                function: rhs_function,
                argument_count: rhs_argument_count,
                rhs: rhs_pattern,
                escape: rhs_escape,
            },
        ) => {
            lhs_negated == rhs_negated
                && lhs_operator == rhs_operator
                && catalog_equivalent(lhs_function, rhs_function)
                && lhs_argument_count == rhs_argument_count
                && equivalent(lhs, rhs)
                && equivalent(lhs_pattern, rhs_pattern)
                && optional_expr_equivalent(lhs_escape.as_deref(), rhs_escape.as_deref())
        }
        (hir::Expr::Row(lhs), hir::Expr::Row(rhs))
        | (hir::Expr::Array(lhs), hir::Expr::Array(rhs)) => expr_slices_equivalent(lhs, rhs),
        (
            hir::Expr::Subscript {
                base: lhs_base,
                index: lhs_index,
            },
            hir::Expr::Subscript {
                base: rhs_base,
                index: rhs_index,
            },
        ) => equivalent(lhs_base, rhs_base) && equivalent(lhs_index, rhs_index),
        (hir::Expr::FieldAccess(lhs), hir::Expr::FieldAccess(rhs)) => {
            lhs.kind == rhs.kind
                && catalog_equivalent(&lhs.container_type, &rhs.container_type)
                && equivalent(&lhs.base, &rhs.base)
        }
        (
            hir::Expr::Raise {
                action: lhs_action,
                message: lhs_message,
            },
            hir::Expr::Raise {
                action: rhs_action,
                message: rhs_message,
            },
        ) => {
            lhs_action == rhs_action
                && optional_expr_equivalent(lhs_message.as_deref(), rhs_message.as_deref())
        }
        _ => false,
    }
}

fn function_equivalent(lhs: &hir::FunctionCall, rhs: &hir::FunctionCall) -> bool {
    catalog_equivalent(&lhs.function, &rhs.function)
        && lhs.star == rhs.star
        && lhs.distinctness == rhs.distinctness
        && expr_slices_equivalent(&lhs.arguments, &rhs.arguments)
        && order_slices_equivalent(&lhs.argument_order, &rhs.argument_order)
        && order_slices_equivalent(&lhs.within_group, &rhs.within_group)
        && optional_expr_equivalent(lhs.filter.as_deref(), rhs.filter.as_deref())
        && window_equivalent(lhs.window.as_ref(), rhs.window.as_ref())
        && custom_operation_equivalent(
            lhs.custom_type_operation.as_ref(),
            rhs.custom_type_operation.as_ref(),
        )
        && sequence_operation_equivalent(
            lhs.sequence_operation.as_ref(),
            rhs.sequence_operation.as_ref(),
        )
}

fn sequence_operation_equivalent(
    lhs: Option<&hir::SequenceOperation>,
    rhs: Option<&hir::SequenceOperation>,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => {
            lhs.kind == rhs.kind
                && lhs.database == rhs.database
                && lhs.user_name == rhs.user_name
                && lhs.normalized_name == rhs.normalized_name
                && catalog_equivalent(&lhs.backing_table, &rhs.backing_table)
                && match (&lhs.sqlite_sequence, &rhs.sqlite_sequence) {
                    (None, None) => true,
                    (Some(lhs), Some(rhs)) => catalog_equivalent(lhs, rhs),
                    _ => false,
                }
                && lhs.schema_cookie == rhs.schema_cookie
                && lhs.sequence.name == rhs.sequence.name
                && lhs.sequence.start_value == rhs.sequence.start_value
                && lhs.sequence.increment_by == rhs.sequence.increment_by
                && lhs.sequence.min_value == rhs.sequence.min_value
                && lhs.sequence.max_value == rhs.sequence.max_value
                && lhs.sequence.cycle == rhs.sequence.cycle
        }
        _ => false,
    }
}

fn custom_operation_equivalent(
    lhs: Option<&hir::CustomTypeOperation>,
    rhs: Option<&hir::CustomTypeOperation>,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (
            Some(hir::CustomTypeOperation::UnionValue {
                union_type: lhs_type,
                tag_index: lhs_tag,
                ..
            }),
            Some(hir::CustomTypeOperation::UnionValue {
                union_type: rhs_type,
                tag_index: rhs_tag,
                ..
            }),
        )
        | (
            Some(hir::CustomTypeOperation::UnionExtract {
                union_type: lhs_type,
                tag_index: lhs_tag,
                ..
            }),
            Some(hir::CustomTypeOperation::UnionExtract {
                union_type: rhs_type,
                tag_index: rhs_tag,
                ..
            }),
        ) => lhs_tag == rhs_tag && catalog_equivalent(lhs_type, rhs_type),
        (
            Some(hir::CustomTypeOperation::UnionTag {
                union_type: lhs_type,
                tag_names: lhs_names,
            }),
            Some(hir::CustomTypeOperation::UnionTag {
                union_type: rhs_type,
                tag_names: rhs_names,
            }),
        ) => lhs_names == rhs_names && catalog_equivalent(lhs_type, rhs_type),
        (
            Some(hir::CustomTypeOperation::StructExtract {
                struct_type: lhs_type,
                field_index: lhs_field,
                ..
            }),
            Some(hir::CustomTypeOperation::StructExtract {
                struct_type: rhs_type,
                field_index: rhs_field,
                ..
            }),
        ) => lhs_field == rhs_field && catalog_equivalent(lhs_type, rhs_type),
        _ => false,
    }
}

fn subquery_equivalent(lhs: &hir::SubqueryExpr, rhs: &hir::SubqueryExpr) -> bool {
    match (lhs, rhs) {
        (
            hir::SubqueryExpr::Scalar {
                query: lhs_query,
                output: lhs_output,
            },
            hir::SubqueryExpr::Scalar {
                query: rhs_query,
                output: rhs_output,
            },
        ) => lhs_query == rhs_query && lhs_output == rhs_output,
        (hir::SubqueryExpr::Exists(lhs), hir::SubqueryExpr::Exists(rhs)) => lhs == rhs,
        (
            hir::SubqueryExpr::In {
                lhs,
                query: lhs_query,
                negated: lhs_negated,
                comparison: lhs_comparison,
            },
            hir::SubqueryExpr::In {
                lhs: rhs,
                query: rhs_query,
                negated: rhs_negated,
                comparison: rhs_comparison,
            },
        ) => {
            lhs_query == rhs_query
                && lhs_negated == rhs_negated
                && lhs_comparison == rhs_comparison
                && equivalent(lhs, rhs)
        }
        _ => false,
    }
}

fn type_name_equivalent(lhs: &hir::TypeName, rhs: &hir::TypeName) -> bool {
    lhs.name.eq_ignore_ascii_case(&rhs.name)
        && lhs.array_dimensions == rhs.array_dimensions
        && lhs.affinity == rhs.affinity
        && expr_slices_equivalent(&lhs.parameters, &rhs.parameters)
        && type_fact_equivalent(&lhs.type_fact, &rhs.type_fact)
}

fn type_fact_equivalent(lhs: &TypeFact, rhs: &TypeFact) -> bool {
    lhs.storage == rhs.storage
        && lhs.array_dimensions == rhs.array_dimensions
        && lhs.array_rank_unbounded == rhs.array_rank_unbounded
        && match (&lhs.declared, &rhs.declared) {
            (None, None) => true,
            (Some(lhs), Some(rhs)) => {
                lhs.name.eq_ignore_ascii_case(&rhs.name)
                    && lhs.array_dimensions == rhs.array_dimensions
                    && lhs.custom_chain == rhs.custom_chain
            }
            _ => false,
        }
}

fn window_equivalent(lhs: Option<&hir::WindowSpec>, rhs: Option<&hir::WindowSpec>) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => {
            expr_slices_equivalent(&lhs.partition_by, &rhs.partition_by)
                && order_slices_equivalent(&lhs.order_by, &rhs.order_by)
                && window_frame_equivalent(lhs.frame.as_ref(), rhs.frame.as_ref())
        }
        _ => false,
    }
}

fn window_frame_equivalent(lhs: Option<&hir::WindowFrame>, rhs: Option<&hir::WindowFrame>) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => {
            lhs.mode == rhs.mode
                && window_bound_equivalent(&lhs.start, &rhs.start)
                && match (&lhs.end, &rhs.end) {
                    (None, None) => true,
                    (Some(lhs), Some(rhs)) => window_bound_equivalent(lhs, rhs),
                    _ => false,
                }
                && lhs.exclude == rhs.exclude
        }
        _ => false,
    }
}

fn window_bound_equivalent(lhs: &hir::WindowFrameBound, rhs: &hir::WindowFrameBound) -> bool {
    match (lhs, rhs) {
        (hir::WindowFrameBound::CurrentRow, hir::WindowFrameBound::CurrentRow)
        | (hir::WindowFrameBound::UnboundedFollowing, hir::WindowFrameBound::UnboundedFollowing)
        | (hir::WindowFrameBound::UnboundedPreceding, hir::WindowFrameBound::UnboundedPreceding) => {
            true
        }
        (hir::WindowFrameBound::Following(lhs), hir::WindowFrameBound::Following(rhs))
        | (hir::WindowFrameBound::Preceding(lhs), hir::WindowFrameBound::Preceding(rhs)) => {
            equivalent(lhs, rhs)
        }
        _ => false,
    }
}

fn order_slices_equivalent(lhs: &[hir::OrderTerm], rhs: &[hir::OrderTerm]) -> bool {
    lhs.len() == rhs.len()
        && lhs.iter().zip(rhs).all(|(lhs, rhs)| {
            lhs.order == rhs.order
                && lhs.nulls == rhs.nulls
                && lhs.type_fact == rhs.type_fact
                && lhs.collation == rhs.collation
                && equivalent(&lhs.expr, &rhs.expr)
        })
}

fn expr_slices_equivalent(lhs: &[hir::Expr], rhs: &[hir::Expr]) -> bool {
    lhs.len() == rhs.len() && lhs.iter().zip(rhs).all(|(lhs, rhs)| equivalent(lhs, rhs))
}

fn expr_pair_slices_equivalent(
    lhs: &[(hir::Expr, hir::Expr)],
    rhs: &[(hir::Expr, hir::Expr)],
) -> bool {
    lhs.len() == rhs.len()
        && lhs
            .iter()
            .zip(rhs)
            .all(|((lhs_when, lhs_then), (rhs_when, rhs_then))| {
                equivalent(lhs_when, rhs_when) && equivalent(lhs_then, rhs_then)
            })
}

fn optional_expr_equivalent(lhs: Option<&hir::Expr>, rhs: Option<&hir::Expr>) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => equivalent(lhs, rhs),
        _ => false,
    }
}

fn optional_schema_call_equivalent(
    lhs: Option<&hir::BoundSchemaCall>,
    rhs: Option<&hir::BoundSchemaCall>,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => {
            lhs.program == rhs.program && expr_slices_equivalent(&lhs.arguments, &rhs.arguments)
        }
        _ => false,
    }
}

fn custom_binary_operator_equivalent(
    lhs: Option<&hir::CustomBinaryOperator>,
    rhs: Option<&hir::CustomBinaryOperator>,
    operands_swapped: bool,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => {
            catalog_equivalent(&lhs.function, &rhs.function)
                && lhs.swap_args == rhs.swap_args
                && lhs.negate == rhs.negate
                && match (&lhs.literal_encoding, &rhs.literal_encoding) {
                    (None, None) => true,
                    (Some(lhs), Some(rhs)) => {
                        let rhs_operand = if operands_swapped {
                            match rhs.operand {
                                hir::BinaryOperand::Left => hir::BinaryOperand::Right,
                                hir::BinaryOperand::Right => hir::BinaryOperand::Left,
                            }
                        } else {
                            rhs.operand
                        };
                        lhs.operand == rhs_operand
                            && optional_schema_call_equivalent(
                                lhs.encoder.as_ref(),
                                rhs.encoder.as_ref(),
                            )
                    }
                    _ => false,
                }
        }
        _ => false,
    }
}

fn catalog_equivalent<T>(lhs: &CatalogObject<T>, rhs: &CatalogObject<T>) -> bool {
    lhs.id() == rhs.id() && lhs.snapshot() == rhs.snapshot() && lhs.database() == rhs.database()
}

fn optional_catalog_equivalent<T>(
    lhs: Option<&CatalogObject<T>>,
    rhs: Option<&CatalogObject<T>>,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => catalog_equivalent(lhs, rhs),
        _ => false,
    }
}
