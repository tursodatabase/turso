//! Bytecode lowering from resolved semantic HIR.

mod autoincrement;
mod delete;
mod dml;
mod emit;
pub(super) mod expr;
mod fkey;
mod insert;
pub(super) mod query;
mod trigger;
mod update;
mod window;

use turso_parser::ast;

use crate::{
    schema::{Schema, Type},
    sync::Arc,
    translate::{
        optimizer::cost_model_params,
        plan::QueryDestination,
        semantic::{
            analyze_root,
            catalog::SemanticCatalog,
            context::DoubleQuotedDml,
            hir::{self, HirDocument, HirRoot, OutputNameKind},
            SemanticOptions, SemanticRootInput,
        },
        semantic_to_plan::{HirPlan, HirRootPlan},
    },
    vdbe::{affinity::Affinity, builder::ProgramBuilder},
    Connection, LimboError, Result,
};

/// Prepare a top-level SELECT through semantic HIR: analyze, plan, and lower
/// it without the legacy binder, planner, or emitter.
pub(crate) fn translate_select(
    statement: &ast::Stmt,
    schema: &Arc<Schema>,
    settings: &HirSettings<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let plan = analyze_and_plan(statement, schema, settings, connection)?;
    let HirRoot::Query(root) = &plan.document.root else {
        return Err(LimboError::InternalError(
            "HIR SELECT analysis did not produce a query root".into(),
        ));
    };
    program.set_hir_plan(plan.clone());
    query::emit_planned_query_body(program, &plan, root.query, &QueryDestination::ResultRows)?;
    program.resolved_result_columns = resolved_result_columns(&plan.document, root.query)?;
    Ok(())
}

/// Prepare a top-level DELETE through semantic HIR. Returns `false`, with
/// nothing emitted, when the statement uses something only legacy lowers
/// today; the caller then hands it to legacy.
pub(crate) fn translate_delete(
    statement: &ast::Stmt,
    schema: &Arc<Schema>,
    settings: &HirSettings<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<bool> {
    let ast::Stmt::Delete { tbl_name, .. } = statement else {
        return Err(LimboError::InternalError(
            "HIR DELETE got another statement".into(),
        ));
    };
    if target_needs_legacy(schema, connection, tbl_name)? {
        return Ok(false);
    }
    let session = HirSession::new(schema, settings, connection)?;
    let plan = session.plan(SemanticRootInput::Statement(statement))?;
    let (HirRoot::Delete(delete), HirRootPlan::Delete(access)) = (&plan.document.root, &plan.root)
    else {
        return Err(LimboError::InternalError(
            "HIR DELETE analysis did not produce a DELETE root".into(),
        ));
    };
    if delete::needs_legacy(&plan.document, delete, access, program)? {
        return Ok(false);
    }
    // Triggers are compiled before anything is emitted, so one that needs
    // legacy still leaves the whole statement to legacy.
    let Some(triggers) = trigger::delete_triggers(&session, program, &plan, delete)? else {
        return Ok(false);
    };
    program.set_hir_plan(plan.clone());
    delete::emit_delete(program, &plan, delete, access, &triggers)?;
    if let Some(returning) = &delete.returning {
        program.resolved_result_columns =
            resolved_output_columns(&plan.document, &returning.outputs)?;
    }
    Ok(true)
}

/// Prepare a top-level UPDATE through semantic HIR. Returns `false`, with
/// nothing emitted, when the statement uses something only legacy lowers
/// today; the caller then hands it to legacy.
pub(crate) fn translate_update(
    statement: &ast::Stmt,
    schema: &Arc<Schema>,
    settings: &HirSettings<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<bool> {
    let ast::Stmt::Update(syntax) = statement else {
        return Err(LimboError::InternalError(
            "HIR UPDATE got another statement".into(),
        ));
    };
    if target_needs_legacy(schema, connection, &syntax.tbl_name)? {
        return Ok(false);
    }
    let session = HirSession::new(schema, settings, connection)?;
    let plan = session.plan(SemanticRootInput::Statement(statement))?;
    let (update, access) = update::update_root(&plan)?;
    if update::needs_legacy(&plan, update, access, program)? {
        return Ok(false);
    }
    let Some(triggers) = trigger::update_triggers(&session, program, &plan, update)? else {
        return Ok(false);
    };
    let mvcc = plan
        .document
        .source(update.target)
        .and_then(|source| source.database)
        .is_some_and(|database| connection.mv_store_for_db(database.index()).is_some());
    program.set_hir_plan(plan.clone());
    update::emit_update(
        program,
        &plan,
        update,
        access,
        &triggers,
        mvcc,
        !connection.check_constraints_ignored(),
    )?;
    if let Some(returning) = &update.returning {
        program.resolved_result_columns =
            resolved_output_columns(&plan.document, &returning.outputs)?;
    }
    Ok(true)
}

/// Prepare a top-level INSERT through semantic HIR. Returns `false`, with
/// nothing emitted, when the statement uses something only legacy lowers
/// today; the caller then hands it to legacy.
pub(crate) fn translate_insert(
    statement: &ast::Stmt,
    schema: &Arc<Schema>,
    settings: &HirSettings<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<bool> {
    let ast::Stmt::Insert { tbl_name, .. } = statement else {
        return Err(LimboError::InternalError(
            "HIR INSERT got another statement".into(),
        ));
    };
    if target_needs_legacy(schema, connection, tbl_name)? {
        return Ok(false);
    }
    let session = HirSession::new(schema, settings, connection)?;
    let plan = session.plan(SemanticRootInput::Statement(statement))?;
    let insert = insert::insert_root(&plan)?;
    if insert::needs_legacy(&plan, insert, program)? {
        return Ok(false);
    }
    let Some(triggers) = trigger::insert_triggers(&session, program, &plan, insert)? else {
        return Ok(false);
    };
    program.set_hir_plan(plan.clone());
    insert::emit_insert(
        program,
        &plan,
        insert,
        &triggers,
        !connection.check_constraints_ignored(),
    )?;
    if let Some(returning) = &insert.returning {
        program.resolved_result_columns =
            resolved_output_columns(&plan.document, &returning.outputs)?;
    }
    Ok(true)
}

/// Checks of a DML target that legacy makes by name, in the main schema,
/// before it looks the table up. A protected system table or a materialized
/// view cannot be written. A table that materialized views read still needs
/// legacy, which keeps the views up to date.
fn target_needs_legacy(
    schema: &Schema,
    connection: &Connection,
    name: &ast::QualifiedName,
) -> Result<bool> {
    let table_name = crate::util::normalize_ident(name.name.as_str());
    if !connection.is_mvcc_bootstrap_connection() && !crate::schema::allow_user_dml(&table_name) {
        crate::bail_parse_error!("table {table_name} may not be modified");
    }
    if schema.is_materialized_view(&table_name) {
        crate::bail_parse_error!("cannot modify materialized view {table_name}");
    }
    Ok(!schema
        .get_dependent_materialized_views(&table_name)
        .is_empty())
}

/// What every HIR document of one statement is analyzed and planned with:
/// the statement itself and each trigger it fires.
pub(super) struct HirSession<'a> {
    pub(super) schema: &'a Arc<Schema>,
    pub(super) connection: &'a Arc<Connection>,
    catalog: SemanticCatalog,
    symbols: &'a crate::SymbolTable,
    dialect: Arc<dyn crate::dialect::Dialect>,
    custom_types_enabled: bool,
    dqs_dml: DoubleQuotedDml,
}

/// The settings a statement is analyzed with, taken from the connection
/// and the prepare call rather than from legacy's resolver.
pub(crate) struct HirSettings<'a> {
    symbols: &'a crate::SymbolTable,
    dialect: Arc<dyn crate::dialect::Dialect>,
    custom_types_enabled: bool,
    dqs_dml: DoubleQuotedDml,
    /// The databases an unqualified name is looked up in, when the prepare
    /// call sets them.
    search_path: Option<&'a [String]>,
}

impl<'a> HirSettings<'a> {
    pub(crate) fn new(
        connection: &Connection,
        symbols: &'a crate::SymbolTable,
        dialect: Arc<dyn crate::dialect::Dialect>,
        search_path: Option<&'a [String]>,
    ) -> Self {
        Self {
            symbols,
            dialect,
            custom_types_enabled: connection.experimental_custom_types_enabled(),
            dqs_dml: if connection.get_dqs_dml() {
                DoubleQuotedDml::Enabled
            } else {
                DoubleQuotedDml::Disabled
            },
            search_path,
        }
    }
}

impl<'a> HirSession<'a> {
    fn new(
        schema: &'a Arc<Schema>,
        settings: &HirSettings<'a>,
        connection: &'a Arc<Connection>,
    ) -> Result<Self> {
        let catalog = SemanticCatalog::capture(
            schema.clone(),
            connection.database_schemas(),
            &connection.temp.database,
            connection.attached_databases(),
            settings.custom_types_enabled,
            settings.dialect.as_ref(),
            settings.search_path,
        )?;
        Ok(Self {
            schema,
            connection,
            catalog,
            symbols: settings.symbols,
            dialect: settings.dialect.clone(),
            custom_types_enabled: settings.custom_types_enabled,
            dqs_dml: settings.dqs_dml,
        })
    }

    /// Analyze `input` into HIR and choose its access paths.
    pub(super) fn plan(&self, input: SemanticRootInput<'_>) -> Result<Arc<HirPlan>> {
        let document = analyze_root(
            &self.catalog,
            self.symbols,
            SemanticOptions {
                dialect: self.dialect.clone(),
                custom_types_enabled: self.custom_types_enabled,
                dqs_dml: self.dqs_dml,
                foreign_keys_enabled: self.connection.foreign_keys_enabled(),
            },
            input,
        )?;
        Ok(Arc::new(HirPlan::build(
            Arc::new(document),
            self.schema,
            cost_model_params(),
        )?))
    }
}

/// Analyze one statement into HIR and choose its access paths.
fn analyze_and_plan(
    statement: &ast::Stmt,
    schema: &Arc<Schema>,
    settings: &HirSettings<'_>,
    connection: &Arc<Connection>,
) -> Result<Arc<HirPlan>> {
    HirSession::new(schema, settings, connection)?.plan(SemanticRootInput::Statement(statement))
}

/// Result-column metadata for a statement prepared from resolved HIR. It
/// answers the same column-name and type questions as the legacy plan's
/// result columns, without parser expressions or table references.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedResultColumn {
    /// The output name: an AS alias, a `*` expansion name, or the SQL text.
    pub(crate) name: String,
    /// AS aliases and `*` expansion names win over column naming rules.
    pub(crate) name_is_alias: bool,
    pub(crate) value: ResolvedResultValue,
    pub(crate) affinity: Affinity,
}

#[derive(Debug, Clone)]
pub(crate) enum ResolvedResultValue {
    Expression,
    /// The output reads one source column directly.
    Column(ResolvedResultSourceColumn),
    /// The output reads a source rowid; this is the name legacy reports.
    RowId(String),
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedResultSourceColumn {
    pub(crate) table_name: String,
    pub(crate) column_name: String,
    /// Declared type text, empty when the column has none.
    pub(crate) declared_type: String,
    pub(crate) ty: Type,
    pub(crate) array_dimensions: u32,
    pub(crate) table_is_strict: bool,
}

fn resolved_result_columns(
    document: &HirDocument,
    query: hir::QueryId,
) -> Result<Vec<ResolvedResultColumn>> {
    let query = document.query(query).ok_or_else(|| {
        LimboError::InternalError(format!("HIR result references missing query {query}"))
    })?;
    let block = query.blocks.first().ok_or_else(|| {
        LimboError::InternalError(format!("HIR query {} has no blocks", query.id))
    })?;
    resolved_output_columns(document, &block.outputs)
}

/// Result-column metadata for a list of outputs, such as a SELECT block's
/// or a RETURNING clause's.
fn resolved_output_columns(
    document: &HirDocument,
    outputs: &[hir::Output],
) -> Result<Vec<ResolvedResultColumn>> {
    outputs
        .iter()
        .map(|output| {
            Ok(ResolvedResultColumn {
                name: output.name.clone(),
                name_is_alias: matches!(
                    output.name_kind,
                    OutputNameKind::ExplicitAlias | OutputNameKind::StarExpansion
                ),
                value: resolved_result_value(document, &output.expr)?,
                affinity: output.affinity,
            })
        })
        .collect()
}

fn resolved_result_value(
    document: &HirDocument,
    expression: &hir::Expr,
) -> Result<ResolvedResultValue> {
    let source_of = |source: hir::SourceId| {
        document.source(source).ok_or_else(|| {
            LimboError::InternalError(format!("HIR result references missing source {source}"))
        })
    };
    match expression {
        hir::Expr::Column(reference) => {
            let source = source_of(reference.source)?;
            if let Some(table) = catalog_table(source) {
                let column = table.get_column_at(reference.column).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "HIR result references missing column {reference:?}"
                    ))
                })?;
                return Ok(ResolvedResultValue::Column(ResolvedResultSourceColumn {
                    table_name: table.get_name().to_string(),
                    column_name: column.name.clone().unwrap_or_default(),
                    declared_type: column.ty_str.clone(),
                    ty: column.ty(),
                    array_dimensions: column.array_dimensions(),
                    table_is_strict: table.is_strict(),
                }));
            }
            let column = source.columns.get(reference.column).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR result references missing column {reference:?}"
                ))
            })?;
            let declared = column.type_fact.declared.as_ref();
            // Like legacy, a subquery column that reads a rowid is declared
            // INTEGER.
            let reads_rowid = declared.is_none()
                && matches!(
                    &source.kind,
                    hir::SourceKind::Derived(query)
                        if document
                            .query(*query)
                            .and_then(|query| query.blocks.first())
                            .and_then(|block| block.outputs.get(reference.column))
                            .is_some_and(|output| matches!(output.expr, hir::Expr::RowId(_)))
                );
            let (declared_type, ty) = if reads_rowid {
                ("INTEGER".to_string(), Type::Integer)
            } else {
                (
                    declared
                        .map(|declared| declared.name.clone())
                        .unwrap_or_default(),
                    declared
                        .map(|declared| declared.storage)
                        .or(column.type_fact.storage)
                        .unwrap_or(Type::Null),
                )
            };
            Ok(ResolvedResultValue::Column(ResolvedResultSourceColumn {
                // Legacy reports a FROM subquery by its alias and a CTE by
                // its name.
                table_name: match (&source.kind, &source.alias) {
                    (hir::SourceKind::Derived(_), Some(alias)) => alias.clone(),
                    _ => source.name.clone(),
                },
                column_name: column.name.clone(),
                declared_type,
                ty,
                array_dimensions: declared.map_or(0, |declared| declared.array_dimensions),
                table_is_strict: false,
            }))
        }
        hir::Expr::RowId(source) => {
            let alias = catalog_table(source_of(*source)?)
                .and_then(|table| table.btree())
                .and_then(|table| {
                    table
                        .get_rowid_alias_column()
                        .and_then(|(_, column)| column.name.clone())
                });
            Ok(ResolvedResultValue::RowId(
                alias.unwrap_or_else(|| "rowid".to_string()),
            ))
        }
        _ => Ok(ResolvedResultValue::Expression),
    }
}

fn catalog_table(source: &hir::Source) -> Option<&crate::schema::Table> {
    match &source.kind {
        hir::SourceKind::Table(table)
        | hir::SourceKind::TableFunction { table, .. }
        | hir::SourceKind::Pseudo { table, .. } => Some(table.value()),
        hir::SourceKind::SchemaExpression
        | hir::SourceKind::Cte(_)
        | hir::SourceKind::Derived(_)
        | hir::SourceKind::FromGroup(_)
        | hir::SourceKind::RecursiveInput(_) => None,
    }
}
