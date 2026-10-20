//! Bytecode lowering from resolved semantic HIR.

mod autoincrement;
mod cdc;
mod delete;
mod dml;
mod emit;
pub(super) mod expr;
mod fkey;
mod fkey_action;
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

use dml::Subprograms;

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

/// A SELECT that a schema statement runs to read rows, planned through HIR:
/// the SELECT of `CREATE TABLE ... AS SELECT`, or the scan that fills a new
/// or rebuilt index.
pub(crate) struct PlannedSelect {
    plan: Arc<HirPlan>,
    query: hir::QueryId,
}

impl PlannedSelect {
    pub(crate) fn new(
        select: ast::Select,
        schema: &Arc<Schema>,
        settings: &HirSettings<'_>,
        connection: &Arc<Connection>,
    ) -> Result<Self> {
        let plan = analyze_and_plan(&ast::Stmt::Select(select), schema, settings, connection)?;
        let HirRoot::Query(root) = &plan.document.root else {
            return Err(LimboError::InternalError(
                "HIR CTAS analysis did not produce a query root".into(),
            ));
        };
        let query = root.query;
        Ok(Self { plan, query })
    }

    /// `SELECT <values>`, without FROM: how a schema statement evaluates
    /// expressions that read no table, such as the arguments of ATTACH.
    pub(crate) fn values(
        values: Vec<ast::Expr>,
        schema: &Arc<Schema>,
        settings: &HirSettings<'_>,
        connection: &Arc<Connection>,
    ) -> Result<Self> {
        let select = Self::select(values, None, None);
        Self::new(select, schema, settings, connection)
    }

    /// `SELECT <values> FROM <database>.<table> NOT INDEXED [WHERE <filter>]`:
    /// how a schema statement reads every row of a table. NOT INDEXED keeps
    /// it reading the table itself, never one of its indexes.
    pub(crate) fn scan(
        values: Vec<ast::Expr>,
        database: String,
        table: String,
        filter: Option<Box<ast::Expr>>,
        schema: &Arc<Schema>,
        settings: &HirSettings<'_>,
        connection: &Arc<Connection>,
    ) -> Result<Self> {
        let from = ast::FromClause {
            select: Box::new(ast::SelectTable::Table(
                ast::QualifiedName {
                    db_name: Some(ast::Name::exact(database)),
                    name: ast::Name::exact(table),
                    alias: None,
                },
                None,
                Some(ast::Indexed::NotIndexed),
            )),
            joins: vec![],
        };
        let select = Self::select(values, Some(from), filter);
        Self::new(select, schema, settings, connection)
    }

    fn select(
        values: Vec<ast::Expr>,
        from: Option<ast::FromClause>,
        where_clause: Option<Box<ast::Expr>>,
    ) -> ast::Select {
        ast::Select {
            with: None,
            body: ast::SelectBody {
                select: ast::OneSelect::Select {
                    distinctness: None,
                    columns: values
                        .into_iter()
                        .map(|value| ast::ResultColumn::Expr(Box::new(value), None))
                        .collect(),
                    from,
                    where_clause,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        }
    }

    /// Emit a loop over the SELECT's rows, which runs as a coroutine. For
    /// each row, `body` gets the first of the registers holding its columns.
    pub(crate) fn emit_each_row(
        &self,
        program: &mut ProgramBuilder,
        body: impl FnOnce(&mut ProgramBuilder, usize) -> Result<()>,
    ) -> Result<()> {
        use crate::vdbe::insn::Insn;
        let yield_reg = program.alloc_register();
        let after_definition = program.allocate_label();
        let start = program.allocate_label();
        program.emit_insn(Insn::InitCoroutine {
            yield_reg,
            jump_on_definition: after_definition,
            start_offset: start,
        });
        program.preassign_label_to_next_insn(start);
        let values = self.emit_rows(program, yield_reg, start)?;
        program.emit_insn(Insn::EndCoroutine { yield_reg });
        program.preassign_label_to_next_insn(after_definition);

        let loop_start = program.allocate_label();
        let loop_end = program.allocate_label();
        program.preassign_label_to_next_insn(loop_start);
        program.emit_insn(Insn::Yield {
            yield_reg,
            end_offset: loop_end,
            subtype_clear_start_reg: 0,
            subtype_clear_count: 0,
        });
        body(program, values)?;
        program.emit_insn(Insn::Goto {
            target_pc: loop_start,
        });
        program.preassign_label_to_next_insn(loop_end);
        Ok(())
    }

    /// Run the SELECT once and write its first row into the returned
    /// registers, one per column; they are NULL when it returns no row.
    pub(crate) fn emit_values(&self, program: &mut ProgramBuilder) -> Result<usize> {
        let width = self
            .plan
            .document
            .query(self.query)
            .map_or(0, |query| query.output.len());
        let values = program.alloc_registers(width.max(1));
        program.emit_insn(crate::vdbe::insn::Insn::Null {
            dest: values,
            dest_end: (width > 1).then(|| values + width - 1),
        });
        program.set_hir_plan(self.plan.clone());
        program.nested(|program| {
            query::emit_planned_query_body(
                program,
                &self.plan,
                self.query,
                &QueryDestination::RowValueSubqueryResult {
                    result_reg_start: values,
                    num_regs: width,
                },
            )
        })?;
        Ok(values)
    }

    /// Whether the SELECT reads a column whose stored value it would decode:
    /// a custom type or an array. An index stores the encoded value, so its
    /// refill cannot read such a column through a SELECT.
    pub(crate) fn decodes_stored_values(&self) -> bool {
        self.plan.document.sources.iter().any(|source| {
            source.column_type_programs.iter().any(Option::is_some)
                || source
                    .columns
                    .iter()
                    .any(|column| column.type_fact.is_array())
        })
    }

    /// Each column of the new table: the SELECT's output name, and the type
    /// SQLite derives from the output's affinity, as legacy does.
    pub(crate) fn columns(&self) -> Result<Vec<(String, &'static str)>> {
        let document = &self.plan.document;
        let query = document
            .query(self.query)
            .ok_or_else(|| LimboError::InternalError("HIR CTAS query is missing".into()))?;
        let names = resolved_result_columns(document, self.query)?;
        query
            .output
            .iter()
            .zip(names)
            .map(|(output, column)| {
                let output = document.output(*output).ok_or_else(|| {
                    LimboError::InternalError("HIR CTAS output is missing".into())
                })?;
                // Like legacy, a column read keeps its own name, without the
                // table that qualifies it, unless it has an alias.
                let name = match column.value {
                    ResolvedResultValue::Column(source) if !column.name_is_alias => {
                        source.column_name
                    }
                    ResolvedResultValue::RowId(name) if !column.name_is_alias => name,
                    _ => column.name,
                };
                Ok((name, output.schema_affinity.short_type_name()))
            })
            .collect()
    }

    /// Run the SELECT as a coroutine that yields each row into the returned
    /// registers, one per column.
    pub(crate) fn emit_rows(
        &self,
        program: &mut ProgramBuilder,
        yield_reg: usize,
        start: crate::vdbe::BranchOffset,
    ) -> Result<usize> {
        let width = self
            .plan
            .document
            .query(self.query)
            .map_or(0, |query| query.output.len());
        let values = program.alloc_registers(width.max(1));
        program.bind_coroutine_row(yield_reg, values);
        program.set_hir_plan(self.plan.clone());
        program.nested(|program| {
            query::emit_planned_query_body(
                program,
                &self.plan,
                self.query,
                &QueryDestination::CoroutineYield {
                    yield_reg,
                    coroutine_implementation_start: start,
                },
            )
        })?;
        Ok(values)
    }
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
    if delete::needs_legacy(&plan.document, delete, access)? {
        return Ok(false);
    }
    let Some(subprograms) = Subprograms::for_delete(&session, program, &plan, delete)? else {
        return Ok(false);
    };
    program.set_hir_plan(plan.clone());
    delete::emit_delete(program, &plan, delete, access, &subprograms)?;
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
    translate_update_with(statement, schema, settings, program, connection, None)
}

/// Prepare the UPDATE of the schema table that ALTER TABLE generates. Like
/// legacy, it may write the schema table, and change data capture records
/// `ddl`, the ALTER statement, as the new `sql` value. Returns `false`, with
/// nothing emitted, when the update needs legacy.
pub(crate) fn translate_schema_update(
    statement: &ast::Stmt,
    schema: &Arc<Schema>,
    settings: &HirSettings<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    ddl: &str,
) -> Result<bool> {
    translate_update_with(statement, schema, settings, program, connection, Some(ddl))
}

fn translate_update_with(
    statement: &ast::Stmt,
    schema: &Arc<Schema>,
    settings: &HirSettings<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    schema_change: Option<&str>,
) -> Result<bool> {
    let ast::Stmt::Update(syntax) = statement else {
        return Err(LimboError::InternalError(
            "HIR UPDATE got another statement".into(),
        ));
    };
    if schema_change.is_none() && target_needs_legacy(schema, connection, &syntax.tbl_name)? {
        return Ok(false);
    }
    let session = HirSession::new(schema, settings, connection)?;
    let plan = session.plan(SemanticRootInput::Statement(statement))?;
    let (update, access) = update::update_root(&plan)?;
    let mvcc = uses_mvcc(connection, &plan.document, update.target);
    if update::needs_legacy(&plan, update, access)? {
        return Ok(false);
    }
    let Some(subprograms) = Subprograms::for_update(&session, program, &plan, update)? else {
        return Ok(false);
    };
    program.set_hir_plan(plan.clone());
    update::emit_update(
        program,
        &plan,
        update,
        access,
        &subprograms,
        mvcc,
        !connection.check_constraints_ignored(),
        schema_change,
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
    let Some(subprograms) = Subprograms::for_insert(&session, program, &plan, insert)? else {
        return Ok(false);
    };
    program.set_hir_plan(plan.clone());
    insert::emit_insert(
        program,
        &plan,
        insert,
        &subprograms,
        uses_mvcc(connection, &plan.document, insert.target),
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
/// Whether the database of `target` keeps its rows in MVCC.
pub(super) fn uses_mvcc(
    connection: &Connection,
    document: &HirDocument,
    target: hir::SourceId,
) -> bool {
    document
        .source(target)
        .and_then(|source| source.database)
        .is_some_and(|database| connection.mv_store_for_db(database.index()).is_some())
}

fn target_needs_legacy(
    schema: &Schema,
    connection: &Connection,
    name: &ast::QualifiedName,
) -> Result<bool> {
    let table_name = crate::util::normalize_ident(name.name.as_str());
    // Like legacy, the statements the engine runs itself, such as VACUUM's,
    // may write its internal tables.
    if !connection.is_nested_stmt()
        && !connection.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(&table_name)
    {
        crate::bail_parse_error!("table {table_name} may not be modified");
    }
    if schema.is_materialized_view(&table_name) {
        crate::bail_parse_error!("cannot modify materialized view {table_name}");
    }
    if schema
        .get_btree_table(&table_name)
        .is_some_and(|table| !table.has_rowid)
        && !schema
            .get_dependent_materialized_views(&table_name)
            .is_empty()
    {
        crate::bail_parse_error!("materialized views on WITHOUT ROWID tables are not supported");
    }
    // The VM keeps dependent materialized views up to date as rows are
    // written; legacy reports views built by another DBSP version.
    Ok(schema.with_incompatible_dependent_views(&table_name, |views| !views.is_empty()))
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
    /// The foreign-key actions being compiled.
    actions: fkey_action::ActionStack,
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
            actions: fkey_action::ActionStack::default(),
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
                capture_changes_table: self
                    .connection
                    .get_capture_data_changes_info()
                    .as_ref()
                    .map(|info| info.table.clone()),
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
    /// The type name `Statement::get_column_type_info` reports for a
    /// computed value, inferred as legacy does.
    pub(crate) primitive: Option<&'static str>,
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
                primitive: expression_primitive(document, &output.expr),
            })
        })
        .collect()
}

/// The type name of a computed value, by legacy's rules: a literal has its
/// own type; arithmetic over two INTEGERs is INTEGER, over INTEGERs and REALs
/// REAL, and NUMERIC otherwise; comparisons, logic and bitwise operators give
/// INTEGER; `||` gives TEXT; anything else reports its affinity.
fn expression_primitive(document: &HirDocument, expression: &hir::Expr) -> Option<&'static str> {
    use ast::{Operator, UnaryOperator};
    let arithmetic = |lhs: Option<&'static str>, rhs: Option<&'static str>| match (lhs, rhs) {
        (Some("INTEGER"), Some("INTEGER")) => "INTEGER",
        (Some("INTEGER" | "REAL"), Some("INTEGER" | "REAL")) => "REAL",
        _ => "NUMERIC",
    };
    match expression {
        hir::Expr::Literal(literal) => {
            match crate::translate::alter::literal_default_value(literal)
                .ok()?
                .value_type()
            {
                crate::types::ValueType::Integer => Some("INTEGER"),
                crate::types::ValueType::Float => Some("REAL"),
                crate::types::ValueType::Text => Some("TEXT"),
                _ => None,
            }
        }
        hir::Expr::Collate { expr, .. } => expression_primitive(document, expr),
        hir::Expr::Unary { operator, expr } => match operator {
            UnaryOperator::Not | UnaryOperator::BitwiseNot => Some("INTEGER"),
            UnaryOperator::Negative => Some(arithmetic(
                Some("INTEGER"),
                expression_primitive(document, expr),
            )),
            UnaryOperator::Positive => expression_primitive(document, expr),
        },
        hir::Expr::Binary {
            lhs, operator, rhs, ..
        } => match operator {
            Operator::Add
            | Operator::Subtract
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulus => Some(arithmetic(
                expression_primitive(document, lhs),
                expression_primitive(document, rhs),
            )),
            Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseNot
            | Operator::LeftShift
            | Operator::RightShift
            | Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals
            | Operator::Is
            | Operator::IsNot
            | Operator::And
            | Operator::Or
            | Operator::ArrayContains
            | Operator::ArrayOverlap => Some("INTEGER"),
            Operator::Concat => Some("TEXT"),
            Operator::ArrowRight | Operator::ArrowRightShift => None,
        },
        hir::Expr::RowId(_) => Some("INTEGER"),
        _ => match expression_affinity(document, expression) {
            Affinity::Integer => Some("INTEGER"),
            Affinity::Real => Some("REAL"),
            Affinity::Text => Some("TEXT"),
            Affinity::Numeric => Some("NUMERIC"),
            Affinity::Blob | Affinity::None => None,
        },
    }
}

/// The affinity legacy gives an operand: a column's or CAST's own, and
/// none for other expressions.
fn expression_affinity(document: &HirDocument, expression: &hir::Expr) -> Affinity {
    match expression {
        hir::Expr::Column(reference) => document
            .source(reference.source)
            .and_then(|source| source.columns.get(reference.column))
            .map_or(Affinity::Blob, |column| column.affinity),
        hir::Expr::RowId(_) => Affinity::Integer,
        hir::Expr::Output(id) => document
            .output(*id)
            .map_or(Affinity::Blob, |output| output.affinity),
        hir::Expr::Cast { target, .. } => target.affinity,
        hir::Expr::Collate { expr, .. } => expression_affinity(document, expr),
        _ => Affinity::Blob,
    }
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
