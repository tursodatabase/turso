//! Bytecode lowering from resolved semantic HIR.

mod autoincrement;
mod cdc;
mod delete;
mod dml;
mod emit;
pub(crate) use emit::format_unique_violation_desc;
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
    function::{Func, ScalarFunc},
    schema::{Schema, Type},
    sync::Arc,
    translate::{
        optimizer::cost_model_params,
        plan::QueryDestination,
        semantic::{
            analyze_root, compound_column_affinity,
            context::SemanticContext,
            hir::{self, HirDocument, HirRoot, OutputNameKind},
            SemanticRootInput,
        },
        semantic_to_plan::{HirPlan, HirRootPlan},
    },
    vdbe::{
        affinity::Affinity,
        builder::{ProgramBuilder, SourceBinding},
        CursorID,
    },
    Connection, LimboError, Result,
};

use dml::Subprograms;

/// Prepare a top-level SELECT through semantic HIR: analyze, plan, and lower
/// it.
pub(crate) fn translate_select(
    statement: &ast::Stmt,
    context: &SemanticContext<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let plan = analyze_and_plan(statement, context, connection)?;
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
    /// Outputs computed with custom-typed columns read as stored.
    stored_outputs: crate::alloc::Vec<usize>,
}

/// One value a schema statement reads from each row of a table.
pub(crate) enum ScanValue {
    /// An expression over the row, with custom-typed columns decoded, as a
    /// query reads them.
    Expr(ast::Expr),
    /// An expression over the row with custom-typed columns read as stored:
    /// an index key column, which DML writes as stored, or a CHECK
    /// constraint, which DML checks over stored values.
    Stored(ast::Expr),
    /// The row's rowid, whatever the table's columns are named.
    RowId,
}

impl PlannedSelect {
    pub(crate) fn new(
        select: ast::Select,
        context: &SemanticContext<'_>,
        connection: &Arc<Connection>,
    ) -> Result<Self> {
        let plan = analyze_and_plan(&ast::Stmt::Select(select), context, connection)?;
        let HirRoot::Query(root) = &plan.document.root else {
            return Err(LimboError::InternalError(
                "HIR CTAS analysis did not produce a query root".into(),
            ));
        };
        let query = root.query;
        Ok(Self {
            plan,
            query,
            stored_outputs: crate::alloc::vec![],
        })
    }

    /// `SELECT <values>`, without FROM: how a schema statement evaluates
    /// expressions that read no table, such as the arguments of ATTACH.
    pub(crate) fn values(
        values: Vec<ast::Expr>,
        context: &SemanticContext<'_>,
        connection: &Arc<Connection>,
    ) -> Result<Self> {
        let select = Self::select(values, None, None);
        Self::new(select, context, connection)
    }

    /// `SELECT <values> FROM <database>.<table> NOT INDEXED [WHERE <filter>]`:
    /// how a schema statement reads every row of a table. NOT INDEXED keeps
    /// it reading the table itself, never one of its indexes.
    pub(crate) fn scan(
        values: Vec<ScanValue>,
        database: String,
        table: String,
        filter: Option<Box<ast::Expr>>,
        context: &SemanticContext<'_>,
        connection: &Arc<Connection>,
    ) -> Result<Self> {
        let mut stored_outputs = crate::alloc::vec![];
        let mut rowid_outputs = Vec::new();
        let columns = values
            .into_iter()
            .enumerate()
            .map(|(position, value)| match value {
                ScanValue::Expr(expr) => expr,
                ScanValue::Stored(expr) => {
                    stored_outputs.push(position);
                    expr
                }
                // A rowid has no name in SQL when the table's columns take
                // every rowid name; the output is rewritten after analysis.
                ScanValue::RowId => {
                    rowid_outputs.push(position);
                    ast::Expr::Literal(ast::Literal::Null)
                }
            })
            .collect();
        let from = ast::FromClause {
            select: Box::new(ast::SelectTable::Table(
                ast::QualifiedName {
                    namespace: Some(ast::Name::exact(database)),
                    name: ast::Name::exact(table),
                    alias: None,
                },
                None,
                Some(ast::Indexed::NotIndexed),
            )),
            joins: vec![],
        };
        let select = Self::select(columns, Some(from), filter);
        let plan = HirSession::new(context, connection).plan_with(
            SemanticRootInput::Statement(&ast::Stmt::Select(select)),
            |document| rewrite_rowid_outputs(document, &rowid_outputs),
        )?;
        let HirRoot::Query(root) = &plan.document.root else {
            return Err(LimboError::InternalError(
                "HIR scan analysis did not produce a query root".into(),
            ));
        };
        let query = root.query;
        Ok(Self {
            plan,
            query,
            stored_outputs,
        })
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
        let values = self.emit_rows(program, yield_reg)?;
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

    /// Each column of the new table: the SELECT's output name, and the type
    /// SQLite derives from the output's affinity.
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
            .enumerate()
            .map(|(position, (output, column))| {
                let output = document.output(*output).ok_or_else(|| {
                    LimboError::InternalError("HIR CTAS output is missing".into())
                })?;
                // A column read keeps its own name, without the table that
                // qualifies it, unless it has an alias.
                let name = match column.value {
                    ResolvedResultValue::Column(source) if !column.name_is_alias => {
                        source.column_name().to_string()
                    }
                    ResolvedResultValue::RowId { name, .. } if !column.name_is_alias => name,
                    _ => column.name,
                };
                // Like SQLite, a compound SELECT's column takes the affinity
                // its arms agree on; arms of different classes leave it
                // without a declared type.
                let affinity = if query.blocks.len() > 1 {
                    compound_column_affinity(query, position, &|source| document.source(source)).0
                } else {
                    output.schema_affinity
                };
                Ok((name, affinity.short_type_name()))
            })
            .collect()
    }

    /// Run the SELECT as a coroutine that yields each row into the returned
    /// registers, one per column.
    pub(crate) fn emit_rows(
        &self,
        program: &mut ProgramBuilder,
        yield_reg: usize,
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
                    stored_outputs: self.stored_outputs.clone(),
                    decode_arrays: true,
                },
            )
        })?;
        Ok(values)
    }

    /// Compute the SELECT's values for the row `cursor` is on, into the
    /// registers `start + column`, one per value. The SELECT reads one
    /// table, which `cursor` is open on, and has no WHERE clause.
    pub(crate) fn emit_values_from_cursor(
        &self,
        program: &mut ProgramBuilder,
        cursor: CursorID,
        start: usize,
    ) -> Result<()> {
        let source = self.table_source()?;
        let document = &self.plan.document;
        let query = document
            .query(self.query)
            .ok_or_else(|| LimboError::InternalError("HIR scan query is missing".into()))?;
        program.bind_source(
            source,
            SourceBinding::BTree {
                scan_cursor: cursor,
                table_cursor: None,
            },
        );
        program.set_hir_plan(self.plan.clone());
        for (position, output) in query.output.iter().enumerate() {
            let output = document
                .output(*output)
                .ok_or_else(|| LimboError::InternalError("HIR scan output is missing".into()))?;
            let target = start + position;
            if self.stored_outputs.contains(&position) {
                dml::with_stored_values(program, |program| {
                    expr::translate_expr_no_constant_opt(program, document, &output.expr, target)
                })?;
            } else {
                expr::translate_expr_no_constant_opt(program, document, &output.expr, target)?;
            }
        }
        Ok(())
    }

    /// STRICT: check the values at `start + column` of the columns `only`
    /// of the table the SELECT reads against the types they accept, encode
    /// them with their custom types, then check the encoded values against
    /// the columns' base types, as INSERT does with new values.
    pub(crate) fn emit_custom_type_writes(
        &self,
        program: &mut ProgramBuilder,
        table: &Arc<crate::schema::BTreeTable>,
        start: usize,
        only: &[usize],
    ) -> Result<()> {
        let document = &self.plan.document;
        let source = document
            .source(self.table_source()?)
            .ok_or_else(|| LimboError::InternalError("HIR scan source is missing".into()))?;
        dml::emit_custom_type_writes(program, document, source, table, start, Some(only))
    }

    /// The one table the SELECT reads.
    fn table_source(&self) -> Result<hir::SourceId> {
        let document = &self.plan.document;
        let query = document
            .query(self.query)
            .ok_or_else(|| LimboError::InternalError("HIR scan query is missing".into()))?;
        let block = document
            .query_block(query.first)
            .ok_or_else(|| LimboError::InternalError("HIR scan block is missing".into()))?;
        match &block.from {
            Some(from) if from.joins.is_empty() && query.blocks.len() == 1 => Ok(from.first),
            _ => Err(LimboError::InternalError(
                "HIR scan does not read exactly one table".into(),
            )),
        }
    }
}

/// The DELETE that DROP TABLE runs first when foreign keys are on and other
/// tables reference the table, as SQLite does: it fires the ON DELETE
/// actions of those foreign keys and counts the RESTRICT and NO ACTION
/// violations, with the table's own triggers off. The caller checks the
/// count.
pub(crate) fn translate_drop_table_delete(
    database: &str,
    table: &str,
    context: &SemanticContext<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let statement = ast::Stmt::Delete {
        with: None,
        tbl_name: ast::QualifiedName {
            namespace: Some(ast::Name::exact(database.to_string())),
            name: ast::Name::exact(table.to_string()),
            alias: None,
        },
        indexed: None,
        where_clause: None,
        returning: Vec::new(),
    };
    let session = HirSession::new(context, connection);
    let plan = session.plan(SemanticRootInput::Statement(&statement))?;
    let (HirRoot::Delete(delete), HirRootPlan::Delete(access)) = (&plan.document.root, &plan.root)
    else {
        return Err(LimboError::InternalError(
            "HIR DELETE analysis did not produce a DELETE root".into(),
        ));
    };
    delete::check(&plan.document, delete, access)?;
    let subprograms = Subprograms {
        actions: fkey_action::delete_actions(&session, program, delete)?,
        ..Subprograms::default()
    };
    program.set_hir_plan(plan.clone());
    delete::emit_delete(program, &plan, delete, access, &subprograms)
}

/// Prepare a top-level DELETE through semantic HIR.
pub(crate) fn translate_delete(
    statement: &ast::Stmt,
    context: &SemanticContext<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let ast::Stmt::Delete { tbl_name, .. } = statement else {
        return Err(LimboError::InternalError(
            "HIR DELETE got another statement".into(),
        ));
    };
    check_target(
        context.main_schema(),
        connection,
        tbl_name,
        "DELETE from table",
    )?;
    let session = HirSession::new(context, connection);
    let plan = session.plan(SemanticRootInput::Statement(statement))?;
    let (HirRoot::Delete(delete), HirRootPlan::Delete(access)) = (&plan.document.root, &plan.root)
    else {
        return Err(LimboError::InternalError(
            "HIR DELETE analysis did not produce a DELETE root".into(),
        ));
    };
    if let Some(table) = dml::virtual_target(&plan.document, delete.target)? {
        program.set_dbpage_writable(dml::dbpage_write_allowed(&table, connection));
    }
    delete::check(&plan.document, delete, access)?;
    let subprograms = Subprograms::for_delete(&session, program, &plan, delete)?;
    program.set_hir_plan(plan.clone());
    delete::emit_delete(program, &plan, delete, access, &subprograms)?;
    if let Some(returning) = &delete.returning {
        program.resolved_result_columns =
            resolved_output_columns(&plan.document, &returning.outputs)?;
    }
    Ok(())
}

/// Prepare a top-level UPDATE through semantic HIR.
pub(crate) fn translate_update(
    statement: &ast::Stmt,
    context: &SemanticContext<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    translate_update_with(statement, context, program, connection, None)
}

/// Prepare the UPDATE of the schema table that ALTER TABLE generates. It
/// may write the schema table, and change data capture records `ddl`, the
/// ALTER statement, as the new `sql` value.
pub(crate) fn translate_schema_update(
    statement: &ast::Stmt,
    context: &SemanticContext<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    ddl: &str,
) -> Result<()> {
    translate_update_with(statement, context, program, connection, Some(ddl))
}

fn translate_update_with(
    statement: &ast::Stmt,
    context: &SemanticContext<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    schema_change: Option<&str>,
) -> Result<()> {
    let ast::Stmt::Update(syntax) = statement else {
        return Err(LimboError::InternalError(
            "HIR UPDATE got another statement".into(),
        ));
    };
    if schema_change.is_none() {
        check_target(
            context.main_schema(),
            connection,
            &syntax.tbl_name,
            "UPDATE table",
        )?;
    }
    let session = HirSession::new(context, connection);
    let plan = session.plan(SemanticRootInput::Statement(statement))?;
    let (update, access) = update::update_root(&plan)?;
    if let Some(table) = dml::virtual_target(&plan.document, update.target)? {
        program.set_dbpage_writable(dml::dbpage_write_allowed(&table, connection));
    }
    let mvcc = uses_mvcc(connection, &plan.document, update.target);
    update::check(&plan, update, access)?;
    let subprograms = Subprograms::for_update(&session, program, &plan, update)?;
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
    Ok(())
}

/// Prepare a top-level INSERT through semantic HIR.
pub(crate) fn translate_insert(
    statement: &ast::Stmt,
    context: &SemanticContext<'_>,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let ast::Stmt::Insert { tbl_name, .. } = statement else {
        return Err(LimboError::InternalError(
            "HIR INSERT got another statement".into(),
        ));
    };
    check_target(
        context.main_schema(),
        connection,
        tbl_name,
        "INSERT into table",
    )?;
    let session = HirSession::new(context, connection);
    let plan = session.plan(SemanticRootInput::Statement(statement))?;
    let insert = insert::insert_root(&plan)?;
    if let Some(table) = dml::virtual_target(&plan.document, insert.target)? {
        program.set_dbpage_writable(dml::dbpage_write_allowed(&table, connection));
    }
    insert::check(&plan, insert, program)?;
    let subprograms = Subprograms::for_insert(&session, program, &plan, insert)?;
    program.set_hir_plan(plan.clone());
    insert::emit_insert(
        program,
        &plan,
        insert,
        &subprograms,
        uses_mvcc(connection, &plan.document, insert.target),
        !connection.check_constraints_ignored(),
        connection
            .dialect()
            .upsert_rejects_row_written_by_same_statement(),
    )?;
    if let Some(returning) = &insert.returning {
        program.resolved_result_columns =
            resolved_output_columns(&plan.document, &returning.outputs)?;
    }
    Ok(())
}

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

fn check_target(
    schema: &Schema,
    connection: &Connection,
    name: &ast::QualifiedName,
    statement: &str,
) -> Result<()> {
    let table_name = crate::util::normalize_ident(name.name.as_str());
    // The statements the engine runs itself, such as VACUUM's, may write
    // its internal tables.
    if !connection.is_nested_stmt()
        && !connection.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(&table_name)
    {
        crate::bail_parse_error!("table {table_name} may not be modified");
    }
    // Materialized views live in the main database: a table of the same
    // name in an attached database is an ordinary table.
    let in_main = name
        .namespace
        .as_ref()
        .is_none_or(|db| crate::util::normalize_ident(db.as_str()) == "main");
    if in_main && schema.is_materialized_view(&table_name) {
        crate::bail_parse_error!("cannot modify materialized view {table_name}");
    }
    if in_main
        && schema
            .get_btree_table(&table_name)
            .is_some_and(|table| !table.has_rowid)
        && !schema
            .get_dependent_materialized_views(&table_name)
            .is_empty()
    {
        crate::bail_parse_error!("materialized views on WITHOUT ROWID tables are not supported");
    }
    // The VM keeps dependent materialized views up to date as rows are
    // written; views built by another DBSP version are an error.
    schema.with_incompatible_dependent_views(&table_name, |views| {
        if views.is_empty() {
            return Ok(());
        }
        use crate::incremental::view::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot {statement} '{table_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().map(|view| view.as_str()).collect::<Vec<_>>().join(", "),
        );
    })
}

/// What every HIR document of one statement is analyzed and planned with:
/// the statement itself and each trigger it fires.
pub(super) struct HirSession<'a> {
    pub(super) context: &'a SemanticContext<'a>,
    pub(super) connection: &'a Arc<Connection>,
    /// The foreign-key actions being compiled.
    actions: fkey_action::ActionStack,
}

impl<'a> HirSession<'a> {
    fn new(context: &'a SemanticContext<'a>, connection: &'a Arc<Connection>) -> Self {
        Self {
            context,
            connection,
            actions: fkey_action::ActionStack::default(),
        }
    }

    /// Analyze `input` into HIR and choose its access paths.
    pub(super) fn plan(&self, input: SemanticRootInput<'_>) -> Result<Arc<HirPlan>> {
        self.plan_with(input, |_| Ok(()))
    }

    /// Like [`Self::plan`], with `rewrite` run on the analyzed document
    /// before its access paths are chosen.
    fn plan_with(
        &self,
        input: SemanticRootInput<'_>,
        rewrite: impl FnOnce(&mut HirDocument) -> Result<()>,
    ) -> Result<Arc<HirPlan>> {
        let mut document = self.analyze(input)?;
        rewrite(&mut document)?;
        Ok(Arc::new(HirPlan::build_with_unnesting_mode(
            Arc::new(document),
            self.context.main_schema(),
            cost_model_params(),
            self.connection.subquery_unnesting_mode(),
        )?))
    }

    /// Analyze `input` into HIR without choosing access paths.
    fn analyze(&self, input: SemanticRootInput<'_>) -> Result<HirDocument> {
        analyze_root(self.context, input)
    }
}

/// The expression lowering, for materialized view maintenance programs,
/// whose operators lower the analyzed expressions of a view's query over
/// register-bound stream values.
pub(crate) use expr::{translate_condition_expr, translate_expr_no_constant_opt};

/// Make each output in `positions` of a scan's one query block read the
/// scanned table's rowid.
fn rewrite_rowid_outputs(document: &mut HirDocument, positions: &[usize]) -> Result<()> {
    if positions.is_empty() {
        return Ok(());
    }
    let HirRoot::Query(root) = &document.root else {
        return Err(LimboError::InternalError(
            "HIR scan analysis did not produce a query root".into(),
        ));
    };
    let query = root.query;
    let [block] = document.queries[query.index()].blocks.as_mut_slice() else {
        return Err(LimboError::InternalError(
            "HIR scan has more than one query block".into(),
        ));
    };
    let source = block
        .from
        .as_ref()
        .map(|from| from.first)
        .ok_or_else(|| LimboError::InternalError("HIR scan reads no table".into()))?;
    for &position in positions {
        let output = block.outputs.get_mut(position).ok_or_else(|| {
            LimboError::InternalError(format!("HIR scan has no output {position}"))
        })?;
        output.expr = hir::Expr::rowid(source);
        output.type_fact = hir::TypeFact::known(Type::Integer);
        output.affinity = Affinity::Integer;
        output.schema_affinity = Affinity::Integer;
        output.has_affinity = true;
    }
    document
        .validate()
        .map_err(|error| LimboError::InternalError(error.to_string()))
}

/// Analyze one statement into HIR and choose its access paths.
fn analyze_and_plan(
    statement: &ast::Stmt,
    context: &SemanticContext<'_>,
    connection: &Arc<Connection>,
) -> Result<Arc<HirPlan>> {
    HirSession::new(context, connection).plan(SemanticRootInput::Statement(statement))
}

/// Result-column metadata for a statement prepared from resolved HIR. It
/// answers the column-name and type questions `Statement` asks about its
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
    /// computed value, inferred by [`expression_primitive`].
    pub(crate) primitive: Option<&'static str>,
}

#[derive(Debug, Clone)]
pub(crate) enum ResolvedResultValue {
    Expression,
    /// The output reads one source column directly.
    Column(ResolvedResultSourceColumn),
    /// The output reads a source rowid, named after its INTEGER PRIMARY KEY
    /// alias when the table has one, else `rowid`.
    RowId {
        name: String,
        /// The alias column's declared type, like SQLite reports.
        declared_type: Option<String>,
    },
}

/// A catalog table column keeps its table and reads its names and type
/// from the schema when asked; a subquery or CTE column keeps them.
#[derive(Debug, Clone)]
pub(crate) enum ResolvedResultSourceColumn {
    Table {
        table: Arc<crate::schema::Table>,
        column: usize,
    },
    Query {
        table_name: String,
        column_name: String,
        /// Declared type text, empty when the column has none.
        declared_type: String,
        ty: Type,
        array_dimensions: u32,
    },
}

impl ResolvedResultSourceColumn {
    pub(crate) fn table_name(&self) -> &str {
        match self {
            Self::Table { table, .. } => table.get_name(),
            Self::Query { table_name, .. } => table_name,
        }
    }

    pub(crate) fn column_name(&self) -> &str {
        match self {
            Self::Table { table, column } => {
                table.columns()[*column].name.as_deref().unwrap_or_default()
            }
            Self::Query { column_name, .. } => column_name,
        }
    }

    pub(crate) fn declared_type(&self) -> &str {
        match self {
            Self::Table { table, column } => &table.columns()[*column].ty_str,
            Self::Query { declared_type, .. } => declared_type,
        }
    }

    pub(crate) fn ty(&self) -> Type {
        match self {
            Self::Table { table, column } => table.columns()[*column].ty(),
            Self::Query { ty, .. } => *ty,
        }
    }

    pub(crate) fn array_dimensions(&self) -> u32 {
        match self {
            Self::Table { table, column } => table.columns()[*column].array_dimensions(),
            Self::Query {
                array_dimensions, ..
            } => *array_dimensions,
        }
    }

    pub(crate) fn table_is_strict(&self) -> bool {
        match self {
            Self::Table { table, .. } => table.is_strict(),
            Self::Query { .. } => false,
        }
    }
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
    resolved_output_columns_from(document, &block.outputs, block.from.as_ref())
}

/// Result-column metadata for a list of outputs, such as a SELECT block's
/// or a RETURNING clause's.
fn resolved_output_columns(
    document: &HirDocument,
    outputs: &[hir::Output],
) -> Result<Vec<ResolvedResultColumn>> {
    resolved_output_columns_from(document, outputs, None)
}

fn resolved_output_columns_from(
    document: &HirDocument,
    outputs: &[hir::Output],
    from: Option<&hir::From>,
) -> Result<Vec<ResolvedResultColumn>> {
    outputs
        .iter()
        .map(|output| {
            let value = match group_column_of(document, from, &output.expr) {
                Some(column) => resolved_result_value(document, &column)?,
                None => resolved_result_value(document, &output.expr)?,
            };
            Ok(ResolvedResultColumn {
                name: output.name.clone(),
                name_is_alias: matches!(
                    output.name_kind,
                    OutputNameKind::ExplicitAlias | OutputNameKind::StarExpansion
                ),
                value,
                affinity: output.affinity,
                primitive: expression_primitive(document, &output.expr),
            })
        })
        .collect()
}

/// SQLite reads a parenthesized join as a subquery, and names a column
/// read through it after that subquery: `(join-1).id:1`, or the group's
/// alias. The one source of a FROM clause that is a group is read without
/// the subquery. Returns the group column the expression reads, through
/// every group it is inside.
fn group_column_of(
    document: &HirDocument,
    from: Option<&hir::From>,
    expression: &hir::Expr,
) -> Option<hir::Expr> {
    let bare = |group: &hir::Source| {
        from.is_some_and(|from| from.first == group.id && from.joins.is_empty())
    };
    let mut found = None;
    let mut expression = match expression {
        hir::Expr::Column(_) | hir::Expr::MergedColumn(_) => expression.clone(),
        _ => return None,
    };
    loop {
        let outer = document.sources.iter().find_map(|source| {
            let hir::SourceKind::FromGroup(group) = &source.kind else {
                return None;
            };
            if bare(source) {
                return None;
            }
            let column = group
                .columns
                .iter()
                .position(|value| value.equivalent(&expression))?;
            Some(hir::Expr::Column(hir::ColumnRef {
                source: source.id,
                column,
            }))
        });
        match outer {
            Some(outer) => {
                expression = outer.clone();
                found = Some(outer);
            }
            None => return found,
        }
    }
}

/// The type name of a computed value: a literal has its
/// own type; arithmetic over two INTEGERs is INTEGER, over INTEGERs and REALs
/// REAL, and NUMERIC otherwise; comparisons, logic and bitwise operators give
/// INTEGER; `||` gives TEXT; anything else reports its affinity.
fn expression_primitive(document: &HirDocument, expression: &hir::Expr) -> Option<&'static str> {
    use ast::{Operator, UnaryOperator};
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
            UnaryOperator::Negative => Some(arithmetic_primitive(
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
            | Operator::Modulus => Some(arithmetic_primitive(
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
        hir::Expr::Case {
            when_then,
            else_expr,
            ..
        } => common_primitive(
            document,
            when_then
                .iter()
                .map(|(_, then)| then)
                .chain(else_expr.as_deref()),
        ),
        hir::Expr::Function(call) => {
            let hir::FunctionArguments::Expressions { values, .. } = &call.arguments else {
                return None;
            };
            match call.function.value() {
                Func::Scalar(
                    ScalarFunc::Coalesce | ScalarFunc::IfNull | ScalarFunc::Max | ScalarFunc::Min,
                ) => common_primitive(document, values.iter()),
                Func::Scalar(ScalarFunc::Nullif) => expression_primitive(document, values.first()?),
                Func::Scalar(ScalarFunc::Abs) => Some(arithmetic_primitive(
                    Some("INTEGER"),
                    expression_primitive(document, values.first()?),
                )),
                _ => None,
            }
        }
        _ => match expression_affinity(document, expression) {
            Affinity::Integer => Some("INTEGER"),
            Affinity::Real => Some("REAL"),
            Affinity::Text => Some("TEXT"),
            Affinity::Numeric => Some("NUMERIC"),
            Affinity::Blob | Affinity::None => None,
        },
    }
}

/// The type every non-NULL expression converts to, as PostgreSQL types
/// CASE branches and COALESCE arguments: mixed numeric types widen the way
/// arithmetic does, and any other mix has no common type.
fn common_primitive<'a>(
    document: &HirDocument,
    expressions: impl Iterator<Item = &'a hir::Expr>,
) -> Option<&'static str> {
    let is_numeric = |primitive| matches!(primitive, "INTEGER" | "REAL" | "NUMERIC");
    let mut common = None;
    for expression in expressions
        .filter(|expression| !matches!(expression, hir::Expr::Literal(ast::Literal::Null)))
    {
        let primitive = expression_primitive(document, expression)?;
        common = match common {
            None => Some(primitive),
            Some(common) if common == primitive => Some(common),
            Some(common) if is_numeric(common) && is_numeric(primitive) => {
                Some(arithmetic_primitive(Some(common), Some(primitive)))
            }
            Some(_) => return None,
        };
    }
    common
}

/// The type of arithmetic over two operands: INTEGER over two INTEGERs,
/// REAL over INTEGERs and REALs, and NUMERIC otherwise.
fn arithmetic_primitive(lhs: Option<&'static str>, rhs: Option<&'static str>) -> &'static str {
    match (lhs, rhs) {
        (Some("INTEGER"), Some("INTEGER")) => "INTEGER",
        (Some("INTEGER" | "REAL"), Some("INTEGER" | "REAL")) => "REAL",
        _ => "NUMERIC",
    }
}

/// The affinity of an operand: a column's or CAST's own, and none for other
/// expressions.
fn expression_affinity(document: &HirDocument, expression: &hir::Expr) -> Affinity {
    match expression {
        hir::Expr::Column(reference) => document
            .source(reference.source)
            .and_then(|source| source.columns.get(reference.column))
            .map_or(Affinity::Blob, |column| column.affinity()),
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
        // SQLite names a USING column after the left table's column.
        hir::Expr::MergedColumn(merged) => resolved_result_value(document, &merged.left),
        hir::Expr::Column(reference) => {
            let source = source_of(reference.source)?;
            if let Some(table) = catalog_table_object(source) {
                if table.value().get_column_at(reference.column).is_none() {
                    return Err(LimboError::InternalError(format!(
                        "HIR result references missing column {reference:?}"
                    )));
                }
                return Ok(ResolvedResultValue::Column(
                    ResolvedResultSourceColumn::Table {
                        table: table.handle(),
                        column: reference.column,
                    },
                ));
            }
            let column = source.columns.get(reference.column).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR result references missing column {reference:?}"
                ))
            })?;
            let type_fact = column.type_fact();
            let declared = type_fact.declared.as_ref();
            // A subquery column that reads a rowid is declared INTEGER.
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
                        .or(column.type_fact().storage)
                        .unwrap_or(Type::Null),
                )
            };
            Ok(ResolvedResultValue::Column(
                ResolvedResultSourceColumn::Query {
                    // A FROM subquery is reported by its alias and a CTE by its
                    // name.
                    table_name: match (&source.kind, &source.alias) {
                        (
                            hir::SourceKind::Derived(_) | hir::SourceKind::FromGroup(_),
                            Some(alias),
                        ) => alias.clone(),
                        _ => source.name.clone(),
                    },
                    column_name: column.name().to_string(),
                    declared_type,
                    ty,
                    array_dimensions: declared.map_or(0, |declared| declared.array_dimensions),
                },
            ))
        }
        hir::Expr::RowId(source) => {
            let alias = catalog_table(source_of(*source)?)
                .and_then(|table| table.btree())
                .and_then(|table| {
                    table
                        .get_rowid_alias_column()
                        .map(|(_, column)| (column.name.clone(), column.ty_str.clone()))
                });
            Ok(match alias {
                Some((name, declared_type)) => ResolvedResultValue::RowId {
                    name: name.unwrap_or_else(|| "rowid".to_string()),
                    declared_type: (!declared_type.is_empty()).then_some(declared_type),
                },
                None => ResolvedResultValue::RowId {
                    name: "rowid".to_string(),
                    declared_type: None,
                },
            })
        }
        _ => Ok(ResolvedResultValue::Expression),
    }
}

fn catalog_table(source: &hir::Source) -> Option<&crate::schema::Table> {
    catalog_table_object(source).map(|table| table.value())
}

fn catalog_table_object(source: &hir::Source) -> Option<&hir::ResolvedTable> {
    match &source.kind {
        hir::SourceKind::Table(table)
        | hir::SourceKind::TableFunction { table, .. }
        | hir::SourceKind::Pseudo { table, .. } => Some(table),
        hir::SourceKind::SchemaExpression
        | hir::SourceKind::Cte(_)
        | hir::SourceKind::Derived(_)
        | hir::SourceKind::FromGroup(_)
        | hir::SourceKind::RecursiveInput(_) => None,
    }
}
