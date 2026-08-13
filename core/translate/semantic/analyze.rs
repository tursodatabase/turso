use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast;

use crate::{
    numeric::Numeric,
    schema::{Type, TypeDef},
    sync::Arc,
    util::parse_numeric_literal,
    LimboError, Result, Value,
};

use super::{
    context::SemanticContext,
    expr::{ExprPolicies, ExprPolicy, QueryFunctionState},
    hir::{
        BoundSchemaProgram, CatalogObjectId, ColumnReadExpression, ColumnRef, CompoundArm, Cte,
        CteId, DatabaseId, Delete, DeleteTargetKind, Expr, FunctionEvaluation, HirDocument,
        HirRoot, Insert, InsertTargetKind, Limit, OrderTerm, Output, OutputId, OutputNameKind,
        OutputOwner, Query, QueryBlock, QueryBlockBody, QueryBlockId, QueryId, QueryRoot,
        SchemaProgramId, Source, SourceId, SourceKind, TriggerBody, TriggerCommand, TypeFact,
        Update, UpdateTargetKind,
    },
    query::FromContext,
    scope::{ExpandedColumn, ExprCollation, ResolvedScopeExpr, Scope},
    AnalyzeInput,
};

pub(crate) fn analyze<'context, 'catalog, 'ast>(
    context: &'context SemanticContext<'catalog>,
    input: AnalyzeInput<'ast>,
) -> Result<HirDocument> {
    let mut analyzer = Analyzer::new(context);
    let root = match input {
        AnalyzeInput::Statement(ast::Stmt::Select(select)) => HirRoot::Query(QueryRoot {
            query: analyzer.analyze_select(select)?,
        }),
        AnalyzeInput::Statement(ast::Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        }) => analyzer.analyze_insert(
            with.as_ref(),
            *or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        )?,
        AnalyzeInput::Statement(ast::Stmt::Update(update)) => analyzer.analyze_update(update)?,
        AnalyzeInput::Statement(ast::Stmt::Delete {
            with,
            tbl_name,
            indexed,
            where_clause,
            returning,
        }) => analyzer.analyze_delete(
            with.as_ref(),
            tbl_name,
            indexed.as_ref(),
            where_clause.as_deref(),
            returning,
        )?,
        AnalyzeInput::Statement(_) => {
            return Err(LimboError::ParseError(
                "semantic analysis accepts SELECT, INSERT, UPDATE, and DELETE statements"
                    .to_string(),
            ));
        }
        AnalyzeInput::TriggerPredicate {
            context,
            expression,
        } => analyzer.analyze_trigger_predicate(context, expression)?,
        AnalyzeInput::TriggerSelect { context, select } => {
            analyzer.analyze_trigger_select(context, select)?
        }
        AnalyzeInput::TriggerInsert { context, insert } => {
            analyzer.analyze_trigger_insert(context, insert)?
        }
        AnalyzeInput::TriggerUpdate { context, update } => {
            analyzer.analyze_trigger_update(context, update)?
        }
        AnalyzeInput::TriggerDelete { context, delete } => {
            analyzer.analyze_trigger_delete(context, delete)?
        }
        AnalyzeInput::TriggerProgram(input) => analyzer.analyze_trigger_program(input)?,
    };
    analyzer.finish_column_reads(&root)?;
    let document = analyzer.finish(root)?;
    document.validate().map_err(|error| {
        LimboError::InternalError(format!("semantic analysis produced invalid HIR: {error}"))
    })?;
    Ok(document)
}

fn collect_root_dml<'a>(
    root: &'a HirRoot,
    inserts: &mut Vec<&'a Insert>,
    updates: &mut Vec<&'a Update>,
    deletes: &mut Vec<&'a Delete>,
) {
    match root {
        HirRoot::Insert(insert) => inserts.push(insert),
        HirRoot::Update(update) => updates.push(update),
        HirRoot::Delete(delete) => deletes.push(delete),
        HirRoot::Trigger(root) => match &root.body {
            TriggerBody::Predicate(_) => {}
            TriggerBody::Command(command) => {
                collect_trigger_command_dml(command, inserts, updates, deletes);
            }
            TriggerBody::Program(program) => {
                for command in &program.commands {
                    collect_trigger_command_dml(command, inserts, updates, deletes);
                }
            }
        },
        HirRoot::Query(_) | HirRoot::SchemaExpressions(_) => {}
    }
}

fn collect_trigger_command_dml<'a>(
    command: &'a TriggerCommand,
    inserts: &mut Vec<&'a Insert>,
    updates: &mut Vec<&'a Update>,
    deletes: &mut Vec<&'a Delete>,
) {
    match command {
        TriggerCommand::Select(_) => {}
        TriggerCommand::Insert(insert) => inserts.push(insert),
        TriggerCommand::Update(update) => updates.push(update),
        TriggerCommand::Delete(delete) => deletes.push(delete),
    }
}

pub(super) struct Analyzer<'context, 'catalog, 'ast> {
    context: &'context SemanticContext<'catalog>,
    queries: Vec<Option<Query>>,
    sources: Vec<Option<Source>>,
    ctes: Vec<Option<Cte>>,
    schema_programs: Vec<Option<BoundSchemaProgram>>,
    schema_programs_in_progress: HashSet<CatalogObjectId>,
    catalog_ids: HashMap<CatalogIdentity, CatalogObjectId>,
    pub(super) cte_scopes: Vec<super::cte::CteScope<'ast>>,
}

pub(super) struct SelectContext<'scope> {
    pub(super) parent: Option<QueryId>,
    pub(super) outer_scope: Option<&'scope Scope>,
    pub(super) expected_outputs: Option<&'scope [Option<Arc<TypeDef>>]>,
    pub(super) policies: ExprPolicies,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum CatalogObjectKind {
    Table,
    Index,
    Sequence,
    Trigger,
    Collation,
    Type,
    Function { argument_count: usize },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CatalogIdentity {
    database: Option<DatabaseId>,
    kind: CatalogObjectKind,
    name: String,
}

impl<'context, 'catalog, 'ast> Analyzer<'context, 'catalog, 'ast> {
    pub(super) fn new(context: &'context SemanticContext<'catalog>) -> Self {
        Self {
            context,
            queries: Vec::new(),
            sources: Vec::new(),
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            schema_programs_in_progress: HashSet::default(),
            catalog_ids: HashMap::default(),
            cte_scopes: Vec::new(),
        }
    }

    pub(super) const fn context(&self) -> &SemanticContext<'catalog> {
        self.context
    }

    pub(super) fn reserve_query(&mut self) -> QueryId {
        let id = QueryId::new(self.queries.len());
        self.queries.push(None);
        id
    }

    pub(super) fn reserve_cte(&mut self) -> CteId {
        let id = CteId::new(self.ctes.len());
        self.ctes.push(None);
        id
    }

    pub(super) fn insert_cte(&mut self, id: CteId, cte: Cte) -> Result<()> {
        if cte.id != id {
            return Err(LimboError::InternalError(format!(
                "CTE {} was inserted into slot {}",
                cte.id, id
            )));
        }
        Self::insert_reserved(&mut self.ctes, id.index(), cte, "CTE")
    }

    pub(super) fn cte(&self, id: CteId) -> Option<&Cte> {
        self.ctes.get(id.index())?.as_ref()
    }

    pub(super) fn reserve_source(&mut self) -> SourceId {
        let id = SourceId::new(self.sources.len());
        self.sources.push(None);
        id
    }

    pub(super) fn insert_source(&mut self, id: SourceId, source: Source) -> Result<()> {
        if source.id != id {
            return Err(LimboError::InternalError(format!(
                "source {} was inserted into slot {}",
                source.id, id
            )));
        }
        Self::insert_reserved(&mut self.sources, id.index(), source, "source")
    }

    pub(super) fn source(&self, id: SourceId) -> Option<&Source> {
        self.sources.get(id.index())?.as_ref()
    }

    pub(super) fn source_mut(&mut self, id: SourceId) -> Option<&mut Source> {
        self.sources.get_mut(id.index())?.as_mut()
    }

    pub(super) fn reserve_schema_program(&mut self) -> SchemaProgramId {
        let id = SchemaProgramId::new(self.schema_programs.len());
        self.schema_programs.push(None);
        id
    }

    pub(super) fn insert_schema_program(
        &mut self,
        id: SchemaProgramId,
        program: BoundSchemaProgram,
    ) -> Result<()> {
        Self::insert_reserved(
            &mut self.schema_programs,
            id.index(),
            program,
            "schema program",
        )
    }

    pub(super) fn enter_schema_program_binding(&mut self, definition: CatalogObjectId) -> bool {
        self.schema_programs_in_progress.insert(definition)
    }

    pub(super) fn leave_schema_program_binding(&mut self, definition: CatalogObjectId) {
        assert!(
            self.schema_programs_in_progress.remove(&definition),
            "schema program binding must be active before it finishes"
        );
    }

    pub(super) fn catalog_object_id(
        &mut self,
        database: Option<DatabaseId>,
        kind: CatalogObjectKind,
        name: impl Into<String>,
    ) -> CatalogObjectId {
        let identity = CatalogIdentity {
            database,
            kind,
            name: name.into(),
        };
        if let Some(id) = self.catalog_ids.get(&identity) {
            return *id;
        }
        let id = CatalogObjectId::new(self.catalog_ids.len() as u64);
        self.catalog_ids.insert(identity, id);
        id
    }

    pub(super) fn insert_query(&mut self, id: QueryId, query: Query) -> Result<()> {
        if query.id != id {
            return Err(LimboError::InternalError(format!(
                "query {} was inserted into slot {}",
                query.id, id
            )));
        }
        Self::insert_reserved(&mut self.queries, id.index(), query, "query")
    }

    pub(super) fn query(&self, id: QueryId) -> Option<&Query> {
        self.queries.get(id.index())?.as_ref()
    }

    fn insert_reserved<T>(
        arena: &mut [Option<T>],
        index: usize,
        value: T,
        kind: &str,
    ) -> Result<()> {
        let Some(slot) = arena.get_mut(index) else {
            return Err(LimboError::InternalError(format!(
                "{kind} slot {index} was not reserved"
            )));
        };
        if slot.is_some() {
            return Err(LimboError::InternalError(format!(
                "{kind} slot {index} was filled twice"
            )));
        }
        *slot = Some(value);
        Ok(())
    }

    fn finish_arena<T>(arena: Vec<Option<T>>, kind: &str) -> Result<Vec<T>> {
        arena
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "reserved {kind} slot {index} was not filled"
                    ))
                })
            })
            .collect()
    }

    fn finish(self, root: HirRoot) -> Result<HirDocument> {
        Ok(HirDocument {
            snapshot: self.context.snapshot(),
            databases: self.context.database_snapshots(),
            root,
            queries: Self::finish_arena(self.queries, "query")?,
            sources: Self::finish_arena(self.sources, "source")?,
            ctes: Self::finish_arena(self.ctes, "CTE")?,
            schema_programs: Self::finish_arena(self.schema_programs, "schema program")?,
            cdc: None,
        })
    }

    fn finish_column_reads(&mut self, root: &HirRoot) -> Result<()> {
        let mut pending = Vec::new();
        for query in self.queries.iter().flatten() {
            pending.extend(query.direct_column_reads(|source| self.source(source)));
        }
        let mut inserts = Vec::new();
        let mut updates = Vec::new();
        let mut deletes = Vec::new();
        collect_root_dml(root, &mut inserts, &mut updates, &mut deletes);

        for insert in inserts {
            if let InsertTargetKind::BTree { foreign_keys, .. } = &insert.target_kind {
                let width = self
                    .source(insert.target)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "missing INSERT target source {}",
                            insert.target
                        ))
                    })?
                    .columns
                    .len();
                pending.extend((0..width).map(|column| ColumnRef {
                    source: insert.target,
                    column,
                }));
                pending.extend(foreign_keys.incoming.iter().flat_map(|foreign_key| {
                    foreign_key
                        .child_positions
                        .iter()
                        .copied()
                        .map(|column| ColumnRef {
                            source: foreign_key.child_source,
                            column,
                        })
                }));
            }
        }
        for update in updates {
            pending.extend(update.direct_column_reads(|source| self.source(source)));
            if let UpdateTargetKind::BTree { foreign_keys, .. } = &update.target_kind {
                for source in [update.target, update.new_source] {
                    let width = self
                        .source(source)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!("missing UPDATE row source {source}"))
                        })?
                        .columns
                        .len();
                    pending.extend((0..width).map(|column| ColumnRef { source, column }));
                }
                pending.extend(foreign_keys.incoming.iter().flat_map(|foreign_key| {
                    foreign_key
                        .child_positions
                        .iter()
                        .copied()
                        .map(|column| ColumnRef {
                            source: foreign_key.child_source,
                            column,
                        })
                }));
            }
        }
        for delete in deletes {
            pending.extend(delete.direct_column_reads());
            if let DeleteTargetKind::BTree { foreign_keys, .. } = &delete.target_kind {
                let width = self
                    .source(delete.target)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "missing DELETE target source {}",
                            delete.target
                        ))
                    })?
                    .columns
                    .len();
                pending.extend((0..width).map(|column| ColumnRef {
                    source: delete.target,
                    column,
                }));
                pending.extend(foreign_keys.incoming.iter().flat_map(|foreign_key| {
                    foreign_key
                        .child_positions
                        .iter()
                        .copied()
                        .map(|column| ColumnRef {
                            source: foreign_key.child_source,
                            column,
                        })
                }));
            }
        }
        let mut finished = HashSet::default();

        while let Some(reference) = pending.pop() {
            if !finished.insert(reference) {
                continue;
            }
            let (scope, generated_syntax, default_syntax) = {
                let source = self.source(reference.source).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing source {} while planning stored column expressions",
                        reference.source
                    ))
                })?;
                source.columns.get(reference.column).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "missing column {}.{} while planning stored column expressions",
                        reference.source, reference.column
                    ))
                })?;
                let catalog_table = match &source.kind {
                    SourceKind::Table(table)
                    | SourceKind::TableFunction { table, .. }
                    | SourceKind::Pseudo { table, .. } => table.value(),
                    _ => continue,
                };
                let catalog_column =
                    catalog_table
                        .columns()
                        .get(reference.column)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "catalog column {}.{} is missing",
                                reference.source, reference.column
                            ))
                        })?;
                let generated_syntax = match &source.generated_expressions[reference.column] {
                    ColumnReadExpression::NotRequired => Some(
                        catalog_column
                            .generated_expr()
                            .ok_or_else(|| {
                                LimboError::InternalError(format!(
                                    "source {} column {} requires a missing generated expression",
                                    reference.source, reference.column
                                ))
                            })?
                            .clone(),
                    ),
                    ColumnReadExpression::Absent | ColumnReadExpression::Planned(_) => None,
                };
                let default_syntax = match &source.default_expressions[reference.column] {
                    ColumnReadExpression::NotRequired => Some(
                        catalog_column
                            .default
                            .as_deref()
                            .ok_or_else(|| {
                                LimboError::InternalError(format!(
                                    "source {} column {} requires a missing default expression",
                                    reference.source, reference.column
                                ))
                            })?
                            .clone(),
                    ),
                    ColumnReadExpression::Absent | ColumnReadExpression::Planned(_) => None,
                };
                let mut scope = Scope::default();
                scope.add_source(source, true);
                (scope, generated_syntax, default_syntax)
            };

            let policy = ExprPolicy::schema_expression().with_self_source(reference.source);
            let generated = generated_syntax
                .as_ref()
                .map(|syntax| self.analyze_expr(syntax, &scope, policy))
                .transpose()?;
            let default = default_syntax
                .as_ref()
                .map(|syntax| self.analyze_expr(syntax, &scope, policy))
                .transpose()?;

            for expression in generated.iter().chain(default.iter()) {
                expression.walk(&mut |expression| match expression {
                    Expr::Column(dependency) => pending.push(*dependency),
                    Expr::MergedColumn(column) => pending.push(column.right),
                    _ => {}
                });
            }

            let source = self.source_mut(reference.source).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "missing source {} after planning stored column expressions",
                    reference.source
                ))
            })?;
            if let Some(expression) = generated {
                source.generated_expressions[reference.column] =
                    ColumnReadExpression::Planned(expression);
            }
            if let Some(expression) = default {
                source.default_expressions[reference.column] =
                    ColumnReadExpression::Planned(expression);
            }
        }
        Ok(())
    }

    pub(super) fn analyze_select(&mut self, select: &'ast ast::Select) -> Result<QueryId> {
        self.analyze_select_with_scope(
            select,
            None,
            None,
            None,
            ExprPolicies::statement(self.context.dqs_dml()),
        )
    }

    pub(super) fn analyze_select_with_expected_outputs(
        &mut self,
        select: &'ast ast::Select,
        expected_outputs: &[Option<Arc<TypeDef>>],
    ) -> Result<QueryId> {
        self.analyze_select_with_scope(
            select,
            None,
            None,
            Some(expected_outputs),
            ExprPolicies::statement(self.context.dqs_dml()),
        )
    }

    pub(super) fn analyze_select_with_parent(
        &mut self,
        select: &'ast ast::Select,
        parent: Option<QueryId>,
    ) -> Result<QueryId> {
        self.analyze_select_with_scope(
            select,
            parent,
            None,
            None,
            ExprPolicies::statement(self.context.dqs_dml()),
        )
    }

    pub(super) fn analyze_subquery(
        &mut self,
        select: &'ast ast::Select,
        parent: Option<QueryId>,
        outer_scope: &Scope,
        policies: ExprPolicies,
    ) -> Result<QueryId> {
        self.analyze_select_with_scope(select, parent, Some(outer_scope), None, policies)
    }

    pub(super) fn analyze_subquery_with_expected_outputs(
        &mut self,
        select: &'ast ast::Select,
        parent: Option<QueryId>,
        outer_scope: &Scope,
        expected_outputs: &[Option<Arc<TypeDef>>],
        policies: ExprPolicies,
    ) -> Result<QueryId> {
        self.analyze_select_with_scope(
            select,
            parent,
            Some(outer_scope),
            Some(expected_outputs),
            policies,
        )
    }

    pub(super) fn analyze_select_with_scope(
        &mut self,
        select: &'ast ast::Select,
        parent: Option<QueryId>,
        outer_scope: Option<&Scope>,
        expected_outputs: Option<&[Option<Arc<TypeDef>>]>,
        policies: ExprPolicies,
    ) -> Result<QueryId> {
        self.with_cte_scope(select.with.as_ref(), |analyzer| {
            analyzer.analyze_select_body(select, parent, outer_scope, expected_outputs, policies)
        })
    }

    fn analyze_select_body(
        &mut self,
        select: &'ast ast::Select,
        parent: Option<QueryId>,
        outer_scope: Option<&Scope>,
        expected_outputs: Option<&[Option<Arc<TypeDef>>]>,
        policies: ExprPolicies,
    ) -> Result<QueryId> {
        self.analyze_select_parts(
            &select.body.select,
            &select.body.compounds,
            &select.order_by,
            select.limit.as_ref(),
            SelectContext {
                parent,
                outer_scope,
                expected_outputs,
                policies,
            },
        )
    }

    pub(super) fn analyze_select_parts(
        &mut self,
        first_select: &'ast ast::OneSelect,
        compounds: &'ast [ast::CompoundSelect],
        order_by_syntax: &'ast [ast::SortedColumn],
        limit_syntax: Option<&'ast ast::Limit>,
        context: SelectContext<'_>,
    ) -> Result<QueryId> {
        let SelectContext {
            parent,
            outer_scope,
            expected_outputs,
            policies,
        } = context;
        let rightmost = compounds
            .last()
            .map(|compound| &compound.select)
            .unwrap_or(first_select);
        if matches!(rightmost, ast::OneSelect::Values(_)) {
            if !order_by_syntax.is_empty() {
                crate::bail_parse_error!("ORDER BY clause is not allowed with VALUES clause");
            }
            if limit_syntax.is_some() {
                crate::bail_parse_error!("LIMIT clause is not allowed with VALUES clause");
            }
        }

        let query_id = self.reserve_query();
        let mut blocks = Vec::with_capacity(compounds.len() + 1);
        let ordinary_order_by = compounds.is_empty().then_some(order_by_syntax);
        let (first_block, mut order_by) = self.analyze_select_block(
            first_select,
            query_id,
            0,
            outer_scope,
            ordinary_order_by,
            expected_outputs,
            policies,
        )?;
        blocks.push(first_block);
        for (index, compound) in compounds.iter().enumerate() {
            let (block, block_order_by) = self.analyze_select_block(
                &compound.select,
                query_id,
                index + 1,
                outer_scope,
                None,
                expected_outputs,
                policies,
            )?;
            debug_assert!(block_order_by.is_empty());
            blocks.push(block);
        }

        if let Some(rightmost) = blocks.last() {
            let rightmost_width = rightmost.outputs.len();
            if let Some((_, compound)) = blocks[..blocks.len() - 1]
                .iter()
                .zip(compounds)
                .find(|(block, _)| block.outputs.len() != rightmost_width)
            {
                crate::bail_parse_error!(
                    "SELECTs to the left and right of {} do not have the same number of result columns",
                    compound.operator
                );
            }
        }

        if !compounds.is_empty() {
            order_by = self.analyze_compound_order_by(order_by_syntax, &blocks)?;
        }
        let limit = limit_syntax
            .map(|limit| self.analyze_limit(limit, query_id, policies))
            .transpose()?;

        let first = blocks[0].id;
        let output = blocks[0].outputs.iter().map(|output| output.id).collect();
        let compounds = compounds
            .iter()
            .zip(blocks.iter().skip(1))
            .map(|(compound, block)| CompoundArm {
                operator: compound.operator,
                block: block.id,
            })
            .collect();
        let reachable_ctes = self.direct_ctes(&blocks)?;

        let mut query = Query {
            id: query_id,
            parent,
            captures: Vec::new(),
            reachable_ctes,
            blocks,
            first,
            compounds,
            order_by,
            limit,
            output,
        };
        query.captures = query.direct_captures(|source| self.source(source));
        self.insert_query(query_id, query)?;

        Ok(query_id)
    }

    pub(super) fn direct_ctes(&self, blocks: &[QueryBlock]) -> Result<Vec<CteId>> {
        let mut ctes = Vec::new();
        for block in blocks {
            let Some(from) = &block.from else {
                continue;
            };
            self.add_from_ctes(from, &mut ctes)?;
        }
        Ok(ctes)
    }

    fn add_from_ctes(&self, from: &super::hir::From, ctes: &mut Vec<CteId>) -> Result<()> {
        self.add_source_cte(from.first, ctes)?;
        for join in &from.joins {
            self.add_source_cte(join.right, ctes)?;
        }
        Ok(())
    }

    fn add_source_cte(&self, source: SourceId, ctes: &mut Vec<CteId>) -> Result<()> {
        let definition = self.source(source).ok_or_else(|| {
            LimboError::InternalError(format!("missing semantic source {source}"))
        })?;
        match &definition.kind {
            super::hir::SourceKind::Cte(cte) | super::hir::SourceKind::RecursiveInput(cte) => {
                if !ctes.contains(cte) {
                    ctes.push(*cte);
                }
            }
            super::hir::SourceKind::FromGroup(group) => {
                self.add_from_ctes(&group.from, ctes)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn analyze_select_block(
        &mut self,
        select: &'ast ast::OneSelect,
        query: QueryId,
        index: usize,
        outer_scope: Option<&Scope>,
        order_by: Option<&'ast [ast::SortedColumn]>,
        expected_outputs: Option<&[Option<Arc<TypeDef>>]>,
        policies: ExprPolicies,
    ) -> Result<(QueryBlock, Vec<OrderTerm>)> {
        match select {
            ast::OneSelect::Select {
                distinctness,
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
            } => self.analyze_projection_block(
                *distinctness,
                columns,
                from.as_ref(),
                where_clause.as_deref(),
                group_by.as_ref(),
                window_clause,
                query,
                index,
                outer_scope,
                order_by,
                expected_outputs,
                policies,
            ),
            ast::OneSelect::Values(rows) => {
                debug_assert!(order_by.is_none_or(<[_]>::is_empty));
                self.analyze_values_block(
                    rows,
                    query,
                    index,
                    outer_scope,
                    expected_outputs,
                    policies,
                )
                .map(|block| (block, Vec::new()))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_projection_block(
        &mut self,
        distinctness: Option<ast::Distinctness>,
        columns: &'ast [ast::ResultColumn],
        from: Option<&'ast ast::FromClause>,
        where_clause: Option<&'ast ast::Expr>,
        group_by: Option<&'ast ast::GroupBy>,
        window_clause: &'ast [ast::WindowDef],
        query: QueryId,
        index: usize,
        outer_scope: Option<&Scope>,
        order_by: Option<&'ast [ast::SortedColumn]>,
        expected_outputs: Option<&[Option<Arc<TypeDef>>]>,
        policies: ExprPolicies,
    ) -> Result<(QueryBlock, Vec<OrderTerm>)> {
        if from.is_none()
            && columns
                .iter()
                .any(|column| matches!(column, ast::ResultColumn::Star))
        {
            crate::bail_parse_error!("no tables specified");
        }

        let block_id = QueryBlockId::new(query, index);
        let (from, scope) = match from {
            Some(from) => {
                let (from, scope) = self.analyze_from_clause(
                    from,
                    FromContext::QueryBlock {
                        block: block_id,
                        outer_scope,
                        policies,
                    },
                )?;
                (Some(from), scope)
            }
            None => (None, Scope::new(outer_scope.cloned())),
        };
        let mut functions = QueryFunctionState::new(block_id);
        self.analyze_named_windows(window_clause, &scope, policies.select(), &mut functions)?;
        let outputs = self.analyze_outputs(
            block_id,
            columns,
            &scope,
            &mut functions,
            expected_outputs,
            policies,
        )?;
        let filter = match where_clause {
            Some(syntax) => {
                let mut clause_scope = scope.clone();
                clause_scope.set_outputs(&outputs);
                let resolved = self.analyze_query_expr(
                    syntax,
                    &clause_scope,
                    policies.where_clause(),
                    &mut functions,
                )?;
                reject_clause_alias_functions(&resolved.expr, block_id, &outputs, false)?;
                Some(resolved.expr)
            }
            None => None,
        };
        let grouping = group_by
            .map(|group_by| {
                self.analyze_grouping(
                    group_by,
                    &scope,
                    &outputs,
                    block_id,
                    &mut functions,
                    policies,
                )
            })
            .transpose()?;
        let aggregate_query = grouping.is_some() || functions.aggregate_count() > 0;
        let order_by = order_by
            .map(|order_by| {
                self.analyze_order_by(
                    order_by,
                    &scope,
                    &outputs,
                    block_id,
                    aggregate_query,
                    &mut functions,
                    policies,
                )
            })
            .transpose()?
            .unwrap_or_default();
        let aggregate_count = functions.aggregate_count();
        let window_function_count = functions.window_function_count();
        let windows = functions.take_windows();

        Ok((
            QueryBlock {
                id: block_id,
                from,
                outputs,
                aggregate_count,
                window_function_count,
                windows,
                body: QueryBlockBody::Select {
                    distinctness,
                    filter,
                    grouping,
                },
            },
            order_by,
        ))
    }

    fn analyze_values_block(
        &mut self,
        rows: &'ast [Vec<Box<ast::Expr>>],
        query: QueryId,
        index: usize,
        outer_scope: Option<&Scope>,
        expected_outputs: Option<&[Option<Arc<TypeDef>>]>,
        policies: ExprPolicies,
    ) -> Result<QueryBlock> {
        let block = QueryBlockId::new(query, index);
        let scope = Scope::new(outer_scope.cloned());
        let policy = policies.select();
        let mut functions = QueryFunctionState::new(block);
        let mut resolved_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut resolved_row = Vec::with_capacity(row.len());
            for (index, expression) in row.iter().enumerate() {
                resolved_row.push(
                    self.analyze_query_expr_with_expected_type(
                        expression,
                        &scope,
                        policy,
                        &mut functions,
                        expected_outputs
                            .and_then(|outputs| outputs.get(index))
                            .cloned()
                            .flatten(),
                    )?,
                );
            }
            resolved_rows.push(resolved_row);
        }

        let first = resolved_rows
            .first()
            .expect("parser guarantees one VALUES row");
        let outputs = first
            .iter()
            .enumerate()
            .map(|(index, resolved)| {
                let mut descriptor = resolved.clone();
                // VALUES rows own runtime expressions. This placeholder only
                // identifies the result position, matching the old planner.
                descriptor.expr = Expr::Literal(ast::Literal::Numeric(index.to_string()));
                output_from_resolved(
                    OutputId::query(block, index),
                    format!("column{}", index + 1),
                    OutputNameKind::Inferred,
                    descriptor,
                )
            })
            .collect();
        let rows = resolved_rows
            .into_iter()
            .map(|row| row.into_iter().map(|resolved| resolved.expr).collect())
            .collect();
        let aggregate_count = functions.aggregate_count();
        let window_function_count = functions.window_function_count();
        let windows = functions.take_windows();

        Ok(QueryBlock {
            id: block,
            from: None,
            outputs,
            aggregate_count,
            window_function_count,
            windows,
            body: QueryBlockBody::Values { rows },
        })
    }

    fn analyze_grouping(
        &mut self,
        syntax: &'ast ast::GroupBy,
        scope: &Scope,
        outputs: &[Output],
        block: QueryBlockId,
        functions: &mut QueryFunctionState,
        policies: ExprPolicies,
    ) -> Result<super::hir::Grouping> {
        let mut grouping_scope = scope.clone().without_outer_resolution();
        grouping_scope.set_outputs(outputs);
        let mut keys = Vec::with_capacity(syntax.exprs.len());
        let mut key_type_facts = Vec::with_capacity(syntax.exprs.len());
        let mut key_collations = Vec::with_capacity(syntax.exprs.len());
        for syntax in &syntax.exprs {
            let resolved =
                match self.resolve_output_ordinal_expr(syntax, &grouping_scope, "1st GROUP BY")? {
                    Some(resolved) => resolved,
                    None => self.analyze_query_expr(
                        syntax,
                        &grouping_scope,
                        policies.group_by(),
                        functions,
                    )?,
                };
            reject_clause_alias_functions(&resolved.expr, block, outputs, false)?;
            key_type_facts.push(resolved.type_fact.clone());
            key_collations.push(resolved.collation.value().cloned());
            keys.push(resolved.expr);
        }

        let having = match syntax.having.as_deref() {
            Some(syntax) => {
                let mut having_scope = scope.clone();
                having_scope.set_outputs(outputs);
                let resolved =
                    self.analyze_query_expr(syntax, &having_scope, policies.having(), functions)?;
                reject_clause_alias_functions(&resolved.expr, block, outputs, true)?;
                reject_aliased_aggregates(&resolved.expr, block, outputs)?;
                Some(resolved.expr)
            }
            None => None,
        };
        Ok(super::hir::Grouping {
            keys,
            key_type_facts,
            key_collations,
            having,
        })
    }

    fn analyze_order_by(
        &mut self,
        syntax: &'ast [ast::SortedColumn],
        scope: &Scope,
        outputs: &[Output],
        block: QueryBlockId,
        aggregate_query: bool,
        functions: &mut QueryFunctionState,
        policies: ExprPolicies,
    ) -> Result<Vec<OrderTerm>> {
        let mut order_scope = scope.clone();
        order_scope.set_outputs(outputs);
        syntax
            .iter()
            .map(|syntax| {
                let resolved = match self.resolve_output_ordinal_expr(
                    &syntax.expr,
                    &order_scope,
                    "1st ORDER BY",
                )? {
                    Some(resolved) => resolved,
                    None => self.analyze_query_expr(
                        &syntax.expr,
                        &order_scope,
                        policies.order_by(aggregate_query),
                        functions,
                    )?,
                };
                reject_aliased_aggregates(&resolved.expr, block, outputs)?;
                Ok(order_term(syntax, resolved))
            })
            .collect()
    }

    pub(super) fn analyze_limit(
        &mut self,
        syntax: &'ast ast::Limit,
        parent: QueryId,
        policies: ExprPolicies,
    ) -> Result<Limit> {
        let scope = Scope::default();
        let policy = policies.limit();
        let limit = self.analyze_query_scalar_expr(&syntax.expr, &scope, policy, parent)?;
        let offset = syntax
            .offset
            .as_deref()
            .map(|offset| self.analyze_query_scalar_expr(offset, &scope, policy, parent))
            .transpose()?;
        Ok(Limit {
            limit: limit.expr,
            offset: offset.map(|offset| offset.expr),
        })
    }

    fn analyze_compound_order_by(
        &mut self,
        syntax: &'ast [ast::SortedColumn],
        blocks: &[QueryBlock],
    ) -> Result<Vec<OrderTerm>> {
        let output_arms = blocks
            .iter()
            .map(|block| block.outputs.as_slice())
            .collect::<Vec<_>>();
        syntax
            .iter()
            .enumerate()
            .map(|(index, syntax)| {
                let resolved =
                    self.resolve_compound_order_by_expr(&syntax.expr, &output_arms, index + 1)?;
                Ok(order_term(syntax, resolved))
            })
            .collect()
    }

    fn resolve_compound_order_by_expr(
        &mut self,
        syntax: &ast::Expr,
        output_arms: &[&[Output]],
        term_number: usize,
    ) -> Result<ResolvedScopeExpr> {
        let (position, collations) = compound_order_by_position(syntax, output_arms, term_number)?;
        let outputs = output_arms
            .first()
            .expect("compound SELECT must have a first output arm");
        let mut resolved = resolved_output(&outputs[position]);
        for name in collations.iter().rev() {
            let collation = self.resolve_collation(name)?;
            resolved.expr = Expr::Collate {
                expr: Box::new(resolved.expr),
                collation: collation.clone(),
            };
            resolved.collation = ExprCollation::Explicit(collation);
        }
        Ok(resolved)
    }

    fn resolve_output_ordinal_expr(
        &mut self,
        syntax: &ast::Expr,
        scope: &Scope,
        clause: &str,
    ) -> Result<Option<ResolvedScopeExpr>> {
        enum Wrapper<'a> {
            Collate(&'a ast::Name),
        }

        let mut wrappers = Vec::new();
        let mut inner = syntax;
        loop {
            match inner {
                ast::Expr::Collate(expression, name) => {
                    wrappers.push(Wrapper::Collate(name));
                    inner = expression;
                }
                ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                    inner = &expressions[0];
                }
                _ => break,
            }
        }

        let Some(ordinal) = output_ordinal(inner) else {
            return Ok(None);
        };
        let position = match ordinal {
            OutputOrdinal::Index(position) => position,
            OutputOrdinal::OutOfRange => 0,
        };
        let mut resolved = scope.resolve_output_ordinal(position, clause)?;
        for wrapper in wrappers.into_iter().rev() {
            match wrapper {
                Wrapper::Collate(name) => {
                    let collation = self.resolve_collation(name)?;
                    resolved.expr = Expr::Collate {
                        expr: Box::new(resolved.expr),
                        collation: collation.clone(),
                    };
                    resolved.collation = ExprCollation::Explicit(collation);
                }
            }
        }
        Ok(Some(resolved))
    }

    fn analyze_outputs(
        &mut self,
        block: QueryBlockId,
        columns: &'ast [ast::ResultColumn],
        scope: &Scope,
        functions: &mut QueryFunctionState,
        expected_outputs: Option<&[Option<Arc<TypeDef>>]>,
        policies: ExprPolicies,
    ) -> Result<Vec<Output>> {
        let mut outputs = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                ast::ResultColumn::Expr(_, _) => {
                    let index = outputs.len();
                    let expected_type = expected_outputs
                        .and_then(|expected| expected.get(index))
                        .cloned()
                        .flatten();
                    outputs.push(self.analyze_output(
                        block,
                        index,
                        column,
                        scope,
                        functions,
                        expected_type,
                        policies,
                    )?);
                }
                ast::ResultColumn::Star => {
                    let expanded = scope.expand_star()?;
                    self.append_star_outputs(block, &mut outputs, expanded, scope)?;
                }
                ast::ResultColumn::TableStar(table) => {
                    let expanded = scope.expand_table_star(table.as_str())?;
                    self.append_star_outputs(block, &mut outputs, expanded, scope)?;
                }
            }
        }
        Ok(outputs)
    }

    fn append_star_outputs(
        &self,
        block: QueryBlockId,
        outputs: &mut Vec<Output>,
        expanded: Vec<ExpandedColumn>,
        scope: &Scope,
    ) -> Result<()> {
        for column in expanded {
            let resolved = self.resolve_atomic_expr(column.resolved.expr, scope)?;
            outputs.push(output_from_resolved(
                OutputId::query(block, outputs.len()),
                column.name,
                OutputNameKind::StarExpansion,
                resolved,
            ));
        }
        Ok(())
    }

    fn analyze_output(
        &mut self,
        block: QueryBlockId,
        index: usize,
        column: &'ast ast::ResultColumn,
        scope: &Scope,
        functions: &mut QueryFunctionState,
        expected_type: Option<Arc<TypeDef>>,
        policies: ExprPolicies,
    ) -> Result<Output> {
        let ast::ResultColumn::Expr(expression, alias) = column else {
            return unsupported_select();
        };
        let syntax = expression;
        let policy = policies.select();
        let resolved = self.analyze_query_expr_with_expected_type(
            syntax,
            scope,
            policy,
            functions,
            expected_type,
        )?;
        let (name, name_kind) = match alias {
            Some(alias) if alias.is_explicit() => (
                alias.name().as_str().to_string(),
                OutputNameKind::ExplicitAlias,
            ),
            Some(ast::As::ImplicitColumnName(name)) => {
                (name.as_str().to_string(), OutputNameKind::Inferred)
            }
            None => (syntax.to_string(), OutputNameKind::Inferred),
            Some(_) => unreachable!("all explicit aliases were handled"),
        };
        Ok(output_from_resolved(
            OutputId::query(block, index),
            name,
            name_kind,
            resolved,
        ))
    }
}

pub(super) fn output_from_resolved(
    id: OutputId,
    name: String,
    name_kind: OutputNameKind,
    resolved: ResolvedScopeExpr,
) -> Output {
    let (collation, collation_is_explicit) = resolved.collation.into_output();
    Output {
        id,
        name,
        expr: resolved.expr,
        type_fact: resolved.type_fact,
        affinity: resolved.affinity,
        schema_affinity: resolved.affinity,
        has_affinity: resolved.has_affinity,
        collation,
        collation_is_explicit,
        name_kind,
    }
}

fn reject_clause_alias_functions(
    expression: &Expr,
    block: QueryBlockId,
    outputs: &[Output],
    allow_aggregates: bool,
) -> Result<()> {
    let mut error = None;
    expression.walk(&mut |expression| {
        if error.is_some() {
            return;
        }
        let Expr::Output(id) = expression else {
            return;
        };
        let OutputOwner::QueryBlock(owner) = id.owner else {
            return;
        };
        if owner != block {
            return;
        }
        let Some(output) = outputs.get(id.index) else {
            error = Some(LimboError::InternalError(format!(
                "WHERE refers to missing output {}",
                id.index
            )));
            return;
        };
        output.expr.walk(&mut |expression| {
            if error.is_some() {
                return;
            }
            let Expr::Function(call) = expression else {
                return;
            };
            error = match call.evaluation {
                FunctionEvaluation::Aggregate { .. } if !allow_aggregates => {
                    Some(LimboError::ParseError(format!(
                        "misuse of aggregate: {}()",
                        call.function.value()
                    )))
                }
                FunctionEvaluation::Window { .. } => Some(LimboError::ParseError(format!(
                    "misuse of aliased window function {}",
                    output.name
                ))),
                FunctionEvaluation::Aggregate { .. } | FunctionEvaluation::Scalar => None,
            };
        });
    });
    error.map_or(Ok(()), Err)
}

fn reject_aliased_aggregates(having: &Expr, block: QueryBlockId, outputs: &[Output]) -> Result<()> {
    let mut error = None;
    having.walk(&mut |expression| {
        if error.is_some() {
            return;
        }
        let Expr::Function(call) = expression else {
            return;
        };
        if !matches!(call.evaluation, FunctionEvaluation::Aggregate { .. }) {
            return;
        }
        for argument in call.arguments.expressions() {
            argument.walk(&mut |expression| {
                if error.is_some() {
                    return;
                }
                let Expr::Output(id) = expression else {
                    return;
                };
                let OutputOwner::QueryBlock(owner) = id.owner else {
                    return;
                };
                if owner != block {
                    return;
                }
                let Some(output) = outputs.get(id.index) else {
                    error = Some(LimboError::InternalError(format!(
                        "HAVING refers to missing output {}",
                        id.index
                    )));
                    return;
                };
                if expression_contains_aggregate(&output.expr) {
                    error = Some(LimboError::ParseError(format!(
                        "misuse of aliased aggregate {}",
                        crate::util::normalize_ident(&output.name)
                    )));
                }
            });
        }
    });
    error.map_or(Ok(()), Err)
}

fn resolved_output(output: &Output) -> ResolvedScopeExpr {
    ResolvedScopeExpr {
        expr: Expr::Output(output.id),
        type_fact: output.type_fact.clone(),
        affinity: output.affinity,
        has_affinity: output.has_affinity,
        collation: ExprCollation::output(output.collation.clone(), output.collation_is_explicit),
    }
}

fn order_term(syntax: &ast::SortedColumn, resolved: ResolvedScopeExpr) -> OrderTerm {
    OrderTerm {
        expr: resolved.expr,
        order: syntax.order.unwrap_or(ast::SortOrder::Asc),
        nulls: syntax.nulls,
        type_fact: resolved.type_fact,
        collation: resolved.collation.value().cloned(),
    }
}

pub(super) fn compound_order_by_position<'syntax>(
    syntax: &'syntax ast::Expr,
    output_arms: &[&[Output]],
    term_number: usize,
) -> Result<(usize, Vec<&'syntax ast::Name>)> {
    let outputs = output_arms
        .first()
        .expect("compound SELECT must have a first output arm");
    let mut collations = Vec::new();
    let mut inner = syntax;
    while let ast::Expr::Collate(expression, name) = inner {
        collations.push(name);
        inner = expression;
    }
    let position = match inner {
        ast::Expr::Literal(ast::Literal::Numeric(value)) => {
            let Ok(position) = value.parse::<i32>() else {
                return compound_order_by_no_match(term_number);
            };
            if position <= 0 || position as usize > outputs.len() {
                crate::bail_parse_error!(
                    "{} ORDER BY term out of range - should be between 1 and {}",
                    position,
                    outputs.len()
                );
            }
            position as usize - 1
        }
        ast::Expr::Id(name) => {
            let name = crate::util::normalize_ident(name.as_str());
            let Some(position) = output_arms.iter().find_map(|outputs| {
                outputs
                    .iter()
                    .position(|output| {
                        output.name_kind == OutputNameKind::ExplicitAlias
                            && crate::util::normalize_ident(&output.name) == name
                    })
                    .or_else(|| {
                        outputs.iter().position(|output| {
                            !output.name.is_empty()
                                && crate::util::normalize_ident(&output.name) == name
                        })
                    })
            }) else {
                return compound_order_by_no_match(term_number);
            };
            position
        }
        _ => return compound_order_by_no_match(term_number),
    };
    Ok((position, collations))
}

fn compound_order_by_no_match<T>(term_number: usize) -> Result<T> {
    crate::bail_parse_error!(
        "{} ORDER BY term does not match any column in the result set",
        ordinal(term_number)
    )
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

fn expression_contains_aggregate(expression: &Expr) -> bool {
    let mut found = false;
    expression.walk(&mut |expression| {
        if let Expr::Function(call) = expression {
            found |= matches!(call.evaluation, FunctionEvaluation::Aggregate { .. });
        }
    });
    found
}

enum OutputOrdinal {
    Index(usize),
    OutOfRange,
}

fn output_ordinal(expression: &ast::Expr) -> Option<OutputOrdinal> {
    match expression {
        ast::Expr::Collate(inner, _) => output_ordinal(inner),
        ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
            output_ordinal(&expressions[0])
        }
        ast::Expr::Literal(ast::Literal::Numeric(value)) => parsed_output_ordinal(value),
        ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => match inner.as_ref() {
            ast::Expr::Literal(ast::Literal::Numeric(value)) => parsed_output_ordinal(value),
            _ => None,
        },
        ast::Expr::Unary(ast::UnaryOperator::Negative, inner) => match inner.as_ref() {
            ast::Expr::Literal(ast::Literal::Numeric(value)) if value.parse::<i32>().is_ok() => {
                Some(OutputOrdinal::OutOfRange)
            }
            _ => None,
        },
        _ => None,
    }
}

fn parsed_output_ordinal(value: &str) -> Option<OutputOrdinal> {
    value.parse::<i32>().ok().map(|value| {
        if value > 0 {
            OutputOrdinal::Index(value as usize)
        } else {
            OutputOrdinal::OutOfRange
        }
    })
}

pub(super) fn literal_type_fact(literal: &ast::Literal) -> Result<TypeFact> {
    let storage = match literal {
        ast::Literal::Numeric(value) => match parse_numeric_literal(value)? {
            Value::Numeric(Numeric::Integer(_)) => Type::Integer,
            Value::Numeric(Numeric::Float(_)) => Type::Real,
            _ => unreachable!("numeric literal parser returned a non-numeric value"),
        },
        ast::Literal::String(_)
        | ast::Literal::CurrentDate
        | ast::Literal::CurrentTime
        | ast::Literal::CurrentTimestamp => Type::Text,
        ast::Literal::Blob(_) => Type::Blob,
        ast::Literal::Null => Type::Null,
        ast::Literal::True | ast::Literal::False => Type::Integer,
        ast::Literal::Keyword(keyword) => {
            return Err(LimboError::ParseError(format!(
                "unresolved keyword literal: {keyword}"
            )));
        }
    };
    Ok(TypeFact::known(storage))
}

pub(super) fn unsupported_select<T>() -> Result<T> {
    Err(LimboError::ParseError(
        "semantic analysis accepts source-free literal SELECT statements".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use turso_parser::{ast, parser::Parser};

    use crate::{
        dialect::SqliteDialect,
        function::ScalarFunc,
        schema::{BTreeTable, Index, Schema, Sequence, Trigger, Type},
        sync::Arc,
        Func, SymbolTable, MAIN_DB_ID, TEMP_DB_ID,
    };

    use super::*;
    use crate::translate::semantic::{
        context::{DoubleQuotedDml, SemanticDatabase},
        hir,
        hir::{
            BinaryOperand, ColumnReadExpression, CteBody, CustomTypeOperation, FieldAccessKind,
            FunctionArguments, FunctionEvaluation, FunctionOperation, HirRoot, IndexCoverage,
            InsertSource, JoinConstraint, JoinKind, MergedColumnValue, OutputNameKind,
            PseudoSource, ResolvedDefault, SourceKind, SourceOwner, SubqueryExpr, TargetColumn,
        },
        TriggerAnalysis,
    };

    fn parse_statement(sql: &str) -> ast::Stmt {
        let command = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains a statement");
        let ast::Cmd::Stmt(statement) = command else {
            panic!("SQL contains a statement");
        };
        statement
    }

    fn analyze_sql(sql: &str) -> Result<HirDocument> {
        let schema = Schema::new();
        analyze_sql_with_schema(&schema, sql)
    }

    fn analyze_sql_with_schema(schema: &Schema, sql: &str) -> Result<HirDocument> {
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        let statement = parse_statement(sql);
        analyze(&context, AnalyzeInput::Statement(&statement))
    }

    fn analyze_sql_with_databases(
        databases: Vec<SemanticDatabase<'_>>,
        search_path: Vec<DatabaseId>,
        sql: &str,
    ) -> Result<HirDocument> {
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_databases(
            databases,
            search_path,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        )?;
        let statement = parse_statement(sql);
        analyze(&context, AnalyzeInput::Statement(&statement))
    }

    fn analyze_trigger_predicate_with_schema(
        schema: &Schema,
        event: ast::TriggerEvent,
        expression: &ast::Expr,
    ) -> Result<HirDocument> {
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        analyze(
            &context,
            AnalyzeInput::TriggerPredicate {
                context: TriggerAnalysis {
                    database: DatabaseId::new(MAIN_DB_ID),
                    table: schema.get_table("writable").expect("writable table exists"),
                    event,
                },
                expression,
            },
        )
    }

    fn analyze_trigger_select_with_schema(
        schema: &Schema,
        event: ast::TriggerEvent,
        select: &ast::Select,
    ) -> Result<HirDocument> {
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        analyze(
            &context,
            AnalyzeInput::TriggerSelect {
                context: TriggerAnalysis {
                    database: DatabaseId::new(MAIN_DB_ID),
                    table: schema.get_table("writable").expect("writable table exists"),
                    event,
                },
                select,
            },
        )
    }

    fn analyze_trigger_insert_with_schema(
        schema: &Schema,
        event: ast::TriggerEvent,
        command: &ast::TriggerCmd,
        conflict_override: Option<ast::ResolveType>,
    ) -> Result<HirDocument> {
        let ast::TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            col_names,
            select,
            upsert,
            returning,
        } = command
        else {
            panic!("trigger command is INSERT");
        };
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        analyze(
            &context,
            AnalyzeInput::TriggerInsert {
                context: TriggerAnalysis {
                    database: DatabaseId::new(MAIN_DB_ID),
                    table: schema.get_table("writable").expect("writable table exists"),
                    event,
                },
                insert: crate::translate::semantic::TriggerInsert {
                    command_conflict: *or_conflict,
                    conflict_override,
                    table: tbl_name,
                    columns: col_names,
                    select,
                    upsert: upsert.as_deref(),
                    returning,
                },
            },
        )
    }

    fn analyze_trigger_update_with_schema(
        schema: &Schema,
        event: ast::TriggerEvent,
        command: &ast::TriggerCmd,
        conflict_override: Option<ast::ResolveType>,
    ) -> Result<HirDocument> {
        let ast::TriggerCmd::Update {
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause,
        } = command
        else {
            panic!("trigger command is UPDATE");
        };
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        analyze(
            &context,
            AnalyzeInput::TriggerUpdate {
                context: TriggerAnalysis {
                    database: DatabaseId::new(MAIN_DB_ID),
                    table: schema.get_table("writable").expect("writable table exists"),
                    event,
                },
                update: crate::translate::semantic::TriggerUpdate {
                    command_conflict: *or_conflict,
                    conflict_override,
                    table: tbl_name,
                    assignments: sets,
                    from: from.as_ref(),
                    predicate: where_clause.as_deref(),
                },
            },
        )
    }

    fn analyze_trigger_delete_with_schema(
        schema: &Schema,
        event: ast::TriggerEvent,
        command: &ast::TriggerCmd,
    ) -> Result<HirDocument> {
        let ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } = command
        else {
            panic!("trigger command is DELETE");
        };
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        analyze(
            &context,
            AnalyzeInput::TriggerDelete {
                context: TriggerAnalysis {
                    database: DatabaseId::new(MAIN_DB_ID),
                    table: schema.get_table("writable").expect("writable table exists"),
                    event,
                },
                delete: crate::translate::semantic::TriggerDelete {
                    table: tbl_name,
                    predicate: where_clause.as_deref(),
                },
            },
        )
    }

    fn first_trigger_command(sql: &str) -> ast::TriggerCmd {
        let ast::Stmt::CreateTrigger { mut commands, .. } = parse_statement(sql) else {
            panic!("statement is CREATE TRIGGER");
        };
        assert_eq!(commands.len(), 1);
        commands.pop().expect("trigger contains one command")
    }

    fn analyze_trigger_program_with_schema(
        schema: &Schema,
        sql: &str,
        conflict_override: Option<ast::ResolveType>,
    ) -> Result<HirDocument> {
        let ast::Stmt::CreateTrigger {
            event,
            when_clause,
            commands,
            ..
        } = parse_statement(sql)
        else {
            panic!("statement is CREATE TRIGGER");
        };
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        analyze(
            &context,
            AnalyzeInput::TriggerProgram(
                crate::translate::semantic::TriggerProgramInput {
                    context: TriggerAnalysis {
                        database: DatabaseId::new(MAIN_DB_ID),
                        table: schema.get_table("writable").expect("writable table exists"),
                        event,
                    },
                    predicate: when_clause.as_deref(),
                    commands: &commands,
                    conflict_override,
                },
            ),
        )
    }

    fn trigger_root(document: &HirDocument) -> &hir::TriggerRoot {
        let HirRoot::Trigger(root) = &document.root else {
            panic!("document has trigger root");
        };
        root
    }

    fn trigger_column(row: &str, column: &str) -> ast::Expr {
        ast::Expr::Qualified(
            ast::Name::exact(row.to_string()),
            ast::Name::exact(column.to_string()),
        )
    }

    fn first_result_expression(sql: &str) -> ast::Expr {
        let ast::Stmt::Select(select) = parse_statement(sql) else {
            panic!("statement is SELECT");
        };
        let ast::OneSelect::Select { columns, .. } = select.body.select else {
            panic!("SELECT has projection body");
        };
        let ast::ResultColumn::Expr(expression, _) = &columns[0] else {
            panic!("first result is an expression");
        };
        expression.as_ref().clone()
    }

    fn schema_with_items() -> Schema {
        let mut schema = Schema::new();
        let table = Arc::new(
            BTreeTable::from_sql(
                "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT COLLATE NOCASE, score REAL)",
                2,
            )
            .expect("fixed table schema parses"),
        );
        schema
            .add_btree_table(table)
            .expect("fixed table name is unique");
        schema
    }

    fn schema_with_join_tables() -> Schema {
        let mut schema = schema_with_items();
        for (sql, root_page) in [
            ("CREATE TABLE categories(id INTEGER, label TEXT)", 3),
            ("CREATE TABLE tags(item_id INTEGER, note TEXT)", 4),
            (
                "CREATE TABLE extras(item_id INTEGER, flag INTEGER, id INTEGER)",
                5,
            ),
            ("CREATE TABLE codes(value INTEGER, label TEXT)", 6),
        ] {
            let table = Arc::new(
                BTreeTable::from_sql(sql, root_page).expect("fixed join table schema parses"),
            );
            schema
                .add_btree_table(table)
                .expect("fixed join table name is unique");
        }
        schema
    }

    fn schema_with_indexes() -> Schema {
        let mut schema = schema_with_join_tables();
        let symbols = SymbolTable::new();
        for (table_name, sql, root_page) in [
            ("items", "CREATE INDEX idx_items_value ON items(value)", 7),
            (
                "categories",
                "CREATE INDEX idx_categories_label ON categories(label)",
                8,
            ),
        ] {
            let table = schema
                .get_btree_table(table_name)
                .expect("indexed table exists");
            let index = Index::from_sql(&symbols, sql, root_page, &table)
                .expect("fixed index schema parses");
            schema
                .add_index(Arc::new(index))
                .expect("fixed index name is unique");
        }
        schema
    }

    fn schema_with_custom_columns() -> Schema {
        let mut schema = Schema::new();
        for sql in [
            "CREATE TYPE scaled(value INTEGER, factor INTEGER) BASE INTEGER \
             ENCODE value * factor DECODE value / factor",
            "CREATE TYPE shifted(value INTEGER) BASE INTEGER \
             ENCODE value + 1 DECODE value - 1",
            "CREATE DOMAIN wrapped AS shifted NOT NULL",
        ] {
            schema
                .add_type_from_sql(sql)
                .expect("custom type definition parses");
        }
        let table = BTreeTable::from_sql(
            "CREATE TABLE typed_values(\
                scalar scaled(4), nested wrapped, array_values scaled(5)[]\
             ) STRICT",
            2,
        )
        .expect("custom column table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("custom column table name is unique");
        schema
            .resolve_all_custom_type_affinities()
            .expect("custom column affinities resolve");
        schema
    }

    fn schema_with_custom_operators() -> Schema {
        let mut schema = Schema::new();
        for sql in [
            "CREATE TYPE amount(value INTEGER, factor INTEGER) BASE INTEGER \
             ENCODE value * factor DECODE value / factor \
             OPERATOR '+' numeric_add OPERATOR '<' numeric_lt OPERATOR '=' numeric_eq",
            "CREATE TYPE alternate(value INTEGER) BASE INTEGER OPERATOR '+' numeric_add",
            "CREATE TYPE ordered(value INTEGER) BASE INTEGER OPERATOR '<'",
        ] {
            schema
                .add_type_from_sql(sql)
                .expect("custom operator type parses");
        }
        let table = BTreeTable::from_sql(
            "CREATE TABLE custom_values(\
                a amount(4), b amount(4), c alternate, d ordered, e ordered\
             ) STRICT",
            2,
        )
        .expect("custom operator table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("custom operator table name is unique");
        schema
            .resolve_all_custom_type_affinities()
            .expect("custom operator column affinities resolve");
        schema
    }

    fn schema_with_writable_table() -> Schema {
        let mut schema = Schema::new();
        let table = BTreeTable::from_sql(
            "CREATE TABLE writable(\
                id INTEGER PRIMARY KEY,\
                value TEXT DEFAULT 'fallback',\
                doubled INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL\
             ) STRICT",
            2,
        )
        .expect("writable table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("writable table name is unique");
        schema
    }

    fn schema_with_insert_metadata() -> Schema {
        let mut schema = Schema::new();
        let table = Arc::new(
            BTreeTable::from_sql(
                "CREATE TABLE guarded(\
                    id INTEGER PRIMARY KEY,\
                    value TEXT,\
                    score INTEGER,\
                    CONSTRAINT positive_score CHECK (score > 0),\
                    CHECK (length(value) > 0)\
                 )",
                2,
            )
            .expect("guarded table parses"),
        );
        schema
            .add_btree_table(table.clone())
            .expect("guarded table name is unique");
        let symbols = SymbolTable::new();
        for (sql, root_page) in [
            ("CREATE UNIQUE INDEX guarded_value ON guarded(value)", 3),
            (
                "CREATE UNIQUE INDEX guarded_expression ON guarded(lower(value)) WHERE score > 1",
                4,
            ),
        ] {
            let index = Index::from_sql(&symbols, sql, root_page, &table)
                .expect("guarded index schema parses");
            schema
                .add_index(Arc::new(index))
                .expect("guarded index name is unique");
        }
        schema
    }

    fn schema_with_insert_source() -> Schema {
        let mut schema = schema_with_writable_table();
        let table = BTreeTable::from_sql("CREATE TABLE insert_source(id INTEGER, value TEXT)", 3)
            .expect("INSERT source table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("INSERT source table name is unique");
        schema
    }

    fn schema_with_insert_triggers() -> Schema {
        let mut schema = schema_with_writable_table();
        for (name, time, event, target_database_id) in [
            (
                "insert_before",
                ast::TriggerTime::Before,
                ast::TriggerEvent::Insert,
                None,
            ),
            (
                "insert_after",
                ast::TriggerTime::After,
                ast::TriggerEvent::Insert,
                None,
            ),
            (
                "update_all",
                ast::TriggerTime::Before,
                ast::TriggerEvent::Update,
                None,
            ),
            (
                "update_value",
                ast::TriggerTime::After,
                ast::TriggerEvent::UpdateOf(vec![ast::Name::exact("value".to_string())]),
                None,
            ),
            (
                "update_id",
                ast::TriggerTime::After,
                ast::TriggerEvent::UpdateOf(vec![ast::Name::exact("id".to_string())]),
                None,
            ),
            (
                "update_missing",
                ast::TriggerTime::After,
                ast::TriggerEvent::UpdateOf(vec![ast::Name::exact("missing".to_string())]),
                None,
            ),
            (
                "delete_only",
                ast::TriggerTime::Before,
                ast::TriggerEvent::Delete,
                None,
            ),
            (
                "delete_other_database",
                ast::TriggerTime::After,
                ast::TriggerEvent::Delete,
                Some(99),
            ),
            (
                "other_database",
                ast::TriggerTime::Before,
                ast::TriggerEvent::Insert,
                Some(99),
            ),
        ] {
            schema
                .add_trigger(
                    Trigger::new(
                        name.to_string(),
                        String::new(),
                        "writable".to_string(),
                        Some(time),
                        event,
                        true,
                        None,
                        Vec::new(),
                        false,
                        target_database_id,
                    ),
                    "writable",
                )
                .expect("fixed trigger is added");
        }
        schema
    }

    fn schema_with_insert_foreign_keys() -> Schema {
        let mut schema = Schema::new();
        let parent = Arc::new(
            BTreeTable::from_sql("CREATE TABLE parents(id INTEGER PRIMARY KEY, code TEXT)", 2)
                .expect("foreign-key parent table parses"),
        );
        schema
            .add_btree_table(parent.clone())
            .expect("foreign-key parent table name is unique");
        let symbols = SymbolTable::new();
        let parent_code = Index::from_sql(
            &symbols,
            "CREATE UNIQUE INDEX parents_code ON parents(code)",
            3,
            &parent,
        )
        .expect("foreign-key parent index parses");
        schema
            .add_index(Arc::new(parent_code))
            .expect("foreign-key parent index name is unique");

        for (sql, root_page) in [
            (
                "CREATE TABLE fk_items(\
                    id INTEGER PRIMARY KEY,\
                    parent_id INTEGER REFERENCES parents(id),\
                    parent_code TEXT REFERENCES parents(code)\
                 )",
                4,
            ),
            (
                "CREATE TABLE item_notes(\
                    item_id INTEGER,\
                    parent_key INTEGER GENERATED ALWAYS AS (item_id + 0),\
                    note TEXT,\
                    FOREIGN KEY(parent_key) REFERENCES fk_items(id)\
                 )",
                5,
            ),
        ] {
            let table =
                BTreeTable::from_sql(sql, root_page).expect("foreign-key child table parses");
            schema
                .add_btree_table(Arc::new(table))
                .expect("foreign-key child table name is unique");
        }
        schema
    }

    fn schema_with_virtual_insert_target() -> Schema {
        let mut schema = Schema::new();
        crate::dialect::sqlite::register_builtin_catalog(&mut schema, false)
            .expect("SQLite virtual catalog registers");
        schema
    }

    fn schema_with_insert_unions() -> Schema {
        let mut schema = Schema::new();
        for sql in [
            "CREATE TYPE inner_u AS UNION(a INTEGER, b TEXT)",
            "CREATE TYPE outer_u AS UNION(x inner_u, y REAL)",
            "CREATE TYPE color_u AS UNION(red TEXT, blue TEXT)",
        ] {
            schema
                .add_type_from_sql(sql)
                .expect("INSERT union type parses");
        }
        let table = BTreeTable::from_sql(
            "CREATE TABLE union_values(\
                id INTEGER PRIMARY KEY, nested outer_u, color color_u\
             ) STRICT",
            2,
        )
        .expect("INSERT union table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("INSERT union table name is unique");
        schema
            .resolve_all_custom_type_affinities()
            .expect("INSERT union affinities resolve");
        schema
    }

    fn schema_with_array_columns() -> Schema {
        let mut schema = Schema::new();
        let table = BTreeTable::from_sql(
            "CREATE TABLE arrays(\
                vals INTEGER[], matrix TEXT[][], anything ANY[]\
             ) STRICT",
            2,
        )
        .expect("array table schema parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("array table name is unique");
        schema
    }

    fn schema_with_struct_and_union_columns() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE point AS STRUCT(x INT, label TEXT)")
            .expect("struct type parses");
        schema
            .add_type_from_sql("CREATE TYPE shape AS UNION(point point, label TEXT)")
            .expect("union type parses");
        let table = BTreeTable::from_sql(
            "CREATE TABLE custom_values(\
                id INTEGER, point_value point, shape_value shape\
             ) STRICT",
            2,
        )
        .expect("custom field table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("custom field table name is unique");
        schema
            .resolve_all_custom_type_affinities()
            .expect("custom field affinities resolve");
        schema
    }

    fn schema_with_stored_column_expressions() -> Schema {
        let mut schema = Schema::new();
        let table = BTreeTable::from_sql(
            "CREATE TABLE calculated(\
                base INTEGER DEFAULT 7,\
                doubled INTEGER GENERATED ALWAYS AS (base * 2) VIRTUAL,\
                combined INTEGER GENERATED ALWAYS AS (doubled + base) VIRTUAL,\
                unused INTEGER DEFAULT 11\
             )",
            2,
        )
        .expect("stored column expressions parse");
        schema
            .add_btree_table(Arc::new(table))
            .expect("stored-expression table name is unique");
        schema
    }

    fn schema_with_sequence(name: &str) -> Schema {
        let mut schema = Schema::new();
        let normalized = crate::util::normalize_ident(name);
        let sequence = Sequence::new(normalized.clone(), None, None, None, None, false)
            .expect("fixed sequence descriptor is valid");
        schema
            .sequences
            .insert(normalized.clone(), Arc::new(sequence));
        let table = BTreeTable::from_sql(
            &crate::translate::sequence::sequence_backing_table_sql(&normalized),
            2,
        )
        .expect("sequence backing table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("sequence backing table name is unique");
        schema
    }

    fn schema_with_autoincrement(include_mvcc_sequence: bool) -> Schema {
        let sequence_name = crate::schema::autoincrement_sequence_name("auto_items");
        let mut schema = if include_mvcc_sequence {
            schema_with_sequence(&sequence_name)
        } else {
            Schema::new()
        };
        for (sql, root_page) in [
            (
                "CREATE TABLE auto_items(\
                    id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT\
                 )",
                3,
            ),
            ("CREATE TABLE sqlite_sequence(name, seq)", 4),
        ] {
            schema
                .add_btree_table(Arc::new(
                    BTreeTable::from_sql(sql, root_page).expect("AUTOINCREMENT schema parses"),
                ))
                .expect("AUTOINCREMENT schema names are unique");
        }
        schema
    }

    #[test]
    fn source_free_literals_become_closed_hir() {
        let document = analyze_sql("SELECT 1, 1.5 AS real_value, 'text', NULL, TRUE")
            .expect("literal SELECT has valid SQL meaning");
        document.validate().expect("analyzer returns closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert!(block.from.is_none());
        assert_eq!(block.outputs.len(), 5);
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Integer));
        assert_eq!(block.outputs[1].type_fact.storage, Some(Type::Real));
        assert_eq!(block.outputs[1].name, "real_value");
        assert_eq!(block.outputs[1].name_kind, OutputNameKind::ExplicitAlias);
        assert_eq!(block.outputs[2].type_fact.storage, Some(Type::Text));
        assert_eq!(block.outputs[3].type_fact.storage, Some(Type::Null));
        assert_eq!(block.outputs[4].type_fact.storage, Some(Type::Integer));
        assert!(block.outputs.iter().all(|output| !output.has_affinity));
    }

    #[test]
    fn parameters_keep_parser_assigned_indexes_and_names() {
        let document =
            analyze_sql("SELECT ?, ?3, :named, :named, @other").expect("parameters bind into HIR");
        document
            .validate()
            .expect("parameter expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        let expected = [
            (1, None),
            (3, None),
            (4, Some(":named")),
            (4, Some(":named")),
            (5, Some("@other")),
        ];

        for (output, (index, name)) in outputs.iter().zip(expected) {
            let Expr::Parameter(parameter) = &output.expr else {
                panic!("parser variable becomes HIR parameter");
            };
            assert_eq!(parameter.index.get(), index);
            assert_eq!(parameter.name.as_deref(), name);
            assert!(parameter.type_fact.storage.is_none());
            assert!(parameter.type_fact.declared.is_none());
            assert!(!output.has_affinity);
            assert!(output.collation.is_none());
            assert!(!output.collation_is_explicit);
        }
    }

    #[test]
    fn unresolved_names_do_not_enter_hir() {
        let error = analyze_sql("SELECT missing_name")
            .expect_err("unresolved name must fail semantic analysis");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: missing_name"
        );
    }

    #[test]
    fn unresolved_double_quoted_names_follow_dqs_setting() {
        let document = analyze_sql("SELECT \"missing name\"")
            .expect("enabled DQS converts unresolved name to text");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        assert!(matches!(
            &output.expr,
            Expr::Literal(ast::Literal::String(value)) if value == "'missing name'"
        ));
        assert_eq!(output.type_fact.storage, Some(Type::Text));

        let schema = Schema::new();
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            &schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        )
        .with_dqs_dml(DoubleQuotedDml::Disabled);
        let statement = parse_statement("SELECT \"missing name\"");
        let error = analyze(&context, AnalyzeInput::Statement(&statement))
            .expect_err("disabled DQS keeps unresolved name error");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: missing name"
        );
    }

    #[test]
    fn plain_table_select_becomes_closed_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value, items.score, main.items.id, rowid FROM items",
        )
        .expect("plain table SELECT has valid SQL meaning");
        document.validate().expect("analyzer returns closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        let block = &query.blocks[0];
        let from = block.from.as_ref().expect("FROM is preserved");
        let source = document.source(from.first).expect("source exists");
        assert_eq!(source.owner, SourceOwner::QueryBlock(block.id));
        assert_eq!(source.name, "items");
        assert!(matches!(&source.kind, SourceKind::Table(_)));
        assert_eq!(source.columns.len(), 3);

        assert!(matches!(
            &block.outputs[0].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 1
        ));
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Text));
        assert_eq!(
            block.outputs[0].affinity,
            crate::vdbe::affinity::Affinity::Text
        );
        assert!(block.outputs[0].has_affinity);
        assert_eq!(
            block.outputs[0]
                .collation
                .as_ref()
                .expect("declared collation is preserved")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(!block.outputs[0].collation_is_explicit);
        assert!(matches!(
            &block.outputs[1].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 2
        ));
        assert!(matches!(
            &block.outputs[2].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 0
        ));
        assert!(matches!(&block.outputs[3].expr, Expr::RowId(id) if *id == source.id));
        assert_eq!(block.outputs[3].type_fact.storage, Some(Type::Integer));
    }

    #[test]
    fn referenced_stored_column_expressions_are_planned_transitively() {
        let schema = schema_with_stored_column_expressions();
        let document = analyze_sql_with_schema(&schema, "SELECT combined FROM calculated")
            .expect("generated-column dependencies bind");
        document
            .validate()
            .expect("stored expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = document
            .source(block.from.as_ref().expect("query has FROM").first)
            .expect("source exists");

        let ColumnReadExpression::Planned(base_default) = &source.default_expressions[0] else {
            panic!("transitive base dependency plans its read-time default");
        };
        assert!(matches!(
            base_default,
            Expr::Literal(ast::Literal::Numeric(value)) if value == "7"
        ));

        let ColumnReadExpression::Planned(doubled) = &source.generated_expressions[1] else {
            panic!("generated dependency is planned");
        };
        assert!(matches!(
            doubled,
            Expr::Binary { lhs, .. }
                if matches!(lhs.as_ref(), Expr::Column(column)
                    if column.source == source.id && column.column == 0)
        ));

        let ColumnReadExpression::Planned(combined) = &source.generated_expressions[2] else {
            panic!("directly read generated column is planned");
        };
        assert!(matches!(
            combined,
            Expr::Binary { lhs, rhs, .. }
                if matches!(lhs.as_ref(), Expr::Column(column)
                    if column.source == source.id && column.column == 1)
                && matches!(rhs.as_ref(), Expr::Column(column)
                    if column.source == source.id && column.column == 0)
        ));
        assert!(matches!(
            &source.default_expressions[3],
            ColumnReadExpression::NotRequired
        ));
    }

    #[test]
    fn unused_stored_column_expressions_remain_not_required() {
        let schema = schema_with_stored_column_expressions();
        let document = analyze_sql_with_schema(&schema, "SELECT 1 FROM calculated")
            .expect("unused stored expressions do not need binding");
        document
            .validate()
            .expect("unused stored expressions remain valid HIR state");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = document
            .source(block.from.as_ref().expect("query has FROM").first)
            .expect("source exists");
        assert!(matches!(
            &source.default_expressions[0],
            ColumnReadExpression::NotRequired
        ));
        assert!(matches!(
            &source.generated_expressions[1],
            ColumnReadExpression::NotRequired
        ));
        assert!(matches!(
            &source.generated_expressions[2],
            ColumnReadExpression::NotRequired
        ));
        assert!(matches!(
            &source.default_expressions[3],
            ColumnReadExpression::NotRequired
        ));
    }

    #[test]
    fn catalog_search_path_and_qualification_choose_database_schema() {
        fn catalog_schema(schema_version: u32, with_index: bool) -> Schema {
            let mut schema = Schema::new();
            schema.schema_version = schema_version;
            let table = Arc::new(
                BTreeTable::from_sql("CREATE TABLE shared(value TEXT)", 2)
                    .expect("catalog table parses"),
            );
            schema
                .add_btree_table(table.clone())
                .expect("catalog table name is unique");
            if with_index {
                let symbols = SymbolTable::new();
                let index = Index::from_sql(
                    &symbols,
                    "CREATE INDEX shared_value ON shared(value)",
                    3,
                    &table,
                )
                .expect("catalog index parses");
                schema
                    .add_index(Arc::new(index))
                    .expect("catalog index name is unique");
            }
            schema
        }

        fn first_source(document: &HirDocument) -> &Source {
            let HirRoot::Query(root) = &document.root else {
                panic!("SELECT produces query root");
            };
            let query = document.query(root.query).expect("query exists");
            let source = query.blocks[0].from.as_ref().expect("query has FROM").first;
            document.source(source).expect("source exists")
        }

        let main = catalog_schema(10, false);
        let temp = catalog_schema(20, false);
        let attached = catalog_schema(30, true);
        let attached_id = DatabaseId::new(2);
        let analyze_catalog = |sql| {
            analyze_sql_with_databases(
                vec![
                    SemanticDatabase::new(DatabaseId::new(MAIN_DB_ID), "main", &main),
                    SemanticDatabase::new(DatabaseId::new(TEMP_DB_ID), "temp", &temp),
                    SemanticDatabase::new(attached_id, "aux", &attached),
                ],
                vec![
                    DatabaseId::new(TEMP_DB_ID),
                    attached_id,
                    DatabaseId::new(MAIN_DB_ID),
                ],
                sql,
            )
        };

        let unqualified = analyze_catalog("SELECT value FROM shared")
            .expect("search path resolves unqualified table");
        assert_eq!(
            first_source(&unqualified).database,
            Some(DatabaseId::new(TEMP_DB_ID))
        );
        assert_eq!(
            unqualified
                .databases
                .iter()
                .map(|snapshot| (snapshot.database.index(), snapshot.schema_version))
                .collect::<Vec<_>>(),
            [(MAIN_DB_ID, 10), (TEMP_DB_ID, 20), (2, 30)]
        );

        let qualified = analyze_catalog("SELECT value FROM aux.shared INDEXED BY shared_value")
            .expect("qualified attached table and index resolve together");
        let source = first_source(&qualified);
        assert_eq!(source.database, Some(attached_id));
        let hir::IndexHint::Indexed(index) = &source.index_hint else {
            panic!("attached index hint is resolved");
        };
        assert_eq!(index.database(), Some(attached_id));

        let main_qualified =
            analyze_catalog("SELECT value FROM main.shared").expect("main qualifier resolves");
        assert_eq!(
            first_source(&main_qualified).database,
            Some(DatabaseId::new(MAIN_DB_ID))
        );

        let insert = analyze_catalog("INSERT INTO aux.shared(value) VALUES ('kept')")
            .expect("qualified attached INSERT resolves metadata in attached schema");
        let HirRoot::Insert(insert_root) = &insert.root else {
            panic!("INSERT produces INSERT root");
        };
        let target = insert
            .source(insert_root.target)
            .expect("INSERT target source exists");
        assert_eq!(target.database, Some(attached_id));
        assert!(matches!(
            &target.index_coverage,
            IndexCoverage::Complete { indexes } if indexes.len() == 1
        ));
    }

    #[test]
    fn table_index_hints_keep_resolved_index_identity() {
        let schema = schema_with_indexes();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value FROM items INDEXED BY IDX_ITEMS_VALUE",
        )
        .expect("existing table index resolves");
        document.validate().expect("index hint produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = document
            .source(block.from.as_ref().expect("query has FROM").first)
            .expect("source exists");
        let SourceKind::Table(table) = &source.kind else {
            panic!("indexed source remains a table");
        };
        let crate::translate::semantic::hir::IndexHint::Indexed(index) = &source.index_hint else {
            panic!("INDEXED BY keeps a resolved index");
        };
        assert_eq!(index.value().name, "idx_items_value");
        assert_eq!(index.value().table_name, "items");
        assert_eq!(index.database(), source.database);
        assert_eq!(index.snapshot(), table.snapshot());
        assert_ne!(index.id(), table.id());

        let document = analyze_sql_with_schema(&schema, "SELECT value FROM items NOT INDEXED")
            .expect("NOT INDEXED binds");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = document
            .source(block.from.as_ref().expect("query has FROM").first)
            .expect("source exists");
        assert!(matches!(
            source.index_hint,
            crate::translate::semantic::hir::IndexHint::NotIndexed
        ));
    }

    #[test]
    fn select_freezes_all_index_expressions() {
        let schema = schema_with_insert_metadata();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value FROM guarded WHERE lower(value) = 'kept' AND score > 1",
        )
        .expect("indexed SELECT binds");
        document
            .validate()
            .expect("SELECT index metadata produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = document
            .source(block.from.as_ref().expect("query has FROM").first)
            .expect("source exists");
        assert_eq!(source.index_expressions.len(), 2);
        let expression_index = source
            .index_expressions
            .iter()
            .find(|metadata| metadata.index.value().name == "guarded_expression")
            .expect("expression index metadata exists");
        assert!(matches!(expression_index.columns.as_slice(), [Some(_)]));
        assert!(expression_index.predicate.is_some());
        assert!(expression_index
            .columns
            .iter()
            .flatten()
            .chain(expression_index.predicate.iter())
            .all(|expression| {
                let mut reads_only_source = true;
                expression.walk(&mut |part| {
                    if let Expr::Column(column) = part {
                        reads_only_source &= column.source == source.id;
                    }
                });
                reads_only_source
            }));

        let ordinary_index = source
            .index_expressions
            .iter()
            .find(|metadata| metadata.index.value().name == "guarded_value")
            .expect("ordinary index metadata exists");
        assert!(matches!(ordinary_index.columns.as_slice(), [None]));
        assert!(ordinary_index.predicate.is_none());
        assert!(matches!(
            &source.index_coverage,
            IndexCoverage::Complete { indexes } if indexes
                == &source
                    .index_expressions
                    .iter()
                    .map(|metadata| metadata.index.id())
                    .collect::<Vec<_>>()
        ));
    }

    #[test]
    fn indexed_by_rejects_missing_or_other_table_indexes() {
        let schema = schema_with_indexes();
        for (sql, expected_name) in [
            ("SELECT value FROM items INDEXED BY Missing", "Missing"),
            (
                "SELECT value FROM items INDEXED BY idx_categories_label",
                "idx_categories_label",
            ),
            (
                "WITH candidates AS (SELECT value FROM items) \
                 SELECT * FROM candidates INDEXED BY idx_items_value",
                "idx_items_value",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("INDEXED BY requires an index on the named table");
            assert_eq!(
                error.to_string(),
                format!("Parse error: no such index: {expected_name}")
            );
        }
    }

    #[test]
    fn derived_from_sources_keep_query_ownership_and_output_facts() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT derived.item_text, derived.item_key \
             FROM (\
                 SELECT value AS item_text, id AS item_key FROM items \
                 UNION ALL \
                 SELECT label, id FROM categories\
             ) AS derived",
        )
        .expect("derived FROM source binds into HIR");
        document
            .validate()
            .expect("derived source produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_block = &outer.blocks[0];
        let source = document
            .source(
                outer_block
                    .from
                    .as_ref()
                    .expect("outer query has FROM")
                    .first,
            )
            .expect("derived source exists");
        let SourceKind::Derived(inner_id) = source.kind else {
            panic!("FROM subquery becomes derived source");
        };
        let inner = document.query(inner_id).expect("inner query exists");

        assert_eq!(inner.parent, Some(outer.id));
        assert!(inner.captures.is_empty());
        assert_eq!(inner.blocks.len(), 2);
        assert_eq!(source.owner, SourceOwner::QueryBlock(outer_block.id));
        assert_eq!(source.name, "(subquery-0)");
        assert_eq!(source.alias.as_deref(), Some("derived"));
        assert!(!source.rowid_available);
        assert_eq!(source.columns.len(), 2);
        assert_eq!(source.columns[0].name, "item_text");
        assert_eq!(source.columns[0].type_fact.storage, Some(Type::Text));
        assert_eq!(
            source.columns[0]
                .collation
                .as_ref()
                .expect("first compound arm supplies source collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert_eq!(source.columns[1].name, "item_key");
        assert!(matches!(
            outer_block.outputs[0].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 0
        ));
        assert!(matches!(
            outer_block.outputs[1].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 1
        ));
    }

    #[test]
    fn derived_sources_keep_positions_and_do_not_see_outer_scope() {
        let document = analyze_sql(
            "SELECT * FROM (SELECT 1 AS one) AS left_side \
             JOIN (SELECT 2 AS two) AS right_side",
        )
        .expect("multiple derived sources bind into HIR");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let from = outer.blocks[0].from.as_ref().expect("outer query has FROM");
        let sources = [
            document.source(from.first).expect("left source exists"),
            document
                .source(from.joins[0].right)
                .expect("right source exists"),
        ];
        assert_eq!(sources[0].name, "(subquery-0)");
        assert_eq!(sources[1].name, "(subquery-1)");
        for source in sources {
            let SourceKind::Derived(query) = source.kind else {
                panic!("FROM subquery becomes derived source");
            };
            assert_eq!(
                document.query(query).expect("derived query exists").parent,
                Some(outer.id)
            );
        }
        assert_eq!(outer.blocks[0].outputs.len(), 2);

        let schema = schema_with_items();
        let error = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items AS outer_items \
             JOIN (SELECT outer_items.id AS captured) AS derived",
        )
        .expect_err("FROM subquery cannot capture the outer source scope");
        assert_eq!(error.to_string(), "Parse error: no such table: outer_items");
    }

    #[test]
    fn expression_subqueries_capture_outer_sources_and_keep_result_facts() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT (SELECT outer_items.value), \
                    EXISTS (SELECT outer_items.id) \
             FROM items AS outer_items",
        )
        .expect("correlated expression subqueries bind into HIR");
        document
            .validate()
            .expect("correlated subqueries produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_block = &outer.blocks[0];
        let outer_source = outer_block
            .from
            .as_ref()
            .expect("outer query has FROM")
            .first;

        let Expr::Subquery(SubqueryExpr::Scalar {
            query: scalar_id,
            output,
        }) = &outer_block.outputs[0].expr
        else {
            panic!("scalar subquery remains explicit in HIR");
        };
        assert_eq!(*output, 0);
        let scalar = document.query(*scalar_id).expect("scalar query exists");
        assert_eq!(scalar.parent, Some(outer.id));
        assert_eq!(scalar.captures, vec![outer_source]);
        assert!(matches!(
            scalar.blocks[0].outputs[0].expr,
            Expr::Column(reference)
                if reference.source == outer_source && reference.column == 1
        ));
        assert_eq!(outer_block.outputs[0].type_fact.storage, Some(Type::Text));
        assert_eq!(
            outer_block.outputs[0].affinity,
            crate::vdbe::affinity::Affinity::Text
        );
        assert!(outer_block.outputs[0].has_affinity);
        assert_eq!(
            outer_block.outputs[0]
                .collation
                .as_ref()
                .expect("scalar output keeps selected collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );

        let Expr::Subquery(SubqueryExpr::Exists(exists_id)) = &outer_block.outputs[1].expr else {
            panic!("EXISTS subquery remains explicit in HIR");
        };
        let exists = document.query(*exists_id).expect("EXISTS query exists");
        assert_eq!(exists.parent, Some(outer.id));
        assert_eq!(exists.captures, vec![outer_source]);
        assert_eq!(
            outer_block.outputs[1].type_fact.storage,
            Some(Type::Integer)
        );
        assert!(!outer_block.outputs[1].has_affinity);
        assert!(outer_block.outputs[1].collation.is_none());
    }

    #[test]
    fn nested_expression_subqueries_capture_any_ancestor_scope() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT (SELECT (SELECT outer_items.id)) \
             FROM items AS outer_items",
        )
        .expect("nested subquery resolves an ancestor source");
        document
            .validate()
            .expect("ancestor capture follows the query parent chain");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_source = outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query has FROM")
            .first;
        let Expr::Subquery(SubqueryExpr::Scalar {
            query: middle_id, ..
        }) = &outer.blocks[0].outputs[0].expr
        else {
            panic!("outer expression contains middle query");
        };
        let middle = document.query(*middle_id).expect("middle query exists");
        assert!(middle.captures.is_empty());
        let Expr::Subquery(SubqueryExpr::Scalar {
            query: inner_id, ..
        }) = &middle.blocks[0].outputs[0].expr
        else {
            panic!("middle expression contains inner query");
        };
        let inner = document.query(*inner_id).expect("inner query exists");
        assert_eq!(inner.parent, Some(middle.id));
        assert_eq!(inner.captures, vec![outer_source]);
    }

    #[test]
    fn expression_subqueries_bind_in_join_constraints() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT outer_items.id \
             FROM items AS outer_items \
             JOIN categories ON EXISTS (SELECT outer_items.id)",
        )
        .expect("JOIN constraint accepts a correlated subquery");
        document
            .validate()
            .expect("JOIN subquery produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let from = outer.blocks[0].from.as_ref().expect("query has FROM");
        let JoinConstraint::On(Expr::Subquery(SubqueryExpr::Exists(inner_id))) =
            &from.joins[0].constraint
        else {
            panic!("JOIN keeps its EXISTS subquery");
        };
        let inner = document.query(*inner_id).expect("inner query exists");
        assert_eq!(inner.parent, Some(outer.id));
        assert_eq!(inner.captures, vec![from.first]);
    }

    #[test]
    fn where_prefers_source_columns_and_falls_back_to_outputs() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value AS score, id + 1 AS next_id \
             FROM items \
             WHERE score > 1 AND next_id > 2",
        )
        .expect("WHERE resolves source columns before output aliases");
        document
            .validate()
            .expect("WHERE output references produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        let block = &query.blocks[0];
        let QueryBlockBody::Select {
            filter: Some(filter),
            ..
        } = &block.body
        else {
            panic!("WHERE becomes the query block filter");
        };
        let source = block.from.as_ref().expect("query has FROM").first;
        let mut columns = Vec::new();
        let mut outputs = Vec::new();
        filter.walk(&mut |expression| match expression {
            Expr::Column(reference) => columns.push(*reference),
            Expr::Output(output) => outputs.push(*output),
            _ => {}
        });
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].source, source);
        assert_eq!(columns[0].column, 2);
        assert_eq!(outputs, vec![OutputId::query(block.id, 1)]);
    }

    #[test]
    fn where_subqueries_capture_outer_sources() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT outer_items.id \
             FROM items AS outer_items \
             WHERE EXISTS (SELECT 1 WHERE outer_items.id > 0)",
        )
        .expect("WHERE accepts a correlated expression subquery");
        document
            .validate()
            .expect("correlated WHERE subquery produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_source = outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query has FROM")
            .first;
        let QueryBlockBody::Select {
            filter: Some(Expr::Subquery(SubqueryExpr::Exists(inner))),
            ..
        } = &outer.blocks[0].body
        else {
            panic!("WHERE keeps its EXISTS subquery");
        };
        let inner = document.query(*inner).expect("inner query exists");
        assert_eq!(inner.parent, Some(outer.id));
        assert_eq!(inner.captures, vec![outer_source]);
        let QueryBlockBody::Select {
            filter: Some(filter),
            ..
        } = &inner.blocks[0].body
        else {
            panic!("inner WHERE becomes a filter");
        };
        assert!(matches!(
            filter,
            Expr::Binary { lhs, .. }
                if matches!(lhs.as_ref(), Expr::Column(reference)
                    if reference.source == outer_source && reference.column == 0)
        ));
    }

    #[test]
    fn where_rejects_aggregate_and_window_functions() {
        let schema = schema_with_items();
        for (sql, message) in [
            (
                "SELECT id FROM items WHERE sum(score) > 1",
                "Parse error: misuse of aggregate function sum()",
            ),
            (
                "SELECT id FROM items WHERE row_number() OVER () > 1",
                "Parse error: misuse of window function: row_number()",
            ),
            (
                "SELECT sum(score) AS total FROM items WHERE total > 1",
                "Parse error: misuse of aggregate: sum()",
            ),
            (
                "SELECT row_number() OVER () AS position FROM items WHERE position > 1",
                "Parse error: misuse of aliased window function position",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("WHERE only accepts scalar functions");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn group_by_keeps_precedence_ordinals_and_key_facts() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value AS score, id + 1 AS next_id \
             FROM items \
             GROUP BY score, next_id, 1",
        )
        .expect("GROUP BY resolves columns, aliases, and ordinals");
        document.validate().expect("GROUP BY produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } = &block.body
        else {
            panic!("GROUP BY becomes block grouping");
        };
        let source = block.from.as_ref().expect("query has FROM").first;
        assert_eq!(grouping.keys.len(), 3);
        assert!(matches!(
            grouping.keys[0],
            Expr::Column(reference) if reference.source == source && reference.column == 2
        ));
        assert!(matches!(
            grouping.keys[1],
            Expr::Output(id) if id == OutputId::query(block.id, 1)
        ));
        assert!(matches!(
            grouping.keys[2],
            Expr::Output(id) if id == OutputId::query(block.id, 0)
        ));
        assert_eq!(grouping.key_type_facts[0].storage, Some(Type::Real));
        assert_eq!(grouping.key_type_facts[2].storage, Some(Type::Text));
        assert!(grouping.key_collations[0].is_none());
        assert_eq!(
            grouping.key_collations[2]
                .as_ref()
                .expect("ordinal keeps output collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
    }

    #[test]
    fn having_prefers_outputs_and_allocates_aggregate_ids() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT sum(id) AS score \
             FROM items \
             GROUP BY value \
             HAVING max(id) > score",
        )
        .expect("HAVING accepts aggregates and output aliases");
        document.validate().expect("HAVING produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 2);
        let QueryBlockBody::Select {
            grouping:
                Some(super::super::hir::Grouping {
                    having:
                        Some(Expr::Binary {
                            lhs,
                            rhs,
                            operator: ast::Operator::Greater,
                            ..
                        }),
                    ..
                }),
            ..
        } = &block.body
        else {
            panic!("HAVING becomes grouping predicate");
        };
        assert!(matches!(
            lhs.as_ref(),
            Expr::Function(call)
                if matches!(call.evaluation, FunctionEvaluation::Aggregate { id, .. }
                    if id.index == 1 && id.block == block.id)
        ));
        assert!(matches!(
            rhs.as_ref(),
            Expr::Output(id) if *id == OutputId::query(block.id, 0)
        ));
    }

    #[test]
    fn grouping_keeps_function_and_ordinal_errors() {
        let schema = schema_with_items();
        for (sql, message) in [
            (
                "SELECT id FROM items GROUP BY sum(score)",
                "Parse error: misuse of aggregate function sum()",
            ),
            (
                "SELECT id FROM items GROUP BY row_number() OVER ()",
                "Parse error: misuse of window function: row_number()",
            ),
            (
                "SELECT sum(id) AS total FROM items GROUP BY total",
                "Parse error: misuse of aggregate: sum()",
            ),
            (
                "SELECT row_number() OVER () AS position FROM items GROUP BY position",
                "Parse error: misuse of aliased window function position",
            ),
            (
                "SELECT id FROM items GROUP BY id HAVING row_number() OVER () > 1",
                "Parse error: misuse of window function: row_number()",
            ),
            (
                "SELECT row_number() OVER () AS position FROM items \
                 GROUP BY value HAVING position > 1",
                "Parse error: misuse of aliased window function position",
            ),
            (
                "SELECT min(id) AS m FROM items \
                 GROUP BY value HAVING max(m + 5) < 10",
                "Parse error: misuse of aliased aggregate m",
            ),
            (
                "SELECT id FROM items GROUP BY 0",
                "Parse error: 1st GROUP BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT id FROM items GROUP BY 2",
                "Parse error: 1st GROUP BY term out of range - should be between 1 and 1",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("grouping expression keeps its clause rules");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn correlated_group_by_does_not_capture_outer_columns() {
        let schema = schema_with_join_tables();
        let error = analyze_sql_with_schema(
            &schema,
            "SELECT (\
                 SELECT categories.id FROM categories GROUP BY outer_items.id\
             ) FROM items AS outer_items",
        )
        .expect_err("GROUP BY cannot capture an outer query column");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: outer_items.id"
        );

        analyze_sql_with_schema(
            &schema,
            "SELECT (\
                 SELECT categories.id \
                 FROM categories \
                 GROUP BY (SELECT categories.id)\
             ) FROM items AS outer_items",
        )
        .expect("a GROUP BY subquery can capture the current query source");
    }

    #[test]
    fn order_by_keeps_alias_precedence_ordinals_and_facts() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value AS score, id + 1 AS next_id \
             FROM items \
             ORDER BY score DESC NULLS LAST, next_id, 1 COLLATE BINARY",
        )
        .expect("ORDER BY resolves aliases and ordinals");
        document.validate().expect("ORDER BY produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        let block = &query.blocks[0];
        assert_eq!(query.order_by.len(), 3);
        assert!(matches!(
            query.order_by[0].expr,
            Expr::Output(id) if id == OutputId::query(block.id, 0)
        ));
        assert_eq!(query.order_by[0].order, ast::SortOrder::Desc);
        assert_eq!(query.order_by[0].nulls, Some(ast::NullsOrder::Last));
        assert_eq!(query.order_by[0].type_fact.storage, Some(Type::Text));
        assert_eq!(
            query.order_by[0]
                .collation
                .as_ref()
                .expect("alias keeps output collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(matches!(
            query.order_by[1].expr,
            Expr::Output(id) if id == OutputId::query(block.id, 1)
        ));
        assert!(matches!(
            &query.order_by[2].expr,
            Expr::Collate { expr, collation }
                if matches!(expr.as_ref(), Expr::Output(id)
                    if *id == OutputId::query(block.id, 0))
                && collation.value() == &crate::translate::collate::CollationSeq::Binary
        ));
        assert_eq!(query.order_by[2].type_fact.storage, Some(Type::Text));
        assert_eq!(
            query.order_by[2]
                .collation
                .as_ref()
                .expect("explicit ordinal collation is frozen")
                .value(),
            &crate::translate::collate::CollationSeq::Binary
        );
    }

    #[test]
    fn order_by_only_functions_get_first_block_identities() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT count(*) FROM items \
             ORDER BY min(score), row_number() OVER (ORDER BY id)",
        )
        .expect("aggregate SELECT accepts ORDER-BY-only aggregate and window functions");
        document
            .validate()
            .expect("ORDER-BY-only functions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        let block = &query.blocks[0];
        assert_eq!(block.aggregate_count, 2);
        assert_eq!(block.window_function_count, 1);
        assert_eq!(block.windows.len(), 1);
        assert!(matches!(
            &query.order_by[0].expr,
            Expr::Function(call)
                if matches!(call.evaluation, FunctionEvaluation::Aggregate { id, .. }
                    if id.block == block.id && id.index == 1)
        ));
        assert!(matches!(
            &query.order_by[1].expr,
            Expr::Function(call)
                if matches!(call.evaluation, FunctionEvaluation::Window { id, .. }
                    if id.block == block.id && id.index == 0)
        ));

        let named = analyze_sql_with_schema(
            &schema,
            "SELECT id FROM items \
             WINDOW ranked AS (ORDER BY score) \
             ORDER BY row_number() OVER ranked",
        )
        .expect("ORDER-BY-only function can use the block's named window");
        let HirRoot::Query(root) = &named.root else {
            panic!("SELECT produces query root");
        };
        let query = named.query(root.query).expect("query exists");
        assert_eq!(query.blocks[0].window_function_count, 1);
        assert_eq!(query.blocks[0].windows.len(), 1);
    }

    #[test]
    fn order_by_subqueries_capture_the_current_query_scope() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT id FROM items AS outer_items \
             ORDER BY (SELECT outer_items.score)",
        )
        .expect("ORDER BY accepts a correlated scalar subquery");
        document
            .validate()
            .expect("correlated ORDER BY produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let source = outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query has FROM")
            .first;
        let Expr::Subquery(SubqueryExpr::Scalar { query: inner, .. }) = &outer.order_by[0].expr
        else {
            panic!("ORDER BY keeps its scalar subquery");
        };
        let inner = document.query(*inner).expect("inner query exists");
        assert_eq!(inner.parent, Some(outer.id));
        assert_eq!(inner.captures, vec![source]);
    }

    #[test]
    fn order_by_keeps_aggregate_and_ordinal_errors() {
        let schema = schema_with_items();
        for (sql, message) in [
            (
                "SELECT id FROM items ORDER BY min(score)",
                "Parse error: misuse of aggregate: min()",
            ),
            (
                "SELECT min(id) AS m FROM items ORDER BY max(m)",
                "Parse error: misuse of aliased aggregate m",
            ),
            (
                "SELECT id FROM items ORDER BY 0",
                "Parse error: 1st ORDER BY term out of range - should be between 1 and 1",
            ),
            (
                "SELECT id FROM items ORDER BY 2",
                "Parse error: 1st ORDER BY term out of range - should be between 1 and 1",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("ORDER BY keeps its aggregate and ordinal rules");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn compound_order_by_resolves_arm_names_to_first_outputs() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT id AS first_name FROM items \
             UNION ALL \
             SELECT score AS later_name FROM items \
             ORDER BY later_name DESC NULLS FIRST, 1 COLLATE NOCASE",
        )
        .expect("compound ORDER BY searches output names from every arm");
        document
            .validate()
            .expect("compound ORDER BY produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        let first_output = OutputId::query(query.blocks[0].id, 0);
        assert_eq!(query.order_by.len(), 2);
        assert!(matches!(query.order_by[0].expr, Expr::Output(id) if id == first_output));
        assert_eq!(query.order_by[0].order, ast::SortOrder::Desc);
        assert_eq!(query.order_by[0].nulls, Some(ast::NullsOrder::First));
        assert!(matches!(
            &query.order_by[1].expr,
            Expr::Collate { expr, collation }
                if matches!(expr.as_ref(), Expr::Output(id) if *id == first_output)
                    && collation.value() == &crate::translate::collate::CollationSeq::NoCase
        ));

        for (sql, message) in [
            (
                "SELECT id FROM items UNION SELECT score FROM items ORDER BY id + 1",
                "Parse error: 1st ORDER BY term does not match any column in the result set",
            ),
            (
                "SELECT id FROM items UNION SELECT score FROM items ORDER BY 2",
                "Parse error: 2 ORDER BY term out of range - should be between 1 and 1",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("compound ORDER BY only accepts result-column references");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn limit_and_offset_become_query_level_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT id FROM items ORDER BY id \
             LIMIT abs(?1) + 2 OFFSET \"3\"",
        )
        .expect("LIMIT and OFFSET accept standalone scalar expressions");
        document
            .validate()
            .expect("LIMIT and OFFSET produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        let limit = query.limit.as_ref().expect("LIMIT is preserved");
        assert!(matches!(
            &limit.limit,
            Expr::Binary {
                lhs,
                operator: ast::Operator::Add,
                rhs,
                ..
            } if matches!(lhs.as_ref(), Expr::Function(call)
                if matches!(call.evaluation, FunctionEvaluation::Scalar))
                && matches!(rhs.as_ref(), Expr::Literal(ast::Literal::Numeric(value))
                    if value == "2")
        ));
        assert!(matches!(
            limit.offset.as_ref(),
            Some(Expr::Literal(ast::Literal::String(value))) if value == "'3'"
        ));

        let comma = analyze_sql("SELECT 1 UNION ALL SELECT 2 LIMIT 4, 2")
            .expect("comma LIMIT syntax binds");
        let HirRoot::Query(root) = &comma.root else {
            panic!("SELECT produces query root");
        };
        let query = comma.query(root.query).expect("query exists");
        assert_eq!(query.blocks.len(), 2);
        let limit = query.limit.as_ref().expect("compound LIMIT is preserved");
        assert!(matches!(
            &limit.limit,
            Expr::Literal(ast::Literal::Numeric(value)) if value == "2"
        ));
        assert!(matches!(
            limit.offset.as_ref(),
            Some(Expr::Literal(ast::Literal::Numeric(value))) if value == "4"
        ));
    }

    #[test]
    fn limit_subqueries_are_independent_children() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH limit_value AS (SELECT 2 AS n) \
             SELECT id FROM items LIMIT (SELECT n FROM limit_value)",
        )
        .expect("LIMIT accepts an independent scalar subquery");
        document
            .validate()
            .expect("LIMIT subquery and its CTE are reachable HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let Expr::Subquery(SubqueryExpr::Scalar { query: inner, .. }) =
            &outer.limit.as_ref().expect("LIMIT is preserved").limit
        else {
            panic!("LIMIT keeps its scalar subquery");
        };
        let inner = document.query(*inner).expect("LIMIT child query exists");
        assert_eq!(inner.parent, Some(outer.id));
        assert!(inner.captures.is_empty());
        assert_eq!(inner.reachable_ctes.len(), 1);
    }

    #[test]
    fn limit_has_no_query_or_function_scope() {
        let schema = schema_with_items();
        for (sql, message) in [
            (
                "SELECT id AS chosen FROM items LIMIT chosen",
                "Parse error: no such column: chosen",
            ),
            (
                "SELECT (SELECT 1 LIMIT outer_items.id) FROM items AS outer_items",
                "Parse error: no such table: outer_items",
            ),
            (
                "SELECT id FROM items LIMIT sum(1)",
                "Parse error: misuse of aggregate function sum()",
            ),
            (
                "SELECT id FROM items LIMIT row_number() OVER ()",
                "Parse error: misuse of window function: row_number()",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("LIMIT cannot see query names or query functions");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn values_rows_become_block_owned_hir() {
        let document = analyze_sql(
            "VALUES(1 COLLATE NOCASE, 'left'), (2, 'right') UNION ALL SELECT 3, 'last'",
        )
        .expect("VALUES rows bind into HIR");
        document.validate().expect("VALUES produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("VALUES produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        assert_eq!(query.blocks.len(), 2);
        let values = &query.blocks[0];
        assert!(values.from.is_none());
        assert_eq!(
            values
                .outputs
                .iter()
                .map(|output| output.name.as_str())
                .collect::<Vec<_>>(),
            ["column1", "column2"]
        );
        assert_eq!(values.outputs[0].type_fact.storage, Some(Type::Integer));
        assert_eq!(values.outputs[1].type_fact.storage, Some(Type::Text));
        assert!(values.outputs[0].collation_is_explicit);
        assert_eq!(
            values.outputs[0]
                .collation
                .as_ref()
                .expect("first-row explicit collation becomes output metadata")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        let QueryBlockBody::Values { rows } = &values.body else {
            panic!("first compound arm remains VALUES");
        };
        assert_eq!(rows.len(), 2);
        assert!(matches!(
            &rows[0][0],
            Expr::Collate { expr, .. }
                if matches!(expr.as_ref(), Expr::Literal(ast::Literal::Numeric(value)) if value == "1")
        ));
        assert!(matches!(
            &rows[1][1],
            Expr::Literal(ast::Literal::String(value)) if value == "'right'"
        ));
    }

    #[test]
    fn values_subqueries_capture_outer_sources() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT (VALUES(outer_items.value), (upper(outer_items.value))) \
             FROM items AS outer_items",
        )
        .expect("VALUES subquery captures outer source");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_source = outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query has FROM")
            .first;
        let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = &outer.blocks[0].outputs[0].expr
        else {
            panic!("VALUES stays a scalar subquery");
        };
        let values = document.query(*query).expect("VALUES query exists");
        assert_eq!(values.parent, Some(outer.id));
        assert_eq!(values.captures, vec![outer_source]);
        let QueryBlockBody::Values { rows } = &values.blocks[0].body else {
            panic!("child query owns VALUES rows");
        };
        assert!(matches!(
            rows[0][0],
            Expr::Column(reference)
                if reference.source == outer_source && reference.column == 1
        ));
    }

    #[test]
    fn values_keep_function_identities_and_name_rules() {
        let document = analyze_sql("VALUES(sum(1), row_number() OVER ())")
            .expect("VALUES allows query functions");
        let HirRoot::Query(root) = &document.root else {
            panic!("VALUES produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 1);
        assert_eq!(block.window_function_count, 1);
        assert_eq!(block.windows.len(), 1);
        let QueryBlockBody::Values { rows } = &block.body else {
            panic!("query owns VALUES rows");
        };
        assert!(matches!(
            &rows[0][0],
            Expr::Function(call)
                if matches!(call.evaluation, FunctionEvaluation::Aggregate { .. })
        ));
        assert!(matches!(
            &rows[0][1],
            Expr::Function(call)
                if matches!(call.evaluation, FunctionEvaluation::Window { .. })
        ));

        let error = analyze_sql("VALUES(missing)").expect_err("VALUES has no local names");
        assert_eq!(error.to_string(), "Parse error: no such column: missing");
        let document = analyze_sql("VALUES(\"missing\")").expect("VALUES keeps DQS fallback");
        let HirRoot::Query(root) = &document.root else {
            panic!("VALUES produces query root");
        };
        let QueryBlockBody::Values { rows } =
            &document.query(root.query).expect("query exists").blocks[0].body
        else {
            panic!("query owns VALUES rows");
        };
        assert!(matches!(
            &rows[0][0],
            Expr::Literal(ast::Literal::String(value)) if value == "'missing'"
        ));
    }

    #[test]
    fn scalar_subqueries_require_one_output() {
        let error = analyze_sql("SELECT (SELECT 1, 2)")
            .expect_err("multi-column scalar subquery is invalid");
        assert_eq!(
            error.to_string(),
            "Parse error: sub-select returns 2 columns - expected 1"
        );

        analyze_sql("SELECT EXISTS (SELECT 1, 2)")
            .expect("EXISTS does not require a one-column query");
    }

    #[test]
    fn in_query_expressions_keep_comparison_facts_and_captures() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT outer_items.value IN (\
                        SELECT label COLLATE RTRIM FROM categories \
                        UNION ALL SELECT outer_items.value\
                    ), \
                    outer_items.id NOT IN (SELECT outer_items.id) \
             FROM items AS outer_items",
        )
        .expect("IN query expressions bind into HIR");
        document
            .validate()
            .expect("IN query expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_source = outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query has FROM")
            .first;

        let Expr::Subquery(SubqueryExpr::In {
            lhs,
            query,
            negated,
            comparison,
        }) = &outer.blocks[0].outputs[0].expr
        else {
            panic!("IN query remains explicit in HIR");
        };
        assert!(!negated);
        assert!(matches!(
            lhs.as_ref(),
            Expr::Column(reference)
                if reference.source == outer_source && reference.column == 1
        ));
        let query = document.query(*query).expect("IN query exists");
        assert_eq!(query.parent, Some(outer.id));
        assert_eq!(query.captures, vec![outer_source]);
        assert_eq!(comparison.components.len(), 1);
        assert_eq!(
            comparison.components[0].affinity,
            crate::vdbe::affinity::Affinity::Blob
        );
        assert_eq!(
            comparison.components[0]
                .collation
                .as_ref()
                .expect("right explicit collation wins")
                .value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );

        let Expr::Subquery(SubqueryExpr::In {
            query,
            negated,
            comparison,
            ..
        }) = &outer.blocks[0].outputs[1].expr
        else {
            panic!("NOT IN query remains explicit in HIR");
        };
        assert!(negated);
        assert_eq!(comparison.components.len(), 1);
        assert_eq!(
            comparison.components[0].affinity,
            crate::vdbe::affinity::Affinity::Numeric
        );
        assert_eq!(
            document
                .query(*query)
                .expect("NOT IN query exists")
                .captures,
            vec![outer_source]
        );
    }

    #[test]
    fn in_named_relations_reuse_query_membership_hir() {
        let mut schema = schema_with_items();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE candidates(value TEXT COLLATE RTRIM)", 3)
                    .expect("fixed membership table schema parses"),
            ))
            .expect("fixed membership table name is unique");
        let document = analyze_sql_with_schema(
            &schema,
            "WITH c(value) AS (VALUES ('cte')) \
             SELECT outer_items.value IN candidates, \
                    outer_items.value NOT IN c \
             FROM items AS outer_items",
        )
        .expect("named relations bind as membership queries");
        document
            .validate()
            .expect("named relation membership produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        assert_eq!(outer.blocks[0].outputs.len(), 2);
        for (index, expected_negated) in [false, true].into_iter().enumerate() {
            let output = &outer.blocks[0].outputs[index];
            assert_eq!(output.type_fact, TypeFact::known(Type::Integer));
            let Expr::Subquery(SubqueryExpr::In {
                query,
                negated,
                comparison,
                ..
            }) = &output.expr
            else {
                panic!("named relation uses IN-query HIR");
            };
            assert_eq!(*negated, expected_negated);
            assert_eq!(comparison.components.len(), 1);
            assert_eq!(
                comparison.components[0]
                    .collation
                    .as_ref()
                    .expect("left declared collation reaches membership comparison")
                    .value(),
                &crate::translate::collate::CollationSeq::NoCase
            );

            let relation = document.query(*query).expect("relation query exists");
            assert_eq!(relation.parent, Some(outer.id));
            assert!(relation.captures.is_empty());
            assert_eq!(relation.output.len(), 1);
            let block = &relation.blocks[0];
            let source = block.from.as_ref().expect("relation query has FROM").first;
            assert!(matches!(
                block.outputs[0].expr,
                Expr::Column(reference)
                    if reference.source == source && reference.column == 0
            ));
            match (&document.source(source).expect("source exists").kind, index) {
                (SourceKind::Table(table), 0) => {
                    assert_eq!(table.value().get_name(), "candidates");
                }
                (SourceKind::Cte(cte), 1) => {
                    assert_eq!(relation.reachable_ctes, vec![*cte]);
                }
                _ => panic!("named relation keeps resolved source kind"),
            }
        }
    }

    #[cfg(feature = "json")]
    #[test]
    fn in_table_function_arguments_become_relation_query_captures() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT (NULL, items.value, NULL, NULL, NULL, NULL, NULL, NULL) \
                    IN json_each(json_array(items.value)) \
             FROM items",
        )
        .expect("row membership matches json_each visible width");
        document
            .validate()
            .expect("table-function membership produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_source = outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query has FROM")
            .first;
        let Expr::Subquery(SubqueryExpr::In {
            lhs,
            query,
            comparison,
            ..
        }) = &outer.blocks[0].outputs[0].expr
        else {
            panic!("table function uses IN-query HIR");
        };
        assert!(matches!(lhs.as_ref(), Expr::Row(values) if values.len() == 8));
        assert_eq!(comparison.components.len(), 8);

        let relation = document.query(*query).expect("relation query exists");
        assert_eq!(relation.captures, vec![outer_source]);
        assert_eq!(relation.output.len(), 8);
        let source = relation.blocks[0]
            .from
            .as_ref()
            .expect("relation query has FROM")
            .first;
        let SourceKind::TableFunction { table, arguments } =
            &document.source(source).expect("source exists").kind
        else {
            panic!("relation source is table function");
        };
        assert_eq!(table.value().get_name(), "json_each");
        assert_eq!(arguments.len(), 1);
    }

    #[test]
    fn in_named_relations_keep_source_and_width_errors() {
        let schema = schema_with_items();
        for (sql, expected) in [
            ("SELECT 1 IN missing", "Parse error: no such table: missing"),
            (
                "SELECT 1 IN items(2)",
                "Parse error: 'items' is not a function",
            ),
            (
                "SELECT 1 IN items",
                "Parse error: sub-select returns 3 columns - expected 1",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid IN table fails");
            assert_eq!(error.to_string(), expected, "{sql}");
        }
    }

    #[test]
    fn row_in_query_has_one_comparison_component_per_column() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT (outer_items.id, outer_items.value) IN (\
                        SELECT categories.id, categories.label FROM categories\
                    ) \
             FROM items AS outer_items",
        )
        .expect("row IN query expression binds into HIR");
        document
            .validate()
            .expect("row IN query expression produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let Expr::Subquery(SubqueryExpr::In {
            lhs, comparison, ..
        }) = &outer.blocks[0].outputs[0].expr
        else {
            panic!("row IN query remains explicit in HIR");
        };
        let Expr::Row(values) = lhs.as_ref() else {
            panic!("row left side remains explicit in HIR");
        };
        assert_eq!(values.len(), 2);
        assert_eq!(comparison.components.len(), 2);
        assert_eq!(
            comparison.components[0].affinity,
            crate::vdbe::affinity::Affinity::Numeric
        );
        assert_eq!(
            comparison.components[1].affinity,
            crate::vdbe::affinity::Affinity::Blob
        );
        assert_eq!(
            comparison.components[1]
                .collation
                .as_ref()
                .expect("left declared collation wins")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
    }

    #[test]
    fn in_queries_require_matching_row_widths() {
        for (sql, message) in [
            (
                "SELECT 1 IN (SELECT 1, 2)",
                "Parse error: sub-select returns 2 columns - expected 1",
            ),
            (
                "SELECT (1, 2) IN (SELECT 1)",
                "Parse error: sub-select returns 1 columns - expected 2",
            ),
        ] {
            let error = analyze_sql(sql).expect_err("IN query widths must match");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn compound_selects_become_ordered_query_blocks() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT id FROM items \
             UNION ALL SELECT id FROM categories \
             UNION SELECT item_id FROM tags \
             INTERSECT SELECT flag FROM extras \
             EXCEPT SELECT value FROM codes",
        )
        .expect("compound SELECT binds into HIR");
        document
            .validate()
            .expect("compound SELECT produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        assert_eq!(query.blocks.len(), 5);
        assert_eq!(query.first, query.blocks[0].id);
        assert_eq!(query.output, vec![query.blocks[0].outputs[0].id]);

        let expected_operators = [
            ast::CompoundOperator::UnionAll,
            ast::CompoundOperator::Union,
            ast::CompoundOperator::Intersect,
            ast::CompoundOperator::Except,
        ];
        for ((arm, block), operator) in query
            .compounds
            .iter()
            .zip(query.blocks.iter().skip(1))
            .zip(expected_operators)
        {
            assert_eq!(arm.operator, operator);
            assert_eq!(arm.block, block.id);
        }

        let expected_tables = ["items", "categories", "tags", "extras", "codes"];
        for (block, table) in query.blocks.iter().zip(expected_tables) {
            let source = document
                .source(block.from.as_ref().expect("arm has FROM").first)
                .expect("arm source exists");
            assert_eq!(source.name, table);
            assert_eq!(source.owner, SourceOwner::QueryBlock(block.id));
            assert_eq!(block.outputs.len(), 1);
        }
    }

    #[test]
    fn compound_aggregate_identities_belong_to_each_arm() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT count(*) FROM items UNION ALL SELECT count(*) FROM categories",
        )
        .expect("aggregate compound SELECT binds into HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        for block in &query.blocks {
            assert_eq!(block.aggregate_count, 1);
            let Expr::Function(function) = &block.outputs[0].expr else {
                panic!("count becomes a resolved function");
            };
            assert!(matches!(
                function.evaluation,
                FunctionEvaluation::Aggregate { id, .. }
                    if id == crate::translate::semantic::hir::AggregateId::new(block.id, 0)
            ));
        }
    }

    #[test]
    fn compound_selects_keep_width_diagnostics() {
        let error = analyze_sql("SELECT 1, 2 UNION ALL SELECT 3 UNION SELECT 4, 5")
            .expect_err("different compound widths fail semantic analysis");
        assert_eq!(
            error.to_string(),
            "Parse error: SELECTs to the left and right of UNION do not have the same number of result columns"
        );
    }

    #[test]
    fn referenced_ctes_become_document_owned_queries_and_sources() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH picked(item_key, item_text) AS MATERIALIZED (\
                 SELECT id, value FROM items \
                 UNION ALL SELECT id, label FROM categories\
             ) \
             SELECT p.item_text FROM picked AS p",
        )
        .expect("ordinary CTE binds into HIR");
        document.validate().expect("CTE produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let root_query = document.query(root.query).expect("root query exists");
        let cte = &document.ctes[0];
        assert_eq!(root_query.reachable_ctes, vec![cte.id]);
        assert_eq!(cte.name, "picked");
        assert_eq!(cte.materialized, ast::Materialized::Yes);
        assert_eq!(cte.columns.len(), 2);
        assert_eq!(cte.columns[0].name, "item_key");
        assert_eq!(cte.columns[1].name, "item_text");
        assert_eq!(cte.columns[1].type_fact.storage, Some(Type::Text));
        assert_eq!(
            cte.columns[1]
                .collation
                .as_ref()
                .expect("leftmost CTE output keeps collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );

        let CteBody::Query(body_id) = cte.body else {
            panic!("ordinary CTE owns a query body");
        };
        let body = document.query(body_id).expect("CTE body query exists");
        assert_eq!(body.blocks.len(), 2);
        assert_eq!(body.compounds[0].operator, ast::CompoundOperator::UnionAll);

        let root_block = &root_query.blocks[0];
        let source = document
            .source(root_block.from.as_ref().expect("root reads CTE").first)
            .expect("CTE source exists");
        assert!(matches!(source.kind, SourceKind::Cte(id) if id == cte.id));
        assert_eq!(source.owner, SourceOwner::QueryBlock(root_block.id));
        assert_eq!(source.alias.as_deref(), Some("p"));
        assert!(!source.rowid_available);
        assert_eq!(source.columns[1].name, "item_text");
        assert!(matches!(
            root_block.outputs[0].expr,
            Expr::Column(reference) if reference.source == source.id && reference.column == 1
        ));
    }

    #[test]
    fn recursive_ctes_split_seed_arms_and_input_occurrences() {
        let document = analyze_sql(
            "WITH RECURSIVE seq(x) AS (\
                 VALUES(1), (10) \
                 UNION ALL SELECT 100 \
                 UNION ALL SELECT x + 1 FROM seq AS first WHERE x < 3 \
                 UNION ALL SELECT x + 10 FROM seq AS second WHERE x < 20\
             ) SELECT x FROM seq",
        )
        .expect("recursive CTE binds into HIR");
        document
            .validate()
            .expect("recursive CTE produces closed HIR");

        assert_eq!(document.ctes.len(), 1);
        let cte = &document.ctes[0];
        assert_eq!(cte.columns.len(), 1);
        assert_eq!(cte.columns[0].name, "x");
        assert_eq!(cte.columns[0].type_fact.storage, Some(Type::Integer));
        let CteBody::Recursive(recursive) = &cte.body else {
            panic!("self-reference produces recursive CTE body");
        };
        let seed = document.query(recursive.seed).expect("seed query exists");
        assert_eq!(seed.blocks.len(), 2);
        let QueryBlockBody::Values { rows } = &seed.blocks[0].body else {
            panic!("VALUES seed remains explicit");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(recursive.arms.len(), 2);
        assert!(recursive
            .arms
            .iter()
            .all(|arm| arm.operator == ast::CompoundOperator::UnionAll));
        assert_eq!(recursive.input_sources.len(), 2);
        assert_eq!(recursive.comparison_collations.len(), 1);
        assert!(recursive.queue_order.is_empty());
        assert!(recursive.limit.is_none());

        let sources = recursive
            .input_sources
            .iter()
            .map(|source| document.source(*source).expect("recursive input exists"))
            .collect::<Vec<_>>();
        assert!(sources
            .iter()
            .all(|source| matches!(source.kind, SourceKind::RecursiveInput(id) if id == cte.id)));
        assert_eq!(sources[0].alias.as_deref(), Some("first"));
        assert_eq!(sources[1].alias.as_deref(), Some("second"));
        assert_ne!(sources[0].id, sources[1].id);
        for (arm, source) in recursive.arms.iter().zip(sources) {
            let query = document.query(arm.query).expect("recursive arm exists");
            assert_eq!(query.blocks.len(), 1);
            assert_eq!(
                query.blocks[0]
                    .from
                    .as_ref()
                    .expect("arm reads input")
                    .first,
                source.id
            );
        }

        let union = analyze_sql(
            "WITH seq(x) AS (\
                 VALUES('a') UNION SELECT upper(x) COLLATE NOCASE FROM seq\
             ) SELECT x FROM seq",
        )
        .expect("recursive UNION binds comparison metadata");
        let CteBody::Recursive(recursive) = &union.ctes[0].body else {
            panic!("UNION self-reference produces recursive CTE");
        };
        assert_eq!(recursive.arms[0].operator, ast::CompoundOperator::Union);
        assert_eq!(
            recursive.comparison_collations[0]
                .as_ref()
                .expect("recursive arm supplies comparison collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
    }

    #[test]
    fn recursive_ctes_bind_queue_order_and_limit() {
        let document = analyze_sql(
            "WITH RECURSIVE seq(value) AS (\
                 SELECT 'a' AS key \
                 UNION ALL SELECT value || 'x' AS next FROM seq WHERE length(value) < 3 \
                 ORDER BY next COLLATE NOCASE DESC NULLS LAST \
                 LIMIT (SELECT 4) OFFSET ?1\
             ) SELECT value FROM seq",
        )
        .expect("recursive queue controls bind into HIR");
        document
            .validate()
            .expect("recursive queue controls produce closed HIR");

        let CteBody::Recursive(recursive) = &document.ctes[0].body else {
            panic!("self-reference produces recursive CTE body");
        };
        assert_eq!(recursive.queue_order.len(), 1);
        let order = &recursive.queue_order[0];
        assert_eq!(order.output, 0);
        assert_eq!(order.order, ast::SortOrder::Desc);
        assert_eq!(order.nulls, Some(ast::NullsOrder::Last));
        assert_eq!(
            order
                .explicit_collation
                .as_ref()
                .expect("COLLATE resolves for queue ordering")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );

        let limit = recursive.limit.as_ref().expect("recursive LIMIT binds");
        let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = &limit.limit else {
            panic!("LIMIT subquery remains explicit HIR");
        };
        assert_eq!(
            document.query(*query).expect("LIMIT query exists").parent,
            Some(recursive.seed)
        );
        assert!(matches!(
            limit.offset,
            Some(Expr::Parameter(ref parameter)) if parameter.index.get() == 1
        ));
    }

    #[test]
    fn recursive_cte_reference_counting_respects_nested_cte_scopes() {
        let shadowed = analyze_sql(
            "WITH outer_seq(x) AS (\
                 WITH outer_seq(y) AS (VALUES(2)) \
                 SELECT y FROM outer_seq\
             ) SELECT x FROM outer_seq",
        )
        .expect("nested CTE shadows outer name");
        shadowed
            .validate()
            .expect("shadowed CTE produces closed HIR");
        assert!(matches!(shadowed.ctes[1].body, CteBody::Query(_)));

        let nested_subquery = analyze_sql(
            "WITH seq(x) AS (\
                 VALUES(1) \
                 UNION ALL \
                 SELECT x + 1 FROM seq \
                 WHERE x < 2 AND EXISTS (\
                     WITH seq(y) AS (VALUES(1)) SELECT 1 FROM seq\
                 )\
             ) SELECT x FROM seq",
        )
        .expect("nested subquery CTE shadows recursive input");
        nested_subquery
            .validate()
            .expect("nested shadowing produces closed recursive HIR");
        assert_eq!(
            nested_subquery
                .ctes
                .iter()
                .filter(|cte| matches!(cte.body, CteBody::Recursive(_)))
                .count(),
            1
        );

        let unused = analyze_sql(
            "WITH seq(x) AS (\
                 WITH unused AS (SELECT x FROM seq) \
                 VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < 2\
             ) SELECT x FROM seq",
        )
        .expect("unused nested CTE contributes no recursive reference");
        unused
            .validate()
            .expect("unused nested CTE produces closed HIR");

        let error = analyze_sql(
            "WITH seq(x) AS (\
                 WITH helper AS (SELECT x FROM seq) \
                 VALUES(1) UNION ALL SELECT seq.x FROM seq, helper\
             ) SELECT x FROM seq",
        )
        .expect_err("used nested CTE contributes its recursive references");
        assert_eq!(
            error.to_string(),
            "Parse error: multiple recursive references: seq"
        );
    }

    #[test]
    fn cte_queries_capture_enclosing_sources() {
        let schema = schema_with_items();
        let ordinary = analyze_sql_with_schema(
            &schema,
            "SELECT (\
                 WITH selected(x) AS (SELECT outer_items.id) \
                 SELECT x FROM selected\
             ) FROM items AS outer_items",
        )
        .expect("ordinary CTE sees its owning query's outer scope");
        ordinary
            .validate()
            .expect("correlated ordinary CTE produces closed HIR");
        let HirRoot::Query(ordinary_root) = &ordinary.root else {
            panic!("SELECT produces query root");
        };
        let ordinary_outer = ordinary
            .query(ordinary_root.query)
            .expect("outer query exists");
        let ordinary_source = ordinary_outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query reads items")
            .first;
        let CteBody::Query(ordinary_body) = ordinary.ctes[0].body else {
            panic!("ordinary CTE owns a query body");
        };
        let ordinary_body = ordinary
            .query(ordinary_body)
            .expect("ordinary CTE query exists");
        assert_eq!(ordinary_body.captures, vec![ordinary_source]);

        let document = analyze_sql_with_schema(
            &schema,
            "SELECT (\
                 WITH RECURSIVE seq(x) AS (\
                     VALUES(outer_items.id) \
                     UNION ALL \
                     SELECT x + 1 FROM seq WHERE x < outer_items.id + 2\
                 ) SELECT max(x) FROM seq\
             ) FROM items AS outer_items",
        )
        .expect("recursive CTE sees its owning query's outer scope");
        document
            .validate()
            .expect("correlated recursive CTE produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_source = outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query reads items")
            .first;
        let Expr::Subquery(SubqueryExpr::Scalar {
            query: owner_id, ..
        }) = outer.blocks[0].outputs[0].expr
        else {
            panic!("outer output contains the CTE-owning query");
        };
        let CteBody::Recursive(recursive) = &document.ctes[0].body else {
            panic!("self-reference produces recursive CTE");
        };
        let seed = document.query(recursive.seed).expect("seed query exists");
        assert_eq!(seed.parent, Some(owner_id));
        assert_eq!(seed.captures, vec![outer_source]);
        assert_eq!(recursive.arms.len(), 1);
        let arm = document
            .query(recursive.arms[0].query)
            .expect("recursive arm query exists");
        assert_eq!(arm.parent, Some(owner_id));
        assert_eq!(arm.captures, vec![outer_source]);

        let error = analyze_sql_with_schema(
            &schema,
            "SELECT (\
                 WITH c(x) AS (SELECT local_items.id) \
                 SELECT x FROM c, items AS local_items\
             ) FROM items AS outer_items",
        )
        .expect_err("CTE cannot see sibling sources from its owning SELECT");
        assert_eq!(error.to_string(), "Parse error: no such table: local_items");
    }

    #[test]
    fn recursive_ctes_keep_structure_and_function_errors() {
        for (sql, message) in [
            (
                "WITH seq(x) AS (SELECT x FROM seq UNION ALL SELECT 1) SELECT x FROM seq",
                "Parse error: circular reference: seq",
            ),
            (
                "WITH seq(x) AS (VALUES(1) UNION ALL SELECT a.x FROM seq a, seq b) SELECT x FROM seq",
                "Parse error: multiple references to recursive table: seq",
            ),
            (
                "WITH seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq UNION SELECT x + 2 FROM seq) SELECT x FROM seq",
                "Parse error: recursive CTE queries must use the same UNION operator",
            ),
            (
                "WITH seq(x) AS (VALUES(1) UNION ALL SELECT x, x + 1 FROM seq) SELECT x FROM seq",
                "Parse error: SELECTs to the left and right of UNION ALL do not have the same number of result columns",
            ),
            (
                "WITH seq(x) AS (VALUES(1) UNION ALL SELECT sum(x) FROM seq) SELECT x FROM seq",
                "Parse error: recursive aggregate queries not supported",
            ),
            (
                "WITH seq(x) AS (VALUES(1) UNION ALL SELECT row_number() OVER () FROM seq) SELECT x FROM seq",
                "Parse error: cannot use window functions in recursive queries",
            ),
            (
                "WITH seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq ORDER BY missing) SELECT x FROM seq",
                "Parse error: 1st ORDER BY term does not match any column in the result set",
            ),
            (
                "WITH seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq LIMIT x) SELECT x FROM seq",
                "Parse error: no such column: x",
            ),
        ] {
            let error = analyze_sql(sql).expect_err("invalid recursive CTE fails analysis");
            assert_eq!(error.to_string(), message);
        }

        analyze_sql("WITH unused(x) AS (SELECT x FROM unused UNION ALL SELECT 1) SELECT 1")
            .expect("unused invalid recursive CTE remains lazy");
    }

    #[test]
    fn ctes_bind_lazily_and_schema_qualification_bypasses_them() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH items(a, b) AS (SELECT missing) SELECT value FROM main.items",
        )
        .expect("unused invalid CTE stays unbound");
        assert!(document.ctes.is_empty());
        assert_eq!(document.queries.len(), 1);

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let source = document
            .source(
                document.query(root.query).expect("query exists").blocks[0]
                    .from
                    .as_ref()
                    .expect("query reads schema table")
                    .first,
            )
            .expect("schema source exists");
        assert!(matches!(source.kind, SourceKind::Table(_)));
    }

    #[test]
    fn ctes_can_reference_later_siblings() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH first AS (SELECT value FROM second), \
                  second AS (SELECT value FROM items) \
             SELECT value FROM first",
        )
        .expect("forward CTE dependency binds lazily");
        document
            .validate()
            .expect("forward CTE dependency produces closed HIR");

        assert_eq!(document.ctes.len(), 2);
        assert_eq!(document.ctes[0].name, "second");
        assert_eq!(document.ctes[1].name, "first");
        let CteBody::Query(first_body) = document.ctes[1].body else {
            panic!("ordinary CTE owns query body");
        };
        assert_eq!(
            document
                .query(first_body)
                .expect("first body exists")
                .reachable_ctes,
            vec![document.ctes[0].id]
        );
    }

    #[test]
    fn repeated_cte_reads_share_one_definition() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH picked AS (SELECT id FROM items) \
             SELECT left_pick.id, right_pick.id \
             FROM picked AS left_pick JOIN picked AS right_pick",
        )
        .expect("repeated CTE reads bind into HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("root query exists");
        assert_eq!(document.ctes.len(), 1);
        assert_eq!(query.reachable_ctes, vec![document.ctes[0].id]);
        let from = query.blocks[0].from.as_ref().expect("query reads CTE");
        let left = document.source(from.first).expect("left source exists");
        let right = document
            .source(from.joins[0].right)
            .expect("right source exists");
        assert!(matches!(left.kind, SourceKind::Cte(id) if id == document.ctes[0].id));
        assert!(matches!(right.kind, SourceKind::Cte(id) if id == document.ctes[0].id));
        assert_ne!(left.id, right.id);
    }

    #[test]
    fn nested_with_clauses_shadow_outer_ctes() {
        let document = analyze_sql(
            "WITH picked AS (SELECT 1 AS value), \
                  nested AS (\
                      WITH picked AS (SELECT 2 AS value) \
                      SELECT value FROM picked\
                  ) \
             SELECT value FROM nested",
        )
        .expect("nested WITH shadows outer CTE");
        document
            .validate()
            .expect("nested CTE scopes produce closed HIR");

        assert_eq!(document.ctes.len(), 2);
        assert_eq!(document.ctes[0].name, "picked");
        assert_eq!(document.ctes[1].name, "nested");
        let CteBody::Query(shadow_body) = document.ctes[0].body else {
            panic!("ordinary CTE owns query body");
        };
        assert!(matches!(
            &document
                .query(shadow_body)
                .expect("shadow body exists")
                .blocks[0]
                .outputs[0]
                .expr,
            Expr::Literal(ast::Literal::Numeric(value)) if value == "2"
        ));
    }

    #[test]
    fn referenced_ctes_keep_lazy_structure_errors() {
        for (sql, expected) in [
            (
                "WITH picked(a, b) AS (SELECT 1) SELECT * FROM picked",
                "Parse error: table picked has 1 values for 2 columns",
            ),
            (
                "WITH a AS (SELECT * FROM b), b AS (SELECT * FROM a) SELECT * FROM a",
                "Parse error: circular reference: a",
            ),
        ] {
            let error = analyze_sql(sql).expect_err("invalid referenced CTE fails analysis");
            assert_eq!(error.to_string(), expected);
        }

        let document = analyze_sql("WITH picked(a, b) AS (SELECT 1) SELECT 1")
            .expect("unused column-count mismatch stays deferred");
        assert!(document.ctes.is_empty());
    }

    #[test]
    fn basic_joins_keep_source_order_kinds_and_closed_on_expressions() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT i.value, c.label, t.note, e.flag \
             FROM items AS i, categories AS c \
             JOIN tags AS t ON e.item_id = i.id \
             CROSS JOIN extras AS e",
        )
        .expect("basic joins bind into HIR");
        document.validate().expect("basic joins produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("joined query has FROM");
        assert_eq!(from.joins.len(), 3);
        assert_eq!(from.joins[0].kind, JoinKind::Comma);
        assert_eq!(from.joins[1].kind, JoinKind::Inner);
        assert_eq!(from.joins[2].kind, JoinKind::Cross);

        let sources = [
            from.first,
            from.joins[0].right,
            from.joins[1].right,
            from.joins[2].right,
        ];
        let aliases: Vec<_> = sources
            .iter()
            .map(|source| {
                document
                    .source(*source)
                    .expect("join source exists")
                    .alias
                    .as_deref()
            })
            .collect();
        assert_eq!(aliases, [Some("i"), Some("c"), Some("t"), Some("e")]);

        let JoinConstraint::On(Expr::Binary { lhs, rhs, .. }) = &from.joins[1].constraint else {
            panic!("JOIN ON becomes a resolved HIR expression");
        };
        assert!(matches!(
            lhs.as_ref(),
            Expr::Column(reference) if reference.source == sources[3] && reference.column == 0
        ));
        assert!(matches!(
            rhs.as_ref(),
            Expr::Column(reference) if reference.source == sources[0] && reference.column == 0
        ));

        for (output, source) in block.outputs.iter().zip(sources) {
            assert!(matches!(
                output.expr,
                Expr::Column(reference) if reference.source == source && reference.column == 1
            ));
        }
    }

    #[test]
    fn joined_sources_keep_column_ambiguity_errors() {
        let schema = schema_with_join_tables();
        let error = analyze_sql_with_schema(
            &schema,
            "SELECT id FROM items JOIN categories ON items.id = categories.id",
        )
        .expect_err("unqualified duplicate join column is ambiguous");
        assert_eq!(error.to_string(), "Parse error: ambiguous column name: id");

        let document = analyze_sql_with_schema(
            &schema,
            "SELECT items.id FROM items \
             INNER JOIN categories ON items.id = categories.id",
        )
        .expect("explicit INNER JOIN binds");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let from = document.query(root.query).expect("query exists").blocks[0]
            .from
            .as_ref()
            .expect("joined query has FROM");
        assert_eq!(from.joins[0].kind, JoinKind::Inner);
    }

    #[test]
    fn parenthesized_from_group_alias_exposes_group_columns_only() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT grouped.id, grouped.value, grouped.label, grouped.* \
             FROM (items JOIN categories USING (id)) AS grouped",
        )
        .expect("aliased FROM group binds");
        document
            .validate()
            .expect("aliased FROM group produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("query has FROM");
        let group_source = document.source(from.first).expect("group source exists");
        let SourceKind::FromGroup(group) = &group_source.kind else {
            panic!("parenthesized FROM becomes a group source");
        };
        assert_eq!(group_source.alias.as_deref(), Some("grouped"));
        assert_eq!(group.columns.len(), group_source.columns.len());
        assert_eq!(group.from.joins.len(), 1);
        assert!(matches!(group.columns[0], Expr::MergedColumn(_)));
        assert_eq!(block.outputs.len(), 7);
        assert!(block.outputs.iter().all(|output| {
            matches!(output.expr, Expr::Column(reference) if reference.source == group_source.id)
        }));

        let error = analyze_sql_with_schema(
            &schema,
            "SELECT items.value FROM (items JOIN categories USING (id)) AS grouped",
        )
        .expect_err("group alias hides inner table qualifiers");
        assert_eq!(error.to_string(), "Parse error: no such table: items");
    }

    #[test]
    fn unaliased_from_group_keeps_inner_qualifiers_and_merged_columns() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT items.value, categories.label, id \
             FROM (items JOIN categories USING (id))",
        )
        .expect("unaliased FROM group binds");
        document
            .validate()
            .expect("unaliased FROM group produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let group_id = block.from.as_ref().expect("query has FROM").first;
        let group_source = document.source(group_id).expect("group source exists");
        let SourceKind::FromGroup(group) = &group_source.kind else {
            panic!("parenthesized FROM becomes a group source");
        };
        let items = group.from.first;
        let categories = group.from.joins[0].right;
        assert!(matches!(
            block.outputs[0].expr,
            Expr::Column(reference) if reference.source == items && reference.column == 1
        ));
        assert!(matches!(
            block.outputs[1].expr,
            Expr::Column(reference) if reference.source == categories && reference.column == 1
        ));
        assert!(matches!(
            block.outputs[2].expr,
            Expr::Column(reference) if reference.source == group_id && reference.column == 0
        ));
        assert!(matches!(group.columns[0], Expr::MergedColumn(_)));
    }

    #[test]
    fn parenthesized_from_group_preserves_nested_join_precedence() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT i.value, c.label \
             FROM items AS i \
             LEFT JOIN (categories AS c JOIN tags AS t ON t.item_id = c.id) \
             ON i.id = c.id",
        )
        .expect("nested join group binds");
        document
            .validate()
            .expect("nested join group produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("query has FROM");
        assert_eq!(from.joins.len(), 1);
        assert_eq!(from.joins[0].kind, JoinKind::Left);
        let group_source = document
            .source(from.joins[0].right)
            .expect("group source exists");
        let SourceKind::FromGroup(group) = &group_source.kind else {
            panic!("right operand remains a FROM group");
        };
        assert_eq!(group.from.joins.len(), 1);
        assert_eq!(group.from.joins[0].kind, JoinKind::Inner);
    }

    #[test]
    fn ctes_inside_parenthesized_from_groups_remain_reachable() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH picked AS (SELECT * FROM items) \
             SELECT p.value FROM (picked AS p JOIN categories USING (id))",
        )
        .expect("CTE inside FROM group binds");
        document
            .validate()
            .expect("nested CTE remains reachable from the query");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let query = document.query(root.query).expect("query exists");
        assert_eq!(query.reachable_ctes, vec![document.ctes[0].id]);
    }

    #[cfg(feature = "json")]
    #[test]
    fn table_function_arguments_bind_against_the_complete_from_scope() {
        let schema = schema_with_items();
        for sql in [
            "SELECT j.value FROM items JOIN json_each(items.value) AS j",
            "SELECT j.value FROM json_each(items.value) AS j JOIN items",
        ] {
            let document = analyze_sql_with_schema(&schema, sql)
                .expect("table-function arguments can use any FROM source");
            document
                .validate()
                .expect("table functions produce closed HIR");

            let HirRoot::Query(root) = &document.root else {
                panic!("SELECT produces query root");
            };
            let block = &document.query(root.query).expect("query exists").blocks[0];
            let from = block.from.as_ref().expect("query has FROM");
            let source_ids = [from.first, from.joins[0].right];
            let function_id = source_ids
                .into_iter()
                .find(|source| {
                    matches!(
                        document.source(*source).expect("source exists").kind,
                        SourceKind::TableFunction { .. }
                    )
                })
                .expect("FROM contains table function");
            let items_id = source_ids
                .into_iter()
                .find(|source| *source != function_id)
                .expect("FROM contains items table");
            let source = document
                .source(function_id)
                .expect("function source exists");
            let SourceKind::TableFunction { table, arguments } = &source.kind else {
                panic!("json_each is a table-function source");
            };
            assert_eq!(table.value().get_name(), "json_each");
            assert_eq!(arguments.len(), 1);
            assert!(matches!(
                arguments[0],
                Expr::Column(reference)
                    if reference.source == items_id && reference.column == 1
            ));
        }
    }

    #[cfg(feature = "json")]
    #[test]
    fn table_function_arguments_contribute_query_captures() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT (SELECT j.value FROM json_each(items.value) AS j LIMIT 1) FROM items",
        )
        .expect("table-function argument can capture an outer source");
        document
            .validate()
            .expect("captured table-function argument produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outer_source = outer.blocks[0]
            .from
            .as_ref()
            .expect("outer query has FROM")
            .first;
        let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = &outer.blocks[0].outputs[0].expr
        else {
            panic!("output contains scalar query");
        };
        let inner = document.query(*query).expect("inner query exists");
        assert_eq!(inner.captures, vec![outer_source]);
        let function_source = inner.blocks[0]
            .from
            .as_ref()
            .expect("inner query has FROM")
            .first;
        let SourceKind::TableFunction { arguments, .. } = &document
            .source(function_source)
            .expect("source exists")
            .kind
        else {
            panic!("inner source is a table function");
        };
        assert!(matches!(
            arguments.as_slice(),
            [Expr::Column(reference)]
                if reference.source == outer_source && reference.column == 1
        ));
    }

    #[cfg(feature = "json")]
    #[test]
    fn table_function_calls_keep_source_diagnostics() {
        let schema = schema_with_items();
        for (sql, expected) in [
            (
                "SELECT * FROM items(1)",
                "Parse error: 'items' is not a function",
            ),
            (
                "SELECT * FROM json_each(1, 2, 3)",
                "Parse error: Too many arguments for json_each: expected at most 2, got 3",
            ),
            (
                "WITH c(value) AS (VALUES (1)) SELECT * FROM c(1)",
                "Parse error: 'c' is not a function",
            ),
            (
                "SELECT * FROM json_each(sum(items.value)) JOIN items",
                "Parse error: misuse of aggregate function sum()",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid call fails");
            assert_eq!(error.to_string(), expected, "{sql}");
        }

        let document = analyze_sql_with_schema(&schema, "SELECT value FROM items()")
            .expect("zero-argument call syntax keeps plain-table behavior");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let source = document.query(root.query).expect("query exists").blocks[0]
            .from
            .as_ref()
            .expect("query has FROM")
            .first;
        assert!(matches!(
            document.source(source).expect("source exists").kind,
            SourceKind::Table(_)
        ));
    }

    #[test]
    fn using_joins_build_one_visible_column_from_resolved_sides() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT id, items.id, categories.id, * \
             FROM items JOIN categories USING (id)",
        )
        .expect("USING join binds into HIR");
        document.validate().expect("USING join produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("joined query has FROM");
        let JoinConstraint::Using(columns) = &from.joins[0].constraint else {
            panic!("USING remains explicit in HIR");
        };
        assert_eq!(columns.len(), 1);
        let column = &columns[0];
        assert_eq!(column.name, "id");
        assert_eq!(column.value, MergedColumnValue::Left);
        assert!(matches!(
            column.left.as_ref(),
            Expr::Column(reference) if reference.source == from.first && reference.column == 0
        ));
        assert_eq!(column.right.source, from.joins[0].right);
        assert_eq!(column.right.column, 0);

        assert!(matches!(block.outputs[0].expr, Expr::MergedColumn(_)));
        assert!(matches!(
            block.outputs[1].expr,
            Expr::Column(reference) if reference.source == from.first && reference.column == 0
        ));
        assert!(matches!(
            block.outputs[2].expr,
            Expr::Column(reference)
                if reference.source == from.joins[0].right && reference.column == 0
        ));
        assert_eq!(block.outputs.len(), 7);
        assert!(matches!(block.outputs[3].expr, Expr::MergedColumn(_)));
    }

    #[test]
    fn natural_joins_use_both_sides_for_comparison_and_left_for_visible_facts() {
        let schema = schema_with_join_tables();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value, items.value, codes.value, * \
             FROM items NATURAL JOIN codes",
        )
        .expect("NATURAL join binds into HIR");
        document
            .validate()
            .expect("NATURAL join produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("joined query has FROM");
        let JoinConstraint::Natural(columns) = &from.joins[0].constraint else {
            panic!("NATURAL remains explicit in HIR");
        };
        assert_eq!(columns.len(), 1);
        let column = &columns[0];
        assert_eq!(column.name, "value");
        assert_eq!(column.type_fact.storage, Some(Type::Text));
        assert_eq!(
            column.affinity,
            crate::vdbe::affinity::Affinity::Text,
            "visible merged value keeps left affinity"
        );
        assert_eq!(
            column.comparison.components[0].affinity,
            crate::vdbe::affinity::Affinity::Numeric,
            "comparison sees the right INTEGER affinity"
        );
        assert_eq!(
            column
                .collation
                .as_ref()
                .expect("visible merged value keeps left collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert_eq!(
            column.comparison.components[0]
                .collation
                .as_ref()
                .expect("comparison resolves collation from both sides")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(matches!(block.outputs[0].expr, Expr::MergedColumn(_)));
        assert_eq!(block.outputs.len(), 7);
        assert!(matches!(block.outputs[3].expr, Expr::Column(_)));
        assert!(matches!(block.outputs[4].expr, Expr::MergedColumn(_)));
    }

    #[test]
    fn using_and_natural_joins_keep_name_errors() {
        let schema = schema_with_join_tables();
        let missing = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items JOIN categories USING (missing)",
        )
        .expect_err("USING requires the name on both sides");
        assert_eq!(
            missing.to_string(),
            "Parse error: cannot join using column missing - column not present in both tables"
        );

        let constrained = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items NATURAL JOIN categories ON items.id = categories.id",
        )
        .expect_err("NATURAL rejects an explicit constraint");
        assert_eq!(
            constrained.to_string(),
            "Parse error: a NATURAL join may not have an ON or USING clause"
        );

        let ambiguous = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items JOIN categories ON TRUE NATURAL JOIN extras",
        )
        .expect_err("NATURAL common name must be unique on the left");
        assert_eq!(
            ambiguous.to_string(),
            "Parse error: ambiguous column name: id"
        );
    }

    #[test]
    fn outer_using_joins_keep_kind_value_facts_and_star_order() {
        let schema = schema_with_join_tables();
        for (operator, kind, value) in [
            ("LEFT JOIN", JoinKind::Left, MergedColumnValue::Left),
            ("RIGHT JOIN", JoinKind::Right, MergedColumnValue::Right),
            ("FULL JOIN", JoinKind::Full, MergedColumnValue::Coalesce),
        ] {
            let sql = format!("SELECT * FROM items {operator} codes USING (value)");
            let document =
                analyze_sql_with_schema(&schema, &sql).expect("outer USING join binds into HIR");
            document
                .validate()
                .expect("outer USING join produces closed HIR");

            let HirRoot::Query(root) = &document.root else {
                panic!("SELECT produces query root");
            };
            let block = &document.query(root.query).expect("query exists").blocks[0];
            let from = block.from.as_ref().expect("joined query has FROM");
            let join = &from.joins[0];
            assert_eq!(join.kind, kind);
            let JoinConstraint::Using(columns) = &join.constraint else {
                panic!("outer USING remains explicit in HIR");
            };
            let column = &columns[0];
            assert_eq!(column.value, value);
            let Expr::MergedColumn(merged) = &block.outputs[1].expr else {
                panic!("star emits one merged USING column");
            };
            assert_eq!(merged.value, value);
            assert_eq!(
                block
                    .outputs
                    .iter()
                    .map(|output| output.name.as_str())
                    .collect::<Vec<_>>(),
                ["id", "value", "score", "label"],
                "RIGHT JOIN does not reorder lexical star columns"
            );

            match value {
                MergedColumnValue::Left => {
                    assert_eq!(column.type_fact.storage, Some(Type::Text));
                    assert_eq!(column.affinity, crate::vdbe::affinity::Affinity::Text);
                    assert!(column.has_affinity);
                    assert!(column.collation.is_some());
                }
                MergedColumnValue::Right => {
                    assert_eq!(column.type_fact.storage, Some(Type::Integer));
                    assert_eq!(column.affinity, crate::vdbe::affinity::Affinity::Integer);
                    assert!(column.has_affinity);
                    assert!(column.collation.is_none());
                }
                MergedColumnValue::Coalesce => {
                    assert!(column.type_fact.storage.is_none());
                    assert_eq!(column.affinity, crate::vdbe::affinity::Affinity::Blob);
                    assert!(!column.has_affinity);
                    assert!(column.collation.is_none());
                }
            }
        }
    }

    #[test]
    fn natural_outer_joins_use_the_same_merge_direction() {
        let schema = schema_with_join_tables();
        let document =
            analyze_sql_with_schema(&schema, "SELECT value FROM items NATURAL RIGHT JOIN codes")
                .expect("NATURAL RIGHT JOIN binds into HIR");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let join = &block.from.as_ref().expect("joined query has FROM").joins[0];
        assert_eq!(join.kind, JoinKind::Right);
        let JoinConstraint::Natural(columns) = &join.constraint else {
            panic!("NATURAL RIGHT JOIN remains explicit in HIR");
        };
        assert_eq!(columns[0].value, MergedColumnValue::Right);
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Integer));
    }

    #[test]
    fn right_join_after_another_join_keeps_existing_error() {
        let schema = schema_with_join_tables();
        let error = analyze_sql_with_schema(
            &schema,
            "SELECT * FROM items JOIN categories ON TRUE \
             RIGHT JOIN extras ON TRUE",
        )
        .expect_err("chained RIGHT JOIN remains unsupported");
        assert_eq!(
            error.to_string(),
            "Parse error: RIGHT JOIN following another join is not yet supported. \
             Try rewriting as LEFT JOIN or using a subquery."
        );
    }

    #[test]
    fn array_columns_keep_element_facts_without_program_metadata() {
        let schema = schema_with_array_columns();
        let document =
            analyze_sql_with_schema(&schema, "SELECT vals, matrix, anything FROM arrays")
                .expect("built-in array columns bind");
        document
            .validate()
            .expect("array type facts produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source_id = block.from.as_ref().expect("array query has source").first;
        let source = document.source(source_id).expect("array source exists");
        assert!(source.column_type_programs.iter().all(Option::is_none));

        let values = &source.columns[0].type_fact;
        assert_eq!(values.storage, Some(Type::Blob));
        assert_eq!(values.array_dimensions, 1);
        let value = values.array_element().expect("INTEGER[] has an element");
        assert_eq!(value.storage, Some(Type::Integer));
        assert_eq!(value.array_dimensions, 0);

        let matrix = &source.columns[1].type_fact;
        assert_eq!(matrix.storage, Some(Type::Blob));
        assert_eq!(matrix.array_dimensions, 2);
        let row = matrix.array_element().expect("TEXT[][] has array elements");
        assert_eq!(row.storage, Some(Type::Blob));
        assert_eq!(row.array_dimensions, 1);
        let value = row.array_element().expect("TEXT[] has scalar elements");
        assert_eq!(value.storage, Some(Type::Text));

        let anything = &source.columns[2].type_fact;
        assert!(anything
            .array_element()
            .expect("ANY[] has an element fact")
            .storage
            .is_none());
        assert!(block.outputs.iter().enumerate().all(|(column, output)| {
            matches!(
                output.expr,
                Expr::Column(reference)
                    if reference.source == source_id && reference.column == column
            )
        }));
    }

    #[test]
    fn array_construction_and_subscripts_use_dedicated_hir() {
        let schema = schema_with_array_columns();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT ARRAY[1, 2], array(ARRAY[1], ARRAY[2]), \
                    vals[1], array_element(matrix, 1) \
             FROM arrays",
        )
        .expect("array constructors and subscripts bind");
        document
            .validate()
            .expect("array expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;

        assert!(matches!(&outputs[0].expr, Expr::Array(elements) if elements.len() == 2));
        assert_eq!(outputs[0].type_fact.storage, Some(Type::Blob));
        assert_eq!(outputs[0].type_fact.array_dimensions, 1);

        let Expr::Array(rows) = &outputs[1].expr else {
            panic!("array() becomes array HIR");
        };
        assert!(rows
            .iter()
            .all(|row| matches!(row, Expr::Array(elements) if elements.len() == 1)));
        assert_eq!(outputs[1].type_fact.storage, Some(Type::Blob));
        assert_eq!(outputs[1].type_fact.array_dimensions, 2);

        let Expr::Subscript { base, index } = &outputs[2].expr else {
            panic!("bracket syntax becomes subscript HIR");
        };
        assert!(matches!(base.as_ref(), Expr::Column(column) if column.column == 0));
        assert!(matches!(
            index.as_ref(),
            Expr::Literal(ast::Literal::Numeric(value)) if value == "1"
        ));
        assert_eq!(outputs[2].type_fact.storage, Some(Type::Integer));
        assert_eq!(outputs[2].type_fact.array_dimensions, 0);

        let Expr::Subscript { base, .. } = &outputs[3].expr else {
            panic!("array_element() becomes subscript HIR");
        };
        assert!(matches!(base.as_ref(), Expr::Column(column) if column.column == 1));
        assert_eq!(outputs[3].type_fact.storage, Some(Type::Blob));
        assert_eq!(outputs[3].type_fact.array_dimensions, 1);
        assert_eq!(
            outputs[3]
                .type_fact
                .declared
                .as_ref()
                .map(|declaration| declaration.name.as_str()),
            Some("TEXT")
        );
    }

    #[test]
    fn array_functions_derive_result_type_facts() {
        let schema = schema_with_array_columns();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT array_append(vals, 1), array_prepend('x', matrix), \
                    array_set_element(vals, 1, 2), array_cat(vals, vals), \
                    array_remove(matrix, 'x'), array_slice(matrix, 1, 2), \
                    string_to_array('a,b', ','), array_length(vals), \
                    array_position(vals, 1), array_contains(vals, 1), \
                    array_overlap(vals, vals), array_contains_all(vals, vals), \
                    array_to_string(vals, ','), \
                    array_element(array_append(vals, 3), 1), \
                    array_remove(?1, 1) \
             FROM arrays",
        )
        .expect("array utility functions bind");
        document
            .validate()
            .expect("typed array calls produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;

        for output in &outputs[..13] {
            let Expr::Function(call) = &output.expr else {
                panic!("array utility remains a resolved function call");
            };
            assert!(matches!(call.operation, FunctionOperation::Ordinary));
            assert_eq!(output.type_fact, call.result_type);
        }

        for index in [0, 2, 3] {
            assert_eq!(outputs[index].type_fact.storage, Some(Type::Blob));
            assert_eq!(outputs[index].type_fact.array_dimensions, 1);
            assert_eq!(
                outputs[index]
                    .type_fact
                    .declared
                    .as_ref()
                    .map(|declaration| declaration.name.as_str()),
                Some("INTEGER")
            );
        }
        for index in [1, 4, 5] {
            assert_eq!(outputs[index].type_fact.storage, Some(Type::Blob));
            assert_eq!(outputs[index].type_fact.array_dimensions, 2);
            assert_eq!(
                outputs[index]
                    .type_fact
                    .declared
                    .as_ref()
                    .map(|declaration| declaration.name.as_str()),
                Some("TEXT")
            );
        }
        assert_eq!(outputs[6].type_fact.storage, Some(Type::Blob));
        assert_eq!(outputs[6].type_fact.array_dimensions, 1);
        assert!(outputs[6].type_fact.declared.is_none());
        assert!(outputs[7..12]
            .iter()
            .all(|output| output.type_fact == TypeFact::known(Type::Integer)));
        assert_eq!(outputs[12].type_fact, TypeFact::known(Type::Text));

        let Expr::Subscript { base, .. } = &outputs[13].expr else {
            panic!("array_element() remains dedicated subscript HIR");
        };
        let Expr::Function(append) = base.as_ref() else {
            panic!("subscript keeps its typed array-producing call");
        };
        assert_eq!(append.result_type.array_dimensions, 1);
        assert_eq!(outputs[13].type_fact.storage, Some(Type::Integer));
        assert_eq!(outputs[13].type_fact.array_dimensions, 0);

        let Expr::Function(dynamic_remove) = &outputs[14].expr else {
            panic!("array_remove() remains a resolved function call");
        };
        assert_eq!(dynamic_remove.result_type.storage, Some(Type::Blob));
        assert_eq!(dynamic_remove.result_type.array_dimensions, 1);
        assert!(dynamic_remove.result_type.array_rank_unbounded);
    }

    #[test]
    fn custom_table_columns_keep_type_facts_and_transform_programs() {
        let schema = schema_with_custom_columns();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT scalar, nested, array_values FROM typed_values",
        )
        .expect("custom table columns bind");
        document
            .validate()
            .expect("custom table columns produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = document
            .source(block.from.as_ref().expect("query has FROM").first)
            .expect("custom table source exists");

        let scalar = &source.columns[0].type_fact;
        assert_eq!(scalar.storage, Some(Type::Integer));
        assert_eq!(scalar.array_dimensions, 0);
        let scalar_chain = &scalar
            .declared
            .as_ref()
            .expect("custom scalar keeps its declaration")
            .custom_chain;
        assert_eq!(scalar_chain.len(), 1);
        assert_eq!(scalar_chain[0].value().name, "scaled");
        assert_eq!(scalar_chain[0].snapshot(), document.snapshot);
        let scalar_programs = source.column_type_programs[0]
            .as_ref()
            .expect("custom scalar has transform programs");
        assert_eq!(scalar_programs.encode.len(), 1);
        assert_eq!(scalar_programs.decode.len(), 1);
        assert!(!scalar_programs.encode_nulls);
        for call in scalar_programs.encode.iter().chain(&scalar_programs.decode) {
            assert!(matches!(
                call.arguments.as_slice(),
                [Expr::Literal(ast::Literal::Numeric(value))] if value == "4"
            ));
        }

        let nested = &source.columns[1].type_fact;
        let nested_chain = &nested
            .declared
            .as_ref()
            .expect("custom chain keeps its declaration")
            .custom_chain;
        assert_eq!(
            nested_chain
                .iter()
                .map(|definition| definition.value().name.as_str())
                .collect::<Vec<_>>(),
            ["wrapped", "shifted"]
        );
        let nested_programs = source.column_type_programs[1]
            .as_ref()
            .expect("custom chain has transform programs");
        assert!(nested_programs.encode_nulls);
        let operators = nested_programs
            .encode
            .iter()
            .chain(&nested_programs.decode)
            .map(|call| {
                let program = document
                    .schema_program(call.program)
                    .expect("transform program exists");
                let Expr::Binary { operator, .. } = program.body else {
                    panic!("fixed transform is binary");
                };
                operator
            })
            .collect::<Vec<_>>();
        assert_eq!(operators, [ast::Operator::Add, ast::Operator::Subtract]);

        let array = &source.columns[2].type_fact;
        assert_eq!(array.storage, Some(Type::Blob));
        assert_eq!(array.array_dimensions, 1);
        let element = array
            .array_element()
            .expect("custom array has element facts");
        assert_eq!(element.storage, Some(Type::Integer));
        assert_eq!(element.array_dimensions, 0);
        assert_eq!(
            element
                .declared
                .as_ref()
                .and_then(|declared| declared.custom())
                .map(|definition| definition.value().name.as_str()),
            Some("scaled")
        );
        let array_programs = source.column_type_programs[2]
            .as_ref()
            .expect("custom array has element transform programs");
        assert!(!array_programs.encode_nulls);
        assert!(matches!(
            array_programs.encode[0].arguments.as_slice(),
            [Expr::Literal(ast::Literal::Numeric(value))] if value == "5"
        ));

        assert!(block
            .outputs
            .iter()
            .zip(&source.columns)
            .all(|(output, column)| output.type_fact == column.type_fact));
    }

    #[test]
    fn custom_type_facts_cross_query_boundaries_without_redecoding() {
        let schema = schema_with_custom_columns();
        for sql in [
            "WITH selected AS (SELECT scalar FROM typed_values) \
             SELECT scalar FROM selected",
            "SELECT scalar FROM (SELECT scalar FROM typed_values) AS selected",
        ] {
            let document = analyze_sql_with_schema(&schema, sql)
                .expect("custom type crosses a query boundary");
            document
                .validate()
                .expect("query output does not decode a custom value twice");

            let HirRoot::Query(root) = &document.root else {
                panic!("SELECT produces query root");
            };
            let block = &document.query(root.query).expect("query exists").blocks[0];
            let source = document
                .source(block.from.as_ref().expect("query has FROM").first)
                .expect("outer source exists");
            assert!(source.column_type_programs[0].is_none());
            assert_eq!(
                source.columns[0]
                    .type_fact
                    .declared
                    .as_ref()
                    .and_then(|declared| declared.custom())
                    .map(|definition| definition.value().name.as_str()),
                Some("scaled")
            );
            assert_eq!(block.outputs[0].type_fact, source.columns[0].type_fact);
        }
    }

    #[test]
    fn custom_binary_operators_freeze_functions_derivation_and_literal_encoding() {
        let schema = schema_with_custom_operators();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT a + b, a > b, a >= 7, 7 <= a, a != 7, a + 'x', a + c, c + 7, d < e \
             FROM custom_values",
        )
        .expect("custom operators bind");
        document
            .validate()
            .expect("custom operators produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        let custom = outputs
            .iter()
            .map(|output| {
                let Expr::Binary { custom, .. } = &output.expr else {
                    panic!("output remains binary HIR");
                };
                custom.as_ref()
            })
            .collect::<Vec<_>>();

        let direct = custom[0].expect("same-type columns use custom addition");
        assert!(matches!(
            direct.function.value(),
            Func::Scalar(ScalarFunc::NumericAdd)
        ));
        assert!(!direct.swap_args);
        assert!(!direct.negate);
        assert!(direct.literal_encoding.is_none());

        let greater = custom[1].expect("greater-than derives from less-than");
        assert!(matches!(
            greater.function.value(),
            Func::Scalar(ScalarFunc::NumericLt)
        ));
        assert!(greater.swap_args);
        assert!(!greater.negate);

        let greater_equal = custom[2].expect("greater-equal derives from less-than");
        assert!(!greater_equal.swap_args);
        assert!(greater_equal.negate);
        let encoding = greater_equal
            .literal_encoding
            .as_ref()
            .expect("right literal is encoded");
        assert_eq!(encoding.operand, BinaryOperand::Right);
        assert!(matches!(
            encoding
                .encoder
                .as_ref()
                .expect("amount has an encoder")
                .arguments
                .as_slice(),
            [Expr::Literal(ast::Literal::Numeric(value))] if value == "4"
        ));

        let reversed = custom[3].expect("left literal uses right custom column");
        assert!(reversed.swap_args);
        assert!(reversed.negate);
        assert_eq!(
            reversed
                .literal_encoding
                .as_ref()
                .expect("left literal is encoded")
                .operand,
            BinaryOperand::Left
        );

        let not_equal = custom[4].expect("not-equal derives from equality");
        assert!(matches!(
            not_equal.function.value(),
            Func::Scalar(ScalarFunc::NumericEq)
        ));
        assert!(!not_equal.swap_args);
        assert!(not_equal.negate);

        assert!(
            custom[5].is_none(),
            "incompatible literal uses normal addition"
        );
        assert!(
            custom[6].is_none(),
            "different custom types use normal addition"
        );

        let unencoded = custom[7].expect("compatible literal uses alternate addition");
        assert!(matches!(
            unencoded.function.value(),
            Func::Scalar(ScalarFunc::NumericAdd)
        ));
        assert!(unencoded
            .literal_encoding
            .as_ref()
            .expect("literal use is recorded")
            .encoder
            .is_none());

        assert!(
            custom[8].is_none(),
            "naked operator keeps normal comparison"
        );
    }

    #[test]
    fn insert_autoincrement_metadata_is_one_resolved_object() {
        for include_mvcc_sequence in [false, true] {
            let schema = schema_with_autoincrement(include_mvcc_sequence);
            let document =
                analyze_sql_with_schema(&schema, "INSERT INTO auto_items(value) VALUES ('kept')")
                    .expect("AUTOINCREMENT INSERT binds");
            document
                .validate()
                .expect("AUTOINCREMENT INSERT produces closed HIR");

            let HirRoot::Insert(insert) = &document.root else {
                panic!("INSERT produces INSERT root");
            };
            let hir::InsertTargetKind::BTree { autoincrement, .. } = &insert.target_kind else {
                panic!("AUTOINCREMENT INSERT has a B-tree target");
            };
            let autoincrement = autoincrement
                .as_ref()
                .expect("AUTOINCREMENT target carries resolved metadata");
            assert_eq!(
                autoincrement.sqlite_sequence.value().get_name(),
                "sqlite_sequence"
            );
            assert_eq!(
                autoincrement.sqlite_sequence.database(),
                document
                    .source(insert.target)
                    .expect("target source exists")
                    .database
            );
            assert_eq!(autoincrement.mvcc_sequence.is_some(), include_mvcc_sequence);
            if let Some(sequence) = &autoincrement.mvcc_sequence {
                assert_eq!(
                    sequence.kind,
                    crate::translate::semantic::hir::SequenceOperationKind::NextValue
                );
                assert_eq!(
                    sequence.sqlite_sequence.as_ref(),
                    Some(&autoincrement.sqlite_sequence)
                );
            }
        }
    }

    #[test]
    fn insert_autoincrement_requires_sqlite_sequence() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE auto_items(id INTEGER PRIMARY KEY AUTOINCREMENT)",
                    2,
                )
                .expect("AUTOINCREMENT table parses"),
            ))
            .expect("AUTOINCREMENT table name is unique");
        let error = analyze_sql_with_schema(&schema, "INSERT INTO auto_items DEFAULT VALUES")
            .expect_err("missing sqlite_sequence is corrupt catalog state");
        assert_eq!(
            error.to_string(),
            "Corrupt database: missing sqlite_sequence table"
        );
    }

    #[test]
    fn trigger_predicates_bind_event_row_sources() {
        let schema = schema_with_writable_table();
        for (event, row, kind) in [
            (ast::TriggerEvent::Insert, "new", PseudoSource::New),
            (ast::TriggerEvent::Update, "new", PseudoSource::New),
            (ast::TriggerEvent::Update, "old", PseudoSource::Old),
            (ast::TriggerEvent::Delete, "old", PseudoSource::Old),
        ] {
            let document = analyze_trigger_predicate_with_schema(
                &schema,
                event,
                &trigger_column(row, "value"),
            )
            .expect("event row is visible");
            let root = trigger_root(&document);
            let hir::TriggerBody::Predicate(expression) = &root.body else {
                panic!("trigger root contains predicate");
            };
            let expected_source = match kind {
                PseudoSource::New => root.environment.new_source,
                PseudoSource::Old => root.environment.old_source,
                PseudoSource::Excluded => unreachable!("EXCLUDED is not a trigger row"),
            }
            .expect("event exposes requested row");
            assert!(matches!(
                expression,
                Expr::Column(column)
                    if column.source == expected_source && column.column == 1
            ));
            let source = document
                .source(expected_source)
                .expect("trigger row source exists");
            assert!(matches!(
                source.kind,
                SourceKind::Pseudo { kind: actual, .. } if actual == kind
            ));
            assert_eq!(source.columns[1].type_fact.storage, Some(Type::Text));
        }
    }

    #[test]
    fn trigger_predicate_rows_are_qualified_only_and_event_checked() {
        let schema = schema_with_writable_table();
        let bare = analyze_trigger_predicate_with_schema(
            &schema,
            ast::TriggerEvent::Update,
            &ast::Expr::Id(ast::Name::exact("value".to_string())),
        )
        .expect_err("bare trigger column is not visible");
        assert_eq!(bare.to_string(), "Parse error: no such column: value");

        for (event, row, expected) in [
            (
                ast::TriggerEvent::Insert,
                "old",
                "Parse error: OLD references are only valid in UPDATE and DELETE triggers",
            ),
            (
                ast::TriggerEvent::Delete,
                "new",
                "Parse error: NEW references are only valid in INSERT and UPDATE triggers",
            ),
        ] {
            let error = analyze_trigger_predicate_with_schema(
                &schema,
                event,
                &trigger_column(row, "value"),
            )
            .expect_err("event rejects unavailable trigger row");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn trigger_predicates_bind_rowid_and_report_missing_columns() {
        let schema = schema_with_writable_table();
        let document = analyze_trigger_predicate_with_schema(
            &schema,
            ast::TriggerEvent::Insert,
            &trigger_column("new", "rowid"),
        )
        .expect("trigger rowid binds");
        let root = trigger_root(&document);
        let hir::TriggerBody::Predicate(expression) = &root.body else {
            panic!("trigger root contains predicate");
        };
        assert!(matches!(
            expression,
            Expr::RowId(source) if Some(*source) == root.environment.new_source
        ));

        let missing = analyze_trigger_predicate_with_schema(
            &schema,
            ast::TriggerEvent::Update,
            &trigger_column("new", "absent"),
        )
        .expect_err("missing trigger column fails");
        assert_eq!(
            missing.to_string(),
            "Parse error: no such column: new.absent"
        );
    }

    #[test]
    fn trigger_expression_policies_reach_nested_select_clauses() {
        let schema = schema_with_writable_table();
        let expression = first_result_expression(
            "SELECT EXISTS(SELECT RAISE(IGNORE) WHERE new.value IS NOT NULL ORDER BY new.id)",
        );
        analyze_trigger_predicate_with_schema(&schema, ast::TriggerEvent::Update, &expression)
            .expect("nested SELECT inherits trigger rows and RAISE policy");

        let unavailable = first_result_expression("SELECT EXISTS(SELECT old.value)");
        let error =
            analyze_trigger_predicate_with_schema(&schema, ast::TriggerEvent::Insert, &unavailable)
                .expect_err("nested SELECT inherits unavailable OLD rule");
        assert_eq!(
            error.to_string(),
            "Parse error: OLD references are only valid in UPDATE and DELETE triggers"
        );
    }

    #[test]
    fn trigger_select_binds_all_clauses_with_one_environment() {
        let schema = schema_with_writable_table();
        let ast::Stmt::Select(select) = parse_statement(
            "WITH c(v) AS (SELECT new.value) \
             SELECT v, old.rowid, RAISE(IGNORE), \
                    row_number() OVER (ORDER BY new.id) \
             FROM c \
             WHERE new.id IS NOT NULL \
             GROUP BY v \
             HAVING old.id IS NOT NULL \
             ORDER BY old.value \
             LIMIT new.id",
        ) else {
            panic!("statement is SELECT");
        };
        let document =
            analyze_trigger_select_with_schema(&schema, ast::TriggerEvent::Update, &select)
                .expect("trigger SELECT binds");
        let root = trigger_root(&document);
        let hir::TriggerBody::Command(hir::TriggerCommand::Select(query_id)) = &root.body else {
            panic!("trigger root contains SELECT command");
        };
        let environment = &root.environment;
        let new_source = environment.new_source.expect("UPDATE has NEW");
        let old_source = environment.old_source.expect("UPDATE has OLD");
        let query = document.query(*query_id).expect("trigger query exists");
        assert!(query.captures.contains(&new_source));
        assert!(query.captures.contains(&old_source));
        assert!(matches!(
            query.limit.as_ref().map(|limit| &limit.limit),
            Some(Expr::Column(column)) if column.source == new_source && column.column == 0
        ));
    }

    #[test]
    fn trigger_rows_are_reserved_inside_select_commands() {
        let schema = schema_with_writable_table();
        let ast::Stmt::Select(select) = parse_statement("SELECT new.value FROM writable AS new")
        else {
            panic!("statement is SELECT");
        };
        let document =
            analyze_trigger_select_with_schema(&schema, ast::TriggerEvent::Insert, &select)
                .expect("trigger NEW shadows a table alias");
        let root = trigger_root(&document);
        let hir::TriggerBody::Command(hir::TriggerCommand::Select(query_id)) = &root.body else {
            panic!("trigger root contains SELECT command");
        };
        let new_source = root.environment.new_source.expect("INSERT has NEW");
        let query = document.query(*query_id).expect("trigger query exists");
        assert!(matches!(
            query.blocks[0].outputs[0].expr,
            Expr::Column(column) if column.source == new_source && column.column == 1
        ));

        let ast::Stmt::Select(select) = parse_statement("SELECT 1 LIMIT old.id") else {
            panic!("statement is SELECT");
        };
        let error = analyze_trigger_select_with_schema(&schema, ast::TriggerEvent::Insert, &select)
            .expect_err("INSERT trigger SELECT rejects OLD in LIMIT");
        assert_eq!(
            error.to_string(),
            "Parse error: OLD references are only valid in UPDATE and DELETE triggers"
        );
    }

    #[test]
    fn trigger_insert_binds_source_query_and_conflict_policy() {
        let schema = schema_with_writable_table();
        let command = first_trigger_command(
            "CREATE TRIGGER copy_row AFTER UPDATE ON writable BEGIN \
             INSERT OR IGNORE INTO writable(id, value) \
             SELECT new.id, old.value; END",
        );
        let document = analyze_trigger_insert_with_schema(
            &schema,
            ast::TriggerEvent::Update,
            &command,
            Some(ast::ResolveType::Replace),
        )
        .expect("trigger INSERT binds");
        let root = trigger_root(&document);
        let hir::TriggerBody::Command(hir::TriggerCommand::Insert(insert)) = &root.body else {
            panic!("trigger root contains INSERT command");
        };
        assert_eq!(insert.conflict, Some(ast::ResolveType::Replace));
        let environment = &root.environment;
        let new_source = environment.new_source.expect("UPDATE has NEW");
        let old_source = environment.old_source.expect("UPDATE has OLD");
        let InsertSource::Query(query) = insert.source else {
            panic!("trigger INSERT uses a query source");
        };
        let query = document.query(query).expect("source query exists");
        assert_eq!(query.captures, [new_source, old_source]);
        let target = document
            .source(insert.target)
            .expect("target source exists");
        assert_eq!(target.database, Some(DatabaseId::new(MAIN_DB_ID)));
        assert_eq!(target.name, "writable");
    }

    #[test]
    fn trigger_insert_binds_values_and_upsert_against_trigger_rows() {
        let schema = schema_with_writable_table();
        let command = first_trigger_command(
            "CREATE TRIGGER copy_row AFTER UPDATE ON writable BEGIN \
             INSERT INTO writable(id, value) VALUES (new.id, old.value) \
             ON CONFLICT(id) DO UPDATE SET value = new.value WHERE old.id > 0; END",
        );
        let document =
            analyze_trigger_insert_with_schema(&schema, ast::TriggerEvent::Update, &command, None)
                .expect("trigger VALUES and UPSERT bind");
        let root = trigger_root(&document);
        let hir::TriggerBody::Command(hir::TriggerCommand::Insert(insert)) = &root.body else {
            panic!("trigger root contains INSERT command");
        };
        let environment = &root.environment;
        let new_source = environment.new_source.expect("UPDATE has NEW");
        let old_source = environment.old_source.expect("UPDATE has OLD");
        let InsertSource::Values(rows) = &insert.source else {
            panic!("trigger INSERT uses VALUES");
        };
        assert!(matches!(rows[0][0], Expr::Column(column) if column.source == new_source));
        assert!(matches!(rows[0][1], Expr::Column(column) if column.source == old_source));
        let hir::UpsertAction::Update {
            assignments,
            predicate,
        } = &insert.upserts[0].action
        else {
            panic!("UPSERT updates");
        };
        assert!(
            matches!(assignments[0].value, Expr::Column(column) if column.source == new_source)
        );
        assert!(
            matches!(predicate, Some(Expr::Binary { lhs, .. }) if matches!(lhs.as_ref(), Expr::Column(column) if column.source == old_source))
        );

        let invalid = first_trigger_command(
            "CREATE TRIGGER bad_row AFTER INSERT ON writable BEGIN \
             INSERT INTO writable(id) VALUES (old.id); END",
        );
        let error =
            analyze_trigger_insert_with_schema(&schema, ast::TriggerEvent::Insert, &invalid, None)
                .expect_err("INSERT trigger rejects OLD");
        assert_eq!(
            error.to_string(),
            "Parse error: OLD references are only valid in UPDATE and DELETE triggers"
        );
    }

    #[test]
    fn trigger_update_keeps_outer_and_changed_rows_distinct() {
        let schema = schema_with_writable_table();
        let command = first_trigger_command(
            "CREATE TRIGGER change_row AFTER UPDATE ON writable BEGIN \
             UPDATE OR IGNORE writable SET value = new.value \
             WHERE writable.id = old.id AND new.id > 0; END",
        );
        let document = analyze_trigger_update_with_schema(
            &schema,
            ast::TriggerEvent::Update,
            &command,
            Some(ast::ResolveType::Replace),
        )
        .expect("trigger UPDATE binds");
        let root = trigger_root(&document);
        let hir::TriggerBody::Command(hir::TriggerCommand::Update(update)) = &root.body else {
            panic!("trigger root contains UPDATE command");
        };
        assert_eq!(update.conflict, Some(ast::ResolveType::Replace));
        let environment = &root.environment;
        let outer_new = environment.new_source.expect("outer UPDATE has NEW");
        let outer_old = environment.old_source.expect("outer UPDATE has OLD");
        assert_ne!(update.target, outer_old);
        assert_ne!(update.new_source, outer_new);
        assert!(matches!(
            update.assignments[0].value,
            Expr::Column(column) if column.source == outer_new
        ));
        let predicate = update.predicate.as_ref().expect("UPDATE has predicate");
        let mut reads = Vec::new();
        predicate.walk(&mut |expression| {
            if let Expr::Column(column) = expression {
                reads.push(column.source);
            }
        });
        assert!(reads.contains(&update.target));
        assert!(reads.contains(&outer_old));
        assert!(reads.contains(&outer_new));
        let target = document
            .source(update.target)
            .expect("target source exists");
        assert_eq!(target.database, Some(DatabaseId::new(MAIN_DB_ID)));
    }

    #[test]
    fn trigger_update_carries_rows_through_from_and_subqueries() {
        let schema = schema_with_writable_table();
        let command = first_trigger_command(
            "CREATE TRIGGER change_row AFTER UPDATE ON writable BEGIN \
             UPDATE writable SET value = (SELECT new.value) \
             FROM (SELECT old.id AS id) AS chosen \
             WHERE writable.id = chosen.id; END",
        );
        let document =
            analyze_trigger_update_with_schema(&schema, ast::TriggerEvent::Update, &command, None)
                .expect("trigger UPDATE FROM binds");
        let root = trigger_root(&document);
        let hir::TriggerBody::Command(hir::TriggerCommand::Update(update)) = &root.body else {
            panic!("trigger root contains UPDATE command");
        };
        let environment = &root.environment;
        let outer_new = environment.new_source.expect("UPDATE has NEW");
        let outer_old = environment.old_source.expect("UPDATE has OLD");
        let Expr::Subquery(hir::SubqueryExpr::Scalar { query, .. }) = &update.assignments[0].value
        else {
            panic!("assignment is scalar subquery");
        };
        assert!(document
            .query(*query)
            .expect("assignment query exists")
            .captures
            .contains(&outer_new));
        let from = update.from.as_ref().expect("UPDATE has FROM");
        let SourceKind::Derived(query) = &document
            .source(from.first)
            .expect("derived source exists")
            .kind
        else {
            panic!("FROM source is derived");
        };
        assert!(document
            .query(*query)
            .expect("derived query exists")
            .captures
            .contains(&outer_old));

        let invalid = first_trigger_command(
            "CREATE TRIGGER bad_row AFTER INSERT ON writable BEGIN \
             UPDATE writable SET value = old.value; END",
        );
        let error =
            analyze_trigger_update_with_schema(&schema, ast::TriggerEvent::Insert, &invalid, None)
                .expect_err("INSERT trigger rejects OLD");
        assert_eq!(
            error.to_string(),
            "Parse error: OLD references are only valid in UPDATE and DELETE triggers"
        );
    }

    #[test]
    fn trigger_delete_keeps_target_and_outer_rows_distinct() {
        let schema = schema_with_writable_table();
        let command = first_trigger_command(
            "CREATE TRIGGER remove_row AFTER UPDATE ON writable BEGIN \
             DELETE FROM writable \
             WHERE writable.id = old.id AND new.value IS NOT NULL; END",
        );
        let document =
            analyze_trigger_delete_with_schema(&schema, ast::TriggerEvent::Update, &command)
                .expect("trigger DELETE binds");
        let root = trigger_root(&document);
        let hir::TriggerBody::Command(hir::TriggerCommand::Delete(delete)) = &root.body else {
            panic!("trigger root contains DELETE command");
        };
        let environment = &root.environment;
        let outer_new = environment.new_source.expect("outer UPDATE has NEW");
        let outer_old = environment.old_source.expect("outer UPDATE has OLD");
        assert_ne!(delete.target, outer_old);
        let mut reads = Vec::new();
        delete
            .predicate
            .as_ref()
            .expect("DELETE has predicate")
            .walk(&mut |expression| {
                if let Expr::Column(column) = expression {
                    reads.push(column.source);
                }
            });
        assert!(reads.contains(&delete.target));
        assert!(reads.contains(&outer_old));
        assert!(reads.contains(&outer_new));
        let target = document
            .source(delete.target)
            .expect("target source exists");
        assert_eq!(target.database, Some(DatabaseId::new(MAIN_DB_ID)));
    }

    #[test]
    fn trigger_delete_predicate_subqueries_keep_trigger_policy() {
        let schema = schema_with_writable_table();
        let command = first_trigger_command(
            "CREATE TRIGGER remove_row AFTER DELETE ON writable BEGIN \
             DELETE FROM writable WHERE id IN (SELECT old.id); END",
        );
        let document =
            analyze_trigger_delete_with_schema(&schema, ast::TriggerEvent::Delete, &command)
                .expect("trigger DELETE subquery binds");
        let root = trigger_root(&document);
        let hir::TriggerBody::Command(hir::TriggerCommand::Delete(delete)) = &root.body else {
            panic!("trigger root contains DELETE command");
        };
        let outer_old = root.environment.old_source.expect("DELETE trigger has OLD");
        let mut queries = Vec::new();
        delete
            .predicate
            .as_ref()
            .expect("DELETE has predicate")
            .walk(&mut |expression| {
                if let Expr::Subquery(hir::SubqueryExpr::In { query, .. }) = expression {
                    queries.push(*query);
                }
            });
        assert_eq!(queries.len(), 1);
        assert!(document
            .query(queries[0])
            .expect("predicate query exists")
            .captures
            .contains(&outer_old));

        let invalid = first_trigger_command(
            "CREATE TRIGGER bad_row AFTER DELETE ON writable BEGIN \
             DELETE FROM writable WHERE new.id > 0; END",
        );
        let error =
            analyze_trigger_delete_with_schema(&schema, ast::TriggerEvent::Delete, &invalid)
                .expect_err("DELETE trigger rejects NEW");
        assert_eq!(
            error.to_string(),
            "Parse error: NEW references are only valid in INSERT and UPDATE triggers"
        );
    }

    #[test]
    fn trigger_program_binds_predicate_and_ordered_commands_once() {
        let schema = schema_with_writable_table();
        let document = analyze_trigger_program_with_schema(
            &schema,
            "CREATE TRIGGER change_row AFTER UPDATE ON writable \
             WHEN new.id > old.id BEGIN \
             SELECT new.value; \
             INSERT OR IGNORE INTO writable(id, value) VALUES(new.id, old.value); \
             UPDATE OR FAIL writable SET value = new.value WHERE writable.id = old.id; \
             DELETE FROM writable WHERE writable.id = old.id; \
             END",
            Some(ast::ResolveType::Replace),
        )
        .expect("whole trigger program binds");
        let root = trigger_root(&document);
        let hir::TriggerBody::Program(program) = &root.body else {
            panic!("trigger root contains whole program");
        };
        let new_source = root.environment.new_source.expect("UPDATE has NEW");
        let old_source = root.environment.old_source.expect("UPDATE has OLD");

        let mut predicate_reads = Vec::new();
        program
            .predicate
            .as_ref()
            .expect("trigger has WHEN predicate")
            .walk(&mut |expression| {
                if let Expr::Column(column) = expression {
                    predicate_reads.push(column.source);
                }
            });
        assert!(predicate_reads.contains(&new_source));
        assert!(predicate_reads.contains(&old_source));

        let [
            hir::TriggerCommand::Select(query_id),
            hir::TriggerCommand::Insert(insert),
            hir::TriggerCommand::Update(update),
            hir::TriggerCommand::Delete(delete),
        ] = program.commands.as_slice()
        else {
            panic!("trigger commands preserve source order and shape");
        };

        let query = document.query(*query_id).expect("SELECT query exists");
        assert!(matches!(
            query.blocks[0].outputs[0].expr,
            Expr::Column(column) if column.source == new_source
        ));

        assert_eq!(insert.conflict, Some(ast::ResolveType::Replace));
        let InsertSource::Values(rows) = &insert.source else {
            panic!("INSERT command uses VALUES");
        };
        assert!(matches!(rows[0][0], Expr::Column(column) if column.source == new_source));
        assert!(matches!(rows[0][1], Expr::Column(column) if column.source == old_source));

        assert_eq!(update.conflict, Some(ast::ResolveType::Replace));
        assert!(matches!(
            update.assignments[0].value,
            Expr::Column(column) if column.source == new_source
        ));
        assert_ne!(update.target, old_source);

        assert_ne!(delete.target, old_source);
        let mut delete_reads = Vec::new();
        delete
            .predicate
            .as_ref()
            .expect("DELETE has predicate")
            .walk(&mut |expression| {
                if let Expr::Column(column) = expression {
                    delete_reads.push(column.source);
                }
            });
        assert!(delete_reads.contains(&delete.target));
        assert!(delete_reads.contains(&old_source));
    }

    #[test]
    fn insert_freezes_matching_insert_and_upsert_update_triggers() {
        let schema = schema_with_insert_triggers();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO writable(id, value) VALUES (1, 'new') \
             ON CONFLICT(id) DO UPDATE SET value = excluded.value",
        )
        .expect("triggered UPSERT binds");
        document
            .validate()
            .expect("triggered UPSERT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let hir::InsertTargetKind::BTree { triggers, .. } = &insert.target_kind else {
            panic!("triggered INSERT has a B-tree target");
        };
        assert_eq!(
            triggers
                .insert
                .iter()
                .map(|trigger| trigger.value().name.as_str())
                .collect::<Vec<_>>(),
            ["insert_after", "insert_before"]
        );
        assert_eq!(
            triggers
                .upsert_update
                .iter()
                .map(|trigger| trigger.value().name.as_str())
                .collect::<Vec<_>>(),
            ["update_value", "update_all"]
        );
        assert!(triggers
            .insert
            .iter()
            .chain(&triggers.upsert_update)
            .all(|trigger| trigger.database() == Some(DatabaseId::new(MAIN_DB_ID))));

        let plain = analyze_sql_with_schema(
            &schema,
            "INSERT INTO writable(id, value) VALUES (2, 'plain')",
        )
        .expect("triggered INSERT without UPSERT binds");
        let HirRoot::Insert(insert) = &plain.root else {
            panic!("INSERT produces INSERT root");
        };
        let hir::InsertTargetKind::BTree { triggers, .. } = &insert.target_kind else {
            panic!("triggered INSERT has a B-tree target");
        };
        assert_eq!(triggers.insert.len(), 2);
        assert!(triggers.upsert_update.is_empty());
    }

    #[test]
    fn insert_freezes_outgoing_and_incoming_foreign_keys() {
        let schema = schema_with_insert_foreign_keys();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO fk_items(id, parent_id, parent_code) VALUES (1, 2, 'two')",
        )
        .expect("foreign-key INSERT binds");
        document
            .validate()
            .expect("foreign-key INSERT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let target = document
            .source(insert.target)
            .expect("INSERT target source exists");
        let SourceKind::Table(target_table) = &target.kind else {
            panic!("INSERT target is a table");
        };
        let hir::InsertTargetKind::BTree { foreign_keys, .. } = &insert.target_kind else {
            panic!("foreign-key INSERT has a B-tree target");
        };
        assert_eq!(foreign_keys.outgoing.len(), 2);
        assert_eq!(foreign_keys.incoming.len(), 1);
        assert!(foreign_keys.outgoing.iter().all(|foreign_key| {
            foreign_key.child_source == insert.target
                && &foreign_key.child_table == target_table
                && foreign_key.parent_table.value().get_name() == "parents"
        }));

        let parent_id = foreign_keys
            .outgoing
            .iter()
            .find(|foreign_key| foreign_key.declaration.child_columns[0] == "parent_id")
            .expect("rowid parent foreign key is present");
        assert_eq!(parent_id.child_positions.as_ref(), [1]);
        assert_eq!(parent_id.parent_positions.as_ref(), [0]);
        assert!(parent_id.parent_uses_rowid);
        assert!(parent_id.parent_unique_index.is_none());

        let parent_code = foreign_keys
            .outgoing
            .iter()
            .find(|foreign_key| foreign_key.declaration.child_columns[0] == "parent_code")
            .expect("indexed parent foreign key is present");
        assert_eq!(parent_code.child_positions.as_ref(), [2]);
        assert_eq!(parent_code.parent_positions.as_ref(), [1]);
        assert!(!parent_code.parent_uses_rowid);
        assert_eq!(
            parent_code
                .parent_unique_index
                .as_ref()
                .expect("non-rowid parent key uses a UNIQUE index")
                .value()
                .name,
            "parents_code"
        );

        let incoming = &foreign_keys.incoming[0];
        assert_ne!(incoming.child_source, insert.target);
        assert_eq!(incoming.child_table.value().get_name(), "item_notes");
        assert_eq!(&incoming.parent_table, target_table);
        assert_eq!(incoming.child_positions.as_ref(), [1]);
        assert_eq!(incoming.parent_positions.as_ref(), [0]);
        assert!(incoming.parent_uses_rowid);
        assert!(!incoming.parent_action_guarantees_new_parent);
        let child_source = document
            .source(incoming.child_source)
            .expect("incoming child scan source exists");
        assert_eq!(child_source.name, "item_notes");
        assert!(matches!(
            &child_source.generated_expressions[1],
            ColumnReadExpression::Planned(hir::Expr::Binary { lhs, .. })
                if matches!(lhs.as_ref(), hir::Expr::Column(column)
                    if column.source == incoming.child_source && column.column == 0)
        ));
    }

    #[test]
    fn virtual_table_insert_has_only_virtual_target_metadata() {
        let schema = schema_with_virtual_insert_target();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO pragma_table_info(\
                rowid, cid, name, type, \"notnull\", dflt_value, pk\
             ) VALUES (9, 0, 'value', 'TEXT', 0, NULL, 0)",
        )
        .expect("virtual-table VALUES INSERT binds");
        document
            .validate()
            .expect("virtual-table INSERT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        assert!(matches!(insert.target_kind, hir::InsertTargetKind::Virtual));
        assert_eq!(insert.columns[0].column, TargetColumn::RowId);
        assert!(insert.upserts.is_empty());
        assert!(insert.excluded_source.is_none());
        let InsertSource::Values(rows) = &insert.source else {
            panic!("virtual-table VALUES stay inline");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 7);

        let target = document
            .source(insert.target)
            .expect("virtual INSERT target exists");
        assert!(matches!(
            &target.kind,
            SourceKind::Table(table) if table.value().virtual_table().is_some()
        ));
        assert!(target.check_constraints.is_none());
        assert!(matches!(target.index_coverage, IndexCoverage::Selective));

        let default_values =
            analyze_sql_with_schema(&schema, "INSERT INTO pragma_table_info DEFAULT VALUES")
                .expect("virtual-table DEFAULT VALUES binds");
        let HirRoot::Insert(insert) = &default_values.root else {
            panic!("INSERT produces INSERT root");
        };
        assert!(matches!(insert.target_kind, hir::InsertTargetKind::Virtual));
        assert!(matches!(insert.source, InsertSource::DefaultValues));
    }

    #[test]
    fn virtual_table_insert_keeps_values_only_rule() {
        let schema = schema_with_virtual_insert_target();
        for sql in [
            "INSERT INTO pragma_table_info(cid) SELECT 1",
            "INSERT INTO pragma_table_info(cid) VALUES (1) ON CONFLICT DO NOTHING",
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("unsupported virtual-table INSERT source fails");
            assert_eq!(
                error.to_string(),
                "Parse error: semantic INSERT does not yet accept UPSERT or non-VALUES sources for virtual-table targets"
            );
        }
    }

    #[test]
    fn insert_values_bind_target_columns_rows_and_complete_row_metadata() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO writable(value, id) VALUES ('first', 7), (\"second\", 8)",
        )
        .expect("plain multi-row INSERT binds");
        document.validate().expect("INSERT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        assert_eq!(insert.columns.len(), 2);
        assert_eq!(insert.columns[0].column, TargetColumn::Column(1));
        assert_eq!(insert.columns[1].column, TargetColumn::Column(0));
        assert!(insert.columns.iter().all(|target| target.uses_value));
        let hir::InsertTargetKind::BTree { defaults, .. } = &insert.target_kind else {
            panic!("ordinary INSERT has a B-tree target");
        };
        assert!(defaults.is_empty());
        let InsertSource::Values(rows) = &insert.source else {
            panic!("VALUES stays inline HIR");
        };
        assert_eq!(rows.len(), 2);
        assert!(matches!(
            rows[1].as_slice(),
            [Expr::Literal(ast::Literal::String(value)), Expr::Literal(ast::Literal::Numeric(id))]
                if value.trim_matches(|ch| ch == '\'' || ch == '"') == "second" && id == "8"
        ));

        let source = document
            .source(insert.target)
            .expect("target source exists");
        assert_eq!(source.owner, SourceOwner::Root);
        assert!(matches!(
            source.index_coverage,
            IndexCoverage::Complete { ref indexes } if indexes.is_empty()
        ));
        assert!(matches!(
            source.default_expressions[1],
            ColumnReadExpression::Planned(Expr::Literal(ast::Literal::String(ref value)))
                if value.trim_matches(|ch| ch == '\'' || ch == '"') == "fallback"
        ));
        assert!(matches!(
            source.generated_expressions[2],
            ColumnReadExpression::Planned(Expr::Binary {
                operator: ast::Operator::Multiply,
                ..
            })
        ));
    }

    #[test]
    fn insert_values_bind_root_owned_scalar_exists_and_in_queries() {
        let schema = schema_with_insert_metadata();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO guarded(id, value, score) VALUES (\
                 (SELECT id FROM guarded LIMIT 1),\
                 EXISTS(SELECT 1),\
                 1 IN (SELECT 1)\
             )",
        )
        .expect("VALUES subqueries bind");
        document
            .validate()
            .expect("VALUES subqueries produce closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Values(rows) = &insert.source else {
            panic!("VALUES stays inline HIR");
        };
        let queries = [
            match rows[0][0] {
                Expr::Subquery(SubqueryExpr::Scalar { query, output: 0 }) => query,
                _ => panic!("first value is scalar query"),
            },
            match rows[0][1] {
                Expr::Subquery(SubqueryExpr::Exists(query)) => query,
                _ => panic!("second value is EXISTS query"),
            },
            match rows[0][2] {
                Expr::Subquery(SubqueryExpr::In { query, .. }) => query,
                _ => panic!("third value is IN query"),
            },
        ];
        for query in queries {
            let query = document.query(query).expect("VALUES child query exists");
            assert_eq!(query.parent, None);
            assert!(query.captures.is_empty());
        }
        let scalar = document.query(queries[0]).expect("scalar query exists");
        let scalar_source = scalar.blocks[0]
            .from
            .as_ref()
            .expect("scalar query owns its FROM source")
            .first;
        assert_ne!(scalar_source, insert.target);
    }

    #[test]
    fn insert_values_subqueries_keep_scopeless_name_and_width_errors() {
        let schema = schema_with_insert_metadata();
        for (sql, expected) in [
            (
                "INSERT INTO guarded(id) VALUES ((SELECT guarded.id))",
                "Parse error: no such table: guarded",
            ),
            (
                "INSERT INTO guarded(id) VALUES ((SELECT 1, 2))",
                "Parse error: sub-select returns 2 columns - expected 1",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid subquery fails");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn compound_values_insert_sources_use_query_hir() {
        let schema = schema_with_insert_metadata();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO guarded(id) \
             VALUES (2) UNION ALL SELECT 1 ORDER BY 1 LIMIT 1",
        )
        .expect("compound decorated VALUES source binds");
        document
            .validate()
            .expect("compound VALUES INSERT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Query(query) = insert.source else {
            panic!("compound VALUES uses query source");
        };
        let query = document.query(query).expect("INSERT source query exists");
        assert_eq!(query.blocks.len(), 2);
        assert!(matches!(
            query.blocks[0].body,
            QueryBlockBody::Values { .. }
        ));
        assert!(matches!(
            query.blocks[1].body,
            QueryBlockBody::Select { .. }
        ));
        assert_eq!(query.order_by.len(), 1);
        assert!(query.limit.is_some());

        let plain = analyze_sql_with_schema(
            &schema,
            "INSERT INTO guarded(id) VALUES (1) UNION ALL VALUES (2)",
        )
        .expect("compound VALUES arms bind");
        let HirRoot::Insert(insert) = &plain.root else {
            panic!("INSERT produces INSERT root");
        };
        assert!(matches!(insert.source, InsertSource::Query(_)));
    }

    #[test]
    fn first_values_compound_arm_receives_insert_destination_type() {
        let schema = schema_with_insert_unions();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO union_values(nested) \
             VALUES (union_value('x', union_value('a', 1))) \
             UNION ALL SELECT union_value('y', 2.0) LIMIT 1",
        )
        .expect("destination types reach first VALUES compound arm");
        document
            .validate()
            .expect("typed compound VALUES produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Query(query) = insert.source else {
            panic!("compound VALUES uses query source");
        };
        let query = document.query(query).expect("INSERT source query exists");
        let QueryBlockBody::Values { rows } = &query.blocks[0].body else {
            panic!("first arm remains VALUES");
        };
        assert!(matches!(
            &rows[0][0],
            Expr::Function(function) if matches!(
                &function.operation,
                FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                    union_type,
                    tag_index: 0,
                }) if union_type.value().name == "outer_u"
            )
        ));
        assert!(matches!(
            &query.blocks[1].outputs[0].expr,
            Expr::Function(function) if matches!(
                &function.operation,
                FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                    union_type,
                    tag_index: 1,
                }) if union_type.value().name == "outer_u"
            )
        ));
    }

    #[test]
    fn query_backed_values_keep_query_diagnostics() {
        let schema = schema_with_insert_metadata();
        for (sql, expected) in [
            (
                "INSERT INTO guarded(id) VALUES (1) UNION ALL VALUES (2) ORDER BY 1",
                "Parse error: ORDER BY clause is not allowed with VALUES clause",
            ),
            (
                "INSERT INTO guarded(id) VALUES (1) UNION ALL SELECT 2, 3",
                "Parse error: SELECTs to the left and right of UNION ALL do not have the same number of result columns",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid VALUES fails");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn insert_freezes_check_constraints_and_all_index_expressions() {
        let schema = schema_with_insert_metadata();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO guarded(value, score) VALUES ('kept', 3)",
        )
        .expect("indexed checked INSERT binds");
        document
            .validate()
            .expect("INSERT metadata produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let source = document
            .source(insert.target)
            .expect("target source exists");
        let constraints = source
            .check_constraints
            .as_ref()
            .expect("INSERT enforces CHECK constraints");
        assert_eq!(constraints.len(), 2);
        assert_eq!(constraints[0].catalog_position, 0);
        assert_eq!(constraints[0].description, "positive_score");
        assert_eq!(constraints[1].catalog_position, 1);
        for constraint in constraints {
            let mut reads_target = false;
            constraint.expression.walk(&mut |expression| {
                if matches!(
                    expression,
                    Expr::Column(column) if column.source == insert.target
                ) {
                    reads_target = true;
                }
            });
            assert!(reads_target, "CHECK reads the INSERT row image");
        }

        assert_eq!(source.index_expressions.len(), 2);
        let expression_index = source
            .index_expressions
            .iter()
            .find(|metadata| metadata.index.value().name == "guarded_expression")
            .expect("expression index metadata exists");
        assert!(matches!(expression_index.columns.as_slice(), [Some(_)]));
        assert!(expression_index.predicate.is_some());
        let ordinary_index = source
            .index_expressions
            .iter()
            .find(|metadata| metadata.index.value().name == "guarded_value")
            .expect("ordinary index metadata exists");
        assert!(matches!(ordinary_index.columns.as_slice(), [None]));
        assert!(ordinary_index.predicate.is_none());

        let IndexCoverage::Complete { indexes } = &source.index_coverage else {
            panic!("INSERT carries a complete index summary");
        };
        assert_eq!(
            indexes,
            &source
                .index_expressions
                .iter()
                .map(|metadata| metadata.index.id())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn insert_select_keeps_query_hir_and_correlated_subquery_captures() {
        let schema = schema_with_insert_source();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO writable(value, id) \
             SELECT value, (SELECT id) FROM insert_source",
        )
        .expect("INSERT SELECT binds");
        document
            .validate()
            .expect("INSERT SELECT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Query(query_id) = insert.source else {
            panic!("SELECT source keeps its query identity");
        };
        let query = document.query(query_id).expect("INSERT query exists");
        assert!(query.parent.is_none());
        assert!(query.captures.is_empty());
        assert_eq!(query.output.len(), 2);
        let block = &query.blocks[0];
        let input = block.from.as_ref().expect("SELECT has FROM").first;
        let Expr::Subquery(SubqueryExpr::Scalar { query: inner, .. }) = &block.outputs[1].expr
        else {
            panic!("second output remains a scalar subquery");
        };
        assert_eq!(
            document.query(*inner).expect("inner query exists").captures,
            [input]
        );
    }

    #[test]
    fn insert_with_uses_lazy_and_recursive_cte_binding() {
        let schema = schema_with_insert_source();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH broken AS (SELECT absent), \
                  picked(value, id) AS (SELECT value, id FROM insert_source) \
             INSERT INTO writable(value, id) SELECT value, id FROM picked",
        )
        .expect("INSERT WITH binds referenced CTEs lazily");
        document
            .validate()
            .expect("INSERT WITH produces closed HIR");

        assert_eq!(document.ctes.len(), 1, "unused invalid CTE stays unbound");
        assert_eq!(document.ctes[0].name, "picked");
        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Query(query) = insert.source else {
            panic!("INSERT WITH retains source query");
        };
        assert_eq!(
            document
                .query(query)
                .expect("source query exists")
                .reachable_ctes,
            [document.ctes[0].id]
        );

        let recursive = analyze_sql_with_schema(
            &schema,
            "WITH RECURSIVE seq(x) AS (\
                 VALUES (1) UNION ALL SELECT x + 1 FROM seq WHERE x < 2\
             ) \
             INSERT INTO writable(id, value) SELECT x, 'recursive' FROM seq",
        )
        .expect("recursive INSERT CTE binds");
        recursive
            .validate()
            .expect("recursive INSERT CTE produces closed HIR");
        assert!(matches!(recursive.ctes[0].body, CteBody::Recursive(_)));
    }

    #[test]
    fn insert_keeps_statement_conflict_resolution() {
        let schema = schema_with_writable_table();
        for (keyword, expected) in [
            ("ROLLBACK", ast::ResolveType::Rollback),
            ("ABORT", ast::ResolveType::Abort),
            ("FAIL", ast::ResolveType::Fail),
            ("IGNORE", ast::ResolveType::Ignore),
            ("REPLACE", ast::ResolveType::Replace),
        ] {
            let document = analyze_sql_with_schema(
                &schema,
                &format!("INSERT OR {keyword} INTO writable(id) VALUES (1)"),
            )
            .expect("INSERT conflict mode binds");
            let HirRoot::Insert(insert) = document.root else {
                panic!("INSERT produces INSERT root");
            };
            assert_eq!(insert.conflict, Some(expected));
        }

        let document = analyze_sql_with_schema(&schema, "INSERT INTO writable(id) VALUES (1)")
            .expect("plain INSERT binds");
        let HirRoot::Insert(insert) = document.root else {
            panic!("INSERT produces INSERT root");
        };
        assert_eq!(insert.conflict, None);
    }

    #[test]
    fn insert_binds_catch_all_upsert_do_nothing() {
        let schema = schema_with_writable_table();
        for sql in [
            "INSERT INTO writable(id) VALUES (1) ON CONFLICT DO NOTHING",
            "INSERT INTO writable(id) SELECT 1 WHERE true ON CONFLICT DO NOTHING",
            "INSERT INTO writable(id) VALUES (1) ON CONFLICT DO NOTHING RETURNING id",
        ] {
            let document =
                analyze_sql_with_schema(&schema, sql).expect("catch-all UPSERT DO NOTHING binds");
            document
                .validate()
                .expect("UPSERT DO NOTHING produces closed HIR");
            let HirRoot::Insert(insert) = &document.root else {
                panic!("INSERT produces INSERT root");
            };
            assert!(matches!(
                insert.upserts.as_slice(),
                [hir::Upsert {
                    target: None,
                    action: hir::UpsertAction::Nothing,
                }]
            ));
            assert!(insert.excluded_source.is_none());
        }
    }

    #[test]
    fn insert_binds_ordered_pk_index_and_partial_upsert_targets() {
        let schema = schema_with_insert_metadata();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO guarded(id, value, score) VALUES (1, 'one', 2) \
             ON CONFLICT(id DESC) DO NOTHING \
             ON CONFLICT(value COLLATE binary DESC) DO NOTHING \
             ON CONFLICT(lower(value)) WHERE score > 1 DO NOTHING \
             ON CONFLICT DO NOTHING",
        )
        .expect("targeted UPSERT DO NOTHING clauses bind");
        document
            .validate()
            .expect("targeted UPSERT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        assert_eq!(insert.upserts.len(), 4);

        let primary = insert.upserts[0]
            .target
            .as_ref()
            .expect("first target is explicit");
        assert!(primary.matched_index.is_none());
        assert!(matches!(
            primary.terms.as_slice(),
            [hir::ConflictTerm {
                expr: Expr::Column(column),
                order: ast::SortOrder::Desc,
                ..
            }] if column.source == insert.target && column.column == 0
        ));

        let ordinary = insert.upserts[1]
            .target
            .as_ref()
            .expect("second target is explicit");
        let ordinary_index = ordinary
            .matched_index
            .as_ref()
            .expect("ordinary UNIQUE index is frozen");
        assert_eq!(ordinary_index.value().name, "guarded_value");
        assert_eq!(ordinary.terms[0].order, ast::SortOrder::Desc);
        assert!(ordinary.terms[0].collation.as_ref().is_some_and(
            |collation| collation.value() == &crate::translate::collate::CollationSeq::Binary
        ));

        let partial = insert.upserts[2]
            .target
            .as_ref()
            .expect("third target is explicit");
        let partial_index = partial
            .matched_index
            .as_ref()
            .expect("partial expression index is frozen");
        assert_eq!(partial_index.value().name, "guarded_expression");
        assert!(matches!(partial.terms[0].expr, Expr::Function(_)));
        assert!(matches!(partial.predicate, Some(Expr::Binary { .. })));

        assert!(insert.upserts[3].target.is_none());
        assert!(insert
            .upserts
            .iter()
            .all(|upsert| matches!(upsert.action, hir::UpsertAction::Nothing)));
        let source = document
            .source(insert.target)
            .expect("INSERT target source exists");
        for resolved in [ordinary_index, partial_index] {
            assert!(source
                .index_expressions
                .iter()
                .any(|metadata| metadata.index.id() == resolved.id()));
        }
    }

    #[test]
    fn insert_conflict_targets_keep_matching_diagnostics() {
        let schema = schema_with_insert_metadata();
        for (sql, expected) in [
            (
                "INSERT INTO guarded(id) VALUES (1) ON CONFLICT(missing) DO NOTHING",
                "Parse error: no such column: missing",
            ),
            (
                "INSERT INTO guarded(id) VALUES (1) ON CONFLICT(score) DO NOTHING",
                "Parse error: ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint",
            ),
            (
                "INSERT INTO guarded(id) VALUES (1) \
                 ON CONFLICT(value COLLATE nocase) DO NOTHING",
                "Parse error: ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint",
            ),
            (
                "INSERT INTO guarded(id) VALUES (1) \
                 ON CONFLICT(lower(value)) WHERE score > 2 DO NOTHING",
                "Parse error: ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint",
            ),
            (
                "INSERT INTO guarded(id) VALUES (1) \
                 ON CONFLICT(value NULLS LAST) DO NOTHING",
                "Parse error: unsupported use of NULLS LAST",
            ),
        ] {
            let error =
                analyze_sql_with_schema(&schema, sql).expect_err("invalid UPSERT target fails");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn insert_binds_upsert_update_against_target_and_excluded_rows() {
        let schema = schema_with_insert_metadata();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO guarded AS g(id, value, score) VALUES (1, 'one', 2) \
             ON CONFLICT(value) DO UPDATE \
             SET (value, score) = (excluded.value, score + 1), \
                 score = excluded.score + 2 \
             WHERE g.id = excluded.id \
             ON CONFLICT DO NOTHING",
        )
        .expect("UPSERT DO UPDATE binds");
        document
            .validate()
            .expect("UPSERT DO UPDATE produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let excluded = insert
            .excluded_source
            .expect("DO UPDATE creates EXCLUDED source");
        let source = document.source(excluded).expect("EXCLUDED source exists");
        assert!(matches!(
            &source.kind,
            SourceKind::Pseudo {
                kind: hir::PseudoSource::Excluded,
                table,
            } if table.value().get_name() == "guarded"
        ));
        assert_eq!(source.owner, SourceOwner::Root);

        let hir::UpsertAction::Update {
            assignments,
            predicate,
        } = &insert.upserts[0].action
        else {
            panic!("first clause is DO UPDATE");
        };
        assert_eq!(assignments.len(), 2, "duplicate score keeps one assignment");
        assert_eq!(assignments[0].columns, [TargetColumn::Column(1)]);
        assert!(matches!(
            assignments[0].value,
            Expr::Column(column) if column.source == excluded && column.column == 1
        ));
        assert_eq!(assignments[1].columns, [TargetColumn::Column(2)]);
        assert!(matches!(
            &assignments[1].value,
            Expr::Binary { lhs, rhs, .. }
                if matches!(lhs.as_ref(), Expr::Column(column)
                    if column.source == excluded && column.column == 2)
                    && matches!(rhs.as_ref(), Expr::Literal(ast::Literal::Numeric(value))
                        if value == "2")
        ));
        assert!(matches!(
            predicate,
            Some(Expr::Binary { lhs, rhs, .. })
                if matches!(lhs.as_ref(), Expr::Column(column)
                    if column.source == insert.target && column.column == 0)
                    && matches!(rhs.as_ref(), Expr::Column(column)
                        if column.source == excluded && column.column == 0)
        ));
        assert!(matches!(
            insert.upserts[1],
            hir::Upsert {
                target: None,
                action: hir::UpsertAction::Nothing,
            }
        ));
    }

    #[test]
    fn upsert_update_assignment_uses_destination_type() {
        let schema = schema_with_insert_unions();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO union_values VALUES (1, union_value('y', 1.0), union_value('red', 'r')) \
             ON CONFLICT(id) DO UPDATE SET \
             nested = union_value('x', union_value('b', 'updated'))",
        )
        .expect("DO UPDATE assignment receives destination type");
        document
            .validate()
            .expect("typed DO UPDATE produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let hir::UpsertAction::Update { assignments, .. } = &insert.upserts[0].action else {
            panic!("UPSERT action is UPDATE");
        };
        assert!(matches!(
            &assignments[0].value,
            Expr::Function(function) if matches!(
                &function.operation,
                FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                    union_type,
                    tag_index: 0,
                }) if union_type.value().name == "outer_u"
            )
        ));
    }

    #[test]
    fn upsert_target_named_excluded_shadows_the_pseudo_source() {
        let mut schema = Schema::new();
        let table = BTreeTable::from_sql(
            "CREATE TABLE excluded(id INTEGER PRIMARY KEY, value TEXT)",
            2,
        )
        .expect("table named excluded parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("table named excluded is unique");
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO excluded(id, value) VALUES (1, 'new') \
             ON CONFLICT DO UPDATE SET value = excluded.value",
        )
        .expect("target qualifier shadows outer EXCLUDED scope");
        document
            .validate()
            .expect("shadowed EXCLUDED source produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let hir::UpsertAction::Update { assignments, .. } = &insert.upserts[0].action else {
            panic!("UPSERT action is UPDATE");
        };
        assert!(matches!(
            assignments[0].value,
            Expr::Column(column) if column.source == insert.target && column.column == 1
        ));
    }

    #[test]
    fn upsert_update_keeps_assignment_diagnostics() {
        let schema = schema_with_writable_table();
        for (sql, expected) in [
            (
                "INSERT INTO writable(id) VALUES (1) \
                 ON CONFLICT DO UPDATE SET missing = 1",
                "Parse error: no such column: missing",
            ),
            (
                "INSERT INTO writable(id) VALUES (1) \
                 ON CONFLICT DO UPDATE SET doubled = 1",
                "Parse error: cannot UPDATE generated column \"doubled\"",
            ),
            (
                "INSERT INTO writable(id) VALUES (1) \
                 ON CONFLICT DO UPDATE SET (id, value) = (1)",
                "Parse error: 2 columns assigned 1 values",
            ),
            (
                "INSERT INTO writable(id) VALUES (1) \
                 ON CONFLICT DO UPDATE SET value = (SELECT value)",
                "Parse error: Subquery is not supported in this position",
            ),
            (
                "INSERT INTO writable(id) VALUES (1) \
                 ON CONFLICT DO UPDATE SET value = excluded.missing",
                "Parse error: no such column: excluded.missing",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid UPDATE fails");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn insert_values_resolve_union_value_from_each_destination_type() {
        let schema = schema_with_insert_unions();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO union_values VALUES (\
                 1,\
                 union_value('x', union_value('a', 42)),\
                 union_value('red', 'crimson')\
             )",
        )
        .expect("destination-aware union values bind");
        document
            .validate()
            .expect("union-value INSERT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Values(rows) = &insert.source else {
            panic!("INSERT keeps VALUES rows");
        };
        let Expr::Function(outer) = &rows[0][1] else {
            panic!("outer union value is a function");
        };
        let FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
            union_type,
            tag_index,
        }) = &outer.operation
        else {
            panic!("outer union value has resolved operation");
        };
        assert_eq!(union_type.value().name, "outer_u");
        assert_eq!(*tag_index, 0);
        assert_eq!(
            outer
                .result_type
                .declared
                .as_ref()
                .expect("outer result has declared type")
                .name,
            "outer_u"
        );
        let FunctionArguments::Expressions { values, .. } = &outer.arguments else {
            panic!("union value has ordinary arguments");
        };
        let Expr::Function(inner) = &values[1] else {
            panic!("selected variant contains nested union value");
        };
        assert!(matches!(
            &inner.operation,
            FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                union_type,
                tag_index: 0,
            }) if union_type.value().name == "inner_u"
        ));

        let Expr::Function(color) = &rows[0][2] else {
            panic!("color union value is a function");
        };
        assert!(matches!(
            &color.operation,
            FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                union_type,
                tag_index: 0,
            }) if union_type.value().name == "color_u"
        ));
    }

    #[test]
    fn insert_select_resolves_union_values_in_every_compound_arm() {
        let schema = schema_with_insert_unions();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO union_values(nested, color) \
             SELECT union_value('x', union_value('b', 'deep')), \
                    union_value('blue', 'navy') \
             UNION ALL \
             VALUES (union_value('y', 1.5), union_value('red', 'scarlet'))",
        )
        .expect("destination types reach every INSERT-SELECT arm");
        document
            .validate()
            .expect("compound union-value INSERT produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Query(query) = insert.source else {
            panic!("INSERT keeps query source");
        };
        let query = document.query(query).expect("INSERT source query exists");
        assert_eq!(query.blocks.len(), 2);

        let Expr::Function(outer) = &query.blocks[0].outputs[0].expr else {
            panic!("first arm resolves outer union value");
        };
        assert!(matches!(
            &outer.operation,
            FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                union_type,
                tag_index: 0,
            }) if union_type.value().name == "outer_u"
        ));
        let FunctionArguments::Expressions { values, .. } = &outer.arguments else {
            panic!("union value has ordinary arguments");
        };
        assert!(matches!(
            &values[1],
            Expr::Function(function) if matches!(
                &function.operation,
                FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                    union_type,
                    tag_index: 1,
                }) if union_type.value().name == "inner_u"
            )
        ));

        let QueryBlockBody::Values { rows } = &query.blocks[1].body else {
            panic!("second arm remains VALUES");
        };
        assert!(matches!(
            &rows[0][0],
            Expr::Function(function) if matches!(
                &function.operation,
                FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                    union_type,
                    tag_index: 1,
                }) if union_type.value().name == "outer_u"
            )
        ));
        assert!(matches!(
            &rows[0][1],
            Expr::Function(function) if matches!(
                &function.operation,
                FunctionOperation::CustomType(CustomTypeOperation::UnionValue {
                    union_type,
                    tag_index: 0,
                }) if union_type.value().name == "color_u"
            )
        ));
    }

    #[test]
    fn insert_union_value_keeps_destination_errors() {
        let schema = schema_with_insert_unions();
        for (sql, expected) in [
            (
                "INSERT INTO union_values(id) VALUES (union_value('x', 1))",
                "Parse error: union_value() can only be used in INSERT/UPDATE targeting a union-typed column",
            ),
            (
                "INSERT INTO union_values(nested) VALUES (union_value('red', 1))",
                "Parse error: unknown variant 'red' in union type 'outer_u'",
            ),
            (
                "INSERT INTO union_values(nested) VALUES (union_value(1, 2))",
                "Parse error: union_value() first argument must be a string literal",
            ),
            (
                "INSERT INTO union_values(nested) SELECT union_value('red', 1)",
                "Parse error: unknown variant 'red' in union type 'outer_u'",
            ),
        ] {
            let error =
                analyze_sql_with_schema(&schema, sql).expect_err("invalid union value fails");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn insert_returning_builds_root_outputs_from_the_target_scope() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO writable(id, value) VALUES (3, 'hello') \
             RETURNING *, value COLLATE nocase AS folded",
        )
        .expect("scalar RETURNING binds");
        document
            .validate()
            .expect("INSERT RETURNING produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let returning = insert.returning.as_ref().expect("RETURNING is preserved");
        assert_eq!(
            returning
                .outputs
                .iter()
                .map(|output| (output.id, output.name.as_str(), output.name_kind))
                .collect::<Vec<_>>(),
            [
                (OutputId::root(0), "id", OutputNameKind::StarExpansion),
                (OutputId::root(1), "value", OutputNameKind::StarExpansion),
                (OutputId::root(2), "doubled", OutputNameKind::StarExpansion),
                (OutputId::root(3), "folded", OutputNameKind::ExplicitAlias),
            ]
        );
        assert!(matches!(
            &returning.outputs[0].expr,
            Expr::Column(column) if column.source == insert.target && column.column == 0
        ));
        assert!(matches!(
            &returning.outputs[3].expr,
            Expr::Collate { expr, .. }
                if matches!(expr.as_ref(), Expr::Column(column)
                    if column.source == insert.target && column.column == 1)
        ));
        assert_eq!(returning.outputs[0].type_fact.storage, Some(Type::Integer));
        assert_eq!(returning.outputs[1].type_fact.storage, Some(Type::Text));
        assert!(returning.outputs[3].collation_is_explicit);
        assert!(document.output(OutputId::root(3)).is_some());

        let target = document
            .source(insert.target)
            .expect("INSERT target source exists");
        assert!(matches!(
            target.generated_expressions[2],
            ColumnReadExpression::Planned(_)
        ));

        let qualified = analyze_sql_with_schema(
            &schema,
            "INSERT INTO writable(id) VALUES (4) RETURNING writable.*",
        )
        .expect("qualified RETURNING star binds");
        let HirRoot::Insert(insert) = &qualified.root else {
            panic!("INSERT produces INSERT root");
        };
        assert_eq!(
            insert
                .returning
                .as_ref()
                .expect("qualified star is preserved")
                .outputs
                .len(),
            3
        );
    }

    #[test]
    fn insert_returning_subqueries_are_root_owned_and_capture_the_target() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "INSERT INTO writable(id, value) VALUES (3, 'hello') RETURNING \
             (SELECT value) AS copied, \
             EXISTS(SELECT 1 WHERE id = 3) AS found, \
             id IN (SELECT id) AS matched",
        )
        .expect("RETURNING subqueries bind");
        document
            .validate()
            .expect("root-owned RETURNING subqueries produce closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let outputs = &insert
            .returning
            .as_ref()
            .expect("RETURNING is preserved")
            .outputs;
        let query_ids = [
            match outputs[0].expr {
                Expr::Subquery(SubqueryExpr::Scalar { query, output: 0 }) => query,
                _ => panic!("first output is scalar subquery"),
            },
            match outputs[1].expr {
                Expr::Subquery(SubqueryExpr::Exists(query)) => query,
                _ => panic!("second output is EXISTS subquery"),
            },
            match outputs[2].expr {
                Expr::Subquery(SubqueryExpr::In { query, .. }) => query,
                _ => panic!("third output is IN subquery"),
            },
        ];
        for query in query_ids {
            let query = document.query(query).expect("RETURNING child query exists");
            assert_eq!(query.parent, None);
            assert_eq!(query.captures, [insert.target]);
        }
    }

    #[test]
    fn insert_returning_subqueries_share_the_insert_with_scope() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH label(v) AS (SELECT 'from cte') \
             INSERT INTO writable(id) SELECT 3 \
             RETURNING (SELECT v FROM label)",
        )
        .expect("RETURNING subquery sees INSERT CTEs");
        document
            .validate()
            .expect("RETURNING CTE reference produces closed HIR");

        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = insert
            .returning
            .as_ref()
            .expect("RETURNING is preserved")
            .outputs[0]
            .expr
        else {
            panic!("RETURNING output is a scalar subquery");
        };
        let query = document.query(query).expect("RETURNING query exists");
        assert_eq!(query.parent, None);
        assert_eq!(query.reachable_ctes.len(), 1);
    }

    #[test]
    fn insert_with_scope_is_shared_by_inline_values_and_returning() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH label(v) AS (VALUES ('from cte')) \
             INSERT INTO writable(id, value) \
             VALUES (3, (SELECT v FROM label)) \
             RETURNING (SELECT v FROM label)",
        )
        .expect("inline VALUES and RETURNING see the INSERT CTE scope");
        document
            .validate()
            .expect("shared INSERT CTE scope produces closed HIR");

        assert_eq!(document.ctes.len(), 1);
        let cte = document.ctes[0].id;
        let HirRoot::Insert(insert) = &document.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Values(rows) = &insert.source else {
            panic!("simple VALUES stays inline HIR");
        };
        let Expr::Subquery(SubqueryExpr::Scalar {
            query: values_query,
            ..
        }) = rows[0][1]
        else {
            panic!("VALUES expression is a scalar subquery");
        };
        let Expr::Subquery(SubqueryExpr::Scalar {
            query: returning_query,
            ..
        }) = insert
            .returning
            .as_ref()
            .expect("RETURNING is preserved")
            .outputs[0]
            .expr
        else {
            panic!("RETURNING expression is a scalar subquery");
        };
        assert_eq!(
            document
                .query(values_query)
                .expect("VALUES query exists")
                .reachable_ctes,
            [cte]
        );
        assert_eq!(
            document
                .query(returning_query)
                .expect("RETURNING query exists")
                .reachable_ctes,
            [cte]
        );
    }

    #[test]
    fn failed_statements_do_not_leak_cte_scopes() {
        let schema = schema_with_writable_table();
        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            &schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        let mut analyzer = Analyzer::new(&context);

        let select = parse_statement("WITH broken AS (SELECT missing) SELECT * FROM broken");
        let ast::Stmt::Select(select) = &select else {
            panic!("SQL contains SELECT");
        };
        analyzer
            .analyze_select(select)
            .expect_err("invalid CTE body fails SELECT analysis");
        assert!(analyzer.cte_scopes.is_empty(), "SELECT scope is removed");

        let insert = parse_statement(
            "WITH broken AS (SELECT missing) \
             INSERT INTO writable(id) VALUES ((SELECT * FROM broken))",
        );
        let ast::Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        } = &insert
        else {
            panic!("SQL contains INSERT");
        };
        analyzer
            .analyze_insert(
                with.as_ref(),
                *or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            )
            .expect_err("invalid CTE body fails INSERT analysis");
        assert!(analyzer.cte_scopes.is_empty(), "INSERT scope is removed");

        let update = parse_statement(
            "WITH broken AS (SELECT missing) \
             UPDATE writable SET value = (SELECT * FROM broken)",
        );
        let ast::Stmt::Update(update) = &update else {
            panic!("SQL contains UPDATE");
        };
        analyzer
            .analyze_update(update)
            .expect_err("invalid CTE body fails UPDATE analysis");
        assert!(analyzer.cte_scopes.is_empty(), "UPDATE scope is removed");

        let delete = parse_statement(
            "WITH broken AS (SELECT missing) \
             DELETE FROM writable WHERE id = (SELECT * FROM broken)",
        );
        let ast::Stmt::Delete {
            with,
            tbl_name,
            indexed,
            where_clause,
            returning,
        } = &delete
        else {
            panic!("SQL contains DELETE");
        };
        analyzer
            .analyze_delete(
                with.as_ref(),
                tbl_name,
                indexed.as_ref(),
                where_clause.as_deref(),
                returning,
            )
            .expect_err("invalid CTE body fails DELETE analysis");
        assert!(analyzer.cte_scopes.is_empty(), "DELETE scope is removed");
    }

    #[test]
    fn insert_returning_rejects_non_scalar_forms_at_analysis_time() {
        let schema = schema_with_writable_table();
        for (sql, expected) in [
            (
                "INSERT INTO writable(id) VALUES (1) RETURNING missing",
                "Parse error: no such column: missing",
            ),
            (
                "INSERT INTO writable(id) VALUES (1) RETURNING sum(id)",
                "Parse error: misuse of aggregate function sum()",
            ),
            (
                "INSERT INTO writable(id) VALUES (1) RETURNING row_number() OVER ()",
                "Parse error: misuse of window function: row_number()",
            ),
            (
                "INSERT INTO writable(id) VALUES (1) RETURNING (SELECT id, value)",
                "Parse error: sub-select returns 2 columns - expected 1",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid RETURNING fails");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn insert_defaults_and_duplicate_targets_keep_write_selection_rules() {
        let schema = schema_with_writable_table();
        let explicit_default =
            analyze_sql_with_schema(&schema, "INSERT INTO writable(value) VALUES (DEFAULT)")
                .expect("explicit DEFAULT binds");
        let HirRoot::Insert(insert) = &explicit_default.root else {
            panic!("INSERT produces INSERT root");
        };
        let InsertSource::Values(rows) = &insert.source else {
            panic!("VALUES stays inline HIR");
        };
        let hir::InsertTargetKind::BTree { defaults, .. } = &insert.target_kind else {
            panic!("ordinary INSERT has a B-tree target");
        };
        assert!(matches!(
            rows[0].as_slice(),
            [Expr::Literal(ast::Literal::String(value))]
                if value.trim_matches(|ch| ch == '\'' || ch == '"') == "fallback"
        ));
        assert!(matches!(
            defaults.as_slice(),
            [ResolvedDefault {
                column: 0,
                value: Expr::Literal(ast::Literal::Null),
            }]
        ));

        let default_values =
            analyze_sql_with_schema(&schema, "INSERT INTO writable DEFAULT VALUES")
                .expect("DEFAULT VALUES binds");
        let HirRoot::Insert(insert) = &default_values.root else {
            panic!("INSERT produces INSERT root");
        };
        assert!(insert.columns.is_empty());
        assert!(matches!(insert.source, InsertSource::DefaultValues));
        let hir::InsertTargetKind::BTree { defaults, .. } = &insert.target_kind else {
            panic!("ordinary INSERT has a B-tree target");
        };
        assert_eq!(defaults.len(), 2);
        assert_eq!(defaults[0].column, 0);
        assert_eq!(defaults[1].column, 1);

        let duplicates = analyze_sql_with_schema(
            &schema,
            "INSERT INTO writable(value, value, id, rowid) VALUES ('first', 'last', 1, 2)",
        )
        .expect("duplicate targets bind");
        let HirRoot::Insert(insert) = &duplicates.root else {
            panic!("INSERT produces INSERT root");
        };
        assert_eq!(
            insert
                .columns
                .iter()
                .map(|target| (target.column, target.uses_value))
                .collect::<Vec<_>>(),
            [
                (TargetColumn::Column(1), true),
                (TargetColumn::Column(1), false),
                (TargetColumn::Column(0), false),
                (TargetColumn::Column(0), true),
            ]
        );
    }

    #[test]
    fn insert_default_values_with_columns_preserves_width_error() {
        let schema = schema_with_writable_table();
        let mut statement = parse_statement("INSERT INTO writable DEFAULT VALUES");
        let ast::Stmt::Insert { columns, .. } = &mut statement else {
            panic!("statement is INSERT");
        };
        columns.push(ast::Name::exact("id".to_string()));

        let symbols = SymbolTable::new();
        let context = SemanticContext::for_main_schema_object(
            &schema,
            &symbols,
            true,
            Arc::new(SqliteDialect),
        );
        let error = analyze(&context, AnalyzeInput::Statement(&statement))
            .expect_err("semantic analysis rejects columns with DEFAULT VALUES");

        assert_eq!(error.to_string(), "Parse error: 0 values for 1 columns");
    }

    #[test]
    fn insert_target_and_row_width_errors_keep_existing_diagnostics() {
        let schema = schema_with_writable_table();
        for (sql, expected) in [
            (
                "INSERT INTO writable VALUES (1)",
                "Parse error: table writable has 2 columns but 1 values were supplied",
            ),
            (
                "INSERT INTO writable(absent) VALUES (1)",
                "Parse error: table writable has no column named absent",
            ),
            (
                "INSERT INTO writable(doubled) VALUES (1)",
                "Parse error: cannot INSERT into generated column \"doubled\"",
            ),
            (
                "INSERT INTO writable(value) VALUES (absent)",
                "Parse error: no such column: absent",
            ),
            (
                "INSERT INTO writable SELECT 1",
                "Parse error: table writable has 2 columns but 1 values were supplied",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid INSERT fails");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn stars_become_ordered_hir_outputs() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT *, score FROM items")
            .expect("star expands against the FROM scope");
        document
            .validate()
            .expect("star expansion produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(
            block
                .outputs
                .iter()
                .map(|output| output.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "value", "score", "score"]
        );
        for (index, output) in block.outputs.iter().enumerate() {
            assert_eq!(output.id, OutputId::query(block.id, index));
        }
        assert!(block.outputs[..3]
            .iter()
            .all(|output| output.name_kind == OutputNameKind::StarExpansion));
        assert_eq!(block.outputs[3].name_kind, OutputNameKind::Inferred);
        assert_eq!(
            block.outputs[1]
                .collation
                .as_ref()
                .expect("star preserves declared collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(!block.outputs[1].collation_is_explicit);
    }

    #[test]
    fn basic_expressions_become_closed_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT -id, NOT value, score + 1.5, value || 'x', \
             value = 'x', value IS NULL, value NOTNULL FROM items",
        )
        .expect("basic expressions have valid SQL meaning");
        document
            .validate()
            .expect("basic expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 7);
        assert!(matches!(
            outputs[0].expr,
            Expr::Unary {
                operator: ast::UnaryOperator::Negative,
                ..
            }
        ));
        assert_eq!(outputs[0].type_fact.storage, Some(Type::Integer));
        assert!(matches!(
            outputs[1].expr,
            Expr::Unary {
                operator: ast::UnaryOperator::Not,
                ..
            }
        ));
        assert_eq!(outputs[1].type_fact.storage, Some(Type::Integer));
        assert_eq!(outputs[2].type_fact.storage, Some(Type::Real));
        assert_eq!(outputs[3].type_fact.storage, Some(Type::Text));
        assert!(outputs.iter().all(|output| !output.has_affinity));

        let Expr::Binary {
            operator: ast::Operator::Equals,
            comparison: Some(comparison),
            ..
        } = &outputs[4].expr
        else {
            panic!("comparison becomes binary HIR with frozen rules");
        };
        assert_eq!(comparison.components.len(), 1);
        assert_eq!(
            comparison.components[0].affinity,
            crate::vdbe::affinity::Affinity::Text
        );
        assert_eq!(
            comparison.components[0]
                .collation
                .as_ref()
                .expect("declared collation reaches comparison")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert!(matches!(outputs[5].expr, Expr::IsNull(_)));
        assert!(matches!(outputs[6].expr, Expr::NotNull(_)));
        assert_eq!(outputs[5].type_fact.storage, Some(Type::Integer));
        assert_eq!(outputs[6].type_fact.storage, Some(Type::Integer));
    }

    #[test]
    fn between_and_in_freeze_comparison_rules_in_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value BETWEEN 'a' AND 'z', \
             value NOT BETWEEN ('a' COLLATE RTRIM) AND 'z', \
             value IN (1, 'x'), 1 IN (value) FROM items",
        )
        .expect("BETWEEN and list IN expressions have valid SQL meaning");
        document
            .validate()
            .expect("BETWEEN and list IN produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 4);
        assert!(outputs.iter().all(|output| {
            output.type_fact.storage == Some(Type::Integer) && !output.has_affinity
        }));

        let Expr::Between {
            negated: false,
            start_comparison,
            end_comparison,
            ..
        } = &outputs[0].expr
        else {
            panic!("BETWEEN becomes HIR with two comparisons");
        };
        for comparison in [start_comparison, end_comparison] {
            assert_eq!(
                comparison.components[0].affinity,
                crate::vdbe::affinity::Affinity::Text
            );
            assert_eq!(
                comparison.components[0]
                    .collation
                    .as_ref()
                    .expect("column collation reaches both bounds")
                    .value(),
                &crate::translate::collate::CollationSeq::NoCase
            );
        }

        let Expr::Between {
            negated: true,
            start_comparison,
            end_comparison,
            ..
        } = &outputs[1].expr
        else {
            panic!("NOT BETWEEN preserves negation");
        };
        assert_eq!(
            start_comparison.components[0]
                .collation
                .as_ref()
                .expect("explicit bound collation wins")
                .value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );
        assert_eq!(
            end_comparison.components[0]
                .collation
                .as_ref()
                .expect("other bound keeps column collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );

        let Expr::InList {
            negated: false,
            values,
            comparisons,
            ..
        } = &outputs[2].expr
        else {
            panic!("list IN becomes HIR");
        };
        assert_eq!(values.len(), 2);
        assert_eq!(comparisons.len(), 2);
        assert!(comparisons.iter().all(|comparison| {
            comparison.components[0].affinity == crate::vdbe::affinity::Affinity::Text
                && comparison.components[0]
                    .collation
                    .as_ref()
                    .is_some_and(|collation| {
                        collation.value() == &crate::translate::collate::CollationSeq::NoCase
                    })
        }));

        let Expr::InList { comparisons, .. } = &outputs[3].expr else {
            panic!("reversed list IN becomes HIR");
        };
        assert_eq!(comparisons.len(), 1);
        assert_eq!(
            comparisons[0].components[0].affinity,
            crate::vdbe::affinity::Affinity::Blob
        );
        assert_eq!(
            comparisons[0].components[0]
                .collation
                .as_ref()
                .expect("IN still uses collation from either operand")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
    }

    #[test]
    fn row_operands_freeze_each_comparison_component() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT ((id, value)) = ((1, 'first')), \
             (id, value) BETWEEN (1, 'a') AND (3, 'z'), \
             (id, value) IN ((1, 'one'), (2, 'two')) \
             FROM items",
        )
        .expect("supported row operands bind");
        document
            .validate()
            .expect("row operands produce closed comparison metadata");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 3);
        assert!(outputs.iter().all(|output| {
            output.type_fact.storage == Some(Type::Integer) && !output.has_affinity
        }));

        let Expr::Binary {
            lhs,
            rhs,
            comparison: Some(comparison),
            ..
        } = &outputs[0].expr
        else {
            panic!("row equality becomes binary HIR");
        };
        assert!(matches!(lhs.as_ref(), Expr::Row(values) if values.len() == 2));
        assert!(matches!(rhs.as_ref(), Expr::Row(values) if values.len() == 2));
        assert_row_comparison_facts(comparison);

        let Expr::Between {
            expr,
            start,
            end,
            start_comparison,
            end_comparison,
            ..
        } = &outputs[1].expr
        else {
            panic!("row BETWEEN becomes HIR");
        };
        assert!(matches!(expr.as_ref(), Expr::Row(values) if values.len() == 2));
        assert!(matches!(start.as_ref(), Expr::Row(values) if values.len() == 2));
        assert!(matches!(end.as_ref(), Expr::Row(values) if values.len() == 2));
        assert_row_comparison_facts(start_comparison);
        assert_row_comparison_facts(end_comparison);

        let Expr::InList {
            lhs,
            values,
            comparisons,
            ..
        } = &outputs[2].expr
        else {
            panic!("row list IN becomes HIR");
        };
        assert!(matches!(lhs.as_ref(), Expr::Row(values) if values.len() == 2));
        assert!(values
            .iter()
            .all(|value| matches!(value, Expr::Row(values) if values.len() == 2)));
        assert_eq!(comparisons.len(), 2);
        for comparison in comparisons {
            assert_row_comparison_facts(comparison);
        }
    }

    fn assert_row_comparison_facts(comparison: &hir::ComparisonSemantics) {
        assert_eq!(comparison.components.len(), 2);
        assert_eq!(
            comparison.components[0].affinity,
            crate::vdbe::affinity::Affinity::Integer
        );
        assert_eq!(
            comparison.components[1].affinity,
            crate::vdbe::affinity::Affinity::Text
        );
        assert_eq!(
            comparison.components[1]
                .collation
                .as_ref()
                .expect("text row component keeps column collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
    }

    #[test]
    fn row_operands_keep_width_and_context_errors() {
        let schema = schema_with_items();
        for (sql, expected) in [
            (
                "SELECT (id, value) = 1 FROM items",
                "Parse error: row value misused",
            ),
            (
                "SELECT (id, value) + (1, 2) FROM items",
                "Parse error: row value misused",
            ),
            (
                "SELECT id IN ((1, 2)) FROM items",
                "Parse error: row value misused",
            ),
            (
                "SELECT (id, value) IN ((1, 2, 3)) FROM items",
                "Parse error: IN(...) element has 3 terms - expected 2",
            ),
            (
                "SELECT (id, value) FROM items",
                "Parse error: row value misused",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid row use fails");
            assert_eq!(error.to_string(), expected, "{sql}");
        }
    }

    #[test]
    fn row_subqueries_bind_as_comparison_operands() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT (id, value) = (SELECT 1, 'first'), \
             (SELECT id, value) BETWEEN (1, 'a') AND (3, 'z') \
             FROM items",
        )
        .expect("row subqueries bind in supported comparison positions");
        document
            .validate()
            .expect("row-subquery comparisons produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outer = document.query(root.query).expect("outer query exists");
        let outputs = &outer.blocks[0].outputs;

        let Expr::Binary {
            rhs,
            comparison: Some(comparison),
            ..
        } = &outputs[0].expr
        else {
            panic!("row-subquery equality becomes binary HIR");
        };
        let Expr::Subquery(SubqueryExpr::Row { query }) = rhs.as_ref() else {
            panic!("binary operand keeps one row-subquery node");
        };
        assert_eq!(
            document
                .query(*query)
                .expect("right query exists")
                .output
                .len(),
            2
        );
        assert_row_comparison_facts(comparison);

        let Expr::Between {
            expr,
            start_comparison,
            end_comparison,
            ..
        } = &outputs[1].expr
        else {
            panic!("row-subquery BETWEEN becomes HIR");
        };
        let Expr::Subquery(SubqueryExpr::Row { query }) = expr.as_ref() else {
            panic!("BETWEEN operand keeps one row-subquery node");
        };
        assert_eq!(
            document
                .query(*query)
                .expect("tested query exists")
                .captures,
            [outer.blocks[0].from.as_ref().expect("FROM exists").first]
        );
        assert_row_comparison_facts(start_comparison);
        assert_row_comparison_facts(end_comparison);
    }

    #[test]
    fn row_subqueries_keep_width_and_scalar_context_errors() {
        let schema = schema_with_items();
        for (sql, expected) in [
            (
                "SELECT (id, value) = (SELECT 1) FROM items",
                "Parse error: row value misused",
            ),
            (
                "SELECT (SELECT 1, 2) FROM items",
                "Parse error: sub-select returns 2 columns - expected 1",
            ),
            (
                "SELECT (SELECT 1, 2) + (1, 2) FROM items",
                "Parse error: row value misused",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid row use fails");
            assert_eq!(error.to_string(), expected, "{sql}");
        }
    }

    #[test]
    fn like_family_outputs_have_integer_boolean_facts() {
        let document = analyze_sql("SELECT 'alphabet' LIKE 'alpha%'")
            .expect("LIKE expression has valid SQL meaning");
        document.validate().expect("LIKE produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        assert_eq!(output.type_fact, TypeFact::known(Type::Integer));
        assert_eq!(output.affinity, crate::vdbe::affinity::Affinity::Blob);
        assert!(!output.has_affinity);
        assert!(output.collation.is_none());
        assert!(!output.collation_is_explicit);
    }

    #[test]
    fn case_expressions_freeze_result_and_comparison_rules_in_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT CASE value \
                 WHEN 'a' THEN 1 \
                 WHEN ('b' COLLATE RTRIM) THEN 2.5 \
                 ELSE NULL END, \
             CASE WHEN id > 0 THEN value ELSE 'none' END, \
             CASE WHEN 0 THEN 1 END \
             FROM items",
        )
        .expect("simple and searched CASE expressions have valid SQL meaning");
        document
            .validate()
            .expect("CASE expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 3);
        assert!(outputs.iter().all(|output| !output.has_affinity));

        let Expr::Case {
            base: Some(_),
            when_then,
            else_expr: Some(_),
            base_comparisons,
        } = &outputs[0].expr
        else {
            panic!("simple CASE keeps base, branches, and ELSE");
        };
        assert_eq!(when_then.len(), 2);
        assert_eq!(base_comparisons.len(), 2);
        assert_eq!(outputs[0].type_fact.storage, Some(Type::Numeric));
        for comparison in base_comparisons {
            assert_eq!(
                comparison.components[0].affinity,
                crate::vdbe::affinity::Affinity::Text
            );
        }
        assert_eq!(
            base_comparisons[0].components[0]
                .collation
                .as_ref()
                .expect("base column supplies first comparison collation")
                .value(),
            &crate::translate::collate::CollationSeq::NoCase
        );
        assert_eq!(
            base_comparisons[1].components[0]
                .collation
                .as_ref()
                .expect("explicit WHEN collation wins")
                .value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );

        let Expr::Case {
            base: None,
            when_then,
            else_expr: Some(_),
            base_comparisons,
        } = &outputs[1].expr
        else {
            panic!("searched CASE has branches and ELSE without base");
        };
        assert_eq!(when_then.len(), 1);
        assert!(base_comparisons.is_empty());
        assert_eq!(outputs[1].type_fact.storage, Some(Type::Text));

        let Expr::Case {
            base: None,
            when_then,
            else_expr: None,
            base_comparisons,
        } = &outputs[2].expr
        else {
            panic!("searched CASE can omit ELSE");
        };
        assert_eq!(when_then.len(), 1);
        assert!(base_comparisons.is_empty());
        assert_eq!(outputs[2].type_fact.storage, Some(Type::Integer));
    }

    #[test]
    fn builtin_casts_freeze_targets_and_parameters_in_hir() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT CAST(value AS INTEGER), CAST(value AS TEXT), \
             CAST(value AS DECIMAL(10, 2)), CAST(value AS CHAR(255)), \
             CAST(value COLLATE RTRIM AS BLOB), CAST(value AS INTEGER[][]) \
             FROM items",
        )
        .expect("built-in CAST targets have valid SQL meaning");
        document
            .validate()
            .expect("built-in CAST expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 6);

        let expected = [
            (
                "INTEGER",
                Type::Integer,
                crate::vdbe::affinity::Affinity::Integer,
                0,
                0,
            ),
            (
                "TEXT",
                Type::Text,
                crate::vdbe::affinity::Affinity::Text,
                0,
                0,
            ),
            (
                "DECIMAL",
                Type::Numeric,
                crate::vdbe::affinity::Affinity::Numeric,
                2,
                0,
            ),
            (
                "CHAR",
                Type::Text,
                crate::vdbe::affinity::Affinity::Text,
                1,
                0,
            ),
            (
                "BLOB",
                Type::Blob,
                crate::vdbe::affinity::Affinity::Blob,
                0,
                0,
            ),
            (
                "INTEGER",
                Type::Blob,
                crate::vdbe::affinity::Affinity::Integer,
                0,
                2,
            ),
        ];
        for (output, (name, storage, affinity, parameters, dimensions)) in
            outputs.iter().zip(expected)
        {
            let Expr::Cast { target, .. } = &output.expr else {
                panic!("CAST becomes resolved HIR");
            };
            assert_eq!(target.name, name);
            assert_eq!(target.parameters.len(), parameters);
            assert_eq!(target.array_dimensions, dimensions);
            assert_eq!(target.type_fact.storage, Some(storage));
            assert_eq!(target.type_fact.array_dimensions, dimensions);
            assert_eq!(target.affinity, affinity);
            assert!(target.programs.encode.is_empty());
            assert!(target.programs.domain.is_none());
            assert!(target.programs.apply_builtin_affinity);
            assert_eq!(output.type_fact, target.type_fact);
            assert_eq!(output.affinity, affinity);
            assert!(output.has_affinity);
        }
        assert!(outputs[5].type_fact.is_array());
        assert_eq!(
            outputs[4]
                .collation
                .as_ref()
                .expect("CAST preserves operand collation")
                .value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );
        assert!(outputs[4].collation_is_explicit);
    }

    #[test]
    fn custom_cast_without_programs_keeps_resolved_type_chain() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT CAST(value AS BIGINT) FROM items")
            .expect("BIGINT needs no custom schema program");
        document
            .validate()
            .expect("catalog-resolved CAST produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("custom CAST becomes resolved HIR");
        };
        assert_eq!(target.name, "BIGINT");
        assert_eq!(target.type_fact.storage, Some(Type::Integer));
        assert_eq!(target.affinity, crate::vdbe::affinity::Affinity::Integer);
        assert_eq!(
            target
                .type_fact
                .declared
                .as_ref()
                .map(|value| value.name.as_str()),
            Some("BIGINT")
        );
        let chain = &target
            .type_fact
            .declared
            .as_ref()
            .expect("custom target keeps declaration")
            .custom_chain;
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].value().name, "bigint");
        assert_eq!(
            chain[0].database(),
            Some(DatabaseId::new(crate::MAIN_DB_ID))
        );
        assert_eq!(chain[0].snapshot(), document.snapshot);
        assert!(target.programs.encode.is_empty());
        assert!(target.programs.domain.is_none());
        assert!(!target.programs.apply_builtin_affinity);
        assert_eq!(output.type_fact, target.type_fact);
        assert_eq!(output.affinity, crate::vdbe::affinity::Affinity::Integer);
        assert!(output.has_affinity);
    }

    #[test]
    fn custom_cast_encoder_uses_a_document_owned_input_source() {
        let mut schema = schema_with_items();
        schema
            .add_type_from_sql(
                "CREATE TYPE positive(value INTEGER, minimum INTEGER) BASE INTEGER \
                 ENCODE CASE WHEN value > minimum THEN value ELSE NULL END",
            )
            .expect("custom type definition parses");
        let document =
            analyze_sql_with_schema(&schema, "SELECT CAST(value AS positive(0)) FROM items")
                .expect("simple custom encoder binds");
        document
            .validate()
            .expect("custom encoder produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("custom CAST becomes resolved HIR");
        };
        assert_eq!(target.programs.encode.len(), 1);
        assert!(!target.programs.apply_builtin_affinity);
        let call = &target.programs.encode[0];
        assert!(matches!(
            call.arguments.as_slice(),
            [Expr::Literal(ast::Literal::Numeric(value))] if value == "0"
        ));

        let program = document
            .schema_program(call.program)
            .expect("encoder program exists");
        let input = document
            .source(program.input_source)
            .expect("encoder input source exists");
        assert!(matches!(input.kind, SourceKind::SchemaExpression));
        assert_eq!(input.owner, SourceOwner::Root);
        assert_eq!(input.columns.len(), 2);
        assert_eq!(input.columns[0].name, "value");
        assert_eq!(input.columns[1].name, "minimum");
        assert_eq!(input.columns[0].type_fact.storage, Some(Type::Integer));

        let Expr::Case { when_then, .. } = &program.body else {
            panic!("stored CASE becomes HIR");
        };
        let Expr::Binary { lhs, rhs, .. } = &when_then[0].0 else {
            panic!("stored condition becomes binary HIR");
        };
        assert!(matches!(
            lhs.as_ref(),
            Expr::Column(column)
                if column.source == program.input_source && column.column == 0
        ));
        assert!(matches!(
            rhs.as_ref(),
            Expr::Column(column)
                if column.source == program.input_source && column.column == 1
        ));
        assert!(matches!(
            &when_then[0].1,
            Expr::Column(column)
                if column.source == program.input_source && column.column == 0
        ));
    }

    #[test]
    fn custom_cast_parameter_mismatch_uses_ordinary_cast_rules() {
        let mut schema = schema_with_items();
        schema
            .add_type_from_sql(
                "CREATE TYPE positive(value INTEGER, minimum INTEGER) BASE INTEGER \
                 ENCODE CASE WHEN value > minimum THEN value ELSE NULL END",
            )
            .expect("custom type definition parses");
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT CAST(value AS positive), CAST(value AS positive(0, 1)) FROM items",
        )
        .expect("custom parameter mismatch falls back to ordinary CAST");
        document
            .validate()
            .expect("fallback CAST targets produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        for (output, parameter_count) in outputs.iter().zip([0, 2]) {
            let Expr::Cast { target, .. } = &output.expr else {
                panic!("CAST becomes resolved HIR");
            };
            assert_eq!(target.name, "positive");
            assert_eq!(target.parameters.len(), parameter_count);
            assert!(target
                .type_fact
                .declared
                .as_ref()
                .expect("ordinary target keeps declared spelling")
                .custom_chain
                .is_empty());
            assert!(target.programs.encode.is_empty());
            assert!(target.programs.domain.is_none());
            assert!(target.programs.apply_builtin_affinity);
            assert_eq!(target.affinity, crate::vdbe::affinity::Affinity::Numeric);
            assert_eq!(target.type_fact.storage, Some(Type::Numeric));
        }
    }

    #[test]
    fn parameterized_custom_array_cast_keeps_custom_type_identity() {
        let mut schema = schema_with_items();
        schema
            .add_type_from_sql(
                "CREATE TYPE positive(value INTEGER, minimum INTEGER) BASE INTEGER \
                 ENCODE CASE WHEN value > minimum THEN value ELSE NULL END",
            )
            .expect("custom type definition parses");
        let document =
            analyze_sql_with_schema(&schema, "SELECT CAST(value AS positive(0)[]) FROM items")
                .expect("parameterized custom array CAST binds");
        document
            .validate()
            .expect("custom array CAST produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("CAST becomes resolved HIR");
        };
        assert_eq!(target.name, "positive");
        assert_eq!(target.parameters.len(), 1);
        assert_eq!(target.array_dimensions, 1);
        assert_eq!(target.type_fact.array_dimensions, 1);
        assert!(target.type_fact.is_array());
        let declared = target
            .type_fact
            .declared
            .as_ref()
            .expect("custom array keeps its declaration");
        assert_eq!(declared.custom_chain.len(), 1);
        assert_eq!(declared.custom_chain[0].value().name, "positive");
        assert_eq!(target.programs.encode.len(), 1);
        assert!(!target.programs.apply_builtin_affinity);
    }

    #[test]
    fn inconsistent_custom_type_chain_reports_schema_program_arity() {
        let mut schema = schema_with_items();
        for sql in [
            "CREATE TYPE positive(value INTEGER, minimum INTEGER) BASE INTEGER \
             ENCODE CASE WHEN value > minimum THEN value ELSE NULL END",
            "CREATE DOMAIN wrapped AS positive",
        ] {
            schema
                .add_type_from_sql(sql)
                .expect("custom type definition parses");
        }

        let error = analyze_sql_with_schema(&schema, "SELECT CAST(value AS wrapped) FROM items")
            .expect_err("parent program cannot consume the child argument list");
        assert_eq!(
            error.to_string(),
            "Internal error: custom type 'positive' encode program expects 1 arguments, got 0"
        );
    }

    #[test]
    fn scalar_functions_keep_resolved_identity_and_result_type() {
        let schema = schema_with_items();
        let document =
            analyze_sql_with_schema(&schema, "SELECT length(value), abs(score) FROM items")
                .expect("scalar functions bind");
        document
            .validate()
            .expect("scalar functions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        assert_eq!(outputs.len(), 2);
        for output in outputs {
            let Expr::Function(call) = &output.expr else {
                panic!("function call becomes resolved HIR");
            };
            assert!(matches!(call.evaluation, FunctionEvaluation::Scalar));
            assert!(matches!(call.operation, FunctionOperation::Ordinary));
            assert!(matches!(
                &call.arguments,
                crate::translate::semantic::hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } if values.len() == 1 && order_by.is_empty()
            ));
        }
        let Expr::Function(length) = &outputs[0].expr else {
            unreachable!();
        };
        assert!(matches!(
            length.function.value(),
            crate::function::Func::Scalar(crate::function::ScalarFunc::Length)
        ));
        assert_eq!(length.result_type.storage, Some(Type::Integer));
        assert_eq!(outputs[0].type_fact, length.result_type);
        let Expr::Function(abs) = &outputs[1].expr else {
            unreachable!();
        };
        assert!(matches!(
            abs.function.value(),
            crate::function::Func::Scalar(crate::function::ScalarFunc::Abs)
        ));
        assert_eq!(abs.result_type.storage, Some(Type::Real));
        assert_eq!(outputs[1].type_fact, abs.result_type);
    }

    #[test]
    fn scalar_function_modifiers_follow_production_normalization() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT length(DISTINCT value), \
                    length(value) FILTER (WHERE id > 0), \
                    random(*) \
             FROM items",
        )
        .expect("ignored scalar modifiers bind");
        document
            .validate()
            .expect("normalized scalar calls produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 0);
        for output in &block.outputs[..2] {
            let Expr::Function(call) = &output.expr else {
                panic!("scalar output becomes a function call");
            };
            assert!(matches!(call.evaluation, FunctionEvaluation::Scalar));
            assert!(matches!(
                &call.arguments,
                crate::translate::semantic::hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } if values.len() == 1 && order_by.is_empty()
            ));
        }
        let Expr::Function(random) = &block.outputs[2].expr else {
            panic!("random(*) becomes a function call");
        };
        assert!(matches!(
            &random.arguments,
            crate::translate::semantic::hir::FunctionArguments::Expressions {
                values,
                distinctness: None,
                order_by,
            } if values.is_empty() && order_by.is_empty()
        ));

        for (sql, expected) in [
            (
                "SELECT length(value ORDER BY score) FROM items",
                "Parse error: ORDER BY clause is not supported yet in aggregate functions",
            ),
            (
                "SELECT length(*) FROM items",
                "Parse error: wrong number of arguments to function length()",
            ),
            (
                "SELECT length(value) FILTER (WHERE missing > 0) FROM items",
                "Parse error: no such column: missing",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid call fails");
            assert_eq!(error.to_string(), expected, "{sql}");
        }

        let document = analyze_sql_with_schema(
            &schema,
            "SELECT length(value) FILTER (WHERE sum(id) > 0) FROM items",
        )
        .expect("ignored scalar filter still binds its expression");
        document
            .validate()
            .expect("ignored scalar filter leaves a closed document");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 1);
        assert!(matches!(
            block.outputs[0].expr,
            Expr::Function(ref call) if matches!(call.evaluation, FunctionEvaluation::Scalar)
        ));
    }

    #[cfg(feature = "json")]
    #[test]
    fn scalar_star_expands_visible_columns_into_arguments() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT json_object(*) FROM items")
            .expect("json_object star binds");
        document
            .validate()
            .expect("expanded scalar star produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = block.from.as_ref().expect("query has FROM").first;
        let Expr::Function(call) = &block.outputs[0].expr else {
            panic!("json_object becomes a function call");
        };
        let crate::translate::semantic::hir::FunctionArguments::Expressions {
            values,
            distinctness: None,
            order_by,
        } = &call.arguments
        else {
            panic!("expanded star becomes expression arguments");
        };
        assert!(order_by.is_empty());
        assert_eq!(values.len(), 6);
        for (column, pair) in ["id", "value", "score"]
            .into_iter()
            .zip(values.chunks_exact(2))
        {
            assert!(matches!(
                &pair[0],
                Expr::Literal(ast::Literal::String(name)) if name == &format!("'{column}'")
            ));
            assert!(matches!(
                pair[1],
                Expr::Column(reference) if reference.source == source
            ));
        }

        let error = analyze_sql("SELECT json_object(*)")
            .expect_err("expanding star without a source fails");
        assert_eq!(
            error.to_string(),
            "Parse error: json_object(*) requires a FROM clause"
        );
    }

    #[test]
    fn custom_type_reads_freeze_types_and_member_indexes() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE telegram_msg AS STRUCT(chat_id INT, text TEXT)")
            .expect("struct type parses");
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram telegram_msg, slack TEXT)")
            .expect("union type parses");
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT union_tag(CAST(NULL AS platform)), \
                    struct_extract(\
                        union_extract(CAST(NULL AS platform), 'telegram'), \
                        'chat_id'\
                    )",
        )
        .expect("custom-type read functions bind");
        document
            .validate()
            .expect("custom-type reads produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;

        let Expr::Function(tag) = &outputs[0].expr else {
            panic!("union_tag becomes a function call");
        };
        let FunctionOperation::CustomType(CustomTypeOperation::UnionTag {
            union_type,
            tag_names,
        }) = &tag.operation
        else {
            panic!("union_tag keeps its resolved union operation");
        };
        assert_eq!(union_type.value().name, "platform");
        assert_eq!(tag_names.as_ref(), ["telegram", "slack"]);
        assert_eq!(tag.result_type.storage, Some(Type::Text));

        let Expr::Function(field) = &outputs[1].expr else {
            panic!("struct_extract becomes a function call");
        };
        let FunctionOperation::CustomType(CustomTypeOperation::StructExtract {
            struct_type,
            field_index,
        }) = &field.operation
        else {
            panic!("struct_extract keeps its resolved field operation");
        };
        assert_eq!(struct_type.value().name, "telegram_msg");
        assert_eq!(*field_index, 0);
        assert_eq!(field.result_type.storage, Some(Type::Integer));

        let [union, Expr::Literal(ast::Literal::String(field_name))] =
            field.arguments.expressions()
        else {
            panic!("struct_extract keeps its input and member name");
        };
        assert_eq!(field_name, "'chat_id'");
        let Expr::Function(union) = union else {
            panic!("struct input comes from union_extract");
        };
        let FunctionOperation::CustomType(CustomTypeOperation::UnionExtract {
            union_type,
            tag_index,
        }) = &union.operation
        else {
            panic!("union_extract keeps its resolved variant operation");
        };
        assert_eq!(union_type.value().name, "platform");
        assert_eq!(*tag_index, 0);
        assert_eq!(
            union
                .result_type
                .declared
                .as_ref()
                .map(|declaration| declaration.name.as_str()),
            Some("telegram_msg")
        );
    }

    #[test]
    fn custom_type_reads_keep_bind_time_errors() {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql("CREATE TYPE platform AS UNION(telegram TEXT, slack TEXT)")
            .expect("union type parses");

        for (sql, expected) in [
            (
                "SELECT union_tag(1)",
                "Parse error: union_tag() argument must have a known union type",
            ),
            (
                "SELECT union_extract(CAST(NULL AS platform), 1)",
                "Parse error: union_extract() second argument must be a string literal",
            ),
            (
                "SELECT union_extract(CAST(NULL AS platform), \"telegram\")",
                "Parse error: union_extract() second argument must be a string literal",
            ),
            (
                "SELECT union_extract(CAST(NULL AS platform), 'discord')",
                "Parse error: unknown variant 'discord' in union type 'platform'",
            ),
            (
                "SELECT union_tag(CAST(NULL AS platform)) FILTER (WHERE TRUE)",
                "Parse error: union_tag() may not be used as an aggregate or window function",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid read must fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn custom_field_access_freezes_member_identity_and_result_type() {
        let schema = schema_with_struct_and_union_columns();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT point_value.x, shape_value.point.x FROM custom_values",
        )
        .expect("direct and nested custom fields bind");
        document
            .validate()
            .expect("custom field access produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;

        let Expr::FieldAccess(point_x) = &outputs[0].expr else {
            panic!("struct field becomes field-access HIR");
        };
        assert_eq!(point_x.field_name, "x");
        assert_eq!(point_x.kind, FieldAccessKind::Struct { field_index: 0 });
        assert_eq!(point_x.container_type.value().name, "point");
        assert_eq!(point_x.result_type.storage, Some(Type::Integer));
        assert_eq!(outputs[0].type_fact, point_x.result_type);
        assert!(matches!(
            point_x.base.as_ref(),
            Expr::Column(column) if column.column == 1
        ));

        let Expr::FieldAccess(nested_x) = &outputs[1].expr else {
            panic!("nested struct field becomes field-access HIR");
        };
        assert_eq!(nested_x.kind, FieldAccessKind::Struct { field_index: 0 });
        assert_eq!(nested_x.container_type.value().name, "point");
        assert_eq!(nested_x.result_type.storage, Some(Type::Integer));
        assert_eq!(outputs[1].type_fact, nested_x.result_type);
        let Expr::FieldAccess(shape_point) = nested_x.base.as_ref() else {
            panic!("nested access keeps the union access as its base");
        };
        assert_eq!(shape_point.field_name, "point");
        assert_eq!(shape_point.kind, FieldAccessKind::Union { tag_index: 0 });
        assert_eq!(shape_point.container_type.value().name, "shape");
        assert_eq!(
            shape_point
                .result_type
                .declared
                .as_ref()
                .map(|declaration| declaration.name.as_str()),
            Some("point")
        );
        assert!(matches!(
            shape_point.base.as_ref(),
            Expr::Column(column) if column.column == 2
        ));
    }

    #[test]
    fn custom_field_access_keeps_qualifier_precedence_and_errors() {
        let schema = schema_with_struct_and_union_columns();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT point_value.id FROM custom_values AS point_value",
        )
        .expect("real table qualifier wins over field fallback");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        assert!(matches!(
            &document.query(root.query).expect("query exists").blocks[0].outputs[0].expr,
            Expr::Column(column) if column.column == 0
        ));

        for (sql, expected) in [
            (
                "SELECT point_value.missing FROM custom_values",
                "Parse error: no such field 'missing' in struct type 'point'",
            ),
            (
                "SELECT shape_value.missing FROM custom_values",
                "Parse error: no such variant 'missing' in union type 'shape'",
            ),
            (
                "SELECT custom_values.id.x FROM custom_values",
                "Parse error: column 'id' is not a STRUCT or UNION type; cannot access field 'x'",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid field must fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn sequence_writes_freeze_catalog_objects_and_names() {
        let schema = schema_with_sequence("orders");
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT nextval('orders'), \
                    setval('main.orders', 7), \
                    setval('orders', 8, TRUE), \
                    currval('orders')",
        )
        .expect("sequence functions bind");
        document
            .validate()
            .expect("sequence functions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;
        for (index, expected_kind, expected_user_name) in [
            (
                0,
                crate::translate::semantic::hir::SequenceOperationKind::NextValue,
                "orders",
            ),
            (
                1,
                crate::translate::semantic::hir::SequenceOperationKind::SetValue,
                "main.orders",
            ),
            (
                2,
                crate::translate::semantic::hir::SequenceOperationKind::SetValue,
                "orders",
            ),
        ] {
            let Expr::Function(call) = &outputs[index].expr else {
                panic!("sequence call becomes a function expression");
            };
            let FunctionOperation::Sequence(operation) = &call.operation else {
                panic!("sequence write keeps its resolved operation");
            };
            assert_eq!(operation.kind, expected_kind);
            assert_eq!(operation.user_name, expected_user_name);
            assert_eq!(operation.normalized_name, "orders");
            assert_eq!(operation.sequence.value().name, "orders");
            assert_eq!(
                operation.backing_table.value().get_name(),
                "__turso_internal_seq_orders"
            );
            assert_eq!(
                operation.sequence.database(),
                Some(DatabaseId::new(crate::MAIN_DB_ID))
            );
            assert_eq!(operation.sequence.snapshot(), document.snapshot);
            assert!(operation.sqlite_sequence.is_none());
            assert_eq!(call.result_type, TypeFact::known(Type::Integer));
        }

        let Expr::Function(currval) = &outputs[3].expr else {
            panic!("currval becomes a function expression");
        };
        assert!(matches!(currval.operation, FunctionOperation::Ordinary));
        assert_eq!(currval.result_type, TypeFact::known(Type::Integer));
    }

    #[test]
    fn sequence_writes_keep_name_errors() {
        let schema = schema_with_sequence("orders");
        for (sql, expected) in [
            (
                "SELECT nextval(orders)",
                "Parse error: expected a string literal argument",
            ),
            (
                "SELECT nextval(\"orders\")",
                "Parse error: expected a string literal argument",
            ),
            (
                "SELECT nextval('missing')",
                "Parse error: sequence \"missing\" does not exist",
            ),
            ("SELECT setval('aux.orders', 1)", "no such database: aux"),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid call must fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn autoincrement_sequence_writes_resolve_sqlite_sequence() {
        let sequence_name = crate::schema::autoincrement_sequence_name("items");
        let mut schema = schema_with_sequence(&sequence_name);
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE sqlite_sequence(name, seq)", 3)
                    .expect("sqlite_sequence schema parses"),
            ))
            .expect("sqlite_sequence name is unique");
        let document =
            analyze_sql_with_schema(&schema, &format!("SELECT nextval('{sequence_name}')"))
                .expect("internal sequence binds");
        document
            .validate()
            .expect("internal sequence produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let Expr::Function(call) =
            &document.query(root.query).expect("query exists").blocks[0].outputs[0].expr
        else {
            panic!("nextval becomes a function expression");
        };
        let FunctionOperation::Sequence(operation) = &call.operation else {
            panic!("nextval keeps its resolved sequence operation");
        };
        let sqlite_sequence = operation
            .sqlite_sequence
            .as_ref()
            .expect("AUTOINCREMENT sequence resolves sqlite_sequence");
        assert_eq!(sqlite_sequence.value().get_name(), "sqlite_sequence");
        assert_eq!(sqlite_sequence.database(), operation.sequence.database());
    }

    #[test]
    fn aggregate_functions_get_stable_block_local_identities() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT sum(score), count(id), max(value), count(*), \
             sum(DISTINCT score), \
             group_concat(value ORDER BY score DESC NULLS LAST) FROM items",
        )
        .expect("plain aggregate calls bind");
        document
            .validate()
            .expect("aggregate calls produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 6);
        for (index, output) in block.outputs.iter().enumerate() {
            let Expr::Function(call) = &output.expr else {
                panic!("aggregate output becomes a function call");
            };
            assert!(matches!(
                &call.evaluation,
                FunctionEvaluation::Aggregate { id, filter: None }
                    if *id == crate::translate::semantic::hir::AggregateId::new(block.id, index)
            ));
        }
        assert!(block.outputs[..3].iter().all(|output| {
            matches!(
                &output.expr,
                Expr::Function(call)
                    if matches!(
                        &call.arguments,
                        crate::translate::semantic::hir::FunctionArguments::Expressions {
                            values,
                            distinctness: None,
                            order_by,
                        } if values.len() == 1 && order_by.is_empty()
                    )
            )
        }));
        let Expr::Function(count_star) = &block.outputs[3].expr else {
            unreachable!();
        };
        assert!(matches!(
            &count_star.arguments,
            crate::translate::semantic::hir::FunctionArguments::Star
        ));
        let Expr::Function(distinct_sum) = &block.outputs[4].expr else {
            unreachable!();
        };
        assert!(matches!(
            &distinct_sum.arguments,
            crate::translate::semantic::hir::FunctionArguments::Expressions {
                values,
                distinctness: Some(ast::Distinctness::Distinct),
                order_by,
            } if values.len() == 1 && order_by.is_empty()
        ));
        let Expr::Function(ordered_concat) = &block.outputs[5].expr else {
            unreachable!();
        };
        let crate::translate::semantic::hir::FunctionArguments::Expressions {
            values,
            distinctness,
            order_by,
        } = &ordered_concat.arguments
        else {
            panic!("ordered aggregate has expression arguments");
        };
        assert_eq!(values.len(), 1);
        assert!(distinctness.is_none());
        assert_eq!(order_by.len(), 1);
        assert!(matches!(
            order_by[0].expr,
            Expr::Column(crate::translate::semantic::hir::ColumnRef { column: 2, .. })
        ));
        assert_eq!(order_by[0].order, ast::SortOrder::Desc);
        assert_eq!(order_by[0].nulls, Some(ast::NullsOrder::Last));
        assert_eq!(order_by[0].type_fact.storage, Some(Type::Real));
        assert!(order_by[0].collation.is_none());
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Numeric));
        assert_eq!(block.outputs[1].type_fact.storage, Some(Type::Integer));
        assert_eq!(block.outputs[2].type_fact.storage, Some(Type::Text));
        assert_eq!(block.outputs[3].type_fact.storage, Some(Type::Integer));
        assert_eq!(block.outputs[4].type_fact.storage, Some(Type::Numeric));
        assert_eq!(block.outputs[5].type_fact.storage, Some(Type::Text));
    }

    #[test]
    fn aggregate_filters_are_bound_into_evaluation() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT sum(score) FILTER (WHERE id > 0), \
             count(*) FILTER (WHERE value IS NOT NULL) FROM items",
        )
        .expect("aggregate FILTER clauses bind");
        document
            .validate()
            .expect("aggregate filters produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 2);

        let Expr::Function(sum) = &block.outputs[0].expr else {
            panic!("filtered aggregate becomes a function call");
        };
        assert!(matches!(
            &sum.evaluation,
            FunctionEvaluation::Aggregate {
                id,
                filter: Some(filter),
            } if *id == crate::translate::semantic::hir::AggregateId::new(block.id, 0)
                && matches!(filter.as_ref(), Expr::Binary { .. })
        ));

        let Expr::Function(count) = &block.outputs[1].expr else {
            panic!("filtered COUNT becomes a function call");
        };
        assert!(matches!(
            count.arguments,
            crate::translate::semantic::hir::FunctionArguments::Star
        ));
        assert!(matches!(
            &count.evaluation,
            FunctionEvaluation::Aggregate {
                id,
                filter: Some(filter),
            } if *id == crate::translate::semantic::hir::AggregateId::new(block.id, 1)
                && matches!(filter.as_ref(), Expr::NotNull(_))
        ));
    }

    #[test]
    fn inline_windows_keep_resolved_specs_and_block_local_identities() {
        use crate::translate::semantic::hir::WindowFrameBound;

        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT row_number() OVER ( \
                 PARTITION BY value ORDER BY score DESC NULLS LAST \
                 ROWS BETWEEN missing PRECEDING AND missing FOLLOWING), \
             sum(score) FILTER (WHERE id > 0) OVER ( \
                 PARTITION BY value ORDER BY id \
                 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), \
             first_value(value) OVER (ORDER BY score) \
             FROM items",
        )
        .expect("inline windows bind");
        document
            .validate()
            .expect("inline windows produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 0);
        assert_eq!(block.window_function_count, 3);

        let Expr::Function(row_number) = &block.outputs[0].expr else {
            panic!("row_number becomes a function call");
        };
        let FunctionEvaluation::Window {
            id,
            window,
            filter: None,
        } = &row_number.evaluation
        else {
            panic!("row_number has window evaluation");
        };
        assert_eq!(
            *id,
            crate::translate::semantic::hir::WindowFunctionId::new(block.id, 0)
        );
        let window = &block.windows[window.index];
        assert_eq!(window.partition_by.len(), 1);
        assert_eq!(window.order_by.len(), 1);
        assert_eq!(window.order_by[0].order, ast::SortOrder::Desc);
        assert_eq!(window.order_by[0].nulls, Some(ast::NullsOrder::Last));
        let frame = &window.frame;
        assert_eq!(frame.mode, ast::FrameMode::Rows);
        assert!(matches!(frame.start, WindowFrameBound::UnboundedPreceding));
        assert!(matches!(frame.end, Some(WindowFrameBound::CurrentRow)));
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Integer));

        let Expr::Function(sum) = &block.outputs[1].expr else {
            panic!("window sum becomes a function call");
        };
        let FunctionEvaluation::Window {
            id,
            window,
            filter: Some(filter),
        } = &sum.evaluation
        else {
            panic!("sum has window evaluation and filter");
        };
        assert_eq!(
            *id,
            crate::translate::semantic::hir::WindowFunctionId::new(block.id, 1)
        );
        assert!(matches!(filter.as_ref(), Expr::Binary { .. }));
        let frame = &block.windows[window.index].frame;
        assert_eq!(frame.mode, ast::FrameMode::Rows);
        assert!(matches!(frame.start, WindowFrameBound::Preceding(_)));
        assert!(matches!(frame.end, Some(WindowFrameBound::Following(_))));
        assert_eq!(block.outputs[1].type_fact.storage, Some(Type::Numeric));

        let Expr::Function(first_value) = &block.outputs[2].expr else {
            panic!("first_value becomes a function call");
        };
        let FunctionEvaluation::Window { window, .. } = &first_value.evaluation else {
            panic!("first_value has window evaluation");
        };
        let frame = &block.windows[window.index].frame;
        assert_eq!(frame.mode, ast::FrameMode::Range);
        assert!(matches!(frame.start, WindowFrameBound::UnboundedPreceding));
        assert!(matches!(frame.end, Some(WindowFrameBound::CurrentRow)));
        assert_eq!(block.outputs[2].type_fact.storage, Some(Type::Text));
    }

    #[test]
    fn inline_windows_keep_existing_restrictions() {
        let schema = schema_with_items();
        for (sql, expected) in [
            (
                "SELECT row_number() FROM items",
                "Parse error: misuse of window function: row_number()",
            ),
            (
                "SELECT length(value) OVER () FROM items",
                "Parse error: length may not be used as a window function",
            ),
            (
                "SELECT row_number() FILTER (WHERE id > 0) OVER () FROM items",
                "Parse error: FILTER clause may only be used with aggregate window functions",
            ),
            (
                "SELECT sum(DISTINCT score) OVER () FROM items",
                "Parse error: DISTINCT is not supported for window functions",
            ),
            (
                "SELECT sum(score ORDER BY id) OVER () FROM items",
                "Parse error: ORDER BY clause is not supported yet in aggregate functions",
            ),
            (
                "SELECT sum(score) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items",
                "Parse error: RANGE with offset PRECEDING/FOLLOWING requires one ORDER BY expression",
            ),
            (
                "SELECT array_agg(value) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM items",
                "Parse error: array_agg() does not yet support window frames with a moving start; use a frame with UNBOUNDED PRECEDING start",
            ),
            (
                "SELECT sum(max(score)) OVER () FROM items",
                "Parse error: misuse of aggregate function max()",
            ),
            (
                "SELECT sum(row_number() OVER ()) FROM items",
                "Parse error: misuse of window function: row_number()",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("unsupported inline window form is rejected");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn named_windows_resolve_inheritance_into_block_owned_windows() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT rank() OVER ranked, sum(score) OVER base, \
                    row_number() OVER (later ORDER BY score) \
             FROM items \
             WINDOW base AS (PARTITION BY value), \
                    ranked AS (base ORDER BY score), \
                    later AS (PARTITION BY value)",
        )
        .expect("named and inline inheritance bind");
        document
            .validate()
            .expect("resolved windows produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.window_function_count, 3);
        assert_eq!(block.windows.len(), 3);
        for (index, output) in block.outputs.iter().enumerate() {
            let Expr::Function(function) = &output.expr else {
                panic!("window output becomes a function call");
            };
            let FunctionEvaluation::Window { window, .. } = function.evaluation else {
                panic!("function has window evaluation");
            };
            assert_eq!(window.index, index);
            assert_eq!(block.windows[index].id, window);
        }
        assert_eq!(block.windows[0].partition_by.len(), 1);
        assert_eq!(block.windows[0].order_by.len(), 1);
        assert_eq!(block.windows[1].partition_by.len(), 1);
        assert!(block.windows[1].order_by.is_empty());
        assert_eq!(block.windows[2].partition_by.len(), 1);
        assert_eq!(block.windows[2].order_by.len(), 1);
    }

    #[test]
    fn named_windows_keep_sqlite_chaining_rules() {
        let schema = schema_with_items();

        analyze_sql_with_schema(
            &schema,
            "SELECT row_number() OVER framed FROM items \
             WINDOW framed AS (ORDER BY score ROWS BETWEEN missing PRECEDING AND CURRENT ROW)",
        )
        .expect("coerced window functions ignore named user frames");

        for (sql, expected) in [
            (
                "SELECT sum(score) OVER absent FROM items",
                "Parse error: no such window: absent",
            ),
            (
                "SELECT sum(score) OVER (base PARTITION BY id) FROM items \
                 WINDOW base AS (PARTITION BY value)",
                "Parse error: cannot override PARTITION clause of window: base",
            ),
            (
                "SELECT sum(score) OVER (base ORDER BY id) FROM items \
                 WINDOW base AS (ORDER BY score)",
                "Parse error: cannot override ORDER BY clause of window: base",
            ),
            (
                "SELECT sum(score) OVER child FROM items \
                 WINDOW base AS (ROWS CURRENT ROW), child AS (base)",
                "Parse error: cannot override frame specification of window: base",
            ),
            (
                "SELECT sum(score) OVER second FROM items \
                 WINDOW first AS (), second AS (later), later AS ()",
                "Parse error: no such window: later",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql).expect_err("invalid chain fails");
            assert_eq!(error.to_string(), expected, "SQL: {sql}");
        }
    }

    #[test]
    fn ordered_set_aggregates_keep_direct_and_ordered_inputs_separate() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT mode() WITHIN GROUP (ORDER BY value), \
             percentile_cont(0.5) WITHIN GROUP (ORDER BY score), \
             percentile_disc(0.5) WITHIN GROUP (ORDER BY value COLLATE BINARY) \
                 FILTER (WHERE id > 0) FROM items",
        )
        .expect("ordered-set aggregates bind");
        document
            .validate()
            .expect("ordered-set aggregates produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.aggregate_count, 3);

        let Expr::Function(mode) = &block.outputs[0].expr else {
            panic!("mode becomes a function call");
        };
        let crate::translate::semantic::hir::FunctionArguments::OrderedSet { direct, order_by } =
            &mode.arguments
        else {
            panic!("mode keeps ordered-set arguments");
        };
        assert!(direct.is_empty());
        assert!(matches!(order_by.expr, Expr::Column(_)));
        assert_eq!(order_by.type_fact.storage, Some(Type::Text));
        assert!(order_by.collation.is_some());
        assert_eq!(block.outputs[0].type_fact.storage, Some(Type::Text));

        let Expr::Function(percentile_cont) = &block.outputs[1].expr else {
            panic!("percentile_cont becomes a function call");
        };
        let crate::translate::semantic::hir::FunctionArguments::OrderedSet { direct, order_by } =
            &percentile_cont.arguments
        else {
            panic!("percentile_cont keeps ordered-set arguments");
        };
        assert_eq!(direct.len(), 1);
        assert!(matches!(order_by.expr, Expr::Column(_)));
        assert_eq!(order_by.type_fact.storage, Some(Type::Real));
        assert_eq!(block.outputs[1].type_fact.storage, Some(Type::Real));

        let Expr::Function(percentile_disc) = &block.outputs[2].expr else {
            panic!("percentile_disc becomes a function call");
        };
        let crate::translate::semantic::hir::FunctionArguments::OrderedSet { direct, order_by } =
            &percentile_disc.arguments
        else {
            panic!("percentile_disc keeps ordered-set arguments");
        };
        assert_eq!(direct.len(), 1);
        assert!(matches!(order_by.expr, Expr::Collate { .. }));
        assert_eq!(order_by.order, ast::SortOrder::Asc);
        assert!(order_by.nulls.is_none());
        assert!(order_by.collation.is_some());
        assert_eq!(block.outputs[2].type_fact.storage, Some(Type::Text));
        assert!(matches!(
            &percentile_disc.evaluation,
            FunctionEvaluation::Aggregate {
                filter: Some(filter),
                ..
            } if matches!(filter.as_ref(), Expr::Binary { .. })
        ));
    }

    #[test]
    fn ordered_set_aggregates_keep_existing_restrictions() {
        let schema = schema_with_items();
        for (sql, expected) in [
            (
                "SELECT sum(score) WITHIN GROUP (ORDER BY score) FROM items",
                "Parse error: WITHIN GROUP is not supported for function sum()",
            ),
            (
                "SELECT mode(score) WITHIN GROUP (ORDER BY value) FROM items",
                "Parse error: wrong number of arguments to function mode()",
            ),
            (
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY score DESC) FROM items",
                "Parse error: DESC and NULLS ordering inside WITHIN GROUP are not supported yet",
            ),
            (
                "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY score, id) FROM items",
                "Parse error: WITHIN GROUP for percentile_disc() must specify exactly one ORDER BY expression",
            ),
        ] {
            let error = analyze_sql_with_schema(&schema, sql)
                .expect_err("unsupported ordered-set form is rejected");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn aggregate_checkpoint_rejects_nested_calls() {
        let schema = schema_with_items();
        let nested = analyze_sql_with_schema(&schema, "SELECT sum(max(score)) FROM items")
            .expect_err("aggregate calls cannot be nested");
        assert_eq!(
            nested.to_string(),
            "Parse error: misuse of aggregate function max()"
        );
        let nested_filter = analyze_sql_with_schema(
            &schema,
            "SELECT sum(score) FILTER (WHERE max(id) > 0) FROM items",
        )
        .expect_err("aggregate filters cannot contain aggregates");
        assert_eq!(
            nested_filter.to_string(),
            "Parse error: misuse of aggregate function max()"
        );
    }

    #[test]
    fn custom_cast_encoder_can_call_scalar_functions() {
        let mut schema = schema_with_items();
        schema
            .add_type_from_sql(
                "CREATE TYPE short_text(value TEXT, maximum INTEGER) BASE TEXT \
                 ENCODE CASE WHEN length(value) <= maximum THEN value ELSE NULL END",
            )
            .expect("custom type definition parses");
        let document =
            analyze_sql_with_schema(&schema, "SELECT CAST(value AS short_text(3)) FROM items")
                .expect("function call in custom encoder binds");
        document
            .validate()
            .expect("function encoder produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("custom CAST becomes resolved HIR");
        };
        let program = document
            .schema_program(target.programs.encode[0].program)
            .expect("encoder program exists");
        let Expr::Case { when_then, .. } = &program.body else {
            panic!("encoder keeps CASE body");
        };
        let Expr::Binary { lhs, .. } = &when_then[0].0 else {
            panic!("encoder condition is a comparison");
        };
        let Expr::Function(length) = lhs.as_ref() else {
            panic!("comparison contains resolved function");
        };
        assert!(matches!(
            length.function.value(),
            crate::function::Func::Scalar(crate::function::ScalarFunc::Length)
        ));
        assert!(matches!(length.evaluation, FunctionEvaluation::Scalar));
        assert_eq!(length.result_type.storage, Some(Type::Integer));
        assert!(matches!(
            length.arguments.expressions(),
            [Expr::Column(column)]
                if column.source == program.input_source && column.column == 0
        ));
    }

    #[test]
    fn varchar_encoder_keeps_abort_raise_in_hir() {
        let schema = schema_with_items();
        let document =
            analyze_sql_with_schema(&schema, "SELECT CAST(value AS VARCHAR(3)) FROM items")
                .expect("built-in VARCHAR encoder binds");
        document
            .validate()
            .expect("VARCHAR encoder produces closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("VARCHAR CAST becomes resolved HIR");
        };
        assert_eq!(target.programs.encode.len(), 1);
        let program = document
            .schema_program(target.programs.encode[0].program)
            .expect("VARCHAR encoder program exists");
        let Expr::Case {
            else_expr: Some(else_expr),
            ..
        } = &program.body
        else {
            panic!("VARCHAR encoder keeps failure branch");
        };
        let Expr::Raise {
            action: ast::ResolveType::Abort,
            message: Some(message),
        } = else_expr.as_ref()
        else {
            panic!("VARCHAR failure branch becomes RAISE(ABORT)");
        };
        assert!(matches!(
            message.as_ref(),
            Expr::Literal(ast::Literal::String(value))
                if value == "'value too long for varchar'"
        ));
    }

    #[test]
    fn non_trigger_raise_only_allows_abort() {
        let document = analyze_sql("SELECT RAISE(ABORT, 'stop')")
            .expect("Turso allows RAISE(ABORT) outside triggers");
        document
            .validate()
            .expect("standalone abort produces closed HIR");

        let error = analyze_sql("SELECT RAISE(FAIL, 'stop')")
            .expect_err("other RAISE actions require a trigger");
        assert_eq!(
            error.to_string(),
            "Parse error: RAISE() may only be used within a trigger-program"
        );
    }

    #[test]
    fn domain_cast_keeps_inherited_not_null_and_check_programs() {
        let mut schema = schema_with_items();
        schema
            .add_type_from_sql(
                "CREATE DOMAIN positive_integer AS INTEGER \
                 CONSTRAINT positive CHECK (value > 0)",
            )
            .expect("parent domain definition parses");
        schema
            .add_type_from_sql("CREATE DOMAIN required_positive AS positive_integer NOT NULL")
            .expect("child domain definition parses");
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT CAST(value AS required_positive) FROM items",
        )
        .expect("domain constraints bind");
        document
            .validate()
            .expect("domain constraints produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let output = &document.query(root.query).expect("query exists").blocks[0].outputs[0];
        let Expr::Cast { target, .. } = &output.expr else {
            panic!("domain CAST becomes resolved HIR");
        };
        let domain = target
            .programs
            .domain
            .as_ref()
            .expect("domain CAST carries constraints");
        assert_eq!(
            domain.not_null_description.as_deref(),
            Some("domain required_positive does not allow null values")
        );
        assert_eq!(domain.checks.len(), 1);
        assert_eq!(
            domain.checks[0].failure_description,
            "value for domain positive_integer violates check constraint \"positive\""
        );
        assert!(domain.checks[0].call.arguments.is_empty());

        let program = document
            .schema_program(domain.checks[0].call.program)
            .expect("domain CHECK program exists");
        let input = document
            .source(program.input_source)
            .expect("domain CHECK input exists");
        assert_eq!(input.columns.len(), 1);
        assert_eq!(input.columns[0].name, "value");
        assert_eq!(input.columns[0].type_fact.storage, target.type_fact.storage);
        assert!(input.columns[0].type_fact.declared.is_none());
        let Expr::Binary { lhs, .. } = &program.body else {
            panic!("domain CHECK becomes binary HIR");
        };
        assert!(matches!(
            lhs.as_ref(),
            Expr::Column(column)
                if column.source == program.input_source && column.column == 0
        ));
    }

    #[test]
    fn explicit_collations_override_inherited_collations() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(
            &schema,
            "SELECT value COLLATE RTRIM, \
             value = ('x' || ('y' COLLATE RTRIM)), \
             (('x' COLLATE RTRIM) || 'y') = value FROM items",
        )
        .expect("explicit collations bind into HIR");
        document
            .validate()
            .expect("collated expressions produce closed HIR");

        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let outputs = &document.query(root.query).expect("query exists").blocks[0].outputs;

        let Expr::Collate { collation, .. } = &outputs[0].expr else {
            panic!("COLLATE becomes resolved HIR");
        };
        assert_eq!(
            collation.value(),
            &crate::translate::collate::CollationSeq::Rtrim
        );
        assert_eq!(outputs[0].affinity, crate::vdbe::affinity::Affinity::Text);
        assert!(outputs[0].has_affinity);
        assert!(outputs[0].collation_is_explicit);

        for output in &outputs[1..] {
            let Expr::Binary {
                comparison: Some(comparison),
                ..
            } = &output.expr
            else {
                panic!("comparison rules are frozen in binary HIR");
            };
            assert_eq!(
                comparison.components[0]
                    .collation
                    .as_ref()
                    .expect("explicit collation reaches comparison")
                    .value(),
                &crate::translate::collate::CollationSeq::Rtrim
            );
            assert!(output.collation_is_explicit);
        }
    }

    #[test]
    fn table_star_uses_alias_visibility() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT i.* FROM items AS i")
            .expect("table alias expands star");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        assert_eq!(block.outputs.len(), 3);
        assert!(block
            .outputs
            .iter()
            .all(|output| output.name_kind == OutputNameKind::StarExpansion));

        let hidden_name = analyze_sql_with_schema(&schema, "SELECT items.* FROM items AS i")
            .expect_err("alias hides the original table name");
        assert_eq!(hidden_name.to_string(), "Parse error: no such table: items");

        let no_from = analyze_sql("SELECT *").expect_err("star requires a FROM source");
        assert_eq!(no_from.to_string(), "Parse error: no tables specified");
    }

    #[test]
    fn table_alias_controls_hir_name_visibility() {
        let schema = schema_with_items();
        let document = analyze_sql_with_schema(&schema, "SELECT i.value FROM items AS i")
            .expect("alias-qualified column resolves");
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source = document
            .source(block.from.as_ref().expect("FROM exists").first)
            .expect("source exists");
        assert_eq!(source.alias.as_deref(), Some("i"));

        let error = analyze_sql_with_schema(&schema, "SELECT main.items.value FROM items AS i")
            .expect_err("alias hides database-qualified table name");
        assert_eq!(
            error.to_string(),
            "Parse error: no such column: main.items.value"
        );
    }

    #[test]
    fn missing_table_fails_before_hir_is_built() {
        let error = analyze_sql("SELECT value FROM absent").expect_err("table must exist");
        assert_eq!(error.to_string(), "Parse error: no such table: absent");
    }

    #[test]
    fn update_binds_old_and_new_rows_assignments_and_predicate() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET value = value || '!', id = id + 1 WHERE id = 7",
        )
        .expect("basic UPDATE binds");
        document.validate().expect("UPDATE produces closed HIR");

        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        assert_ne!(update.target, update.new_source);
        assert_eq!(update.assignments.len(), 2);
        assert_eq!(update.assignments[0].columns, [TargetColumn::Column(1)]);
        assert_eq!(update.assignments[1].columns, [TargetColumn::Column(0)]);
        assert!(matches!(
            update.assignments[0].value,
            Expr::Binary {
                lhs: ref left,
                operator: ast::Operator::Concat,
                ..
            } if matches!(left.as_ref(), Expr::Column(reference) if reference.source == update.target && reference.column == 1)
        ));
        assert!(matches!(
            update.predicate,
            Some(Expr::Binary { lhs: ref left, .. })
                if matches!(left.as_ref(), Expr::Column(reference) if reference.source == update.target && reference.column == 0)
        ));

        let old = document.source(update.target).expect("OLD source exists");
        let new = document
            .source(update.new_source)
            .expect("NEW source exists");
        let SourceKind::Table(table) = &old.kind else {
            panic!("OLD row is the target table");
        };
        assert!(matches!(
            &new.kind,
            SourceKind::Pseudo {
                kind: PseudoSource::New,
                table: new_table,
            } if new_table == table
        ));
        for source in [old, new] {
            assert!(matches!(
                source.index_coverage,
                IndexCoverage::Complete { ref indexes } if indexes.is_empty()
            ));
            assert!(matches!(
                source.generated_expressions[2],
                ColumnReadExpression::Planned(Expr::Binary {
                    lhs: ref left,
                    operator: ast::Operator::Multiply,
                    ..
                }) if matches!(left.as_ref(), Expr::Column(reference) if reference.source == source.id && reference.column == 0)
            ));
        }
    }

    #[test]
    fn update_freezes_only_matching_update_triggers() {
        let schema = schema_with_insert_triggers();
        let value = analyze_sql_with_schema(&schema, "UPDATE writable SET value = 'changed'")
            .expect("triggered UPDATE binds");
        value
            .validate()
            .expect("triggered UPDATE produces closed HIR");
        let HirRoot::Update(update) = &value.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let hir::UpdateTargetKind::BTree { triggers, .. } = &update.target_kind else {
            panic!("catalog table UPDATE has B-tree metadata");
        };
        assert_eq!(
            triggers
                .iter()
                .map(|trigger| trigger.value().name.as_str())
                .collect::<Vec<_>>(),
            ["update_value", "update_all"]
        );
        assert!(triggers
            .iter()
            .all(|trigger| trigger.database() == Some(DatabaseId::new(MAIN_DB_ID))));

        let rowid = analyze_sql_with_schema(&schema, "UPDATE writable SET rowid = 9")
            .expect("rowid-alias UPDATE binds matching triggers");
        let HirRoot::Update(update) = &rowid.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let hir::UpdateTargetKind::BTree { triggers, .. } = &update.target_kind else {
            panic!("catalog table UPDATE has B-tree metadata");
        };
        assert_eq!(
            triggers
                .iter()
                .map(|trigger| trigger.value().name.as_str())
                .collect::<Vec<_>>(),
            ["update_id", "update_all"]
        );
    }

    #[test]
    fn update_freezes_foreign_keys_against_new_and_child_rows() {
        let schema = schema_with_insert_foreign_keys();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE fk_items SET parent_id = 2, parent_code = 'two' WHERE id = 1",
        )
        .expect("foreign-key UPDATE binds");
        document
            .validate()
            .expect("foreign-key UPDATE produces closed HIR");

        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let hir::UpdateTargetKind::BTree { foreign_keys, .. } = &update.target_kind else {
            panic!("catalog table UPDATE has B-tree metadata");
        };
        assert_eq!(foreign_keys.outgoing.len(), 2);
        assert_eq!(foreign_keys.incoming.len(), 1);
        assert!(foreign_keys.outgoing.iter().all(|foreign_key| {
            foreign_key.child_source == update.new_source
                && foreign_key.child_table.value().get_name() == "fk_items"
                && foreign_key.parent_table.value().get_name() == "parents"
        }));

        let parent_code = foreign_keys
            .outgoing
            .iter()
            .find(|foreign_key| foreign_key.declaration.child_columns[0] == "parent_code")
            .expect("indexed parent foreign key is present");
        assert_eq!(parent_code.child_positions.as_ref(), [2]);
        assert_eq!(parent_code.parent_positions.as_ref(), [1]);
        assert_eq!(
            parent_code
                .parent_unique_index
                .as_ref()
                .expect("non-rowid parent key uses a UNIQUE index")
                .value()
                .name,
            "parents_code"
        );

        let incoming = &foreign_keys.incoming[0];
        assert_ne!(incoming.child_source, update.target);
        assert_ne!(incoming.child_source, update.new_source);
        assert_eq!(incoming.child_table.value().get_name(), "item_notes");
        assert_eq!(incoming.parent_table.value().get_name(), "fk_items");
        let child_source = document
            .source(incoming.child_source)
            .expect("incoming child scan source exists");
        assert!(matches!(child_source.kind, SourceKind::Table(_)));
        assert!(matches!(
            &child_source.generated_expressions[1],
            ColumnReadExpression::Planned(Expr::Binary { lhs, .. })
                if matches!(lhs.as_ref(), Expr::Column(column)
                    if column.source == incoming.child_source && column.column == 0)
        ));
    }

    #[test]
    fn update_keeps_row_assignment_duplicate_and_rowid_rules() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET (value, id) = ('first', 8), value = 'last', rowid = 9",
        )
        .expect("row assignments and duplicate targets bind");
        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        assert_eq!(update.assignments.len(), 2);
        assert_eq!(update.assignments[0].columns, [TargetColumn::Column(1)]);
        assert!(matches!(
            update.assignments[0].value,
            Expr::Literal(ast::Literal::String(ref value)) if value.contains("last")
        ));
        assert_eq!(update.assignments[1].columns, [TargetColumn::Column(0)]);
        assert!(matches!(
            update.assignments[1].value,
            Expr::Literal(ast::Literal::Numeric(ref value)) if value == "9"
        ));

        let generated = analyze_sql_with_schema(&schema, "UPDATE writable SET doubled = 4")
            .expect_err("generated columns cannot be assigned");
        assert!(generated.to_string().contains("generated column"));
        let missing = analyze_sql_with_schema(&schema, "UPDATE writable SET absent = 4")
            .expect_err("missing assignment target fails");
        assert_eq!(
            missing.to_string(),
            "Parse error: no such column: writable.absent"
        );

        let default = analyze_sql_with_schema(&schema, "UPDATE writable SET value = DEFAULT")
            .expect("SET DEFAULT binds the target default");
        let HirRoot::Update(default) = &default.root else {
            panic!("UPDATE produces UPDATE root");
        };
        assert!(matches!(
            default.assignments[0].value,
            Expr::Literal(ast::Literal::String(ref value)) if value.contains("fallback")
        ));
    }

    #[test]
    fn update_binds_row_subquery_as_one_assignment() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET (value, id) = (SELECT value, id)",
        )
        .expect("row subquery assignment binds");
        document
            .validate()
            .expect("row subquery assignment produces closed HIR");

        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        assert_eq!(update.assignments.len(), 1);
        assert_eq!(
            update.assignments[0].columns,
            [TargetColumn::Column(1), TargetColumn::Column(0)]
        );
        let Expr::Subquery(SubqueryExpr::Row { query }) = update.assignments[0].value else {
            panic!("multi-column SET keeps one row subquery");
        };
        let query = document.query(query).expect("assignment query exists");
        assert_eq!(query.output.len(), 2);
        assert_eq!(query.captures, [update.target]);
        assert_eq!(
            query.blocks[0].outputs[0].type_fact.storage,
            Some(Type::Text)
        );
        assert_eq!(
            query.blocks[0].outputs[1].type_fact.storage,
            Some(Type::Integer)
        );
    }

    #[test]
    fn update_row_subquery_keeps_width_and_duplicate_rules() {
        let schema = schema_with_writable_table();
        let error =
            analyze_sql_with_schema(&schema, "UPDATE writable SET (value, id) = (SELECT value)")
                .expect_err("row subquery width mismatch fails");
        assert_eq!(
            error.to_string(),
            "Parse error: 2 columns assigned 1 values"
        );

        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable \
             SET (value, id) = (SELECT 'first', 8), value = 'last'",
        )
        .expect("later scalar assignment replaces one row-subquery output");
        document
            .validate()
            .expect("split row assignment produces closed HIR");
        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        assert_eq!(update.assignments.len(), 2);
        assert!(matches!(
            update.assignments[0].value,
            Expr::Literal(ast::Literal::String(ref value)) if value.contains("last")
        ));
        assert!(matches!(
            update.assignments[1].value,
            Expr::Subquery(SubqueryExpr::Scalar { output: 1, .. })
        ));

        let duplicate = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET (value, value) = (SELECT 'first', 'last')",
        )
        .expect("duplicate row targets keep the last output");
        let HirRoot::Update(update) = &duplicate.root else {
            panic!("UPDATE produces UPDATE root");
        };
        assert_eq!(update.assignments.len(), 1);
        assert!(matches!(
            update.assignments[0].value,
            Expr::Subquery(SubqueryExpr::Scalar { output: 1, .. })
        ));
    }

    #[test]
    fn update_composes_array_setters_and_keeps_subquery_captures() {
        let arrays = schema_with_array_columns();
        let document = analyze_sql_with_schema(
            &arrays,
            "UPDATE arrays \
             SET vals = array_set_element(vals, 1, 2), \
                 vals = array_set_element(vals, 2, 3)",
        )
        .expect("repeated array setters bind");
        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let Expr::Function(outer) = &update.assignments[0].value else {
            panic!("last array setter remains the assignment");
        };
        let FunctionArguments::Expressions { values, .. } = &outer.arguments else {
            panic!("array setter has expression arguments");
        };
        assert!(matches!(values[0], Expr::Function(_)));

        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET value = (SELECT value) WHERE EXISTS (SELECT id)",
        )
        .expect("UPDATE expressions may own correlated subqueries");
        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = &update.assignments[0].value
        else {
            panic!("SET subquery remains explicit");
        };
        assert_eq!(
            document.query(*query).expect("SET query exists").captures,
            [update.target]
        );
        let Some(Expr::Subquery(SubqueryExpr::Exists(query))) = update.predicate.as_ref() else {
            panic!("WHERE subquery remains explicit");
        };
        assert_eq!(
            document.query(*query).expect("WHERE query exists").captures,
            [update.target]
        );
    }

    #[test]
    fn update_returning_reads_new_row_with_base_table_visibility() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable AS old SET value = 'changed' \
             RETURNING writable.*, value COLLATE nocase AS folded",
        )
        .expect("UPDATE RETURNING binds");
        document
            .validate()
            .expect("UPDATE RETURNING produces closed HIR");

        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let returning = update.returning.as_ref().expect("RETURNING is preserved");
        assert_eq!(
            returning
                .outputs
                .iter()
                .map(|output| (output.id, output.name.as_str(), output.name_kind))
                .collect::<Vec<_>>(),
            [
                (OutputId::root(0), "id", OutputNameKind::StarExpansion),
                (OutputId::root(1), "value", OutputNameKind::StarExpansion),
                (OutputId::root(2), "doubled", OutputNameKind::StarExpansion),
                (OutputId::root(3), "folded", OutputNameKind::ExplicitAlias),
            ]
        );
        assert!(returning.outputs.iter().all(|output| {
            let mut uses_other_source = false;
            output.expr.walk(&mut |expression| {
                if matches!(expression, Expr::Column(column) if column.source != update.new_source)
                {
                    uses_other_source = true;
                }
            });
            !uses_other_source
        }));
        assert!(returning.outputs[3].collation_is_explicit);
        assert!(document.output(OutputId::root(3)).is_some());

        let alias = analyze_sql_with_schema(
            &schema,
            "UPDATE writable AS old SET value = 'changed' RETURNING old.value",
        )
        .expect_err("UPDATE alias is hidden from RETURNING");
        assert_eq!(alias.to_string(), "Parse error: no such table: old");
    }

    #[test]
    fn update_returning_subqueries_capture_new_row() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET value = 'changed' RETURNING \
             (SELECT value) AS copied, EXISTS(SELECT 1 WHERE id = 3) AS found",
        )
        .expect("UPDATE RETURNING subqueries bind");
        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let outputs = &update
            .returning
            .as_ref()
            .expect("RETURNING is preserved")
            .outputs;
        let query_ids = [
            match outputs[0].expr {
                Expr::Subquery(SubqueryExpr::Scalar { query, output: 0 }) => query,
                _ => panic!("first output is scalar subquery"),
            },
            match outputs[1].expr {
                Expr::Subquery(SubqueryExpr::Exists(query)) => query,
                _ => panic!("second output is EXISTS subquery"),
            },
        ];
        for query in query_ids {
            let query = document.query(query).expect("RETURNING child query exists");
            assert_eq!(query.parent, None);
            assert_eq!(query.captures, [update.new_source]);
        }
    }

    #[test]
    fn update_with_scope_reaches_set_where_and_returning_subqueries() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH values_to_use(id, text) AS (VALUES(3, 'from cte')) \
             UPDATE writable \
             SET value = (SELECT text FROM values_to_use) \
             WHERE id = (SELECT id FROM values_to_use) \
             RETURNING (SELECT text FROM values_to_use)",
        )
        .expect("UPDATE clauses share the statement CTE scope");
        document
            .validate()
            .expect("UPDATE WITH produces closed HIR");
        assert_eq!(document.ctes.len(), 1);

        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let mut query_ids = Vec::new();
        update.assignments[0].value.walk(&mut |expression| {
            if let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = expression {
                query_ids.push(*query);
            }
        });
        update
            .predicate
            .as_ref()
            .expect("WHERE is preserved")
            .walk(&mut |expression| {
                if let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = expression {
                    query_ids.push(*query);
                }
            });
        update
            .returning
            .as_ref()
            .expect("RETURNING is preserved")
            .outputs[0]
            .expr
            .walk(&mut |expression| {
                if let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = expression {
                    query_ids.push(*query);
                }
            });
        assert_eq!(query_ids.len(), 3);
        assert!(query_ids.iter().all(|query| {
            document
                .query(*query)
                .expect("clause subquery exists")
                .reachable_ctes
                == [document.ctes[0].id]
        }));
    }

    #[test]
    fn update_with_is_lazy_recursive_and_does_not_shadow_target() {
        let schema = schema_with_writable_table();
        let lazy = analyze_sql_with_schema(
            &schema,
            "WITH broken AS (SELECT missing) UPDATE writable SET value = 'ok'",
        )
        .expect("unused invalid UPDATE CTE stays unbound");
        assert!(lazy.ctes.is_empty());

        let recursive = analyze_sql_with_schema(
            &schema,
            "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL \
                 SELECT x + 1 FROM seq WHERE x < 2) \
             UPDATE writable SET id = (SELECT max(x) FROM seq)",
        )
        .expect("recursive UPDATE CTE binds");
        assert!(matches!(recursive.ctes[0].body, CteBody::Recursive(_)));

        let shadowed = analyze_sql_with_schema(
            &schema,
            "WITH writable(id, value) AS (VALUES(9, 'cte')) \
             UPDATE writable SET value = (SELECT value FROM writable)",
        )
        .expect("CTE name does not shadow UPDATE catalog target");
        let HirRoot::Update(update) = &shadowed.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let target = shadowed
            .source(update.target)
            .expect("target source exists");
        assert!(matches!(target.kind, SourceKind::Table(_)));
        assert_eq!(shadowed.ctes.len(), 1);
    }

    #[test]
    fn update_from_sources_are_root_owned_and_visible_to_set_and_where() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable AS target \
             SET value = source.value \
             FROM writable AS source \
             WHERE target.id = source.id",
        )
        .expect("UPDATE FROM binds");
        document
            .validate()
            .expect("UPDATE FROM produces closed HIR");

        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let from = update.from.as_ref().expect("FROM is preserved");
        let source = document.source(from.first).expect("FROM source exists");
        assert_eq!(source.owner, SourceOwner::Root);
        assert_eq!(source.alias.as_deref(), Some("source"));
        assert!(matches!(
            update.assignments[0].value,
            Expr::Column(column) if column.source == source.id && column.column == 1
        ));
        assert!(matches!(
            update.predicate,
            Some(Expr::Binary { ref lhs, ref rhs, .. })
                if matches!(lhs.as_ref(), Expr::Column(column) if column.source == update.target && column.column == 0)
                    && matches!(rhs.as_ref(), Expr::Column(column) if column.source == source.id && column.column == 0)
        ));
    }

    #[test]
    fn update_from_join_constraints_exclude_the_target() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE writable AS target \
             SET value = right_side.value \
             FROM writable AS left_side \
             JOIN writable AS right_side ON left_side.id = right_side.id \
             WHERE target.id = left_side.id",
        )
        .expect("JOIN constraints see UPDATE FROM sources");
        document
            .validate()
            .expect("joined UPDATE FROM produces closed HIR");
        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let from = update.from.as_ref().expect("FROM is preserved");
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(
            from.joins[0].constraint,
            hir::JoinConstraint::On(Expr::Binary { .. })
        ));

        let error = analyze_sql_with_schema(
            &schema,
            "UPDATE writable AS target SET value = right_side.value \
             FROM writable AS left_side \
             JOIN writable AS right_side ON target.id = right_side.id",
        )
        .expect_err("JOIN constraints cannot see the UPDATE target");
        assert_eq!(error.to_string(), "Parse error: no such table: target");
    }

    #[test]
    fn update_from_keeps_cte_ambiguity_and_returning_rules() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH input(id, value) AS (VALUES(1, 'from cte')) \
             UPDATE writable SET value = input.value \
             FROM input WHERE writable.id = input.id",
        )
        .expect("UPDATE FROM resolves statement CTEs");
        document
            .validate()
            .expect("CTE UPDATE FROM produces closed HIR");
        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let from = update.from.as_ref().expect("FROM is preserved");
        assert!(matches!(
            document.source(from.first).expect("CTE source exists").kind,
            SourceKind::Cte(_)
        ));

        let derived = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET value = input.value \
             FROM (SELECT 1 AS id, 'derived' AS value) AS input \
             WHERE writable.id = input.id",
        )
        .expect("UPDATE FROM resolves derived sources");
        derived
            .validate()
            .expect("derived UPDATE FROM produces closed HIR");
        let HirRoot::Update(update) = &derived.root else {
            panic!("UPDATE produces UPDATE root");
        };
        let source = derived
            .source(update.from.as_ref().expect("FROM is preserved").first)
            .expect("derived source exists");
        let SourceKind::Derived(query) = source.kind else {
            panic!("FROM source remains derived");
        };
        assert_eq!(source.owner, SourceOwner::Root);
        assert_eq!(
            derived.query(query).expect("derived query exists").parent,
            None
        );

        let ambiguous = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET value = 'x' FROM writable AS source WHERE id = source.id",
        )
        .expect_err("target and FROM sources share one resolution level");
        assert_eq!(
            ambiguous.to_string(),
            "Parse error: ambiguous column name: id"
        );

        let returning = analyze_sql_with_schema(
            &schema,
            "UPDATE writable SET value = source.value \
             FROM writable AS source RETURNING source.value",
        )
        .expect_err("RETURNING cannot see UPDATE FROM sources");
        assert_eq!(returning.to_string(), "Parse error: no such table: source");
    }

    #[test]
    fn virtual_table_update_has_only_virtual_target_metadata() {
        let schema = schema_with_virtual_insert_target();
        let document = analyze_sql_with_schema(
            &schema,
            "UPDATE pragma_table_info AS target \
             SET rowid = 9, cid = source.cid \
             FROM pragma_table_info AS source \
             WHERE target.rowid = source.rowid \
             RETURNING pragma_table_info.rowid, cid",
        )
        .expect("virtual-table UPDATE binds");
        document
            .validate()
            .expect("virtual-table UPDATE produces closed HIR");

        let HirRoot::Update(update) = &document.root else {
            panic!("UPDATE produces UPDATE root");
        };
        assert!(matches!(update.target_kind, hir::UpdateTargetKind::Virtual));
        assert_eq!(update.assignments[0].columns, [TargetColumn::RowId]);
        assert_eq!(update.assignments[1].columns, [TargetColumn::Column(0)]);
        let from = update.from.as_ref().expect("FROM is preserved");
        assert!(matches!(
            update.assignments[1].value,
            Expr::Column(column) if column.source == from.first && column.column == 0
        ));
        assert!(matches!(
            update.predicate,
            Some(Expr::Binary { ref lhs, ref rhs, .. })
                if matches!(lhs.as_ref(), Expr::RowId(source) if *source == update.target)
                    && matches!(rhs.as_ref(), Expr::RowId(source) if *source == from.first)
        ));
        let returning = update.returning.as_ref().expect("RETURNING is preserved");
        assert!(matches!(
            returning.outputs[0].expr,
            Expr::RowId(source) if source == update.new_source
        ));
        assert!(matches!(
            returning.outputs[1].expr,
            Expr::Column(column) if column.source == update.new_source && column.column == 0
        ));
        for source in [update.target, update.new_source] {
            let source = document.source(source).expect("UPDATE row source exists");
            assert!(source.check_constraints.is_none());
            assert!(matches!(source.index_coverage, IndexCoverage::Selective));
        }

        let mut invalid = document.clone();
        let HirRoot::Update(update) = &mut invalid.root else {
            panic!("UPDATE produces UPDATE root");
        };
        update.target_kind = hir::UpdateTargetKind::BTree {
            defaults: Vec::new(),
            triggers: Vec::new(),
            foreign_keys: hir::DmlForeignKeys::default(),
        };
        assert_eq!(
            invalid
                .validate()
                .expect_err("target kind must match table")
                .to_string(),
            "B-tree UPDATE metadata belongs to a non-B-tree target"
        );
    }

    #[test]
    fn update_without_rowid_keeps_existing_rejection() {
        let mut schema = Schema::new();
        let table = BTreeTable::from_sql(
            "CREATE TABLE no_rowid(id INTEGER PRIMARY KEY, value TEXT) WITHOUT ROWID",
            2,
        )
        .expect("WITHOUT ROWID table parses");
        schema
            .add_btree_table(Arc::new(table))
            .expect("WITHOUT ROWID table name is unique");

        let error = analyze_sql_with_schema(&schema, "UPDATE no_rowid SET value = 'changed'")
            .expect_err("WITHOUT ROWID UPDATE remains unsupported");
        assert_eq!(
            error.to_string(),
            "Parse error: UPDATE of WITHOUT ROWID tables is not supported"
        );
    }

    #[test]
    fn delete_binds_target_predicate_and_complete_write_metadata() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "DELETE FROM writable AS doomed \
             WHERE doomed.rowid = 7 AND EXISTS (SELECT value)",
        )
        .expect("basic DELETE binds");
        document.validate().expect("DELETE produces closed HIR");

        let HirRoot::Delete(delete) = &document.root else {
            panic!("DELETE produces DELETE root");
        };
        let hir::DeleteTargetKind::BTree {
            triggers,
            foreign_keys,
        } = &delete.target_kind
        else {
            panic!("catalog table DELETE has B-tree metadata");
        };
        assert!(triggers.is_empty());
        assert!(foreign_keys.outgoing.is_empty());
        assert!(foreign_keys.incoming.is_empty());

        let source = document
            .source(delete.target)
            .expect("DELETE target exists");
        assert_eq!(source.alias.as_deref(), Some("doomed"));
        assert!(source.check_constraints.is_none());
        assert!(matches!(
            source.index_coverage,
            IndexCoverage::Complete { ref indexes } if indexes.is_empty()
        ));
        assert!(matches!(
            source.generated_expressions[2],
            ColumnReadExpression::Planned(Expr::Binary { ref lhs, .. })
                if matches!(lhs.as_ref(), Expr::Column(column)
                    if column.source == delete.target && column.column == 0)
        ));

        let predicate = delete.predicate.as_ref().expect("WHERE is preserved");
        assert!(matches!(
            predicate,
            Expr::Binary { lhs, .. }
                if matches!(lhs.as_ref(), Expr::Binary { lhs, .. }
                    if matches!(lhs.as_ref(), Expr::RowId(source) if *source == delete.target))
        ));
        let mut query = None;
        predicate.walk(&mut |expression| {
            if let Expr::Subquery(SubqueryExpr::Exists(id)) = expression {
                query = Some(*id);
            }
        });
        assert_eq!(
            document
                .query(query.expect("WHERE contains EXISTS query"))
                .expect("EXISTS query exists")
                .captures,
            [delete.target]
        );

        let mut invalid = document.clone();
        let HirRoot::Delete(delete) = &mut invalid.root else {
            panic!("DELETE produces DELETE root");
        };
        delete.target_kind = hir::DeleteTargetKind::Virtual;
        assert_eq!(
            invalid
                .validate()
                .expect_err("target kind must match table")
                .to_string(),
            "virtual DELETE metadata belongs to a non-virtual target"
        );
    }

    #[test]
    fn delete_resolves_index_hint_and_closes_every_index() {
        let schema = schema_with_insert_metadata();
        let document = analyze_sql_with_schema(
            &schema,
            "DELETE FROM guarded INDEXED BY guarded_expression WHERE score = 2",
        )
        .expect("indexed DELETE binds");
        document
            .validate()
            .expect("indexed DELETE produces closed HIR");

        let HirRoot::Delete(delete) = &document.root else {
            panic!("DELETE produces DELETE root");
        };
        let source = document
            .source(delete.target)
            .expect("DELETE target exists");
        let hir::IndexHint::Indexed(hint) = &source.index_hint else {
            panic!("INDEXED BY remains explicit");
        };
        assert_eq!(hint.value().name, "guarded_expression");
        let IndexCoverage::Complete { indexes } = &source.index_coverage else {
            panic!("DELETE owns complete index metadata");
        };
        assert_eq!(indexes.len(), 2);
        assert_eq!(source.index_expressions.len(), 2);
        assert!(source.index_expressions.iter().any(|index| {
            index.index.value().name == "guarded_expression"
                && index.columns[0].is_some()
                && index.predicate.is_some()
        }));
    }

    #[test]
    fn delete_freezes_only_target_database_delete_triggers() {
        let schema = schema_with_insert_triggers();
        let document = analyze_sql_with_schema(&schema, "DELETE FROM writable WHERE id = 1")
            .expect("triggered DELETE binds");
        document
            .validate()
            .expect("triggered DELETE produces closed HIR");

        let HirRoot::Delete(delete) = &document.root else {
            panic!("DELETE produces DELETE root");
        };
        let hir::DeleteTargetKind::BTree { triggers, .. } = &delete.target_kind else {
            panic!("catalog table DELETE has B-tree metadata");
        };
        assert_eq!(
            triggers
                .iter()
                .map(|trigger| trigger.value().name.as_str())
                .collect::<Vec<_>>(),
            ["delete_only"]
        );
        assert!(triggers
            .iter()
            .all(|trigger| trigger.database() == Some(DatabaseId::new(MAIN_DB_ID))));
    }

    #[test]
    fn delete_freezes_foreign_keys_against_old_and_child_rows() {
        let schema = schema_with_insert_foreign_keys();
        let document = analyze_sql_with_schema(&schema, "DELETE FROM fk_items WHERE id = 1")
            .expect("foreign-key DELETE binds");
        document
            .validate()
            .expect("foreign-key DELETE produces closed HIR");

        let HirRoot::Delete(delete) = &document.root else {
            panic!("DELETE produces DELETE root");
        };
        let hir::DeleteTargetKind::BTree { foreign_keys, .. } = &delete.target_kind else {
            panic!("catalog table DELETE has B-tree metadata");
        };
        assert_eq!(foreign_keys.outgoing.len(), 2);
        assert_eq!(foreign_keys.incoming.len(), 1);
        assert!(foreign_keys.outgoing.iter().all(|foreign_key| {
            foreign_key.child_source == delete.target
                && foreign_key.child_table.value().get_name() == "fk_items"
                && foreign_key.parent_table.value().get_name() == "parents"
                && !foreign_key.parent_action_guarantees_new_parent
        }));

        let parent_code = foreign_keys
            .outgoing
            .iter()
            .find(|foreign_key| foreign_key.declaration.child_columns[0] == "parent_code")
            .expect("indexed parent foreign key is present");
        assert_eq!(parent_code.child_positions.as_ref(), [2]);
        assert_eq!(parent_code.parent_positions.as_ref(), [1]);
        assert_eq!(
            parent_code
                .parent_unique_index
                .as_ref()
                .expect("non-rowid parent key uses a UNIQUE index")
                .value()
                .name,
            "parents_code"
        );

        let incoming = &foreign_keys.incoming[0];
        assert_ne!(incoming.child_source, delete.target);
        assert_eq!(incoming.child_table.value().get_name(), "item_notes");
        assert_eq!(incoming.parent_table.value().get_name(), "fk_items");
        assert_eq!(incoming.child_positions.as_ref(), [1]);
        assert_eq!(incoming.parent_positions.as_ref(), [0]);
        let child_source = document
            .source(incoming.child_source)
            .expect("incoming child scan source exists");
        assert!(matches!(child_source.kind, SourceKind::Table(_)));
        assert!(matches!(
            &child_source.generated_expressions[1],
            ColumnReadExpression::Planned(Expr::Binary { lhs, .. })
                if matches!(lhs.as_ref(), Expr::Column(column)
                    if column.source == incoming.child_source && column.column == 0)
        ));
    }

    #[test]
    fn delete_returning_reads_deleted_row_with_target_alias_visibility() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "DELETE FROM writable AS deleted WHERE id = 1 \
             RETURNING deleted.*, deleted.rowid, value COLLATE nocase AS folded",
        )
        .expect("DELETE RETURNING binds");
        document
            .validate()
            .expect("DELETE RETURNING produces closed HIR");

        let HirRoot::Delete(delete) = &document.root else {
            panic!("DELETE produces DELETE root");
        };
        let returning = delete.returning.as_ref().expect("RETURNING is preserved");
        assert_eq!(
            returning
                .outputs
                .iter()
                .map(|output| (output.id, output.name.as_str(), output.name_kind))
                .collect::<Vec<_>>(),
            [
                (OutputId::root(0), "id", OutputNameKind::StarExpansion),
                (OutputId::root(1), "value", OutputNameKind::StarExpansion),
                (OutputId::root(2), "doubled", OutputNameKind::StarExpansion),
                (OutputId::root(3), "deleted.rowid", OutputNameKind::Inferred),
                (OutputId::root(4), "folded", OutputNameKind::ExplicitAlias),
            ]
        );
        assert!(returning.outputs.iter().all(|output| {
            let mut uses_other_source = false;
            output.expr.walk(&mut |expression| match expression {
                Expr::Column(column) if column.source != delete.target => {
                    uses_other_source = true;
                }
                Expr::RowId(source) if *source != delete.target => uses_other_source = true,
                _ => {}
            });
            !uses_other_source
        }));
        assert!(matches!(
            returning.outputs[3].expr,
            Expr::RowId(source) if source == delete.target
        ));
        assert!(returning.outputs[4].collation_is_explicit);
        assert_eq!(returning.outputs[4].type_fact.storage, Some(Type::Text));
        assert!(document.output(OutputId::root(4)).is_some());

        let hidden_name = analyze_sql_with_schema(
            &schema,
            "DELETE FROM writable AS deleted RETURNING writable.value",
        )
        .expect_err("DELETE alias hides the original table name");
        assert_eq!(
            hidden_name.to_string(),
            "Parse error: no such table: writable"
        );
    }

    #[test]
    fn delete_returning_subqueries_capture_deleted_row() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "DELETE FROM writable WHERE id = 1 RETURNING \
             (SELECT value) AS copied, EXISTS(SELECT 1 WHERE rowid = 1) AS found",
        )
        .expect("DELETE RETURNING subqueries bind");

        let HirRoot::Delete(delete) = &document.root else {
            panic!("DELETE produces DELETE root");
        };
        let outputs = &delete
            .returning
            .as_ref()
            .expect("RETURNING is preserved")
            .outputs;
        let query_ids = [
            match outputs[0].expr {
                Expr::Subquery(SubqueryExpr::Scalar { query, output: 0 }) => query,
                _ => panic!("first output is scalar subquery"),
            },
            match outputs[1].expr {
                Expr::Subquery(SubqueryExpr::Exists(query)) => query,
                _ => panic!("second output is EXISTS subquery"),
            },
        ];
        for query in query_ids {
            let query = document.query(query).expect("RETURNING child query exists");
            assert_eq!(query.parent, None);
            assert_eq!(query.captures, [delete.target]);
        }
    }

    #[test]
    fn delete_with_scope_reaches_where_and_returning_subqueries() {
        let schema = schema_with_writable_table();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH values_to_use(id, text) AS (VALUES(3, 'from cte')) \
             DELETE FROM writable \
             WHERE id = (SELECT id FROM values_to_use) \
             RETURNING (SELECT text FROM values_to_use)",
        )
        .expect("DELETE clauses share the statement CTE scope");
        document
            .validate()
            .expect("DELETE WITH produces closed HIR");
        assert_eq!(document.ctes.len(), 1);

        let HirRoot::Delete(delete) = &document.root else {
            panic!("DELETE produces DELETE root");
        };
        let mut query_ids = Vec::new();
        delete
            .predicate
            .as_ref()
            .expect("WHERE is preserved")
            .walk(&mut |expression| {
                if let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = expression {
                    query_ids.push(*query);
                }
            });
        delete
            .returning
            .as_ref()
            .expect("RETURNING is preserved")
            .outputs[0]
            .expr
            .walk(&mut |expression| {
                if let Expr::Subquery(SubqueryExpr::Scalar { query, .. }) = expression {
                    query_ids.push(*query);
                }
            });
        assert_eq!(query_ids.len(), 2);
        assert!(query_ids.iter().all(|query| {
            document
                .query(*query)
                .expect("clause subquery exists")
                .reachable_ctes
                == [document.ctes[0].id]
        }));
    }

    #[test]
    fn delete_with_is_lazy_recursive_and_does_not_shadow_target() {
        let schema = schema_with_writable_table();
        let lazy = analyze_sql_with_schema(
            &schema,
            "WITH broken AS (SELECT missing) DELETE FROM writable WHERE id = 1",
        )
        .expect("unused invalid DELETE CTE stays unbound");
        assert!(lazy.ctes.is_empty());

        let recursive = analyze_sql_with_schema(
            &schema,
            "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL \
                 SELECT x + 1 FROM seq WHERE x < 2) \
             DELETE FROM writable WHERE id = (SELECT max(x) FROM seq)",
        )
        .expect("recursive DELETE CTE binds");
        assert!(matches!(recursive.ctes[0].body, CteBody::Recursive(_)));

        let shadowed = analyze_sql_with_schema(
            &schema,
            "WITH writable(id, value) AS (VALUES(9, 'cte')) \
             DELETE FROM writable WHERE id = (SELECT id FROM writable)",
        )
        .expect("CTE name does not shadow DELETE catalog target");
        let HirRoot::Delete(delete) = &shadowed.root else {
            panic!("DELETE produces DELETE root");
        };
        let target = shadowed
            .source(delete.target)
            .expect("target source exists");
        assert!(matches!(target.kind, SourceKind::Table(_)));
        assert_eq!(shadowed.ctes.len(), 1);
    }

    #[test]
    fn virtual_table_delete_has_only_virtual_target_metadata() {
        let schema = schema_with_virtual_insert_target();
        let document = analyze_sql_with_schema(
            &schema,
            "WITH chosen(row) AS (VALUES(9)) \
             DELETE FROM pragma_table_info AS target \
             WHERE target.rowid = (SELECT row FROM chosen) \
             RETURNING target.rowid, target.cid",
        )
        .expect("virtual-table DELETE binds");
        document
            .validate()
            .expect("virtual-table DELETE produces closed HIR");

        let HirRoot::Delete(delete) = &document.root else {
            panic!("DELETE produces DELETE root");
        };
        assert!(matches!(delete.target_kind, hir::DeleteTargetKind::Virtual));
        assert!(matches!(
            delete.predicate,
            Some(Expr::Binary { ref lhs, ref rhs, .. })
                if matches!(lhs.as_ref(), Expr::RowId(source) if *source == delete.target)
                    && matches!(rhs.as_ref(), Expr::Subquery(SubqueryExpr::Scalar { .. }))
        ));
        let returning = delete.returning.as_ref().expect("RETURNING is preserved");
        assert!(matches!(
            returning.outputs[0].expr,
            Expr::RowId(source) if source == delete.target
        ));
        assert!(matches!(
            returning.outputs[1].expr,
            Expr::Column(column) if column.source == delete.target && column.column == 0
        ));
        let source = document
            .source(delete.target)
            .expect("DELETE target exists");
        assert_eq!(source.alias.as_deref(), Some("target"));
        assert!(source.check_constraints.is_none());
        assert!(source.index_expressions.is_empty());
        assert!(matches!(source.index_coverage, IndexCoverage::Selective));
        assert_eq!(document.ctes.len(), 1);

        let mut invalid = document.clone();
        let HirRoot::Delete(delete) = &mut invalid.root else {
            panic!("DELETE produces DELETE root");
        };
        delete.target_kind = hir::DeleteTargetKind::BTree {
            triggers: Vec::new(),
            foreign_keys: hir::DmlForeignKeys::default(),
        };
        assert_eq!(
            invalid
                .validate()
                .expect_err("target kind must match table")
                .to_string(),
            "B-tree DELETE metadata belongs to a non-B-tree target"
        );
    }

    #[test]
    fn basic_delete_rejects_deferred_and_unsupported_targets() {
        let mut without_rowid_schema = Schema::new();
        let table = BTreeTable::from_sql(
            "CREATE TABLE no_rowid(id INTEGER PRIMARY KEY, value TEXT) WITHOUT ROWID",
            2,
        )
        .expect("WITHOUT ROWID table parses");
        without_rowid_schema
            .add_btree_table(Arc::new(table))
            .expect("WITHOUT ROWID table name is unique");
        let without_rowid_error =
            analyze_sql_with_schema(&without_rowid_schema, "DELETE FROM no_rowid")
                .expect_err("WITHOUT ROWID DELETE remains unsupported");
        assert_eq!(
            without_rowid_error.to_string(),
            "Parse error: DELETE from WITHOUT ROWID tables is not supported"
        );
    }
}
