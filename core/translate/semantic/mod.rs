//! SQL semantic analysis.
//!
//! This module is the only layer that turns parser-owned syntax into resolved
//! SQL meaning.  Its output is an owned [`hir::HirDocument`]; planners may not
//! consult the syntax tree to make semantic decisions after analysis succeeds.

pub(crate) mod context;
pub(crate) mod hir;

mod cte;
mod cte_bindings;
mod cte_rules;
mod dml;
mod dml_rules;
mod expr;
mod query;
pub(crate) mod schema_expr;
mod schema_program;
mod scope;
mod sequence;
mod trigger;
mod trigger_rules;

#[cfg(test)]
mod analysis_properties;
#[cfg(test)]
mod scope_properties;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast;

use self::{
    context::SemanticContext,
    hir::{
        BoundSchemaProgram, CatalogObjectId, Cte, CteId, HirDocument, HirRoot, Query, QueryId,
        SchemaProgramId, Source, SourceId,
    },
    schema_program::{SchemaProgramBindingState, SchemaProgramKey},
};
use crate::{schema::Table, sync::Arc, LimboError, Result};

/// Syntax root accepted by semantic analysis.
///
/// Trigger commands and predicates carry their row-image visibility as an
/// explicit input. Runtime parameter and register locations are assigned only
/// when the resulting HIR is lowered.
pub(crate) enum AnalyzeInput<'syntax> {
    Statement(&'syntax ast::Stmt),
    TriggerCommand {
        syntax: &'syntax ast::TriggerCmd,
        trigger: &'syntax TriggerAnalysisInput,
    },
    TriggerPredicate {
        syntax: &'syntax ast::Expr,
        trigger: &'syntax TriggerAnalysisInput,
    },
}

/// Resolve one syntax root into a closed, owned HIR document.
///
/// This is the semantic module's planner-facing operation. Once it succeeds,
/// consumers use only the returned document and may not consult parser syntax
/// to rediscover SQL meaning.
pub(crate) fn analyze(
    context: &SemanticContext<'_>,
    input: AnalyzeInput<'_>,
) -> Result<HirDocument> {
    let mut analyzer = Analyzer::new(context);
    let root = match input {
        AnalyzeInput::Statement(ast::Stmt::Select(select)) => {
            let query = analyzer.analyze_query(select, scope::QueryEnvironment::empty())?;
            HirRoot::Query(hir::QueryRoot {
                query,
                trigger: None,
            })
        }
        AnalyzeInput::Statement(
            statement
            @ (ast::Stmt::Insert { .. } | ast::Stmt::Update(_) | ast::Stmt::Delete { .. }),
        ) => analyzer.analyze_dml_statement(statement, None)?,
        AnalyzeInput::Statement(_) => {
            return Err(LimboError::InternalError(
                "semantic analysis received an unsupported statement kind".to_string(),
            ));
        }
        AnalyzeInput::TriggerCommand { syntax, trigger } => {
            analyzer.analyze_trigger_command(syntax, trigger)?
        }
        AnalyzeInput::TriggerPredicate { syntax, trigger } => {
            analyzer.analyze_trigger_predicate(syntax, trigger)?
        }
    };
    let document = analyzer.finish(root)?;
    document.validate().map_err(|error| {
        LimboError::InternalError(format!("semantic analysis produced invalid HIR: {error}"))
    })?;
    Ok(document)
}

/// Target row images visible to a trigger command or `WHEN` predicate.
///
/// Runtime parameter/register locations are intentionally absent. Trigger
/// execution lowering assigns those only after semantic analysis succeeds.
#[derive(Clone)]
pub(crate) struct TriggerAnalysisInput {
    pub(crate) database_id: usize,
    pub(crate) table: Arc<Table>,
    pub(crate) new_visible: bool,
    pub(crate) old_visible: bool,
    /// Conflict policy inherited from the statement that fired the trigger.
    /// SQLite applies this to INSERT and UPDATE commands in the trigger body.
    pub(crate) override_conflict: Option<ast::ResolveType>,
}

/// Kind component of a resolved catalog identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CatalogObjectKind {
    Table,
    Index,
    Function { argument_count: usize },
    Collation,
    Type,
    Trigger,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CatalogIdentityKey {
    database: Option<usize>,
    kind: CatalogObjectKind,
    name: String,
}

/// Shared state for analyzing one syntax root.
///
/// Query, DML, trigger, and expression modules all extend this type.  Keeping
/// the arenas here guarantees that every identity in the finished document
/// belongs to the same owner and that nested analysis cannot create a second,
/// detached map.
pub(crate) struct Analyzer<'context, 'catalog> {
    context: &'context SemanticContext<'catalog>,
    queries: Vec<Option<Query>>,
    sources: Vec<Option<Source>>,
    /// Catalog tables needed only while resolving source-dependent semantic
    /// rules. Schema-expression HIR deliberately does not retain these
    /// runtime objects.
    source_catalog_tables: HashMap<SourceId, Arc<Table>>,
    /// Source columns whose values participate in this statement. Stored
    /// expressions for these columns are materialized before HIR is finished.
    required_source_columns: HashSet<(SourceId, usize)>,
    ctes: Vec<Option<Cte>>,
    schema_programs: Vec<Option<BoundSchemaProgram>>,
    schema_program_bindings: HashMap<SchemaProgramKey, SchemaProgramBindingState>,
    catalog_ids: HashMap<CatalogIdentityKey, CatalogObjectId>,
    next_aggregate_ids: HashMap<hir::QueryBlockId, usize>,
    next_window_function_ids: HashMap<hir::QueryBlockId, usize>,
}

impl<'context, 'catalog> Analyzer<'context, 'catalog> {
    pub(crate) fn new(context: &'context SemanticContext<'catalog>) -> Self {
        Self {
            context,
            queries: Vec::new(),
            sources: Vec::new(),
            source_catalog_tables: HashMap::default(),
            required_source_columns: HashSet::default(),
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            schema_program_bindings: HashMap::default(),
            catalog_ids: HashMap::default(),
            next_aggregate_ids: HashMap::default(),
            next_window_function_ids: HashMap::default(),
        }
    }

    pub(crate) fn allocate_aggregate_id(&mut self, block: hir::QueryBlockId) -> hir::AggregateId {
        let next = self.next_aggregate_ids.entry(block).or_default();
        let id = hir::AggregateId::new(block, *next);
        *next += 1;
        id
    }

    pub(crate) fn allocate_window_function_id(
        &mut self,
        block: hir::QueryBlockId,
    ) -> hir::WindowFunctionId {
        let next = self.next_window_function_ids.entry(block).or_default();
        let id = hir::WindowFunctionId::new(block, *next);
        *next += 1;
        id
    }

    pub(crate) fn query_block_function_counts(&self, block: hir::QueryBlockId) -> (usize, usize) {
        (
            self.next_aggregate_ids.get(&block).copied().unwrap_or(0),
            self.next_window_function_ids
                .get(&block)
                .copied()
                .unwrap_or(0),
        )
    }

    pub(crate) fn context(&self) -> &'context SemanticContext<'catalog> {
        self.context
    }

    pub(crate) fn reserve_query(&mut self) -> QueryId {
        let id = QueryId::new(self.queries.len());
        self.queries.push(None);
        id
    }

    pub(crate) fn insert_query(&mut self, id: QueryId, query: Query) -> Result<()> {
        if query.id != id {
            return Err(LimboError::InternalError(format!(
                "query {} was inserted into slot {}",
                query.id, id
            )));
        }
        Self::insert_reserved(&mut self.queries, id.index(), query, "query")
    }

    pub(crate) fn query(&self, id: QueryId) -> Option<&Query> {
        self.queries.get(id.index())?.as_ref()
    }

    pub(crate) fn query_mut(&mut self, id: QueryId) -> Option<&mut Query> {
        self.queries.get_mut(id.index())?.as_mut()
    }

    pub(crate) fn reserve_source(&mut self) -> SourceId {
        let id = SourceId::new(self.sources.len());
        self.sources.push(None);
        id
    }

    pub(crate) fn insert_source(&mut self, id: SourceId, source: Source) -> Result<()> {
        if source.id != id {
            return Err(LimboError::InternalError(format!(
                "source {} was inserted into slot {}",
                source.id, id
            )));
        }
        Self::insert_reserved(&mut self.sources, id.index(), source, "source")
    }

    pub(crate) fn source(&self, id: SourceId) -> Option<&Source> {
        self.sources.get(id.index())?.as_ref()
    }

    pub(crate) fn source_mut(&mut self, id: SourceId) -> Option<&mut Source> {
        self.sources.get_mut(id.index())?.as_mut()
    }

    pub(crate) fn bind_source_catalog_table(&mut self, id: SourceId, table: Arc<Table>) {
        self.source_catalog_tables.insert(id, table);
    }

    pub(crate) fn source_catalog_table(&self, id: SourceId) -> Option<&Arc<Table>> {
        self.source_catalog_tables.get(&id)
    }

    /// Mark one source column as semantically required by this statement.
    /// Stored-expression and custom-codec analysis use the same hook so the
    /// final HIR closes every source-dependent program in one pass.
    pub(crate) fn require_source_column(&mut self, source: SourceId, column: usize) {
        self.required_source_columns.insert((source, column));
    }

    pub(crate) fn reserve_cte(&mut self) -> CteId {
        let id = CteId::new(self.ctes.len());
        self.ctes.push(None);
        id
    }

    pub(crate) fn insert_cte(&mut self, id: CteId, cte: Cte) -> Result<()> {
        if cte.id != id {
            return Err(LimboError::InternalError(format!(
                "CTE {} was inserted into slot {}",
                cte.id, id
            )));
        }
        Self::insert_reserved(&mut self.ctes, id.index(), cte, "CTE")
    }

    pub(crate) fn cte(&self, id: CteId) -> Option<&Cte> {
        self.ctes.get(id.index())?.as_ref()
    }

    pub(crate) fn cte_mut(&mut self, id: CteId) -> Option<&mut Cte> {
        self.ctes.get_mut(id.index())?.as_mut()
    }

    pub(crate) fn reserve_schema_program(&mut self) -> SchemaProgramId {
        let id = SchemaProgramId::new(self.schema_programs.len());
        self.schema_programs.push(None);
        id
    }

    pub(crate) fn insert_schema_program(
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

    pub(crate) fn schema_program(&self, id: SchemaProgramId) -> Option<&BoundSchemaProgram> {
        self.schema_programs.get(id.index())?.as_ref()
    }

    pub(crate) fn catalog_object_id(
        &mut self,
        database: Option<usize>,
        kind: CatalogObjectKind,
        name: impl Into<String>,
    ) -> CatalogObjectId {
        let key = CatalogIdentityKey {
            database,
            kind,
            name: name.into(),
        };
        if let Some(id) = self.catalog_ids.get(&key) {
            return *id;
        }
        let id = CatalogObjectId::new(self.catalog_ids.len() as u64);
        self.catalog_ids.insert(key, id);
        id
    }

    pub(crate) fn finish(mut self, root: HirRoot) -> Result<HirDocument> {
        self.materialize_required_source_expressions(&root)?;
        let cdc = self.resolve_cdc_plan(&root)?;
        let mut document = HirDocument {
            snapshot: self.context.snapshot(),
            databases: self.context.database_snapshots(),
            root,
            queries: Self::finish_arena(self.queries, "query")?,
            sources: Self::finish_arena(self.sources, "source")?,
            ctes: Self::finish_arena(self.ctes, "CTE")?,
            schema_programs: Self::finish_arena(self.schema_programs, "schema program")?,
            cdc,
        };
        for index in 0..document.queries.len() {
            let id = QueryId::new(index);
            let captures = document.direct_query_captures(id);
            document.queries[index].captures = captures;
        }
        Ok(document)
    }

    fn resolve_cdc_plan(&mut self, root: &HirRoot) -> Result<Option<hir::CdcPlan>> {
        let target = match root {
            HirRoot::Insert(root) => root.target,
            HirRoot::Update(root) => root.target,
            HirRoot::Delete(root) => root.target,
            HirRoot::Query(_) | HirRoot::TriggerPredicate(_) | HirRoot::SchemaExpressions(_) => {
                return Ok(None)
            }
        };
        let Some(info) = self.context.capture_data_changes().cloned() else {
            return Ok(None);
        };
        let target_name = self
            .sources
            .get(target.index())
            .and_then(Option::as_ref)
            .map(|source| source.name.as_str())
            .ok_or_else(|| {
                LimboError::InternalError("HIR CDC target source is missing".to_string())
            })?;
        if target_name == info.table
            || target_name == crate::translate::pragma::TURSO_CDC_VERSION_TABLE_NAME
        {
            return Ok(None);
        }

        let table = self
            .context
            .main_schema()
            .get_table(&info.table)
            .ok_or_else(|| LimboError::ParseError(format!("no such table: {}", info.table)))?;
        if table.btree().is_none() {
            return Err(LimboError::ParseError(format!(
                "no such table: {}",
                info.table
            )));
        }
        let table_id = self.catalog_object_id(
            Some(crate::MAIN_DB_ID),
            CatalogObjectKind::Table,
            info.table.clone(),
        );
        let table = hir::CatalogObject::new(
            table_id,
            self.context.snapshot(),
            Some(hir::DatabaseId::new(crate::MAIN_DB_ID)),
            table,
        );
        let sequence_name = crate::schema::autoincrement_sequence_name(&info.table);
        let sequence = self
            .context
            .main_schema()
            .get_sequence(&sequence_name)
            .is_some()
            .then(|| {
                self.resolve_sequence_catalog_operation(
                    hir::SequenceOperationKind::NextValue,
                    sequence_name,
                )
            })
            .transpose()?;
        Ok(Some(hir::CdcPlan {
            info,
            table,
            sequence,
        }))
    }

    fn insert_reserved<T>(
        arena: &mut [Option<T>],
        index: usize,
        value: T,
        kind: &str,
    ) -> Result<()> {
        let Some(slot) = arena.get_mut(index) else {
            return Err(LimboError::InternalError(format!(
                "unreserved {kind} slot {index}"
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
}
