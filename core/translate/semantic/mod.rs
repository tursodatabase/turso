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
mod expr;
mod query;
pub(crate) mod schema_expr;
mod schema_program;
mod scope;
mod sequence;
mod trigger;

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
    analyzer.finish(root)
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
        }
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
        Ok(HirDocument {
            snapshot: self.context.snapshot(),
            root,
            queries: Self::finish_arena(self.queries, "query")?,
            sources: Self::finish_arena(self.sources, "source")?,
            ctes: Self::finish_arena(self.ctes, "CTE")?,
            schema_programs: Self::finish_arena(self.schema_programs, "schema program")?,
        })
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
