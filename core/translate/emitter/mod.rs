// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.
use super::{
    catalog::{CatalogContext, DatabaseScopeContext},
    collate::CollationSeq,
    compound_select::emit_program_for_compound_select,
    emitter::{
        delete::emit_program_for_delete, select::emit_program_for_select,
        update::emit_program_for_update,
    },
    expr::{
        bind_and_rewrite_expr, emit_stored_column, emit_table_column,
        translate_expr_no_constant_opt, walk_expr, BindingBehavior, NoConstantOptReason,
        WalkControl,
    },
    group_by::GroupByMetadata,
    main_loop::{LeftJoinMetadata, LoopLabels, RightJoinMetadata, SemiAntiJoinMetadata},
    order_by::SortMetadata,
    plan::{
        BitSet, HashJoinType, JoinedTable, NonFromClauseSubquery, Plan, ResultSetColumn,
        TableReferences,
    },
    planner::{TableMask, ROWID_STRS},
    trigger_exec::{get_triggers_including_temp, has_triggers_including_temp},
    window::WindowMetadata,
};
use crate::alloc::{TryClone, TursoIteratorExt};
use crate::instrument;
use crate::namespace::{NamespaceId, NamespaceRef};
use crate::schema::{
    BTreeTable, CheckConstraint, Column, ColumnLayout, GeneratedType, Index, IndexColumn, Schema,
    Table, EXPR_INDEX_SENTINEL,
};
use crate::translate::fkeys::FkActionCompileStack;
use crate::translate::plan::{Aggregate, ColumnMask};
use crate::vdbe::{
    affinity::Affinity,
    builder::{CursorKey, CursorType, DmlColumnContext, ProgramBuilder, SelfTableContext},
    insn::{to_u32, ConstraintInfo, InsertFlags, Insn},
    BranchOffset, CursorID,
};
use crate::{
    error::SQLITE_CONSTRAINT_CHECK,
    function::Func,
    sync::Arc,
    util::{check_expr_references_column, exprs_are_equivalent, parse_numeric_literal},
    CaptureDataChangesExt, Connection, DatabaseCatalog, LimboError, Result, RwLock, SymbolTable,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::borrow::Cow;
use std::cell::RefCell;
use turso_parser::ast::{
    self, Expr, Literal, ResolveType, SubqueryType, TableInternalId, TriggerTime,
};

pub(crate) mod delete;
pub(crate) mod gencol;
pub(crate) mod select;
pub(crate) mod update;

/// Initialize EXISTS subquery result registers to 0, but only for subqueries that haven't
/// been evaluated yet (i.e., correlated subqueries that will be evaluated in the loop).
/// Non-correlated EXISTS subqueries are evaluated before the loop and their result_reg
/// is already properly initialized and populated by emit_non_from_clause_subquery.
fn init_exists_result_regs(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    non_from_clause_subqueries: &[NonFromClauseSubquery],
) {
    let _ = walk_expr(expr, &mut |e| {
        if let ast::Expr::SubqueryResult {
            subquery_id,
            query_type: SubqueryType::Exists { result_reg },
            ..
        } = e
        {
            // Only initialize if the subquery hasn't been evaluated yet.
            // Non-correlated EXISTS subqueries are evaluated before the loop and their
            // result_reg is already set correctly. Initializing them here would overwrite
            // the correct result with 0.
            let already_evaluated = non_from_clause_subqueries
                .iter()
                .find(|s| s.internal_id == *subquery_id)
                .is_some_and(|s| s.has_been_evaluated());
            if !already_evaluated {
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
            }
        }
        Ok(WalkControl::Continue)
    });
}

// Would make more sense to not have RwLock for the attached databases and get all the schemas on prepare,
// because there could be some data race where at 1 point you check the attached db, it has a table,
// but after some write it could not be there anymore. However, leaving it as it is to avoid more complicated logic on something that is experimental
#[derive(Debug, Clone)]
pub struct CachedExprReg<'a> {
    pub expr: Cow<'a, ast::Expr>,
    pub reg: usize,
    pub needs_decode: bool,
}

pub type CachedExprRegHit = (usize, bool);

/// Whether SQLite's DQS (double-quoted strings) misfeature is enabled for DML.
/// When `Enabled`, unresolved double-quoted identifiers fall back to string literals;
/// when `Disabled`, they raise "no such column" errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoubleQuotedDml {
    Enabled,
    Disabled,
}

impl DoubleQuotedDml {
    pub fn is_enabled(self) -> bool {
        matches!(self, DoubleQuotedDml::Enabled)
    }
}

impl From<bool> for DoubleQuotedDml {
    fn from(value: bool) -> Self {
        if value {
            DoubleQuotedDml::Enabled
        } else {
            DoubleQuotedDml::Disabled
        }
    }
}

pub struct Resolver<'a> {
    pub(crate) catalog: CatalogContext<'a>,
    pub expr_to_reg_cache_enabled: bool,
    /// Cache entries for previously translated expressions.
    /// The `needs_custom_type_decode` flag is true for hash-join payload registers
    /// that contain raw encoded values and need DECODE applied when read.
    pub expr_to_reg_cache: Vec<CachedExprReg<'a>>,
    /// The column each register stands for, where column references were
    /// rewritten to Expr::Register: index expressions and partial index WHERE
    /// clauses during INSERT and UPDATE, UPSERT DO UPDATE, and NEW./OLD. in
    /// triggers. Comparisons of such a register still use the column's
    /// affinity and collation. Analogous to SQLite's iSelfTab mechanism, but
    /// operates as a side-channel since limbo rewrites the AST rather than
    /// redirecting column reads at codegen time.
    pub register_columns: HashMap<usize, RegisterColumn>,
    /// Affinity metadata for planned scalar subqueries keyed by their internal ID.
    /// This lets comparison affinity follow SQLite rules for expressions like
    /// `(SELECT text_col FROM ...) > some_numeric_expr`.
    pub(crate) subquery_affinities: RefCell<HashMap<TableInternalId, Affinity>>,
    /// Context and metadata for resolving Expr::Column values that use
    /// [TableInternalId::SELF_TABLE] as a placeholder.
    self_table_scope: RefCell<Option<SelfTableScope>>,
    /// One list per enclosing query, mirroring SQLite's NameContext chain
    /// (resolve.c `resolveExprStep`). An aggregate whose argument columns
    /// belong to an enclosing query is computed by that query, not by the
    /// subquery it is written in. When a subquery resolves such an aggregate it
    /// adds it to the enclosing query's list here (the last entry) instead of
    /// keeping it, so the outer query never has to reach in and take it out
    /// later. Each query adds an empty list before planning its subqueries and
    /// removes it — folding the collected aggregates into its own aggregate
    /// list — afterwards.
    enclosing_query_aggregates: RefCell<Vec<Vec<Aggregate>>>,
    /// Controls whether unresolved double-quoted identifiers fall back to string
    /// literals (SQLite's DQS misfeature) in DML statements.
    pub dqs_dml: DoubleQuotedDml,
    #[cfg(feature = "simulator")]
    subquery_unnesting_mode: crate::SubqueryUnnestingMode,
    /// Foreign-key action programs currently being compiled by this resolver.
    ///
    /// This is shared with forked resolvers because `translate_inner` can fork
    /// the resolver while compiling generated foreign-key action SQL. Without
    /// shared state, a self-referential `ON DELETE CASCADE` could fail to see
    /// that its own action program is already being built.
    pub(super) fk_action_compile_stack: FkActionCompileStack,
}

/// The column a register stands for in [Resolver::register_columns].
#[derive(Debug, Clone, Copy)]
pub struct RegisterColumn {
    pub affinity: Affinity,
    /// None for the rowid, which has no collation.
    pub collation: Option<CollationSeq>,
}

impl RegisterColumn {
    /// A column of the table being written, with its affinity and collation.
    pub fn table_column(column: &Column) -> Self {
        Self {
            affinity: column.affinity(),
            collation: crate::translate::collate::column_collation(column),
        }
    }

    /// NEW.x or OLD.x in a trigger. It has the collation of column x but, as
    /// in SQLite, no affinity unless x is the rowid alias.
    /// See https://www.sqlite.org/forum/forumpost/819f2d6627
    pub fn trigger_row_column(column: &Column) -> Self {
        Self {
            affinity: if column.is_rowid_alias() {
                Affinity::Integer
            } else {
                Affinity::None
            },
            collation: crate::translate::collate::column_collation(column),
        }
    }

    pub fn rowid() -> Self {
        Self {
            affinity: Affinity::Integer,
            collation: None,
        }
    }
}

#[derive(Clone)]
struct SelfTableScope {
    context: SelfTableContext,
    affinities: Option<Arc<[Affinity]>>,
}

impl SelfTableScope {
    fn new(context: SelfTableContext) -> Self {
        let affinities = match &context {
            SelfTableContext::ForDML { table, .. } => {
                Some(table.columns().iter().map(|c| c.affinity()).collect())
            }
            SelfTableContext::ForSelect {
                table_ref_id,
                referenced_tables,
            } => referenced_tables
                .find_table_by_internal_id(*table_ref_id)
                .and_then(|(_, table_ref)| table_ref.btree())
                .map(|btree| btree.columns().iter().map(|c| c.affinity()).collect()),
        };

        Self {
            context,
            affinities,
        }
    }

    fn affinity(&self, column: usize) -> Option<Affinity> {
        self.affinities
            .as_ref()
            .and_then(|affinities| affinities.get(column).copied())
    }

    fn column_type_str(&self, column: usize) -> Option<String> {
        match &self.context {
            SelfTableContext::ForDML { table, .. } => {
                table.columns().get(column).map(|c| c.ty_str.clone())
            }
            SelfTableContext::ForSelect {
                table_ref_id,
                referenced_tables,
            } => referenced_tables
                .find_table_by_internal_id(*table_ref_id)
                .and_then(|(_, table_ref)| table_ref.columns().get(column))
                .map(|c| c.ty_str.clone()),
        }
    }
}

#[cfg(test)]
impl Resolver<'static> {
    /// A resolver over an empty main schema with the SQLite dialect. Its state is leaked so
    /// that unit tests can create one in a single call.
    pub(crate) fn empty_for_tests() -> Self {
        Self::new(
            Box::leak(Box::new(Schema::default())),
            Box::leak(Box::new(RwLock::new(HashMap::default()))),
            Box::leak(Box::new(RwLock::new(None))),
            Box::leak(Box::new(RwLock::new(DatabaseCatalog::new()))),
            Box::leak(Box::new(crate::Qualifiers::default())),
            Box::leak(Box::new(SymbolTable::new())),
            true,
            DoubleQuotedDml::Enabled,
            Arc::new(crate::dialect::SqliteDialect),
            None,
        )
    }
}

impl<'a> Resolver<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: &'a Schema,
        database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
        temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
        attached_databases: &'a RwLock<DatabaseCatalog>,
        qualifiers: &'a crate::Qualifiers,
        symbol_table: &'a SymbolTable,
        enable_custom_types: bool,
        dqs_dml: DoubleQuotedDml,
        dialect: Arc<dyn crate::dialect::Dialect>,
        search_path: Option<&[String]>,
    ) -> Self {
        Self {
            catalog: CatalogContext::new(
                schema,
                database_schemas,
                temp_database,
                attached_databases,
                qualifiers,
                symbol_table,
                enable_custom_types,
                dialect,
                search_path,
            ),
            expr_to_reg_cache_enabled: false,
            expr_to_reg_cache: Vec::new(),
            register_columns: HashMap::default(),
            subquery_affinities: RefCell::new(HashMap::default()),
            self_table_scope: RefCell::new(None),
            enclosing_query_aggregates: RefCell::new(Vec::new()),
            dqs_dml,
            #[cfg(feature = "simulator")]
            subquery_unnesting_mode: crate::SubqueryUnnestingMode::Auto,
            fk_action_compile_stack: FkActionCompileStack::default(),
        }
    }

    pub fn schema(&self) -> &Schema {
        self.catalog.schema()
    }

    #[cfg(feature = "simulator")]
    pub(crate) fn set_subquery_unnesting_mode(&mut self, mode: crate::SubqueryUnnestingMode) {
        self.subquery_unnesting_mode = mode;
    }

    #[cfg(feature = "simulator")]
    pub(crate) fn subquery_unnesting_mode(&self) -> crate::SubqueryUnnestingMode {
        self.subquery_unnesting_mode
    }

    pub fn has_temp_database(&self) -> bool {
        self.catalog.has_temp_database()
    }

    pub fn fork(&self) -> Resolver<'a> {
        Resolver {
            catalog: self.catalog.fork(),
            expr_to_reg_cache_enabled: false,
            expr_to_reg_cache: Vec::new(),
            register_columns: HashMap::default(),
            subquery_affinities: RefCell::new(self.subquery_affinities.borrow().clone()),
            self_table_scope: RefCell::new(self.self_table_scope.borrow().clone()),
            enclosing_query_aggregates: RefCell::new(Vec::new()),
            dqs_dml: self.dqs_dml,
            #[cfg(feature = "simulator")]
            subquery_unnesting_mode: self.subquery_unnesting_mode,
            fk_action_compile_stack: self.fk_action_compile_stack.clone(),
        }
    }

    pub fn fork_with_expr_cache(&self) -> Resolver<'a> {
        Resolver {
            catalog: self.catalog.fork(),
            expr_to_reg_cache_enabled: self.expr_to_reg_cache_enabled,
            expr_to_reg_cache: self.expr_to_reg_cache.clone(),
            register_columns: self.register_columns.clone(),
            subquery_affinities: RefCell::new(self.subquery_affinities.borrow().clone()),
            self_table_scope: RefCell::new(self.self_table_scope.borrow().clone()),
            enclosing_query_aggregates: RefCell::new(Vec::new()),
            dqs_dml: self.dqs_dml,
            #[cfg(feature = "simulator")]
            subquery_unnesting_mode: self.subquery_unnesting_mode,
            fk_action_compile_stack: self.fk_action_compile_stack.clone(),
        }
    }

    pub fn require_custom_types(&self, feature: &str) -> crate::Result<()> {
        self.catalog.require_custom_types(feature)
    }

    pub(crate) fn with_self_table_context<T>(
        &self,
        program: &mut ProgramBuilder,
        ctx: Option<&SelfTableContext>,
        f: impl FnOnce(&mut ProgramBuilder, Option<&SelfTableContext>) -> Result<T>,
    ) -> Result<T> {
        match ctx {
            Some(ctx) => {
                let scope = SelfTableScope::new(ctx.clone());
                let prev = self.self_table_scope.borrow_mut().replace(scope);
                let result = f(program, Some(ctx));
                *self.self_table_scope.borrow_mut() = prev;
                result
            }
            None => f(program, None),
        }
    }

    pub(crate) fn with_existing_self_table_context<T>(
        &self,
        f: impl FnOnce(Option<&SelfTableContext>) -> Result<T>,
    ) -> Result<T> {
        let ctx = self
            .self_table_scope
            .borrow()
            .as_ref()
            .map(|scope| scope.context.clone());
        f(ctx.as_ref())
    }

    pub(crate) fn self_table_affinity(&self, column: usize) -> Option<Affinity> {
        self.self_table_scope
            .borrow()
            .as_ref()
            .and_then(|scope| scope.affinity(column))
    }

    /// The table that `SELF_TABLE` column references refer to.
    pub(crate) fn self_table(&self) -> Option<Arc<BTreeTable>> {
        match &self.self_table_scope.borrow().as_ref()?.context {
            SelfTableContext::ForDML { table, .. } => Some(Arc::clone(table)),
            SelfTableContext::ForSelect {
                table_ref_id,
                referenced_tables,
            } => referenced_tables
                .find_table_by_internal_id(*table_ref_id)?
                .1
                .btree(),
        }
    }

    pub(crate) fn self_table_with_custom_type_operators(&self) -> Option<Arc<BTreeTable>> {
        match &self.self_table_scope.borrow().as_ref()?.context {
            SelfTableContext::ForDML {
                table,
                uses_custom_type_operators: true,
                ..
            } => Some(Arc::clone(table)),
            _ => None,
        }
    }

    pub(crate) fn self_table_column_type_str(&self, column: usize) -> Option<String> {
        self.self_table_scope
            .borrow()
            .as_ref()
            .and_then(|scope| scope.column_type_str(column))
    }

    /// Like `CatalogContext::with_database_scope`, but for callers that hold `&mut Resolver`
    /// and need to pass it on mutably to `f` (trigger subprogram compilation
    /// calls `translate_inner`, which takes `&mut Resolver`, once per trigger
    /// command). `f` receives the same `&mut Resolver` back so it can keep
    /// reborrowing it, instead of capturing it from the environment, which
    /// would fight the mutable borrow this method itself takes.
    pub(crate) fn with_database_scope_mut<T>(
        &mut self,
        namespace: NamespaceRef,
        kind: &'static str,
        object_name: String,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let previous = self
            .catalog
            .database_scope
            .borrow_mut()
            .replace(DatabaseScopeContext {
                namespace,
                kind,
                object_name,
            });
        let result = f(self);
        *self.catalog.database_scope.borrow_mut() = previous;
        result
    }

    /// Restrict table resolution to the trigger's own database while `f` runs.
    pub(crate) fn with_trigger_scope<T>(
        &mut self,
        namespace: NamespaceRef,
        trigger_name: String,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        self.with_database_scope_mut(namespace, "trigger", trigger_name, f)
    }

    /// Restrict table resolution to the view's own database while `f` runs,
    /// the same way `with_trigger_scope` does for triggers: a persistent
    /// view's unqualified table names resolve within the database the view
    /// lives in, never through a TEMP table that happens to share a source
    /// table's name, matching SQLite's view schema fixer.
    pub(crate) fn with_view_scope<T>(
        &self,
        namespace: NamespaceRef,
        view_name: String,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        self.catalog.with_view_scope(namespace, view_name, f)
    }

    pub fn resolve_function(
        &self,
        func_name: &str,
        arg_count: usize,
    ) -> Result<Option<Func>, LimboError> {
        self.catalog.resolve_function(func_name, arg_count)
    }

    pub(crate) fn enable_expr_to_reg_cache(&mut self) {
        self.expr_to_reg_cache_enabled = true;
    }

    /// Start collecting aggregates that this query's subqueries find to belong
    /// to this query, before planning those subqueries.
    /// [`Self::take_aggregates_from_subqueries`] retrieves them afterwards.
    pub(crate) fn begin_collecting_aggregates_from_subqueries(&self) {
        self.enclosing_query_aggregates
            .borrow_mut()
            .push(Vec::new());
    }

    /// Stop collecting and return the aggregates this query's subqueries moved
    /// up to it.
    pub(crate) fn take_aggregates_from_subqueries(&self) -> Vec<Aggregate> {
        self.enclosing_query_aggregates
            .borrow_mut()
            .pop()
            .unwrap_or_default()
    }

    /// Number of aggregates moved up so far to the innermost collecting query.
    pub(crate) fn count_aggregates_moved_from_subqueries(&self) -> usize {
        self.enclosing_query_aggregates
            .borrow()
            .last()
            .map_or(0, Vec::len)
    }

    /// The function name of the aggregate at `index` among those moved up to
    /// the innermost collecting query, if there is one.
    pub(crate) fn aggregate_moved_from_subqueries(&self, index: usize) -> Option<String> {
        self.enclosing_query_aggregates
            .borrow()
            .last()
            .and_then(|collected| collected.get(index))
            .map(|agg| agg.func.to_string())
    }

    /// Move an aggregate up to the innermost enclosing query that is
    /// collecting. Returns false when there is none — the aggregate then stays
    /// with the query that resolved it.
    pub(crate) fn move_aggregate_to_enclosing_query(&self, agg: Aggregate) -> bool {
        match self.enclosing_query_aggregates.borrow_mut().last_mut() {
            Some(collected) => {
                collected.push(agg);
                true
            }
            None => false,
        }
    }

    pub fn cache_expr_reg(&mut self, expr: Cow<'a, ast::Expr>, reg: usize, needs_decode: bool) {
        self.expr_to_reg_cache.push(CachedExprReg {
            expr,
            reg,
            needs_decode,
        });
    }

    pub fn resolve_collation(&self, name: &str) -> Result<CollationSeq> {
        self.catalog.resolve_collation(name)
    }

    /// Returns the register and decode flag for a previously translated expression.
    ///
    /// We scan from newest to oldest so later translations win when equivalent
    /// expressions are seen multiple times in the same translation pass.
    /// Returns `(register, needs_custom_type_decode)`.
    pub fn resolve_cached_expr_reg(&self, expr: &ast::Expr) -> Option<CachedExprRegHit> {
        if self.expr_to_reg_cache_enabled {
            self.expr_to_reg_cache
                .iter()
                .rev()
                .find(|entry| exprs_are_equivalent(expr, &entry.expr))
                .map(|entry| (entry.reg, entry.needs_decode))
        } else {
            None
        }
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        self.catalog.with_schema(database_id, f)
    }

    pub(crate) fn attached_database_ids_in_search_order(&self) -> Result<BitSet> {
        self.catalog.attached_database_ids_in_search_order()
    }

    pub(crate) fn resolve_unqualified_existing<F>(
        &self,
        object_name: &str,
        namespace_has_object: F,
    ) -> Result<NamespaceRef>
    where
        F: Fn(&Schema, NamespaceId, &str) -> bool,
    {
        self.catalog
            .resolve_unqualified_existing(object_name, namespace_has_object)
    }

    /// The namespace of the table, view or materialized view named
    /// `qualified_name`.
    pub(crate) fn resolve_existing_table(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        self.catalog.resolve_existing_table(qualified_name)
    }

    /// Like `resolve_existing_table`, for a table that a statement reads or
    /// writes. A table in a database that does not exist is a missing table
    /// here, as in SQLite's sqlite3LocateTable.
    pub(crate) fn resolve_queried_table(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        self.catalog.resolve_queried_table(qualified_name)
    }

    pub(crate) fn resolve_existing_table_named(&self, table_name: &str) -> Result<NamespaceRef> {
        self.catalog.resolve_existing_table_named(table_name)
    }

    pub(crate) fn find_existing_table(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<Option<NamespaceRef>> {
        self.catalog.find_existing_table(qualified_name)
    }

    pub(crate) fn resolve_existing_index(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        self.catalog.resolve_existing_index(qualified_name)
    }

    pub(crate) fn find_existing_index(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<Option<NamespaceRef>> {
        self.catalog.find_existing_index(qualified_name)
    }

    pub(crate) fn resolve_existing_trigger(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        self.catalog.resolve_existing_trigger(qualified_name)
    }

    /// The namespace of a sequence named without a qualifier: the first
    /// namespace of the search order that has it, or the first namespace of
    /// the search order when none has it.
    pub(crate) fn resolve_existing_sequence(&self, seq_name: &str) -> Result<NamespaceRef> {
        self.catalog.resolve_existing_sequence(seq_name)
    }

    /// The namespace and bare name of the sequence whose mangled name is
    /// `mangled`.
    pub(crate) fn sequence_of_mangled_name(&self, mangled: &str) -> Option<(NamespaceRef, String)> {
        self.catalog.sequence_of_mangled_name(mangled)
    }

    pub(crate) fn find_existing_sequence(&self, seq_name: &str) -> Option<NamespaceRef> {
        self.catalog.find_existing_sequence(seq_name)
    }

    /// `seq_name` qualified with the namespace it resolved to, for the
    /// opcodes that look a sequence up again by its name when the statement
    /// runs.
    pub(crate) fn runtime_sequence_name(&self, namespace: NamespaceRef, seq_name: &str) -> String {
        self.catalog.runtime_sequence_name(namespace, seq_name)
    }

    /// The name that object `name` of `namespace` is stored under in the
    /// schema rows and the SQL text of its database.
    pub(crate) fn mangle_name(&self, namespace: NamespaceRef, name: &str) -> String {
        crate::namespace::mangle_name(namespace.id, name)
    }

    /// The name to write into SQL text for the object `written` of
    /// `namespace`: the name as written for a root namespace, else the
    /// mangled name.
    pub(crate) fn name_in_sql(&self, namespace: NamespaceRef, written: &ast::Name) -> ast::Name {
        if namespace.id.is_root() {
            return written.clone();
        }
        ast::Name::exact(self.mangle_name(namespace, written.as_str()))
    }

    /// The namespace that the qualifier of `qualified_name` refers to, or
    /// the one an unqualified name is in: the scope's own namespace inside
    /// the body of a trigger or a view, else the root of main.
    pub(crate) fn resolve_namespace(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        self.catalog.resolve_namespace(qualified_name)
    }

    /// Resolve the namespace that CREATE puts an object named
    /// `qualified_name` in.
    pub(crate) fn resolve_namespace_for_create(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        self.catalog.resolve_namespace_for_create(qualified_name)
    }

    /// The namespace called `name`, whatever the statement is being
    /// compiled for. `name` is normalized.
    pub(crate) fn namespace_named(&self, name: &str) -> Option<NamespaceRef> {
        self.catalog.namespace_named(name)
    }

    /// Get the database name for a given database index.
    /// Returns "main" for index 0, "temp" for index 1, and the alias for attached databases.
    pub(crate) fn namespace_name(&self, index: usize) -> Option<String> {
        self.catalog.namespace_name(index)
    }

    /// The name of `namespace`: the database's name for a root namespace,
    /// else the namespace's name.
    pub(crate) fn name_of_namespace(&self, namespace: NamespaceRef) -> Option<String> {
        self.catalog.name_of_namespace(namespace)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LimitCtx {
    /// Register holding the LIMIT value (e.g. LIMIT 5)
    pub reg_limit: usize,
    /// Whether to initialize the LIMIT counter to the LIMIT value;
    /// There are cases like compound SELECTs where all the sub-selects
    /// utilize the same limit register, but it is initialized only once.
    pub initialize_counter: bool,
}

/// Identifies a value stored in a materialized hash-build input.
///
/// These references are used to map payload registers back to the original
/// table expressions during hash-probe evaluation. They are deliberately
/// table-qualified so payloads can span multiple tables when the build input
/// is derived from a join prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MaterializedColumnRef {
    /// A concrete column from a specific table, including rowid alias metadata.
    Column {
        table_id: TableInternalId,
        column_idx: usize,
        is_rowid_alias: bool,
    },
    /// The implicit rowid (or integer primary key) of a specific table.
    RowId { table_id: TableInternalId },
}

/// Describes how a hash-join build input was materialized.
///
/// Rowid-only materialization preserves prior join constraints while keeping
/// the hash table payload small, but requires `SeekRowid` into the build table
/// during probing. Key+payload materialization stores the join keys and needed
/// payload columns directly so the hash build can operate without seeking.
#[derive(Debug, Clone)]
pub enum MaterializedBuildInputMode {
    /// Ephemeral table contains only build-side rowids.
    RowidOnly,
    /// Ephemeral table contains join keys followed by payload columns.
    KeyPayload {
        /// Number of join keys stored at the start of each row.
        num_keys: usize,
        /// Payload columns (after the keys) in ephemeral-table order.
        payload_columns: Vec<MaterializedColumnRef>,
    },
}

/// Metadata for a materialized build input keyed by build table index.
///
/// The cursor refers to the ephemeral table containing the materialized rows.
/// `prefix_tables` tracks which join-prefix tables were captured so we can
/// prune redundant scans from downstream join orders.
#[derive(Debug, Clone)]
pub struct MaterializedBuildInput {
    /// Cursor id for the ephemeral table holding the materialized rows.
    pub cursor_id: CursorID,
    /// Encoding mode for the materialized rows.
    pub mode: MaterializedBuildInputMode,
    /// Join-prefix table indices folded into this materialization.
    pub prefix_tables: TableMask,
}

impl MaterializedBuildInput {
    pub(crate) fn requires_build_table(&self) -> bool {
        matches!(self.mode, MaterializedBuildInputMode::RowidOnly)
    }
}

impl LimitCtx {
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            reg_limit: program.alloc_register(),
            initialize_counter: true,
        }
    }

    pub fn new_shared(reg_limit: usize) -> Self {
        Self {
            reg_limit,
            initialize_counter: false,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HashLabels {
    /// Label for hash join match processing (points to just after HashProbe instruction)
    /// Used by HashNext to jump back to process additional matches without re-probing
    pub match_found: BranchOffset,
    /// Label for advancing to the next hash match (points to HashNext instruction).
    /// When conditions fail within a hash join, they should jump here to try the next
    /// hash match, rather than jumping to the outer loop's next label.
    pub next: BranchOffset,
    /// Jump target for unmatched probe rows (outer joins only).
    pub check_outer: Option<BranchOffset>,
    /// Entry label for the inner-loop subroutine.
    pub inner_loop_gosub: Option<BranchOffset>,
    /// Return label for the inner-loop subroutine.
    pub inner_loop_return: Option<BranchOffset>,
    /// Label that skips past the subroutine body (resolved after Return).
    pub inner_loop_skip: Option<BranchOffset>,
    /// Label for the grace loop's own HashNext (resolved during grace loop emission).
    pub grace_hash_next: Option<BranchOffset>,
}

impl HashLabels {
    pub fn new(match_found: BranchOffset, next: BranchOffset) -> Self {
        Self {
            match_found,
            next,
            check_outer: None,
            inner_loop_gosub: None,
            inner_loop_return: None,
            inner_loop_skip: None,
            grace_hash_next: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashCtx {
    pub match_reg: usize,
    pub hash_table_reg: usize,
    pub labels: HashLabels,
    /// Starting register where payload columns are stored after HashProbe/HashNext.
    /// None if payload optimization is not used for this hash join.
    pub payload_start_reg: Option<usize>,
    /// Column references stored in payload, in order.
    /// `payload_start_reg + i` contains the value for `payload_columns[i]`.
    /// These references may point at multiple tables when a build input was
    /// materialized from a join prefix.
    pub payload_columns: Vec<MaterializedColumnRef>,
    pub build_table_cursor_id: Option<CursorID>,
    pub join_type: HashJoinType,
    /// Gosub register for the inner-loop subroutine wrapping subsequent tables.
    /// Outer hash joins wrap inner loops so unmatched-row paths can re-enter via Gosub.
    pub inner_loop_gosub_reg: Option<usize>,
    /// Probe-side rowid register for grace hash join (from RowId before HashProbe).
    pub probe_rowid_reg: Option<usize>,
    /// Starting register for probe key values.
    pub key_start_reg: usize,
    /// Number of join keys.
    pub num_keys: usize,
    /// Register: 0 during main probe loop, 1 during grace loop.
    /// Used by IfPos dispatch before HashNext to route to the grace loop's HashNext.
    pub grace_flag_reg: Option<usize>,
}

/// The TranslateCtx struct holds various information and labels used during bytecode generation.
/// It is used for maintaining state and control flow during the bytecode
/// generation process.
pub struct TranslateCtx<'a> {
    // A typical query plan is a nested loop. Each loop has its own LoopLabels (see the definition of LoopLabels for more details)
    pub labels_main_loop: Vec<LoopLabels>,
    // label for the instruction that jumps to the next phase of the query after the main loop
    // we don't know ahead of time what that is (GROUP BY, ORDER BY, etc.)
    pub label_main_loop_end: Option<BranchOffset>,
    // First register of the aggregation results
    pub reg_agg_start: Option<usize>,
    // In non-group-by statements with aggregations (e.g. SELECT foo, bar, sum(baz) FROM t),
    // we want to emit the non-aggregate columns (foo and bar) only once.
    // This register is a flag that tracks whether we have already done that.
    pub reg_nonagg_emit_once_flag: Option<usize>,
    // First register of the result columns of the query
    pub reg_result_cols_start: Option<usize>,
    pub limit_ctx: Option<LimitCtx>,
    // The register holding the offset value, if any.
    pub reg_offset: Option<usize>,
    // The register holding the limit+offset value, if any.
    pub reg_limit_offset_sum: Option<usize>,
    // metadata for the group by operator
    pub meta_group_by: Option<GroupByMetadata>,
    // metadata for the order by operator
    pub meta_sort: Option<SortMetadata>,
    /// Match state for each JOIN that keeps unmatched left rows.
    ///
    /// Left-row and right-row match state are separate because a FULL JOIN needs both.
    /// This matches SQLite's `WhereLevel.iLeftJoin` and `WhereLevel.pRJ` fields.
    pub meta_left_joins: Vec<Option<LeftJoinMetadata>>,
    /// Match state for each JOIN that keeps unmatched right rows.
    pub meta_right_joins: Vec<Option<RightJoinMetadata>>,
    /// mapping between table loop index and associated metadata (for semi/anti joins)
    pub meta_semi_anti_joins: Vec<Option<SemiAntiJoinMetadata>>,
    pub resolver: Resolver<'a>,
    /// Hash table contexts for hash joins, keyed by build table index.
    pub hash_table_contexts: HashMap<usize, HashCtx>,
    /// Materialized build inputs for hash joins, keyed by build table index.
    /// These entries are reused during nested materialization so we avoid
    /// re-scanning prefix tables and preserve prior join constraints.
    pub materialized_build_inputs: HashMap<usize, MaterializedBuildInput>,
    /// A list of expressions that are not aggregates, along with a flag indicating
    /// whether the expression should be included in the output for each group.
    ///
    /// Each entry is a tuple:
    /// - `&'ast Expr`: the expression itself
    /// - `bool`: `true` if the expression should be included in the output for each group, `false` otherwise.
    ///
    /// The order of expressions is **significant**:
    /// - First: all `GROUP BY` expressions, in the order they appear in the `GROUP BY` clause.
    /// - Then: remaining non-aggregate expressions that are not part of `GROUP BY`.
    pub non_aggregate_expressions: Vec<(&'a Expr, bool)>,
    /// Unique leaf column expressions extracted from aggregate function arguments.
    /// Only populated when GROUP BY uses a sorter, enabling deferred expression
    /// evaluation: the sorter stores raw columns instead of pre-computed expressions,
    /// and full expressions are re-evaluated from the pseudo cursor during aggregation.
    /// An expression that the selected index stores is kept whole.
    pub agg_leaf_columns: Vec<Expr>,
    /// Cursor id for cdc table (if capture_data_changes PRAGMA is set and query can modify the data)
    pub cdc_cursor_id: Option<usize>,
    pub meta_window: Option<WindowMetadata<'a>>,
    /// Metadata stored during `open_loop` for `Search::InSeek`, consumed by `close_loop`.
    pub meta_in_seeks: Vec<Option<InSeekMetadata>>,
    pub unsafe_testing: bool,
}

/// Metadata for the two-level loop emitted by `Search::InSeek`.
#[derive(Debug)]
pub struct InSeekMetadata {
    pub ephemeral_cursor_id: CursorID,
    pub outer_loop_start: BranchOffset,
    pub next_val_label: BranchOffset,
}

impl<'a> TranslateCtx<'a> {
    pub fn new(
        program: &mut ProgramBuilder,
        resolver: Resolver<'a>,
        table_count: usize,
        unsafe_testing: bool,
    ) -> Self {
        TranslateCtx {
            labels_main_loop: (0..table_count).map(|_| LoopLabels::new(program)).collect(),
            label_main_loop_end: None,
            reg_agg_start: None,
            reg_nonagg_emit_once_flag: None,
            limit_ctx: None,
            reg_offset: None,
            reg_limit_offset_sum: None,
            reg_result_cols_start: None,
            meta_group_by: None,
            meta_left_joins: (0..table_count).map(|_| None).collect(),
            meta_right_joins: (0..table_count).map(|_| None).collect(),
            meta_semi_anti_joins: (0..table_count).map(|_| None).collect(),
            meta_sort: None,
            hash_table_contexts: HashMap::default(),
            materialized_build_inputs: HashMap::default(),
            resolver,
            non_aggregate_expressions: Vec::new(),
            agg_leaf_columns: Vec::new(),
            cdc_cursor_id: None,
            meta_window: None,
            meta_in_seeks: (0..table_count).map(|_| None).collect(),
            unsafe_testing,
        }
    }
}

#[derive(Debug, Clone)]
/// Update row source for UPDATE statements
/// `Normal` is the default mode, it will iterate either the table itself or an index on the table.
/// `PrebuiltEphemeralTable` is used when an ephemeral table containing the target rowids to update has
/// been built and it is being used for iteration.
pub enum UpdateRowSource {
    /// Iterate over the table itself or an index on the table
    Normal,
    /// Iterate over an ephemeral table containing the target rowids to update
    PrebuiltEphemeralTable {
        /// The cursor id of the ephemeral table that is being used to iterate the target rowids to update.
        ephemeral_table_cursor_id: usize,
        /// The table that is being updated.
        target_table: Arc<JoinedTable>,
    },
}

/// Used to distinguish database operations
#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug, Clone)]
pub enum OperationMode {
    SELECT,
    INSERT,
    UPDATE(UpdateRowSource),
    DELETE,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Sqlite always considers Read transactions implicit
pub enum TransactionMode {
    None,
    Read,
    Write,
    Concurrent,
}

/// Main entry point for emitting bytecode for a SQL query
/// Takes a query plan and generates the corresponding bytecode program
#[instrument(skip_all, level = tracing::Level::DEBUG)]
#[turso_macros::trace_stack]
pub fn emit_program(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    plan: Plan,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    match plan {
        Plan::Select(plan) => emit_program_for_select(program, resolver, *plan),
        Plan::Delete(plan) => emit_program_for_delete(connection, resolver, program, *plan),
        Plan::Update(plan) => emit_program_for_update(connection, resolver, program, *plan, after),
        mut plan @ Plan::CompoundSelect { .. } => {
            emit_program_for_compound_select(program, resolver, &mut plan).map(|_| ())
        }
        Plan::RecursiveCte(mut recursive_cte) => {
            super::recursive_cte::emit_recursive_cte(program, resolver, &mut recursive_cte)
                .map(|_| ())
        }
    }
}

/// Returns the single-column schema used by rowid-only hash build inputs.
fn build_rowid_column() -> Column {
    Column::new_default_integer(Some("build_rowid".to_string()), "INTEGER".to_string(), None)
}

pub fn prepare_cdc_if_necessary(
    program: &mut ProgramBuilder,
    schema: &Schema,
    database_id: usize,
    changed_table_name: Option<&str>,
) -> Result<Option<(usize, Arc<BTreeTable>)>> {
    let mode = program.capture_data_changes_info();
    let cdc_table = mode.table();
    let Some(cdc_table) = cdc_table else {
        return Ok(None);
    };
    if database_id == crate::TEMP_DB_ID {
        return Ok(None);
    }
    // Self-exclusion: never capture changes to CDC's own bookkeeping tables. `None` means the
    // caller has no associated table (e.g. a transaction-boundary COMMIT record) and always
    // gets the cursor.
    if let Some(changed_table_name) = changed_table_name {
        if changed_table_name == cdc_table
            || changed_table_name == crate::cdc::TURSO_CDC_VERSION_TABLE_NAME
        {
            return Ok(None);
        }
    }
    let Some(turso_cdc_table) = schema.get_table(NamespaceId::ROOT, cdc_table) else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let Some(cdc_btree) = turso_cdc_table.btree() else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(cdc_btree.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: cdc_btree.root_page.into(),
        db: crate::MAIN_DB_ID, // CDC table always lives in the main database
    });
    Ok(Some((cursor_id, cdc_btree)))
}

pub fn emit_cdc_patch_record(
    program: &mut ProgramBuilder,
    table: &Table,
    columns_reg: usize,
    record_reg: usize,
    rowid_reg: usize,
    layout: &ColumnLayout,
) -> usize {
    let columns = table.columns();
    let rowid_alias_position = columns.iter().position(|x| x.is_rowid_alias());
    if let Some(rowid_alias_position) = rowid_alias_position {
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: layout.to_register(columns_reg, rowid_alias_position),
            extra_amount: 0,
        });
        let storable_count = columns.iter().filter(|c| !c.is_virtual_generated()).count();
        let affinity_str = columns
            .iter()
            .filter(|col| !col.is_virtual_generated())
            .map(|col| col.affinity().aff_mask())
            .collect::<String>();

        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(columns_reg),
            count: to_u32(storable_count),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: Some(affinity_str),
        });
        record_reg
    } else {
        record_reg
    }
}

pub(super) fn emit_make_record<'a>(
    program: &mut ProgramBuilder,
    cols: impl IntoIterator<Item = &'a Column>,
    start_reg: usize,
    dest_reg: usize,
) {
    let storable_cols: Vec<&Column> = cols
        .into_iter()
        .filter(|c| !c.is_virtual_generated())
        .collect();
    let storable_count = storable_cols.len();

    let affinity_str: String = storable_cols
        .iter()
        .map(|c| c.affinity().aff_mask())
        .collect();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(start_reg),
        count: to_u32(storable_count),
        dest_reg: to_u32(dest_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
}

pub fn emit_cdc_full_record(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    table_cursor_id: usize,
    rowid_reg: usize,
) -> Result<usize> {
    let storable_count = columns.iter().filter(|c| !c.is_virtual_generated()).count();
    let columns_reg = program.alloc_registers(storable_count + 1);
    let mut slot = 0;
    for (i, column) in columns.iter().enumerate() {
        if column.is_virtual_generated() {
            continue;
        }
        if column.is_rowid_alias() {
            program.emit_insn(Insn::Copy {
                src_reg: rowid_reg,
                dst_reg: columns_reg + 1 + slot,
                extra_amount: 0,
            });
        } else {
            emit_stored_column(
                program,
                table_cursor_id,
                i,
                columns_reg + 1 + slot,
                resolver,
            )?;
        }
        slot += 1;
    }
    let affinity_str = columns
        .iter()
        .filter(|col| !col.is_virtual_generated())
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(columns_reg + 1),
        count: to_u32(storable_count),
        dest_reg: to_u32(columns_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    Ok(columns_reg)
}

#[allow(clippy::too_many_arguments)]
/// Allocate the rowid for a CDC row into `dest_reg`. The CDC table's `change_id`
/// column is `INTEGER PRIMARY KEY`, so the rowid IS the change id.
///
/// In MVCC journal mode the id is drawn from the CDC table's implicit
/// AUTOINCREMENT sequence. This makes change ids monotonic and never reused after
/// CDC rows are pruned, and registers each in-flight allocation with the MVCC
/// store so the sync push loop can call `sequence_watermark_experimental` to
/// avoid advancing the push watermark past a change id that a concurrent
/// transaction commits out of change-id order under snapshot isolation. In WAL
/// mode we keep the cheaper `NewRowid` (max rowid + 1) assignment; the WAL push
/// loop does not depend on the sequence watermark, so its insert path is
/// unchanged and pays no per-row sequence cost.
fn emit_cdc_change_id(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    cdc_cursor_id: usize,
    dest_reg: usize,
) -> Result<()> {
    if !program.is_mvcc_enabled() {
        program.emit_insn(Insn::NewRowid {
            cursor: cdc_cursor_id,
            rowid_reg: dest_reg,
            prev_largest_reg: 0,
        });
        return Ok(());
    }
    let Some(cdc_table) = program
        .capture_data_changes_info()
        .as_ref()
        .map(|info| info.table.clone())
    else {
        return Err(crate::LimboError::InternalError(
            "CDC change-id allocation requested without an active CDC config".to_string(),
        ));
    };
    let seq_name = crate::schema::autoincrement_sequence_name(&cdc_table);
    let seq = resolver
        .with_schema(crate::MAIN_DB_ID, |s| {
            s.get_sequence(NamespaceId::ROOT, &seq_name).cloned()
        })
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing implicit AUTOINCREMENT sequence for CDC table \"{cdc_table}\""
            ))
        })?;
    crate::translate::sequence::emit_disk_read_nextval(
        program,
        resolver,
        NamespaceRef::root_of(crate::MAIN_DB_ID),
        &seq_name,
        &seq,
        dest_reg,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn emit_cdc_insns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    let cdc_info = program.capture_data_changes_info().as_ref();
    match cdc_info.map(|info| info.cdc_version()) {
        Some(crate::CdcVersion::V2) => emit_cdc_insns_v2(
            program,
            resolver,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        ),
        Some(crate::CdcVersion::V1) => emit_cdc_insns_v1(
            program,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        ),
        None => Err(crate::LimboError::InternalError(
            "cdc info not set".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_cdc_insns_v1(
    program: &mut ProgramBuilder,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // v1: (change_id, change_time, change_type, table_name, id, before, after, updates)
    let turso_cdc_registers = program.alloc_registers(8);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
        collation: CollationSeq::Binary,
    };

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE { .. } | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 2);
    program.mark_last_insn_constant();

    program.emit_string8(table_name.to_string(), turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 4,
        extra_amount: 0,
    });

    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 5,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 5, None);
        program.mark_last_insn_constant();
    }

    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0, // todo(sivukhin): properly set value here from sqlite_sequence table when AUTOINCREMENT will be properly implemented in Turso
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(turso_cdc_registers),
        count: to_u32(8),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_cdc_insns_v2(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // v2: (change_id, change_time, change_txn_id, change_type, table_name, id, before, after, updates)
    let turso_cdc_registers = program.alloc_registers(9);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    // change_time = unixepoch()
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
        collation: CollationSeq::Binary,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    // change_txn_id = conn_txn_id(change_id)
    // First allocate the change id (the CDC rowid), then pass it to conn_txn_id
    // for get-or-set. In MVCC mode this draws from the CDC AUTOINCREMENT sequence
    // (see `emit_cdc_change_id`); in WAL mode it is a plain NewRowid.
    let candidate_reg = program.alloc_register();
    emit_cdc_change_id(program, resolver, cdc_cursor_id, candidate_reg)?;
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
        arg_count: 1,
        collation: CollationSeq::Binary,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: candidate_reg,
        dest: turso_cdc_registers + 2,
        func: conn_txn_id_fn_ctx,
    });

    // change_type
    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE { .. } | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    // table_name
    program.emit_string8(table_name.to_string(), turso_cdc_registers + 4);
    program.mark_last_insn_constant();

    // id
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 5,
        extra_amount: 0,
    });

    // before
    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    // after
    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    // updates
    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 8,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 8, None);
        program.mark_last_insn_constant();
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(turso_cdc_registers),
        count: to_u32(9),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: candidate_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

/// Emit a COMMIT record into the CDC table (v2 only).
/// change_type=2, all other data fields NULL.
pub fn emit_cdc_commit_insns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    cdc_cursor_id: usize,
) -> Result<()> {
    // v2 COMMIT record: (NULL, unixepoch(), conn_txn_id(-1), 2, NULL, NULL, NULL, NULL, NULL)
    let regs = program.alloc_registers(9);
    // reg+0: NULL (change_id, autoincrement)
    program.emit_insn(Insn::Null {
        dest: regs,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    // reg+1: change_time = unixepoch()
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
        collation: CollationSeq::Binary,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: regs + 1,
        func: unixepoch_fn_ctx,
    });

    // reg+2: change_txn_id = conn_txn_id(-1)
    // Pass -1 as candidate: if a txn_id exists, return it; if not, -1 is stored (and will be reset).
    let minus_one_reg = program.alloc_register();
    program.emit_int(-1, minus_one_reg);
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
        arg_count: 1,
        collation: CollationSeq::Binary,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: minus_one_reg,
        dest: regs + 2,
        func: conn_txn_id_fn_ctx,
    });

    // reg+3: change_type = 2 (COMMIT)
    program.emit_int(2, regs + 3);
    program.mark_last_insn_constant();

    // reg+4..8: NULL (table_name, id, before, after, updates)
    program.emit_insn(Insn::Null {
        dest: regs + 4,
        dest_end: Some(regs + 8),
    });
    program.mark_last_insn_constant();

    // Allocate the COMMIT record's change id from the same source as row records
    // (the CDC AUTOINCREMENT sequence in MVCC mode) so COMMIT and row change ids
    // stay in one monotonic, never-reused stream.
    let rowid_reg = program.alloc_register();
    emit_cdc_change_id(program, resolver, cdc_cursor_id, rowid_reg)?;

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(regs),
        count: to_u32(9),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

/// Emit the CDC COMMIT record at the end of a statement, once, when in autocommit mode (v2 only).
/// Only the root statement writes it; a trigger or FK action subprogram never does.
pub fn emit_cdc_autocommit_commit(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    cdc_cursor_id: Option<usize>,
) -> Result<()> {
    if program.flags.is_subprogram() {
        return Ok(());
    }
    let cdc_info = program.capture_data_changes_info().as_ref();
    if !cdc_info.is_some_and(|info| info.cdc_version().has_commit_record()) {
        return Ok(());
    }
    if cdc_cursor_id.is_none() && !program.has_emitted_program_call() {
        // Our own target table was not captured (e.g. a TEMP table), and no trigger or FK
        // action ran during this statement that could have captured a row elsewhere.
        return Ok(());
    }

    // Check if we're in autocommit mode; if so, emit a COMMIT record.
    let is_autocommit_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::IsAutocommit),
        arg_count: 0,
        collation: CollationSeq::Binary,
    };
    let autocommit_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: autocommit_reg,
        func: is_autocommit_fn_ctx,
    });

    // IfNot jumps when reg == 0 (not autocommit). Skip the COMMIT in that case.
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::IfNot {
        reg: autocommit_reg,
        target_pc: skip_label,
        jump_if_null: true,
    });

    let cdc_cursor_id = match cdc_cursor_id {
        Some(cdc_cursor_id) => cdc_cursor_id,
        None => {
            // A trigger or FK action fired during this statement may have captured a row
            // elsewhere. Only write the COMMIT record when something was actually captured.
            let minus_one_reg = program.alloc_register();
            program.emit_int(-1, minus_one_reg);
            let txn_id_reg = program.alloc_register();
            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: minus_one_reg,
                dest: txn_id_reg,
                func: crate::function::FuncCtx {
                    func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
                    arg_count: 1,
                    collation: CollationSeq::Binary,
                },
            });
            program.emit_insn(Insn::Eq {
                lhs: txn_id_reg,
                rhs: minus_one_reg,
                target_pc: skip_label,
                flags: crate::vdbe::insn::CmpInsFlags::default(),
                collation: CollationSeq::Binary,
            });
            let (cursor_id, _) =
                prepare_cdc_if_necessary(program, resolver.schema(), crate::MAIN_DB_ID, None)?
                    .expect("CDC is active, so prepare_cdc_if_necessary must return a cursor");
            cursor_id
        }
    };

    emit_cdc_commit_insns(program, resolver, cdc_cursor_id)?;

    program.preassign_label_to_next_insn(skip_label);

    Ok(())
}

/// Emit the CDC COMMIT record for an explicit `COMMIT` statement, gated on the transaction
/// having actually captured a change.
///
/// Data-modifying statements always establish a write transaction before reaching their CDC
/// emission, but an explicit `COMMIT` does not: for an empty or read-only transaction the
/// connection's `tx_state` is still `None`/`Read`. Emitting the record unconditionally would
/// then dirty the CDC table page without a write transaction; the commit path neither flushes
/// nor clears that page, so it leaks into the next transaction and trips the "dirty pages
/// should be empty for read txn" assertion on a later ROLLBACK
/// (https://github.com/tursodatabase/turso/issues/7677).
///
/// `conn_txn_id(-1)` returns the active CDC transaction id, or -1 when nothing was captured.
/// When it is set, the transaction already performed a write (the data-change statement
/// established the write transaction), so inserting the commit record is safe. When it is -1
/// the transaction made no changes and we skip the record entirely, leaving the transaction
/// read-only.
pub fn emit_cdc_explicit_commit_insns(
    program: &mut ProgramBuilder,
    schema: &Schema,
    resolver: &Resolver,
) -> Result<()> {
    let minus_one_reg = program.alloc_register();
    program.emit_int(-1, minus_one_reg);
    let txn_id_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: minus_one_reg,
        dest: txn_id_reg,
        func: crate::function::FuncCtx {
            func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
            arg_count: 1,
            collation: CollationSeq::Binary,
        },
    });

    // Skip the whole record (including the CDC OpenWrite) when no change was captured.
    // `emit_cdc_commit_insns` recomputes `conn_txn_id(-1)` for the record itself; because the
    // opcode is an idempotent get-or-set, the second call returns the same value we gated on.
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::Eq {
        lhs: txn_id_reg,
        rhs: minus_one_reg,
        target_pc: skip_label,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: CollationSeq::Binary,
    });

    // The CDC record write needs a transaction; joins the open one (keeping its mode) if any.
    program.emit_insn(Insn::Transaction {
        db: crate::MAIN_DB_ID,
        tx_mode: TransactionMode::Write,
        schema_cookie: schema.schema_version,
    });

    // A COMMIT record has no associated table, so pass `None` (no self-exclusion check).
    if let Some((cdc_cursor_id, _)) =
        prepare_cdc_if_necessary(program, schema, crate::MAIN_DB_ID, None)?
    {
        emit_cdc_commit_insns(program, resolver, cdc_cursor_id)?;
    }

    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}
/// Initialize the limit/offset counters and registers.
/// In case of compound SELECTs, the limit counter is initialized only once,
/// hence [LimitCtx::initialize_counter] being false in those cases.
pub(crate) fn init_limit(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    limit: &Option<Box<Expr>>,
    offset: &Option<Box<Expr>>,
    end_label: BranchOffset,
) -> Result<()> {
    if t_ctx.limit_ctx.is_none() && limit.is_some() {
        t_ctx.limit_ctx = Some(LimitCtx::new(program));
    }
    let Some(limit_ctx) = &t_ctx.limit_ctx else {
        return Ok(());
    };

    if limit_ctx.initialize_counter {
        if let Some(expr) = limit {
            match expr.as_ref() {
                Expr::Literal(Literal::Numeric(n)) => match parse_numeric_literal(n)? {
                    crate::types::Value::Numeric(crate::Numeric::Integer(value)) => {
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::Integer {
                            value,
                            dest: limit_ctx.reg_limit,
                        });
                    }
                    crate::types::Value::Numeric(crate::Numeric::Float(value)) => {
                        program.emit_insn(Insn::Real {
                            value: value.into(),
                            dest: limit_ctx.reg_limit,
                        });
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::MustBeInt {
                            reg: limit_ctx.reg_limit,
                            target_pc: None,
                        });
                    }
                    _ => unreachable!("parse_numeric_literal only returns Integer or Float"),
                },
                _ => {
                    let r = limit_ctx.reg_limit;

                    _ = translate_expr_no_constant_opt(
                        program,
                        None,
                        expr,
                        r,
                        &t_ctx.resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                    program.emit_insn(Insn::MustBeInt {
                        reg: r,
                        target_pc: None,
                    });
                }
            }
        }
    }

    if t_ctx.reg_offset.is_none() {
        if let Some(expr) = offset {
            let offset_reg = program.alloc_register();
            t_ctx.reg_offset = Some(offset_reg);
            match expr.as_ref() {
                Expr::Literal(Literal::Numeric(n)) => match parse_numeric_literal(n)? {
                    crate::types::Value::Numeric(crate::Numeric::Integer(value)) => {
                        program.emit_insn(Insn::Integer {
                            value,
                            dest: offset_reg,
                        });
                    }
                    crate::types::Value::Numeric(crate::Numeric::Float(value)) => {
                        program.emit_insn(Insn::Real {
                            value: value.into(),
                            dest: offset_reg,
                        });
                        program.emit_insn(Insn::MustBeInt {
                            reg: offset_reg,
                            target_pc: None,
                        });
                    }
                    _ => unreachable!("parse_numeric_literal only returns Integer or Float"),
                },
                _ => {
                    _ = translate_expr_no_constant_opt(
                        program,
                        None,
                        expr,
                        offset_reg,
                        &t_ctx.resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                }
            }
            program.add_comment(program.offset(), "OFFSET counter");
            program.emit_insn(Insn::MustBeInt {
                reg: offset_reg,
                target_pc: None,
            });

            let combined_reg = program.alloc_register();
            t_ctx.reg_limit_offset_sum = Some(combined_reg);
            program.add_comment(program.offset(), "OFFSET + LIMIT");
            program.emit_insn(Insn::OffsetLimit {
                limit_reg: limit_ctx.reg_limit,
                offset_reg,
                combined_reg,
            });
        }
    }

    // exit early if LIMIT 0
    program.emit_insn(Insn::IfNot {
        reg: limit_ctx.reg_limit,
        target_pc: end_label,
        jump_if_null: false,
    });

    Ok(())
}

/// Emits `target_columns`, plus the stored columns needed by `target_columns`, into a
/// DML row context. This takes into account stored columns, and any stored columns
/// required by virtual columns in `target_columns`.
///
/// Non-rowid target columns are allocated in target order. Rowid-alias columns resolve
/// to `rowid_reg`, so callers that need an unpacked contiguous key or record must
/// materialize one from `DmlColumnContext::to_column_reg`.
pub(crate) fn emit_columns_and_dependencies(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    cursor_id: usize,
    rowid_reg: usize,
    target_columns: impl IntoIterator<Item = usize>,
    resolver: &Resolver,
) -> Result<DmlColumnContext> {
    let targets: Vec<usize> = target_columns.into_iter().collect();
    let target_mask: ColumnMask = targets.iter().copied().try_collect()?;
    let non_rowid_targets: Vec<usize> = targets
        .iter()
        .copied()
        .filter(|&idx| !table.columns()[idx].is_rowid_alias())
        .collect();
    let mut non_rowid_target_positions = vec![None; table.columns().len()];
    for (pos, idx) in non_rowid_targets.iter().copied().enumerate() {
        non_rowid_target_positions[idx] = Some(pos);
    }
    let dependencies = table.dependencies_of_columns(targets.iter().copied())?;

    let target_base = if non_rowid_targets.is_empty() {
        0
    } else {
        program.alloc_registers(non_rowid_targets.len())
    };
    let extra_base = {
        let mut dependencies_not_in_targets: ColumnMask = dependencies.try_clone()?;
        dependencies_not_in_targets -= &target_mask;

        let extra_count = table
            .columns()
            .iter()
            .enumerate()
            .filter(|(idx, col)| dependencies_not_in_targets.get(*idx) && !col.is_rowid_alias())
            .count();

        if extra_count > 0 {
            program.alloc_registers(extra_count)
        } else {
            0
        }
    };

    let mut extra_idx = 0;
    let mut pairs = Vec::with_capacity(table.columns().len());
    for (idx, col) in table.columns().iter().enumerate() {
        let reg = if let Some(pos) = non_rowid_target_positions[idx] {
            let reg = target_base + pos;
            if !col.is_virtual_generated() {
                emit_stored_column(program, cursor_id, idx, reg, resolver)?;
            }
            reg
        } else if col.is_rowid_alias() {
            rowid_reg
        } else if dependencies.get(idx) {
            let reg = extra_base + extra_idx;
            emit_stored_column(program, cursor_id, idx, reg, resolver)?;
            extra_idx += 1;
            reg
        } else {
            0
        };
        pairs.push((col, reg));
    }
    let dml_ctx = DmlColumnContext::from_column_reg_mapping(pairs.into_iter());
    if targets
        .iter()
        .all(|&idx| !table.columns()[idx].is_rowid_alias())
    {
        debug_assert!(targets
            .windows(2)
            .all(|w| { dml_ctx.to_column_reg(w[1]) == dml_ctx.to_column_reg(w[0]) + 1 }));
    }

    let table_arc = Arc::new(table.clone());
    gencol::compute_virtual_columns(
        program,
        &table.columns_topo_sort()?,
        &dml_ctx,
        resolver,
        &table_arc,
    )?;

    Ok(dml_ctx)
}

/// Emit code to load the value of an IndexColumn from the OLD image of the row being updated.
/// Handling expression indexes and regular columns
pub(crate) fn emit_index_column_value_old_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: &mut TableReferences,
    table_cursor_id: usize,
    table_internal_id: TableInternalId,
    idx_col: &IndexColumn,
    dest_reg: usize,
) -> Result<()> {
    if let Some(expr) = &idx_col.expr {
        let mut expr = expr.as_ref().clone();
        bind_and_rewrite_expr(
            &mut expr,
            Some(table_references),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;

        let self_table_context = SelfTableContext::ForSelect {
            table_ref_id: table_internal_id,
            referenced_tables: table_references.clone(),
        };
        resolver.with_self_table_context(program, Some(&self_table_context), |program, _| {
            translate_expr_no_constant_opt(
                program,
                Some(table_references),
                &expr,
                dest_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            Ok(())
        })?;
        // For virtual generated column references, apply the column's
        // declared affinity to the computed expression result.
        if idx_col.pos_in_table != EXPR_INDEX_SENTINEL {
            if let Some(table) = program.btree_table_from_cursor(table_cursor_id) {
                let column = &table.columns()[idx_col.pos_in_table];
                if column.is_virtual_generated() {
                    program.emit_column_affinity(dest_reg, column.affinity());
                }
            }
        }
    } else if let Some(generated_column) = generated_column(program, table_cursor_id, idx_col) {
        emit_table_column(
            program,
            table_cursor_id,
            table_internal_id,
            table_references,
            &generated_column,
            idx_col.pos_in_table,
            dest_reg,
            resolver,
        )?;
    } else {
        emit_stored_column(
            program,
            table_cursor_id,
            idx_col.pos_in_table,
            dest_reg,
            resolver,
        )?;
    }
    Ok(())
}

/// Emit code that evaluates a partial index's WHERE for the row under the table
/// cursor, and return the register that holds the result. Return None for an
/// index without a WHERE. The columns are read from the table cursor because it
/// can be on a different row than a scan cursor, e.g. the row that REPLACE deletes.
pub(crate) fn emit_partial_index_where_old_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: &mut TableReferences,
    table_cursor_id: usize,
    table_internal_id: TableInternalId,
    index: &Index,
) -> Result<Option<usize>> {
    let Some(where_clause) = index.bind_where_expr(Some(table_references), resolver)? else {
        return Ok(None);
    };
    let result_reg = program.alloc_register();
    let table_cursor = [(CursorKey::table(table_internal_id), table_cursor_id)];
    program.with_cursor_overrides(&table_cursor, |program| {
        translate_expr_no_constant_opt(
            program,
            Some(table_references),
            &where_clause,
            result_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )
    })?;
    Ok(Some(result_reg))
}

fn generated_column(
    program: &mut ProgramBuilder,
    table_cursor_id: usize,
    idx_col: &IndexColumn,
) -> Option<Column> {
    program
        .btree_table_from_cursor(table_cursor_id)
        .iter()
        .cloned()
        .flat_map(|table| {
            table
                .columns()
                .get(idx_col.pos_in_table)
                .filter(|col| col.is_virtual_generated())
                .cloned()
        })
        .next()
}

/// Emit code to load the value of an IndexColumn from the NEW image of the row being updated.
/// Handling expression indexes and regular columns
#[allow(clippy::too_many_arguments)]
fn emit_index_column_value_new_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    columns_start_reg: usize,
    rowid_reg: usize,
    idx_col: &IndexColumn,
    dest_reg: usize,
    layout: &ColumnLayout,
    table: &Arc<BTreeTable>,
) -> Result<()> {
    if let Some(expr) = &idx_col.expr {
        let expr = expr.as_ref().clone();
        let mut column_regs: Vec<usize> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if col.is_rowid_alias() {
                    rowid_reg
                } else {
                    layout.to_register(columns_start_reg, i)
                }
            })
            .collect();
        crate::translate::expr::emit_dml_expr_index_value(
            program,
            resolver,
            expr,
            columns,
            &mut column_regs,
            table,
            dest_reg,
            idx_col.pos_in_table == EXPR_INDEX_SENTINEL,
        )?;
    } else {
        let col_in_table = columns
            .get(idx_col.pos_in_table)
            .expect("column index out of bounds");
        match col_in_table.generated_type() {
            GeneratedType::Virtual { ref expr, .. } => {
                gencol::emit_gencol_expr_from_registers(
                    program,
                    expr,
                    dest_reg,
                    columns_start_reg,
                    columns,
                    resolver,
                    rowid_reg,
                    layout,
                    table,
                )?;
                program.emit_column_affinity(dest_reg, col_in_table.affinity());
            }
            GeneratedType::NotGenerated => {
                let src_reg = if col_in_table.is_rowid_alias() {
                    rowid_reg
                } else {
                    layout.to_register(columns_start_reg, idx_col.pos_in_table)
                };
                program.emit_insn(Insn::Copy {
                    src_reg,
                    dst_reg: dest_reg,
                    extra_amount: 0,
                });
            }
        }
    }
    Ok(())
}

/// Emit bytecode for evaluating CHECK constraints.
/// Assumes the resolver cache is already populated with column-to-register mappings.
#[allow(clippy::too_many_arguments)]
fn emit_check_constraint_bytecode(
    program: &mut ProgramBuilder,
    check_constraints: &[CheckConstraint],
    resolver: &mut Resolver,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
    referenced_tables: Option<&TableReferences>,
    table_name: &str,
    column_mappings: &[(&str, usize)],
) -> Result<()> {
    for check_constraint in check_constraints {
        let expr_result_reg = program.alloc_register();

        let mut rewritten_expr = check_constraint.expr.clone();
        if let Some(referenced_tables) = referenced_tables {
            let mut binding_tables = referenced_tables.clone();
            if let Some(joined_table) = binding_tables.joined_tables_mut().first_mut() {
                // CHECK expressions come from schema SQL and may use the base table name
                // even when the query references the table through an alias.
                joined_table.identifier = table_name.to_string();
                joined_table.has_alias = false;
            }
            bind_and_rewrite_expr(
                &mut rewritten_expr,
                Some(&mut binding_tables),
                None,
                resolver,
                BindingBehavior::ResultColumnsNotAllowed,
            )?;
        }

        translate_expr_no_constant_opt(
            program,
            referenced_tables,
            &rewritten_expr,
            expr_result_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;

        // CHECK constraint passes if the result is NULL or non-zero (truthy)
        let constraint_passed_label = program.allocate_label();

        // NULL means unknown, which passes CHECK constraints in SQLite
        program.emit_insn(Insn::IsNull {
            reg: expr_result_reg,
            target_pc: constraint_passed_label,
        });

        program.emit_insn(Insn::If {
            reg: expr_result_reg,
            target_pc: constraint_passed_label,
            jump_if_null: false,
        });

        // SQLite reports a failed CHECK by its constraint name, or by the
        // expression's source text exactly as the user wrote it.
        let constraint_name = match (&check_constraint.name, &check_constraint.source) {
            (Some(name), _) => name.clone(),
            (None, Some(source)) => crate::util::dequote(source),
            (None, None) => format!("{}", check_constraint.expr),
        };

        match or_conflict {
            ResolveType::Ignore => {
                program.emit_insn(Insn::Goto {
                    target_pc: skip_row_label,
                });
            }
            // In SQLite, REPLACE does not apply to CHECK constraints — it aborts,
            // same as Abort/Fail/Rollback.
            ResolveType::Abort
            | ResolveType::Fail
            | ResolveType::Rollback
            | ResolveType::Replace => {
                let col_regs: Vec<usize> = column_mappings.iter().map(|(_, r)| *r).collect();
                let col_names: Vec<String> =
                    column_mappings.iter().map(|(n, _)| n.to_string()).collect();
                program.emit_insn(Insn::Halt {
                    constraint_info: Some(Box::new(ConstraintInfo::check(
                        table_name,
                        check_constraint.name.clone(),
                        col_regs,
                        col_names,
                    ))),
                    err_code: SQLITE_CONSTRAINT_CHECK,
                    description: constraint_name.to_string(),
                    on_error: program.constraint_halt_on_error(or_conflict),
                    description_reg: None,
                });
            }
        }

        program.preassign_label_to_next_insn(constraint_passed_label);
    }
    Ok(())
}

/// Returns true if the CHECK constraint expression references any column whose
/// normalized name is in `column_names`. This is used during UPDATE to skip
/// CHECK constraints that only reference columns not in the SET clause, matching
/// SQLite's optimization behavior.
fn check_expr_references_columns(expr: &ast::Expr, column_names: &HashSet<String>) -> bool {
    column_names
        .iter()
        .any(|name| check_expr_references_column(expr, name))
}

/// Emit CHECK constraint evaluation with resolver cache setup and teardown.
/// Takes column-to-register mappings as an iterator to avoid heap allocation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_check_constraints<'a>(
    program: &mut ProgramBuilder,
    check_constraints: &[CheckConstraint],
    resolver: &mut Resolver,
    table_name: &str,
    rowid_reg: usize,
    column_mappings: impl Iterator<Item = (&'a str, usize)>,
    connection: &Arc<Connection>,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
    referenced_tables: Option<&TableReferences>,
) -> Result<()> {
    if connection.check_constraints_ignored() || check_constraints.is_empty() {
        return Ok(());
    }

    let column_mappings: Vec<(&str, usize)> = column_mappings.collect();
    let initial_cache_size = resolver.expr_to_reg_cache.len();
    let joined_table = referenced_tables.and_then(|tables| tables.joined_tables().first());

    // Map rowid aliases to the actual rowid register.
    // We cache both unqualified (Expr::Id) and qualified (Expr::Qualified) forms
    // so that CHECK expressions like `CHECK(rowid > 0)` and `CHECK(t.rowid > 0)` both resolve.
    for rowid_name in ROWID_STRS {
        let rowid_expr = ast::Expr::Id(ast::Name::exact(rowid_name.to_string()));
        resolver.cache_expr_reg(Cow::Owned(rowid_expr), rowid_reg, false);
        let qualified_expr = ast::Expr::Qualified(
            ast::Name::exact(table_name.to_string()),
            ast::Name::exact(rowid_name.to_string()),
        );
        resolver.cache_expr_reg(Cow::Owned(qualified_expr), rowid_reg, false);
    }

    // Map each column to its register (both unqualified and qualified forms).
    for (col_name, register) in column_mappings.iter().copied() {
        let column_expr = ast::Expr::Id(ast::Name::exact(col_name.to_string()));
        resolver.cache_expr_reg(Cow::Owned(column_expr), register, false);
        let qualified_expr = ast::Expr::Qualified(
            ast::Name::exact(table_name.to_string()),
            ast::Name::exact(col_name.to_string()),
        );
        resolver.cache_expr_reg(Cow::Owned(qualified_expr), register, false);
    }

    if let Some(joined_table) = joined_table {
        resolver.cache_expr_reg(
            Cow::Owned(ast::Expr::RowId {
                database: None,
                table: joined_table.internal_id,
            }),
            rowid_reg,
            false,
        );

        for (col_name, register) in column_mappings.iter().copied() {
            if let Some((idx, col)) = joined_table.columns().iter().enumerate().find(|(_, c)| {
                c.name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(col_name))
            }) {
                resolver.cache_expr_reg(
                    Cow::Owned(ast::Expr::Column {
                        database: None,
                        table: joined_table.internal_id,
                        column: idx,
                        is_rowid_alias: col.is_rowid_alias(),
                    }),
                    register,
                    false,
                );
            }
        }
    }

    resolver.enable_expr_to_reg_cache();

    let result = emit_check_constraint_bytecode(
        program,
        check_constraints,
        resolver,
        or_conflict,
        skip_row_label,
        referenced_tables,
        table_name,
        &column_mappings,
    );

    // Always restore resolver state, even on error.
    resolver.expr_to_reg_cache.truncate(initial_cache_size);
    resolver.expr_to_reg_cache_enabled = false;

    result
}
