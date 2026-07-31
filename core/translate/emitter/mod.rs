// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.
use super::{
    collate::CollationSeq,
    compound_select::emit_program_for_compound_select,
    emitter::{
        delete::emit_program_for_delete, select::emit_program_for_select,
        update::emit_program_for_update,
    },
    expr::{
        translate_plan_expr, translate_plan_expr_no_constant_opt, walk_expr, NoConstantOptReason,
        WalkControl,
    },
    group_by::GroupByMetadata,
    main_loop::{LeftJoinMetadata, LoopLabels, SemiAntiJoinMetadata},
    order_by::SortMetadata,
    plan::{
        BitSet, HashJoinType, JoinedTable, NonFromClauseSubquery, Plan, PlanCheckConstraint,
        PlanOutputFact, PlanRowDependency, PlanRuntimeBindings, PlanSubqueryType, ResultSetColumn,
        RuntimeOutputBinding, RuntimeOutputDefinition, RuntimeRowBinding, RuntimeSubqueryBinding,
        RuntimeValueBinding, SelectPlan, TableReferences,
    },
    plan_expr::{
        lower_hir_expr, plan_expr_collations, plan_expr_dependencies, plan_exprs_are_equivalent,
        walk_plan_expr, PlanColumnRef, PlanExpr, PlanExprFactSource, PlanIdentityMap, PlanSourceId,
        PlanSubqueryExpr, PlanWalkControl,
    },
    planner::TableMask,
    semantic::schema_expr::analyze_schema_exprs,
    trigger_exec::{get_triggers_including_temp, has_triggers_including_temp},
    window::WindowMetadata,
};
use crate::alloc::{TryClone, TursoIteratorExt};
use crate::instrument;
use crate::schema::{
    BTreeTable, CheckConstraint, Column, ColumnLayout, Schema, Table, EXPR_INDEX_SENTINEL,
};
use crate::translate::fkeys::FkActionCompileStack;
use crate::translate::plan::ColumnMask;
use crate::translate::semantic::hir::ResolvedIndex;
use crate::vdbe::{
    builder::{CursorType, DmlColumnContext, ProgramBuilder},
    insn::{to_u32, InsertFlags, Insn},
    BranchOffset, CursorID,
};
use crate::{
    error::SQLITE_CONSTRAINT_CHECK,
    function::Func,
    sync::Arc,
    turso_assert_ne,
    util::{exprs_are_equivalent, normalize_ident, parse_numeric_literal},
    CaptureDataChangesExt, Connection, Database, DatabaseCatalog, LimboError, Result, RwLock,
    SymbolTable,
};
use rustc_hash::FxHashMap as HashMap;
use std::borrow::Cow;
use std::cell::RefCell;
use turso_parser::ast::{self, Literal, ResolveType, TriggerTime};

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
    expr: &PlanExpr,
    non_from_clause_subqueries: &[NonFromClauseSubquery],
) {
    let _ = walk_plan_expr(expr, &mut |expr| {
        if let PlanExpr::Subquery(PlanSubqueryExpr::Exists(subquery_id)) = expr {
            // Only initialize if the subquery hasn't been evaluated yet.
            // Non-correlated EXISTS subqueries are evaluated before the loop and their
            // result_reg is already set correctly. Initializing them here would overwrite
            // the correct result with 0.
            let subquery = non_from_clause_subqueries
                .iter()
                .find(|subquery| subquery.internal_id == *subquery_id);
            if let Some(NonFromClauseSubquery {
                query_type: PlanSubqueryType::Exists { result_reg },
                ..
            }) = subquery.filter(|subquery| !subquery.has_been_evaluated())
            {
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
            }
        }
        Ok(PlanWalkControl::Continue)
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
    pub collation: CachedExprCollation,
}

#[derive(Debug, Clone)]
pub struct CachedPlanExprReg {
    pub expr: PlanExpr,
    pub reg: usize,
    pub needs_decode: bool,
    pub collation: CachedExprCollation,
}

pub type CachedExprCollation = Option<(CollationSeq, bool)>;
pub type CachedExprRegHit = (usize, bool, CachedExprCollation);

pub use super::semantic::context::DoubleQuotedDml;
use super::semantic::context::TriggerCatalogContext as TriggerDatabaseContext;

pub struct Resolver<'a> {
    schema: &'a Schema,
    database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
    temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
    attached_databases: &'a RwLock<DatabaseCatalog>,
    non_main_schema_cache: RefCell<HashMap<usize, Arc<Schema>>>,
    pub symbol_table: &'a SymbolTable,
    pub expr_to_reg_cache_enabled: bool,
    /// Cache entries for previously translated expressions.
    /// The `needs_custom_type_decode` flag is true for hash-join payload registers
    /// that contain raw encoded values and need DECODE applied when read.
    pub expr_to_reg_cache: Vec<CachedExprReg<'a>>,
    pub plan_expr_to_reg_cache: Vec<CachedPlanExprReg>,
    /// Runtime locations for cursorless plan sources such as NEW, OLD, and
    /// EXCLUDED. This is emitter state, not semantic name-resolution state.
    plan_runtime_bindings: RefCell<PlanRuntimeBindings>,
    pub enable_custom_types: bool,
    /// Controls whether unresolved double-quoted identifiers fall back to string
    /// literals (SQLite's DQS misfeature) in DML statements.
    pub dqs_dml: DoubleQuotedDml,
    /// Schema dialect of the database being compiled against; used when a
    /// fresh placeholder schema must be constructed during resolution.
    pub(crate) dialect: Arc<dyn crate::dialect::Dialect>,
    /// When set, we are compiling a trigger subprogram for this database.
    /// Ordinary triggers are restricted to their own database, but temp-backed
    /// triggers follow SQLite's looser resolution rules and may access objects
    /// across schemas.
    pub(crate) trigger_context: Option<TriggerDatabaseContext>,
    /// Cached flag: true when this connection has an active temp database.
    ///
    /// Computed once at Resolver construction to avoid repeated
    /// `RwLock` reads on every table-name resolution. Safe because a
    /// `Resolver` is short-lived (single translate pass) and a
    /// connection is single-threaded at the VDBE layer: the temp
    /// database can only be initialized / torn down *between*
    /// Resolvers on the same connection, not during. If you add a
    /// path that can initialize the temp database *inside* translate
    /// (e.g. via a nested sub-program), update this field on that
    /// path or switch to a live read.
    has_temp_schema: bool,
    /// Foreign-key action programs currently being compiled by this resolver.
    ///
    /// This is shared with forked resolvers because `translate_inner` can fork
    /// the resolver while compiling generated foreign-key action SQL. Without
    /// shared state, a self-referential `ON DELETE CASCADE` could fail to see
    /// that its own action program is already being built.
    pub(super) fk_action_compile_stack: FkActionCompileStack,
}

impl<'a> Resolver<'a> {
    const MAIN_DB: &'static str = "main";
    const TEMP_DB: &'static str = "temp";

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: &'a Schema,
        database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
        temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
        attached_databases: &'a RwLock<DatabaseCatalog>,
        symbol_table: &'a SymbolTable,
        enable_custom_types: bool,
        dqs_dml: DoubleQuotedDml,
        dialect: Arc<dyn crate::dialect::Dialect>,
    ) -> Self {
        let has_temp_schema = temp_database.read().is_some();
        Self {
            schema,
            database_schemas,
            temp_database,
            attached_databases,
            non_main_schema_cache: RefCell::new(HashMap::default()),
            symbol_table,
            expr_to_reg_cache_enabled: false,
            expr_to_reg_cache: Vec::new(),
            plan_expr_to_reg_cache: Vec::new(),
            plan_runtime_bindings: RefCell::new(PlanRuntimeBindings::default()),
            enable_custom_types,
            dqs_dml,
            dialect,
            trigger_context: None,
            has_temp_schema,
            fk_action_compile_stack: FkActionCompileStack::default(),
        }
    }

    pub fn schema(&self) -> &Schema {
        self.schema
    }

    /// Freeze the read-only catalog and settings used by semantic analysis.
    /// Emission caches and runtime allocation state are intentionally not
    /// copied into this value.
    pub(crate) fn semantic_context(&self) -> super::semantic::context::SemanticContext<'a> {
        super::semantic::context::SemanticContext::new(
            self.schema,
            self.database_schemas,
            self.temp_database,
            self.attached_databases,
            self.symbol_table,
            self.enable_custom_types,
            self.dqs_dml,
            self.dialect.clone(),
        )
    }

    pub fn has_temp_database(&self) -> bool {
        self.has_temp_schema
    }

    pub fn fork(&self) -> Resolver<'a> {
        Resolver {
            schema: self.schema,
            database_schemas: self.database_schemas,
            temp_database: self.temp_database,
            attached_databases: self.attached_databases,
            non_main_schema_cache: RefCell::new(HashMap::default()),
            symbol_table: self.symbol_table,
            expr_to_reg_cache_enabled: false,
            expr_to_reg_cache: Vec::new(),
            plan_expr_to_reg_cache: Vec::new(),
            plan_runtime_bindings: RefCell::new(self.plan_runtime_bindings.borrow().clone()),
            enable_custom_types: self.enable_custom_types,
            dqs_dml: self.dqs_dml,
            dialect: self.dialect.clone(),
            trigger_context: self.trigger_context.clone(),
            has_temp_schema: self.has_temp_schema,
            fk_action_compile_stack: self.fk_action_compile_stack.clone(),
        }
    }

    pub fn fork_with_expr_cache(&self) -> Resolver<'a> {
        Resolver {
            schema: self.schema,
            database_schemas: self.database_schemas,
            temp_database: self.temp_database,
            attached_databases: self.attached_databases,
            non_main_schema_cache: RefCell::new(HashMap::default()),
            symbol_table: self.symbol_table,
            expr_to_reg_cache_enabled: self.expr_to_reg_cache_enabled,
            expr_to_reg_cache: self.expr_to_reg_cache.clone(),
            plan_expr_to_reg_cache: self.plan_expr_to_reg_cache.clone(),
            plan_runtime_bindings: RefCell::new(self.plan_runtime_bindings.borrow().clone()),
            enable_custom_types: self.enable_custom_types,
            dqs_dml: self.dqs_dml,
            dialect: self.dialect.clone(),
            trigger_context: self.trigger_context.clone(),
            has_temp_schema: self.has_temp_schema,
            fk_action_compile_stack: self.fk_action_compile_stack.clone(),
        }
    }

    pub fn require_custom_types(&self, feature: &str) -> crate::Result<()> {
        if !self.enable_custom_types {
            crate::bail_parse_error!("{} require --experimental-custom-types flag", feature);
        }
        Ok(())
    }

    pub fn plan_runtime_bindings(&self) -> std::cell::Ref<'_, PlanRuntimeBindings> {
        self.plan_runtime_bindings.borrow()
    }

    /// Add the runtime locations and frozen output facts owned by one plan scope.
    ///
    /// Callers scope the resolver to that owner so repeated CTE occurrences
    /// can reuse semantic subquery identities without sharing runtime storage.
    pub(crate) fn bind_plan_subqueries(&self, subqueries: &[NonFromClauseSubquery]) {
        let mut bindings = self.plan_runtime_bindings.borrow_mut();
        for subquery in subqueries {
            bindings.bind_subquery(
                subquery.internal_id,
                RuntimeSubqueryBinding {
                    query_type: subquery.query_type.clone(),
                    output_facts: subquery.output_facts.clone(),
                },
            );
        }
    }

    /// Bind this SELECT block's output identities to its stable result-register
    /// range before any nested query can read an alias from the block.
    pub(crate) fn bind_plan_outputs(&self, outputs: &[ResultSetColumn], start_register: usize) {
        let mut bindings = self.plan_runtime_bindings.borrow_mut();
        for (index, output) in outputs.iter().enumerate() {
            let previous = bindings.bind_output(
                output.id,
                RuntimeOutputBinding {
                    value: RuntimeValueBinding::Register {
                        register: start_register + index,
                        needs_decode: false,
                    },
                    fact: PlanOutputFact::from(output),
                    definition: RuntimeOutputDefinition::Plan(output.expr.clone()),
                },
            );
            assert!(
                previous.is_none(),
                "plan output {} was bound by more than one query block",
                output.id
            );
        }
    }

    /// Return the stable register range already owned by this SELECT block.
    /// A partially bound or non-contiguous range is an emitter invariant
    /// violation: one query block must install all of its outputs together.
    pub(crate) fn bound_plan_outputs_start(&self, outputs: &[ResultSetColumn]) -> Option<usize> {
        let first = outputs.first()?;
        let bindings = self.plan_runtime_bindings.borrow();
        let first_binding = bindings.output(first.id)?;
        let RuntimeValueBinding::Register {
            register: start_register,
            needs_decode,
        } = &first_binding.value
        else {
            panic!("plan output {} is not bound to a register", first.id);
        };
        assert!(
            !*needs_decode,
            "plan output {} unexpectedly requires decoding",
            first.id
        );
        let start_register = *start_register;

        for (index, output) in outputs.iter().enumerate() {
            let binding = bindings.output(output.id).unwrap_or_else(|| {
                panic!(
                    "plan output {} is missing from a partially bound query block",
                    output.id
                )
            });
            let RuntimeValueBinding::Register {
                register,
                needs_decode,
            } = &binding.value
            else {
                panic!("plan output {} is not bound to a register", output.id);
            };
            assert!(
                !*needs_decode,
                "plan output {} unexpectedly requires decoding",
                output.id
            );
            assert_eq!(
                *register,
                start_register + index,
                "plan outputs for one query block are not contiguous"
            );
        }
        Some(start_register)
    }

    /// Temporarily replace cursorless source bindings while emitting one row
    /// image. The previous map is restored even when emission returns an
    /// error, so nested trigger/UPSERT lowering cannot leak bindings outward.
    pub fn with_plan_runtime_bindings<T>(
        &self,
        bindings: PlanRuntimeBindings,
        emit: impl FnOnce(&Resolver<'a>) -> Result<T>,
    ) -> Result<T> {
        let previous = self.plan_runtime_bindings.replace(bindings);
        let result = emit(self);
        self.plan_runtime_bindings.replace(previous);
        result
    }

    /// Mutable counterpart used by a statement-level HIR dispatcher. INSERT
    /// lowering still updates resolver-owned emission caches, while the
    /// semantic row-image bindings must remain scoped to this one root.
    pub fn with_plan_runtime_bindings_mut<T>(
        &mut self,
        bindings: PlanRuntimeBindings,
        emit: impl FnOnce(&mut Resolver<'a>) -> Result<T>,
    ) -> Result<T> {
        let previous = self.plan_runtime_bindings.replace(bindings);
        let result = emit(self);
        self.plan_runtime_bindings.replace(previous);
        result
    }

    fn cached_non_main_schema(&self, database_id: usize) -> Arc<Schema> {
        turso_assert_ne!(database_id, crate::MAIN_DB_ID);

        if let Some(schema) = self
            .non_main_schema_cache
            .borrow()
            .get(&database_id)
            .cloned()
        {
            return schema;
        }

        // TEMP uses `temp_db.db.schema` as its single source of truth; skip
        // `database_schemas` which is never populated for TEMP.
        if database_id != crate::TEMP_DB_ID {
            if let Some(schema) = self.database_schemas.read().get(&database_id).cloned() {
                self.non_main_schema_cache
                    .borrow_mut()
                    .insert(database_id, schema.clone());
                return schema;
            }
        }

        let loaded_schema = match database_id {
            crate::TEMP_DB_ID => self
                .temp_database
                .read()
                .as_ref()
                .map(|temp_db| temp_db.db.schema.lock().clone())
                .unwrap_or_else(|| {
                    // Building an empty schema only fails if built-in type SQL is malformed.
                    Arc::new(
                        Schema::with_options_and_symbols(
                            self.enable_custom_types,
                            self.dialect.as_ref(),
                            self.symbol_table,
                        )
                        .expect("built-in type definitions are malformed"),
                    )
                }),
            _ => {
                let attached_dbs = self.attached_databases.read();
                let (db, _pager) = attached_dbs
                    .index_to_data
                    .get(&database_id)
                    .expect("Database ID should be valid after resolve_database_id");
                let schema = db.schema.lock().clone();
                schema
            }
        };

        self.non_main_schema_cache
            .borrow_mut()
            .insert(database_id, loaded_schema.clone());
        loaded_schema
    }

    /// Set trigger database context to restrict table resolution to the trigger's database.
    pub(crate) fn set_trigger_context(&mut self, database_id: usize, trigger_name: String) {
        self.trigger_context = Some(TriggerDatabaseContext::new(database_id, trigger_name));
    }

    pub fn resolve_function(
        &self,
        func_name: &str,
        arg_count: usize,
    ) -> Result<Option<Func>, LimboError> {
        // The dialect owns the function name surface of user SQL; extension
        // functions resolve after it.
        match self.dialect.resolve_function(func_name, arg_count)? {
            Some(func) => Ok(Some(func)),
            None => Ok(self
                .symbol_table
                .resolve_function(func_name, arg_count)
                .map(Func::External)),
        }
    }

    pub(crate) fn enable_expr_to_reg_cache(&mut self) {
        self.expr_to_reg_cache_enabled = true;
    }

    pub fn cache_expr_reg(
        &mut self,
        expr: Cow<'a, ast::Expr>,
        reg: usize,
        needs_decode: bool,
        collation: CachedExprCollation,
    ) {
        self.expr_to_reg_cache.push(CachedExprReg {
            expr,
            reg,
            needs_decode,
            collation,
        });
    }

    pub fn cache_plan_expr_reg(
        &mut self,
        expr: PlanExpr,
        reg: usize,
        needs_decode: bool,
        collation: CachedExprCollation,
    ) {
        self.plan_expr_to_reg_cache.push(CachedPlanExprReg {
            expr,
            reg,
            needs_decode,
            collation,
        });
    }

    pub fn cache_plan_scalar_expr_reg(
        &mut self,
        expr: PlanExpr,
        reg: usize,
        needs_decode: bool,
        facts: &impl PlanExprFactSource,
    ) -> Result<()> {
        let collations = plan_expr_collations(&expr, facts)?;
        let collation = collations
            .explicit
            .map(|collation| (collation, true))
            .or_else(|| collations.implicit.map(|collation| (collation, false)));
        self.cache_plan_expr_reg(expr, reg, needs_decode, collation);
        Ok(())
    }

    /// Cache a scalar expression result together with the collation metadata that
    /// standalone expression translation would have propagated to a parent comparison.
    pub fn cache_scalar_expr_reg(
        &mut self,
        expr: Cow<'a, ast::Expr>,
        reg: usize,
        needs_decode: bool,
        _referenced_tables: &TableReferences,
    ) -> Result<()> {
        // This cache belongs to the legacy syntax emitter. Resolved implicit
        // collations are PlanExpr facts; only explicit COLLATE syntax is valid
        // metadata to recover from a parser expression here.
        let mut collation = None;
        walk_expr(expr.as_ref(), &mut |node| {
            if let ast::Expr::Collate(_, name) = node {
                if collation.is_none() {
                    collation = Some((
                        self.resolve_collation(name.as_str()).unwrap_or_default(),
                        true,
                    ));
                }
                return Ok(WalkControl::SkipChildren);
            }
            Ok(WalkControl::Continue)
        })?;
        self.cache_expr_reg(expr, reg, needs_decode, collation);
        Ok(())
    }

    pub fn resolve_collation(&self, name: &str) -> Result<CollationSeq> {
        if let Some(collation) = self.symbol_table.resolve_collation(name) {
            return Ok(collation);
        }
        CollationSeq::new(name)
    }

    /// Returns the register, decode flag, and collation metadata for a previously translated expression.
    ///
    /// We scan from newest to oldest so later translations win when equivalent
    /// expressions are seen multiple times in the same translation pass.
    /// Returns `(register, needs_custom_type_decode, collation_ctx)`.
    pub fn resolve_cached_expr_reg(&self, expr: &ast::Expr) -> Option<CachedExprRegHit> {
        if self.expr_to_reg_cache_enabled {
            self.expr_to_reg_cache
                .iter()
                .rev()
                .find(|entry| exprs_are_equivalent(expr, &entry.expr))
                .map(|entry| (entry.reg, entry.needs_decode, entry.collation))
        } else {
            None
        }
    }

    pub(crate) fn plan_expr_reads_runtime_binding(&self, expr: &PlanExpr) -> bool {
        // A runtime binding describes the row image in the current emission
        // scope (for example NEW during UPDATE or UPSERT). Cached expressions
        // and index reads may belong to a different image of the same source,
        // so they must not override the scoped binding.
        let bindings = self.plan_runtime_bindings.borrow();
        bindings.has_value_bindings()
            && plan_expr_dependencies(expr).map_or(true, |dependencies| {
                dependencies
                    .sources()
                    .any(|source| bindings.row(source).is_some())
                    || dependencies
                        .outputs
                        .iter()
                        .any(|output| bindings.output(*output).is_some())
            })
    }

    pub fn resolve_cached_plan_expr_reg(&self, expr: &PlanExpr) -> Option<CachedExprRegHit> {
        if self.plan_expr_reads_runtime_binding(expr) {
            return None;
        }
        if self.expr_to_reg_cache_enabled {
            self.plan_expr_to_reg_cache
                .iter()
                .rev()
                .find(|entry| plan_exprs_are_equivalent(expr, &entry.expr))
                .map(|entry| (entry.reg, entry.needs_decode, entry.collation))
        } else {
            None
        }
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        match database_id {
            crate::MAIN_DB_ID => f(self.schema),
            _ => {
                let schema = self.cached_non_main_schema(database_id);
                f(&schema)
            }
        }
    }

    pub(crate) fn attached_database_ids_in_search_order(&self) -> Result<BitSet> {
        Ok(self
            .attached_databases
            .read()
            .index_to_data
            .keys()
            .copied()
            .try_collect()?)
    }

    fn resolve_unqualified_existing_database_id<F>(
        &self,
        object_name: &str,
        schema_contains_object: F,
    ) -> Result<usize>
    where
        F: Fn(&Schema, &str) -> bool,
    {
        // Only check the temp schema when a temp database actually exists.
        // This avoids expensive schema construction/lookup on every table
        // resolution when no temp objects have been created.
        if self.has_temp_schema
            && self.with_schema(crate::TEMP_DB_ID, |schema| {
                schema_contains_object(schema, object_name)
            })
        {
            return Ok(crate::TEMP_DB_ID);
        }

        if self.with_schema(crate::MAIN_DB_ID, |schema| {
            schema_contains_object(schema, object_name)
        }) {
            return Ok(crate::MAIN_DB_ID);
        }

        for database_id in self.attached_database_ids_in_search_order()? {
            if self.with_schema(database_id, |schema| {
                schema_contains_object(schema, object_name)
            }) {
                return Ok(database_id);
            }
        }

        Ok(crate::MAIN_DB_ID)
    }

    fn schema_has_table_like_object(schema: &Schema, table_name: &str) -> bool {
        schema.get_table(table_name).is_some()
            || schema.get_view(table_name).is_some()
            || schema.get_materialized_view(table_name).is_some()
    }

    fn schema_has_index(schema: &Schema, index_name: &str) -> bool {
        schema
            .indexes
            .values()
            .flat_map(|indexes| indexes.iter())
            .any(|index| index.name.eq_ignore_ascii_case(index_name))
    }

    fn schema_has_trigger(schema: &Schema, trigger_name: &str) -> bool {
        schema.get_trigger(trigger_name).is_some()
    }

    fn resolve_schema_table_database_id(table_name: &str) -> Option<usize> {
        if table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME_ALT)
        {
            return Some(crate::TEMP_DB_ID);
        }

        if table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME_ALT)
        {
            return Some(crate::MAIN_DB_ID);
        }

        None
    }

    pub(crate) fn resolve_existing_table_database_id_qualified(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }
        self.resolve_existing_table_database_id(qualified_name.name.as_str())
    }

    pub(crate) fn resolve_existing_table_database_id(&self, table_name: &str) -> Result<usize> {
        if let Some(ref ctx) = self.trigger_context {
            if ctx.restricts_db_references() {
                return Ok(ctx.database_id);
            }

            return self.resolve_unqualified_existing_database_id(
                table_name,
                Self::schema_has_table_like_object,
            );
        }

        if let Some(database_id) = Self::resolve_schema_table_database_id(table_name) {
            return Ok(database_id);
        }

        self.resolve_unqualified_existing_database_id(
            table_name,
            Self::schema_has_table_like_object,
        )
    }

    pub(crate) fn resolve_existing_index_database_id(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }

        let index_name = normalize_ident(qualified_name.name.as_str());
        self.resolve_unqualified_existing_database_id(&index_name, Self::schema_has_index)
    }

    pub(crate) fn resolve_existing_trigger_database_id(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }

        let trigger_name = qualified_name.name.as_str();
        self.resolve_unqualified_existing_database_id(trigger_name, Self::schema_has_trigger)
    }

    /// Resolve database ID from a qualified name
    pub(crate) fn resolve_database_id(&self, qualified_name: &ast::QualifiedName) -> Result<usize> {
        // Check if this is a qualified name (database.table) or unqualified
        let resolved_id = if let Some(db_name) = &qualified_name.db_name {
            let db_name_normalized = normalize_ident(db_name.as_str());
            match db_name_normalized.as_str() {
                "main" => Ok(crate::MAIN_DB_ID),
                "temp" => Ok(crate::TEMP_DB_ID),
                _ => {
                    // Look up attached database
                    if let Some((idx, _attached_db)) =
                        self.get_attached_database(&db_name_normalized)
                    {
                        Ok(idx)
                    } else {
                        Err(LimboError::InvalidArgument(format!(
                            "no such database: {db_name_normalized}"
                        )))
                    }
                }
            }
        } else {
            // Unqualified table name — when compiling a trigger subprogram,
            // resolve to the trigger's database (matching SQLite behavior).
            // Otherwise default to main.
            if let Some(ref ctx) = self.trigger_context {
                if ctx.restricts_db_references() {
                    Ok(ctx.database_id)
                } else {
                    Ok(crate::MAIN_DB_ID)
                }
            } else {
                Ok(0)
            }
        }?;

        // Triggers can only reference tables in their own database.
        // This only fires for explicitly qualified names (e.g. "aux.table")
        // since unqualified names already resolve to the trigger's database above.
        if let Some(ref ctx) = self.trigger_context {
            if !ctx.restricts_db_references() {
                return Ok(resolved_id);
            }
            if resolved_id != ctx.database_id {
                let db_name = qualified_name
                    .db_name
                    .as_ref()
                    .map(|n| n.as_str())
                    .unwrap_or("main");
                return Err(LimboError::ParseError(format!(
                    "trigger {} cannot reference objects in database {}",
                    ctx.trigger_name, db_name
                )));
            }
        }

        Ok(resolved_id)
    }

    // Get an attached database by alias name
    pub(crate) fn get_attached_database(&self, alias: &str) -> Option<(usize, Arc<Database>)> {
        self.attached_databases.read().get_database_by_name(alias)
    }

    /// Get the database name for a given database index.
    /// Returns "main" for index 0, "temp" for index 1, and the alias for attached databases.
    pub(crate) fn get_database_name_by_index(&self, index: usize) -> Option<String> {
        match index {
            crate::MAIN_DB_ID => Some(Self::MAIN_DB.to_string()),
            crate::TEMP_DB_ID => Some(Self::TEMP_DB.to_string()),
            _ => self.attached_databases.read().get_name_by_index(index),
        }
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
#[derive(Debug, Clone)]
pub enum MaterializedColumnRef {
    /// A concrete column with the semantic facts frozen during analysis.
    Column { column: PlanColumnRef },
    /// The implicit rowid (or integer primary key) of a specific table.
    RowId { table_id: PlanSourceId },
}

impl PartialEq for MaterializedColumnRef {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Column { column: lhs }, Self::Column { column: rhs }) => {
                lhs.source == rhs.source && lhs.column == rhs.column
            }
            (Self::RowId { table_id: lhs }, Self::RowId { table_id: rhs }) => lhs == rhs,
            _ => false,
        }
    }
}

impl Eq for MaterializedColumnRef {}

impl std::hash::Hash for MaterializedColumnRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Column { column } => {
                std::hash::Hash::hash(&0_u8, state);
                std::hash::Hash::hash(&column.source, state);
                std::hash::Hash::hash(&column.column, state);
            }
            Self::RowId { table_id } => {
                std::hash::Hash::hash(&1_u8, state);
                std::hash::Hash::hash(table_id, state);
            }
        }
    }
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
    /// Build table cursor (for NullRow in outer joins).
    pub build_cursor_id: Option<CursorID>,
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
    /// mapping between table loop index and associated metadata (for left joins only)
    /// this metadata exists for the right table in a given left join
    pub meta_left_joins: Vec<Option<LeftJoinMetadata>>,
    /// mapping between table loop index and associated metadata (for semi/anti joins)
    pub meta_semi_anti_joins: Vec<Option<SemiAntiJoinMetadata>>,
    pub resolver: Resolver<'a>,
    /// Hash table contexts for hash joins, keyed by build table index.
    pub hash_table_contexts: HashMap<usize, HashCtx>,
    /// Materialized build inputs for hash joins, keyed by build table index.
    /// These entries are reused during nested materialization so we avoid
    /// re-scanning prefix tables and preserve prior join constraints.
    pub materialized_build_inputs: HashMap<usize, MaterializedBuildInput>,
    /// Exact semantic column facts needed when hash payloads detach values
    /// from their source cursors.
    pub source_row_dependencies: HashMap<PlanSourceId, PlanRowDependency>,
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
    pub non_aggregate_expressions: Vec<(&'a PlanExpr, bool)>,
    /// Unique leaf column expressions extracted from aggregate function arguments.
    /// Only populated when GROUP BY uses a sorter, enabling deferred expression
    /// evaluation: the sorter stores raw columns instead of pre-computed expressions,
    /// and full expressions are re-evaluated from the pseudo cursor during aggregation.
    pub agg_leaf_columns: Vec<PlanExpr>,
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
            meta_semi_anti_joins: (0..table_count).map(|_| None).collect(),
            meta_sort: None,
            hash_table_contexts: HashMap::default(),
            materialized_build_inputs: HashMap::default(),
            source_row_dependencies: HashMap::default(),
            resolver,
            non_aggregate_expressions: Vec::new(),
            agg_leaf_columns: Vec::new(),
            cdc_cursor_id: None,
            meta_window: None,
            meta_in_seeks: (0..table_count).map(|_| None).collect(),
            unsafe_testing,
        }
    }

    pub fn with_runtime_bindings(mut self, bindings: PlanRuntimeBindings) -> Self {
        *self.resolver.plan_runtime_bindings.get_mut() = bindings;
        self
    }

    pub fn capture_source_row_dependencies(&mut self, plan: &SelectPlan) -> Result<()> {
        self.source_row_dependencies.clear();
        for table in plan.table_references.joined_tables() {
            self.source_row_dependencies.insert(
                table.internal_id,
                plan.source_row_dependency(table.internal_id)?,
            );
        }
        Ok(())
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
    changed_table_name: Option<&str>,
) -> Result<Option<(usize, Arc<BTreeTable>)>> {
    let mode = program.capture_data_changes_info();
    let cdc_table = mode.table();
    let Some(cdc_table) = cdc_table else {
        return Ok(None);
    };
    // Self-exclusion: never capture changes to CDC's own bookkeeping tables. `None` means the
    // caller has no associated table (e.g. a transaction-boundary COMMIT record) and always
    // gets the cursor.
    if let Some(changed_table_name) = changed_table_name {
        if changed_table_name == cdc_table
            || changed_table_name == crate::translate::pragma::TURSO_CDC_VERSION_TABLE_NAME
        {
            return Ok(None);
        }
    }
    let Some(turso_cdc_table) = schema.get_table(cdc_table) else {
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
        let is_strict = table.btree().is_some_and(|btree| btree.is_strict);
        let affinity_str = columns
            .iter()
            .filter(|col| !col.is_virtual_generated())
            .map(|col| col.affinity_with_strict(is_strict).aff_mask())
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
    is_strict: bool,
) {
    let storable_cols: Vec<&Column> = cols
        .into_iter()
        .filter(|c| !c.is_virtual_generated())
        .collect();
    let storable_count = storable_cols.len();

    let affinity_str: String = storable_cols
        .iter()
        .map(|c| c.affinity_with_strict(is_strict).aff_mask())
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
    columns: &[Column],
    table_cursor_id: usize,
    rowid_reg: usize,
    is_strict: bool,
) -> usize {
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
            program.emit_column_or_rowid(table_cursor_id, i, columns_reg + 1 + slot);
        }
        slot += 1;
    }
    let affinity_str = columns
        .iter()
        .filter(|col| !col.is_virtual_generated())
        .map(|col| col.affinity_with_strict(is_strict).aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(columns_reg + 1),
        count: to_u32(storable_count),
        dest_reg: to_u32(columns_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    columns_reg
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
        .with_schema(crate::MAIN_DB_ID, |s| s.get_sequence(&seq_name).cloned())
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing implicit AUTOINCREMENT sequence for CDC table \"{cdc_table}\""
            ))
        })?;
    crate::translate::sequence::emit_disk_read_nextval(
        program,
        resolver,
        crate::MAIN_DB_ID,
        &seq_name,
        &seq,
        dest_reg,
        None,
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

/// Emit a CDC COMMIT record at end-of-statement when in autocommit mode (v2 only).
/// This should be called once per statement, after the main loop, not per-row.
pub fn emit_cdc_autocommit_commit(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    cdc_cursor_id: usize,
) -> Result<()> {
    let cdc_info = program.capture_data_changes_info().as_ref();
    if cdc_info.is_some_and(|info| info.cdc_version().has_commit_record()) {
        // Check if we're in autocommit mode; if so, emit a COMMIT record.
        let is_autocommit_fn_ctx = crate::function::FuncCtx {
            func: Func::Scalar(crate::function::ScalarFunc::IsAutocommit),
            arg_count: 0,
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

        emit_cdc_commit_insns(program, resolver, cdc_cursor_id)?;

        program.preassign_label_to_next_insn(skip_label);
    }

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
        collation: None,
    });

    // A COMMIT record has no associated table, so pass `None` (no self-exclusion check).
    if let Some((cdc_cursor_id, _)) = prepare_cdc_if_necessary(program, schema, None)? {
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
    limit: &Option<PlanExpr>,
    offset: &Option<PlanExpr>,
) -> Result<()> {
    if t_ctx.limit_ctx.is_none() && limit.is_some() {
        t_ctx.limit_ctx = Some(LimitCtx::new(program));
    }
    let Some(limit_ctx) = &t_ctx.limit_ctx else {
        return Ok(());
    };

    if limit_ctx.initialize_counter {
        if let Some(expr) = limit {
            match expr {
                PlanExpr::Literal(Literal::Numeric(n)) => match parse_numeric_literal(n)? {
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

                    _ = translate_plan_expr(program, None, expr, r, &t_ctx.resolver)?;
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
            match expr {
                PlanExpr::Literal(Literal::Numeric(n)) => match parse_numeric_literal(n)? {
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
                    _ = translate_plan_expr(program, None, expr, offset_reg, &t_ctx.resolver)?;
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
    let main_loop_end = t_ctx
        .label_main_loop_end
        .expect("label_main_loop_end must be set before init_limit");
    program.emit_insn(Insn::IfNot {
        reg: limit_ctx.reg_limit,
        target_pc: main_loop_end,
        jump_if_null: false,
    });

    Ok(())
}

/// Materialize columns using generated expressions planned for this target source.
pub(crate) fn emit_columns_and_dependencies_for_target(
    program: &mut ProgramBuilder,
    target: &JoinedTable,
    cursor_id: usize,
    rowid_reg: usize,
    target_columns: impl IntoIterator<Item = usize>,
    resolver: &Resolver,
) -> Result<DmlColumnContext> {
    let table = target.table.btree().ok_or_else(|| {
        LimboError::InternalError("generated-column target is not a B-tree table".to_string())
    })?;
    emit_columns_and_dependencies(
        program,
        &table,
        cursor_id,
        rowid_reg,
        target_columns,
        |program, dml_ctx| {
            gencol::compute_planned_virtual_columns(program, target, dml_ctx, resolver)
        },
    )
}

/// Materialize columns for a standalone schema path that has no semantic HIR source.
pub(crate) fn emit_columns_and_dependencies_from_schema(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    database_id: usize,
    cursor_id: usize,
    rowid_reg: usize,
    target_columns: impl IntoIterator<Item = usize>,
    resolver: &Resolver,
) -> Result<DmlColumnContext> {
    emit_columns_and_dependencies(
        program,
        table,
        cursor_id,
        rowid_reg,
        target_columns,
        |program, dml_ctx| {
            let table = Arc::new(table.clone());
            gencol::compute_virtual_columns_from_schema(
                program,
                &table,
                database_id,
                dml_ctx,
                resolver,
            )
        },
    )
}

/// Emits `target_columns`, plus the stored columns needed by `target_columns`, into a
/// DML row context. This takes into account stored columns, and any stored columns
/// required by virtual columns in `target_columns`.
///
/// Non-rowid target columns are allocated in target order. Rowid-alias columns resolve
/// to `rowid_reg`, so callers that need an unpacked contiguous key or record must
/// materialize one from `DmlColumnContext::to_column_reg`.
fn emit_columns_and_dependencies(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    cursor_id: usize,
    rowid_reg: usize,
    target_columns: impl IntoIterator<Item = usize>,
    compute_virtual_columns: impl FnOnce(&mut ProgramBuilder, &DmlColumnContext) -> Result<()>,
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
    let pairs = table.columns().iter().enumerate().map(|(idx, col)| {
        let reg = if let Some(pos) = non_rowid_target_positions[idx] {
            let reg = target_base + pos;
            if !col.is_virtual_generated() {
                program.emit_column_or_rowid(cursor_id, idx, reg);
            }
            reg
        } else if col.is_rowid_alias() {
            rowid_reg
        } else if dependencies.get(idx) {
            let reg = extra_base + extra_idx;
            program.emit_column_or_rowid(cursor_id, idx, reg);
            extra_idx += 1;
            reg
        } else {
            0
        };
        (col, reg)
    });
    let dml_ctx = DmlColumnContext::from_column_reg_mapping(pairs);
    if targets
        .iter()
        .all(|&idx| !table.columns()[idx].is_rowid_alias())
    {
        debug_assert!(targets
            .windows(2)
            .all(|w| { dml_ctx.to_column_reg(w[1]) == dml_ctx.to_column_reg(w[0]) + 1 }));
    }

    compute_virtual_columns(program, &dml_ctx)?;

    Ok(dml_ctx)
}

/// Emit one index key from the OLD row image. Computed keys were instantiated
/// into PlanExpr when the source was lowered; this path never rebinds a stored
/// schema expression.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_index_column_value_old_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: &TableReferences,
    joined_table: &JoinedTable,
    index: &ResolvedIndex,
    index_column: usize,
    table_cursor_id: usize,
    dest_reg: usize,
) -> Result<()> {
    let index_column_definition = index.value().columns.get(index_column).ok_or_else(|| {
        LimboError::InternalError("planned index column is out of bounds".to_string())
    })?;
    let planned_expression = planned_index_column_expression(joined_table, index, index_column)?;

    if let Some(expr) = planned_expression {
        translate_plan_expr_no_constant_opt(
            program,
            Some(table_references),
            expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
        // For virtual generated column references, apply the column's
        // declared affinity to the computed expression result.
        if index_column_definition.pos_in_table != EXPR_INDEX_SENTINEL {
            if let Some(table) = program.btree_table_from_cursor(table_cursor_id) {
                let column = &table.columns()[index_column_definition.pos_in_table];
                if column.is_virtual_generated() {
                    program.emit_column_affinity(dest_reg, column.affinity());
                }
            }
        }
    } else {
        if index_column_definition.pos_in_table == EXPR_INDEX_SENTINEL {
            return Err(LimboError::InternalError(
                "expression index key has no lowered PlanExpr".to_string(),
            ));
        }
        program.emit_column_or_rowid(
            table_cursor_id,
            index_column_definition.pos_in_table,
            dest_reg,
        );
    }
    Ok(())
}

/// Emit one index key from the NEW row image installed in the Resolver's
/// scoped PlanRuntimeBindings.
pub(crate) fn emit_index_column_value_new_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    joined_table: &JoinedTable,
    index: &ResolvedIndex,
    index_column: usize,
    dest_reg: usize,
) -> Result<()> {
    let index_column_definition = index.value().columns.get(index_column).ok_or_else(|| {
        LimboError::InternalError("planned index column is out of bounds".to_string())
    })?;
    let planned_expression = planned_index_column_expression(joined_table, index, index_column)?;

    if let Some(expr) = planned_expression {
        translate_plan_expr_no_constant_opt(
            program,
            None,
            expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
        if index_column_definition.pos_in_table != EXPR_INDEX_SENTINEL {
            if let Some(column) = joined_table
                .columns()
                .get(index_column_definition.pos_in_table)
                .filter(|column| column.is_virtual_generated())
            {
                program.emit_column_affinity(dest_reg, column.affinity());
            }
        }
    } else {
        if index_column_definition.pos_in_table == EXPR_INDEX_SENTINEL {
            return Err(LimboError::InternalError(
                "expression index key has no lowered PlanExpr".to_string(),
            ));
        }
        let column = joined_table
            .columns()
            .get(index_column_definition.pos_in_table)
            .ok_or_else(|| {
                LimboError::InternalError("index column points outside its table".to_string())
            })?;
        let binding = {
            let bindings = resolver.plan_runtime_bindings();
            if column.is_rowid_alias() {
                bindings.rowid(joined_table.internal_id).cloned()
            } else {
                bindings
                    .value(
                        joined_table.internal_id,
                        index_column_definition.pos_in_table,
                    )
                    .cloned()
            }
        }
        .ok_or_else(|| {
            LimboError::InternalError("NEW row image has no runtime binding".to_string())
        })?;
        match binding {
            RuntimeValueBinding::Register { register, .. } => {
                program.emit_insn(Insn::Copy {
                    src_reg: register,
                    dst_reg: dest_reg,
                    extra_amount: 0,
                });
            }
            RuntimeValueBinding::Parameter(parameter) => {
                translate_plan_expr(
                    program,
                    None,
                    &PlanExpr::Parameter(parameter),
                    dest_reg,
                    resolver,
                )?;
            }
        }
    }
    Ok(())
}

/// Return the semantic program for an index key that must be computed.
///
/// Most computed index keys own a program in `PlanIndexExpressions`. Catalog
/// autoindexes for column-level UNIQUE constraints keep a direct column
/// position instead, so a virtual column reuses its source-local read program.
/// It must never fall through to a physical column read or a row-image copy
/// because virtual columns are not stored in the table record.
fn planned_index_column_expression<'a>(
    joined_table: &'a JoinedTable,
    index: &ResolvedIndex,
    index_column: usize,
) -> Result<Option<&'a PlanExpr>> {
    let index_column_definition = index.value().columns.get(index_column).ok_or_else(|| {
        LimboError::InternalError("planned index column is out of bounds".to_string())
    })?;
    if let Some(expression) = joined_table
        .plan_index_expressions(index.value())
        .and_then(|expressions| expressions.columns.get(index_column))
        .and_then(Option::as_ref)
    {
        return Ok(Some(expression));
    }
    if index_column_definition.pos_in_table == EXPR_INDEX_SENTINEL {
        return Ok(None);
    }

    let column = joined_table
        .columns()
        .get(index_column_definition.pos_in_table)
        .ok_or_else(|| {
            LimboError::InternalError("index column points outside its table".to_string())
        })?;
    if !column.is_virtual_generated() {
        return Ok(None);
    }

    joined_table
        .read_programs
        .generated_expressions
        .get(index_column_definition.pos_in_table)
        .and_then(Option::as_ref)
        .map(Some)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "virtual index column {}.{} has no planned expression",
                joined_table.identifier, index_column_definition.pos_in_table
            ))
        })
}

/// Emit bytecode for already-planned CHECK constraints.
fn emit_check_constraint_bytecode(
    program: &mut ProgramBuilder,
    check_constraints: &[&PlanCheckConstraint],
    resolver: &Resolver,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
) -> Result<()> {
    for check_constraint in check_constraints {
        let expr_result_reg = program.alloc_register();
        translate_plan_expr_no_constant_opt(
            program,
            None,
            &check_constraint.expression,
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
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_CHECK,
                    description: check_constraint.description.clone(),
                    on_error: None,
                    description_reg: None,
                });
            }
        }

        program.preassign_label_to_next_insn(constraint_passed_label);
    }
    Ok(())
}

/// Emit planned CHECK constraints against the exact row image being written.
/// The target's plan identity is bound directly to the DML registers.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_check_constraints<'a>(
    program: &mut ProgramBuilder,
    target: &JoinedTable,
    check_constraints: impl IntoIterator<Item = &'a PlanCheckConstraint>,
    dml_ctx: &DmlColumnContext,
    rowid_reg: usize,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
) -> Result<()> {
    if connection.check_constraints_ignored() {
        return Ok(());
    }
    let check_constraints = check_constraints.into_iter().collect::<Vec<_>>();
    if check_constraints.is_empty() {
        return Ok(());
    }
    let Table::BTree(table) = &target.table else {
        return Err(LimboError::InternalError(format!(
            "CHECK constraint target '{}' is not a B-tree table",
            target.identifier
        )));
    };
    emit_planned_check_constraints(
        program,
        target.internal_id,
        table,
        &check_constraints,
        dml_ctx,
        rowid_reg,
        resolver,
        or_conflict,
        skip_row_label,
    )
}

/// Schema-only adapter used while ALTER TABLE validates rows against catalog
/// expressions that do not belong to a normal DML HIR document.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_check_constraints_from_schema(
    program: &mut ProgramBuilder,
    check_constraints: &[CheckConstraint],
    resolver: &Resolver,
    database_id: usize,
    table: &Arc<BTreeTable>,
    dml_ctx: &DmlColumnContext,
    rowid_reg: usize,
    connection: &Arc<Connection>,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
) -> Result<()> {
    if connection.check_constraints_ignored() || check_constraints.is_empty() {
        return Ok(());
    }
    let column_names = table
        .columns()
        .iter()
        .map(|column| {
            column.name.as_deref().ok_or_else(|| {
                LimboError::InternalError(format!(
                    "table '{}' has an unnamed column in CHECK constraint emission",
                    table.name
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let descriptions = check_constraints
        .iter()
        .map(|check| match &check.name {
            Some(name) => Ok(name.clone()),
            None => check.expr.render(&column_names),
        })
        .collect::<Result<Vec<_>>>()?;
    let expressions = check_constraints
        .iter()
        .map(|check| check.expr.as_valid())
        .collect::<Result<Vec<_>>>()?;

    let context = resolver.semantic_context();
    let analyzed = analyze_schema_exprs(
        &context,
        database_id,
        Arc::new(Table::BTree(Arc::clone(table))),
        &expressions,
    )?;
    let source = program.next_plan_source_id();
    let mut identities = PlanIdentityMap::new();
    identities.bind_source_definition(&analyzed.source, source);
    let planned_checks = analyzed
        .expressions
        .iter()
        .zip(descriptions)
        .map(|(expression, description)| {
            let expression = lower_hir_expr(expression, &identities).map_err(|error| {
                LimboError::InternalError(format!(
                    "failed to lower CHECK constraint expression: {error}"
                ))
            })?;
            Ok(PlanCheckConstraint {
                expression,
                description,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let planned_check_refs = planned_checks.iter().collect::<Vec<_>>();
    emit_planned_check_constraints(
        program,
        source,
        table,
        &planned_check_refs,
        dml_ctx,
        rowid_reg,
        resolver,
        or_conflict,
        skip_row_label,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_planned_check_constraints(
    program: &mut ProgramBuilder,
    source: PlanSourceId,
    table: &BTreeTable,
    check_constraints: &[&PlanCheckConstraint],
    dml_ctx: &DmlColumnContext,
    rowid_reg: usize,
    resolver: &Resolver,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
) -> Result<()> {
    let columns = table
        .columns()
        .iter()
        .enumerate()
        .map(|(column_index, _)| RuntimeValueBinding::Register {
            register: dml_ctx.to_column_reg(column_index),
            needs_decode: false,
        })
        .collect();
    let mut bindings = PlanRuntimeBindings::default();
    bindings.bind_row(
        source,
        RuntimeRowBinding {
            columns,
            rowid: table.has_rowid.then_some(RuntimeValueBinding::Register {
                register: rowid_reg,
                needs_decode: false,
            }),
            read_programs: None,
        },
    );

    resolver.with_plan_runtime_bindings(bindings, |resolver| {
        emit_check_constraint_bytecode(
            program,
            check_constraints,
            resolver,
            or_conflict,
            skip_row_label,
        )
    })
}
