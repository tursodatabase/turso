// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.
use super::{
    collate::{get_expr_collation_ctx, CollationSeq},
    compound_select::emit_program_for_compound_select,
    emitter::{
        delete::emit_program_for_delete, select::emit_program_for_select,
        update::emit_program_for_update,
    },
    expr::{
        bind_and_rewrite_expr, emit_table_column, translate_expr, translate_expr_no_constant_opt,
        walk_expr, BindingBehavior, ExprAffinityInfo, NoConstantOptReason, WalkControl,
    },
    group_by::GroupByMetadata,
    main_loop::{LeftJoinMetadata, LoopLabels, SemiAntiJoinMetadata},
    order_by::SortMetadata,
    plan::{
        BitSet, HashJoinType, JoinedTable, NonFromClauseSubquery, Plan, ResultSetColumn,
        TableReferences,
    },
    planner::{TableMask, ROWID_STRS},
    trigger_exec::{get_triggers_including_temp, has_triggers_including_temp},
    window::WindowMetadata,
};
use crate::instrument;
use crate::schema::{
    BTreeTable, CheckConstraint, Column, ColumnLayout, GeneratedType, IndexColumn, Schema, Table,
};
use crate::translate::plan::ColumnMask;
use crate::vdbe::{
    affinity::Affinity,
    builder::{CursorType, DmlColumnContext, ProgramBuilder, SelfTableContext},
    insn::{to_u16, InsertFlags, Insn},
    BranchOffset, CursorID,
};
use crate::{
    bail_parse_error,
    error::SQLITE_CONSTRAINT_CHECK,
    function::Func,
    sync::Arc,
    turso_assert_ne,
    util::{
        check_expr_references_column, exprs_are_equivalent, normalize_ident, parse_numeric_literal,
    },
    CaptureDataChangesExt, Connection, Database, DatabaseCatalog, LimboError, Result, RwLock,
    SymbolTable,
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
    pub collation: CachedExprCollation,
}

pub type CachedExprCollation = Option<(CollationSeq, bool)>;
pub type CachedExprRegHit = (usize, bool, CachedExprCollation);

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
    /// Maps register indices to column affinities for expression index evaluation.
    /// Populated temporarily during UPDATE new-image expression index key computation,
    /// where column references have been rewritten to Expr::Register and comparison
    /// operators need the original column affinity. Analogous to SQLite's iSelfTab
    /// mechanism, but operates as a side-channel since limbo rewrites the AST rather
    /// than redirecting column reads at codegen time.
    pub register_affinities: HashMap<usize, Affinity>,
    /// Affinity metadata for planned scalar subqueries keyed by their internal ID.
    /// This lets comparison affinity follow SQLite rules for expressions like
    /// `(SELECT text_col FROM ...) > some_numeric_expr`.
    pub(crate) subquery_affinities: RefCell<HashMap<TableInternalId, ExprAffinityInfo>>,
    /// Context and metadata for resolving Expr::Column values that use
    /// [TableInternalId::SELF_TABLE] as a placeholder.
    self_table_scope: RefCell<Option<SelfTableScope>>,
    pub enable_custom_types: bool,
    /// Controls whether unresolved double-quoted identifiers fall back to string
    /// literals (SQLite's DQS misfeature) in DML statements.
    pub dqs_dml: DoubleQuotedDml,
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
}

#[derive(Clone)]
struct SelfTableScope {
    context: SelfTableContext,
    affinities: Option<Arc<[Affinity]>>,
}

impl SelfTableScope {
    fn new(context: SelfTableContext) -> Self {
        let affinities = match &context {
            SelfTableContext::ForDML { table, .. } => Some(
                table
                    .columns()
                    .iter()
                    .map(|c| c.affinity_with_strict(table.is_strict))
                    .collect(),
            ),
            SelfTableContext::ForSelect {
                table_ref_id,
                referenced_tables,
            } => referenced_tables
                .find_table_by_internal_id(*table_ref_id)
                .and_then(|(_, table_ref)| table_ref.btree())
                .map(|btree| {
                    btree
                        .columns()
                        .iter()
                        .map(|c| c.affinity_with_strict(btree.is_strict))
                        .collect()
                }),
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

/// Context for restricting table resolution during trigger subprogram compilation.
#[derive(Debug, Clone)]
pub(crate) struct TriggerDatabaseContext {
    /// The database ID the trigger belongs to.
    database_id: usize,
    /// The trigger name (for error messages).
    trigger_name: String,
}

impl TriggerDatabaseContext {
    fn restricts_db_references(&self) -> bool {
        self.database_id != crate::TEMP_DB_ID
    }
}

impl<'a> Resolver<'a> {
    const MAIN_DB: &'static str = "main";
    const TEMP_DB: &'static str = "temp";

    pub(crate) fn new(
        schema: &'a Schema,
        database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
        temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
        attached_databases: &'a RwLock<DatabaseCatalog>,
        symbol_table: &'a SymbolTable,
        enable_custom_types: bool,
        dqs_dml: DoubleQuotedDml,
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
            register_affinities: HashMap::default(),
            subquery_affinities: RefCell::new(HashMap::default()),
            self_table_scope: RefCell::new(None),
            enable_custom_types,
            dqs_dml,
            trigger_context: None,
            has_temp_schema,
        }
    }

    pub fn schema(&self) -> &Schema {
        self.schema
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
            register_affinities: HashMap::default(),
            subquery_affinities: RefCell::new(self.subquery_affinities.borrow().clone()),
            self_table_scope: RefCell::new(self.self_table_scope.borrow().clone()),
            enable_custom_types: self.enable_custom_types,
            dqs_dml: self.dqs_dml,
            trigger_context: self.trigger_context.clone(),
            has_temp_schema: self.has_temp_schema,
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
            register_affinities: self.register_affinities.clone(),
            subquery_affinities: RefCell::new(self.subquery_affinities.borrow().clone()),
            self_table_scope: RefCell::new(self.self_table_scope.borrow().clone()),
            enable_custom_types: self.enable_custom_types,
            dqs_dml: self.dqs_dml,
            trigger_context: self.trigger_context.clone(),
            has_temp_schema: self.has_temp_schema,
        }
    }

    pub fn require_custom_types(&self, feature: &str) -> crate::Result<()> {
        if !self.enable_custom_types {
            crate::bail_parse_error!("{} require --experimental-custom-types flag", feature);
        }
        Ok(())
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

    pub(crate) fn self_table_column_type_str(&self, column: usize) -> Option<String> {
        self.self_table_scope
            .borrow()
            .as_ref()
            .and_then(|scope| scope.column_type_str(column))
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
                    // with_options only fails if built-in type SQL is malformed (programmer bug).
                    Arc::new(
                        Schema::with_options(self.enable_custom_types)
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
        self.trigger_context = Some(TriggerDatabaseContext {
            database_id,
            trigger_name,
        });
    }

    pub fn resolve_function(
        &self,
        func_name: &str,
        arg_count: usize,
    ) -> Result<Option<Func>, LimboError> {
        match Func::resolve_function(func_name, arg_count)? {
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

    /// Cache a scalar expression result together with the collation metadata that
    /// standalone expression translation would have propagated to a parent comparison.
    pub fn cache_scalar_expr_reg(
        &mut self,
        expr: Cow<'a, ast::Expr>,
        reg: usize,
        needs_decode: bool,
        referenced_tables: &TableReferences,
    ) -> Result<()> {
        let collation = get_expr_collation_ctx(expr.as_ref(), referenced_tables)?;
        self.cache_expr_reg(expr, reg, needs_decode, collation);
        Ok(())
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

    pub(crate) fn attached_database_ids_in_search_order(&self) -> BitSet {
        self.attached_databases
            .read()
            .index_to_data
            .keys()
            .copied()
            .collect()
    }

    fn resolve_unqualified_existing_database_id<F>(
        &self,
        object_name: &str,
        schema_contains_object: F,
    ) -> usize
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
            return crate::TEMP_DB_ID;
        }

        if self.with_schema(crate::MAIN_DB_ID, |schema| {
            schema_contains_object(schema, object_name)
        }) {
            return crate::MAIN_DB_ID;
        }

        for database_id in self.attached_database_ids_in_search_order() {
            if self.with_schema(database_id, |schema| {
                schema_contains_object(schema, object_name)
            }) {
                return database_id;
            }
        }

        crate::MAIN_DB_ID
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

            return Ok(self.resolve_unqualified_existing_database_id(
                table_name,
                Self::schema_has_table_like_object,
            ));
        }

        if let Some(database_id) = Self::resolve_schema_table_database_id(table_name) {
            return Ok(database_id);
        }

        Ok(self.resolve_unqualified_existing_database_id(
            table_name,
            Self::schema_has_table_like_object,
        ))
    }

    pub(crate) fn resolve_existing_index_database_id(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }

        let index_name = normalize_ident(qualified_name.name.as_str());
        Ok(self.resolve_unqualified_existing_database_id(&index_name, Self::schema_has_index))
    }

    pub(crate) fn resolve_existing_trigger_database_id(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }

        let trigger_name = qualified_name.name.as_str();
        Ok(self.resolve_unqualified_existing_database_id(trigger_name, Self::schema_has_trigger))
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
        Plan::CompoundSelect { .. } => {
            emit_program_for_compound_select(program, resolver, plan).map(|_| ())
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
    changed_table_name: &str,
) -> Result<Option<(usize, Arc<BTreeTable>)>> {
    let mode = program.capture_data_changes_info();
    let cdc_table = mode.table();
    let Some(cdc_table) = cdc_table else {
        return Ok(None);
    };
    if changed_table_name == cdc_table
        || changed_table_name == crate::translate::pragma::TURSO_CDC_VERSION_TABLE_NAME
    {
        return Ok(None);
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
            start_reg: to_u16(columns_reg),
            count: to_u16(storable_count),
            dest_reg: to_u16(record_reg),
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
        start_reg: to_u16(start_reg),
        count: to_u16(storable_count),
        dest_reg: to_u16(dest_reg),
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
        start_reg: to_u16(columns_reg + 1),
        count: to_u16(storable_count),
        dest_reg: to_u16(columns_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    columns_reg
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
            resolver,
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
    resolver: &Resolver,
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

    let Some(unixepoch_fn) = resolver.resolve_function("unixepoch", 0)? else {
        bail_parse_error!("no function {}", "unixepoch");
    };
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: unixepoch_fn,
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
        start_reg: to_u16(turso_cdc_registers),
        count: to_u16(8),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
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
    let Some(unixepoch_fn) = resolver.resolve_function("unixepoch", 0)? else {
        bail_parse_error!("no function {}", "unixepoch");
    };
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: unixepoch_fn,
        arg_count: 0,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    // change_txn_id = conn_txn_id(new_rowid)
    // First generate a candidate rowid, then pass it to conn_txn_id for get-or-set.
    let candidate_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg: candidate_reg,
        prev_largest_reg: 0,
    });
    let Some(conn_txn_id_fn) = resolver.resolve_function("conn_txn_id", 1)? else {
        bail_parse_error!("no function {}", "conn_txn_id");
    };
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: conn_txn_id_fn,
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

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(turso_cdc_registers),
        count: to_u16(9),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
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
    let Some(unixepoch_fn) = resolver.resolve_function("unixepoch", 0)? else {
        bail_parse_error!("no function {}", "unixepoch");
    };
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: unixepoch_fn,
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
    let Some(conn_txn_id_fn) = resolver.resolve_function("conn_txn_id", 1)? else {
        bail_parse_error!("no function {}", "conn_txn_id");
    };
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: conn_txn_id_fn,
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

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(regs),
        count: to_u16(9),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
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
        let Some(is_autocommit_fn) = resolver.resolve_function("is_autocommit", 0)? else {
            bail_parse_error!("no function {}", "is_autocommit");
        };
        let is_autocommit_fn_ctx = crate::function::FuncCtx {
            func: is_autocommit_fn,
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
/// Initialize the limit/offset counters and registers.
/// In case of compound SELECTs, the limit counter is initialized only once,
/// hence [LimitCtx::initialize_counter] being false in those cases.
pub(crate) fn init_limit(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    limit: &Option<Box<Expr>>,
    offset: &Option<Box<Expr>>,
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
                        });
                    }
                    _ => unreachable!("parse_numeric_literal only returns Integer or Float"),
                },
                _ => {
                    let r = limit_ctx.reg_limit;

                    _ = translate_expr(program, None, expr, r, &t_ctx.resolver)?;
                    program.emit_insn(Insn::MustBeInt { reg: r });
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
                        program.emit_insn(Insn::MustBeInt { reg: offset_reg });
                    }
                    _ => unreachable!("parse_numeric_literal only returns Integer or Float"),
                },
                _ => {
                    _ = translate_expr(program, None, expr, offset_reg, &t_ctx.resolver)?;
                }
            }
            program.add_comment(program.offset(), "OFFSET counter");
            program.emit_insn(Insn::MustBeInt { reg: offset_reg });

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

/// Emits  `target_columns`, plus the stored columns needed by `target_columns`, into compact
/// registers. This takes into account stored columns, and any stored columns required
/// by virtual columns in `target_columns`.
///
/// Target columns are guaranteed to be in a contiguous block, in the given order, at the start of
/// registers. The following postcondition holds:
///
/// ```text
/// dml_ctx.to_column_reg(target_columns[i]) == dml_ctx.to_column_reg(target_columns[0]) + i
/// ```
///
/// This way, target_columns[0] can be used as a base for opcodes that require unpacked records.
pub(crate) fn emit_columns_and_dependencies(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    cursor_id: usize,
    rowid_reg: usize,
    target_columns: impl IntoIterator<Item = usize>,
    resolver: &Resolver,
) -> Result<DmlColumnContext> {
    let targets: Vec<usize> = target_columns.into_iter().collect();
    let dependencies = table.dependencies_of_columns(targets.iter().copied())?;

    let target_base = program.alloc_registers(targets.len());
    let extra_base = {
        let mut dependencies_not_in_targets: ColumnMask = dependencies.clone();
        let target_mask = targets.iter().copied().collect();
        dependencies_not_in_targets -= &target_mask;

        let extra_count = dependencies_not_in_targets.count();

        if extra_count > 0 {
            program.alloc_registers(extra_count)
        } else {
            0
        }
    };

    let mut extra_idx = 0;
    let pairs = table.columns().iter().enumerate().map(|(idx, col)| {
        let reg = if col.is_rowid_alias() {
            rowid_reg
        } else if let Some(pos) = targets.iter().position(|&t| t == idx) {
            let reg = target_base + pos;
            if !col.is_virtual_generated() {
                program.emit_column_or_rowid(cursor_id, idx, reg);
            }
            reg
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
    debug_assert!(targets
        .windows(2)
        .all(|w| { dml_ctx.to_column_reg(w[1]) == dml_ctx.to_column_reg(w[0]) + 1 }));

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
        program.emit_column_or_rowid(table_cursor_id, idx_col.pos_in_table, dest_reg);
    }
    Ok(())
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
fn emit_check_constraint_bytecode(
    program: &mut ProgramBuilder,
    check_constraints: &[CheckConstraint],
    resolver: &mut Resolver,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
    referenced_tables: Option<&TableReferences>,
    table_name: &str,
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

        let constraint_name = match &check_constraint.name {
            Some(name) => name.clone(),
            None => format!("{}", check_constraint.expr),
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
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_CHECK,
                    description: constraint_name.to_string(),
                    on_error: None,
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
        resolver.cache_expr_reg(Cow::Owned(rowid_expr), rowid_reg, false, None);
        let qualified_expr = ast::Expr::Qualified(
            ast::Name::exact(table_name.to_string()),
            ast::Name::exact(rowid_name.to_string()),
        );
        resolver.cache_expr_reg(Cow::Owned(qualified_expr), rowid_reg, false, None);
    }

    // Map each column to its register (both unqualified and qualified forms).
    for (col_name, register) in column_mappings.iter().copied() {
        let collation = joined_table
            .and_then(|table| {
                table.columns().iter().find(|col| {
                    col.name
                        .as_ref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(col_name))
                })
            })
            .map(|col| (col.collation(), false));
        let column_expr = ast::Expr::Id(ast::Name::exact(col_name.to_string()));
        resolver.cache_expr_reg(Cow::Owned(column_expr), register, false, collation);
        let qualified_expr = ast::Expr::Qualified(
            ast::Name::exact(table_name.to_string()),
            ast::Name::exact(col_name.to_string()),
        );
        resolver.cache_expr_reg(Cow::Owned(qualified_expr), register, false, collation);
    }

    if let Some(joined_table) = joined_table {
        resolver.cache_expr_reg(
            Cow::Owned(ast::Expr::RowId {
                database: None,
                table: joined_table.internal_id,
            }),
            rowid_reg,
            false,
            None,
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
                    Some((col.collation(), false)),
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
    );

    // Always restore resolver state, even on error.
    resolver.expr_to_reg_cache.truncate(initial_cache_size);
    resolver.expr_to_reg_cache_enabled = false;

    result
}
