// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.
use super::{
    collate::{CollationSeq, get_expr_collation_ctx_with_symbols},
    expr::ExprAffinityInfo,
    plan::{BitSet, ColumnMask, JoinedTable, TableReferences},
};
use crate::alloc::{TryClone, TursoIteratorExt};
use crate::schema::{BTreeTable, Column, ColumnLayout, Schema, Table};
use crate::translate::fkeys::FkActionCompileStack;
use crate::vdbe::{
    affinity::Affinity,
    builder::{CursorType, DmlColumnContext, ProgramBuilder, SelfTableContext},
    insn::{InsertFlags, Insn, to_u32},
};
use crate::{
    CaptureDataChangesExt, Database, DatabaseCatalog, LimboError, Result, RwLock, SymbolTable,
    function::Func,
    sync::Arc,
    turso_assert_ne,
    util::{exprs_are_equivalent, normalize_ident},
};
use rustc_hash::FxHashMap as HashMap;
use std::borrow::Cow;
use std::cell::RefCell;
use turso_parser::ast::{self, TableInternalId};

pub(crate) mod gencol;

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
    /// Maps register indices to declared column collations, the collation
    /// counterpart of `register_affinities`: when column references are
    /// rewritten to Expr::Register (UPSERT DO UPDATE WHERE/SET), comparisons
    /// must still use the column's implicit collation per SQLite's rule 2.
    pub register_collations: HashMap<usize, CollationSeq>,
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
            register_affinities: HashMap::default(),
            register_collations: HashMap::default(),
            subquery_affinities: RefCell::new(HashMap::default()),
            self_table_scope: RefCell::new(None),
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
            register_collations: HashMap::default(),
            subquery_affinities: RefCell::new(self.subquery_affinities.borrow().clone()),
            self_table_scope: RefCell::new(self.self_table_scope.borrow().clone()),
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
            register_affinities: self.register_affinities.clone(),
            register_collations: self.register_collations.clone(),
            subquery_affinities: RefCell::new(self.subquery_affinities.borrow().clone()),
            self_table_scope: RefCell::new(self.self_table_scope.borrow().clone()),
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
                        Schema::with_options(self.enable_custom_types, self.dialect.as_ref())
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

    /// Cache a scalar expression result together with the collation metadata that
    /// standalone expression translation would have propagated to a parent comparison.
    pub fn cache_scalar_expr_reg(
        &mut self,
        expr: Cow<'a, ast::Expr>,
        reg: usize,
        needs_decode: bool,
        referenced_tables: &TableReferences,
    ) -> Result<()> {
        let collation = get_expr_collation_ctx_with_symbols(
            expr.as_ref(),
            referenced_tables,
            Some(self.symbol_table),
        )?;
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
        debug_assert!(
            targets
                .windows(2)
                .all(|w| { dml_ctx.to_column_reg(w[1]) == dml_ctx.to_column_reg(w[0]) + 1 })
        );
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
