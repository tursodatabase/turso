//! Read-only inputs to SQL semantic analysis.
//!
//! A context snapshots the catalog handles that are visible while one SQL
//! root is analyzed.  It deliberately contains no bytecode builder, register
//! cache, cursor, label, or other execution state.

use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast;

use crate::{
    connection::TempDatabase,
    dialect::Dialect,
    function::Func,
    schema::{Schema, Table},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    translate::collate::CollationSeq,
    DatabaseCatalog, LimboError, Result, SymbolTable, MAIN_DB_ID, TEMP_DB_ID,
};

use super::hir::CatalogSnapshot;

static NEXT_CATALOG_SNAPSHOT: AtomicU64 = AtomicU64::new(1);

/// Whether SQLite's double-quoted-string fallback is enabled for DML.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoubleQuotedDml {
    Enabled,
    Disabled,
}

impl DoubleQuotedDml {
    pub fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

impl From<bool> for DoubleQuotedDml {
    fn from(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

#[derive(Clone)]
enum SchemaSnapshot<'a> {
    Borrowed(&'a Schema),
    Shared(Arc<Schema>),
}

impl SchemaSnapshot<'_> {
    fn schema(&self) -> &Schema {
        match self {
            Self::Borrowed(schema) => schema,
            Self::Shared(schema) => schema,
        }
    }
}

/// Database restriction in force while a trigger body is analyzed.
#[derive(Clone, Debug)]
pub(crate) struct TriggerCatalogContext {
    pub(crate) database_id: usize,
    pub(crate) trigger_name: String,
}

/// Statement-policy facts that affect SQLite DML validation.
///
/// These are captured before semantic analysis so DML rules do not need a
/// connection or bytecode builder.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DmlPolicy {
    nested_statement: bool,
    mvcc_bootstrap_connection: bool,
    internal_schema_change: bool,
    check_constraints_ignored: bool,
}

impl DmlPolicy {
    pub(crate) const fn new(
        nested_statement: bool,
        mvcc_bootstrap_connection: bool,
        internal_schema_change: bool,
        check_constraints_ignored: bool,
    ) -> Self {
        Self {
            nested_statement,
            mvcc_bootstrap_connection,
            internal_schema_change,
            check_constraints_ignored,
        }
    }

    pub(crate) const fn nested_statement(self) -> bool {
        self.nested_statement
    }

    pub(crate) const fn mvcc_bootstrap_connection(self) -> bool {
        self.mvcc_bootstrap_connection
    }

    pub(crate) const fn internal_schema_change(self) -> bool {
        self.internal_schema_change
    }

    pub(crate) const fn check_constraints_ignored(self) -> bool {
        self.check_constraints_ignored
    }
}

impl TriggerCatalogContext {
    pub(crate) fn new(database_id: usize, trigger_name: String) -> Self {
        Self {
            database_id,
            trigger_name,
        }
    }

    pub(crate) fn database_id(&self) -> usize {
        self.database_id
    }

    pub(crate) fn restricts_db_references(&self) -> bool {
        super::trigger_rules::restricts_database_references(self.database_id)
    }
}

/// Immutable catalog and settings view used by one semantic-analysis run.
///
/// The `Schema` values are fixed when this value is created.  Resolved HIR
/// objects therefore cannot accidentally combine facts read before and after
/// a schema change.  The numeric snapshot token is copied into the HIR and is
/// also supplied to physical planning when it reads index or statistics data.
#[derive(Clone)]
pub(crate) struct SemanticContext<'a> {
    snapshot_serial: u64,
    schemas: HashMap<usize, SchemaSnapshot<'a>>,
    database_names: HashMap<String, usize>,
    attached_search_order: Vec<usize>,
    symbol_table: &'a SymbolTable,
    dialect: Arc<dyn Dialect>,
    custom_types_enabled: bool,
    dqs_dml: DoubleQuotedDml,
    trigger: Option<TriggerCatalogContext>,
    dml_policy: DmlPolicy,
}

impl<'a> SemanticContext<'a> {
    /// Build the semantic snapshot used for a stored main-schema object.
    ///
    /// Materialized views are currently restricted to the main database, so
    /// schema reload must not add temp or attached catalogs to their lookup
    /// domain. It still uses the same analyzer, function table, custom-type
    /// setting, and dialect as ordinary statement preparation.
    pub(crate) fn for_main_schema_object(
        main_schema: &'a Schema,
        symbol_table: &'a SymbolTable,
        custom_types_enabled: bool,
        dialect: Arc<dyn Dialect>,
    ) -> Self {
        let mut schemas = HashMap::default();
        schemas.insert(MAIN_DB_ID, SchemaSnapshot::Borrowed(main_schema));

        let mut database_names = HashMap::default();
        database_names.insert("main".to_string(), MAIN_DB_ID);

        Self {
            snapshot_serial: NEXT_CATALOG_SNAPSHOT.fetch_add(1, Ordering::Relaxed),
            schemas,
            database_names,
            attached_search_order: Vec::new(),
            symbol_table,
            dialect,
            custom_types_enabled,
            // Existing stored definitions predate per-connection DQS
            // provenance. Use SQLite's default when rebuilding them.
            dqs_dml: DoubleQuotedDml::Enabled,
            trigger: None,
            dml_policy: DmlPolicy::default(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        main_schema: &'a Schema,
        database_schemas: &RwLock<HashMap<usize, Arc<Schema>>>,
        temp_database: &RwLock<Option<TempDatabase>>,
        attached_databases: &RwLock<DatabaseCatalog>,
        symbol_table: &'a SymbolTable,
        custom_types_enabled: bool,
        dqs_dml: DoubleQuotedDml,
        dialect: Arc<dyn Dialect>,
    ) -> Self {
        let mut schemas = HashMap::default();
        schemas.insert(MAIN_DB_ID, SchemaSnapshot::Borrowed(main_schema));

        let mut database_names = HashMap::default();
        database_names.insert("main".to_string(), MAIN_DB_ID);

        let temp_schema = temp_database
            .read()
            .as_ref()
            .map(|temp| temp.db.schema.lock().clone())
            .unwrap_or_else(|| {
                Arc::new(
                    Schema::with_options_and_symbols(
                        custom_types_enabled,
                        dialect.as_ref(),
                        symbol_table,
                    )
                    .expect("built-in type definitions are malformed"),
                )
            });
        schemas.insert(TEMP_DB_ID, SchemaSnapshot::Shared(temp_schema));
        database_names.insert("temp".to_string(), TEMP_DB_ID);

        let staged_schemas = database_schemas.read();
        let attached = attached_databases.read();
        let mut attached_search_order: Vec<_> = attached.index_to_data.keys().copied().collect();
        attached_search_order.sort_unstable();
        for database_id in &attached_search_order {
            let (database, _) = attached
                .index_to_data
                .get(database_id)
                .expect("attached database id disappeared while catalog lock was held");
            let schema = staged_schemas
                .get(database_id)
                .cloned()
                .unwrap_or_else(|| database.schema.lock().clone());
            schemas.insert(*database_id, SchemaSnapshot::Shared(schema));
            if let Some(name) = attached.get_name_by_index(*database_id) {
                database_names.insert(name, *database_id);
            }
        }

        Self {
            snapshot_serial: NEXT_CATALOG_SNAPSHOT.fetch_add(1, Ordering::Relaxed),
            schemas,
            database_names,
            attached_search_order,
            symbol_table,
            dialect,
            custom_types_enabled,
            dqs_dml,
            trigger: None,
            dml_policy: DmlPolicy::default(),
        }
    }

    pub(crate) fn snapshot_serial(&self) -> u64 {
        self.snapshot_serial
    }

    pub(crate) fn snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot::from_id(self.snapshot_serial)
    }

    pub(crate) fn main_schema(&self) -> &Schema {
        self.schema(MAIN_DB_ID)
            .expect("a semantic context always contains the main schema")
    }

    pub(crate) fn schema(&self, database_id: usize) -> Option<&Schema> {
        self.schemas.get(&database_id).map(SchemaSnapshot::schema)
    }

    pub(crate) fn with_schema<T>(
        &self,
        database_id: usize,
        read: impl FnOnce(&Schema) -> T,
    ) -> Option<T> {
        self.schema(database_id).map(read)
    }

    pub(crate) fn has_temp_database(&self) -> bool {
        self.schemas.contains_key(&TEMP_DB_ID)
    }

    pub(crate) fn custom_types_enabled(&self) -> bool {
        self.custom_types_enabled
    }

    pub(crate) fn require_custom_types(&self, feature: &str) -> Result<()> {
        if !self.custom_types_enabled {
            crate::bail_parse_error!("{} require --experimental-custom-types flag", feature);
        }
        Ok(())
    }

    pub(crate) fn dqs_dml(&self) -> DoubleQuotedDml {
        self.dqs_dml
    }

    pub(crate) fn dialect(&self) -> &Arc<dyn Dialect> {
        &self.dialect
    }

    pub(crate) fn symbol_table(&self) -> &SymbolTable {
        self.symbol_table
    }

    pub(crate) fn trigger(&self) -> Option<&TriggerCatalogContext> {
        self.trigger.as_ref()
    }

    pub(crate) fn dml_policy(&self) -> DmlPolicy {
        self.dml_policy
    }

    pub(crate) fn with_dml_policy(&self, policy: DmlPolicy) -> SemanticContext<'a> {
        let mut context = self.clone();
        context.dml_policy = policy;
        context
    }

    pub(crate) fn for_trigger(
        &self,
        database_id: usize,
        trigger_name: String,
    ) -> SemanticContext<'a> {
        let mut context = self.clone();
        context.trigger = Some(TriggerCatalogContext::new(database_id, trigger_name));
        context
    }

    pub(crate) fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        match self.dialect.resolve_function(name, arg_count)? {
            Some(function) => Ok(Some(function)),
            None => Ok(self
                .symbol_table
                .resolve_function(name, arg_count)
                .map(Func::External)),
        }
    }

    pub(crate) fn resolve_collation(&self, name: &str) -> Result<CollationSeq> {
        if let Some(collation) = self.symbol_table.resolve_collation(name) {
            return Ok(collation);
        }
        CollationSeq::new(name)
    }

    pub(crate) fn resolve_database_id(&self, name: &ast::QualifiedName) -> Result<usize> {
        let resolved = if let Some(database_name) = &name.db_name {
            let normalized = crate::util::normalize_ident(database_name.as_str());
            self.database_names
                .get(&normalized)
                .copied()
                .ok_or_else(|| {
                    LimboError::InvalidArgument(format!("no such database: {normalized}"))
                })?
        } else if let Some(trigger) = &self.trigger {
            super::trigger_rules::default_database(trigger.database_id)
        } else {
            MAIN_DB_ID
        };

        if let Some(trigger) = &self.trigger {
            if !super::trigger_rules::database_reference_allowed(trigger.database_id, resolved) {
                let database_name = name
                    .db_name
                    .as_ref()
                    .map(|name| name.as_str())
                    .unwrap_or("main");
                return Err(LimboError::ParseError(format!(
                    "trigger {} cannot reference objects in database {}",
                    trigger.trigger_name, database_name
                )));
            }
        }
        Ok(resolved)
    }

    pub(crate) fn database_name(&self, database_id: usize) -> Option<&str> {
        self.database_names
            .iter()
            .find_map(|(name, id)| (*id == database_id).then_some(name.as_str()))
    }

    pub(crate) fn resolve_existing_table_database_id(
        &self,
        name: &ast::QualifiedName,
    ) -> Result<usize> {
        if name.db_name.is_some() {
            return self.resolve_database_id(name);
        }

        if let Some(trigger) = &self.trigger {
            if trigger.restricts_db_references() {
                return Ok(trigger.database_id);
            }
        }

        let table_name = name.name.as_str();
        if table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME_ALT)
        {
            return Ok(TEMP_DB_ID);
        }
        if table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME_ALT)
        {
            return Ok(MAIN_DB_ID);
        }

        if self.schema_contains_table(TEMP_DB_ID, table_name) {
            return Ok(TEMP_DB_ID);
        }
        if self.schema_contains_table(MAIN_DB_ID, table_name) {
            return Ok(MAIN_DB_ID);
        }
        for database_id in &self.attached_search_order {
            if self.schema_contains_table(*database_id, table_name) {
                return Ok(*database_id);
            }
        }

        // Preserve SQLite's error surface: the caller reports the missing
        // object against main rather than treating the database as missing.
        Ok(MAIN_DB_ID)
    }

    pub(crate) fn resolve_table(
        &self,
        name: &ast::QualifiedName,
    ) -> Result<(usize, Option<Arc<Table>>)> {
        let database_id = self.resolve_existing_table_database_id(name)?;
        let table = self
            .schema(database_id)
            .and_then(|schema| schema.get_table(name.name.as_str()));
        Ok((database_id, table))
    }

    fn schema_contains_table(&self, database_id: usize, name: &str) -> bool {
        let normalized = crate::util::normalize_ident(name);
        self.schema(database_id).is_some_and(|schema| {
            schema.get_table(name).is_some()
                || schema.get_view(name).is_some()
                || schema.get_materialized_view(name).is_some()
                || schema.incompatible_views.contains(&normalized)
                || schema.broken_views.contains(&normalized)
        })
    }
}
