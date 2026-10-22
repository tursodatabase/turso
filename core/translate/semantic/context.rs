use std::sync::atomic::{AtomicU64, Ordering};

use rustc_hash::FxHashMap as HashMap;

use crate::{
    alloc::TursoIteratorExt,
    dialect::Dialect,
    function::Func,
    schema::{Index, Schema, Table},
    sync::Arc,
    translate::{collate::CollationSeq, plan::BitSet},
    util::normalize_ident,
    LimboError, Result, SymbolTable, MAIN_DB_ID, TEMP_DB_ID,
};

use super::catalog::SemanticCatalog;
use super::hir::{CatalogSnapshot, DatabaseId, DatabaseSnapshot};

static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

/// Whether unresolved double-quoted DML names become string literals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DoubleQuotedDml {
    Enabled,
    Disabled,
}

pub(crate) struct SemanticDatabase<'catalog> {
    id: DatabaseId,
    name: String,
    schema: &'catalog Schema,
}

impl<'catalog> SemanticDatabase<'catalog> {
    pub(crate) fn new(id: DatabaseId, name: impl Into<String>, schema: &'catalog Schema) -> Self {
        Self {
            id,
            name: name.into(),
            schema,
        }
    }
}

impl DoubleQuotedDml {
    pub(crate) const fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Catalog inputs available while converting parser AST into resolved HIR.
pub(crate) struct SemanticContext<'catalog> {
    /// The captured catalog this context reads, when it was built from one.
    catalog: Option<&'catalog SemanticCatalog>,
    main_schema: &'catalog Schema,
    database_schemas: HashMap<DatabaseId, &'catalog Schema>,
    symbols: &'catalog SymbolTable,
    custom_types_enabled: bool,
    dialect: Arc<dyn Dialect>,
    database_names: HashMap<String, DatabaseId>,
    unqualified_database_search_path: Vec<DatabaseId>,
    /// See [super::catalog::SemanticCatalog::has_temp_database].
    has_temp_database: bool,
    /// See [super::catalog::SemanticCatalog::search_path_configured].
    search_path_configured: bool,
    dqs_dml: DoubleQuotedDml,
    /// `PRAGMA foreign_keys`. When off, DML carries no foreign-key metadata,
    /// so a child table whose parent is missing can still be written.
    foreign_keys_enabled: bool,
    /// The table change data capture writes to, when it is on.
    capture_changes_table: Option<String>,
    /// See [super::SemanticOptions::long_column_names].
    long_column_names: bool,
    snapshot: CatalogSnapshot,
}

impl<'catalog> SemanticContext<'catalog> {
    pub(crate) fn for_catalog(
        catalog: &'catalog super::catalog::SemanticCatalog,
        symbols: &'catalog SymbolTable,
        custom_types_enabled: bool,
        dialect: Arc<dyn Dialect>,
        dqs_dml: DoubleQuotedDml,
        foreign_keys_enabled: bool,
    ) -> Result<Self> {
        Self::for_databases(
            catalog.databases.iter().map(|database| {
                SemanticDatabase::new(database.id, database.name.clone(), &database.schema)
            }),
            catalog.unqualified_database_search_path.clone(),
            symbols,
            custom_types_enabled,
            dialect,
        )
        .map(|context| {
            context
                .with_catalog(catalog)
                .with_dqs_dml(dqs_dml)
                .with_foreign_keys(foreign_keys_enabled)
                .with_temp_database(catalog.has_temp_database)
                .with_search_path_configured(catalog.search_path_configured)
        })
    }

    /// A context with this one's settings over `catalog` instead.
    pub(crate) fn for_other_catalog<'other>(
        &self,
        catalog: &'other SemanticCatalog,
    ) -> Result<SemanticContext<'other>>
    where
        'catalog: 'other,
    {
        SemanticContext::for_catalog(
            catalog,
            self.symbols,
            self.custom_types_enabled,
            self.dialect.clone(),
            self.dqs_dml,
            self.foreign_keys_enabled,
        )
        .map(|context| {
            context
                .with_capture_changes(self.capture_changes_table.clone())
                .with_long_column_names(self.long_column_names)
        })
    }

    /// The captured catalog with the main database's schema replaced by
    /// `schema`, for a statement that must read the latest shared schema.
    pub(crate) fn catalog_with_main_schema(&self, schema: Arc<Schema>) -> Result<SemanticCatalog> {
        self.catalog
            .map(|catalog| catalog.with_main_schema(schema))
            .ok_or_else(|| {
                LimboError::InternalError("semantic context has no captured catalog".to_string())
            })
    }

    fn with_catalog(mut self, catalog: &'catalog SemanticCatalog) -> Self {
        self.catalog = Some(catalog);
        self
    }

    #[cfg(test)]
    pub(crate) fn for_main_schema_object(
        main_schema: &'catalog Schema,
        symbols: &'catalog SymbolTable,
        custom_types_enabled: bool,
        dialect: Arc<dyn Dialect>,
    ) -> Self {
        Self::for_databases(
            [SemanticDatabase::new(
                DatabaseId::new(MAIN_DB_ID),
                "main",
                main_schema,
            )],
            vec![DatabaseId::new(MAIN_DB_ID)],
            symbols,
            custom_types_enabled,
            dialect,
        )
        .expect("the main-only semantic catalog is valid")
    }

    pub(crate) fn for_databases(
        databases: impl IntoIterator<Item = SemanticDatabase<'catalog>>,
        unqualified_database_search_path: Vec<DatabaseId>,
        symbols: &'catalog SymbolTable,
        custom_types_enabled: bool,
        dialect: Arc<dyn Dialect>,
    ) -> Result<Self> {
        let snapshot = NEXT_SNAPSHOT_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("catalog snapshot identity space exhausted");
        let mut database_names = HashMap::default();
        let mut database_schemas = HashMap::default();
        for database in databases {
            let name = normalize_ident(&database.name);
            if database_names.insert(name.clone(), database.id).is_some() {
                return Err(LimboError::InvalidArgument(format!(
                    "duplicate database name: {name}"
                )));
            }
            if database_schemas
                .insert(database.id, database.schema)
                .is_some()
            {
                return Err(LimboError::InvalidArgument(format!(
                    "duplicate database id: {}",
                    database.id.index()
                )));
            }
        }
        let main_schema = database_schemas
            .get(&DatabaseId::new(MAIN_DB_ID))
            .copied()
            .ok_or_else(|| LimboError::InvalidArgument("missing main database".to_string()))?;
        for database in &unqualified_database_search_path {
            if !database_schemas.contains_key(database) {
                return Err(LimboError::InvalidArgument(format!(
                    "search path contains unknown database id: {}",
                    database.index()
                )));
            }
        }
        Ok(Self {
            catalog: None,
            main_schema,
            database_schemas,
            symbols,
            custom_types_enabled,
            dialect,
            database_names,
            unqualified_database_search_path,
            has_temp_database: false,
            search_path_configured: false,
            dqs_dml: DoubleQuotedDml::Enabled,
            foreign_keys_enabled: true,
            capture_changes_table: None,
            long_column_names: false,
            snapshot: CatalogSnapshot::from_id(snapshot),
        })
    }

    pub(crate) fn with_temp_database(mut self, has_temp_database: bool) -> Self {
        self.has_temp_database = has_temp_database;
        self
    }

    pub(crate) fn with_search_path_configured(mut self, configured: bool) -> Self {
        self.search_path_configured = configured;
        self
    }

    /// The main database's schema.
    pub(crate) const fn main_schema(&self) -> &'catalog Schema {
        self.main_schema
    }

    /// Whether the connection has a TEMP database with objects of its own.
    pub(crate) const fn has_temp_database(&self) -> bool {
        self.has_temp_database
    }

    /// Run `f` on the schema of the database numbered `database_id`, which
    /// an earlier lookup resolved against this catalog.
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        let schema = self
            .database_schemas
            .get(&DatabaseId::new(database_id))
            .copied()
            .unwrap_or_else(|| panic!("database {database_id} is not in the catalog"));
        f(schema)
    }

    /// The numbers of the attached databases, lowest first.
    pub(crate) fn attached_database_ids_in_search_order(&self) -> Result<BitSet> {
        let mut ids: Vec<usize> = self
            .database_schemas
            .keys()
            .map(|database| database.index())
            .filter(|&database| database != MAIN_DB_ID && database != TEMP_DB_ID)
            .collect();
        ids.sort_unstable();
        Ok(ids.into_iter().try_collect()?)
    }

    pub(crate) fn database_name_by_index(&self, index: usize) -> Option<String> {
        self.database_name(DatabaseId::new(index))
            .map(str::to_string)
    }

    /// The number of the attached database called `name`, if there is one.
    /// Main and temp are not attached databases.
    pub(crate) fn attached_database_id(&self, name: &str) -> Option<usize> {
        self.database_names
            .get(&normalize_ident(name))
            .map(|database| database.index())
            .filter(|&database| database != MAIN_DB_ID && database != TEMP_DB_ID)
    }

    /// The database a schema statement names: the qualifier when there is
    /// one, else main.
    pub(crate) fn resolve_database_id(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<usize> {
        let Some(db_name) = &qualified_name.db_name else {
            return Ok(MAIN_DB_ID);
        };
        let db_name = normalize_ident(db_name.as_str());
        match db_name.as_str() {
            "main" => Ok(MAIN_DB_ID),
            "temp" => Ok(TEMP_DB_ID),
            _ => self
                .attached_database_id(&db_name)
                .ok_or_else(|| LimboError::InvalidArgument(format!("no such database: {db_name}"))),
        }
    }

    /// The database holding the table, view or materialized view
    /// `qualified_name` names.
    pub(crate) fn resolve_existing_table_database_id_qualified(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }
        self.resolve_existing_table_database_id(qualified_name.name.as_str())
    }

    /// The database holding the table, view or materialized view named
    /// `table_name`, looked up along the unqualified search path.
    pub(crate) fn resolve_existing_table_database_id(&self, table_name: &str) -> Result<usize> {
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
        self.resolve_unqualified_existing_database_id(table_name, |schema, name| {
            schema.get_table(name).is_some()
                || schema.get_view(name).is_some()
                || schema.get_materialized_view(name).is_some()
        })
    }

    pub(crate) fn resolve_existing_index_database_id(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }
        let index_name = normalize_ident(qualified_name.name.as_str());
        self.resolve_unqualified_existing_database_id(&index_name, |schema, name| {
            schema
                .indexes
                .values()
                .flat_map(|indexes| indexes.iter())
                .any(|index| index.name.eq_ignore_ascii_case(name))
        })
    }

    pub(crate) fn resolve_existing_trigger_database_id(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }
        self.resolve_unqualified_existing_database_id(
            qualified_name.name.as_str(),
            |schema, name| schema.get_trigger(name).is_some(),
        )
    }

    /// The first database of the unqualified search path holding an object
    /// `schema_has` finds. Without a configured search path an object that
    /// is nowhere is taken to be in main, so the caller reports it missing
    /// there; with one it is an error.
    fn resolve_unqualified_existing_database_id(
        &self,
        object_name: &str,
        schema_has: impl Fn(&Schema, &str) -> bool,
    ) -> Result<usize> {
        for database in &self.unqualified_database_search_path {
            if schema_has(self.schema(*database)?, object_name) {
                return Ok(database.index());
            }
        }
        if self.search_path_configured {
            return Err(LimboError::ParseError(format!(
                "no such object: {object_name}"
            )));
        }
        Ok(MAIN_DB_ID)
    }

    pub(crate) fn resolve_collation(&self, name: &str) -> Result<CollationSeq> {
        if let Some(collation) = self.symbols.resolve_collation(name) {
            return Ok(collation);
        }
        CollationSeq::new(name)
    }

    pub(crate) fn with_dqs_dml(mut self, dqs_dml: DoubleQuotedDml) -> Self {
        self.dqs_dml = dqs_dml;
        self
    }

    pub(crate) fn with_capture_changes(mut self, table: Option<String>) -> Self {
        self.capture_changes_table = table;
        self
    }

    pub(crate) fn capture_changes_table(&self) -> Option<&str> {
        self.capture_changes_table.as_deref()
    }

    pub(crate) fn with_foreign_keys(mut self, enabled: bool) -> Self {
        self.foreign_keys_enabled = enabled;
        self
    }

    pub(crate) fn with_long_column_names(mut self, enabled: bool) -> Self {
        self.long_column_names = enabled;
        self
    }

    pub(crate) const fn long_column_names(&self) -> bool {
        self.long_column_names
    }

    pub(crate) const fn symbols(&self) -> &SymbolTable {
        self.symbols
    }

    pub(crate) const fn custom_types_enabled(&self) -> bool {
        self.custom_types_enabled
    }

    pub(crate) fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        match self.dialect.resolve_function(name, arg_count)? {
            Some(function) => Ok(Some(function)),
            None => Ok(self
                .symbols
                .resolve_function(name, arg_count)
                .map(Func::External)),
        }
    }

    /// The name a database is attached under, such as "main".
    pub(crate) fn database_name(&self, database: DatabaseId) -> Option<&str> {
        self.database_names
            .iter()
            .find(|(_, id)| **id == database)
            .map(|(name, _)| name.as_str())
    }

    pub(crate) fn database(&self, name: &str) -> Option<DatabaseId> {
        let name = normalize_ident(name);
        if name == "public" {
            return Some(DatabaseId::new(MAIN_DB_ID));
        }
        self.database_names.get(&name).copied()
    }

    pub(crate) fn schema(&self, database: DatabaseId) -> Result<&Schema> {
        self.database_schemas
            .get(&database)
            .copied()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "semantic catalog has no database {}",
                    database.index()
                ))
            })
    }

    pub(crate) fn database_snapshots(&self) -> Vec<DatabaseSnapshot> {
        let mut snapshots = self
            .database_schemas
            .iter()
            .map(|(database, schema)| DatabaseSnapshot {
                database: *database,
                schema_version: schema.schema_version,
            })
            .collect::<Vec<_>>();
        snapshots.sort_unstable_by_key(|snapshot| snapshot.database.index());
        snapshots
    }

    /// Whether `name` resolves to a table, without reporting a missing one.
    pub(crate) fn table_exists(&self, name: &turso_parser::ast::QualifiedName) -> Result<bool> {
        let table_name = normalize_ident(name.name.as_str());
        Ok(match &name.db_name {
            Some(database) => {
                let database = self.database(database.as_str()).ok_or_else(|| {
                    LimboError::InvalidArgument(format!(
                        "no such database: {}",
                        normalize_ident(database.as_str())
                    ))
                })?;
                self.schema(database)?.get_table(&table_name).is_some()
            }
            None => self
                .unqualified_database_search_path
                .iter()
                .any(|database| {
                    self.database_schemas
                        .get(database)
                        .is_some_and(|schema| schema.get_table(&table_name).is_some())
                }),
        })
    }

    /// Find a view by name when no table has it, searching databases in the
    /// same order as tables.
    pub(crate) fn resolve_view(
        &self,
        name: &turso_parser::ast::QualifiedName,
    ) -> Result<Option<&'catalog crate::schema::View>> {
        let view_name = normalize_ident(name.name.as_str());
        let has_view = |database: &DatabaseId| {
            self.database_schemas
                .get(database)
                .is_some_and(|schema| schema.views.contains_key(&view_name))
        };
        let database = match &name.db_name {
            Some(database) => Some(self.database(database.as_str()).ok_or_else(|| {
                LimboError::InvalidArgument(format!(
                    "no such database: {}",
                    normalize_ident(database.as_str())
                ))
            })?),
            None => self
                .unqualified_database_search_path
                .iter()
                .copied()
                .find(has_view),
        };
        Ok(database
            .and_then(|database| self.database_schemas.get(&database).copied())
            .and_then(|schema| schema.views.get(&view_name))
            .map(AsRef::as_ref))
    }

    pub(crate) fn resolve_table(
        &self,
        name: &turso_parser::ast::QualifiedName,
    ) -> Result<(DatabaseId, Arc<Table>)> {
        let table_name = normalize_ident(name.name.as_str());
        let database = match &name.db_name {
            Some(database) => self.database(database.as_str()).ok_or_else(|| {
                LimboError::InvalidArgument(format!(
                    "no such database: {}",
                    normalize_ident(database.as_str())
                ))
            })?,
            None => schema_table_database(&table_name)
                .filter(|database| self.unqualified_database_search_path.contains(database))
                .or_else(|| {
                    self.unqualified_database_search_path
                        .iter()
                        .copied()
                        .find(|database| {
                            self.database_schemas
                                .get(database)
                                .is_some_and(|schema| schema.get_table(&table_name).is_some())
                        })
                })
                .ok_or_else(|| no_such_table(name))?,
        };
        let table = self
            .schema(database)?
            .get_table(&table_name)
            .ok_or_else(|| no_such_table(name))?;
        Ok((database, table))
    }

    /// Whether `name` is a view whose stored SQL failed to parse when the
    /// schema was loaded.
    pub(crate) fn is_broken_view(&self, name: &turso_parser::ast::QualifiedName) -> bool {
        let view_name = normalize_ident(name.name.as_str());
        let broken = |database: &DatabaseId| {
            self.database_schemas
                .get(database)
                .is_some_and(|schema| schema.broken_views.contains(&view_name))
        };
        match &name.db_name {
            Some(database) => self
                .database(database.as_str())
                .is_some_and(|database| broken(&database)),
            None => self.unqualified_database_search_path.iter().any(broken),
        }
    }

    pub(crate) fn resolve_index(
        &self,
        database: DatabaseId,
        table_name: &str,
        index_name: &str,
    ) -> Result<Arc<Index>> {
        let normalized_name = normalize_ident(index_name);
        self.schema(database)?
            .get_index(table_name, &normalized_name)
            .cloned()
            .ok_or_else(|| LimboError::ParseError(format!("no such index: {index_name}")))
    }

    pub(crate) const fn dqs_dml(&self) -> DoubleQuotedDml {
        self.dqs_dml
    }

    pub(crate) const fn foreign_keys_enabled(&self) -> bool {
        self.foreign_keys_enabled
    }

    pub(crate) const fn snapshot(&self) -> CatalogSnapshot {
        self.snapshot
    }
}

/// Legacy's message, which keeps the schema name the statement used.
fn no_such_table(name: &turso_parser::ast::QualifiedName) -> LimboError {
    LimboError::ParseError(format!(
        "no such table: {}",
        crate::util::table_name_for_error(name)
    ))
}

/// Like legacy and SQLite, an unqualified `sqlite_schema` or `sqlite_master`
/// is main's schema table even when a temp schema exists, and
/// `sqlite_temp_schema` or `sqlite_temp_master` is temp's.
fn schema_table_database(table_name: &str) -> Option<DatabaseId> {
    use crate::schema::{
        SCHEMA_TABLE_NAME, SCHEMA_TABLE_NAME_ALT, TEMP_SCHEMA_TABLE_NAME,
        TEMP_SCHEMA_TABLE_NAME_ALT,
    };
    let is = |name: &str| table_name.eq_ignore_ascii_case(name);
    if is(TEMP_SCHEMA_TABLE_NAME) || is(TEMP_SCHEMA_TABLE_NAME_ALT) {
        Some(DatabaseId::new(crate::TEMP_DB_ID))
    } else if is(SCHEMA_TABLE_NAME) || is(SCHEMA_TABLE_NAME_ALT) {
        Some(DatabaseId::new(crate::MAIN_DB_ID))
    } else {
        None
    }
}
