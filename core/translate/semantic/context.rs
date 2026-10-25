use std::sync::atomic::{AtomicU64, Ordering};

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

use super::catalog::{EmptyTempSchema, SemanticCatalog};
use super::hir::{CatalogSnapshot, DatabaseId, DatabaseSnapshot};
use crate::namespace::{NamespaceId, NamespaceRef};

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
    database_schemas: Vec<(DatabaseId, &'catalog Schema)>,
    symbols: &'catalog SymbolTable,
    custom_types_enabled: bool,
    dialect: Arc<dyn Dialect>,
    database_names: Vec<(String, DatabaseId)>,
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
    /// A table definition that stands in for the catalog's while a schema
    /// statement rewrites it: ALTER COLUMN validates constraints and refills
    /// indexes against the table as it will be, before the catalog has it.
    altered_table: Option<AlteredTable>,
}

/// See [`SemanticContext::with_altered_table`].
#[derive(Clone)]
struct AlteredTable {
    database: DatabaseId,
    table: Arc<Table>,
    indexes: Vec<Arc<Index>>,
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
                .with_empty_temp_database_name(catalog)
                .with_dqs_dml(dqs_dml)
                .with_foreign_keys(foreign_keys_enabled)
                .with_temp_database(catalog.has_temp_database)
                .with_search_path_configured(catalog.search_path_configured)
        })
    }

    fn with_catalog(mut self, catalog: &'catalog SemanticCatalog) -> Self {
        self.catalog = Some(catalog);
        self
    }

    fn with_empty_temp_database_name(mut self, catalog: &SemanticCatalog) -> Self {
        if catalog.empty_temp_schema.is_some() {
            let temp = DatabaseId::new(TEMP_DB_ID);
            match self
                .database_names
                .iter_mut()
                .find(|(name, _)| name == "temp")
            {
                Some((_, id)) => *id = temp,
                None => self.database_names.push(("temp".to_string(), temp)),
            }
        }
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
        let mut database_names: Vec<(String, DatabaseId)> = Vec::new();
        let mut database_schemas: Vec<(DatabaseId, &Schema)> = Vec::new();
        for database in databases {
            let name = normalize_ident(&database.name);
            if database_names.iter().any(|(existing, _)| *existing == name) {
                return Err(LimboError::InvalidArgument(format!(
                    "duplicate database name: {name}"
                )));
            }
            if database_schemas.iter().any(|(id, _)| *id == database.id) {
                return Err(LimboError::InvalidArgument(format!(
                    "duplicate database id: {}",
                    database.id.index()
                )));
            }
            database_names.push((name, database.id));
            database_schemas.push((database.id, database.schema));
        }
        let main_schema = database_schemas
            .iter()
            .find(|(id, _)| *id == DatabaseId::new(MAIN_DB_ID))
            .map(|(_, schema)| *schema)
            .ok_or_else(|| LimboError::InvalidArgument("missing main database".to_string()))?;
        for database in &unqualified_database_search_path {
            if !database_schemas.iter().any(|(id, _)| id == database) {
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
            altered_table: None,
        })
    }

    /// This context with every unqualified name resolved in `database`
    /// alone, the way a view's body resolves its names: a TEMP table does
    /// not shadow a table of the view's own database.
    pub(crate) fn scoped_to_database(&self, database: DatabaseId) -> Self {
        Self {
            catalog: self.catalog,
            main_schema: self.main_schema,
            database_schemas: self.database_schemas.clone(),
            symbols: self.symbols,
            custom_types_enabled: self.custom_types_enabled,
            dialect: self.dialect.clone(),
            database_names: self.database_names.clone(),
            unqualified_database_search_path: vec![database],
            has_temp_database: false,
            search_path_configured: self.search_path_configured,
            dqs_dml: self.dqs_dml,
            foreign_keys_enabled: self.foreign_keys_enabled,
            capture_changes_table: self.capture_changes_table.clone(),
            long_column_names: self.long_column_names,
            snapshot: self.snapshot,
            altered_table: self.altered_table.clone(),
        }
    }

    /// This context with `table` and its `indexes` read in place of the
    /// catalog's table of the same name in `database`.
    pub(crate) fn with_altered_table(
        &self,
        database: DatabaseId,
        table: Arc<Table>,
        indexes: Vec<Arc<Index>>,
    ) -> Self {
        Self {
            catalog: self.catalog,
            main_schema: self.main_schema,
            database_schemas: self.database_schemas.clone(),
            symbols: self.symbols,
            custom_types_enabled: self.custom_types_enabled,
            dialect: self.dialect.clone(),
            database_names: self.database_names.clone(),
            unqualified_database_search_path: self.unqualified_database_search_path.clone(),
            has_temp_database: self.has_temp_database,
            search_path_configured: self.search_path_configured,
            dqs_dml: self.dqs_dml,
            foreign_keys_enabled: self.foreign_keys_enabled,
            capture_changes_table: self.capture_changes_table.clone(),
            long_column_names: self.long_column_names,
            snapshot: self.snapshot,
            altered_table: Some(AlteredTable {
                database,
                table,
                indexes,
            }),
        }
    }

    /// The indexes of `table_name` in `database`, other than the ones that
    /// back a table.
    pub(crate) fn table_indexes(
        &self,
        database: DatabaseId,
        table_name: &str,
    ) -> Result<Vec<Arc<Index>>> {
        if let Some(altered) = &self.altered_table {
            if altered.database == database
                && altered.table.get_name().eq_ignore_ascii_case(table_name)
            {
                return Ok(altered.indexes.clone());
            }
        }
        Ok(self
            .schema(database)?
            .get_indices(NamespaceId::ROOT, table_name)
            .cloned()
            .collect())
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
            .database_schema(DatabaseId::new(database_id))
            .unwrap_or_else(|| panic!("database {database_id} is not in the catalog"));
        f(schema)
    }

    /// The numbers of the attached databases, lowest first.
    pub(crate) fn attached_database_ids_in_search_order(&self) -> Result<BitSet> {
        let mut ids: Vec<usize> = self
            .database_schemas
            .iter()
            .map(|(database, _)| database.index())
            .filter(|&database| database != MAIN_DB_ID && database != TEMP_DB_ID)
            .collect();
        ids.sort_unstable();
        Ok(ids.into_iter().try_collect()?)
    }

    /// The namespace that refers to database `index`, if any.
    pub(crate) fn namespace_name(&self, index: usize) -> Option<String> {
        self.database_name_by_index(index)
    }

    pub(crate) fn dialect(&self) -> &Arc<dyn Dialect> {
        &self.dialect
    }

    pub(crate) fn database_name_by_index(&self, index: usize) -> Option<String> {
        self.database_name(DatabaseId::new(index))
            .map(str::to_string)
    }

    /// The database a schema statement names: the qualifier when there is
    /// one, else main.
    pub(crate) fn resolve_namespace(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        let Some(db_name) = &qualified_name.namespace else {
            return Ok(NamespaceRef::root_of(MAIN_DB_ID));
        };
        let db_name = normalize_ident(db_name.as_str());
        self.database(&db_name)
            .map(|database| NamespaceRef::root_of(database.index()))
            .ok_or_else(|| LimboError::InvalidArgument(format!("no such database: {db_name}")))
    }

    /// The database a CREATE puts an object whose name has no qualifier
    /// in: the first database of the search path.
    pub(crate) fn resolve_namespace_for_create(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        if qualified_name.namespace.is_some() {
            return self.resolve_namespace(qualified_name);
        }
        self.unqualified_database_search_path
            .iter()
            .map(|database| database.index())
            .find(|database| *database != crate::TEMP_DB_ID)
            .map(NamespaceRef::root_of)
            .ok_or_else(|| {
                LimboError::ParseError("no schema has been selected to create in".to_string())
            })
    }

    /// The database of a sequence named without a qualifier: the first
    /// database of the search path that has it.
    pub(crate) fn find_existing_sequence(&self, seq_name: &str) -> Option<NamespaceRef> {
        self.unqualified_database_search_path
            .iter()
            .map(|database| NamespaceRef::root_of(database.index()))
            .find(|namespace| {
                self.with_schema(namespace.database_id, |schema| {
                    schema.get_sequence(namespace.id, seq_name).is_some()
                })
            })
    }

    /// The database holding the table, view or materialized view
    /// `qualified_name` names.
    pub(crate) fn resolve_existing_table(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        if qualified_name.namespace.is_some() {
            return self.resolve_namespace(qualified_name);
        }
        self.resolve_existing_table_named(qualified_name.name.as_str())
    }

    /// The database holding the table, view or materialized view named
    /// `table_name`, looked up along the unqualified search path.
    pub(crate) fn resolve_existing_table_named(&self, table_name: &str) -> Result<NamespaceRef> {
        if table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME_ALT)
        {
            return Ok(NamespaceRef::root_of(TEMP_DB_ID));
        }
        if table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME_ALT)
        {
            return Ok(NamespaceRef::root_of(MAIN_DB_ID));
        }
        self.resolve_unqualified_existing(table_name, |schema, namespace, name| {
            schema.get_table(namespace, name).is_some()
                || schema.get_view(namespace, name).is_some()
                || schema.get_materialized_view(namespace, name).is_some()
        })
    }

    pub(crate) fn resolve_existing_index(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        if qualified_name.namespace.is_some() {
            return self.resolve_namespace(qualified_name);
        }
        let index_name = normalize_ident(qualified_name.name.as_str());
        self.resolve_unqualified_existing(&index_name, |schema, namespace, name| {
            schema
                .indexes(namespace)
                .any(|index| index.name.eq_ignore_ascii_case(name))
        })
    }

    pub(crate) fn resolve_existing_trigger(
        &self,
        qualified_name: &turso_parser::ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        if qualified_name.namespace.is_some() {
            return self.resolve_namespace(qualified_name);
        }
        self.resolve_unqualified_existing(
            qualified_name.name.as_str(),
            |schema, namespace, name| schema.get_trigger(namespace, name).is_some(),
        )
    }

    /// The first database of the unqualified search path holding an object
    /// `schema_has` finds. Without a configured search path an object that
    /// is nowhere is taken to be in main, so the caller reports it missing
    /// there; with one it is an error.
    pub(crate) fn resolve_unqualified_existing(
        &self,
        object_name: &str,
        namespace_has: impl Fn(&Schema, NamespaceId, &str) -> bool,
    ) -> Result<NamespaceRef> {
        for database in &self.unqualified_database_search_path {
            if namespace_has(self.schema(*database)?, NamespaceId::ROOT, object_name) {
                return Ok(NamespaceRef::root_of(database.index()));
            }
        }
        if self.search_path_configured {
            return Err(LimboError::ParseError(format!(
                "no such object: {object_name}"
            )));
        }
        Ok(NamespaceRef::root_of(MAIN_DB_ID))
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
            .find(|(_, id)| *id == database)
            .map(|(name, _)| name.as_str())
    }

    pub(crate) fn database(&self, name: &str) -> Option<DatabaseId> {
        let name = normalize_ident(name);
        if name == "public" {
            return Some(DatabaseId::new(MAIN_DB_ID));
        }
        self.database_names
            .iter()
            .find(|(existing, _)| *existing == name)
            .map(|(_, id)| *id)
    }

    pub(crate) fn schema(&self, database: DatabaseId) -> Result<&Schema> {
        self.database_schema(database).ok_or_else(|| {
            LimboError::InternalError(format!(
                "semantic catalog has no database {}",
                database.index()
            ))
        })
    }

    pub(crate) fn database_snapshots(&self) -> Vec<DatabaseSnapshot> {
        let empty_temp_snapshot = self.empty_temp_schema().map(|empty_temp| DatabaseSnapshot {
            database: DatabaseId::new(TEMP_DB_ID),
            schema_version: empty_temp.schema_version(),
        });
        let mut snapshots = self
            .database_schemas
            .iter()
            .map(|(database, schema)| DatabaseSnapshot {
                database: *database,
                schema_version: schema.schema_version,
            })
            .chain(empty_temp_snapshot)
            .collect::<Vec<_>>();
        snapshots.sort_unstable_by_key(|snapshot| snapshot.database.index());
        snapshots
    }

    /// The databases an unqualified name `table_name` is looked up in, in
    /// order. Inside the body of a view of database `view_database`, only
    /// that database, except that a TEMP view may name any database and the
    /// schema table of a TEMP view is main's.
    fn unqualified_databases(
        &self,
        table_name: &str,
        view_database: Option<DatabaseId>,
    ) -> Vec<DatabaseId> {
        match view_database {
            Some(database) if database.index() != TEMP_DB_ID => vec![database],
            Some(_) if schema_table_database(table_name).is_some() => {
                vec![DatabaseId::new(MAIN_DB_ID)]
            }
            _ => schema_table_database(table_name)
                .filter(|database| self.unqualified_database_search_path.contains(database))
                .map(|database| vec![database])
                .unwrap_or_else(|| self.unqualified_database_search_path.clone()),
        }
    }

    /// Whether `name` resolves to a table, without reporting a missing one,
    /// with unqualified names looked up as inside a
    /// view of `view_database`.
    /// Find a view by name when no table has it, searching databases in the
    /// same order as tables, with unqualified names looked up as inside a
    /// view of `view_database`. Returns the database the view is in.
    pub(crate) fn resolve_view_in(
        &self,
        name: &turso_parser::ast::QualifiedName,
        view_database: Option<DatabaseId>,
    ) -> Result<Option<(DatabaseId, &'catalog crate::schema::View)>> {
        let view_name = normalize_ident(name.name.as_str());
        let has_view = |database: &DatabaseId| {
            self.database_schema(*database)
                .is_some_and(|schema| schema.get_view(NamespaceId::ROOT, &view_name).is_some())
        };
        let database = match &name.namespace {
            Some(database) => Some(self.database(database.as_str()).ok_or_else(|| {
                LimboError::InvalidArgument(format!(
                    "no such database: {}",
                    normalize_ident(database.as_str())
                ))
            })?),
            None => self
                .unqualified_databases(&view_name, view_database)
                .into_iter()
                .find(has_view),
        };
        let Some(database) = database else {
            return Ok(None);
        };
        Ok(self
            .database_schema(database)
            .and_then(|schema| {
                schema
                    .views(NamespaceId::ROOT)
                    .find(|(name, _)| *name == view_name)
            })
            .map(|(_, view)| (database, view.as_ref())))
    }

    pub(crate) fn resolve_table(
        &self,
        name: &turso_parser::ast::QualifiedName,
    ) -> Result<(DatabaseId, Arc<Table>)> {
        self.resolve_table_in(name, None)
    }

    /// [`Self::resolve_table`] with unqualified names looked up as inside a
    /// view of `view_database`.
    pub(crate) fn resolve_table_in(
        &self,
        name: &turso_parser::ast::QualifiedName,
        view_database: Option<DatabaseId>,
    ) -> Result<(DatabaseId, Arc<Table>)> {
        self.find_table_in(name, view_database)?
            .ok_or_else(|| no_such_table(name))
    }

    pub(crate) fn find_table_in(
        &self,
        name: &turso_parser::ast::QualifiedName,
        view_database: Option<DatabaseId>,
    ) -> Result<Option<(DatabaseId, Arc<Table>)>> {
        let table_name = normalize_ident(name.name.as_str());
        let found = match &name.namespace {
            Some(database) => {
                let database = self.database(database.as_str()).ok_or_else(|| {
                    LimboError::InvalidArgument(format!(
                        "no such database: {}",
                        normalize_ident(database.as_str())
                    ))
                })?;
                self.schema(database)?
                    .get_table(NamespaceId::ROOT, &table_name)
                    .map(|table| (database, table))
            }
            None => self
                .unqualified_databases(&table_name, view_database)
                .into_iter()
                .find_map(|database| {
                    self.database_schema(database)?
                        .get_table(NamespaceId::ROOT, &table_name)
                        .map(|table| (database, table))
                }),
        };
        let Some((database, table)) = found else {
            return Ok(None);
        };
        if let Some(altered) = &self.altered_table {
            if altered.database == database
                && altered.table.get_name().eq_ignore_ascii_case(&table_name)
            {
                return Ok(Some((database, altered.table.clone())));
            }
        }
        Ok(Some((database, table)))
    }

    /// Whether `name` is a view whose stored SQL failed to parse when the
    /// schema was loaded.
    pub(crate) fn is_broken_view(&self, name: &turso_parser::ast::QualifiedName) -> bool {
        let view_name = normalize_ident(name.name.as_str());
        let broken = |database: &DatabaseId| {
            self.database_schema(*database)
                .is_some_and(|schema| schema.is_broken_view(NamespaceId::ROOT, &view_name))
        };
        match &name.namespace {
            Some(database) => self
                .database(database.as_str())
                .is_some_and(|database| broken(&database)),
            None => self.unqualified_database_search_path.iter().any(broken),
        }
    }

    /// Whether `name` is a materialized view whose stored state was written
    /// by a Turso version with a different DBSP circuit version. The view
    /// stays in the schema so it can be dropped, but no statement reads it.
    pub(crate) fn is_incompatible_view(&self, name: &turso_parser::ast::QualifiedName) -> bool {
        let view_name = normalize_ident(name.name.as_str());
        let incompatible = |database: &DatabaseId| {
            self.database_schema(*database)
                .is_some_and(|schema| schema.is_incompatible_view(NamespaceId::ROOT, &view_name))
        };
        match &name.namespace {
            Some(database) => self
                .database(database.as_str())
                .is_some_and(|database| incompatible(&database)),
            None => self
                .unqualified_database_search_path
                .iter()
                .any(incompatible),
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
            .get_index(NamespaceId::ROOT, table_name, &normalized_name)
            .cloned()
            .ok_or_else(|| LimboError::ParseError(format!("no such index: {index_name}")))
    }

    /// PostgreSQL rule: `t.c` reads the nearest source named `t` only.
    pub(crate) fn qualified_column_uses_nearest_table_only(&self) -> bool {
        self.dialect.qualified_column_uses_nearest_table_only()
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

    fn database_schema(&self, database: DatabaseId) -> Option<&'catalog Schema> {
        if let Some((_, schema)) = self.database_schemas.iter().find(|(id, _)| *id == database) {
            return Some(schema);
        }
        if database.index() != TEMP_DB_ID {
            return None;
        }
        self.empty_temp_schema()
            .map(|empty_temp| empty_temp.schema(self.dialect.as_ref()))
    }

    fn empty_temp_schema(&self) -> Option<&'catalog EmptyTempSchema> {
        self.catalog?.empty_temp_schema.as_ref()
    }
}

/// The error message keeps the schema name the statement used.
fn no_such_table(name: &turso_parser::ast::QualifiedName) -> LimboError {
    LimboError::ParseError(format!(
        "no such table: {}",
        crate::util::table_name_for_error(name)
    ))
}

/// Like SQLite, an unqualified `sqlite_schema` or `sqlite_master` is main's
/// schema table even when a temp schema exists, and
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
