use std::sync::atomic::{AtomicU64, Ordering};

use rustc_hash::FxHashMap as HashMap;

use crate::{
    dialect::Dialect,
    function::Func,
    schema::{Index, Schema, Table},
    sync::Arc,
    util::normalize_ident,
    LimboError, Result, SymbolTable, MAIN_DB_ID,
};

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
    main_schema: &'catalog Schema,
    database_schemas: HashMap<DatabaseId, &'catalog Schema>,
    symbols: &'catalog SymbolTable,
    custom_types_enabled: bool,
    dialect: Arc<dyn Dialect>,
    database_names: HashMap<String, DatabaseId>,
    unqualified_database_search_path: Vec<DatabaseId>,
    dqs_dml: DoubleQuotedDml,
    snapshot: CatalogSnapshot,
}

impl<'catalog> SemanticContext<'catalog> {
    pub(crate) fn for_catalog(
        catalog: &'catalog super::catalog::SemanticCatalog,
        symbols: &'catalog SymbolTable,
        custom_types_enabled: bool,
        dialect: Arc<dyn Dialect>,
        dqs_dml: DoubleQuotedDml,
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
        .map(|context| context.with_dqs_dml(dqs_dml))
    }

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
            main_schema,
            database_schemas,
            symbols,
            custom_types_enabled,
            dialect,
            database_names,
            unqualified_database_search_path,
            dqs_dml: DoubleQuotedDml::Enabled,
            snapshot: CatalogSnapshot::from_id(snapshot),
        })
    }

    pub(crate) fn with_dqs_dml(mut self, dqs_dml: DoubleQuotedDml) -> Self {
        self.dqs_dml = dqs_dml;
        self
    }

    pub(crate) const fn main_schema(&self) -> &Schema {
        self.main_schema
    }

    pub(crate) const fn symbols(&self) -> &SymbolTable {
        self.symbols
    }

    pub(crate) const fn custom_types_enabled(&self) -> bool {
        self.custom_types_enabled
    }

    pub(crate) fn dialect(&self) -> &Arc<dyn Dialect> {
        &self.dialect
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
            None => self
                .unqualified_database_search_path
                .iter()
                .copied()
                .find(|database| {
                    self.database_schemas
                        .get(database)
                        .is_some_and(|schema| schema.get_table(&table_name).is_some())
                })
                .ok_or_else(|| LimboError::ParseError(format!("no such table: {table_name}")))?,
        };
        let table = self
            .schema(database)?
            .get_table(&table_name)
            .ok_or_else(|| LimboError::ParseError(format!("no such table: {table_name}")))?;
        Ok((database, table))
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

    pub(crate) const fn snapshot(&self) -> CatalogSnapshot {
        self.snapshot
    }
}
