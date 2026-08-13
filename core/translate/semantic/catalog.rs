use rustc_hash::FxHashMap as HashMap;

use crate::{
    connection::TempDatabase, dialect::Dialect, schema::Schema, sync::Arc, DatabaseCatalog, Result,
    RwLock, MAIN_DB_ID, TEMP_DB_ID,
};

use super::hir::DatabaseId;

/// Owned catalog inputs for one semantic-analysis pass.
pub(crate) struct SemanticCatalogDatabase {
    pub(crate) id: DatabaseId,
    pub(crate) name: String,
    pub(crate) schema: Arc<Schema>,
}

/// Stable schema references and lookup order captured before semantic analysis.
pub(crate) struct SemanticCatalog {
    pub(crate) databases: Vec<SemanticCatalogDatabase>,
    pub(crate) unqualified_database_search_path: Vec<DatabaseId>,
}

impl SemanticCatalog {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn capture(
        main_schema: Arc<Schema>,
        database_schemas: &RwLock<HashMap<usize, Arc<Schema>>>,
        temp_database: &RwLock<Option<TempDatabase>>,
        attached_databases: &RwLock<DatabaseCatalog>,
        custom_types_enabled: bool,
        dialect: &dyn Dialect,
        configured_search_path: Option<&[String]>,
    ) -> Result<Self> {
        let (has_temp_schema, temp_schema) = match temp_database.read().as_ref() {
            Some(temp) => (true, temp.db.schema.lock().clone()),
            None => (
                false,
                Arc::new(Schema::with_options(custom_types_enabled, dialect)?),
            ),
        };

        let mut attached = {
            let catalog = attached_databases.read();
            catalog
                .name_to_index
                .iter()
                .map(|(name, database)| {
                    let (database_handle, _) = catalog
                        .index_to_data
                        .get(database)
                        .expect("attached database name must have matching catalog data");
                    (*database, name.clone(), database_handle.clone())
                })
                .collect::<Vec<_>>()
        };
        attached.sort_unstable_by_key(|(database, _, _)| *database);

        // Clone the Arc values and release the connection-local schema lock before
        // reading shared attached schemas.
        let staged_schemas = database_schemas.read().clone();

        let mut databases = vec![
            SemanticCatalogDatabase {
                id: DatabaseId::new(MAIN_DB_ID),
                name: "main".to_string(),
                schema: main_schema,
            },
            SemanticCatalogDatabase {
                id: DatabaseId::new(TEMP_DB_ID),
                name: "temp".to_string(),
                schema: temp_schema,
            },
        ];
        for (database, name, database_handle) in &attached {
            let schema = staged_schemas
                .get(database)
                .cloned()
                .unwrap_or_else(|| database_handle.schema.lock().clone());
            databases.push(SemanticCatalogDatabase {
                id: DatabaseId::new(*database),
                name: name.clone(),
                schema,
            });
        }

        let mut unqualified_database_search_path = Vec::new();
        if has_temp_schema {
            unqualified_database_search_path.push(DatabaseId::new(TEMP_DB_ID));
        }
        match configured_search_path {
            Some(configured) => {
                for name in configured {
                    if name.eq_ignore_ascii_case("public") {
                        unqualified_database_search_path.push(DatabaseId::new(MAIN_DB_ID));
                    } else if let Some((database, _, _)) = attached
                        .iter()
                        .find(|(_, attached_name, _)| attached_name.eq_ignore_ascii_case(name))
                    {
                        unqualified_database_search_path.push(DatabaseId::new(*database));
                    }
                }
            }
            None => {
                unqualified_database_search_path.push(DatabaseId::new(MAIN_DB_ID));
                unqualified_database_search_path.extend(
                    attached
                        .iter()
                        .map(|(database, _, _)| DatabaseId::new(*database)),
                );
            }
        }

        Ok(Self {
            databases,
            unqualified_database_search_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::SqliteDialect;

    #[test]
    fn captured_catalog_owns_schema_arcs_and_lookup_order() {
        let main_schema = Arc::new(Schema::new());
        let database_schemas = RwLock::new(HashMap::default());
        let temp_database = RwLock::new(None);
        let attached_databases = RwLock::new(DatabaseCatalog::new());

        let catalog = SemanticCatalog::capture(
            main_schema.clone(),
            &database_schemas,
            &temp_database,
            &attached_databases,
            true,
            &SqliteDialect,
            None,
        )
        .expect("catalog capture succeeds");
        drop(main_schema);
        drop(database_schemas);
        drop(temp_database);
        drop(attached_databases);

        assert_eq!(
            catalog
                .databases
                .iter()
                .map(|database| (database.id.index(), database.name.as_str()))
                .collect::<Vec<_>>(),
            [(MAIN_DB_ID, "main"), (TEMP_DB_ID, "temp")]
        );
        assert_eq!(
            catalog.unqualified_database_search_path,
            [DatabaseId::new(MAIN_DB_ID)]
        );
        assert_eq!(catalog.databases[0].schema.schema_version, 0);
    }
}
