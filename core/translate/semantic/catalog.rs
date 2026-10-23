use rustc_hash::FxHashMap as HashMap;

use crate::{
    connection::{PlaceholderTempSchema, TempDbContext},
    dialect::Dialect,
    schema::Schema,
    sync::Arc,
    DatabaseCatalog, Result, RwLock, MAIN_DB_ID, TEMP_DB_ID,
};

use super::hir::DatabaseId;

/// Owned catalog inputs for one semantic-analysis pass.
#[derive(Clone)]
pub(crate) struct SemanticCatalogDatabase {
    pub(crate) id: DatabaseId,
    pub(crate) name: String,
    pub(crate) schema: Arc<Schema>,
}

/// Stable schema references and lookup order captured before semantic analysis.
pub(crate) struct SemanticCatalog {
    pub(crate) databases: Vec<SemanticCatalogDatabase>,
    pub(crate) unqualified_database_search_path: Vec<DatabaseId>,
    /// Whether the connection has a TEMP database with objects of its own.
    pub(crate) has_temp_database: bool,
    /// Whether the prepare call set the search path, in which case an
    /// unqualified name that is in none of its databases is an error.
    pub(crate) search_path_configured: bool,
}

impl SemanticCatalog {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn capture(
        main_schema: Arc<Schema>,
        database_schemas: &RwLock<HashMap<usize, Arc<Schema>>>,
        temp: &TempDbContext,
        attached_databases: &RwLock<DatabaseCatalog>,
        namespaces: &crate::Namespaces,
        custom_types_enabled: bool,
        dialect: &dyn Dialect,
        configured_search_path: Option<&[String]>,
    ) -> Result<Self> {
        let temp_schema = temp
            .database
            .read()
            .as_ref()
            .map(|temp| temp.db.schema.lock().clone());
        let has_temp_schema = temp_schema.is_some();
        let temp_schema = match temp_schema {
            Some(schema) => schema,
            None => placeholder_temp_schema(temp, &main_schema, custom_types_enabled, dialect)?,
        };

        let mut attached = {
            let catalog = attached_databases.read();
            catalog
                .index_to_data
                .iter()
                .map(|(database, attached)| {
                    let name = namespaces
                        .name(*database)
                        .expect("attached database must have a namespace");
                    (*database, name, attached.db.clone())
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
            has_temp_database: has_temp_schema,
            search_path_configured: configured_search_path.is_some(),
        })
    }
}

/// The schema a statement reads as `temp` while the connection has no
/// temp database: empty, with the main schema's table-valued functions.
/// The connection keeps the one built last, and a statement prepared
/// against the same main schema reads it again.
fn placeholder_temp_schema(
    temp: &TempDbContext,
    main_schema: &Arc<Schema>,
    custom_types_enabled: bool,
    dialect: &dyn Dialect,
) -> Result<Arc<Schema>> {
    if let Some(placeholder) = temp.placeholder_schema.read().as_ref() {
        if Arc::ptr_eq(&placeholder.main, main_schema)
            && placeholder.custom_types_enabled == custom_types_enabled
        {
            return Ok(placeholder.schema.clone());
        }
    }
    let mut schema = Schema::with_options(custom_types_enabled, dialect)?;
    schema.copy_table_valued_functions(main_schema);
    let schema = Arc::new(schema);
    *temp.placeholder_schema.write() = Some(PlaceholderTempSchema {
        main: main_schema.clone(),
        custom_types_enabled,
        schema: schema.clone(),
    });
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::SqliteDialect;

    #[test]
    fn captured_catalog_owns_schema_arcs_and_lookup_order() {
        let main_schema = Arc::new(Schema::new());
        let database_schemas = RwLock::new(HashMap::default());
        let temp = TempDbContext::new();
        let attached_databases = RwLock::new(DatabaseCatalog::new());

        let catalog = SemanticCatalog::capture(
            main_schema.clone(),
            &database_schemas,
            &temp,
            &attached_databases,
            &crate::Namespaces::default(),
            true,
            &SqliteDialect,
            None,
        )
        .expect("catalog capture succeeds");
        drop(main_schema);
        drop(database_schemas);
        drop(temp);
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

    #[test]
    fn statements_without_a_temp_database_share_one_placeholder_temp_schema() {
        let main_schema = Arc::new(Schema::new());
        let database_schemas = RwLock::new(HashMap::default());
        let temp = TempDbContext::new();
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let capture = |main_schema: &Arc<Schema>| {
            SemanticCatalog::capture(
                main_schema.clone(),
                &database_schemas,
                &temp,
                &attached_databases,
                &crate::Namespaces::default(),
                true,
                &SqliteDialect,
                None,
            )
            .expect("catalog capture succeeds")
            .databases[TEMP_DB_ID]
                .schema
                .clone()
        };

        let first = capture(&main_schema);
        let second = capture(&main_schema);
        assert!(Arc::ptr_eq(&first, &second));

        let changed_main_schema = Arc::new(Schema::new());
        let third = capture(&changed_main_schema);
        assert!(!Arc::ptr_eq(&first, &third));
        assert!(Arc::ptr_eq(&third, &capture(&changed_main_schema)));
    }
}
