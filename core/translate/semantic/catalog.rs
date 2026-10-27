use rustc_hash::FxHashMap as HashMap;

use crate::{
    connection::TempDbContext,
    dialect::Dialect,
    schema::Schema,
    sync::{Arc, OnceLock},
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
    pub(crate) namespaces: Vec<crate::namespace::Namespace>,
    pub(crate) unqualified_database_search_path: Vec<DatabaseId>,
    /// The schema `temp` names while the connection has no TEMP database.
    pub(crate) empty_temp_schema: Option<EmptyTempSchema>,
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
        configured_search_path: Option<&[String]>,
    ) -> Result<Self> {
        let temp_schema = temp
            .database
            .read()
            .as_ref()
            .map(|temp| temp.db.schema.lock().clone());
        let has_temp_schema = temp_schema.is_some();
        let empty_temp_schema = (!has_temp_schema).then(|| EmptyTempSchema {
            main: main_schema.clone(),
            custom_types_enabled,
            schema: OnceLock::new(),
        });

        let mut attached = {
            let catalog = attached_databases.read();
            catalog
                .index_to_data
                .iter()
                .map(|(database, attached)| {
                    let name = namespaces
                        .of_database(*database)
                        .expect("attached database must have a namespace")
                        .name;
                    (*database, name, attached.db.clone())
                })
                .collect::<Vec<_>>()
        };
        attached.sort_unstable_by_key(|(database, _, _)| *database);

        // Clone the Arc values and release the connection-local schema lock before
        // reading shared attached schemas.
        let staged_schemas = database_schemas.read().clone();

        let mut databases = vec![SemanticCatalogDatabase {
            id: DatabaseId::new(MAIN_DB_ID),
            name: "main".to_string(),
            schema: main_schema,
        }];
        if let Some(temp_schema) = temp_schema {
            databases.push(SemanticCatalogDatabase {
                id: DatabaseId::new(TEMP_DB_ID),
                name: "temp".to_string(),
                schema: temp_schema,
            });
        }
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
        unqualified_database_search_path.extend(
            namespaces
                .search_order(configured_search_path)
                .into_iter()
                .map(|namespace| DatabaseId::from_namespace(namespace.target)),
        );

        Ok(Self {
            databases,
            namespaces: namespaces.all(),
            unqualified_database_search_path,
            empty_temp_schema,
            has_temp_database: has_temp_schema,
            search_path_configured: configured_search_path.is_some(),
        })
    }
}

/// An empty schema with the main schema's table-valued functions, built
/// the first time a statement reads `temp`.
pub(crate) struct EmptyTempSchema {
    main: Arc<Schema>,
    custom_types_enabled: bool,
    schema: OnceLock<Arc<Schema>>,
}

impl EmptyTempSchema {
    pub(crate) fn schema(&self, dialect: &dyn Dialect) -> &Schema {
        self.schema.get_or_init(|| {
            #[cfg(test)]
            tests::EMPTY_TEMP_SCHEMAS_BUILT.with(|built| built.set(built.get() + 1));
            let mut schema = Schema::with_options(self.custom_types_enabled, dialect)
                .expect("built-in type definitions are malformed");
            schema.copy_table_valued_functions(&self.main);
            Arc::new(schema)
        })
    }

    /// The version of the schema, which an empty schema that was never
    /// built has as 0.
    pub(crate) fn schema_version(&self) -> u32 {
        self.schema.get().map_or(0, |schema| schema.schema_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::SqliteDialect;
    use std::cell::Cell;

    thread_local! {
        pub(super) static EMPTY_TEMP_SCHEMAS_BUILT: Cell<usize> = const { Cell::new(0) };
    }

    #[test]
    fn only_a_statement_that_reads_temp_builds_the_empty_temp_schema() {
        let io = Arc::new(crate::MemoryIO::new());
        let db = crate::Database::open_file(io, ":memory:", Arc::new(SqliteDialect))
            .expect("database opens");
        let conn = db
            .connect(crate::ConnectOpts::new())
            .expect("connection opens");
        conn.execute("CREATE TABLE t(a)").expect("table is created");
        EMPTY_TEMP_SCHEMAS_BUILT.with(|built| built.set(0));

        for sql in [
            "SELECT a FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 2",
            "DELETE FROM t",
        ] {
            conn.prepare(sql).expect("statement prepares");
        }
        assert_eq!(EMPTY_TEMP_SCHEMAS_BUILT.with(Cell::get), 0);

        conn.prepare("SELECT name FROM temp.sqlite_temp_master")
            .expect("SELECT from temp prepares");
        assert_eq!(EMPTY_TEMP_SCHEMAS_BUILT.with(Cell::get), 1);
    }

    #[test]
    fn prepare_resolves_namespaces_and_aliases_without_reading_the_root_table() {
        use crate::{Namespace, NamespaceId, NamespaceRef};

        let io = Arc::new(crate::MemoryIO::new());
        let db = crate::Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        let app = NamespaceId::new(7);
        conn.with_schema_mut(|schema| {
            schema.add_namespace(app);
            schema
                .add_btree_table(
                    NamespaceId::ROOT,
                    Arc::new(
                        crate::schema::BTreeTable::from_sql("CREATE TABLE t(root_value)", 2)
                            .unwrap(),
                    ),
                )
                .unwrap();
            schema
                .add_btree_table(
                    app,
                    Arc::new(
                        crate::schema::BTreeTable::from_sql("CREATE TABLE t(app_value)", 3)
                            .unwrap(),
                    ),
                )
                .unwrap();
        })
        .unwrap();
        conn.add_namespace(Namespace {
            name: "app".into(),
            target: NamespaceRef {
                database_id: MAIN_DB_ID,
                id: app,
            },
        });
        conn.add_namespace(Namespace::database("public", MAIN_DB_ID));
        conn.set_search_path(Some(vec!["app".into(), "public".into()]));

        for (sql, column) in [
            ("SELECT * FROM t", "app_value"),
            ("SELECT * FROM app.t", "app_value"),
            ("SELECT * FROM public.t", "root_value"),
            ("SELECT * FROM main.t", "root_value"),
            (
                "INSERT INTO t(app_value) VALUES (1) RETURNING *",
                "app_value",
            ),
            ("UPDATE app.t SET app_value = 2 RETURNING *", "app_value"),
            ("DELETE FROM app.t RETURNING *", "app_value"),
        ] {
            let statement = conn
                .prepare(sql)
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(statement.num_columns(), 1, "{sql}");
            assert_eq!(statement.get_column_name(0).as_ref(), column, "{sql}");
        }
        assert!(conn.prepare("SELECT root_value FROM t").is_err());
        conn.set_search_path(Some(vec!["public".into(), "app".into()]));
        assert_eq!(
            conn.prepare("SELECT * FROM t")
                .unwrap()
                .get_column_name(0)
                .as_ref(),
            "root_value"
        );
    }

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
            [(MAIN_DB_ID, "main")]
        );
        assert!(catalog
            .empty_temp_schema
            .as_ref()
            .is_some_and(|empty_temp| empty_temp.schema.get().is_none()));
        assert_eq!(
            catalog.unqualified_database_search_path,
            [DatabaseId::new(MAIN_DB_ID)]
        );
        assert_eq!(catalog.databases[0].schema.schema_version, 0);
    }
}
