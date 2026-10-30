use std::cell::RefCell;

use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast;

use crate::alloc::TursoIteratorExt;
use crate::function::Func;
use crate::namespace::{NamespaceId, NamespaceRef};
use crate::schema::Schema;
use crate::sync::Arc;
use crate::util::normalize_ident;
use crate::{DatabaseCatalog, LimboError, Result, RwLock, SymbolTable};

use super::collate::TextCollation;
use super::plan::BitSet;

pub(crate) struct CatalogContext<'a> {
    schema: &'a Schema,
    database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
    temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
    attached_databases: &'a RwLock<DatabaseCatalog>,
    qualifiers: &'a crate::Qualifiers,
    non_main_schema_cache: RefCell<HashMap<usize, Arc<Schema>>>,
    pub symbol_table: &'a SymbolTable,
    pub enable_custom_types: bool,
    /// Schema dialect of the database being compiled against; used when a
    /// fresh placeholder schema must be constructed during resolution.
    pub dialect: Arc<dyn crate::dialect::Dialect>,
    pub(super) database_scope: RefCell<Option<DatabaseScopeContext>>,
    /// Cached flag: true when this connection has an active temp database.
    ///
    /// Computed once at context construction to avoid repeated
    /// `RwLock` reads on every table-name resolution. Safe because a
    /// context is short-lived (single translate pass) and a
    /// connection is single-threaded at the VDBE layer: the temp
    /// database can only be initialized / torn down *between*
    /// contexts on the same connection, not during. If you add a
    /// path that can initialize the temp database *inside* translate
    /// (e.g. via a nested sub-program), update this field on that
    /// path or switch to a live read.
    has_temp_schema: bool,
    /// The namespaces that a name without a qualifier is looked up in, in
    /// order, from the connection's search path. Temp comes before them.
    /// The first one is where CREATE puts an object whose name has no
    /// qualifier.
    search_order: Vec<NamespaceRef>,
}

/// Context for restricting table resolution while compiling the body of a
/// database object (a trigger or a view). Mirrors SQLite's view/trigger
/// schema fixer (`sqlite3FixSelect`/`DbFixer` in `src/fix.c`): unqualified
/// table names inside the object's body resolve within the database the
/// object itself lives in, not through the normal temp-first search.
#[derive(Debug, Clone)]
pub(super) struct DatabaseScopeContext {
    /// The namespace the object belongs to.
    pub namespace: NamespaceRef,
    /// "trigger" or "view" (for error messages).
    pub kind: &'static str,
    /// The object's own name (for error messages).
    pub object_name: String,
}

impl DatabaseScopeContext {
    fn restricts_db_references(&self) -> bool {
        self.namespace.database_id != crate::TEMP_DB_ID
    }
}

impl<'a> CatalogContext<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: &'a Schema,
        database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
        temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
        attached_databases: &'a RwLock<DatabaseCatalog>,
        qualifiers: &'a crate::Qualifiers,
        symbol_table: &'a SymbolTable,
        enable_custom_types: bool,
        dialect: Arc<dyn crate::dialect::Dialect>,
        search_path: Option<&[String]>,
    ) -> Self {
        let has_temp_schema = temp_database.read().is_some();
        Self {
            schema,
            database_schemas,
            temp_database,
            attached_databases,
            qualifiers,
            non_main_schema_cache: RefCell::new(HashMap::default()),
            symbol_table,
            enable_custom_types,
            dialect,
            database_scope: RefCell::new(None),
            has_temp_schema,
            search_order: qualifiers
                .search_order(search_path)
                .into_iter()
                .map(|namespace| namespace.target)
                .collect(),
        }
    }

    pub(crate) fn fork(&self) -> Self {
        Self {
            schema: self.schema,
            database_schemas: self.database_schemas,
            temp_database: self.temp_database,
            attached_databases: self.attached_databases,
            qualifiers: self.qualifiers,
            non_main_schema_cache: RefCell::new(HashMap::default()),
            symbol_table: self.symbol_table,
            enable_custom_types: self.enable_custom_types,
            dialect: self.dialect.clone(),
            database_scope: RefCell::new(self.database_scope.borrow().clone()),
            has_temp_schema: self.has_temp_schema,
            search_order: self.search_order.clone(),
        }
    }

    pub fn schema(&self) -> &Schema {
        self.schema
    }

    pub fn has_temp_database(&self) -> bool {
        self.has_temp_schema
    }

    pub fn require_custom_types(&self, feature: &str) -> Result<()> {
        if !self.enable_custom_types {
            crate::bail_parse_error!("{} require --experimental-custom-types flag", feature);
        }
        Ok(())
    }

    pub(crate) fn with_database_scope<T>(
        &self,
        namespace: NamespaceRef,
        kind: &'static str,
        object_name: String,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let previous = self
            .database_scope
            .borrow_mut()
            .replace(DatabaseScopeContext {
                namespace,
                kind,
                object_name,
            });
        let result = f();
        *self.database_scope.borrow_mut() = previous;
        result
    }

    pub(crate) fn with_view_scope<T>(
        &self,
        namespace: NamespaceRef,
        view_name: String,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        self.with_database_scope(namespace, "view", view_name, f)
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

    pub fn resolve_collation(&self, name: &str) -> Result<TextCollation> {
        if let Some(collation) = self.symbol_table.resolve_collation(name) {
            return Ok(collation);
        }
        TextCollation::new(name)
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

    fn cached_non_main_schema(&self, database_id: usize) -> Arc<Schema> {
        crate::turso_assert_ne!(database_id, crate::MAIN_DB_ID);

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
                    let mut schema =
                        Schema::with_options(self.enable_custom_types, self.dialect.as_ref())
                            .expect("built-in type definitions are malformed");
                    schema.copy_table_valued_functions(self.schema);
                    Arc::new(schema)
                }),
            _ => {
                let attached_dbs = self.attached_databases.read();
                let entry = attached_dbs
                    .index_to_data
                    .get(&database_id)
                    .expect("Database ID should be valid after resolve_namespace");
                let schema = entry.db.schema.lock().clone();
                schema
            }
        };

        self.non_main_schema_cache
            .borrow_mut()
            .insert(database_id, loaded_schema.clone());
        loaded_schema
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

    pub(crate) fn resolve_unqualified_existing<F>(
        &self,
        object_name: &str,
        namespace_has_object: F,
    ) -> Result<NamespaceRef>
    where
        F: Fn(&Schema, NamespaceId, &str) -> bool,
    {
        self.find_unqualified_existing(object_name, namespace_has_object)
            .ok_or_else(|| LimboError::ParseError(format!("no such object: {object_name}")))
    }

    /// The namespace of an object named without a qualifier: temp when it
    /// has the object, else the first namespace of the search order that
    /// has it. When none has it, the first namespace of the search order,
    /// where the caller reports the object missing or does nothing for IF
    /// EXISTS. None only when the search order is empty.
    fn find_unqualified_existing<F>(
        &self,
        object_name: &str,
        namespace_has_object: F,
    ) -> Option<NamespaceRef>
    where
        F: Fn(&Schema, NamespaceId, &str) -> bool,
    {
        // Only check the temp schema when a temp database actually exists.
        // This avoids expensive schema construction/lookup on every table
        // resolution when no temp objects have been created.
        if self.has_temp_schema
            && self.with_schema(crate::TEMP_DB_ID, |schema| {
                namespace_has_object(schema, NamespaceId::ROOT, object_name)
            })
        {
            return Some(NamespaceRef::root_of(crate::TEMP_DB_ID));
        }

        self.search_order
            .iter()
            .copied()
            .find(|namespace| {
                self.with_schema(namespace.database_id, |schema| {
                    namespace_has_object(schema, namespace.id, object_name)
                })
            })
            .or_else(|| self.search_order.first().copied())
    }

    /// The namespace of the table, view or materialized view named
    /// `qualified_name`.
    pub(crate) fn resolve_existing_table(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        self.find_existing_table(qualified_name)?.ok_or_else(|| {
            LimboError::ParseError(format!("no such object: {}", qualified_name.name.as_str()))
        })
    }

    /// Like `resolve_existing_table`, for a table that a statement reads or
    /// writes. A table in a database that does not exist is a missing table
    /// here, as in SQLite's sqlite3LocateTable.
    pub(crate) fn resolve_queried_table(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        if let Some(db_name) = &qualified_name.namespace {
            if self
                .namespace_database_id(&normalize_ident(db_name.as_str()))
                .is_none()
            {
                return Err(LimboError::ParseError(format!(
                    "no such table: {}",
                    crate::util::table_name_for_error(qualified_name)
                )));
            }
        }
        let namespace = self.resolve_existing_table(qualified_name)?;
        self.reject_query_of_other_relation_kind(namespace, qualified_name.name.as_str())?;
        Ok(namespace)
    }

    /// Fails when a dialect whose relations share names finds an index or a
    /// sequence where a statement reads or writes a table.
    fn reject_query_of_other_relation_kind(
        &self,
        namespace: NamespaceRef,
        name: &str,
    ) -> Result<()> {
        if !self.dialect.relations_share_names() {
            return Ok(());
        }
        let name = normalize_ident(name);
        let kind = self.with_schema(namespace.database_id, |schema| {
            if Self::namespace_has_table_like_object(schema, namespace.id, &name) {
                None
            } else if Self::namespace_has_index(schema, namespace.id, &name) {
                Some("an index")
            } else if schema.has_sequence(namespace.id, &name) {
                Some("a sequence")
            } else {
                None
            }
        });
        match kind {
            Some(kind) => Err(self
                .dialect
                .wrong_relation_kind_error(format!("\"{name}\" is {kind}"))),
            None => Ok(()),
        }
    }

    pub(crate) fn resolve_existing_table_named(&self, table_name: &str) -> Result<NamespaceRef> {
        self.find_existing_table_named(table_name)
            .ok_or_else(|| LimboError::ParseError(format!("no such object: {table_name}")))
    }

    pub(crate) fn find_existing_table(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<Option<NamespaceRef>> {
        if qualified_name.namespace.is_some() {
            return self.resolve_namespace(qualified_name).map(Some);
        }
        Ok(self.find_existing_table_named(qualified_name.name.as_str()))
    }

    fn find_existing_table_named(&self, table_name: &str) -> Option<NamespaceRef> {
        // A TEMP view or trigger carries a scope (so explicit `aux.table`
        // references still see its own database below), but the scope does
        // not restrict unqualified names: SQLite's schema fixer skips TEMP
        // objects entirely (`bTemp` in sqlite3FixInit, attach.c:547, checked
        // by fixSelectCb at attach.c:495), so a TEMP view resolves names the
        // same way a top-level query does, sqlite_schema included.
        if let Some(ctx) = self.database_scope.borrow().as_ref() {
            if ctx.restricts_db_references() {
                // The virtual tables of main are found from the namespaces of
                // main only: an attached database's view or trigger sees its
                // own database, as SQLite's fixer binds it there.
                if ctx.namespace.database_id == crate::MAIN_DB_ID
                    && !self.with_schema(ctx.namespace.database_id, |schema| {
                        Self::namespace_has_table_like_object(schema, ctx.namespace.id, table_name)
                    })
                {
                    if let Some(root) = self.root_virtual_table(table_name) {
                        return Some(root);
                    }
                }
                return Some(ctx.namespace);
            }
        }

        if let Some(namespace) = Self::namespace_of_schema_table(table_name) {
            return Some(namespace);
        }

        let found =
            self.find_unqualified_relation(table_name, Self::namespace_has_table_like_object);
        match found {
            Some(namespace)
                if self.with_schema(namespace.database_id, |schema| {
                    Self::namespace_has_table_like_object(schema, namespace.id, table_name)
                }) =>
            {
                Some(namespace)
            }
            other => self.root_virtual_table(table_name).or(other),
        }
    }

    /// The root namespace of the main database when it has a virtual table
    /// called `table_name`. The engine's virtual tables, such as table-valued
    /// functions and a frontend's catalog tables, exist only there and are
    /// found from every namespace, the way PostgreSQL's `pg_catalog` is on
    /// every search path.
    fn root_virtual_table(&self, table_name: &str) -> Option<NamespaceRef> {
        self.with_schema(crate::MAIN_DB_ID, |schema| {
            matches!(
                schema.get_table(NamespaceId::ROOT, table_name).as_deref(),
                Some(crate::schema::Table::Virtual(_))
            )
        })
        .then(|| NamespaceRef::root_of(crate::MAIN_DB_ID))
    }

    fn namespace_of_schema_table(table_name: &str) -> Option<NamespaceRef> {
        if table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME_ALT)
        {
            return Some(NamespaceRef::root_of(crate::TEMP_DB_ID));
        }

        if table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME_ALT)
        {
            return Some(NamespaceRef::root_of(crate::MAIN_DB_ID));
        }

        None
    }

    /// Like `find_unqualified_existing` for a relation: when the relations of
    /// the dialect share names, the first namespace that has a relation of
    /// the name, whatever its kind.
    fn find_unqualified_relation<F>(
        &self,
        object_name: &str,
        namespace_has_object: F,
    ) -> Option<NamespaceRef>
    where
        F: Fn(&Schema, NamespaceId, &str) -> bool,
    {
        if self.dialect.relations_share_names() {
            self.find_unqualified_existing(object_name, Self::namespace_has_relation)
        } else {
            self.find_unqualified_existing(object_name, namespace_has_object)
        }
    }

    fn namespace_has_relation(schema: &Schema, namespace: NamespaceId, name: &str) -> bool {
        Self::namespace_has_table_like_object(schema, namespace, name)
            || Self::namespace_has_index(schema, namespace, name)
            || schema.has_sequence(namespace, name)
    }

    pub(crate) fn namespace_has_table_like_object(
        schema: &Schema,
        namespace: NamespaceId,
        table_name: &str,
    ) -> bool {
        schema.get_table(namespace, table_name).is_some()
            || schema.get_view(namespace, table_name).is_some()
            || schema
                .get_materialized_view(namespace, table_name)
                .is_some()
    }

    pub(crate) fn resolve_existing_index(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        self.find_existing_index(qualified_name)?.ok_or_else(|| {
            LimboError::ParseError(format!(
                "no such object: {}",
                normalize_ident(qualified_name.name.as_str())
            ))
        })
    }

    pub(crate) fn find_existing_index(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<Option<NamespaceRef>> {
        if qualified_name.namespace.is_some() {
            return self.resolve_namespace(qualified_name).map(Some);
        }

        let index_name = normalize_ident(qualified_name.name.as_str());
        Ok(self.find_unqualified_existing(&index_name, Self::namespace_has_index))
    }

    fn namespace_has_index(schema: &Schema, namespace: NamespaceId, index_name: &str) -> bool {
        schema
            .indexes(namespace)
            .any(|index| index.name.eq_ignore_ascii_case(index_name))
    }

    pub(crate) fn resolve_existing_trigger(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        if qualified_name.namespace.is_some() {
            return self.resolve_namespace(qualified_name);
        }

        let trigger_name = qualified_name.name.as_str();
        self.resolve_unqualified_existing(trigger_name, Self::namespace_has_trigger)
    }

    fn namespace_has_trigger(schema: &Schema, namespace: NamespaceId, trigger_name: &str) -> bool {
        schema.get_trigger(namespace, trigger_name).is_some()
    }

    /// The namespace of a sequence named without a qualifier: the first
    /// namespace of the search order that has it, or the first namespace of
    /// the search order when none has it.
    pub(crate) fn resolve_existing_sequence(&self, seq_name: &str) -> Result<NamespaceRef> {
        let namespace = self
            .find_existing_sequence(seq_name)
            .ok_or_else(|| LimboError::ParseError(format!("no such object: {seq_name}")))?;
        let is_other_relation = self.dialect.relations_share_names()
            && self.with_schema(namespace.database_id, |schema| {
                !schema.has_sequence(namespace.id, seq_name)
                    && Self::namespace_has_relation(schema, namespace.id, seq_name)
            });
        if is_other_relation {
            return Err(self
                .dialect
                .wrong_relation_kind_error(format!("\"{seq_name}\" is not a sequence")));
        }
        Ok(namespace)
    }

    pub(crate) fn find_existing_sequence(&self, seq_name: &str) -> Option<NamespaceRef> {
        self.find_unqualified_relation(seq_name, |schema, namespace, seq_name| {
            schema.get_sequence(namespace, seq_name).is_some()
        })
    }

    /// The namespace and bare name of the sequence whose mangled name is
    /// `mangled`. A column default read from the stored text of a table of
    /// a namespace names its serial sequence so, since that text names the
    /// table by its mangled name too.
    pub(crate) fn sequence_of_mangled_name(&self, mangled: &str) -> Option<(NamespaceRef, String)> {
        self.with_schema(crate::MAIN_DB_ID, |schema| {
            let (id, name) = schema.demangle_name(mangled);
            (!id.is_root() && schema.has_sequence(id, name)).then(|| {
                (
                    NamespaceRef {
                        database_id: crate::MAIN_DB_ID,
                        id,
                    },
                    name.to_string(),
                )
            })
        })
    }

    /// `seq_name` qualified with the namespace it resolved to, for the
    /// opcodes that look a sequence up again by its name when the statement
    /// runs.
    pub(crate) fn runtime_sequence_name(&self, namespace: NamespaceRef, seq_name: &str) -> String {
        let namespace_name = self
            .name_of_namespace(namespace)
            .expect("the namespace of a sequence has a name");
        format!("{namespace_name}.{seq_name}")
    }

    /// The namespace that the qualifier of `qualified_name` refers to, or
    /// the one an unqualified name is in: the scope's own namespace inside
    /// the body of a trigger or a view, else the root of main.
    pub(crate) fn resolve_namespace(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        let resolved = if let Some(db_name) = &qualified_name.namespace {
            let db_name_normalized = normalize_ident(db_name.as_str());
            self.namespace_named(&db_name_normalized).ok_or_else(|| {
                LimboError::InvalidArgument(format!("no such database: {db_name_normalized}"))
            })
        } else if let Some(ctx) = self.database_scope.borrow().as_ref() {
            if ctx.restricts_db_references() {
                Ok(ctx.namespace)
            } else {
                Ok(NamespaceRef::root_of(crate::MAIN_DB_ID))
            }
        } else {
            Ok(NamespaceRef::root_of(crate::MAIN_DB_ID))
        }?;

        // A trigger or view can only reference tables in its own database.
        // This only fires for explicitly qualified names (e.g. "aux.table")
        // since unqualified names already resolve to the object's own
        // database above.
        if let Some(ctx) = self.database_scope.borrow().as_ref() {
            if !ctx.restricts_db_references() {
                return Ok(resolved);
            }
            if resolved.database_id != ctx.namespace.database_id {
                let db_name = qualified_name
                    .namespace
                    .as_ref()
                    .map(|n| n.as_str())
                    .unwrap_or("main");
                return Err(LimboError::ParseError(format!(
                    "{} {} cannot reference objects in database {}",
                    ctx.kind, ctx.object_name, db_name
                )));
            }
        }

        Ok(resolved)
    }

    /// Resolve the namespace that CREATE puts an object named
    /// `qualified_name` in.
    pub(crate) fn resolve_namespace_for_create(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<NamespaceRef> {
        if qualified_name.namespace.is_some() {
            return self.resolve_namespace(qualified_name);
        }
        self.search_order.first().copied().ok_or_else(|| {
            LimboError::ParseError("no schema has been selected to create in".to_string())
        })
    }

    /// The database that namespace `name` refers to. `name` is normalized.
    pub(crate) fn namespace_database_id(&self, name: &str) -> Option<usize> {
        self.qualifiers.database_id(name)
    }

    /// The namespace called `name`, whatever the statement is being
    /// compiled for. `name` is normalized.
    pub(crate) fn namespace_named(&self, name: &str) -> Option<NamespaceRef> {
        self.qualifiers.get(name).map(|namespace| namespace.target)
    }

    /// Get the database name for a given database index.
    /// Returns "main" for index 0, "temp" for index 1, and the alias for attached databases.
    pub(crate) fn namespace_name(&self, index: usize) -> Option<String> {
        self.qualifiers
            .of_database(index)
            .map(|namespace| namespace.name)
    }

    /// The name of `namespace`: the database's name for a root namespace,
    /// else the name the namespace was added under.
    pub(crate) fn name_of_namespace(&self, namespace: NamespaceRef) -> Option<String> {
        self.qualifiers.name_of(namespace)
    }
}
