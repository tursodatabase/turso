//! The PostgreSQL frontend's schema state: what the engine keeps for it as
//! the opaque dialect schema state of the main database. It holds the
//! relation catalog of [`crate::classes`], the roles of [`crate::roles`] and
//! the default privileges of [`crate::default_privileges`], each loaded from
//! its own table.

use std::any::Any;
use std::sync::Arc;

use turso_core::schema::Schema;
use turso_core::{CatalogChange, LimboError, Value};

use crate::classes::{self, ClassCatalog};
use crate::default_privileges::{self, DefaultAcls};
use crate::roles::{self, RoleCatalog};

#[derive(Debug, Clone, Default)]
pub(crate) struct SchemaState {
    pub(crate) classes: Arc<ClassCatalog>,
    pub(crate) roles: Arc<RoleCatalog>,
    pub(crate) default_acls: Arc<DefaultAcls>,
}

/// The frontend's schema state, or None when it is not loaded, as in a
/// read-only open of an empty database.
pub(crate) fn schema_state(schema: &Schema) -> Option<&SchemaState> {
    schema.dialect_schema.as_ref()?.downcast_ref()
}

/// [`turso_core::Dialect::schema_state_queries`] for the PostgreSQL
/// frontend: the relation catalog, then the roles, then the default
/// privileges, leaving out the queries of tables the database does not have.
/// A database without the relation catalog has no schema state.
pub(crate) fn schema_state_queries(schema: &Schema) -> Vec<String> {
    let Some(classes_query) = classes::schema_state_query(schema) else {
        return Vec::new();
    };
    std::iter::once(classes_query)
        .chain(roles::schema_state_query(schema))
        .chain(default_privileges::schema_state_query(schema))
        .collect()
}

/// [`turso_core::Dialect::build_schema_state`] for the PostgreSQL frontend.
/// A database without the roles table has no roles, and one without the
/// default privileges table has no default privileges.
pub(crate) fn build_schema_state(
    schema: &Schema,
    results: &[Vec<Vec<Value>>],
) -> turso_core::Result<Arc<dyn Any + Send + Sync>> {
    let mut results = results.iter();
    let mut next_rows = || {
        results.next().ok_or_else(|| {
            LimboError::InternalError("fewer schema state results than queries".to_string())
        })
    };
    let class_rows = next_rows()?;
    let roles = match roles::schema_state_query(schema) {
        Some(_) => RoleCatalog::new(next_rows()?)?,
        None => RoleCatalog::default(),
    };
    let default_acls = match default_privileges::schema_state_query(schema) {
        Some(_) => DefaultAcls::new(next_rows()?)?,
        None => DefaultAcls::default(),
    };
    if results.next().is_some() {
        return Err(LimboError::InternalError(
            "more schema state results than queries".to_string(),
        ));
    }
    Ok(Arc::new(SchemaState {
        classes: Arc::new(ClassCatalog::build(schema, class_rows)?),
        roles: Arc::new(roles),
        default_acls: Arc::new(default_acls),
    }))
}

/// [`turso_core::Dialect::apply_catalog_change`] for the PostgreSQL
/// frontend: keeps the in-memory state in step with the stored rows the
/// running statement wrote.
pub(crate) fn apply_catalog_change(
    schema: &mut Schema,
    change: &CatalogChange,
    values: &[Value],
) -> turso_core::Result<()> {
    let Some(state) = schema.dialect_schema.take() else {
        return Ok(());
    };
    let mut state: Arc<SchemaState> = state.downcast().map_err(|_| {
        LimboError::InternalError("the dialect schema state is not the frontend's".into())
    })?;
    let state_mut = Arc::make_mut(&mut state);
    let result = match change {
        CatalogChange::CreateRole { name, options } => {
            Arc::make_mut(&mut state_mut.roles).apply_create_role(name, options, values)
        }
        CatalogChange::DefaultPrivileges { .. } => {
            Arc::make_mut(&mut state_mut.default_acls).apply(values)
        }
        _ => Arc::make_mut(&mut state_mut.classes).apply(change, values),
    };
    schema.dialect_schema = Some(state);
    result
}
