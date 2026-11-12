//! The PostgreSQL frontend's schema state: what the engine keeps for it as
//! the opaque dialect schema state of the main database. It holds the
//! relation catalog of [`crate::classes`] and the roles of [`crate::roles`],
//! each loaded from its own table.

use std::any::Any;
use std::sync::Arc;

use turso_core::schema::Schema;
use turso_core::{CatalogChange, LimboError, Value};

use crate::classes::{self, ClassCatalog};
use crate::roles::{self, RoleCatalog};

#[derive(Debug, Clone, Default)]
pub(crate) struct SchemaState {
    pub(crate) classes: Arc<ClassCatalog>,
    pub(crate) roles: Arc<RoleCatalog>,
}

/// The frontend's schema state, or None when it is not loaded, as in a
/// read-only open of an empty database.
pub(crate) fn schema_state(schema: &Schema) -> Option<&SchemaState> {
    schema.dialect_schema.as_ref()?.downcast_ref()
}

/// [`turso_core::Dialect::schema_state_queries`] for the PostgreSQL
/// frontend: the relation catalog, then the roles. A database without the
/// relation catalog has no schema state.
pub(crate) fn schema_state_queries(schema: &Schema) -> Vec<String> {
    let Some(classes_query) = classes::schema_state_query(schema) else {
        return Vec::new();
    };
    std::iter::once(classes_query)
        .chain(roles::schema_state_query(schema))
        .collect()
}

/// [`turso_core::Dialect::build_schema_state`] for the PostgreSQL frontend.
/// A database without the roles table has no roles.
pub(crate) fn build_schema_state(
    schema: &Schema,
    results: &[Vec<Vec<Value>>],
) -> turso_core::Result<Arc<dyn Any + Send + Sync>> {
    let (class_rows, role_rows) = match results {
        [class_rows] => (class_rows, None),
        [class_rows, role_rows] => (class_rows, Some(role_rows)),
        _ => {
            return Err(LimboError::InternalError(format!(
                "expected the rows of two schema state queries, got {}",
                results.len()
            )))
        }
    };
    let roles = match role_rows {
        Some(rows) => RoleCatalog::new(rows)?,
        None => RoleCatalog::default(),
    };
    Ok(Arc::new(SchemaState {
        classes: Arc::new(ClassCatalog::build(schema, class_rows)?),
        roles: Arc::new(roles),
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
        CatalogChange::CreateRole { name, .. } => {
            Arc::make_mut(&mut state_mut.roles).apply_create_role(name, values)
        }
        _ => Arc::make_mut(&mut state_mut.classes).apply(change, values),
    };
    schema.dialect_schema = Some(state);
    result
}
