//! The PostgreSQL frontend's schema state: what the engine keeps for it as
//! the opaque dialect schema state of the main database. It holds the
//! relation catalog of [`crate::classes`], the roles of [`crate::roles`] and
//! the functions of [`crate::sql_functions`], each loaded from its own table.

use std::any::Any;
use std::sync::Arc;

use turso_core::schema::Schema;
use turso_core::{CatalogChange, LimboError, Value};

use crate::classes::{self, ClassCatalog};
use crate::roles::{self, RoleCatalog};
use crate::sql_functions::{self, FunctionCatalog};

#[derive(Debug, Clone, Default)]
pub(crate) struct SchemaState {
    pub(crate) classes: Arc<ClassCatalog>,
    pub(crate) roles: Arc<RoleCatalog>,
    pub(crate) functions: Arc<FunctionCatalog>,
}

/// The frontend's schema state, or None when it is not loaded, as in a
/// read-only open of an empty database.
pub(crate) fn schema_state(schema: &Schema) -> Option<&SchemaState> {
    schema.dialect_schema.as_ref()?.downcast_ref()
}

/// [`turso_core::Dialect::schema_state_queries`] for the PostgreSQL
/// frontend: the relation catalog, then the roles, then the functions. A
/// database without the relation catalog has no schema state.
pub(crate) fn schema_state_queries(schema: &Schema) -> Vec<String> {
    let Some(classes_query) = classes::schema_state_query(schema) else {
        return Vec::new();
    };
    std::iter::once(classes_query)
        .chain(roles::schema_state_query(schema))
        .chain(sql_functions::schema_state_query(schema))
        .collect()
}

/// [`turso_core::Dialect::build_schema_state`] for the PostgreSQL frontend.
/// A database without the roles table has no roles, and one without the
/// functions table has no functions.
pub(crate) fn build_schema_state(
    schema: &Schema,
    results: &[Vec<Vec<Value>>],
) -> turso_core::Result<Arc<dyn Any + Send + Sync>> {
    let has_roles = roles::schema_state_query(schema).is_some();
    let has_functions = sql_functions::schema_state_query(schema).is_some();
    let expected = 1 + usize::from(has_roles) + usize::from(has_functions);
    if results.len() != expected {
        return Err(LimboError::InternalError(format!(
            "expected the rows of {expected} schema state queries, got {}",
            results.len()
        )));
    }
    let mut results = results.iter();
    let class_rows = results.next().expect("the length is checked above");
    let roles = match has_roles {
        true => RoleCatalog::new(results.next().expect("the length is checked above"))?,
        false => RoleCatalog::default(),
    };
    let functions = match has_functions {
        true => FunctionCatalog::new(results.next().expect("the length is checked above"))?,
        false => FunctionCatalog::default(),
    };
    Ok(Arc::new(SchemaState {
        classes: Arc::new(ClassCatalog::build(schema, class_rows)?),
        roles: Arc::new(roles),
        functions: Arc::new(functions),
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
    update(schema, |state| match change {
        CatalogChange::CreateRole { name, options } => {
            Arc::make_mut(&mut state.roles).apply_create_role(name, options, values)
        }
        _ => Arc::make_mut(&mut state.classes).apply(change, values),
    })
}

/// Changes the state of `schema` with `change`. Does nothing when the state
/// is not loaded.
pub(crate) fn update(
    schema: &mut Schema,
    change: impl FnOnce(&mut SchemaState) -> turso_core::Result<()>,
) -> turso_core::Result<()> {
    let Some(state) = schema.dialect_schema.take() else {
        return Ok(());
    };
    let mut state: Arc<SchemaState> = state.downcast().map_err(|_| {
        LimboError::InternalError("the dialect schema state is not the frontend's".into())
    })?;
    let result = change(Arc::make_mut(&mut state));
    schema.dialect_schema = Some(state);
    result
}
