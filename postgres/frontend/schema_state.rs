//! The PostgreSQL frontend's schema state: what the engine keeps for it as
//! the opaque dialect schema state of the main database. It holds the
//! relation catalog of [`crate::classes`], the roles of [`crate::roles`]
//! with their memberships of [`crate::auth_members`], and the privileges on
//! schemas of [`crate::namespaces`], each loaded from its own table.

use std::any::Any;
use std::sync::Arc;

use turso_core::schema::Schema;
use turso_core::{CatalogChange, GrantTarget, LimboError, Value};

use crate::auth_members;
use crate::classes::{self, ClassCatalog};
use crate::namespaces::{self, NamespaceAcls};
use crate::roles::{self, RoleCatalog};

#[derive(Debug, Clone, Default)]
pub(crate) struct SchemaState {
    pub(crate) classes: Arc<ClassCatalog>,
    pub(crate) roles: Arc<RoleCatalog>,
    pub(crate) namespace_acls: Arc<NamespaceAcls>,
}

/// The frontend's schema state, or None when it is not loaded, as in a
/// read-only open of an empty database.
pub(crate) fn schema_state(schema: &Schema) -> Option<&SchemaState> {
    schema.dialect_schema.as_ref()?.downcast_ref()
}

/// [`turso_core::Dialect::schema_state_queries`] for the PostgreSQL
/// frontend: the relation catalog, then the roles, then the memberships,
/// then the privileges on schemas. A database without the relation catalog
/// has no schema state. Each later table is read only with the ones before
/// it, which are created before it.
pub(crate) fn schema_state_queries(schema: &Schema) -> Vec<String> {
    let Some(classes_query) = classes::schema_state_query(schema) else {
        return Vec::new();
    };
    let mut queries = vec![classes_query];
    if let Some(roles_query) = roles::schema_state_query(schema) {
        queries.push(roles_query);
        if let Some(memberships_query) = auth_members::schema_state_query(schema) {
            queries.push(memberships_query);
            queries.extend(namespaces::schema_state_query(schema));
        }
    }
    queries
}

/// [`turso_core::Dialect::build_schema_state`] for the PostgreSQL frontend.
/// A database without the roles table has no roles, one without the
/// membership table has no memberships, and one without the table of the
/// privileges on schemas has the default privileges on every schema.
pub(crate) fn build_schema_state(
    schema: &Schema,
    results: &[Vec<Vec<Value>>],
) -> turso_core::Result<Arc<dyn Any + Send + Sync>> {
    let (class_rows, role_rows, membership_rows, namespace_acl_rows): (_, &[_], &[_], &[_]) =
        match results {
            [class_rows] => (class_rows, &[], &[], &[]),
            [class_rows, role_rows] => (class_rows, role_rows, &[], &[]),
            [class_rows, role_rows, membership_rows] => {
                (class_rows, role_rows, membership_rows, &[])
            }
            [class_rows, role_rows, membership_rows, namespace_acl_rows] => {
                (class_rows, role_rows, membership_rows, namespace_acl_rows)
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "expected the rows of up to four schema state queries, got {}",
                    results.len()
                )))
            }
        };
    let roles = RoleCatalog::new(role_rows, membership_rows)?;
    Ok(Arc::new(SchemaState {
        classes: Arc::new(ClassCatalog::build(schema, class_rows)?),
        roles: Arc::new(roles),
        namespace_acls: Arc::new(NamespaceAcls::new(namespace_acl_rows)?),
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
        CatalogChange::GrantRole { revoke, .. } => {
            Arc::make_mut(&mut state_mut.roles).apply_grant_role(*revoke, values)
        }
        CatalogChange::Grant {
            target: GrantTarget::Namespace(_),
            ..
        }
        | CatalogChange::DropNamespace { .. } => {
            Arc::make_mut(&mut state_mut.namespace_acls).apply(change, values)
        }
        _ => Arc::make_mut(&mut state_mut.classes).apply(change, values),
    };
    schema.dialect_schema = Some(state);
    result
}
