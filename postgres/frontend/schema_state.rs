//! The PostgreSQL frontend's schema state: what the engine keeps for it as
//! the opaque dialect schema state of the main database. It holds the
//! relation catalog of [`crate::classes`], and the roles of [`crate::roles`]
//! with their memberships of [`crate::auth_members`], each loaded from its
//! own table.

use std::any::Any;
use std::sync::Arc;

use turso_core::schema::Schema;
use turso_core::{CatalogApply, Insn, LimboError, ProgramBuilder, Value, MAIN_DB_ID};

use crate::auth_members;
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
/// frontend: the relation catalog, then the roles, then the memberships. A
/// database without the relation catalog has no schema state. The
/// membership table is read only with the roles table, which is created
/// before it.
pub(crate) fn schema_state_queries(schema: &Schema) -> Vec<String> {
    let Some(classes_query) = classes::schema_state_query(schema) else {
        return Vec::new();
    };
    let mut queries = vec![classes_query];
    if let Some(roles_query) = roles::schema_state_query(schema) {
        queries.push(roles_query);
        queries.extend(auth_members::schema_state_query(schema));
    }
    queries
}

/// [`turso_core::Dialect::build_schema_state`] for the PostgreSQL frontend.
/// A database without the roles table has no roles, and one without the
/// membership table has no memberships.
pub(crate) fn build_schema_state(
    schema: &Schema,
    results: &[Vec<Vec<Value>>],
) -> turso_core::Result<Arc<dyn Any + Send + Sync>> {
    let (class_rows, role_rows, membership_rows): (_, &[_], &[_]) = match results {
        [class_rows] => (class_rows, &[], &[]),
        [class_rows, role_rows] => (class_rows, role_rows, &[]),
        [class_rows, role_rows, membership_rows] => (class_rows, role_rows, membership_rows),
        _ => {
            return Err(LimboError::InternalError(format!(
                "expected the rows of up to three schema state queries, got {}",
                results.len()
            )))
        }
    };
    let roles = RoleCatalog::new(role_rows, membership_rows)?;
    Ok(Arc::new(SchemaState {
        classes: Arc::new(ClassCatalog::build(schema, class_rows)?),
        roles: Arc::new(roles),
    }))
}

/// Changes the frontend's schema state in `schema` with `change`, so that
/// it follows the stored rows the running statement wrote. Does nothing when
/// the state is not loaded.
pub(crate) fn change_schema_state(
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

/// Emits the instruction that makes the change `apply` to the in-memory
/// schema state of the main database, with the values of `value_regs`.
pub(crate) fn emit_catalog_apply(
    program: &mut ProgramBuilder,
    apply: impl CatalogApply + 'static,
    value_regs: Vec<usize>,
) {
    program.emit_insn(Insn::CatalogApply {
        db: MAIN_DB_ID,
        apply: Arc::new(apply),
        value_regs,
    });
}
