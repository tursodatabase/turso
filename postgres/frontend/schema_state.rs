//! The PostgreSQL frontend's schema state: what the engine keeps for it as
//! the opaque dialect schema state of the main database. It holds the
//! relation catalog of [`crate::classes`], and the roles of [`crate::roles`]
//! with their memberships of [`crate::auth_members`], each loaded from its
//! own table.

use std::sync::{Arc, Mutex};

use turso_core::schema::Schema;
use turso_core::{
    CatalogApply, Insn, LimboError, LoadQuery, LoadStep, ProgramBuilder, Row, SchemaLoad,
    MAIN_DB_ID,
};

use crate::auth_members::MembershipRows;
use crate::classes::{ClassCatalog, ClassRows};
use crate::roles::{RoleCatalog, RoleRows};

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

/// The steps that load the frontend's schema state, in the order they run:
/// the relation catalog, then the roles, then the memberships, each from
/// its own table. The last step stores what they read in the schema.
pub(crate) fn load_steps() -> Vec<Box<dyn LoadStep>> {
    let read = Arc::new(Mutex::new(SchemaStateRead::default()));
    vec![
        Box::new(ClassRows::new(read.clone())),
        Box::new(RoleRows::new(read.clone())),
        Box::new(MembershipRows::new(read.clone())),
        Box::new(StoresSchemaState { read }),
    ]
}

/// What the steps of [`load_steps`] have read so far. It stays out of the
/// schema until all of it is read, so that no statement is compiled against
/// a relation catalog without its roles.
#[derive(Default)]
pub(crate) struct SchemaStateRead {
    /// None when the database has no relation catalog. Such a database has
    /// no schema state, and its roles and memberships are not read.
    pub(crate) classes: Option<ClassCatalog>,
    /// None when the roles were not read. A database without the roles
    /// table has no roles, and its memberships are not read.
    pub(crate) roles: Option<RoleCatalog>,
}

/// Stores what the steps before it read as the schema state of the schema
/// being read. It has no statement of its own.
struct StoresSchemaState {
    read: Arc<Mutex<SchemaStateRead>>,
}

impl LoadStep for StoresSchemaState {
    fn next_query(&mut self, load: &mut SchemaLoad) -> turso_core::Result<Option<LoadQuery>> {
        let read = std::mem::take(&mut *self.read.lock().unwrap());
        let Some(classes) = read.classes else {
            return Ok(None);
        };
        load.schema_mut()?.dialect_schema = Some(Arc::new(SchemaState {
            classes: Arc::new(classes),
            roles: Arc::new(read.roles.unwrap_or_default()),
        }));
        Ok(None)
    }

    fn row(&mut self, _load: &mut SchemaLoad, _row: &Row) -> turso_core::Result<()> {
        unreachable!("storing the schema state has no statement")
    }
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
