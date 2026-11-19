//! The PostgreSQL frontend's schema state: what the engine keeps for it as
//! the opaque dialect schema state of the main database. It holds the
//! relation catalog of [`crate::classes`], and the roles of [`crate::roles`]
//! with their memberships of [`crate::auth_members`], each loaded from its
//! own table.

use std::sync::Arc;

use turso_core::schema::Schema;
use turso_core::{
    CatalogApply, Insn, LimboError, LoadQuery, LoadStep, ProgramBuilder, Row, SchemaLoad, Value,
    MAIN_DB_ID,
};

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

/// The step that loads the frontend's schema state.
pub(crate) fn load_steps() -> Vec<Box<dyn LoadStep>> {
    vec![Box::new(SchemaStateRows::default())]
}

/// Reads the relation catalog, then the roles, then the memberships, each
/// from its own table, and stores the schema state after the last one. One
/// step reads all three so that no statement is compiled against a relation
/// catalog without its roles. `table` is the table that the last statement
/// reads, None before the first statement.
#[derive(Default)]
struct SchemaStateRows {
    table: Option<Table>,
    class_rows: Vec<Vec<Value>>,
    role_rows: Vec<Vec<Value>>,
    membership_rows: Vec<Vec<Value>>,
}

#[derive(Clone, Copy)]
enum Table {
    Classes,
    Roles,
    Memberships,
}

impl LoadStep for SchemaStateRows {
    fn next_query(&mut self, load: &mut SchemaLoad) -> turso_core::Result<Option<LoadQuery>> {
        let schema = load.schema();
        let next = match self.table {
            None => classes::schema_state_query(schema).map(|sql| (Table::Classes, sql)),
            Some(Table::Classes) => {
                roles::schema_state_query(schema).map(|sql| (Table::Roles, sql))
            }
            Some(Table::Roles) => {
                auth_members::schema_state_query(schema).map(|sql| (Table::Memberships, sql))
            }
            Some(Table::Memberships) => None,
        };
        // A table that the database lacks ends the read. The roles table is
        // created before the membership table, so a database without the
        // first has no memberships to read.
        let Some((table, sql)) = next else {
            // A database without the relation catalog has no schema state.
            if self.table.is_some() {
                self.store_schema_state(load)?;
            }
            return Ok(None);
        };
        self.table = Some(table);
        Ok(Some(LoadQuery::engine_metadata_read(sql)))
    }

    fn row(&mut self, _load: &mut SchemaLoad, row: &Row) -> turso_core::Result<()> {
        let rows = match self.table.expect("a row belongs to a statement") {
            Table::Classes => &mut self.class_rows,
            Table::Roles => &mut self.role_rows,
            Table::Memberships => &mut self.membership_rows,
        };
        rows.push(row.get_values().cloned().collect());
        Ok(())
    }
}

impl SchemaStateRows {
    /// A database without the roles table has no roles, and one without the
    /// membership table has no memberships.
    fn store_schema_state(&self, load: &mut SchemaLoad) -> turso_core::Result<()> {
        let roles = RoleCatalog::new(&self.role_rows, &self.membership_rows)?;
        let classes = ClassCatalog::build(load.schema(), &self.class_rows)?;
        load.schema_mut()?.dialect_schema = Some(Arc::new(SchemaState {
            classes: Arc::new(classes),
            roles: Arc::new(roles),
        }));
        Ok(())
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
