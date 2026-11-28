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

/// Loads the frontend's schema state. It reads three tables in order:
/// classes, roles, memberships. `reading` is the table being read now, None
/// before the first one. When a table ends, its rows go into `classes` or
/// `roles`. The step stores the result only after the last table, because
/// a statement must never see classes without their roles.
#[derive(Default)]
struct SchemaStateRows {
    reading: Option<TableRows>,
    classes: ClassCatalog,
    roles: RoleCatalog,
}

struct TableRows {
    table: Table,
    rows: Vec<Vec<Value>>,
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
        let next = match self.reading.as_ref().map(|reading| reading.table) {
            None => classes::schema_state_query(schema)?.map(|sql| (Table::Classes, sql)),
            Some(Table::Classes) => {
                roles::schema_state_query(schema).map(|sql| (Table::Roles, sql))
            }
            Some(Table::Roles) => {
                auth_members::schema_state_query(schema).map(|sql| (Table::Memberships, sql))
            }
            Some(Table::Memberships) => None,
        };
        // Stop at the first table that is missing. Memberships point at
        // roles, so without a roles table there are none to read.
        let Some((table, sql)) = next else {
            // No classes table: this database has no schema state at all.
            if self.reading.is_some() {
                self.store_schema_state(load)?;
            }
            return Ok(None);
        };
        self.reading = Some(TableRows {
            table,
            rows: Vec::new(),
        });
        Ok(Some(LoadQuery::engine_metadata_read(sql)))
    }

    fn row(&mut self, _load: &mut SchemaLoad, row: &Row) -> turso_core::Result<()> {
        let reading = self.reading.as_mut().expect("a row belongs to a statement");
        reading.rows.push(row.get_values().cloned().collect());
        Ok(())
    }

    fn end_of_rows(&mut self, load: &mut SchemaLoad) -> turso_core::Result<()> {
        let TableRows { table, rows } = self.reading.as_ref().expect("rows belong to a statement");
        match table {
            Table::Classes => {
                self.classes = ClassCatalog::build(load.schema(), rows)?;
            }
            Table::Roles => self.roles = RoleCatalog::new(rows, &[])?,
            Table::Memberships => self.roles.set_memberships(rows)?,
        }
        Ok(())
    }
}

impl SchemaStateRows {
    fn store_schema_state(&mut self, load: &mut SchemaLoad) -> turso_core::Result<()> {
        load.schema_mut()?.dialect_schema = Some(Arc::new(SchemaState {
            classes: Arc::new(std::mem::take(&mut self.classes)),
            roles: Arc::new(std::mem::take(&mut self.roles)),
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
