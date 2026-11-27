mod function;
mod vtab;

pub use crate::function::ReturnType;

pub use function::{
    Aggregate, AggregateFunction, FunctionArity, ScalarCall, ScalarFunction, ScalarReturn,
};
pub use turso_ext::{ConstraintInfo, ConstraintUsage, IndexInfo, OrderByInfo, VTabKind};
pub use vtab::{TableUpdate, VirtualTable, VirtualTableCursor, VirtualTableModule};

pub use function::AggregateState;
pub use function::{
    finalize_aggregate, step_aggregate, step_scalar, AggregateFactory, ScalarFactory, ScalarStep,
};
pub(crate) use vtab::{ModuleFactory, NativeCursor, NativeTable};

use crate::function::ExternalFunc;
use crate::sync::Arc;
use crate::types::Cursor;
use crate::{Database, OpenOptions, Result};

pub enum ExtensionState {
    None,
    ScalarCall(Box<dyn ScalarStep>),
    TableUpdate(Box<dyn TableUpdate>),
}

impl OpenOptions {
    pub fn native_module<M: VirtualTableModule + 'static>(
        mut self,
        name: &str,
        kind: VTabKind,
        module: M,
    ) -> Self {
        let name = crate::util::normalize_ident(name);
        let module = Arc::new(crate::ext::VTabImpl {
            module_kind: kind,
            implementation: crate::ext::ModuleImplementation::Native(Arc::new(module)),
        });
        self.native_extensions.modules.push((name, module));
        self
    }

    /// Gives the stored table `name` of the root namespace of the main
    /// database an overlay: a virtual table of `module` with the same name.
    ///
    /// A statement of the connection's user, and a trigger that such a
    /// statement fires, reads the overlay where it names the table, and may
    /// not insert, update or delete rows of the table. A statement of the
    /// engine, such as one that [`crate::Connection::prepare_internal`]
    /// prepares, reads and writes the stored table. So the cursor of the
    /// overlay can read the stored table with a statement that names it.
    ///
    /// The overlay applies only while the database has a stored table with
    /// this name. It is not written to the database file. The overlay
    /// changes where the rows come from, not the columns: the schema of the
    /// module has the columns of the stored table, with the same names in
    /// the same order, and after them only HIDDEN columns. A read of a stored
    /// table that does not have the columns of its overlay fails.
    ///
    /// Row-level security does not limit the reads through the overlay:
    /// its cursor reads the stored table with a statement of the engine,
    /// which no policy limits. A dialect must not let policies apply to a
    /// table that has an overlay.
    pub fn table_overlay<M: VirtualTableModule + 'static>(mut self, name: &str, module: M) -> Self {
        let name = crate::util::normalize_ident(name);
        let module = Arc::new(crate::ext::VTabImpl {
            module_kind: VTabKind::TableValuedFunction,
            implementation: crate::ext::ModuleImplementation::Native(Arc::new(module)),
        });
        self.native_extensions.overlays.push((name, module));
        self
    }
}

#[derive(Clone, Default)]
pub struct NativeExtensions {
    pub functions: Vec<Arc<ExternalFunc>>,
    modules: Vec<(String, Arc<crate::ext::VTabImpl>)>,
    overlays: Vec<(String, Arc<crate::ext::VTabImpl>)>,
}

impl NativeExtensions {
    pub(crate) fn register(&self, db: &Database) -> Result<()> {
        let mut syms = db.builtin_syms.write();
        for function in &self.functions {
            syms.functions
                .insert(function.name.clone(), function.clone());
        }
        for (name, module) in &self.modules {
            syms.vtab_modules.insert(name.clone(), module.clone());
            if module.module_kind == VTabKind::TableValuedFunction {
                db.register_virtual_table(crate::vtab::VirtualTable::function(
                    db.dialect().as_ref(),
                    name,
                    &syms,
                )?)?;
            }
        }
        for (name, module) in &self.overlays {
            let overlay = crate::vtab::VirtualTable::overlay(db.dialect().as_ref(), name, module)?;
            if syms.table_overlays.insert(name.clone(), overlay).is_some() {
                return Err(crate::LimboError::InvalidArgument(format!(
                    "the table {name} has two overlays"
                )));
            }
        }
        Ok(())
    }
}

pub(crate) fn close_cursors(cursors: &mut [Option<Cursor>]) {
    for cursor in cursors {
        if matches!(cursor, Some(Cursor::Virtual(cursor)) if cursor.needs_close_at_done()) {
            *cursor = None;
        }
    }
}

pub(crate) fn abort_aggregates(registers: &mut [crate::Register]) {
    for register in registers.iter_mut() {
        if matches!(
            register,
            crate::Register::Aggregate(crate::types::AggContext::Native(_))
        ) {
            register.set_null();
        }
    }
}
