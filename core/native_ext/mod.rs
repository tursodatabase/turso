mod function;
mod vtab;

pub use function::{Aggregate, AggregateFunction, ScalarCall, ScalarFunction};
pub use turso_ext::{ConstraintInfo, ConstraintUsage, IndexInfo, OrderByInfo, VTabKind};
pub use vtab::{TableUpdate, VirtualTable, VirtualTableCursor, VirtualTableModule};

pub use function::AggregateState;
pub(crate) use function::{
    finalize_aggregate, step_aggregate, step_scalar, AggregateFactory, ScalarFactory,
};
pub(crate) use vtab::{ModuleFactory, NativeCursor, NativeTable};

use crate::function::ExternalFunc;
use crate::sync::Arc;
use crate::types::Cursor;
use crate::{Database, OpenOptions, Result};

pub(crate) enum ExtensionState {
    None,
    ScalarCall(Box<dyn ScalarCall>),
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
}

#[derive(Clone, Default)]
pub(crate) struct NativeExtensions {
    pub(crate) functions: Vec<Arc<ExternalFunc>>,
    modules: Vec<(String, Arc<crate::ext::VTabImpl>)>,
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
                db.register_virtual_table(crate::vtab::VirtualTable::function(name, &syms)?)?;
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
