mod function;
mod vtab;

pub use function::{Aggregate, AggregateFunction, ScalarCall, ScalarFunction};
pub use turso_ext::{ConstraintInfo, ConstraintUsage, IndexInfo, OrderByInfo, VTabKind};
pub use vtab::{TableUpdate, VirtualTable, VirtualTableCursor, VirtualTableModule};

pub use function::AggregateState;
pub(crate) use function::{
    finalize_aggregate, step_aggregate, step_scalar, AggregateFactory, ScalarFactory,
};
pub(crate) use vtab::{create_virtual_table, ModuleImplementation, NativeCursor, NativeTable};

use crate::function::{ExtFunc, ExternalFunc};
use crate::sync::Arc;
use crate::types::Cursor;
use crate::{Database, LimboError, OpenOptions, Result};

pub(crate) enum ExtensionState {
    None,
    ScalarCall(Box<dyn ScalarCall>),
    TableUpdate(Box<dyn TableUpdate>),
}

impl OpenOptions {
    pub fn native_scalar<F: ScalarFunction + 'static>(
        mut self,
        name: &str,
        argc: i32,
        deterministic: bool,
        function: F,
    ) -> Result<Self> {
        validate_arg_count(argc)?;
        let name = crate::util::normalize_ident(name);
        self.native_extensions
            .functions
            .push(Arc::new(ExternalFunc {
                name,
                func: ExtFunc::NativeScalar {
                    argc,
                    deterministic,
                    function: Arc::new(function),
                },
            }));
        Ok(self)
    }

    pub fn native_aggregate<F: AggregateFunction + 'static>(
        mut self,
        name: &str,
        argc: i32,
        function: F,
    ) -> Result<Self> {
        validate_arg_count(argc)?;
        let name = crate::util::normalize_ident(name);
        self.native_extensions
            .functions
            .push(Arc::new(ExternalFunc {
                name,
                func: ExtFunc::NativeAggregate {
                    argc,
                    function: Arc::new(function),
                },
            }));
        Ok(self)
    }

    pub fn native_module<M: VirtualTableModule + 'static>(
        mut self,
        name: &str,
        kind: VTabKind,
        module: M,
    ) -> Self {
        let name = crate::util::normalize_ident(name);
        let module = Arc::new(crate::ext::VTabImpl {
            module_kind: kind,
            implementation: ModuleImplementation::Native(Arc::new(module)),
        });
        self.native_extensions.modules.push((name, module));
        self
    }
}

#[derive(Clone, Default)]
pub(crate) struct NativeExtensions {
    functions: Vec<Arc<ExternalFunc>>,
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
            if module.module_kind == VTabKind::TableValuedFunction {
                let (vtab_type, schema) = module.implementation.create(Vec::new())?;
                db.register_virtual_table(Arc::new(
                    crate::vtab::VirtualTable::new_native_function(
                        name.clone(),
                        schema,
                        vtab_type,
                    )?,
                ))?;
            }
            syms.vtab_modules.insert(name.clone(), module.clone());
        }
        Ok(())
    }
}

fn validate_arg_count(argc: i32) -> Result<()> {
    if argc < -1 {
        return Err(LimboError::InvalidArgument(
            "function argument count must be at least -1".into(),
        ));
    }
    Ok(())
}

pub(crate) fn close_cursors(cursors: &mut [Option<Cursor>]) {
    for cursor in cursors {
        if matches!(cursor, Some(Cursor::Virtual(cursor)) if cursor.is_native()) {
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

#[cfg(test)]
mod tests;
