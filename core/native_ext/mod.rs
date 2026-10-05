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
use crate::{Connection, LimboError, Result};

pub(crate) enum ExtensionState {
    None,
    ScalarCall(Box<dyn ScalarCall>),
    TableUpdate(Box<dyn TableUpdate>),
}

impl Connection {
    pub fn register_native_scalar<F: ScalarFunction + 'static>(
        &self,
        name: &str,
        argc: i32,
        deterministic: bool,
        function: F,
    ) -> Result<()> {
        validate_arg_count(argc)?;
        let name = crate::util::normalize_ident(name);
        self.syms.write().functions.insert(
            name.clone(),
            Arc::new(ExternalFunc {
                name,
                func: ExtFunc::NativeScalar {
                    argc,
                    deterministic,
                    function: Arc::new(function),
                },
            }),
        );
        self.bump_prepare_context_generation();
        Ok(())
    }

    pub fn register_native_aggregate<F: AggregateFunction + 'static>(
        &self,
        name: &str,
        argc: i32,
        function: F,
    ) -> Result<()> {
        validate_arg_count(argc)?;
        let name = crate::util::normalize_ident(name);
        self.syms.write().functions.insert(
            name.clone(),
            Arc::new(ExternalFunc {
                name,
                func: ExtFunc::NativeAggregate {
                    argc,
                    function: Arc::new(function),
                },
            }),
        );
        self.bump_prepare_context_generation();
        Ok(())
    }

    pub fn register_native_module<M: VirtualTableModule + 'static>(
        &self,
        name: &str,
        kind: VTabKind,
        module: M,
    ) -> Result<()> {
        let name = crate::util::normalize_ident(name);
        let module = Arc::new(crate::ext::VTabImpl {
            module_kind: kind,
            implementation: ModuleImplementation::Native(Arc::new(module)),
        });
        let table = if kind == VTabKind::TableValuedFunction {
            let (vtab_type, schema) = module.implementation.create(Vec::new())?;
            Some(Arc::new(crate::vtab::VirtualTable::new_native_function(
                name.clone(),
                schema,
                vtab_type,
            )?))
        } else {
            None
        };
        let mut syms = self.syms.write();
        if let Some(table) = table {
            let mut schema = self.schema.write();
            crate::schema::Schema::try_make_mut(&mut schema)?
                .tables
                .insert(name.clone(), Arc::new(crate::schema::Table::Virtual(table)));
        }
        syms.vtab_modules.insert(name, module);
        self.bump_prepare_context_generation();
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
