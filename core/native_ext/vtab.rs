use super::{ConstraintInfo, ExtensionState, IndexInfo, OrderByInfo, VTabKind};
use crate::alloc::TryClone;
use crate::sync::{Arc, RwLock};
use crate::types::IOResultOr;
use crate::vtab::{ExtVirtualTable, VirtualTableType};
use crate::{Connection, IOResult, LimboError, Result, Value};
use std::fmt::Debug;
use turso_ext::{ResultCode, VTabModuleImpl};

pub trait VirtualTableModule: Debug + Send + Sync {
    type Table: VirtualTable + 'static;

    fn schema(&self, args: &[Value]) -> Result<String>;
    fn create(&self, args: &[Value]) -> Result<Self::Table>;
}

pub trait VirtualTable: Debug + Send + Sync {
    type Cursor: VirtualTableCursor + 'static;

    fn open(&self, connection: Arc<Connection>) -> Result<Self::Cursor>;
    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode>;
    fn readonly(&self) -> bool {
        true
    }
    fn create_update(&mut self, _args: Vec<Value>) -> Result<Box<dyn TableUpdate>> {
        Err(LimboError::ReadOnly)
    }
    fn begin(&mut self) -> Result<()> {
        Ok(())
    }
    fn commit(&mut self) -> Result<()> {
        Ok(())
    }
    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }
    fn rename(&mut self, _name: &str) -> Result<()> {
        Ok(())
    }
    fn destroy(&mut self) -> Result<()> {
        Ok(())
    }
}

pub trait VirtualTableCursor: Send + Sync {
    fn filter(&mut self, args: &[Value], idx_str: Option<&str>, idx_num: i32) -> IOResultOr<bool>;
    fn next(&mut self) -> IOResultOr<bool>;
    fn column(&mut self, column: usize) -> IOResultOr<Value>;
    fn rowid(&self) -> i64;
}

pub trait TableUpdate: Send + Sync {
    fn step(&mut self) -> IOResultOr<Option<i64>>;
}

#[derive(Clone, Debug)]
pub(crate) enum ModuleImplementation {
    C(Arc<VTabModuleImpl>),
    Native(Arc<dyn ModuleFactory>),
}

impl ModuleImplementation {
    pub(crate) fn create_schema(&self, args: Vec<turso_ext::Value>) -> Result<String> {
        match self {
            Self::C(module) => Ok(module.create_schema(args)?),
            Self::Native(module) => module.schema(&native_args(args)?),
        }
    }

    pub(crate) fn create(&self, args: Vec<turso_ext::Value>) -> Result<(VirtualTableType, String)> {
        match self {
            Self::C(module) => {
                let (table, schema) = ExtVirtualTable::create(module.clone(), args)?;
                Ok((VirtualTableType::External(table), schema))
            }
            Self::Native(module) => {
                let args = native_args(args)?;
                let schema = module.schema(&args)?;
                let table = module.create(&args)?;
                Ok((VirtualTableType::Native(table), schema))
            }
        }
    }
}

pub(crate) fn create_virtual_table(
    module_name: &str,
    module: Option<&Arc<crate::ext::VTabImpl>>,
    args: Vec<turso_ext::Value>,
    kind: VTabKind,
) -> Result<(VirtualTableType, String)> {
    let module = module.ok_or_else(|| {
        LimboError::ExtensionError(format!("Virtual table module not found: {module_name}"))
    })?;
    if kind != module.module_kind {
        let expected = match kind {
            VTabKind::VirtualTable => "virtual table",
            VTabKind::TableValuedFunction => "table-valued function",
        };
        return Err(LimboError::ExtensionError(format!(
            "{module_name} is not a {expected} module"
        )));
    }
    module.implementation.create(args)
}

impl crate::vtab::VirtualTable {
    pub(crate) fn new_native_function(
        name: String,
        schema: String,
        vtab_type: VirtualTableType,
    ) -> Result<Self> {
        Ok(Self {
            name,
            columns: Self::resolve_columns(schema)?,
            kind: VTabKind::TableValuedFunction,
            vtab_type,
            vtab_id: 0,
            is_droppable: false,
            innocuous: false,
        })
    }
}

fn native_args(args: Vec<turso_ext::Value>) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(args.len());
    let mut error = None;
    for arg in args {
        match Value::from_ffi(arg) {
            Ok(value) => values.push(value),
            Err(err) => {
                error.get_or_insert(err);
            }
        }
    }
    match error {
        Some(error) => Err(error),
        None => Ok(values),
    }
}

pub(crate) trait ModuleFactory: Debug + Send + Sync {
    fn schema(&self, args: &[Value]) -> Result<String>;
    fn create(&self, args: &[Value]) -> Result<NativeTable>;
}

impl<M: VirtualTableModule> ModuleFactory for M {
    fn schema(&self, args: &[Value]) -> Result<String> {
        VirtualTableModule::schema(self, args)
    }

    fn create(&self, args: &[Value]) -> Result<NativeTable> {
        Ok(NativeTable(Arc::new(RwLock::new(Box::new(
            VirtualTableModule::create(self, args)?,
        )))))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NativeTable(Arc<RwLock<Box<dyn TableInstance>>>);

impl NativeTable {
    pub(crate) fn readonly(&self) -> bool {
        self.0.read().readonly()
    }

    pub(crate) fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        self.0.read().best_index(constraints, order_by)
    }

    pub(crate) fn open(&self, connection: Arc<Connection>, vtab_id: u64) -> Result<NativeCursor> {
        Ok(NativeCursor {
            cursor: self.0.read().open(connection)?,
            vtab_id,
        })
    }

    pub(crate) fn update(
        &self,
        state: &mut ExtensionState,
        args: &[Value],
    ) -> IOResultOr<Option<i64>> {
        if matches!(state, ExtensionState::None) {
            let args = args
                .iter()
                .map(Value::try_clone)
                .collect::<Result<Vec<_>, _>>()?;
            *state = ExtensionState::TableUpdate(self.0.write().create_update(args)?);
        }
        let ExtensionState::TableUpdate(update) = state else {
            unreachable!("update instruction requires table update state");
        };
        let result = update.step();
        if !matches!(result, Ok(IOResult::IO(_))) {
            *state = ExtensionState::None;
        }
        result
    }

    pub(crate) fn begin(&self) -> Result<()> {
        self.0.write().begin()
    }

    pub(crate) fn commit(&self) -> Result<()> {
        self.0.write().commit()
    }

    pub(crate) fn rollback(&self) -> Result<()> {
        self.0.write().rollback()
    }

    pub(crate) fn rename(&self, name: &str) -> Result<()> {
        self.0.write().rename(name)
    }

    pub(crate) fn destroy(&self) -> Result<()> {
        self.0.write().destroy()
    }
}

trait TableInstance: Debug + Send + Sync {
    fn open(&self, connection: Arc<Connection>) -> Result<Box<dyn VirtualTableCursor>>;
    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode>;
    fn readonly(&self) -> bool;
    fn create_update(&mut self, args: Vec<Value>) -> Result<Box<dyn TableUpdate>>;
    fn begin(&mut self) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    fn rename(&mut self, name: &str) -> Result<()>;
    fn destroy(&mut self) -> Result<()>;
}

impl<T: VirtualTable> TableInstance for T {
    fn open(&self, connection: Arc<Connection>) -> Result<Box<dyn VirtualTableCursor>> {
        Ok(Box::new(VirtualTable::open(self, connection)?))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        VirtualTable::best_index(self, constraints, order_by)
    }

    fn readonly(&self) -> bool {
        VirtualTable::readonly(self)
    }

    fn create_update(&mut self, args: Vec<Value>) -> Result<Box<dyn TableUpdate>> {
        VirtualTable::create_update(self, args)
    }

    fn begin(&mut self) -> Result<()> {
        VirtualTable::begin(self)
    }

    fn commit(&mut self) -> Result<()> {
        VirtualTable::commit(self)
    }

    fn rollback(&mut self) -> Result<()> {
        VirtualTable::rollback(self)
    }

    fn rename(&mut self, name: &str) -> Result<()> {
        VirtualTable::rename(self, name)
    }

    fn destroy(&mut self) -> Result<()> {
        VirtualTable::destroy(self)
    }
}

pub(crate) struct NativeCursor {
    pub(crate) cursor: Box<dyn VirtualTableCursor>,
    pub(crate) vtab_id: u64,
}
