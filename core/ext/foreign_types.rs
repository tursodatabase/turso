use limbo_ext::CustomTypeImpl;

use crate::types::{OwnedValue, OwnedValueType};
use std::{ffi::CString, fmt::Display, rc::Rc};

/// Because we don't want to add additional opcodes, types that are
/// defined in extensions will operate internally as functions.
#[derive(Clone, Debug)]
pub struct ForeignTypeFunc {
    pub ext_type: Rc<ForeignType>,
    pub op: ForeignTypeOp,
}

impl ForeignTypeFunc {
    pub fn new_insert_hook(ext_type: Rc<ForeignType>) -> Self {
        Self {
            ext_type,
            op: ForeignTypeOp::OnInsertHook,
        }
    }
}

impl Display for ForeignTypeFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self.op {
            ForeignTypeOp::OnInsertHook => "on_insert",
        };
        write!(f, "{}: {}", self.ext_type.name, kind)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ForeignTypeOp {
    OnInsertHook,
    // TODO: Sorting, comparison
}

#[derive(Default)]
pub struct TypeRegistry(Vec<(String, Rc<ForeignType>)>);
impl TypeRegistry {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn register(&mut self, name: &str, ctx: *const CustomTypeImpl, _type: OwnedValueType) {
        self.0
            .push((name.to_string(), ForeignType::new(name, ctx, _type).into()));
    }

    pub fn get(&self, name: &str) -> Option<Rc<ForeignType>> {
        self.0
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, t)| t.clone())
    }
}

#[derive(Clone, Debug)]
pub struct ForeignType {
    pub name: String,
    ctx: Box<CustomTypeImpl>,
    _type: OwnedValueType,
}

impl ForeignType {
    pub fn new(name: &str, ctx: *const CustomTypeImpl, type_of: OwnedValueType) -> Self {
        Self {
            name: name.to_string(),
            ctx: unsafe { Box::from_raw(ctx as *mut CustomTypeImpl) },
            _type: type_of,
        }
    }

    pub fn type_of(&self) -> OwnedValueType {
        self._type
    }

    pub fn on_insert(
        &self,
        column_name: Option<&str>,
        insert_val: Option<OwnedValue>,
    ) -> crate::Result<OwnedValue> {
        let ctx = &*self.ctx as *const limbo_ext::CustomTypeImpl;
        let col = if let Ok(col_name_cstr) = CString::new(column_name.unwrap_or("")) {
            col_name_cstr
        } else {
            CString::new("").unwrap()
        };
        let val = insert_val.unwrap_or(OwnedValue::Null).to_ffi();
        let value = unsafe { ((*ctx).on_insert)(col.as_ptr(), &val as *const limbo_ext::Value) };
        unsafe {
            val.__free_internal_type();
        }
        OwnedValue::from_ffi(value)
    }
}
