use limbo_ext::CustomTypeImpl;

use crate::types::{OwnedValue, OwnedValueType};
use std::{ffi::CString, fmt::Display, rc::Rc};

/// Because we don't want to add additional opcodes, types that are
/// defined in extensions will operate internally as functions.
#[derive(Clone, Debug)]
pub struct TsFunc {
    pub ext_type: Rc<ExternType>,
    pub op: TsFuncOp,
}

impl TsFunc {
    pub fn new(ext_type: Rc<ExternType>, op: TsFuncOp) -> Self {
        Self { ext_type, op }
    }
}

impl Display for TsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self.op {
            TsFuncOp::Generate => "generate",
        };
        write!(f, "{}: {}", self.ext_type.name, kind)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TsFuncOp {
    Generate,
    // TODO
}

#[derive(Default)]
pub struct TypeRegistry(Vec<(String, Rc<ExternType>)>);
impl TypeRegistry {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn register(&mut self, name: &str, ctx: *const CustomTypeImpl, _type: OwnedValueType) {
        self.0
            .push((name.to_string(), ExternType::new(name, ctx, _type).into()));
    }

    pub fn get(&self, name: &str) -> Option<Rc<ExternType>> {
        self.0
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, t)| t.clone())
    }
}

#[derive(Clone, Debug)]
pub struct ExternType {
    pub name: String,
    ctx: Box<CustomTypeImpl>,
    _type: OwnedValueType,
}

impl ExternType {
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

    pub fn generate(
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
        let value = unsafe { ((*ctx).generate)(col.as_ptr(), &val as *const limbo_ext::Value) };
        unsafe {
            val.free();
        }
        OwnedValue::from_ffi(value)
    }
}
