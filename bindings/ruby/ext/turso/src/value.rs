use magnus::prelude::*;
use magnus::{Error, RHash, Ruby, TryConvert, Value};
use turso_sdk_kit::rsapi::TursoStatement;

pub fn bind_value(stmt: &mut TursoStatement, index: usize, value: Value) -> Result<(), Error> {
    use turso_core::Value as CoreValue;

    let ruby = Ruby::get().expect("Ruby not initialized");

    let core_value = if value.is_nil() {
        CoreValue::Null
    } else if value.is_kind_of(ruby.class_true_class()) {
        CoreValue::Integer(1)
    } else if value.is_kind_of(ruby.class_false_class()) {
        CoreValue::Integer(0)
    } else if let Ok(i) = i64::try_convert(value) {
        CoreValue::Integer(i)
    } else if let Ok(f) = f64::try_convert(value) {
        CoreValue::Float(f)
    } else if let Ok(s) = String::try_convert(value) {
        CoreValue::Text(s.into())
    } else if let Ok(bytes) = Vec::<u8>::try_convert(value) {
        CoreValue::Blob(bytes)
    } else {
        return Err(Error::new(
            ruby.exception_type_error(),
            format!("Cannot convert {:?} to SQL value", unsafe {
                value.classname()
            }),
        ));
    };

    stmt.bind_positional(index, core_value)
        .map_err(crate::errors::map_turso_error)
}

pub fn extract_row(stmt: &TursoStatement) -> Result<RHash, Error> {
    let ruby = Ruby::get().expect("Ruby not initialized");
    let hash = ruby.hash_new();
    let count = stmt.column_count();

    for i in 0..count {
        let name = stmt
            .column_name(i)
            .map_err(crate::errors::map_turso_error)?;
        let key = ruby.sym_new(name.as_ref());
        let val = turso_to_ruby(
            &ruby,
            stmt.row_value(i).map_err(crate::errors::map_turso_error)?,
        )?;
        hash.aset(key, val)?;
    }

    Ok(hash)
}

fn turso_to_ruby(ruby: &Ruby, value: turso_core::ValueRef) -> Result<Value, Error> {
    use turso_core::ValueRef;

    Ok(match value {
        ValueRef::Null => ruby.qnil().as_value(),
        ValueRef::Integer(i) => ruby.integer_from_i64(i).as_value(),
        ValueRef::Float(f) => ruby.float_from_f64(f).as_value(),
        ValueRef::Text(t) => ruby.str_new(t.as_str()).as_value(),
        ValueRef::Blob(b) => ruby.str_from_slice(b).as_value(),
    })
}
