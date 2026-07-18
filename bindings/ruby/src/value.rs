use magnus::{
    encoding::{EncodingCapable, RbEncoding},
    Error, Float, Integer, IntoValue, Ruby, RString, Value,
};
use turso_core::types::Text;
use turso_core::{NonNan, Numeric, Value as TursoValue};

pub fn to_turso_value(ruby: &Ruby, value: Value) -> Result<TursoValue, Error> {
    if value.is_nil() {
        return Ok(TursoValue::Null);
    }

    if let Some(i) = Integer::from_value(value) {
        let n = i.to_i64();
        return Ok(TursoValue::Numeric(Numeric::Integer(n)));
    }

    if let Some(f) = Float::from_value(value) {
        let nn =
            NonNan::new(f.to_f64()).ok_or_else(|| Error::new(ruby.exception_type_error(), "NaN"))?;
        return Ok(TursoValue::Numeric(Numeric::Float(nn)));
    }

    if let Some(s) = RString::from_value(value) {
        let encoding = s.enc_get();
        let bytes = unsafe { s.as_slice() };
        if encoding == ruby.ascii8bit_encoding() {
            return Ok(TursoValue::Blob(bytes.to_vec()));
        } else {
            let str = unsafe { s.to_str() }?;
            return Ok(TursoValue::Text(Text::new(str.to_string())));
        }
    }

    Err(Error::new(
        ruby.exception_type_error(),
        format!("cannot convert {} to Turso value", unsafe {
            value.classname()
        }),
    ))
}

pub fn to_ruby_value(ruby: &Ruby, value: &TursoValue) -> Result<Value, Error> {
    match value {
        TursoValue::Null => Ok(ruby.qnil().as_value()),
        TursoValue::Numeric(Numeric::Integer(i)) => {
            Ok(ruby.integer_from_i64(*i).into_value_with(ruby))
        }
        TursoValue::Numeric(Numeric::Float(f)) => {
            Ok(ruby.float_from_f64(f64::from(*f)).into_value_with(ruby))
        }
        TursoValue::Text(s) => Ok(ruby.str_new(s.as_str()).into_value_with(ruby)),
        TursoValue::Blob(b) => {
            let s = RString::buf_new(b.len());
            unsafe {
                s.append(b.as_slice())?;
                s.enc_set(ruby.ascii8bit_encoding())?;
            }
            Ok(s.into_value_with(ruby))
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_module_compiles() {
        // Verifies that the module compiles correctly.
        // Runtime tests require magnus's `embed` feature.
    }
}
