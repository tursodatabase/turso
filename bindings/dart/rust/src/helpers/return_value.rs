pub enum ReturnValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl From<turso_core::Value> for ReturnValue {
    fn from(value: turso_core::Value) -> Self {
        match value {
            turso_core::Value::Integer(i) => ReturnValue::Integer(i),
            turso_core::Value::Float(f) => ReturnValue::Real(f),
            turso_core::Value::Text(t) => ReturnValue::Text(t.as_str().to_string()),
            turso_core::Value::Blob(b) => {
                if b.unalloc_bytes > 0 {
                    panic!("ReturnValue from conversion called on unexpanded zeroblob with {} unallocated bytes", b.unalloc_bytes);
                }
                ReturnValue::Blob(b.value)
            }
            turso_core::Value::Null => ReturnValue::Null,
        }
    }
}
