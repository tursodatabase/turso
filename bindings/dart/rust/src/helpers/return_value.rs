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
            turso_core::Value::Blob(b) => ReturnValue::Blob(b),
            turso_core::Value::Null => ReturnValue::Null,
        }
    }
}
