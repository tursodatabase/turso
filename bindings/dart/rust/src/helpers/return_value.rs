pub enum ReturnValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl From<limbo_core::Value> for ReturnValue {
    fn from(value: limbo_core::Value) -> Self {
        match value {
            limbo_core::Value::Integer(i) => ReturnValue::Integer(i),
            limbo_core::Value::Float(f) => ReturnValue::Real(f),
            limbo_core::Value::Text(t) => ReturnValue::Text(t.as_str().to_string()),
            limbo_core::Value::Blob(b) => ReturnValue::Blob(b),
            limbo_core::Value::Null => ReturnValue::Null,
        }
    }
}
