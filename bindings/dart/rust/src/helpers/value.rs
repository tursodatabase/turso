pub enum Value {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl From<Value> for turso_core::Value {
    fn from(val: Value) -> Self {
        match val {
            Value::Null => turso_core::Value::Null,
            Value::Integer(n) => turso_core::Value::Integer(n),
            Value::Real(n) => turso_core::Value::Float(n),
            Value::Text(t) => turso_core::Value::from_text(&t),
            Value::Blob(items) => turso_core::Value::build_blob(items),
        }
    }
}
