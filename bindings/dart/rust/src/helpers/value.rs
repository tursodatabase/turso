pub enum Value {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl Into<turso_core::Value> for Value {
    fn into(self) -> turso_core::Value {
        match self {
            Value::Null => turso_core::Value::Null,
            Value::Integer(n) => turso_core::Value::Integer(n),
            Value::Real(n) => turso_core::Value::Float(n),
            Value::Text(t) => turso_core::Value::from_text(&t),
            Value::Blob(items) => turso_core::Value::from_blob(items),
        }
    }
}
