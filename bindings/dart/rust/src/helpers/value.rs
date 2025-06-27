pub enum Value {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl Into<limbo_core::Value> for Value {
    fn into(self) -> limbo_core::Value {
        match self {
            Value::Null => limbo_core::Value::Null,
            Value::Integer(n) => limbo_core::Value::Integer(n),
            Value::Real(n) => limbo_core::Value::Float(n),
            Value::Text(t) => limbo_core::Value::from_text(&t),
            Value::Blob(items) => limbo_core::Value::from_blob(items),
        }
    }
}
