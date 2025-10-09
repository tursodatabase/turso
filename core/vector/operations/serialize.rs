use crate::{
    vector::vector_types::{Vector, VectorType},
    Value,
};

pub fn vector_serialize(mut x: Vector) -> Value {
    match x.vector_type {
        VectorType::Float32Dense => Value::from_blob(x.data),
        VectorType::Float64Dense => {
            x.data.push(2);
            Value::from_blob(x.data)
        }
        VectorType::Float32Sparse => {
            x.data.extend_from_slice(&(x.dims as u32).to_le_bytes());
            x.data.push(9);
            Value::from_blob(x.data)
        }
    }
}
