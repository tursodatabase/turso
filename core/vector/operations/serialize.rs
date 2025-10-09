use crate::{
    vector::vector_types::{Vector, VectorType},
    Value,
};

pub fn vector_serialize(x: Vector) -> Value {
    match x.vector_type {
        VectorType::Float32Dense => vector_f32_serialize(x),
        VectorType::Float64Dense => vector_f64_serialize(x),
        _ => todo!(),
    }
}

fn vector_f64_serialize(x: Vector) -> Value {
    let mut blob = Vec::with_capacity(x.dims * 8 + 1);
    blob.extend_from_slice(&x.data);
    blob.push(2);
    Value::from_blob(blob)
}

fn vector_f32_serialize(x: Vector) -> Value {
    Value::from_blob(x.data)
}
