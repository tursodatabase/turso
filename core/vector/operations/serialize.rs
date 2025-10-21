use crate::{
    vector::vector_types::{Vector, VectorType},
    Value,
};

pub fn vector_serialize(x: Vector) -> Value {
    match x.vector_type {
        VectorType::Float32Dense => Value::from_blob(x.bin_eject()),
        VectorType::Float64Dense => {
            let mut data = x.bin_eject();
            data.push(2);
            Value::from_blob(data)
        }
        VectorType::Float32Sparse => {
            let dims = x.dims;
            let mut data = x.bin_eject();
            data.extend_from_slice(&(dims as u32).to_le_bytes());
            data.push(9);
            Value::from_blob(data)
        }
    }
}
