use crate::{
    vector::vector_types::{Vector, VectorType},
    Result,
};

pub fn vector_convert(v: Vector, target_type: VectorType) -> Result<Vector> {
    match (v.vector_type, target_type) {
        (VectorType::Float32Dense, VectorType::Float32Dense)
        | (VectorType::Float64Dense, VectorType::Float64Dense) => Ok(v),
        (VectorType::Float32Dense, VectorType::Float64Dense) => {
            let mut data = Vec::with_capacity(v.dims * 8);
            for &x in v.as_f32_slice() {
                data.extend_from_slice(&f64::to_le_bytes(x as f64));
            }
            Vector::from_data(target_type, data)
        }
        (VectorType::Float64Dense, VectorType::Float32Dense) => {
            let mut data = Vec::with_capacity(v.dims * 4);
            for &x in v.as_f32_slice() {
                data.extend_from_slice(&f64::to_le_bytes(x as f64));
            }
            Vector::from_data(target_type, data)
        }
        _ => todo!(),
    }
}
