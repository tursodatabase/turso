use crate::{
    vector::vector_types::{Vector, VectorType},
    Result,
};

pub fn vector_convert(v: Vector, target_type: VectorType) -> Result<Vector> {
    match (v.vector_type, target_type) {
        (VectorType::Float32Dense, VectorType::Float32Dense)
        | (VectorType::Float64Dense, VectorType::Float64Dense)
        | (VectorType::Float32Sparse, VectorType::Float32Sparse) => Ok(v),
        (VectorType::Float32Dense, VectorType::Float64Dense) => {
            let mut data = Vec::with_capacity(v.dims * 8);
            for &x in v.as_f32_slice() {
                data.extend_from_slice(&f64::to_le_bytes(x as f64));
            }
            Ok(Vector {
                vector_type: target_type,
                dims: v.dims,
                data,
            })
        }
        (VectorType::Float64Dense, VectorType::Float32Dense) => {
            let mut data = Vec::with_capacity(v.dims * 4);
            for &x in v.as_f32_slice() {
                data.extend_from_slice(&f64::to_le_bytes(x as f64));
            }
            Ok(Vector {
                vector_type: target_type,
                dims: v.dims,
                data,
            })
        }
        (VectorType::Float32Dense, VectorType::Float32Sparse) => {
            let (mut idx, mut values) = (Vec::new(), Vec::new());
            for (i, &value) in v.as_f32_slice().iter().enumerate() {
                if value == 0.0 {
                    continue;
                }
                idx.extend_from_slice(&(i as u32).to_le_bytes());
                values.extend_from_slice(&value.to_le_bytes());
            }
            values.extend_from_slice(&idx);
            Ok(Vector {
                vector_type: target_type,
                dims: v.dims,
                data: values,
            })
        }
        (VectorType::Float64Dense, VectorType::Float32Sparse) => {
            let (mut idx, mut values) = (Vec::new(), Vec::new());
            for (i, &value) in v.as_f64_slice().iter().enumerate() {
                if value == 0.0 {
                    continue;
                }
                idx.extend_from_slice(&(i as u32).to_le_bytes());
                values.extend_from_slice(&(value as f32).to_le_bytes());
            }
            values.extend_from_slice(&idx);
            Ok(Vector {
                vector_type: target_type,
                dims: v.dims,
                data: values,
            })
        }
        (VectorType::Float32Sparse, VectorType::Float32Dense) => {
            let sparse = v.as_f32_sparse();
            let mut data = vec![0u8; v.dims * 4];
            for (&i, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                data.splice((4 * i) as usize..4 * (i + 1) as usize, value.to_le_bytes());
            }
            Ok(Vector {
                vector_type: target_type,
                dims: v.dims,
                data,
            })
        }
        (VectorType::Float32Sparse, VectorType::Float64Dense) => {
            let sparse = v.as_f32_sparse();
            let mut data = vec![0u8; v.dims * 8];
            for (&i, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                data.splice(
                    (8 * i) as usize..8 * (i + 1) as usize,
                    (value as f64).to_le_bytes(),
                );
            }
            Ok(Vector {
                vector_type: target_type,
                dims: v.dims,
                data,
            })
        }
    }
}
