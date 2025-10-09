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
            for &x in v.as_f64_slice() {
                data.extend_from_slice(&f32::to_le_bytes(x as f32));
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

#[cfg(test)]
mod tests {
    use crate::vector::{
        operations::convert::vector_convert,
        vector_types::{Vector, VectorType},
    };

    fn concat<const N: usize>(data: &[[u8; N]]) -> Vec<u8> {
        data.iter().flatten().cloned().collect::<Vec<u8>>()
    }

    fn assert_vectors(v1: &Vector, v2: &Vector) {
        assert_eq!(v1.vector_type, v2.vector_type);
        assert_eq!(v1.dims, v2.dims);
        assert_eq!(v1.data, v2.data);
    }

    #[test]
    pub fn test_vector_convert() {
        let vf32 = Vector {
            vector_type: VectorType::Float32Dense,
            dims: 3,
            data: concat(&[
                1.0f32.to_le_bytes(),
                0.0f32.to_le_bytes(),
                2.0f32.to_le_bytes(),
            ]),
        };
        let vf64 = Vector {
            vector_type: VectorType::Float64Dense,
            dims: 3,
            data: concat(&[
                1.0f64.to_le_bytes(),
                0.0f64.to_le_bytes(),
                2.0f64.to_le_bytes(),
            ]),
        };
        let vf32_sparse = Vector {
            vector_type: VectorType::Float32Sparse,
            dims: 3,
            data: concat(&[
                1.0f32.to_le_bytes(),
                2.0f32.to_le_bytes(),
                0u32.to_le_bytes(),
                2u32.to_le_bytes(),
            ]),
        };

        let vectors = [vf32, vf64, vf32_sparse];
        for v1 in &vectors {
            for v2 in &vectors {
                println!("{:?} -> {:?}", v1.vector_type, v2.vector_type);
                let v_copy = Vector {
                    vector_type: v1.vector_type,
                    dims: v1.dims,
                    data: v1.data.clone(),
                };
                assert_vectors(&vector_convert(v_copy, v2.vector_type).unwrap(), &v2);
            }
        }
    }
}
