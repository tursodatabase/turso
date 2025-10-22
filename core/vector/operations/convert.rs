use crate::{
    vector::vector_types::{Vector, VectorType},
    Result,
};

pub fn vector_convert(v: Vector, target_type: VectorType) -> Result<Vector> {
    match (v.vector_type, target_type) {
        (VectorType::Float32Dense, VectorType::Float32Dense)
        | (VectorType::Float64Dense, VectorType::Float64Dense)
        | (VectorType::Float32Sparse, VectorType::Float32Sparse) => Ok(v),
        (VectorType::Float32Dense, VectorType::Float64Dense) => Ok(Vector::from_f64(
            v.as_f32_slice().iter().map(|&x| x as f64).collect(),
        )),
        (VectorType::Float64Dense, VectorType::Float32Dense) => Ok(Vector::from_f32(
            v.as_f64_slice().iter().map(|&x| x as f32).collect(),
        )),
        (VectorType::Float32Dense, VectorType::Float32Sparse) => {
            let (mut idx, mut values) = (Vec::new(), Vec::new());
            for (i, &value) in v.as_f32_slice().iter().enumerate() {
                if value == 0.0 {
                    continue;
                }
                idx.push(i as u32);
                values.push(value);
            }
            Ok(Vector::from_f32_sparse(v.dims, values, idx))
        }
        (VectorType::Float64Dense, VectorType::Float32Sparse) => {
            let (mut idx, mut values) = (Vec::new(), Vec::new());
            for (i, &value) in v.as_f64_slice().iter().enumerate() {
                if value == 0.0 {
                    continue;
                }
                idx.push(i as u32);
                values.push(value as f32);
            }
            Ok(Vector::from_f32_sparse(v.dims, values, idx))
        }
        (VectorType::Float32Sparse, VectorType::Float32Dense) => {
            let sparse = v.as_f32_sparse();
            let mut data = vec![0f32; v.dims];
            for (&i, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                data[i as usize] = value;
            }
            Ok(Vector::from_f32(data))
        }
        (VectorType::Float32Sparse, VectorType::Float64Dense) => {
            let sparse = v.as_f32_sparse();
            let mut data = vec![0f64; v.dims];
            for (&i, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                data[i as usize] = value as f64;
            }
            Ok(Vector::from_f64(data))
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
        assert_eq!(v1.bin_data(), v2.bin_data());
    }

    #[test]
    pub fn test_vector_convert() {
        let vf32 = Vector {
            vector_type: VectorType::Float32Dense,
            dims: 3,
            owned: Some(concat(&[
                1.0f32.to_le_bytes(),
                0.0f32.to_le_bytes(),
                2.0f32.to_le_bytes(),
            ])),
            refer: None,
        };
        let vf64 = Vector {
            vector_type: VectorType::Float64Dense,
            dims: 3,
            owned: Some(concat(&[
                1.0f64.to_le_bytes(),
                0.0f64.to_le_bytes(),
                2.0f64.to_le_bytes(),
            ])),
            refer: None,
        };
        let vf32_sparse = Vector {
            vector_type: VectorType::Float32Sparse,
            dims: 3,
            owned: Some(concat(&[
                1.0f32.to_le_bytes(),
                2.0f32.to_le_bytes(),
                0u32.to_le_bytes(),
                2u32.to_le_bytes(),
            ])),
            refer: None,
        };

        let vectors = [vf32, vf64, vf32_sparse];
        for v1 in &vectors {
            for v2 in &vectors {
                println!("{:?} -> {:?}", v1.vector_type, v2.vector_type);
                let v_copy = Vector {
                    vector_type: v1.vector_type,
                    dims: v1.dims,
                    owned: v1.owned.clone(),
                    refer: None,
                };
                assert_vectors(&vector_convert(v_copy, v2.vector_type).unwrap(), v2);
            }
        }
    }
}
