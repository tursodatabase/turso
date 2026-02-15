use crate::{
    vector::vector_types::{Vector, VectorType},
    Result,
};

pub fn vector_convert(v: Vector, target_type: VectorType) -> Result<Vector> {
    if v.vector_type == target_type {
        return Ok(v);
    }
    match (v.vector_type, target_type) {
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
        // Float1Bit conversions
        (VectorType::Float32Dense, VectorType::Float1Bit) => {
            let dims = v.dims;
            let byte_count = dims.div_ceil(8);
            let mut bits = vec![0u8; byte_count];
            for (i, &val) in v.as_f32_slice().iter().enumerate() {
                if val > 0.0 {
                    bits[i / 8] |= 1 << (i & 7);
                }
            }
            Ok(Vector::from_1bit(dims, bits))
        }
        (VectorType::Float64Dense, VectorType::Float1Bit) => {
            let dims = v.dims;
            let byte_count = dims.div_ceil(8);
            let mut bits = vec![0u8; byte_count];
            for (i, &val) in v.as_f64_slice().iter().enumerate() {
                if val > 0.0 {
                    bits[i / 8] |= 1 << (i & 7);
                }
            }
            Ok(Vector::from_1bit(dims, bits))
        }
        (VectorType::Float1Bit, VectorType::Float32Dense) => {
            let data = v.as_1bit_data();
            let floats: Vec<f32> = (0..v.dims)
                .map(|i| {
                    if (data[i / 8] >> (i & 7)) & 1 == 1 {
                        1.0
                    } else {
                        -1.0
                    }
                })
                .collect();
            Ok(Vector::from_f32(floats))
        }
        (VectorType::Float1Bit, VectorType::Float64Dense) => {
            let data = v.as_1bit_data();
            let floats: Vec<f64> = (0..v.dims)
                .map(|i| {
                    if (data[i / 8] >> (i & 7)) & 1 == 1 {
                        1.0
                    } else {
                        -1.0
                    }
                })
                .collect();
            Ok(Vector::from_f64(floats))
        }
        // Float8 conversions
        (VectorType::Float32Dense, VectorType::Float8) => {
            convert_floats_to_f8(v.as_f32_slice().iter().copied(), v.dims)
        }
        (VectorType::Float64Dense, VectorType::Float8) => {
            convert_floats_to_f8(v.as_f64_slice().iter().map(|&x| x as f32), v.dims)
        }
        (VectorType::Float8, VectorType::Float32Dense) => {
            let (quantized, alpha, shift) = v.as_f8_data();
            let floats: Vec<f32> = quantized
                .iter()
                .map(|&q| alpha * q as f32 + shift)
                .collect();
            Ok(Vector::from_f32(floats))
        }
        (VectorType::Float8, VectorType::Float64Dense) => {
            let (quantized, alpha, shift) = v.as_f8_data();
            let floats: Vec<f64> = quantized
                .iter()
                .map(|&q| alpha as f64 * q as f64 + shift as f64)
                .collect();
            Ok(Vector::from_f64(floats))
        }
        // Cross-conversions via intermediate
        (VectorType::Float1Bit, VectorType::Float8) => {
            let f32_vec = vector_convert(v, VectorType::Float32Dense)?;
            vector_convert(f32_vec, VectorType::Float8)
        }
        (VectorType::Float8, VectorType::Float1Bit) => {
            let f32_vec = vector_convert(v, VectorType::Float32Dense)?;
            vector_convert(f32_vec, VectorType::Float1Bit)
        }
        (VectorType::Float1Bit, VectorType::Float32Sparse)
        | (VectorType::Float8, VectorType::Float32Sparse)
        | (VectorType::Float32Sparse, VectorType::Float1Bit)
        | (VectorType::Float32Sparse, VectorType::Float8) => {
            let f32_vec = vector_convert(v, VectorType::Float32Dense)?;
            vector_convert(f32_vec, target_type)
        }
        _ => unreachable!(
            "unexpected conversion: {:?} -> {:?}",
            v.vector_type, target_type
        ),
    }
}

fn convert_floats_to_f8(
    values: impl Iterator<Item = f32> + Clone,
    dims: usize,
) -> Result<Vector<'static>> {
    let mut min_val = f32::INFINITY;
    let mut max_val = f32::NEG_INFINITY;
    for val in values.clone() {
        if val < min_val {
            min_val = val;
        }
        if val > max_val {
            max_val = val;
        }
    }
    let alpha = (max_val - min_val) / 255.0;
    let shift = min_val;
    let mut quantized = Vec::with_capacity(dims);
    for val in values {
        let q = if alpha == 0.0 {
            0u8
        } else {
            let v = (val - shift) / alpha + 0.5;
            (v as i32).clamp(0, 255) as u8
        };
        quantized.push(q);
    }
    Ok(Vector::from_f8(dims, quantized, alpha, shift))
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
