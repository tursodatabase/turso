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
    if dims == 0 {
        return Ok(Vector::from_f8(0, Vec::new(), 0.0, 0.0));
    }
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
        vector_types::{tests::ArbitraryVector, Vector, VectorType},
    };
    use quickcheck_macros::quickcheck;

    fn concat<const N: usize>(data: &[[u8; N]]) -> Vec<u8> {
        data.iter().flatten().cloned().collect::<Vec<u8>>()
    }

    fn assert_vectors(v1: &Vector, v2: &Vector) {
        assert_eq!(v1.vector_type, v2.vector_type);
        assert_eq!(v1.dims, v2.dims);
        assert_eq!(v1.bin_data(), v2.bin_data());
    }

    fn clone_vector(v: &Vector) -> Vector<'static> {
        Vector {
            vector_type: v.vector_type,
            dims: v.dims,
            owned: Some(v.bin_data().to_vec()),
            refer: None,
        }
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

    /// Test that all 5x5 type conversions succeed and produce correct type/dims.
    #[test]
    pub fn test_vector_convert_all_types() {
        let source = Vector::from_f32(vec![1.0, 0.5, 2.0]);

        let all_types = [
            VectorType::Float32Dense,
            VectorType::Float64Dense,
            VectorType::Float32Sparse,
            VectorType::Float1Bit,
            VectorType::Float8,
        ];

        for &src_type in &all_types {
            let src = vector_convert(clone_vector(&source), src_type).unwrap();
            for &dst_type in &all_types {
                let result = vector_convert(clone_vector(&src), dst_type);
                assert!(
                    result.is_ok(),
                    "conversion {:?} -> {:?} failed: {:?}",
                    src_type,
                    dst_type,
                    result.err()
                );
                let converted = result.unwrap();
                assert_eq!(converted.vector_type, dst_type);
                assert_eq!(converted.dims, 3);
            }
        }
    }

    /// Lossless conversions roundtrip exactly.
    #[test]
    pub fn test_vector_convert_lossless_roundtrip() {
        let vf32 = Vector::from_f32(vec![1.0, 0.0, 2.0]);

        // f32 -> f64 -> f32 is exact
        let via_f64 = vector_convert(
            vector_convert(clone_vector(&vf32), VectorType::Float64Dense).unwrap(),
            VectorType::Float32Dense,
        )
        .unwrap();
        assert_eq!(vf32.bin_data(), via_f64.bin_data());

        // f32 -> sparse -> f32 is exact
        let via_sparse = vector_convert(
            vector_convert(clone_vector(&vf32), VectorType::Float32Sparse).unwrap(),
            VectorType::Float32Dense,
        )
        .unwrap();
        assert_eq!(vf32.bin_data(), via_sparse.bin_data());
    }

    /// Float1Bit roundtrip preserves sign information: positive → 1, non-positive → -1.
    #[quickcheck]
    fn prop_vector_convert_1bit_roundtrip(v: ArbitraryVector<100>) -> bool {
        let v_f32 = vector_convert(v.into(), VectorType::Float32Dense).unwrap();
        let orig_slice = v_f32.as_f32_slice().to_vec();
        let v_1bit = vector_convert(v_f32, VectorType::Float1Bit).unwrap();
        let v_back = vector_convert(v_1bit, VectorType::Float32Dense).unwrap();
        let back_slice = v_back.as_f32_slice();

        for i in 0..100 {
            let expected = if orig_slice[i] > 0.0 { 1.0f32 } else { -1.0f32 };
            if back_slice[i] != expected {
                return false;
            }
        }
        true
    }

    /// Float8 roundtrip approximately preserves values within one quantization step.
    #[quickcheck]
    fn prop_vector_convert_f8_roundtrip(v: ArbitraryVector<100>) -> bool {
        let v_f32 = vector_convert(v.into(), VectorType::Float32Dense).unwrap();
        let orig_slice = v_f32.as_f32_slice().to_vec();
        let v_f8 = vector_convert(v_f32, VectorType::Float8).unwrap();
        let v_back = vector_convert(v_f8, VectorType::Float32Dense).unwrap();
        let back_slice = v_back.as_f32_slice();

        let min_val = orig_slice.iter().cloned().fold(f32::INFINITY, f32::min);
        let max_val = orig_slice.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let alpha = (max_val - min_val) / 255.0;
        let tolerance = alpha + 1e-6;

        for i in 0..100 {
            if (orig_slice[i] - back_slice[i]).abs() > tolerance {
                return false;
            }
        }
        true
    }

    /// All type-pair conversions succeed for arbitrary vectors.
    #[quickcheck]
    fn prop_vector_convert_all_pairs(v: ArbitraryVector<16>) -> bool {
        let v: Vector = v.into();
        let all_types = [
            VectorType::Float32Dense,
            VectorType::Float64Dense,
            VectorType::Float32Sparse,
            VectorType::Float1Bit,
            VectorType::Float8,
        ];

        for &target_type in &all_types {
            if vector_convert(clone_vector(&v), target_type).is_err() {
                return false;
            }
        }
        true
    }

    #[test]
    fn test_vector_convert_empty_to_f8() {
        let empty_f32 = Vector::from_f32(vec![]);
        let f8 = vector_convert(empty_f32, VectorType::Float8).unwrap();
        assert_eq!(f8.dims, 0);
        assert_eq!(f8.vector_type, VectorType::Float8);
        let (quantized, alpha, shift) = f8.as_f8_data();
        assert!(quantized.is_empty());
        assert_eq!(alpha, 0.0);
        assert_eq!(shift, 0.0);
    }

    #[test]
    fn test_vector_convert_empty_f8_to_f32() {
        let empty_f8 = Vector::from_f8(0, Vec::new(), 0.0, 0.0);
        let f32_vec = vector_convert(empty_f8, VectorType::Float32Dense).unwrap();
        assert_eq!(f32_vec.dims, 0);
        assert!(f32_vec.as_f32_slice().is_empty());
    }
}
