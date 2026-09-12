use crate::{
    alloc::{TursoIteratorExt, ALLOC_ERR_MSG},
    types::value_blob_from_slice,
    vector::{
        operations::convert::vector_convert,
        vector_types::{tests::ArbitraryVector, Vector, VectorType},
    },
    ValueBlob,
};
use quickcheck_macros::quickcheck;

fn concat<const N: usize>(data: &[[u8; N]]) -> ValueBlob {
    data.iter()
        .flatten()
        .cloned()
        .try_collect()
        .expect(ALLOC_ERR_MSG)
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
        owned: Some(value_blob_from_slice(v.bin_data()).expect(ALLOC_ERR_MSG)),
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
    let source = Vector::from_f32(crate::alloc::vec![1.0, 0.5, 2.0]);

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
    let vf32 = Vector::from_f32(crate::alloc::vec![1.0, 0.0, 2.0]);

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
    let v_f32 = vector_convert(
        v.try_into().expect("generated vector must be valid"),
        VectorType::Float32Dense,
    )
    .unwrap();
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
    let v_f32 = vector_convert(
        v.try_into().expect("generated vector must be valid"),
        VectorType::Float32Dense,
    )
    .unwrap();
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
    let v: Vector = v.try_into().expect("generated vector must be valid");
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
    let empty_f32 = Vector::from_f32(crate::alloc::vec![]);
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
    let empty_f8 = Vector::from_f8(0, crate::alloc::vec![], 0.0, 0.0).unwrap();
    let f32_vec = vector_convert(empty_f8, VectorType::Float32Dense).unwrap();
    assert_eq!(f32_vec.dims, 0);
    assert!(f32_vec.as_f32_slice().is_empty());
}
