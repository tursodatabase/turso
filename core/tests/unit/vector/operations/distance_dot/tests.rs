use crate::vector::{operations::convert::vector_convert, vector_types::tests::ArbitraryVector};

use super::*;
use quickcheck_macros::quickcheck;

#[test]
fn test_vector_distance_dot_f32() {
    assert_eq!(vector_f32_distance_dot_simsimd(&[], &[]), 0.0);
    assert_eq!(
        vector_f32_distance_dot_simsimd(&[1.0, 0.0], &[0.0, 1.0]),
        0.0
    );
    assert!((vector_f32_distance_dot_simsimd(&[1.0, 2.0], &[1.0, 2.0]) - (-5.0)).abs() < 1e-6);
    assert!((vector_f32_distance_dot_simsimd(&[1.0, 2.0], &[2.0, 4.0]) - (-10.0)).abs() < 1e-6);
    assert!((vector_f32_distance_dot_simsimd(&[1.0, 2.0], &[-1.0, -2.0]) - 5.0).abs() < 1e-6);
}

#[test]
fn test_vector_distance_dot_f64() {
    assert_eq!(vector_f64_distance_dot_simsimd(&[], &[]), 0.0);
    assert_eq!(
        vector_f64_distance_dot_simsimd(&[1.0, 0.0], &[0.0, 1.0]),
        0.0
    );
    assert!((vector_f64_distance_dot_simsimd(&[1.0, 2.0], &[1.0, 2.0]) - (-5.0)).abs() < 1e-6);
    assert!((vector_f64_distance_dot_simsimd(&[1.0, 2.0], &[-1.0, -2.0]) - 5.0).abs() < 1e-6);
}

#[test]
fn test_vector_distance_dot_f32_sparse() {
    let v1_sparse = VectorSparse {
        idx: &[1, 2],
        values: &[1.0, 2.0],
    };
    let v2_sparse = VectorSparse {
        idx: &[1, 3],
        values: &[2.0, 3.0],
    };
    let v1_dense = &[0.0, 1.0, 2.0, 0.0];
    let v2_dense = &[0.0, 2.0, 0.0, 3.0];
    let sparse_dist = vector_f32_sparse_distance_dot(v1_sparse, v2_sparse);
    let dense_dist = vector_f32_distance_dot_simsimd(v1_dense, v2_dense);
    assert!((sparse_dist - dense_dist).abs() < 1e-7);
    assert!((sparse_dist - (-2.0)).abs() < 1e-7);
}

#[quickcheck]
fn prop_vector_distance_dot_dense_vs_sparse(
    v1: ArbitraryVector<100>,
    v2: ArbitraryVector<100>,
) -> bool {
    let v1_dense = vector_convert(
        v1.try_into().expect("generated vector must be valid"),
        VectorType::Float32Dense,
    )
    .unwrap();
    let v2_dense = vector_convert(
        v2.try_into().expect("generated vector must be valid"),
        VectorType::Float32Dense,
    )
    .unwrap();
    let d_dense = vector_f32_distance_dot_rust(v1_dense.as_f32_slice(), v2_dense.as_f32_slice());
    let v1_sparse = vector_convert(v1_dense, VectorType::Float32Sparse).unwrap();
    let v2_sparse = vector_convert(v2_dense, VectorType::Float32Sparse).unwrap();
    let d_sparse =
        vector_f32_sparse_distance_dot(v1_sparse.as_f32_sparse(), v2_sparse.as_f32_sparse());
    (d_dense.is_nan() && d_sparse.is_nan()) || (d_dense - d_sparse).abs() < 1e-5
}

#[quickcheck]
fn prop_vector_distance_dot_rust_vs_simsimd_f32(
    v1: ArbitraryVector<100>,
    v2: ArbitraryVector<100>,
) -> bool {
    let v1 = vector_convert(
        v1.try_into().expect("generated vector must be valid"),
        VectorType::Float32Dense,
    )
    .unwrap();
    let v2 = vector_convert(
        v2.try_into().expect("generated vector must be valid"),
        VectorType::Float32Dense,
    )
    .unwrap();
    let d_rust = vector_f32_distance_dot_rust(v1.as_f32_slice(), v2.as_f32_slice());
    let d_simd = vector_f32_distance_dot_simsimd(v1.as_f32_slice(), v2.as_f32_slice());
    (d_rust.is_nan() && d_simd.is_nan()) || (d_rust - d_simd).abs() < 1e-4
}

#[quickcheck]
fn prop_vector_distance_dot_rust_vs_simsimd_f64(
    v1: ArbitraryVector<100>,
    v2: ArbitraryVector<100>,
) -> bool {
    let v1 = vector_convert(
        v1.try_into().expect("generated vector must be valid"),
        VectorType::Float64Dense,
    )
    .unwrap();
    let v2 = vector_convert(
        v2.try_into().expect("generated vector must be valid"),
        VectorType::Float64Dense,
    )
    .unwrap();
    let d_rust = vector_f64_distance_dot_rust(v1.as_f64_slice(), v2.as_f64_slice());
    let d_simd = vector_f64_distance_dot_simsimd(v1.as_f64_slice(), v2.as_f64_slice());
    (d_rust.is_nan() && d_simd.is_nan()) || (d_rust - d_simd).abs() < 1e-6
}

/// Float8 optimized dot distance matches dequantized Float32 dot distance.
#[quickcheck]
fn prop_vector_distance_dot_f8_vs_dequantized(
    v1: ArbitraryVector<100>,
    v2: ArbitraryVector<100>,
) -> bool {
    let v1 = vector_convert(
        v1.try_into().expect("generated vector must be valid"),
        VectorType::Float32Dense,
    )
    .unwrap();
    let v2 = vector_convert(
        v2.try_into().expect("generated vector must be valid"),
        VectorType::Float32Dense,
    )
    .unwrap();
    let v1_f8 = vector_convert(v1, VectorType::Float8).unwrap();
    let v2_f8 = vector_convert(v2, VectorType::Float8).unwrap();
    let d_f8 = vector_distance_dot(&v1_f8, &v2_f8).unwrap();
    let v1_deq = vector_convert(v1_f8, VectorType::Float32Dense).unwrap();
    let v2_deq = vector_convert(v2_f8, VectorType::Float32Dense).unwrap();
    let d_deq = vector_distance_dot(&v1_deq, &v2_deq).unwrap();
    (d_f8.is_nan() && d_deq.is_nan()) || (d_f8 - d_deq).abs() < 1e-3
}

/// Float1Bit dot distance matches dequantized ±1 Float32 dot distance.
#[quickcheck]
fn prop_vector_distance_dot_1bit_vs_dequantized(
    v1: ArbitraryVector<100>,
    v2: ArbitraryVector<100>,
) -> bool {
    let v1 = vector_convert(
        v1.try_into().expect("generated vector must be valid"),
        VectorType::Float1Bit,
    )
    .unwrap();
    let v2 = vector_convert(
        v2.try_into().expect("generated vector must be valid"),
        VectorType::Float1Bit,
    )
    .unwrap();
    let d_1bit = vector_distance_dot(&v1, &v2).unwrap();
    let v1_f32 = vector_convert(v1, VectorType::Float32Dense).unwrap();
    let v2_f32 = vector_convert(v2, VectorType::Float32Dense).unwrap();
    let d_f32 = vector_distance_dot(&v1_f32, &v2_f32).unwrap();
    (d_1bit - d_f32).abs() < 1e-6
}
