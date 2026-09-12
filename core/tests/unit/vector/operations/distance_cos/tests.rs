use crate::vector::{operations::convert::vector_convert, vector_types::tests::ArbitraryVector};

use super::*;
use quickcheck_macros::quickcheck;

#[test]
fn test_vector_distance_cos_f32() {
    assert_eq!(vector_f32_distance_cos_simsimd(&[], &[]), 0.0);
    assert_eq!(
        vector_f32_distance_cos_simsimd(&[1.0, 2.0], &[0.0, 0.0]),
        1.0
    );
    assert!(vector_f32_distance_cos_simsimd(&[1.0, 2.0], &[1.0, 2.0]).abs() < 1e-6);
    assert!((vector_f32_distance_cos_simsimd(&[1.0, 2.0], &[-1.0, -2.0]) - 2.0).abs() < 1e-6);
    assert!((vector_f32_distance_cos_simsimd(&[1.0, 2.0], &[-2.0, 1.0]) - 1.0).abs() < 1e-6);
}

#[test]
fn test_vector_distance_cos_f64() {
    assert_eq!(vector_f64_distance_cos_simsimd(&[], &[]), 0.0);
    assert_eq!(
        vector_f64_distance_cos_simsimd(&[1.0, 2.0], &[0.0, 0.0]),
        1.0
    );
    assert!(vector_f64_distance_cos_simsimd(&[1.0, 2.0], &[1.0, 2.0]).abs() < 1e-6);
    assert!((vector_f64_distance_cos_simsimd(&[1.0, 2.0], &[-1.0, -2.0]) - 2.0).abs() < 1e-6);
    assert!((vector_f64_distance_cos_simsimd(&[1.0, 2.0], &[-2.0, 1.0]) - 1.0).abs() < 1e-6);
}

#[test]
fn test_vector_distance_cos_f32_rust_zero_vectors() {
    assert_eq!(vector_f32_distance_cos_rust(&[], &[]), 0.0);
    assert_eq!(vector_f32_distance_cos_rust(&[1.0, 2.0], &[0.0, 0.0]), 1.0);
}

#[test]
fn test_vector_distance_cos_f64_rust_zero_vectors() {
    assert_eq!(vector_f64_distance_cos_rust(&[], &[]), 0.0);
    assert_eq!(vector_f64_distance_cos_rust(&[1.0, 2.0], &[0.0, 0.0]), 1.0);
}

#[test]
fn test_vector_distance_cos_f32_sparse() {
    assert!(
        (vector_f32_sparse_distance_cos(
            VectorSparse {
                idx: &[0, 1],
                values: &[1.0, 2.0]
            },
            VectorSparse {
                idx: &[1, 2],
                values: &[1.0, 3.0]
            },
        ) - vector_f32_distance_cos_simsimd(&[1.0, 2.0, 0.0], &[0.0, 1.0, 3.0]))
        .abs()
            < 1e-7
    );
}

#[quickcheck]
fn prop_vector_distance_cos_dense_vs_sparse(
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
    let d1 = vector_distance_cos(&v1, &v2).unwrap();

    let sparse1 = vector_convert(v1, VectorType::Float32Sparse).unwrap();
    let sparse2 = vector_convert(v2, VectorType::Float32Sparse).unwrap();
    let d2 = vector_f32_sparse_distance_cos(sparse1.as_f32_sparse(), sparse2.as_f32_sparse());

    (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-6
}

#[quickcheck]
fn prop_vector_distance_cos_rust_vs_simsimd_f32(
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
    let d1 = vector_f32_distance_cos_rust(v1.as_f32_slice(), v2.as_f32_slice());
    let d2 = vector_f32_distance_cos_simsimd(v1.as_f32_slice(), v2.as_f32_slice());
    println!("d1 vs d2: {d1} vs {d2}");
    (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-4
}

#[quickcheck]
fn prop_vector_distance_cos_rust_vs_simsimd_f64(
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
    let d1 = vector_f64_distance_cos_rust(v1.as_f64_slice(), v2.as_f64_slice());
    let d2 = vector_f64_distance_cos_simsimd(v1.as_f64_slice(), v2.as_f64_slice());
    println!("d1 vs d2: {d1} vs {d2}");
    (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-6
}

/// Float8 optimized cosine distance matches dequantized Float32 cosine distance.
#[quickcheck]
fn prop_vector_distance_cos_f8_vs_dequantized(
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
    let d_f8 = vector_distance_cos(&v1_f8, &v2_f8).unwrap();
    let v1_deq = vector_convert(v1_f8, VectorType::Float32Dense).unwrap();
    let v2_deq = vector_convert(v2_f8, VectorType::Float32Dense).unwrap();
    let d_deq = vector_distance_cos(&v1_deq, &v2_deq).unwrap();
    (d_f8.is_nan() && d_deq.is_nan()) || (d_f8 - d_deq).abs() < 1e-4
}

/// Float1Bit cosine distance (hamming) matches dot-product relationship:
/// hamming = (dims + dot_distance) / 2
#[quickcheck]
fn prop_vector_distance_cos_1bit_dot_relationship(
    v1: ArbitraryVector<100>,
    v2: ArbitraryVector<100>,
) -> bool {
    use crate::vector::operations::distance_dot::vector_distance_dot;
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
    let cos = vector_distance_cos(&v1, &v2).unwrap();
    let dot = vector_distance_dot(&v1, &v2).unwrap();
    // hamming = cos, dot = -(dims - 2*hamming), so cos = (dims + dot) / 2
    (cos - (100.0 + dot) / 2.0).abs() < 1e-10
}
