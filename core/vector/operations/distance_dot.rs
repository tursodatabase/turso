use crate::{
    vector::vector_types::{Vector, VectorSparse, VectorType},
    LimboError, Result,
};
use simsimd::SpatialSimilarity;

pub fn vector_distance_dot(v1: &Vector, v2: &Vector) -> Result<f64> {
    if v1.dims != v2.dims {
        return Err(LimboError::ConversionError(
            "Vectors must have the same dimensions".to_string(),
        ));
    }
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Vectors must be of the same type".to_string(),
        ));
    }
    match v1.vector_type {
        #[cfg(not(target_family = "wasm"))]
        VectorType::Float32Dense => Ok(vector_f32_distance_dot_simsimd(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        #[cfg(target_family = "wasm")]
        VectorType::Float32Dense => Ok(vector_f32_distance_dot_rust(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        #[cfg(not(target_family = "wasm"))]
        VectorType::Float64Dense => Ok(vector_f64_distance_dot_simsimd(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        #[cfg(target_family = "wasm")]
        VectorType::Float64Dense => Ok(vector_f64_distance_dot_rust(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        VectorType::Float32Sparse => Ok(vector_f32_sparse_distance_dot(
            v1.as_f32_sparse(),
            v2.as_f32_sparse(),
        )),
    }
}

#[allow(dead_code)]
fn vector_f32_distance_dot_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    -f32::dot(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD do not support WASM for now, so we have alternative implementation: https://github.com/ashvardanian/SimSIMD/issues/189
#[allow(dead_code)]
fn vector_f32_distance_dot_rust(v1: &[f32], v2: &[f32]) -> f64 {
    let mut dot = 0.0;
    for (a, b) in v1.iter().zip(v2.iter()) {
        dot += (*a as f64) * (*b as f64);
    }
    -dot
}

#[allow(dead_code)]
fn vector_f64_distance_dot_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    -f64::dot(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD do not support WASM for now, so we have alternative implementation: https://github.com/ashvardanian/SimSIMD/issues/189
#[allow(dead_code)]
fn vector_f64_distance_dot_rust(v1: &[f64], v2: &[f64]) -> f64 {
    let mut dot = 0.0;
    for (a, b) in v1.iter().zip(v2.iter()) {
        dot += *a * *b;
    }
    -dot
}

fn vector_f32_sparse_distance_dot(v1: VectorSparse<f32>, v2: VectorSparse<f32>) -> f64 {
    let mut v1_pos = 0;
    let mut v2_pos = 0;
    let mut dot = 0.0;
    while v1_pos < v1.idx.len() && v2_pos < v2.idx.len() {
        let idx1 = v1.idx[v1_pos];
        let idx2 = v2.idx[v2_pos];
        if idx1 == idx2 {
            let e1 = v1.values[v1_pos];
            let e2 = v2.values[v2_pos];
            dot += (e1 as f64) * (e2 as f64);
            v1_pos += 1;
            v2_pos += 1;
        } else if idx1 < idx2 {
            v1_pos += 1;
        } else {
            v2_pos += 1;
        }
    }
    -dot
}

#[cfg(test)]
mod tests {
    use crate::vector::{
        operations::convert::vector_convert, vector_types::tests::ArbitraryVector,
    };

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
        let v1_dense = vector_convert(v1.into(), VectorType::Float32Dense).unwrap();
        let v2_dense = vector_convert(v2.into(), VectorType::Float32Dense).unwrap();
        let d_dense =
            vector_f32_distance_dot_rust(v1_dense.as_f32_slice(), v2_dense.as_f32_slice());
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
        let v1 = vector_convert(v1.into(), VectorType::Float32Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float32Dense).unwrap();
        let d_rust = vector_f32_distance_dot_rust(v1.as_f32_slice(), v2.as_f32_slice());
        let d_simd = vector_f32_distance_dot_simsimd(v1.as_f32_slice(), v2.as_f32_slice());
        (d_rust.is_nan() && d_simd.is_nan()) || (d_rust - d_simd).abs() < 1e-4
    }

    #[quickcheck]
    fn prop_vector_distance_dot_rust_vs_simsimd_f64(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(v1.into(), VectorType::Float64Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float64Dense).unwrap();
        let d_rust = vector_f64_distance_dot_rust(v1.as_f64_slice(), v2.as_f64_slice());
        let d_simd = vector_f64_distance_dot_simsimd(v1.as_f64_slice(), v2.as_f64_slice());
        (d_rust.is_nan() && d_simd.is_nan()) || (d_rust - d_simd).abs() < 1e-6
    }
}
