use crate::{
    vector::vector_types::{Vector, VectorSparse, VectorType},
    LimboError, Result,
};
use simsimd::SpatialSimilarity;

pub fn vector_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
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
        VectorType::Float32Dense => Ok(vector_f32_distance_cos_simsimd(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        #[cfg(target_family = "wasm")]
        VectorType::Float32Dense => Ok(vector_f32_distance_cos_rust(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        #[cfg(not(target_family = "wasm"))]
        VectorType::Float64Dense => Ok(vector_f64_distance_cos_simsimd(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        #[cfg(target_family = "wasm")]
        VectorType::Float64Dense => Ok(vector_f64_distance_cos_rust(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        VectorType::Float32Sparse => Ok(vector_f32_sparse_distance_cos(
            v1.as_f32_sparse(),
            v2.as_f32_sparse(),
        )),
    }
}

#[allow(dead_code)]
fn vector_f32_distance_cos_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    f32::cosine(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD do not support WASM for now, so we have alternative implementation: https://github.com/ashvardanian/SimSIMD/issues/189
#[allow(dead_code)]
fn vector_f32_distance_cos_rust(v1: &[f32], v2: &[f32]) -> f64 {
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);
    for (a, b) in v1.iter().zip(v2.iter()) {
        dot += a * b;
        norm1 += a * a;
        norm2 += b * b;
    }
    if norm1 == 0.0 || norm2 == 0.0 {
        return 0.0;
    }
    (1.0 - dot / (norm1 * norm2).sqrt()) as f64
}

#[allow(dead_code)]
fn vector_f64_distance_cos_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    f64::cosine(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD do not support WASM for now, so we have alternative implementation: https://github.com/ashvardanian/SimSIMD/issues/189
#[allow(dead_code)]
fn vector_f64_distance_cos_rust(v1: &[f64], v2: &[f64]) -> f64 {
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);
    for (a, b) in v1.iter().zip(v2.iter()) {
        dot += a * b;
        norm1 += a * a;
        norm2 += b * b;
    }
    if norm1 == 0.0 || norm2 == 0.0 {
        return 0.0;
    }
    1.0 - dot / (norm1 * norm2).sqrt()
}

fn vector_f32_sparse_distance_cos(v1: VectorSparse<f32>, v2: VectorSparse<f32>) -> f64 {
    let mut v1_pos = 0;
    let mut v2_pos = 0;
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);
    while v1_pos < v1.idx.len() && v2_pos < v2.idx.len() {
        let e1 = v1.values[v1_pos];
        let e2 = v2.values[v2_pos];
        if v1.idx[v1_pos] == v2.idx[v2_pos] {
            dot += e1 * e2;
            norm1 += e1 * e1;
            norm2 += e2 * e2;
            v1_pos += 1;
            v2_pos += 1;
        } else if v1.idx[v1_pos] < v2.idx[v2_pos] {
            norm1 += e1 * e1;
            v1_pos += 1;
        } else {
            norm2 += e2 * e2;
            v2_pos += 1;
        }
    }

    while v1_pos < v1.idx.len() {
        norm1 += v1.values[v1_pos] * v1.values[v1_pos];
        v1_pos += 1;
    }
    while v2_pos < v2.idx.len() {
        norm2 += v2.values[v2_pos] * v2.values[v2_pos];
        v2_pos += 1;
    }

    // Check for zero norms
    if norm1 == 0.0f32 || norm2 == 0.0f32 {
        return f64::NAN;
    }

    (1.0f32 - (dot / (norm1 * norm2).sqrt())) as f64
}

#[cfg(test)]
mod tests {
    use crate::vector::{
        operations::convert::vector_convert, vector_types::tests::ArbitraryVector,
    };

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
        let v1 = vector_convert(v1.into(), VectorType::Float32Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float32Dense).unwrap();
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
        let v1 = vector_convert(v1.into(), VectorType::Float32Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float32Dense).unwrap();
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
        let v1 = vector_convert(v1.into(), VectorType::Float64Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float64Dense).unwrap();
        let d1 = vector_f64_distance_cos_rust(v1.as_f64_slice(), v2.as_f64_slice());
        let d2 = vector_f64_distance_cos_simsimd(v1.as_f64_slice(), v2.as_f64_slice());
        println!("d1 vs d2: {d1} vs {d2}");
        (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-6
    }
}
