use crate::{
    vector::vector_types::{Vector, VectorSparse, VectorType},
    LimboError, Result,
};
use simsimd::SpatialSimilarity;

pub fn vector_distance_l2(v1: &Vector, v2: &Vector) -> Result<f64> {
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
        VectorType::Float32Dense => Ok(vector_f32_distance_l2_simsimd(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        #[cfg(target_family = "wasm")]
        VectorType::Float32Dense => Ok(vector_f32_distance_l2_rust(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        #[cfg(not(target_family = "wasm"))]
        VectorType::Float64Dense => Ok(vector_f64_distance_l2_simsimd(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        #[cfg(target_family = "wasm")]
        VectorType::Float64Dense => Ok(vector_f64_distance_l2_rust(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        VectorType::Float32Sparse => Ok(vector_f32_sparse_distance_l2(
            v1.as_f32_sparse(),
            v2.as_f32_sparse(),
        )),
    }
}

#[allow(dead_code)]
fn vector_f32_distance_l2_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    f32::euclidean(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD do not support WASM for now, so we have alternative implementation: https://github.com/ashvardanian/SimSIMD/issues/189
#[allow(dead_code)]
fn vector_f32_distance_l2_rust(v1: &[f32], v2: &[f32]) -> f64 {
    let sum = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f32>() as f64;
    sum.sqrt()
}

#[allow(dead_code)]
fn vector_f64_distance_l2_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    f64::euclidean(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD do not support WASM for now, so we have alternative implementation: https://github.com/ashvardanian/SimSIMD/issues/189
#[allow(dead_code)]
fn vector_f64_distance_l2_rust(v1: &[f64], v2: &[f64]) -> f64 {
    let sum = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f64>();
    sum.sqrt()
}

fn vector_f32_sparse_distance_l2(v1: VectorSparse<f32>, v2: VectorSparse<f32>) -> f64 {
    let mut v1_pos = 0;
    let mut v2_pos = 0;
    let mut sum = 0.0;
    while v1_pos < v1.idx.len() && v2_pos < v2.idx.len() {
        if v1.idx[v1_pos] == v2.idx[v2_pos] {
            sum += (v1.values[v1_pos] - v2.values[v2_pos]).powi(2);
            v1_pos += 1;
            v2_pos += 1;
        } else if v1.idx[v1_pos] < v2.idx[v2_pos] {
            sum += v1.values[v1_pos].powi(2);
            v1_pos += 1;
        } else {
            sum += v2.values[v2_pos].powi(2);
            v2_pos += 1;
        }
    }
    while v1_pos < v1.idx.len() {
        sum += v1.values[v1_pos].powi(2);
        v1_pos += 1;
    }
    while v2_pos < v2.idx.len() {
        sum += v2.values[v2_pos].powi(2);
        v2_pos += 1;
    }
    (sum as f64).sqrt()
}

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use crate::vector::{
        operations::convert::vector_convert, vector_types::tests::ArbitraryVector,
    };

    use super::*;

    #[test]
    fn test_vector_distance_l2_f32_another() {
        let vectors = [
            (0..8).map(|x| x as f32).collect::<Vec<f32>>(),
            (1..9).map(|x| x as f32).collect::<Vec<f32>>(),
            (2..10).map(|x| x as f32).collect::<Vec<f32>>(),
            (3..11).map(|x| x as f32).collect::<Vec<f32>>(),
        ];
        let query = (2..10).map(|x| x as f32).collect::<Vec<f32>>();

        let expected: Vec<f64> = vec![
            32.0_f64.sqrt(),
            8.0_f64.sqrt(),
            0.0_f64.sqrt(),
            8.0_f64.sqrt(),
        ];
        let results = vectors
            .iter()
            .map(|v| vector_f32_distance_l2_rust(&query, v))
            .collect::<Vec<f64>>();
        assert_eq!(results, expected);
    }

    #[test]
    fn test_vector_distance_l2_odd_len() {
        let v = (0..5).map(|x| x as f32).collect::<Vec<f32>>();
        let query = (2..7).map(|x| x as f32).collect::<Vec<f32>>();
        assert_eq!(vector_f32_distance_l2_rust(&v, &query), 20.0_f64.sqrt());
    }

    #[test]
    fn test_vector_distance_l2_f32() {
        assert_eq!(vector_f32_distance_l2_rust(&[], &[]), 0.0);
        assert_eq!(
            vector_f32_distance_l2_rust(&[1.0, 2.0], &[0.0, 0.0]),
            (1f64 + 2f64 * 2f64).sqrt()
        );
        assert_eq!(vector_f32_distance_l2_rust(&[1.0, 2.0], &[1.0, 2.0]), 0.0);
        assert_eq!(
            vector_f32_distance_l2_rust(&[1.0, 2.0], &[-1.0, -2.0]),
            (2f64 * 2f64 + 4f64 * 4f64).sqrt()
        );
        assert_eq!(
            vector_f32_distance_l2_rust(&[1.0, 2.0], &[-2.0, 1.0]),
            (3f64 * 3f64 + 1f64 * 1f64).sqrt()
        );
    }

    #[test]
    fn test_vector_distance_l2_f64() {
        assert_eq!(vector_f64_distance_l2_rust(&[], &[]), 0.0);
        assert_eq!(
            vector_f64_distance_l2_rust(&[1.0, 2.0], &[0.0, 0.0]),
            (1f64 + 2f64 * 2f64).sqrt()
        );
        assert_eq!(vector_f64_distance_l2_rust(&[1.0, 2.0], &[1.0, 2.0]), 0.0);
        assert_eq!(
            vector_f64_distance_l2_rust(&[1.0, 2.0], &[-1.0, -2.0]),
            (2f64 * 2f64 + 4f64 * 4f64).sqrt()
        );
        assert_eq!(
            vector_f64_distance_l2_rust(&[1.0, 2.0], &[-2.0, 1.0]),
            (3f64 * 3f64 + 1f64 * 1f64).sqrt()
        );
    }

    #[test]
    fn test_vector_distance_l2_f32_sparse() {
        assert!(
            (vector_f32_sparse_distance_l2(
                VectorSparse {
                    idx: &[0, 1],
                    values: &[1.0, 2.0]
                },
                VectorSparse {
                    idx: &[1, 2],
                    values: &[1.0, 3.0]
                },
            ) - vector_f32_distance_l2_rust(&[1.0, 2.0, 0.0], &[0.0, 1.0, 3.0]))
            .abs()
                < 1e-7
        );
    }

    #[quickcheck]
    fn prop_vector_distance_l2_dense_vs_sparse(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(v1.into(), VectorType::Float32Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float32Dense).unwrap();
        let d1 = vector_distance_l2(&v1, &v2).unwrap();

        let sparse1 = vector_convert(v1, VectorType::Float32Sparse).unwrap();
        let sparse2 = vector_convert(v2, VectorType::Float32Sparse).unwrap();
        let d2 = vector_f32_sparse_distance_l2(sparse1.as_f32_sparse(), sparse2.as_f32_sparse());

        (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-6
    }

    #[quickcheck]
    fn prop_vector_distance_l2_rust_vs_simsimd_f32(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(v1.into(), VectorType::Float32Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float32Dense).unwrap();
        let d1 = vector_f32_distance_l2_rust(v1.as_f32_slice(), v2.as_f32_slice());
        let d2 = vector_f32_distance_l2_simsimd(v1.as_f32_slice(), v2.as_f32_slice());
        (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-4
    }

    #[quickcheck]
    fn prop_vector_distance_l2_rust_vs_simsimd_f64(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(v1.into(), VectorType::Float64Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float64Dense).unwrap();
        let d1 = vector_f64_distance_l2_rust(v1.as_f64_slice(), v2.as_f64_slice());
        let d2 = vector_f64_distance_l2_simsimd(v1.as_f64_slice(), v2.as_f64_slice());
        (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-6
    }
}
