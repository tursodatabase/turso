use crate::{
    vector::vector_types::{Vector, VectorSparse, VectorType},
    LimboError, Result,
};

pub fn vector_distance_jaccard(v1: &Vector, v2: &Vector) -> Result<f64> {
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
        VectorType::Float32Dense => Ok(vector_f32_distance_jaccard(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        VectorType::Float64Dense => Ok(vector_f64_distance_jaccard(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        VectorType::Float32Sparse => Ok(vector_f32_sparse_distance_jaccard(
            v1.as_f32_sparse(),
            v2.as_f32_sparse(),
        )),
    }
}

fn vector_f32_distance_jaccard(v1: &[f32], v2: &[f32]) -> f64 {
    let (mut min_sum, mut max_sum) = (0.0, 0.0);
    for (&a, &b) in v1.iter().zip(v2.iter()) {
        min_sum += a.min(b);
        max_sum += a.max(b);
    }
    if max_sum == 0.0 {
        return f64::NAN;
    }
    1. - (min_sum / max_sum) as f64
}

fn vector_f64_distance_jaccard(v1: &[f64], v2: &[f64]) -> f64 {
    let (mut min_sum, mut max_sum) = (0.0, 0.0);
    for (&a, &b) in v1.iter().zip(v2.iter()) {
        min_sum += a.min(b);
        max_sum += a.max(b);
    }
    if max_sum == 0.0 {
        return f64::NAN;
    }
    1. - min_sum / max_sum
}

fn vector_f32_sparse_distance_jaccard(v1: VectorSparse<f32>, v2: VectorSparse<f32>) -> f64 {
    let mut v1_pos = 0;
    let mut v2_pos = 0;
    let (mut min_sum, mut max_sum) = (0.0, 0.0);
    while v1_pos < v1.idx.len() && v2_pos < v2.idx.len() {
        if v1.idx[v1_pos] == v2.idx[v2_pos] {
            min_sum += v1.values[v1_pos].min(v2.values[v2_pos]);
            max_sum += v1.values[v1_pos].max(v2.values[v2_pos]);
            v1_pos += 1;
            v2_pos += 1;
        } else if v1.idx[v1_pos] < v2.idx[v2_pos] {
            min_sum += v1.values[v1_pos].min(0.);
            max_sum += v1.values[v1_pos].max(0.);
            v1_pos += 1;
        } else {
            min_sum += v2.values[v2_pos].min(0.);
            max_sum += v2.values[v2_pos].max(0.);
            v2_pos += 1;
        }
    }
    while v1_pos < v1.idx.len() {
        min_sum += v1.values[v1_pos].min(0.);
        max_sum += v1.values[v1_pos].max(0.);
        v1_pos += 1;
    }
    while v2_pos < v2.idx.len() {
        min_sum += v2.values[v2_pos].min(0.);
        max_sum += v2.values[v2_pos].max(0.);
        v2_pos += 1;
    }
    if max_sum == 0.0 {
        return f64::NAN;
    }
    1. - (min_sum / max_sum) as f64
}

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use crate::vector::{
        operations::convert::vector_convert, vector_types::tests::ArbitraryVector,
    };

    use super::*;

    #[test]
    fn test_vector_distance_jaccard_f32() {
        assert!(vector_f32_distance_jaccard(&[0.0, 0.0, 0.0], &[0.0, 0.0, 0.0]).is_nan());
        assert_eq!(vector_f32_distance_jaccard(&[1.0, 2.0], &[0.0, 0.0]), 1.0);
        assert_eq!(vector_f32_distance_jaccard(&[1.0, 2.0], &[1.0, 2.0]), 0.0);
        assert_eq!(
            vector_f32_distance_jaccard(&[1.0, 2.0], &[2.0, 1.0]),
            1. - (1.0 + 1.0) / (2.0 + 2.0)
        );
    }

    #[test]
    fn test_vector_distance_jaccard_f64() {
        assert!(vector_f64_distance_jaccard(&[], &[]).is_nan());
        assert!(vector_f64_distance_jaccard(&[0.0, 0.0, 0.0], &[0.0, 0.0, 0.0]).is_nan());
        assert_eq!(vector_f64_distance_jaccard(&[1.0, 2.0], &[0.0, 0.0]), 1.0);
        assert_eq!(vector_f64_distance_jaccard(&[1.0, 2.0], &[1.0, 2.0]), 0.0);
        assert_eq!(
            vector_f64_distance_jaccard(&[1.0, 2.0], &[2.0, 1.0]),
            1. - (1.0 + 1.0) / (2.0 + 2.0)
        );
    }

    #[test]
    fn test_vector_distance_jaccard_f32_sparse() {
        assert!(
            (vector_f32_sparse_distance_jaccard(
                VectorSparse {
                    idx: &[0, 1],
                    values: &[1.0, 2.0]
                },
                VectorSparse {
                    idx: &[1, 2],
                    values: &[1.0, 3.0]
                },
            ) - vector_f32_distance_jaccard(&[1.0, 2.0, 0.0], &[0.0, 1.0, 3.0]))
            .abs()
                < 1e-7
        );
    }

    #[quickcheck]
    fn prop_vector_distance_jaccard_dense_vs_sparse(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(v1.into(), VectorType::Float32Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float32Dense).unwrap();
        let d1 = vector_distance_jaccard(&v1, &v2).unwrap();
        println!("v1: {:?}, v2: {:?}", v1.as_f32_slice(), v2.as_f32_slice());

        let sparse1 = vector_convert(v1, VectorType::Float32Sparse).unwrap();
        let sparse2 = vector_convert(v2, VectorType::Float32Sparse).unwrap();
        let d2 =
            vector_f32_sparse_distance_jaccard(sparse1.as_f32_sparse(), sparse2.as_f32_sparse());

        println!("d1: {}, d2: {}, delta: {}", d1, d2, (d1 - d2).abs());
        (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-6
    }
}
