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
        VectorType::Float1Bit => Ok(vector_1bit_distance_jaccard(v1, v2)),
        VectorType::Float8 => Ok(vector_f8_distance_jaccard(v1, v2)),
    }
}

fn vector_1bit_distance_jaccard(v1: &Vector, v2: &Vector) -> f64 {
    // Binary Jaccard: 1.0 - popcount(a & b) / popcount(a | b)
    let d1 = v1.as_1bit_data();
    let d2 = v2.as_1bit_data();
    let mut intersection = 0u32;
    let mut union = 0u32;
    for (&a, &b) in d1.iter().zip(d2.iter()) {
        intersection += (a & b).count_ones();
        union += (a | b).count_ones();
    }
    if union == 0 {
        return f64::NAN;
    }
    1.0 - intersection as f64 / union as f64
}

fn vector_f8_distance_jaccard(v1: &Vector, v2: &Vector) -> f64 {
    let (data1, alpha1, shift1) = v1.as_f8_data();
    let (data2, alpha2, shift2) = v2.as_f8_data();
    let (mut min_sum, mut max_sum) = (0.0f64, 0.0f64);
    for i in 0..v1.dims {
        let f1 = alpha1 as f64 * data1[i] as f64 + shift1 as f64;
        let f2 = alpha2 as f64 * data2[i] as f64 + shift2 as f64;
        min_sum += f1.min(f2);
        max_sum += f1.max(f2);
    }
    if max_sum == 0.0 {
        return f64::NAN;
    }
    1.0 - min_sum / max_sum
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

    /// Float8 optimized Jaccard distance matches dequantized Float32 Jaccard distance.
    /// Tolerance is looser here because the Float8 path computes in f64 precision
    /// while the dequantized path accumulates in f32, causing precision differences
    /// that are amplified by Jaccard's min/max ratio when values are close to zero.
    #[quickcheck]
    fn prop_vector_distance_jaccard_f8_vs_dequantized(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(v1.into(), VectorType::Float32Dense).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float32Dense).unwrap();
        let v1_f8 = vector_convert(v1, VectorType::Float8).unwrap();
        let v2_f8 = vector_convert(v2, VectorType::Float8).unwrap();
        let d_f8 = vector_distance_jaccard(&v1_f8, &v2_f8).unwrap();
        let v1_deq = vector_convert(v1_f8, VectorType::Float32Dense).unwrap();
        let v2_deq = vector_convert(v2_f8, VectorType::Float32Dense).unwrap();
        let d_deq = vector_distance_jaccard(&v1_deq, &v2_deq).unwrap();
        (d_f8.is_nan() && d_deq.is_nan()) || (d_f8 - d_deq).abs() < 0.01
    }

    /// Float1Bit binary Jaccard matches manual computation from dequantized ±1 set bits.
    #[quickcheck]
    fn prop_vector_distance_jaccard_1bit_vs_manual(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(v1.into(), VectorType::Float1Bit).unwrap();
        let v2 = vector_convert(v2.into(), VectorType::Float1Bit).unwrap();
        let d = vector_distance_jaccard(&v1, &v2).unwrap();
        // Manual: binary Jaccard = 1 - |intersection| / |union| over set bits
        let d1 = v1.as_1bit_data();
        let d2 = v2.as_1bit_data();
        let mut intersection = 0u32;
        let mut union = 0u32;
        for (&a, &b) in d1.iter().zip(d2.iter()) {
            intersection += (a & b).count_ones();
            union += (a | b).count_ones();
        }
        if union == 0 {
            return d.is_nan();
        }
        let expected = 1.0 - intersection as f64 / union as f64;
        (d - expected).abs() < 1e-10
    }
}
