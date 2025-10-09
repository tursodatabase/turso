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
            max_sum += v1.values[v1_pos];
            v1_pos += 1;
        } else {
            max_sum += v2.values[v2_pos];
            v2_pos += 1;
        }
    }
    while v1_pos < v1.idx.len() {
        max_sum += v1.values[v1_pos];
        v1_pos += 1;
    }
    while v2_pos < v2.idx.len() {
        max_sum += v2.values[v2_pos];
        v2_pos += 1;
    }
    if max_sum == 0.0 {
        return f64::NAN;
    }
    1. - (min_sum / max_sum) as f64
}
