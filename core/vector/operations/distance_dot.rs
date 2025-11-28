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
