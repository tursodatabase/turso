use crate::{
    vector::vector_types::{Vector, VectorSparse, VectorType},
    LimboError, Result,
};
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
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
        VectorType::Float32Dense => Ok(vector_f32_distance_l2_simsimd(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        VectorType::Float64Dense => Ok(vector_f64_distance_l2_simsimd(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        VectorType::Float32Sparse => Ok(vector_f32_sparse_distance_l2(
            v1.as_f32_sparse(),
            v2.as_f32_sparse(),
        )),
        VectorType::Float1Bit => Err(LimboError::ConversionError(
            "L2 distance is not supported for float1bit vectors".to_string(),
        )),
        VectorType::Float8 => Ok(vector_f8_distance_l2(v1, v2)),
    }
}

fn vector_f8_distance_l2(v1: &Vector, v2: &Vector) -> f64 {
    let (data1, alpha1, shift1) = v1.as_f8_data();
    let (data2, alpha2, shift2) = v2.as_f8_data();
    let mut sum = 0.0f64;
    for i in 0..v1.dims {
        let f1 = alpha1 as f64 * data1[i] as f64 + shift1 as f64;
        let f2 = alpha2 as f64 * data2[i] as f64 + shift2 as f64;
        let d = f1 - f2;
        sum += d * d;
    }
    sum.sqrt()
}

#[allow(dead_code)]
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
fn vector_f32_distance_l2_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    f32::euclidean(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD does not support WASM, and Windows AArch64 has linker issues with simsimd.lib.
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
#[cfg(not(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
)))]
fn vector_f32_distance_l2_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    vector_f32_distance_l2_rust(v1, v2)
}

#[allow(dead_code)]
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
fn vector_f64_distance_l2_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    f64::euclidean(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD does not support WASM, and Windows AArch64 has linker issues with simsimd.lib.
#[allow(dead_code)]
fn vector_f64_distance_l2_rust(v1: &[f64], v2: &[f64]) -> f64 {
    let sum = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f64>();
    sum.sqrt()
}

#[allow(dead_code)]
#[cfg(not(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
)))]
fn vector_f64_distance_l2_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    vector_f64_distance_l2_rust(v1, v2)
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
#[path = "../../tests/unit/vector/operations/distance_l2/tests.rs"]
mod tests;
