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
        VectorType::Float32Dense => Ok(vector_f32_distance_dot_simsimd(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        VectorType::Float64Dense => Ok(vector_f64_distance_dot_simsimd(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        VectorType::Float32Sparse => Ok(vector_f32_sparse_distance_dot(
            v1.as_f32_sparse(),
            v2.as_f32_sparse(),
        )),
        VectorType::Float1Bit => Ok(vector_1bit_distance_dot(v1, v2)),
        VectorType::Float8 => Ok(vector_f8_distance_dot(v1, v2)),
    }
}

fn vector_1bit_distance_dot(v1: &Vector, v2: &Vector) -> f64 {
    // 1-bit values represent +1/-1.
    // Dot product = dims - 2 * hamming_distance
    // Return negated (consistent with existing dot distance convention).
    let d1 = v1.as_1bit_data();
    let d2 = v2.as_1bit_data();
    let mut hamming = 0u32;
    for (&a, &b) in d1.iter().zip(d2.iter()) {
        hamming += (a ^ b).count_ones();
    }
    let dot = v1.dims as f64 - 2.0 * hamming as f64;
    -dot
}

fn vector_f8_distance_dot(v1: &Vector, v2: &Vector) -> f64 {
    let (data1, alpha1, shift1) = v1.as_f8_data();
    let (data2, alpha2, shift2) = v2.as_f8_data();
    let dims = v1.dims;

    let (mut sum1, mut sum2, mut doti) = (0u64, 0u64, 0u64);
    for i in 0..dims {
        let q1 = data1[i] as u64;
        let q2 = data2[i] as u64;
        sum1 += q1;
        sum2 += q2;
        doti += q1 * q2;
    }

    let a1 = alpha1 as f64;
    let a2 = alpha2 as f64;
    let s1 = shift1 as f64;
    let s2 = shift2 as f64;
    let d = dims as f64;

    let dot = a1 * a2 * doti as f64 + a1 * s2 * sum1 as f64 + a2 * s1 * sum2 as f64 + s1 * s2 * d;
    -dot
}

#[allow(dead_code)]
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
fn vector_f32_distance_dot_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    -f32::dot(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD does not support WASM, and Windows AArch64 has linker issues with simsimd.lib.
#[allow(dead_code)]
fn vector_f32_distance_dot_rust(v1: &[f32], v2: &[f32]) -> f64 {
    let mut dot = 0.0;
    for (a, b) in v1.iter().zip(v2.iter()) {
        dot += (*a as f64) * (*b as f64);
    }
    -dot
}

#[allow(dead_code)]
#[cfg(not(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
)))]
fn vector_f32_distance_dot_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    vector_f32_distance_dot_rust(v1, v2)
}

#[allow(dead_code)]
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
fn vector_f64_distance_dot_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    -f64::dot(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD does not support WASM, and Windows AArch64 has linker issues with simsimd.lib.
#[allow(dead_code)]
fn vector_f64_distance_dot_rust(v1: &[f64], v2: &[f64]) -> f64 {
    let mut dot = 0.0;
    for (a, b) in v1.iter().zip(v2.iter()) {
        dot += *a * *b;
    }
    -dot
}

#[allow(dead_code)]
#[cfg(not(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
)))]
fn vector_f64_distance_dot_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    vector_f64_distance_dot_rust(v1, v2)
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
#[path = "../../tests/unit/vector/operations/distance_dot/tests.rs"]
mod tests;
