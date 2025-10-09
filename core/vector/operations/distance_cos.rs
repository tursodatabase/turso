use crate::{
    vector::vector_types::{Vector, VectorType},
    LimboError, Result,
};

pub fn vector_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
    match v1.vector_type {
        VectorType::Float32Dense => vector_f32_distance_cos(v1, v2),
        VectorType::Float64Dense => vector_f64_distance_cos(v1, v2),
    }
}

fn vector_f32_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
    if v1.dims != v2.dims {
        return Err(LimboError::ConversionError(
            "Invalid vector dimensions".to_string(),
        ));
    }
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        ));
    }
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);
    let v1_data = v1.as_f32_slice();
    let v2_data = v2.as_f32_slice();

    // Check for non-finite values
    if v1_data.iter().any(|x| !x.is_finite()) || v2_data.iter().any(|x| !x.is_finite()) {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }

    for i in 0..v1.dims {
        let e1 = v1_data[i];
        let e2 = v2_data[i];
        dot += e1 * e2;
        norm1 += e1 * e1;
        norm2 += e2 * e2;
    }

    // Check for zero norms to avoid division by zero
    if norm1 == 0.0 || norm2 == 0.0 {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }

    Ok(1.0 - (dot / (norm1 * norm2).sqrt()) as f64)
}

fn vector_f64_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
    if v1.dims != v2.dims {
        return Err(LimboError::ConversionError(
            "Invalid vector dimensions".to_string(),
        ));
    }
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        ));
    }
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);
    let v1_data = v1.as_f64_slice();
    let v2_data = v2.as_f64_slice();

    // Check for non-finite values
    if v1_data.iter().any(|x| !x.is_finite()) || v2_data.iter().any(|x| !x.is_finite()) {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }

    for i in 0..v1.dims {
        let e1 = v1_data[i];
        let e2 = v2_data[i];
        dot += e1 * e2;
        norm1 += e1 * e1;
        norm2 += e2 * e2;
    }

    // Check for zero norms
    if norm1 == 0.0 || norm2 == 0.0 {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }

    Ok(1.0 - (dot / (norm1 * norm2).sqrt()))
}
