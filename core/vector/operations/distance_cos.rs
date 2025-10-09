use crate::{
    vector::vector_types::{Vector, VectorType},
    LimboError, Result,
};

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
        VectorType::Float32Dense => Ok(vector_f32_distance_cos(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        VectorType::Float64Dense => Ok(vector_f64_distance_cos(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        _ => todo!(),
    }
}

fn vector_f32_distance_cos(v1: &[f32], v2: &[f32]) -> f64 {
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);

    let dims = v1.len();
    for i in 0..dims {
        let e1 = v1[i];
        let e2 = v2[i];
        dot += e1 * e2;
        norm1 += e1 * e1;
        norm2 += e2 * e2;
    }

    // Check for zero norms to avoid division by zero
    if norm1 == 0.0 || norm2 == 0.0 {
        return f64::NAN;
    }

    1.0 - (dot / (norm1 * norm2).sqrt()) as f64
}

fn vector_f64_distance_cos(v1: &[f64], v2: &[f64]) -> f64 {
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);

    let dims = v1.len();
    for i in 0..dims {
        let e1 = v1[i];
        let e2 = v2[i];
        dot += e1 * e2;
        norm1 += e1 * e1;
        norm2 += e2 * e2;
    }

    // Check for zero norms
    if norm1 == 0.0 || norm2 == 0.0 {
        return f64::NAN;
    }

    1.0 - (dot / (norm1 * norm2).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_cos_f32() {
        assert!(vector_f32_distance_cos(&[], &[]).is_nan());
        assert!(vector_f32_distance_cos(&[1.0, 2.0], &[0.0, 0.0]).is_nan());
        assert_eq!(vector_f32_distance_cos(&[1.0, 2.0], &[1.0, 2.0]), 0.0);
        assert_eq!(vector_f32_distance_cos(&[1.0, 2.0], &[-1.0, -2.0]), 2.0);
        assert_eq!(vector_f32_distance_cos(&[1.0, 2.0], &[-2.0, 1.0]), 1.0);
    }
}
