use crate::{
    vector::vector_types::{Vector, VectorType},
    LimboError, Result,
};

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
        VectorType::Float32Dense => {
            Ok(vector_f32_distance_l2(v1.as_f32_slice(), v2.as_f32_slice()))
        }
        VectorType::Float64Dense => {
            Ok(vector_f64_distance_l2(v1.as_f64_slice(), v2.as_f64_slice()))
        }
    }
}

fn vector_f32_distance_l2(v1: &[f32], v2: &[f32]) -> f64 {
    let sum = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f32>() as f64;
    sum.sqrt()
}

fn vector_f64_distance_l2(v1: &[f64], v2: &[f64]) -> f64 {
    let sum = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f64>();
    sum.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance_f32() {
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
            .map(|v| vector_f32_distance_l2(&query, v))
            .collect::<Vec<f64>>();
        assert_eq!(results, expected);
    }

    #[test]
    fn test_odd_len() {
        let v = (0..5).map(|x| x as f32).collect::<Vec<f32>>();
        let query = (2..7).map(|x| x as f32).collect::<Vec<f32>>();
        assert_eq!(vector_f32_distance_l2(&v, &query), 20.0_f64.sqrt());
    }
}
