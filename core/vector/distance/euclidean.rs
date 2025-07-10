use super::{DistanceCalculator, DistanceType};
use crate::vector::vector_types::{Vector, VectorType};
use crate::Result;

#[derive(Debug, Clone)]
pub struct Euclidean;

impl DistanceCalculator for Euclidean {
    fn distance_type() -> DistanceType {
        DistanceType::Euclidean
    }

    fn calculate(v1: &Vector, v2: &Vector) -> Result<f64> {
        match v1.vector_type {
            VectorType::Float32 => Ok(euclidean_distance_f32(v1.as_f32_slice(), v2.as_f32_slice())),
            VectorType::Float64 => Ok(euclidean_distance_f64(v1.as_f64_slice(), v2.as_f64_slice())),
        }
    }
}

fn euclidean_distance_f32(v1: &[f32], v2: &[f32]) -> f64 {
    let sum = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f32>() as f64;
    sum.sqrt()
}

fn euclidean_distance_f64(v1: &[f64], v2: &[f64]) -> f64 {
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
            .map(|v| euclidean_distance_f32(&query, v))
            .collect::<Vec<f64>>();
        assert_eq!(results, expected);
    }

    #[test]
    fn test_odd_len() {
        let v = (0..5).map(|x| x as f32).collect::<Vec<f32>>();
        let query = (2..7).map(|x| x as f32).collect::<Vec<f32>>();
        assert_eq!(euclidean_distance_f32(&v, &query), 20.0_f64.sqrt());
    }
}
