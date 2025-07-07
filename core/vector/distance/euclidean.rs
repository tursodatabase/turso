use super::{DistanceCalculator, DistanceType};
use crate::vector::vector_types::{Vector, VectorType};
use crate::{LimboError, Result};

#[derive(Debug, Clone)]
pub struct Euclidean;

impl DistanceCalculator for Euclidean {
    fn distance_type() -> DistanceType {
        DistanceType::Euclidean
    }

    fn calculate(v1: &[Vector], v2: &[Vector]) -> Result<f64> {
        let mut sum = 0.0;
        // We only pass two vectors, so it will only iterate once, it's ok to use `sum`
        for (v1, v2) in v1.iter().zip(v2.iter()) {
            if v1.dims != v2.dims {
                return Err(LimboError::ConversionError(
                    "Vectors must have the same dimensions".to_string(),
                ));
            }

            match (&v1.vector_type, &v2.vector_type) {
                (VectorType::Float32, VectorType::Float32) => {
                    sum += euclidean_distance_f32(v1.as_f32_slice(), v2.as_f32_slice());
                }
                (VectorType::Float64, VectorType::Float64) => {
                    sum += euclidean_distance_f64(v1.as_f64_slice(), v2.as_f64_slice());
                }
                _ => {
                    return Err(LimboError::ConversionError(
                        "Vectors must be of the same type (either Float32 or Float64)".to_string(),
                    ));
                }
            }
        }
        Ok(sum)
    }

    // TODO(asukamilet): Implement batch calculation for Euclidean distance
    fn batch_calculate<'a>(
        _v1: &'a [Vector],
        _v2: &'a [Vector],
    ) -> Box<dyn Iterator<Item = Result<f64>> + 'a> {
        todo!()
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
    use quickcheck::{Arbitrary, Gen};

    // Helper to generate arbitrary vectors of specific type and dimensions
    #[derive(Debug, Clone)]
    struct ArbitraryVector<const DIMS: usize> {
        vector_type: VectorType,
        data: Vec<u8>,
    }

    /// How to create an arbitrary vector of DIMS dims.
    impl<const DIMS: usize> ArbitraryVector<DIMS> {
        fn generate_f32_data(g: &mut Gen) -> Vec<f32> {
            (0..DIMS)
                .map(|_| {
                    loop {
                        let f = f32::arbitrary(g);
                        // f32::arbitrary() can generate "problem values" like NaN, infinity, and very small values
                        // Skip these values
                        if f.is_finite() && f.abs() >= 1e-6 {
                            // Scale to [-1, 1] range
                            return f % 2.0 - 1.0;
                        }
                    }
                })
                .collect()
        }

        fn generate_f64_data(g: &mut Gen) -> Vec<f64> {
            (0..DIMS)
                .map(|_| {
                    loop {
                        let f = f64::arbitrary(g);
                        // f64::arbitrary() can generate "problem values" like NaN, infinity, and very small values
                        // Skip these values
                        if f.is_finite() && f.abs() >= 1e-6 {
                            // Scale to [-1, 1] range
                            return f % 2.0 - 1.0;
                        }
                    }
                })
                .collect()
        }

        fn generate_f32_vector(g: &mut Gen) -> ArbitraryVector<DIMS> {
            let vector_type = VectorType::Float32;
            let data = Self::generate_f32_data(g);
            let data_bytes: Vec<u8> = data.iter().flat_map(|f| f.to_le_bytes()).collect();
            ArbitraryVector {
                vector_type,
                data: data_bytes,
            }
        }

        fn generate_f64_vector(g: &mut Gen) -> ArbitraryVector<DIMS> {
            let vector_type = VectorType::Float64;
            let data = Self::generate_f64_data(g);
            let data_bytes: Vec<u8> = data.iter().flat_map(|f| f.to_le_bytes()).collect();
            ArbitraryVector {
                vector_type,
                data: data_bytes,
            }
        }
    }

    /// Convert an ArbitraryVector to a Vector.
    impl<const DIMS: usize> From<ArbitraryVector<DIMS>> for Vector {
        fn from(v: ArbitraryVector<DIMS>) -> Self {
            Vector {
                vector_type: v.vector_type,
                dims: DIMS,
                data: v.data,
            }
        }
    }

    #[test]
    fn test_type_check() {
        let v1: Vector = ArbitraryVector::<1536>::generate_f32_vector(&mut Gen::new(0)).into();
        let v2: Vector = ArbitraryVector::<1536>::generate_f32_vector(&mut Gen::new(0)).into();
        assert!(Euclidean::calculate(&[v1], &[v2]).is_ok());

        let v1: Vector = ArbitraryVector::<1536>::generate_f64_vector(&mut Gen::new(0)).into();
        let v2: Vector = ArbitraryVector::<1536>::generate_f64_vector(&mut Gen::new(0)).into();
        assert!(Euclidean::calculate(&[v1], &[v2]).is_ok());

        let v1: Vector = ArbitraryVector::<1536>::generate_f32_vector(&mut Gen::new(0)).into();
        let v2: Vector = ArbitraryVector::<1536>::generate_f64_vector(&mut Gen::new(0)).into();
        assert!(Euclidean::calculate(&[v1], &[v2]).is_err());
    }

    #[test]
    fn test_dims_check() {
        let mut gen = Gen::new(0);

        let v1: Vector = ArbitraryVector::<1536>::generate_f32_vector(&mut gen).into();
        let v2: Vector = ArbitraryVector::<1024>::generate_f32_vector(&mut gen).into();
        assert!(Euclidean::calculate(&[v1], &[v2]).is_err());

        let v1: Vector = ArbitraryVector::<1024>::generate_f32_vector(&mut gen).into();
        let v2: Vector = ArbitraryVector::<1024>::generate_f32_vector(&mut gen).into();
        assert!(Euclidean::calculate(&[v1], &[v2]).is_ok());

        let v: Vector = ArbitraryVector::<1536>::generate_f64_vector(&mut gen).into();
        let v2: Vector = ArbitraryVector::<1536>::generate_f64_vector(&mut gen).into();
        assert!(Euclidean::calculate(&[v], &[v2]).is_ok());

        let v1: Vector = ArbitraryVector::<1536>::generate_f64_vector(&mut gen).into();
        let v2: Vector = ArbitraryVector::<1024>::generate_f64_vector(&mut gen).into();
        assert!(Euclidean::calculate(&[v1], &[v2]).is_err());
    }

    #[test]
    fn test_euclidean_distance_f32() {
        let vectors = vec![
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
