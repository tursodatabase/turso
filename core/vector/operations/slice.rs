use crate::{
    vector::vector_types::{Vector, VectorType},
    LimboError, Result,
};

pub fn vector_slice(vector: &Vector, start_idx: usize, end_idx: usize) -> Result<Vector> {
    fn extract_bytes<T, const N: usize>(
        slice: &[T],
        start: usize,
        end: usize,
        to_bytes: impl Fn(&T) -> [u8; N],
    ) -> Result<Vec<u8>> {
        if start > end {
            return Err(LimboError::InvalidArgument(
                "start index must not be greater than end index".into(),
            ));
        }
        if end > slice.len() || end < start {
            return Err(LimboError::ConversionError(
                "vector_slice range out of bounds".into(),
            ));
        }

        let mut buf = Vec::with_capacity((end - start) * N);
        for item in &slice[start..end] {
            buf.extend_from_slice(&to_bytes(item));
        }
        Ok(buf)
    }

    let (vector_type, data) = match vector.vector_type {
        VectorType::Float32Dense => (
            VectorType::Float32Dense,
            extract_bytes::<f32, 4>(vector.as_f32_slice(), start_idx, end_idx, |v| {
                v.to_le_bytes()
            })?,
        ),
        VectorType::Float64Dense => (
            VectorType::Float64Dense,
            extract_bytes::<f64, 8>(vector.as_f64_slice(), start_idx, end_idx, |v| {
                v.to_le_bytes()
            })?,
        ),
    };

    Ok(Vector {
        vector_type,
        dims: end_idx - start_idx,
        data,
    })
}

#[cfg(test)]
mod tests {
    use crate::vector::{
        operations::slice::vector_slice,
        vector_types::{Vector, VectorType},
    };

    fn float32_vec_from(slice: &[f32]) -> Vector {
        let mut data = Vec::new();
        for &v in slice {
            data.extend_from_slice(&v.to_le_bytes());
        }

        Vector {
            vector_type: VectorType::Float32Dense,
            dims: slice.len(),
            data,
        }
    }

    fn f32_slice_from_vector(vector: &Vector) -> Vec<f32> {
        vector.as_f32_slice().to_vec()
    }

    #[test]
    fn test_vector_slice_normal_case() {
        let input_vec = float32_vec_from(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let result = vector_slice(&input_vec, 1, 4).unwrap();

        assert_eq!(result.dims, 3);
        assert_eq!(f32_slice_from_vector(&result), vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_vector_slice_full_range() {
        let input_vec = float32_vec_from(&[10.0, 20.0, 30.0]);
        let result = vector_slice(&input_vec, 0, 3).unwrap();

        assert_eq!(result.dims, 3);
        assert_eq!(f32_slice_from_vector(&result), vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn test_vector_slice_single_element() {
        let input_vec = float32_vec_from(&[4.40, 2.71]);
        let result = vector_slice(&input_vec, 1, 2).unwrap();

        assert_eq!(result.dims, 1);
        assert_eq!(f32_slice_from_vector(&result), vec![2.71]);
    }

    #[test]
    fn test_vector_slice_empty_list() {
        let input_vec = float32_vec_from(&[1.0, 2.0]);
        let result = vector_slice(&input_vec, 2, 2).unwrap();

        assert_eq!(result.dims, 0);
    }

    #[test]
    fn test_vector_slice_zero_length() {
        let input_vec = float32_vec_from(&[1.0, 2.0, 3.0]);
        let err = vector_slice(&input_vec, 2, 1);
        assert!(err.is_err(), "Expected error on zero-length range");
    }

    #[test]
    fn test_vector_slice_out_of_bounds() {
        let input_vec = float32_vec_from(&[1.0, 2.0]);
        let err = vector_slice(&input_vec, 0, 5);
        assert!(err.is_err());
    }

    #[test]
    fn test_vector_slice_start_out_of_bounds() {
        let input_vec = float32_vec_from(&[1.0, 2.0]);
        let err = vector_slice(&input_vec, 5, 5);
        assert!(err.is_err());
    }

    #[test]
    fn test_vector_slice_end_out_of_bounds() {
        let input_vec = float32_vec_from(&[1.0, 2.0]);
        let err = vector_slice(&input_vec, 1, 3);
        assert!(err.is_err());
    }
}
