use crate::{
    vector::vector_types::{Vector, VectorType},
    LimboError, Result,
};

pub fn vector_slice(vector: &Vector, start: usize, end: usize) -> Result<Vector<'static>> {
    if start > end {
        return Err(LimboError::InvalidArgument(
            "start index must not be greater than end index".into(),
        ));
    }
    if end > vector.dims || end < start {
        return Err(LimboError::ConversionError(
            "vector_slice range out of bounds".into(),
        ));
    }
    match vector.vector_type {
        VectorType::Float32Dense => Ok(Vector {
            vector_type: vector.vector_type,
            dims: end - start,
            owned: Some(vector.bin_data()[start * 4..end * 4].to_vec()),
            refer: None,
        }),
        VectorType::Float64Dense => Ok(Vector {
            vector_type: vector.vector_type,
            dims: end - start,
            owned: Some(vector.bin_data()[start * 8..end * 8].to_vec()),
            refer: None,
        }),
        VectorType::Float32Sparse => {
            let mut values = Vec::new();
            let mut idx = Vec::new();
            let sparse = vector.as_f32_sparse();
            for (&i, &value) in sparse.idx.iter().zip(sparse.values.iter()) {
                let i = i as usize;
                if i < start || i >= end {
                    continue;
                }
                values.extend_from_slice(&value.to_le_bytes());
                idx.extend_from_slice(&i.to_le_bytes());
            }
            values.extend_from_slice(&idx);
            Ok(Vector {
                vector_type: vector.vector_type,
                dims: end - start,
                owned: Some(values),
                refer: None,
            })
        }
    }
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
            owned: Some(data),
            refer: None,
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
