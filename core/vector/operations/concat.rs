use crate::{
    vector::vector_types::{Vector, VectorType},
    LimboError, Result,
};

pub fn vector_concat(v1: &Vector, v2: &Vector) -> Result<Vector<'static>> {
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Mismatched vector types".into(),
        ));
    }

    let data = match v1.vector_type {
        VectorType::Float32Dense | VectorType::Float64Dense => {
            let mut data = Vec::with_capacity(v1.bin_len() + v2.bin_len());
            data.extend_from_slice(v1.bin_data());
            data.extend_from_slice(v2.bin_data());
            data
        }
        VectorType::Float32Sparse => {
            let mut data = Vec::with_capacity(v1.bin_len() + v2.bin_len());
            data.extend_from_slice(&v1.bin_data()[..v1.bin_len() / 2]);
            data.extend_from_slice(&v2.bin_data()[..v2.bin_len() / 2]);
            data.extend_from_slice(&v1.bin_data()[v1.bin_len() / 2..]);
            data.extend_from_slice(&v2.bin_data()[v2.bin_len() / 2..]);
            data
        }
    };

    Ok(Vector {
        vector_type: v1.vector_type,
        dims: v1.dims + v2.dims,
        owned: Some(data),
        refer: None,
    })
}

#[cfg(test)]
mod tests {
    use crate::vector::{
        operations::concat::vector_concat,
        vector_types::{Vector, VectorType},
    };

    fn float32_vec_from(slice: &[f32]) -> Vector<'static> {
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
    fn test_vector_concat_normal_case() {
        let v1 = float32_vec_from(&[1.0, 2.0, 3.0]);
        let v2 = float32_vec_from(&[4.0, 5.0, 6.0]);

        let result = vector_concat(&v1, &v2).unwrap();

        assert_eq!(result.dims, 6);
        assert_eq!(result.vector_type, VectorType::Float32Dense);
        assert_eq!(
            f32_slice_from_vector(&result),
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        );
    }

    #[test]
    fn test_vector_concat_empty_left() {
        let v1 = float32_vec_from(&[]);
        let v2 = float32_vec_from(&[4.0, 5.0]);

        let result = vector_concat(&v1, &v2).unwrap();

        assert_eq!(result.dims, 2);
        assert_eq!(f32_slice_from_vector(&result), vec![4.0, 5.0]);
    }

    #[test]
    fn test_vector_concat_empty_right() {
        let v1 = float32_vec_from(&[1.0, 2.0]);
        let v2 = float32_vec_from(&[]);

        let result = vector_concat(&v1, &v2).unwrap();

        assert_eq!(result.dims, 2);
        assert_eq!(f32_slice_from_vector(&result), vec![1.0, 2.0]);
    }

    #[test]
    fn test_vector_concat_both_empty() {
        let v1 = float32_vec_from(&[]);
        let v2 = float32_vec_from(&[]);
        let result = vector_concat(&v1, &v2).unwrap();
        assert_eq!(result.dims, 0);
        assert_eq!(f32_slice_from_vector(&result), Vec::<f32>::new());
    }

    #[test]
    fn test_vector_concat_different_lengths() {
        let v1 = float32_vec_from(&[1.0]);
        let v2 = float32_vec_from(&[2.0, 3.0, 4.0]);

        let result = vector_concat(&v1, &v2).unwrap();

        assert_eq!(result.dims, 4);
        assert_eq!(f32_slice_from_vector(&result), vec![1.0, 2.0, 3.0, 4.0]);
    }
}
