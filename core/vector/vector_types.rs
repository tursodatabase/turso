use crate::types::{Value, ValueType};
use crate::vdbe::Register;
use crate::{LimboError, Result};

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum VectorType {
    Float32,
    Float64,
}

impl VectorType {
    pub fn size_to_dims(&self, size: usize) -> usize {
        match self {
            VectorType::Float32 => size / 4,
            VectorType::Float64 => size / 8,
        }
    }
}

#[derive(Debug)]
pub struct Vector {
    pub vector_type: VectorType,
    pub dims: usize,
    pub data: Vec<u8>,
}

impl Vector {
    /// # Safety
    ///
    /// This method is used to reinterpret the underlying `Vec<u8>` data
    /// as a `&[f32]` slice. This is only valid if:
    /// - The buffer is correctly aligned for `f32`
    /// - The length of the buffer is exactly `dims * size_of::<f32>()`
    pub fn as_f32_slice(&self) -> &[f32] {
        if self.dims == 0 {
            return &[];
        }

        assert_eq!(
            self.data.len(),
            self.dims * std::mem::size_of::<f32>(),
            "data length must equal dims * size_of::<f32>()"
        );

        let ptr = self.data.as_ptr();
        let align = std::mem::align_of::<f32>();
        assert_eq!(
            ptr.align_offset(align),
            0,
            "data pointer must be aligned to {align} bytes for f32 access"
        );

        unsafe { std::slice::from_raw_parts(ptr as *const f32, self.dims) }
    }

    /// # Safety
    ///
    /// This method is used to reinterpret the underlying `Vec<u8>` data
    /// as a `&[f64]` slice. This is only valid if:
    /// - The buffer is correctly aligned for `f64`
    /// - The length of the buffer is exactly `dims * size_of::<f64>()`
    pub fn as_f64_slice(&self) -> &[f64] {
        if self.dims == 0 {
            return &[];
        }

        assert_eq!(
            self.data.len(),
            self.dims * std::mem::size_of::<f64>(),
            "data length must equal dims * size_of::<f64>()"
        );

        let ptr = self.data.as_ptr();
        let align = std::mem::align_of::<f64>();
        assert_eq!(
            ptr.align_offset(align),
            0,
            "data pointer must be aligned to {align} bytes for f64 access"
        );

        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const f64, self.dims) }
    }
}

/// Parse a vector in text representation into a Vector.
///
/// The format of a vector in text representation looks as follows:
///
/// ```console
/// [1.0, 2.0, 3.0]
/// ```
pub fn parse_string_vector(vector_type: VectorType, value: &Value) -> Result<Vector> {
    let Some(text) = value.to_text() else {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    };
    let text = text.trim();
    let mut chars = text.chars();
    if chars.next() != Some('[') || chars.last() != Some(']') {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }
    let mut data: Vec<u8> = Vec::new();
    let text = &text[1..text.len() - 1];
    if text.trim().is_empty() {
        return Ok(Vector {
            vector_type,
            dims: 0,
            data,
        });
    }
    let xs = text.split(',');
    for x in xs {
        let x = x.trim();
        if x.is_empty() {
            return Err(LimboError::ConversionError(
                "Invalid vector value".to_string(),
            ));
        }
        match vector_type {
            VectorType::Float32 => {
                let x = x
                    .parse::<f32>()
                    .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
                if !x.is_finite() {
                    return Err(LimboError::ConversionError(
                        "Invalid vector value".to_string(),
                    ));
                }
                data.extend_from_slice(&x.to_le_bytes());
            }
            VectorType::Float64 => {
                let x = x
                    .parse::<f64>()
                    .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
                if !x.is_finite() {
                    return Err(LimboError::ConversionError(
                        "Invalid vector value".to_string(),
                    ));
                }
                data.extend_from_slice(&x.to_le_bytes());
            }
        };
    }
    let dims = vector_type.size_to_dims(data.len());
    Ok(Vector {
        vector_type,
        dims,
        data,
    })
}

pub fn parse_vector(value: &Register, vec_ty: Option<VectorType>) -> Result<Vector> {
    match value.get_owned_value().value_type() {
        ValueType::Text => parse_string_vector(
            vec_ty.unwrap_or(VectorType::Float32),
            value.get_owned_value(),
        ),
        ValueType::Blob => {
            let Some(blob) = value.get_owned_value().to_blob() else {
                return Err(LimboError::ConversionError(
                    "Invalid vector value".to_string(),
                ));
            };
            let vector_type = vector_type(blob)?;
            if let Some(vec_ty) = vec_ty {
                if vec_ty != vector_type {
                    return Err(LimboError::ConversionError(
                        "Invalid vector type".to_string(),
                    ));
                }
            }
            vector_deserialize(vector_type, blob)
        }
        _ => Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        )),
    }
}

pub fn vector_to_text(vector: &Vector) -> String {
    let mut text = String::new();
    text.push('[');
    match vector.vector_type {
        VectorType::Float32 => {
            let data = vector.as_f32_slice();
            for (i, value) in data.iter().enumerate().take(vector.dims) {
                text.push_str(&value.to_string());
                if i < vector.dims - 1 {
                    text.push(',');
                }
            }
        }
        VectorType::Float64 => {
            let data = vector.as_f64_slice();
            for (i, value) in data.iter().enumerate().take(vector.dims) {
                text.push_str(&value.to_string());
                if i < vector.dims - 1 {
                    text.push(',');
                }
            }
        }
    }
    text.push(']');
    text
}

pub fn vector_deserialize(vector_type: VectorType, blob: &[u8]) -> Result<Vector> {
    match vector_type {
        VectorType::Float32 => vector_deserialize_f32(blob),
        VectorType::Float64 => vector_deserialize_f64(blob),
    }
}

pub fn vector_serialize_f64(x: Vector) -> Value {
    let mut blob = Vec::with_capacity(x.dims * 8 + 1);
    blob.extend_from_slice(&x.data);
    blob.push(2);
    Value::from_blob(blob)
}

pub fn vector_deserialize_f64(blob: &[u8]) -> Result<Vector> {
    Ok(Vector {
        vector_type: VectorType::Float64,
        dims: (blob.len() - 1) / 8,
        data: blob[..blob.len() - 1].to_vec(),
    })
}

pub fn vector_serialize_f32(x: Vector) -> Value {
    Value::from_blob(x.data)
}

pub fn vector_deserialize_f32(blob: &[u8]) -> Result<Vector> {
    Ok(Vector {
        vector_type: VectorType::Float32,
        dims: blob.len() / 4,
        data: blob.to_vec(),
    })
}

pub fn do_vector_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
    match v1.vector_type {
        VectorType::Float32 => vector_f32_distance_cos(v1, v2),
        VectorType::Float64 => vector_f64_distance_cos(v1, v2),
    }
}

pub fn vector_f32_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
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

pub fn vector_f64_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
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

pub fn vector_type(blob: &[u8]) -> Result<VectorType> {
    // Even-sized blobs are always float32.
    if blob.len() % 2 == 0 {
        return Ok(VectorType::Float32);
    }
    // Odd-sized blobs have type byte at the end
    let (data_blob, type_byte) = blob.split_at(blob.len() - 1);
    let vector_type = type_byte[0];
    match vector_type {
        1 => {
            if data_blob.len() % 4 != 0 {
                return Err(LimboError::ConversionError(
                    "Invalid vector value".to_string(),
                ));
            }
            Ok(VectorType::Float32)
        }
        2 => {
            if data_blob.len() % 8 != 0 {
                return Err(LimboError::ConversionError(
                    "Invalid vector value".to_string(),
                ));
            }
            Ok(VectorType::Float64)
        }
        _ => Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        )),
    }
}

pub fn vector_concat(v1: &Vector, v2: &Vector) -> Result<Vector> {
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Mismatched vector types".into(),
        ));
    }

    let mut data = Vec::with_capacity(v1.data.len() + v2.data.len());
    data.extend_from_slice(&v1.data);
    data.extend_from_slice(&v2.data);

    Ok(Vector {
        vector_type: v1.vector_type,
        dims: v1.dims + v2.dims,
        data,
    })
}

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
        VectorType::Float32 => (
            VectorType::Float32,
            extract_bytes::<f32, 4>(vector.as_f32_slice(), start_idx, end_idx, |v| {
                v.to_le_bytes()
            })?,
        ),
        VectorType::Float64 => (
            VectorType::Float64,
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
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use turso_macros::turso_test;

    // Helper to generate arbitrary vectors of specific type and dimensions
    #[derive(Debug, Clone)]
    struct ArbitraryVector<const DIMS: usize> {
        vector_type: VectorType,
        data: Vec<u8>,
    }

    /// How to create an arbitrary vector of DIMS dims.
    impl<const DIMS: usize> ArbitraryVector<DIMS> {
        fn generate_f32_vector(g: &mut Gen) -> Vec<f32> {
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

        fn generate_f64_vector(g: &mut Gen) -> Vec<f64> {
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

    /// Implement the quickcheck Arbitrary trait for ArbitraryVector.
    impl<const DIMS: usize> Arbitrary for ArbitraryVector<DIMS> {
        fn arbitrary(g: &mut Gen) -> Self {
            let vector_type = if bool::arbitrary(g) {
                VectorType::Float32
            } else {
                VectorType::Float64
            };

            let data = match vector_type {
                VectorType::Float32 => {
                    let floats = Self::generate_f32_vector(g);
                    floats.iter().flat_map(|f| f.to_le_bytes()).collect()
                }
                VectorType::Float64 => {
                    let floats = Self::generate_f64_vector(g);
                    floats.iter().flat_map(|f| f.to_le_bytes()).collect()
                }
            };

            ArbitraryVector { vector_type, data }
        }
    }

    #[quickcheck]
    fn prop_vector_type_identification_2d(v: ArbitraryVector<2>) -> bool {
        test_vector_type::<2>(v.into())
    }

    #[quickcheck]
    fn prop_vector_type_identification_3d(v: ArbitraryVector<3>) -> bool {
        test_vector_type::<3>(v.into())
    }

    #[quickcheck]
    fn prop_vector_type_identification_4d(v: ArbitraryVector<4>) -> bool {
        test_vector_type::<4>(v.into())
    }

    #[quickcheck]
    fn prop_vector_type_identification_100d(v: ArbitraryVector<100>) -> bool {
        test_vector_type::<100>(v.into())
    }

    #[quickcheck]
    fn prop_vector_type_identification_1536d(v: ArbitraryVector<1536>) -> bool {
        test_vector_type::<1536>(v.into())
    }

    /// Test if the vector type identification is correct for a given vector.
    fn test_vector_type<const DIMS: usize>(v: Vector) -> bool {
        let vtype = v.vector_type;
        let value = match &vtype {
            VectorType::Float32 => vector_serialize_f32(v),
            VectorType::Float64 => vector_serialize_f64(v),
        };

        let blob = value.to_blob().unwrap();
        match vector_type(blob) {
            Ok(detected_type) => detected_type == vtype,
            Err(_) => false,
        }
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_2d(v: ArbitraryVector<2>) -> bool {
        test_slice_conversion::<2>(v.into())
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_3d(v: ArbitraryVector<3>) -> bool {
        test_slice_conversion::<3>(v.into())
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_4d(v: ArbitraryVector<4>) -> bool {
        test_slice_conversion::<4>(v.into())
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_100d(v: ArbitraryVector<100>) -> bool {
        test_slice_conversion::<100>(v.into())
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_1536d(v: ArbitraryVector<1536>) -> bool {
        test_slice_conversion::<1536>(v.into())
    }

    /// Test if the slice conversion is safe for a given vector:
    /// - The slice length matches the dimensions
    /// - The data length is correct (4 bytes per float for f32, 8 bytes per float for f64)
    fn test_slice_conversion<const DIMS: usize>(v: Vector) -> bool {
        match v.vector_type {
            VectorType::Float32 => {
                let slice = v.as_f32_slice();
                // Check if the slice length matches the dimensions and the data length is correct (4 bytes per float)
                slice.len() == DIMS && (slice.len() * 4 == v.data.len())
            }
            VectorType::Float64 => {
                let slice = v.as_f64_slice();
                // Check if the slice length matches the dimensions and the data length is correct (8 bytes per float)
                slice.len() == DIMS && (slice.len() * 8 == v.data.len())
            }
        }
    }

    // Test size_to_dims calculation with different dimensions
    #[quickcheck]
    fn prop_size_to_dims_calculation_2d(v: ArbitraryVector<2>) -> bool {
        test_size_to_dims::<2>(v.into())
    }

    #[quickcheck]
    fn prop_size_to_dims_calculation_3d(v: ArbitraryVector<3>) -> bool {
        test_size_to_dims::<3>(v.into())
    }

    #[quickcheck]
    fn prop_size_to_dims_calculation_4d(v: ArbitraryVector<4>) -> bool {
        test_size_to_dims::<4>(v.into())
    }

    #[quickcheck]
    fn prop_size_to_dims_calculation_100d(v: ArbitraryVector<100>) -> bool {
        test_size_to_dims::<100>(v.into())
    }

    #[quickcheck]
    fn prop_size_to_dims_calculation_1536d(v: ArbitraryVector<1536>) -> bool {
        test_size_to_dims::<1536>(v.into())
    }

    /// Test if the size_to_dims calculation is correct for a given vector.
    fn test_size_to_dims<const DIMS: usize>(v: Vector) -> bool {
        let size = v.data.len();
        let calculated_dims = v.vector_type.size_to_dims(size);
        calculated_dims == DIMS
    }

    #[quickcheck]
    fn prop_vector_distance_safety_2d(v1: ArbitraryVector<2>, v2: ArbitraryVector<2>) -> bool {
        test_vector_distance::<2>(&v1.into(), &v2.into())
    }

    #[quickcheck]
    fn prop_vector_distance_safety_3d(v1: ArbitraryVector<3>, v2: ArbitraryVector<3>) -> bool {
        test_vector_distance::<3>(&v1.into(), &v2.into())
    }

    #[quickcheck]
    fn prop_vector_distance_safety_4d(v1: ArbitraryVector<4>, v2: ArbitraryVector<4>) -> bool {
        test_vector_distance::<4>(&v1.into(), &v2.into())
    }

    #[quickcheck]
    fn prop_vector_distance_safety_100d(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        test_vector_distance::<100>(&v1.into(), &v2.into())
    }

    #[quickcheck]
    fn prop_vector_distance_safety_1536d(
        v1: ArbitraryVector<1536>,
        v2: ArbitraryVector<1536>,
    ) -> bool {
        test_vector_distance::<1536>(&v1.into(), &v2.into())
    }

    /// Test if the vector distance calculation is correct for a given pair of vectors:
    /// - Skips cases with invalid input vectors.
    /// - Assumes vectors are well-formed (same type and dimension)
    /// - The distance must be between 0 and 2
    fn test_vector_distance<const DIMS: usize>(v1: &Vector, v2: &Vector) -> bool {
        match do_vector_distance_cos(v1, v2) {
            Ok(distance) => (0.0..=2.0).contains(&distance),
            Err(_) => true,
        }
    }

    #[turso_test]
    fn parse_string_vector_zero_length() {
        let value = Value::from_text("[]");
        let vector = parse_string_vector(VectorType::Float32, &value).unwrap();
        assert_eq!(vector.dims, 0);
        assert_eq!(vector.vector_type, VectorType::Float32);
    }

    #[turso_test]
    fn test_parse_string_vector_valid_whitespace() {
        let value = Value::from_text("  [  1.0  ,  2.0  ,  3.0  ]  ");
        let vector = parse_string_vector(VectorType::Float32, &value).unwrap();
        assert_eq!(vector.dims, 3);
        assert_eq!(vector.vector_type, VectorType::Float32);
    }

    #[turso_test]
    fn test_parse_string_vector_valid() {
        let value = Value::from_text("[1.0, 2.0, 3.0]");
        let vector = parse_string_vector(VectorType::Float32, &value).unwrap();
        assert_eq!(vector.dims, 3);
        assert_eq!(vector.vector_type, VectorType::Float32);
    }

    fn float32_vec_from(slice: &[f32]) -> Vector {
        let mut data = Vec::new();
        for &v in slice {
            data.extend_from_slice(&v.to_le_bytes());
        }

        Vector {
            vector_type: VectorType::Float32,
            dims: slice.len(),
            data,
        }
    }

    fn f32_slice_from_vector(vector: &Vector) -> Vec<f32> {
        vector.as_f32_slice().to_vec()
    }

    #[turso_test]
    fn test_vector_concat_normal_case() {
        let v1 = float32_vec_from(&[1.0, 2.0, 3.0]);
        let v2 = float32_vec_from(&[4.0, 5.0, 6.0]);

        let result = vector_concat(&v1, &v2).unwrap();

        assert_eq!(result.dims, 6);
        assert_eq!(result.vector_type, VectorType::Float32);
        assert_eq!(
            f32_slice_from_vector(&result),
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        );
    }

    #[turso_test]
    fn test_vector_concat_empty_left() {
        let v1 = float32_vec_from(&[]);
        let v2 = float32_vec_from(&[4.0, 5.0]);

        let result = vector_concat(&v1, &v2).unwrap();

        assert_eq!(result.dims, 2);
        assert_eq!(f32_slice_from_vector(&result), vec![4.0, 5.0]);
    }

    #[turso_test]
    fn test_vector_concat_empty_right() {
        let v1 = float32_vec_from(&[1.0, 2.0]);
        let v2 = float32_vec_from(&[]);

        let result = vector_concat(&v1, &v2).unwrap();

        assert_eq!(result.dims, 2);
        assert_eq!(f32_slice_from_vector(&result), vec![1.0, 2.0]);
    }

    #[turso_test]
    fn test_vector_concat_both_empty() {
        let v1 = float32_vec_from(&[]);
        let v2 = float32_vec_from(&[]);
        let result = vector_concat(&v1, &v2).unwrap();
        assert_eq!(result.dims, 0);
        assert_eq!(f32_slice_from_vector(&result), Vec::<f32>::new());
    }

    #[turso_test]
    fn test_vector_concat_different_lengths() {
        let v1 = float32_vec_from(&[1.0]);
        let v2 = float32_vec_from(&[2.0, 3.0, 4.0]);

        let result = vector_concat(&v1, &v2).unwrap();

        assert_eq!(result.dims, 4);
        assert_eq!(f32_slice_from_vector(&result), vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[turso_test]
    fn test_vector_slice_normal_case() {
        let input_vec = float32_vec_from(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let result = vector_slice(&input_vec, 1, 4).unwrap();

        assert_eq!(result.dims, 3);
        assert_eq!(f32_slice_from_vector(&result), vec![2.0, 3.0, 4.0]);
    }

    #[turso_test]
    fn test_vector_slice_full_range() {
        let input_vec = float32_vec_from(&[10.0, 20.0, 30.0]);
        let result = vector_slice(&input_vec, 0, 3).unwrap();

        assert_eq!(result.dims, 3);
        assert_eq!(f32_slice_from_vector(&result), vec![10.0, 20.0, 30.0]);
    }

    #[turso_test]
    fn test_vector_slice_single_element() {
        let input_vec = float32_vec_from(&[4.40, 2.71]);
        let result = vector_slice(&input_vec, 1, 2).unwrap();

        assert_eq!(result.dims, 1);
        assert_eq!(f32_slice_from_vector(&result), vec![2.71]);
    }

    #[turso_test]
    fn test_vector_slice_empty_list() {
        let input_vec = float32_vec_from(&[1.0, 2.0]);
        let result = vector_slice(&input_vec, 2, 2).unwrap();

        assert_eq!(result.dims, 0);
    }

    #[turso_test]
    fn test_vector_slice_zero_length() {
        let input_vec = float32_vec_from(&[1.0, 2.0, 3.0]);
        let err = vector_slice(&input_vec, 2, 1);
        assert!(err.is_err(), "Expected error on zero-length range");
    }

    #[turso_test]
    fn test_vector_slice_out_of_bounds() {
        let input_vec = float32_vec_from(&[1.0, 2.0]);
        let err = vector_slice(&input_vec, 0, 5);
        assert!(err.is_err());
    }

    #[turso_test]
    fn test_vector_slice_start_out_of_bounds() {
        let input_vec = float32_vec_from(&[1.0, 2.0]);
        let err = vector_slice(&input_vec, 5, 5);
        assert!(err.is_err());
    }

    #[turso_test]
    fn test_vector_slice_end_out_of_bounds() {
        let input_vec = float32_vec_from(&[1.0, 2.0]);
        let err = vector_slice(&input_vec, 1, 3);
        assert!(err.is_err());
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_2d(v: ArbitraryVector<2>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_3d(v: ArbitraryVector<3>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_4d(v: ArbitraryVector<4>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_100d(v: ArbitraryVector<100>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_1536d(v: ArbitraryVector<1536>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    /// Test that a vector can be converted to text and back without loss of precision
    fn test_vector_text_roundtrip(v: Vector) -> bool {
        // Convert to text
        let text = vector_to_text(&v);

        // Parse back from text
        let value = Value::from_text(&text);
        let parsed = parse_string_vector(v.vector_type, &value);

        match parsed {
            Ok(parsed_vector) => {
                // Check dimensions match
                if v.dims != parsed_vector.dims {
                    return false;
                }

                match v.vector_type {
                    VectorType::Float32 => {
                        let original = v.as_f32_slice();
                        let parsed = parsed_vector.as_f32_slice();
                        original.iter().zip(parsed.iter()).all(|(a, b)| a == b)
                    }
                    VectorType::Float64 => {
                        let original = v.as_f64_slice();
                        let parsed = parsed_vector.as_f64_slice();
                        original.iter().zip(parsed.iter()).all(|(a, b)| a == b)
                    }
                }
            }
            Err(_) => false,
        }
    }
}
