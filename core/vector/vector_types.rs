use crate::types::ValueType;
use crate::vdbe::Register;
use crate::vector::operations;
use crate::{LimboError, Result};

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum VectorType {
    Float32Dense,
    Float64Dense,
}

impl VectorType {
    pub fn size_to_dims(&self, size: usize) -> usize {
        match self {
            VectorType::Float32Dense => size / 4,
            VectorType::Float64Dense => size / 8,
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

pub fn parse_vector(value: &Register, vec_ty: Option<VectorType>) -> Result<Vector> {
    match value.get_value().value_type() {
        ValueType::Text => operations::text::vector_from_text(
            vec_ty.unwrap_or(VectorType::Float32Dense),
            value.get_value().to_text().expect("value must be text"),
        ),
        ValueType::Blob => {
            let Some(blob) = value.get_value().to_blob() else {
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

pub fn vector_deserialize(vector_type: VectorType, blob: &[u8]) -> Result<Vector> {
    match vector_type {
        VectorType::Float32Dense => vector_deserialize_f32(blob),
        VectorType::Float64Dense => vector_deserialize_f64(blob),
    }
}

pub fn vector_deserialize_f64(blob: &[u8]) -> Result<Vector> {
    Ok(Vector {
        vector_type: VectorType::Float64Dense,
        dims: (blob.len() - 1) / 8,
        data: blob[..blob.len() - 1].to_vec(),
    })
}

pub fn vector_deserialize_f32(blob: &[u8]) -> Result<Vector> {
    Ok(Vector {
        vector_type: VectorType::Float32Dense,
        dims: blob.len() / 4,
        data: blob.to_vec(),
    })
}

pub fn vector_type(blob: &[u8]) -> Result<VectorType> {
    // Even-sized blobs are always float32.
    if blob.len() % 2 == 0 {
        return Ok(VectorType::Float32Dense);
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
            Ok(VectorType::Float32Dense)
        }
        2 => {
            if data_blob.len() % 8 != 0 {
                return Err(LimboError::ConversionError(
                    "Invalid vector value".to_string(),
                ));
            }
            Ok(VectorType::Float64Dense)
        }
        _ => Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::vector::operations;

    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

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
                VectorType::Float32Dense
            } else {
                VectorType::Float64Dense
            };

            let data = match vector_type {
                VectorType::Float32Dense => {
                    let floats = Self::generate_f32_vector(g);
                    floats.iter().flat_map(|f| f.to_le_bytes()).collect()
                }
                VectorType::Float64Dense => {
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
        let value = operations::serialize::vector_serialize(v);
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
            VectorType::Float32Dense => {
                let slice = v.as_f32_slice();
                // Check if the slice length matches the dimensions and the data length is correct (4 bytes per float)
                slice.len() == DIMS && (slice.len() * 4 == v.data.len())
            }
            VectorType::Float64Dense => {
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
        match operations::distance_cos::vector_distance_cos(v1, v2) {
            Ok(distance) => (0.0..=2.0).contains(&distance),
            Err(_) => true,
        }
    }

    #[test]
    fn parse_string_vector_zero_length() {
        let vector = operations::text::vector_from_text(VectorType::Float32Dense, "[]").unwrap();
        assert_eq!(vector.dims, 0);
        assert_eq!(vector.vector_type, VectorType::Float32Dense);
    }

    #[test]
    fn test_parse_string_vector_valid_whitespace() {
        let vector = operations::text::vector_from_text(
            VectorType::Float32Dense,
            "  [  1.0  ,  2.0  ,  3.0  ]  ",
        )
        .unwrap();
        assert_eq!(vector.dims, 3);
        assert_eq!(vector.vector_type, VectorType::Float32Dense);
    }

    #[test]
    fn test_parse_string_vector_valid() {
        let vector =
            operations::text::vector_from_text(VectorType::Float32Dense, "[1.0, 2.0, 3.0]")
                .unwrap();
        assert_eq!(vector.dims, 3);
        assert_eq!(vector.vector_type, VectorType::Float32Dense);
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
        let text = operations::text::vector_to_text(&v);

        // Parse back from text
        let parsed = operations::text::vector_from_text(v.vector_type, &text);

        match parsed {
            Ok(parsed_vector) => {
                // Check dimensions match
                if v.dims != parsed_vector.dims {
                    return false;
                }

                match v.vector_type {
                    VectorType::Float32Dense => {
                        let original = v.as_f32_slice();
                        let parsed = parsed_vector.as_f32_slice();
                        original.iter().zip(parsed.iter()).all(|(a, b)| a == b)
                    }
                    VectorType::Float64Dense => {
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
