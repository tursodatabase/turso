use crate::{LimboError, Result};

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum VectorType {
    Float32Dense,
    Float64Dense,
    Float32Sparse,
}

#[derive(Debug)]
pub struct Vector<'a> {
    pub vector_type: VectorType,
    pub dims: usize,
    pub owned: Option<Vec<u8>>,
    pub refer: Option<&'a [u8]>,
}

#[derive(Debug)]
pub struct VectorSparse<'a, T: std::fmt::Debug> {
    pub idx: &'a [u32],
    pub values: &'a [T],
}

impl<'a> Vector<'a> {
    pub fn vector_type(blob: &[u8]) -> Result<(VectorType, usize)> {
        // Even-sized blobs are always float32.
        if blob.len() % 2 == 0 {
            return Ok((VectorType::Float32Dense, blob.len()));
        }
        // Odd-sized blobs have type byte at the end
        let vector_type = blob[blob.len() - 1];
        /*
        vector types used by LibSQL:
        (see https://github.com/tursodatabase/libsql/blob/a55bf61192bdb89e97568de593c4af5b70d24bde/libsql-sqlite3/src/vectorInt.h#L52)
            #define VECTOR_TYPE_FLOAT32   1
            #define VECTOR_TYPE_FLOAT64   2
            #define VECTOR_TYPE_FLOAT1BIT 3
            #define VECTOR_TYPE_FLOAT8    4
            #define VECTOR_TYPE_FLOAT16   5
            #define VECTOR_TYPE_FLOATB16  6
        */
        match vector_type {
            1 => Ok((VectorType::Float32Dense, blob.len() - 1)),
            2 => Ok((VectorType::Float64Dense, blob.len() - 1)),
            3..=6 => Err(LimboError::ConversionError(
                "unsupported vector type from LibSQL".to_string(),
            )),
            9 => Ok((VectorType::Float32Sparse, blob.len() - 1)),
            _ => Err(LimboError::ConversionError(format!(
                "unknown vector type: {vector_type}"
            ))),
        }
    }
    pub fn from_f32(mut values_f32: Vec<f32>) -> Self {
        let dims = values_f32.len();
        let values = unsafe {
            Vec::from_raw_parts(
                values_f32.as_mut_ptr() as *mut u8,
                values_f32.len() * 4,
                values_f32.capacity() * 4,
            )
        };
        std::mem::forget(values_f32);
        Self {
            vector_type: VectorType::Float32Dense,
            dims,
            owned: Some(values),
            refer: None,
        }
    }
    pub fn from_f64(mut values_f64: Vec<f64>) -> Self {
        let dims = values_f64.len();
        let values = unsafe {
            Vec::from_raw_parts(
                values_f64.as_mut_ptr() as *mut u8,
                values_f64.len() * 8,
                values_f64.capacity() * 8,
            )
        };
        std::mem::forget(values_f64);
        Self {
            vector_type: VectorType::Float64Dense,
            dims,
            owned: Some(values),
            refer: None,
        }
    }
    pub fn from_f32_sparse(dims: usize, mut values_f32: Vec<f32>, mut idx_u32: Vec<u32>) -> Self {
        let mut values = unsafe {
            Vec::from_raw_parts(
                values_f32.as_mut_ptr() as *mut u8,
                values_f32.len() * 4,
                values_f32.capacity() * 4,
            )
        };
        std::mem::forget(values_f32);

        let idx = unsafe {
            Vec::from_raw_parts(
                idx_u32.as_mut_ptr() as *mut u8,
                idx_u32.len() * 4,
                idx_u32.capacity() * 4,
            )
        };
        std::mem::forget(idx_u32);

        values.extend_from_slice(&idx);
        Self {
            vector_type: VectorType::Float32Sparse,
            dims,
            owned: Some(values),
            refer: None,
        }
    }
    pub fn from_vec(mut blob: Vec<u8>) -> Result<Self> {
        let (vector_type, len) = Self::vector_type(&blob)?;
        blob.truncate(len);
        Self::from_data(vector_type, Some(blob), None)
    }
    pub fn from_slice(blob: &'a [u8]) -> Result<Self> {
        let (vector_type, len) = Self::vector_type(blob)?;
        Self::from_data(vector_type, None, Some(&blob[..len]))
    }
    pub fn from_data(
        vector_type: VectorType,
        owned: Option<Vec<u8>>,
        refer: Option<&'a [u8]>,
    ) -> Result<Self> {
        let owned_slice = owned.as_deref();
        let refer_slice = refer.as_ref().map(|&x| x);
        let data = owned_slice.unwrap_or_else(|| refer_slice.unwrap());
        match vector_type {
            VectorType::Float32Dense => {
                if data.len() % 4 != 0 {
                    return Err(LimboError::InvalidArgument(format!(
                        "f32 dense vector unexpected data length: {}",
                        data.len(),
                    )));
                }
                Ok(Vector {
                    vector_type,
                    dims: data.len() / 4,
                    owned,
                    refer,
                })
            }
            VectorType::Float64Dense => {
                if data.len() % 8 != 0 {
                    return Err(LimboError::InvalidArgument(format!(
                        "f64 dense vector unexpected data length: {}",
                        data.len(),
                    )));
                }
                Ok(Vector {
                    vector_type,
                    dims: data.len() / 8,
                    owned,
                    refer,
                })
            }
            VectorType::Float32Sparse => {
                if data.is_empty() || data.len() % 4 != 0 || (data.len() - 4) % 8 != 0 {
                    return Err(LimboError::InvalidArgument(format!(
                        "f32 sparse vector unexpected data length: {}",
                        data.len(),
                    )));
                }
                let original_len = data.len();
                let dims_bytes = &data[original_len - 4..];
                let dims = u32::from_le_bytes(dims_bytes.try_into().unwrap()) as usize;
                let owned = owned.map(|mut x| {
                    x.truncate(original_len - 4);
                    x
                });
                let refer = refer.map(|x| &x[0..original_len - 4]);
                let vector = Vector {
                    vector_type,
                    dims,
                    owned,
                    refer,
                };
                Ok(vector)
            }
        }
    }

    pub fn bin_len(&self) -> usize {
        let owned = self.owned.as_ref().map(|x| x.len());
        let refer = self.refer.as_ref().map(|x| x.len());
        owned.unwrap_or_else(|| refer.unwrap())
    }

    pub fn bin_data(&'a self) -> &'a [u8] {
        let owned = self.owned.as_deref();
        let refer = self.refer.as_ref().map(|&x| x);
        owned.unwrap_or_else(|| refer.unwrap())
    }

    pub fn bin_eject(self) -> Vec<u8> {
        self.owned.unwrap_or_else(|| self.refer.unwrap().to_vec())
    }

    /// # Safety
    ///
    /// This method is used to reinterpret the underlying `Vec<u8>` data
    /// as a `&[f32]` slice. This is only valid if:
    /// - The buffer is correctly aligned for `f32`
    /// - The length of the buffer is exactly `dims * size_of::<f32>()`
    pub fn as_f32_slice(&self) -> &[f32] {
        debug_assert!(self.vector_type == VectorType::Float32Dense);
        if self.dims == 0 {
            return &[];
        }

        assert_eq!(
            self.bin_len(),
            self.dims * std::mem::size_of::<f32>(),
            "data length must equal dims * size_of::<f32>()"
        );

        let ptr = self.bin_data().as_ptr();
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
        debug_assert!(self.vector_type == VectorType::Float64Dense);
        if self.dims == 0 {
            return &[];
        }

        assert_eq!(
            self.bin_len(),
            self.dims * std::mem::size_of::<f64>(),
            "data length must equal dims * size_of::<f64>()"
        );

        let ptr = self.bin_data().as_ptr();
        let align = std::mem::align_of::<f64>();
        assert_eq!(
            ptr.align_offset(align),
            0,
            "data pointer must be aligned to {align} bytes for f64 access"
        );

        unsafe { std::slice::from_raw_parts(ptr as *const f64, self.dims) }
    }

    pub fn as_f32_sparse(&self) -> VectorSparse<'_, f32> {
        debug_assert!(self.vector_type == VectorType::Float32Sparse);
        let ptr = self.bin_data().as_ptr();
        let align = std::mem::align_of::<f32>();
        assert_eq!(
            ptr.align_offset(align),
            0,
            "data pointer must be aligned to {align} bytes for f32 access"
        );
        let length = self.bin_data().len() / 4 / 2;
        let values = unsafe { std::slice::from_raw_parts(ptr as *const f32, length) };
        let idx = unsafe { std::slice::from_raw_parts((ptr as *const u32).add(length), length) };
        debug_assert!(idx.is_sorted());
        VectorSparse { idx, values }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::vector::operations;

    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    // Helper to generate arbitrary vectors of specific type and dimensions
    #[derive(Debug, Clone)]
    pub struct ArbitraryVector<const DIMS: usize> {
        vector_type: VectorType,
        data: Vec<u8>,
    }

    /// How to create an arbitrary vector of DIMS dims.
    impl<const DIMS: usize> ArbitraryVector<DIMS> {
        fn generate_f32_vector(g: &mut Gen) -> Vec<f32> {
            (0..DIMS)
                .map(|_| {
                    loop {
                        // generate zeroes with some probability since we have support for sparse vectors
                        if bool::arbitrary(g) {
                            return 0.0;
                        }
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
                        // generate zeroes with some probability since we have support for sparse vectors
                        if bool::arbitrary(g) {
                            return 0.0;
                        }
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
    impl<const DIMS: usize> From<ArbitraryVector<DIMS>> for Vector<'static> {
        fn from(v: ArbitraryVector<DIMS>) -> Self {
            Vector {
                vector_type: v.vector_type,
                dims: DIMS,
                owned: Some(v.data),
                refer: None,
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
                _ => unreachable!(),
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
        let blob = value.to_blob().unwrap().to_vec();
        match Vector::vector_type(&blob) {
            Ok((detected_type, _)) => detected_type == vtype,
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
                slice.len() == DIMS && (slice.len() * 4 == v.bin_len())
            }
            VectorType::Float64Dense => {
                let slice = v.as_f64_slice();
                // Check if the slice length matches the dimensions and the data length is correct (8 bytes per float)
                slice.len() == DIMS && (slice.len() * 8 == v.bin_len())
            }
            _ => unreachable!(),
        }
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
            Ok(distance) => distance.is_nan() || (0.0 - 1e-6..=2.0 + 1e-6).contains(&distance),
            Err(_) => true,
        }
    }

    #[test]
    fn test_vector_some_cosine_dist() {
        let a = Vector {
            vector_type: VectorType::Float32Dense,
            dims: 2,
            owned: Some(vec![0, 0, 0, 0, 52, 208, 106, 63]),
            refer: None,
        };
        let b = Vector {
            vector_type: VectorType::Float32Dense,
            dims: 2,
            owned: Some(vec![0, 0, 0, 0, 58, 100, 45, 192]),
            refer: None,
        };
        assert!(
            (operations::distance_cos::vector_distance_cos(&a, &b).unwrap() - 2.0).abs() <= 1e-6
        );
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
                    _ => unreachable!(),
                }
            }
            Err(_) => false,
        }
    }
}
