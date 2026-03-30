use crate::turso_debug_assert;
use crate::{LimboError, Result};

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum VectorType {
    Float32Dense,
    Float64Dense,
    Float32Sparse,
    Float1Bit,
    Float8,
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
    /// Returns (VectorType, data_length, dims) from a serialized blob.
    /// `data_length` is the number of bytes of actual vector data (before meta/type bytes).
    /// `dims` is the number of vector dimensions (only meaningful for Float1Bit/Float8 where
    /// it can't be inferred from data length alone; for other types it's set to 0 and
    /// computed later in from_data).
    pub fn vector_type(blob: &[u8]) -> Result<(VectorType, usize, usize)> {
        // Even-sized blobs are always float32.
        if blob.len() % 2 == 0 {
            return Ok((VectorType::Float32Dense, blob.len(), 0));
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
            1 => Ok((VectorType::Float32Dense, blob.len() - 1, 0)),
            2 => Ok((VectorType::Float64Dense, blob.len() - 1, 0)),
            3 => {
                // Float1Bit: [data bytes][optional padding][trailing_bits][0x03]
                let n_blob_size = blob.len() - 1; // without type byte
                if n_blob_size == 0 || n_blob_size % 2 != 0 {
                    return Err(LimboError::ConversionError(
                        "float1bit vector blob length must be even and non-empty".to_string(),
                    ));
                }
                let trailing_bits = blob[n_blob_size - 1] as usize;
                let dims = n_blob_size * 8 - trailing_bits;
                let data_size = dims.div_ceil(8);
                Ok((VectorType::Float1Bit, data_size, dims))
            }
            4 => {
                // Float8: [quantized bytes][alignment padding][alpha f32][shift f32][padding 0x00][trailing_bytes][0x04]
                let n_blob_size = blob.len() - 1; // without type byte
                if n_blob_size < 2 || n_blob_size % 2 != 0 {
                    return Err(LimboError::ConversionError(
                        "float8 vector blob must have even length >= 2 (excluding type byte)"
                            .to_string(),
                    ));
                }
                let trailing_bytes = blob[n_blob_size - 1] as usize;
                let dims = (n_blob_size - 2) - 8 - trailing_bytes;
                // data_size = ALIGN(dims, 4) + 8
                let data_size = n_blob_size - 2;
                Ok((VectorType::Float8, data_size, dims))
            }
            5..=6 => Err(LimboError::ConversionError(
                "unsupported vector type from LibSQL".to_string(),
            )),
            9 => Ok((VectorType::Float32Sparse, blob.len() - 1, 0)),
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
    fn align4(n: usize) -> usize {
        n.div_ceil(4) * 4
    }

    pub fn from_1bit(dims: usize, bits: Vec<u8>) -> Self {
        debug_assert!(bits.len() == dims.div_ceil(8));
        Self {
            vector_type: VectorType::Float1Bit,
            dims,
            owned: Some(bits),
            refer: None,
        }
    }

    pub fn from_f8(dims: usize, quantized: Vec<u8>, alpha: f32, shift: f32) -> Self {
        let aligned = Self::align4(dims);
        let mut data = Vec::with_capacity(aligned + 8);
        data.extend_from_slice(&quantized);
        data.resize(aligned, 0); // alignment padding
        data.extend_from_slice(&alpha.to_le_bytes());
        data.extend_from_slice(&shift.to_le_bytes());
        debug_assert!(data.len() == aligned + 8);
        Self {
            vector_type: VectorType::Float8,
            dims,
            owned: Some(data),
            refer: None,
        }
    }

    pub fn from_vec(mut blob: Vec<u8>) -> Result<Self> {
        let (vector_type, len, explicit_dims) = Self::vector_type(&blob)?;
        blob.truncate(len);
        Self::from_data_with_dims(vector_type, Some(blob), None, explicit_dims)
    }
    pub fn from_slice(blob: &'a [u8]) -> Result<Self> {
        let (vector_type, len, explicit_dims) = Self::vector_type(blob)?;
        Self::from_data_with_dims(vector_type, None, Some(&blob[..len]), explicit_dims)
    }
    pub fn from_data(
        vector_type: VectorType,
        owned: Option<Vec<u8>>,
        refer: Option<&'a [u8]>,
    ) -> Result<Self> {
        Self::from_data_with_dims(vector_type, owned, refer, 0)
    }

    fn from_data_with_dims(
        vector_type: VectorType,
        owned: Option<Vec<u8>>,
        refer: Option<&'a [u8]>,
        explicit_dims: usize,
    ) -> Result<Self> {
        let owned_slice = owned.as_deref();
        let refer_slice = refer.as_ref().map(|&x| x);
        let data = owned_slice.or(refer_slice).ok_or_else(|| {
            LimboError::InternalError("Vector must have either owned or refer data".to_string())
        })?;
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
                let dims = u32::from_le_bytes([
                    dims_bytes[0],
                    dims_bytes[1],
                    dims_bytes[2],
                    dims_bytes[3],
                ]) as usize;
                let owned = owned.map(|mut x| {
                    x.truncate(original_len - 4);
                    x
                });
                let refer = refer.map(|x| &x[0..original_len - 4]);
                Ok(Vector {
                    vector_type,
                    dims,
                    owned,
                    refer,
                })
            }
            VectorType::Float1Bit => {
                let expected_len = explicit_dims.div_ceil(8);
                if explicit_dims == 0 || data.len() != expected_len {
                    return Err(LimboError::InvalidArgument(format!(
                        "f1bit vector data length mismatch: got {} expected {} for {} dims",
                        data.len(),
                        expected_len,
                        explicit_dims,
                    )));
                }
                Ok(Vector {
                    vector_type,
                    dims: explicit_dims,
                    owned,
                    refer,
                })
            }
            VectorType::Float8 => {
                if data.len() < 8 {
                    return Err(LimboError::InvalidArgument(format!(
                        "f8 vector data too short: {}",
                        data.len(),
                    )));
                }
                let expected_len = Self::align4(explicit_dims) + 8;
                if explicit_dims == 0 || data.len() != expected_len {
                    return Err(LimboError::InvalidArgument(format!(
                        "f8 vector data length mismatch: got {} expected {} for {} dims",
                        data.len(),
                        expected_len,
                        explicit_dims,
                    )));
                }
                Ok(Vector {
                    vector_type,
                    dims: explicit_dims,
                    owned,
                    refer,
                })
            }
        }
    }

    pub fn bin_len(&self) -> usize {
        let owned = self.owned.as_ref().map(|x| x.len());
        let refer = self.refer.as_ref().map(|x| x.len());
        owned
            .or(refer)
            .expect("Vector invariant: exactly one of owned or refer must be Some")
    }

    pub fn bin_data(&'a self) -> &'a [u8] {
        let owned = self.owned.as_deref();
        let refer = self.refer.as_ref().map(|&x| x);
        owned
            .or(refer)
            .expect("Vector invariant: exactly one of owned or refer must be Some")
    }

    pub fn bin_eject(self) -> Vec<u8> {
        self.owned.unwrap_or_else(|| {
            self.refer
                .expect("Vector invariant: exactly one of owned or refer must be Some")
                .to_vec()
        })
    }

    /// # Safety
    ///
    /// This method is used to reinterpret the underlying `Vec<u8>` data
    /// as a `&[f32]` slice. This is only valid if:
    /// - The buffer is correctly aligned for `f32`
    /// - The length of the buffer is exactly `dims * size_of::<f32>()`
    pub fn as_f32_slice(&self) -> &[f32] {
        turso_debug_assert!(self.vector_type == VectorType::Float32Dense);
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
        turso_debug_assert!(self.vector_type == VectorType::Float64Dense);
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
        turso_debug_assert!(self.vector_type == VectorType::Float32Sparse);
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
        turso_debug_assert!(idx.is_sorted());
        VectorSparse { idx, values }
    }

    /// Returns the raw bit-packed bytes for a Float1Bit vector.
    /// Bit `i` is at byte `i/8`, position `i & 7`.
    pub fn as_1bit_data(&self) -> &[u8] {
        debug_assert!(self.vector_type == VectorType::Float1Bit);
        let data = self.bin_data();
        &data[..self.dims.div_ceil(8)]
    }

    /// Returns (quantized_bytes, alpha, shift) for a Float8 vector.
    /// Dequantization: `f_i = alpha * q_i + shift`
    pub fn as_f8_data(&self) -> (&[u8], f32, f32) {
        debug_assert!(self.vector_type == VectorType::Float8);
        let data = self.bin_data();
        let aligned = Self::align4(self.dims);
        let alpha = f32::from_le_bytes([
            data[aligned],
            data[aligned + 1],
            data[aligned + 2],
            data[aligned + 3],
        ]);
        let shift = f32::from_le_bytes([
            data[aligned + 4],
            data[aligned + 5],
            data[aligned + 6],
            data[aligned + 7],
        ]);
        (&data[..self.dims], alpha, shift)
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
            let choice = u8::arbitrary(g) % 4;
            let vector_type = match choice {
                0 => VectorType::Float32Dense,
                1 => VectorType::Float64Dense,
                2 => VectorType::Float1Bit,
                _ => VectorType::Float8,
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
                VectorType::Float1Bit => {
                    // Generate random bits
                    let byte_count = DIMS.div_ceil(8);
                    let mut bits = vec![0u8; byte_count];
                    for b in bits.iter_mut() {
                        *b = u8::arbitrary(g);
                    }
                    // Mask off unused bits in the last byte
                    if DIMS % 8 != 0 {
                        let mask = (1u8 << (DIMS % 8)) - 1;
                        if let Some(last) = bits.last_mut() {
                            *last &= mask;
                        }
                    }
                    bits
                }
                VectorType::Float8 => {
                    // Generate random quantized values + alpha/shift
                    let floats = Self::generate_f32_vector(g);
                    let min_val = floats.iter().cloned().fold(f32::INFINITY, f32::min);
                    let max_val = floats.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
                    let alpha = (max_val - min_val) / 255.0;
                    let shift = min_val;
                    let aligned = DIMS.div_ceil(4) * 4;
                    let mut data = Vec::with_capacity(aligned + 8);
                    for &f in &floats {
                        let q = if alpha == 0.0 {
                            0u8
                        } else {
                            ((f - shift) / alpha + 0.5) as u8
                        };
                        data.push(q);
                    }
                    data.resize(aligned, 0); // alignment padding
                    data.extend_from_slice(&alpha.to_le_bytes());
                    data.extend_from_slice(&shift.to_le_bytes());
                    data
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
            Ok((detected_type, _, _)) => detected_type == vtype,
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
                slice.len() == DIMS && (slice.len() * 4 == v.bin_len())
            }
            VectorType::Float64Dense => {
                let slice = v.as_f64_slice();
                slice.len() == DIMS && (slice.len() * 8 == v.bin_len())
            }
            VectorType::Float1Bit => {
                let data = v.as_1bit_data();
                data.len() == DIMS.div_ceil(8) && v.dims == DIMS
            }
            VectorType::Float8 => {
                let (quantized, _alpha, _shift) = v.as_f8_data();
                quantized.len() == DIMS && v.dims == DIMS
            }
            _ => true,
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
    fn test_vector_distance<const DIMS: usize>(v1: &Vector, v2: &Vector) -> bool {
        match operations::distance_cos::vector_distance_cos(v1, v2) {
            Ok(distance) => {
                if distance.is_nan() {
                    return true;
                }
                match v1.vector_type {
                    // Float1Bit cosine distance returns hamming distance [0, dims]
                    VectorType::Float1Bit => (0.0 - 1e-6..=DIMS as f64 + 1e-6).contains(&distance),
                    // Normal cosine distance [0, 2]
                    _ => (0.0 - 1e-6..=2.0 + 1e-6).contains(&distance),
                }
            }
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
    fn parse_string_vector_f8_zero_length() {
        let vector = operations::text::vector_from_text(VectorType::Float8, "[]").unwrap();
        assert_eq!(vector.dims, 0);
        assert_eq!(vector.vector_type, VectorType::Float8);
        let (quantized, alpha, shift) = vector.as_f8_data();
        assert!(quantized.is_empty());
        assert_eq!(alpha, 0.0);
        assert_eq!(shift, 0.0);
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
                    VectorType::Float1Bit => {
                        // 1bit text roundtrip: bits → +1/-1 text → threshold → bits
                        let original = v.as_1bit_data();
                        let parsed_data = parsed_vector.as_1bit_data();
                        original == parsed_data
                    }
                    VectorType::Float8 => {
                        // Float8 text roundtrip: lossy due to quantization
                        // Just check dims match (re-quantization changes alpha/shift)
                        true
                    }
                    _ => true,
                }
            }
            Err(_) => false,
        }
    }
}
