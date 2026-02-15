use crate::{
    vector::vector_types::{Vector, VectorType},
    Value,
};

pub fn vector_serialize(x: Vector) -> Value {
    match x.vector_type {
        VectorType::Float32Dense => Value::from_blob(x.bin_eject()),
        VectorType::Float64Dense => {
            let mut data = x.bin_eject();
            data.push(2);
            Value::from_blob(data)
        }
        VectorType::Float32Sparse => {
            let dims = x.dims;
            let mut data = x.bin_eject();
            data.extend_from_slice(&(dims as u32).to_le_bytes());
            data.push(9);
            Value::from_blob(data)
        }
        VectorType::Float1Bit => {
            // Format: [data bytes][optional padding][trailing_bits][0x03]
            let dims = x.dims;
            let data_size = dims.div_ceil(8);
            let needs_padding = data_size % 2 == 0;
            let meta_size = if needs_padding { 3 } else { 2 };
            let blob_size = data_size + meta_size;
            let mut blob = Vec::with_capacity(blob_size);
            let raw = x.bin_eject();
            blob.extend_from_slice(&raw[..data_size]);
            if needs_padding {
                blob.push(0); // padding
            }
            let trailing_bits = (blob_size - 1) * 8 - dims;
            blob.push(trailing_bits as u8);
            blob.push(3); // type byte
            Value::from_blob(blob)
        }
        VectorType::Float8 => {
            // Format: [quantized bytes][alignment padding][alpha f32][shift f32][padding 0x00][trailing_bytes][0x04]
            let dims = x.dims;
            let data = x.bin_eject(); // ALIGN(dims, 4) + 8 bytes
            let trailing_bytes = dims.div_ceil(4) * 4 - dims;
            let mut blob = Vec::with_capacity(data.len() + 3);
            blob.extend_from_slice(&data);
            blob.push(0); // padding
            blob.push(trailing_bytes as u8);
            blob.push(4); // type byte
            Value::from_blob(blob)
        }
    }
}
