use std::io;

use zstd::bulk::{compress_to_buffer, decompress_to_buffer};
use zstd::DEFAULT_COMPRESSION_LEVEL;

#[inline]
pub fn compress(
    uncompressed: &[u8],
    compressed: &mut Vec<u8>,
    compression_level: Option<i32>,
) -> io::Result<()> {
    let count_size = std::mem::size_of::<u32>();
    let max_size = zstd::zstd_safe::compress_bound(uncompressed.len()) + count_size;

    compressed.clear();
    compressed.resize(max_size, 0);

    let compressed_size = compress_to_buffer(
        uncompressed,
        &mut compressed[count_size..],
        compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL),
    )?;

    compressed[0..count_size].copy_from_slice(&(uncompressed.len() as u32).to_le_bytes());
    compressed.resize(compressed_size + count_size, 0);

    Ok(())
}

#[inline]
pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    let count_size = std::mem::size_of::<u32>();
    let uncompressed_size = u32::from_le_bytes(
        compressed
            .get(..count_size)
            .ok_or(io::ErrorKind::InvalidData)?
            .try_into()
            .unwrap(),
    ) as usize;

    decompressed.clear();
    decompressed.resize(uncompressed_size, 0);

    let decompressed_size = decompress_to_buffer(&compressed[count_size..], decompressed)?;

    if decompressed_size != uncompressed_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "doc store block not completely decompressed, data corruption".to_string(),
        ));
    }

    Ok(())
}
