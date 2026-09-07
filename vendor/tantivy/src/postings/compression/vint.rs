#[inline]
pub fn compress_sorted<'a>(input: &[u32], output: &'a mut [u8], mut offset: u32) -> &'a [u8] {
    let mut byte_written = 0;
    for &v in input {
        let mut to_encode: u32 = v - offset;
        offset = v;
        loop {
            let next_byte: u8 = (to_encode % 128u32) as u8;
            to_encode /= 128u32;
            if to_encode == 0u32 {
                output[byte_written] = next_byte | 128u8;
                byte_written += 1;
                break;
            } else {
                output[byte_written] = next_byte;
                byte_written += 1;
            }
        }
    }
    &output[..byte_written]
}

#[inline]
pub(crate) fn compress_unsorted<'a>(input: &[u32], output: &'a mut [u8]) -> &'a [u8] {
    let mut byte_written = 0;
    for &v in input {
        let mut to_encode: u32 = v;
        loop {
            let next_byte: u8 = (to_encode % 128u32) as u8;
            to_encode /= 128u32;
            if to_encode == 0u32 {
                output[byte_written] = next_byte | 128u8;
                byte_written += 1;
                break;
            } else {
                output[byte_written] = next_byte;
                byte_written += 1;
            }
        }
    }
    &output[..byte_written]
}

#[inline]
pub fn uncompress_sorted(compressed_data: &[u8], output: &mut [u32], offset: u32) -> usize {
    let mut read_byte = 0;
    let mut result = offset;
    for output_mut in output.iter_mut() {
        let mut shift = 0u32;
        loop {
            let cur_byte = compressed_data[read_byte];
            read_byte += 1;
            result += u32::from(cur_byte % 128u8) << shift;
            if cur_byte & 128u8 != 0u8 {
                break;
            }
            shift += 7;
        }
        *output_mut = result;
    }
    read_byte
}

#[inline]
pub(crate) fn uncompress_unsorted(compressed_data: &[u8], output_arr: &mut [u32]) -> usize {
    let mut num_read_bytes = 0;
    for output_mut in output_arr.iter_mut() {
        let mut result = 0u32;
        let mut shift = 0u32;
        loop {
            let cur_byte = compressed_data[num_read_bytes];
            num_read_bytes += 1;
            result += u32::from(cur_byte % 128u8) << shift;
            if cur_byte & 128u8 != 0u8 {
                break;
            }
            shift += 7;
        }
        *output_mut = result;
    }
    num_read_bytes
}

#[inline]
pub(crate) fn uncompress_unsorted_until_end(
    compressed_data: &[u8],
    output_arr: &mut [u32],
) -> usize {
    let mut num_read_bytes = 0;
    for (num_ints_written, output_mut) in output_arr.iter_mut().enumerate() {
        if compressed_data.len() == num_read_bytes {
            return num_ints_written;
        }
        let mut result = 0u32;
        let mut shift = 0u32;
        loop {
            let cur_byte = compressed_data[num_read_bytes];
            num_read_bytes += 1;
            result += u32::from(cur_byte % 128u8) << shift;
            if cur_byte & 128u8 != 0u8 {
                break;
            }
            shift += 7;
        }
        *output_mut = result;
    }
    output_arr.len()
}
