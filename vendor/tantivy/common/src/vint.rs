use std::io;
use std::io::{Read, Write};

use super::BinarySerializable;

/// Variable int serializes a u128 number
pub fn serialize_vint_u128(mut val: u128, output: &mut Vec<u8>) {
    loop {
        let next_byte: u8 = (val % 128u128) as u8;
        val /= 128u128;
        if val == 0 {
            output.push(next_byte | STOP_BIT);
            return;
        } else {
            output.push(next_byte);
        }
    }
}

///   Wrapper over a `u128` that serializes as a variable int.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VIntU128(pub u128);

impl BinarySerializable for VIntU128 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        let mut buffer = vec![];
        serialize_vint_u128(self.0, &mut buffer);
        writer.write_all(&buffer)
    }

    #[allow(clippy::unbuffered_bytes)]
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        #[allow(clippy::unbuffered_bytes)]
        let mut bytes = reader.bytes();
        let mut result = 0u128;
        let mut shift = 0u64;
        loop {
            match bytes.next() {
                Some(Ok(b)) => {
                    result |= u128::from(b % 128u8) << shift;
                    if b >= STOP_BIT {
                        return Ok(VIntU128(result));
                    }
                    shift += 7;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Reach end of buffer while reading VInt",
                    ));
                }
            }
        }
    }
}

///   Wrapper over a `u64` that serializes as a variable int.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VInt(pub u64);

const STOP_BIT: u8 = 128;

#[inline]
pub fn serialize_vint_u32(val: u32, buf: &mut [u8; 8]) -> &[u8] {
    const START_2: u64 = 1 << 7;
    const START_3: u64 = 1 << 14;
    const START_4: u64 = 1 << 21;
    const START_5: u64 = 1 << 28;

    const MASK_1: u64 = 127;
    const MASK_2: u64 = MASK_1 << 7;
    const MASK_3: u64 = MASK_2 << 7;
    const MASK_4: u64 = MASK_3 << 7;
    const MASK_5: u64 = MASK_4 << 7;

    let val = u64::from(val);
    const STOP_BIT: u64 = 128u64;
    let (res, num_bytes) = if val < START_2 {
        (val | STOP_BIT, 1)
    } else if val < START_3 {
        (
            (val & MASK_1) | ((val & MASK_2) << 1) | (STOP_BIT << (8)),
            2,
        )
    } else if val < START_4 {
        (
            (val & MASK_1) | ((val & MASK_2) << 1) | ((val & MASK_3) << 2) | (STOP_BIT << (8 * 2)),
            3,
        )
    } else if val < START_5 {
        (
            (val & MASK_1)
                | ((val & MASK_2) << 1)
                | ((val & MASK_3) << 2)
                | ((val & MASK_4) << 3)
                | (STOP_BIT << (8 * 3)),
            4,
        )
    } else {
        (
            (val & MASK_1)
                | ((val & MASK_2) << 1)
                | ((val & MASK_3) << 2)
                | ((val & MASK_4) << 3)
                | ((val & MASK_5) << 4)
                | (STOP_BIT << (8 * 4)),
            5,
        )
    };
    *buf = res.to_le_bytes();
    &buf[0..num_bytes]
}

/// Returns the number of bytes covered by a
/// serialized vint `u32`.
///
/// Expects a buffer data that starts
/// by the serialized `vint`, scans at most 5 bytes ahead until
/// it finds the vint final byte.
///
/// # May Panic
/// If the payload does not start by a valid `vint`
fn vint_len(data: &[u8]) -> usize {
    for (i, &val) in data.iter().enumerate().take(5) {
        if val >= STOP_BIT {
            return i + 1;
        }
    }
    panic!("Corrupted data. Invalid VInt 32");
}

/// Reads a vint `u32` from a buffer, and
/// consumes its payload data.
///
/// # Panics
///
/// If the buffer does not start by a valid
/// vint payload
pub fn read_u32_vint(data: &mut &[u8]) -> u32 {
    let (result, vlen) = read_u32_vint_no_advance(data);
    *data = &data[vlen..];
    result
}

pub fn read_u32_vint_no_advance(data: &[u8]) -> (u32, usize) {
    let vlen = vint_len(data);
    let mut result = 0u32;
    let mut shift = 0u64;
    for &b in &data[..vlen] {
        result |= u32::from(b & 127u8) << shift;
        shift += 7;
    }
    (result, vlen)
}
/// Write a `u32` as a vint payload.
pub fn write_u32_vint<W: io::Write + ?Sized>(val: u32, writer: &mut W) -> io::Result<()> {
    let mut buf = [0u8; 8];
    let data = serialize_vint_u32(val, &mut buf);
    writer.write_all(data)
}

impl VInt {
    pub fn val(&self) -> u64 {
        self.0
    }

    pub fn deserialize_u64<R: Read>(reader: &mut R) -> io::Result<u64> {
        VInt::deserialize(reader).map(|vint| vint.0)
    }

    pub fn serialize_into_vec(&self, output: &mut Vec<u8>) {
        let mut buffer = [0u8; 10];
        let num_bytes = self.serialize_into(&mut buffer);
        output.extend(&buffer[0..num_bytes]);
    }

    pub fn serialize_into(&self, buffer: &mut [u8; 10]) -> usize {
        let mut remaining = self.0;
        for (i, b) in buffer.iter_mut().enumerate() {
            let next_byte: u8 = (remaining % 128u64) as u8;
            remaining /= 128u64;
            if remaining == 0u64 {
                *b = next_byte | STOP_BIT;
                return i + 1;
            } else {
                *b = next_byte;
            }
        }
        unreachable!();
    }
}

impl BinarySerializable for VInt {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        let mut buffer = [0u8; 10];
        let num_bytes = self.serialize_into(&mut buffer);
        writer.write_all(&buffer[0..num_bytes])
    }

    #[allow(clippy::unbuffered_bytes)]
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        #[allow(clippy::unbuffered_bytes)]
        let mut bytes = reader.bytes();
        let mut result = 0u64;
        let mut shift = 0u64;
        loop {
            match bytes.next() {
                Some(Ok(b)) => {
                    result |= u64::from(b % 128u8) << shift;
                    if b >= STOP_BIT {
                        return Ok(VInt(result));
                    }
                    shift += 7;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Reach end of buffer while reading VInt",
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{BinarySerializable, VInt, serialize_vint_u32};

    fn aux_test_vint(val: u64) {
        let mut v = [14u8; 10];
        let num_bytes = VInt(val).serialize_into(&mut v);
        for el in &v[num_bytes..10] {
            assert_eq!(el, &14u8);
        }
        assert!(num_bytes > 0);
        if num_bytes < 10 {
            assert!(1u64 << (7 * num_bytes) > val);
        }
        if num_bytes > 1 {
            assert!(1u64 << (7 * (num_bytes - 1)) <= val);
        }
        let serdeser_val = VInt::deserialize(&mut &v[..]).unwrap();
        assert_eq!(val, serdeser_val.0);
    }

    #[test]
    fn test_vint() {
        aux_test_vint(0);
        aux_test_vint(1);
        aux_test_vint(5);
        aux_test_vint(u64::MAX);
        for i in 1..9 {
            let power_of_128 = 1u64 << (7 * i);
            aux_test_vint(power_of_128 - 1u64);
            aux_test_vint(power_of_128);
            aux_test_vint(power_of_128 + 1u64);
        }
        aux_test_vint(10);
    }

    fn aux_test_serialize_vint_u32(val: u32) {
        let mut buffer = [0u8; 10];
        let mut buffer2 = [0u8; 8];
        let len_vint = VInt(val as u64).serialize_into(&mut buffer);
        let res2 = serialize_vint_u32(val, &mut buffer2);
        assert_eq!(&buffer[..len_vint], res2, "array wrong for {val}");
    }

    #[test]
    fn test_vint_u32() {
        aux_test_serialize_vint_u32(0);
        aux_test_serialize_vint_u32(1);
        aux_test_serialize_vint_u32(5);
        for i in 1..3 {
            let power_of_128 = 1u32 << (7 * i);
            aux_test_serialize_vint_u32(power_of_128 - 1u32);
            aux_test_serialize_vint_u32(power_of_128);
            aux_test_serialize_vint_u32(power_of_128 + 1u32);
        }
        aux_test_serialize_vint_u32(u32::MAX);
    }
}
