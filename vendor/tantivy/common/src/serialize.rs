use std::borrow::Cow;
use std::io::{Read, Write};
use std::{fmt, io};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{Endianness, VInt};

#[derive(Default)]
struct Counter(u64);

impl io::Write for Counter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0 += buf.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Trait for a simple binary serialization.
pub trait BinarySerializable: fmt::Debug + Sized {
    /// Serialize
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()>;
    /// Deserialize
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self>;

    fn num_bytes(&self) -> u64 {
        let mut counter = Counter::default();
        self.serialize(&mut counter).unwrap();
        counter.0
    }
}

pub trait DeserializeFrom<T: BinarySerializable> {
    fn deserialize(&mut self) -> io::Result<T>;
}

/// Implement deserialize from &[u8] for all types which implement BinarySerializable.
///
/// TryFrom would actually be preferable, but not possible because of the orphan
/// rules (not completely sure if this could be resolved)
impl<T: BinarySerializable> DeserializeFrom<T> for &[u8] {
    fn deserialize(&mut self) -> io::Result<T> {
        T::deserialize(self)
    }
}

/// `FixedSize` marks a `BinarySerializable` as
/// always serializing to the same size.
pub trait FixedSize: BinarySerializable {
    const SIZE_IN_BYTES: usize;
}

impl BinarySerializable for () {
    fn serialize<W: Write + ?Sized>(&self, _: &mut W) -> io::Result<()> {
        Ok(())
    }
    fn deserialize<R: Read>(_: &mut R) -> io::Result<Self> {
        Ok(())
    }
}

impl FixedSize for () {
    const SIZE_IN_BYTES: usize = 0;
}

impl<T: BinarySerializable> BinarySerializable for Vec<T> {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        BinarySerializable::serialize(&VInt(self.len() as u64), writer)?;
        for it in self {
            it.serialize(writer)?;
        }
        Ok(())
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Vec<T>> {
        let num_items = <VInt as BinarySerializable>::deserialize(reader)?.val();
        let mut items: Vec<T> = Vec::with_capacity(num_items as usize);
        for _ in 0..num_items {
            let item = T::deserialize(reader)?;
            items.push(item);
        }
        Ok(items)
    }
}

impl<Left: BinarySerializable, Right: BinarySerializable> BinarySerializable for (Left, Right) {
    fn serialize<W: Write + ?Sized>(&self, write: &mut W) -> io::Result<()> {
        self.0.serialize(write)?;
        self.1.serialize(write)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        Ok((Left::deserialize(reader)?, Right::deserialize(reader)?))
    }
}
impl<Left: BinarySerializable + FixedSize, Right: BinarySerializable + FixedSize> FixedSize
    for (Left, Right)
{
    const SIZE_IN_BYTES: usize = Left::SIZE_IN_BYTES + Right::SIZE_IN_BYTES;
}

impl BinarySerializable for u32 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32::<Endianness>(*self)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<u32> {
        reader.read_u32::<Endianness>()
    }
}

impl FixedSize for u32 {
    const SIZE_IN_BYTES: usize = 4;
}

impl BinarySerializable for u16 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u16::<Endianness>(*self)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<u16> {
        reader.read_u16::<Endianness>()
    }
}

impl FixedSize for u16 {
    const SIZE_IN_BYTES: usize = 2;
}

impl BinarySerializable for u64 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u64::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_u64::<Endianness>()
    }
}

impl FixedSize for u64 {
    const SIZE_IN_BYTES: usize = 8;
}

impl BinarySerializable for u128 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u128::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_u128::<Endianness>()
    }
}

impl FixedSize for u128 {
    const SIZE_IN_BYTES: usize = 16;
}

impl BinarySerializable for f32 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_f32::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_f32::<Endianness>()
    }
}

impl FixedSize for f32 {
    const SIZE_IN_BYTES: usize = 4;
}

impl BinarySerializable for i64 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_i64::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_i64::<Endianness>()
    }
}

impl FixedSize for i64 {
    const SIZE_IN_BYTES: usize = 8;
}

impl BinarySerializable for f64 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_f64::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_f64::<Endianness>()
    }
}

impl FixedSize for f64 {
    const SIZE_IN_BYTES: usize = 8;
}

impl BinarySerializable for u8 {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<u8> {
        reader.read_u8()
    }
}

impl FixedSize for u8 {
    const SIZE_IN_BYTES: usize = 1;
}

impl BinarySerializable for bool {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(u8::from(*self))
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<bool> {
        let val = reader.read_u8()?;
        match val {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid bool value on deserialization, data corrupted",
            )),
        }
    }
}

impl FixedSize for bool {
    const SIZE_IN_BYTES: usize = 1;
}

impl BinarySerializable for String {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        let data: &[u8] = self.as_bytes();
        BinarySerializable::serialize(&VInt(data.len() as u64), writer)?;
        writer.write_all(data)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<String> {
        let string_length = <VInt as BinarySerializable>::deserialize(reader)?.val() as usize;
        let mut result = String::with_capacity(string_length);
        reader
            .take(string_length as u64)
            .read_to_string(&mut result)?;
        Ok(result)
    }
}

impl<'a> BinarySerializable for Cow<'a, str> {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        let data: &[u8] = self.as_bytes();
        BinarySerializable::serialize(&VInt(data.len() as u64), writer)?;
        writer.write_all(data)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Cow<'a, str>> {
        let string_length = <VInt as BinarySerializable>::deserialize(reader)?.val() as usize;
        let mut result = String::with_capacity(string_length);
        reader
            .take(string_length as u64)
            .read_to_string(&mut result)?;
        Ok(Cow::Owned(result))
    }
}

impl<'a> BinarySerializable for Cow<'a, [u8]> {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        BinarySerializable::serialize(&VInt(self.len() as u64), writer)?;
        for it in self.iter() {
            BinarySerializable::serialize(it, writer)?;
        }
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Cow<'a, [u8]>> {
        let num_items = <VInt as BinarySerializable>::deserialize(reader)?.val();
        let mut items: Vec<u8> = Vec::with_capacity(num_items as usize);
        for _ in 0..num_items {
            let item = <u8 as BinarySerializable>::deserialize(reader)?;
            items.push(item);
        }
        Ok(Cow::Owned(items))
    }
}

#[cfg(test)]
pub mod test {

    use super::*;
    pub fn fixed_size_test<O: BinarySerializable + FixedSize + Default>() {
        let mut buffer = Vec::new();
        O::default().serialize(&mut buffer).unwrap();
        assert_eq!(buffer.len(), O::SIZE_IN_BYTES);
    }

    fn serialize_test<T: BinarySerializable + Eq>(v: T) -> usize {
        let mut buffer: Vec<u8> = Vec::new();
        v.serialize(&mut buffer).unwrap();
        let num_bytes = buffer.len();
        let mut cursor = &buffer[..];
        let deser = T::deserialize(&mut cursor).unwrap();
        assert_eq!(deser, v);
        num_bytes
    }

    #[test]
    fn test_serialize_u8() {
        fixed_size_test::<u8>();
    }

    #[test]
    fn test_serialize_u32() {
        fixed_size_test::<u32>();
        assert_eq!(4, serialize_test(3u32));
        assert_eq!(4, serialize_test(5u32));
        assert_eq!(4, serialize_test(u32::MAX));
    }

    #[test]
    fn test_serialize_i64() {
        fixed_size_test::<i64>();
    }

    #[test]
    fn test_serialize_f64() {
        fixed_size_test::<f64>();
    }

    #[test]
    fn test_serialize_u64() {
        fixed_size_test::<u64>();
    }

    #[test]
    fn test_serialize_bool() {
        fixed_size_test::<bool>();
    }

    #[test]
    fn test_serialize_string() {
        assert_eq!(serialize_test(String::from("")), 1);
        assert_eq!(serialize_test(String::from("ぽよぽよ")), 1 + 3 * 4);
        assert_eq!(serialize_test(String::from("富士さん見える。")), 1 + 3 * 8);
    }

    #[test]
    fn test_serialize_vec() {
        assert_eq!(serialize_test(Vec::<u8>::new()), 1);
        assert_eq!(serialize_test(vec![1u32, 3u32]), 1 + 4 * 2);
    }

    #[test]
    fn test_serialize_vint() {
        for i in 0..10_000 {
            serialize_test(VInt(i as u64));
        }
        assert_eq!(serialize_test(VInt(7u64)), 1);
        assert_eq!(serialize_test(VInt(127u64)), 1);
        assert_eq!(serialize_test(VInt(128u64)), 2);
        assert_eq!(serialize_test(VInt(129u64)), 2);
        assert_eq!(serialize_test(VInt(1234u64)), 2);
        assert_eq!(serialize_test(VInt(16_383u64)), 2);
        assert_eq!(serialize_test(VInt(16_384u64)), 3);
        assert_eq!(serialize_test(VInt(u64::MAX)), 10);
    }
}
