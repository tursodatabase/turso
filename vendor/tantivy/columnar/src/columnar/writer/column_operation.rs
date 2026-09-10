use std::net::Ipv6Addr;

use crate::dictionary::UnorderedId;
use crate::utils::{place_bits, pop_first_byte, select_bits};
use crate::value::NumericalValue;
use crate::{InvalidData, NumericalType, RowId};

/// When we build a columnar dataframe, we first just group
/// all mutations per column, and appends them in append-only buffer
/// in the stacker.
///
/// These ColumnOperation<T> are therefore serialize/deserialized
/// in memory.
///
/// We represents all of these operations as `ColumnOperation`.
#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub(super) enum ColumnOperation<T> {
    NewDoc(RowId),
    Value(T),
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct ColumnOperationMetadata {
    op_type: ColumnOperationType,
    len: u8,
}

impl ColumnOperationMetadata {
    fn to_code(self) -> u8 {
        place_bits::<0, 6>(self.len) | place_bits::<6, 8>(self.op_type.to_code())
    }

    fn try_from_code(code: u8) -> Result<Self, InvalidData> {
        let len = select_bits::<0, 6>(code);
        let typ_code = select_bits::<6, 8>(code);
        let column_type = ColumnOperationType::try_from_code(typ_code)?;
        Ok(ColumnOperationMetadata {
            op_type: column_type,
            len,
        })
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[repr(u8)]
enum ColumnOperationType {
    NewDoc = 0u8,
    AddValue = 1u8,
}

impl ColumnOperationType {
    pub fn to_code(self) -> u8 {
        self as u8
    }

    pub fn try_from_code(code: u8) -> Result<Self, InvalidData> {
        match code {
            0 => Ok(Self::NewDoc),
            1 => Ok(Self::AddValue),
            _ => Err(InvalidData),
        }
    }
}

impl<V: SymbolValue> ColumnOperation<V> {
    pub(super) fn serialize(self) -> impl AsRef<[u8]> {
        let mut minibuf = MiniBuffer::default();
        let column_op_metadata = match self {
            ColumnOperation::NewDoc(new_doc) => {
                let symbol_len = new_doc.serialize(&mut minibuf.bytes[1..]);
                ColumnOperationMetadata {
                    op_type: ColumnOperationType::NewDoc,
                    len: symbol_len,
                }
            }
            ColumnOperation::Value(val) => {
                let symbol_len = val.serialize(&mut minibuf.bytes[1..]);
                ColumnOperationMetadata {
                    op_type: ColumnOperationType::AddValue,
                    len: symbol_len,
                }
            }
        };
        minibuf.bytes[0] = column_op_metadata.to_code();
        // +1 for the metadata
        minibuf.len = 1 + column_op_metadata.len;
        minibuf
    }

    /// Deserialize a column operation.
    /// Returns None if the buffer is empty.
    ///
    /// Panics if the payload is invalid:
    /// this deserialize method is meant to target in memory.
    pub(super) fn deserialize(bytes: &mut &[u8]) -> Option<Self> {
        let column_op_metadata_byte = pop_first_byte(bytes)?;
        let column_op_metadata = ColumnOperationMetadata::try_from_code(column_op_metadata_byte)
            .expect("Invalid op metadata byte");
        let symbol_bytes: &[u8];
        (symbol_bytes, *bytes) = bytes.split_at(column_op_metadata.len as usize);
        match column_op_metadata.op_type {
            ColumnOperationType::NewDoc => {
                let new_doc = u32::deserialize(symbol_bytes);
                Some(ColumnOperation::NewDoc(new_doc))
            }
            ColumnOperationType::AddValue => {
                let value = V::deserialize(symbol_bytes);
                Some(ColumnOperation::Value(value))
            }
        }
    }
}

impl<T> From<T> for ColumnOperation<T> {
    fn from(value: T) -> Self {
        ColumnOperation::Value(value)
    }
}

// Serialization trait very local to the writer.
// As we write fast fields, we accumulate them in "in memory".
// In order to limit memory usage, and in order
// to benefit from the stacker, we do this by serialization our data
// as "Symbols".
pub(super) trait SymbolValue: Clone + Copy {
    // Serializes the symbol into the given buffer.
    // Returns the number of bytes written into the buffer.
    /// # Panics
    /// May not exceed 9bytes
    fn serialize(self, buffer: &mut [u8]) -> u8;
    // Panics if invalid
    fn deserialize(bytes: &[u8]) -> Self;
}

impl SymbolValue for bool {
    fn serialize(self, buffer: &mut [u8]) -> u8 {
        buffer[0] = u8::from(self);
        1u8
    }

    fn deserialize(bytes: &[u8]) -> Self {
        bytes[0] == 1u8
    }
}

impl SymbolValue for Ipv6Addr {
    fn serialize(self, buffer: &mut [u8]) -> u8 {
        buffer[0..16].copy_from_slice(&self.octets());
        16
    }

    fn deserialize(bytes: &[u8]) -> Self {
        let octets: [u8; 16] = bytes[0..16].try_into().unwrap();
        Ipv6Addr::from(octets)
    }
}

#[derive(Default)]
struct MiniBuffer {
    pub bytes: [u8; 17],
    pub len: u8,
}

impl AsRef<[u8]> for MiniBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

impl SymbolValue for NumericalValue {
    fn deserialize(mut bytes: &[u8]) -> Self {
        let type_code = pop_first_byte(&mut bytes).unwrap();
        let symbol_type = NumericalType::try_from_code(type_code).unwrap();
        let mut octet: [u8; 8] = [0u8; 8];
        octet[..bytes.len()].copy_from_slice(bytes);
        match symbol_type {
            NumericalType::U64 => {
                let val: u64 = u64::from_le_bytes(octet);
                NumericalValue::U64(val)
            }
            NumericalType::I64 => {
                let encoded: u64 = u64::from_le_bytes(octet);
                let val: i64 = decode_zig_zag(encoded);
                NumericalValue::I64(val)
            }
            NumericalType::F64 => {
                debug_assert_eq!(bytes.len(), 8);
                let val: f64 = f64::from_le_bytes(octet);
                NumericalValue::F64(val)
            }
        }
    }

    /// F64: Serialize with a fixed size of 9 bytes
    /// U64: Serialize without leading zeroes
    /// I64: ZigZag encoded and serialize without leading zeroes
    fn serialize(self, output: &mut [u8]) -> u8 {
        match self {
            NumericalValue::F64(val) => {
                output[0] = NumericalType::F64 as u8;
                output[1..9].copy_from_slice(&val.to_le_bytes());
                9u8
            }
            NumericalValue::U64(val) => {
                let len = compute_num_bytes_for_u64(val) as u8;
                output[0] = NumericalType::U64 as u8;
                output[1..9].copy_from_slice(&val.to_le_bytes());
                len + 1u8
            }
            NumericalValue::I64(val) => {
                let zig_zag_encoded = encode_zig_zag(val);
                let len = compute_num_bytes_for_u64(zig_zag_encoded) as u8;
                output[0] = NumericalType::I64 as u8;
                output[1..9].copy_from_slice(&zig_zag_encoded.to_le_bytes());
                len + 1u8
            }
        }
    }
}

impl SymbolValue for u32 {
    fn serialize(self, output: &mut [u8]) -> u8 {
        let len = compute_num_bytes_for_u64(self as u64);
        output[0..4].copy_from_slice(&self.to_le_bytes());
        len as u8
    }

    fn deserialize(bytes: &[u8]) -> Self {
        let mut quartet: [u8; 4] = [0u8; 4];
        quartet[..bytes.len()].copy_from_slice(bytes);
        u32::from_le_bytes(quartet)
    }
}

impl SymbolValue for UnorderedId {
    fn serialize(self, output: &mut [u8]) -> u8 {
        self.0.serialize(output)
    }

    fn deserialize(bytes: &[u8]) -> Self {
        UnorderedId(u32::deserialize(bytes))
    }
}

fn compute_num_bytes_for_u64(val: u64) -> usize {
    let msb = (64u32 - val.leading_zeros()) as usize;
    msb.div_ceil(8)
}

fn encode_zig_zag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn decode_zig_zag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn test_zig_zag_aux(val: i64) {
        let encoded = super::encode_zig_zag(val);
        assert_eq!(decode_zig_zag(encoded), val);
        if let Some(abs_val) = val.checked_abs() {
            let abs_val = abs_val as u64;
            assert!(encoded <= abs_val * 2);
        }
    }

    #[test]
    fn test_zig_zag() {
        assert_eq!(encode_zig_zag(0i64), 0u64);
        assert_eq!(encode_zig_zag(-1i64), 1u64);
        assert_eq!(encode_zig_zag(1i64), 2u64);
        test_zig_zag_aux(0i64);
        test_zig_zag_aux(i64::MIN);
        test_zig_zag_aux(i64::MAX);
    }

    use proptest::prelude::any;
    use proptest::proptest;

    proptest! {
        #[test]
        fn test_proptest_zig_zag(val in any::<i64>()) {
            test_zig_zag_aux(val);
        }
    }

    #[test]
    fn test_column_op_metadata_byte_serialization() {
        for len in 0..=15 {
            for op_type in [ColumnOperationType::AddValue, ColumnOperationType::NewDoc] {
                let column_op_metadata = ColumnOperationMetadata { op_type, len };
                let column_op_metadata_code = column_op_metadata.to_code();
                let serdeser_metadata =
                    ColumnOperationMetadata::try_from_code(column_op_metadata_code).unwrap();
                assert_eq!(column_op_metadata, serdeser_metadata);
            }
        }
    }

    #[track_caller]
    fn ser_deser_symbol(column_op: ColumnOperation<NumericalValue>) {
        let buf = column_op.serialize();
        let mut buffer = buf.as_ref().to_vec();
        buffer.extend_from_slice(b"234234");
        let mut bytes = &buffer[..];
        let serdeser_symbol = ColumnOperation::deserialize(&mut bytes).unwrap();
        assert_eq!(bytes.len() + buf.as_ref().len(), buffer.len());
        assert_eq!(column_op, serdeser_symbol);
    }

    #[test]
    fn test_compute_num_bytes_for_u64() {
        assert_eq!(compute_num_bytes_for_u64(0), 0);
        assert_eq!(compute_num_bytes_for_u64(1), 1);
        assert_eq!(compute_num_bytes_for_u64(255), 1);
        assert_eq!(compute_num_bytes_for_u64(256), 2);
        assert_eq!(compute_num_bytes_for_u64((1 << 16) - 1), 2);
        assert_eq!(compute_num_bytes_for_u64(1 << 16), 3);
    }

    #[test]
    fn test_symbol_serialization() {
        ser_deser_symbol(ColumnOperation::NewDoc(0));
        ser_deser_symbol(ColumnOperation::NewDoc(3));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::I64(0i64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::I64(1i64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::U64(257u64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::I64(-257i64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::I64(i64::MIN)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::U64(0u64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::U64(u64::MIN)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::U64(u64::MAX)));
    }

    fn test_column_operation_unordered_aux(val: u32, expected_len: usize) {
        let column_op = ColumnOperation::Value(UnorderedId(val));
        let minibuf = column_op.serialize();
        assert_eq!({ minibuf.as_ref().len() }, expected_len);
        let mut buf = minibuf.as_ref().to_vec();
        buf.extend_from_slice(&[2, 2, 2, 2, 2, 2]);
        let mut cursor = &buf[..];
        let column_op_serdeser: ColumnOperation<UnorderedId> =
            ColumnOperation::deserialize(&mut cursor).unwrap();
        assert_eq!(column_op_serdeser, ColumnOperation::Value(UnorderedId(val)));
        assert_eq!(cursor.len() + expected_len, buf.len());
    }

    #[test]
    fn test_column_operation_unordered() {
        test_column_operation_unordered_aux(300u32, 3);
        test_column_operation_unordered_aux(1u32, 2);
        test_column_operation_unordered_aux(0u32, 1);
    }
}
