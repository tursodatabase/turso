use branches::{mark_unlikely, unlikely};
use std::ops::Deref;

use crate::storage::sqlite3_ondisk::{read_varint, write_varint};
use crate::vdbe::Register;
use crate::{LimboError, Result};

use super::{
    AppendWriter, AsValueRef, Numeric, Record, SerialType, SerialTypeKind, Value, ValueIterator,
    ValueRef,
};

#[repr(transparent)]
pub struct ImmutableRecordRef([u8]);

impl ImmutableRecordRef {
    #[inline]
    pub fn new(payload: &[u8]) -> &Self {
        // SAFETY: ImmutableRecordRef is repr(transparent) over [u8].
        unsafe { &*(payload as *const [u8] as *const Self) }
    }

    #[inline]
    pub fn from_value_ref(payload: ValueRef<'_>) -> &Self {
        match payload {
            ValueRef::Blob(blob) => Self::new(blob),
            _ => panic!("ImmutableRecordRef payload must be a blob"),
        }
    }

    #[inline]
    pub fn as_blob(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn as_blob_value(&self) -> ValueRef<'_> {
        ValueRef::Blob(self.as_blob())
    }

    #[inline]
    pub fn get_payload(&self) -> &[u8] {
        self.as_blob()
    }

    #[inline]
    pub fn is_invalidated(&self) -> bool {
        self.as_blob().is_empty()
    }

    // Don't use this in performance critical paths, prefer using `iter()` instead
    pub fn get_values(&self) -> Result<Vec<ValueRef<'_>>> {
        let iter = self.iter()?;
        let mut values = Vec::with_capacity(iter.size_hint().0);
        for value in iter {
            values.push(value?);
        }
        Ok(values)
    }

    // Don't use this in performance critical paths, prefer using `iter()` instead
    pub fn get_values_range(&self, range: std::ops::Range<usize>) -> Result<Vec<ValueRef<'_>>> {
        let mut iter = self.iter()?;
        let mut values = Vec::with_capacity(range.end - range.start);
        // advance to start
        if let Some(value) = iter.nth(range.start) {
            values.push(value?);
        } else {
            return Ok(values);
        }
        // collect rest
        for _ in range.start + 1..range.end {
            if let Some(value) = iter.next() {
                values.push(value?);
            } else {
                break;
            }
        }
        Ok(values)
    }

    // Idx values must be sorted ascending
    pub fn get_two_values(&self, idx1: usize, idx2: usize) -> Result<(ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = self.iter()?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1); // idx2 - idx1 - 1 because we already advanced to idx1
        match (val1, val2) {
            (Some(v1), Some(v2)) => Ok((v1?, v2?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    // Idx values must be sorted ascending
    pub fn get_three_values(
        &self,
        idx1: usize,
        idx2: usize,
        idx3: usize,
    ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = self.iter()?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1); // idx2 - idx1 - 1 because we already advanced to idx1
        let val3 = iter.nth(idx3 - idx2 - 1); // idx3 - idx2 - 1 because we already advanced to idx2
        match (val1, val2, val3) {
            (Some(v1), Some(v2), Some(v3)) => Ok((v1?, v2?, v3?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    // Idx values must be sorted ascending
    pub fn get_four_values(
        &self,
        idx1: usize,
        idx2: usize,
        idx3: usize,
        idx4: usize,
    ) -> Result<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)> {
        let mut iter = self.iter()?;
        let val1 = iter.nth(idx1);
        let val2 = iter.nth(idx2 - idx1 - 1); // idx2 - idx1 - 1 because we already advanced to idx1
        let val3 = iter.nth(idx3 - idx2 - 1); // idx3 - idx2 - 1 because we already advanced to idx2
        let val4 = iter.nth(idx4 - idx3 - 1); // idx4 - idx3 - 1 because we already advanced to idx3
        match (val1, val2, val3, val4) {
            (Some(v1), Some(v2), Some(v3), Some(v4)) => Ok((v1?, v2?, v3?, v4?)),
            _ => Err(LimboError::InternalError("index out of bound".to_string())),
        }
    }

    // Don't use this in performance critical paths, prefer using `iter()` instead
    pub fn get_values_owned(&self) -> Result<Vec<Value>> {
        let iter = self.iter().expect("Failed to create payload iterator");
        let mut values = Vec::with_capacity(iter.size_hint().0);
        for value in iter {
            values.push(value?.to_owned());
        }
        Ok(values)
    }

    // Don't use this in performance critical paths, prefer using `iter()` instead
    pub fn get_values_owned_range(&self, range: std::ops::Range<usize>) -> Result<Vec<Value>> {
        let mut iter = self.iter().expect("Failed to create payload iterator");
        let mut values = Vec::with_capacity(range.end - range.start);
        // advance to start
        if let Some(value) = iter.nth(range.start) {
            values.push(value?.to_owned());
        } else {
            return Ok(values);
        }
        // collect rest
        for _ in range.start + 1..range.end {
            if let Some(value) = iter.next() {
                values.push(value?.to_owned());
            } else {
                break;
            }
        }
        Ok(values)
    }

    #[inline(always)]
    pub fn iter(&self) -> Result<ValueIterator<'_>, LimboError> {
        ValueIterator::new(self.get_payload())
    }

    #[inline]
    /// Returns true if the record contains any NULL values.
    /// This is an optimization that only examines the header (serial types)
    /// without deserializing the data section.
    pub fn contains_null(&self) -> Result<bool> {
        let payload = self.get_payload();
        let (header_size, header_varint_len) = read_varint(payload)?;
        let header_size = header_size as usize;

        if header_size > payload.len() || header_varint_len > payload.len() {
            return Err(LimboError::Corrupt(
                "Payload too small for indicated header size".into(),
            ));
        }

        let mut header = &payload[header_varint_len..header_size];

        while !header.is_empty() {
            let (serial_type, bytes_read) = read_varint(header)?;
            if serial_type == 0 {
                return Ok(true);
            }
            header = &header[bytes_read..];
        }

        Ok(false)
    }

    #[inline]
    pub fn last_value(&self) -> Option<Result<ValueRef<'_>>> {
        if unlikely(self.is_invalidated()) {
            return Some(Err(LimboError::InternalError(
                "Record is invalidated".into(),
            )));
        }
        let iter = match self.iter() {
            Ok(it) => it,
            Err(e) => return Some(Err(e)),
        };
        iter.last()
    }

    #[inline]
    pub fn first_value(&self) -> Result<ValueRef<'_>> {
        if unlikely(self.is_invalidated()) {
            return Err(LimboError::InternalError("Record is invalidated".into()));
        }
        match self.iter()?.next() {
            Some(v) => v,
            None => Err(LimboError::InternalError("Record has no columns".into())),
        }
    }

    #[inline]
    pub fn get_value(&self, idx: usize) -> Result<ValueRef<'_>> {
        if unlikely(self.is_invalidated()) {
            return Err(LimboError::InternalError("Record is invalidated".into()));
        }
        let mut iter = self.iter()?;
        iter.nth(idx)
            .transpose()?
            .ok_or_else(|| LimboError::InternalError("Index out of bounds".into()))
    }

    #[inline]
    pub fn get_value_opt(&self, idx: usize) -> Option<ValueRef<'_>> {
        let mut iter = match self.iter() {
            Ok(it) => it,
            Err(_) => {
                mark_unlikely();
                return None;
            }
        };
        match iter.nth(idx) {
            Some(Ok(v)) => Some(v),
            _ => {
                mark_unlikely();
                None
            }
        }
    }

    pub fn column_count(&self) -> usize {
        self.iter().map(|it| it.count()).unwrap_or_default()
    }
}

impl std::fmt::Debug for ImmutableRecordRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.as_blob();
        let preview = if bytes.len() > 20 {
            format!("{:?} ... ({} bytes total)", &bytes[..20], bytes.len())
        } else {
            format!("{bytes:?}")
        };
        write!(f, "ImmutableRecordRef {{ payload: {preview} }}")
    }
}

impl PartialEq for ImmutableRecordRef {
    fn eq(&self, other: &Self) -> bool {
        self.as_blob() == other.as_blob()
    }
}

impl Eq for ImmutableRecordRef {}

impl PartialOrd for ImmutableRecordRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImmutableRecordRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_blob().cmp(other.as_blob())
    }
}

/// This struct serves the purpose of not allocating multiple vectors of bytes if not needed.
/// A value in a record that has already been serialized can stay serialized and what this struct offsers
/// is easy acces to each value which point to the payload.
/// The name might be contradictory as it is immutable in the sense that you cannot modify the values without modifying the payload.
pub struct ImmutableRecord {
    // We have to be super careful with this buffer since we make values point to the payload we need to take care reallocations
    // happen in a controlled manner. If we realocate with values that should be correct, they will now point to undefined data.
    // We don't use pin here because it would make it imposible to reuse the buffer if we need to push a new record in the same struct.
    //
    // payload is the Vec<u8> but in order to use Register which holds ImmutableRecord as a Value - we store Vec<u8> as Value::Blob
    payload: Value,
}

// SAFETY: all ImmutableRecord instances are intended to be used in a single thread
// by a single connection.
unsafe impl Send for ImmutableRecord {}
unsafe impl Sync for ImmutableRecord {}

impl Clone for ImmutableRecord {
    fn clone(&self) -> Self {
        Self {
            payload: self.payload.clone(),
        }
    }
}

impl PartialEq for ImmutableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.payload == other.payload // Only compare payload, ignore cursor state
    }
}

impl Eq for ImmutableRecord {}

impl PartialOrd for ImmutableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImmutableRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.payload.cmp(&other.payload) // Only compare payload, ignore cursor state
    }
}

impl std::fmt::Debug for ImmutableRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.payload {
            Value::Blob(bytes) => {
                let preview = if bytes.len() > 20 {
                    format!("{:?} ... ({} bytes total)", &bytes[..20], bytes.len())
                } else {
                    format!("{bytes:?}")
                };
                write!(f, "ImmutableRecord {{ payload: {preview} }}")
            }
            Value::Text(s) => {
                let string = s.as_str();
                let preview = if string.len() > 20 {
                    format!("{:?} ... ({} chars total)", &string[..20], string.len())
                } else {
                    format!("{string:?}")
                };
                write!(f, "ImmutableRecord {{ payload: {preview} }}")
            }
            other => write!(f, "ImmutableRecord {{ payload: {other:?} }}"),
        }
    }
}

impl Deref for ImmutableRecord {
    type Target = ImmutableRecordRef;

    fn deref(&self) -> &Self::Target {
        ImmutableRecordRef::new(self.as_blob())
    }
}

impl ImmutableRecord {
    pub fn new(payload_capacity: usize) -> Self {
        Self {
            payload: Value::Blob(Vec::with_capacity(payload_capacity)),
        }
    }

    pub fn from_bin_record(payload: Vec<u8>) -> Self {
        Self {
            payload: Value::Blob(payload),
        }
    }

    pub fn from_registers<'a, I: Iterator<Item = &'a Register> + Clone>(
        // we need to accept both &[Register] and &[&Register] values - that's why non-trivial signature
        //
        // std::slice::Iter under the hood just stores pointer and length of slice and also implements a Clone which just copy those meta-values
        // (without copying the data itself)
        registers: impl IntoIterator<Item = &'a Register, IntoIter = I>,
        len: usize,
    ) -> Self {
        Self::from_values(registers.into_iter().map(|x| x.get_value()), len)
    }

    pub fn from_values<'a>(
        values: impl IntoIterator<Item = impl AsValueRef + 'a> + Clone,
        len: usize,
    ) -> Self {
        let mut serials = Vec::with_capacity(len);
        let mut size_header = 0;
        let mut size_values = 0;

        let mut serial_type_buf = [0; 9];
        // write serial types
        for value in values.clone() {
            let serial_type = SerialType::from(value.as_value_ref());
            let n = write_varint(&mut serial_type_buf[0..], serial_type.into());
            serials.push((serial_type_buf, n));

            let value_size = serial_type.size();

            size_header += n;
            size_values += value_size;
        }

        let header_size = Record::calc_header_size(size_header);

        // 1. write header size
        let mut buf = Vec::new();
        buf.reserve_exact(header_size + size_values);
        assert_eq!(buf.capacity(), header_size + size_values);
        let n = write_varint(&mut serial_type_buf, header_size as u64);

        buf.resize(buf.capacity(), 0);
        let mut writer = AppendWriter::new(&mut buf, 0);
        writer.extend_from_slice(&serial_type_buf[..n]);

        // 2. Write serial
        for (value, n) in serials {
            writer.extend_from_slice(&value[..n]);
        }

        // write content
        for value in values {
            let value = value.as_value_ref();
            match value {
                ValueRef::Null => {}
                ValueRef::Numeric(Numeric::Integer(i)) => {
                    let serial_type = SerialType::from(value);
                    match serial_type.kind() {
                        SerialTypeKind::ConstInt0 | SerialTypeKind::ConstInt1 => {}
                        SerialTypeKind::I8 => writer.extend_from_slice(&(i as i8).to_be_bytes()),
                        SerialTypeKind::I16 => writer.extend_from_slice(&(i as i16).to_be_bytes()),
                        SerialTypeKind::I24 => {
                            writer.extend_from_slice(&(i as i32).to_be_bytes()[1..])
                        } // remove most significant byte
                        SerialTypeKind::I32 => writer.extend_from_slice(&(i as i32).to_be_bytes()),
                        SerialTypeKind::I48 => writer.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                        SerialTypeKind::I64 => writer.extend_from_slice(&i.to_be_bytes()),
                        other => panic!("Serial type is not an integer: {other:?}"),
                    }
                }
                ValueRef::Numeric(Numeric::Float(f)) => {
                    let fval: f64 = f.into();
                    writer.extend_from_slice(&fval.to_be_bytes());
                }
                ValueRef::Text(t) => {
                    writer.extend_from_slice(t.value.as_bytes());
                }
                ValueRef::Blob(b) => {
                    writer.extend_from_slice(b);
                }
            };
        }

        writer.assert_finish_capacity();
        Self {
            payload: Value::Blob(buf),
        }
    }

    #[inline]
    pub fn into_payload(self) -> Vec<u8> {
        match self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    #[inline]
    pub fn as_blob(&self) -> &Vec<u8> {
        match &self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    #[inline]
    pub fn as_blob_mut(&mut self) -> &mut Vec<u8> {
        match &mut self.payload {
            Value::Blob(b) => b,
            _ => panic!("payload must be a blob"),
        }
    }

    #[inline]
    pub fn as_blob_value(&self) -> &Value {
        &self.payload
    }

    #[inline]
    pub fn start_serialization(&mut self, payload: &[u8]) {
        self.as_blob_mut().extend_from_slice(payload);
    }

    #[inline]
    pub fn invalidate(&mut self) {
        self.as_blob_mut().clear();
    }
}
