//! The serial types and value positions of the record under a b-tree cursor.

use crate::storage::sqlite3_ondisk::read_varint;
use crate::types::get_serial_type_size;
use crate::vdbe::{decode_serial_type_into_register, read_serial_type, Register};
use crate::{turso_assert, turso_debug_assert, LimboError, Result};

/// The most columns whose serial types and value positions [`ParsedHeader`]
/// keeps. A read of a later column walks the header from the last kept one.
const MAX_PARSED_COLUMNS: usize = 64;

/// What a read of one column of the row under a cursor found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnPresence {
    /// The cursor is on no row or on a null row.
    NoRow,
    /// The record is too short to have the column (ALTER TABLE ADD COLUMN).
    Missing,
    Present,
}

/// The serial types and value positions of the record under a b-tree
/// cursor, parsed from its header up to the highest column read so far, so
/// that the column reads of one row do not walk the header from the start
/// each time (SQLite keeps the same in VdbeCursor.aType and aOffset).
/// Positions count from the start of the payload, so they hold for the
/// payload on the page and for a copy of it.
pub(crate) struct ParsedHeader {
    /// Size of the record header, which is where the value of column 0
    /// starts. 0 when nothing is parsed for the row under the cursor.
    header_size: u32,
    payload_size: u32,
    /// Number of columns whose serial type and value position are known.
    count: u32,
    /// Position of the serial type of column `count` in the header.
    next_type_pos: u32,
    serial_types: [u64; MAX_PARSED_COLUMNS],
    /// `value_starts[i]` is where the value of column `i` starts, and
    /// `value_starts[count]` is where the value of column `count - 1` ends.
    value_starts: [u32; MAX_PARSED_COLUMNS + 1],
}

impl ParsedHeader {
    pub(crate) const fn new() -> Self {
        Self {
            header_size: 0,
            payload_size: 0,
            count: 0,
            next_type_pos: 0,
            serial_types: [0; MAX_PARSED_COLUMNS],
            value_starts: [0; MAX_PARSED_COLUMNS + 1],
        }
    }

    #[inline(always)]
    pub(crate) fn forget(&mut self) {
        self.header_size = 0;
    }

    #[inline(always)]
    pub(crate) fn read_column_into(
        &mut self,
        payload: &[u8],
        column: usize,
        dest: &mut Register,
    ) -> Result<ColumnPresence> {
        let Some((serial_type, value_start)) = self.locate(payload, column)? else {
            return Ok(ColumnPresence::Missing);
        };
        decode_serial_type_into_register(serial_type, &mut &payload[value_start..], dest)?;
        Ok(ColumnPresence::Present)
    }

    /// Reads the columns from `start` on into `dests`, and returns how many
    /// of them the record has.
    #[inline(always)]
    pub(crate) fn read_columns_into(
        &mut self,
        payload: &[u8],
        start: usize,
        dests: &mut [Register],
    ) -> Result<usize> {
        if self.locate(payload, start)?.is_none() {
            return Ok(0);
        }
        let parsed_end = (self.count as usize).clamp(start, start + dests.len());
        for (column, dest) in (start..parsed_end).zip(dests.iter_mut()) {
            let serial_type = self.serial_types[column];
            let value_start = self.value_starts[column] as usize;
            turso_debug_assert!(
                matches!(locate_by_walk(payload, column), Ok(Some(walked)) if walked == (serial_type, value_start)),
                "the parsed header disagrees with a walk of the record header",
                { "column": column }
            );
            decode_serial_type_into_register(serial_type, &mut &payload[value_start..], dest)?;
        }
        let mut read = parsed_end - start;
        if read == dests.len() {
            return Ok(read);
        }
        let Some((type_pos, value_start)) = self.walk_from_parsed_end(payload, parsed_end)? else {
            return Ok(read);
        };
        let mut header = &payload[type_pos..self.header_size as usize];
        let mut data = &payload[value_start..];
        for dest in &mut dests[read..] {
            if header.is_empty() {
                break;
            }
            let serial_type = read_serial_type(&mut header)?;
            decode_serial_type_into_register(serial_type, &mut data, dest)?;
            read += 1;
        }
        Ok(read)
    }

    /// The serial type and value position of `column`, or None when the
    /// record is too short to have it.
    #[inline(always)]
    fn locate(&mut self, payload: &[u8], column: usize) -> Result<Option<(u64, usize)>> {
        if self.header_size == 0 {
            self.start(payload)?;
        }
        turso_assert!(
            self.payload_size as usize == payload.len(),
            "the parsed header belongs to another row",
            { "parsed_payload_size": self.payload_size, "payload_size": payload.len() }
        );
        let location = if column < self.count as usize {
            Some((
                self.serial_types[column],
                self.value_starts[column] as usize,
            ))
        } else {
            self.parse_through(payload, column)?
        };
        turso_debug_assert!(
            matches!(locate_by_walk(payload, column), Ok(walked) if walked == location),
            "the parsed header disagrees with a walk of the record header",
            { "column": column }
        );
        Ok(location)
    }

    fn start(&mut self, payload: &[u8]) -> Result<()> {
        let payload_size = u32::try_from(payload.len()).map_err(|_| LimboError::TooBig)?;
        let (header_size, first_type_pos) = read_varint(payload)?;
        if header_size > u64::from(payload_size) || first_type_pos as u64 > header_size {
            return Err(LimboError::Corrupt(
                "Payload too small for indicated header size".into(),
            ));
        }
        self.payload_size = payload_size;
        self.count = 0;
        self.next_type_pos = first_type_pos as u32;
        self.value_starts[0] = header_size as u32;
        self.header_size = header_size as u32;
        Ok(())
    }

    /// Parses serial types until `column` is known or the header ends. The
    /// entries are written before `count` grows, so an error in a corrupt
    /// header leaves the known columns as they were.
    fn parse_through(&mut self, payload: &[u8], column: usize) -> Result<Option<(u64, usize)>> {
        let header_size = self.header_size as usize;
        let parse_end = column.min(MAX_PARSED_COLUMNS - 1) + 1;
        let mut count = self.count as usize;
        let mut type_pos = self.next_type_pos as usize;
        let mut value_start = self.value_starts[count] as usize;
        while count < parse_end && type_pos < header_size {
            let mut header = &payload[type_pos..header_size];
            let serial_type = read_serial_type(&mut header)?;
            let value_end = value_end(value_start, serial_type, payload.len())?;
            self.serial_types[count] = serial_type;
            self.value_starts[count + 1] = value_end as u32;
            count += 1;
            type_pos = header_size - header.len();
            value_start = value_end;
        }
        self.count = count as u32;
        self.next_type_pos = type_pos as u32;
        if column < count {
            return Ok(Some((
                self.serial_types[column],
                self.value_starts[column] as usize,
            )));
        }
        if count < MAX_PARSED_COLUMNS {
            return Ok(None);
        }
        let Some((type_pos, value_start)) = self.walk_from_parsed_end(payload, column)? else {
            return Ok(None);
        };
        let serial_type = read_serial_type(&mut &payload[type_pos..header_size])?;
        value_end(value_start, serial_type, payload.len())?;
        Ok(Some((serial_type, value_start)))
    }

    /// The positions of the serial type and the value of `column`, found by a
    /// walk that starts after the last parsed column. None when the record
    /// is too short to have `column`.
    fn walk_from_parsed_end(
        &self,
        payload: &[u8],
        column: usize,
    ) -> Result<Option<(usize, usize)>> {
        let header_size = self.header_size as usize;
        let count = self.count as usize;
        turso_debug_assert!(column >= count);
        let mut header = &payload[self.next_type_pos as usize..header_size];
        let mut value_start = self.value_starts[count] as usize;
        for _ in count..column {
            if header.is_empty() {
                return Ok(None);
            }
            let serial_type = read_serial_type(&mut header)?;
            value_start = value_end(value_start, serial_type, payload.len())?;
        }
        if header.is_empty() {
            return Ok(None);
        }
        Ok(Some((header_size - header.len(), value_start)))
    }
}

fn value_end(value_start: usize, serial_type: u64, payload_size: usize) -> Result<usize> {
    value_start
        .checked_add(get_serial_type_size(serial_type)?)
        .filter(|end| *end <= payload_size)
        .ok_or_else(|| {
            LimboError::Corrupt("Data section too small for indicated serial type size".into())
        })
}

/// The serial type and value position of `column` found by a walk of the
/// whole header, without the parsed header. Debug builds compare every read
/// through the parsed header with it.
fn locate_by_walk(payload: &[u8], column: usize) -> Result<Option<(u64, usize)>> {
    let (header_size, mut type_pos) = read_varint(payload)?;
    let header_size = usize::try_from(header_size).unwrap_or(usize::MAX);
    if header_size > payload.len() || type_pos > header_size {
        return Err(LimboError::Corrupt(
            "Payload too small for indicated header size".into(),
        ));
    }
    let mut value_start = header_size;
    let mut current = 0;
    while type_pos < header_size {
        let (serial_type, type_len) = read_varint(&payload[type_pos..header_size])?;
        let value_end = value_end(value_start, serial_type, payload.len())?;
        if current == column {
            return Ok(Some((serial_type, value_start)));
        }
        type_pos += type_len;
        value_start = value_end;
        current += 1;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ImmutableRecord;
    use crate::vdbe::Register;
    use crate::Value;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    fn record(values: &[Value]) -> ImmutableRecord {
        ImmutableRecord::from_values(values, values.len()).unwrap()
    }

    fn read(header: &mut ParsedHeader, payload: &[u8], column: usize) -> Result<Option<Value>> {
        let mut dest = Register::Value(Value::Null);
        Ok(match header.read_column_into(payload, column, &mut dest)? {
            ColumnPresence::Present => Some(dest.get_value().clone()),
            ColumnPresence::Missing => None,
            ColumnPresence::NoRow => unreachable!("a payload always has a row"),
        })
    }

    fn read_range(
        header: &mut ParsedHeader,
        payload: &[u8],
        start: usize,
        len: usize,
    ) -> Vec<Value> {
        let mut dests: Vec<Register> = (0..len).map(|_| Register::Value(Value::Null)).collect();
        let read = header
            .read_columns_into(payload, start, &mut dests)
            .unwrap();
        dests[..read]
            .iter()
            .map(|r| r.get_value().clone())
            .collect()
    }

    fn random_value(rng: &mut ChaCha8Rng) -> Value {
        let integers = [
            0,
            1,
            -1,
            127,
            -128,
            32767,
            -32768,
            8_388_607,
            -8_388_608,
            2_147_483_647,
            -2_147_483_648,
            140_737_488_355_327,
            -140_737_488_355_328,
            i64::MAX,
            i64::MIN,
        ];
        match rng.next_u64() % 6 {
            0 => Value::Null,
            1 => Value::from_i64(integers[rng.next_u64() as usize % integers.len()]),
            2 => Value::from_f64((rng.next_u64() % 1000) as f64 / 7.0),
            3 => Value::build_text("t".repeat(rng.next_u64() as usize % 300)),
            4 => Value::Blob(crate::alloc::vec![7u8; rng.next_u64() as usize % 300]),
            _ => Value::from_i64(rng.next_u64() as i64),
        }
    }

    #[test]
    fn reads_in_any_order_give_the_values_of_the_record() {
        let mut rng = ChaCha8Rng::seed_from_u64(4);
        let mut header = ParsedHeader::new();
        for _ in 0..300 {
            let column_count = 1 + rng.next_u64() as usize % 150;
            let values: Vec<Value> = (0..column_count).map(|_| random_value(&mut rng)).collect();
            let record = record(&values);
            let payload = record.get_payload();
            header.forget();
            for _ in 0..20 {
                let column = rng.next_u64() as usize % (column_count + 5);
                if rng.next_u64() % 2 == 0 {
                    assert_eq!(
                        read(&mut header, payload, column).unwrap(),
                        values.get(column).cloned()
                    );
                } else {
                    let len = 1 + rng.next_u64() as usize % 80;
                    let expected: Vec<Value> =
                        values.iter().skip(column).take(len).cloned().collect();
                    assert_eq!(read_range(&mut header, payload, column, len), expected);
                }
            }
        }
    }

    #[test]
    fn columns_past_the_kept_columns_are_found_by_a_walk() {
        let values: Vec<Value> = (0..MAX_PARSED_COLUMNS as i64 + 10)
            .map(Value::from_i64)
            .collect();
        let record = record(&values);
        let payload = record.get_payload();
        let mut header = ParsedHeader::new();
        let last = values.len() - 1;
        assert_eq!(
            read(&mut header, payload, last).unwrap(),
            Some(values[last].clone())
        );
        assert_eq!(header.count as usize, MAX_PARSED_COLUMNS);
        assert_eq!(read(&mut header, payload, last + 1).unwrap(), None);
        assert_eq!(
            read_range(&mut header, payload, MAX_PARSED_COLUMNS - 2, 5),
            values[MAX_PARSED_COLUMNS - 2..MAX_PARSED_COLUMNS + 3].to_vec()
        );
        assert_eq!(
            read_range(&mut header, payload, last, 3),
            vec![values[last].clone()]
        );
    }

    #[test]
    fn a_value_past_the_payload_is_corrupt_and_keeps_the_parsed_columns() {
        let record = record(&[
            Value::from_i64(1),
            Value::build_text("abc"),
            Value::from_i64(300),
        ]);
        let mut payload = record.get_payload().to_vec();
        payload[2] = 13 + 2 * 50;
        let mut header = ParsedHeader::new();
        assert_eq!(
            read(&mut header, &payload, 0).unwrap(),
            Some(Value::from_i64(1))
        );
        assert!(matches!(
            read(&mut header, &payload, 2),
            Err(LimboError::Corrupt(_))
        ));
        assert_eq!(header.count, 1);
        assert_eq!(
            read(&mut header, &payload, 0).unwrap(),
            Some(Value::from_i64(1))
        );
        assert!(matches!(
            read(&mut header, &payload, 1),
            Err(LimboError::Corrupt(_))
        ));
    }

    #[test]
    fn a_header_size_past_the_payload_is_corrupt() {
        let record = record(&[Value::from_i64(1), Value::from_i64(2)]);
        let mut payload = record.get_payload().to_vec();
        payload[0] = payload.len() as u8 + 1;
        let mut header = ParsedHeader::new();
        assert!(matches!(
            read(&mut header, &payload, 0),
            Err(LimboError::Corrupt(_))
        ));
        assert_eq!(header.header_size, 0);
    }

    #[test]
    fn a_reserved_serial_type_is_corrupt() {
        let record = record(&[Value::from_i64(1), Value::from_i64(2)]);
        let mut payload = record.get_payload().to_vec();
        payload[2] = 10;
        let mut header = ParsedHeader::new();
        assert_eq!(
            read(&mut header, &payload, 0).unwrap(),
            Some(Value::from_i64(1))
        );
        assert!(read(&mut header, &payload, 1).is_err());
    }

    #[test]
    #[should_panic(expected = "the parsed header belongs to another row")]
    fn a_payload_of_another_size_without_forget_panics() {
        let first = record(&[Value::from_i64(1), Value::from_i64(2)]);
        let second = record(&[Value::from_i64(1), Value::build_text("longer")]);
        let mut header = ParsedHeader::new();
        read(&mut header, first.get_payload(), 0).unwrap();
        let _ = read(&mut header, second.get_payload(), 1);
    }
}
