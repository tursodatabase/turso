use crate::storage::sqlite3_ondisk::read_varint;
use crate::types::get_serial_type_size;
use crate::vdbe::{decode_serial_type_into_register, read_serial_type, Register};
use crate::{turso_assert, turso_debug_assert, LimboError, Result};

/// A read of a later column walks the header from the last kept one.
const MAX_PARSED_COLUMNS: usize = 64;

/// The serial types and value positions of the record under a b-tree
/// cursor, parsed from its header up to the highest column read so far, so
/// that the column reads of one row do not walk the header from the start
/// each time (SQLite keeps the same in VdbeCursor.aType and aOffset).
/// Positions count from the start of the payload, so they hold for the
/// payload on the page and for a copy of it.
pub(crate) struct ParsedHeader {
    /// Where the value of column 0 starts. 0 when nothing is parsed for the
    /// row under the cursor.
    header_size: u32,
    payload_size: u32,
    /// Number of columns whose serial type and value position are known.
    count: u32,
    /// Position of the serial type of column `count` in the header.
    next_type_pos: u32,
    serial_types: [u64; MAX_PARSED_COLUMNS],
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

    /// False when the record is too short to have `column`.
    #[inline(always)]
    pub(crate) fn read_column_into(
        &mut self,
        payload: &[u8],
        column: usize,
        dest: &mut Register,
    ) -> Result<bool> {
        let Some((serial_type, value_start)) = self.locate(payload, column)? else {
            return Ok(false);
        };
        decode_serial_type_into_register(serial_type, &mut &payload[value_start..], dest)?;
        Ok(true)
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
        self.start_if_needed(payload)?;
        let end = start + dests.len();
        self.parse_columns_before(payload, end)?;
        let count = self.count as usize;
        let parsed_end = count.clamp(start, end);
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
        if read == dests.len() || count < MAX_PARSED_COLUMNS {
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

    /// False when the record is too short to have `column`.
    pub(crate) fn has_column(&mut self, payload: &[u8], column: usize) -> Result<bool> {
        Ok(self.locate(payload, column)?.is_some())
    }

    /// The serial type and value position of `column`, or None when the
    /// record is too short to have it.
    #[inline(always)]
    fn locate(&mut self, payload: &[u8], column: usize) -> Result<Option<(u64, usize)>> {
        self.start_if_needed(payload)?;
        if column >= self.count as usize {
            self.parse_columns_before(payload, column + 1)?;
        }
        let location = if column < self.count as usize {
            Some((
                self.serial_types[column],
                self.value_starts[column] as usize,
            ))
        } else if (self.count as usize) < MAX_PARSED_COLUMNS {
            None
        } else if let Some((type_pos, value_start)) = self.walk_from_parsed_end(payload, column)? {
            let serial_type = read_serial_type(&mut &payload[type_pos..self.header_size as usize])?;
            checked_value_end(value_start, serial_type, payload.len())?;
            Some((serial_type, value_start))
        } else {
            None
        };
        turso_debug_assert!(
            matches!(locate_by_walk(payload, column), Ok(walked) if walked == location),
            "the parsed header disagrees with a walk of the record header",
            { "column": column }
        );
        Ok(location)
    }

    #[inline(always)]
    fn start_if_needed(&mut self, payload: &[u8]) -> Result<()> {
        if self.header_size == 0 {
            self.start(payload)?;
        }
        turso_assert!(
            self.payload_size as usize == payload.len(),
            "the parsed header belongs to another row",
            { "parsed_payload_size": self.payload_size, "payload_size": payload.len() }
        );
        Ok(())
    }

    #[inline(always)]
    fn start(&mut self, payload: &[u8]) -> Result<()> {
        let payload_size = u32::try_from(payload.len()).map_err(|_| LimboError::TooBig)?;
        let (header_size, first_type_pos) = match payload.first() {
            Some(&size) if size < 0x80 => (u64::from(size), 1),
            _ => read_varint(payload)?,
        };
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

    /// Parses serial types until the columns before `end` are known, the
    /// arrays are full or the header ends. The value positions are checked
    /// against the payload size once, after the loop; `count` grows only when
    /// that check passes, so an error in a corrupt header leaves the known
    /// columns as they were.
    #[inline(always)]
    fn parse_columns_before(&mut self, payload: &[u8], end: usize) -> Result<()> {
        let header_size = self.header_size as usize;
        let end = end.min(MAX_PARSED_COLUMNS);
        let mut count = self.count as usize;
        let mut type_pos = self.next_type_pos as usize;
        let mut value_end = u64::from(self.value_starts[count]);
        while count < end && type_pos < header_size {
            let first_byte = payload[type_pos];
            let serial_type = if first_byte < 0x80 {
                type_pos += 1;
                u64::from(first_byte)
            } else {
                let (serial_type, varint_len) = read_varint(&payload[type_pos..header_size])?;
                type_pos += varint_len;
                serial_type
            };
            value_end = value_end.saturating_add(value_size(serial_type));
            self.serial_types[count] = serial_type;
            self.value_starts[count + 1] = value_end as u32;
            count += 1;
        }
        if value_end > payload.len() as u64 {
            return self.parse_columns_checked(payload, end);
        }
        self.count = count as u32;
        self.next_type_pos = type_pos as u32;
        Ok(())
    }

    /// The same parse as `parse_columns_before`, with a check of each value
    /// position, for a header that the fast parse found to be corrupt: it
    /// keeps the columns before the first bad one and returns its error.
    #[cold]
    #[inline(never)]
    fn parse_columns_checked(&mut self, payload: &[u8], end: usize) -> Result<()> {
        let header_size = self.header_size as usize;
        let mut count = self.count as usize;
        let mut type_pos = self.next_type_pos as usize;
        let mut value_start = self.value_starts[count] as usize;
        while count < end && type_pos < header_size {
            let mut header = &payload[type_pos..header_size];
            let serial_type = read_serial_type(&mut header)?;
            let value_end = checked_value_end(value_start, serial_type, payload.len())?;
            self.serial_types[count] = serial_type;
            self.value_starts[count + 1] = value_end as u32;
            count += 1;
            type_pos = header_size - header.len();
            value_start = value_end;
            self.count = count as u32;
            self.next_type_pos = type_pos as u32;
        }
        unreachable!("the fast parse found a value past the payload, but each value fits")
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
            value_start = checked_value_end(value_start, serial_type, payload.len())?;
        }
        if header.is_empty() {
            return Ok(None);
        }
        Ok(Some((header_size - header.len(), value_start)))
    }
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
        let value_end = checked_value_end(value_start, serial_type, payload.len())?;
        if current == column {
            return Ok(Some((serial_type, value_start)));
        }
        type_pos += type_len;
        value_start = value_end;
        current += 1;
    }
    Ok(None)
}

/// The size of a value with `serial_type`. The reserved serial types 10 and
/// 11 get u64::MAX, so the check of the parse after the loop fails for them.
#[inline(always)]
fn value_size(serial_type: u64) -> u64 {
    const SIZES: [u64; 12] = [0, 1, 2, 3, 4, 6, 8, 8, 0, 0, u64::MAX, u64::MAX];
    if serial_type >= 12 {
        (serial_type - 12) / 2
    } else {
        SIZES[serial_type as usize]
    }
}

fn checked_value_end(value_start: usize, serial_type: u64, payload_size: usize) -> Result<usize> {
    value_start
        .checked_add(get_serial_type_size(serial_type)?)
        .filter(|end| *end <= payload_size)
        .ok_or_else(|| {
            LimboError::Corrupt("Data section too small for indicated serial type size".into())
        })
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
        let has_column = header.read_column_into(payload, column, &mut dest)?;
        Ok(has_column.then(|| dest.get_value().clone()))
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
    fn presence_of_a_column_follows_the_length_of_the_record() {
        let short = record(&[Value::from_i64(1), Value::build_text("a")]);
        let mut header = ParsedHeader::new();
        for (column, has_column) in [(1, true), (2, false), (0, true), (70, false)] {
            assert_eq!(
                header.has_column(short.get_payload(), column).unwrap(),
                has_column
            );
        }
        let values: Vec<Value> = (0..70).map(Value::from_i64).collect();
        let wide = record(&values);
        header.forget();
        assert!(header.has_column(wide.get_payload(), 69).unwrap());
        assert!(!header.has_column(wide.get_payload(), 70).unwrap());
    }

    #[test]
    fn a_range_past_the_kept_columns_walks_the_header_once() {
        let values: Vec<Value> = (0..MAX_PARSED_COLUMNS as i64 + 40)
            .map(|i| Value::build_text("v".repeat(i as usize)))
            .collect();
        let record = record(&values);
        let payload = record.get_payload();
        let mut header = ParsedHeader::new();
        let start = MAX_PARSED_COLUMNS + 10;
        assert_eq!(
            read_range(&mut header, payload, start, 50),
            values[start..].to_vec()
        );
        assert_eq!(header.count as usize, MAX_PARSED_COLUMNS);
        assert_eq!(
            read_range(&mut header, payload, 3, 4),
            values[3..7].to_vec()
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
    #[cfg(debug_assertions)]
    #[should_panic(expected = "the parsed header disagrees with a walk of the record header")]
    fn a_payload_of_the_same_size_without_forget_panics() {
        let first = record(&[
            Value::from_i64(5),
            Value::build_text("x"),
            Value::from_i64(30),
        ]);
        let second = record(&[
            Value::build_text("y"),
            Value::from_i64(33),
            Value::build_text("z"),
        ]);
        assert_eq!(first.get_payload().len(), second.get_payload().len());
        assert_ne!(first.get_payload()[..4], second.get_payload()[..4]);
        let mut header = ParsedHeader::new();
        read(&mut header, first.get_payload(), 1).unwrap();
        let _ = read(&mut header, second.get_payload(), 1);
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
