//! Column deltas of SQLite records.
//!
//! A delta rebuilds one record, the target, from another record, the base. It
//! keeps only the columns of the target that differ from the base:
//!
//! ```text
//! delta = varint(column_count) varint(changed_count)
//!         { varint(column_gap) varint(serial_type) value_bytes }*
//! ```
//!
//! `column_count` is the column count of the target. It can differ from the
//! column count of the base, for example after `ALTER TABLE ADD COLUMN`.
//! `column_gap` is the distance from the previous changed column, or from
//! column 0 for the first changed column. A delta does not depend on the schema.

use crate::storage::sqlite3_ondisk::{read_varint, varint_len, write_varint, write_varint_to_vec};
use crate::types::{Record, SerialType};
use crate::{LimboError, Result};
use smallvec::SmallVec;
use std::cmp::Ordering;

/// The bytes of a delta. Most deltas are short, so they stay on the stack.
pub(crate) type DeltaBytes = SmallVec<[u8; 128]>;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Column<'a> {
    serial_type: u64,
    value: &'a [u8],
}

type Columns<'a> = SmallVec<[Column<'a>; 32]>;

/// Writes into `delta` a delta that rebuilds `target` from `base`.
///
/// Returns false when the delta is longer than `max_len`, or when `apply`
/// cannot rebuild the exact bytes of `target`: when a record does not parse,
/// or when `target` does not have the encoding that `apply` writes (the
/// shortest varints, and no bytes after the last value).
pub(crate) fn encode(base: &[u8], target: &[u8], max_len: usize, delta: &mut DeltaBytes) -> bool {
    let mut base_columns = Columns::new();
    let mut target_columns = Columns::new();
    if parse(base, &mut base_columns).is_err()
        || !matches!(parse(target, &mut target_columns), Ok(true))
    {
        return false;
    }
    delta.clear();
    push_varint(delta, target_columns.len() as u64);
    // The changed count usually fits in one byte. It is known after the columns.
    let count_pos = delta.len();
    delta.push(0);
    let mut changed_count = 0;
    let mut previous = 0;
    for (i, column) in target_columns.iter().enumerate() {
        if base_columns.get(i) == Some(column) {
            continue;
        }
        push_varint(delta, (i - previous) as u64);
        push_varint(delta, column.serial_type);
        delta.extend_from_slice(column.value);
        if delta.len() > max_len {
            return false;
        }
        changed_count += 1;
        previous = i;
    }
    let mut count = [0u8; 9];
    let count_len = write_varint(&mut count, changed_count);
    delta[count_pos] = count[0];
    delta.insert_from_slice(count_pos + 1, &count[1..count_len]);
    delta.len() <= max_len
}

/// Writes into `out` the record that `deltas` rebuild from `base`. The first
/// delta rebuilds its target from `base`, and each next delta rebuilds its
/// target from the target of the delta before it.
///
/// The deltas change a list of columns that point into `base` and into the
/// deltas, so the record bytes are written only once, for the last target.
pub(crate) fn apply<'a>(
    base: &'a [u8],
    deltas: impl IntoIterator<Item = &'a [u8]>,
    out: &mut Vec<u8>,
) -> Result<()> {
    let mut columns = Columns::new();
    parse(base, &mut columns)?;
    for delta in deltas {
        apply_to_columns(delta, &mut columns)?;
    }
    write_record(&columns, out);
    Ok(())
}

/// Changes `columns` from the columns of the base of `delta` to the columns of
/// its target.
fn apply_to_columns<'a>(delta: &'a [u8], columns: &mut Columns<'a>) -> Result<()> {
    let mut reader = DeltaReader { delta, pos: 0 };
    let column_count = reader.read_count(columns.len() + delta.len())?;
    let changed_count = reader.read_count(column_count)?;
    columns.truncate(column_count);
    let mut index = 0;
    for changed in 0..changed_count {
        let gap = reader.read_count(column_count)?;
        if changed > 0 && gap == 0 {
            return Err(corrupt("two changed columns have the same index"));
        }
        index += gap;
        if index >= column_count {
            return Err(corrupt("a changed column is past the end of the target"));
        }
        let column = reader.read_column()?;
        match index.cmp(&columns.len()) {
            Ordering::Less => columns[index] = column,
            Ordering::Equal => columns.push(column),
            Ordering::Greater => return Err(corrupt("an unchanged column is missing in the base")),
        }
    }
    if columns.len() != column_count {
        return Err(corrupt("an unchanged column is missing in the base"));
    }
    if reader.pos != delta.len() {
        return Err(corrupt("the delta has bytes that no column uses"));
    }
    Ok(())
}

fn write_record(columns: &[Column], out: &mut Vec<u8>) {
    let serial_types_len = columns.iter().map(|c| varint_len(c.serial_type)).sum();
    let header_size = Record::calc_header_size(serial_types_len);
    let values_len: usize = columns.iter().map(|c| c.value.len()).sum();
    out.clear();
    out.reserve(header_size + values_len);
    write_varint_to_vec(header_size as u64, out);
    for column in columns {
        write_varint_to_vec(column.serial_type, out);
    }
    for column in columns {
        out.extend_from_slice(column.value);
    }
}

/// Appends the columns of `record` to `columns`. Returns true when `apply`
/// writes the record with the same bytes.
fn parse<'a>(record: &'a [u8], columns: &mut Columns<'a>) -> Result<bool> {
    let (header_size, header_size_len) = read_varint(record)?;
    let header_size = usize::try_from(header_size)
        .ok()
        .filter(|size| (header_size_len..=record.len()).contains(size))
        .ok_or_else(|| corrupt("invalid record header size"))?;
    let mut canonical = header_size_len == varint_len(header_size as u64)
        && header_size == Record::calc_header_size(header_size - header_size_len);
    let mut header_pos = header_size_len;
    let mut value_pos = header_size;
    while header_pos < header_size {
        let (serial_type, len) = read_varint(&record[header_pos..header_size])?;
        canonical &= len == varint_len(serial_type);
        header_pos += len;
        let size = SerialType::try_from(serial_type)?.size();
        let value = record
            .get(value_pos..value_pos + size)
            .ok_or_else(|| corrupt("a record value is past the end of the record"))?;
        columns.push(Column { serial_type, value });
        value_pos += size;
    }
    Ok(canonical && value_pos == record.len())
}

fn push_varint(out: &mut DeltaBytes, value: u64) {
    let mut bytes = [0u8; 9];
    let len = write_varint(&mut bytes, value);
    out.extend_from_slice(&bytes[..len]);
}

struct DeltaReader<'a> {
    delta: &'a [u8],
    pos: usize,
}

impl<'a> DeltaReader<'a> {
    fn read_varint(&mut self) -> Result<u64> {
        let (value, len) = read_varint(&self.delta[self.pos..])?;
        self.pos += len;
        Ok(value)
    }

    fn read_count(&mut self, max: usize) -> Result<usize> {
        let value = self.read_varint()?;
        usize::try_from(value)
            .ok()
            .filter(|count| *count <= max)
            .ok_or_else(|| corrupt("a count in the delta is out of range"))
    }

    fn read_column(&mut self) -> Result<Column<'a>> {
        let serial_type = self.read_varint()?;
        let size = SerialType::try_from(serial_type)?.size();
        let value = self
            .delta
            .get(self.pos..self.pos + size)
            .ok_or_else(|| corrupt("a delta value is past the end of the delta"))?;
        self.pos += size;
        Ok(Column { serial_type, value })
    }
}

fn corrupt(reason: &str) -> LimboError {
    LimboError::Corrupt(format!("row version delta: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::{apply, encode, DeltaBytes};
    use crate::types::{ImmutableRecord, Value};
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn record(values: &[Value]) -> Vec<u8> {
        ImmutableRecord::from_values(values, values.len())
            .unwrap()
            .get_payload()
            .to_vec()
    }

    fn text(s: &str) -> Value {
        Value::build_text(s.to_string())
    }

    fn blob(bytes: &[u8]) -> Value {
        Value::from_slice(bytes).unwrap()
    }

    /// Encodes the delta from `base` to `target`, applies it to `base`, and
    /// returns the delta.
    fn round_trip(base: &[u8], target: &[u8]) -> Vec<u8> {
        let mut delta = DeltaBytes::new();
        assert!(
            encode(base, target, usize::MAX, &mut delta),
            "encode refused the target"
        );
        let mut rebuilt = Vec::new();
        apply(base, [&delta[..]], &mut rebuilt).unwrap();
        assert_eq!(rebuilt, target);
        delta.to_vec()
    }

    #[test]
    fn every_serial_type_round_trips() {
        let values = [
            Value::Null,
            Value::from_i64(0),
            Value::from_i64(1),
            Value::from_i64(-100),
            Value::from_i64(1000),
            Value::from_i64(-100_000),
            Value::from_i64(10_000_000),
            Value::from_i64(1 << 40),
            Value::from_i64(i64::MIN),
            Value::from_f64(1.5),
            Value::from_f64(-0.0),
            text(""),
            text("hello"),
            blob(&[]),
            blob(&[0, 1, 2, 255]),
        ];
        for (i, base_value) in values.iter().enumerate() {
            for (j, target_value) in values.iter().enumerate() {
                let base = record(&[text("key"), base_value.clone(), Value::from_i64(7)]);
                let target = record(&[text("key"), target_value.clone(), Value::from_i64(7)]);
                let delta = round_trip(&base, &target);
                let changed_count = delta[1];
                assert_eq!(changed_count, u8::from(i != j), "values {i} and {j}");
            }
        }
    }

    #[test]
    fn the_constants_zero_and_one_have_no_value_bytes() {
        let base = record(&[Value::from_i64(0), Value::from_i64(5)]);
        let target = record(&[Value::from_i64(1), Value::from_i64(5)]);
        // column_count, changed_count, gap, serial type 9.
        assert_eq!(round_trip(&base, &target), [2, 1, 0, 9]);
        assert_eq!(round_trip(&target, &base), [2, 1, 0, 8]);
    }

    #[test]
    fn a_delta_keeps_only_the_changed_columns() {
        let filler = "x".repeat(50);
        let base = record(&[
            text(&filler),
            Value::from_i64(10),
            text(&filler),
            Value::from_i64(20),
        ]);
        let target = record(&[
            text(&filler),
            Value::from_i64(11),
            text(&filler),
            Value::from_i64(21),
        ]);
        // column_count, changed_count, (gap 1, I8, 11), (gap 2, I8, 21).
        assert_eq!(round_trip(&base, &target), [4, 2, 1, 1, 11, 2, 1, 21]);
    }

    #[test]
    fn a_header_of_more_than_127_bytes_round_trips() {
        let long_text = "y".repeat(100);
        let base_values: Vec<Value> = (0..150).map(|i| text(&format!("{long_text}{i}"))).collect();
        let mut target_values = base_values.clone();
        target_values[3] = Value::from_i64(3);
        target_values[149] = Value::Null;
        let base = record(&base_values);
        let target = record(&target_values);
        assert!(target[0] & 0x80 != 0, "the header size needs two bytes");
        round_trip(&base, &target);
        round_trip(&target, &base);
    }

    #[test]
    fn the_header_size_varint_can_change_its_length() {
        // 126 one-byte serial types fit a one-byte header size. One more
        // two-byte serial type does not.
        let base_values: Vec<Value> = (0..126).map(|i| Value::from_i64(i % 2)).collect();
        let mut target_values = base_values.clone();
        target_values[0] = text(&"z".repeat(70));
        let base = record(&base_values);
        let target = record(&target_values);
        assert_ne!(base[0] & 0x80, target[0] & 0x80);
        round_trip(&base, &target);
        round_trip(&target, &base);
    }

    #[test]
    fn more_than_127_changed_columns_round_trip() {
        let base_values: Vec<Value> = (0..200).map(Value::from_i64).collect();
        let target_values: Vec<Value> = (0..200).map(|i| Value::from_i64(i + 1000)).collect();
        let delta = round_trip(&record(&base_values), &record(&target_values));
        // column_count 200 and changed_count 200 need two bytes each.
        assert_eq!(delta[..4], [0x81, 0x48, 0x81, 0x48]);
    }

    #[test]
    fn encode_stops_at_the_length_limit() {
        let base = record(&[Value::from_i64(1), text(&"a".repeat(100))]);
        let target = record(&[Value::from_i64(1), text(&"b".repeat(100))]);
        let full_len = round_trip(&base, &target).len();
        let mut delta = DeltaBytes::new();
        assert!(encode(&base, &target, full_len, &mut delta));
        assert!(!encode(&base, &target, full_len - 1, &mut delta));
    }

    #[test]
    fn the_target_can_have_more_or_fewer_columns_than_the_base() {
        let short = record(&[Value::from_i64(1), text("a")]);
        let long = record(&[Value::from_i64(1), text("a"), Value::Null, text("added")]);
        // column_count 4, changed_count 2: the two added columns.
        assert_eq!(round_trip(&short, &long)[..2], [4, 2]);
        // column_count 2, nothing changed in the first two columns.
        assert_eq!(round_trip(&long, &short), [2, 0]);
        round_trip(&record(&[]), &short);
        round_trip(&short, &record(&[]));
    }

    #[test]
    fn encode_refuses_a_target_that_apply_cannot_rebuild_exactly() {
        let base = record(&[Value::from_i64(1), text("abc")]);
        let mut delta = DeltaBytes::new();

        // The header size 3 written as a two-byte varint.
        let long_header_size = [0x80, 0x04, 0x01, 0x13, 0x01, b'a', b'b', b'c'];
        assert!(!encode(&base, &long_header_size, usize::MAX, &mut delta));

        // The serial type 1 written as a two-byte varint.
        let long_serial_type = [0x04, 0x80, 0x01, 0x13, 0x01, b'a', b'b', b'c'];
        assert!(!encode(&base, &long_serial_type, usize::MAX, &mut delta));

        // One byte after the last value.
        let mut trailing_byte = record(&[Value::from_i64(1), text("abc")]);
        trailing_byte.push(0);
        assert!(!encode(&base, &trailing_byte, usize::MAX, &mut delta));

        assert!(!encode(&base, &[0x05, 0x01], usize::MAX, &mut delta));
        assert!(!encode(&[0x05, 0x01], &base, usize::MAX, &mut delta));
    }

    #[test]
    fn apply_rejects_a_delta_that_does_not_match_the_base() {
        let base = record(&[Value::from_i64(1), text("abc")]);
        let target = record(&[Value::from_i64(2), text("abc"), Value::Null]);
        let delta = round_trip(&base, &target);
        let mut out = Vec::new();

        // The target keeps column 1 of the base, so a base of one column fails.
        let short_base = record(&[Value::from_i64(1)]);
        assert!(apply(&short_base, [&delta[..]], &mut out).is_err());

        assert!(apply(&base, [&delta[..delta.len() - 1]], &mut out).is_err());
        let mut long_delta = delta.clone();
        long_delta.push(0);
        assert!(apply(&base, [&long_delta[..]], &mut out).is_err());
    }

    #[test]
    fn random_records_round_trip() {
        let mut rng = ChaCha8Rng::seed_from_u64(0x5eed_de17a);
        for _ in 0..2000 {
            let base_len = rng.random_range(0..40);
            let base_values: Vec<Value> = (0..base_len).map(|_| random_value(&mut rng)).collect();
            let target_values = random_values_like(&base_values, &mut rng);
            let base = record(&base_values);
            let target = record(&target_values);
            round_trip(&base, &target);
            round_trip(&target, &base);
        }
    }

    #[test]
    fn a_chain_of_deltas_rebuilds_each_older_record() {
        let mut rng = ChaCha8Rng::seed_from_u64(0xc4a1_de17a);
        for _ in 0..300 {
            let len = rng.random_range(0..40);
            let mut values: Vec<Value> = (0..len).map(|_| random_value(&mut rng)).collect();
            let newest = record(&values);
            let mut older_records: Vec<Vec<u8>> = Vec::new();
            let mut deltas = Vec::new();
            for _ in 0..rng.random_range(1..=16) {
                values = random_values_like(&values, &mut rng);
                let older = record(&values);
                deltas.push(round_trip(older_records.last().unwrap_or(&newest), &older));
                older_records.push(older);
            }
            for (i, older) in older_records.iter().enumerate() {
                let mut rebuilt = Vec::new();
                apply(
                    &newest,
                    deltas[..=i].iter().map(Vec::as_slice),
                    &mut rebuilt,
                )
                .unwrap();
                assert_eq!(&rebuilt, older);
            }
        }
    }

    fn random_value(rng: &mut ChaCha8Rng) -> Value {
        match rng.random_range(0..6) {
            0 => Value::Null,
            1 => Value::from_i64(rng.random_range(0..2)),
            2 => Value::from_i64(rng.random()),
            3 => Value::from_f64(rng.random()),
            4 => {
                let len = rng.random_range(0..300);
                text(&"t".repeat(len))
            }
            _ => {
                let len = rng.random_range(0..300);
                let bytes: Vec<u8> = (0..len).map(|_| rng.random()).collect();
                blob(&bytes)
            }
        }
    }

    /// Values that keep most of the values of `base`. Sometimes they have
    /// another column count.
    fn random_values_like(base: &[Value], rng: &mut ChaCha8Rng) -> Vec<Value> {
        let len = match rng.random_range(0..4) {
            0 => rng.random_range(0..40),
            _ => base.len(),
        };
        (0..len)
            .map(|i| match base.get(i) {
                Some(value) if rng.random_bool(0.8) => value.clone(),
                _ => random_value(rng),
            })
            .collect()
    }
}
