//! Column patches of SQLite records.
//!
//! A patch turns one record, the base, into another record, the target:
//!
//! ```text
//! patch = varint(column_count) varint(set_count)
//!         { varint(column_gap) varint(serial_type) value_bytes }*
//! ```
//!
//! `column_count` is the column count of the target. The patch sets
//! `set_count` columns. `column_gap` is the distance from the previous column
//! that the patch sets, or from column 0. The target takes every other column
//! from the base.
//!
//! A patch sets columns to values. Thus it makes the same target from every
//! base that has the target's value in each column that the patch does not
//! set.

use crate::storage::sqlite3_ondisk::{read_varint, varint_len, write_varint, write_varint_to_vec};
use crate::types::SerialType;
use crate::{LimboError, Result};
use smallvec::SmallVec;

/// Writes into `out` a patch that turns `base` into `target`. The patch sets
/// each column in which the two records differ, and also each column that the
/// patch `also_set` sets.
///
/// Returns `false`, and writes nothing, when the patch would be longer than
/// `max_len` bytes, or when `target` does not have the encoding that [`apply`]
/// writes. In the second case a patch cannot make the same bytes.
pub(crate) fn diff(
    base: &[u8],
    target: &[u8],
    also_set: Option<&[u8]>,
    max_len: usize,
    out: &mut Vec<u8>,
) -> Result<bool> {
    let base_columns = parse_record(base)?;
    let target_columns = parse_record(target)?;
    if record_size(&target_columns) != target.len() {
        return Ok(false);
    }
    let mut also_set = match also_set {
        Some(patch) => Some(PatchReader::new(patch)?),
        None => None,
    };
    let mut next_also_set = next_set_column(&mut also_set)?;
    let mut set_columns: SmallVec<[usize; 16]> = SmallVec::new();
    let mut set_columns_len = 0;
    for (index, column) in target_columns.iter().enumerate() {
        while next_also_set.is_some_and(|set| set.index < index) {
            next_also_set = next_set_column(&mut also_set)?;
        }
        let set_by_also_set = next_also_set.is_some_and(|set| set.index == index);
        if set_by_also_set || base_columns.get(index) != Some(column) {
            let gap = index - set_columns.last().copied().unwrap_or(0);
            set_columns_len +=
                varint_len(gap as u64) + varint_len(column.serial_type) + column.value.len();
            set_columns.push(index);
        }
    }
    let patch_len = varint_len(target_columns.len() as u64)
        + varint_len(set_columns.len() as u64)
        + set_columns_len;
    if patch_len > max_len {
        return Ok(false);
    }

    out.clear();
    write_varint_to_vec(target_columns.len() as u64, out);
    write_varint_to_vec(set_columns.len() as u64, out);
    let mut previous = 0;
    for index in set_columns {
        let column = target_columns[index];
        write_varint_to_vec((index - previous) as u64, out);
        write_varint_to_vec(column.serial_type, out);
        out.extend_from_slice(column.value);
        previous = index;
    }
    Ok(true)
}

/// Writes into `out` the record that `patch` makes from `base`.
pub(crate) fn apply(base: &[u8], patch: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let base_columns = parse_record(base)?;
    let reader = PatchReader::new(patch)?;
    if apply_in_place(base, &base_columns, reader.clone(), out)? {
        return Ok(());
    }
    let mut reader = reader;
    let mut columns: Columns<'_> = SmallVec::with_capacity(reader.column_count);
    let mut next_set = reader.next_column()?;
    for index in 0..reader.column_count {
        match next_set {
            Some(set) if set.index == index => {
                columns.push(set.column);
                next_set = reader.next_column()?;
            }
            _ => {
                let Some(column) = base_columns.get(index) else {
                    crate::bail_corrupt_error!(
                        "row patch needs column {index}, but its base record has {} columns",
                        base_columns.len()
                    );
                };
                columns.push(*column);
            }
        }
    }
    if let Some(set) = next_set {
        crate::bail_corrupt_error!(
            "row patch sets column {} of a record with {} columns",
            set.index,
            reader.column_count
        );
    }
    write_record(&columns, out);
    Ok(())
}

/// Writes into `out` the record that `patch` makes from `base` by copying
/// `base` and overwriting the bytes of each column that the patch sets. This
/// works when the patch keeps the column count, and each column that it sets
/// keeps the size of its value and of its serial type. Then the header keeps
/// its size and each value keeps its offset. Returns `false` otherwise, and
/// `out` then holds no record.
fn apply_in_place(
    base: &[u8],
    base_columns: &Columns<'_>,
    mut patch: PatchReader<'_>,
    out: &mut Vec<u8>,
) -> Result<bool> {
    let Some(last) = base_columns.last() else {
        return Ok(false);
    };
    let record_end = offset_in(base, last.value) + last.value.len();
    if patch.column_count != base_columns.len() || record_end != base.len() {
        return Ok(false);
    }
    out.clear();
    out.extend_from_slice(base);
    let (_, mut header_offset) = read_varint(base)?;
    let mut header_index = 0;
    while let Some(set) = patch.next_column()? {
        while header_index < set.index {
            header_offset += read_varint(&base[header_offset..])?.1;
            header_index += 1;
        }
        let serial_type_len = read_varint(&base[header_offset..])?.1;
        let old = base_columns[set.index];
        if set.column.value.len() != old.value.len()
            || varint_len(set.column.serial_type) != serial_type_len
        {
            out.clear();
            return Ok(false);
        }
        write_varint(&mut out[header_offset..], set.column.serial_type);
        let value_offset = offset_in(base, old.value);
        out[value_offset..value_offset + old.value.len()].copy_from_slice(set.column.value);
    }
    Ok(true)
}

/// The offset of `part`, a subslice of `record`, in `record`.
fn offset_in(record: &[u8], part: &[u8]) -> usize {
    part.as_ptr() as usize - record.as_ptr() as usize
}

/// The column count of the record that `patch` makes.
pub(crate) fn column_count(patch: &[u8]) -> Result<usize> {
    Ok(PatchReader::new(patch)?.column_count)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Column<'a> {
    serial_type: u64,
    value: &'a [u8],
}

type Columns<'a> = SmallVec<[Column<'a>; 32]>;

#[derive(Clone, Copy)]
struct SetColumn<'a> {
    index: usize,
    column: Column<'a>,
}

#[derive(Clone)]
struct PatchReader<'a> {
    patch: &'a [u8],
    offset: usize,
    column_count: usize,
    remaining: u64,
    index: usize,
    first: bool,
}

impl<'a> PatchReader<'a> {
    fn new(patch: &'a [u8]) -> Result<Self> {
        let (column_count, column_count_len) = read_varint(patch)?;
        let (set_count, set_count_len) = read_varint(
            patch
                .get(column_count_len..)
                .ok_or_else(|| corrupt_patch("no set count"))?,
        )?;
        if set_count > column_count {
            return Err(corrupt_patch("sets more columns than the record has"));
        }
        Ok(Self {
            patch,
            offset: column_count_len + set_count_len,
            column_count: usize::try_from(column_count)
                .map_err(|_| corrupt_patch("column count does not fit in memory"))?,
            remaining: set_count,
            index: 0,
            first: true,
        })
    }

    fn next_column(&mut self) -> Result<Option<SetColumn<'a>>> {
        if self.remaining == 0 {
            if self.offset != self.patch.len() {
                return Err(corrupt_patch("bytes after the last column"));
            }
            return Ok(None);
        }
        self.remaining -= 1;
        let gap = self.read_varint()?;
        if !self.first && gap == 0 {
            return Err(corrupt_patch("sets a column twice"));
        }
        self.first = false;
        self.index = usize::try_from(gap)
            .ok()
            .and_then(|gap| self.index.checked_add(gap))
            .filter(|index| *index < self.column_count)
            .ok_or_else(|| corrupt_patch("column gap past the last column"))?;
        let serial_type = self.read_varint()?;
        let size = SerialType::try_from(serial_type)?.size();
        let value = self
            .offset
            .checked_add(size)
            .and_then(|end| self.patch.get(self.offset..end))
            .ok_or_else(|| corrupt_patch("value past the end of the patch"))?;
        self.offset += size;
        Ok(Some(SetColumn {
            index: self.index,
            column: Column { serial_type, value },
        }))
    }

    fn read_varint(&mut self) -> Result<u64> {
        let (value, len) = read_varint(
            self.patch
                .get(self.offset..)
                .ok_or_else(|| corrupt_patch("varint past the end of the patch"))?,
        )?;
        self.offset += len;
        Ok(value)
    }
}

fn next_set_column<'a>(reader: &mut Option<PatchReader<'a>>) -> Result<Option<SetColumn<'a>>> {
    match reader {
        Some(reader) => reader.next_column(),
        None => Ok(None),
    }
}

fn corrupt_patch(reason: &str) -> LimboError {
    LimboError::Corrupt(format!("invalid row patch: {reason}"))
}

/// The columns of `record`, in order.
fn parse_record(record: &[u8]) -> Result<Columns<'_>> {
    let (header_size, mut header_offset) = read_varint(record)?;
    let header_size = usize::try_from(header_size)
        .ok()
        .filter(|size| *size >= header_offset && *size <= record.len())
        .ok_or_else(|| LimboError::Corrupt("record header size out of range".to_string()))?;
    let mut body_offset = header_size;
    let mut columns = Columns::new();
    while header_offset < header_size {
        let (serial_type, len) = read_varint(&record[header_offset..header_size])?;
        header_offset += len;
        let size = SerialType::try_from(serial_type)?.size();
        let value = body_offset
            .checked_add(size)
            .and_then(|end| record.get(body_offset..end))
            .ok_or_else(|| LimboError::Corrupt("record value past the end".to_string()))?;
        body_offset += size;
        columns.push(Column { serial_type, value });
    }
    Ok(columns)
}

/// Appends the record of `columns` to `out`, with the shortest header.
fn write_record(columns: &[Column<'_>], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(record_size(columns));
    write_varint_to_vec(header_size(columns) as u64, out);
    for column in columns {
        write_varint_to_vec(column.serial_type, out);
    }
    for column in columns {
        out.extend_from_slice(column.value);
    }
}

/// The size of the record that [`write_record`] writes for `columns`.
fn record_size(columns: &[Column<'_>]) -> usize {
    header_size(columns)
        + columns
            .iter()
            .map(|column| column.value.len())
            .sum::<usize>()
}

/// The header size of a record, which includes the varint of the header size.
fn header_size(columns: &[Column<'_>]) -> usize {
    let serial_types: usize = columns
        .iter()
        .map(|column| varint_len(column.serial_type))
        .sum();
    let mut size = serial_types + 1;
    while serial_types + varint_len(size as u64) != size {
        size = serial_types + varint_len(size as u64);
    }
    size
}

#[cfg(test)]
mod tests {
    use super::{apply, apply_in_place, column_count, diff, parse_record, PatchReader};
    use crate::types::{ImmutableRecord, Value};
    use crate::LimboError;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn record(values: &[Value]) -> Vec<u8> {
        ImmutableRecord::from_values(values, values.len())
            .unwrap()
            .into_payload()
            .to_vec()
    }

    fn patch(base: &[u8], target: &[u8], also_set: Option<&[u8]>) -> Vec<u8> {
        let mut out = Vec::new();
        assert!(diff(base, target, also_set, usize::MAX, &mut out).unwrap());
        out
    }

    fn rebuilt(base: &[u8], patch: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        apply(base, patch, &mut out).unwrap();
        out
    }

    fn assert_round_trip(base: &[Value], target: &[Value]) -> Vec<u8> {
        let base = record(base);
        let target = record(target);
        let patch = patch(&base, &target, None);
        assert_eq!(rebuilt(&base, &patch), target);
        assert_eq!(
            column_count(&patch).unwrap(),
            ImmutableRecord::from_bin_record(target).column_count()
        );
        patch
    }

    fn text(s: &str) -> Value {
        Value::build_text(s.to_string())
    }

    fn blob(bytes: &[u8]) -> Value {
        Value::from_slice(bytes).unwrap()
    }

    #[test]
    fn a_patch_of_equal_records_sets_no_column() {
        let row = [Value::from_i64(1), text("a"), Value::Null];
        let patch = assert_round_trip(&row, &row);
        assert_eq!(patch, [3, 0]);
    }

    #[test]
    fn a_patch_sets_only_the_changed_columns() {
        let base = [
            Value::from_i64(1),
            text("abcdefghijklmnop"),
            Value::from_i64(7),
        ];
        let target = [
            Value::from_i64(1),
            text("abcdefghijklmnop"),
            Value::from_i64(8),
        ];
        let patch = assert_round_trip(&base, &target);
        // column count 3, one column, gap 2, serial type 1, value 8.
        assert_eq!(patch, [3, 1, 2, 1, 8]);
    }

    #[test]
    fn every_value_type_round_trips() {
        let values = [
            Value::Null,
            Value::from_i64(0),
            Value::from_i64(1),
            Value::from_i64(-1),
            Value::from_i64(127),
            Value::from_i64(-32768),
            Value::from_i64(8_388_607),
            Value::from_i64(i32::MIN as i64),
            Value::from_i64(140_737_488_355_327),
            Value::from_i64(i64::MAX),
            Value::from_i64(i64::MIN),
            Value::from_f64(0.5),
            Value::from_f64(-1e300),
            text(""),
            text("hello"),
            blob(&[]),
            blob(&[0, 1, 2, 3]),
        ];
        for base in &values {
            for target in &values {
                assert_round_trip(
                    &[base.clone(), text("x"), base.clone()],
                    &[target.clone(), text("x"), target.clone()],
                );
            }
        }
    }

    #[test]
    fn a_header_of_more_than_127_bytes_round_trips() {
        let base: Vec<Value> = (0..200).map(|i| Value::from_i64(i * 1000)).collect();
        let mut target = base.clone();
        target[150] = text("changed");
        let base_record = record(&base);
        assert!(
            base_record[0] & 0x80 != 0,
            "the header size needs two bytes"
        );
        assert_round_trip(&base, &target);
    }

    #[test]
    fn the_header_size_varint_can_change_length() {
        let base: Vec<Value> = (0..120).map(|_| Value::from_i64(1)).collect();
        let mut target = base.clone();
        for value in target.iter_mut().take(10) {
            *value = text("a text that needs a two byte serial type, because it is long......");
        }
        let base_record = record(&base);
        let target_record = record(&target);
        assert!(base_record[0] & 0x80 == 0);
        assert!(target_record[0] & 0x80 != 0);
        assert_round_trip(&base, &target);
        assert_round_trip(&target, &base);
    }

    #[test]
    fn a_target_with_more_columns_takes_the_new_columns_from_the_patch() {
        let base = [Value::from_i64(1), text("a")];
        let target = [
            Value::from_i64(1),
            text("a"),
            Value::Null,
            Value::from_i64(5),
        ];
        let patch = assert_round_trip(&base, &target);
        assert_eq!(column_count(&patch).unwrap(), 4);
    }

    #[test]
    fn a_target_with_fewer_columns_drops_the_last_columns_of_the_base() {
        let base = [Value::from_i64(1), text("a"), Value::from_i64(3)];
        let target = [Value::from_i64(1), text("a")];
        let patch = assert_round_trip(&base, &target);
        assert_eq!(patch, [2, 0]);
    }

    #[test]
    fn also_set_keeps_the_columns_of_an_earlier_patch() {
        let base = record(&[Value::from_i64(1), Value::from_i64(2), Value::from_i64(3)]);
        let first = record(&[Value::from_i64(9), Value::from_i64(2), Value::from_i64(3)]);
        let second = record(&[Value::from_i64(9), Value::from_i64(2), Value::from_i64(4)]);
        let first_patch = patch(&base, &first, None);
        // The second version keeps the first change. A patch from the first
        // version alone does not set column 0, so it is wrong on the base.
        let without_also_set = patch(&first, &second, None);
        assert_eq!(rebuilt(&first, &without_also_set), second);
        assert_ne!(rebuilt(&base, &without_also_set), second);
        let with_also_set = patch(&first, &second, Some(&first_patch));
        assert_eq!(rebuilt(&first, &with_also_set), second);
        assert_eq!(rebuilt(&base, &with_also_set), second);
    }

    #[test]
    fn also_set_keeps_a_column_that_a_later_version_sets_back() {
        let base = record(&[Value::from_i64(1), Value::from_i64(2)]);
        let first = record(&[Value::from_i64(9), Value::from_i64(2)]);
        let second = record(&[Value::from_i64(1), Value::from_i64(5)]);
        let first_patch = patch(&base, &first, None);
        let second_patch = patch(&first, &second, Some(&first_patch));
        assert_eq!(rebuilt(&base, &second_patch), second);
        assert_eq!(rebuilt(&first, &second_patch), second);
    }

    #[test]
    fn a_patch_makes_the_same_record_from_its_own_result() {
        let base = record(&[Value::from_i64(1), text("abc"), Value::from_f64(2.5)]);
        let target = record(&[Value::from_i64(2), text("abc"), Value::from_f64(3.5)]);
        let patch = patch(&base, &target, None);
        assert_eq!(rebuilt(&target, &patch), target);
    }

    #[test]
    fn random_records_round_trip() {
        let mut rng = ChaCha8Rng::seed_from_u64(0x0B7E_E9A7);
        let random_value = |rng: &mut ChaCha8Rng| match rng.random_range(0..6) {
            0 => Value::Null,
            1 => Value::from_i64(rng.random_range(-3..3)),
            2 => Value::from_i64(rng.random()),
            3 => Value::from_f64(rng.random_range(-1e6..1e6)),
            4 => text(&"t".repeat(rng.random_range(0..300))),
            _ => blob(&vec![7; rng.random_range(0..300)]),
        };
        for _ in 0..2000 {
            let base_len = rng.random_range(0..40);
            let base: Vec<Value> = (0..base_len).map(|_| random_value(&mut rng)).collect();
            let target_len = rng.random_range(0..40);
            let target: Vec<Value> = (0..target_len)
                .map(|i| match base.get(i) {
                    Some(value) if rng.random_bool(0.7) => value.clone(),
                    _ => random_value(&mut rng),
                })
                .collect();
            let middle_len = rng.random_range(0..40);
            let middle: Vec<Value> = (0..middle_len).map(|_| random_value(&mut rng)).collect();
            let base = record(&base);
            let middle = record(&middle);
            let target = record(&target);
            let direct = patch(&base, &target, None);
            assert_eq!(rebuilt(&base, &direct), target);
            let first = patch(&base, &middle, None);
            let second = patch(&middle, &target, Some(&first));
            assert_eq!(rebuilt(&middle, &second), target);
            assert_eq!(rebuilt(&base, &second), target);
        }
    }

    #[test]
    fn diff_refuses_a_target_with_a_longer_header_than_needed() {
        let base = record(&[Value::from_i64(1)]);
        // Header size 3 in two bytes, serial type 1, value 5.
        let target = [0x80, 0x03, 0x01, 0x05];
        let mut out = Vec::new();
        assert!(!diff(&base, &target, None, usize::MAX, &mut out).unwrap());
    }

    #[test]
    fn a_patch_that_keeps_the_sizes_overwrites_a_copy_of_the_base() {
        let base = record(&[
            Value::from_i64(1),
            Value::from_i64(10),
            Value::build_text("abc"),
        ]);
        let same_sizes = record(&[
            Value::from_i64(1),
            Value::from_i64(20),
            Value::build_text("xyz"),
        ]);
        let in_place = |patch: &[u8]| {
            let mut out = Vec::new();
            let columns = parse_record(&base).unwrap();
            let done = apply_in_place(&base, &columns, PatchReader::new(patch).unwrap(), &mut out)
                .unwrap();
            done.then_some(out)
        };
        let same_sizes_patch = patch(&base, &same_sizes, None);
        assert_eq!(in_place(&same_sizes_patch), Some(same_sizes));

        // A value of another size, or another column count, needs a new header.
        for target in [
            record(&[
                Value::from_i64(1),
                Value::from_i64(1000),
                Value::build_text("abc"),
            ]),
            record(&[Value::from_i64(1), Value::from_i64(10)]),
        ] {
            let other_sizes_patch = patch(&base, &target, None);
            assert_eq!(in_place(&other_sizes_patch), None);
            assert_eq!(rebuilt(&base, &other_sizes_patch), target);
        }
    }

    #[test]
    fn diff_refuses_a_patch_longer_than_its_limit() {
        let base = record(&[Value::from_i64(1), Value::build_text("a")]);
        let target = record(&[Value::from_i64(1), Value::build_text("bcd")]);
        let full = patch(&base, &target, None);
        let mut out = Vec::new();
        assert!(!diff(&base, &target, None, full.len() - 1, &mut out).unwrap());
        assert!(out.is_empty());
        assert!(diff(&base, &target, None, full.len(), &mut out).unwrap());
        assert_eq!(out, full);
    }

    #[test]
    fn apply_needs_every_column_that_the_patch_does_not_set() {
        let base = record(&[Value::from_i64(1)]);
        let target = record(&[Value::from_i64(1), Value::from_i64(2), Value::from_i64(3)]);
        let patch = patch(&base, &target, None);
        let short_base = record(&[]);
        let mut out = Vec::new();
        assert!(matches!(
            apply(&short_base, &patch, &mut out),
            Err(LimboError::Corrupt(_))
        ));
    }

    #[test]
    fn apply_rejects_corrupt_patches() {
        let base = record(&[Value::from_i64(1), Value::from_i64(2)]);
        let corrupt: [&[u8]; 7] = [
            &[],
            &[2],
            &[2, 3, 0, 1, 1, 1, 1, 1, 1],
            &[2, 1, 2, 1, 1],
            &[2, 2, 0, 1, 1, 0, 1, 1],
            &[2, 1, 0, 1],
            &[2, 1, 0, 1, 1, 9],
        ];
        for patch in corrupt {
            let mut out = Vec::new();
            assert!(
                matches!(apply(&base, patch, &mut out), Err(LimboError::Corrupt(_))),
                "patch {patch:?}"
            );
        }
    }
}
