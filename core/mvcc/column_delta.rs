//! Column patches between two SQLite records.
//!
//! The MVCC logical log can store an UPDATE of a table row as a patch that turns
//! the last committed record of the row (the base) into the new record. A patch
//! lists only the columns that differ from the base:
//!
//! ```text
//! patch = varint(column_count) varint(changed_count)
//!         { varint(column_gap) varint(serial_type) value_bytes }*
//! ```
//!
//! `column_count` is the column count of the new record. `column_gap` is the
//! distance from the previous changed column, or from column 0 for the first
//! changed column. A column that is not in the patch has the serial type and the
//! value bytes of the base column with the same index.
//!
//! [`encode`] makes a patch only for a record whose header has the canonical
//! form that `ImmutableRecord::build` writes. Thus [`apply`] gives back the same
//! bytes, if it gets the same base.

use crate::storage::sqlite3_ondisk::{read_varint, varint_len, write_varint};
use crate::types::{get_serial_type_size, Record};
use crate::{LimboError, Result};

/// Makes the patch that turns `base` into `target`, and writes it to `out`.
///
/// Returns `false` and leaves `out` empty when the header of `target` is not in
/// the canonical form, because [`apply`] cannot give back its exact bytes.
pub(crate) fn encode(base: &[u8], target: &[u8], out: &mut Vec<u8>) -> Result<bool> {
    out.clear();
    let mut base_columns = Columns::new(base)?;
    let mut target_columns = Columns::new(target)?;
    let mut column_count = 0u64;
    let mut changed_count = 0u64;
    let mut previous_changed = 0u64;
    let mut serial_types_len = 0usize;
    while let Some(column) = target_columns.next_column()? {
        if column.serial_type_len != varint_len(column.serial_type) {
            out.clear();
            return Ok(false);
        }
        serial_types_len += column.serial_type_len;
        let base_column = base_columns.next_column()?;
        let unchanged = base_column.is_some_and(|base_column| {
            base_column.serial_type == column.serial_type && base_column.value == column.value
        });
        if !unchanged {
            push_varint(out, column_count - previous_changed);
            push_varint(out, column.serial_type);
            out.extend_from_slice(column.value);
            previous_changed = column_count;
            changed_count += 1;
        }
        column_count += 1;
    }
    let canonical = target_columns.body_pos == target.len()
        && Record::calc_header_size(serial_types_len) == target_columns.header_end;
    if !canonical {
        out.clear();
        return Ok(false);
    }
    prepend_counts(out, column_count, changed_count);
    Ok(true)
}

/// Rebuilds the record that `patch` makes from `base`, and writes it to `out`.
pub(crate) fn apply(base: &[u8], patch: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let (column_count, _) = PatchEntries::new(patch)?.counts();
    let mut serial_types_len = 0usize;
    let mut body_len = 0usize;
    for_each_rebuilt_column(base, patch, |serial_type, value| {
        serial_types_len += varint_len(serial_type);
        body_len += value.len();
    })?;
    let header_size = Record::calc_header_size(serial_types_len);
    let total_size = header_size
        .checked_add(body_len)
        .ok_or_else(|| corrupt("rebuilt record size overflows usize"))?;
    out.clear();
    out.resize(total_size, 0);
    let mut header_pos = write_varint(&mut out[..], header_size as u64);
    let mut body_pos = header_size;
    let mut columns_written = 0u64;
    for_each_rebuilt_column(base, patch, |serial_type, value| {
        header_pos += write_varint(&mut out[header_pos..header_size], serial_type);
        out[body_pos..body_pos + value.len()].copy_from_slice(value);
        body_pos += value.len();
        columns_written += 1;
    })?;
    crate::turso_assert!(
        header_pos == header_size && body_pos == total_size && columns_written == column_count,
        "column patch sizing pass and writing pass disagree"
    );
    Ok(())
}

/// Joins two patches of one row: `first` turns the base into a middle record, and
/// `second` turns the middle record into the target. The result turns the base
/// into the target.
pub(crate) fn merge(first: &[u8], second: &[u8], out: &mut Vec<u8>) -> Result<()> {
    out.clear();
    let mut first_entries = PatchEntries::new(first)?;
    let mut second_entries = PatchEntries::new(second)?;
    let (middle_column_count, _) = first_entries.counts();
    let (column_count, _) = second_entries.counts();
    let mut next_first = first_entries.next_entry()?;
    let mut next_second = second_entries.next_entry()?;
    let mut changed_count = 0u64;
    let mut previous_changed = 0u64;
    loop {
        let from_second = match (&next_first, &next_second) {
            (_, Some(second_entry))
                if next_first
                    .as_ref()
                    .is_none_or(|first_entry| second_entry.index <= first_entry.index) =>
            {
                true
            }
            (Some(first_entry), _) if first_entry.index < column_count => false,
            _ => break,
        };
        let entry = if from_second {
            let entry = next_second.take().expect("entry selected above");
            if next_first
                .as_ref()
                .is_some_and(|first_entry| first_entry.index == entry.index)
            {
                next_first = first_entries.next_entry()?;
            }
            next_second = second_entries.next_entry()?;
            entry
        } else {
            let entry = next_first.take().expect("entry selected above");
            next_first = first_entries.next_entry()?;
            entry
        };
        push_varint(out, entry.index - previous_changed);
        push_varint(out, entry.serial_type);
        out.extend_from_slice(entry.value);
        previous_changed = entry.index;
        changed_count += 1;
    }
    while next_first.is_some() {
        next_first = first_entries.next_entry()?;
    }
    if column_count > middle_column_count {
        let covered = PatchEntries::new(second)?.covers_range(middle_column_count, column_count)?;
        if !covered {
            return Err(corrupt(
                "column patch reads a column that the earlier patch does not make",
            ));
        }
    }
    prepend_counts(out, column_count, changed_count);
    Ok(())
}

/// Calls `visit` with the serial type and the value bytes of each column of the
/// record that `patch` makes from `base`, in column order.
fn for_each_rebuilt_column(
    base: &[u8],
    patch: &[u8],
    mut visit: impl FnMut(u64, &[u8]),
) -> Result<()> {
    let mut entries = PatchEntries::new(patch)?;
    let (column_count, _) = entries.counts();
    let mut base_columns = Columns::new(base)?;
    let mut next_entry = entries.next_entry()?;
    for index in 0..column_count {
        let base_column = base_columns.next_column()?;
        match next_entry.as_ref() {
            Some(entry) if entry.index == index => {
                visit(entry.serial_type, entry.value);
                next_entry = entries.next_entry()?;
            }
            _ => {
                let Some(base_column) = base_column else {
                    return Err(corrupt(
                        "column patch needs a column that the base does not have",
                    ));
                };
                visit(base_column.serial_type, base_column.value);
            }
        }
    }
    crate::turso_assert!(
        next_entry.is_none(),
        "patch entries are in column order and inside the column count"
    );
    Ok(())
}

struct Column<'a> {
    serial_type: u64,
    serial_type_len: usize,
    value: &'a [u8],
}

/// Reads the columns of a record in order.
struct Columns<'a> {
    record: &'a [u8],
    header_pos: usize,
    header_end: usize,
    body_pos: usize,
}

impl<'a> Columns<'a> {
    fn new(record: &'a [u8]) -> Result<Self> {
        let (header_size, header_size_len) = read_varint(record)?;
        let header_end = usize::try_from(header_size)
            .ok()
            .filter(|header_end| *header_end >= header_size_len && *header_end <= record.len())
            .ok_or_else(|| corrupt("record header size is out of bounds"))?;
        Ok(Self {
            record,
            header_pos: header_size_len,
            header_end,
            body_pos: header_end,
        })
    }

    fn next_column(&mut self) -> Result<Option<Column<'a>>> {
        if self.header_pos >= self.header_end {
            return Ok(None);
        }
        let (serial_type, serial_type_len) =
            read_varint(&self.record[self.header_pos..self.header_end])?;
        self.header_pos += serial_type_len;
        let value_end = self
            .body_pos
            .checked_add(get_serial_type_size(serial_type)?)
            .filter(|value_end| *value_end <= self.record.len())
            .ok_or_else(|| corrupt("record value is out of bounds"))?;
        let value = &self.record[self.body_pos..value_end];
        self.body_pos = value_end;
        Ok(Some(Column {
            serial_type,
            serial_type_len,
            value,
        }))
    }
}

struct PatchEntry<'a> {
    index: u64,
    serial_type: u64,
    value: &'a [u8],
}

/// Reads the entries of a patch in column order.
struct PatchEntries<'a> {
    patch: &'a [u8],
    pos: usize,
    column_count: u64,
    changed_count: u64,
    entries_read: u64,
    previous_index: u64,
}

impl<'a> PatchEntries<'a> {
    fn new(patch: &'a [u8]) -> Result<Self> {
        let (column_count, column_count_len) = read_varint(patch)?;
        let (changed_count, changed_count_len) = read_varint(&patch[column_count_len..])?;
        if changed_count > column_count {
            return Err(corrupt("column patch changes more columns than it has"));
        }
        Ok(Self {
            patch,
            pos: column_count_len + changed_count_len,
            column_count,
            changed_count,
            entries_read: 0,
            previous_index: 0,
        })
    }

    fn counts(&self) -> (u64, u64) {
        (self.column_count, self.changed_count)
    }

    fn next_entry(&mut self) -> Result<Option<PatchEntry<'a>>> {
        if self.entries_read == self.changed_count {
            if self.pos != self.patch.len() {
                return Err(corrupt("column patch has bytes after its last entry"));
            }
            return Ok(None);
        }
        let (gap, gap_len) = read_varint(&self.patch[self.pos..])?;
        self.pos += gap_len;
        if self.entries_read > 0 && gap == 0 {
            return Err(corrupt("column patch changes one column two times"));
        }
        let index = self
            .previous_index
            .checked_add(gap)
            .filter(|index| *index < self.column_count)
            .ok_or_else(|| corrupt("column patch changes a column after the last column"))?;
        let (serial_type, serial_type_len) = read_varint(&self.patch[self.pos..])?;
        self.pos += serial_type_len;
        let value_end = self
            .pos
            .checked_add(get_serial_type_size(serial_type)?)
            .filter(|value_end| *value_end <= self.patch.len())
            .ok_or_else(|| corrupt("column patch value is out of bounds"))?;
        let value = &self.patch[self.pos..value_end];
        self.pos = value_end;
        self.previous_index = index;
        self.entries_read += 1;
        Ok(Some(PatchEntry {
            index,
            serial_type,
            value,
        }))
    }

    /// True when the patch changes every column in `start..end`.
    fn covers_range(mut self, start: u64, end: u64) -> Result<bool> {
        let mut next_expected = start;
        while let Some(entry) = self.next_entry()? {
            if entry.index < start {
                continue;
            }
            if entry.index != next_expected {
                return Ok(false);
            }
            next_expected += 1;
        }
        Ok(next_expected == end)
    }
}

fn push_varint(out: &mut Vec<u8>, value: u64) {
    let mut bytes = [0u8; 9];
    let len = write_varint(&mut bytes, value);
    out.extend_from_slice(&bytes[..len]);
}

/// Puts `varint(column_count) varint(changed_count)` before the entries that are
/// already in `out`.
fn prepend_counts(out: &mut Vec<u8>, column_count: u64, changed_count: u64) {
    let entries_len = out.len();
    push_varint(out, column_count);
    push_varint(out, changed_count);
    let counts_len = out.len() - entries_len;
    out.rotate_right(counts_len);
}

fn corrupt(message: &str) -> LimboError {
    LimboError::Corrupt(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{value_blob_from_slice, ImmutableRecord};
    use crate::Value;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn record(values: &[Value]) -> Vec<u8> {
        ImmutableRecord::from_values(values, values.len())
            .unwrap()
            .get_payload()
            .to_vec()
    }

    fn int(value: i64) -> Value {
        Value::from_i64(value)
    }

    fn real(value: f64) -> Value {
        Value::from_f64(value)
    }

    fn text(value: &str) -> Value {
        Value::build_text(value.to_string())
    }

    fn blob(len: usize, fill: u8) -> Value {
        Value::from_blob(value_blob_from_slice(&vec![fill; len]).unwrap())
    }

    fn round_trip(base: &[u8], target: &[u8]) -> Vec<u8> {
        let mut patch = Vec::new();
        assert!(encode(base, target, &mut patch).unwrap());
        let mut rebuilt = Vec::new();
        apply(base, &patch, &mut rebuilt).unwrap();
        assert_eq!(
            rebuilt, target,
            "apply(encode(base, target)) must give target"
        );
        patch
    }

    #[test]
    fn round_trip_of_all_value_kinds() {
        let values = [
            Value::Null,
            int(0),
            int(1),
            int(-1),
            int(127),
            int(-128),
            int(32_767),
            int(-32_768),
            int(8_388_607),
            int(-8_388_608),
            int(2_147_483_647),
            int(-2_147_483_648),
            int(140_737_488_355_327),
            int(-140_737_488_355_328),
            int(i64::MAX),
            int(i64::MIN),
            real(0.5),
            real(-1e300),
            text(""),
            text("a"),
            text(&"x".repeat(1000)),
            blob(0, 0),
            blob(3, 7),
            blob(5000, 9),
        ];
        for base_value in &values {
            for target_value in &values {
                let base = record(&[int(5), base_value.clone(), text("tail")]);
                let target = record(&[int(5), target_value.clone(), text("tail")]);
                round_trip(&base, &target);
            }
        }
    }

    #[test]
    fn patch_has_only_changed_columns() {
        let base = record(&[int(1), text("same"), real(2.5), int(10)]);
        let target = record(&[int(1), text("same"), real(2.5), int(11)]);
        let patch = round_trip(&base, &target);
        // column_count=4, changed_count=1, gap=3, serial type 1, value 11.
        assert_eq!(patch, [4, 1, 3, 1, 11]);
    }

    #[test]
    fn identical_records_give_an_empty_patch() {
        let row = record(&[int(1), text("same"), Value::Null]);
        let patch = round_trip(&row, &row);
        assert_eq!(patch, [3, 0]);
    }

    #[test]
    fn constants_zero_and_one_change_only_the_serial_type() {
        let base = record(&[int(0), int(1)]);
        let target = record(&[int(1), int(0)]);
        let patch = round_trip(&base, &target);
        // column_count=2, changed_count=2, (gap 0, serial 9), (gap 1, serial 8).
        assert_eq!(patch, [2, 2, 0, 9, 1, 8]);
    }

    #[test]
    fn header_of_more_than_127_bytes() {
        let base_values: Vec<Value> = (0..200).map(|i| text(&format!("value {i}"))).collect();
        let mut target_values = base_values.clone();
        target_values[150] = int(42);
        target_values[199] = blob(300, 1);
        let base = record(&base_values);
        let target = record(&target_values);
        assert!(target[0] & 0x80 != 0, "header size must need two bytes");
        round_trip(&base, &target);
        round_trip(&target, &base);
    }

    #[test]
    fn header_size_changes_from_one_byte_to_two_bytes() {
        let base_values: Vec<Value> = (0..126).map(|_| Value::Null).collect();
        let mut target_values = base_values.clone();
        target_values[0] = text(&"y".repeat(100));
        let base = record(&base_values);
        let target = record(&target_values);
        assert_eq!(base[0], 127);
        assert!(target[0] & 0x80 != 0);
        round_trip(&base, &target);
        round_trip(&target, &base);
    }

    #[test]
    fn target_with_more_columns_than_base() {
        let base = record(&[int(1), text("a")]);
        let target = record(&[int(1), text("a"), int(3), Value::Null]);
        let patch = round_trip(&base, &target);
        assert_eq!(patch, [4, 2, 2, 1, 3, 1, 0]);
    }

    #[test]
    fn target_with_fewer_columns_than_base() {
        let base = record(&[int(1), text("a"), int(3), Value::Null]);
        let target = record(&[int(1), text("a")]);
        let patch = round_trip(&base, &target);
        assert_eq!(patch, [2, 0]);
    }

    #[test]
    fn many_changed_columns_need_a_two_byte_count() {
        let base_values: Vec<Value> = (0..300).map(int).collect();
        let target_values: Vec<Value> = (0..300).map(|i| int(i + 1000)).collect();
        let patch = round_trip(&record(&base_values), &record(&target_values));
        assert_eq!(&patch[..4], [0x82, 0x2c, 0x82, 0x2c]);
    }

    #[test]
    fn non_canonical_target_header_gives_no_patch() {
        let canonical = record(&[int(7), text("abc")]);
        // A two-byte varint for the serial type of column 0.
        let mut long_serial_type = vec![canonical[0] + 1, 0x80, canonical[1]];
        long_serial_type.extend_from_slice(&canonical[2..]);
        let mut patch = vec![1];
        assert!(!encode(&canonical, &long_serial_type, &mut patch).unwrap());
        assert!(patch.is_empty());

        // Bytes after the last value.
        let mut trailing_bytes = canonical.clone();
        trailing_bytes.push(0);
        assert!(!encode(&canonical, &trailing_bytes, &mut patch).unwrap());
    }

    #[test]
    fn apply_rejects_a_base_without_a_needed_column() {
        let base = record(&[int(1), text("a"), int(3)]);
        let target = record(&[int(1), text("a"), int(4)]);
        let mut patch = Vec::new();
        assert!(encode(&base, &target, &mut patch).unwrap());
        let short_base = record(&[int(1)]);
        let mut out = Vec::new();
        assert!(matches!(
            apply(&short_base, &patch, &mut out),
            Err(LimboError::Corrupt(_))
        ));
    }

    #[test]
    fn apply_rejects_malformed_patches() {
        let base = record(&[int(1), text("a")]);
        let mut out = Vec::new();
        let malformed: [&[u8]; 6] = [
            // More changed columns than columns.
            &[1, 2, 0, 1, 5, 1, 1, 6],
            // A column index after the last column.
            &[2, 1, 2, 1, 5],
            // The same column two times.
            &[2, 2, 0, 1, 5, 0, 1, 6],
            // A value that ends after the patch.
            &[2, 1, 0, 6, 1],
            // Bytes after the last entry.
            &[2, 0, 9],
            // Reserved serial type 10.
            &[2, 1, 0, 10],
        ];
        for patch in malformed {
            assert!(
                matches!(apply(&base, patch, &mut out), Err(LimboError::Corrupt(_))),
                "patch {patch:?} must be rejected"
            );
        }
    }

    #[test]
    fn merge_keeps_the_last_value_of_each_column() {
        let base = record(&[int(1), text("a"), int(3), real(1.5)]);
        let middle = record(&[int(1), text("b"), int(3), real(2.5)]);
        let target = record(&[int(9), text("b"), int(3), real(3.5)]);
        let first = round_trip(&base, &middle);
        let second = round_trip(&middle, &target);
        let mut merged = Vec::new();
        merge(&first, &second, &mut merged).unwrap();
        let mut rebuilt = Vec::new();
        apply(&base, &merged, &mut rebuilt).unwrap();
        assert_eq!(rebuilt, target);
        // Columns 0, 1 and 3 changed.
        assert_eq!(merged[..2], [4, 3]);
    }

    #[test]
    fn merge_with_column_count_changes() {
        let base = record(&[int(1), text("a")]);
        let wider = record(&[int(1), text("a"), int(5), text("new")]);
        let narrower = record(&[int(2)]);
        let wider_again = record(&[int(2), text("a"), Value::Null]);
        let records = [&base, &wider, &narrower, &wider_again];
        for start in 0..records.len() {
            let mut merged = round_trip(records[start], records[start]);
            for end in start + 1..records.len() {
                let next = round_trip(records[end - 1], records[end]);
                let mut joined = Vec::new();
                merge(&merged, &next, &mut joined).unwrap();
                merged = joined;
                let mut rebuilt = Vec::new();
                apply(records[start], &merged, &mut rebuilt).unwrap();
                assert_eq!(&rebuilt, records[end], "merge from {start} to {end}");
            }
        }
    }

    #[test]
    fn merge_rejects_a_second_patch_that_reads_a_missing_middle_column() {
        // The first patch makes a record with one column. The second patch claims
        // three columns, but it gives only column 2, so column 1 is not known.
        let first = [1u8, 0];
        let second = [3u8, 1, 2, 1, 5];
        let mut out = Vec::new();
        assert!(matches!(
            merge(&first, &second, &mut out),
            Err(LimboError::Corrupt(_))
        ));
    }

    fn random_value(rng: &mut ChaCha8Rng) -> Value {
        match rng.random_range(0..7) {
            0 => Value::Null,
            1 => int(rng.random_range(0..2)),
            2 => int(rng.random_range(-200..200)),
            3 => int(rng.random()),
            4 => real(rng.random_range(-1e6..1e6)),
            5 => text(&"t".repeat(rng.random_range(0..300))),
            _ => blob(rng.random_range(0..300), rng.random()),
        }
    }

    fn random_record(rng: &mut ChaCha8Rng, base: Option<&[Value]>) -> Vec<Value> {
        let column_count = match base {
            Some(base) if rng.random_bool(0.8) => base.len(),
            _ => rng.random_range(0..40),
        };
        (0..column_count)
            .map(|i| match base.and_then(|base| base.get(i)) {
                Some(value) if rng.random_bool(0.7) => value.clone(),
                _ => random_value(rng),
            })
            .collect()
    }

    #[test]
    fn random_records_round_trip_and_merge() {
        let seed = rand::rng().random();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        for _ in 0..2000 {
            let base_values = random_record(&mut rng, None);
            let middle_values = random_record(&mut rng, Some(&base_values));
            let target_values = random_record(&mut rng, Some(&middle_values));
            let base = record(&base_values);
            let middle = record(&middle_values);
            let target = record(&target_values);
            let first = round_trip(&base, &middle);
            let second = round_trip(&middle, &target);
            let mut merged = Vec::new();
            merge(&first, &second, &mut merged).unwrap();
            let mut rebuilt = Vec::new();
            apply(&base, &merged, &mut rebuilt).unwrap();
            assert_eq!(rebuilt, target, "seed {seed}");
        }
    }
}
