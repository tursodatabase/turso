use proptest::prelude::*;
use proptest::{prop_oneof, proptest};

use super::*;
use crate::{ColumnarReader, ColumnarWriter, DynamicColumnHandle};

#[test]
fn test_optional_index_bug_2293() {
    // tests for panic in docid_range_to_rowids for docid == num_docs
    test_optional_index_with_num_docs(ELEMENTS_PER_BLOCK - 1);
    test_optional_index_with_num_docs(ELEMENTS_PER_BLOCK);
    test_optional_index_with_num_docs(ELEMENTS_PER_BLOCK + 1);
}
fn test_optional_index_with_num_docs(num_docs: u32) {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_numerical(100, "score", 80i64);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(num_docs, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("score").unwrap();
    assert_eq!(cols.len(), 1);

    let col = cols[0].open().unwrap();
    col.column_index().docid_range_to_rowids(0..num_docs);
}

#[test]
fn test_dense_block_threshold() {
    assert_eq!(super::DENSE_BLOCK_THRESHOLD, 5_120);
}

fn random_bitvec() -> BoxedStrategy<Vec<bool>> {
    prop_oneof![
        1 => prop::collection::vec(proptest::bool::weighted(1.0), 0..100),
        1 => prop::collection::vec(proptest::bool::weighted(0.00), 0..(ELEMENTS_PER_BLOCK as usize * 3)), // empty blocks
        1 => prop::collection::vec(proptest::bool::weighted(1.00), 0..(ELEMENTS_PER_BLOCK as usize + 10)), // full block
        1 => prop::collection::vec(proptest::bool::weighted(0.01), 0..100),
        1 => prop::collection::vec(proptest::bool::weighted(0.01), 0..u16::MAX as usize),
        8 => vec![any::<bool>()],
    ]
    .boxed()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]
    #[test]
    fn test_with_random_bitvecs(bitvec1 in random_bitvec(), bitvec2 in random_bitvec(), bitvec3 in random_bitvec()) {
        let mut bitvec = Vec::new();
        bitvec.extend_from_slice(&bitvec1);
        bitvec.extend_from_slice(&bitvec2);
        bitvec.extend_from_slice(&bitvec3);
        test_null_index(&bitvec[..]);
    }
}

#[test]
fn test_with_random_sets_simple() {
    let vals = 10..ELEMENTS_PER_BLOCK * 2;
    let mut out: Vec<u8> = Vec::new();
    serialize_optional_index(&vals, 100, &mut out).unwrap();
    let null_index = open_optional_index(OwnedBytes::new(out)).unwrap();
    let ranks: Vec<u32> = (65_472u32..65_473u32).collect();
    let els: Vec<u32> = ranks.iter().copied().map(|rank| rank + 10).collect();
    let mut select_cursor = null_index.select_cursor();
    for (rank, el) in ranks.iter().copied().zip(els.iter().copied()) {
        assert_eq!(select_cursor.select(rank), el);
    }
}

#[test]
fn test_optional_index_trailing_empty_blocks() {
    test_null_index(&[false]);
}

#[test]
fn test_optional_index_one_block_false() {
    let mut iter = vec![false; ELEMENTS_PER_BLOCK as usize];
    iter.push(true);
    test_null_index(&iter[..]);
}

#[test]
fn test_optional_index_one_block_true() {
    let mut iter = vec![true; ELEMENTS_PER_BLOCK as usize];
    iter.push(true);
    test_null_index(&iter[..]);
}

impl<'a> Iterable<RowId> for &'a [bool] {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = RowId> + 'a> {
        Box::new(
            self.iter()
                .cloned()
                .enumerate()
                .filter(|(_pos, val)| *val)
                .map(|(pos, _val)| pos as u32),
        )
    }
}

fn test_null_index(data: &[bool]) {
    let mut out: Vec<u8> = Vec::new();
    serialize_optional_index(&data, data.len() as RowId, &mut out).unwrap();
    let null_index = open_optional_index(OwnedBytes::new(out)).unwrap();
    let orig_idx_with_value: Vec<u32> = data
        .iter()
        .enumerate()
        .filter(|(_pos, val)| **val)
        .map(|(pos, _val)| pos as u32)
        .collect();
    let mut select_iter = null_index.select_cursor();
    for (i, expected) in orig_idx_with_value.iter().enumerate() {
        assert_eq!(select_iter.select(i as u32), *expected);
    }

    let step_size = (orig_idx_with_value.len() / 100).max(1);
    for (dense_idx, orig_idx) in orig_idx_with_value.iter().enumerate().step_by(step_size) {
        assert_eq!(null_index.rank_if_exists(*orig_idx), Some(dense_idx as u32));
    }

    // 100 samples
    let step_size = (data.len() / 100).max(1);
    for (pos, value) in data.iter().enumerate().step_by(step_size) {
        assert_eq!(null_index.contains(pos as u32), *value);
    }
}

#[test]
fn test_optional_index_test_translation() {
    let optional_index = OptionalIndex::for_test(4, &[0, 2]);
    let mut select_cursor = optional_index.select_cursor();
    assert_eq!(select_cursor.select(0), 0);
    assert_eq!(select_cursor.select(1), 2);
}

#[test]
fn test_optional_index_translate() {
    let optional_index = OptionalIndex::for_test(4, &[0, 2]);
    assert_eq!(optional_index.rank_if_exists(0), Some(0));
    assert_eq!(optional_index.rank_if_exists(2), Some(1));
}

#[test]
fn test_optional_index_small() {
    let optional_index = OptionalIndex::for_test(4, &[0, 2]);
    assert!(optional_index.contains(0));
    assert!(!optional_index.contains(1));
    assert!(optional_index.contains(2));
    assert!(!optional_index.contains(3));
}

#[test]
fn test_optional_index_large() {
    let row_ids = &[ELEMENTS_PER_BLOCK, ELEMENTS_PER_BLOCK + 1];
    let optional_index = OptionalIndex::for_test(ELEMENTS_PER_BLOCK + 2, row_ids);
    assert!(!optional_index.contains(0));
    assert!(!optional_index.contains(100));
    assert!(!optional_index.contains(ELEMENTS_PER_BLOCK - 1));
    assert!(optional_index.contains(ELEMENTS_PER_BLOCK));
    assert!(optional_index.contains(ELEMENTS_PER_BLOCK + 1));
}

fn test_optional_index_iter_aux(row_ids: &[RowId], num_rows: RowId) {
    let optional_index = OptionalIndex::for_test(num_rows, row_ids);
    assert_eq!(optional_index.num_docs(), num_rows);
    assert!(
        optional_index
            .iter_non_null_docs()
            .eq(row_ids.iter().copied())
    );
}

#[test]
fn test_optional_index_iter_empty() {
    test_optional_index_iter_aux(&[], 0u32);
}

fn test_optional_index_rank_aux(row_ids: &[RowId]) {
    let num_rows = row_ids.last().copied().unwrap_or(0u32) + 1;
    let null_index = OptionalIndex::for_test(num_rows, row_ids);
    assert_eq!(null_index.num_docs(), num_rows);
    for (row_id, row_val) in row_ids.iter().copied().enumerate() {
        assert_eq!(null_index.rank(row_val), row_id as u32);
        assert_eq!(null_index.rank_if_exists(row_val), Some(row_id as u32));
        if row_val > 0 && !null_index.contains(&row_val - 1) {
            assert_eq!(null_index.rank(row_val - 1), row_id as u32);
        }
        assert_eq!(null_index.rank(row_val + 1), row_id as u32 + 1);
    }
}

#[test]
fn test_optional_index_rank() {
    test_optional_index_rank_aux(&[1u32]);
    test_optional_index_rank_aux(&[0u32, 1u32]);
    let mut block = Vec::new();
    block.push(3u32);
    block.extend((0..ELEMENTS_PER_BLOCK).map(|i| i + ELEMENTS_PER_BLOCK + 1));
    test_optional_index_rank_aux(&block);
}

#[test]
fn test_optional_index_iter_empty_one() {
    test_optional_index_iter_aux(&[1], 2u32);
    test_optional_index_iter_aux(&[100_000], 200_000u32);
}

#[test]
fn test_optional_index_iter_dense_block() {
    let mut block = Vec::new();
    block.push(3u32);
    block.extend((0..ELEMENTS_PER_BLOCK).map(|i| i + ELEMENTS_PER_BLOCK + 1));
    test_optional_index_iter_aux(&block, 3 * ELEMENTS_PER_BLOCK);
}

#[test]
fn test_optional_index_for_tests() {
    let optional_index = OptionalIndex::for_test(4, &[1, 2]);
    assert!(!optional_index.contains(0));
    assert!(optional_index.contains(1));
    assert!(optional_index.contains(2));
    assert!(!optional_index.contains(3));
    assert_eq!(optional_index.num_docs(), 4);
}
