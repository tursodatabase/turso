use std::collections::HashMap;

use crate::column_index::optional_index::set_block::dense::DENSE_BLOCK_NUM_BYTES;
use crate::column_index::optional_index::set_block::{DenseBlockCodec, SparseBlockCodec};
use crate::column_index::optional_index::{SelectCursor, Set, SetCodec};

fn test_set_helper<C: SetCodec<Item = u16>>(vals: &[u16]) -> usize {
    let mut buffer = Vec::new();
    C::serialize(vals.iter().copied(), &mut buffer).unwrap();
    let tested_set = C::open(buffer.as_slice());
    let hash_set: HashMap<C::Item, C::Item> = vals
        .iter()
        .copied()
        .enumerate()
        .map(|(ord, val)| (val, C::Item::try_from(ord).ok().unwrap()))
        .collect();
    for val in 0u16..=u16::MAX {
        assert_eq!(tested_set.contains(val), hash_set.contains_key(&val));
        assert_eq!(tested_set.rank_if_exists(val), hash_set.get(&val).copied());
        assert_eq!(
            tested_set.rank(val),
            vals.iter().cloned().take_while(|v| *v < val).count() as u16
        );
    }
    for (rank, val) in vals.iter().enumerate() {
        assert_eq!(tested_set.select(rank as u16), *val);
    }
    buffer.len()
}

#[test]
fn test_dense_block_set_u16_empty() {
    let buffer_len = test_set_helper::<DenseBlockCodec>(&[]);
    assert_eq!(buffer_len, DENSE_BLOCK_NUM_BYTES as usize);
}

#[test]
fn test_dense_block_set_u16_max() {
    let buffer_len = test_set_helper::<DenseBlockCodec>(&[u16::MAX]);
    assert_eq!(buffer_len, DENSE_BLOCK_NUM_BYTES as usize);
}

#[test]
fn test_sparse_block_set_u16_empty() {
    let buffer_len = test_set_helper::<SparseBlockCodec>(&[]);
    assert_eq!(buffer_len, 0);
}

#[test]
fn test_sparse_block_set_u16_max() {
    let buffer_len = test_set_helper::<SparseBlockCodec>(&[u16::MAX]);
    assert_eq!(buffer_len, 2);
}

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1))]
    #[test]
    fn test_prop_test_dense(els in proptest::collection::btree_set(0..=u16::MAX, 0..=u16::MAX as usize)) {
        let vals: Vec<u16> = els.into_iter().collect();
        let buffer_len = test_set_helper::<DenseBlockCodec>(&vals);
        assert_eq!(buffer_len, DENSE_BLOCK_NUM_BYTES as usize);
    }

    #[test]
    fn test_prop_test_sparse(els in proptest::collection::btree_set(0..=u16::MAX, 0..=u16::MAX as usize)) {
        let vals: Vec<u16> = els.into_iter().collect();
        let buffer_len = test_set_helper::<SparseBlockCodec>(&vals);
        assert_eq!(buffer_len, vals.len() * 2);
    }
}

#[test]
fn test_simple_translate_codec_codec_idx_to_original_idx_dense() {
    let mut buffer = Vec::new();
    DenseBlockCodec::serialize([1, 3, 17, 32, 30_000, 30_001].iter().copied(), &mut buffer)
        .unwrap();
    let tested_set = DenseBlockCodec::open(buffer.as_slice());
    assert!(tested_set.contains(1));
    let mut select_cursor = tested_set.select_cursor();
    assert_eq!(select_cursor.select(0), 1);
    assert_eq!(select_cursor.select(1), 3);
    assert_eq!(select_cursor.select(2), 17);
}

#[test]
fn test_simple_translate_codec_idx_to_original_idx_sparse() {
    let mut buffer = Vec::new();
    SparseBlockCodec::serialize([1, 3, 17].iter().copied(), &mut buffer).unwrap();
    let tested_set = SparseBlockCodec::open(buffer.as_slice());
    assert!(tested_set.contains(1));
    let mut select_cursor = tested_set.select_cursor();
    assert_eq!(SelectCursor::select(&mut select_cursor, 0), 1);
    assert_eq!(SelectCursor::select(&mut select_cursor, 1), 3);
    assert_eq!(SelectCursor::select(&mut select_cursor, 2), 17);
}

#[test]
fn test_simple_translate_codec_idx_to_original_idx_dense() {
    let mut buffer = Vec::new();
    DenseBlockCodec::serialize(0u16..150u16, &mut buffer).unwrap();
    let tested_set = DenseBlockCodec::open(buffer.as_slice());
    assert!(tested_set.contains(1));
    let mut select_cursor = tested_set.select_cursor();
    for i in 0..150 {
        assert_eq!(i, select_cursor.select(i));
    }
}

#[test]
fn test_simple_translate_idx_to_value_idx_dense() {
    let mut buffer = Vec::new();
    DenseBlockCodec::serialize([1, 10].iter().copied(), &mut buffer).unwrap();
    let tested_set = DenseBlockCodec::open(buffer.as_slice());
    assert!(tested_set.contains(1));
    assert!(!tested_set.contains(2));
    assert_eq!(tested_set.rank(0), 0);
    assert_eq!(tested_set.rank(1), 0);
    for rank in 2..10 {
        // ranks that don't exist select the next highest one
        assert_eq!(tested_set.rank_if_exists(rank), None);
        assert_eq!(tested_set.rank(rank), 1);
    }
    assert_eq!(tested_set.rank(10), 1);
}

#[test]
fn test_simple_translate_idx_to_value_idx_sparse() {
    let mut buffer = Vec::new();
    SparseBlockCodec::serialize([1, 10].iter().copied(), &mut buffer).unwrap();
    let tested_set = SparseBlockCodec::open(buffer.as_slice());
    assert!(tested_set.contains(1));
    assert!(!tested_set.contains(2));
    assert_eq!(tested_set.rank(0), 0);
    assert_eq!(tested_set.select(tested_set.rank(0)), 1);
    assert_eq!(tested_set.rank(1), 0);
    assert_eq!(tested_set.select(tested_set.rank(1)), 1);
    for rank in 2..10 {
        // ranks that don't exist select the next highest one
        assert_eq!(tested_set.rank_if_exists(rank), None);
        assert_eq!(tested_set.rank(rank), 1);
        assert_eq!(tested_set.select(tested_set.rank(rank)), 10);
    }
    assert_eq!(tested_set.rank(10), 1);
    assert_eq!(tested_set.select(tested_set.rank(10)), 10);
}
