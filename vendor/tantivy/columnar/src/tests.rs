use std::collections::HashMap;
use std::fmt::Debug;
use std::net::Ipv6Addr;

use common::DateTime;
use proptest::prelude::*;
use proptest::sample::subsequence;

use crate::column_values::MonotonicallyMappableToU128;
use crate::columnar::{ColumnType, ColumnTypeCategory};
use crate::dynamic_column::{DynamicColumn, DynamicColumnHandle};
use crate::value::{Coerce, NumericalValue};
use crate::{
    BytesColumn, Cardinality, Column, ColumnarReader, ColumnarWriter, RowAddr, RowId,
    ShuffleMergeOrder, StackMergeOrder,
};

#[test]
fn test_dataframe_writer_str() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_str(1u32, "my_string", "hello");
    dataframe_writer.record_str(3u32, "my_string", "helloeee");
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("my_string").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 73);
}

#[test]
fn test_dataframe_writer_bytes() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_bytes(1u32, "my_string", b"hello");
    dataframe_writer.record_bytes(3u32, "my_string", b"helloeee");
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("my_string").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 73);
}

#[test]
fn test_dataframe_writer_bool() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_bool(1u32, "bool.value", false);
    dataframe_writer.record_bool(3u32, "bool.value", true);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("bool.value").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 22);
    assert_eq!(cols[0].column_type(), ColumnType::Bool);
    let dyn_bool_col = cols[0].open().unwrap();
    let DynamicColumn::Bool(bool_col) = dyn_bool_col else {
        panic!();
    };
    let vals: Vec<Option<bool>> = (0..5).map(|doc_id| bool_col.first(doc_id)).collect();
    assert_eq!(&vals, &[None, Some(false), None, Some(true), None,]);
}

#[test]
fn test_dataframe_writer_u64_multivalued() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_numerical(2u32, "divisor", 2u64);
    dataframe_writer.record_numerical(3u32, "divisor", 3u64);
    dataframe_writer.record_numerical(4u32, "divisor", 2u64);
    dataframe_writer.record_numerical(5u32, "divisor", 5u64);
    dataframe_writer.record_numerical(6u32, "divisor", 2u64);
    dataframe_writer.record_numerical(6u32, "divisor", 3u64);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(7, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("divisor").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 50);
    let dyn_i64_col = cols[0].open().unwrap();
    let DynamicColumn::I64(divisor_col) = dyn_i64_col else {
        panic!();
    };
    assert_eq!(
        divisor_col.get_cardinality(),
        crate::Cardinality::Multivalued
    );
    assert_eq!(divisor_col.num_docs(), 7);
}

#[test]
fn test_dataframe_writer_ip_addr() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_ip_addr(1, "ip_addr", Ipv6Addr::from_u128(1001));
    dataframe_writer.record_ip_addr(3, "ip_addr", Ipv6Addr::from_u128(1050));
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("ip_addr").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 42);
    assert_eq!(cols[0].column_type(), ColumnType::IpAddr);
    let dyn_bool_col = cols[0].open().unwrap();
    let DynamicColumn::IpAddr(ip_col) = dyn_bool_col else {
        panic!();
    };
    let vals: Vec<Option<Ipv6Addr>> = (0..5).map(|doc_id| ip_col.first(doc_id)).collect();
    assert_eq!(
        &vals,
        &[
            None,
            Some(Ipv6Addr::from_u128(1001)),
            None,
            Some(Ipv6Addr::from_u128(1050)),
            None,
        ]
    );
}

#[test]
fn test_dataframe_writer_numerical() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_numerical(1u32, "srical.value", NumericalValue::U64(12u64));
    dataframe_writer.record_numerical(2u32, "srical.value", NumericalValue::U64(13u64));
    dataframe_writer.record_numerical(4u32, "srical.value", NumericalValue::U64(15u64));
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(6, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("srical.value").unwrap();
    assert_eq!(cols.len(), 1);
    // Right now this 31 bytes are spent as follows
    //
    // - header 14 bytes
    // - vals  8 //< due to padding? could have been 1byte?.
    // - null footer 6 bytes
    assert_eq!(cols[0].num_bytes(), 33);
    let column = cols[0].open().unwrap();
    let DynamicColumn::I64(column_i64) = column else {
        panic!();
    };
    assert_eq!(column_i64.index.get_cardinality(), Cardinality::Optional);
    assert_eq!(column_i64.first(0), None);
    assert_eq!(column_i64.first(1), Some(12i64));
    assert_eq!(column_i64.first(2), Some(13i64));
    assert_eq!(column_i64.first(3), None);
    assert_eq!(column_i64.first(4), Some(15i64));
    assert_eq!(column_i64.first(5), None);
    assert_eq!(column_i64.first(6), None); //< we can change the spec for that one.
}

#[test]
fn test_dictionary_encoded_str() {
    let mut buffer = Vec::new();
    let mut columnar_writer = ColumnarWriter::default();
    columnar_writer.record_str(1, "my.column", "a");
    columnar_writer.record_str(3, "my.column", "c");
    columnar_writer.record_str(3, "my.column2", "different_column!");
    columnar_writer.record_str(4, "my.column", "b");
    columnar_writer.serialize(5, &mut buffer).unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_columns(), 2);
    let col_handles = columnar_reader.read_columns("my.column").unwrap();
    assert_eq!(col_handles.len(), 1);
    let DynamicColumn::Str(str_col) = col_handles[0].open().unwrap() else {
        panic!();
    };
    let index: Vec<Option<u64>> = (0..5).map(|doc_id| str_col.ords().first(doc_id)).collect();
    assert_eq!(index, &[None, Some(0), None, Some(2), Some(1)]);
    assert_eq!(str_col.num_rows(), 5);
    let mut term_buffer = String::new();
    let term_ords = str_col.ords();
    assert_eq!(term_ords.first(0), None);
    assert_eq!(term_ords.first(1), Some(0));
    str_col.ord_to_str(0u64, &mut term_buffer).unwrap();
    assert_eq!(term_buffer, "a");
    assert_eq!(term_ords.first(2), None);
    assert_eq!(term_ords.first(3), Some(2));
    str_col.ord_to_str(2u64, &mut term_buffer).unwrap();
    assert_eq!(term_buffer, "c");
    assert_eq!(term_ords.first(4), Some(1));
    str_col.ord_to_str(1u64, &mut term_buffer).unwrap();
    assert_eq!(term_buffer, "b");
}

#[test]
fn test_dictionary_encoded_bytes() {
    let mut buffer = Vec::new();
    let mut columnar_writer = ColumnarWriter::default();
    columnar_writer.record_bytes(1, "my.column", b"a");
    columnar_writer.record_bytes(3, "my.column", b"c");
    columnar_writer.record_bytes(3, "my.column2", b"different_column!");
    columnar_writer.record_bytes(4, "my.column", b"b");
    columnar_writer.serialize(5, &mut buffer).unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_columns(), 2);
    let col_handles = columnar_reader.read_columns("my.column").unwrap();
    assert_eq!(col_handles.len(), 1);
    let DynamicColumn::Bytes(bytes_col) = col_handles[0].open().unwrap() else {
        panic!();
    };
    let index: Vec<Option<u64>> = (0..5)
        .map(|doc_id| bytes_col.ords().first(doc_id))
        .collect();
    assert_eq!(index, &[None, Some(0), None, Some(2), Some(1)]);
    assert_eq!(bytes_col.num_rows(), 5);
    let mut term_buffer = Vec::new();
    let term_ords = bytes_col.ords();
    assert_eq!(term_ords.first(0), None);
    assert_eq!(term_ords.first(1), Some(0));
    bytes_col
        .dictionary
        .ord_to_term(0u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"a");
    assert_eq!(term_ords.first(2), None);
    assert_eq!(term_ords.first(3), Some(2));
    bytes_col
        .dictionary
        .ord_to_term(2u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"c");
    assert_eq!(term_ords.first(4), Some(1));
    bytes_col
        .dictionary
        .ord_to_term(1u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"b");
}

fn num_strategy() -> impl Strategy<Value = NumericalValue> {
    prop_oneof![
        3 => Just(NumericalValue::U64(0u64)),
        3 => Just(NumericalValue::U64(u64::MAX)),
        3 => Just(NumericalValue::I64(0i64)),
        3 => Just(NumericalValue::I64(i64::MIN)),
        3 => Just(NumericalValue::I64(i64::MAX)),
        3 => Just(NumericalValue::F64(1.2f64)),
        1 => any::<f64>().prop_map(NumericalValue::from),
        1 => any::<u64>().prop_map(NumericalValue::from),
        1 => any::<i64>().prop_map(NumericalValue::from),
    ]
}

#[derive(Debug, Clone, Copy)]
enum ColumnValue {
    Str(&'static str),
    Bytes(&'static [u8]),
    Numerical(NumericalValue),
    IpAddr(Ipv6Addr),
    Bool(bool),
    DateTime(DateTime),
}

impl<T: Into<NumericalValue>> From<T> for ColumnValue {
    fn from(val: T) -> ColumnValue {
        ColumnValue::Numerical(val.into())
    }
}

impl ColumnValue {
    pub(crate) fn column_type_category(&self) -> ColumnTypeCategory {
        match self {
            ColumnValue::Str(_) => ColumnTypeCategory::Str,
            ColumnValue::Bytes(_) => ColumnTypeCategory::Bytes,
            ColumnValue::Numerical(_) => ColumnTypeCategory::Numerical,
            ColumnValue::IpAddr(_) => ColumnTypeCategory::IpAddr,
            ColumnValue::Bool(_) => ColumnTypeCategory::Bool,
            ColumnValue::DateTime(_) => ColumnTypeCategory::DateTime,
        }
    }
}

fn column_name_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("c1"), Just("c2")]
}

fn string_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("a"), Just("b")]
}

fn bytes_strategy() -> impl Strategy<Value = &'static [u8]> {
    prop_oneof![Just(&[0u8][..]), Just(&[1u8][..])]
}

// A random column value
fn column_value_strategy() -> impl Strategy<Value = ColumnValue> {
    prop_oneof![
        10 => string_strategy().prop_map(ColumnValue::Str),
        1 => bytes_strategy().prop_map(ColumnValue::Bytes),
        40 => num_strategy().prop_map(ColumnValue::Numerical),
        1 => (1u16..3u16).prop_map(|ip_addr_byte| ColumnValue::IpAddr(Ipv6Addr::new(
            127,
            0,
            0,
            0,
            0,
            0,
            0,
            ip_addr_byte
        ))),
        1 => any::<bool>().prop_map(ColumnValue::Bool),
        1 => (679_723_993i64..1_679_723_995i64)
            .prop_map(|val| { ColumnValue::DateTime(DateTime::from_timestamp_secs(val)) })
    ]
}

// A document contains up to 4 values.
fn doc_strategy() -> impl Strategy<Value = Vec<(&'static str, ColumnValue)>> {
    proptest::collection::vec((column_name_strategy(), column_value_strategy()), 0..=4)
}

fn num_docs_strategy() -> impl Strategy<Value = usize> {
    prop_oneof!(
        // We focus heavily on the 0..2 case as we assume it is sufficient to cover all edge cases.
        0usize..=3usize,
        // We leave 50% of the effort exploring more defensively.
        3usize..=12usize
    )
}

// A columnar contains up to 2 docs.
fn columnar_docs_strategy() -> impl Strategy<Value = Vec<Vec<(&'static str, ColumnValue)>>> {
    num_docs_strategy()
        .prop_flat_map(|num_docs| proptest::collection::vec(doc_strategy(), num_docs))
}

fn permutation_and_subset_strategy(n: usize) -> impl Strategy<Value = Vec<usize>> {
    let vals: Vec<usize> = (0..n).collect();
    subsequence(vals, 0..=n).prop_shuffle()
}

fn build_columnar_with_mapping(docs: &[Vec<(&'static str, ColumnValue)>]) -> ColumnarReader {
    let num_docs = docs.len() as u32;
    let mut buffer = Vec::new();
    let mut columnar_writer = ColumnarWriter::default();
    for (doc_id, vals) in docs.iter().enumerate() {
        for (column_name, col_val) in vals {
            match *col_val {
                ColumnValue::Str(str_val) => {
                    columnar_writer.record_str(doc_id as u32, column_name, str_val);
                }
                ColumnValue::Bytes(bytes) => {
                    columnar_writer.record_bytes(doc_id as u32, column_name, bytes)
                }
                ColumnValue::Numerical(num) => {
                    columnar_writer.record_numerical(doc_id as u32, column_name, num);
                }
                ColumnValue::IpAddr(ip_addr) => {
                    columnar_writer.record_ip_addr(doc_id as u32, column_name, ip_addr);
                }
                ColumnValue::Bool(bool_val) => {
                    columnar_writer.record_bool(doc_id as u32, column_name, bool_val);
                }
                ColumnValue::DateTime(date_time) => {
                    columnar_writer.record_datetime(doc_id as u32, column_name, date_time);
                }
            }
        }
    }
    columnar_writer.serialize(num_docs, &mut buffer).unwrap();

    ColumnarReader::open(buffer).unwrap()
}

fn build_columnar(docs: &[Vec<(&'static str, ColumnValue)>]) -> ColumnarReader {
    build_columnar_with_mapping(docs)
}

fn assert_columnar_eq_strict(left: &ColumnarReader, right: &ColumnarReader) {
    assert_columnar_eq(left, right, false);
}

fn assert_columnar_eq(
    left: &ColumnarReader,
    right: &ColumnarReader,
    lenient_on_numerical_value: bool,
) {
    assert_eq!(left.num_docs(), right.num_docs());
    let left_columns = left.list_columns().unwrap();
    let right_columns = right.list_columns().unwrap();
    assert_eq!(left_columns.len(), right_columns.len());
    for i in 0..left_columns.len() {
        assert_eq!(left_columns[i].0, right_columns[i].0);
        let left_column = left_columns[i].1.open().unwrap();
        let right_column = right_columns[i].1.open().unwrap();
        assert_dyn_column_eq(&left_column, &right_column, lenient_on_numerical_value);
    }
}

#[track_caller]
fn assert_column_eq<T: Copy + PartialOrd + Debug + Send + Sync + 'static>(
    left: &Column<T>,
    right: &Column<T>,
) {
    assert_eq!(left.get_cardinality(), right.get_cardinality());
    assert_eq!(left.num_docs(), right.num_docs());
    let num_docs = left.num_docs();
    for doc in 0..num_docs {
        assert_eq!(
            left.index.value_row_ids(doc),
            right.index.value_row_ids(doc)
        );
    }
    assert_eq!(left.values.num_vals(), right.values.num_vals());
    let num_vals = left.values.num_vals();
    for i in 0..num_vals {
        assert_eq!(left.values.get_val(i), right.values.get_val(i));
    }
}

fn assert_bytes_column_eq(left: &BytesColumn, right: &BytesColumn) {
    assert_eq!(
        left.term_ord_column.get_cardinality(),
        right.term_ord_column.get_cardinality()
    );
    assert_eq!(left.num_rows(), right.num_rows());
    assert_column_eq(&left.term_ord_column, &right.term_ord_column);
    assert_eq!(left.dictionary.num_terms(), right.dictionary.num_terms());
    let num_terms = left.dictionary.num_terms();
    let mut left_terms = left.dictionary.stream().unwrap();
    let mut right_terms = right.dictionary.stream().unwrap();
    for _ in 0..num_terms {
        assert!(left_terms.advance());
        assert!(right_terms.advance());
        assert_eq!(left_terms.key(), right_terms.key());
    }
    assert!(!left_terms.advance());
    assert!(!right_terms.advance());
}

fn assert_dyn_column_eq(
    left_dyn_column: &DynamicColumn,
    right_dyn_column: &DynamicColumn,
    lenient_on_numerical_value: bool,
) {
    assert_eq!(
        &left_dyn_column.get_cardinality(),
        &right_dyn_column.get_cardinality()
    );
    match &(left_dyn_column, right_dyn_column) {
        (DynamicColumn::Bool(left_col), DynamicColumn::Bool(right_col)) => {
            assert_column_eq(left_col, right_col);
        }
        (DynamicColumn::I64(left_col), DynamicColumn::I64(right_col)) => {
            assert_column_eq(left_col, right_col);
        }
        (DynamicColumn::U64(left_col), DynamicColumn::U64(right_col)) => {
            assert_column_eq(left_col, right_col);
        }
        (DynamicColumn::F64(left_col), DynamicColumn::F64(right_col)) => {
            assert_column_eq(left_col, right_col);
        }
        (DynamicColumn::DateTime(left_col), DynamicColumn::DateTime(right_col)) => {
            assert_column_eq(left_col, right_col);
        }
        (DynamicColumn::IpAddr(left_col), DynamicColumn::IpAddr(right_col)) => {
            assert_column_eq(left_col, right_col);
        }
        (DynamicColumn::Bytes(left_col), DynamicColumn::Bytes(right_col)) => {
            assert_bytes_column_eq(left_col, right_col);
        }
        (DynamicColumn::Str(left_col), DynamicColumn::Str(right_col)) => {
            assert_bytes_column_eq(left_col, right_col);
        }
        (left, right) => {
            if lenient_on_numerical_value {
                assert_eq!(
                    ColumnTypeCategory::from(left.column_type()),
                    ColumnTypeCategory::from(right.column_type())
                );
            } else {
                panic!(
                    "Column type are not the same: {:?} vs {:?}",
                    left.column_type(),
                    right.column_type()
                );
            }
        }
    }
}

trait AssertEqualToColumnValue {
    fn assert_equal_to_column_value(&self, column_value: &ColumnValue);
}

impl AssertEqualToColumnValue for bool {
    fn assert_equal_to_column_value(&self, column_value: &ColumnValue) {
        let ColumnValue::Bool(val) = column_value else {
            panic!()
        };
        assert_eq!(self, val);
    }
}

impl AssertEqualToColumnValue for Ipv6Addr {
    fn assert_equal_to_column_value(&self, column_value: &ColumnValue) {
        let ColumnValue::IpAddr(val) = column_value else {
            panic!()
        };
        assert_eq!(self, val);
    }
}

impl<T: Coerce + PartialEq + Debug + Into<NumericalValue>> AssertEqualToColumnValue for T {
    fn assert_equal_to_column_value(&self, column_value: &ColumnValue) {
        let ColumnValue::Numerical(num) = column_value else {
            panic!()
        };
        assert_eq!(self, &T::coerce(*num));
    }
}

impl AssertEqualToColumnValue for DateTime {
    fn assert_equal_to_column_value(&self, column_value: &ColumnValue) {
        let ColumnValue::DateTime(dt) = column_value else {
            panic!()
        };
        assert_eq!(self, dt);
    }
}

fn assert_column_values<
    T: AssertEqualToColumnValue + PartialEq + Copy + PartialOrd + Debug + Send + Sync + 'static,
>(
    col: &Column<T>,
    expected: &HashMap<u32, Vec<&ColumnValue>>,
) {
    let mut num_non_empty_rows = 0;
    for doc in 0..col.num_docs() {
        let doc_vals: Vec<T> = col.values_for_doc(doc).collect();
        if doc_vals.is_empty() {
            continue;
        }
        num_non_empty_rows += 1;
        let expected_vals = expected.get(&doc).unwrap();
        assert_eq!(doc_vals.len(), expected_vals.len());
        for (val, &expected) in doc_vals.iter().zip(expected_vals.iter()) {
            val.assert_equal_to_column_value(expected)
        }
    }
    assert_eq!(num_non_empty_rows, expected.len());
}

fn assert_bytes_column_values(
    col: &BytesColumn,
    expected: &HashMap<u32, Vec<&ColumnValue>>,
    is_str: bool,
) {
    let mut num_non_empty_rows = 0;
    let mut buffer = Vec::new();
    for doc in 0..col.term_ord_column.num_docs() {
        let doc_vals: Vec<u64> = col.term_ords(doc).collect();
        if doc_vals.is_empty() {
            continue;
        }
        let expected_vals = expected.get(&doc).unwrap();
        assert_eq!(doc_vals.len(), expected_vals.len());
        for (&expected_col_val, &ord) in expected_vals.iter().zip(&doc_vals) {
            col.ord_to_bytes(ord, &mut buffer).unwrap();
            match expected_col_val {
                ColumnValue::Str(str_val) => {
                    assert!(is_str);
                    assert_eq!(str_val.as_bytes(), &buffer);
                }
                ColumnValue::Bytes(bytes_val) => {
                    assert!(!is_str);
                    assert_eq!(bytes_val, &buffer);
                }
                _ => {
                    panic!();
                }
            }
        }
        num_non_empty_rows += 1;
    }
    assert_eq!(num_non_empty_rows, expected.len());
}

// This proptest attempts to create a tiny columnar based of up to 3 rows, and checks that the
// resulting columnar matches the row data.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]
    #[test]
    fn test_single_columnar_builder_proptest(docs in columnar_docs_strategy()) {
        let columnar = build_columnar(&docs[..]);
        assert_eq!(columnar.num_docs() as usize, docs.len());
        let mut expected_columns: HashMap<(&str, ColumnTypeCategory), HashMap<u32, Vec<&ColumnValue>> > = Default::default();
        for (doc_id, doc_vals) in docs.iter().enumerate() {
            for (col_name, col_val) in doc_vals {
                expected_columns
                    .entry((col_name, col_val.column_type_category()))
                    .or_default()
                    .entry(doc_id as u32)
                    .or_default()
                    .push(col_val);
            }
        }
        let column_list = columnar.list_columns().unwrap();
        assert_eq!(expected_columns.len(), column_list.len());
        for (column_name, column) in column_list {
            let dynamic_column = column.open().unwrap();
            let col_category: ColumnTypeCategory = dynamic_column.column_type().into();
            let expected_col_values: &HashMap<u32, Vec<&ColumnValue>> = expected_columns.get(&(column_name.as_str(), col_category)).unwrap();
            match &dynamic_column {
                DynamicColumn::Bool(col) =>
                    assert_column_values(col, expected_col_values),
                DynamicColumn::I64(col) =>
                    assert_column_values(col, expected_col_values),
                DynamicColumn::U64(col) =>
                    assert_column_values(col, expected_col_values),
                DynamicColumn::F64(col) =>
                    assert_column_values(col, expected_col_values),
                DynamicColumn::IpAddr(col) =>
                    assert_column_values(col, expected_col_values),
                DynamicColumn::DateTime(col) =>
                    assert_column_values(col, expected_col_values),
                DynamicColumn::Bytes(col) =>
                    assert_bytes_column_values(col, expected_col_values, false),
                DynamicColumn::Str(col) =>
                    assert_bytes_column_values(col, expected_col_values, true),
            }
        }
    }
}

// This tests create 2 or 3 random small columnar and attempts to merge them.
// It compares the resulting merged dataframe with what would have been obtained by building the
// dataframe from the concatenated rows to begin with.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn test_columnar_merge_proptest(columnar_docs in proptest::collection::vec(columnar_docs_strategy(), 2..=3)) {
        let columnar_readers: Vec<ColumnarReader> = columnar_docs.iter()
            .map(|docs| build_columnar(&docs[..]))
            .collect::<Vec<_>>();
        let columnar_readers_arr: Vec<&ColumnarReader> = columnar_readers.iter().collect();
        let mut output: Vec<u8> = Vec::new();
        let stack_merge_order = StackMergeOrder::stack(&columnar_readers_arr[..]).into();
        crate::merge_columnar(&columnar_readers_arr[..], &[], stack_merge_order, &mut output).unwrap();
        let merged_columnar = ColumnarReader::open(output).unwrap();
        let concat_rows: Vec<Vec<(&'static str, ColumnValue)>> = columnar_docs.iter().flatten().cloned().collect();
        let expected_merged_columnar = build_columnar(&concat_rows[..]);
        assert_columnar_eq_strict(&merged_columnar, &expected_merged_columnar);
    }
}

#[test]
fn test_columnar_merging_empty_columnar() {
    let columnar_docs: Vec<Vec<Vec<(&str, ColumnValue)>>> =
        vec![vec![], vec![vec![("c1", ColumnValue::Str("a"))]]];
    let columnar_readers: Vec<ColumnarReader> = columnar_docs
        .iter()
        .map(|docs| build_columnar(&docs[..]))
        .collect::<Vec<_>>();
    let columnar_readers_arr: Vec<&ColumnarReader> = columnar_readers.iter().collect();
    let mut output: Vec<u8> = Vec::new();
    let stack_merge_order = StackMergeOrder::stack(&columnar_readers_arr[..]);
    crate::merge_columnar(
        &columnar_readers_arr[..],
        &[],
        crate::MergeRowOrder::Stack(stack_merge_order),
        &mut output,
    )
    .unwrap();
    let merged_columnar = ColumnarReader::open(output).unwrap();
    let concat_rows: Vec<Vec<(&'static str, ColumnValue)>> =
        columnar_docs.iter().flatten().cloned().collect();
    let expected_merged_columnar = build_columnar(&concat_rows[..]);
    assert_columnar_eq_strict(&merged_columnar, &expected_merged_columnar);
}

#[test]
fn test_columnar_merging_number_columns() {
    let columnar_docs: Vec<Vec<Vec<(&str, ColumnValue)>>> = vec![
        // columnar 1
        vec![
            // doc 1.1
            vec![("c2", ColumnValue::Numerical(0i64.into()))],
        ],
        // columnar2
        vec![
            // doc 2.1
            vec![("c2", ColumnValue::Numerical(0u64.into()))],
            // doc 2.2
            vec![("c2", ColumnValue::Numerical(u64::MAX.into()))],
        ],
    ];
    let columnar_readers: Vec<ColumnarReader> = columnar_docs
        .iter()
        .map(|docs| build_columnar(&docs[..]))
        .collect::<Vec<_>>();
    let columnar_readers_arr: Vec<&ColumnarReader> = columnar_readers.iter().collect();
    let mut output: Vec<u8> = Vec::new();
    let stack_merge_order = StackMergeOrder::stack(&columnar_readers_arr[..]);
    crate::merge_columnar(
        &columnar_readers_arr[..],
        &[],
        crate::MergeRowOrder::Stack(stack_merge_order),
        &mut output,
    )
    .unwrap();
    let merged_columnar = ColumnarReader::open(output).unwrap();
    let concat_rows: Vec<Vec<(&'static str, ColumnValue)>> =
        columnar_docs.iter().flatten().cloned().collect();
    let expected_merged_columnar = build_columnar(&concat_rows[..]);
    assert_columnar_eq_strict(&merged_columnar, &expected_merged_columnar);
}

// TODO add non trivial remap and merge
// TODO test required_columns
// TODO document edge case: required_columns incompatible with values.

#[allow(clippy::type_complexity)]
fn columnar_docs_and_remap()
-> impl Strategy<Value = (Vec<Vec<Vec<(&'static str, ColumnValue)>>>, Vec<RowAddr>)> {
    proptest::collection::vec(columnar_docs_strategy(), 2..=3).prop_flat_map(
        |columnars_docs: Vec<Vec<Vec<(&str, ColumnValue)>>>| {
            let row_addrs: Vec<RowAddr> = columnars_docs
                .iter()
                .enumerate()
                .flat_map(|(segment_ord, columnar_docs)| {
                    (0u32..columnar_docs.len() as u32).map(move |row_id| RowAddr {
                        segment_ord: segment_ord as u32,
                        row_id,
                    })
                })
                .collect();
            permutation_and_subset_strategy(row_addrs.len()).prop_map(move |shuffled_subset| {
                let shuffled_row_addr_subset: Vec<RowAddr> =
                    shuffled_subset.iter().map(|ord| row_addrs[*ord]).collect();
                (columnars_docs.clone(), shuffled_row_addr_subset)
            })
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    #[test]
    fn test_columnar_merge_and_remap_proptest((columnar_docs, shuffle_merge_order) in
columnar_docs_and_remap()) {
        test_columnar_merge_and_remap(columnar_docs, shuffle_merge_order);
    }
}

fn test_columnar_merge_and_remap(
    columnar_docs: Vec<Vec<Vec<(&'static str, ColumnValue)>>>,
    shuffle_merge_order: Vec<RowAddr>,
) {
    let shuffled_rows: Vec<Vec<(&'static str, ColumnValue)>> = shuffle_merge_order
        .iter()
        .map(|row_addr| {
            columnar_docs[row_addr.segment_ord as usize][row_addr.row_id as usize].clone()
        })
        .collect();
    let expected_merged_columnar = build_columnar(&shuffled_rows[..]);
    let columnar_readers: Vec<ColumnarReader> = columnar_docs
        .iter()
        .map(|docs| build_columnar(&docs[..]))
        .collect::<Vec<_>>();
    let columnar_readers_ref: Vec<&ColumnarReader> = columnar_readers.iter().collect();
    let mut output: Vec<u8> = Vec::new();
    let segment_num_rows: Vec<RowId> = columnar_docs
        .iter()
        .map(|docs| docs.len() as RowId)
        .collect();
    let shuffle_merge_order = ShuffleMergeOrder::for_test(&segment_num_rows, shuffle_merge_order);
    crate::merge_columnar(
        &columnar_readers_ref[..],
        &[],
        shuffle_merge_order.into(),
        &mut output,
    )
    .unwrap();
    let merged_columnar = ColumnarReader::open(output).unwrap();
    assert_columnar_eq(&merged_columnar, &expected_merged_columnar, true);
}

#[test]
fn test_columnar_merge_and_remap_bug_1() {
    let columnar_docs = vec![vec![
        vec![
            ("c1", ColumnValue::Numerical(NumericalValue::U64(0))),
            ("c1", ColumnValue::Numerical(NumericalValue::U64(0))),
        ],
        vec![],
    ]];
    let shuffle_merge_order: Vec<RowAddr> = vec![
        RowAddr {
            segment_ord: 0,
            row_id: 1,
        },
        RowAddr {
            segment_ord: 0,
            row_id: 0,
        },
    ];

    test_columnar_merge_and_remap(columnar_docs, shuffle_merge_order);
}

#[test]
fn test_columnar_merge_empty() {
    let columnar_reader_1 = build_columnar(&[]);
    let rows: &[Vec<_>] = &[vec![("c1", ColumnValue::Str("a"))]][..];
    let columnar_reader_2 = build_columnar(rows);
    let mut output: Vec<u8> = Vec::new();
    let segment_num_rows: Vec<RowId> = vec![0, 0];
    let shuffle_merge_order = ShuffleMergeOrder::for_test(&segment_num_rows, vec![]);
    crate::merge_columnar(
        &[&columnar_reader_1, &columnar_reader_2],
        &[],
        shuffle_merge_order.into(),
        &mut output,
    )
    .unwrap();
    let merged_columnar = ColumnarReader::open(output).unwrap();
    assert_eq!(merged_columnar.num_docs(), 0);
    assert_eq!(merged_columnar.num_columns(), 0);
}

#[test]
fn test_columnar_merge_single_str_column() {
    let columnar_reader_1 = build_columnar(&[]);
    let rows: &[Vec<_>] = &[vec![("c1", ColumnValue::Str("a"))]][..];
    let columnar_reader_2 = build_columnar(rows);
    let mut output: Vec<u8> = Vec::new();
    let segment_num_rows: Vec<RowId> = vec![0, 1];
    let shuffle_merge_order = ShuffleMergeOrder::for_test(
        &segment_num_rows,
        vec![RowAddr {
            segment_ord: 1u32,
            row_id: 0u32,
        }],
    );
    crate::merge_columnar(
        &[&columnar_reader_1, &columnar_reader_2],
        &[],
        shuffle_merge_order.into(),
        &mut output,
    )
    .unwrap();
    let merged_columnar = ColumnarReader::open(output).unwrap();
    assert_eq!(merged_columnar.num_docs(), 1);
    assert_eq!(merged_columnar.num_columns(), 1);
}

#[test]
fn test_delete_decrease_cardinality() {
    let columnar_reader_1 = build_columnar(&[]);
    let rows: &[Vec<_>] = &[
        vec![
            ("c", ColumnValue::from(0i64)),
            ("c", ColumnValue::from(0i64)),
        ],
        vec![("c", ColumnValue::from(0i64))],
    ][..];
    // c is multivalued here
    let columnar_reader_2 = build_columnar(rows);
    let mut output: Vec<u8> = Vec::new();
    let shuffle_merge_order = ShuffleMergeOrder::for_test(
        &[0, 2],
        vec![RowAddr {
            segment_ord: 1u32,
            row_id: 1u32,
        }],
    );
    crate::merge_columnar(
        &[&columnar_reader_1, &columnar_reader_2],
        &[],
        shuffle_merge_order.into(),
        &mut output,
    )
    .unwrap();
    let merged_columnar = ColumnarReader::open(output).unwrap();
    assert_eq!(merged_columnar.num_docs(), 1);
    assert_eq!(merged_columnar.num_columns(), 1);
    let cols = merged_columnar.read_columns("c").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].column_type(), ColumnType::I64);
    assert_eq!(cols[0].open().unwrap().get_cardinality(), Cardinality::Full);
}
