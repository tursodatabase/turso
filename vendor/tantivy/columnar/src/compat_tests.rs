use std::path::PathBuf;

use itertools::Itertools;

use crate::{
    CURRENT_VERSION, Cardinality, Column, ColumnarReader, DynamicColumn, StackMergeOrder,
    merge_columnar,
};

const NUM_DOCS: u32 = u16::MAX as u32;

fn generate_columnar(num_docs: u32, value_offset: u64) -> Vec<u8> {
    use crate::ColumnarWriter;

    let mut columnar_writer = ColumnarWriter::default();

    for i in 0..num_docs {
        if i % 100 == 0 {
            columnar_writer.record_numerical(i, "sparse", value_offset + i as u64);
        }
        if i % 5 == 0 {
            columnar_writer.record_numerical(i, "dense", value_offset + i as u64);
        }
        columnar_writer.record_numerical(i, "full", value_offset + i as u64);
        columnar_writer.record_numerical(i, "multi", value_offset + i as u64);
        columnar_writer.record_numerical(i, "multi", value_offset + i as u64);
    }

    let mut wrt: Vec<u8> = Vec::new();
    columnar_writer.serialize(num_docs, &mut wrt).unwrap();

    wrt
}

#[test]
/// Writes a columnar for the CURRENT_VERSION to disk.
fn create_format() {
    let version = CURRENT_VERSION.to_string();
    let file_path = path_for_version(&version);
    if PathBuf::from(file_path.clone()).exists() {
        return;
    }
    let columnar = generate_columnar(NUM_DOCS, 0);
    std::fs::write(file_path, columnar).unwrap();
}

fn path_for_version(version: &str) -> String {
    format!("./compat_tests_data/{}.columnar", version)
}

#[test]
fn test_format_v1() {
    let path = path_for_version("v1");
    test_format(&path);
}

#[test]
fn test_format_v2() {
    let path = path_for_version("v2");
    test_format(&path);
}

fn test_format(path: &str) {
    let file_content = std::fs::read(path).unwrap();
    let reader = ColumnarReader::open(file_content).unwrap();

    check_columns(&reader);

    // Test merge
    let reader2 = ColumnarReader::open(generate_columnar(NUM_DOCS, NUM_DOCS as u64)).unwrap();
    let columnar_readers = vec![&reader, &reader2];
    let merge_row_order = StackMergeOrder::stack(&columnar_readers[..]);
    let mut out = Vec::new();
    merge_columnar(&columnar_readers, &[], merge_row_order.into(), &mut out).unwrap();
    let reader = ColumnarReader::open(out).unwrap();
    check_columns(&reader);
}

fn check_columns(reader: &ColumnarReader) {
    let column = open_column(reader, "full");
    check_column(&column, |doc_id| vec![(doc_id, doc_id as u64).into()]);
    assert_eq!(column.get_cardinality(), Cardinality::Full);

    let column = open_column(reader, "multi");
    check_column(&column, |doc_id| {
        vec![
            (doc_id * 2, doc_id as u64).into(),
            (doc_id * 2 + 1, doc_id as u64).into(),
        ]
    });
    assert_eq!(column.get_cardinality(), Cardinality::Multivalued);

    let column = open_column(reader, "sparse");
    check_column(&column, |doc_id| {
        if doc_id % 100 == 0 {
            vec![(doc_id / 100, doc_id as u64).into()]
        } else {
            vec![]
        }
    });
    assert_eq!(column.get_cardinality(), Cardinality::Optional);

    let column = open_column(reader, "dense");
    check_column(&column, |doc_id| {
        if doc_id % 5 == 0 {
            vec![(doc_id / 5, doc_id as u64).into()]
        } else {
            vec![]
        }
    });
    assert_eq!(column.get_cardinality(), Cardinality::Optional);
}

struct RowIdAndValue {
    row_id: u32,
    value: u64,
}
impl From<(u32, u64)> for RowIdAndValue {
    fn from((row_id, value): (u32, u64)) -> Self {
        Self { row_id, value }
    }
}

fn check_column<F: Fn(u32) -> Vec<RowIdAndValue>>(column: &Column<u64>, expected: F) {
    let num_docs = column.num_docs();
    let test_doc = |doc: u32| {
        if expected(doc).is_empty() {
            assert_eq!(column.first(doc), None);
        } else {
            assert_eq!(column.first(doc), Some(expected(doc)[0].value));
        }
        let values = column.values_for_doc(doc).collect_vec();
        assert_eq!(values, expected(doc).iter().map(|x| x.value).collect_vec());
        let mut row_ids = Vec::new();
        column.row_ids_for_docs(&[doc], &mut vec![], &mut row_ids);
        assert_eq!(
            row_ids,
            expected(doc).iter().map(|x| x.row_id).collect_vec()
        );
        let values = column.values_for_doc(doc).collect_vec();
        assert_eq!(values, expected(doc).iter().map(|x| x.value).collect_vec());

        // Docid rowid conversion
        let mut row_ids = Vec::new();
        let safe_next_doc = |doc: u32| (doc + 1).min(num_docs - 1);
        column
            .index
            .docids_to_rowids(&[doc, safe_next_doc(doc)], &mut vec![], &mut row_ids);
        let expected_rowids = expected(doc)
            .iter()
            .map(|x| x.row_id)
            .chain(expected(safe_next_doc(doc)).iter().map(|x| x.row_id))
            .collect_vec();
        assert_eq!(row_ids, expected_rowids);
        let rowid_range = column
            .index
            .docid_range_to_rowids(doc..safe_next_doc(doc) + 1);
        if expected_rowids.is_empty() {
            assert!(rowid_range.is_empty());
        } else {
            assert_eq!(
                rowid_range,
                expected_rowids[0]..expected_rowids.last().unwrap() + 1
            );
        }
    };
    test_doc(0);
    test_doc(num_docs - 1);
    test_doc(num_docs - 2);
    test_doc(65000);
}

fn open_column(reader: &ColumnarReader, name: &str) -> Column<u64> {
    let column = reader.read_columns(name).unwrap()[0]
        .open()
        .unwrap()
        .coerce_numerical(crate::NumericalType::U64)
        .unwrap();
    let DynamicColumn::U64(column) = column else {
        panic!();
    };
    column
}
