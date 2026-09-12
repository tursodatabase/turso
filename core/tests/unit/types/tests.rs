use super::*;
use crate::alloc::vec;
use crate::translate::collate::CollationSeq;

#[test]
fn is_ascii_checks_every_byte_of_every_length() {
    for len in 0..40 {
        let mut ascii: Vec<u8> = vec![];
        ascii.extend((0..len).map(|i| b'a' + (i % 26) as u8));
        assert!(is_ascii(&ascii), "length {len}");
        assert_eq!(validate_utf8(&ascii), std::str::from_utf8(&ascii).ok());
        for position in 0..len {
            let mut bytes = ascii.clone();
            bytes[position] = 0xc3;
            assert!(!is_ascii(&bytes), "length {len}, byte {position}");
            assert_eq!(validate_utf8(&bytes), std::str::from_utf8(&bytes).ok());
        }
    }
    let text = "héllo wörld, ünïcödé";
    assert!(!is_ascii(text.as_bytes()));
    assert_eq!(validate_utf8(text.as_bytes()), Some(text));
}

fn assert_integer_conversions<T>(in_range: &[(i64, T)], out_of_range: &[i64])
where
    T: Copy + std::fmt::Debug + PartialEq + FromValue,
{
    for &(input, expected) in in_range {
        assert_eq!(T::from_sql(Value::from_i64(input)).unwrap(), expected);
    }
    for &input in out_of_range {
        assert!(
            matches!(
                T::from_sql(Value::from_i64(input)),
                Err(LimboError::IntegerOverflow)
            ),
            "{input} should overflow {}",
            std::any::type_name::<T>()
        );
    }
}

#[test]
fn from_value_checks_integer_ranges() {
    assert_integer_conversions::<i32>(
        &[
            (i32::MIN as i64, i32::MIN),
            (-1, -1),
            (0, 0),
            (1, 1),
            (i32::MAX as i64, i32::MAX),
        ],
        &[i32::MIN as i64 - 1, i32::MAX as i64 + 1],
    );
    assert_integer_conversions::<u32>(
        &[(0, 0), (1, 1), (u32::MAX as i64, u32::MAX)],
        &[-2, -1, u32::MAX as i64 + 1],
    );
    assert_integer_conversions::<i64>(
        &[
            (i64::MIN, i64::MIN),
            (-1, -1),
            (0, 0),
            (1, 1),
            (i64::MAX, i64::MAX),
        ],
        &[],
    );
    assert_integer_conversions::<u64>(&[(0, 0), (1, 1), (i64::MAX, i64::MAX as u64)], &[-2, -1]);
}

#[test]
fn from_value_converts_integers_to_f64() {
    for input in [i64::MIN, -1, 0, 1, (1_i64 << 53) + 1, i64::MAX] {
        assert_eq!(f64::from_sql(Value::from_i64(input)).unwrap(), input as f64);
    }
    assert_eq!(f64::from_sql(Value::from_f64(1.5)).unwrap(), 1.5);
}

#[cfg(nightly)]
#[test]
fn moving_blobs_through_value_preserves_allocation() {
    fn assert_move_preserves_allocation(blob: ValueBlob) {
        let pointer = blob.as_ptr();
        let capacity = blob.capacity();
        let len = blob.len();

        let value = Value::from_blob(blob);
        let Value::Blob(blob) = value else {
            unreachable!();
        };

        assert_eq!(blob.as_ptr(), pointer);
        assert_eq!(blob.capacity(), capacity);
        assert_eq!(blob.len(), len);
    }

    assert_move_preserves_allocation(vec![]);
    assert_move_preserves_allocation(vec![1, 2, 3, 4]);
}

#[cfg(feature = "serde")]
#[test]
fn value_blob_serde_preserves_sequence_format() {
    let value = Value::from_slice(&[1, 2, 3, 4]).expect(crate::alloc::ALLOC_ERR_MSG);
    let encoded = serde_json::to_string(&value).unwrap();
    assert_eq!(encoded, r#"{"Blob":[1,2,3,4]}"#);

    let decoded: Value = serde_json::from_str(&encoded).unwrap();
    let Value::Blob(blob) = decoded else {
        panic!("expected blob value");
    };
    assert_eq!(blob.as_slice(), &[1, 2, 3, 4]);
}

#[test]
fn test_value_iterator_simple() {
    let mut buf = std::vec::Vec::new();
    let record = Record::new(vec![Value::from_i64(42), Value::Text(Text::new("hello"))]);
    record.serialize(&mut buf);

    let iter = ValueIterator::new(&buf).unwrap();
    assert!(!iter.is_empty());
    assert_eq!(iter.clone().count(), 2);

    let mut iter = ValueIterator::new(&buf).unwrap();

    let val = iter.next().unwrap().unwrap();
    assert_eq!(val, ValueRef::from_i64(42));

    let val = iter.next().unwrap().unwrap();
    assert_eq!(
        val,
        ValueRef::Text(TextRef::new("hello", TextSubtype::Text))
    );

    assert!(iter.next().is_none());
}

#[test]
fn test_value_iterator_nulls() {
    let mut buf = std::vec::Vec::new();
    let record = Record::new(vec![Value::Null, Value::Null, Value::Null]);
    record.serialize(&mut buf);

    let iter = ValueIterator::new(&buf).unwrap();

    for val in iter {
        assert_eq!(val.unwrap(), ValueRef::Null);
    }
}

#[test]
fn test_value_iterator_mixed_types() {
    let mut buf = std::vec::Vec::new();
    let record = Record::new(vec![
        Value::Null,
        Value::from_i64(100),
        Value::from_f64(std::f64::consts::PI),
        Value::Text(Text::new("test")),
        Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG),
        Value::from_i64(0),
        Value::from_i64(1),
    ]);
    record.serialize(&mut buf);

    let iter = ValueIterator::new(&buf).unwrap();
    let values: Vec<_> = iter.try_collect::<Result<Vec<_>>>().unwrap().unwrap();

    assert_eq!(values[0], ValueRef::Null);
    assert_eq!(values[1], ValueRef::from_i64(100));
    assert_eq!(values[2], ValueRef::from_f64(std::f64::consts::PI));
    assert_eq!(
        values[3],
        ValueRef::Text(TextRef::new("test", TextSubtype::Text))
    );
    assert_eq!(values[4], ValueRef::Blob(&[1, 2, 3]));
    assert_eq!(values[5], ValueRef::from_i64(0));
    assert_eq!(values[6], ValueRef::from_i64(1));
}

#[test]
fn test_value_iterator_last_decodes_only_the_last_value() {
    let mut buf = std::vec::Vec::new();
    let record = Record::new(vec![
        Value::Null,
        Value::from_i64(100),
        Value::from_f64(std::f64::consts::PI),
        Value::Text(Text::new("test")),
        Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG),
        Value::from_i64(0),
        Value::from_i64(1),
        Value::from_i64(-7_000_000_000),
    ]);
    record.serialize(&mut buf);

    let iter = ValueIterator::new(&buf).unwrap();
    assert_eq!(
        iter.last().unwrap().unwrap(),
        ValueRef::from_i64(-7_000_000_000)
    );

    let mut buf = std::vec::Vec::new();
    let record = Record::new(vec![Value::Text(Text::new("only"))]);
    record.serialize(&mut buf);
    let iter = ValueIterator::new(&buf).unwrap();
    assert_eq!(
        iter.last().unwrap().unwrap(),
        ValueRef::Text(TextRef::new("only", TextSubtype::Text))
    );

    let mut buf = std::vec::Vec::new();
    let record = Record::new(vec![]);
    record.serialize(&mut buf);
    let iter = ValueIterator::new(&buf).unwrap();
    assert!(iter.last().is_none());
}

#[test]
fn test_value_iterator_large_record() {
    let mut buf = std::vec::Vec::new();
    let values: Vec<Value> = (0..20)
        .map(|i| Value::from_i64(i as i64))
        .try_collect()
        .unwrap();
    let record = Record::new(values);
    record.serialize(&mut buf);

    let iter = ValueIterator::new(&buf).unwrap();
    assert_eq!(iter.count(), 20);

    let iter = ValueIterator::new(&buf).unwrap();
    for (i, val) in iter.enumerate() {
        assert_eq!(val.unwrap(), ValueRef::from_i64(i as i64));
    }
}

#[test]
fn test_value_iterator_zero_allocation() {
    let mut buf = std::vec::Vec::new();
    let values: Vec<Value> = (0..5)
        .map(|i| Value::from_i64(i as i64))
        .try_collect()
        .unwrap();
    let record = Record::new(values);
    record.serialize(&mut buf);

    let mut iter = ValueIterator::new(&buf).unwrap();
    let _ = iter.next();
    let _ = iter.next();
}

pub fn compare_immutable_for_testing(
    l: &[ValueRef],
    r: &[ValueRef],
    index_key_info: &[KeyInfo],
    tie_breaker: std::cmp::Ordering,
) -> std::cmp::Ordering {
    let min_len = l.len().min(r.len());

    for i in 0..min_len {
        let column_order = index_key_info[i].sort_order;
        let collation = index_key_info[i].collation;

        let cmp = match (&l[i], &r[i]) {
            (ValueRef::Text(left), ValueRef::Text(right)) => collation.compare_strings(left, right),
            _ => l[i].partial_cmp(&r[i]).unwrap_or(std::cmp::Ordering::Equal),
        };

        if cmp != std::cmp::Ordering::Equal {
            return match column_order {
                SortOrder::Asc => cmp,
                SortOrder::Desc => cmp.reverse(),
            };
        }
    }

    tie_breaker
}

fn create_record(values: Vec<Value>) -> ImmutableRecord {
    let registers: Vec<Register> = values
        .into_iter()
        .map(Register::Value)
        .try_collect()
        .unwrap();
    ImmutableRecord::from_registers(&registers, registers.len()).unwrap()
}

#[test]
fn immutable_record_ref_borrows_bin_record_payload() {
    let expected_values = vec![Value::from_i64(42), Value::build_text("borrowed")];
    let record = create_record(expected_values.clone());
    let payload = record.get_payload();

    let borrowed = ImmutableRecordRef::from_bin_record(payload);

    assert_eq!(borrowed.get_payload().as_ptr(), payload.as_ptr());
    assert_eq!(borrowed.column_count(), 2);
    assert_eq!(borrowed.get_values_owned().unwrap(), expected_values);
}

fn create_index_info(
    num_cols: usize,
    sort_orders: Vec<SortOrder>,
    collations: Vec<CollationSeq>,
) -> IndexInfo {
    IndexInfo::new(
        sort_orders
            .into_iter()
            .zip(collations)
            .map(|(sort_order, collation)| KeyInfo {
                sort_order,
                collation,
                nulls_order: None,
            }),
        false,
        num_cols,
        false,
    )
    .unwrap()
}

fn assert_compare_matches_full_comparison(
    serialized_values: Vec<Value>,
    unpacked_values: Vec<ValueRef>,
    index_info: &IndexInfo,
    test_name: &str,
) {
    let serialized = create_record(serialized_values.clone());

    let serialized_ref_values: Vec<ValueRef> = serialized_values
        .iter()
        .map(Value::as_ref)
        .try_collect()
        .unwrap();

    let tie_breaker = std::cmp::Ordering::Equal;

    let gold_result = compare_immutable_for_testing(
        &serialized_ref_values,
        &unpacked_values,
        &index_info.key_info,
        tie_breaker,
    );

    let comparer = find_compare(unpacked_values.iter().peekable(), index_info);
    let optimized_result = comparer
        .compare_payload(
            serialized.get_payload(),
            &unpacked_values,
            index_info,
            tie_breaker,
        )
        .unwrap();

    assert_eq!(
        gold_result, optimized_result,
        "Test '{test_name}' failed: Full Comparison: {gold_result:?}, Optimized: {optimized_result:?}, Strategy: {comparer:?}"
    );

    let selected_result = compare_record(
        serialized.get_payload(),
        unpacked_values.iter(),
        index_info,
        tie_breaker,
    )
    .unwrap();
    assert_eq!(gold_result, selected_result, "Test '{test_name}' failed");

    let generic_result = compare_payload_generic(
        serialized.get_payload(),
        unpacked_values.iter(),
        index_info,
        0,
        tie_breaker,
    )
    .unwrap();
    assert_eq!(
        gold_result, generic_result,
        "Test '{test_name}' failed with generic: Full Comparison: {gold_result:?}, Generic: {generic_result:?}\n LHS: {serialized_values:?}\n RHS: {unpacked_values:?}"
    );
}

#[test]
fn test_calc_header_size() {
    // Test 1-byte header size (serial type sizes 0 to 126)
    const MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER: usize = 0;
    assert_eq!(
        Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER),
        MIN_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1
    );
    const BITS_7_MAX: usize = (1 << 7) - 1; // varints use 7 bits for the value and 1 continuation bit
    const MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER: usize = BITS_7_MAX - 1;
    assert_eq!(
        Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER),
        MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1
    );

    // Test 2-byte header size (serial type sizes 127 to 16381)
    const MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER: usize =
        MAX_SERIALTYPES_SIZE_FOR_1_BYTE_HEADER + 1;
    assert_eq!(
        Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER),
        MIN_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 2
    );
    const BITS_14_MAX: usize = (1 << 14) - 1;
    const MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER: usize = BITS_14_MAX - 2;
    assert_eq!(
        Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER),
        MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 2
    );

    // Test 3-byte header size (serial type sizes 16382 to 2097148)
    const MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER: usize =
        MAX_SERIALTYPES_SIZE_FOR_2_BYTE_HEADER + 1;
    assert_eq!(
        Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER),
        MIN_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 3
    );
    const BITS_21_MAX: usize = (1 << 21) - 1;
    const MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER: usize = BITS_21_MAX - 3;
    assert_eq!(
        Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER),
        MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 3
    );

    // Test 4-byte header size (serial type sizes 2097149 to 268435451)
    const MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER: usize =
        MAX_SERIALTYPES_SIZE_FOR_3_BYTE_HEADER + 1;
    assert_eq!(
        Record::calc_header_size(MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER),
        MIN_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER + 4
    );
    const BITS_28_MAX: usize = (1 << 28) - 1;
    const MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER: usize = BITS_28_MAX - 4;
    assert_eq!(
        Record::calc_header_size(MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER),
        MAX_SERIALTYPES_SIZE_FOR_4_BYTE_HEADER + 4
    );
}

#[test]
fn test_integer_fast_path() {
    let index_info = create_index_info(
        2,
        vec![SortOrder::Asc, SortOrder::Asc],
        vec![CollationSeq::Binary; 2],
    );

    let test_cases = vec![
        (
            vec![Value::from_i64(42)],
            vec![ValueRef::from_i64(42)],
            "equal_integers",
        ),
        (
            vec![Value::from_i64(10)],
            vec![ValueRef::from_i64(20)],
            "less_than_integers",
        ),
        (
            vec![Value::from_i64(30)],
            vec![ValueRef::from_i64(20)],
            "greater_than_integers",
        ),
        (
            vec![Value::from_i64(0)],
            vec![ValueRef::from_i64(0)],
            "zero_integers",
        ),
        (
            vec![Value::from_i64(-5)],
            vec![ValueRef::from_i64(-5)],
            "negative_integers",
        ),
        (
            vec![Value::from_i64(i64::MAX)],
            vec![ValueRef::from_i64(i64::MAX)],
            "max_integers",
        ),
        (
            vec![Value::from_i64(i64::MIN)],
            vec![ValueRef::from_i64(i64::MIN)],
            "min_integers",
        ),
        (
            vec![Value::from_i64(42), Value::Text(Text::new("hello"))],
            vec![
                ValueRef::from_i64(42),
                ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
            ],
            "integer_text_equal",
        ),
        (
            vec![Value::from_i64(42), Value::Text(Text::new("hello"))],
            vec![
                ValueRef::from_i64(42),
                ValueRef::Text(TextRef::new("world", TextSubtype::Text)),
            ],
            "integer_equal_text_different",
        ),
    ];

    for (serialized_values, unpacked_values, test_name) in test_cases {
        println!(
            "Testing integer fast path `{test_name}`\nLHS: {serialized_values:?}\nRHS: {unpacked_values:?}"
        );
        assert_compare_matches_full_comparison(
            serialized_values,
            unpacked_values,
            &index_info,
            test_name,
        );
    }
}

#[test]
fn test_string_fast_path() {
    let index_info = create_index_info(
        2,
        vec![SortOrder::Asc, SortOrder::Asc],
        vec![CollationSeq::Binary; 2],
    );

    let test_cases = vec![
        (
            vec![Value::Text(Text::new("hello"))],
            vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
            "equal_strings",
        ),
        (
            vec![Value::Text(Text::new("abc"))],
            vec![ValueRef::Text(TextRef::new("def", TextSubtype::Text))],
            "less_than_strings",
        ),
        (
            vec![Value::Text(Text::new("xyz"))],
            vec![ValueRef::Text(TextRef::new("abc", TextSubtype::Text))],
            "greater_than_strings",
        ),
        (
            vec![Value::Text(Text::new(""))],
            vec![ValueRef::Text(TextRef::new("", TextSubtype::Text))],
            "empty_strings",
        ),
        (
            vec![Value::Text(Text::new("a"))],
            vec![ValueRef::Text(TextRef::new("aa", TextSubtype::Text))],
            "prefix_strings",
        ),
        // Multi-field with string first
        (
            vec![Value::Text(Text::new("hello")), Value::from_i64(42)],
            vec![
                ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                ValueRef::from_i64(42),
            ],
            "string_integer_equal",
        ),
        (
            vec![Value::Text(Text::new("hello")), Value::from_i64(42)],
            vec![
                ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
                ValueRef::from_i64(99),
            ],
            "string_equal_integer_different",
        ),
    ];

    for (serialized_values, unpacked_values, test_name) in test_cases {
        assert_compare_matches_full_comparison(
            serialized_values,
            unpacked_values,
            &index_info,
            test_name,
        );
    }
}

#[test]
fn test_type_precedence() {
    let index_info = create_index_info(1, vec![SortOrder::Asc], vec![CollationSeq::Binary]);

    // Test SQLite type precedence: NULL < Numbers < Text < Blob
    let test_cases = vec![
        // NULL vs others
        (
            vec![Value::Null],
            vec![ValueRef::from_i64(42)],
            "null_vs_integer",
        ),
        (
            vec![Value::Null],
            vec![ValueRef::from_f64(64.4)],
            "null_vs_float",
        ),
        (
            vec![Value::Null],
            vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
            "null_vs_text",
        ),
        (
            vec![Value::Null],
            vec![ValueRef::Blob(b"blob")],
            "null_vs_blob",
        ),
        // Numbers vs Text/Blob
        (
            vec![Value::from_i64(42)],
            vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
            "integer_vs_text",
        ),
        (
            vec![Value::from_f64(64.4)],
            vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
            "float_vs_text",
        ),
        (
            vec![Value::from_i64(42)],
            vec![ValueRef::Blob(b"blob")],
            "integer_vs_blob",
        ),
        (
            vec![Value::from_f64(64.4)],
            vec![ValueRef::Blob(b"blob")],
            "float_vs_blob",
        ),
        // Text vs Blob
        (
            vec![Value::Text(Text::new("hello"))],
            vec![ValueRef::Blob(b"blob")],
            "text_vs_blob",
        ),
        // Integer vs Float (affinity conversion)
        (
            vec![Value::from_i64(42)],
            vec![ValueRef::from_f64(42.0)],
            "integer_vs_equal_float",
        ),
        (
            vec![Value::from_i64(42)],
            vec![ValueRef::from_f64(42.5)],
            "integer_vs_different_float",
        ),
        (
            vec![Value::from_f64(42.5)],
            vec![ValueRef::from_i64(42)],
            "float_vs_integer",
        ),
    ];

    for (serialized_values, unpacked_values, test_name) in test_cases {
        assert_compare_matches_full_comparison(
            serialized_values,
            unpacked_values,
            &index_info,
            test_name,
        );
    }
}

#[test]
fn test_sort_order_desc() {
    let index_info = create_index_info(
        2,
        vec![SortOrder::Desc, SortOrder::Asc],
        vec![CollationSeq::Binary; 2],
    );

    let test_cases = vec![
        // DESC order should reverse first field comparison
        (
            vec![Value::from_i64(10)],
            vec![ValueRef::from_i64(20)],
            "desc_integer_reversed",
        ),
        (
            vec![Value::Text(Text::new("abc"))],
            vec![ValueRef::Text(TextRef::new("def", TextSubtype::Text))],
            "desc_string_reversed",
        ),
        // Mixed sort orders
        (
            vec![Value::from_i64(10), Value::Text(Text::new("hello"))],
            vec![
                ValueRef::from_i64(20),
                ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
            ],
            "desc_first_asc_second",
        ),
    ];

    for (serialized_values, unpacked_values, test_name) in test_cases {
        assert_compare_matches_full_comparison(
            serialized_values,
            unpacked_values,
            &index_info,
            test_name,
        );
    }
}

#[test]
fn test_edge_cases() {
    let index_info =
        create_index_info(15, vec![SortOrder::Asc; 15], vec![CollationSeq::Binary; 15]);

    let test_cases = vec![
        (
            vec![Value::from_i64(42)],
            vec![
                ValueRef::from_i64(42),
                ValueRef::Text(TextRef::new("extra", TextSubtype::Text)),
            ],
            "fewer_serialized_fields",
        ),
        (
            vec![Value::from_i64(42), Value::Text(Text::new("extra"))],
            vec![ValueRef::from_i64(42)],
            "fewer_unpacked_fields",
        ),
        (vec![], vec![], "both_empty"),
        (vec![], vec![ValueRef::from_i64(42)], "empty_serialized"),
        (
            (0..15).map(Value::from_i64).try_collect().unwrap(),
            (0..15).map(ValueRef::from_i64).try_collect().unwrap(),
            "large_field_count",
        ),
        (
            vec![Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG)],
            vec![ValueRef::Blob(&[1, 2, 3])],
            "blob_first_field",
        ),
        (
            vec![Value::Text(Text::new("hello")), Value::from_i64(5)],
            vec![ValueRef::Text(TextRef::new("hello", TextSubtype::Text))],
            "equal_text_prefix_but_more_serialized_fields",
        ),
        (
            vec![Value::Text(Text::new("same")), Value::from_i64(5)],
            vec![
                ValueRef::Text(TextRef::new("same", TextSubtype::Text)),
                ValueRef::from_i64(5),
            ],
            "equal_text_then_equal_int",
        ),
    ];

    for (serialized_values, unpacked_values, test_name) in test_cases {
        assert_compare_matches_full_comparison(
            serialized_values,
            unpacked_values,
            &index_info,
            test_name,
        );
    }
}

#[test]
fn compare_record_preserves_prefix_tie_breakers() {
    let index_info = create_index_info(2, vec![SortOrder::Asc; 2], vec![CollationSeq::Binary; 2]);
    for first in [Value::from_i64(42), Value::build_text("key"), Value::Null] {
        let serialized = create_record(vec![first.clone(), Value::from_i64(99)]);
        for tie_breaker in [Ordering::Less, Ordering::Equal, Ordering::Greater] {
            let right_values = [first.as_ref()];
            assert_eq!(
                compare_record(
                    serialized.get_payload(),
                    right_values.into_iter(),
                    &index_info,
                    tie_breaker,
                )
                .unwrap(),
                tie_breaker,
            );
        }
    }
}

#[test]
fn test_skip_parameter() {
    let index_info = create_index_info(
        3,
        vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc],
        vec![CollationSeq::Binary; 3],
    );

    let serialized = create_record(vec![
        Value::from_i64(1),
        Value::from_i64(2),
        Value::from_i64(3),
    ]);
    let unpacked = [
        ValueRef::from_i64(1),
        ValueRef::from_i64(99),
        ValueRef::from_i64(3),
    ];

    let tie_breaker = std::cmp::Ordering::Equal;
    let result_skip_0 = compare_payload_generic(
        serialized.get_payload(),
        unpacked.iter(),
        &index_info,
        0,
        tie_breaker,
    )
    .unwrap();
    let result_skip_1 = compare_payload_generic(
        serialized.get_payload(),
        unpacked.iter(),
        &index_info,
        1,
        tie_breaker,
    )
    .unwrap();

    assert_eq!(result_skip_0, std::cmp::Ordering::Less);

    assert_eq!(result_skip_1, std::cmp::Ordering::Less);
}

#[test]
fn test_strategy_selection() {
    let collations_small = vec![CollationSeq::Binary; 3];
    let collations_large = vec![CollationSeq::Binary; 15];
    let index_info_small = create_index_info(
        3,
        vec![SortOrder::Asc, SortOrder::Asc, SortOrder::Asc],
        collations_small,
    );
    let index_info_large = create_index_info(15, vec![SortOrder::Asc; 15], collations_large);

    let int_values = [
        ValueRef::from_i64(42),
        ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
    ];
    assert!(matches!(
        find_compare(int_values.iter().peekable(), &index_info_small),
        RecordCompare::Int {
            rhs_first_value: 42
        }
    ));

    let string_values = [
        ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
        ValueRef::from_i64(42),
    ];
    assert!(matches!(
        find_compare(string_values.iter().peekable(), &index_info_small),
        RecordCompare::String
    ));

    let large_values: Vec<ValueRef> = (0..15).map(ValueRef::from_i64).try_collect().unwrap();
    assert!(matches!(
        find_compare(large_values.iter().peekable(), &index_info_large),
        RecordCompare::Generic
    ));

    let blob_values = [ValueRef::Blob(&[1, 2, 3])];
    assert!(matches!(
        find_compare(blob_values.iter().peekable(), &index_info_small),
        RecordCompare::Generic
    ));
}

#[test]
fn test_serialize_null() {
    let record = Record::new(vec![Value::Null]);
    let mut buf = std::vec::Vec::new();
    record.serialize(&mut buf);

    let header_length = record.values.len() + 1;
    let header = &buf[0..header_length];
    // First byte should be header size
    assert_eq!(header[0], header_length as u8);
    // Second byte should be serial type for NULL
    assert_eq!(header[1] as u64, u64::from(SerialType::null()));
    // Check that the buffer is empty after the header
    assert_eq!(buf.len(), header_length);
}

#[test]
fn test_serialize_integers() {
    let record = Record::new(vec![
        Value::from_i64(0),                 // Should use ConstInt0
        Value::from_i64(1),                 // Should use ConstInt1
        Value::from_i64(42),                // Should use SERIAL_TYPE_I8
        Value::from_i64(1000),              // Should use SERIAL_TYPE_I16
        Value::from_i64(1_000_000),         // Should use SERIAL_TYPE_I24
        Value::from_i64(1_000_000_000),     // Should use SERIAL_TYPE_I32
        Value::from_i64(1_000_000_000_000), // Should use SERIAL_TYPE_I48
        Value::from_i64(i64::MAX),          // Should use SERIAL_TYPE_I64
    ]);
    let mut buf = std::vec::Vec::new();
    record.serialize(&mut buf);

    let header_length = record.values.len() + 1;
    let header = &buf[0..header_length];
    // First byte should be header size
    assert_eq!(header[0], header_length as u8); // Header should be larger than number of values

    // Check that correct serial types were chosen
    assert_eq!(header[1] as u64, u64::from(SerialType::const_int0())); // 8
    assert_eq!(header[2] as u64, u64::from(SerialType::const_int1())); // 9
    assert_eq!(header[3] as u64, u64::from(SerialType::i8())); // 1
    assert_eq!(header[4] as u64, u64::from(SerialType::i16())); // 2
    assert_eq!(header[5] as u64, u64::from(SerialType::i24())); // 3
    assert_eq!(header[6] as u64, u64::from(SerialType::i32())); // 4
    assert_eq!(header[7] as u64, u64::from(SerialType::i48())); // 5
    assert_eq!(header[8] as u64, u64::from(SerialType::i64())); // 6

    // test that the bytes after the header can be interpreted as the correct values
    let mut cur_offset = header_length;

    // Value::from_i64(0) - ConstInt0: NO PAYLOAD BYTES
    // Value::from_i64(1) - ConstInt1: NO PAYLOAD BYTES

    // Value::from_i64(42) - I8: 1 byte
    let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
    cur_offset += size_of::<i8>();

    // Value::from_i64(1000) - I16: 2 bytes
    let i16_bytes = &buf[cur_offset..cur_offset + size_of::<i16>()];
    cur_offset += size_of::<i16>();

    // Value::from_i64(1_000_000) - I24: 3 bytes
    let i24_bytes = &buf[cur_offset..cur_offset + 3];
    cur_offset += 3;

    // Value::from_i64(1_000_000_000) - I32: 4 bytes
    let i32_bytes = &buf[cur_offset..cur_offset + size_of::<i32>()];
    cur_offset += size_of::<i32>();

    // Value::from_i64(1_000_000_000_000) - I48: 6 bytes
    let i48_bytes = &buf[cur_offset..cur_offset + 6];
    cur_offset += 6;

    // Value::from_i64(i64::MAX) - I64: 8 bytes
    let i64_bytes = &buf[cur_offset..cur_offset + size_of::<i64>()];

    // Verify the payload values
    let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
    let val_int16 = i16::from_be_bytes(i16_bytes.try_into().unwrap());

    let mut i24_with_padding = vec![0];
    i24_with_padding.extend(i24_bytes);
    let val_int24 = i32::from_be_bytes(i24_with_padding.try_into().unwrap());

    let val_int32 = i32::from_be_bytes(i32_bytes.try_into().unwrap());

    let mut i48_with_padding = vec![0, 0];
    i48_with_padding.extend(i48_bytes);
    let val_int48 = i64::from_be_bytes(i48_with_padding.try_into().unwrap());

    let val_int64 = i64::from_be_bytes(i64_bytes.try_into().unwrap());

    assert_eq!(val_int8, 42);
    assert_eq!(val_int16, 1000);
    assert_eq!(val_int24, 1_000_000);
    assert_eq!(val_int32, 1_000_000_000);
    assert_eq!(val_int48, 1_000_000_000_000);
    assert_eq!(val_int64, i64::MAX);

    //Size of buffer = header + payload bytes
    // ConstInt0 and ConstInt1 contribute 0 bytes to payload
    assert_eq!(
        buf.len(),
        header_length  // 9 bytes (header size + 8 serial types)
            + size_of::<i8>()        // I8: 1 byte
            + size_of::<i16>()        // I16: 2 bytes
            + (size_of::<i32>() - 1)        // I24: 3 bytes
            + size_of::<i32>()        // I32: 4 bytes
            + (size_of::<i64>() - 2)        // I48: 6 bytes
            + size_of::<i64>() // I64: 8 bytes
    );
}

#[test]
fn test_serialize_const_integers() {
    let record = Record::new(vec![Value::from_i64(0), Value::from_i64(1)]);
    let mut buf = std::vec::Vec::new();
    record.serialize(&mut buf);

    // [header_size, serial_type_0, serial_type_1] + no payload bytes
    let expected_header_size = 3; // 1 byte for header size + 2 bytes for serial types

    assert_eq!(buf.len(), expected_header_size);

    // Check header size
    assert_eq!(buf[0], expected_header_size as u8);

    assert_eq!(buf[1] as u64, u64::from(SerialType::const_int0())); // Should be 8
    assert_eq!(buf[2] as u64, u64::from(SerialType::const_int1())); // Should be 9

    assert_eq!(buf[1], 8); // ConstInt0 serial type
    assert_eq!(buf[2], 9); // ConstInt1 serial type
}

#[test]
fn test_serialize_single_const_int0() {
    let record = Record::new(vec![Value::from_i64(0)]);
    let mut buf = std::vec::Vec::new();
    record.serialize(&mut buf);

    // Expected: [header_size=2, serial_type=8]
    assert_eq!(buf.len(), 2);
    assert_eq!(buf[0], 2); // Header size
    assert_eq!(buf[1], 8); // ConstInt0 serial type
}

#[test]
fn test_serialize_float() {
    #[warn(clippy::approx_constant)]
    let record = Record::new(vec![Value::from_f64(3.15555)]);
    let mut buf = std::vec::Vec::new();
    record.serialize(&mut buf);

    let header_length = record.values.len() + 1;
    let header = &buf[0..header_length];
    assert_eq!(header[0], header_length as u8);
    // Second byte should be serial type for FLOAT
    assert_eq!(header[1] as u64, u64::from(SerialType::f64()));
    // Check that the bytes after the header can be interpreted as the float
    let float_bytes = &buf[header_length..header_length + size_of::<f64>()];
    let float = f64::from_be_bytes(float_bytes.try_into().unwrap());
    assert_eq!(float, 3.15555);
    // Check that buffer length is correct
    assert_eq!(buf.len(), header_length + size_of::<f64>());
}

#[test]
fn test_serialize_text() {
    let text = "hello";
    let record = Record::new(vec![Value::Text(Text::new(text))]);
    let mut buf = std::vec::Vec::new();
    record.serialize(&mut buf);

    let header_length = record.values.len() + 1;
    let header = &buf[0..header_length];
    // First byte should be header size
    assert_eq!(header[0], header_length as u8);
    // Second byte should be serial type for TEXT, which is (len * 2 + 13)
    assert_eq!(header[1], (5 * 2 + 13) as u8);
    // Check the actual text bytes
    assert_eq!(&buf[2..7], b"hello");
    // Check that buffer length is correct
    assert_eq!(buf.len(), header_length + text.len());
}

#[test]
fn test_serialize_blob() {
    let blob = std::vec![1, 2, 3, 4, 5];
    let record = Record::new(vec![
        Value::from_slice(&blob).expect(crate::alloc::ALLOC_ERR_MSG)
    ]);
    let mut buf = std::vec::Vec::new();
    record.serialize(&mut buf);

    let header_length = record.values.len() + 1;
    let header = &buf[0..header_length];
    // First byte should be header size
    assert_eq!(header[0], header_length as u8);
    // Second byte should be serial type for BLOB, which is (len * 2 + 12)
    assert_eq!(header[1], (5 * 2 + 12) as u8);
    // Check the actual blob bytes
    assert_eq!(&buf[2..7], &[1, 2, 3, 4, 5]);
    // Check that buffer length is correct
    assert_eq!(buf.len(), header_length + blob.len());
}

#[test]
fn test_serialize_mixed_types() {
    let text = "test";
    let record = Record::new(vec![
        Value::Null,
        Value::from_i64(42),
        Value::from_f64(3.15),
        Value::Text(Text::new(text)),
    ]);
    let mut buf = std::vec::Vec::new();
    record.serialize(&mut buf);

    let header_length = record.values.len() + 1;
    let header = &buf[0..header_length];
    // First byte should be header size
    assert_eq!(header[0], header_length as u8);
    // Second byte should be serial type for NULL
    assert_eq!(header[1] as u64, u64::from(SerialType::null()));
    // Third byte should be serial type for I8
    assert_eq!(header[2] as u64, u64::from(SerialType::i8()));
    // Fourth byte should be serial type for F64
    assert_eq!(header[3] as u64, u64::from(SerialType::f64()));
    // Fifth byte should be serial type for TEXT, which is (len * 2 + 13)
    assert_eq!(header[4] as u64, (4 * 2 + 13) as u64);

    // Check that the bytes after the header can be interpreted as the correct values
    let mut cur_offset = header_length;
    let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
    cur_offset += size_of::<i8>();
    let f64_bytes = &buf[cur_offset..cur_offset + size_of::<f64>()];
    cur_offset += size_of::<f64>();
    let text_bytes = &buf[cur_offset..cur_offset + text.len()];

    let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
    let val_float = f64::from_be_bytes(f64_bytes.try_into().unwrap());
    let val_text = String::from_utf8(text_bytes.to_vec()).unwrap();

    assert_eq!(val_int8, 42);
    assert_eq!(val_float, 3.15);
    assert_eq!(val_text, "test");

    // Check that buffer length is correct
    assert_eq!(
        buf.len(),
        header_length + size_of::<i8>() + size_of::<f64>() + text.len()
    );
}

/// Before the Numeric refactor, ValueRef had separate Float(f64) and Integer(i64)
/// variants. A raw f64::NAN could be stored in Float, and comparing two NaN floats
/// via partial_cmp returned None. The .unwrap() in Ord::cmp and
/// compare_immutable_single would then panic.
///
/// Now Numeric::Float wraps NonNan, which rejects NaN at construction time.
/// This makes it impossible to represent NaN in a ValueRef, so partial_cmp
/// is total and can never return None for any representable value.
#[test]
fn test_valueref_partial_cmp_no_panic_on_nan() {
    use crate::numeric::nonnan::NonNan;

    // NonNan::new rejects NaN — this is the type-level guarantee that
    // prevents the old panic. No ValueRef::Float(NAN) can be constructed.
    assert!(NonNan::new(f64::NAN).is_none());

    // from_f64(NAN) falls back to Null instead of storing a NaN float.
    assert_eq!(ValueRef::from_f64(f64::NAN), ValueRef::Null);

    // Exercise every representable float edge case through partial_cmp,
    // Ord::cmp, and compare_immutable_single — none of these can panic now.
    let values: Vec<ValueRef> = vec![
        ValueRef::Null,
        ValueRef::from_i64(0),
        ValueRef::from_i64(-1),
        ValueRef::from_i64(i64::MAX),
        ValueRef::from_i64(i64::MIN),
        ValueRef::from_f64(0.0),
        ValueRef::from_f64(-0.0),
        ValueRef::from_f64(1.5),
        ValueRef::from_f64(-1.5),
        ValueRef::from_f64(f64::MAX),
        ValueRef::from_f64(f64::MIN),
        ValueRef::from_f64(f64::MIN_POSITIVE),
        ValueRef::from_f64(f64::INFINITY),
        ValueRef::from_f64(f64::NEG_INFINITY),
        ValueRef::from_f64(f64::NAN), // becomes Null
        ValueRef::Text(TextRef::new("hello", TextSubtype::Text)),
        ValueRef::Text(TextRef::new("", TextSubtype::Text)),
        ValueRef::Blob(&[1, 2, 3]),
        ValueRef::Blob(&[]),
    ];

    // partial_cmp must return Some for every pair — the old code panicked
    // here when either side was Float(NAN).
    for (i, a) in values.iter().enumerate() {
        for (j, b) in values.iter().enumerate() {
            let result = a.partial_cmp(b);
            assert!(
                result.is_some(),
                "partial_cmp returned None for values[{i}]={a:?} vs values[{j}]={b:?}"
            );
            // Ord::cmp (which previously called partial_cmp().unwrap()) must agree.
            assert_eq!(result.unwrap(), a.cmp(b));
        }
    }

    // compare_immutable_single is where the unwrap panic originally surfaced.
    for a in &values {
        for b in &values {
            let _ = compare_immutable_single(*a, *b, CollationSeq::Binary);
        }
    }

    // Antisymmetry holds for all pairs.
    for a in &values {
        for b in &values {
            let ab = a.cmp(b);
            let ba = b.cmp(a);
            assert_eq!(ab, ba.reverse(), "antisymmetry failed for {a:?} vs {b:?}");
        }
    }
}

#[test]
fn test_column_count_matches_values_written() {
    // Test with different numbers of values
    for num_values in 1..=10 {
        let values: Vec<Value> = (0..num_values)
            .map(|i| Value::from_i64(i as i64))
            .try_collect()
            .unwrap();

        let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
        let cnt = record.column_count();
        assert_eq!(
            cnt, num_values,
            "column_count should be {num_values}, not {cnt}"
        );
    }
}

#[test]
fn test_value_try_clone_from_reuses_allocations() {
    let src = Value::build_text(String::from("short"));
    let mut dst = Value::build_text(String::from("a destination string with plenty of capacity"));
    let ptr = match &dst {
        Value::Text(t) => t.as_str().as_ptr(),
        _ => unreachable!(),
    };
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);
    match &dst {
        Value::Text(t) => assert_eq!(t.as_str().as_ptr(), ptr),
        _ => unreachable!(),
    }

    let mut dst = Value::build_text("static text");
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);

    let src = Value::Blob(vec![1, 2, 3]);
    let mut big_blob: ValueBlob = vec![];
    big_blob.extend(0u8..32);
    let mut dst = Value::Blob(big_blob);
    let ptr = match &dst {
        Value::Blob(b) => b.as_ptr(),
        _ => unreachable!(),
    };
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);
    match &dst {
        Value::Blob(b) => assert_eq!(b.as_ptr(), ptr),
        _ => unreachable!(),
    }

    let src = Value::from_i64(9);
    let mut dst = Value::build_text("text");
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);
    let src = Value::build_text("into an integer slot");
    let mut dst = Value::from_i64(3);
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);
}

#[test]
fn test_build_reuses_retired_buffer_and_matches_from_values() {
    let mut blob: ValueBlob = vec![];
    blob.extend(0u8..64);
    let big = vec![
        Value::build_text("a longer text value that forces a real allocation"),
        Value::Blob(blob),
        Value::from_i64(42),
    ];
    let small = vec![Value::from_i64(1), Value::Null];

    let expected_big = ImmutableRecord::from_values(&big, big.len()).unwrap();
    let expected_small = ImmutableRecord::from_values(&small, small.len()).unwrap();

    let record = ImmutableRecord::build(&big, RecordBuf::alloc()).unwrap();
    assert_eq!(record.get_payload(), expected_big.get_payload());

    let capacity = record.as_blob().capacity();
    let ptr = record.get_payload().as_ptr();
    let record = ImmutableRecord::build(&small, record.retire()).unwrap();
    assert_eq!(record.get_payload(), expected_small.get_payload());
    assert_eq!(record.as_blob().capacity(), capacity);
    assert_eq!(record.get_payload().as_ptr(), ptr);

    let record = ImmutableRecord::build(&big, expected_small.retire()).unwrap();
    assert_eq!(record.get_payload(), expected_big.get_payload());
}

#[test]
fn test_copy_payload_reuses_buffer() {
    let values = vec![Value::build_text("payload to copy"), Value::from_i64(7)];
    let source = ImmutableRecord::from_values(&values, values.len()).unwrap();
    let spare = ImmutableRecord::from_values(&values, values.len()).unwrap();

    let ptr = spare.get_payload().as_ptr();
    let copy = ImmutableRecord::copy_payload(source.get_payload(), spare.retire()).unwrap();
    assert_eq!(copy.get_payload(), source.get_payload());
    assert_eq!(copy.get_payload().as_ptr(), ptr);
}

#[test]
fn test_value_try_clone() {
    let values = [
        Value::Null,
        Value::from_i64(7),
        Value::build_text("text"),
        Value::Blob(vec![1, 2, 3]),
    ];

    for value in values {
        assert_eq!(value.try_clone().unwrap(), value);
    }
}
