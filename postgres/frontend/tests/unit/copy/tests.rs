use super::*;

#[test]
fn test_basic_tsv() {
    let data = "1\thello\n2\tworld\n";
    let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
    assert_eq!(rows[1], vec![Some("2".into()), Some("world".into())]);
}

#[test]
fn test_null_values() {
    let data = "1\t\\N\n\\N\thello\n";
    let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
    assert_eq!(rows[0], vec![Some("1".into()), None]);
    assert_eq!(rows[1], vec![None, Some("hello".into())]);
}

#[test]
fn test_empty_string() {
    let data = "1\t\n";
    let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
    assert_eq!(rows[0], vec![Some("1".into()), Some(String::new())]);
}

#[test]
fn test_backslash_escapes() {
    let data = "hello\\\\world\tline1\\nline2\n";
    let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
    assert_eq!(
        rows[0],
        vec![Some("hello\\world".into()), Some("line1\nline2".into())]
    );
}

#[test]
fn test_end_of_data_marker() {
    let data = "1\thello\n\\.\n2\tworld\n";
    let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
}

#[test]
fn test_wrong_column_count() {
    let data = "1\t2\t3\n";
    let result = parse_copy_text_format(data, '\t', "\\N", 2);
    assert!(result.is_err());
}

#[test]
fn test_custom_delimiter() {
    let data = "1,hello\n2,world\n";
    let rows = parse_copy_text_format(data, ',', "\\N", 2).unwrap();
    assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
}

#[test]
fn test_custom_null_string() {
    let data = "1\tNULL\n";
    let rows = parse_copy_text_format(data, '\t', "NULL", 2).unwrap();
    assert_eq!(rows[0], vec![Some("1".into()), None]);
}

#[test]
fn test_header_skip() {
    let data = "id\tname\n1\thello\n";
    let rows = parse_copy_text_format(data, '\t', "\\N", 2).unwrap();
    assert_eq!(rows.len(), 2);
}
