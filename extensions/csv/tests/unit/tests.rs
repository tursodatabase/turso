use super::*;
use std::io::Write;
use tempfile::NamedTempFile;
use turso_ext::{Value, ValueType};

fn write_csv(content: &str) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().expect("Failed to create temp file");
    write!(tmp, "{content}").unwrap();
    tmp
}

fn new_table(args: Vec<&str>) -> CsvTable {
    try_new_table(args).unwrap().1
}

fn try_new_table(args: Vec<&str>) -> Result<(String, CsvTable), ResultCode> {
    let args = &args
        .iter()
        .map(|s| Value::from_text(s.to_string()))
        .collect::<Vec<_>>();
    CsvVTabModule::create(args)
}

fn read_rows(mut cursor: CsvCursor, column_count: u32) -> Vec<Vec<Option<String>>> {
    let mut results = vec![];
    cursor.filter(&[], None);

    while !cursor.eof() {
        let mut row = vec![];

        for i in 0..column_count {
            let cell = match cursor.column(i) {
                Ok(v) => match v.value_type() {
                    ValueType::Null => None,
                    ValueType::Text => v.to_text().map(|s| s.to_owned()),
                    _ => panic!("Unexpected column type"),
                },
                Err(_) => panic!("Error reading column"),
            };
            row.push(cell);
        }

        results.push(row);
        cursor.next();
    }

    results
}

macro_rules! cell {
    ($x:expr) => {
        Some($x.to_owned())
    };
}

#[test]
fn test_file_with_header() {
    let file = write_csv("id,name\n1,Alice\n2,Bob\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=true",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice")],
            vec![cell!("2"), cell!("Bob")]
        ]
    );
}

#[test]
fn test_data_with_header() {
    let table = new_table(vec!["data=id,name\n1,Alice\n2,Bob\n", "header=true"]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice")],
            vec![cell!("2"), cell!("Bob")]
        ]
    );
}

#[test]
fn test_file_without_header() {
    let file = write_csv("1,Alice\n2,Bob\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=false",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice")],
            vec![cell!("2"), cell!("Bob")]
        ]
    );
}

#[test]
fn test_data_without_header() {
    let table = new_table(vec!["data=1,Alice\n2,Bob\n", "header=false"]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice")],
            vec![cell!("2"), cell!("Bob")]
        ]
    );
}

#[test]
fn test_empty_file_with_header() {
    let file = write_csv("id,name\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=true",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert!(rows.is_empty());
}

#[test]
fn test_empty_data_with_header() {
    let table = new_table(vec!["data=id,name\n", "header=true"]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert!(rows.is_empty());
}

#[test]
fn test_empty_file_no_header() {
    let file = write_csv("");
    let (schema, table) = try_new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=false",
    ])
    .unwrap();
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert!(rows.is_empty());
    assert_eq!(schema, "CREATE TABLE x (\"c0\" TEXT)");
}

#[test]
fn test_empty_data_no_header() {
    let (schema, table) = try_new_table(vec!["data=", "header=false"]).unwrap();
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert!(rows.is_empty());
    assert_eq!(schema, "CREATE TABLE x (\"c0\" TEXT)");
}

#[test]
fn test_empty_file_with_header_enabled() {
    let file = write_csv("");
    let (schema, table) = try_new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=true",
    ])
    .unwrap();
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert!(rows.is_empty());
    assert_eq!(schema, "CREATE TABLE x (\"(NULL)\" TEXT)");
}

#[test]
fn test_empty_data_with_header_enabled() {
    let (schema, table) = try_new_table(vec!["data=", "header=true"]).unwrap();
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert!(rows.is_empty());
    assert_eq!(schema, "CREATE TABLE x (\"(NULL)\" TEXT)");
}

#[test]
fn test_quoted_field() {
    let file = write_csv("id,name\n1,\"A,l,i,c,e\"\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=true",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(rows, vec![vec![cell!("1"), cell!("A,l,i,c,e")],]);
}

#[test]
fn test_quote_inside_field() {
    let file = write_csv("\"aaa\",\"b\"\"bb\",\"ccc\"\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=false",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 3);
    assert_eq!(
        rows,
        vec![vec![cell!("aaa"), cell!("b\"bb"), cell!("ccc")],]
    );
}

#[test]
fn test_custom_schema() {
    let file = write_csv("1,Alice\n2,Bob\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=false",
        "schema=CREATE TABLE x (id INT, name TEXT)",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice")],
            vec![cell!("2"), cell!("Bob")]
        ]
    );
}

#[test]
fn test_more_than_one_filename_argument() {
    let result = try_new_table(vec!["filename=aaa.csv", "filename=bbb.csv"]);
    assert!(matches!(result, Err(ResultCode::InvalidArgs)));
}

#[test]
fn test_more_than_one_data_argument() {
    let result = try_new_table(vec!["data=1,Alice\n2,Bob\n", "data=3,Alice\n4,Bob\n"]);
    assert!(matches!(result, Err(ResultCode::InvalidArgs)));
}

#[test]
fn test_more_than_one_schema_argument() {
    let result = try_new_table(vec![
        "schema=CREATE TABLE x (id INT, name TEXT)",
        "schema=CREATE TABLE x (key INT, value TEXT)",
    ]);
    assert!(matches!(result, Err(ResultCode::InvalidArgs)));
}

#[test]
fn test_more_than_one_columns_argument() {
    let result = try_new_table(vec!["columns=2", "columns=6"]);
    assert!(matches!(result, Err(ResultCode::InvalidArgs)));
}

#[test]
fn test_more_than_one_header_argument() {
    let result = try_new_table(vec!["header=true", "header=false"]);
    assert!(matches!(result, Err(ResultCode::InvalidArgs)));
}

#[test]
fn test_unrecognized_argument() {
    let result = try_new_table(vec!["non_existent=abc"]);
    assert!(matches!(result, Err(ResultCode::InvalidArgs)));
}

#[test]
fn test_missing_filename_and_data() {
    let result = try_new_table(vec!["header=false"]);
    assert!(matches!(result, Err(ResultCode::InvalidArgs)));
}

#[test]
fn test_conflicting_filename_and_data() {
    let result = try_new_table(vec!["filename=a.csv", "data=id,name\n1,Alice\n2,Bob\n"]);
    assert!(matches!(result, Err(ResultCode::InvalidArgs)));
}

#[test]
fn test_header_argument_parsing() {
    let true_values = ["true", "TRUE", "yes", "on", "1"];
    let false_values = ["false", "FALSE", "no", "off", "0"];

    for &val in &true_values {
        let result = try_new_table(vec![
            "data=id,name\n1,Alice\n2,Bob\n",
            &format!("header={val}"),
        ]);
        assert!(result.is_ok(), "Expected Ok for header='{val}'");
        assert!(result.unwrap().1.header, "Expected true for '{val}'");
    }

    for &val in &false_values {
        let result = try_new_table(vec![
            "data=id,name\n1,Alice\n2,Bob\n",
            &format!("header={val}"),
        ]);
        assert!(result.is_ok(), "Expected Ok for header='{val}'");
        assert!(!result.unwrap().1.header, "Expected false for '{val}'");
    }
}

#[test]
fn test_invalid_header_argument() {
    let invalid_values = ["tru", "2", "maybe", "onoff", "", "\"true\""];

    for &val in &invalid_values {
        let result = try_new_table(vec![
            "data=id,name\n1,Alice\n2,Bob\n",
            &format!("header={val}"),
        ]);
        assert!(matches!(result, Err(ResultCode::InvalidArgs)));
    }
}

#[test]
fn test_arguments_with_whitespace() {
    let table = new_table(vec![
        " data =    id,name\n1,Alice\n2,Bob\n ",
        "   header  =   true    ",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice")],
            vec![cell!("2"), cell!("Bob")]
        ]
    );
}

#[test]
fn test_unparsable_argument() {
    let unparsable_arguments = [
        "header",
        "schema='CREATE TABLE x (id INT, name TEXT)",
        "schema=\"CREATE TABLE x (id INT, name TEXT)",
        "schema=\"CREATE TABLE x (id INT, name TEXT)'",
    ];

    for &val in &unparsable_arguments {
        let result = try_new_table(vec!["data=id,name\n1,Alice\n2,Bob\n", val]);
        assert!(matches!(result, Err(ResultCode::InvalidArgs)));
    }
}

#[test]
fn test_escaped_quote() {
    let quotes = ["'", "\""];

    for &quote in &quotes {
        let table = new_table(vec![&format!("data={quote}aa{quote}{quote}bb{quote}")]);
        let cursor = table.open(None).unwrap();
        let rows = read_rows(cursor, 1);
        assert_eq!(rows, vec![vec![cell!(format!("aa{quote}bb"))]]);
    }
}

#[test]
fn test_unescaped_quote() {
    let cases = [("", "'"), ("", "\""), ("'", "\""), ("\"", "'")];

    for &case in &cases {
        let (outer, inner) = case;
        let table = new_table(vec![&format!("data={outer}aa{inner}{inner}bb{outer}")]);
        let cursor = table.open(None).unwrap();
        let rows = read_rows(cursor, 1);
        assert_eq!(rows, vec![vec![cell!(format!("aa{inner}{inner}bb"))]]);
    }
}

#[test]
fn test_non_existent_file() {
    let result = try_new_table(vec!["filename=non_existent.csv"]);
    assert!(matches!(result, Err(ResultCode::Error)));
}

#[test]
fn test_invalid_columns_argument() {
    let invalid_values = ["0", "-2", "\"2\"", "'2'"];

    for &val in &invalid_values {
        let result = try_new_table(vec![
            "data=id,name\n1,Alice\n2,Bob\n",
            &format!("columns={val}"),
        ]);
        assert!(matches!(result, Err(ResultCode::InvalidArgs)));
    }
}

#[test]
fn test_more_columns_than_in_file() {
    let file = write_csv("1,Alice\n2,Bob\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=false",
        "columns=4",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 4);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice"), None, None],
            vec![cell!("2"), cell!("Bob"), None, None]
        ]
    );
}

#[test]
fn test_fewer_columns_than_in_file() {
    let file = write_csv("1,Alice\n2,Bob\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=false",
        "columns=1",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 1);
    assert_eq!(rows, vec![vec![cell!("1")], vec![cell!("2")]]);
}

#[test]
fn test_fewer_columns_than_in_schema() {
    let file = write_csv("1,Alice,2002\n2,Bob,2000\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=false",
        "columns=1",
        "schema='CREATE TABLE x (id INT, name TEXT)'",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(rows, vec![vec![cell!("1"), None], vec![cell!("2"), None]]);
}

#[test]
fn test_more_columns_than_in_schema() {
    let file = write_csv("1,Alice,2002\n2,Bob,2000\n");
    let table = new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=false",
        "columns=5",
        "schema='CREATE TABLE x (id INT, name TEXT)'",
    ]);
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice")],
            vec![cell!("2"), cell!("Bob")]
        ]
    );
}

#[test]
fn test_double_quote_in_header() {
    let file = write_csv("id,first\"name\n1,Alice\n2,Bob\n");
    let (schema, table) = try_new_table(vec![
        &format!("filename={}", file.path().to_string_lossy()),
        "header=true",
    ])
    .unwrap();
    let cursor = table.open(None).unwrap();
    let rows = read_rows(cursor, 2);
    assert_eq!(
        rows,
        vec![
            vec![cell!("1"), cell!("Alice")],
            vec![cell!("2"), cell!("Bob")]
        ]
    );
    assert_eq!(
        schema,
        "CREATE TABLE x (\"id\" TEXT, \"first\"\"name\" TEXT)"
    );
}
