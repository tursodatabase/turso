use super::*;

#[test]
fn test_pg_bytes_to_value_integer() {
    let val = pg_bytes_to_value(b"42", &Type::INT4).unwrap();
    assert_eq!(val, Value::from_i64(42));

    let val = pg_bytes_to_value(b"-100", &Type::INT8).unwrap();
    assert_eq!(val, Value::from_i64(-100));

    let val = pg_bytes_to_value(b"0", &Type::INT2).unwrap();
    assert_eq!(val, Value::from_i64(0));
}

#[test]
fn test_pg_bytes_to_value_float() {
    let val = pg_bytes_to_value(b"3.25", &Type::FLOAT8).unwrap();
    assert_eq!(val, Value::from_f64(3.25));

    let val = pg_bytes_to_value(b"-0.5", &Type::FLOAT4).unwrap();
    assert_eq!(val, Value::from_f64(-0.5));

    let val = pg_bytes_to_value(b"1.23", &Type::NUMERIC).unwrap();
    assert_eq!(val, Value::from_f64(1.23));
}

#[test]
fn test_pg_bytes_to_value_bool() {
    let val = pg_bytes_to_value(b"t", &Type::BOOL).unwrap();
    assert_eq!(val, Value::from_i64(1));

    let val = pg_bytes_to_value(b"f", &Type::BOOL).unwrap();
    assert_eq!(val, Value::from_i64(0));

    let val = pg_bytes_to_value(b"true", &Type::BOOL).unwrap();
    assert_eq!(val, Value::from_i64(1));

    let val = pg_bytes_to_value(b"false", &Type::BOOL).unwrap();
    assert_eq!(val, Value::from_i64(0));
}

#[test]
fn test_pg_bytes_to_value_text() {
    let val = pg_bytes_to_value(b"hello world", &Type::TEXT).unwrap();
    assert_eq!(val, Value::from_text("hello world".to_owned()));

    let val = pg_bytes_to_value(b"Alice", &Type::VARCHAR).unwrap();
    assert_eq!(val, Value::from_text("Alice".to_owned()));
}

#[test]
fn test_pg_bytes_to_value_bytea() {
    let val = pg_bytes_to_value(b"\\xDEADBEEF", &Type::BYTEA).unwrap();
    assert_eq!(val, Value::from_blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[test]
fn test_pg_bytes_to_value_unknown_type_as_text() {
    // Unknown types should be treated as text
    let val = pg_bytes_to_value(b"some-uuid-value", &Type::UUID).unwrap();
    assert_eq!(val, Value::from_text("some-uuid-value".to_owned()));
}

#[test]
fn test_pg_bytes_to_value_integer_parse_error() {
    let result = pg_bytes_to_value(b"not_a_number", &Type::INT4);
    assert!(result.is_err());
}

#[test]
fn test_pg_bytes_to_value_float_parse_error() {
    let result = pg_bytes_to_value(b"not_a_float", &Type::FLOAT8);
    assert!(result.is_err());
}

#[test]
fn test_pg_bytes_to_value_bool_invalid() {
    let result = pg_bytes_to_value(b"maybe", &Type::BOOL);
    assert!(result.is_err());
}

#[test]
fn test_decode_hex() {
    assert_eq!(
        decode_hex("DEADBEEF").unwrap(),
        vec![0xDE, 0xAD, 0xBE, 0xEF]
    );
    assert_eq!(decode_hex("00ff").unwrap(), vec![0x00, 0xFF]);
    assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
    assert!(decode_hex("0").is_err()); // odd length
    assert!(decode_hex("GG").is_err()); // invalid hex
}

#[test]
fn test_sqlite_type_to_pg_type() {
    assert_eq!(sqlite_type_to_pg_type("INTEGER"), Type::INT4);
    assert_eq!(sqlite_type_to_pg_type("INT"), Type::INT4);
    assert_eq!(sqlite_type_to_pg_type("INT4"), Type::INT4);
    assert_eq!(sqlite_type_to_pg_type("SMALLINT"), Type::INT4);
    assert_eq!(sqlite_type_to_pg_type("BIGINT"), Type::INT8);
    assert_eq!(sqlite_type_to_pg_type("INT8"), Type::INT8);
    assert_eq!(sqlite_type_to_pg_type("REAL"), Type::FLOAT8);
    assert_eq!(sqlite_type_to_pg_type("TEXT"), Type::TEXT);
    assert_eq!(sqlite_type_to_pg_type("BLOB"), Type::BYTEA);
    assert_eq!(sqlite_type_to_pg_type("BOOLEAN"), Type::BOOL);
    assert_eq!(sqlite_type_to_pg_type("TIMESTAMP"), Type::TIMESTAMP);
    assert_eq!(sqlite_type_to_pg_type("TIMESTAMPTZ"), Type::TIMESTAMPTZ);
    assert_eq!(sqlite_type_to_pg_type("DATE"), Type::DATE);
    assert_eq!(sqlite_type_to_pg_type("JSON"), Type::JSON);
    assert_eq!(sqlite_type_to_pg_type("JSONB"), Type::JSONB);
    assert_eq!(sqlite_type_to_pg_type("UUID"), Type::UUID);
    // Unknown types map to TEXT
    assert_eq!(sqlite_type_to_pg_type("UNKNOWN"), Type::TEXT);
}

#[test]
fn test_unknown_type_inference() {
    // UNKNOWN type should infer integers from numeric-looking strings
    let val = pg_bytes_to_value(b"42", &Type::UNKNOWN).unwrap();
    assert!(matches!(
        val,
        Value::Numeric(turso_core::Numeric::Integer(42))
    ));

    // UNKNOWN type should infer floats
    let val = pg_bytes_to_value(b"3.14", &Type::UNKNOWN).unwrap();
    if let Value::Numeric(turso_core::Numeric::Float(f)) = val {
        #[allow(clippy::approx_constant)]
        let expected = 3.14;
        assert!((f64::from(f) - expected).abs() < 0.001);
    } else {
        panic!("Expected Float");
    }

    // UNKNOWN type should keep text for non-numeric strings
    let val = pg_bytes_to_value(b"hello", &Type::UNKNOWN).unwrap();
    assert!(matches!(val, Value::Text(_)));
}

#[test]
fn test_is_create_table_as() {
    assert!(is_create_table_as("CREATE TABLE T AS SELECT 1"));
    assert!(is_create_table_as("CREATE TEMP TABLE T AS SELECT 1"));
    assert!(is_create_table_as("CREATE UNLOGGED TABLE T AS SELECT 1"));
    assert!(is_create_table_as(
        "CREATE TABLE IF NOT EXISTS T AS SELECT 1"
    ));
    assert!(is_create_table_as("CREATE TABLE S.T AS SELECT 1"));
    assert!(is_create_table_as("CREATE TABLE T AS(SELECT 1)"));
    assert!(is_create_table_as("CREATE TABLE \"T\" AS SELECT 1"));

    assert!(!is_create_table_as("CREATE TABLE T (X INT)"));
    assert!(!is_create_table_as("CREATE INDEX I ON T (X)"));
    assert!(!is_create_table_as("CREATE VIEW V AS SELECT 1"));
    // Quoted name containing whitespace: `AS` is part of the name.
    assert!(!is_create_table_as("CREATE TABLE \"A AS B\" (X INT)"));
}

#[test]
fn test_ends_with_with_no_data() {
    assert!(ends_with_with_no_data(
        "CREATE TABLE T AS SELECT 1 WITH NO DATA"
    ));
    assert!(ends_with_with_no_data(
        "CREATE TABLE T AS SELECT 1 WITH  NO\nDATA ; "
    ));

    assert!(!ends_with_with_no_data("CREATE TABLE T AS SELECT 1"));
    assert!(!ends_with_with_no_data("SELECT 'WITH NO DATA'"));
}
