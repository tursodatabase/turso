use super::*;

#[test]
fn parse_dsn_full() {
    let p = ConnParams::parse("postgres://alice:secret@db.example.com:5433/mydb").unwrap();
    assert_eq!(p.host, "db.example.com");
    assert_eq!(p.port, 5433);
    assert_eq!(p.user, "alice");
    assert_eq!(p.password.as_deref(), Some("secret"));
    assert_eq!(p.database, "mydb");
}

#[test]
fn parse_dsn_minimal() {
    let p = ConnParams::parse("postgres://localhost").unwrap();
    assert_eq!(p.host, "localhost");
    assert_eq!(p.port, 5432);
    assert_eq!(p.database, "postgres");
    assert_eq!(p.password, None);
}

#[test]
fn parse_dsn_query_string_ignored() {
    let p = ConnParams::parse("postgresql://h/db?sslmode=disable").unwrap();
    assert_eq!(p.host, "h");
    assert_eq!(p.database, "db");
}

#[test]
fn parse_dsn_rejects_other_schemes() {
    assert!(ConnParams::parse("mysql://localhost").is_err());
}

#[test]
fn parse_row_description_body() {
    // Two columns: "id" (int4, oid 23) and "name" (text, oid 25).
    let mut body = Vec::new();
    body.extend_from_slice(&2u16.to_be_bytes());
    for (name, oid) in [("id", 23u32), ("name", 25u32)] {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0u32.to_be_bytes()); // table oid
        body.extend_from_slice(&0u16.to_be_bytes()); // attnum
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes()); // typlen
        body.extend_from_slice(&0u32.to_be_bytes()); // typmod
        body.extend_from_slice(&0u16.to_be_bytes()); // format
    }
    let cols = parse_row_description(&body).unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[0].type_oid, 23);
    assert_eq!(cols[1].name, "name");
    assert_eq!(cols[1].type_oid, 25);
}

#[test]
fn parse_data_row_with_null() {
    let mut body = Vec::new();
    body.extend_from_slice(&2u16.to_be_bytes());
    body.extend_from_slice(&2i32.to_be_bytes());
    body.extend_from_slice(b"42");
    body.extend_from_slice(&(-1i32).to_be_bytes());
    let row = parse_data_row(&body).unwrap();
    assert_eq!(row, vec![Some("42".to_string()), None]);
}

#[test]
fn parse_data_row_truncated() {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&10i32.to_be_bytes());
    body.extend_from_slice(b"ab");
    assert!(parse_data_row(&body).is_err());
}

#[test]
fn parse_error_fields_body() {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'M');
    body.extend_from_slice(b"relation \"t\" does not exist\0");
    body.push(0);
    let fields = parse_error_fields(&body).unwrap();
    assert_eq!(fields[&b'S'], "ERROR");
    assert_eq!(error_message(&fields), "relation \"t\" does not exist");
}
