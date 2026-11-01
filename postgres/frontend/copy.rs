//! COPY text encoding and parsing.
//!
//! Implements PostgreSQL's default COPY text format:
//! - One row per line (`\n`)
//! - Columns separated by tab (`\t`) by default
//! - `\N` = NULL
//! - Backslash escapes: `\\` = `\`, `\t` = tab, `\n` = newline, `\r` = CR,
//!   `\NNN` = octal byte, `\xNN` = hex byte
//! - Empty field = empty string (not NULL)
//! - Lines starting with `\.` = end of data

use crate::session::relation_lookup_namespaces;
use turso_core::{ColumnTypeKind, LimboError, Numeric, Result, Row, Value};
use turso_pg_parser::{copy::CopyToStdout, quote_identifier};

pub struct CopyTextEncoder {
    columns: Vec<CopyTextType>,
}

enum CopyTextType {
    Plain,
    Boolean,
    TimestampTz,
    ByteaArray,
}

impl CopyTextEncoder {
    pub fn new(stmt: &crate::Statement) -> Self {
        let columns = (0..stmt.num_columns())
            .map(|index| {
                let Some(info) = stmt.get_column_type_info(index).ok().flatten() else {
                    return CopyTextType::Plain;
                };
                if matches!(info.kind, ColumnTypeKind::Struct | ColumnTypeKind::Union) {
                    return CopyTextType::Plain;
                }
                std::iter::once(info.declared_name.as_str())
                    .chain(info.base_type.as_deref())
                    .find_map(|name| {
                        match (name.to_ascii_uppercase().as_str(), info.array_dimensions) {
                            ("BOOL" | "BOOLEAN", 0) => Some(CopyTextType::Boolean),
                            ("TIMESTAMPTZ", 0) => Some(CopyTextType::TimestampTz),
                            ("BLOB" | "BYTEA", 1..) => Some(CopyTextType::ByteaArray),
                            _ => None,
                        }
                    })
                    .unwrap_or(CopyTextType::Plain)
            })
            .collect();
        Self { columns }
    }

    /// Append one COPY text record, including its trailing newline.
    pub fn encode_row(&self, row: &Row, output: &mut Vec<u8>) {
        assert_eq!(row.len(), self.columns.len());
        for (index, (value, ty)) in row.get_values().zip(&self.columns).enumerate() {
            if index > 0 {
                output.push(b'\t');
            }
            match value {
                Value::Null => output.extend_from_slice(b"\\N"),
                Value::Numeric(Numeric::Integer(value)) => {
                    if matches!(ty, CopyTextType::Boolean) {
                        output.push(if *value != 0 { b't' } else { b'f' });
                    } else {
                        output.extend_from_slice(value.to_string().as_bytes());
                    }
                }
                Value::Numeric(Numeric::Float(value)) => {
                    let mut buffer = ryu::Buffer::new();
                    output.extend_from_slice(
                        copy_float_text(f64::from(*value), &mut buffer).as_bytes(),
                    );
                }
                Value::Text(text) => {
                    let text = text.value.as_ref();
                    escape_copy_text(text.as_bytes(), output);
                    if matches!(ty, CopyTextType::TimestampTz)
                        && !text.contains('+')
                        && !text.contains('Z')
                        && !text.ends_with("-00")
                    {
                        output.extend_from_slice(b"+00");
                    }
                }
                Value::Blob(bytes) => {
                    let array = matches!(ty, CopyTextType::ByteaArray);
                    if array {
                        escape_copy_text(b"\"\\", output);
                    }
                    escape_copy_text(b"\\x", output);
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    for byte in bytes {
                        output.push(HEX[(byte >> 4) as usize]);
                        output.push(HEX[(byte & 15) as usize]);
                    }
                    if array {
                        output.push(b'"');
                    }
                }
            }
        }
        output.push(b'\n');
    }
}

// Adapted from pgwire 0.36.3's ExtraFloatDigits::format_f64 (default options).
// Copyright (c) 2018 Ning Sun. See licenses/pgwire-mit-license.md.
fn copy_float_text(value: f64, buffer: &mut ryu::Buffer) -> &str {
    if value.is_nan() {
        "NaN"
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "inf"
        } else {
            "-inf"
        }
    } else if value == 0.0 {
        "0"
    } else {
        buffer.format(value)
    }
}

fn escape_copy_text(bytes: &[u8], output: &mut Vec<u8>) {
    for byte in bytes {
        match byte {
            b'\\' => output.extend_from_slice(b"\\\\"),
            b'\t' => output.extend_from_slice(b"\\t"),
            b'\n' => output.extend_from_slice(b"\\n"),
            b'\r' => output.extend_from_slice(b"\\r"),
            8 => output.extend_from_slice(b"\\b"),
            12 => output.extend_from_slice(b"\\f"),
            11 => output.extend_from_slice(b"\\v"),
            byte => output.push(*byte),
        }
    }
}

pub(crate) fn prepare_copy_to_stdout(
    conn: &std::sync::Arc<turso_core::Connection>,
    spec: &CopyToStdout,
) -> Result<turso_core::Statement> {
    let mut found = None;
    for namespace in relation_lookup_namespaces(conn, spec.schema.as_deref()) {
        let table = conn.with_schema(namespace.database_id(), |schema| {
            let ns = namespace.target.id;
            if schema.get_view(ns, &spec.table).is_some()
                || schema.is_materialized_view(ns, &spec.table)
            {
                return Err(LimboError::ParseError(
                    "COPY TO STDOUT requires a plain table".to_owned(),
                ));
            }
            schema
                .get_table(ns, &spec.table)
                .map(|table| {
                    table.btree().ok_or_else(|| {
                        LimboError::ParseError("COPY TO STDOUT requires a plain table".to_owned())
                    })
                })
                .transpose()
        })?;
        if let Some(table) = table {
            found = Some((namespace, table));
            break;
        }
    }
    let (namespace, table) = found.ok_or_else(|| {
        LimboError::ParseError(format!("COPY table '{}' does not exist", spec.table))
    })?;
    let columns = if spec.columns.is_empty() {
        table
            .columns()
            .iter()
            .filter(|column| !column.hidden() && !column.is_generated())
            .map(|column| column.name.as_ref().expect("table columns have names"))
            .collect::<Vec<_>>()
    } else {
        for name in &spec.columns {
            let (_, column) = table.get_column(name).ok_or_else(|| {
                LimboError::ParseError(format!("COPY column '{name}' does not exist"))
            })?;
            column.ensure_not_generated("COPY", name)?;
        }
        spec.columns.iter().collect()
    };
    let columns = columns
        .into_iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    conn.prepare_sqlite(format!(
        "SELECT {columns} FROM {}.{}",
        quote_identifier(&namespace.name),
        quote_identifier(&spec.table)
    ))
}

/// A parsed row from COPY text format: each element is None for NULL, Some for a value.
type CopyRow = Vec<Option<String>>;

/// Parse COPY text format data into rows of column values.
pub fn parse_copy_text_format(
    data: &str,
    delimiter: char,
    null_string: &str,
    num_columns: usize,
    header: bool,
) -> Result<Vec<CopyRow>> {
    let mut rows = Vec::new();

    for (line_num, line) in data.lines().enumerate() {
        // End-of-data marker (used in STDIN mode, but handle it for files too)
        if line == "\\." {
            break;
        }

        if header && line_num == 0 {
            continue;
        }

        // Skip empty lines at the end of file
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(delimiter).collect();
        if fields.len() != num_columns {
            return Err(LimboError::ParseError(format!(
                "COPY: line {}: expected {} columns, got {}",
                line_num + 1,
                num_columns,
                fields.len()
            )));
        }

        let row = fields
            .iter()
            .map(|field| {
                if *field == null_string {
                    Ok(None)
                } else {
                    unescape_copy_field(field).map(Some)
                }
            })
            .collect::<Result<CopyRow>>()?;

        rows.push(row);
    }

    Ok(rows)
}

/// Unescape backslash sequences in a COPY text field.
fn unescape_copy_field(field: &str) -> Result<String> {
    let mut result = Vec::with_capacity(field.len());
    let mut bytes = field.bytes().peekable();

    while let Some(b) = bytes.next() {
        if b != b'\\' {
            result.push(b);
            continue;
        }
        match bytes.next() {
            Some(b'\\') => result.push(b'\\'),
            Some(b't') => result.push(b'\t'),
            Some(b'n') => result.push(b'\n'),
            Some(b'r') => result.push(b'\r'),
            Some(b'b') => result.push(0x08),
            Some(b'f') => result.push(0x0C),
            Some(b'v') => result.push(0x0B),
            Some(first @ b'0'..=b'7') => {
                let mut value = first - b'0';
                for _ in 0..2 {
                    match bytes.next_if(|b| matches!(b, b'0'..=b'7')) {
                        Some(digit) => value = (value << 3).wrapping_add(digit - b'0'),
                        None => break,
                    }
                }
                result.push(value);
            }
            Some(b'x') if bytes.peek().is_some_and(u8::is_ascii_hexdigit) => {
                let mut value = 0;
                for _ in 0..2 {
                    match bytes.next_if(u8::is_ascii_hexdigit) {
                        Some(digit) => {
                            value = (value << 4) | (digit as char).to_digit(16).unwrap() as u8
                        }
                        None => break,
                    }
                }
                result.push(value);
            }
            Some(other) => result.push(other),
            None => result.push(b'\\'),
        }
    }

    let utf8_error = match std::str::from_utf8(&result) {
        Ok(_) => None,
        Err(e) => Some(e.valid_up_to()),
    };
    let nul = result.iter().position(|&b| b == 0);
    match utf8_error.into_iter().chain(nul).min() {
        None => Ok(String::from_utf8(result).expect("checked above")),
        Some(start) => Err(invalid_byte_sequence(&result[start..])),
    }
}

/// Builds PostgreSQL's error for a bad byte sequence. Like PostgreSQL, it names
/// every byte of the character the lead byte starts, not only the first one.
fn invalid_byte_sequence(bytes: &[u8]) -> LimboError {
    let char_len = match bytes[0] {
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf7 => 4,
        _ => 1,
    };
    let shown = bytes[..char_len.min(bytes.len())]
        .iter()
        .map(|b| format!("0x{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    LimboError::ParseError(format!(
        "invalid byte sequence for encoding \"UTF8\": {shown}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tsv() {
        let data = "1\thello\n2\tworld\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2, false).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
        assert_eq!(rows[1], vec![Some("2".into()), Some("world".into())]);
    }

    #[test]
    fn test_null_values() {
        let data = "1\t\\N\n\\N\thello\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2, false).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), None]);
        assert_eq!(rows[1], vec![None, Some("hello".into())]);
    }

    #[test]
    fn test_empty_string() {
        let data = "1\t\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2, false).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), Some(String::new())]);
    }

    #[test]
    fn test_backslash_escapes() {
        let data = "hello\\\\world\tline1\\nline2\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2, false).unwrap();
        assert_eq!(
            rows[0],
            vec![Some("hello\\world".into()), Some("line1\nline2".into())]
        );
    }

    #[test]
    fn test_octal_and_hex_escapes() {
        let data = "a\\x41b\t\\101z\n\\xg\t\\x4\n\\1234\t\\303\\xA9\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2, false).unwrap();
        assert_eq!(rows[0], vec![Some("aAb".into()), Some("Az".into())]);
        assert_eq!(rows[1], vec![Some("xg".into()), Some("\x04".into())]);
        assert_eq!(rows[2], vec![Some("S4".into()), Some("\u{e9}".into())]);
    }

    #[test]
    fn test_escapes_that_produce_invalid_utf8() {
        for (data, bad_bytes) in [
            ("\\377\n", "0xff"),
            ("\\xff\n", "0xff"),
            ("a\\0b\n", "0x00"),
            ("a\\377\\0\n", "0xff"),
            ("a\\0\\377\n", "0x00"),
            ("\\303(\n", "0xc3 0x28"),
            ("\\342\\202\n", "0xe2 0x82"),
            ("\\300\\200x\n", "0xc0 0x80"),
            ("\\360\\237\\230\n", "0xf0 0x9f 0x98"),
        ] {
            let result = parse_copy_text_format(data, '\t', "\\N", 1, false);
            let expected = format!("invalid byte sequence for encoding \"UTF8\": {bad_bytes}");
            assert!(
                matches!(&result, Err(LimboError::ParseError(msg)) if *msg == expected),
                "{data:?} gave {result:?}"
            );
        }
    }

    #[test]
    fn test_end_of_data_marker() {
        let data = "1\thello\n\\.\n2\tworld\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2, false).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
    }

    #[test]
    fn test_wrong_column_count() {
        let data = "1\t2\t3\n";
        let result = parse_copy_text_format(data, '\t', "\\N", 2, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_delimiter() {
        let data = "1,hello\n2,world\n";
        let rows = parse_copy_text_format(data, ',', "\\N", 2, false).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
    }

    #[test]
    fn test_custom_null_string() {
        let data = "1\tNULL\n";
        let rows = parse_copy_text_format(data, '\t', "NULL", 2, false).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), None]);
    }

    #[test]
    fn test_header_skip() {
        let data = "id\tname\n1\thello\n";
        let rows = parse_copy_text_format(data, '\t', "\\N", 2, false).unwrap();
        assert_eq!(rows.len(), 2);
        let rows = parse_copy_text_format(data, '\t', "\\N", 2, true).unwrap();
        assert_eq!(rows, vec![vec![Some("1".into()), Some("hello".into())]]);
    }

    #[test]
    fn test_header_is_skipped_without_decoding() {
        for data in ["h\\377\nok\n", "h\\0\nok\n", "a\tb\tc\nok\n", "\nok\n"] {
            let rows = parse_copy_text_format(data, '\t', "\\N", 1, true).unwrap();
            assert_eq!(rows, vec![vec![Some("ok".into())]], "{data:?}");
        }
    }

    #[test]
    fn test_end_of_data_marker_on_header_line() {
        let rows = parse_copy_text_format("\\.\nok\n", '\t', "\\N", 1, true).unwrap();
        assert!(rows.is_empty());
    }
}
