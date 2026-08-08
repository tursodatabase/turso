//! COPY FROM/TO format parser and writer.
//!
//! Text format:
//! - One row per line (`\n`)
//! - Columns separated by tab (`\t`) by default
//! - `\N` = NULL; empty field = empty string
//! - Backslash escapes: `\\` = `\`, `\t` = tab, `\n` = newline, `\r` = CR
//! - Lines starting with `\.` = end of data
//!
//! CSV format:
//! - Comma delimiter, double-quote quoting, and an empty default NULL marker

use turso_pg_parser::translator::{
    PgCopyCsvOptions, PgCopyFromFormat, PgCopyTextOptions, PgCopyToFormat,
};

use crate::{LimboError, Result};
use std::io::{BufWriter, Write};

/// A parsed row from COPY format: each element is None for NULL, Some for a value.
type CopyRow = Vec<Option<String>>;

#[allow(clippy::large_enum_variant)]
pub(crate) enum CopyToWriter<'a, W: Write> {
    Text {
        writer: BufWriter<W>,
        options: &'a PgCopyTextOptions,
    },
    Csv {
        writer: csv::Writer<W>,
        options: &'a PgCopyCsvOptions,
    },
}

impl<'a, W: Write> CopyToWriter<'a, W> {
    pub(crate) fn new(writer: W, format: &'a PgCopyToFormat) -> Self {
        match format {
            PgCopyToFormat::Text(options) => Self::Text {
                writer: BufWriter::new(writer),
                options,
            },
            PgCopyToFormat::Csv(options) => Self::Csv {
                writer: new_csv_writer(writer, options),
                options,
            },
        }
    }

    pub(crate) fn write_header(&mut self, columns: &[String]) -> Result<()> {
        match self {
            Self::Text { writer, options } if options.header => {
                let fields: Vec<Option<String>> = columns.iter().map(|c| Some(c.clone())).collect();
                write_text_record(writer, &fields, options)
            }
            Self::Csv { writer, options } if options.header => writer
                .write_record(columns)
                .map_err(|e| LimboError::Conflict(e.to_string())),
            _ => Ok(()),
        }
    }

    pub(crate) fn write_record(&mut self, fields: &[Option<String>]) -> Result<()> {
        match self {
            Self::Text { writer, options } => write_text_record(writer, fields, options),
            Self::Csv { writer, options } => write_csv_record(writer, fields, options),
        }
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        match &mut self {
            Self::Text { writer, .. } => writer
                .flush()
                .map_err(|e| LimboError::Conflict(e.to_string())),
            Self::Csv { writer, .. } => writer
                .flush()
                .map_err(|e| LimboError::Conflict(e.to_string())),
        }
    }
}

/// Parse COPY FROM input, dispatching to text or CSV based on `format`.
pub(crate) fn parse_copy_format(
    data: &str,
    format: &PgCopyFromFormat,
    num_columns: usize,
) -> Result<Vec<CopyRow>> {
    match format {
        PgCopyFromFormat::Text(t) => parse_copy_text_format(data, num_columns, t),
        PgCopyFromFormat::Csv(c) => parse_copy_csv_format(data, num_columns, c),
    }
}

fn new_csv_writer<W: Write>(w: W, options: &PgCopyCsvOptions) -> csv::Writer<W> {
    let delimiter = options
        .delimiter
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or(',');
    let quote = options
        .quote
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or('"');
    let escape = options
        .escape
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or(quote);

    let mut builder = csv::WriterBuilder::new();
    builder.delimiter(delimiter as u8);
    builder.quote(quote as u8);
    if escape != quote {
        builder.double_quote(false);
        builder.escape(escape as u8);
    }
    builder.from_writer(w)
}

fn write_text_record<W: Write>(
    writer: &mut W,
    fields: &[Option<String>],
    options: &PgCopyTextOptions,
) -> Result<()> {
    let delimiter = options
        .delimiter
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or('\t');
    let null_string = options.null_string.as_deref().unwrap_or("\\N");

    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            write!(writer, "{delimiter}").map_err(|e| LimboError::Conflict(e.to_string()))?;
        }
        match field {
            None => {
                write!(writer, "{null_string}").map_err(|e| LimboError::Conflict(e.to_string()))?
            }
            Some(value) => {
                let escaped = escape_copy_text_field(value, delimiter);
                write!(writer, "{escaped}").map_err(|e| LimboError::Conflict(e.to_string()))?;
            }
        }
    }
    writeln!(writer).map_err(|e| LimboError::Conflict(e.to_string()))
}

fn write_csv_record<W: Write>(
    writer: &mut csv::Writer<W>,
    fields: &[Option<String>],
    options: &PgCopyCsvOptions,
) -> Result<()> {
    let null_string = options.null_string.as_deref().unwrap_or("");
    writer
        .write_record(
            fields
                .iter()
                .map(|field| field.as_deref().unwrap_or(null_string)),
        )
        .map_err(|e| LimboError::Conflict(e.to_string()))
}

fn parse_copy_csv_format(
    data: &str,
    num_columns: usize,
    options: &PgCopyCsvOptions,
) -> Result<Vec<CopyRow>> {
    let delimiter = options
        .delimiter
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or(',');
    let quote = options
        .quote
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or('"');
    let escape = options
        .escape
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or(quote);
    let null_string = options.null_string.as_deref().unwrap_or("");

    // has_headers(false): header skipping is handled manually below
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter as u8)
        .quote(quote as u8)
        .has_headers(false)
        .escape(u8::try_from(escape).ok())
        .from_reader(data.as_bytes());

    let mut rows = Vec::new();
    let skip = usize::from(options.header);
    for (line_num, record) in reader.records().enumerate().skip(skip) {
        match record {
            Ok(record) => {
                if record.len() != num_columns {
                    return Err(LimboError::ParseError(format!(
                        "COPY: line {}: expected {} columns, got {}",
                        line_num + 1,
                        num_columns,
                        record.len()
                    )));
                }
                let row: CopyRow = record
                    .iter()
                    .map(|field| {
                        if field == null_string {
                            None
                        } else {
                            Some(field.to_string())
                        }
                    })
                    .collect();
                rows.push(row);
            }
            Err(e) => return Err(LimboError::ParseError(format!("COPY: got err: {e}"))),
        }
    }
    Ok(rows)
}

fn parse_copy_text_format(
    data: &str,
    num_columns: usize,
    options: &PgCopyTextOptions,
) -> Result<Vec<CopyRow>> {
    let delimiter = options
        .delimiter
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or('\t');
    let null_string = options.null_string.as_deref().unwrap_or("\\N");
    let mut rows = Vec::new();
    let mut header_skipped = false;

    // `str::lines()` keeps a lone empty line but skips the extra row after a trailing newline.
    for (line_num, line) in data.lines().enumerate() {
        // End-of-data marker (used in STDIN mode, but handle it for files too)
        if line == "\\." {
            break;
        }
        if options.header && !header_skipped {
            header_skipped = true;
            continue;
        }

        let fields: Vec<&str> = split_copy_text_line(line, delimiter);
        if fields.len() != num_columns {
            return Err(LimboError::ParseError(format!(
                "COPY: line {}: expected {} columns, got {}",
                line_num + 1,
                num_columns,
                fields.len()
            )));
        }

        let row: CopyRow = fields
            .iter()
            .map(|field| {
                if *field == null_string {
                    None
                } else {
                    Some(unescape_copy_field(field))
                }
            })
            .collect();
        rows.push(row);
    }
    Ok(rows)
}

/// Unescape backslash sequences in a COPY text field.
fn unescape_copy_field(field: &str) -> String {
    let mut result = String::with_capacity(field.len());
    let mut chars = field.chars();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => result.push('\\'),
                Some('t') => result.push('\t'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('b') => result.push('\x08'),
                Some('f') => result.push('\x0C'),
                Some('v') => result.push('\x0B'),
                Some(other) => result.push(other),
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

/// Split a COPY text line into raw field slices, treating the delimiter as a field
/// separator only when it is not preceded by a backslash.
/// Each returned slice still contains backslash escape sequences; the caller must
/// pass them through `unescape_copy_field` to get the final value.
fn split_copy_text_line(line: &str, delimiter: char) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut field_start = 0;
    let mut chars = line.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            chars.next();
        } else if c == delimiter {
            fields.push(&line[field_start..i]);
            field_start = i + delimiter.len_utf8();
        }
    }
    fields.push(&line[field_start..]);
    fields
}

/// Escape special characters for COPY text output.
/// Backslash, tab, newline, and carriage return are always escaped.
/// If the delimiter appears in the value it is also escaped so round-trips parse cleanly.
fn escape_copy_text_field(value: &str, delimiter: char) -> String {
    let mut result = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '\t' => result.push_str("\\t"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            c if c == delimiter => {
                result.push('\\');
                result.push(c);
            }
            c => result.push(c),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_pg_parser::translator::{PgCopyCsvOptions, PgCopyFromFormat, PgCopyTextOptions};

    fn text_format(delimiter: Option<&str>, null_string: Option<&str>) -> PgCopyFromFormat {
        PgCopyFromFormat::Text(PgCopyTextOptions {
            delimiter: delimiter.map(str::to_owned),
            null_string: null_string.map(str::to_owned),
            header: false,
        })
    }

    fn csv_format(opts: PgCopyCsvOptions) -> PgCopyFromFormat {
        PgCopyFromFormat::Csv(opts)
    }

    fn default_csv() -> PgCopyCsvOptions {
        PgCopyCsvOptions::default()
    }

    fn default_text_opts() -> PgCopyTextOptions {
        PgCopyTextOptions::default()
    }

    // --- text FROM tests ---

    #[test]
    fn test_basic_tsv() {
        let data = "1\thello\n2\tworld\n";
        let rows = parse_copy_format(data, &text_format(None, None), 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
        assert_eq!(rows[1], vec![Some("2".into()), Some("world".into())]);
    }

    #[test]
    fn test_null_values() {
        let data = "1\t\\N\n\\N\thello\n";
        let rows = parse_copy_format(data, &text_format(None, None), 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), None]);
        assert_eq!(rows[1], vec![None, Some("hello".into())]);
    }

    #[test]
    fn test_empty_string() {
        let data = "1\t\n";
        let rows = parse_copy_format(data, &text_format(None, None), 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), Some(String::new())]);
    }

    #[test]
    fn test_empty_line_single_column_is_empty_string() {
        // A bare newline is one row whose single field is an empty string.
        let data = "\n";
        let rows = parse_copy_format(data, &text_format(None, None), 1).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![Some(String::new())]);
    }

    #[test]
    fn test_empty_line_multi_column_is_error() {
        // A bare newline with a 2-column table is a column-count mismatch, not a skip.
        let data = "\n";
        let result = parse_copy_format(data, &text_format(None, None), 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_backslash_escapes() {
        let data = "hello\\\\world\tline1\\nline2\n";
        let rows = parse_copy_format(data, &text_format(None, None), 2).unwrap();
        assert_eq!(
            rows[0],
            vec![Some("hello\\world".into()), Some("line1\nline2".into())]
        );
    }

    #[test]
    fn test_end_of_data_marker() {
        let data = "1\thello\n\\.\n2\tworld\n";
        let rows = parse_copy_format(data, &text_format(None, None), 2).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
    }

    #[test]
    fn test_wrong_column_count() {
        let data = "1\t2\t3\n";
        let result = parse_copy_format(data, &text_format(None, None), 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_delimiter() {
        let data = "1,hello\n2,world\n";
        let rows = parse_copy_format(data, &text_format(Some(","), None), 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
    }

    #[test]
    fn test_custom_null_string() {
        let data = "1\tNULL\n";
        let rows = parse_copy_format(data, &text_format(None, Some("NULL")), 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), None]);
    }

    // --- text TO tests ---

    #[test]
    fn test_write_text_record_basic() {
        let mut out = Vec::new();
        let fields = vec![Some("1".to_owned()), Some("hello".to_owned())];
        write_text_record(&mut out, &fields, &default_text_opts()).unwrap();
        assert_eq!(out, b"1\thello\n");
    }

    #[test]
    fn test_write_text_record_null() {
        let mut out = Vec::new();
        let fields = vec![Some("1".to_owned()), None];
        write_text_record(&mut out, &fields, &default_text_opts()).unwrap();
        assert_eq!(out, b"1\t\\N\n");
    }

    #[test]
    fn test_write_text_record_escapes_special_chars() {
        let mut out = Vec::new();
        let fields = vec![Some("a\tb".to_owned()), Some("c\nd".to_owned())];
        write_text_record(&mut out, &fields, &default_text_opts()).unwrap();
        assert_eq!(out, b"a\\tb\tc\\nd\n");
    }

    #[test]
    fn test_write_text_record_roundtrip() {
        let original = vec![
            Some("hello\\world".to_owned()),
            Some("line1\nline2".to_owned()),
        ];
        let mut out = Vec::new();
        write_text_record(&mut out, &original, &default_text_opts()).unwrap();
        let written = String::from_utf8(out).unwrap();
        let parsed = parse_copy_format(&written, &text_format(None, None), 2).unwrap();
        assert_eq!(parsed[0], original);
    }

    #[test]
    fn test_custom_delimiter_with_escaped_delimiter_in_value() {
        // A value that contains the delimiter character must roundtrip correctly.
        // write_text_record escapes it as \,; split_copy_text_line must not split on it.
        let delimiter = Some(",");
        let original = vec![
            Some("a,b".to_owned()), // comma inside value
            Some("plain".to_owned()),
        ];
        let opts = PgCopyTextOptions {
            delimiter: Some(",".to_owned()),
            ..Default::default()
        };
        let mut out = Vec::new();
        write_text_record(&mut out, &original, &opts).unwrap();
        // written form: "a\,b,plain\n"
        let written = String::from_utf8(out).unwrap();
        let parsed = parse_copy_format(&written, &text_format(delimiter, None), 2).unwrap();
        assert_eq!(parsed[0], original);
    }

    // --- CSV FROM tests ---

    #[test]
    fn test_csv_basic() {
        let data = "1,hello\n2,world\n";
        let rows = parse_copy_format(data, &csv_format(default_csv()), 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Some("1".into()), Some("hello".into())]);
        assert_eq!(rows[1], vec![Some("2".into()), Some("world".into())]);
    }

    #[test]
    fn test_csv_quoted_comma() {
        let data = "\"hello, world\",42\n";
        let rows = parse_copy_format(data, &csv_format(default_csv()), 2).unwrap();
        assert_eq!(
            rows[0],
            vec![Some("hello, world".into()), Some("42".into())]
        );
    }

    #[test]
    fn test_csv_escaped_quote() {
        let data = "\"say \"\"hi\"\"\",42\n";
        let rows = parse_copy_format(data, &csv_format(default_csv()), 2).unwrap();
        assert_eq!(rows[0], vec![Some("say \"hi\"".into()), Some("42".into())]);
    }

    #[test]
    fn test_csv_header_skipped() {
        let data = "id,name\n1,alice\n";
        let opts = PgCopyCsvOptions {
            header: true,
            ..Default::default()
        };
        let rows = parse_copy_format(data, &csv_format(opts), 2).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![Some("1".into()), Some("alice".into())]);
    }

    #[test]
    fn test_csv_empty_field_is_null() {
        let data = "1,\n";
        let rows = parse_copy_format(data, &csv_format(default_csv()), 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), None]);
    }

    #[test]
    fn test_csv_custom_null_string() {
        let data = "1,NULL\n";
        let opts = PgCopyCsvOptions {
            null_string: Some("NULL".to_owned()),
            ..Default::default()
        };
        let rows = parse_copy_format(data, &csv_format(opts), 2).unwrap();
        assert_eq!(rows[0], vec![Some("1".into()), None]);
    }

    // --- CSV TO tests ---

    #[test]
    fn test_write_csv_record_basic() {
        let mut out = Vec::new();
        let fields = vec![Some("1".to_owned()), Some("hello".to_owned())];
        let mut w = new_csv_writer(&mut out, &default_csv());
        write_csv_record(&mut w, &fields, &default_csv()).unwrap();
        w.flush().unwrap();
        drop(w);
        assert_eq!(out, b"1,hello\n");
    }

    #[test]
    fn test_write_csv_record_null() {
        let mut out = Vec::new();
        let fields = vec![Some("1".to_owned()), None];
        let mut w = new_csv_writer(&mut out, &default_csv());
        write_csv_record(&mut w, &fields, &default_csv()).unwrap();
        w.flush().unwrap();
        drop(w);
        assert_eq!(out, b"1,\n");
    }

    #[test]
    fn test_write_csv_record_quoted_comma() {
        let mut out = Vec::new();
        let fields = vec![Some("hello, world".to_owned()), Some("42".to_owned())];
        let mut w = new_csv_writer(&mut out, &default_csv());
        write_csv_record(&mut w, &fields, &default_csv()).unwrap();
        w.flush().unwrap();
        drop(w);
        assert_eq!(out, b"\"hello, world\",42\n");
    }
}
