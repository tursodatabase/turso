//! COPY FROM text format parser.
//!
//! Implements PostgreSQL's default COPY text format:
//! - One row per line (`\n`)
//! - Columns separated by tab (`\t`) by default
//! - `\N` = NULL
//! - Backslash escapes: `\\` = `\`, `\t` = tab, `\n` = newline, `\r` = CR
//! - Empty field = empty string (not NULL)
//! - Lines starting with `\.` = end of data

use crate::{LimboError, Result};

/// A parsed row from COPY text format: each element is None for NULL, Some for a value.
type CopyRow = Vec<Option<String>>;

/// Parse COPY text format data into rows of column values.
pub fn parse_copy_text_format(
    data: &str,
    delimiter: char,
    null_string: &str,
    num_columns: usize,
) -> Result<Vec<CopyRow>> {
    let mut rows = Vec::new();

    for (line_num, line) in data.lines().enumerate() {
        // End-of-data marker (used in STDIN mode, but handle it for files too)
        if line == "\\." {
            break;
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
                Some(other) => {
                    result.push(other);
                }
                None => {
                    result.push('\\');
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
#[path = "tests/unit/copy/tests.rs"]
mod tests;
