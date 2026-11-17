pub mod generation;
mod report;
pub mod runner;
pub mod summary;

use anyhow::{bail, Context, Result};
use serde_json::Value;

pub fn dump_ast(dump: &str) -> Result<Vec<Value>> {
    let sql = unwrap_dump(dump)?;
    let mut statements = Vec::new();
    let mut inserts = Vec::new();
    let mut pending = String::new();
    let mut copy_columns = None;
    let mut copy_rows = Vec::new();
    for line in sql.split_inclusive('\n') {
        if let Some(columns) = copy_columns {
            let row = line.strip_suffix('\n').unwrap_or(line);
            let row = row.strip_suffix('\r').unwrap_or(row);
            if row == "\\." {
                copy_rows.sort_by_cached_key(Value::to_string);
                statements.push(serde_json::json!({"CopyData": copy_rows}));
                copy_rows.clear();
                copy_columns = None;
            } else {
                let fields: Vec<_> = row.split('\t').map(decode_field).collect::<Result<_>>()?;
                if columns != 0 && fields.len() != columns {
                    bail!("COPY row has {} fields, expected {columns}", fields.len());
                }
                copy_rows.push(Value::Array(fields));
            }
            continue;
        }
        pending.push_str(line);
        if line.contains(';') {
            if let Ok(parsed) = pg_query::parse(&pending) {
                copy_columns = append_statements(parsed, &mut statements, &mut inserts)?;
                pending.clear();
            }
        }
    }
    if copy_columns.is_some() {
        bail!("unterminated COPY data");
    }
    let parsed = pg_query::parse(&pending).context("parse dump SQL")?;
    if append_statements(parsed, &mut statements, &mut inserts)?.is_some() {
        bail!("COPY statement has no data terminator");
    }
    append_rows(&mut statements, &mut inserts);
    Ok(statements)
}

fn append_statements(
    parsed: pg_query::ParseResult,
    statements: &mut Vec<Value>,
    inserts: &mut Vec<Value>,
) -> Result<Option<usize>> {
    let mut copy_columns = None;
    for statement in parsed.protobuf.stmts {
        if copy_columns.is_some() {
            bail!("SQL follows COPY header on the same line");
        }
        let node = statement.stmt.context("dump statement has no AST")?;
        let is_insert = matches!(node.node, Some(pg_query::NodeEnum::InsertStmt(_)));
        if let Some(pg_query::NodeEnum::CopyStmt(copy)) = &node.node {
            if !copy.is_from
                || !copy.filename.is_empty()
                || copy.is_program
                || !copy.options.is_empty()
                || copy.query.is_some()
                || copy.where_clause.is_some()
            {
                bail!("only default text COPY FROM STDIN dumps are supported");
            }
            copy_columns = Some(copy.attlist.len());
        }
        let mut value = serde_json::to_value(node)?;
        remove_locations(&mut value);
        if is_insert {
            inserts.push(value);
        } else {
            append_rows(statements, inserts);
            statements.push(value);
        }
    }
    Ok(copy_columns)
}

fn decode_field(field: &str) -> Result<Value> {
    if field == "\\N" {
        return Ok(Value::Null);
    }
    let mut bytes = field.bytes().peekable();
    let mut decoded = Vec::new();
    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            decoded.push(byte);
            continue;
        }
        let escape = bytes.next().context("trailing backslash in COPY field")?;
        decoded.push(match escape {
            b'b' => 8,
            b'f' => 12,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'v' => 11,
            b'0'..=b'7' => {
                let mut value = escape - b'0';
                for _ in 0..2 {
                    match bytes.peek() {
                        Some(b'0'..=b'7') => {
                            value = value
                                .wrapping_mul(8)
                                .wrapping_add(bytes.next().unwrap() - b'0')
                        }
                        _ => break,
                    }
                }
                value
            }
            b'x' => {
                let mut value = 0;
                let mut digits = 0;
                while digits < 2 {
                    let Some(digit) = bytes.peek().and_then(|b| (*b as char).to_digit(16)) else {
                        break;
                    };
                    bytes.next();
                    value = value * 16 + digit as u8;
                    digits += 1;
                }
                if digits == 0 {
                    b'x'
                } else {
                    value
                }
            }
            other => other,
        });
    }
    if decoded.contains(&0) {
        bail!("NUL in COPY text field");
    }
    Ok(Value::String(
        String::from_utf8(decoded).context("invalid UTF-8 in COPY field")?,
    ))
}

fn unwrap_dump(dump: &str) -> Result<&str> {
    let start = "\\restrict TursoPgdumpConformance";
    let end = "\\unrestrict TursoPgdumpConformance";
    let mut offset = 0;
    for line in dump.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == start {
            break;
        }
        if !line.trim().is_empty() && !line.trim_start().starts_with("--") {
            return Ok(dump);
        }
        offset += line.len();
    }
    if offset == dump.len() {
        return Ok(dump);
    }
    let rest = &dump[offset + start.len()..];
    let mut end_offset = rest.len();
    for line in rest.split_inclusive('\n').rev() {
        end_offset -= line.len();
        if line.trim_end_matches(['\r', '\n']) == end {
            return Ok(&rest[..end_offset]);
        }
        if !line.trim().is_empty() && !line.trim_start().starts_with("--") {
            bail!("missing dump unrestrict command or unexpected SQL after it");
        }
    }
    bail!("missing dump unrestrict command");
}

fn remove_locations(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            fields.remove("location");
            for value in fields.values_mut() {
                remove_locations(value);
            }
        }
        Value::Array(values) => values.iter_mut().for_each(remove_locations),
        _ => {}
    }
}

fn append_rows(statements: &mut Vec<Value>, rows: &mut Vec<Value>) {
    rows.sort_by_cached_key(Value::to_string);
    statements.append(rows);
}
