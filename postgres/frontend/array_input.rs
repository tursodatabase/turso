//! The text of an array literal read as PostgreSQL's array_in reads it: the
//! shape is checked, each element is read with the input function of the
//! element type, and the array is written back in one canonical form.

use crate::casts::CastType;
use crate::error::sqlstate_error;
use crate::functions::{parse_pg_bool, validate_pg_input};
use turso_core::{LimboError, Register, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

/// `pg_array_in(text, element_type)`: the array literal `text` with its
/// elements read as `element_type`, which is a type name such as `int4`.
pub(crate) fn exec_pg_array_in(args: &[Register]) -> Result<Value> {
    let Value::Text(text) = args[0].get_value() else {
        return Ok(args[0].get_value().clone());
    };
    let element_type = match args[1].get_value() {
        Value::Text(name) => CastType::from_name(name.as_str()),
        _ => None,
    };
    let Some(element_type) = element_type else {
        return Err(LimboError::InternalError(
            "pg_array_in needs the name of an element type".to_string(),
        ));
    };
    let text = text.as_str();
    let array = parse(text)?;
    if shape_of(&array).len() > 1 {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "an array literal with more than one dimension is not supported",
            None,
        ));
    }
    let mut out = String::with_capacity(text.len());
    write_array(&array, element_type, &mut out)?;
    Ok(Value::build_text(out))
}

/// The types whose elements `pg_array_in` knows how to read.
pub(crate) fn reads_elements_of(element_type: CastType) -> bool {
    use CastType::*;
    matches!(
        element_type,
        Int2 | Int4 | Int8 | Float4 | Float8 | Bool | Uuid | Text
    )
}

enum Node {
    Null,
    Element(String),
    Array(Vec<Node>),
}

fn parse(text: &str) -> Result<Node> {
    let malformed = |detail: &str| {
        sqlstate_error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            format!("malformed array literal: \"{text}\""),
            Some(detail.to_string()),
        )
    };
    let mut chars = text.trim_start().chars().peekable();
    match chars.peek() {
        Some('{') => {}
        Some('[') => {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "an array literal with explicit bounds is not supported",
                None,
            ))
        }
        _ => {
            return Err(malformed(
                "Array value must start with \"{\" or dimension information.",
            ))
        }
    }
    let array = parse_array(&mut chars, &malformed)?;
    if chars.any(|c| !c.is_whitespace()) {
        return Err(malformed("Junk after closing right brace."));
    }
    check_shape(&array, &malformed)?;
    Ok(array)
}

type Chars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

fn parse_array(chars: &mut Chars, malformed: &dyn Fn(&str) -> LimboError) -> Result<Node> {
    chars.next();
    let mut items = Vec::new();
    skip_whitespace(chars);
    if chars.peek() == Some(&'}') {
        chars.next();
        return Ok(Node::Array(items));
    }
    loop {
        skip_whitespace(chars);
        items.push(match chars.peek() {
            Some('{') => parse_array(chars, malformed)?,
            Some('}') | Some(',') => return Err(malformed("Unexpected array element.")),
            Some(_) => parse_element(chars, malformed)?,
            None => return Err(malformed("Unexpected end of input.")),
        });
        skip_whitespace(chars);
        match chars.next() {
            Some(',') => {}
            Some('}') => return Ok(Node::Array(items)),
            Some(_) => return Err(malformed("Unexpected array element.")),
            None => return Err(malformed("Unexpected end of input.")),
        }
    }
}

fn parse_element(chars: &mut Chars, malformed: &dyn Fn(&str) -> LimboError) -> Result<Node> {
    let mut element = String::new();
    let mut quoted = false;
    let mut escaped = false;
    if chars.peek() == Some(&'"') {
        chars.next();
        quoted = true;
        loop {
            match chars.next() {
                Some('\\') => match chars.next() {
                    Some(c) => element.push(c),
                    None => return Err(malformed("Unexpected end of input.")),
                },
                Some('"') => break,
                Some(c) => element.push(c),
                None => return Err(malformed("Unexpected end of input.")),
            }
        }
    } else {
        let mut trailing_spaces = 0;
        while let Some(&c) = chars.peek() {
            if c == ',' || c == '}' {
                break;
            }
            chars.next();
            if c == '{' || c == '"' {
                return Err(malformed("Unexpected array element."));
            }
            if c == '\\' {
                let Some(next) = chars.next() else {
                    return Err(malformed("Unexpected end of input."));
                };
                escaped = true;
                element.push(next);
                trailing_spaces = 0;
            } else {
                element.push(c);
                if c.is_whitespace() {
                    trailing_spaces += 1;
                } else {
                    trailing_spaces = 0;
                }
            }
        }
        element.truncate(element.len() - trailing_spaces);
        if element.is_empty() && !escaped {
            return Err(malformed("Unexpected array element."));
        }
    }
    if !quoted && !escaped && element.eq_ignore_ascii_case("null") {
        return Ok(Node::Null);
    }
    Ok(Node::Element(element))
}

fn skip_whitespace(chars: &mut Chars) {
    while chars.peek().is_some_and(|c| c.is_whitespace()) {
        chars.next();
    }
}

/// Every sub-array of an array has the same number of elements and the same
/// depth, and an array holds sub-arrays or elements, not both.
fn check_shape(node: &Node, malformed: &dyn Fn(&str) -> LimboError) -> Result<()> {
    let Node::Array(items) = node else {
        return Ok(());
    };
    let has_arrays = items.iter().any(|item| matches!(item, Node::Array(_)));
    if has_arrays && !items.iter().all(|item| matches!(item, Node::Array(_))) {
        return Err(malformed("Unexpected array element."));
    }
    let mut shape: Option<Vec<usize>> = None;
    for item in items {
        check_shape(item, malformed)?;
        if let Node::Array(_) = item {
            let item_shape = shape_of(item);
            match &shape {
                Some(first) if *first != item_shape => {
                    return Err(malformed(
                        "Multidimensional arrays must have sub-arrays with matching dimensions.",
                    ))
                }
                _ => shape = Some(item_shape),
            }
        }
    }
    Ok(())
}

fn shape_of(node: &Node) -> Vec<usize> {
    match node {
        Node::Array(items) => {
            let mut shape = vec![items.len()];
            if let Some(first) = items.first() {
                shape.extend(shape_of(first));
            }
            shape
        }
        _ => Vec::new(),
    }
}

fn write_array(node: &Node, element_type: CastType, out: &mut String) -> Result<()> {
    match node {
        Node::Null => out.push_str("NULL"),
        Node::Element(text) => write_element(text, element_type, out)?,
        Node::Array(items) => {
            out.push('{');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_array(item, element_type, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn write_element(text: &str, element_type: CastType, out: &mut String) -> Result<()> {
    use CastType::*;
    let input_type = match element_type {
        Int2 => Some("smallint"),
        Int4 => Some("integer"),
        Int8 => Some("bigint"),
        Float4 => Some("real"),
        Float8 => Some("double precision"),
        Bool => Some("boolean"),
        Uuid => Some("uuid"),
        _ => None,
    };
    if let Some(input_type) = input_type {
        if let Some((message, code)) = validate_pg_input(text, input_type) {
            let sqlstate = if code == SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE {
                SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE
            } else {
                SQLSTATE_INVALID_TEXT_REPRESENTATION
            };
            return Err(sqlstate_error(sqlstate, message, None));
        }
    }
    match element_type {
        Int2 | Int4 | Int8 => match text.trim().parse::<i64>() {
            Ok(number) => out.push_str(&number.to_string()),
            Err(_) => out.push_str(text.trim()),
        },
        Float4 | Float8 => out.push_str(text.trim()),
        Bool => out.push(if parse_pg_bool(text) == Some(true) {
            '1'
        } else {
            '0'
        }),
        Uuid => {
            let hex: String = text
                .trim()
                .chars()
                .filter(|c| *c != '-')
                .map(|c| c.to_ascii_lowercase())
                .collect();
            out.push_str(&format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            ));
        }
        _ => write_quoted(text, out),
    }
    Ok(())
}

fn write_quoted(text: &str, out: &mut String) {
    let needs_quotes = text.is_empty()
        || text.eq_ignore_ascii_case("null")
        || text
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '{' | '}' | ',' | '"' | '\\'));
    if !needs_quotes {
        out.push_str(text);
        return;
    }
    out.push('"');
    for c in text.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}
