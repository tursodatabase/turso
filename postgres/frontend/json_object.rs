//! json_object() and jsonb_object(): a JSON object built from text arrays,
//! either one array of alternating keys and values or one array of keys and
//! one array of values.

use crate::error::sqlstate_error;
use crate::functions::{pg_type_name, undefined_function};
use turso_core::{array_values_from_any, LimboError, Register, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_ARRAY_SUBSCRIPT_ERROR, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NULL_VALUE_NOT_ALLOWED, SQLSTATE_PROGRAM_LIMIT_EXCEEDED,
};

/// A key of the object and its value. A `None` value is JSON null.
pub(crate) type Field = (String, Option<String>);

/// The fields of the object, in the order of the arrays. Returns `None` when
/// an argument is NULL.
pub(crate) fn object_fields(function: &str, args: &[Register]) -> Result<Option<Vec<Field>>> {
    let arrays = args
        .iter()
        .map(|arg| array_arg(arg.get_value()))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| not_text_arrays(function, args))?
        .into_iter()
        .map(|arg| match arg {
            ArrayArg::Null => Ok(None),
            ArrayArg::Literal(text) => parse_array_literal(text).map(Some),
            ArrayArg::Array(array) => Ok(Some(array)),
        })
        .collect::<Result<Vec<_>>>()?;
    let Some(arrays) = arrays.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(None);
    };
    let pairs = match arrays.as_slice() {
        [array] => pairs_of_one_array(array)?,
        [keys, values] => pairs_of_two_arrays(keys, values)?,
        _ => unreachable!("{function}() is registered with one or two arguments"),
    };
    pairs
        .into_iter()
        .map(|(key, value)| {
            let key = key.ok_or_else(|| {
                sqlstate_error(
                    SQLSTATE_NULL_VALUE_NOT_ALLOWED,
                    "null value not allowed for object key",
                )
            })?;
            Ok((key, value))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

/// The text of a json value, with the spacing of PostgreSQL's json_object():
/// `{"a" : "1", "b" : null}`. Fields keep their order and repeated keys.
pub(crate) fn json_text(fields: &[Field]) -> String {
    object_text(fields.iter(), " : ")
}

/// The text of a jsonb value, as PostgreSQL prints it: keys sorted by length
/// and then by their bytes, and the last value of a repeated key.
pub(crate) fn jsonb_text(mut fields: Vec<Field>) -> String {
    fields.reverse();
    // A stable sort keeps the reversed order among equal keys, so dedup keeps
    // the last value the arrays gave.
    fields.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
    fields.dedup_by(|(a, _), (b, _)| a == b);
    object_text(fields.iter(), ": ")
}

fn object_text<'a>(fields: impl Iterator<Item = &'a Field>, separator: &str) -> String {
    let mut text = String::from("{");
    for (i, (key, value)) in fields.enumerate() {
        if i > 0 {
            text.push_str(", ");
        }
        push_json_string(&mut text, key);
        text.push_str(separator);
        match value {
            Some(value) => push_json_string(&mut text, value),
            None => text.push_str("null"),
        }
    }
    text.push('}');
    text
}

/// Writes `value` as a JSON string with PostgreSQL's escapes (escape_json).
fn push_json_string(text: &mut String, value: &str) {
    text.push('"');
    for c in value.chars() {
        match c {
            '\u{8}' => text.push_str("\\b"),
            '\u{c}' => text.push_str("\\f"),
            '\n' => text.push_str("\\n"),
            '\r' => text.push_str("\\r"),
            '\t' => text.push_str("\\t"),
            '"' => text.push_str("\\\""),
            '\\' => text.push_str("\\\\"),
            c if c < ' ' => text.push_str(&format!("\\u{:04x}", c as u32)),
            c => text.push(c),
        }
    }
    text.push('"');
}

/// The pairs of `{k1, v1, k2, v2}` or `{{k1, v1}, {k2, v2}}`.
fn pairs_of_one_array(array: &TextArray) -> Result<Vec<(Option<String>, Option<String>)>> {
    match array.dims.as_slice() {
        [] => return Ok(Vec::new()),
        [count] if count % 2 != 0 => {
            return Err(array_subscript_error(
                "array must have even number of elements",
            ))
        }
        [_] => {}
        [_, 2] => {}
        [_, _] => return Err(array_subscript_error("array must have two columns")),
        _ => return Err(array_subscript_error("wrong number of array subscripts")),
    }
    let mut elements = array.elements.iter().cloned();
    let mut pairs = Vec::with_capacity(array.elements.len() / 2);
    while let (Some(key), Some(value)) = (elements.next(), elements.next()) {
        pairs.push((key, value));
    }
    Ok(pairs)
}

fn pairs_of_two_arrays(
    keys: &TextArray,
    values: &TextArray,
) -> Result<Vec<(Option<String>, Option<String>)>> {
    if keys.dims.len() > 1 || keys.dims.len() != values.dims.len() {
        return Err(array_subscript_error("wrong number of array subscripts"));
    }
    if keys.elements.len() != values.elements.len() {
        return Err(array_subscript_error("mismatched array dimensions"));
    }
    Ok(keys
        .elements
        .iter()
        .cloned()
        .zip(values.elements.iter().cloned())
        .collect())
}

fn array_subscript_error(message: &str) -> LimboError {
    sqlstate_error(SQLSTATE_ARRAY_SUBSCRIPT_ERROR, message)
}

/// A text array: the length of each dimension, and the elements in row-major
/// order. An empty array has no dimensions.
struct TextArray {
    dims: Vec<usize>,
    elements: Vec<Option<String>>,
}

/// An argument that may be a text array. The literal is read after every
/// argument's type is checked, so a wrong type is reported before a malformed
/// literal, as in PostgreSQL.
enum ArrayArg<'a> {
    Null,
    /// A string such as `'{a,b}'`.
    Literal(&'a str),
    /// An array value such as `ARRAY['a', 'b']`.
    Array(TextArray),
}

fn array_arg(value: &Value) -> Option<ArrayArg<'_>> {
    match value {
        Value::Null => Some(ArrayArg::Null),
        Value::Text(text) => Some(ArrayArg::Literal(text.as_str())),
        Value::Blob(_) => array_of_values(value).map(ArrayArg::Array),
        _ => None,
    }
}

fn not_text_arrays(function: &str, args: &[Register]) -> LimboError {
    let types = args
        .iter()
        .map(|arg| array_type_name(arg.get_value()))
        .collect::<Vec<_>>();
    undefined_function(
        function,
        &types.iter().map(String::as_str).collect::<Vec<_>>(),
    )
}

/// The text array of an array value, whose elements are texts, NULLs, or
/// sub-arrays of the same length. None if the value is not such an array.
fn array_of_values(value: &Value) -> Option<TextArray> {
    let items = array_values_from_any(value)?;
    let sub_arrays = items
        .iter()
        .map(|item| match item {
            Value::Blob(_) => array_of_values(item),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    match sub_arrays {
        Some(sub_arrays) if !sub_arrays.is_empty() => {
            let inner_dims = &sub_arrays[0].dims;
            if inner_dims.is_empty() || sub_arrays.iter().any(|sub| &sub.dims != inner_dims) {
                return None;
            }
            let mut dims = vec![sub_arrays.len()];
            dims.extend(inner_dims);
            let elements = sub_arrays
                .into_iter()
                .flat_map(|sub| sub.elements)
                .collect();
            Some(TextArray { dims, elements })
        }
        _ => {
            let elements = items
                .into_iter()
                .map(|item| match item {
                    Value::Null => Some(None),
                    Value::Text(text) => Some(Some(text.as_str().to_string())),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let dims = if elements.is_empty() {
                Vec::new()
            } else {
                vec![elements.len()]
            };
            Some(TextArray { dims, elements })
        }
    }
}

/// The PostgreSQL type of an argument, for the error about a missing function.
fn array_type_name(value: &Value) -> String {
    let element = array_values_from_any(value)
        .and_then(|items| items.into_iter().find(|item| !matches!(item, Value::Null)));
    match (value, element) {
        (Value::Blob(_), Some(element)) => format!("{}[]", pg_type_name(&element)),
        _ => pg_type_name(value).to_string(),
    }
}

/// Reads the text form of an array, as PostgreSQL's array_in does: an
/// optional dimension decoration such as `[1:2]=`, then nested braces with
/// elements that may be double-quoted, backslash-escaped, or the unquoted
/// word NULL.
fn parse_array_literal(input: &str) -> Result<TextArray> {
    let mut parser = ArrayLiteralParser {
        input,
        chars: input.chars().peekable(),
    };
    parser.parse()
}

struct ArrayLiteralParser<'a> {
    input: &'a str,
    chars: std::iter::Peekable<std::str::Chars<'a>>,
}

/// The most dimensions an array can have in PostgreSQL (MAXDIM).
const MAX_DIMENSIONS: usize = 6;

/// An element of an array literal, or a list of items in braces.
enum Item {
    Element(Option<String>),
    List(Vec<Item>),
}

impl ArrayLiteralParser<'_> {
    fn parse(&mut self) -> Result<TextArray> {
        self.skip_whitespace();
        let declared_dims = if self.peek() == Some('[') {
            Some(self.parse_dimension_decoration()?)
        } else {
            None
        };
        if self.next() != Some('{') {
            return Err(self.malformed());
        }
        let top = self.parse_list(1)?;
        self.skip_whitespace();
        if self.peek().is_some() {
            return Err(self.malformed());
        }
        let mut dims = Vec::new();
        let mut elements = Vec::new();
        // Braces with no element at all, such as `{{},{}}`, are an empty array.
        if has_elements(&top) {
            flatten(&top, 0, &mut dims, &mut elements).ok_or_else(|| self.malformed())?;
        }
        if let Some(declared_dims) = declared_dims {
            if declared_dims != dims {
                return Err(self.malformed());
            }
        }
        Ok(TextArray { dims, elements })
    }

    /// Reads `[lower:upper]` or `[upper]` groups and the `=` after them, and
    /// returns the length of each dimension.
    fn parse_dimension_decoration(&mut self) -> Result<Vec<usize>> {
        let mut dims = Vec::new();
        while self.peek() == Some('[') {
            if dims.len() == MAX_DIMENSIONS {
                return Err(too_many_dimensions());
            }
            self.next();
            let first = self.parse_bound()?;
            let (lower, upper) = if self.peek() == Some(':') {
                self.next();
                (first, self.parse_bound()?)
            } else {
                (1, first)
            };
            if self.next() != Some(']') {
                return Err(self.malformed());
            }
            if upper < lower {
                return Err(array_subscript_error(
                    "upper bound cannot be less than lower bound",
                ));
            }
            if upper == i64::from(i32::MAX) {
                return Err(sqlstate_error(
                    SQLSTATE_PROGRAM_LIMIT_EXCEEDED,
                    format!("array upper bound is too large: {upper}"),
                ));
            }
            dims.push(usize::try_from(upper - lower + 1).expect("upper >= lower"));
            self.skip_whitespace();
        }
        if self.next() != Some('=') {
            return Err(self.malformed());
        }
        self.skip_whitespace();
        Ok(dims)
    }

    /// Reads a bound, which PostgreSQL keeps in a 32-bit integer.
    fn parse_bound(&mut self) -> Result<i64> {
        let mut digits = String::new();
        if let Some(sign @ ('+' | '-')) = self.peek() {
            digits.push(sign);
            self.next();
        }
        while let Some(c) = self.peek().filter(char::is_ascii_digit) {
            digits.push(c);
            self.next();
        }
        if digits.is_empty() || digits == "+" || digits == "-" {
            return Err(self.malformed());
        }
        digits.parse::<i32>().map(i64::from).map_err(|_| {
            sqlstate_error(
                SQLSTATE_PROGRAM_LIMIT_EXCEEDED,
                "array bound is out of integer range",
            )
        })
    }

    /// Reads the items of a list, after its `{`, up to and including its `}`.
    /// `depth` is the number of braces open, this list's included.
    fn parse_list(&mut self, depth: usize) -> Result<Item> {
        if depth > MAX_DIMENSIONS {
            return Err(too_many_dimensions());
        }
        let mut items = Vec::new();
        loop {
            self.skip_whitespace();
            match self.peek() {
                Some('}') if items.is_empty() => {
                    self.next();
                    return Ok(Item::List(items));
                }
                Some('{') => {
                    self.next();
                    items.push(self.parse_list(depth + 1)?);
                }
                Some('"') => {
                    self.next();
                    items.push(Item::Element(Some(self.parse_quoted()?)));
                }
                Some(_) => items.push(Item::Element(self.parse_unquoted()?)),
                None => return Err(self.malformed()),
            }
            self.skip_whitespace();
            match self.next() {
                Some(',') => {}
                Some('}') => return Ok(Item::List(items)),
                _ => return Err(self.malformed()),
            }
        }
    }

    fn parse_quoted(&mut self) -> Result<String> {
        let mut element = String::new();
        loop {
            match self.next() {
                Some('"') => return Ok(element),
                Some('\\') => element.push(self.next().ok_or_else(|| self.malformed())?),
                Some(c) => element.push(c),
                None => return Err(self.malformed()),
            }
        }
    }

    /// Reads an unquoted element. Whitespace around it is not part of it,
    /// unless escaped, and the word NULL without escapes is a NULL element.
    fn parse_unquoted(&mut self) -> Result<Option<String>> {
        let mut element = String::new();
        let mut escaped = false;
        // The length of the element up to its last escaped or non-space character.
        let mut kept_len = 0;
        while let Some(c) = self.peek() {
            match c {
                ',' | '}' => break,
                '{' | '"' => return Err(self.malformed()),
                '\\' => {
                    self.next();
                    element.push(self.next().ok_or_else(|| self.malformed())?);
                    escaped = true;
                    kept_len = element.len();
                }
                c => {
                    self.next();
                    element.push(c);
                    if !is_array_whitespace(c) {
                        kept_len = element.len();
                    }
                }
            }
        }
        element.truncate(kept_len);
        if element.is_empty() && !escaped {
            return Err(self.malformed());
        }
        if !escaped && element.eq_ignore_ascii_case("NULL") {
            return Ok(None);
        }
        Ok(Some(element))
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(is_array_whitespace) {
            self.next();
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.chars.peek().copied()
    }

    fn next(&mut self) -> Option<char> {
        self.chars.next()
    }

    fn malformed(&self) -> LimboError {
        sqlstate_error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            format!("malformed array literal: \"{}\"", self.input),
        )
    }
}

fn too_many_dimensions() -> LimboError {
    sqlstate_error(
        SQLSTATE_PROGRAM_LIMIT_EXCEEDED,
        format!("number of array dimensions exceeds the maximum allowed ({MAX_DIMENSIONS})"),
    )
}

/// The whitespace that array_in skips (array_isspace).
fn is_array_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{b}' | '\u{c}')
}

fn has_elements(item: &Item) -> bool {
    match item {
        Item::Element(_) => true,
        Item::List(items) => items.iter().any(has_elements),
    }
}

/// Collects the elements of `item` and checks that the levels at each depth
/// have the same length and that all elements are at the same depth. None if
/// they are not.
fn flatten(
    item: &Item,
    depth: usize,
    dims: &mut Vec<usize>,
    elements: &mut Vec<Option<String>>,
) -> Option<()> {
    let Item::List(items) = item else {
        unreachable!("flatten is called on lists only");
    };
    if items.is_empty() {
        return None;
    }
    match dims.get(depth) {
        Some(&len) if len != items.len() => return None,
        Some(_) => {}
        None => dims.push(items.len()),
    }
    let mut level_elements = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Item::Element(element) => level_elements.push(element.clone()),
            Item::List(_) => {
                if !level_elements.is_empty() || (dims.len() == depth + 1 && !elements.is_empty()) {
                    return None;
                }
                flatten(item, depth + 1, dims, elements)?;
            }
        }
    }
    if level_elements.is_empty() {
        return Some(());
    }
    if level_elements.len() != items.len() || dims.len() != depth + 1 {
        return None;
    }
    elements.extend(level_elements);
    Some(())
}
