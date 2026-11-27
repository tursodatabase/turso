//! The functions of text, as PostgreSQL's `varlena.c` defines them.

use crate::bytea_functions::{integer_arg, substring_range};
use turso_core::{Register, Result, Value};

/// `substr(text, start, length)` and `substr(text, start)`: the characters from
/// `start`, counting from 1, where a start before the first character uses up
/// part of the length.
pub(crate) fn text_substring(args: &[Register], with_length: bool) -> Result<Option<String>> {
    let Value::Text(text) = args[0].get_value() else {
        return Ok(None);
    };
    let Some(start) = integer_arg(args, 1)? else {
        return Ok(None);
    };
    let length = if with_length {
        match integer_arg(args, 2)? {
            Some(length) => Some(length),
            None => return Ok(None),
        }
    } else {
        None
    };
    let characters: Vec<char> = text.as_str().chars().collect();
    let Some((from, to)) = substring_range(characters.len(), start, length)? else {
        return Ok(Some(String::new()));
    };
    Ok(Some(characters[from..to].iter().collect()))
}
