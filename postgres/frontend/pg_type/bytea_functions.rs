//! The functions of PostgreSQL's `pg_proc` that run on bytea: `byteacat` for
//! `||`, and `bytealike` and `byteanlike` for `~~` and `!~~`.

use crate::error::sqlstate_error;
use crate::functions::{like_matches, pg_scalar_function, Boolean};
use turso_core::native_ext::{FunctionArity, ReturnType, ScalarReturn};
use turso_core::{OpenOptions, Register, Result, Value};
use turso_pg_parser::sqlstate::SQLSTATE_DATATYPE_MISMATCH;

/// A bytea value. Returning one gives the function the type `bytea`.
pub(crate) struct Bytea(pub(crate) Vec<u8>);

impl ScalarReturn for Bytea {
    const TYPE: Option<ReturnType> = Some(ReturnType::Custom("bytea"));

    fn into_value(self) -> Value {
        Value::from_blob(self.0)
    }
}

pub(crate) fn register_bytea_functions(options: OpenOptions) -> OpenOptions {
    options
        .extension_function(pg_scalar_function(
            "byteacat",
            FunctionArity::Exact(2),
            true,
            |_, args| Ok(byteas(args)?.map(|(left, right)| Bytea(byteacat(left, right)))),
        ))
        .extension_function(pg_scalar_function(
            "bytealike",
            FunctionArity::Exact(2),
            true,
            |_, args| {
                byteas(args)?
                    .map(|(text, pattern)| Ok(Boolean(bytealike(text, pattern)?)))
                    .transpose()
            },
        ))
        .extension_function(pg_scalar_function(
            "byteanlike",
            FunctionArity::Exact(2),
            true,
            |_, args| {
                byteas(args)?
                    .map(|(text, pattern)| Ok(Boolean(!bytealike(text, pattern)?)))
                    .transpose()
            },
        ))
}

/// The two bytea arguments of a function, or None when one is NULL.
fn byteas(args: &[Register]) -> Result<Option<(&[u8], &[u8])>> {
    match (args[0].get_value(), args[1].get_value()) {
        (Value::Null, _) | (_, Value::Null) => Ok(None),
        (Value::Blob(left), Value::Blob(right)) => Ok(Some((left, right))),
        (Value::Blob(_), other) | (other, _) => Err(sqlstate_error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!("the value {other} is not of type bytea"),
            None,
        )),
    }
}

/// PostgreSQL's `byteacat`: the bytes of `left` followed by those of `right`.
pub(crate) fn byteacat(left: &[u8], right: &[u8]) -> Vec<u8> {
    [left, right].concat()
}

/// PostgreSQL's `bytealike`: whether `text` matches the LIKE `pattern`, byte
/// by byte. `%` matches any bytes, `_` one byte and a backslash makes the
/// next byte literal.
pub(crate) fn bytealike(text: &[u8], pattern: &[u8]) -> Result<bool> {
    like_matches(text, pattern, |_, _| 1)
}

#[cfg(test)]
mod tests {
    use super::{byteacat, bytealike};

    fn like(text: &[u8], pattern: &[u8]) -> bool {
        bytealike(text, pattern).unwrap()
    }

    #[test]
    fn concatenation_joins_the_bytes() {
        assert_eq!(byteacat(b"\xff", b"\x00"), vec![0xff, 0x00]);
        assert_eq!(byteacat(b"", b""), Vec::<u8>::new());
    }

    #[test]
    fn like_matches_whole_values_byte_by_byte() {
        assert!(like(b"abc", b"abc"));
        assert!(!like(b"abc", b"ab"));
        assert!(!like(b"abc", b"ABC"));
        assert!(like(b"", b""));
        assert!(like(b"\xff\x01", b"\xff\x01"));
    }

    #[test]
    fn like_percent_matches_any_bytes() {
        assert!(like(b"abc", b"%"));
        assert!(like(b"", b"%"));
        assert!(like(b"abc", b"a%"));
        assert!(like(b"abc", b"%c"));
        assert!(like(b"abc", b"a%c"));
        assert!(like(b"ac", b"a%c"));
        assert!(!like(b"abd", b"a%c"));
        assert!(like(b"abcabc", b"%bc%bc"));
        assert!(like(b"abc", b"%%%"));
        assert!(like(b"\xff\x00\x01", b"%\x01"));
    }

    #[test]
    fn like_underscore_matches_one_byte() {
        assert!(like(b"abc", b"a_c"));
        assert!(!like(b"ac", b"a_c"));
        assert!(like(b"\xff", b"_"));
        assert!(!like(b"", b"_"));
        assert!(like(b"abc", b"%_"));
        assert!(!like(b"", b"%_"));
        assert!(like(b"abc", b"_%_"));
        assert!(!like(b"a", b"_%_"));
    }

    #[test]
    fn like_backslash_makes_the_next_byte_literal() {
        assert!(like(b"a%c", b"a\\%c"));
        assert!(!like(b"abc", b"a\\%c"));
        assert!(like(b"a_c", b"a\\_c"));
        assert!(like(b"a\\c", b"a\\\\c"));
        assert!(like(b"a%", b"%\\%"));
        assert!(!like(b"ab", b"%\\%"));
    }

    #[test]
    fn like_refuses_a_pattern_that_ends_with_a_backslash_when_it_reads_it() {
        for (text, pattern) in [(&b"ab"[..], &b"a\\"[..]), (b"ab", b"%\\"), (b"a", b"\\")] {
            let error = bytealike(text, pattern).unwrap_err().to_string();
            assert!(error.contains("must not end with escape"), "{error}");
        }
        assert!(!like(b"a", b"a\\"));
        assert!(!like(b"ab", b"ab\\"));
    }
}
