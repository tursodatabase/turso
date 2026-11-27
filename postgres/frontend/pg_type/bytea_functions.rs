//! The functions of PostgreSQL's `pg_proc` that run on bytea: `byteacat` for
//! `||`, `bytealike` and `byteanlike` for `~~` and `!~~`, and the casts to
//! and from the integers.

use super::IntegerType;
use crate::error::sqlstate_error;
use crate::functions::{like_matches, pg_scalar_function, Boolean};
use turso_core::native_ext::{FunctionArity, ReturnType, ScalarReturn};
use turso_core::{OpenOptions, Register, Result, Value};
use turso_pg_parser::sqlstate::{SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE};

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

/// PostgreSQL's cast from an integer of type `from` to bytea (`int2_bytea`,
/// `int4_bytea` and `int8_bytea`): its bytes, most significant first.
pub(crate) fn int_to_bytea(value: i64, from: IntegerType) -> Value {
    let bytes = value.to_be_bytes();
    let width = usize::from(from.digits());
    Value::from_blob(bytes[bytes.len() - width..].to_vec())
}

/// PostgreSQL's cast from bytea to an integer of type `target` (`bytea_int2`,
/// `bytea_int4` and `bytea_int8`): the bytes are the integer, most significant
/// first. Fewer bytes than the type has give a number that is not negative.
/// More bytes fail.
pub(crate) fn bytea_to_int(bytes: &[u8], target: IntegerType) -> Result<i64> {
    if bytes.len() > usize::from(target.digits()) {
        return Err(sqlstate_error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("{} out of range", target.sql_name()),
            None,
        ));
    }
    let unsigned = bytes
        .iter()
        .fold(0u64, |number, byte| (number << 8) | u64::from(*byte));
    Ok(match target {
        IntegerType::Int2 => i64::from(unsigned as u16 as i16),
        IntegerType::Int4 => i64::from(unsigned as u32 as i32),
        IntegerType::Int8 => unsigned as i64,
    })
}

#[cfg(test)]
mod tests {
    use super::{bytea_to_int, byteacat, bytealike, int_to_bytea};
    use crate::pg_type::IntegerType::{Int2, Int4, Int8};
    use turso_core::Value;

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

    #[test]
    fn an_integer_casts_to_its_bytes() {
        assert_eq!(int_to_bytea(1, Int2), Value::from_blob(vec![0, 1]));
        assert_eq!(int_to_bytea(-1, Int2), Value::from_blob(vec![0xff, 0xff]));
        assert_eq!(int_to_bytea(258, Int4), Value::from_blob(vec![0, 0, 1, 2]));
        assert_eq!(
            int_to_bytea(-2, Int8),
            Value::from_blob(vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe])
        );
    }

    #[test]
    fn bytes_cast_to_an_integer_of_at_most_their_width() {
        assert_eq!(bytea_to_int(&[1, 0], Int2).unwrap(), 256);
        assert_eq!(bytea_to_int(&[0xff, 0xff], Int2).unwrap(), -1);
        assert_eq!(bytea_to_int(&[0xff, 0xff, 0xff, 0xff], Int4).unwrap(), -1);
        assert_eq!(bytea_to_int(&[], Int4).unwrap(), 0);
        assert_eq!(bytea_to_int(&[1], Int8).unwrap(), 1);
        assert_eq!(
            bytea_to_int(&[0x80, 0, 0, 0, 0, 0, 0, 0], Int8).unwrap(),
            i64::MIN
        );
    }

    #[test]
    fn bytes_wider_than_the_integer_fail() {
        for (bytes, target, name) in [
            (vec![0; 3], Int2, "smallint"),
            (vec![0; 5], Int4, "integer"),
            (vec![0; 9], Int8, "bigint"),
        ] {
            let error = bytea_to_int(&bytes, target).unwrap_err().to_string();
            assert!(error.contains(&format!("{name} out of range")), "{error}");
        }
    }
}
