//! The types of SQLite.
//!
//! SQLite has no types, only affinities: the name a column is declared with,
//! or a CAST names, decides how values are converted. [`SqliteTypes`] gives
//! the parser a type for each name it reads, and core asks that type what
//! to do.

use std::sync::{Arc, LazyLock};
use turso_core::ast::{TypeHandle, TypePlace, TypeResolver};
use turso_core::dialect::types::{type_handle, CastContext, DialectType};
use turso_core::{Affinity, Result, Value};

/// Gives the SQLite parser the type of each type name it reads.
pub struct SqliteTypes;

impl TypeResolver for SqliteTypes {
    fn resolve(&self, type_name: &str, place: TypePlace) -> TypeHandle {
        handle_of(affinity_of_type_name(type_name, place))
    }
}

/// A type that is nothing but an affinity.
#[derive(Debug)]
struct SqliteType {
    affinity: Affinity,
}

impl DialectType for SqliteType {
    fn cast(
        &self,
        value: &Value,
        _from: Option<&dyn DialectType>,
        _context: CastContext,
    ) -> Result<Value> {
        Ok(value.exec_cast(self.affinity)?)
    }

    fn comparison_affinity(&self) -> Affinity {
        self.affinity
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl std::fmt::Display for SqliteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.affinity)
    }
}

/// The one handle of each affinity, so that reading a type allocates nothing.
fn handle_of(affinity: Affinity) -> TypeHandle {
    static HANDLES: LazyLock<[TypeHandle; 6]> = LazyLock::new(|| {
        [
            Affinity::Blob,
            Affinity::Text,
            Affinity::Numeric,
            Affinity::Integer,
            Affinity::Real,
            Affinity::None,
        ]
        .map(|affinity| type_handle(Arc::new(SqliteType { affinity })))
    });
    HANDLES[affinity as usize - 1].clone()
}

fn affinity_of_type_name(type_name: &str, place: TypePlace) -> Affinity {
    let declared_type = match place {
        TypePlace::Column => column_declared_type(type_name),
        TypePlace::Cast => dequote(type_name),
    };
    // A type name that is empty once its quotes are removed, such as `""`,
    // has NUMERIC affinity. A column declared with no type has none to parse.
    if declared_type.is_empty() {
        Affinity::Numeric
    } else {
        affinity_of_declared_type(&declared_type)
    }
}

/// 3.1. Determination Of Column Affinity
/// For tables not declared as STRICT, the affinity of a column is determined by the declared type of the column, according to the following rules in the order shown:
///
/// If the declared type contains the string "INT" then it is assigned INTEGER affinity.
///
/// If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
///
/// If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
///
/// If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
///
/// Otherwise, the affinity is NUMERIC.
///
/// Note that the order of the rules for determining column affinity is important. A column whose declared type is "CHARINT" will match both rules 1 and 2 but the first rule takes precedence and so the column affinity will be INTEGER.
fn affinity_of_declared_type(datatype: &str) -> Affinity {
    const INT: u32 = u32::from_be_bytes([0, b'I', b'N', b'T']);
    const CHAR: u32 = u32::from_be_bytes(*b"CHAR");
    const CLOB: u32 = u32::from_be_bytes(*b"CLOB");
    const TEXT: u32 = u32::from_be_bytes(*b"TEXT");
    const BLOB: u32 = u32::from_be_bytes(*b"BLOB");
    const REAL: u32 = u32::from_be_bytes(*b"REAL");
    const FLOA: u32 = u32::from_be_bytes(*b"FLOA");
    const DOUB: u32 = u32::from_be_bytes(*b"DOUB");

    // One pass finds all the keywords of the rules, as SQLite does: `window`
    // holds the last four bytes in upper case.
    let mut window: u32 = 0;
    let (mut has_text_keyword, mut has_blob, mut has_real_keyword) = (false, false, false);
    for &byte in datatype.as_bytes() {
        window = (window << 8) | u32::from(byte.to_ascii_uppercase());
        // Rule 1: INT -> INTEGER affinity
        if window & 0x00FF_FFFF == INT {
            return Affinity::Integer;
        }
        match window {
            CHAR | CLOB | TEXT => has_text_keyword = true,
            BLOB => has_blob = true,
            REAL | FLOA | DOUB => has_real_keyword = true,
            _ => {}
        }
    }

    // Rule 2: CHAR/CLOB/TEXT -> TEXT affinity
    if has_text_keyword {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB affinity (historically called NONE)
    if has_blob || datatype.is_empty() {
        return Affinity::Blob;
    }

    // Rule 4: REAL/FLOA/DOUB -> REAL affinity
    if has_real_keyword {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC affinity
    Affinity::Numeric
}

/// Returns a column's declared type from its type name the way SQLite's
/// `sqlite3AddColumn` does: a name that starts with a quote and has no other
/// quote characters before its last character loses its first and last
/// characters, so `[x] INT` becomes `x] IN`. Other names go through [dequote].
fn column_declared_type(type_name: &str) -> String {
    match strip_type_name_quotes(type_name) {
        Some(stripped) => stripped.to_string(),
        None => dequote(type_name),
    }
}

fn strip_type_name_quotes(type_name: &str) -> Option<&str> {
    let is_quote = |b: &u8| matches!(b, b'"' | b'\'' | b'[' | b'`');
    let bytes = type_name.as_bytes();
    if bytes.len() >= 3 && is_quote(&bytes[0]) && !bytes[1..bytes.len() - 1].iter().any(is_quote) {
        let mut chars = type_name.chars();
        chars.next();
        chars.next_back();
        return Some(chars.as_str());
    }
    None
}

/// Removes the quotes around the start of `source` the way SQLite's
/// `sqlite3Dequote` does: when it starts with a quote character, only the
/// first quoted token survives, so `"x" < +5` becomes just `x`.
fn dequote(source: &str) -> String {
    let bytes = source.as_bytes();
    let close = match bytes.first() {
        Some(&c @ (b'"' | b'\'' | b'`')) => c,
        Some(b'[') => b']',
        _ => return source.to_string(),
    };
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == close {
            // A doubled quote is an escaped quote, not the end.
            if close != b']' && bytes.get(i + 1) == Some(&close) {
                i += 2;
                continue;
            }
            break;
        }
        i += 1;
    }
    let inner = &source[1..i.min(source.len())];
    if close == b']' {
        inner.to_string()
    } else {
        let quote = (close as char).to_string();
        inner.replace(&format!("{quote}{quote}"), &quote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn affinity(type_name: &str, place: TypePlace) -> Affinity {
        affinity_of_type_name(type_name, place)
    }

    #[test]
    fn a_keyword_in_the_name_decides_the_affinity() {
        for place in [TypePlace::Column, TypePlace::Cast] {
            assert_eq!(affinity("BIGINT", place), Affinity::Integer);
            assert_eq!(affinity("CHARINT", place), Affinity::Integer);
            assert_eq!(affinity("VARCHAR", place), Affinity::Text);
            assert_eq!(affinity("blob", place), Affinity::Blob);
            assert_eq!(affinity("double precision", place), Affinity::Real);
            assert_eq!(affinity("DECIMAL", place), Affinity::Numeric);
        }
    }

    #[test]
    fn a_name_that_is_empty_without_its_quotes_is_numeric() {
        assert_eq!(affinity("\"\"", TypePlace::Column), Affinity::Numeric);
        assert_eq!(affinity("\"\"", TypePlace::Cast), Affinity::Numeric);
    }

    #[test]
    fn a_column_and_a_cast_remove_quotes_differently() {
        // A column keeps what is between the first and last character, and a
        // CAST keeps the first quoted word.
        assert_eq!(affinity("[x] INT", TypePlace::Column), Affinity::Numeric);
        assert_eq!(affinity("[x] INTS", TypePlace::Column), Affinity::Integer);
        assert_eq!(affinity("[x] INTS", TypePlace::Cast), Affinity::Numeric);
    }
}
