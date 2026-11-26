//! The columns of a catalog as Rust: the type of a column, and the Rust
//! expression for a value of the file in that column.

use std::fmt::Display;

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use crate::bki::{Catalog, Nullness, Token};
use crate::text_out::{float4_as_f64, is_array_space, push_array, push_vector, Float4};
use crate::SqlValue;

pub(crate) struct Field<'a> {
    pub(crate) name: &'a str,
    /// The type name as the file spells it.
    pub(crate) bki_type: &'a str,
    pub(crate) kind: ColumnKind,
    /// Whether a row can have NULL in the column.
    pub(crate) nullable: bool,
}

impl<'a> Field<'a> {
    /// The fields of the struct of `catalog`, one for each column.
    ///
    /// A column is NOT NULL as PostgreSQL decides it when it reads the
    /// file: a column list can force it. If it does not, the column is
    /// NOT NULL when its type has a fixed width and each column before it
    /// is NOT NULL and has a fixed width. Those are the columns that
    /// PostgreSQL reads through a C struct.
    pub(crate) fn of_catalog(catalog: &'a Catalog) -> Result<Vec<Self>, String> {
        let mut fields = Vec::new();
        let mut only_fixed_not_null_so_far = true;
        for column in &catalog.columns {
            let kind = ColumnKind::of(&column.bki_type).ok_or_else(|| {
                format!(
                    "column {} has type {}, which the generator does not know",
                    column.name, column.bki_type
                )
            })?;
            let fixed_width = has_fixed_width(&column.bki_type);
            let nullable = match column.nullness {
                Nullness::ForceNotNull => false,
                Nullness::ForceNull => true,
                Nullness::Auto => !(fixed_width && only_fixed_not_null_so_far),
            };
            only_fixed_not_null_so_far &= fixed_width && !nullable;
            fields.push(Field {
                name: &column.name,
                bki_type: &column.bki_type,
                kind,
                nullable,
            });
        }
        Ok(fields)
    }

    pub(crate) fn ident(&self) -> Ident {
        Ident::new(self.name, Span::call_site())
    }

    pub(crate) fn rust_type(&self) -> TokenStream {
        let rust_type = self.kind.rust_type();
        if self.nullable {
            quote! { Option<#rust_type> }
        } else {
            rust_type
        }
    }

    /// The Rust expression for `token` in this column.
    pub(crate) fn literal(&self, token: &Token) -> Result<TokenStream, String> {
        Ok(match self.read(token)? {
            None => quote! { None },
            Some(value) if self.nullable => {
                let value = value.literal();
                quote! { Some(#value) }
            }
            Some(value) => value.literal(),
        })
    }

    /// The SQL value of `token` in this column, as the built-in rows of the
    /// catalog write it.
    pub(crate) fn sql_value(&self, token: &Token) -> Result<SqlValue, String> {
        Ok(match self.read(token)? {
            None => SqlValue::Null,
            Some(value) => value.sql_value(self.is_vector()),
        })
    }

    /// Whether the column is an `oidvector` or an `int2vector`, whose text
    /// separates the elements with spaces, and not an array.
    fn is_vector(&self) -> bool {
        matches!(self.bki_type, "oidvector" | "int2vector")
    }

    /// The value of `token` in this column, or None for NULL.
    fn read(&self, token: &Token) -> Result<Option<ColumnValue>, String> {
        if token.is_null() {
            return if self.nullable {
                Ok(None)
            } else {
                Err("the column is NOT NULL and the row has NULL in it".to_string())
            };
        }
        self.kind.read(token).map(Some)
    }

    /// The Rust expression that gives the value of this column as a
    /// `turso_core::Value`, for a row in `self`. It calls `value` of the
    /// module `builtin_rows` of `turso_pg_catalog`.
    pub(crate) fn value_expression(&self) -> String {
        let name = self.name;
        let wrapper = match self.kind {
            _ if self.is_vector() => Some("Vector"),
            ColumnKind::Oids
            | ColumnKind::Int2s
            | ColumnKind::Floats
            | ColumnKind::Chars
            | ColumnKind::Texts => Some("Array"),
            _ => None,
        };
        match (wrapper, self.nullable) {
            (None, _) => format!("value(self.{name})"),
            (Some(wrapper), false) => format!("value({wrapper}(self.{name}))"),
            (Some(wrapper), true) => format!("value(self.{name}.map({wrapper}))"),
        }
    }
}

/// What a column holds, which decides its Rust type.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ColumnKind {
    /// An oid, or the oid of a function in a `regproc` column.
    Oid,
    /// `xid`, the number of a transaction.
    Xid,
    Int2,
    Int4,
    Int8,
    Float4,
    Bool,
    /// PostgreSQL's one-byte `"char"`.
    Char,
    /// `name` and `text`, and the expression trees of `pg_node_tree`, which
    /// are kept as the text of the file.
    Text,
    /// A type whose values the generator does not read, such as
    /// `timestamptz` or `bytea`. No built-in row has a value of these
    /// types, and a value stops the generation.
    Unread,
    /// `oidvector`, and `_oid`, the array of oids.
    Oids,
    /// `int2vector`, and `_int2`.
    Int2s,
    /// `_float4`.
    Floats,
    /// `_char`.
    Chars,
    /// `_text`, and `_aclitem`, which is kept as text.
    Texts,
}

impl ColumnKind {
    /// Whether a value of the kind is an integer of SQLite.
    pub(crate) fn is_integer(self) -> bool {
        matches!(
            self,
            Self::Oid | Self::Xid | Self::Int2 | Self::Int4 | Self::Int8
        )
    }

    pub(crate) fn of(bki_type: &str) -> Option<Self> {
        Some(match bki_type {
            "oid" | "regproc" => Self::Oid,
            "xid" => Self::Xid,
            "int2" => Self::Int2,
            "int4" => Self::Int4,
            "int8" => Self::Int8,
            "float4" => Self::Float4,
            "bool" => Self::Bool,
            "char" => Self::Char,
            "name" | "text" | "pg_node_tree" => Self::Text,
            "timestamptz" | "pg_lsn" | "bytea" | "anyarray" | "_pg_statistic" | "pg_ndistinct"
            | "pg_dependencies" | "pg_mcv_list" => Self::Unread,
            "oidvector" | "_oid" => Self::Oids,
            "int2vector" | "_int2" => Self::Int2s,
            "_float4" => Self::Floats,
            "_char" => Self::Chars,
            "_text" | "_aclitem" => Self::Texts,
            _ => return None,
        })
    }

    fn rust_type(self) -> TokenStream {
        match self {
            Self::Oid => quote! { Oid },
            Self::Xid => quote! { u32 },
            Self::Int2 => quote! { i16 },
            Self::Int4 => quote! { i32 },
            Self::Int8 => quote! { i64 },
            Self::Float4 => quote! { f32 },
            Self::Bool => quote! { bool },
            Self::Char => quote! { char },
            Self::Text | Self::Unread => quote! { &'static str },
            Self::Oids => quote! { &'static [Oid] },
            Self::Int2s => quote! { &'static [i16] },
            Self::Floats => quote! { &'static [f32] },
            Self::Chars => quote! { &'static [char] },
            Self::Texts => quote! { &'static [&'static str] },
        }
    }

    /// The Rust expression for `token`, which is not NULL.
    #[cfg(test)]
    fn literal(self, token: &Token) -> Result<TokenStream, String> {
        Ok(self.read(token)?.literal())
    }

    /// The value of `token`, which is not NULL, in a column of this kind.
    fn read(self, token: &Token) -> Result<ColumnValue, String> {
        let text = substitute_symbol(token);
        Ok(match self {
            Self::Oid => ColumnValue::Oid(oid_value(token)?),
            Self::Xid => ColumnValue::Xid(parse(text, "an xid")?),
            Self::Int2 => ColumnValue::Int2(parse(text, "an int2")?),
            Self::Int4 => ColumnValue::Int4(parse(text, "an int4")?),
            Self::Int8 => ColumnValue::Int8(parse(text, "an int8")?),
            Self::Float4 => ColumnValue::Float4(finite_float(text)?),
            Self::Bool => match text {
                "t" => ColumnValue::Bool(true),
                "f" => ColumnValue::Bool(false),
                _ => return Err(format!("\"{text}\" is not a bool")),
            },
            Self::Char => ColumnValue::Char(one_char(text)?),
            Self::Text => ColumnValue::Text(text.to_string()),
            Self::Unread => {
                return Err(format!(
                    "the generator does not read values of this type, such as \"{text}\""
                ))
            }
            Self::Oids => ColumnValue::Oids(
                elements(token)?
                    .iter()
                    .map(|element| parse(element, "an oid"))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Int2s => ColumnValue::Int2s(
                elements(token)?
                    .iter()
                    .map(|element| parse(element, "an int2"))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Floats => ColumnValue::Floats(
                elements(token)?
                    .iter()
                    .map(|element| finite_float(element))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Chars => ColumnValue::Chars(
                elements(token)?
                    .iter()
                    .map(|element| one_char(element))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Texts => ColumnValue::Texts(elements(token)?),
        })
    }
}

/// A value of the file that is not NULL, read for the kind of its column.
#[derive(Debug, PartialEq)]
enum ColumnValue {
    Oid(u32),
    Xid(u32),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Bool(bool),
    Char(char),
    Text(String),
    Oids(Vec<u32>),
    Int2s(Vec<i16>),
    Floats(Vec<f32>),
    Chars(Vec<char>),
    Texts(Vec<String>),
}

impl ColumnValue {
    /// The Rust expression of the value.
    fn literal(&self) -> TokenStream {
        match self {
            Self::Oid(value) | Self::Xid(value) => {
                let value = Literal::u32_unsuffixed(*value);
                quote! { #value }
            }
            Self::Int2(value) => {
                let value = Literal::i16_unsuffixed(*value);
                quote! { #value }
            }
            Self::Int4(value) => {
                let value = Literal::i32_unsuffixed(*value);
                quote! { #value }
            }
            Self::Int8(value) => {
                let value = Literal::i64_unsuffixed(*value);
                quote! { #value }
            }
            Self::Float4(value) => {
                let value = Literal::f32_unsuffixed(*value);
                quote! { #value }
            }
            Self::Bool(value) => quote! { #value },
            Self::Char(value) => quote! { #value },
            Self::Text(value) => quote! { #value },
            Self::Oids(values) => {
                let values = values.iter().map(|value| Literal::u32_unsuffixed(*value));
                quote! { &[#(#values),*] }
            }
            Self::Int2s(values) => {
                let values = values.iter().map(|value| Literal::i16_unsuffixed(*value));
                quote! { &[#(#values),*] }
            }
            Self::Floats(values) => {
                let values = values.iter().map(|value| Literal::f32_unsuffixed(*value));
                quote! { &[#(#values),*] }
            }
            Self::Chars(values) => quote! { &[#(#values),*] },
            Self::Texts(values) => quote! { &[#(#values),*] },
        }
    }

    /// The SQL value of the value, as the built-in rows of the catalog write it:
    /// see the crate documentation. `vector` tells an `oidvector` or
    /// `int2vector` from an array.
    fn sql_value(&self, vector: bool) -> SqlValue {
        fn list<T: Display>(values: &[T], vector: bool) -> SqlValue {
            let mut text = String::new();
            if vector {
                push_vector(values, &mut text);
            } else {
                push_array(values, &mut text);
            }
            SqlValue::Text(text)
        }
        match self {
            Self::Oid(value) | Self::Xid(value) => SqlValue::Integer(i64::from(*value)),
            Self::Int2(value) => SqlValue::Integer(i64::from(*value)),
            Self::Int4(value) => SqlValue::Integer(i64::from(*value)),
            Self::Int8(value) => SqlValue::Integer(*value),
            Self::Float4(value) => SqlValue::Real(float4_as_f64(*value)),
            Self::Bool(value) => SqlValue::Integer(i64::from(*value)),
            Self::Char(value) => SqlValue::Text(value.to_string()),
            Self::Text(value) => SqlValue::Text(value.clone()),
            Self::Oids(values) => list(values, vector),
            Self::Int2s(values) => list(values, vector),
            Self::Floats(values) => {
                let values: Vec<Float4> = values.iter().map(|value| Float4(*value)).collect();
                list(&values, vector)
            }
            Self::Chars(values) => list(values, vector),
            Self::Texts(values) => list(values, vector),
        }
    }
}

/// The number in `text`, of the type that `what` names.
fn parse<T: std::str::FromStr>(text: &str, what: &str) -> Result<T, String> {
    text.parse()
        .map_err(|_| format!("\"{text}\" is not {what}"))
}

/// Whether each value of the type takes the same number of bytes in a
/// row. The other types can be NULL in a catalog unless the column list
/// forces them.
fn has_fixed_width(bki_type: &str) -> bool {
    matches!(
        bki_type,
        "oid"
            | "regproc"
            | "xid"
            | "int2"
            | "int4"
            | "int8"
            | "float4"
            | "bool"
            | "char"
            | "name"
            | "timestamptz"
            | "pg_lsn"
    )
}

/// The `float4` in `text`. Rust has no literal for infinity and NaN.
fn finite_float(text: &str) -> Result<f32, String> {
    text.parse::<f32>()
        .ok()
        .filter(|value| value.is_finite())
        .ok_or_else(|| format!("\"{text}\" is not a finite float4"))
}

/// The oid in `token`. `-` in a function column means no function, which
/// PostgreSQL stores as oid 0.
fn oid_value(token: &Token) -> Result<u32, String> {
    match token.text() {
        "-" => Ok(0),
        text => text
            .parse()
            .map_err(|_| format!("\"{text}\" is not an oid")),
    }
}

/// The value of a symbol that `genbki.pl` leaves for `initdb` to replace.
/// The sizes are those of a 64-bit build of PostgreSQL. `POSTGRES` is the
/// name of the role that owns each built-in object: `initdb` gives it the
/// name of the operating system user, and Turso names it `postgres`. Any
/// other token is returned as it is.
fn substitute_symbol(token: &Token) -> &str {
    match token {
        Token::Bare(text) => match text.as_str() {
            "NAMEDATALEN" => "64",
            "SIZEOF_POINTER" => "8",
            "ALIGNOF_POINTER" => "d",
            "FLOAT8PASSBYVAL" => "t",
            "POSTGRES" => "postgres",
            other => other,
        },
        Token::Quoted(text) => text,
    }
}

/// The character of a `"char"` value. PostgreSQL's `"char"` is one byte,
/// so the character is ASCII.
fn one_char(text: &str) -> Result<char, String> {
    let mut chars = text.chars();
    match (chars.next(), chars.next()) {
        (Some(only), None) if only.is_ascii() && only != '\0' => Ok(only),
        _ => Err(format!("\"{text}\" is not one ASCII character")),
    }
}

/// The elements of a list value. An `oidvector` separates them with
/// spaces. An array is in braces and separates them with commas.
///
/// An array is read as PostgreSQL's `array_in` reads it (PostgreSQL
/// documentation, 8.15.2 "Array Value Input"): the white space before and
/// after an element that is not in double quotes is not part of it, so
/// `'{string, pattern}'` has the elements `string` and `pattern`. An element
/// in double quotes keeps its white space, and a backslash in it makes the
/// next character part of the element.
fn elements(token: &Token) -> Result<Vec<String>, String> {
    let text = token.text();
    let Some(mut rest) = text.strip_prefix('{') else {
        return Ok(text.split_whitespace().map(str::to_string).collect());
    };
    let refuse = |what: &str| {
        Err(format!(
            "the array \"{text}\" has {what}, which the generator does not read"
        ))
    };
    let mut elements = Vec::new();
    rest = rest.trim_start_matches(is_array_space);
    if let Some(after) = rest.strip_prefix('}') {
        if !after.is_empty() {
            return refuse("text after the closing brace");
        }
        return Ok(elements);
    }
    loop {
        rest = rest.trim_start_matches(is_array_space);
        let element = if let Some(quoted) = rest.strip_prefix('"') {
            let mut element = String::new();
            let mut chars = quoted.char_indices();
            let end = loop {
                match chars.next() {
                    Some((position, '"')) => break position,
                    Some((_, '\\')) => match chars.next() {
                        Some((_, escaped)) => element.push(escaped),
                        None => return refuse("a quoted element that does not end"),
                    },
                    Some((_, c)) => element.push(c),
                    None => return refuse("a quoted element that does not end"),
                }
            };
            rest = quoted[end + 1..].trim_start_matches(is_array_space);
            element
        } else {
            let end = rest
                .find([',', '}'])
                .ok_or_else(|| format!("the array \"{text}\" does not end with }}"))?;
            let element = rest[..end].trim_end_matches(is_array_space);
            rest = &rest[end..];
            // A brace is a nested array, and a backslash or a quote inside
            // an element that is not quoted is an escape.
            if element.contains(['{', '"', '\\']) {
                return refuse("a nested array or an escape");
            }
            // An empty element is not valid, and the element NULL is a
            // NULL, which the Rust types of the generated file cannot hold.
            if element.is_empty() || element.eq_ignore_ascii_case("NULL") {
                return refuse("an empty or NULL element");
            }
            element.to_string()
        };
        elements.push(element);
        if let Some(after) = rest.strip_prefix(',') {
            rest = after;
        } else if let Some(after) = rest.strip_prefix('}') {
            if !after.is_empty() {
                return refuse("text after the closing brace");
            }
            return Ok(elements);
        } else {
            return refuse("an element that a comma does not follow");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_symbol_becomes_the_value_of_a_64_bit_build() {
        let literal = |kind: ColumnKind, text: &str| {
            kind.literal(&Token::Bare(text.to_string()))
                .map(|tokens| tokens.to_string())
        };
        assert_eq!(literal(ColumnKind::Int2, "NAMEDATALEN").unwrap(), "64");
        assert_eq!(
            literal(ColumnKind::Bool, "FLOAT8PASSBYVAL").unwrap(),
            "true"
        );
        assert_eq!(literal(ColumnKind::Char, "ALIGNOF_POINTER").unwrap(), "'d'");
        assert!(literal(ColumnKind::Int4, "ENCODING").is_err());
    }

    #[test]
    fn a_float_that_rust_cannot_write_stops_the_generation() {
        let literal = |text: &str| {
            ColumnKind::Float4
                .literal(&Token::Bare(text.to_string()))
                .map(|tokens| tokens.to_string())
        };
        assert_eq!(literal("0.01").unwrap(), "0.01");
        assert_eq!(literal("1").unwrap(), "1.0");
        assert_eq!(
            literal("Infinity").unwrap_err(),
            "\"Infinity\" is not a finite float4"
        );
        assert!(literal("NaN").is_err());
    }

    #[test]
    fn the_spaces_around_an_array_element_are_not_part_of_it() {
        let read = |text: &str| elements(&Token::Quoted(text.to_string()));
        assert_eq!(read("{string, pattern}").unwrap(), ["string", "pattern"]);
        assert_eq!(read("{ a ,b }").unwrap(), ["a", "b"]);
        // A quoted element keeps its spaces; the spaces outside the quotes
        // are not part of it.
        assert_eq!(read("{a, b,\"c d\" ,e}").unwrap(), ["a", "b", "c d", "e"]);
        assert_eq!(
            read(r#"{" x ","a\"b","c\\d","NULL",""}"#).unwrap(),
            [" x ", "a\"b", "c\\d", "NULL", ""]
        );
        assert_eq!(read("{ }").unwrap(), Vec::<String>::new());
        assert_eq!(read("{}").unwrap(), Vec::<String>::new());
        assert_eq!(read("23 25").unwrap(), ["23", "25"]);
        for malformed in [
            "{a,,b}",
            "{a,NULL}",
            "{a,null}",
            "{{a}}",
            "{a\\,b}",
            "{\"a}",
            "{\"a\" b}",
            "{a",
            "{a}x",
            "{a,}",
        ] {
            assert!(read(malformed).is_err(), "{malformed}");
        }

        let literal = |text: &str| {
            ColumnKind::Texts
                .literal(&Token::Quoted(text.to_string()))
                .map(|tokens| tokens.to_string())
        };
        assert_eq!(
            literal("{string, pattern}").unwrap(),
            "& [\"string\" , \"pattern\"]"
        );
    }

    /// The SQL value of `text` in a column of type `bki_type`, which can be
    /// NULL.
    fn sql_value(bki_type: &str, token: Token) -> Result<SqlValue, String> {
        let field = Field {
            name: "c",
            bki_type,
            kind: ColumnKind::of(bki_type).unwrap(),
            nullable: true,
        };
        field.sql_value(&token)
    }

    #[test]
    fn each_type_has_the_sql_value_of_its_text_output() {
        let bare = |text: &str| Token::Bare(text.to_string());
        let quoted = |text: &str| Token::Quoted(text.to_string());
        let text = |text: &str| SqlValue::Text(text.to_string());
        let cases = [
            ("oid", bare("16"), SqlValue::Integer(16)),
            ("oid", bare("4294967295"), SqlValue::Integer(4_294_967_295)),
            ("regproc", bare("1242"), SqlValue::Integer(1242)),
            ("regproc", bare("-"), SqlValue::Integer(0)),
            ("xid", bare("3"), SqlValue::Integer(3)),
            ("int2", bare("-1"), SqlValue::Integer(-1)),
            ("int2", bare("NAMEDATALEN"), SqlValue::Integer(64)),
            (
                "int4",
                bare("-2147483648"),
                SqlValue::Integer(-2_147_483_648),
            ),
            (
                "int8",
                bare("9223372036854775807"),
                SqlValue::Integer(i64::MAX),
            ),
            ("bool", bare("t"), SqlValue::Integer(1)),
            ("bool", bare("f"), SqlValue::Integer(0)),
            ("bool", bare("FLOAT8PASSBYVAL"), SqlValue::Integer(1)),
            ("float4", bare("0.01"), SqlValue::Real(0.01)),
            ("float4", bare("1"), SqlValue::Real(1.0)),
            ("char", bare("b"), text("b")),
            ("char", quoted(","), text(",")),
            ("char", bare("ALIGNOF_POINTER"), text("d")),
            ("name", bare("bool"), text("bool")),
            ("name", bare("POSTGRES"), text("postgres")),
            (
                "text",
                quoted("database's default"),
                text("database's default"),
            ),
            ("text", quoted(""), text("")),
            ("text", quoted("_null_"), text("_null_")),
            (
                "pg_node_tree",
                quoted("({CONST :consttype 16})"),
                text("({CONST :consttype 16})"),
            ),
            ("oidvector", quoted("23 25"), text("23 25")),
            ("oidvector", quoted(""), text("")),
            ("int2vector", quoted("1 -2"), text("1 -2")),
            ("_oid", quoted("{25,20,1184}"), text("{25,20,1184}")),
            ("_int2", quoted("{1, 2}"), text("{1,2}")),
            ("_float4", quoted("{0.5,1e10}"), text("{0.5,1e+10}")),
            ("_char", quoted("{i,o,o}"), text("{i,o,o}")),
            (
                "_text",
                quoted("{string, pattern}"),
                text("{string,pattern}"),
            ),
            (
                "_text",
                quoted("{\"a b\",\"NULL\",\"\",x}"),
                text("{\"a b\",\"NULL\",\"\",x}"),
            ),
            ("_text", quoted("{\"a\\\"b\"}"), text("{\"a\\\"b\"}")),
            ("_aclitem", quoted("{=Tc/postgres}"), text("{=Tc/postgres}")),
            ("oid", bare("_null_"), SqlValue::Null),
            ("_text", bare("_null_"), SqlValue::Null),
            ("timestamptz", bare("_null_"), SqlValue::Null),
        ];
        for (bki_type, token, expected) in cases {
            let description = format!("{bki_type} {:?}", token.text());
            assert_eq!(
                sql_value(bki_type, token).unwrap(),
                expected,
                "{description}"
            );
        }
    }

    #[test]
    fn a_value_that_a_row_cannot_write_stops_the_generation() {
        let bare = |text: &str| Token::Bare(text.to_string());
        for bki_type in [
            "timestamptz",
            "pg_lsn",
            "bytea",
            "anyarray",
            "_pg_statistic",
            "pg_ndistinct",
            "pg_dependencies",
            "pg_mcv_list",
        ] {
            assert!(sql_value(bki_type, bare("0")).is_err(), "{bki_type}");
        }
        // PostgreSQL's "char" is one byte.
        assert!(sql_value("char", bare("é")).is_err());
        assert!(sql_value("char", bare("ab")).is_err());
        assert!(sql_value("_char", Token::Quoted("{a,é}".to_string())).is_err());
        assert!(sql_value("bool", bare("yes")).is_err());
        assert!(sql_value("int2", bare("40000")).is_err());
    }

    /// `postgres.bki` has the `pg_attribute` rows of four catalogs, which
    /// say what PostgreSQL itself decided for each of their columns.
    #[test]
    fn a_column_is_not_null_where_postgresql_says_so() {
        let catalogs = crate::bki::parse(include_str!("../../upstream/postgres.bki")).unwrap();
        let catalog = |name: &str| catalogs.iter().find(|c| c.name == name).unwrap();
        let pg_attribute = catalog("pg_attribute");
        let position = |column: &str| {
            pg_attribute
                .columns
                .iter()
                .position(|c| c.name == column)
                .unwrap()
        };
        let (attrelid, attname, attnum, attnotnull) = (
            position("attrelid"),
            position("attname"),
            position("attnum"),
            position("attnotnull"),
        );

        for (name, oid) in [
            ("pg_proc", "1255"),
            ("pg_type", "1247"),
            ("pg_attribute", "1249"),
            ("pg_class", "1259"),
        ] {
            let fields = Field::of_catalog(catalog(name)).unwrap();
            let mut checked = 0;
            for row in &pg_attribute.rows {
                // A negative number is a system column such as `ctid`,
                // which the column list of the catalog does not have.
                if row[attrelid].text() != oid || row[attnum].text().starts_with('-') {
                    continue;
                }
                let field = fields
                    .iter()
                    .find(|field| field.name == row[attname].text())
                    .unwrap();
                let not_null = row[attnotnull].text() == "t";
                assert_eq!(!field.nullable, not_null, "{name}.{}", field.name);
                checked += 1;
            }
            assert_eq!(checked, fields.len(), "{name}");
        }
    }
}
