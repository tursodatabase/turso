//! The columns of a catalog as Rust: the type of a column, and the Rust
//! expression for a value of the file in that column.

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use crate::bki::{Catalog, Nullness, Token};

pub(crate) struct Field<'a> {
    pub(crate) name: &'a str,
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
        if token.is_null() {
            return if self.nullable {
                Ok(quote! { None })
            } else {
                Err("the column is NOT NULL and the row has NULL in it".to_string())
            };
        }
        let value = self.kind.literal(token)?;
        Ok(if self.nullable {
            quote! { Some(#value) }
        } else {
            value
        })
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
    /// `name` and `text`, and each type that has no Rust form here and is
    /// kept as the text of the file, such as the expression trees of
    /// `pg_node_tree`.
    Text,
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
            // No built-in row has a value of these types.
            "timestamptz" | "pg_lsn" | "bytea" | "anyarray" | "_pg_statistic" | "pg_ndistinct"
            | "pg_dependencies" | "pg_mcv_list" => Self::Text,
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
            Self::Text => quote! { &'static str },
            Self::Oids => quote! { &'static [Oid] },
            Self::Int2s => quote! { &'static [i16] },
            Self::Floats => quote! { &'static [f32] },
            Self::Chars => quote! { &'static [char] },
            Self::Texts => quote! { &'static [&'static str] },
        }
    }

    /// The Rust expression for `token`, which is not NULL.
    fn literal(self, token: &Token) -> Result<TokenStream, String> {
        let text = substitute_symbol(token);
        Ok(match self {
            Self::Oid => {
                let oid = Literal::u32_unsuffixed(oid_value(token)?);
                quote! { #oid }
            }
            Self::Int2 => {
                let value = text
                    .parse()
                    .map(Literal::i16_unsuffixed)
                    .map_err(|_| format!("\"{text}\" is not an int2"))?;
                quote! { #value }
            }
            Self::Int4 => {
                let value = text
                    .parse()
                    .map(Literal::i32_unsuffixed)
                    .map_err(|_| format!("\"{text}\" is not an int4"))?;
                quote! { #value }
            }
            Self::Int8 => {
                let value = text
                    .parse()
                    .map(Literal::i64_unsuffixed)
                    .map_err(|_| format!("\"{text}\" is not an int8"))?;
                quote! { #value }
            }
            Self::Xid => {
                let value = text
                    .parse()
                    .map(Literal::u32_unsuffixed)
                    .map_err(|_| format!("\"{text}\" is not an xid"))?;
                quote! { #value }
            }
            Self::Float4 => {
                let value = float_literal(text)?;
                quote! { #value }
            }
            Self::Bool => match text {
                "t" => quote! { true },
                "f" => quote! { false },
                _ => return Err(format!("\"{text}\" is not a bool")),
            },
            Self::Char => {
                let value = one_char(text)?;
                quote! { #value }
            }
            Self::Text => quote! { #text },
            Self::Oids => {
                let oids = elements(token)?
                    .iter()
                    .map(|element| {
                        element
                            .parse()
                            .map(Literal::u32_unsuffixed)
                            .map_err(|_| format!("\"{element}\" is not an oid"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                quote! { &[#(#oids),*] }
            }
            Self::Int2s => {
                let values = elements(token)?
                    .iter()
                    .map(|element| {
                        element
                            .parse()
                            .map(Literal::i16_unsuffixed)
                            .map_err(|_| format!("\"{element}\" is not an int2"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                quote! { &[#(#values),*] }
            }
            Self::Floats => {
                let values = elements(token)?
                    .iter()
                    .map(|element| float_literal(element))
                    .collect::<Result<Vec<_>, _>>()?;
                quote! { &[#(#values),*] }
            }
            Self::Chars => {
                let chars = elements(token)?
                    .iter()
                    .map(|element| one_char(element))
                    .collect::<Result<Vec<_>, _>>()?;
                quote! { &[#(#chars),*] }
            }
            Self::Texts => {
                let texts = elements(token)?;
                quote! { &[#(#texts),*] }
            }
        })
    }
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

/// The Rust literal for a `float4`. Rust has no literal for infinity and
/// NaN.
fn float_literal(text: &str) -> Result<Literal, String> {
    text.parse::<f32>()
        .ok()
        .filter(|value| value.is_finite())
        .map(Literal::f32_unsuffixed)
        .ok_or_else(|| format!("\"{text}\" is not a finite float4"))
}

/// The oid in `token`. `-` in a function column means no function, which
/// PostgreSQL stores as oid 0.
pub(crate) fn oid_value(token: &Token) -> Result<u32, String> {
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

fn one_char(text: &str) -> Result<char, String> {
    let mut chars = text.chars();
    match (chars.next(), chars.next()) {
        (Some(only), None) => Ok(only),
        _ => Err(format!("\"{text}\" is not one character")),
    }
}

/// The elements of a list value. An `oidvector` separates them with
/// spaces. An array is in braces and separates them with commas.
fn elements(token: &Token) -> Result<Vec<&str>, String> {
    let text = token.text();
    let Some(inner) = text.strip_prefix('{') else {
        return Ok(text.split_whitespace().collect());
    };
    let inner = inner
        .strip_suffix('}')
        .ok_or_else(|| format!("the array \"{text}\" does not end with }}"))?;
    // PostgreSQL quotes an array element that has a comma, a brace or a
    // space in it. No built-in row has one.
    if inner.contains(['"', '\\', '{']) {
        return Err(format!(
            "the array \"{text}\" has a quoted or nested element, which the generator does not read"
        ));
    }
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    Ok(inner.split(',').collect())
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
