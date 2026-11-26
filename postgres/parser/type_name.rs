//! A type name written as text, such as the second argument of
//! `pg_input_is_valid`, read with PostgreSQL's grammar.

use crate::sqlstate::SQLSTATE_SYNTAX_ERROR;
use crate::ParseError;
use pg_query::protobuf::{a_const, node::Node, Token, TypeName};

/// A type name as PostgreSQL's grammar reads it. The grammar turns the SQL
/// spellings into the name of the pg_type row: `integer` is `int4`,
/// `character varying(4)` is `varchar` with the modifier 4. A quoted name is
/// kept as it is written, so `"integer"` names no built-in type. `schema` is
/// the schema that the text names, and not the `pg_catalog` that the grammar
/// adds to an SQL spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenTypeName {
    pub schema: Option<String>,
    pub name: String,
    pub modifiers: Vec<i64>,
    pub array_dimensions: u32,
}

impl WrittenTypeName {
    /// The name with its schema, as PostgreSQL writes it in the error for a
    /// type that does not exist.
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

/// Reads `text` as a type name (PostgreSQL's `parseTypeString`). Text that is
/// not one type name fails with 42601.
pub fn parse_type_name(text: &str) -> Result<WrittenTypeName, ParseError> {
    let invalid = || {
        ParseError::sqlstate(
            SQLSTATE_SYNTAX_ERROR,
            format!("invalid type name \"{text}\""),
        )
    };
    // The grammar has no entry point for a type name alone, so the name is
    // read as the target of a cast. Text that closes the parenthesis of the
    // cast, such as `int4) --`, would go on as the rest of the statement.
    if closes_a_parenthesis_it_did_not_open(text).ok_or_else(invalid)? {
        return Err(invalid());
    }
    let parsed = crate::parse(&format!("SELECT CAST(NULL AS {text})")).map_err(|_| invalid())?;
    let [statement] = parsed.protobuf.stmts.as_slice() else {
        return Err(invalid());
    };
    let Some(Node::SelectStmt(select)) = statement.stmt.as_ref().and_then(|s| s.node.as_ref())
    else {
        return Err(invalid());
    };
    let [target] = select.target_list.as_slice() else {
        return Err(invalid());
    };
    let Some(Node::ResTarget(target)) = target.node.as_ref() else {
        return Err(invalid());
    };
    let Some(Node::TypeCast(cast)) = target.val.as_ref().and_then(|val| val.node.as_ref()) else {
        return Err(invalid());
    };
    let is_null_constant = matches!(
        cast.arg.as_ref().and_then(|arg| arg.node.as_ref()),
        Some(Node::AConst(constant)) if constant.isnull
    );
    let Some(type_name) = cast.type_name.as_ref() else {
        return Err(invalid());
    };
    if !is_null_constant || type_name.setof || type_name.pct_type {
        return Err(invalid());
    }
    let mut written = written_type_name(type_name).ok_or_else(invalid)?;
    if written.schema.as_deref() == Some("pg_catalog") && !names_pg_catalog(text) {
        written.schema = None;
    }
    Ok(written)
}

/// None when the scanner of PostgreSQL cannot read `text`.
fn closes_a_parenthesis_it_did_not_open(text: &str) -> Option<bool> {
    let mut depth = 0i32;
    for token in pg_query::scan(text).ok()?.tokens {
        match token.token() {
            Token::Ascii40 => depth += 1,
            Token::Ascii41 => depth -= 1,
            _ => {}
        }
        if depth < 0 {
            return Some(true);
        }
    }
    Some(false)
}

fn names_pg_catalog(text: &str) -> bool {
    let text = text.trim_start();
    let text = text.strip_prefix('"').unwrap_or(text);
    text.get(.."pg_catalog".len())
        .is_some_and(|schema| schema.eq_ignore_ascii_case("pg_catalog"))
}

fn written_type_name(type_name: &TypeName) -> Option<WrittenTypeName> {
    let mut names = type_name
        .names
        .iter()
        .map(|name| match name.node.as_ref() {
            Some(Node::String(name)) => Some(name.sval.clone()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    let name = names.pop()?;
    let schema = names.pop();
    if !names.is_empty() {
        return None;
    }
    let modifiers = type_name
        .typmods
        .iter()
        .map(|modifier| match modifier.node.as_ref() {
            Some(Node::AConst(constant)) => match constant.val.as_ref() {
                Some(a_const::Val::Ival(integer)) => Some(i64::from(integer.ival)),
                _ => None,
            },
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some(WrittenTypeName {
        schema,
        name,
        modifiers,
        array_dimensions: u32::try_from(type_name.array_bounds.len()).ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_type_name, WrittenTypeName};

    fn written(
        schema: Option<&str>,
        name: &str,
        modifiers: &[i64],
        dimensions: u32,
    ) -> WrittenTypeName {
        WrittenTypeName {
            schema: schema.map(str::to_string),
            name: name.to_string(),
            modifiers: modifiers.to_vec(),
            array_dimensions: dimensions,
        }
    }

    #[test]
    fn sql_spellings_become_the_names_of_pg_type() {
        for (text, expected) in [
            ("integer", written(None, "int4", &[], 0)),
            ("INT", written(None, "int4", &[], 0)),
            ("smallint", written(None, "int2", &[], 0)),
            ("boolean", written(None, "bool", &[], 0)),
            ("double precision", written(None, "float8", &[], 0)),
            ("character varying(4)", written(None, "varchar", &[4], 0)),
            ("numeric(10, 2)", written(None, "numeric", &[10, 2], 0)),
            (
                "timestamp with time zone",
                written(None, "timestamptz", &[], 0),
            ),
            ("int[]", written(None, "int4", &[], 1)),
            ("int4", written(None, "int4", &[], 0)),
            (
                "pg_catalog.int4",
                written(Some("pg_catalog"), "int4", &[], 0),
            ),
            (
                " PG_CATALOG.int4",
                written(Some("pg_catalog"), "int4", &[], 0),
            ),
            (
                "\"pg_catalog\".int4",
                written(Some("pg_catalog"), "int4", &[], 0),
            ),
            ("public.int4", written(Some("public"), "int4", &[], 0)),
            ("tinyint", written(None, "tinyint", &[], 0)),
        ] {
            assert_eq!(parse_type_name(text).unwrap(), expected, "{text}");
        }
    }

    #[test]
    fn quoted_name_is_kept_as_written() {
        assert_eq!(
            parse_type_name("\"integer\"").unwrap(),
            written(None, "integer", &[], 0)
        );
        assert_eq!(
            parse_type_name("\"INT4\"").unwrap(),
            written(None, "INT4", &[], 0)
        );
    }

    #[test]
    fn text_that_is_not_one_type_name_is_refused() {
        for text in [
            "",
            "int4 x",
            "int4), (1",
            "int4) FROM t --",
            "int4) --",
            "int4) GROUP BY (1",
            "int4) HAVING (true",
            "int4) OFFSET (1",
            "int4) WINDOW w AS (",
            "int4 --",
            "int4); SELECT (1",
            "a.b.c.d",
        ] {
            let error = parse_type_name(text).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("invalid type name \"{text}\""),
                "{text}"
            );
        }
    }
}
