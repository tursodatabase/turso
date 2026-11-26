use pg_query::protobuf::{node::Node, Token};

use crate::ParseError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyToStdout {
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
}

pub fn parse_copy_to_stdout(sql: &str) -> Result<Option<CopyToStdout>, ParseError> {
    let parsed = crate::parse(sql)?;
    if parsed.protobuf.stmts.len() > 1 {
        return Err(ParseError::sqlstate(
            crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
            "expected one statement",
        ));
    }
    try_extract_copy_to_stdout(sql, &parsed)
}

pub(crate) fn try_extract_copy_to_stdout(
    sql: &str,
    parsed: &crate::ParseResult,
) -> Result<Option<CopyToStdout>, ParseError> {
    let Some(Node::CopyStmt(copy)) = crate::first_statement(parsed) else {
        return Ok(None);
    };
    if copy.is_from {
        return Ok(None);
    }
    if parsed.protobuf.stmts.len() > 1 {
        return Err(ParseError::sqlstate(
            crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
            "expected one statement",
        ));
    }
    if copy.is_program {
        return Err(ParseError::sqlstate(
            crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
            "COPY TO PROGRAM is not supported",
        ));
    }
    if !copy.filename.is_empty() {
        return Err(ParseError::sqlstate(
            crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
            "COPY TO only supports STDOUT",
        ));
    }
    let table = copy.relation.as_ref().ok_or_else(|| {
        ParseError::sqlstate(
            crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
            "COPY TO STDOUT only supports a table, not a query",
        )
    })?;
    // pg_query's protobuf represents both TO STDOUT (a null filename in PostgreSQL)
    // and TO '' as an empty string. Scan the SQL to reject the empty filename
    // instead of treating it as STDOUT.
    let scanned = pg_query::scan(sql).map_err(|error| ParseError::PgQuery(error.into()))?;
    let destination = scanned
        .tokens
        .iter()
        .map(|token| token.token())
        .filter(|token| !matches!(token, Token::SqlComment | Token::CComment))
        .skip_while(|token| *token != Token::To)
        .nth(1);
    if destination != Some(Token::Stdout) {
        return Err(ParseError::sqlstate(
            crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
            "COPY TO only supports STDOUT",
        ));
    }
    if !table.catalogname.is_empty() {
        return Err(ParseError::sqlstate(
            crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
            "COPY cannot name another database",
        ));
    }
    check_copy_options(&copy.options)?;
    for option in &copy.options {
        let Some(Node::DefElem(option)) = &option.node else {
            unreachable!("COPY options are DefElem nodes");
        };
        // PostgreSQL rejects FREEZE and an ON_ERROR other than stop for COPY TO
        // after it has read all options.
        let rejected_for_copy_to = match option.defname.as_str() {
            "freeze" => boolean_option(option)?,
            "on_error" => !matches!(
                option.arg.as_ref().and_then(|arg| arg.node.as_ref()),
                Some(Node::String(value)) if value.sval.eq_ignore_ascii_case("stop")
            ),
            _ => false,
        };
        if rejected_for_copy_to {
            return Err(ParseError::sqlstate(
                crate::sqlstate::SQLSTATE_INVALID_PARAMETER_VALUE,
                format!(
                    "COPY {} cannot be used with COPY TO",
                    option.defname.to_uppercase()
                ),
            ));
        }
        if option.defname != "format" {
            return Err(ParseError::sqlstate(
                crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "COPY TO STDOUT option '{}' is not supported",
                    option.defname
                ),
            ));
        }
        match option.arg.as_ref().and_then(|arg| arg.node.as_ref()) {
            Some(Node::String(format)) if format.sval == "text" => {}
            _ => {
                return Err(ParseError::sqlstate(
                    crate::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED,
                    "COPY TO STDOUT only supports text format",
                ))
            }
        }
    }
    let mut columns = Vec::with_capacity(copy.attlist.len());
    for column in &copy.attlist {
        let Some(Node::String(column)) = &column.node else {
            unreachable!("COPY column names are String nodes");
        };
        if columns.contains(&column.sval) {
            return Err(ParseError::sqlstate(
                crate::sqlstate::SQLSTATE_DUPLICATE_COLUMN,
                format!("COPY column '{}' was specified more than once", column.sval),
            ));
        }
        columns.push(column.sval.clone());
    }
    Ok(Some(CopyToStdout {
        schema: (!table.schemaname.is_empty()).then(|| table.schemaname.clone()),
        table: table.relname.clone(),
        columns,
    }))
}

/// Checks the COPY option list the way PostgreSQL does while it reads the
/// options: an unknown or repeated option, a HEADER value that is not a
/// Boolean or "match", and a FORMAT other than text, csv or binary are
/// errors of their own.
pub(crate) fn check_copy_options(options: &[pg_query::protobuf::Node]) -> Result<(), ParseError> {
    use crate::sqlstate::{SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_SYNTAX_ERROR};

    let mut seen = std::collections::HashSet::new();
    for option in options {
        let Some(Node::DefElem(option)) = &option.node else {
            unreachable!("COPY options are DefElem nodes");
        };
        let name = option.defname.as_str();
        if !matches!(
            name,
            "format"
                | "freeze"
                | "delimiter"
                | "null"
                | "default"
                | "header"
                | "quote"
                | "escape"
                | "force_quote"
                | "force_not_null"
                | "force_null"
                | "on_error"
                | "reject_limit"
                | "log_verbosity"
                | "encoding"
                | "convert_selectively"
        ) {
            return Err(ParseError::sqlstate(
                SQLSTATE_SYNTAX_ERROR,
                format!("option \"{name}\" not recognized"),
            ));
        }
        if !seen.insert(name) {
            return Err(ParseError::syntax_error("conflicting or redundant options"));
        }
        if name == "freeze" {
            boolean_option(option)?;
        }
        let arg = option.arg.as_ref().and_then(|arg| arg.node.as_ref());
        match (name, arg) {
            ("header", None) => {}
            ("header", Some(Node::String(value))) => {
                if !["true", "false", "on", "off", "match"]
                    .iter()
                    .any(|word| value.sval.eq_ignore_ascii_case(word))
                {
                    return Err(header_error());
                }
            }
            ("header", Some(Node::Integer(value))) if matches!(value.ival, 0 | 1) => {}
            ("header", Some(_)) => return Err(header_error()),
            ("format", Some(Node::String(value)))
                if !["text", "csv", "binary"].contains(&value.sval.as_str()) =>
            {
                return Err(ParseError::sqlstate(
                    SQLSTATE_INVALID_PARAMETER_VALUE,
                    format!("COPY format \"{}\" not recognized", value.sval),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Reads a Boolean COPY option the way PostgreSQL's defGetBoolean does: no
/// value means true.
fn boolean_option(option: &pg_query::protobuf::DefElem) -> Result<bool, ParseError> {
    match option.arg.as_ref().and_then(|arg| arg.node.as_ref()) {
        None => Ok(true),
        Some(Node::Integer(value)) if matches!(value.ival, 0 | 1) => Ok(value.ival == 1),
        Some(Node::String(value))
            if ["true", "on"]
                .iter()
                .any(|w| value.sval.eq_ignore_ascii_case(w)) =>
        {
            Ok(true)
        }
        Some(Node::String(value))
            if ["false", "off"]
                .iter()
                .any(|w| value.sval.eq_ignore_ascii_case(w)) =>
        {
            Ok(false)
        }
        _ => Err(ParseError::syntax_error(format!(
            "{} requires a Boolean value",
            option.defname
        ))),
    }
}

fn header_error() -> ParseError {
    ParseError::syntax_error("header requires a Boolean value or \"match\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_to_stdout_preserves_identifiers_and_column_order() {
        assert_eq!(
            parse_copy_to_stdout(
                r#"/* export */ COPY "Odd Schema"."A""B;C" ("Email", id) TO /* destination */ stdout WITH (FORMAT text)"#,
            )
            .unwrap(),
            Some(CopyToStdout {
                schema: Some("Odd Schema".to_owned()),
                table: "A\"B;C".to_owned(),
                columns: vec!["Email".to_owned(), "id".to_owned()],
            })
        );

        assert_eq!(
            parse_copy_to_stdout("copy USERS to stdout").unwrap(),
            Some(CopyToStdout {
                schema: None,
                table: "users".to_owned(),
                columns: Vec::new(),
            })
        );
    }

    #[test]
    fn copy_to_stdout_rejects_unsupported_forms() {
        for (sql, error) in [
            ("COPY users TO '/tmp/export'", "STDOUT"),
            ("COPY users TO ''", "STDOUT"),
            ("COPY stdout (stdout) TO ''", "STDOUT"),
            ("COPY users TO PROGRAM 'cat'", "PROGRAM"),
            ("COPY (SELECT * FROM users) TO STDOUT", "table"),
            ("COPY other.public.users TO STDOUT", "database"),
            ("COPY users (id, id) TO STDOUT", "more than once"),
            ("COPY users TO STDOUT (FORMAT csv)", "text"),
            ("COPY BINARY users TO STDOUT", "text"),
            ("COPY users TO STDOUT (DELIMITER ',')", "delimiter"),
            ("COPY users TO STDOUT (HEADER)", "header"),
            ("COPY users TO STDOUT (NULL '')", "null"),
            (
                "COPY users TO STDOUT (FORMAT text, FORMAT text)",
                "conflicting or redundant options",
            ),
            ("COPY users TO STDOUT; SELECT 1", "one statement"),
        ] {
            let actual = parse_copy_to_stdout(sql).unwrap_err().to_string();
            assert!(actual.contains(error), "{sql}: {actual}");
        }
    }

    #[test]
    fn copy_to_stdout_leaves_other_statements_unchanged() {
        for sql in [
            "SELECT 'COPY users TO STDOUT'",
            "COPY users FROM STDIN",
            "COPY users FROM '/tmp/import'",
            "",
        ] {
            assert_eq!(parse_copy_to_stdout(sql).unwrap(), None, "{sql}");
        }
    }
}
