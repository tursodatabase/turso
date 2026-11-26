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
    // PostgreSQL reads the options before it opens the destination.
    check_copy_options(&copy.options, false)?;
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
        )
        .with_hint("Use COPY ... TO STDOUT and write the file in the client (psql \\copy)."));
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
    for option in &copy.options {
        let Some(Node::DefElem(option)) = &option.node else {
            unreachable!("COPY options are DefElem nodes");
        };
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

/// Checks the COPY option list the way PostgreSQL's ProcessCopyOptions does:
/// an unknown or repeated option, an option without the value it needs, a
/// HEADER value that is not a Boolean or "match", a FORMAT other than text,
/// csv or binary, and the options that COPY TO does not allow are errors of
/// their own. `is_from` is true for COPY FROM.
pub(crate) fn check_copy_options(
    options: &[pg_query::protobuf::Node],
    is_from: bool,
) -> Result<(), ParseError> {
    use crate::sqlstate::{SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_SYNTAX_ERROR};

    let mut seen = std::collections::HashSet::new();
    let mut freeze = false;
    let mut on_error_ignore = false;
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
        let arg = option.arg.as_ref().and_then(|arg| arg.node.as_ref());
        match name {
            "freeze" => freeze = crate::defelem::boolean_option(option)?,
            "format" | "delimiter" | "null" | "default" | "quote" | "escape" | "encoding"
            | "on_error" | "log_verbosity"
                if arg.is_none() =>
            {
                return Err(ParseError::syntax_error(format!(
                    "{name} requires a parameter"
                )));
            }
            "reject_limit" if arg.is_none() => {
                return Err(ParseError::syntax_error(format!(
                    "{name} requires a numeric value"
                )));
            }
            "force_quote" | "force_not_null" | "force_null"
                if !matches!(arg, Some(Node::List(_) | Node::AStar(_))) =>
            {
                return Err(ParseError::sqlstate(
                    SQLSTATE_INVALID_PARAMETER_VALUE,
                    format!("argument to option \"{name}\" must be a list of column names"),
                ));
            }
            "on_error" if !is_from => {
                return Err(ParseError::sqlstate(
                    SQLSTATE_INVALID_PARAMETER_VALUE,
                    "COPY ON_ERROR cannot be used with COPY TO",
                ));
            }
            "on_error" => {
                on_error_ignore = matches!(
                    arg,
                    Some(Node::String(value)) if value.sval.eq_ignore_ascii_case("ignore")
                );
            }
            _ => {}
        }
        match (name, arg) {
            ("header", Some(Node::String(value))) if value.sval.eq_ignore_ascii_case("match") => {}
            ("header", _) => {
                crate::defelem::boolean_option(option).map_err(|_| header_error())?;
            }
            ("format", Some(value)) => {
                let value = match value {
                    Node::String(value) => value.sval.clone(),
                    Node::Integer(value) => value.ival.to_string(),
                    Node::Float(value) => value.fval.clone(),
                    _ => String::new(),
                };
                if !["text", "csv", "binary"].contains(&value.as_str()) {
                    return Err(ParseError::sqlstate(
                        SQLSTATE_INVALID_PARAMETER_VALUE,
                        format!("COPY format \"{value}\" not recognized"),
                    ));
                }
            }
            _ => {}
        }
    }
    // PostgreSQL checks these after it has read all options.
    if freeze && !is_from {
        return Err(ParseError::sqlstate(
            SQLSTATE_INVALID_PARAMETER_VALUE,
            "COPY FREEZE cannot be used with COPY TO",
        ));
    }
    if seen.contains("reject_limit") && !on_error_ignore {
        return Err(ParseError::sqlstate(
            SQLSTATE_INVALID_PARAMETER_VALUE,
            "COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE",
        ));
    }
    Ok(())
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
