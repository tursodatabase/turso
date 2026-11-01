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
        return Err(ParseError::ParseError("expected one statement".to_owned()));
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
        return Err(ParseError::ParseError("expected one statement".to_owned()));
    }
    if copy.is_program {
        return Err(ParseError::ParseError(
            "COPY TO PROGRAM is not supported".to_owned(),
        ));
    }
    if !copy.filename.is_empty() {
        return Err(ParseError::ParseError(
            "COPY TO only supports STDOUT".to_owned(),
        ));
    }
    let table = copy.relation.as_ref().ok_or_else(|| {
        ParseError::ParseError("COPY TO STDOUT only supports a table, not a query".to_owned())
    })?;
    let scanned = pg_query::scan(sql).map_err(|error| ParseError::ParseError(error.to_string()))?;
    let destination = scanned
        .tokens
        .iter()
        .map(|token| token.token())
        .filter(|token| !matches!(token, Token::SqlComment | Token::CComment))
        .skip_while(|token| *token != Token::To)
        .nth(1);
    if destination != Some(Token::Stdout) {
        return Err(ParseError::ParseError(
            "COPY TO only supports STDOUT".to_owned(),
        ));
    }
    if !table.catalogname.is_empty() {
        return Err(ParseError::ParseError(
            "COPY cannot name another database".to_owned(),
        ));
    }
    let mut format_seen = false;
    for option in &copy.options {
        let Some(Node::DefElem(option)) = &option.node else {
            unreachable!("COPY options are DefElem nodes");
        };
        if option.defname != "format" {
            return Err(ParseError::ParseError(format!(
                "COPY TO STDOUT option '{}' is not supported",
                option.defname
            )));
        }
        if format_seen {
            return Err(ParseError::ParseError(
                "COPY format was specified more than once".to_owned(),
            ));
        }
        format_seen = true;
        match option.arg.as_ref().and_then(|arg| arg.node.as_ref()) {
            Some(Node::String(format)) if format.sval == "text" => {}
            _ => {
                return Err(ParseError::ParseError(
                    "COPY TO STDOUT only supports text format".to_owned(),
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
            return Err(ParseError::ParseError(format!(
                "COPY column '{}' was specified more than once",
                column.sval
            )));
        }
        columns.push(column.sval.clone());
    }
    Ok(Some(CopyToStdout {
        schema: (!table.schemaname.is_empty()).then(|| table.schemaname.clone()),
        table: table.relname.clone(),
        columns,
    }))
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
                "more than once",
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
