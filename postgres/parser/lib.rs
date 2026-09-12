use pg_query::ParseResult;
use thiserror::Error;

pub mod translator;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("{0}")]
    ParseError(String),
}

/// Parse a PostgreSQL SQL statement using pg_query
pub fn parse(sql: &str) -> Result<ParseResult, ParseError> {
    pg_query::parse(sql).map_err(|e| ParseError::ParseError(e.to_string()))
}

/// Split a multi-statement SQL string into individual statements.
/// Uses pg_query's scanner which correctly handles semicolons inside
/// string literals, comments, and dollar-quoted strings.
/// Returns the individual statement strings (without trailing semicolons).
pub fn split_statements(sql: &str) -> Result<Vec<String>, ParseError> {
    let parts =
        pg_query::split_with_scanner(sql).map_err(|e| ParseError::ParseError(e.to_string()))?;
    Ok(parts
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

/// Get tables referenced in a query
pub fn get_tables(sql: &str) -> Result<Vec<String>, ParseError> {
    let result = parse(sql)?;
    Ok(result.tables())
}

/// Normalize a query (replace constants with $1, $2, etc.)
pub fn normalize(sql: &str) -> Result<String, ParseError> {
    pg_query::normalize(sql).map_err(|e| ParseError::ParseError(e.to_string()))
}

/// Get a fingerprint for a query (for caching/deduplication)
pub fn fingerprint(sql: &str) -> Result<String, ParseError> {
    pg_query::fingerprint(sql)
        .map(|fp| fp.hex)
        .map_err(|e| ParseError::ParseError(e.to_string()))
}

/// Quote an identifier following PostgreSQL's server-side quote_identifier()
/// rules: return it bare only when it is all lower-case ASCII letters,
/// digits, and underscores, does not start with a digit, and is not a
/// keyword outside the unreserved category; otherwise wrap it in double
/// quotes with embedded quotes doubled.
pub fn quote_identifier(ident: &str) -> String {
    let safe_chars = !ident.is_empty()
        && !ident.starts_with(|c: char| c.is_ascii_digit())
        && ident
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if safe_chars && !keyword_requires_quoting(ident) {
        return ident.to_string();
    }
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// pg_query's scanner is the same lexer the PostgreSQL server uses, so its
/// keyword classification matches server-side quote_identifier().
fn keyword_requires_quoting(ident: &str) -> bool {
    use pg_query::protobuf::KeywordKind;
    let scan =
        pg_query::scan(ident).expect("scanning a bare lower-case ASCII identifier cannot fail");
    match scan.tokens.as_slice() {
        [token] => !matches!(
            token.keyword_kind(),
            KeywordKind::NoKeyword | KeywordKind::UnreservedKeyword
        ),
        _ => false,
    }
}

#[cfg(test)]
#[path = "tests/unit/tests.rs"]
mod tests;
