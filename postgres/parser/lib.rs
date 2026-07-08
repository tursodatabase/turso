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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let result = parse(sql);
        assert!(result.is_ok());

        let tables = get_tables(sql).unwrap();
        assert_eq!(tables, vec!["users"]);
    }

    #[test]
    fn test_parse_complex_query() {
        let sql = "WITH regional_sales AS (
            SELECT region, SUM(amount) AS total_sales
            FROM orders
            GROUP BY region
        )
        SELECT * FROM regional_sales ORDER BY total_sales DESC";

        assert!(parse(sql).is_ok());
    }

    #[test]
    fn test_normalize() {
        let sql = "SELECT * FROM users WHERE age > 25 AND name = 'John'";
        let normalized = normalize(sql).unwrap();
        assert!(normalized.contains("$1"));
        assert!(normalized.contains("$2"));
    }

    #[test]
    fn test_parse_postgresql_specific() {
        // Test PostgreSQL-specific syntax that we struggled with before
        let queries = vec![
            "SELECT * FROM users ORDER BY name USING >",
            "SELECT * FROM person* p",
            "VALUES (1,2), (3,4)",
            "SELECT foo FROM (SELECT 1) AS foo",
            "INSERT INTO users (name, data) VALUES ('John', '{\"key\": \"value\"}'::jsonb)",
            "SELECT * FROM users WHERE data @> '{\"active\": true}'",
            "UPDATE users SET (name, age) = ('John', 30) WHERE id = 1",
            "CREATE TABLE posts PARTITION OF main_posts FOR VALUES IN (1, 2, 3)",
            "SELECT COUNT(*) FILTER (WHERE active) FROM users",
            "SELECT DISTINCT ON (region) * FROM sales ORDER BY region, amount DESC",
        ];

        for sql in queries {
            let result = parse(sql);
            assert!(result.is_ok(), "Failed to parse: {sql}");
        }
    }
}
