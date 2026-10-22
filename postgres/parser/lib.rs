pub use pg_query::ParseResult;
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

/// `sql` with `schema` in front of the table name that starts at byte
/// offset `location`.
pub fn qualify_create_table_name(sql: &str, location: usize, schema: &str) -> String {
    qualify_table_names(sql, &[(location, schema.to_string())])
}

/// `sql` with each schema of `names` in front of the table name that starts
/// at the byte offset given with it.
pub fn qualify_table_names(sql: &str, names: &[(usize, String)]) -> String {
    let mut names: Vec<&(usize, String)> = names.iter().collect();
    names.sort_by_key(|(location, _)| std::cmp::Reverse(*location));
    let mut sql = sql.to_string();
    for (location, schema) in names {
        sql.insert_str(*location, &format!("{}.", quote_identifier(schema)));
    }
    sql
}

/// `sql` with the index name `name` before the `ON` keyword of a CREATE
/// INDEX statement, which starts at byte offset `location`.
pub fn name_create_index(sql: &str, location: usize, name: &str) -> String {
    let mut sql = sql.to_string();
    sql.insert_str(location, &format!("{} ", quote_identifier(name)));
    sql
}

/// `sql`, a CREATE TABLE statement, with its table named `name` and no
/// schema. `sql` is returned as it is when the table is already called
/// `name` or when `sql` is not a CREATE TABLE statement.
pub fn name_create_table(sql: &str, name: &str) -> Result<String, ParseError> {
    use pg_query::protobuf::Token;
    use pg_query::NodeRef;

    let parse_result = parse(sql)?;
    let nodes = parse_result.protobuf.nodes();
    let Some((NodeRef::CreateStmt(create), ..)) = nodes.first() else {
        return Ok(sql.to_string());
    };
    let Some(relation) = create.relation.as_ref() else {
        return Ok(sql.to_string());
    };
    let start = relation.location as usize;
    let scan = pg_query::scan(sql).map_err(|e| ParseError::ParseError(e.to_string()))?;
    let mut tokens = scan
        .tokens
        .iter()
        .skip_while(|token| (token.start as usize) < start);
    let first = tokens
        .next()
        .ok_or_else(|| ParseError::ParseError("CREATE TABLE names no table".to_string()))?;
    let mut end = first.end as usize;
    let mut rest = tokens.peekable();
    if rest
        .peek()
        .is_some_and(|token| token.token == Token::Ascii46 as i32)
    {
        rest.next();
        if let Some(ident) = rest.next() {
            end = ident.end as usize;
        }
    }
    Ok(format!(
        "{}{}{}",
        &sql[..start],
        quote_identifier(name),
        &sql[end..]
    ))
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
mod tests {
    use super::*;

    #[test]
    fn name_create_table_writes_the_stored_name_over_the_written_one() {
        assert_eq!(
            name_create_table("CREATE TABLE t (x int)", "app.t").unwrap(),
            "CREATE TABLE \"app.t\" (x int)"
        );
        assert_eq!(
            name_create_table("CREATE TABLE \"app\".t(x int)", "app.t").unwrap(),
            "CREATE TABLE \"app.t\"(x int)"
        );
        assert_eq!(
            name_create_table("create table if not exists app . \"T\" (x int)", "app.T").unwrap(),
            "create table if not exists \"app.T\" (x int)"
        );
        assert_eq!(
            name_create_table("CREATE TABLE t (x int)", "t").unwrap(),
            "CREATE TABLE t (x int)"
        );
        assert_eq!(name_create_table("SELECT 1", "t").unwrap(), "SELECT 1");
    }

    #[test]
    fn unqualified_foreign_key_targets_lists_the_tables_named_without_a_schema() {
        let parse_result = parse(
            "CREATE TABLE app.c (a int REFERENCES p (id), b int REFERENCES app.q (id), \
             FOREIGN KEY (a, b) REFERENCES r (x, y))",
        )
        .unwrap();
        let foreign_keys = translator::unqualified_foreign_key_targets(&parse_result).unwrap();
        assert_eq!(foreign_keys.table, "c");
        assert_eq!(foreign_keys.schema.as_deref(), Some("app"));
        let names: Vec<&str> = foreign_keys
            .targets
            .iter()
            .map(|(_, name)| name.as_str())
            .collect();
        assert_eq!(names, ["p", "r"]);
        let sql = "CREATE TABLE c (a int REFERENCES p (id), FOREIGN KEY (a) REFERENCES r (x))";
        let parse_result = parse(sql).unwrap();
        let targets = translator::unqualified_foreign_key_targets(&parse_result)
            .unwrap()
            .targets;
        let schemas: Vec<(usize, String)> = targets
            .iter()
            .zip(["app", "a.b"])
            .map(|((location, _), schema)| (*location, schema.to_string()))
            .collect();
        assert_eq!(
            qualify_table_names(sql, &schemas),
            "CREATE TABLE c (a int REFERENCES app.p (id), FOREIGN KEY (a) REFERENCES \"a.b\".r (x))"
        );
        assert!(translator::unqualified_foreign_key_targets(&parse("SELECT 1").unwrap()).is_none());
    }

    #[test]
    fn name_create_table_drops_the_database_of_a_name_that_is_already_the_stored_name() {
        assert_eq!(
            name_create_table(
                "CREATE TABLE main.\"pg_turso_schema_app\" (schema_name TEXT)",
                "pg_turso_schema_app"
            )
            .unwrap(),
            "CREATE TABLE pg_turso_schema_app (schema_name TEXT)"
        );
        assert_eq!(
            name_create_table("CREATE TABLE public.t2(a INT)", "t2").unwrap(),
            "CREATE TABLE t2(a INT)"
        );
    }

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
