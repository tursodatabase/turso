//! Tests for invalid PostgreSQL SQL that should fail to parse

use turso_parser_pg::parse;

#[test]
fn test_invalid_syntax() {
    let queries = vec![
        // Note: "SELECT FROM users" is valid PostgreSQL (returns rows with no columns)
        "SELECT * FORM users",       // Typo in FROM
        "SELECT * FROM",             // Missing table
        "SELECT * FROM users WHERE", // Incomplete WHERE
        "INSERT INTO users",         // Missing VALUES or SELECT
        "UPDATE users",              // Missing SET
        "DELETE users",              // Missing FROM
        "CREATE TABLE",              // Missing table name
        "DROP",                      // Incomplete DROP
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_dollar_parameters() {
    let queries = vec![
        "SELECT * FROM users WHERE id = $", // Missing parameter number
        // Note: $0 is valid in PostgreSQL (it refers to the function's own return value in PL/pgSQL)
        "SELECT * FROM users WHERE id = $abc", // Non-numeric parameter
        "SELECT * FROM users WHERE id = $-1",  // Negative parameter
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_unterminated_strings() {
    let queries = vec![
        "SELECT 'unterminated",
        "SELECT \"unterminated identifier",
        "SELECT $tag$unterminated dollar quote",
        "SELECT $$unterminated",
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_mismatched_parentheses() {
    let queries = vec![
        "SELECT * FROM users WHERE (id = 1",
        "SELECT * FROM users WHERE id = 1)",
        "INSERT INTO users (name, email VALUES ('test', 'test@example.com')",
        "CREATE TABLE users (id INTEGER",
        "SELECT ARRAY[1, 2, 3",
        "SELECT func(1, 2",
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_type_casts() {
    let queries = vec![
        "SELECT 'test':",   // Single colon
        "SELECT 'test'::",  // Double colon without type
        "SELECT ::integer", // Missing expression
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_json_operators() {
    let queries = vec![
        "SELECT data->",                // Incomplete operator
        "SELECT data#>",                // Incomplete operator
        "SELECT ->>'field' FROM table", // Missing left operand
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_on_conflict() {
    let queries = vec![
        "INSERT INTO users VALUES (1) ON CONFLICT", // Missing action
        "INSERT INTO users VALUES (1) ON CONFLICT DO", // Incomplete DO
        "INSERT INTO users VALUES (1) ON CONFLICT (email)", // Missing DO clause
        "INSERT INTO users VALUES (1) ON CONFLICT DO SOMETHING", // Invalid action
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_window_functions() {
    let queries = vec![
        "SELECT ROW_NUMBER() OVER",             // Missing parentheses
        "SELECT ROW_NUMBER() OVER (",           // Unclosed parentheses
        "SELECT ROW_NUMBER() OVER (PARTITION)", // Missing BY
        "SELECT ROW_NUMBER() OVER (ORDER)",     // Missing BY
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_cte() {
    let queries = vec![
        "WITH AS (SELECT * FROM users) SELECT * FROM cte", // Missing CTE name
        "WITH cte SELECT * FROM cte",                      // Missing AS
        "WITH cte AS SELECT * FROM users",                 // Missing parentheses
        "WITH cte AS () SELECT * FROM cte",                // Empty CTE
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_case() {
    let queries = vec![
        "SELECT CASE WHEN true THEN 1",       // Missing END
        "SELECT CASE WHEN true 1 END",        // Missing THEN
        "SELECT CASE true THEN 1 END",        // WHEN required for searched CASE
        "SELECT CASE status WHEN THEN 1 END", // Missing value after WHEN
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_create_table() {
    let queries = vec![
        "CREATE TABLE users", // Missing columns
        // Note: "CREATE TABLE users ()" is valid PostgreSQL (creates table with no columns)
        "CREATE TABLE users (id INTEGER,)", // Trailing comma
                                            // Note: "CREATE TABLE users (PRIMARY KEY (id))" is valid PostgreSQL (table constraint only)
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_invalid_nulls_ordering() {
    let queries = vec![
        "SELECT * FROM users ORDER BY name NULLS", // Missing FIRST/LAST
        "SELECT * FROM users ORDER BY name NULLS MIDDLE", // Invalid option
    ];

    for sql in queries {
        assert!(parse(sql).is_err(), "Should have failed to parse: {sql}");
    }
}

#[test]
fn test_sqlite_specific_syntax() {
    // These are SQLite-specific and should fail in PostgreSQL parser
    let queries = vec![
        "ATTACH DATABASE 'file.db' AS other",
        "CREATE VIRTUAL TABLE ft USING fts5(content)",
        "PRAGMA table_info(users)",
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT)", // PostgreSQL uses SERIAL
        "SELECT * FROM users LIMIT 10 OFFSET 20", // SQLite allows this, PostgreSQL requires OFFSET before LIMIT or LIMIT n OFFSET m
    ];

    // Note: Some of these might actually parse depending on how lenient we make the parser
    // This is more about documenting differences
    for sql in queries {
        println!("Testing SQLite-specific syntax: {sql}");
        let result = parse(sql);
        // We don't strictly require these to fail, just document the behavior
        if result.is_err() {
            println!("  -> Correctly rejected SQLite syntax");
        } else {
            println!("  -> Accepted (may need stricter validation)");
        }
    }
}

// Additional tests based on PostgreSQL regression test patterns

#[test]
fn test_missing_statement_features() {
    // These should fail because they use features not yet implemented
    let queries = vec![
        "\\getenv var value",          // psql meta-commands should not parse as SQL
        "COPY users FROM :'variable'", // Variable interpolation
        "\\set var value",             // psql commands
        "\\d users",                   // psql describe
    ];

    for sql in queries {
        assert!(
            parse(sql).is_err(),
            "Should fail to parse psql command: {sql}"
        );
    }
}

#[test]
fn test_complex_invalid_syntax() {
    let queries = vec![
        "SELECT max(row(a,b)) FROM", // Incomplete FROM
        // Note: "explain (verbose, costs off) select" is valid PostgreSQL (EXPLAIN of a SELECT with no target list)
        "array(select sum(x+y) s",  // Incomplete array expression
        "SELECT '{1,2,3}'::int[",   // Incomplete array cast
        "CREATE TEMPORARY TABLE (", // Incomplete CREATE
    ];

    for sql in queries {
        assert!(
            parse(sql).is_err(),
            "Should fail to parse invalid syntax: {sql}"
        );
    }
}

#[test]
fn test_postgresql_unsupported_admin_commands() {
    // These PostgreSQL administrative commands should not be supported in a basic parser
    let queries = vec![
        "VACUUM",
        "REINDEX TABLE users",
        "CLUSTER users USING users_idx",
        "CHECKPOINT",
        "LOAD 'extension'",
    ];

    for sql in queries {
        let result = parse(sql);
        // These may or may not fail depending on implementation - document behavior
        {
            let status = if result.is_ok() { "OK" } else { "FAIL" };
            println!("Testing admin command: {sql} -> {status}");
        }
    }
}

#[test]
fn test_invalid_operators() {
    let queries = vec![
        // Note: "SELECT 1 ++ 2" is valid PostgreSQL (unary + applied to +2)
        // Note: "SELECT 1 <=> 2" parses in PostgreSQL (spaceship-like operator syntax is accepted)
        // Note: "SELECT name ~~ pattern" is valid PostgreSQL (~~ is the internal LIKE operator)
        "SELECT array @>", // Incomplete operator
    ];

    for sql in queries {
        assert!(
            parse(sql).is_err(),
            "Should fail to parse invalid operator: {sql}"
        );
    }
}
