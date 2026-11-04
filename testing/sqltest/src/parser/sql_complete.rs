use turso_parser::complete::{CompletionScanner, CompletionState};

/// Count the number of complete SQL statements in a string, using the rules
/// of SQLite's `sqlite3_complete()`.
///
/// This properly handles:
/// - Triggers (which contain semicolons but are one statement)
/// - String literals with various quoting styles (', ", `, [])
/// - Comments (-- and /* */)
/// - EXPLAIN CREATE TRIGGER combinations
pub fn count_sql_statements(sql: &str) -> usize {
    let mut scanner = CompletionScanner::default();
    let mut count = 0;

    for byte in sql.bytes() {
        let was_complete = scanner.state().is_complete();
        scanner.feed(&[byte]);

        // Count when we transition INTO the Start state (statement completed)
        if scanner.state().is_complete() && !was_complete {
            count += 1;
        }
    }

    // If we end in a non-terminal state but have seen content, count as incomplete statement
    // For validation purposes, we want to know if there's content even without semicolon
    if count == 0
        && !matches!(
            scanner.end_state(),
            CompletionState::Invalid | CompletionState::Start
        )
    {
        return 1;
    }

    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_simple_statements() {
        assert_eq!(count_sql_statements("SELECT 1;"), 1);
        assert_eq!(count_sql_statements("SELECT 1"), 1); // No semicolon but has content
        assert_eq!(count_sql_statements("SELECT 1; SELECT 2;"), 2);
        assert_eq!(count_sql_statements("SELECT 1; SELECT 2; SELECT 3;"), 3);
    }

    #[test]
    fn test_count_empty() {
        assert_eq!(count_sql_statements(""), 0);
        assert_eq!(count_sql_statements("   "), 0);
        assert_eq!(count_sql_statements("\n\t\n"), 0);
    }

    #[test]
    fn test_count_string_literals() {
        assert_eq!(count_sql_statements("SELECT 'a;b;c';"), 1);
        assert_eq!(count_sql_statements("SELECT 'it''s';"), 1); // Escaped quote
        assert_eq!(count_sql_statements(r#"SELECT "col;name";"#), 1);
        assert_eq!(count_sql_statements("SELECT `col;name`;"), 1);
        assert_eq!(count_sql_statements("SELECT [col;name];"), 1);
        assert_eq!(count_sql_statements("'a;b'"), 1);
    }

    #[test]
    fn test_count_comments() {
        assert_eq!(count_sql_statements("SELECT 1; -- comment;"), 1);
        assert_eq!(count_sql_statements("SELECT /* ; */ 1;"), 1);
        assert_eq!(count_sql_statements("-- just a comment"), 0);
    }

    #[test]
    fn test_count_trigger_single_statement() {
        let trigger = r#"
            CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
            END;
        "#;
        assert_eq!(count_sql_statements(trigger), 1);
    }

    #[test]
    fn test_count_trigger_with_multiple_internal_statements() {
        let trigger = r#"
            CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
                UPDATE stats SET count = count + 1;
            END;
        "#;
        // This is still ONE statement (the CREATE TRIGGER)
        assert_eq!(count_sql_statements(trigger), 1);
    }

    #[test]
    fn test_count_trigger_followed_by_select() {
        let sql = r#"
            CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
            END;
            SELECT 1;
        "#;
        assert_eq!(count_sql_statements(sql), 2);
    }

    #[test]
    fn test_count_create_temp_trigger() {
        let trigger = r#"
            CREATE TEMP TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
            END;
        "#;
        assert_eq!(count_sql_statements(trigger), 1);
    }

    #[test]
    fn test_count_explain_create_trigger() {
        let trigger = r#"
            EXPLAIN CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
            END;
        "#;
        assert_eq!(count_sql_statements(trigger), 1);
    }

    #[test]
    fn test_count_end_in_string_inside_trigger() {
        // END inside a string shouldn't end the trigger
        let trigger = r#"
            CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('END');
            END;
        "#;
        assert_eq!(count_sql_statements(trigger), 1);
    }

    #[test]
    fn test_count_create_table_not_trigger() {
        assert_eq!(count_sql_statements("CREATE TABLE foo (id INT);"), 1);
        assert_eq!(
            count_sql_statements("CREATE TABLE foo (id INT); CREATE TABLE bar (id INT);"),
            2
        );
    }
}
