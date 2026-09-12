use super::*;

fn is_complete(sql: &str) -> bool {
    let mut state = ReadState::default();
    state.process(sql);
    state.is_complete()
}

#[test]
fn test_simple_statements() {
    assert!(is_complete("SELECT 1;"));
    assert!(is_complete("SELECT * FROM foo;"));
    assert!(is_complete("INSERT INTO foo VALUES (1, 2, 3);"));
    assert!(!is_complete("SELECT 1"));
    assert!(!is_complete("SELECT * FROM"));
}

#[test]
fn test_multiple_statements() {
    assert!(is_complete("SELECT 1; SELECT 2;"));
    assert!(!is_complete("SELECT 1; SELECT 2"));
}

#[test]
fn test_string_with_semicolon() {
    assert!(!is_complete("SELECT ';'"));
    assert!(is_complete("SELECT ';';"));
    assert!(!is_complete("SELECT 'test;test'"));
    assert!(is_complete("SELECT 'test;test';"));
}

#[test]
fn test_comments() {
    assert!(is_complete("SELECT 1; -- comment"));
    assert!(!is_complete("SELECT 1 -- comment;"));
    assert!(is_complete("SELECT /* ; */ 1;"));
    assert!(!is_complete("SELECT 1 /* ; */"));
}

#[test]
fn test_simple_trigger() {
    let trigger = r#"
            CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
            END;
        "#;
    assert!(is_complete(trigger));
}

#[test]
fn test_trigger_incomplete() {
    let trigger = r#"
            CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
        "#;
    assert!(!is_complete(trigger));
}

#[test]
fn test_trigger_multiple_statements() {
    let trigger = r#"
            CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
                UPDATE stats SET count = count + 1;
            END;
        "#;
    assert!(is_complete(trigger));
}

#[test]
fn test_create_temp_trigger() {
    let trigger = r#"
            CREATE TEMP TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
            END;
        "#;
    assert!(is_complete(trigger));
}

#[test]
fn test_create_temporary_trigger() {
    let trigger = r#"
            CREATE TEMPORARY TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
            END;
        "#;
    assert!(is_complete(trigger));
}

#[test]
fn test_explain_create_trigger() {
    let trigger = r#"
            EXPLAIN CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('inserted');
            END;
        "#;
    assert!(is_complete(trigger));
}

#[test]
fn test_end_in_string_inside_trigger() {
    // END inside a string shouldn't end the trigger
    let trigger = r#"
            CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
                INSERT INTO log VALUES('END');
            END;
        "#;
    assert!(is_complete(trigger));
}

#[test]
fn test_create_table_not_trigger() {
    assert!(is_complete("CREATE TABLE foo (id INT);"));
    assert!(!is_complete("CREATE TABLE foo (id INT)"));
}

#[test]
fn test_empty_and_whitespace() {
    assert!(!is_complete(""));
    assert!(!is_complete("   "));
    assert!(!is_complete("\n\t\n"));
    assert!(is_complete(";"));
    assert!(is_complete("  ;  "));
}

#[test]
fn test_quoted_identifiers() {
    assert!(is_complete(r#"SELECT "column;name" FROM foo;"#));
    assert!(is_complete("SELECT `column;name` FROM foo;"));
    assert!(is_complete("SELECT [column;name] FROM foo;"));
}

#[test]
fn test_escaped_quotes() {
    assert!(is_complete("SELECT 'it''s';"));
    assert!(is_complete(r#"SELECT "col""name";"#));
}

#[test]
fn test_non_terminated_literal() {
    assert!(!is_complete(
        "create virtual table t1 using csv(data=\"12');"
    ));
}
