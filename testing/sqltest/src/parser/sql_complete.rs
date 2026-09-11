use std::ops::ControlFlow;

use itertools::Itertools;

/// State machine for determining if a SQL statement is complete.
/// Based on SQLite's `sqlite3_complete()` from src/complete.c
///
/// Copied from `cli/read_state_machine.rs`
///
/// This handles the tricky case of triggers which contain semicolons
/// in their body but should only be considered complete when the
/// `;END;` pattern is seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadState {
    /// No non-whitespace seen yet (initial state)
    #[default]
    Invalid,
    /// A complete statement was just finished (terminal state)
    Start,
    /// In the middle of an ordinary statement
    Normal,
    /// Saw EXPLAIN at the start, watching for CREATE
    Explain,
    /// Saw CREATE (possibly after EXPLAIN), watching for TRIGGER
    Create,
    /// Inside a trigger definition, need ;END; to escape
    Trigger,
    /// Just saw a semicolon inside a trigger, looking for END
    Semi,
    /// Saw ;END in trigger, one more semicolon completes it
    End,
}

/// Token types recognized by the state machine
#[expect(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    TkSemi,
    TkWhitespace,
    TkOther,
    TkExplain,
    TkCreate,
    TkTemp,
    TkTrigger,
    TkEnd,
}

struct Tokenizer<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
}

impl<'a> Tokenizer<'a> {
    fn new(chars: std::iter::Peekable<std::str::Chars<'a>>) -> Self {
        Self { chars }
    }

    /// Read an identifier/keyword and classify it
    fn read_keyword(&mut self, first: char) -> Token {
        let word: String = std::iter::once(first)
            .chain(
                self.chars
                    .peeking_take_while(|c| c.is_ascii_alphanumeric() || *c == '_'),
            )
            .collect();

        match word.to_ascii_uppercase().as_str() {
            "EXPLAIN" => Token::TkExplain,
            "CREATE" => Token::TkCreate,
            "TEMP" | "TEMPORARY" => Token::TkTemp,
            "TRIGGER" => Token::TkTrigger,
            "END" => Token::TkEnd,
            _ => Token::TkOther,
        }
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        loop {
            let c = self.chars.next()?;

            let token = match c {
                '\'' | '"' | '`' | '[' => {
                    let end_char = if c == '[' { ']' } else { c };
                    // Consumes all tokens between the delimiters
                    self.chars
                        .by_ref()
                        .take_while_inclusive(|&ch| ch != end_char)
                        .for_each(drop);
                    continue;
                }
                // Handle Comments
                '-' if self.chars.peek() == Some(&'-') => {
                    self.chars.next(); // Consume second `-`
                    // Consume until you find a new line
                    self.chars.by_ref().find(|&ch| ch == '\n');
                    continue;
                }
                '/' if self.chars.peek() == Some(&'*') => {
                    // Consumes until you find a `*/`
                    let _ = self.chars.by_ref().try_fold(false, |saw_star, c| {
                        if saw_star && c == '/' {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(c == '*')
                        }
                    });
                    continue;
                }
                ';' => Token::TkSemi,
                c if c.is_ascii_whitespace() => Token::TkWhitespace,
                c if c.is_ascii_alphabetic() || c == '_' => self.read_keyword(c),
                _ => Token::TkOther,
            };

            break Some(token);
        }
    }
}

impl ReadState {
    /// Process a single token and return the new state.
    fn transition(&self, token: Token) -> ReadState {
        use ReadState::*;
        use Token::*;

        match (self, token) {
            // State 0: INVALID - nothing meaningful seen yet
            (Invalid, TkSemi) => Start,
            (Invalid, TkWhitespace) => Invalid,
            (Invalid, TkOther) => Normal,
            (Invalid, TkExplain) => Explain,
            (Invalid, TkCreate) => Create,
            (Invalid, TkTemp) => Normal,
            (Invalid, TkTrigger) => Normal,
            (Invalid, TkEnd) => Normal,

            // State 1: START - complete statement, ready for new one
            (Start, TkSemi) => Start,
            (Start, TkWhitespace) => Start,
            (Start, TkOther) => Normal,
            (Start, TkExplain) => Explain,
            (Start, TkCreate) => Create,
            (Start, TkTemp) => Normal,
            (Start, TkTrigger) => Normal,
            (Start, TkEnd) => Normal,

            // State 2: NORMAL - in middle of ordinary statement
            (Normal, TkSemi) => Start,
            (Normal, TkWhitespace) => Normal,
            (Normal, _) => Normal,

            // State 3: EXPLAIN - saw EXPLAIN, watching for CREATE
            (Explain, TkSemi) => Start,
            (Explain, TkWhitespace) => Explain,
            (Explain, TkOther) => Explain,
            (Explain, TkExplain) => Normal,
            (Explain, TkCreate) => Create,
            (Explain, TkTemp) => Normal,
            (Explain, TkTrigger) => Normal,
            (Explain, TkEnd) => Normal,

            // State 4: CREATE - saw CREATE, watching for TRIGGER
            (Create, TkSemi) => Start,
            (Create, TkWhitespace) => Create,
            (Create, TkOther) => Normal,
            (Create, TkExplain) => Normal,
            (Create, TkCreate) => Normal,
            (Create, TkTemp) => Create,     // CREATE TEMP still watching
            (Create, TkTrigger) => Trigger, // Enter trigger mode!
            (Create, TkEnd) => Normal,

            // State 5: TRIGGER - inside trigger body, need ;END; to escape
            (Trigger, TkSemi) => Semi,
            (Trigger, TkWhitespace) => Trigger,
            (Trigger, _) => Trigger,

            // State 6: SEMI - saw ; in trigger, looking for END
            (Semi, TkSemi) => Semi,
            (Semi, TkWhitespace) => Semi,
            (Semi, TkEnd) => End,
            (Semi, _) => Trigger, // false alarm, back to body

            // State 7: END - saw ;END, one more ; completes
            (End, TkSemi) => Start, // ;END; - COMPLETE!
            (End, TkWhitespace) => End,
            (End, _) => Trigger, // false alarm
        }
    }
}

/// Count the number of complete SQL statements in a string.
///
/// This properly handles:
/// - Triggers (which contain semicolons but are one statement)
/// - String literals with various quoting styles (', ", `, [])
/// - Comments (-- and /* */)
/// - EXPLAIN CREATE TRIGGER combinations
pub fn count_sql_statements(sql: &str) -> usize {
    let chars = sql.chars().peekable();
    let mut state = ReadState::default();
    let mut count = 0;

    for token in Tokenizer::new(chars) {
        let new_state = state.transition(token);

        // Count when we transition INTO the Start state (statement completed)
        if new_state == ReadState::Start && state != ReadState::Start {
            count += 1;
        }

        state = new_state;
    }

    // If we end in a non-terminal state but have seen content, count as incomplete statement
    // For validation purposes, we want to know if there's content even without semicolon
    if count == 0 && !matches!(state, ReadState::Invalid | ReadState::Start) {
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
