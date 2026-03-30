use std::ops::ControlFlow;

use itertools::Itertools;

/// State machine for determining if a SQL statement is complete.
/// Based on SQLite's `sqlite3_complete()` from src/complete.c
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
                    // Consumes all tokens between the delimeters
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
    /// Returns true if the state machine is in a "complete" state,
    /// meaning the accumulated SQL forms a complete statement.
    pub fn is_complete(&self) -> bool {
        matches!(self, ReadState::Start)
    }

    // Copied form SQLite
    /// Process a single character and return the new state.
    /// This should be called for each character in the input.
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

    /// Process a SQL string and update the state.
    /// Returns the new state after processing all input.
    pub fn process(&mut self, sql: &str) {
        let chars = sql.chars().peekable();

        *self = Tokenizer::new(chars).fold(*self, |state, token| state.transition(token));
    }
}

#[cfg(test)]
mod tests {
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
}

// create virtual table t1 using csv(data=\"12');
