use itertools::Itertools;

/// Whether the input read so far is a complete statement. The input comes
/// in pieces, such as lines, and a quoted string or a comment can start in
/// one piece and end in a later one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadState {
    statement: StatementState,
    quote: Quote,
}

/// State machine for determining if a SQL statement is complete.
/// Based on SQLite's `sqlite3_complete()` from src/complete.c
///
/// This handles the tricky case of triggers which contain semicolons
/// in their body but should only be considered complete when the
/// `;END;` pattern is seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum StatementState {
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

/// The quoted string or comment that the input read so far ends inside.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum Quote {
    #[default]
    None,
    /// A string or identifier that the character `end` closes.
    Delimited {
        end: char,
    },
    /// A dollar-quoted string that `tag`, such as `$$` or `$body$`, closes.
    /// `matched` is how many characters of the tag the input ends with.
    Dollar {
        tag: Vec<char>,
        matched: usize,
    },
    LineComment,
    BlockComment {
        after_star: bool,
    },
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

type Chars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

impl ReadState {
    /// Returns true if the state machine is in a "complete" state,
    /// meaning the accumulated SQL forms a complete statement.
    pub fn is_complete(&self) -> bool {
        // A line comment that the input ends inside cannot hide a semicolon
        // that is still to come.
        matches!(self.quote, Quote::None | Quote::LineComment)
            && self.statement == StatementState::Start
    }

    /// Reads the next piece of the input.
    pub fn process(&mut self, sql: &str) {
        let mut chars = sql.chars().peekable();
        while self.skip_quote(&mut chars) {
            let Some(c) = chars.next() else {
                return;
            };
            let token = match c {
                '\'' | '"' | '`' => {
                    self.quote = Quote::Delimited { end: c };
                    continue;
                }
                '[' => {
                    self.quote = Quote::Delimited { end: ']' };
                    continue;
                }
                '$' => match read_dollar_tag(&mut chars) {
                    Some(tag) => {
                        self.quote = Quote::Dollar { tag, matched: 0 };
                        continue;
                    }
                    // Not a dollar-quote (e.g. $1 positional param) — treat as other
                    None => Token::TkOther,
                },
                '-' if chars.peek() == Some(&'-') => {
                    chars.next();
                    self.quote = Quote::LineComment;
                    continue;
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    self.quote = Quote::BlockComment { after_star: false };
                    continue;
                }
                ';' => Token::TkSemi,
                c if c.is_ascii_whitespace() => Token::TkWhitespace,
                c if c.is_ascii_alphabetic() || c == '_' => read_keyword(c, &mut chars),
                _ => Token::TkOther,
            };
            self.statement = self.statement.transition(token);
        }
    }

    /// Reads to the end of the quoted string or comment that the input is
    /// inside, if any. Returns false when the input ends first.
    fn skip_quote(&mut self, chars: &mut Chars<'_>) -> bool {
        while self.quote != Quote::None {
            let Some(c) = chars.next() else {
                return false;
            };
            let closed = match &mut self.quote {
                Quote::None => unreachable!("the loop runs only inside a quote"),
                Quote::Delimited { end } => c == *end,
                Quote::LineComment => c == '\n',
                Quote::BlockComment { after_star } => {
                    let closed = *after_star && c == '/';
                    *after_star = c == '*';
                    closed
                }
                Quote::Dollar { tag, matched } => {
                    // Only the first and the last character of a tag are
                    // `$`, so a mismatch can start a new match only at a `$`.
                    if c == tag[*matched] {
                        *matched += 1;
                    } else {
                        *matched = usize::from(c == '$');
                    }
                    *matched == tag.len()
                }
            };
            if closed {
                self.quote = Quote::None;
            }
        }
        true
    }
}

impl StatementState {
    // Copied form SQLite
    /// Process a single character and return the new state.
    /// This should be called for each character in the input.
    fn transition(self, token: Token) -> StatementState {
        use StatementState::*;
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

/// Read an identifier/keyword and classify it
fn read_keyword(first: char, chars: &mut Chars<'_>) -> Token {
    let word: String = std::iter::once(first)
        .chain(chars.peeking_take_while(|c| c.is_ascii_alphanumeric() || *c == '_'))
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

/// Reads the rest of a dollar-quote tag after its first `$`, such as `$` or
/// `body$`, and returns the whole tag. Returns None when the `$` does not
/// start a tag, as in the parameter `$1`.
fn read_dollar_tag(chars: &mut Chars<'_>) -> Option<Vec<char>> {
    let mut tag = vec!['$'];
    loop {
        match chars.peek() {
            Some(&'$') => {
                tag.push(chars.next().unwrap());
                return Some(tag);
            }
            Some(&ch)
                if ch == '_'
                    || ch.is_ascii_alphabetic()
                    || (tag.len() > 1 && ch.is_ascii_digit()) =>
            {
                tag.push(chars.next().unwrap());
            }
            _ => return None,
        }
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

    #[test]
    fn test_dollar_quoted_string() {
        assert!(is_complete("SELECT $$hello;world$$;"));
        assert!(!is_complete("SELECT $$hello;world$$"));
    }

    #[test]
    fn test_create_function_dollar_quoted() {
        let func = r#"
            CREATE FUNCTION test() RETURNS integer AS $$
            BEGIN
                RETURN 1;
            END;
            $$ LANGUAGE plpgsql;
        "#;
        assert!(is_complete(func));
    }

    #[test]
    fn test_create_function_incomplete_dollar_quote() {
        let func = r#"
            CREATE FUNCTION test() RETURNS integer AS $$
            BEGIN
                RETURN 1;
            END;
        "#;
        assert!(!is_complete(func));
    }

    #[test]
    fn test_dollar_quoted_with_tag() {
        assert!(is_complete("SELECT $body$hello;world$body$;"));
        assert!(!is_complete("SELECT $body$hello;world$body$"));
        // Mismatched tags — inner $fn$ doesn't close $body$
        assert!(!is_complete("SELECT $body$hello;world$fn$;"));
    }

    #[test]
    fn test_dollar_sign_not_quote() {
        // Positional params are not dollar-quotes
        assert!(is_complete("SELECT $1;"));
        assert!(!is_complete("SELECT $1"));
    }

    /// Whether the input is complete after each of `pieces`, read one
    /// after the other.
    fn is_complete_after_each(pieces: &[&str]) -> Vec<bool> {
        let mut state = ReadState::default();
        pieces
            .iter()
            .map(|piece| {
                state.process(piece);
                state.is_complete()
            })
            .collect()
    }

    #[test]
    fn test_quote_over_several_pieces() {
        assert_eq!(
            is_complete_after_each(&["SELECT 'a\n", "b;\n", "c';\n"]),
            [false, false, true]
        );
        assert_eq!(
            is_complete_after_each(&["SELECT $body$a\n", "b;\n", "$body;$\n", "$body$;\n"]),
            [false, false, false, true]
        );
        assert_eq!(
            is_complete_after_each(&["SELECT 1 /* a\n", "b; */\n", ";\n"]),
            [false, false, true]
        );
        assert_eq!(
            is_complete_after_each(&["SELECT 1 -- a;\n", ";\n"]),
            [false, true]
        );
    }

    #[test]
    fn test_dollar_tag_after_a_partial_match() {
        assert!(is_complete("SELECT $a$x$$a$;"));
        assert!(!is_complete("SELECT $ab$x$a$;"));
    }
}

// create virtual table t1 using csv(data=\"12');
