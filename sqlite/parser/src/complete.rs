//! The rules of SQLite's `sqlite3_complete()` (`src/complete.c`): whether a
//! piece of SQL text ends with a finished statement.
//!
//! The text is split into a few kinds of tokens, which drive a small state
//! machine. Comments count as whitespace, and strings and quoted identifiers
//! count as one ordinary token. A `CREATE TRIGGER` statement only ends at
//! `;END;`, because its body contains semicolons.

/// Returns true if `sql` ends with a semicolon that is outside any string,
/// quoted identifier, comment, or unfinished `CREATE TRIGGER` body.
pub fn is_complete_statement(sql: &[u8]) -> bool {
    let mut scanner = CompletionScanner::default();
    scanner.feed(sql);
    scanner.is_complete()
}

/// Reads SQL text in pieces and tells whether the text so far ends with a
/// finished statement. A string, comment or word may be split across pieces.
#[derive(Debug, Clone, Default)]
pub struct CompletionScanner {
    state: CompletionState,
    unfinished: Unfinished,
}

impl CompletionScanner {
    pub fn feed(&mut self, sql: &[u8]) {
        for &b in sql {
            self.feed_byte(b);
        }
    }

    pub fn is_complete(&self) -> bool {
        self.end_state().is_complete()
    }

    /// The state after all tokens read so far, not counting a token that the
    /// next byte could still extend.
    pub fn state(&self) -> CompletionState {
        self.state
    }

    /// The state if the text ended here.
    pub fn end_state(&self) -> CompletionState {
        let mut scanner = self.clone();
        scanner.finish_token();
        scanner.state
    }

    fn feed_byte(&mut self, b: u8) {
        match &mut self.unfinished {
            Unfinished::Quote(close) => {
                if b == *close {
                    self.finish_token();
                }
                return;
            }
            Unfinished::BlockComment { after_star } => {
                if *after_star && b == b'/' {
                    self.unfinished = Unfinished::None;
                } else {
                    *after_star = b == b'*';
                }
                return;
            }
            Unfinished::LineComment => {
                if b == b'\n' {
                    self.unfinished = Unfinished::None;
                }
                return;
            }
            Unfinished::Slash if b == b'*' => {
                self.unfinished = Unfinished::BlockComment { after_star: false };
                return;
            }
            Unfinished::Dash if b == b'-' => {
                self.unfinished = Unfinished::LineComment;
                return;
            }
            Unfinished::Word(word) if is_id_char(b) => {
                word.push(b);
                return;
            }
            Unfinished::Slash | Unfinished::Dash | Unfinished::Word(_) => self.finish_token(),
            Unfinished::None => {}
        }
        match b {
            b';' => self.state = self.state.next(CompletionToken::Semi),
            b' ' | b'\r' | b'\t' | b'\n' | b'\x0c' => {}
            b'/' => self.unfinished = Unfinished::Slash,
            b'-' => self.unfinished = Unfinished::Dash,
            b'[' => self.unfinished = Unfinished::Quote(b']'),
            b'`' | b'"' | b'\'' => self.unfinished = Unfinished::Quote(b),
            b if is_id_char(b) => {
                let mut word = Word::default();
                word.push(b);
                self.unfinished = Unfinished::Word(word);
            }
            _ => self.state = self.state.next(CompletionToken::Other),
        }
    }

    /// Ends the unfinished token and applies it to the state. An unclosed
    /// string, quoted identifier or `/*` comment counts as an ordinary token,
    /// so text that ends inside one is never complete.
    fn finish_token(&mut self) {
        let token = match std::mem::take(&mut self.unfinished) {
            Unfinished::None | Unfinished::LineComment => return,
            Unfinished::Quote(_)
            | Unfinished::BlockComment { .. }
            | Unfinished::Slash
            | Unfinished::Dash => CompletionToken::Other,
            Unfinished::Word(word) => word.token(),
        };
        self.state = self.state.next(token);
    }
}

/// A token that the next byte could still extend or end.
#[derive(Debug, Clone, Copy, Default)]
enum Unfinished {
    #[default]
    None,
    /// A string or quoted identifier that ends at this byte.
    Quote(u8),
    /// A `/*` comment. `after_star` is true right after a `*`.
    BlockComment {
        after_star: bool,
    },
    /// A `--` comment.
    LineComment,
    /// A `/` that may start a comment.
    Slash,
    /// A `-` that may start a comment.
    Dash,
    Word(Word),
}

/// An identifier or keyword. Only its first bytes are kept, lowercased: enough
/// to recognize the longest keyword, `temporary`.
#[derive(Debug, Clone, Copy, Default)]
struct Word {
    start: [u8; 9],
    len: usize,
}

impl Word {
    fn push(&mut self, b: u8) {
        if let Some(slot) = self.start.get_mut(self.len) {
            *slot = b.to_ascii_lowercase();
        }
        self.len += 1;
    }

    fn token(&self) -> CompletionToken {
        let Some(word) = self.start.get(..self.len) else {
            return CompletionToken::Other;
        };
        match word {
            b"create" => CompletionToken::Create,
            b"trigger" => CompletionToken::Trigger,
            b"temp" | b"temporary" => CompletionToken::Temp,
            b"end" => CompletionToken::End,
            b"explain" => CompletionToken::Explain,
            _ => CompletionToken::Other,
        }
    }
}

/// Where the state machine of `sqlite3_complete()` is after some tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompletionState {
    /// Only whitespace and comments so far.
    #[default]
    Invalid,
    /// The last statement is finished.
    Start,
    /// Inside an ordinary statement.
    Normal,
    /// Inside a statement that starts with `EXPLAIN`, before any `CREATE`.
    Explain,
    /// After `CREATE` or `CREATE TEMP`, before `TRIGGER`.
    Create,
    /// Inside a `CREATE TRIGGER` statement.
    Trigger,
    /// After a semicolon inside a `CREATE TRIGGER` statement.
    Semi,
    /// After `;END` inside a `CREATE TRIGGER` statement.
    End,
}

impl CompletionState {
    pub fn is_complete(self) -> bool {
        self == CompletionState::Start
    }

    fn next(self, token: CompletionToken) -> Self {
        use CompletionState::*;
        #[rustfmt::skip]
        const TRANSITIONS: [[CompletionState; 8]; 8] = [
            /* Token:       SEMI   WS       OTHER    EXPLAIN  CREATE  TEMP     TRIGGER  END */
            /* Invalid: */ [Start, Invalid, Normal,  Explain, Create, Normal,  Normal,  Normal],
            /* Start:   */ [Start, Start,   Normal,  Explain, Create, Normal,  Normal,  Normal],
            /* Normal:  */ [Start, Normal,  Normal,  Normal,  Normal, Normal,  Normal,  Normal],
            /* Explain: */ [Start, Explain, Explain, Normal,  Create, Normal,  Normal,  Normal],
            /* Create:  */ [Start, Create,  Normal,  Normal,  Normal, Create,  Trigger, Normal],
            /* Trigger: */ [Semi,  Trigger, Trigger, Trigger, Trigger, Trigger, Trigger, Trigger],
            /* Semi:    */ [Semi,  Semi,    Trigger, Trigger, Trigger, Trigger, Trigger, End],
            /* End:     */ [Start, End,     Trigger, Trigger, Trigger, Trigger, Trigger, Trigger],
        ];
        TRANSITIONS[self as usize][token as usize]
    }
}

/// The kinds of tokens that `sqlite3_complete()` tells apart. Whitespace and
/// comments never change the state, so the scanner skips them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionToken {
    Semi,
    #[expect(dead_code, reason = "keeps the column order of SQLite's table")]
    Whitespace,
    /// Any other token, including strings and quoted identifiers.
    Other,
    Explain,
    Create,
    /// `TEMP` or `TEMPORARY`.
    Temp,
    Trigger,
    End,
}

/// SQLite's `IdChar()`: letters, digits, `_`, `$` and every non-ASCII byte.
fn is_id_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_complete_statement_matches_sqlite() {
        let cases: &[(&str, bool)] = &[
            ("SELECT 1", false),
            ("SELECT 1;", true),
            ("", false),
            ("   ", false),
            (";", true),
            ("SELECT 4/2;", true),
            ("SELECT 1; - 1", false),
            // Comments after the semicolon count as whitespace.
            ("SELECT 1; -- trailing comment", true),
            ("SELECT 1; -- comment\n", true),
            ("SELECT 1; -- comment\nSELECT 2", false),
            ("SELECT 1 -- comment;", false),
            ("-- only a comment", false),
            ("SELECT 1; /* block */", true),
            ("SELECT /* ; */ 1;", true),
            ("SELECT 1 /* ; */", false),
            ("SELECT 1; /* unterminated", false),
            ("SELECT 1; /*/", false),
            ("SELECT 1; /**/", true),
            ("SELECT 1; /* a **/", true),
            ("SELECT 1; -", false),
            ("SELECT 1; /", false),
            ("SELECT 1; temporary_x", false),
            // Strings and quoted identifiers are ordinary tokens.
            ("SELECT ';'", false),
            ("SELECT 'it''s';", true),
            ("SELECT \"a;b\";", true),
            ("SELECT `a;b`;", true),
            ("SELECT [a;b];", true),
            ("SELECT 1; 'abc'", false),
            ("SELECT 1; 'unterminated", false),
            ("SELECT 1; [a", false),
            ("SELECT 'a;\nb';", true),
            ("create virtual table t1 using csv(data=\"12');", false),
            // A trigger body only ends at `;END;`.
            ("CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1;", false),
            (
                "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END;",
                true,
            ),
            (
                "create trigger t after insert on x begin select 1; end;",
                true,
            ),
            (
                "CREATE TEMP TRIGGER t AFTER INSERT ON x BEGIN SELECT 1;",
                false,
            ),
            (
                "CREATE TEMPORARY TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END;",
                true,
            ),
            (
                "EXPLAIN CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1;",
                false,
            ),
            (
                "EXPLAIN CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END;",
                true,
            ),
            (
                "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 'END;'; END;",
                true,
            ),
            (
                "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END_X;",
                false,
            ),
            (
                "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END$;",
                false,
            ),
            (
                "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END\u{e9};",
                false,
            ),
            (
                "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; /* c */ END -- c\n;",
                true,
            ),
            ("CREATE TABLE trigger(a);", true),
            (
                "SELECT 1; CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1;",
                false,
            ),
        ];
        for &(sql, expected) in cases {
            assert_eq!(
                is_complete_statement(sql.as_bytes()),
                expected,
                "is_complete_statement({sql:?})"
            );
            for split in 0..=sql.len() {
                let (head, tail) = sql.as_bytes().split_at(split);
                let mut scanner = CompletionScanner::default();
                scanner.feed(head);
                scanner.feed(tail);
                assert_eq!(
                    scanner.is_complete(),
                    expected,
                    "{sql:?} fed in two pieces split at byte {split}"
                );
            }
        }
    }
}
