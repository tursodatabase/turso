//! Finds the encryption keys in SQL text that the PostgreSQL frontend gets,
//! so that core can hide them when it writes the text to its log.

use std::ops::Range;
use turso_core::Dialect;
use turso_parser::{lexer::Lexer, token::TokenType};

/// Returns the byte ranges of the keys in `sql`.
///
/// The statements that carry a key are SQLite statements: PRAGMA, ATTACH and
/// the table functions of PRAGMAs. pg_query rejects PRAGMA and ATTACH, and it
/// reads all the rest of the text, so the frontend runs such a statement, and
/// each statement before it, through the SQLite parser. The SQLite scan reads
/// that text as the SQLite parser does, so it finds their keys.
///
/// The PostgreSQL lexer reads some text in another way than the SQLite lexer.
/// After such text, the two lexers can disagree about where the strings, the
/// comments and the statements are, so the SQLite scan can miss a key that
/// the user wrote for the PostgreSQL reading. The statement with that key
/// does not run, but its text goes to the log. So the scan hides the rest of
/// the text from the first such text, as it does after text that the SQLite
/// lexer rejects.
pub(crate) fn key_ranges(sql: &str) -> Vec<Range<usize>> {
    let mut ranges = turso_sqlite::SqliteDialect.key_ranges(sql);
    if let Some(start) = postgres_only_text_start(sql) {
        ranges.push(start..sql.len());
    }
    ranges
}

/// The start of the first text that the PostgreSQL lexer reads in another way
/// than the SQLite lexer, if there is such text:
///
/// - An escape string with a backslash, such as `E'\''`. A backslash escapes
///   the quote after it.
/// - A dollar-quoted string, such as `$$it's$$` or `$tag$...$tag$`.
/// - A block comment with `/*` in it. Block comments nest.
/// - A line comment with a carriage return that no line feed follows. The
///   carriage return ends the comment.
/// - A Unicode escape string or name, such as `U&"\0070ragma_hexkey"`. Its
///   escapes can spell any name.
/// - A backtick. It is an operator character, not a quote.
///
/// The SQLite lexer finds this text, because the forms before it read the
/// same in the two lexers.
///
/// FIXME(pg-lexer): this list hand-codes where the PostgreSQL lexer differs
/// from the SQLite lexer. A key scan that reads the tokens of pg_query's
/// scanner wherever the frontend parses with pg_query would replace it.
fn postgres_only_text_start(sql: &str) -> Option<usize> {
    let mut lexer = Lexer::new(sql.as_bytes());
    // The two tokens before the token that the loop reads, with their spans.
    let mut before: [Option<(TokenType, Range<usize>)>; 2] = [None, None];
    loop {
        let start = sql.len() - lexer.remaining().len();
        // At text that the SQLite lexer rejects, the SQLite scan hides the
        // rest of the text.
        let Some(Ok(token)) = lexer.next() else {
            return None;
        };
        let end = sql.len() - lexer.remaining().len();
        let text = &sql.as_bytes()[start..end];
        let quoted = token.token_type == TokenType::TK_STRING
            || (token.token_type == TokenType::TK_ID && text[0] == b'"');
        match token.token_type {
            TokenType::TK_NONE if text.starts_with(b"/*") && contains(&text[2..], b"/*") => {
                return Some(start)
            }
            TokenType::TK_NONE
                if text.starts_with(b"--")
                    && text
                        .windows(2)
                        .any(|pair| pair[0] == b'\r' && pair[1] != b'\n') =>
            {
                return Some(start)
            }
            TokenType::TK_VARIABLE if text[0] == b'$' && text[1..].contains(&b'$') => {
                return Some(start)
            }
            TokenType::TK_ID if text[0] == b'`' => return Some(start),
            _ => {}
        }
        if quoted {
            if token.token_type == TokenType::TK_STRING && text.contains(&b'\\') {
                if let Some(at) = prefix_start(sql, &before[1], b'e', start) {
                    return Some(at);
                }
            }
            if let Some((TokenType::TK_BITAND, ampersand)) = &before[1] {
                if ampersand.end == start {
                    if let Some(at) = prefix_start(sql, &before[0], b'u', ampersand.start) {
                        return Some(at);
                    }
                }
            }
        }
        before = [before[1].take(), Some((token.token_type, start..end))];
    }
}

/// The start of `token` when it is the one-letter name `letter`, in either
/// case, and ends at `at`, so that it is the prefix of the token at `at`, such
/// as the `E` of `E'...'`.
fn prefix_start(
    sql: &str,
    token: &Option<(TokenType, Range<usize>)>,
    letter: u8,
    at: usize,
) -> Option<usize> {
    match token {
        Some((TokenType::TK_ID, span))
            if span.end == at && sql.as_bytes()[span.clone()].eq_ignore_ascii_case(&[letter]) =>
        {
            Some(span.start)
        }
        _ => None,
    }
}

/// Whether `needle` is in `text`.
fn contains(text: &[u8], needle: &[u8]) -> bool {
    text.windows(needle.len()).any(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::key_ranges;
    use std::ops::Range;
    use turso_core::Dialect;

    const KEY: &str = "cafe00";

    /// `sql` with each range in `ranges` replaced by `<redacted>`. Ranges
    /// that overlap or touch become one.
    fn redact(mut ranges: Vec<Range<usize>>, sql: &str) -> String {
        ranges.sort_unstable_by_key(|range| range.start);
        let mut merged: Vec<Range<usize>> = Vec::new();
        for range in ranges {
            match merged.last_mut() {
                Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
                _ => merged.push(range),
            }
        }
        let mut text = String::new();
        let mut written = 0;
        for range in merged {
            text.push_str(&sql[written..range.start]);
            text.push_str("<redacted>");
            written = range.end;
        }
        text.push_str(&sql[written..]);
        text
    }

    /// Text that the PostgreSQL lexer reads in another way than the SQLite
    /// lexer. pg_query, which holds PostgreSQL's own parser, reads each
    /// `check` as PostgreSQL SQL. In each `sql`, the SQLite scan alone shows
    /// the key, and the scan of the frontend hides the text from `hidden_from`.
    #[test]
    fn rest_of_text_after_postgres_only_text_is_hidden() {
        for (check, sql, hidden_from) in [
            (
                "SELECT E'\\'' AS a",
                "SELECT E'\\'' AS a; PRAGMA hexkey = 'cafe00'; SELECT '",
                "E'",
            ),
            (
                "SELECT $$it's$$",
                "SELECT $$it's$$; PRAGMA hexkey = 'cafe00'; SELECT '",
                "$$",
            ),
            (
                "SELECT 1 /* a /* b */ ' */",
                "SELECT 1 /* a /* b */ ' */; PRAGMA hexkey = 'cafe00'; SELECT '",
                "/*",
            ),
            (
                "SELECT 1 AS -- c\ra",
                "SELECT 1; -- c\rPRAGMA hexkey = 'cafe00'",
                "--",
            ),
            (
                "SELECT * FROM U&\"\\0070ragma_hexkey\"('cafe00')",
                "SELECT * FROM U&\"\\0070ragma_hexkey\"('cafe00')",
                "U&",
            ),
            (
                "SELECT 1 ` 2",
                "SELECT 1 ` 2; PRAGMA hexkey = 'cafe00'; SELECT `",
                "`",
            ),
        ] {
            assert!(turso_pg_parser::parse(check).is_ok(), "{check:?}");
            let sqlite = turso_sqlite::SqliteDialect.key_ranges(sql);
            assert!(redact(sqlite, sql).contains(KEY), "{sql:?}");
            let at = sql.find(hidden_from).unwrap();
            assert_eq!(
                redact(key_ranges(sql), sql),
                format!("{}<redacted>", &sql[..at]),
                "{sql:?}"
            );
        }
    }

    /// Text that the two lexers read in the same way keeps its log text, and
    /// the SQLite scan hides the keys of its statements.
    #[test]
    fn text_that_reads_the_same_keeps_its_text() {
        for (sql, expected) in [
            (
                "SELECT $1, 'it''s', E'x' FROM t WHERE a = $2; PRAGMA hexkey = 'cafe00'",
                "SELECT $1, 'it''s', E'x' FROM t WHERE a = $2; PRAGMA hexkey <redacted>",
            ),
            (
                "SELECT 1; -- c\r\nPRAGMA hexkey = 'cafe00'",
                "SELECT 1; -- c\r\nPRAGMA hexkey <redacted>",
            ),
            (
                "SELECT $a(') ; PRAGMA hexkey = 'cafe00' --'",
                "SELECT $a(') ; PRAGMA hexkey <redacted> --'",
            ),
            (
                "SELECT x::text, \"a\"\"b\" /* c */ FROM t",
                "SELECT x::text, \"a\"\"b\" /* c */ FROM t",
            ),
        ] {
            assert_eq!(redact(key_ranges(sql), sql), expected);
        }
    }
}
