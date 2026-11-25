//! Finds the encryption keys that SQL statements carry, so that core can hide
//! them when it writes SQL text to its log.

use std::ops::Range;
use turso_parser::{
    lexer::{is_identifier_start, Lexer, Token},
    token::TokenType,
};

/// Returns the byte ranges of the keys that the statements in `sql` carry:
///
/// - the value of a PRAGMA whose name ends in `key`, such as
///   `PRAGMA hexkey = '<key>'`,
/// - the arguments of the table function of such a PRAGMA, such as
///   `pragma_hexkey('<key>')`,
/// - the KEY clause of ATTACH, such as `ATTACH 'a.db' AS a KEY '<key>'`.
///
/// A range runs from the token after the PRAGMA name, the function name or
/// KEY to the end of the statement. Turso reads `hexkey`. SQLCipher and the
/// SQLite Encryption Extension use `key`, `textkey`, `rekey` and other names
/// that end in `key`, and users can send these to Turso too, so all such names
/// count.
///
/// PRAGMA and ATTACH count only as the first word of a statement, or after
/// `EXPLAIN`, `EXPLAIN QUERY PLAN` or `EXPLAIN QUERY PLAN FORMAT = <format>`,
/// so that a column named `attach` or `pragma` does not hide the rest of a
/// query. When the name after PRAGMA is not a name, the statement does not
/// parse, and the scan hides the rest of it. At text that the lexer rejects,
/// the scan cannot tell where the statements and their keys are, so it hides
/// the rest of the text.
pub(crate) fn key_ranges(sql: &str) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut state = State::Start;
    let mut lexer = Lexer::new(sql.as_bytes());
    loop {
        let start = sql.len() - lexer.remaining().len();
        let token = match lexer.next() {
            None => break,
            Some(Ok(token)) if token.token_type != TokenType::TK_ILLEGAL => token,
            Some(_) => {
                // A key that this statement started continues into the
                // hidden text.
                let start = match state {
                    State::Key(range) => range.start,
                    _ => start,
                };
                ranges.push(start..sql.len());
                return ranges;
            }
        };
        if token.token_type.is_none() {
            continue; // white space or a comment
        }
        let end = sql.len() - lexer.remaining().len();
        state = state.next(&token, start..end, &mut ranges);
    }
    state.finish(&mut ranges);
    ranges
}

/// What [`key_ranges`] knows about the statement that it reads.
enum State {
    /// At the start of a statement, or after the format of
    /// `EXPLAIN QUERY PLAN FORMAT = <format>`.
    Start,
    /// After EXPLAIN at the start of a statement.
    Explain,
    /// After `EXPLAIN QUERY`.
    ExplainQuery,
    /// After `EXPLAIN QUERY PLAN`.
    ExplainPlan,
    /// After `EXPLAIN QUERY PLAN FORMAT`, or after the `=` that follows it.
    ExplainFormat,
    /// The statement does not carry a key.
    Other,
    /// After PRAGMA.
    Pragma,
    /// After `PRAGMA name`. A `.` after it makes the name a schema name.
    PragmaName { is_key: bool },
    /// After `PRAGMA schema.`.
    PragmaSchemaDot,
    /// In an ATTACH statement, before KEY.
    Attach,
    /// After the name of a key PRAGMA or of its table function, or after the
    /// KEY of ATTACH.
    AfterKeyName,
    /// The text of a key, from its first token to the last token so far.
    Key(Range<usize>),
}

impl State {
    /// The state after `token`, which is at `span` and is not white space or
    /// a comment. Records the key of a statement that ends.
    fn next(self, token: &Token, span: Range<usize>, ranges: &mut Vec<Range<usize>>) -> State {
        use TokenType::*;
        let name = name(token);
        match (self, token.token_type) {
            (state, TK_SEMI) => {
                state.finish(ranges);
                State::Start
            }
            // A table function can be anywhere in a statement.
            (state, _)
                if !matches!(state, State::Key(_) | State::AfterKeyName)
                    && name.is_some_and(is_key_pragma_function) =>
            {
                State::AfterKeyName
            }
            (State::Start, TK_EXPLAIN) => State::Explain,
            (State::Explain, TK_QUERY) => State::ExplainQuery,
            (State::ExplainQuery, TK_PLAN) => State::ExplainPlan,
            (State::ExplainPlan, TK_ID) if token.value.eq_ignore_ascii_case(b"format") => {
                State::ExplainFormat
            }
            (State::ExplainFormat, TK_EQ) => State::ExplainFormat,
            // A statement that leaves out the format does not parse, but its
            // key must not show.
            (
                State::Start | State::Explain | State::ExplainPlan | State::ExplainFormat,
                TK_PRAGMA,
            ) => State::Pragma,
            (
                State::Start | State::Explain | State::ExplainPlan | State::ExplainFormat,
                TK_ATTACH,
            ) => State::Attach,
            // The format name, such as JSON. The statement follows it.
            (State::ExplainFormat, _) if name.is_some() => State::Start,
            (State::Pragma, _) => match name {
                Some(name) => State::PragmaName {
                    is_key: is_key_name(name),
                },
                None => State::Key(span),
            },
            (State::PragmaName { .. }, TK_DOT) => State::PragmaSchemaDot,
            (State::PragmaName { is_key: true }, _) => State::Key(span),
            (State::PragmaSchemaDot, _) => match name {
                Some(name) if is_key_name(name) => State::AfterKeyName,
                Some(_) => State::Other,
                None => State::Key(span),
            },
            (State::Attach, TK_KEY) => State::AfterKeyName,
            (State::Attach, _) => State::Attach,
            (State::AfterKeyName, _) => State::Key(span),
            (State::Key(range), _) => State::Key(range.start..span.end),
            _ => State::Other,
        }
    }

    /// Records the key of a statement that ends.
    fn finish(self, ranges: &mut Vec<Range<usize>>) {
        if let State::Key(range) = self {
            ranges.push(range);
        }
    }
}

/// The name that `token` holds, without the quotes around a quoted name, or
/// None when the token is not a name. A keyword counts as a name, because
/// many keywords can be names, such as `key`. A string literal counts too,
/// because SQLite reads a string literal as a name where only a name can be.
fn name<'a>(token: &Token<'a>) -> Option<&'a [u8]> {
    let value = token.value;
    match token.token_type {
        // A quoted token always has both quotes: the lexer rejects a quote
        // without an end.
        TokenType::TK_STRING => Some(&value[1..value.len() - 1]),
        TokenType::TK_ID if matches!(value.first(), Some(b'"' | b'`')) => {
            Some(&value[1..value.len() - 1])
        }
        TokenType::TK_BLOB => None,
        _ if value.first().copied().is_some_and(is_identifier_start) => Some(value),
        _ => None,
    }
}

/// Whether a table function with this name is the function of a PRAGMA that
/// carries a key, such as `pragma_hexkey`.
fn is_key_pragma_function(name: &[u8]) -> bool {
    name.len() > b"pragma_".len()
        && name[..b"pragma_".len()].eq_ignore_ascii_case(b"pragma_")
        && is_key_name(name)
}

/// Whether a PRAGMA with this name carries a key.
fn is_key_name(name: &[u8]) -> bool {
    name.len() >= 3 && name[name.len() - 3..].eq_ignore_ascii_case(b"key")
}

#[cfg(test)]
mod tests {
    use super::key_ranges;
    use std::ops::Range;
    use turso_core::ast;
    use turso_parser::{lexer::Lexer, parser::Parser};

    /// `sql` with each key range replaced by `<redacted>`. Ranges that
    /// overlap or touch become one.
    fn redact(sql: &str) -> String {
        let mut ranges = key_ranges(sql);
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

    #[test]
    fn text_without_keys_does_not_change() {
        for sql in [
            "",
            "SELECT * FROM t WHERE key = 'hexkey' AND k = ?",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, key TEXT, FOREIGN KEY(key) REFERENCES u)",
            "PRAGMA foreign_keys = ON",
            "PRAGMA main.cache_size = 10",
            "PRAGMA hexkey",
            "PRAGMA aux.hexkey;",
            "ATTACH 'file:a.db?cipher=aegis256&mode=ro' AS a",
            "ATTACH 'file:a.db?cipher=aegis256&hexkey=5ec7e7' AS a",
            "SELECT attach, key, value FROM t",
            "SELECT pragma, hexkey FROM t WHERE hexkey = 'x'",
            "CREATE TABLE attach(pragma TEXT, key TEXT)",
            "EXPLAIN SELECT attach FROM t; EXPLAIN QUERY PLAN SELECT key FROM pragma",
            "EXPLAIN QUERY PLAN FORMAT=JSON SELECT attach, key FROM t",
            "SELECT 'café', \"naïve\" -- ünïcode\nFROM t /* comment",
            "SELECT * FROM pragma_hexkey",
            "SELECT * FROM pragma_table_info('t')",
            "SELECT * FROM pragma_foreign_key_list('t')",
        ] {
            assert_eq!(redact(sql), sql);
        }
    }

    #[test]
    fn key_pragma_values_are_hidden() {
        for (sql, expected) in [
            ("PRAGMA hexkey = '5ec7e7'", "PRAGMA hexkey <redacted>"),
            ("pragma HexKey='5ec7e7';", "pragma HexKey<redacted>;"),
            (
                "PRAGMA main.hexkey('5ec7e7')",
                "PRAGMA main.hexkey<redacted>",
            ),
            (
                "PRAGMA \"aux\" . \"hexkey\" = x'5ec7e7'",
                "PRAGMA \"aux\" . \"hexkey\" <redacted>",
            ),
            ("PRAGMA 'key' = \"5ec7e7\"", "PRAGMA 'key' <redacted>"),
            ("PRAGMA key.rekey = 5ec7e7", "PRAGMA key.rekey <redacted>"),
            (
                "EXPLAIN PRAGMA /* c */ hexkey /* c */ = '5ec7e7' -- c",
                "EXPLAIN PRAGMA /* c */ hexkey /* c */ <redacted> -- c",
            ),
            (
                "-- c\nexplain query plan PRAGMA hexkey = '5ec7e7'",
                "-- c\nexplain query plan PRAGMA hexkey <redacted>",
            ),
            (
                "EXPLAIN QUERY PLAN FORMAT=JSON PRAGMA hexkey = '5ec7e7'",
                "EXPLAIN QUERY PLAN FORMAT=JSON PRAGMA hexkey <redacted>",
            ),
            ("PRAGMA hexkey = '5ec;7e7", "PRAGMA hexkey <redacted>"),
            (
                "SELECT 1; PRAGMA textrekey = '5e''c7e7'; SELECT 'x'",
                "SELECT 1; PRAGMA textrekey <redacted>; SELECT 'x'",
            ),
        ] {
            assert_eq!(redact(sql), expected);
        }
    }

    /// A PRAGMA name that is not a name, such as `[hexkey]`, does not parse in
    /// Turso. The scan cannot tell which PRAGMA it is, so it hides the rest of
    /// the statement.
    #[test]
    fn pragma_without_a_name_hides_the_rest_of_the_statement() {
        for (sql, expected) in [
            (
                "PRAGMA [hexkey] = '5ec7e7'; SELECT 1",
                "PRAGMA <redacted>; SELECT 1",
            ),
            ("PRAGMA main.[key] = '5ec7e7'", "PRAGMA main.<redacted>"),
            ("PRAGMA main.cache_size = 1", "PRAGMA main.cache_size = 1"),
        ] {
            assert_eq!(redact(sql), expected);
        }
    }

    #[test]
    fn key_pragma_function_arguments_are_hidden() {
        for (sql, expected) in [
            (
                "SELECT * FROM pragma_hexkey('5ec7e7')",
                "SELECT * FROM pragma_hexkey<redacted>",
            ),
            (
                "select hexkey from main.PRAGMA_HEXKEY ( '5ec7e7' ) where 1; SELECT 2",
                "select hexkey from main.PRAGMA_HEXKEY <redacted>; SELECT 2",
            ),
            (
                "SELECT * FROM \"pragma_key\"('5ec7e7')",
                "SELECT * FROM \"pragma_key\"<redacted>",
            ),
            (
                "SELECT 1 FROM t JOIN pragma_rekey(x'5ec7e7') ON 1",
                "SELECT 1 FROM t JOIN pragma_rekey<redacted>",
            ),
        ] {
            assert_eq!(redact(sql), expected);
        }
    }

    #[test]
    fn attach_key_clauses_are_hidden() {
        for (sql, expected) in [
            (
                "ATTACH 'a.db' AS a KEY '5ec7e7'",
                "ATTACH 'a.db' AS a KEY <redacted>",
            ),
            (
                "SELECT 1; EXPLAIN ATTACH 'a.db' AS a KEY '5ec7e7'",
                "SELECT 1; EXPLAIN ATTACH 'a.db' AS a KEY <redacted>",
            ),
            (
                "explain query plan format = text attach 'a.db' as a key '5ec7e7'",
                "explain query plan format = text attach 'a.db' as a key <redacted>",
            ),
            (
                "attach database 'a.db' as \"key\" key lower('5ec7e7'); SELECT 1",
                "attach database 'a.db' as \"key\" key <redacted>; SELECT 1",
            ),
        ] {
            assert_eq!(redact(sql), expected);
        }
    }

    /// The lexer reads a TCL variable, such as `$a(')`, as one parameter. A
    /// quote in it does not start a string.
    #[test]
    fn tcl_variables_are_parameters() {
        for (sql, expected) in [
            (
                "SELECT $a(') ; PRAGMA hexkey = '5ec7e7' --'",
                "SELECT $a(') ; PRAGMA hexkey <redacted> --'",
            ),
            (
                "SELECT @a(\"), :b('), $::ns::c(') FROM t; PRAGMA key = '5ec7e7'",
                "SELECT @a(\"), :b('), $::ns::c(') FROM t; PRAGMA key <redacted>",
            ),
            (
                "SELECT $a(x) FROM t; ATTACH 'a.db' AS a KEY '5ec7e7'",
                "SELECT $a(x) FROM t; ATTACH 'a.db' AS a KEY <redacted>",
            ),
            (
                "SELECT ?1, $v, :v, @v; PRAGMA hexkey = '5ec7e7'",
                "SELECT ?1, $v, :v, @v; PRAGMA hexkey <redacted>",
            ),
            // White space in the suffix makes the lexer reject the variable.
            (
                "SELECT $a(' ; PRAGMA hexkey = '5ec7e7'",
                "SELECT <redacted>",
            ),
        ] {
            assert_eq!(redact(sql), expected);
        }
    }

    /// Turso's lexer reads `[` and `]` as brackets for subscripts. A `]` in a
    /// string does not end anything.
    #[test]
    fn brackets_are_subscripts() {
        for (sql, expected) in [
            (
                "SELECT x[length('a]')] FROM t; PRAGMA hexkey='5ec7e7'",
                "SELECT x[length('a]')] FROM t; PRAGMA hexkey<redacted>",
            ),
            (
                "SELECT a[1], b[2:3] FROM t; PRAGMA key = '5ec7e7'",
                "SELECT a[1], b[2:3] FROM t; PRAGMA key <redacted>",
            ),
            (
                "SELECT [x; PRAGMA hexkey = '5ec7e7'",
                "SELECT [x; PRAGMA hexkey <redacted>",
            ),
            (
                "SELECT x['k]'] FROM t; ATTACH 'a.db' AS a KEY '5ec7e7'",
                "SELECT x['k]'] FROM t; ATTACH 'a.db' AS a KEY <redacted>",
            ),
        ] {
            assert_eq!(redact(sql), expected);
        }
    }

    /// The scan cannot tell where the statements are after text that the
    /// lexer rejects, so it hides the rest of the text.
    #[test]
    fn rest_of_text_after_rejected_text_is_hidden() {
        for (sql, expected) in [
            ("SELECT \"x; PRAGMA hexkey='5ec7e7'", "SELECT <redacted>"),
            ("SELECT 'x; PRAGMA hexkey=5ec7e7", "SELECT <redacted>"),
            (
                "SELECT 1 # x; PRAGMA hexkey='5ec7e7'",
                "SELECT 1 <redacted>",
            ),
            ("SELECT {x}; PRAGMA hexkey='5ec7e7'", "SELECT <redacted>"),
            ("SELECT 1 ! PRAGMA hexkey = '5ec7e7'", "SELECT 1 <redacted>"),
            ("\x0BPRAGMA hexkey = '5ec7e7'", "<redacted>"),
            (
                "SELECT 1 != 2; PRAGMA hexkey = '5ec7e7'",
                "SELECT 1 != 2; PRAGMA hexkey <redacted>",
            ),
        ] {
            assert_eq!(redact(sql), expected);
        }
    }

    /// Each prefix of the text ends in a different place: inside a quote, a
    /// comment, a name, a variable or a key. No prefix can show a part of the
    /// key.
    #[test]
    fn prefixes_never_show_the_key() {
        let sql = "SELECT 'ü' /* ; */; ATTACH 'ä.db' AS \"ö\" KEY '5ec7e7'; \
                   PRAGMA main.\"hexkey\" = x'5ec7e7' -- é\nSELECT $a(') ; \
                   PRAGMA hexkey = '5ec7e7' --'";
        for end in (0..=sql.len()).filter(|end| sql.is_char_boundary(*end)) {
            let redacted = redact(&sql[..end]);
            assert!(!redacted.contains("5ec"), "{redacted}");
        }
    }

    /// A fixed linear congruential generator, so that each run reads the
    /// same text. It returns a number below `below`.
    fn random_below(seed: &mut u64, below: usize) -> usize {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (*seed >> 33) as usize % below
    }

    /// Text made of random SQL pieces never makes the scan panic, and each
    /// range is a part of the text that starts and ends at a character
    /// boundary.
    #[test]
    fn random_text_gives_ranges_in_the_text() {
        let pieces = [
            "SELECT ",
            "PRAGMA ",
            "ATTACH ",
            "EXPLAIN ",
            "QUERY PLAN ",
            "FORMAT=JSON ",
            "hexkey",
            "main.",
            "KEY ",
            "pragma_hexkey(",
            "'",
            "\"",
            "`",
            "[",
            "]",
            "(",
            ")",
            ";",
            " ",
            "\n",
            "-- ",
            "/*",
            "*/",
            "=",
            "!",
            "#",
            "{",
            "?",
            "$a(')",
            "@b(\")",
            ":c",
            "$::d",
            "x'5ec7e7'",
            "ü",
            "\u{A0}",
        ];
        let mut seed = 11;
        for _ in 0..20_000 {
            let len = random_below(&mut seed, 24);
            let sql: String = (0..len)
                .map(|_| pieces[random_below(&mut seed, pieces.len())])
                .collect();
            let ranges = key_ranges(&sql);
            for range in &ranges {
                assert!(
                    range.start <= range.end
                        && range.end <= sql.len()
                        && sql.is_char_boundary(range.start)
                        && sql.is_char_boundary(range.end),
                    "{sql:?} -> {ranges:?}"
                );
            }
        }
    }

    /// In random text made of statements, the scan hides the value of each
    /// statement that the parser reads as a key PRAGMA.
    #[test]
    fn random_statements_never_show_a_parsed_key() {
        let explains = [
            "",
            "EXPLAIN ",
            "explain query plan ",
            "EXPLAIN QUERY PLAN FORMAT=JSON ",
        ];
        let spaces = [" ", "\n", " /* ; ' */ ", " -- ;'\n"];
        let schemas = ["", "main.", "\"main\" . ", "aux."];
        let names = [
            "hexkey",
            "\"hexkey\"",
            "'key'",
            "key",
            "rekey",
            "TextKey",
            "cache_size",
        ];
        let values = [
            "= '5ec7e7'",
            "('5ec7e7')",
            "= x'5ec7e7'",
            "= \"5ec7e7\"",
            "= '5ec7e7''x'",
        ];
        let others = [
            "SELECT $a(')",
            "SELECT @b(\"), :c, $::d(')",
            "SELECT x[length('a]')] FROM t",
            "SELECT 'it''s;'",
            "SELECT \"a;b\" FROM t",
            "SELECT 1 /* ; ' */",
            "ATTACH 'a.db' AS a",
            "PRAGMA cache_size = 1",
            "SELECT attach, pragma, key FROM t",
            "SELECT * FROM pragma_table_info('t')",
        ];
        let separators = [";", " ; ", ";\n", "; /* ; */ ", "; -- ;\n"];
        let mut seed = 11;
        let mut checked = 0;
        for _ in 0..20_000 {
            let mut sql = String::new();
            for i in 0..=random_below(&mut seed, 3) {
                if i > 0 {
                    sql.push_str(separators[random_below(&mut seed, separators.len())]);
                }
                if random_below(&mut seed, 2) == 0 {
                    sql.push_str(others[random_below(&mut seed, others.len())]);
                    continue;
                }
                for part in [
                    explains[random_below(&mut seed, explains.len())],
                    "PRAGMA",
                    spaces[random_below(&mut seed, spaces.len())],
                    schemas[random_below(&mut seed, schemas.len())],
                    names[random_below(&mut seed, names.len())],
                    spaces[random_below(&mut seed, spaces.len())],
                    values[random_below(&mut seed, values.len())],
                ] {
                    sql.push_str(part);
                }
            }
            let ranges = key_ranges(&sql);
            for statement in key_pragma_statements(&sql) {
                for key in marker_ranges(&sql, statement, "5ec7e7") {
                    checked += 1;
                    assert!(
                        ranges
                            .iter()
                            .any(|range| range.start <= key.start && key.end <= range.end),
                        "{sql:?} -> {ranges:?}"
                    );
                }
            }
        }
        // Most of the statements parse, so the test checks many keys.
        assert!(checked > 5_000, "{checked}");
    }

    /// The byte ranges of the statements in `sql` that the parser reads as a
    /// PRAGMA with a value and with a name that ends in `key`. It stops at
    /// the first statement that does not parse.
    fn key_pragma_statements(sql: &str) -> Vec<Range<usize>> {
        let mut statements = Vec::new();
        let mut parser = Parser::new(sql.as_bytes());
        let mut start = 0;
        while let Ok(Some(cmd)) = parser.next_cmd() {
            let end = parser.offset();
            let stmt = match cmd {
                ast::Cmd::Stmt(stmt) | ast::Cmd::Explain(stmt) => stmt,
                ast::Cmd::ExplainQueryPlan { stmt, .. } => stmt,
            };
            if let ast::Stmt::Pragma {
                name,
                body: Some(_),
            } = stmt
            {
                if name.name.as_str().to_ascii_lowercase().ends_with("key") {
                    statements.push(start..end);
                }
            }
            start = end;
        }
        statements
    }

    /// The byte ranges of `marker` in the tokens of `sql` that are in
    /// `statement`. A marker in white space or in a comment is not a value.
    fn marker_ranges(sql: &str, statement: Range<usize>, marker: &str) -> Vec<Range<usize>> {
        let mut found = Vec::new();
        let mut lexer = Lexer::new(&sql.as_bytes()[..statement.end]);
        loop {
            let start = statement.end - lexer.remaining().len();
            let Some(Ok(token)) = lexer.next() else {
                return found;
            };
            if start < statement.start || token.token_type.is_none() {
                continue;
            }
            let text = &sql[start..statement.end - lexer.remaining().len()];
            found.extend(
                text.match_indices(marker)
                    .map(|(at, _)| start + at..start + at + marker.len()),
            );
        }
    }
}
