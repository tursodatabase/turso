//! Hides encryption keys in text that the engine writes to its log.
//!
//! SQL text can carry an encryption key in these places:
//!
//! - A URI parameter, such as `ATTACH 'file:a.db?cipher=aegis256&hexkey=<key>' AS a`.
//! - The value of a key PRAGMA, such as `PRAGMA hexkey = '<key>'`.
//! - The argument of a key PRAGMA function, such as
//!   `SELECT * FROM pragma_hexkey('<key>')`.
//! - The KEY clause of ATTACH, such as `ATTACH 'a.db' AS a KEY '<key>'`.
//!
//! Turso rejects the last three, but the log shows the text of a statement
//! before the engine compiles it. The scans treat the value of a PRAGMA or
//! URI parameter as a key when the name ends in `key`. Turso reads `hexkey`.
//! SQLCipher and the SQLite Encryption Extension use `key`, `textkey`, `rekey`
//! and other names that end in `key`, and users can send these to Turso too.
//!
//! [`RedactKeys`] formats SQL text. It hides the keys in URI parameters, and
//! the keys that the statements carry, which the dialect finds with
//! [`Dialect::key_ranges`]: core has no SQL lexer, and the frontend reads the
//! text with the lexer of its own parser. [`RedactUriKeys`] and
//! [`RedactKeysDebug`] format text that is not SQL, such as the `Debug` output
//! of a compiled instruction, and they hide only the keys in URI parameters.
//! Use [`RedactKeys`] only for the raw text of SQL statements. Use the other
//! two for formatted text, such as query plans, expressions, instructions,
//! names, values and errors: the SQLite scan hides all the text after text
//! that its lexer rejects, and formatted text can have such text, such as the
//! "`--" of a query plan.
//!
//! When a scan is not sure, it hides more text: a hidden value that is not a
//! key costs less than a key in a log file. The scans are best effort, and
//! they read only the literal text. These limits apply:
//!
//! - They do not find a key that an SQL expression puts together, such as
//!   `'file:a.db?hexkey=' || '<key>'`.
//! - In the SQLite dialect, PRAGMA and ATTACH count only as the first word of
//!   a statement. A statement that does not parse can hide them: for example,
//!   a missing `;` before them, a byte order mark or a no-break space before
//!   them, or `(PRAGMA ...)`. Turso rejects these statements, and it rejects
//!   key PRAGMAs too, so such a key never works.
//! - A key PRAGMA inside an SQL comment is not hidden. The engine never uses
//!   it as a key.
//! - Turso rejects a key literal in ATTACH KEY, such as `KEY '<key>'`, before
//!   it compiles the statement. It checks any other KEY expression, such as
//!   `KEY lower('<key>')`, when the statement runs, so a key literal inside
//!   that expression can show in the TRACE log of the compiled instructions.
//! - The scans apply to SQL text, expressions and compiled instructions.
//!   Logs at DEBUG and TRACE can still show stored row data, and a key URI
//!   that is stored as row data is not hidden. The schema stores the SQL of
//!   each CREATE statement as row data, so a key URI literal in the
//!   definition of a view, a table or a trigger can show in a row dump.
//!
//! To keep a key out of the log, bind the URI as a parameter, or write it as
//! one string literal.

use crate::Dialect;
use std::fmt;
use std::ops::Range;

/// The text that replaces a key.
const REDACTED: &str = "<redacted>";

/// Formats the `Debug` text of a value, such as a compiled instruction that
/// holds a SQL literal, with each key in a URI parameter replaced by
/// `<redacted>`. The text is not SQL, so it gets the scan of
/// [`RedactUriKeys`]. It does the work only when it is formatted.
pub(crate) struct RedactKeysDebug<'a, T: ?Sized>(pub(crate) &'a T);

impl<T: fmt::Debug + ?Sized> fmt::Display for RedactKeysDebug<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = format!("{:?}", self.0);
        write!(f, "{}", RedactUriKeys(&text))
    }
}

/// Formats text that is not SQL, such as the text of a compiled instruction,
/// with each key in a URI parameter replaced by `<redacted>`. The statement
/// scan does not apply, because it reads the text as SQL tokens, and it hides
/// all the text after a character that is not SQL, such as `{`.
pub(crate) struct RedactUriKeys<'a>(pub(crate) &'a str);

impl fmt::Display for RedactUriKeys<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ranges = Vec::new();
        uri_key_ranges(self.0.as_bytes(), &mut ranges);
        write_redacted(f, self.0, ranges)
    }
}

/// Formats SQL text with each encryption key in it replaced by `<redacted>`:
/// the keys in URI parameters, and the keys that the statements carry, which
/// the dialect finds.
///
/// The scans run only when the value is formatted. A log call at a level that
/// is disabled does not format its arguments, so it does not pay for the scans.
pub(crate) struct RedactKeys<'a>(pub(crate) &'a dyn Dialect, pub(crate) &'a str);

impl fmt::Display for RedactKeys<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(dialect, sql) = self;
        let mut ranges = dialect.key_ranges(sql);
        uri_key_ranges(sql.as_bytes(), &mut ranges);
        write_redacted(f, sql, ranges)
    }
}

/// Writes `text` with each range in `ranges` replaced by one `<redacted>`.
/// Ranges that overlap or touch are joined. A range can come from a dialect,
/// so it can end past the text or inside a character: it grows to whole
/// characters and stops at the end of the text, and an empty range hides
/// nothing.
fn write_redacted(
    f: &mut fmt::Formatter<'_>,
    text: &str,
    mut ranges: Vec<Range<usize>>,
) -> fmt::Result {
    ranges.retain_mut(|range| {
        range.end = range.end.min(text.len());
        range.start = range.start.min(range.end);
        if range.is_empty() {
            return false;
        }
        while !text.is_char_boundary(range.start) {
            range.start -= 1;
        }
        while !text.is_char_boundary(range.end) {
            range.end += 1;
        }
        true
    });
    ranges.sort_unstable_by_key(|range| range.start);
    let mut written = 0;
    let mut pending: Option<Range<usize>> = None;
    for range in ranges {
        match &mut pending {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => {
                if let Some(last) = pending.replace(range) {
                    f.write_str(&text[written..last.start])?;
                    f.write_str(REDACTED)?;
                    written = last.end;
                }
            }
        }
    }
    if let Some(last) = pending {
        f.write_str(&text[written..last.start])?;
        f.write_str(REDACTED)?;
        written = last.end;
    }
    f.write_str(&text[written..])
}

/// Finds the value of each URI parameter whose name ends in `key`, such as
/// the `<key>` in `file:a.db?cipher=aegis256&hexkey=<key>`.
fn uri_key_ranges(text: &[u8], ranges: &mut Vec<Range<usize>>) {
    for (separator, _) in text
        .iter()
        .enumerate()
        .filter(|&(_, &byte)| byte == b'?' || byte == b'&')
    {
        let name_start = separator + 1;
        let name_end = name_start
            + text[name_start..]
                .iter()
                .take_while(|&&byte| byte.is_ascii_alphanumeric() || byte == b'_')
                .count();
        let equals = name_end
            + text[name_end..]
                .iter()
                .take_while(|byte| byte.is_ascii_whitespace())
                .count();
        if text.get(equals) != Some(&b'=') || !is_key_name(&text[name_start..name_end]) {
            continue;
        }
        let value_start = equals + 1;
        let value_end = value_start
            + text[value_start..]
                .iter()
                .take_while(|&&byte| !ends_uri_value(byte))
                .count();
        // A value of only white space holds no key.
        if text[value_start..value_end]
            .iter()
            .any(|byte| !byte.is_ascii_whitespace())
        {
            ranges.push(value_start..value_end);
        }
    }
}

/// Whether `byte` ends the value of a URI parameter. The value ends at the
/// next parameter or at the fragment, and also at a quote, a bracket or a
/// semicolon, which end the SQL text around the URI. A hex key has none of
/// these bytes. The value also ends at a `?`, so that no scan of a value goes
/// past the start of the next parameter, and the scans of all values together
/// read each byte at most one time.
///
/// White space does not end a value. The engine decodes percent escapes in a
/// value and then removes all white space around the key, so a key after any
/// white space, such as `%20`, a tab or a no-break space, still works.
fn ends_uri_value(byte: u8) -> bool {
    matches!(byte, b'?' | b'&' | b'#' | b'\'' | b'"' | b'`' | b']' | b';')
}

/// Whether a URI parameter with this name carries a key.
fn is_key_name(name: &[u8]) -> bool {
    name.len() >= 3 && name[name.len() - 3..].eq_ignore_ascii_case(b"key")
}

#[cfg(test)]
mod tests {
    use super::{write_redacted, RedactKeys, RedactKeysDebug, RedactUriKeys};
    use crate::dialect::cannot_parse::DialectThatCannotParse;
    use std::fmt;
    use std::ops::Range;

    fn redact(text: &str) -> String {
        RedactUriKeys(text).to_string()
    }

    /// Formats a text with each of the ranges replaced by `<redacted>`, as a
    /// dialect gives them to [`RedactKeys`].
    struct WithRanges<'a>(&'a str, Vec<Range<usize>>);

    impl fmt::Display for WithRanges<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write_redacted(f, self.0, self.1.clone())
        }
    }

    #[test]
    fn debug_text_is_redacted() {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct String8 {
            value: String,
            dest: usize,
        }
        let insn = String8 {
            value: "file:a.db?cipher=aegis256&hexkey=5ec7e7".to_string(),
            dest: 1,
        };
        assert_eq!(
            RedactKeysDebug(&insn).to_string(),
            "String8 { value: \"file:a.db?cipher=aegis256&hexkey=<redacted>\", dest: 1 }"
        );
    }

    #[test]
    fn text_without_uri_keys_does_not_change() {
        for text in [
            "",
            "SELECT * FROM t WHERE key = 'hexkey' AND k = ?",
            "PRAGMA hexkey = '5ec7e7'",
            "ATTACH 'file:a.db?cipher=aegis256&mode=ro' AS a",
            "SELECT 'file:a.db?hexkey=' || x FROM t",
            "SELECT 'file:a.db?hexkey= ' || x FROM t",
            "SELECT 'café', \"naïve\" -- ünïcode\nFROM t /* comment",
        ] {
            assert_eq!(redact(text), text);
        }
    }

    #[test]
    fn uri_key_parameters_are_hidden() {
        for (text, expected) in [
            (
                "ATTACH 'file:a.db?cipher=aegis256&hexkey=5ec7e7' AS a",
                "ATTACH 'file:a.db?cipher=aegis256&hexkey=<redacted>' AS a",
            ),
            (
                "ATTACH 'file:a.db?cipher=aegis256&hexkey= 5ec7e7 ' AS a",
                "ATTACH 'file:a.db?cipher=aegis256&hexkey=<redacted>' AS a",
            ),
            (
                "ATTACH 'file:a.db?cipher=aegis256&hexkey =5ec7e7' AS a",
                "ATTACH 'file:a.db?cipher=aegis256&hexkey =<redacted>' AS a",
            ),
            (
                "ATTACH 'file:a.db?hexkey \t= 5ec7e7' AS a",
                "ATTACH 'file:a.db?hexkey \t=<redacted>' AS a",
            ),
            (
                "ATTACH 'file:a.db?hexkey=\n\t 5ec7e7&cipher=aegis256' AS a",
                "ATTACH 'file:a.db?hexkey=<redacted>&cipher=aegis256' AS a",
            ),
            (
                "ATTACH 'file:a.db?hexkey=5ec7e7&cipher=aegis256#part' AS a",
                "ATTACH 'file:a.db?hexkey=<redacted>&cipher=aegis256#part' AS a",
            ),
            (
                "ATTACH \"file:a.db?KEY=5ec7e7\" AS a",
                "ATTACH \"file:a.db?KEY=<redacted>\" AS a",
            ),
            (
                "SELECT 'file:ä.db?textkey=5ec7e7;'",
                "SELECT 'file:ä.db?textkey=<redacted>;'",
            ),
            (
                "file:a.db?cipher=aegis256&hexkey=5e%63",
                "file:a.db?cipher=aegis256&hexkey=<redacted>",
            ),
            (
                "file:a.db?key=5ec?key=7e7&key=",
                "file:a.db?key=<redacted>?key=<redacted>&key=",
            ),
        ] {
            assert_eq!(redact(text), expected);
        }
    }

    /// The engine decodes percent escapes in a URI value and then removes all
    /// white space around the key, so each of these keys works.
    #[test]
    fn uri_keys_after_white_space_are_hidden() {
        for space in [
            "%20 ",
            " %20 ",
            "\x0B ",
            "\u{A0} ",
            "\u{3000} ",
            "\u{85} ",
            "%09%0A ",
        ] {
            let text = format!("ATTACH 'file:a.db?cipher=aegis256&hexkey={space}5ec7e7' AS a");
            assert_eq!(
                redact(&text),
                "ATTACH 'file:a.db?cipher=aegis256&hexkey=<redacted>' AS a"
            );
        }
    }

    /// SQL text gets the URI scan and the key scan of the dialect. A dialect
    /// that has no key scan of its own hides all the text.
    #[test]
    fn sql_text_without_a_dialect_key_scan_is_hidden() {
        for (sql, expected) in [
            ("SELECT 1", "<redacted>"),
            ("ATTACH 'file:a.db?hexkey=5ec7e7' AS a", "<redacted>"),
            ("", ""),
        ] {
            assert_eq!(
                RedactKeys(&DialectThatCannotParse, sql).to_string(),
                expected
            );
        }
    }

    /// Ranges that overlap or touch become one. A range past the end of the
    /// text stops at the end, a range that ends inside a character grows to
    /// the whole character, and an empty range hides nothing.
    #[test]
    fn ranges_are_joined_and_kept_in_the_text() {
        // The bytes of "é" are 1 and 2. Each range is a (start, end) pair.
        for (ranges, expected) in [
            (&[][..], "aé-b"),
            (&[(0, 1), (1, 2)], "<redacted>-b"),
            (&[(2, 4)], "a<redacted>b"),
            (&[(4, 99), (98, 99)], "aé-<redacted>"),
            (&[(2, 2), (3, 1), (9, 12)], "aé-b"),
            (&[(3, 4), (0, 1)], "<redacted>é<redacted>b"),
            (&[(0, 5), (1, 2)], "<redacted>"),
        ] {
            let ranges = ranges.iter().map(|&(start, end)| start..end).collect();
            assert_eq!(WithRanges("aé-b", ranges).to_string(), expected);
        }
    }

    /// A query plan is not SQL. It draws its tree with characters such as
    /// "`--", which the SQLite lexer rejects, so the plan log uses the URI
    /// scan and keeps its text.
    #[test]
    fn plan_text_keeps_its_text() {
        let plan =
            "QUERY PLAN\n`--MULTI-INDEX OR t (ia, ib)\n   |--SEARCH t USING INDEX ia (a=?)\n";
        assert_eq!(redact(plan), plan);
        assert_eq!(
            redact("QUERY PLAN\n`--SCAN t WHERE u = 'file:a.db?hexkey=5ec7e7'"),
            "QUERY PLAN\n`--SCAN t WHERE u = 'file:a.db?hexkey=<redacted>'"
        );
    }

    /// Text that is not SQL gets only the URI scan, so characters such as `{`
    /// and `#` do not hide the rest of it.
    #[test]
    fn text_that_is_not_sql_gets_only_the_uri_scan() {
        let text = "Init { target_pc: 1 } # r[1]='file:a.db?hexkey=5ec7e7' PRAGMA hexkey=1";
        assert_eq!(
            redact(text),
            "Init { target_pc: 1 } # r[1]='file:a.db?hexkey=<redacted>' PRAGMA hexkey=1"
        );
    }

    /// Each prefix of the text ends in a different place in a URI. No prefix
    /// can show a part of the key.
    #[test]
    fn prefixes_never_show_the_key() {
        let text = "SELECT 'ü'; ATTACH 'file:ä.db?cipher=aegis256&hexkey= 5ec7e7&x=1' AS \"ö\"";
        for end in (0..=text.len()).filter(|end| text.is_char_boundary(*end)) {
            let redacted = redact(&text[..end]);
            assert!(!redacted.contains("5ec"), "{redacted}");
        }
    }

    /// Random text never makes the scans panic, and a key that a piece holds
    /// never shows. Ranges from a dialect can be wrong in any way, and they
    /// never make the formatting panic.
    #[test]
    fn random_text_never_shows_the_key() {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        let pieces = [
            "SELECT ",
            "ATTACH ",
            "hexkey",
            "?",
            "&",
            "=",
            " ",
            "'",
            "\"",
            "#",
            ";",
            "'file:ä.db?cipher=aegis256&hexkey=5ec7e7'",
            "x'5ec7e7'",
            "ü",
            "\u{A0}",
        ];
        let mut rng = StdRng::seed_from_u64(11);
        for _ in 0..20_000 {
            let len = rng.random_range(0..24);
            let text: String = (0..len)
                .map(|_| pieces[rng.random_range(0..pieces.len())])
                .collect();
            let redacted = redact(&text);
            assert!(
                !redacted.contains("hexkey=5ec7e7"),
                "{text:?} -> {redacted:?}"
            );
            let ranges = (0..rng.random_range(0..4))
                .map(|_| rng.random_range(0..=text.len() + 2)..rng.random_range(0..=text.len() + 2))
                .collect();
            WithRanges(&text, ranges).to_string();
        }
    }
}
