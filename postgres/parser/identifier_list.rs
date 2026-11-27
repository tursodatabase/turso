//! PostgreSQL's SplitIdentifierString, which reads the lists of identifiers
//! of the `search_path` setting and of the names that `regclass` and the
//! other reg types take.
//!
//! This is a translation of `SplitIdentifierString` of
//! `src/backend/utils/adt/varlena.c`, and of the functions it calls in
//! `src/backend/parser/scansup.c` (`scanner_isspace`,
//! `downcase_truncate_identifier` and `truncate_identifier`), from
//! PostgreSQL 18 (REL_18_6), for a database in UTF-8. The control flow of the C is kept,
//! so that a difference from PostgreSQL can be found by reading the two side
//! by side.
//!
//! The C modifies its input in place and gives pointers into it. Here the
//! input is read by position and each name is a new `String`. The input is
//! UTF-8, and the characters that the C compares with, the quote, the
//! separator and white space, are ASCII, so the bytes of a multibyte
//! character are never taken for them.

/// The length of the longest name plus one, in bytes (`NAMEDATALEN`).
const NAMEDATALEN: usize = 64;

/// `scanner_isspace`: whether the lexer counts `ch` as white space. `None` is
/// the end of the string, the NUL of the C.
fn scanner_isspace(ch: Option<u8>) -> bool {
    matches!(
        ch,
        Some(b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c) // 0x0b is \v and 0x0c is \f
    )
}

/// `truncate_identifier`, without the warning: the name cut to
/// `NAMEDATALEN - 1` bytes, at the end of a character (`pg_mbcliplen`).
fn truncate_identifier(mut ident: Vec<u8>) -> Vec<u8> {
    if ident.len() >= NAMEDATALEN {
        let mut len = NAMEDATALEN - 1;
        // A continuation byte of UTF-8 is 10xxxxxx.
        while len > 0 && ident[len] & 0xC0 == 0x80 {
            len -= 1;
        }
        ident.truncate(len);
    }
    ident
}

/// `downcase_truncate_identifier`, without the warning. For UTF-8 the
/// downcasing is for the ASCII letters only: `enc_is_single_byte` is false.
fn downcase_truncate_identifier(ident: &[u8]) -> Vec<u8> {
    truncate_identifier(ident.to_ascii_lowercase())
}

/// `SplitIdentifierString`: the identifiers of `rawstring`, which `separator`
/// separates, or None when the string has a syntax error. White space may
/// appear around an identifier. A name in double quotes keeps its case, and
/// two quotes in it are one quote. Another name has its ASCII letters in
/// lower case. A name is cut to the longest one PostgreSQL has. An empty
/// string is a list of no names.
pub fn split_identifier_string(rawstring: &str, separator: char) -> Option<Vec<String>> {
    let separator = u8::try_from(separator).expect("a separator is an ASCII character");
    let raw = rawstring.as_bytes();
    // The byte at `i`, or None for the NUL at the end of a C string.
    let at = |i: usize| raw.get(i).copied();

    let mut next = 0;
    let mut namelist = Vec::new();

    // skip leading whitespace
    while scanner_isspace(at(next)) {
        next += 1;
    }

    // allow empty string
    if at(next).is_none() {
        return Some(namelist);
    }

    // At the top of the loop, we are at start of a new identifier.
    loop {
        let curname: Vec<u8>;

        if at(next) == Some(b'"') {
            // Quoted name --- collapse quote-quote pairs, no downcasing
            let mut name = Vec::new();
            let mut start = next + 1;
            loop {
                // endp = strchr(nextp + 1, '"'), where nextp + 1 is `start`
                // once pairs of quotes have been collapsed.
                let endp = start + raw[start..].iter().position(|&b| b == b'"')?; // mismatched quotes
                if at(endp + 1) != Some(b'"') {
                    // found end of quoted name
                    name.extend_from_slice(&raw[start..endp]);
                    // endp points at the terminating quote
                    next = endp + 1;
                    break;
                }
                // Collapse adjacent quotes into one quote, and look again
                name.extend_from_slice(&raw[start..=endp]);
                start = endp + 2;
            }
            curname = name;
        } else {
            // Unquoted name --- extends to separator or whitespace
            let start = next;
            while at(next).is_some_and(|b| b != separator && !scanner_isspace(Some(b))) {
                next += 1;
            }
            // empty unquoted name not allowed
            if start == next {
                return None;
            }
            // Downcase the identifier, using same code as main lexer does.
            curname = downcase_truncate_identifier(&raw[start..next]);
        }

        // skip trailing whitespace
        while scanner_isspace(at(next)) {
            next += 1;
        }

        let done;
        if at(next) == Some(separator) {
            next += 1;
            // skip leading whitespace for next
            while scanner_isspace(at(next)) {
                next += 1;
            }
            // we expect another name, so done remains false
            done = false;
        } else if at(next).is_none() {
            done = true;
        } else {
            // invalid syntax
            return None;
        }

        // Truncate name if it's overlength, and add it to the list.
        let curname = truncate_identifier(curname);
        namelist.push(
            String::from_utf8(curname)
                .expect("a name is cut at ASCII characters and at the end of a character"),
        );

        // Loop back if we didn't reach end of string
        if done {
            return Some(namelist);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::split_identifier_string;

    fn names(input: &str, separator: char) -> Option<Vec<String>> {
        split_identifier_string(input, separator)
    }

    fn some(names: &[&str]) -> Option<Vec<String>> {
        Some(names.iter().map(|name| name.to_string()).collect())
    }

    #[test]
    fn an_empty_string_is_a_list_of_no_names() {
        assert_eq!(names("", ','), some(&[]));
        assert_eq!(names(" \t\n\r\u{b}\u{c}", ','), some(&[]));
    }

    #[test]
    fn names_are_lowercased_unless_quoted() {
        assert_eq!(
            names(" PUBLIC , \"MiXeD\" ", ','),
            some(&["public", "MiXeD"])
        );
        assert_eq!(names("Public . \"T\"", '.'), some(&["public", "T"]));
        assert_eq!(names("$USER", ','), some(&["$user"]));
        // Only the ASCII letters are lowercased.
        assert_eq!(names("ÉA", ','), some(&["Éa"]));
    }

    #[test]
    fn a_quoted_name_keeps_its_characters_and_collapses_pairs_of_quotes() {
        assert_eq!(names("\"a,b\", \"a\"\"b\"", ','), some(&["a,b", "a\"b"]));
        assert_eq!(names("\"\"\"\"", ','), some(&["\""]));
        assert_eq!(names("\"a\"\"\"\"b\"", ','), some(&["a\"\"b"]));
        // An empty quoted name is accepted.
        assert_eq!(names("\"\", public", ','), some(&["", "public"]));
    }

    #[test]
    fn white_space_is_that_of_the_lexer() {
        // A vertical tab and a form feed are white space, a no-break space is not.
        assert_eq!(names("\u{b}PUBLIC\u{c}", ','), some(&["public"]));
        assert_eq!(names("t9\u{a0}", '.'), some(&["t9\u{a0}"]));
        assert_eq!(names("public\"extra", ','), some(&["public\"extra"]));
    }

    #[test]
    fn a_name_is_cut_to_the_longest_identifier_at_the_end_of_a_character() {
        assert_eq!(names(&"a".repeat(70), '.'), Some(vec!["a".repeat(63)]));
        assert_eq!(
            names(&format!("\"{}\"", "a".repeat(70)), '.'),
            Some(vec!["a".repeat(63)])
        );
        // The last character would end at the byte 64, so it is dropped.
        assert_eq!(
            names(&format!("{}é", "b".repeat(62)), '.'),
            Some(vec!["b".repeat(62)])
        );
        assert_eq!(
            names(&format!("{}é", "b".repeat(61)), '.'),
            Some(vec![format!("{}é", "b".repeat(61))])
        );
    }

    #[test]
    fn a_list_with_a_syntax_error_is_refused() {
        for (input, separator) in [
            (",public", ','),
            ("public,", ','),
            ("public, ", ','),
            ("public,,main", ','),
            ("one two", ','),
            ("\"\"\"", ','),
            ("\"unclosed", ','),
            ("\"a\"b", ','),
            ("\"a\" \"b\"", ','),
            ("a.", '.'),
            (".a", '.'),
            ("a..b", '.'),
        ] {
            assert_eq!(names(input, separator), None, "{input:?}");
        }
    }
}
