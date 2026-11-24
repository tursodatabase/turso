use std::borrow::Cow;

/// Formats a path for the bad-path message the way SQLite's %Q does:
/// every apostrophe in the path is doubled inside the quoted output.
/// %Q reads a C string, so anything after an embedded NUL is dropped.
fn quote_path(path: &str) -> String {
    let path = &path[..path.find('\0').unwrap_or(path.len())];
    path.replace('\'', "''")
}

pub fn bad_path_error(path: &str) -> crate::LimboError {
    crate::LimboError::ParseError(format!("bad JSON path: '{}'", quote_path(path)))
}

/// Describes a JSON path, which is a sequence of keys and/or array locators.
#[derive(Clone, Debug)]
pub struct JsonPath<'a> {
    pub elements: Vec<PathElement<'a>>,
}

impl JsonPath<'_> {
    pub fn into_owned(self) -> JsonPath<'static> {
        let elements = self
            .elements
            .into_iter()
            .map(|element| match element {
                PathElement::Root() => PathElement::Root(),
                PathElement::Key(key, raw) => PathElement::Key(Cow::Owned(key.into_owned()), raw),
                PathElement::ArrayLocator(index) => PathElement::ArrayLocator(index),
                PathElement::Malformed(step) => PathElement::Malformed(MalformedStep {
                    path: Cow::Owned(step.path.into_owned()),
                    kind: step.kind,
                }),
            })
            .collect();
        JsonPath { elements }
    }
}

type RawString = bool;

/// PathElement describes a single element of a JSON path.
#[derive(Clone, Debug, PartialEq)]
pub enum PathElement<'a> {
    /// Root element: '$'
    Root(),
    /// JSON key
    Key(Cow<'a, str>, RawString),
    /// Array locator, eg. [2], [#-5]
    ArrayLocator(Option<i32>),
    /// A step that is not valid path syntax. It is the last element of the
    /// path.
    Malformed(MalformedStep<'a>),
}

/// A step that is not valid path syntax. SQLite reads each step of a path
/// only when a lookup applies the step to an element, so the step is an
/// error only when the steps before it find an element, and only when that
/// element is one that SQLite reads the step on.
#[derive(Clone, Debug, PartialEq)]
pub struct MalformedStep<'a> {
    /// The whole path, which the error message quotes.
    pub path: Cow<'a, str>,
    pub kind: MalformedStepKind,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MalformedStepKind {
    /// A step that starts with `[`. SQLite reads it only on an array, so it
    /// finds nothing in any other element. `from_end` is N when the step
    /// starts with `[#-N`: SQLite finds nothing in an array of fewer than N
    /// elements before it reads the rest of the step.
    Array { from_end: Option<u32> },
    /// Any other step, which SQLite reads on any element.
    Other,
}

impl PathElement<'_> {
    /// Array locator for `[index]`, or for `[#-index]` when `from_end` is set.
    pub fn array_locator(index: u32, from_end: bool) -> PathElement<'static> {
        // An index that does not fit in an i32 becomes i32::MAX or i32::MIN.
        // Only an array of more than 2^31 elements could match those, so in
        // practice the path finds nothing, as in SQLite.
        match (from_end, index) {
            // "#-0" addresses the same one-past-the-end position as a bare "#".
            (true, 0) => PathElement::ArrayLocator(None),
            (true, _) => PathElement::ArrayLocator(Some(
                i32::try_from(-i64::from(index)).unwrap_or(i32::MIN),
            )),
            (false, _) => PathElement::ArrayLocator(Some(i32::try_from(index).unwrap_or(i32::MAX))),
        }
    }
}

fn estimate_path_capacity(input: &str) -> usize {
    // After $ we need either . or [ for each component
    // So divide remaining length by 2 (minimum chars per component)
    // Add 1 for the root component
    1 + (input.len() - 1) / 2
}

/// Parses a path that starts with `$` into its elements. Like SQLite, the
/// path is read one step at a time, and a step that is not valid path
/// syntax becomes a [PathElement::Malformed] element that ends the path.
pub fn json_path(path: &str) -> crate::Result<JsonPath<'_>> {
    let Some(mut rest) = path.strip_prefix('$') else {
        return Err(bad_path_error(path));
    };
    let mut elements = Vec::with_capacity(estimate_path_capacity(path));
    elements.push(PathElement::Root());
    while !rest.is_empty() {
        let step = match rest.as_bytes()[0] {
            b'.' => key_step(rest),
            b'[' => array_step(rest),
            _ => None,
        };
        let Some((element, step_len)) = step else {
            let kind = if rest.starts_with('[') {
                MalformedStepKind::Array {
                    from_end: from_end_number(rest).map(|(index, _)| index),
                }
            } else {
                MalformedStepKind::Other
            };
            elements.push(PathElement::Malformed(MalformedStep {
                path: Cow::Borrowed(path),
                kind,
            }));
            break;
        };
        elements.push(element);
        rest = &rest[step_len..];
    }
    Ok(JsonPath { elements })
}

/// Parses the `.` step at the start of `step` the way SQLite's
/// jsonLookupStep does. Returns the key and the length of the step, or None
/// when the key is empty or its closing quote is missing.
fn key_step(step: &str) -> Option<(PathElement<'_>, usize)> {
    let key = &step[1..];
    if let Some(quoted) = key.strip_prefix('"') {
        let bytes = quoted.as_bytes();
        let mut i = 0;
        while i < bytes.len() && bytes[i] != b'"' {
            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                i += 1;
            }
            i += 1;
        }
        if i == bytes.len() {
            return None;
        }
        // The step is the dot, the key and its two quotes.
        return Some((PathElement::Key(Cow::Borrowed(&quoted[..i]), true), i + 3));
    }
    let key_len = key.find(['.', '[']).unwrap_or(key.len());
    if key_len == 0 {
        return None;
    }
    Some((
        PathElement::Key(Cow::Borrowed(&key[..key_len]), false),
        key_len + 1,
    ))
}

/// Parses the `[N]`, `[#]` or `[#-N]` step at the start of `step` the way
/// SQLite's jsonLookupStep does. Returns the array locator and the length of
/// the step, or None when the step is not one of these.
fn array_step(step: &str) -> Option<(PathElement<'static>, usize)> {
    let (index, digits) = leading_number(&step[1..]);
    let (locator, len) = if digits > 0 {
        (PathElement::array_locator(index, false), 1 + digits)
    } else if step[1..].starts_with('#') {
        match from_end_number(step) {
            Some((index, digits)) => (PathElement::array_locator(index, true), 3 + digits),
            None => (PathElement::ArrayLocator(None), 2),
        }
    } else {
        return None;
    };
    (step.as_bytes().get(len) == Some(&b']')).then_some((locator, len + 1))
}

/// Returns N and the number of its digits when `step` starts with `[#-N`.
fn from_end_number(step: &str) -> Option<(u32, usize)> {
    let (index, digits) = leading_number(step.strip_prefix("[#-")?);
    (digits > 0).then_some((index, digits))
}

/// Returns the number that the ASCII digits at the start of `text` spell and
/// the number of those digits. Like SQLite, the number is accumulated in a
/// u32, so a long number wraps modulo 2^32.
fn leading_number(text: &str) -> (u32, usize) {
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    let number = text.as_bytes()[..digits]
        .iter()
        .fold(0u32, |number, &digit| {
            number
                .wrapping_mul(10)
                .wrapping_add(u32::from(digit - b'0'))
        });
    (number, digits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_path_root() {
        let path = json_path("$").unwrap();
        assert_eq!(path.elements.len(), 1);
        assert_eq!(path.elements[0], PathElement::Root());
    }

    #[test]
    fn test_json_path_single_locator() {
        let path = json_path("$.x").unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("x"), false)
        );
    }

    #[test]
    fn test_json_path_single_array_locator() {
        let path = json_path("$[0]").unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(0)));
    }

    #[test]
    fn test_json_path_single_negative_array_locator() {
        let path = json_path("$[#-2]").unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(-2)));
    }

    #[test]
    fn test_json_path_invalid() {
        for value in ["", "x", "[]", " $"] {
            let path = json_path(value);
            match path {
                Err(crate::error::LimboError::ParseError(_)) => {}
                _ => panic!("Expected error for: {value:?}, got: {path:?}"),
            }
        }
    }

    /// Returns the elements of `path` before its malformed step, and the
    /// kind of that step.
    fn split_malformed(path: &str) -> (Vec<PathElement<'_>>, MalformedStepKind) {
        let mut elements = json_path(path).unwrap().elements;
        match elements.pop() {
            Some(PathElement::Malformed(step)) => (elements, step.kind),
            last => panic!("Expected a malformed step at the end of {path:?}, got {last:?}"),
        }
    }

    #[test]
    fn test_malformed_step_ends_path() {
        let other = MalformedStepKind::Other;
        let array = MalformedStepKind::Array { from_end: None };
        for (path, valid_steps, kind) in [
            ("$$$", 1, other),
            ("$ ", 1, other),
            ("$.", 1, other),
            ("$..key", 1, other),
            ("$.a.", 2, other),
            ("$.a.[0]", 2, other),
            (r#"$."a"x"#, 2, other),
            (r#"$."a"#, 1, other),
            (r#"$."a\""#, 1, other),
            ("$]", 1, other),
            ("$[", 1, array),
            ("$[0", 1, array),
            ("$[0x]", 1, array),
            ("$[-1]", 1, array),
            ("$[ 0]", 1, array),
            ("$[]", 1, array),
            ("$[#x]", 1, array),
            ("$[#-]", 1, array),
            ("$[0]]", 2, other),
            ("$.a[-1].b", 2, array),
            ("$.b[", 2, array),
            (
                "$[#-12x",
                1,
                MalformedStepKind::Array { from_end: Some(12) },
            ),
            ("$[#-0", 1, MalformedStepKind::Array { from_end: Some(0) }),
        ] {
            let (elements, step_kind) = split_malformed(path);
            assert_eq!(elements.len(), valid_steps, "{path}");
            assert_eq!(step_kind, kind, "{path}");
        }
    }

    #[test]
    fn test_malformed_step_error_quotes_whole_path() {
        let path = json_path("$.a'[-1]").unwrap();
        let Some(PathElement::Malformed(step)) = path.elements.last() else {
            panic!("Expected a malformed step, got {:?}", path.elements);
        };
        assert_eq!(
            bad_path_error(&step.path).to_string(),
            "bad JSON path: '$.a''[-1]'"
        );
    }

    #[test]
    fn test_json_path() {
        let path = json_path("$.store.book[0].title").unwrap();
        assert_eq!(path.elements.len(), 5);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("store"), false)
        );
        assert_eq!(
            path.elements[2],
            PathElement::Key(Cow::Borrowed("book"), false)
        );
        assert_eq!(path.elements[3], PathElement::ArrayLocator(Some(0)));
        assert_eq!(
            path.elements[4],
            PathElement::Key(Cow::Borrowed("title"), false)
        );
    }

    #[test]
    fn test_large_index_wrapping() {
        let path = json_path("$[4294967296]").unwrap();
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(0)));

        let path = json_path("$[4294967297]").unwrap();
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(1)));
    }

    #[test]
    fn test_deeply_nested_path() {
        let path = json_path("$[0][1][2].key[3].other").unwrap();
        assert_eq!(path.elements.len(), 7);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(0)));
        assert_eq!(path.elements[2], PathElement::ArrayLocator(Some(1)));
        assert_eq!(path.elements[3], PathElement::ArrayLocator(Some(2)));
        assert_eq!(
            path.elements[4],
            PathElement::Key(Cow::Borrowed("key"), false)
        );
        assert_eq!(path.elements[5], PathElement::ArrayLocator(Some(3)));
    }

    #[test]
    fn test_unquoted_key_keeps_quotes() {
        // Like SQLite, only a key that starts with a quote is quoted.
        let path = json_path(r#"$.a"b"[0]"#).unwrap();
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed(r#"a"b""#), false)
        );
        assert_eq!(path.elements[2], PathElement::ArrayLocator(Some(0)));
    }

    #[test]
    fn test_path_capacity() {
        // Test that our capacity estimation is reasonable
        let short_path = "$[0]";
        assert!(estimate_path_capacity(short_path) >= 2);

        let long_path = "$.a.b.c.d.e.f.g[0][1][2]";
        assert!(estimate_path_capacity(long_path) >= 11);
    }

    #[test]
    fn test_quoted_keys() {
        let path = json_path(r#"$."key""#).unwrap();
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("key"), true)
        );

        let path = json_path(r#"$."key.with.dots""#).unwrap();
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("key.with.dots"), true)
        );

        let path = json_path(r#"$."key[0]""#).unwrap();
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("key[0]"), true)
        );
    }

    #[test]
    fn test_empty_quoted_key() {
        assert!(json_path(r#"$."""#).is_ok());
    }

    #[test]
    fn test_quoted_key_after_multibyte_utf8_chars() {
        // Regression test for issue #5028
        // The path contains multi-byte UTF-8 chars before a quoted key.
        // This should not panic with "byte index is not a char boundary".
        // '՜' is a 2-byte UTF-8 character (bytes 2-3 in the path).
        // The important thing is that it doesn't panic - the result
        // (valid parse or parse error) is less important.
        let _ = json_path(r#"$.՜O'"R"RE"#);

        // Also test a simpler case where UTF-8 chars appear before a quoted key
        // $.世界"key" - Chinese characters followed by a quoted key
        let _ = json_path(r#"$.世界"key""#);

        // Test with a valid quoted key containing UTF-8 chars
        let path = json_path(r#"$."世界""#).unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("世界"), true)
        );
    }

    #[test]
    fn test_bracket_quoted_key() {
        // SQLite reads a quoted key in brackets as a malformed `[` step.
        for path in [
            r#"$["key"]"#,
            r#"$['key']"#,
            r#"$[""]"#,
            r#"$["key"#,
            r#"$["key"x]"#,
        ] {
            let (elements, kind) = split_malformed(path);
            assert_eq!(elements, vec![PathElement::Root()], "{path}");
            assert_eq!(kind, MalformedStepKind::Array { from_end: None }, "{path}");
        }
        let (elements, _) = split_malformed(r#"$.outer["inner key"][2]"#);
        assert_eq!(
            elements,
            vec![
                PathElement::Root(),
                PathElement::Key(Cow::Borrowed("outer"), false)
            ]
        );
    }
}
