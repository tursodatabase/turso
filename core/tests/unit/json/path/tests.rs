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
    let invalid_values = vec![
        "", "$$$", "$.", "$ ", "$[", "$]", "$[-1]", "x", "[]", "$[0", "$[0x]", "$\"",
    ];

    for value in invalid_values {
        let path = json_path(value);

        match path {
            Err(crate::error::LimboError::ParseError(_)) => {
                // happy path
            }
            _ => panic!("Expected error for: {value:?}, got: {path:?}"),
        }
    }
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
fn test_edge_cases() {
    // Empty key
    assert!(json_path("$.").is_err());

    // Multiple dots
    assert!(json_path("$..key").is_err());

    // Unclosed brackets
    assert!(json_path("$[0").is_err());
    assert!(json_path("$[").is_err());

    // Invalid negative index format
    assert!(json_path("$[-1]").is_err()); // should be $[#-1]
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
    // Issue #6099: bracket notation with double-quoted key.
    // Parsed successfully but produces BracketQuotedKey (never matches
    // during extraction — SQLite compat, always returns NULL).
    let path = json_path(r#"$["key"]"#).unwrap();
    assert_eq!(path.elements.len(), 2);
    assert_eq!(path.elements[0], PathElement::Root());
    assert_eq!(
        path.elements[1],
        PathElement::BracketQuotedKey(Cow::Borrowed("key"))
    );

    // Key containing spaces (the original issue example).
    let path = json_path(r#"$["key with spaces"]"#).unwrap();
    assert_eq!(
        path.elements[1],
        PathElement::BracketQuotedKey(Cow::Borrowed("key with spaces"))
    );

    // Key containing dots.
    let path = json_path(r#"$["key.with.dots"]"#).unwrap();
    assert_eq!(
        path.elements[1],
        PathElement::BracketQuotedKey(Cow::Borrowed("key.with.dots"))
    );

    // Key containing brackets.
    let path = json_path(r#"$["key[0]"]"#).unwrap();
    assert_eq!(
        path.elements[1],
        PathElement::BracketQuotedKey(Cow::Borrowed("key[0]"))
    );

    // Single-quoted variant.
    let path = json_path(r#"$['key']"#).unwrap();
    assert_eq!(
        path.elements[1],
        PathElement::BracketQuotedKey(Cow::Borrowed("key"))
    );

    // Empty quoted key.
    let path = json_path(r#"$[""]"#).unwrap();
    assert_eq!(
        path.elements[1],
        PathElement::BracketQuotedKey(Cow::Borrowed(""))
    );

    // Mixed with dot notation and array indices.
    let path = json_path(r#"$.outer["inner key"][2]"#).unwrap();
    assert_eq!(path.elements.len(), 4);
    assert_eq!(
        path.elements[1],
        PathElement::Key(Cow::Borrowed("outer"), false)
    );
    assert_eq!(
        path.elements[2],
        PathElement::BracketQuotedKey(Cow::Borrowed("inner key"))
    );
    assert_eq!(path.elements[3], PathElement::ArrayLocator(Some(2)));
}

#[test]
fn test_bracket_quoted_key_invalid() {
    // Unclosed quote.
    assert!(json_path(r#"$["key"#).is_err());
    // Closing quote but no closing bracket.
    assert!(json_path(r#"$["key""#).is_err());
    // Mismatched quote characters (single quote inside double-quoted key
    // is fine; this case has the wrong closing quote).
    assert!(json_path(r#"$["key']"#).is_err());
    // Trailing junk between closing quote and bracket.
    assert!(json_path(r#"$["key"x]"#).is_err());
}
