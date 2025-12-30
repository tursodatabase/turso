use chumsky::prelude::*;

use crate::text_parser;

/// Parser that parses content that can be recursively delimted by braces
pub fn braced<'a>() -> text_parser!('a, String) {
    recursive(|braced| {
        choice((
            braced.map(|s: String| format!("{{{}}}", s)),
            none_of("{}").map(|c: char| c.to_string()),
        ))
        .repeated()
        .collect::<Vec<_>>()
        .map(|s| s.concat())
        .delimited_by(just('{'), just('}'))
    })
}

/// Word argument (unbraced, no whitespace)
pub fn word<'a>() -> impl Parser<'a, &'a str, String, extra::Err<Rich<'a, char>>> {
    none_of(" \t\n{}")
        .repeated()
        .at_least(1)
        .collect::<String>()
}

/// Single argument: braced or word
pub fn arg<'a>() -> text_parser!('a, String) {
    choice((braced(), word()))
}

/// Parser for comment lines
pub fn comment<'a>() -> text_parser!('a, &'a str) {
    just('#').then(none_of('\n').repeated()).to_slice()
}

/// Parser for lines to skip (set, source, load_extension, etc.)
pub fn skip_line<'a>() -> impl Parser<'a, &'a str, (), extra::Err<Rich<'a, char>>> {
    choice((
        just("set "),
        just("source "),
        just("load_extension"),
        just("#!/"),
    ))
    .then(none_of('\n').repeated())
    .ignored()
}

/// Consume horizontal whitespace, not newlines
pub fn hspace<'a>() -> text_parser!('a, ()) {
    one_of(" \t").repeated()
}

/// Horizontal whitespace only (no newlines)
pub fn hpad<'a, T>(p: text_parser!('a, T)) -> text_parser!('a, T) {
    let hspace = one_of(" \t").repeated();
    hspace.clone().ignore_then(p).then_ignore(hspace)
}

/// Clean up a test name (remove quotes, convert invalid chars)
pub fn clean_name(s: &str) -> String {
    let name = s
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches('{')
        .trim_matches('}');

    // Replace characters not allowed in identifiers
    let mut result = String::new();
    for ch in name.chars() {
        match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' => result.push(ch),
            '.' => result.push('_'), // Replace dots with underscore
            ':' => result.push('_'), // Replace colons with underscore
            ' ' => result.push('-'), // Replace spaces with hyphen
            _ => {}                  // Skip other invalid chars
        }
    }

    // Ensure name starts with a letter or underscore
    if result.is_empty() {
        return "unnamed-test".to_string();
    }

    if result
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
    {
        result = format!("test-{}", result);
    }

    result
}

/// Clean up SQL (normalize whitespace, preserve comments)
pub fn clean_sql(s: &str) -> String {
    let mut result = String::new();
    let mut prev_was_empty = false;

    for line in s.lines() {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            // Collapse multiple empty lines into one
            if !prev_was_empty && !result.is_empty() {
                result.push('\n');
                prev_was_empty = true;
            }
        } else {
            if !result.is_empty() && !prev_was_empty {
                result.push('\n');
            }
            result.push_str(trimmed);
            prev_was_empty = false;
        }
    }

    result
}

/// Check if SQL contains DDL statements
pub fn check_has_ddl(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.contains("CREATE ")
        || upper.contains("DROP ")
        || upper.contains("ALTER ")
        || upper.contains("INSERT ")
        || upper.contains("UPDATE ")
        || upper.contains("DELETE ")
}
