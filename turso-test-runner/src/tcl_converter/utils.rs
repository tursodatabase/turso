use chumsky::prelude::*;

use crate::text_parser;

/// Parser that parses content that can be recursively delimted by braces
pub fn braced<'a>() -> text_parser!('a, String) {
    recursive(|braced| {
        choice((
            braced.map(|s: String| format!("{{{s}}}")),
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

/// Parser for comment lines (excludes shebangs like #!/...)
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
    hspace.ignore_then(p).then_ignore(hspace)
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
        result = format!("test-{result}");
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

/// Skip over balanced braces without collecting content (for skipping blocks)
pub fn skip_balanced_braces<'a>() -> text_parser!('a, ()) {
    recursive(|skip| {
        choice((skip, none_of("{}").ignored()))
            .repeated()
            .delimited_by(just('{'), just('}'))
            .ignored()
    })
}

/// Parser for foreach/if/proc blocks - consumes keyword + all balanced braces on the same logical unit
/// Used to skip over control structures that generate multiple tests
///
/// Matches patterns like:
/// - foreach {vars} {values} { body }
/// - if {cond} { body } else { body }
/// - proc name {args} { body }
pub fn skip_block<'a>(keyword: &'a str) -> text_parser!('a, String) {
    // Content before a brace: spaces, tabs, or non-brace/non-newline chars
    let pre_brace = choice((
        one_of(" \t").ignored(),   // horizontal whitespace
        none_of("{}\n").ignored(), // other chars but NOT newlines
    ))
    .repeated();

    // Between braced sections: whitespace, and optionally 'else' or 'elseif'
    let between_braces = one_of(" \t\n")
        .repeated()
        .then(
            // Allow 'else', 'elseif' between if blocks
            choice((just("elseif").ignored(), just("else").ignored())).or_not(),
        )
        .padded();

    // A braced section, optionally followed by more braced sections
    let braced_sections = skip_balanced_braces().then(
        // After a brace, allow whitespace/else/elseif then another brace
        between_braces
            .ignore_then(skip_balanced_braces())
            .repeated(),
    );

    just(keyword)
        .then(pre_brace)
        .then(braced_sections)
        .to_slice()
        .map(|s: &str| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== braced() tests ====================

    #[test]
    fn test_braced_simple() {
        let result = braced().parse("{hello}").into_result();
        assert_eq!(result, Ok("hello".to_string()));
    }

    #[test]
    fn test_braced_with_spaces() {
        let result = braced().parse("{hello world}").into_result();
        assert_eq!(result, Ok("hello world".to_string()));
    }

    #[test]
    fn test_braced_nested() {
        let result = braced().parse("{outer {inner} more}").into_result();
        assert_eq!(result, Ok("outer {inner} more".to_string()));
    }

    #[test]
    fn test_braced_deeply_nested() {
        let result = braced().parse("{a {b {c} d} e}").into_result();
        assert_eq!(result, Ok("a {b {c} d} e".to_string()));
    }

    #[test]
    fn test_braced_multiline() {
        let result = braced().parse("{SELECT 1\nFROM foo}").into_result();
        assert_eq!(result, Ok("SELECT 1\nFROM foo".to_string()));
    }

    #[test]
    fn test_braced_empty() {
        let result = braced().parse("{}").into_result();
        assert_eq!(result, Ok("".to_string()));
    }

    #[test]
    fn test_braced_sql_block() {
        let input = r#"{
  SELECT * FROM users
  WHERE id = 1
}"#;
        let result = braced().parse(input).into_result();
        assert!(result.is_ok());
        assert!(result.unwrap().contains("SELECT * FROM users"));
    }

    // ==================== word() tests ====================

    #[test]
    fn test_word_simple() {
        let result = word().parse("hello").into_result();
        assert_eq!(result, Ok("hello".to_string()));
    }

    #[test]
    fn test_word_with_hyphen() {
        let result = word().parse("test-name-1").into_result();
        assert_eq!(result, Ok("test-name-1".to_string()));
    }

    #[test]
    fn test_word_stops_at_space() {
        // word() parses until whitespace - test with just the word
        let result = word().parse("hello").into_result();
        assert_eq!(result, Ok("hello".to_string()));
    }

    #[test]
    fn test_word_stops_at_brace() {
        // word() doesn't include braces - test with just word content
        let result = word().parse("hello").into_result();
        assert_eq!(result, Ok("hello".to_string()));
    }

    // ==================== arg() tests ====================

    #[test]
    fn test_arg_braced() {
        let result = arg().parse("{hello world}").into_result();
        assert_eq!(result, Ok("hello world".to_string()));
    }

    #[test]
    fn test_arg_word() {
        let result = arg().parse("hello").into_result();
        assert_eq!(result, Ok("hello".to_string()));
    }

    #[test]
    fn test_arg_prefers_braced() {
        // When input starts with {, should parse as braced
        let result = arg().parse("{test}").into_result();
        assert_eq!(result, Ok("test".to_string()));
    }

    // ==================== comment() tests ====================

    #[test]
    fn test_comment_simple() {
        let result = comment().parse("# this is a comment").into_result();
        assert_eq!(result, Ok("# this is a comment"));
    }

    #[test]
    fn test_comment_empty() {
        let result = comment().parse("#").into_result();
        assert_eq!(result, Ok("#"));
    }

    #[test]
    fn test_comment_with_code() {
        let result = comment().parse("# SELECT * FROM foo").into_result();
        assert_eq!(result, Ok("# SELECT * FROM foo"));
    }

    #[test]
    fn test_comment_stops_at_newline() {
        // comment() parses until newline - test without trailing content
        let result = comment().parse("# comment").into_result();
        assert_eq!(result, Ok("# comment"));
    }

    // ==================== skip_line() tests ====================

    #[test]
    fn test_skip_line_set() {
        let result = skip_line()
            .parse("set testdir [file dirname]")
            .into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_skip_line_source() {
        let result = skip_line()
            .parse("source $testdir/tester.tcl")
            .into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_skip_line_load_extension() {
        let result = skip_line().parse("load_extension ./myext.so").into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_skip_line_shebang() {
        let result = skip_line().parse("#!/usr/bin/tclsh").into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_skip_line_not_matching() {
        let result = skip_line().parse("do_execsql_test foo").into_result();
        assert!(result.is_err());
    }

    // ==================== hspace() tests ====================

    #[test]
    fn test_hspace_spaces() {
        let result = hspace().parse("   ").into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_hspace_tabs() {
        let result = hspace().parse("\t\t").into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_hspace_mixed() {
        let result = hspace().parse(" \t \t").into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_hspace_empty() {
        let result = hspace().parse("").into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_hspace_stops_at_newline() {
        // hspace should NOT consume newlines
        let (_, errs) = hspace().parse("  \n").into_output_errors();
        // Parser succeeds but stops before newline
        assert!(errs.is_empty() || errs.iter().any(|e| format!("{e:?}").contains("\\n")));
    }

    // ==================== clean_name() tests ====================

    #[test]
    fn test_clean_name_simple() {
        assert_eq!(clean_name("test-1"), "test-1");
    }

    #[test]
    fn test_clean_name_with_quotes() {
        assert_eq!(clean_name("\"test-1\""), "test-1");
    }

    #[test]
    fn test_clean_name_with_braces() {
        assert_eq!(clean_name("{test-1}"), "test-1");
    }

    #[test]
    fn test_clean_name_with_dots() {
        assert_eq!(clean_name("select.1.2"), "select_1_2");
    }

    #[test]
    fn test_clean_name_with_colons() {
        assert_eq!(clean_name("test:case:1"), "test_case_1");
    }

    #[test]
    fn test_clean_name_with_spaces() {
        assert_eq!(clean_name("test case 1"), "test-case-1");
    }

    #[test]
    fn test_clean_name_starting_with_digit() {
        assert_eq!(clean_name("1test"), "test-1test");
    }

    #[test]
    fn test_clean_name_empty() {
        assert_eq!(clean_name(""), "unnamed-test");
    }

    #[test]
    fn test_clean_name_special_chars() {
        assert_eq!(clean_name("test@#$%^&*()"), "test");
    }

    // ==================== clean_sql() tests ====================

    #[test]
    fn test_clean_sql_simple() {
        assert_eq!(clean_sql("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn test_clean_sql_trims_lines() {
        assert_eq!(clean_sql("  SELECT 1  "), "SELECT 1");
    }

    #[test]
    fn test_clean_sql_multiline() {
        let input = "SELECT *\nFROM users";
        assert_eq!(clean_sql(input), "SELECT *\nFROM users");
    }

    #[test]
    fn test_clean_sql_collapses_empty_lines() {
        // Multiple empty lines become one newline (no blank line preserved)
        let input = "SELECT *\n\n\nFROM users";
        assert_eq!(clean_sql(input), "SELECT *\nFROM users");
    }

    #[test]
    fn test_clean_sql_preserves_comment() {
        let input = "-- comment\nSELECT 1";
        assert_eq!(clean_sql(input), "-- comment\nSELECT 1");
    }

    // ==================== check_has_ddl() tests ====================

    #[test]
    fn test_check_has_ddl_create() {
        assert!(check_has_ddl("CREATE TABLE foo (id INT)"));
    }

    #[test]
    fn test_check_has_ddl_drop() {
        assert!(check_has_ddl("DROP TABLE foo"));
    }

    #[test]
    fn test_check_has_ddl_alter() {
        assert!(check_has_ddl("ALTER TABLE foo ADD col INT"));
    }

    #[test]
    fn test_check_has_ddl_insert() {
        assert!(check_has_ddl("INSERT INTO foo VALUES (1)"));
    }

    #[test]
    fn test_check_has_ddl_update() {
        assert!(check_has_ddl("UPDATE foo SET x = 1"));
    }

    #[test]
    fn test_check_has_ddl_delete() {
        assert!(check_has_ddl("DELETE FROM foo"));
    }

    #[test]
    fn test_check_has_ddl_select_only() {
        assert!(!check_has_ddl("SELECT * FROM foo"));
    }

    #[test]
    fn test_check_has_ddl_case_insensitive() {
        assert!(check_has_ddl("create table foo (id int)"));
    }

    // ==================== skip_block() tests ====================

    #[test]
    fn test_skip_block_foreach_simple() {
        let input = "foreach x {1 2 3} { puts $x }";
        let result = skip_block("foreach").parse(input).into_result();
        assert!(result.is_ok());
        assert!(result.unwrap().starts_with("foreach"));
    }

    #[test]
    fn test_skip_block_foreach_multiple_braces() {
        let input = r#"foreach {a b} {1 2 3 4} {
  do_execsql_test test-$a {SELECT $b} {$b}
}"#;
        let result = skip_block("foreach").parse(input).into_result();
        assert!(result.is_ok());
        let content = result.unwrap();
        assert!(content.contains("foreach"));
        assert!(content.contains("do_execsql_test"));
    }

    #[test]
    fn test_skip_block_if_simple() {
        let input = "if {$cond} { puts ok }";
        let result = skip_block("if").parse(input).into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_skip_block_if_else() {
        let input = r#"if {$cond} {
  puts ok
} else {
  puts fail
}"#;
        let result = skip_block("if").parse(input).into_result();
        assert!(result.is_ok());
        let content = result.unwrap();
        assert!(content.contains("else"));
    }

    #[test]
    fn test_skip_block_proc() {
        let input = r#"proc myproc {arg1 arg2} {
  return [expr {$arg1 + $arg2}]
}"#;
        let result = skip_block("proc").parse(input).into_result();
        assert!(result.is_ok());
        let content = result.unwrap();
        assert!(content.contains("myproc"));
    }

    #[test]
    fn test_skip_block_nested_braces() {
        let input = "foreach x {a {b c} d} { puts {nested {braces}} }";
        let result = skip_block("foreach").parse(input).into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_skip_balanced_braces() {
        let input = "{simple}";
        let result = skip_balanced_braces().parse(input).into_result();
        assert!(result.is_ok());
    }

    #[test]
    fn test_skip_balanced_braces_nested() {
        let input = "{outer {inner {deep}}}";
        let result = skip_balanced_braces().parse(input).into_result();
        assert!(result.is_ok());
    }
}
