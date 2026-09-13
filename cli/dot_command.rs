pub(crate) fn tokenize_dot_command(line: &str) -> (Vec<String>, Option<char>) {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut token_started = false;

    for ch in line.chars() {
        match quote {
            Some(delimiter) if ch == delimiter => quote = None,
            Some(_) => current.push(ch),
            None if ch.is_whitespace() => {
                if token_started {
                    args.push(std::mem::take(&mut current));
                    token_started = false;
                }
            }
            None if matches!(ch, '\'' | '"') && !token_started => {
                quote = Some(ch);
                token_started = true;
            }
            None => {
                current.push(ch);
                token_started = true;
            }
        }
    }

    if token_started {
        args.push(current);
    }

    (args, quote)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_unquoted_windows_paths() {
        assert_eq!(
            tokenize_dot_command(r#"import C:\Users\Jane\data.csv "people""#),
            (
                vec![
                    "import".to_string(),
                    r"C:\Users\Jane\data.csv".to_string(),
                    "people".to_string(),
                ],
                None,
            )
        );
    }

    #[test]
    fn groups_quoted_paths() {
        assert_eq!(
            tokenize_dot_command(r#"open "C:\Users\Jane Doe\test.db" custom_vfs"#),
            (
                vec![
                    "open".to_string(),
                    r"C:\Users\Jane Doe\test.db".to_string(),
                    "custom_vfs".to_string(),
                ],
                None,
            )
        );
    }

    #[test]
    fn preserves_quoted_unc_paths() {
        assert_eq!(
            tokenize_dot_command(r#"open "\\server\share name\test.db""#),
            (
                vec![
                    "open".to_string(),
                    r"\\server\share name\test.db".to_string(),
                ],
                None,
            )
        );
    }

    #[test]
    fn preserves_trailing_backslash_in_quoted_paths() {
        assert_eq!(
            tokenize_dot_command(r#"cd "C:\Users\Jane\""#),
            (vec!["cd".to_string(), r"C:\Users\Jane\".to_string()], None,)
        );
    }

    #[test]
    fn preserves_apostrophes_inside_unquoted_arguments() {
        assert_eq!(
            tokenize_dot_command(r"read C:\Users\O'Brien\script.sql"),
            (
                vec![
                    "read".to_string(),
                    r"C:\Users\O'Brien\script.sql".to_string(),
                ],
                None,
            )
        );
    }

    #[test]
    fn preserves_empty_quoted_arguments() {
        assert_eq!(
            tokenize_dot_command(r#"parameter set :value """#),
            (
                vec![
                    "parameter".to_string(),
                    "set".to_string(),
                    ":value".to_string(),
                    String::new(),
                ],
                None,
            )
        );
    }

    #[test]
    fn preserves_backslash_before_whitespace() {
        assert_eq!(
            tokenize_dot_command(r"open C:\data\ custom_vfs"),
            (
                vec![
                    "open".to_string(),
                    r"C:\data\".to_string(),
                    "custom_vfs".to_string(),
                ],
                None,
            )
        );
    }

    #[test]
    fn preserves_double_quotes_inside_single_quoted_arguments() {
        assert_eq!(
            tokenize_dot_command(r#"parameter set :value 'say "hi"'"#),
            (
                vec![
                    "parameter".to_string(),
                    "set".to_string(),
                    ":value".to_string(),
                    r#"say "hi""#.to_string(),
                ],
                None,
            )
        );
    }

    #[test]
    fn reports_unterminated_quotes_for_execution() {
        assert_eq!(
            tokenize_dot_command(r#"read "C:\Users\Jane Doe\script.sql"#),
            (
                vec![
                    "read".to_string(),
                    r"C:\Users\Jane Doe\script.sql".to_string()
                ],
                Some('"'),
            )
        );
    }

    #[test]
    fn retains_unterminated_argument_for_completion() {
        assert_eq!(
            tokenize_dot_command(r#"read "C:\Users\Jane D"#),
            (
                vec!["read".to_string(), r"C:\Users\Jane D".to_string()],
                Some('"'),
            )
        );
    }
}
