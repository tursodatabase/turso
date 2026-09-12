use super::dot_completion_args;

fn args(line: &str) -> Vec<String> {
    dot_completion_args(line)
        .into_iter()
        .map(|arg| arg.into_string().expect("test arguments must be UTF-8"))
        .collect()
}

#[test]
fn completion_keeps_trailing_whitespace_in_open_quote() {
    assert_eq!(
        args(r#"read "C:\Users\Jane Doe "#),
        vec!["", "read", r"C:\Users\Jane Doe "]
    );
}

#[test]
fn completion_starts_new_argument_after_closed_quote() {
    assert_eq!(
        args(r#"open "C:\Users\Jane Doe\test.db" "#),
        vec!["", "open", r"C:\Users\Jane Doe\test.db", ""]
    );
}
