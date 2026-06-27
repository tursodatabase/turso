use std::io::Write;
use std::process::{Command, Output, Stdio};

fn run_cli(input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().expect("failed to take stdin");
    stdin.write_all(input).expect("failed to write stdin");
    drop(stdin);

    child.wait_with_output().expect("failed to wait for output")
}

fn stdout_lines(output: &Output) -> Vec<&str> {
    let s = std::str::from_utf8(&output.stdout).expect("non-utf8 stdout");
    s.lines().collect()
}

fn stderr_text(output: &Output) -> &str {
    std::str::from_utf8(&output.stderr).expect("non-utf8 stderr")
}

#[test]
fn parameter_set_binds_named_slot() {
    let output = run_cli(b".mode list\n.parameter set :x 41\nselect :x;\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["41"]);
}

#[test]
fn parameter_set_binds_positional_slot() {
    let output = run_cli(b".mode list\n.parameter set ?1 9\nselect ?1;\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["9"]);
}

#[test]
fn parameter_clear_removes_binding() {
    let output = run_cli(b".mode list\n.parameter set :x 41\n.parameter clear :x\nselect :x;\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec![""]);
}

#[test]
fn parameter_set_rejects_bare_name() {
    let output = run_cli(b".mode list\n.parameter set x 41\nselect :x;\n");

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr_text(&output).contains("Error: parameter name must start with one of"),
        "expected bare-name validation error, got: {:?}",
        stderr_text(&output)
    );
}

#[test]
fn parameter_set_supports_quoted_multi_word_text() {
    let output = run_cli(b".mode list\n.parameter set :msg \"hello world\"\nselect :msg;\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["hello world"]);
}

#[test]
fn parameter_set_preserves_escaped_newline_text() {
    let output = run_cli(b".mode list\n.parameter set @x \"\\xA\"\nselect hex(@x), length(@x);\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["0A|1"]);
}

#[test]
fn parameter_set_preserves_fallback_text_spaces() {
    let output =
        run_cli(b".mode list\n.parameter set @x \"  a  \"\nselect quote(@x), length(@x);\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["'  a  '|5"]);
}

#[test]
fn parameter_set_truncates_nul_escape_like_sqlite_shell() {
    let output = run_cli(b".mode list\n.parameter set @x \"\\xZZ\"\nselect hex(@x), quote(@x);\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["|''"]);
}

#[test]
fn parameter_set_rejects_invalid_utf8_escape_current_turso_policy() {
    let output = run_cli(b".mode list\n.parameter set @x \"\\xFF\"\nselect hex(@x);\n");

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout_lines(&output), vec![""]);
    assert!(
        stderr_text(&output).contains("dot-command escape produced invalid UTF-8"),
        "expected invalid UTF-8 escape error, got: {:?}",
        stderr_text(&output)
    );
}

#[test]
fn parameter_clear_only_removes_requested_name() {
    let output = run_cli(
        b".mode list\n.parameter set :x 1\n.parameter set @x 2\n.parameter clear :x\nselect :x, @x;\n",
    );

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["|2"]);
}

#[test]
fn parameter_set_rejects_zero_positional_index() {
    let output = run_cli(b".mode list\n.parameter set ?0 41\n");

    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr_text(&output).contains("?N' must use an index >= 1"),
        "expected positional index bounds validation error, got: {:?}",
        stderr_text(&output)
    );
}

#[test]
fn parameter_set_mixed_named_and_positional() {
    let output = run_cli(
        b".mode list\n.parameter set :name alice\n.parameter set ?2 30\nselect :name, ?2;\n",
    );

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["alice|30"]);
}

#[test]
fn parameter_set_anonymous_positional() {
    let output =
        run_cli(b".mode list\n.parameter set ?1 first\n.parameter set ?2 second\nselect ?, ?;\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["first|second"]);
}

#[test]
fn parameter_set_mixed_named_and_anonymous_positional() {
    let output = run_cli(
        b".mode list\n.parameter set :name alice\n.parameter set ?2 30\nselect :name, ?;\n",
    );

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["alice|30"]);
}

#[test]
fn parameter_set_parses_hex_blob_literal() {
    let output = run_cli(b".mode list\n.parameter set :blob \"x'4142'\"\nselect :blob;\n");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_lines(&output), vec!["AB"]);
}
