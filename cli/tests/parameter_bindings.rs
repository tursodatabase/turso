use std::io::Write;
use std::process::{Command, Output, Stdio};

fn run_cli(input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().expect("failed to take stdin");
    stdin.write_all(input).expect("failed to write stdin");
    drop(stdin);

    child.wait_with_output().expect("failed to wait for output")
}

#[test]
fn parameter_set_binds_named_slot() {
    let output = run_cli(b".mode list\n.parameter set :x 41\nselect :x;\n");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("41"),
        "expected bound named value in output"
    );
}

#[test]
fn parameter_set_binds_positional_slot() {
    let output = run_cli(b".mode list\n.parameter set ?1 9\nselect ?1;\n");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("9"),
        "expected bound positional value in output"
    );
}

#[test]
fn parameter_clear_removes_binding() {
    let output = run_cli(b".mode list\n.parameter set :x 41\n.parameter clear :x\nselect :x;\n");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.lines().any(|line| line.trim() == "41"),
        "cleared value should not appear in output"
    );
}

#[test]
fn parameter_set_rejects_bare_name() {
    let output = run_cli(b".mode list\n.parameter set x 41\nselect :x;\n");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Error: parameter name must start with one of"),
        "expected bare-name validation error"
    );
}

#[test]
fn parameter_set_supports_quoted_multi_word_text() {
    let output = run_cli(b".mode list\n.parameter set :msg \"hello world\"\nselect :msg;\n");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("hello world"),
        "expected quoted multi-word text value"
    );
}

#[test]
fn parameter_clear_only_removes_requested_name() {
    let output = run_cli(
        b".mode list\n.parameter set :x 1\n.parameter set @x 2\n.parameter clear :x\nselect :x, @x;\n",
    );

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("1|2"),
        "cleared :x should not remain bound"
    );
    assert!(stdout.contains("|2"), "@x should remain bound");
}

#[test]
fn parameter_set_rejects_zero_positional_index() {
    let output = run_cli(b".mode list\n.parameter set ?0 41\n");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("?N' must use an index >= 1"),
        "expected positional index bounds validation error"
    );
}

#[test]
fn parameter_set_parses_hex_blob_literal() {
    let output = run_cli(b".mode list\n.parameter set :blob \"x'4142'\"\nselect :blob;\n");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("AB"), "expected decoded blob content");
}
