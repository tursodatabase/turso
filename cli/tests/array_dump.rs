use std::io::Write;
use std::process::{Command, Output, Stdio};

#[test]
fn dump_text_arrays_preserves_values() {
    assert_array_dump_roundtrip(
        "TEXT[]",
        r#"ARRAY['001', '+12', '1e2', '1.0', 'NULL', '',
                  'X''00FF''', 'x''41''', 'hello', '你好 🌍', 'a,b', '{nested}',
                  'say "hello"', 'back\slash', char(0, 1, 8, 9, 10, 12, 13, 31, 127, 133)]"#,
    );
}

#[test]
fn dump_numeric_and_blob_looking_text_arrays_preserves_values() {
    assert_array_dump_roundtrip(
        "TEXT[]",
        "ARRAY['NaN', 'Infinity', '1e309', 'inf', '-inf', 'X''GG''', 'X''']",
    );
}

#[test]
fn dump_blob_arrays_preserves_values() {
    assert_array_dump_roundtrip("BLOB[]", "ARRAY[X'', X'00FF8041', NULL]");
}

#[test]
fn dump_any_arrays_preserves_types() {
    assert_array_dump_roundtrip(
        "ANY[]",
        "ARRAY['001', 1, 1.0, '1.0', X'00FF', 'X''00FF''', char(0, 8, 127), NULL]",
    );
}

#[test]
fn dump_integer_arrays_preserves_values() {
    assert_array_dump_roundtrip(
        "INTEGER[]",
        "ARRAY[-9223372036854775808, -1, 0, 1, 9223372036854775807, NULL]",
    );
}

#[test]
fn dump_real_arrays_preserves_values() {
    assert_array_dump_roundtrip(
        "REAL[]",
        "ARRAY[0.0, 1.0, -2.5, 1e100, 1e-100, 1.7976931348623157e308, 5e-324, NULL]",
    );
}

#[test]
fn dump_real_array_infinities_and_signed_zero_preserves_bits() {
    assert_array_dump_roundtrip("REAL[]", "ARRAY[1e309, -1e309, -0.0, 0.0]");
}

#[test]
fn dump_nested_arrays_preserves_values() {
    assert_array_dump_roundtrip(
        "ANY[][]",
        "ARRAY[ARRAY['001', X'00FF', char(0)], ARRAY[], NULL]",
    );
}

fn assert_array_dump_roundtrip(column_type: &str, array: &str) {
    let setup = format!(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a {column_type}) STRICT;\n\
         INSERT INTO t VALUES (1, {array}), (2, ARRAY[]), (3, NULL), (4, ARRAY[NULL]);\n"
    );
    let query = "SELECT id, typeof(a), hex(a) FROM t ORDER BY id;\n";
    let original = run_cli(&format!("{setup}{query}"));
    let dump = run_cli(&format!("{setup}.dump\n"));
    let dump = String::from_utf8(dump.stdout).expect("dump must be UTF-8");
    let restored = run_cli(&format!("{dump}{query}"));
    assert_eq!(
        String::from_utf8_lossy(&restored.stdout),
        String::from_utf8_lossy(&original.stdout),
        "array values or storage types changed after replaying:\n{dump}"
    );
}

fn run_cli(input: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .args([
            "--experimental-custom-types",
            "-q",
            "-m",
            "list",
            ":memory:",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run tursodb");
    child
        .stdin
        .take()
        .expect("piped stdin must be available")
        .write_all(input.as_bytes())
        .expect("failed to write CLI input");
    let output = child
        .wait_with_output()
        .expect("failed to wait for tursodb");
    assert!(
        output.status.success(),
        "CLI failed for {input}:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
