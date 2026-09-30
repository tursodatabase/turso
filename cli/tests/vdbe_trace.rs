use std::process::Command;

fn assert_trace(sql: &str, expected_stdout: &str, expected_stderr: &str) {
    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .args(["-m", "list", ":memory:", sql])
        .output()
        .expect("failed to run tursodb");
    assert!(
        output.status.success(),
        "tursodb exited with {:?}, stderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let got_out: Vec<&str> = stdout.lines().map(str::trim_end).collect();
    let want_out: Vec<&str> = expected_stdout.lines().map(str::trim_end).collect();
    assert_eq!(got_out, want_out, "\n--- full stdout ---\n{stdout}");
    let got_err: Vec<&str> = stderr.lines().map(str::trim_end).collect();
    let want_err: Vec<&str> = expected_stderr.lines().map(str::trim_end).collect();
    assert_eq!(got_err, want_err, "\n--- full stderr ---\n{stderr}");
}

#[test]
fn vdbe_trace_on_traces_literal_select() {
    assert_trace(
        "PRAGMA vdbe_trace = ON; SELECT 7;",
        "7",
        "\
VDBE Trace:
0     Init               0     2     0                    0   Start at 2
2     Goto               0     1     0                    0
1     Halt               0     0     0                    0
VDBE Trace:
0     Init               0     3     0                    0   Start at 3
3     Integer            7     1     0                    0   r[1]=7
R[1] = 7
4     Goto               0     1     0                    0
1     ResultRow          1     1     0                    0   output=r[1]
2     Halt               0     0     0                    0",
    );
}

#[test]
fn vdbe_trace_on_traces_computed_expression() {
    assert_trace(
        "PRAGMA vdbe_trace = ON; SELECT 3 + 4;",
        "7",
        "\
VDBE Trace:
0     Init               0     2     0                    0   Start at 2
2     Goto               0     1     0                    0
1     Halt               0     0     0                    0
VDBE Trace:
0     Init               0     3     0                    0   Start at 3
3     Integer            3     2     0                    0   r[2]=3
R[2] = 3
4     Integer            4     3     0                    0   r[3]=4
R[3] = 4
5     Add                2     3     1                    0   r[1]=r[2]+r[3]
R[1] = 7
6     Goto               0     1     0                    0
1     ResultRow          1     1     0                    0   output=r[1]
2     Halt               0     0     0                    0",
    );
}

#[test]
fn vdbe_trace_off_by_default_emits_only_result() {
    assert_trace("SELECT 7;", "7", "");
}
