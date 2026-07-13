#![cfg(all(not(miri), not(shuttle)))]

use std::process::Command;

fn run_multiprocess(nr_iterations: &str, extra_args: &[&str]) {
    let exe = env!("CARGO_BIN_EXE_turso_stress");
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("stress.db");
    let output = Command::new(exe)
        .args([
            "--nr-processes",
            "2",
            "--nr-threads",
            "2",
            "--nr-iterations",
            nr_iterations,
            "--tables",
            "2",
            "--seed",
            "42",
            "--db-file",
            db_path.to_str().unwrap(),
        ])
        .args(extra_args)
        .output()
        .expect("failed to run turso_stress");
    assert!(
        output.status.success(),
        "turso_stress multiprocess run failed: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn multiprocess_stress_wal() {
    run_multiprocess("150", &[]);
}

#[test]
fn multiprocess_stress_kill_workers_wal() {
    run_multiprocess("1000", &["--kill-workers"]);
}

/// MVCC does not support multiprocess access: the engine rejects the
/// combination, and the harness rejects it up front with a clear error.
#[test]
fn multiprocess_stress_rejects_mvcc() {
    let exe = env!("CARGO_BIN_EXE_turso_stress");
    let output = Command::new(exe)
        .args(["--nr-processes", "2", "--tx-mode", "concurrent"])
        .output()
        .expect("failed to run turso_stress");
    assert!(
        !output.status.success(),
        "multiprocess MVCC must be rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("multiprocess"),
        "expected multiprocess rejection message, got: {stderr}"
    );
}
