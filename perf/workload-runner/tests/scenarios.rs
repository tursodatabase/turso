use serde_json::Value;
use std::{fs::File, process::Command};

#[test]
fn real_backends_verify_inserts_and_overlapping_snapshots() {
    for scenario in ["insert", "held-snapshot"] {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("result.json");
        let result = Command::new(env!("CARGO_BIN_EXE_turso-workload-runner"))
            .args([
                "--workload",
                scenario,
                "--count",
                "12",
                "--rate",
                "100",
                "--connections",
                "2",
                "--batch-size",
                "3",
                "--checkpoint-ms",
                "5",
                "--repetitions",
                "2",
                "--raw-samples",
                "--output",
            ])
            .arg(&output)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let results: Value = serde_json::from_reader(File::open(output).unwrap()).unwrap();
        assert_eq!(results["schema_version"], 1);
        let runs = results["runs"].as_array().unwrap();
        assert_eq!(runs.len(), 6);
        assert_eq!(
            runs.iter()
                .map(|r| r["settings"]["engine"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["Sqlite", "Turso", "Turso", "Turso", "Turso", "Sqlite"]
        );
        for (index, run) in runs.iter().enumerate() {
            assert_eq!(run["verified"], true);
            assert_eq!(run["report"]["complete"], true);
            assert_eq!(run["config"]["seed"], 42 + index as u64 / 3);
            assert_eq!(run["actual_settings"]["synchronous"], "[[Integer(2)]]");
            assert!(!run["git_revision"].as_str().unwrap().is_empty());
            let stage = run["report"]["stages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["name"] == "load")
                .unwrap();
            assert_eq!(stage["operations"]["insert"]["successes"], 12);
            assert_eq!(stage["operations"]["insert"]["failures"], 0);
            if scenario == "held-snapshot" {
                assert_eq!(stage["operations"]["held_snapshot"]["successes"], 1);
                assert!(
                    stage["operations"]["checkpoint"]["successes"]
                        .as_u64()
                        .unwrap()
                        > 0
                );
            }
        }
    }
}

#[test]
fn parent_terminates_overlong_child_and_reports_incomplete() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("result.json");
    let result = Command::new(env!("CARGO_BIN_EXE_turso-workload-runner"))
        .args([
            "--targets",
            "sqlite-wal",
            "--workload",
            "noop",
            "--duration",
            "20",
            "--hard-timeout",
            "1",
            "--output",
        ])
        .arg(&output)
        .output()
        .unwrap();
    assert!(!result.status.success());
    let results: Value = serde_json::from_reader(File::open(output).unwrap()).unwrap();
    assert_eq!(results["runs"][0]["verified"], false);
    assert!(results["runs"][0]["error"]
        .as_str()
        .unwrap()
        .contains("hard timeout"));
}
