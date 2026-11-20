/// `--swarm` picks the statements, the properties, the faults and the
/// connection count from the seed. Two runs with the same seed and flags must
/// print the same output, so that a seed that fails in CI fails again locally.
#[test]
fn swarm_runs_repeat_with_the_seed() {
    // The output contains the path of the run, so all runs use one directory.
    let directory = tempfile::tempdir().expect("create a directory for the runs");
    let run = |seed: u64| -> String {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_limbo_sim"))
            .args([
                "--swarm",
                "--disable-bugbase",
                "--io-backend",
                "memory",
                "--minimum-tests",
                "50",
                "--maximum-tests",
                "100",
                "--seed",
            ])
            .arg(seed.to_string())
            .env_remove("RUST_LOG")
            .current_dir(directory.path())
            .output()
            .expect("start limbo_sim");
        let stdout = String::from_utf8(output.stdout).expect("the output is UTF-8");
        assert!(
            output.status.success(),
            "seed {seed} failed:\n{stdout}{}",
            String::from_utf8_lossy(&output.stderr)
        );
        stdout
    };

    fn swarm_lines(output: &str) -> Vec<&str> {
        output
            .lines()
            .filter(|line| line.starts_with("swarm:"))
            .collect()
    }

    /// The first line that is different in the two outputs. A full output has
    /// thousands of lines, so a failed test shows only this line.
    fn first_difference(first: &str, second: &str) -> Option<String> {
        let (mut first, mut second) = (first.lines(), second.lines());
        (1..).find_map(|number| match (first.next(), second.next()) {
            (None, None) => Some(None),
            (a, b) if a != b => Some(Some(format!("line {number}: {a:?} != {b:?}"))),
            _ => None,
        })?
    }

    let first = run(11);
    assert_eq!(swarm_lines(&first).len(), 4, "{first}");
    assert_eq!(first_difference(&first, &run(11)), None);
    assert_ne!(swarm_lines(&first), swarm_lines(&run(12)));
}
