#![cfg(unix)]
use hegel::{generators as gs, TestCase};
use std::{fs, os::unix::fs::PermissionsExt};
use turso_pg_dump_fuzzer::runner::Runner;

#[hegel::test(test_cases = 20)]
fn successful_cases_leave_no_artifacts(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let (root, runner) = fixture("ok", n);
    runner.check(&format!("SELECT {n};")).unwrap();
    assert_eq!(fs::read_dir(&runner.output).unwrap().count(), 0);
    assert!(fs::read_to_string(root.path().join("calls"))
        .unwrap()
        .contains("DROP DATABASE"));
}

#[hegel::test(test_cases = 50)]
fn failures_preserve_sql_logs_and_cleanup(tc: TestCase) {
    let stage = tc.draw(gs::sampled_from(&[
        "create",
        "startup",
        "bootstrap",
        "dump",
        "parse",
        "mismatch",
        "cleanup",
        "launch",
    ]));
    let n = tc.draw(gs::integers::<i32>());
    let (root, runner) = fixture(stage, n);
    let sql = format!("SELECT {n};");
    assert!(runner.check(&sql).is_err());
    let cases: Vec<_> = fs::read_dir(&runner.output)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|path| path.is_dir())
        .collect();
    assert_eq!(cases.len(), 1);
    assert_eq!(
        fs::read_to_string(cases[0].join("profile.txt")).unwrap(),
        "compatible"
    );
    assert_eq!(fs::read_to_string(cases[0].join("input.sql")).unwrap(), sql);
    assert!(!fs::read_to_string(cases[0].join("failure.txt"))
        .unwrap()
        .is_empty());
    assert!(fs::read_to_string(root.path().join("calls"))
        .unwrap()
        .contains("DROP DATABASE"));
}

#[test]
fn fail_fast_executes_only_one_failing_workload() {
    for stage in ["mismatch", "bootstrap"] {
        let (root, runner) = fixture(stage, 7);
        let result = std::process::Command::new(env!("CARGO_BIN_EXE_pg-dump-fuzzer"))
            .args(["--profile", "tables", "--postgres-dsn", &runner.reference])
            .arg("--binary")
            .arg(&runner.binary)
            .arg("--psql")
            .arg(&runner.psql)
            .arg("--pg-dump")
            .arg(&runner.pg_dump)
            .arg("--output")
            .arg(&runner.output)
            .args(["fuzz", "--cases", "10", "--seed", "0", "--fail-fast"])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&result.stdout);
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            stdout.contains("Loads and data"),
            "status: {}; stdout: {stdout}; stderr: {stderr}",
            result.status
        );
        assert!(stdout.contains("Generated input"), "{stdout}");
        assert!(
            stdout.contains("SQL statements and constraints"),
            "{stdout}"
        );
        assert!(
            !stdout.contains('\x1b'),
            "redirected report contains ANSI escapes"
        );
        assert_eq!(result.status.code(), Some(101), "{stderr}");
        assert!(
            stderr.contains("1 attempts, 0 passed, 1 failed"),
            "{stderr}"
        );
        let cases: Vec<_> = fs::read_dir(&runner.output)
            .unwrap()
            .map(|p| p.unwrap().path())
            .filter(|path| path.is_dir())
            .collect();
        assert_eq!(cases.len(), 1);
        let report = fs::read_dir(&runner.output)
            .unwrap()
            .map(|p| p.unwrap().path())
            .find(|path| path.extension().is_some_and(|ext| ext == "json"))
            .unwrap();
        let report: serde_json::Value = serde_json::from_slice(&fs::read(report).unwrap()).unwrap();
        assert_eq!(report["summary"]["attempts"], 1);
        assert_eq!(report["summary"]["failed"], 1);
        assert_eq!(report["summary"]["generated"]["workloads"], 1);
        assert_eq!(
            report["summary"]["engines"]["postgres"]["completed"]["workloads"],
            if stage == "mismatch" { 1 } else { 0 }
        );
        assert!(cases[0].join("input.sql").is_file());
        assert!(cases[0].join("failure.txt").is_file());
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert_eq!(calls.matches("CREATE DATABASE").count(), 1);
        assert_eq!(calls.matches("DROP DATABASE").count(), 1);
    }
}

#[test]
fn shrinking_failures_save_the_report_and_cleanup() {
    let (root, runner) = fixture("create", 7);
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_pg-dump-fuzzer"))
        .args(["--profile", "tables", "--postgres-dsn", &runner.reference])
        .arg("--binary")
        .arg(&runner.binary)
        .arg("--psql")
        .arg(&runner.psql)
        .arg("--pg-dump")
        .arg(&runner.pg_dump)
        .arg("--output")
        .arg(&runner.output)
        .args(["fuzz", "--cases", "1", "--seed", "0"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert_eq!(result.status.code(), Some(101), "{stderr}");
    let report = fs::read_dir(&runner.output)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "json"))
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&fs::read(report).unwrap()).unwrap();
    let attempts = report["summary"]["attempts"].as_u64().unwrap();
    assert!(attempts > 1, "shrinking/replay should recheck the failure");
    assert_eq!(report["summary"]["failed"], attempts);
    let calls = fs::read_to_string(root.path().join("calls")).unwrap();
    assert_eq!(calls.matches("CREATE DATABASE").count() as u64, attempts);
    assert_eq!(calls.matches("DROP DATABASE").count() as u64, attempts);
}

#[hegel::test(test_cases = 20)]
fn replay_preserves_disabled_feature_metadata_without_filtering_sql(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let metadata_present = tc.draw(gs::booleans());
    let (root, runner) = fixture("mismatch", n);
    let sql = format!("CREATE TABLE t (x INTEGER DEFAULT {n}); GRANT SELECT ON t TO PUBLIC;\n");
    let input = root.path().join("input.sql");
    fs::write(&input, &sql).unwrap();
    fs::write(root.path().join("profile.txt"), "compatible").unwrap();
    let disabled = if metadata_present {
        vec![
            "defaults".to_owned(),
            "grants".to_owned(),
            "grants".to_owned(),
            format!("future-{n}"),
        ]
    } else {
        Vec::new()
    };
    if metadata_present {
        fs::write(
            root.path().join("disabled-features.json"),
            serde_json::to_vec(&disabled).unwrap(),
        )
        .unwrap();
    }
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_pg-dump-fuzzer"))
        .args([
            "--postgres-dsn",
            "postgresql://postgres@localhost/postgres",
            "--no-progress",
        ])
        .arg("--binary")
        .arg(&runner.binary)
        .arg("--psql")
        .arg(&runner.psql)
        .arg("--pg-dump")
        .arg(&runner.pg_dump)
        .arg("--output")
        .arg(&runner.output)
        .arg("replay")
        .arg(input)
        .output()
        .unwrap();
    assert!(!result.status.success());
    let case = fs::read_dir(&runner.output)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert_eq!(fs::read_to_string(case.join("input.sql")).unwrap(), sql);
    assert_eq!(
        serde_json::from_slice::<Vec<String>>(
            &fs::read(case.join("disabled-features.json")).unwrap()
        )
        .unwrap(),
        disabled
    );
    assert!(fs::read_to_string(case.join("failure.txt"))
        .unwrap()
        .contains("dump ASTs differ"));
    let flags = fs::read_to_string(root.path().join("dump-calls")).unwrap();
    assert!(!flags.contains("--no-privileges"));
}

fn fixture(stage: &str, n: i32) -> (tempfile::TempDir, Runner) {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("stage"), stage).unwrap();
    fs::write(root.path().join("reference"), format!("SELECT {n};")).unwrap();
    let actual = match stage {
        "parse" => "SELECT (".to_owned(),
        "mismatch" => format!("SELECT {};", n.wrapping_add(1)),
        _ => format!("-- different header\nSELECT {n};"),
    };
    fs::write(root.path().join("actual"), actual).unwrap();
    let common = "#!/bin/sh\nroot=$(dirname \"$0\")\nstage=$(cat \"$root/stage\")\n";
    let common = format!("{common}if [ \"$(basename \"$0\")\" = pg_dump ]; then printf '%s\\n' \"$@\" >> \"$root/dump-calls\"; fi\n");
    let scripts = [
        ("server", "[ \"$stage\" = startup ] && exit 1\necho 'PostgreSQL server listening on 127.0.0.1:54321 (database: test.db)'\nexec sleep 60\n"),
        ("psql", "printf '%s\\n' \"$@\" >> \"$root/calls\"\ncase \"$*\" in\n*'CREATE DATABASE'*) [ \"$stage\" = create ] && exit 1;;\n*'DROP DATABASE'*) [ \"$stage\" = cleanup ] && exit 1;;\n*--file*) [ \"$stage\" = bootstrap ] && exit 1;;\nesac\nexit 0\n"),
        ("pg_dump", "[ \"$stage\" = dump ] && exit 1\nsource=reference\nwhile [ $# -gt 0 ]; do\ncase \"$1\" in\n--dbname) shift; case \"$1\" in *:54321/*) source=actual;; esac;;\n--file) shift; output=$1;;\nesac\nshift\ndone\ncp \"$root/$source\" \"$output\"\n"),
    ];
    for (name, script) in scripts {
        let path = root.path().join(name);
        fs::write(&path, format!("{common}{script}")).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let runner = Runner {
        reference: "postgresql://test@localhost/postgres?dbname=postgres".to_owned(),
        binary: root.path().join(if stage == "launch" {
            "missing"
        } else {
            "server"
        }),
        psql: root.path().join("psql"),
        pg_dump: root.path().join("pg_dump"),
        output: root.path().join("results"),
        privileges: true,
        disabled_features: Vec::new(),
    };
    (root, runner)
}
