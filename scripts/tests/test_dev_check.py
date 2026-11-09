import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "dev-check.sh"


class DevCheckTests(unittest.TestCase):
    def run_script(self, args, fail="", jobs="2", debug="line-tables-only"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cargo = root / "cargo"
            cargo.write_text(
                "#!/bin/bash\n"
                'printf "%s|%s|%s|%s\\n" "$*" "$CARGO_BUILD_JOBS" '
                '"$CARGO_PROFILE_DEV_DEBUG" "$CARGO_PROFILE_TEST_DEBUG" >> "$LOG"\n'
                'if [[ "$1" == "$FAIL" ]]; then exit 7; fi\n'
            )
            cargo.chmod(0o755)
            env = dict(
                os.environ,
                PATH=f"{root}:{os.environ['PATH']}",
                LOG=str(root / "log"),
                FAIL=fail,
                CARGO_BUILD_JOBS=jobs,
                CARGO_PROFILE_DEV_DEBUG=debug,
                CARGO_PROFILE_TEST_DEBUG=debug,
            )
            for key in list(env):
                if env[key] is None:
                    del env[key]
            result = subprocess.run(["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True)
            log = root / "log"
            return result, log.read_text().splitlines() if log.exists() else []

    def test_defaults_and_debug_overrides(self):
        result, lines = self.run_script(["turso_parser", "tests::test_offset"], jobs=None, debug=None)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(line.endswith("|2|line-tables-only|line-tables-only") for line in lines))
        result, lines = self.run_script(["turso_parser", "tests::test_offset"], debug="2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(line.endswith("|2|2|2") for line in lines))

    def test_sequence_and_arguments(self):
        result, lines = self.run_script(["turso_parser", "parser::", "serde"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            lines,
            [
                "fmt --all -- --check|2|line-tables-only|line-tables-only",
                "check --locked -p turso_parser --lib --features serde|2|line-tables-only|line-tables-only",
                "test --locked -p turso_parser --lib --features serde parser:: -- --test-threads=1"
                "|2|line-tables-only|line-tables-only",
                "clippy --locked -p turso_parser --lib --features serde -- --deny=warnings"
                "|2|line-tables-only|line-tables-only",
            ],
        )

    def test_stops_at_each_failure(self):
        for index, stage in enumerate(["fmt", "check", "test", "clippy"]):
            with self.subTest(stage=stage):
                result, lines = self.run_script(["turso_parser", "parser::"], fail=stage)
                self.assertEqual(result.returncode, 7)
                self.assertEqual(len(lines), index + 1)
                self.assertIn(stage, result.stderr)

    def test_rejects_invalid_input_before_cargo(self):
        for args, jobs in [
            ([], "2"),
            (["p", ""], "2"),
            (["--workspace", "x"], "2"),
            (["p", "--ignored"], "2"),
            (["p", "x"], "0"),
        ]:
            with self.subTest(args=args, jobs=jobs):
                result, lines = self.run_script(args, jobs=jobs)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(lines, [])

    def test_preserves_job_override(self):
        result, lines = self.run_script(["turso_parser", "parser::"], jobs="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all("|1|" in line for line in lines))
        self.assertTrue(all("--features" not in line for line in lines))


if __name__ == "__main__":
    unittest.main()
