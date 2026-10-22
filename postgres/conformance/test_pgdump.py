import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class PgDumpTest(unittest.TestCase):
    def test_dump_files_must_match_byte_for_byte(self):
        cases = [
            (b"COPY items FROM stdin;\n7\n\\.\n", b"COPY items FROM stdin;\n7\n\\.\n", False),
            (b"COPY items FROM stdin;\n7\n\\.\n", b"COPY items FROM stdin;\n19\n\\.\n", False),
            (b"SELECT 7;\n", b"SELECT 7;\r\n", False),
            (b"ALTER TABLE items ADD PRIMARY KEY (id);\n", b"", False),
            (b"-- PostgreSQL 17\n", b"-- PostgreSQL 16\n", False),
            (b"SELECT 7;\n", b"SELECT 7;\n", True),
        ]
        for reference, actual, dump_fails in cases:
            with (
                self.subTest(reference=reference, actual=actual, dump_fails=dump_fails),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                (root / "reference.dump").write_bytes(reference)
                (root / "actual.dump").write_bytes(actual)
                if dump_fails:
                    (root / "fail-dump").touch()
                client = root / "client.py"
                client.write_text(
                    f"#!{sys.executable}\n"
                    "import pathlib, signal, sys\n"
                    "root = pathlib.Path(__file__).resolve().parent\n"
                    "name = pathlib.Path(sys.argv[0]).name\n"
                    "if '--server' in sys.argv:\n"
                    "    print('PostgreSQL server listening on 127.0.0.1:54321', flush=True)\n"
                    "    signal.pause()\n"
                    "elif name == 'pg_dump':\n"
                    "    source = sys.argv[sys.argv.index('--dbname') + 1]\n"
                    "    output = pathlib.Path(sys.argv[sys.argv.index('--file') + 1])\n"
                    "    turso = ':54321/' in source\n"
                    "    output.write_bytes((root / ('actual.dump' if turso else 'reference.dump')).read_bytes())\n"
                    "    if turso and (root / 'fail-dump').exists():\n"
                    "        print('dump failed', file=sys.stderr)\n"
                    "        sys.exit(1)\n"
                )
                client.chmod(0o755)
                for name in ("tursopg", "psql", "pg_dump"):
                    (root / name).symlink_to(client)
                fixture = root / "tables.sql"
                fixture.write_text("CREATE TABLE items (id INTEGER PRIMARY KEY);\n")
                output = root / "results"
                result = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).with_name("pgdump.py")),
                        "--binary",
                        str(root / "tursopg"),
                        "--psql",
                        str(root / "psql"),
                        "--pg-dump",
                        str(root / "pg_dump"),
                        "--output-dir",
                        str(output),
                        str(fixture),
                    ],
                    check=False,
                    env={**os.environ, "PG_DSN": "postgresql://sqltest@127.0.0.1:5432/postgres"},
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
                expected_success = reference == actual and not dump_fails
                self.assertEqual(result.returncode, 0 if expected_success else 1, result.stdout + result.stderr)
                files = list(output.glob("*/tables"))
                self.assertEqual(len(files), 1, result.stdout + result.stderr)
                self.assertEqual((files[0] / "postgres.sql").read_bytes(), reference)
                self.assertEqual((files[0] / "turso.sql").read_bytes(), actual)
                if dump_fails:
                    self.assertIn("dump failed", (files[0] / "turso-dump.log").read_text())
                    self.assertFalse((files[0] / "dump.diff").exists())
                elif not expected_success:
                    self.assertIn("dump files differ", result.stdout)
                    self.assertTrue((files[0] / "dump.diff").read_bytes())


if __name__ == "__main__":
    unittest.main()
