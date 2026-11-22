"""Real HTTP plaintext and encrypted sync tests, using the Rust SDK test driver.

Build first:
  cargo build -p turso_cli -p turso --bin tursodb --example sync_encryption_e2e --features turso/sync
Run:
  python3 testing/cli_tests/sync_encryption.py
"""

import base64
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CLI = Path(os.environ.get("TURSO_CLI", ROOT / "target/debug/tursodb"))
DRIVER = Path(os.environ.get("SYNC_E2E_DRIVER", ROOT / "target/debug/examples/sync_encryption_e2e"))


class EncryptedSync(unittest.TestCase):
    def scenario(self, cipher, directory, mvcc):
        with tempfile.TemporaryDirectory(prefix="sample-sync-encryption-") as tmp:
            tmp = Path(tmp)
            key = tmp / "key.hex"
            key.write_text("0b" * 32)
            key.chmod(0o600)
            dbs = tmp / "dbs"
            dbs.mkdir()
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            address = f"127.0.0.1:{port}"
            url = f"http://{address}" + ("/db/example" if directory else "")
            args = [str(CLI), "--sync-server", address]
            if cipher:
                args += ["--sync-encryption-cipher", cipher,
                         "--sync-encryption-key-file", str(key)]
            args += ["--sync-dir", str(dbs)] if directory else [str(tmp / "source.db")]
            headers = {"x-turso-encryption-key": base64.b64encode(bytes([11]) * 32).decode(),
                       "x-turso-encryption-cipher": cipher} if cipher else {}
            key_only_headers = {name: value for name, value in headers.items()
                                if name == "x-turso-encryption-key"}
            env = {**os.environ, "SYNC_E2E_CIPHER": cipher or "none"}
            replica = tmp / "replica.db"
            source = dbs / "example/data" if directory else tmp / "source.db"
            self.prepare_legacy_source(source, mvcc and not cipher)

            def run(action, sql=None, path=replica, environment=env, remote=url):
                result = subprocess.run([str(DRIVER), str(path), remote, action] + ([sql] if sql else []),
                                        env=environment, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                return json.loads(result.stdout)

            def pipeline(sql, request_headers=headers):
                body = json.dumps({"requests": [{"type": "execute", "stmt": {"sql": sql}}]}).encode()
                request = urllib.request.Request(url + "/v2/pipeline", body, request_headers)
                with urllib.request.urlopen(request, timeout=10) as response:
                    result = json.load(response)
                self.assertEqual(result["results"][0]["type"], "ok", result)
                return result

            log = (tmp / "server.log").open("w+")
            process = None

            def start():
                nonlocal process
                process = subprocess.Popen(args, stdout=log, stderr=log)
                self.wait_for_server(process, log, port)

            try:
                start()
                # Reject missing/wrong credentials before opening or mutating a database.
                for bad in ([{}, {**headers, "x-turso-encryption-key": base64.b64encode(bytes(32)).decode()},
                            {"x-turso-encryption-key": base64.b64encode(bytes(32)).decode()},
                            {**headers, "x-turso-encryption-cipher": "aes128gcm"}] if cipher else []):
                    with self.assertRaises(urllib.error.HTTPError) as error:
                        pipeline("CREATE TABLE forbidden(x)", bad)
                    self.assertEqual(error.exception.code, 400)
                    error.exception.close()
                if mvcc:
                    pipeline("PRAGMA journal_mode=mvcc")
                run("bootstrap")
                if mvcc and not cipher:
                    self.assertEqual(run("scalar", "SELECT count(*) FROM legacy")["value"], 1)
                run("exec", "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
                run("exec", "INSERT INTO items VALUES (1, 'client secret')")
                run("push")
                # JavaScript RemoteWriter sends the cloud key without a cipher.
                pipeline("INSERT INTO items VALUES (2, '" + "server secret " * 8000 + "')",
                         key_only_headers)
                run("pull")
                self.assertEqual(run("scalar", "SELECT count(*) FROM items")["value"], 2)
                run("checkpoint")
                self.assertEqual(run("scalar", "SELECT count(*) FROM items", remote="-")["value"], 2)
                stop_server(process)
                start()
                pipeline("INSERT INTO items VALUES (3, 'after restart')")
                run("pull")
                self.assertEqual(run("scalar", "SELECT count(*) FROM items")["value"], 3)
                if not mvcc:
                    self.check_partial_checkpoint_reads(run, pipeline, cipher)
                    pipeline("PRAGMA wal_checkpoint(TRUNCATE)")
                # Fresh replicas need base pages even when the WAL is empty.
                fresh = tmp / "fresh.db"
                run("bootstrap", path=fresh)
                self.assertEqual(run("scalar", "SELECT count(*) FROM items", path=fresh)["value"], 3)
                plain_env = {**env, "SYNC_E2E_LOCAL_KEY": "none"}
                plain = tmp / "plain.db"
                run("bootstrap", path=plain, environment=plain_env)
                self.assertEqual(
                    run("scalar", "SELECT count(*) FROM items", path=plain, environment=plain_env)["value"], 3
                )
                self.assert_encrypted_files(cipher, [
                    source, Path(str(source) + "-wal"), source.with_suffix(".db-log"), replica,
                    Path(str(replica) + "-info"),
                ])
                if mvcc:
                    before = run("scalar", "SELECT count(*) FROM items", remote="-")
                    source_log = source.with_suffix(".db-log")
                    original = source_log.read_bytes()
                    corrupt = bytearray(original)
                    self.assertEqual(corrupt[4], 3)
                    corrupt[4] = 2  # Leave the header CRC invalid.
                    source_log.write_bytes(corrupt)
                    try:
                        result = subprocess.run(
                            [str(DRIVER), str(replica), url, "pull"],
                            env=env, capture_output=True, text=True, timeout=30,
                        )
                        self.assertNotEqual(result.returncode, 0, result.stdout)
                        self.assertIn("header checksum mismatch", result.stderr)
                        after = run("scalar", "SELECT count(*) FROM items", remote="-")
                        self.assertEqual(after["revision"], before["revision"])
                        self.assertEqual(after["value"], before["value"])
                    finally:
                        source_log.write_bytes(original)
            finally:
                stop_server(process)
                log.close()

    def check_partial_checkpoint_reads(self, run, pipeline, cipher):
        if cipher:
            return
        pipeline("CREATE TABLE lazy_items(id INTEGER PRIMARY KEY, value TEXT)")
        pipeline("INSERT INTO lazy_items SELECT value, 'old'||printf('%0500d', value) "
                 "FROM generate_series(1, 200)")
        for mode in ["PASSIVE", "FULL"]:
            pipeline("UPDATE lazy_items SET value = 'old'||substr(value, 4)")
            self.assertEqual(run("partial-checkpoint", mode, path=Path(":memory:"))["value"], 200)

    def prepare_legacy_source(self, source, enabled):
        if not enabled:
            return
        source.parent.mkdir(parents=True, exist_ok=True)
        prepared = subprocess.run(
            [str(CLI), str(source), "PRAGMA journal_mode=mvcc; CREATE TABLE legacy(value); "
             "INSERT INTO legacy VALUES ('existing row'); PRAGMA wal_checkpoint(TRUNCATE);"],
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(prepared.returncode, 0, prepared.stdout + prepared.stderr)

    def assert_encrypted_files(self, cipher, paths):
        if not cipher:
            return
        for path in paths:
            if path.exists():
                data = path.read_bytes()
                self.assertNotIn(b"client secret", data, str(path))
                self.assertNotIn(b"server secret", data, str(path))

    def wait_for_server(self, process, log, port):
        for _ in range(100):
            if process.poll() is not None:
                log.seek(0)
                self.fail(log.read())
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.1):
                    return
            except OSError:
                time.sleep(.05)
        self.fail("sample server did not start")

    def test_mvcc(self):
        for cipher in [None, "aes256gcm", "aegis256"]:
            for directory in [False, True]:
                with self.subTest(cipher=cipher, directory=directory):
                    self.scenario(cipher, directory, mvcc=True)

    def test_wal(self):
        for cipher in [None, "aes256gcm"]:
            for directory in [False, True]:
                with self.subTest(cipher=cipher, directory=directory):
                    self.scenario(cipher, directory, mvcc=False)


def stop_server(process):
    if process and process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


if __name__ == "__main__":
    unittest.main(verbosity=2)
