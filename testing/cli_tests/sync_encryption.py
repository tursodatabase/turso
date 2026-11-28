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
            key_size = 16 if cipher in ["aes128gcm", "aegis128l", "aegis128x2", "aegis128x4"] else 32
            key.write_text("0b" * key_size)
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
            headers = {"x-turso-encryption-key": base64.b64encode(bytes([11]) * key_size).decode(),
                       "x-turso-encryption-cipher": cipher} if cipher else {}
            key_only_headers = {name: value for name, value in headers.items()
                                if name == "x-turso-encryption-key"}
            env = {**os.environ, "SYNC_E2E_CIPHER": cipher or "none",
                   "SYNC_E2E_LOCAL_KEY": "16" * key_size,
                   "SYNC_E2E_CLOUD_KEY": headers.get("x-turso-encryption-key", "none")}
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
                for bad in ([
                    {},
                    {**headers, "x-turso-encryption-key": base64.b64encode(bytes(32)).decode()},
                    {"x-turso-encryption-key": base64.b64encode(bytes(32)).decode()},
                    {**headers, "x-turso-encryption-cipher":
                        "aes256gcm" if cipher == "aes128gcm" else "aes128gcm"},
                ] if cipher else []):
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
                run("exec", "UPDATE items SET value='pending local update' WHERE id=1")
                run("pull")
                self.assertEqual(run("scalar", "SELECT count(*) FROM items")["value"], 2)
                self.assertEqual(run("scalar", "SELECT value='pending local update' FROM items WHERE id=1")["value"], 1)
                run("push")
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
        for cipher in [
            None, "aes256gcm", "aes128gcm", "aegis128l", "aegis128x2", "aegis128x4",
            "aegis256", "aegis256x2", "aegis256x4",
        ]:
            for directory in [False, True]:
                with self.subTest(cipher=cipher, directory=directory):
                    self.scenario(cipher, directory, mvcc=True)

    def test_wal(self):
        for cipher in [None, "aes256gcm"]:
            for directory in [False, True]:
                with self.subTest(cipher=cipher, directory=directory):
                    self.scenario(cipher, directory, mvcc=False)

    def test_mvcc_push_after_server_checkpoint_is_not_sent_again(self):
        for directory in [False, True]:
            for change in [
                "UPDATE t SET v = 'from server' WHERE id = 100",
                "DELETE FROM t WHERE id = 100",
            ]:
                with self.subTest(directory=directory, change=change):
                    self.push_after_server_checkpoint(directory, change)

    def push_after_server_checkpoint(self, directory, change):
        """A replica pushes a row after the server checkpointed its MVCC log,
        the server changes that row, and the replica pulls and pushes. The
        replica must not send its pushed row again over the change of the
        server."""
        with tempfile.TemporaryDirectory(prefix="sample-sync-checkpoint-") as tmp:
            tmp = Path(tmp)
            dbs = tmp / "dbs"
            source = dbs / "example/data" if directory else tmp / "source.db"
            source.parent.mkdir(parents=True, exist_ok=True)
            prepared = subprocess.run(
                [str(CLI), str(source), "PRAGMA journal_mode=mvcc; "
                 "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); PRAGMA wal_checkpoint(TRUNCATE);"],
                capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(prepared.returncode, 0, prepared.stdout + prepared.stderr)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            address = f"127.0.0.1:{port}"
            url = f"http://{address}" + ("/db/example" if directory else "")
            args = [str(CLI), "--sync-server", address]
            args += ["--sync-dir", str(dbs)] if directory else [str(source)]
            env = {**os.environ, "SYNC_E2E_CIPHER": "none"}
            replica = tmp / "replica.db"

            def run(action, sql=None):
                result = subprocess.run([str(DRIVER), str(replica), url, action] + ([sql] if sql else []),
                                        env=env, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                return json.loads(result.stdout)

            def rows(sql):
                body = json.dumps({"requests": [{"type": "execute", "stmt": {"sql": sql}}]}).encode()
                request = urllib.request.Request(url + "/v2/pipeline", body)
                with urllib.request.urlopen(request, timeout=10) as response:
                    result = json.load(response)["results"][0]
                self.assertEqual(result["type"], "ok", result)
                return [[cell.get("value") for cell in row] for row in result["response"]["result"]["rows"]]

            log = (tmp / "server.log").open("w+")
            process = subprocess.Popen(args, stdout=log, stderr=log)
            try:
                self.wait_for_server(process, log, port)
                # The replica reads its revision while the log has this frame.
                rows("INSERT INTO t VALUES (1, 'one')")
                run("bootstrap")
                run("pull")
                rows("PRAGMA wal_checkpoint(TRUNCATE)")
                run("exec", "INSERT INTO t VALUES (100, 'from replica')")
                run("push")
                rows(change)
                expected = rows("SELECT id, v FROM t ORDER BY id")
                self.assertEqual(run("pull")["pending"], 0, "the pushed row is pending again")
                run("push")
                self.assertEqual(rows("SELECT id, v FROM t ORDER BY id"), expected)
                self.assertEqual(run("scalar", "SELECT count(*) FROM t")["value"], len(expected))
                self.assertEqual(
                    run("scalar", "SELECT count(*) FROM t WHERE v = 'from replica'")["value"], 0
                )
            finally:
                stop_server(process)
                log.close()


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
