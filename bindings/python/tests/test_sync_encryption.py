"""Encrypted sync binding coverage with real and empty encrypted sources."""
import asyncio
import base64
import contextlib
import http.server
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path

from turso import sync
from turso.aio import sync as aio_sync

LOCAL_KEY = "16" * 32
CLOUD_KEY = base64.b64encode(bytes([11]) * 32).decode()
HEADER = bytes([10, 3, 114, 101, 118, 26, 0, 64, 1])


@contextlib.contextmanager
def real_encrypted_source(cipher):
    size = 32 if cipher == "aes256gcm" else 16
    cloud_key = base64.b64encode(bytes([11]) * size).decode()
    local_key = "16" * size
    root = Path(__file__).resolve().parents[3]
    executable = os.environ.get(
        "LOCAL_SYNC_SERVER", str(root / "target/debug" / ("tursodb.exe" if os.name == "nt" else "tursodb"))
    )
    with tempfile.TemporaryDirectory() as tmp, socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.close()
        key = Path(tmp) / "key.hex"
        key.write_text("0b" * size)
        with (Path(tmp) / "server.log").open("w+") as log:
            process = subprocess.Popen(
                [executable, "--sync-server", f"127.0.0.1:{port}", "--sync-encryption-cipher", cipher,
                 "--sync-encryption-key-file", str(key), str(Path(tmp) / "source.db")],
                stdout=log, stderr=log,
            )
            url = f"http://127.0.0.1:{port}"
            try:
                for _ in range(200):
                    if process.poll() is not None:
                        log.seek(0)
                        raise AssertionError(log.read())
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=.1):
                            break
                    except OSError:
                        time.sleep(.025)
                else:
                    raise AssertionError("encrypted source readiness timeout")

                def sql(statement):
                    request = urllib.request.Request(url + "/v2/pipeline", json.dumps({"requests": [
                        {"type": "execute", "stmt": {"sql": statement}}]}).encode(), headers={
                        "x-turso-encryption-key": cloud_key, "x-turso-encryption-cipher": cipher})
                    with urllib.request.urlopen(request, timeout=10) as response:
                        result = json.load(response)["results"][0]
                    assert result["type"] == "ok", result
                    return result["response"]["result"]

                sql("PRAGMA journal_mode=mvcc")
                sql("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
                sql("INSERT INTO items VALUES (1, 'nonempty bootstrap')")
                yield url, cloud_key, local_key, sql
            finally:
                process.terminate()
                process.wait(timeout=10)


class EmptyEncryptedSource(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.server.requests += 1
        assert self.headers["x-turso-encryption-key"] == CLOUD_KEY
        assert self.headers["x-turso-encryption-cipher"] == "aes256gcm"
        assert LOCAL_KEY not in str(self.headers)
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        body = bytes([len(HEADER)]) + HEADER
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


class SyncEncryptionTests(unittest.TestCase):
    def test_local_encryption_requires_exact_experimental_feature_before_io(self):
        for features in [None, "encryption", "sync_encryption_extra"]:
            with self.subTest(features=features):
                options = self.options(True)
                options["experimental_features"] = features
                with self.assertRaisesRegex(Exception, "sync_encryption"):
                    sync.connect(self.path, **options)
                self.assertEqual(self.server.requests, 0)
                self.assertFalse(Path(self.path).exists())
                self.assertFalse(Path(self.path + "-info").exists())

    def test_real_incremental_sync_and_cipher_mappings(self):
        for cipher, enum in [("aes256gcm", sync.RemoteEncryptionCipher.Aes256Gcm),
                            ("aes128gcm", sync.RemoteEncryptionCipher.Aes128Gcm),
                            ("aegis128l", sync.RemoteEncryptionCipher.Aegis128L)]:
            with self.subTest(cipher=cipher), real_encrypted_source(cipher) as (url, cloud, local, sql):
                path = str(Path(self.directory.name) / (cipher + ".db"))
                options = dict(
                    experimental_features="sync_encryption", remote_encryption_cipher=enum, local_encryption_key=local
                )
                db = sync.connect(path, remote_url=url, remote_encryption_key=cloud, **options)
                try:
                    self.assertEqual(
                        db.execute("SELECT value FROM items WHERE id=1").fetchone(), ("nonempty bootstrap",)
                    )
                    db.execute("INSERT INTO items VALUES (2, 'python local marker')")
                    db.commit()
                    db.push()
                    self.assertEqual(sql("SELECT count(*) FROM items")["rows"][0][0]["value"], "2")
                    sql("INSERT INTO items VALUES (3, hex(zeroblob(40000)))")
                    db.pull()
                    self.assertEqual(db.execute("SELECT length(value) FROM items WHERE id=3").fetchone(), (80000,))
                    db.checkpoint()
                finally:
                    db.close()
                db = sync.connect(path, **options)
                try:
                    self.assertEqual(db.execute("SELECT count(*) FROM items").fetchone(), (3,))
                finally:
                    db.close()

    def test_real_async_incremental_sync(self):
        with real_encrypted_source("aes256gcm") as (url, cloud, local, sql):
            async def run():
                options = dict(
                    experimental_features="sync_encryption",
                    remote_encryption_cipher=sync.RemoteEncryptionCipher.Aes256Gcm,
                    local_encryption_key=local,
                )
                db = await aio_sync.connect(self.path, remote_url=url, remote_encryption_key=cloud, **options)
                try:
                    await db.execute("INSERT INTO items VALUES (2, 'async local marker')")
                    await db.commit()
                    await db.push()
                    self.assertEqual(sql("SELECT count(*) FROM items")["rows"][0][0]["value"], "2")
                    sql("INSERT INTO items VALUES (3, hex(zeroblob(40000)))")
                    await db.pull()
                    cursor = await db.execute("SELECT length(value) FROM items WHERE id=3")
                    self.assertEqual(await cursor.fetchone(), (80000,))
                    await cursor.close()
                    await db.checkpoint()
                finally:
                    await db.close()
                db = await aio_sync.connect(self.path, **options)
                try:
                    cursor = await db.execute("SELECT count(*) FROM items")
                    self.assertEqual(await cursor.fetchone(), (3,))
                    await cursor.close()
                finally:
                    await db.close()
            asyncio.run(run())

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "replica.db")
        self.server = None

    def options(self, online):
        if online and self.server is None:
            self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), EmptyEncryptedSource)
            self.server.requests = 0
            thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(self.server.server_close)
            self.addCleanup(self.server.shutdown)
        return dict(experimental_features="sync_encryption",
                    remote_url=f"http://127.0.0.1:{self.server.server_port}" if online else None,
                    remote_encryption_cipher=sync.RemoteEncryptionCipher.Aes256Gcm,
                    remote_encryption_key=CLOUD_KEY if online else None,
                    local_encryption_key=LOCAL_KEY)

    def assert_encrypted(self):
        self.assertTrue(Path(self.path + "-info").read_bytes().startswith(b"TSYNMETA"))
        self.assertNotIn(b"binding private marker", Path(self.path).read_bytes())

    def test_sync_bootstrap_checkpoint_offline_reopen_and_wrong_key(self):
        db = sync.connect(self.path, **self.options(True))
        db.execute("CREATE TABLE t(value TEXT)")
        db.execute("INSERT INTO t VALUES ('binding private marker')")
        db.commit()
        db.checkpoint()
        db.close()
        self.assert_encrypted()
        db = sync.connect(self.path, **self.options(False))
        self.assertEqual(db.execute("SELECT value FROM t").fetchone(), ("binding private marker",))
        db.close()
        opts = self.options(False)
        opts["local_encryption_key"] = "17" * 32
        with self.assertRaises(Exception):
            sync.connect(self.path, **opts)

    def test_async_bootstrap_checkpoint_and_offline_reopen(self):
        async def run():
            db = await aio_sync.connect(self.path, **self.options(True))
            await db.execute("CREATE TABLE t(value TEXT)")
            await db.execute("INSERT INTO t VALUES ('binding private marker')")
            await db.commit()
            await db.checkpoint()
            await db.close()
            self.assert_encrypted()
            db = await aio_sync.connect(self.path, **self.options(False))
            cursor = await db.execute("SELECT value FROM t")
            self.assertEqual(await cursor.fetchone(), ("binding private marker",))
            await db.close()
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
