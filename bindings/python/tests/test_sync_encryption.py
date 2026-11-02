"""Binding runtime coverage with an empty encrypted-source bootstrap response."""
import asyncio
import base64
import http.server
import tempfile
import threading
import unittest
from pathlib import Path

from turso import sync
from turso.aio import sync as aio_sync

LOCAL_KEY = "16" * 32
CLOUD_KEY = base64.b64encode(bytes([11]) * 32).decode()
HEADER = bytes([10, 3, 114, 101, 118, 26, 0, 64, 1])


class EmptyEncryptedSource(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
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
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "replica.db")
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), EmptyEncryptedSource)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def options(self, online):
        return dict(remote_url=self.url if online else None,
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
