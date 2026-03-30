import logging
import multiprocessing
import os
import tempfile
import time

import turso
import turso.sync

from .utils import TursoServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)


def test_bootstrap():
    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
        server.db_sql("SELECT * FROM t")

        conn = turso.sync.connect(":memory:", server.db_url())
        rows = conn.execute("SELECT * FROM t").fetchall()
        assert rows == [("hello",), ("turso",), ("sync",)]


def test_pull():
    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
        server.db_sql("SELECT * FROM t")

        conn = turso.sync.connect(":memory:", server.db_url())
        rows = conn.execute("SELECT * FROM t").fetchall()
        assert rows == [("hello",), ("turso",), ("sync",)]

        server.db_sql("INSERT INTO t VALUES ('pull works')")

        rows = conn.execute("SELECT * FROM t").fetchall()
        assert rows == [("hello",), ("turso",), ("sync",)]

        assert conn.pull()

        rows = conn.execute("SELECT * FROM t").fetchall()
        assert rows == [("hello",), ("turso",), ("sync",), ("pull works",)]

        assert not conn.pull()


def test_push():
    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
        server.db_sql("SELECT * FROM t")

        conn = turso.sync.connect(":memory:", server.db_url())
        rows = conn.execute("SELECT * FROM t").fetchall()
        assert rows == [("hello",), ("turso",), ("sync",)]

        conn.execute("INSERT INTO t VALUES ('push works')")
        conn.commit()

        r1 = server.db_sql("SELECT * FROM t")
        assert r1 == [["hello"], ["turso"], ["sync"]]

        conn.push()

        r2 = server.db_sql("SELECT * FROM t")
        assert r2 == [["hello"], ["turso"], ["sync"], ["push works"]]


def test_checkpoint():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        conn = turso.sync.connect(":memory:", remote_url=server.db_url())
        conn.execute("CREATE TABLE t(x)")
        conn.commit()
        for i in range(1024):
            conn.execute(f"INSERT INTO t VALUES ({i})")
            conn.commit()
        stats1 = conn.stats()
        conn.checkpoint()
        stats2 = conn.stats()

        assert stats1.main_wal_size > 1024 * 1024
        assert stats1.revert_wal_size == 0

        assert stats2.main_wal_size == 0
        assert stats2.revert_wal_size < 8 * 1024

        conn.push()

        assert server.db_sql("SELECT SUM(x) FROM t") == [[f"{1024 * 1023 // 2}"]]


def test_partial_sync():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")

        conn_full = turso.sync.connect(":memory:", remote_url=server.db_url())
        assert conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
        assert conn_full.stats().network_received_bytes > 2000 * 1024
        assert conn_full.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]

        conn_partial = turso.sync.connect(
            ":memory:",
            remote_url=server.db_url(),
            partial_sync_experimental=turso.sync.PartialSyncOpts(
                bootstrap_strategy=turso.sync.PartialSyncPrefixBootstrap(length=128 * 1024),
            ),
        )
        assert conn_partial.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
        assert conn_partial.stats().network_received_bytes < 256 * (1024 + 10)

        start = time.time()
        assert conn_partial.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]
        print(time.time() - start)
        assert conn_partial.stats().network_received_bytes > 2000 * 1024


def test_partial_sync_segment_size():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 256)")

        conn_full = turso.sync.connect(":memory:", remote_url=server.db_url())
        assert conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
        assert conn_full.stats().network_received_bytes > 256 * 1024
        assert conn_full.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(256 * 1024,)]

        conn_partial = turso.sync.connect(
            ":memory:",
            remote_url=server.db_url(),
            partial_sync_experimental=turso.sync.PartialSyncOpts(
                bootstrap_strategy=turso.sync.PartialSyncPrefixBootstrap(length=128 * 1024),
                segment_size=4 * 1024,
            ),
        )
        assert conn_partial.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
        assert conn_partial.stats().network_received_bytes < 128 * 1024 * 1.5

        start = time.time()
        assert conn_partial.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(256 * 1024,)]
        print(time.time() - start)
        assert conn_partial.stats().network_received_bytes > 256 * 1024


def test_partial_sync_prefetch():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")

        conn_full = turso.sync.connect(":memory:", remote_url=server.db_url())
        assert conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
        assert conn_full.stats().network_received_bytes > 2000 * 1024
        assert conn_full.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]

        conn_partial = turso.sync.connect(
            ":memory:",
            remote_url=server.db_url(),
            partial_sync_experimental=turso.sync.PartialSyncOpts(
                bootstrap_strategy=turso.sync.PartialSyncPrefixBootstrap(length=128 * 1024),
                segment_size=4 * 1024,
                prefetch=True,
            ),
        )
        assert conn_partial.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
        assert conn_partial.stats().network_received_bytes < 1200 * 1024

        start = time.time()
        assert conn_partial.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]
        print(time.time() - start)
        assert conn_partial.stats().network_received_bytes > 2000 * 1024


def run_full(path: str, remote_url: str, barrier: any):
    barrier.wait(timeout=60)
    try:
        print(turso.sync.connect(path, remote_url=remote_url))
    except Exception as e:
        print("valid error", e, type(e), isinstance(e, turso.Error), turso.Error)


def test_bootstrap_concurrency():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")

        with tempfile.TemporaryDirectory(prefix="pyturso-") as dir:
            path = os.path.join(dir, "local.db")
            print(path)
            barrier = multiprocessing.Barrier(2)
            t1 = multiprocessing.Process(target=run_full, args=(path, server.db_url(), barrier))
            t2 = multiprocessing.Process(target=run_full, args=(path, server.db_url(), barrier))

            t1.start()
            t2.start()

            t1.join(timeout=120)
            t2.join(timeout=120)

            if t1.is_alive():
                t1.kill()
                t1.join()
            if t2.is_alive():
                t2.kill()
                t2.join()

            assert t1.exitcode == 0, f"t1 exitcode: {t1.exitcode}"
            assert t2.exitcode == 0, f"t2 exitcode: {t2.exitcode}"


def test_configuration_persistence():
    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t VALUES (42)")

        with tempfile.TemporaryDirectory(prefix="pyturso-") as dir:
            path = os.path.join(dir, "local.db")
            print(path)
            conn1 = turso.sync.connect(path, remote_url=server.db_url())
            assert conn1.execute("SELECT * FROM t").fetchall() == [(42,)]
            conn1.close()

            server.db_sql("INSERT INTO t VALUES (43)")

            assert "http://localhost" in open(f"{path}-info", "r").read()

            conn2 = turso.sync.connect(path)
            conn2.pull()
            assert conn2.execute("SELECT * FROM t").fetchall() == [(42,), (43,)]
            conn2.close()
