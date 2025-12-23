import logging
import time

import pytest
import turso.aio.sync

from .utils import TursoServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)

@pytest.mark.asyncio
async def test_bootstrap():
    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
        server.db_sql("SELECT * FROM t")

        conn = await turso.aio.sync.connect(":memory:", server.db_url())
        rows = await (await conn.execute("SELECT * FROM t")).fetchall()
        assert rows == [("hello",), ("turso",), ("sync",)]


@pytest.mark.asyncio
async def test_pull():
    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
        server.db_sql("SELECT * FROM t")

        conn = await turso.aio.sync.connect(":memory:", server.db_url())
        rows = await (await conn.execute("SELECT * FROM t")).fetchall()
        assert rows == [("hello",), ("turso",), ("sync",)]

        server.db_sql("INSERT INTO t VALUES ('pull works')")

        rows = await (await conn.execute("SELECT * FROM t")).fetchall()
        assert rows == [("hello",), ("turso",), ("sync",)]

        assert await conn.pull()

        rows = await (await conn.execute("SELECT * FROM t")).fetchall()
        assert rows == [("hello",), ("turso",), ("sync",), ("pull works",)]

        assert not await conn.pull()


@pytest.mark.asyncio
async def test_push():
    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
        server.db_sql("SELECT * FROM t")

        conn = await turso.aio.sync.connect(":memory:", server.db_url())
        rows = await (await conn.execute("SELECT * FROM t")).fetchall()
        assert rows == [("hello",), ("turso",), ("sync",)]

        await conn.execute("INSERT INTO t VALUES ('push works')")
        await conn.commit()

        r1 = server.db_sql("SELECT * FROM t")
        assert r1 == [["hello"], ["turso"], ["sync"]]

        await conn.push()

        r2 = server.db_sql("SELECT * FROM t")
        assert r2 == [["hello"], ["turso"], ["sync"], ["push works"]]

@pytest.mark.asyncio
async def test_checkpoint():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        conn = await turso.aio.sync.connect(":memory:", remote_url=server.db_url())
        await conn.execute("CREATE TABLE t(x)")
        await conn.commit()
        for i in range(1024):
            await conn.execute(f"INSERT INTO t VALUES ({i})")
            await conn.commit()
        stats1 = await conn.stats()
        await conn.checkpoint()
        stats2 = await conn.stats()

        assert stats1.main_wal_size > 1024 * 1024
        assert stats1.revert_wal_size == 0

        assert stats2.main_wal_size == 0
        assert stats2.revert_wal_size < 8 * 1024

        await conn.push()

        assert server.db_sql("SELECT SUM(x) FROM t") == [[f"{1024 * 1023 // 2}"]]


@pytest.mark.asyncio
async def test_partial_sync():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")

        conn_full = await turso.aio.sync.connect(":memory:", remote_url=server.db_url())
        assert await (await conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1")).fetchall() == [(1024,)]
        assert (await conn_full.stats()).network_received_bytes > 2000 * 1024
        assert await (await conn_full.execute("SELECT SUM(LENGTH(x)) FROM t")).fetchall() == [(2000 * 1024,)]

        conn_partial = await turso.aio.sync.connect(
            ":memory:",
            remote_url=server.db_url(),
            partial_sync_experimental=turso.aio.sync.PartialSyncOpts(
                bootstrap_strategy=turso.aio.sync.PartialSyncPrefixBootstrap(length=128*1024),
            ),
        )
        assert await (await conn_partial.execute("SELECT LENGTH(x) FROM t LIMIT 1")).fetchall() == [(1024,)]
        assert (await conn_partial.stats()).network_received_bytes < 256 * (1024 + 10)

        start = time.time()
        assert await (await conn_partial.execute("SELECT SUM(LENGTH(x)) FROM t")).fetchall() == [(2000 * 1024,)]
        print(time.time() - start)
        assert (await conn_partial.stats()).network_received_bytes > 2000 * 1024

@pytest.mark.asyncio
async def test_partial_sync_segment_size():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 256)")

        conn_full = await turso.aio.sync.connect(":memory:", remote_url=server.db_url())
        assert await (await conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1")).fetchall() == [(1024,)]
        assert (await conn_full.stats()).network_received_bytes > 256 * 1024
        assert await (await conn_full.execute("SELECT SUM(LENGTH(x)) FROM t")).fetchall() == [(256 * 1024,)]

        conn_partial = await turso.aio.sync.connect(
            ":memory:",
            remote_url=server.db_url(),
            partial_sync_experimental=turso.aio.sync.PartialSyncOpts(
                bootstrap_strategy=turso.aio.sync.PartialSyncPrefixBootstrap(length=128*1024),
                segment_size=4 * 1024,
            ),
        )
        assert await (await conn_partial.execute("SELECT LENGTH(x) FROM t LIMIT 1")).fetchall() == [(1024,)]
        assert (await conn_partial.stats()).network_received_bytes < 128 * 1024 * 1.5

        start = time.time()
        assert await (await conn_partial.execute("SELECT SUM(LENGTH(x)) FROM t")).fetchall() == [(256 * 1024,)]
        print(time.time() - start)
        assert (await conn_partial.stats()).network_received_bytes > 256 * 1024

@pytest.mark.asyncio
async def test_partial_sync_prefetch():
    # turso.setup_logging(level=logging.DEBUG)

    with TursoServer() as server:
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")

        conn_full = await turso.aio.sync.connect(":memory:", remote_url=server.db_url())
        assert await (await conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1")).fetchall() == [(1024,)]
        assert (await conn_full.stats()).network_received_bytes > 2000 * 1024
        assert await (await conn_full.execute("SELECT SUM(LENGTH(x)) FROM t")).fetchall() == [(2000 * 1024,)]

        # turso.setup_logging(logging.DEBUG)
        conn_partial = await turso.aio.sync.connect(
            ":memory:",
            remote_url=server.db_url(),
            partial_sync_experimental=turso.aio.sync.PartialSyncOpts(
                bootstrap_strategy=turso.aio.sync.PartialSyncPrefixBootstrap(length=128*1024),
                segment_size=4 * 1024,
                prefetch=True,
            ),
        )
        assert await (await conn_partial.execute("SELECT LENGTH(x) FROM t LIMIT 1")).fetchall() == [(1024,)]
        assert (await conn_partial.stats()).network_received_bytes < 1200 * 1024

        start = time.time()
        assert await (await conn_partial.execute("SELECT SUM(LENGTH(x)) FROM t")).fetchall() == [(2000 * 1024,)]
        print(time.time() - start)
        assert (await conn_partial.stats()).network_received_bytes > 2000 * 1024
