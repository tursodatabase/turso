import logging
import random
import string
import time

import requests
import turso.sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)


def random_str() -> str:
    return "".join([random.choice(string.ascii_letters) for _ in range(8)])


def handle_response(r):
    if r.status_code == 400 and "already exists" in r.text:
        return
    r.raise_for_status()


class TursoServer:
    def __init__(self, admin_url: str, user_url: str):
        self.admin_url = admin_url
        self.user_url = user_url

    def create_tenant(self, tenant: str):
        handle_response(requests.post(self.admin_url + f"/v1/tenants/{tenant}"))

    def create_group(self, tenant: str, group: str):
        handle_response(requests.post(self.admin_url + f"/v1/tenants/{tenant}/groups/{group}"))

    def create_db(self, tenant: str, group: str, db: str):
        handle_response(requests.post(self.admin_url + f"/v1/tenants/{tenant}/groups/{group}/databases/{db}"))

    def db_url(self, tenant: str, group: str, db: str) -> str:
        tokens = self.user_url.split("://")
        return f"{tokens[0]}://{db}--{tenant}--{group}.{tokens[1]}"

    def db_sql(self, tenant: str, group: str, db: str, sql: str):
        result = requests.post(
            self.user_url + "/v2/pipeline",
            json={"requests": [{"type": "execute", "stmt": {"sql": sql}}]},
            headers={"Host": f"{db}--{tenant}--{group}.localhost"},
        )
        result.raise_for_status()
        result = result.json()
        if result["results"][0]["type"] != "ok":
            raise Exception(f"remote sql execution failed: {result}")
        return [[cell["value"] for cell in row] for row in result["results"][0]["response"]["result"]["rows"]]


server = TursoServer(admin_url="http://localhost:8081", user_url="http://localhost:8080")


def test_bootstrap():
    name = random_str()
    server.create_tenant(name)
    server.create_group(name, name)
    server.create_db(name, name, name)
    server.db_sql(name, name, name, "CREATE TABLE t(x)")
    server.db_sql(name, name, name, "INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
    server.db_sql(name, name, name, "SELECT * FROM t")

    conn = turso.sync.connect(":memory:", server.db_url(name, name, name))
    rows = conn.execute("SELECT * FROM t").fetchall()
    assert rows == [("hello",), ("turso",), ("sync",)]


def test_pull():
    name = random_str()
    server.create_tenant(name)
    server.create_group(name, name)
    server.create_db(name, name, name)
    server.db_sql(name, name, name, "CREATE TABLE t(x)")
    server.db_sql(name, name, name, "INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
    server.db_sql(name, name, name, "SELECT * FROM t")

    conn = turso.sync.connect(":memory:", server.db_url(name, name, name))
    rows = conn.execute("SELECT * FROM t").fetchall()
    assert rows == [("hello",), ("turso",), ("sync",)]

    server.db_sql(name, name, name, "INSERT INTO t VALUES ('pull works')")

    rows = conn.execute("SELECT * FROM t").fetchall()
    assert rows == [("hello",), ("turso",), ("sync",)]

    assert conn.pull()

    rows = conn.execute("SELECT * FROM t").fetchall()
    assert rows == [("hello",), ("turso",), ("sync",), ("pull works",)]

    assert not conn.pull()


def test_push():
    name = random_str()
    server.create_tenant(name)
    server.create_group(name, name)
    server.create_db(name, name, name)
    server.db_sql(name, name, name, "CREATE TABLE t(x)")
    server.db_sql(name, name, name, "INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
    server.db_sql(name, name, name, "SELECT * FROM t")

    conn = turso.sync.connect(":memory:", server.db_url(name, name, name))
    rows = conn.execute("SELECT * FROM t").fetchall()
    assert rows == [("hello",), ("turso",), ("sync",)]

    conn.execute("INSERT INTO t VALUES ('push works')")
    conn.commit()

    r1 = server.db_sql(name, name, name, "SELECT * FROM t")
    assert r1 == [["hello"], ["turso"], ["sync"]]

    conn.push()

    r2 = server.db_sql(name, name, name, "SELECT * FROM t")
    assert r2 == [["hello"], ["turso"], ["sync"], ["push works"]]


def test_checkpoint():
    # turso.setup_logging(level=logging.DEBUG)

    name = random_str()
    server.create_tenant(name)
    server.create_group(name, name)
    server.create_db(name, name, name)

    conn = turso.sync.connect(":memory:", remote_url=server.db_url(name, name, name))
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

    assert server.db_sql(name, name, name, "SELECT SUM(x) FROM t") == [[f"{1024 * 1023 // 2}"]]


def test_partial_sync():
    # turso.setup_logging(level=logging.DEBUG)

    name = random_str()
    server.create_tenant(name)
    server.create_group(name, name)
    server.create_db(name, name, name)
    server.db_sql(name, name, name, "CREATE TABLE t(x)")
    server.db_sql(name, name, name, "INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")

    conn_full = turso.sync.connect(":memory:", remote_url=server.db_url(name, name, name))
    assert conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
    assert conn_full.stats().network_received_bytes > 2000 * 1024
    assert conn_full.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]

    conn_partial = turso.sync.connect(
        ":memory:",
        remote_url=server.db_url(name, name, name),
        partial_sync_opts=turso.sync.PartialSyncOpts(
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

    name = random_str()
    server.create_tenant(name)
    server.create_group(name, name)
    server.create_db(name, name, name)
    server.db_sql(name, name, name, "CREATE TABLE t(x)")
    server.db_sql(name, name, name, "INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")

    conn_full = turso.sync.connect(":memory:", remote_url=server.db_url(name, name, name))
    assert conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
    assert conn_full.stats().network_received_bytes > 2000 * 1024
    assert conn_full.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]

    conn_partial = turso.sync.connect(
        ":memory:",
        remote_url=server.db_url(name, name, name),
        partial_sync_opts=turso.sync.PartialSyncOpts(
            bootstrap_strategy=turso.sync.PartialSyncPrefixBootstrap(length=128 * 1024),
            segment_size=4 * 1024,
        ),
    )
    assert conn_partial.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
    assert conn_partial.stats().network_received_bytes < 256 * 1024 * 1024

    start = time.time()
    assert conn_partial.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]
    print(time.time() - start)
    assert conn_partial.stats().network_received_bytes > 2000 * 1024

def test_partial_sync_prefetch():
    # turso.setup_logging(level=logging.DEBUG)

    name = random_str()
    server.create_tenant(name)
    server.create_group(name, name)
    server.create_db(name, name, name)
    server.db_sql(name, name, name, "CREATE TABLE t(x)")
    server.db_sql(name, name, name, "INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")

    conn_full = turso.sync.connect(":memory:", remote_url=server.db_url(name, name, name))
    assert conn_full.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
    assert conn_full.stats().network_received_bytes > 2000 * 1024
    assert conn_full.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]

    conn_partial = turso.sync.connect(
        ":memory:",
        remote_url=server.db_url(name, name, name),
        partial_sync_opts=turso.sync.PartialSyncOpts(
            bootstrap_strategy=turso.sync.PartialSyncPrefixBootstrap(length=128 * 1024),
            segment_size=4 * 1024,
            prefetch=True,
        ),
    )
    assert conn_partial.execute("SELECT LENGTH(x) FROM t LIMIT 1").fetchall() == [(1024,)]
    assert conn_partial.stats().network_received_bytes < 256 * 1024 * 1024

    start = time.time()
    assert conn_partial.execute("SELECT SUM(LENGTH(x)) FROM t").fetchall() == [(2000 * 1024,)]
    print(time.time() - start)
    assert conn_partial.stats().network_received_bytes > 2000 * 1024
