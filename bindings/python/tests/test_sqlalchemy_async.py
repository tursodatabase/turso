"""Tests for the async SQLAlchemy dialect."""

import pytest

# Skip all tests if SQLAlchemy is not installed
sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy import text  # noqa: E402
from sqlalchemy.dialects import registry  # noqa: E402
from sqlalchemy.engine import URL  # noqa: E402
from sqlalchemy.exc import DatabaseError as SADatabaseError  # noqa: E402
from sqlalchemy.exc import IntegrityError as SAIntegrityError  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine  # noqa: E402
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column  # noqa: E402
from turso.sqlalchemy import (  # noqa: E402
    AioTursoDialect,
    AioTursoSyncDialect,
    get_async_sync_connection,
)

registry.register("sqlite.aioturso_sync", "turso.sqlalchemy", "AioTursoSyncDialect")


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users_async"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]


def test_import_dbapi():
    """The async dialect exposes a DBAPI-shaped adapter for SQLAlchemy."""
    dbapi = AioTursoDialect.import_dbapi()
    assert hasattr(dbapi, "connect")
    assert dbapi.apilevel == "2.0"
    assert dbapi.paramstyle == "qmark"


def test_dialect_attributes():
    """The async dialect uses the sqlite+aioturso driver name."""
    assert AioTursoDialect.name == "sqlite"
    assert AioTursoDialect.driver == "aioturso"


def test_sync_import_dbapi():
    """The async sync dialect exposes a DBAPI-shaped adapter for SQLAlchemy."""
    dbapi = AioTursoSyncDialect.import_dbapi()
    assert hasattr(dbapi, "connect")
    assert dbapi.apilevel == "2.0"
    assert dbapi.paramstyle == "qmark"


def test_sync_dialect_attributes():
    """The async sync dialect uses the sqlite+aioturso_sync driver name."""
    assert AioTursoSyncDialect.name == "sqlite"
    assert AioTursoSyncDialect.driver == "aioturso_sync"


def test_memory_database_url_parsing():
    """In-memory async URLs map to a single database argument."""
    dialect = AioTursoDialect()
    url = URL.create("sqlite+aioturso", database=":memory:")

    args, kwargs = dialect.create_connect_args(url)

    assert args == [":memory:"]
    assert kwargs == {}


def test_file_database_url_parsing():
    """File async URLs map to the file path database argument."""
    dialect = AioTursoDialect()
    url = URL.create("sqlite+aioturso", database="/path/to/db.db")

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["/path/to/db.db"]
    assert kwargs == {}


def test_isolation_level_param():
    """The async dialect accepts isolation_level query parameters."""
    dialect = AioTursoDialect()
    url = URL.create(
        "sqlite+aioturso",
        database="test.db",
        query={"isolation_level": "IMMEDIATE"},
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["test.db"]
    assert kwargs["isolation_level"] == "IMMEDIATE"


def test_autocommit_isolation_level():
    """AUTOCOMMIT isolation maps to None, matching the sync local dialect."""
    dialect = AioTursoDialect()
    url = URL.create(
        "sqlite+aioturso",
        database="test.db",
        query={"isolation_level": "AUTOCOMMIT"},
    )

    _, kwargs = dialect.create_connect_args(url)

    assert kwargs["isolation_level"] is None


def test_memory_uses_static_pool():
    """Async :memory: databases use StaticPool."""
    from sqlalchemy import pool

    dialect = AioTursoDialect()
    url = URL.create("sqlite+aioturso", database=":memory:")

    pool_class = dialect.get_pool_class(url)

    assert pool_class is pool.StaticPool


def test_file_uses_async_adapted_queue_pool():
    """Async file databases use AsyncAdaptedQueuePool."""
    from sqlalchemy import pool

    dialect = AioTursoDialect()
    url = URL.create("sqlite+aioturso", database="test.db")

    pool_class = dialect.get_pool_class(url)

    assert pool_class is pool.AsyncAdaptedQueuePool


def test_sync_basic_url_parsing():
    """Async sync URLs map path and remote_url to connect args."""
    dialect = AioTursoSyncDialect()
    url = URL.create(
        "sqlite+aioturso_sync",
        database="/path/to/db.db",
        query={"remote_url": "https://db.turso.io"},
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["/path/to/db.db", "https://db.turso.io"]
    assert kwargs["client_name"] == "turso-sqlalchemy"


def test_sync_url_alias():
    """Async sync accepts sync_url as a remote_url alias."""
    dialect = AioTursoSyncDialect()
    url = URL.create(
        "sqlite+aioturso_sync",
        database="test.db",
        query={"sync_url": "https://db.turso.io"},
    )

    args, _ = dialect.create_connect_args(url)

    assert args == ["test.db", "https://db.turso.io"]


def test_sync_remote_url_takes_precedence():
    """remote_url wins when both remote_url and sync_url are supplied."""
    dialect = AioTursoSyncDialect()
    url = URL.create(
        "sqlite+aioturso_sync",
        database="test.db",
        query={
            "remote_url": "https://primary.turso.io",
            "sync_url": "https://fallback.turso.io",
        },
    )

    args, _ = dialect.create_connect_args(url)

    assert args == ["test.db", "https://primary.turso.io"]


def test_sync_all_query_params():
    """Async sync supports the same scalar query parameters as sync."""
    dialect = AioTursoSyncDialect()
    url = URL.create(
        "sqlite+aioturso_sync",
        database="test.db",
        query={
            "remote_url": "https://db.turso.io",
            "auth_token": "secret",
            "client_name": "my-app",
            "long_poll_timeout_ms": "5000",
            "bootstrap_if_empty": "false",
            "isolation_level": "IMMEDIATE",
            "experimental_features": "mvcc",
        },
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["test.db", "https://db.turso.io"]
    assert kwargs["auth_token"] == "secret"
    assert kwargs["client_name"] == "my-app"
    assert kwargs["long_poll_timeout_ms"] == 5000
    assert kwargs["bootstrap_if_empty"] is False
    assert kwargs["isolation_level"] == "IMMEDIATE"
    assert kwargs["experimental_features"] == "mvcc"


def test_sync_autocommit_isolation_level():
    """Async sync maps AUTOCOMMIT isolation to None."""
    dialect = AioTursoSyncDialect()
    url = URL.create(
        "sqlite+aioturso_sync",
        database="test.db",
        query={"remote_url": "https://db.turso.io", "isolation_level": "AUTOCOMMIT"},
    )

    _, kwargs = dialect.create_connect_args(url)

    assert kwargs["isolation_level"] is None


def test_sync_rejects_username_password():
    """Async sync rejects username/password URL components."""
    dialect = AioTursoSyncDialect()
    url = URL.create(
        "sqlite+aioturso_sync",
        username="user",
        password="pass",
        database="test.db",
        query={"remote_url": "https://db.turso.io"},
    )

    with pytest.raises(ValueError, match="username/password"):
        dialect.create_connect_args(url)


def test_sync_rejects_host_port():
    """Async sync rejects host/port URL components."""
    dialect = AioTursoSyncDialect()
    url = URL.create(
        "sqlite+aioturso_sync",
        host="localhost",
        port=5432,
        database="test.db",
        query={"remote_url": "https://db.turso.io"},
    )

    with pytest.raises(ValueError, match="host/port"):
        dialect.create_connect_args(url)


def test_sync_warns_on_unknown_params():
    """Async sync warns on unrecognized query parameters."""
    dialect = AioTursoSyncDialect()
    url = URL.create(
        "sqlite+aioturso_sync",
        database="test.db",
        query={"remote_url": "https://db.turso.io", "unknown_param": "value"},
    )

    with pytest.warns(UserWarning, match="unknown_param"):
        dialect.create_connect_args(url)


def test_sync_memory_uses_static_pool():
    """Async sync :memory: databases use StaticPool."""
    from sqlalchemy import pool

    dialect = AioTursoSyncDialect()
    url = URL.create("sqlite+aioturso_sync", database=":memory:")

    pool_class = dialect.get_pool_class(url)

    assert pool_class is pool.StaticPool


def test_sync_file_uses_async_adapted_queue_pool():
    """Async sync file databases use AsyncAdaptedQueuePool."""
    from sqlalchemy import pool

    dialect = AioTursoSyncDialect()
    url = URL.create("sqlite+aioturso_sync", database="test.db")

    pool_class = dialect.get_pool_class(url)

    assert pool_class is pool.AsyncAdaptedQueuePool


def test_local_async_url_is_registered():
    """The async local dialect should resolve through the entry point."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")
    assert engine.dialect.driver == "aioturso"
    assert engine.dialect.is_async is True


@pytest.mark.asyncio
async def test_sync_async_url_is_registered():
    """The async sync dialect should resolve through the entry point."""
    engine = create_async_engine("sqlite+aioturso_sync:///:memory:")
    assert engine.dialect.driver == "aioturso_sync"
    assert engine.dialect.is_async is True
    await engine.dispose()


@pytest.mark.asyncio
async def test_local_async_core_crud():
    """Core async CRUD works for local Turso."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"))
        await conn.execute(text("INSERT INTO items (name) VALUES ('alice')"))

    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT name FROM items"))
        assert result.scalar() == "alice"

    await engine.dispose()


@pytest.fixture
def server():
    """Create a TursoServer for async sync SQLAlchemy tests."""
    pytest.importorskip("requests")
    from tests.utils import TursoServer

    try:
        context = TursoServer()
        s = context.__enter__()
    except Exception as e:
        pytest.skip(f"TursoServer not available: {e}")
    try:
        yield s
    finally:
        context.__exit__(None, None, None)


@pytest.mark.asyncio
async def test_sync_async_core_crud(server):
    """Core async CRUD works through the async sync dialect."""
    engine = create_async_engine(
        "sqlite+aioturso_sync:///:memory:",
        connect_args={"remote_url": server.db_url()},
    )

    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE sync_items (id INTEGER PRIMARY KEY, name TEXT)"))
        await conn.execute(text("INSERT INTO sync_items (name) VALUES ('alice')"))

    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT name FROM sync_items"))
        assert result.scalar() == "alice"

    await engine.dispose()


@pytest.mark.asyncio
async def test_get_async_sync_connection(server):
    """get_async_sync_connection returns the async sync driver connection."""
    from turso.lib_sync_aio import ConnectionSync

    engine = create_async_engine(
        "sqlite+aioturso_sync:///:memory:",
        connect_args={"remote_url": server.db_url()},
    )

    async with engine.connect() as conn:
        sync = get_async_sync_connection(conn)
        assert isinstance(sync, ConnectionSync)

    await engine.dispose()


@pytest.mark.asyncio
async def test_sync_async_bootstrap_push_pull(server):
    """Async sync dialect can bootstrap, push local changes, and pull remote changes."""
    server.db_sql("CREATE TABLE t(x)")
    server.db_sql("INSERT INTO t VALUES ('remote')")

    engine = create_async_engine(
        "sqlite+aioturso_sync:///:memory:",
        connect_args={"remote_url": server.db_url()},
    )

    async with engine.connect() as conn:
        sync = get_async_sync_connection(conn)

        result = await conn.execute(text("SELECT * FROM t"))
        assert result.fetchall() == [("remote",)]

        await conn.execute(text("INSERT INTO t VALUES ('local')"))
        await conn.commit()
        await sync.push()
        assert ["local"] in server.db_sql("SELECT * FROM t")

        server.db_sql("INSERT INTO t VALUES ('pulled')")
        assert await sync.pull()

        result = await conn.execute(text("SELECT * FROM t ORDER BY x"))
        assert result.fetchall() == [("local",), ("pulled",), ("remote",)]

    await engine.dispose()


@pytest.mark.asyncio
async def test_local_async_orm():
    """AsyncSession ORM flow works for local Turso."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSession(engine) as session:
        session.add(User(name="bob"))
        await session.commit()

    async with AsyncSession(engine) as session:
        rows = (await session.execute(text("SELECT name FROM users_async ORDER BY id"))).scalars().all()
        assert rows == ["bob"]

    await engine.dispose()


@pytest.mark.asyncio
async def test_transaction_commit_rollback():
    """Explicit commit and rollback work through the async dialect."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")

    async with engine.connect() as conn:
        await conn.execute(text("CREATE TABLE txn_test (id INTEGER, val TEXT)"))
        await conn.commit()

        await conn.execute(text("INSERT INTO txn_test VALUES (1, 'should_vanish')"))
        await conn.rollback()

        result = await conn.execute(text("SELECT COUNT(*) FROM txn_test"))
        assert result.scalar() == 0

        await conn.execute(text("INSERT INTO txn_test VALUES (2, 'should_stay')"))
        await conn.commit()

        result = await conn.execute(text("SELECT val FROM txn_test WHERE id = 2"))
        assert result.scalar() == "should_stay"

    await engine.dispose()


@pytest.mark.asyncio
async def test_null_handling():
    """NULL values round-trip correctly through the async dialect."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")

    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE nullable (id INTEGER, val TEXT)"))
        await conn.execute(text("INSERT INTO nullable VALUES (1, NULL)"))

    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT val FROM nullable WHERE id = 1"))
        assert result.scalar() is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_unicode_data():
    """Unicode strings round-trip correctly through the async dialect."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")

    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE uni (id INTEGER, val TEXT)"))
        await conn.execute(text("INSERT INTO uni VALUES (1, '日本語テスト')"))
        await conn.execute(text("INSERT INTO uni VALUES (2, '🚀🎉')"))

    async with engine.connect() as conn:
        rows = (await conn.execute(text("SELECT val FROM uni ORDER BY id"))).fetchall()
        assert rows[0][0] == "日本語テスト"
        assert rows[1][0] == "🚀🎉"

    await engine.dispose()


@pytest.mark.asyncio
async def test_sql_syntax_error():
    """SQL errors propagate through SQLAlchemy's async error wrappers."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")

    async with engine.connect() as conn:
        with pytest.raises(SADatabaseError):
            await conn.execute(text("SELEKT * FORM nonexistent"))

    await engine.dispose()


@pytest.mark.asyncio
async def test_integrity_error_propagation():
    """Unique constraint violations raise SQLAlchemy IntegrityError."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")

    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE uniq (id INTEGER PRIMARY KEY, email TEXT UNIQUE)"))
        await conn.execute(text("INSERT INTO uniq VALUES (1, 'a@b.com')"))

    async with engine.connect() as conn:
        with pytest.raises(SAIntegrityError):
            await conn.execute(text("INSERT INTO uniq VALUES (2, 'a@b.com')"))

    await engine.dispose()


@pytest.mark.asyncio
async def test_multiple_connections_same_engine():
    """Async engine handles multiple sequential connections."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")

    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE multi (id INTEGER)"))
        await conn.execute(text("INSERT INTO multi VALUES (1)"))

    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT * FROM multi"))
        assert result.fetchall() == [(1,)]

    await engine.dispose()


@pytest.mark.asyncio
async def test_large_text_data():
    """Large text values round-trip correctly through the async dialect."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")
    large = "x" * 100_000

    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE big (id INTEGER, content TEXT)"))
        await conn.execute(
            text("INSERT INTO big VALUES (1, :content)"),
            {"content": large},
        )

    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT content FROM big WHERE id = 1"))
        assert result.scalar() == large

    await engine.dispose()
