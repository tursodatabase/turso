"""Tests for the async SQLAlchemy dialect."""

import pytest

# Skip all tests if SQLAlchemy is not installed
sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import URL  # noqa: E402
from sqlalchemy.exc import DatabaseError as SADatabaseError  # noqa: E402
from sqlalchemy.exc import IntegrityError as SAIntegrityError  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine  # noqa: E402
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column  # noqa: E402
from turso.sqlalchemy import AioTursoDialect  # noqa: E402


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


def test_local_async_url_is_registered():
    """The async local dialect should resolve through the entry point."""
    engine = create_async_engine("sqlite+aioturso:///:memory:")
    assert engine.dialect.driver == "aioturso"
    assert engine.dialect.is_async is True


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
