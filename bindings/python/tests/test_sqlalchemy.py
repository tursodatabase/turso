"""Tests for the SQLAlchemy dialect."""

import pytest

# Skip all tests if SQLAlchemy is not installed
sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, declarative_base

from turso.sqlalchemy import TursoDialect, TursoSyncDialect, get_sync_connection


class TestTursoDialectImport:
    """Test TursoDialect module imports."""

    def test_import_dbapi(self):
        """Test that import_dbapi returns turso module."""
        dbapi = TursoDialect.import_dbapi()
        assert hasattr(dbapi, "connect")
        assert hasattr(dbapi, "Connection")
        assert hasattr(dbapi, "Cursor")
        assert dbapi.apilevel == "2.0"

    def test_dialect_attributes(self):
        """Test dialect class attributes."""
        assert TursoDialect.name == "sqlite"
        assert TursoDialect.driver == "turso"


class TestTursoSyncDialectImport:
    """Test TursoSyncDialect module imports."""

    def test_import_dbapi(self):
        """Test that import_dbapi returns turso.sync module."""
        dbapi = TursoSyncDialect.import_dbapi()
        assert hasattr(dbapi, "connect")
        assert hasattr(dbapi, "ConnectionSync")

    def test_dialect_attributes(self):
        """Test dialect class attributes."""
        assert TursoSyncDialect.name == "sqlite"
        assert TursoSyncDialect.driver == "turso_sync"


class TestTursoDialectURLParsing:
    """Test URL parsing for basic TursoDialect."""

    def test_memory_database(self):
        """Test in-memory database URL."""
        dialect = TursoDialect()
        url = URL.create("sqlite+turso", database=":memory:")

        args, kwargs = dialect.create_connect_args(url)

        assert args == [":memory:"]
        assert kwargs == {}

    def test_file_database(self):
        """Test file-based database URL."""
        dialect = TursoDialect()
        url = URL.create("sqlite+turso", database="/path/to/db.db")

        args, kwargs = dialect.create_connect_args(url)

        assert args == ["/path/to/db.db"]

    def test_isolation_level_param(self):
        """Test isolation_level query parameter."""
        dialect = TursoDialect()
        url = URL.create(
            "sqlite+turso", database="test.db", query={"isolation_level": "IMMEDIATE"}
        )

        args, kwargs = dialect.create_connect_args(url)

        assert args == ["test.db"]
        assert kwargs["isolation_level"] == "IMMEDIATE"

    def test_autocommit_isolation_level(self):
        """Test AUTOCOMMIT isolation level converts to None."""
        dialect = TursoDialect()
        url = URL.create(
            "sqlite+turso", database="test.db", query={"isolation_level": "AUTOCOMMIT"}
        )

        args, kwargs = dialect.create_connect_args(url)

        assert kwargs["isolation_level"] is None


class TestTursoSyncDialectURLParsing:
    """Test URL parsing for TursoSyncDialect."""

    def test_basic_url_parsing(self):
        """Test basic URL with path and remote_url."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="/path/to/db.db",
            query={"remote_url": "https://db.turso.io"},
        )

        args, kwargs = dialect.create_connect_args(url)

        assert args == ["/path/to/db.db", "https://db.turso.io"]
        assert kwargs.get("client_name") == "turso-sqlalchemy"

    def test_memory_database(self):
        """Test in-memory database URL."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database=":memory:",
            query={"remote_url": "https://db.turso.io"},
        )

        args, kwargs = dialect.create_connect_args(url)

        assert args[0] == ":memory:"

    def test_all_query_params(self):
        """Test all supported query parameters."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="test.db",
            query={
                "remote_url": "https://db.turso.io",
                "auth_token": "secret",
                "client_name": "my-app",
                "long_poll_timeout_ms": "5000",
                "bootstrap_if_empty": "false",
                "isolation_level": "IMMEDIATE",
            },
        )

        args, kwargs = dialect.create_connect_args(url)

        assert args == ["test.db", "https://db.turso.io"]
        assert kwargs["auth_token"] == "secret"
        assert kwargs["client_name"] == "my-app"
        assert kwargs["long_poll_timeout_ms"] == 5000
        assert kwargs["bootstrap_if_empty"] is False
        assert kwargs["isolation_level"] == "IMMEDIATE"

    def test_bootstrap_if_empty_variations(self):
        """Test various boolean string representations."""
        dialect = TursoSyncDialect()

        for true_val in ["true", "True", "TRUE", "1", "yes"]:
            url = URL.create(
                "sqlite+turso_sync",
                database="test.db",
                query={"remote_url": "https://db.turso.io", "bootstrap_if_empty": true_val},
            )
            _, kwargs = dialect.create_connect_args(url)
            assert kwargs["bootstrap_if_empty"] is True, f"Failed for {true_val}"

        for false_val in ["false", "False", "FALSE", "0", "no"]:
            url = URL.create(
                "sqlite+turso_sync",
                database="test.db",
                query={"remote_url": "https://db.turso.io", "bootstrap_if_empty": false_val},
            )
            _, kwargs = dialect.create_connect_args(url)
            assert kwargs["bootstrap_if_empty"] is False, f"Failed for {false_val}"

    def test_rejects_username_password(self):
        """Test that username/password in URL raises error."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            username="user",
            password="pass",
            database="test.db",
            query={"remote_url": "https://db.turso.io"},
        )

        with pytest.raises(ValueError, match="username/password"):
            dialect.create_connect_args(url)

    def test_rejects_host_port(self):
        """Test that host/port in URL raises error."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            host="localhost",
            port=5432,
            database="test.db",
            query={"remote_url": "https://db.turso.io"},
        )

        with pytest.raises(ValueError, match="host/port"):
            dialect.create_connect_args(url)

    def test_warns_on_unknown_params(self):
        """Test that unknown query parameters trigger a warning."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="test.db",
            query={
                "remote_url": "https://db.turso.io",
                "unknown_param": "value",
            },
        )

        with pytest.warns(UserWarning, match="unknown_param"):
            dialect.create_connect_args(url)


class TestPoolClass:
    """Test connection pool class selection."""

    def test_memory_uses_singleton_pool(self):
        """Test that :memory: databases use SingletonThreadPool."""
        from sqlalchemy import pool

        dialect = TursoDialect()
        url = URL.create("sqlite+turso", database=":memory:")

        pool_class = dialect.get_pool_class(url)

        assert pool_class is pool.SingletonThreadPool

    def test_file_uses_queue_pool(self):
        """Test that file databases use QueuePool."""
        from sqlalchemy import pool

        dialect = TursoDialect()
        url = URL.create("sqlite+turso", database="test.db")

        pool_class = dialect.get_pool_class(url)

        assert pool_class is pool.QueuePool


class TestGetSyncConnectionErrors:
    """Test get_sync_connection error handling."""

    def test_raises_for_non_sync_dialect(self):
        """Test that get_sync_connection raises for non-turso connections."""
        engine = create_engine("sqlite:///:memory:")

        with engine.connect() as conn:
            with pytest.raises(TypeError, match="ConnectionSync"):
                get_sync_connection(conn)


class TestBasicDialectIntegration:
    """Integration tests for the basic TursoDialect."""

    def test_basic_crud(self):
        """Test basic CRUD operations through SQLAlchemy."""
        engine = create_engine("sqlite+turso:///:memory:")

        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE test (id INTEGER, name TEXT)"))
            conn.execute(text("INSERT INTO test VALUES (1, 'alice')"))
            conn.commit()

            result = conn.execute(text("SELECT * FROM test"))
            rows = result.fetchall()

            assert rows == [(1, "alice")]

    def test_orm_usage(self):
        """Test ORM usage with basic dialect."""
        Base = declarative_base()

        class Item(Base):
            __tablename__ = "items"
            id = Column(Integer, primary_key=True)
            name = Column(String(100))

        engine = create_engine("sqlite+turso:///:memory:")
        Base.metadata.create_all(engine)

        with Session(engine) as session:
            session.add(Item(name="Widget"))
            session.commit()

            items = session.query(Item).all()
            assert len(items) == 1
            assert items[0].name == "Widget"


class TestSyncDialectIntegration:
    """Integration tests for sync dialect with real sync server.

    These tests require a running sync server. They will be skipped
    if the server is not available.
    """

    @pytest.fixture
    def server(self):
        """Create a TursoServer for testing."""
        requests = pytest.importorskip("requests")
        from tests.utils import TursoServer

        try:
            with TursoServer() as s:
                yield s
        except Exception as e:
            pytest.skip(f"TursoServer not available: {e}")

    def test_basic_sync_connection(self, server):
        """Test creating a sync connection through SQLAlchemy."""
        engine = create_engine(
            "sqlite+turso_sync:///:memory:",
            connect_args={"remote_url": server.db_url()},
        )

        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE test (id INTEGER, name TEXT)"))
            conn.execute(text("INSERT INTO test VALUES (1, 'alice')"))
            conn.commit()

            result = conn.execute(text("SELECT * FROM test"))
            rows = result.fetchall()

            assert rows == [(1, "alice")]

    def test_get_sync_connection(self, server):
        """Test that get_sync_connection returns ConnectionSync."""
        from turso.lib_sync import ConnectionSync

        engine = create_engine(
            "sqlite+turso_sync:///:memory:",
            connect_args={"remote_url": server.db_url()},
        )

        with engine.connect() as conn:
            sync = get_sync_connection(conn)
            assert isinstance(sync, ConnectionSync)

    def test_sync_operations(self, server):
        """Test pull/push through SQLAlchemy connection."""
        # Create initial data on server
        server.db_sql("CREATE TABLE t(x)")
        server.db_sql("INSERT INTO t VALUES ('remote')")

        engine = create_engine(
            "sqlite+turso_sync:///:memory:",
            connect_args={"remote_url": server.db_url()},
        )

        with engine.connect() as conn:
            sync = get_sync_connection(conn)

            # Data should be bootstrapped
            result = conn.execute(text("SELECT * FROM t"))
            assert result.fetchall() == [("remote",)]

            # Insert locally
            conn.execute(text("INSERT INTO t VALUES ('local')"))
            conn.commit()

            # Push to remote
            sync.push()

            # Verify on remote
            remote_rows = server.db_sql("SELECT * FROM t")
            assert ["local"] in remote_rows

    def test_orm_with_sync(self, server):
        """Test ORM usage with sync dialect."""
        Base = declarative_base()

        class Item(Base):
            __tablename__ = "items"
            id = Column(Integer, primary_key=True)
            name = Column(String(100))

        engine = create_engine(
            "sqlite+turso_sync:///:memory:",
            connect_args={"remote_url": server.db_url()},
        )

        Base.metadata.create_all(engine)

        with Session(engine) as session:
            session.add(Item(name="Widget"))
            session.commit()

            items = session.query(Item).all()
            assert len(items) == 1
            assert items[0].name == "Widget"

    def test_url_with_remote_url_param(self, server):
        """Test passing remote_url as URL query parameter."""
        db_url = server.db_url()
        engine = create_engine(f"sqlite+turso_sync:///:memory:?remote_url={db_url}")

        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE test (x TEXT)"))
            conn.execute(text("INSERT INTO test VALUES ('hello')"))
            conn.commit()

            result = conn.execute(text("SELECT * FROM test"))
            assert result.fetchall() == [("hello",)]
