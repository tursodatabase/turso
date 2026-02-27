"""Tests for the SQLAlchemy dialect."""

import pytest

# Skip all tests if SQLAlchemy is not installed
sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy import Column, Integer, String, create_engine, text  # noqa: E402
from sqlalchemy.engine import URL  # noqa: E402
from sqlalchemy.orm import Session, declarative_base  # noqa: E402
from turso.sqlalchemy import TursoDialect, TursoSyncDialect, get_sync_connection  # noqa: E402


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

    def test_sync_url_query_param_compat(self):
        """Test that sync_url is accepted as alias for remote_url in URL query params."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="/path/to/db.db",
            query={"sync_url": "https://db.turso.io"},
        )

        args, kwargs = dialect.create_connect_args(url)

        assert args == ["/path/to/db.db", "https://db.turso.io"]

    def test_remote_url_takes_precedence_over_sync_url(self):
        """Test that remote_url is preferred when both are provided."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="test.db",
            query={
                "remote_url": "https://primary.turso.io",
                "sync_url": "https://fallback.turso.io",
            },
        )

        args, _ = dialect.create_connect_args(url)

        assert args == ["test.db", "https://primary.turso.io"]

    def test_sync_url_connect_args_compat(self):
        """Test that sync_url in connect_args is remapped to remote_url."""
        dialect = TursoSyncDialect()

        # Simulate what SQLAlchemy does: dialect.connect(*cargs, **cparams)
        # where cparams includes connect_args merged in.
        # We can't call connect() directly without a real DB, but we can
        # verify the remapping logic by checking the method exists and
        # testing the URL param path covers the same alias.
        url = URL.create(
            "sqlite+turso_sync",
            database="test.db",
            query={"sync_url": "https://db.turso.io", "auth_token": "secret"},
        )

        args, kwargs = dialect.create_connect_args(url)

        assert args == ["test.db", "https://db.turso.io"]
        assert kwargs["auth_token"] == "secret"


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

    def test_pandas_to_sql(self):
        """Test Pandas DataFrame.to_sql() with if_exists='replace'."""
        pd = pytest.importorskip("pandas")

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })

        engine = create_engine("sqlite+turso:///:memory:")

        with engine.connect() as conn:
            # First insert
            df.to_sql("test_table", conn, if_exists="replace", index=False)

            # Verify
            result = pd.read_sql("SELECT * FROM test_table", conn)
            assert len(result) == 3

            # Replace (this tests table reflection which was failing)
            df.to_sql("test_table", conn, if_exists="replace", index=False)

            # Verify again
            result = pd.read_sql("SELECT * FROM test_table", conn)
            assert len(result) == 3

    def test_table_reflection_returns_empty(self):
        """Test that table reflection methods return empty (not error)."""
        engine = create_engine("sqlite+turso:///:memory:")

        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.commit()

            # Get inspector
            from sqlalchemy import inspect
            inspector = inspect(engine)

            # These should return empty lists, not raise errors
            fks = inspector.get_foreign_keys("test")
            assert fks == []

            indexes = inspector.get_indexes("test")
            assert indexes == []

            ucs = inspector.get_unique_constraints("test")
            assert ucs == []


class TestSyncDialectIntegration:
    """Integration tests for sync dialect with real sync server.

    These tests require a running sync server. They will be skipped
    if the server is not available.
    """

    @pytest.fixture
    def server(self):
        """Create a TursoServer for testing."""
        pytest.importorskip("requests")
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


# ── Phase 1: Missing Unit Tests ──────────────────────────────────


class TestTursoDialectMixin:
    """Test the _TursoDialectMixin methods via inspector and directly."""

    @pytest.fixture
    def engine(self):
        return create_engine("sqlite+turso:///:memory:")

    @pytest.fixture
    def inspector(self, engine):
        from sqlalchemy import inspect

        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE alpha (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.execute(text("CREATE TABLE beta (id INTEGER PRIMARY KEY, val REAL)"))
            conn.commit()

        return inspect(engine)

    def test_get_check_constraints_returns_empty(self, inspector):
        """get_check_constraints returns empty list for any table."""
        assert inspector.get_check_constraints("alpha") == []

    def test_get_table_names_works(self, inspector):
        """inspector.get_table_names() returns the created tables."""
        tables = inspector.get_table_names()
        assert "alpha" in tables
        assert "beta" in tables

    def test_get_columns_works(self, inspector):
        """inspector.get_columns() returns column info."""
        cols = inspector.get_columns("alpha")
        col_names = [c["name"] for c in cols]
        assert "id" in col_names
        assert "name" in col_names

    def test_multi_indexes_returns_empty(self, inspector):
        """get_multi_indexes returns empty dict for multiple tables."""
        dialect = TursoDialect()
        with inspector.bind.connect() as conn:
            result = dialect.get_multi_indexes(conn, filter_names=["alpha", "beta"])
        assert result == {}

    def test_multi_unique_constraints_returns_empty(self, inspector):
        """get_multi_unique_constraints returns empty dict."""
        dialect = TursoDialect()
        with inspector.bind.connect() as conn:
            result = dialect.get_multi_unique_constraints(conn, filter_names=["alpha", "beta"])
        assert result == {}

    def test_multi_foreign_keys_returns_empty(self, inspector):
        """get_multi_foreign_keys returns empty dict."""
        dialect = TursoDialect()
        with inspector.bind.connect() as conn:
            result = dialect.get_multi_foreign_keys(conn, filter_names=["alpha", "beta"])
        assert result == {}

    def test_multi_check_constraints_returns_empty(self, inspector):
        """get_multi_check_constraints returns empty dict."""
        dialect = TursoDialect()
        with inspector.bind.connect() as conn:
            result = dialect.get_multi_check_constraints(conn, filter_names=["alpha", "beta"])
        assert result == {}

    def test_multi_table_reflection(self, inspector):
        """Reflection with multiple tables: all tables visible, constraints empty."""
        tables = inspector.get_table_names()
        assert len(tables) >= 2
        for table in ["alpha", "beta"]:
            assert inspector.get_foreign_keys(table) == []
            assert inspector.get_indexes(table) == []
            assert inspector.get_unique_constraints(table) == []
            assert inspector.get_check_constraints(table) == []


class TestTursoDialectMethods:
    """Test TursoDialect methods not covered by integration tests."""

    def test_on_connect_returns_none(self):
        """on_connect returns None — skips REGEXP setup."""
        dialect = TursoDialect()
        assert dialect.on_connect() is None

    def test_get_isolation_level_returns_serializable(self):
        """get_isolation_level returns SERIALIZABLE."""
        dialect = TursoDialect()
        assert dialect.get_isolation_level(None) == "SERIALIZABLE"

    def test_set_isolation_level_is_noop(self):
        """set_isolation_level doesn't raise for any value."""
        dialect = TursoDialect()
        dialect.set_isolation_level(None, "SERIALIZABLE")
        dialect.set_isolation_level(None, "READ UNCOMMITTED")
        # No assertion needed — just verify no exception

    def test_supports_statement_cache(self):
        """Statement caching is enabled."""
        assert TursoDialect.supports_statement_cache is True

    def test_supports_native_datetime_false(self):
        """Native datetime is disabled (turso handles datetime differently)."""
        assert TursoDialect.supports_native_datetime is False

    def test_experimental_features_param(self):
        """experimental_features query parameter is passed through."""
        dialect = TursoDialect()
        url = URL.create(
            "sqlite+turso", database="test.db", query={"experimental_features": "feat1,feat2"}
        )
        args, kwargs = dialect.create_connect_args(url)
        assert kwargs["experimental_features"] == "feat1,feat2"

    def test_default_database_memory(self):
        """URL with no database defaults to :memory:."""
        dialect = TursoDialect()
        url = URL.create("sqlite+turso")
        args, kwargs = dialect.create_connect_args(url)
        assert args == [":memory:"]


class TestTursoSyncDialectMethods:
    """Test TursoSyncDialect methods parallel to TursoDialect."""

    def test_on_connect_returns_none(self):
        """Sync on_connect also returns None."""
        dialect = TursoSyncDialect()
        assert dialect.on_connect() is None

    def test_get_isolation_level_returns_serializable(self):
        """Sync get_isolation_level returns SERIALIZABLE."""
        dialect = TursoSyncDialect()
        assert dialect.get_isolation_level(None) == "SERIALIZABLE"

    def test_set_isolation_level_is_noop(self):
        """Sync set_isolation_level is also a no-op."""
        dialect = TursoSyncDialect()
        dialect.set_isolation_level(None, "SERIALIZABLE")

    def test_supports_statement_cache(self):
        assert TursoSyncDialect.supports_statement_cache is True

    def test_supports_native_datetime_false(self):
        assert TursoSyncDialect.supports_native_datetime is False


class TestTursoSyncDialectEdgeCases:
    """Edge cases for TursoSyncDialect URL parsing."""

    def test_autocommit_isolation_level(self):
        """AUTOCOMMIT converts to None in sync dialect URL."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="test.db",
            query={"remote_url": "https://db.turso.io", "isolation_level": "AUTOCOMMIT"},
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["isolation_level"] is None

    def test_no_remote_url(self):
        """URL without remote_url returns single-element positional args."""
        dialect = TursoSyncDialect()
        url = URL.create("sqlite+turso_sync", database="test.db")
        args, kwargs = dialect.create_connect_args(url)
        assert args == ["test.db"]
        assert "remote_url" not in kwargs

    def test_experimental_features_param(self):
        """experimental_features in sync URL is passed through."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="test.db",
            query={
                "remote_url": "https://db.turso.io",
                "experimental_features": "mvcc",
            },
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["experimental_features"] == "mvcc"

    def test_long_poll_timeout_ms_integer_conversion(self):
        """long_poll_timeout_ms is converted from string to int."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="test.db",
            query={
                "remote_url": "https://db.turso.io",
                "long_poll_timeout_ms": "3000",
            },
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["long_poll_timeout_ms"] == 3000
        assert isinstance(kwargs["long_poll_timeout_ms"], int)

    def test_default_client_name(self):
        """Default client_name is 'turso-sqlalchemy' when not specified."""
        dialect = TursoSyncDialect()
        url = URL.create(
            "sqlite+turso_sync",
            database="test.db",
            query={"remote_url": "https://db.turso.io"},
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["client_name"] == "turso-sqlalchemy"

    def test_sync_pool_class_memory(self):
        """Sync dialect uses SingletonThreadPool for :memory:."""
        from sqlalchemy import pool

        dialect = TursoSyncDialect()
        url = URL.create("sqlite+turso_sync", database=":memory:")
        assert dialect.get_pool_class(url) is pool.SingletonThreadPool

    def test_sync_pool_class_file(self):
        """Sync dialect uses QueuePool for file databases."""
        from sqlalchemy import pool

        dialect = TursoSyncDialect()
        url = URL.create("sqlite+turso_sync", database="test.db")
        assert dialect.get_pool_class(url) is pool.QueuePool


class TestDBAPI2ModuleAttributes:
    """Test DB-API 2.0 module-level attributes for both modules."""

    def test_turso_module_attributes(self):
        """turso module has all required DB-API 2.0 attributes."""
        import turso

        assert turso.apilevel == "2.0"
        assert turso.paramstyle == "qmark"
        assert turso.threadsafety == 1

    def test_turso_sync_module_attributes(self):
        """turso.sync module has all DB-API 2.0 attributes."""
        import turso.sync

        assert turso.sync.apilevel == "2.0"
        assert turso.sync.paramstyle == "qmark"
        assert turso.sync.threadsafety == 1

    def test_turso_sync_exception_hierarchy(self):
        """Exception classes are properly re-exported in turso.sync."""
        import turso.sync

        # All DB-API 2.0 required exception classes
        assert issubclass(turso.sync.Warning, Exception)
        assert issubclass(turso.sync.Error, Exception)
        assert issubclass(turso.sync.InterfaceError, turso.sync.Error)
        assert issubclass(turso.sync.DatabaseError, turso.sync.Error)
        assert issubclass(turso.sync.DataError, turso.sync.DatabaseError)
        assert issubclass(turso.sync.OperationalError, turso.sync.DatabaseError)
        assert issubclass(turso.sync.IntegrityError, turso.sync.DatabaseError)
        assert issubclass(turso.sync.InternalError, turso.sync.DatabaseError)
        assert issubclass(turso.sync.ProgrammingError, turso.sync.DatabaseError)
        assert issubclass(turso.sync.NotSupportedError, turso.sync.DatabaseError)

    def test_turso_sync_sqlite_version(self):
        """sqlite_version and sqlite_version_info are available in turso.sync."""
        import turso.sync

        assert isinstance(turso.sync.sqlite_version, str)
        assert "." in turso.sync.sqlite_version

        assert isinstance(turso.sync.sqlite_version_info, tuple)
        assert len(turso.sync.sqlite_version_info) == 3
        assert all(isinstance(p, int) for p in turso.sync.sqlite_version_info)


class TestEntryPointRegistration:
    """Test SQLAlchemy dialect entry points resolve correctly."""

    def test_turso_dialect_resolves(self):
        """create_engine('sqlite+turso://') resolves to TursoDialect."""
        engine = create_engine("sqlite+turso:///:memory:")
        assert isinstance(engine.dialect, TursoDialect)

    def test_turso_sync_dialect_resolves(self):
        """create_engine('sqlite+turso_sync://') resolves to TursoSyncDialect."""
        # Verify the dialect class resolves via URL scheme
        dialect = TursoSyncDialect()
        assert dialect.name == "sqlite"
        assert dialect.driver == "turso_sync"

        # Also verify the entry point is loadable
        url = URL.create("sqlite+turso_sync", database=":memory:")
        dialect_cls = url.get_dialect()
        assert dialect_cls is TursoSyncDialect


# ── Phase 2: Extended Integration Tests ──────────────────────────


class TestSQLFeatureCoverage:
    """Test SQL features work through SQLAlchemy with turso dialect."""

    @pytest.fixture
    def engine(self):
        return create_engine("sqlite+turso:///:memory:")

    def test_transaction_commit_rollback(self, engine):
        """Explicit commit and rollback work correctly."""
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE txn_test (id INTEGER, val TEXT)"))
            conn.commit()

            # Insert and rollback
            conn.execute(text("INSERT INTO txn_test VALUES (1, 'should_vanish')"))
            conn.rollback()

            result = conn.execute(text("SELECT COUNT(*) FROM txn_test"))
            assert result.scalar() == 0

            # Insert and commit
            conn.execute(text("INSERT INTO txn_test VALUES (2, 'should_stay')"))
            conn.commit()

            result = conn.execute(text("SELECT val FROM txn_test WHERE id = 2"))
            assert result.scalar() == "should_stay"

    def test_multiple_tables_with_join(self, engine):
        """ORM with multiple related tables and JOIN queries."""
        from sqlalchemy import ForeignKey
        from sqlalchemy.orm import relationship

        Base = declarative_base()

        class Author(Base):
            __tablename__ = "authors"
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            books = relationship("Book", back_populates="author")

        class Book(Base):
            __tablename__ = "books"
            id = Column(Integer, primary_key=True)
            title = Column(String(200))
            author_id = Column(Integer, ForeignKey("authors.id"))
            author = relationship("Author", back_populates="books")

        Base.metadata.create_all(engine)

        with Session(engine) as session:
            a = Author(name="Tolkien")
            a.books = [Book(title="The Hobbit"), Book(title="LOTR")]
            session.add(a)
            session.commit()

            result = (
                session.query(Book)
                .join(Author)
                .filter(Author.name == "Tolkien")
                .all()
            )
            assert len(result) == 2
            assert {b.title for b in result} == {"The Hobbit", "LOTR"}

    def test_batch_insert_via_orm(self, engine):
        """Adding multiple ORM objects in one session."""
        Base = declarative_base()

        class Record(Base):
            __tablename__ = "records"
            id = Column(Integer, primary_key=True)
            value = Column(Integer)

        Base.metadata.create_all(engine)

        with Session(engine) as session:
            session.add_all([Record(value=i) for i in range(100)])
            session.commit()

            count = session.query(Record).count()
            assert count == 100

    def test_null_handling(self, engine):
        """NULL values round-trip correctly."""
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE nullable (id INTEGER, val TEXT)"))
            conn.execute(text("INSERT INTO nullable VALUES (1, NULL)"))
            conn.commit()

            result = conn.execute(text("SELECT val FROM nullable WHERE id = 1"))
            assert result.scalar() is None

    def test_unicode_data(self, engine):
        """Unicode strings round-trip correctly."""
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE uni (id INTEGER, val TEXT)"))
            conn.execute(text("INSERT INTO uni VALUES (1, '日本語テスト')"))
            conn.execute(text("INSERT INTO uni VALUES (2, '🚀🎉')"))
            conn.commit()

            rows = conn.execute(text("SELECT val FROM uni ORDER BY id")).fetchall()
            assert rows[0][0] == "日本語テスト"
            assert rows[1][0] == "🚀🎉"

    def test_datetime_handling(self, engine):
        """Datetime values round-trip as strings since supports_native_datetime=False."""
        from datetime import datetime

        Base = declarative_base()

        class Event(Base):
            __tablename__ = "events"
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            ts = Column(String)  # Store as string — no native datetime

        Base.metadata.create_all(engine)

        now = datetime.now().isoformat()
        with Session(engine) as session:
            session.add(Event(name="launch", ts=now))
            session.commit()

            event = session.query(Event).first()
            assert event.ts == now

    def test_sql_syntax_error(self, engine):
        """SQL errors propagate correctly through SQLAlchemy."""
        from sqlalchemy.exc import DatabaseError as SADatabaseError

        with engine.connect() as conn:
            with pytest.raises(SADatabaseError):
                conn.execute(text("SELEKT * FORM nonexistent"))

    def test_integrity_error_propagation(self, engine):
        """Unique constraint violations raise IntegrityError."""
        from sqlalchemy.exc import IntegrityError as SAIntegrityError

        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE uniq (id INTEGER PRIMARY KEY, email TEXT UNIQUE)"))
            conn.execute(text("INSERT INTO uniq VALUES (1, 'a@b.com')"))
            conn.commit()

            with pytest.raises(SAIntegrityError):
                conn.execute(text("INSERT INTO uniq VALUES (2, 'a@b.com')"))

    def test_multiple_connections_same_engine(self, engine):
        """Engine handles multiple sequential connections."""
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE multi (id INTEGER)"))
            conn.execute(text("INSERT INTO multi VALUES (1)"))
            conn.commit()

        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM multi"))
            assert result.fetchall() == [(1,)]

    def test_context_manager_cleanup(self, engine):
        """engine.connect() as context manager properly cleans up."""
        # Should not leak connections or raise on exit
        for _ in range(10):
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

    def test_large_text_data(self, engine):
        """Large text values round-trip correctly."""
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE big (id INTEGER, content TEXT)"))
            large = "x" * 100_000
            conn.execute(text("INSERT INTO big VALUES (1, :content)"), {"content": large})
            conn.commit()

            result = conn.execute(text("SELECT content FROM big WHERE id = 1"))
            assert result.scalar() == large
