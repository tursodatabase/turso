"""SQLAlchemy integration tests for TursoServerlessDialect against a real libsql-server.

Requires TURSO_DATABASE_URL env var (default: http://localhost:8080).
Start a server with:
    docker run -d -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest
"""

from __future__ import annotations

import os

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine, text  # noqa: E402
from sqlalchemy.engine import URL  # noqa: E402
from sqlalchemy.orm import Session, declarative_base, relationship  # noqa: E402

SERVER_URL = os.environ.get("TURSO_DATABASE_URL", "http://localhost:8080")
AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")


def try_connect():
    """Check if the server is reachable."""
    try:
        from turso_serverless import connect

        kwargs = {}
        if AUTH_TOKEN:
            kwargs["auth_token"] = AUTH_TOKEN
        conn = connect(SERVER_URL, **kwargs)
        conn.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not try_connect(),
    reason=f"libsql-server not reachable at {SERVER_URL}",
)


def make_engine(**kwargs):
    connect_args = {}
    if AUTH_TOKEN:
        connect_args["auth_token"] = AUTH_TOKEN
    if connect_args:
        kwargs["connect_args"] = connect_args
    return create_engine(f"sqlite+turso_serverless:///{SERVER_URL}", **kwargs)


# ---------------------------------------------------------------------------
# Dialect import/attributes
# ---------------------------------------------------------------------------


class TestDialectImport:
    def test_import_dbapi(self):
        from turso.sqlalchemy import TursoServerlessDialect

        dbapi = TursoServerlessDialect.import_dbapi()
        assert hasattr(dbapi, "connect")
        assert hasattr(dbapi, "Connection")
        assert hasattr(dbapi, "Cursor")
        assert dbapi.apilevel == "2.0"

    def test_dialect_attributes(self):
        from turso.sqlalchemy import TursoServerlessDialect

        assert TursoServerlessDialect.name == "sqlite"
        assert TursoServerlessDialect.driver == "turso_serverless"


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------


class TestURLParsing:
    def test_basic_url(self):
        from turso.sqlalchemy import TursoServerlessDialect

        dialect = TursoServerlessDialect()
        url = URL.create("sqlite+turso_serverless", database="http://localhost:8080")
        args, kwargs = dialect.create_connect_args(url)
        assert args == ["http://localhost:8080"]
        assert kwargs == {}

    def test_url_with_auth_token(self):
        from turso.sqlalchemy import TursoServerlessDialect

        dialect = TursoServerlessDialect()
        url = URL.create(
            "sqlite+turso_serverless",
            database="turso://my-db.turso.io",
            query={"auth_token": "secret"},
        )
        args, kwargs = dialect.create_connect_args(url)
        assert args == ["turso://my-db.turso.io"]
        assert kwargs["auth_token"] == "secret"

    def test_autocommit_isolation(self):
        from turso.sqlalchemy import TursoServerlessDialect

        dialect = TursoServerlessDialect()
        url = URL.create(
            "sqlite+turso_serverless",
            database="http://localhost:8080",
            query={"isolation_level": "AUTOCOMMIT"},
        )
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["isolation_level"] is None


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


class TestCRUD:
    def test_basic_crud(self):
        engine = make_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_test"))
            conn.execute(text("CREATE TABLE sa_test (id INTEGER, name TEXT)"))
            conn.execute(text("INSERT INTO sa_test VALUES (1, 'alice')"))
            conn.commit()

            result = conn.execute(text("SELECT * FROM sa_test"))
            rows = result.fetchall()
            assert rows == [(1, "alice")]

    def test_parameterized_query(self):
        engine = make_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_param"))
            conn.execute(text("CREATE TABLE sa_param (id INTEGER, name TEXT)"))
            conn.execute(
                text("INSERT INTO sa_param VALUES (:id, :name)"),
                {"id": 1, "name": "bob"},
            )
            conn.commit()

            result = conn.execute(text("SELECT name FROM sa_param WHERE id = :id"), {"id": 1})
            assert result.fetchone() == ("bob",)


# ---------------------------------------------------------------------------
# ORM
# ---------------------------------------------------------------------------


class TestORM:
    def test_orm_crud(self):
        Base = declarative_base()

        class Item(Base):
            __tablename__ = "sa_items"
            id = Column(Integer, primary_key=True)
            name = Column(String(100))

        engine = make_engine()

        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_items"))
            conn.commit()

        Base.metadata.create_all(engine)

        with Session(engine) as session:
            session.add(Item(name="Widget"))
            session.commit()

            items = session.query(Item).all()
            assert len(items) == 1
            assert items[0].name == "Widget"


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


class TestTransactions:
    def test_commit(self):
        engine = make_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_tx"))
            conn.execute(text("CREATE TABLE sa_tx (id INTEGER, val TEXT)"))
            conn.commit()

            conn.execute(text("INSERT INTO sa_tx VALUES (1, 'committed')"))
            conn.commit()

            result = conn.execute(text("SELECT val FROM sa_tx WHERE id = 1"))
            assert result.scalar() == "committed"

    def test_rollback(self):
        engine = make_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_tx_rb"))
            conn.execute(text("CREATE TABLE sa_tx_rb (id INTEGER, val TEXT)"))
            conn.commit()

            conn.execute(text("INSERT INTO sa_tx_rb VALUES (1, 'should_vanish')"))
            conn.rollback()

            result = conn.execute(text("SELECT COUNT(*) FROM sa_tx_rb"))
            assert result.scalar() == 0


# ---------------------------------------------------------------------------
# Joins
# ---------------------------------------------------------------------------


class TestJoins:
    def test_multi_table_join(self):
        Base = declarative_base()

        class Author(Base):
            __tablename__ = "sa_authors"
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            books = relationship("Book", back_populates="author")

        class Book(Base):
            __tablename__ = "sa_books"
            id = Column(Integer, primary_key=True)
            title = Column(String(200))
            author_id = Column(Integer, ForeignKey("sa_authors.id"))
            author = relationship("Author", back_populates="books")

        engine = make_engine()

        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_books"))
            conn.execute(text("DROP TABLE IF EXISTS sa_authors"))
            conn.commit()

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


# ---------------------------------------------------------------------------
# Unicode/NULL
# ---------------------------------------------------------------------------


class TestUnicodeNull:
    def test_unicode_roundtrip(self):
        engine = make_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_uni"))
            conn.execute(text("CREATE TABLE sa_uni (id INTEGER, val TEXT)"))
            conn.execute(text("INSERT INTO sa_uni VALUES (1, '日本語テスト')"))
            conn.commit()

            result = conn.execute(text("SELECT val FROM sa_uni WHERE id = 1"))
            assert result.scalar() == "日本語テスト"

    def test_null_roundtrip(self):
        engine = make_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_null"))
            conn.execute(text("CREATE TABLE sa_null (id INTEGER, val TEXT)"))
            conn.execute(text("INSERT INTO sa_null VALUES (1, NULL)"))
            conn.commit()

            result = conn.execute(text("SELECT val FROM sa_null WHERE id = 1"))
            assert result.scalar() is None


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    def test_sql_error(self):
        from sqlalchemy.exc import DatabaseError as SADatabaseError

        engine = make_engine()
        with engine.connect() as conn:
            with pytest.raises(SADatabaseError):
                conn.execute(text("SELEKT * FORM nonexistent"))

    def test_integrity_error(self):
        from sqlalchemy.exc import IntegrityError as SAIntegrityError

        engine = make_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_uniq"))
            conn.execute(text("CREATE TABLE sa_uniq (id INTEGER PRIMARY KEY, email TEXT UNIQUE)"))
            conn.execute(text("INSERT INTO sa_uniq VALUES (1, 'a@b.com')"))
            conn.commit()

            with pytest.raises(SAIntegrityError):
                conn.execute(text("INSERT INTO sa_uniq VALUES (2, 'a@b.com')"))


# ---------------------------------------------------------------------------
# Table reflection
# ---------------------------------------------------------------------------


class TestTableReflection:
    def test_reflection_returns_empty(self):
        engine = make_engine()
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_reflect"))
            conn.execute(text("CREATE TABLE sa_reflect (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.commit()

        from sqlalchemy import inspect

        inspector = inspect(engine)

        fks = inspector.get_foreign_keys("sa_reflect")
        assert fks == []

        indexes = inspector.get_indexes("sa_reflect")
        assert indexes == []

        ucs = inspector.get_unique_constraints("sa_reflect")
        assert ucs == []


# ---------------------------------------------------------------------------
# Dialect methods
# ---------------------------------------------------------------------------


class TestDialectMethods:
    def test_on_connect_returns_none(self):
        from turso.sqlalchemy import TursoServerlessDialect

        dialect = TursoServerlessDialect()
        assert dialect.on_connect() is None

    def test_get_isolation_level(self):
        from turso.sqlalchemy import TursoServerlessDialect

        dialect = TursoServerlessDialect()
        assert dialect.get_isolation_level(None) == "SERIALIZABLE"

    def test_supports_statement_cache(self):
        from turso.sqlalchemy import TursoServerlessDialect

        assert TursoServerlessDialect.supports_statement_cache is True
