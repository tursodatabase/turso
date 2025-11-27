from typing import Any, Callable, Iterator, Optional, Sequence, Tuple, Union
from collections.abc import Iterable
import logging
import re

from ._turso import (
    PyTursoStatusCode,
    PyTursoSetupConfig,
    PyTursoDatabaseConfig,
    PyTursoStatement,
    py_turso_setup,
    py_turso_database_open,
    Busy,
    Interrupt,
    Error,
    Misuse,
    Constraint,
    Readonly,
    DatabaseFull,
    NotAdb,
    Corrupt,
)


# DB-API 2.0 module attributes
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

# DB-API 2.0 transaction control constants
LEGACY_TRANSACTION_CONTROL = "legacy"


# Exception hierarchy following DB-API 2.0
class Warning(Exception):
    """Exception raised for important warnings."""

    pass


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""

    pass


class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface."""

    pass


class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the processed data."""

    pass


class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's operation."""

    pass


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected."""

    pass


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error."""

    pass


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors."""

    pass


class NotSupportedError(DatabaseError):
    """Exception raised when a method or database API is not supported."""

    pass


def _map_turso_exception(exc: Exception) -> Exception:
    """Maps Turso-specific exceptions to DB-API 2.0 exception hierarchy"""
    if isinstance(exc, Busy):
        return OperationalError(str(exc))
    elif isinstance(exc, Interrupt):
        return OperationalError(str(exc))
    elif isinstance(exc, Misuse):
        return ProgrammingError(str(exc))
    elif isinstance(exc, Constraint):
        return IntegrityError(str(exc))
    elif isinstance(exc, Readonly):
        return OperationalError(str(exc))
    elif isinstance(exc, DatabaseFull):
        return OperationalError(str(exc))
    elif isinstance(exc, NotAdb):
        return DatabaseError(str(exc))
    elif isinstance(exc, Corrupt):
        return DatabaseError(str(exc))
    elif isinstance(exc, Error):
        return DatabaseError(str(exc))
    return exc


# DML statement detection pattern
_DML_PATTERN = re.compile(r"^\s*(INSERT|UPDATE|DELETE|REPLACE)\s", re.IGNORECASE | re.MULTILINE)


def _is_dml_statement(sql: str) -> bool:
    """Check if SQL statement is a DML statement that requires transaction handling."""
    return bool(_DML_PATTERN.match(sql.strip()))


class Connection:
    """
    A Connection object represents a connection to a Turso database.

    Connection objects are created using the connect() function.
    They are used to create Cursor objects and manage transactions.
    """

    def __init__(
        self,
        database: str,
        *,
        experimental_features: Optional[str] = None,
        async_io: bool = False,
        autocommit: Union[bool, str] = LEGACY_TRANSACTION_CONTROL,
        isolation_level: Optional[str] = "",
    ):
        """Initialize a connection to a Turso database."""
        self._database_path = database
        self._experimental_features = experimental_features
        self._async_io = async_io
        self._autocommit = autocommit
        self._isolation_level = isolation_level
        self._closed = False

        # Open database and create connection
        config = PyTursoDatabaseConfig(path=database, experimental_features=experimental_features, async_io=async_io)
        try:
            self._db = py_turso_database_open(config)
            self._conn = self._db.connect()
        except Exception as e:
            raise _map_turso_exception(e)

        # Track transaction state for legacy mode
        self._in_transaction = False

    def cursor(self, factory: type = None) -> "Cursor":
        """
        Create and return a Cursor object.

        Args:
            factory: Optional cursor factory. If supplied, this must be a callable
                    returning an instance of Cursor or its subclasses.

        Returns:
            A new Cursor object.
        """
        self._check_closed()
        if factory is None:
            return Cursor(self)
        return factory(self)

    def commit(self) -> None:
        """
        Commit any pending transaction to the database.

        If autocommit is True or LEGACY_TRANSACTION_CONTROL with no open transaction,
        this method does nothing.
        """
        self._check_closed()

        # Handle autocommit mode
        if self._autocommit is True:
            return

        # Handle legacy transaction control
        if self._autocommit == LEGACY_TRANSACTION_CONTROL:
            if not self._in_transaction:
                return
            try:
                stmt = self._conn.prepare_single("COMMIT")
                result = stmt.execute()
                self._handle_io_loop(stmt, result.status)
                stmt.finalize()
                self._in_transaction = False
            except Exception as e:
                raise _map_turso_exception(e)
        else:
            # PEP 249 compliant mode (autocommit=False)
            try:
                stmt = self._conn.prepare_single("COMMIT")
                result = stmt.execute()
                self._handle_io_loop(stmt, result.status)
                stmt.finalize()
                # Implicitly start a new transaction
                self._begin_transaction()
            except Exception as e:
                raise _map_turso_exception(e)

    def rollback(self) -> None:
        """
        Roll back to the start of any pending transaction.

        If autocommit is True or LEGACY_TRANSACTION_CONTROL with no open transaction,
        this method does nothing.
        """
        self._check_closed()

        # Handle autocommit mode
        if self._autocommit is True:
            return

        # Handle legacy transaction control
        if self._autocommit == LEGACY_TRANSACTION_CONTROL:
            if not self._in_transaction:
                return
            try:
                stmt = self._conn.prepare_single("ROLLBACK")
                result = stmt.execute()
                self._handle_io_loop(stmt, result.status)
                stmt.finalize()
                self._in_transaction = False
            except Exception as e:
                raise _map_turso_exception(e)
        else:
            # PEP 249 compliant mode (autocommit=False)
            try:
                stmt = self._conn.prepare_single("ROLLBACK")
                result = stmt.execute()
                self._handle_io_loop(stmt, result.status)
                stmt.finalize()
                # Implicitly start a new transaction
                self._begin_transaction()
            except Exception as e:
                raise _map_turso_exception(e)

    def close(self) -> None:
        """
        Close the database connection.

        If autocommit is False, any pending transaction is implicitly rolled back.
        """
        if self._closed:
            return

        # Roll back pending transaction if in PEP 249 mode
        if self._autocommit is False:
            try:
                self.rollback()
            except Exception:
                pass

        try:
            self._conn.close()
        except Exception as e:
            raise _map_turso_exception(e)
        finally:
            self._closed = True

    def execute(self, sql: str, parameters: Union[Sequence, dict] = ()) -> "Cursor":
        """
        Create a new Cursor object and call execute() on it with the given sql and parameters.

        Returns:
            The new cursor object.
        """
        cursor = self.cursor()
        cursor.execute(sql, parameters)
        return cursor

    def executemany(self, sql: str, parameters: Iterable) -> "Cursor":
        """
        Create a new Cursor object and call executemany() on it with the given sql and parameters.

        Returns:
            The new cursor object.
        """
        cursor = self.cursor()
        cursor.executemany(sql, parameters)
        return cursor

    def executescript(self, sql_script: str) -> "Cursor":
        """
        Create a new Cursor object and call executescript() on it with the given sql_script.

        Returns:
            The new cursor object.
        """
        cursor = self.cursor()
        cursor.executescript(sql_script)
        return cursor

    def _check_closed(self) -> None:
        """Check if connection is closed and raise exception if it is."""
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed connection")

    def _begin_transaction(self) -> None:
        """Begin a new transaction with the appropriate isolation level."""
        if self._isolation_level is None:
            return

        # Determine BEGIN statement based on isolation level
        if self._isolation_level == "" or self._isolation_level == "DEFERRED":
            begin_sql = "BEGIN DEFERRED"
        elif self._isolation_level == "IMMEDIATE":
            begin_sql = "BEGIN IMMEDIATE"
        elif self._isolation_level == "EXCLUSIVE":
            begin_sql = "BEGIN EXCLUSIVE"
        else:
            begin_sql = "BEGIN DEFERRED"

        try:
            stmt = self._conn.prepare_single(begin_sql)
            result = stmt.execute()
            self._handle_io_loop(stmt, result.status)
            stmt.finalize()
            if self._autocommit == LEGACY_TRANSACTION_CONTROL:
                self._in_transaction = True
        except Exception as e:
            raise _map_turso_exception(e)

    def _handle_io_loop(self, stmt: PyTursoStatement, status: PyTursoStatusCode) -> None:
        """Handle async IO loop if needed."""
        if not self._async_io:
            return

        while status == PyTursoStatusCode.Io:
            stmt.run_io()
            # For execute, we need to call execute again to get updated status
            # But since we're in execute path, status should not be Io unless async_io is true
            # The Rust layer handles this, so we just need to run_io
            break

    @property
    def autocommit(self) -> Union[bool, str]:
        """Get or set autocommit mode."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: Union[bool, str]) -> None:
        """Set autocommit mode."""
        self._check_closed()

        old_value = self._autocommit
        self._autocommit = value

        # Handle transition from False to True: commit pending transaction
        if old_value is False and value is True:
            if not self._conn.get_auto_commit():
                self.commit()

        # Handle transition to False: start new transaction
        if value is False:
            if self._conn.get_auto_commit():
                self._begin_transaction()

    @property
    def in_transaction(self) -> bool:
        """
        Returns True if a transaction is active (uncommitted changes exist).

        This corresponds to SQLite's autocommit mode being off.
        """
        self._check_closed()
        return not self._conn.get_auto_commit()

    @property
    def isolation_level(self) -> Optional[str]:
        """Get or set the isolation level for legacy transaction control."""
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, value: Optional[str]) -> None:
        """Set the isolation level for legacy transaction control."""
        self._check_closed()
        self._isolation_level = value

    @property
    def row_factory(self) -> Optional[Callable]:
        """Get or set the initial row_factory for Cursor objects created from this connection."""
        return getattr(self, "_row_factory", None)

    @row_factory.setter
    def row_factory(self, value: Optional[Callable]) -> None:
        """Set the initial row_factory for Cursor objects created from this connection."""
        self._row_factory = value

    @property
    def total_changes(self) -> int:
        """
        Return the total number of database rows that have been modified, inserted,
        or deleted since the database connection was opened.
        """
        self._check_closed()
        # This would require additional Rust API support
        # For now, return 0 as a placeholder
        return 0

    def __enter__(self) -> "Connection":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - commits or rolls back based on exception."""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()


class Cursor:
    """
    A Cursor object represents a database cursor which is used to execute
    SQL statements and manage the context of a fetch operation.

    Cursors are created using Connection.cursor().
    """

    def __init__(self, connection: Connection):
        """Initialize a cursor with the given connection."""
        self._connection = connection
        self._row_factory = getattr(connection, "_row_factory", None)
        self._arraysize = 1
        self._description = None
        self._rowcount = -1
        self._lastrowid = None
        self._current_stmt = None
        self._current_rows = []
        self._current_row_index = 0
        self._closed = False

    def execute(self, sql: str, parameters: Union[Sequence, dict] = ()) -> "Cursor":
        """
        Execute a single SQL statement, optionally binding Python values using placeholders.

        Args:
            sql: A single SQL statement.
            parameters: Python values to bind to placeholders in sql.

        Returns:
            self

        Raises:
            ProgrammingError: When sql contains more than one SQL statement,
                            or when parameter binding fails.
        """
        self._check_closed()
        self._check_connection_closed()

        # Reset state
        self._description = None
        self._rowcount = -1
        self._lastrowid = None
        self._current_rows = []
        self._current_row_index = 0

        # Clean up previous statement if any
        if self._current_stmt is not None:
            try:
                self._current_stmt.finalize()
            except Exception:
                pass
            self._current_stmt = None

        # Handle legacy transaction control for DML statements
        if (
            self._connection._autocommit == LEGACY_TRANSACTION_CONTROL
            and self._connection._isolation_level is not None
            and _is_dml_statement(sql)
            and not self._connection._in_transaction
        ):
            self._connection._begin_transaction()

        try:
            # Prepare statement
            self._current_stmt = self._connection._conn.prepare_single(sql)

            # Bind parameters
            if parameters:
                self._bind_parameters(self._current_stmt, parameters)

            # Get column information
            columns = self._current_stmt.columns()
            if columns:
                self._description = tuple((name, None, None, None, None, None, None) for name in columns)

            # Execute and fetch all rows for SELECT statements
            if self._description:
                self._fetch_all_rows()
            else:
                # For non-SELECT statements, just execute
                result = self._current_stmt.execute()
                self._connection._handle_io_loop(self._current_stmt, result.status)
                self._rowcount = int(result.rows_changed)

                # Update lastrowid for INSERT/REPLACE statements
                if _DML_PATTERN.match(sql.strip()) and self._rowcount > 0:
                    # Note: lastrowid tracking would require additional Rust API
                    pass

        except Exception as e:
            if self._current_stmt is not None:
                try:
                    self._current_stmt.finalize()
                except Exception:
                    pass
                self._current_stmt = None
            raise _map_turso_exception(e)

        return self

    def executemany(self, sql: str, parameters: Iterable) -> "Cursor":
        """
        For every item in parameters, repeatedly execute the parameterized DML SQL statement.

        Args:
            sql: A single SQL DML statement.
            parameters: An iterable of parameters to bind with the placeholders in sql.

        Returns:
            self

        Raises:
            ProgrammingError: When sql contains more than one SQL statement or is not a DML statement.
        """
        self._check_closed()
        self._check_connection_closed()

        # Reset state
        self._description = None
        self._rowcount = 0
        self._lastrowid = None

        # Verify DML statement
        if not _is_dml_statement(sql):
            # For compatibility, we'll still execute but won't enforce DML-only
            pass

        try:
            for param_set in parameters:
                # Prepare statement for each parameter set
                stmt = self._connection._conn.prepare_single(sql)

                # Bind parameters
                self._bind_parameters(stmt, param_set)

                # Execute without fetching rows
                result = stmt.execute()
                self._connection._handle_io_loop(stmt, result.status)
                self._rowcount += int(result.rows_changed)

                # Finalize statement
                stmt.finalize()

        except Exception as e:
            raise _map_turso_exception(e)

        return self

    def executescript(self, sql_script: str) -> "Cursor":
        """
        Execute the SQL statements in sql_script.

        If autocommit is LEGACY_TRANSACTION_CONTROL and there is a pending transaction,
        an implicit COMMIT is executed first.

        Args:
            sql_script: A string containing one or more SQL statements.

        Returns:
            self
        """
        self._check_closed()
        self._check_connection_closed()

        # Reset state
        self._description = None
        self._rowcount = -1
        self._lastrowid = None

        # Commit pending transaction in legacy mode
        if self._connection._autocommit == LEGACY_TRANSACTION_CONTROL and self._connection._in_transaction:
            self._connection.commit()

        try:
            # Process all statements in the script
            remaining_sql = sql_script

            while remaining_sql.strip():
                result = self._connection._conn.prepare_first(remaining_sql)
                if result is None:
                    break

                stmt, tail_idx = result

                # Execute statement
                exec_result = stmt.execute()
                self._connection._handle_io_loop(stmt, exec_result.status)
                stmt.finalize()

                # Move to next statement
                remaining_sql = remaining_sql[tail_idx:]

        except Exception as e:
            raise _map_turso_exception(e)

        return self

    def fetchone(self) -> Optional[Any]:
        """
        Fetch the next row of a query result set.

        Returns:
            A single row or None when no more data is available.
        """
        self._check_closed()

        if self._current_row_index >= len(self._current_rows):
            return None

        row = self._current_rows[self._current_row_index]
        self._current_row_index += 1

        return self._make_row(row)

    def fetchmany(self, size: int = None) -> list:
        """
        Fetch the next set of rows of a query result.

        Args:
            size: Number of rows to fetch. If not given, arraysize determines the number.

        Returns:
            A list of rows. An empty list is returned when no more rows are available.
        """
        self._check_closed()

        if size is None:
            size = self._arraysize

        if size < 0:
            raise ValueError("size must be non-negative")

        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def fetchall(self) -> list:
        """
        Fetch all (remaining) rows of a query result.

        Returns:
            A list of rows. An empty list is returned when no rows are available.
        """
        self._check_closed()

        result = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def close(self) -> None:
        """Close the cursor now."""
        if self._closed:
            return

        # Clean up current statement
        if self._current_stmt is not None:
            try:
                self._current_stmt.finalize()
            except Exception:
                pass
            self._current_stmt = None

        self._closed = True

    def setinputsizes(self, sizes: Sequence) -> None:
        """Required by DB-API. Does nothing in turso."""
        pass

    def setoutputsize(self, size: int, column: int = None) -> None:
        """Required by DB-API. Does nothing in turso."""
        pass

    @property
    def arraysize(self) -> int:
        """Get or set the number of rows returned by fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the number of rows returned by fetchmany()."""
        if value < 0:
            raise ValueError("arraysize must be non-negative")
        self._arraysize = value

    @property
    def connection(self) -> Connection:
        """The Connection object this cursor belongs to."""
        return self._connection

    @property
    def description(self) -> Optional[Tuple]:
        """
        Column names and type information for the last query.

        Returns a 7-tuple for each column where the last six items are None.
        """
        return self._description

    @property
    def lastrowid(self) -> Optional[int]:
        """The row id of the last inserted row."""
        return self._lastrowid

    @property
    def rowcount(self) -> int:
        """
        The number of modified rows for INSERT, UPDATE, DELETE, and REPLACE statements.

        Returns -1 for other statements.
        """
        return self._rowcount

    @property
    def row_factory(self) -> Optional[Callable]:
        """Get or set how a row fetched from this Cursor is represented."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: Optional[Callable]) -> None:
        """Set how a row fetched from this Cursor is represented."""
        self._row_factory = value

    def _check_closed(self) -> None:
        """Check if cursor is closed and raise exception if it is."""
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed cursor")

    def _check_connection_closed(self) -> None:
        """Check if connection is closed and raise exception if it is."""
        if self._connection._closed:
            raise ProgrammingError("Cannot operate on a closed connection")

    def _bind_parameters(self, stmt: PyTursoStatement, parameters: Union[Sequence, dict]) -> None:
        """Bind parameters to a prepared statement."""
        if isinstance(parameters, dict):
            # Named parameters not directly supported by current API
            # Would need to convert to positional or extend Rust API
            raise ProgrammingError("Named parameters are not yet supported")
        else:
            # Positional parameters
            stmt.bind(tuple(parameters))

    def _fetch_all_rows(self) -> None:
        """Fetch all rows from the current statement."""
        self._current_rows = []
        self._current_row_index = 0

        try:
            while True:
                status = self._current_stmt.step()
                self._connection._handle_io_loop(self._current_stmt, status)

                if status == PyTursoStatusCode.Row:
                    row = self._current_stmt.row()
                    self._current_rows.append(row)
                elif status == PyTursoStatusCode.Done:
                    break
                else:
                    # Unexpected status
                    break

            # Update rowcount for SELECT statements
            self._rowcount = len(self._current_rows)

        except Exception as e:
            raise _map_turso_exception(e)

    def _make_row(self, row_tuple: tuple) -> Any:
        """Apply row_factory to create row object."""
        if self._row_factory is None:
            return row_tuple
        return self._row_factory(self, row_tuple)

    def __iter__(self) -> Iterator:
        """Make cursor an iterator."""
        return self

    def __next__(self) -> Any:
        """Fetch next row for iterator protocol."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class Row:
    """
    A Row instance serves as a highly optimized row_factory for Connection objects.

    It supports iteration, equality testing, len(), and mapping access by column name and index.
    """

    def __init__(self, cursor: Cursor, data: tuple):
        """Initialize a Row with cursor and data."""
        self._data = data
        self._description = cursor.description
        if self._description:
            self._name_map = {desc[0]: idx for idx, desc in enumerate(self._description)}
        else:
            self._name_map = {}

    def keys(self) -> list:
        """Return a list of column names as strings."""
        if self._description:
            return [desc[0] for desc in self._description]
        return []

    def __getitem__(self, key: Union[int, str]) -> Any:
        """Get item by column index or name."""
        if isinstance(key, int):
            return self._data[key]
        elif isinstance(key, str):
            if key in self._name_map:
                return self._data[self._name_map[key]]
            raise KeyError(f"No column named {key}")
        elif isinstance(key, slice):
            return self._data[key]
        else:
            raise TypeError(f"indices must be integers or strings, not {type(key).__name__}")

    def __len__(self) -> int:
        """Return the number of columns."""
        return len(self._data)

    def __iter__(self) -> Iterator:
        """Iterate over row values."""
        return iter(self._data)

    def __eq__(self, other: Any) -> bool:
        """Compare rows for equality."""
        if not isinstance(other, Row):
            return NotImplemented
        return self._data == other._data and self.keys() == other.keys()

    def __hash__(self) -> int:
        """Return hash of row."""
        return hash((tuple(self.keys()), self._data))

    def __repr__(self) -> str:
        """Return string representation of row."""
        return f"<turso.Row {self._data}>"


def connect(
    database: str,
    *,
    experimental_features: Optional[str] = None,
    async_io: bool = False,
    autocommit: Union[bool, str] = LEGACY_TRANSACTION_CONTROL,
    isolation_level: Optional[str] = "",
) -> Connection:
    """
    Open a connection to a Turso database.

    Args:
        database: Path to the database file.
        experimental_features: Comma-separated list of experimental features to enable.
        async_io: If True, use async IO mode.
        autocommit: Transaction control mode. Can be True, False, or LEGACY_TRANSACTION_CONTROL.
        isolation_level: Transaction isolation level for legacy mode ("DEFERRED", "IMMEDIATE", "EXCLUSIVE", or None).

    Returns:
        A Connection object.
    """
    return Connection(
        database=database,
        experimental_features=experimental_features,
        async_io=async_io,
        autocommit=autocommit,
        isolation_level=isolation_level,
    )


def setup_logging(level: int = logging.INFO) -> None:
    """
    Setup logging for the turso driver.

    Args:
        level: Logging level (e.g., logging.INFO, logging.DEBUG).
    """
    logger = logging.getLogger("turso")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def log_callback(log):
        """Callback to handle Turso logs."""
        turso_logger = logging.getLogger(f"turso.{log.target}")

        level_map = {
            "ERROR": logging.ERROR,
            "WARN": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
            "TRACE": logging.DEBUG,
        }

        log_level = level_map.get(log.level.upper(), logging.INFO)
        turso_logger.log(log_level, f"{log.message} ({log.file}:{log.line})")

    try:
        config = PyTursoSetupConfig(logger=log_callback, log_level=logging.getLevelName(level))
        py_turso_setup(config)
    except Exception as e:
        logger.warning(f"Failed to setup Turso logging: {e}")
