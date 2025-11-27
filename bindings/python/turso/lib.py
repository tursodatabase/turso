```python
from typing import Optional, Any, Callable, Tuple, List, Dict, Union, Iterator
from enum import IntEnum
from ._turso import (
    PyTursoStatusCode,
    PyTursoDatabase,
    PyTursoConnection as _PyTursoConnection,
    PyTursoStatement as _PyTursoStatement,
    PyTursoDatabaseConfig,
    py_turso_database_open,
    Error,
    Busy,
    Interrupt,
    Misuse,
    Constraint,
    Readonly,
    DatabaseFull,
    NotAdb,
    Corrupt,
)


PARSE_DECLTYPES = 0
PARSE_COLNAMES = 0

paramstyle = "qmark"

apilevel = "2.0"
threadsafety = 1

LEGACY_TRANSACTION_CONTROL = -1


class Row:
    """Represents a single row from a database query result"""
    
    def __init__(self, cursor: 'Cursor', values: Tuple[Any, ...]):
        self._cursor = cursor
        self._values = values
        self._keys = cursor._column_names if cursor._column_names else []
    
    def keys(self) -> List[str]:
        """Return column names"""
        return list(self._keys)
    
    def __getitem__(self, key: Union[int, str]) -> Any:
        """Access by index or column name"""
        if isinstance(key, int):
            return self._values[key]
        elif isinstance(key, str):
            for i, name in enumerate(self._keys):
                if name.lower() == key.lower():
                    return self._values[i]
            raise KeyError(f"No such column: {key}")
        else:
            raise TypeError(f"indices must be integers or strings, not {type(key).__name__}")
    
    def __len__(self) -> int:
        return len(self._values)
    
    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)
    
    def __repr__(self) -> str:
        return f"<turso.Row {self._values}>"


class Cursor:
    """Database cursor for executing queries and fetching results"""
    
    def __init__(self, connection: 'Connection'):
        self.connection = connection
        self._statement: Optional[_PyTursoStatement] = None
        self._column_names: Optional[Tuple[str, ...]] = None
        self._current_row: Optional[Tuple[Any, ...]] = None
        self._exhausted = False
        self.arraysize = 1
        self.rowcount = -1
        self.lastrowid: Optional[int] = None
        self._closed = False
        self.row_factory: Optional[Callable] = None
    
    @property
    def description(self) -> Optional[List[Tuple[str, None, None, None, None, None, None]]]:
        """Return column descriptions as 7-tuples (only name is populated)"""
        if self._column_names is None:
            return None
        return [(name, None, None, None, None, None, None) for name in self._column_names]
    
    def _check_closed(self):
        """Check if cursor or connection is closed"""
        if self._closed:
            raise Misuse("Cannot operate on a closed cursor")
        if self.connection._closed:
            raise Misuse("Cannot operate on a closed database")
    
    def _bind_parameters(self, statement: _PyTursoStatement, parameters: Any):
        """Bind parameters to statement (positional or named)"""
        if not parameters:
            return
        
        if isinstance(parameters, dict):
            raise Misuse("Named parameters are not yet supported")
        elif isinstance(parameters, (tuple, list)):
            statement.bind(tuple(parameters))
        else:
            raise Misuse("Parameters must be a sequence or dict")
    
    def _is_dml(self, sql: str) -> bool:
        """Check if SQL is a DML statement (INSERT, UPDATE, DELETE, REPLACE)"""
        sql_upper = sql.strip().upper()
        return any(sql_upper.startswith(stmt) for stmt in ('INSERT', 'UPDATE', 'DELETE', 'REPLACE'))
    
    def _ensure_transaction(self, sql: str):
        """Ensure transaction is started for DML in legacy mode"""
        if (self.connection.autocommit == LEGACY_TRANSACTION_CONTROL and 
            self.connection.isolation_level is not None and
            self._is_dml(sql) and
            not self.connection.in_transaction):
            self.connection._begin_transaction()
    
    def execute(self, sql: str, parameters: Any = ()) -> 'Cursor':
        """Execute a SQL query with optional parameters"""
        self._check_closed()
        
        self._ensure_transaction(sql)
        
        if self._statement:
            self._statement.finalize()
            self._statement = None
        
        self._column_names = None
        self._current_row = None
        self._exhausted = False
        self.rowcount = -1
        
        self._statement = self.connection._connection.prepare_single(sql)
        
        self._bind_parameters(self._statement, parameters)
        
        status = self._statement.step()
        
        if status == PyTursoStatusCode.Row:
            self._column_names = self._statement.columns()
            self._current_row = self._statement.row()
        elif status == PyTursoStatusCode.Done:
            self._exhausted = True
            self._column_names = self._statement.columns()
        elif status == PyTursoStatusCode.Io:
            while status == PyTursoStatusCode.Io:
                self._statement.run_io()
                status = self._statement.step()
            
            if status == PyTursoStatusCode.Row:
                self._column_names = self._statement.columns()
                self._current_row = self._statement.row()
            elif status == PyTursoStatusCode.Done:
                self._exhausted = True
                self._column_names = self._statement.columns()
        
        if self.connection.autocommit != LEGACY_TRANSACTION_CONTROL:
            self.connection._mark_transaction_active()
        
        return self
    
    def executemany(self, sql: str, seq_of_parameters: List[Any]) -> 'Cursor':
        """Execute SQL query multiple times with different parameters"""
        self._check_closed()
        
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)
            while self.fetchone() is not None:
                pass
        
        return self
    
    def executescript(self, sql_script: str) -> 'Cursor':
        """Execute multiple SQL statements separated by semicolons"""
        self._check_closed()
        
        if self.connection.autocommit == False:
            self.connection.commit()
        
        remaining = sql_script.strip()
        
        while remaining:
            result = self.connection._connection.prepare_first(remaining)
            
            if result is None:
                break
            
            statement, tail_idx = result
            remaining = sql_script[tail_idx:].strip()
            
            status = statement.step()
            while status == PyTursoStatusCode.Row:
                status = statement.step()
            
            while status == PyTursoStatusCode.Io:
                statement.run_io()
                status = statement.step()
            
            statement.finalize()
        
        return self
    
    def fetchone(self) -> Optional[Union[Tuple[Any, ...], Row]]:
        """Fetch next row of query result"""
        self._check_closed()
        
        if self._statement is None:
            raise Misuse("No active statement")
        
        if self._current_row is not None:
            row = self._current_row
            self._current_row = None
            
            status = self._statement.step()
            while status == PyTursoStatusCode.Io:
                self._statement.run_io()
                status = self._statement.step()
            
            if status == PyTursoStatusCode.Row:
                self._current_row = self._statement.row()
            elif status == PyTursoStatusCode.Done:
                self._exhausted = True
            
            row_factory = self.row_factory or self.connection.row_factory
            if row_factory:
                return row_factory(self, row)
            return row
        
        if self._exhausted:
            return None
        
        status = self._statement.step()
        while status == PyTursoStatusCode.Io:
            self._statement.run_io()
            status = self._statement.step()
        
        if status == PyTursoStatusCode.Row:
            row = self._statement.row()
            row_factory = self.row_factory or self.connection.row_factory
            if row_factory:
                return row_factory(self, row)
            return row
        elif status == PyTursoStatusCode.Done:
            self._exhausted = True
            return None
        
        return None
    
    def fetchmany(self, size: Optional[int] = None) -> List[Union[Tuple[Any, ...], Row]]:
        """Fetch multiple rows (up to size)"""
        if size is None:
            size = self.arraysize
        
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        
        return rows
    
    def fetchall(self) -> List[Union[Tuple[Any, ...], Row]]:
        """Fetch all remaining rows"""
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows
    
    def close(self):
        """Close the cursor"""
        if self._statement:
            self._statement.finalize()
            self._statement = None
        self._closed = True
    
    def __iter__(self) -> Iterator[Union[Tuple[Any, ...], Row]]:
        """Make cursor iterable"""
        while True:
            row = self.fetchone()
            if row is None:
                break
            yield row
    
    def __enter__(self) -> 'Cursor':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Connection:
    """Database connection"""
    
    def __init__(
        self, 
        database: PyTursoDatabase,
        autocommit: Union[bool, int] = LEGACY_TRANSACTION_CONTROL,
        isolation_level: Optional[str] = "DEFERRED"
    ):
        self._database = database
        self._connection: _PyTursoConnection = database.connect()
        self._closed = False
        self.autocommit = autocommit
        self.isolation_level = isolation_level
        self.row_factory: Optional[Callable] = None
        self.text_factory = str
        self._in_transaction = False
        
        if autocommit == False:
            self._begin_transaction()
    
    def _mark_transaction_active(self):
        """Mark that we have an active transaction"""
        self._in_transaction = True
    
    @property
    def in_transaction(self) -> bool:
        """Check if transaction is active"""
        if self.autocommit == True:
            return False
        if self.autocommit == False:
            return self._in_transaction
        return not self._connection.get_auto_commit()
    
    @property
    def total_changes(self) -> int:
        """Total number of database rows modified"""
        return 0
    
    def _check_closed(self):
        """Check if connection is closed"""
        if self._closed:
            raise Misuse("Cannot operate on a closed database")
    
    def _begin_transaction(self):
        """Begin a transaction with appropriate isolation level"""
        if self.isolation_level:
            stmt = self._connection.prepare_single(f"BEGIN {self.isolation_level}")
            stmt.execute()
            stmt.finalize()
        else:
            stmt = self._connection.prepare_single("BEGIN")
            stmt.execute()
            stmt.finalize()
        self._in_transaction = True
    
    def cursor(self, factory: type = Cursor) -> Cursor:
        """Create a new cursor"""
        self._check_closed()
        return factory(self)
    
    def commit(self):
        """Commit the current transaction"""
        self._check_closed()
        
        if self.autocommit == True:
            return
        
        if self._in_transaction or (self.autocommit == LEGACY_TRANSACTION_CONTROL and not self._connection.get_auto_commit()):
            stmt = self._connection.prepare_single("COMMIT")
            stmt.execute()
            stmt.finalize()
            self._in_transaction = False
        
        if self.autocommit == False:
            self._begin_transaction()
    
    def rollback(self):
        """Rollback the current transaction"""
        self._check_closed()
        
        if self.autocommit == True:
            return
        
        if self._in_transaction or (self.autocommit == LEGACY_TRANSACTION_CONTROL and not self._connection.get_auto_commit()):
            stmt = self._connection.prepare_single("ROLLBACK")
            stmt.execute()
            stmt.finalize()
            self._in_transaction = False
        
        if self.autocommit == False:
            self._begin_transaction()
    
    def close(self):
        """Close the connection"""
        if not self._closed:
            if self.autocommit == False and self._in_transaction:
                self.rollback()
            self._connection.close()
            self._closed = True
    
    def execute(self, sql: str, parameters: Any = ()) -> Cursor:
        """Shortcut to create cursor and execute"""
        cursor = self.cursor()
        return cursor.execute(sql, parameters)
    
    def executemany(self, sql: str, parameters: List[Any]) -> Cursor:
        """Shortcut to create cursor and executemany"""
        cursor = self.cursor()
        return cursor.executemany(sql, parameters)
    
    def executescript(self, sql_script: str) -> Cursor:
        """Shortcut to create cursor and executescript"""
        cursor = self.cursor()
        return cursor.executescript(sql_script)
    
    def __enter__(self) -> 'Connection':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()


def connect(
    database: str,
    timeout: float = 5.0,
    detect_types: int = 0,
    isolation_level: Optional[str] = "DEFERRED",
    check_same_thread: bool = True,
    factory: type = Connection,
    cached_statements: int = 128,
    uri: bool = False,
    autocommit: Union[bool, int] = LEGACY_TRANSACTION_CONTROL
) -> Connection:
    """
    Open a connection to a Turso database
    
    Args:
        database: Path to database file
        timeout: Connection timeout (not used)
        detect_types: Type detection flags (not used)
        isolation_level: Transaction isolation level (DEFERRED, IMMEDIATE, EXCLUSIVE, or None)
        check_same_thread: Whether to check thread safety (not enforced)
        factory: Connection factory class
        cached_statements: Number of cached statements (not used)
        uri: Whether database is a URI (not used)
        autocommit: Transaction control mode (False, True, or LEGACY_TRANSACTION_CONTROL)
    
    Returns:
        Connection object
    """
    config = PyTursoDatabaseConfig(
        path=database,
        experimental_features=None,
        async_io=False
    )
    
    db = py_turso_database_open(config)
    
    return factory(db, autocommit=autocommit, isolation_level=isolation_level)
```