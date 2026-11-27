"""Django's sqlite3 backend with the `sqlite3` driver module replaced by turso."""

from collections.abc import Mapping
from itertools import tee

import turso_sqlite3 as Database
from django.db.backends.sqlite3 import base as sqlite_base
from django.db.backends.sqlite3._functions import register as register_functions


class SQLiteCursorWrapper(Database.Cursor):
    """Copy of sqlite_base.SQLiteCursorWrapper on top of the turso cursor.

    Django's class subclasses the stdlib cursor and calls zero-argument
    super(), so its methods cannot be reused here.
    """

    convert_query = sqlite_base.SQLiteCursorWrapper.convert_query

    def execute(self, query, params=None):
        if params is None:
            return Database.Cursor.execute(self, query)
        param_names = list(params) if isinstance(params, Mapping) else None
        return Database.Cursor.execute(self, self.convert_query(query, param_names=param_names), params)

    def executemany(self, query, param_list):
        peekable, param_list = tee(iter(param_list))
        params = next(peekable, None)
        param_names = list(params) if params and isinstance(params, Mapping) else None
        return Database.Cursor.executemany(self, self.convert_query(query, param_names=param_names), param_list)


class DatabaseWrapper(sqlite_base.DatabaseWrapper):
    Database = Database

    def get_new_connection(self, conn_params):
        conn = Database.connect(**conn_params)
        register_functions(_MathFunctionsBuiltIn(conn))
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA legacy_alter_table = OFF")
        for init_command in self.init_commands:
            if init_command := init_command.strip():
                conn.execute(init_command)
        return conn

    def create_cursor(self, name=None):
        return self.connection.cursor(factory=SQLiteCursorWrapper)


class _MathFunctionsBuiltIn:
    """Connection proxy for Django's register_functions().

    register_functions() asks `sqlite_compileoption_used('ENABLE_MATH_FUNCTIONS')`,
    which turso does not implement. Turso has the math functions built in, so
    answer 1 for that query and forward everything else.
    """

    def __init__(self, conn):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def execute(self, sql, *args):
        if "sqlite_compileoption_used('ENABLE_MATH_FUNCTIONS')" in sql:
            return _OneRow((1,))
        return self._conn.execute(sql, *args)


class _OneRow:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row
