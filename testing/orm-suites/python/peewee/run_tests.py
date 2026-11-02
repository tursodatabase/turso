"""Runs Peewee's test suite with JUnit XML output, on sqlite3 or on turso.

Usage (from the Peewee checkout): run_tests.py <sqlite|turso> <out.xml> [test modules...]
With `turso`, every SqliteDatabase (and subclass) connects through turso.
"""

import sys

import peewee
import runtests
import turso_sqlite3
import xmlrunner


def turso_connect(self):
    # Same as peewee.SqliteDatabase._connect, with turso in place of sqlite3.
    conn = turso_sqlite3.connect(self.database, timeout=self._timeout, isolation_level=None, **self.connect_params)
    try:
        self._add_conn_hooks(conn)
    except Exception:
        conn.close()
        raise
    return conn


driver, output, modules = sys.argv[1], sys.argv[2], sys.argv[3:]
if driver == "turso":
    peewee.SqliteDatabase._connect = turso_connect
    # Peewee picks its SQL by SQLite version, read from the stdlib sqlite3
    # module at import. Report turso's version so it doesn't send turso syntax
    # from a newer SQLite.
    peewee.__sqlite_version__ = peewee.SqliteDatabase.server_version = turso_sqlite3.sqlite_version_info
with open(output, "wb") as out:
    xmlrunner.XMLTestRunner(output=out, verbosity=1).run(runtests.collect_tests(modules))
if driver == "turso":
    print("sqlite3 APIs missing in turso, called:", dict(turso_sqlite3.MISSING_CALLS))
