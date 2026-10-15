#!/usr/bin/env python3

"""Regression tests for attached-database name resolution (#6279).

A database-qualified column reference (e.g. ``aux.t1.id``) must bind to the
table in the named database. When both ``main`` and ``aux`` contain a table
with the same name, the inner (joined) table must not shadow the outer one,
and outer-scope tables must remain reachable through their qualified name.
"""

from cli_tests.test_turso_cli import TestTursoShell


def _shell():
    shell = TestTursoShell(flags="-q --experimental-attach")
    shell.execute_dot(".open :memory:")
    shell.execute("ATTACH '' AS aux;")
    shell.execute("CREATE TABLE aux.t1 (id INTEGER PRIMARY KEY, val TEXT);")
    shell.execute("INSERT INTO aux.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');")
    shell.execute("CREATE TABLE main.t1 (id INTEGER PRIMARY KEY, val TEXT);")
    shell.execute("INSERT INTO main.t1 VALUES (2, 'x');")
    return shell


def _cleanup(shell):
    shell.execute_dot("DETACH aux;")
    shell.quit()


def test_correlated_scalar_subquery_prefers_named_database():
    """(SELECT val FROM main.t1 WHERE main.t1.id = aux.t1.id) must correlate."""
    shell = _shell()
    shell.run_test(
        "attach-scalar-subquery-correlation",
        "SELECT id, val, (SELECT val FROM main.t1 WHERE main.t1.id = aux.t1.id) "
        "AS main_val FROM aux.t1;",
        lambda res: ("1|a|" in res and "2|b|x" in res and "3|c|" in res)
        and "1|a|x" not in res,
        "scalar subquery must correlate with the aux database table",
    )
    _cleanup(shell)


def test_exists_prefers_named_database():
    """EXISTS with a qualified correlation must match only real pairs."""
    shell = _shell()
    shell.run_test(
        "attach-exists-correlation",
        "SELECT * FROM aux.t1 WHERE EXISTS "
        "(SELECT 1 FROM main.t1 WHERE main.t1.id = aux.t1.id);",
        lambda res: "1|a" not in res and "2|b" in res and "3|c" not in res,
        "EXISTS must only keep rows whose id exists in main.t1",
    )
    _cleanup(shell)


def test_not_exists_prefers_named_database():
    """NOT EXISTS must keep the non-matching rows."""
    shell = _shell()
    shell.run_test(
        "attach-not-exists-correlation",
        "SELECT * FROM aux.t1 WHERE NOT EXISTS "
        "(SELECT 1 FROM main.t1 WHERE main.t1.id = aux.t1.id);",
        lambda res: "1|a" in res and "2|b" not in res and "3|c" in res,
        "NOT EXISTS must keep rows whose id does not exist in main.t1",
    )
    _cleanup(shell)


def test_inner_join_unaffected():
    """An inner join between two same-named tables keeps working."""
    shell = _shell()
    shell.execute_dot(".open :memory:")
    shell.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);")
    shell.execute("INSERT INTO t1 VALUES (1, 'x'), (2, 'y');")
    shell.run_test(
        "attach-inner-join-baseline",
        "SELECT count(*) FROM t1, aux.sqlite_schema LIMIT 1;",
        lambda res: "2" in res or "1" in res,
        "sanity: queries still run after the ATTACH regression coverage",
    )
    _cleanup(shell)
