#!/usr/bin/env python3

from cli_tests import console
from cli_tests.test_turso_cli import TestTursoShell
from pydantic import BaseModel

# This tests verify that experimental MVCC feature works as expected. The test
# suite will go away once the feature becomes more stable because we will just
# run the TCL tests, for example, with MVCC enabled.

class MVCCTest(BaseModel):
    pass


def test_create_table_with_mvcc():
    """Test CREATE TABLE t(x) with MVCC journal mode"""
    shell = TestTursoShell(init_commands="PRAGMA journal_mode = 'experimental_mvcc';")
    shell.run_test("create-table-mvcc", "CREATE TABLE t(x);", "")
    shell.run_test("insert-mvcc", "INSERT INTO t(x) VALUES (1);", "")
    shell.quit()


def main():
    console.info("Running MVCC CLI tests...")
    test_create_table_with_mvcc()
    console.info("All MVCC tests have passed")


if __name__ == "__main__":
    main()
