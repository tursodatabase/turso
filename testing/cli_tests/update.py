#!/usr/bin/env python3
import os

from cli_tests import console
from cli_tests.test_turso_cli import TestTursoShell
from pydantic import BaseModel

sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


class UpdateTest(BaseModel):
    name: str
    db_schema: str = "CREATE TABLE test (key INTEGER, t1 BLOB, t2 INTEGER, t3 TEXT);"
    blob_size: int = 1024
    vals: int = 1000
    updates: int = 1
    db_path: str = "testing/system/update.db"

    def init_db(self):
        with TestTursoShell(
            init_commands="",
            exec_name="sqlite3",
            flags=f"{self.db_path}",
        ) as sqlite:
            sqlite.execute_dot(f".open {self.db_path}")
            zero_blob = "0" * self.blob_size * 2
            t2_val = "1"
            t3_val = "2"
            stmt = [self.db_schema]
            stmt = stmt + [
                f"INSERT INTO test (key, t1, t2, t3) VALUES ({i} ,zeroblob({self.blob_size}), {t2_val}, {t3_val});"
                for i in range(self.vals)
            ]
            stmt.append("SELECT count(*) FROM test;")

            sqlite.run_test(
                "Init Update Db in Sqlite",
                "".join(stmt),
                f"{self.vals}",
            )

            stmt = [f"SELECT hex(t1), t2, t3 FROM test LIMIT 1 OFFSET {i};" for i in range(self.vals)]

            expected = [f"{zero_blob}|{t2_val}|{t3_val}" for _ in range(self.vals)]
            sqlite.run_test(
                "Check Values correctly inserted in Sqlite",
                "".join(stmt),
                "\n".join(expected),
            )

    def run(self, limbo: TestTursoShell):
        limbo.execute_dot(f".open {self.db_path}")
        # TODO blobs are hard. Forget about blob updates for now
        # one_blob = ("0" * ((self.blob_size * 2) - 1)) + "1"
        # TODO For now update just on one row. To expand the tests in the future
        # use self.updates and do more than 1 update
        t2_update_val = "123"
        stmt = f"UPDATE test SET t2 = {t2_update_val} WHERE key = {0};"
        limbo.run_test(self.name, stmt, "")

    def test_compat(self):
        console.info("Testing in SQLite\n")

        with TestTursoShell(
            init_commands="",
            exec_name="sqlite3",
            flags=f"{self.db_path}",
        ) as sqlite:
            sqlite.execute_dot(f".open {self.db_path}")
            zero_blob = "0" * self.blob_size * 2

            t2_val = "1"
            t2_update_val = "123"
            t3_val = "2"
            stmt = []
            stmt.append("SELECT count(*) FROM test;")

            sqlite.run_test(
                "Check all rows present in Sqlite",
                "".join(stmt),
                f"{self.vals}",
            )

            stmt = [f"SELECT hex(t1), t2, t3 FROM test LIMIT 1 OFFSET {i};" for i in range(self.vals)]

            expected = [
                f"{zero_blob}|{t2_val}|{t3_val}" if i != 0 else f"{zero_blob}|{t2_update_val}|{t3_val}"
                for i in range(self.vals)
            ]
            sqlite.run_test(
                "Check Values correctly updated in Sqlite",
                "".join(stmt),
                "\n".join(expected),
            )
        console.info()


def cleanup(db_fullpath: str):
    wal_path = f"{db_fullpath}-wal"
    shm_path = f"{db_fullpath}-shm"
    paths = [db_fullpath, wal_path, shm_path]
    for path in paths:
        if os.path.exists(path):
            os.remove(path)


def main():
    test = UpdateTest(name="Update 1 column", vals=1)
    console.info(test)

    db_path = test.db_path
    try:
        test.init_db()
        # Use with syntax to automatically close shell on error
        with TestTursoShell("") as limbo:
            test.run(limbo)

        test.test_compat()

    except Exception as e:
        console.error(f"Test FAILED: {e}")
        cleanup(db_path)
        exit(1)
    # delete db after every compat test so we we have fresh db for next test
    cleanup(db_path)
    console.info("All tests passed successfully.")


if __name__ == "__main__":
    main()
