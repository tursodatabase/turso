#!/usr/bin/env python3
import os
import tempfile
from cli_tests.test_limbo_cli import TestLimboShell
from pydantic import BaseModel
from cli_tests import console


sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


class InsertTest(BaseModel):
    name: str
    db_schema: str = "CREATE TABLE test (t1 BLOB, t2 INTEGER);"
    blob_size: int = 1024**2
    vals: int = 100
    has_blob: bool = True
    db_path: str = ""

    def run(self, limbo: TestLimboShell):
        zero_blob = "0" * self.blob_size * 2
        big_stmt = [self.db_schema, "CREATE INDEX test_index ON test(t1);"]
        big_stmt = big_stmt + [
            f"INSERT INTO test (t1) VALUES (zeroblob({self.blob_size}));"
            if i % 2 == 0 and self.has_blob
            else f"INSERT INTO test (t2) VALUES ({i});"
            for i in range(self.vals * 2)
        ]
        expected = []
        for i in range(self.vals * 2):
            if i % 2 == 0 and self.has_blob:
                big_stmt.append(f"SELECT hex(t1) FROM test LIMIT 1 OFFSET {i};")
                expected.append(zero_blob)
            else:
                big_stmt.append(f"SELECT t2 FROM test LIMIT 1 OFFSET {i};")
                expected.append(f"{i}")

        big_stmt.append("SELECT count(*) FROM test;")
        expected.append(str(self.vals * 2))

        big_stmt.append("DELETE FROM temp;")
        big_stmt.append("SELECT count(*) FROM temp;")
        expected.append(str(0))

        big_stmt = "".join(big_stmt)
        expected = "\n".join(expected)

        limbo.run_test_fn(
            big_stmt, lambda res: validate_with_expected(res, expected), self.name
        )

    def test_compat(self):
        console.info("Testing in SQLite\n")

        with TestLimboShell(
            init_commands="",
            exec_name="sqlite3",
            flags=f"{self.db_path}",
        ) as sqlite:
            sqlite.run_test_fn(
                ".show",
                lambda res: f"filename: {self.db_path}" in res,
                "Opened db file created with Limbo in sqlite3",
            )
            sqlite.run_test_fn(
                ".schema",
                lambda res: self.db_schema in res,
                "Tables created by previous Limbo test exist in db file",
            )
            sqlite.run_test_fn(
                "SELECT count(*) FROM test;",
                lambda res: res == str(self.vals * 2),
                "Counting total rows inserted",
            )
            sqlite.run_test_fn(
                "PRAGMA integrity_check;",
                lambda res: res == "ok",
                "Integrity Check",
            )
        console.info()


def validate_with_expected(result: str, expected: str):
    return (expected in result, expected)


# TODO no delete tests for now
def blob_tests() -> list[InsertTest]:
    tests: list[InsertTest] = []

    for vals in range(0, 1000, 100):
        tests.append(
            InsertTest(
                name=f"small-insert-integer-vals-{vals}",
                vals=vals,
                has_blob=False,
            )
        )

    tests.append(
        InsertTest(
            name=f"small-insert-blob-interleaved-blob-size-{1024}",
            vals=10,
            blob_size=1024,
        )
    )
    tests.append(
        InsertTest(
            name=f"big-insert-blob-interleaved-blob-size-{1024}",
            vals=100,
            blob_size=1024,
        )
    )

    for blob_size in range(0, (1024 * 1024) + 1, 1024 * 4**4):
        if blob_size == 0:
            continue
        tests.append(
            InsertTest(
                name=f"small-insert-blob-interleaved-blob-size-{blob_size}",
                vals=10,
                blob_size=blob_size,
            )
        )
        tests.append(
            InsertTest(
                name=f"big-insert-blob-interleaved-blob-size-{blob_size}",
                vals=100,
                blob_size=blob_size,
            )
        )
    return tests


def cleanup(db_fullpath: str):
    wal_path = f"{db_fullpath}-wal"
    shm_path = f"{db_fullpath}-shm"
    paths = [db_fullpath, wal_path, shm_path]
    for path in paths:
        if os.path.exists(path):
            os.remove(path)


def main():
    tests = blob_tests()
    for test in tests:
        console.info(test)
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            test.db_path = tmp.name
            try:
                # Use with syntax to automatically close shell on error
                with TestLimboShell("") as limbo:
                    limbo.execute_dot(f".open {test.db_path}")
                    test.run(limbo)
                test.test_compat()

            except Exception as e:
                console.error(f"Test FAILED: {e}")
                cleanup(test.db_path)
                exit(1)
            # delete db after every compat test so we we have fresh db for next test
            cleanup(test.db_path)
    console.info("All tests passed successfully.")


if __name__ == "__main__":
    main()
