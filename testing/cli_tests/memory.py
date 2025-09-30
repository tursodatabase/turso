#!/usr/bin/env python3
import os

from cli_tests import console
from cli_tests.test_turso_cli import TestTursoShell

sqlite_flags = os.getenv("SQLITE_FLAGS", "-q").split(" ")


def validate_with_expected(result: str, expected: str):
    return (expected in result, expected)


def stub_memory_test(
    turso: TestTursoShell,
    name: str,
    blob_size: int = 1024**2,
    vals: int = 100,
    blobs: bool = True,
):
    # zero_blob_size = 1024 **2
    zero_blob = "0" * blob_size * 2
    # vals = 100
    big_stmt = [
        "CREATE TABLE temp (t1 BLOB, t2 INTEGER);",
        "CREATE INDEX temp_index ON temp(t1);",
    ]
    big_stmt = big_stmt + [
        f"INSERT INTO temp (t1) VALUES (zeroblob({blob_size}));"
        if i % 2 == 0 and blobs
        else f"INSERT INTO temp (t2) VALUES ({i});"
        for i in range(vals * 2)
    ]
    expected = []
    for i in range(vals * 2):
        if i % 2 == 0 and blobs:
            big_stmt.append(f"SELECT hex(t1) FROM temp LIMIT 1 OFFSET {i};")
            expected.append(zero_blob)
        else:
            big_stmt.append(f"SELECT t2 FROM temp LIMIT 1 OFFSET {i};")
            expected.append(f"{i}")

    big_stmt.append("SELECT count(*) FROM temp;")
    expected.append(str(vals * 2))

    big_stmt.append("DELETE FROM temp;")
    big_stmt.append("SELECT count(*) FROM temp;")
    expected.append(str(0))

    big_stmt = "".join(big_stmt)
    expected = "\n".join(expected)

    turso.run_test_fn(big_stmt, lambda res: validate_with_expected(res, expected), name)


# TODO no delete tests for now because of turso outputs some debug information on delete
def memory_tests() -> list[dict]:
    tests = []

    for vals in range(0, 1000, 100):
        tests.append(
            {
                "name": f"small-insert-integer-vals-{vals}",
                "vals": vals,
                "blobs": False,
            }
        )

    tests.append(
        {
            "name": f"small-insert-blob-interleaved-blob-size-{1024}",
            "vals": 10,
            "blob_size": 1024,
        }
    )
    tests.append(
        {
            "name": f"big-insert-blob-interleaved-blob-size-{1024}",
            "vals": 100,
            "blob_size": 1024,
        }
    )

    for blob_size in range(0, (1024 * 1024) + 1, 1024 * 4**4):
        if blob_size == 0:
            continue
        tests.append(
            {
                "name": f"small-insert-blob-interleaved-blob-size-{blob_size}",
                "vals": 10,
                "blob_size": blob_size,
            }
        )
        tests.append(
            {
                "name": f"big-insert-blob-interleaved-blob-size-{blob_size}",
                "vals": 100,
                "blob_size": blob_size,
            }
        )
    return tests


def main():
    tests = memory_tests()
    # TODO see how to parallelize this loop with different subprocesses
    for test in tests:
        try:
            with TestTursoShell("") as turso:
                stub_memory_test(turso, **test)
        except Exception as e:
            console.error(f"Test FAILED: {e}")
            exit(1)
    console.info("All tests passed successfully.")


if __name__ == "__main__":
    main()
